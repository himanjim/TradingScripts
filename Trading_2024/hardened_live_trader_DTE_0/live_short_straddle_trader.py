"""
live_short_straddle_trader.py
==============================
Live/paper implementation of ``atm_straddle_expiry_day_V3OPT.py`` using the
parameters in ``straddle_config_v3opt_DTE0.properties``.

Implemented V3OPT behaviour
---------------------------
* AUTO selection of the eligible NIFTY/SENSEX expiry under ``ALLOWED_DTE``.
* ATM short-straddle entry at ``ENTRY_TIME_IST``.
* Momentum gate for the first entry and every re-entry: current ATM CE+PE LTP
  must be below the same strike's combined premium ``MOMENTUM_LOOKBACK_MIN``
  minutes earlier.
* Attempt-specific percentage stop, capped by an absolute rupee stop.
* Percentage profit-protect arm/give-back.
* Per-underlying NIFTY/SENSEX profiles.
* Late-session tighter profit-protect give-back.
* Normal and late-session profit targets.
* Re-entry after STOPLOSS, PROFIT_PROTECT and (when enabled) PROFIT_TARGET.
* Per-attempt re-entry delays and maximum re-attempt count.
* Daily loss circuit breaker based on estimated net P&L after charges.
* Hard strategy exit at ``EXIT_TIME_IST``.
* WebSocket LTP monitoring, restart state persistence, and live broker
  reconciliation.

Safety
------
``PAPER_TRADING=1`` is the built-in and supplied configuration default. In
paper mode no order-placement API is called. Do not change it to 0 until the
logs have been checked over several expiry sessions.

The live execution layer is fail-closed and bounded:
* every entry requires COMPLETE order rows plus exact CE/PE broker positions;
* ambiguous place-order responses are reconciled before any resubmission;
* at most four complete entry cycles are allowed;
* failed cycles cancel pending orders and verify a flat book before retrying;
* unresolved residual exposure raises a fatal error and stops automation;
* API retries and process restart loops have finite limits.

Live/backtest parity note
-------------------------
The backtester evaluates one-minute OHLC bars. A live process sees ticks. Stops,
targets and trailing exits therefore trigger on observed live LTP and actual
fills, not on synthetic intrabar high/low assumptions. The strategy rules and
threshold calculations are the same; individual fills will not be identical.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Callable, Iterable, Optional
from zoneinfo import ZoneInfo


# =============================================================================
# 0. PROPERTY FILE LOADING
# =============================================================================
def _load_property_file() -> str:
    """Load KEY=VALUE settings before importing OptionTradeUtils_env."""
    cfg_path = os.getenv(
        "STRADDLE_LIVE_CONFIG",
        str(Path(__file__).resolve().parent / "live_trader_config.properties"),
    )
    if not os.path.exists(cfg_path):
        print(f"[CONFIG] Property file not found: {cfg_path}; using code defaults.")
        return cfg_path

    loaded = 0
    with open(cfg_path, "r", encoding="utf-8") as handle:
        for raw in handle:
            line = raw.strip()
            if not line or line.startswith(("#", ";")) or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key, value = key.strip(), value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                value = value[1:-1]
            # Explicit process environment variables override the property file.
            if key and key not in os.environ:
                os.environ[key] = value
                loaded += 1
    print(f"[CONFIG] Loaded {loaded} setting(s) from {cfg_path}")
    return cfg_path


PROPERTY_FILE_PATH = _load_property_file()

# Import after property loading so AUTO/ALLOWED_DTE are visible to the utility.
import OptionTradeUtils_env as oUtils  # noqa: E402

try:  # noqa: E402
    from kiteconnect import KiteTicker
except Exception:  # pragma: no cover
    KiteTicker = None


# =============================================================================
# 1. CONFIG PARSING
# =============================================================================
IST = ZoneInfo("Asia/Kolkata")
SESSION_START_IST = dtime(9, 15)
SESSION_END_IST = dtime(15, 30)


def _bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return float(default)
    try:
        return float(str(raw).replace(",", "").strip())
    except ValueError as exc:
        raise RuntimeError(f"Invalid numeric value for {name}: {raw}") from exc


def _int_env(name: str, default: int) -> int:
    return int(round(_float_env(name, float(default))))


def _parse_hhmm(raw: str) -> dtime:
    try:
        hh, mm = str(raw).strip().split(":", 1)
        value = dtime(int(hh), int(mm))
    except Exception as exc:
        raise RuntimeError(f"Invalid HH:MM value: {raw}") from exc
    return value


def _csv_ints(raw: Optional[str], default: Iterable[int]) -> list[int]:
    if raw is None or not str(raw).strip():
        return list(default)
    try:
        values = [int(round(float(x.strip()))) for x in str(raw).split(",") if x.strip()]
    except ValueError as exc:
        raise RuntimeError(f"Invalid integer list: {raw}") from exc
    return values or list(default)


def _parse_pct_value(raw: Any) -> float:
    """Accept 0.33, 33, or 33% and return decimal fraction 0.33."""
    text = str(raw).strip().replace("%", "")
    if not text:
        raise RuntimeError("Blank percentage value")
    value = float(text)
    if abs(value) > 1.0:
        value /= 100.0
    if value < 0:
        raise RuntimeError(f"Percentage cannot be negative: {raw}")
    return float(value)


def _csv_pcts(raw: Optional[str], default: Iterable[float]) -> list[float]:
    if raw is None or not str(raw).strip():
        return [_parse_pct_value(x) for x in default]
    try:
        values = [_parse_pct_value(x) for x in str(raw).split(",") if x.strip()]
    except ValueError as exc:
        raise RuntimeError(f"Invalid percentage list: {raw}") from exc
    return values or [_parse_pct_value(x) for x in default]


def _underlying_env(name: str, key: str, fallback: Any) -> str:
    raw = os.getenv(f"{name}_{key}")
    return str(fallback) if raw is None or not str(raw).strip() else str(raw).strip()


# Hard safety default: paper.
PAPER_TRADING = _bool_env("PAPER_TRADING", True)

ENTRY_TIME_IST = os.getenv("ENTRY_TIME_IST", "09:16")
EXIT_TIME_IST = os.getenv("EXIT_TIME_IST", os.getenv("SQUAREOFF_TIME_IST", "15:26"))
ENTRY_TIME = _parse_hhmm(ENTRY_TIME_IST)
EXIT_TIME = _parse_hhmm(EXIT_TIME_IST)

ENTRY_MOMENTUM_GATE = _bool_env("ENTRY_MOMENTUM_GATE", True)
MOMENTUM_LOOKBACK_MIN = _int_env("MOMENTUM_LOOKBACK_MIN", 8)
MOMENTUM_RETRY_SECONDS = _float_env("MOMENTUM_RETRY_SECONDS", 5.0)
MAX_LATE_START_MINUTES = _int_env("MAX_LATE_START_MINUTES", 15)

LOSS_LIMIT_RUPEES_BY_ATTEMPT = _csv_pcts(
    os.getenv("LOSS_LIMIT_RUPEES_BY_ATTEMPT", os.getenv("STOP_PCT_BY_ATTEMPT")),
    [0.5877, 0.6054, 0.6230, 0.6406, 0.6583, 0.6759, 0.6935],
)
MAX_LOSS_LIMIT_RUPEES_BY_ATTEMPT = _float_env(
    "MAX_LOSS_LIMIT_RUPEES_BY_ATTEMPT",
    _float_env("STOP_CAP_RUPEES", 3000.0),
)

PROFIT_PROTECT_TRIGGER_RUPEES = _parse_pct_value(
    os.getenv("PROFIT_PROTECT_TRIGGER_RUPEES", os.getenv("PROFIT_PROTECT_PCT", "0.3755"))
)
PROFIT_TARGET_PCT = _parse_pct_value(os.getenv("PROFIT_TARGET_PCT", "0.75"))
PROFIT_TARGET_PCT_LATE = _parse_pct_value(os.getenv("PROFIT_TARGET_PCT_LATE", "0.50"))
PROFIT_TARGET_LATE_FROM_IST = os.getenv("PROFIT_TARGET_LATE_FROM_IST", "14:15")
PROFIT_TARGET_REENTRY_ENABLED = _bool_env("PROFIT_TARGET_REENTRY_ENABLED", True)
PROFIT_TARGET_REENTRY_DELAY = _int_env("PROFIT_TARGET_REENTRY_DELAY", 3)

PROFIT_PROTECT_LATE_GIVEBACK = _parse_pct_value(
    os.getenv("PROFIT_PROTECT_LATE_GIVEBACK", "0")
)
PROFIT_PROTECT_LATE_FROM_IST = os.getenv("PROFIT_PROTECT_LATE_FROM_IST", "14:30")

MAX_DAILY_LOSS_RUPEES = _float_env("MAX_DAILY_LOSS_RUPEES", 30000.0)
MAX_REATTEMPTS = _int_env("MAX_REATTEMPTS", 12)
REENTRY_DELAY_BY_ATTEMPT = _csv_ints(
    os.getenv("REENTRY_DELAY_BY_ATTEMPT"), [7, 7, 7, 7, 7, 7, 7, 7]
)

ALLOWED_DTE = _csv_ints(os.getenv("ALLOWED_DTE"), [0])
ENFORCE_DTE = _bool_env("ENFORCE_DTE", True)

MONITOR_POLL_SECONDS = _float_env("MONITOR_POLL_SECONDS", 0.20)
MONITOR_HEARTBEAT_SECONDS = _float_env("MONITOR_HEARTBEAT_SECONDS", 5.0)
PAPER_SLIPPAGE_TICKS = _int_env("PAPER_SLIPPAGE_TICKS", 1)
OPTION_TICK = _float_env("OPTION_TICK", 0.05)

ORDER_STATUS_POLL_SECONDS = _float_env("ORDER_STATUS_POLL_SECONDS", 0.50)
ORDER_STATUS_MAX_POLLS = _int_env("ORDER_STATUS_MAX_POLLS", 20)
ORDER_CONFIRM_TIMEOUT_SECONDS = _float_env("ORDER_CONFIRM_TIMEOUT_SECONDS", 12.0)
ORDER_PRODUCT = os.getenv("ORDER_PRODUCT", "NRML").strip().upper()

# Live entry execution is retried as a complete, reconciled two-leg cycle.
# A cycle never blindly resubmits an order after an ambiguous API response.
ENTRY_EXECUTION_MAX_ATTEMPTS = _int_env("ENTRY_EXECUTION_MAX_ATTEMPTS", 4)
ENTRY_EXECUTION_RETRY_DELAY_SECONDS = _float_env(
    "ENTRY_EXECUTION_RETRY_DELAY_SECONDS", 2.0
)
AMBIGUOUS_ORDER_RECONCILE_SECONDS = _float_env(
    "AMBIGUOUS_ORDER_RECONCILE_SECONDS", 8.0
)
CLEANUP_MAX_ATTEMPTS = _int_env("CLEANUP_MAX_ATTEMPTS", 4)
CLEANUP_VERIFY_DELAY_SECONDS = _float_env("CLEANUP_VERIFY_DELAY_SECONDS", 0.75)

# All API retries are bounded. The historical value 0 (infinite retry) is no
# longer accepted by configuration validation. Place-order calls themselves
# always use one network submission and are reconciled through orders/positions.
API_MAX_RETRIES = _int_env("API_MAX_RETRIES", 5)
API_ORDER_MAX_RETRIES = _int_env("API_ORDER_MAX_RETRIES", 3)
API_RETRY_BACKOFF_SECONDS = _float_env("API_RETRY_BACKOFF_SECONDS", 2.0)
API_RETRY_BACKOFF_MAX = _float_env("API_RETRY_BACKOFF_MAX", 15.0)
API_HTTP_TIMEOUT_SECONDS = _float_env("API_HTTP_TIMEOUT_SECONDS", 10.0)
PROCESS_RESTART_MAX_ATTEMPTS = _int_env("PROCESS_RESTART_MAX_ATTEMPTS", 3)
PROCESS_RESTART_DELAY_SECONDS = _float_env("PROCESS_RESTART_DELAY_SECONDS", 5.0)
# Serialize REST calls and keep their start times apart. WebSocket monitoring is
# unaffected. The default is deliberately conservative for Kite REST limits.
REST_MIN_INTERVAL_SECONDS = _float_env("REST_MIN_INTERVAL_SECONDS", 0.35)

LOG_FILE = os.getenv(
    "LOG_FILE", str(Path.home() / "short_straddle_v3opt_live.log")
)
STATE_FILE = os.getenv(
    "STATE_FILE", str(Path.home() / "short_straddle_v3opt_state.json")
)

# Estimated charges: same formula used by the supplied V3OPT backtester.
INCLUDE_TRANSACTION_COSTS = _bool_env("INCLUDE_TRANSACTION_COSTS", True)
BROKERAGE_PER_ORDER = _float_env("BROKERAGE_PER_ORDER", 20.0)
ORDERS_PER_TRADE = _int_env("ORDERS_PER_TRADE", 4)
STT_SELL_PCT = _float_env("STT_SELL_PCT", 0.001)
EXCHANGE_TXN_PCT = _float_env("EXCHANGE_TXN_PCT", 0.0003553)
SEBI_PER_CRORE = _float_env("SEBI_PER_CRORE", 10.0)
STAMP_BUY_PCT = _float_env("STAMP_BUY_PCT", 0.00003)
IPFT_PER_CRORE = _float_env("IPFT_PER_CRORE", 0.010)
GST_PCT = _float_env("GST_PCT", 0.18)


@dataclass(frozen=True)
class StrategyProfile:
    underlying: str
    profit_target_pct: float
    profit_target_pct_late: float
    profit_target_late_from: dtime
    profit_protect_pct: float
    max_daily_loss_rupees: float
    late_giveback_pct: float
    late_giveback_from: dtime


def resolve_profile(underlying: str) -> StrategyProfile:
    """Resolve the V3OPT per-underlying profile."""
    return StrategyProfile(
        underlying=underlying,
        profit_target_pct=_parse_pct_value(
            _underlying_env(underlying, "PROFIT_TARGET_PCT", PROFIT_TARGET_PCT)
        ),
        profit_target_pct_late=_parse_pct_value(
            _underlying_env(underlying, "PROFIT_TARGET_PCT_LATE", PROFIT_TARGET_PCT_LATE)
        ),
        profit_target_late_from=_parse_hhmm(
            _underlying_env(
                underlying, "PROFIT_TARGET_LATE_FROM_IST", PROFIT_TARGET_LATE_FROM_IST
            )
        ),
        profit_protect_pct=_parse_pct_value(
            _underlying_env(
                underlying,
                "PROFIT_PROTECT_TRIGGER_RUPEES",
                PROFIT_PROTECT_TRIGGER_RUPEES,
            )
        ),
        max_daily_loss_rupees=float(
            _underlying_env(
                underlying, "MAX_DAILY_LOSS_RUPEES", MAX_DAILY_LOSS_RUPEES
            )
        ),
        late_giveback_pct=_parse_pct_value(
            _underlying_env(
                underlying,
                "PROFIT_PROTECT_LATE_GIVEBACK",
                PROFIT_PROTECT_LATE_GIVEBACK,
            )
        ),
        late_giveback_from=_parse_hhmm(
            _underlying_env(
                underlying,
                "PROFIT_PROTECT_LATE_FROM_IST",
                PROFIT_PROTECT_LATE_FROM_IST,
            )
        ),
    )


# =============================================================================
# 2. LOGGING AND API RETRY
# =============================================================================
def _build_logger() -> logging.Logger:
    logger = logging.getLogger("ss_v3opt_live")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    try:
        Path(LOG_FILE).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            LOG_FILE, maxBytes=5_000_000, backupCount=5, encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception as exc:
        print(f"[LOG] File logging unavailable ({exc}); console logging remains active.")

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger


log = _build_logger()

_REST_CALL_LOCK = threading.Lock()
_REST_LAST_CALL_MONOTONIC = 0.0


def _respect_rest_interval() -> None:
    """Serialize REST calls and enforce a minimum interval between starts."""
    global _REST_LAST_CALL_MONOTONIC
    if REST_MIN_INTERVAL_SECONDS <= 0:
        return
    elapsed = time.monotonic() - _REST_LAST_CALL_MONOTONIC
    wait = REST_MIN_INTERVAL_SECONDS - elapsed
    if wait > 0:
        time.sleep(wait)
    _REST_LAST_CALL_MONOTONIC = time.monotonic()


def _api(
    fn: Callable[..., Any],
    *args: Any,
    desc: str = "Kite API call",
    max_retries: Optional[int] = None,
    deadline: Optional[datetime] = None,
    **kwargs: Any,
) -> Any:
    """Call a Kite API function with a finite retry budget.

    ``max_retries`` is retained as the configuration/API name for backward
    compatibility, but represents the maximum TOTAL call attempts. A
    ``place_order`` submission must always pass ``max_retries=1``; ambiguous
    responses are reconciled from the broker order book before any resubmission.
    """
    attempt_limit = API_MAX_RETRIES if max_retries is None else int(max_retries)
    if attempt_limit < 1:
        raise RuntimeError(
            f"{desc}: retry limit must be at least 1; unbounded retries are disabled."
        )

    delay = max(0.0, API_RETRY_BACKOFF_SECONDS)
    last_error: Optional[BaseException] = None
    for attempt in range(1, attempt_limit + 1):
        if deadline is not None and now_ist() >= deadline:
            raise TimeoutError(f"{desc}: deadline reached before API attempt {attempt}.")
        try:
            with _REST_CALL_LOCK:
                _respect_rest_interval()
                if deadline is not None and now_ist() >= deadline:
                    raise TimeoutError(f"{desc}: deadline reached before request start.")
                return fn(*args, **kwargs)
        except Exception as exc:
            last_error = exc
            if attempt >= attempt_limit:
                log.error(
                    f"[API] {desc} failed after {attempt}/{attempt_limit} attempt(s): {exc}"
                )
                break

            sleep_for = min(delay, API_RETRY_BACKOFF_MAX)
            if deadline is not None:
                remaining = (deadline - now_ist()).total_seconds()
                if remaining <= 0:
                    break
                sleep_for = min(sleep_for, max(0.0, remaining))
            log.warning(
                f"[API] {desc} failed ({attempt}/{attempt_limit}): {exc}; "
                f"retrying in {sleep_for:.1f}s"
            )
            if sleep_for > 0:
                time.sleep(sleep_for)
            delay = min(max(delay * 2.0, 0.1), API_RETRY_BACKOFF_MAX)

    if last_error is None:
        raise RuntimeError(f"{desc}: API call ended without a result.")
    raise last_error


# =============================================================================
# 3. SMALL STRATEGY HELPERS
# =============================================================================
def now_ist() -> datetime:
    return datetime.now(IST)


def today_ist() -> date:
    return now_ist().date()


def combine_ist(day: date, value: dtime) -> datetime:
    return datetime.combine(day, value, tzinfo=IST)


def round_to_tick(price: float, tick: float = OPTION_TICK) -> float:
    value = max(float(price), tick)
    return round(round(value / tick) * tick, 2)


def round_to_step(value: float, step: int) -> int:
    return int(round(float(value) / int(step)) * int(step))


def stop_pct_for_attempt(attempt_idx: int) -> float:
    values = LOSS_LIMIT_RUPEES_BY_ATTEMPT
    if not values:
        return 0.0
    return float(values[attempt_idx]) if attempt_idx < len(values) else float(values[-1])


def reentry_delay_for_attempt(attempt_idx: int) -> int:
    values = REENTRY_DELAY_BY_ATTEMPT
    if not values:
        return 0
    return int(values[attempt_idx]) if attempt_idx < len(values) else int(values[-1])


def effective_stop_rupees(attempt_idx: int, premium_sum: float) -> float:
    uncapped = stop_pct_for_attempt(attempt_idx) * premium_sum
    if MAX_LOSS_LIMIT_RUPEES_BY_ATTEMPT > 0:
        return float(min(uncapped, MAX_LOSS_LIMIT_RUPEES_BY_ATTEMPT))
    return float(uncapped)


def compute_trade_charges(
    entry_ce: float,
    entry_pe: float,
    exit_ce: float,
    exit_pe: float,
    qty: int,
) -> float:
    """Estimate charges for one complete short-straddle attempt."""
    if not INCLUDE_TRANSACTION_COSTS:
        return 0.0
    entry_turnover = (entry_ce + entry_pe) * qty
    exit_turnover = (exit_ce + exit_pe) * qty
    total_turnover = entry_turnover + exit_turnover

    brokerage = BROKERAGE_PER_ORDER * ORDERS_PER_TRADE
    stt = entry_turnover * STT_SELL_PCT
    txn = total_turnover * EXCHANGE_TXN_PCT
    sebi = total_turnover * SEBI_PER_CRORE / 1_00_00_000
    stamp = exit_turnover * STAMP_BUY_PCT
    ipft = total_turnover * IPFT_PER_CRORE / 1_00_00_000
    gst = (brokerage + txn + sebi) * GST_PCT
    return round(brokerage + stt + txn + sebi + stamp + ipft + gst, 2)


# =============================================================================
# 4. KITE WEBSOCKET PRICE FEED
# =============================================================================
class PriceFeed:
    """Thread-safe latest-LTP cache with dynamic subscriptions."""

    def __init__(self, api_key: str, access_token: str):
        if KiteTicker is None:
            raise RuntimeError("kiteconnect is not installed. Run: pip install kiteconnect")
        try:
            self.ticker = KiteTicker(
                api_key,
                access_token,
                reconnect=True,
                reconnect_max_tries=300,
                reconnect_max_delay=60,
            )
        except TypeError:
            self.ticker = KiteTicker(api_key, access_token)

        self._lock = threading.Lock()
        self._connected = threading.Event()
        self._subscribed: set[int] = set()
        self._ltp: dict[int, float] = {}

        self.ticker.on_ticks = self._on_ticks
        self.ticker.on_connect = self._on_connect
        self.ticker.on_close = self._on_close
        self.ticker.on_error = self._on_error
        self.ticker.on_reconnect = self._on_reconnect
        self.ticker.on_noreconnect = self._on_noreconnect

    def _on_ticks(self, ws: Any, ticks: list[dict[str, Any]]) -> None:
        with self._lock:
            for tick in ticks:
                token = tick.get("instrument_token")
                price = tick.get("last_price")
                if token is not None and price is not None:
                    self._ltp[int(token)] = float(price)

    def _on_connect(self, ws: Any, response: Any) -> None:
        log.info("[WS] Connected.")
        self._connected.set()
        if self._subscribed:
            tokens = list(self._subscribed)
            ws.subscribe(tokens)
            ws.set_mode(ws.MODE_LTP, tokens)

    def _on_close(self, ws: Any, code: Any, reason: Any) -> None:
        log.warning(f"[WS] Closed code={code}, reason={reason}")

    def _on_error(self, ws: Any, code: Any, reason: Any) -> None:
        log.warning(f"[WS] Error code={code}, reason={reason}")

    def _on_reconnect(self, ws: Any, attempts_count: int) -> None:
        log.warning(f"[WS] Reconnecting; attempt={attempts_count}")

    def _on_noreconnect(self, ws: Any) -> None:
        log.error("[WS] Reconnection budget exhausted.")

    def start(self, timeout: float = 15.0) -> None:
        log.info("[WS] Starting KiteTicker ...")
        self.ticker.connect(threaded=True)
        if not self._connected.wait(timeout):
            log.warning("[WS] Connection was not confirmed within the timeout.")

    def stop(self) -> None:
        try:
            self.ticker.close()
        except Exception:
            pass

    def subscribe(self, tokens: Iterable[int]) -> None:
        clean = [int(token) for token in tokens]
        self._subscribed.update(clean)
        try:
            self.ticker.subscribe(clean)
            self.ticker.set_mode(self.ticker.MODE_LTP, clean)
            log.info(f"[WS] Subscribed: {clean}")
        except Exception as exc:
            log.warning(f"[WS] Subscribe failed for {clean}: {exc}")

    def unsubscribe(self, tokens: Iterable[int]) -> None:
        clean = [int(token) for token in tokens]
        self._subscribed.difference_update(clean)
        try:
            self.ticker.unsubscribe(clean)
        except Exception as exc:
            log.warning(f"[WS] Unsubscribe failed for {clean}: {exc}")

    def get(self, token: int) -> Optional[float]:
        with self._lock:
            return self._ltp.get(int(token))

    def wait_for(self, tokens: Iterable[int], timeout: float = 10.0) -> bool:
        clean = [int(token) for token in tokens]
        deadline = time.time() + timeout
        while time.time() < deadline:
            if all(self.get(token) is not None for token in clean):
                return True
            time.sleep(0.10)
        return all(self.get(token) is not None for token in clean)


# =============================================================================
# 5. PAPER/LIVE BROKER
# =============================================================================
class FatalExecutionError(RuntimeError):
    """Execution uncertainty that must stop further automated trading."""


@dataclass
class LegOrderResult:
    symbol: str
    side: str
    requested_qty: int
    order_id: Optional[str] = None
    status: str = "UNKNOWN"
    filled_qty: int = 0
    average_price: float = 0.0
    confirmed: bool = False
    message: str = ""


class Broker:
    """Bounded and reconciled paper/live execution for both option legs.

    Entry safety invariant
    ----------------------
    A live entry is accepted only when BOTH conditions are true for CE and PE:
      1. broker order rows show a COMPLETE SELL fill for the intended quantity;
      2. broker net positions show exactly ``-qty`` in each symbol.

    A failed cycle is always cleaned back to a broker-verified flat state before
    another cycle can place an order. This prevents duplicate exposure after a
    lost/ambiguous ``place_order`` response.
    """

    MARKET_PROTECTION = -1
    OPEN_ORDER_STATUSES = {
        "OPEN",
        "TRIGGER PENDING",
        "OPEN PENDING",
        "MODIFY PENDING",
        "VALIDATION PENDING",
        "AMO REQ RECEIVED",
        "PUT ORDER REQ RECEIVED",
    }
    FAILURE_ORDER_STATUSES = {"REJECTED", "CANCELLED"}

    def __init__(self, kite: Any, feed: PriceFeed, exchange: str, paper: bool):
        self.kite = kite
        self.feed = feed
        self.exchange = exchange
        self.paper = bool(paper)

    # ------------------------------------------------------------------
    # Generic bounded broker helpers
    # ------------------------------------------------------------------
    def _product(self) -> str:
        if ORDER_PRODUCT == "MIS":
            return self.kite.PRODUCT_MIS
        if ORDER_PRODUCT == "NRML":
            return self.kite.PRODUCT_NRML
        raise RuntimeError(f"Unsupported ORDER_PRODUCT={ORDER_PRODUCT}; use NRML or MIS.")

    def _strategy_exit_deadline(self) -> datetime:
        return combine_ist(today_ist(), EXIT_TIME)

    def _bounded_deadline(self, seconds: float, *, respect_exit: bool) -> datetime:
        deadline = now_ist() + timedelta(seconds=max(0.1, float(seconds)))
        return min(deadline, self._strategy_exit_deadline()) if respect_exit else deadline

    def _paper_fill(self, token: int, side: str) -> float:
        ltp = self.feed.get(token)
        if ltp is None:
            raise RuntimeError(f"No WebSocket tick for paper fill token={token}")
        slip = PAPER_SLIPPAGE_TICKS * OPTION_TICK
        price = ltp - slip if side == "SELL" else ltp + slip
        return round_to_tick(price)

    def _orders(self, desc: str, *, deadline: Optional[datetime] = None) -> list[dict[str, Any]]:
        rows = _api(
            self.kite.orders,
            desc=desc,
            max_retries=API_MAX_RETRIES,
            deadline=deadline,
        )
        return list(rows or [])

    def _positions(self, desc: str, *, deadline: Optional[datetime] = None) -> list[dict[str, Any]]:
        payload = _api(
            self.kite.positions,
            desc=desc,
            max_retries=API_MAX_RETRIES,
            deadline=deadline,
        )
        return list((payload or {}).get("net", []))

    @staticmethod
    def _order_filled_qty(order: dict[str, Any]) -> int:
        filled = order.get("filled_quantity")
        if filled is not None:
            return int(filled or 0)
        quantity = int(order.get("quantity") or 0)
        pending = int(order.get("pending_quantity") or 0)
        return max(0, quantity - pending)

    @staticmethod
    def _order_average(order: dict[str, Any]) -> float:
        return float(order.get("average_price") or 0.0)

    @staticmethod
    def _order_map(orders: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        return {
            str(order.get("order_id")): order
            for order in orders
            if order.get("order_id") is not None
        }

    @staticmethod
    def _position_map(positions: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        return {
            str(position.get("tradingsymbol")): position
            for position in positions
            if position.get("tradingsymbol")
        }

    @staticmethod
    def _position_qty(position_map: dict[str, dict[str, Any]], symbol: str) -> int:
        return int(position_map.get(symbol, {}).get("quantity") or 0)

    @staticmethod
    def _position_side_average(
        position_map: dict[str, dict[str, Any]], symbol: str, side: str
    ) -> float:
        row = position_map.get(symbol, {})
        if side == "SELL":
            qty = float(row.get("sell_quantity") or 0.0)
            value = float(row.get("sell_value") or 0.0)
            fallback = float(row.get("sell_price") or row.get("average_price") or 0.0)
        else:
            qty = float(row.get("buy_quantity") or 0.0)
            value = float(row.get("buy_value") or 0.0)
            fallback = float(row.get("buy_price") or row.get("average_price") or 0.0)
        return value / qty if qty > 0 and value > 0 else fallback

    def _marketable_limit_price(self, symbol: str, transaction_type: str) -> float:
        deadline = self._bounded_deadline(ORDER_CONFIRM_TIMEOUT_SECONDS, respect_exit=True)
        key = f"{self.exchange}:{symbol}"
        quote = _api(
            self.kite.quote,
            key,
            desc=f"quote {symbol}",
            max_retries=API_MAX_RETRIES,
            deadline=deadline,
        )[key]
        depth = quote.get("depth", {})
        buy_depth = depth.get("buy", [])
        sell_depth = depth.get("sell", [])
        ltp = float(quote.get("last_price") or 0.0)
        if ltp <= 0:
            raise RuntimeError(f"Invalid quote/LTP for {symbol}: {quote}")
        if transaction_type == self.kite.TRANSACTION_TYPE_SELL:
            price = (
                float(buy_depth[0]["price"])
                if buy_depth and buy_depth[0].get("price")
                else ltp * 0.995
            )
        else:
            price = (
                float(sell_depth[0]["price"])
                if sell_depth and sell_depth[0].get("price")
                else ltp * 1.005
            )
        return round_to_tick(price)

    def _matching_new_orders(
        self,
        orders: Iterable[dict[str, Any]],
        before_ids: set[str],
        *,
        symbol: str,
        side: str,
        qty: int,
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for order in orders:
            order_id = str(order.get("order_id") or "")
            if not order_id or order_id in before_ids:
                continue
            if order.get("tag") != oUtils.SS_ORDER_TAG:
                continue
            if str(order.get("tradingsymbol", "")) != symbol:
                continue
            if str(order.get("transaction_type", "")).upper() != side:
                continue
            if int(order.get("quantity") or 0) != int(qty):
                continue
            out.append(order)
        return out

    def _submit_once_reconciled(
        self,
        *,
        symbol: str,
        side: str,
        qty: int,
        order_type: str,
        price: Optional[float],
        respect_exit: bool,
        context: str,
    ) -> Optional[str]:
        """Submit exactly once; reconcile an ambiguous response before returning.

        No caller may resubmit this logical order until this method has either
        found the broker order ID or proved, within a bounded reconciliation
        window, that no matching new order exists.
        """
        if qty <= 0:
            raise ValueError(f"{context}: quantity must be positive; got {qty}")
        if respect_exit and now_ist().time() >= EXIT_TIME:
            log.warning(f"[{context}] Exit cutoff reached before {side} {symbol}.")
            return None

        reconcile_deadline = self._bounded_deadline(
            AMBIGUOUS_ORDER_RECONCILE_SECONDS, respect_exit=respect_exit
        )
        before_orders = self._orders(
            f"orders baseline {context} {symbol}", deadline=reconcile_deadline
        )
        before_ids = set(self._order_map(before_orders))
        transaction = (
            self.kite.TRANSACTION_TYPE_SELL
            if side == "SELL"
            else self.kite.TRANSACTION_TYPE_BUY
        )
        kwargs: dict[str, Any] = {
            "tradingsymbol": symbol,
            "variety": self.kite.VARIETY_REGULAR,
            "exchange": self.exchange,
            "transaction_type": transaction,
            "quantity": int(qty),
            "order_type": order_type,
            "product": self._product(),
            "tag": oUtils.SS_ORDER_TAG,
        }
        if order_type == self.kite.ORDER_TYPE_LIMIT:
            if price is None:
                raise ValueError(f"{context}: LIMIT order requires price")
            kwargs["price"] = float(price)
        else:
            kwargs["market_protection"] = self.MARKET_PROTECTION

        log.info(
            f"[{context}] SUBMIT ONCE: {side} {symbol}, qty={qty}, "
            f"type={order_type}{', price=' + str(price) if price is not None else ''}"
        )
        try:
            order_id = _api(
                self.kite.place_order,
                desc=f"{context} place {side} {symbol}",
                max_retries=1,
                deadline=reconcile_deadline,
                **kwargs,
            )
            order_id = str(order_id)
            log.info(f"[{context}] Broker acknowledged {side} {symbol}; order_id={order_id}")
            return order_id
        except Exception as exc:
            log.warning(
                f"[{context}] Ambiguous/failed place response for {side} {symbol}: {exc}. "
                "Reconciling broker order book before any retry."
            )

        poll_no = 0
        while now_ist() < reconcile_deadline:
            poll_no += 1
            try:
                orders = self._orders(
                    f"orders reconcile {context} {symbol}", deadline=reconcile_deadline
                )
            except Exception as exc:
                log.warning(f"[{context}] Reconcile poll {poll_no} failed for {symbol}: {exc}")
                orders = []
            candidates = self._matching_new_orders(
                orders, before_ids, symbol=symbol, side=side, qty=qty
            )
            if len(candidates) == 1:
                order_id = str(candidates[0]["order_id"])
                log.warning(
                    f"[{context}] Recovered ambiguous submission from order book: "
                    f"{side} {symbol}, order_id={order_id}, "
                    f"status={candidates[0].get('status')}"
                )
                return order_id
            if len(candidates) > 1:
                ids = [str(row.get("order_id")) for row in candidates]
                log.critical(
                    f"[{context}] Multiple matching new orders found for {side} {symbol}: {ids}."
                )
                raise FatalExecutionError(
                    f"Duplicate-order ambiguity for {symbol}; matching order IDs={ids}"
                )
            remaining = (reconcile_deadline - now_ist()).total_seconds()
            if remaining > 0:
                time.sleep(min(ORDER_STATUS_POLL_SECONDS, remaining))

        log.error(
            f"[{context}] No matching broker order discovered for {side} {symbol} "
            "within the reconciliation timeout."
        )
        return None

    def _cancel_order_and_confirm(
        self,
        order: dict[str, Any],
        *,
        context: str,
        respect_exit: bool = False,
    ) -> bool:
        order_id = str(order.get("order_id") or "")
        symbol = str(order.get("tradingsymbol") or "")
        if not order_id:
            return True
        deadline = self._bounded_deadline(ORDER_CONFIRM_TIMEOUT_SECONDS, respect_exit=respect_exit)
        try:
            _api(
                self.kite.cancel_order,
                variety=self.kite.VARIETY_REGULAR,
                order_id=order_id,
                desc=f"{context} cancel {symbol} {order_id}",
                max_retries=API_ORDER_MAX_RETRIES,
                deadline=deadline,
            )
            log.warning(f"[{context}] Cancel requested: {symbol}, order_id={order_id}")
        except Exception as exc:
            log.warning(
                f"[{context}] Cancel request uncertain for {symbol}, order_id={order_id}: {exc}"
            )

        while now_ist() < deadline:
            try:
                row = self._order_map(
                    self._orders(f"orders verify cancel {order_id}", deadline=deadline)
                ).get(order_id)
            except Exception as exc:
                log.warning(f"[{context}] Cancel verification failed for {order_id}: {exc}")
                row = None
            if row is None:
                time.sleep(min(ORDER_STATUS_POLL_SECONDS, max(0.05, (deadline-now_ist()).total_seconds())))
                continue
            status = str(row.get("status", "")).upper()
            if status not in self.OPEN_ORDER_STATUSES:
                log.info(
                    f"[{context}] Cancel/final status confirmed: {symbol}, "
                    f"order_id={order_id}, status={status}"
                )
                return True
            time.sleep(min(ORDER_STATUS_POLL_SECONDS, max(0.05, (deadline-now_ist()).total_seconds())))
        log.error(f"[{context}] Could not confirm cancellation/final state for order_id={order_id}")
        return False

    def _wait_for_order_fill(
        self,
        order_id: str,
        *,
        symbol: str,
        side: str,
        qty: int,
        allow_market_modify: bool,
        respect_exit: bool,
        context: str,
    ) -> LegOrderResult:
        deadline = self._bounded_deadline(ORDER_CONFIRM_TIMEOUT_SECONDS, respect_exit=respect_exit)
        max_polls = max(1, ORDER_STATUS_MAX_POLLS)
        market_modified = False
        last_status = "UNKNOWN"
        last_row: Optional[dict[str, Any]] = None

        for poll in range(1, max_polls + 1):
            if now_ist() >= deadline:
                break
            orders = self._orders(
                f"orders confirm {context} {symbol}", deadline=deadline
            )
            row = self._order_map(orders).get(str(order_id))
            if row is None:
                log.warning(
                    f"[{context}] Confirmation poll {poll}/{max_polls}: "
                    f"order_id={order_id} not yet visible."
                )
            else:
                last_row = row
                status = str(row.get("status", "")).upper()
                pending = int(row.get("pending_quantity") or 0)
                filled = self._order_filled_qty(row)
                average = self._order_average(row)
                if status != last_status:
                    log.info(
                        f"[{context}] {symbol} order_id={order_id}: status={status}, "
                        f"filled={filled}/{qty}, pending={pending}, avg={average:.2f}"
                    )
                    last_status = status
                if status == "COMPLETE" and pending == 0 and filled == qty:
                    return LegOrderResult(
                        symbol=symbol,
                        side=side,
                        requested_qty=qty,
                        order_id=order_id,
                        status=status,
                        filled_qty=filled,
                        average_price=average,
                        confirmed=True,
                        message="Complete fill confirmed from order book",
                    )
                if status in self.FAILURE_ORDER_STATUSES:
                    return LegOrderResult(
                        symbol=symbol,
                        side=side,
                        requested_qty=qty,
                        order_id=order_id,
                        status=status,
                        filled_qty=filled,
                        average_price=average,
                        confirmed=False,
                        message=f"Order ended with {status}",
                    )
                if pending > 0 and allow_market_modify and not market_modified:
                    try:
                        _api(
                            self.kite.modify_order,
                            variety=self.kite.VARIETY_REGULAR,
                            order_id=order_id,
                            order_type=self.kite.ORDER_TYPE_MARKET,
                            market_protection=self.MARKET_PROTECTION,
                            desc=f"{context} modify {symbol} to MARKET",
                            max_retries=API_ORDER_MAX_RETRIES,
                            deadline=deadline,
                        )
                        market_modified = True
                        log.warning(
                            f"[{context}] Pending/partial {symbol} order_id={order_id} "
                            "converted once to MARKET."
                        )
                    except Exception as exc:
                        log.warning(
                            f"[{context}] MARKET modification failed/ambiguous for "
                            f"{symbol} order_id={order_id}: {exc}"
                        )
            remaining = (deadline - now_ist()).total_seconds()
            if remaining > 0:
                time.sleep(min(ORDER_STATUS_POLL_SECONDS, remaining))

        if last_row is not None and str(last_row.get("status", "")).upper() in self.OPEN_ORDER_STATUSES:
            self._cancel_order_and_confirm(last_row, context=context, respect_exit=False)
        filled = self._order_filled_qty(last_row or {})
        average = self._order_average(last_row or {})
        status = str((last_row or {}).get("status", "TIMEOUT")).upper()
        log.error(
            f"[{context}] Fill confirmation timeout: {symbol}, order_id={order_id}, "
            f"status={status}, filled={filled}/{qty}."
        )
        return LegOrderResult(
            symbol=symbol,
            side=side,
            requested_qty=qty,
            order_id=order_id,
            status=status,
            filled_qty=filled,
            average_price=average,
            confirmed=False,
            message="Fill confirmation timeout or partial execution",
        )

    def _cancel_pending_for_symbols(self, symbols: set[str], *, context: str) -> bool:
        all_clear = True
        # Two bounded scans catch orders whose cancellation/update appears after
        # the first order-book refresh.
        for scan in range(1, 3):
            deadline = self._bounded_deadline(ORDER_CONFIRM_TIMEOUT_SECONDS, respect_exit=False)
            orders = self._orders(f"orders pending scan {context} #{scan}", deadline=deadline)
            pending = [
                order
                for order in orders
                if order.get("tag") == oUtils.SS_ORDER_TAG
                and str(order.get("tradingsymbol", "")) in symbols
                and str(order.get("status", "")).upper() in self.OPEN_ORDER_STATUSES
            ]
            if not pending:
                log.info(f"[{context}] Pending-order scan #{scan}: clear for {sorted(symbols)}")
                return all_clear
            log.warning(
                f"[{context}] Pending-order scan #{scan}: cancelling {len(pending)} order(s)."
            )
            for order in pending:
                all_clear = self._cancel_order_and_confirm(order, context=context) and all_clear
        final_orders = self._orders(f"orders final pending verify {context}")
        residual = [
            order
            for order in final_orders
            if order.get("tag") == oUtils.SS_ORDER_TAG
            and str(order.get("tradingsymbol", "")) in symbols
            and str(order.get("status", "")).upper() in self.OPEN_ORDER_STATUSES
        ]
        if residual:
            log.error(
                f"[{context}] Pending orders remain after bounded cancellation: "
                f"{[row.get('order_id') for row in residual]}"
            )
            return False
        return all_clear

    def _positions_match(
        self, expected: dict[str, int], *, context: str
    ) -> tuple[bool, dict[str, dict[str, Any]]]:
        positions = self._positions(f"positions verify {context}")
        position_map = self._position_map(positions)
        actual = {symbol: self._position_qty(position_map, symbol) for symbol in expected}
        ok = all(actual[symbol] == qty for symbol, qty in expected.items())
        log.info(f"[{context}] Position verification: expected={expected}, actual={actual}, ok={ok}")
        return ok, position_map

    def _verify_flat_stable(self, symbols: set[str], *, context: str) -> bool:
        expected = {symbol: 0 for symbol in symbols}
        first_ok, _ = self._positions_match(expected, context=f"{context} flat check 1")
        if not first_ok:
            return False
        time.sleep(max(0.0, CLEANUP_VERIFY_DELAY_SECONDS))
        second_ok, _ = self._positions_match(expected, context=f"{context} flat check 2")
        return second_ok

    def _cleanup_to_flat(
        self, symbols: set[str], *, reason: str
    ) -> tuple[bool, dict[str, list[str]]]:
        """Cancel strategy orders, flatten every residual quantity, verify flat.

        Returns ``(flat_confirmed, action_order_ids_by_symbol)``. Every loop is
        bounded by ``CLEANUP_MAX_ATTEMPTS`` and order confirmation timeouts.
        """
        action_ids: dict[str, list[str]] = {symbol: [] for symbol in symbols}
        log.warning(f"[CLEANUP] START reason={reason}, symbols={sorted(symbols)}")
        for cleanup_attempt in range(1, CLEANUP_MAX_ATTEMPTS + 1):
            log.warning(
                f"[CLEANUP] Pass {cleanup_attempt}/{CLEANUP_MAX_ATTEMPTS}: "
                f"reason={reason}"
            )
            self._cancel_pending_for_symbols(symbols, context=f"CLEANUP-{cleanup_attempt}")
            positions = self._positions(f"positions cleanup pass {cleanup_attempt}")
            position_map = self._position_map(positions)
            residual = {
                symbol: self._position_qty(position_map, symbol)
                for symbol in symbols
                if self._position_qty(position_map, symbol) != 0
            }
            if not residual:
                if self._verify_flat_stable(symbols, context=f"CLEANUP-{cleanup_attempt}"):
                    log.warning(f"[CLEANUP] SUCCESS: verified flat for {sorted(symbols)}")
                    return True, action_ids
            else:
                log.error(f"[CLEANUP] Residual positions detected: {residual}")

            for symbol, net_qty in residual.items():
                side = "BUY" if net_qty < 0 else "SELL"
                qty = abs(net_qty)
                context = f"CLEANUP-{cleanup_attempt}"
                order_id = self._submit_once_reconciled(
                    symbol=symbol,
                    side=side,
                    qty=qty,
                    order_type=self.kite.ORDER_TYPE_MARKET,
                    price=None,
                    respect_exit=False,
                    context=context,
                )
                if order_id is None:
                    log.error(
                        f"[{context}] No order ID confirmed for flattening {symbol}; "
                        "next cleanup pass will reconcile positions again."
                    )
                    continue
                action_ids[symbol].append(order_id)
                result = self._wait_for_order_fill(
                    order_id,
                    symbol=symbol,
                    side=side,
                    qty=qty,
                    allow_market_modify=False,
                    respect_exit=False,
                    context=context,
                )
                log.warning(
                    f"[{context}] Flatten result {symbol}: confirmed={result.confirmed}, "
                    f"filled={result.filled_qty}/{qty}, status={result.status}"
                )

            time.sleep(max(0.0, CLEANUP_VERIFY_DELAY_SECONDS))

        self._cancel_pending_for_symbols(symbols, context="CLEANUP-FINAL")
        if self._verify_flat_stable(symbols, context="CLEANUP-FINAL"):
            log.warning(f"[CLEANUP] FINAL SUCCESS: verified flat for {sorted(symbols)}")
            return True, action_ids
        log.critical(
            f"[CLEANUP] FATAL: residual position or pending-order uncertainty remains "
            f"after {CLEANUP_MAX_ATTEMPTS} pass(es) for {sorted(symbols)}."
        )
        return False, action_ids

    def _verify_entry_pair(
        self,
        *,
        pe_symbol: str,
        ce_symbol: str,
        pe_order_id: str,
        ce_order_id: str,
        qty: int,
        context: str,
    ) -> tuple[bool, float, float, str]:
        symbols = {pe_symbol, ce_symbol}
        if not self._cancel_pending_for_symbols(symbols, context=f"{context}-PENDING-CHECK"):
            return False, 0.0, 0.0, "Pending entry order could not be cleared/confirmed"

        orders = self._orders(f"orders final entry verify {context}")
        order_map = self._order_map(orders)
        pe_order = order_map.get(str(pe_order_id))
        ce_order = order_map.get(str(ce_order_id))
        failures: list[str] = []
        for leg_name, symbol, order in (
            ("PE", pe_symbol, pe_order),
            ("CE", ce_symbol, ce_order),
        ):
            if order is None:
                failures.append(f"{leg_name} order missing")
                continue
            status = str(order.get("status", "")).upper()
            filled = self._order_filled_qty(order)
            pending = int(order.get("pending_quantity") or 0)
            if status != "COMPLETE" or filled != qty or pending != 0:
                failures.append(
                    f"{leg_name} order status={status}, filled={filled}/{qty}, pending={pending}"
                )

        position_ok, position_map = self._positions_match(
            {pe_symbol: -qty, ce_symbol: -qty}, context=f"{context}-ENTRY"
        )
        if not position_ok:
            failures.append("Broker positions do not show exact intended short quantities")

        if failures:
            return False, 0.0, 0.0, "; ".join(failures)

        pe_fill = self._order_average(pe_order or {}) or self._position_side_average(
            position_map, pe_symbol, "SELL"
        )
        ce_fill = self._order_average(ce_order or {}) or self._position_side_average(
            position_map, ce_symbol, "SELL"
        )
        if pe_fill <= 0 or ce_fill <= 0:
            return False, 0.0, 0.0, "Confirmed quantities but invalid average fill price"
        return True, pe_fill, ce_fill, "Both order and position confirmations passed"

    @staticmethod
    def _weighted_average_from_order_ids(
        orders: list[dict[str, Any]], order_ids: Iterable[str], side: str
    ) -> float:
        wanted = {str(order_id) for order_id in order_ids}
        total_qty = 0
        total_value = 0.0
        for order in orders:
            if str(order.get("order_id")) not in wanted:
                continue
            if str(order.get("transaction_type", "")).upper() != side:
                continue
            filled = Broker._order_filled_qty(order)
            average = Broker._order_average(order)
            if filled > 0 and average > 0:
                total_qty += filled
                total_value += filled * average
        return total_value / total_qty if total_qty > 0 else 0.0

    # ------------------------------------------------------------------
    # Public execution API
    # ------------------------------------------------------------------
    def open_short_straddle(
        self, pe_symbol: str, ce_symbol: str, pe_token: int, ce_token: int, qty: int
    ) -> dict[str, Any]:
        if self.paper:
            pe_fill = self._paper_fill(pe_token, "SELL")
            ce_fill = self._paper_fill(ce_token, "SELL")
            log.info(
                f"[PAPER ENTRY] VERIFIED SIMULATION: SELL {pe_symbol}@{pe_fill} | "
                f"SELL {ce_symbol}@{ce_fill} | qty={qty}"
            )
            return {
                "pe_fill": pe_fill,
                "ce_fill": ce_fill,
                "ok": True,
                "cycles_used": 1,
                "reason": "Paper fills simulated",
            }

        symbols = {pe_symbol, ce_symbol}
        for cycle in range(1, ENTRY_EXECUTION_MAX_ATTEMPTS + 1):
            if now_ist().time() >= EXIT_TIME:
                log.error(
                    f"[ENTRY EXEC] Exit cutoff reached before cycle "
                    f"{cycle}/{ENTRY_EXECUTION_MAX_ATTEMPTS}."
                )
                break
            context = f"ENTRY-CYCLE-{cycle}"
            log.info(
                f"[{context}] START: intended short qty={qty} on "
                f"CE={ce_symbol}, PE={pe_symbol}"
            )

            # Every cycle starts from a proven-flat state. If the previous cycle
            # left anything uncertain, cleanup must succeed before resubmission.
            if not self._verify_flat_stable(symbols, context=f"{context}-PRECHECK"):
                flat, _ = self._cleanup_to_flat(symbols, reason=f"{context} pre-entry not flat")
                if not flat:
                    raise FatalExecutionError(
                        f"Cannot establish flat pre-entry state for {sorted(symbols)}"
                    )

            pe_price = self._marketable_limit_price(
                pe_symbol, self.kite.TRANSACTION_TYPE_SELL
            )
            pe_id = self._submit_once_reconciled(
                symbol=pe_symbol,
                side="SELL",
                qty=qty,
                order_type=self.kite.ORDER_TYPE_LIMIT,
                price=pe_price,
                respect_exit=True,
                context=context,
            )
            if pe_id is None:
                log.error(f"[{context}] PE submission could not be confirmed; cleaning up.")
                flat, _ = self._cleanup_to_flat(symbols, reason=f"{context} PE unconfirmed")
                if not flat:
                    raise FatalExecutionError("Residual position after unconfirmed PE entry")
                if cycle < ENTRY_EXECUTION_MAX_ATTEMPTS:
                    time.sleep(ENTRY_EXECUTION_RETRY_DELAY_SECONDS)
                continue

            ce_price = self._marketable_limit_price(
                ce_symbol, self.kite.TRANSACTION_TYPE_SELL
            )
            ce_id = self._submit_once_reconciled(
                symbol=ce_symbol,
                side="SELL",
                qty=qty,
                order_type=self.kite.ORDER_TYPE_LIMIT,
                price=ce_price,
                respect_exit=True,
                context=context,
            )
            if ce_id is None:
                log.error(f"[{context}] CE submission could not be confirmed; cleaning up.")
                flat, _ = self._cleanup_to_flat(symbols, reason=f"{context} CE unconfirmed")
                if not flat:
                    raise FatalExecutionError("Residual position after unconfirmed CE entry")
                if cycle < ENTRY_EXECUTION_MAX_ATTEMPTS:
                    time.sleep(ENTRY_EXECUTION_RETRY_DELAY_SECONDS)
                continue

            pe_result = self._wait_for_order_fill(
                pe_id,
                symbol=pe_symbol,
                side="SELL",
                qty=qty,
                allow_market_modify=True,
                respect_exit=True,
                context=context,
            )
            ce_result = self._wait_for_order_fill(
                ce_id,
                symbol=ce_symbol,
                side="SELL",
                qty=qty,
                allow_market_modify=True,
                respect_exit=True,
                context=context,
            )
            log.info(
                f"[{context}] Leg results: PE confirmed={pe_result.confirmed} "
                f"filled={pe_result.filled_qty}/{qty}; CE confirmed={ce_result.confirmed} "
                f"filled={ce_result.filled_qty}/{qty}."
            )

            verified, pe_fill, ce_fill, detail = self._verify_entry_pair(
                pe_symbol=pe_symbol,
                ce_symbol=ce_symbol,
                pe_order_id=pe_id,
                ce_order_id=ce_id,
                qty=qty,
                context=context,
            )
            if verified:
                log.info(
                    f"[{context}] SUCCESS: both legs fully filled and position-verified. "
                    f"PE={pe_fill:.2f}, CE={ce_fill:.2f}, qty={qty}."
                )
                return {
                    "pe_fill": pe_fill,
                    "ce_fill": ce_fill,
                    "ok": True,
                    "cycles_used": cycle,
                    "reason": detail,
                    "pe_order_id": pe_id,
                    "ce_order_id": ce_id,
                }

            log.error(f"[{context}] FAILED VERIFICATION: {detail}")
            flat, _ = self._cleanup_to_flat(
                symbols, reason=f"{context} verification failure"
            )
            if not flat:
                raise FatalExecutionError(
                    f"Entry failed and cleanup could not verify flat for {sorted(symbols)}"
                )
            if cycle < ENTRY_EXECUTION_MAX_ATTEMPTS and now_ist().time() < EXIT_TIME:
                log.warning(
                    f"[{context}] Retry permitted only after verified-flat cleanup; "
                    f"sleeping {ENTRY_EXECUTION_RETRY_DELAY_SECONDS:.1f}s."
                )
                time.sleep(ENTRY_EXECUTION_RETRY_DELAY_SECONDS)

        flat, _ = self._cleanup_to_flat(symbols, reason="ENTRY FINAL FAILURE")
        if not flat:
            raise FatalExecutionError(
                f"Final entry cleanup could not verify flat for {sorted(symbols)}"
            )
        log.error(
            f"[ENTRY EXEC] FINAL FAILURE after {ENTRY_EXECUTION_MAX_ATTEMPTS} bounded "
            "cycle(s); no position remains."
        )
        return {
            "pe_fill": 0.0,
            "ce_fill": 0.0,
            "ok": False,
            "cycles_used": ENTRY_EXECUTION_MAX_ATTEMPTS,
            "reason": "Entry cycles exhausted; verified flat",
        }

    def square_off_symbol(self, symbol: str) -> bool:
        """Bounded emergency flatten for one symbol, with flat verification."""
        flat, _ = self._cleanup_to_flat({symbol}, reason=f"square_off_symbol {symbol}")
        return flat

    def close_short_straddle(
        self, pe_symbol: str, ce_symbol: str, pe_token: int, ce_token: int, qty: int
    ) -> dict[str, Any]:
        if self.paper:
            pe_fill = self._paper_fill(pe_token, "BUY")
            ce_fill = self._paper_fill(ce_token, "BUY")
            log.info(
                f"[PAPER EXIT] BUY {pe_symbol}@{pe_fill} | "
                f"BUY {ce_symbol}@{ce_fill} | qty={qty}"
            )
            return {"pe_fill": pe_fill, "ce_fill": ce_fill, "ok": True}

        symbols = {pe_symbol, ce_symbol}
        flat, action_ids = self._cleanup_to_flat(symbols, reason="STRATEGY EXIT")
        if not flat:
            raise FatalExecutionError(
                f"Exit cleanup could not verify flat for {sorted(symbols)}"
            )

        orders = self._orders("orders calculate exit fills")
        pe_fill = self._weighted_average_from_order_ids(
            orders, action_ids.get(pe_symbol, []), "BUY"
        )
        ce_fill = self._weighted_average_from_order_ids(
            orders, action_ids.get(ce_symbol, []), "BUY"
        )
        # A broker/manual action can make the book flat before our cleanup order
        # is needed. Use the latest observed LTP solely for P&L accounting in that
        # rare case; flatness itself has already been confirmed from positions.
        if pe_fill <= 0:
            pe_fill = float(self.feed.get(pe_token) or 0.0)
            log.warning(f"[EXIT] PE broker fill unavailable; accounting fallback LTP={pe_fill:.2f}")
        if ce_fill <= 0:
            ce_fill = float(self.feed.get(ce_token) or 0.0)
            log.warning(f"[EXIT] CE broker fill unavailable; accounting fallback LTP={ce_fill:.2f}")
        if pe_fill <= 0 or ce_fill <= 0:
            raise FatalExecutionError(
                "Positions are flat but exit fill prices could not be established safely."
            )
        log.info(
            f"[EXIT EXEC] SUCCESS: broker positions verified flat; "
            f"PE exit={pe_fill:.2f}, CE exit={ce_fill:.2f}."
        )
        return {"pe_fill": pe_fill, "ce_fill": ce_fill, "ok": True}

# =============================================================================
# 6. V3OPT LIVE STRATEGY ENGINE
# =============================================================================
class LiveStraddleTrader:
    def __init__(
        self,
        kite: Any,
        feed: PriceFeed,
        broker: Broker,
        selection: oUtils.InstrumentSelection,
        profile: StrategyProfile,
    ):
        self.kite = kite
        self.feed = feed
        self.broker = broker
        self.selection = selection
        self.profile = profile

        self.underlying_name = selection.underlying_name
        self.underlying_quote_key = selection.underlying_quote_key
        self.part_symbol = selection.part_symbol.replace(":", "")
        self.strike_step = int(selection.strike_multiple)
        self.qty = int(selection.quantity_units)
        self.expiry_date = selection.expiry_date

        self.entry_time = ENTRY_TIME
        self.exit_time = EXIT_TIME
        self.attempt_idx = 0
        self.daily_realized_pnl = 0.0  # estimated net P&L
        self.phase = "WAITING_ENTRY"
        self.position: Optional[dict[str, Any]] = None
        self.reentry_target: Optional[datetime] = None
        self.pending_entry_kind = "FIRST"
        self._state_loaded_today = False
        self._option_token_cache: dict[str, int] = {}

    # ------------------------------------------------------------------
    # Basic market data and symbol resolution
    # ------------------------------------------------------------------
    def _underlying_ltp(self) -> float:
        data = _api(
            self.kite.ltp,
            [self.underlying_quote_key],
            desc=f"underlying LTP {self.underlying_name}",
        )[self.underlying_quote_key]
        return float(data["last_price"])

    def _symbols_for_atm(self, atm: int) -> tuple[str, str]:
        return f"{self.part_symbol}{atm}PE", f"{self.part_symbol}{atm}CE"

    def _resolve_option_snapshot(self, symbol: str) -> tuple[int, float]:
        key = f"{self.selection.options_exchange}:{symbol}"
        data = _api(self.kite.ltp, [key], desc=f"option LTP {symbol}")[key]
        token = int(data["instrument_token"])
        self._option_token_cache[symbol] = token
        return token, float(data["last_price"])

    def _resolve_option_pair_snapshot(
        self, pe_symbol: str, ce_symbol: str
    ) -> tuple[int, float, int, float]:
        """Resolve both option legs in one REST request."""
        pe_key = f"{self.selection.options_exchange}:{pe_symbol}"
        ce_key = f"{self.selection.options_exchange}:{ce_symbol}"
        data = _api(
            self.kite.ltp,
            [pe_key, ce_key],
            desc=f"option LTP pair {pe_symbol}/{ce_symbol}",
        )
        pe_row, ce_row = data[pe_key], data[ce_key]
        pe_token, ce_token = int(pe_row["instrument_token"]), int(ce_row["instrument_token"])
        self._option_token_cache[pe_symbol] = pe_token
        self._option_token_cache[ce_symbol] = ce_token
        return pe_token, float(pe_row["last_price"]), ce_token, float(ce_row["last_price"])

    def _historical_close_at(self, token: int, target: datetime) -> Optional[float]:
        """Return the latest one-minute close at or before ``target``."""
        start = combine_ist(target.date(), SESSION_START_IST)
        end = min(now_ist(), combine_ist(target.date(), SESSION_END_IST))
        if end < start:
            return None
        rows = _api(
            self.kite.historical_data,
            instrument_token=int(token),
            from_date=start,
            to_date=end,
            interval="minute",
            continuous=False,
            oi=False,
            desc=f"historical minute token={token}",
        )
        best_time: Optional[datetime] = None
        best_close: Optional[float] = None
        target_minute = target.replace(second=0, microsecond=0)
        for row in rows:
            raw_dt = row.get("date")
            if raw_dt is None:
                continue
            if isinstance(raw_dt, str):
                candle_dt = datetime.fromisoformat(raw_dt.replace("Z", "+00:00"))
            else:
                candle_dt = raw_dt
            if candle_dt.tzinfo is None:
                candle_dt = candle_dt.replace(tzinfo=IST)
            else:
                candle_dt = candle_dt.astimezone(IST)
            candle_dt = candle_dt.replace(second=0, microsecond=0)
            if candle_dt <= target_minute and (best_time is None or candle_dt > best_time):
                best_time = candle_dt
                best_close = float(row["close"])
        return best_close

    # ------------------------------------------------------------------
    # Momentum gate
    # ------------------------------------------------------------------
    def _momentum_gate_snapshot(self) -> tuple[bool, str]:
        now = now_ist()
        underlying = self._underlying_ltp()
        atm = round_to_step(underlying, self.strike_step)
        pe_symbol, ce_symbol = self._symbols_for_atm(atm)
        pe_token, pe_now, ce_token, ce_now = self._resolve_option_pair_snapshot(
            pe_symbol, ce_symbol
        )

        session_start = combine_ist(now.date(), SESSION_START_IST)
        back_time = now - timedelta(minutes=MOMENTUM_LOOKBACK_MIN)
        if back_time < session_start:
            back_time = session_start
        back_time = back_time.replace(second=0, microsecond=0)

        pe_before = self._historical_close_at(pe_token, back_time)
        ce_before = self._historical_close_at(ce_token, back_time)
        if pe_before is None or ce_before is None:
            return False, (
                f"ATM={atm}; historical premium unavailable at {back_time.strftime('%H:%M')}"
            )

        premium_now = pe_now + ce_now
        premium_before = pe_before + ce_before
        passed = premium_now < premium_before
        return passed, (
            f"ATM={atm}; premium now={premium_now:.2f}, "
            f"{back_time.strftime('%H:%M')}={premium_before:.2f}, "
            f"decay={'YES' if passed else 'no'}"
        )

    def wait_for_momentum_gate(self) -> bool:
        """Wait minute-by-minute for premium decay, or stop at exit time."""
        if not ENTRY_MOMENTUM_GATE:
            log.info("[MOMENTUM] Gate disabled; entry may proceed.")
            return now_ist().time() < self.exit_time

        self.phase = "WAITING_GATE"
        self._save_state()
        last_logged_minute: Optional[tuple[int, int]] = None
        while now_ist().time() < self.exit_time:
            try:
                passed, detail = self._momentum_gate_snapshot()
            except Exception as exc:
                log.warning(f"[MOMENTUM] Check failed: {exc}")
                time.sleep(MOMENTUM_RETRY_SECONDS)
                continue

            current = now_ist()
            minute_key = (current.hour, current.minute)
            if minute_key != last_logged_minute or passed:
                log.info(f"[MOMENTUM] {detail}")
                last_logged_minute = minute_key
            if passed:
                self.phase = "WAITING_ENTRY"
                self._save_state()
                return True

            # Backtester advances the candidate timestamp minute-by-minute.
            sleep_seconds = max(
                1.0,
                60.2 - current.second - current.microsecond / 1_000_000,
            )
            time.sleep(min(sleep_seconds, 60.0))

        log.info("[MOMENTUM] Exit cutoff reached before decay confirmation.")
        return False

    # ------------------------------------------------------------------
    # State persistence and startup reconciliation
    # ------------------------------------------------------------------
    def _state_identity(self) -> dict[str, str]:
        return {
            "date": today_ist().isoformat(),
            "underlying": self.underlying_name,
            "part_symbol": self.part_symbol,
            "mode": "PAPER" if self.broker.paper else "LIVE",
        }

    def _save_state(self) -> None:
        state = {
            **self._state_identity(),
            "phase": self.phase,
            "attempt_idx": self.attempt_idx,
            "daily_realized_pnl": self.daily_realized_pnl,
            "reentry_target": self.reentry_target.isoformat() if self.reentry_target else None,
            "pending_entry_kind": self.pending_entry_kind,
            "position": self.position,
            "expiry_date": self.expiry_date.isoformat(),
        }
        try:
            path = Path(STATE_FILE).expanduser()
            path.parent.mkdir(parents=True, exist_ok=True)
            temp = path.with_suffix(path.suffix + ".tmp")
            temp.write_text(json.dumps(state, indent=2, default=str), encoding="utf-8")
            os.replace(temp, path)
        except Exception as exc:
            log.warning(f"[STATE] Save failed: {exc}")

    def _load_state(self) -> None:
        self._state_loaded_today = False
        path = Path(STATE_FILE).expanduser()
        if not path.exists():
            return
        try:
            state = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            log.warning(f"[STATE] Read failed: {exc}")
            return

        identity = self._state_identity()
        for key, expected in identity.items():
            if state.get(key) != expected:
                log.info(
                    f"[STATE] Ignoring incompatible state ({key}={state.get(key)!r}, "
                    f"expected {expected!r})."
                )
                return

        self._state_loaded_today = True
        self.phase = str(state.get("phase", "WAITING_ENTRY"))
        self.attempt_idx = int(state.get("attempt_idx", 0))
        self.daily_realized_pnl = float(state.get("daily_realized_pnl", 0.0))
        self.pending_entry_kind = str(state.get("pending_entry_kind", "FIRST"))
        self.position = state.get("position")
        raw_target = state.get("reentry_target")
        self.reentry_target = datetime.fromisoformat(raw_target) if raw_target else None
        if self.reentry_target and self.reentry_target.tzinfo is None:
            self.reentry_target = self.reentry_target.replace(tzinfo=IST)
        log.info(
            f"[STATE] Loaded: phase={self.phase}, attempt={self.attempt_idx + 1}, "
            f"day_net=Rs{self.daily_realized_pnl:,.0f}, "
            f"position={'YES' if self.position else 'no'}"
        )

    def _is_strategy_symbol(self, symbol: str) -> bool:
        return bool(symbol) and symbol.startswith(self.part_symbol) and symbol.endswith(("CE", "PE"))

    def _open_strategy_legs(self, positions: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
        legs: dict[str, dict[str, float]] = {}
        for position in positions:
            symbol = str(position.get("tradingsymbol", ""))
            qty = int(position.get("quantity") or 0)
            if qty == 0 or not self._is_strategy_symbol(symbol):
                continue
            sell_qty = float(position.get("sell_quantity") or 0.0)
            sell_value = float(position.get("sell_value") or 0.0)
            sell_avg = (
                sell_value / sell_qty
                if sell_qty
                else float(position.get("sell_price") or 0.0)
            )
            legs[symbol] = {
                "qty": float(qty),
                "entry": sell_avg,
                "average": float(position.get("average_price") or 0.0),
            }
        return legs

    def _broker_realised_pnl(self, positions: list[dict[str, Any]]) -> float:
        return sum(
            float(position.get("realised") or 0.0)
            for position in positions
            if self._is_strategy_symbol(str(position.get("tradingsymbol", "")))
        )

    def _completed_entries(self, orders: list[dict[str, Any]]) -> int:
        count = 0
        for order in orders:
            if (
                order.get("tag") == oUtils.SS_ORDER_TAG
                and str(order.get("transaction_type", "")).upper() == "SELL"
                and str(order.get("tradingsymbol", "")).endswith("CE")
                and str(order.get("status", "")).upper() == "COMPLETE"
                and self._is_strategy_symbol(str(order.get("tradingsymbol", "")))
            ):
                count += 1
        return count

    def _cancel_pending_orders(self, orders: list[dict[str, Any]]) -> None:
        open_statuses = {
            "OPEN", "TRIGGER PENDING", "OPEN PENDING", "MODIFY PENDING",
            "VALIDATION PENDING", "AMO REQ RECEIVED", "PUT ORDER REQ RECEIVED",
        }
        for order in orders:
            symbol = str(order.get("tradingsymbol", ""))
            if order.get("tag") != oUtils.SS_ORDER_TAG or not self._is_strategy_symbol(symbol):
                continue
            if str(order.get("status", "")).upper() not in open_statuses:
                continue
            try:
                _api(
                    self.kite.cancel_order,
                    variety=self.kite.VARIETY_REGULAR,
                    order_id=order.get("order_id"),
                    desc=f"cancel stale {symbol}",
                    max_retries=API_ORDER_MAX_RETRIES,
                )
                log.warning(f"[RECONCILE] Cancelled pending order {symbol}.")
            except Exception as exc:
                log.warning(f"[RECONCILE] Could not cancel {symbol}: {exc}")

    def _effective_target_pct(self, entry_dt: datetime) -> float:
        if (
            self.profile.profit_target_pct_late > 0
            and entry_dt.time() >= self.profile.profit_target_late_from
        ):
            return self.profile.profit_target_pct_late
        return self.profile.profit_target_pct

    def _new_position_state(
        self,
        *,
        attempt_idx: int,
        pe_symbol: str,
        ce_symbol: str,
        pe_token: int,
        ce_token: int,
        pe_entry: float,
        ce_entry: float,
        qty: int,
        entry_dt: datetime,
    ) -> dict[str, Any]:
        premium_sum = (pe_entry + ce_entry) * qty
        target_pct = self._effective_target_pct(entry_dt)
        return {
            "attempt_idx": attempt_idx,
            "pe_sym": pe_symbol,
            "ce_sym": ce_symbol,
            "pe_tok": int(pe_token),
            "ce_tok": int(ce_token),
            "pe_entry": float(pe_entry),
            "ce_entry": float(ce_entry),
            "qty": int(qty),
            "entry_dt": entry_dt.isoformat(),
            "premium_sum": float(premium_sum),
            "stop_pct": float(stop_pct_for_attempt(attempt_idx)),
            "stop_rupees": float(effective_stop_rupees(attempt_idx, premium_sum)),
            "target_pct": float(target_pct),
            "target_rupees": float(target_pct * premium_sum) if target_pct > 0 else None,
            "protect_pct": float(self.profile.profit_protect_pct),
            "G": float(self.profile.profit_protect_pct * premium_sum)
            if self.profile.profit_protect_pct > 0
            else None,
            "late_giveback_pct": float(self.profile.late_giveback_pct),
            "late_giveback_rupees": float(self.profile.late_giveback_pct * premium_sum)
            if self.profile.late_giveback_pct > 0
            else None,
            "peak": 0.0,
            "armed": False,
        }

    def _adopt_open_straddle(
        self,
        ce_symbol: str,
        pe_symbol: str,
        open_legs: dict[str, dict[str, float]],
        completed_entries: int,
    ) -> None:
        saved = self.position
        if saved and saved.get("ce_sym") == ce_symbol and saved.get("pe_sym") == pe_symbol:
            log.warning("[RECONCILE] Reusing saved thresholds for broker-confirmed open straddle.")
            self.attempt_idx = int(saved.get("attempt_idx", self.attempt_idx))
            self.position = saved
        else:
            ce_leg, pe_leg = open_legs[ce_symbol], open_legs[pe_symbol]
            qty = int(abs(ce_leg["qty"]) or self.qty)
            ce_entry = float(ce_leg["entry"] or ce_leg["average"])
            pe_entry = float(pe_leg["entry"] or pe_leg["average"])
            ce_token, _ = self._resolve_option_snapshot(ce_symbol)
            pe_token, _ = self._resolve_option_snapshot(pe_symbol)
            self.attempt_idx = max(self.attempt_idx, max(0, completed_entries - 1))
            self.position = self._new_position_state(
                attempt_idx=self.attempt_idx,
                pe_symbol=pe_symbol,
                ce_symbol=ce_symbol,
                pe_token=pe_token,
                ce_token=ce_token,
                pe_entry=pe_entry,
                ce_entry=ce_entry,
                qty=qty,
                entry_dt=now_ist(),
            )
            log.warning(
                "[RECONCILE] Rebuilt thresholds from broker fills; trailing peak reset to zero."
            )
        self.phase = "IN_POSITION"

    def reconcile_on_startup(self) -> None:
        self._load_state()
        if self.broker.paper:
            log.info("[RECONCILE] PAPER mode: using state file only; no broker positions queried.")
            return

        orders = _api(self.kite.orders, desc="orders startup")
        self._cancel_pending_orders(orders)
        completed_entries = self._completed_entries(orders)

        positions = _api(self.kite.positions, desc="positions startup").get("net", [])
        open_legs = self._open_strategy_legs(positions)
        ce_symbols = sorted(symbol for symbol in open_legs if symbol.endswith("CE"))
        pe_symbols = sorted(symbol for symbol in open_legs if symbol.endswith("PE"))

        if bool(ce_symbols) != bool(pe_symbols):
            lone = (ce_symbols or pe_symbols)[0]
            log.error(f"[RECONCILE] Naked strategy leg found: {lone}; flattening.")
            if not self.broker.square_off_symbol(lone):
                raise FatalExecutionError(
                    f"Startup reconciliation could not verify {lone} as flat."
                )
            ce_symbols, pe_symbols, open_legs = [], [], {}

        if ce_symbols and pe_symbols:
            if len(ce_symbols) != 1 or len(pe_symbols) != 1:
                raise RuntimeError(
                    f"Multiple strategy legs found on startup: CE={ce_symbols}, PE={pe_symbols}. "
                    "Manual inspection required."
                )
            self._adopt_open_straddle(
                ce_symbols[0], pe_symbols[0], open_legs, completed_entries
            )
        else:
            if self.position is not None:
                log.warning("[RECONCILE] State showed a position but broker is flat; clearing state position.")
                self.position = None
                if self.phase == "IN_POSITION":
                    self.phase = "WAITING_ENTRY"
            if not self._state_loaded_today and completed_entries > 0:
                log.warning(
                    f"[RECONCILE] {completed_entries} completed prior entries, flat book, no state; "
                    "marking day DONE to prevent duplicate trading."
                )
                self.phase = "DONE"
            elif not self._state_loaded_today:
                self.phase = "WAITING_ENTRY"
                self.attempt_idx = 0

        if not self._state_loaded_today:
            broker_realised = self._broker_realised_pnl(positions)
            if abs(broker_realised) > 1e-9:
                self.daily_realized_pnl = broker_realised
                log.warning(
                    f"[RECONCILE] Seeded daily P&L from broker realised gross: "
                    f"Rs{broker_realised:,.0f}; historical charges are not recoverable."
                )
        self._save_state()

    # ------------------------------------------------------------------
    # Waiting and entry
    # ------------------------------------------------------------------
    def _wait_until(self, target: datetime, label: str) -> bool:
        log.info(f"[WAIT] {label}: target={target.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        while now_ist().time() < self.exit_time:
            remaining = (target - now_ist()).total_seconds()
            if remaining <= 0:
                return True
            time.sleep(min(1.0, max(0.10, remaining)))
        return False

    def enter(self) -> bool:
        if now_ist().time() >= self.exit_time:
            return False
        underlying = self._underlying_ltp()
        atm = round_to_step(underlying, self.strike_step)
        pe_symbol, ce_symbol = self._symbols_for_atm(atm)
        log.info(
            f"[ENTRY] attempt #{self.attempt_idx + 1}: {self.underlying_name}={underlying:.2f}, "
            f"ATM={atm}, CE={ce_symbol}, PE={pe_symbol}"
        )

        try:
            pe_token, _, ce_token, _ = self._resolve_option_pair_snapshot(
                pe_symbol, ce_symbol
            )
        except Exception as exc:
            log.error(f"[ENTRY] Option resolution failed: {exc}")
            return False

        self.feed.subscribe([pe_token, ce_token])
        if not self.feed.wait_for([pe_token, ce_token], timeout=10.0):
            log.error("[ENTRY] Live ticks not received for both legs.")
            self.feed.unsubscribe([pe_token, ce_token])
            return False

        fills = self.broker.open_short_straddle(
            pe_symbol, ce_symbol, pe_token, ce_token, self.qty
        )
        if not fills.get("ok"):
            log.error("[ENTRY] Both legs were not confirmed filled; attempt aborted.")
            self.feed.unsubscribe([pe_token, ce_token])
            return False

        entry_dt = now_ist()
        self.position = self._new_position_state(
            attempt_idx=self.attempt_idx,
            pe_symbol=pe_symbol,
            ce_symbol=ce_symbol,
            pe_token=pe_token,
            ce_token=ce_token,
            pe_entry=float(fills["pe_fill"]),
            ce_entry=float(fills["ce_fill"]),
            qty=self.qty,
            entry_dt=entry_dt,
        )
        self.phase = "IN_POSITION"
        self._save_state()

        p = self.position
        log.info(
            f"[ENTRY] filled CE={p['ce_entry']:.2f}, PE={p['pe_entry']:.2f}, "
            f"premium=Rs{p['premium_sum']:,.0f}, stop={p['stop_pct']:.4f}/Rs{p['stop_rupees']:,.0f}, "
            f"target={p['target_pct']:.4f}/"
            f"{'Rs%.0f' % p['target_rupees'] if p['target_rupees'] else 'off'}, "
            f"protect={p['protect_pct']:.4f}/"
            f"{'Rs%.0f' % p['G'] if p['G'] else 'off'}"
        )
        return True

    # ------------------------------------------------------------------
    # Monitoring and exit
    # ------------------------------------------------------------------
    def monitor_and_exit(self) -> str:
        if not self.position:
            raise RuntimeError("monitor_and_exit called without an open position")
        p = self.position
        pe_token, ce_token = int(p["pe_tok"]), int(p["ce_tok"])
        pe_entry, ce_entry = float(p["pe_entry"]), float(p["ce_entry"])
        qty = int(p["qty"])
        stop_rupees = float(p["stop_rupees"])
        target_rupees = p.get("target_rupees")
        G = p.get("G")
        peak = float(p.get("peak", 0.0))
        armed = bool(p.get("armed", False))

        self.feed.subscribe([pe_token, ce_token])
        self.feed.wait_for([pe_token, ce_token], timeout=10.0)

        last_heartbeat = 0.0
        last_state_save = time.time()
        exit_reason = "TIME_EXIT"

        while now_ist().time() < self.exit_time:
            current = now_ist()

            pe_ltp = self.feed.get(pe_token)
            ce_ltp = self.feed.get(ce_token)
            if pe_ltp is None or ce_ltp is None:
                time.sleep(MONITOR_POLL_SECONDS)
                continue

            pnl = (ce_entry - ce_ltp) * qty + (pe_entry - pe_ltp) * qty
            peak = max(peak, pnl)
            if G is not None and not armed and peak >= float(G):
                armed = True
                log.info(f"[PROTECT] Armed: peak=Rs{peak:,.0f} >= G=Rs{float(G):,.0f}")

            giveback = float(G) if G is not None else None
            late_active = (
                armed
                and p.get("late_giveback_rupees") is not None
                and current.time() >= self.profile.late_giveback_from
            )
            if late_active:
                # Match the backtester: late mode may only tighten the trail,
                # never loosen an already tighter base give-back.
                late_value = float(p["late_giveback_rupees"])
                giveback = min(float(giveback), late_value) if giveback is not None else late_value

            if time.time() - last_state_save >= 10.0:
                p["peak"], p["armed"] = peak, armed
                self._save_state()
                last_state_save = time.time()

            if time.time() - last_heartbeat >= MONITOR_HEARTBEAT_SECONDS:
                trail_text = (
                    f"giveback=Rs{giveback:,.0f}{' LATE' if late_active else ''}"
                    if giveback is not None
                    else "giveback=off"
                )
                log.info(
                    f"[MONITOR] pnl=Rs{pnl:,.0f}, peak=Rs{peak:,.0f}, "
                    f"CE={ce_ltp:.2f}, PE={pe_ltp:.2f}, armed={armed}, {trail_text}"
                )
                last_heartbeat = time.time()

            # Same priority as the backtester on simultaneous observations.
            if pnl <= -stop_rupees:
                exit_reason = "STOPLOSS"
                break
            if target_rupees is not None and pnl >= float(target_rupees):
                exit_reason = "PROFIT_TARGET"
                break
            if armed and giveback is not None and pnl <= peak - giveback:
                exit_reason = "PROFIT_PROTECT"
                break
            time.sleep(MONITOR_POLL_SECONDS)

        if now_ist().time() >= self.exit_time and exit_reason == "TIME_EXIT":
            log.info("[MONITOR] Strategy exit cutoff reached; flattening position.")

        close = self.broker.close_short_straddle(
            p["pe_sym"], p["ce_sym"], pe_token, ce_token, qty
        )
        if not close.get("ok"):
            raise RuntimeError("Exit legs not both confirmed; restart reconciliation required.")

        pe_exit = float(close["pe_fill"])
        ce_exit = float(close["ce_fill"])
        gross = (ce_entry - ce_exit) * qty + (pe_entry - pe_exit) * qty
        charges = compute_trade_charges(
            entry_ce=ce_entry,
            entry_pe=pe_entry,
            exit_ce=ce_exit,
            exit_pe=pe_exit,
            qty=qty,
        )
        net = gross - charges
        self.daily_realized_pnl += net

        log.info(
            f"[EXIT] {exit_reason}: CE {ce_entry:.2f}->{ce_exit:.2f}, "
            f"PE {pe_entry:.2f}->{pe_exit:.2f}, gross=Rs{gross:,.0f}, "
            f"est_charges=Rs{charges:,.0f}, net=Rs{net:,.0f}, "
            f"day_net=Rs{self.daily_realized_pnl:,.0f}"
        )

        self.feed.unsubscribe([pe_token, ce_token])
        self.position = None
        self.phase = "WAITING_ENTRY"
        self._save_state()
        return exit_reason

    # ------------------------------------------------------------------
    # Re-entry state machine
    # ------------------------------------------------------------------
    def _schedule_reentry(self, exit_reason: str) -> bool:
        if self.attempt_idx >= MAX_REATTEMPTS:
            log.info("[REENTRY] Maximum re-attempt count exhausted.")
            return False

        if exit_reason in {"STOPLOSS", "PROFIT_PROTECT"}:
            delay = reentry_delay_for_attempt(self.attempt_idx)
            kind = exit_reason
        elif exit_reason == "PROFIT_TARGET" and PROFIT_TARGET_REENTRY_ENABLED:
            delay = PROFIT_TARGET_REENTRY_DELAY
            kind = "PROFIT_TARGET"
        else:
            log.info(f"[DAY] No re-entry after {exit_reason}.")
            return False

        self.attempt_idx += 1
        self.pending_entry_kind = kind
        self.reentry_target = now_ist() + timedelta(minutes=delay)
        self.phase = "WAITING_REENTRY"
        self._save_state()
        log.info(
            f"[REENTRY] {kind}: wait {delay} minute(s), then momentum gate, "
            f"next attempt #{self.attempt_idx + 1}."
        )
        return True

    def _schedule_after_entry_failure(self) -> bool:
        """Consume the failed operational attempt and defer to the next one.

        This path is used only when no straddle position was established and the
        broker execution layer has verified that the relevant symbols are flat.
        It does not alter stop, target, momentum, or normal re-entry rules.
        """
        if self.position is not None:
            raise FatalExecutionError(
                "Entry failure scheduling requested while a position is still recorded."
            )
        if self.attempt_idx >= MAX_REATTEMPTS:
            log.error(
                "[ENTRY FAILURE] No later strategy attempt remains; maximum re-attempt "
                "count is exhausted."
            )
            return False
        if now_ist().time() >= self.exit_time:
            log.error("[ENTRY FAILURE] Exit cutoff reached; cannot defer another entry.")
            return False

        delay = reentry_delay_for_attempt(self.attempt_idx)
        self.attempt_idx += 1
        self.pending_entry_kind = "ENTRY_EXECUTION_FAILED"
        self.reentry_target = now_ist() + timedelta(minutes=delay)
        self.phase = "WAITING_REENTRY"
        self._save_state()
        log.warning(
            f"[ENTRY FAILURE] Current attempt abandoned after bounded execution failure; "
            f"verified flat. Deferring {delay} minute(s) to scheduled attempt "
            f"#{self.attempt_idx + 1}, then applying the momentum gate again."
        )
        return True

    def _daily_breaker_hit(self) -> bool:
        limit = self.profile.max_daily_loss_rupees
        return limit > 0 and self.daily_realized_pnl <= -limit

    def _finish_day(self, reason: str) -> None:
        self.phase = "DONE"
        self.position = None
        self.reentry_target = None
        self._save_state()
        log.info(
            f"[DAY DONE] reason={reason}; estimated net P&L=Rs{self.daily_realized_pnl:,.0f}"
        )

    def run_day(self) -> None:
        self.reconcile_on_startup()
        log.info("=" * 92)
        log.info(
            f"[DAY] {today_ist()} | {self.underlying_name} expiry={self.expiry_date} "
            f"DTE={self.selection.days_to_expiry} | mode={'PAPER' if self.broker.paper else 'LIVE'}"
        )
        log.info(
            f"[DAY] entry={ENTRY_TIME_IST}, exit={EXIT_TIME_IST}, qty={self.qty}, "
            f"max_reattempts={MAX_REATTEMPTS}, daily_loss=Rs{self.profile.max_daily_loss_rupees:,.0f}"
        )

        if self.phase == "DONE":
            log.info("[DAY] State indicates the strategy has already completed today.")
            return

        if now_ist().time() >= self.exit_time:
            if self.position:
                log.warning("[DAY] Started after exit cutoff with a position; forcing exit.")
                self.monitor_and_exit()
            self._finish_day("PAST_EXIT_TIME")
            return

        # Resume a saved/broker-confirmed open position.
        if self.position:
            log.warning(
                f"[RESUME] Monitoring {self.position['ce_sym']}/{self.position['pe_sym']} "
                f"from attempt #{self.attempt_idx + 1}."
            )
            reason = self.monitor_and_exit()
            if self._daily_breaker_hit():
                self._finish_day("DAILY_LOSS_LIMIT")
                return
            if not self._schedule_reentry(reason):
                self._finish_day(reason)
                return

        # Resume a pending delay after restart.
        if self.phase == "WAITING_REENTRY" and self.reentry_target:
            if not self._wait_until(self.reentry_target, "saved re-entry delay"):
                self._finish_day("EXIT_TIME_DURING_REENTRY_DELAY")
                return
            self.reentry_target = None
            self.phase = "WAITING_GATE"
            self._save_state()

        # Fresh first-entry wait.
        if self.attempt_idx == 0 and self.phase in {"WAITING_ENTRY", "WAITING_GATE"}:
            entry_dt = combine_ist(today_ist(), self.entry_time)
            current = now_ist()
            if current < entry_dt:
                if not self._wait_until(entry_dt, "first entry"):
                    self._finish_day("EXIT_TIME_BEFORE_ENTRY")
                    return
            elif (
                not self._state_loaded_today
                and MAX_LATE_START_MINUTES > 0
                and current > entry_dt + timedelta(minutes=MAX_LATE_START_MINUTES)
            ):
                self._finish_day(
                    f"LATE_START_OVER_{MAX_LATE_START_MINUTES}_MINUTES"
                )
                return
            elif not self._state_loaded_today and current > entry_dt:
                log.warning(
                    f"[DAY] Started {int((current-entry_dt).total_seconds()//60)} minute(s) "
                    "after configured entry; applying momentum gate from current time."
                )

        while now_ist().time() < self.exit_time:
            if self._daily_breaker_hit():
                log.warning(
                    f"[BREAKER] Daily net loss Rs{self.daily_realized_pnl:,.0f} reached "
                    f"limit Rs{self.profile.max_daily_loss_rupees:,.0f}."
                )
                self._finish_day("DAILY_LOSS_LIMIT")
                return

            if self.phase == "WAITING_REENTRY" and self.reentry_target:
                if not self._wait_until(self.reentry_target, "re-entry delay"):
                    self._finish_day("EXIT_TIME_DURING_REENTRY_DELAY")
                    return
                self.reentry_target = None

            if not self.wait_for_momentum_gate():
                self._finish_day("MOMENTUM_GATE_NOT_MET")
                return

            if not self.enter():
                if not self._schedule_after_entry_failure():
                    self._finish_day("ENTRY_FAILED_NO_LATER_ATTEMPT")
                    return
                continue

            exit_reason = self.monitor_and_exit()
            if self._daily_breaker_hit():
                self._finish_day("DAILY_LOSS_LIMIT")
                return
            if not self._schedule_reentry(exit_reason):
                self._finish_day(exit_reason)
                return

        self._finish_day("EXIT_TIME")


# =============================================================================
# 7. BOOTSTRAP
# =============================================================================
def _validate_configuration() -> None:
    if ENTRY_TIME >= EXIT_TIME:
        raise RuntimeError(
            f"ENTRY_TIME_IST={ENTRY_TIME_IST} must be earlier than EXIT_TIME_IST={EXIT_TIME_IST}."
        )
    if MOMENTUM_LOOKBACK_MIN < 1 and ENTRY_MOMENTUM_GATE:
        raise RuntimeError("MOMENTUM_LOOKBACK_MIN must be >= 1 when the gate is enabled.")
    if MAX_REATTEMPTS < 0:
        raise RuntimeError("MAX_REATTEMPTS cannot be negative.")
    if not LOSS_LIMIT_RUPEES_BY_ATTEMPT:
        raise RuntimeError("LOSS_LIMIT_RUPEES_BY_ATTEMPT cannot be empty.")
    if API_MAX_RETRIES < 1:
        raise RuntimeError(
            "API_MAX_RETRIES must be >= 1. Infinite API retries are disabled."
        )
    if API_ORDER_MAX_RETRIES < 1:
        raise RuntimeError("API_ORDER_MAX_RETRIES must be >= 1.")
    if not 1 <= ENTRY_EXECUTION_MAX_ATTEMPTS <= 4:
        raise RuntimeError(
            "ENTRY_EXECUTION_MAX_ATTEMPTS must be between 1 and 4."
        )
    if CLEANUP_MAX_ATTEMPTS < 1:
        raise RuntimeError("CLEANUP_MAX_ATTEMPTS must be >= 1.")
    if ORDER_STATUS_MAX_POLLS < 1 or ORDER_CONFIRM_TIMEOUT_SECONDS <= 0:
        raise RuntimeError(
            "ORDER_STATUS_MAX_POLLS and ORDER_CONFIRM_TIMEOUT_SECONDS must be positive."
        )
    if PROCESS_RESTART_MAX_ATTEMPTS < 1:
        raise RuntimeError("PROCESS_RESTART_MAX_ATTEMPTS must be >= 1.")


def main() -> None:
    _validate_configuration()
    log.info("#" * 92)
    log.info(
        f"[BOOT] V3OPT short-straddle trader starting in "
        f"{'PAPER' if PAPER_TRADING else 'LIVE'} mode."
    )
    log.info(f"[BOOT] Config: {PROPERTY_FILE_PATH}")
    if PAPER_TRADING:
        log.warning("[BOOT] PAPER SAFETY LOCK ACTIVE: no order-placement call will be made.")
    else:
        log.critical("[BOOT] LIVE MODE ACTIVE: real orders can be transmitted.")

    if not oUtils.is_trading_day(today_ist()):
        log.info(f"[BOOT] {today_ist()} is not a configured trading day; exiting.")
        return

    allowed_for_selection = ALLOWED_DTE if ENFORCE_DTE else list(range(0, 8))
    # Resolve the calendar gate before authenticating. On a non-strategy day the
    # script exits cleanly even if today's Kite access token has not been refreshed.
    try:
        oUtils.select_underlying_for_day(
            as_of_day=today_ist(),
            allowed_dte=allowed_for_selection,
        )
    except oUtils.NoTradeDay as exc:
        log.info(f"[BOOT] No strategy trade today: {exc}")
        return

    kite = _api(
        oUtils.intialize_kite_api,
        desc="Kite session initialisation",
        max_retries=API_MAX_RETRIES,
    )
    # KiteConnect uses this attribute for HTTP request timeouts in supported
    # versions. Setting it is harmless on versions that read it dynamically.
    try:
        kite.timeout = float(API_HTTP_TIMEOUT_SECONDS)
        log.info(f"[BOOT] Kite HTTP timeout set to {API_HTTP_TIMEOUT_SECONDS:.1f}s.")
    except Exception as exc:
        log.warning(f"[BOOT] Could not set Kite HTTP timeout attribute: {exc}")
    log.info("[BOOT] Kite session initialised.")
    selection = oUtils.resolve_instrument_selection(
        kite,
        as_of_day=today_ist(),
        allowed_dte=allowed_for_selection,
    )

    if ENFORCE_DTE and selection.days_to_expiry not in ALLOWED_DTE:
        log.info(
            f"[BOOT] DTE gate rejected {selection.underlying_name}: "
            f"DTE={selection.days_to_expiry}, allowed={ALLOWED_DTE}."
        )
        return

    profile = resolve_profile(selection.underlying_name)
    log.info(
        f"[BOOT] Selected {selection.underlying_name}; expiry={selection.expiry_date}, "
        f"DTE={selection.days_to_expiry}, part={selection.part_symbol}, "
        f"exchange={selection.options_exchange}, qty={selection.quantity_units}."
    )
    log.info(
        f"[BOOT] Profile: protect={profile.profit_protect_pct:.4f}, "
        f"late_giveback={profile.late_giveback_pct:.4f} from "
        f"{profile.late_giveback_from.strftime('%H:%M')}, target={profile.profit_target_pct:.4f}, "
        f"late_target={profile.profit_target_pct_late:.4f} from "
        f"{profile.profit_target_late_from.strftime('%H:%M')}, "
        f"daily_loss=Rs{profile.max_daily_loss_rupees:,.0f}."
    )

    api_key = getattr(kite, "api_key", None) or getattr(oUtils, "API_KEY", None)
    access_token = getattr(kite, "access_token", None) or getattr(oUtils, "ACCESS_TOKEN", None)
    if not api_key or not access_token:
        raise RuntimeError("Could not obtain api_key/access_token for KiteTicker.")

    feed = PriceFeed(str(api_key), str(access_token))
    feed.start()
    broker = Broker(
        kite,
        feed,
        selection.options_exchange,
        paper=PAPER_TRADING,
    )
    trader = LiveStraddleTrader(kite, feed, broker, selection, profile)

    try:
        for process_attempt in range(1, PROCESS_RESTART_MAX_ATTEMPTS + 1):
            try:
                log.info(
                    f"[RESILIENCE] Strategy process attempt "
                    f"{process_attempt}/{PROCESS_RESTART_MAX_ATTEMPTS}."
                )
                trader.run_day()
                break
            except KeyboardInterrupt:
                log.warning("[SHUTDOWN] Interrupted by user.")
                break
            except FatalExecutionError as exc:
                log.critical(
                    f"[RESILIENCE] FATAL execution uncertainty: {exc}. "
                    "Automated restarts are disabled for this condition; manual broker "
                    "inspection is required."
                )
                break
            except Exception as exc:
                log.exception(
                    f"[RESILIENCE] run_day failed on process attempt "
                    f"{process_attempt}/{PROCESS_RESTART_MAX_ATTEMPTS}: {exc}"
                )
                if now_ist().time() >= trader.exit_time:
                    log.error("[RESILIENCE] Exit cutoff passed; not restarting strategy loop.")
                    break
                if process_attempt >= PROCESS_RESTART_MAX_ATTEMPTS:
                    log.critical(
                        "[RESILIENCE] Bounded process restart budget exhausted; stopping."
                    )
                    break
                log.warning(
                    f"[RESILIENCE] Restarting strategy loop in "
                    f"{PROCESS_RESTART_DELAY_SECONDS:.1f}s; state retained."
                )
                time.sleep(PROCESS_RESTART_DELAY_SECONDS)
    finally:
        feed.stop()
        log.info("[SHUTDOWN] WebSocket closed.")


if __name__ == "__main__":
    main()
