"""
============================================================================
live_short_straddle_trader.py
============================================================================
LIVE / PAPER executor for the strategy defined by:

    atm_straddle_backtest_v2.py
    straddle_config_DTE_1_v2.properties

The live state machine mirrors the v2 backtest rules that matter for order
execution:

* Enter one short ATM straddle at ENTRY_TIME_IST on an allowed-DTE day.
* Per-attempt stop:
      min(loss-limit percentage x collected premium,
          MAX_LOSS_LIMIT_RUPEES_BY_ATTEMPT)
* Split profit-protect:
      arm at PROFIT_PROTECT_ARM_PCT x premium;
      exit after PROFIT_PROTECT_GIVEBACK_PCT x premium is surrendered from peak.
* Optional breakeven ratchet:
      after BREAKEVEN_ARM_PCT x premium is reached, lift the stop floor to
      BREAKEVEN_LOCK_PCT x premium.
* Profit target:
      PROFIT_TARGET_PCT x premium; reaching it ends the trading day.
* Re-entry only after STOPLOSS or PROFIT_PROTECT, using the configured delay.
* Re-entry premium gate:
      reject a new ATM straddle when its current premium exceeds
      REENTRY_MAX_PREMIUM_RATIO x the immediately preceding entry premium.
* Stop further attempts after the configured daily realised-loss limit.
* Force exit at EXIT_TIME_IST. No fresh entry is allowed at or after that time.

PAPER MODE IS THE DEFAULT AND THE PROVIDED CONFIG KEEPS IT ENABLED. Live mode
also requires an explicit confirmation phrase, preventing an accidental switch
from paper to real orders.

Operational differences that cannot be identical to a one-minute backtest:

* The live strategy uses actual WebSocket ticks and actual/simulated fills.
* It monitors immediately after entry rather than ignoring risk for the rest of
  the entry minute.
* Intraminute stop/target fills are whatever the broker or paper-fill model can
  obtain; the backtest can assume candle-extreme threshold fills.

The process writes restart state to disk and reconciles broker positions on a
live-mode restart. Test this version over several paper sessions before enabling
real orders.
============================================================================
"""

import os
import sys
import time
import math
import json
import threading
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, date, time as dtime, timedelta
from typing import Optional

import pytz

# Your existing helper module (same one A and B import).
import OptionTradeUtils_env as oUtils

# KiteTicker is the Kite WebSocket client.
try:
    from kiteconnect import KiteTicker
except Exception as _e:  # pragma: no cover
    KiteTicker = None  # surfaced at runtime with a clear message


# ===========================================================================
# 0) CONFIGURATION SOURCE: external property file
# ===========================================================================
# Every tunable below is read via os.getenv(...). This loader first pushes the
# values from an external KEY=VALUE property file into the environment, so
# settings can be changed WITHOUT editing this script. Path defaults to
# "live_trader_config.properties" next to this file; override with the
# STRADDLE_LIVE_CONFIG environment variable. A real environment variable that
# is already set takes precedence over the file.
def _load_property_file() -> str:
    cfg_path = os.getenv(
        "STRADDLE_LIVE_CONFIG",
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "live_trader_config.properties"),
    )
    if not os.path.exists(cfg_path):
        print(f"[CONFIG] Property file not found at {cfg_path}; using built-in defaults.")
        return cfg_path
    loaded = 0
    with open(cfg_path, "r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#") or line.startswith(";") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            key, val = key.strip(), val.strip()
            if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
                val = val[1:-1]
            if key and key not in os.environ:   # real env vars win over the file
                os.environ[key] = val
                loaded += 1
    print(f"[CONFIG] Loaded {loaded} setting(s) from {cfg_path}")
    return cfg_path


PROPERTY_FILE_PATH = _load_property_file()


def _parse_bool_env(name: str, default: bool) -> bool:
    """Parse common boolean spellings from an environment/property value."""
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return bool(default)
    value = str(raw).strip().lower()
    if value in {"1", "true", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"Invalid boolean for {name}: {raw!r}")


def _parse_pct_value(raw, default: float = 0.0) -> float:
    """
    Parse a percentage into decimal form.

    Accepted examples:
        0.17 -> 0.17
        17   -> 0.17
        17%  -> 0.17
    """
    if raw is None or str(raw).strip() == "":
        return float(default)
    value = float(str(raw).strip().replace("%", ""))
    if abs(value) > 1.0:
        value /= 100.0
    if value < 0:
        raise ValueError(f"Percentage cannot be negative: {raw!r}")
    return float(value)


def _csv_pcts(raw, default):
    """Parse comma-separated percentages into decimal form."""
    if raw is None or str(raw).strip() == "":
        return [_parse_pct_value(v) for v in default]
    values = [x for x in str(raw).replace(" ", "").split(",") if x != ""]
    if not values:
        return [_parse_pct_value(v) for v in default]
    return [_parse_pct_value(v) for v in values]


def _csv_ints(raw, default):
    """Parse a comma-separated integer list."""
    if raw is None or str(raw).strip() == "":
        return list(default)
    values = [x for x in str(raw).replace(" ", "").split(",") if x != ""]
    if not values:
        return list(default)
    return [int(round(float(x))) for x in values]


def _float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return float(default)
    return float(str(raw).replace(",", "").strip())


# ===========================================================================
# 1) CONFIGURATION -- aligned with atm_straddle_backtest_v2.py
# ===========================================================================
STRATEGY_ID = os.getenv("STRATEGY_ID", "atm-straddle-v2-dte1-live-20260729")
# Prefix for dynamic broker tags. The full tag also contains timestamp,
# strategy-attempt number, action and leg and remains within Kite's 20-character
# limit. Old SSSTRADDLE tags are still recognised during reconciliation.
EXECUTION_TAG_PREFIX = os.getenv("EXECUTION_TAG_PREFIX", "SSV2").strip().upper()
# AUTO_DTE mirrors the backtest: choose the eligible NIFTY/SENSEX contract for
# the day. MANUAL uses `.env` choice and is retained for controlled testing.
UNDERLYING_SELECTION_MODE = os.getenv(
    "UNDERLYING_SELECTION_MODE", "AUTO_DTE"
).strip().upper()

# ---- Hard safety -----------------------------------------------------------
# Paper is the default even when the property key is absent.
PAPER_TRADING = _parse_bool_env("PAPER_TRADING", True)
LIVE_CONFIRM_PHRASE = "YES_I_ACCEPT_LIVE_ORDERS"
LIVE_TRADING_CONFIRM = os.getenv("LIVE_TRADING_CONFIRM", "").strip()

# ---- Timing (IST) ----------------------------------------------------------
IST = pytz.timezone("Asia/Kolkata")
ENTRY_TIME_IST = os.getenv("ENTRY_TIME_IST", "09:20")
EXIT_TIME_IST = os.getenv(
    "EXIT_TIME_IST",
    os.getenv("SQUAREOFF_TIME_IST", "15:26"),  # legacy-key fallback
)
# Backtest has an exact minute. Live launch jitter is allowed only within this
# short grace window; a much later process start does not invent a late entry.
ENTRY_GRACE_SECONDS = int(_float_env("ENTRY_GRACE_SECONDS", 60))
SESSION_START_IST = dtime(9, 15)
SESSION_END_IST = dtime(15, 30)

# ---- Per-attempt stop ------------------------------------------------------
# New v2 key first; old live-trader key remains a compatibility fallback.
_stop_raw = os.getenv("LOSS_LIMIT_RUPEES_BY_ATTEMPT")
if _stop_raw is None:
    _stop_raw = os.getenv("STOP_PCT_BY_ATTEMPT")
LOSS_LIMIT_RUPEES_BY_ATTEMPT = _csv_pcts(
    _stop_raw,
    [0.2444, 0.2555, 0.2666, 0.2776, 0.2887, 0.2998, 0.3108],
)
STOP_PCT_BY_ATTEMPT = LOSS_LIMIT_RUPEES_BY_ATTEMPT  # legacy alias

_stop_cap_raw = os.getenv("MAX_LOSS_LIMIT_RUPEES_BY_ATTEMPT")
if _stop_cap_raw is None:
    _stop_cap_raw = os.getenv("STOP_CAP_RUPEES")
MAX_LOSS_LIMIT_RUPEES_BY_ATTEMPT = (
    float(str(_stop_cap_raw).replace(",", "").strip())
    if _stop_cap_raw not in (None, "")
    else 3500.0
)
STOP_CAP_RUPEES = MAX_LOSS_LIMIT_RUPEES_BY_ATTEMPT  # legacy alias

# ---- Split profit-protect --------------------------------------------------
_pp_fallback_raw = os.getenv(
    "PROFIT_PROTECT_TRIGGER_RUPEES",
    os.getenv("PROFIT_PROTECT_PCT", "0.0816"),
)
PROFIT_PROTECT_TRIGGER_RUPEES = _parse_pct_value(_pp_fallback_raw, 0.0816)
PROFIT_PROTECT_ARM_PCT = _parse_pct_value(
    os.getenv("PROFIT_PROTECT_ARM_PCT"),
    PROFIT_PROTECT_TRIGGER_RUPEES,
)
PROFIT_PROTECT_GIVEBACK_PCT = _parse_pct_value(
    os.getenv("PROFIT_PROTECT_GIVEBACK_PCT"),
    PROFIT_PROTECT_TRIGGER_RUPEES,
)

# ---- Breakeven ratchet -----------------------------------------------------
BREAKEVEN_ARM_PCT = _parse_pct_value(os.getenv("BREAKEVEN_ARM_PCT"), 0.0)
BREAKEVEN_LOCK_PCT = _parse_pct_value(os.getenv("BREAKEVEN_LOCK_PCT"), 0.0)

# ---- Profit target / daily circuit breaker --------------------------------
PROFIT_TARGET_PCT = _parse_pct_value(os.getenv("PROFIT_TARGET_PCT"), 0.17)
MAX_DAILY_LOSS_RUPEES = _float_env("MAX_DAILY_LOSS_RUPEES", 20000.0)

# ---- Re-entries ------------------------------------------------------------
REENTRY_DELAY_BY_ATTEMPT = _csv_ints(
    os.getenv("REENTRY_DELAY_BY_ATTEMPT"),
    [7, 10, 13, 16, 19, 22],
)
MAX_REATTEMPTS = int(_float_env("MAX_REATTEMPTS", 6))
REENTRY_MAX_PREMIUM_RATIO = _float_env("REENTRY_MAX_PREMIUM_RATIO", 1.32)

# ---- DTE gate --------------------------------------------------------------
ALLOWED_DTE = _csv_ints(os.getenv("ALLOWED_DTE"), [1])
ENFORCE_DTE = _parse_bool_env("ENFORCE_DTE", True)

# ---- Estimated transaction costs ------------------------------------------
# Threshold exits use gross P&L, exactly as in the backtest. Estimated charges
# are deducted only from realised daily P&L, which controls the daily breaker.
INCLUDE_ESTIMATED_TRANSACTION_COSTS = _parse_bool_env(
    "INCLUDE_ESTIMATED_TRANSACTION_COSTS", True
)
BROKERAGE_PER_ORDER = _float_env("BROKERAGE_PER_ORDER", 20.0)
ORDERS_PER_TRADE = int(_float_env("ORDERS_PER_TRADE", 4))
STT_SELL_PCT = _float_env("STT_SELL_PCT", 0.001)
EXCHANGE_TXN_PCT = _float_env("EXCHANGE_TXN_PCT", 0.0003553)
SEBI_PER_CRORE = _float_env("SEBI_PER_CRORE", 10.0)
STAMP_BUY_PCT = _float_env("STAMP_BUY_PCT", 0.00003)
IPFT_PER_CRORE = _float_env("IPFT_PER_CRORE", 0.010)
GST_PCT = _float_env("GST_PCT", 0.18)

# ---- Feed / paper-fill model ----------------------------------------------
MONITOR_POLL_SECONDS = _float_env("MONITOR_POLL_SECONDS", 0.2)
MONITOR_HEARTBEAT_SECONDS = _float_env("MONITOR_HEARTBEAT_SECONDS", 5.0)
PAPER_SLIPPAGE_TICKS = int(_float_env("PAPER_SLIPPAGE_TICKS", 1))
OPTION_TICK = _float_env("OPTION_TICK", 0.05)

LOG_FILE = os.getenv(
    "LOG_FILE",
    os.path.join(os.path.expanduser("~"), "short_straddle_v2_live.log"),
)

# ---- API and execution resilience -----------------------------------------
# All retries are finite. API_MAX_RETRIES is the maximum number of attempts for
# ordinary read calls. Order placement itself is never blindly retried: after
# any ambiguous response the broker reconciles orders and positions before any
# further action.
API_MAX_RETRIES = int(_float_env("API_MAX_RETRIES", 4))
API_ORDER_MAX_RETRIES = int(_float_env("API_ORDER_MAX_RETRIES", 2))
API_RETRY_BACKOFF_SECONDS = _float_env("API_RETRY_BACKOFF_SECONDS", 1.0)
API_RETRY_BACKOFF_MAX = _float_env("API_RETRY_BACKOFF_MAX", 5.0)

# A logical straddle entry receives at most four broker-execution cycles. Each
# cycle first reconciles existing tagged orders/positions, so a lost API reply
# cannot cause a duplicate order to be placed.
ENTRY_EXECUTION_MAX_ATTEMPTS = int(
    _float_env("ENTRY_EXECUTION_MAX_ATTEMPTS", 4)
)
EXIT_EXECUTION_MAX_ATTEMPTS = int(
    _float_env("EXIT_EXECUTION_MAX_ATTEMPTS", 4)
)
ORDER_CONFIRM_TIMEOUT_SECONDS = _float_env(
    "ORDER_CONFIRM_TIMEOUT_SECONDS", 8.0
)
ORDER_CONFIRM_POLL_SECONDS = _float_env(
    "ORDER_CONFIRM_POLL_SECONDS", 0.5
)
ORDER_DISCOVERY_TIMEOUT_SECONDS = _float_env(
    "ORDER_DISCOVERY_TIMEOUT_SECONDS", 5.0
)
CLEANUP_MAX_ATTEMPTS = int(_float_env("CLEANUP_MAX_ATTEMPTS", 4))
CLEANUP_CONFIRM_TIMEOUT_SECONDS = _float_env(
    "CLEANUP_CONFIRM_TIMEOUT_SECONDS", 8.0
)

# The process-level reconciliation loop is also finite.
PROCESS_MAX_RESTARTS = int(_float_env("PROCESS_MAX_RESTARTS", 3))
PROCESS_RESTART_DELAY_SECONDS = _float_env(
    "PROCESS_RESTART_DELAY_SECONDS", 5.0
)

# ---- Restart state ---------------------------------------------------------
STATE_FILE = os.getenv(
    "STATE_FILE",
    os.path.join(os.path.expanduser("~"), "short_straddle_v2_state.json"),
)


# ===========================================================================
# 2) LOGGING  (file + console; every epoch is recorded)
# ===========================================================================
def _build_logger() -> logging.Logger:
    lg = logging.getLogger("ss_live")
    lg.setLevel(logging.INFO)
    lg.propagate = False
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    # Rotating file handler so long-running sessions don't grow unbounded.
    fh = RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=5)
    fh.setFormatter(fmt)
    # Console handler.
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    if not lg.handlers:           # avoid duplicate handlers on re-import
        lg.addHandler(fh)
        lg.addHandler(ch)
    return lg


log = _build_logger()


# ===========================================================================
# API RETRY WRAPPER  (resilience against Zerodha API outages)
# ===========================================================================
def _api(fn, *args, desc="kite call", max_retries=None, **kwargs):
    """
    Execute a Kite API call with a finite attempt budget.

    `max_retries` is retained as a compatibility name but represents the total
    number of attempts, not retries-after-first-attempt. Order placement calls
    must use one attempt only and then reconcile broker state before deciding
    whether any further order is safe.
    """
    max_attempts = API_MAX_RETRIES if max_retries is None else int(max_retries)
    if max_attempts <= 0:
        raise ValueError(
            f"Finite positive max attempts required for {desc}; got {max_attempts}."
        )

    delay = max(0.0, API_RETRY_BACKOFF_SECONDS)
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            last_error = exc
            if attempt >= max_attempts:
                log.error(
                    f"[API] {desc} failed after {attempt}/{max_attempts} "
                    f"attempt(s): {exc}"
                )
                raise
            sleep_for = min(delay, API_RETRY_BACKOFF_MAX)
            log.warning(
                f"[API] {desc} failed on attempt {attempt}/{max_attempts}: "
                f"{exc}; retrying in {sleep_for:.1f}s"
            )
            if sleep_for > 0:
                time.sleep(sleep_for)
            delay = min(max(delay * 2, 0.1), API_RETRY_BACKOFF_MAX)

    raise RuntimeError(f"[API] {desc} failed without exception: {last_error}")


# ===========================================================================
# 3) SMALL HELPERS  (faithful to B's parameter semantics)
# ===========================================================================
def now_ist() -> datetime:
    """Current wall-clock time in IST."""
    return datetime.now(IST)


def parse_hhmm(s: str) -> dtime:
    """Parse HH:MM or HH:MM:SS into datetime.time."""
    parts = [int(x) for x in str(s).strip().split(":")]
    if len(parts) == 2:
        hh, mm = parts
        ss = 0
    elif len(parts) == 3:
        hh, mm, ss = parts
    else:
        raise ValueError(f"Invalid time {s!r}; expected HH:MM or HH:MM:SS")
    return dtime(hh, mm, ss)


def round_to_tick(price: float, tick: float = OPTION_TICK) -> float:
    """Round a price to the nearest exchange tick (>= one tick)."""
    price = max(float(price), tick)
    return round(round(price / tick) * tick, 2)


def round_to_step(x: float, step: int) -> int:
    """Round an underlying price to the nearest strike step (B's ATM rule)."""
    return int(round(float(x) / step) * step)


def stop_pct_for_attempt(attempt_idx: int) -> float:
    """Loss-limit percentage for attempt index 0, reusing the final value."""
    values = LOSS_LIMIT_RUPEES_BY_ATTEMPT
    if not values:
        return 0.0
    return float(values[attempt_idx]) if attempt_idx < len(values) else float(values[-1])


def reentry_delay_for_attempt(attempt_idx: int) -> int:
    """Delay before the next attempt, reusing the final configured value."""
    values = REENTRY_DELAY_BY_ATTEMPT
    if not values:
        return 0
    return int(values[attempt_idx]) if attempt_idx < len(values) else int(values[-1])


def effective_stop_rupees(attempt_idx: int, premium_sum_rupees: float) -> float:
    """Return min(loss percentage x premium, absolute cap)."""
    uncapped = stop_pct_for_attempt(attempt_idx) * float(premium_sum_rupees)
    if MAX_LOSS_LIMIT_RUPEES_BY_ATTEMPT > 0:
        return float(min(uncapped, MAX_LOSS_LIMIT_RUPEES_BY_ATTEMPT))
    return float(uncapped)


def compute_estimated_trade_charges(
    entry_ce: float,
    entry_pe: float,
    exit_ce: float,
    exit_pe: float,
    qty: int,
) -> float:
    """
    Mirror the transaction-cost model used by atm_straddle_backtest_v2.py.

    This is an estimate for strategy-state parity, not a replacement for the
    broker contract note. It affects only cumulative realised P&L and the daily
    loss breaker; stop/target/protect thresholds remain gross-P&L thresholds.
    """
    if not INCLUDE_ESTIMATED_TRANSACTION_COSTS:
        return 0.0

    entry_turnover = (float(entry_ce) + float(entry_pe)) * int(qty)
    exit_turnover = (float(exit_ce) + float(exit_pe)) * int(qty)
    total_turnover = entry_turnover + exit_turnover

    brokerage = BROKERAGE_PER_ORDER * ORDERS_PER_TRADE
    stt = entry_turnover * STT_SELL_PCT
    txn = total_turnover * EXCHANGE_TXN_PCT
    sebi = total_turnover * SEBI_PER_CRORE / 1_00_00_000
    stamp = exit_turnover * STAMP_BUY_PCT
    ipft = total_turnover * IPFT_PER_CRORE / 1_00_00_000
    gst = (brokerage + txn + sebi) * GST_PCT
    return round(brokerage + stt + txn + sebi + stamp + ipft + gst, 2)


# ===========================================================================
# 4) PRICE FEED  (Kite WebSocket -> latest LTP per instrument token)
# ===========================================================================
class PriceFeed:
    """
    Thin wrapper over KiteTicker. Maintains the latest traded price per
    instrument token in a dict that the strategy thread reads each poll.
    Supports dynamic (un)subscription because each re-entry uses new strikes.
    """

    def __init__(self, api_key: str, access_token: str):
        if KiteTicker is None:
            raise RuntimeError("kiteconnect is not installed: pip install kiteconnect")
        # Auto-reconnect (on by default in pykiteconnect) keeps the feed alive
        # through transient Zerodha outages; we raise the retry budget.
        try:
            self.ticker = KiteTicker(api_key, access_token,
                                     reconnect=True, reconnect_max_tries=300,
                                     reconnect_max_delay=60)
        except TypeError:
            self.ticker = KiteTicker(api_key, access_token)
        self.ltp = {}                      # token -> last_price
        self._subscribed = set()
        self._connected = threading.Event()

        # Bind callbacks.
        self.ticker.on_ticks = self._on_ticks
        self.ticker.on_connect = self._on_connect
        self.ticker.on_close = self._on_close
        self.ticker.on_error = self._on_error
        self.ticker.on_reconnect = self._on_reconnect
        self.ticker.on_noreconnect = self._on_noreconnect

    # --- websocket callbacks (run on the ticker's own thread) ---
    def _on_ticks(self, ws, ticks):
        for t in ticks:
            tok = t.get("instrument_token")
            px = t.get("last_price")
            if tok is not None and px is not None:
                self.ltp[tok] = float(px)

    def _on_connect(self, ws, response):
        log.info("[WS] Connected to Kite ticker.")
        self._connected.set()
        if self._subscribed:               # re-subscribe after a reconnect
            ws.subscribe(list(self._subscribed))
            ws.set_mode(ws.MODE_LTP, list(self._subscribed))

    def _on_close(self, ws, code, reason):
        log.warning(f"[WS] Closed (code={code}, reason={reason}).")

    def _on_error(self, ws, code, reason):
        log.warning(f"[WS] Error (code={code}, reason={reason}).")

    def _on_reconnect(self, ws, attempts_count):
        log.warning(f"[WS] Reconnecting... (attempt {attempts_count})")

    def _on_noreconnect(self, ws):
        log.error("[WS] Reconnection attempts exhausted; relying on process restart.")

    # --- control ---
    def start(self, timeout: float = 15.0):
        log.info("[WS] Starting ticker (threaded) ...")
        self.ticker.connect(threaded=True)
        if not self._connected.wait(timeout=timeout):
            log.warning("[WS] Ticker not confirmed connected within timeout; continuing.")

    def stop(self):
        try:
            self.ticker.close()
        except Exception:
            pass

    def subscribe(self, tokens):
        tokens = [int(t) for t in tokens]
        self._subscribed.update(tokens)
        try:
            self.ticker.subscribe(tokens)
            self.ticker.set_mode(self.ticker.MODE_LTP, tokens)
            log.info(f"[WS] Subscribed tokens: {tokens}")
        except Exception as e:
            log.warning(f"[WS] subscribe failed for {tokens}: {e}")

    def unsubscribe(self, tokens):
        tokens = [int(t) for t in tokens]
        self._subscribed.difference_update(tokens)
        try:
            self.ticker.unsubscribe(tokens)
        except Exception as e:
            log.warning(f"[WS] unsubscribe failed for {tokens}: {e}")

    def wait_for(self, tokens, timeout: float = 10.0) -> bool:
        """Block until every token has at least one tick (or timeout)."""
        deadline = time.time() + timeout
        tokens = [int(t) for t in tokens]
        while time.time() < deadline:
            if all(t in self.ltp for t in tokens):
                return True
            time.sleep(0.1)
        return all(t in self.ltp for t in tokens)

    def get(self, token):
        return self.ltp.get(int(token))


# ===========================================================================
# 5) BROKER  (paper + live order placement; live flow adapted from A)
# ===========================================================================
class ResidualPositionError(RuntimeError):
    """Raised when broker data cannot prove that strategy exposure is flat."""


class Broker:
    """
    Bounded, broker-reconciled execution for the two straddle legs.

    Safety invariants
    -----------------
    1. Every logical leg uses a deterministic tag containing date, strategy
       attempt, action and leg. Retrying the logical straddle therefore first
       finds the earlier broker order instead of blindly placing another.
    2. `place_order` is called once per placement decision. If its response is
       ambiguous, the tag is quarantined: no replacement is submitted until
       broker order/position data proves what happened.
    3. Entry succeeds only when BOTH:
           a) net position change equals the intended quantity; and
           b) tagged filled quantity equals the intended quantity
       for CE and PE.
    4. Failed entry immediately cancels pending orders, restores both symbols
       to their pre-entry quantities, and verifies the restoration.
    5. All loops have finite attempt/time budgets.
    """

    MARKET_PROTECTION = -1
    PENDING_STATUSES = {
        "OPEN",
        "TRIGGER PENDING",
        "OPEN PENDING",
        "MODIFY PENDING",
        "VALIDATION PENDING",
        "AMO REQ RECEIVED",
        "PUT ORDER REQ RECEIVED",
    }
    TERMINAL_FAILURE_STATUSES = {"REJECTED", "CANCELLED"}

    def __init__(self, kite, feed: PriceFeed, options_exchange: str, paper: bool):
        self.kite = kite
        self.feed = feed
        self.exchange = options_exchange
        self.paper = paper
        self._instrument_meta = None

    # ------------------------------------------------------------------
    # Instrument and tag helpers
    # ------------------------------------------------------------------
    def _load_instrument_meta(self) -> dict:
        if self._instrument_meta is None:
            rows = _api(
                self.kite.instruments,
                self.exchange,
                desc=f"instruments {self.exchange}",
            )
            self._instrument_meta = {
                str(row.get("tradingsymbol", "")): row
                for row in rows
                if row.get("tradingsymbol")
            }
            log.info(
                f"[INSTRUMENTS] Cached {len(self._instrument_meta)} symbols "
                f"for {self.exchange}."
            )
        return self._instrument_meta

    def _instrument_row(self, tradingsymbol: str) -> dict:
        row = self._load_instrument_meta().get(tradingsymbol)
        if row is None:
            raise RuntimeError(
                f"Instrument metadata not found on {self.exchange}: "
                f"{tradingsymbol}"
            )
        return row

    def _tick_size(self, tradingsymbol: str) -> float:
        tick = float(self._instrument_row(tradingsymbol).get("tick_size") or 0.0)
        if tick <= 0:
            raise RuntimeError(f"Invalid tick size for {tradingsymbol}: {tick}")
        return tick

    def _validate_quantity(self, tradingsymbol: str, qty: int) -> None:
        lot_size = int(self._instrument_row(tradingsymbol).get("lot_size") or 0)
        if lot_size <= 0:
            raise RuntimeError(
                f"Invalid lot size for {tradingsymbol}: {lot_size}"
            )
        if int(qty) <= 0 or int(qty) % lot_size != 0:
            raise RuntimeError(
                f"Quantity {qty} is not a positive multiple of exchange lot "
                f"size {lot_size} for {tradingsymbol}."
            )

    @staticmethod
    def _is_strategy_tag(value) -> bool:
        tag = str(value or "")
        return (
            tag.startswith(EXECUTION_TAG_PREFIX)
            or tag.startswith(str(oUtils.SS_ORDER_TAG))
        )

    @staticmethod
    def _leg_code(tradingsymbol: str) -> str:
        return "C" if str(tradingsymbol).endswith("CE") else "P"

    def _execution_tag(
        self,
        attempt_idx: int,
        action: str,
        tradingsymbol: str,
        *,
        execution_stamp: Optional[str] = None,
    ) -> str:
        """
        Build a unique Kite tag of at most 20 characters.

        Layout with the default prefix:
            SSV2 + YYMMDDHHMMSS + attempt(2) + action(1) + leg(1)

        The same execution_stamp is shared by both legs of one logical broker
        operation and by all four internal reconciliation attempts. A later
        scheduled entry gets a new stamp, so historical fills from an earlier
        cleaned-up attempt cannot be mistaken for current fills.
        """
        stamp = execution_stamp or now_ist().strftime("%y%m%d%H%M%S")
        if len(stamp) != 12 or not stamp.isdigit():
            raise RuntimeError(f"Invalid execution tag timestamp: {stamp!r}")
        attempt_no = min(max(int(attempt_idx) + 1, 0), 99)
        tag = (
            f"{EXECUTION_TAG_PREFIX}{stamp}{attempt_no:02d}"
            f"{str(action).upper()[:1]}{self._leg_code(tradingsymbol)}"
        )
        if len(tag) > 20:
            raise RuntimeError(f"Generated Kite order tag is too long: {tag}")
        return tag

    # ------------------------------------------------------------------
    # Broker snapshots
    # ------------------------------------------------------------------
    def _orders(self) -> list:
        rows = _api(
            self.kite.orders,
            desc="orders snapshot",
            max_retries=API_MAX_RETRIES,
        )
        return list(rows or [])

    def _positions(self) -> list:
        response = _api(
            self.kite.positions,
            desc="positions snapshot",
            max_retries=API_MAX_RETRIES,
        )
        return list((response or {}).get("net", []))

    def _position_qty_map(self, symbols) -> dict:
        wanted = {str(symbol) for symbol in symbols}
        quantities = {symbol: 0 for symbol in wanted}
        for row in self._positions():
            symbol = str(row.get("tradingsymbol", ""))
            if symbol in wanted:
                quantities[symbol] = int(row.get("quantity") or 0)
        return quantities

    @staticmethod
    def _filled_quantity(order: dict) -> int:
        if order.get("filled_quantity") is not None:
            return int(order.get("filled_quantity") or 0)
        quantity = int(order.get("quantity") or 0)
        pending = int(order.get("pending_quantity") or 0)
        return max(0, quantity - pending)

    @staticmethod
    def _pending_quantity(order: dict) -> int:
        return int(order.get("pending_quantity") or 0)

    def _matching_orders(
        self,
        orders,
        *,
        tag: str,
        symbol: str,
        transaction_type: str,
    ) -> list:
        txn = str(transaction_type).upper()
        return [
            row
            for row in orders
            if str(row.get("tag") or "") == tag
            and str(row.get("tradingsymbol") or "") == symbol
            and str(row.get("transaction_type") or "").upper() == txn
        ]

    def _weighted_average_fill(self, orders) -> tuple:
        total_qty = 0
        total_value = 0.0
        for row in orders:
            filled = self._filled_quantity(row)
            average = float(row.get("average_price") or 0.0)
            if filled > 0 and average > 0:
                total_qty += filled
                total_value += average * filled
        return (
            (total_value / total_qty if total_qty > 0 else 0.0),
            total_qty,
        )

    def _pair_snapshot(
        self,
        *,
        symbols: dict,
        targets: dict,
        tags: dict,
        transaction_types: dict,
        expected_filled: dict,
    ) -> dict:
        orders = self._orders()
        positions = self._position_qty_map(symbols.values())
        legs = {}
        all_confirmed = True
        overfilled = False

        for leg, symbol in symbols.items():
            matches = self._matching_orders(
                orders,
                tag=tags[leg],
                symbol=symbol,
                transaction_type=transaction_types[leg],
            )
            average, filled = self._weighted_average_fill(matches)
            pending_rows = [
                row
                for row in matches
                if (
                    str(row.get("status") or "").upper()
                    in self.PENDING_STATUSES
                    or self._pending_quantity(row) > 0
                )
            ]
            rejected_rows = [
                row
                for row in matches
                if str(row.get("status") or "").upper()
                in self.TERMINAL_FAILURE_STATUSES
            ]
            current_qty = int(positions.get(symbol, 0))
            intended_fill = int(expected_filled[leg])
            confirmed = (
                current_qty == int(targets[leg])
                and filled == intended_fill
                and not pending_rows
            )
            if filled > intended_fill:
                overfilled = True
            all_confirmed = all_confirmed and confirmed
            legs[leg] = {
                "symbol": symbol,
                "tag": tags[leg],
                "current_qty": current_qty,
                "target_qty": int(targets[leg]),
                "filled_qty": filled,
                "expected_filled_qty": intended_fill,
                "average_price": average,
                "pending_orders": pending_rows,
                "failed_orders": rejected_rows,
                "matching_orders": matches,
                "confirmed": confirmed,
            }

        return {
            "orders": orders,
            "positions": positions,
            "legs": legs,
            "confirmed": all_confirmed,
            "overfilled": overfilled,
        }

    # ------------------------------------------------------------------
    # Bounded order operations
    # ------------------------------------------------------------------
    def _paper_fill(self, token: float, side: str) -> float:
        ltp = self.feed.get(token)
        if ltp is None:
            raise RuntimeError("No tick yet for paper fill")
        slip = PAPER_SLIPPAGE_TICKS * OPTION_TICK
        price = ltp - slip if side == "SELL" else ltp + slip
        return round_to_tick(price)

    def _marketable_limit_price(
        self,
        tradingsymbol: str,
        transaction_type,
    ) -> float:
        key = f"{self.exchange}:{tradingsymbol}"
        quote = _api(
            self.kite.quote,
            key,
            desc=f"quote {tradingsymbol}",
            max_retries=API_MAX_RETRIES,
        )[key]
        depth = quote.get("depth", {})
        buy_depth = depth.get("buy", [])
        sell_depth = depth.get("sell", [])
        ltp = float(quote.get("last_price") or 0.0)
        if ltp <= 0:
            raise RuntimeError(f"Invalid LTP for {tradingsymbol}: {ltp}")

        if transaction_type == self.kite.TRANSACTION_TYPE_SELL:
            raw_price = (
                float(buy_depth[0]["price"])
                if buy_depth and buy_depth[0].get("price")
                else ltp * 0.995
            )
        else:
            raw_price = (
                float(sell_depth[0]["price"])
                if sell_depth and sell_depth[0].get("price")
                else ltp * 1.005
            )
        return round_to_tick(raw_price, self._tick_size(tradingsymbol))

    def _place_order_once(
        self,
        *,
        tradingsymbol: str,
        transaction_type,
        quantity: int,
        tag: str,
        order_type,
        price: Optional[float] = None,
        context: str,
    ):
        """
        Submit exactly once. A transport exception is intentionally not retried.
        The caller must reconcile the deterministic tag and net position.
        """
        kwargs = {
            "tradingsymbol": tradingsymbol,
            "variety": self.kite.VARIETY_REGULAR,
            "exchange": self.exchange,
            "transaction_type": transaction_type,
            "quantity": int(quantity),
            "order_type": order_type,
            "product": self.kite.PRODUCT_NRML,
            "tag": tag,
        }
        if order_type == self.kite.ORDER_TYPE_LIMIT:
            kwargs["price"] = float(price)
        else:
            kwargs["market_protection"] = self.MARKET_PROTECTION

        log.info(
            f"[ORDER PLACE] {context}: tag={tag}, symbol={tradingsymbol}, "
            f"side={transaction_type}, qty={quantity}, type={order_type}"
            + (f", price={float(price):.2f}" if price is not None else "")
        )
        try:
            order_id = self.kite.place_order(**kwargs)
            log.info(
                f"[ORDER ACK] {context}: tag={tag}, order_id={order_id}, "
                f"symbol={tradingsymbol}, qty={quantity}"
            )
            return order_id, False
        except Exception as exc:
            log.error(
                f"[ORDER AMBIGUOUS] {context}: tag={tag}, symbol={tradingsymbol}, "
                f"qty={quantity}; place_order raised {exc!r}. No blind retry "
                "will be made until broker reconciliation."
            )
            return None, True

    def _cancel_order(self, row: dict, context: str) -> bool:
        order_id = row.get("order_id")
        symbol = row.get("tradingsymbol")
        if not order_id:
            return False
        try:
            _api(
                self.kite.cancel_order,
                variety=self.kite.VARIETY_REGULAR,
                order_id=order_id,
                desc=f"cancel {context} {symbol}",
                max_retries=API_ORDER_MAX_RETRIES,
            )
            log.warning(
                f"[ORDER CANCEL] {context}: order_id={order_id}, "
                f"tag={row.get('tag')}, symbol={symbol}"
            )
            return True
        except Exception as exc:
            log.error(
                f"[ORDER CANCEL FAIL] {context}: order_id={order_id}, "
                f"symbol={symbol}: {exc}"
            )
            return False

    def _modify_to_market(self, row: dict, context: str) -> bool:
        order_id = row.get("order_id")
        pending = self._pending_quantity(row)
        if not order_id or pending <= 0:
            return False
        try:
            _api(
                self.kite.modify_order,
                variety=self.kite.VARIETY_REGULAR,
                order_id=order_id,
                order_type=self.kite.ORDER_TYPE_MARKET,
                market_protection=self.MARKET_PROTECTION,
                desc=f"modify market {context} {row.get('tradingsymbol')}",
                max_retries=API_ORDER_MAX_RETRIES,
            )
            log.warning(
                f"[ORDER MARKET] {context}: order_id={order_id}, "
                f"tag={row.get('tag')}, symbol={row.get('tradingsymbol')}, "
                f"pending_qty={pending}"
            )
            return True
        except Exception as exc:
            log.error(
                f"[ORDER MARKET FAIL] {context}: order_id={order_id}, "
                f"symbol={row.get('tradingsymbol')}: {exc}"
            )
            return False

    def _pending_strategy_orders(self, orders, symbols) -> list:
        """Return pending/open orders carrying this strategy's tag."""
        wanted_symbols = {str(symbol) for symbol in symbols}
        pending = []
        for row in orders:
            symbol = str(row.get("tradingsymbol") or "")
            status = str(row.get("status") or "").upper()
            if (
                symbol in wanted_symbols
                and self._is_strategy_tag(row.get("tag"))
                and (
                    status in self.PENDING_STATUSES
                    or self._pending_quantity(row) > 0
                )
            ):
                pending.append(row)
        return pending

    def _cancel_pending_strategy_orders(
        self,
        symbols,
        *,
        exact_tags=None,
        context: str,
    ) -> None:
        wanted_symbols = {str(symbol) for symbol in symbols}
        wanted_tags = {str(tag) for tag in exact_tags or []}
        try:
            orders = self._orders()
        except Exception as exc:
            log.error(
                f"[CLEANUP] {context}: cannot read orders before cancellation: {exc}"
            )
            return

        for row in orders:
            symbol = str(row.get("tradingsymbol") or "")
            tag = str(row.get("tag") or "")
            status = str(row.get("status") or "").upper()
            pending = self._pending_quantity(row)
            if symbol not in wanted_symbols:
                continue
            if wanted_tags:
                tag_match = tag in wanted_tags
            else:
                tag_match = self._is_strategy_tag(tag)
            if not tag_match:
                continue
            if status in self.PENDING_STATUSES or pending > 0:
                self._cancel_order(row, context)

    def _discover_ambiguous_order(
        self,
        *,
        tag: str,
        symbol: str,
        transaction_type: str,
        baseline_qty: int,
        context: str,
    ) -> bool:
        """
        Poll broker state after an ambiguous place_order response.

        True means an order or position change was discovered. False still does
        not authorise a duplicate submission; the tag remains quarantined and
        the logical pair will be cleaned up.
        """
        deadline = time.monotonic() + ORDER_DISCOVERY_TIMEOUT_SECONDS
        poll_no = 0
        while time.monotonic() < deadline:
            poll_no += 1
            try:
                orders = self._orders()
                positions = self._position_qty_map([symbol])
            except Exception as exc:
                log.warning(
                    f"[ORDER DISCOVERY] {context}: poll={poll_no}, tag={tag}, "
                    f"broker read failed: {exc}"
                )
                time.sleep(min(ORDER_CONFIRM_POLL_SECONDS, 0.5))
                continue

            matches = self._matching_orders(
                orders,
                tag=tag,
                symbol=symbol,
                transaction_type=transaction_type,
            )
            current = int(positions.get(symbol, 0))
            if matches or current != int(baseline_qty):
                log.warning(
                    f"[ORDER DISCOVERY] {context}: tag={tag}, symbol={symbol}, "
                    f"orders_found={len(matches)}, position={current}, "
                    f"baseline={baseline_qty}. Treating submission as potentially live."
                )
                return True
            time.sleep(ORDER_CONFIRM_POLL_SECONDS)

        log.error(
            f"[ORDER DISCOVERY TIMEOUT] {context}: no broker evidence for tag={tag}, "
            f"symbol={symbol} within {ORDER_DISCOVERY_TIMEOUT_SECONDS:.1f}s. "
            "The tag remains quarantined; no replacement order will be placed."
        )
        return False

    def _wait_for_pair_confirmation(
        self,
        *,
        symbols: dict,
        targets: dict,
        tags: dict,
        transaction_types: dict,
        expected_filled: dict,
        timeout_seconds: float,
        cutoff_time: Optional[dtime],
        context: str,
    ) -> tuple:
        deadline = time.monotonic() + max(0.1, float(timeout_seconds))
        modified_ids = set()
        last_snapshot = None
        poll_no = 0

        while time.monotonic() < deadline:
            if cutoff_time is not None and now_ist().time() >= cutoff_time:
                log.warning(
                    f"[ORDER CONFIRM CUTOFF] {context}: strategy exit time reached."
                )
                break

            poll_no += 1
            try:
                snapshot = self._pair_snapshot(
                    symbols=symbols,
                    targets=targets,
                    tags=tags,
                    transaction_types=transaction_types,
                    expected_filled=expected_filled,
                )
                last_snapshot = snapshot
            except Exception as exc:
                log.warning(
                    f"[ORDER CONFIRM] {context}: poll={poll_no}, broker read "
                    f"failed: {exc}"
                )
                time.sleep(ORDER_CONFIRM_POLL_SECONDS)
                continue

            if snapshot["overfilled"]:
                log.critical(
                    f"[ORDER OVERFILL] {context}: tagged filled quantity exceeded "
                    "the intended quantity. Immediate cleanup required."
                )
                return False, snapshot

            if snapshot["confirmed"]:
                details = ", ".join(
                    f"{leg} pos={row['current_qty']} fill={row['filled_qty']}"
                    for leg, row in snapshot["legs"].items()
                )
                log.info(
                    f"[ORDER CONFIRMED] {context}: both legs fully filled and "
                    f"position-confirmed ({details})."
                )
                return True, snapshot

            for leg, row in snapshot["legs"].items():
                for pending_order in row["pending_orders"]:
                    order_id = pending_order.get("order_id")
                    if order_id and order_id not in modified_ids:
                        self._modify_to_market(
                            pending_order,
                            f"{context} {leg}",
                        )
                        modified_ids.add(order_id)

            if poll_no == 1 or poll_no % 4 == 0:
                details = "; ".join(
                    f"{leg}:pos={row['current_qty']}/{row['target_qty']},"
                    f"filled={row['filled_qty']}/{row['expected_filled_qty']},"
                    f"pending={sum(self._pending_quantity(x) for x in row['pending_orders'])},"
                    f"failed={len(row['failed_orders'])}"
                    for leg, row in snapshot["legs"].items()
                )
                log.info(
                    f"[ORDER WAIT] {context}: poll={poll_no}; {details}"
                )

            time.sleep(ORDER_CONFIRM_POLL_SECONDS)

        log.warning(
            f"[ORDER CONFIRM TIMEOUT] {context}: both legs were not confirmed "
            f"within {timeout_seconds:.1f}s."
        )
        return False, last_snapshot

    def _reconcile_and_submit_missing(
        self,
        *,
        symbols: dict,
        baselines: dict,
        targets: dict,
        tags: dict,
        transaction_types: dict,
        expected_filled: dict,
        ambiguous_tags: set,
        context: str,
    ) -> bool:
        """
        Reconcile one execution cycle and submit only provably missing quantity.

        Returns False when an ambiguous submission prevents safe continuation.
        """
        snapshot = self._pair_snapshot(
            symbols=symbols,
            targets=targets,
            tags=tags,
            transaction_types=transaction_types,
            expected_filled=expected_filled,
        )
        if snapshot["confirmed"]:
            return True
        if snapshot["overfilled"]:
            return False

        for leg in ("PE", "CE"):
            row = snapshot["legs"][leg]
            symbol = symbols[leg]
            tag = tags[leg]
            current = int(row["current_qty"])
            target = int(targets[leg])

            if current == target:
                # Any remaining order could create a late overfill.
                for pending_order in row["pending_orders"]:
                    self._cancel_order(
                        pending_order,
                        f"{context} target already reached {leg}",
                    )
                continue

            delta = target - current
            required_txn = (
                self.kite.TRANSACTION_TYPE_BUY
                if delta > 0
                else self.kite.TRANSACTION_TYPE_SELL
            )
            required_qty = abs(delta)

            if str(required_txn).upper() != str(
                transaction_types[leg]
            ).upper():
                log.critical(
                    f"[ORDER DIRECTION ERROR] {context} {leg}: current={current}, "
                    f"target={target}, required={required_txn}, configured="
                    f"{transaction_types[leg]}."
                )
                return False

            if row["pending_orders"]:
                pending_total = sum(
                    self._pending_quantity(order)
                    for order in row["pending_orders"]
                )
                if pending_total > required_qty:
                    log.error(
                        f"[ORDER PENDING EXCESS] {context} {leg}: pending="
                        f"{pending_total} > missing={required_qty}; cancelling "
                        "before any further order."
                    )
                    for pending_order in row["pending_orders"]:
                        self._cancel_order(
                            pending_order,
                            f"{context} excess pending {leg}",
                        )
                else:
                    log.warning(
                        f"[ORDER REUSE] {context} {leg}: found existing tagged "
                        f"pending quantity={pending_total}; no duplicate order "
                        "will be placed."
                    )
                    for pending_order in row["pending_orders"]:
                        self._modify_to_market(
                            pending_order,
                            f"{context} existing {leg}",
                        )
                continue

            if tag in ambiguous_tags:
                log.error(
                    f"[ORDER QUARANTINED] {context} {leg}: tag={tag} had an "
                    "ambiguous submission. No replacement order is permitted."
                )
                return False

            self._validate_quantity(symbol, required_qty)
            price = self._marketable_limit_price(symbol, required_txn)
            _order_id, ambiguous = self._place_order_once(
                tradingsymbol=symbol,
                transaction_type=required_txn,
                quantity=required_qty,
                tag=tag,
                order_type=self.kite.ORDER_TYPE_LIMIT,
                price=price,
                context=f"{context} {leg}",
            )
            if ambiguous:
                ambiguous_tags.add(tag)
                self._discover_ambiguous_order(
                    tag=tag,
                    symbol=symbol,
                    transaction_type=required_txn,
                    baseline_qty=baselines[leg],
                    context=f"{context} {leg}",
                )
                # Do not place the other leg after unresolved acknowledgement.
                return False

        return True

    def _execute_pair_to_targets(
        self,
        *,
        symbols: dict,
        baselines: dict,
        targets: dict,
        tags: dict,
        transaction_types: dict,
        expected_filled: dict,
        max_attempts: int,
        cutoff_time: Optional[dtime],
        context: str,
    ) -> tuple:
        ambiguous_tags = set()
        last_snapshot = None

        for execution_attempt in range(1, int(max_attempts) + 1):
            if cutoff_time is not None and now_ist().time() >= cutoff_time:
                log.warning(
                    f"[ORDER ATTEMPT BLOCKED] {context}: exit cutoff reached "
                    f"before attempt {execution_attempt}/{max_attempts}."
                )
                break

            log.info(
                f"[ORDER ATTEMPT] {context}: execution attempt "
                f"{execution_attempt}/{max_attempts}."
            )
            try:
                safe_to_wait = self._reconcile_and_submit_missing(
                    symbols=symbols,
                    baselines=baselines,
                    targets=targets,
                    tags=tags,
                    transaction_types=transaction_types,
                    expected_filled=expected_filled,
                    ambiguous_tags=ambiguous_tags,
                    context=f"{context} attempt {execution_attempt}",
                )
            except Exception as exc:
                log.exception(
                    f"[ORDER ATTEMPT FAIL] {context}: reconciliation/submission "
                    f"failed on attempt {execution_attempt}/{max_attempts}: {exc}"
                )
                safe_to_wait = False

            confirmed, snapshot = self._wait_for_pair_confirmation(
                symbols=symbols,
                targets=targets,
                tags=tags,
                transaction_types=transaction_types,
                expected_filled=expected_filled,
                timeout_seconds=ORDER_CONFIRM_TIMEOUT_SECONDS,
                cutoff_time=cutoff_time,
                context=f"{context} attempt {execution_attempt}",
            )
            last_snapshot = snapshot
            if confirmed:
                return True, snapshot

            if snapshot and snapshot.get("overfilled"):
                break
            if not safe_to_wait and ambiguous_tags:
                log.error(
                    f"[ORDER RECONCILE ONLY] {context}: ambiguous tag(s) "
                    f"{sorted(ambiguous_tags)} prevent replacement orders. "
                    "Remaining execution attempts will inspect broker state only."
                )

            log.warning(
                f"[ORDER RETRY] {context}: attempt "
                f"{execution_attempt}/{max_attempts} not confirmed."
            )

        log.error(
            f"[ORDER FINAL FAILURE] {context}: both legs were not confirmed "
            f"after at most {max_attempts} execution attempt(s)."
        )
        return False, last_snapshot

    # ------------------------------------------------------------------
    # Cleanup and residual-position verification
    # ------------------------------------------------------------------
    def _positions_match_targets(
        self,
        targets_by_symbol: dict,
        *,
        timeout_seconds: float,
        context: str,
    ) -> bool:
        """
        Verify target quantities and absence of live strategy orders.

        Two consecutive matching snapshots are required. A flat position is not
        accepted while any tagged order can still fill later.
        """
        deadline = time.monotonic() + max(0.1, float(timeout_seconds))
        successful_reads = 0
        last_positions = None
        last_pending = None
        while time.monotonic() < deadline:
            try:
                current = self._position_qty_map(targets_by_symbol.keys())
                orders = self._orders()
                pending = self._pending_strategy_orders(
                    orders, targets_by_symbol.keys()
                )
                last_positions = current
                last_pending = pending
            except Exception as exc:
                log.warning(
                    f"[POSITION VERIFY] {context}: broker read failed: {exc}"
                )
                time.sleep(ORDER_CONFIRM_POLL_SECONDS)
                continue

            quantities_match = all(
                int(current.get(symbol, 0)) == int(target)
                for symbol, target in targets_by_symbol.items()
            )
            matched = quantities_match and not pending
            if matched:
                successful_reads += 1
                if successful_reads >= 2:
                    log.info(
                        f"[POSITION VERIFIED] {context}: positions={current}, "
                        "pending_strategy_orders=0."
                    )
                    return True
            else:
                successful_reads = 0
                if pending:
                    log.warning(
                        f"[POSITION VERIFY PENDING] {context}: "
                        f"{[(x.get('order_id'), x.get('tradingsymbol'), x.get('tag'), x.get('pending_quantity')) for x in pending]}"
                    )
            time.sleep(ORDER_CONFIRM_POLL_SECONDS)

        log.error(
            f"[POSITION VERIFY TIMEOUT] {context}: expected="
            f"{targets_by_symbol}, observed={last_positions}, "
            f"pending_strategy_orders="
            f"{[(x.get('order_id'), x.get('tradingsymbol'), x.get('tag')) for x in (last_pending or [])]}."
        )
        return False

    def _restore_position_targets(
        self,
        *,
        symbols: dict,
        targets: dict,
        attempt_idx: int,
        context: str,
        cleanup_tags: Optional[dict] = None,
    ) -> bool:
        if cleanup_tags is None:
            cleanup_stamp = now_ist().strftime("%y%m%d%H%M%S")
            cleanup_tags = {
                leg: self._execution_tag(
                    attempt_idx,
                    "C",
                    symbol,
                    execution_stamp=cleanup_stamp,
                )
                for leg, symbol in symbols.items()
            }
        ambiguous_tags = set()

        self._cancel_pending_strategy_orders(
            symbols.values(),
            context=f"{context} initial cancellation",
        )

        target_by_symbol = {
            symbols[leg]: int(targets[leg])
            for leg in symbols
        }
        if self._positions_match_targets(
            target_by_symbol,
            timeout_seconds=min(2.0, CLEANUP_CONFIRM_TIMEOUT_SECONDS),
            context=f"{context} pre-cleanup check",
        ):
            return True

        for cleanup_attempt in range(1, CLEANUP_MAX_ATTEMPTS + 1):
            log.warning(
                f"[CLEANUP ATTEMPT] {context}: "
                f"{cleanup_attempt}/{CLEANUP_MAX_ATTEMPTS}."
            )
            self._cancel_pending_strategy_orders(
                symbols.values(),
                context=f"{context} cleanup {cleanup_attempt}",
            )

            try:
                current = self._position_qty_map(symbols.values())
                cleanup_orders = self._orders()
                pending_by_symbol = {}
                for pending_order in self._pending_strategy_orders(
                    cleanup_orders, symbols.values()
                ):
                    pending_by_symbol.setdefault(
                        str(pending_order.get("tradingsymbol")), []
                    ).append(pending_order)
            except Exception as exc:
                log.error(
                    f"[CLEANUP READ FAIL] {context}: broker snapshots unavailable "
                    f"on attempt {cleanup_attempt}: {exc}"
                )
                continue

            if all(
                int(current.get(symbols[leg], 0)) == int(targets[leg])
                for leg in symbols
            ):
                if self._positions_match_targets(
                    target_by_symbol,
                    timeout_seconds=CLEANUP_CONFIRM_TIMEOUT_SECONDS,
                    context=f"{context} cleanup confirmation",
                ):
                    return True

            for leg in ("PE", "CE"):
                symbol = symbols[leg]
                current_qty = int(current.get(symbol, 0))
                target_qty = int(targets[leg])
                delta = target_qty - current_qty
                pending_orders = pending_by_symbol.get(symbol, [])
                if pending_orders:
                    if delta == 0:
                        log.warning(
                            f"[CLEANUP CANCEL LATE] {context} {leg}: target "
                            "quantity is already reached but a tagged order is "
                            "still pending."
                        )
                        for pending_order in pending_orders:
                            self._cancel_order(
                                pending_order,
                                f"{context} target reached {leg}",
                            )
                        continue

                    required_txn = (
                        self.kite.TRANSACTION_TYPE_BUY
                        if delta > 0
                        else self.kite.TRANSACTION_TYPE_SELL
                    )
                    same_direction = [
                        row for row in pending_orders
                        if str(row.get("transaction_type") or "").upper()
                        == str(required_txn).upper()
                    ]
                    opposite_direction = [
                        row for row in pending_orders
                        if row not in same_direction
                    ]
                    for pending_order in opposite_direction:
                        self._cancel_order(
                            pending_order,
                            f"{context} wrong-direction pending {leg}",
                        )

                    pending_total = sum(
                        self._pending_quantity(row)
                        for row in same_direction
                    )
                    if pending_total > abs(delta):
                        for pending_order in same_direction:
                            self._cancel_order(
                                pending_order,
                                f"{context} excess pending {leg}",
                            )
                    else:
                        for pending_order in same_direction:
                            self._modify_to_market(
                                pending_order,
                                f"{context} reuse pending {leg}",
                            )
                    # Never place another cleanup order in the same cycle while
                    # a prior order remains live or cancellation is uncertain.
                    continue

                if delta == 0:
                    continue

                tag = cleanup_tags[leg]
                if tag in ambiguous_tags:
                    log.error(
                        f"[CLEANUP QUARANTINED] {context} {leg}: tag={tag}; "
                        "waiting for broker reconciliation instead of duplicating."
                    )
                    continue

                txn = (
                    self.kite.TRANSACTION_TYPE_BUY
                    if delta > 0
                    else self.kite.TRANSACTION_TYPE_SELL
                )
                qty = abs(delta)
                self._validate_quantity(symbol, qty)
                _order_id, ambiguous = self._place_order_once(
                    tradingsymbol=symbol,
                    transaction_type=txn,
                    quantity=qty,
                    tag=tag,
                    order_type=self.kite.ORDER_TYPE_MARKET,
                    context=f"{context} cleanup {cleanup_attempt} {leg}",
                )
                if ambiguous:
                    ambiguous_tags.add(tag)
                    self._discover_ambiguous_order(
                        tag=tag,
                        symbol=symbol,
                        transaction_type=txn,
                        baseline_qty=current_qty,
                        context=f"{context} cleanup {leg}",
                    )

            if self._positions_match_targets(
                target_by_symbol,
                timeout_seconds=CLEANUP_CONFIRM_TIMEOUT_SECONDS,
                context=f"{context} cleanup attempt {cleanup_attempt}",
            ):
                self._cancel_pending_strategy_orders(
                    symbols.values(),
                    context=f"{context} post-cleanup cancellation",
                )
                return self._positions_match_targets(
                    target_by_symbol,
                    timeout_seconds=CLEANUP_CONFIRM_TIMEOUT_SECONDS,
                    context=f"{context} final verification",
                )

        self._cancel_pending_strategy_orders(
            symbols.values(),
            context=f"{context} final cancellation",
        )
        return self._positions_match_targets(
            target_by_symbol,
            timeout_seconds=CLEANUP_CONFIRM_TIMEOUT_SECONDS,
            context=f"{context} exhausted cleanup verification",
        )

    def _square_off_naked(self, tradingsymbol):
        """
        Compatibility helper used during startup reconciliation.

        It flattens one strategy symbol with finite attempts and proves the final
        zero position. Failure is fatal because continuing could compound an
        unknown naked exposure.
        """
        symbol = str(tradingsymbol)
        symbols = {
            "PE": symbol if symbol.endswith("PE") else "__NO_PE__",
            "CE": symbol if symbol.endswith("CE") else "__NO_CE__",
        }
        # Use a dedicated one-leg implementation to avoid querying fake symbols.
        current = self._position_qty_map([symbol]).get(symbol, 0)
        if int(current) == 0:
            log.info(f"[SAFETY] {symbol} already flat.")
            return

        leg = "CE" if symbol.endswith("CE") else "PE"
        one_symbols = {leg: symbol}
        one_targets = {leg: 0}
        tag = self._execution_tag(98, "C", symbol)
        ambiguous = False

        self._cancel_pending_strategy_orders(
            [symbol],
            context=f"startup flatten {symbol}",
        )
        for cleanup_attempt in range(1, CLEANUP_MAX_ATTEMPTS + 1):
            current = int(self._position_qty_map([symbol]).get(symbol, 0))
            pending_orders = self._pending_strategy_orders(
                self._orders(), [symbol]
            )
            if current == 0 and not pending_orders:
                break
            if pending_orders:
                required_txn = (
                    self.kite.TRANSACTION_TYPE_BUY
                    if current < 0
                    else self.kite.TRANSACTION_TYPE_SELL
                )
                for pending_order in pending_orders:
                    if (
                        current != 0
                        and str(pending_order.get("transaction_type") or "").upper()
                        == str(required_txn).upper()
                        and self._pending_quantity(pending_order) <= abs(current)
                    ):
                        self._modify_to_market(
                            pending_order,
                            f"startup flatten reuse {symbol}",
                        )
                    else:
                        self._cancel_order(
                            pending_order,
                            f"startup flatten cancel {symbol}",
                        )
            elif ambiguous:
                log.error(
                    f"[SAFETY] {symbol}: ambiguous cleanup tag={tag}; "
                    "not placing a duplicate."
                )
            else:
                txn = (
                    self.kite.TRANSACTION_TYPE_BUY
                    if current < 0
                    else self.kite.TRANSACTION_TYPE_SELL
                )
                _order_id, ambiguous = self._place_order_once(
                    tradingsymbol=symbol,
                    transaction_type=txn,
                    quantity=abs(current),
                    tag=tag,
                    order_type=self.kite.ORDER_TYPE_MARKET,
                    context=f"startup flatten {symbol} {cleanup_attempt}",
                )
                if ambiguous:
                    self._discover_ambiguous_order(
                        tag=tag,
                        symbol=symbol,
                        transaction_type=txn,
                        baseline_qty=current,
                        context=f"startup flatten {symbol}",
                    )
            if self._positions_match_targets(
                {symbol: 0},
                timeout_seconds=CLEANUP_CONFIRM_TIMEOUT_SECONDS,
                context=f"startup flatten {symbol}",
            ):
                log.warning(f"[SAFETY] Flattened naked leg {symbol}.")
                return

        raise ResidualPositionError(
            f"Could not verify zero residual position for naked leg {symbol}."
        )

    # ------------------------------------------------------------------
    # Public execution API
    # ------------------------------------------------------------------
    def open_short_straddle(
        self,
        pe_sym,
        ce_sym,
        pe_tok,
        ce_tok,
        qty,
        *,
        attempt_idx: int,
        cutoff_time: dtime,
    ) -> dict:
        """Sell and broker-confirm both legs, or restore the pre-entry book."""
        if self.paper:
            pe_fill = self._paper_fill(pe_tok, "SELL")
            ce_fill = self._paper_fill(ce_tok, "SELL")
            log.info(
                f"[PAPER ENTRY CONFIRMED] SELL {pe_sym}@{pe_fill:.2f}, "
                f"SELL {ce_sym}@{ce_fill:.2f}, qty={qty}."
            )
            return {
                "pe_fill": pe_fill,
                "ce_fill": ce_fill,
                "ok": True,
                "baseline_qty": {pe_sym: 0, ce_sym: 0},
            }

        self._validate_quantity(pe_sym, qty)
        self._validate_quantity(ce_sym, qty)
        symbols = {"PE": pe_sym, "CE": ce_sym}
        baselines_by_symbol = self._position_qty_map(symbols.values())
        baselines = {
            leg: int(baselines_by_symbol.get(symbol, 0))
            for leg, symbol in symbols.items()
        }
        targets = {
            leg: int(baselines[leg]) - int(qty)
            for leg in symbols
        }
        entry_stamp = now_ist().strftime("%y%m%d%H%M%S")
        tags = {
            leg: self._execution_tag(
                attempt_idx,
                "E",
                symbol,
                execution_stamp=entry_stamp,
            )
            for leg, symbol in symbols.items()
        }
        transaction_types = {
            leg: self.kite.TRANSACTION_TYPE_SELL
            for leg in symbols
        }
        expected_filled = {leg: int(qty) for leg in symbols}

        log.info(
            f"[ENTRY BASELINE] attempt={attempt_idx + 1}, "
            f"PE={pe_sym}:{baselines['PE']}->{targets['PE']}, "
            f"CE={ce_sym}:{baselines['CE']}->{targets['CE']}, "
            f"tags={tags}."
        )

        confirmed, snapshot = self._execute_pair_to_targets(
            symbols=symbols,
            baselines=baselines,
            targets=targets,
            tags=tags,
            transaction_types=transaction_types,
            expected_filled=expected_filled,
            max_attempts=ENTRY_EXECUTION_MAX_ATTEMPTS,
            cutoff_time=cutoff_time,
            context=f"ENTRY strategy attempt {attempt_idx + 1}",
        )
        if confirmed and snapshot:
            pe_fill = float(snapshot["legs"]["PE"]["average_price"])
            ce_fill = float(snapshot["legs"]["CE"]["average_price"])
            if pe_fill <= 0 or ce_fill <= 0:
                confirmed = False
                log.error(
                    "[ENTRY CONFIRM ERROR] Position/fill quantities matched, but "
                    f"average fills were invalid: PE={pe_fill}, CE={ce_fill}."
                )

        if confirmed:
            log.info(
                f"[ENTRY FINAL SUCCESS] attempt={attempt_idx + 1}: "
                f"PE={pe_sym}@{pe_fill:.2f}, CE={ce_sym}@{ce_fill:.2f}, "
                f"qty={qty}; both order and position data confirmed."
            )
            return {
                "pe_fill": pe_fill,
                "ce_fill": ce_fill,
                "ok": True,
                "baseline_qty": {
                    pe_sym: baselines["PE"],
                    ce_sym: baselines["CE"],
                },
                "tags": tags,
            }

        log.error(
            f"[ENTRY CLEANUP START] attempt={attempt_idx + 1}: entry was not "
            "fully confirmed. Cancelling pending orders and restoring baselines."
        )
        self._cancel_pending_strategy_orders(
            symbols.values(),
            exact_tags=tags.values(),
            context=f"failed entry {attempt_idx + 1}",
        )
        cleanup_ok = self._restore_position_targets(
            symbols=symbols,
            targets=baselines,
            attempt_idx=attempt_idx,
            context=f"failed entry {attempt_idx + 1}",
        )
        if not cleanup_ok:
            log.critical(
                f"[ENTRY CLEANUP FATAL] attempt={attempt_idx + 1}: residual "
                "position could not be ruled out. Trading must stop."
            )
            raise ResidualPositionError(
                f"Failed entry cleanup could not restore {pe_sym}/{ce_sym} "
                f"to baselines {baselines}."
            )

        log.warning(
            f"[ENTRY FINAL DEFER] attempt={attempt_idx + 1}: broker book was "
            "restored and verified. No position remains; defer to the next "
            "scheduled strategy attempt."
        )
        return {
            "pe_fill": 0.0,
            "ce_fill": 0.0,
            "ok": False,
            "cleanup_ok": True,
            "defer": True,
            "reason": "entry_not_confirmed_after_bounded_attempts",
        }

    def _fills_for_tags(
        self,
        *,
        symbol: str,
        transaction_type: str,
        tags,
    ) -> tuple:
        orders = self._orders()
        matches = []
        for tag in tags:
            matches.extend(
                self._matching_orders(
                    orders,
                    tag=tag,
                    symbol=symbol,
                    transaction_type=transaction_type,
                )
            )
        return self._weighted_average_fill(matches)

    def close_short_straddle(
        self,
        pe_sym,
        ce_sym,
        pe_tok,
        ce_tok,
        qty,
        *,
        attempt_idx: int,
        baseline_qty: Optional[dict] = None,
    ) -> dict:
        """Buy back and verify both legs with the same bounded safeguards."""
        if self.paper:
            pe_fill = self._paper_fill(pe_tok, "BUY")
            ce_fill = self._paper_fill(ce_tok, "BUY")
            log.info(
                f"[PAPER EXIT CONFIRMED] BUY {pe_sym}@{pe_fill:.2f}, "
                f"BUY {ce_sym}@{ce_fill:.2f}, qty={qty}."
            )
            return {"pe_fill": pe_fill, "ce_fill": ce_fill, "ok": True}

        self._validate_quantity(pe_sym, qty)
        self._validate_quantity(ce_sym, qty)
        symbols = {"PE": pe_sym, "CE": ce_sym}
        baseline_qty = baseline_qty or {pe_sym: 0, ce_sym: 0}
        targets = {
            leg: int(baseline_qty.get(symbol, 0))
            for leg, symbol in symbols.items()
        }
        current_by_symbol = self._position_qty_map(symbols.values())
        baselines = {
            leg: int(current_by_symbol.get(symbol, 0))
            for leg, symbol in symbols.items()
        }
        expected_filled = {
            leg: abs(int(targets[leg]) - int(baselines[leg]))
            for leg in symbols
        }
        exit_stamp = now_ist().strftime("%y%m%d%H%M%S")
        tags = {
            leg: self._execution_tag(
                attempt_idx,
                "X",
                symbol,
                execution_stamp=exit_stamp,
            )
            for leg, symbol in symbols.items()
        }
        transaction_types = {
            leg: (
                self.kite.TRANSACTION_TYPE_BUY
                if targets[leg] > baselines[leg]
                else self.kite.TRANSACTION_TYPE_SELL
            )
            for leg in symbols
        }

        # A position already at target requires no order. The generic exact-fill
        # confirmation therefore expects zero tagged fill for that leg.
        log.info(
            f"[EXIT BASELINE] attempt={attempt_idx + 1}, "
            f"PE={pe_sym}:{baselines['PE']}->{targets['PE']}, "
            f"CE={ce_sym}:{baselines['CE']}->{targets['CE']}, tags={tags}."
        )
        confirmed, snapshot = self._execute_pair_to_targets(
            symbols=symbols,
            baselines=baselines,
            targets=targets,
            tags=tags,
            transaction_types=transaction_types,
            expected_filled=expected_filled,
            max_attempts=EXIT_EXECUTION_MAX_ATTEMPTS,
            cutoff_time=None,
            context=f"EXIT strategy attempt {attempt_idx + 1}",
        )

        cleanup_stamp = now_ist().strftime("%y%m%d%H%M%S")
        cleanup_tags = {
            leg: self._execution_tag(
                attempt_idx,
                "C",
                symbol,
                execution_stamp=cleanup_stamp,
            )
            for leg, symbol in symbols.items()
        }
        if not confirmed:
            log.error(
                f"[EXIT CLEANUP START] attempt={attempt_idx + 1}: regular exit "
                "was not fully confirmed. Forcing and verifying target positions."
            )
            self._cancel_pending_strategy_orders(
                symbols.values(),
                context=f"failed exit {attempt_idx + 1}",
            )
            confirmed = self._restore_position_targets(
                symbols=symbols,
                targets=targets,
                attempt_idx=attempt_idx,
                context=f"failed exit {attempt_idx + 1}",
                cleanup_tags=cleanup_tags,
            )

        if not confirmed:
            log.critical(
                f"[EXIT CLEANUP FATAL] attempt={attempt_idx + 1}: residual "
                "strategy position could not be ruled out."
            )
            raise ResidualPositionError(
                f"Exit cleanup could not restore targets for {pe_sym}/{ce_sym}."
            )

        pe_fill, pe_filled = self._fills_for_tags(
            symbol=pe_sym,
            transaction_type=transaction_types["PE"],
            tags=[tags["PE"], cleanup_tags["PE"]],
        )
        ce_fill, ce_filled = self._fills_for_tags(
            symbol=ce_sym,
            transaction_type=transaction_types["CE"],
            tags=[tags["CE"], cleanup_tags["CE"]],
        )

        # Normal strategy exits should always have fills. A restart may find a
        # leg already flattened externally; use current LTP only for P&L
        # bookkeeping in that exceptional, already-flat case.
        if pe_fill <= 0:
            pe_fill = float(self.feed.get(pe_tok) or 0.0)
            log.warning(
                f"[EXIT FILL FALLBACK] {pe_sym}: no tagged average price found; "
                f"using current feed price {pe_fill:.2f} for accounting."
            )
        if ce_fill <= 0:
            ce_fill = float(self.feed.get(ce_tok) or 0.0)
            log.warning(
                f"[EXIT FILL FALLBACK] {ce_sym}: no tagged average price found; "
                f"using current feed price {ce_fill:.2f} for accounting."
            )
        if pe_fill <= 0 or ce_fill <= 0:
            raise RuntimeError(
                f"Exit positions are flat but usable fill prices are unavailable: "
                f"PE={pe_fill}, CE={ce_fill}."
            )

        log.info(
            f"[EXIT FINAL SUCCESS] attempt={attempt_idx + 1}: "
            f"PE={pe_sym}@{pe_fill:.2f} filled={pe_filled}, "
            f"CE={ce_sym}@{ce_fill:.2f} filled={ce_filled}; "
            "target positions verified."
        )
        return {
            "pe_fill": pe_fill,
            "ce_fill": ce_fill,
            "ok": True,
        }


# ===========================================================================
# 6) STRATEGY ENGINE  (B's per-day state machine, tick-driven)
# ===========================================================================
class LiveStraddleTrader:
    """Tick-driven executor for the v2 short-straddle day state machine."""

    def __init__(
        self,
        kite,
        feed: PriceFeed,
        broker: Broker,
        *,
        underlying_quote_key: str,
        part_symbol: str,
        strike_step: int,
        qty: int,
        expiry_date: date,
    ):
        self.kite = kite
        self.feed = feed
        self.broker = broker
        self.underlying_quote_key = underlying_quote_key
        self.part_symbol = part_symbol
        self.strike_step = int(strike_step)
        self.qty = int(qty)
        self.expiry_date = expiry_date

        self.entry_time = parse_hhmm(ENTRY_TIME_IST)
        self.exit_time = parse_hhmm(EXIT_TIME_IST)
        self.squareoff_time = self.exit_time  # compatibility with outer restart loop

        self.daily_realized_pnl = 0.0
        self.previous_entry_premium_per_unit: Optional[float] = None

        # WAITING | IN_POSITION | EXITING | EXITED | WAITING_REENTRY | DONE
        self.phase = "WAITING"
        self.attempt_idx = 0
        self.position = None
        self.reentry_target = None
        self.pending_exit_reason: Optional[str] = None
        # ENTERED | DEFER | BLOCK | STOP. Used only for operational handling
        # after a failed/blocked entry; it does not alter strategy thresholds.
        self.last_entry_outcome = "STOP"
        self._state_loaded_today = False

    # ------------------------------------------------------------------
    # Market-data and time helpers
    # ------------------------------------------------------------------
    def _underlying_ltp(self) -> float:
        response = _api(
            self.kite.ltp,
            [self.underlying_quote_key],
            desc="ltp underlying",
        )
        return float(response[self.underlying_quote_key]["last_price"])

    def _resolve_option(self, tradingsymbol: str):
        """Return (instrument_token, last_price) for an option symbol."""
        key = f"{self.broker.exchange}:{tradingsymbol}"
        info = _api(self.kite.ltp, [key], desc=f"ltp {tradingsymbol}")[key]
        return int(info["instrument_token"]), float(info["last_price"])

    def _sleep_until(self, target: dtime, label: str) -> bool:
        """Bounded wall-clock wait; never waits beyond the strategy exit time."""
        current_dt = now_ist()
        target_dt = current_dt.replace(
            hour=target.hour,
            minute=target.minute,
            second=target.second,
            microsecond=0,
        )
        exit_dt = current_dt.replace(
            hour=self.exit_time.hour,
            minute=self.exit_time.minute,
            second=self.exit_time.second,
            microsecond=0,
        )
        if target_dt > exit_dt:
            log.warning(
                f"[WAIT BLOCKED] {label}: target={target_dt.time()} is after "
                f"exit={exit_dt.time()}."
            )
            return False
        if current_dt >= target_dt:
            return current_dt < exit_dt

        log.info(
            f"[WAIT] Waiting until {target_dt.strftime('%H:%M:%S')} "
            f"({label}); hard deadline={exit_dt.strftime('%H:%M:%S')}."
        )
        while now_ist() < target_dt and now_ist() < exit_dt:
            remaining = min(
                (target_dt - now_ist()).total_seconds(),
                (exit_dt - now_ist()).total_seconds(),
            )
            if remaining <= 0:
                break
            time.sleep(
                min(
                    1.0,
                    max(0.05, MONITOR_POLL_SECONDS),
                    max(0.05, remaining),
                )
            )
        return now_ist() >= target_dt and now_ist() < exit_dt

    def _current_dte(self) -> int:
        return int((self.expiry_date - now_ist().date()).days)

    def _dte_allowed(self) -> bool:
        current_dte = self._current_dte()
        allowed = current_dte in ALLOWED_DTE
        if allowed:
            log.info(
                f"[DTE] expiry={self.expiry_date.isoformat()} current_dte={current_dte} "
                f"allowed={ALLOWED_DTE} -> PASS"
            )
            return True

        message = (
            f"[DTE] expiry={self.expiry_date.isoformat()} current_dte={current_dte} "
            f"allowed={ALLOWED_DTE} -> BLOCK"
        )
        if ENFORCE_DTE:
            log.warning(message)
            return False
        log.warning(message + " (ENFORCE_DTE=0, continuing by explicit override)")
        return True

    # ------------------------------------------------------------------
    # Restart-state persistence
    # ------------------------------------------------------------------
    def _today_str(self) -> str:
        return now_ist().date().isoformat()

    def _state_identity_matches(self, state: dict) -> bool:
        return (
            state.get("strategy_id") == STRATEGY_ID
            and state.get("part_symbol") == self.part_symbol
            and state.get("underlying_quote_key") == self.underlying_quote_key
            and state.get("mode") == ("PAPER" if self.broker.paper else "LIVE")
        )

    def _save_state(self) -> None:
        """Persist state atomically so a restart cannot read a partial file."""
        state = {
            "date": self._today_str(),
            "strategy_id": STRATEGY_ID,
            "part_symbol": self.part_symbol,
            "underlying_quote_key": self.underlying_quote_key,
            "expiry_date": self.expiry_date.isoformat(),
            "phase": self.phase,
            "attempt_idx": self.attempt_idx,
            "daily_realized_pnl": self.daily_realized_pnl,
            "previous_entry_premium_per_unit": self.previous_entry_premium_per_unit,
            "reentry_target": (
                self.reentry_target.strftime("%H:%M:%S")
                if self.reentry_target
                else None
            ),
            "pending_exit_reason": self.pending_exit_reason,
            "position": self.position,
            "mode": "PAPER" if self.broker.paper else "LIVE",
        }
        try:
            state_path = os.path.abspath(os.path.expanduser(STATE_FILE))
            state_dir = os.path.dirname(state_path)
            if state_dir:
                os.makedirs(state_dir, exist_ok=True)
            tmp = state_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as handle:
                json.dump(state, handle, indent=2, default=str)
            os.replace(tmp, state_path)
        except Exception as exc:
            log.warning(f"[STATE] Could not save state: {exc}")

    def _load_state(self) -> None:
        self.phase = "WAITING"
        self.attempt_idx = 0
        self.daily_realized_pnl = 0.0
        self.previous_entry_premium_per_unit = None
        self.position = None
        self.reentry_target = None
        self.pending_exit_reason = None
        self._state_loaded_today = False

        state_path = os.path.abspath(os.path.expanduser(STATE_FILE))
        if not os.path.exists(state_path):
            return

        try:
            with open(state_path, "r", encoding="utf-8") as handle:
                state = json.load(handle)
        except Exception as exc:
            log.warning(f"[STATE] Could not read state file: {exc}")
            return

        if state.get("date") != self._today_str():
            log.info("[STATE] Saved state is from another date; ignoring it.")
            return
        if not self._state_identity_matches(state):
            log.warning(
                "[STATE] Today's state belongs to another strategy, symbol or mode; "
                "ignoring it to prevent cross-run contamination."
            )
            return

        self._state_loaded_today = True
        self.phase = str(state.get("phase", "WAITING"))
        self.attempt_idx = int(state.get("attempt_idx", 0))
        self.daily_realized_pnl = float(state.get("daily_realized_pnl", 0.0))
        previous = state.get("previous_entry_premium_per_unit")
        self.previous_entry_premium_per_unit = (
            float(previous) if previous is not None else None
        )
        self.position = state.get("position")
        self.pending_exit_reason = state.get("pending_exit_reason")
        target = state.get("reentry_target")
        self.reentry_target = parse_hhmm(target) if target else None
        log.info(
            f"[STATE] Loaded: phase={self.phase}, attempt={self.attempt_idx + 1}, "
            f"day_net=Rs{self.daily_realized_pnl:,.0f}, "
            f"previous_premium={self.previous_entry_premium_per_unit}, "
            f"pending_exit={self.pending_exit_reason}, "
            f"position={'YES' if self.position else 'no'}"
        )

    # ------------------------------------------------------------------
    # Broker reconciliation
    # ------------------------------------------------------------------
    def _is_ss_option(self, symbol: str) -> bool:
        return bool(symbol) and symbol.startswith(self.part_symbol) and (
            symbol.endswith("CE") or symbol.endswith("PE")
        )

    def _ss_open_legs(self, net_positions) -> dict:
        legs = {}
        for row in net_positions:
            symbol = row.get("tradingsymbol", "")
            quantity = int(row.get("quantity") or 0)
            if quantity == 0 or not self._is_ss_option(symbol):
                continue
            sell_quantity = float(row.get("sell_quantity") or 0)
            sell_value = float(row.get("sell_value") or 0.0)
            sell_average = (
                sell_value / sell_quantity
                if sell_quantity
                else float(row.get("sell_price") or 0.0)
            )
            legs[symbol] = {
                "qty": quantity,
                "entry": sell_average,
                "avg": float(row.get("average_price") or 0.0),
            }
        return legs

    def _ss_day_realised(self, net_positions) -> float:
        total = 0.0
        for row in net_positions:
            if self._is_ss_option(row.get("tradingsymbol", "")):
                total += float(row.get("realised", 0.0) or 0.0)
        return total

    def _count_completed_entries(self, orders) -> int:
        """Count unique logical CE-entry tags, not raw retry orders."""
        completed_tags = set()
        for order in orders:
            symbol = str(order.get("tradingsymbol", ""))
            tag = str(order.get("tag") or "")
            is_entry_tag = (
                tag == str(oUtils.SS_ORDER_TAG)
                or (
                    self.broker._is_strategy_tag(tag)
                    and len(tag) >= 2
                    and tag[-2:-1] == "E"
                )
            )
            if (
                is_entry_tag
                and self._is_ss_option(symbol)
                and str(order.get("transaction_type", "")).upper() == "SELL"
                and symbol.endswith("CE")
                and str(order.get("status", "")).upper() == "COMPLETE"
            ):
                completed_tags.add(tag)
        return len(completed_tags)

    def _cancel_stale_ss_orders(self, orders) -> None:
        open_states = {
            "OPEN",
            "TRIGGER PENDING",
            "OPEN PENDING",
            "MODIFY PENDING",
            "VALIDATION PENDING",
            "AMO REQ RECEIVED",
            "PUT ORDER REQ RECEIVED",
        }
        for order in orders:
            symbol = str(order.get("tradingsymbol", ""))
            if not self.broker._is_strategy_tag(order.get("tag")) or not self._is_ss_option(symbol):
                continue
            if str(order.get("status", "")).upper() not in open_states:
                continue
            try:
                _api(
                    self.kite.cancel_order,
                    variety=self.kite.VARIETY_REGULAR,
                    order_id=order.get("order_id"),
                    desc=f"cancel stale {symbol}",
                    max_retries=API_ORDER_MAX_RETRIES,
                )
                log.warning(
                    f"[RECONCILE] Cancelled stale pending order {symbol} "
                    f"({order.get('order_id')})."
                )
            except Exception as exc:
                log.warning(f"[RECONCILE] Could not cancel {symbol}: {exc}")

    def _build_position_thresholds(
        self,
        attempt_idx: int,
        ce_entry: float,
        pe_entry: float,
        qty: int,
    ) -> dict:
        premium_sum = (float(ce_entry) + float(pe_entry)) * int(qty)
        return {
            "premium_sum": premium_sum,
            "stop_rupees": effective_stop_rupees(attempt_idx, premium_sum),
            "target_rupees": (
                PROFIT_TARGET_PCT * premium_sum if PROFIT_TARGET_PCT > 0 else None
            ),
            "protect_arm_rupees": (
                PROFIT_PROTECT_ARM_PCT * premium_sum
                if PROFIT_PROTECT_ARM_PCT > 0
                else None
            ),
            "protect_giveback_rupees": (
                PROFIT_PROTECT_GIVEBACK_PCT * premium_sum
                if PROFIT_PROTECT_GIVEBACK_PCT > 0
                else None
            ),
            "breakeven_arm_rupees": (
                BREAKEVEN_ARM_PCT * premium_sum if BREAKEVEN_ARM_PCT > 0 else None
            ),
            "breakeven_lock_rupees": BREAKEVEN_LOCK_PCT * premium_sum,
        }

    def _adopt_open_straddle(
        self,
        ce_symbol: str,
        pe_symbol: str,
        open_legs: dict,
        completed_entries: int,
    ) -> None:
        ce_leg = open_legs[ce_symbol]
        pe_leg = open_legs[pe_symbol]
        if abs(int(ce_leg["qty"])) != abs(int(pe_leg["qty"])):
            raise RuntimeError(
                f"Open CE/PE quantities differ: {ce_symbol}={ce_leg['qty']}, "
                f"{pe_symbol}={pe_leg['qty']}"
            )

        qty = abs(int(ce_leg["qty"])) or self.qty
        ce_entry = float(ce_leg["entry"] or ce_leg["avg"])
        pe_entry = float(pe_leg["entry"] or pe_leg["avg"])
        ce_token, _ = self._resolve_option(ce_symbol)
        pe_token, _ = self._resolve_option(pe_symbol)

        saved = self.position
        if saved and saved.get("ce_sym") == ce_symbol and saved.get("pe_sym") == pe_symbol:
            position = dict(saved)
            position["ce_tok"] = int(ce_token)
            position["pe_tok"] = int(pe_token)
            position["qty"] = qty
            position.setdefault("peak", 0.0)
            position.setdefault("protect_armed", False)
            position.setdefault("breakeven_armed", False)
            attempt_idx = int(position.get("attempt_idx", max(0, completed_entries - 1)))
            log.info(
                "[RECONCILE] Reusing saved v2 thresholds for the broker-confirmed "
                f"open straddle; peak=Rs{float(position.get('peak', 0.0)):,.0f}."
            )
        else:
            attempt_idx = max(self.attempt_idx, max(0, completed_entries - 1))
            thresholds = self._build_position_thresholds(
                attempt_idx, ce_entry, pe_entry, qty
            )
            position = {
                "attempt_idx": attempt_idx,
                "pe_sym": pe_symbol,
                "ce_sym": ce_symbol,
                "pe_tok": int(pe_token),
                "ce_tok": int(ce_token),
                "ce_entry": ce_entry,
                "pe_entry": pe_entry,
                "qty": qty,
                **thresholds,
                "peak": 0.0,
                "protect_armed": False,
                "breakeven_armed": False,
            }
            log.warning(
                "[RECONCILE] No matching saved v2 state; thresholds were rebuilt "
                "from broker fills and trailing state was reset."
            )

        self.position = position
        self.attempt_idx = attempt_idx
        self.previous_entry_premium_per_unit = float(ce_entry + pe_entry)
        self.phase = "IN_POSITION"
        log.warning(
            f"[RECONCILE] Adopted {ce_symbol}/{pe_symbol}, qty={qty}, "
            f"attempt={attempt_idx + 1}, stop=Rs{position['stop_rupees']:,.0f}."
        )

    def reconcile_on_startup(self) -> None:
        self._load_state()

        if self.broker.paper:
            log.info("[RECONCILE] Paper mode: using today's compatible state file only.")
            return

        try:
            orders = _api(self.kite.orders, desc="orders(startup)")
        except Exception as exc:
            raise RuntimeError(
                f"Live startup reconciliation cannot read the order book: {exc}"
            ) from exc

        self._cancel_stale_ss_orders(orders)

        # Re-read after cancellation. A flat position is not safe if an old
        # tagged order can still fill later.
        orders = _api(self.kite.orders, desc="orders(after stale cancellation)")
        remaining_pending = []
        for order in orders:
            symbol = str(order.get("tradingsymbol", ""))
            status = str(order.get("status", "")).upper()
            pending = int(order.get("pending_quantity") or 0)
            if (
                self.broker._is_strategy_tag(order.get("tag"))
                and self._is_ss_option(symbol)
                and (
                    status in self.broker.PENDING_STATUSES
                    or pending > 0
                )
            ):
                remaining_pending.append(
                    {
                        "order_id": order.get("order_id"),
                        "symbol": symbol,
                        "tag": order.get("tag"),
                        "status": status,
                        "pending": pending,
                    }
                )
        if remaining_pending:
            raise ResidualPositionError(
                "Startup reconciliation could not cancel all pending strategy "
                f"orders: {remaining_pending}"
            )

        completed_entries = self._count_completed_entries(orders)

        try:
            net_positions = _api(
                self.kite.positions, desc="positions(startup)"
            )["net"]
        except Exception as exc:
            raise RuntimeError(
                f"Live startup reconciliation cannot read positions: {exc}"
            ) from exc

        open_legs = self._ss_open_legs(net_positions)
        day_realised = self._ss_day_realised(net_positions)
        ce_symbols = sorted(symbol for symbol in open_legs if symbol.endswith("CE"))
        pe_symbols = sorted(symbol for symbol in open_legs if symbol.endswith("PE"))

        if len(ce_symbols) != len(pe_symbols):
            log.critical(
                f"[RECONCILE] Unpaired strategy legs detected: CE={ce_symbols}, "
                f"PE={pe_symbols}. Squaring all tagged open legs."
            )
            for symbol in sorted(open_legs):
                self.broker._square_off_naked(symbol)
            ce_symbols, pe_symbols, open_legs = [], [], {}

        if len(ce_symbols) > 1 or len(pe_symbols) > 1:
            raise RuntimeError(
                f"Multiple open straddles detected for prefix {self.part_symbol}: "
                f"CE={ce_symbols}, PE={pe_symbols}. Manual inspection required."
            )

        if ce_symbols and pe_symbols:
            self._adopt_open_straddle(
                ce_symbols[0], pe_symbols[0], open_legs, completed_entries
            )
        else:
            if self.position is not None:
                log.warning(
                    "[RECONCILE] State showed an open position but broker is flat; "
                    "clearing the saved position."
                )
                self.position = None
                if self.phase == "EXITING" and self.pending_exit_reason:
                    # Exit intent was persisted before order placement and the
                    # broker is now flat, so the exit completed while the process
                    # was down. Continue with the post-exit decision exactly once.
                    self.phase = "EXITED"
                elif self.phase == "IN_POSITION":
                    self.phase = "WAITING"
            if not self._state_loaded_today:
                if completed_entries > 0:
                    log.warning(
                        f"[RECONCILE] {completed_entries} prior entries, flat book and no "
                        "compatible state: treating the day as completed."
                    )
                    self.phase = "DONE"
                else:
                    self.phase = "WAITING"
                    self.attempt_idx = 0

        if not self._state_loaded_today and abs(day_realised) > 1e-9:
            log.warning(
                "[RECONCILE] Seeding realised P&L from broker positions. This value "
                "may differ from the backtest charge estimate."
            )
            self.daily_realized_pnl = float(day_realised)

        self._save_state()
        log.warning(
            f"[RECONCILE] Final state: phase={self.phase}, "
            f"attempt={self.attempt_idx + 1}, open={'YES' if self.position else 'no'}, "
            f"day_net=Rs{self.daily_realized_pnl:,.0f}."
        )

    # ------------------------------------------------------------------
    # Entry / monitoring / exit
    # ------------------------------------------------------------------
    def enter(self, attempt_idx: int) -> bool:
        self.last_entry_outcome = "STOP"
        if now_ist().time() >= self.exit_time:
            log.info("[ENTRY] Exit cutoff reached; no new position will be opened.")
            self.last_entry_outcome = "BLOCK"
            return False

        underlying_ltp = self._underlying_ltp()
        atm = round_to_step(underlying_ltp, self.strike_step)
        pe_symbol = f"{self.part_symbol}{atm}PE"
        ce_symbol = f"{self.part_symbol}{atm}CE"
        log.info(
            f"[ENTRY] attempt={attempt_idx + 1}, underlying={underlying_ltp:.2f}, "
            f"ATM={atm}, legs={ce_symbol}/{pe_symbol}"
        )

        try:
            pe_token, pe_api_ltp = self._resolve_option(pe_symbol)
            ce_token, ce_api_ltp = self._resolve_option(ce_symbol)
        except Exception as exc:
            log.error(
                f"[ENTRY PRECHECK FAIL] attempt={attempt_idx + 1}: option "
                f"resolution failed: {exc}. Deferring without placing orders."
            )
            self.last_entry_outcome = "DEFER"
            return False

        self.feed.subscribe([pe_token, ce_token])
        if not self.feed.wait_for([pe_token, ce_token], timeout=10):
            log.error(
                f"[ENTRY PRECHECK FAIL] attempt={attempt_idx + 1}: no WebSocket "
                "ticks for both legs. Deferring without placing orders."
            )
            self.feed.unsubscribe([pe_token, ce_token])
            self.last_entry_outcome = "DEFER"
            return False

        ce_ltp = self.feed.get(ce_token)
        pe_ltp = self.feed.get(pe_token)
        if ce_ltp is None:
            ce_ltp = ce_api_ltp
        if pe_ltp is None:
            pe_ltp = pe_api_ltp
        proposed_premium_per_unit = float(ce_ltp) + float(pe_ltp)

        if (
            attempt_idx > 0
            and REENTRY_MAX_PREMIUM_RATIO > 0
            and self.previous_entry_premium_per_unit
            and proposed_premium_per_unit
            > self.previous_entry_premium_per_unit * REENTRY_MAX_PREMIUM_RATIO
        ):
            threshold = (
                self.previous_entry_premium_per_unit * REENTRY_MAX_PREMIUM_RATIO
            )
            log.warning(
                f"[REENTRY GATE] No re-entry: current ATM premium "
                f"{proposed_premium_per_unit:.2f} > {REENTRY_MAX_PREMIUM_RATIO:.4f}x "
                f"previous {self.previous_entry_premium_per_unit:.2f} "
                f"(threshold {threshold:.2f})."
            )
            self.feed.unsubscribe([pe_token, ce_token])
            self.last_entry_outcome = "BLOCK"
            return False

        fills = self.broker.open_short_straddle(
            pe_symbol,
            ce_symbol,
            pe_token,
            ce_token,
            self.qty,
            attempt_idx=attempt_idx,
            cutoff_time=self.exit_time,
        )
        if not fills.get("ok"):
            self.feed.unsubscribe([pe_token, ce_token])
            if fills.get("cleanup_ok") and fills.get("defer"):
                self.last_entry_outcome = "DEFER"
                log.warning(
                    f"[ENTRY DEFERRED] attempt={attempt_idx + 1}: no residual "
                    "position remains. The next configured attempt may proceed."
                )
                return False
            raise RuntimeError(
                f"Entry failed without verified cleanup for "
                f"{ce_symbol}/{pe_symbol}."
            )

        ce_entry = float(fills["ce_fill"])
        pe_entry = float(fills["pe_fill"])
        thresholds = self._build_position_thresholds(
            attempt_idx, ce_entry, pe_entry, self.qty
        )
        self.previous_entry_premium_per_unit = ce_entry + pe_entry
        self.position = {
            "attempt_idx": attempt_idx,
            "pe_sym": pe_symbol,
            "ce_sym": ce_symbol,
            "pe_tok": int(pe_token),
            "ce_tok": int(ce_token),
            "ce_entry": ce_entry,
            "pe_entry": pe_entry,
            "qty": self.qty,
            "baseline_qty": fills.get(
                "baseline_qty", {pe_symbol: 0, ce_symbol: 0}
            ),
            **thresholds,
            "peak": 0.0,
            "protect_armed": False,
            "breakeven_armed": False,
        }
        self.phase = "IN_POSITION"
        self.last_entry_outcome = "ENTERED"
        self._save_state()

        log.info(
            f"[ENTRY] fills CE={ce_entry:.2f}, PE={pe_entry:.2f}, "
            f"premium=Rs{thresholds['premium_sum']:,.0f}, "
            f"stop=Rs{thresholds['stop_rupees']:,.0f}, "
            f"target={('Rs%.0f' % thresholds['target_rupees']) if thresholds['target_rupees'] else 'off'}, "
            f"protect_arm={('Rs%.0f' % thresholds['protect_arm_rupees']) if thresholds['protect_arm_rupees'] else 'off'}, "
            f"protect_giveback={('Rs%.0f' % thresholds['protect_giveback_rupees']) if thresholds['protect_giveback_rupees'] else 'off'}, "
            f"BE_arm={('Rs%.0f' % thresholds['breakeven_arm_rupees']) if thresholds['breakeven_arm_rupees'] else 'off'}, "
            f"BE_lock=Rs{thresholds['breakeven_lock_rupees']:,.0f}"
        )
        return True

    def monitor_and_exit(self) -> str:
        if not self.position:
            raise RuntimeError("monitor_and_exit called without an open position")

        position = self.position
        pe_token = int(position["pe_tok"])
        ce_token = int(position["ce_tok"])
        ce_entry = float(position["ce_entry"])
        pe_entry = float(position["pe_entry"])
        qty = int(position["qty"])

        stop_rupees = float(position["stop_rupees"])
        target_rupees = position.get("target_rupees")
        protect_arm_rupees = position.get("protect_arm_rupees")
        protect_giveback_rupees = position.get("protect_giveback_rupees")
        breakeven_arm_rupees = position.get("breakeven_arm_rupees")
        breakeven_lock_rupees = float(position.get("breakeven_lock_rupees", 0.0))

        peak = float(position.get("peak", 0.0))
        protect_armed = bool(position.get("protect_armed", False))
        breakeven_armed = bool(position.get("breakeven_armed", False))

        self.feed.subscribe([pe_token, ce_token])
        if not self.feed.wait_for([pe_token, ce_token], timeout=10):
            log.warning("[MONITOR] Initial ticks delayed; retaining the open position.")

        last_heartbeat = 0.0
        last_save = time.time()
        exit_reason = "TIME_EXIT"

        # This loop is bounded by the configured strategy exit time.
        while now_ist().time() < self.exit_time:
            ce_ltp = self.feed.get(ce_token)
            pe_ltp = self.feed.get(pe_token)
            if ce_ltp is None or pe_ltp is None:
                time.sleep(MONITOR_POLL_SECONDS)
                continue

            pnl = (
                (ce_entry - float(ce_ltp)) * qty
                + (pe_entry - float(pe_ltp)) * qty
            )
            peak = max(peak, pnl)

            if (
                protect_arm_rupees is not None
                and not protect_armed
                and peak >= float(protect_arm_rupees)
            ):
                protect_armed = True
                log.info(
                    f"[PROTECT] Armed at peak=Rs{peak:,.0f}; "
                    f"arm threshold=Rs{float(protect_arm_rupees):,.0f}."
                )

            if (
                breakeven_arm_rupees is not None
                and not breakeven_armed
                and peak >= float(breakeven_arm_rupees)
            ):
                breakeven_armed = True
                log.info(
                    f"[BREAKEVEN] Armed at peak=Rs{peak:,.0f}; "
                    f"new floor=Rs{breakeven_lock_rupees:,.0f}."
                )

            stop_floor = -stop_rupees
            if breakeven_armed:
                stop_floor = max(stop_floor, breakeven_lock_rupees)

            if time.time() - last_save >= 10:
                position["peak"] = peak
                position["protect_armed"] = protect_armed
                position["breakeven_armed"] = breakeven_armed
                self._save_state()
                last_save = time.time()

            if time.time() - last_heartbeat >= MONITOR_HEARTBEAT_SECONDS:
                trail_floor = (
                    peak - float(protect_giveback_rupees)
                    if protect_armed and protect_giveback_rupees is not None
                    else None
                )
                log.info(
                    f"[MONITOR] pnl=Rs{pnl:,.0f}, peak=Rs{peak:,.0f}, "
                    f"stop_floor=Rs{stop_floor:,.0f}, "
                    f"trail_floor={('Rs%.0f' % trail_floor) if trail_floor is not None else 'off'}, "
                    f"CE={float(ce_ltp):.2f}, PE={float(pe_ltp):.2f}"
                )
                last_heartbeat = time.time()

            # Same tie priority as the backtest: STOP, TARGET, PROTECT.
            if pnl <= stop_floor:
                exit_reason = "STOPLOSS"
                break
            if target_rupees is not None and pnl >= float(target_rupees):
                exit_reason = "PROFIT_TARGET"
                break
            if (
                protect_armed
                and protect_giveback_rupees is not None
                and pnl <= peak - float(protect_giveback_rupees)
            ):
                exit_reason = "PROFIT_PROTECT"
                break

            time.sleep(MONITOR_POLL_SECONDS)

        # Natural loop exhaustion means the hard time cutoff was reached.
        if now_ist().time() >= self.exit_time and exit_reason == "TIME_EXIT":
            log.info("[MONITOR] Strategy exit cutoff reached.")

        # Persist exit intent before placing orders. On a live restart, a flat
        # broker book plus phase=EXITING means the exit completed and only the
        # post-exit decision remains.
        self.phase = "EXITING"
        self.pending_exit_reason = exit_reason
        position["peak"] = peak
        position["protect_armed"] = protect_armed
        position["breakeven_armed"] = breakeven_armed
        self._save_state()

        close = self.broker.close_short_straddle(
            position["pe_sym"],
            position["ce_sym"],
            pe_token,
            ce_token,
            qty,
            attempt_idx=int(position.get("attempt_idx", self.attempt_idx)),
            baseline_qty=position.get("baseline_qty"),
        )
        if not close.get("ok"):
            position["peak"] = peak
            position["protect_armed"] = protect_armed
            position["breakeven_armed"] = breakeven_armed
            self._save_state()
            raise RuntimeError(
                f"Exit orders unresolved for {position['ce_sym']}/{position['pe_sym']}; "
                "position state retained for broker reconciliation."
            )

        ce_exit = float(close["ce_fill"])
        pe_exit = float(close["pe_fill"])
        gross = (ce_entry - ce_exit) * qty + (pe_entry - pe_exit) * qty
        charges = compute_estimated_trade_charges(
            ce_entry, pe_entry, ce_exit, pe_exit, qty
        )
        net = gross - charges
        self.daily_realized_pnl += net

        log.info(
            f"[EXIT] {exit_reason}: CE {ce_entry:.2f}->{ce_exit:.2f}, "
            f"PE {pe_entry:.2f}->{pe_exit:.2f}, gross=Rs{gross:,.0f}, "
            f"estimated_charges=Rs{charges:,.0f}, net=Rs{net:,.0f}, "
            f"day_net=Rs{self.daily_realized_pnl:,.0f}"
        )

        self.feed.unsubscribe([pe_token, ce_token])
        self.position = None
        self.phase = "EXITED"
        self.pending_exit_reason = exit_reason
        self._save_state()
        return exit_reason

    # ------------------------------------------------------------------
    # Day orchestration
    # ------------------------------------------------------------------
    def _handle_post_exit(self, exit_reason: str) -> bool:
        if (
            exit_reason in {"STOPLOSS", "PROFIT_PROTECT"}
            and self.attempt_idx < MAX_REATTEMPTS
        ):
            delay = reentry_delay_for_attempt(self.attempt_idx)
            self.attempt_idx += 1
            target_dt = now_ist() + timedelta(minutes=delay)
            self.reentry_target = target_dt.time().replace(microsecond=0)
            self.phase = "WAITING_REENTRY"
            self.pending_exit_reason = None
            self._save_state()
            log.info(
                f"[REENTRY] {exit_reason}; wait={delay} min; "
                f"next_attempt={self.attempt_idx + 1}; "
                f"target={self.reentry_target.strftime('%H:%M:%S')}"
            )
            if not self._sleep_until(
                self.reentry_target, f"re-entry {self.attempt_idx + 1}"
            ):
                return False
            self.phase = "WAITING"
            self.reentry_target = None
            self._save_state()
            return True

        self.pending_exit_reason = None
        self._save_state()
        log.info(f"[DAY] No re-entry after {exit_reason}.")
        return False

    def _handle_failed_entry_defer(self) -> bool:
        """
        Move a safely cleaned-up failed entry to the next configured attempt.

        This is execution-failure handling only. No synthetic P&L or exit signal
        is created, and no immediate order retry occurs after the broker-level
        four-attempt budget is exhausted.
        """
        if self.last_entry_outcome != "DEFER":
            return False
        if self.attempt_idx >= MAX_REATTEMPTS:
            log.warning(
                "[ENTRY DEFER] No later configured attempt remains today."
            )
            return False

        delay = reentry_delay_for_attempt(self.attempt_idx)
        self.attempt_idx += 1
        target_dt = now_ist() + timedelta(minutes=delay)
        self.reentry_target = target_dt.time().replace(microsecond=0)
        self.phase = "WAITING_REENTRY"
        self._save_state()
        log.warning(
            f"[ENTRY DEFER] Broker entry was safely abandoned; wait={delay} min, "
            f"next_attempt={self.attempt_idx + 1}, "
            f"target={self.reentry_target.strftime('%H:%M:%S')}."
        )
        if not self._sleep_until(
            self.reentry_target,
            f"deferred entry {self.attempt_idx + 1}",
        ):
            return False

        self.phase = "WAITING"
        self.reentry_target = None
        self.last_entry_outcome = "STOP"
        self._save_state()
        return True

    def _finish_day(self, reason: str = "completed") -> None:
        self.phase = "DONE"
        self.reentry_target = None
        self.pending_exit_reason = None
        self._save_state()
        log.info(
            f"[DAY DONE] reason={reason}; total estimated net P&L="
            f"Rs{self.daily_realized_pnl:,.0f}"
        )

    def _wait_for_first_entry(self) -> bool:
        now = now_ist()
        entry_dt = now.replace(
            hour=self.entry_time.hour,
            minute=self.entry_time.minute,
            second=0,
            microsecond=0,
        )
        grace_deadline = entry_dt + timedelta(seconds=ENTRY_GRACE_SECONDS)

        if now < entry_dt:
            return self._sleep_until(self.entry_time, "first entry")
        if now <= grace_deadline:
            if now > entry_dt:
                log.warning(
                    f"[ENTRY] Started {int((now-entry_dt).total_seconds())}s after "
                    "configured entry; within grace window."
                )
            return True

        log.warning(
            f"[ENTRY] Process started after the {ENTRY_GRACE_SECONDS}s entry grace "
            "window. The backtest has no equivalent late entry, so today is skipped."
        )
        return False

    def run_day(self):
        self.reconcile_on_startup()
        current_dte = self._current_dte()

        log.info("=" * 80)
        log.info(
            f"[DAY] {now_ist().date()} | mode={'PAPER' if self.broker.paper else 'LIVE'} | "
            f"entry={ENTRY_TIME_IST} | exit={EXIT_TIME_IST} | "
            f"expiry={self.expiry_date} | DTE={current_dte}"
        )
        log.info(
            f"[DAY] qty={self.qty}, step={self.strike_step}, "
            f"max_reentries={MAX_REATTEMPTS}, daily_loss=Rs{MAX_DAILY_LOSS_RUPEES:,.0f}, "
            f"premium_gate={REENTRY_MAX_PREMIUM_RATIO:.4f}x"
        )

        if self.phase == "DONE":
            log.info("[DAY] Compatible state says today is already complete.")
            return

        if self.position is None and not oUtils.is_trading_day(now_ist().date()):
            self._finish_day("exchange holiday/weekend")
            return

        if now_ist().time() >= self.exit_time:
            if self.position is not None:
                log.warning("[DAY] Past exit time with an open position; closing now.")
                self.monitor_and_exit()
            self._finish_day("past exit cutoff")
            return

        # An existing broker-confirmed position must be managed even if a config
        # or calendar error makes today's DTE fail.
        if self.position is None and not self._dte_allowed():
            self._finish_day("DTE blocked")
            return

        if self.position is not None:
            log.warning(
                f"[RESUME] Monitoring {self.position['ce_sym']}/"
                f"{self.position['pe_sym']} from attempt {self.attempt_idx + 1}."
            )
            exit_reason = self.monitor_and_exit()
            if not self._handle_post_exit(exit_reason):
                self._finish_day(f"exit={exit_reason}")
                return

        if self.phase == "EXITED" and self.pending_exit_reason:
            exit_reason = self.pending_exit_reason
            log.warning(
                f"[RESUME] Broker is flat after persisted exit={exit_reason}; "
                "continuing with the pending post-exit decision."
            )
            if not self._handle_post_exit(exit_reason):
                self._finish_day(f"exit={exit_reason}")
                return

        if self.phase == "WAITING_REENTRY" and self.reentry_target is not None:
            log.info(
                f"[RESUME] Waiting for saved re-entry time "
                f"{self.reentry_target.strftime('%H:%M:%S')}."
            )
            if not self._sleep_until(self.reentry_target, "saved re-entry"):
                self._finish_day("re-entry cutoff")
                return
            self.phase = "WAITING"
            self.reentry_target = None
            self._save_state()

        if self.attempt_idx == 0 and self.position is None and self.phase == "WAITING":
            if not self._wait_for_first_entry():
                self._finish_day("missed first-entry window")
                return

        # Total strategy attempt numbers are bounded by MAX_REATTEMPTS + 1.
        remaining_attempt_slots = max(
            0, (MAX_REATTEMPTS + 1) - int(self.attempt_idx)
        )
        for _day_cycle in range(remaining_attempt_slots):
            if (
                MAX_DAILY_LOSS_RUPEES > 0
                and self.daily_realized_pnl <= -MAX_DAILY_LOSS_RUPEES
            ):
                log.warning(
                    f"[BREAKER] Daily loss limit reached: "
                    f"Rs{self.daily_realized_pnl:,.0f} <= -Rs{MAX_DAILY_LOSS_RUPEES:,.0f}."
                )
                break

            if now_ist().time() >= self.exit_time:
                log.info("[DAY] Exit cutoff reached; no new attempt.")
                break

            if not self.enter(self.attempt_idx):
                if self._handle_failed_entry_defer():
                    continue
                break

            exit_reason = self.monitor_and_exit()
            if not self._handle_post_exit(exit_reason):
                break
        else:
            log.info(
                "[DAY] Finite strategy-attempt budget exhausted; no further "
                "entry is permitted."
            )

        self._finish_day("strategy complete")


# ===========================================================================
# 7) MAIN
# ===========================================================================
def _validate_strategy_config() -> None:
    """Fail before connecting to Kite when the strategy configuration is unsafe."""
    entry = parse_hhmm(ENTRY_TIME_IST)
    exit_time = parse_hhmm(EXIT_TIME_IST)
    if not (SESSION_START_IST <= entry < exit_time <= SESSION_END_IST):
        raise ValueError(
            f"Require {SESSION_START_IST.strftime('%H:%M')} <= ENTRY_TIME_IST "
            f"< EXIT_TIME_IST <= {SESSION_END_IST.strftime('%H:%M')}; got "
            f"{ENTRY_TIME_IST} and {EXIT_TIME_IST}."
        )
    if (
        not EXECUTION_TAG_PREFIX
        or len(EXECUTION_TAG_PREFIX) > 4
        or not EXECUTION_TAG_PREFIX.isalnum()
    ):
        raise ValueError(
            "EXECUTION_TAG_PREFIX must be 1-4 alphanumeric characters."
        )
    if UNDERLYING_SELECTION_MODE not in {"AUTO_DTE", "MANUAL"}:
        raise ValueError(
            "UNDERLYING_SELECTION_MODE must be AUTO_DTE or MANUAL; got "
            f"{UNDERLYING_SELECTION_MODE!r}."
        )
    if UNDERLYING_SELECTION_MODE == "AUTO_DTE" and os.getenv("PART_SYMBOL", "").strip():
        raise ValueError(
            "PART_SYMBOL manual override cannot be used with AUTO_DTE. Clear "
            "PART_SYMBOL/PART_SYMBOL_EXPIRY or set UNDERLYING_SELECTION_MODE=MANUAL."
        )
    if ENTRY_GRACE_SECONDS < 0:
        raise ValueError("ENTRY_GRACE_SECONDS cannot be negative.")
    if not ALLOWED_DTE or any(dte < 0 for dte in ALLOWED_DTE):
        raise ValueError(f"ALLOWED_DTE must contain non-negative integers: {ALLOWED_DTE}")
    if not LOSS_LIMIT_RUPEES_BY_ATTEMPT or any(
        value < 0 for value in LOSS_LIMIT_RUPEES_BY_ATTEMPT
    ):
        raise ValueError("LOSS_LIMIT_RUPEES_BY_ATTEMPT must contain non-negative values.")
    if MAX_LOSS_LIMIT_RUPEES_BY_ATTEMPT < 0:
        raise ValueError("MAX_LOSS_LIMIT_RUPEES_BY_ATTEMPT cannot be negative.")
    if MAX_DAILY_LOSS_RUPEES < 0:
        raise ValueError("MAX_DAILY_LOSS_RUPEES cannot be negative.")
    if MAX_REATTEMPTS < 0:
        raise ValueError("MAX_REATTEMPTS cannot be negative.")
    if any(delay < 0 for delay in REENTRY_DELAY_BY_ATTEMPT):
        raise ValueError("REENTRY_DELAY_BY_ATTEMPT cannot contain negative values.")
    if REENTRY_MAX_PREMIUM_RATIO < 0:
        raise ValueError("REENTRY_MAX_PREMIUM_RATIO cannot be negative.")
    if BREAKEVEN_LOCK_PCT > BREAKEVEN_ARM_PCT and BREAKEVEN_ARM_PCT > 0:
        raise ValueError(
            "BREAKEVEN_LOCK_PCT cannot exceed BREAKEVEN_ARM_PCT; that would "
            "demand more locked profit than was required to arm the ratchet."
        )
    if MONITOR_POLL_SECONDS <= 0:
        raise ValueError("MONITOR_POLL_SECONDS must be greater than zero.")
    if OPTION_TICK <= 0:
        raise ValueError("OPTION_TICK must be greater than zero.")
    if API_MAX_RETRIES <= 0 or API_ORDER_MAX_RETRIES <= 0:
        raise ValueError(
            "API_MAX_RETRIES and API_ORDER_MAX_RETRIES must be finite positive integers."
        )
    if not (1 <= ENTRY_EXECUTION_MAX_ATTEMPTS <= 4):
        raise ValueError(
            "ENTRY_EXECUTION_MAX_ATTEMPTS must be between 1 and 4."
        )
    if not (1 <= EXIT_EXECUTION_MAX_ATTEMPTS <= 4):
        raise ValueError(
            "EXIT_EXECUTION_MAX_ATTEMPTS must be between 1 and 4."
        )
    if not (1 <= CLEANUP_MAX_ATTEMPTS <= 4):
        raise ValueError("CLEANUP_MAX_ATTEMPTS must be between 1 and 4.")
    if ORDER_CONFIRM_TIMEOUT_SECONDS <= 0:
        raise ValueError("ORDER_CONFIRM_TIMEOUT_SECONDS must be positive.")
    if ORDER_CONFIRM_POLL_SECONDS <= 0:
        raise ValueError("ORDER_CONFIRM_POLL_SECONDS must be positive.")
    if ORDER_DISCOVERY_TIMEOUT_SECONDS <= 0:
        raise ValueError("ORDER_DISCOVERY_TIMEOUT_SECONDS must be positive.")
    if CLEANUP_CONFIRM_TIMEOUT_SECONDS <= 0:
        raise ValueError("CLEANUP_CONFIRM_TIMEOUT_SECONDS must be positive.")
    if PROCESS_MAX_RESTARTS < 0:
        raise ValueError("PROCESS_MAX_RESTARTS cannot be negative.")
    if PROCESS_RESTART_DELAY_SECONDS < 0:
        raise ValueError("PROCESS_RESTART_DELAY_SECONDS cannot be negative.")


def _validate_live_safety() -> None:
    if PAPER_TRADING:
        return
    if LIVE_TRADING_CONFIRM != LIVE_CONFIRM_PHRASE:
        raise RuntimeError(
            "PAPER_TRADING=0 was requested, but LIVE_TRADING_CONFIRM does not "
            f"equal {LIVE_CONFIRM_PHRASE!r}. Real orders are blocked."
        )


def main():
    _validate_strategy_config()
    _validate_live_safety()

    log.info("#" * 80)
    log.info(
        f"[BOOT] {STRATEGY_ID} starting in "
        f"{'PAPER' if PAPER_TRADING else 'LIVE'} mode."
    )
    log.info(f"[BOOT] Config={PROPERTY_FILE_PATH}")
    if PAPER_TRADING:
        log.warning("[BOOT] PAPER SAFETY: no exchange orders will be placed.")
    else:
        log.critical("[BOOT] LIVE MODE CONFIRMED: real exchange orders can be placed.")

    if UNDERLYING_SELECTION_MODE == "AUTO_DTE":
        candidates = oUtils.get_dte_candidates()
        for row in candidates:
            log.info(
                f"[SELECT] {row['underlying']}: expiry={row['expiry']}, "
                f"DTE={row['dte']}, part={row['part_symbol']}"
            )
        selected_choice = oUtils.select_choice_for_allowed_dte(ALLOWED_DTE)
        if selected_choice is None:
            log.info(
                f"[SELECT] Neither NIFTY nor SENSEX has DTE in {ALLOWED_DTE}; "
                "no trade today."
            )
            return
    else:
        selected_choice = oUtils.get_selected_choice()
        log.warning(
            f"[SELECT] MANUAL mode uses .env choice={selected_choice}; the "
            "backtest's automatic one-underlying-per-day selection is bypassed."
        )

    kite = _api(oUtils.intialize_kite_api, desc="kite session init")
    log.info("[BOOT] Kite session initialised.")

    (
        underlying_exchange,
        underlying,
        options_exchange,
        part_symbol,
        quantity,
        strike_multiple,
        _stoploss_points,
        _minimum_lots,
        _long_straddle_distance,
    ) = _api(
        oUtils.get_instruments_for_choice,
        kite,
        selected_choice,
        desc="get_instruments_for_choice",
    )

    part_symbol, expiry_date = oUtils.get_part_symbol_and_expiry(selected_choice)
    part_symbol = part_symbol.replace(":", "")
    underlying_quote_key = underlying_exchange + underlying

    log.info(
        f"[BOOT] underlying={underlying_quote_key}, options_exchange={options_exchange}, "
        f"part_symbol={part_symbol}, expiry={expiry_date}, qty={quantity}, "
        f"strike_step={strike_multiple}"
    )
    log.info(
        f"[BOOT] stop_pct={LOSS_LIMIT_RUPEES_BY_ATTEMPT}, "
        f"stop_cap=Rs{MAX_LOSS_LIMIT_RUPEES_BY_ATTEMPT:,.0f}, "
        f"protect_arm={PROFIT_PROTECT_ARM_PCT:.2%}, "
        f"protect_giveback={PROFIT_PROTECT_GIVEBACK_PCT:.2%}, "
        f"breakeven={BREAKEVEN_ARM_PCT:.2%}->{BREAKEVEN_LOCK_PCT:.2%}, "
        f"target={PROFIT_TARGET_PCT:.2%}, reentry_delays={REENTRY_DELAY_BY_ATTEMPT}, "
        f"premium_gate={REENTRY_MAX_PREMIUM_RATIO:.4f}x, allowed_dte={ALLOWED_DTE}, "
        f"selection_mode={UNDERLYING_SELECTION_MODE}"
    )

    api_key = getattr(kite, "api_key", None) or getattr(
        oUtils, "KITE_API_KEY", None
    )
    access_token = getattr(kite, "access_token", None) or getattr(
        oUtils, "KITE_ACCESS_CODE", None
    )
    if not api_key or not access_token:
        raise RuntimeError(
            "KiteTicker requires api_key and access_token, but they could not be "
            "read from the Kite object or OptionTradeUtils_env."
        )

    feed = PriceFeed(api_key, access_token)
    feed.start()

    broker = Broker(kite, feed, options_exchange, paper=PAPER_TRADING)
    trader = LiveStraddleTrader(
        kite,
        feed,
        broker,
        underlying_quote_key=underlying_quote_key,
        part_symbol=part_symbol,
        strike_step=int(strike_multiple),
        qty=int(quantity),
        expiry_date=expiry_date,
    )

    try:
        for restart_no in range(PROCESS_MAX_RESTARTS + 1):
            try:
                trader.run_day()
                break
            except KeyboardInterrupt:
                log.warning("[SHUTDOWN] Interrupted by user.")
                break
            except ResidualPositionError as exc:
                log.critical(
                    f"[FATAL EXECUTION] {exc} Automatic trading is stopped; "
                    "inspect Zerodha positions immediately."
                )
                break
            except Exception as exc:
                log.exception(
                    f"[RESILIENCE] run_day failed on process attempt "
                    f"{restart_no + 1}/{PROCESS_MAX_RESTARTS + 1}: {exc}"
                )
                if now_ist().time() >= trader.exit_time:
                    log.info("[RESILIENCE] Past exit cutoff; not restarting.")
                    break
                if restart_no >= PROCESS_MAX_RESTARTS:
                    log.critical(
                        "[RESILIENCE] Finite process restart budget exhausted. "
                        "Automatic trading is stopped."
                    )
                    break
                log.warning(
                    f"[RESILIENCE] Reconcile/restart in "
                    f"{PROCESS_RESTART_DELAY_SECONDS:.1f}s."
                )
                if PROCESS_RESTART_DELAY_SECONDS > 0:
                    time.sleep(PROCESS_RESTART_DELAY_SECONDS)
    finally:
        feed.stop()
        log.info("[SHUTDOWN] Feed closed.")


if __name__ == "__main__":
    main()
