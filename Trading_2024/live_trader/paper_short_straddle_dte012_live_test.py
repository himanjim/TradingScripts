"""
Paper-only live-market validator for the ATM short-straddle on DTE-0, DTE-1, or DTE-2.

Purpose
-------
This program does NOT place broker orders. It observes live Kite Connect quotes,
simulates executable fills from five-level bid/ask depth, runs the optimized
NIFTY/SENSEX state machine on configured DTE-0/DTE-1/DTE-2 days, and writes a complete audit trail.

The central test is deliberately stricter than the minute-bar backtest:

* short entries are valued from BUY depth (the bid side), not LTP;
* exits are valued from SELL depth (the ask side), not independent candle lows;
* CE and PE quotes must be fresh and close in time;
* configured slippage and depth shortfall penalties are applied;
* estimated Zerodha/statutory charges are deducted from realised P&L;
* profit targets may trigger a fresh ATM entry after the configured target delay;
* every raw tick, strategy evaluation, simulated fill and trade is persisted;
* the nearest listed expiry is traded only when its calendar DTE is configured
  (defaults: 0, 1, or 2); otherwise the program exits without taking a trade.

Dependencies
------------
    pip install kiteconnect pytz pandas openpyxl

The existing OptionTradeUtils_env.py is used only to initialise the authenticated
Kite session. No place_order/modify_order/cancel_order function exists here.

Usage
-----
    python paper_short_straddle_dte012_live_test.py
    python paper_short_straddle_dte012_live_test.py --self-test

Keep paper_short_straddle_dte012_config.properties beside this script, or set:
    STRADDLE_PAPER_CONFIG=<full path to property file>
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import os
import queue
import shutil
import sys
import threading
import time
import traceback
from dataclasses import asdict, dataclass
from datetime import date, datetime, time as dtime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pytz

try:
    from kiteconnect import KiteTicker
except Exception:  # Imported lazily again in main if required.
    KiteTicker = None  # type: ignore

try:
    import Trading_2024.OptionTradeUtils as oUtils
except Exception:
    oUtils = None  # type: ignore


# =============================================================================
# 0. CONFIGURATION
# =============================================================================

def _load_property_file() -> str:
    cfg_path = os.getenv(
        "STRADDLE_PAPER_CONFIG",
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "paper_short_straddle_dte012_config.properties",
        ),
    )
    if not os.path.exists(cfg_path):
        print(f"[CONFIG] Property file not found: {cfg_path}; built-in defaults apply.")
        return cfg_path

    loaded = 0
    with open(cfg_path, "r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith(("#", ";")) or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key, value = key.strip(), value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
                value = value[1:-1]
            if key and key not in os.environ:
                os.environ[key] = value
                loaded += 1
    print(f"[CONFIG] Loaded {loaded} setting(s) from {cfg_path}")
    return cfg_path


PROPERTY_FILE_PATH = _load_property_file()
IST = pytz.timezone("Asia/Kolkata")
BUILD_ID = "paper-dte012-depth-validator-v1.1-20260724"


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    try:
        return float(raw) if raw is not None and raw.strip() else float(default)
    except Exception:
        return float(default)


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    try:
        return int(round(float(raw))) if raw is not None and raw.strip() else int(default)
    except Exception:
        return int(default)


def _csv_floats(raw: Optional[str], default: Sequence[float]) -> Tuple[float, ...]:
    if raw is None or not raw.strip():
        return tuple(float(x) for x in default)
    try:
        values = [float(x) for x in raw.replace(" ", "").split(",") if x]
        return tuple(values) if values else tuple(float(x) for x in default)
    except Exception:
        return tuple(float(x) for x in default)


def _csv_ints(raw: Optional[str], default: Sequence[int]) -> Tuple[int, ...]:
    if raw is None or not raw.strip():
        return tuple(int(x) for x in default)
    try:
        values = [int(round(float(x))) for x in raw.replace(" ", "").split(",") if x]
        return tuple(values) if values else tuple(int(x) for x in default)
    except Exception:
        return tuple(int(x) for x in default)


def parse_hhmm(value: str) -> dtime:
    hh, mm = value.strip().split(":", 1)
    return dtime(hour=int(hh), minute=int(mm))


def now_ist() -> datetime:
    return datetime.now(IST)


def iso_ist(dt: Optional[datetime] = None) -> str:
    return (dt or now_ist()).isoformat(timespec="milliseconds")


def round_to_step(value: float, step: int) -> int:
    return int(round(float(value) / int(step)) * int(step))


def adverse_round(price: float, side: str, tick_size: float) -> float:
    """Round against the paper trader: SELL down, BUY up."""
    if tick_size <= 0:
        return round(float(price), 2)
    ticks = float(price) / tick_size
    rounded = math.floor(ticks + 1e-12) if side.upper() == "SELL" else math.ceil(ticks - 1e-12)
    return round(max(tick_size, rounded * tick_size), 2)


# This executable is intentionally incapable of sending orders.
PAPER_ONLY = _env_bool("PAPER_ONLY", True)
if not PAPER_ONLY:
    raise RuntimeError(
        "PAPER_ONLY must remain 1. This validator contains no live-order mode."
    )

ALLOWED_DTE = tuple(sorted(set(_csv_ints(os.getenv("ALLOWED_DTE"), [0, 1, 2]))))
if not ALLOWED_DTE:
    raise ValueError("ALLOWED_DTE must contain at least one non-negative DTE value")
if any(dte < 0 for dte in ALLOWED_DTE):
    raise ValueError(f"ALLOWED_DTE cannot contain negative values: {ALLOWED_DTE}")
TRADE_SELECTION = os.getenv("TRADE_SELECTION", "NEAREST_EXPIRY_ONE").strip().upper()
TRADE_PRIORITY = tuple(
    x.strip().upper()
    for x in os.getenv("TRADE_PRIORITY", "NIFTY,SENSEX").split(",")
    if x.strip()
)
LATE_START_MODE = os.getenv("LATE_START_MODE", "SKIP").strip().upper()

OUTPUT_ROOT = Path(
    os.getenv(
        "OUTPUT_ROOT",
        str(Path.home() / "paper_straddle_live_tests"),
    )
).expanduser()
STATE_FILE = Path(
    os.getenv(
        "STATE_FILE",
        str(OUTPUT_ROOT / "paper_short_straddle_state.json"),
    )
).expanduser()

MONITOR_POLL_SECONDS = _env_float("MONITOR_POLL_SECONDS", 0.20)
HEARTBEAT_SECONDS = _env_float("HEARTBEAT_SECONDS", 5.0)
STATE_SAVE_SECONDS = _env_float("STATE_SAVE_SECONDS", 5.0)
QUOTE_WAIT_SECONDS = _env_float("QUOTE_WAIT_SECONDS", 15.0)
MAX_QUOTE_AGE_SECONDS = _env_float("MAX_QUOTE_AGE_SECONDS", 2.5)
MAX_LEG_QUOTE_SKEW_MS = _env_float("MAX_LEG_QUOTE_SKEW_MS", 1500.0)
ENTRY_SLIPPAGE_TICKS = _env_int("ENTRY_SLIPPAGE_TICKS", 1)
EXIT_SLIPPAGE_TICKS = _env_int("EXIT_SLIPPAGE_TICKS", 1)
DEPTH_SHORTFALL_PENALTY_TICKS = _env_int("DEPTH_SHORTFALL_PENALTY_TICKS", 4)
REQUIRE_FULL_DEPTH_AT_ENTRY = _env_bool("REQUIRE_FULL_DEPTH_AT_ENTRY", True)
ALLOW_LTP_FALLBACK = _env_bool("ALLOW_LTP_FALLBACK", False)
LOG_RAW_TICKS = _env_bool("LOG_RAW_TICKS", True)
GENERATE_EXCEL_ON_EXIT = _env_bool("GENERATE_EXCEL_ON_EXIT", True)
RESUME_TODAY_STATE = _env_bool("RESUME_TODAY_STATE", True)

API_MAX_RETRIES = _env_int("API_MAX_RETRIES", 0)  # 0 = unlimited for reads
API_RETRY_BACKOFF_SECONDS = _env_float("API_RETRY_BACKOFF_SECONDS", 2.0)
API_RETRY_BACKOFF_MAX_SECONDS = _env_float("API_RETRY_BACKOFF_MAX_SECONDS", 30.0)

# Current charge model defaults. These remain configurable because statutory and
# exchange charges can change.
BROKERAGE_PER_ORDER = _env_float("BROKERAGE_PER_ORDER", 20.0)
STT_SELL_PCT = _env_float("STT_SELL_PCT", 0.0015)
NFO_TXN_PCT = _env_float("NFO_TXN_PCT", 0.0003553)
BFO_TXN_PCT = _env_float("BFO_TXN_PCT", 0.0003250)
SEBI_PER_CRORE = _env_float("SEBI_PER_CRORE", 10.0)
STAMP_BUY_PCT = _env_float("STAMP_BUY_PCT", 0.00003)
NFO_IPFT_PER_CRORE = _env_float("NFO_IPFT_PER_CRORE", 0.01)
GST_PCT = _env_float("GST_PCT", 0.18)


@dataclass(frozen=True)
class UnderlyingSpec:
    name: str
    spot_key: str
    options_exchange: str
    option_name: str
    strike_step: int
    qty_units: int


@dataclass(frozen=True)
class StrategySettings:
    entry_time: dtime
    exit_time: dtime
    reentry_cutoff_time: dtime
    stop_pct_by_attempt: Tuple[float, ...]
    stop_cap_rupees: float
    profit_protect_pct: float
    profit_target_pct: float
    profit_target_reentry_enabled: bool
    target_reentry_delay_by_attempt: Tuple[int, ...]
    risk_reentry_delay_by_attempt: Tuple[int, ...]
    max_reattempts: int
    max_daily_loss_rupees: float

    def stop_pct(self, attempt_idx: int) -> float:
        if not self.stop_pct_by_attempt:
            return 0.0
        return float(self.stop_pct_by_attempt[min(attempt_idx, len(self.stop_pct_by_attempt) - 1)])

    def target_delay(self, attempt_idx: int) -> int:
        if not self.target_reentry_delay_by_attempt:
            return 0
        return int(self.target_reentry_delay_by_attempt[min(attempt_idx, len(self.target_reentry_delay_by_attempt) - 1)])

    def risk_delay(self, attempt_idx: int) -> int:
        if not self.risk_reentry_delay_by_attempt:
            return 0
        return int(self.risk_reentry_delay_by_attempt[min(attempt_idx, len(self.risk_reentry_delay_by_attempt) - 1)])


SPECS: Dict[str, UnderlyingSpec] = {
    "NIFTY": UnderlyingSpec(
        name="NIFTY",
        spot_key=os.getenv("NIFTY_SPOT_KEY", "NSE:NIFTY 50"),
        options_exchange=os.getenv("NIFTY_OPTIONS_EXCHANGE", "NFO"),
        option_name=os.getenv("NIFTY_OPTION_NAME", "NIFTY"),
        strike_step=_env_int("NIFTY_STRIKE_STEP", 50),
        qty_units=_env_int("NIFTY_QTY_UNITS", 325),
    ),
    "SENSEX": UnderlyingSpec(
        name="SENSEX",
        spot_key=os.getenv("SENSEX_SPOT_KEY", "BSE:SENSEX"),
        options_exchange=os.getenv("SENSEX_OPTIONS_EXCHANGE", "BFO"),
        option_name=os.getenv("SENSEX_OPTION_NAME", "SENSEX"),
        strike_step=_env_int("SENSEX_STRIKE_STEP", 100),
        qty_units=_env_int("SENSEX_QTY_UNITS", 100),
    ),
}


def _profile_raw(prefix: str, dte: int, key: str, default: Any) -> str:
    """Resolve a strategy value using a DTE-specific override first.

    Resolution order:
        1. <UNDERLYING>_DTE<0|1|2>_<KEY>
        2. <UNDERLYING>_<KEY>
        3. built-in default

    Example:
        NIFTY_DTE1_PROFIT_TARGET_PCT=0.10

    If the DTE-specific key is absent, DTE-1 and DTE-2 automatically use the
    common NIFTY/SENSEX profile. This keeps the initial live test comparable,
    while allowing later optimisation without changing the Python program.
    """
    dte_key = f"{prefix}_DTE{int(dte)}_{key}"
    base_key = f"{prefix}_{key}"
    raw = os.getenv(dte_key)
    if raw is None or not raw.strip():
        raw = os.getenv(base_key)
    if raw is None or not str(raw).strip():
        raw = str(default)
    return str(raw).strip()


def _profile_bool(prefix: str, dte: int, key: str, default: bool) -> bool:
    raw = _profile_raw(prefix, dte, key, "1" if default else "0")
    return raw.lower() in {"1", "true", "yes", "y", "on"}


def _settings(prefix: str, dte: int) -> StrategySettings:
    """Return the effective strategy profile for one underlying and DTE."""
    default_entry = "09:38" if prefix == "NIFTY" else "10:00"
    default_exit = "15:15" if prefix == "NIFTY" else "15:23"
    default_cutoff = "15:12" if prefix == "NIFTY" else "15:10"
    return StrategySettings(
        entry_time=parse_hhmm(_profile_raw(prefix, dte, "ENTRY_TIME_IST", default_entry)),
        exit_time=parse_hhmm(_profile_raw(prefix, dte, "EXIT_TIME_IST", default_exit)),
        reentry_cutoff_time=parse_hhmm(
            _profile_raw(prefix, dte, "REENTRY_CUTOFF_TIME_IST", default_cutoff)
        ),
        stop_pct_by_attempt=_csv_floats(
            _profile_raw(
                prefix,
                dte,
                "STOP_PCT_BY_ATTEMPT",
                "0.3716,0.3969,0.4222,0.4475,0.4728,0.4982,0.5235,0.5488",
            ),
            [0.3716, 0.3969, 0.4222, 0.4475, 0.4728, 0.4982, 0.5235, 0.5488],
        ),
        stop_cap_rupees=float(
            _profile_raw(prefix, dte, "STOP_CAP_RUPEES", 3300.0 if prefix == "NIFTY" else 2350.0)
        ),
        profit_protect_pct=float(
            _profile_raw(prefix, dte, "PROFIT_PROTECT_PCT", 0.355 if prefix == "NIFTY" else 0.360)
        ),
        profit_target_pct=float(
            _profile_raw(prefix, dte, "PROFIT_TARGET_PCT", 0.12 if prefix == "NIFTY" else 0.08)
        ),
        profit_target_reentry_enabled=_profile_bool(
            prefix, dte, "PROFIT_TARGET_REENTRY_ENABLED", True
        ),
        target_reentry_delay_by_attempt=_csv_ints(
            _profile_raw(prefix, dte, "PROFIT_TARGET_REENTRY_DELAY_BY_ATTEMPT", "1"),
            [1],
        ),
        risk_reentry_delay_by_attempt=_csv_ints(
            _profile_raw(prefix, dte, "RISK_REENTRY_DELAY_BY_ATTEMPT", "8,8,8,8,8,8,8,8"),
            [8, 8, 8, 8, 8, 8, 8, 8],
        ),
        max_reattempts=int(float(_profile_raw(prefix, dte, "MAX_REATTEMPTS", 20))),
        max_daily_loss_rupees=float(
            _profile_raw(prefix, dte, "MAX_DAILY_LOSS_RUPEES", 30000.0)
        ),
    )



# =============================================================================
# 1. GENERAL UTILITIES
# =============================================================================

def _api(fn, *args, desc: str = "Kite read", max_retries: Optional[int] = None, **kwargs):
    retries = API_MAX_RETRIES if max_retries is None else max_retries
    attempt = 0
    delay = API_RETRY_BACKOFF_SECONDS
    while True:
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            attempt += 1
            if retries and attempt >= retries:
                raise RuntimeError(f"{desc} failed after {attempt} attempt(s): {exc}") from exc
            logging.getLogger("paper_straddle").warning(
                "[API] %s failed (attempt %s): %s; retry in %.1fs",
                desc,
                attempt,
                exc,
                delay,
            )
            time.sleep(delay)
            delay = min(delay * 2.0, API_RETRY_BACKOFF_MAX_SECONDS)


def safe_json(value: Any) -> Any:
    if isinstance(value, (datetime, date, dtime)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(k): safe_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [safe_json(v) for v in value]
    return value


def append_csv_row(path: Path, fieldnames: Sequence[str], row: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists() and path.stat().st_size > 0
    with path.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        if not exists:
            writer.writeheader()
        writer.writerow({k: safe_json(row.get(k, "")) for k in fieldnames})


# =============================================================================
# 2. AUDIT OUTPUT
# =============================================================================

TICK_FIELDS = [
    "received_at", "exchange_timestamp", "last_trade_time", "symbol", "token",
    "last_price", "last_quantity", "volume", "oi", "total_buy_quantity",
    "total_sell_quantity", "best_bid", "best_bid_qty", "best_ask", "best_ask_qty",
    "spread",
] + [
    f"{side}{level}_{field}"
    for side in ("bid", "ask")
    for level in range(1, 6)
    for field in ("price", "qty", "orders")
]

DECISION_FIELDS = [
    "timestamp", "trade_id", "underlying", "attempt_no", "state", "ce_symbol",
    "pe_symbol", "ce_ltp", "pe_ltp", "ce_bid", "ce_ask", "pe_bid", "pe_ask",
    "ce_spread", "pe_spread", "ce_quote_age_ms", "pe_quote_age_ms",
    "leg_quote_skew_ms", "estimated_ce_exit", "estimated_pe_exit",
    "ce_depth_coverage", "pe_depth_coverage", "gross_pnl", "estimated_exit_charges",
    "estimated_net_pnl", "peak_gross_pnl", "stop_rupees", "target_rupees",
    "profit_protect_g", "protect_armed", "protect_floor", "decision", "detail",
]

TRADE_FIELDS = [
    "trade_id", "date", "underlying", "exchange", "expiry", "dte", "attempt_no",
    "atm_strike", "qty", "entry_time", "exit_time", "duration_seconds",
    "exit_reason", "ce_symbol", "pe_symbol", "entry_spot", "exit_spot",
    "ce_entry_fill", "pe_entry_fill", "ce_exit_fill", "pe_exit_fill",
    "entry_premium_rupees", "gross_pnl", "charges", "net_pnl", "peak_gross_pnl",
    "max_drawdown_gross", "stop_pct", "stop_rupees", "target_pct", "target_rupees",
    "profit_protect_pct", "profit_protect_g", "entry_ce_depth_coverage",
    "entry_pe_depth_coverage", "exit_ce_depth_coverage", "exit_pe_depth_coverage",
    "entry_slippage_ticks", "exit_slippage_ticks", "entry_quote_skew_ms",
    "exit_quote_skew_ms", "daily_realized_net_after_trade",
]

SUMMARY_FIELDS = [
    "date", "build_id", "mode", "underlying", "expiry", "dte", "trades", "wins", "losses",
    "gross_pnl", "charges", "net_pnl", "max_trade_profit", "max_trade_loss",
    "profit_target_exits", "stoploss_exits", "profit_protect_exits", "time_exits",
    "raw_ticks_logged", "raw_ticks_dropped", "started_at", "finished_at",
    "session_directory",
]


class AuditWriter:
    """Non-blocking raw-tick writer plus synchronous event/trade audit files."""

    def __init__(self):
        stamp = now_ist().strftime("%Y-%m-%d_%H%M%S")
        self.session_dir = OUTPUT_ROOT / now_ist().strftime("%Y-%m-%d") / f"run_{stamp}"
        self.session_dir.mkdir(parents=True, exist_ok=True)
        self.log_path = self.session_dir / "strategy.log"
        self.events_path = self.session_dir / "events.jsonl"
        self.ticks_path = self.session_dir / "ticks.csv"
        self.decisions_path = self.session_dir / "decisions.csv"
        self.trades_path = self.session_dir / "trades.csv"
        self.summary_path = self.session_dir / "daily_summary.csv"
        self.report_path = self.session_dir / "paper_trade_report.xlsx"
        self.config_snapshot_path = self.session_dir / "config_snapshot.properties"

        self._tick_queue: "queue.Queue[Optional[Dict[str, Any]]]" = queue.Queue(maxsize=200_000)
        self._tick_thread = threading.Thread(target=self._tick_worker, name="tick-audit-writer", daemon=True)
        self._tick_thread.start()
        self.raw_ticks_logged = 0
        self.raw_ticks_dropped = 0
        self._event_lock = threading.Lock()
        self._csv_lock = threading.Lock()

        if os.path.exists(PROPERTY_FILE_PATH):
            shutil.copyfile(PROPERTY_FILE_PATH, self.config_snapshot_path)

    def build_logger(self) -> logging.Logger:
        lg = logging.getLogger("paper_straddle")
        lg.setLevel(logging.INFO)
        lg.propagate = False
        if lg.handlers:
            return lg
        fmt = logging.Formatter(
            "%(asctime)s | %(levelname)-7s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        fh = RotatingFileHandler(self.log_path, maxBytes=10_000_000, backupCount=5, encoding="utf-8")
        ch = logging.StreamHandler(sys.stdout)
        fh.setFormatter(fmt)
        ch.setFormatter(fmt)
        lg.addHandler(fh)
        lg.addHandler(ch)
        return lg

    def event(self, event_type: str, **payload: Any) -> None:
        row = {
            "timestamp": iso_ist(),
            "event_type": event_type,
            "payload": safe_json(payload),
        }
        with self._event_lock:
            with self.events_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")

    def enqueue_tick(self, row: Dict[str, Any]) -> None:
        if not LOG_RAW_TICKS:
            return
        try:
            self._tick_queue.put_nowait(row)
        except queue.Full:
            self.raw_ticks_dropped += 1

    def _tick_worker(self) -> None:
        while True:
            row = self._tick_queue.get()
            if row is None:
                self._tick_queue.task_done()
                break
            try:
                append_csv_row(self.ticks_path, TICK_FIELDS, row)
                self.raw_ticks_logged += 1
            finally:
                self._tick_queue.task_done()

    def decision(self, row: Dict[str, Any]) -> None:
        with self._csv_lock:
            append_csv_row(self.decisions_path, DECISION_FIELDS, row)

    def trade(self, row: Dict[str, Any]) -> None:
        with self._csv_lock:
            append_csv_row(self.trades_path, TRADE_FIELDS, row)

    def summary(self, row: Dict[str, Any]) -> None:
        with self._csv_lock:
            append_csv_row(self.summary_path, SUMMARY_FIELDS, row)

    def flush_ticks(self) -> None:
        """Wait until all ticks already queued have reached ticks.csv."""
        self._tick_queue.join()

    def close(self) -> None:
        self._tick_queue.put(None)
        self._tick_queue.join()
        self._tick_thread.join(timeout=5.0)

    def generate_excel(self, log: logging.Logger) -> None:
        if not GENERATE_EXCEL_ON_EXIT:
            return
        try:
            import pandas as pd
        except Exception as exc:
            log.warning("[REPORT] pandas/openpyxl unavailable; Excel report skipped: %s", exc)
            return

        try:
            with pd.ExcelWriter(self.report_path, engine="openpyxl") as writer:
                for sheet, path in (
                    ("trades", self.trades_path),
                    ("decisions", self.decisions_path),
                    ("daily_summary", self.summary_path),
                ):
                    frame = pd.read_csv(path) if path.exists() and path.stat().st_size else pd.DataFrame()
                    frame.to_excel(writer, sheet_name=sheet, index=False)
                # Raw ticks remain in CSV; including every tick in Excel can make
                # the workbook unnecessarily large and slow to open.
                pd.DataFrame([
                    {"raw_ticks_csv": str(self.ticks_path), "events_jsonl": str(self.events_path)}
                ]).to_excel(writer, sheet_name="raw_log_locations", index=False)
            log.info("[REPORT] Excel summary written: %s", self.report_path)
        except Exception as exc:
            log.exception("[REPORT] Excel generation failed: %s", exc)


# =============================================================================
# 3. MARKET DEPTH AND PAPER FILL MODEL
# =============================================================================

@dataclass
class FillEstimate:
    side: str
    fill_price: float
    requested_qty: int
    displayed_qty_used: int
    coverage_ratio: float
    depth_levels_used: int
    source: str
    slippage_ticks: int
    shortfall_qty: int
    best_price: float
    worst_displayed_price: float


@dataclass
class PairSnapshot:
    ce_tick: Dict[str, Any]
    pe_tick: Dict[str, Any]
    ce_age_ms: float
    pe_age_ms: float
    skew_ms: float


def _depth_side(tick: Dict[str, Any], side: str) -> List[Dict[str, Any]]:
    depth = tick.get("depth") or {}
    key = "buy" if side.upper() == "SELL" else "sell"
    values = depth.get(key) or []
    return [x for x in values if float(x.get("price") or 0) > 0 and int(x.get("quantity") or 0) > 0]


def simulate_depth_fill(
    tick: Dict[str, Any],
    side: str,
    qty: int,
    tick_size: float,
    slippage_ticks: int,
    shortfall_penalty_ticks: int,
    allow_ltp_fallback: bool,
) -> FillEstimate:
    """Simulate a marketable fill by walking five-level depth.

    SELL consumes bids. BUY consumes asks. Any configured slippage is applied
    after the depth-weighted price. If displayed depth is insufficient, the
    unshown remainder is valued beyond the worst visible level by the configured
    penalty. This makes the shortfall explicit rather than silently assuming LTP.
    """
    side = side.upper()
    if side not in {"SELL", "BUY"}:
        raise ValueError(f"Unsupported side: {side}")
    if qty <= 0:
        raise ValueError("qty must be positive")

    levels = _depth_side(tick, side)
    ltp = float(tick.get("last_price") or 0.0)
    if not levels:
        if not allow_ltp_fallback or ltp <= 0:
            raise RuntimeError(f"No {'bid' if side == 'SELL' else 'ask'} depth available")
        direction = -1 if side == "SELL" else 1
        raw = ltp + direction * (slippage_ticks + shortfall_penalty_ticks) * tick_size
        return FillEstimate(
            side=side,
            fill_price=adverse_round(raw, side, tick_size),
            requested_qty=qty,
            displayed_qty_used=0,
            coverage_ratio=0.0,
            depth_levels_used=0,
            source="LTP_FALLBACK",
            slippage_ticks=slippage_ticks,
            shortfall_qty=qty,
            best_price=ltp,
            worst_displayed_price=ltp,
        )

    remaining = qty
    value = 0.0
    used_qty = 0
    used_levels = 0
    for level in levels:
        level_qty = int(level.get("quantity") or 0)
        level_px = float(level.get("price") or 0.0)
        take = min(remaining, level_qty)
        if take <= 0:
            continue
        value += take * level_px
        used_qty += take
        remaining -= take
        used_levels += 1
        if remaining <= 0:
            break

    best = float(levels[0]["price"])
    worst = float(levels[min(used_levels, len(levels)) - 1]["price"]) if used_levels else best
    if remaining > 0:
        direction = -1 if side == "SELL" else 1
        penalty_price = worst + direction * shortfall_penalty_ticks * tick_size
        value += remaining * penalty_price

    raw_avg = value / qty
    direction = -1 if side == "SELL" else 1
    raw_avg += direction * slippage_ticks * tick_size
    fill = adverse_round(raw_avg, side, tick_size)
    coverage = min(1.0, used_qty / qty)
    return FillEstimate(
        side=side,
        fill_price=fill,
        requested_qty=qty,
        displayed_qty_used=used_qty,
        coverage_ratio=coverage,
        depth_levels_used=used_levels,
        source="DEPTH" if remaining <= 0 else "DEPTH_WITH_SHORTFALL_PENALTY",
        slippage_ticks=slippage_ticks,
        shortfall_qty=max(0, remaining),
        best_price=best,
        worst_displayed_price=worst,
    )


def estimate_trade_charges(
    exchange: str,
    entry_ce: float,
    entry_pe: float,
    exit_ce: float,
    exit_pe: float,
    qty: int,
) -> Dict[str, float]:
    entry_turnover = (entry_ce + entry_pe) * qty
    exit_turnover = (exit_ce + exit_pe) * qty
    total_turnover = entry_turnover + exit_turnover
    brokerage = BROKERAGE_PER_ORDER * 4
    stt = entry_turnover * STT_SELL_PCT
    txn_rate = NFO_TXN_PCT if exchange.upper() == "NFO" else BFO_TXN_PCT
    transaction = total_turnover * txn_rate
    sebi = total_turnover * SEBI_PER_CRORE / 10_000_000
    stamp = exit_turnover * STAMP_BUY_PCT
    ipft = total_turnover * NFO_IPFT_PER_CRORE / 10_000_000 if exchange.upper() == "NFO" else 0.0
    gst = (brokerage + transaction + sebi + ipft) * GST_PCT
    total = brokerage + stt + transaction + sebi + stamp + ipft + gst
    return {
        "brokerage": round(brokerage, 2),
        "stt": round(stt, 2),
        "transaction": round(transaction, 2),
        "sebi": round(sebi, 2),
        "stamp": round(stamp, 2),
        "ipft": round(ipft, 2),
        "gst": round(gst, 2),
        "total": round(total, 2),
    }


# =============================================================================
# 4. LIVE FULL-DEPTH FEED
# =============================================================================

class PriceFeed:
    def __init__(self, api_key: str, access_token: str, audit: AuditWriter, log: logging.Logger):
        if KiteTicker is None:
            raise RuntimeError("kiteconnect is unavailable. Run: pip install kiteconnect")
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
        self.audit = audit
        self.log = log
        self._latest: Dict[int, Dict[str, Any]] = {}
        self._symbols: Dict[int, str] = {}
        self._lock = threading.RLock()
        self._connected = threading.Event()
        self._subscribed: set[int] = set()

        self.ticker.on_ticks = self._on_ticks
        self.ticker.on_connect = self._on_connect
        self.ticker.on_close = self._on_close
        self.ticker.on_error = self._on_error
        self.ticker.on_reconnect = self._on_reconnect
        self.ticker.on_noreconnect = self._on_noreconnect

    def _on_ticks(self, ws, ticks):
        received = now_ist()
        for original in ticks:
            tick = dict(original)
            token = int(tick.get("instrument_token"))
            tick["_received_at"] = received
            with self._lock:
                self._latest[token] = tick
                symbol = self._symbols.get(token, str(token))
            if LOG_RAW_TICKS:
                self.audit.enqueue_tick(self._tick_to_row(symbol, token, tick))

    def _tick_to_row(self, symbol: str, token: int, tick: Dict[str, Any]) -> Dict[str, Any]:
        buy = (tick.get("depth") or {}).get("buy") or []
        sell = (tick.get("depth") or {}).get("sell") or []
        row: Dict[str, Any] = {
            "received_at": safe_json(tick.get("_received_at")),
            "exchange_timestamp": safe_json(tick.get("exchange_timestamp")),
            "last_trade_time": safe_json(tick.get("last_trade_time")),
            "symbol": symbol,
            "token": token,
            "last_price": tick.get("last_price"),
            "last_quantity": tick.get("last_quantity"),
            "volume": tick.get("volume_traded") or tick.get("volume"),
            "oi": tick.get("oi"),
            "total_buy_quantity": tick.get("total_buy_quantity"),
            "total_sell_quantity": tick.get("total_sell_quantity"),
            "best_bid": buy[0].get("price") if buy else None,
            "best_bid_qty": buy[0].get("quantity") if buy else None,
            "best_ask": sell[0].get("price") if sell else None,
            "best_ask_qty": sell[0].get("quantity") if sell else None,
        }
        if row["best_bid"] is not None and row["best_ask"] is not None:
            row["spread"] = round(float(row["best_ask"]) - float(row["best_bid"]), 4)
        for side_name, values in (("bid", buy), ("ask", sell)):
            for idx in range(5):
                level = values[idx] if idx < len(values) else {}
                row[f"{side_name}{idx+1}_price"] = level.get("price")
                row[f"{side_name}{idx+1}_qty"] = level.get("quantity")
                row[f"{side_name}{idx+1}_orders"] = level.get("orders")
        return row

    def _on_connect(self, ws, response):
        self._connected.set()
        self.log.info("[WS] Connected.")
        if self._subscribed:
            tokens = list(self._subscribed)
            ws.subscribe(tokens)
            ws.set_mode(ws.MODE_FULL, tokens)

    def _on_close(self, ws, code, reason):
        self.log.warning("[WS] Closed code=%s reason=%s", code, reason)

    def _on_error(self, ws, code, reason):
        self.log.warning("[WS] Error code=%s reason=%s", code, reason)

    def _on_reconnect(self, ws, attempts_count):
        self.log.warning("[WS] Reconnecting attempt=%s", attempts_count)

    def _on_noreconnect(self, ws):
        self.log.error("[WS] Reconnection attempts exhausted.")

    def start(self) -> None:
        self.log.info("[WS] Starting KiteTicker in FULL mode.")
        self.ticker.connect(threaded=True)
        if not self._connected.wait(timeout=20.0):
            self.log.warning("[WS] Connection was not confirmed within 20 seconds.")

    def stop(self) -> None:
        try:
            self.ticker.close()
        except Exception:
            pass

    def subscribe(self, instruments: Sequence[Dict[str, Any]]) -> None:
        tokens = [int(row["instrument_token"]) for row in instruments]
        with self._lock:
            for row in instruments:
                self._symbols[int(row["instrument_token"])] = str(row["tradingsymbol"])
            self._subscribed.update(tokens)
        self.ticker.subscribe(tokens)
        self.ticker.set_mode(self.ticker.MODE_FULL, tokens)
        self.log.info("[WS] FULL subscriptions: %s", [self._symbols[t] for t in tokens])

    def unsubscribe(self, tokens: Iterable[int]) -> None:
        token_list = [int(x) for x in tokens]
        with self._lock:
            self._subscribed.difference_update(token_list)
        try:
            self.ticker.unsubscribe(token_list)
        except Exception as exc:
            self.log.warning("[WS] Unsubscribe failed: %s", exc)

    def latest(self, token: int) -> Optional[Dict[str, Any]]:
        with self._lock:
            tick = self._latest.get(int(token))
            return dict(tick) if tick is not None else None

    def pair_snapshot(self, ce_token: int, pe_token: int) -> Optional[PairSnapshot]:
        ce = self.latest(ce_token)
        pe = self.latest(pe_token)
        if ce is None or pe is None:
            return None
        now = now_ist()
        ce_recv = ce.get("_received_at")
        pe_recv = pe.get("_received_at")
        if not isinstance(ce_recv, datetime) or not isinstance(pe_recv, datetime):
            return None
        ce_age = max(0.0, (now - ce_recv).total_seconds() * 1000.0)
        pe_age = max(0.0, (now - pe_recv).total_seconds() * 1000.0)
        skew = abs((ce_recv - pe_recv).total_seconds() * 1000.0)
        if ce_age > MAX_QUOTE_AGE_SECONDS * 1000 or pe_age > MAX_QUOTE_AGE_SECONDS * 1000:
            return None
        if skew > MAX_LEG_QUOTE_SKEW_MS:
            return None
        return PairSnapshot(ce, pe, ce_age, pe_age, skew)

    def wait_for_pair(self, ce_token: int, pe_token: int, timeout: float = QUOTE_WAIT_SECONDS) -> PairSnapshot:
        deadline = time.time() + timeout
        while time.time() < deadline:
            snapshot = self.pair_snapshot(ce_token, pe_token)
            if snapshot is not None:
                return snapshot
            time.sleep(0.05)
        raise RuntimeError(
            f"No fresh synchronized CE/PE full-depth pair within {timeout:.1f}s"
        )


# =============================================================================
# 5. INSTRUMENT DISCOVERY
# =============================================================================

@dataclass(frozen=True)
class Opportunity:
    spec: UnderlyingSpec
    settings: StrategySettings
    expiry: date
    dte: int


class InstrumentBook:
    def __init__(self, kite, log: logging.Logger, audit: AuditWriter):
        self.kite = kite
        self.log = log
        self.audit = audit
        self.rows_by_underlying: Dict[str, List[Dict[str, Any]]] = {}

    def load(self) -> None:
        for name, spec in SPECS.items():
            rows = _api(
                self.kite.instruments,
                spec.options_exchange,
                desc=f"instruments({spec.options_exchange})",
            )
            filtered: List[Dict[str, Any]] = []
            for raw in rows:
                instrument_type = str(raw.get("instrument_type", "")).upper()
                if instrument_type not in {"CE", "PE"}:
                    continue
                row_name = str(raw.get("name") or "").upper()
                symbol = str(raw.get("tradingsymbol") or "").upper()
                if row_name != spec.option_name.upper() and not symbol.startswith(spec.option_name.upper()):
                    continue
                expiry = raw.get("expiry")
                if isinstance(expiry, datetime):
                    expiry = expiry.date()
                elif isinstance(expiry, str):
                    expiry = datetime.fromisoformat(expiry).date()
                if not isinstance(expiry, date):
                    continue
                row = dict(raw)
                row["expiry"] = expiry
                row["strike"] = int(round(float(row.get("strike") or 0)))
                row["instrument_type"] = instrument_type
                filtered.append(row)
            self.rows_by_underlying[name] = filtered
            expiries = sorted({row["expiry"] for row in filtered})
            self.log.info(
                "[INSTRUMENTS] %s %s options=%s nearest_expiries=%s",
                name,
                spec.options_exchange,
                len(filtered),
                expiries[:5],
            )
            self.audit.event(
                "INSTRUMENT_BOOK_LOADED",
                underlying=name,
                exchange=spec.options_exchange,
                option_rows=len(filtered),
                nearest_expiries=expiries[:10],
            )

    def opportunities_for_today(self, today: date) -> List[Opportunity]:
        result: List[Opportunity] = []
        for name, rows in self.rows_by_underlying.items():
            expiries = sorted({row["expiry"] for row in rows if row["expiry"] >= today})
            if not expiries:
                continue
            expiry = expiries[0]
            dte = (expiry - today).days
            if dte in ALLOWED_DTE:
                result.append(Opportunity(SPECS[name], _settings(name, dte), expiry, dte))
        return result

    def select_today(self, today: date) -> Opportunity:
        opportunities = self.opportunities_for_today(today)
        if not opportunities:
            details = {
                name: sorted({r["expiry"] for r in rows if r["expiry"] >= today})[:3]
                for name, rows in self.rows_by_underlying.items()
            }
            raise RuntimeError(
                f"No DTE-0/DTE-1/DTE-2 opportunity for {today}; ALLOWED_DTE={ALLOWED_DTE}; next_expiries={details}"
            )
        if TRADE_SELECTION == "ONLY_NIFTY":
            matches = [x for x in opportunities if x.spec.name == "NIFTY"]
            if not matches:
                raise RuntimeError("NIFTY is not an allowed-DTE opportunity today")
            return matches[0]
        if TRADE_SELECTION == "ONLY_SENSEX":
            matches = [x for x in opportunities if x.spec.name == "SENSEX"]
            if not matches:
                raise RuntimeError("SENSEX is not an allowed-DTE opportunity today")
            return matches[0]
        priority = {name: idx for idx, name in enumerate(TRADE_PRIORITY)}
        return sorted(
            opportunities,
            key=lambda x: (x.expiry, priority.get(x.spec.name, 999)),
        )[0]

    def option_pair(self, underlying: str, expiry: date, strike: int) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        rows = [
            r for r in self.rows_by_underlying[underlying]
            if r["expiry"] == expiry and int(r["strike"]) == int(strike)
        ]
        ce = [r for r in rows if r["instrument_type"] == "CE"]
        pe = [r for r in rows if r["instrument_type"] == "PE"]
        if not ce or not pe:
            available = sorted({int(r["strike"]) for r in self.rows_by_underlying[underlying] if r["expiry"] == expiry})
            raise RuntimeError(
                f"Missing exact ATM pair for {underlying} expiry={expiry} strike={strike}; "
                f"nearby strikes={available[:5]}...{available[-5:] if available else []}"
            )
        return ce[0], pe[0]


# =============================================================================
# 6. PAPER STRATEGY ENGINE
# =============================================================================

@dataclass
class Position:
    trade_id: str
    underlying: str
    exchange: str
    expiry: str
    dte: int
    attempt_idx: int
    atm_strike: int
    qty: int
    ce_symbol: str
    pe_symbol: str
    ce_token: int
    pe_token: int
    entry_time: str
    entry_monotonic: float
    entry_spot: float
    ce_entry_fill: float
    pe_entry_fill: float
    entry_ce_depth_coverage: float
    entry_pe_depth_coverage: float
    entry_quote_skew_ms: float
    premium_sum_rupees: float
    stop_pct: float
    stop_rupees: float
    target_pct: float
    target_rupees: float
    profit_protect_pct: float
    profit_protect_g: float
    peak_gross_pnl: float = 0.0
    max_drawdown_gross: float = 0.0
    protect_armed: bool = False


class PaperStraddleTrader:
    def __init__(
        self,
        kite,
        feed: PriceFeed,
        book: InstrumentBook,
        opportunity: Opportunity,
        audit: AuditWriter,
        log: logging.Logger,
    ):
        self.kite = kite
        self.feed = feed
        self.book = book
        self.opportunity = opportunity
        self.spec = opportunity.spec
        self.settings = opportunity.settings
        self.audit = audit
        self.log = log
        self.position: Optional[Position] = None
        self.attempt_idx = 0
        self.daily_realized_gross = 0.0
        self.daily_charges = 0.0
        self.daily_realized_net = 0.0
        self.reentry_at: Optional[datetime] = None
        self.phase = "WAITING"
        self.started_at = now_ist()
        self.finished_at: Optional[datetime] = None
        self.trades: List[Dict[str, Any]] = []
        self._last_state_save = 0.0
        self._last_heartbeat = 0.0

    def _spot(self) -> float:
        result = _api(self.kite.ltp, [self.spec.spot_key], desc=f"ltp({self.spec.spot_key})")
        if self.spec.spot_key not in result:
            raise RuntimeError(f"Spot quote missing for {self.spec.spot_key}")
        return float(result[self.spec.spot_key]["last_price"])

    def _save_state(self) -> None:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "date": now_ist().date().isoformat(),
            "build_id": BUILD_ID,
            "underlying": self.spec.name,
            "expiry": self.opportunity.expiry.isoformat(),
            "dte": self.opportunity.dte,
            "phase": self.phase,
            "attempt_idx": self.attempt_idx,
            "daily_realized_gross": self.daily_realized_gross,
            "daily_charges": self.daily_charges,
            "daily_realized_net": self.daily_realized_net,
            "reentry_at": self.reentry_at.isoformat() if self.reentry_at else None,
            "position": asdict(self.position) if self.position else None,
            "session_directory": str(self.audit.session_dir),
            "saved_at": iso_ist(),
        }
        temp = STATE_FILE.with_suffix(STATE_FILE.suffix + ".tmp")
        temp.write_text(json.dumps(safe_json(payload), indent=2), encoding="utf-8")
        os.replace(temp, STATE_FILE)
        self._last_state_save = time.time()

    def _load_state(self) -> None:
        if not RESUME_TODAY_STATE or not STATE_FILE.exists():
            return
        try:
            state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception as exc:
            self.log.warning("[STATE] Cannot read state: %s", exc)
            return
        if state.get("date") != now_ist().date().isoformat():
            return
        if (
            state.get("underlying") != self.spec.name
            or state.get("expiry") != self.opportunity.expiry.isoformat()
            or int(state.get("dte", self.opportunity.dte)) != self.opportunity.dte
        ):
            self.log.warning("[STATE] Today's state belongs to a different opportunity; ignoring it.")
            return
        self.phase = str(state.get("phase", "WAITING"))
        self.attempt_idx = int(state.get("attempt_idx", 0))
        self.daily_realized_gross = float(state.get("daily_realized_gross", 0.0))
        self.daily_charges = float(state.get("daily_charges", 0.0))
        self.daily_realized_net = float(state.get("daily_realized_net", 0.0))
        rt = state.get("reentry_at")
        self.reentry_at = datetime.fromisoformat(rt) if rt else None
        raw_position = state.get("position")
        if raw_position:
            self.position = Position(**raw_position)
            self.phase = "IN_POSITION"
        self.log.warning(
            "[STATE] Resumed phase=%s attempt=%s position=%s day_net=Rs%.2f. "
            "Any trigger that occurred while the process was offline cannot be reconstructed.",
            self.phase,
            self.attempt_idx + 1,
            bool(self.position),
            self.daily_realized_net,
        )
        self.audit.event("STATE_RESUMED", state=state)

    def _wait_until(self, target: datetime, label: str) -> bool:
        self.log.info("[WAIT] %s until %s", label, target.strftime("%Y-%m-%d %H:%M:%S"))
        while now_ist() < target:
            if now_ist().time() >= self.settings.exit_time:
                return False
            time.sleep(min(1.0, max(0.05, (target - now_ist()).total_seconds())))
        return True

    def _pair_fills(
        self,
        snapshot: PairSnapshot,
        side: str,
        qty: int,
        slippage_ticks: int,
        require_full_depth: bool,
    ) -> Tuple[FillEstimate, FillEstimate]:
        ce_tick_size = float(snapshot.ce_tick.get("tick_size") or 0.05)
        pe_tick_size = float(snapshot.pe_tick.get("tick_size") or 0.05)
        ce = simulate_depth_fill(
            snapshot.ce_tick,
            side,
            qty,
            ce_tick_size,
            slippage_ticks,
            DEPTH_SHORTFALL_PENALTY_TICKS,
            ALLOW_LTP_FALLBACK,
        )
        pe = simulate_depth_fill(
            snapshot.pe_tick,
            side,
            qty,
            pe_tick_size,
            slippage_ticks,
            DEPTH_SHORTFALL_PENALTY_TICKS,
            ALLOW_LTP_FALLBACK,
        )
        if require_full_depth and (ce.coverage_ratio < 1.0 or pe.coverage_ratio < 1.0):
            raise RuntimeError(
                f"Insufficient displayed depth: CE coverage={ce.coverage_ratio:.1%}, "
                f"PE coverage={pe.coverage_ratio:.1%}"
            )
        return ce, pe

    @staticmethod
    def _best_prices(tick: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
        depth = tick.get("depth") or {}
        bids = depth.get("buy") or []
        asks = depth.get("sell") or []
        bid = float(bids[0]["price"]) if bids and bids[0].get("price") else None
        ask = float(asks[0]["price"]) if asks and asks[0].get("price") else None
        return bid, ask

    def enter(self) -> bool:
        if now_ist().time() >= self.settings.reentry_cutoff_time:
            self.log.info("[ENTRY] Re-entry cutoff reached; no fresh attempt.")
            return False
        spot = self._spot()
        atm = round_to_step(spot, self.spec.strike_step)
        ce_row, pe_row = self.book.option_pair(self.spec.name, self.opportunity.expiry, atm)
        lot_size = int(ce_row.get("lot_size") or 0)
        if lot_size > 0 and self.spec.qty_units % lot_size != 0:
            self.log.warning(
                "[ENTRY] Configured qty=%s is not a multiple of current lot_size=%s.",
                self.spec.qty_units,
                lot_size,
            )
        ce_token = int(ce_row["instrument_token"])
        pe_token = int(pe_row["instrument_token"])
        self.feed.subscribe([ce_row, pe_row])
        try:
            snapshot = self.feed.wait_for_pair(ce_token, pe_token)
            ce_fill, pe_fill = self._pair_fills(
                snapshot,
                side="SELL",
                qty=self.spec.qty_units,
                slippage_ticks=ENTRY_SLIPPAGE_TICKS,
                require_full_depth=REQUIRE_FULL_DEPTH_AT_ENTRY,
            )
        except Exception as exc:
            self.log.error("[ENTRY] Paper entry rejected: %s", exc)
            self.audit.event(
                "ENTRY_REJECTED",
                underlying=self.spec.name,
                attempt_no=self.attempt_idx + 1,
                atm=atm,
                reason=str(exc),
            )
            self.feed.unsubscribe([ce_token, pe_token])
            return False

        premium = (ce_fill.fill_price + pe_fill.fill_price) * self.spec.qty_units
        stop_pct = self.settings.stop_pct(self.attempt_idx)
        uncapped_stop = stop_pct * premium
        stop_rupees = min(uncapped_stop, self.settings.stop_cap_rupees) if self.settings.stop_cap_rupees > 0 else uncapped_stop
        target_rupees = self.settings.profit_target_pct * premium
        protect_g = self.settings.profit_protect_pct * premium
        trade_id = f"{now_ist().strftime('%Y%m%d')}-{self.spec.name}-A{self.attempt_idx+1:02d}-{now_ist().strftime('%H%M%S')}"
        self.position = Position(
            trade_id=trade_id,
            underlying=self.spec.name,
            exchange=self.spec.options_exchange,
            expiry=self.opportunity.expiry.isoformat(),
            dte=self.opportunity.dte,
            attempt_idx=self.attempt_idx,
            atm_strike=atm,
            qty=self.spec.qty_units,
            ce_symbol=str(ce_row["tradingsymbol"]),
            pe_symbol=str(pe_row["tradingsymbol"]),
            ce_token=ce_token,
            pe_token=pe_token,
            entry_time=iso_ist(),
            entry_monotonic=time.monotonic(),
            entry_spot=spot,
            ce_entry_fill=ce_fill.fill_price,
            pe_entry_fill=pe_fill.fill_price,
            entry_ce_depth_coverage=ce_fill.coverage_ratio,
            entry_pe_depth_coverage=pe_fill.coverage_ratio,
            entry_quote_skew_ms=snapshot.skew_ms,
            premium_sum_rupees=premium,
            stop_pct=stop_pct,
            stop_rupees=stop_rupees,
            target_pct=self.settings.profit_target_pct,
            target_rupees=target_rupees,
            profit_protect_pct=self.settings.profit_protect_pct,
            profit_protect_g=protect_g,
        )
        self.phase = "IN_POSITION"
        self._save_state()
        self.log.info(
            "[ENTRY] %s attempt=%s ATM=%s qty=%s | SELL CE %s @ %.2f, PE %s @ %.2f | "
            "premium=Rs%.2f stop=Rs%.2f target=Rs%.2f protectG=Rs%.2f | depth CE=%.0f%% PE=%.0f%% skew=%.0fms",
            self.spec.name,
            self.attempt_idx + 1,
            atm,
            self.spec.qty_units,
            self.position.ce_symbol,
            ce_fill.fill_price,
            self.position.pe_symbol,
            pe_fill.fill_price,
            premium,
            stop_rupees,
            target_rupees,
            protect_g,
            ce_fill.coverage_ratio * 100,
            pe_fill.coverage_ratio * 100,
            snapshot.skew_ms,
        )
        self.audit.event(
            "PAPER_ENTRY",
            position=asdict(self.position),
            ce_fill=asdict(ce_fill),
            pe_fill=asdict(pe_fill),
        )
        return True

    def _decision_row(
        self,
        snapshot: PairSnapshot,
        ce_exit: FillEstimate,
        pe_exit: FillEstimate,
        gross_pnl: float,
        estimated_charges: float,
        decision: str,
        detail: str = "",
    ) -> Dict[str, Any]:
        assert self.position is not None
        p = self.position
        ce_bid, ce_ask = self._best_prices(snapshot.ce_tick)
        pe_bid, pe_ask = self._best_prices(snapshot.pe_tick)
        protect_floor = p.peak_gross_pnl - p.profit_protect_g if p.protect_armed else None
        return {
            "timestamp": iso_ist(),
            "trade_id": p.trade_id,
            "underlying": p.underlying,
            "attempt_no": p.attempt_idx + 1,
            "state": self.phase,
            "ce_symbol": p.ce_symbol,
            "pe_symbol": p.pe_symbol,
            "ce_ltp": snapshot.ce_tick.get("last_price"),
            "pe_ltp": snapshot.pe_tick.get("last_price"),
            "ce_bid": ce_bid,
            "ce_ask": ce_ask,
            "pe_bid": pe_bid,
            "pe_ask": pe_ask,
            "ce_spread": (ce_ask - ce_bid) if ce_bid is not None and ce_ask is not None else None,
            "pe_spread": (pe_ask - pe_bid) if pe_bid is not None and pe_ask is not None else None,
            "ce_quote_age_ms": snapshot.ce_age_ms,
            "pe_quote_age_ms": snapshot.pe_age_ms,
            "leg_quote_skew_ms": snapshot.skew_ms,
            "estimated_ce_exit": ce_exit.fill_price,
            "estimated_pe_exit": pe_exit.fill_price,
            "ce_depth_coverage": ce_exit.coverage_ratio,
            "pe_depth_coverage": pe_exit.coverage_ratio,
            "gross_pnl": gross_pnl,
            "estimated_exit_charges": estimated_charges,
            "estimated_net_pnl": gross_pnl - estimated_charges,
            "peak_gross_pnl": p.peak_gross_pnl,
            "stop_rupees": p.stop_rupees,
            "target_rupees": p.target_rupees,
            "profit_protect_g": p.profit_protect_g,
            "protect_armed": p.protect_armed,
            "protect_floor": protect_floor,
            "decision": decision,
            "detail": detail,
        }

    def monitor_until_exit(self) -> str:
        if self.position is None:
            raise RuntimeError("monitor_until_exit called without a position")
        p = self.position
        self.feed.subscribe([
            {"instrument_token": p.ce_token, "tradingsymbol": p.ce_symbol},
            {"instrument_token": p.pe_token, "tradingsymbol": p.pe_symbol},
        ])
        self._last_heartbeat = 0.0
        while True:
            if now_ist().time() >= self.settings.exit_time:
                return "TIME_EXIT"
            try:
                snapshot = self.feed.wait_for_pair(p.ce_token, p.pe_token, timeout=max(1.0, MAX_QUOTE_AGE_SECONDS))
                ce_exit, pe_exit = self._pair_fills(
                    snapshot,
                    side="BUY",
                    qty=p.qty,
                    slippage_ticks=EXIT_SLIPPAGE_TICKS,
                    require_full_depth=False,
                )
            except Exception as exc:
                self.audit.decision({
                    "timestamp": iso_ist(),
                    "trade_id": p.trade_id,
                    "underlying": p.underlying,
                    "attempt_no": p.attempt_idx + 1,
                    "state": self.phase,
                    "decision": "WAIT_BAD_QUOTES",
                    "detail": str(exc),
                })
                time.sleep(MONITOR_POLL_SECONDS)
                continue

            gross = (
                (p.ce_entry_fill - ce_exit.fill_price)
                + (p.pe_entry_fill - pe_exit.fill_price)
            ) * p.qty
            p.peak_gross_pnl = max(p.peak_gross_pnl, gross)
            p.max_drawdown_gross = min(p.max_drawdown_gross, gross)
            if not p.protect_armed and p.profit_protect_g > 0 and p.peak_gross_pnl >= p.profit_protect_g:
                p.protect_armed = True
                self.log.info(
                    "[PROTECT] %s armed: peak=Rs%.2f >= G=Rs%.2f",
                    p.trade_id,
                    p.peak_gross_pnl,
                    p.profit_protect_g,
                )
                self.audit.event("PROFIT_PROTECT_ARMED", trade_id=p.trade_id, peak=p.peak_gross_pnl, G=p.profit_protect_g)

            charges = estimate_trade_charges(
                p.exchange,
                p.ce_entry_fill,
                p.pe_entry_fill,
                ce_exit.fill_price,
                pe_exit.fill_price,
                p.qty,
            )["total"]

            decision = "HOLD"
            detail = ""
            if gross <= -p.stop_rupees:
                decision = "STOPLOSS"
            elif p.target_rupees > 0 and gross >= p.target_rupees:
                decision = "PROFIT_TARGET"
            elif p.protect_armed and gross <= p.peak_gross_pnl - p.profit_protect_g:
                decision = "PROFIT_PROTECT"

            self.audit.decision(self._decision_row(snapshot, ce_exit, pe_exit, gross, charges, decision, detail))
            if decision != "HOLD":
                return decision

            if time.time() - self._last_heartbeat >= HEARTBEAT_SECONDS:
                self.log.info(
                    "[MONITOR] %s gross=Rs%.2f est.net=Rs%.2f peak=Rs%.2f | "
                    "exit CE=%.2f PE=%.2f | target=Rs%.2f stop=-Rs%.2f protect=%s",
                    p.trade_id,
                    gross,
                    gross - charges,
                    p.peak_gross_pnl,
                    ce_exit.fill_price,
                    pe_exit.fill_price,
                    p.target_rupees,
                    p.stop_rupees,
                    p.protect_armed,
                )
                self._last_heartbeat = time.time()

            if time.time() - self._last_state_save >= STATE_SAVE_SECONDS:
                self._save_state()
            time.sleep(MONITOR_POLL_SECONDS)

    def exit_position(self, reason: str) -> Dict[str, Any]:
        if self.position is None:
            raise RuntimeError("exit_position called without a position")
        p = self.position
        snapshot = self.feed.wait_for_pair(p.ce_token, p.pe_token, timeout=QUOTE_WAIT_SECONDS)
        ce_exit, pe_exit = self._pair_fills(
            snapshot,
            side="BUY",
            qty=p.qty,
            slippage_ticks=EXIT_SLIPPAGE_TICKS,
            require_full_depth=False,
        )
        gross = ((p.ce_entry_fill - ce_exit.fill_price) + (p.pe_entry_fill - pe_exit.fill_price)) * p.qty
        costs = estimate_trade_charges(
            p.exchange,
            p.ce_entry_fill,
            p.pe_entry_fill,
            ce_exit.fill_price,
            pe_exit.fill_price,
            p.qty,
        )
        charges = costs["total"]
        net = gross - charges
        try:
            exit_spot = self._spot()
        except Exception:
            exit_spot = float("nan")
        exit_time = now_ist()
        duration = max(0.0, time.monotonic() - p.entry_monotonic)
        self.daily_realized_gross += gross
        self.daily_charges += charges
        self.daily_realized_net += net
        row = {
            "trade_id": p.trade_id,
            "date": now_ist().date().isoformat(),
            "underlying": p.underlying,
            "exchange": p.exchange,
            "expiry": p.expiry,
            "dte": p.dte,
            "attempt_no": p.attempt_idx + 1,
            "atm_strike": p.atm_strike,
            "qty": p.qty,
            "entry_time": p.entry_time,
            "exit_time": exit_time.isoformat(timespec="milliseconds"),
            "duration_seconds": round(duration, 3),
            "exit_reason": reason,
            "ce_symbol": p.ce_symbol,
            "pe_symbol": p.pe_symbol,
            "entry_spot": p.entry_spot,
            "exit_spot": exit_spot,
            "ce_entry_fill": p.ce_entry_fill,
            "pe_entry_fill": p.pe_entry_fill,
            "ce_exit_fill": ce_exit.fill_price,
            "pe_exit_fill": pe_exit.fill_price,
            "entry_premium_rupees": p.premium_sum_rupees,
            "gross_pnl": round(gross, 2),
            "charges": round(charges, 2),
            "net_pnl": round(net, 2),
            "peak_gross_pnl": round(p.peak_gross_pnl, 2),
            "max_drawdown_gross": round(p.max_drawdown_gross, 2),
            "stop_pct": p.stop_pct,
            "stop_rupees": p.stop_rupees,
            "target_pct": p.target_pct,
            "target_rupees": p.target_rupees,
            "profit_protect_pct": p.profit_protect_pct,
            "profit_protect_g": p.profit_protect_g,
            "entry_ce_depth_coverage": p.entry_ce_depth_coverage,
            "entry_pe_depth_coverage": p.entry_pe_depth_coverage,
            "exit_ce_depth_coverage": ce_exit.coverage_ratio,
            "exit_pe_depth_coverage": pe_exit.coverage_ratio,
            "entry_slippage_ticks": ENTRY_SLIPPAGE_TICKS,
            "exit_slippage_ticks": EXIT_SLIPPAGE_TICKS,
            "entry_quote_skew_ms": p.entry_quote_skew_ms,
            "exit_quote_skew_ms": snapshot.skew_ms,
            "daily_realized_net_after_trade": round(self.daily_realized_net, 2),
        }
        self.trades.append(row)
        self.audit.trade(row)
        self.audit.event(
            "PAPER_EXIT",
            trade=row,
            ce_fill=asdict(ce_exit),
            pe_fill=asdict(pe_exit),
            charge_breakdown=costs,
        )
        self.log.info(
            "[EXIT] %s %s | BUY CE %.2f, PE %.2f | gross=Rs%.2f charges=Rs%.2f "
            "net=Rs%.2f day_net=Rs%.2f",
            p.trade_id,
            reason,
            ce_exit.fill_price,
            pe_exit.fill_price,
            gross,
            charges,
            net,
            self.daily_realized_net,
        )
        self.feed.unsubscribe([p.ce_token, p.pe_token])
        self.position = None
        self.phase = "WAITING"
        self._save_state()
        return row

    def _schedule_reentry(self, reason: str, completed_attempt_idx: int) -> bool:
        target_reentry = reason == "PROFIT_TARGET" and self.settings.profit_target_reentry_enabled
        risk_reentry = reason in {"STOPLOSS", "PROFIT_PROTECT"}
        if not (target_reentry or risk_reentry):
            return False
        if completed_attempt_idx >= self.settings.max_reattempts:
            self.log.info("[REENTRY] Maximum re-attempt count reached.")
            return False
        delay_minutes = (
            self.settings.target_delay(completed_attempt_idx)
            if target_reentry
            else self.settings.risk_delay(completed_attempt_idx)
        )
        self.attempt_idx = completed_attempt_idx + 1
        self.reentry_at = now_ist() + timedelta(minutes=delay_minutes)
        cutoff_dt = IST.localize(datetime.combine(now_ist().date(), self.settings.reentry_cutoff_time))
        if self.reentry_at >= cutoff_dt:
            self.log.info(
                "[REENTRY] Next entry %s is at/after cutoff %s; day ends.",
                self.reentry_at.strftime("%H:%M:%S"),
                self.settings.reentry_cutoff_time.strftime("%H:%M"),
            )
            return False
        self.phase = "WAITING_REENTRY"
        self._save_state()
        self.log.info(
            "[REENTRY] %s -> wait %s minute(s); attempt %s scheduled at %s",
            reason,
            delay_minutes,
            self.attempt_idx + 1,
            self.reentry_at.strftime("%H:%M:%S"),
        )
        self.audit.event(
            "REENTRY_SCHEDULED",
            reason=reason,
            completed_attempt=completed_attempt_idx + 1,
            next_attempt=self.attempt_idx + 1,
            delay_minutes=delay_minutes,
            reentry_at=self.reentry_at,
        )
        return True

    def _write_summary(self) -> None:
        # Ensure the summary reports the final number of raw ticks already
        # received for the completed strategy session.
        self.audit.flush_ticks()
        self.finished_at = now_ist()
        net_values = [float(t["net_pnl"]) for t in self.trades]
        reasons = [str(t["exit_reason"]) for t in self.trades]
        row = {
            "date": now_ist().date().isoformat(),
            "build_id": BUILD_ID,
            "mode": "PAPER_ONLY_DEPTH",
            "underlying": self.spec.name,
            "expiry": self.opportunity.expiry.isoformat(),
            "dte": self.opportunity.dte,
            "trades": len(self.trades),
            "wins": sum(x > 0 for x in net_values),
            "losses": sum(x <= 0 for x in net_values),
            "gross_pnl": round(self.daily_realized_gross, 2),
            "charges": round(self.daily_charges, 2),
            "net_pnl": round(self.daily_realized_net, 2),
            "max_trade_profit": max(net_values) if net_values else 0.0,
            "max_trade_loss": min(net_values) if net_values else 0.0,
            "profit_target_exits": reasons.count("PROFIT_TARGET"),
            "stoploss_exits": reasons.count("STOPLOSS"),
            "profit_protect_exits": reasons.count("PROFIT_PROTECT"),
            "time_exits": reasons.count("TIME_EXIT"),
            "raw_ticks_logged": self.audit.raw_ticks_logged,
            "raw_ticks_dropped": self.audit.raw_ticks_dropped,
            "started_at": self.started_at.isoformat(timespec="seconds"),
            "finished_at": self.finished_at.isoformat(timespec="seconds"),
            "session_directory": str(self.audit.session_dir),
        }
        self.audit.summary(row)
        self.audit.event("DAY_SUMMARY", summary=row)
        self.log.info(
            "[DAY DONE] trades=%s gross=Rs%.2f charges=Rs%.2f NET=Rs%.2f | files=%s",
            len(self.trades),
            self.daily_realized_gross,
            self.daily_charges,
            self.daily_realized_net,
            self.audit.session_dir,
        )

    def run(self) -> None:
        self._load_state()
        if self.phase == "DONE":
            self.log.info("[DAY] Persistent state shows that today's strategy run is already complete; no duplicate run.")
            self.audit.event("ALREADY_DONE_SKIP", underlying=self.spec.name, expiry=self.opportunity.expiry)
            return
        self.audit.event(
            "STRATEGY_START",
            build_id=BUILD_ID,
            opportunity={
                "underlying": self.spec.name,
                "expiry": self.opportunity.expiry,
                "dte": self.opportunity.dte,
            },
            settings=asdict(self.settings),
            spec=asdict(self.spec),
        )
        self.log.info("=" * 100)
        self.log.info(
            "[DAY] PAPER ONLY | %s expiry=%s DTE=%s qty=%s | entry=%s exit=%s cutoff=%s",
            self.spec.name,
            self.opportunity.expiry,
            self.opportunity.dte,
            self.spec.qty_units,
            self.settings.entry_time.strftime("%H:%M"),
            self.settings.exit_time.strftime("%H:%M"),
            self.settings.reentry_cutoff_time.strftime("%H:%M"),
        )

        # Resume an open paper position first.
        if self.position is not None:
            reason = self.monitor_until_exit()
            completed = self.position.attempt_idx
            self.exit_position(reason)
            if not self._schedule_reentry(reason, completed):
                self.phase = "DONE"
                self._save_state()
                self._write_summary()
                return

        # Resume a pending re-entry timer.
        if self.phase == "WAITING_REENTRY" and self.reentry_at is not None:
            if not self._wait_until(self.reentry_at, "saved re-entry"):
                self.phase = "DONE"
                self._save_state()
                self._write_summary()
                return
            self.phase = "WAITING"
            self.reentry_at = None
            self._save_state()

        # Initial entry timing.
        entry_dt = IST.localize(datetime.combine(now_ist().date(), self.settings.entry_time))
        if self.attempt_idx == 0 and self.phase == "WAITING":
            if now_ist() < entry_dt:
                if not self._wait_until(entry_dt, "initial entry"):
                    self.phase = "DONE"
                    self._write_summary()
                    return
            elif LATE_START_MODE == "SKIP":
                self.log.warning(
                    "[DAY] Started after exact entry time %s; LATE_START_MODE=SKIP, so no trade.",
                    self.settings.entry_time.strftime("%H:%M"),
                )
                self.audit.event("LATE_START_SKIPPED", entry_time=self.settings.entry_time)
                self.phase = "DONE"
                self._save_state()
                self._write_summary()
                return
            else:
                self.log.warning("[DAY] Late start; entering immediately because LATE_START_MODE=%s", LATE_START_MODE)

        while now_ist().time() < self.settings.exit_time:
            if self.settings.max_daily_loss_rupees > 0 and self.daily_realized_net <= -self.settings.max_daily_loss_rupees:
                self.log.warning(
                    "[BREAKER] Daily net loss Rs%.2f reached limit Rs%.2f.",
                    self.daily_realized_net,
                    self.settings.max_daily_loss_rupees,
                )
                self.audit.event(
                    "DAILY_LOSS_BREAKER",
                    daily_net=self.daily_realized_net,
                    limit=self.settings.max_daily_loss_rupees,
                )
                break
            if now_ist().time() >= self.settings.reentry_cutoff_time:
                self.log.info("[DAY] Fresh-entry cutoff reached.")
                break
            if not self.enter():
                break
            reason = self.monitor_until_exit()
            assert self.position is not None
            completed = self.position.attempt_idx
            self.exit_position(reason)
            if not self._schedule_reentry(reason, completed):
                break
            assert self.reentry_at is not None
            if not self._wait_until(self.reentry_at, f"re-entry attempt {self.attempt_idx+1}"):
                break
            self.phase = "WAITING"
            self.reentry_at = None
            self._save_state()

        if self.position is not None:
            self.exit_position("TIME_EXIT")
        self.phase = "DONE"
        self.reentry_at = None
        self._save_state()
        self._write_summary()


# =============================================================================
# 7. SELF-TESTS
# =============================================================================

def run_self_test() -> None:
    tick = {
        "last_price": 100.0,
        "depth": {
            "buy": [
                {"price": 99.95, "quantity": 100, "orders": 1},
                {"price": 99.90, "quantity": 100, "orders": 1},
                {"price": 99.85, "quantity": 200, "orders": 1},
            ],
            "sell": [
                {"price": 100.05, "quantity": 100, "orders": 1},
                {"price": 100.10, "quantity": 100, "orders": 1},
                {"price": 100.15, "quantity": 200, "orders": 1},
            ],
        },
    }
    sell = simulate_depth_fill(tick, "SELL", 325, 0.05, 1, 4, False)
    buy = simulate_depth_fill(tick, "BUY", 325, 0.05, 1, 4, False)
    assert sell.fill_price < 99.95
    assert buy.fill_price > 100.05
    assert sell.coverage_ratio == 1.0
    assert buy.coverage_ratio == 1.0
    costs = estimate_trade_charges("NFO", 100, 100, 90, 90, 325)
    assert costs["total"] > 80
    assert round_to_step(24776, 50) == 24800
    assert set(ALLOWED_DTE) == {0, 1, 2}, f"Expected ALLOWED_DTE 0,1,2; got {ALLOWED_DTE}"
    assert _settings("NIFTY", 0).entry_time == _settings("NIFTY", 2).entry_time
    assert _settings("SENSEX", 1).profit_target_pct > 0
    print("SELF-TEST PASSED")
    print("SELL fill:", asdict(sell))
    print("BUY fill:", asdict(buy))
    print("Charges:", costs)


# =============================================================================
# 8. MAIN
# =============================================================================

def main() -> int:
    parser = argparse.ArgumentParser(description="Paper-only live DTE-0/DTE-1/DTE-2 short-straddle validator")
    parser.add_argument("--self-test", action="store_true", help="Run pure fill/cost tests without Kite")
    args = parser.parse_args()
    if args.self_test:
        run_self_test()
        return 0

    if oUtils is None:
        raise RuntimeError(
            "OptionTradeUtils_env.py could not be imported. Keep it on PYTHONPATH or beside this script."
        )
    if KiteTicker is None:
        raise RuntimeError("kiteconnect is unavailable. Run: pip install kiteconnect")

    audit = AuditWriter()
    log = audit.build_logger()
    feed: Optional[PriceFeed] = None
    trader: Optional[PaperStraddleTrader] = None
    try:
        log.info("#" * 100)
        log.info("[BOOT] %s", BUILD_ID)
        log.info("[BOOT] HARD SAFETY LOCK: PAPER ONLY; this program has no order-placement code.")
        log.info("[BOOT] Config=%s | output=%s | state=%s", PROPERTY_FILE_PATH, audit.session_dir, STATE_FILE)
        log.info("[BOOT] Allowed DTE=%s | selection=%s | priority=%s", ALLOWED_DTE, TRADE_SELECTION, TRADE_PRIORITY)
        audit.event(
            "BOOT",
            build_id=BUILD_ID,
            config=PROPERTY_FILE_PATH,
            output=audit.session_dir,
            state_file=STATE_FILE,
            allowed_dte=ALLOWED_DTE,
            trade_selection=TRADE_SELECTION,
        )

        kite = _api(oUtils.intialize_kite_api, desc="initialise Kite session")
        api_key = getattr(kite, "api_key", None) or getattr(oUtils, "API_KEY", None)
        access_token = getattr(kite, "access_token", None) or getattr(oUtils, "ACCESS_TOKEN", None)
        if not api_key or not access_token:
            raise RuntimeError(
                "Could not obtain Kite api_key/access_token. Ensure OptionTradeUtils_env exposes a valid authenticated session."
            )

        book = InstrumentBook(kite, log, audit)
        book.load()
        opportunity = book.select_today(now_ist().date())
        log.info(
            "[SELECT] %s selected: expiry=%s DTE=%s selection=%s priority=%s",
            opportunity.spec.name,
            opportunity.expiry,
            opportunity.dte,
            TRADE_SELECTION,
            TRADE_PRIORITY,
        )
        audit.event(
            "OPPORTUNITY_SELECTED",
            underlying=opportunity.spec.name,
            expiry=opportunity.expiry,
            dte=opportunity.dte,
            selection=TRADE_SELECTION,
        )

        feed = PriceFeed(str(api_key), str(access_token), audit, log)
        feed.start()
        trader = PaperStraddleTrader(kite, feed, book, opportunity, audit, log)
        trader.run()
        return 0

    except KeyboardInterrupt:
        if 'log' in locals():
            log.warning("[SHUTDOWN] Interrupted by user.")
        if trader is not None and trader.position is not None:
            # This is paper-only, so no market exposure exists. Preserve state
            # for an explicit resume rather than fabricating an exit quote.
            trader._save_state()
        return 130
    except Exception as exc:
        if 'log' in locals():
            log.exception("[FATAL] %s", exc)
        audit.event("FATAL_ERROR", error=str(exc), traceback=traceback.format_exc())
        if trader is not None:
            trader._save_state()
        return 1
    finally:
        if feed is not None:
            feed.stop()
        audit.close()
        if 'log' in locals():
            audit.generate_excel(log)
            log.info("[SHUTDOWN] Audit directory: %s", audit.session_dir)


if __name__ == "__main__":
    raise SystemExit(main())
