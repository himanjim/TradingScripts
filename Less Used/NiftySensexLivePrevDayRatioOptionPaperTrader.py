#!/usr/bin/env python3
"""
NiftySensexLivePrevDayRatioOptionPaperTrader.py
===============================================

Purpose
-------
Live PAPER-TRADING / VIRTUAL-TRADE monitor for the simpler NIFTY-SENSEX
previous-day average ratio mean-reversion idea.

This script DOES NOT place real orders. It only observes live quotes using
`kite.quote()` polling and records virtual option trades into CSV logs.

Strategy idea
-------------
Instead of using z-score, this script uses the ratio:

    ratio = SENSEX / NIFTY

For the current trading day, it computes a fixed baseline from the previous
completed trading day:

    prev_day_avg_ratio = average(SENSEX / NIFTY) over the previous trading day

During live market, it calculates:

    deviation_pct = ((current_ratio / prev_day_avg_ratio) - 1) * 100

Entry:
    If deviation_pct >= ENTRY_DEVIATION_PCT:
        SENSEX is rich relative to NIFTY.
        Futures-equivalent trade: short SENSEX, long NIFTY.
        Option proxy used here: BUY SENSEX ITM PUT + BUY NIFTY ITM CALL.

    If deviation_pct <= -ENTRY_DEVIATION_PCT:
        SENSEX is cheap relative to NIFTY.
        Futures-equivalent trade: long SENSEX, short NIFTY.
        Option proxy used here: BUY SENSEX ITM CALL + BUY NIFTY ITM PUT.

Exit:
    Exit virtual trade when abs(deviation_pct) <= SETTLE_DEVIATION_PCT.

Why this file exists
--------------------
Your earlier z-score live logger monitored:

    spread = log(SENSEX) - beta * log(NIFTY)
    z = (spread - rolling_mean) / rolling_std

This file deliberately avoids z-score and rolling-window rebasing. It freezes
one daily baseline: previous day's average SENSEX/NIFTY ratio.

Important limitation
--------------------
This is a paper-trading logger. It is not a live-ordering bot. Actual trading
will face option bid-ask spreads, slippage, liquidity gaps, order execution
latency, option theta/gamma/vega, expiry mismatch, and taxes/charges.

Dependencies
------------
pip install pandas numpy python-dateutil kiteconnect openpyxl

Your existing project dependency:
    Trading_2024.OptionTradeUtils

Typical Windows CMD run
-----------------------
set NIFTY_EXPIRY_DATE=2026-06-02
set SENSEX_EXPIRY_DATE=2026-06-04
set ENTRY_DEVIATION_PCT=0.03
set SETTLE_DEVIATION_PCT=0.01
set ITM_STRIKES_AWAY=2
python NiftySensexLivePrevDayRatioOptionPaperTrader.py

Output logs
-----------
Default output folder:
    ./nifty_sensex_live_prevday_ratio_logs/YYYYMMDD/

Files:
    market_state_YYYYMMDD.csv       every POLL_INTERVAL_SEC seconds
    trade_events_YYYYMMDD.csv       signal / entry / exit / quote errors
    virtual_trades_YYYYMMDD.csv     completed virtual trades
    startup_config_YYYYMMDD.csv     run configuration and baseline info

Stop safely
-----------
Press Ctrl+C. The script will flush logs and exit.
"""

from __future__ import annotations

import csv
import math
import os
import signal
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

import Trading_2024 as oUtils

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


# =============================================================================
# CONFIGURATION
# =============================================================================

SESSION_START = dtime(9, 15, 0)
SESSION_END = dtime(15, 30, 0)

# Entry time filter. Keep ON by default because the first 15 minutes often have
# gap adjustment / noisy index-ratio behaviour.
ENABLE_ENTRY_TIME_FILTER = os.environ.get("ENABLE_ENTRY_TIME_FILTER", "1").strip().lower() in {"1", "true", "yes", "y"}
ENTRY_START_TIME = os.environ.get("ENTRY_START_TIME", "09:30")
LAST_ENTRY_TIME = os.environ.get("LAST_ENTRY_TIME", "14:30")

# Force virtual exit near close. This avoids overnight option-risk in paper logs.
FORCE_EXIT_TIME = os.environ.get("FORCE_EXIT_TIME", "15:20")
EXIT_ON_FORCE_EXIT_TIME = os.environ.get("EXIT_ON_FORCE_EXIT_TIME", "1").strip().lower() in {"1", "true", "yes", "y"}

OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "nifty_sensex_live_prevday_ratio_logs")
POLL_INTERVAL_SEC = float(os.environ.get("POLL_INTERVAL_SEC", "1.0"))

# Main strategy thresholds in percent points.
# Example: 0.03 means 0.03%, not 3%.
ENTRY_DEVIATION_PCT = float(os.environ.get("ENTRY_DEVIATION_PCT", "0.03"))
SETTLE_DEVIATION_PCT = float(os.environ.get("SETTLE_DEVIATION_PCT", "0.01"))

# Rearm prevents repeated entries inside the same unresolved deviation episode.
# After a trade exits, the system will not enter again until abs(deviation_pct)
# falls below this value.
REARM_DEVIATION_PCT = float(os.environ.get("REARM_DEVIATION_PCT", str(SETTLE_DEVIATION_PCT)))

# Baseline settings. The script automatically finds the most recent completed
# trading day from historical data and uses its average SENSEX/NIFTY ratio.
# You may override the baseline date if needed.
BASELINE_DATE_ENV = os.environ.get("BASELINE_DATE", "").strip()  # YYYY-MM-DD or DD-MM-YYYY, optional
SEED_LOOKBACK_CALENDAR_DAYS = int(os.environ.get("SEED_LOOKBACK_CALENDAR_DAYS", "15"))
MIN_BASELINE_ALIGNED_ROWS = int(os.environ.get("MIN_BASELINE_ALIGNED_ROWS", "250"))
MAX_DAYS_PER_CHUNK = int(os.environ.get("MAX_DAYS_PER_CHUNK", "10"))
MAX_ATTEMPTS = int(os.environ.get("MAX_ATTEMPTS", "5"))
SLEEP_BETWEEN_CALLS_SEC = float(os.environ.get("SLEEP_BETWEEN_CALLS_SEC", "0.20"))

# Expiry dates MUST be configured by you before market open.
NIFTY_EXPIRY_DATE_ENV = os.environ.get("NIFTY_EXPIRY_DATE", "2026-06-02").strip()
SENSEX_EXPIRY_DATE_ENV = os.environ.get("SENSEX_EXPIRY_DATE", "2026-06-04").strip()

# Option selection. 2 strikes away ITM is the default requested proxy.
ITM_STRIKES_AWAY = int(os.environ.get("ITM_STRIKES_AWAY", "2"))
NIFTY_STRIKE_STEP = int(os.environ.get("NIFTY_STRIKE_STEP", "50"))
SENSEX_STRIKE_STEP = int(os.environ.get("SENSEX_STRIKE_STEP", "100"))

# Virtual quantities. Verify current exchange lot sizes before any real use.
NIFTY_OPTION_QTY = int(os.environ.get("NIFTY_OPTION_QTY", "325"))
SENSEX_OPTION_QTY = int(os.environ.get("SENSEX_OPTION_QTY", "100"))

# Optional paper risk/hold controls. They are disabled by default except close-time exit.
# MAX_HOLD_SECONDS=14400 roughly equals 4 hours; 0 disables.
MAX_HOLD_SECONDS = int(os.environ.get("MAX_HOLD_SECONDS", "0"))
STOP_LOSS_RUPEES = float(os.environ.get("STOP_LOSS_RUPEES", "0"))
TARGET_PROFIT_RUPEES = float(os.environ.get("TARGET_PROFIT_RUPEES", "0"))

# Realistic PnL for long options uses ask for entry and bid for exit/MTM.
# LTP PnL is also logged separately.
USE_BID_ASK_REALISTIC_PNL = os.environ.get("USE_BID_ASK_REALISTIC_PNL", "1").strip().lower() in {"1", "true", "yes", "y"}

# Kite instruments.
NIFTY_INDEX_EXCHANGE = "NSE"
NIFTY_INDEX_SYMBOL = "NIFTY 50"
SENSEX_INDEX_EXCHANGE = "BSE"
SENSEX_INDEX_SYMBOL = "SENSEX"

NIFTY_OPTION_EXCHANGE = "NFO"
NIFTY_OPTION_PREFIX = "NIFTY"
SENSEX_OPTION_EXCHANGE = "BFO"
SENSEX_OPTION_PREFIX = "SENSEX"

NIFTY_INDEX_KEY = f"{NIFTY_INDEX_EXCHANGE}:{NIFTY_INDEX_SYMBOL}"
SENSEX_INDEX_KEY = f"{SENSEX_INDEX_EXCHANGE}:{SENSEX_INDEX_SYMBOL}"


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass(frozen=True)
class InstrumentSpec:
    label: str
    exchange: str
    tradingsymbol: str


@dataclass(frozen=True)
class OptionInstrument:
    label: str
    exchange: str
    tradingsymbol: str
    instrument_token: int
    strike: int
    option_type: str  # CE or PE
    expiry: date
    lot_size: int

    @property
    def quote_key(self) -> str:
        return f"{self.exchange}:{self.tradingsymbol}"


@dataclass
class OptionLeg:
    underlying_label: str  # NIFTY or SENSEX
    action: str            # BUY only in this option proxy
    option: OptionInstrument
    qty: int
    entry_ltp: float
    entry_bid: float
    entry_ask: float
    entry_price_realistic: float


@dataclass
class VirtualTrade:
    trade_id: int
    side: str
    signal_time: datetime
    entry_time: datetime
    entry_deviation_pct: float
    entry_abs_deviation_pct: float
    entry_ratio: float
    entry_baseline_ratio: float
    entry_nifty: float
    entry_sensex: float
    legs: List[OptionLeg]
    max_ltp_pnl: float = 0.0
    min_ltp_pnl: float = 0.0
    max_realistic_pnl: float = 0.0
    min_realistic_pnl: float = 0.0
    last_ltp_pnl: float = 0.0
    last_realistic_pnl: float = 0.0
    last_deviation_pct: float = 0.0
    last_abs_deviation_pct: float = 0.0


@dataclass
class LiveState:
    baseline_ratio: float
    baseline_date: date
    baseline_rows: int
    active_trade: Optional[VirtualTrade] = None
    trade_id_counter: int = 0
    rearmed: bool = True
    stop_requested: bool = False


# =============================================================================
# GENERIC HELPERS
# =============================================================================

def ist_now() -> datetime:
    """Return timezone-naive current datetime in Asia/Kolkata."""
    if ZoneInfo is not None:
        try:
            return datetime.now(ZoneInfo("Asia/Kolkata")).replace(tzinfo=None)
        except Exception:
            pass
    return datetime.now()


def parse_hhmm(raw: str) -> dtime:
    """Parse HH:MM or HH:MM:SS string into a Python time object."""
    raw = raw.strip()
    for fmt in ("%H:%M", "%H:%M:%S"):
        try:
            return datetime.strptime(raw, fmt).time()
        except ValueError:
            continue
    raise ValueError(f"Invalid time value: {raw!r}. Use HH:MM or HH:MM:SS.")


def parse_date(raw: str) -> date:
    """Parse YYYY-MM-DD or DD-MM-YYYY date strings."""
    raw = raw.strip()
    for fmt in ("%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Could not parse date: {raw!r}. Use YYYY-MM-DD or DD-MM-YYYY.")


def normalize_expiry(e) -> date:
    """Normalize expiry from Kite instruments dump to Python date."""
    if isinstance(e, date) and not isinstance(e, datetime):
        return e
    if isinstance(e, datetime):
        return e.date()
    if isinstance(e, str):
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(e, fmt).date()
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(e).date()
        except Exception:
            pass
    raise ValueError(f"Cannot parse expiry: {e!r}")


def round_to_step(value: float, step: int) -> int:
    """Round an index level to nearest strike step."""
    return int(round(value / step) * step)


def in_entry_time_window(now: datetime) -> bool:
    """Return True when entries are allowed by configured time filter."""
    if not ENABLE_ENTRY_TIME_FILTER:
        return True
    t = now.time()
    return parse_hhmm(ENTRY_START_TIME) <= t <= parse_hhmm(LAST_ENTRY_TIME)


def should_force_exit_by_time(now: datetime) -> bool:
    """Return True when a virtual trade should be force-exited near close."""
    if not EXIT_ON_FORCE_EXIT_TIME:
        return False
    return now.time() >= parse_hhmm(FORCE_EXIT_TIME)


def safe_float(x, default: float = np.nan) -> float:
    """Safely convert a value to float."""
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def best_bid_ask(q: Dict) -> Tuple[float, float]:
    """Extract top bid and ask from Kite quote depth."""
    try:
        depth = q.get("depth") or {}
        buy = depth.get("buy") or []
        sell = depth.get("sell") or []
        bid = safe_float(buy[0].get("price")) if buy else np.nan
        ask = safe_float(sell[0].get("price")) if sell else np.nan
        return bid, ask
    except Exception:
        return np.nan, np.nan


def compute_ratio_deviation(nifty_ltp: float, sensex_ltp: float, baseline_ratio: float) -> Tuple[float, float, float]:
    """
    Compute current ratio and percentage deviation from previous-day baseline.

    Returns:
        current_ratio, deviation_pct, abs_deviation_pct
    """
    if nifty_ltp <= 0 or sensex_ltp <= 0 or baseline_ratio <= 0:
        raise ValueError("Invalid values for ratio deviation calculation.")
    current_ratio = sensex_ltp / nifty_ltp
    deviation_pct = ((current_ratio / baseline_ratio) - 1.0) * 100.0
    return float(current_ratio), float(deviation_pct), float(abs(deviation_pct))


# =============================================================================
# CSV LOGGER
# =============================================================================

class CsvLogger:
    """Append-only CSV logger with headers."""

    def __init__(self, path: str, fieldnames: List[str]):
        self.path = path
        self.fieldnames = fieldnames
        os.makedirs(os.path.dirname(path), exist_ok=True)
        file_exists = os.path.exists(path) and os.path.getsize(path) > 0
        self.fh = open(path, "a", newline="", encoding="utf-8")
        self.writer = csv.DictWriter(self.fh, fieldnames=fieldnames, extrasaction="ignore")
        if not file_exists:
            self.writer.writeheader()
            self.fh.flush()

    def write(self, row: Dict) -> None:
        clean = {k: row.get(k, "") for k in self.fieldnames}
        self.writer.writerow(clean)
        self.fh.flush()

    def close(self) -> None:
        try:
            self.fh.flush()
            self.fh.close()
        except Exception:
            pass


@dataclass
class Loggers:
    market_state: CsvLogger
    trade_events: CsvLogger
    virtual_trades: CsvLogger
    startup_config: CsvLogger

    def close(self) -> None:
        self.market_state.close()
        self.trade_events.close()
        self.virtual_trades.close()
        self.startup_config.close()


def setup_loggers() -> Tuple[str, Loggers]:
    """Create dated output folder and CSV loggers."""
    run_date = ist_now().strftime("%Y%m%d")
    run_dir = os.path.join(OUTPUT_DIR, run_date)
    os.makedirs(run_dir, exist_ok=True)

    market_fields = [
        "timestamp", "state", "rearmed",
        "nifty_ltp", "sensex_ltp", "current_ratio", "baseline_ratio", "baseline_date",
        "deviation_pct", "abs_deviation_pct",
        "active_trade_id", "active_side", "active_entry_time", "active_entry_deviation_pct",
        "nifty_option", "sensex_option",
        "nifty_option_ltp", "sensex_option_ltp",
        "nifty_option_bid", "nifty_option_ask", "sensex_option_bid", "sensex_option_ask",
        "ltp_pnl", "realistic_pnl", "max_ltp_pnl", "min_ltp_pnl", "max_realistic_pnl", "min_realistic_pnl",
        "quote_keys_count", "missing_quote_keys", "message",
    ]
    event_fields = [
        "timestamp", "event_type", "trade_id", "side", "deviation_pct", "abs_deviation_pct", "current_ratio", "baseline_ratio",
        "nifty_ltp", "sensex_ltp", "nifty_option", "sensex_option", "ltp_pnl", "realistic_pnl", "message",
    ]
    trade_fields = [
        "trade_id", "side", "signal_time", "entry_time", "exit_time", "exit_reason",
        "entry_deviation_pct", "entry_abs_deviation_pct", "exit_deviation_pct", "exit_abs_deviation_pct",
        "entry_ratio", "exit_ratio", "baseline_ratio", "baseline_date",
        "entry_nifty", "entry_sensex", "exit_nifty", "exit_sensex",
        "nifty_option", "nifty_qty", "nifty_entry_ltp", "nifty_entry_bid", "nifty_entry_ask", "nifty_exit_ltp", "nifty_exit_bid", "nifty_exit_ask",
        "sensex_option", "sensex_qty", "sensex_entry_ltp", "sensex_entry_bid", "sensex_entry_ask", "sensex_exit_ltp", "sensex_exit_bid", "sensex_exit_ask",
        "ltp_exit_pnl", "realistic_exit_pnl", "max_ltp_pnl", "min_ltp_pnl", "max_realistic_pnl", "min_realistic_pnl",
        "hold_seconds",
    ]
    config_fields = ["timestamp", "parameter", "value"]

    logs = Loggers(
        market_state=CsvLogger(os.path.join(run_dir, f"market_state_{run_date}.csv"), market_fields),
        trade_events=CsvLogger(os.path.join(run_dir, f"trade_events_{run_date}.csv"), event_fields),
        virtual_trades=CsvLogger(os.path.join(run_dir, f"virtual_trades_{run_date}.csv"), trade_fields),
        startup_config=CsvLogger(os.path.join(run_dir, f"startup_config_{run_date}.csv"), config_fields),
    )
    return run_dir, logs


def write_config(logs: Loggers, state: LiveState) -> None:
    """Write run configuration and baseline details."""
    params = {
        "strategy": "previous_day_avg_ratio_deviation",
        "ENTRY_DEVIATION_PCT": ENTRY_DEVIATION_PCT,
        "SETTLE_DEVIATION_PCT": SETTLE_DEVIATION_PCT,
        "REARM_DEVIATION_PCT": REARM_DEVIATION_PCT,
        "POLL_INTERVAL_SEC": POLL_INTERVAL_SEC,
        "baseline_date": state.baseline_date,
        "baseline_ratio": state.baseline_ratio,
        "baseline_rows": state.baseline_rows,
        "BASELINE_DATE_ENV": BASELINE_DATE_ENV,
        "SEED_LOOKBACK_CALENDAR_DAYS": SEED_LOOKBACK_CALENDAR_DAYS,
        "NIFTY_EXPIRY_DATE": NIFTY_EXPIRY_DATE_ENV,
        "SENSEX_EXPIRY_DATE": SENSEX_EXPIRY_DATE_ENV,
        "ITM_STRIKES_AWAY": ITM_STRIKES_AWAY,
        "NIFTY_STRIKE_STEP": NIFTY_STRIKE_STEP,
        "SENSEX_STRIKE_STEP": SENSEX_STRIKE_STEP,
        "NIFTY_OPTION_QTY": NIFTY_OPTION_QTY,
        "SENSEX_OPTION_QTY": SENSEX_OPTION_QTY,
        "ENABLE_ENTRY_TIME_FILTER": ENABLE_ENTRY_TIME_FILTER,
        "ENTRY_START_TIME": ENTRY_START_TIME,
        "LAST_ENTRY_TIME": LAST_ENTRY_TIME,
        "FORCE_EXIT_TIME": FORCE_EXIT_TIME,
        "EXIT_ON_FORCE_EXIT_TIME": EXIT_ON_FORCE_EXIT_TIME,
        "MAX_HOLD_SECONDS": MAX_HOLD_SECONDS,
        "STOP_LOSS_RUPEES": STOP_LOSS_RUPEES,
        "TARGET_PROFIT_RUPEES": TARGET_PROFIT_RUPEES,
        "USE_BID_ASK_REALISTIC_PNL": USE_BID_ASK_REALISTIC_PNL,
        "note": "Paper trading only. Uses kite.quote polling. No real orders are placed.",
    }
    ts = ist_now().isoformat(sep=" ")
    for k, v in params.items():
        logs.startup_config.write({"timestamp": ts, "parameter": k, "value": v})


# =============================================================================
# KITE INSTRUMENT / HISTORY HELPERS
# =============================================================================

def kite_instruments_cached(kite, exchange: str, cache: Dict[str, List[Dict]]) -> List[Dict]:
    """Load and cache Kite instruments dump for an exchange."""
    ex = exchange.upper().strip()
    if ex not in cache:
        print(f"[STEP] Loading instruments dump for {ex} ...")
        cache[ex] = kite.instruments(ex)
        print(f"[INFO] {ex} instruments loaded: {len(cache[ex])}")
    return cache[ex]


def get_instrument_token(kite, spec: InstrumentSpec, cache: Dict[str, List[Dict]]) -> int:
    """Resolve instrument token from exchange and tradingsymbol."""
    rows = kite_instruments_cached(kite, spec.exchange, cache)
    wanted = spec.tradingsymbol.upper().strip()
    for r in rows:
        if str(r.get("tradingsymbol", "")).upper().strip() == wanted:
            return int(r["instrument_token"])
    raise ValueError(f"Instrument not found: {spec.exchange}:{spec.tradingsymbol}")


def iter_chunks_by_date(from_dt: datetime, to_dt: datetime, days_per_chunk: int) -> List[Tuple[datetime, datetime]]:
    """Split datetime range into date chunks while preserving intraday times."""
    if from_dt > to_dt:
        raise ValueError("from_dt must be <= to_dt")
    chunks: List[Tuple[datetime, datetime]] = []
    cur = from_dt.date()
    end_d = to_dt.date()
    while cur <= end_d:
        chunk_end_d = min(cur + timedelta(days=days_per_chunk - 1), end_d)
        c_from = from_dt if cur == from_dt.date() else datetime.combine(cur, SESSION_START)
        c_to = to_dt if chunk_end_d == end_d else datetime.combine(chunk_end_d, SESSION_END)
        chunks.append((c_from, c_to))
        cur = chunk_end_d + timedelta(days=1)
    return chunks


def fetch_history_minute(kite, token: int, from_dt: datetime, to_dt: datetime, label: str) -> List[Dict]:
    """Fetch 1-minute history with chunking and retries."""
    chunks = iter_chunks_by_date(from_dt, to_dt, MAX_DAYS_PER_CHUNK)
    rows_all: List[Dict] = []
    print(f"[INFO] Fetching seed history for {label}, chunks={len(chunks)}")

    for idx, (c_from, c_to) in enumerate(chunks, start=1):
        last_err: Optional[Exception] = None
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                rows = kite.historical_data(
                    instrument_token=token,
                    from_date=c_from,
                    to_date=c_to,
                    interval="minute",
                    continuous=False,
                    oi=False,
                )
                rows_all.extend(rows)
                last_err = None
                break
            except Exception as e:  # noqa: BLE001
                last_err = e
                wait = min(8.0, 1.5 * attempt)
                print(f"[WARN] {label} chunk {idx}/{len(chunks)} attempt {attempt} failed: {e}; sleep {wait:.1f}s")
                time.sleep(wait)
        if last_err is not None:
            print(f"[ERROR] Giving up on {label} chunk {idx}/{len(chunks)}: {last_err}")
        time.sleep(SLEEP_BETWEEN_CALLS_SEC)
    return rows_all


def rows_to_df(rows: List[Dict], close_col: str) -> pd.DataFrame:
    """Convert Kite historical rows to date + close DataFrame."""
    if not rows:
        return pd.DataFrame(columns=["date", close_col])
    df = pd.DataFrame(rows)
    if "date" not in df.columns or "close" not in df.columns:
        return pd.DataFrame(columns=["date", close_col])
    df["date"] = pd.to_datetime(df["date"])
    try:
        if df["date"].dt.tz is not None:
            df["date"] = df["date"].dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    except Exception:
        df["date"] = pd.to_datetime(df["date"].astype(str), errors="coerce")
    df["date"] = df["date"].dt.floor("min")
    df = df.dropna(subset=["date", "close"])
    df = df.drop_duplicates("date", keep="last").sort_values("date")
    df = df.rename(columns={"close": close_col})[["date", close_col]]
    return df.reset_index(drop=True)


def seed_prev_day_ratio_baseline(kite, cache: Dict[str, List[Dict]]) -> Tuple[float, date, int]:
    """
    Compute previous-day average SENSEX/NIFTY ratio from recent 1-minute history.

    If BASELINE_DATE is provided, that exact trading date is used. Otherwise,
    the script picks the latest available trading date strictly before today.
    """
    today = ist_now().date()
    end_dt = datetime.combine(today, SESSION_END)
    start_dt = end_dt - timedelta(days=SEED_LOOKBACK_CALENDAR_DAYS)

    nifty_token = get_instrument_token(kite, InstrumentSpec("NIFTY", NIFTY_INDEX_EXCHANGE, NIFTY_INDEX_SYMBOL), cache)
    sensex_token = get_instrument_token(kite, InstrumentSpec("SENSEX", SENSEX_INDEX_EXCHANGE, SENSEX_INDEX_SYMBOL), cache)

    nifty_rows = fetch_history_minute(kite, nifty_token, start_dt, end_dt, "NIFTY 50")
    sensex_rows = fetch_history_minute(kite, sensex_token, start_dt, end_dt, "SENSEX")

    ndf = rows_to_df(nifty_rows, "nifty_close")
    sdf = rows_to_df(sensex_rows, "sensex_close")
    merged = pd.merge(ndf, sdf, on="date", how="inner")
    merged = merged[(merged["nifty_close"] > 0) & (merged["sensex_close"] > 0)].copy()
    if merged.empty:
        raise RuntimeError("No aligned NIFTY/SENSEX historical data found for baseline.")

    merged["trading_date"] = merged["date"].dt.date
    merged["ratio"] = merged["sensex_close"] / merged["nifty_close"]

    if BASELINE_DATE_ENV:
        baseline_date = parse_date(BASELINE_DATE_ENV)
    else:
        available_dates = sorted(d for d in merged["trading_date"].unique() if d < today)
        if not available_dates:
            raise RuntimeError("No completed previous trading day found in seed history.")
        baseline_date = available_dates[-1]

    base = merged[merged["trading_date"] == baseline_date].copy()
    if len(base) < MIN_BASELINE_ALIGNED_ROWS:
        raise RuntimeError(
            f"Baseline date {baseline_date} has only {len(base)} aligned rows; "
            f"need at least {MIN_BASELINE_ALIGNED_ROWS}. Increase lookback or set BASELINE_DATE."
        )

    baseline_ratio = float(base["ratio"].mean())
    print(f"[INFO] Baseline date={baseline_date}, rows={len(base)}, prev_day_avg_ratio={baseline_ratio:.8f}")
    return baseline_ratio, baseline_date, int(len(base))


# =============================================================================
# OPTION LOOKUP
# =============================================================================

def find_option_instrument(
    instruments: List[Dict],
    label: str,
    exchange: str,
    prefix: str,
    expiry: date,
    strike: int,
    option_type: str,
) -> OptionInstrument:
    """Find option instrument by prefix, expiry, strike, and CE/PE."""
    prefix_u = prefix.upper().strip()
    opt_u = option_type.upper().strip()

    matches: List[Dict] = []
    for r in instruments:
        try:
            tsym = str(r.get("tradingsymbol", "")).upper().strip()
            inst_type = str(r.get("instrument_type", "")).upper().strip()
            exp = normalize_expiry(r.get("expiry"))
            st = int(float(r.get("strike") or 0))
            if tsym.startswith(prefix_u) and inst_type == opt_u and exp == expiry and st == strike:
                matches.append(r)
        except Exception:
            continue

    if not matches:
        raise ValueError(f"Option not found: {exchange}:{prefix} expiry={expiry} strike={strike} type={option_type}")

    r = matches[0]
    return OptionInstrument(
        label=label,
        exchange=str(r.get("exchange", exchange)),
        tradingsymbol=str(r["tradingsymbol"]),
        instrument_token=int(r["instrument_token"]),
        strike=int(float(r.get("strike") or strike)),
        option_type=opt_u,
        expiry=normalize_expiry(r.get("expiry")),
        lot_size=int(r.get("lot_size") or 0),
    )


def select_itm_options_for_signal(
    deviation_pct: float,
    nifty_ltp: float,
    sensex_ltp: float,
    nfo: List[Dict],
    bfo: List[Dict],
    nifty_expiry: date,
    sensex_expiry: date,
) -> Tuple[str, OptionInstrument, OptionInstrument]:
    """
    Select configured ITM options for current ratio-deviation signal.

    Positive deviation:
        SENSEX/NIFTY ratio is above previous-day baseline.
        SENSEX is rich. Buy SENSEX PUT + NIFTY CALL.

    Negative deviation:
        SENSEX/NIFTY ratio is below previous-day baseline.
        SENSEX is cheap. Buy SENSEX CALL + NIFTY PUT.
    """
    nifty_atm = round_to_step(nifty_ltp, NIFTY_STRIKE_STEP)
    sensex_atm = round_to_step(sensex_ltp, SENSEX_STRIKE_STEP)

    if deviation_pct > 0:
        side = "SENSEX_RICH_BUY_SENSEX_PUT_BUY_NIFTY_CALL"
        nifty_type = "CE"
        sensex_type = "PE"
        nifty_strike = nifty_atm - ITM_STRIKES_AWAY * NIFTY_STRIKE_STEP
        sensex_strike = sensex_atm + ITM_STRIKES_AWAY * SENSEX_STRIKE_STEP
    else:
        side = "SENSEX_CHEAP_BUY_SENSEX_CALL_BUY_NIFTY_PUT"
        nifty_type = "PE"
        sensex_type = "CE"
        nifty_strike = nifty_atm + ITM_STRIKES_AWAY * NIFTY_STRIKE_STEP
        sensex_strike = sensex_atm - ITM_STRIKES_AWAY * SENSEX_STRIKE_STEP

    nifty_option = find_option_instrument(
        nfo, "NIFTY_OPTION", NIFTY_OPTION_EXCHANGE, NIFTY_OPTION_PREFIX,
        nifty_expiry, nifty_strike, nifty_type,
    )
    sensex_option = find_option_instrument(
        bfo, "SENSEX_OPTION", SENSEX_OPTION_EXCHANGE, SENSEX_OPTION_PREFIX,
        sensex_expiry, sensex_strike, sensex_type,
    )
    return side, nifty_option, sensex_option


# =============================================================================
# LIVE QUOTE / PNL LOGIC
# =============================================================================

def get_quote_batch(kite, keys: List[str], logs: Optional[Loggers] = None) -> Dict[str, Dict]:
    """Fetch quote snapshots for all keys in one kite.quote() call."""
    unique_keys = list(dict.fromkeys(keys))
    try:
        return kite.quote(unique_keys)
    except Exception as e:  # noqa: BLE001
        msg = f"kite.quote failed for {len(unique_keys)} instruments: {e}"
        print(f"[WARN] {msg}")
        if logs is not None:
            logs.trade_events.write({
                "timestamp": ist_now().isoformat(sep=" "),
                "event_type": "QUOTE_ERROR",
                "message": msg,
            })
        return {}


def compute_option_pnls(trade: VirtualTrade, quotes: Dict[str, Dict]) -> Tuple[float, float, Dict[str, Dict[str, float]]]:
    """
    Compute current option PnL using both LTP and bid/ask realistic modes.

    For long option legs:
        LTP PnL       = (current_ltp - entry_ltp) * qty
        Realistic PnL = (current_bid - entry_ask) * qty
    """
    ltp_pnl = 0.0
    realistic_pnl = 0.0
    leg_snapshots: Dict[str, Dict[str, float]] = {}

    for leg in trade.legs:
        key = leg.option.quote_key
        q = quotes.get(key, {})
        ltp = safe_float(q.get("last_price"))
        bid, ask = best_bid_ask(q)

        if not math.isfinite(ltp):
            ltp = leg.entry_ltp
        exit_price_realistic = bid if math.isfinite(bid) and bid > 0 else ltp

        leg_ltp_pnl = (ltp - leg.entry_ltp) * leg.qty
        leg_realistic_pnl = (exit_price_realistic - leg.entry_price_realistic) * leg.qty
        ltp_pnl += leg_ltp_pnl
        realistic_pnl += leg_realistic_pnl

        leg_snapshots[leg.underlying_label] = {
            "ltp": ltp,
            "bid": bid,
            "ask": ask,
            "ltp_pnl": leg_ltp_pnl,
            "realistic_pnl": leg_realistic_pnl,
        }

    return float(ltp_pnl), float(realistic_pnl), leg_snapshots


def build_quote_keys(state: LiveState) -> List[str]:
    """Return quote keys to fetch in the next kite.quote call."""
    keys = [NIFTY_INDEX_KEY, SENSEX_INDEX_KEY]
    if state.active_trade is not None:
        for leg in state.active_trade.legs:
            keys.append(leg.option.quote_key)
    return list(dict.fromkeys(keys))


def start_virtual_trade(
    kite,
    state: LiveState,
    logs: Loggers,
    now: datetime,
    deviation_pct: float,
    abs_deviation_pct: float,
    current_ratio: float,
    nifty_ltp: float,
    sensex_ltp: float,
    nfo: List[Dict],
    bfo: List[Dict],
    nifty_expiry: date,
    sensex_expiry: date,
) -> None:
    """Create a virtual option trade after ratio-deviation signal."""
    try:
        side, nifty_opt, sensex_opt = select_itm_options_for_signal(
            deviation_pct=deviation_pct,
            nifty_ltp=nifty_ltp,
            sensex_ltp=sensex_ltp,
            nfo=nfo,
            bfo=bfo,
            nifty_expiry=nifty_expiry,
            sensex_expiry=sensex_expiry,
        )
    except Exception as e:  # noqa: BLE001
        logs.trade_events.write({
            "timestamp": now.isoformat(sep=" "),
            "event_type": "OPTION_LOOKUP_FAILED",
            "deviation_pct": deviation_pct,
            "abs_deviation_pct": abs_deviation_pct,
            "current_ratio": current_ratio,
            "baseline_ratio": state.baseline_ratio,
            "nifty_ltp": nifty_ltp,
            "sensex_ltp": sensex_ltp,
            "message": str(e),
        })
        return

    option_quotes = get_quote_batch(kite, [nifty_opt.quote_key, sensex_opt.quote_key], logs)
    if nifty_opt.quote_key not in option_quotes or sensex_opt.quote_key not in option_quotes:
        logs.trade_events.write({
            "timestamp": now.isoformat(sep=" "),
            "event_type": "OPTION_QUOTE_MISSING_AT_ENTRY",
            "deviation_pct": deviation_pct,
            "abs_deviation_pct": abs_deviation_pct,
            "nifty_option": nifty_opt.quote_key,
            "sensex_option": sensex_opt.quote_key,
            "message": "One or both selected option quotes missing; virtual entry skipped.",
        })
        return

    legs: List[OptionLeg] = []
    for underlying, opt, qty in [
        ("NIFTY", nifty_opt, NIFTY_OPTION_QTY),
        ("SENSEX", sensex_opt, SENSEX_OPTION_QTY),
    ]:
        q = option_quotes[opt.quote_key]
        ltp = safe_float(q.get("last_price"))
        bid, ask = best_bid_ask(q)
        if not math.isfinite(ltp) or ltp <= 0:
            logs.trade_events.write({
                "timestamp": now.isoformat(sep=" "),
                "event_type": "BAD_OPTION_LTP_AT_ENTRY",
                "deviation_pct": deviation_pct,
                "abs_deviation_pct": abs_deviation_pct,
                "nifty_option": nifty_opt.quote_key,
                "sensex_option": sensex_opt.quote_key,
                "message": f"Bad LTP for {opt.quote_key}: {ltp}",
            })
            return
        entry_realistic = ask if USE_BID_ASK_REALISTIC_PNL and math.isfinite(ask) and ask > 0 else ltp
        legs.append(OptionLeg(
            underlying_label=underlying,
            action="BUY",
            option=opt,
            qty=qty,
            entry_ltp=ltp,
            entry_bid=bid,
            entry_ask=ask,
            entry_price_realistic=entry_realistic,
        ))

    state.trade_id_counter += 1
    trade = VirtualTrade(
        trade_id=state.trade_id_counter,
        side=side,
        signal_time=now,
        entry_time=now,
        entry_deviation_pct=deviation_pct,
        entry_abs_deviation_pct=abs_deviation_pct,
        entry_ratio=current_ratio,
        entry_baseline_ratio=state.baseline_ratio,
        entry_nifty=nifty_ltp,
        entry_sensex=sensex_ltp,
        legs=legs,
        last_deviation_pct=deviation_pct,
        last_abs_deviation_pct=abs_deviation_pct,
    )
    state.active_trade = trade
    state.rearmed = False

    logs.trade_events.write({
        "timestamp": now.isoformat(sep=" "),
        "event_type": "VIRTUAL_ENTRY",
        "trade_id": trade.trade_id,
        "side": trade.side,
        "deviation_pct": deviation_pct,
        "abs_deviation_pct": abs_deviation_pct,
        "current_ratio": current_ratio,
        "baseline_ratio": state.baseline_ratio,
        "nifty_ltp": nifty_ltp,
        "sensex_ltp": sensex_ltp,
        "nifty_option": nifty_opt.quote_key,
        "sensex_option": sensex_opt.quote_key,
        "message": "Virtual option trade entered.",
    })


def exit_virtual_trade(
    state: LiveState,
    logs: Loggers,
    now: datetime,
    reason: str,
    current_ratio: float,
    deviation_pct: float,
    abs_deviation_pct: float,
    nifty_ltp: float,
    sensex_ltp: float,
    quotes: Dict[str, Dict],
) -> None:
    """Close active virtual trade and write completed trade row."""
    trade = state.active_trade
    if trade is None:
        return

    ltp_pnl, realistic_pnl, snaps = compute_option_pnls(trade, quotes)

    nifty_leg = next((x for x in trade.legs if x.underlying_label == "NIFTY"), None)
    sensex_leg = next((x for x in trade.legs if x.underlying_label == "SENSEX"), None)
    if nifty_leg is None or sensex_leg is None:
        return

    nq = quotes.get(nifty_leg.option.quote_key, {})
    sq = quotes.get(sensex_leg.option.quote_key, {})
    nifty_exit_ltp = safe_float(nq.get("last_price"), nifty_leg.entry_ltp)
    sensex_exit_ltp = safe_float(sq.get("last_price"), sensex_leg.entry_ltp)
    nifty_exit_bid, nifty_exit_ask = best_bid_ask(nq)
    sensex_exit_bid, sensex_exit_ask = best_bid_ask(sq)

    logs.virtual_trades.write({
        "trade_id": trade.trade_id,
        "side": trade.side,
        "signal_time": trade.signal_time.isoformat(sep=" "),
        "entry_time": trade.entry_time.isoformat(sep=" "),
        "exit_time": now.isoformat(sep=" "),
        "exit_reason": reason,
        "entry_deviation_pct": trade.entry_deviation_pct,
        "entry_abs_deviation_pct": trade.entry_abs_deviation_pct,
        "exit_deviation_pct": deviation_pct,
        "exit_abs_deviation_pct": abs_deviation_pct,
        "entry_ratio": trade.entry_ratio,
        "exit_ratio": current_ratio,
        "baseline_ratio": state.baseline_ratio,
        "baseline_date": state.baseline_date,
        "entry_nifty": trade.entry_nifty,
        "entry_sensex": trade.entry_sensex,
        "exit_nifty": nifty_ltp,
        "exit_sensex": sensex_ltp,
        "nifty_option": nifty_leg.option.quote_key,
        "nifty_qty": nifty_leg.qty,
        "nifty_entry_ltp": nifty_leg.entry_ltp,
        "nifty_entry_bid": nifty_leg.entry_bid,
        "nifty_entry_ask": nifty_leg.entry_ask,
        "nifty_exit_ltp": nifty_exit_ltp,
        "nifty_exit_bid": nifty_exit_bid,
        "nifty_exit_ask": nifty_exit_ask,
        "sensex_option": sensex_leg.option.quote_key,
        "sensex_qty": sensex_leg.qty,
        "sensex_entry_ltp": sensex_leg.entry_ltp,
        "sensex_entry_bid": sensex_leg.entry_bid,
        "sensex_entry_ask": sensex_leg.entry_ask,
        "sensex_exit_ltp": sensex_exit_ltp,
        "sensex_exit_bid": sensex_exit_bid,
        "sensex_exit_ask": sensex_exit_ask,
        "ltp_exit_pnl": ltp_pnl,
        "realistic_exit_pnl": realistic_pnl,
        "max_ltp_pnl": trade.max_ltp_pnl,
        "min_ltp_pnl": trade.min_ltp_pnl,
        "max_realistic_pnl": trade.max_realistic_pnl,
        "min_realistic_pnl": trade.min_realistic_pnl,
        "hold_seconds": int((now - trade.entry_time).total_seconds()),
    })

    logs.trade_events.write({
        "timestamp": now.isoformat(sep=" "),
        "event_type": "VIRTUAL_EXIT",
        "trade_id": trade.trade_id,
        "side": trade.side,
        "deviation_pct": deviation_pct,
        "abs_deviation_pct": abs_deviation_pct,
        "current_ratio": current_ratio,
        "baseline_ratio": state.baseline_ratio,
        "nifty_ltp": nifty_ltp,
        "sensex_ltp": sensex_ltp,
        "nifty_option": nifty_leg.option.quote_key,
        "sensex_option": sensex_leg.option.quote_key,
        "ltp_pnl": ltp_pnl,
        "realistic_pnl": realistic_pnl,
        "message": reason,
    })

    state.active_trade = None


# =============================================================================
# MAIN LOOP
# =============================================================================

def main() -> None:
    """Run live quote-polling paper trader for previous-day ratio strategy."""
    print("============================================================")
    print("NIFTY-SENSEX live previous-day ratio option paper trader")
    print("============================================================")

    if not NIFTY_EXPIRY_DATE_ENV or not SENSEX_EXPIRY_DATE_ENV:
        raise RuntimeError("Set NIFTY_EXPIRY_DATE and SENSEX_EXPIRY_DATE before running.")

    nifty_expiry = parse_date(NIFTY_EXPIRY_DATE_ENV)
    sensex_expiry = parse_date(SENSEX_EXPIRY_DATE_ENV)

    run_dir, logs = setup_loggers()
    print(f"[INFO] Logs: {run_dir}")

    print("[STEP] Initializing Kite API ...")
    kite = oUtils.intialize_kite_api()
    print("[INFO] Kite API initialized.")

    instruments_cache: Dict[str, List[Dict]] = {}

    print("[STEP] Loading option instruments for lookup ...")
    nfo = kite_instruments_cached(kite, NIFTY_OPTION_EXCHANGE, instruments_cache)
    bfo = kite_instruments_cached(kite, SENSEX_OPTION_EXCHANGE, instruments_cache)

    print("[STEP] Computing previous-day average SENSEX/NIFTY ratio baseline ...")
    baseline_ratio, baseline_date, baseline_rows = seed_prev_day_ratio_baseline(kite, instruments_cache)
    state = LiveState(baseline_ratio=baseline_ratio, baseline_date=baseline_date, baseline_rows=baseline_rows)
    write_config(logs, state)

    def handle_signal(signum, frame):  # noqa: ANN001
        print("\n[INFO] Stop requested. Exiting after current loop...")
        state.stop_requested = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    print("[INFO] Starting quote polling loop. Press Ctrl+C to stop.")

    try:
        while not state.stop_requested:
            loop_start = time.time()
            now = ist_now()

            if now.time() > SESSION_END and state.active_trade is None:
                print("[INFO] Market session ended and no active virtual trade remains.")
                break

            keys = build_quote_keys(state)
            quotes = get_quote_batch(kite, keys, logs)
            missing_keys = [k for k in keys if k not in quotes]

            nq = quotes.get(NIFTY_INDEX_KEY, {})
            sq = quotes.get(SENSEX_INDEX_KEY, {})
            nifty_ltp = safe_float(nq.get("last_price"))
            sensex_ltp = safe_float(sq.get("last_price"))

            message = ""
            if not math.isfinite(nifty_ltp) or not math.isfinite(sensex_ltp) or nifty_ltp <= 0 or sensex_ltp <= 0:
                message = "Missing or invalid index quote; skipping ratio calculation."
                logs.market_state.write({
                    "timestamp": now.isoformat(sep=" "),
                    "state": "ACTIVE" if state.active_trade else "IDLE",
                    "rearmed": state.rearmed,
                    "baseline_ratio": state.baseline_ratio,
                    "baseline_date": state.baseline_date,
                    "quote_keys_count": len(keys),
                    "missing_quote_keys": ";".join(missing_keys),
                    "message": message,
                })
                time.sleep(max(0.0, POLL_INTERVAL_SEC - (time.time() - loop_start)))
                continue

            current_ratio, deviation_pct, abs_deviation_pct = compute_ratio_deviation(
                nifty_ltp=nifty_ltp,
                sensex_ltp=sensex_ltp,
                baseline_ratio=state.baseline_ratio,
            )

            if (not state.rearmed) and state.active_trade is None and abs_deviation_pct <= REARM_DEVIATION_PCT:
                state.rearmed = True
                logs.trade_events.write({
                    "timestamp": now.isoformat(sep=" "),
                    "event_type": "REARMED",
                    "deviation_pct": deviation_pct,
                    "abs_deviation_pct": abs_deviation_pct,
                    "current_ratio": current_ratio,
                    "baseline_ratio": state.baseline_ratio,
                    "nifty_ltp": nifty_ltp,
                    "sensex_ltp": sensex_ltp,
                    "message": f"System rearmed because abs_deviation_pct <= {REARM_DEVIATION_PCT}.",
                })

            # Default market-state fields.
            active_trade_id = ""
            active_side = ""
            active_entry_time = ""
            active_entry_dev = ""
            nifty_option_key = ""
            sensex_option_key = ""
            nifty_option_ltp = np.nan
            sensex_option_ltp = np.nan
            nifty_option_bid = np.nan
            nifty_option_ask = np.nan
            sensex_option_bid = np.nan
            sensex_option_ask = np.nan
            ltp_pnl = np.nan
            realistic_pnl = np.nan
            max_ltp_pnl = np.nan
            min_ltp_pnl = np.nan
            max_realistic_pnl = np.nan
            min_realistic_pnl = np.nan

            # Update active trade PnL and evaluate exits.
            if state.active_trade is not None:
                trade = state.active_trade
                active_trade_id = trade.trade_id
                active_side = trade.side
                active_entry_time = trade.entry_time.isoformat(sep=" ")
                active_entry_dev = trade.entry_deviation_pct

                ltp_pnl, realistic_pnl, snaps = compute_option_pnls(trade, quotes)
                trade.last_ltp_pnl = ltp_pnl
                trade.last_realistic_pnl = realistic_pnl
                trade.last_deviation_pct = deviation_pct
                trade.last_abs_deviation_pct = abs_deviation_pct
                trade.max_ltp_pnl = max(trade.max_ltp_pnl, ltp_pnl)
                trade.min_ltp_pnl = min(trade.min_ltp_pnl, ltp_pnl)
                trade.max_realistic_pnl = max(trade.max_realistic_pnl, realistic_pnl)
                trade.min_realistic_pnl = min(trade.min_realistic_pnl, realistic_pnl)

                max_ltp_pnl = trade.max_ltp_pnl
                min_ltp_pnl = trade.min_ltp_pnl
                max_realistic_pnl = trade.max_realistic_pnl
                min_realistic_pnl = trade.min_realistic_pnl

                for leg in trade.legs:
                    snap = snaps.get(leg.underlying_label, {})
                    if leg.underlying_label == "NIFTY":
                        nifty_option_key = leg.option.quote_key
                        nifty_option_ltp = snap.get("ltp", np.nan)
                        nifty_option_bid = snap.get("bid", np.nan)
                        nifty_option_ask = snap.get("ask", np.nan)
                    elif leg.underlying_label == "SENSEX":
                        sensex_option_key = leg.option.quote_key
                        sensex_option_ltp = snap.get("ltp", np.nan)
                        sensex_option_bid = snap.get("bid", np.nan)
                        sensex_option_ask = snap.get("ask", np.nan)

                exit_reason = ""
                hold_seconds = int((now - trade.entry_time).total_seconds())
                pnl_for_stop = realistic_pnl if USE_BID_ASK_REALISTIC_PNL else ltp_pnl

                if abs_deviation_pct <= SETTLE_DEVIATION_PCT:
                    exit_reason = "RATIO_DEVIATION_SETTLED"
                elif MAX_HOLD_SECONDS > 0 and hold_seconds >= MAX_HOLD_SECONDS:
                    exit_reason = "MAX_HOLD_SECONDS"
                elif STOP_LOSS_RUPEES > 0 and pnl_for_stop <= -abs(STOP_LOSS_RUPEES):
                    exit_reason = "STOP_LOSS_RUPEES"
                elif TARGET_PROFIT_RUPEES > 0 and pnl_for_stop >= TARGET_PROFIT_RUPEES:
                    exit_reason = "TARGET_PROFIT_RUPEES"
                elif should_force_exit_by_time(now):
                    exit_reason = "FORCE_EXIT_TIME"

                if exit_reason:
                    exit_virtual_trade(
                        state=state,
                        logs=logs,
                        now=now,
                        reason=exit_reason,
                        current_ratio=current_ratio,
                        deviation_pct=deviation_pct,
                        abs_deviation_pct=abs_deviation_pct,
                        nifty_ltp=nifty_ltp,
                        sensex_ltp=sensex_ltp,
                        quotes=quotes,
                    )

            # Entry logic: only one virtual trade at a time.
            if state.active_trade is None and state.rearmed:
                if abs_deviation_pct >= ENTRY_DEVIATION_PCT and in_entry_time_window(now):
                    logs.trade_events.write({
                        "timestamp": now.isoformat(sep=" "),
                        "event_type": "RATIO_SIGNAL",
                        "deviation_pct": deviation_pct,
                        "abs_deviation_pct": abs_deviation_pct,
                        "current_ratio": current_ratio,
                        "baseline_ratio": state.baseline_ratio,
                        "nifty_ltp": nifty_ltp,
                        "sensex_ltp": sensex_ltp,
                        "message": f"abs_deviation_pct >= ENTRY_DEVIATION_PCT ({ENTRY_DEVIATION_PCT}); attempting virtual option entry.",
                    })
                    start_virtual_trade(
                        kite=kite,
                        state=state,
                        logs=logs,
                        now=now,
                        deviation_pct=deviation_pct,
                        abs_deviation_pct=abs_deviation_pct,
                        current_ratio=current_ratio,
                        nifty_ltp=nifty_ltp,
                        sensex_ltp=sensex_ltp,
                        nfo=nfo,
                        bfo=bfo,
                        nifty_expiry=nifty_expiry,
                        sensex_expiry=sensex_expiry,
                    )
                elif abs_deviation_pct >= ENTRY_DEVIATION_PCT and not in_entry_time_window(now):
                    message = f"Signal ignored due to entry time filter: {ENTRY_START_TIME}-{LAST_ENTRY_TIME}"

            logs.market_state.write({
                "timestamp": now.isoformat(sep=" "),
                "state": "ACTIVE" if state.active_trade else "IDLE",
                "rearmed": state.rearmed,
                "nifty_ltp": nifty_ltp,
                "sensex_ltp": sensex_ltp,
                "current_ratio": current_ratio,
                "baseline_ratio": state.baseline_ratio,
                "baseline_date": state.baseline_date,
                "deviation_pct": deviation_pct,
                "abs_deviation_pct": abs_deviation_pct,
                "active_trade_id": active_trade_id,
                "active_side": active_side,
                "active_entry_time": active_entry_time,
                "active_entry_deviation_pct": active_entry_dev,
                "nifty_option": nifty_option_key,
                "sensex_option": sensex_option_key,
                "nifty_option_ltp": nifty_option_ltp,
                "sensex_option_ltp": sensex_option_ltp,
                "nifty_option_bid": nifty_option_bid,
                "nifty_option_ask": nifty_option_ask,
                "sensex_option_bid": sensex_option_bid,
                "sensex_option_ask": sensex_option_ask,
                "ltp_pnl": ltp_pnl,
                "realistic_pnl": realistic_pnl,
                "max_ltp_pnl": max_ltp_pnl,
                "min_ltp_pnl": min_ltp_pnl,
                "max_realistic_pnl": max_realistic_pnl,
                "min_realistic_pnl": min_realistic_pnl,
                "quote_keys_count": len(keys),
                "missing_quote_keys": ";".join(missing_keys),
                "message": message,
            })

            time.sleep(max(0.0, POLL_INTERVAL_SEC - (time.time() - loop_start)))

    finally:
        if state.active_trade is not None:
            logs.trade_events.write({
                "timestamp": ist_now().isoformat(sep=" "),
                "event_type": "SCRIPT_STOPPED_WITH_ACTIVE_TRADE",
                "trade_id": state.active_trade.trade_id,
                "side": state.active_trade.side,
                "deviation_pct": state.active_trade.last_deviation_pct,
                "abs_deviation_pct": state.active_trade.last_abs_deviation_pct,
                "ltp_pnl": state.active_trade.last_ltp_pnl,
                "realistic_pnl": state.active_trade.last_realistic_pnl,
                "message": "Script stopped before virtual trade exit.",
            })
        logs.close()
        print("[DONE] Logs closed.")


if __name__ == "__main__":
    main()
