#!/usr/bin/env python3
"""
HdfcIciciPrevDayRatioBacktester.py
===================================

Purpose
-------
Backtest the same **previous-day ratio deviation** idea, but for a stock pair:

    NUMERATOR   = ICICIBANK   (default)
    DENOMINATOR = HDFCBANK    (default)

The default ratio is therefore:

    ratio = ICICIBANK_close / HDFCBANK_close

Normal baseline:

    prev_day_avg_ratio = average(ratio) on the previous completed trading day

Live / bar-level deviation:

    deviation_pct = ((current_ratio / active_baseline_ratio) - 1) * 100

Trade hypothesis
----------------
If deviation_pct is positive and large:
    The numerator stock (ICICIBANK by default) is rich versus the denominator
    stock (HDFCBANK by default).

    Virtual pair trade:
        SHORT NUMERATOR
        LONG  DENOMINATOR

If deviation_pct is negative and large:
    The numerator stock is cheap versus the denominator stock.

    Virtual pair trade:
        LONG  NUMERATOR
        SHORT DENOMINATOR

This script is adapted from the user's NIFTY-SENSEX previous-day ratio
backtester, but it is made generic for two NSE equity instruments and can
DOWNLOAD the historical 1-minute data from Kite if cached files are not found.

Important design choices
------------------------
1. The script can download 1-minute historical data for both symbols using Kite.
2. Downloaded files are cached with the symbol and date range in the filename,
   avoiding the old cache bug where old ranges could silently be reused.
3. The default quantity mode is NOTIONAL_BALANCED, because HDFCBANK and
   ICICIBANK have different prices. Equal number of shares is usually not a
   good hedge. The script sizes both legs to approximately equal rupee notional.
4. By default, trades do NOT carry overnight.
5. By default, trades are time-stopped after 100 1-minute bars.
6. By default, trades are stopped if interim PnL reaches -Rs 5,000.
7. After forced exits, the baseline can be recalibrated from the last 375 bars,
   similar to the revised NIFTY-SENSEX script.

Caution
-------
This is a backtest / event-study using spot equity close values. If you later
trade with stock futures, replace quantities, lot sizes, costs, slippage, and
execution assumptions accordingly. Stock-pair trading is not arbitrage; it is
statistical mean-reversion and can break during stock-specific news.

Dependencies
------------
pip install pandas numpy openpyxl python-dateutil

Your existing project dependency is required only if download is needed:
    Trading_2024.OptionTradeUtils

Typical Windows CMD run
-----------------------
    set NUMERATOR_SYMBOL=ICICIBANK
    set DENOMINATOR_SYMBOL=HDFCBANK
    set LOOKBACK_YEARS=4
    set THRESHOLDS_PCT=0.20,0.30,0.50,0.75,1.00
    set SETTLE_DEVIATION_PCT=0.05
    set HARD_TIME_STOP_BARS=100
    set MAX_LOSS_RUPEES=5000
    set QTY_MODE=NOTIONAL_BALANCED
    set BASE_NOTIONAL_RUPEES=1000000
    python HdfcIciciPrevDayRatioBacktester.py

If you want to force a fresh download:
    set FORCE_DOWNLOAD=1
    python HdfcIciciPrevDayRatioBacktester.py

Output
------
Default output folder:
    ./hdfc_icici_prevday_ratio_output

For each threshold, the script writes:
    - Excel report
    - events CSV
    - recalibration CSV

The Excel report includes:
    - summary
    - events
    - exit_reason_summary
    - baseline_recalibrations
    - daily_counts
    - by_side
    - holding_buckets
    - config
"""

from __future__ import annotations

import math
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

try:
    from dateutil.relativedelta import relativedelta
except Exception:  # pragma: no cover
    relativedelta = None  # type: ignore

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


# =============================================================================
# CONFIGURATION
# =============================================================================

# Indian regular market session.
SESSION_START = dtime(9, 15, 0)
SESSION_END = dtime(15, 30, 0)

# Default pair: ratio = ICICIBANK / HDFCBANK.
EXCHANGE = os.environ.get("EXCHANGE", "NSE").strip().upper()
NUMERATOR_SYMBOL = os.environ.get("NUMERATOR_SYMBOL", "ICICIBANK").strip().upper()
DENOMINATOR_SYMBOL = os.environ.get("DENOMINATOR_SYMBOL", "HDFCBANK").strip().upper()
PAIR_NAME = os.environ.get("PAIR_NAME", f"{NUMERATOR_SYMBOL}_{DENOMINATOR_SYMBOL}").strip()

OUTPUT_DIR = os.environ.get("OUTPUT_DIR", f"./{PAIR_NAME.lower()}_prevday_ratio_output")

# Historical download / cache settings.
LOOKBACK_YEARS = int(os.environ.get("LOOKBACK_YEARS", "4"))
START_DATE_ENV = os.environ.get("START_DATE", "").strip()
END_DATE_ENV = os.environ.get("END_DATE", "").strip()
FORCE_DOWNLOAD = os.environ.get("FORCE_DOWNLOAD", "0").strip().lower() in {"1", "true", "yes", "y"}
MAX_DAYS_PER_CHUNK = int(os.environ.get("MAX_DAYS_PER_CHUNK", "25"))
MAX_ATTEMPTS = int(os.environ.get("MAX_ATTEMPTS", "5"))
SLEEP_BETWEEN_CALLS_SEC = float(os.environ.get("SLEEP_BETWEEN_CALLS_SEC", "0.20"))

# Optional explicit data paths. If aligned path is given, download is skipped.
ALIGNED_PATH_ENV = os.environ.get("ALIGNED_PATH", "").strip()
NUMERATOR_CANDLES_PATH_ENV = os.environ.get("NUMERATOR_CANDLES_PATH", "").strip()
DENOMINATOR_CANDLES_PATH_ENV = os.environ.get("DENOMINATOR_CANDLES_PATH", "").strip()

# Previous-day baseline mode.
PREV_DAY_BASELINE_MODE = os.environ.get("PREV_DAY_BASELINE_MODE", "MEAN").strip().upper()

# Stock-pair ratio deviations are normally larger than NIFTY-SENSEX index-ratio
# deviations. Therefore defaults are wider than 0.03%.
THRESHOLDS_PCT = [
    float(x.strip())
    for x in os.environ.get("THRESHOLDS_PCT", "0.20,0.30,0.50,0.75,1.00").split(",")
    if x.strip()
]

# Settlement near the active baseline. Example: 0.05 means +/-0.05%.
SETTLE_DEVIATION_PCT = float(os.environ.get("SETTLE_DEVIATION_PCT", "0.05"))

# Diagnostic cap; hard stop usually exits earlier.
MAX_WAIT_TRADING_DAYS = int(os.environ.get("MAX_WAIT_TRADING_DAYS", "10"))
INTRADAY_BARS_PER_DAY = int(os.environ.get("INTRADAY_BARS_PER_DAY", "375"))
MAX_LOOKAHEAD_BARS = int(os.environ.get("MAX_LOOKAHEAD_BARS", str(MAX_WAIT_TRADING_DAYS * INTRADAY_BARS_PER_DAY)))

# Trading-style controls.
HARD_TIME_STOP_BARS = int(os.environ.get("HARD_TIME_STOP_BARS", "100"))
MAX_LOSS_RUPEES = float(os.environ.get("MAX_LOSS_RUPEES", "5000"))
NO_OVERNIGHT = os.environ.get("NO_OVERNIGHT", "1").strip().lower() in {"1", "true", "yes", "y"}
FORCE_EXIT_TIME = dtime.fromisoformat(os.environ.get("FORCE_EXIT_TIME", "15:20"))

# Intraday baseline recalibration after forced exits.
ENABLE_REBASE_AFTER_FORCED_EXIT = os.environ.get("ENABLE_REBASE_AFTER_FORCED_EXIT", "1").strip().lower() in {
    "1", "true", "yes", "y"
}
REBASE_LOOKBACK_BARS = int(os.environ.get("REBASE_LOOKBACK_BARS", "375"))
REBASE_MIN_BARS = int(os.environ.get("REBASE_MIN_BARS", "50"))
REBASE_AFTER_EXIT_REASONS = {
    x.strip().upper()
    for x in os.environ.get("REBASE_AFTER_EXIT_REASONS", "MAX_LOSS_STOP,HARD_TIME_STOP,NO_OVERNIGHT_EXIT").split(",")
    if x.strip()
}

# Quantity handling.
# FIXED:
#     Use NUMERATOR_QTY_FIXED and DENOMINATOR_QTY_FIXED for every event.
# NOTIONAL_BALANCED:
#     At each entry, size each leg to approximately BASE_NOTIONAL_RUPEES.
#     This is usually more sensible for stock pairs.
QTY_MODE = os.environ.get("QTY_MODE", "NOTIONAL_BALANCED").strip().upper()
BASE_NOTIONAL_RUPEES = float(os.environ.get("BASE_NOTIONAL_RUPEES", "1000000"))
NUMERATOR_QTY_FIXED = int(os.environ.get("NUMERATOR_QTY_FIXED", "1"))
DENOMINATOR_QTY_FIXED = int(os.environ.get("DENOMINATOR_QTY_FIXED", "1"))

# If you test futures, set these to current lot sizes manually.
# Defaults are 1 because this script uses cash-stock proxy by default.
NUMERATOR_LOT_SIZE = int(os.environ.get("NUMERATOR_LOT_SIZE", "1"))
DENOMINATOR_LOT_SIZE = int(os.environ.get("DENOMINATOR_LOT_SIZE", "1"))
ROUND_QTY_TO_LOTS = os.environ.get("ROUND_QTY_TO_LOTS", "1").strip().lower() in {"1", "true", "yes", "y"}

COST_PER_TRADE_RUPEES = float(os.environ.get("COST_PER_TRADE_RUPEES", "0"))

# Non-overlap means one unresolved deviation episode is counted once.
SKIP_OVERLAPPING_EVENTS = os.environ.get("SKIP_OVERLAPPING_EVENTS", "1").strip().lower() in {
    "1", "true", "yes", "y"
}

# Entry time filter.
ENABLE_ENTRY_TIME_FILTER = os.environ.get("ENABLE_ENTRY_TIME_FILTER", "1").strip().lower() in {
    "1", "true", "yes", "y"
}
ENTRY_START_TIME = dtime.fromisoformat(os.environ.get("ENTRY_START_TIME", "09:30"))
LAST_ENTRY_TIME = dtime.fromisoformat(os.environ.get("LAST_ENTRY_TIME", "14:30"))


# =============================================================================
# DATA STRUCTURES
# =============================================================================

@dataclass(frozen=True)
class DataSourceInfo:
    """Records where the input data came from for auditability."""

    mode: str
    path_1: str
    path_2: str = ""


@dataclass
class BaselineState:
    """
    Holds the active ratio baseline during event scanning.

    Normally this is the previous day's average ratio. After forced exits, it
    can be recalibrated from recent intraday bars and remains valid only for the
    same trading date.
    """

    source: str = "PREV_DAY"
    value: float = np.nan
    set_time: Optional[pd.Timestamp] = None
    set_index: Optional[int] = None
    valid_trading_date: Optional[date] = None
    lookback_bars_used: int = 0


@dataclass(frozen=True)
class EntryQuantities:
    """Quantity and notional information fixed at event entry."""

    numerator_qty: int
    denominator_qty: int
    numerator_notional: float
    denominator_notional: float
    qty_mode: str


# =============================================================================
# DATE / FILE HELPERS
# =============================================================================

def ensure_output_dir() -> None:
    """Create output directories."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(os.path.join(OUTPUT_DIR, "candles"), exist_ok=True)


def ist_today() -> date:
    """Return today's date in India."""
    if ZoneInfo is not None:
        try:
            return datetime.now(ZoneInfo("Asia/Kolkata")).date()
        except Exception:
            pass
    return datetime.now().date()


def parse_date(raw: str) -> date:
    """Parse YYYY-MM-DD or DD-MM-YYYY."""
    raw = raw.strip()
    for fmt in ("%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Could not parse date: {raw!r}")


def get_date_range() -> Tuple[date, date]:
    """Resolve start/end dates for download/cache."""
    end_d = parse_date(END_DATE_ENV) if END_DATE_ENV else ist_today()
    if START_DATE_ENV:
        start_d = parse_date(START_DATE_ENV)
    else:
        if relativedelta is not None:
            start_d = end_d - relativedelta(years=LOOKBACK_YEARS)
        else:
            start_d = end_d - timedelta(days=365 * LOOKBACK_YEARS)

    if start_d >= end_d:
        raise ValueError(f"START_DATE must be earlier than END_DATE. Got {start_d} >= {end_d}")
    return start_d, end_d


def normalize_dt(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize date column to timezone-naive pandas datetime sorted by date."""
    out = df.copy()
    if "date" not in out.columns:
        raise ValueError("Input data must contain a 'date' column.")
    out["date"] = pd.to_datetime(out["date"], errors="coerce")

    # Convert timezone-aware values to IST and then remove timezone metadata.
    try:
        if out["date"].dt.tz is not None:
            out["date"] = out["date"].dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    except Exception:
        out["date"] = pd.to_datetime(out["date"].astype(str), errors="coerce")

    out["date"] = out["date"].dt.floor("min")
    out = out.dropna(subset=["date"]).sort_values("date").drop_duplicates(subset=["date"], keep="last")
    return out.reset_index(drop=True)


def load_pickle_or_csv(path: str) -> pd.DataFrame:
    """Load a DataFrame from pickle or CSV."""
    if not path:
        raise ValueError("Empty path supplied.")
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    lower = path.lower()
    if lower.endswith((".pkl", ".pickle")):
        return pd.read_pickle(path)
    if lower.endswith(".csv"):
        return pd.read_csv(path)
    raise ValueError(f"Unsupported data file extension: {path}")


def sanitize_filename_part(x: str) -> str:
    """Make a safe filename component."""
    return "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in x)


# =============================================================================
# KITE DOWNLOAD HELPERS
# =============================================================================

def initialize_kite_if_needed():
    """Initialize Kite only when a download is actually needed."""
    import Trading_2024.OptionTradeUtils as oUtils  # local project dependency

    return oUtils.intialize_kite_api()


def kite_instruments_cached(kite, exchange: str, cache: Dict[str, List[Dict]]) -> List[Dict]:
    """Load and cache Kite instruments dump for an exchange."""
    ex = exchange.upper().strip()
    if ex not in cache:
        print(f"[STEP] Loading instruments dump for {ex} ...")
        cache[ex] = kite.instruments(ex)
        print(f"[INFO] {ex} instruments loaded: {len(cache[ex])}")
    return cache[ex]


def get_instrument_token(kite, exchange: str, tradingsymbol: str, cache: Dict[str, List[Dict]]) -> int:
    """Resolve instrument token by exchange and tradingsymbol."""
    rows = kite_instruments_cached(kite, exchange, cache)
    wanted = tradingsymbol.upper().strip()
    for r in rows:
        if str(r.get("tradingsymbol", "")).upper().strip() == wanted:
            return int(r["instrument_token"])
    raise ValueError(f"Instrument not found: {exchange}:{tradingsymbol}")


def iter_chunks_by_date(from_dt: datetime, to_dt: datetime, days_per_chunk: int) -> List[Tuple[datetime, datetime]]:
    """Split datetime range into chunks while preserving session times."""
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


def fetch_history_1min(kite, token: int, from_dt: datetime, to_dt: datetime, label: str) -> List[Dict]:
    """Fetch 1-minute historical rows with chunking and retry logic."""
    chunks = iter_chunks_by_date(from_dt, to_dt, MAX_DAYS_PER_CHUNK)
    rows_all: List[Dict] = []
    print(f"[INFO] Fetching {label}, token={token}, chunks={len(chunks)}")

    for idx, (c_from, c_to) in enumerate(chunks, start=1):
        print(f"  [CHUNK {idx:03d}/{len(chunks):03d}] {c_from} -> {c_to}")
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
                print(f"    [OK] {len(rows)} candles")
                last_err = None
                break
            except Exception as e:  # noqa: BLE001
                last_err = e
                wait = min(10.0, 1.5 * attempt)
                print(f"    [WARN] attempt {attempt}/{MAX_ATTEMPTS} failed: {e}; sleeping {wait:.1f}s")
                time.sleep(wait)
        if last_err is not None:
            print(f"    [ERROR] Giving up on chunk {idx}: {last_err}")
        time.sleep(SLEEP_BETWEEN_CALLS_SEC)

    return rows_all


def rows_to_candle_df(rows: List[Dict], label: str) -> pd.DataFrame:
    """Convert Kite rows into clean OHLCV DataFrame."""
    if not rows:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume", "instrument"])

    df = pd.DataFrame(rows)
    for col in ["date", "open", "high", "low", "close", "volume"]:
        if col not in df.columns:
            df[col] = np.nan

    df = normalize_dt(df)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["close"])
    df = df[df["close"] > 0].copy()

    # Keep regular session and weekdays.
    t = df["date"].dt.time
    df = df[(t >= SESSION_START) & (t <= SESSION_END)].copy()
    df = df[df["date"].dt.weekday < 5].copy()
    df["instrument"] = label
    return df.sort_values("date").reset_index(drop=True)


def candle_cache_path(symbol: str, start_d: date, end_d: date) -> str:
    """Cache filename includes symbol and date range, preventing stale reuse."""
    safe = sanitize_filename_part(f"{EXCHANGE}_{symbol}")
    return os.path.join(OUTPUT_DIR, "candles", f"{safe}_1min_{start_d}_{end_d}.pkl")


def load_or_download_symbol(symbol: str, start_d: date, end_d: date) -> pd.DataFrame:
    """Load cached symbol candles or download from Kite if missing."""
    path = candle_cache_path(symbol, start_d, end_d)

    if os.path.exists(path) and not FORCE_DOWNLOAD:
        print(f"[CACHE] Loading {symbol}: {path}")
        df = pd.read_pickle(path)
        return normalize_dt(df)

    print(f"[STEP] Download required for {EXCHANGE}:{symbol}")
    kite = initialize_kite_if_needed()
    cache: Dict[str, List[Dict]] = {}
    token = get_instrument_token(kite, EXCHANGE, symbol, cache)
    from_dt = datetime.combine(start_d, SESSION_START)
    to_dt = datetime.combine(end_d, SESSION_END)
    rows = fetch_history_1min(kite, token, from_dt, to_dt, f"{EXCHANGE}:{symbol}")
    df = rows_to_candle_df(rows, label=symbol)
    if df.empty:
        raise RuntimeError(f"No data downloaded for {EXCHANGE}:{symbol}")

    df.to_pickle(path)
    df.to_csv(path.replace(".pkl", ".csv"), index=False)
    print(f"[DONE] Saved {symbol}: {path}; rows={len(df):,}")
    return df


# =============================================================================
# DATA LOADING / ALIGNMENT
# =============================================================================

def is_aligned_df(df: pd.DataFrame) -> bool:
    """Return True if DataFrame already has generic aligned columns."""
    cols = {c.lower() for c in df.columns}
    return {"date", "denominator_close", "numerator_close"}.issubset(cols)


def standardize_aligned_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize aligned columns and add trading_date."""
    out = df.copy()
    lower_map = {c.lower(): c for c in out.columns}
    out = out.rename(
        columns={
            lower_map["date"]: "date",
            lower_map["denominator_close"]: "denominator_close",
            lower_map["numerator_close"]: "numerator_close",
        }
    )
    out = normalize_dt(out)
    out["denominator_close"] = pd.to_numeric(out["denominator_close"], errors="coerce")
    out["numerator_close"] = pd.to_numeric(out["numerator_close"], errors="coerce")
    out = out.dropna(subset=["denominator_close", "numerator_close"])
    out = out[(out["denominator_close"] > 0) & (out["numerator_close"] > 0)].copy()

    t = out["date"].dt.time
    out = out[(t >= SESSION_START) & (t <= SESSION_END)].copy()
    out["trading_date"] = out["date"].dt.date
    return out.sort_values("date").reset_index(drop=True)


def align_from_candles(denominator_df: pd.DataFrame, numerator_df: pd.DataFrame) -> pd.DataFrame:
    """Align denominator and numerator close prices by common timestamp."""
    den = normalize_dt(denominator_df)[["date", "close"]].rename(columns={"close": "denominator_close"})
    num = normalize_dt(numerator_df)[["date", "close"]].rename(columns={"close": "numerator_close"})
    aligned = pd.merge(den, num, on="date", how="inner")
    return standardize_aligned_columns(aligned)


def find_or_download_data() -> Tuple[pd.DataFrame, DataSourceInfo]:
    """Find aligned/candle files, otherwise download both stock histories."""
    ensure_output_dir()
    start_d, end_d = get_date_range()

    # 1) Explicit aligned path.
    if ALIGNED_PATH_ENV:
        df = load_pickle_or_csv(ALIGNED_PATH_ENV)
        if not is_aligned_df(df):
            raise ValueError(f"ALIGNED_PATH must contain date, denominator_close, numerator_close: {ALIGNED_PATH_ENV}")
        return standardize_aligned_columns(df), DataSourceInfo("explicit_aligned", ALIGNED_PATH_ENV)

    aligned_path = os.path.join(
        OUTPUT_DIR,
        f"aligned_{sanitize_filename_part(DENOMINATOR_SYMBOL)}_{sanitize_filename_part(NUMERATOR_SYMBOL)}_1min_{start_d}_{end_d}.pkl",
    )
    if os.path.exists(aligned_path) and not FORCE_DOWNLOAD:
        print(f"[CACHE] Loading aligned data: {aligned_path}")
        return standardize_aligned_columns(pd.read_pickle(aligned_path)), DataSourceInfo("auto_aligned_cache", aligned_path)

    # 2) Explicit candle paths.
    if DENOMINATOR_CANDLES_PATH_ENV and NUMERATOR_CANDLES_PATH_ENV:
        den = load_pickle_or_csv(DENOMINATOR_CANDLES_PATH_ENV)
        num = load_pickle_or_csv(NUMERATOR_CANDLES_PATH_ENV)
        aligned = align_from_candles(den, num)
        aligned.to_pickle(aligned_path)
        aligned.to_csv(aligned_path.replace(".pkl", ".csv"), index=False)
        return aligned, DataSourceInfo("explicit_candles", DENOMINATOR_CANDLES_PATH_ENV, NUMERATOR_CANDLES_PATH_ENV)

    # 3) Cache/download symbol candles.
    den_df = load_or_download_symbol(DENOMINATOR_SYMBOL, start_d, end_d)
    num_df = load_or_download_symbol(NUMERATOR_SYMBOL, start_d, end_d)
    aligned = align_from_candles(den_df, num_df)
    if aligned.empty:
        raise RuntimeError("No common timestamps after aligning stock candles.")

    aligned.to_pickle(aligned_path)
    aligned.to_csv(aligned_path.replace(".pkl", ".csv"), index=False)
    return aligned, DataSourceInfo("downloaded_or_cached_candles", candle_cache_path(DENOMINATOR_SYMBOL, start_d, end_d), candle_cache_path(NUMERATOR_SYMBOL, start_d, end_d))


# =============================================================================
# RATIO / BASELINE CALCULATION
# =============================================================================

def add_prev_day_ratio_baseline(df: pd.DataFrame) -> pd.DataFrame:
    """Add ratio, raw difference, previous-day baseline, and prev-day deviation."""
    out = df.copy().sort_values("date").reset_index(drop=True)
    out["ratio"] = out["numerator_close"] / out["denominator_close"]
    out["raw_difference"] = out["numerator_close"] - out["denominator_close"]

    if PREV_DAY_BASELINE_MODE == "MEAN":
        day_ratio = out.groupby("trading_date")["ratio"].mean()
    elif PREV_DAY_BASELINE_MODE == "MEDIAN":
        day_ratio = out.groupby("trading_date")["ratio"].median()
    else:
        raise ValueError("PREV_DAY_BASELINE_MODE must be MEAN or MEDIAN")

    trading_dates = sorted(day_ratio.index)
    prev_map: Dict[date, float] = {}
    for idx, d in enumerate(trading_dates):
        prev_map[d] = np.nan if idx == 0 else float(day_ratio.loc[trading_dates[idx - 1]])

    out["prev_day_avg_ratio"] = out["trading_date"].map(prev_map)
    out["prev_day_deviation_pct"] = ((out["ratio"] / out["prev_day_avg_ratio"]) - 1.0) * 100.0
    out["prev_day_abs_deviation_pct"] = out["prev_day_deviation_pct"].abs()
    return out


def deviation_pct_from_baseline(ratio_value: float, baseline_value: float) -> float:
    """Calculate percentage deviation from any supplied baseline ratio."""
    if not np.isfinite(ratio_value) or not np.isfinite(baseline_value) or baseline_value <= 0:
        return np.nan
    return float(((ratio_value / baseline_value) - 1.0) * 100.0)


def recalibrate_baseline(
    ratio: np.ndarray,
    dates: np.ndarray,
    trading_dates: np.ndarray,
    exit_i: int,
    current_trading_date: date,
) -> Optional[BaselineState]:
    """Recalculate baseline from the last REBASE_LOOKBACK_BARS same-day bars."""
    start_i = max(0, exit_i - REBASE_LOOKBACK_BARS + 1)
    idxs = [k for k in range(start_i, exit_i + 1) if trading_dates[k] == current_trading_date]
    if len(idxs) < REBASE_MIN_BARS:
        return None
    values = ratio[idxs]
    values = values[np.isfinite(values)]
    if len(values) < REBASE_MIN_BARS:
        return None
    return BaselineState(
        source="RECALIBRATED_AFTER_FORCED_EXIT",
        value=float(np.mean(values)),
        set_time=pd.Timestamp(dates[exit_i]),
        set_index=exit_i,
        valid_trading_date=current_trading_date,
        lookback_bars_used=int(len(values)),
    )


def get_active_baseline_for_index(
    i: int,
    baseline_state: BaselineState,
    trading_dates: np.ndarray,
    prev_day_baseline: np.ndarray,
) -> Tuple[float, str, Optional[pd.Timestamp], int]:
    """Return active baseline and metadata for current row."""
    current_day = trading_dates[i]
    if (
        baseline_state.source.startswith("RECALIBRATED")
        and baseline_state.valid_trading_date == current_day
        and np.isfinite(baseline_state.value)
        and baseline_state.value > 0
    ):
        return (
            float(baseline_state.value),
            baseline_state.source,
            baseline_state.set_time,
            int(baseline_state.lookback_bars_used),
        )
    return float(prev_day_baseline[i]), "PREV_DAY", None, 0


# =============================================================================
# QUANTITY / PNL LOGIC
# =============================================================================

def round_qty_to_lot(raw_qty: float, lot_size: int) -> int:
    """Round quantity to nearest positive lot multiple."""
    lot = max(int(lot_size), 1)
    if raw_qty <= 0 or not math.isfinite(raw_qty):
        return lot
    if not ROUND_QTY_TO_LOTS or lot == 1:
        return max(1, int(round(raw_qty)))
    lots = max(1, int(round(raw_qty / lot)))
    return lots * lot


def determine_entry_quantities(denominator_price: float, numerator_price: float) -> EntryQuantities:
    """Determine pair quantities at entry."""
    if QTY_MODE == "FIXED":
        den_qty = max(1, int(DENOMINATOR_QTY_FIXED))
        num_qty = max(1, int(NUMERATOR_QTY_FIXED))
    elif QTY_MODE in {"NOTIONAL_BALANCED", "EQUAL_NOTIONAL"}:
        den_qty = round_qty_to_lot(BASE_NOTIONAL_RUPEES / denominator_price, DENOMINATOR_LOT_SIZE)
        num_qty = round_qty_to_lot(BASE_NOTIONAL_RUPEES / numerator_price, NUMERATOR_LOT_SIZE)
    else:
        raise ValueError("QTY_MODE must be FIXED or NOTIONAL_BALANCED")

    return EntryQuantities(
        numerator_qty=int(num_qty),
        denominator_qty=int(den_qty),
        numerator_notional=float(num_qty * numerator_price),
        denominator_notional=float(den_qty * denominator_price),
        qty_mode=QTY_MODE,
    )


def infer_side(entry_deviation_pct: float) -> str:
    """Map deviation sign to pair-trade direction."""
    if entry_deviation_pct > 0:
        return f"{NUMERATOR_SYMBOL}_RICH_SHORT_{NUMERATOR_SYMBOL}_LONG_{DENOMINATOR_SYMBOL}"
    return f"{NUMERATOR_SYMBOL}_CHEAP_LONG_{NUMERATOR_SYMBOL}_SHORT_{DENOMINATOR_SYMBOL}"


def compute_pair_pnl_path(
    side: str,
    entry_denominator: float,
    entry_numerator: float,
    path_denominator: np.ndarray,
    path_numerator: np.ndarray,
    denominator_qty: int,
    numerator_qty: int,
) -> np.ndarray:
    """Compute rupee PnL path for the pair."""
    numerator_rich_side = side.startswith(f"{NUMERATOR_SYMBOL}_RICH")
    numerator_cheap_side = side.startswith(f"{NUMERATOR_SYMBOL}_CHEAP")

    if numerator_rich_side:
        # Short numerator, long denominator.
        return ((entry_numerator - path_numerator) * numerator_qty) + ((path_denominator - entry_denominator) * denominator_qty)

    if numerator_cheap_side:
        # Long numerator, short denominator.
        return ((path_numerator - entry_numerator) * numerator_qty) + ((entry_denominator - path_denominator) * denominator_qty)

    raise ValueError(f"Unknown side: {side}")


def current_pair_pnl(
    side: str,
    entry_denominator: float,
    entry_numerator: float,
    current_denominator: float,
    current_numerator: float,
    denominator_qty: int,
    numerator_qty: int,
) -> float:
    """Compute current PnL for a single bar."""
    return float(
        compute_pair_pnl_path(
            side,
            entry_denominator,
            entry_numerator,
            np.array([current_denominator], dtype=float),
            np.array([current_numerator], dtype=float),
            denominator_qty,
            numerator_qty,
        )[0]
    )


# =============================================================================
# EVENT BUILDING
# =============================================================================

def is_time_allowed(ts: pd.Timestamp) -> bool:
    """Return True if entry timestamp passes optional entry-time filter."""
    if not ENABLE_ENTRY_TIME_FILTER:
        return True
    return ENTRY_START_TIME <= ts.time() <= LAST_ENTRY_TIME


def last_allowed_same_day_index(dates: np.ndarray, trading_dates: np.ndarray, entry_i: int) -> int:
    """Return last index allowed for no-overnight holding."""
    entry_day = trading_dates[entry_i]
    j = entry_i
    last_same_day = entry_i
    last_before_force = None
    while j < len(dates) and trading_dates[j] == entry_day:
        last_same_day = j
        if pd.Timestamp(dates[j]).time() <= FORCE_EXIT_TIME:
            last_before_force = j
        j += 1
    return last_before_force if last_before_force is not None else last_same_day


def build_events_for_threshold(df: pd.DataFrame, threshold_pct: float) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Build non-overlapping pair-deviation events for one threshold."""
    required_cols = ["date", "trading_date", "denominator_close", "numerator_close", "ratio", "prev_day_avg_ratio"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    work = df.dropna(subset=["ratio", "prev_day_avg_ratio"]).copy().reset_index(drop=True)

    dates = work["date"].to_numpy()
    trading_dates = work["trading_date"].to_numpy()
    denominator = work["denominator_close"].to_numpy(dtype=float)
    numerator = work["numerator_close"].to_numpy(dtype=float)
    ratio = work["ratio"].to_numpy(dtype=float)
    prev_day_baseline = work["prev_day_avg_ratio"].to_numpy(dtype=float)

    rows: List[Dict] = []
    rebase_rows: List[Dict] = []
    n = len(work)
    i = 1
    event_id = 0
    baseline_state = BaselineState()

    while i < n:
        baseline_i, baseline_source_i, baseline_set_time_i, baseline_lb_i = get_active_baseline_for_index(
            i, baseline_state, trading_dates, prev_day_baseline
        )

        dev_i = deviation_pct_from_baseline(float(ratio[i]), baseline_i)
        dev_prev = deviation_pct_from_baseline(float(ratio[i - 1]), baseline_i)

        crossed = np.isfinite(dev_i) and np.isfinite(dev_prev) and abs(dev_i) >= threshold_pct and abs(dev_prev) < threshold_pct
        if not crossed:
            i += 1
            continue

        entry_time = pd.Timestamp(dates[i])
        if not is_time_allowed(entry_time):
            i += 1
            continue

        event_id += 1
        entry_i = i
        entry_baseline = float(baseline_i)
        entry_ratio = float(ratio[entry_i])
        entry_dev = deviation_pct_from_baseline(entry_ratio, entry_baseline)
        entry_abs_dev = abs(entry_dev)
        side = infer_side(entry_dev)

        entry_denominator = float(denominator[entry_i])
        entry_numerator = float(numerator[entry_i])
        q = determine_entry_quantities(entry_denominator, entry_numerator)

        max_j = min(n - 1, entry_i + MAX_LOOKAHEAD_BARS)
        if HARD_TIME_STOP_BARS > 0:
            max_j = min(max_j, entry_i + HARD_TIME_STOP_BARS)

        same_day_limit_j = last_allowed_same_day_index(dates, trading_dates, entry_i) if NO_OVERNIGHT else n - 1
        max_j = min(max_j, same_day_limit_j)

        exit_j: Optional[int] = None
        settle_j: Optional[int] = None
        exit_reason = "FORCED_MAX_WAIT_EXIT"

        for j in range(entry_i + 1, max_j + 1):
            dev_j = deviation_pct_from_baseline(float(ratio[j]), entry_baseline)
            pnl_j = current_pair_pnl(
                side, entry_denominator, entry_numerator,
                float(denominator[j]), float(numerator[j]),
                q.denominator_qty, q.numerator_qty,
            )
            bars_held = j - entry_i

            if abs(dev_j) <= SETTLE_DEVIATION_PCT:
                settle_j = j
                exit_j = j
                exit_reason = "SETTLED_TO_ENTRY_BASELINE"
                break
            if MAX_LOSS_RUPEES > 0 and pnl_j <= -abs(MAX_LOSS_RUPEES):
                exit_j = j
                exit_reason = "MAX_LOSS_STOP"
                break
            if HARD_TIME_STOP_BARS > 0 and bars_held >= HARD_TIME_STOP_BARS:
                exit_j = j
                exit_reason = "HARD_TIME_STOP"
                break
            if NO_OVERNIGHT and j >= same_day_limit_j:
                exit_j = j
                exit_reason = "NO_OVERNIGHT_EXIT"
                break

        if exit_j is None:
            exit_j = max_j
            if NO_OVERNIGHT and exit_j >= same_day_limit_j:
                exit_reason = "NO_OVERNIGHT_EXIT"
            elif HARD_TIME_STOP_BARS > 0 and exit_j >= entry_i + HARD_TIME_STOP_BARS:
                exit_reason = "HARD_TIME_STOP"
            else:
                exit_reason = "FORCED_MAX_WAIT_EXIT"

        path_slice = slice(entry_i, exit_j + 1)
        path_denominator = denominator[path_slice]
        path_numerator = numerator[path_slice]
        path_ratio = ratio[path_slice]
        path_dates = dates[path_slice]

        path_dev = ((path_ratio / entry_baseline) - 1.0) * 100.0
        path_abs_dev = np.abs(path_dev)

        pnl_path = compute_pair_pnl_path(
            side,
            entry_denominator,
            entry_numerator,
            path_denominator,
            path_numerator,
            q.denominator_qty,
            q.numerator_qty,
        )
        gross_exit_pnl = float(pnl_path[-1])
        net_exit_pnl = gross_exit_pnl - COST_PER_TRADE_RUPEES

        min_pnl_idx = int(np.nanargmin(pnl_path))
        max_pnl_idx = int(np.nanargmax(pnl_path))
        max_loss_rupees = float(pnl_path[min_pnl_idx])
        max_profit_rupees = float(pnl_path[max_pnl_idx])
        max_loss_abs_rupees = abs(min(0.0, max_loss_rupees))

        max_abs_dev_idx = int(np.nanargmax(path_abs_dev))
        max_abs_dev_value = float(path_abs_dev[max_abs_dev_idx])

        if entry_dev > 0:
            directional_worst_dev = float(np.nanmax(path_dev))
            directional_worst_idx = int(np.nanargmax(path_dev))
        else:
            directional_worst_dev = float(np.nanmin(path_dev))
            directional_worst_idx = int(np.nanargmin(path_dev))

        positive_indices = np.where(pnl_path > 0)[0]
        first_positive_idx = int(positive_indices[0]) if len(positive_indices) else None

        bars_held = int(exit_j - entry_i)
        calendar_minutes_held = float((pd.Timestamp(dates[exit_j]) - pd.Timestamp(dates[entry_i])).total_seconds() / 60.0)

        rows.append({
            "event_id": event_id,
            "threshold_pct": threshold_pct,
            "entry_time": entry_time,
            "entry_date": entry_time.date(),
            "entry_trading_date": trading_dates[entry_i],
            "pair": f"{NUMERATOR_SYMBOL}/{DENOMINATOR_SYMBOL}",
            "numerator_symbol": NUMERATOR_SYMBOL,
            "denominator_symbol": DENOMINATOR_SYMBOL,
            "side": side,
            "entry_ratio": entry_ratio,
            "entry_baseline_ratio": entry_baseline,
            "entry_baseline_source": baseline_source_i,
            "entry_baseline_set_time": baseline_set_time_i if baseline_set_time_i is not None else pd.NaT,
            "entry_baseline_lookback_bars": baseline_lb_i,
            "entry_deviation_pct": entry_dev,
            "entry_abs_deviation_pct": entry_abs_dev,
            "entry_denominator_close": entry_denominator,
            "entry_numerator_close": entry_numerator,
            "settled": bool(settle_j is not None),
            "settle_time": pd.Timestamp(dates[settle_j]) if settle_j is not None else pd.NaT,
            "exit_time": pd.Timestamp(dates[exit_j]),
            "exit_reason": exit_reason,
            "exit_ratio": float(ratio[exit_j]),
            "exit_deviation_from_entry_baseline_pct": float(path_dev[-1]),
            "exit_abs_deviation_from_entry_baseline_pct": float(path_abs_dev[-1]),
            "exit_denominator_close": float(denominator[exit_j]),
            "exit_numerator_close": float(numerator[exit_j]),
            "bars_to_exit": bars_held,
            "bars_to_settle": bars_held if settle_j is not None else np.nan,
            "approx_trading_days_to_exit": bars_held / float(INTRADAY_BARS_PER_DAY),
            "calendar_minutes_to_exit": calendar_minutes_held,
            "max_abs_deviation_during_wait_pct": max_abs_dev_value,
            "max_abs_deviation_time": pd.Timestamp(path_dates[max_abs_dev_idx]),
            "directional_worst_deviation_pct": directional_worst_dev,
            "directional_worst_deviation_time": pd.Timestamp(path_dates[directional_worst_idx]),
            "max_loss_rupees": max_loss_rupees,
            "max_loss_abs_rupees": max_loss_abs_rupees,
            "max_loss_time": pd.Timestamp(path_dates[min_pnl_idx]),
            "max_profit_rupees": max_profit_rupees,
            "max_profit_time": pd.Timestamp(path_dates[max_pnl_idx]),
            "first_positive_pnl_time": pd.Timestamp(path_dates[first_positive_idx]) if first_positive_idx is not None else pd.NaT,
            "first_positive_pnl_bars": int(first_positive_idx) if first_positive_idx is not None else np.nan,
            "gross_exit_pnl_rupees": gross_exit_pnl,
            "cost_rupees": COST_PER_TRADE_RUPEES,
            "net_exit_pnl_rupees": net_exit_pnl,
            "qty_mode": q.qty_mode,
            "base_notional_rupees": BASE_NOTIONAL_RUPEES,
            "denominator_qty": q.denominator_qty,
            "numerator_qty": q.numerator_qty,
            "denominator_notional_at_entry": q.denominator_notional,
            "numerator_notional_at_entry": q.numerator_notional,
            "denominator_points_at_exit": float(denominator[exit_j] - denominator[entry_i]),
            "numerator_points_at_exit": float(numerator[exit_j] - numerator[entry_i]),
        })

        # Recalibrate baseline after forced exit, if enabled.
        if ENABLE_REBASE_AFTER_FORCED_EXIT and exit_reason.upper() in REBASE_AFTER_EXIT_REASONS:
            new_state = recalibrate_baseline(ratio, dates, trading_dates, exit_j, trading_dates[exit_j])
            if new_state is not None:
                baseline_state = new_state
                rebase_rows.append({
                    "threshold_pct": threshold_pct,
                    "after_event_id": event_id,
                    "forced_exit_reason": exit_reason,
                    "rebase_time": new_state.set_time,
                    "rebase_index": new_state.set_index,
                    "rebase_trading_date": new_state.valid_trading_date,
                    "rebase_lookback_bars_requested": REBASE_LOOKBACK_BARS,
                    "rebase_lookback_bars_used": new_state.lookback_bars_used,
                    "new_baseline_ratio": new_state.value,
                    "prev_day_baseline_at_exit": float(prev_day_baseline[exit_j]),
                    "ratio_at_exit": float(ratio[exit_j]),
                    "deviation_from_new_baseline_at_exit_pct": deviation_pct_from_baseline(float(ratio[exit_j]), new_state.value),
                })
            else:
                rebase_rows.append({
                    "threshold_pct": threshold_pct,
                    "after_event_id": event_id,
                    "forced_exit_reason": exit_reason,
                    "rebase_time": pd.Timestamp(dates[exit_j]),
                    "rebase_index": exit_j,
                    "rebase_trading_date": trading_dates[exit_j],
                    "rebase_lookback_bars_requested": REBASE_LOOKBACK_BARS,
                    "rebase_lookback_bars_used": 0,
                    "new_baseline_ratio": np.nan,
                    "prev_day_baseline_at_exit": float(prev_day_baseline[exit_j]),
                    "ratio_at_exit": float(ratio[exit_j]),
                    "deviation_from_new_baseline_at_exit_pct": np.nan,
                    "message": "Not enough same-day bars to recalibrate baseline.",
                })

        i = exit_j + 1 if SKIP_OVERLAPPING_EVENTS else i + 1

    return pd.DataFrame(rows), pd.DataFrame(rebase_rows)


# =============================================================================
# SUMMARY / REPORTING
# =============================================================================

def safe_percent(numer: float, denom: float) -> float:
    """Return percentage safely."""
    return float(numer / denom * 100.0) if denom else np.nan


def profit_factor(pnl: pd.Series) -> float:
    """Gross profit divided by gross loss."""
    wins = float(pnl[pnl > 0].sum())
    losses = float(-pnl[pnl < 0].sum())
    if losses == 0:
        return np.inf if wins > 0 else np.nan
    return wins / losses


def summarize_events(events: pd.DataFrame, threshold_pct: float, trading_days: int) -> pd.DataFrame:
    """Create one-row summary for one threshold."""
    if events.empty:
        return pd.DataFrame([{
            "threshold_pct": threshold_pct,
            "total_events": 0,
            "trading_days": trading_days,
            "events_per_trading_day": 0.0,
        }])

    settled = events[events["settled"] == True].copy()  # noqa: E712
    pnl = events["net_exit_pnl_rupees"].astype(float)
    exit_counts = events["exit_reason"].value_counts().to_dict()

    return pd.DataFrame([{
        "pair": f"{NUMERATOR_SYMBOL}/{DENOMINATOR_SYMBOL}",
        "threshold_pct": threshold_pct,
        "total_events": int(len(events)),
        "trading_days": int(trading_days),
        "events_per_trading_day": float(len(events) / trading_days) if trading_days else np.nan,
        "settled_count": int(len(settled)),
        "not_settled_count": int(len(events) - len(settled)),
        "settlement_rate_pct": safe_percent(len(settled), len(events)),
        "exit_settled_count": int(exit_counts.get("SETTLED_TO_ENTRY_BASELINE", 0)),
        "exit_max_loss_count": int(exit_counts.get("MAX_LOSS_STOP", 0)),
        "exit_hard_time_stop_count": int(exit_counts.get("HARD_TIME_STOP", 0)),
        "exit_no_overnight_count": int(exit_counts.get("NO_OVERNIGHT_EXIT", 0)),
        "median_bars_to_settle": float(settled["bars_to_settle"].median()) if not settled.empty else np.nan,
        "p75_bars_to_settle": float(settled["bars_to_settle"].quantile(0.75)) if not settled.empty else np.nan,
        "p90_bars_to_settle": float(settled["bars_to_settle"].quantile(0.90)) if not settled.empty else np.nan,
        "p95_bars_to_settle": float(settled["bars_to_settle"].quantile(0.95)) if not settled.empty else np.nan,
        "max_bars_to_settle": float(settled["bars_to_settle"].max()) if not settled.empty else np.nan,
        "avg_net_pnl_per_event": float(pnl.mean()),
        "median_net_pnl_per_event": float(pnl.median()),
        "net_total_pnl_rupees": float(pnl.sum()),
        "win_count_net": int((pnl > 0).sum()),
        "loss_count_net": int((pnl <= 0).sum()),
        "win_rate_net_pct": safe_percent(int((pnl > 0).sum()), len(events)),
        "profit_factor_net": profit_factor(pnl),
        "avg_max_loss_abs_rupees": float(events["max_loss_abs_rupees"].mean()),
        "median_max_loss_abs_rupees": float(events["max_loss_abs_rupees"].median()),
        "p90_max_loss_abs_rupees": float(events["max_loss_abs_rupees"].quantile(0.90)),
        "max_loss_abs_rupees_worst_case": float(events["max_loss_abs_rupees"].max()),
        "qty_mode": QTY_MODE,
        "base_notional_rupees": BASE_NOTIONAL_RUPEES,
        "settle_deviation_pct": SETTLE_DEVIATION_PCT,
        "hard_time_stop_bars": HARD_TIME_STOP_BARS,
        "max_loss_rupees_stop": MAX_LOSS_RUPEES,
        "no_overnight": NO_OVERNIGHT,
        "force_exit_time": FORCE_EXIT_TIME.isoformat(timespec="minutes"),
        "rebase_enabled": ENABLE_REBASE_AFTER_FORCED_EXIT,
    }])


def build_daily_counts(events: pd.DataFrame) -> pd.DataFrame:
    """Count events by entry date."""
    if events.empty:
        return pd.DataFrame(columns=["entry_date", "events"])
    return events.groupby("entry_date", as_index=False).size().rename(columns={"size": "events"})


def build_by_side_summary(events: pd.DataFrame) -> pd.DataFrame:
    """Summarize by side."""
    if events.empty:
        return pd.DataFrame()
    rows = []
    for side, g in events.groupby("side"):
        pnl = g["net_exit_pnl_rupees"].astype(float)
        settled_count = int(g["settled"].sum())
        rows.append({
            "side": side,
            "events": int(len(g)),
            "settled_count": settled_count,
            "settlement_rate_pct": safe_percent(settled_count, len(g)),
            "avg_net_pnl_per_event": float(pnl.mean()),
            "median_net_pnl_per_event": float(pnl.median()),
            "net_total_pnl_rupees": float(pnl.sum()),
            "win_rate_net_pct": safe_percent(int((pnl > 0).sum()), len(g)),
            "profit_factor_net": profit_factor(pnl),
            "median_bars_to_settle": float(g.loc[g["settled"] == True, "bars_to_settle"].median()) if settled_count else np.nan,  # noqa: E712
            "p90_max_loss_abs_rupees": float(g["max_loss_abs_rupees"].quantile(0.90)),
        })
    return pd.DataFrame(rows)


def build_holding_bucket_summary(events: pd.DataFrame) -> pd.DataFrame:
    """Summarize PnL by holding bucket."""
    if events.empty:
        return pd.DataFrame()
    bins = [-1, 5, 15, 30, 60, 100, 120, 240, 375, 750, 999999]
    labels = ["<=5", "6-15", "16-30", "31-60", "61-100", "101-120", "121-240", "241-375", "376-750", ">750"]
    tmp = events.copy()
    tmp["holding_bucket"] = pd.cut(tmp["bars_to_exit"], bins=bins, labels=labels)
    rows = []
    for bucket, g in tmp.groupby("holding_bucket", observed=True):
        pnl = g["net_exit_pnl_rupees"].astype(float)
        rows.append({
            "holding_bucket": str(bucket),
            "events": int(len(g)),
            "net_total_pnl_rupees": float(pnl.sum()),
            "avg_net_pnl_per_event": float(pnl.mean()),
            "median_net_pnl_per_event": float(pnl.median()),
            "win_rate_net_pct": safe_percent(int((pnl > 0).sum()), len(g)),
            "profit_factor_net": profit_factor(pnl),
            "worst_exit_pnl": float(pnl.min()),
            "worst_max_loss_abs_rupees": float(g["max_loss_abs_rupees"].max()),
        })
    return pd.DataFrame(rows)


def build_exit_reason_summary(events: pd.DataFrame) -> pd.DataFrame:
    """Summarize by exit reason."""
    if events.empty:
        return pd.DataFrame()
    rows = []
    for reason, g in events.groupby("exit_reason"):
        pnl = g["net_exit_pnl_rupees"].astype(float)
        rows.append({
            "exit_reason": reason,
            "events": int(len(g)),
            "settled_count": int(g["settled"].sum()),
            "net_total_pnl_rupees": float(pnl.sum()),
            "avg_net_pnl_per_event": float(pnl.mean()),
            "median_net_pnl_per_event": float(pnl.median()),
            "win_rate_net_pct": safe_percent(int((pnl > 0).sum()), len(g)),
            "profit_factor_net": profit_factor(pnl),
            "worst_exit_pnl": float(pnl.min()),
            "worst_max_loss_abs_rupees": float(g["max_loss_abs_rupees"].max()),
        })
    return pd.DataFrame(rows).sort_values("events", ascending=False).reset_index(drop=True)


def autosize_excel_columns(writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame, max_width: int = 45) -> None:
    """Best-effort Excel column autosizing."""
    try:
        ws = writer.sheets[sheet_name]
        for idx, col in enumerate(df.columns, start=1):
            values = [str(col)] + [str(x) for x in df[col].head(200).tolist()]
            width = min(max(len(x) for x in values) + 2, max_width)
            ws.column_dimensions[ws.cell(row=1, column=idx).column_letter].width = width
    except Exception:
        pass


def make_config_df(data_source: DataSourceInfo, aligned: pd.DataFrame) -> pd.DataFrame:
    """Create key-value config table."""
    start_d, end_d = get_date_range()
    rows = [
        ("strategy", "prev_day_avg_ratio_pair_recalibrated"),
        ("exchange", EXCHANGE),
        ("pair", f"{NUMERATOR_SYMBOL}/{DENOMINATOR_SYMBOL}"),
        ("numerator_symbol", NUMERATOR_SYMBOL),
        ("denominator_symbol", DENOMINATOR_SYMBOL),
        ("data_source_mode", data_source.mode),
        ("data_source_path_1", data_source.path_1),
        ("data_source_path_2", data_source.path_2),
        ("start_date", str(start_d)),
        ("end_date", str(end_d)),
        ("aligned_rows", len(aligned)),
        ("first_timestamp", str(aligned["date"].min())),
        ("last_timestamp", str(aligned["date"].max())),
        ("trading_days", aligned["trading_date"].nunique()),
        ("baseline_mode", PREV_DAY_BASELINE_MODE),
        ("thresholds_pct", ",".join(str(x) for x in THRESHOLDS_PCT)),
        ("settle_deviation_pct", SETTLE_DEVIATION_PCT),
        ("max_lookahead_bars", MAX_LOOKAHEAD_BARS),
        ("hard_time_stop_bars", HARD_TIME_STOP_BARS),
        ("max_loss_rupees_stop", MAX_LOSS_RUPEES),
        ("no_overnight", NO_OVERNIGHT),
        ("force_exit_time", FORCE_EXIT_TIME.isoformat(timespec="minutes")),
        ("enable_rebase_after_forced_exit", ENABLE_REBASE_AFTER_FORCED_EXIT),
        ("rebase_after_exit_reasons", ",".join(sorted(REBASE_AFTER_EXIT_REASONS))),
        ("rebase_lookback_bars", REBASE_LOOKBACK_BARS),
        ("rebase_min_bars", REBASE_MIN_BARS),
        ("qty_mode", QTY_MODE),
        ("base_notional_rupees", BASE_NOTIONAL_RUPEES),
        ("numerator_qty_fixed", NUMERATOR_QTY_FIXED),
        ("denominator_qty_fixed", DENOMINATOR_QTY_FIXED),
        ("numerator_lot_size", NUMERATOR_LOT_SIZE),
        ("denominator_lot_size", DENOMINATOR_LOT_SIZE),
        ("round_qty_to_lots", ROUND_QTY_TO_LOTS),
        ("cost_per_trade_rupees", COST_PER_TRADE_RUPEES),
        ("skip_overlapping_events", SKIP_OVERLAPPING_EVENTS),
        ("enable_entry_time_filter", ENABLE_ENTRY_TIME_FILTER),
        ("entry_start_time", ENTRY_START_TIME.isoformat(timespec="minutes")),
        ("last_entry_time", LAST_ENTRY_TIME.isoformat(timespec="minutes")),
        ("note", "Ratio = numerator / denominator. Positive deviation means numerator rich vs denominator."),
        ("pnl_note", "Spot equity close levels are used as proxy. Configure lot sizes/costs for futures-style testing."),
    ]
    return pd.DataFrame(rows, columns=["parameter", "value"])


def write_threshold_report(
    threshold_pct: float,
    events: pd.DataFrame,
    rebase_events: pd.DataFrame,
    summary: pd.DataFrame,
    config_df: pd.DataFrame,
) -> str:
    """Write threshold Excel + CSV reports."""
    label = str(threshold_pct).replace(".", "_").rstrip("0").rstrip("_")
    threshold_dir = os.path.join(OUTPUT_DIR, f"ratio_dev_ge_{label}pct")
    os.makedirs(threshold_dir, exist_ok=True)

    xlsx_path = os.path.join(threshold_dir, f"{PAIR_NAME.lower()}_ratio_dev_ge_{label}pct_recalibrated.xlsx")
    csv_path = os.path.join(threshold_dir, f"{PAIR_NAME.lower()}_ratio_dev_ge_{label}pct_recalibrated_events.csv")
    rebase_csv_path = os.path.join(threshold_dir, f"{PAIR_NAME.lower()}_ratio_dev_ge_{label}pct_recalibrations.csv")

    preferred_cols = [
        "event_id", "threshold_pct", "entry_time", "pair", "side",
        "entry_deviation_pct", "entry_abs_deviation_pct", "entry_ratio", "entry_baseline_ratio",
        "entry_baseline_source", "entry_baseline_set_time", "entry_baseline_lookback_bars",
        "settled", "settle_time", "exit_time", "exit_reason",
        "exit_deviation_from_entry_baseline_pct", "bars_to_settle", "bars_to_exit",
        "entry_denominator_close", "entry_numerator_close", "exit_denominator_close", "exit_numerator_close",
        "max_loss_abs_rupees", "max_loss_rupees", "max_loss_time", "max_profit_rupees", "max_profit_time",
        "gross_exit_pnl_rupees", "cost_rupees", "net_exit_pnl_rupees",
        "first_positive_pnl_time", "first_positive_pnl_bars",
        "max_abs_deviation_during_wait_pct", "directional_worst_deviation_pct",
        "denominator_points_at_exit", "numerator_points_at_exit",
        "qty_mode", "base_notional_rupees", "denominator_qty", "numerator_qty",
        "denominator_notional_at_entry", "numerator_notional_at_entry",
    ]

    events_out = events.copy() if events.empty else events[preferred_cols + [c for c in events.columns if c not in preferred_cols]].copy()
    events_out.to_csv(csv_path, index=False)
    rebase_events.to_csv(rebase_csv_path, index=False)

    daily_counts = build_daily_counts(events)
    by_side = build_by_side_summary(events)
    holding_buckets = build_holding_bucket_summary(events)
    exit_reason_summary = build_exit_reason_summary(events)

    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        for sheet_name, data in [
            ("summary", summary),
            ("events", events_out),
            ("exit_reason_summary", exit_reason_summary),
            ("baseline_recalibrations", rebase_events),
            ("daily_counts", daily_counts),
            ("by_side", by_side),
            ("holding_buckets", holding_buckets),
            ("config", config_df),
        ]:
            data.to_excel(writer, sheet_name=sheet_name, index=False)
            autosize_excel_columns(writer, sheet_name, data)

    print(f"[DONE] threshold={threshold_pct}% events={len(events)} -> {xlsx_path}")
    return xlsx_path


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    """Run the HDFC-ICICI / generic pair previous-day ratio backtest."""
    print("============================================================")
    print("Generic stock-pair previous-day ratio backtester")
    print("============================================================")
    print(f"[PAIR] ratio = {NUMERATOR_SYMBOL} / {DENOMINATOR_SYMBOL}")

    ensure_output_dir()

    print("[STEP] Loading or downloading data ...")
    aligned, data_source = find_or_download_data()
    if aligned.empty:
        raise RuntimeError("Aligned dataset is empty.")

    print(f"[INFO] Data source: {data_source.mode} {data_source.path_1} {data_source.path_2}")
    print(f"[INFO] Rows: {len(aligned):,}; trading days: {aligned['trading_date'].nunique():,}")
    print(f"[INFO] Range: {aligned['date'].min()} -> {aligned['date'].max()}")

    print("[STEP] Calculating ratio and previous-day baseline ...")
    enriched = add_prev_day_ratio_baseline(aligned)
    enriched_path = os.path.join(OUTPUT_DIR, f"{PAIR_NAME.lower()}_prevday_ratio_enriched.pkl")
    enriched.to_pickle(enriched_path)
    if os.environ.get("SAVE_ENRICHED_CSV", "0").strip().lower() in {"1", "true", "yes", "y"}:
        enriched.to_csv(enriched_path.replace(".pkl", ".csv"), index=False)
    print(f"[DONE] Saved enriched data: {enriched_path}")

    trading_days = int(enriched["trading_date"].nunique())
    config_df = make_config_df(data_source, enriched)

    all_summaries: List[pd.DataFrame] = []
    files: List[Dict] = []

    print("[STEP] Building threshold reports ...")
    for threshold in THRESHOLDS_PCT:
        events, rebase_events = build_events_for_threshold(enriched, threshold_pct=threshold)
        summary = summarize_events(events, threshold_pct=threshold, trading_days=trading_days)
        all_summaries.append(summary)
        report_path = write_threshold_report(threshold, events, rebase_events, summary, config_df)
        files.append({"threshold_pct": threshold, "events": len(events), "recalibrations": len(rebase_events), "file": report_path})

    combined_summary = pd.concat(all_summaries, ignore_index=True) if all_summaries else pd.DataFrame()
    files_df = pd.DataFrame(files)

    combined_path = os.path.join(OUTPUT_DIR, f"{PAIR_NAME.lower()}_combined_prevday_ratio_summary.xlsx")
    with pd.ExcelWriter(combined_path, engine="openpyxl") as writer:
        combined_summary.to_excel(writer, sheet_name="combined_summary", index=False)
        autosize_excel_columns(writer, "combined_summary", combined_summary)
        files_df.to_excel(writer, sheet_name="files", index=False)
        autosize_excel_columns(writer, "files", files_df)
        config_df.to_excel(writer, sheet_name="config", index=False)
        autosize_excel_columns(writer, "config", config_df)

    print("\n==================== FINAL SUMMARY ====================")
    if not combined_summary.empty:
        cols = [
            "threshold_pct", "total_events", "events_per_trading_day",
            "settlement_rate_pct", "exit_max_loss_count", "exit_hard_time_stop_count", "exit_no_overnight_count",
            "net_total_pnl_rupees", "avg_net_pnl_per_event", "win_rate_net_pct", "profit_factor_net", "max_loss_abs_rupees_worst_case",
        ]
        existing_cols = [c for c in cols if c in combined_summary.columns]
        print(combined_summary[existing_cols].to_string(index=False))
    print("-------------------------------------------------------")
    print(f"Combined summary: {combined_path}")
    print(f"Output directory : {OUTPUT_DIR}")
    print("=======================================================")


if __name__ == "__main__":
    main()
