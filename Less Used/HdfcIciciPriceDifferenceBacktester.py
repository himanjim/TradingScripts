#!/usr/bin/env python3
"""
HdfcIciciPriceDifferenceBacktester.py
====================================

Purpose
-------
Backtest the user's **raw share-price difference** idea for the HDFCBANK-ICICIBANK pair:

    diff = ICICIBANK price - HDFCBANK price

The hypothesis is:

    "The average price difference between ICICIBANK and HDFCBANK remains
     reasonably stable intraday. If the live difference moves away from the
     previous trading day's average difference by a configurable number of
     rupees/points, for example 10 points, the difference may later return
     toward that average."

This is deliberately different from the earlier ratio and z-score methods.

Earlier z-score method:
    spread = log(ICICIBANK) - beta * log(HDFCBANK)
    z      = (spread - rolling_mean) / rolling_std

Earlier ratio method:
    ratio = ICICIBANK / HDFCBANK
    deviation_pct = ((ratio / prev_day_avg_ratio) - 1) * 100

This script's raw-difference method:
    diff = ICICIBANK - HDFCBANK
    prev_day_avg_diff = average(diff) on previous trading day
    deviation_points = diff - prev_day_avg_diff

Trade interpretation
--------------------
Let NUMERATOR   = ICICIBANK
Let DENOMINATOR = HDFCBANK

If deviation_points is positive and large:
    ICICIBANK is expensive/rich versus HDFCBANK.
    Pair trade assumed:
        SHORT ICICIBANK
        LONG  HDFCBANK

If deviation_points is negative and large:
    ICICIBANK is cheap versus HDFCBANK.
    Pair trade assumed:
        LONG  ICICIBANK
        SHORT HDFCBANK

Entry / exit logic
------------------
For each configured threshold, for example 10 points:

Entry:
    abs(deviation_points) >= threshold_points
    and previous bar was inside the threshold zone

Normal exit:
    abs(deviation_points from frozen entry baseline) <= SETTLE_DIFF_POINTS

Risk exits:
    1. STOP_LOSS_RUPEES hit
    2. HARD_EXIT_BARS reached, default 60 bars
    3. NO_OVERNIGHT exit at FORCE_EXIT_TIME / last same-day bar
    4. MAX_LOOKAHEAD_BARS reached

Important point about equal quantity
------------------------------------
The raw-difference idea is easiest to interpret when both legs use the SAME
quantity. For example:

    If diff moves back by 10 points and qty = 700 shares,
    approximate gross profit = 10 * 700 = Rs 7,000

Therefore this script supports three quantity modes:

1. EQUAL_QTY [default]
   Use the same quantity on both legs.

2. NOTIONAL_BALANCED
   Size both legs to the same rupee notional at entry.
   This is more realistic for portfolio exposure but makes raw-diff PnL less
   direct.

3. FIXED
   Use separate fixed quantities for HDFCBANK and ICICIBANK.

Data handling
-------------
The script first tries to load cached/aligned/candle files from:

    ./hdfc_icici_difference_output/
    ./hdfc_icici_deviation_output/

If missing, or FORCE_DOWNLOAD=1, it downloads 1-minute NSE historical data using:

    Trading_2024.OptionTradeUtils.intialize_kite_api()

Install dependencies
--------------------
    pip install pandas numpy openpyxl python-dateutil

Typical Windows CMD run
-----------------------
    set LOOKBACK_YEARS=4
    set DIFF_THRESHOLDS_POINTS=5,10,15,20,25
    set SETTLE_DIFF_POINTS=1
    set HARD_EXIT_BARS=60
    set STOP_LOSS_RUPEES=5000
    set QTY_MODE=EQUAL_QTY
    set PAIR_QTY=700
    python HdfcIciciPriceDifferenceBacktester.py

Output
------
Default output directory:

    ./hdfc_icici_difference_output

Files:
    candles/HDFCBANK_1min_<start>_<end>.pkl/csv
    candles/ICICIBANK_1min_<start>_<end>.pkl/csv
    icicibank_hdfcbank_difference_aligned_1min.pkl
    icicibank_hdfcbank_combined_difference_summary.xlsx
    diff_ge_10/icicibank_hdfcbank_diff_ge_10.xlsx
    diff_ge_10/icicibank_hdfcbank_diff_ge_10_events.csv

Limitations
-----------
1. Uses equity close prices as proxy execution prices.
2. Does not include bid-ask spread, slippage, taxes, or actual futures lot constraints.
3. Raw difference is scale-sensitive and can be distorted by stock-specific trends.
4. This is a research backtester, not a live trading system.
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

# Your existing Zerodha/Kite initializer used in earlier trading scripts.
import Trading_2024.OptionTradeUtils as oUtils

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

SESSION_START = dtime(9, 15, 0)
SESSION_END = dtime(15, 30, 0)

# Pair configuration.
# Difference is always calculated as:
#     diff = NUMERATOR - DENOMINATOR
DENOMINATOR_SYMBOL = os.environ.get("DENOMINATOR_SYMBOL", "HDFCBANK").strip().upper()
NUMERATOR_SYMBOL = os.environ.get("NUMERATOR_SYMBOL", "ICICIBANK").strip().upper()
DENOMINATOR_LABEL = os.environ.get("DENOMINATOR_LABEL", DENOMINATOR_SYMBOL).strip().upper()
NUMERATOR_LABEL = os.environ.get("NUMERATOR_LABEL", NUMERATOR_SYMBOL).strip().upper()
EXCHANGE = os.environ.get("EXCHANGE", "NSE").strip().upper()

# Output and history settings.
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "./hdfc_icici_difference_output")
INTERVAL = "minute"
LOOKBACK_YEARS = int(os.environ.get("LOOKBACK_YEARS", "4"))
START_DATE_ENV = os.environ.get("START_DATE", "").strip()
END_DATE_ENV = os.environ.get("END_DATE", "").strip()
FORCE_DOWNLOAD = os.environ.get("FORCE_DOWNLOAD", "0").strip().lower() in {"1", "true", "yes", "y"}

# Kite historical API safety settings.
MAX_DAYS_PER_CHUNK = int(os.environ.get("MAX_DAYS_PER_CHUNK", "25"))
MAX_ATTEMPTS = int(os.environ.get("MAX_ATTEMPTS", "5"))
SLEEP_BETWEEN_CALLS_SEC = float(os.environ.get("SLEEP_BETWEEN_CALLS_SEC", "0.20"))

# Baseline. MEAN follows your idea of average difference. MEDIAN is optional.
PREV_DAY_BASELINE_MODE = os.environ.get("PREV_DAY_BASELINE_MODE", "MEDIAN").strip().upper()

# Difference thresholds in raw rupee/point terms.
# Example: 10 means diff has moved 10 points away from previous-day average diff.
DIFF_THRESHOLDS_POINTS = [
    float(x.strip())
    for x in os.environ.get("DIFF_THRESHOLDS_POINTS", "5,10,15,20,25").split(",")
    if x.strip()
]

# Settlement threshold in raw rupee/point terms.
# Example: 1 means exit when diff returns within +/-1 point of the frozen entry baseline.
SETTLE_DIFF_POINTS = float(os.environ.get("SETTLE_DIFF_POINTS", "1"))

# Risk controls.
HARD_EXIT_BARS = int(os.environ.get("HARD_EXIT_BARS", "600"))
STOP_LOSS_RUPEES = float(os.environ.get("STOP_LOSS_RUPEES", "50000"))
NO_OVERNIGHT = os.environ.get("NO_OVERNIGHT", "1").strip().lower() in {"1", "true", "yes", "y"}
FORCE_EXIT_TIME = dtime.fromisoformat(os.environ.get("FORCE_EXIT_TIME", "15:20"))

# Diagnostic fallback max lookahead. In trading-style use, HARD_EXIT_BARS and
# NO_OVERNIGHT normally exit much earlier.
MAX_WAIT_TRADING_DAYS = int(os.environ.get("MAX_WAIT_TRADING_DAYS", "10"))
INTRADAY_BARS_PER_DAY = int(os.environ.get("INTRADAY_BARS_PER_DAY", "375"))
MAX_LOOKAHEAD_BARS = int(os.environ.get("MAX_LOOKAHEAD_BARS", str(MAX_WAIT_TRADING_DAYS * INTRADAY_BARS_PER_DAY)))

# Quantity mode.
# For the raw-difference idea, EQUAL_QTY is the cleanest diagnostic default.
QTY_MODE = os.environ.get("QTY_MODE", "EQUAL_QTY").strip().upper()
PAIR_QTY = int(os.environ.get("PAIR_QTY", "700"))
BASE_NOTIONAL_RUPEES = float(os.environ.get("BASE_NOTIONAL_RUPEES", "1000000"))
DENOMINATOR_QTY_FIXED = int(os.environ.get("DENOMINATOR_QTY_FIXED", "550"))
NUMERATOR_QTY_FIXED = int(os.environ.get("NUMERATOR_QTY_FIXED", "700"))
QTY_ROUND_STEP = int(os.environ.get("QTY_ROUND_STEP", "1"))

# Optional flat cost per completed pair trade.
COST_PER_TRADE_RUPEES = float(os.environ.get("COST_PER_TRADE_RUPEES", "0"))

# Non-overlap: count one unresolved deviation episode once.
SKIP_OVERLAPPING_EVENTS = os.environ.get("SKIP_OVERLAPPING_EVENTS", "1").strip().lower() in {
    "1", "true", "yes", "y"
}

# Optional entry-time filter. ON by default because this is now closer to a trading test.
ENABLE_ENTRY_TIME_FILTER = os.environ.get("ENABLE_ENTRY_TIME_FILTER", "1").strip().lower() in {
    "1", "true", "yes", "y"
}
ENTRY_START_TIME = dtime.fromisoformat(os.environ.get("ENTRY_START_TIME", "09:30"))
LAST_ENTRY_TIME = dtime.fromisoformat(os.environ.get("LAST_ENTRY_TIME", "14:30"))


# =============================================================================
# DATA STRUCTURES
# =============================================================================

@dataclass(frozen=True)
class EquitySpec:
    """Minimal instrument description needed to locate NSE equity token."""

    label: str
    exchange: str
    tradingsymbol: str


@dataclass(frozen=True)
class DataSourceInfo:
    """Records where the input data came from for auditability."""

    mode: str
    path_1: str
    path_2: str = ""


DENOMINATOR_SPEC = EquitySpec(DENOMINATOR_LABEL, EXCHANGE, DENOMINATOR_SYMBOL)
NUMERATOR_SPEC = EquitySpec(NUMERATOR_LABEL, EXCHANGE, NUMERATOR_SYMBOL)


# =============================================================================
# DATE / FILE HELPERS
# =============================================================================

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
    raise ValueError(f"Could not parse date: {raw!r}. Use YYYY-MM-DD or DD-MM-YYYY.")


def get_date_range() -> Tuple[date, date]:
    """Resolve start/end dates from environment variables or defaults."""
    end_d = parse_date(END_DATE_ENV) if END_DATE_ENV else ist_today()
    if START_DATE_ENV:
        start_d = parse_date(START_DATE_ENV)
    else:
        start_d = end_d - relativedelta(years=LOOKBACK_YEARS) if relativedelta else end_d - timedelta(days=365 * LOOKBACK_YEARS)
    if start_d >= end_d:
        raise ValueError(f"START_DATE must be earlier than END_DATE. Got {start_d} >= {end_d}")
    return start_d, end_d


def ensure_dirs() -> Dict[str, str]:
    """Create output directories and return their paths."""
    paths = {
        "root": OUTPUT_DIR,
        "candles": os.path.join(OUTPUT_DIR, "candles"),
    }
    for p in paths.values():
        os.makedirs(p, exist_ok=True)
    return paths


def cache_label(start_d: date, end_d: date) -> str:
    """Compact date tag for cache filenames."""
    return f"{start_d.strftime('%Y%m%d')}_{end_d.strftime('%Y%m%d')}"


def round_qty(qty: float) -> int:
    """Round quantity to configured lot/share step and ensure positive quantity."""
    if not math.isfinite(qty) or qty <= 0:
        return 0
    step = max(1, int(QTY_ROUND_STEP))
    return max(step, int(round(qty / step) * step))


# =============================================================================
# KITE / DATA LOADING HELPERS
# =============================================================================

def iter_chunks_by_date(from_dt: datetime, to_dt: datetime, days_per_chunk: int) -> List[Tuple[datetime, datetime]]:
    """Split a datetime range into chunks while preserving intraday session times."""
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


def kite_instruments_cached(kite, exchange: str, cache: Dict[str, List[Dict]]) -> List[Dict]:
    """Load and cache Kite instrument dump for an exchange."""
    ex = exchange.upper().strip()
    if ex not in cache:
        print(f"[STEP] Loading instruments dump for {ex} ...")
        cache[ex] = kite.instruments(ex)
        print(f"[INFO] {ex} instruments loaded: {len(cache[ex])}")
    return cache[ex]


def get_instrument_token(kite, spec: EquitySpec, cache: Dict[str, List[Dict]]) -> Tuple[int, str]:
    """Resolve NSE equity instrument token from tradingsymbol."""
    rows = kite_instruments_cached(kite, spec.exchange, cache)
    wanted = spec.tradingsymbol.upper().strip()

    matches = [r for r in rows if str(r.get("tradingsymbol", "")).upper().strip() == wanted]
    if not matches:
        raise ValueError(f"Instrument not found: {spec.exchange}:{spec.tradingsymbol}")

    for r in matches:
        segment = str(r.get("segment", "")).upper()
        instrument_type = str(r.get("instrument_type", "")).upper()
        if "NSE" in segment and instrument_type in {"EQ", ""}:
            return int(r["instrument_token"]), str(r.get("exchange", spec.exchange))

    r = matches[0]
    return int(r["instrument_token"]), str(r.get("exchange", spec.exchange))


def fetch_history_1min(kite, token: int, from_dt: datetime, to_dt: datetime, label: str) -> List[Dict]:
    """Fetch 1-minute historical candles from Kite with chunking and retries."""
    chunks = iter_chunks_by_date(from_dt, to_dt, MAX_DAYS_PER_CHUNK)
    print(f"[INFO] Fetching {label} token={token}, range={from_dt} to {to_dt}, chunks={len(chunks)}")
    all_rows: List[Dict] = []

    for idx, (c_from, c_to) in enumerate(chunks, start=1):
        print(f"  [CHUNK {idx:03d}/{len(chunks):03d}] {c_from} -> {c_to}")
        last_err: Optional[Exception] = None
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                rows = kite.historical_data(
                    instrument_token=token,
                    from_date=c_from,
                    to_date=c_to,
                    interval=INTERVAL,
                    continuous=False,
                    oi=False,
                )
                print(f"    [OK] {len(rows)} candles")
                all_rows.extend(rows)
                last_err = None
                break
            except Exception as e:  # noqa: BLE001
                last_err = e
                wait = min(10.0, 1.5 * attempt)
                print(f"    [WARN] attempt {attempt}/{MAX_ATTEMPTS} failed: {e}. Sleeping {wait:.1f}s")
                time.sleep(wait)

        if last_err is not None:
            print(f"    [ERROR] Giving up on chunk {idx}/{len(chunks)} for {label}: {last_err}")
        time.sleep(SLEEP_BETWEEN_CALLS_SEC)

    return all_rows


def normalize_datetime_series(s: pd.Series) -> pd.Series:
    """Normalize Kite date column to timezone-naive IST-like minute timestamps."""
    out = pd.to_datetime(s)
    try:
        if out.dt.tz is not None:
            out = out.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    except Exception:
        out = pd.to_datetime(s.astype(str), errors="coerce")
    return out.dt.floor("min")


def rows_to_dataframe(rows: List[Dict], label: str) -> pd.DataFrame:
    """Convert Kite candle rows into a clean OHLCV DataFrame."""
    if not rows:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    df = pd.DataFrame(rows)
    for col in ["date", "open", "high", "low", "close", "volume"]:
        if col not in df.columns:
            df[col] = np.nan

    df["date"] = normalize_datetime_series(df["date"])
    df = df.dropna(subset=["date", "close"])
    df = df.drop_duplicates(subset=["date"], keep="last")
    df = df.sort_values("date").reset_index(drop=True)
    df["instrument"] = label

    times = df["date"].dt.time
    df = df[(times >= SESSION_START) & (times <= SESSION_END)].copy()
    df = df[df["date"].dt.weekday < 5].copy()
    return df.reset_index(drop=True)


def load_pickle_or_csv(path: str) -> pd.DataFrame:
    """Load .pkl/.pickle/.csv into a DataFrame."""
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    lower = path.lower()
    if lower.endswith((".pkl", ".pickle")):
        return pd.read_pickle(path)
    if lower.endswith(".csv"):
        return pd.read_csv(path)
    raise ValueError(f"Unsupported file extension: {path}")


def load_or_download_equity(kite, spec: EquitySpec, start_d: date, end_d: date, paths: Dict[str, str], cache: Dict[str, List[Dict]]) -> pd.DataFrame:
    """Load cached 1-minute candles or download them from Kite."""
    range_tag = cache_label(start_d, end_d)
    out_path = os.path.join(paths["candles"], f"{spec.label}_1min_{range_tag}.pkl")

    # First try this script's date-specific cache.
    if os.path.exists(out_path) and not FORCE_DOWNLOAD:
        print(f"[CACHE] Loading {spec.label} candles from {out_path}")
        df = pd.read_pickle(out_path)
        df["date"] = pd.to_datetime(df["date"])
        return df

    # Also try earlier HDFC-ICICI z-score cache folder if it exists.
    earlier_path = os.path.join("./hdfc_icici_deviation_output", "candles", f"{spec.label}_1min_{range_tag}.pkl")
    if os.path.exists(earlier_path) and not FORCE_DOWNLOAD:
        print(f"[CACHE] Loading {spec.label} candles from earlier cache {earlier_path}")
        df = pd.read_pickle(earlier_path)
        df["date"] = pd.to_datetime(df["date"])
        # Save a local copy in this strategy's output folder for reproducibility.
        df.to_pickle(out_path)
        df.to_csv(out_path.replace(".pkl", ".csv"), index=False)
        return df

    token, real_ex = get_instrument_token(kite, spec, cache)
    from_dt = datetime.combine(start_d, SESSION_START)
    to_dt = datetime.combine(end_d, SESSION_END)
    rows = fetch_history_1min(kite, token, from_dt, to_dt, f"{real_ex}:{spec.tradingsymbol}")
    df = rows_to_dataframe(rows, spec.label)
    if df.empty:
        raise RuntimeError(f"No candle data returned for {spec.label}")

    df.to_pickle(out_path)
    df.to_csv(out_path.replace(".pkl", ".csv"), index=False)
    print(f"[DONE] Saved {spec.label} candles: {out_path} rows={len(df)}")
    return df


# =============================================================================
# DIFFERENCE / BASELINE CALCULATION
# =============================================================================

def align_pair(denom_df: pd.DataFrame, numer_df: pd.DataFrame) -> pd.DataFrame:
    """Inner-join denominator and numerator closes on common 1-minute timestamps."""
    d = denom_df[["date", "close"]].copy().rename(columns={"close": "denom_close"})
    n = numer_df[["date", "close"]].copy().rename(columns={"close": "numer_close"})
    d["date"] = pd.to_datetime(d["date"])
    n["date"] = pd.to_datetime(n["date"])

    df = pd.merge(d, n, on="date", how="inner")
    df = df.dropna(subset=["denom_close", "numer_close"])
    df = df[(df["denom_close"] > 0) & (df["numer_close"] > 0)].copy()
    df = df.drop_duplicates("date", keep="last").sort_values("date").reset_index(drop=True)
    df["trading_date"] = df["date"].dt.date
    return df


def add_previous_day_difference_baseline(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add raw difference and previous-day average difference baseline.

    diff = numer_close - denom_close
    prev_day_avg_diff = previous trading day's average diff
    deviation_points = diff - prev_day_avg_diff
    """
    out = df.copy().sort_values("date").reset_index(drop=True)
    out["diff"] = out["numer_close"] - out["denom_close"]

    if PREV_DAY_BASELINE_MODE == "MEAN":
        day_diff = out.groupby("trading_date")["diff"].mean()
    elif PREV_DAY_BASELINE_MODE == "MEDIAN":
        day_diff = out.groupby("trading_date")["diff"].median()
    else:
        raise ValueError("PREV_DAY_BASELINE_MODE must be MEAN or MEDIAN")

    trading_dates = sorted(day_diff.index)
    prev_map: Dict[date, float] = {}
    for idx, d in enumerate(trading_dates):
        prev_map[d] = np.nan if idx == 0 else float(day_diff.loc[trading_dates[idx - 1]])

    out["prev_day_avg_diff"] = out["trading_date"].map(prev_map)
    out["deviation_points"] = out["diff"] - out["prev_day_avg_diff"]
    out["abs_deviation_points"] = out["deviation_points"].abs()
    return out


# =============================================================================
# PNL / EVENT LOGIC
# =============================================================================

def compute_quantities(entry_denom: float, entry_numer: float) -> Tuple[int, int]:
    """Return denominator and numerator quantities as per QTY_MODE."""
    if QTY_MODE == "EQUAL_QTY":
        return int(PAIR_QTY), int(PAIR_QTY)

    if QTY_MODE == "FIXED":
        return int(DENOMINATOR_QTY_FIXED), int(NUMERATOR_QTY_FIXED)

    if QTY_MODE == "NOTIONAL_BALANCED":
        denom_qty = round_qty(BASE_NOTIONAL_RUPEES / entry_denom)
        numer_qty = round_qty(BASE_NOTIONAL_RUPEES / entry_numer)
        return int(denom_qty), int(numer_qty)

    raise ValueError("QTY_MODE must be EQUAL_QTY, NOTIONAL_BALANCED, or FIXED")


def infer_side(entry_deviation_points: float) -> str:
    """Map positive/negative difference deviation to pair-trade direction."""
    if entry_deviation_points > 0:
        return "NUMERATOR_RICH_SHORT_NUMERATOR_LONG_DENOMINATOR"
    return "NUMERATOR_CHEAP_LONG_NUMERATOR_SHORT_DENOMINATOR"


def compute_pair_pnl_path(
    side: str,
    entry_denom: float,
    entry_numer: float,
    path_denom: np.ndarray,
    path_numer: np.ndarray,
    denom_qty: int,
    numer_qty: int,
) -> np.ndarray:
    """Compute rupee PnL path for the pair trade."""
    if side == "NUMERATOR_RICH_SHORT_NUMERATOR_LONG_DENOMINATOR":
        return ((entry_numer - path_numer) * numer_qty) + ((path_denom - entry_denom) * denom_qty)

    if side == "NUMERATOR_CHEAP_LONG_NUMERATOR_SHORT_DENOMINATOR":
        return ((path_numer - entry_numer) * numer_qty) + ((entry_denom - path_denom) * denom_qty)

    raise ValueError(f"Unknown side: {side}")


def current_pair_pnl(
    side: str,
    entry_denom: float,
    entry_numer: float,
    current_denom: float,
    current_numer: float,
    denom_qty: int,
    numer_qty: int,
) -> float:
    """Compute current PnL at one bar."""
    if side == "NUMERATOR_RICH_SHORT_NUMERATOR_LONG_DENOMINATOR":
        return ((entry_numer - current_numer) * numer_qty) + ((current_denom - entry_denom) * denom_qty)

    if side == "NUMERATOR_CHEAP_LONG_NUMERATOR_SHORT_DENOMINATOR":
        return ((current_numer - entry_numer) * numer_qty) + ((entry_denom - current_denom) * denom_qty)

    raise ValueError(f"Unknown side: {side}")


def is_time_allowed(ts: pd.Timestamp) -> bool:
    """Return True if optional entry-time filter allows the timestamp."""
    if not ENABLE_ENTRY_TIME_FILTER:
        return True
    return ENTRY_START_TIME <= ts.time() <= LAST_ENTRY_TIME


def last_allowed_same_day_index(dates: np.ndarray, trading_dates: np.ndarray, entry_i: int) -> int:
    """Return last index to which a trade can be held without overnight carry."""
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


def build_events_for_threshold(df: pd.DataFrame, threshold_points: float) -> pd.DataFrame:
    """
    Build non-overlapping raw-difference deviation events.

    Entry:
        abs(deviation_points[i]) >= threshold_points
        and abs(deviation_points[i-1]) < threshold_points

    Settlement:
        abs(diff[j] - entry_baseline_diff) <= SETTLE_DIFF_POINTS

    The entry baseline is frozen. If the trade continues later in the day, we do
    not keep changing its baseline.
    """
    required = [
        "date", "trading_date", "denom_close", "numer_close",
        "diff", "prev_day_avg_diff", "deviation_points", "abs_deviation_points",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    work = df.dropna(subset=["prev_day_avg_diff", "deviation_points", "abs_deviation_points"]).copy().reset_index(drop=True)

    dates = work["date"].to_numpy()
    trading_dates = work["trading_date"].to_numpy()
    denom = work["denom_close"].to_numpy(dtype=float)
    numer = work["numer_close"].to_numpy(dtype=float)
    diff = work["diff"].to_numpy(dtype=float)
    baseline = work["prev_day_avg_diff"].to_numpy(dtype=float)
    dev = work["deviation_points"].to_numpy(dtype=float)
    abs_dev = work["abs_deviation_points"].to_numpy(dtype=float)

    rows: List[Dict] = []
    i = 1
    n = len(work)
    event_id = 0

    while i < n:
        crossed = abs_dev[i] >= threshold_points and abs_dev[i - 1] < threshold_points
        if not crossed:
            i += 1
            continue

        entry_time = pd.Timestamp(dates[i])
        if not is_time_allowed(entry_time):
            i += 1
            continue

        event_id += 1
        entry_i = i
        entry_baseline = float(baseline[entry_i])
        entry_diff = float(diff[entry_i])
        entry_dev = float(entry_diff - entry_baseline)
        side = infer_side(entry_dev)

        denom_qty, numer_qty = compute_quantities(float(denom[entry_i]), float(numer[entry_i]))
        entry_denom = float(denom[entry_i])
        entry_numer = float(numer[entry_i])

        max_j = min(n - 1, entry_i + MAX_LOOKAHEAD_BARS)
        if HARD_EXIT_BARS > 0:
            max_j = min(max_j, entry_i + HARD_EXIT_BARS)
        if NO_OVERNIGHT:
            max_j = min(max_j, last_allowed_same_day_index(dates, trading_dates, entry_i))

        exit_j: Optional[int] = None
        settle_j: Optional[int] = None
        exit_reason = "FORCED_MAX_WAIT_EXIT"

        for j in range(entry_i + 1, max_j + 1):
            current_dev = float(diff[j] - entry_baseline)
            pnl_j = current_pair_pnl(
                side=side,
                entry_denom=entry_denom,
                entry_numer=entry_numer,
                current_denom=float(denom[j]),
                current_numer=float(numer[j]),
                denom_qty=denom_qty,
                numer_qty=numer_qty,
            )
            bars_held = j - entry_i

            if abs(current_dev) <= SETTLE_DIFF_POINTS:
                settle_j = j
                exit_j = j
                exit_reason = "SETTLED_TO_BASELINE_DIFF"
                break

            if STOP_LOSS_RUPEES > 0 and pnl_j <= -abs(STOP_LOSS_RUPEES):
                exit_j = j
                exit_reason = "STOP_LOSS_RUPEES"
                break

            if HARD_EXIT_BARS > 0 and bars_held >= HARD_EXIT_BARS:
                exit_j = j
                exit_reason = "HARD_EXIT_BARS"
                break

            if NO_OVERNIGHT and j >= max_j and pd.Timestamp(dates[j]).date() == entry_time.date():
                # This catches same-day forced exit at the calculated end point.
                exit_j = j
                exit_reason = "NO_OVERNIGHT_EXIT"
                break

        if exit_j is None:
            exit_j = max_j
            if HARD_EXIT_BARS > 0 and exit_j >= entry_i + HARD_EXIT_BARS:
                exit_reason = "HARD_EXIT_BARS"
            elif NO_OVERNIGHT and trading_dates[exit_j] == trading_dates[entry_i]:
                exit_reason = "NO_OVERNIGHT_EXIT"
            else:
                exit_reason = "FORCED_MAX_WAIT_EXIT"

        path_slice = slice(entry_i, exit_j + 1)
        path_denom = denom[path_slice]
        path_numer = numer[path_slice]
        path_diff = diff[path_slice]
        path_dates = dates[path_slice]
        path_dev = path_diff - entry_baseline
        path_abs_dev = np.abs(path_dev)

        pnl_path = compute_pair_pnl_path(
            side=side,
            entry_denom=entry_denom,
            entry_numer=entry_numer,
            path_denom=path_denom,
            path_numer=path_numer,
            denom_qty=denom_qty,
            numer_qty=numer_qty,
        )

        gross_exit_pnl = float(pnl_path[-1])
        net_exit_pnl = gross_exit_pnl - COST_PER_TRADE_RUPEES

        min_pnl_idx = int(np.nanargmin(pnl_path))
        max_pnl_idx = int(np.nanargmax(pnl_path))
        max_loss_rupees = float(pnl_path[min_pnl_idx])
        max_profit_rupees = float(pnl_path[max_pnl_idx])
        max_loss_abs_rupees = abs(min(0.0, max_loss_rupees))

        max_abs_dev_idx = int(np.nanargmax(path_abs_dev))
        max_abs_dev_points = float(path_abs_dev[max_abs_dev_idx])

        # Direction-specific adverse deviation: if entry_dev is positive,
        # worse means diff becomes even more positive. If negative, worse means
        # diff becomes even more negative.
        if entry_dev > 0:
            directional_worst_dev = float(np.nanmax(path_dev))
            directional_worst_idx = int(np.nanargmax(path_dev))
        else:
            directional_worst_dev = float(np.nanmin(path_dev))
            directional_worst_idx = int(np.nanargmin(path_dev))

        positive_indices = np.where(pnl_path > 0)[0]
        first_positive_idx = int(positive_indices[0]) if len(positive_indices) else None

        bars_held = int(exit_j - entry_i)
        rows.append({
            "event_id": event_id,
            "threshold_points": threshold_points,
            "entry_time": entry_time,
            "entry_date": entry_time.date(),
            "entry_trading_date": trading_dates[entry_i],
            "side": side,
            "denominator_symbol": DENOMINATOR_LABEL,
            "numerator_symbol": NUMERATOR_LABEL,
            "entry_denom_close": entry_denom,
            "entry_numer_close": entry_numer,
            "entry_diff": entry_diff,
            "entry_baseline_diff": entry_baseline,
            "entry_deviation_points": entry_dev,
            "entry_abs_deviation_points": abs(entry_dev),
            "denom_qty": denom_qty,
            "numer_qty": numer_qty,
            "denom_entry_notional": entry_denom * denom_qty,
            "numer_entry_notional": entry_numer * numer_qty,
            "qty_mode": QTY_MODE,
            "pair_qty": PAIR_QTY if QTY_MODE == "EQUAL_QTY" else np.nan,
            "base_notional_rupees": BASE_NOTIONAL_RUPEES if QTY_MODE == "NOTIONAL_BALANCED" else np.nan,
            "settled": bool(settle_j is not None),
            "settle_time": pd.Timestamp(dates[settle_j]) if settle_j is not None else pd.NaT,
            "exit_time": pd.Timestamp(dates[exit_j]),
            "exit_reason": exit_reason,
            "exit_denom_close": float(denom[exit_j]),
            "exit_numer_close": float(numer[exit_j]),
            "exit_diff": float(diff[exit_j]),
            "exit_deviation_points": float(path_dev[-1]),
            "exit_abs_deviation_points": float(abs(path_dev[-1])),
            "bars_to_exit": bars_held,
            "bars_to_settle": bars_held if settle_j is not None else np.nan,
            "calendar_minutes_to_exit": float((pd.Timestamp(dates[exit_j]) - pd.Timestamp(dates[entry_i])).total_seconds() / 60.0),
            "approx_trading_days_to_exit": bars_held / float(INTRADAY_BARS_PER_DAY),
            "max_abs_deviation_during_wait_points": max_abs_dev_points,
            "max_abs_deviation_time": pd.Timestamp(path_dates[max_abs_dev_idx]),
            "directional_worst_deviation_points": directional_worst_dev,
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
            "denom_points_at_exit": float(denom[exit_j] - denom[entry_i]),
            "numer_points_at_exit": float(numer[exit_j] - numer[entry_i]),
            "settle_diff_points_config": SETTLE_DIFF_POINTS,
            "hard_exit_bars_config": HARD_EXIT_BARS,
            "stop_loss_rupees_config": STOP_LOSS_RUPEES,
        })

        if SKIP_OVERLAPPING_EVENTS:
            i = exit_j + 1
        else:
            i += 1

    return pd.DataFrame(rows)


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


def summarize_events(events: pd.DataFrame, threshold_points: float, trading_days: int) -> pd.DataFrame:
    """Create one-row summary for one threshold."""
    if events.empty:
        return pd.DataFrame([{
            "threshold_points": threshold_points,
            "total_events": 0,
            "trading_days": trading_days,
            "events_per_trading_day": 0.0,
        }])

    settled = events[events["settled"] == True].copy()  # noqa: E712
    pnl = events["net_exit_pnl_rupees"].astype(float)
    exit_counts = events["exit_reason"].value_counts().to_dict()

    return pd.DataFrame([{
        "threshold_points": threshold_points,
        "total_events": int(len(events)),
        "trading_days": int(trading_days),
        "events_per_trading_day": float(len(events) / trading_days) if trading_days else np.nan,
        "settled_count": int(len(settled)),
        "not_settled_count": int(len(events) - len(settled)),
        "settlement_rate_pct": safe_percent(len(settled), len(events)),
        "exit_settled_count": int(exit_counts.get("SETTLED_TO_BASELINE_DIFF", 0)),
        "exit_stop_loss_count": int(exit_counts.get("STOP_LOSS_RUPEES", 0)),
        "exit_hard_exit_count": int(exit_counts.get("HARD_EXIT_BARS", 0)),
        "exit_no_overnight_count": int(exit_counts.get("NO_OVERNIGHT_EXIT", 0)),
        "median_bars_to_settle": float(settled["bars_to_settle"].median()) if not settled.empty else np.nan,
        "p90_bars_to_settle": float(settled["bars_to_settle"].quantile(0.90)) if not settled.empty else np.nan,
        "max_bars_to_settle": float(settled["bars_to_settle"].max()) if not settled.empty else np.nan,
        "net_total_pnl_rupees": float(pnl.sum()),
        "avg_net_pnl_per_event": float(pnl.mean()),
        "median_net_pnl_per_event": float(pnl.median()),
        "win_count_net": int((pnl > 0).sum()),
        "loss_count_net": int((pnl <= 0).sum()),
        "win_rate_net_pct": safe_percent(int((pnl > 0).sum()), len(events)),
        "profit_factor_net": profit_factor(pnl),
        "avg_max_loss_abs_rupees": float(events["max_loss_abs_rupees"].mean()),
        "median_max_loss_abs_rupees": float(events["max_loss_abs_rupees"].median()),
        "p90_max_loss_abs_rupees": float(events["max_loss_abs_rupees"].quantile(0.90)),
        "max_loss_abs_rupees_worst_case": float(events["max_loss_abs_rupees"].max()),
        "settle_diff_points": SETTLE_DIFF_POINTS,
        "hard_exit_bars": HARD_EXIT_BARS,
        "stop_loss_rupees": STOP_LOSS_RUPEES,
        "qty_mode": QTY_MODE,
        "pair_qty": PAIR_QTY if QTY_MODE == "EQUAL_QTY" else np.nan,
        "base_notional_rupees": BASE_NOTIONAL_RUPEES if QTY_MODE == "NOTIONAL_BALANCED" else np.nan,
        "cost_per_trade_rupees": COST_PER_TRADE_RUPEES,
    }])


def build_exit_reason_summary(events: pd.DataFrame) -> pd.DataFrame:
    """Summarize PnL by exit reason."""
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


def build_daily_counts(events: pd.DataFrame) -> pd.DataFrame:
    """Count events by entry date."""
    if events.empty:
        return pd.DataFrame(columns=["entry_date", "events"])
    return events.groupby("entry_date", as_index=False).size().rename(columns={"size": "events"})


def build_by_side_summary(events: pd.DataFrame) -> pd.DataFrame:
    """Summarize results by rich/cheap side."""
    if events.empty:
        return pd.DataFrame()
    rows = []
    for side, g in events.groupby("side"):
        pnl = g["net_exit_pnl_rupees"].astype(float)
        rows.append({
            "side": side,
            "events": int(len(g)),
            "settled_count": int(g["settled"].sum()),
            "settlement_rate_pct": safe_percent(int(g["settled"].sum()), len(g)),
            "net_total_pnl_rupees": float(pnl.sum()),
            "avg_net_pnl_per_event": float(pnl.mean()),
            "median_net_pnl_per_event": float(pnl.median()),
            "win_rate_net_pct": safe_percent(int((pnl > 0).sum()), len(g)),
            "profit_factor_net": profit_factor(pnl),
            "p90_max_loss_abs_rupees": float(g["max_loss_abs_rupees"].quantile(0.90)),
        })
    return pd.DataFrame(rows)


def build_holding_bucket_summary(events: pd.DataFrame) -> pd.DataFrame:
    """Summarize results by holding-time bucket."""
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


def make_config_df(start_d: date, end_d: date, aligned: pd.DataFrame) -> pd.DataFrame:
    """Create reproducibility config sheet."""
    rows = [
        ("strategy", "previous_day_average_raw_price_difference"),
        ("start_date", str(start_d)),
        ("end_date", str(end_d)),
        ("exchange", EXCHANGE),
        ("denominator_symbol", DENOMINATOR_SYMBOL),
        ("numerator_symbol", NUMERATOR_SYMBOL),
        ("difference_definition", f"{NUMERATOR_LABEL} - {DENOMINATOR_LABEL}"),
        ("aligned_rows", len(aligned)),
        ("trading_days", aligned["trading_date"].nunique()),
        ("baseline_mode", PREV_DAY_BASELINE_MODE),
        ("diff_thresholds_points", ",".join(str(x) for x in DIFF_THRESHOLDS_POINTS)),
        ("settle_diff_points", SETTLE_DIFF_POINTS),
        ("hard_exit_bars", HARD_EXIT_BARS),
        ("stop_loss_rupees", STOP_LOSS_RUPEES),
        ("no_overnight", NO_OVERNIGHT),
        ("force_exit_time", FORCE_EXIT_TIME.isoformat(timespec="minutes")),
        ("qty_mode", QTY_MODE),
        ("pair_qty", PAIR_QTY),
        ("base_notional_rupees", BASE_NOTIONAL_RUPEES),
        ("denominator_qty_fixed", DENOMINATOR_QTY_FIXED),
        ("numerator_qty_fixed", NUMERATOR_QTY_FIXED),
        ("qty_round_step", QTY_ROUND_STEP),
        ("cost_per_trade_rupees", COST_PER_TRADE_RUPEES),
        ("enable_entry_time_filter", ENABLE_ENTRY_TIME_FILTER),
        ("entry_start_time", ENTRY_START_TIME.isoformat(timespec="minutes")),
        ("last_entry_time", LAST_ENTRY_TIME.isoformat(timespec="minutes")),
        ("note", "Raw difference works cleanest with equal quantities; futures-lot execution may distort results."),
    ]
    return pd.DataFrame(rows, columns=["parameter", "value"])


def write_threshold_report(threshold_points: float, events: pd.DataFrame, summary: pd.DataFrame, config_df: pd.DataFrame) -> str:
    """Write Excel and CSV for one difference threshold."""
    label = str(threshold_points).replace(".", "_").rstrip("0").rstrip("_")
    threshold_dir = os.path.join(OUTPUT_DIR, f"diff_ge_{label}")
    os.makedirs(threshold_dir, exist_ok=True)

    base_name = f"{NUMERATOR_LABEL.lower()}_{DENOMINATOR_LABEL.lower()}_diff_ge_{label}"
    xlsx_path = os.path.join(threshold_dir, f"{base_name}.xlsx")
    csv_path = os.path.join(threshold_dir, f"{base_name}_events.csv")

    preferred_cols = [
        "event_id", "threshold_points", "entry_time", "side",
        "entry_diff", "entry_baseline_diff", "entry_deviation_points", "entry_abs_deviation_points",
        "settled", "settle_time", "exit_time", "exit_reason",
        "exit_diff", "exit_deviation_points", "bars_to_settle", "bars_to_exit",
        "entry_denom_close", "entry_numer_close", "exit_denom_close", "exit_numer_close",
        "denom_qty", "numer_qty", "denom_entry_notional", "numer_entry_notional",
        "max_loss_abs_rupees", "max_loss_rupees", "max_loss_time",
        "max_profit_rupees", "max_profit_time",
        "gross_exit_pnl_rupees", "cost_rupees", "net_exit_pnl_rupees",
        "first_positive_pnl_time", "first_positive_pnl_bars",
        "max_abs_deviation_during_wait_points", "directional_worst_deviation_points",
        "denom_points_at_exit", "numer_points_at_exit", "qty_mode", "pair_qty",
    ]
    if events.empty:
        events_out = events.copy()
    else:
        other_cols = [c for c in events.columns if c not in preferred_cols]
        events_out = events[preferred_cols + other_cols].copy()

    events_out.to_csv(csv_path, index=False)

    exit_reason_summary = build_exit_reason_summary(events)
    daily_counts = build_daily_counts(events)
    by_side = build_by_side_summary(events)
    holding_buckets = build_holding_bucket_summary(events)

    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        summary.to_excel(writer, sheet_name="summary", index=False)
        autosize_excel_columns(writer, "summary", summary)

        events_out.to_excel(writer, sheet_name="events", index=False)
        autosize_excel_columns(writer, "events", events_out)

        exit_reason_summary.to_excel(writer, sheet_name="exit_reason_summary", index=False)
        autosize_excel_columns(writer, "exit_reason_summary", exit_reason_summary)

        daily_counts.to_excel(writer, sheet_name="daily_counts", index=False)
        autosize_excel_columns(writer, "daily_counts", daily_counts)

        by_side.to_excel(writer, sheet_name="by_side", index=False)
        autosize_excel_columns(writer, "by_side", by_side)

        holding_buckets.to_excel(writer, sheet_name="holding_buckets", index=False)
        autosize_excel_columns(writer, "holding_buckets", holding_buckets)

        config_df.to_excel(writer, sheet_name="config", index=False)
        autosize_excel_columns(writer, "config", config_df)

    print(f"[DONE] threshold={threshold_points} points events={len(events)} -> {xlsx_path}")
    return xlsx_path


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    """Run the complete raw price-difference backtest."""
    pair_name = f"{NUMERATOR_LABEL}-{DENOMINATOR_LABEL}"
    print("============================================================")
    print(f"{pair_name} raw price-difference backtester")
    print("============================================================")

    paths = ensure_dirs()
    start_d, end_d = get_date_range()

    print(f"[CONFIG] Date range       : {start_d} to {end_d}")
    print(f"[CONFIG] Output dir       : {OUTPUT_DIR}")
    print(f"[CONFIG] Difference       : {NUMERATOR_LABEL} - {DENOMINATOR_LABEL}")
    print(f"[CONFIG] Thresholds       : {DIFF_THRESHOLDS_POINTS} points")
    print(f"[CONFIG] Settle threshold : {SETTLE_DIFF_POINTS} points")
    print(f"[CONFIG] Hard exit bars   : {HARD_EXIT_BARS}")
    print(f"[CONFIG] Stop loss        : Rs {STOP_LOSS_RUPEES}")
    print(f"[CONFIG] Qty mode         : {QTY_MODE}")
    print(f"[CONFIG] FORCE_DOWNLOAD   : {FORCE_DOWNLOAD}")

    print("\n[STEP] Initializing Kite API ...")
    kite = oUtils.intialize_kite_api()
    print("[INFO] Kite API initialized.")

    instruments_cache: Dict[str, List[Dict]] = {}

    print(f"\n[STEP] Loading/downloading {DENOMINATOR_LABEL} and {NUMERATOR_LABEL} 1-min candles ...")
    denom_df = load_or_download_equity(kite, DENOMINATOR_SPEC, start_d, end_d, paths, instruments_cache)
    numer_df = load_or_download_equity(kite, NUMERATOR_SPEC, start_d, end_d, paths, instruments_cache)

    print(f"\n[STEP] Aligning {DENOMINATOR_LABEL} and {NUMERATOR_LABEL} candles ...")
    aligned = align_pair(denom_df, numer_df)
    if aligned.empty:
        raise RuntimeError(f"No common timestamps for {pair_name}")

    print("[STEP] Calculating raw difference and previous-day average difference baseline ...")
    enriched = add_previous_day_difference_baseline(aligned)
    trading_days = int(enriched["trading_date"].nunique())
    print(f"[INFO] Aligned rows={len(enriched):,}; trading days={trading_days:,}")

    aligned_path = os.path.join(OUTPUT_DIR, f"{NUMERATOR_LABEL.lower()}_{DENOMINATOR_LABEL.lower()}_difference_aligned_1min.pkl")
    enriched.to_pickle(aligned_path)
    if os.environ.get("SAVE_ALIGNED_CSV", "0").strip().lower() in {"1", "true", "yes", "y"}:
        enriched.to_csv(aligned_path.replace(".pkl", ".csv"), index=False)
    print(f"[DONE] Saved enriched dataset: {aligned_path}")

    config_df = make_config_df(start_d, end_d, enriched)

    all_summaries: List[pd.DataFrame] = []
    files: List[Dict] = []

    print("\n[STEP] Building threshold reports ...")
    for threshold in DIFF_THRESHOLDS_POINTS:
        events = build_events_for_threshold(enriched, threshold_points=threshold)
        summary = summarize_events(events, threshold_points=threshold, trading_days=trading_days)
        all_summaries.append(summary)
        path = write_threshold_report(threshold, events, summary, config_df)
        files.append({"threshold_points": threshold, "events": len(events), "file": path})

    combined_summary = pd.concat(all_summaries, ignore_index=True) if all_summaries else pd.DataFrame()
    files_df = pd.DataFrame(files)

    combined_path = os.path.join(OUTPUT_DIR, f"{NUMERATOR_LABEL.lower()}_{DENOMINATOR_LABEL.lower()}_combined_difference_summary.xlsx")
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
            "threshold_points", "total_events", "events_per_trading_day",
            "settlement_rate_pct", "exit_stop_loss_count", "exit_hard_exit_count", "exit_no_overnight_count",
            "net_total_pnl_rupees", "avg_net_pnl_per_event", "win_rate_net_pct", "profit_factor_net",
            "max_loss_abs_rupees_worst_case",
        ]
        existing_cols = [c for c in cols if c in combined_summary.columns]
        print(combined_summary[existing_cols].to_string(index=False))
    print("-------------------------------------------------------")
    print(f"Combined summary: {combined_path}")
    print(f"Output directory : {OUTPUT_DIR}")
    print("=======================================================")


if __name__ == "__main__":
    main()
