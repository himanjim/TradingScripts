#!/usr/bin/env python3
"""
NiftySensexPrevDayRatioBacktester_RECALIBRATED.py
=================================================

Purpose
-------
Backtest the user's **previous-day SENSEX/NIFTY ratio deviation** idea with
more realistic trading controls:

    1. A trade is NOT allowed to continue to the next trading day.
    2. A trade must settle within a configurable number of 1-minute bars.
       Default: HARD_TIME_STOP_BARS = 100.
    3. A trade is force-exited if interim futures-proxy loss reaches a
       configurable maximum loss.
       Default: MAX_LOSS_RUPEES = 5000.
    4. After a forced exit due to loss, bar limit, or no-overnight exit,
       the baseline ratio is recalculated from the last configurable number
       of bars and the scanner tries again using that recalibrated baseline
       for the rest of that trading day.
       Default: REBASE_LOOKBACK_BARS = 375, i.e. 6.25 hours of 1-minute bars.

This script is deliberately separate from the z-score scripts.
It tests the simpler ratio idea:

    ratio = SENSEX / NIFTY

Normal baseline:

    prev_day_avg_ratio = average(SENSEX / NIFTY) on previous trading day

Live deviation at any row:

    deviation_pct = ((current_ratio / active_baseline_ratio) - 1) * 100

Where active_baseline_ratio is normally previous-day average, but after a
forced exit it may be recalibrated from the most recent intraday bars.

Trade hypothesis
----------------
If deviation_pct is positive and large:
    SENSEX is rich versus NIFTY.
    Virtual futures trade = short SENSEX, long NIFTY.

If deviation_pct is negative and large:
    SENSEX is cheap versus NIFTY.
    Virtual futures trade = long SENSEX, short NIFTY.

Exit logic
----------
For each event, baseline is frozen at entry. A trade exits on the first of:

    1. abs(deviation from ENTRY baseline) <= SETTLE_DEVIATION_PCT
    2. current PnL <= -MAX_LOSS_RUPEES
    3. bars held >= HARD_TIME_STOP_BARS
    4. current row reaches FORCE_EXIT_TIME / last same-day bar, so no overnight
    5. global MAX_LOOKAHEAD_BARS is reached, if still applicable

Important point
---------------
This is still a **backtest / event-study** using spot index close values as a
proxy for futures prices. For live-grade validation, use actual NIFTY/SENSEX
futures or option premium data with bid/ask and slippage.

Data source
-----------
This script DOES NOT download data. It reuses existing downloaded 1-minute data.
It tries, in order:

    1. ALIGNED_PATH env var, if supplied.
    2. ./nifty_sensex_4y_deviation_output/nifty_sensex_aligned_1min.pkl
    3. ./nifty_sensex_4y_deviation_output_z225/nifty_sensex_aligned_1min.pkl
    4. ./nifty_sensex_4y_deviation_output_z375/nifty_sensex_aligned_1min.pkl
    5. ./nifty_sensex_4y_deviation_output_z50/nifty_sensex_aligned_1min.pkl
    6. candle pickle pair under common output folders.

Expected aligned columns:
    date, nifty_close, sensex_close

Install dependencies:
    pip install pandas numpy openpyxl

Typical Windows CMD run
-----------------------
    set THRESHOLDS_PCT=0.03,0.05
    set SETTLE_DEVIATION_PCT=0.01
    set HARD_TIME_STOP_BARS=100
    set MAX_LOSS_RUPEES=5000
    set REBASE_LOOKBACK_BARS=375
    set ENABLE_ENTRY_TIME_FILTER=1
    set ENTRY_START_TIME=09:30
    set LAST_ENTRY_TIME=14:30
    python NiftySensexPrevDayRatioBacktester_RECALIBRATED.py

Output
------
Default output folder:
    ./nifty_sensex_prevday_ratio_recalibrated_output

For each threshold, an Excel and CSV are created. Reports include:
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

import os
from dataclasses import dataclass
from datetime import date, time as dtime
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


# =============================================================================
# CONFIGURATION
# =============================================================================

SESSION_START = dtime(9, 15, 0)
SESSION_END = dtime(15, 30, 0)

OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "./nifty_sensex_prevday_ratio_recalibrated_output")

# Optional explicit aligned data file.
ALIGNED_PATH_ENV = os.environ.get("ALIGNED_PATH", "").strip()

# Optional explicit candle paths.
NIFTY_CANDLES_PATH_ENV = os.environ.get("NIFTY_CANDLES_PATH", "").strip()
SENSEX_CANDLES_PATH_ENV = os.environ.get("SENSEX_CANDLES_PATH", "").strip()

# Previous-day baseline mode. MEAN follows your original idea. MEDIAN is optional.
PREV_DAY_BASELINE_MODE = os.environ.get("PREV_DAY_BASELINE_MODE", "MEAN").strip().upper()

# Thresholds are percentage deviations, not fractions.
# Example: 0.03 means 0.03%, not 3%.
THRESHOLDS_PCT = [
    float(x.strip())
    for x in os.environ.get("THRESHOLDS_PCT", "0.03,0.05,0.07,0.10").split(",")
    if x.strip()
]

# Settlement near the active baseline frozen at entry.
# Example: 0.01 means +/-0.01% from the baseline ratio.
SETTLE_DEVIATION_PCT = float(os.environ.get("SETTLE_DEVIATION_PCT", "0.01"))

# Large diagnostic cap. In this revised version, HARD_TIME_STOP_BARS and
# no-overnight logic usually exit earlier.
MAX_WAIT_TRADING_DAYS = int(os.environ.get("MAX_WAIT_TRADING_DAYS", "10"))
INTRADAY_BARS_PER_DAY = int(os.environ.get("INTRADAY_BARS_PER_DAY", "375"))
MAX_LOOKAHEAD_BARS = int(os.environ.get("MAX_LOOKAHEAD_BARS", str(MAX_WAIT_TRADING_DAYS * INTRADAY_BARS_PER_DAY)))

# Trading-style controls requested by user.
# 100 bars is a practical starting value based on earlier findings that long
# unresolved trades are the weak part of this strategy.
HARD_TIME_STOP_BARS = int(os.environ.get("HARD_TIME_STOP_BARS", "100"))
MAX_LOSS_RUPEES = float(os.environ.get("MAX_LOSS_RUPEES", "5000"))

# No overnight carry. If enabled, exit at FORCE_EXIT_TIME if reached, otherwise
# at the last available row for the entry trading date.
NO_OVERNIGHT = os.environ.get("NO_OVERNIGHT", "1").strip().lower() in {"1", "true", "yes", "y"}
FORCE_EXIT_TIME = dtime.fromisoformat(os.environ.get("FORCE_EXIT_TIME", "15:20"))

# After a forced exit, recalculate baseline from the last 6.25 hours by default.
# On 1-minute bars, 6.25 hours = 375 bars.
ENABLE_REBASE_AFTER_FORCED_EXIT = os.environ.get("ENABLE_REBASE_AFTER_FORCED_EXIT", "1").strip().lower() in {
    "1", "true", "yes", "y"
}
REBASE_LOOKBACK_BARS = int(os.environ.get("REBASE_LOOKBACK_BARS", "375"))
REBASE_MIN_BARS = int(os.environ.get("REBASE_MIN_BARS", "50"))

# Which exits trigger intraday baseline recalibration.
REBASE_AFTER_EXIT_REASONS = {
    x.strip().upper()
    for x in os.environ.get("REBASE_AFTER_EXIT_REASONS", "MAX_LOSS_STOP,HARD_TIME_STOP,NO_OVERNIGHT_EXIT").split(",")
    if x.strip()
}

# Fixed futures-like quantities from your strategy discussions.
NIFTY_QTY = int(os.environ.get("NIFTY_QTY", "325"))
SENSEX_QTY = int(os.environ.get("SENSEX_QTY", "100"))

# Optional flat cost per complete virtual pair trade.
COST_PER_TRADE_RUPEES = float(os.environ.get("COST_PER_TRADE_RUPEES", "0"))

# Non-overlap means one unresolved deviation episode is counted once.
SKIP_OVERLAPPING_EVENTS = os.environ.get("SKIP_OVERLAPPING_EVENTS", "1").strip().lower() in {
    "1", "true", "yes", "y"
}

# Optional entry time filter. ON by default now because the user is moving toward
# a trading-style test rather than a pure diagnostic.
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
    Holds the active baseline used for scanning.

    Normally baseline_source is PREV_DAY and baseline_value is the previous
    trading day's average ratio. After a forced exit, baseline_source becomes
    RECALIBRATED and baseline_value is the average ratio over the last
    REBASE_LOOKBACK_BARS bars.

    The recalibrated baseline is valid only for the same trading day. It resets
    automatically on the next trading day.
    """

    source: str = "PREV_DAY"
    value: float = np.nan
    set_time: Optional[pd.Timestamp] = None
    set_index: Optional[int] = None
    valid_trading_date: Optional[date] = None
    lookback_bars_used: int = 0


# =============================================================================
# DATA LOADING
# =============================================================================

def ensure_output_dir() -> None:
    """Create output directory if it does not exist."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def normalize_dt(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize date column to timezone-naive pandas datetime sorted by date."""
    out = df.copy()
    if "date" not in out.columns:
        raise ValueError("Input data must contain a 'date' column.")
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.dropna(subset=["date"]).sort_values("date").drop_duplicates(subset=["date"], keep="last")
    return out.reset_index(drop=True)


def load_pickle_or_csv(path: str) -> pd.DataFrame:
    """Load a DataFrame from .pkl/.pickle or .csv path."""
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


def is_aligned_df(df: pd.DataFrame) -> bool:
    """Return True if DataFrame already contains aligned NIFTY/SENSEX closes."""
    cols = {c.lower() for c in df.columns}
    return {"date", "nifty_close", "sensex_close"}.issubset(cols)


def standardize_aligned_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize aligned DataFrame column names and required fields."""
    out = df.copy()
    lower_map = {c.lower(): c for c in out.columns}
    out = out.rename(
        columns={
            lower_map["date"]: "date",
            lower_map["nifty_close"]: "nifty_close",
            lower_map["sensex_close"]: "sensex_close",
        }
    )

    out = normalize_dt(out)
    out["nifty_close"] = pd.to_numeric(out["nifty_close"], errors="coerce")
    out["sensex_close"] = pd.to_numeric(out["sensex_close"], errors="coerce")
    out = out.dropna(subset=["nifty_close", "sensex_close"])
    out = out[(out["nifty_close"] > 0) & (out["sensex_close"] > 0)].copy()

    # Keep normal session rows only. This protects the code if input data has
    # any pre/post-market rows from a vendor.
    t = out["date"].dt.time
    out = out[(t >= SESSION_START) & (t <= SESSION_END)].copy()

    out["trading_date"] = out["date"].dt.date
    return out.reset_index(drop=True)


def align_from_candles(nifty_path: str, sensex_path: str) -> pd.DataFrame:
    """Load separate candle files and align NIFTY/SENSEX close by timestamp."""
    nifty = normalize_dt(load_pickle_or_csv(nifty_path))
    sensex = normalize_dt(load_pickle_or_csv(sensex_path))

    if "close" not in nifty.columns or "close" not in sensex.columns:
        raise ValueError("Separate candle files must contain columns: date, close")

    n = nifty[["date", "close"]].rename(columns={"close": "nifty_close"})
    s = sensex[["date", "close"]].rename(columns={"close": "sensex_close"})
    aligned = pd.merge(n, s, on="date", how="inner")
    return standardize_aligned_columns(aligned)


def find_existing_data() -> Tuple[pd.DataFrame, DataSourceInfo]:
    """Locate and load existing 4-year NIFTY/SENSEX data."""
    if ALIGNED_PATH_ENV:
        df = load_pickle_or_csv(ALIGNED_PATH_ENV)
        if not is_aligned_df(df):
            raise ValueError(f"ALIGNED_PATH does not contain aligned columns: {ALIGNED_PATH_ENV}")
        return standardize_aligned_columns(df), DataSourceInfo("explicit_aligned", ALIGNED_PATH_ENV)

    candidate_aligned_paths = [
        "./nifty_sensex_4y_deviation_output/nifty_sensex_aligned_1min.pkl",
        "./nifty_sensex_4y_deviation_output_z225/nifty_sensex_aligned_1min.pkl",
        "./nifty_sensex_4y_deviation_output_z375/nifty_sensex_aligned_1min.pkl",
        "./nifty_sensex_4y_deviation_output_z50/nifty_sensex_aligned_1min.pkl",
    ]
    for p in candidate_aligned_paths:
        if os.path.exists(p):
            df = load_pickle_or_csv(p)
            if is_aligned_df(df):
                return standardize_aligned_columns(df), DataSourceInfo("auto_aligned", p)

    if NIFTY_CANDLES_PATH_ENV and SENSEX_CANDLES_PATH_ENV:
        aligned = align_from_candles(NIFTY_CANDLES_PATH_ENV, SENSEX_CANDLES_PATH_ENV)
        return aligned, DataSourceInfo("explicit_candles", NIFTY_CANDLES_PATH_ENV, SENSEX_CANDLES_PATH_ENV)

    candidate_candle_pairs = [
        (
            "./nifty_sensex_4y_deviation_output/candles/nifty_1min.pkl",
            "./nifty_sensex_4y_deviation_output/candles/sensex_1min.pkl",
        ),
        (
            "./nifty_sensex_4y_deviation_output_z225/candles/nifty_1min.pkl",
            "./nifty_sensex_4y_deviation_output_z225/candles/sensex_1min.pkl",
        ),
        (
            "./nifty_sensex_4y_deviation_output_z375/candles/nifty_1min.pkl",
            "./nifty_sensex_4y_deviation_output_z375/candles/sensex_1min.pkl",
        ),
        (
            "./nifty_sensex_4y_deviation_output_z50/candles/nifty_1min.pkl",
            "./nifty_sensex_4y_deviation_output_z50/candles/sensex_1min.pkl",
        ),
    ]
    for n_path, s_path in candidate_candle_pairs:
        if os.path.exists(n_path) and os.path.exists(s_path):
            aligned = align_from_candles(n_path, s_path)
            return aligned, DataSourceInfo("auto_candles", n_path, s_path)

    raise FileNotFoundError(
        "Could not find existing aligned/candle files. Set ALIGNED_PATH or both "
        "NIFTY_CANDLES_PATH and SENSEX_CANDLES_PATH."
    )


# =============================================================================
# RATIO / BASELINE CALCULATION
# =============================================================================

def add_prev_day_ratio_baseline(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add ratio, previous-day baseline, and prev-day deviation_pct.

    This baseline is used initially. During event scanning, it can be replaced
    intraday by recalibrated baselines after forced exits.
    """
    out = df.copy().sort_values("date").reset_index(drop=True)
    out["ratio"] = out["sensex_close"] / out["nifty_close"]
    out["raw_difference"] = out["sensex_close"] - out["nifty_close"]

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
    """
    Recalculate baseline from the last REBASE_LOOKBACK_BARS of the same day.

    The logic uses only data available up to and including the forced-exit bar.
    It deliberately avoids using future data.
    """
    # Collect same-day bars up to exit_i.
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
    """
    Return active baseline value and metadata for the current row.

    Recalibrated baseline is valid only for the same trading date. Otherwise,
    fall back to that row's previous-day baseline.
    """
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
# PNL LOGIC
# =============================================================================

def infer_side(entry_deviation_pct: float) -> str:
    """Map deviation sign to virtual pair-trade direction."""
    if entry_deviation_pct > 0:
        return "SENSEX_RICH_SHORT_SENSEX_LONG_NIFTY"
    return "SENSEX_CHEAP_LONG_SENSEX_SHORT_NIFTY"


def compute_pair_pnl_path(
    side: str,
    entry_nifty: float,
    entry_sensex: float,
    path_nifty: np.ndarray,
    path_sensex: np.ndarray,
) -> np.ndarray:
    """Compute futures-like rupee PnL path using spot index closes as proxy."""
    if side == "SENSEX_RICH_SHORT_SENSEX_LONG_NIFTY":
        return ((entry_sensex - path_sensex) * SENSEX_QTY) + ((path_nifty - entry_nifty) * NIFTY_QTY)

    if side == "SENSEX_CHEAP_LONG_SENSEX_SHORT_NIFTY":
        return ((path_sensex - entry_sensex) * SENSEX_QTY) + ((entry_nifty - path_nifty) * NIFTY_QTY)

    raise ValueError(f"Unknown side: {side}")


def current_pair_pnl(
    side: str,
    entry_nifty: float,
    entry_sensex: float,
    current_nifty: float,
    current_sensex: float,
) -> float:
    """Compute current PnL for one bar without rebuilding a whole path."""
    if side == "SENSEX_RICH_SHORT_SENSEX_LONG_NIFTY":
        return ((entry_sensex - current_sensex) * SENSEX_QTY) + ((current_nifty - entry_nifty) * NIFTY_QTY)

    if side == "SENSEX_CHEAP_LONG_SENSEX_SHORT_NIFTY":
        return ((current_sensex - entry_sensex) * SENSEX_QTY) + ((entry_nifty - current_nifty) * NIFTY_QTY)

    raise ValueError(f"Unknown side: {side}")


# =============================================================================
# EVENT BUILDING
# =============================================================================

def is_time_allowed(ts: pd.Timestamp) -> bool:
    """Return True if entry time passes optional time filter."""
    if not ENABLE_ENTRY_TIME_FILTER:
        return True
    t = ts.time()
    return ENTRY_START_TIME <= t <= LAST_ENTRY_TIME


def last_allowed_same_day_index(dates: np.ndarray, trading_dates: np.ndarray, entry_i: int) -> int:
    """
    Return the last index to which a trade may be held without overnight carry.

    If FORCE_EXIT_TIME exists in the same day, use the last bar at or before that
    time. Otherwise, use the last available same-day bar.
    """
    entry_day = trading_dates[entry_i]
    j = entry_i
    last_same_day = entry_i
    last_before_force_time = None

    while j < len(dates) and trading_dates[j] == entry_day:
        last_same_day = j
        if pd.Timestamp(dates[j]).time() <= FORCE_EXIT_TIME:
            last_before_force_time = j
        j += 1

    return last_before_force_time if last_before_force_time is not None else last_same_day


def build_events_for_threshold(df: pd.DataFrame, threshold_pct: float) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build non-overlapping event rows for one deviation threshold.

    Entry condition:
        abs(deviation_from_active_baseline[i]) >= threshold_pct
        and abs(deviation_from_active_baseline[i-1]) < threshold_pct

    The active baseline is usually previous-day average ratio. After forced
    exits, it can be recalibrated from the last REBASE_LOOKBACK_BARS bars.

    Exit condition is first of:
        - SETTLED_TO_ENTRY_BASELINE
        - MAX_LOSS_STOP
        - HARD_TIME_STOP
        - NO_OVERNIGHT_EXIT
        - FORCED_MAX_WAIT_EXIT
    """
    required_cols = [
        "date", "trading_date", "nifty_close", "sensex_close", "ratio", "prev_day_avg_ratio"
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    work = df.dropna(subset=["ratio", "prev_day_avg_ratio"]).copy().reset_index(drop=True)

    dates = work["date"].to_numpy()
    trading_dates = work["trading_date"].to_numpy()
    nifty = work["nifty_close"].to_numpy(dtype=float)
    sensex = work["sensex_close"].to_numpy(dtype=float)
    ratio = work["ratio"].to_numpy(dtype=float)
    prev_day_baseline = work["prev_day_avg_ratio"].to_numpy(dtype=float)

    rows: List[Dict] = []
    rebase_rows: List[Dict] = []

    n = len(work)
    i = 1
    event_id = 0

    # Recalibrated baseline state. Resets implicitly when trading day changes.
    baseline_state = BaselineState()

    while i < n:
        current_day = trading_dates[i]

        baseline_i, baseline_source_i, baseline_set_time_i, baseline_lb_i = get_active_baseline_for_index(
            i, baseline_state, trading_dates, prev_day_baseline
        )
        baseline_prev, _, _, _ = get_active_baseline_for_index(
            i - 1, baseline_state, trading_dates, prev_day_baseline
        )

        dev_i = deviation_pct_from_baseline(float(ratio[i]), baseline_i)
        # For crossing detection, compare previous bar using the same current
        # active baseline. This avoids false non-crossing immediately after a
        # recalibration changes the baseline.
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
        entry_baseline_source = baseline_source_i
        entry_baseline_set_time = baseline_set_time_i
        entry_baseline_lookback_bars = baseline_lb_i
        entry_ratio = float(ratio[entry_i])
        entry_dev = deviation_pct_from_baseline(entry_ratio, entry_baseline)
        entry_abs_dev = abs(entry_dev)
        side = infer_side(entry_dev)

        max_j = min(n - 1, entry_i + MAX_LOOKAHEAD_BARS)

        if HARD_TIME_STOP_BARS > 0:
            max_j = min(max_j, entry_i + HARD_TIME_STOP_BARS)

        same_day_limit_j = last_allowed_same_day_index(dates, trading_dates, entry_i) if NO_OVERNIGHT else n - 1
        max_j = min(max_j, same_day_limit_j)

        exit_j: Optional[int] = None
        settle_j: Optional[int] = None
        exit_reason = "FORCED_MAX_WAIT_EXIT"

        entry_nifty = float(nifty[entry_i])
        entry_sensex = float(sensex[entry_i])

        for j in range(entry_i + 1, max_j + 1):
            dev_j = deviation_pct_from_baseline(float(ratio[j]), entry_baseline)
            pnl_j = current_pair_pnl(side, entry_nifty, entry_sensex, float(nifty[j]), float(sensex[j]))
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
        path_nifty = nifty[path_slice]
        path_sensex = sensex[path_slice]
        path_ratio = ratio[path_slice]
        path_dates = dates[path_slice]

        path_dev_from_entry_baseline = ((path_ratio / entry_baseline) - 1.0) * 100.0
        path_abs_dev_from_entry_baseline = np.abs(path_dev_from_entry_baseline)

        pnl_path = compute_pair_pnl_path(
            side=side,
            entry_nifty=entry_nifty,
            entry_sensex=entry_sensex,
            path_nifty=path_nifty,
            path_sensex=path_sensex,
        )

        gross_exit_pnl = float(pnl_path[-1])
        net_exit_pnl = gross_exit_pnl - COST_PER_TRADE_RUPEES

        min_pnl_idx = int(np.nanargmin(pnl_path))
        max_pnl_idx = int(np.nanargmax(pnl_path))
        max_loss_rupees = float(pnl_path[min_pnl_idx])
        max_profit_rupees = float(pnl_path[max_pnl_idx])
        max_loss_abs_rupees = abs(min(0.0, max_loss_rupees))

        max_abs_dev_idx = int(np.nanargmax(path_abs_dev_from_entry_baseline))
        max_abs_dev_value = float(path_abs_dev_from_entry_baseline[max_abs_dev_idx])

        if entry_dev > 0:
            directional_worst_dev = float(np.nanmax(path_dev_from_entry_baseline))
            directional_worst_idx = int(np.nanargmax(path_dev_from_entry_baseline))
        else:
            directional_worst_dev = float(np.nanmin(path_dev_from_entry_baseline))
            directional_worst_idx = int(np.nanargmin(path_dev_from_entry_baseline))

        positive_indices = np.where(pnl_path > 0)[0]
        first_positive_idx = int(positive_indices[0]) if len(positive_indices) else None

        bars_held = int(exit_j - entry_i)
        calendar_minutes_held = float((pd.Timestamp(dates[exit_j]) - pd.Timestamp(dates[entry_i])).total_seconds() / 60.0)

        rows.append(
            {
                "event_id": event_id,
                "threshold_pct": threshold_pct,
                "entry_time": entry_time,
                "entry_date": entry_time.date(),
                "entry_trading_date": trading_dates[entry_i],
                "side": side,
                "entry_ratio": entry_ratio,
                "entry_baseline_ratio": entry_baseline,
                "entry_baseline_source": entry_baseline_source,
                "entry_baseline_set_time": entry_baseline_set_time if entry_baseline_set_time is not None else pd.NaT,
                "entry_baseline_lookback_bars": entry_baseline_lookback_bars,
                "entry_deviation_pct": entry_dev,
                "entry_abs_deviation_pct": entry_abs_dev,
                "entry_nifty_close": entry_nifty,
                "entry_sensex_close": entry_sensex,
                "settled": bool(settle_j is not None),
                "settle_time": pd.Timestamp(dates[settle_j]) if settle_j is not None else pd.NaT,
                "exit_time": pd.Timestamp(dates[exit_j]),
                "exit_reason": exit_reason,
                "exit_ratio": float(ratio[exit_j]),
                "exit_deviation_from_entry_baseline_pct": float(path_dev_from_entry_baseline[-1]),
                "exit_abs_deviation_from_entry_baseline_pct": float(path_abs_dev_from_entry_baseline[-1]),
                "exit_nifty_close": float(nifty[exit_j]),
                "exit_sensex_close": float(sensex[exit_j]),
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
                "nifty_qty": NIFTY_QTY,
                "sensex_qty": SENSEX_QTY,
                "nifty_points_at_exit": float(nifty[exit_j] - nifty[entry_i]),
                "sensex_points_at_exit": float(sensex[exit_j] - sensex[entry_i]),
            }
        )

        # Recalibrate baseline after forced exit. Normal settled exits do not
        # trigger recalibration because the old baseline worked.
        if ENABLE_REBASE_AFTER_FORCED_EXIT and exit_reason.upper() in REBASE_AFTER_EXIT_REASONS:
            new_state = recalibrate_baseline(ratio, dates, trading_dates, exit_j, trading_dates[exit_j])
            if new_state is not None:
                baseline_state = new_state
                rebase_rows.append(
                    {
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
                    }
                )
            else:
                rebase_rows.append(
                    {
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
                    }
                )

        if SKIP_OVERLAPPING_EVENTS:
            i = exit_j + 1
        else:
            i += 1

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
        return pd.DataFrame([
            {
                "threshold_pct": threshold_pct,
                "total_events": 0,
                "trading_days": trading_days,
                "events_per_trading_day": 0.0,
            }
        ])

    settled = events[events["settled"] == True].copy()  # noqa: E712
    forced = events[events["settled"] == False].copy()  # noqa: E712
    pnl = events["net_exit_pnl_rupees"].astype(float)

    exit_reason_counts = events["exit_reason"].value_counts().to_dict()

    return pd.DataFrame([
        {
            "threshold_pct": threshold_pct,
            "total_events": int(len(events)),
            "trading_days": int(trading_days),
            "events_per_trading_day": float(len(events) / trading_days) if trading_days else np.nan,
            "settled_count": int(len(settled)),
            "not_settled_count": int(len(forced)),
            "settlement_rate_pct": safe_percent(len(settled), len(events)),
            "exit_settled_count": int(exit_reason_counts.get("SETTLED_TO_ENTRY_BASELINE", 0)),
            "exit_max_loss_count": int(exit_reason_counts.get("MAX_LOSS_STOP", 0)),
            "exit_hard_time_stop_count": int(exit_reason_counts.get("HARD_TIME_STOP", 0)),
            "exit_no_overnight_count": int(exit_reason_counts.get("NO_OVERNIGHT_EXIT", 0)),
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
            "settle_deviation_pct": SETTLE_DEVIATION_PCT,
            "hard_time_stop_bars": HARD_TIME_STOP_BARS,
            "max_loss_rupees_stop": MAX_LOSS_RUPEES,
            "no_overnight": NO_OVERNIGHT,
            "force_exit_time": FORCE_EXIT_TIME.isoformat(timespec="minutes"),
            "rebase_enabled": ENABLE_REBASE_AFTER_FORCED_EXIT,
            "rebase_lookback_bars": REBASE_LOOKBACK_BARS,
        }
    ])


def build_daily_counts(events: pd.DataFrame) -> pd.DataFrame:
    """Count events by entry date."""
    if events.empty:
        return pd.DataFrame(columns=["entry_date", "events"])
    return events.groupby("entry_date", as_index=False).size().rename(columns={"size": "events"})


def build_by_side_summary(events: pd.DataFrame) -> pd.DataFrame:
    """Summarize separately by rich/cheap side."""
    if events.empty:
        return pd.DataFrame()
    rows = []
    for side, g in events.groupby("side"):
        pnl = g["net_exit_pnl_rupees"].astype(float)
        settled_count = int(g["settled"].sum())
        rows.append(
            {
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
            }
        )
    return pd.DataFrame(rows)


def build_holding_bucket_summary(events: pd.DataFrame) -> pd.DataFrame:
    """Summarize PnL by holding-time bucket."""
    if events.empty:
        return pd.DataFrame()

    bins = [-1, 5, 15, 30, 60, 100, 120, 240, 375, 750, 999999]
    labels = ["<=5", "6-15", "16-30", "31-60", "61-100", "101-120", "121-240", "241-375", "376-750", ">750"]
    tmp = events.copy()
    tmp["holding_bucket"] = pd.cut(tmp["bars_to_exit"], bins=bins, labels=labels)

    rows = []
    for bucket, g in tmp.groupby("holding_bucket", observed=True):
        pnl = g["net_exit_pnl_rupees"].astype(float)
        rows.append(
            {
                "holding_bucket": str(bucket),
                "events": int(len(g)),
                "net_total_pnl_rupees": float(pnl.sum()),
                "avg_net_pnl_per_event": float(pnl.mean()),
                "median_net_pnl_per_event": float(pnl.median()),
                "win_rate_net_pct": safe_percent(int((pnl > 0).sum()), len(g)),
                "profit_factor_net": profit_factor(pnl),
                "worst_exit_pnl": float(pnl.min()),
                "worst_max_loss_abs_rupees": float(g["max_loss_abs_rupees"].max()),
            }
        )
    return pd.DataFrame(rows)


def build_exit_reason_summary(events: pd.DataFrame) -> pd.DataFrame:
    """Summarize events and PnL by exit reason."""
    if events.empty:
        return pd.DataFrame()

    rows = []
    for reason, g in events.groupby("exit_reason"):
        pnl = g["net_exit_pnl_rupees"].astype(float)
        rows.append(
            {
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
            }
        )
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
    """Create key-value config table for reproducibility."""
    rows = [
        ("strategy", "prev_day_avg_ratio_recalibrated_after_forced_exit"),
        ("data_source_mode", data_source.mode),
        ("data_source_path_1", data_source.path_1),
        ("data_source_path_2", data_source.path_2),
        ("aligned_rows", len(aligned)),
        ("first_timestamp", str(aligned["date"].min())),
        ("last_timestamp", str(aligned["date"].max())),
        ("trading_days", aligned["trading_date"].nunique()),
        ("baseline_mode", PREV_DAY_BASELINE_MODE),
        ("thresholds_pct", ",".join(str(x) for x in THRESHOLDS_PCT)),
        ("settle_deviation_pct", SETTLE_DEVIATION_PCT),
        ("max_wait_trading_days", MAX_WAIT_TRADING_DAYS),
        ("max_lookahead_bars", MAX_LOOKAHEAD_BARS),
        ("hard_time_stop_bars", HARD_TIME_STOP_BARS),
        ("max_loss_rupees_stop", MAX_LOSS_RUPEES),
        ("no_overnight", NO_OVERNIGHT),
        ("force_exit_time", FORCE_EXIT_TIME.isoformat(timespec="minutes")),
        ("enable_rebase_after_forced_exit", ENABLE_REBASE_AFTER_FORCED_EXIT),
        ("rebase_after_exit_reasons", ",".join(sorted(REBASE_AFTER_EXIT_REASONS))),
        ("rebase_lookback_bars", REBASE_LOOKBACK_BARS),
        ("rebase_min_bars", REBASE_MIN_BARS),
        ("nifty_qty", NIFTY_QTY),
        ("sensex_qty", SENSEX_QTY),
        ("cost_per_trade_rupees", COST_PER_TRADE_RUPEES),
        ("skip_overlapping_events", SKIP_OVERLAPPING_EVENTS),
        ("enable_entry_time_filter", ENABLE_ENTRY_TIME_FILTER),
        ("entry_start_time", ENTRY_START_TIME.isoformat(timespec="minutes")),
        ("last_entry_time", LAST_ENTRY_TIME.isoformat(timespec="minutes")),
        ("note", "Normal baseline is previous-day ratio; after forced exit, baseline may be recalibrated intraday."),
        ("pnl_note", "Index close levels are used as futures proxy."),
    ]
    return pd.DataFrame(rows, columns=["parameter", "value"])


def write_threshold_report(
    threshold_pct: float,
    events: pd.DataFrame,
    rebase_events: pd.DataFrame,
    summary: pd.DataFrame,
    config_df: pd.DataFrame,
) -> str:
    """Write one Excel report and one CSV event file for a threshold."""
    label = str(threshold_pct).replace(".", "_").rstrip("0").rstrip("_")
    threshold_dir = os.path.join(OUTPUT_DIR, f"ratio_dev_ge_{label}pct")
    os.makedirs(threshold_dir, exist_ok=True)

    xlsx_path = os.path.join(threshold_dir, f"nifty_sensex_ratio_dev_ge_{label}pct_recalibrated.xlsx")
    csv_path = os.path.join(threshold_dir, f"nifty_sensex_ratio_dev_ge_{label}pct_recalibrated_events.csv")
    rebase_csv_path = os.path.join(threshold_dir, f"nifty_sensex_ratio_dev_ge_{label}pct_recalibrations.csv")

    preferred_cols = [
        "event_id", "threshold_pct", "entry_time", "side",
        "entry_deviation_pct", "entry_abs_deviation_pct", "entry_ratio", "entry_baseline_ratio",
        "entry_baseline_source", "entry_baseline_set_time", "entry_baseline_lookback_bars",
        "settled", "settle_time", "exit_time", "exit_reason",
        "exit_deviation_from_entry_baseline_pct", "bars_to_settle", "bars_to_exit",
        "entry_nifty_close", "entry_sensex_close", "exit_nifty_close", "exit_sensex_close",
        "max_loss_abs_rupees", "max_loss_rupees", "max_loss_time", "max_profit_rupees", "max_profit_time",
        "gross_exit_pnl_rupees", "cost_rupees", "net_exit_pnl_rupees",
        "first_positive_pnl_time", "first_positive_pnl_bars",
        "max_abs_deviation_during_wait_pct", "directional_worst_deviation_pct",
        "nifty_points_at_exit", "sensex_points_at_exit", "nifty_qty", "sensex_qty",
    ]

    if events.empty:
        events_out = events.copy()
    else:
        other_cols = [c for c in events.columns if c not in preferred_cols]
        events_out = events[preferred_cols + other_cols].copy()

    events_out.to_csv(csv_path, index=False)
    rebase_events.to_csv(rebase_csv_path, index=False)

    daily_counts = build_daily_counts(events)
    by_side = build_by_side_summary(events)
    holding_buckets = build_holding_bucket_summary(events)
    exit_reason_summary = build_exit_reason_summary(events)

    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        summary.to_excel(writer, sheet_name="summary", index=False)
        autosize_excel_columns(writer, "summary", summary)

        events_out.to_excel(writer, sheet_name="events", index=False)
        autosize_excel_columns(writer, "events", events_out)

        exit_reason_summary.to_excel(writer, sheet_name="exit_reason_summary", index=False)
        autosize_excel_columns(writer, "exit_reason_summary", exit_reason_summary)

        rebase_events.to_excel(writer, sheet_name="baseline_recalibrations", index=False)
        autosize_excel_columns(writer, "baseline_recalibrations", rebase_events)

        daily_counts.to_excel(writer, sheet_name="daily_counts", index=False)
        autosize_excel_columns(writer, "daily_counts", daily_counts)

        by_side.to_excel(writer, sheet_name="by_side", index=False)
        autosize_excel_columns(writer, "by_side", by_side)

        holding_buckets.to_excel(writer, sheet_name="holding_buckets", index=False)
        autosize_excel_columns(writer, "holding_buckets", holding_buckets)

        config_df.to_excel(writer, sheet_name="config", index=False)
        autosize_excel_columns(writer, "config", config_df)

    print(f"[DONE] threshold={threshold_pct}% events={len(events)} -> {xlsx_path}")
    return xlsx_path


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    """Run the previous-day average ratio backtest with forced-exit rebasing."""
    print("============================================================")
    print("NIFTY-SENSEX previous-day ratio backtester with recalibration")
    print("============================================================")

    ensure_output_dir()

    print("[STEP] Loading existing NIFTY/SENSEX data ...")
    aligned, data_source = find_existing_data()
    if aligned.empty:
        raise RuntimeError("Aligned dataset is empty.")

    print(f"[INFO] Data source: {data_source.mode} {data_source.path_1} {data_source.path_2}")
    print(f"[INFO] Rows: {len(aligned):,}; trading days: {aligned['trading_date'].nunique():,}")
    print(f"[INFO] Range: {aligned['date'].min()} -> {aligned['date'].max()}")

    print("[STEP] Calculating ratio and previous-day average baseline ...")
    enriched = add_prev_day_ratio_baseline(aligned)

    enriched_path = os.path.join(OUTPUT_DIR, "nifty_sensex_prevday_ratio_enriched_recalibrated.pkl")
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

    combined_path = os.path.join(OUTPUT_DIR, "combined_prevday_ratio_recalibrated_summary.xlsx")
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
            "net_total_pnl_rupees", "win_rate_net_pct", "profit_factor_net", "max_loss_abs_rupees_worst_case",
        ]
        existing_cols = [c for c in cols if c in combined_summary.columns]
        print(combined_summary[existing_cols].to_string(index=False))
    print("-------------------------------------------------------")
    print(f"Combined summary: {combined_path}")
    print(f"Output directory : {OUTPUT_DIR}")
    print("=======================================================")


if __name__ == "__main__":
    main()
