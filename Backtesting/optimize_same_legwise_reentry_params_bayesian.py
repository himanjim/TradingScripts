"""
BAYESIAN PARAMETER OPTIMIZER FOR:
    same_legwise_reentry_same_premium_backtest.py

Purpose
-------
This script searches for the best parameters for the leg-wise same-premium
re-entry short option-pair strategy.

The strategy being optimized is the one implemented in:
    same_legwise_reentry_same_premium_backtest.py

It imports that backtester once, patches its global configuration for each
trial, and calls its own simulation functions. This avoids copying the trading
logic into the optimizer and keeps the optimizer aligned with the backtester.

Optimized parameters
--------------------
1. ENTRY_TIME_IST
2. OTM_DISTANCE_STEPS
3. LEG_PREMIUM_RISE_EXIT_PCT
4. MAX_RETRIES_PER_LEG
5. MIN_REENTRY_GAP_MINUTES
6. REENTRY_REQUIRE_RANGE_TOUCH

Primary objective
-----------------
Default objective is score_profit_accuracy, a composite score designed to
reward both:
    - total net profit
    - daily winning accuracy / win rate
while applying a moderate penalty for drawdown.

You can also directly optimize:
    total_net_pnl
    win_rate_daily_pct
    sharpe
    profit_factor
    max_drawdown_inverse
    score_balanced

Important practical point
-------------------------
This optimizer calls Kite only once to download underlying data, and preloads
option pickle groups once. Each Optuna trial then runs against the cached data.
This is much faster and cleaner than launching the full backtester script for
every parameter combination.

Requirements
------------
pip install optuna pandas numpy openpyxl python-dateutil pytz

Place this optimizer in the SAME folder as:
    same_legwise_reentry_same_premium_backtest.py

Usage examples
--------------
# Bayesian search, default 200 trials, default objective score_profit_accuracy
python optimize_same_legwise_reentry_params_bayesian.py

# More trials
python optimize_same_legwise_reentry_params_bayesian.py --trials 400

# Optimize raw profit only
python optimize_same_legwise_reentry_params_bayesian.py --objective total_net_pnl

# Optimize daily accuracy / win rate only
python optimize_same_legwise_reentry_params_bayesian.py --objective win_rate_daily_pct

# Grid search instead of Bayesian search
python optimize_same_legwise_reentry_params_bayesian.py --mode grid

# Override pickle folder and lookback
python optimize_same_legwise_reentry_params_bayesian.py --pickles-dir "G:\\My Drive\\Trading\\Historical_Options_Data" --lookback-months 12

Outputs
-------
1. Excel file in Downloads:
       same_legwise_optimizer_<mode>_<objective>.xlsx
2. Text file with best parameters:
       same_legwise_optimizer_<mode>_<objective>_best_params.txt
3. Crash-recovery checkpoint CSV:
       same_legwise_optimizer_checkpoint.csv

Notes on overfitting
--------------------
The best historical parameter set is not automatically the best live-market
parameter set. After running the optimizer, inspect:
    - top_20_detail
    - parameter sensitivity sheets
    - baseline_current
If the top results are clustered around similar parameter ranges, the result is
more credible. If the best result is a one-off outlier, treat it as overfit.

This updated version also applies a minimum-coverage rule. A parameter set must
complete trades on a configurable percentage of eligible trading days before it
can be ranked normally. This prevents high-win-rate but low-sample results from
appearing as the best strategy.
"""

from __future__ import annotations

import argparse
import glob
import itertools
import os
import sys
import time
import warnings
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")


# =============================================================================
# IMPORT THE STRATEGY BACKTESTER MODULE
# =============================================================================
# The optimizer and backtester should be in the same directory. The module is
# imported once, and its globals are patched for every trial.
#
# This is intentionally single-threaded because monkey-patching module globals is
# not thread-safe. Do not run Optuna trials with n_jobs > 1 in this script.
# =============================================================================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

try:
    import same_legwise_reentry_same_premium_backtest as bt
except ImportError as e:
    print(
        "[ERROR] Cannot import 'same_legwise_reentry_same_premium_backtest.py'.\n"
        f"Place this optimizer in the same folder as that file.\n\nOriginal error: {e}"
    )
    sys.exit(1)


# =============================================================================
# SEARCH SPACE
# =============================================================================
# Edit these ranges if you want wider/narrower optimization.
#
# Keep the search space sensible. Very wide spaces produce impressive-looking
# but often overfit results.
# =============================================================================

# Bayesian search space. Optuna will sample intelligently from these ranges.
BAYESIAN_SPACE = {
    # Use practical entry times. Add more if required.
    "ENTRY_TIME_IST": [
        "09:20", "09:25", "09:30", "09:35", "09:40", "09:45",
        "09:50", "09:55", "10:00", "10:05", "10:10", "10:15",
    ],

    # 0 => ATM short straddle. 1/2/3 => OTM short strangle-style pair.
    "OTM_DISTANCE_STEPS": (0, 3),

    # Leg stop percentage. Example: 15 means exit one short leg if premium rises 15%.
    "LEG_PREMIUM_RISE_EXIT_PCT": (5.0, 40.0, 1.0),  # low, high, step

    # Retry count per leg, not for the combined pair.
    "MAX_RETRIES_PER_LEG": (0, 5),

    # Waiting time after stop before re-entry is allowed.
    "MIN_REENTRY_GAP_MINUTES": (0, 15),

    # True is safer; False matches a looser fill assumption.
    "REENTRY_REQUIRE_RANGE_TOUCH": [True, False],
}

# Grid search space. This is smaller by design because exhaustive search can
# become very slow.
GRID_SPACE = {
    "ENTRY_TIME_IST": ["09:20", "09:25", "09:30", "09:35", "09:40", "09:45", "10:00"],
    "OTM_DISTANCE_STEPS": [0, 1, 2],
    "LEG_PREMIUM_RISE_EXIT_PCT": [8, 10, 12, 15, 18, 20, 25, 30],
    "MAX_RETRIES_PER_LEG": [0, 1, 2, 3, 4],
    "MIN_REENTRY_GAP_MINUTES": [0, 1, 3, 5, 10],
    "REENTRY_REQUIRE_RANGE_TOUCH": [True],
}

PARAM_COLS = [
    "ENTRY_TIME_IST",
    "OTM_DISTANCE_STEPS",
    "LEG_PREMIUM_RISE_EXIT_PCT",
    "MAX_RETRIES_PER_LEG",
    "MIN_REENTRY_GAP_MINUTES",
    "REENTRY_REQUIRE_RANGE_TOUCH",
]

# Cap profit factor to avoid infinity dominating scores or breaking Excel.
PROFIT_FACTOR_CAP = 99.99

# Save intermediate CSV every N trials so a long run can be recovered.
CHECKPOINT_EVERY = 10

# Default minimum coverage for an optimized result.
# Example: 80 means a parameter set must complete trades on at least 80% of
# baseline-eligible trading days. This prevents the optimizer from selecting a
# fragile parameter set that trades only a few days but shows a high win rate.
DEFAULT_MIN_COVERAGE_PCT = 80.0


# =============================================================================
# SMALL UTILITIES
# =============================================================================

def downloads_folder() -> Path:
    """Return the user's Downloads folder if it exists, else the home folder."""
    d = Path.home() / "Downloads"
    return d if d.exists() else Path.home()


def checkpoint_path() -> Path:
    """Return checkpoint CSV path."""
    return downloads_folder() / "same_legwise_optimizer_checkpoint.csv"


def save_checkpoint(results: List[Dict[str, Any]]) -> None:
    """Save intermediate results to CSV. Failure to checkpoint is non-fatal."""
    if not results:
        return
    try:
        pd.DataFrame(results).to_csv(checkpoint_path(), index=False)
    except Exception:
        pass


def validate_entry_time(entry_time_str: str) -> bool:
    """Return True if entry time is within the configured market session."""
    try:
        t = bt.parse_hhmm(entry_time_str)
    except Exception:
        return False
    return bt.SESSION_START_IST <= t < bt.SESSION_END_IST


def discover_pickles(pickles_dir: str) -> List[str]:
    """Return sorted .pkl/.pickle paths from the input folder."""
    if not os.path.isdir(pickles_dir):
        raise FileNotFoundError(f"Pickle directory not found: {pickles_dir}")
    paths = sorted(
        glob.glob(os.path.join(pickles_dir, "*.pkl"))
        + glob.glob(os.path.join(pickles_dir, "*.pickle"))
    )
    if not paths:
        raise FileNotFoundError(f"No .pkl/.pickle files found in: {pickles_dir}")
    return paths


# =============================================================================
# PRELOADED DATA
# =============================================================================
# This class performs the expensive I/O once:
#   1. scan pickles
#   2. determine nearest expiry per underlying/day
#   3. determine one actual underlying per day
#   4. download underlying candles from Kite once
#   5. keep option day groups in memory
#
# Per-trial simulation only patches parameters and calls bt.simulate_day().
# =============================================================================

class PreloadedData:
    """Container for cached option groups and underlying candles."""

    def __init__(
        self,
        *,
        pickles_dir: str,
        lookback_months: int,
        force_window_end: Optional[date] = None,
    ) -> None:
        self.pickles_dir = pickles_dir
        self.lookback_months = int(lookback_months)
        self.paths = discover_pickles(pickles_dir)

        self.min_day_seen: Optional[date] = None
        self.max_day_seen: Optional[date] = None
        self.window_start: Optional[date] = None
        self.window_end: Optional[date] = None

        self.min_expiry_map: Dict[Tuple[str, date], date] = {}
        self.actual_underlying_by_day: Dict[date, str] = {}
        self.underlying_data: Dict[str, pd.DataFrame] = {}
        self.groups: List[Dict[str, Any]] = []

        # Number of unique day/underlying/expiry candidates after all filters.
        # Used to penalize low-coverage parameter sets during optimization.
        self.expected_day_keys: set[Tuple[str, date, date]] = set()
        self.expected_n_days: int = 0

        self._scan_range_and_expiries(force_window_end=force_window_end)
        self._download_underlying_data_once()
        self._preload_option_groups()

    def _scan_range_and_expiries(self, *, force_window_end: Optional[date]) -> None:
        """
        Scan all pickles once using the backtester's own standardization function.
        This keeps expiry/underlying selection aligned with the backtester.
        """
        print("[PRELOAD] Scanning pickles for date range and nearest expiries...")
        t0 = time.time()

        min_day: Optional[date] = None
        max_day: Optional[date] = None
        min_expiry_map: Dict[Tuple[str, date], date] = {}

        for i, p in enumerate(self.paths, start=1):
            src = os.path.basename(p)
            try:
                raw = pd.read_pickle(p)
                if not isinstance(raw, pd.DataFrame) or raw.empty:
                    continue

                d = bt.standardize_option_df(raw, src)
                if d.empty:
                    continue

                file_min = d["day"].min()
                file_max = d["day"].max()
                min_day = file_min if min_day is None or file_min < min_day else min_day
                max_day = file_max if max_day is None or file_max > max_day else max_day

                grp = d.groupby(["underlying", "day"], sort=False)["expiry_date"].min()
                for (und, dy), ex in grp.items():
                    key = (und, dy)
                    if key not in min_expiry_map or ex < min_expiry_map[key]:
                        min_expiry_map[key] = ex

            except Exception as e:
                print(f"  [WARN] scan failed for {src}: {e}")
                if getattr(bt, "FAIL_ON_PICKLE_ERROR", False):
                    raise

            if i % 5 == 0 or i == len(self.paths):
                print(f"  ... scanned {i}/{len(self.paths)} pickles")

        if min_day is None or max_day is None:
            raise RuntimeError("No usable option data found in pickles.")

        if force_window_end is not None:
            max_day = force_window_end

        self.min_day_seen = min_day
        self.max_day_seen = max_day
        self.window_start = bt.compute_window_start(max_day, self.lookback_months)
        self.window_end = max_day
        self.min_expiry_map = min_expiry_map
        self.actual_underlying_by_day = bt.pick_actual_underlying_by_day(min_expiry_map)

        print(
            f"[PRELOAD] Range: {self.min_day_seen} -> {self.max_day_seen} | "
            f"window: {self.window_start} -> {self.window_end} | "
            f"actual-trade days: {len(self.actual_underlying_by_day)} | "
            f"{time.time() - t0:.1f}s\n"
        )

    def _download_underlying_data_once(self) -> None:
        """Initialize Kite and download underlying candles once for the full backtest window."""
        assert self.window_start is not None and self.window_end is not None

        print("[PRELOAD] Initializing Kite and downloading underlying candles once...")
        t0 = time.time()
        kite = bt.oUtils.intialize_kite_api()
        self.underlying_data = bt.download_underlyings(kite, self.window_start, self.window_end)
        print(f"[PRELOAD] Underlying download complete in {time.time() - t0:.1f}s\n")

    def _preload_option_groups(self) -> None:
        """
        Load option slices needed for simulation.

        Unlike a simple dedup-first approach, this keeps duplicate candidate
        groups from multiple files. During each trial, a day is marked processed
        only after bt.simulate_day() succeeds. This mirrors the improved logic in
        the backtester where a later complete pickle can rescue a day if an
        earlier duplicate file was incomplete.
        """
        assert self.window_start is not None and self.window_end is not None

        print("[PRELOAD] Loading option day groups into memory...")
        t0 = time.time()
        groups: List[Dict[str, Any]] = []
        total_rows = 0
        unique_day_keys: set[Tuple[str, date, date]] = set()

        for i, p in enumerate(self.paths, start=1):
            src = os.path.basename(p)
            try:
                raw = pd.read_pickle(p)
                if not isinstance(raw, pd.DataFrame) or raw.empty:
                    continue

                d = bt.standardize_option_df(raw, src)
                if d.empty:
                    continue

                # Keep only the same 0-DTE / 1-DTE regime as the backtester.
                d["days_to_expiry"] = (
                    pd.to_datetime(d["expiry_date"]) - pd.to_datetime(d["day"])
                ).dt.days
                d = d[d["days_to_expiry"].isin([0, 1])]
                d = d[(d["day"] >= self.window_start) & (d["day"] <= self.window_end)]
                if d.empty:
                    continue

                for (und, dy, ex), g in d.groupby(["underlying", "day", "expiry_date"], sort=False):
                    # Same nearest-expiry and one-underlying-per-day discipline as backtester.
                    if self.min_expiry_map.get((und, dy)) != ex:
                        continue
                    if self.actual_underlying_by_day.get(dy) != und:
                        continue

                    uday = self.underlying_data.get(und, pd.DataFrame())
                    uday = uday[uday["day"] == dy] if not uday.empty else uday
                    if uday.empty:
                        continue

                    # Keep a copy of the option slice because each group is small enough and
                    # this avoids accidental mutation across trials. The underlying-day slice is
                    # not copied because simulate_day() does not mutate it; this saves memory.
                    g2 = g.copy()
                    day_key = (und, dy, ex)
                    unique_day_keys.add(day_key)
                    groups.append(
                        {
                            "und": und,
                            "dy": dy,
                            "expiry": ex,
                            "source_pickle": src,
                            "day_opt": g2,
                            "underlying_day": uday,
                        }
                    )
                    total_rows += len(g2)

            except Exception as e:
                print(f"  [WARN] preload failed for {src}: {e}")
                if getattr(bt, "FAIL_ON_PICKLE_ERROR", False):
                    raise

            if i % 5 == 0 or i == len(self.paths):
                print(f"  ... loaded {i}/{len(self.paths)} pickles")

        # Keep deterministic order. Do not dedup here; per-trial simulation will
        # mark a day processed only after success.
        groups.sort(key=lambda x: (x["dy"], x["und"], x["expiry"], x["source_pickle"]))
        self.groups = groups
        self.expected_day_keys = unique_day_keys
        self.expected_n_days = len(unique_day_keys)

        approx_mb = total_rows * 120 / (1024 * 1024)
        print(
            f"[PRELOAD] Done: {len(groups):,} candidate groups | "
            f"{self.expected_n_days:,} unique eligible days | "
            f"{total_rows:,} option rows | approx {approx_mb:.0f} MB | "
            f"{time.time() - t0:.1f}s\n"
        )


# =============================================================================
# PARAMETER PATCHING
# =============================================================================

def patch_backtester_globals(params: Dict[str, Any], *, include_transaction_costs: bool) -> None:
    """
    Patch only the backtester globals that affect this strategy.

    This function is called before every trial. Because bt.simulate_day() reads
    these globals at runtime, monkey-patching is enough; no subprocess is needed.
    """
    bt.ENTRY_TIME_IST = str(params["ENTRY_TIME_IST"])
    bt.ENTRY_TIME = bt.parse_hhmm(bt.ENTRY_TIME_IST)

    bt.OTM_DISTANCE_STEPS = int(params["OTM_DISTANCE_STEPS"])
    bt.LEG_PREMIUM_RISE_EXIT_PCT = float(params["LEG_PREMIUM_RISE_EXIT_PCT"])
    bt.MAX_RETRIES_PER_LEG = int(params["MAX_RETRIES_PER_LEG"])
    bt.MIN_REENTRY_GAP_MINUTES = int(params["MIN_REENTRY_GAP_MINUTES"])
    bt.REENTRY_REQUIRE_RANGE_TOUCH = bool(params["REENTRY_REQUIRE_RANGE_TOUCH"])

    # Usually keep transaction costs enabled. You can disable through CLI.
    bt.INCLUDE_TRANSACTION_COSTS = bool(include_transaction_costs)


# =============================================================================
# SIMULATION AND METRICS
# =============================================================================

def empty_metrics(params: Dict[str, Any], *, reason: str = "", expected_n_days: int = 0) -> Dict[str, Any]:
    """Return a metrics row for invalid/no-trade trials."""
    return {
        **params,
        "total_net_pnl": 0.0,
        "total_gross_pnl": 0.0,
        "total_txn_charges": 0.0,
        "avg_daily_pnl": 0.0,
        "median_daily_pnl": 0.0,
        "win_rate_daily_pct": 0.0,
        "accuracy_pct": 0.0,
        "profit_factor": 0.0,
        "sharpe": 0.0,
        "max_drawdown": 0.0,
        "max_drawdown_inverse": 0.0,
        "best_day": 0.0,
        "worst_day": 0.0,
        "avg_profit_on_win_days": 0.0,
        "avg_loss_on_loss_days": 0.0,
        "n_days": 0,
        "expected_days": int(expected_n_days),
        "coverage_pct": 0.0,
        "winning_days": 0,
        "losing_days": 0,
        "flat_days": 0,
        "n_leg_cycles": 0,
        "pe_stop_count": 0,
        "ce_stop_count": 0,
        "total_stops": 0,
        "pe_retries_used": 0,
        "ce_retries_used": 0,
        "total_retries_used": 0,
        "score_profit_accuracy": -999.0,
        "score_balanced": -999.0,
        "objective_value_used": -999.0,
        "coverage_penalty_applied": 1,
        "no_trade_reason": reason,
    }


_BASELINE = {
    "total_net_pnl": 1.0,
    "sharpe": 0.01,
    "max_drawdown": -1.0,
}


def compute_max_drawdown(daily_pnls: pd.Series) -> float:
    """Compute max drawdown from daily PnL sequence."""
    if daily_pnls.empty:
        return 0.0
    cumulative = daily_pnls.astype(float).cumsum()
    peak = cumulative.cummax()
    return float((cumulative - peak).min())


def compute_profit_factor(values: pd.Series) -> float:
    """Compute capped profit factor from PnL values."""
    pnl = pd.to_numeric(values, errors="coerce").dropna().astype(float)
    gross_profit = float(pnl[pnl > 0].sum())
    gross_loss = float(-pnl[pnl < 0].sum())
    if gross_loss > 0:
        return min(gross_profit / gross_loss, PROFIT_FACTOR_CAP)
    if gross_profit > 0:
        return PROFIT_FACTOR_CAP
    return 0.0


def numeric_sum(df: pd.DataFrame, column: str) -> float:
    """
    Safely sum a numeric column.

    Returns 0.0 if the column is missing. This keeps the optimizer compatible
    with small future edits to the backtester output schema.
    """
    if df is None or df.empty or column not in df.columns:
        return 0.0
    return float(pd.to_numeric(df[column], errors="coerce").fillna(0.0).sum())


def compute_scores(metrics: Dict[str, Any]) -> Tuple[float, float]:
    """
    Compute composite optimizer scores.

    score_profit_accuracy is the default. It rewards profit and accuracy first,
    but still penalizes drawdown.

    score_balanced is more conservative and gives more weight to risk-adjusted
    performance.
    """
    base_pnl = max(abs(float(_BASELINE.get("total_net_pnl", 1.0))), 1.0)
    base_sharpe = max(abs(float(_BASELINE.get("sharpe", 0.01))), 0.01)
    base_dd = min(float(_BASELINE.get("max_drawdown", -1.0)), -1.0)

    pnl_score = float(metrics.get("total_net_pnl", 0.0)) / base_pnl
    accuracy_score = float(metrics.get("win_rate_daily_pct", 0.0)) / 100.0
    sharpe_score = float(metrics.get("sharpe", 0.0)) / base_sharpe

    # max_drawdown is negative. A smaller absolute drawdown is better.
    dd_score = float(metrics.get("max_drawdown", 0.0)) / abs(base_dd)

    # Profit factor is useful, but should not dominate.
    pf_score = min(float(metrics.get("profit_factor", 0.0)) / 2.0, 2.0)

    score_profit_accuracy = (
        0.50 * pnl_score
        + 0.30 * accuracy_score
        + 0.10 * pf_score
        + 0.10 * dd_score
    )

    score_balanced = (
        0.35 * pnl_score
        + 0.25 * sharpe_score
        + 0.20 * accuracy_score
        + 0.10 * pf_score
        + 0.10 * dd_score
    )

    return float(score_profit_accuracy), float(score_balanced)


def objective_value_for_metrics(
    metrics: Dict[str, Any],
    *,
    objective_col: str,
    min_coverage_pct: float,
) -> float:
    """
    Return the scalar value actually given to Optuna/grid search.

    If a parameter set completes too few trading days, it is penalized even if
    its raw win rate is high. This avoids the classic optimizer trap:
    "100% win rate on only two days".
    """
    raw_value = float(metrics.get(objective_col, -999.0))
    coverage = float(metrics.get("coverage_pct", 0.0))
    if coverage < float(min_coverage_pct):
        # Preserve ordering among low-coverage trials, but keep them below valid trials.
        return -999.0 + coverage / 100.0
    return raw_value


def annotate_objective_value(
    metrics: Dict[str, Any],
    *,
    objective_col: str,
    min_coverage_pct: float,
) -> Dict[str, Any]:
    """Add objective_value_used and coverage_penalty_applied to a metrics row."""
    metrics["objective_value_used"] = objective_value_for_metrics(
        metrics,
        objective_col=objective_col,
        min_coverage_pct=min_coverage_pct,
    )
    metrics["coverage_penalty_applied"] = int(
        float(metrics.get("coverage_pct", 0.0)) < float(min_coverage_pct)
    )
    return metrics


def compute_metrics(
    *,
    summary_df: pd.DataFrame,
    cycle_df: pd.DataFrame,
    params: Dict[str, Any],
    expected_n_days: int,
) -> Dict[str, Any]:
    """Compute scalar metrics from the backtester's summary/cycle rows."""
    if summary_df.empty:
        return empty_metrics(params, reason="no completed days", expected_n_days=expected_n_days)

    pnl = pd.to_numeric(summary_df["total_net_pnl"], errors="coerce").fillna(0.0).astype(float)
    gross = pd.to_numeric(summary_df["total_gross_pnl"], errors="coerce").fillna(0.0).astype(float)
    charges = pd.to_numeric(summary_df["total_txn_charges"], errors="coerce").fillna(0.0).astype(float)

    n_days = int(len(summary_df))
    winning_days = int((pnl > 0).sum())
    losing_days = int((pnl < 0).sum())
    flat_days = int((pnl == 0).sum())

    avg_daily = float(pnl.mean()) if n_days else 0.0
    median_daily = float(pnl.median()) if n_days else 0.0
    std_daily = float(pnl.std()) if n_days > 1 else 0.0
    sharpe = avg_daily / std_daily if std_daily > 0 else 0.0

    max_dd = compute_max_drawdown(pnl)
    profit_factor = compute_profit_factor(pnl)

    pe_stop_count = int(numeric_sum(summary_df, "pe_stop_count"))
    ce_stop_count = int(numeric_sum(summary_df, "ce_stop_count"))
    pe_retries = int(numeric_sum(summary_df, "pe_retries_used"))
    ce_retries = int(numeric_sum(summary_df, "ce_retries_used"))

    coverage_pct = float(100.0 * n_days / expected_n_days) if expected_n_days else 100.0

    metrics: Dict[str, Any] = {
        **params,
        "total_net_pnl": float(pnl.sum()),
        "total_gross_pnl": float(gross.sum()),
        "total_txn_charges": float(charges.sum()),
        "avg_daily_pnl": avg_daily,
        "median_daily_pnl": median_daily,
        "win_rate_daily_pct": float(100.0 * winning_days / n_days) if n_days else 0.0,
        # Alias because traders often call this strategy accuracy.
        "accuracy_pct": float(100.0 * winning_days / n_days) if n_days else 0.0,
        "profit_factor": float(profit_factor),
        "sharpe": float(sharpe),
        "max_drawdown": float(max_dd),
        # Higher is better. This is a true inverse, so smaller absolute drawdown
        # gives a larger value. Do not use -max_dd here; that would reward worse
        # drawdowns because max_dd is negative.
        "max_drawdown_inverse": float(1.0 / (1.0 + abs(max_dd))),
        "best_day": float(pnl.max()) if n_days else 0.0,
        "worst_day": float(pnl.min()) if n_days else 0.0,
        "avg_profit_on_win_days": float(pnl[pnl > 0].mean()) if winning_days else 0.0,
        "avg_loss_on_loss_days": float(pnl[pnl < 0].mean()) if losing_days else 0.0,
        "n_days": n_days,
        "expected_days": int(expected_n_days),
        "coverage_pct": float(coverage_pct),
        "winning_days": winning_days,
        "losing_days": losing_days,
        "flat_days": flat_days,
        "n_leg_cycles": int(len(cycle_df)) if cycle_df is not None else 0,
        "pe_stop_count": pe_stop_count,
        "ce_stop_count": ce_stop_count,
        "total_stops": pe_stop_count + ce_stop_count,
        "pe_retries_used": pe_retries,
        "ce_retries_used": ce_retries,
        "total_retries_used": pe_retries + ce_retries,
        "no_trade_reason": "",
    }

    score_profit_accuracy, score_balanced = compute_scores(metrics)
    metrics["score_profit_accuracy"] = score_profit_accuracy
    metrics["score_balanced"] = score_balanced
    metrics["objective_value_used"] = score_profit_accuracy
    metrics["coverage_penalty_applied"] = 0
    return metrics


def run_simulation(
    *,
    data: PreloadedData,
    params: Dict[str, Any],
    include_transaction_costs: bool,
) -> Dict[str, Any]:
    """
    Run one full backtest trial with the supplied parameters.

    This function does not write Excel. It only returns metrics for Optuna/grid.
    """
    if not validate_entry_time(str(params["ENTRY_TIME_IST"])):
        return empty_metrics(params, reason="invalid entry time", expected_n_days=data.expected_n_days)

    patch_backtester_globals(params, include_transaction_costs=include_transaction_costs)

    summaries: List[Dict[str, Any]] = []
    cycles: List[Dict[str, Any]] = []
    processed_day_keys: set[Tuple[str, date, date]] = set()

    for g in data.groups:
        day_key = (g["und"], g["dy"], g["expiry"])
        if day_key in processed_day_keys:
            continue

        try:
            summary, leg_cycles, _skip_rows = bt.simulate_day(
                und=g["und"],
                dy=g["dy"],
                expiry=g["expiry"],
                day_opt=g["day_opt"],
                underlying_day=g["underlying_day"],
            )
        except Exception:
            # Trial-specific failures should not kill the optimizer.
            # They usually indicate missing strikes/candles for a parameter set.
            continue

        if summary is not None:
            processed_day_keys.add(day_key)
            summaries.append(asdict(summary))
            cycles.extend(asdict(c) for c in leg_cycles)

    if not summaries:
        return empty_metrics(params, reason="no completed days", expected_n_days=data.expected_n_days)

    summary_df = pd.DataFrame(summaries)
    cycle_df = pd.DataFrame(cycles)
    return compute_metrics(
        summary_df=summary_df,
        cycle_df=cycle_df,
        params=params,
        expected_n_days=data.expected_n_days,
    )


# =============================================================================
# BASELINE
# =============================================================================

def current_backtester_params() -> Dict[str, Any]:
    """Read the current default params from the imported backtester."""
    return {
        "ENTRY_TIME_IST": bt.ENTRY_TIME_IST,
        "OTM_DISTANCE_STEPS": int(bt.OTM_DISTANCE_STEPS),
        "LEG_PREMIUM_RISE_EXIT_PCT": float(bt.LEG_PREMIUM_RISE_EXIT_PCT),
        "MAX_RETRIES_PER_LEG": int(bt.MAX_RETRIES_PER_LEG),
        "MIN_REENTRY_GAP_MINUTES": int(bt.MIN_REENTRY_GAP_MINUTES),
        "REENTRY_REQUIRE_RANGE_TOUCH": bool(bt.REENTRY_REQUIRE_RANGE_TOUCH),
    }


def run_baseline(data: PreloadedData, *, include_transaction_costs: bool) -> Dict[str, Any]:
    """Run current backtester defaults to establish the before/after benchmark."""
    params = current_backtester_params()
    print("[BASELINE] Running current defaults from backtester...")
    t0 = time.time()
    metrics = run_simulation(
        data=data,
        params=params,
        include_transaction_costs=include_transaction_costs,
    )

    # Normalize composite scores against baseline. If the baseline is poor or
    # has no trades, use safe non-zero anchors.
    if metrics.get("n_days", 0) > 0:
        _BASELINE["total_net_pnl"] = max(abs(float(metrics.get("total_net_pnl", 1.0))), 1.0)
        _BASELINE["sharpe"] = max(abs(float(metrics.get("sharpe", 0.01))), 0.01)
        _BASELINE["max_drawdown"] = min(float(metrics.get("max_drawdown", -1.0)), -1.0)

    score_profit_accuracy, score_balanced = compute_scores(metrics)
    metrics["score_profit_accuracy"] = score_profit_accuracy
    metrics["score_balanced"] = score_balanced

    print(
        f"[BASELINE] {time.time() - t0:.1f}s | "
        f"PnL=Rs {metrics['total_net_pnl']:,.0f} | "
        f"Accuracy={metrics['win_rate_daily_pct']:.1f}% | "
        f"Sharpe={metrics['sharpe']:.3f} | "
        f"DD=Rs {metrics['max_drawdown']:,.0f} | "
        f"Days={metrics['n_days']}"
    )
    print(f"[BASELINE] Params: {params}\n")
    return metrics


# =============================================================================
# OPTIMIZATION MODES
# =============================================================================

def sample_bayesian_params(trial: "Any") -> Dict[str, Any]:
    """Ask Optuna for one parameter set."""
    sl_low, sl_high, sl_step = BAYESIAN_SPACE["LEG_PREMIUM_RISE_EXIT_PCT"]
    return {
        "ENTRY_TIME_IST": trial.suggest_categorical(
            "ENTRY_TIME_IST", BAYESIAN_SPACE["ENTRY_TIME_IST"]
        ),
        "OTM_DISTANCE_STEPS": trial.suggest_int(
            "OTM_DISTANCE_STEPS",
            BAYESIAN_SPACE["OTM_DISTANCE_STEPS"][0],
            BAYESIAN_SPACE["OTM_DISTANCE_STEPS"][1],
        ),
        "LEG_PREMIUM_RISE_EXIT_PCT": round(
            trial.suggest_float(
                "LEG_PREMIUM_RISE_EXIT_PCT",
                float(sl_low),
                float(sl_high),
                step=float(sl_step),
            ),
            2,
        ),
        "MAX_RETRIES_PER_LEG": trial.suggest_int(
            "MAX_RETRIES_PER_LEG",
            BAYESIAN_SPACE["MAX_RETRIES_PER_LEG"][0],
            BAYESIAN_SPACE["MAX_RETRIES_PER_LEG"][1],
        ),
        "MIN_REENTRY_GAP_MINUTES": trial.suggest_int(
            "MIN_REENTRY_GAP_MINUTES",
            BAYESIAN_SPACE["MIN_REENTRY_GAP_MINUTES"][0],
            BAYESIAN_SPACE["MIN_REENTRY_GAP_MINUTES"][1],
        ),
        "REENTRY_REQUIRE_RANGE_TOUCH": trial.suggest_categorical(
            "REENTRY_REQUIRE_RANGE_TOUCH",
            BAYESIAN_SPACE["REENTRY_REQUIRE_RANGE_TOUCH"],
        ),
    }


def run_bayesian(
    *,
    data: PreloadedData,
    n_trials: int,
    objective_col: str,
    include_transaction_costs: bool,
    min_coverage_pct: float,
) -> pd.DataFrame:
    """Run Optuna Bayesian/TPE optimization."""
    try:
        import optuna
        optuna.logging.set_verbosity(optuna.logging.WARNING)
    except ImportError:
        print("[ERROR] optuna is not installed. Install it with: pip install optuna")
        sys.exit(1)

    results: List[Dict[str, Any]] = []
    best_value = -float("inf")
    trial_times: List[float] = []

    def objective(trial: "Any") -> float:
        nonlocal best_value
        t0 = time.time()
        params = sample_bayesian_params(trial)

        metrics = run_simulation(
            data=data,
            params=params,
            include_transaction_costs=include_transaction_costs,
        )
        metrics = annotate_objective_value(
            metrics,
            objective_col=objective_col,
            min_coverage_pct=min_coverage_pct,
        )
        results.append(metrics)

        value = float(metrics.get("objective_value_used", -999.0))
        elapsed = time.time() - t0
        trial_times.append(elapsed)

        if trial.number == 0:
            eta = elapsed * max(n_trials, 1) / 60.0
            print(f"  Trial 0 took {elapsed:.1f}s -> rough ETA {eta:.0f} min for {n_trials} trials\n")

        if value > best_value and metrics.get("n_days", 0) > 0:
            best_value = value
            print(
                f"  * Trial {trial.number:>4d} NEW BEST {objective_col}={value:,.4f} | "
                f"PnL=Rs {metrics['total_net_pnl']:,.0f} | "
                f"Acc={metrics['win_rate_daily_pct']:.1f}% | "
                f"Sharpe={metrics['sharpe']:.3f} | "
                f"DD=Rs {metrics['max_drawdown']:,.0f} | "
                f"Cov={metrics.get('coverage_pct', 0.0):.1f}% | "
                f"ET={params['ENTRY_TIME_IST']} "
                f"OTM={params['OTM_DISTANCE_STEPS']} "
                f"SL%={params['LEG_PREMIUM_RISE_EXIT_PCT']} "
                f"R={params['MAX_RETRIES_PER_LEG']} "
                f"Gap={params['MIN_REENTRY_GAP_MINUTES']} "
                f"Touch={params['REENTRY_REQUIRE_RANGE_TOUCH']}"
            )
        elif trial.number > 0 and trial.number % 25 == 0:
            avg_t = float(np.mean(trial_times)) if trial_times else 0.0
            remaining = max(n_trials - trial.number - 1, 0) * avg_t / 60.0
            print(
                f"  Trial {trial.number:>4d}/{n_trials} | "
                f"best {objective_col}={best_value:,.4f} | "
                f"{avg_t:.1f}s/trial | ETA {remaining:.0f}m"
            )

        if (trial.number + 1) % CHECKPOINT_EVERY == 0:
            save_checkpoint(results)

        return value

    print(f"[BAYESIAN] Starting {n_trials} trials | objective={objective_col}")
    print(f"[BAYESIAN] Minimum coverage required: {min_coverage_pct:.1f}%")
    print(f"[BAYESIAN] Checkpoint: {checkpoint_path()}\n")

    study = optuna.create_study(
        direction="maximize",
        sampler=optuna.samplers.TPESampler(seed=42),
    )
    study.optimize(objective, n_trials=n_trials, show_progress_bar=False, n_jobs=1)

    save_checkpoint(results)
    return pd.DataFrame(results)


def iter_grid_params() -> Iterable[Dict[str, Any]]:
    """Yield every parameter combination from GRID_SPACE."""
    keys = list(GRID_SPACE.keys())
    for values in itertools.product(*[GRID_SPACE[k] for k in keys]):
        yield dict(zip(keys, values))


def run_grid(
    *,
    data: PreloadedData,
    objective_col: str,
    include_transaction_costs: bool,
    min_coverage_pct: float,
) -> pd.DataFrame:
    """Run exhaustive grid search."""
    combos = list(iter_grid_params())
    total = len(combos)
    print(f"[GRID] Evaluating {total:,} combinations | objective={objective_col}")
    print(f"[GRID] Minimum coverage required: {min_coverage_pct:.1f}%")
    print(f"[GRID] Checkpoint: {checkpoint_path()}\n")

    results: List[Dict[str, Any]] = []
    best_value = -float("inf")
    t_start = time.time()
    trial_times: List[float] = []

    for i, params in enumerate(combos, start=1):
        t0 = time.time()
        metrics = run_simulation(
            data=data,
            params=params,
            include_transaction_costs=include_transaction_costs,
        )
        metrics = annotate_objective_value(
            metrics,
            objective_col=objective_col,
            min_coverage_pct=min_coverage_pct,
        )
        results.append(metrics)
        elapsed = time.time() - t0
        trial_times.append(elapsed)

        value = float(metrics.get("objective_value_used", -999.0))
        if value > best_value and metrics.get("n_days", 0) > 0:
            best_value = value
            print(
                f"  * [{i:>5d}/{total}] NEW BEST {objective_col}={value:,.4f} | "
                f"PnL=Rs {metrics['total_net_pnl']:,.0f} | "
                f"Acc={metrics['win_rate_daily_pct']:.1f}% | "
                f"ET={params['ENTRY_TIME_IST']} OTM={params['OTM_DISTANCE_STEPS']} "
                f"SL%={params['LEG_PREMIUM_RISE_EXIT_PCT']} R={params['MAX_RETRIES_PER_LEG']} "
                f"Gap={params['MIN_REENTRY_GAP_MINUTES']} Touch={params['REENTRY_REQUIRE_RANGE_TOUCH']}"
            )
        elif i % 200 == 0:
            avg_t = float(np.mean(trial_times)) if trial_times else 0.0
            remaining = max(total - i, 0) * avg_t / 60.0
            print(
                f"  [{i:>5d}/{total}] elapsed={(time.time() - t_start) / 60:.1f}m | "
                f"ETA={remaining:.0f}m | best {objective_col}={best_value:,.4f}"
            )

        if i % CHECKPOINT_EVERY == 0:
            save_checkpoint(results)

    save_checkpoint(results)
    return pd.DataFrame(results)


# =============================================================================
# OUTPUT
# =============================================================================

def autosize_columns_safe(ws: "Any") -> None:
    """Autosize Excel columns defensively."""
    try:
        for col_idx in range(1, (ws.max_column or 0) + 1):
            col_letter = ws.cell(row=1, column=col_idx).column_letter
            max_len = 0
            for row_idx in range(1, min(ws.max_row or 1, 2000) + 1):
                value = ws.cell(row=row_idx, column=col_idx).value
                if value is not None:
                    max_len = max(max_len, len(str(value)))
            ws.column_dimensions[col_letter].width = min(60, max(10, max_len + 2))
    except Exception:
        return


def write_best_params_text(
    *,
    best: pd.Series,
    baseline: Dict[str, Any],
    objective_col: str,
    txt_path: str,
) -> None:
    """Write best parameters in CMD, PowerShell and Python env formats."""
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("# ============================================================\n")
        f.write("# Best parameters for same_legwise_reentry_same_premium_backtest.py\n")
        f.write("# ============================================================\n")
        f.write(f"# Objective:          {objective_col}\n")
        f.write(f"# Total Net PnL:      Rs {float(best['total_net_pnl']):,.0f}\n")
        f.write(f"# Accuracy / Win %:   {float(best['win_rate_daily_pct']):.1f}%\n")
        f.write(f"# Sharpe:             {float(best['sharpe']):.3f}\n")
        f.write(f"# Max Drawdown:       Rs {float(best['max_drawdown']):,.0f}\n")
        f.write(f"# Profit Factor:      {float(best['profit_factor']):.2f}\n")
        f.write(f"# Days:               {int(best['n_days'])}\n\n")

        f.write("# --- Windows CMD: set before running the backtester ---\n")
        for p in PARAM_COLS:
            f.write(f"set {p}={best[p]}\n")

        f.write("\n# --- PowerShell: set before running the backtester ---\n")
        for p in PARAM_COLS:
            f.write(f'$env:{p}="{best[p]}"\n')

        f.write("\n# --- Python os.environ format ---\n")
        for p in PARAM_COLS:
            f.write(f'os.environ["{p}"] = "{best[p]}"\n')

        f.write("\n# ============================================================\n")
        f.write("# Baseline current-default performance\n")
        f.write("# ============================================================\n")
        f.write(f"# Baseline PnL:       Rs {float(baseline.get('total_net_pnl', 0)):,.0f}\n")
        f.write(f"# Baseline Accuracy:  {float(baseline.get('win_rate_daily_pct', 0)):.1f}%\n")
        f.write(f"# Baseline Sharpe:    {float(baseline.get('sharpe', 0)):.3f}\n")
        f.write(f"# Baseline Drawdown:  Rs {float(baseline.get('max_drawdown', 0)):,.0f}\n")


def build_sensitivity_table(results_df: pd.DataFrame, param: str) -> pd.DataFrame:
    """Build parameter sensitivity table for one parameter."""
    return (
        results_df.groupby(param, as_index=False)
        .agg(
            trials=("total_net_pnl", "count"),
            avg_total_net_pnl=("total_net_pnl", "mean"),
            best_total_net_pnl=("total_net_pnl", "max"),
            avg_accuracy_pct=("win_rate_daily_pct", "mean"),
            best_accuracy_pct=("win_rate_daily_pct", "max"),
            avg_sharpe=("sharpe", "mean"),
            avg_profit_factor=("profit_factor", "mean"),
            avg_max_drawdown=("max_drawdown", "mean"),
            avg_coverage_pct=("coverage_pct", "mean"),
            penalty_count=("coverage_penalty_applied", "sum"),
            avg_score_profit_accuracy=("score_profit_accuracy", "mean"),
            avg_objective_value=("objective_value_used", "mean"),
        )
        .sort_values("avg_total_net_pnl", ascending=False)
        .reset_index(drop=True)
    )


def write_results(
    *,
    results_df: pd.DataFrame,
    baseline: Dict[str, Any],
    objective_col: str,
    output_path: str,
    min_coverage_pct: float,
) -> None:
    """Write ranked optimizer results to Excel and best-params text file."""
    if results_df.empty:
        print("[WARN] No results to write.")
        return

    results_df = results_df.copy()

    # Ensure score columns are current after baseline normalization.
    recalculated = results_df.apply(lambda row: compute_scores(row.to_dict()), axis=1)
    results_df["score_profit_accuracy"] = [x[0] for x in recalculated]
    results_df["score_balanced"] = [x[1] for x in recalculated]

    if objective_col not in results_df.columns:
        raise ValueError(f"Objective column not found in results: {objective_col}")

    # Sort by the value actually optimized. This applies the same coverage
    # penalty used during Optuna/grid search while preserving the raw objective
    # column for reporting.
    annotated_rows = results_df.apply(
        lambda row: annotate_objective_value(
            row.to_dict(),
            objective_col=objective_col,
            min_coverage_pct=min_coverage_pct,
        ),
        axis=1,
    )
    results_df["objective_value_used"] = [r["objective_value_used"] for r in annotated_rows]
    results_df["coverage_penalty_applied"] = [r["coverage_penalty_applied"] for r in annotated_rows]

    results_df = results_df.sort_values("objective_value_used", ascending=False).reset_index(drop=True)
    results_df.index.name = "rank"
    results_df.index += 1

    # Put parameter columns first.
    metric_cols = [c for c in results_df.columns if c not in PARAM_COLS]
    results_df = results_df[PARAM_COLS + metric_cols]

    best = results_df.iloc[0]

    print("\n" + "=" * 120)
    print("TOP 20 PARAMETER COMBINATIONS")
    print("=" * 120)
    display_cols = PARAM_COLS + [
        "total_net_pnl", "win_rate_daily_pct", "sharpe", "profit_factor",
        "max_drawdown", "coverage_pct", "objective_value_used",
        "score_profit_accuracy", "score_balanced", "n_days",
    ]
    display_cols = [c for c in display_cols if c in results_df.columns]
    print(results_df.head(20)[display_cols].to_string())

    print("\n" + "=" * 120)
    print("BEST PARAMETERS VS CURRENT DEFAULTS")
    print("=" * 120)
    for p in PARAM_COLS:
        print(f"{p:32s}  best={str(best[p]):>12s}  baseline={str(baseline.get(p, '?')):>12s}")
    print("-" * 120)
    for m in ["total_net_pnl", "win_rate_daily_pct", "coverage_pct", "sharpe", "profit_factor", "max_drawdown", "n_days"]:
        b = baseline.get(m, 0)
        v = best[m]
        if m in {"total_net_pnl", "max_drawdown"}:
            print(f"{m:32s}  best=Rs {float(v):>12,.0f}  baseline=Rs {float(b):>12,.0f}")
        elif m in {"win_rate_daily_pct", "coverage_pct"}:
            print(f"{m:32s}  best={float(v):>13.1f}%  baseline={float(b):>13.1f}%")
        else:
            print(f"{m:32s}  best={float(v):>14.3f}  baseline={float(b):>14.3f}")

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    with pd.ExcelWriter(output_path, engine="openpyxl") as xw:
        results_df.to_excel(xw, sheet_name="all_trials_ranked", index=True)
        results_df.head(20).T.to_excel(xw, sheet_name="top_20_detail")
        pd.DataFrame([baseline]).to_excel(xw, sheet_name="baseline_current", index=False)

        # Sensitivity sheets are essential for spotting overfitting.
        for p in PARAM_COLS:
            try:
                sens = build_sensitivity_table(results_df.reset_index(drop=True), p)
                sens.to_excel(xw, sheet_name=f"sens_{p}"[:31], index=False)
            except Exception:
                pass

        wb = xw.book
        for ws in wb.worksheets:
            ws.freeze_panes = "A2"
            autosize_columns_safe(ws)

    print(f"\n[SAVED] {output_path}")

    txt_path = output_path.replace(".xlsx", "_best_params.txt")
    write_best_params_text(
        best=best,
        baseline=baseline,
        objective_col=objective_col,
        txt_path=txt_path,
    )
    print(f"[SAVED] {txt_path}")

    # Remove checkpoint after successful final output.
    try:
        cp = checkpoint_path()
        if cp.exists():
            cp.unlink()
            print(f"[CLEANUP] Removed checkpoint: {cp}")
    except Exception:
        pass


# =============================================================================
# MAIN
# =============================================================================

def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Bayesian/grid optimizer for same-legwise re-entry strategy.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python optimize_same_legwise_reentry_params_bayesian.py
  python optimize_same_legwise_reentry_params_bayesian.py --trials 400
  python optimize_same_legwise_reentry_params_bayesian.py --objective total_net_pnl
  python optimize_same_legwise_reentry_params_bayesian.py --objective win_rate_daily_pct
  python optimize_same_legwise_reentry_params_bayesian.py --mode grid
        """,
    )
    parser.add_argument(
        "--mode",
        choices=["bayesian", "grid"],
        default="bayesian",
        help="Optimization mode. Default: bayesian",
    )
    parser.add_argument(
        "--trials",
        type=int,
        default=200,
        help="Number of Bayesian trials. Ignored for grid. Default: 200",
    )
    parser.add_argument(
        "--objective",
        choices=[
            "score_profit_accuracy",
            "score_balanced",
            "total_net_pnl",
            "win_rate_daily_pct",
            "accuracy_pct",
            "sharpe",
            "profit_factor",
            "max_drawdown_inverse",
        ],
        default="score_profit_accuracy",
        help="Metric to maximize. Default: score_profit_accuracy",
    )
    parser.add_argument(
        "--pickles-dir",
        default=None,
        help="Override option pickle directory. Default: backtester PICKLES_DIR",
    )
    parser.add_argument(
        "--lookback-months",
        type=int,
        default=None,
        help="Override lookback months. Default: backtester LOOKBACK_MONTHS",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output Excel path. Default: Downloads/same_legwise_optimizer_<mode>_<objective>.xlsx",
    )
    parser.add_argument(
        "--no-transaction-costs",
        action="store_true",
        help="Disable transaction costs during optimization. Default: costs enabled.",
    )
    parser.add_argument(
        "--min-coverage-pct",
        type=float,
        default=DEFAULT_MIN_COVERAGE_PCT,
        help=(
            "Minimum percentage of eligible trading days a parameter set must complete "
            "before it can be ranked normally. Lower-coverage trials are penalized. "
            f"Default: {DEFAULT_MIN_COVERAGE_PCT:.1f}"
        ),
    )
    return parser.parse_args()


def main() -> None:
    """Program entry point."""
    args = parse_args()

    pickles_dir = args.pickles_dir or bt.PICKLES_DIR
    lookback = int(args.lookback_months if args.lookback_months is not None else bt.LOOKBACK_MONTHS)
    include_transaction_costs = not bool(args.no_transaction_costs)
    output_path = args.output or str(
        downloads_folder() / f"same_legwise_optimizer_{args.mode}_{args.objective}.xlsx"
    )

    print("=" * 88)
    print(" SAME-LEGWISE RE-ENTRY STRATEGY OPTIMIZER")
    print("=" * 88)
    print(f" Mode:                    {args.mode}")
    print(f" Objective:               {args.objective}")
    if args.mode == "bayesian":
        print(f" Trials:                  {args.trials}")
    else:
        total_grid = 1
        for vals in GRID_SPACE.values():
            total_grid *= len(vals)
        print(f" Grid combinations:       {total_grid:,}")
    print(f" Pickles dir:             {pickles_dir}")
    print(f" Lookback months:         {lookback}")
    print(f" Include transaction cost:{include_transaction_costs}")
    print(f" Min coverage pct:        {args.min_coverage_pct:.1f}")
    print(f" Output:                  {output_path}")
    print(f" Checkpoint:              {checkpoint_path()}")
    print()

    # Preload all data once.
    data = PreloadedData(
        pickles_dir=pickles_dir,
        lookback_months=lookback,
    )
    if not data.groups:
        print("[ERROR] No eligible option groups found after filtering.")
        sys.exit(1)

    # Baseline run anchors the composite scores and gives a proper before/after comparison.
    baseline = run_baseline(data, include_transaction_costs=include_transaction_costs)

    # Optimize.
    t0 = time.time()
    if args.mode == "bayesian":
        results_df = run_bayesian(
            data=data,
            n_trials=int(args.trials),
            objective_col=args.objective,
            include_transaction_costs=include_transaction_costs,
            min_coverage_pct=float(args.min_coverage_pct),
        )
    else:
        results_df = run_grid(
            data=data,
            objective_col=args.objective,
            include_transaction_costs=include_transaction_costs,
            min_coverage_pct=float(args.min_coverage_pct),
        )

    print(f"\n[DONE] {len(results_df):,} trials completed in {(time.time() - t0) / 60:.1f} minutes")

    # Final output.
    write_results(
        results_df=results_df,
        baseline=baseline,
        objective_col=args.objective,
        output_path=output_path,
        min_coverage_pct=float(args.min_coverage_pct),
    )

    print("\n" + "=" * 88)
    print("CAUTION")
    print("=" * 88)
    print("Do not deploy the #1 parameter set blindly. Check the sensitivity sheets.")
    print("A robust result usually shows several top-ranked combinations in the same parameter region.")
    print("If one result is isolated, it is probably overfit to historical data.")


if __name__ == "__main__":
    main()
