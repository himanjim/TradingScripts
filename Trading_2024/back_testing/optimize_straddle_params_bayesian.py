"""
SHORT-STRADDLE PARAMETER OPTIMIZER  (v2 — corrected)
=====================================================
Finds optimal values for the 6 key strategy variables by running the backtest
engine hundreds of times against pre-loaded pickle data.

Two modes:
  1. BAYESIAN (default) — uses optuna to intelligently explore the parameter
     space.  Typically finds the optimum in 150-300 trials.
  2. GRID — exhaustive search over a user-defined grid.

Place this file in the SAME directory as:
    dhan_atm_straddle_prem_jump_reattempt_prem_perc.py

Requirements:
    pip install optuna openpyxl pandas

Usage examples:
    python optimize_straddle_params_bayesian.py                          # Bayesian, 200 trials, balanced score
    python optimize_straddle_params_bayesian.py --mode grid              # Full grid search
    python optimize_straddle_params_bayesian.py --trials 300             # More Bayesian trials
    python optimize_straddle_params_bayesian.py --objective total_pnl    # Maximize raw profit
    python optimize_straddle_params_bayesian.py --objective sharpe       # Maximize risk-adjusted return
    python optimize_straddle_params_bayesian.py --lookback-months 12     # Only use last 12 months of data
    python optimize_straddle_params_bayesian.py --pickles-dir "D:\\Data" # Override pickle location

Output:
    ~/Downloads/optimizer_results_<mode>_<objective>.xlsx    (all trials ranked)
    ~/Downloads/optimizer_results_<mode>_<objective>_best_params.txt
    ~/Downloads/optimizer_checkpoint.csv  (incremental saves - survives crashes)
"""

import os
import sys
import glob
import time
import argparse
import warnings
import itertools
from datetime import date, time as dtime, datetime
from dataclasses import asdict
from typing import Dict, List, Tuple, Any, Optional
from pathlib import Path

import pandas as pd
import numpy as np

warnings.filterwarnings("ignore")


# =============================================================================
# IMPORT THE ORIGINAL BACKTESTER MODULE
# =============================================================================
# The module is imported ONCE; we monkey-patch its globals for each trial.
# This is safe because simulation is single-threaded.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

try:
    import dhan_atm_straddle_prem_jump_reattempt_prem_perc as bt
except ImportError as e:
    print(f"[ERROR] Cannot import the backtester module.\n"
          f"  Make sure 'dhan_atm_straddle_prem_jump_reattempt_prem_perc.py' is in:\n"
          f"  {SCRIPT_DIR}\n\n  Original error: {e}")
    sys.exit(1)


# =============================================================================
# SEARCH SPACE CONFIGURATION  (edit these to widen/narrow the search)
# =============================================================================
# NOTE: SESSION_START_IST in the backtester is 09:15.
#       Entry times BEFORE 09:15 will produce zero trades - don't include them.
#
# NOTE: MAX_STOPLOSS_RUPEES = 0 means "no hard rupee cap, use % of premium only".
#       The backtester's logic: `if MAX_STOPLOSS_RUPEES and MSR > 0` - so 0 disables it.

# --- Bayesian search (continuous + categorical for optuna) ---
BAYESIAN_SPACE = {
    "ENTRY_TIME_IST":             ["09:20", "09:25", "09:30", "09:35", "09:40",
                                   "09:45", "09:50", "10:00"],
    "LOSS_LIMIT_PCT":             (0.05, 0.40),       # float range, step 0.01
    "PROFIT_PROTECT_TRIGGER_PCT": (0.10, 0.50),       # float range, step 0.01
    "MAX_STOPLOSS_RUPEES":        [0, 2000, 3000, 4000, 5000, 6000, 7000,
                                   8000, 10000, 12000, 15000],  # categorical (0=disabled)
    "MAX_REATTEMPTS":             (0, 3),              # int range
    "REENTRY_DELAY_MINUTES":      (1, 15),             # int range
}

# --- Grid search (explicit values per variable) ---
GRID_SPACE = {
    "ENTRY_TIME_IST":             ["09:20", "09:25", "09:30", "09:35", "09:40", "09:45"],
    "LOSS_LIMIT_PCT":             [0.10, 0.15, 0.20, 0.25, 0.30],
    "PROFIT_PROTECT_TRIGGER_PCT": [0.15, 0.20, 0.25, 0.30, 0.40],
    "MAX_STOPLOSS_RUPEES":        [0, 3000, 5000, 7000, 10000],
    "MAX_REATTEMPTS":             [0, 1, 2],
    "REENTRY_DELAY_MINUTES":      [1, 3, 5, 10],
}
# Grid size: 6 x 5 x 5 x 5 x 3 x 4 = 9,000 combinations


# Cap for profit_factor to avoid inf breaking optuna/pandas
PROFIT_FACTOR_CAP = 99.99

# Checkpoint: save intermediate results every N trials (survives crashes)
CHECKPOINT_EVERY = 10


# =============================================================================
# STEP 1 - PRE-LOAD ALL PICKLE DATA (runs once, cached in memory)
# =============================================================================

class PreloadedData:
    """Holds normalized + grouped pickle data ready for fast per-trial simulation."""

    def __init__(self, pickles_dir: str, window_start: date, window_end: date):
        self.groups: List[Dict[str, Any]] = []
        self.n_pickles = 0
        self.memory_mb = 0.0
        self._load(pickles_dir, window_start, window_end)

    def _load(self, pickles_dir: str, window_start: date, window_end: date):
        paths = sorted(
            glob.glob(os.path.join(pickles_dir, "*.pkl"))
            + glob.glob(os.path.join(pickles_dir, "*.pickle"))
        )
        if not paths:
            raise FileNotFoundError(f"No .pkl / .pickle files found in: {pickles_dir}")

        self.n_pickles = len(paths)
        print(f"[PRELOAD] Found {self.n_pickles} pickle files.  Normalizing ...")
        t0 = time.time()

        all_groups: List[Dict[str, Any]] = []

        for i, p in enumerate(paths):
            src = os.path.basename(p)
            try:
                raw = pd.read_pickle(p)
                if not isinstance(raw, pd.DataFrame) or raw.empty:
                    continue

                d = bt._normalize_dhan_df(raw, src)
                if d.empty:
                    continue

                d = d[(d["day"] >= window_start) & (d["day"] <= window_end)]
                if d.empty:
                    continue

                # Nearest expiry per (underlying, day) within this pickle
                min_expiry = (
                    d.groupby(["underlying", "day"], sort=False)["expiry"]
                    .min().to_dict()
                )

                for (und, dy, ex), g in d.groupby(["underlying", "day", "expiry"],
                                                    sort=False):
                    if min_expiry.get((und, dy)) != ex:
                        continue
                    all_groups.append({
                        "und": und,
                        "dy": dy,
                        "expiry": ex,
                        "day_opt": g.copy(),
                        "source_pickle": src,
                    })

                if (i + 1) % 5 == 0 or (i + 1) == self.n_pickles:
                    print(f"  ... {i+1}/{self.n_pickles} pickles loaded")

            except Exception as e:
                print(f"  [WARN] {src}: {e}")
                continue

        # Dedup groups across pickles: keep first per (und, dy, expiry),
        # sorted by source_pickle to match original backtester's dedup behavior.
        all_groups.sort(key=lambda g: (g["und"], g["dy"], g["expiry"],
                                       g["source_pickle"]))
        seen = set()
        deduped = []
        for g in all_groups:
            key = (g["und"], g["dy"], g["expiry"])
            if key not in seen:
                seen.add(key)
                deduped.append(g)

        self.groups = deduped

        # Estimate memory usage
        total_rows = sum(len(g["day_opt"]) for g in self.groups)
        self.memory_mb = total_rows * 80 / (1024 * 1024)  # ~80 bytes/row estimate

        elapsed = time.time() - t0
        print(f"[PRELOAD] Done: {len(self.groups):,} groups | "
              f"{total_rows:,} data rows | ~{self.memory_mb:.0f} MB | "
              f"{elapsed:.1f}s\n")


# =============================================================================
# STEP 2 - FAST SIMULATION WITH PARAMETER PATCHING
# =============================================================================

def _patch_globals(params: Dict[str, Any]):
    """Monkey-patch the backtester module's globals with trial parameters."""
    bt.ENTRY_TIME_IST             = params["ENTRY_TIME_IST"]
    bt.ENTRY_TIME                 = bt.parse_hhmm(params["ENTRY_TIME_IST"])
    bt.LOSS_LIMIT_PCT             = float(params["LOSS_LIMIT_PCT"])
    bt.PROFIT_PROTECT_TRIGGER_PCT = float(params["PROFIT_PROTECT_TRIGGER_PCT"])
    bt.MAX_STOPLOSS_RUPEES        = float(params["MAX_STOPLOSS_RUPEES"])
    bt.MAX_REATTEMPTS             = int(params["MAX_REATTEMPTS"])
    bt.REENTRY_DELAY_MINUTES      = int(params["REENTRY_DELAY_MINUTES"])


def _validate_entry_time(entry_time_str: str) -> bool:
    """Check that entry time falls within the trading session."""
    t = bt.parse_hhmm(entry_time_str)
    return bt.SESSION_START_IST <= t <= bt.SESSION_END_IST


def run_simulation(data: PreloadedData, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Run the full backtest with given params against pre-loaded data.
    Returns a dict of scalar performance metrics.
    """
    # Skip obviously invalid entry times
    if not _validate_entry_time(params["ENTRY_TIME_IST"]):
        return _empty_metrics(params)

    _patch_globals(params)

    all_trade_rows: List[Dict] = []

    for g in data.groups:
        try:
            trades, _ = bt.simulate_day_multi_trades_dhan(
                und=g["und"],
                dy=g["dy"],
                expiry=g["expiry"],
                day_opt=g["day_opt"],
                source_pickle=g["source_pickle"],
            )
            for t in trades:
                all_trade_rows.append(asdict(t))
        except Exception:
            continue

    if not all_trade_rows:
        return _empty_metrics(params)

    all_trades_df = pd.DataFrame(all_trade_rows)

    # Dedup across pickles (same logic as original backtester)
    if not all_trades_df.empty:
        key_cols = ["underlying", "day", "expiry", "trade_seq", "entry_time"]
        all_trades_df = (
            all_trades_df.sort_values(key_cols + ["source_pickle"])
            .drop_duplicates(subset=key_cols, keep="first")
            .reset_index(drop=True)
        )

    # Build actual trades (one underlying per day, D0/D-1 only)
    actual = bt.build_actual_trades_df(all_trades_df)
    if actual.empty:
        return _empty_metrics(params)

    return _compute_metrics(actual, params)


# =============================================================================
# METRICS
# =============================================================================

# These will be set from a baseline run to scale the balanced score properly
_BASELINE = {
    "total_pnl": 2_000_000,   # will be overwritten by baseline run
    "sharpe": 0.28,
    "max_drawdown": -60_000,
}


def _compute_metrics(actual: pd.DataFrame, params: Dict[str, Any]) -> Dict[str, Any]:
    """Compute all performance metrics from the actual-trades DataFrame."""
    pnl = pd.to_numeric(actual["exit_pnl"], errors="coerce").astype(float)

    daily = actual.groupby("day")["exit_pnl"].sum().reset_index()
    daily_pnls = daily["exit_pnl"].astype(float)

    total_pnl = float(pnl.sum())
    n_trades  = len(actual)
    n_days    = len(daily)

    win_rate_trade = float((pnl > 0).mean()) * 100
    win_rate_daily = float((daily_pnls > 0).mean()) * 100

    avg_daily = float(daily_pnls.mean())
    std_daily = float(daily_pnls.std()) if n_days > 1 else 1.0
    sharpe    = avg_daily / std_daily if std_daily > 0 else 0.0

    worst_day = float(daily_pnls.min()) if n_days > 0 else 0.0
    best_day  = float(daily_pnls.max()) if n_days > 0 else 0.0

    # Max drawdown
    cumulative   = daily_pnls.cumsum()
    peak         = cumulative.cummax()
    max_drawdown = float((cumulative - peak).min())

    # Profit factor (capped to avoid inf)
    gross_profit  = float(pnl[pnl > 0].sum())
    gross_loss    = float(-pnl[pnl < 0].sum())
    if gross_loss > 0:
        profit_factor = min(gross_profit / gross_loss, PROFIT_FACTOR_CAP)
    else:
        profit_factor = PROFIT_FACTOR_CAP if gross_profit > 0 else 0.0

    # Exit reason breakdown
    er = actual["exit_reason"].astype(str).str.upper()
    sl_count  = int(er.eq("STOPLOSS").sum())
    pp_count  = int(er.eq("PROFIT_PROTECT").sum())
    eod_count = int(er.eq("EOD").sum())

    sl_pnl  = float(pnl[er.eq("STOPLOSS")].sum())
    eod_pnl = float(pnl[er.eq("EOD")].sum())
    pp_pnl  = float(pnl[er.eq("PROFIT_PROTECT")].sum())

    sl_pct  = round(100.0 * sl_count / n_trades, 1) if n_trades > 0 else 0.0
    eod_pct = round(100.0 * eod_count / n_trades, 1) if n_trades > 0 else 0.0
    pp_pct  = round(100.0 * pp_count / n_trades, 1) if n_trades > 0 else 0.0

    # Reattempt stats
    re_mask        = actual["trade_seq"] > 1
    reattempt_pnl  = float(pnl[re_mask].sum())
    reattempt_count = int(re_mask.sum())

    # Loss-day severity
    loss_days_gt_10k = int((daily_pnls <= -10000).sum())
    loss_days_gt_15k = int((daily_pnls <= -15000).sum())

    avg_loss_on_loss_days  = float(daily_pnls[daily_pnls < 0].mean()) \
                             if (daily_pnls < 0).any() else 0.0
    avg_profit_on_win_days = float(daily_pnls[daily_pnls > 0].mean()) \
                             if (daily_pnls > 0).any() else 0.0

    return {
        # --- Parameters (for traceability) ---
        **params,
        # --- Primary metrics ---
        "total_pnl":          total_pnl,
        "avg_daily_pnl":      avg_daily,
        "sharpe":             sharpe,
        "win_rate_trade_pct": win_rate_trade,
        "win_rate_daily_pct": win_rate_daily,
        "profit_factor":      profit_factor,
        "max_drawdown":       max_drawdown,
        # --- Counts ---
        "n_trades":        n_trades,
        "n_days":          n_days,
        "sl_count":        sl_count,
        "sl_pct":          sl_pct,
        "pp_count":        pp_count,
        "pp_pct":          pp_pct,
        "eod_count":       eod_count,
        "eod_pct":         eod_pct,
        "reattempt_count": reattempt_count,
        # --- PnL breakdown ---
        "sl_total_pnl":        sl_pnl,
        "eod_total_pnl":       eod_pnl,
        "pp_total_pnl":        pp_pnl,
        "reattempt_total_pnl": reattempt_pnl,
        # --- Risk ---
        "worst_day":              worst_day,
        "best_day":               best_day,
        "loss_days_gt_10k":       loss_days_gt_10k,
        "loss_days_gt_15k":       loss_days_gt_15k,
        "avg_loss_on_loss_days":  avg_loss_on_loss_days,
        "avg_profit_on_win_days": avg_profit_on_win_days,
        # --- Composite (placeholder, filled after baseline is known) ---
        "score_balanced":         0.0,
    }


def _empty_metrics(params: Dict[str, Any]) -> Dict[str, Any]:
    """Return zeroed metrics when no trades are produced."""
    return {
        **params,
        "total_pnl": 0, "avg_daily_pnl": 0, "sharpe": 0,
        "win_rate_trade_pct": 0, "win_rate_daily_pct": 0,
        "profit_factor": 0, "max_drawdown": 0,
        "n_trades": 0, "n_days": 0,
        "sl_count": 0, "sl_pct": 0,
        "pp_count": 0, "pp_pct": 0,
        "eod_count": 0, "eod_pct": 0,
        "reattempt_count": 0,
        "sl_total_pnl": 0, "eod_total_pnl": 0,
        "pp_total_pnl": 0, "reattempt_total_pnl": 0,
        "worst_day": 0, "best_day": 0,
        "loss_days_gt_10k": 0, "loss_days_gt_15k": 0,
        "avg_loss_on_loss_days": 0, "avg_profit_on_win_days": 0,
        "score_balanced": -999,
    }


def _compute_balanced_score(m: Dict[str, Any]) -> float:
    """
    Composite score that balances profit with risk.  Uses the baseline run
    (your current params) as the normalization anchor so the score adapts to
    your actual data range instead of using hardcoded magic numbers.

    Components (higher = better):
      40%  total PnL         (normalized: 1.0 = baseline PnL)
      25%  Sharpe ratio      (normalized: 1.0 = baseline Sharpe)
      20%  daily win rate    (already 0-100, mapped to 0-1)
      15%  drawdown penalty  (normalized: -1.0 = baseline drawdown)
    """
    base_pnl = max(abs(_BASELINE["total_pnl"]), 1)
    base_sh  = max(abs(_BASELINE["sharpe"]), 0.01)
    base_dd  = min(_BASELINE["max_drawdown"], -1)   # negative

    pnl_score = m["total_pnl"] / base_pnl
    sh_score  = m["sharpe"] / base_sh
    wr_score  = m["win_rate_daily_pct"] / 100.0
    dd_score  = m["max_drawdown"] / abs(base_dd)     # negative / positive = negative

    return 0.40 * pnl_score + 0.25 * sh_score + 0.20 * wr_score + 0.15 * dd_score


# =============================================================================
# BASELINE RUN  (your current defaults - the "before" benchmark)
# =============================================================================

def run_baseline(data: PreloadedData) -> Dict[str, Any]:
    """Run with original defaults to establish the performance baseline."""
    defaults = {
        "ENTRY_TIME_IST":             bt.ENTRY_TIME_IST,
        "LOSS_LIMIT_PCT":             bt.LOSS_LIMIT_PCT,
        "PROFIT_PROTECT_TRIGGER_PCT": bt.PROFIT_PROTECT_TRIGGER_PCT,
        "MAX_STOPLOSS_RUPEES":        bt.MAX_STOPLOSS_RUPEES,
        "MAX_REATTEMPTS":             bt.MAX_REATTEMPTS,
        "REENTRY_DELAY_MINUTES":      bt.REENTRY_DELAY_MINUTES,
    }
    print("[BASELINE] Running with your current defaults ...")
    t0 = time.time()
    metrics = run_simulation(data, defaults)
    elapsed = time.time() - t0

    # Update global baseline for balanced-score normalization
    if metrics["n_trades"] > 0:
        _BASELINE["total_pnl"]    = max(abs(metrics["total_pnl"]), 1)
        _BASELINE["sharpe"]       = max(abs(metrics["sharpe"]), 0.01)
        _BASELINE["max_drawdown"] = min(metrics["max_drawdown"], -1)

    metrics["score_balanced"] = _compute_balanced_score(metrics)

    print(f"[BASELINE] Done in {elapsed:.1f}s  |  "
          f"PnL=Rs {metrics['total_pnl']:,.0f}  Sharpe={metrics['sharpe']:.3f}  "
          f"WR={metrics['win_rate_daily_pct']:.1f}%  DD=Rs {metrics['max_drawdown']:,.0f}  "
          f"Trades={metrics['n_trades']}")
    print(f"[BASELINE] Params: ET={defaults['ENTRY_TIME_IST']}  "
          f"LL={defaults['LOSS_LIMIT_PCT']}  PP={defaults['PROFIT_PROTECT_TRIGGER_PCT']}  "
          f"MSR={defaults['MAX_STOPLOSS_RUPEES']}  MR={defaults['MAX_REATTEMPTS']}  "
          f"RDM={defaults['REENTRY_DELAY_MINUTES']}")
    print(f"[BASELINE] This is your 'before' - optimizer must beat this.\n")
    return metrics


# =============================================================================
# CHECKPOINT  (crash recovery)
# =============================================================================

def _checkpoint_path() -> str:
    return str(Path.home() / "Downloads" / "optimizer_checkpoint.csv")


def _save_checkpoint(results_list: List[Dict], trial_num: int):
    """Save intermediate results to CSV so progress survives crashes."""
    if not results_list:
        return
    try:
        df = pd.DataFrame(results_list)
        df.to_csv(_checkpoint_path(), index=False)
    except Exception:
        pass  # non-fatal


# =============================================================================
# STEP 3A - BAYESIAN OPTIMIZATION (optuna)
# =============================================================================

def run_bayesian(data: PreloadedData, n_trials: int, objective_col: str) -> pd.DataFrame:
    """Run optuna-based Bayesian optimization."""
    try:
        import optuna
        optuna.logging.set_verbosity(optuna.logging.WARNING)
    except ImportError:
        print("[ERROR] optuna is not installed.  Install it with:\n"
              "    pip install optuna\n")
        sys.exit(1)

    results_list: List[Dict] = []
    best_value = -float("inf")
    trial_times: List[float] = []

    def objective(trial: "optuna.Trial") -> float:
        nonlocal best_value

        t0 = time.time()

        params = {
            "ENTRY_TIME_IST": trial.suggest_categorical(
                "ENTRY_TIME_IST", BAYESIAN_SPACE["ENTRY_TIME_IST"]
            ),
            "LOSS_LIMIT_PCT": round(trial.suggest_float(
                "LOSS_LIMIT_PCT",
                BAYESIAN_SPACE["LOSS_LIMIT_PCT"][0],
                BAYESIAN_SPACE["LOSS_LIMIT_PCT"][1],
                step=0.01,
            ), 2),
            "PROFIT_PROTECT_TRIGGER_PCT": round(trial.suggest_float(
                "PROFIT_PROTECT_TRIGGER_PCT",
                BAYESIAN_SPACE["PROFIT_PROTECT_TRIGGER_PCT"][0],
                BAYESIAN_SPACE["PROFIT_PROTECT_TRIGGER_PCT"][1],
                step=0.01,
            ), 2),
            "MAX_STOPLOSS_RUPEES": trial.suggest_categorical(
                "MAX_STOPLOSS_RUPEES", BAYESIAN_SPACE["MAX_STOPLOSS_RUPEES"]
            ),
            "MAX_REATTEMPTS": trial.suggest_int(
                "MAX_REATTEMPTS",
                BAYESIAN_SPACE["MAX_REATTEMPTS"][0],
                BAYESIAN_SPACE["MAX_REATTEMPTS"][1],
            ),
            "REENTRY_DELAY_MINUTES": trial.suggest_int(
                "REENTRY_DELAY_MINUTES",
                BAYESIAN_SPACE["REENTRY_DELAY_MINUTES"][0],
                BAYESIAN_SPACE["REENTRY_DELAY_MINUTES"][1],
            ),
        }

        metrics = run_simulation(data, params)
        metrics["score_balanced"] = _compute_balanced_score(metrics)
        results_list.append(metrics)

        value = float(metrics.get(objective_col, 0))

        elapsed_trial = time.time() - t0
        trial_times.append(elapsed_trial)

        # Print ETA after first trial
        if trial.number == 0:
            eta = elapsed_trial * n_trials / 60
            print(f"  Trial 0 took {elapsed_trial:.1f}s  ->  estimated total: "
                  f"~{eta:.0f} min for {n_trials} trials\n")

        # Live progress
        if value > best_value and metrics["n_trades"] > 0:
            best_value = value
            p = params
            print(f"  * Trial {trial.number:>3d} NEW BEST  "
                  f"{objective_col}={value:>12,.2f}  |  "
                  f"PnL=Rs {metrics['total_pnl']:>10,.0f}  "
                  f"Sharpe={metrics['sharpe']:.3f}  "
                  f"WR={metrics['win_rate_daily_pct']:.1f}%  "
                  f"DD=Rs {metrics['max_drawdown']:>8,.0f}  |  "
                  f"ET={p['ENTRY_TIME_IST']} "
                  f"LL={p['LOSS_LIMIT_PCT']} "
                  f"PP={p['PROFIT_PROTECT_TRIGGER_PCT']} "
                  f"MSR={p['MAX_STOPLOSS_RUPEES']} "
                  f"MR={p['MAX_REATTEMPTS']} "
                  f"RDM={p['REENTRY_DELAY_MINUTES']}")
        elif trial.number % 25 == 0 and trial.number > 0:
            avg_t = np.mean(trial_times)
            remaining = (n_trials - trial.number - 1) * avg_t / 60
            print(f"  Trial {trial.number:>3d}/{n_trials}  "
                  f"{objective_col}={value:>12,.2f}  "
                  f"PnL=Rs {metrics['total_pnl']:>10,.0f}  "
                  f"[{avg_t:.1f}s/trial, ~{remaining:.0f}m left]")

        # Checkpoint
        if (trial.number + 1) % CHECKPOINT_EVERY == 0:
            _save_checkpoint(results_list, trial.number)

        return value

    study = optuna.create_study(
        direction="maximize",
        sampler=optuna.samplers.TPESampler(seed=42),
    )
    print(f"[BAYESIAN] Starting {n_trials} trials, optimizing: {objective_col}")
    print(f"[BAYESIAN] Checkpoint saved every {CHECKPOINT_EVERY} trials to: "
          f"{_checkpoint_path()}\n")

    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)

    return pd.DataFrame(results_list)


# =============================================================================
# STEP 3B - GRID SEARCH
# =============================================================================

def run_grid(data: PreloadedData, objective_col: str) -> pd.DataFrame:
    """Exhaustive grid search over GRID_SPACE."""
    keys = list(GRID_SPACE.keys())
    combos = list(itertools.product(*[GRID_SPACE[k] for k in keys]))
    total = len(combos)
    print(f"[GRID] {total:,} combinations to evaluate")
    print(f"[GRID] Checkpoint saved every {CHECKPOINT_EVERY} trials to: "
          f"{_checkpoint_path()}\n")

    results_list: List[Dict] = []
    best_value = -float("inf")
    t_start = time.time()
    trial_times: List[float] = []

    for i, vals in enumerate(combos):
        t0 = time.time()
        params = dict(zip(keys, vals))
        metrics = run_simulation(data, params)
        metrics["score_balanced"] = _compute_balanced_score(metrics)
        results_list.append(metrics)
        elapsed_trial = time.time() - t0
        trial_times.append(elapsed_trial)

        value = float(metrics.get(objective_col, 0))

        # Print ETA after first 5 trials
        if i == 4:
            avg_t = np.mean(trial_times)
            eta = avg_t * (total - 5) / 60
            print(f"  Avg {avg_t:.1f}s/trial  ->  estimated total: ~{eta:.0f} min\n")

        if value > best_value and metrics["n_trades"] > 0:
            best_value = value
            p = params
            print(f"  * [{i+1:>5d}/{total}] NEW BEST  "
                  f"{objective_col}={value:>12,.2f}  |  "
                  f"PnL=Rs {metrics['total_pnl']:>10,.0f}  "
                  f"Sharpe={metrics['sharpe']:.3f}  "
                  f"WR={metrics['win_rate_daily_pct']:.1f}%  |  "
                  f"ET={p['ENTRY_TIME_IST']} LL={p['LOSS_LIMIT_PCT']} "
                  f"PP={p['PROFIT_PROTECT_TRIGGER_PCT']} "
                  f"MSR={p['MAX_STOPLOSS_RUPEES']} MR={p['MAX_REATTEMPTS']} "
                  f"RDM={p['REENTRY_DELAY_MINUTES']}")
        elif (i + 1) % 200 == 0:
            wall = time.time() - t_start
            avg_t = np.mean(trial_times)
            remaining = avg_t * (total - i - 1) / 60
            print(f"  [{i+1:>5d}/{total}]  "
                  f"elapsed={wall/60:.1f}m  ETA={remaining:.0f}m  "
                  f"best {objective_col}={best_value:,.2f}")

        # Checkpoint
        if (i + 1) % CHECKPOINT_EVERY == 0:
            _save_checkpoint(results_list, i)

    return pd.DataFrame(results_list)


# =============================================================================
# STEP 4 - OUTPUT
# =============================================================================

PARAM_COLS = [
    "ENTRY_TIME_IST", "LOSS_LIMIT_PCT", "PROFIT_PROTECT_TRIGGER_PCT",
    "MAX_STOPLOSS_RUPEES", "MAX_REATTEMPTS", "REENTRY_DELAY_MINUTES",
]

def write_results(results_df: pd.DataFrame, baseline: Dict[str, Any],
                  objective_col: str, output_path: str):
    """Write ranked results to Excel + best params to text file."""
    if results_df.empty:
        print("[WARN] No results to write.")
        return

    # Sort by objective (descending)
    results_df = (
        results_df.sort_values(objective_col, ascending=False)
        .reset_index(drop=True)
    )
    results_df.index.name = "rank"
    results_df.index += 1  # 1-based rank

    # Put parameter columns first
    metric_cols = [c for c in results_df.columns if c not in PARAM_COLS]
    results_df = results_df[PARAM_COLS + metric_cols]

    # --- Console: Top 20 ---
    print("\n" + "=" * 110)
    print("TOP 20 PARAMETER COMBINATIONS")
    print("=" * 110)
    display_cols = PARAM_COLS + [
        "total_pnl", "sharpe", "win_rate_daily_pct",
        "max_drawdown", "profit_factor", "score_balanced",
    ]
    display_cols = [c for c in display_cols if c in results_df.columns]
    print(results_df.head(20)[display_cols].to_string())

    # --- Console: Best vs Baseline ---
    best = results_df.iloc[0]
    print("\n" + "=" * 110)
    print("  BEST PARAMETERS  vs  YOUR CURRENT DEFAULTS (baseline)")
    print("=" * 110)
    hdr = f"  {'':35s}  {'BEST':>15s}  {'BASELINE':>15s}  {'CHANGE':>15s}"
    print(hdr)
    print("  " + "-" * 85)
    for p in PARAM_COLS:
        b_val = baseline.get(p, "?")
        print(f"  {p:35s}  {str(best[p]):>15s}  {str(b_val):>15s}")
    print("  " + "-" * 85)
    for m in ["total_pnl", "avg_daily_pnl", "sharpe", "win_rate_daily_pct",
              "max_drawdown", "profit_factor"]:
        bv = baseline.get(m, 0)
        nv = best[m]
        if m in ("total_pnl", "avg_daily_pnl", "max_drawdown"):
            delta = f"Rs {nv - bv:+,.0f}"
            print(f"  {m:35s}  Rs {nv:>13,.0f}  Rs {bv:>13,.0f}  {delta:>15s}")
        elif m in ("win_rate_daily_pct",):
            delta = f"{nv - bv:+.1f}%"
            print(f"  {m:35s}  {nv:>14.1f}%  {bv:>14.1f}%  {delta:>15s}")
        else:
            delta = f"{nv - bv:+.3f}"
            print(f"  {m:35s}  {nv:>15.3f}  {bv:>15.3f}  {delta:>15s}")

    # --- Excel ---
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    with pd.ExcelWriter(output_path, engine="openpyxl") as xw:
        results_df.to_excel(xw, sheet_name="all_trials_ranked", index=True)

        # Top 10 transposed for easy reading
        results_df.head(10).T.to_excel(xw, sheet_name="top_10_detail")

        # Baseline comparison
        base_df = pd.DataFrame([baseline])
        base_df.to_excel(xw, sheet_name="baseline_current", index=False)

        # Parameter sensitivity: average metric per parameter value
        for p in PARAM_COLS:
            try:
                sens = (
                    results_df.groupby(p, as_index=False)
                    .agg(
                        trials=("total_pnl", "count"),
                        avg_total_pnl=("total_pnl", "mean"),
                        best_total_pnl=("total_pnl", "max"),
                        avg_sharpe=("sharpe", "mean"),
                        avg_win_rate=("win_rate_daily_pct", "mean"),
                        avg_max_dd=("max_drawdown", "mean"),
                        avg_score=("score_balanced", "mean"),
                    )
                    .sort_values("avg_total_pnl", ascending=False)
                )
                # Excel sheet names: max 31 chars
                sheet_name = f"sens_{p}"[:31]
                sens.to_excel(xw, sheet_name=sheet_name, index=False)
            except Exception:
                pass

        wb = xw.book
        for ws in wb.worksheets:
            ws.freeze_panes = "A2"

    print(f"\n[SAVED] {output_path}")

    # --- Best params text file ---
    txt_path = output_path.replace(".xlsx", "_best_params.txt")
    with open(txt_path, "w") as f:
        f.write("# =============================================\n")
        f.write("# Best parameters found by optimizer\n")
        f.write("# =============================================\n")
        f.write(f"# Objective:       {objective_col}\n")
        f.write(f"# Total PnL:       Rs {best['total_pnl']:,.0f}\n")
        f.write(f"# Sharpe:          {best['sharpe']:.3f}\n")
        f.write(f"# Daily Win Rate:  {best['win_rate_daily_pct']:.1f}%\n")
        f.write(f"# Max Drawdown:    Rs {best['max_drawdown']:,.0f}\n")
        f.write(f"# Profit Factor:   {best['profit_factor']:.2f}\n")
        f.write(f"# Trades:          {best['n_trades']:.0f}\n\n")

        f.write("# --- Windows CMD (set before running backtester) ---\n")
        for p in PARAM_COLS:
            f.write(f"set {p}={best[p]}\n")

        f.write("\n# --- PowerShell ---\n")
        for p in PARAM_COLS:
            f.write(f'$env:{p}="{best[p]}"\n')

        f.write("\n# --- Python os.environ ---\n")
        for p in PARAM_COLS:
            f.write(f'os.environ["{p}"] = "{best[p]}"\n')

        f.write("\n\n# =============================================\n")
        f.write("# BASELINE (your current defaults for reference)\n")
        f.write("# =============================================\n")
        f.write(f"# Total PnL:       Rs {baseline.get('total_pnl', 0):,.0f}\n")
        f.write(f"# Sharpe:          {baseline.get('sharpe', 0):.3f}\n")
        f.write(f"# Daily Win Rate:  {baseline.get('win_rate_daily_pct', 0):.1f}%\n")
        f.write(f"# Max Drawdown:    Rs {baseline.get('max_drawdown', 0):,.0f}\n")
    print(f"[SAVED] {txt_path}")

    # Clean up checkpoint
    try:
        cp = _checkpoint_path()
        if os.path.exists(cp):
            os.remove(cp)
            print(f"[CLEANUP] Removed checkpoint: {cp}")
    except Exception:
        pass


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Short-straddle parameter optimizer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python optimize_straddle_params_bayesian.py                          # Quick start
  python optimize_straddle_params_bayesian.py --mode grid              # Exhaustive
  python optimize_straddle_params_bayesian.py --trials 300 --objective sharpe
  python optimize_straddle_params_bayesian.py --lookback-months 12     # Recent data only
        """,
    )
    parser.add_argument(
        "--mode", choices=["bayesian", "grid"], default="bayesian",
        help="Optimization mode (default: bayesian)",
    )
    parser.add_argument(
        "--trials", type=int, default=200,
        help="Number of Bayesian trials (ignored for grid). Default: 200",
    )
    parser.add_argument(
        "--objective", default="score_balanced",
        choices=["total_pnl", "sharpe", "win_rate_daily_pct",
                 "profit_factor", "score_balanced"],
        help="Metric to maximize. Default: score_balanced",
    )
    parser.add_argument(
        "--pickles-dir", default=None,
        help="Override pickle directory (default: from backtester config)",
    )
    parser.add_argument(
        "--lookback-months", type=int, default=None,
        help="Override lookback window in months (default: from backtester config)",
    )
    parser.add_argument(
        "--output", default=None,
        help="Output Excel path (default: ~/Downloads/optimizer_results_...xlsx)",
    )
    args = parser.parse_args()

    pickles_dir    = args.pickles_dir or bt.PICKLES_DIR
    lookback       = args.lookback_months or bt.LOOKBACK_MONTHS
    output_path    = args.output or str(
        Path.home() / "Downloads"
        / f"optimizer_results_{args.mode}_{args.objective}.xlsx"
    )

    # --- Banner ---
    print("=" * 80)
    print("  SHORT-STRADDLE PARAMETER OPTIMIZER  v2")
    print("=" * 80)
    print(f"  Mode:            {args.mode}")
    print(f"  Objective:       {args.objective}")
    if args.mode == "bayesian":
        print(f"  Trials:          {args.trials}")
    else:
        total_grid = 1
        for v in GRID_SPACE.values():
            total_grid *= len(v)
        print(f"  Grid combos:     {total_grid:,}")
    print(f"  Pickles dir:     {pickles_dir}")
    print(f"  Lookback:        {lookback} months")
    print(f"  Output:          {output_path}")
    print(f"  Checkpoint:      {_checkpoint_path()}")
    print()

    # --- Discover data range ---
    paths = sorted(
        glob.glob(os.path.join(pickles_dir, "*.pkl"))
        + glob.glob(os.path.join(pickles_dir, "*.pickle"))
    )
    if not paths:
        print(f"[ERROR] No .pkl/.pickle files found in: {pickles_dir}")
        sys.exit(1)

    end_day = bt.discover_data_max_day(paths) or date.today()
    window_start = bt.compute_window_start(end_day, lookback)
    print(f"  Data window:     {window_start}  ->  {end_day}")
    print()

    # --- STEP 1: Pre-load data ---
    data = PreloadedData(pickles_dir, window_start, end_day)
    if not data.groups:
        print("[ERROR] No tradeable (underlying, day, expiry) groups found in data.")
        sys.exit(1)

    # --- STEP 2: Baseline run ---
    baseline = run_baseline(data)

    # --- STEP 3: Optimize ---
    t_start = time.time()

    if args.mode == "bayesian":
        results_df = run_bayesian(data, args.trials, args.objective)
    else:
        results_df = run_grid(data, args.objective)

    elapsed = time.time() - t_start
    print(f"\n[DONE] {len(results_df)} trials completed in {elapsed/60:.1f} minutes")

    # Ensure balanced scores are computed for all results
    # (they should already be set during the run, but recompute for safety)
    scores = results_df.apply(
        lambda row: _compute_balanced_score(row.to_dict()), axis=1
    )
    results_df["score_balanced"] = scores

    # --- STEP 4: Output ---
    write_results(results_df, baseline, args.objective, output_path)

    print("\n" + "=" * 80)
    print("  IMPORTANT: Optimized parameters may overfit to historical data.")
    print("  Check the sensitivity sheets - if top-5 results share similar")
    print("  parameter ranges, the finding is robust.  If the #1 result is an")
    print("  outlier with very different params from #2-#10, treat with caution.")
    print("=" * 80)


if __name__ == "__main__":
    main()
