#!/usr/bin/env python3
"""
OPTIMIZER FOR OTM LEG-WISE STRADDLE STRATEGY
============================================

This script is designed for the backtester in:
    otm_straddle_legwise_reattempt_final_commented.py

It optimizes these strategy variables:
    1. ENTRY_TIME_IST
    2. MAX_REATTEMPTS
    3. REENTRY_DELAY_MINUTES
    4. OTM_DISTANCE_STEPS
    5. LEG_PREMIUM_RISE_EXIT_PCT

Methodology
-----------
1. Data is preloaded once from the option pickles.
2. Underlying minute data is downloaded once from Kite and cached locally,
   then reused across optimizer runs.
3. Each trial monkey-patches only the required strategy globals and reuses the
   already-loaded day groups.
4. Two optimization modes are available:
   - bayesian : Optuna TPE sampler maximizing a robust composite score.
   - pareto   : Optuna NSGA-II multi-objective search maximizing
                (median fold PnL, median fold daily win-rate).
5. Walk-forward fold statistics are computed on chronological folds so the
   optimizer prefers parameter sets that are profitable and stable, not merely
   lucky on one subperiod.

Notes
-----
- The optimizer mirrors the current strategy engine's semantics:
  one underlying per trading day, 0-DTE/1-DTE only, leg-wise exits,
  retry slots consumed on failed entry attempts, and retry scheduling after the
  later of the two leg exits.
- Duplicate (underlying, day, expiry) groups across pickles are handled the
  same way as the strategy file: first occurrence wins.
- This optimizer does NOT change the execution logic of the backtester. It only
  searches parameter values.

Requirements
------------
    pip install optuna pandas numpy openpyxl

Examples
--------
    python optimize_otm_straddle_params_optuna.py
    python optimize_otm_straddle_params_optuna.py --trials 250
    python optimize_otm_straddle_params_optuna.py --mode pareto --trials 300
    python optimize_otm_straddle_params_optuna.py --objective total_pnl
    python optimize_otm_straddle_params_optuna.py --lookback-months 12
    python optimize_otm_straddle_params_optuna.py --refresh-underlying-cache
"""

from __future__ import annotations

import argparse
import glob
import hashlib
import importlib.util
import itertools
import os
import sys
import time
import warnings
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")


# =============================================================================
# SEARCH SPACE CONFIGURATION
# =============================================================================
# Adjust these ranges if you want a wider or narrower search.
# ENTRY_TIME_IST must remain within the session declared by the backtester.
BAYESIAN_SPACE: Dict[str, Any] = {
    "ENTRY_TIME_IST": [
        "09:15", "09:20", "09:25", "09:30", "09:35", "09:40",
        "09:45", "09:50", "09:55", "10:00", "10:05", "10:10",
        "10:15", "10:20", "10:25", "10:30",
    ],
    "MAX_REATTEMPTS": (0, 6),
    "REENTRY_DELAY_MINUTES": (1, 45),
    "OTM_DISTANCE_STEPS": (1, 8),
    "LEG_PREMIUM_RISE_EXIT_PCT": (5.0, 80.0),
}

GRID_SPACE: Dict[str, List[Any]] = {
    "ENTRY_TIME_IST": ["09:20", "09:25", "09:30", "09:35", "09:40", "09:45", "10:00"],
    "MAX_REATTEMPTS": [0, 1, 2, 3, 4],
    "REENTRY_DELAY_MINUTES": [1, 5, 10, 15, 20, 30],
    "OTM_DISTANCE_STEPS": [1, 2, 3, 4, 5, 6],
    "LEG_PREMIUM_RISE_EXIT_PCT": [10.0, 15.0, 20.0, 25.0, 30.0, 40.0, 50.0],
}

CHECKPOINT_EVERY = 10
PROFIT_FACTOR_CAP = 99.99
DEFAULT_FOLDS = 5

# Trials with zero executed trades are not useful for optimization.
# They are explicitly pushed to the bottom of the ranking so they do not beat
# losing-but-real strategies merely because their PnL is exactly zero.
INVALID_TRIAL_PENALTY = -1_000_000_000.0

PARAM_COLS = [
    "ENTRY_TIME_IST",
    "MAX_REATTEMPTS",
    "REENTRY_DELAY_MINUTES",
    "OTM_DISTANCE_STEPS",
    "LEG_PREMIUM_RISE_EXIT_PCT",
]


# =============================================================================
# MODULE LOADING
# =============================================================================
def load_strategy_module(strategy_file: str):
    """Load the strategy/backtester module from an explicit file path."""
    strategy_path = os.path.abspath(strategy_file)
    if not os.path.exists(strategy_path):
        raise FileNotFoundError(f"Strategy file not found: {strategy_path}")

    module_name = Path(strategy_path).stem
    spec = importlib.util.spec_from_file_location(module_name, strategy_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module spec from: {strategy_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


# =============================================================================
# PATHS / CACHE HELPERS
# =============================================================================
def safe_fname_part(s: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in str(s))


def get_downloads_folder() -> Path:
    downloads = Path.home() / "Downloads"
    return downloads if downloads.exists() else Path.home()


def checkpoint_path(mode: str, objective: str) -> Path:
    return get_downloads_folder() / f"optimizer_checkpoint_{safe_fname_part(mode)}_{safe_fname_part(objective)}.csv"


def make_underlying_cache_path(strategy_file: str, pickles_dir: str, window_start: date, window_end: date) -> Path:
    key = f"{os.path.abspath(strategy_file)}|{os.path.abspath(pickles_dir)}|{window_start}|{window_end}"
    digest = hashlib.md5(key.encode("utf-8")).hexdigest()[:12]
    return get_downloads_folder() / f"optimizer_underlying_cache_{digest}.pkl"


# =============================================================================
# PRELOADED DATA
# =============================================================================
class PreloadedData:
    """
    Holds all day groups needed by the strategy engine, already filtered to:
    - one selected underlying per day
    - nearest expiry for that underlying/day
    - 0-DTE / 1-DTE only
    - requested lookback window

    It also holds:
    - min_expiry_map from pass-1
    - actual_underlying_by_day mapping
    - list of unique trade days for fold construction
    """

    def __init__(
        self,
        *,
        bt,
        pickles_dir: str,
        window_start: date,
        window_end: date,
        min_expiry_map: Dict[Tuple[str, date], date],
        actual_underlying_by_day: Dict[date, str],
    ):
        self.bt = bt
        self.pickles_dir = pickles_dir
        self.window_start = window_start
        self.window_end = window_end
        self.min_expiry_map = min_expiry_map
        self.actual_underlying_by_day = actual_underlying_by_day

        self.groups: List[Dict[str, Any]] = []
        self.trade_days: List[date] = []
        self.paths: List[str] = []
        self.total_rows = 0
        self.n_pickles = 0

        self._load()

    def _load(self) -> None:
        bt = self.bt
        paths = sorted(
            glob.glob(os.path.join(self.pickles_dir, "*.pkl"))
            + glob.glob(os.path.join(self.pickles_dir, "*.pickle"))
        )
        if not paths:
            raise FileNotFoundError(f"No .pkl/.pickle files found in: {self.pickles_dir}")

        self.paths = paths
        self.n_pickles = len(paths)
        processed_day_keys: set[Tuple[str, date, date]] = set()
        groups: List[Dict[str, Any]] = []

        print(f"[PRELOAD] Loading {len(paths)} option pickle files ...")
        t0 = time.time()

        for i, p in enumerate(paths, start=1):
            try:
                df = pd.read_pickle(p)
                if not isinstance(df, pd.DataFrame) or df.empty:
                    continue

                needed_cols = [
                    "date", "name", "type", "option_type", "strike", "expiry",
                    "instrument", "high", "close",
                ]
                missing = [c for c in needed_cols if c not in df.columns]
                if missing:
                    raise ValueError(f"Missing columns {missing} in {p}")

                d2 = df[df["type"].astype(str).str.upper().eq("OPTION")][needed_cols].copy()
                if d2.empty:
                    continue

                d2["date"] = bt.ensure_ist(d2["date"])
                d2["day"] = d2["date"].dt.date
                d2["underlying"] = d2["name"].astype(str).map(bt.normalize_underlying)
                d2 = d2[d2["underlying"].isin(bt.TRADEABLE)]
                if d2.empty:
                    continue

                d2["expiry_date"] = pd.to_datetime(d2["expiry"], errors="coerce").dt.date
                d2["strike_num"] = pd.to_numeric(d2["strike"], errors="coerce")
                d2["strike_int"] = d2["strike_num"].round().astype("Int64")
                d2["option_type"] = d2["option_type"].astype(str).str.upper()
                d2 = d2.dropna(subset=["day", "underlying", "expiry_date", "strike_int", "close"])
                if d2.empty:
                    continue
                d2["strike_int"] = d2["strike_int"].astype(int)

                d2 = d2[d2["expiry_date"] >= d2["day"]]
                if d2.empty:
                    continue

                d2["days_to_expiry"] = (
                    pd.to_datetime(d2["expiry_date"]) - pd.to_datetime(d2["day"])
                ).dt.days
                d2 = d2[d2["days_to_expiry"].isin([0, 1])]
                if d2.empty:
                    continue

                d2 = d2[(d2["day"] >= self.window_start) & (d2["day"] <= self.window_end)]
                if d2.empty:
                    continue

                for (und, dy, ex), g in d2.groupby(["underlying", "day", "expiry_date"], sort=False):
                    key_ud = (und, dy)
                    if key_ud not in self.min_expiry_map:
                        continue
                    if self.min_expiry_map[key_ud] != ex:
                        continue
                    if self.actual_underlying_by_day.get(dy) != und:
                        continue

                    day_key = (und, dy, ex)
                    if day_key in processed_day_keys:
                        continue
                    processed_day_keys.add(day_key)

                    groups.append({
                        "und": und,
                        "dy": dy,
                        "expiry": ex,
                        "day_opt": g.copy(),
                        "source_pickle": os.path.basename(p),
                    })
                    self.total_rows += len(g)

                if i % 5 == 0 or i == len(paths):
                    print(f"  ... {i}/{len(paths)} files processed")

            except Exception as e:
                print(f"  [WARN] {os.path.basename(p)}: {e}")
                continue

        groups.sort(key=lambda x: (x["dy"], x["und"], x["expiry"]))
        self.groups = groups
        self.trade_days = sorted({g["dy"] for g in groups})

        elapsed = time.time() - t0
        print(
            f"[PRELOAD] Done: {len(self.groups):,} day-groups | {len(self.trade_days):,} trade days | "
            f"{self.total_rows:,} rows | {elapsed:.1f}s\n"
        )


# =============================================================================
# FOLD HELPERS
# =============================================================================
def build_time_folds(trade_days: Sequence[date], n_folds: int) -> List[set[date]]:
    """
    Split chronological trade days into contiguous folds.
    Empty folds are dropped.
    """
    if not trade_days:
        return []

    n_folds = max(1, min(int(n_folds), len(trade_days)))
    indices = np.array_split(np.arange(len(trade_days)), n_folds)
    folds: List[set[date]] = []
    for idx in indices:
        if len(idx) == 0:
            continue
        folds.append({trade_days[i] for i in idx.tolist()})
    return folds


# =============================================================================
# UNDERLYING DATA CACHE
# =============================================================================
def load_or_build_underlying_data(
    *,
    bt,
    strategy_file: str,
    pickles_dir: str,
    window_start: date,
    window_end: date,
    refresh: bool,
) -> Dict[str, pd.DataFrame]:
    """
    Load underlying minute data from local cache if available, else download once
    through the backtester's Kite helper and cache it.
    """
    cache_path = make_underlying_cache_path(strategy_file, pickles_dir, window_start, window_end)

    if cache_path.exists() and not refresh:
        print(f"[CACHE] Loading underlying cache: {cache_path}")
        data = pd.read_pickle(cache_path)
        if isinstance(data, dict):
            return data
        raise ValueError(f"Underlying cache is invalid: {cache_path}")

    print("[STEP] Initializing Kite for one-time underlying download ...")
    kite = bt.oUtils.intialize_kite_api()
    print("[OK] Kite ready.")

    underlying_data = bt.download_underlyings(kite, window_start, window_end)
    pd.to_pickle(underlying_data, cache_path)
    print(f"[CACHE] Saved underlying cache: {cache_path}\n")
    return underlying_data


# =============================================================================
# PARAMETER PATCHING
# =============================================================================
def patch_strategy_globals(bt, params: Dict[str, Any]) -> None:
    """Patch only the strategy globals that are part of the search space."""
    bt.ENTRY_TIME_IST = str(params["ENTRY_TIME_IST"])
    bt.ENTRY_TIME = bt.parse_hhmm(bt.ENTRY_TIME_IST)
    bt.MAX_REATTEMPTS = int(params["MAX_REATTEMPTS"])
    bt.REENTRY_DELAY_MINUTES = int(params["REENTRY_DELAY_MINUTES"])
    bt.OTM_DISTANCE_STEPS = int(params["OTM_DISTANCE_STEPS"])
    bt.LEG_PREMIUM_RISE_EXIT_PCT = float(params["LEG_PREMIUM_RISE_EXIT_PCT"])
    bt.validate_user_config()


# =============================================================================
# METRICS
# =============================================================================
_BASELINE = {
    "total_pnl": 1.0,
    "median_fold_pnl": 1.0,
    "median_fold_daily_win_rate": 50.0,
    "profit_factor": 1.0,
    "max_drawdown": -1.0,
}


def empty_metrics(params: Dict[str, Any]) -> Dict[str, Any]:
    """Return a clearly-invalid result for trials that produce no usable trades."""
    return {
        **params,
        "total_pnl": 0.0,
        "avg_daily_pnl": 0.0,
        "sharpe": 0.0,
        "trade_win_rate_pct": 0.0,
        "daily_win_rate_pct": 0.0,
        "profit_factor": 0.0,
        "max_drawdown": 0.0,
        "worst_day": 0.0,
        "best_day": 0.0,
        "n_trades": 0,
        "n_days": 0,
        "avg_attempts_per_day": 0.0,
        "avg_trade_seq": 0.0,
        "median_fold_pnl": 0.0,
        "median_fold_daily_win_rate": 0.0,
        "median_fold_profit_factor": 0.0,
        "median_fold_max_drawdown": 0.0,
        "fold_pnl_std": 0.0,
        "fold_wr_std": 0.0,
        "score_robust": -999999.0,
        "score_knee": 0.0,
    }


def compute_basic_metrics(actual: pd.DataFrame) -> Dict[str, Any]:
    pnl = pd.to_numeric(actual["exit_pnl"], errors="coerce").fillna(0.0).astype(float)
    daily = actual.groupby("day", as_index=False)["exit_pnl"].sum()
    daily_pnl = daily["exit_pnl"].astype(float)

    total_pnl = float(pnl.sum())
    n_trades = int(len(actual))
    n_days = int(len(daily))

    trade_wr = float((pnl > 0).mean() * 100.0) if n_trades else 0.0
    daily_wr = float((daily_pnl > 0).mean() * 100.0) if n_days else 0.0
    avg_daily_pnl = float(daily_pnl.mean()) if n_days else 0.0

    std_daily = float(daily_pnl.std()) if n_days > 1 else 0.0
    sharpe = float(avg_daily_pnl / std_daily) if std_daily > 0 else 0.0

    cumulative = daily_pnl.cumsum()
    peak = cumulative.cummax()
    max_drawdown = float((cumulative - peak).min()) if n_days else 0.0
    worst_day = float(daily_pnl.min()) if n_days else 0.0
    best_day = float(daily_pnl.max()) if n_days else 0.0

    gross_profit = float(pnl[pnl > 0].sum())
    gross_loss = float(-pnl[pnl < 0].sum())
    if gross_loss > 0:
        profit_factor = min(gross_profit / gross_loss, PROFIT_FACTOR_CAP)
    else:
        profit_factor = PROFIT_FACTOR_CAP if gross_profit > 0 else 0.0

    attempts_per_day = actual.groupby("day")["trade_seq"].max() if n_trades else pd.Series(dtype=float)
    avg_attempts_per_day = float(attempts_per_day.mean()) if not attempts_per_day.empty else 0.0
    avg_trade_seq = float(actual["trade_seq"].mean()) if n_trades else 0.0

    return {
        "total_pnl": total_pnl,
        "avg_daily_pnl": avg_daily_pnl,
        "sharpe": sharpe,
        "trade_win_rate_pct": trade_wr,
        "daily_win_rate_pct": daily_wr,
        "profit_factor": float(profit_factor),
        "max_drawdown": max_drawdown,
        "worst_day": worst_day,
        "best_day": best_day,
        "n_trades": n_trades,
        "n_days": n_days,
        "avg_attempts_per_day": avg_attempts_per_day,
        "avg_trade_seq": avg_trade_seq,
    }


def compute_fold_metrics(actual: pd.DataFrame, folds: Sequence[set[date]]) -> Dict[str, Any]:
    if actual.empty or not folds:
        return {
            "median_fold_pnl": 0.0,
            "median_fold_daily_win_rate": 0.0,
            "median_fold_profit_factor": 0.0,
            "median_fold_max_drawdown": 0.0,
            "fold_pnl_std": 0.0,
            "fold_wr_std": 0.0,
        }

    fold_pnls: List[float] = []
    fold_wrs: List[float] = []
    fold_pfs: List[float] = []
    fold_dds: List[float] = []

    for fold_days in folds:
        sub = actual[actual["day"].isin(fold_days)]
        if sub.empty:
            continue
        m = compute_basic_metrics(sub)
        fold_pnls.append(float(m["total_pnl"]))
        fold_wrs.append(float(m["daily_win_rate_pct"]))
        fold_pfs.append(float(m["profit_factor"]))
        fold_dds.append(float(m["max_drawdown"]))

    if not fold_pnls:
        return {
            "median_fold_pnl": 0.0,
            "median_fold_daily_win_rate": 0.0,
            "median_fold_profit_factor": 0.0,
            "median_fold_max_drawdown": 0.0,
            "fold_pnl_std": 0.0,
            "fold_wr_std": 0.0,
        }

    return {
        "median_fold_pnl": float(np.median(fold_pnls)),
        "median_fold_daily_win_rate": float(np.median(fold_wrs)),
        "median_fold_profit_factor": float(np.median(fold_pfs)),
        "median_fold_max_drawdown": float(np.median(fold_dds)),
        "fold_pnl_std": float(np.std(fold_pnls, ddof=0)),
        "fold_wr_std": float(np.std(fold_wrs, ddof=0)),
    }


def finalize_robust_score(m: Dict[str, Any]) -> float:
    """
    Compute the final single-objective score used for ranking.

    Trials with zero trades or zero trade-days are treated as invalid and receive
    a very large negative score. This prevents empty trials from floating to the
    top when the chosen objective would otherwise treat zero as acceptable.
    """
    if int(m.get("n_trades", 0)) <= 0 or int(m.get("n_days", 0)) <= 0:
        return INVALID_TRIAL_PENALTY
    return compute_robust_score(m)


def compute_robust_score(m: Dict[str, Any]) -> float:
    """
    Composite score for single-objective Bayesian search.

    Goal:
    - favor profit and daily accuracy
    - prefer stable fold performance
    - penalize large drawdowns

    The score is normalized against the baseline strategy defaults so the scale
    adapts to the user's actual dataset.
    """
    base_pnl = max(abs(_BASELINE["median_fold_pnl"]), 1.0)
    base_wr = max(_BASELINE["median_fold_daily_win_rate"], 1.0)
    base_pf = max(_BASELINE["profit_factor"], 0.1)
    base_dd = max(abs(_BASELINE["max_drawdown"]), 1.0)

    pnl_term = m["median_fold_pnl"] / base_pnl
    wr_term = m["median_fold_daily_win_rate"] / 100.0
    pf_term = min(m["profit_factor"] / base_pf, 3.0)
    dd_penalty = abs(m["max_drawdown"]) / base_dd
    stability_penalty = 0.0
    if abs(m["median_fold_pnl"]) > 1e-9:
        stability_penalty += m["fold_pnl_std"] / max(abs(m["median_fold_pnl"]), 1.0)
    stability_penalty += m["fold_wr_std"] / 100.0

    # Slight penalty for excessive retries, to prevent the optimizer from
    # preferring extreme retry-heavy parameter sets only because they overtrade.
    retry_penalty = max(m["avg_attempts_per_day"] - 2.0, 0.0) * 0.05

    score = (
        0.50 * pnl_term
        + 0.25 * wr_term
        + 0.15 * pf_term
        - 0.15 * dd_penalty
        - 0.10 * stability_penalty
        - retry_penalty
    )
    return float(score)


# =============================================================================
# SIMULATION
# =============================================================================
def run_simulation(
    *,
    bt,
    data: PreloadedData,
    underlying_data: Dict[str, pd.DataFrame],
    folds: Sequence[set[date]],
    params: Dict[str, Any],
) -> Dict[str, Any]:
    """Run one full simulation across all selected day groups for one parameter set."""
    try:
        patch_strategy_globals(bt, params)
    except Exception:
        return empty_metrics(params)

    all_trade_rows: List[Dict[str, Any]] = []

    for g in data.groups:
        try:
            uday = underlying_data.get(g["und"])
            if uday is None:
                continue
            uday_sub = uday[uday["day"] == g["dy"]]
            if uday_sub.empty:
                continue

            trades, _ = bt.simulate_day_multi_trades(
                und=g["und"],
                dy=g["dy"],
                expiry=g["expiry"],
                day_opt=g["day_opt"],
                underlying_day=uday_sub,
            )
            for t in trades:
                all_trade_rows.append(asdict(t))
        except Exception:
            continue

    if not all_trade_rows:
        return empty_metrics(params)

    all_trades_df = pd.DataFrame(all_trade_rows)
    if all_trades_df.empty:
        return empty_metrics(params)

    # The preloaded day-groups are already restricted to one selected underlying
    # per day, nearest expiry only. Using build_actual_trades_df keeps the result
    # aligned with the strategy file's reporting semantics.
    actual = bt.build_actual_trades_df(all_trades_df, data.min_expiry_map)
    if actual.empty:
        return empty_metrics(params)

    metrics = {**params}
    metrics.update(compute_basic_metrics(actual))
    metrics.update(compute_fold_metrics(actual, folds))
    metrics["score_robust"] = finalize_robust_score(metrics)
    metrics["score_knee"] = 0.0
    return metrics


# =============================================================================
# BASELINE
# =============================================================================
def run_baseline(bt, data: PreloadedData, underlying_data: Dict[str, pd.DataFrame], folds: Sequence[set[date]]) -> Dict[str, Any]:
    defaults = {
        "ENTRY_TIME_IST": bt.ENTRY_TIME_IST,
        "MAX_REATTEMPTS": int(bt.MAX_REATTEMPTS),
        "REENTRY_DELAY_MINUTES": int(bt.REENTRY_DELAY_MINUTES),
        "OTM_DISTANCE_STEPS": int(bt.OTM_DISTANCE_STEPS),
        "LEG_PREMIUM_RISE_EXIT_PCT": float(bt.LEG_PREMIUM_RISE_EXIT_PCT),
    }

    print("[BASELINE] Running current strategy defaults ...")
    t0 = time.time()
    metrics = run_simulation(bt=bt, data=data, underlying_data=underlying_data, folds=folds, params=defaults)
    elapsed = time.time() - t0

    if metrics["n_trades"] > 0:
        _BASELINE["total_pnl"] = max(abs(metrics["total_pnl"]), 1.0)
        _BASELINE["median_fold_pnl"] = max(abs(metrics["median_fold_pnl"]), 1.0)
        _BASELINE["median_fold_daily_win_rate"] = max(metrics["median_fold_daily_win_rate"], 1.0)
        _BASELINE["profit_factor"] = max(metrics["profit_factor"], 0.1)
        _BASELINE["max_drawdown"] = min(metrics["max_drawdown"], -1.0) if metrics["max_drawdown"] < 0 else -1.0

    metrics["score_robust"] = finalize_robust_score(metrics)

    print(
        f"[BASELINE] Done in {elapsed:.1f}s | PnL=Rs {metrics['total_pnl']:,.0f} | "
        f"Daily WR={metrics['daily_win_rate_pct']:.1f}% | Trades={metrics['n_trades']} | "
        f"MaxDD=Rs {metrics['max_drawdown']:,.0f} | RobustScore={metrics['score_robust']:.3f}\n"
    )
    return metrics


# =============================================================================
# CHECKPOINT
# =============================================================================
def save_checkpoint(results: List[Dict[str, Any]], cp_path: Path) -> None:
    if not results:
        return
    try:
        pd.DataFrame(results).to_csv(cp_path, index=False)
    except Exception:
        pass


# =============================================================================
# OPTIMIZATION MODES
# =============================================================================
def suggest_params_bayesian(trial) -> Dict[str, Any]:
    return {
        "ENTRY_TIME_IST": trial.suggest_categorical("ENTRY_TIME_IST", BAYESIAN_SPACE["ENTRY_TIME_IST"]),
        "MAX_REATTEMPTS": trial.suggest_int("MAX_REATTEMPTS", BAYESIAN_SPACE["MAX_REATTEMPTS"][0], BAYESIAN_SPACE["MAX_REATTEMPTS"][1]),
        "REENTRY_DELAY_MINUTES": trial.suggest_int("REENTRY_DELAY_MINUTES", BAYESIAN_SPACE["REENTRY_DELAY_MINUTES"][0], BAYESIAN_SPACE["REENTRY_DELAY_MINUTES"][1]),
        "OTM_DISTANCE_STEPS": trial.suggest_int("OTM_DISTANCE_STEPS", BAYESIAN_SPACE["OTM_DISTANCE_STEPS"][0], BAYESIAN_SPACE["OTM_DISTANCE_STEPS"][1]),
        "LEG_PREMIUM_RISE_EXIT_PCT": round(
            trial.suggest_float(
                "LEG_PREMIUM_RISE_EXIT_PCT",
                BAYESIAN_SPACE["LEG_PREMIUM_RISE_EXIT_PCT"][0],
                BAYESIAN_SPACE["LEG_PREMIUM_RISE_EXIT_PCT"][1],
                step=1.0,
            ),
            2,
        ),
    }


def run_bayesian(
    *,
    bt,
    data: PreloadedData,
    underlying_data: Dict[str, pd.DataFrame],
    folds: Sequence[set[date]],
    n_trials: int,
    objective_col: str,
    cp_path: Path,
) -> pd.DataFrame:
    try:
        import optuna
        optuna.logging.set_verbosity(optuna.logging.WARNING)
    except ImportError:
        raise ImportError("optuna is not installed. Install it with: pip install optuna")

    results: List[Dict[str, Any]] = []
    best_value = -float("inf")
    trial_times: List[float] = []

    def objective(trial: "optuna.Trial") -> float:
        nonlocal best_value
        t0 = time.time()

        params = suggest_params_bayesian(trial)
        metrics = run_simulation(bt=bt, data=data, underlying_data=underlying_data, folds=folds, params=params)
        metrics["score_robust"] = finalize_robust_score(metrics)
        results.append(metrics)

        value = float(metrics.get(objective_col, 0.0))
        elapsed = time.time() - t0
        trial_times.append(elapsed)

        if trial.number == 0:
            eta = elapsed * n_trials / 60.0
            print(f"  Trial 0 took {elapsed:.1f}s -> estimated total ~{eta:.0f} min for {n_trials} trials\n")

        if metrics["n_trades"] > 0 and value > best_value:
            best_value = value
            print(
                f"  * Trial {trial.number:>3d} NEW BEST {objective_col}={value:,.3f} | "
                f"PnL=Rs {metrics['total_pnl']:,.0f} | Daily WR={metrics['daily_win_rate_pct']:.1f}% | "
                f"DD=Rs {metrics['max_drawdown']:,.0f} | "
                f"ET={params['ENTRY_TIME_IST']} MR={params['MAX_REATTEMPTS']} "
                f"RDM={params['REENTRY_DELAY_MINUTES']} OTM={params['OTM_DISTANCE_STEPS']} "
                f"SL%={params['LEG_PREMIUM_RISE_EXIT_PCT']}"
            )
        elif trial.number > 0 and trial.number % 25 == 0:
            avg_t = float(np.mean(trial_times)) if trial_times else 0.0
            remaining = (n_trials - trial.number - 1) * avg_t / 60.0
            print(f"  Trial {trial.number:>3d}/{n_trials} | avg {avg_t:.1f}s/trial | ETA ~{remaining:.0f}m")

        if (trial.number + 1) % CHECKPOINT_EVERY == 0:
            save_checkpoint(results, cp_path)

        return value

    study = optuna.create_study(
        direction="maximize",
        sampler=optuna.samplers.TPESampler(seed=42),
    )

    print(f"[BAYESIAN] Starting {n_trials} trials | objective={objective_col}")
    print(f"[BAYESIAN] Checkpoint: {cp_path}\n")
    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)

    return pd.DataFrame(results)


def run_grid(
    *,
    bt,
    data: PreloadedData,
    underlying_data: Dict[str, pd.DataFrame],
    folds: Sequence[set[date]],
    objective_col: str,
    cp_path: Path,
) -> pd.DataFrame:
    keys = list(GRID_SPACE.keys())
    combos = list(itertools.product(*[GRID_SPACE[k] for k in keys]))
    total = len(combos)
    print(f"[GRID] Evaluating {total:,} combinations")
    print(f"[GRID] Checkpoint: {cp_path}\n")

    results: List[Dict[str, Any]] = []
    best_value = -float("inf")
    trial_times: List[float] = []

    for i, vals in enumerate(combos):
        t0 = time.time()
        params = dict(zip(keys, vals))
        metrics = run_simulation(bt=bt, data=data, underlying_data=underlying_data, folds=folds, params=params)
        metrics["score_robust"] = finalize_robust_score(metrics)
        results.append(metrics)

        value = float(metrics.get(objective_col, 0.0))
        elapsed = time.time() - t0
        trial_times.append(elapsed)

        if i == 4:
            avg_t = float(np.mean(trial_times))
            eta = avg_t * max(total - 5, 0) / 60.0
            print(f"  Avg {avg_t:.1f}s/trial -> estimated total ~{eta:.0f} min\n")

        if metrics["n_trades"] > 0 and value > best_value:
            best_value = value
            print(
                f"  * [{i+1:>5d}/{total}] NEW BEST {objective_col}={value:,.3f} | "
                f"PnL=Rs {metrics['total_pnl']:,.0f} | Daily WR={metrics['daily_win_rate_pct']:.1f}% | "
                f"DD=Rs {metrics['max_drawdown']:,.0f} | ET={params['ENTRY_TIME_IST']} "
                f"MR={params['MAX_REATTEMPTS']} RDM={params['REENTRY_DELAY_MINUTES']} "
                f"OTM={params['OTM_DISTANCE_STEPS']} SL%={params['LEG_PREMIUM_RISE_EXIT_PCT']}"
            )
        elif (i + 1) % 100 == 0:
            avg_t = float(np.mean(trial_times)) if trial_times else 0.0
            remaining = avg_t * (total - i - 1) / 60.0
            print(f"  [{i+1:>5d}/{total}] avg {avg_t:.1f}s/trial | ETA ~{remaining:.0f}m")

        if (i + 1) % CHECKPOINT_EVERY == 0:
            save_checkpoint(results, cp_path)

    return pd.DataFrame(results)


def run_pareto(
    *,
    bt,
    data: PreloadedData,
    underlying_data: Dict[str, pd.DataFrame],
    folds: Sequence[set[date]],
    n_trials: int,
    cp_path: Path,
) -> pd.DataFrame:
    """
    Multi-objective mode.
    Maximizes:
      1. median fold PnL
      2. median fold daily win-rate

    NSGA-II is used because it is widely available in Optuna and avoids forcing
    profit and accuracy into one scalar during search.
    """
    try:
        import optuna
        optuna.logging.set_verbosity(optuna.logging.WARNING)
    except ImportError:
        raise ImportError("optuna is not installed. Install it with: pip install optuna")

    results: List[Dict[str, Any]] = []
    trial_times: List[float] = []

    def objective(trial: "optuna.Trial") -> Tuple[float, float]:
        t0 = time.time()
        params = suggest_params_bayesian(trial)
        metrics = run_simulation(bt=bt, data=data, underlying_data=underlying_data, folds=folds, params=params)
        metrics["score_robust"] = finalize_robust_score(metrics)
        results.append(metrics)

        elapsed = time.time() - t0
        trial_times.append(elapsed)

        if trial.number == 0:
            eta = elapsed * n_trials / 60.0
            print(f"  Trial 0 took {elapsed:.1f}s -> estimated total ~{eta:.0f} min for {n_trials} trials\n")
        elif trial.number > 0 and trial.number % 25 == 0:
            avg_t = float(np.mean(trial_times)) if trial_times else 0.0
            remaining = (n_trials - trial.number - 1) * avg_t / 60.0
            print(f"  Trial {trial.number:>3d}/{n_trials} | avg {avg_t:.1f}s/trial | ETA ~{remaining:.0f}m")

        if (trial.number + 1) % CHECKPOINT_EVERY == 0:
            save_checkpoint(results, cp_path)

        if int(metrics.get("n_trades", 0)) <= 0 or int(metrics.get("n_days", 0)) <= 0:
            return (INVALID_TRIAL_PENALTY, INVALID_TRIAL_PENALTY)

        return float(metrics["median_fold_pnl"]), float(metrics["median_fold_daily_win_rate"])

    study = optuna.create_study(
        directions=["maximize", "maximize"],
        sampler=optuna.samplers.NSGAIISampler(seed=42),
    )
    print(f"[PARETO] Starting {n_trials} trials | objectives=(median_fold_pnl, median_fold_daily_win_rate)")
    print(f"[PARETO] Checkpoint: {cp_path}\n")
    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)

    df = pd.DataFrame(results)
    if df.empty:
        return df

    # Compute a knee score for convenience after the Pareto search has finished.
    pnl = df["median_fold_pnl"].astype(float)
    wr = df["median_fold_daily_win_rate"].astype(float)

    pnl_range = max(float(pnl.max() - pnl.min()), 1.0)
    wr_range = max(float(wr.max() - wr.min()), 1.0)

    pnl_norm = (pnl - pnl.min()) / pnl_range
    wr_norm = (wr - wr.min()) / wr_range

    # Penalize large drawdown slightly so the recommended knee point is not a
    # reckless extreme-profit / poor-risk solution.
    dd = df["max_drawdown"].astype(float)
    dd_penalty = (dd.abs() - dd.abs().min()) / max(float(dd.abs().max() - dd.abs().min()), 1.0)

    df["score_knee"] = (0.50 * pnl_norm + 0.40 * wr_norm - 0.10 * dd_penalty).astype(float)
    return df


# =============================================================================
# OUTPUT
# =============================================================================
def write_results(
    *,
    results_df: pd.DataFrame,
    baseline: Dict[str, Any],
    mode: str,
    objective: str,
    output_path: Path,
) -> None:
    if results_df.empty:
        print("[WARN] No results to write.")
        return

    if mode == "pareto":
        sort_col = "score_knee" if "score_knee" in results_df.columns else "score_robust"
    else:
        sort_col = objective

    results_df = results_df.sort_values(sort_col, ascending=False).reset_index(drop=True)
    results_df.index.name = "rank"
    results_df.index += 1

    metric_cols = [c for c in results_df.columns if c not in PARAM_COLS]
    ordered_df = results_df[PARAM_COLS + metric_cols]

    print("\n" + "=" * 120)
    print("TOP 20 PARAMETER COMBINATIONS")
    print("=" * 120)
    display_cols = PARAM_COLS + [
        "total_pnl",
        "daily_win_rate_pct",
        "trade_win_rate_pct",
        "profit_factor",
        "max_drawdown",
        "median_fold_pnl",
        "median_fold_daily_win_rate",
        sort_col,
    ]
    display_cols = [c for c in display_cols if c in ordered_df.columns]
    print(ordered_df.head(20)[display_cols].to_string())

    best = ordered_df.iloc[0]
    print("\n" + "=" * 120)
    print("BEST PARAMETERS vs BASELINE")
    print("=" * 120)
    for p in PARAM_COLS:
        print(f"{p:28s}  best={str(best[p]):>10s}   baseline={str(baseline.get(p, '?')):>10s}")
    print("-" * 120)
    for m in [
        "total_pnl", "daily_win_rate_pct", "trade_win_rate_pct", "profit_factor",
        "max_drawdown", "median_fold_pnl", "median_fold_daily_win_rate", "score_robust",
    ]:
        if m in best.index:
            print(f"{m:28s}  best={best[m]:>10.3f}   baseline={float(baseline.get(m, 0.0)):>10.3f}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(output_path, engine="openpyxl") as xw:
        ordered_df.to_excel(xw, sheet_name="all_trials_ranked", index=True)
        ordered_df.head(20).T.to_excel(xw, sheet_name="top_20_detail")
        pd.DataFrame([baseline]).to_excel(xw, sheet_name="baseline", index=False)

        for p in PARAM_COLS:
            try:
                sens = (
                    ordered_df.groupby(p, as_index=False)
                    .agg(
                        trials=(sort_col, "count"),
                        avg_total_pnl=("total_pnl", "mean"),
                        best_total_pnl=("total_pnl", "max"),
                        avg_daily_wr=("daily_win_rate_pct", "mean"),
                        avg_trade_wr=("trade_win_rate_pct", "mean"),
                        avg_profit_factor=("profit_factor", "mean"),
                        avg_max_dd=("max_drawdown", "mean"),
                        avg_score=(sort_col, "mean"),
                    )
                    .sort_values("avg_score", ascending=False)
                )
                sens.to_excel(xw, sheet_name=f"sens_{p}"[:31], index=False)
            except Exception:
                pass

        wb = xw.book
        for ws in wb.worksheets:
            ws.freeze_panes = "A2"

    print(f"\n[SAVED] {output_path}")

    txt_path = output_path.with_name(output_path.stem + "_best_params.txt")
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("# Best parameter set\n")
        f.write(f"# Mode: {mode}\n")
        f.write(f"# Objective / sort column: {sort_col}\n\n")
        for p in PARAM_COLS:
            f.write(f"{p}={best[p]}\n")
        f.write("\n# PowerShell\n")
        for p in PARAM_COLS:
            f.write(f'$env:{p}="{best[p]}"\n')
        f.write("\n# CMD\n")
        for p in PARAM_COLS:
            f.write(f"set {p}={best[p]}\n")
    print(f"[SAVED] {txt_path}")


# =============================================================================
# MAIN
# =============================================================================
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Optimizer for the OTM leg-wise straddle backtester",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python optimize_otm_straddle_params_optuna.py
  python optimize_otm_straddle_params_optuna.py --trials 250
  python optimize_otm_straddle_params_optuna.py --mode pareto --trials 300
  python optimize_otm_straddle_params_optuna.py --objective total_pnl
  python optimize_otm_straddle_params_optuna.py --lookback-months 12
        """,
    )
    parser.add_argument("--mode", choices=["bayesian", "pareto", "grid"], default="bayesian")
    parser.add_argument(
        "--objective",
        choices=[
            "score_robust",
            "total_pnl",
            "daily_win_rate_pct",
            "trade_win_rate_pct",
            "profit_factor",
            "median_fold_pnl",
            "median_fold_daily_win_rate",
        ],
        default="score_robust",
    )
    parser.add_argument("--trials", type=int, default=200, help="Number of trials for bayesian/pareto modes")
    parser.add_argument("--folds", type=int, default=DEFAULT_FOLDS, help="Chronological folds for robustness metrics")
    parser.add_argument("--lookback-months", type=int, default=None, help="Override backtester LOOKBACK_MONTHS")
    parser.add_argument("--pickles-dir", default=None, help="Override pickle directory")
    parser.add_argument(
        "--strategy-file",
        default=str(Path(__file__).with_name("otm_straddle_legwise_reattempt_final_commented.py")),
        help="Path to the OTM strategy file",
    )
    parser.add_argument("--refresh-underlying-cache", action="store_true", help="Force one fresh underlying download")
    parser.add_argument("--output", default=None, help="Output Excel path")
    args = parser.parse_args()

    bt = load_strategy_module(args.strategy_file)
    pickles_dir = args.pickles_dir or bt.PICKLES_DIR

    paths = sorted(
        glob.glob(os.path.join(pickles_dir, "*.pkl"))
        + glob.glob(os.path.join(pickles_dir, "*.pickle"))
    )
    if not paths:
        raise FileNotFoundError(f"No .pkl/.pickle files found in: {pickles_dir}")

    end_day, min_expiry_map, min_day_seen = bt.scan_pickles_pass1(paths)
    lookback_months = args.lookback_months if args.lookback_months is not None else bt.LOOKBACK_MONTHS
    window_start = bt.compute_window_start(end_day, lookback_months)
    actual_underlying_by_day = bt.pick_actual_underlying_by_day(min_expiry_map)

    cp_path = checkpoint_path(args.mode, args.objective)
    output_path = Path(args.output) if args.output else get_downloads_folder() / f"optimizer_otm_{safe_fname_part(args.mode)}_{safe_fname_part(args.objective)}.xlsx"

    print("=" * 92)
    print("OTM LEG-WISE STRADDLE OPTIMIZER")
    print("=" * 92)
    print(f"Mode                 : {args.mode}")
    print(f"Objective            : {args.objective}")
    if args.mode in {"bayesian", "pareto"}:
        print(f"Trials               : {args.trials}")
    else:
        total_grid = 1
        for vals in GRID_SPACE.values():
            total_grid *= len(vals)
        print(f"Grid combinations    : {total_grid:,}")
    print(f"Strategy file        : {os.path.abspath(args.strategy_file)}")
    print(f"Pickles dir          : {pickles_dir}")
    print(f"Lookback months      : {lookback_months}")
    print(f"Data day-range seen  : {min_day_seen} -> {end_day}")
    print(f"Optimization window  : {window_start} -> {end_day}")
    print(f"Output               : {output_path}")
    print(f"Checkpoint           : {cp_path}")
    print()

    data = PreloadedData(
        bt=bt,
        pickles_dir=pickles_dir,
        window_start=window_start,
        window_end=end_day,
        min_expiry_map=min_expiry_map,
        actual_underlying_by_day=actual_underlying_by_day,
    )
    if not data.groups:
        raise RuntimeError("No usable day-groups found after applying the strategy filters.")

    folds = build_time_folds(data.trade_days, args.folds)
    print(f"[FOLDS] Built {len(folds)} chronological folds from {len(data.trade_days)} trade days\n")

    underlying_data = load_or_build_underlying_data(
        bt=bt,
        strategy_file=args.strategy_file,
        pickles_dir=pickles_dir,
        window_start=window_start,
        window_end=end_day,
        refresh=args.refresh_underlying_cache,
    )

    baseline = run_baseline(bt, data, underlying_data, folds)

    t0 = time.time()
    if args.mode == "bayesian":
        results_df = run_bayesian(
            bt=bt,
            data=data,
            underlying_data=underlying_data,
            folds=folds,
            n_trials=args.trials,
            objective_col=args.objective,
            cp_path=cp_path,
        )
    elif args.mode == "pareto":
        results_df = run_pareto(
            bt=bt,
            data=data,
            underlying_data=underlying_data,
            folds=folds,
            n_trials=args.trials,
            cp_path=cp_path,
        )
    else:
        results_df = run_grid(
            bt=bt,
            data=data,
            underlying_data=underlying_data,
            folds=folds,
            objective_col=args.objective,
            cp_path=cp_path,
        )
    elapsed = time.time() - t0
    print(f"\n[DONE] {len(results_df)} trials completed in {elapsed/60:.1f} minutes")

    # Recompute robust score after baseline normalization, for safety.
    if not results_df.empty:
        results_df["score_robust"] = results_df.apply(lambda r: finalize_robust_score(r.to_dict()), axis=1)

    write_results(
        results_df=results_df,
        baseline=baseline,
        mode=args.mode,
        objective=args.objective,
        output_path=output_path,
    )

    try:
        if cp_path.exists():
            cp_path.unlink()
            print(f"[CLEANUP] Removed checkpoint: {cp_path}")
    except Exception:
        pass

    print("\n" + "=" * 92)
    print("IMPORTANT")
    print("=" * 92)
    print("Top-ranked parameters can still overfit historical data.")
    print("Focus on clusters of similar good settings, not just the single #1 row.")
    print("Pareto mode is useful when you want to inspect the profit-vs-accuracy frontier.")
    print("Bayesian mode is useful when you want one practical robust recommendation.")
    print("=" * 92)


if __name__ == "__main__":
    main()
