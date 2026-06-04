"""
SHORT-STRADDLE PARAMETER OPTIMIZER  v3
=======================================
All 4 enhancements over v2:
  1. Transaction charges (Zerodha F&O) deducted from every trade
  2. Separate D0 / D-1 parameters for SL, PP, MSR
  3. Progressive reattempt delays (escalating per attempt)
  4. Parallel optimization via multiprocessing

Place this file in the SAME directory as:
    dhan_atm_straddle_prem_jump_reattempt_prem_perc.py

Requirements:
    pip install optuna openpyxl pandas numpy

Usage:
    python optimize_straddle_params.py                                # 200 trials, 1 worker
    python optimize_straddle_params.py --workers 4                    # 4 parallel workers
    python optimize_straddle_params.py --trials 300 --workers 4       # More trials, parallel
    python optimize_straddle_params.py --objective sharpe              # Risk-adjusted
    python optimize_straddle_params.py --lookback-months 12            # Recent data only
    python optimize_straddle_params.py --mode grid --workers 4         # Parallel grid
"""

import os
import sys
import glob
import time
import math
import pickle
import tempfile
import argparse
import warnings
import itertools
from datetime import date, time as dtime, datetime
from dataclasses import dataclass, asdict
from typing import Dict, List, Tuple, Any, Optional
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd
import numpy as np

warnings.filterwarnings("ignore")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

try:
    import dhan_atm_straddle_prem_jump_reattempt_prem_perc as bt
except ImportError as e:
    print(f"[ERROR] Cannot import backtester module.\n"
          f"  Ensure 'dhan_atm_straddle_prem_jump_reattempt_prem_perc.py' is in:\n"
          f"  {SCRIPT_DIR}\n\n  {e}")
    sys.exit(1)

# =============================================================================
# SEARCH SPACE  (11 params: shared + D0 + D-1 + progressive delays)
# =============================================================================
BAYESIAN_SPACE = {
    "ENTRY_TIME_IST":    ["09:20", "09:25", "09:30", "09:35", "09:40", "09:45", "09:50", "10:00"],
    "MAX_REATTEMPTS":    (0, 3),
    "LOSS_LIMIT_PCT_D0":             (0.05, 0.40),
    "PROFIT_PROTECT_TRIGGER_PCT_D0": (0.10, 0.50),
    "MAX_STOPLOSS_RUPEES_D0":        [0, 1500, 2000, 2500, 3000, 4000, 5000, 7000, 10000],
    "LOSS_LIMIT_PCT_D1":             (0.05, 0.40),
    "PROFIT_PROTECT_TRIGGER_PCT_D1": (0.10, 0.50),
    "MAX_STOPLOSS_RUPEES_D1":        [0, 2000, 3000, 4000, 5000, 7000, 10000, 15000],
    "REENTRY_DELAY_1":   (1, 20),
    "REENTRY_DELAY_2":   (1, 30),
    "REENTRY_DELAY_3":   (1, 45),
}

GRID_SPACE = {
    "ENTRY_TIME_IST":                ["09:25", "09:30", "09:35", "09:40"],
    "MAX_REATTEMPTS":                [1, 2, 3],
    "LOSS_LIMIT_PCT_D0":             [0.15, 0.25, 0.35],
    "PROFIT_PROTECT_TRIGGER_PCT_D0": [0.25, 0.33, 0.40],
    "MAX_STOPLOSS_RUPEES_D0":        [2000, 3000, 5000],
    "LOSS_LIMIT_PCT_D1":             [0.15, 0.25, 0.35],
    "PROFIT_PROTECT_TRIGGER_PCT_D1": [0.25, 0.33, 0.40],
    "MAX_STOPLOSS_RUPEES_D1":        [3000, 5000, 7000],
    "REENTRY_DELAY_1":               [5, 10, 14],
    "REENTRY_DELAY_2":               [10, 20],
    "REENTRY_DELAY_3":               [15, 30],
}

PARAM_COLS = [
    "ENTRY_TIME_IST", "MAX_REATTEMPTS",
    "LOSS_LIMIT_PCT_D0", "PROFIT_PROTECT_TRIGGER_PCT_D0", "MAX_STOPLOSS_RUPEES_D0",
    "LOSS_LIMIT_PCT_D1", "PROFIT_PROTECT_TRIGGER_PCT_D1", "MAX_STOPLOSS_RUPEES_D1",
    "REENTRY_DELAY_1", "REENTRY_DELAY_2", "REENTRY_DELAY_3",
]

PROFIT_FACTOR_CAP = 99.99
CHECKPOINT_EVERY = 10

# =============================================================================
# CUSTOM SIMULATION (standalone, no globals, D0/D-1 + progressive delays)
# =============================================================================
def _simulate_day_custom(und, dy, expiry, day_opt, source_pickle, params):
    dte = (expiry - dy).days
    if dte == 0:
        loss_limit_pct = float(params["LOSS_LIMIT_PCT_D0"])
        pp_trigger_pct = float(params["PROFIT_PROTECT_TRIGGER_PCT_D0"])
        max_sl_rupees  = float(params["MAX_STOPLOSS_RUPEES_D0"])
    else:
        loss_limit_pct = float(params["LOSS_LIMIT_PCT_D1"])
        pp_trigger_pct = float(params["PROFIT_PROTECT_TRIGGER_PCT_D1"])
        max_sl_rupees  = float(params["MAX_STOPLOSS_RUPEES_D1"])

    max_reattempts = int(params["MAX_REATTEMPTS"])
    reentry_delays = [
        int(params.get("REENTRY_DELAY_1", 5)),
        int(params.get("REENTRY_DELAY_2", 10)),
        int(params.get("REENTRY_DELAY_3", 15)),
    ]

    entry_time = bt.parse_hhmm(params["ENTRY_TIME_IST"])
    idx_all = bt.build_minute_index(dy, bt.SESSION_START_IST, bt.SESSION_END_IST)
    session_end_ts = idx_all[-1]
    qty  = int(bt.QTY_UNITS[und])
    step = int(bt.STRIKE_STEP[und])
    spot_s = bt._build_underlying_series_from_spot(day_opt, idx_all)
    cur_entry_ts = pd.Timestamp(datetime.combine(dy, entry_time), tz=bt.ist_tz())
    trade_seq = 1
    results = []

    while cur_entry_ts <= session_end_ts:
        if cur_entry_ts not in idx_all:
            break
        u_px = float(spot_s.loc[cur_entry_ts]) if pd.notna(spot_s.loc[cur_entry_ts]) else float("nan")
        if pd.isna(u_px):
            break
        atm = bt.round_to_step(float(u_px), step)
        ce_close = bt._build_leg_series_fixed_strike(day_opt, idx_all, atm, "CE", "close_f")
        pe_close = bt._build_leg_series_fixed_strike(day_opt, idx_all, atm, "PE", "close_f")
        ce_high  = bt._build_leg_series_fixed_strike(day_opt, idx_all, atm, "CE", "high_f")
        ce_low   = bt._build_leg_series_fixed_strike(day_opt, idx_all, atm, "CE", "low_f")
        pe_high  = bt._build_leg_series_fixed_strike(day_opt, idx_all, atm, "PE", "high_f")
        pe_low   = bt._build_leg_series_fixed_strike(day_opt, idx_all, atm, "PE", "low_f")
        ce_entry = ce_close.loc[cur_entry_ts]
        pe_entry = pe_close.loc[cur_entry_ts]
        if pd.isna(ce_entry) or pd.isna(pe_entry):
            break
        premium_sum_rupees = (float(ce_entry) + float(pe_entry)) * qty
        loss_limit_rupees = premium_sum_rupees * loss_limit_pct
        effective_sl = loss_limit_rupees
        if max_sl_rupees > 0:
            effective_sl = min(loss_limit_rupees, max_sl_rupees)
        G = premium_sum_rupees * pp_trigger_pct
        pp_enabled = G > 0
        pnl_close_all = (float(ce_entry) - ce_close) * qty + (float(pe_entry) - pe_close) * qty
        pnl = pnl_close_all.loc[cur_entry_ts:].dropna()
        pnl_ceH_peL = (float(ce_entry) - ce_high) * qty + (float(pe_entry) - pe_low) * qty
        pnl_ceL_peH = (float(ce_entry) - ce_low) * qty + (float(pe_entry) - pe_high) * qty
        pnl_sl_all = pd.concat([pnl_close_all, pnl_ceH_peL, pnl_ceL_peH], axis=1).min(axis=1)
        pnl_sl = pnl_sl_all.loc[cur_entry_ts:].dropna()
        if pnl.empty:
            break
        eod_ts = pnl.index[-1]
        eod_pnl = float(pnl.iloc[-1])
        max_profit = float(max(0.0, pnl.max()))
        max_loss = float(min(0.0, pnl.min()))
        stop_hit = pnl_sl <= -effective_sl
        stop_ts = pnl_sl.index[stop_hit.to_numpy().argmax()] if stop_hit.any() else None
        protect_ts = None
        if pp_enabled:
            peak = pnl.cummax()
            armed = peak >= G
            trail = peak - G
            protect_hit = armed & (pnl <= trail)
            protect_ts = pnl.index[protect_hit.to_numpy().argmax()] if protect_hit.any() else None
        exit_ts, exit_reason = eod_ts, "EOD"
        if stop_ts is not None and protect_ts is not None:
            if stop_ts <= protect_ts:
                exit_ts, exit_reason = stop_ts, "STOPLOSS"
            else:
                exit_ts, exit_reason = protect_ts, "PROFIT_PROTECT"
        elif stop_ts is not None:
            exit_ts, exit_reason = stop_ts, "STOPLOSS"
        elif protect_ts is not None:
            exit_ts, exit_reason = protect_ts, "PROFIT_PROTECT"
        exit_pnl_gross = float(pnl.loc[exit_ts])
        if exit_reason == "STOPLOSS" and exit_pnl_gross < -effective_sl:
            exit_pnl_gross = -float(effective_sl)
        exit_ce = float(ce_close.loc[exit_ts]) if pd.notna(ce_close.loc[exit_ts]) else 0.0
        exit_pe = float(pe_close.loc[exit_ts]) if pd.notna(pe_close.loc[exit_ts]) else 0.0
        txn_charges = bt.compute_trade_charges(
            entry_ce=float(ce_entry), entry_pe=float(pe_entry),
            exit_ce=exit_ce, exit_pe=exit_pe, qty=qty)
        exit_pnl = exit_pnl_gross - txn_charges
        results.append({
            "day": dy, "underlying": und, "trade_seq": trade_seq,
            "expiry": expiry, "days_to_expiry": dte,
            "atm_strike": int(atm), "qty_units": qty,
            "entry_time": pd.Timestamp(cur_entry_ts).strftime("%H:%M"),
            "exit_time": pd.Timestamp(exit_ts).strftime("%H:%M"),
            "exit_reason": exit_reason, "entry_underlying": float(u_px),
            "entry_ce": float(ce_entry), "entry_pe": float(pe_entry),
            "exit_ce": exit_ce, "exit_pe": exit_pe,
            "exit_pnl_gross": exit_pnl_gross, "txn_charges": txn_charges,
            "exit_pnl": exit_pnl, "eod_pnl": eod_pnl,
            "max_profit": max_profit, "max_loss": max_loss,
            "source_pickle": source_pickle,
        })
        if exit_reason in ("STOPLOSS", "PROFIT_PROTECT") and (trade_seq - 1) < max_reattempts:
            delay_idx = min(trade_seq - 1, len(reentry_delays) - 1)
            delay_min = reentry_delays[delay_idx]
            trade_seq += 1
            cur_entry_ts = pd.Timestamp(exit_ts) + pd.Timedelta(minutes=delay_min)
            if cur_entry_ts > session_end_ts:
                break
            continue
        break
    return results

# =============================================================================
# TRIAL RUNNER (process-safe)
# =============================================================================
_WORKER_GROUPS = None

def _init_worker(data_path):
    global _WORKER_GROUPS
    with open(data_path, "rb") as f:
        _WORKER_GROUPS = pickle.load(f)

def run_one_trial(params):
    groups = _WORKER_GROUPS
    if groups is None:
        return _empty_metrics(params)
    entry_t = bt.parse_hhmm(params["ENTRY_TIME_IST"])
    if not (bt.SESSION_START_IST <= entry_t <= bt.SESSION_END_IST):
        return _empty_metrics(params)
    all_trade_rows = []
    for g in groups:
        try:
            trades = _simulate_day_custom(
                und=g["und"], dy=g["dy"], expiry=g["expiry"],
                day_opt=g["day_opt"], source_pickle=g["source_pickle"],
                params=params)
            all_trade_rows.extend(trades)
        except Exception as e:
            print(f"  [DEBUG] {g['und']} {g['dy']}: {e}")
            continue
    if not all_trade_rows:
        return _empty_metrics(params)
    all_trades_df = pd.DataFrame(all_trade_rows)
    key_cols = ["underlying", "day", "expiry", "trade_seq", "entry_time"]
    all_trades_df = (all_trades_df.sort_values(key_cols + ["source_pickle"])
                     .drop_duplicates(subset=key_cols, keep="first")
                     .reset_index(drop=True))
    actual = bt.build_actual_trades_df(all_trades_df)
    if actual.empty:
        return _empty_metrics(params)
    return _compute_metrics(actual, params)

# =============================================================================
# DATA PRELOADING
# =============================================================================
class PreloadedData:
    def __init__(self, pickles_dir, window_start, window_end):
        self.groups = []
        self.temp_path = None
        self._load(pickles_dir, window_start, window_end)

    def _load(self, pickles_dir, window_start, window_end):
        paths = sorted(glob.glob(os.path.join(pickles_dir, "*.pkl"))
                       + glob.glob(os.path.join(pickles_dir, "*.pickle")))
        if not paths:
            raise FileNotFoundError(f"No pickle files in: {pickles_dir}")
        n = len(paths)
        print(f"[PRELOAD] Found {n} pickle files.  Normalizing ...")
        t0 = time.time()
        all_groups = []
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
                min_expiry = d.groupby(["underlying", "day"], sort=False)["expiry"].min().to_dict()
                for (und, dy, ex), g in d.groupby(["underlying", "day", "expiry"], sort=False):
                    if min_expiry.get((und, dy)) != ex:
                        continue
                    all_groups.append({"und": und, "dy": dy, "expiry": ex,
                                       "day_opt": g.copy(), "source_pickle": src})
                if (i + 1) % 5 == 0 or (i + 1) == n:
                    print(f"  ... {i+1}/{n} pickles loaded")
            except Exception as e:
                print(f"  [WARN] {src}: {e}")
        all_groups.sort(key=lambda g: (g["und"], g["dy"], g["expiry"], g["source_pickle"]))
        seen = set()
        deduped = []
        for g in all_groups:
            key = (g["und"], g["dy"], g["expiry"])
            if key not in seen:
                seen.add(key)
                deduped.append(g)
        self.groups = deduped
        total_rows = sum(len(g["day_opt"]) for g in self.groups)
        print(f"[PRELOAD] Done: {len(self.groups):,} groups | {total_rows:,} rows | {time.time()-t0:.1f}s\n")

    def save_for_workers(self):
        fd, path = tempfile.mkstemp(suffix=".pkl", prefix="opt_data_")
        os.close(fd)
        with open(path, "wb") as f:
            pickle.dump(self.groups, f, protocol=pickle.HIGHEST_PROTOCOL)
        size_mb = os.path.getsize(path) / (1024 * 1024)
        print(f"[PARALLEL] Data serialized: {path} ({size_mb:.0f} MB)")
        self.temp_path = path
        return path

    def cleanup(self):
        if self.temp_path and os.path.exists(self.temp_path):
            os.remove(self.temp_path)

# =============================================================================
# METRICS
# =============================================================================
_BASELINE = {"total_pnl": 2_000_000, "sharpe": 0.28, "max_drawdown": -60_000}

def _compute_metrics(actual, params):
    pnl = pd.to_numeric(actual["exit_pnl"], errors="coerce").astype(float)
    daily = actual.groupby("day")["exit_pnl"].sum().reset_index()
    daily_pnls = daily["exit_pnl"].astype(float)
    total_pnl = float(pnl.sum())
    n_trades = len(actual)
    n_days = len(daily)
    win_rate_trade = float((pnl > 0).mean()) * 100
    win_rate_daily = float((daily_pnls > 0).mean()) * 100
    avg_daily = float(daily_pnls.mean())
    std_daily = float(daily_pnls.std()) if n_days > 1 else 1.0
    sharpe = avg_daily / std_daily if std_daily > 0 else 0.0
    worst_day = float(daily_pnls.min()) if n_days > 0 else 0.0
    best_day = float(daily_pnls.max()) if n_days > 0 else 0.0
    cumulative = daily_pnls.cumsum()
    max_drawdown = float((cumulative - cumulative.cummax()).min())
    gross_profit = float(pnl[pnl > 0].sum())
    gross_loss = float(-pnl[pnl < 0].sum())
    if gross_loss > 0:
        profit_factor = min(gross_profit / gross_loss, PROFIT_FACTOR_CAP)
    else:
        profit_factor = PROFIT_FACTOR_CAP if gross_profit > 0 else 0.0
    er = actual["exit_reason"].astype(str).str.upper()
    sl_count = int(er.eq("STOPLOSS").sum())
    pp_count = int(er.eq("PROFIT_PROTECT").sum())
    eod_count = int(er.eq("EOD").sum())
    sl_pnl = float(pnl[er.eq("STOPLOSS")].sum())
    eod_pnl_total = float(pnl[er.eq("EOD")].sum())
    pp_pnl = float(pnl[er.eq("PROFIT_PROTECT")].sum())
    sl_pct = round(100.0 * sl_count / n_trades, 1) if n_trades > 0 else 0.0
    eod_pct = round(100.0 * eod_count / n_trades, 1) if n_trades > 0 else 0.0
    pp_pct = round(100.0 * pp_count / n_trades, 1) if n_trades > 0 else 0.0
    re_mask = actual["trade_seq"] > 1
    reattempt_pnl = float(pnl[re_mask].sum())
    reattempt_count = int(re_mask.sum())
    txn_total = float(actual["txn_charges"].sum()) if "txn_charges" in actual.columns else 0.0
    loss_days_gt_10k = int((daily_pnls <= -10000).sum())
    loss_days_gt_15k = int((daily_pnls <= -15000).sum())
    avg_loss_ld = float(daily_pnls[daily_pnls < 0].mean()) if (daily_pnls < 0).any() else 0.0
    avg_profit_wd = float(daily_pnls[daily_pnls > 0].mean()) if (daily_pnls > 0).any() else 0.0
    return {**params,
        "total_pnl": total_pnl, "avg_daily_pnl": avg_daily,
        "sharpe": sharpe, "win_rate_trade_pct": win_rate_trade,
        "win_rate_daily_pct": win_rate_daily, "profit_factor": profit_factor,
        "max_drawdown": max_drawdown, "n_trades": n_trades, "n_days": n_days,
        "sl_count": sl_count, "sl_pct": sl_pct,
        "pp_count": pp_count, "pp_pct": pp_pct,
        "eod_count": eod_count, "eod_pct": eod_pct,
        "reattempt_count": reattempt_count,
        "sl_total_pnl": sl_pnl, "eod_total_pnl": eod_pnl_total,
        "pp_total_pnl": pp_pnl, "reattempt_total_pnl": reattempt_pnl,
        "total_txn_charges": txn_total,
        "worst_day": worst_day, "best_day": best_day,
        "loss_days_gt_10k": loss_days_gt_10k, "loss_days_gt_15k": loss_days_gt_15k,
        "avg_loss_on_loss_days": avg_loss_ld, "avg_profit_on_win_days": avg_profit_wd,
        "score_balanced": 0.0}

def _empty_metrics(params):
    return {**params,
        "total_pnl": 0, "avg_daily_pnl": 0, "sharpe": 0,
        "win_rate_trade_pct": 0, "win_rate_daily_pct": 0,
        "profit_factor": 0, "max_drawdown": 0,
        "n_trades": 0, "n_days": 0,
        "sl_count": 0, "sl_pct": 0, "pp_count": 0, "pp_pct": 0,
        "eod_count": 0, "eod_pct": 0, "reattempt_count": 0,
        "sl_total_pnl": 0, "eod_total_pnl": 0,
        "pp_total_pnl": 0, "reattempt_total_pnl": 0,
        "total_txn_charges": 0,
        "worst_day": 0, "best_day": 0,
        "loss_days_gt_10k": 0, "loss_days_gt_15k": 0,
        "avg_loss_on_loss_days": 0, "avg_profit_on_win_days": 0,
        "score_balanced": -999}

def _compute_balanced_score(m):
    base_pnl = max(abs(_BASELINE["total_pnl"]), 1)
    base_sh = max(abs(_BASELINE["sharpe"]), 0.01)
    base_dd = min(_BASELINE["max_drawdown"], -1)
    return (0.40 * m["total_pnl"] / base_pnl +
            0.25 * m["sharpe"] / base_sh +
            0.20 * m["win_rate_daily_pct"] / 100.0 +
            0.15 * m["max_drawdown"] / abs(base_dd))

# =============================================================================
# BASELINE
# =============================================================================
def run_baseline(data):
    defaults = {
        "ENTRY_TIME_IST": bt.ENTRY_TIME_IST, "MAX_REATTEMPTS": bt.MAX_REATTEMPTS,
        "LOSS_LIMIT_PCT_D0": bt.LOSS_LIMIT_PCT, "PROFIT_PROTECT_TRIGGER_PCT_D0": bt.PROFIT_PROTECT_TRIGGER_PCT,
        "MAX_STOPLOSS_RUPEES_D0": bt.MAX_STOPLOSS_RUPEES,
        "LOSS_LIMIT_PCT_D1": bt.LOSS_LIMIT_PCT, "PROFIT_PROTECT_TRIGGER_PCT_D1": bt.PROFIT_PROTECT_TRIGGER_PCT,
        "MAX_STOPLOSS_RUPEES_D1": bt.MAX_STOPLOSS_RUPEES,
        "REENTRY_DELAY_1": bt.REENTRY_DELAY_MINUTES,
        "REENTRY_DELAY_2": bt.REENTRY_DELAY_MINUTES,
        "REENTRY_DELAY_3": bt.REENTRY_DELAY_MINUTES,
    }
    print("[BASELINE] Running with current defaults ...")
    t0 = time.time()
    global _WORKER_GROUPS
    _WORKER_GROUPS = data.groups
    metrics = run_one_trial(defaults)
    _WORKER_GROUPS = None
    elapsed = time.time() - t0
    if metrics["n_trades"] > 0:
        _BASELINE["total_pnl"] = max(abs(metrics["total_pnl"]), 1)
        _BASELINE["sharpe"] = max(abs(metrics["sharpe"]), 0.01)
        _BASELINE["max_drawdown"] = min(metrics["max_drawdown"], -1)
    metrics["score_balanced"] = _compute_balanced_score(metrics)
    print(f"[BASELINE] Done in {elapsed:.1f}s  |  PnL=Rs {metrics['total_pnl']:,.0f}  "
          f"Sharpe={metrics['sharpe']:.3f}  WR={metrics['win_rate_daily_pct']:.1f}%  "
          f"DD=Rs {metrics['max_drawdown']:,.0f}  Txn=Rs {metrics['total_txn_charges']:,.0f}")
    print(f"[BASELINE] This is your 'before' - optimizer must beat this.\n")
    return metrics

# =============================================================================
# CHECKPOINT
# =============================================================================
def _checkpoint_path():
    return str(Path.home() / "Downloads" / "optimizer_checkpoint.csv")

def _save_checkpoint(results_list):
    if not results_list:
        return
    try:
        pd.DataFrame(results_list).to_csv(_checkpoint_path(), index=False)
    except Exception:
        pass

# =============================================================================
# BAYESIAN SUGGEST HELPER
# =============================================================================
def _suggest_params(trial):
    return {
        "ENTRY_TIME_IST": trial.suggest_categorical("ENTRY_TIME_IST", BAYESIAN_SPACE["ENTRY_TIME_IST"]),
        "MAX_REATTEMPTS": trial.suggest_int("MAX_REATTEMPTS", *BAYESIAN_SPACE["MAX_REATTEMPTS"]),
        "LOSS_LIMIT_PCT_D0": round(trial.suggest_float("LOSS_LIMIT_PCT_D0", *BAYESIAN_SPACE["LOSS_LIMIT_PCT_D0"], step=0.01), 2),
        "PROFIT_PROTECT_TRIGGER_PCT_D0": round(trial.suggest_float("PROFIT_PROTECT_TRIGGER_PCT_D0", *BAYESIAN_SPACE["PROFIT_PROTECT_TRIGGER_PCT_D0"], step=0.01), 2),
        "MAX_STOPLOSS_RUPEES_D0": trial.suggest_categorical("MAX_STOPLOSS_RUPEES_D0", BAYESIAN_SPACE["MAX_STOPLOSS_RUPEES_D0"]),
        "LOSS_LIMIT_PCT_D1": round(trial.suggest_float("LOSS_LIMIT_PCT_D1", *BAYESIAN_SPACE["LOSS_LIMIT_PCT_D1"], step=0.01), 2),
        "PROFIT_PROTECT_TRIGGER_PCT_D1": round(trial.suggest_float("PROFIT_PROTECT_TRIGGER_PCT_D1", *BAYESIAN_SPACE["PROFIT_PROTECT_TRIGGER_PCT_D1"], step=0.01), 2),
        "MAX_STOPLOSS_RUPEES_D1": trial.suggest_categorical("MAX_STOPLOSS_RUPEES_D1", BAYESIAN_SPACE["MAX_STOPLOSS_RUPEES_D1"]),
        "REENTRY_DELAY_1": trial.suggest_int("REENTRY_DELAY_1", *BAYESIAN_SPACE["REENTRY_DELAY_1"]),
        "REENTRY_DELAY_2": trial.suggest_int("REENTRY_DELAY_2", *BAYESIAN_SPACE["REENTRY_DELAY_2"]),
        "REENTRY_DELAY_3": trial.suggest_int("REENTRY_DELAY_3", *BAYESIAN_SPACE["REENTRY_DELAY_3"]),
    }

# =============================================================================
# BAYESIAN SEQUENTIAL (workers=1)
# =============================================================================
def run_bayesian_sequential(data, n_trials, objective_col):
    import optuna
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    global _WORKER_GROUPS
    _WORKER_GROUPS = data.groups
    results_list = []
    best_value = -float("inf")
    trial_times = []

    def objective(trial):
        nonlocal best_value
        t0 = time.time()
        params = _suggest_params(trial)
        metrics = run_one_trial(params)
        metrics["score_balanced"] = _compute_balanced_score(metrics)
        results_list.append(metrics)
        value = float(metrics.get(objective_col, 0))
        elapsed = time.time() - t0
        trial_times.append(elapsed)
        if trial.number == 0:
            print(f"  Trial 0 took {elapsed:.1f}s -> ~{elapsed * n_trials / 60:.0f} min total\n")
        if value > best_value and metrics["n_trades"] > 0:
            best_value = value
            p = params
            print(f"  * Trial {trial.number:>3d} NEW BEST  {objective_col}={value:>10,.2f}  |  "
                  f"PnL=Rs {metrics['total_pnl']:>10,.0f}  Sharpe={metrics['sharpe']:.3f}  "
                  f"WR={metrics['win_rate_daily_pct']:.1f}%  DD=Rs {metrics['max_drawdown']:>8,.0f}  "
                  f"Txn=Rs {metrics['total_txn_charges']:>7,.0f}  |  "
                  f"D0[LL={p['LOSS_LIMIT_PCT_D0']} PP={p['PROFIT_PROTECT_TRIGGER_PCT_D0']} MSR={p['MAX_STOPLOSS_RUPEES_D0']}] "
                  f"D1[LL={p['LOSS_LIMIT_PCT_D1']} PP={p['PROFIT_PROTECT_TRIGGER_PCT_D1']} MSR={p['MAX_STOPLOSS_RUPEES_D1']}] "
                  f"RD=[{p['REENTRY_DELAY_1']},{p['REENTRY_DELAY_2']},{p['REENTRY_DELAY_3']}]")
        elif trial.number % 25 == 0 and trial.number > 0:
            avg_t = np.mean(trial_times)
            rem = (n_trials - trial.number - 1) * avg_t / 60
            print(f"  Trial {trial.number:>3d}/{n_trials}  PnL=Rs {metrics['total_pnl']:>10,.0f}  [{avg_t:.1f}s/trial, ~{rem:.0f}m left]")
        if (trial.number + 1) % CHECKPOINT_EVERY == 0:
            _save_checkpoint(results_list)
        return value

    study = optuna.create_study(direction="maximize", sampler=optuna.samplers.TPESampler(seed=42))
    print(f"[BAYESIAN] {n_trials} trials, 1 worker, optimizing: {objective_col}\n")
    study.optimize(objective, n_trials=n_trials)
    _WORKER_GROUPS = None
    return pd.DataFrame(results_list)

# =============================================================================
# BAYESIAN PARALLEL (workers>1)
# =============================================================================
def run_bayesian_parallel(data, n_trials, objective_col, n_workers):
    import optuna
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    data_path = data.save_for_workers()
    results_list = []
    best_value = -float("inf")
    completed = 0
    study = optuna.create_study(direction="maximize", sampler=optuna.samplers.TPESampler(seed=42))
    batch_size = n_workers * 2
    print(f"[BAYESIAN] {n_trials} trials, {n_workers} workers, batch={batch_size}, optimizing: {objective_col}\n")
    t_start = time.time()

    with ProcessPoolExecutor(max_workers=n_workers, initializer=_init_worker, initargs=(data_path,)) as pool:
        remaining = n_trials
        while remaining > 0:
            this_batch = min(batch_size, remaining)
            trials_and_params = []
            for _ in range(this_batch):
                trial = study.ask()
                params = {
                    "ENTRY_TIME_IST": trial.suggest_categorical("ENTRY_TIME_IST", BAYESIAN_SPACE["ENTRY_TIME_IST"]),
                    "MAX_REATTEMPTS": trial.suggest_int("MAX_REATTEMPTS", *BAYESIAN_SPACE["MAX_REATTEMPTS"]),
                    "LOSS_LIMIT_PCT_D0": round(trial.suggest_float("LOSS_LIMIT_PCT_D0", *BAYESIAN_SPACE["LOSS_LIMIT_PCT_D0"], step=0.01), 2),
                    "PROFIT_PROTECT_TRIGGER_PCT_D0": round(trial.suggest_float("PROFIT_PROTECT_TRIGGER_PCT_D0", *BAYESIAN_SPACE["PROFIT_PROTECT_TRIGGER_PCT_D0"], step=0.01), 2),
                    "MAX_STOPLOSS_RUPEES_D0": trial.suggest_categorical("MAX_STOPLOSS_RUPEES_D0", BAYESIAN_SPACE["MAX_STOPLOSS_RUPEES_D0"]),
                    "LOSS_LIMIT_PCT_D1": round(trial.suggest_float("LOSS_LIMIT_PCT_D1", *BAYESIAN_SPACE["LOSS_LIMIT_PCT_D1"], step=0.01), 2),
                    "PROFIT_PROTECT_TRIGGER_PCT_D1": round(trial.suggest_float("PROFIT_PROTECT_TRIGGER_PCT_D1", *BAYESIAN_SPACE["PROFIT_PROTECT_TRIGGER_PCT_D1"], step=0.01), 2),
                    "MAX_STOPLOSS_RUPEES_D1": trial.suggest_categorical("MAX_STOPLOSS_RUPEES_D1", BAYESIAN_SPACE["MAX_STOPLOSS_RUPEES_D1"]),
                    "REENTRY_DELAY_1": trial.suggest_int("REENTRY_DELAY_1", *BAYESIAN_SPACE["REENTRY_DELAY_1"]),
                    "REENTRY_DELAY_2": trial.suggest_int("REENTRY_DELAY_2", *BAYESIAN_SPACE["REENTRY_DELAY_2"]),
                    "REENTRY_DELAY_3": trial.suggest_int("REENTRY_DELAY_3", *BAYESIAN_SPACE["REENTRY_DELAY_3"]),
                }
                trials_and_params.append((trial, params))
            futures = {pool.submit(run_one_trial, p): (t, p) for t, p in trials_and_params}
            for future in as_completed(futures):
                trial_obj, params = futures[future]
                try:
                    metrics = future.result()
                except Exception:
                    metrics = _empty_metrics(params)
                metrics["score_balanced"] = _compute_balanced_score(metrics)
                results_list.append(metrics)
                value = float(metrics.get(objective_col, 0))
                study.tell(trial_obj, value)
                completed += 1
                if value > best_value and metrics["n_trades"] > 0:
                    best_value = value
                    print(f"  * [{completed:>3d}/{n_trials}] NEW BEST {objective_col}={value:>10,.2f}  "
                          f"PnL=Rs {metrics['total_pnl']:>10,.0f}  Sharpe={metrics['sharpe']:.3f}  "
                          f"WR={metrics['win_rate_daily_pct']:.1f}%  DD=Rs {metrics['max_drawdown']:>8,.0f}")
                elif completed % 25 == 0:
                    wall = time.time() - t_start
                    rate = wall / completed
                    rem = rate * (n_trials - completed) / 60
                    print(f"  [{completed:>3d}/{n_trials}] {wall/60:.1f}m elapsed  ETA={rem:.0f}m  best={best_value:,.2f}")
                if completed % CHECKPOINT_EVERY == 0:
                    _save_checkpoint(results_list)
            remaining -= this_batch
    return pd.DataFrame(results_list)

# =============================================================================
# GRID (with parallel)
# =============================================================================
def run_grid(data, objective_col, n_workers):
    keys = list(GRID_SPACE.keys())
    combos = list(itertools.product(*[GRID_SPACE[k] for k in keys]))
    total = len(combos)
    print(f"[GRID] {total:,} combos, {n_workers} workers\n")
    all_params = [dict(zip(keys, vals)) for vals in combos]
    results_list = []
    best_value = -float("inf")
    t_start = time.time()

    if n_workers <= 1:
        global _WORKER_GROUPS
        _WORKER_GROUPS = data.groups
        for i, params in enumerate(all_params):
            metrics = run_one_trial(params)
            metrics["score_balanced"] = _compute_balanced_score(metrics)
            results_list.append(metrics)
            value = float(metrics.get(objective_col, 0))
            if value > best_value and metrics["n_trades"] > 0:
                best_value = value
                print(f"  * [{i+1:>5d}/{total}] NEW BEST {objective_col}={value:>10,.2f}  PnL=Rs {metrics['total_pnl']:>10,.0f}")
            elif (i + 1) % 200 == 0:
                print(f"  [{i+1:>5d}/{total}] {(time.time()-t_start)/60:.1f}m elapsed")
            if (i + 1) % CHECKPOINT_EVERY == 0:
                _save_checkpoint(results_list)
        _WORKER_GROUPS = None
    else:
        data_path = data.save_for_workers()
        completed = 0
        with ProcessPoolExecutor(max_workers=n_workers, initializer=_init_worker, initargs=(data_path,)) as pool:
            futures = {pool.submit(run_one_trial, p): p for p in all_params}
            for future in as_completed(futures):
                params = futures[future]
                try:
                    metrics = future.result()
                except Exception:
                    metrics = _empty_metrics(params)
                metrics["score_balanced"] = _compute_balanced_score(metrics)
                results_list.append(metrics)
                completed += 1
                value = float(metrics.get(objective_col, 0))
                if value > best_value and metrics["n_trades"] > 0:
                    best_value = value
                    print(f"  * [{completed:>5d}/{total}] NEW BEST {objective_col}={value:>10,.2f}  PnL=Rs {metrics['total_pnl']:>10,.0f}")
                elif completed % 200 == 0:
                    print(f"  [{completed:>5d}/{total}] {(time.time()-t_start)/60:.1f}m elapsed")
                if completed % CHECKPOINT_EVERY == 0:
                    _save_checkpoint(results_list)
    return pd.DataFrame(results_list)

# =============================================================================
# OUTPUT
# =============================================================================
def write_results(results_df, baseline, objective_col, output_path):
    if results_df.empty:
        print("[WARN] No results.")
        return
    results_df = results_df.sort_values(objective_col, ascending=False).reset_index(drop=True)
    results_df.index.name = "rank"
    results_df.index += 1
    metric_cols = [c for c in results_df.columns if c not in PARAM_COLS]
    results_df = results_df[PARAM_COLS + metric_cols]

    print("\n" + "=" * 140)
    print("TOP 20 PARAMETER COMBINATIONS")
    print("=" * 140)
    disp = [c for c in PARAM_COLS + ["total_pnl", "sharpe", "win_rate_daily_pct",
            "max_drawdown", "profit_factor", "total_txn_charges", "score_balanced"]
            if c in results_df.columns]
    print(results_df.head(20)[disp].to_string())

    best = results_df.iloc[0]
    print("\n" + "=" * 140)
    print("  BEST vs BASELINE")
    print("=" * 140)
    print(f"  {'':40s}  {'BEST':>15s}  {'BASELINE':>15s}")
    print("  " + "-" * 75)
    for p in PARAM_COLS:
        print(f"  {p:40s}  {str(best[p]):>15s}  {str(baseline.get(p, '?')):>15s}")
    print("  " + "-" * 75)
    for m in ["total_pnl", "avg_daily_pnl", "sharpe", "win_rate_daily_pct", "max_drawdown", "profit_factor", "total_txn_charges"]:
        bv = baseline.get(m, 0)
        nv = best.get(m, 0)
        if m in ("total_pnl", "avg_daily_pnl", "max_drawdown", "total_txn_charges"):
            print(f"  {m:40s}  Rs {nv:>12,.0f}  Rs {bv:>12,.0f}")
        elif "pct" in m:
            print(f"  {m:40s}  {nv:>14.1f}%  {bv:>14.1f}%")
        else:
            print(f"  {m:40s}  {nv:>15.3f}  {bv:>15.3f}")

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as xw:
        results_df.to_excel(xw, sheet_name="all_trials_ranked", index=True)
        results_df.head(10).T.to_excel(xw, sheet_name="top_10_detail")
        pd.DataFrame([baseline]).to_excel(xw, sheet_name="baseline", index=False)
        # Abbreviation map to keep sheet names under 31 chars and avoid collisions
        _SHEET_ABBREV = {
            "ENTRY_TIME_IST": "ET",
            "MAX_REATTEMPTS": "MR",
            "LOSS_LIMIT_PCT_D0": "LL_D0",
            "PROFIT_PROTECT_TRIGGER_PCT_D0": "PPT_D0",
            "MAX_STOPLOSS_RUPEES_D0": "MSR_D0",
            "LOSS_LIMIT_PCT_D1": "LL_D1",
            "PROFIT_PROTECT_TRIGGER_PCT_D1": "PPT_D1",
            "MAX_STOPLOSS_RUPEES_D1": "MSR_D1",
            "REENTRY_DELAY_1": "RD1",
            "REENTRY_DELAY_2": "RD2",
            "REENTRY_DELAY_3": "RD3",
        }
        for p in PARAM_COLS:
            try:
                sens = (results_df.groupby(p, as_index=False)
                        .agg(trials=("total_pnl", "count"), avg_pnl=("total_pnl", "mean"),
                             best_pnl=("total_pnl", "max"), avg_sharpe=("sharpe", "mean"),
                             avg_wr=("win_rate_daily_pct", "mean"), avg_dd=("max_drawdown", "mean"))
                        .sort_values("avg_pnl", ascending=False))
                sheet_name = f"sens_{_SHEET_ABBREV.get(p, p)}"[:31]
                sens.to_excel(xw, sheet_name=sheet_name, index=False)
            except Exception:
                pass
        for ws in xw.book.worksheets:
            ws.freeze_panes = "A2"
    print(f"\n[SAVED] {output_path}")

    txt = output_path.replace(".xlsx", "_best_params.txt")
    with open(txt, "w") as f:
        f.write("# Best parameters (v3: D0/D-1 + progressive delays + txn charges)\n\n")
        f.write("# --- Windows CMD ---\n")
        for p in PARAM_COLS:
            f.write(f"set {p}={best[p]}\n")
        f.write("\n# --- PowerShell ---\n")
        for p in PARAM_COLS:
            f.write(f'$env:{p}="{best[p]}"\n')
        f.write(f"\n# PnL (net): Rs {best['total_pnl']:,.0f}  |  Sharpe: {best['sharpe']:.3f}  |  "
                f"WR: {best['win_rate_daily_pct']:.1f}%  |  DD: Rs {best['max_drawdown']:,.0f}  |  "
                f"Txn: Rs {best.get('total_txn_charges', 0):,.0f}\n")
    print(f"[SAVED] {txt}")

    try:
        cp = _checkpoint_path()
        if os.path.exists(cp):
            os.remove(cp)
    except Exception:
        pass

# =============================================================================
# MAIN
# =============================================================================
def main():
    parser = argparse.ArgumentParser(description="Short-straddle optimizer v3")
    parser.add_argument("--mode", choices=["bayesian", "grid"], default="bayesian")
    parser.add_argument("--trials", type=int, default=200)
    parser.add_argument("--workers", type=int, default=1, help="Parallel workers (default 1)")
    parser.add_argument("--objective", default="score_balanced",
                        choices=["total_pnl", "sharpe", "win_rate_daily_pct", "profit_factor", "score_balanced"])
    parser.add_argument("--pickles-dir", default=None)
    parser.add_argument("--lookback-months", type=int, default=None)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    pickles_dir = args.pickles_dir or bt.PICKLES_DIR
    lookback = args.lookback_months or bt.LOOKBACK_MONTHS
    output_path = args.output or str(Path.home() / "Downloads" / f"optimizer_v3_{args.mode}_{args.objective}.xlsx")
    cpu_count = os.cpu_count() or 4
    n_workers = min(args.workers, cpu_count)

    print("=" * 80)
    print("  SHORT-STRADDLE PARAMETER OPTIMIZER  v3")
    print("  D0/D-1 split | Progressive delays | Txn charges | Parallel")
    print("=" * 80)
    print(f"  Mode:        {args.mode}")
    print(f"  Objective:   {args.objective}")
    if args.mode == "bayesian":
        print(f"  Trials:      {args.trials}")
    else:
        total_grid = 1
        for v in GRID_SPACE.values():
            total_grid *= len(v)
        print(f"  Grid combos: {total_grid:,}")
    print(f"  Workers:     {n_workers} / {cpu_count} CPUs")
    print(f"  Pickles:     {pickles_dir}")
    print(f"  Lookback:    {lookback} months")
    print(f"  Output:      {output_path}")
    print()

    paths = sorted(glob.glob(os.path.join(pickles_dir, "*.pkl")) + glob.glob(os.path.join(pickles_dir, "*.pickle")))
    if not paths:
        print(f"[ERROR] No pickles in: {pickles_dir}")
        sys.exit(1)

    end_day = bt.discover_data_max_day(paths) or date.today()
    window_start = bt.compute_window_start(end_day, lookback)
    print(f"  Data window: {window_start} -> {end_day}\n")

    data = PreloadedData(pickles_dir, window_start, end_day)
    if not data.groups:
        print("[ERROR] No data groups.")
        sys.exit(1)

    baseline = run_baseline(data)

    t_start = time.time()
    if args.mode == "bayesian":
        if n_workers <= 1:
            results_df = run_bayesian_sequential(data, args.trials, args.objective)
        else:
            results_df = run_bayesian_parallel(data, args.trials, args.objective, n_workers)
    else:
        results_df = run_grid(data, args.objective, n_workers)

    elapsed = time.time() - t_start
    print(f"\n[DONE] {len(results_df)} trials in {elapsed/60:.1f} min")

    scores = results_df.apply(lambda r: _compute_balanced_score(r.to_dict()), axis=1)
    results_df["score_balanced"] = scores

    write_results(results_df, baseline, args.objective, output_path)
    data.cleanup()

    print("\n" + "=" * 80)
    print("  Check sensitivity sheets to verify robustness.")
    print("  If top-5 share similar D0/D-1 params, the finding is solid.")
    print("=" * 80)

if __name__ == "__main__":
    main()
