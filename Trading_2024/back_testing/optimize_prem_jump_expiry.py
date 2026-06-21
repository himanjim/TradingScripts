"""
optimize_prem_jump_expiry.py
================================================================================
Bayesian (Optuna TPE) + walk-forward robustness optimiser for the EXPIRY-DAY-ONLY
ATM short-straddle-with-reattempts strategy implemented in
`atm_straddle_prem_jump_reattempt.py`.

What it tunes
-------------
  ENTRY_TIME_IST              entry minute of the ATM straddle
  LOSS_LIMIT_RUPEES           combined rupee stoploss
  PROFIT_PROTECT_TRIGGER      trailing giveback trigger G (0 = disabled)
  MAX_REATTEMPTS              number of re-entries after a stop/protect exit
  REENTRY_DELAY_MINUTES       wait after an exit before re-entering

How it works (why this is "better" than plain Bayesian on total PnL)
--------------------------------------------------------------------
  * The strategy's `simulate_day_multi_trades` reads every tunable from the
    module's globals AT CALL TIME, so we mutate those globals per trial and
    re-run the in-memory simulation. No edits to your source file are needed.
  * Underlying 1-min data is downloaded ONCE and cached to disk; Optuna reruns
    never touch Kite again.
  * Only DTE==0 (expiry-day) campaigns are simulated.
  * The objective is a ROBUSTNESS-AWARE composite (not raw PnL), evaluated under
    chronological K-fold cross-validation and aggregated as
        robust = mean(fold_utility) - LAMBDA*std(fold_utility) - worst-fold penalty
    Each fold's utility blends win-rate (accuracy, dominant), per-day Sharpe,
    profit factor and return/max-drawdown, all mapped to ~[0,1]. This stops the
    optimiser from picking a tiny-stop / fat-tail solution that "looks" accurate.
  * Optuna sampler = TPE (Bayesian), multivariate + grouped, with a MedianPruner
    that prunes weak trials after the first folds.

Modes
-----
  optimize     (default)  one robust parameter set on the whole expiry-day set,
                          plus an in-sample per-fold consistency table.
  walkforward             nested expanding-window walk-forward: re-optimise on
                          each train window, evaluate OOS on the next window.
                          This is the honest robustness validation.

Run
---
  python optimize_prem_jump_expiry.py                  # optimize, default trials
  python optimize_prem_jump_expiry.py --mode walkforward
  OPT_N_TRIALS=600 UNDERLYING_FILTER=NIFTY python optimize_prem_jump_expiry.py

Notes
-----
  * Single-process by design (n_jobs=1): trials share the strategy module's
    globals, so parallel trials would corrupt each other. For parallelism you
    would need process isolation (out of scope here).
  * Requires: optuna, pandas, numpy. (openpyxl optional, for the .xlsx dump.)
"""

from __future__ import annotations

import os
import sys
import json
import glob
import argparse
import importlib.util
from datetime import datetime, date, time as dtime
from typing import Dict, List, Tuple, Optional, Any

import numpy as np
import pandas as pd

try:
    import optuna
    from optuna.samplers import TPESampler
    from optuna.pruners import MedianPruner
except Exception:  # pragma: no cover
    optuna = None  # handled in main()


# =============================================================================
# CONFIG  (env-overridable)
# =============================================================================

# Path to the strategy file we optimise. Defaults to the uploaded filename.
STRATEGY_PATH = os.getenv(
    "STRATEGY_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "atm_straddle_prem_jump_reattempt.py"),
)

# Optimise on this many months of history (expiry days only).
OPT_LOOKBACK_MONTHS = int(os.getenv("OPT_LOOKBACK_MONTHS", "18"))

# "NIFTY", "SENSEX", or "" / "BOTH" to use every expiry-day campaign.
# Strong recommendation: NIFTY and SENSEX 0-DTE behave very differently;
# optimising each separately usually beats a single shared parameter set.
UNDERLYING_FILTER = os.getenv("UNDERLYING_FILTER", "").strip().upper()

# If True, keep at most one underlying per calendar day (prefer NIFTY on ties),
# mirroring the strategy's "actual_trades" convention. They rarely collide on
# expiry days, so False (keep both) just yields more samples.
ONE_PER_DAY = os.getenv("ONE_PER_DAY", "0").strip() == "1"

# Optuna
OPT_N_TRIALS = int(os.getenv("OPT_N_TRIALS", "400"))
OPT_SEED = int(os.getenv("OPT_SEED", "42"))
USE_PRUNER = os.getenv("OPT_PRUNE", "1").strip() == "1"

# Cross-validation
K_FOLDS = int(os.getenv("OPT_KFOLDS", "5"))
LAMBDA_STD = float(os.getenv("OPT_LAMBDA_STD", "0.5"))     # penalty on cross-fold std
WORST_FOLD_W = float(os.getenv("OPT_WORST_W", "0.5"))     # penalty on worst fold being negative

# Sample-count guards (reject degenerate solutions)
MIN_TOTAL_SAMPLES = int(os.getenv("OPT_MIN_SAMPLES", "40"))
MIN_FOLD_SAMPLES = int(os.getenv("OPT_MIN_FOLD_SAMPLES", "6"))

# Composite utility weights (per fold). Win-rate dominant per the "accuracy" ask.
W_ACC = float(os.getenv("OPT_W_ACC", "0.40"))   # win rate
W_SHARPE = float(os.getenv("OPT_W_SHARPE", "0.30"))  # per-day Sharpe
W_RETDD = float(os.getenv("OPT_W_RETDD", "0.20"))    # total / max drawdown
W_PF = float(os.getenv("OPT_W_PF", "0.10"))          # profit factor
# Bounded-map caps for the [0,1] normalisation
SHARPE_CAP = float(os.getenv("OPT_SHARPE_CAP", "0.50"))  # per-day Sharpe ~0.5 is strong
RETDD_CAP = float(os.getenv("OPT_RETDD_CAP", "5.0"))
PF_CAP = float(os.getenv("OPT_PF_CAP", "3.0"))

# Search-space bounds
ENTRY_MIN_LO = int(os.getenv("OPT_ENTRY_MIN_LO", "5"))    # minutes after 09:15 -> 09:20
ENTRY_MIN_HI = int(os.getenv("OPT_ENTRY_MIN_HI", "330"))  # -> 14:45
ENTRY_MIN_STEP = int(os.getenv("OPT_ENTRY_STEP", "5"))

SL_LO = int(os.getenv("OPT_SL_LO", "2000"))
SL_HI = int(os.getenv("OPT_SL_HI", "40000"))
SL_STEP = int(os.getenv("OPT_SL_STEP", "500"))

PP_LO = int(os.getenv("OPT_PP_LO", "2000"))
PP_HI = int(os.getenv("OPT_PP_HI", "40000"))
PP_STEP = int(os.getenv("OPT_PP_STEP", "500"))

RE_LO = int(os.getenv("OPT_RE_LO", "0"))
RE_HI = int(os.getenv("OPT_RE_HI", "5"))

RDM_LO = int(os.getenv("OPT_RDM_LO", "1"))
RDM_HI = int(os.getenv("OPT_RDM_HI", "60"))

# Walk-forward (nested) settings
WF_FOLDS = int(os.getenv("OPT_WF_FOLDS", "6"))            # number of OOS test windows
WF_MIN_TRAIN_FOLDS = int(os.getenv("OPT_WF_MIN_TRAIN", "2"))
WF_TRIALS = int(os.getenv("OPT_WF_TRIALS", "200"))       # trials per train window

# Output
def _downloads() -> str:
    from pathlib import Path
    d = Path.home() / "Downloads"
    return str(d if d.exists() else Path.home())

OUT_DIR = os.getenv("OPT_OUT_DIR", os.path.join(_downloads(), "straddle_opt"))
CACHE_DIR = os.getenv("OPT_CACHE_DIR", os.path.join(OUT_DIR, "cache"))
REFRESH_DATA = os.getenv("OPT_REFRESH_DATA", "0").strip() == "1"

BIG_NEG = -10.0  # objective floor for rejected trials


# =============================================================================
# STRATEGY MODULE LOADER
# =============================================================================

def load_strategy(path: str):
    """Import the strategy file as a module (executes its top-level imports)."""
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Strategy file not found: {path}\n"
            f"Set STRATEGY_PATH to point at atm_straddle_prem_jump_reattempt.py"
        )
    spec = importlib.util.spec_from_file_location("strat_mod", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["strat_mod"] = mod
    spec.loader.exec_module(mod)  # type: ignore
    return mod


# =============================================================================
# DATA PREP  (run once, cached)
# =============================================================================

def _underlying_cache_path(window_start: date, end_day: date) -> str:
    os.makedirs(CACHE_DIR, exist_ok=True)
    return os.path.join(CACHE_DIR, f"underlyings_{window_start}_{end_day}.pkl")


def get_underlyings(mod, window_start: date, end_day: date) -> Dict[str, pd.DataFrame]:
    """Download underlying 1-min data once; cache to disk for subsequent runs."""
    cache = _underlying_cache_path(window_start, end_day)
    if (not REFRESH_DATA) and os.path.exists(cache):
        print(f"[CACHE] Loading underlyings from {cache}")
        return pd.read_pickle(cache)

    print("[STEP] Initializing Kite (one-time, for underlying download) ...")
    kite = mod.oUtils.intialize_kite_api()
    print("[OK] Kite ready.")
    data = mod.download_underlyings(kite, window_start, end_day)
    pd.to_pickle(data, cache)
    print(f"[CACHE] Saved underlyings to {cache}")
    return data


def _prepare_option_frame(mod, df: pd.DataFrame) -> pd.DataFrame:
    """Replicates the PASS-2 per-file option normalisation from the strategy."""
    needed = ["date", "name", "type", "option_type", "strike", "expiry",
              "instrument", "high", "low", "close"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns {missing}")

    d2 = df[df["type"].astype(str).str.upper().eq("OPTION")][needed].copy()
    if d2.empty:
        return d2
    d2["date"] = mod.ensure_ist(d2["date"])
    d2["day"] = d2["date"].dt.date
    d2["underlying"] = d2["name"].astype(str).map(mod.normalize_underlying)
    d2 = d2[d2["underlying"].isin(mod.TRADEABLE)]
    if d2.empty:
        return d2
    d2["expiry_date"] = pd.to_datetime(d2["expiry"], errors="coerce").dt.date
    d2["strike_num"] = pd.to_numeric(d2["strike"], errors="coerce")
    d2["strike_int"] = d2["strike_num"].round().astype("Int64")
    d2["option_type"] = d2["option_type"].astype(str).str.upper()
    d2 = d2.dropna(subset=["day", "underlying", "expiry_date", "strike_int", "close"])
    d2["strike_int"] = d2["strike_int"].astype(int)
    d2 = d2[d2["expiry_date"] >= d2["day"]]
    return d2


def collect_expiry_units(mod, paths, min_expiry_map, underlying_data,
                         window_start, window_end) -> List[Dict[str, Any]]:
    """
    Build the in-memory list of DTE==0 simulation units:
        {und, day, expiry, day_opt(DataFrame), underlying_day(DataFrame)}
    """
    # Optional one-underlying-per-day selection (DTE==0 only)
    sel: Optional[Dict[date, str]] = None
    if ONE_PER_DAY:
        by_day: Dict[date, List[Tuple[date, str]]] = {}
        for (und, dy), ex in min_expiry_map.items():
            if und not in mod.TRADEABLE:
                continue
            if int((ex - dy).days) != 0:
                continue
            by_day.setdefault(dy, []).append((ex, und))
        sel = {}
        for dy, lst in by_day.items():
            lst.sort(key=lambda t: (t[0], 0 if t[1] == "NIFTY" else 1))
            sel[dy] = lst[0][1]

    keep_und = None
    if UNDERLYING_FILTER in ("NIFTY", "SENSEX"):
        keep_und = UNDERLYING_FILTER

    units: List[Dict[str, Any]] = []
    processed: set = set()

    for p in paths:
        try:
            df = pd.read_pickle(p)
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue
            d2 = _prepare_option_frame(mod, df)
            if d2.empty:
                continue
            d2 = d2[(d2["day"] >= window_start) & (d2["day"] <= window_end)]
            if d2.empty:
                continue

            for (und, dy, ex), g in d2.groupby(["underlying", "day", "expiry_date"], sort=False):
                if min_expiry_map.get((und, dy)) != ex:
                    continue
                if int((ex - dy).days) != 0:          # EXPIRY DAY ONLY
                    continue
                if keep_und is not None and und != keep_und:
                    continue
                if sel is not None and sel.get(dy) != und:
                    continue
                key = (und, dy, ex)
                if key in processed:
                    continue
                processed.add(key)

                uday = underlying_data.get(und)
                if uday is None:
                    continue
                uday = uday[uday["day"] == dy]
                if uday.empty:
                    continue

                units.append({
                    "und": und,
                    "day": dy,
                    "expiry": ex,
                    "day_opt": g.copy(),
                    "underlying_day": uday.copy(),
                })
            print(f"[PREP OK] {os.path.basename(p)}")
        except Exception as e:
            print(f"[PREP WARN] {os.path.basename(p)} failed: {e}")

    units.sort(key=lambda u: (u["day"], u["und"]))
    return units


# =============================================================================
# PARAMETERS  ->  strategy globals
# =============================================================================

def entry_minutes_to_dtime(minutes_after_open: int) -> dtime:
    total = 9 * 60 + 15 + int(minutes_after_open)
    return dtime(total // 60, total % 60)


def apply_params(mod, params: Dict[str, Any]) -> None:
    et = entry_minutes_to_dtime(params["entry_minutes"])
    mod.ENTRY_TIME = et
    mod.ENTRY_TIME_IST = et.strftime("%H:%M")
    mod.LOSS_LIMIT_RUPEES = int(params["loss_limit"])
    mod.PROFIT_PROTECT_TRIGGER_RUPEES = int(params["profit_protect"])
    mod.MAX_REATTEMPTS = int(params["max_reattempts"])
    mod.REENTRY_DELAY_MINUTES = int(params["reentry_delay"])
    mod.INCLUDE_TRANSACTION_COSTS = True


def suggest_params(trial) -> Dict[str, Any]:
    entry_minutes = trial.suggest_int("entry_minutes_after_open",
                                      ENTRY_MIN_LO, ENTRY_MIN_HI, step=ENTRY_MIN_STEP)
    loss_limit = trial.suggest_int("loss_limit_rupees", SL_LO, SL_HI, step=SL_STEP)

    pp_enabled = trial.suggest_categorical("profit_protect_enabled", [True, False])
    if pp_enabled:
        profit_protect = trial.suggest_int("profit_protect_rupees", PP_LO, PP_HI, step=PP_STEP)
    else:
        profit_protect = 0

    max_reattempts = trial.suggest_int("max_reattempts", RE_LO, RE_HI)
    if max_reattempts > 0:
        reentry_delay = trial.suggest_int("reentry_delay_minutes", RDM_LO, RDM_HI)
    else:
        reentry_delay = 0

    return {
        "entry_minutes": entry_minutes,
        "loss_limit": loss_limit,
        "profit_protect": profit_protect,
        "max_reattempts": max_reattempts,
        "reentry_delay": reentry_delay,
    }


# =============================================================================
# SIMULATION RUNNER  &  METRICS
# =============================================================================

def run_sim_collect(mod, units: List[Dict[str, Any]]) -> pd.DataFrame:
    """Run the strategy across all expiry units; return one row per (und, day) campaign."""
    rows = []
    for u in units:
        try:
            trades, _ = mod.simulate_day_multi_trades(
                und=u["und"], dy=u["day"], expiry=u["expiry"],
                day_opt=u["day_opt"], underlying_day=u["underlying_day"],
            )
        except Exception:
            continue
        if not trades:
            continue
        pnl = float(np.nansum([t.exit_pnl for t in trades]))
        rows.append({
            "day": u["day"],
            "underlying": u["und"],
            "daily_pnl": pnl,
            "n_trades": len(trades),
        })
    return pd.DataFrame(rows)


def _metrics(daily: pd.DataFrame) -> Dict[str, float]:
    """Campaign-level metrics from a daily-pnl DataFrame (one row per campaign)."""
    d = daily["daily_pnl"].to_numpy(dtype=float)
    n = len(d)
    if n == 0:
        return {"n": 0}
    wins = int((d > 0).sum())
    total = float(d.sum())
    mean = float(d.mean())
    std = float(d.std(ddof=1)) if n > 1 else (abs(mean) + 1.0)
    sharpe = mean / (std + 1e-9)
    gp = float(d[d > 0].sum())
    gl = float(-d[d < 0].sum())
    pf = gp / (gl + 1e-9)
    equity = np.cumsum(d)
    peak = np.maximum.accumulate(equity)
    max_dd = float(np.max(peak - equity)) if n else 0.0
    ret_dd = total / (max_dd + 1e-9)
    k = max(1, int(np.ceil(0.05 * n)))
    cvar5 = float(np.sort(d)[:k].mean())
    return {
        "n": n,
        "win_rate": wins / n,
        "total_pnl": total,
        "avg_pnl": mean,
        "std_pnl": std,
        "sharpe_day": sharpe,
        "profit_factor": pf,
        "max_drawdown": max_dd,
        "return_over_dd": ret_dd,
        "cvar5": cvar5,
        "total_trades": int(daily["n_trades"].sum()),
    }


def _clip01(x: float) -> float:
    return float(min(1.0, max(0.0, x)))


def fold_utility(daily: pd.DataFrame) -> Optional[float]:
    """Robust [~ -1 .. 1] utility for one fold. None if too few samples."""
    m = _metrics(daily)
    if m.get("n", 0) < MIN_FOLD_SAMPLES:
        return None

    if m["avg_pnl"] <= 0 or m["total_pnl"] <= 0:
        # Losing fold: keep a smooth, ordered negative score so TPE still learns.
        return -0.5 + 0.5 * float(np.tanh(m["total_pnl"] / (abs(m["max_drawdown"]) + 1e3)))

    acc_s = _clip01(m["win_rate"])
    sharpe_s = _clip01(m["sharpe_day"] / SHARPE_CAP)
    pf_s = _clip01((m["profit_factor"] - 1.0) / (PF_CAP - 1.0))
    retdd_s = _clip01(m["return_over_dd"] / RETDD_CAP)

    return W_ACC * acc_s + W_SHARPE * sharpe_s + W_PF * pf_s + W_RETDD * retdd_s


def make_folds(daily: pd.DataFrame, k: int) -> List[pd.DataFrame]:
    """Chronological folds; whole calendar days never split across folds."""
    days = sorted(daily["day"].unique())
    if len(days) < k:
        k = max(1, len(days))
    splits = np.array_split(np.array(days, dtype=object), k)
    folds = []
    for s in splits:
        sset = set(s.tolist())
        folds.append(daily[daily["day"].isin(sset)])
    return folds


def robust_score_from_folds(folds: List[pd.DataFrame], trial=None) -> float:
    futils: List[float] = []
    for i, f in enumerate(folds):
        fu = fold_utility(f)
        if fu is None:
            return BIG_NEG
        futils.append(fu)
        if trial is not None:
            trial.report(float(np.mean(futils)), step=i)
            if USE_PRUNER and trial.should_prune():
                raise optuna.TrialPruned()
    arr = np.asarray(futils, dtype=float)
    return float(arr.mean() - LAMBDA_STD * arr.std() - WORST_FOLD_W * max(0.0, -arr.min()))


# =============================================================================
# OBJECTIVE
# =============================================================================

def build_objective(mod, units: List[Dict[str, Any]]):
    def objective(trial) -> float:
        params = suggest_params(trial)
        apply_params(mod, params)

        daily = run_sim_collect(mod, units)
        if daily.empty or len(daily) < MIN_TOTAL_SAMPLES:
            return BIG_NEG

        daily = daily.sort_values(["day", "underlying"]).reset_index(drop=True)
        folds = make_folds(daily, K_FOLDS)
        score = robust_score_from_folds(folds, trial=trial)

        m = _metrics(daily)
        trial.set_user_attr("win_rate", round(m.get("win_rate", 0.0), 4))
        trial.set_user_attr("total_pnl", round(m.get("total_pnl", 0.0), 2))
        trial.set_user_attr("avg_pnl", round(m.get("avg_pnl", 0.0), 2))
        trial.set_user_attr("sharpe_day", round(m.get("sharpe_day", 0.0), 4))
        trial.set_user_attr("profit_factor", round(m.get("profit_factor", 0.0), 3))
        trial.set_user_attr("max_drawdown", round(m.get("max_drawdown", 0.0), 2))
        trial.set_user_attr("n_campaigns", int(m.get("n", 0)))
        trial.set_user_attr("total_trades", int(m.get("total_trades", 0)))
        return score
    return objective


def params_from_optuna(p: Dict[str, Any]) -> Dict[str, Any]:
    """Normalise Optuna's (possibly conditional) params into the full param dict."""
    pp = p.get("profit_protect_rupees", 0) if p.get("profit_protect_enabled", False) else 0
    re = int(p.get("max_reattempts", 0))
    rdm = int(p.get("reentry_delay_minutes", 0)) if re > 0 else 0
    return {
        "entry_minutes": int(p["entry_minutes_after_open"]),
        "loss_limit": int(p["loss_limit_rupees"]),
        "profit_protect": int(pp),
        "max_reattempts": re,
        "reentry_delay": rdm,
    }


def pretty_params(params: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "ENTRY_TIME_IST": entry_minutes_to_dtime(params["entry_minutes"]).strftime("%H:%M"),
        "LOSS_LIMIT_RUPEES": params["loss_limit"],
        "PROFIT_PROTECT_TRIGGER_RUPEES": params["profit_protect"],
        "MAX_REATTEMPTS": params["max_reattempts"],
        "REENTRY_DELAY_MINUTES": params["reentry_delay"],
    }


# =============================================================================
# MODE: OPTIMIZE
# =============================================================================

def run_optimize(mod, units: List[Dict[str, Any]]):
    os.makedirs(OUT_DIR, exist_ok=True)
    sampler = TPESampler(seed=OPT_SEED, multivariate=True, group=True)
    pruner = MedianPruner(n_startup_trials=20, n_warmup_steps=1) if USE_PRUNER else optuna.pruners.NopPruner()
    study = optuna.create_study(direction="maximize", sampler=sampler, pruner=pruner)

    objective = build_objective(mod, units)
    study.optimize(objective, n_trials=OPT_N_TRIALS, n_jobs=1, show_progress_bar=False)

    best = params_from_optuna(study.best_params)
    print("\n" + "=" * 78)
    print(f"BEST robust score: {study.best_value:.4f}")
    print("BEST parameters (expiry-day only):")
    for k, v in pretty_params(best).items():
        print(f"    {k:32s} = {v}")
    ba = study.best_trial.user_attrs
    print("\nIn-sample metrics at best params:")
    for k in ("n_campaigns", "total_trades", "win_rate", "total_pnl",
              "avg_pnl", "sharpe_day", "profit_factor", "max_drawdown"):
        print(f"    {k:16s} = {ba.get(k)}")

    # Per-fold consistency table at best params
    apply_params(mod, best)
    daily = run_sim_collect(mod, units).sort_values(["day", "underlying"]).reset_index(drop=True)
    folds = make_folds(daily, K_FOLDS)
    fold_rows = []
    for i, f in enumerate(folds, start=1):
        fm = _metrics(f)
        fm["fold"] = i
        fm["from_day"] = str(f["day"].min())
        fm["to_day"] = str(f["day"].max())
        fold_rows.append(fm)
    fold_df = pd.DataFrame(fold_rows)
    print("\nPer-fold consistency (in-sample) at best params:")
    cols = ["fold", "from_day", "to_day", "n", "win_rate", "total_pnl",
            "avg_pnl", "sharpe_day", "profit_factor", "max_drawdown"]
    print(fold_df[[c for c in cols if c in fold_df.columns]].to_string(index=False))

    # Persist
    trials_df = study.trials_dataframe()
    trials_csv = os.path.join(OUT_DIR, "optuna_trials.csv")
    trials_df.to_csv(trials_csv, index=False)

    best_json = os.path.join(OUT_DIR, "best_params.json")
    with open(best_json, "w") as fh:
        json.dump({
            "robust_score": study.best_value,
            "params_pretty": pretty_params(best),
            "params_raw": best,
            "in_sample_metrics": {k: ba.get(k) for k in ba},
            "config": {
                "OPT_LOOKBACK_MONTHS": OPT_LOOKBACK_MONTHS,
                "UNDERLYING_FILTER": UNDERLYING_FILTER or "BOTH",
                "ONE_PER_DAY": ONE_PER_DAY,
                "K_FOLDS": K_FOLDS,
                "LAMBDA_STD": LAMBDA_STD,
                "weights": {"acc": W_ACC, "sharpe": W_SHARPE, "retdd": W_RETDD, "pf": W_PF},
            },
        }, fh, indent=2, default=str)

    try:
        xlsx = os.path.join(OUT_DIR, "optimize_report.xlsx")
        with pd.ExcelWriter(xlsx, engine="openpyxl") as xw:
            pd.DataFrame([pretty_params(best)]).to_excel(xw, sheet_name="best_params", index=False)
            fold_df.to_excel(xw, sheet_name="fold_consistency", index=False)
            daily.to_excel(xw, sheet_name="daily_campaigns", index=False)
            trials_df.to_excel(xw, sheet_name="optuna_trials", index=False)
        print(f"\n[OUT] {xlsx}")
    except Exception as e:
        print(f"[WARN] xlsx skipped ({e}); CSV/JSON written instead.")

    print(f"[OUT] {trials_csv}")
    print(f"[OUT] {best_json}")

    print("\nReady-to-run env block for the original backtest:")
    pp = pretty_params(best)
    for k, v in pp.items():
        print(f"    set {k}={v}" if os.name == "nt" else f"    export {k}={v}")

    return study, best


# =============================================================================
# MODE: NESTED WALK-FORWARD  (honest OOS robustness)
# =============================================================================

def _optimize_on(units_subset: List[Dict[str, Any]], mod, n_trials: int, seed: int):
    sampler = TPESampler(seed=seed, multivariate=True, group=True)
    study = optuna.create_study(direction="maximize", sampler=sampler,
                                pruner=optuna.pruners.NopPruner())
    study.optimize(build_objective(mod, units_subset), n_trials=n_trials, n_jobs=1)
    return params_from_optuna(study.best_params), study.best_value


def run_walkforward(mod, units: List[Dict[str, Any]]):
    os.makedirs(OUT_DIR, exist_ok=True)
    days = sorted({u["day"] for u in units})
    blocks = np.array_split(np.array(days, dtype=object), WF_FOLDS)
    block_daysets = [set(b.tolist()) for b in blocks]

    print(f"[WF] {len(days)} expiry days -> {WF_FOLDS} blocks; "
          f"min train blocks = {WF_MIN_TRAIN_FOLDS}")

    oos_rows = []
    for test_i in range(WF_MIN_TRAIN_FOLDS, WF_FOLDS):
        train_days: set = set()
        for j in range(test_i):
            train_days |= block_daysets[j]
        test_days = block_daysets[test_i]

        train_units = [u for u in units if u["day"] in train_days]
        test_units = [u for u in units if u["day"] in test_days]
        if len(train_units) < MIN_TOTAL_SAMPLES or len(test_units) < MIN_FOLD_SAMPLES:
            print(f"[WF] window {test_i}: insufficient data, skipped")
            continue

        best, train_score = _optimize_on(train_units, mod, WF_TRIALS, OPT_SEED + test_i)

        # OOS evaluation
        apply_params(mod, best)
        test_daily = run_sim_collect(mod, test_units)
        tm = _metrics(test_daily) if not test_daily.empty else {"n": 0}

        row = {
            "window": test_i,
            "train_days": len(train_days),
            "test_from": str(min(test_days)),
            "test_to": str(max(test_days)),
            "train_robust": round(train_score, 4),
            **pretty_params(best),
            "oos_n": tm.get("n", 0),
            "oos_win_rate": round(tm.get("win_rate", 0.0), 4),
            "oos_total_pnl": round(tm.get("total_pnl", 0.0), 2),
            "oos_avg_pnl": round(tm.get("avg_pnl", 0.0), 2),
            "oos_sharpe_day": round(tm.get("sharpe_day", 0.0), 4),
            "oos_profit_factor": round(tm.get("profit_factor", 0.0), 3),
            "oos_max_dd": round(tm.get("max_drawdown", 0.0), 2),
        }
        oos_rows.append(row)
        print(f"[WF] window {test_i} OOS: "
              f"win={row['oos_win_rate']:.2%} pnl={row['oos_total_pnl']:.0f} "
              f"PF={row['oos_profit_factor']:.2f} params={pretty_params(best)}")

    wf_df = pd.DataFrame(oos_rows)
    if not wf_df.empty:
        n_profitable = int((wf_df["oos_total_pnl"] > 0).sum())
        print("\n" + "=" * 78)
        print(f"WALK-FORWARD OOS: {n_profitable}/{len(wf_df)} windows profitable")
        print(f"OOS total PnL (sum):  {wf_df['oos_total_pnl'].sum():.0f}")
        print(f"OOS win-rate (mean):  {wf_df['oos_win_rate'].mean():.2%}")
        print(wf_df.to_string(index=False))
        out = os.path.join(OUT_DIR, "walkforward_oos.csv")
        wf_df.to_csv(out, index=False)
        print(f"\n[OUT] {out}")
    else:
        print("[WF] No OOS windows evaluated. Increase lookback or reduce WF_FOLDS.")
    return wf_df


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Optimise expiry-day ATM short straddle.")
    parser.add_argument("--mode", choices=["optimize", "walkforward"], default="optimize")
    args = parser.parse_args()

    if optuna is None:
        raise SystemExit("optuna is required:  pip install optuna")

    optuna.logging.set_verbosity(optuna.logging.WARNING)

    print(f"[STEP] Loading strategy: {STRATEGY_PATH}")
    mod = load_strategy(STRATEGY_PATH)

    paths = sorted(glob.glob(os.path.join(mod.PICKLES_DIR, "*.pkl"))
                   + glob.glob(os.path.join(mod.PICKLES_DIR, "*.pickle")))
    if not paths:
        raise FileNotFoundError(f"No pickles in {mod.PICKLES_DIR}")
    print(f"[INFO] Pickles: {len(paths)}")

    end_day, min_expiry_map, min_day_seen = mod.scan_pickles_pass1(paths)
    window_start = mod.compute_window_start(end_day, OPT_LOOKBACK_MONTHS)
    print(f"[INFO] Optimise window: {window_start} -> {end_day} "
          f"(filter={UNDERLYING_FILTER or 'BOTH'}, one_per_day={ONE_PER_DAY})")

    underlying_data = get_underlyings(mod, window_start, end_day)
    units = collect_expiry_units(mod, paths, min_expiry_map, underlying_data,
                                 window_start, end_day)
    print(f"[INFO] Expiry-day (DTE=0) units collected: {len(units)}")
    if len(units) < MIN_TOTAL_SAMPLES:
        raise SystemExit(f"Too few expiry-day units ({len(units)}). "
                         f"Increase OPT_LOOKBACK_MONTHS or check data.")

    if args.mode == "optimize":
        run_optimize(mod, units)
    else:
        run_walkforward(mod, units)


if __name__ == "__main__":
    main()
