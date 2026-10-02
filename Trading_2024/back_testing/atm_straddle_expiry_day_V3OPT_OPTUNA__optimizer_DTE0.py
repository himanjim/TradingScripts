"""
V3-OPT ATM short-straddle backtester + Optuna profit optimizer
FAST-REENTRY EDITION (matches straddle_config_v3opt_DTE0_fastreentry.properties)

This file is the optimizer-enabled version of:
    A. atm_straddle_expiry_day_V3OPT.py                       (strategy engine)
    B. straddle_config_v3opt_DTE0_fastreentry.properties      (strategy config)

The strategy rules are NOT changed. The reference simulator below
(simulate_day_multi_trades) is a line-for-line parameterised copy of the one in
atm_straddle_expiry_day_V3OPT.py:
    * hard entry/exit cut-offs;
    * momentum-gated first entries and re-entries;
    * percentage stop-loss with an absolute rupee cap;
    * per-underlying profit targets, late targets, profit-protect settings,
      late-session tighter giveback, and daily-loss circuit breakers;
    * target-based re-entry and stop/protect re-entry;
    * transaction charges and actual-trade selection.

RUN_MODE is read from the same property file:
    RUN_MODE=backtest  -> one normal backtest and Excel report (unchanged).
    RUN_MODE=optimize  -> load data once (or from the local cache), then search.

Dependencies:
    pandas, numpy, openpyxl, optuna (3.1+; tested on 5.0)
    optional: cmaes  (pip install cmaes) for the CMA-ES refinement phase

=============================================================================
CHANGE LOG - fast-reentry edition (all changes are in the OPTIMIZER only)
=============================================================================
1. CONFIG / SEARCH SPACE (section "RUN CONTROL AND OPTIMIZER SETTINGS")
   a. Default property file is now configs/straddle_config_v3opt_DTE0_
      fastreentry.properties and the default study name is new, so an old
      SQLite study built on the old baseline is never resumed by accident.
   b. Ranges now CONTAIN the fast-reentry values. Previously four values fell
      outside the ranges and the "seed" trial silently replaced them:
        PROFIT_TARGET_REENTRY_DELAY=0      (old range 1-10   -> now 0-5)
        MAX_REATTEMPTS=20                  (old range 3-12   -> now 10-30)
        EXIT_TIME_IST=15:40                (engine clamps to 15:30; the seed
                                            now uses the clamped value)
        MAX_LOSS_LIMIT_RUPEES_BY_ATTEMPT=2200 (old grid 2000+250k rounded it
                                            to 2250 -> now 1500-3500 step 50)
   c. The seed trial now reproduces the property file EXACTLY. The optimizer
      checks this itself: trial 0 must equal the exact baseline to the paisa.
   d. Dimensions that no longer change the result were removed:
        - stop-loss % list (base/step/growth = 3 dims) -> ONE scale factor on
          the property-file list (the Rs cap binds on almost every expiry-day
          attempt, so the % only matters late in the day; the scale keeps the
          list's shape and makes the seed exact at scale=1.0);
        - re-entry delay base/step/growth (3 dims) -> ONE flat delay;
        - ENTRY_MOMENTUM_GATE / MOMENTUM_LOOKBACK_MIN and
          PROFIT_TARGET_REENTRY_ENABLED are frozen at the property-file values
          (set OPT_TUNE_SWITCHES=1 to search them again; the lookback is then
          only searched when the gate is on).
      28 searched dimensions -> 21.
   e. Profile ranges re-centred where the fast-reentry values sat on an edge
      (NIFTY target 0.58, NIFTY daily loss 17k, SENSEX late target 0.35,
      SENSEX protect 0.47).
   f. Floats are rounded to 6 decimals inside each trial, so the BEST
      .properties file reproduces the optimizer's number exactly when it is
      run through atm_straddle_expiry_day_V3OPT.py.

2. SPEED
   a. Only the day-groups that are actually traded (nearest expiry, DTE in
      ALLOWED_DTE, the one underlying picked for that date) are simulated.
      The old optimizer simulated every nearest-expiry day for BOTH indices
      and discarded most of them afterwards.
   b. New numpy "fast engine" (section FAST ENGINE). Every per-day array
      (option close/high/low per strike, forward-filled close, underlying
      as-of close per minute) is built once; each attempt is pure numpy on
      integer minute indices. Profiling on synthetic expiry days showed the
      old pandas path at ~9.5 ms per attempt (about half of it inside
      Series.combine for the late-giveback trail, ~14% in asof_close); the
      numpy kernel does the same maths in tens of microseconds.
   c. The objective returns per-day net P&L only. No TradeRow objects, no
      DataFrames, no groupby per trial. The full pandas reference engine is
      used only for the baseline, the parity checks and the final reports.
   d. PARITY IS ENFORCED: at start-up the fast engine is compared with the
      reference engine on the exact baseline (all days) and on random
      parameter sets; any difference above Rs 0.01 per day stops the run.
      The winner is re-checked the same way at the end.
   e. Loaded data is cached to a local file (OPT_DATA_CACHE). Re-runs skip the
      pickle scan, the Kite download and the day-group build entirely. The
      cache key includes every pickle's size and modification time.
   f. Optuna storage defaults to a JournalStorage log file (cheap appends,
      Windows-safe file lock, resumable) instead of SQLite, and each trial
      writes ONE user attribute instead of eleven.
   Measured end to end on the same synthetic 6-month data set (53 traded
   days out of 264 nearest-expiry day-groups): old optimizer ~4.1 s per
   trial, this version ~0.03 s per trial including sampler and storage.

3. SEARCH ALGORITHM
   a. TPE is now multivariate (multivariate=True, group=True) so it models
      interactions between target, protect, stop cap and re-entry delay.
   b. Optional CMA-ES refinement phase (OPT_REFINE_TRIALS), warm-started from
      the best TPE trials (WS-CMA-ES via Optuna's source_trials). Skipped with
      a message if the 'cmaes' package is absent.

4. HONEST SCORING
   a. OPT_SLIPPAGE_PTS_<UND>: optional per-order slippage (points per leg per
      order; 4 orders per attempt) deducted inside the objective. Default 0
      keeps the score identical to the strategy engine.
   b. Stress report: the top trials and the baseline are re-scored with
      OPT_STRESS_SLIPPAGE_PTS_<UND> so you can see which settings survive
      imperfect fills. Calibrate these from your live trader's fills.
   c. OPT_HOLDOUT_FROM=YYYY-MM-DD: days on/after this date are excluded from
      the objective and reported separately for the top trials (true
      out-of-sample check). Blank = all days in-sample (old behaviour).

5. SESSION END (closing auction session, from 3 Aug 2026)
   SESSION_END_IST (default 15:30) sets the last minute of option data the
   engine reads and monitors. Set SESSION_END_IST=15:40 to trade/monitor until
   the new derivatives close. The exit-time search range, the data cache key
   and the EXIT_TIME_IST clamp all follow it. day_session_end() keeps any day
   whose option data has no bar after 15:30 (everything before CAS) on the old
   15:30 session, so those days give exactly the same trades as before. The
   same change (SESSION_END_IST line + day_session_end + one call) is needed
   in atm_straddle_expiry_day_V3OPT.py so its backtests match.

NEW / CHANGED PROPERTY KEYS (all optional; defaults shown in the code below)
    OPT_SAMPLER, OPT_REFINE_TRIALS, OPT_STORAGE, OPT_DATA_CACHE,
    OPT_REFRESH_DATA_CACHE, OPT_PARITY_CHECK, OPT_PARITY_RANDOM_TRIALS,
    OPT_PARITY_MAX_DAYS, OPT_HOLDOUT_FROM, OPT_SLIPPAGE_PTS_NIFTY,
    OPT_SLIPPAGE_PTS_SENSEX, OPT_STRESS_SLIPPAGE_PTS_NIFTY,
    OPT_STRESS_SLIPPAGE_PTS_SENSEX, OPT_REPORT_TOP_K, OPT_TUNE_SWITCHES,
    OPT_STOP_SCALE_MIN, OPT_STOP_SCALE_MAX, SESSION_END_IST
Removed keys (no longer used): OPT_SL_BASE_MIN/MAX, OPT_SL_STEP_MIN/MAX,
    OPT_REENTRY_STEP_MIN/MAX.
"""

# =============================================================================
# V3-OPT = V2-OPT + per-underlying profiles:
#   Any of these keys may be prefixed NIFTY_ / SENSEX_ to override per index:
#     PROFIT_TARGET_PCT, PROFIT_TARGET_PCT_LATE, PROFIT_TARGET_LATE_FROM_IST,
#     PROFIT_PROTECT_TRIGGER_RUPEES, MAX_DAILY_LOSS_RUPEES,
#     PROFIT_PROTECT_LATE_GIVEBACK, PROFIT_PROTECT_LATE_FROM_IST
#   New: PROFIT_PROTECT_LATE_GIVEBACK - once armed, from
#   PROFIT_PROTECT_LATE_FROM_IST onward the trailing giveback tightens to this
#   fraction of premium (captures late-day peaks the wide trail returns).
# -----------------------------------------------------------------------------
# V2-OPT (expiry-day profit optimization) - changes vs original:
#   1. ENTRY_MOMENTUM_GATE: first entry, stop/protect re-entries and target
#      re-entries wait (minute by minute) until the current-ATM straddle
#      premium is LOWER than it was MOMENTUM_LOOKBACK_MIN minutes earlier,
#      i.e. entries only happen while premium is already decaying.
#   2. PROFIT_TARGET_REENTRY_ENABLED: hitting the profit target no longer ends
#      the day; a fresh ATM straddle is opened after
#      PROFIT_TARGET_REENTRY_DELAY minutes (plus the momentum gate).
#   3. PROFIT_TARGET_PCT_LATE: attempts ENTERED at/after
#      PROFIT_TARGET_LATE_FROM_IST use a smaller profit target (the last-hour
#      premium crush cannot deliver the full-day percentage).
#   Everything else (stop-loss model, profit-protect, charges, daily circuit
#   breaker, quantities) is IDENTICAL to the original engine.
# =============================================================================
import os
import re
from pathlib import Path
import glob
import time
import json
import math
import pickle
import hashlib
import warnings
from dataclasses import dataclass, field
from datetime import datetime, date, time as dtime, timedelta
from typing import Dict, List, Tuple, Optional, Any

import numpy as np
import pandas as pd

import Trading_2024.OptionTradeUtils as oUtils

try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:
    ZoneInfo = None  # type: ignore

try:
    import pytz  # type: ignore
except Exception:
    pytz = None  # type: ignore

try:
    from dateutil.relativedelta import relativedelta  # type: ignore
except Exception:
    relativedelta = None  # type: ignore


# =============================================================================
# USER CONFIG
# =============================================================================
# ---------------------------------------------------------------------------
# CONFIGURATION SOURCE: external property file
# ---------------------------------------------------------------------------
# Every tunable setting lives in a simple KEY=VALUE property file so it can be
# changed WITHOUT editing this script. Override the path with the
# STRADDLE_CONFIG environment variable. Values are pushed into the process
# environment so all the os.getenv(...) reads below pick them up. A real
# environment variable that is already set takes precedence over the file.
#
# CHANGED (fast-reentry): the default now points at the fast-reentry config,
# the same file atm_straddle_expiry_day_V3OPT.py loads by default, so the
# optimizer's "exact baseline" is the configuration you are actually running.
def _load_property_file() -> str:
    cfg_path = os.getenv(
        "STRADDLE_CONFIG",
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "configs/straddle_config_v3opt_DTE0_fastreentry.properties",
        ),
    )
    if not os.path.exists(cfg_path):
        print(f"[CONFIG] Property file not found at {cfg_path}; using built-in defaults.")
        return cfg_path
    loaded = 0
    with open(cfg_path, "r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#") or line.startswith(";"):
                continue
            if "=" not in line:
                continue
            key, val = line.split("=", 1)
            key, val = key.strip(), val.strip()
            # Inline comments ("KEY=value   # note") are removed here. The
            # strategy script does NOT remove them (the note becomes part of
            # the value), so in a file both scripts read, keep comments on
            # their own line.
            parts = re.split(r"\s+#", val, maxsplit=1)
            had_inline_comment = len(parts) == 2
            val = parts[0].rstrip()
            # strip optional surrounding quotes
            if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
                val = val[1:-1]
            if key and key not in os.environ:   # real env vars win over the file
                os.environ[key] = val
                loaded += 1
                if had_inline_comment:
                    print(f"[CONFIG] {key}: inline comment removed. Put comments on their own line; "
                          "the strategy script keeps them as part of the value.")
    print(f"[CONFIG] Loaded {loaded} setting(s) from {cfg_path}")
    return cfg_path

PROPERTY_FILE_PATH = _load_property_file()


def _env_float(name: str, default: float) -> float:
    """Read a float optimizer setting; blank means 'use the default'."""
    raw = os.getenv(name, "")
    return float(raw) if str(raw).strip() != "" else float(default)


def _env_int(name: str, default: int) -> int:
    """Read an integer optimizer setting; accepts '20' or '20.0'."""
    raw = os.getenv(name, "")
    return int(float(raw)) if str(raw).strip() != "" else int(default)


def _env_flag(name: str, default: bool) -> bool:
    """Read a 0/1 optimizer switch."""
    raw = os.getenv(name, "")
    if str(raw).strip() == "":
        return bool(default)
    return str(raw).strip() == "1"


# =============================================================================
# RUN CONTROL AND OPTIMIZER SETTINGS
# =============================================================================
# All OPT_* settings can be placed in the same property file as the strategy
# settings (atm_straddle_expiry_day_V3OPT.py ignores them). A real process
# environment variable still takes precedence.

RUN_MODE = os.getenv("RUN_MODE", "optimize").strip().lower()

# ---- Search budget -----------------------------------------------------------
# Number of TPE trials in this run. Trials are now ~100x cheaper, so a larger
# budget costs minutes, not hours.
OPT_TRIALS = _env_int("OPT_TRIALS", 1000)

# NEW: search algorithm for the main phase.
#   tpe            -> multivariate TPE (models parameter interactions)  [default]
#   tpe_univariate -> the old behaviour (each parameter sampled independently)
#   cmaes          -> CMA-ES from the start (needs 'pip install cmaes')
#   random         -> random search (useful only as a sanity benchmark)
OPT_SAMPLER = os.getenv("OPT_SAMPLER", "tpe").strip().lower()

# NEW: optional CMA-ES refinement trials after the main phase. CMA-ES is
# warm-started from the best main-phase trials (its starting mean/covariance
# come from them), so it searches locally around the TPE winners.
# 0 disables. Skipped with a message if 'cmaes' is not installed.
OPT_REFINE_TRIALS = _env_int("OPT_REFINE_TRIALS", 300)

# 1 = maximize total net profit over the in-sample days.
# >1 = contiguous monthly folds; objective becomes mean fold profit minus a
#      dispersion penalty. This is optional and is not pure total-profit scoring.
OPT_CV_FOLDS = _env_int("OPT_CV_FOLDS", 1)
OPT_CV_PENALTY = _env_float("OPT_CV_PENALTY", 0.15)

OPT_SEED = _env_int("OPT_SEED", 42)
OPT_STARTUP_TRIALS = _env_int("OPT_STARTUP_TRIALS", 50)
# CHANGED: default 10 (trials are fast now; printing every trial is just noise).
# A new best is ALWAYS printed regardless of this interval.
OPT_PROGRESS_EVERY = max(1, _env_int("OPT_PROGRESS_EVERY", 10))

# ---- Output / storage ----------------------------------------------------------
_OPT_DEFAULT_PARENT = Path.home() / "Downloads"
if not _OPT_DEFAULT_PARENT.exists():
    _OPT_DEFAULT_PARENT = Path.home()
OPT_OUTPUT_DIR = os.getenv(
    "OPT_OUTPUT_DIR",
    str(_OPT_DEFAULT_PARENT / "v3opt_optimizer_runs"),
)
# CHANGED: new default study name. The old study was built on the old
# baseline and the old search space; resuming it would either fail on the
# signature check or mix two different problems in one TPE history.
_STUDY_NAME_RAW = os.getenv("OPT_STUDY_NAME", "short_straddle_v3opt_fastreentry").strip()
# The study name is also used in file names (journal/db, CSVs, BEST.properties).
# A character Windows does not allow in a file name - for example ':' from a
# time - makes the journal lock fail with "WinError 87 ... did not possess
# lock". Anything other than letters, digits, '.', '_' and '-' becomes '_'.
OPT_STUDY_NAME = re.sub(r"[^A-Za-z0-9._-]+", "_", _STUDY_NAME_RAW).strip("_") or "short_straddle_v3opt_fastreentry"
if OPT_STUDY_NAME != _STUDY_NAME_RAW:
    print(f"[CONFIG] OPT_STUDY_NAME '{_STUDY_NAME_RAW}' changed to '{OPT_STUDY_NAME}' "
          "(only letters, digits, '.', '_' and '-' are safe in file names).")

# NEW: Optuna storage backend.
#   journal -> append-only log file, resumable, Windows-safe lock   [default]
#   sqlite  -> the old behaviour (one DB transaction per write; slow at
#              millisecond-scale trials)
#   memory  -> nothing persisted (fastest; not resumable)
# OPT_SAVE_DB=0 is still honoured and means "memory".
OPT_STORAGE = os.getenv("OPT_STORAGE", "journal").strip().lower()
if os.getenv("OPT_SAVE_DB", "1").strip() == "0":
    OPT_STORAGE = "memory"

# NEW: local cache of the loaded, filtered, numpy-converted day data.
# Re-runs with unchanged pickles skip the pickle scan, the Kite download and the
# day-group build. Set OPT_REFRESH_DATA_CACHE=1 to force a rebuild (for
# example when the Kite underlying data for a recent day was incomplete).
OPT_DATA_CACHE = _env_flag("OPT_DATA_CACHE", True)
OPT_REFRESH_DATA_CACHE = _env_flag("OPT_REFRESH_DATA_CACHE", False)

# ---- Built-in verification -----------------------------------------------------
# NEW: the fast engine must reproduce the reference (pandas) engine. At start-up
# the exact baseline is compared on ALL days and OPT_PARITY_RANDOM_TRIALS random
# parameter sets on up to OPT_PARITY_MAX_DAYS evenly spaced days. Any per-day
# difference above Rs 0.01 stops the run. Set OPT_PARITY_CHECK=0 only after the
# check has passed on your data at least once.
OPT_PARITY_CHECK = _env_flag("OPT_PARITY_CHECK", True)
OPT_PARITY_RANDOM_TRIALS = max(0, _env_int("OPT_PARITY_RANDOM_TRIALS", 2))
OPT_PARITY_MAX_DAYS = max(0, _env_int("OPT_PARITY_MAX_DAYS", 60))
_PARITY_TOLERANCE_RUPEES = 0.01

# ---- Honest scoring ----------------------------------------------------------
# NEW: out-of-sample holdout. Days ON/AFTER this date are excluded from the
# objective and only reported at the end. Example: OPT_HOLDOUT_FROM=2025-11-01
# optimizes on Jul-24..Oct-25 and tests on Nov-25 onward. Blank = no holdout.
OPT_HOLDOUT_FROM_RAW = os.getenv("OPT_HOLDOUT_FROM", "").strip()
OPT_HOLDOUT_FROM: Optional[date] = (
    datetime.strptime(OPT_HOLDOUT_FROM_RAW, "%Y-%m-%d").date()
    if OPT_HOLDOUT_FROM_RAW else None
)

# NEW: slippage deducted INSIDE the objective, in option points per leg per
# order. One attempt = 4 orders (sell CE, sell PE, buy CE, buy PE), so the
# rupee cost per attempt is 4 * points * qty. 0 keeps the objective identical
# to the strategy engine (which has no slippage model).
OPT_SLIPPAGE_PTS = {
    "NIFTY": _env_float("OPT_SLIPPAGE_PTS_NIFTY", 0.0),
    "SENSEX": _env_float("OPT_SLIPPAGE_PTS_SENSEX", 0.0),
}
# NEW: slippage used only for the end-of-run stress report. These are
# ASSUMPTIONS - calibrate them from your live trader's actual fills.
OPT_STRESS_SLIPPAGE_PTS = {
    "NIFTY": _env_float("OPT_STRESS_SLIPPAGE_PTS_NIFTY", 0.25),
    "SENSEX": _env_float("OPT_STRESS_SLIPPAGE_PTS_SENSEX", 0.75),
}
# NEW: how many of the best trials to re-score (holdout + stress) at the end.
OPT_REPORT_TOP_K = max(1, _env_int("OPT_REPORT_TOP_K", 20))

# ---- Smoke-test controls. Leave blank/0 for the full data set. ---------------
_SAMPLE_PICKLES_RAW = os.getenv("SAMPLE_MAX_PICKLES", "").strip()
_SAMPLE_DAYS_RAW = os.getenv("SAMPLE_MAX_DAYS", "").strip()
SAMPLE_MAX_PICKLES = int(float(_SAMPLE_PICKLES_RAW)) if _SAMPLE_PICKLES_RAW else None
SAMPLE_MAX_DAYS = int(float(_SAMPLE_DAYS_RAW)) if _SAMPLE_DAYS_RAW else None

# Minimum activity guards. These stop a broken/thin configuration from winning
# merely because it produced only a few lucky trades. Applied to IN-SAMPLE days.
#
# CHANGED: the guards are now CAPPED to the data actually available. A trial
# must trade on at least min(OPT_MIN_DAYS, 75% of the in-sample traded days)
# days and in at least min(OPT_MIN_MONTHS, in-sample months) months. Without
# the cap, a data set shorter than OPT_MIN_DAYS (for example 85 expiry days,
# or ~50 in-sample days after a holdout split) disqualified EVERY trial and
# the run silently returned the baseline. The effective values are printed
# at the start of each run ("[GUARD] ...").
OPT_MIN_DAYS = _env_int("OPT_MIN_DAYS", 100)
OPT_MIN_MONTHS = _env_int("OPT_MIN_MONTHS", 6)
_MIN_DAYS_FRACTION = 0.75
_OPT_DISQUALIFY = -1.0e18
# Set once per run by _configure_activity_minima(); None = not yet configured.
_ACTIVE_MINIMA: Optional[Tuple[int, int]] = None

# ---- Search-group switches -----------------------------------------------------
# Setting a switch to 0 freezes that group at the values in the property file.
OPT_TUNE_TIMES = _env_flag("OPT_TUNE_TIMES", True)
OPT_TUNE_STOPLOSS = _env_flag("OPT_TUNE_STOPLOSS", True)
OPT_TUNE_REENTRY = _env_flag("OPT_TUNE_REENTRY", True)
OPT_TUNE_PROFILES = _env_flag("OPT_TUNE_PROFILES", True)
OPT_TUNE_DAILY_LOSS = _env_flag("OPT_TUNE_DAILY_LOSS", True)
# NEW: the two on/off strategy switches (ENTRY_MOMENTUM_GATE and
# PROFIT_TARGET_REENTRY_ENABLED) are frozen by default. Fast re-entry IS the
# target-recycle idea, and the momentum gate works against "get back in
# quickly". Set to 1 to search them again.
OPT_TUNE_SWITCHES = _env_flag("OPT_TUNE_SWITCHES", False)

# ---- Search bounds -------------------------------------------------------------
# CHANGED (fast-reentry): every bound below contains the value in
# straddle_config_v3opt_DTE0_fastreentry.properties, and the integer grids are
# aligned so those values sit exactly on a grid point.
OPT_ENTRY_FROM_IST = os.getenv("OPT_ENTRY_FROM_IST", "09:15")
OPT_ENTRY_TO_IST = os.getenv("OPT_ENTRY_TO_IST", "10:15")
OPT_ENTRY_STEP_MIN = max(1, _env_int("OPT_ENTRY_STEP_MIN", 1))

# The engine never monitors past SESSION_END_IST (15:30 unless the config sets
# SESSION_END_IST, e.g. 15:40 since the closing auction session), so a later
# EXIT_TIME_IST behaves exactly like the session end. The exit-time search
# range ends at the session end by default.
_SESSION_END_RAW = os.getenv("SESSION_END_IST", "15:30").strip()
OPT_EXIT_FROM_IST = os.getenv("OPT_EXIT_FROM_IST", "15:00")
OPT_EXIT_TO_IST = os.getenv("OPT_EXIT_TO_IST", _SESSION_END_RAW)
OPT_EXIT_STEP_MIN = max(1, _env_int("OPT_EXIT_STEP_MIN", 1))

# NEW: one multiplier applied to the whole LOSS_LIMIT_RUPEES_BY_ATTEMPT list
# from the property file (replaces base/step/growth). 1.0 = the file's list.
# Each scaled value is capped at 0.95 because the strategy's percentage parser
# would read a value above 1.0 as a percent (1.02 -> 1.02%).
OPT_STOP_SCALE_MIN = _env_float("OPT_STOP_SCALE_MIN", 0.50)
OPT_STOP_SCALE_MAX = _env_float("OPT_STOP_SCALE_MAX", 1.60)
_STOP_PCT_UPPER = 0.95
# CHANGED: 2000-5000 step 250 -> 1500-3500 step 50 (2200 is on the grid).
OPT_STOP_CAP_MIN = _env_int("OPT_STOP_CAP_MIN", 1500)
OPT_STOP_CAP_MAX = _env_int("OPT_STOP_CAP_MAX", 3500)
OPT_STOP_CAP_STEP = max(1, _env_int("OPT_STOP_CAP_STEP", 50))

# CHANGED: 3-12 -> 10-30. Note: target re-entries and stop/protect re-entries
# share this one counter, so 0-minute target recycling uses it up quickly. The
# end-of-run report shows on how many days the cap stopped a re-entry.
OPT_REATTEMPTS_MIN = _env_int("OPT_REATTEMPTS_MIN", 10)
OPT_REATTEMPTS_MAX = _env_int("OPT_REATTEMPTS_MAX", 30)
# CHANGED: one flat delay (minutes) after a STOPLOSS / PROFIT_PROTECT exit.
# 1-15 -> 0-8 around the fast-reentry value of 2.
OPT_REENTRY_DELAY_MIN = _env_int("OPT_REENTRY_DELAY_MIN", 0)
OPT_REENTRY_DELAY_MAX = _env_int("OPT_REENTRY_DELAY_MAX", 8)
# CHANGED: 1-10 -> 0-5 (the fast-reentry value is 0).
OPT_TARGET_REENTRY_DELAY_MIN = _env_int("OPT_TARGET_REENTRY_DELAY_MIN", 0)
OPT_TARGET_REENTRY_DELAY_MAX = _env_int("OPT_TARGET_REENTRY_DELAY_MAX", 5)
# Only used when OPT_TUNE_SWITCHES=1 and the gate is switched on.
OPT_MOMENTUM_LOOKBACK_MIN = _env_int("OPT_MOMENTUM_LOOKBACK_MIN", 1)
OPT_MOMENTUM_LOOKBACK_MAX = _env_int("OPT_MOMENTUM_LOOKBACK_MAX", 8)

# CHANGED: ranges widened where the fast-reentry values were on an edge.
OPT_PROTECT_MIN = _env_float("OPT_PROTECT_MIN", 0.20)          # was 0.20
OPT_PROTECT_MAX = _env_float("OPT_PROTECT_MAX", 0.70)          # was 0.50 (SENSEX 0.47)
OPT_LATE_GIVEBACK_MIN = _env_float("OPT_LATE_GIVEBACK_MIN", 0.00)
OPT_LATE_GIVEBACK_MAX = _env_float("OPT_LATE_GIVEBACK_MAX", 0.45)
OPT_TARGET_MIN = _env_float("OPT_TARGET_MIN", 0.35)            # was 0.55 (NIFTY 0.58)
OPT_TARGET_MAX = _env_float("OPT_TARGET_MAX", 0.95)            # was 0.90
OPT_LATE_TARGET_MIN = _env_float("OPT_LATE_TARGET_MIN", 0.15)  # was 0.30 (SENSEX 0.35)
OPT_LATE_TARGET_MAX = _env_float("OPT_LATE_TARGET_MAX", 0.70)  # was 0.65
OPT_LATE_FROM_IST = os.getenv("OPT_LATE_FROM_IST", "13:00")    # was 13:30
OPT_LATE_TO_IST = os.getenv("OPT_LATE_TO_IST", "15:00")
OPT_LATE_TIME_STEP_MIN = max(1, _env_int("OPT_LATE_TIME_STEP_MIN", 5))
OPT_DAILY_LOSS_MIN = _env_int("OPT_DAILY_LOSS_MIN", 8000)      # was 15000 (NIFTY 17000)
OPT_DAILY_LOSS_MAX = _env_int("OPT_DAILY_LOSS_MAX", 40000)
OPT_DAILY_LOSS_STEP = max(1, _env_int("OPT_DAILY_LOSS_STEP", 1000))

# Floats suggested by the optimizer are rounded to this many decimals so the
# written .properties file reproduces the optimizer's result exactly.
_PARAM_DECIMALS = 6

# PICKLES_DIR = r"G:\My Drive\Trading\Historical_Options_Data"
PICKLES_DIR = os.getenv("PICKLES_DIR", r"G:\My Drive\Trading\Dhan_Historical_Options_Data_New")
ENTRY_TIME_IST = os.getenv("ENTRY_TIME_IST", "11:55")  # "HH:MM"
# EXIT_TIME_IST is the strategy time filter / square-off cutoff.
# No fresh entry or re-entry is initiated at or after this time. If an attempt is
# still open at this time, it is closed at the cutoff with exit_reason="TIME_EXIT".
EXIT_TIME_IST = os.getenv("EXIT_TIME_IST", "15:30")  # "HH:MM"

def _safe_fname_part(s: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in s)

def _get_downloads_folder() -> str:
    """
    Returns the current user's default Downloads folder.
    Falls back to home directory if Downloads is not found.
    """
    downloads = Path.home() / "Downloads"
    return str(downloads if downloads.exists() else Path.home())

# --- Generic integer-list parser used for DTE and re-entry delay settings ---
def _parse_int_list(env_val, default_list):
    """
    Parse a comma-separated integer list from an environment variable.

    Examples:
        ALLOWED_DTE="0,1"              -> [0, 1]
        REENTRY_DELAY_BY_ATTEMPT="1,5" -> [1, 5]

    If parsing fails or the env var is blank, the supplied default is used.
    """
    if env_val:
        try:
            vals = [int(round(float(x))) for x in env_val.replace(" ", "").split(",") if x != ""]
            if vals:
                return vals
        except Exception:
            pass
    return list(default_list)


def _parse_pct_value(x) -> float:
    """
    Parse a percentage value into decimal form.

    Accepted user formats:
        10      -> 0.10  (10%)
        "10%"   -> 0.10  (10%)
        0.10    -> 0.10  (10%)
        "0.10"  -> 0.10  (10%)

    This lets you configure percentages naturally from environment variables
    while keeping calculations internally consistent.
    """
    s = str(x).strip().replace("%", "")
    if s == "":
        raise ValueError("blank percentage value")

    v = float(s)

    # If user entered 10, treat it as 10%; if user entered 0.10, keep it as 10%.
    if abs(v) > 1.0:
        v = v / 100.0

    if v < 0:
        raise ValueError("percentage cannot be negative")

    return float(v)


def _parse_pct_list(env_val, default_list):
    """
    Parse comma-separated percentages into decimal form.

    Examples:
        LOSS_LIMIT_RUPEES_BY_ATTEMPT="10,12,15" -> [0.10, 0.12, 0.15]
        LOSS_LIMIT_RUPEES_BY_ATTEMPT="0.10"     -> [0.10]
    """
    if env_val:
        try:
            vals = [_parse_pct_value(x) for x in env_val.replace(" ", "").split(",") if x != ""]
            if vals:
                return vals
        except Exception:
            pass

    return [_parse_pct_value(x) for x in default_list]


def _fmt_int_list(lst) -> str:
    """Format integer-list settings for the output filename."""
    return "-".join(str(int(v)) for v in lst) if lst else "off"


def _fmt_pct_value(v: float) -> str:
    """Format a decimal percentage such as 0.10 as '10pct' for filenames/logs."""
    return f"{v * 100:.2f}".rstrip("0").rstrip(".") + "pct"


def _fmt_pct_list(lst) -> str:
    """Format a list of decimal percentages for the output filename."""
    return "-".join(_fmt_pct_value(float(v)) for v in lst) if lst else "off"


def _parse_float_env(env_name: str, default_value: float) -> float:
    """
    Parse a positive/zero floating-point rupee setting from an environment variable.

    If parsing fails, the supplied default is used. A value <= 0 is treated by
    the relevant logic as disabled where applicable.
    """
    raw = os.getenv(env_name)
    if raw is None or str(raw).strip() == "":
        return float(default_value)
    try:
        return float(str(raw).replace(",", "").strip())
    except Exception:
        return float(default_value)


def _fmt_rupee_value(v: float) -> str:
    """Format rupee values compactly for output filenames/logs."""
    return str(int(round(float(v))))


LOSS_LIMIT_RUPEES_BY_ATTEMPT = _parse_pct_list(
    os.getenv("LOSS_LIMIT_RUPEES_BY_ATTEMPT"),
    [0.2487, 0.2824, 0.3162, 0.3499, 0.3837, 0.4174, 0.4512],
)


def loss_limit_pct_for_attempt(attempt_idx: int) -> float:
    """Return the stop-loss percentage, in decimal form, for the given attempt."""
    s = LOSS_LIMIT_RUPEES_BY_ATTEMPT
    if not s:
        return 0.0
    return float(s[attempt_idx]) if attempt_idx < len(s) else float(s[-1])


# --- Allowed days-to-expiry to trade: [0,1]=expiry day + day before; [0]=expiry only ---
ALLOWED_DTE = _parse_int_list(os.getenv("ALLOWED_DTE"), [0])

# --- Profit-protect threshold/giveback as % of premium collected on that attempt ---
# Default: 30%.
#
# Env examples:
#     PROFIT_PROTECT_TRIGGER_RUPEES="30"
#     PROFIT_PROTECT_TRIGGER_RUPEES="0.30"
#
# Current logic uses the same rupee amount for:
#     1. arming profit-protect once peak P&L reaches G
#     2. exiting when current P&L falls to peak - G
PROFIT_PROTECT_TRIGGER_RUPEES = _parse_pct_value(os.getenv("PROFIT_PROTECT_TRIGGER_RUPEES", 0.254741))

# --- Absolute daily circuit breaker -------------------------------------------------
# Once cumulative realized NET P&L for the current underlying/day reaches this
# loss, no further re-entry is allowed for that day.
#
# Default: Rs. 30,000 loss. Set MAX_DAILY_LOSS_RUPEES=0 to disable.
MAX_DAILY_LOSS_RUPEES = _parse_float_env("MAX_DAILY_LOSS_RUPEES", 30000.0)

# --- Absolute cap on the percentage-based per-attempt stop-loss ----------------------
# The stop-loss is still calculated as:
#     LOSS_LIMIT_% * entry_premium_sum
# But it is capped at this absolute rupee value.
#
# Effective stop-loss per attempt:
#     min(LOSS_LIMIT_% * entry_premium_sum, MAX_LOSS_LIMIT_RUPEES_BY_ATTEMPT)
#
# Default: Rs. 3,000 loss per attempt. Set to 0 to disable the cap.
MAX_LOSS_LIMIT_RUPEES_BY_ATTEMPT = _parse_float_env("MAX_LOSS_LIMIT_RUPEES_BY_ATTEMPT", 3000.0)

MAX_REATTEMPTS = int(os.getenv("MAX_REATTEMPTS", "10"))  # 1 = only one re-entry

# --- Per-DAY profit target as a fraction of premium collected on the CURRENT attempt ---
# When an attempt's profit reaches PROFIT_TARGET_PCT * (CE+PE)*qty, it exits at the
# target and NO further trades are taken that day. 0 disables. e.g. 0.70 = 70%.
PROFIT_TARGET_PCT = float(os.getenv("PROFIT_TARGET_PCT", 0.7))

# --- V2-OPT: late-attempt profit target -------------------------------------
# Attempts ENTERED at/after PROFIT_TARGET_LATE_FROM_IST use this target %.
# 0 disables (all attempts use PROFIT_TARGET_PCT).
PROFIT_TARGET_PCT_LATE = float(os.getenv("PROFIT_TARGET_PCT_LATE", "0") or 0)
PROFIT_TARGET_LATE_FROM_IST = os.getenv("PROFIT_TARGET_LATE_FROM_IST", "14:15")

# --- V2-OPT: re-entry after PROFIT_TARGET ------------------------------------
PROFIT_TARGET_REENTRY_ENABLED = os.getenv("PROFIT_TARGET_REENTRY_ENABLED", "0").strip() == "1"
PROFIT_TARGET_REENTRY_DELAY = int(float(os.getenv("PROFIT_TARGET_REENTRY_DELAY", "3")))

# --- V2-OPT: premium-momentum entry gate -------------------------------------
ENTRY_MOMENTUM_GATE = os.getenv("ENTRY_MOMENTUM_GATE", "0").strip() == "1"
MOMENTUM_LOOKBACK_MIN = int(float(os.getenv("MOMENTUM_LOOKBACK_MIN", "3")))

# --- V3-OPT: late-session tighter profit-protect giveback --------------------
# 0 disables. Example 0.30 = once armed, giveback shrinks to 30% of premium
# from PROFIT_PROTECT_LATE_FROM_IST onward.
PROFIT_PROTECT_LATE_GIVEBACK = float(os.getenv("PROFIT_PROTECT_LATE_GIVEBACK", "0") or 0)
PROFIT_PROTECT_LATE_FROM_IST = os.getenv("PROFIT_PROTECT_LATE_FROM_IST", "14:30")


def _und_env(und: str, key: str, fallback):
    """Per-underlying override: NIFTY_<key> / SENSEX_<key> wins over <key>."""
    raw = os.getenv(f"{und}_{key}")
    if raw is None or str(raw).strip() == "":
        return fallback
    return str(raw).strip()


def resolve_underlying_profile(und: str) -> Dict[str, Any]:
    """Resolve the per-underlying V3 settings dictionary."""
    prof: Dict[str, Any] = {}
    prof["pt_pct"] = float(_und_env(und, "PROFIT_TARGET_PCT", PROFIT_TARGET_PCT))
    prof["pt_pct_late"] = float(_und_env(und, "PROFIT_TARGET_PCT_LATE", PROFIT_TARGET_PCT_LATE))
    prof["pt_late_from"] = parse_hhmm(str(_und_env(und, "PROFIT_TARGET_LATE_FROM_IST", PROFIT_TARGET_LATE_FROM_IST)))
    prof["g_pct"] = _parse_pct_value(_und_env(und, "PROFIT_PROTECT_TRIGGER_RUPEES", PROFIT_PROTECT_TRIGGER_RUPEES))
    prof["dll"] = float(_und_env(und, "MAX_DAILY_LOSS_RUPEES", MAX_DAILY_LOSS_RUPEES))
    prof["g2_pct"] = float(_und_env(und, "PROFIT_PROTECT_LATE_GIVEBACK", PROFIT_PROTECT_LATE_GIVEBACK))
    prof["g2_from"] = parse_hhmm(str(_und_env(und, "PROFIT_PROTECT_LATE_FROM_IST", PROFIT_PROTECT_LATE_FROM_IST)))
    return prof
# --- Per-attempt RE-ENTRY GAP in minutes (index 0 = gap before 1st re-entry, 1 = before 2nd, ...) ---
# Attempts beyond the list reuse the LAST value. Override via env comma list, e.g.
# REENTRY_DELAY_BY_ATTEMPT="10,15,20".
REENTRY_DELAY_BY_ATTEMPT = _parse_int_list(
    os.getenv("REENTRY_DELAY_BY_ATTEMPT"),
    [6, 8, 10, 12, 14, 16, 18, 15, 15, 15, 15],
)

def reentry_delay_for_attempt(attempt_idx: int) -> int:
    s = REENTRY_DELAY_BY_ATTEMPT
    if not s:
        return 0
    return int(s[attempt_idx]) if attempt_idx < len(s) else int(s[-1])

_DEFAULT_OUT = os.path.join(
    _get_downloads_folder(),
    f"short_straddle_V3OPT_{_safe_fname_part(ENTRY_TIME_IST)}"
    f"_exit{_safe_fname_part(EXIT_TIME_IST)}"
    # f"_SLpct_{_safe_fname_part(_fmt_pct_list(LOSS_LIMIT_RUPEES_BY_ATTEMPT))}"
    # f"_DTE_{_safe_fname_part('-'.join(str(d) for d in ALLOWED_DTE))}"
    f"_PPTpct_{_safe_fname_part(_fmt_pct_value(PROFIT_PROTECT_TRIGGER_RUPEES))}"
    f"_DailyMaxLoss_{_safe_fname_part(_fmt_rupee_value(MAX_DAILY_LOSS_RUPEES))}"
    f"_StopCap_{_safe_fname_part(_fmt_rupee_value(MAX_LOSS_LIMIT_RUPEES_BY_ATTEMPT))}"
    f"_MR_{_safe_fname_part(str(MAX_REATTEMPTS))}"
    # f"_PT_{_safe_fname_part(str(int(round(PROFIT_TARGET_PCT * 100))))}pct"
    f"_RDM_{_safe_fname_part(_fmt_int_list(REENTRY_DELAY_BY_ATTEMPT))}.xlsx"
)

OUTPUT_XLSX = os.getenv("OUTPUT_XLSX", _DEFAULT_OUT)

FAIL_ON_PICKLE_ERROR = os.getenv("FAIL_ON_PICKLE_ERROR", "0").strip() == "1"

SESSION_START_IST = dtime(9, 15)
# SESSION_END_IST: last minute of option data the engine reads and monitors.
#   15:30 (default) = market close before the Closing Auction Session (CAS).
#   15:40           = equity-derivatives close since CAS went live (3 Aug 2026).
# Days whose option data has no bar after 15:30 (everything before CAS) still
# end at 15:30 - see day_session_end() - so they give the same trades as before.
SESSION_END_IST = dtime(*(int(part) for part in _SESSION_END_RAW.split(":")))
if not (dtime(15, 30) <= SESSION_END_IST <= dtime(16, 0)):
    raise ValueError(f"SESSION_END_IST={_SESSION_END_RAW} must be between 15:30 and 16:00")
if SESSION_END_IST != dtime(15, 30):
    print(f"[CONFIG] SESSION_END_IST={SESSION_END_IST.strftime('%H:%M')}: option bars up to "
          "this minute are read and monitored.", flush=True)

# LOOKBACK_MONTHS is AUTO by default: the script backtests the full date range
# present in the option pickles. If you explicitly set LOOKBACK_MONTHS to a
# number, that number acts as an optional manual cap.
LOOKBACK_MONTHS_RAW = os.getenv("LOOKBACK_MONTHS", "AUTO").strip()
LOOKBACK_MONTHS: Optional[int]
if LOOKBACK_MONTHS_RAW.upper() in ("", "AUTO", "ALL", "MAX", "FULL"):
    LOOKBACK_MONTHS = None
else:
    LOOKBACK_MONTHS = int(float(LOOKBACK_MONTHS_RAW))

QTY_UNITS = {"NIFTY": 325, "SENSEX": 100}
TRADEABLE = set(QTY_UNITS.keys())

STRIKE_STEP = {"NIFTY": 50, "SENSEX": 100}

# =============================================================================
# TRANSACTION CHARGES (Zerodha F&O Options — NSE)
# =============================================================================
# Each short-straddle attempt = 4 executed orders (sell CE, sell PE, buy CE, buy PE)
BROKERAGE_PER_ORDER       = 20.0       # ₹20 flat per executed order
ORDERS_PER_TRADE          = 4          # sell CE + sell PE + buy CE + buy PE
STT_SELL_PCT              = 0.001      # 0.1% on sell-side premium
EXCHANGE_TXN_PCT          = 0.0003553  # 0.03553% on premium (NSE options)
SEBI_PER_CRORE            = 10.0       # ₹10 per crore of turnover
STAMP_BUY_PCT             = 0.00003    # 0.003% on buy-side premium
IPFT_PER_CRORE            = 0.010      # ₹0.01 per crore (on premium)
GST_PCT                   = 0.18       # 18% on (brokerage + txn charges + SEBI)
INCLUDE_TRANSACTION_COSTS = True       # set False to disable

UNDERLYING_KITE = {
    "NIFTY": {"exchange": "NSE", "tradingsymbol": "NIFTY 50"},
    "SENSEX": {"exchange": "BSE", "tradingsymbol": "SENSEX"},
}

MAX_DAYS_PER_CHUNK = 25
MAX_ATTEMPTS = 5
SLEEP_BETWEEN_CALLS_SEC = 0.20


# =============================================================================
# HELPERS
# =============================================================================
def parse_hhmm(s: str) -> dtime:
    hh, mm = s.strip().split(":")
    return dtime(int(hh), int(mm))

ENTRY_TIME = parse_hhmm(ENTRY_TIME_IST)
PT_LATE_FROM_TIME = parse_hhmm(PROFIT_TARGET_LATE_FROM_IST)
# Strategy cutoff time. The simulator will not initiate a new attempt at or
# after this time, and the current attempt will not be monitored beyond it.
EXIT_TIME = parse_hhmm(EXIT_TIME_IST)

def ist_tz():
    if ZoneInfo is not None:
        return ZoneInfo("Asia/Kolkata")
    if pytz is not None:
        return pytz.timezone("Asia/Kolkata")
    return "Asia/Kolkata"

def ensure_ist(series_or_scalar) -> Any:
    tz = ist_tz()
    dt = pd.to_datetime(series_or_scalar, errors="coerce")
    if isinstance(dt, pd.Series):
        if dt.dt.tz is None:
            return dt.dt.tz_localize(tz)
        return dt.dt.tz_convert(tz)
    if getattr(dt, "tzinfo", None) is None:
        return dt.tz_localize(tz)
    return dt.tz_convert(tz)

def normalize_underlying(name: str) -> Optional[str]:
    if not isinstance(name, str):
        return None
    u = name.upper().strip()
    if "SENSEX" in u:
        return "SENSEX"
    if "BANKNIFTY" in u or "NIFTY BANK" in u:
        return "BANKNIFTY"
    if "NIFTY" in u:
        return "NIFTY"
    return None

def round_to_step(x: float, step: int) -> int:
    return int(round(x / step) * step)

def build_minute_index(day_d: date, start_t: dtime, end_t: dtime) -> pd.DatetimeIndex:
    tz = ist_tz()
    start = pd.Timestamp(datetime.combine(day_d, start_t), tz=tz)
    end = pd.Timestamp(datetime.combine(day_d, end_t), tz=tz)
    return pd.date_range(start=start, end=end, freq="1min")

PRE_CAS_CLOSE_IST = dtime(15, 30)

def day_session_end(day_opt: pd.DataFrame) -> dtime:
    """
    Last minute to simulate for one day (same function in the strategy script).

    Before the closing auction session (CAS, 3 Aug 2026) the market closed at
    15:30. A day whose option data has no bar after 15:30 therefore ends at
    15:30 even when SESSION_END_IST is later; otherwise the engine could open a
    new attempt on the 15:30 close and hold it to 15:40 on stale prices. Days
    with option bars after 15:30 end at SESSION_END_IST.
    """
    if SESSION_END_IST <= PRE_CAS_CLOSE_IST:
        return SESSION_END_IST
    if day_opt.empty:
        return PRE_CAS_CLOSE_IST
    ts = ensure_ist(day_opt["date"])
    minute_of_day = ts.dt.hour * 60 + ts.dt.minute
    has_late_bars = bool((minute_of_day > PRE_CAS_CLOSE_IST.hour * 60 + PRE_CAS_CLOSE_IST.minute).any())
    return SESSION_END_IST if has_late_bars else PRE_CAS_CLOSE_IST

def asof_close(df: pd.DataFrame, ts: pd.Timestamp) -> float:
    if df.empty:
        return float("nan")
    d = df[["date", "close"]].dropna().copy()
    d["date"] = ensure_ist(d["date"])
    d = d.sort_values("date").set_index("date")
    loc = d.index.get_indexer([ts], method="pad")
    if loc[0] == -1:
        return float("nan")
    return float(d.iloc[loc[0]]["close"])

def compute_window_start(end_day: date, months: int) -> date:
    if relativedelta is not None:
        return (pd.Timestamp(end_day) - relativedelta(months=months)).date()
    return (pd.Timestamp(end_day) - pd.Timedelta(days=30 * months)).date()


def determine_backtest_window_start(min_day_seen: date, end_day: date) -> date:
    """
    Determine the backtest start date.

    Default behaviour: use the earliest usable option-data date found in the
    pickles, i.e. the maximum available backtest period.

    Optional override: if LOOKBACK_MONTHS is set to a numeric value through the
    environment, use the later of:
        1. earliest option-data date; and
        2. end_day - LOOKBACK_MONTHS
    so the script never requests data before the pickles actually start.
    """
    if LOOKBACK_MONTHS is None:
        return min_day_seen

    capped_start = compute_window_start(end_day, LOOKBACK_MONTHS)
    return max(min_day_seen, capped_start)

# =============================================================================
# TRANSACTION COST CALCULATOR
# =============================================================================
def compute_trade_charges(
    entry_ce: float, entry_pe: float,
    exit_ce: float, exit_pe: float,
    qty: int,
) -> float:
    """
    Compute total Zerodha transaction charges for one short-straddle attempt.

    Entry = SELL CE + SELL PE  (2 orders, sell side)
    Exit  = BUY  CE + BUY  PE (2 orders, buy side)

    Returns total charges in rupees (always positive).
    """
    if not INCLUDE_TRANSACTION_COSTS:
        return 0.0

    # Turnover values (in rupees)
    entry_turnover = (entry_ce + entry_pe) * qty   # sell side
    exit_turnover  = (exit_ce + exit_pe) * qty     # buy side
    total_turnover = entry_turnover + exit_turnover

    # 1. Brokerage: ₹20 × 4 orders
    brokerage = BROKERAGE_PER_ORDER * ORDERS_PER_TRADE

    # 2. STT: 0.1% on sell-side premium only (entry for short straddle)
    stt = entry_turnover * STT_SELL_PCT

    # 3. Exchange transaction charges: 0.03553% on both sides
    txn_charges = total_turnover * EXCHANGE_TXN_PCT

    # 4. SEBI charges: ₹10 per crore on total turnover
    sebi = total_turnover * SEBI_PER_CRORE / 1_00_00_000

    # 5. Stamp duty: 0.003% on buy side only (exit for short straddle)
    stamp = exit_turnover * STAMP_BUY_PCT

    # 6. IPFT: ₹0.01 per crore on premium (both sides)
    ipft = total_turnover * IPFT_PER_CRORE / 1_00_00_000

    # 7. GST: 18% on (brokerage + transaction charges + SEBI charges)
    gst = (brokerage + txn_charges + sebi) * GST_PCT

    total_charges = brokerage + stt + txn_charges + sebi + stamp + ipft + gst
    return round(total_charges, 2)

# =============================================================================
# Kite historical helpers
# =============================================================================
def _iter_chunks_by_date(from_dt: datetime, to_dt: datetime, days_per_chunk: int) -> List[Tuple[datetime, datetime]]:
    if from_dt > to_dt:
        raise ValueError("from_dt must be <= to_dt")
    chunks: List[Tuple[datetime, datetime]] = []
    cur = from_dt.date()
    end_d = to_dt.date()
    while cur <= end_d:
        chunk_end = min(cur + timedelta(days=days_per_chunk - 1), end_d)
        c_from = from_dt if cur == from_dt.date() else datetime.combine(cur, SESSION_START_IST)
        c_to = to_dt if chunk_end == end_d else datetime.combine(chunk_end, SESSION_END_IST)
        chunks.append((c_from, c_to))
        cur = chunk_end + timedelta(days=1)
    return chunks

def _kite_instruments_cached(kite, exchange: str, cache: Dict[str, List[Dict]]) -> List[Dict]:
    ex = exchange.upper().strip()
    if ex not in cache:
        print(f"[STEP] Loading instruments dump for {ex} ...")
        cache[ex] = kite.instruments(ex)
        print(f"[INFO] {ex} instruments: {len(cache[ex])}")
    return cache[ex]

def get_instrument_token(kite, exchange: str, tradingsymbol: str, cache: Dict[str, List[Dict]]) -> int:
    ex = exchange.upper().strip()
    wanted = tradingsymbol.strip().upper()
    for r in _kite_instruments_cached(kite, ex, cache):
        if str(r.get("tradingsymbol", "")).upper() == wanted:
            return int(r["instrument_token"])
    raise ValueError(f"Instrument not found on {ex}: '{tradingsymbol}'")

def fetch_history_minute(kite, instrument_token: int, from_dt: datetime, to_dt: datetime, label: str) -> List[Dict]:
    interval = "minute"
    chunks = _iter_chunks_by_date(from_dt, to_dt, MAX_DAYS_PER_CHUNK)
    rows_all: List[Dict] = []
    print(f"[INFO] Fetch {label} token={instrument_token} chunks={len(chunks)} {from_dt} -> {to_dt}")
    for i, (c_from, c_to) in enumerate(chunks, start=1):
        last_err = None
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                rows = kite.historical_data(
                    instrument_token=instrument_token,
                    from_date=c_from,
                    to_date=c_to,
                    interval=interval,
                    continuous=False,
                    oi=False,
                )
                rows_all.extend(rows)
                last_err = None
                break
            except Exception as e:
                last_err = e
                time.sleep(min(8.0, 1.5 * attempt))
        if last_err is not None:
            print(f"[ERROR] {label} chunk {i}/{len(chunks)} failed: {c_from}->{c_to}: {last_err}")
        time.sleep(SLEEP_BETWEEN_CALLS_SEC)
    return rows_all

def rows_to_df(rows: List[Dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
    df = pd.DataFrame(rows)
    df["date"] = ensure_ist(df["date"])
    df = df.drop_duplicates(subset=["date"], keep="last").sort_values("date").reset_index(drop=True)
    return df


# =============================================================================
# DATA STRUCTURES
# =============================================================================
@dataclass
class TradeRow:
    day: date
    underlying: str
    trade_seq: int
    expiry: date
    days_to_expiry: int
    atm_strike: int
    qty_units: int
    entry_time: str
    exit_time: str
    exit_reason: str
    entry_underlying: float
    ce_symbol: str
    pe_symbol: str
    entry_ce: float
    entry_pe: float
    exit_ce: float
    exit_pe: float
    exit_pnl_gross: float   # P&L before charges
    txn_charges: float      # total transaction charges for this attempt
    exit_pnl: float         # net P&L after deducting charges
    eod_pnl: float
    max_profit: float
    max_loss: float
    max_profit_before_exit: float   # peak profit reached before this trade exited

    # Premium/risk diagnostics for percentage-based risk rules
    entry_premium_sum: float                 # (entry_ce + entry_pe) * qty
    stop_pct: float                          # stop-loss % of entry premium, decimal form; 0.10 = 10%
    uncapped_stop_rupees: float              # percentage-based stop before absolute cap
    stop_cap_rupees: float                   # configured absolute cap; <=0 means cap disabled
    stop_rupees: float                       # effective rupee stop after cap
    profit_protect_trigger_pct: float         # profit-protect % of entry premium, decimal form; 0.30 = 30%
    profit_protect_trigger_rupees: float      # computed rupee profit-protect trigger/giveback
    daily_realized_pnl_after_trade: float     # cumulative net P&L after this attempt
    daily_loss_limit_rupees: float            # configured daily loss circuit breaker
    daily_loss_limit_hit: bool                # True means no further trades for that day


# =============================================================================
# PASS-1: nearest expiry per (underlying, day)
# =============================================================================
def scan_pickles_pass1(pickle_paths: List[str]) -> Tuple[date, Dict[Tuple[str, date], date], date]:
    max_day_seen: Optional[date] = None
    min_day_seen: Optional[date] = None
    min_expiry_map: Dict[Tuple[str, date], date] = {}

    for p in pickle_paths:
        try:
            df = pd.read_pickle(p)
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue

            for c in ("date", "name", "expiry", "type"):
                if c not in df.columns:
                    raise ValueError(f"Missing column '{c}' in {p}")

            d2 = df[df["type"].astype(str).str.upper().eq("OPTION")]
            if d2.empty:
                continue

            d2 = d2[["date", "name", "expiry"]].copy()
            d2["date"] = ensure_ist(d2["date"])
            d2["day"] = d2["date"].dt.date
            d2["underlying"] = d2["name"].astype(str).map(normalize_underlying)
            d2["expiry_date"] = pd.to_datetime(d2["expiry"], errors="coerce").dt.date
            d2 = d2.dropna(subset=["underlying", "day", "expiry_date"])

            d2 = d2[d2["underlying"].isin(TRADEABLE)]
            d2 = d2[d2["expiry_date"] >= d2["day"]]
            if d2.empty:
                continue

            file_min_day = d2["day"].min()
            file_max_day = d2["day"].max()
            max_day_seen = file_max_day if (max_day_seen is None or file_max_day > max_day_seen) else max_day_seen
            min_day_seen = file_min_day if (min_day_seen is None or file_min_day < min_day_seen) else min_day_seen

            grp = d2.groupby(["underlying", "day"], sort=False)["expiry_date"].min()
            for (und, dy), ex in grp.items():
                key = (und, dy)
                if key not in min_expiry_map or ex < min_expiry_map[key]:
                    min_expiry_map[key] = ex

            print(f"[PASS1 OK] {os.path.basename(p)} option_days={d2['day'].nunique()}")

        except Exception as e:
            msg = f"[PASS1 WARN] {os.path.basename(p)} failed: {e}"
            if FAIL_ON_PICKLE_ERROR:
                raise RuntimeError(msg) from e
            print(msg)

    if max_day_seen is None or min_day_seen is None:
        raise RuntimeError("No usable option data found in pickles (PASS1) for tradeable underlyings.")

    return max_day_seen, min_expiry_map, min_day_seen


# =============================================================================
# Underlying download
# =============================================================================
def download_underlyings(kite, day_start: date, day_end: date) -> Dict[str, pd.DataFrame]:
    cache: Dict[str, List[Dict]] = {}
    from_dt = datetime.combine(day_start, SESSION_START_IST)
    to_dt = datetime.combine(day_end, SESSION_END_IST)

    out: Dict[str, pd.DataFrame] = {}
    for und, meta in UNDERLYING_KITE.items():
        token = get_instrument_token(kite, meta["exchange"], meta["tradingsymbol"], cache)
        rows = fetch_history_minute(kite, token, from_dt, to_dt, label=f"{meta['exchange']}:{meta['tradingsymbol']}")
        df = rows_to_df(rows)
        df["day"] = df["date"].dt.tz_convert(ist_tz()).dt.date
        out[und] = df
        print(f"[UNDERLYING OK] {und}: candles={len(df)} days={df['day'].nunique()}")
    return out


# =============================================================================
# Simulation helpers
# =============================================================================
def _pick_symbol(day_opt: pd.DataFrame, strike: int, opt_type: str) -> Optional[str]:
    sub = day_opt[(day_opt["strike_int"] == strike) & (day_opt["option_type"] == opt_type)]
    if sub.empty:
        return None
    syms = sorted(sub["instrument"].astype(str).unique().tolist())
    return syms[0] if syms else None

def _build_leg_series(day_opt: pd.DataFrame, idx_all: pd.DatetimeIndex,
                      strike: int, opt_type: str, symbol: str,
                      price_col: str = "close", do_ffill: bool = True) -> pd.Series:
    sub = day_opt[
        (day_opt["strike_int"] == strike) &
        (day_opt["option_type"] == opt_type) &
        (day_opt["instrument"].astype(str) == symbol)
    ][["date", price_col]].dropna()

    if sub.empty:
        return pd.Series(index=idx_all, dtype="float64")

    sub = sub.copy()
    sub["date"] = ensure_ist(sub["date"])
    sub = sub.sort_values("date").drop_duplicates(subset=["date"], keep="last").set_index("date")
    s = sub[price_col].astype(float).reindex(idx_all)
    return s.ffill() if do_ffill else s


def build_price_book(
    day_opt: pd.DataFrame,
    idx_all: pd.DatetimeIndex,
) -> Tuple[Dict[Tuple[int, str, str], pd.Series], Dict[Tuple[int, str], str]]:
    """
    Build all parameter-independent option-price series once for one day.

    The old single-backtest path rebuilt and reindexed CE/PE close/high/low
    series every time a strategy parameter changed. That is unacceptable inside
    an optimizer. This cache preserves the exact symbol-selection and reindexing
    rules while making later trials dictionary lookups.
    """
    book: Dict[Tuple[int, str, str], pd.Series] = {}
    symbols: Dict[Tuple[int, str], str] = {}
    if day_opt is None or day_opt.empty:
        return book, symbols

    for (strike, opt_type), sub in day_opt.groupby(["strike_int", "option_type"], sort=False):
        available = sorted(sub["instrument"].astype(str).unique().tolist())
        if not available:
            continue

        symbol = available[0]  # identical rule to _pick_symbol()
        strike_i = int(strike)
        opt_s = str(opt_type)
        symbols[(strike_i, opt_s)] = symbol

        selected = sub[sub["instrument"].astype(str) == symbol][
            ["date", "close", "high", "low"]
        ].copy()
        selected["date"] = ensure_ist(selected["date"])
        selected = (
            selected.sort_values("date")
            .drop_duplicates(subset=["date"], keep="last")
            .set_index("date")
        )
        for col in ("close", "high", "low"):
            book[(strike_i, opt_s, col)] = selected[col].astype(float).reindex(idx_all)

    return book, symbols


def _leg_from_book(
    price_book: Dict[Tuple[int, str, str], pd.Series],
    idx_all: pd.DatetimeIndex,
    strike: int,
    opt_type: str,
    price_col: str,
) -> pd.Series:
    """Return a cached raw minute series; missing legs become an empty series."""
    series = price_book.get((int(strike), str(opt_type), str(price_col)))
    if series is None:
        return pd.Series(index=idx_all, dtype="float64")
    return series


@dataclass(frozen=True)
class UnderlyingProfile:
    """All V3 exit/risk settings that may differ between NIFTY and SENSEX."""

    profit_target_pct: float
    profit_target_pct_late: float
    profit_target_late_from: dtime
    profit_protect_pct: float
    max_daily_loss_rupees: float
    profit_protect_late_giveback_pct: float
    profit_protect_late_from: dtime


@dataclass
class Params:
    """
    Complete parameter set consumed by the simulator.

    Passing this object explicitly is critical: an Optuna trial can run a fresh
    configuration in the same Python process without mutating module globals or
    re-importing the strategy.
    """

    entry_time: dtime
    exit_time: dtime
    loss_limit_pct_by_attempt: List[float]
    max_loss_limit_cap_rupees: float
    max_reattempts: int
    reentry_delay_by_attempt: List[int]

    entry_momentum_gate: bool
    momentum_lookback_min: int
    profit_target_reentry_enabled: bool
    profit_target_reentry_delay_min: int

    profiles: Dict[str, UnderlyingProfile]

    def loss_limit_pct_for_attempt(self, attempt_idx: int) -> float:
        values = self.loss_limit_pct_by_attempt
        if not values:
            return 0.0
        return float(values[attempt_idx]) if attempt_idx < len(values) else float(values[-1])

    def reentry_delay_for_attempt(self, attempt_idx: int) -> int:
        values = self.reentry_delay_by_attempt
        if not values:
            return 0
        return int(values[attempt_idx]) if attempt_idx < len(values) else int(values[-1])

    def profile_for(self, underlying: str) -> UnderlyingProfile:
        try:
            return self.profiles[underlying]
        except KeyError as exc:
            raise KeyError(f"No strategy profile configured for {underlying}") from exc


def default_params() -> Params:
    """Build a parameter object from the loaded V3 property file."""
    profiles: Dict[str, UnderlyingProfile] = {}
    for underlying in sorted(TRADEABLE):
        raw = resolve_underlying_profile(underlying)
        profiles[underlying] = UnderlyingProfile(
            profit_target_pct=float(raw["pt_pct"]),
            profit_target_pct_late=float(raw["pt_pct_late"]),
            profit_target_late_from=raw["pt_late_from"],
            profit_protect_pct=float(raw["g_pct"]),
            max_daily_loss_rupees=float(raw["dll"]),
            profit_protect_late_giveback_pct=float(raw["g2_pct"]),
            profit_protect_late_from=raw["g2_from"],
        )

    return Params(
        entry_time=ENTRY_TIME,
        exit_time=EXIT_TIME,
        loss_limit_pct_by_attempt=list(LOSS_LIMIT_RUPEES_BY_ATTEMPT),
        max_loss_limit_cap_rupees=float(MAX_LOSS_LIMIT_RUPEES_BY_ATTEMPT),
        max_reattempts=int(MAX_REATTEMPTS),
        reentry_delay_by_attempt=list(REENTRY_DELAY_BY_ATTEMPT),
        entry_momentum_gate=bool(ENTRY_MOMENTUM_GATE),
        momentum_lookback_min=int(MOMENTUM_LOOKBACK_MIN),
        profit_target_reentry_enabled=bool(PROFIT_TARGET_REENTRY_ENABLED),
        profit_target_reentry_delay_min=int(PROFIT_TARGET_REENTRY_DELAY),
        profiles=profiles,
    )


@dataclass
class DayGroup:
    """
    One parameter-independent (underlying, date, nearest-expiry) simulation unit.

    Pickle parsing, date normalization, day slicing and option-series reindexing
    are done once. Every Optuna trial reuses these cached objects.
    """

    und: str
    dy: date
    expiry: date
    day_opt: pd.DataFrame
    underlying_day: pd.DataFrame
    price_book: Optional[Dict[Tuple[int, str, str], pd.Series]] = None
    symbols: Optional[Dict[Tuple[int, str], str]] = None
    idx_all: Optional[pd.DatetimeIndex] = None


def simulate_day_multi_trades(
    *,
    und: str,
    dy: date,
    expiry: date,
    day_opt: pd.DataFrame,
    underlying_day: pd.DataFrame,
    params: Params,
    price_book: Optional[Dict[Tuple[int, str, str], pd.Series]] = None,
    symbols: Optional[Dict[Tuple[int, str], str]] = None,
    idx_all: Optional[pd.DatetimeIndex] = None,
) -> Tuple[List[TradeRow], List[Dict[str, Any]]]:

    results: List[TradeRow] = []
    skipped: List[Dict[str, Any]] = []

    if idx_all is None:
        idx_all = build_minute_index(dy, SESSION_START_IST, day_session_end(day_opt))
    if price_book is None or symbols is None:
        price_book, symbols = build_price_book(day_opt, idx_all)
    session_end_ts = idx_all[-1]

    # EXIT_TIME_IST is the strategy time filter.
    #
    # Earlier versions used EXIT_TIME_IST only as a "last new entry" gate while
    # still allowing the currently-open trade to run till SESSION_END_IST. That
    # made the Excel output look as if the filter was not working because trade
    # rows could still show exit_time after EXIT_TIME_IST.
    #
    # In this corrected version:
    #   1. no fresh entry/re-entry is initiated at or after EXIT_TIME_IST; and
    #   2. the active attempt is monitored only up to EXIT_TIME_IST.
    #
    # Therefore, when EXIT_TIME_IST is earlier than 15:30, the trade exits at the
    # cutoff if STOPLOSS / PROFIT_TARGET / PROFIT_PROTECT has not already fired.
    configured_exit_cutoff_ts = pd.Timestamp(datetime.combine(dy, params.exit_time), tz=ist_tz())
    trade_end_ts = min(session_end_ts, configured_exit_cutoff_ts)

    qty = int(QTY_UNITS[und])
    step = int(STRIKE_STEP[und])

    # --- V3-OPT: per-underlying profile --------------------------------------
    _prof = params.profile_for(und)

    # --- V2-OPT: ATM-straddle premium series cache + momentum gate ----------
    _prem_cache: Dict[int, Optional[pd.Series]] = {}

    def _atm_prem_series(strike: int) -> Optional[pd.Series]:
        if strike not in _prem_cache:
            ce_symbol = symbols.get((int(strike), "CE"))
            pe_symbol = symbols.get((int(strike), "PE"))
            if not ce_symbol or not pe_symbol:
                _prem_cache[strike] = None
            else:
                ce = _leg_from_book(price_book, idx_all, strike, "CE", "close").ffill()
                pe = _leg_from_book(price_book, idx_all, strike, "PE", "close").ffill()
                _prem_cache[strike] = ce + pe
        return _prem_cache[strike]

    def _momentum_wait(ts: pd.Timestamp) -> pd.Timestamp:
        """Advance ts minute-by-minute until the current-ATM straddle premium
        is lower than MOMENTUM_LOOKBACK_MIN minutes earlier (i.e. premium is
        decaying), or until the strategy cutoff. Uses only information
        available at ts (close of already-completed minutes)."""
        if not params.entry_momentum_gate:
            return ts
        session_start_ts = idx_all[0]
        while ts < trade_end_ts:
            u = asof_close(underlying_day, ts)
            if pd.isna(u):
                return ts
            atm_g = round_to_step(float(u), step)
            s = _atm_prem_series(atm_g)
            if s is None:
                return ts
            now = s.loc[ts] if ts in s.index else float("nan")
            ts_back = ts - pd.Timedelta(minutes=params.momentum_lookback_min)
            if ts_back < session_start_ts:
                ts_back = session_start_ts
            before = s.loc[ts_back] if ts_back in s.index else float("nan")
            if pd.notna(now) and pd.notna(before) and float(now) < float(before):
                return ts
            ts = ts + pd.Timedelta(minutes=1)
        return ts

    # Profit-protect is now percentage-based, so the actual rupee value is not
    # known until CE/PE entry prices are available for the current attempt.
    profit_protect_pct = float(_prof.profit_protect_pct)
    profit_protect_enabled = profit_protect_pct > 0.0

    cur_entry_ts = pd.Timestamp(datetime.combine(dy, params.entry_time), tz=ist_tz())
    trade_seq = 1

    # V2-OPT: momentum-gate the FIRST entry of the day.
    cur_entry_ts = _momentum_wait(cur_entry_ts)

    # If the configured first entry itself is at/after EXIT_TIME_IST, the day
    # is skipped cleanly. This is intentional: EXIT_TIME_IST is the hard strategy
    # time filter, so a trade needs at least one minute of monitoring before it.
    if cur_entry_ts >= trade_end_ts:
        skipped.append({
            "day": dy,
            "underlying": und,
            "expiry": expiry,
            "trade_seq": trade_seq,
            "reason": (
                f"No entry: entry {params.entry_time.strftime('%H:%M')} is at/after "
                f"exit cutoff {params.exit_time.strftime('%H:%M')}"
            ),
        })
        return results, skipped

    # Cumulative realized NET P&L for this underlying/day. Used for the daily
    # loss circuit breaker. Charges are included through exit_pnl.
    daily_realized_pnl = 0.0
    max_daily_loss_rupees = float(_prof.max_daily_loss_rupees)
    daily_loss_limit_enabled = max_daily_loss_rupees > 0

    while cur_entry_ts < trade_end_ts:
        if daily_loss_limit_enabled and daily_realized_pnl <= -float(max_daily_loss_rupees):
            skipped.append({
                "day": dy,
                "underlying": und,
                "expiry": expiry,
                "trade_seq": trade_seq,
                "reason": (
                    f"Daily loss limit hit before next entry: "
                    f"realized_pnl={daily_realized_pnl:.2f}, "
                    f"limit={max_daily_loss_rupees:.2f}"
                ),
            })
            break

        u_px = asof_close(underlying_day, cur_entry_ts)
        if pd.isna(u_px):
            skipped.append({"day": dy, "underlying": und, "expiry": expiry, "trade_seq": trade_seq,
                            "reason": f"No underlying price at entry {cur_entry_ts.strftime('%H:%M')}"})
            break

        atm = round_to_step(float(u_px), step)

        ce_sym = symbols.get((int(atm), "CE"))
        pe_sym = symbols.get((int(atm), "PE"))
        if not ce_sym or not pe_sym:
            skipped.append({"day": dy, "underlying": und, "expiry": expiry, "trade_seq": trade_seq,
                            "atm_strike": atm, "reason": "ATM CE/PE not available in pickle band"})
            break

        # Raw entry series and forward-filled close series come from the
        # parameter-independent day cache.
        ce_close_raw = _leg_from_book(price_book, idx_all, atm, "CE", "close")
        pe_close_raw = _leg_from_book(price_book, idx_all, atm, "PE", "close")
        ce_close = ce_close_raw.ffill()
        pe_close = pe_close_raw.ffill()

        # High/low are intentionally not forward-filled: they are used only for
        # intraminute stop-loss and profit-target detection.
        ce_high = _leg_from_book(price_book, idx_all, atm, "CE", "high")
        ce_low = _leg_from_book(price_book, idx_all, atm, "CE", "low")
        pe_high = _leg_from_book(price_book, idx_all, atm, "PE", "high")
        pe_low = _leg_from_book(price_book, idx_all, atm, "PE", "low")

        if cur_entry_ts not in idx_all:
            skipped.append({"day": dy, "underlying": und, "expiry": expiry, "trade_seq": trade_seq,
                            "reason": "Entry timestamp not in session index"})
            break

        ce_entry = ce_close_raw.loc[cur_entry_ts]
        pe_entry = pe_close_raw.loc[cur_entry_ts]
        monitor_start_ts = pd.Timestamp(cur_entry_ts) + pd.Timedelta(minutes=1)
        if monitor_start_ts > trade_end_ts:
            break

        if pd.isna(ce_entry) or pd.isna(pe_entry):
            skipped.append({"day": dy, "underlying": und, "expiry": expiry, "trade_seq": trade_seq,
                            "atm_strike": atm, "reason": "No CE/PE price at entry (after ffill)"})
            break

        # ---------------------------------------------------------------------
        # Percentage-based risk basis for THIS attempt
        # ---------------------------------------------------------------------
        # For every entry/re-entry, compute the premium collected in rupees.
        # Stop-loss and profit-protect thresholds are derived from this value.
        #
        # Example:
        #   entry_ce=70, entry_pe=50, qty=325
        #   entry_premium_sum = (70 + 50) * 325 = 39,000
        #   10% stop-loss = 3,900
        #   30% profit-protect threshold/giveback = 11,700
        # ---------------------------------------------------------------------
        entry_premium_sum = (float(ce_entry) + float(pe_entry)) * qty

        loss_limit_pct = params.loss_limit_pct_for_attempt(trade_seq - 1)
        uncapped_loss_limit_rupees = float(loss_limit_pct * entry_premium_sum)

        # Absolute cap on the percentage-based stop-loss.
        # Example: 10% of premium may be Rs. 4,500, but with a Rs. 3,000 cap
        # the effective stop used by the simulator is Rs. 3,000.
        stop_cap_rupees = float(params.max_loss_limit_cap_rupees)
        if stop_cap_rupees > 0:
            loss_limit_rupees = float(min(uncapped_loss_limit_rupees, stop_cap_rupees))
        else:
            loss_limit_rupees = float(uncapped_loss_limit_rupees)

        # G is the same variable used by the existing profit-protect logic:
        #   - profit-protect arms when peak P&L >= G
        #   - profit-protect exits when current P&L <= peak - G
        G = float(profit_protect_pct * entry_premium_sum)

        # Close-based PnL (same as before)
        pnl_close_all = (float(ce_entry) - ce_close) * qty + (float(pe_entry) - pe_close) * qty
        pnl = pnl_close_all.loc[monitor_start_ts:trade_end_ts].dropna()  # keep 'pnl' as close-based for profit-protect

        # STOPLOSS worst-case PnL candidates within each minute:
        #  A) CE high, PE low
        pnl_ceHigh_peLow_all = (float(ce_entry) - ce_high) * qty + (float(pe_entry) - pe_low) * qty
        #  B) CE low, PE high
        pnl_ceLow_peHigh_all = (float(ce_entry) - ce_low) * qty + (float(pe_entry) - pe_high) * qty

        # Worst-case PnL per minute among (close, A, B)
        pnl_sl_all = pd.concat([pnl_close_all, pnl_ceHigh_peLow_all, pnl_ceLow_peHigh_all], axis=1).min(axis=1)
        pnl_sl = pnl_sl_all.loc[monitor_start_ts:trade_end_ts].dropna()

        if pnl.empty:
            skipped.append({"day": dy, "underlying": und, "expiry": expiry, "trade_seq": trade_seq,
                            "atm_strike": atm, "reason": "PnL series empty after entry"})
            break

        eod_ts = pnl.index[-1]
        eod_pnl = float(pnl.iloc[-1])

        # If EXIT_TIME_IST is earlier than market close and no risk/profit event
        # triggers before that, the attempt is closed at the configured cutoff.
        # The old "EOD" label is retained only when the monitoring horizon is
        # the real session end.
        default_exit_reason = "TIME_EXIT" if trade_end_ts < session_end_ts else "EOD"

        max_profit = float(max(0.0, pnl.max()))
        max_loss = float(min(0.0, pnl.min()))

        # STOPLOSS uses the attempt-specific rupee value after applying the
        # absolute per-attempt cap.
        stop_hit = pnl_sl <= -loss_limit_rupees
        stop_ts = pnl_sl.index[stop_hit.to_numpy().argmax()] if stop_hit.any() else None

        protect_ts = None
        if profit_protect_enabled:
            peak = pnl.cummax()
            armed = peak >= G
            trail = peak - G
            # V3-OPT: tighter giveback from PROFIT_PROTECT_LATE_FROM_IST onward
            if _prof.profit_protect_late_giveback_pct > 0.0:
                _g2_ts = pd.Timestamp(datetime.combine(dy, _prof.profit_protect_late_from), tz=ist_tz())
                _late_mask = pnl.index >= _g2_ts
                if _late_mask.any():
                    _trail_late = peak - float(_prof.profit_protect_late_giveback_pct) * entry_premium_sum
                    trail = trail.where(~_late_mask, trail.combine(_trail_late, max))
            protect_hit = armed & (pnl <= trail)
            protect_ts = pnl.index[protect_hit.to_numpy().argmax()] if protect_hit.any() else None

        # --- Per-day PROFIT TARGET: % of premium collected on this attempt ---
        # When reached, this trade exits at the target AND no further trades are
        # taken for the day (PROFIT_TARGET is excluded from the re-entry rule below).
        target_ts = None
        target_rupees = None
        # V2-OPT: attempts entered at/after PROFIT_TARGET_LATE_FROM_IST use
        # the (smaller) late-session profit target.
        _pt_pct_eff = _prof.profit_target_pct
        if _prof.profit_target_pct_late > 0.0 and cur_entry_ts.timetz().replace(tzinfo=None) >= _prof.profit_target_late_from:
            _pt_pct_eff = _prof.profit_target_pct_late
        if _pt_pct_eff > 0.0:
            target_rupees = _pt_pct_eff * entry_premium_sum
            # best-case (favourable) intrabar profit: both legs bought back at their lows
            pnl_best_all = (float(ce_entry) - ce_low) * qty + (float(pe_entry) - pe_low) * qty
            pnl_tp = pd.concat([pnl_close_all, pnl_best_all], axis=1).max(axis=1)
            pnl_tp = pnl_tp.loc[monitor_start_ts:trade_end_ts].dropna()
            tp_hit = pnl_tp >= float(target_rupees)
            target_ts = pnl_tp.index[tp_hit.to_numpy().argmax()] if tp_hit.any() else None

        # Earliest triggered exit wins; on identical timestamps prefer the more
        # conservative outcome: STOPLOSS, then PROFIT_TARGET, then PROFIT_PROTECT.
        exit_ts = eod_ts
        exit_reason = default_exit_reason
        _candidates = []
        if stop_ts is not None:
            _candidates.append((stop_ts, 0, "STOPLOSS"))
        if target_ts is not None:
            _candidates.append((target_ts, 1, "PROFIT_TARGET"))
        if protect_ts is not None:
            _candidates.append((protect_ts, 2, "PROFIT_PROTECT"))
        if _candidates:
            _candidates.sort(key=lambda c: (c[0], c[1]))
            exit_ts, _, exit_reason = _candidates[0]

        if exit_reason == "STOPLOSS":
            exit_pnl_gross = -float(loss_limit_rupees)
        elif exit_reason == "PROFIT_TARGET":
            exit_pnl_gross = float(target_rupees)
        else:
            exit_pnl_gross = float(pnl.loc[exit_ts])

        # Peak (close-based) profit reached during this trade's life, up to its exit
        pnl_pre_exit = pnl.loc[:exit_ts]
        max_profit_before_exit = float(max(0.0, pnl_pre_exit.max())) if len(pnl_pre_exit) else 0.0

        exit_ce = float(ce_close.loc[exit_ts]) if pd.notna(ce_close.loc[exit_ts]) else float("nan")
        exit_pe = float(pe_close.loc[exit_ts]) if pd.notna(pe_close.loc[exit_ts]) else float("nan")

        txn_charges = compute_trade_charges(
            entry_ce=float(ce_entry), entry_pe=float(pe_entry),
            exit_ce=exit_ce if not pd.isna(exit_ce) else 0.0,
            exit_pe=exit_pe if not pd.isna(exit_pe) else 0.0,
            qty=qty,
        )
        exit_pnl = exit_pnl_gross - txn_charges

        # Update cumulative realized NET P&L for the day. This is checked
        # before allowing any further re-entry.
        daily_realized_pnl += float(exit_pnl)
        daily_loss_limit_hit = bool(
            daily_loss_limit_enabled and daily_realized_pnl <= -float(max_daily_loss_rupees)
        )

        dte = int((expiry - dy).days)

        results.append(
            TradeRow(
                day=dy,
                underlying=und,
                trade_seq=trade_seq,
                expiry=expiry,
                days_to_expiry=dte,
                atm_strike=int(atm),
                qty_units=qty,
                entry_time=pd.Timestamp(cur_entry_ts).strftime("%H:%M"),
                exit_time=pd.Timestamp(exit_ts).strftime("%H:%M"),
                exit_reason=exit_reason,
                entry_underlying=float(u_px),
                ce_symbol=ce_sym,
                pe_symbol=pe_sym,
                entry_ce=float(ce_entry),
                entry_pe=float(pe_entry),
                exit_ce=exit_ce,
                exit_pe=exit_pe,
                exit_pnl_gross=exit_pnl_gross,
                txn_charges=txn_charges,
                exit_pnl=exit_pnl,
                eod_pnl=eod_pnl,
                max_profit=max_profit,
                max_loss=max_loss,
                max_profit_before_exit=max_profit_before_exit,
                entry_premium_sum=float(entry_premium_sum),
                stop_pct=float(loss_limit_pct),
                uncapped_stop_rupees=float(uncapped_loss_limit_rupees),
                stop_cap_rupees=float(stop_cap_rupees),
                stop_rupees=float(loss_limit_rupees),
                profit_protect_trigger_pct=float(profit_protect_pct),
                profit_protect_trigger_rupees=float(G),
                daily_realized_pnl_after_trade=float(daily_realized_pnl),
                daily_loss_limit_rupees=float(max_daily_loss_rupees),
                daily_loss_limit_hit=bool(daily_loss_limit_hit),
            )
        )

        if daily_loss_limit_hit:
            skipped.append({
                "day": dy,
                "underlying": und,
                "expiry": expiry,
                "trade_seq": trade_seq + 1,
                "reason": (
                    f"No re-entry: daily loss limit hit after trade_seq={trade_seq}; "
                    f"realized_pnl={daily_realized_pnl:.2f}, "
                    f"limit={max_daily_loss_rupees:.2f}"
                ),
            })
            break

        if exit_reason in ("STOPLOSS", "PROFIT_PROTECT") and (trade_seq - 1) < params.max_reattempts:
            delay_min = params.reentry_delay_for_attempt(trade_seq - 1)  # gap before this re-entry
            trade_seq += 1
            cur_entry_ts = pd.Timestamp(exit_ts) + pd.Timedelta(minutes=delay_min)
            # V2-OPT: wait for premium decay confirmation before re-entering.
            cur_entry_ts = _momentum_wait(cur_entry_ts)

            # Do not initiate any further trade at or after EXIT_TIME_IST.
            # Because trade_end_ts is also the monitoring end, this keeps the
            # output strictly filtered by EXIT_TIME_IST.
            if cur_entry_ts >= trade_end_ts:
                skipped.append({
                    "day": dy,
                    "underlying": und,
                    "expiry": expiry,
                    "trade_seq": trade_seq,
                    "reason": (
                        f"No re-entry: next entry time {pd.Timestamp(cur_entry_ts).strftime('%H:%M')} "
                        f"is at/after exit cutoff {params.exit_time.strftime('%H:%M')}"
                    ),
                })
                break
            continue

        # V2-OPT: PROFIT_TARGET no longer necessarily ends the day. Recycle
        # into a fresh ATM straddle after PROFIT_TARGET_REENTRY_DELAY minutes
        # (plus the momentum gate). Position stays sequential: one straddle
        # at a time, same quantities.
        if (
            exit_reason == "PROFIT_TARGET"
            and params.profit_target_reentry_enabled
            and (trade_seq - 1) < params.max_reattempts
        ):
            trade_seq += 1
            cur_entry_ts = pd.Timestamp(exit_ts) + pd.Timedelta(minutes=params.profit_target_reentry_delay_min)
            cur_entry_ts = _momentum_wait(cur_entry_ts)
            if cur_entry_ts >= trade_end_ts:
                skipped.append({
                    "day": dy,
                    "underlying": und,
                    "expiry": expiry,
                    "trade_seq": trade_seq,
                    "reason": (
                        f"No target re-entry: next entry time "
                        f"{pd.Timestamp(cur_entry_ts).strftime('%H:%M')} is at/after "
                        f"exit cutoff {params.exit_time.strftime('%H:%M')}"
                    ),
                })
                break
            continue

        break

    return results, skipped


# =============================================================================
# PASS-2: process each pickle and simulate trades for days where this expiry is nearest
# =============================================================================

def build_day_groups(
    pickle_paths: List[str],
    min_expiry_map: Dict[Tuple[str, date], date],
    underlying_data: Dict[str, pd.DataFrame],
    window_start: date,
    window_end: date,
    max_pickles: Optional[int] = None,
    max_days: Optional[int] = None,
    traded_only: Optional[Dict[date, str]] = None,
    release_raw: bool = False,
) -> Tuple[List[DayGroup], List[Dict[str, Any]]]:
    """
    Parse all option pickles once and cache each nearest-expiry trading day.

    `max_pickles` and `max_days` are smoke-test controls only. A production
    optimization should leave both as None.

    NEW (fast-reentry edition):
    `traded_only` - mapping {date: underlying} from pick_actual_underlying_by_day().
        When given, a day-group is kept ONLY if its underlying is the one that
        is actually traded on that date (nearest expiry, DTE in ALLOWED_DTE).
        build_actual_trades_df() discards every other group after simulation,
        so skipping them up-front changes no result - it only removes work.
        The optimizer passes this; RUN_MODE=backtest does not (its Excel
        'all_trades_backtested' sheet still shows every nearest-expiry day).
    `release_raw` - drop the raw per-day option DataFrame once its price book
        is built. The simulator never reads it when a price book is supplied,
        so this only frees memory.
    """
    groups: List[DayGroup] = []
    skipped_rows: List[Dict[str, Any]] = []
    processed_day_keys: set[Tuple[str, date, date]] = set()

    paths = list(pickle_paths)
    if max_pickles is not None and max_pickles > 0:
        paths = paths[:max_pickles]

    total_files = len(paths)
    filtered_out = 0
    for file_index, path in enumerate(paths, start=1):
        try:
            df = pd.read_pickle(path)
            if not isinstance(df, pd.DataFrame) or df.empty:
                print(f"[LOAD {file_index}/{total_files}] {os.path.basename(path)}: empty")
                continue

            needed_cols = [
                "date", "name", "type", "option_type", "strike", "expiry",
                "instrument", "high", "low", "close",
            ]
            missing = [column for column in needed_cols if column not in df.columns]
            if missing:
                raise ValueError(f"Missing columns {missing} in {path}")

            d2 = df[df["type"].astype(str).str.upper().eq("OPTION")][needed_cols].copy()
            if d2.empty:
                continue

            d2["date"] = ensure_ist(d2["date"])
            d2["day"] = d2["date"].dt.date
            d2["underlying"] = d2["name"].astype(str).map(normalize_underlying)
            d2 = d2[d2["underlying"].isin(TRADEABLE)]
            if d2.empty:
                continue

            d2["expiry_date"] = pd.to_datetime(d2["expiry"], errors="coerce").dt.date
            d2["strike_num"] = pd.to_numeric(d2["strike"], errors="coerce")
            d2["strike_int"] = d2["strike_num"].round().astype("Int64")
            d2["option_type"] = d2["option_type"].astype(str).str.upper()
            d2 = d2.dropna(
                subset=["day", "underlying", "expiry_date", "strike_int", "close"]
            )
            d2["strike_int"] = d2["strike_int"].astype(int)

            d2 = d2[d2["expiry_date"] >= d2["day"]]
            d2 = d2[(d2["day"] >= window_start) & (d2["day"] <= window_end)]
            if d2.empty:
                continue

            for (und, dy, expiry), group in d2.groupby(
                ["underlying", "day", "expiry_date"], sort=False
            ):
                if min_expiry_map.get((und, dy)) != expiry:
                    continue

                # NEW: skip groups that can never appear in actual_trades.
                # Checked BEFORE the duplicate-key bookkeeping so the kept
                # groups follow exactly the same first-pickle-wins rule as
                # before.
                if traded_only is not None and traded_only.get(dy) != und:
                    filtered_out += 1
                    continue

                day_key = (und, dy, expiry)
                if day_key in processed_day_keys:
                    skipped_rows.append({
                        "day": dy,
                        "underlying": und,
                        "expiry": expiry,
                        "reason": (
                            "Duplicate (underlying, day, expiry) across pickles; "
                            "skipped to prevent double-counting"
                        ),
                    })
                    continue
                processed_day_keys.add(day_key)

                underlying_all = underlying_data.get(und)
                if underlying_all is None:
                    skipped_rows.append({
                        "day": dy, "underlying": und, "expiry": expiry,
                        "reason": "No underlying series downloaded",
                    })
                    continue

                underlying_day = underlying_all[underlying_all["day"] == dy]
                if underlying_day.empty:
                    skipped_rows.append({
                        "day": dy, "underlying": und, "expiry": expiry,
                        "reason": "Underlying minute data missing for day",
                    })
                    continue

                groups.append(
                    DayGroup(
                        und=und,
                        dy=dy,
                        expiry=expiry,
                        day_opt=group.copy(),
                        underlying_day=underlying_day.copy(),
                    )
                )

            print(
                f"[LOAD {file_index}/{total_files}] {os.path.basename(path)} "
                f"(day-groups={len(groups)})",
                flush=True,
            )

        except Exception as exc:
            message = (
                f"[LOAD {file_index}/{total_files} WARN] "
                f"{os.path.basename(path)} failed: {exc}"
            )
            if FAIL_ON_PICKLE_ERROR:
                raise RuntimeError(message) from exc
            print(message, flush=True)

    if traded_only is not None:
        print(
            f"[LOAD] traded-day filter: kept {len(groups)} day-groups, "
            f"skipped {filtered_out} that are never traded "
            f"(wrong underlying for the date or DTE not in {ALLOWED_DTE})",
            flush=True,
        )

    groups.sort(key=lambda item: (item.dy, item.und))

    if max_days is not None and max_days > 0:
        unique_days = sorted({item.dy for item in groups})
        keep_days = set(unique_days[-max_days:])
        groups = [item for item in groups if item.dy in keep_days]
        print(
            f"[LOAD] smoke sample: retained {len(keep_days)} dates / "
            f"{len(groups)} day-groups",
            flush=True,
        )

    if groups:
        print(
            f"[LOAD] precomputing option price books for {len(groups)} day-groups ...",
            flush=True,
        )
        for group_index, item in enumerate(groups, start=1):
            item.idx_all = build_minute_index(item.dy, SESSION_START_IST, day_session_end(item.day_opt))
            item.price_book, item.symbols = build_price_book(item.day_opt, item.idx_all)
            if release_raw:
                # The simulator only falls back to day_opt when price_book is
                # missing, which can no longer happen for this group.
                item.day_opt = pd.DataFrame()
            if group_index % 50 == 0 or group_index == len(groups):
                print(
                    f"[LOAD] price books {group_index}/{len(groups)}",
                    flush=True,
                )

    print(f"[LOAD] ready: {len(groups)} day-groups", flush=True)
    return groups, skipped_rows



def simulate_groups(
    params: Params,
    groups: List[DayGroup],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Simulate one complete parameter set over the already-cached day-groups."""
    all_trades: List[Dict[str, Any]] = []
    skipped_rows: List[Dict[str, Any]] = []

    for item in groups:
        if item.idx_all is None or item.price_book is None or item.symbols is None:
            item.idx_all = build_minute_index(item.dy, SESSION_START_IST, day_session_end(item.day_opt))
            item.price_book, item.symbols = build_price_book(item.day_opt, item.idx_all)

        trades, skips = simulate_day_multi_trades(
            und=item.und,
            dy=item.dy,
            expiry=item.expiry,
            day_opt=item.day_opt,
            underlying_day=item.underlying_day,
            params=params,
            price_book=item.price_book,
            symbols=item.symbols,
            idx_all=item.idx_all,
        )
        all_trades.extend(trade.__dict__ for trade in trades)
        skipped_rows.extend(skips)

    all_df = pd.DataFrame(all_trades)
    if not all_df.empty:
        all_df = all_df.sort_values(
            ["day", "underlying", "trade_seq"]
        ).reset_index(drop=True)

    return all_df, pd.DataFrame(skipped_rows)


def process_pickles_generate_trades(
    params: Params,
    pickle_paths: List[str],
    min_expiry_map: Dict[Tuple[str, date], date],
    underlying_data: Dict[str, pd.DataFrame],
    window_start: date,
    window_end: date,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Normal single-backtest path, using the same cached engine as Optuna."""
    groups, parse_skips = build_day_groups(
        pickle_paths,
        min_expiry_map,
        underlying_data,
        window_start,
        window_end,
    )
    all_df, sim_skips = simulate_groups(params, groups)

    frames: List[pd.DataFrame] = []
    if parse_skips:
        frames.append(pd.DataFrame(parse_skips))
    if not sim_skips.empty:
        frames.append(sim_skips)
    skip_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    if not skip_df.empty:
        if "day" not in skip_df.columns:
            skip_df["day"] = pd.NaT
        if "underlying" not in skip_df.columns:
            skip_df["underlying"] = pd.NA
        skip_df = skip_df.sort_values(
            ["day", "underlying"], na_position="last"
        ).reset_index(drop=True)

    return all_df, skip_df


# =============================================================================
# Actual trades: one underlying per day (nearest expiry), include all re-entries for that underlying/day
# =============================================================================
def pick_actual_underlying_by_day(min_expiry_map: Dict[Tuple[str, date], date]) -> Dict[date, str]:
    by_day: Dict[date, List[Tuple[date, str]]] = {}
    for (und, dy), ex in min_expiry_map.items():
        if und not in TRADEABLE:
            continue

        dte = int((ex - dy).days)
        if dte not in ALLOWED_DTE:
            continue

        by_day.setdefault(dy, []).append((ex, und))

    out: Dict[date, str] = {}
    for dy, lst in by_day.items():
        # nearest expiry first; if tied, prefer NIFTY
        lst_sorted = sorted(lst, key=lambda t: (t[0], 0 if t[1] == "NIFTY" else 1))
        out[dy] = lst_sorted[0][1]
    return out

def build_actual_trades_df(all_trades_df: pd.DataFrame, min_expiry_map: Dict[Tuple[str, date], date]) -> pd.DataFrame:
    if all_trades_df.empty:
        return pd.DataFrame()

    actual_underlying = pick_actual_underlying_by_day(min_expiry_map)

    m = all_trades_df.copy()
    m["actual_underlying_for_day"] = m["day"].map(actual_underlying)

    # keep only days for which a 0/1-DTE actual underlying exists
    m = m[m["actual_underlying_for_day"].notna()]

    # keep only the selected underlying for that day
    m = m[m["underlying"] == m["actual_underlying_for_day"]]

    # keep only 0- and 1-DTE rows
    # keep only 0- and 1-DTE rows
    m = m[m["days_to_expiry"].isin(ALLOWED_DTE)]

    # keep all reattempts for the one selected underlying on that day
    m = m.drop(columns=["actual_underlying_for_day"])
    m = m.sort_values(["day", "trade_seq"]).reset_index(drop=True)

    # 1 if net exit PnL is positive, else 0
    m["is_exit_pnl_positive"] = (m["exit_pnl"] > 0).astype(int)

    return m


# =============================================================================
# Excel output
# =============================================================================
def _autosize_columns_safe(ws) -> None:
    # Safe autosize even when the sheet is "empty-ish"
    try:
        max_col = ws.max_column or 0
        if max_col <= 0:
            return
        for col_idx in range(1, max_col + 1):
            col_letter = ws.cell(row=1, column=col_idx).column_letter
            max_len = 0
            for row_idx in range(1, min(ws.max_row or 1, 2000) + 1):
                v = ws.cell(row=row_idx, column=col_idx).value
                if v is None:
                    continue
                max_len = max(max_len, len(str(v)))
            ws.column_dimensions[col_letter].width = min(60, max(10, max_len + 2))
    except Exception:
        # Never fail the whole run just because autosize misbehaved
        return

def _color_actual_trades_by_date(wb, actual_trades_df) -> None:
    """Shade rows so all attempts on the same calendar date share one colour,
    alternating between two soft fills as the date changes (visual grouping)."""
    if actual_trades_df is None or actual_trades_df.empty:
        return
    if "actual_trades" not in wb.sheetnames:
        return
    cols = list(actual_trades_df.columns)
    if "day" not in cols:
        return
    from openpyxl.styles import PatternFill
    ws = wb["actual_trades"]
    ncols = len(cols)
    fills = [
        PatternFill(fill_type="solid", fgColor="E8F0FE"),  # light blue
        PatternFill(fill_type="solid", fgColor="FFF3E0"),  # light amber
    ]
    days = actual_trades_df["day"].tolist()
    color_idx = 0
    prev_day = None
    first = True
    for i, d in enumerate(days):
        if first:
            first = False
        elif d != prev_day:
            color_idx ^= 1
        prev_day = d
        fill = fills[color_idx]
        excel_row = i + 2  # header occupies row 1
        for c in range(1, ncols + 1):
            ws.cell(row=excel_row, column=c).fill = fill


def write_excel(all_trades_df: pd.DataFrame, actual_trades_df: pd.DataFrame, skipped_df: pd.DataFrame) -> None:
    out_dir = os.path.dirname(os.path.abspath(OUTPUT_XLSX))
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    piv_exit = pd.DataFrame()
    piv_eod_first = pd.DataFrame()
    monthwise_summary = pd.DataFrame()
    if not all_trades_df.empty:
        piv_exit = all_trades_df.pivot_table(index="day", columns="underlying", values="exit_pnl", aggfunc="sum").reset_index()

        first = all_trades_df[all_trades_df["trade_seq"] == 1]
        piv_eod_first = first.pivot_table(index="day", columns="underlying", values="eod_pnl", aggfunc="sum").reset_index()

        inst = all_trades_df.copy()
        inst["is_win_exit"] = inst["exit_pnl"] > 0
        inst["is_stoploss"] = inst["exit_reason"].astype(str).str.upper().eq("STOPLOSS")
        inst["is_profit_protect"] = inst["exit_reason"].astype(str).str.upper().eq("PROFIT_PROTECT")
        instrument_summary = (
            inst.groupby("underlying", as_index=False)
            .agg(
                trades=("exit_pnl", "count"),
                total_exit_pnl=("exit_pnl", "sum"),
                avg_exit_pnl=("exit_pnl", "mean"),
                win_rate_exit_pct=("is_win_exit", lambda s: 100.0 * s.mean()),
                stoploss_rate_pct=("is_stoploss", lambda s: 100.0 * s.mean()),
                profit_protect_rate_pct=("is_profit_protect", lambda s: 100.0 * s.mean()),
                avg_max_profit=("max_profit", "mean"),
                avg_max_loss=("max_loss", "mean"),
                worst_max_loss=("max_loss", "min"),
            )
            .sort_values("total_exit_pnl", ascending=False)
            .reset_index(drop=True)
        )
    else:
        instrument_summary = pd.DataFrame()

    if not actual_trades_df.empty:
        tmp = actual_trades_df.copy()
        tmp["month"] = pd.to_datetime(tmp["day"]).dt.to_period("M").astype(str)

        # Existing trade-level monthly summary
        monthwise_summary = (
            tmp.groupby("month", as_index=False)
            .agg(
                trades=("exit_pnl", "count"),
                total_exit_pnl=("exit_pnl", "sum"),
                avg_exit_pnl=("exit_pnl", "mean"),
                winning_trades=("is_exit_pnl_positive", "sum"),
            )
        )
        monthwise_summary["losing_trades"] = monthwise_summary["trades"] - monthwise_summary["winning_trades"]
        monthwise_summary["win_rate_pct"] = (
                100.0 * monthwise_summary["winning_trades"] / monthwise_summary["trades"]
        ).round(2)

        # New: daily PnL inside each month
        daily_tmp = (
            tmp.groupby(["month", "day"], as_index=False)
            .agg(daily_pnl=("exit_pnl", "sum"))
        )

        loss_day_stats = (
            daily_tmp.groupby("month", as_index=False)
            .agg(
                avg_loss_on_loss_days=(
                    "daily_pnl",
                    lambda s: float(s[s < 0].mean()) if (s < 0).any() else 0.0
                ),
                max_loss_in_a_day=(
                    "daily_pnl",
                    lambda s: float(s.min()) if len(s) else 0.0
                ),
            )
        )

        # Count of profitable vs loss days within each month
        day_count_stats = (
            daily_tmp.groupby("month", as_index=False)
            .agg(
                profitable_days=("daily_pnl", lambda s: int((s > 0).sum())),
                loss_days=("daily_pnl", lambda s: int((s < 0).sum())),
            )
        )

        # Date on which the worst (maximum-loss) day occurred, per month
        worst_rows = daily_tmp.loc[daily_tmp.groupby("month")["daily_pnl"].idxmin()]
        worst_day = worst_rows[["month", "day"]].rename(columns={"day": "max_loss_day_date"})

        monthwise_summary = monthwise_summary.merge(loss_day_stats, on="month", how="left")
        monthwise_summary = monthwise_summary.merge(worst_day, on="month", how="left")
        monthwise_summary = monthwise_summary.merge(day_count_stats, on="month", how="left")

        # Place the date column right after the max-loss value column
        _cols = list(monthwise_summary.columns)
        if "max_loss_day_date" in _cols and "max_loss_in_a_day" in _cols:
            _cols.remove("max_loss_day_date")
            _cols.insert(_cols.index("max_loss_in_a_day") + 1, "max_loss_day_date")
            monthwise_summary = monthwise_summary[_cols]

        # ---- Grand-total row across all months (totals for every column) ----
        if not monthwise_summary.empty:
            tot_trades = int(monthwise_summary["trades"].sum())
            tot_win = int(monthwise_summary["winning_trades"].sum())
            tot_lose = int(monthwise_summary["losing_trades"].sum())
            tot_pnl = float(monthwise_summary["total_exit_pnl"].sum())
            tot_prof_days = int(monthwise_summary["profitable_days"].sum())
            tot_loss_days = int(monthwise_summary["loss_days"].sum())
            neg_days = daily_tmp[daily_tmp["daily_pnl"] < 0]
            overall_worst_idx = daily_tmp["daily_pnl"].idxmin()
            total_row = {
                "month": "TOTAL",
                "trades": tot_trades,
                "total_exit_pnl": round(tot_pnl, 2),
                "avg_exit_pnl": round(tot_pnl / tot_trades, 2) if tot_trades else 0.0,
                "winning_trades": tot_win,
                "losing_trades": tot_lose,
                "win_rate_pct": round(100.0 * tot_win / tot_trades, 2) if tot_trades else 0.0,
                "avg_loss_on_loss_days": round(float(neg_days["daily_pnl"].mean()), 2) if len(neg_days) else 0.0,
                "max_loss_in_a_day": round(float(daily_tmp["daily_pnl"].min()), 2),
                "max_loss_day_date": daily_tmp.loc[overall_worst_idx, "day"],
                "profitable_days": tot_prof_days,
                "loss_days": tot_loss_days,
            }
            total_df = pd.DataFrame([total_row]).reindex(columns=monthwise_summary.columns)
            monthwise_summary = pd.concat([monthwise_summary, total_df], ignore_index=True)
    else:
        monthwise_summary = pd.DataFrame()

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as xw:
        all_trades_df.to_excel(xw, sheet_name="all_trades_backtested", index=False)
        actual_trades_df.to_excel(xw, sheet_name="actual_trades", index=False)
        monthwise_summary.to_excel(xw, sheet_name="monthwise_summary", index=False)
        piv_exit.to_excel(xw, sheet_name="exit_pnl_pivot", index=False)
        piv_eod_first.to_excel(xw, sheet_name="eod_pnl_first_trade_pivot", index=False)
        instrument_summary.to_excel(xw, sheet_name="instrument_summary", index=False)
        skipped_df.to_excel(xw, sheet_name="skipped", index=False)

        wb = xw.book
        for ws in wb.worksheets:
            ws.freeze_panes = "A2"
            _autosize_columns_safe(ws)

        _color_actual_trades_by_date(wb, actual_trades_df)

    print(f"[DONE] Excel written: {OUTPUT_XLSX}")


# =============================================================================
# MAIN
# =============================================================================
def main():
    paths = sorted(glob.glob(os.path.join(PICKLES_DIR, "*.pkl")) + glob.glob(os.path.join(PICKLES_DIR, "*.pickle")))
    if not paths:
        raise FileNotFoundError(f"No .pkl/.pickle files found in: {PICKLES_DIR}")

    print(f"[INFO] Pickles found: {len(paths)}")

    end_day, min_expiry_map, min_day_seen = scan_pickles_pass1(paths)
    window_start = determine_backtest_window_start(min_day_seen, end_day)

    lookback_label = "AUTO/full pickle range" if LOOKBACK_MONTHS is None else f"{LOOKBACK_MONTHS} months cap"

    print(f"[INFO] Data day-range seen: {min_day_seen} -> {end_day}")
    print(f"[INFO] Backtest window: {window_start} -> {end_day} ({lookback_label})")
    print(f"[INFO] Stoploss %/attempt: {_fmt_pct_list(LOSS_LIMIT_RUPEES_BY_ATTEMPT)} | "
          f"Per-attempt stop cap: Rs {_fmt_rupee_value(MAX_LOSS_LIMIT_RUPEES_BY_ATTEMPT)} | "
          f"Daily max loss: Rs {_fmt_rupee_value(MAX_DAILY_LOSS_RUPEES)} | "
          f"ProfitProtect trigger/giveback %: {_fmt_pct_value(PROFIT_PROTECT_TRIGGER_RUPEES)} | "
          f"Re-entry delay min/attempt: {REENTRY_DELAY_BY_ATTEMPT} | Allowed DTE: {ALLOWED_DTE}")
    print(f"[INFO] Entry time: {ENTRY_TIME_IST} | Strategy exit/filter cutoff: {EXIT_TIME_IST}")
    print(f"[INFO] Day profit target: {PROFIT_TARGET_PCT:.0%} of premium (0 = disabled)")
    print(f"[INFO] Tradeables: {sorted(TRADEABLE)}")
    print(f"[INFO] Output: {OUTPUT_XLSX}")

    print("[STEP] Initializing Kite ...")
    kite = oUtils.intialize_kite_api()
    print("[OK] Kite ready.")

    underlying_data = download_underlyings(kite, window_start, end_day)

    all_trades_df, skipped_df = process_pickles_generate_trades(
        default_params(), paths, min_expiry_map, underlying_data, window_start, end_day
    )

    actual_trades_df = build_actual_trades_df(all_trades_df, min_expiry_map)

    write_excel(all_trades_df, actual_trades_df, skipped_df)

    if not all_trades_df.empty:
        print(all_trades_df.groupby("underlying")[["exit_pnl"]].describe())
    else:
        print("[WARN] No completed trades. Check 'skipped' sheet for reasons.")




# =============================================================================
# FAST ENGINE (numpy) - used by the optimizer objective only
# =============================================================================
# WHY THIS EXISTS
#   The reference engine above (simulate_day_multi_trades) is kept untouched
#   because it produces the trade-level reports and is the source of truth.
#   Inside an optimizer, however, it is dominated by pandas overhead:
#     * Series.combine(..., max) for the late-giveback trail runs a Python loop
#       over every minute of every attempt (~50% of simulation time);
#     * asof_close() re-copies, re-parses and re-sorts the whole underlying
#       day on every attempt (~14%);
#     * tz-aware Timestamp arithmetic, .loc label slicing and pd.concat on
#       376-row series for every attempt.
#   The fast engine performs EXACTLY the same arithmetic on numpy arrays
#   indexed by integer minute offsets (0 = 09:15, 375 = 15:30, 385 = 15:40):
#     * all parameter-independent arrays are built once per day and cached;
#     * the same operations, in the same order, on the same float64 values,
#       so results are bit-identical to the reference engine (the start-up
#       parity check proves this on your data before any trial is run).
#
# MAPPING (reference -> fast)
#   cur_entry_ts / exit_ts       -> integer minute offset m
#   trade_end_ts                 -> end = min(last minute, exit_time - 09:15)
#   asof_close(underlying, ts)   -> u_asof[m]   (same 'pad' lookup, done once)
#   _leg_from_book(..)           -> raw close / ffilled close / high / low arrays
#   DataFrame.min/max(axis=1)    -> np.fmin / np.fmax (both skip NaN like pandas)
#   Series.cummax()              -> np.maximum.accumulate
#   trail.combine(late, max)     -> np.maximum
#   first True of a mask         -> mask.argmax() + a check that it is True
# =============================================================================
_SESSION_START_MINUTE = SESSION_START_IST.hour * 60 + SESSION_START_IST.minute  # 555


def _minute_of_day(value: dtime) -> int:
    """09:15 -> 555. Seconds are ignored (all strategy times are whole minutes)."""
    return int(value.hour * 60 + value.minute)


def _ffill_array(values: np.ndarray) -> np.ndarray:
    """
    Forward-fill NaNs in a 1-D float array (identical to pandas Series.ffill()).

    Leading NaNs (before the first real price) stay NaN, exactly as in pandas.
    """
    valid = ~np.isnan(values)
    if valid.all():
        return values.copy()
    positions = np.where(valid, np.arange(values.size), 0)
    np.maximum.accumulate(positions, out=positions)
    return values[positions]


def _underlying_asof_array(underlying_day: pd.DataFrame, idx_all: pd.DatetimeIndex) -> np.ndarray:
    """
    Underlying 'as-of' close for every minute of the session, computed once.

    Reproduces asof_close() for every timestamp in idx_all in one vectorised
    call: drop NaNs, normalise to IST, sort, then 'pad' (last close at or
    before the minute). Minutes before the first candle are NaN.
    """
    out = np.full(len(idx_all), np.nan, dtype=np.float64)
    if underlying_day is None or underlying_day.empty:
        return out
    frame = underlying_day[["date", "close"]].dropna().copy()
    if frame.empty:
        return out
    frame["date"] = ensure_ist(frame["date"])
    frame = frame.sort_values("date").set_index("date")
    loc = frame.index.get_indexer(idx_all, method="pad")
    closes = frame["close"].to_numpy(dtype=np.float64)
    hit = loc >= 0
    out[hit] = closes[loc[hit]]
    return out


@dataclass
class FastDay:
    """
    One traded (underlying, date, nearest-expiry) unit in numpy form.

    raw[(strike, 'CE'|'PE')] = (close_raw, high, low)   - unfilled, as in the price book
    legs[(strike, 'CE'|'PE')] = (close_raw, close_ffill, high, low) - built by prepare()
    u_asof[m]                = underlying as-of close at minute offset m
    symbols                  = same symbol map as the reference price book
    underlying_day           = kept only to rebuild a reference DayGroup for
                               parity checks and the final trade report
    """

    und: str
    dy: date
    expiry: date
    qty: int
    step: int
    n: int
    month_key: int
    month_label: str
    u_asof: np.ndarray
    raw: Dict[Tuple[int, str], Tuple[np.ndarray, np.ndarray, np.ndarray]]
    symbols: Dict[Tuple[int, str], str]
    underlying_day: pd.DataFrame
    legs: Dict[Tuple[int, str], Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]] = field(
        default_factory=dict
    )
    # ATM straddle premium (ffilled CE + PE close) per strike, for the momentum
    # gate. Parameter-independent, so it is cached across ALL trials.
    prem_cache: Dict[int, Optional[np.ndarray]] = field(default_factory=dict)

    def prepare(self) -> "FastDay":
        """Build the forward-filled close arrays (not stored in the cache file)."""
        self.legs = {
            key: (close_raw, _ffill_array(close_raw), high, low)
            for key, (close_raw, high, low) in self.raw.items()
        }
        self.prem_cache = {}
        return self

    def premium(self, strike: int) -> Optional[np.ndarray]:
        """Same as _atm_prem_series() in the reference engine, cached per strike."""
        if strike not in self.prem_cache:
            ce = self.legs.get((strike, "CE")) if self.symbols.get((strike, "CE")) else None
            pe = self.legs.get((strike, "PE")) if self.symbols.get((strike, "PE")) else None
            self.prem_cache[strike] = None if (ce is None or pe is None) else ce[1] + pe[1]
        return self.prem_cache[strike]


def build_fast_day(group: DayGroup) -> FastDay:
    """Convert one cached reference DayGroup into its numpy form."""
    if group.idx_all is None or group.price_book is None or group.symbols is None:
        group.idx_all = build_minute_index(group.dy, SESSION_START_IST, day_session_end(group.day_opt))
        group.price_book, group.symbols = build_price_book(group.day_opt, group.idx_all)

    raw: Dict[Tuple[int, str], Tuple[np.ndarray, np.ndarray, np.ndarray]] = {}
    for (strike, opt_type) in group.symbols:
        key = (int(strike), str(opt_type))
        raw[key] = tuple(
            _leg_from_book(group.price_book, group.idx_all, key[0], key[1], col)
            .to_numpy(dtype=np.float64, copy=True)
            for col in ("close", "high", "low")
        )  # type: ignore[assignment]

    return FastDay(
        und=group.und,
        dy=group.dy,
        expiry=group.expiry,
        qty=int(QTY_UNITS[group.und]),
        step=int(STRIKE_STEP[group.und]),
        n=len(group.idx_all),
        month_key=group.dy.year * 12 + (group.dy.month - 1),
        month_label=f"{group.dy.year:04d}-{group.dy.month:02d}",  # == str(pd.Period(..., 'M'))
        u_asof=_underlying_asof_array(group.underlying_day, group.idx_all),
        raw=raw,
        symbols={(int(k), str(t)): s for (k, t), s in group.symbols.items()},
        underlying_day=group.underlying_day,
    ).prepare()


def daygroup_from_fast(fd: FastDay) -> DayGroup:
    """
    Rebuild a reference DayGroup from a FastDay (for parity checks and the
    final trade report). The price book is rebuilt from the very same arrays,
    so both engines read identical prices.
    """
    # fd.n is 376 (to 15:30) or longer (to SESSION_END_IST), as chosen by
    # day_session_end() when the day was loaded.
    idx_all = build_minute_index(fd.dy, SESSION_START_IST, SESSION_END_IST)[: fd.n]
    book: Dict[Tuple[int, str, str], pd.Series] = {}
    for (strike, opt_type), (close_raw, high, low) in fd.raw.items():
        book[(strike, opt_type, "close")] = pd.Series(close_raw, index=idx_all, dtype="float64")
        book[(strike, opt_type, "high")] = pd.Series(high, index=idx_all, dtype="float64")
        book[(strike, opt_type, "low")] = pd.Series(low, index=idx_all, dtype="float64")
    return DayGroup(
        und=fd.und,
        dy=fd.dy,
        expiry=fd.expiry,
        day_opt=pd.DataFrame(),
        underlying_day=fd.underlying_day,
        price_book=book,
        symbols=dict(fd.symbols),
        idx_all=idx_all,
    )


@dataclass(frozen=True)
class _FastProfile:
    """Per-underlying settings pre-converted to plain floats / minute numbers."""

    pt: float               # profit target % of premium
    pt_late: float          # late profit target % (0 = off)
    pt_late_from_mod: int   # minute-of-day from which ENTRIES use pt_late
    g: float                # profit-protect arm/giveback % (0 = off)
    dll: float              # daily loss circuit breaker in Rs (0 = off)
    g2: float               # late tighter giveback % (0 = off)
    g2_m: int               # minute OFFSET (0 = 09:15) from which g2 applies


@dataclass(frozen=True)
class _FastParams:
    """A Params object pre-converted for the fast kernel (built once per trial)."""

    entry_m: int
    exit_m: int
    loss_pcts: Tuple[float, ...]
    cap: float
    max_re: int
    delays: Tuple[int, ...]
    gate: bool
    lookback: int
    tgt_on: bool
    tgt_delay: int
    profiles: Dict[str, _FastProfile]


def _fast_params(params: Params) -> _FastParams:
    """Translate the simulator's Params into the kernel's flat representation."""
    profiles = {
        und: _FastProfile(
            pt=float(p.profit_target_pct),
            pt_late=float(p.profit_target_pct_late),
            pt_late_from_mod=_minute_of_day(p.profit_target_late_from),
            g=float(p.profit_protect_pct),
            dll=float(p.max_daily_loss_rupees),
            g2=float(p.profit_protect_late_giveback_pct),
            g2_m=_minute_of_day(p.profit_protect_late_from) - _SESSION_START_MINUTE,
        )
        for und, p in params.profiles.items()
    }
    return _FastParams(
        entry_m=_minute_of_day(params.entry_time) - _SESSION_START_MINUTE,
        exit_m=_minute_of_day(params.exit_time) - _SESSION_START_MINUTE,
        loss_pcts=tuple(float(v) for v in params.loss_limit_pct_by_attempt),
        cap=float(params.max_loss_limit_cap_rupees),
        max_re=int(params.max_reattempts),
        delays=tuple(int(v) for v in params.reentry_delay_by_attempt),
        gate=bool(params.entry_momentum_gate),
        lookback=int(params.momentum_lookback_min),
        tgt_on=bool(params.profit_target_reentry_enabled),
        tgt_delay=int(params.profit_target_reentry_delay_min),
        profiles=profiles,
    )


def _fast_momentum_wait(fd: FastDay, fp: _FastParams, m: int, end: int) -> int:
    """
    Numpy twin of _momentum_wait(): advance minute by minute until the current
    ATM straddle premium is LOWER than `lookback` minutes earlier, or until the
    strategy cutoff. Returns m unchanged when the gate is off.
    """
    if not fp.gate:
        return m
    n = fd.n
    while m < end:
        u = fd.u_asof[m] if 0 <= m < n else math.nan
        if u != u:  # NaN: no underlying price -> enter now (reference behaviour)
            return m
        series = fd.premium(round_to_step(float(u), fd.step))
        if series is None:
            return m
        now = series[m] if 0 <= m < n else math.nan
        back = m - fp.lookback
        if back < 0:
            back = 0          # clamp to session start, as the reference does
        before = series[back] if back < n else math.nan
        if now == now and before == before and float(now) < float(before):
            return m
        m += 1
    return m


def fast_simulate_day(fd: FastDay, fp: _FastParams, slippage_pts: float = 0.0) -> Tuple[float, int, bool]:
    """
    Simulate one day with the fast kernel.

    Returns (net_pnl_for_the_day, number_of_attempts, reattempt_cap_hit) where
    reattempt_cap_hit is True when MAX_REATTEMPTS stopped a re-entry that the
    rules would otherwise have taken.

    Every block below mirrors the reference loop in simulate_day_multi_trades
    in the same order; comments name the reference step.
    """
    nan = math.nan
    n = fd.n
    end = min(n - 1, fp.exit_m)                      # trade_end_ts
    qty = fd.qty
    prof = fp.profiles[fd.und]
    slip_cost = 4.0 * float(slippage_pts) * qty if slippage_pts > 0 else 0.0

    # First entry, momentum-gated; at/after the cutoff -> no trade that day.
    m = _fast_momentum_wait(fd, fp, fp.entry_m, end)
    if m >= end:
        return 0.0, 0, False

    seq = 1
    daily = 0.0
    n_trades = 0
    cap_hit = False
    dll = prof.dll
    dll_on = dll > 0
    loss_pcts = fp.loss_pcts
    delays = fp.delays
    u_asof = fd.u_asof
    legs = fd.legs
    e1 = end + 1                                      # inclusive label slice end

    while m < end:
        # Daily loss circuit breaker (checked before every entry).
        if dll_on and daily <= -dll:
            break

        # Underlying as-of price -> ATM strike.
        u = u_asof[m] if 0 <= m < n else nan
        if u != u:
            break
        atm = round_to_step(float(u), fd.step)
        if not fd.symbols.get((atm, "CE")) or not fd.symbols.get((atm, "PE")):
            break
        ce_raw, ce_ff, ce_hi, ce_lo = legs[(atm, "CE")]
        pe_raw, pe_ff, pe_hi, pe_lo = legs[(atm, "PE")]

        # 'Entry timestamp not in session index'
        if m < 0 or m >= n:
            break
        ce0 = ce_raw[m]
        pe0 = pe_raw[m]
        ms = m + 1                                    # monitor_start_ts
        if ms > end:
            break
        if ce0 != ce0 or pe0 != pe0:                  # no CE/PE price at entry
            break
        ce0 = float(ce0)
        pe0 = float(pe0)

        # Percentage-based risk basis for THIS attempt.
        prem = (ce0 + pe0) * qty                      # entry_premium_sum
        k = seq - 1
        if loss_pcts:
            sl_pct = loss_pcts[k] if k < len(loss_pcts) else loss_pcts[-1]
        else:
            sl_pct = 0.0
        uncapped = float(sl_pct * prem)
        loss_r = float(min(uncapped, fp.cap)) if fp.cap > 0 else uncapped
        G = float(prof.g * prem)

        # Close-based P&L from monitor start to the cutoff (inclusive).
        pnl = (ce0 - ce_ff[ms:e1]) * qty + (pe0 - pe_ff[ms:e1]) * qty

        # STOPLOSS: worst of (close, CE-high/PE-low, CE-low/PE-high) per minute.
        a = (ce0 - ce_hi[ms:e1]) * qty + (pe0 - pe_lo[ms:e1]) * qty
        b = (ce0 - ce_lo[ms:e1]) * qty + (pe0 - pe_hi[ms:e1]) * qty
        worst = np.fmin(np.fmin(pnl, a), b)
        hit = worst <= -loss_r
        i_stop = int(hit.argmax())
        if not hit[i_stop]:
            i_stop = -1

        # PROFIT_PROTECT: arm at peak >= G, exit when P&L <= peak - giveback.
        i_prot = -1
        if prof.g > 0.0:
            peak = np.maximum.accumulate(pnl)
            trail = peak - G
            if prof.g2 > 0.0:
                k0 = prof.g2_m - ms                   # first index at/after g2 time
                if k0 < pnl.size:
                    if k0 < 0:
                        k0 = 0
                    late = peak[k0:] - float(prof.g2) * prem
                    trail[k0:] = np.maximum(trail[k0:], late)
            phit = (peak >= G) & (pnl <= trail)
            i_prot = int(phit.argmax())
            if not phit[i_prot]:
                i_prot = -1

        # PROFIT_TARGET: late target for attempts ENTERED at/after late-from.
        pt_eff = prof.pt
        if prof.pt_late > 0.0 and (m + _SESSION_START_MINUTE) >= prof.pt_late_from_mod:
            pt_eff = prof.pt_late
        i_tgt = -1
        target_rupees = 0.0
        if pt_eff > 0.0:
            target_rupees = pt_eff * prem
            best = (ce0 - ce_lo[ms:e1]) * qty + (pe0 - pe_lo[ms:e1]) * qty
            thit = np.fmax(pnl, best) >= float(target_rupees)
            i_tgt = int(thit.argmax())
            if not thit[i_tgt]:
                i_tgt = -1

        # Earliest exit wins; ties: STOPLOSS(0) < PROFIT_TARGET(1) < PROFIT_PROTECT(2).
        exit_i = pnl.size - 1
        reason = 3                                    # EOD / TIME_EXIT
        chosen: Optional[Tuple[int, int]] = None
        for idx_hit, priority in ((i_stop, 0), (i_tgt, 1), (i_prot, 2)):
            if idx_hit >= 0 and (chosen is None or (idx_hit, priority) < chosen):
                chosen = (idx_hit, priority)
        if chosen is not None:
            exit_i, reason = chosen

        if reason == 0:
            gross = -float(loss_r)
        elif reason == 1:
            gross = float(target_rupees)
        else:
            gross = float(pnl[exit_i])

        exit_m = ms + exit_i
        exit_ce = float(ce_ff[exit_m])
        exit_pe = float(pe_ff[exit_m])
        charges = compute_trade_charges(
            entry_ce=ce0, entry_pe=pe0,
            exit_ce=exit_ce if exit_ce == exit_ce else 0.0,
            exit_pe=exit_pe if exit_pe == exit_pe else 0.0,
            qty=qty,
        )
        net = gross - charges
        if slip_cost:
            net -= slip_cost                           # optimizer-only cost model
        daily += float(net)
        n_trades += 1

        if dll_on and daily <= -dll:
            break

        # STOPLOSS / PROFIT_PROTECT -> re-entry after the per-attempt delay.
        if reason == 0 or reason == 2:
            if k < fp.max_re:
                if delays:
                    delay = delays[k] if k < len(delays) else delays[-1]
                else:
                    delay = 0
                seq += 1
                m = _fast_momentum_wait(fd, fp, exit_m + delay, end)
                if m >= end:
                    break
                continue
            cap_hit = True
            break

        # PROFIT_TARGET -> recycle into a fresh ATM straddle.
        if reason == 1 and fp.tgt_on:
            if k < fp.max_re:
                seq += 1
                m = _fast_momentum_wait(fd, fp, exit_m + fp.tgt_delay, end)
                if m >= end:
                    break
                continue
            cap_hit = True
        break

    return daily, n_trades, cap_hit


def fast_simulate(
    fp: _FastParams,
    days: List[FastDay],
    slippage_pts: Dict[str, float],
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Run the fast kernel over a list of days -> (net_pnl, n_trades, cap_hit) arrays."""
    count = len(days)
    pnl = np.zeros(count, dtype=np.float64)
    trades = np.zeros(count, dtype=np.int64)
    caps = np.zeros(count, dtype=bool)
    for i, fd in enumerate(days):
        pnl[i], trades[i], caps[i] = fast_simulate_day(fd, fp, slippage_pts.get(fd.und, 0.0))
    return pnl, trades, caps


@dataclass
class DaySet:
    """A fixed list of FastDays plus the month bookkeeping used for scoring."""

    days: List[FastDay]
    month_keys: np.ndarray
    month_labels: Dict[int, str]

    @staticmethod
    def of(days: List[FastDay]) -> "DaySet":
        return DaySet(
            days=list(days),
            month_keys=np.array([fd.month_key for fd in days], dtype=np.int64),
            month_labels={fd.month_key: fd.month_label for fd in days},
        )


def _summarise_fast(pnl: np.ndarray, trades: np.ndarray, caps: np.ndarray, dayset: DaySet) -> Dict[str, Any]:
    """
    Fast twin of robustness_metrics(): only days with at least one trade count,
    exactly like the pandas version (which groups actual trades by day).
    All values are plain Python types so they can be stored as a trial attribute.
    """
    traded = trades > 0
    daily = pnl[traded]
    base: Dict[str, Any] = {
        "n_trades": int(trades.sum()),
        "cap_hit_days": int(caps.sum()),
    }
    if daily.size == 0:
        base.update({
            "n_days": 0, "n_months": 0, "profitable_day_ratio": 0.0,
            "profitable_month_ratio": 0.0, "total_pnl": 0.0, "mean_month": 0.0,
            "median_month": 0.0, "worst_day": 0.0, "worst_month": 0.0,
            "monthly": {},
        })
        return base

    keys, inverse = np.unique(dayset.month_keys[traded], return_inverse=True)
    monthly = np.bincount(inverse, weights=daily)
    base.update({
        "n_days": int(daily.size),
        "n_months": int(monthly.size),
        "profitable_day_ratio": float((daily > 0).mean()),
        "profitable_month_ratio": float((monthly > 0).mean()),
        "total_pnl": float(daily.sum()),
        "mean_month": float(monthly.mean()),
        "median_month": float(np.median(monthly)),
        "worst_day": float(daily.min()),
        "worst_month": float(monthly.min()),
        "monthly": {dayset.month_labels[int(key)]: float(val) for key, val in zip(keys, monthly)},
    })
    return base



# =============================================================================
# DATA LOADING + LOCAL CACHE (optimizer only)
# =============================================================================
# The old optimizer re-ran three slow phases on every start:
#   Phase 1  read every pickle to find the nearest expiry per (underlying, day)
#   Phase 2  download NIFTY/SENSEX minute data from Kite
#   Phase 3  read every pickle AGAIN and build per-day price books
# Reading pickles from a Google Drive folder (G:\My Drive\...) is especially
# slow. The result of all three phases - restricted to the days that are
# actually traded - is now saved to one local file. The cache key covers the
# pickle list (name, size, modification time) and every setting that changes
# which days are loaded, so a stale cache is never reused silently.
#
# The cache stores only plain Python / numpy / pandas objects (no classes from
# this module), so it loads no matter how the script is started.
_CACHE_FORMAT_VERSION = 3


def _pickle_paths(max_pickles: Optional[int]) -> List[str]:
    """Sorted list of option pickles in scope (same rule as the backtest)."""
    paths = sorted(
        glob.glob(os.path.join(PICKLES_DIR, "*.pkl"))
        + glob.glob(os.path.join(PICKLES_DIR, "*.pickle"))
    )
    if not paths:
        raise FileNotFoundError(f"No .pkl/.pickle files found in: {PICKLES_DIR}")
    if max_pickles is not None and max_pickles > 0:
        paths = paths[:max_pickles]
    return paths


def _data_cache_path(paths: List[str], max_days: Optional[int]) -> str:
    """Cache file name derived from everything that decides the loaded data."""
    stats = []
    for path in paths:
        st = os.stat(path)
        stats.append([os.path.basename(path), int(st.st_size), int(st.st_mtime)])
    key = json.dumps(
        {
            "version": _CACHE_FORMAT_VERSION,
            "pickles_dir": os.path.abspath(PICKLES_DIR),
            "files": stats,
            "lookback": LOOKBACK_MONTHS_RAW,
            "allowed_dte": list(ALLOWED_DTE),
            "tradeable": sorted(TRADEABLE),
            "qty": QTY_UNITS,
            "strike_step": STRIKE_STEP,
            "session_end": SESSION_END_IST.strftime("%H:%M"),   # minute grid length
            "max_days": max_days,
        },
        sort_keys=True,
    )
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]
    return os.path.join(OPT_OUTPUT_DIR, "data_cache", f"v3opt_days_{digest}.pkl")


def _fast_day_to_record(fd: FastDay) -> Dict[str, Any]:
    """Plain-dict form of a FastDay for the cache file."""
    return {
        "und": fd.und, "dy": fd.dy, "expiry": fd.expiry,
        "n": fd.n, "u_asof": fd.u_asof, "raw": fd.raw,
        "symbols": fd.symbols, "underlying_day": fd.underlying_day,
    }


def _fast_day_from_record(rec: Dict[str, Any]) -> FastDay:
    """Inverse of _fast_day_to_record(); qty/step come from the current settings."""
    dy: date = rec["dy"]
    return FastDay(
        und=rec["und"], dy=dy, expiry=rec["expiry"],
        qty=int(QTY_UNITS[rec["und"]]), step=int(STRIKE_STEP[rec["und"]]),
        n=int(rec["n"]),
        month_key=dy.year * 12 + (dy.month - 1),
        month_label=f"{dy.year:04d}-{dy.month:02d}",
        u_asof=rec["u_asof"], raw=rec["raw"], symbols=rec["symbols"],
        underlying_day=rec["underlying_day"],
    ).prepare()


def load_or_build_fast_data(
    max_pickles: Optional[int],
    max_days: Optional[int],
) -> Tuple[List[FastDay], Dict[Tuple[str, date], date], date, date, List[Dict[str, Any]]]:
    """
    Return (fast_days, min_expiry_map, window_start, end_day, parse_skips).

    Cache hit  -> load the local file (seconds).
    Cache miss -> Phase 1 scan, Phase 2 Kite download, Phase 3 day-group build
                  restricted to traded days, numpy conversion, then save.
    """
    paths = _pickle_paths(max_pickles)
    print(f"[DATA] pickle files in scope: {len(paths)}", flush=True)
    cache_path = _data_cache_path(paths, max_days)

    if OPT_DATA_CACHE and not OPT_REFRESH_DATA_CACHE and os.path.exists(cache_path):
        t0 = time.time()
        try:
            with open(cache_path, "rb") as handle:
                blob = pickle.load(handle)
            if blob.get("version") != _CACHE_FORMAT_VERSION:
                raise ValueError("cache format version mismatch")
            fast_days = [_fast_day_from_record(rec) for rec in blob["days"]]
            print(
                f"[DATA] loaded {len(fast_days)} traded days from cache in "
                f"{time.time() - t0:.1f}s: {cache_path}",
                flush=True,
            )
            return (
                fast_days, blob["min_expiry_map"], blob["window_start"],
                blob["end_day"], blob["parse_skips"],
            )
        except Exception as exc:  # corrupt / old cache -> rebuild
            print(f"[DATA] cache unusable ({exc}); rebuilding", flush=True)

    print("[PHASE 1] Scanning option pickles ...", flush=True)
    end_day, min_expiry_map, min_day_seen = scan_pickles_pass1(paths)
    window_start = determine_backtest_window_start(min_day_seen, end_day)
    print(f"[PHASE 1] backtest window: {window_start} -> {end_day}", flush=True)

    print("[PHASE 2] Downloading NIFTY/SENSEX underlying minute data ...", flush=True)
    kite = oUtils.intialize_kite_api()
    underlying_data = download_underlyings(kite, window_start, end_day)

    print("[PHASE 3] Building cached day-groups (traded days only) ...", flush=True)
    groups, parse_skips = build_day_groups(
        paths,
        min_expiry_map,
        underlying_data,
        window_start,
        end_day,
        max_pickles=max_pickles,
        max_days=max_days,
        traded_only=pick_actual_underlying_by_day(min_expiry_map),
        release_raw=True,
    )
    if not groups:
        raise RuntimeError(
            "No day-groups were built. Check the pickle path, date window, "
            "allowed underlyings and underlying data."
        )

    print(f"[PHASE 3] converting {len(groups)} day-groups to numpy ...", flush=True)
    fast_days = [build_fast_day(group) for group in groups]
    del groups

    if OPT_DATA_CACHE:
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        tmp_path = cache_path + ".tmp"
        with open(tmp_path, "wb") as handle:
            pickle.dump(
                {
                    "version": _CACHE_FORMAT_VERSION,
                    "days": [_fast_day_to_record(fd) for fd in fast_days],
                    "min_expiry_map": min_expiry_map,
                    "window_start": window_start,
                    "end_day": end_day,
                    "parse_skips": parse_skips,
                },
                handle,
                protocol=pickle.HIGHEST_PROTOCOL,
            )
        os.replace(tmp_path, cache_path)  # atomic: never leaves a half-written cache
        print(f"[DATA] cache written: {cache_path}", flush=True)

    return fast_days, min_expiry_map, window_start, end_day, parse_skips


# =============================================================================
# PARITY CHECKS (fast engine vs reference engine)
# =============================================================================
def _reference_run(
    params: Params,
    days: List[FastDay],
    min_expiry_map: Dict[Tuple[str, date], date],
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Run the untouched pandas engine on the given days -> (all, actual, skipped)."""
    groups = [daygroup_from_fast(fd) for fd in days]
    all_df, skips_df = simulate_groups(params, groups)
    actual_df = build_actual_trades_df(all_df, min_expiry_map)
    return all_df, actual_df, skips_df


def _reference_per_day(actual_df: pd.DataFrame) -> Dict[Tuple[str, date], Tuple[float, int]]:
    """{(underlying, day): (net P&L, number of attempts)} from reference trades."""
    if actual_df is None or actual_df.empty:
        return {}
    grouped = actual_df.groupby(["underlying", "day"])["exit_pnl"].agg(["sum", "count"])
    return {
        (str(und), dy): (float(row["sum"]), int(row["count"]))
        for (und, dy), row in grouped.iterrows()
    }


def _assert_parity(
    label: str,
    params: Params,
    days: List[FastDay],
    min_expiry_map: Dict[Tuple[str, date], date],
    actual_df: Optional[pd.DataFrame] = None,
) -> None:
    """
    Compare the fast kernel (no slippage) with the reference engine day by day.

    Raises RuntimeError if any day's net P&L differs by more than Rs 0.01 or
    the number of attempts differs. Pass `actual_df` to reuse a reference run.
    """
    t0 = time.time()
    reused = actual_df is not None
    if actual_df is None:
        _, actual_df, _ = _reference_run(params, days, min_expiry_map)
    t_ref = time.time() - t0

    t1 = time.time()
    pnl, trades, _ = fast_simulate(_fast_params(params), days, {})
    t_fast = time.time() - t1

    ref = _reference_per_day(actual_df)
    mismatches = []
    max_diff = 0.0
    for i, fd in enumerate(days):
        ref_pnl, ref_n = ref.get((fd.und, fd.dy), (0.0, 0))
        diff = abs(float(pnl[i]) - ref_pnl)
        max_diff = max(max_diff, diff)
        if diff > _PARITY_TOLERANCE_RUPEES or int(trades[i]) != ref_n:
            mismatches.append(
                f"{fd.dy} {fd.und}: fast={pnl[i]:.2f} ({trades[i]} trades) "
                f"reference={ref_pnl:.2f} ({ref_n} trades)"
            )

    if mismatches:
        raise RuntimeError(
            f"[PARITY FAIL] {label}: {len(mismatches)} of {len(days)} days differ. "
            "The fast engine does not reproduce the reference engine on this data; "
            "do not trust the optimizer until this is fixed. First differences:\n  "
            + "\n  ".join(mismatches[:10])
        )
    timing = f"fast {t_fast:.2f}s"
    if not reused and t_fast > 0:
        timing = f"reference {t_ref:.1f}s vs fast {t_fast:.2f}s (~{t_ref / t_fast:.0f}x)"
    print(
        f"[PARITY OK] {label}: {len(days)} days, max |diff|=Rs {max_diff:.6f}, {timing}",
        flush=True,
    )


def _even_sample(days: List[FastDay], limit: int) -> List[FastDay]:
    """Up to `limit` evenly spaced days (all days when limit is 0 or larger)."""
    if limit <= 0 or len(days) <= limit:
        return list(days)
    picks = np.linspace(0, len(days) - 1, limit).round().astype(int)
    return [days[i] for i in sorted(set(picks.tolist()))]


# =============================================================================
# SCORING
# =============================================================================
def _configure_activity_minima(in_days: List[FastDay]) -> Tuple[int, int]:
    """
    NEW: fix the effective activity guard for this run from the in-sample data.

    min days   = min(OPT_MIN_DAYS,   75% of in-sample traded days)
    min months = min(OPT_MIN_MONTHS, number of in-sample months)
    Smoke samples (SAMPLE_MAX_*) keep the old relaxation to 1 / 1.
    """
    global _ACTIVE_MINIMA
    n_days = len(in_days)
    n_months = len({fd.month_key for fd in in_days})
    if SAMPLE_MAX_DAYS is not None or SAMPLE_MAX_PICKLES is not None:
        _ACTIVE_MINIMA = (1, 1)
    else:
        _ACTIVE_MINIMA = (
            max(1, min(OPT_MIN_DAYS, int(_MIN_DAYS_FRACTION * n_days))),
            max(1, min(OPT_MIN_MONTHS, n_months)),
        )
    print(
        f"[GUARD] a trial must trade on >= {_ACTIVE_MINIMA[0]} of the {n_days} in-sample "
        f"days and in >= {_ACTIVE_MINIMA[1]} of the {n_months} in-sample months "
        f"(OPT_MIN_DAYS={OPT_MIN_DAYS}, OPT_MIN_MONTHS={OPT_MIN_MONTHS}, capped to the data).",
        flush=True,
    )
    return _ACTIVE_MINIMA


def _activity_minima() -> Tuple[int, int]:
    """Effective guard for this run (see _configure_activity_minima)."""
    if _ACTIVE_MINIMA is not None:
        return _ACTIVE_MINIMA
    # Fallback before configuration (not used by optimize()).
    if SAMPLE_MAX_DAYS is not None or SAMPLE_MAX_PICKLES is not None:
        return 1, 1
    return max(1, OPT_MIN_DAYS), max(1, OPT_MIN_MONTHS)


def _score_from_metrics(metrics: Dict[str, Any]) -> float:
    """Default objective: total net profit in rupees (in-sample days only)."""
    min_days, min_months = _activity_minima()
    if metrics["n_days"] < min_days or metrics["n_months"] < min_months:
        return _OPT_DISQUALIFY
    return float(metrics["total_pnl"])


def _cv_score_from_metrics(metrics: Dict[str, Any], folds: int) -> float:
    """
    Optional contiguous-month walk-forward objective (OPT_CV_FOLDS > 1).

    Same rule as before: split the months into `folds` contiguous blocks and
    return mean(block profit) - OPT_CV_PENALTY * std(block profit, ddof=0).
    """
    min_days, min_months = _activity_minima()
    monthly = list(metrics["monthly"].values())          # chronological
    if metrics["n_days"] < min_days or len(monthly) < max(min_months, folds):
        return _OPT_DISQUALIFY

    count = len(monthly)
    blocks: List[float] = []
    for fold_index in range(folds):
        lower = (fold_index * count) // folds
        upper = ((fold_index + 1) * count) // folds
        if upper > lower:
            blocks.append(float(sum(monthly[lower:upper])))
    if not blocks:
        return _OPT_DISQUALIFY
    values = np.asarray(blocks, dtype=np.float64)
    return float(values.mean() - OPT_CV_PENALTY * values.std(ddof=0))


def evaluate_fast(
    params: Params,
    dayset: DaySet,
    slippage_pts: Dict[str, float],
    cv_folds: int,
) -> Dict[str, Any]:
    """One complete fast evaluation: simulate, summarise, score."""
    pnl, trades, caps = fast_simulate(_fast_params(params), dayset.days, slippage_pts)
    metrics = _summarise_fast(pnl, trades, caps, dayset)
    metrics["objective"] = (
        _cv_score_from_metrics(metrics, cv_folds) if cv_folds > 1
        else _score_from_metrics(metrics)
    )
    return metrics


def robustness_metrics(actual_df: pd.DataFrame) -> Dict[str, Any]:
    """Daily/monthly diagnostics from REFERENCE-engine trades (final reports)."""
    if actual_df is None or actual_df.empty:
        return {
            "n_days": 0,
            "n_months": 0,
            "profitable_day_ratio": 0.0,
            "profitable_month_ratio": 0.0,
            "total_pnl": 0.0,
            "mean_month": 0.0,
            "median_month": 0.0,
            "worst_day": 0.0,
            "worst_month": 0.0,
            "monthly": pd.Series(dtype="float64"),
        }

    trades = actual_df.copy()
    day_values = pd.to_datetime(trades["day"])
    daily = trades.groupby(day_values.dt.date)["exit_pnl"].sum().sort_index()
    monthly = (
        trades.groupby(day_values.dt.to_period("M"))["exit_pnl"]
        .sum()
        .sort_index()
    )

    return {
        "n_days": int(len(daily)),
        "n_months": int(len(monthly)),
        "profitable_day_ratio": float((daily > 0).mean()),
        "profitable_month_ratio": float((monthly > 0).mean()),
        "total_pnl": float(daily.sum()),
        "mean_month": float(monthly.mean()),
        "median_month": float(monthly.median()),
        "worst_day": float(daily.min()),
        "worst_month": float(monthly.min()),
        "monthly": monthly,
    }



# =============================================================================
# SEARCH SPACE
# =============================================================================
def _time_to_minutes(value: dtime) -> int:
    return int(value.hour * 60 + value.minute)


def _minutes_to_time(total_minutes: int) -> dtime:
    total_minutes = int(total_minutes)
    hour, minute = divmod(total_minutes, 60)
    if not (0 <= hour <= 23):
        raise ValueError(f"Invalid minute-of-day value: {total_minutes}")
    return dtime(hour, minute)


def _round_param(value: float) -> float:
    """Round an optimizer float so the .properties file reproduces it exactly."""
    return round(float(value), _PARAM_DECIMALS)


def _bounded_time_suggestion(
    trial,
    *,
    name: str,
    from_ist: str,
    to_ist: str,
    step_minutes: int,
    base_value: dtime,
    enabled: bool,
) -> dtime:
    """Suggest a time on a fixed minute grid, or return the property-file value."""
    if not enabled:
        return base_value

    start_minute = _time_to_minutes(parse_hhmm(from_ist))
    end_minute = _time_to_minutes(parse_hhmm(to_ist))
    if end_minute < start_minute:
        raise ValueError(f"{name}: end time must not precede start time")

    offset_max = end_minute - start_minute
    offset = trial.suggest_int(name, 0, offset_max, step=max(1, step_minutes))
    return _minutes_to_time(start_minute + offset)


def _scaled_stop_schedule(base_list: List[float], scale: float) -> List[float]:
    """
    NEW: the stop-loss % list is searched as ONE multiplier on the property-file
    list. scale=1.0 returns the file's list unchanged (exact seed).

    Each value is capped at 0.95 because the strategy's _parse_pct_value()
    treats anything above 1.0 as a percent (1.02 would become 1.02%).
    """
    return [_round_param(min(_STOP_PCT_UPPER, float(v) * float(scale))) for v in base_list]


def _profile_from_trial(
    trial,
    *,
    underlying: str,
    base: UnderlyingProfile,
) -> UnderlyingProfile:
    """
    Create one NIFTY/SENSEX profile from independent suggestions.

    Two consistency rules are kept from the previous version:
      * a late giveback wider than the normal giveback cannot tighten the
        trail, so it is clipped to the normal giveback;
      * the late-session target is never harder than the normal target.
    """
    if OPT_TUNE_PROFILES:
        protect_pct = _round_param(trial.suggest_float(
            f"{underlying}_profit_protect_pct", OPT_PROTECT_MIN, OPT_PROTECT_MAX,
        ))
        late_giveback_raw = _round_param(trial.suggest_float(
            f"{underlying}_late_giveback_pct", OPT_LATE_GIVEBACK_MIN, OPT_LATE_GIVEBACK_MAX,
        ))
        late_giveback_pct = min(late_giveback_raw, protect_pct)

        target_pct = _round_param(trial.suggest_float(
            f"{underlying}_profit_target_pct", OPT_TARGET_MIN, OPT_TARGET_MAX,
        ))
        late_target_raw = _round_param(trial.suggest_float(
            f"{underlying}_late_profit_target_pct", OPT_LATE_TARGET_MIN, OPT_LATE_TARGET_MAX,
        ))
        late_target_pct = min(late_target_raw, target_pct)

        target_late_from = _bounded_time_suggestion(
            trial,
            name=f"{underlying}_target_late_from_offset",
            from_ist=OPT_LATE_FROM_IST,
            to_ist=OPT_LATE_TO_IST,
            step_minutes=OPT_LATE_TIME_STEP_MIN,
            base_value=base.profit_target_late_from,
            enabled=True,
        )
        protect_late_from = _bounded_time_suggestion(
            trial,
            name=f"{underlying}_protect_late_from_offset",
            from_ist=OPT_LATE_FROM_IST,
            to_ist=OPT_LATE_TO_IST,
            step_minutes=OPT_LATE_TIME_STEP_MIN,
            base_value=base.profit_protect_late_from,
            enabled=True,
        )
    else:
        protect_pct = base.profit_protect_pct
        late_giveback_pct = base.profit_protect_late_giveback_pct
        target_pct = base.profit_target_pct
        late_target_pct = base.profit_target_pct_late
        target_late_from = base.profit_target_late_from
        protect_late_from = base.profit_protect_late_from

    if OPT_TUNE_DAILY_LOSS:
        daily_loss = float(trial.suggest_int(
            f"{underlying}_max_daily_loss_rupees",
            OPT_DAILY_LOSS_MIN,
            OPT_DAILY_LOSS_MAX,
            step=OPT_DAILY_LOSS_STEP,
        ))
    else:
        daily_loss = float(base.max_daily_loss_rupees)

    return UnderlyingProfile(
        profit_target_pct=float(target_pct),
        profit_target_pct_late=float(late_target_pct),
        profit_target_late_from=target_late_from,
        profit_protect_pct=float(protect_pct),
        max_daily_loss_rupees=float(daily_loss),
        profit_protect_late_giveback_pct=float(late_giveback_pct),
        profit_protect_late_from=protect_late_from,
    )


def _params_from_trial(trial, base: Params) -> Params:
    """
    Map one trial to a complete simulator parameter object.

    Searched (defaults): entry/exit time (2), max re-attempts, flat re-entry
    delay, target re-entry delay (3), stop % scale + Rs cap (2), and seven
    settings per underlying (14) = 21 dimensions.
    Frozen at the property-file values unless OPT_TUNE_SWITCHES=1:
    ENTRY_MOMENTUM_GATE, MOMENTUM_LOOKBACK_MIN, PROFIT_TARGET_REENTRY_ENABLED.
    """
    entry_time = _bounded_time_suggestion(
        trial,
        name="entry_time_offset",
        from_ist=OPT_ENTRY_FROM_IST,
        to_ist=OPT_ENTRY_TO_IST,
        step_minutes=OPT_ENTRY_STEP_MIN,
        base_value=base.entry_time,
        enabled=OPT_TUNE_TIMES,
    )
    exit_time = _bounded_time_suggestion(
        trial,
        name="exit_time_offset",
        from_ist=OPT_EXIT_FROM_IST,
        to_ist=OPT_EXIT_TO_IST,
        step_minutes=OPT_EXIT_STEP_MIN,
        base_value=base.exit_time,
        enabled=OPT_TUNE_TIMES,
    )
    if entry_time >= exit_time:
        # Cannot happen with the default windows; protects custom ranges.
        raise ValueError(
            f"Invalid trial: entry {entry_time.strftime('%H:%M')} must precede "
            f"exit {exit_time.strftime('%H:%M')}"
        )

    # ---- On/off strategy switches (frozen by default) ------------------------
    if OPT_TUNE_SWITCHES:
        momentum_enabled = bool(trial.suggest_int("entry_momentum_gate", 0, 1))
        # Conditional: the lookback only matters when the gate is on.
        momentum_lookback = (
            trial.suggest_int(
                "momentum_lookback_min", OPT_MOMENTUM_LOOKBACK_MIN, OPT_MOMENTUM_LOOKBACK_MAX,
            )
            if momentum_enabled else base.momentum_lookback_min
        )
        target_reentry_enabled = bool(trial.suggest_int("profit_target_reentry_enabled", 0, 1))
    else:
        momentum_enabled = base.entry_momentum_gate
        momentum_lookback = base.momentum_lookback_min
        target_reentry_enabled = base.profit_target_reentry_enabled

    # ---- Re-entry -------------------------------------------------------------
    if OPT_TUNE_REENTRY:
        max_reattempts = trial.suggest_int(
            "max_reattempts", OPT_REATTEMPTS_MIN, OPT_REATTEMPTS_MAX,
        )
        # CHANGED: one flat delay instead of base/step/growth-slots.
        reentry_delay = trial.suggest_int(
            "reentry_delay_min", OPT_REENTRY_DELAY_MIN, OPT_REENTRY_DELAY_MAX,
        )
        delay_schedule = [int(reentry_delay)]
        # Conditional: the target re-entry delay only matters when recycling is on.
        target_reentry_delay = (
            trial.suggest_int(
                "profit_target_reentry_delay_min",
                OPT_TARGET_REENTRY_DELAY_MIN,
                OPT_TARGET_REENTRY_DELAY_MAX,
            )
            if target_reentry_enabled else base.profit_target_reentry_delay_min
        )
    else:
        max_reattempts = base.max_reattempts
        delay_schedule = list(base.reentry_delay_by_attempt)
        target_reentry_delay = base.profit_target_reentry_delay_min

    # ---- Stop-loss ------------------------------------------------------------
    if OPT_TUNE_STOPLOSS:
        # CHANGED: one scale on the file's % list (base/step/growth removed).
        stop_scale = _round_param(trial.suggest_float(
            "stop_loss_pct_scale", OPT_STOP_SCALE_MIN, OPT_STOP_SCALE_MAX,
        ))
        stop_schedule = _scaled_stop_schedule(base.loss_limit_pct_by_attempt, stop_scale)
        stop_cap = trial.suggest_int(
            "max_stop_loss_rupees", OPT_STOP_CAP_MIN, OPT_STOP_CAP_MAX, step=OPT_STOP_CAP_STEP,
        )
    else:
        stop_schedule = list(base.loss_limit_pct_by_attempt)
        stop_cap = base.max_loss_limit_cap_rupees

    profiles = {
        underlying: _profile_from_trial(
            trial,
            underlying=underlying,
            base=base.profile_for(underlying),
        )
        for underlying in sorted(base.profiles)
    }

    return Params(
        entry_time=entry_time,
        exit_time=exit_time,
        loss_limit_pct_by_attempt=stop_schedule,
        max_loss_limit_cap_rupees=float(stop_cap),
        max_reattempts=int(max_reattempts),
        reentry_delay_by_attempt=delay_schedule,
        entry_momentum_gate=bool(momentum_enabled),
        momentum_lookback_min=int(momentum_lookback),
        profit_target_reentry_enabled=bool(target_reentry_enabled),
        profit_target_reentry_delay_min=int(target_reentry_delay),
        profiles=profiles,
    )


class _FrozenTrialView:
    """Re-run the same suggestion code against a completed trial's values."""

    def __init__(self, trial) -> None:
        self._params = dict(trial.params)

    def suggest_int(self, name: str, *args, **kwargs) -> int:
        return int(self._params[name])

    def suggest_float(self, name: str, *args, **kwargs) -> float:
        return float(self._params[name])


class _RecordingTrial:
    """
    NEW: records every distribution _params_from_trial() asks for.

    Used to validate the seed against the REAL search space (range AND grid)
    without keeping a second, hand-maintained copy of every bound (the old
    _value_within_search_space table could drift from the actual ranges).
    Returns the seed value when one is given, otherwise the lower bound.
    """

    def __init__(self, values: Dict[str, Any]) -> None:
        self.values = dict(values)
        self.specs: Dict[str, Tuple[str, float, float, Optional[float]]] = {}

    def suggest_int(self, name: str, low: int, high: int, step: int = 1, log: bool = False) -> int:
        self.specs[name] = ("int", low, high, step)
        return int(self.values.get(name, low))

    def suggest_float(self, name: str, low: float, high: float, *, step=None, log: bool = False) -> float:
        self.specs[name] = ("float", low, high, step)
        return float(self.values.get(name, low))


def _normalise_base(base: Params) -> Params:
    """
    The engine never monitors past SESSION_END_IST (15:30 unless configured),
    so EXIT_TIME_IST values after it behave exactly like the session end.
    Using the clamped value lets the seed sit inside the exit-time range.
    """
    if base.exit_time > SESSION_END_IST:
        print(
            f"[SEED] EXIT_TIME_IST {base.exit_time.strftime('%H:%M')} is after the "
            f"session end; the engine treats it as {SESSION_END_IST.strftime('%H:%M')} "
            f"(identical results). Using {SESSION_END_IST.strftime('%H:%M')} for the optimizer baseline.",
            flush=True,
        )
        base.exit_time = SESSION_END_IST
    return base


def _base_trial_values(base: Params) -> Tuple[Dict[str, Any], List[str]]:
    """
    Convert the property-file configuration into this search space's values.

    Returns (seed_values, notes). With the fast-reentry file every value is
    representable exactly; notes list anything that is not (for example a
    non-flat REENTRY_DELAY_BY_ATTEMPT list).
    """
    values: Dict[str, Any] = {}
    notes: List[str] = []

    def time_offset(value: dtime, start_text: str) -> int:
        return _time_to_minutes(value) - _time_to_minutes(parse_hhmm(start_text))

    if OPT_TUNE_TIMES:
        values["entry_time_offset"] = time_offset(base.entry_time, OPT_ENTRY_FROM_IST)
        values["exit_time_offset"] = time_offset(base.exit_time, OPT_EXIT_FROM_IST)

    if OPT_TUNE_SWITCHES:
        values["entry_momentum_gate"] = 1 if base.entry_momentum_gate else 0
        if base.entry_momentum_gate:
            values["momentum_lookback_min"] = int(base.momentum_lookback_min)
        values["profit_target_reentry_enabled"] = 1 if base.profit_target_reentry_enabled else 0

    if OPT_TUNE_REENTRY:
        values["max_reattempts"] = int(base.max_reattempts)
        delays = list(base.reentry_delay_by_attempt) or [0]
        if len(set(delays)) > 1:
            notes.append(
                f"REENTRY_DELAY_BY_ATTEMPT={delays} is not flat; the seed uses "
                f"{delays[0]} for every re-entry."
            )
        values["reentry_delay_min"] = int(delays[0])
        if base.profit_target_reentry_enabled:
            values["profit_target_reentry_delay_min"] = int(base.profit_target_reentry_delay_min)

    if OPT_TUNE_STOPLOSS:
        values["stop_loss_pct_scale"] = 1.0
        values["max_stop_loss_rupees"] = int(round(base.max_loss_limit_cap_rupees))

    for underlying in sorted(base.profiles):
        profile = base.profile_for(underlying)
        if OPT_TUNE_PROFILES:
            values[f"{underlying}_profit_protect_pct"] = float(profile.profit_protect_pct)
            values[f"{underlying}_late_giveback_pct"] = float(profile.profit_protect_late_giveback_pct)
            values[f"{underlying}_profit_target_pct"] = float(profile.profit_target_pct)
            values[f"{underlying}_late_profit_target_pct"] = float(profile.profit_target_pct_late)
            values[f"{underlying}_target_late_from_offset"] = time_offset(
                profile.profit_target_late_from, OPT_LATE_FROM_IST,
            )
            values[f"{underlying}_protect_late_from_offset"] = time_offset(
                profile.profit_protect_late_from, OPT_LATE_FROM_IST,
            )
        if OPT_TUNE_DAILY_LOSS:
            values[f"{underlying}_max_daily_loss_rupees"] = int(round(profile.max_daily_loss_rupees))

    return values, notes


def _validate_seed(base: Params, values: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
    """
    Check every seed value against the distribution the search actually uses
    (range AND step grid).

    CHANGED: a value outside your OPT_* limits is no longer dropped (which
    made Optuna fill it with a RANDOM value). It is CLAMPED to the nearest
    allowed value, so the seed is "the property file, moved inside your
    limits". Every adjustment is reported. A non-empty problem list also means
    the property-file baseline itself lies outside the limits, so it is kept
    as a benchmark only and can never be selected as the winner.
    """
    recorder = _RecordingTrial(values)
    _params_from_trial(recorder, base)
    kept: Dict[str, Any] = {}
    problems: List[str] = []
    for name, value in values.items():
        spec = recorder.specs.get(name)
        if spec is None:
            problems.append(f"{name}={value}: not part of the current search space (dropped)")
            continue
        kind, low, high, step = spec
        if kind == "int":
            step_i = int(step) if step else 1
            clamped = min(max(int(value), int(low)), int(high))
            snapped = int(low) + int(round((clamped - int(low)) / step_i)) * step_i
            if snapped > int(high):
                snapped -= step_i
            new_value: Any = int(snapped)
        else:
            new_value = min(max(float(value), float(low)), float(high))
        kept[name] = new_value
        if new_value != value:
            problems.append(
                f"{name}: {value} -> {new_value} "
                f"(allowed range [{low}, {high}]"
                + (f", step {step}" if kind == "int" and step and int(step) > 1 else "")
                + ")"
            )
    return kept, problems


def _param_differences(a: Params, b: Params) -> List[str]:
    """Human-readable list of fields where two parameter sets differ."""
    diffs: List[str] = []
    for name in (
        "entry_time", "exit_time", "loss_limit_pct_by_attempt", "max_loss_limit_cap_rupees",
        "max_reattempts", "reentry_delay_by_attempt", "entry_momentum_gate",
        "momentum_lookback_min", "profit_target_reentry_enabled",
        "profit_target_reentry_delay_min",
    ):
        va, vb = getattr(a, name), getattr(b, name)
        if va != vb:
            diffs.append(f"{name}: {va} vs {vb}")
    for und in sorted(set(a.profiles) | set(b.profiles)):
        pa, pb = a.profiles.get(und), b.profiles.get(und)
        if pa != pb:
            diffs.append(f"{und} profile: {pa} vs {pb}")
    return diffs


# =============================================================================
# OUTPUT HELPERS
# =============================================================================
def _inr(value: Any) -> str:
    """Indian-grouped rupee formatting used in console progress."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        number = 0.0
    if not math.isfinite(number):
        return "n/a"

    sign = "-" if number < 0 else ""
    integer = str(int(round(abs(number))))
    if len(integer) <= 3:
        grouped = integer
    else:
        tail = integer[-3:]
        head = integer[:-3]
        pairs: List[str] = []
        while len(head) > 2:
            pairs.append(head[-2:])
            head = head[:-2]
        if head:
            pairs.append(head)
        grouped = ",".join(reversed(pairs)) + "," + tail
    return f"{sign}Rs {grouped}"


def params_to_properties(params: Params, *, include_run_control: bool = True) -> str:
    """
    Render a complete, unambiguous property block for a parameter set.

    The block loads directly into atm_straddle_expiry_day_V3OPT.py. Floats are
    written with 6 decimals, which is exactly the precision used inside the
    optimizer, so the backtest reproduces the optimizer's number.
    """
    rows: List[str] = [
        "# Generated by atm_straddle_expiry_day_V3OPT_OPTUNA__optimizer_DTE0.py (fast-reentry edition)",
        "# Verify with atm_straddle_expiry_day_V3OPT.py (or RUN_MODE=backtest) before any live use.",
    ]
    if include_run_control:
        rows.append("RUN_MODE=backtest")

    rows.extend([
        f"PICKLES_DIR={PICKLES_DIR}",
        f"ENTRY_TIME_IST={params.entry_time.strftime('%H:%M')}",
        f"EXIT_TIME_IST={params.exit_time.strftime('%H:%M')}",
        # Read by the strategy script once it has the SESSION_END_IST change;
        # an older script ignores this key.
        f"SESSION_END_IST={SESSION_END_IST.strftime('%H:%M')}",
        f"ENTRY_MOMENTUM_GATE={1 if params.entry_momentum_gate else 0}",
        f"MOMENTUM_LOOKBACK_MIN={params.momentum_lookback_min}",
        "ALLOWED_DTE=" + ", ".join(str(value) for value in ALLOWED_DTE),
        "LOSS_LIMIT_RUPEES_BY_ATTEMPT="
        + ", ".join(f"{value:.6f}" for value in params.loss_limit_pct_by_attempt),
        f"MAX_LOSS_LIMIT_RUPEES_BY_ATTEMPT={int(round(params.max_loss_limit_cap_rupees))}",
        f"MAX_REATTEMPTS={params.max_reattempts}",
        "REENTRY_DELAY_BY_ATTEMPT="
        + ", ".join(str(value) for value in params.reentry_delay_by_attempt),
        f"PROFIT_TARGET_REENTRY_ENABLED={1 if params.profit_target_reentry_enabled else 0}",
        f"PROFIT_TARGET_REENTRY_DELAY={params.profit_target_reentry_delay_min}",
    ])

    for underlying in sorted(params.profiles):
        profile = params.profile_for(underlying)
        rows.extend([
            "",
            f"# ---- {underlying} optimized profile ----",
            f"{underlying}_PROFIT_PROTECT_TRIGGER_RUPEES={profile.profit_protect_pct:.6f}",
            f"{underlying}_PROFIT_PROTECT_LATE_GIVEBACK="
            f"{profile.profit_protect_late_giveback_pct:.6f}",
            f"{underlying}_PROFIT_PROTECT_LATE_FROM_IST="
            f"{profile.profit_protect_late_from.strftime('%H:%M')}",
            f"{underlying}_PROFIT_TARGET_PCT={profile.profit_target_pct:.6f}",
            f"{underlying}_PROFIT_TARGET_PCT_LATE={profile.profit_target_pct_late:.6f}",
            f"{underlying}_PROFIT_TARGET_LATE_FROM_IST="
            f"{profile.profit_target_late_from.strftime('%H:%M')}",
            f"{underlying}_MAX_DAILY_LOSS_RUPEES="
            f"{int(round(profile.max_daily_loss_rupees))}",
        ])

    rows.extend([
        "",
        f"LOOKBACK_MONTHS={LOOKBACK_MONTHS_RAW}",
        f"FAIL_ON_PICKLE_ERROR={1 if FAIL_ON_PICKLE_ERROR else 0}",
    ])
    return "\n".join(rows) + "\n"


def _properties_json(params: Params) -> str:
    """The property block as a compact JSON object (for CSV logs)."""
    return json.dumps(
        {
            line.split("=", 1)[0]: line.split("=", 1)[1]
            for line in params_to_properties(params, include_run_control=False).splitlines()
            if line and not line.startswith("#") and "=" in line
        },
        sort_keys=True,
    )


_SCALAR_METRICS = (
    "n_days", "n_months", "n_trades", "cap_hit_days", "profitable_day_ratio",
    "profitable_month_ratio", "total_pnl", "mean_month", "median_month",
    "worst_day", "worst_month",
)


def _trial_record(
    trial,
    params: Params,
    metrics: Dict[str, Any],
    run_index: int,
    phase: str,
    elapsed_seconds: float,
) -> Dict[str, Any]:
    """Compact durable record written after every completed trial."""
    return {
        "run_index": int(run_index),
        "phase": phase,
        "trial_number": int(trial.number),
        "state": str(getattr(trial, "state", "")),
        "objective_score": trial.value,
        "net_pnl": round(float(metrics.get("total_pnl", 0.0)), 2),
        "n_trades": int(metrics.get("n_trades", 0)),
        "cap_hit_days": int(metrics.get("cap_hit_days", 0)),
        "mean_month": round(float(metrics.get("mean_month", 0.0)), 2),
        "median_month": round(float(metrics.get("median_month", 0.0)), 2),
        "worst_month": round(float(metrics.get("worst_month", 0.0)), 2),
        "worst_day": round(float(metrics.get("worst_day", 0.0)), 2),
        "profitable_month_ratio": round(float(metrics.get("profitable_month_ratio", 0.0)), 6),
        "profitable_day_ratio": round(float(metrics.get("profitable_day_ratio", 0.0)), 6),
        "n_months": int(metrics.get("n_months", 0)),
        "n_days": int(metrics.get("n_days", 0)),
        "entry_time": params.entry_time.strftime("%H:%M"),
        "exit_time": params.exit_time.strftime("%H:%M"),
        "max_reattempts": params.max_reattempts,
        "reentry_delay": ";".join(str(v) for v in params.reentry_delay_by_attempt),
        "target_reentry_delay": params.profit_target_reentry_delay_min,
        "stop_cap": int(round(params.max_loss_limit_cap_rupees)),
        "stop_schedule": ";".join(f"{v:.6f}" for v in params.loss_limit_pct_by_attempt),
        "params_json": json.dumps(trial.params, sort_keys=True),
        "properties_json": _properties_json(params),
        "elapsed_seconds": round(float(elapsed_seconds), 1),
    }


# =============================================================================
# OPTUNA PLUMBING
# =============================================================================
def _make_storage(optuna) -> Tuple[Any, str]:
    """
    Build the Optuna storage selected by OPT_STORAGE.

    journal (default): append-only log + JournalFileOpenLock, which works on
    Windows without symlink privileges. Supports Optuna 4.x/5.x
    (optuna.storages.journal.JournalFileBackend) and 3.1-3.6
    (optuna.storages.JournalFileStorage).
    """
    if OPT_STORAGE == "memory":
        return None, "in-memory (not resumable)"

    if OPT_STORAGE == "sqlite":
        db_path = os.path.abspath(os.path.join(OPT_OUTPUT_DIR, f"{OPT_STUDY_NAME}.db"))
        return "sqlite:///" + db_path.replace("\\", "/"), db_path

    if OPT_STORAGE == "journal":
        log_path = os.path.abspath(os.path.join(OPT_OUTPUT_DIR, f"{OPT_STUDY_NAME}.journal.log"))
        try:
            from optuna.storages.journal import JournalFileBackend, JournalFileOpenLock
            backend = JournalFileBackend(log_path, lock_obj=JournalFileOpenLock(log_path))
        except ImportError:
            from optuna.storages import JournalFileStorage, JournalFileOpenLock  # type: ignore
            backend = JournalFileStorage(log_path, lock_obj=JournalFileOpenLock(log_path))
        return optuna.storages.JournalStorage(backend), log_path

    raise ValueError(f"Unknown OPT_STORAGE={OPT_STORAGE!r}; use journal, sqlite or memory.")


def _cmaes_available() -> bool:
    try:
        import cmaes  # noqa: F401
        return True
    except ImportError:
        return False


def _make_sampler(optuna, kind: str, seed: int, source_trials: Optional[List[Any]] = None):
    """
    Build a sampler.

    tpe            : multivariate + grouped TPE. Models interactions between
                     parameters (target vs protect vs stop cap vs delays) and
                     handles conditional parameters through grouping.
    tpe_univariate : the previous behaviour.
    cmaes          : CMA-ES (with_margin keeps integer parameters moving).
                     With `source_trials` it is warm-started (WS-CMA-ES): its
                     initial mean and covariance are estimated from the best
                     of those trials. (CmaEsSampler's x0/sigma0 arguments are
                     deprecated from Optuna 4.9, so they are not used.)
    random         : sanity benchmark only.
    """
    if kind == "tpe":
        return optuna.samplers.TPESampler(
            seed=seed,
            n_startup_trials=max(1, OPT_STARTUP_TRIALS),
            multivariate=True,
            group=True,
            constant_liar=True,
        )
    if kind == "tpe_univariate":
        return optuna.samplers.TPESampler(seed=seed, n_startup_trials=max(1, OPT_STARTUP_TRIALS))
    if kind == "cmaes":
        if not _cmaes_available():
            raise RuntimeError("OPT_SAMPLER=cmaes needs the 'cmaes' package: pip install cmaes")
        return optuna.samplers.CmaEsSampler(
            seed=seed,
            with_margin=True,
            warn_independent_sampling=False,
            source_trials=source_trials,
        )
    if kind == "random":
        return optuna.samplers.RandomSampler(seed=seed)
    raise ValueError(f"Unknown OPT_SAMPLER={kind!r}; use tpe, tpe_univariate, cmaes or random.")


def _search_signature(base: Params) -> str:
    """
    Everything that defines the optimisation problem. A resumed study must
    match it exactly; otherwise its TPE history would describe a different
    problem. The baseline config is part of it because frozen values and the
    stop-loss scale are taken from it.
    """
    return json.dumps(
        {
            "version": "V3OPT_FASTREENTRY_1",
            "baseline": params_to_properties(base, include_run_control=False),
            "tune": [OPT_TUNE_TIMES, OPT_TUNE_STOPLOSS, OPT_TUNE_REENTRY,
                     OPT_TUNE_PROFILES, OPT_TUNE_DAILY_LOSS, OPT_TUNE_SWITCHES],
            "entry_range": [OPT_ENTRY_FROM_IST, OPT_ENTRY_TO_IST, OPT_ENTRY_STEP_MIN],
            "exit_range": [OPT_EXIT_FROM_IST, OPT_EXIT_TO_IST, OPT_EXIT_STEP_MIN],
            "stop_range": [OPT_STOP_SCALE_MIN, OPT_STOP_SCALE_MAX, OPT_STOP_CAP_MIN,
                           OPT_STOP_CAP_MAX, OPT_STOP_CAP_STEP],
            "reentry_range": [OPT_REATTEMPTS_MIN, OPT_REATTEMPTS_MAX,
                              OPT_REENTRY_DELAY_MIN, OPT_REENTRY_DELAY_MAX,
                              OPT_TARGET_REENTRY_DELAY_MIN, OPT_TARGET_REENTRY_DELAY_MAX,
                              OPT_MOMENTUM_LOOKBACK_MIN, OPT_MOMENTUM_LOOKBACK_MAX],
            "profile_range": [OPT_PROTECT_MIN, OPT_PROTECT_MAX,
                              OPT_LATE_GIVEBACK_MIN, OPT_LATE_GIVEBACK_MAX,
                              OPT_TARGET_MIN, OPT_TARGET_MAX,
                              OPT_LATE_TARGET_MIN, OPT_LATE_TARGET_MAX,
                              OPT_LATE_FROM_IST, OPT_LATE_TO_IST, OPT_LATE_TIME_STEP_MIN,
                              OPT_DAILY_LOSS_MIN, OPT_DAILY_LOSS_MAX, OPT_DAILY_LOSS_STEP],
            "objective": [OPT_CV_FOLDS, OPT_CV_PENALTY, OPT_MIN_DAYS, OPT_MIN_MONTHS,
                          OPT_HOLDOUT_FROM_RAW, OPT_SLIPPAGE_PTS],
        },
        sort_keys=True,
    )



def _data_fingerprint(days: List[FastDay]) -> str:
    """Identifies the in-sample day list (a resumed study must use the same days)."""
    text = ";".join(f"{fd.und}:{fd.dy.isoformat()}" for fd in days)
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:16]


def _top_k_report(
    study,
    base: Params,
    in_set: DaySet,
    out_set: DaySet,
    cv_folds: int,
    csv_path: str,
) -> List[Dict[str, Any]]:
    """
    NEW: re-score the baseline and the best OPT_REPORT_TOP_K distinct trials on
      * the in-sample days with the objective's slippage (what was optimized);
      * the in-sample days with the STRESS slippage;
      * the holdout days (if OPT_HOLDOUT_FROM is set), with both slippages.
    The holdout numbers are computed only here, after the search, so they can
    never influence which trial the search prefers.
    """
    import optuna

    complete = [
        t for t in study.trials
        if t.state == optuna.trial.TrialState.COMPLETE
        and t.value is not None
        and t.value > _OPT_DISQUALIFY
    ]
    complete.sort(key=lambda t: t.value, reverse=True)
    chosen = []
    seen: set = set()
    for trial in complete:
        key = json.dumps(trial.params, sort_keys=True)
        if key in seen:
            continue
        seen.add(key)
        chosen.append(trial)
        if len(chosen) >= OPT_REPORT_TOP_K:
            break

    def score(rank: Any, trial_number: Any, params: Params) -> Dict[str, Any]:
        ins = evaluate_fast(params, in_set, OPT_SLIPPAGE_PTS, cv_folds)
        ins_stress = evaluate_fast(params, in_set, OPT_STRESS_SLIPPAGE_PTS, cv_folds)
        row: Dict[str, Any] = {
            "rank": rank,
            "trial_number": trial_number,
            "objective": round(ins["objective"], 2),
            "in_sample_net": round(ins["total_pnl"], 2),
            "in_sample_net_stress": round(ins_stress["total_pnl"], 2),
            "in_sample_worst_day": round(ins["worst_day"], 2),
            "in_sample_worst_month": round(ins["worst_month"], 2),
            "in_sample_trades": ins["n_trades"],
            "in_sample_cap_hit_days": ins["cap_hit_days"],
        }
        if out_set.days:
            oos = evaluate_fast(params, out_set, OPT_SLIPPAGE_PTS, 1)
            oos_stress = evaluate_fast(params, out_set, OPT_STRESS_SLIPPAGE_PTS, 1)
            row.update({
                "holdout_net": round(oos["total_pnl"], 2),
                "holdout_net_stress": round(oos_stress["total_pnl"], 2),
                "holdout_worst_day": round(oos["worst_day"], 2),
                "holdout_profitable_month_ratio": round(oos["profitable_month_ratio"], 4),
                "holdout_trades": oos["n_trades"],
            })
        row["properties_json"] = _properties_json(params)
        return row

    rows = [score("BASELINE", "", base)]
    for rank, trial in enumerate(chosen, start=1):
        rows.append(score(rank, trial.number, _params_from_trial(_FrozenTrialView(trial), base)))

    pd.DataFrame(rows).to_csv(csv_path, index=False)

    stress_text = ", ".join(f"{u} {p:g} pt" for u, p in OPT_STRESS_SLIPPAGE_PTS.items())
    print(
        f"\n---- TOP {len(chosen)} TRIALS vs BASELINE "
        f"(stress = {stress_text} per leg per order) ----",
        flush=True,
    )
    header = f"{'rank':>8} {'trial':>6} {'in-sample':>16} {'in-s stress':>16}"
    if out_set.days:
        header += f" {'holdout':>16} {'holdout stress':>16}"
    header += f" {'worst day':>14} {'cap days':>8}"
    print(header, flush=True)
    for row in rows[: min(len(rows), 11)]:
        line = (
            f"{str(row['rank']):>8} {str(row['trial_number']):>6} "
            f"{_inr(row['in_sample_net']):>16} {_inr(row['in_sample_net_stress']):>16}"
        )
        if out_set.days:
            line += f" {_inr(row['holdout_net']):>16} {_inr(row['holdout_net_stress']):>16}"
        line += f" {_inr(row['in_sample_worst_day']):>14} {row['in_sample_cap_hit_days']:>8}"
        print(line, flush=True)
    print(f"[OPT] top-trials table: {csv_path}", flush=True)
    return rows


def optimize(
    fast_days: List[FastDay],
    min_expiry_map: Dict[Tuple[str, date], date],
    parse_skips: List[Dict[str, Any]],
    *,
    n_trials: int,
    cv_folds: int,
    seed: int,
    progress_every: int,
):
    """
    Search -> verify -> report.

    1. split days into in-sample / holdout;
    2. build the exact baseline and an exact seed; report anything inexact;
    3. open (or resume) the study and verify its signature;
    4. parity-check the fast engine against the reference engine;
    5. score the baseline, then run the main sampler and the optional
       CMA-ES refinement;
    6. pick the winner (baseline kept if nothing beats it in-sample);
    7. re-score the top trials (holdout + stress slippage);
    8. re-simulate the winner with the reference engine, parity-check it, and
       write the .properties file and trade-level CSVs.
    """
    import csv
    import datetime as dt

    try:
        import optuna
    except ImportError as exc:
        raise RuntimeError(
            "Optuna is required. Install it in the active interpreter with: "
            "pip install optuna"
        ) from exc

    warnings.filterwarnings("ignore", category=optuna.exceptions.ExperimentalWarning)
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    os.makedirs(OPT_OUTPUT_DIR, exist_ok=True)

    run_stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    prefix = os.path.join(OPT_OUTPUT_DIR, f"{OPT_STUDY_NAME}_{run_stamp}")

    # ---- 1. In-sample / holdout split -----------------------------------------
    if OPT_HOLDOUT_FROM is None:
        in_days, out_days = list(fast_days), []
        print(
            "[SPLIT] no holdout (OPT_HOLDOUT_FROM blank): every day is in-sample, "
            "so the winner's P&L below is an IN-SAMPLE figure.",
            flush=True,
        )
    else:
        in_days = [fd for fd in fast_days if fd.dy < OPT_HOLDOUT_FROM]
        out_days = [fd for fd in fast_days if fd.dy >= OPT_HOLDOUT_FROM]
        print(
            f"[SPLIT] in-sample {len(in_days)} days "
            f"({in_days[0].dy if in_days else '-'} .. {in_days[-1].dy if in_days else '-'}), "
            f"holdout {len(out_days)} days from {OPT_HOLDOUT_FROM}",
            flush=True,
        )
    # A holdout date outside the loaded data leaves one side empty. Stop with a
    # concrete date that works for THIS data (about 65% in-sample) instead of
    # just saying the date is wrong.
    split_at = min(len(fast_days) - 1, max(1, int(round(0.65 * len(fast_days)))))
    suggested = fast_days[split_at].dy
    suggestion = (
        f"The loaded data covers {fast_days[0].dy} .. {fast_days[-1].dy} "
        f"({len(fast_days)} traded days). For example OPT_HOLDOUT_FROM={suggested} gives "
        f"{split_at} in-sample and {len(fast_days) - split_at} holdout days; "
        "or leave OPT_HOLDOUT_FROM blank for no holdout."
    )
    if not in_days:
        raise RuntimeError(
            f"No in-sample days: OPT_HOLDOUT_FROM={OPT_HOLDOUT_FROM} is on or before the "
            f"first traded day. {suggestion}"
        )
    if OPT_HOLDOUT_FROM is not None and not out_days:
        print(
            f"[SPLIT WARNING] OPT_HOLDOUT_FROM={OPT_HOLDOUT_FROM} is after the last traded "
            f"day, so there is NO holdout this run. {suggestion}",
            flush=True,
        )
    in_set = DaySet.of(in_days)
    out_set = DaySet.of(out_days)
    _configure_activity_minima(in_days)

    # ---- 2. Exact baseline and exact seed ----------------------------------------
    base = _normalise_base(default_params())
    seed_values, seed_notes = _base_trial_values(base)
    seed_values, seed_problems = _validate_seed(base, seed_values)
    seed_params = _params_from_trial(_RecordingTrial(seed_values), base)
    seed_diffs = _param_differences(seed_params, base)
    for note in seed_notes:
        print(f"[SEED NOTE] {note}", flush=True)
    for problem in seed_problems:
        print(f"[SEED ADJUSTED] {problem}", flush=True)
    # The baseline may win only if it lies inside your OPT_* limits.
    baseline_inside_limits = not seed_problems
    if not baseline_inside_limits:
        print(
            "[SEED] the property file lies outside your OPT_* limits: the seed above is "
            "the nearest allowed configuration, and the property-file baseline is kept "
            "as a BENCHMARK ONLY (it cannot be selected as the winner).",
            flush=True,
        )
    if seed_diffs:
        print("[SEED] the seed trial differs from the property file:", flush=True)
        for diff in seed_diffs:
            print(f"        {diff}", flush=True)
    else:
        print("[SEED] seed trial reproduces the property file exactly.", flush=True)

    # ---- 3. Study (signature checked BEFORE the slower parity checks) ----------
    storage, storage_label = _make_storage(optuna)
    print(f"[OPT] storage: {storage_label}", flush=True)
    main_kind = OPT_SAMPLER
    study = optuna.create_study(
        direction="maximize",
        sampler=_make_sampler(optuna, main_kind, seed),
        study_name=OPT_STUDY_NAME,
        storage=storage,
        load_if_exists=storage is not None,
    )

    signature = _search_signature(base) + "|data=" + _data_fingerprint(in_days)
    existing_signature = study.user_attrs.get("search_signature")
    if existing_signature is not None and existing_signature != signature:
        raise RuntimeError(
            "The saved study was created with a different search space, objective, "
            "baseline config or in-sample day list. Use a new OPT_STUDY_NAME "
            "(recommended) or restore the original settings before resuming it."
        )
    if existing_signature is None:
        study.set_user_attr("search_signature", signature)

    if len(study.trials) == 0 and seed_values:
        study.enqueue_trial(seed_values)
        print("[OPT] exact seed (= property file) queued as the first trial.", flush=True)
    elif len(study.trials) > 0:
        print(f"[OPT] resuming study with {len(study.trials)} existing trial(s).", flush=True)

    # ---- 4. Parity: fast engine must equal the reference engine ------------------
    if OPT_PARITY_CHECK:
        print("[PARITY] checking fast engine against the reference engine ...", flush=True)
        _assert_parity("exact baseline, all days", base, fast_days, min_expiry_map)
        if OPT_PARITY_RANDOM_TRIALS > 0:
            probe = optuna.create_study(
                direction="maximize",
                sampler=optuna.samplers.RandomSampler(seed=seed + 1),
            )
            sample_days = _even_sample(fast_days, OPT_PARITY_MAX_DAYS)
            for index in range(OPT_PARITY_RANDOM_TRIALS):
                random_params = _params_from_trial(probe.ask(), base)
                _assert_parity(
                    f"random parameter set {index + 1}", random_params,
                    sample_days, min_expiry_map,
                )
    else:
        print("[PARITY] skipped (OPT_PARITY_CHECK=0).", flush=True)

    # ---- 5. Baseline score --------------------------------------------------------
    baseline_eval = evaluate_fast(base, in_set, OPT_SLIPPAGE_PTS, cv_folds)
    slip_text = ", ".join(f"{u} {p:g} pt" for u, p in OPT_SLIPPAGE_PTS.items())
    print(
        f"[OPT] exact property-file baseline (in-sample, slippage {slip_text}): "
        f"net={_inr(baseline_eval['total_pnl'])}, objective={_inr(baseline_eval['objective'])}, "
        f"trades={baseline_eval['n_trades']}, days where MAX_REATTEMPTS stopped a "
        f"re-entry={baseline_eval['cap_hit_days']}",
        flush=True,
    )
    if baseline_eval["objective"] <= _OPT_DISQUALIFY:
        # NEW: say it loudly. If even the baseline fails the guard, most trials
        # will too and the search cannot find anything meaningful.
        print(
            f"[GUARD WARNING] the baseline itself is DISQUALIFIED: it traded on "
            f"{baseline_eval['n_days']} days / {baseline_eval['n_months']} months, below the "
            f"guard {_activity_minima()}. Check for missing data days, or lower "
            "OPT_MIN_DAYS / OPT_MIN_MONTHS.",
            flush=True,
        )

    # ---- 6. Objective, logging, progress -------------------------------------------
    trial_csv_path = prefix + "_trials.csv"
    csv_fields = [
        "run_index", "phase", "trial_number", "state", "objective_score", "net_pnl",
        "n_trades", "cap_hit_days", "mean_month", "median_month", "worst_month",
        "worst_day", "profitable_month_ratio", "profitable_day_ratio", "n_months",
        "n_days", "entry_time", "exit_time", "max_reattempts", "reentry_delay",
        "target_reentry_delay", "stop_cap", "stop_schedule", "params_json",
        "properties_json", "elapsed_seconds",
    ]
    csv_file = open(trial_csv_path, "w", newline="", encoding="utf-8")
    csv_writer = csv.DictWriter(csv_file, fieldnames=csv_fields)
    csv_writer.writeheader()
    csv_file.flush()

    refine_planned = OPT_REFINE_TRIALS if main_kind != "cmaes" else 0
    total_planned = n_trials + refine_planned
    try:
        best_so_far: Optional[float] = study.best_value
    except Exception:
        best_so_far = None
    state: Dict[str, Any] = {"count": 0, "phase": main_kind, "best": best_so_far}
    start_time = time.time()

    def objective(trial):
        params = _params_from_trial(trial, base)
        metrics = evaluate_fast(params, in_set, OPT_SLIPPAGE_PTS, cv_folds)
        # CHANGED: ONE user attribute per trial (was eleven separate writes).
        trial.set_user_attr(
            "metrics",
            {**{key: metrics[key] for key in _SCALAR_METRICS}, "monthly": metrics["monthly"]},
        )
        return metrics["objective"]

    def progress_callback(study_obj, trial):
        state["count"] += 1
        run_index = state["count"]
        elapsed = time.time() - start_time
        metrics = trial.user_attrs.get("metrics", {})

        try:
            params = _params_from_trial(_FrozenTrialView(trial), base)
            csv_writer.writerow(
                _trial_record(trial, params, metrics, run_index, state["phase"], elapsed)
            )
            csv_file.flush()
        except Exception as exc:
            print(f"[OPT WARN] failed to persist trial {trial.number}: {exc}", flush=True)

        value = trial.value
        is_new_best = value is not None and (state["best"] is None or value > state["best"])
        if is_new_best:
            state["best"] = value

        if run_index % progress_every == 0 or is_new_best or run_index == total_planned:
            rate = run_index / elapsed if elapsed > 0 else 0.0
            print(
                f"[TRIAL {run_index:>5}/{total_planned}] {state['phase']:<8} #{trial.number:<5} "
                f"net={_inr(metrics.get('total_pnl', 0.0))} "
                f"trades={metrics.get('n_trades', 0)} "
                f"cap_days={metrics.get('cap_hit_days', 0)} "
                f"prof_month={float(metrics.get('profitable_month_ratio', 0.0)) * 100:5.1f}% "
                f"worst_day={_inr(metrics.get('worst_day', 0.0))} "
                f"| best={_inr(state['best'])} ({rate:.1f} trials/s)"
                + ("  <<< NEW BEST" if is_new_best else ""),
                flush=True,
            )
        if is_new_best:
            monthly = metrics.get("monthly", {})
            if monthly:
                cells = [f"{month}:{_inr(val)}" for month, val in monthly.items()]
                for start in range(0, len(cells), 4):
                    print("       " + "   ".join(cells[start:start + 4]), flush=True)

    # ---- 7. Main phase + optional CMA-ES refinement ------------------------------
    try:
        print(
            f"[OPT] main phase: {n_trials} trial(s) with sampler '{main_kind}', "
            f"in-sample days={len(in_days)}, cv_folds={cv_folds}",
            flush=True,
        )
        print(f"[OPT] trial log: {trial_csv_path}", flush=True)
        study.optimize(
            objective, n_trials=n_trials, callbacks=[progress_callback], show_progress_bar=False,
        )

        if refine_planned > 0:
            if not _cmaes_available():
                print(
                    "[OPT] CMA-ES refinement skipped: 'cmaes' is not installed "
                    "(pip install cmaes).",
                    flush=True,
                )
            else:
                # Warm start (WS-CMA-ES): CMA-ES keeps the best ~10% of the
                # source trials to set its starting mean and covariance, so it
                # searches around the TPE winners.
                #
                # CMA-ES only optimises parameters that are present in EVERY
                # trial AND can take more than one value. It accepts a source
                # trial only if that trial's parameter set is exactly this
                # space. Two cases break that:
                #   * conditional parameters (OPT_TUNE_SWITCHES=1): trials that
                #     carry an extra parameter are skipped;
                #   * pinned parameters (an OPT_*_MIN equal to its _MAX): they
                #     are stripped from copies of the source trials (CMA-ES
                #     leaves them out of its space; the study still records them).
                # Fall back to a cold start when fewer than 10 usable trials
                # remain (10% of fewer than 10 trials would select nobody).
                complete = [
                    t for t in study.trials if t.state == optuna.trial.TrialState.COMPLETE
                ]

                def _free_params(trial) -> set:
                    return {
                        name for name, dist in trial.distributions.items() if not dist.single()
                    }

                common = (
                    set.intersection(*(_free_params(t) for t in complete)) if complete else set()
                )
                warm = [
                    optuna.trial.create_trial(
                        params={name: t.params[name] for name in common},
                        distributions={name: t.distributions[name] for name in common},
                        value=t.value,
                    )
                    for t in complete
                    if t.value is not None
                    and t.value > _OPT_DISQUALIFY
                    and _free_params(t) == common
                ]
                if len(warm) >= 10:
                    start_text = f"warm-started from the best of {len(warm)} completed trial(s)"
                else:
                    start_text = (
                        f"cold start (only {len(warm)} trial(s) share the full search "
                        "space; at least 10 are needed for a warm start)"
                    )
                    warm = []
                print(
                    f"[OPT] refinement phase: {refine_planned} CMA-ES trial(s), {start_text}",
                    flush=True,
                )
                study.sampler = _make_sampler(
                    optuna, "cmaes", seed + 7, source_trials=warm or None,
                )
                state["phase"] = "cmaes"
                study.optimize(
                    objective, n_trials=refine_planned, callbacks=[progress_callback],
                    show_progress_bar=False,
                )
    finally:
        csv_file.close()
        try:
            study.trials_dataframe().to_csv(prefix + "_optuna_full.csv", index=False)
            print(f"[OPT] full Optuna table: {prefix}_optuna_full.csv", flush=True)
        except Exception as exc:
            print(f"[OPT WARN] full Optuna table not written: {exc}", flush=True)

    print(
        f"[OPT] search finished: {state['count']} trial(s) in "
        f"{time.time() - start_time:.0f}s",
        flush=True,
    )

    # ---- 8. Winner selection (in-sample objective only) -----------------------------
    try:
        best_trial = study.best_trial
    except ValueError:
        best_trial = None

    selected_params = base
    selected_source = "exact property-file baseline"
    selected_eval = baseline_eval
    if best_trial is not None:
        optuna_params = _params_from_trial(_FrozenTrialView(best_trial), base)
        optuna_eval = evaluate_fast(optuna_params, in_set, OPT_SLIPPAGE_PTS, cv_folds)
        if abs(optuna_eval["objective"] - float(best_trial.value)) > 0.01:
            print(
                f"[OPT WARN] best trial re-scored to {_inr(optuna_eval['objective'])} "
                f"but was stored as {_inr(best_trial.value)}.",
                flush=True,
            )
        # CHANGED: when the baseline violates your limits it is a benchmark
        # only, so the best trial (which respects them) is always selected.
        if (not baseline_inside_limits) or optuna_eval["objective"] > baseline_eval["objective"]:
            selected_params = optuna_params
            selected_source = f"trial {best_trial.number}"
            selected_eval = optuna_eval
            if not baseline_inside_limits and optuna_eval["objective"] <= baseline_eval["objective"]:
                print(
                    "[SELECT] the best trial inside your limits scores below the "
                    "property-file baseline; it is selected anyway because the "
                    "baseline breaks the limits. Compare both in the TOP TRIALS table.",
                    flush=True,
                )
    elif not baseline_inside_limits:
        print(
            "[SELECT WARNING] no completed trial: falling back to the property-file "
            "baseline, which lies OUTSIDE your OPT_* limits.",
            flush=True,
        )

    # ---- 9. Top trials: holdout + stress slippage ------------------------------------
    top_rows = _top_k_report(study, base, in_set, out_set, cv_folds, prefix + "_TOP_TRIALS.csv")

    # ---- 10. Reference engine re-simulation of the winner (all days) ---------------
    print("[REPORT] re-simulating the selected config with the reference engine ...", flush=True)
    _all_df, best_actual_df, sim_skips_df = _reference_run(selected_params, fast_days, min_expiry_map)
    if OPT_PARITY_CHECK:
        _assert_parity(
            "selected config, all days", selected_params, fast_days, min_expiry_map,
            actual_df=best_actual_df,
        )
    if not best_actual_df.empty:
        if OPT_HOLDOUT_FROM is None:
            best_actual_df["sample"] = "IN_SAMPLE"
        else:
            best_actual_df["sample"] = np.where(
                pd.to_datetime(best_actual_df["day"]) >= pd.Timestamp(OPT_HOLDOUT_FROM),
                "HOLDOUT", "IN_SAMPLE",
            )
    ref_all = robustness_metrics(best_actual_df)

    best_trades_path = prefix + "_BEST_actual_trades.csv"
    best_actual_df.to_csv(best_trades_path, index=False)
    best_monthly_path = prefix + "_BEST_monthly_pnl.csv"
    ref_all["monthly"].rename("net_pnl").to_csv(best_monthly_path)

    skip_frames = [frame for frame in (pd.DataFrame(parse_skips), sim_skips_df) if not frame.empty]
    if skip_frames:
        pd.concat(skip_frames, ignore_index=True).to_csv(prefix + "_BEST_skipped.csv", index=False)

    # ---- 11. Properties file --------------------------------------------------------------
    selected_row = next(
        (row for row in top_rows
         if row["properties_json"] == _properties_json(selected_params)),
        None,
    )
    summary_lines = [
        f"# Selected source           : {selected_source}",
        f"# In-sample days            : {len(in_days)}  "
        f"({in_days[0].dy} .. {in_days[-1].dy})",
        f"# In-sample net (objective) : {_inr(selected_eval['total_pnl'])}   "
        f"baseline {_inr(baseline_eval['total_pnl'])}",
    ]
    if selected_row is not None:
        summary_lines.append(
            f"# In-sample net, stress slip: {_inr(selected_row['in_sample_net_stress'])}"
        )
        if out_days:
            label = f"Holdout net ({len(out_days)} days)"
            summary_lines.append(
                f"# {label:<26}: {_inr(selected_row['holdout_net'])}   "
                f"stress {_inr(selected_row['holdout_net_stress'])}   "
                f"baseline {_inr(top_rows[0]['holdout_net'])}"
            )
    summary_lines.append(
        f"# Days MAX_REATTEMPTS bound : {selected_eval['cap_hit_days']}"
    )
    properties_path = prefix + "_BEST.properties"
    with open(properties_path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(summary_lines) + "\n")
        handle.write(params_to_properties(selected_params))

    # ---- 12. Console summary -----------------------------------------------------------
    print("\n================ BEST V3-OPT CONFIG (fast-reentry) ================", flush=True)
    print(f"Selected source        : {selected_source}", flush=True)
    if best_trial is not None:
        print(f"Best trial / objective : {best_trial.number} / {_inr(best_trial.value)}", flush=True)
    print(f"Baseline objective     : {_inr(baseline_eval['objective'])}", flush=True)
    print(f"In-sample net profit   : {_inr(selected_eval['total_pnl'])}", flush=True)
    if selected_row is not None:
        print(f"In-sample, stress slip : {_inr(selected_row['in_sample_net_stress'])}", flush=True)
        baseline_row = top_rows[0]
        if out_days:
            print(
                f"Holdout net profit     : {_inr(selected_row['holdout_net'])} "
                f"(baseline {_inr(baseline_row['holdout_net'])})",
                flush=True,
            )
            print(
                f"Holdout, stress slip   : {_inr(selected_row['holdout_net_stress'])} "
                f"(baseline {_inr(baseline_row['holdout_net_stress'])})",
                flush=True,
            )
            if selected_row["holdout_net"] < baseline_row["holdout_net"]:
                print(
                    "WARNING                : the selected config is WORSE than the "
                    "baseline on the holdout days - treat the in-sample gain as overfit.",
                    flush=True,
                )
    print(f"Mean monthly P/L (all) : {_inr(ref_all['mean_month'])}", flush=True)
    print(f"Worst month (all)      : {_inr(ref_all['worst_month'])}", flush=True)
    print(f"Worst day (all)        : {_inr(ref_all['worst_day'])}", flush=True)
    print(f"Profitable months (all): {ref_all['profitable_month_ratio'] * 100:.1f}%", flush=True)
    print(f"Profitable days (all)  : {ref_all['profitable_day_ratio'] * 100:.1f}%", flush=True)
    print(f"Days MAX_REATTEMPTS bound (in-sample): {selected_eval['cap_hit_days']}", flush=True)
    print(f"Best properties        : {properties_path}", flush=True)
    print(f"Best actual trades     : {best_trades_path}", flush=True)
    print(f"Best monthly P/L       : {best_monthly_path}", flush=True)
    print("\n---- READY-TO-PASTE PROPERTY BLOCK ----", flush=True)
    print(params_to_properties(selected_params), flush=True)

    return study, selected_params


def _report_post_1530_data(fast_days: List[FastDay]) -> None:
    """
    With SESSION_END_IST after 15:30, say how many traded days actually have
    option prices after 15:30. Pickles downloaded before the closing auction
    session (or by a source that stops at 15:30) have none, and then the later
    session end changes nothing: those days are simulated to 15:30 exactly
    as before (day_session_end).
    """
    if SESSION_END_IST <= dtime(15, 30):
        return
    last_1530 = (15 * 60 + 30) - _SESSION_START_MINUTE        # minute offset 375
    with_bars = [fd for fd in fast_days if fd.n - 1 > last_1530]   # see day_session_end()
    first = with_bars[0].dy if with_bars else "-"
    print(
        f"[DATA] {len(with_bars)} of {len(fast_days)} traded days have option prices after 15:30 "
        f"(first: {first}).",
        flush=True,
    )
    if not with_bars:
        print(
            f"[DATA WARNING] SESSION_END_IST={SESSION_END_IST.strftime('%H:%M')} but no day has option "
            "bars after 15:30, so results equal a 15:30 session. Re-download data from 3 Aug 2026 "
            "onward including the 15:30-15:40 bars.",
            flush=True,
        )


def run_optimizer(
    *,
    n_trials: int,
    cv_folds: int,
    max_pickles: Optional[int],
    max_days: Optional[int],
    progress_every: int,
    seed: int,
):
    """End-to-end optimizer entry point: load once (or from cache), verify, search."""
    t0 = time.time()
    fast_days, min_expiry_map, window_start, end_day, parse_skips = load_or_build_fast_data(
        max_pickles, max_days,
    )
    if not fast_days:
        raise RuntimeError("No traded days available for optimization.")
    print(
        f"[DATA] ready in {time.time() - t0:.1f}s: {len(fast_days)} traded days "
        f"({fast_days[0].dy} .. {fast_days[-1].dy}), window {window_start} -> {end_day}",
        flush=True,
    )
    _report_post_1530_data(fast_days)
    return optimize(
        fast_days,
        min_expiry_map,
        parse_skips,
        n_trials=n_trials,
        cv_folds=cv_folds,
        seed=seed,
        progress_every=progress_every,
    )


# =============================================================================
# ENTRYPOINT
# =============================================================================
if __name__ == "__main__":
    if RUN_MODE == "optimize":
        run_optimizer(
            n_trials=OPT_TRIALS,
            cv_folds=OPT_CV_FOLDS,
            max_pickles=SAMPLE_MAX_PICKLES,
            max_days=SAMPLE_MAX_DAYS,
            progress_every=OPT_PROGRESS_EVERY,
            seed=OPT_SEED,
        )
    elif RUN_MODE == "backtest":
        main()
    else:
        raise ValueError(
            f"Unknown RUN_MODE={RUN_MODE!r}; use 'optimize' or 'backtest'."
        )
