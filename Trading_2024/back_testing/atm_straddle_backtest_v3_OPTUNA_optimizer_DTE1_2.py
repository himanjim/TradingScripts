"""
ATM short-straddle v3 backtester + Optuna profit optimizer for DTE-1 / DTE-2.

This file is the optimizer-enabled version of:
    A. atm_straddle_backtest_v3_new_claude.py          (strategy engine, v3)
    B. straddle_config_DTE_1_v3_new_claude.properties   (DTE-1 config)   or
       atm_straddle_DTE2_v3_claude_new.properties       (DTE-2 config)

ONE optimizer serves both DTE configs. Point it at the config you want to
optimize (CONFIG_FILE below, or the STRADDLE_CONFIG environment variable). The
DTE traded comes from ALLOWED_DTE inside that config, and the study name, data
cache and output files are all keyed by it, so DTE-1 and DTE-2 runs never mix.

The v3 strategy rules are NOT changed. The reference simulator below
(simulate_day_multi_trades) is a parameterised, line-for-line copy of the one
in atm_straddle_backtest_v3_new_claude.py:
    * hard entry/exit cut-offs;
    * percentage stop-loss with an absolute rupee cap;
    * breakeven ratchet (BREAKEVEN_ARM_PCT / BREAKEVEN_LOCK_PCT);
    * profit-protect with separate arm and giveback;
    * profit target with LOWLOW or CLOSE fill detection;
    * re-entry ramp after STOPLOSS / PROFIT_PROTECT, premium-ratio re-entry gate;
    * optional re-entry after PROFIT_TARGET; daily loss circuit breaker;
    * transaction charges and actual-trade selection (one index per day).

RUN_MODE is read from the same property file:
    RUN_MODE=backtest  -> one normal backtest and Excel report (same as the
                          strategy script).
    RUN_MODE=optimize  -> load data once (or from the local cache), verify the
                          fast engine, then search.            [default]

Dependencies:
    pandas, numpy, openpyxl, optuna (3.1+; tested on 3.6, 4.2 and 5.0)
    optional: cmaes  (pip install cmaes) for the CMA-ES refinement phase

=============================================================================
HOW IT WORKS (same framework as the expiry-day V3OPT optimizer)
=============================================================================
1. DATA      Pickles are scanned once; only the days actually traded for this
             ALLOWED_DTE (nearest expiry, one index per date) are kept and
             converted to numpy. The result is cached locally, so re-runs
             start in seconds.
2. VERIFY    A numpy "fast engine" (~200-300x faster) must reproduce the
             pandas reference engine to the paisa on the exact baseline (all
             days) and on random parameter sets before any trial runs. Any
             difference above Rs 0.01 stops the run.
3. SEARCH    Multivariate TPE (models parameter interactions), then an
             optional CMA-ES refinement warm-started from the best TPE trials.
             Trial 0 is the property file itself (exact seed).
4. REPORT    The best trial is compared with the exact baseline. The top
             trials are re-scored on the holdout days, with stress slippage,
             and with the conservative PROFIT_TARGET_FILL=CLOSE check. The
             winner is re-simulated with the reference engine and written as
             a ready-to-use .properties file.

=============================================================================
OBJECTIVE MODES  (OPT_OBJECTIVE; every changed spot is tagged [MODES])
=============================================================================
  profit    maximize total net P&L                                   [default]
            identical to the previous version (OPT_CV_FOLDS still works)
  win_days  maximize the NUMBER OF PROFITABLE DAYS (day net > OPT_WIN_DAY_MIN_RUPEES);
            ties broken by net P&L; by default the config must also be net
            positive (OPT_MIN_NET_PNL=0)
  calmar    maximize net P&L / max drawdown of the daily equity curve -- the
            figure that actually limits how many lots an account can carry
  sharpe    maximize mean / std of daily net P&L (steadiest days)

  Guards (any mode; a trial that misses one can never be selected):
    OPT_MIN_NET_PNL, OPT_MIN_NET_VS_BASELINE, OPT_MAX_LOSS_DAY, OPT_MAX_DRAWDOWN

  Example -- "most winning days, but keep >= 80% of the profit and never lose
  more than Rs 15,000 in a day":
    OPT_OBJECTIVE=win_days
    OPT_WIN_DAY_MIN_RUPEES=500
    OPT_MIN_NET_VS_BASELINE=0.8
    OPT_MAX_LOSS_DAY=15000
    OPT_STUDY_NAME=<new name>

  Every mode reports the same day-level figures (profitable / losing days,
  max drawdown, net/DD, worst day) for the winner and the baseline, in-sample
  and holdout, so modes can be compared side by side.

=============================================================================
WHAT IS SEARCHED (defaults; every group can be frozen or narrowed)
=============================================================================
  Times          ENTRY_TIME_IST, EXIT_TIME_IST
  Stop-loss      MAX_LOSS_LIMIT_RUPEES_BY_ATTEMPT (Rs cap)
                 LOSS_LIMIT_RUPEES_BY_ATTEMPT scale - FROZEN by default (your
                 configs note the Rs cap binds on 100% of trades)
  Re-entry       MAX_REATTEMPTS
                 REENTRY_DELAY_BY_ATTEMPT as a ramp: base, step, ramp length
                 (7,10,13,16,19,22 = base 7, step 3, length 6)
                 PROFIT_TARGET_REENTRY_DELAY
  Target         PROFIT_TARGET_PCT
  Protect        on/off, PROFIT_PROTECT_ARM_PCT, PROFIT_PROTECT_GIVEBACK_PCT
  Breakeven      on/off, BREAKEVEN_ARM_PCT, BREAKEVEN_LOCK_PCT (lock <= arm)
  Premium gate   on/off, REENTRY_MAX_PREMIUM_RATIO
  Daily loss     MAX_DAILY_LOSS_RUPEES
  Frozen unless OPT_TUNE_SWITCHES=1: PROFIT_TARGET_REENTRY_ENABLED.
  Never searched: PROFIT_TARGET_FILL (a fill model, not a strategy choice; set
  OPT_TARGET_FILL=CLOSE to optimize under the conservative check).

=============================================================================
OPTIMIZER PROPERTY KEYS (all optional; put them in the same .properties file)
=============================================================================
  Run control : RUN_MODE, OPT_TRIALS, OPT_REFINE_TRIALS, OPT_SAMPLER,
                OPT_STARTUP_TRIALS, OPT_SEED, OPT_STUDY_NAME, OPT_STORAGE,
                OPT_OUTPUT_DIR, OPT_PROGRESS_EVERY, OPT_REPORT_TOP_K,
                OPT_DATA_CACHE, OPT_REFRESH_DATA_CACHE, SAMPLE_MAX_DAYS,
                SAMPLE_MAX_PICKLES
  Checks      : OPT_PARITY_CHECK, OPT_PARITY_RANDOM_TRIALS, OPT_PARITY_MAX_DAYS
  Objective   : OPT_OBJECTIVE, OPT_WIN_DAY_MIN_RUPEES, OPT_CALMAR_DD_FLOOR,
                OPT_MIN_NET_PNL, OPT_MIN_NET_VS_BASELINE, OPT_MAX_LOSS_DAY,
                OPT_MAX_DRAWDOWN
  Scoring     : OPT_HOLDOUT_FROM, OPT_SLIPPAGE_PTS_NIFTY/_SENSEX,
                OPT_STRESS_SLIPPAGE_PTS_NIFTY/_SENSEX, OPT_TARGET_FILL,
                OPT_MIN_DAYS, OPT_MIN_MONTHS, OPT_CV_FOLDS, OPT_CV_PENALTY
  Groups      : OPT_TUNE_TIMES, OPT_TUNE_STOPLOSS, OPT_TUNE_STOP_PCT,
                OPT_TUNE_REENTRY, OPT_TUNE_TARGET, OPT_TUNE_PROTECT,
                OPT_TUNE_BREAKEVEN, OPT_TUNE_PREMIUM_GATE, OPT_TUNE_DAILY_LOSS,
                OPT_TUNE_SWITCHES
  Bounds      : see "SEARCH BOUNDS" below. Set _MIN = _MAX to pin a value.
Change OPT_STUDY_NAME whenever you change bounds, scoring or the config.
"""

import os
from pathlib import Path
import glob
import time
import json
import math
import pickle
import hashlib
import warnings
from dataclasses import dataclass, field, replace
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
# CONFIG FILE - CHANGE THIS LINE TO SWITCH BETWEEN DTE-1 AND DTE-2
# =============================================================================
# Relative paths are resolved next to this script. The STRADDLE_CONFIG
# environment variable, when set, overrides this line.
# CONFIG_FILE = "configs/straddle_config_DTE_1_v3_new_claude.properties"
CONFIG_FILE = "configs/atm_straddle_DTE2_v3_claude_new.properties"


# ---------------------------------------------------------------------------
# CONFIGURATION SOURCE: external property file
# ---------------------------------------------------------------------------
# Every tunable setting lives in a simple KEY=VALUE property file. Values are
# pushed into the process environment so all the os.getenv(...) reads below
# pick them up. A real environment variable that is already set takes
# precedence over the file.
def _load_property_file() -> str:
    cfg_path = os.getenv(
        "STRADDLE_CONFIG",
        os.path.join(os.path.dirname(os.path.abspath(__file__)), CONFIG_FILE),
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
            # strip optional surrounding quotes
            if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
                val = val[1:-1]
            if key and key not in os.environ:   # real env vars win over the file
                os.environ[key] = val
                loaded += 1
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


# The DTE label is read here (before the strategy settings below) only to name
# the default study, cache and output files. The strategy itself parses
# ALLOWED_DTE again further down, exactly as the v3 script does.
_DTE_LABEL = "-".join(
    str(int(float(x))) for x in os.getenv("ALLOWED_DTE", "0").replace(" ", "").split(",") if x
) or "0"
if not os.getenv("ALLOWED_DTE", "").strip():
    # The strategy's own default (kept unchanged below) is [0] = expiry day.
    print("[CONFIG WARNING] ALLOWED_DTE is not set in the config; the v3 strategy "
          "default [0] (expiry day only) will be used. Set ALLOWED_DTE=1 or 2.",
          flush=True)


# =============================================================================
# RUN CONTROL AND OPTIMIZER SETTINGS
# =============================================================================
RUN_MODE = os.getenv("RUN_MODE", "optimize").strip().lower()

# ---- Search budget -----------------------------------------------------------
OPT_TRIALS = _env_int("OPT_TRIALS", 1000)          # main (TPE) phase
# Search algorithm for the main phase:
#   tpe            -> multivariate TPE (models parameter interactions)  [default]
#   tpe_univariate -> each parameter sampled independently
#   cmaes          -> CMA-ES from the start (needs 'pip install cmaes'); the
#                     on/off features stay at the config's setting
#   random         -> random search (sanity benchmark only)
OPT_SAMPLER = os.getenv("OPT_SAMPLER", "tpe").strip().lower()
# CMA-ES refinement trials after the main phase, around the best trial: its
# on/off feature pattern is kept fixed and every active setting is tuned
# together (warm-started from same-pattern trials). 0 disables. Skipped with a
# message if 'cmaes' is missing.
OPT_REFINE_TRIALS = _env_int("OPT_REFINE_TRIALS", 300)

# 1 = maximize total net profit over the in-sample days.
# >1 = contiguous monthly folds; objective = mean fold profit minus
#      OPT_CV_PENALTY x fold-profit std (a consistency-weighted score).
OPT_CV_FOLDS = _env_int("OPT_CV_FOLDS", 1)
OPT_CV_PENALTY = _env_float("OPT_CV_PENALTY", 0.15)

# ---- [MODES] What the search maximizes --------------------------------------
# OPT_OBJECTIVE selects the score every trial is ranked by (in-sample days):
#   profit    total net P&L in rupees.                                [default]
#             Exactly the previous behaviour (OPT_CV_FOLDS still applies).
#   win_days  number of PROFITABLE days (day net P&L > OPT_WIN_DAY_MIN_RUPEES).
#             Ties are broken by total net P&L, so among configs with the same
#             count the more profitable one wins. Guarded by OPT_MIN_NET_PNL
#             (default 0 in this mode: the config must also make money).
#   calmar    total net P&L / maximum drawdown of the daily equity curve.
#             "Profit per rupee of pain" -- the number that decides how many
#             lots the account can carry.
#   sharpe    mean daily net P&L / standard deviation of daily net P&L
#             (per traded day, not annualised). Rewards smooth, steady days.
# OPT_CV_FOLDS > 1 is a profit-mode feature; other modes ignore it (a message
# is printed).
#
# Why win_days is guarded: counting winning days alone rewards configs that
# bank many tiny wins and take rare large losses (tiny target, wide stop).
# Such a config can win 80%+ of days and still lose money, and adding lots
# multiplies the rare big loss too. The guards below keep the search honest.
OPT_OBJECTIVE = os.getenv("OPT_OBJECTIVE", "profit").strip().lower()
_OBJECTIVE_MODES = ("profit", "win_days", "calmar", "sharpe")
if OPT_OBJECTIVE not in _OBJECTIVE_MODES:
    raise ValueError(f"OPT_OBJECTIVE={OPT_OBJECTIVE!r}; use one of {', '.join(_OBJECTIVE_MODES)}")

# A day counts as "profitable" only above this net P&L (Rs). 0 = any profit.
# Raising it (e.g. 500) stops trivially small wins from padding the count.
OPT_WIN_DAY_MIN_RUPEES = _env_float("OPT_WIN_DAY_MIN_RUPEES", 0.0)

# ---- [MODES] Guards: a trial that breaks any of these cannot win -------------
# They apply in EVERY mode. Blank = off (except OPT_MIN_NET_PNL in win_days
# mode, which defaults to 0).
#   OPT_MIN_NET_PNL            in-sample net P&L must be >= this (Rs)
#   OPT_MIN_NET_VS_BASELINE    in-sample net P&L must be >= this fraction of the
#                              property-file baseline's (e.g. 0.8 = give up at
#                              most 20% of profit). Ignored if baseline <= 0.
#   OPT_MAX_LOSS_DAY           worst day must not lose more than this (Rs, >0)
#   OPT_MAX_DRAWDOWN           max drawdown must not exceed this (Rs, >0)
# Breaking trials get a score below every valid trial, ordered by how far they
# miss, so the sampler is still steered toward the valid region.
def _env_optional_float(name: str) -> Optional[float]:
    raw = os.getenv(name, "").strip()
    return float(raw.replace(",", "")) if raw else None


OPT_MIN_NET_PNL = _env_optional_float("OPT_MIN_NET_PNL")
if OPT_MIN_NET_PNL is None and OPT_OBJECTIVE == "win_days":
    OPT_MIN_NET_PNL = 0.0
OPT_MIN_NET_VS_BASELINE = _env_optional_float("OPT_MIN_NET_VS_BASELINE")
OPT_MAX_LOSS_DAY = _env_optional_float("OPT_MAX_LOSS_DAY")
OPT_MAX_DRAWDOWN = _env_optional_float("OPT_MAX_DRAWDOWN")
# calmar: drawdowns smaller than this (Rs) are treated as this, so a config
# with almost no drawdown (e.g. hardly trading) cannot score an absurd ratio.
OPT_CALMAR_DD_FLOOR = _env_float("OPT_CALMAR_DD_FLOOR", 5000.0)

OPT_SEED = _env_int("OPT_SEED", 42)
OPT_STARTUP_TRIALS = _env_int("OPT_STARTUP_TRIALS", 50)
OPT_PROGRESS_EVERY = max(1, _env_int("OPT_PROGRESS_EVERY", 10))  # a new best is always printed

# ---- Output / storage ----------------------------------------------------------
_OPT_DEFAULT_PARENT = Path.home() / "Downloads"
if not _OPT_DEFAULT_PARENT.exists():
    _OPT_DEFAULT_PARENT = Path.home()
OPT_OUTPUT_DIR = os.getenv("OPT_OUTPUT_DIR", str(_OPT_DEFAULT_PARENT / "v3_dte_optimizer_runs"))
# Default study name carries the DTE, so DTE-1 and DTE-2 studies never mix.
OPT_STUDY_NAME = os.getenv("OPT_STUDY_NAME", f"short_straddle_v3_dte{_DTE_LABEL}")
# Optuna storage: journal (append-only log, resumable, Windows-safe) [default],
# sqlite, or memory (not resumable). OPT_SAVE_DB=0 also means memory.
OPT_STORAGE = os.getenv("OPT_STORAGE", "journal").strip().lower()
if os.getenv("OPT_SAVE_DB", "1").strip() == "0":
    OPT_STORAGE = "memory"

# Local cache of the loaded, filtered, numpy-converted day data. The cache key
# includes every pickle's size/modification time and ALLOWED_DTE.
OPT_DATA_CACHE = _env_flag("OPT_DATA_CACHE", True)
OPT_REFRESH_DATA_CACHE = _env_flag("OPT_REFRESH_DATA_CACHE", False)

# ---- Built-in verification -----------------------------------------------------
# The fast engine must reproduce the reference engine: exact baseline on ALL
# days, plus OPT_PARITY_RANDOM_TRIALS random parameter sets on up to
# OPT_PARITY_MAX_DAYS evenly spaced days. Any per-day difference above
# Rs 0.01 stops the run. Set OPT_PARITY_CHECK=0 only after it has passed once.
OPT_PARITY_CHECK = _env_flag("OPT_PARITY_CHECK", True)
OPT_PARITY_RANDOM_TRIALS = max(0, _env_int("OPT_PARITY_RANDOM_TRIALS", 2))
OPT_PARITY_MAX_DAYS = max(0, _env_int("OPT_PARITY_MAX_DAYS", 60))
_PARITY_TOLERANCE_RUPEES = 0.01

# ---- Honest scoring ----------------------------------------------------------
# Out-of-sample holdout: days ON/AFTER this date are excluded from the
# objective and reported only at the end. Blank = no holdout.
OPT_HOLDOUT_FROM_RAW = os.getenv("OPT_HOLDOUT_FROM", "").strip()
OPT_HOLDOUT_FROM: Optional[date] = (
    datetime.strptime(OPT_HOLDOUT_FROM_RAW, "%Y-%m-%d").date()
    if OPT_HOLDOUT_FROM_RAW else None
)
# Slippage deducted INSIDE the objective, in option points per leg per order.
# One attempt = 4 orders, so the cost per attempt is 4 * points * qty.
OPT_SLIPPAGE_PTS = {
    "NIFTY": _env_float("OPT_SLIPPAGE_PTS_NIFTY", 0.0),
    "SENSEX": _env_float("OPT_SLIPPAGE_PTS_SENSEX", 0.0),
}
# Slippage used only for the end-of-run stress columns (ASSUMPTIONS: calibrate
# them from your live trader's fills).
OPT_STRESS_SLIPPAGE_PTS = {
    "NIFTY": _env_float("OPT_STRESS_SLIPPAGE_PTS_NIFTY", 0.25),
    "SENSEX": _env_float("OPT_STRESS_SLIPPAGE_PTS_SENSEX", 0.75),
}
# Profit-target fill model used by the OBJECTIVE. Blank = PROFIT_TARGET_FILL
# from the config. CLOSE = target counted on minute closes only (conservative:
# the CE low and the PE low of a minute rarely happen together). The report
# always shows both, whatever this is set to.
OPT_TARGET_FILL_RAW = os.getenv("OPT_TARGET_FILL", "").strip().upper()
OPT_REPORT_TOP_K = max(1, _env_int("OPT_REPORT_TOP_K", 20))

# ---- Smoke-test controls. Leave blank/0 for the full data set. ---------------
_SAMPLE_PICKLES_RAW = os.getenv("SAMPLE_MAX_PICKLES", "").strip()
_SAMPLE_DAYS_RAW = os.getenv("SAMPLE_MAX_DAYS", "").strip()
SAMPLE_MAX_PICKLES = int(float(_SAMPLE_PICKLES_RAW)) if _SAMPLE_PICKLES_RAW else None
SAMPLE_MAX_DAYS = int(float(_SAMPLE_DAYS_RAW)) if _SAMPLE_DAYS_RAW else None

# Minimum activity guards (in-sample), CAPPED to the data available: a trial
# must trade on >= min(OPT_MIN_DAYS, 75% of in-sample days) days and in
# >= min(OPT_MIN_MONTHS, in-sample months) months. DTE-2 trades only about one
# day a week, so the cap matters there.
OPT_MIN_DAYS = _env_int("OPT_MIN_DAYS", 100)
OPT_MIN_MONTHS = _env_int("OPT_MIN_MONTHS", 6)
_MIN_DAYS_FRACTION = 0.75
_OPT_DISQUALIFY = -1.0e18
# [MODES] Trials breaking an OPT_MIN_*/OPT_MAX_* guard score in
# (_OPT_DISQUALIFY, _OPT_INFEASIBLE]: below every valid trial, above trials
# that fail the activity guard. Anything <= _OPT_INFEASIBLE is "not valid".
_OPT_INFEASIBLE = -1.0e15
# Net-P&L floor actually enforced this run (OPT_MIN_NET_PNL and/or
# OPT_MIN_NET_VS_BASELINE x baseline); set once in optimize().
_NET_FLOOR: Optional[float] = None
_ACTIVE_MINIMA: Optional[Tuple[int, int]] = None   # set once per run

# ---- Search-group switches (0 = freeze the group at the config values) --------
OPT_TUNE_TIMES = _env_flag("OPT_TUNE_TIMES", True)
OPT_TUNE_STOPLOSS = _env_flag("OPT_TUNE_STOPLOSS", True)        # Rs stop cap
# The % stop list is FROZEN by default: in both v3 configs the Rs cap binds on
# 100% of trades, so the % list has no effect. Set 1 to search a scale on it.
OPT_TUNE_STOP_PCT = _env_flag("OPT_TUNE_STOP_PCT", False)
OPT_TUNE_REENTRY = _env_flag("OPT_TUNE_REENTRY", True)          # attempts, ramp, target delay
OPT_TUNE_TARGET = _env_flag("OPT_TUNE_TARGET", True)            # PROFIT_TARGET_PCT
OPT_TUNE_PROTECT = _env_flag("OPT_TUNE_PROTECT", True)          # on/off, arm, giveback
OPT_TUNE_BREAKEVEN = _env_flag("OPT_TUNE_BREAKEVEN", True)      # on/off, arm, lock
OPT_TUNE_PREMIUM_GATE = _env_flag("OPT_TUNE_PREMIUM_GATE", True)  # on/off, ratio
OPT_TUNE_DAILY_LOSS = _env_flag("OPT_TUNE_DAILY_LOSS", True)
# PROFIT_TARGET_REENTRY_ENABLED is the core v3 idea, so it is frozen by default.
OPT_TUNE_SWITCHES = _env_flag("OPT_TUNE_SWITCHES", False)

# =============================================================================
# SEARCH BOUNDS
# =============================================================================
# Every bound below contains the values in BOTH v3 configs (DTE-1 and DTE-2),
# and every integer grid has those values exactly on a grid point, so trial 0
# reproduces either config exactly. Set _MIN = _MAX to pin a value.
#
#                          DTE-1 config      DTE-2 config
#   entry                  09:20             09:40
#   exit                   15:35 (=15:30)    15:07
#   stop cap               3500              5000
#   max re-attempts        10                10
#   re-entry ramp          7,10..22 (6)      20,23..47 (10)
#   target re-entry delay  10                10
#   profit target          0.15              0.11
#   protect arm/giveback   0.235 / 0.20      0.12 / 0.01
#   breakeven arm/lock     off               0.12 / 0.01
#   premium ratio          1.32              off
#   daily loss             20000             20000
OPT_ENTRY_FROM_IST = os.getenv("OPT_ENTRY_FROM_IST", "09:15")
OPT_ENTRY_TO_IST = os.getenv("OPT_ENTRY_TO_IST", "11:00")
OPT_ENTRY_STEP_MIN = max(1, _env_int("OPT_ENTRY_STEP_MIN", 5))
# The engine never monitors past 15:30, so EXIT_TIME_IST=15:35 behaves exactly
# like 15:30; the seed uses 15:30.
OPT_EXIT_FROM_IST = os.getenv("OPT_EXIT_FROM_IST", "14:30")
OPT_EXIT_TO_IST = os.getenv("OPT_EXIT_TO_IST", "15:30")
OPT_EXIT_STEP_MIN = max(1, _env_int("OPT_EXIT_STEP_MIN", 1))

OPT_STOP_CAP_MIN = _env_int("OPT_STOP_CAP_MIN", 1500)
OPT_STOP_CAP_MAX = _env_int("OPT_STOP_CAP_MAX", 10000)
OPT_STOP_CAP_STEP = max(1, _env_int("OPT_STOP_CAP_STEP", 250))
# Multiplier on the config's LOSS_LIMIT_RUPEES_BY_ATTEMPT list (only searched
# with OPT_TUNE_STOP_PCT=1). Each value is capped at 0.95 because the
# strategy's percentage parser reads anything above 1.0 as a percent.
OPT_STOP_SCALE_MIN = _env_float("OPT_STOP_SCALE_MIN", 0.20)
OPT_STOP_SCALE_MAX = _env_float("OPT_STOP_SCALE_MAX", 1.50)
_STOP_PCT_UPPER = 0.95

# MAX_REATTEMPTS counts target re-entries AND stop/protect re-entries.
OPT_REATTEMPTS_MIN = _env_int("OPT_REATTEMPTS_MIN", 2)
OPT_REATTEMPTS_MAX = _env_int("OPT_REATTEMPTS_MAX", 25)
# Re-entry delay ramp after STOPLOSS / PROFIT_PROTECT:
#   delay before re-entry k (k = 0, 1, ...) = base + step * min(k, length - 1)
# i.e. REENTRY_DELAY_BY_ATTEMPT = base, base+step, ... (length values); later
# re-entries reuse the last value, exactly like the strategy's list.
OPT_REENTRY_BASE_MIN = _env_int("OPT_REENTRY_BASE_MIN", 0)
OPT_REENTRY_BASE_MAX = _env_int("OPT_REENTRY_BASE_MAX", 40)
OPT_REENTRY_STEP_MIN = _env_int("OPT_REENTRY_STEP_MIN", 0)
OPT_REENTRY_STEP_MAX = _env_int("OPT_REENTRY_STEP_MAX", 6)
OPT_REENTRY_LENGTH_MIN = _env_int("OPT_REENTRY_LENGTH_MIN", 1)
OPT_REENTRY_LENGTH_MAX = _env_int("OPT_REENTRY_LENGTH_MAX", 12)
OPT_TARGET_REENTRY_DELAY_MIN = _env_int("OPT_TARGET_REENTRY_DELAY_MIN", 0)
OPT_TARGET_REENTRY_DELAY_MAX = _env_int("OPT_TARGET_REENTRY_DELAY_MAX", 30)

OPT_TARGET_MIN = _env_float("OPT_TARGET_MIN", 0.05)
OPT_TARGET_MAX = _env_float("OPT_TARGET_MAX", 0.50)
OPT_PROTECT_ARM_MIN = _env_float("OPT_PROTECT_ARM_MIN", 0.02)
OPT_PROTECT_ARM_MAX = _env_float("OPT_PROTECT_ARM_MAX", 0.40)
OPT_PROTECT_GIVEBACK_MIN = _env_float("OPT_PROTECT_GIVEBACK_MIN", 0.00)
OPT_PROTECT_GIVEBACK_MAX = _env_float("OPT_PROTECT_GIVEBACK_MAX", 0.30)
OPT_BREAKEVEN_ARM_MIN = _env_float("OPT_BREAKEVEN_ARM_MIN", 0.02)
OPT_BREAKEVEN_ARM_MAX = _env_float("OPT_BREAKEVEN_ARM_MAX", 0.40)
# The lock is clipped to <= the arm level: a lock above the arm would book a
# profit the trade never had at the moment it armed.
OPT_BREAKEVEN_LOCK_MIN = _env_float("OPT_BREAKEVEN_LOCK_MIN", 0.00)
OPT_BREAKEVEN_LOCK_MAX = _env_float("OPT_BREAKEVEN_LOCK_MAX", 0.10)
OPT_PREMIUM_RATIO_MIN = _env_float("OPT_PREMIUM_RATIO_MIN", 1.05)
OPT_PREMIUM_RATIO_MAX = _env_float("OPT_PREMIUM_RATIO_MAX", 2.00)
OPT_DAILY_LOSS_MIN = _env_int("OPT_DAILY_LOSS_MIN", 8000)
OPT_DAILY_LOSS_MAX = _env_int("OPT_DAILY_LOSS_MAX", 40000)
OPT_DAILY_LOSS_STEP = max(1, _env_int("OPT_DAILY_LOSS_STEP", 1000))

# Floats suggested by the optimizer are rounded to this many decimals so the
# written .properties file reproduces the optimizer's result exactly.
_PARAM_DECIMALS = 6


# =============================================================================
# STRATEGY SETTINGS AND HELPERS - copied unchanged from
# atm_straddle_backtest_v3_new_claude.py (they define the baseline config)
# =============================================================================
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

# =============================================================================
# v2 ADDITIONS
# =============================================================================
# --- Profit-protect ARM and GIVEBACK are now separate ------------------------
# The original used ONE number for two different jobs: how much profit arms the
# trail, and how much of the peak you hand back before exiting. Leave either
# unset to fall back to PROFIT_PROTECT_TRIGGER_RUPEES (identical old behaviour).
_pp_arm_raw = os.getenv("PROFIT_PROTECT_ARM_PCT")
_pp_give_raw = os.getenv("PROFIT_PROTECT_GIVEBACK_PCT")
PROFIT_PROTECT_ARM_PCT = (_parse_pct_value(_pp_arm_raw) if _pp_arm_raw
                          else PROFIT_PROTECT_TRIGGER_RUPEES)
PROFIT_PROTECT_GIVEBACK_PCT = (_parse_pct_value(_pp_give_raw) if _pp_give_raw
                               else PROFIT_PROTECT_TRIGGER_RUPEES)

# --- Breakeven ratchet -------------------------------------------------------
# Once an attempt's peak profit reaches BREAKEVEN_ARM_PCT of the premium
# collected, the stop floor is lifted to BREAKEVEN_LOCK_PCT of premium, so a
# trade that ran into profit can no longer come back as a full stop-out.
#
# This is the only parameter change that survived walk-forward validation:
# it improved 5 of 6 independent time blocks and cut max drawdown ~30%.
# Set 0 to disable.
BREAKEVEN_ARM_PCT = _parse_pct_value(os.getenv("BREAKEVEN_ARM_PCT", 0.0))
BREAKEVEN_LOCK_PCT = _parse_pct_value(os.getenv("BREAKEVEN_LOCK_PCT", 0.0))

# --- Re-entry premium gate ---------------------------------------------------
# Stop re-entering once the fresh ATM straddle is richer than the one just
# stopped out of by more than this ratio (volatility still expanding).
# 0 disables. 1.25 = skip if the new straddle costs >25% more.
REENTRY_MAX_PREMIUM_RATIO = _parse_float_env("REENTRY_MAX_PREMIUM_RATIO", 0.0)

# --- v3: re-entry after PROFIT_TARGET ---------------------------------------
# 0 (default) = original behaviour: hitting the profit target ends the day.
# 1 = open a fresh ATM straddle PROFIT_TARGET_REENTRY_DELAY minutes later
# (counts toward MAX_REATTEMPTS; one straddle at a time, same lots).
PROFIT_TARGET_REENTRY_ENABLED = os.getenv("PROFIT_TARGET_REENTRY_ENABLED", "0").strip() == "1"
PROFIT_TARGET_REENTRY_DELAY = int(float(os.getenv("PROFIT_TARGET_REENTRY_DELAY", "0") or 0))

# --- v3: how a PROFIT_TARGET touch is detected -------------------------------
# LOWLOW (default, original): best of minute close and CE-low + PE-low.
# CLOSE: minute close only (conservative check; both legs' lows rarely coincide).
PROFIT_TARGET_FILL = os.getenv("PROFIT_TARGET_FILL", "LOWLOW").strip().upper()


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
    f"short_straddle_backtest_v3_reattempt{_safe_fname_part(ENTRY_TIME_IST)}"
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
SESSION_END_IST = dtime(15, 30)

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


# =============================================================================
# OPTIMIZER: PARAMETER OBJECT
# =============================================================================
@dataclass
class Params:
    """
    Complete v3 parameter set consumed by the simulators.

    Passing this object explicitly (instead of reading module globals) lets one
    Python process run thousands of different configurations without
    re-importing the strategy. Field-by-field it maps to the property keys:

        entry_time / exit_time            ENTRY_TIME_IST / EXIT_TIME_IST
        loss_limit_pct_by_attempt         LOSS_LIMIT_RUPEES_BY_ATTEMPT
        max_loss_limit_cap_rupees         MAX_LOSS_LIMIT_RUPEES_BY_ATTEMPT
        max_reattempts                    MAX_REATTEMPTS
        reentry_delay_by_attempt          REENTRY_DELAY_BY_ATTEMPT
        profit_target_pct                 PROFIT_TARGET_PCT
        profit_target_fill                PROFIT_TARGET_FILL (LOWLOW | CLOSE)
        profit_target_reentry_enabled     PROFIT_TARGET_REENTRY_ENABLED
        profit_target_reentry_delay_min   PROFIT_TARGET_REENTRY_DELAY
        profit_protect_trigger_pct        PROFIT_PROTECT_TRIGGER_RUPEES (report only)
        profit_protect_arm_pct            PROFIT_PROTECT_ARM_PCT (0 = protect off)
        profit_protect_giveback_pct       PROFIT_PROTECT_GIVEBACK_PCT
        breakeven_arm_pct                 BREAKEVEN_ARM_PCT (0 = ratchet off)
        breakeven_lock_pct                BREAKEVEN_LOCK_PCT
        reentry_max_premium_ratio         REENTRY_MAX_PREMIUM_RATIO (0 = gate off)
        max_daily_loss_rupees             MAX_DAILY_LOSS_RUPEES (0 = off)
    """

    entry_time: dtime
    exit_time: dtime
    loss_limit_pct_by_attempt: List[float]
    max_loss_limit_cap_rupees: float
    max_reattempts: int
    reentry_delay_by_attempt: List[int]
    profit_target_pct: float
    profit_target_fill: str
    profit_target_reentry_enabled: bool
    profit_target_reentry_delay_min: int
    profit_protect_trigger_pct: float
    profit_protect_arm_pct: float
    profit_protect_giveback_pct: float
    breakeven_arm_pct: float
    breakeven_lock_pct: float
    reentry_max_premium_ratio: float
    max_daily_loss_rupees: float

    def loss_limit_pct_for_attempt(self, attempt_idx: int) -> float:
        """Same rule as loss_limit_pct_for_attempt(): beyond the list, reuse the last."""
        values = self.loss_limit_pct_by_attempt
        if not values:
            return 0.0
        return float(values[attempt_idx]) if attempt_idx < len(values) else float(values[-1])

    def reentry_delay_for_attempt(self, attempt_idx: int) -> int:
        """Same rule as reentry_delay_for_attempt(): beyond the list, reuse the last."""
        values = self.reentry_delay_by_attempt
        if not values:
            return 0
        return int(values[attempt_idx]) if attempt_idx < len(values) else int(values[-1])


def default_params() -> Params:
    """Build a parameter object from the loaded property file (the baseline)."""
    return Params(
        entry_time=ENTRY_TIME,
        exit_time=EXIT_TIME,
        loss_limit_pct_by_attempt=list(LOSS_LIMIT_RUPEES_BY_ATTEMPT),
        max_loss_limit_cap_rupees=float(MAX_LOSS_LIMIT_RUPEES_BY_ATTEMPT),
        max_reattempts=int(MAX_REATTEMPTS),
        reentry_delay_by_attempt=list(REENTRY_DELAY_BY_ATTEMPT),
        profit_target_pct=float(PROFIT_TARGET_PCT),
        profit_target_fill=str(PROFIT_TARGET_FILL),
        profit_target_reentry_enabled=bool(PROFIT_TARGET_REENTRY_ENABLED),
        profit_target_reentry_delay_min=int(PROFIT_TARGET_REENTRY_DELAY),
        profit_protect_trigger_pct=float(PROFIT_PROTECT_TRIGGER_RUPEES),
        profit_protect_arm_pct=float(PROFIT_PROTECT_ARM_PCT),
        profit_protect_giveback_pct=float(PROFIT_PROTECT_GIVEBACK_PCT),
        breakeven_arm_pct=float(BREAKEVEN_ARM_PCT),
        breakeven_lock_pct=float(BREAKEVEN_LOCK_PCT),
        reentry_max_premium_ratio=float(REENTRY_MAX_PREMIUM_RATIO),
        max_daily_loss_rupees=float(MAX_DAILY_LOSS_RUPEES),
    )


@dataclass
class DayGroup:
    """
    One parameter-independent (underlying, date, nearest-expiry) simulation unit.

    Pickle parsing, date normalization, day slicing and option-series
    reindexing are done once; every trial reuses these cached objects.
    """

    und: str
    dy: date
    expiry: date
    day_opt: pd.DataFrame
    underlying_day: pd.DataFrame
    price_book: Optional[Dict[Tuple[int, str, str], pd.Series]] = None
    symbols: Optional[Dict[Tuple[int, str], str]] = None
    idx_all: Optional[pd.DatetimeIndex] = None


# =============================================================================
# REFERENCE ENGINE (pandas) - parameterised copy of the v3 simulator
# =============================================================================
# Identical to simulate_day_multi_trades() in atm_straddle_backtest_v3_new_claude.py
# except that (1) every setting comes from `params` instead of module globals
# and (2) option series come from the per-day price book (same symbol rule and
# reindexing as _pick_symbol/_build_leg_series). Used for the parity checks and
# the trade-level reports; the search itself uses the fast engine below.
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
        idx_all = build_minute_index(dy, SESSION_START_IST, SESSION_END_IST)
    if price_book is None or symbols is None:
        price_book, symbols = build_price_book(day_opt, idx_all)
    session_end_ts = idx_all[-1]

    # EXIT_TIME_IST is the strategy time filter: no entry at/after it, and the
    # open attempt is monitored only up to it (TIME_EXIT if nothing fired).
    configured_exit_cutoff_ts = pd.Timestamp(datetime.combine(dy, params.exit_time), tz=ist_tz())
    trade_end_ts = min(session_end_ts, configured_exit_cutoff_ts)

    qty = int(QTY_UNITS[und])
    step = int(STRIKE_STEP[und])

    # Reporting value (G) uses PROFIT_PROTECT_TRIGGER_RUPEES; the protect logic
    # itself uses the separate ARM / GIVEBACK values (v2).
    profit_protect_pct = float(params.profit_protect_trigger_pct)
    profit_protect_enabled = float(params.profit_protect_arm_pct) > 0.0

    cur_entry_ts = pd.Timestamp(datetime.combine(dy, params.entry_time), tz=ist_tz())
    trade_seq = 1

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

    daily_realized_pnl = 0.0
    max_daily_loss_rupees = float(params.max_daily_loss_rupees)
    daily_loss_limit_enabled = max_daily_loss_rupees > 0
    prev_entry_premium_per_unit = None   # v2: for REENTRY_MAX_PREMIUM_RATIO

    while cur_entry_ts < trade_end_ts:
        if daily_loss_limit_enabled and daily_realized_pnl <= -float(max_daily_loss_rupees):
            skipped.append({
                "day": dy, "underlying": und, "expiry": expiry, "trade_seq": trade_seq,
                "reason": (
                    f"Daily loss limit hit before next entry: "
                    f"realized_pnl={daily_realized_pnl:.2f}, limit={max_daily_loss_rupees:.2f}"
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

        ce_close_raw = _leg_from_book(price_book, idx_all, atm, "CE", "close")
        pe_close_raw = _leg_from_book(price_book, idx_all, atm, "PE", "close")
        ce_close = ce_close_raw.ffill()
        pe_close = pe_close_raw.ffill()
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

        # --- v2: do not re-sell into still-expanding volatility ---
        ratio = float(params.reentry_max_premium_ratio)
        if trade_seq > 1 and ratio > 0 and prev_entry_premium_per_unit:
            if (float(ce_entry) + float(pe_entry)) > (prev_entry_premium_per_unit * ratio):
                skipped.append({
                    "day": dy, "underlying": und, "expiry": expiry,
                    "trade_seq": trade_seq, "atm_strike": atm,
                    "reason": (
                        f"No re-entry: ATM premium {float(ce_entry)+float(pe_entry):.2f} "
                        f"> {ratio:.2f}x previous {prev_entry_premium_per_unit:.2f}"),
                })
                break
        prev_entry_premium_per_unit = float(ce_entry) + float(pe_entry)

        # Percentage-based risk basis for THIS attempt.
        entry_premium_sum = (float(ce_entry) + float(pe_entry)) * qty

        loss_limit_pct = params.loss_limit_pct_for_attempt(trade_seq - 1)
        uncapped_loss_limit_rupees = float(loss_limit_pct * entry_premium_sum)
        stop_cap_rupees = float(params.max_loss_limit_cap_rupees)
        if stop_cap_rupees > 0:
            loss_limit_rupees = float(min(uncapped_loss_limit_rupees, stop_cap_rupees))
        else:
            loss_limit_rupees = float(uncapped_loss_limit_rupees)

        G = float(profit_protect_pct * entry_premium_sum)   # reporting only

        pnl_close_all = (float(ce_entry) - ce_close) * qty + (float(pe_entry) - pe_close) * qty
        pnl = pnl_close_all.loc[monitor_start_ts:trade_end_ts].dropna()

        pnl_ceHigh_peLow_all = (float(ce_entry) - ce_high) * qty + (float(pe_entry) - pe_low) * qty
        pnl_ceLow_peHigh_all = (float(ce_entry) - ce_low) * qty + (float(pe_entry) - pe_high) * qty
        pnl_sl_all = pd.concat([pnl_close_all, pnl_ceHigh_peLow_all, pnl_ceLow_peHigh_all], axis=1).min(axis=1)
        pnl_sl = pnl_sl_all.loc[monitor_start_ts:trade_end_ts].dropna()

        if pnl.empty:
            skipped.append({"day": dy, "underlying": und, "expiry": expiry, "trade_seq": trade_seq,
                            "atm_strike": atm, "reason": "PnL series empty after entry"})
            break

        eod_ts = pnl.index[-1]
        eod_pnl = float(pnl.iloc[-1])
        default_exit_reason = "TIME_EXIT" if trade_end_ts < session_end_ts else "EOD"
        max_profit = float(max(0.0, pnl.max()))
        max_loss = float(min(0.0, pnl.min()))

        # --- v2: breakeven ratchet ---
        stop_floor = pd.Series(-float(loss_limit_rupees), index=pnl_sl.index)
        if params.breakeven_arm_pct > 0:
            running_peak = pnl.cummax().reindex(pnl_sl.index).ffill()
            be_armed = running_peak >= (params.breakeven_arm_pct * entry_premium_sum)
            be_floor = float(params.breakeven_lock_pct * entry_premium_sum)
            stop_floor = stop_floor.mask(be_armed.fillna(False),
                                         max(-float(loss_limit_rupees), be_floor))

        stop_hit = pnl_sl <= stop_floor
        stop_ts = pnl_sl.index[stop_hit.to_numpy().argmax()] if stop_hit.any() else None

        protect_ts = None
        if profit_protect_enabled:
            peak = pnl.cummax()
            arm_rupees = float(params.profit_protect_arm_pct * entry_premium_sum)
            give_rupees = float(params.profit_protect_giveback_pct * entry_premium_sum)
            armed = peak >= arm_rupees
            trail = peak - give_rupees
            protect_hit = armed & (pnl <= trail)
            protect_ts = pnl.index[protect_hit.to_numpy().argmax()] if protect_hit.any() else None

        target_ts = None
        target_rupees = None
        if params.profit_target_pct > 0.0:
            target_rupees = params.profit_target_pct * entry_premium_sum
            pnl_best_all = (float(ce_entry) - ce_low) * qty + (float(pe_entry) - pe_low) * qty
            pnl_tp = pd.concat([pnl_close_all, pnl_best_all], axis=1).max(axis=1)
            if params.profit_target_fill == "CLOSE":  # v3
                pnl_tp = pnl_close_all
            pnl_tp = pnl_tp.loc[monitor_start_ts:trade_end_ts].dropna()
            tp_hit = pnl_tp >= float(target_rupees)
            target_ts = pnl_tp.index[tp_hit.to_numpy().argmax()] if tp_hit.any() else None

        # Earliest exit wins; ties: STOPLOSS, then PROFIT_TARGET, then PROFIT_PROTECT.
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
            exit_pnl_gross = float(stop_floor.loc[exit_ts])   # may be a small profit (ratchet)
        elif exit_reason == "PROFIT_TARGET":
            exit_pnl_gross = float(target_rupees)
        else:
            exit_pnl_gross = float(pnl.loc[exit_ts])

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
                "day": dy, "underlying": und, "expiry": expiry, "trade_seq": trade_seq + 1,
                "reason": (
                    f"No re-entry: daily loss limit hit after trade_seq={trade_seq}; "
                    f"realized_pnl={daily_realized_pnl:.2f}, limit={max_daily_loss_rupees:.2f}"
                ),
            })
            break

        if exit_reason in ("STOPLOSS", "PROFIT_PROTECT") and (trade_seq - 1) < params.max_reattempts:
            delay_min = params.reentry_delay_for_attempt(trade_seq - 1)
            trade_seq += 1
            cur_entry_ts = pd.Timestamp(exit_ts) + pd.Timedelta(minutes=delay_min)
            if cur_entry_ts >= trade_end_ts:
                skipped.append({
                    "day": dy, "underlying": und, "expiry": expiry, "trade_seq": trade_seq,
                    "reason": (
                        f"No re-entry: next entry time {pd.Timestamp(cur_entry_ts).strftime('%H:%M')} "
                        f"is at/after exit cutoff {params.exit_time.strftime('%H:%M')}"
                    ),
                })
                break
            continue

        # v3: optionally recycle after a PROFIT_TARGET instead of ending the day
        if (exit_reason == "PROFIT_TARGET" and params.profit_target_reentry_enabled
                and (trade_seq - 1) < params.max_reattempts):
            trade_seq += 1
            cur_entry_ts = pd.Timestamp(exit_ts) + pd.Timedelta(minutes=params.profit_target_reentry_delay_min)
            prev_entry_premium_per_unit = None  # premium-ratio gate compares stop re-entries only
            if cur_entry_ts >= trade_end_ts:
                skipped.append({
                    "day": dy, "underlying": und, "expiry": expiry, "trade_seq": trade_seq,
                    "reason": (f"No target re-entry: next entry time "
                               f"{pd.Timestamp(cur_entry_ts).strftime('%H:%M')} is at/after "
                               f"exit cutoff {params.exit_time.strftime('%H:%M')}"),
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

    Optimizer additions:
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

                # Skip groups that can never appear in actual_trades.
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
            item.idx_all = build_minute_index(item.dy, SESSION_START_IST, SESSION_END_IST)
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
            item.idx_all = build_minute_index(item.dy, SESSION_START_IST, SESSION_END_IST)
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
# MAIN (RUN_MODE=backtest) - same output as the v3 strategy script
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
    print(f"[INFO] v2 ProfitProtect arm/giveback: {PROFIT_PROTECT_ARM_PCT:.2%} / "
          f"{PROFIT_PROTECT_GIVEBACK_PCT:.2%}")
    print(f"[INFO] v2 Breakeven ratchet: arm {BREAKEVEN_ARM_PCT:.2%} -> lock "
          f"{BREAKEVEN_LOCK_PCT:.2%} of premium (0 = off)")
    print(f"[INFO] v2 Re-entry premium gate: {REENTRY_MAX_PREMIUM_RATIO:.2f}x (0 = off)")
    print(f"[INFO] v3 Re-entry after profit target: {'ON' if PROFIT_TARGET_REENTRY_ENABLED else 'off'}"
          f" (delay {PROFIT_TARGET_REENTRY_DELAY} min) | target check: {PROFIT_TARGET_FILL}")
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
#   The reference engine above is the source of truth and produces the
#   trade-level reports, but inside an optimizer it is dominated by pandas
#   overhead: asof_close() re-parses the whole underlying day on every attempt,
#   and every attempt builds tz-aware Timestamps, .loc slices and pd.concat
#   frames over 376-row series. The fast engine performs EXACTLY the same
#   arithmetic on numpy arrays indexed by integer minute offsets
#   (0 = 09:15, 375 = 15:30), with every parameter-independent array built once
#   per day. Same operations, same order, same float64 values, so results are
#   bit-identical; the start-up parity check proves it on your data.
#
# MAPPING (reference -> fast)
#   cur_entry_ts / exit_ts       -> integer minute offset m
#   trade_end_ts                 -> end = min(375, exit_time - 09:15)
#   asof_close(underlying, ts)   -> u_asof[m]   (same 'pad' lookup, done once)
#   _leg_from_book(..)           -> raw close / ffilled close / high / low arrays
#   DataFrame.min/max(axis=1)    -> np.fmin / np.fmax (both skip NaN like pandas)
#   Series.cummax()              -> np.maximum.accumulate
#   stop_floor.mask(armed, v)    -> np.where(armed, v, -stop)
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

    def prepare(self) -> "FastDay":
        """Build the forward-filled close arrays (not stored in the cache file)."""
        self.legs = {
            key: (close_raw, _ffill_array(close_raw), high, low)
            for key, (close_raw, high, low) in self.raw.items()
        }
        return self


def build_fast_day(group: DayGroup) -> FastDay:
    """Convert one cached reference DayGroup into its numpy form."""
    if group.idx_all is None or group.price_book is None or group.symbols is None:
        group.idx_all = build_minute_index(group.dy, SESSION_START_IST, SESSION_END_IST)
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
    idx_all = build_minute_index(fd.dy, SESSION_START_IST, SESSION_END_IST)
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
class _FastParams:
    """A Params object pre-converted for the fast kernel (built once per trial)."""

    entry_m: int                 # entry minute offset (0 = 09:15)
    exit_m: int                  # exit cutoff minute offset
    loss_pcts: Tuple[float, ...]
    cap: float
    max_re: int
    delays: Tuple[int, ...]
    pt: float                    # profit target % (0 = off)
    pt_close_only: bool          # PROFIT_TARGET_FILL == "CLOSE"
    tgt_on: bool
    tgt_delay: int
    pp_arm: float                # profit-protect arm % (0 = off)
    pp_give: float               # profit-protect giveback %
    be_arm: float                # breakeven arm % (0 = off)
    be_lock: float               # breakeven lock %
    ratio: float                 # re-entry premium ratio (0 = off)
    dll: float                   # daily loss limit Rs (0 = off)


def _fast_params(params: Params) -> _FastParams:
    """Translate the simulator's Params into the kernel's flat representation."""
    return _FastParams(
        entry_m=_minute_of_day(params.entry_time) - _SESSION_START_MINUTE,
        exit_m=_minute_of_day(params.exit_time) - _SESSION_START_MINUTE,
        loss_pcts=tuple(float(v) for v in params.loss_limit_pct_by_attempt),
        cap=float(params.max_loss_limit_cap_rupees),
        max_re=int(params.max_reattempts),
        delays=tuple(int(v) for v in params.reentry_delay_by_attempt),
        pt=float(params.profit_target_pct),
        pt_close_only=(params.profit_target_fill == "CLOSE"),
        tgt_on=bool(params.profit_target_reentry_enabled),
        tgt_delay=int(params.profit_target_reentry_delay_min),
        pp_arm=float(params.profit_protect_arm_pct),
        pp_give=float(params.profit_protect_giveback_pct),
        be_arm=float(params.breakeven_arm_pct),
        be_lock=float(params.breakeven_lock_pct),
        ratio=float(params.reentry_max_premium_ratio),
        dll=float(params.max_daily_loss_rupees),
    )


def fast_simulate_day(fd: FastDay, fp: _FastParams, slippage_pts: float = 0.0) -> Tuple[float, int, bool]:
    """
    Simulate one day with the fast kernel.

    Returns (net_pnl_for_the_day, number_of_attempts, reattempt_cap_hit) where
    reattempt_cap_hit is True when MAX_REATTEMPTS stopped a re-entry that the
    rules would otherwise have taken.

    Every block mirrors the v3 reference loop in the same order and with the
    same arithmetic; comments name the reference step.
    """
    nan = math.nan
    n = fd.n
    end = min(n - 1, fp.exit_m)                      # trade_end_ts
    qty = fd.qty
    slip_cost = 4.0 * float(slippage_pts) * qty if slippage_pts > 0 else 0.0

    m = fp.entry_m
    if m >= end:                                     # entry at/after the cutoff
        return 0.0, 0, False

    seq = 1
    daily = 0.0
    n_trades = 0
    cap_hit = False
    dll = fp.dll
    dll_on = dll > 0
    loss_pcts = fp.loss_pcts
    delays = fp.delays
    u_asof = fd.u_asof
    legs = fd.legs
    e1 = end + 1                                     # inclusive label slice end
    prev_prem_unit: Optional[float] = None           # premium-ratio gate memory

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

        # v2 premium gate: do not re-sell into still-expanding volatility.
        if seq > 1 and fp.ratio > 0 and prev_prem_unit:
            if (ce0 + pe0) > (prev_prem_unit * fp.ratio):
                break
        prev_prem_unit = ce0 + pe0

        # Percentage-based risk basis for THIS attempt.
        prem = (ce0 + pe0) * qty                      # entry_premium_sum
        k = seq - 1
        if loss_pcts:
            sl_pct = loss_pcts[k] if k < len(loss_pcts) else loss_pcts[-1]
        else:
            sl_pct = 0.0
        uncapped = float(sl_pct * prem)
        loss_r = float(min(uncapped, fp.cap)) if fp.cap > 0 else uncapped

        # Close-based P&L from monitor start to the cutoff (inclusive).
        pnl = (ce0 - ce_ff[ms:e1]) * qty + (pe0 - pe_ff[ms:e1]) * qty

        # Worst intraminute P&L: min of (close, CE-high/PE-low, CE-low/PE-high).
        a = (ce0 - ce_hi[ms:e1]) * qty + (pe0 - pe_lo[ms:e1]) * qty
        b = (ce0 - ce_lo[ms:e1]) * qty + (pe0 - pe_hi[ms:e1]) * qty
        worst = np.fmin(np.fmin(pnl, a), b)

        peak = None
        # v2 breakeven ratchet: once the running close-based peak reaches
        # be_arm x premium, the stop floor lifts to max(-stop, be_lock x premium).
        if fp.be_arm > 0:
            peak = np.maximum.accumulate(pnl)
            be_armed = peak >= (fp.be_arm * prem)
            be_value = max(-float(loss_r), float(fp.be_lock * prem))
            floor = np.where(be_armed, be_value, -float(loss_r))
            hit = worst <= floor
        else:
            be_armed = None
            be_value = 0.0
            hit = worst <= -float(loss_r)
        i_stop = int(hit.argmax())
        if not hit[i_stop]:
            i_stop = -1

        # PROFIT_PROTECT with separate arm and giveback.
        i_prot = -1
        if fp.pp_arm > 0.0:
            if peak is None:
                peak = np.maximum.accumulate(pnl)
            arm_r = float(fp.pp_arm * prem)
            give_r = float(fp.pp_give * prem)
            phit = (peak >= arm_r) & (pnl <= peak - give_r)
            i_prot = int(phit.argmax())
            if not phit[i_prot]:
                i_prot = -1

        # PROFIT_TARGET: LOWLOW = max(close, CE-low + PE-low); CLOSE = close only.
        i_tgt = -1
        target_rupees = 0.0
        if fp.pt > 0.0:
            target_rupees = fp.pt * prem
            if fp.pt_close_only:
                tp = pnl
            else:
                best = (ce0 - ce_lo[ms:e1]) * qty + (pe0 - pe_lo[ms:e1]) * qty
                tp = np.fmax(pnl, best)
            thit = tp >= float(target_rupees)
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
            # Stop floor at the exit minute (a small profit if the ratchet armed).
            if be_armed is not None and be_armed[exit_i]:
                gross = float(be_value)
            else:
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

        # STOPLOSS / PROFIT_PROTECT -> re-entry after the ramp delay.
        if reason == 0 or reason == 2:
            if k < fp.max_re:
                if delays:
                    delay = delays[k] if k < len(delays) else delays[-1]
                else:
                    delay = 0
                seq += 1
                m = exit_m + delay
                if m >= end:
                    break
                continue
            cap_hit = True
            break

        # PROFIT_TARGET -> optional recycle; the premium gate memory is reset.
        if reason == 1 and fp.tgt_on:
            if k < fp.max_re:
                seq += 1
                m = exit_m + fp.tgt_delay
                prev_prem_unit = None
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


def _day_risk_stats(daily: np.ndarray) -> Dict[str, Any]:
    """
    [MODES] Day-level statistics behind the objective modes, from the net P&L
    of each TRADED day in chronological order (one index per date).

      win_days        days with net P&L > OPT_WIN_DAY_MIN_RUPEES
      loss_days       days with net P&L < 0
      max_drawdown    largest peak-to-trough fall of the cumulative daily P&L,
                      counted from a starting equity of 0 (Rs, >= 0)
      daily_sharpe    mean / std (ddof=1) of daily P&L; 0 if < 2 days or std 0
      return_over_dd  total net / max(max_drawdown, OPT_CALMAR_DD_FLOOR)
    """
    daily = np.asarray(daily, dtype=np.float64)
    if daily.size == 0:
        return {"win_days": 0, "loss_days": 0, "max_drawdown": 0.0,
                "daily_sharpe": 0.0, "return_over_dd": 0.0}
    equity = np.cumsum(daily)
    peak = np.maximum.accumulate(np.concatenate(([0.0], equity)))[1:]
    max_dd = float(max(0.0, np.max(peak - equity)))
    std = float(daily.std(ddof=1)) if daily.size >= 2 else 0.0
    return {
        "win_days": int((daily > OPT_WIN_DAY_MIN_RUPEES).sum()),
        "loss_days": int((daily < 0).sum()),
        "max_drawdown": max_dd,
        "daily_sharpe": float(daily.mean() / std) if std > 0 else 0.0,
        "return_over_dd": float(daily.sum() / max(max_dd, OPT_CALMAR_DD_FLOOR)),
    }


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
        base.update(_day_risk_stats(daily))          # [MODES]
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
    base.update(_day_risk_stats(daily))              # [MODES]
    return base


# =============================================================================


# =============================================================================
# DATA LOADING + LOCAL CACHE (optimizer only)
# =============================================================================
# Without a cache, every start re-runs three slow phases:
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
_CACHE_FORMAT_VERSION = 2


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
            "max_days": max_days,
        },
        sort_keys=True,
    )
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]
    return os.path.join(OPT_OUTPUT_DIR, "data_cache", f"v3dte_days_{digest}.pkl")


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


def _dte_report(
    min_expiry_map: Dict[Tuple[str, date], date],
    window_start: date,
    end_day: date,
) -> None:
    """
    Print how many (underlying, day) pairs the pickles contain per
    days-to-expiry, and stop early if none match ALLOWED_DTE.

    The DTE of a day is (nearest expiry - day) in CALENDAR days, exactly as the
    strategy computes it, e.g. NIFTY Tuesday expiry: Monday = DTE 1, Friday =
    DTE 4 (the weekend counts).
    """
    counts: Dict[int, int] = {}
    for (_und, dy), expiry in min_expiry_map.items():
        if window_start <= dy <= end_day:
            dte = int((expiry - dy).days)
            counts[dte] = counts.get(dte, 0) + 1
    found = ", ".join(f"DTE {k}: {v}" for k, v in sorted(counts.items())) or "none"
    traded = [
        dy for dy in pick_actual_underlying_by_day(min_expiry_map)
        if window_start <= dy <= end_day
    ]
    print(
        f"[PHASE 1] (underlying, day) pairs in the pickles by days-to-expiry: {found} | "
        f"ALLOWED_DTE={ALLOWED_DTE} -> {len(traded)} tradable day(s)",
        flush=True,
    )
    if not traded:
        raise RuntimeError(
            f"No day in PICKLES_DIR has a days-to-expiry in ALLOWED_DTE={ALLOWED_DTE} "
            f"(found {found}). If only DTE 0 is listed, this folder holds expiry-day "
            f"data only (files named ..._DTE0_...); point PICKLES_DIR at a folder whose "
            f"pickles include the days before expiry (e.g. ..._DTE0_1_2_... files). "
            f"PICKLES_DIR={PICKLES_DIR}"
        )


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

    # Check BEFORE the Kite download and the second pickle read: does the data
    # contain any day whose days-to-expiry is in ALLOWED_DTE? A folder of
    # expiry-day-only pickles (..._DTE0_...) has none for DTE 1 or 2.
    _dte_report(min_expiry_map, window_start, end_day)

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
    Fix the effective activity guard for this run from the in-sample data.

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


def _mode_score(metrics: Dict[str, Any]) -> float:
    """
    [MODES] Raw score of the selected OPT_OBJECTIVE (higher is better).

    win_days: count + tie-break. The tie-break is total net P&L squeezed into
    (-0.5, +0.5), so it can reorder configs with the SAME number of winning
    days but can never outweigh one extra winning day.
    """
    if OPT_OBJECTIVE == "win_days":
        tie = 0.5 * math.tanh(float(metrics["total_pnl"]) / 1.0e7)
        return float(metrics["win_days"]) + tie
    if OPT_OBJECTIVE == "calmar":
        return float(metrics["return_over_dd"])
    if OPT_OBJECTIVE == "sharpe":
        return float(metrics["daily_sharpe"])
    return float(metrics["total_pnl"])


def _guard_shortfall(metrics: Dict[str, Any]) -> float:
    """
    [MODES] Total amount (Rs) by which a trial misses the OPT_MIN_*/OPT_MAX_*
    guards; 0.0 = all guards met.
    """
    miss = 0.0
    if _NET_FLOOR is not None and metrics["total_pnl"] < _NET_FLOOR:
        miss += _NET_FLOOR - float(metrics["total_pnl"])
    if OPT_MAX_LOSS_DAY is not None and metrics["worst_day"] < -abs(OPT_MAX_LOSS_DAY):
        miss += -abs(OPT_MAX_LOSS_DAY) - float(metrics["worst_day"])
    if OPT_MAX_DRAWDOWN is not None and metrics["max_drawdown"] > abs(OPT_MAX_DRAWDOWN):
        miss += float(metrics["max_drawdown"]) - abs(OPT_MAX_DRAWDOWN)
    return miss


def _objective_from_metrics(metrics: Dict[str, Any], cv_folds: int) -> float:
    """[MODES] Activity guard -> OPT_MIN/OPT_MAX guards -> mode score."""
    if OPT_OBJECTIVE == "profit":
        # Unchanged previous objective (including the optional CV folds).
        score = (_cv_score_from_metrics(metrics, cv_folds) if cv_folds > 1
                 else _score_from_metrics(metrics))
    else:
        min_days, min_months = _activity_minima()
        if metrics["n_days"] < min_days or metrics["n_months"] < min_months:
            return _OPT_DISQUALIFY
        score = _mode_score(metrics)
    if score <= _OPT_DISQUALIFY:
        return score
    miss = _guard_shortfall(metrics)
    if miss > 0.0:
        # Below every valid trial; a smaller miss ranks higher, so the
        # sampler is still pulled toward configurations that meet the guards.
        return _OPT_INFEASIBLE - miss
    return score


def evaluate_fast(
    params: Params,
    dayset: DaySet,
    slippage_pts: Dict[str, float],
    cv_folds: int,
) -> Dict[str, Any]:
    """One complete fast evaluation: simulate, summarise, score."""
    pnl, trades, caps = fast_simulate(_fast_params(params), dayset.days, slippage_pts)
    metrics = _summarise_fast(pnl, trades, caps, dayset)
    metrics["objective"] = _objective_from_metrics(metrics, cv_folds)   # [MODES]
    return metrics


def _fmt_objective(value: Any) -> str:
    """[MODES] Human-readable objective value for the selected mode."""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return "n/a"
    if v <= _OPT_DISQUALIFY:
        return "DISQUALIFIED (activity guard)"
    if v <= _OPT_INFEASIBLE:
        return f"GUARD MISS by {_inr(_OPT_INFEASIBLE - v)}"
    if OPT_OBJECTIVE == "win_days":
        return f"{int(round(v))} win days"   # tie-break part is always < 0.5
    if OPT_OBJECTIVE == "calmar":
        return f"{v:.3f}x net/DD"
    if OPT_OBJECTIVE == "sharpe":
        return f"{v:.4f} daily Sharpe"
    return _inr(v)


def _mode_label() -> str:
    """[MODES] One-line description of the active objective and guards."""
    parts = {
        "profit": "maximize total net P&L",
        "win_days": f"maximize profitable days (day net > {_inr(OPT_WIN_DAY_MIN_RUPEES)}), "
                    "tie-break by net P&L",
        "calmar": f"maximize net P&L / max drawdown (DD floor {_inr(OPT_CALMAR_DD_FLOOR)})",
        "sharpe": "maximize mean / std of daily net P&L",
    }
    guards = []
    if _NET_FLOOR is not None:
        guards.append(f"net >= {_inr(_NET_FLOOR)}")
    if OPT_MAX_LOSS_DAY is not None:
        guards.append(f"worst day >= -{_inr(abs(OPT_MAX_LOSS_DAY))}")
    if OPT_MAX_DRAWDOWN is not None:
        guards.append(f"max drawdown <= {_inr(abs(OPT_MAX_DRAWDOWN))}")
    text = f"OPT_OBJECTIVE={OPT_OBJECTIVE}: {parts[OPT_OBJECTIVE]}"
    return text + (f" | guards: {', '.join(guards)}" if guards else " | guards: none")


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
            **_day_risk_stats(np.zeros(0)),          # [MODES]
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
        **_day_risk_stats(daily.to_numpy(dtype=np.float64)),   # [MODES] (chronological)
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
    The stop-loss % list is searched as ONE multiplier on the property-file
    list (only with OPT_TUNE_STOP_PCT=1). scale=1.0 returns the file's list
    unchanged (exact seed). Each value is capped at 0.95 because the
    strategy's _parse_pct_value() treats anything above 1.0 as a percent.
    """
    return [_round_param(min(_STOP_PCT_UPPER, float(v) * float(scale))) for v in base_list]


def _ramp_schedule(base: int, step: int, length: int) -> List[int]:
    """
    Re-entry delay ramp: [base, base+step, ..., base+(length-1)*step].

    The strategy reuses the LAST list value for every later re-entry, so this
    list has exactly the strategy's semantics. Examples:
        base 7,  step 3, length 6  -> 7, 10, 13, 16, 19, 22          (DTE-1)
        base 20, step 3, length 10 -> 20, 23, ..., 47                (DTE-2)
    """
    ramp = [int(base) + int(step) * i for i in range(max(1, int(length)))]
    return _effective_list(ramp)   # step 0 -> one value (identical behaviour)


# On/off feature switches of the v3 search space. Their dependent settings
# (e.g. the breakeven arm/lock) exist in a trial only when the switch is 1.
_SWITCH_PARAMS = (
    "profit_target_reentry_enabled",
    "profit_protect_enabled",
    "breakeven_enabled",
    "premium_gate_enabled",
)
# Switches pinned to one value (used by the CMA-ES phases, which need a fixed
# set of parameters). Empty = every switch is searched normally.
_PINNED_SWITCHES: Dict[str, int] = {}


def _switch(trial, name: str) -> int:
    """Suggest an on/off switch, or pin it (single-valued) when requested."""
    if name in _PINNED_SWITCHES:
        value = int(_PINNED_SWITCHES[name])
        return int(trial.suggest_int(name, value, value))
    return int(trial.suggest_int(name, 0, 1))


def _params_from_trial(trial, base: Params) -> Params:
    """
    Map one trial to a complete v3 parameter object.

    Conditional parameters (only suggested when their switch is on):
      * protect arm/giveback  <- profit_protect_enabled = 1
      * breakeven arm/lock    <- breakeven_enabled = 1
      * premium ratio         <- premium_gate_enabled = 1
      * target re-entry delay <- PROFIT_TARGET_REENTRY_ENABLED = 1
    A switched-off feature is written as 0 in the .properties file, which is
    exactly how the strategy script disables it.
    """
    # ---- Times -------------------------------------------------------------------
    entry_time = _bounded_time_suggestion(
        trial, name="entry_time_offset", from_ist=OPT_ENTRY_FROM_IST, to_ist=OPT_ENTRY_TO_IST,
        step_minutes=OPT_ENTRY_STEP_MIN, base_value=base.entry_time, enabled=OPT_TUNE_TIMES,
    )
    exit_time = _bounded_time_suggestion(
        trial, name="exit_time_offset", from_ist=OPT_EXIT_FROM_IST, to_ist=OPT_EXIT_TO_IST,
        step_minutes=OPT_EXIT_STEP_MIN, base_value=base.exit_time, enabled=OPT_TUNE_TIMES,
    )
    if entry_time >= exit_time:
        # Cannot happen with the default windows; protects custom ranges.
        raise ValueError(
            f"Invalid trial: entry {entry_time.strftime('%H:%M')} must precede "
            f"exit {exit_time.strftime('%H:%M')}"
        )

    # ---- Stop-loss ---------------------------------------------------------------
    if OPT_TUNE_STOPLOSS:
        stop_cap = float(trial.suggest_int(
            "max_stop_loss_rupees", OPT_STOP_CAP_MIN, OPT_STOP_CAP_MAX, step=OPT_STOP_CAP_STEP,
        ))
    else:
        stop_cap = float(base.max_loss_limit_cap_rupees)
    if OPT_TUNE_STOP_PCT:
        stop_scale = _round_param(trial.suggest_float(
            "stop_loss_pct_scale", OPT_STOP_SCALE_MIN, OPT_STOP_SCALE_MAX,
        ))
        stop_schedule = _scaled_stop_schedule(base.loss_limit_pct_by_attempt, stop_scale)
    else:
        stop_schedule = list(base.loss_limit_pct_by_attempt)

    # ---- Target re-entry switch (frozen by default) --------------------------------
    if OPT_TUNE_SWITCHES:
        target_reentry_enabled = bool(_switch(trial, "profit_target_reentry_enabled"))
    else:
        target_reentry_enabled = bool(base.profit_target_reentry_enabled)

    # ---- Re-entry ----------------------------------------------------------------
    if OPT_TUNE_REENTRY:
        max_reattempts = trial.suggest_int("max_reattempts", OPT_REATTEMPTS_MIN, OPT_REATTEMPTS_MAX)
        ramp_base = trial.suggest_int("reentry_delay_base_min", OPT_REENTRY_BASE_MIN, OPT_REENTRY_BASE_MAX)
        ramp_step = trial.suggest_int("reentry_delay_step_min", OPT_REENTRY_STEP_MIN, OPT_REENTRY_STEP_MAX)
        ramp_len = trial.suggest_int("reentry_delay_ramp_length", OPT_REENTRY_LENGTH_MIN, OPT_REENTRY_LENGTH_MAX)
        delay_schedule = _ramp_schedule(ramp_base, ramp_step, ramp_len)
        target_reentry_delay = (
            trial.suggest_int(
                "profit_target_reentry_delay_min",
                OPT_TARGET_REENTRY_DELAY_MIN, OPT_TARGET_REENTRY_DELAY_MAX,
            )
            if target_reentry_enabled else int(base.profit_target_reentry_delay_min)
        )
    else:
        max_reattempts = int(base.max_reattempts)
        delay_schedule = list(base.reentry_delay_by_attempt)
        target_reentry_delay = int(base.profit_target_reentry_delay_min)

    # ---- Profit target -----------------------------------------------------------
    if OPT_TUNE_TARGET:
        target_pct = _round_param(trial.suggest_float("profit_target_pct", OPT_TARGET_MIN, OPT_TARGET_MAX))
    else:
        target_pct = float(base.profit_target_pct)

    # ---- Profit-protect (on/off + arm + giveback) ---------------------------------
    if OPT_TUNE_PROTECT:
        if _switch(trial, "profit_protect_enabled"):
            pp_arm = _round_param(trial.suggest_float(
                "profit_protect_arm_pct", OPT_PROTECT_ARM_MIN, OPT_PROTECT_ARM_MAX,
            ))
            pp_give = _round_param(trial.suggest_float(
                "profit_protect_giveback_pct", OPT_PROTECT_GIVEBACK_MIN, OPT_PROTECT_GIVEBACK_MAX,
            ))
        else:
            pp_arm, pp_give = 0.0, 0.0
    else:
        pp_arm = float(base.profit_protect_arm_pct)
        pp_give = float(base.profit_protect_giveback_pct)

    # ---- Breakeven ratchet (on/off + arm + lock, lock clipped to <= arm) ----------
    if OPT_TUNE_BREAKEVEN:
        if _switch(trial, "breakeven_enabled"):
            be_arm = _round_param(trial.suggest_float(
                "breakeven_arm_pct", OPT_BREAKEVEN_ARM_MIN, OPT_BREAKEVEN_ARM_MAX,
            ))
            be_lock_raw = _round_param(trial.suggest_float(
                "breakeven_lock_pct", OPT_BREAKEVEN_LOCK_MIN, OPT_BREAKEVEN_LOCK_MAX,
            ))
            be_lock = min(be_lock_raw, be_arm)
        else:
            be_arm, be_lock = 0.0, 0.0
    else:
        be_arm = float(base.breakeven_arm_pct)
        be_lock = float(base.breakeven_lock_pct)

    # ---- Re-entry premium gate (on/off + ratio) ------------------------------------
    if OPT_TUNE_PREMIUM_GATE:
        if _switch(trial, "premium_gate_enabled"):
            ratio = _round_param(trial.suggest_float(
                "reentry_max_premium_ratio", OPT_PREMIUM_RATIO_MIN, OPT_PREMIUM_RATIO_MAX,
            ))
        else:
            ratio = 0.0
    else:
        ratio = float(base.reentry_max_premium_ratio)

    # ---- Daily loss limit --------------------------------------------------------
    if OPT_TUNE_DAILY_LOSS:
        daily_loss = float(trial.suggest_int(
            "max_daily_loss_rupees", OPT_DAILY_LOSS_MIN, OPT_DAILY_LOSS_MAX, step=OPT_DAILY_LOSS_STEP,
        ))
    else:
        daily_loss = float(base.max_daily_loss_rupees)

    return Params(
        entry_time=entry_time,
        exit_time=exit_time,
        loss_limit_pct_by_attempt=stop_schedule,
        max_loss_limit_cap_rupees=stop_cap,
        max_reattempts=int(max_reattempts),
        reentry_delay_by_attempt=delay_schedule,
        profit_target_pct=float(target_pct),
        profit_target_fill=base.profit_target_fill,        # fill model: never searched
        profit_target_reentry_enabled=bool(target_reentry_enabled),
        profit_target_reentry_delay_min=int(target_reentry_delay),
        profit_protect_trigger_pct=base.profit_protect_trigger_pct,   # report-only value
        profit_protect_arm_pct=float(pp_arm),
        profit_protect_giveback_pct=float(pp_give),
        breakeven_arm_pct=float(be_arm),
        breakeven_lock_pct=float(be_lock),
        reentry_max_premium_ratio=float(ratio),
        max_daily_loss_rupees=daily_loss,
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
    Records every distribution _params_from_trial() asks for.

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
    The engine never monitors past SESSION_END_IST (15:30), so EXIT_TIME_IST
    values after it (the property file has 15:40) behave exactly like 15:30.
    Using the clamped value lets the seed sit inside the exit-time range.
    """
    if base.exit_time > SESSION_END_IST:
        print(
            f"[SEED] EXIT_TIME_IST {base.exit_time.strftime('%H:%M')} is after the "
            f"session end; the engine treats it as {SESSION_END_IST.strftime('%H:%M')} "
            "(identical results). Using 15:30 for the optimizer baseline.",
            flush=True,
        )
        base.exit_time = SESSION_END_IST
    return base


def _apply_objective_fill(base: Params) -> Params:
    """
    OPT_TARGET_FILL lets you optimize under the conservative CLOSE target check
    without editing PROFIT_TARGET_FILL in the config. The baseline, every trial
    and the written .properties then all use that fill model, so they stay
    comparable and the BEST file reproduces the optimizer's number.
    """
    if not OPT_TARGET_FILL_RAW:
        print(f"[FILL] objective uses PROFIT_TARGET_FILL={base.profit_target_fill} (from the config).",
              flush=True)
        return base
    if OPT_TARGET_FILL_RAW not in ("LOWLOW", "CLOSE"):
        raise ValueError(f"OPT_TARGET_FILL must be LOWLOW or CLOSE, not {OPT_TARGET_FILL_RAW!r}")
    if OPT_TARGET_FILL_RAW != base.profit_target_fill:
        print(
            f"[FILL] OPT_TARGET_FILL={OPT_TARGET_FILL_RAW} overrides PROFIT_TARGET_FILL="
            f"{base.profit_target_fill}: the baseline below is your config scored with "
            f"{OPT_TARGET_FILL_RAW} target detection.",
            flush=True,
        )
        base.profit_target_fill = OPT_TARGET_FILL_RAW
    return base


def _base_trial_values(base: Params) -> Tuple[Dict[str, Any], List[str]]:
    """
    Convert the property-file configuration into this search space's values.

    Returns (seed_values, notes). Both v3 configs are representable exactly:
    the re-entry list is a linear ramp, and each on/off feature maps to its
    switch (arm/ratio = 0 means off). Notes list anything that is not exact.
    """
    values: Dict[str, Any] = {}
    notes: List[str] = []

    def time_offset(value: dtime, start_text: str) -> int:
        return _time_to_minutes(value) - _time_to_minutes(parse_hhmm(start_text))

    if OPT_TUNE_TIMES:
        values["entry_time_offset"] = time_offset(base.entry_time, OPT_ENTRY_FROM_IST)
        values["exit_time_offset"] = time_offset(base.exit_time, OPT_EXIT_FROM_IST)

    if OPT_TUNE_STOPLOSS:
        values["max_stop_loss_rupees"] = int(round(base.max_loss_limit_cap_rupees))
    if OPT_TUNE_STOP_PCT:
        values["stop_loss_pct_scale"] = 1.0

    if OPT_TUNE_SWITCHES:
        values["profit_target_reentry_enabled"] = 1 if base.profit_target_reentry_enabled else 0

    if OPT_TUNE_REENTRY:
        values["max_reattempts"] = int(base.max_reattempts)
        delays = list(base.reentry_delay_by_attempt) or [0]
        # Represent the list as base + step ramp. Trailing repeats of the last
        # value are dropped first (they change nothing: the strategy reuses the
        # last value anyway), e.g. 5,7,9,9,9 -> base 5, step 2, length 3.
        trimmed = list(delays)
        while len(trimmed) > 1 and trimmed[-1] == trimmed[-2]:
            trimmed.pop()
        ramp_base = int(trimmed[0])
        ramp_step = int(trimmed[1] - trimmed[0]) if len(trimmed) > 1 else 0
        ramp_len = len(trimmed)
        if _ramp_schedule(ramp_base, ramp_step, ramp_len) != trimmed:
            notes.append(
                f"REENTRY_DELAY_BY_ATTEMPT={delays} is not a linear ramp; the seed uses "
                f"{_ramp_schedule(ramp_base, ramp_step, ramp_len)} instead."
            )
        values["reentry_delay_base_min"] = ramp_base
        values["reentry_delay_step_min"] = ramp_step
        values["reentry_delay_ramp_length"] = ramp_len
        if base.profit_target_reentry_enabled:
            values["profit_target_reentry_delay_min"] = int(base.profit_target_reentry_delay_min)

    if OPT_TUNE_TARGET:
        values["profit_target_pct"] = float(base.profit_target_pct)

    if OPT_TUNE_PROTECT:
        if base.profit_protect_arm_pct > 0:
            values["profit_protect_enabled"] = 1
            values["profit_protect_arm_pct"] = float(base.profit_protect_arm_pct)
            values["profit_protect_giveback_pct"] = float(base.profit_protect_giveback_pct)
        else:
            values["profit_protect_enabled"] = 0

    if OPT_TUNE_BREAKEVEN:
        if base.breakeven_arm_pct > 0:
            values["breakeven_enabled"] = 1
            values["breakeven_arm_pct"] = float(base.breakeven_arm_pct)
            values["breakeven_lock_pct"] = float(base.breakeven_lock_pct)
            if base.breakeven_lock_pct > base.breakeven_arm_pct:
                notes.append(
                    "BREAKEVEN_LOCK_PCT is above BREAKEVEN_ARM_PCT; the search clips the "
                    "lock to the arm level."
                )
        else:
            values["breakeven_enabled"] = 0

    if OPT_TUNE_PREMIUM_GATE:
        if base.reentry_max_premium_ratio > 0:
            values["premium_gate_enabled"] = 1
            values["reentry_max_premium_ratio"] = float(base.reentry_max_premium_ratio)
        else:
            values["premium_gate_enabled"] = 0

    if OPT_TUNE_DAILY_LOSS:
        values["max_daily_loss_rupees"] = int(round(base.max_daily_loss_rupees))

    return values, notes


def _validate_seed(base: Params, values: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
    """
    Check every seed value against the distribution the search actually uses
    (range AND step grid).

    A value outside your OPT_* limits is not dropped (Optuna would then fill
    it with a RANDOM value); it is CLAMPED to the nearest
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


def _effective_list(values: List[Any]) -> List[Any]:
    """Drop trailing repeats: the strategy reuses the last value anyway."""
    out = list(values)
    while len(out) > 1 and out[-1] == out[-2]:
        out.pop()
    return out


def _param_differences(a: Params, b: Params) -> List[str]:
    """Human-readable list of fields where two parameter sets BEHAVE differently."""
    diffs: List[str] = []
    for name in (
        "entry_time", "exit_time", "max_loss_limit_cap_rupees", "max_reattempts",
        "profit_target_pct", "profit_target_fill", "profit_target_reentry_enabled",
        "profit_protect_arm_pct", "breakeven_arm_pct", "reentry_max_premium_ratio",
        "max_daily_loss_rupees",
    ):
        va, vb = getattr(a, name), getattr(b, name)
        if va != vb:
            diffs.append(f"{name}: {va} vs {vb}")
    # Values that only matter when their feature is on.
    if a.profit_target_reentry_enabled and (
        a.profit_target_reentry_delay_min != b.profit_target_reentry_delay_min
    ):
        diffs.append(
            f"profit_target_reentry_delay_min: {a.profit_target_reentry_delay_min} "
            f"vs {b.profit_target_reentry_delay_min}"
        )
    if a.profit_protect_arm_pct > 0 and a.profit_protect_giveback_pct != b.profit_protect_giveback_pct:
        diffs.append(
            f"profit_protect_giveback_pct: {a.profit_protect_giveback_pct} "
            f"vs {b.profit_protect_giveback_pct}"
        )
    if a.breakeven_arm_pct > 0 and a.breakeven_lock_pct != b.breakeven_lock_pct:
        diffs.append(f"breakeven_lock_pct: {a.breakeven_lock_pct} vs {b.breakeven_lock_pct}")
    for name in ("loss_limit_pct_by_attempt", "reentry_delay_by_attempt"):
        va, vb = _effective_list(getattr(a, name)), _effective_list(getattr(b, name))
        if va != vb:
            diffs.append(f"{name}: {va} vs {vb}")
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

    The block loads directly into atm_straddle_backtest_v3_new_claude.py.
    Floats are written with 6 decimals, which is exactly the precision used
    inside the optimizer, so the backtest reproduces the optimizer's number.
    Switched-off features are written as 0, which is how the strategy turns
    them off (PROFIT_PROTECT_ARM_PCT=0, BREAKEVEN_ARM_PCT=0,
    REENTRY_MAX_PREMIUM_RATIO=0).
    """
    rows: List[str] = [
        "# Generated by the v3 DTE optimizer. Verify with "
        "atm_straddle_backtest_v3_new_claude.py before any live use.",
    ]
    if include_run_control:
        rows.append("RUN_MODE=backtest")

    rows.extend([
        f"PICKLES_DIR={PICKLES_DIR}",
        f"ENTRY_TIME_IST={params.entry_time.strftime('%H:%M')}",
        f"EXIT_TIME_IST={params.exit_time.strftime('%H:%M')}",
        "ALLOWED_DTE=" + ", ".join(str(value) for value in ALLOWED_DTE),
        "",
        "# ---- Stop-loss ----",
        "LOSS_LIMIT_RUPEES_BY_ATTEMPT="
        + ", ".join(f"{value:.6f}" for value in params.loss_limit_pct_by_attempt),
        f"MAX_LOSS_LIMIT_RUPEES_BY_ATTEMPT={int(round(params.max_loss_limit_cap_rupees))}",
        "",
        "# ---- Profit-protect (ARM 0 = off) ----",
        f"PROFIT_PROTECT_TRIGGER_RUPEES={params.profit_protect_trigger_pct:.6f}",
        f"PROFIT_PROTECT_ARM_PCT={params.profit_protect_arm_pct:.6f}",
        f"PROFIT_PROTECT_GIVEBACK_PCT={params.profit_protect_giveback_pct:.6f}",
        "",
        "# ---- Breakeven ratchet (ARM 0 = off) ----",
        f"BREAKEVEN_ARM_PCT={params.breakeven_arm_pct:.6f}",
        f"BREAKEVEN_LOCK_PCT={params.breakeven_lock_pct:.6f}",
        "",
        "# ---- Profit target ----",
        f"PROFIT_TARGET_PCT={params.profit_target_pct:.6f}",
        f"PROFIT_TARGET_REENTRY_ENABLED={1 if params.profit_target_reentry_enabled else 0}",
        f"PROFIT_TARGET_REENTRY_DELAY={params.profit_target_reentry_delay_min}",
        f"PROFIT_TARGET_FILL={params.profit_target_fill}",
        "",
        "# ---- Daily circuit breaker / re-entries ----",
        f"MAX_DAILY_LOSS_RUPEES={int(round(params.max_daily_loss_rupees))}",
        f"MAX_REATTEMPTS={params.max_reattempts}",
        "REENTRY_DELAY_BY_ATTEMPT="
        + ", ".join(str(value) for value in params.reentry_delay_by_attempt),
        f"REENTRY_MAX_PREMIUM_RATIO={params.reentry_max_premium_ratio:.6f}",
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
    # [MODES]
    "win_days", "loss_days", "max_drawdown", "daily_sharpe", "return_over_dd",
)

# Column order of the per-trial CSV log. Must list every key _trial_record()
# returns (the CSV writer rejects unknown keys).
_TRIAL_CSV_FIELDS = (
    "run_index", "phase", "trial_number", "state", "objective_score", "net_pnl",
    "n_trades", "cap_hit_days", "mean_month", "median_month", "worst_month",
    "worst_day", "profitable_month_ratio", "profitable_day_ratio", "n_months",
    "n_days", "win_days", "loss_days", "max_drawdown", "daily_sharpe",
    "return_over_dd", "entry_time", "exit_time", "stop_cap", "max_reattempts",
    "reentry_delay", "target_reentry_delay", "profit_target_pct",
    "protect_arm_pct", "protect_giveback_pct", "breakeven_arm_pct",
    "breakeven_lock_pct", "premium_ratio", "daily_loss", "params_json",
    "properties_json", "elapsed_seconds",
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
        # [MODES]
        "win_days": int(metrics.get("win_days", 0)),
        "loss_days": int(metrics.get("loss_days", 0)),
        "max_drawdown": round(float(metrics.get("max_drawdown", 0.0)), 2),
        "daily_sharpe": round(float(metrics.get("daily_sharpe", 0.0)), 6),
        "return_over_dd": round(float(metrics.get("return_over_dd", 0.0)), 6),
        "entry_time": params.entry_time.strftime("%H:%M"),
        "exit_time": params.exit_time.strftime("%H:%M"),
        "stop_cap": int(round(params.max_loss_limit_cap_rupees)),
        "max_reattempts": params.max_reattempts,
        "reentry_delay": ";".join(str(v) for v in params.reentry_delay_by_attempt),
        "target_reentry_delay": params.profit_target_reentry_delay_min,
        "profit_target_pct": params.profit_target_pct,
        "protect_arm_pct": params.profit_protect_arm_pct,
        "protect_giveback_pct": params.profit_protect_giveback_pct,
        "breakeven_arm_pct": params.breakeven_arm_pct,
        "breakeven_lock_pct": params.breakeven_lock_pct,
        "premium_ratio": params.reentry_max_premium_ratio,
        "daily_loss": int(round(params.max_daily_loss_rupees)),
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
    problem. The baseline config is part of it because frozen values, the
    stop % list and the fill model are taken from it.
    """
    return json.dumps(
        {
            "version": "V3_DTE_1",
            "baseline": params_to_properties(base, include_run_control=False),
            "tune": [OPT_TUNE_TIMES, OPT_TUNE_STOPLOSS, OPT_TUNE_STOP_PCT, OPT_TUNE_REENTRY,
                     OPT_TUNE_TARGET, OPT_TUNE_PROTECT, OPT_TUNE_BREAKEVEN,
                     OPT_TUNE_PREMIUM_GATE, OPT_TUNE_DAILY_LOSS, OPT_TUNE_SWITCHES],
            "entry_range": [OPT_ENTRY_FROM_IST, OPT_ENTRY_TO_IST, OPT_ENTRY_STEP_MIN],
            "exit_range": [OPT_EXIT_FROM_IST, OPT_EXIT_TO_IST, OPT_EXIT_STEP_MIN],
            "stop_range": [OPT_STOP_CAP_MIN, OPT_STOP_CAP_MAX, OPT_STOP_CAP_STEP,
                           OPT_STOP_SCALE_MIN, OPT_STOP_SCALE_MAX],
            "reentry_range": [OPT_REATTEMPTS_MIN, OPT_REATTEMPTS_MAX,
                              OPT_REENTRY_BASE_MIN, OPT_REENTRY_BASE_MAX,
                              OPT_REENTRY_STEP_MIN, OPT_REENTRY_STEP_MAX,
                              OPT_REENTRY_LENGTH_MIN, OPT_REENTRY_LENGTH_MAX,
                              OPT_TARGET_REENTRY_DELAY_MIN, OPT_TARGET_REENTRY_DELAY_MAX],
            "exit_rule_range": [OPT_TARGET_MIN, OPT_TARGET_MAX,
                                OPT_PROTECT_ARM_MIN, OPT_PROTECT_ARM_MAX,
                                OPT_PROTECT_GIVEBACK_MIN, OPT_PROTECT_GIVEBACK_MAX,
                                OPT_BREAKEVEN_ARM_MIN, OPT_BREAKEVEN_ARM_MAX,
                                OPT_BREAKEVEN_LOCK_MIN, OPT_BREAKEVEN_LOCK_MAX,
                                OPT_PREMIUM_RATIO_MIN, OPT_PREMIUM_RATIO_MAX,
                                OPT_DAILY_LOSS_MIN, OPT_DAILY_LOSS_MAX, OPT_DAILY_LOSS_STEP],
            "objective": [OPT_CV_FOLDS, OPT_CV_PENALTY, OPT_MIN_DAYS, OPT_MIN_MONTHS,
                          OPT_HOLDOUT_FROM_RAW, OPT_SLIPPAGE_PTS],
            **_mode_signature(),
        },
        sort_keys=True,
    )


def _mode_signature() -> Dict[str, Any]:
    """
    [MODES] Objective-mode part of the study signature. Empty for the default
    (profit, no guards), so studies created before modes existed still resume;
    any other mode or guard makes it a different problem (new study needed).
    """
    settings = {
        "mode": OPT_OBJECTIVE,
        "win_day_min": OPT_WIN_DAY_MIN_RUPEES if OPT_OBJECTIVE == "win_days" else None,
        "calmar_floor": OPT_CALMAR_DD_FLOOR if OPT_OBJECTIVE == "calmar" else None,
        "min_net": OPT_MIN_NET_PNL,
        "min_net_vs_base": OPT_MIN_NET_VS_BASELINE,
        "max_loss_day": OPT_MAX_LOSS_DAY,
        "max_drawdown": OPT_MAX_DRAWDOWN,
    }
    is_default = OPT_OBJECTIVE == "profit" and all(
        settings[k] is None for k in ("min_net", "min_net_vs_base", "max_loss_day", "max_drawdown")
    )
    return {} if is_default else {"objective_mode": settings}


def _data_fingerprint(days: List[FastDay]) -> str:
    """Identifies the in-sample day list (a resumed study must use the same days)."""
    text = ";".join(f"{fd.und}:{fd.dy.isoformat()}" for fd in days)
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:16]


def _run_cmaes_refinement(optuna, study, objective, progress_callback, state, *, n_trials: int, seed: int) -> None:
    """
    CMA-ES refinement around the best trial so far.

    WHY THE SWITCHES ARE PINNED
      CMA-ES can only tune parameters that exist in EVERY trial. In the v3
      space the protect / breakeven / premium-gate settings exist only when
      their switch is on, so a plain CMA-ES phase would tune the switches but
      draw those settings at random. Instead:
        1. pin every on/off switch to the best trial's value;
        2. run CMA-ES in a separate in-memory study, where every trial has the
           same parameter set, so ALL active settings are tuned together;
        3. warm-start it (WS-CMA-ES) from the main-study trials that share the
           best trial's switch pattern (needs at least 10 of them);
        4. the first refinement trial re-evaluates the best trial itself, so
           CMA-ES starts from it even when a warm start is not possible;
        5. copy every finished refinement trial into the main study, so the
           trial log, the top-trials table and the winner selection see them.
    Pinned bounds (an OPT_*_MIN equal to its _MAX) are single-valued and are
    left out of CMA-ES's space automatically.
    """
    try:
        best = study.best_trial
    except ValueError:
        print("[OPT] CMA-ES refinement skipped: no completed trial to refine.", flush=True)
        return

    pinned = {name: int(best.params[name]) for name in _SWITCH_PARAMS if name in best.params}
    switch_names = set(_SWITCH_PARAMS)

    def active_params(trial) -> set:
        return {
            name for name, dist in trial.distributions.items()
            if not dist.single() and name not in switch_names
        }

    best_active = active_params(best)
    warm = []
    for trial in study.trials:
        if trial.state != optuna.trial.TrialState.COMPLETE or trial.value is None:
            continue
        if trial.value <= _OPT_INFEASIBLE:      # [MODES] also skips guard misses
            continue
        if any(int(trial.params.get(name, -1)) != value for name, value in pinned.items()):
            continue
        if active_params(trial) != best_active:
            continue
        warm.append(optuna.trial.create_trial(
            params={name: trial.params[name] for name in best_active},
            distributions={name: trial.distributions[name] for name in best_active},
            value=trial.value,
        ))

    pattern = ", ".join(f"{name}={value}" for name, value in sorted(pinned.items())) or "none"
    if len(warm) >= 10:
        start_text = f"warm-started from {len(warm)} trial(s) with the same switch pattern"
    else:
        start_text = (
            f"started at the best trial ({len(warm)} trial(s) share its switch pattern; "
            "at least 10 are needed for a warm start)"
        )
        warm = []
    print(
        f"[OPT] refinement phase: {n_trials} CMA-ES trial(s) around trial {best.number}, "
        f"switches pinned ({pattern}), {start_text}",
        flush=True,
    )

    refine_study = optuna.create_study(
        direction="maximize",
        sampler=_make_sampler(optuna, "cmaes", seed, source_trials=warm or None),
    )
    refine_study.enqueue_trial(dict(best.params))   # trial 0 = the best trial

    def copy_into_main_study(_refine_study, trial) -> None:
        if trial.state != optuna.trial.TrialState.COMPLETE:
            return
        study.add_trial(optuna.trial.create_trial(
            params=trial.params,
            distributions=trial.distributions,
            value=trial.value,
            user_attrs=trial.user_attrs,
        ))
        progress_callback(study, study.trials[-1])   # logged with its main-study number

    previous_pins = dict(_PINNED_SWITCHES)
    _PINNED_SWITCHES.clear()
    _PINNED_SWITCHES.update(pinned)
    state["phase"] = "cmaes"
    try:
        refine_study.optimize(
            objective, n_trials=n_trials, callbacks=[copy_into_main_study],
            show_progress_bar=False,
        )
    finally:
        _PINNED_SWITCHES.clear()
        _PINNED_SWITCHES.update(previous_pins)


def _top_k_report(
    study,
    base: Params,
    in_set: DaySet,
    out_set: DaySet,
    cv_folds: int,
    csv_path: str,
) -> List[Dict[str, Any]]:
    """
    Re-score the baseline and the best OPT_REPORT_TOP_K distinct trials on
      * the in-sample days with the objective's slippage (what was optimized);
      * the in-sample days with the STRESS slippage;
      * the in-sample days with PROFIT_TARGET_FILL=CLOSE (conservative target
        check: the target counts only when a minute CLOSE reaches it);
      * the holdout days (if OPT_HOLDOUT_FROM is set), with the same three views.
    The holdout numbers are computed only here, after the search, so they can
    never influence which trial the search prefers.
    """
    import optuna

    complete = [
        t for t in study.trials
        if t.state == optuna.trial.TrialState.COMPLETE
        and t.value is not None
        and t.value > _OPT_INFEASIBLE          # [MODES] valid trials only
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
        # Same parameters, conservative target detection (minute closes only).
        close_params = replace(params, profit_target_fill="CLOSE")
        ins = evaluate_fast(params, in_set, OPT_SLIPPAGE_PTS, cv_folds)
        ins_stress = evaluate_fast(params, in_set, OPT_STRESS_SLIPPAGE_PTS, cv_folds)
        ins_close = evaluate_fast(close_params, in_set, OPT_SLIPPAGE_PTS, cv_folds)
        row: Dict[str, Any] = {
            "rank": rank,
            "trial_number": trial_number,
            "objective": round(ins["objective"], 2),
            "in_sample_net": round(ins["total_pnl"], 2),
            "in_sample_net_stress": round(ins_stress["total_pnl"], 2),
            "in_sample_net_close_fill": round(ins_close["total_pnl"], 2),
            "in_sample_worst_day": round(ins["worst_day"], 2),
            "in_sample_worst_month": round(ins["worst_month"], 2),
            "in_sample_trades": ins["n_trades"],
            "in_sample_cap_hit_days": ins["cap_hit_days"],
            # [MODES] day-level view used by the win_days / calmar / sharpe modes
            "in_sample_days": ins["n_days"],
            "in_sample_win_days": ins["win_days"],
            "in_sample_loss_days": ins["loss_days"],
            "in_sample_max_drawdown": round(ins["max_drawdown"], 2),
            "in_sample_return_over_dd": round(ins["return_over_dd"], 4),
            "in_sample_daily_sharpe": round(ins["daily_sharpe"], 4),
        }
        if out_set.days:
            oos = evaluate_fast(params, out_set, OPT_SLIPPAGE_PTS, 1)
            oos_stress = evaluate_fast(params, out_set, OPT_STRESS_SLIPPAGE_PTS, 1)
            oos_close = evaluate_fast(close_params, out_set, OPT_SLIPPAGE_PTS, 1)
            row.update({
                "holdout_net": round(oos["total_pnl"], 2),
                "holdout_net_stress": round(oos_stress["total_pnl"], 2),
                "holdout_net_close_fill": round(oos_close["total_pnl"], 2),
                "holdout_worst_day": round(oos["worst_day"], 2),
                "holdout_profitable_month_ratio": round(oos["profitable_month_ratio"], 4),
                "holdout_trades": oos["n_trades"],
                # [MODES]
                "holdout_days": oos["n_days"],
                "holdout_win_days": oos["win_days"],
                "holdout_max_drawdown": round(oos["max_drawdown"], 2),
                "holdout_return_over_dd": round(oos["return_over_dd"], 4),
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
    print("          (close = PROFIT_TARGET_FILL=CLOSE, the conservative target check)", flush=True)
    header = (
        f"{'rank':>8} {'trial':>6} {'in-sample':>15} {'in-s stress':>15} {'in-s close':>15}"
    )
    if out_set.days:
        header += f" {'holdout':>15} {'hold stress':>15} {'hold close':>15}"
    header += f" {'worst day':>13} {'cap days':>8} {'win days':>9} {'max DD':>13}"
    if out_set.days:
        header += f" {'hold win':>9}"
    print(header, flush=True)
    for row in rows[: min(len(rows), 11)]:
        line = (
            f"{str(row['rank']):>8} {str(row['trial_number']):>6} "
            f"{_inr(row['in_sample_net']):>15} {_inr(row['in_sample_net_stress']):>15} "
            f"{_inr(row['in_sample_net_close_fill']):>15}"
        )
        if out_set.days:
            line += (
                f" {_inr(row['holdout_net']):>15} {_inr(row['holdout_net_stress']):>15}"
                f" {_inr(row['holdout_net_close_fill']):>15}"
            )
        line += f" {_inr(row['in_sample_worst_day']):>13} {row['in_sample_cap_hit_days']:>8}"
        # [MODES] profitable days as wins/traded days, and max drawdown
        line += (f" {str(row['in_sample_win_days']) + '/' + str(row['in_sample_days']):>9}"
                 f" {_inr(row['in_sample_max_drawdown']):>13}")
        if out_set.days:
            line += f" {str(row['holdout_win_days']) + '/' + str(row['holdout_days']):>9}"
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
    base = _apply_objective_fill(base)
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
    if main_kind == "cmaes":
        # CMA-ES needs a fixed parameter set (see _run_cmaes_refinement), so a
        # CMA-ES-only run keeps every on/off feature at the config's setting.
        _PINNED_SWITCHES.update({
            name: int(seed_values[name]) for name in _SWITCH_PARAMS if name in seed_values
        })
        print(
            "[OPT] OPT_SAMPLER=cmaes: on/off features pinned to the config "
            f"({_PINNED_SWITCHES}); use the default 'tpe' to search them.",
            flush=True,
        )
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
    # [MODES] Fix the net-P&L guard for this run before anything is scored.
    global _NET_FLOOR
    floors = []
    if OPT_MIN_NET_PNL is not None:
        floors.append(float(OPT_MIN_NET_PNL))
    if OPT_MIN_NET_VS_BASELINE is not None:
        base_net = _summarise_fast(
            *fast_simulate(_fast_params(base), in_set.days, OPT_SLIPPAGE_PTS), in_set
        )["total_pnl"]
        if base_net > 0:
            floors.append(float(OPT_MIN_NET_VS_BASELINE) * base_net)
        else:
            print(
                f"[MODE WARNING] OPT_MIN_NET_VS_BASELINE ignored: the baseline's in-sample "
                f"net is {_inr(base_net)} (not positive).",
                flush=True,
            )
    _NET_FLOOR = max(floors) if floors else None
    print(f"[MODE] {_mode_label()}", flush=True)
    if OPT_OBJECTIVE != "profit" and cv_folds > 1:
        print(
            f"[MODE] OPT_CV_FOLDS={cv_folds} is a profit-mode feature and is ignored "
            f"with OPT_OBJECTIVE={OPT_OBJECTIVE}.",
            flush=True,
        )

    baseline_eval = evaluate_fast(base, in_set, OPT_SLIPPAGE_PTS, cv_folds)
    slip_text = ", ".join(f"{u} {p:g} pt" for u, p in OPT_SLIPPAGE_PTS.items())
    print(
        f"[OPT] exact property-file baseline (in-sample, slippage {slip_text}): "
        f"net={_inr(baseline_eval['total_pnl'])}, objective={_fmt_objective(baseline_eval['objective'])}, "
        f"trades={baseline_eval['n_trades']}, days where MAX_REATTEMPTS stopped a "
        f"re-entry={baseline_eval['cap_hit_days']}",
        flush=True,
    )
    print(
        f"[OPT] baseline day profile: {baseline_eval['win_days']} profitable / "
        f"{baseline_eval['loss_days']} losing of {baseline_eval['n_days']} traded days, "
        f"max drawdown {_inr(baseline_eval['max_drawdown'])}, "
        f"net/DD {baseline_eval['return_over_dd']:.3f}, daily Sharpe {baseline_eval['daily_sharpe']:.4f}",
        flush=True,
    )
    if _OPT_DISQUALIFY < baseline_eval["objective"] <= _OPT_INFEASIBLE:
        print(
            "[MODE WARNING] the baseline itself misses the OPT_MIN_*/OPT_MAX_* guards "
            f"({_fmt_objective(baseline_eval['objective'])}); loosen them if no trial can meet them.",
            flush=True,
        )
    if baseline_eval["objective"] <= _OPT_DISQUALIFY:
        # Say it loudly: if even the baseline fails the guard, most trials
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
    csv_file = open(trial_csv_path, "w", newline="", encoding="utf-8")
    csv_writer = csv.DictWriter(csv_file, fieldnames=list(_TRIAL_CSV_FIELDS))
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
        # ONE user attribute per trial keeps journal/SQLite writes cheap.
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
                f"win_days={metrics.get('win_days', 0)}/{metrics.get('n_days', 0)} "
                f"maxDD={_inr(metrics.get('max_drawdown', 0.0))} "
                f"worst_day={_inr(metrics.get('worst_day', 0.0))} "
                f"| best={_fmt_objective(state['best'])} ({rate:.1f} trials/s)"
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
                _run_cmaes_refinement(
                    optuna, study, objective, progress_callback, state,
                    n_trials=refine_planned, seed=seed + 7,
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
                f"[OPT WARN] best trial re-scored to {_fmt_objective(optuna_eval['objective'])} "
                f"but was stored as {_fmt_objective(best_trial.value)}.",
                flush=True,
            )
        if float(best_trial.value) <= _OPT_INFEASIBLE:
            print(
                "[MODE WARNING] NO trial met the OPT_MIN_*/OPT_MAX_* guards; the best "
                f"trial misses them ({_fmt_objective(best_trial.value)}). Loosen the guards "
                "or widen the search before trusting this result.",
                flush=True,
            )
        # When the baseline violates your limits it is a benchmark
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
        f"# Objective mode            : {_mode_label()}",
        f"# Objective (in-sample)     : {_fmt_objective(selected_eval['objective'])}   "
        f"baseline {_fmt_objective(baseline_eval['objective'])}",
        f"# Profitable days (in-samp.): {selected_eval['win_days']} of {selected_eval['n_days']}   "
        f"baseline {baseline_eval['win_days']} of {baseline_eval['n_days']}",
        f"# Max drawdown (in-sample)  : {_inr(selected_eval['max_drawdown'])}   "
        f"baseline {_inr(baseline_eval['max_drawdown'])}",
        f"# Selected source           : {selected_source}",
        f"# In-sample days            : {len(in_days)}  "
        f"({in_days[0].dy} .. {in_days[-1].dy})",
        f"# In-sample net             : {_inr(selected_eval['total_pnl'])}   "
        f"baseline {_inr(baseline_eval['total_pnl'])}",
    ]
    if selected_row is not None:
        summary_lines.append(
            f"# In-sample net, stress slip: {_inr(selected_row['in_sample_net_stress'])}"
        )
        summary_lines.append(
            f"# In-sample net, CLOSE fill : {_inr(selected_row['in_sample_net_close_fill'])}"
        )
        if out_days:
            label = f"Holdout net ({len(out_days)} days)"
            summary_lines.append(
                f"# {label:<26}: {_inr(selected_row['holdout_net'])}   "
                f"stress {_inr(selected_row['holdout_net_stress'])}   "
                f"CLOSE fill {_inr(selected_row['holdout_net_close_fill'])}   "
                f"baseline {_inr(top_rows[0]['holdout_net'])}"
            )
            summary_lines.append(
                f"# Holdout profitable days   : {selected_row['holdout_win_days']} of "
                f"{selected_row['holdout_days']}   baseline {top_rows[0]['holdout_win_days']} of "
                f"{top_rows[0]['holdout_days']}"
            )
    summary_lines.append(
        f"# Days MAX_REATTEMPTS bound : {selected_eval['cap_hit_days']}"
    )
    properties_path = prefix + "_BEST.properties"
    with open(properties_path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(summary_lines) + "\n")
        handle.write(params_to_properties(selected_params))

    # ---- 12. Console summary -----------------------------------------------------------
    print(f"\n================ BEST V3 CONFIG (DTE {_DTE_LABEL}) ================", flush=True)
    print(f"Objective mode         : {_mode_label()}", flush=True)
    print(f"Selected source        : {selected_source}", flush=True)
    if best_trial is not None:
        print(f"Best trial / objective : {best_trial.number} / {_fmt_objective(best_trial.value)}", flush=True)
    print(f"Baseline objective     : {_fmt_objective(baseline_eval['objective'])}", flush=True)
    print(f"In-sample net profit   : {_inr(selected_eval['total_pnl'])} "
          f"(baseline {_inr(baseline_eval['total_pnl'])})", flush=True)
    # [MODES] the day-level numbers every mode should be judged on
    print(f"In-sample profit days  : {selected_eval['win_days']} of {selected_eval['n_days']} "
          f"(baseline {baseline_eval['win_days']} of {baseline_eval['n_days']}); "
          f"losing days {selected_eval['loss_days']} (baseline {baseline_eval['loss_days']})", flush=True)
    print(f"In-sample max drawdown : {_inr(selected_eval['max_drawdown'])} "
          f"(baseline {_inr(baseline_eval['max_drawdown'])}); net/DD "
          f"{selected_eval['return_over_dd']:.3f} (baseline {baseline_eval['return_over_dd']:.3f})", flush=True)
    print(f"In-sample worst day    : {_inr(selected_eval['worst_day'])} "
          f"(baseline {_inr(baseline_eval['worst_day'])})", flush=True)
    if selected_row is not None:
        baseline_row = top_rows[0]
        print(
            f"In-sample, stress slip : {_inr(selected_row['in_sample_net_stress'])} "
            f"(baseline {_inr(baseline_row['in_sample_net_stress'])})",
            flush=True,
        )
        print(
            f"In-sample, CLOSE fill  : {_inr(selected_row['in_sample_net_close_fill'])} "
            f"(baseline {_inr(baseline_row['in_sample_net_close_fill'])})",
            flush=True,
        )
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
            print(
                f"Holdout, CLOSE fill    : {_inr(selected_row['holdout_net_close_fill'])} "
                f"(baseline {_inr(baseline_row['holdout_net_close_fill'])})",
                flush=True,
            )
            print(
                f"Holdout profit days    : {selected_row['holdout_win_days']} of "
                f"{selected_row['holdout_days']} (baseline {baseline_row['holdout_win_days']} of "
                f"{baseline_row['holdout_days']}); max DD {_inr(selected_row['holdout_max_drawdown'])} "
                f"(baseline {_inr(baseline_row['holdout_max_drawdown'])})",
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
    print(f"Profitable days (all)  : {ref_all['profitable_day_ratio'] * 100:.1f}% "
          f"({int(round(ref_all['profitable_day_ratio'] * ref_all['n_days']))} of "
          f"{ref_all['n_days']} days with net > 0)", flush=True)
    print(f"Max drawdown (all)     : {_inr(ref_all['max_drawdown'])}", flush=True)
    print(f"Days MAX_REATTEMPTS bound (in-sample): {selected_eval['cap_hit_days']}", flush=True)
    print(f"Best properties        : {properties_path}", flush=True)
    print(f"Best actual trades     : {best_trades_path}", flush=True)
    print(f"Best monthly P/L       : {best_monthly_path}", flush=True)
    print("\n---- READY-TO-PASTE PROPERTY BLOCK ----", flush=True)
    print(params_to_properties(selected_params), flush=True)

    return study, selected_params


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
