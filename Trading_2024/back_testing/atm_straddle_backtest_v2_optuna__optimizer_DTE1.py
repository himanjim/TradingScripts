"""
Optuna optimiser for ``atm_straddle_backtest_v2.py``.
=======================================================

This is a companion optimiser for the v2 short-straddle strategy.  Keep this
file, ``atm_straddle_backtest_v2.py`` and
``straddle_config_DTE_1_v2.properties`` in the same project (the property file
may alternatively be inside a ``configs`` subfolder).

Why this is a companion file instead of a copied backtester
------------------------------------------------------------
The optimiser imports the actual v2 backtester and calls its own simulation
function for every trial.  Therefore:

* the backtest and optimiser cannot silently drift into different strategies;
* bug fixes made in ``atm_straddle_backtest_v2.py`` are automatically used;
* the original property file remains the baseline configuration;
* only the trial parameters are changed in memory; and
* a ready-to-run best-configuration property file is written at the end.

Run modes
---------
``RUN_MODE = "optimize"``
    Load option/underlying data once and run the Optuna search.

``RUN_MODE = "backtest"``
    Run the original v2 backtest with the selected property file.

The optimiser maximises net profit after the transaction charges already
implemented by the v2 strategy.  Set ``OPT_CV_FOLDS`` above 1 to use contiguous
walk-forward blocks and penalise unstable profit across time blocks.

Important modelling choices
----------------------------
1. ``ALLOWED_DTE`` and ``LOSS_LIMIT_RUPEES_BY_ATTEMPT`` remain fixed from the
   baseline property file.  In the supplied DTE-1 configuration the Rs.3,000
   absolute stop cap binds, so optimising the percentage schedule is mostly
   wasted search capacity.
2. Optimisation is intentionally sequential.  The imported strategy uses
   module-level settings, which are changed for each trial.  Running trials in
   parallel threads/processes would cause settings to leak between trials.
3. Historical optimisation can overfit.  Use a large sample, inspect the
   month-wise output, and confirm the winner on a genuinely out-of-sample
   period before live deployment.

Dependencies
------------
    pip install optuna pandas openpyxl
"""

from __future__ import annotations

import csv
import glob
import importlib.util
import os
import sys
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, time as dtime
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import pandas as pd


# =============================================================================
# RUN CONTROL
# =============================================================================
# Edit these values and press Run in PyCharm.
RUN_MODE = "optimize"  # "optimize" or "backtest"

# Number of Optuna trials.  First use 5-10 trials with the sample limits below.
OPT_TRIALS = 1000
OPT_SEED = 42

# 1 = maximise full-sample total net profit exactly.
# >1 = split months into contiguous blocks and maximise:
#      mean(block profit) - OPT_CV_PENALTY * std(block profit)
# Use 5 or 6 for a robustness-oriented search after the smoke test succeeds.
OPT_CV_FOLDS = 1
OPT_CV_PENALTY = 0.50

# Data guard.  Configurations producing fewer observations are disqualified.
OPT_MIN_DAYS = 30
OPT_MIN_MONTHS = 3

# Every completed trial is appended and flushed to CSV.  SQLite persistence is
# optional; when enabled, re-running this script continues the same study.
OPT_SAVE_DB = False
OPT_STUDY_NAME = "atm_straddle_DTE2_v2_profit"
OPT_OUTPUT_DIR = str(Path.home() / "Downloads" / "straddle_optimizer_runs")

# Generate a detailed Excel backtest and a ready-to-run .properties file for
# the winning configuration after optimisation.
OPT_WRITE_BEST_EXCEL = True
OPT_WRITE_BEST_PROPERTIES = True

# Always test the supplied property-file configuration (rounded only where the
# Optuna grid requires it).  This prevents the optimiser from returning a winner
# that was never compared with your current validated baseline.
OPT_ENQUEUE_BASELINE = True

# Small-sample smoke test.  Set both to None for the real optimisation.
SAMPLE_MAX_PICKLES: Optional[int] = None  # example: 3
SAMPLE_MAX_DAY_GROUPS: Optional[int] = None  # example: 20

# Cache option leg series built by the imported strategy.  This is a major speed
# improvement after the first few trials, at the cost of additional RAM.
OPT_CACHE_LEG_SERIES = True
OPT_CACHE_UNDERLYING_ASOF = True


# =============================================================================
# FILE LOCATIONS
# =============================================================================
BASE_DIR = Path(__file__).resolve().parent

# Override either path through an environment variable when your filenames or
# project layout differ.
STRATEGY_FILE = Path(
    os.getenv("STRADDLE_STRATEGY_FILE", str(BASE_DIR / "atm_straddle_backtest_v2.py"))
).expanduser()


def _default_config_path() -> Path:
    """Prefer a same-folder property file, then a ``configs`` subfolder."""
    same_folder = BASE_DIR / "straddle_config_DTE_1_v2.properties"
    configs_folder = BASE_DIR / "configs" / "straddle_config_DTE_1_v2.properties"
    if same_folder.exists():
        return same_folder
    return configs_folder


CONFIG_FILE = Path(
    os.getenv("STRADDLE_CONFIG", str(_default_config_path()))
).expanduser()

# The strategy reads STRADDLE_CONFIG while it is imported.  Set it before the
# dynamic import so the exact baseline property file is used.
os.environ["STRADDLE_CONFIG"] = str(CONFIG_FILE)


# =============================================================================
# SEARCH SPACE
# =============================================================================
# Bounds are deliberately narrower than arbitrary mathematical ranges.  They
# cover realistic DTE-1 short-straddle settings without wasting hundreds of
# trials on obviously impractical combinations.

# First entry: 09:20 through 12:30 in five-minute steps.
OPT_ENTRY_EARLIEST = "09:20"
OPT_ENTRY_LATEST = "12:30"
OPT_ENTRY_STEP_MIN = 5

# Hard square-off: 15:00 through 15:29 in one-minute steps.
OPT_EXIT_EARLIEST = "15:00"
OPT_EXIT_LATEST = "15:29"
OPT_EXIT_STEP_MIN = 1

# Effective absolute stop cap.  The percentage stop schedule remains fixed from
# the property file.
OPT_STOP_CAP_MIN = 2_000
OPT_STOP_CAP_MAX = 5_000
OPT_STOP_CAP_STEP = 250

# Separate profit-protect controls.  Giveback is generated as a fraction of the
# arm threshold, ensuring giveback never exceeds arm.
OPT_PP_ARM_MIN = 0.04
OPT_PP_ARM_MAX = 0.25
OPT_PP_ARM_STEP = 0.005
OPT_PP_GIVEBACK_RATIO_MIN = 0.10
OPT_PP_GIVEBACK_RATIO_MAX = 1.00
OPT_PP_GIVEBACK_RATIO_STEP = 0.05

# Breakeven ratchet.  A Boolean trial switch can disable it.  When enabled,
# lock is a non-negative fraction of its arm threshold.
OPT_BE_ARM_MIN = 0.02
OPT_BE_ARM_MAX = 0.12
OPT_BE_ARM_STEP = 0.005
OPT_BE_LOCK_RATIO_MIN = 0.0
OPT_BE_LOCK_RATIO_MAX = 0.50
OPT_BE_LOCK_RATIO_STEP = 0.05

# Profit target as a fraction of premium collected.  It can be disabled.
OPT_PROFIT_TARGET_MIN = 0.10
OPT_PROFIT_TARGET_MAX = 0.60
OPT_PROFIT_TARGET_STEP = 0.005
OPT_ALLOW_DISABLED_PROFIT_TARGET = True

# Daily realised-loss circuit breaker.  Zero means disabled.
OPT_DAILY_LOSS_CHOICES = [0, 10_000, 12_500, 15_000, 17_500, 20_000, 25_000, 30_000]

# Re-entry controls.
OPT_MAX_REATTEMPTS_MIN = 0
OPT_MAX_REATTEMPTS_MAX = 10
OPT_REENTRY_DELAY_BASE_MIN = 3
OPT_REENTRY_DELAY_BASE_MAX = 20
OPT_REENTRY_DELAY_STEP_MIN = 0
OPT_REENTRY_DELAY_STEP_MAX = 8

# Premium-expansion gate.  It can be disabled; otherwise the fresh ATM premium
# must not exceed this multiple of the previous attempt's entry premium.
OPT_PREMIUM_GATE_MIN = 1.02
OPT_PREMIUM_GATE_MAX = 1.50
OPT_PREMIUM_GATE_STEP = 0.01
OPT_ALLOW_DISABLED_PREMIUM_GATE = True


# =============================================================================
# DYNAMIC IMPORT OF THE ACTUAL STRATEGY
# =============================================================================
def _load_strategy_module(path: Path) -> ModuleType:
    """Import the v2 strategy from an explicit file path."""
    if not path.exists():
        raise FileNotFoundError(
            f"Strategy file not found: {path}\n"
            "Place atm_straddle_backtest_v2.py beside this optimiser or set "
            "STRADDLE_STRATEGY_FILE."
        )
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(
            f"Property file not found: {CONFIG_FILE}\n"
            "Place straddle_config_DTE_1_v2.properties beside this optimiser, "
            "inside a configs subfolder, or set STRADDLE_CONFIG."
        )

    module_name = "atm_straddle_backtest_v2_runtime"
    spec = importlib.util.spec_from_file_location(module_name, str(path))
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not create an import specification for {path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


strategy = _load_strategy_module(STRATEGY_FILE)


# =============================================================================
# BASIC HELPERS
# =============================================================================
def _parse_hhmm(value: str) -> dtime:
    """Parse an HH:MM string with strict validation."""
    try:
        hh_text, mm_text = value.strip().split(":", 1)
        return dtime(hour=int(hh_text), minute=int(mm_text))
    except Exception as exc:
        raise ValueError(f"Invalid HH:MM value: {value!r}") from exc


def _minutes(value: str | dtime) -> int:
    """Convert HH:MM or datetime.time into minutes after midnight."""
    t = _parse_hhmm(value) if isinstance(value, str) else value
    return t.hour * 60 + t.minute


def _hhmm(total_minutes: int) -> str:
    """Convert minutes after midnight into HH:MM."""
    hh, mm = divmod(int(total_minutes), 60)
    return f"{hh:02d}:{mm:02d}"


def _float_grid_round(value: float, step: float, digits: int = 6) -> float:
    """Round a derived percentage to the configured grid and avoid float noise."""
    if step <= 0:
        return round(float(value), digits)
    return round(round(float(value) / step) * step, digits)


def _inr(value: Any) -> str:
    """Format a rupee value using Indian digit grouping and ASCII 'Rs.' text."""
    try:
        n = int(round(float(value)))
    except (TypeError, ValueError):
        return "Rs.0"

    sign = "-" if n < 0 else ""
    digits = str(abs(n))
    if len(digits) <= 3:
        grouped = digits
    else:
        last_three = digits[-3:]
        remaining = digits[:-3]
        pairs: List[str] = []
        while len(remaining) > 2:
            pairs.insert(0, remaining[-2:])
            remaining = remaining[:-2]
        if remaining:
            pairs.insert(0, remaining)
        grouped = ",".join(pairs) + "," + last_three
    return f"Rs.{sign}{grouped}"


def _safe_name(value: str) -> str:
    """Return a filesystem-safe fragment."""
    return "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in value)


# =============================================================================
# TRIAL PARAMETER MODEL
# =============================================================================
@dataclass(frozen=True)
class TrialParams:
    """Concrete strategy settings used for one Optuna trial."""

    entry_time_ist: str
    exit_time_ist: str
    stop_cap_rupees: float

    profit_protect_arm_pct: float
    profit_protect_giveback_pct: float

    breakeven_arm_pct: float
    breakeven_lock_pct: float

    profit_target_pct: float
    max_daily_loss_rupees: float

    max_reattempts: int
    reentry_delay_by_attempt: Tuple[int, ...]
    reentry_max_premium_ratio: float

    def to_property_updates(self) -> Dict[str, str]:
        """Convert the trial into KEY=VALUE strings used by the strategy file."""
        # The legacy trigger is retained as a sensible fallback/diagnostic value.
        return {
            "ENTRY_TIME_IST": self.entry_time_ist,
            "EXIT_TIME_IST": self.exit_time_ist,
            "MAX_LOSS_LIMIT_RUPEES_BY_ATTEMPT": str(int(round(self.stop_cap_rupees))),
            "PROFIT_PROTECT_TRIGGER_RUPEES": f"{self.profit_protect_arm_pct:.6f}",
            "PROFIT_PROTECT_ARM_PCT": f"{self.profit_protect_arm_pct:.6f}",
            "PROFIT_PROTECT_GIVEBACK_PCT": f"{self.profit_protect_giveback_pct:.6f}",
            "BREAKEVEN_ARM_PCT": f"{self.breakeven_arm_pct:.6f}",
            "BREAKEVEN_LOCK_PCT": f"{self.breakeven_lock_pct:.6f}",
            "PROFIT_TARGET_PCT": f"{self.profit_target_pct:.6f}",
            "MAX_DAILY_LOSS_RUPEES": str(int(round(self.max_daily_loss_rupees))),
            "MAX_REATTEMPTS": str(int(self.max_reattempts)),
            "REENTRY_DELAY_BY_ATTEMPT": ", ".join(
                str(int(v)) for v in self.reentry_delay_by_attempt
            ),
            "REENTRY_MAX_PREMIUM_RATIO": f"{self.reentry_max_premium_ratio:.6f}",
        }


def _params_from_optuna_trial(trial: Any) -> TrialParams:
    """Suggest one internally valid parameter configuration."""
    entry_minute = trial.suggest_int(
        "entry_minute",
        _minutes(OPT_ENTRY_EARLIEST),
        _minutes(OPT_ENTRY_LATEST),
        step=OPT_ENTRY_STEP_MIN,
    )
    exit_minute = trial.suggest_int(
        "exit_minute",
        _minutes(OPT_EXIT_EARLIEST),
        _minutes(OPT_EXIT_LATEST),
        step=OPT_EXIT_STEP_MIN,
    )

    stop_cap = trial.suggest_int(
        "stop_cap_rupees",
        OPT_STOP_CAP_MIN,
        OPT_STOP_CAP_MAX,
        step=OPT_STOP_CAP_STEP,
    )

    pp_arm = trial.suggest_float(
        "profit_protect_arm_pct",
        OPT_PP_ARM_MIN,
        OPT_PP_ARM_MAX,
        step=OPT_PP_ARM_STEP,
    )
    pp_giveback_ratio = trial.suggest_float(
        "profit_protect_giveback_ratio",
        OPT_PP_GIVEBACK_RATIO_MIN,
        OPT_PP_GIVEBACK_RATIO_MAX,
        step=OPT_PP_GIVEBACK_RATIO_STEP,
    )
    pp_giveback = _float_grid_round(
        pp_arm * pp_giveback_ratio,
        OPT_PP_ARM_STEP,
    )
    pp_giveback = min(pp_arm, max(OPT_PP_ARM_STEP, pp_giveback))

    be_enabled = trial.suggest_categorical("breakeven_enabled", [False, True])
    if be_enabled:
        be_arm = trial.suggest_float(
            "breakeven_arm_pct",
            OPT_BE_ARM_MIN,
            OPT_BE_ARM_MAX,
            step=OPT_BE_ARM_STEP,
        )
        be_lock_ratio = trial.suggest_float(
            "breakeven_lock_ratio",
            OPT_BE_LOCK_RATIO_MIN,
            OPT_BE_LOCK_RATIO_MAX,
            step=OPT_BE_LOCK_RATIO_STEP,
        )
        be_lock = _float_grid_round(be_arm * be_lock_ratio, OPT_BE_ARM_STEP)
        be_lock = min(be_arm, max(0.0, be_lock))
    else:
        be_arm = 0.0
        be_lock = 0.0

    if OPT_ALLOW_DISABLED_PROFIT_TARGET:
        target_enabled = trial.suggest_categorical("profit_target_enabled", [False, True])
    else:
        target_enabled = True
    if target_enabled:
        profit_target = trial.suggest_float(
            "profit_target_pct",
            OPT_PROFIT_TARGET_MIN,
            OPT_PROFIT_TARGET_MAX,
            step=OPT_PROFIT_TARGET_STEP,
        )
    else:
        profit_target = 0.0

    daily_loss = float(
        trial.suggest_categorical("max_daily_loss_rupees", OPT_DAILY_LOSS_CHOICES)
    )

    max_reattempts = trial.suggest_int(
        "max_reattempts",
        OPT_MAX_REATTEMPTS_MIN,
        OPT_MAX_REATTEMPTS_MAX,
    )
    delay_base = trial.suggest_int(
        "reentry_delay_base_min",
        OPT_REENTRY_DELAY_BASE_MIN,
        OPT_REENTRY_DELAY_BASE_MAX,
    )
    delay_step = trial.suggest_int(
        "reentry_delay_step_min",
        OPT_REENTRY_DELAY_STEP_MIN,
        OPT_REENTRY_DELAY_STEP_MAX,
    )
    # One slot is needed before each possible re-entry.  Keeping at least one
    # value also preserves valid property-file syntax when max_reattempts=0.
    n_delay_slots = max(1, max_reattempts)
    delays = tuple(delay_base + i * delay_step for i in range(n_delay_slots))

    if OPT_ALLOW_DISABLED_PREMIUM_GATE:
        gate_enabled = trial.suggest_categorical("premium_gate_enabled", [False, True])
    else:
        gate_enabled = True
    if gate_enabled:
        premium_gate = trial.suggest_float(
            "reentry_max_premium_ratio",
            OPT_PREMIUM_GATE_MIN,
            OPT_PREMIUM_GATE_MAX,
            step=OPT_PREMIUM_GATE_STEP,
        )
    else:
        premium_gate = 0.0

    return TrialParams(
        entry_time_ist=_hhmm(entry_minute),
        exit_time_ist=_hhmm(exit_minute),
        stop_cap_rupees=float(stop_cap),
        profit_protect_arm_pct=float(pp_arm),
        profit_protect_giveback_pct=float(pp_giveback),
        breakeven_arm_pct=float(be_arm),
        breakeven_lock_pct=float(be_lock),
        profit_target_pct=float(profit_target),
        max_daily_loss_rupees=float(daily_loss),
        max_reattempts=int(max_reattempts),
        reentry_delay_by_attempt=delays,
        reentry_max_premium_ratio=float(premium_gate),
    )


def _params_from_finished_trial(frozen_trial: Any) -> TrialParams:
    """Reconstruct concrete parameters from a completed Optuna trial."""
    p = dict(frozen_trial.params)

    pp_arm = float(p["profit_protect_arm_pct"])
    pp_giveback = _float_grid_round(
        pp_arm * float(p["profit_protect_giveback_ratio"]),
        OPT_PP_ARM_STEP,
    )
    pp_giveback = min(pp_arm, max(OPT_PP_ARM_STEP, pp_giveback))

    if bool(p.get("breakeven_enabled", False)):
        be_arm = float(p["breakeven_arm_pct"])
        be_lock = _float_grid_round(
            be_arm * float(p["breakeven_lock_ratio"]),
            OPT_BE_ARM_STEP,
        )
        be_lock = min(be_arm, max(0.0, be_lock))
    else:
        be_arm = 0.0
        be_lock = 0.0

    if bool(p.get("profit_target_enabled", True)):
        profit_target = float(p.get("profit_target_pct", 0.0))
    else:
        profit_target = 0.0

    max_reattempts = int(p["max_reattempts"])
    delay_base = int(p["reentry_delay_base_min"])
    delay_step = int(p["reentry_delay_step_min"])
    delays = tuple(
        delay_base + i * delay_step for i in range(max(1, max_reattempts))
    )

    if bool(p.get("premium_gate_enabled", True)):
        premium_gate = float(p.get("reentry_max_premium_ratio", 0.0))
    else:
        premium_gate = 0.0

    return TrialParams(
        entry_time_ist=_hhmm(int(p["entry_minute"])),
        exit_time_ist=_hhmm(int(p["exit_minute"])),
        stop_cap_rupees=float(p["stop_cap_rupees"]),
        profit_protect_arm_pct=pp_arm,
        profit_protect_giveback_pct=pp_giveback,
        breakeven_arm_pct=be_arm,
        breakeven_lock_pct=be_lock,
        profit_target_pct=profit_target,
        max_daily_loss_rupees=float(p["max_daily_loss_rupees"]),
        max_reattempts=max_reattempts,
        reentry_delay_by_attempt=delays,
        reentry_max_premium_ratio=premium_gate,
    )


def _apply_trial_params(params: TrialParams) -> None:
    """
    Apply a trial to the imported strategy module.

    The backtester reads these names as module globals inside
    ``simulate_day_multi_trades``.  Both the textual HH:MM values and parsed
    ``datetime.time`` objects are updated to keep logs and calculations aligned.
    """
    strategy.ENTRY_TIME_IST = params.entry_time_ist
    strategy.ENTRY_TIME = strategy.parse_hhmm(params.entry_time_ist)
    strategy.EXIT_TIME_IST = params.exit_time_ist
    strategy.EXIT_TIME = strategy.parse_hhmm(params.exit_time_ist)

    strategy.MAX_LOSS_LIMIT_RUPEES_BY_ATTEMPT = float(params.stop_cap_rupees)

    strategy.PROFIT_PROTECT_TRIGGER_RUPEES = float(params.profit_protect_arm_pct)
    strategy.PROFIT_PROTECT_ARM_PCT = float(params.profit_protect_arm_pct)
    strategy.PROFIT_PROTECT_GIVEBACK_PCT = float(params.profit_protect_giveback_pct)

    strategy.BREAKEVEN_ARM_PCT = float(params.breakeven_arm_pct)
    strategy.BREAKEVEN_LOCK_PCT = float(params.breakeven_lock_pct)

    strategy.PROFIT_TARGET_PCT = float(params.profit_target_pct)
    strategy.MAX_DAILY_LOSS_RUPEES = float(params.max_daily_loss_rupees)

    strategy.MAX_REATTEMPTS = int(params.max_reattempts)
    strategy.REENTRY_DELAY_BY_ATTEMPT = list(params.reentry_delay_by_attempt)
    strategy.REENTRY_MAX_PREMIUM_RATIO = float(params.reentry_max_premium_ratio)


# =============================================================================
# DATA PREPARATION: BUILD ONCE, REUSE FOR EVERY TRIAL
# =============================================================================
@dataclass
class DayGroup:
    """One nearest-expiry (underlying, day, expiry) simulation unit."""

    und: str
    dy: date
    expiry: date
    day_opt: pd.DataFrame
    underlying_day: pd.DataFrame


def _paths_in_scope(max_pickles: Optional[int]) -> List[str]:
    """Return deterministic pickle paths, optionally truncated for a smoke test."""
    patterns = [
        os.path.join(strategy.PICKLES_DIR, "*.pkl"),
        os.path.join(strategy.PICKLES_DIR, "*.pickle"),
    ]
    paths = sorted(path for pattern in patterns for path in glob.glob(pattern))
    if not paths:
        raise FileNotFoundError(
            f"No .pkl/.pickle files found in configured PICKLES_DIR: {strategy.PICKLES_DIR}"
        )
    if max_pickles is not None and max_pickles > 0:
        paths = paths[:max_pickles]
    return paths


def build_day_groups(
    pickle_paths: Sequence[str],
    min_expiry_map: Mapping[Tuple[str, date], date],
    underlying_data: Mapping[str, pd.DataFrame],
    window_start: date,
    window_end: date,
    max_day_groups: Optional[int] = None,
) -> Tuple[List[DayGroup], List[Dict[str, Any]]]:
    """
    Parse option pickles once and retain reusable per-day slices.

    This mirrors the filtering in the v2 strategy's
    ``process_pickles_generate_trades`` function, including nearest-expiry and
    duplicate-day protection.  It deliberately does not simulate trades yet.
    """
    groups: List[DayGroup] = []
    skipped: List[Dict[str, Any]] = []
    processed_day_keys: set[Tuple[str, date, date]] = set()

    needed_cols = [
        "date", "name", "type", "option_type", "strike", "expiry",
        "instrument", "high", "low", "close",
    ]

    total = len(pickle_paths)
    for file_index, path in enumerate(pickle_paths, start=1):
        try:
            df = pd.read_pickle(path)
            if not isinstance(df, pd.DataFrame) or df.empty:
                print(f"[LOAD {file_index}/{total}] {os.path.basename(path)}: empty")
                continue

            missing = [column for column in needed_cols if column not in df.columns]
            if missing:
                raise ValueError(f"Missing columns {missing}")

            d2 = df[df["type"].astype(str).str.upper().eq("OPTION")][needed_cols].copy()
            if d2.empty:
                continue

            d2["date"] = strategy.ensure_ist(d2["date"])
            d2["day"] = d2["date"].dt.date
            d2["underlying"] = d2["name"].astype(str).map(strategy.normalize_underlying)
            d2 = d2[d2["underlying"].isin(strategy.TRADEABLE)]
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

                day_key = (und, dy, expiry)
                if day_key in processed_day_keys:
                    skipped.append(
                        {
                            "day": dy,
                            "underlying": und,
                            "expiry": expiry,
                            "reason": (
                                "Duplicate (underlying,day,expiry) across pickles; "
                                "skipped to avoid double-count"
                            ),
                        }
                    )
                    continue
                processed_day_keys.add(day_key)

                underlying_full = underlying_data.get(und)
                if underlying_full is None:
                    skipped.append(
                        {
                            "day": dy,
                            "underlying": und,
                            "expiry": expiry,
                            "reason": "No underlying series downloaded",
                        }
                    )
                    continue

                underlying_day = underlying_full[underlying_full["day"] == dy].copy()
                if underlying_day.empty:
                    skipped.append(
                        {
                            "day": dy,
                            "underlying": und,
                            "expiry": expiry,
                            "reason": "Underlying missing for day",
                        }
                    )
                    continue

                groups.append(
                    DayGroup(
                        und=str(und),
                        dy=dy,
                        expiry=expiry,
                        day_opt=group.copy(),
                        underlying_day=underlying_day,
                    )
                )

            print(
                f"[LOAD {file_index}/{total}] {os.path.basename(path)} "
                f"(day-groups so far: {len(groups)})",
                flush=True,
            )

        except Exception as exc:
            message = f"[LOAD WARN] {os.path.basename(path)} failed: {exc}"
            if bool(getattr(strategy, "FAIL_ON_PICKLE_ERROR", False)):
                raise RuntimeError(message) from exc
            print(message, flush=True)

    groups.sort(key=lambda item: (item.dy, item.und, item.expiry))
    if max_day_groups is not None and max_day_groups > 0 and len(groups) > max_day_groups:
        groups = groups[-max_day_groups:]
        print(f"[LOAD] Smoke-test limit: retained {len(groups)} latest day-groups")

    print(f"[LOAD] {len(groups)} reusable day-groups ready", flush=True)
    return groups, skipped


# =============================================================================
# PERFORMANCE CACHES FOR THE IMPORTED STRATEGY
# =============================================================================
_ORIGINAL_BUILD_LEG_SERIES = strategy._build_leg_series
_ORIGINAL_ASOF_CLOSE = strategy.asof_close

# The key includes id(day_opt), which is stable because every DayGroup retains
# the same DataFrame object throughout the optimiser run.
_LEG_SERIES_CACHE: Dict[Tuple[int, int, str, str, str, bool], pd.Series] = {}
_UNDERLYING_INDEX_CACHE: Dict[int, pd.Series] = {}


def _cached_build_leg_series(
    day_opt: pd.DataFrame,
    idx_all: pd.DatetimeIndex,
    strike: int,
    opt_type: str,
    symbol: str,
    price_col: str = "close",
    do_ffill: bool = True,
) -> pd.Series:
    """Cache the expensive filter/sort/reindex operation used for each leg."""
    key = (
        id(day_opt),
        int(strike),
        str(opt_type),
        str(symbol),
        str(price_col),
        bool(do_ffill),
    )
    cached = _LEG_SERIES_CACHE.get(key)
    if cached is None:
        cached = _ORIGINAL_BUILD_LEG_SERIES(
            day_opt,
            idx_all,
            strike,
            opt_type,
            symbol,
            price_col,
            do_ffill,
        )
        _LEG_SERIES_CACHE[key] = cached
    return cached


def _cached_asof_close(df: pd.DataFrame, ts: pd.Timestamp) -> float:
    """Cache the indexed underlying close series and perform a fast as-of lookup."""
    if df.empty:
        return float("nan")

    key = id(df)
    indexed = _UNDERLYING_INDEX_CACHE.get(key)
    if indexed is None:
        prepared = df[["date", "close"]].dropna().copy()
        prepared["date"] = strategy.ensure_ist(prepared["date"])
        prepared = prepared.sort_values("date").drop_duplicates("date", keep="last")
        indexed = prepared.set_index("date")["close"].astype(float)
        _UNDERLYING_INDEX_CACHE[key] = indexed

    location = indexed.index.get_indexer([ts], method="pad")
    if location[0] == -1:
        return float("nan")
    return float(indexed.iloc[location[0]])


def _install_performance_caches() -> None:
    """Monkey-patch only the two pure data-access helpers used by simulation."""
    if OPT_CACHE_LEG_SERIES:
        strategy._build_leg_series = _cached_build_leg_series
    if OPT_CACHE_UNDERLYING_ASOF:
        strategy.asof_close = _cached_asof_close


# =============================================================================
# SIMULATION AND METRICS
# =============================================================================
def simulate_groups(
    params: TrialParams,
    groups: Sequence[DayGroup],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Run the imported v2 simulation over all pre-built day-groups."""
    _apply_trial_params(params)

    trade_rows: List[Dict[str, Any]] = []
    skipped_rows: List[Dict[str, Any]] = []
    for group in groups:
        trades, skipped = strategy.simulate_day_multi_trades(
            und=group.und,
            dy=group.dy,
            expiry=group.expiry,
            day_opt=group.day_opt,
            underlying_day=group.underlying_day,
        )
        trade_rows.extend(trade.__dict__ for trade in trades)
        skipped_rows.extend(skipped)

    all_trades = pd.DataFrame(trade_rows)
    if not all_trades.empty:
        all_trades = all_trades.sort_values(
            ["day", "underlying", "trade_seq"]
        ).reset_index(drop=True)

    skipped_df = pd.DataFrame(skipped_rows)
    return all_trades, skipped_df


def _max_drawdown(daily_pnl: pd.Series) -> float:
    """Return maximum peak-to-trough drawdown of cumulative daily net P&L."""
    if daily_pnl.empty:
        return 0.0
    equity = daily_pnl.cumsum()
    running_peak = equity.cummax()
    drawdown = equity - running_peak
    return float(abs(drawdown.min()))


def performance_metrics(actual_df: pd.DataFrame) -> Dict[str, Any]:
    """Calculate profit, stability and cost diagnostics on actual trades."""
    empty_monthly = pd.Series(dtype="float64")
    if actual_df is None or actual_df.empty:
        return {
            "n_days": 0,
            "n_months": 0,
            "n_trades": 0,
            "total_pnl": 0.0,
            "total_charges": 0.0,
            "profitable_day_ratio": 0.0,
            "profitable_month_ratio": 0.0,
            "mean_month": 0.0,
            "median_month": 0.0,
            "worst_day": 0.0,
            "worst_month": 0.0,
            "max_drawdown": 0.0,
            "monthly": empty_monthly,
        }

    frame = actual_df.copy()
    day_values = pd.to_datetime(frame["day"])
    daily = frame.groupby(day_values.dt.date)["exit_pnl"].sum().sort_index()
    monthly = frame.groupby(day_values.dt.to_period("M"))["exit_pnl"].sum().sort_index()

    charges = 0.0
    if "txn_charges" in frame.columns:
        charges = float(pd.to_numeric(frame["txn_charges"], errors="coerce").fillna(0).sum())

    return {
        "n_days": int(len(daily)),
        "n_months": int(len(monthly)),
        "n_trades": int(len(frame)),
        "total_pnl": float(daily.sum()),
        "total_charges": charges,
        "profitable_day_ratio": float((daily > 0).mean()),
        "profitable_month_ratio": float((monthly > 0).mean()),
        "mean_month": float(monthly.mean()),
        "median_month": float(monthly.median()),
        "worst_day": float(daily.min()),
        "worst_month": float(monthly.min()),
        "max_drawdown": _max_drawdown(daily),
        "monthly": monthly,
    }


def objective_score(actual_df: pd.DataFrame, metrics: Mapping[str, Any]) -> float:
    """Return the profit-only objective, optionally with walk-forward stability."""
    if (
        int(metrics["n_days"]) < OPT_MIN_DAYS
        or int(metrics["n_months"]) < OPT_MIN_MONTHS
    ):
        return -1.0e15

    if OPT_CV_FOLDS <= 1:
        return float(metrics["total_pnl"])

    frame = actual_df.copy()
    day_values = pd.to_datetime(frame["day"])
    monthly = frame.groupby(day_values.dt.to_period("M"))["exit_pnl"].sum().sort_index()
    if len(monthly) < max(OPT_MIN_MONTHS, OPT_CV_FOLDS):
        return -1.0e15

    month_values = monthly.to_list()
    n_months = len(month_values)
    block_profits: List[float] = []
    for fold in range(OPT_CV_FOLDS):
        low = fold * n_months // OPT_CV_FOLDS
        high = (fold + 1) * n_months // OPT_CV_FOLDS
        if high > low:
            block_profits.append(float(sum(month_values[low:high])))

    if not block_profits:
        return -1.0e15

    series = pd.Series(block_profits, dtype="float64")
    return float(series.mean() - OPT_CV_PENALTY * series.std(ddof=0))


# =============================================================================
# OUTPUT HELPERS
# =============================================================================
TRIAL_CSV_COLUMNS = [
    "run_index",
    "trial_number",
    "state",
    "score",
    "entry_time_ist",
    "exit_time_ist",
    "stop_cap_rupees",
    "profit_protect_arm_pct",
    "profit_protect_giveback_pct",
    "breakeven_arm_pct",
    "breakeven_lock_pct",
    "profit_target_pct",
    "max_daily_loss_rupees",
    "max_reattempts",
    "reentry_delay_by_attempt",
    "reentry_max_premium_ratio",
    "total_pnl",
    "total_charges",
    "n_trades",
    "n_days",
    "n_months",
    "profitable_day_ratio",
    "profitable_month_ratio",
    "mean_month",
    "median_month",
    "worst_day",
    "worst_month",
    "max_drawdown",
    "elapsed_seconds",
]


def _trial_csv_record(
    run_index: int,
    trial: Any,
    params: TrialParams,
    elapsed_seconds: float,
) -> Dict[str, Any]:
    """Flatten a completed trial into one stable CSV record."""
    attrs = trial.user_attrs
    return {
        "run_index": run_index,
        "trial_number": trial.number,
        "state": str(trial.state),
        "score": trial.value,
        "entry_time_ist": params.entry_time_ist,
        "exit_time_ist": params.exit_time_ist,
        "stop_cap_rupees": int(round(params.stop_cap_rupees)),
        "profit_protect_arm_pct": params.profit_protect_arm_pct,
        "profit_protect_giveback_pct": params.profit_protect_giveback_pct,
        "breakeven_arm_pct": params.breakeven_arm_pct,
        "breakeven_lock_pct": params.breakeven_lock_pct,
        "profit_target_pct": params.profit_target_pct,
        "max_daily_loss_rupees": int(round(params.max_daily_loss_rupees)),
        "max_reattempts": params.max_reattempts,
        "reentry_delay_by_attempt": ";".join(
            str(value) for value in params.reentry_delay_by_attempt
        ),
        "reentry_max_premium_ratio": params.reentry_max_premium_ratio,
        "total_pnl": round(float(attrs.get("total_pnl", 0.0)), 2),
        "total_charges": round(float(attrs.get("total_charges", 0.0)), 2),
        "n_trades": int(attrs.get("n_trades", 0)),
        "n_days": int(attrs.get("n_days", 0)),
        "n_months": int(attrs.get("n_months", 0)),
        "profitable_day_ratio": round(float(attrs.get("profitable_day_ratio", 0.0)), 6),
        "profitable_month_ratio": round(float(attrs.get("profitable_month_ratio", 0.0)), 6),
        "mean_month": round(float(attrs.get("mean_month", 0.0)), 2),
        "median_month": round(float(attrs.get("median_month", 0.0)), 2),
        "worst_day": round(float(attrs.get("worst_day", 0.0)), 2),
        "worst_month": round(float(attrs.get("worst_month", 0.0)), 2),
        "max_drawdown": round(float(attrs.get("max_drawdown", 0.0)), 2),
        "elapsed_seconds": round(float(elapsed_seconds), 2),
    }


def write_updated_properties(
    source_path: Path,
    output_path: Path,
    updates: Mapping[str, str],
) -> None:
    """
    Copy the baseline property file and replace only optimised keys.

    Existing comments, ordering and all non-optimised settings—especially DTE,
    pickle path, lookback and failure policy—are preserved.
    """
    source_lines = source_path.read_text(encoding="utf-8").splitlines()
    remaining = dict(updates)
    output_lines: List[str] = []

    for line in source_lines:
        stripped = line.strip()
        if stripped and not stripped.startswith(("#", ";")) and "=" in line:
            key = line.split("=", 1)[0].strip()
            if key in remaining:
                output_lines.append(f"{key}={remaining.pop(key)}")
                continue
        output_lines.append(line)

    if remaining:
        output_lines.extend(
            [
                "",
                "# -----------------------------------------------------------------------------",
                "# Optimised values added by atm_straddle_backtest_v2_optuna__optimizer_DTE1.py",
                "# -----------------------------------------------------------------------------",
            ]
        )
        for key, value in remaining.items():
            output_lines.append(f"{key}={value}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(output_lines) + "\n", encoding="utf-8")


def _print_params(params: TrialParams, prefix: str = "") -> None:
    """Print a ready-to-understand strategy configuration."""
    print(f"{prefix}ENTRY_TIME_IST                  = {params.entry_time_ist}")
    print(f"{prefix}EXIT_TIME_IST                   = {params.exit_time_ist}")
    print(f"{prefix}MAX_LOSS_LIMIT_RUPEES_BY_ATTEMPT = {int(params.stop_cap_rupees)}")
    print(f"{prefix}PROFIT_PROTECT_ARM_PCT           = {params.profit_protect_arm_pct:.4f}")
    print(f"{prefix}PROFIT_PROTECT_GIVEBACK_PCT      = {params.profit_protect_giveback_pct:.4f}")
    print(f"{prefix}BREAKEVEN_ARM_PCT                = {params.breakeven_arm_pct:.4f}")
    print(f"{prefix}BREAKEVEN_LOCK_PCT               = {params.breakeven_lock_pct:.4f}")
    print(f"{prefix}PROFIT_TARGET_PCT                = {params.profit_target_pct:.4f}")
    print(f"{prefix}MAX_DAILY_LOSS_RUPEES            = {int(params.max_daily_loss_rupees)}")
    print(f"{prefix}MAX_REATTEMPTS                   = {params.max_reattempts}")
    print(f"{prefix}REENTRY_DELAY_BY_ATTEMPT         = {list(params.reentry_delay_by_attempt)}")
    print(f"{prefix}REENTRY_MAX_PREMIUM_RATIO        = {params.reentry_max_premium_ratio:.4f}")



def _nearest_grid_value(value: float, low: float, high: float, step: float) -> float:
    """Clamp a baseline value to a valid Optuna grid point."""
    clamped = min(high, max(low, float(value)))
    if step <= 0:
        return clamped
    grid_index = round((clamped - low) / step)
    return round(low + grid_index * step, 10)


def _baseline_trial_suggestions() -> Dict[str, Any]:
    """
    Translate the loaded property-file baseline into Optuna parameter names.

    Derived controls (profit-protect giveback ratio and breakeven lock ratio)
    are reconstructed from the strategy's concrete percentage settings.  Values
    are moved only to the nearest legal search-grid point.
    """
    entry_minute = int(
        _nearest_grid_value(
            _minutes(strategy.ENTRY_TIME_IST),
            _minutes(OPT_ENTRY_EARLIEST),
            _minutes(OPT_ENTRY_LATEST),
            OPT_ENTRY_STEP_MIN,
        )
    )
    exit_minute = int(
        _nearest_grid_value(
            _minutes(strategy.EXIT_TIME_IST),
            _minutes(OPT_EXIT_EARLIEST),
            _minutes(OPT_EXIT_LATEST),
            OPT_EXIT_STEP_MIN,
        )
    )
    stop_cap = int(
        _nearest_grid_value(
            float(strategy.MAX_LOSS_LIMIT_RUPEES_BY_ATTEMPT),
            OPT_STOP_CAP_MIN,
            OPT_STOP_CAP_MAX,
            OPT_STOP_CAP_STEP,
        )
    )

    pp_arm = _nearest_grid_value(
        float(strategy.PROFIT_PROTECT_ARM_PCT),
        OPT_PP_ARM_MIN,
        OPT_PP_ARM_MAX,
        OPT_PP_ARM_STEP,
    )
    raw_give_ratio = (
        float(strategy.PROFIT_PROTECT_GIVEBACK_PCT) / pp_arm if pp_arm > 0 else 1.0
    )
    pp_give_ratio = _nearest_grid_value(
        raw_give_ratio,
        OPT_PP_GIVEBACK_RATIO_MIN,
        OPT_PP_GIVEBACK_RATIO_MAX,
        OPT_PP_GIVEBACK_RATIO_STEP,
    )

    be_enabled = float(strategy.BREAKEVEN_ARM_PCT) > 0
    target_enabled = float(strategy.PROFIT_TARGET_PCT) > 0
    gate_enabled = float(strategy.REENTRY_MAX_PREMIUM_RATIO) > 0

    daily_loss = min(
        OPT_DAILY_LOSS_CHOICES,
        key=lambda candidate: abs(float(candidate) - float(strategy.MAX_DAILY_LOSS_RUPEES)),
    )
    max_reattempts = int(
        min(
            OPT_MAX_REATTEMPTS_MAX,
            max(OPT_MAX_REATTEMPTS_MIN, int(strategy.MAX_REATTEMPTS)),
        )
    )

    configured_delays = list(strategy.REENTRY_DELAY_BY_ATTEMPT)
    baseline_delay = int(configured_delays[0]) if configured_delays else OPT_REENTRY_DELAY_BASE_MIN
    baseline_step = (
        int(configured_delays[1] - configured_delays[0])
        if len(configured_delays) >= 2
        else 0
    )
    delay_base = int(
        min(OPT_REENTRY_DELAY_BASE_MAX, max(OPT_REENTRY_DELAY_BASE_MIN, baseline_delay))
    )
    delay_step = int(
        min(OPT_REENTRY_DELAY_STEP_MAX, max(OPT_REENTRY_DELAY_STEP_MIN, baseline_step))
    )

    suggestions: Dict[str, Any] = {
        "entry_minute": entry_minute,
        "exit_minute": exit_minute,
        "stop_cap_rupees": stop_cap,
        "profit_protect_arm_pct": pp_arm,
        "profit_protect_giveback_ratio": pp_give_ratio,
        "breakeven_enabled": be_enabled,
        "max_daily_loss_rupees": daily_loss,
        "max_reattempts": max_reattempts,
        "reentry_delay_base_min": delay_base,
        "reentry_delay_step_min": delay_step,
    }

    if be_enabled:
        be_arm = _nearest_grid_value(
            float(strategy.BREAKEVEN_ARM_PCT),
            OPT_BE_ARM_MIN,
            OPT_BE_ARM_MAX,
            OPT_BE_ARM_STEP,
        )
        raw_lock_ratio = (
            float(strategy.BREAKEVEN_LOCK_PCT) / be_arm if be_arm > 0 else 0.0
        )
        suggestions["breakeven_arm_pct"] = be_arm
        suggestions["breakeven_lock_ratio"] = _nearest_grid_value(
            raw_lock_ratio,
            OPT_BE_LOCK_RATIO_MIN,
            OPT_BE_LOCK_RATIO_MAX,
            OPT_BE_LOCK_RATIO_STEP,
        )

    if OPT_ALLOW_DISABLED_PROFIT_TARGET:
        suggestions["profit_target_enabled"] = target_enabled
    if target_enabled:
        suggestions["profit_target_pct"] = _nearest_grid_value(
            float(strategy.PROFIT_TARGET_PCT),
            OPT_PROFIT_TARGET_MIN,
            OPT_PROFIT_TARGET_MAX,
            OPT_PROFIT_TARGET_STEP,
        )

    if OPT_ALLOW_DISABLED_PREMIUM_GATE:
        suggestions["premium_gate_enabled"] = gate_enabled
    if gate_enabled:
        suggestions["reentry_max_premium_ratio"] = _nearest_grid_value(
            float(strategy.REENTRY_MAX_PREMIUM_RATIO),
            OPT_PREMIUM_GATE_MIN,
            OPT_PREMIUM_GATE_MAX,
            OPT_PREMIUM_GATE_STEP,
        )

    return suggestions


# =============================================================================
# OPTUNA STUDY
# =============================================================================
def optimize(
    groups: Sequence[DayGroup],
    min_expiry_map: Mapping[Tuple[str, date], date],
    parse_skips: Sequence[Mapping[str, Any]],
) -> Tuple[Any, TrialParams]:
    """Run the sequential Optuna TPE study and persist every trial."""
    try:
        import optuna
    except ImportError as exc:
        raise RuntimeError("Optuna is required. Install it with: pip install optuna") from exc

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    output_dir = Path(OPT_OUTPUT_DIR).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)

    run_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    trial_csv_path = output_dir / f"{OPT_STUDY_NAME}_{run_stamp}_trials.csv"
    full_csv_path = output_dir / f"{OPT_STUDY_NAME}_{run_stamp}_optuna_full.csv"

    storage: Optional[str] = None
    if OPT_SAVE_DB:
        database_path = output_dir / f"{OPT_STUDY_NAME}.db"
        storage = f"sqlite:///{database_path.as_posix()}"
        print(f"[OPT] Resumable SQLite study: {database_path}")

    study = optuna.create_study(
        direction="maximize",
        sampler=optuna.samplers.TPESampler(seed=OPT_SEED),
        study_name=OPT_STUDY_NAME,
        storage=storage,
        load_if_exists=bool(storage),
    )

    if OPT_ENQUEUE_BASELINE and len(study.trials) == 0:
        baseline = _baseline_trial_suggestions()
        study.enqueue_trial(baseline)
        print("[OPT] Enqueued property-file baseline as the first trial.")
    elif OPT_ENQUEUE_BASELINE:
        print("[OPT] Existing persisted study detected; baseline was not enqueued again.")

    csv_file = trial_csv_path.open("w", newline="", encoding="utf-8")
    csv_writer = csv.DictWriter(csv_file, fieldnames=TRIAL_CSV_COLUMNS)
    csv_writer.writeheader()
    csv_file.flush()

    start_time = time.time()
    run_counter = {"completed": 0}

    def objective(trial: Any) -> float:
        params = _params_from_optuna_trial(trial)
        all_trades, _ = simulate_groups(params, groups)
        actual_trades = strategy.build_actual_trades_df(all_trades, dict(min_expiry_map))
        metrics = performance_metrics(actual_trades)

        for key, value in metrics.items():
            if key == "monthly":
                continue
            trial.set_user_attr(key, value)
        trial.set_user_attr(
            "monthly_pnl",
            {str(month): float(value) for month, value in metrics["monthly"].items()},
        )
        trial.set_user_attr("concrete_params", asdict(params))
        return objective_score(actual_trades, metrics)

    def progress_callback(study_: Any, trial: Any) -> None:
        run_counter["completed"] += 1
        completed = run_counter["completed"]
        elapsed = time.time() - start_time
        eta = elapsed / completed * max(0, OPT_TRIALS - completed)

        params = _params_from_finished_trial(trial)
        attrs = trial.user_attrs
        csv_writer.writerow(_trial_csv_record(completed, trial, params, elapsed))
        csv_file.flush()

        try:
            best_value = study_.best_value
            best_number = study_.best_trial.number
        except Exception:
            best_value = float("nan")
            best_number = -1

        print(
            f"[TRIAL {completed:>4}/{OPT_TRIALS}] "
            f"profit={_inr(attrs.get('total_pnl', 0))} "
            f"trades={int(attrs.get('n_trades', 0))} "
            f"prof_mo={float(attrs.get('profitable_month_ratio', 0))*100:5.1f}% "
            f"prof_day={float(attrs.get('profitable_day_ratio', 0))*100:5.1f}% "
            f"worst_day={_inr(attrs.get('worst_day', 0))} "
            f"max_dd={_inr(attrs.get('max_drawdown', 0))} "
            f"| best_score={_inr(best_value)} "
            f"| elapsed={elapsed:.0f}s eta={eta:.0f}s",
            flush=True,
        )

        if trial.number == best_number:
            monthly = attrs.get("monthly_pnl", {})
            if monthly:
                print("   >>> NEW BEST: month-wise net P&L", flush=True)
                cells = [f"{month}:{_inr(value)}" for month, value in sorted(monthly.items())]
                for index in range(0, len(cells), 4):
                    print("       " + "   ".join(cells[index:index + 4]), flush=True)

    print(f"[OPT] Every trial will be saved to: {trial_csv_path}", flush=True)
    print(
        f"[OPT] Starting {OPT_TRIALS} sequential trials over {len(groups)} day-groups; "
        f"CV folds={OPT_CV_FOLDS}",
        flush=True,
    )

    try:
        study.optimize(
            objective,
            n_trials=OPT_TRIALS,
            callbacks=[progress_callback],
            show_progress_bar=False,
            n_jobs=1,  # mandatory because strategy parameters are module globals
        )
    finally:
        csv_file.close()
        try:
            study.trials_dataframe().to_csv(full_csv_path, index=False)
            print(f"[OPT] Full Optuna table: {full_csv_path}")
        except Exception as exc:
            print(f"[OPT WARN] Could not write full Optuna table: {exc}")

    best_trial = study.best_trial
    best_params = _params_from_finished_trial(best_trial)
    attrs = best_trial.user_attrs

    print("\n" + "=" * 80)
    print("BEST V2 STRADDLE CONFIGURATION")
    print("=" * 80)
    print(f"Objective score             = {_inr(best_trial.value)}")
    print(f"Full-sample net P&L         = {_inr(attrs.get('total_pnl', 0))}")
    print(f"Total transaction charges  = {_inr(attrs.get('total_charges', 0))}")
    print(f"Trades / days / months     = {attrs.get('n_trades', 0)} / "
          f"{attrs.get('n_days', 0)} / {attrs.get('n_months', 0)}")
    print(f"Profitable days            = {float(attrs.get('profitable_day_ratio', 0))*100:.1f}%")
    print(f"Profitable months          = {float(attrs.get('profitable_month_ratio', 0))*100:.1f}%")
    print(f"Mean / median month        = {_inr(attrs.get('mean_month', 0))} / "
          f"{_inr(attrs.get('median_month', 0))}")
    print(f"Worst day / month          = {_inr(attrs.get('worst_day', 0))} / "
          f"{_inr(attrs.get('worst_month', 0))}")
    print(f"Maximum drawdown           = {_inr(attrs.get('max_drawdown', 0))}")
    print("-" * 80)
    _print_params(best_params)

    best_properties_path = output_dir / f"{OPT_STUDY_NAME}_{run_stamp}_BEST.properties"
    if OPT_WRITE_BEST_PROPERTIES:
        write_updated_properties(
            CONFIG_FILE,
            best_properties_path,
            best_params.to_property_updates(),
        )
        print(f"[BEST] Ready-to-run property file: {best_properties_path}")

    if OPT_WRITE_BEST_EXCEL:
        print("[BEST] Running one final detailed backtest for the winning parameters ...")
        best_all, best_sim_skips = simulate_groups(best_params, groups)
        best_actual = strategy.build_actual_trades_df(best_all, dict(min_expiry_map))

        skip_frames = [pd.DataFrame(parse_skips), best_sim_skips]
        non_empty_skips = [frame for frame in skip_frames if frame is not None and not frame.empty]
        best_skipped = (
            pd.concat(non_empty_skips, ignore_index=True)
            if non_empty_skips
            else pd.DataFrame()
        )
        if not best_skipped.empty:
            sort_columns = [
                column for column in ("day", "underlying") if column in best_skipped.columns
            ]
            if sort_columns:
                best_skipped = best_skipped.sort_values(
                    sort_columns, na_position="last"
                ).reset_index(drop=True)

        best_excel_path = output_dir / f"{OPT_STUDY_NAME}_{run_stamp}_BEST.xlsx"
        strategy.OUTPUT_XLSX = str(best_excel_path)
        strategy.write_excel(best_all, best_actual, best_skipped)
        print(f"[BEST] Detailed winning backtest: {best_excel_path}")

    print(f"[OPT] Per-trial CSV: {trial_csv_path}")
    return study, best_params


# =============================================================================
# END-TO-END ENTRYPOINTS
# =============================================================================
def run_optimizer() -> Tuple[Any, TrialParams]:
    """Load market data once, build reusable groups, then run Optuna."""
    _install_performance_caches()

    print(f"[CONFIG] Strategy file : {STRATEGY_FILE}")
    print(f"[CONFIG] Property file : {CONFIG_FILE}")
    print(f"[CONFIG] Pickle folder : {strategy.PICKLES_DIR}")
    print(f"[CONFIG] Allowed DTE   : {strategy.ALLOWED_DTE} (fixed during optimisation)")
    print(
        "[CONFIG] Stop % schedule remains fixed: "
        f"{strategy.LOSS_LIMIT_RUPEES_BY_ATTEMPT}"
    )

    print("[PHASE 1] Scanning option files for date range and nearest expiries ...")
    paths = _paths_in_scope(SAMPLE_MAX_PICKLES)
    print(f"[PHASE 1] Pickles in scope: {len(paths)}")
    end_day, min_expiry_map, min_day_seen = strategy.scan_pickles_pass1(paths)
    window_start = strategy.determine_backtest_window_start(min_day_seen, end_day)
    print(f"[PHASE 1] Backtest window: {window_start} -> {end_day}")

    print("[PHASE 2] Initialising Kite and downloading underlying minute data ...")
    kite = strategy.oUtils.intialize_kite_api()
    underlying_data = strategy.download_underlyings(kite, window_start, end_day)

    print("[PHASE 3] Building reusable day-groups ...")
    groups, parse_skips = build_day_groups(
        paths,
        min_expiry_map,
        underlying_data,
        window_start,
        end_day,
        max_day_groups=SAMPLE_MAX_DAY_GROUPS,
    )
    if not groups:
        raise RuntimeError(
            "No simulatable day-groups were built. Check the property file, DTE, "
            "pickle date range and underlying download."
        )

    print("[PHASE 4] Running Optuna ...")
    return optimize(groups, min_expiry_map, parse_skips)


def main() -> None:
    mode = RUN_MODE.strip().lower()
    if mode == "optimize":
        run_optimizer()
    elif mode == "backtest":
        strategy.main()
    else:
        raise SystemExit(
            f"Unknown RUN_MODE={RUN_MODE!r}. Use 'optimize' or 'backtest'."
        )


if __name__ == "__main__":
    main()
