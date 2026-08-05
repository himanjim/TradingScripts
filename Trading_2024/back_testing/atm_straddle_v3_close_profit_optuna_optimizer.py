"""
Optuna optimizer for the latest DTE-1 ATM short-straddle v3 strategy.
====================================================================

Purpose
-------
This is a STANDALONE optimizer for:

    A. atm_straddle_backtest_v3.py
    B. straddle_config_DTE_1_v3.properties

It deliberately IMPORTS the supplied backtester instead of maintaining a second
copy of the trading rules. Every trial therefore executes the latest strategy
implementation, including its synchronized CE+PE MINUTE-CLOSE profit-target
logic. Independent CE/PE candle lows are never reintroduced by this optimizer.

The expensive work is done once:

    1. Load the base properties file.
    2. Import the latest backtester with those properties.
    3. Scan option pickle files once.
    4. Download NIFTY/SENSEX underlying minute data once.
    5. Build and cache every eligible (underlying, day, expiry) group once.
    6. Re-run only the parameter-dependent strategy simulation per Optuna trial.

The optimizer writes:

    * one flushed CSV row after every completed trial;
    * an optional resumable Optuna SQLite database;
    * Optuna's full trials table;
    * a ready-to-use optimized .properties file;
    * an optional detailed Excel verification report for the best configuration.

Important execution constraint
------------------------------
The imported strategy uses module-level settings. Trials therefore run
SEQUENTIALLY in this process. Do not increase OPT_N_JOBS above 1. Parallel
execution would let trials overwrite one another's strategy parameters.

Dependencies
------------
    pip install pandas openpyxl optuna python-dateutil kiteconnect

Place this optimizer in the same folder as the backtest and properties files.
The filename resolver also recognises the uploaded '(3)' filenames. Explicit
paths can be supplied through BACKTEST_SCRIPT_PATH and BASE_STRATEGY_CONFIG.
"""

from __future__ import annotations

import csv
import importlib.util
import json
import math
import os
import sys
import time
from dataclasses import dataclass, replace
from datetime import date, datetime, time as dtime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd


# =============================================================================
# 1. RUN CONTROL -- EDIT THESE AND RUN THE FILE
# =============================================================================
RUN_MODE = os.getenv("OPT_RUN_MODE", "optimize").strip().lower()  # optimize / verify_baseline / self_test

# Number of Optuna trials. For an initial smoke test, use 5-10 trials together
# with SAMPLE_MAX_PICKLES and SAMPLE_MAX_DAYS below.
OPT_TRIALS = 700

# 1 = maximize full-sample TOTAL NET PROFIT exactly.
# >1 = contiguous walk-forward block score:
#      mean(block profit) - OPT_CV_PENALTY * std(block profit)
# Keep 1 when the sole objective is maximum historical net profit.
OPT_CV_FOLDS = 1
OPT_CV_PENALTY = 0.50

OPT_SEED = 42
OPT_PROGRESS_EVERY = 1
OPT_N_JOBS = 1  # MUST remain 1; see module docstring.

# "core" optimizes the parameters most likely to matter for the v3 target-
# re-entry architecture. "full" also optimizes stop schedules and daily risk
# controls, requiring materially more trials (prefer 800+).
OPT_SEARCH_PROFILE = "core"  # "core" or "full"

# Persistent outputs.
OPT_OUTPUT_DIR = r"G:\My Drive\Trading\optimizer_runs"
OPT_STUDY_NAME = "atm_straddle_dte1_v3_close_profit"
OPT_SAVE_DB = False
OPT_WRITE_BEST_EXCEL = True
OPT_WRITE_BEST_PROPERTIES = True
OPT_ENQUEUE_BASELINE = True

# Optional data reduction for a quick end-to-end test. Set both to None for the
# real optimization. SAMPLE_MAX_DAYS counts unique dates, not day-groups.
SAMPLE_MAX_PICKLES: Optional[int] = None  # e.g. 3
SAMPLE_MAX_DAYS: Optional[int] = None     # e.g. 20

# Guardrails only prevent invalid/thin-data configurations from winning. They
# do not reward smoothness and therefore do not alter the profit-only objective.
OPT_MIN_DAYS = 30
OPT_MIN_MONTHS = 3
OPT_MAX_ALLOWED_DRAWDOWN = 0.0  # 0 disables; otherwise disqualify worse DD.
DISQUALIFIED_SCORE = -1.0e15


# =============================================================================
# 2. SOURCE FILE RESOLUTION
# =============================================================================
THIS_DIR = Path(__file__).resolve().parent


def _first_existing(candidates: Sequence[Path], label: str) -> Path:
    """Return the first existing path or raise an actionable error."""
    for path in candidates:
        if path.exists() and path.is_file():
            return path.resolve()
    rendered = "\n  - ".join(str(p) for p in candidates)
    raise FileNotFoundError(f"Could not locate {label}. Checked:\n  - {rendered}")


_BACKTEST_OVERRIDE = os.getenv("BACKTEST_SCRIPT_PATH", "").strip()
_CONFIG_OVERRIDE = os.getenv("BASE_STRATEGY_CONFIG", "").strip()

BACKTEST_SCRIPT_PATH = _first_existing(
    [Path(_BACKTEST_OVERRIDE)] if _BACKTEST_OVERRIDE else [
        THIS_DIR / "atm_straddle_backtest_v3.py",
        THIS_DIR / "atm_straddle_backtest_v3(3).py",
    ],
    "latest backtest script",
)

BASE_STRATEGY_CONFIG = _first_existing(
    [Path(_CONFIG_OVERRIDE)] if _CONFIG_OVERRIDE else [
        THIS_DIR / "configs/straddle_config_DTE_1_v3.properties",
        THIS_DIR / "configs/straddle_config_DTE_1_v3(3).properties",
    ],
    "base strategy properties file",
)


# =============================================================================
# 3. PROPERTY LOADING AND LATEST-BACKTEST IMPORT
# =============================================================================
def _read_properties(path: Path) -> Dict[str, str]:
    """Read a simple KEY=VALUE property file, preserving values as strings."""
    out: Dict[str, str] = {}
    with path.open("r", encoding="utf-8") as handle:
        for raw in handle:
            line = raw.strip()
            if not line or line.startswith(("#", ";")) or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key, value = key.strip(), value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                value = value[1:-1]
            if key:
                out[key] = value
    return out


BASE_PROPERTY_VALUES = _read_properties(BASE_STRATEGY_CONFIG)

# Remove only keys defined by the selected strategy file, then let the imported
# backtester load them normally. This prevents stale PyCharm/Windows strategy
# variables from overriding file B, while leaving Kite credentials untouched.
for _key in BASE_PROPERTY_VALUES:
    os.environ.pop(_key, None)
os.environ["STRADDLE_CONFIG"] = str(BASE_STRATEGY_CONFIG)


def _import_strategy_module(path: Path):
    """Import the latest backtester without executing its main() function."""
    module_name = "_latest_atm_straddle_backtest_v3_for_optimizer"
    spec = importlib.util.spec_from_file_location(module_name, str(path))
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not create import specification for {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


strategy = _import_strategy_module(BACKTEST_SCRIPT_PATH)


# =============================================================================
# 4. SEARCH RANGES
# =============================================================================
# Times are represented as minutes from the stated anchor. The ranges are kept
# around the supplied 09:20 / 15:29 baseline to avoid wasting trials on clearly
# different strategies. Expand only after a stable local optimum is found.
ENTRY_ANCHOR_MINUTE = 9 * 60 + 16
ENTRY_OFFSET_MIN = 0       # 09:16
ENTRY_OFFSET_MAX = 44      # 10:00
ENTRY_OFFSET_STEP = 2

EXIT_ANCHOR_MINUTE = 15 * 60 + 20
EXIT_OFFSET_MIN = 0        # 15:20
EXIT_OFFSET_MAX = 9        # 15:29
EXIT_OFFSET_STEP = 1

STOP_CAP_MIN = 2_000
STOP_CAP_MAX = 5_000
STOP_CAP_STEP = 250

PROFIT_TARGET_MIN = 0.05
PROFIT_TARGET_MAX = 0.30
PROFIT_TARGET_STEP = 0.01

MAX_REATTEMPTS_MIN = 3
MAX_REATTEMPTS_MAX = 15

TARGET_REENTRY_DELAY_MIN = 1
TARGET_REENTRY_DELAY_MAX = 10

STOP_DELAY_BASE_MIN = 3
STOP_DELAY_BASE_MAX = 15
STOP_DELAY_STEP_MIN = 0
STOP_DELAY_STEP_MAX = 5
STOP_DELAY_RAMP_SLOTS_MIN = 1
STOP_DELAY_RAMP_SLOTS_MAX = 8

PREMIUM_RATIO_MIN = 1.00
PREMIUM_RATIO_MAX = 1.80
PREMIUM_RATIO_STEP = 0.01

MIN_MINUTES_LEFT_CHOICES = [0, 5, 10, 15, 20]

# Additional "full"-profile ranges.
SL_BASE_MIN = 0.10
SL_BASE_MAX = 0.45
SL_STEP_MIN = 0.00
SL_STEP_MAX = 0.05
SL_RAMP_SLOTS_MIN = 1
SL_RAMP_SLOTS_MAX = 8

DAILY_LOSS_CHOICES = [10_000, 15_000, 20_000, 25_000, 30_000]
DAILY_TRAIL_ARM_CHOICES = [0, 5_000, 7_500, 10_000, 12_500, 15_000, 20_000]
DAILY_TRAIL_GIVEBACK_CHOICES = [0, 5_000, 7_500, 10_000, 12_500, 15_000, 20_000]

PROTECT_ARM_MIN = 0.10
PROTECT_ARM_MAX = 0.50
PROTECT_GIVEBACK_MIN = 0.05
PROTECT_GIVEBACK_MAX = 0.35


# =============================================================================
# 5. STRATEGY PARAMETER SNAPSHOT / APPLICATION
# =============================================================================
def _time_to_minutes(value: dtime) -> int:
    return value.hour * 60 + value.minute


def _minutes_to_time(total: int) -> dtime:
    if not 0 <= int(total) < 24 * 60:
        raise ValueError(f"Invalid minute-of-day: {total}")
    return dtime(int(total) // 60, int(total) % 60)


def _fmt_time(value: dtime) -> str:
    return value.strftime("%H:%M")


def _schedule_value(values: Sequence[Any], index: int) -> Any:
    if not values:
        return None
    return values[index] if index < len(values) else values[-1]


def _infer_linear_schedule(values: Sequence[float]) -> Tuple[float, float, int]:
    """Infer base, first-step, and plateau slot count from a supplied schedule."""
    clean = [float(x) for x in values]
    if not clean:
        return 0.0, 0.0, 1
    if len(clean) == 1:
        return clean[0], 0.0, 1
    step = clean[1] - clean[0]
    slots = len(clean)
    # If a plateau already exists, count only the increasing/ramping section.
    for i in range(1, len(clean)):
        expected = clean[0] + i * step
        if not math.isclose(clean[i], expected, rel_tol=1e-7, abs_tol=1e-7):
            slots = i
            break
    return clean[0], step, max(1, slots)


def _build_schedule(base: float, step: float, ramp_slots: int, total_slots: int,
                    *, rounding: int, integer: bool = False) -> List[Any]:
    """Build a rising schedule that plateaus after ``ramp_slots`` values."""
    n = max(1, int(total_slots))
    ramp = max(1, min(int(ramp_slots), n))
    out: List[Any] = []
    for i in range(n):
        ramp_index = min(i, ramp - 1)
        value = base + ramp_index * step
        if integer:
            out.append(int(round(value)))
        else:
            out.append(round(float(value), rounding))
    return out


@dataclass(frozen=True)
class StrategyParams:
    """Complete parameter set consumed by the imported v3 backtester."""

    entry_time: dtime
    exit_time: dtime

    loss_limit_pct_by_attempt: List[float]
    max_loss_cap_rupees: float

    profit_protect_trigger_pct: float
    profit_protect_arm_pct: float
    profit_protect_giveback_pct: float
    breakeven_arm_pct: float
    breakeven_lock_pct: float

    profit_target_pct: float
    reentry_on_profit_target: bool
    reentry_delay_after_target: List[int]

    max_daily_loss_rupees: float
    daily_profit_trail_arm_rupees: float
    daily_profit_trail_giveback_rupees: float

    max_reattempts: int
    reentry_delay_by_attempt: List[int]
    min_minutes_left_for_reentry: int
    reentry_max_premium_ratio: float

    def validate(self) -> None:
        if self.entry_time >= self.exit_time:
            raise ValueError(
                f"Entry {_fmt_time(self.entry_time)} must be before exit "
                f"{_fmt_time(self.exit_time)}"
            )
        if self.max_reattempts < 0:
            raise ValueError("max_reattempts cannot be negative")
        if self.max_loss_cap_rupees < 0:
            raise ValueError("max_loss_cap_rupees cannot be negative")
        if self.profit_target_pct < 0:
            raise ValueError("profit_target_pct cannot be negative")
        if any(float(v) < 0 for v in self.loss_limit_pct_by_attempt):
            raise ValueError("stop-loss percentages cannot be negative")
        if any(int(v) < 0 for v in self.reentry_delay_by_attempt):
            raise ValueError("re-entry delays cannot be negative")
        if any(int(v) < 0 for v in self.reentry_delay_after_target):
            raise ValueError("target re-entry delays cannot be negative")
        if self.daily_profit_trail_giveback_rupees < 0:
            raise ValueError("daily trail giveback cannot be negative")


BASE_PARAMS = StrategyParams(
    entry_time=strategy.ENTRY_TIME,
    exit_time=strategy.EXIT_TIME,
    loss_limit_pct_by_attempt=list(strategy.LOSS_LIMIT_RUPEES_BY_ATTEMPT),
    max_loss_cap_rupees=float(strategy.MAX_LOSS_LIMIT_RUPEES_BY_ATTEMPT),
    profit_protect_trigger_pct=float(strategy.PROFIT_PROTECT_TRIGGER_RUPEES),
    profit_protect_arm_pct=float(strategy.PROFIT_PROTECT_ARM_PCT),
    profit_protect_giveback_pct=float(strategy.PROFIT_PROTECT_GIVEBACK_PCT),
    breakeven_arm_pct=float(strategy.BREAKEVEN_ARM_PCT),
    breakeven_lock_pct=float(strategy.BREAKEVEN_LOCK_PCT),
    profit_target_pct=float(strategy.PROFIT_TARGET_PCT),
    reentry_on_profit_target=bool(strategy.REENTRY_ON_PROFIT_TARGET),
    reentry_delay_after_target=list(strategy.REENTRY_DELAY_AFTER_TARGET),
    max_daily_loss_rupees=float(strategy.MAX_DAILY_LOSS_RUPEES),
    daily_profit_trail_arm_rupees=float(strategy.DAILY_PROFIT_TRAIL_ARM_RUPEES),
    daily_profit_trail_giveback_rupees=float(strategy.DAILY_PROFIT_TRAIL_GIVEBACK_RUPEES),
    max_reattempts=int(strategy.MAX_REATTEMPTS),
    reentry_delay_by_attempt=list(strategy.REENTRY_DELAY_BY_ATTEMPT),
    min_minutes_left_for_reentry=int(strategy.MIN_MINUTES_LEFT_FOR_REENTRY),
    reentry_max_premium_ratio=float(strategy.REENTRY_MAX_PREMIUM_RATIO),
)
BASE_PARAMS.validate()


def apply_params(params: StrategyParams) -> None:
    """Atomically update every strategy global used by one simulation trial."""
    params.validate()

    strategy.ENTRY_TIME = params.entry_time
    strategy.ENTRY_TIME_IST = _fmt_time(params.entry_time)
    strategy.EXIT_TIME = params.exit_time
    strategy.EXIT_TIME_IST = _fmt_time(params.exit_time)

    strategy.LOSS_LIMIT_RUPEES_BY_ATTEMPT = list(params.loss_limit_pct_by_attempt)
    strategy.MAX_LOSS_LIMIT_RUPEES_BY_ATTEMPT = float(params.max_loss_cap_rupees)

    strategy.PROFIT_PROTECT_TRIGGER_RUPEES = float(params.profit_protect_trigger_pct)
    strategy.PROFIT_PROTECT_ARM_PCT = float(params.profit_protect_arm_pct)
    strategy.PROFIT_PROTECT_GIVEBACK_PCT = float(params.profit_protect_giveback_pct)
    strategy.BREAKEVEN_ARM_PCT = float(params.breakeven_arm_pct)
    strategy.BREAKEVEN_LOCK_PCT = float(params.breakeven_lock_pct)

    strategy.PROFIT_TARGET_PCT = float(params.profit_target_pct)
    strategy.REENTRY_ON_PROFIT_TARGET = bool(params.reentry_on_profit_target)
    strategy.REENTRY_DELAY_AFTER_TARGET = list(params.reentry_delay_after_target)

    strategy.MAX_DAILY_LOSS_RUPEES = float(params.max_daily_loss_rupees)
    strategy.DAILY_PROFIT_TRAIL_ARM_RUPEES = float(
        params.daily_profit_trail_arm_rupees
    )
    strategy.DAILY_PROFIT_TRAIL_GIVEBACK_RUPEES = float(
        params.daily_profit_trail_giveback_rupees
    )

    strategy.MAX_REATTEMPTS = int(params.max_reattempts)
    strategy.REENTRY_DELAY_BY_ATTEMPT = list(params.reentry_delay_by_attempt)
    strategy.MIN_MINUTES_LEFT_FOR_REENTRY = int(params.min_minutes_left_for_reentry)
    strategy.REENTRY_MAX_PREMIUM_RATIO = float(params.reentry_max_premium_ratio)

    # This optimizer is specifically for the supplied DTE-1 strategy.
    strategy.ALLOWED_DTE = [1]


# =============================================================================
# 6. PARAMETER-INDEPENDENT DATA CACHE
# =============================================================================
@dataclass
class DayGroup:
    """One reusable (underlying, trading day, nearest expiry) simulation unit."""

    und: str
    dy: date
    expiry: date
    day_opt: pd.DataFrame
    underlying_day: pd.DataFrame
    idx_all: Optional[pd.DatetimeIndex] = None
    price_book: Optional[Dict[Tuple[int, str, str, str], pd.Series]] = None
    symbols: Optional[Dict[Tuple[int, str], str]] = None
    underlying_close: Optional[pd.Series] = None


# These maps let the imported strategy retain its original function signatures
# while reading parameter-independent cached series instead of rebuilding them
# on every trial.
_PRICE_BOOK_BY_FRAME_ID: Dict[int, Dict[Tuple[int, str, str, str], pd.Series]] = {}
_SYMBOLS_BY_FRAME_ID: Dict[int, Dict[Tuple[int, str], str]] = {}
_UNDERLYING_CLOSE_BY_FRAME_ID: Dict[int, pd.Series] = {}

_ORIGINAL_BUILD_LEG_SERIES = strategy._build_leg_series
_ORIGINAL_PICK_SYMBOL = strategy._pick_symbol
_ORIGINAL_ASOF_CLOSE = strategy.asof_close


def _build_price_book(
    day_opt: pd.DataFrame,
    idx_all: pd.DatetimeIndex,
) -> Tuple[Dict[Tuple[int, str, str, str], pd.Series], Dict[Tuple[int, str], str]]:
    """Precompute the exact raw close/high/low series used by the backtester."""
    book: Dict[Tuple[int, str, str, str], pd.Series] = {}
    symbols: Dict[Tuple[int, str], str] = {}

    for (strike, opt_type), sub in day_opt.groupby(
        ["strike_int", "option_type"], sort=False
    ):
        syms = sorted(sub["instrument"].astype(str).unique().tolist())
        if not syms:
            continue
        symbol = syms[0]  # identical to strategy._pick_symbol
        strike_i = int(strike)
        opt = str(opt_type)
        symbols[(strike_i, opt)] = symbol

        selected = sub[sub["instrument"].astype(str) == symbol][
            ["date", "close", "high", "low"]
        ].copy()
        selected["date"] = strategy.ensure_ist(selected["date"])
        selected = (
            selected.sort_values("date")
            .drop_duplicates(subset=["date"], keep="last")
            .set_index("date")
        )
        for col in ("close", "high", "low"):
            book[(strike_i, opt, symbol, col)] = (
                selected[col].astype(float).reindex(idx_all)
            )
    return book, symbols


def _build_underlying_close(df: pd.DataFrame) -> pd.Series:
    """Cache the exact sorted close series used by strategy.asof_close()."""
    d = df[["date", "close"]].dropna().copy()
    d["date"] = strategy.ensure_ist(d["date"])
    return (
        d.sort_values("date")
        .drop_duplicates(subset=["date"], keep="last")
        .set_index("date")["close"]
        .astype(float)
    )


def _cached_build_leg_series(
    day_opt: pd.DataFrame,
    idx_all: pd.DatetimeIndex,
    strike: int,
    opt_type: str,
    symbol: str,
    price_col: str = "close",
    do_ffill: bool = True,
) -> pd.Series:
    book = _PRICE_BOOK_BY_FRAME_ID.get(id(day_opt))
    if book is None:
        return _ORIGINAL_BUILD_LEG_SERIES(
            day_opt, idx_all, strike, opt_type, symbol, price_col, do_ffill
        )
    series = book.get((int(strike), str(opt_type), str(symbol), str(price_col)))
    if series is None:
        return pd.Series(index=idx_all, dtype="float64")
    return series.ffill() if do_ffill else series


def _cached_pick_symbol(
    day_opt: pd.DataFrame, strike: int, opt_type: str
) -> Optional[str]:
    symbols = _SYMBOLS_BY_FRAME_ID.get(id(day_opt))
    if symbols is None:
        return _ORIGINAL_PICK_SYMBOL(day_opt, strike, opt_type)
    return symbols.get((int(strike), str(opt_type)))


def _cached_asof_close(df: pd.DataFrame, ts: pd.Timestamp) -> float:
    series = _UNDERLYING_CLOSE_BY_FRAME_ID.get(id(df))
    if series is None:
        return _ORIGINAL_ASOF_CLOSE(df, ts)
    if series.empty:
        return float("nan")
    loc = series.index.get_indexer([ts], method="pad")
    if int(loc[0]) == -1:
        return float("nan")
    return float(series.iloc[int(loc[0])])


# Install the performance wrappers once. They do not alter numerical rules.
strategy._build_leg_series = _cached_build_leg_series
strategy._pick_symbol = _cached_pick_symbol
strategy.asof_close = _cached_asof_close


def _prepare_option_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize an option pickle exactly as the supplied backtester does."""
    needed = [
        "date", "name", "type", "option_type", "strike", "expiry",
        "instrument", "high", "low", "close",
    ]
    missing = [col for col in needed if col not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    d2 = df[df["type"].astype(str).str.upper().eq("OPTION")][needed].copy()
    if d2.empty:
        return d2

    d2["date"] = strategy.ensure_ist(d2["date"])
    d2["day"] = d2["date"].dt.date
    d2["underlying"] = d2["name"].astype(str).map(strategy.normalize_underlying)
    d2 = d2[d2["underlying"].isin(strategy.TRADEABLE)]
    if d2.empty:
        return d2

    d2["expiry_date"] = pd.to_datetime(d2["expiry"], errors="coerce").dt.date
    d2["strike_num"] = pd.to_numeric(d2["strike"], errors="coerce")
    d2["strike_int"] = d2["strike_num"].round().astype("Int64")
    d2["option_type"] = d2["option_type"].astype(str).str.upper()
    d2 = d2.dropna(
        subset=["day", "underlying", "expiry_date", "strike_int", "close"]
    )
    if d2.empty:
        return d2
    d2["strike_int"] = d2["strike_int"].astype(int)
    return d2[d2["expiry_date"] >= d2["day"]]


def build_day_groups(
    pickle_paths: Sequence[str],
    min_expiry_map: Dict[Tuple[str, date], date],
    underlying_data: Dict[str, pd.DataFrame],
    window_start: date,
    window_end: date,
    *,
    max_days: Optional[int] = None,
) -> Tuple[List[DayGroup], List[Dict[str, Any]]]:
    """Read and cache all eligible day-groups once for every optimizer trial."""
    groups: List[DayGroup] = []
    skipped: List[Dict[str, Any]] = []
    processed: set[Tuple[str, date, date]] = set()

    total = len(pickle_paths)
    for file_no, raw_path in enumerate(pickle_paths, start=1):
        path = Path(raw_path)
        try:
            frame = pd.read_pickle(path)
            if not isinstance(frame, pd.DataFrame) or frame.empty:
                print(f"[LOAD {file_no}/{total}] {path.name}: empty", flush=True)
                continue
            d2 = _prepare_option_frame(frame)
            if d2.empty:
                print(f"[LOAD {file_no}/{total}] {path.name}: no usable options", flush=True)
                continue
            d2 = d2[(d2["day"] >= window_start) & (d2["day"] <= window_end)]
            if d2.empty:
                continue

            for (und, dy, expiry), group in d2.groupby(
                ["underlying", "day", "expiry_date"], sort=False
            ):
                if min_expiry_map.get((und, dy)) != expiry:
                    continue
                day_key = (str(und), dy, expiry)
                if day_key in processed:
                    skipped.append({
                        "day": dy,
                        "underlying": und,
                        "expiry": expiry,
                        "reason": "Duplicate day-group across pickles; skipped",
                    })
                    continue
                processed.add(day_key)

                underlying = underlying_data.get(str(und))
                if underlying is None:
                    skipped.append({
                        "day": dy, "underlying": und, "expiry": expiry,
                        "reason": "No downloaded underlying series",
                    })
                    continue
                uday = underlying[underlying["day"] == dy].copy()
                if uday.empty:
                    skipped.append({
                        "day": dy, "underlying": und, "expiry": expiry,
                        "reason": "Underlying minute data missing for day",
                    })
                    continue

                groups.append(DayGroup(
                    und=str(und),
                    dy=dy,
                    expiry=expiry,
                    day_opt=group.copy(),
                    underlying_day=uday,
                ))

            print(
                f"[LOAD {file_no}/{total}] {path.name}: groups={len(groups)}",
                flush=True,
            )
        except Exception as exc:
            message = f"[LOAD WARN] {path.name}: {exc}"
            if strategy.FAIL_ON_PICKLE_ERROR:
                raise RuntimeError(message) from exc
            print(message, flush=True)

    groups.sort(key=lambda g: (g.dy, g.und, g.expiry))

    if max_days is not None and max_days > 0 and groups:
        unique_days = sorted({group.dy for group in groups})
        keep_days = set(unique_days[-int(max_days):])
        groups = [group for group in groups if group.dy in keep_days]
        print(
            f"[LOAD] sample keeps {len(keep_days)} dates / {len(groups)} groups",
            flush=True,
        )

    print(f"[CACHE] Building price books for {len(groups)} groups ...", flush=True)
    for idx, group in enumerate(groups, start=1):
        group.idx_all = strategy.build_minute_index(
            group.dy, strategy.SESSION_START_IST, strategy.SESSION_END_IST
        )
        group.price_book, group.symbols = _build_price_book(
            group.day_opt, group.idx_all
        )
        group.underlying_close = _build_underlying_close(group.underlying_day)

        _PRICE_BOOK_BY_FRAME_ID[id(group.day_opt)] = group.price_book
        _SYMBOLS_BY_FRAME_ID[id(group.day_opt)] = group.symbols
        _UNDERLYING_CLOSE_BY_FRAME_ID[id(group.underlying_day)] = (
            group.underlying_close
        )

        if idx % 50 == 0 or idx == len(groups):
            print(f"[CACHE] {idx}/{len(groups)}", flush=True)

    return groups, skipped


def simulate_groups(
    params: StrategyParams,
    groups: Sequence[DayGroup],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Run one complete parameter set through the latest imported strategy."""
    apply_params(params)
    trades: List[Dict[str, Any]] = []
    skips: List[Dict[str, Any]] = []

    for group in groups:
        result, skipped = strategy.simulate_day_multi_trades(
            und=group.und,
            dy=group.dy,
            expiry=group.expiry,
            day_opt=group.day_opt,
            underlying_day=group.underlying_day,
        )
        trades.extend(row.__dict__ for row in result)
        skips.extend(skipped)

    trade_df = pd.DataFrame(trades)
    if not trade_df.empty:
        trade_df = trade_df.sort_values(
            ["day", "underlying", "trade_seq"]
        ).reset_index(drop=True)
    return trade_df, pd.DataFrame(skips)


# =============================================================================
# 7. PERFORMANCE METRICS AND OBJECTIVE
# =============================================================================
def _max_drawdown(daily_pnl: pd.Series) -> float:
    if daily_pnl.empty:
        return 0.0
    equity = daily_pnl.cumsum()
    drawdown = equity - equity.cummax()
    return float(drawdown.min())


def performance_metrics(actual_df: pd.DataFrame) -> Dict[str, Any]:
    """Compute profit and downside diagnostics from the actual-trade book."""
    empty = {
        "n_days": 0,
        "n_months": 0,
        "n_trades": 0,
        "avg_attempts_per_day": 0.0,
        "prof_day_ratio": 0.0,
        "prof_month_ratio": 0.0,
        "total_pnl": 0.0,
        "mean_month": 0.0,
        "median_month": 0.0,
        "worst_day": 0.0,
        "worst_month": 0.0,
        "max_drawdown": 0.0,
        "monthly": pd.Series(dtype="float64"),
        "daily": pd.Series(dtype="float64"),
        "exit_counts": {},
    }
    if actual_df is None or actual_df.empty:
        return empty

    frame = actual_df.copy()
    dt = pd.to_datetime(frame["day"])
    daily = frame.groupby(dt.dt.date)["exit_pnl"].sum().sort_index()
    monthly = frame.groupby(dt.dt.to_period("M"))["exit_pnl"].sum().sort_index()
    exit_counts = (
        frame["exit_reason"].astype(str).value_counts().sort_index().to_dict()
    )

    return {
        "n_days": int(len(daily)),
        "n_months": int(len(monthly)),
        "n_trades": int(len(frame)),
        "avg_attempts_per_day": float(len(frame) / len(daily)) if len(daily) else 0.0,
        "prof_day_ratio": float((daily > 0).mean()),
        "prof_month_ratio": float((monthly > 0).mean()),
        "total_pnl": float(daily.sum()),
        "mean_month": float(monthly.mean()),
        "median_month": float(monthly.median()),
        "worst_day": float(daily.min()),
        "worst_month": float(monthly.min()),
        "max_drawdown": _max_drawdown(daily),
        "monthly": monthly,
        "daily": daily,
        "exit_counts": exit_counts,
    }


def score_from_metrics(metrics: Dict[str, Any]) -> float:
    """Pure full-sample total-net-profit objective with optional hard guards."""
    if metrics["n_days"] < OPT_MIN_DAYS or metrics["n_months"] < OPT_MIN_MONTHS:
        return DISQUALIFIED_SCORE
    if (
        OPT_MAX_ALLOWED_DRAWDOWN > 0
        and metrics["max_drawdown"] < -float(OPT_MAX_ALLOWED_DRAWDOWN)
    ):
        return DISQUALIFIED_SCORE
    return float(metrics["total_pnl"])


def cv_score(actual_df: pd.DataFrame, folds: int) -> float:
    """Contiguous walk-forward profit score for optional robustness searches."""
    if actual_df is None or actual_df.empty:
        return DISQUALIFIED_SCORE
    frame = actual_df.copy()
    dt = pd.to_datetime(frame["day"])
    daily = frame.groupby(dt.dt.date)["exit_pnl"].sum().sort_index()
    monthly = frame.groupby(dt.dt.to_period("M"))["exit_pnl"].sum().sort_index()
    if len(daily) < OPT_MIN_DAYS or len(monthly) < max(OPT_MIN_MONTHS, folds):
        return DISQUALIFIED_SCORE

    items = list(monthly.items())
    block_profits: List[float] = []
    n = len(items)
    for fold in range(int(folds)):
        lo = fold * n // int(folds)
        hi = (fold + 1) * n // int(folds)
        block = items[lo:hi]
        if block:
            block_profits.append(float(sum(value for _, value in block)))
    if not block_profits:
        return DISQUALIFIED_SCORE
    values = pd.Series(block_profits, dtype="float64")
    return float(values.mean() - OPT_CV_PENALTY * values.std(ddof=0))


# =============================================================================
# 8. OPTUNA PARAMETER MAPPING
# =============================================================================
def _suggest_float_or_default(
    trial: Any,
    name: str,
    low: float,
    high: float,
    default: float,
    *,
    step: Optional[float] = None,
    enabled: bool = True,
) -> float:
    if not enabled:
        return float(default)
    kwargs: Dict[str, Any] = {}
    if step is not None:
        kwargs["step"] = step
    return float(trial.suggest_float(name, low, high, **kwargs))


def _suggest_int_or_default(
    trial: Any,
    name: str,
    low: int,
    high: int,
    default: int,
    *,
    step: int = 1,
    enabled: bool = True,
) -> int:
    if not enabled:
        return int(default)
    return int(trial.suggest_int(name, low, high, step=step))


def _params_from_trial(trial: Any, base: StrategyParams) -> StrategyParams:
    """Translate Optuna suggestions into one complete v3 strategy config."""
    full = OPT_SEARCH_PROFILE.strip().lower() == "full"
    if OPT_SEARCH_PROFILE.strip().lower() not in {"core", "full"}:
        raise ValueError("OPT_SEARCH_PROFILE must be 'core' or 'full'")

    entry_offset = trial.suggest_int(
        "entry_offset_from_0916_min",
        ENTRY_OFFSET_MIN,
        ENTRY_OFFSET_MAX,
        step=ENTRY_OFFSET_STEP,
    )
    exit_offset = trial.suggest_int(
        "exit_offset_from_1520_min",
        EXIT_OFFSET_MIN,
        EXIT_OFFSET_MAX,
        step=EXIT_OFFSET_STEP,
    )
    entry_time = _minutes_to_time(ENTRY_ANCHOR_MINUTE + int(entry_offset))
    exit_time = _minutes_to_time(EXIT_ANCHOR_MINUTE + int(exit_offset))

    max_reattempts = trial.suggest_int(
        "max_reattempts", MAX_REATTEMPTS_MIN, MAX_REATTEMPTS_MAX
    )
    total_slots = max_reattempts + 1

    target_pct = trial.suggest_float(
        "profit_target_pct",
        PROFIT_TARGET_MIN,
        PROFIT_TARGET_MAX,
        step=PROFIT_TARGET_STEP,
    )
    target_delay = trial.suggest_int(
        "target_reentry_delay_min",
        TARGET_REENTRY_DELAY_MIN,
        TARGET_REENTRY_DELAY_MAX,
    )

    stop_cap = trial.suggest_int(
        "stop_cap_rupees", STOP_CAP_MIN, STOP_CAP_MAX, step=STOP_CAP_STEP
    )

    delay_base = trial.suggest_int(
        "stop_reentry_delay_base_min", STOP_DELAY_BASE_MIN, STOP_DELAY_BASE_MAX
    )
    delay_step = trial.suggest_int(
        "stop_reentry_delay_step_min", STOP_DELAY_STEP_MIN, STOP_DELAY_STEP_MAX
    )
    delay_ramp_slots = trial.suggest_int(
        "stop_reentry_delay_ramp_slots",
        STOP_DELAY_RAMP_SLOTS_MIN,
        STOP_DELAY_RAMP_SLOTS_MAX,
    )
    stop_delay_schedule = _build_schedule(
        float(delay_base),
        float(delay_step),
        delay_ramp_slots,
        total_slots,
        rounding=0,
        integer=True,
    )

    premium_ratio = trial.suggest_float(
        "reentry_max_premium_ratio",
        PREMIUM_RATIO_MIN,
        PREMIUM_RATIO_MAX,
        step=PREMIUM_RATIO_STEP,
    )
    min_minutes_left = int(
        trial.suggest_categorical(
            "min_minutes_left_for_reentry", MIN_MINUTES_LEFT_CHOICES
        )
    )

    if full:
        sl_base = trial.suggest_float("sl_base_pct", SL_BASE_MIN, SL_BASE_MAX)
        sl_step = trial.suggest_float("sl_step_pct", SL_STEP_MIN, SL_STEP_MAX)
        sl_ramp_slots = trial.suggest_int(
            "sl_ramp_slots", SL_RAMP_SLOTS_MIN, SL_RAMP_SLOTS_MAX
        )
        stop_schedule = _build_schedule(
            sl_base,
            sl_step,
            sl_ramp_slots,
            total_slots,
            rounding=4,
            integer=False,
        )
        stop_schedule = [min(0.95, float(v)) for v in stop_schedule]

        daily_loss = float(
            trial.suggest_categorical("max_daily_loss_rupees", DAILY_LOSS_CHOICES)
        )
        trail_arm = float(
            trial.suggest_categorical(
                "daily_profit_trail_arm_rupees", DAILY_TRAIL_ARM_CHOICES
            )
        )
        trail_giveback = float(
            trial.suggest_categorical(
                "daily_profit_trail_giveback_rupees",
                DAILY_TRAIL_GIVEBACK_CHOICES,
            )
        )

        protect_arm = trial.suggest_float(
            "profit_protect_arm_pct", PROTECT_ARM_MIN, PROTECT_ARM_MAX
        )
        protect_giveback = trial.suggest_float(
            "profit_protect_giveback_pct",
            PROTECT_GIVEBACK_MIN,
            PROTECT_GIVEBACK_MAX,
        )
    else:
        stop_schedule = [
            float(_schedule_value(base.loss_limit_pct_by_attempt, i))
            for i in range(total_slots)
        ]
        daily_loss = base.max_daily_loss_rupees
        trail_arm = base.daily_profit_trail_arm_rupees
        trail_giveback = base.daily_profit_trail_giveback_rupees
        protect_arm = base.profit_protect_arm_pct
        protect_giveback = base.profit_protect_giveback_pct

    params = StrategyParams(
        entry_time=entry_time,
        exit_time=exit_time,
        loss_limit_pct_by_attempt=[float(v) for v in stop_schedule],
        max_loss_cap_rupees=float(stop_cap),
        profit_protect_trigger_pct=base.profit_protect_trigger_pct,
        profit_protect_arm_pct=float(protect_arm),
        profit_protect_giveback_pct=float(protect_giveback),
        breakeven_arm_pct=base.breakeven_arm_pct,
        breakeven_lock_pct=base.breakeven_lock_pct,
        profit_target_pct=float(target_pct),
        reentry_on_profit_target=True,  # defining v3 behaviour
        reentry_delay_after_target=[int(target_delay)],
        max_daily_loss_rupees=float(daily_loss),
        daily_profit_trail_arm_rupees=float(trail_arm),
        daily_profit_trail_giveback_rupees=float(trail_giveback),
        max_reattempts=int(max_reattempts),
        reentry_delay_by_attempt=[int(v) for v in stop_delay_schedule],
        min_minutes_left_for_reentry=int(min_minutes_left),
        reentry_max_premium_ratio=float(premium_ratio),
    )
    params.validate()
    return params


class FrozenTrialView:
    """Reconstruct StrategyParams from a finished Optuna trial."""

    def __init__(self, params: Dict[str, Any]):
        self.params = dict(params)

    def suggest_int(self, name: str, *args: Any, **kwargs: Any) -> int:
        return int(self.params[name])

    def suggest_float(self, name: str, *args: Any, **kwargs: Any) -> float:
        return float(self.params[name])

    def suggest_categorical(self, name: str, choices: Sequence[Any]) -> Any:
        return self.params[name]


# =============================================================================
# 9. BASELINE TRIAL AND OUTPUT HELPERS
# =============================================================================
def _nearest_grid_int(value: int, low: int, high: int, step: int) -> int:
    clipped = min(max(int(value), int(low)), int(high))
    return int(low + round((clipped - low) / step) * step)


def _nearest_choice(value: float, choices: Sequence[Any]) -> Any:
    return min(choices, key=lambda x: abs(float(x) - float(value)))


def baseline_trial_parameters(base: StrategyParams) -> Dict[str, Any]:
    """Map the supplied properties configuration into the current search space."""
    delay_base, delay_step, delay_slots = _infer_linear_schedule(
        base.reentry_delay_by_attempt
    )
    mapping: Dict[str, Any] = {
        "entry_offset_from_0916_min": _nearest_grid_int(
            _time_to_minutes(base.entry_time) - ENTRY_ANCHOR_MINUTE,
            ENTRY_OFFSET_MIN,
            ENTRY_OFFSET_MAX,
            ENTRY_OFFSET_STEP,
        ),
        "exit_offset_from_1520_min": _nearest_grid_int(
            _time_to_minutes(base.exit_time) - EXIT_ANCHOR_MINUTE,
            EXIT_OFFSET_MIN,
            EXIT_OFFSET_MAX,
            EXIT_OFFSET_STEP,
        ),
        "max_reattempts": min(
            max(base.max_reattempts, MAX_REATTEMPTS_MIN), MAX_REATTEMPTS_MAX
        ),
        "profit_target_pct": round(
            min(max(base.profit_target_pct, PROFIT_TARGET_MIN), PROFIT_TARGET_MAX)
            / PROFIT_TARGET_STEP
        ) * PROFIT_TARGET_STEP,
        "target_reentry_delay_min": min(
            max(
                int(_schedule_value(base.reentry_delay_after_target or [1], 0)),
                TARGET_REENTRY_DELAY_MIN,
            ),
            TARGET_REENTRY_DELAY_MAX,
        ),
        "stop_cap_rupees": _nearest_grid_int(
            int(round(base.max_loss_cap_rupees)),
            STOP_CAP_MIN,
            STOP_CAP_MAX,
            STOP_CAP_STEP,
        ),
        "stop_reentry_delay_base_min": min(
            max(int(round(delay_base)), STOP_DELAY_BASE_MIN), STOP_DELAY_BASE_MAX
        ),
        "stop_reentry_delay_step_min": min(
            max(int(round(delay_step)), STOP_DELAY_STEP_MIN), STOP_DELAY_STEP_MAX
        ),
        "stop_reentry_delay_ramp_slots": min(
            max(int(delay_slots), STOP_DELAY_RAMP_SLOTS_MIN),
            STOP_DELAY_RAMP_SLOTS_MAX,
        ),
        "reentry_max_premium_ratio": round(
            min(
                max(base.reentry_max_premium_ratio, PREMIUM_RATIO_MIN),
                PREMIUM_RATIO_MAX,
            )
            / PREMIUM_RATIO_STEP
        ) * PREMIUM_RATIO_STEP,
        "min_minutes_left_for_reentry": _nearest_choice(
            base.min_minutes_left_for_reentry, MIN_MINUTES_LEFT_CHOICES
        ),
    }

    if OPT_SEARCH_PROFILE.strip().lower() == "full":
        sl_base, sl_step, sl_slots = _infer_linear_schedule(
            base.loss_limit_pct_by_attempt
        )
        mapping.update({
            "sl_base_pct": min(max(sl_base, SL_BASE_MIN), SL_BASE_MAX),
            "sl_step_pct": min(max(sl_step, SL_STEP_MIN), SL_STEP_MAX),
            "sl_ramp_slots": min(
                max(sl_slots, SL_RAMP_SLOTS_MIN), SL_RAMP_SLOTS_MAX
            ),
            "max_daily_loss_rupees": _nearest_choice(
                base.max_daily_loss_rupees, DAILY_LOSS_CHOICES
            ),
            "daily_profit_trail_arm_rupees": _nearest_choice(
                base.daily_profit_trail_arm_rupees, DAILY_TRAIL_ARM_CHOICES
            ),
            "daily_profit_trail_giveback_rupees": _nearest_choice(
                base.daily_profit_trail_giveback_rupees,
                DAILY_TRAIL_GIVEBACK_CHOICES,
            ),
            "profit_protect_arm_pct": min(
                max(base.profit_protect_arm_pct, PROTECT_ARM_MIN), PROTECT_ARM_MAX
            ),
            "profit_protect_giveback_pct": min(
                max(
                    base.profit_protect_giveback_pct,
                    PROTECT_GIVEBACK_MIN,
                ),
                PROTECT_GIVEBACK_MAX,
            ),
        })
    return mapping


def _inr(value: float) -> str:
    """ASCII-safe Indian-number formatting for Windows terminals."""
    try:
        number = int(round(float(value)))
    except (TypeError, ValueError):
        number = 0
    sign = "-" if number < 0 else ""
    digits = str(abs(number))
    if len(digits) <= 3:
        body = digits
    else:
        last3, rest = digits[-3:], digits[:-3]
        pairs: List[str] = []
        while len(rest) > 2:
            pairs.insert(0, rest[-2:])
            rest = rest[:-2]
        if rest:
            pairs.insert(0, rest)
        body = ",".join(pairs + [last3])
    return f"Rs.{sign}{body}"


def _serialize_schedule(values: Sequence[Any], decimals: int = 4) -> str:
    rendered: List[str] = []
    for value in values:
        if isinstance(value, int):
            rendered.append(str(value))
        else:
            rendered.append(f"{float(value):.{decimals}f}".rstrip("0").rstrip("."))
    return ",".join(rendered)


def params_to_property_values(params: StrategyParams) -> Dict[str, str]:
    """Convert a winning parameter set into backtester-compatible properties."""
    return {
        "ENTRY_TIME_IST": _fmt_time(params.entry_time),
        "EXIT_TIME_IST": _fmt_time(params.exit_time),
        "ALLOWED_DTE": "1",
        "LOSS_LIMIT_RUPEES_BY_ATTEMPT": _serialize_schedule(
            params.loss_limit_pct_by_attempt
        ),
        "MAX_LOSS_LIMIT_RUPEES_BY_ATTEMPT": str(
            int(round(params.max_loss_cap_rupees))
        ),
        "PROFIT_PROTECT_TRIGGER_RUPEES": f"{params.profit_protect_trigger_pct:.4f}",
        "PROFIT_PROTECT_ARM_PCT": f"{params.profit_protect_arm_pct:.4f}",
        "PROFIT_PROTECT_GIVEBACK_PCT": f"{params.profit_protect_giveback_pct:.4f}",
        "BREAKEVEN_ARM_PCT": f"{params.breakeven_arm_pct:.4f}",
        "BREAKEVEN_LOCK_PCT": f"{params.breakeven_lock_pct:.4f}",
        "PROFIT_TARGET_PCT": f"{params.profit_target_pct:.4f}",
        "REENTRY_ON_PROFIT_TARGET": "1" if params.reentry_on_profit_target else "0",
        "REENTRY_DELAY_AFTER_TARGET": _serialize_schedule(
            params.reentry_delay_after_target, decimals=0
        ),
        "MAX_DAILY_LOSS_RUPEES": str(int(round(params.max_daily_loss_rupees))),
        "DAILY_PROFIT_TRAIL_ARM_RUPEES": str(
            int(round(params.daily_profit_trail_arm_rupees))
        ),
        "DAILY_PROFIT_TRAIL_GIVEBACK_RUPEES": str(
            int(round(params.daily_profit_trail_giveback_rupees))
        ),
        "MAX_REATTEMPTS": str(int(params.max_reattempts)),
        "REENTRY_DELAY_BY_ATTEMPT": _serialize_schedule(
            params.reentry_delay_by_attempt, decimals=0
        ),
        "MIN_MINUTES_LEFT_FOR_REENTRY": str(
            int(params.min_minutes_left_for_reentry)
        ),
        "REENTRY_MAX_PREMIUM_RATIO": f"{params.reentry_max_premium_ratio:.4f}",
    }


def write_optimized_properties(
    output_path: Path,
    base_path: Path,
    params: StrategyParams,
    metrics: Dict[str, Any],
) -> None:
    """Write a clean ready-to-run properties file for the winning trial.

    The original config contains historical result commentary from the earlier
    low-based target accounting. Those comments are intentionally not copied,
    because this optimizer evaluates the latest synchronized close-based logic.
    All actual KEY=VALUE settings are retained, with optimized values replaced.
    """
    base_values = _read_properties(base_path)
    replacements = params_to_property_values(params)
    merged = dict(base_values)  # insertion order from the supplied file
    merged.update(replacements)

    output: List[str] = [
        "# =============================================================================",
        "# OPTUNA-OPTIMIZED DTE-1 V3 CONFIGURATION",
        f"# Generated: {datetime.now().isoformat(timespec='seconds')}",
        f"# Backtester: {BACKTEST_SCRIPT_PATH.name}",
        f"# Objective score: {_inr(metrics.get('objective_score', 0.0))}",
        f"# Full-sample net P&L: {_inr(metrics.get('total_pnl', 0.0))}",
        f"# Max drawdown: {_inr(metrics.get('max_drawdown', 0.0))}",
        "# Profit targets and positive exits use synchronized CE+PE minute closes.",
        "# Confirm the winner with RUN_MODE='verify_baseline' before deployment.",
        "# =============================================================================",
        "",
    ]

    section_order = [
        ("Data", ["PICKLES_DIR"]),
        ("Entry and exit", ["ENTRY_TIME_IST", "EXIT_TIME_IST", "ALLOWED_DTE"]),
        ("Stop-loss", [
            "LOSS_LIMIT_RUPEES_BY_ATTEMPT",
            "MAX_LOSS_LIMIT_RUPEES_BY_ATTEMPT",
        ]),
        ("Profit protection", [
            "PROFIT_PROTECT_TRIGGER_RUPEES",
            "PROFIT_PROTECT_ARM_PCT",
            "PROFIT_PROTECT_GIVEBACK_PCT",
            "BREAKEVEN_ARM_PCT",
            "BREAKEVEN_LOCK_PCT",
        ]),
        ("Profit target and target recycling", [
            "PROFIT_TARGET_PCT",
            "REENTRY_ON_PROFIT_TARGET",
            "REENTRY_DELAY_AFTER_TARGET",
        ]),
        ("Daily controls", [
            "MAX_DAILY_LOSS_RUPEES",
            "DAILY_PROFIT_TRAIL_ARM_RUPEES",
            "DAILY_PROFIT_TRAIL_GIVEBACK_RUPEES",
        ]),
        ("Re-entries", [
            "MAX_REATTEMPTS",
            "REENTRY_DELAY_BY_ATTEMPT",
            "MIN_MINUTES_LEFT_FOR_REENTRY",
            "REENTRY_MAX_PREMIUM_RATIO",
        ]),
        ("Backtest window", ["LOOKBACK_MONTHS", "FAIL_ON_PICKLE_ERROR"]),
    ]

    written: set[str] = set()
    for title, keys in section_order:
        present = [key for key in keys if key in merged]
        if not present:
            continue
        output.append(f"# ---- {title} " + "-" * max(1, 68 - len(title)))
        for key in present:
            output.append(f"{key}={merged[key]}")
            written.add(key)
        output.append("")

    extras = [key for key in merged if key not in written]
    if extras:
        output.append("# ---- Other retained settings -----------------------------------------------")
        output.extend(f"{key}={merged[key]}" for key in extras)
        output.append("")

    output_path.write_text("\n".join(output), encoding="utf-8")


TRIAL_COLUMNS = [
    "run_index", "trial_number", "state", "score", "elapsed_s",
    "entry_time", "exit_time", "profit_target_pct", "stop_cap_rupees",
    "max_reattempts", "target_reentry_delay_min",
    "stop_reentry_delay_schedule", "loss_limit_schedule",
    "reentry_max_premium_ratio", "min_minutes_left_for_reentry",
    "max_daily_loss_rupees", "daily_trail_arm_rupees",
    "daily_trail_giveback_rupees", "profit_protect_arm_pct",
    "profit_protect_giveback_pct", "net_pnl", "max_drawdown",
    "worst_day", "worst_month", "prof_day_ratio", "prof_month_ratio",
    "n_days", "n_months", "n_trades", "avg_attempts_per_day",
    "exit_counts", "error",
]


def trial_record(
    trial: Any,
    params: StrategyParams,
    run_index: int,
    elapsed: float,
) -> Dict[str, Any]:
    attrs = trial.user_attrs
    return {
        "run_index": run_index,
        "trial_number": trial.number,
        "state": str(getattr(trial, "state", "")),
        "score": trial.value,
        "elapsed_s": round(elapsed, 2),
        "entry_time": _fmt_time(params.entry_time),
        "exit_time": _fmt_time(params.exit_time),
        "profit_target_pct": params.profit_target_pct,
        "stop_cap_rupees": params.max_loss_cap_rupees,
        "max_reattempts": params.max_reattempts,
        "target_reentry_delay_min": int(
            _schedule_value(params.reentry_delay_after_target, 0) or 0
        ),
        "stop_reentry_delay_schedule": _serialize_schedule(
            params.reentry_delay_by_attempt, decimals=0
        ),
        "loss_limit_schedule": _serialize_schedule(
            params.loss_limit_pct_by_attempt
        ),
        "reentry_max_premium_ratio": params.reentry_max_premium_ratio,
        "min_minutes_left_for_reentry": params.min_minutes_left_for_reentry,
        "max_daily_loss_rupees": params.max_daily_loss_rupees,
        "daily_trail_arm_rupees": params.daily_profit_trail_arm_rupees,
        "daily_trail_giveback_rupees": params.daily_profit_trail_giveback_rupees,
        "profit_protect_arm_pct": params.profit_protect_arm_pct,
        "profit_protect_giveback_pct": params.profit_protect_giveback_pct,
        "net_pnl": attrs.get("total_pnl", 0.0),
        "max_drawdown": attrs.get("max_drawdown", 0.0),
        "worst_day": attrs.get("worst_day", 0.0),
        "worst_month": attrs.get("worst_month", 0.0),
        "prof_day_ratio": attrs.get("prof_day_ratio", 0.0),
        "prof_month_ratio": attrs.get("prof_month_ratio", 0.0),
        "n_days": attrs.get("n_days", 0),
        "n_months": attrs.get("n_months", 0),
        "n_trades": attrs.get("n_trades", 0),
        "avg_attempts_per_day": attrs.get("avg_attempts_per_day", 0.0),
        "exit_counts": json.dumps(attrs.get("exit_counts", {}), sort_keys=True),
        "error": attrs.get("error", ""),
    }


# =============================================================================
# 10. OPTIMIZER
# =============================================================================
def optimize(
    groups: Sequence[DayGroup],
    min_expiry_map: Dict[Tuple[str, date], date],
):
    import optuna

    if OPT_N_JOBS != 1:
        raise RuntimeError(
            "OPT_N_JOBS must remain 1 because strategy parameters are module globals."
        )

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    output_dir = Path(OPT_OUTPUT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)
    run_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    trial_csv = output_dir / f"{OPT_STUDY_NAME}_{run_stamp}_trials.csv"
    full_csv = output_dir / f"{OPT_STUDY_NAME}_{run_stamp}_full.csv"
    best_properties = output_dir / f"{OPT_STUDY_NAME}_{run_stamp}_best.properties"
    best_excel = output_dir / f"{OPT_STUDY_NAME}_{run_stamp}_best.xlsx"

    storage: Optional[str] = None
    if OPT_SAVE_DB:
        db_path = output_dir / f"{OPT_STUDY_NAME}.db"
        storage = f"sqlite:///{db_path.as_posix()}"
        print(f"[OPT] Resumable study DB: {db_path}", flush=True)

    study = optuna.create_study(
        direction="maximize",
        sampler=optuna.samplers.TPESampler(seed=OPT_SEED),
        study_name=OPT_STUDY_NAME,
        storage=storage,
        load_if_exists=bool(storage),
    )

    if OPT_ENQUEUE_BASELINE and not any(
        trial.state.name == "COMPLETE" for trial in study.trials
    ):
        baseline_map = baseline_trial_parameters(BASE_PARAMS)
        study.enqueue_trial(baseline_map)
        print("[OPT] Baseline configuration enqueued as the first trial.", flush=True)

    csv_handle = trial_csv.open("w", newline="", encoding="utf-8")
    writer = csv.DictWriter(csv_handle, fieldnames=TRIAL_COLUMNS)
    writer.writeheader()
    csv_handle.flush()

    start = time.time()
    run_counter = {"count": 0}

    def objective(trial: Any) -> float:
        try:
            params = _params_from_trial(trial, BASE_PARAMS)
            all_df, _ = simulate_groups(params, groups)
            actual_df = strategy.build_actual_trades_df(all_df, min_expiry_map)
            metrics = performance_metrics(actual_df)

            scalar_keys = [
                "n_days", "n_months", "n_trades", "avg_attempts_per_day",
                "prof_day_ratio", "prof_month_ratio", "total_pnl", "mean_month",
                "median_month", "worst_day", "worst_month", "max_drawdown",
                "exit_counts",
            ]
            for key in scalar_keys:
                trial.set_user_attr(key, metrics[key])
            trial.set_user_attr(
                "monthly_pnl",
                {str(period): float(value) for period, value in metrics["monthly"].items()},
            )

            if OPT_CV_FOLDS > 1:
                score = cv_score(actual_df, OPT_CV_FOLDS)
            else:
                score = score_from_metrics(metrics)
            trial.set_user_attr("objective_score", score)
            return float(score)
        except Exception as exc:
            # A malformed/operationally invalid parameter set is disqualified,
            # but the optimization continues and records the error.
            trial.set_user_attr("error", f"{type(exc).__name__}: {exc}")
            trial.set_user_attr("objective_score", DISQUALIFIED_SCORE)
            return DISQUALIFIED_SCORE

    def progress(study_: Any, trial: Any) -> None:
        run_counter["count"] += 1
        run_index = run_counter["count"]
        elapsed = time.time() - start
        eta = elapsed / run_index * max(0, OPT_TRIALS - run_index)

        try:
            params = _params_from_trial(FrozenTrialView(trial.params), BASE_PARAMS)
        except Exception:
            params = BASE_PARAMS

        try:
            writer.writerow(trial_record(trial, params, run_index, elapsed))
            csv_handle.flush()
        except Exception as exc:
            print(f"[OPT WARN] Trial CSV write failed: {exc}", flush=True)

        if run_index % max(1, OPT_PROGRESS_EVERY) == 0:
            attrs = trial.user_attrs
            try:
                best_value = study_.best_value
            except Exception:
                best_value = DISQUALIFIED_SCORE
            print(
                f"[TRIAL {run_index:>4}/{OPT_TRIALS}] "
                f"score={_inr(trial.value or 0)} "
                f"net={_inr(attrs.get('total_pnl', 0))} "
                f"DD={_inr(attrs.get('max_drawdown', 0))} "
                f"worst_day={_inr(attrs.get('worst_day', 0))} "
                f"prof_days={float(attrs.get('prof_day_ratio', 0))*100:5.1f}% "
                f"trades={int(attrs.get('n_trades', 0))} "
                f"| BEST={_inr(best_value)} | elapsed={elapsed:.0f}s eta={eta:.0f}s",
                flush=True,
            )
            error = attrs.get("error")
            if error:
                print(f"    disqualified/error: {error}", flush=True)

    print("=" * 100, flush=True)
    print(f"[OPT] Backtester : {BACKTEST_SCRIPT_PATH}", flush=True)
    print(f"[OPT] Base config: {BASE_STRATEGY_CONFIG}", flush=True)
    print(
        f"[OPT] Profile={OPT_SEARCH_PROFILE}; trials={OPT_TRIALS}; "
        f"CV folds={OPT_CV_FOLDS}; groups={len(groups)}",
        flush=True,
    )
    print(f"[OPT] Per-trial CSV: {trial_csv}", flush=True)
    print("=" * 100, flush=True)

    try:
        study.optimize(
            objective,
            n_trials=OPT_TRIALS,
            callbacks=[progress],
            n_jobs=1,
            show_progress_bar=False,
            gc_after_trial=False,
        )
    finally:
        csv_handle.close()
        try:
            study.trials_dataframe().to_csv(full_csv, index=False)
            print(f"[OPT] Full Optuna table: {full_csv}", flush=True)
        except Exception as exc:
            print(f"[OPT WARN] Full trial export failed: {exc}", flush=True)

    best_trial = study.best_trial
    best_params = _params_from_trial(
        FrozenTrialView(best_trial.params), BASE_PARAMS
    )

    # Re-run the best configuration through the complete cached sample. This
    # supplies a detailed output workbook and protects against stale user attrs.
    best_all, best_skips = simulate_groups(best_params, groups)
    best_actual = strategy.build_actual_trades_df(best_all, min_expiry_map)
    best_metrics = performance_metrics(best_actual)
    best_metrics["objective_score"] = float(best_trial.value)

    print("\n" + "=" * 40 + " BEST CONFIG " + "=" * 40)
    print(f"Objective score                 = {_inr(best_trial.value)}")
    print(f"Full-sample net P&L             = {_inr(best_metrics['total_pnl'])}")
    print(f"Maximum drawdown                = {_inr(best_metrics['max_drawdown'])}")
    print(f"Worst day / month               = {_inr(best_metrics['worst_day'])} / {_inr(best_metrics['worst_month'])}")
    print(f"Profitable days / months        = {best_metrics['prof_day_ratio']*100:.1f}% / {best_metrics['prof_month_ratio']*100:.1f}%")
    print(f"Trades / avg attempts per day   = {best_metrics['n_trades']} / {best_metrics['avg_attempts_per_day']:.2f}")
    print(f"ENTRY_TIME_IST                  = {_fmt_time(best_params.entry_time)}")
    print(f"EXIT_TIME_IST                   = {_fmt_time(best_params.exit_time)}")
    print(f"LOSS_LIMIT_RUPEES_BY_ATTEMPT    = {_serialize_schedule(best_params.loss_limit_pct_by_attempt)}")
    print(f"MAX_LOSS_LIMIT_RUPEES_BY_ATTEMPT= {best_params.max_loss_cap_rupees:.0f}")
    print(f"PROFIT_TARGET_PCT               = {best_params.profit_target_pct:.4f}")
    print(f"REENTRY_DELAY_AFTER_TARGET      = {_serialize_schedule(best_params.reentry_delay_after_target, 0)}")
    print(f"MAX_REATTEMPTS                  = {best_params.max_reattempts}")
    print(f"REENTRY_DELAY_BY_ATTEMPT        = {_serialize_schedule(best_params.reentry_delay_by_attempt, 0)}")
    print(f"MIN_MINUTES_LEFT_FOR_REENTRY    = {best_params.min_minutes_left_for_reentry}")
    print(f"REENTRY_MAX_PREMIUM_RATIO       = {best_params.reentry_max_premium_ratio:.4f}")
    print(f"MAX_DAILY_LOSS_RUPEES           = {best_params.max_daily_loss_rupees:.0f}")
    print(f"DAILY_PROFIT_TRAIL_ARM_RUPEES   = {best_params.daily_profit_trail_arm_rupees:.0f}")
    print(f"DAILY_PROFIT_TRAIL_GIVEBACK     = {best_params.daily_profit_trail_giveback_rupees:.0f}")
    print(f"Exit counts                     = {best_metrics['exit_counts']}")

    if OPT_WRITE_BEST_PROPERTIES:
        write_optimized_properties(
            best_properties, BASE_STRATEGY_CONFIG, best_params, best_metrics
        )
        print(f"[BEST] Optimized properties: {best_properties}")

    if OPT_WRITE_BEST_EXCEL:
        apply_params(best_params)
        strategy.OUTPUT_XLSX = str(best_excel)
        strategy.write_excel(best_all, best_actual, best_skips)
        print(f"[BEST] Detailed verification workbook: {best_excel}")

    print(f"[BEST] Per-trial results: {trial_csv}")
    return study, best_params, best_metrics


# =============================================================================
# 11. END-TO-END DATA PREPARATION
# =============================================================================
def prepare_optimizer_data() -> Tuple[
    List[DayGroup], Dict[Tuple[str, date], date]
]:
    """Scan pickles, download underlyings once, and prepare cached day-groups."""
    paths = sorted(
        list(Path(strategy.PICKLES_DIR).glob("*.pkl"))
        + list(Path(strategy.PICKLES_DIR).glob("*.pickle"))
    )
    if not paths:
        raise FileNotFoundError(
            f"No .pkl/.pickle files found in PICKLES_DIR={strategy.PICKLES_DIR}"
        )
    if SAMPLE_MAX_PICKLES is not None and SAMPLE_MAX_PICKLES > 0:
        paths = paths[: int(SAMPLE_MAX_PICKLES)]

    path_strings = [str(path) for path in paths]
    print(f"[PHASE 1] Scanning {len(path_strings)} option pickle(s) ...", flush=True)
    end_day, min_expiry_map, min_day_seen = strategy.scan_pickles_pass1(path_strings)
    window_start = strategy.determine_backtest_window_start(min_day_seen, end_day)
    print(f"[PHASE 1] Window: {window_start} -> {end_day}", flush=True)

    print("[PHASE 2] Initializing Kite and downloading underlying data ...", flush=True)
    kite = strategy.oUtils.intialize_kite_api()
    underlying_data = strategy.download_underlyings(kite, window_start, end_day)

    print("[PHASE 3] Building reusable day-groups and caches ...", flush=True)
    groups, _ = build_day_groups(
        path_strings,
        min_expiry_map,
        underlying_data,
        window_start,
        end_day,
        max_days=SAMPLE_MAX_DAYS,
    )
    if not groups:
        raise RuntimeError("No eligible DTE-1 day-groups were built.")
    return groups, min_expiry_map


# =============================================================================
# 12. SELF-TESTS
# =============================================================================
def run_self_tests() -> None:
    """Fast tests that do not read market data or call Kite."""
    assert BACKTEST_SCRIPT_PATH.exists()
    assert BASE_STRATEGY_CONFIG.exists()
    assert list(strategy.ALLOWED_DTE) == [1]

    # Confirm the latest source contains the close-based target implementation
    # and no longer contains the old independent-low target variable.
    source = BACKTEST_SCRIPT_PATH.read_text(encoding="utf-8")
    assert "tp_hit = pnl >= float(target_rupees)" in source, (
        "Latest backtester does not contain synchronized close-based target logic"
    )
    assert "pnl_best_all" not in source, (
        "Old independent CE/PE low-based target logic is still present"
    )

    # Baseline round-trip through the search-space mapping.
    baseline_map = baseline_trial_parameters(BASE_PARAMS)
    round_trip = _params_from_trial(FrozenTrialView(baseline_map), BASE_PARAMS)
    round_trip.validate()
    assert round_trip.entry_time < round_trip.exit_time
    assert round_trip.reentry_on_profit_target is True
    assert round_trip.profit_target_pct > 0

    # Schedule plateau regression test: 7,10,13,16,19,22,22,...
    schedule = _build_schedule(7, 3, 6, 11, rounding=0, integer=True)
    assert schedule[:7] == [7, 10, 13, 16, 19, 22, 22]

    print("[SELF TEST] All optimizer structural tests passed.")
    print(f"[SELF TEST] Latest backtester: {BACKTEST_SCRIPT_PATH}")
    print(f"[SELF TEST] Base config: {BASE_STRATEGY_CONFIG}")


# =============================================================================
# 13. ENTRYPOINT
# =============================================================================
def main() -> None:
    if RUN_MODE == "self_test":
        run_self_tests()
        return

    groups, min_expiry_map = prepare_optimizer_data()

    if RUN_MODE == "verify_baseline":
        all_df, skips = simulate_groups(BASE_PARAMS, groups)
        actual_df = strategy.build_actual_trades_df(all_df, min_expiry_map)
        metrics = performance_metrics(actual_df)
        print("=" * 100)
        print("BASELINE VERIFICATION")
        print(f"Net P&L       : {_inr(metrics['total_pnl'])}")
        print(f"Max drawdown  : {_inr(metrics['max_drawdown'])}")
        print(f"Worst day     : {_inr(metrics['worst_day'])}")
        print(f"Trades        : {metrics['n_trades']}")
        print(f"Exit counts   : {metrics['exit_counts']}")
        return

    if RUN_MODE == "optimize":
        optimize(groups, min_expiry_map)
        return

    raise SystemExit(
        f"Unknown RUN_MODE={RUN_MODE!r}; use optimize, verify_baseline, or self_test"
    )


if __name__ == "__main__":
    main()
