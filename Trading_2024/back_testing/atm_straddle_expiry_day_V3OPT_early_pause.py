# =============================================================================
# OPTIONAL EARLY-LOSS PAUSE COMBINATIONS (research variant):
#   EARLY_PAUSE_ENABLED=1 enables a pause after a closed net losing trade
#   reaches a configured count/streak/net-loss threshold before BEFORE_IST.
#   MODE=fixed waits until RESUME_IST. MODE=cooldown waits COOLDOWN_MINUTES
#   from the exit, for at most MAX_PAUSES (1 or 2) per expiry day. Repeated
#   cooldowns count a fresh losing streak after the preceding pause.
#   Requires negative realized daily net P&L. Daily losses and total attempt
#   counts are never reset. NIFTY_ / SENSEX_ overrides are supported.
#   See the supplied trial properties and REPORT.md for fitted-history limits.
# =============================================================================
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
# -----------------------------------------------------------------------------
# SPEED REVISION -- results unchanged: every output sheet is identical, cell by
# cell, to the previous V3-OPT script (verified on pandas 2.2 and 3.0, with the
# momentum gate, late target, late giveback and per-index profiles exercised).
# Every changed spot is tagged "[SPEED Sx]" -- search for it.
#
#   S1. ensure_ist()                fast path for data already in IST, and a fixed
#                                   +05:30 shift instead of pandas' slow zoneinfo
#                                   tz_localize for naive timestamps.
#   S2. _map_unique()               row-wise Python (.map / .str.upper) runs once
#                                   per DISTINCT value, not once per row.
#   S3. _load_option_pickle()       each pickle is read ONCE per run (PASS-1 and
#       scan_pickles_pass1()        PASS-2 share the read); only nearest-expiry
#                                   rows are kept for PASS-2.
#   S4. process_pickles_generate_trades() / _prep_asof()
#                                   spot candles split by day once; the as-of
#                                   spot lookup is sorted once per day.
#   S5. _DayLegs                    per day, each (strike, CE/PE) leg series is
#                                   built once and reused by every re-entry and
#                                   by the momentum gate.
#   S6. simulate_day_multi_trades() per-attempt P&L / stop / target / protect
#                                   math on numpy arrays (same arithmetic, same
#                                   tie-break order) instead of ~30 pandas ops.
#   S7. download_underlyings()      Kite spot candles cached on disk and reused
#                                   while the cached range covers the window.
#   S8. persistent pickle cache     prepared rows of every pickle are cached on
#                                   a local disk, keyed by (path, size, modified
#                                   time). Unchanged folder -> no pickle is
#                                   parsed at all; a new or edited pickle is
#                                   re-read on its own; deleted ones are ignored.
#
# Optional keys (property file or environment) -- defaults are fine:
#   PICKLE_CACHE_DIR=            blank = %LOCALAPPDATA%\straddle_backtest_cache
#                                (Windows); OFF disables the pickle cache.
#   UNDERLYING_CACHE_FILE=       blank = <PICKLE_CACHE_DIR>\kite_spot_cache.pkl;
#                                OFF disables the spot cache.
#   KEEP_PICKLES_IN_MEMORY=1     0 = lower RAM (rows reloaded in PASS-2).
#   SIMULATE_ONLY_ACTUAL_DAYS=0  1 = simulate only days that can reach
#                                actual_trades (actual_trades / monthwise_summary
#                                identical; the all-trades sheets then cover
#                                only those days). Big win for optimizer runs.
# To force a full rebuild just delete the cache folder.
# =============================================================================
import os
from pathlib import Path
import glob
import time
from dataclasses import dataclass
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
# changed WITHOUT editing this script. Path defaults to
# "straddle_config.properties" next to this file; override with the
# STRADDLE_CONFIG environment variable. Values are pushed into the process
# environment so all the os.getenv(...) reads below pick them up. A real
# environment variable that is already set takes precedence over the file.
def _load_property_file() -> str:
    cfg_path = os.getenv(
        "STRADDLE_CONFIG",
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "configs/straddle_config_v3opt_DTE0_early_pause.properties"),
        # os.path.join(os.path.dirname(os.path.abspath(__file__)),
        #              "configs/straddle_config_v3opt_DTE0_fastreentry_BEST_50.properties"),
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

# --- [SPEED S7/S8] options (none of these change any computed number) -------
# PICKLE_CACHE_DIR  : persistent cache of the prepared pickle data. Each pickle
#                     is cached separately and keyed by (path, size, modified
#                     time), so an unchanged folder is never parsed again, a
#                     new/edited pickle is re-read on its own, and a deleted one
#                     is simply ignored. Blank = default local folder
#                     (%LOCALAPPDATA%\straddle_backtest_cache on Windows,
#                     ~/.cache/straddle_backtest_cache elsewhere). OFF disables.
#                     Keep it on a LOCAL disk (not Google Drive) -- that is the
#                     point of it.
# KEEP_PICKLES_IN_MEMORY=1 (default): rows read fresh in PASS-1 stay in RAM for
#                     PASS-2. 0 = reload them in PASS-2 (from the cache, or
#                     from the pickle when the cache is off): lower RAM.
# UNDERLYING_CACHE_FILE: Kite spot candles are reused when the cached range
#                     covers the backtest window (only completed days are
#                     saved). Blank = <PICKLE_CACHE_DIR>/kite_spot_cache.pkl,
#                     OFF disables.
# SIMULATE_ONLY_ACTUAL_DAYS=1: simulate only the (underlying, day) pairs that
#                     can reach actual_trades (ALLOWED_DTE + chosen index).
#                     actual_trades / monthwise_summary stay identical; the
#                     all-trades sheets then cover only those days. Default 0.
_OFF_WORDS = ("0", "OFF", "NONE", "FALSE", "NO", "DISABLE", "DISABLED")


def _default_cache_dir() -> str:
    base = os.getenv("LOCALAPPDATA") or os.path.join(str(Path.home()), ".cache")
    return os.path.join(base, "straddle_backtest_cache")


_pc_raw = os.getenv("PICKLE_CACHE_DIR", "").strip()
PICKLE_CACHE_DIR = "" if _pc_raw.upper() in _OFF_WORDS else (_pc_raw or _default_cache_dir())
KEEP_PICKLES_IN_MEMORY = os.getenv("KEEP_PICKLES_IN_MEMORY", "1").strip() == "1"
_uc_raw = os.getenv("UNDERLYING_CACHE_FILE", "").strip()
if _uc_raw.upper() in _OFF_WORDS:
    UNDERLYING_CACHE_FILE = ""
elif _uc_raw:
    UNDERLYING_CACHE_FILE = _uc_raw
else:
    UNDERLYING_CACHE_FILE = os.path.join(PICKLE_CACHE_DIR, "kite_spot_cache.pkl") if PICKLE_CACHE_DIR else ""
SIMULATE_ONLY_ACTUAL_DAYS = os.getenv("SIMULATE_ONLY_ACTUAL_DAYS", "0").strip() == "1"


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
# Keep an experimental run from overwriting the baseline workbook by default.
if "OUTPUT_XLSX" not in os.environ:
    OUTPUT_XLSX = str(Path(OUTPUT_XLSX).with_stem(Path(OUTPUT_XLSX).stem + "_pause_combination"))

FAIL_ON_PICKLE_ERROR = os.getenv("FAIL_ON_PICKLE_ERROR", "0").strip() == "1"

SESSION_START_IST = dtime(9, 15)
SESSION_END_IST = dtime(15, 40)

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
    # [SPEED S1] a Series already in this exact IST zone is returned as-is
    # (tz_convert to the same zone is a no-op on the values).
    if isinstance(series_or_scalar, pd.Series) and isinstance(series_or_scalar.dtype, pd.DatetimeTZDtype):
        cur = series_or_scalar.dtype.tz
        if type(cur) is type(tz) and str(cur) == str(tz):
            return series_or_scalar
    dt = pd.to_datetime(series_or_scalar, errors="coerce")
    if isinstance(dt, pd.Series):
        if dt.dt.tz is None:
            # [SPEED S1] Asia/Kolkata is a fixed +05:30 (no DST) since 1945, so
            # localizing == shifting to UTC; pandas' generic zoneinfo localize
            # is ~3 us/row. Same values and dtype; older data uses the slow path.
            if str(tz) == "Asia/Kolkata" and dt.notna().any() and dt.min() >= pd.Timestamp("1946-01-01"):
                return (dt - pd.Timedelta(hours=5, minutes=30)).dt.tz_localize("UTC").dt.tz_convert(tz)
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

# --- [SPEED] helpers: same results as the per-call versions, computed once ------
def _prep_asof(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """asof_close() preparation, done once per day instead of once per attempt."""
    if df.empty:
        return None
    d = df[["date", "close"]].dropna().copy()
    d["date"] = ensure_ist(d["date"])
    return d.sort_values("date").set_index("date")

def _asof_lookup(d: Optional[pd.DataFrame], ts: pd.Timestamp) -> float:
    if d is None:
        return float("nan")
    loc = d.index.get_indexer([ts], method="pad")
    if loc[0] == -1:
        return float("nan")
    return float(d.iloc[loc[0]]["close"])

def _map_unique(s: pd.Series, func) -> pd.Series:
    """[SPEED S2] s.map(func) evaluated once per DISTINCT value (row-wise Python is the
    slowest part of the pickle prep on millions of rows)."""
    codes, uniq = pd.factorize(s)
    out = np.array([func(v) for v in uniq] + [func(np.nan)], dtype=object)
    return pd.Series(out[codes], index=s.index)

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
_PASS2_COLS = ["date", "name", "type", "option_type", "strike", "expiry", "instrument", "high", "low", "close"]

# [SPEED S3] Where PASS-2 gets each file's prepared rows (filled by PASS-1):
#   path -> ("mem", DataFrame) | ("disk", rows_cache_path) | ("err", message)
#           | ("none", None)    (file had no usable rows)
# A path missing from this dict (PASS-1 failed on it) is read from the pickle
# again in PASS-2, exactly like the original.
_PASS2_SOURCE: Dict[str, Tuple[str, Any]] = {}

# Bump this whenever _load_option_pickle() changes what it produces, so old
# cache entries are ignored. pandas/numpy versions are part of the key because
# pickled DataFrames are not guaranteed to load across versions.
_CACHE_FORMAT = "v3opt-1"
_CACHE_TAG = f"{_CACHE_FORMAT}|pandas {pd.__version__}|numpy {np.__version__}"


def _load_option_pickle(p: str):
    """[SPEED S3] Read one pickle ONCE and return (stats, pass2_frame, pass2_error).

    stats: what PASS-1 needs from this file -- the same numbers the original
        PASS-1 computed (None if the file has no usable option rows):
        {"min_day", "max_day", "option_days", "min_exp": [((und, day), expiry), ...]}
    pass2_frame: rows exactly as the original PASS-2 kept them (before the
        window filter), further restricted to this file's nearest expiry per
        (underlying, day). The global nearest expiry is <= this file's, so a
        dropped row could never have matched min_expiry_map in PASS-2.
    pass2_error: message of the error the original PASS-2 would have raised
        for this file (e.g. missing columns), else None.

    Nothing here depends on the .properties settings (the backtest window is
    applied later), so the result can be cached across runs and configs.
    """
    df = pd.read_pickle(p)
    if not isinstance(df, pd.DataFrame) or df.empty:
        return None, None, None

    for c in ("date", "name", "expiry", "type"):
        if c not in df.columns:
            raise ValueError(f"Missing column '{c}' in {p}")

    # Option rows only (== df["type"].astype(str).str.upper().eq("OPTION")).
    is_opt = _map_unique(df["type"], lambda v: str(v).upper() == "OPTION").to_numpy(dtype=bool)
    # PASS-2 needs more columns than PASS-1. If any are missing, PASS-1 still
    # uses the file and PASS-2 later reports the same "Missing columns" warning
    # the original printed.
    missing = [c for c in _PASS2_COLS if c not in df.columns]
    miss_err = f"Missing columns {missing} in {p}" if missing else None
    d2 = df.loc[is_opt, (["date", "name", "expiry"] if missing else _PASS2_COLS)].copy()
    del df
    if d2.empty:
        return None, None, miss_err

    d2["date"] = ensure_ist(d2["date"])
    # == .dt.date. Asia/Kolkata has been a fixed +05:30 since 1945, so the IST
    # wall-clock day is (UTC + 5:30) floored; this avoids pandas' slow
    # per-element tz_localize(None) on zoneinfo zones.
    d2["day"] = (d2["date"].dt.tz_convert("UTC").dt.tz_localize(None)
                 + pd.Timedelta(hours=5, minutes=30)).dt.normalize()
    d2["underlying"] = _map_unique(d2["name"], lambda v: normalize_underlying(str(v)))
    # == pd.to_datetime(expiry).dt.date, evaluated once per distinct expiry
    exp = pd.to_datetime(d2["expiry"], errors="coerce")
    codes, uniq = pd.factorize(exp)
    uniq = pd.DatetimeIndex(uniq)
    if uniq.tz is not None:
        uniq = uniq.tz_localize(None)
    uarr = uniq.normalize().to_numpy()
    # Slot for code -1 (unparseable expiry). The NaT must carry the array's own
    # unit: a unit-less np.datetime64("NaT") raises a DeprecationWarning on
    # newer NumPy and will become an error.
    nat = np.array([np.datetime64("NaT", np.datetime_data(uarr.dtype)[0])], dtype=uarr.dtype)
    d2["expiry_date"] = pd.Series(np.concatenate([uarr, nat])[codes], index=d2.index)
    d2 = d2.dropna(subset=["underlying", "day", "expiry_date"])
    d2 = d2[d2["underlying"].isin(TRADEABLE)]
    d2 = d2[d2["expiry_date"] >= d2["day"]]
    if d2.empty:
        return None, None, miss_err

    # Everything above is the original PASS-1 filter chain (same rows).
    # PASS-1 statistics; keys go back to datetime.date so min_expiry_map is
    # exactly what the original produced.
    grp = d2.groupby(["underlying", "day"], sort=False)["expiry_date"].min()
    stats = {
        "min_day": d2["day"].min().date(),
        "max_day": d2["day"].max().date(),
        "option_days": int(d2["day"].nunique()),
        "min_exp": [((und, dy.date()), ex.date()) for (und, dy), ex in grp.items()],
    }

    if missing:
        return stats, None, miss_err

    # PASS-2 preparation (same steps as the original PASS-2, minus the window
    # filter which needs the global date range and is applied later).
    try:
        # Keep only this file's nearest expiry per (underlying, day).
        loc_min = d2.groupby(["underlying", "day"], sort=False)["expiry_date"].transform("min")
        p2 = d2[d2["expiry_date"] == loc_min].copy()
        p2["strike_num"] = pd.to_numeric(p2["strike"], errors="coerce")
        p2["strike_int"] = p2["strike_num"].round().astype("Int64")
        p2["option_type"] = _map_unique(p2["option_type"], lambda v: str(v).upper())
        p2 = p2.dropna(subset=["strike_int", "close"])
        p2["strike_int"] = p2["strike_int"].astype(int)
        # astype(str) once here; the original repeated it on every leg lookup.
        p2["instrument"] = p2["instrument"].astype(str)
        p2 = p2[["date", "underlying", "day", "expiry_date", "strike_int",
                 "option_type", "instrument", "high", "low", "close"]]
        return stats, p2, None
    except Exception as e:  # reported in PASS-2, like the original
        return stats, None, str(e)


# ----------------------------------------------------------------------------
# [SPEED S8] Persistent per-pickle cache
# ----------------------------------------------------------------------------
def _file_signature(p: str) -> Tuple[str, int, int]:
    """(absolute path, size in bytes, modified time in ns). Any edit, re-download
    or replacement of the pickle changes size and/or mtime -> cache miss."""
    st = os.stat(p)
    return (os.path.abspath(p), int(st.st_size), int(st.st_mtime_ns))


def _cache_paths(sig: Tuple[str, int, int]) -> Tuple[str, str]:
    import hashlib
    key = hashlib.sha1(sig[0].encode("utf-8", "surrogatepass")).hexdigest()[:24]
    base = os.path.join(PICKLE_CACHE_DIR, key)
    return base + ".meta.pkl", base + ".rows.pkl"


def _atomic_to_pickle(obj: Any, path: str) -> None:
    """Write to a temp file then rename, so an interrupted run never leaves a
    half-written cache file behind."""
    tmp = f"{path}.{os.getpid()}.tmp"
    pd.to_pickle(obj, tmp)
    os.replace(tmp, path)


def _cache_lookup(sig: Tuple[str, int, int]) -> Optional[Dict[str, Any]]:
    """Return the cached meta dict if it matches this exact file version."""
    if not PICKLE_CACHE_DIR:
        return None
    meta_path, rows_path = _cache_paths(sig)
    try:
        meta = pd.read_pickle(meta_path)
    except Exception:
        return None
    if not isinstance(meta, dict) or meta.get("tag") != _CACHE_TAG or tuple(meta.get("sig", ())) != tuple(sig):
        return None
    if meta.get("has_rows") and not os.path.exists(rows_path):
        return None
    meta["rows_path"] = rows_path
    return meta


def _cache_store(sig: Tuple[str, int, int], stats, p2, p2_err) -> None:
    if not PICKLE_CACHE_DIR:
        return
    try:
        os.makedirs(PICKLE_CACHE_DIR, exist_ok=True)
        meta_path, rows_path = _cache_paths(sig)
        if p2 is not None:
            _atomic_to_pickle(p2, rows_path)          # rows first ...
        meta = {"tag": _CACHE_TAG, "sig": sig, "stats": stats,
                "p2_err": p2_err, "has_rows": p2 is not None}
        _atomic_to_pickle(meta, meta_path)            # ... meta last = entry valid
    except Exception as e:  # caching is best-effort; never fail the backtest
        print(f"[CACHE WARN] could not write cache for {os.path.basename(sig[0])}: {e}")


def scan_pickles_pass1(pickle_paths: List[str]) -> Tuple[date, Dict[Tuple[str, date], date], date]:
    """PASS-1: earliest/latest option day and nearest expiry per (underlying, day).

    [SPEED S3/S8] Each pickle is read at most once per run, and not at all when
    the persistent cache holds this exact file version. It also records in
    _PASS2_SOURCE where PASS-2 will find the prepared rows. Signature and
    return value are unchanged.
    """
    max_day_seen: Optional[date] = None
    min_day_seen: Optional[date] = None
    min_expiry_map: Dict[Tuple[str, date], date] = {}
    _PASS2_SOURCE.clear()
    n_hit = n_read = 0
    if PICKLE_CACHE_DIR:
        print(f"[CACHE] pickle cache: {PICKLE_CACHE_DIR}")

    for p in pickle_paths:
        try:
            sig = _file_signature(p)
            meta = _cache_lookup(sig)
            if meta is not None:
                n_hit += 1
                stats, p2_err = meta["stats"], meta["p2_err"]
                if p2_err is not None:
                    _PASS2_SOURCE[p] = ("err", p2_err)
                elif meta["has_rows"]:
                    _PASS2_SOURCE[p] = ("disk", meta["rows_path"])
                else:
                    _PASS2_SOURCE[p] = ("none", None)
                tag = " (cache)"
            else:
                n_read += 1
                stats, p2, p2_err = _load_option_pickle(p)
                _cache_store(sig, stats, p2, p2_err)
                if p2_err is not None:
                    _PASS2_SOURCE[p] = ("err", p2_err)
                elif p2 is None:
                    _PASS2_SOURCE[p] = ("none", None)
                elif KEEP_PICKLES_IN_MEMORY:
                    _PASS2_SOURCE[p] = ("mem", p2)          # no second read at all
                elif PICKLE_CACHE_DIR:
                    _PASS2_SOURCE[p] = ("disk", _cache_paths(sig)[1])
                else:
                    _PASS2_SOURCE[p] = ("reread", None)     # low RAM, no cache
                del p2
                tag = ""
            if stats is None:
                continue

            file_min_day, file_max_day = stats["min_day"], stats["max_day"]
            max_day_seen = file_max_day if (max_day_seen is None or file_max_day > max_day_seen) else max_day_seen
            min_day_seen = file_min_day if (min_day_seen is None or file_min_day < min_day_seen) else min_day_seen

            for key, ex in stats["min_exp"]:
                if key not in min_expiry_map or ex < min_expiry_map[key]:
                    min_expiry_map[key] = ex

            print(f"[PASS1 OK] {os.path.basename(p)} option_days={stats['option_days']}{tag}")

        except Exception as e:
            _PASS2_SOURCE.pop(p, None)   # PASS-2 retries from the pickle, as before
            msg = f"[PASS1 WARN] {os.path.basename(p)} failed: {e}"
            if FAIL_ON_PICKLE_ERROR:
                raise RuntimeError(msg) from e
            print(msg)

    print(f"[PASS1] pickles parsed: {n_read} | served from cache: {n_hit}")

    if max_day_seen is None or min_day_seen is None:
        raise RuntimeError("No usable option data found in pickles (PASS1) for tradeable underlyings.")

    return max_day_seen, min_expiry_map, min_day_seen


# =============================================================================
# Underlying download
# =============================================================================
def download_underlyings(kite, day_start: date, day_end: date) -> Dict[str, pd.DataFrame]:
    # [SPEED S7] Optional disk cache of the (immutable) historical spot candles.
    if UNDERLYING_CACHE_FILE and os.path.exists(UNDERLYING_CACHE_FILE):
        try:
            obj = pd.read_pickle(UNDERLYING_CACHE_FILE)
            if (obj["from"] <= day_start and obj["to"] >= day_end
                    and set(UNDERLYING_KITE) <= set(obj["data"])):
                print(f"[UNDERLYING CACHE] using {UNDERLYING_CACHE_FILE} ({obj['from']} -> {obj['to']})")
                return {u: obj["data"][u] for u in UNDERLYING_KITE}
            print("[UNDERLYING CACHE] does not cover the window; downloading")
        except Exception as e:
            print(f"[UNDERLYING CACHE] unreadable ({e}); downloading")
    out = _download_underlyings_kite(kite, day_start, day_end)
    if UNDERLYING_CACHE_FILE and day_end < date.today():   # never cache a partial session
        try:
            os.makedirs(os.path.dirname(os.path.abspath(UNDERLYING_CACHE_FILE)), exist_ok=True)
            _atomic_to_pickle({"from": day_start, "to": day_end, "data": out}, UNDERLYING_CACHE_FILE)
            print(f"[UNDERLYING CACHE] saved {UNDERLYING_CACHE_FILE}")
        except Exception as e:
            print(f"[UNDERLYING CACHE] save failed: {e}")
    return out


def _download_underlyings_kite(kite, day_start: date, day_end: date) -> Dict[str, pd.DataFrame]:
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
# NOTE: _pick_symbol / _build_leg_series (and asof_close above) are no longer
# called by the simulator -- _DayLegs / _asof_lookup replace them with the same
# logic. They are kept unchanged in case another script imports them.
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


class _DayLegs:
    """[SPEED S5] Per-day cache for symbol picks and leg series.

    The original filtered the whole day's chain (and re-ran astype(str)) for
    every leg/column/attempt. Here the (strike, CE/PE) row positions are found
    once per day, and each leg series is built once and reused by re-entries at
    the same strike. The per-leg pandas steps are exactly those of
    _pick_symbol / _build_leg_series on the same rows in the same order, so
    every value is identical.
    """

    def __init__(self, day_opt: pd.DataFrame, idx_all: pd.DatetimeIndex):
        self.day_opt = day_opt
        self.idx_all = idx_all
        self.pos = day_opt.groupby(["strike_int", "option_type"], sort=False).indices
        self._rows: Dict[Tuple[int, str], Tuple[Optional[str], Optional[pd.DataFrame]]] = {}
        self._series: Dict[Tuple[int, str, str], pd.Series] = {}
        self._arrays: Dict[Tuple[int, str], Tuple[np.ndarray, ...]] = {}

    def _rows_for(self, strike: int, opt_type: str):
        key = (int(strike), opt_type)
        if key not in self._rows:
            p = self.pos.get(key)
            if p is None or len(p) == 0:
                self._rows[key] = (None, None)
            else:
                sub = self.day_opt.iloc[p]
                instr = sub["instrument"].astype(str)
                syms = sorted(instr.unique().tolist())
                sym = syms[0] if syms else None
                self._rows[key] = (sym, sub[instr == sym] if sym else None)
        return self._rows[key]

    def symbol(self, strike: int, opt_type: str) -> Optional[str]:
        return self._rows_for(strike, opt_type)[0]

    def series(self, strike: int, opt_type: str, price_col: str) -> pd.Series:
        """Un-forward-filled series (do_ffill=False); callers must not mutate it."""
        key = (int(strike), opt_type, price_col)
        if key not in self._series:
            _, rows = self._rows_for(strike, opt_type)
            if rows is None:
                s = pd.Series(index=self.idx_all, dtype="float64")
            else:
                sub = rows[["date", price_col]].dropna()
                if sub.empty:
                    s = pd.Series(index=self.idx_all, dtype="float64")
                else:
                    sub = sub.copy()
                    sub["date"] = ensure_ist(sub["date"])
                    sub = sub.sort_values("date").drop_duplicates(subset=["date"], keep="last").set_index("date")
                    s = sub[price_col].astype(float).reindex(self.idx_all)
            self._series[key] = s
        return self._series[key]

    def arrays(self, strike: int, opt_type: str):
        """(close_raw, close_ffilled, high, low) as float64 numpy arrays on idx_all."""
        key = (int(strike), opt_type)
        if key not in self._arrays:
            close_raw = self.series(strike, opt_type, "close")
            self._arrays[key] = (
                close_raw.to_numpy(dtype="float64"),
                close_raw.ffill().to_numpy(dtype="float64"),
                self.series(strike, opt_type, "high").to_numpy(dtype="float64"),
                self.series(strike, opt_type, "low").to_numpy(dtype="float64"),
            )
        return self._arrays[key]

@dataclass(frozen=True)
class EarlyPausePolicy:
    """Fixed-clock or elapsed-time pauses after closed NET losses on a red day.

    Prefix any EARLY_PAUSE_* key with NIFTY_ or SENSEX_ to override it.
    Enabled nonzero thresholds are OR conditions; zero disables a threshold.
    The daily circuit breaker and total reattempt budget always take priority.
    """
    enabled: bool = False
    before: dtime = dtime(12, 0)
    resume: dtime = dtime(13, 0)
    losses: int = 2
    consecutive_losses: int = 0
    net_loss_rupees: float = 0.0
    mode: str = "fixed"
    cooldown_minutes: int = 30
    max_pauses: int = 1

    @classmethod
    def from_env(cls, und: str):
        def value(key, default):
            return _und_env(und, "EARLY_PAUSE_" + key, os.getenv("EARLY_PAUSE_" + key, str(default)))
        enabled = str(value("ENABLED", "0")).strip()
        if enabled not in ("0", "1"):
            raise ValueError("EARLY_PAUSE_ENABLED must be 0 or 1")
        if enabled == "0":
            return cls()
        policy = cls(True, parse_hhmm(str(value("BEFORE_IST", "12:00"))),
                     parse_hhmm(str(value("RESUME_IST", "13:00"))),
                     int(value("LOSSES", 2)), int(value("CONSECUTIVE_LOSSES", 0)),
                     float(value("NET_LOSS_RUPEES", 0)),str(value("MODE", "fixed")).lower(),
                     int(value("COOLDOWN_MINUTES", 30)),int(value("MAX_PAUSES", 1)))
        if policy.mode not in ("fixed", "cooldown"):
            raise ValueError("EARLY_PAUSE_MODE must be fixed or cooldown")
        if not SESSION_START_IST < policy.before < SESSION_END_IST:
            raise ValueError("Early pause trigger cutoff must lie inside the session")
        if policy.mode == "fixed" and not policy.before <= policy.resume < SESSION_END_IST:
            raise ValueError("Fixed pause requires BEFORE <= RESUME < session end")
        if policy.max_pauses not in (1, 2):
            raise ValueError("EARLY_PAUSE_MAX_PAUSES must be 1 or 2")
        if policy.mode == "fixed" and policy.max_pauses != 1:
            raise ValueError("Fixed-clock mode supports one pause per day")
        if not 1 <= policy.cooldown_minutes <= 180:
            raise ValueError("EARLY_PAUSE_COOLDOWN_MINUTES must be between 1 and 180")
        if min(policy.losses, policy.consecutive_losses, policy.net_loss_rupees) < 0 or not np.isfinite(policy.net_loss_rupees):
            raise ValueError("Early pause thresholds must be finite and nonnegative")
        if not (policy.losses or policy.consecutive_losses or policy.net_loss_rupees):
            raise ValueError("Enabled early pause needs at least one nonzero loss threshold")
        return policy


@dataclass
class EarlyPauseState:
    policy: EarlyPausePolicy
    expiry_day: bool
    losses: int = 0
    streak: int = 0
    resume_at: Optional[pd.Timestamp] = None
    pauses: int = 0

    def observe_exit(self, exit_ts: pd.Timestamp, net_pnl: float, daily_net: float) -> bool:
        """Use only observed exits. Counters restart after a completed cooldown."""
        p = self.policy
        if not p.enabled or not self.expiry_day or self.pauses >= p.max_pauses:
            return False
        if self.resume_at is not None:
            if exit_ts < self.resume_at:
                return False
            # Count a fresh set of losses after the prior pause; daily net and
            # trade sequence continue unchanged in the parent simulator.
            self.losses = 0
            self.streak = 0
            self.resume_at = None
        if exit_ts.time() >= p.before:
            return False
        if net_pnl < 0:
            self.losses += 1
            self.streak += 1
        else:
            self.streak = 0
        if net_pnl >= 0 or daily_net >= 0:
            return False
        hit = ((p.losses > 0 and self.losses >= p.losses)
               or (p.consecutive_losses > 0 and self.streak >= p.consecutive_losses)
               or (p.net_loss_rupees > 0 and daily_net <= -p.net_loss_rupees))
        if hit:
            self.pauses += 1
            if p.mode == "cooldown":
                self.resume_at = exit_ts + pd.Timedelta(minutes=p.cooldown_minutes)
            else:
                self.resume_at = exit_ts.normalize() + pd.Timedelta(hours=p.resume.hour, minutes=p.resume.minute)
        return bool(hit)

    def next_entry(self, requested: pd.Timestamp) -> pd.Timestamp:
        return max(requested, self.resume_at) if self.resume_at is not None else requested


def simulate_day_multi_trades(
    *,
    und: str,
    dy: date,
    expiry: date,
    day_opt: pd.DataFrame,
    underlying_day: pd.DataFrame,
) -> Tuple[List[TradeRow], List[Dict[str, Any]]]:

    results: List[TradeRow] = []
    skipped: List[Dict[str, Any]] = []

    idx_all = build_minute_index(dy, SESSION_START_IST, SESSION_END_IST)
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
    configured_exit_cutoff_ts = pd.Timestamp(datetime.combine(dy, EXIT_TIME), tz=ist_tz())
    trade_end_ts = min(session_end_ts, configured_exit_cutoff_ts)

    qty = int(QTY_UNITS[und])
    step = int(STRIKE_STEP[und])

    # --- V3-OPT: per-underlying profile --------------------------------------
    _prof = resolve_underlying_profile(und)
    early_pause = EarlyPauseState(EarlyPausePolicy.from_env(und), expiry_day=(dy == expiry))

    # [SPEED S5] per-day caches shared by the momentum gate and every attempt
    legs = _DayLegs(day_opt, idx_all)            # leg rows/series built once per strike
    und_asof = _prep_asof(underlying_day)        # [SPEED S4] spot sorted once per day

    # --- V2-OPT: ATM-straddle premium series cache + momentum gate ----------
    _prem_cache: Dict[int, Optional[pd.Series]] = {}

    def _atm_prem_series(strike: int) -> Optional[pd.Series]:
        if strike not in _prem_cache:
            ce_s = legs.symbol(strike, "CE")
            pe_s = legs.symbol(strike, "PE")
            if not ce_s or not pe_s:
                _prem_cache[strike] = None
            else:
                # == _build_leg_series(..., "close", do_ffill=True) for each leg
                ce = legs.series(strike, "CE", "close").ffill()
                pe = legs.series(strike, "PE", "close").ffill()
                _prem_cache[strike] = ce + pe
        return _prem_cache[strike]

    def _momentum_wait(ts: pd.Timestamp) -> pd.Timestamp:
        """Advance ts minute-by-minute until the current-ATM straddle premium
        is lower than MOMENTUM_LOOKBACK_MIN minutes earlier (i.e. premium is
        decaying), or until the strategy cutoff. Uses only information
        available at ts (close of already-completed minutes)."""
        if not ENTRY_MOMENTUM_GATE:
            return ts
        session_start_ts = idx_all[0]
        while ts < trade_end_ts:
            u = _asof_lookup(und_asof, ts)          # [SPEED S4] == asof_close(underlying_day, ts)
            if pd.isna(u):
                return ts
            atm_g = round_to_step(float(u), step)
            s = _atm_prem_series(atm_g)
            if s is None:
                return ts
            now = s.loc[ts] if ts in s.index else float("nan")
            ts_back = ts - pd.Timedelta(minutes=MOMENTUM_LOOKBACK_MIN)
            if ts_back < session_start_ts:
                ts_back = session_start_ts
            before = s.loc[ts_back] if ts_back in s.index else float("nan")
            if pd.notna(now) and pd.notna(before) and float(now) < float(before):
                return ts
            ts = ts + pd.Timedelta(minutes=1)
        return ts

    # Profit-protect is now percentage-based, so the actual rupee value is not
    # known until CE/PE entry prices are available for the current attempt.
    profit_protect_pct = float(_prof["g_pct"])  # V3-OPT per-underlying
    profit_protect_enabled = profit_protect_pct > 0.0

    cur_entry_ts = pd.Timestamp(datetime.combine(dy, ENTRY_TIME), tz=ist_tz())
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
                f"No entry: ENTRY_TIME_IST {ENTRY_TIME_IST} is at/after "
                f"configured EXIT_TIME_IST {EXIT_TIME_IST}"
            ),
        })
        return results, skipped

    # Cumulative realized NET P&L for this underlying/day. Used for the daily
    # loss circuit breaker. Charges are included through exit_pnl.
    daily_realized_pnl = 0.0
    MAX_DAILY_LOSS_RUPEES = _prof["dll"]  # V3-OPT: shadow module constant per underlying
    daily_loss_limit_enabled = MAX_DAILY_LOSS_RUPEES > 0

    while cur_entry_ts < trade_end_ts:
        # Pausing never resets realized losses, trade sequence or the daily cap.
        cur_entry_ts = early_pause.next_entry(cur_entry_ts)
        if cur_entry_ts >= trade_end_ts:
            break
        if daily_loss_limit_enabled and daily_realized_pnl <= -float(MAX_DAILY_LOSS_RUPEES):
            skipped.append({
                "day": dy,
                "underlying": und,
                "expiry": expiry,
                "trade_seq": trade_seq,
                "reason": (
                    f"Daily loss limit hit before next entry: "
                    f"realized_pnl={daily_realized_pnl:.2f}, "
                    f"limit={MAX_DAILY_LOSS_RUPEES:.2f}"
                ),
            })
            break

        u_px = _asof_lookup(und_asof, cur_entry_ts)   # [SPEED S4] == asof_close(...)
        if pd.isna(u_px):
            skipped.append({"day": dy, "underlying": und, "expiry": expiry, "trade_seq": trade_seq,
                            "reason": f"No underlying price at entry {cur_entry_ts.strftime('%H:%M')}"})
            break

        atm = round_to_step(float(u_px), step)

        ce_sym = legs.symbol(atm, "CE")          # [SPEED S5] == _pick_symbol(day_opt, atm, "CE")
        pe_sym = legs.symbol(atm, "PE")
        if not ce_sym or not pe_sym:
            skipped.append({"day": dy, "underlying": und, "expiry": expiry, "trade_seq": trade_seq,
                            "atm_strike": atm, "reason": "ATM CE/PE not available in pickle band"})
            break

        # [SPEED S6] all leg series are float64 numpy arrays aligned on idx_all
        # (same values as the original pandas Series; positions in idx_all
        # replace timestamp labels).
        #   close_raw : un-filled close, for exact entry validation
        #   close     : forward-filled close, for tracking / reporting
        #   high/low  : only for intraminute extremes (stop-loss / target)
        ce_close_raw, ce_close, ce_high, ce_low = legs.arrays(atm, "CE")
        pe_close_raw, pe_close, pe_high, pe_low = legs.arrays(atm, "PE")

        if cur_entry_ts not in idx_all:
            skipped.append({"day": dy, "underlying": und, "expiry": expiry, "trade_seq": trade_seq,
                            "reason": "Entry timestamp not in session index"})
            break

        entry_pos = idx_all.get_loc(cur_entry_ts)
        ce_entry = ce_close_raw[entry_pos]
        pe_entry = pe_close_raw[entry_pos]
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

        loss_limit_pct = loss_limit_pct_for_attempt(trade_seq - 1)
        uncapped_loss_limit_rupees = float(loss_limit_pct * entry_premium_sum)

        # Absolute cap on the percentage-based stop-loss.
        # Example: 10% of premium may be Rs. 4,500, but with a Rs. 3,000 cap
        # the effective stop used by the simulator is Rs. 3,000.
        stop_cap_rupees = float(MAX_LOSS_LIMIT_RUPEES_BY_ATTEMPT)
        if stop_cap_rupees > 0:
            loss_limit_rupees = float(min(uncapped_loss_limit_rupees, stop_cap_rupees))
        else:
            loss_limit_rupees = float(uncapped_loss_limit_rupees)

        # G is the same variable used by the existing profit-protect logic:
        #   - profit-protect arms when peak P&L >= G
        #   - profit-protect exits when current P&L <= peak - G
        G = float(profit_protect_pct * entry_premium_sum)

        # [SPEED S6] Monitoring window [monitor_start_ts, trade_end_ts] as
        # positions in idx_all (== the inclusive .loc[monitor_start_ts:trade_end_ts]).
        i0 = int(idx_all.searchsorted(monitor_start_ts, side="left"))
        i1 = int(idx_all.searchsorted(trade_end_ts, side="right"))
        win = np.arange(i0, i1)

        # Close-based PnL (same as before)
        pnl_close_all = (float(ce_entry) - ce_close) * qty + (float(pe_entry) - pe_close) * qty
        pnl_pos = win[~np.isnan(pnl_close_all[i0:i1])]   # == .dropna() on the window
        pnl = pnl_close_all[pnl_pos]                      # keep 'pnl' as close-based for profit-protect

        # STOPLOSS worst-case PnL candidates within each minute:
        #  A) CE high, PE low
        pnl_ceHigh_peLow_all = (float(ce_entry) - ce_high) * qty + (float(pe_entry) - pe_low) * qty
        #  B) CE low, PE high
        pnl_ceLow_peHigh_all = (float(ce_entry) - ce_low) * qty + (float(pe_entry) - pe_high) * qty

        # Worst-case PnL per minute among (close, A, B). np.fmin skips NaN
        # exactly like DataFrame.min(axis=1).
        pnl_sl_all = np.fmin(np.fmin(pnl_close_all, pnl_ceHigh_peLow_all), pnl_ceLow_peHigh_all)
        sl_pos = win[~np.isnan(pnl_sl_all[i0:i1])]
        pnl_sl = pnl_sl_all[sl_pos]

        if len(pnl) == 0:
            skipped.append({"day": dy, "underlying": und, "expiry": expiry, "trade_seq": trade_seq,
                            "atm_strike": atm, "reason": "PnL series empty after entry"})
            break

        eod_pos = int(pnl_pos[-1])
        eod_pnl = float(pnl[-1])

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
        stop_k = int(stop_hit.argmax()) if stop_hit.any() else None

        protect_k = None
        if profit_protect_enabled:
            peak = np.maximum.accumulate(pnl)                 # == pnl.cummax()
            armed = peak >= G
            trail = peak - G
            # V3-OPT: tighter giveback from PROFIT_PROTECT_LATE_FROM_IST onward
            if _prof["g2_pct"] > 0.0:
                _g2_ts = pd.Timestamp(datetime.combine(dy, _prof["g2_from"]), tz=ist_tz())
                _late_mask = np.asarray(idx_all[pnl_pos] >= _g2_ts)
                if _late_mask.any():
                    _trail_late = peak - float(_prof["g2_pct"]) * entry_premium_sum
                    # == trail.where(~late, trail.combine(_trail_late, max));
                    # pnl/peak never contain NaN here, so np.maximum == max().
                    trail = np.where(_late_mask, np.maximum(trail, _trail_late), trail)
            protect_hit = armed & (pnl <= trail)
            protect_k = int(protect_hit.argmax()) if protect_hit.any() else None

        # --- Per-day PROFIT TARGET: % of premium collected on this attempt ---
        target_pos = None
        target_rupees = None
        # V2-OPT: attempts entered at/after PROFIT_TARGET_LATE_FROM_IST use
        # the (smaller) late-session profit target.
        _pt_pct_eff = _prof["pt_pct"]
        if _prof["pt_pct_late"] > 0.0 and cur_entry_ts.timetz().replace(tzinfo=None) >= _prof["pt_late_from"]:
            _pt_pct_eff = _prof["pt_pct_late"]
        if _pt_pct_eff > 0.0:
            target_rupees = _pt_pct_eff * entry_premium_sum
            # best-case (favourable) intrabar profit: both legs bought back at their lows
            pnl_best_all = (float(ce_entry) - ce_low) * qty + (float(pe_entry) - pe_low) * qty
            pnl_tp_all = np.fmax(pnl_close_all, pnl_best_all)    # == DataFrame.max(axis=1)
            tp_pos = win[~np.isnan(pnl_tp_all[i0:i1])]
            tp_hit = pnl_tp_all[tp_pos] >= float(target_rupees)
            target_pos = int(tp_pos[tp_hit.argmax()]) if tp_hit.any() else None

        # Earliest triggered exit wins; on identical timestamps prefer the more
        # conservative outcome: STOPLOSS, then PROFIT_TARGET, then PROFIT_PROTECT.
        # (positions in idx_all sort exactly like their timestamps)
        exit_pos = eod_pos
        exit_reason = default_exit_reason
        _candidates = []
        if stop_k is not None:
            _candidates.append((int(sl_pos[stop_k]), 0, "STOPLOSS"))
        if target_pos is not None:
            _candidates.append((target_pos, 1, "PROFIT_TARGET"))
        if protect_k is not None:
            _candidates.append((int(pnl_pos[protect_k]), 2, "PROFIT_PROTECT"))
        if _candidates:
            _candidates.sort(key=lambda c: (c[0], c[1]))
            exit_pos, _, exit_reason = _candidates[0]
        exit_ts = idx_all[exit_pos]

        if exit_reason == "STOPLOSS":
            exit_pnl_gross = -float(loss_limit_rupees)
        elif exit_reason == "PROFIT_TARGET":
            exit_pnl_gross = float(target_rupees)
        else:  # EOD / TIME_EXIT / PROFIT_PROTECT: exit_pos is a pnl position
            exit_pnl_gross = float(pnl[int(np.searchsorted(pnl_pos, exit_pos))])

        # Peak (close-based) profit reached during this trade's life, up to its exit
        pnl_pre_exit = pnl[pnl_pos <= exit_pos]
        max_profit_before_exit = float(max(0.0, pnl_pre_exit.max())) if len(pnl_pre_exit) else 0.0

        exit_ce = float(ce_close[exit_pos]) if pd.notna(ce_close[exit_pos]) else float("nan")
        exit_pe = float(pe_close[exit_pos]) if pd.notna(pe_close[exit_pos]) else float("nan")

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
            daily_loss_limit_enabled and daily_realized_pnl <= -float(MAX_DAILY_LOSS_RUPEES)
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
                daily_loss_limit_rupees=float(MAX_DAILY_LOSS_RUPEES),
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
                    f"limit={MAX_DAILY_LOSS_RUPEES:.2f}"
                ),
            })
            break

        if exit_reason in ("STOPLOSS", "PROFIT_PROTECT") and (trade_seq - 1) < MAX_REATTEMPTS:
            if early_pause.observe_exit(pd.Timestamp(exit_ts), float(exit_pnl), daily_realized_pnl):
                skipped.append({
                    "day": dy, "underlying": und, "expiry": expiry, "trade_seq": trade_seq + 1,
                    "reason": (f"Early loss pause until {early_pause.resume_at.strftime('%H:%M')}; "
                               f"closed_net_losses={early_pause.losses}, loss_streak={early_pause.streak}, "
                               f"realized_net_pnl={daily_realized_pnl:.2f}"),
                })
            delay_min = reentry_delay_for_attempt(trade_seq - 1)  # gap before this re-entry
            trade_seq += 1
            cur_entry_ts = pd.Timestamp(exit_ts) + pd.Timedelta(minutes=delay_min)
            # V2-OPT: wait for premium decay confirmation before re-entering.
            cur_entry_ts = _momentum_wait(early_pause.next_entry(cur_entry_ts))

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
                        f"is at/after configured EXIT_TIME_IST {EXIT_TIME_IST}"
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
            and PROFIT_TARGET_REENTRY_ENABLED
            and (trade_seq - 1) < MAX_REATTEMPTS
        ):
            # Profitable exits break a consecutive-loss streak. A target can
            # still be a net loss after charges, so use net P&L consistently.
            if early_pause.observe_exit(pd.Timestamp(exit_ts), float(exit_pnl), daily_realized_pnl):
                skipped.append({
                    "day": dy, "underlying": und, "expiry": expiry, "trade_seq": trade_seq + 1,
                    "reason": (f"Early loss pause until {early_pause.resume_at.strftime('%H:%M')}; "
                               f"closed_net_losses={early_pause.losses}, loss_streak={early_pause.streak}, "
                               f"realized_net_pnl={daily_realized_pnl:.2f}"),
                })
            trade_seq += 1
            cur_entry_ts = pd.Timestamp(exit_ts) + pd.Timedelta(minutes=PROFIT_TARGET_REENTRY_DELAY)
            cur_entry_ts = _momentum_wait(early_pause.next_entry(cur_entry_ts))
            if cur_entry_ts >= trade_end_ts:
                skipped.append({
                    "day": dy,
                    "underlying": und,
                    "expiry": expiry,
                    "trade_seq": trade_seq,
                    "reason": (
                        f"No target re-entry: next entry time "
                        f"{pd.Timestamp(cur_entry_ts).strftime('%H:%M')} is at/after "
                        f"configured EXIT_TIME_IST {EXIT_TIME_IST}"
                    ),
                })
                break
            continue

        break

    return results, skipped


# =============================================================================
# PASS-2: process each pickle and simulate trades for days where this expiry is nearest
# =============================================================================
def process_pickles_generate_trades(
    pickle_paths: List[str],
    min_expiry_map: Dict[Tuple[str, date], date],
    underlying_data: Dict[str, pd.DataFrame],
    window_start: date,
    window_end: date,
) -> Tuple[pd.DataFrame, pd.DataFrame]:

    all_trades: List[Dict[str, Any]] = []
    skipped_rows: List[Dict[str, Any]] = []

    # IMPORTANT: prevent double-count if same (und,day,expiry) appears in multiple files
    processed_day_keys: set[Tuple[str, date, date]] = set()

    # [SPEED S4] split each underlying's spot candles by day once (was a
    # full-frame filter for every (underlying, day)).
    und_by_day: Dict[str, Dict[date, pd.DataFrame]] = {
        u: {k: g for k, g in udf.groupby("day", sort=False)}
        for u, udf in underlying_data.items()
    }
    actual_und = pick_actual_underlying_by_day(min_expiry_map) if SIMULATE_ONLY_ACTUAL_DAYS else None
    ws_ts, we_ts = pd.Timestamp(window_start), pd.Timestamp(window_end)

    for p in pickle_paths:
        try:
            # [SPEED S3/S8] prepared rows come from RAM, the disk cache, or (if
            # PASS-1 could not prepare them) a fresh read of the pickle.
            kind, val = _PASS2_SOURCE.pop(p, ("reread", None))
            d2 = None
            if kind == "mem":
                d2 = val
            elif kind == "disk":
                try:
                    d2 = pd.read_pickle(val)
                except Exception:
                    kind = "repair"                 # cache file vanished/corrupt
            elif kind == "err":
                raise ValueError(val)
            elif kind == "none":
                continue
            if kind in ("reread", "repair"):
                stats, d2, err = _load_option_pickle(p)
                if kind == "repair":                # rewrite the bad cache entry
                    _cache_store(_file_signature(p), stats, d2, err)
                if err is not None:
                    raise ValueError(err)
            if d2 is None or d2.empty:
                continue

            # window filter
            d2 = d2[(d2["day"] >= ws_ts) & (d2["day"] <= we_ts)]
            if d2.empty:
                continue

            # group by (und, day, expiry). Keys are midnight Timestamps; convert
            # to datetime.date so every downstream value is unchanged.
            for (und, dy_ts, ex_ts), g in d2.groupby(["underlying", "day", "expiry_date"], sort=False):
                dy = dy_ts.date()
                ex = ex_ts.date()
                key_ud = (und, dy)
                if key_ud not in min_expiry_map:
                    continue
                if min_expiry_map[key_ud] != ex:
                    continue
                # Optional: skip days that can never reach actual_trades.
                if actual_und is not None and actual_und.get(dy) != und:
                    continue

                day_key = (und, dy, ex)
                if day_key in processed_day_keys:
                    skipped_rows.append({
                        "day": dy, "underlying": und, "expiry": ex,
                        "reason": "Duplicate (underlying,day,expiry) encountered in multiple pickles; skipped to avoid double-count"
                    })
                    continue
                processed_day_keys.add(day_key)

                udays = und_by_day.get(und)
                if udays is None:
                    skipped_rows.append({"day": dy, "underlying": und, "expiry": ex, "reason": "No underlying series downloaded"})
                    continue
                uday = udays.get(dy)
                if uday is None or uday.empty:
                    skipped_rows.append({"day": dy, "underlying": und, "expiry": ex, "reason": "Underlying missing for day"})
                    continue

                trades, skips = simulate_day_multi_trades(
                    und=und,
                    dy=dy,
                    expiry=ex,
                    day_opt=g,
                    underlying_day=uday,
                )
                all_trades.extend([t.__dict__ for t in trades])
                skipped_rows.extend(skips)

            print(f"[PASS2 OK] {os.path.basename(p)} processed")

        except Exception as e:
            msg = f"[PASS2 WARN] {os.path.basename(p)} failed: {e}"
            if FAIL_ON_PICKLE_ERROR:
                raise RuntimeError(msg) from e
            print(msg)

    all_df = pd.DataFrame(all_trades)
    if not all_df.empty:
        all_df = all_df.sort_values(["day", "underlying", "trade_seq"]).reset_index(drop=True)

    skip_df = pd.DataFrame(skipped_rows)
    if not skip_df.empty:
        if "day" not in skip_df.columns:
            skip_df["day"] = pd.NaT
        if "underlying" not in skip_df.columns:
            skip_df["underlying"] = pd.NA
        skip_df = skip_df.sort_values(["day", "underlying"], na_position="last").reset_index(drop=True)

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
        paths, min_expiry_map, underlying_data, window_start, end_day
    )

    actual_trades_df = build_actual_trades_df(all_trades_df, min_expiry_map)

    write_excel(all_trades_df, actual_trades_df, skipped_df)

    if not all_trades_df.empty:
        print(all_trades_df.groupby("underlying")[["exit_pnl"]].describe())
    else:
        print("[WARN] No completed trades. Check 'skipped' sheet for reasons.")


if __name__ == "__main__":
    main()
