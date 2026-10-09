"""ATM short-straddle backtest -- v3 (= v2 + two opt-in keys, defaults reproduce v2 exactly).

SPEED REVISION (results unchanged -- every output sheet is identical to the
previous v3 script; verified cell-by-cell on pandas 2.2 and 3.0).
Every changed spot is tagged "[SPEED]" -- search for it. Summary:

  S1. ensure_ist()                 fast path for Series already in IST, and a
                                   fixed +05:30 shift instead of pandas' slow
                                   zoneinfo tz_localize for naive timestamps.
  S2. _map_unique()                row-wise Python (.map / .str.upper) now runs
                                   once per DISTINCT value, not once per row.
  S3. _load_option_pickle()        each pickle is read ONCE; PASS-1 stats and
      scan_pickles_pass1()         the PASS-2 frame come from the same read.
                                   Only nearest-expiry rows are kept for PASS-2.
  S4. process_pickles_generate_trades()
                                   uses the cached PASS-2 frames; spot candles
                                   are split by day once (dict lookup per day).
  S5. _DayLegs                     per-day cache: (strike, CE/PE) rows located
                                   once, each leg series built once and reused
                                   by re-entries at the same strike.
  S6. simulate_day_multi_trades()  per-attempt P&L / stop / target / protect
                                   math on numpy arrays (same arithmetic, same
                                   tie-break order) instead of ~30 small pandas
                                   operations per attempt.
  S7. download_underlyings()       optional on-disk cache of Kite spot candles.

Optional keys (property file or environment):
  KEEP_PICKLES_IN_MEMORY=1   (default) keep trimmed pickles in RAM between passes;
                             0 = re-read in PASS-2 (lower RAM, still vectorised).
  UNDERLYING_CACHE_FILE=...  path of a .pkl to reuse the Kite spot download.
  SIMULATE_ONLY_ACTUAL_DAYS=1  simulate only days that can reach actual_trades
                             (actual_trades / monthwise_summary identical; the
                             all-trades sheets then cover only those days).

v3 additions:
  5. PROFIT_TARGET_REENTRY_ENABLED / PROFIT_TARGET_REENTRY_DELAY
     Hitting the profit target no longer has to end the day: a fresh ATM
     straddle is opened DELAY minutes later (counts toward MAX_REATTEMPTS).
  6. PROFIT_TARGET_FILL = LOWLOW (default, v2 behaviour) | CLOSE
     CLOSE detects the target on minute closes only -- a conservative check,
     because the CE low and the PE low of a minute rarely happen together.

ATM short-straddle backtest -- v2.

Changes vs the original (all opt-in via the .properties file; leaving the new
keys unset reproduces the original P&L exactly):

  1. PROFIT_PROTECT_ARM_PCT / PROFIT_PROTECT_GIVEBACK_PCT
     The arm threshold and the trail give-back are no longer forced to be equal.

  2. BREAKEVEN_ARM_PCT / BREAKEVEN_LOCK_PCT
     Breakeven ratchet: a trade that ran into profit can no longer round-trip
     into a full stop-out. Validated by walk-forward: helped 5 of 6 time blocks,
     cut max drawdown ~30%, cut worst day from -22,945 to -16,561.

  3. REENTRY_MAX_PREMIUM_RATIO
     Stops re-entering when the ATM straddle is materially richer than the one
     just stopped out of (volatility still expanding).

  4. ALLOWED_DTE=0,1 is the recommended setting -- see FINDINGS.md. Trading the
     expiry day as well as the day before roughly doubles annual P&L WITHOUT
     increasing lots or concurrent exposure, because only one index is traded
     per day and attempts within a day are sequential.

NOTE: with the default config the Rs.3,000 stop cap binds on 100% of trades, so
LOSS_LIMIT_RUPEES_BY_ATTEMPT has no effect whatsoever. Do not spend time tuning
it unless you lower it below ~4.5% of premium.
"""

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
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "configs/straddle_config_DTE_1_v3_new_claude.properties"),
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

# --- [SPEED] options (none of these change any computed number) -------------
# KEEP_PICKLES_IN_MEMORY=1 (default): each pickle is read ONCE; the rows PASS-2
#   can use (nearest expiry only) are kept in RAM. 0 = re-read in PASS-2 (low RAM).
# UNDERLYING_CACHE_FILE=<path.pkl>: reuse the Kite spot download across runs when
#   the cached range covers the backtest window (only completed days are cached).
# SIMULATE_ONLY_ACTUAL_DAYS=1: simulate only the (underlying, day) pairs that can
#   reach actual_trades (ALLOWED_DTE + chosen index). actual_trades/monthwise are
#   identical; all_trades_backtested / pivots / instrument_summary then cover
#   those days only. Default 0 = full original output.
KEEP_PICKLES_IN_MEMORY = os.getenv("KEEP_PICKLES_IN_MEMORY", "1").strip() == "1"
UNDERLYING_CACHE_FILE = os.getenv("UNDERLYING_CACHE_FILE", "").strip()
SIMULATE_ONLY_ACTUAL_DAYS = os.getenv("SIMULATE_ONLY_ACTUAL_DAYS", "0").strip() == "1"


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

# Filled by scan_pickles_pass1 so PASS-2 does not read every pickle a second time.
# path -> ("ok", frame) | ("err", exception raised by the PASS-2 preparation)
_PASS2_CACHE: Dict[str, Tuple[str, Any]] = {}


def _load_option_pickle(p: str):
    """[SPEED S3] Read one pickle ONCE and return (pass1_frame, pass2_frame, pass2_error).

    pass1_frame: rows exactly as the original PASS-1 kept them (option rows,
        tradeable underlying, expiry >= day); "day" / "expiry_date" are
        midnight Timestamps instead of datetime.date objects (vectorised).
    pass2_frame: rows exactly as the original PASS-2 kept them (before the
        window filter), further restricted to this file's nearest expiry per
        (underlying, day). The global nearest expiry is <= this file's, so a
        dropped row could never have matched min_expiry_map in PASS-2.
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
    miss_err = ValueError(f"Missing columns {missing} in {p}") if missing else None
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
    uvals = np.concatenate([uarr, nat])
    d2["expiry_date"] = pd.Series(uvals[codes], index=d2.index)
    d2 = d2.dropna(subset=["underlying", "day", "expiry_date"])
    d2 = d2[d2["underlying"].isin(TRADEABLE)]
    d2 = d2[d2["expiry_date"] >= d2["day"]]
    if d2.empty:
        return None, None, miss_err

    # Everything above is the original PASS-1 filter chain (same rows).
    pass1 = d2[["underlying", "day", "expiry_date"]]

    if missing:
        return pass1, None, miss_err

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
        return pass1, p2, None
    except Exception as e:  # reported in PASS-2, like the original
        return pass1, None, e


def scan_pickles_pass1(pickle_paths: List[str]) -> Tuple[date, Dict[Tuple[str, date], date], date]:
    """PASS-1: earliest/latest option day and nearest expiry per (underlying, day).

    [SPEED S3] Also stores each file's PASS-2 frame in _PASS2_CACHE so PASS-2
    does not read the pickle again. Signature and return value are unchanged.
    """
    max_day_seen: Optional[date] = None
    min_day_seen: Optional[date] = None
    min_expiry_map: Dict[Tuple[str, date], date] = {}
    _PASS2_CACHE.clear()
    cached_rows = 0

    for p in pickle_paths:
        try:
            d2, p2, p2_err = _load_option_pickle(p)
            if KEEP_PICKLES_IN_MEMORY:
                if p2_err is not None:
                    _PASS2_CACHE[p] = ("err", p2_err)
                else:
                    _PASS2_CACHE[p] = ("ok", p2)
                    cached_rows += 0 if p2 is None else len(p2)
            if d2 is None:
                continue

            file_min_day = d2["day"].min().date()
            file_max_day = d2["day"].max().date()
            max_day_seen = file_max_day if (max_day_seen is None or file_max_day > max_day_seen) else max_day_seen
            min_day_seen = file_min_day if (min_day_seen is None or file_min_day < min_day_seen) else min_day_seen

            # Keys are converted back to datetime.date so min_expiry_map is
            # exactly what the original produced.
            grp = d2.groupby(["underlying", "day"], sort=False)["expiry_date"].min()
            for (und, dy), ex in grp.items():
                key = (und, dy.date())
                ex = ex.date()
                if key not in min_expiry_map or ex < min_expiry_map[key]:
                    min_expiry_map[key] = ex

            print(f"[PASS1 OK] {os.path.basename(p)} option_days={d2['day'].nunique()}")

        except Exception as e:
            msg = f"[PASS1 WARN] {os.path.basename(p)} failed: {e}"
            if FAIL_ON_PICKLE_ERROR:
                raise RuntimeError(msg) from e
            print(msg)

    if KEEP_PICKLES_IN_MEMORY:
        print(f"[PASS1] kept {cached_rows:,} nearest-expiry option rows in memory for PASS-2")

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
            pd.to_pickle({"from": day_start, "to": day_end, "data": out}, UNDERLYING_CACHE_FILE)
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

    # Profit-protect is now percentage-based, so the actual rupee value is not
    # known until CE/PE entry prices are available for the current attempt.
    profit_protect_pct = float(PROFIT_PROTECT_TRIGGER_RUPEES)
    profit_protect_enabled = float(PROFIT_PROTECT_ARM_PCT) > 0.0

    cur_entry_ts = pd.Timestamp(datetime.combine(dy, ENTRY_TIME), tz=ist_tz())
    trade_seq = 1

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
    daily_loss_limit_enabled = MAX_DAILY_LOSS_RUPEES > 0
    prev_entry_premium_per_unit = None   # v2: for REENTRY_MAX_PREMIUM_RATIO

    legs = _DayLegs(day_opt, idx_all)            # [SPEED S5] per-day leg cache
    und_asof = _prep_asof(underlying_day)        # [SPEED S4] spot sorted once per day

    while cur_entry_ts < trade_end_ts:
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

        u_px = _asof_lookup(und_asof, cur_entry_ts)
        if pd.isna(u_px):
            skipped.append({"day": dy, "underlying": und, "expiry": expiry, "trade_seq": trade_seq,
                            "reason": f"No underlying price at entry {cur_entry_ts.strftime('%H:%M')}"})
            break

        atm = round_to_step(float(u_px), step)

        ce_sym = legs.symbol(atm, "CE")
        pe_sym = legs.symbol(atm, "PE")
        if not ce_sym or not pe_sym:
            skipped.append({"day": dy, "underlying": und, "expiry": expiry, "trade_seq": trade_seq,
                            "atm_strike": atm, "reason": "ATM CE/PE not available in pickle band"})
            break

        # [SPEED S6] all leg series are float64 numpy arrays aligned on idx_all
        # (same values as the pandas Series of the original; positions replace
        # timestamp labels).
        # close_raw = un-filled close for exact entry validation; close = ffilled
        # close for tracking/reporting; high/low only for intraminute extremes.
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

        # --- v2: do not re-sell into still-expanding volatility ---
        if (trade_seq > 1 and REENTRY_MAX_PREMIUM_RATIO > 0
                and prev_entry_premium_per_unit):
            if (float(ce_entry) + float(pe_entry)) > (
                    prev_entry_premium_per_unit * REENTRY_MAX_PREMIUM_RATIO):
                skipped.append({
                    "day": dy, "underlying": und, "expiry": expiry,
                    "trade_seq": trade_seq, "atm_strike": atm,
                    "reason": (
                        f"No re-entry: ATM premium {float(ce_entry)+float(pe_entry):.2f} "
                        f"> {REENTRY_MAX_PREMIUM_RATIO:.2f}x previous "
                        f"{prev_entry_premium_per_unit:.2f}"),
                })
                break
        prev_entry_premium_per_unit = float(ce_entry) + float(pe_entry)

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

        # [SPEED S6] Monitoring window [monitor_start_ts, trade_end_ts] as positions in idx_all
        # (== the inclusive .loc[monitor_start_ts:trade_end_ts] label slice).
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

        # Worst-case PnL per minute among (close, A, B); fmin skips NaN exactly
        # like DataFrame.min(axis=1).
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
        # --- v2: breakeven ratchet ---------------------------------------
        # The stop floor starts at -loss_limit_rupees. Once the running peak of
        # the close-based P&L reaches BREAKEVEN_ARM_PCT of premium, the floor is
        # lifted to BREAKEVEN_LOCK_PCT of premium for the rest of the trade.
        stop_floor = np.full(len(pnl_sl), -float(loss_limit_rupees))
        if BREAKEVEN_ARM_PCT > 0:
            # == pnl.cummax().reindex(pnl_sl.index).ffill()
            peak_full = np.full(len(idx_all), np.nan)
            peak_full[pnl_pos] = np.maximum.accumulate(pnl)
            running_peak = pd.Series(peak_full[sl_pos]).ffill().to_numpy()
            be_armed = running_peak >= (BREAKEVEN_ARM_PCT * entry_premium_sum)   # NaN -> False
            be_floor = float(BREAKEVEN_LOCK_PCT * entry_premium_sum)
            stop_floor = np.where(be_armed, max(-float(loss_limit_rupees), be_floor), stop_floor)

        stop_hit = pnl_sl <= stop_floor
        stop_k = int(stop_hit.argmax()) if stop_hit.any() else None

        protect_k = None
        if profit_protect_enabled:
            peak = np.maximum.accumulate(pnl)
            arm_rupees = float(PROFIT_PROTECT_ARM_PCT * entry_premium_sum)
            give_rupees = float(PROFIT_PROTECT_GIVEBACK_PCT * entry_premium_sum)
            armed = peak >= arm_rupees
            trail = peak - give_rupees
            protect_hit = armed & (pnl <= trail)
            protect_k = int(protect_hit.argmax()) if protect_hit.any() else None

        # --- Per-day PROFIT TARGET: % of premium collected on this attempt ---
        # When reached, this trade exits at the target AND no further trades are
        # taken for the day (PROFIT_TARGET is excluded from the re-entry rule below).
        target_pos = None
        target_rupees = None
        if PROFIT_TARGET_PCT > 0.0:
            target_rupees = PROFIT_TARGET_PCT * entry_premium_sum
            # best-case (favourable) intrabar profit: both legs bought back at their lows
            pnl_best_all = (float(ce_entry) - ce_low) * qty + (float(pe_entry) - pe_low) * qty
            pnl_tp_all = np.fmax(pnl_close_all, pnl_best_all)   # == DataFrame.max(axis=1)
            if PROFIT_TARGET_FILL == "CLOSE":  # v3
                pnl_tp_all = pnl_close_all
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
            # v2: with the ratchet armed this may be a small PROFIT, not a loss.
            exit_pnl_gross = float(stop_floor[stop_k])
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
            delay_min = reentry_delay_for_attempt(trade_seq - 1)  # gap before this re-entry
            trade_seq += 1
            cur_entry_ts = pd.Timestamp(exit_ts) + pd.Timedelta(minutes=delay_min)

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

        # v3: optionally recycle after a PROFIT_TARGET instead of ending the day
        if (exit_reason == "PROFIT_TARGET" and PROFIT_TARGET_REENTRY_ENABLED
                and (trade_seq - 1) < MAX_REATTEMPTS):
            trade_seq += 1
            cur_entry_ts = pd.Timestamp(exit_ts) + pd.Timedelta(minutes=PROFIT_TARGET_REENTRY_DELAY)
            prev_entry_premium_per_unit = None  # premium-ratio gate compares stop re-entries only
            if cur_entry_ts >= trade_end_ts:
                skipped.append({
                    "day": dy, "underlying": und, "expiry": expiry, "trade_seq": trade_seq,
                    "reason": (f"No target re-entry: next entry time "
                               f"{pd.Timestamp(cur_entry_ts).strftime('%H:%M')} is at/after "
                               f"configured EXIT_TIME_IST {EXIT_TIME_IST}"),
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

    # [SPEED S4] split each underlying's spot candles by day once (was a full-frame
    # filter for every (underlying, day)).
    und_by_day: Dict[str, Dict[date, pd.DataFrame]] = {
        u: {k: g for k, g in udf.groupby("day", sort=False)}
        for u, udf in underlying_data.items()
    }
    actual_und = pick_actual_underlying_by_day(min_expiry_map) if SIMULATE_ONLY_ACTUAL_DAYS else None
    ws_ts, we_ts = pd.Timestamp(window_start), pd.Timestamp(window_end)

    for p in pickle_paths:
        try:
            # [SPEED S3/S4] take the frame prepared in PASS-1 (pop frees RAM as
            # we go); fall back to reading the file when it was not cached.
            cached = _PASS2_CACHE.pop(p, None)
            if cached is None:                      # not cached (low-RAM mode or PASS-1 failed)
                _, d2, err = _load_option_pickle(p)
            else:
                kind, val = cached
                d2, err = (val, None) if kind == "ok" else (None, val)
            if err is not None:
                raise err
            if d2 is None or d2.empty:
                continue

            # window filter
            d2 = d2[(d2["day"] >= ws_ts) & (d2["day"] <= we_ts)]
            if d2.empty:
                continue

            # group by (und, day, expiry)
            # Group keys are midnight Timestamps; convert to datetime.date so
            # every downstream value (TradeRow.day, DTE, map keys) is unchanged.
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
