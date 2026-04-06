"""
DHAN rolling-options OTM short straddle backtest
================================================

Purpose
-------
This script applies the SAME STRATEGY ENGINE as the user's reference file
'otm_straddle_legwise_reattempt_final_commented.py', but on Dhan rolling-option
pickles rather than Zerodha-format pickles.

Core strategy logic (kept aligned with file B)
----------------------------------------------
1) Entry at a configurable minute (ENTRY_TIME_IST).
2) ATM is computed from the underlying spot available inside the Dhan pickle.
3) OTM strikes are chosen from ATM using OTM_DISTANCE_STEPS.
4) Exact entry-minute close is required for BOTH CE and PE.
5) Each short leg is managed independently:
   - exit PE if PE premium rises by LEG_PREMIUM_RISE_EXIT_PCT from PE entry
   - exit CE if CE premium rises by LEG_PREMIUM_RISE_EXIT_PCT from CE entry
6) After one leg exits, the other leg continues until its own stop or EOD.
7) Reattempt happens only after the LATER of the two leg exits.
8) Failed entry attempts also consume an attempt slot.
9) Only 0-DTE and 1-DTE trades are considered.
10) For each day, only ONE underlying is selected for the "actual_trades" path:
    - earliest expiry wins
    - tie-break prefers NIFTY over SENSEX

Important implementation note
-----------------------------
The earlier generated adaptation had one non-trivial strategic deviation from file B:
nearest-expiry and one-underlying-per-day selection were being decided too locally,
inside each pickle or from already-generated trades. That can distort results when
multiple overlapping pickles exist. This final version fixes that by doing a GLOBAL
pass-1 scan first, exactly like the reference script's intent.
"""

import os
import glob
from dataclasses import dataclass
from datetime import datetime, date, time as dtime
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any, Set

import pandas as pd

# Optional timezone backends for broad compatibility.
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
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
PICKLES_DIR = os.getenv("DHAN_PICKLES_DIR", r"G:\My Drive\Trading\Dhan_Historical_Options_Data")

# Main strategy controls
ENTRY_TIME_IST = os.getenv("ENTRY_TIME_IST", "10:05")  # HH:MM
LOOKBACK_MONTHS = int(os.getenv("LOOKBACK_MONTHS", "36"))

# Reattempt controls
# MAX_REATTEMPTS means reattempts AFTER the first attempt.
# Example:
#   0 => only the first attempt
#   1 => first attempt + at most one reattempt
MAX_REATTEMPTS = int(os.getenv("MAX_REATTEMPTS", "4"))
REENTRY_DELAY_MINUTES = int(os.getenv("REENTRY_DELAY_MINUTES", "9"))

# OTM distance in STRIKE STEPS, not raw points.
# Example:
#   NIFTY step is 50, SENSEX step is 100.
#   OTM_DISTANCE_STEPS=2 means:
#       NIFTY  => PE strike = ATM - 100, CE strike = ATM + 100
#       SENSEX => PE strike = ATM - 200, CE strike = ATM + 200
OTM_DISTANCE_STEPS = int(os.getenv("OTM_DISTANCE_STEPS", "1"))

# Exit a short leg when its premium rises by this percentage from its entry premium.
LEG_PREMIUM_RISE_EXIT_PCT = float(os.getenv("LEG_PREMIUM_RISE_EXIT_PCT", "13"))

# Window selection:
#   data  => use the latest day actually present in the pickles
#   today => use today's date as window end
WINDOW_END_MODE = os.getenv("WINDOW_END_MODE", "data").strip().lower()

# Rolling-band hygiene:
# If enabled, reject attempts where the chosen fixed strike disappears for too long after entry.
STRICT_STRIKE_PRESENCE = os.getenv("STRICT_STRIKE_PRESENCE", "0").strip() == "1"
MAX_MISSING_STREAK_MIN = int(os.getenv("MAX_MISSING_STREAK_MIN", "10"))

# Dedup controls
DEDUP_WITHIN_PICKLE = os.getenv("DEDUP_WITHIN_PICKLE", "1").strip() not in ("0", "false", "False")
DEDUP_ACROSS_PICKLES = os.getenv("DEDUP_ACROSS_PICKLES", "1").strip() not in ("0", "false", "False")

# Operational flags
FAIL_ON_PICKLE_ERROR = os.getenv("FAIL_ON_PICKLE_ERROR", "0").strip() == "1"
INCLUDE_TRANSACTION_COSTS = os.getenv("INCLUDE_TRANSACTION_COSTS", "1").strip() not in ("0", "false", "False")

# Trading session bounds (IST)
SESSION_START_IST = dtime(9, 15)
SESSION_END_IST = dtime(15, 30)

# Tradeable underlyings and their lot sizes / strike steps
QTY_UNITS = {"NIFTY": 325, "SENSEX": 100}
TRADEABLE = set(QTY_UNITS.keys())
STRIKE_STEP = {"NIFTY": 50, "SENSEX": 100}


# =============================================================================
# TRANSACTION CHARGE CONFIG
# =============================================================================
# Kept aligned with the user's existing backtest style.
BROKERAGE_PER_ORDER = 20.0
STT_SELL_PCT = 0.001
EXCHANGE_TXN_PCT = 0.0003553
SEBI_PER_CRORE = 10.0
STAMP_BUY_PCT = 0.00003
IPFT_PER_CRORE = 0.010
GST_PCT = 0.18


# =============================================================================
# OUTPUT PATH HELPERS
# =============================================================================
def _safe_fname_part(s: str) -> str:
    """Make a string safe for use inside a filename."""
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in s)


def _get_downloads_folder() -> str:
    """Prefer Downloads if it exists, else fall back to home directory."""
    downloads = Path.home() / "Downloads"
    return str(downloads if downloads.exists() else Path.home())


_DEFAULT_OUT = os.path.join(
    _get_downloads_folder(),
    f"dhan_otm_short_straddle_legwise_{_safe_fname_part(ENTRY_TIME_IST)}"
    f"_OTM_{OTM_DISTANCE_STEPS}"
    f"_SLPCT_{_safe_fname_part(str(LEG_PREMIUM_RISE_EXIT_PCT))}"
    f"_MR_{MAX_REATTEMPTS}"
    f"_RDM_{REENTRY_DELAY_MINUTES}.xlsx",
)
OUTPUT_XLSX = os.getenv("OUTPUT_XLSX", _DEFAULT_OUT)


# =============================================================================
# TIME / TZ HELPERS
# =============================================================================
def parse_hhmm(s: str) -> dtime:
    """Parse HH:MM string to datetime.time."""
    hh, mm = s.strip().split(":")
    return dtime(int(hh), int(mm))


ENTRY_TIME = parse_hhmm(ENTRY_TIME_IST)


def ist_tz():
    """Return a timezone object for Asia/Kolkata."""
    if ZoneInfo is not None:
        return ZoneInfo("Asia/Kolkata")
    if pytz is not None:
        return pytz.timezone("Asia/Kolkata")
    return "Asia/Kolkata"


def ensure_ist(series_or_scalar):
    """
    Convert pandas datetime series/scalar to IST.
    - If naive, localize to IST.
    - If already tz-aware, convert to IST.
    """
    dt = pd.to_datetime(series_or_scalar, errors="coerce")
    tz = ist_tz()

    if isinstance(dt, pd.Series):
        if dt.dt.tz is None:
            return dt.dt.tz_localize(tz)
        return dt.dt.tz_convert(tz)

    if pd.isna(dt):
        return dt
    if getattr(dt, "tzinfo", None) is None:
        return dt.tz_localize(tz)
    return dt.tz_convert(tz)


def build_minute_index(day_d: date, start_t: dtime, end_t: dtime) -> pd.DatetimeIndex:
    """Build a 1-minute IST index for the session."""
    tz = ist_tz()
    start = pd.Timestamp(datetime.combine(day_d, start_t), tz=tz)
    end = pd.Timestamp(datetime.combine(day_d, end_t), tz=tz)
    return pd.date_range(start=start, end=end, freq="1min")


def compute_window_start(end_day: date, months: int) -> date:
    """Compute lookback start date from end_day."""
    if relativedelta is not None:
        return (pd.Timestamp(end_day) - relativedelta(months=months)).date()
    return (pd.Timestamp(end_day) - pd.Timedelta(days=30 * months)).date()


def round_to_step(x: float, step: int) -> int:
    """Round a price to the nearest strike step."""
    return int(round(x / step) * step)


def compute_otm_strikes(atm: int, step: int, distance_steps: int) -> Tuple[int, int]:
    """Return (OTM PE strike, OTM CE strike)."""
    dist = int(step * distance_steps)
    return atm - dist, atm + dist


def compute_leg_exit_trigger(entry_price: float) -> float:
    """Return stop trigger price for one short leg."""
    return float(entry_price) * (1.0 + LEG_PREMIUM_RISE_EXIT_PCT / 100.0)


def validate_user_config() -> None:
    """Fail fast on invalid runtime config."""
    if LOOKBACK_MONTHS < 0:
        raise ValueError("LOOKBACK_MONTHS must be >= 0")
    if MAX_REATTEMPTS < 0:
        raise ValueError("MAX_REATTEMPTS must be >= 0")
    if REENTRY_DELAY_MINUTES < 0:
        raise ValueError("REENTRY_DELAY_MINUTES must be >= 0")
    if OTM_DISTANCE_STEPS < 0:
        raise ValueError("OTM_DISTANCE_STEPS must be >= 0")
    if LEG_PREMIUM_RISE_EXIT_PCT <= 0:
        raise ValueError("LEG_PREMIUM_RISE_EXIT_PCT must be > 0")
    if ENTRY_TIME < SESSION_START_IST or ENTRY_TIME >= SESSION_END_IST:
        raise ValueError(
            f"ENTRY_TIME_IST must be within session "
            f"[{SESSION_START_IST.strftime('%H:%M')}, {SESSION_END_IST.strftime('%H:%M')})"
        )
    if WINDOW_END_MODE not in ("data", "today"):
        raise ValueError("WINDOW_END_MODE must be 'data' or 'today'")


# =============================================================================
# TRANSACTION COST CALCULATOR
# =============================================================================
def compute_trade_charges(
    entry_ce: float,
    entry_pe: float,
    exit_ce: float,
    exit_pe: float,
    qty: int,
) -> float:
    """
    Compute estimated charges for one full short-straddle attempt:
    SELL CE + SELL PE + BUY CE + BUY PE.
    """
    if not INCLUDE_TRANSACTION_COSTS:
        return 0.0

    entry_turnover = (entry_ce + entry_pe) * qty
    exit_turnover = (exit_ce + exit_pe) * qty
    total_turnover = entry_turnover + exit_turnover

    brokerage = BROKERAGE_PER_ORDER * 4.0
    stt = entry_turnover * STT_SELL_PCT
    txn_charges = total_turnover * EXCHANGE_TXN_PCT
    sebi = total_turnover * SEBI_PER_CRORE / 1_00_00_000
    stamp = exit_turnover * STAMP_BUY_PCT
    ipft = total_turnover * IPFT_PER_CRORE / 1_00_00_000
    gst = (brokerage + txn_charges + sebi) * GST_PCT

    return round(brokerage + stt + txn_charges + sebi + stamp + ipft + gst, 2)


# =============================================================================
# DATA STRUCTURE FOR ONE COMPLETED ATTEMPT
# =============================================================================
@dataclass
class TradeRow:
    day: date
    underlying: str
    trade_seq: int
    expiry: date
    days_to_expiry: int
    qty_units: int
    entry_time: str
    final_exit_time: str
    final_exit_reason: str
    entry_underlying: float
    atm_strike: int
    otm_distance_steps: int
    pe_strike: int
    ce_strike: int
    pe_symbol: str
    ce_symbol: str
    entry_pe: float
    entry_ce: float
    pe_exit_time: str
    ce_exit_time: str
    pe_exit_reason: str
    ce_exit_reason: str
    pe_exit_price: float
    ce_exit_price: float
    pe_exit_trigger: float
    ce_exit_trigger: float
    pe_minutes_held: int
    ce_minutes_held: int
    pe_pnl_gross: float
    ce_pnl_gross: float
    exit_pnl_gross: float
    txn_charges: float
    exit_pnl: float
    eod_pnl_if_held: float
    source_pickle: str


# =============================================================================
# DHAN PICKLE NORMALIZATION
# =============================================================================
def _pick_time_col(df: pd.DataFrame) -> str:
    """Pick the best available timestamp column from supported Dhan pickle layouts."""
    if "dt_ist" in df.columns:
        return "dt_ist"
    if "timestamp_dt" in df.columns:
        return "timestamp_dt"
    if "timestamp" in df.columns:
        return "timestamp"
    raise ValueError("No usable time column found (expected dt_ist / timestamp_dt / timestamp).")


def _normalize_underlying_from_symbol(s: Any) -> Optional[str]:
    """
    Normalize underlying symbol labels found in the Dhan pickles.
    Only NIFTY and SENSEX are kept because those are the user's configured tradeables.
    """
    if not isinstance(s, str):
        return None
    u = s.upper().strip()
    if "SENSEX" in u:
        return "SENSEX"
    if "BANKNIFTY" in u or "NIFTY BANK" in u:
        return "BANKNIFTY"
    if "NIFTY" in u:
        return "NIFTY"
    return None


def _normalize_dhan_df(df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    """
    Normalize Dhan rolling-option pickle into a stable schema.

    Output columns:
      ts, day, underlying, expiry, leg, strike_int, close_f, high_f, spot_f
    """
    needed = ["symbol", "leg", "strike", "close", "spot", "target_expiry_date"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(f"{source_name}: missing columns {missing}")

    tcol = _pick_time_col(df)
    d = df.copy()

    # Canonical minute timestamp in IST.
    if tcol in ("dt_ist", "timestamp_dt"):
        d["ts"] = ensure_ist(d[tcol])
    else:
        dt_utc = pd.to_datetime(d["timestamp"], unit="s", utc=True, errors="coerce")
        d["ts"] = dt_utc.dt.tz_convert("Asia/Kolkata")
    d["ts"] = d["ts"].dt.floor("min")

    # Canonical trading day.
    if "date_ist" in d.columns:
        d["day"] = pd.to_datetime(d["date_ist"], errors="coerce").dt.date
    else:
        d["day"] = d["ts"].dt.date

    d["underlying"] = d["symbol"].map(_normalize_underlying_from_symbol)
    d = d[d["underlying"].isin(TRADEABLE)]

    d["expiry"] = pd.to_datetime(d["target_expiry_date"], errors="coerce").dt.date
    d["strike_num"] = pd.to_numeric(d["strike"], errors="coerce")
    d["strike_int"] = d["strike_num"].round().astype("Int64")

    d["close_f"] = pd.to_numeric(d["close"], errors="coerce")
    d["high_f"] = pd.to_numeric(d["high"] if "high" in d.columns else d["close"], errors="coerce")
    d["spot_f"] = pd.to_numeric(d["spot"], errors="coerce")

    # If high is missing, fall back to close for stop detection.
    d["high_f"] = d["high_f"].fillna(d["close_f"])

    d["leg"] = d["leg"].astype(str).str.upper().str.strip()
    d = d[d["leg"].isin(["CE", "PE"])]

    d = d.dropna(subset=["ts", "day", "expiry", "strike_int", "close_f", "high_f", "spot_f"])
    d["strike_int"] = d["strike_int"].astype(int)
    d["close_f"] = d["close_f"].astype(float)
    d["high_f"] = d["high_f"].astype(float)
    d["spot_f"] = d["spot_f"].astype(float)

    # Ignore rows where expiry is already behind the trading day.
    d = d[d["expiry"] >= d["day"]]

    # Rolling-option pickles can contain duplicate rows for the same strike/minute.
    if DEDUP_WITHIN_PICKLE and not d.empty:
        d = d.sort_values("ts").drop_duplicates(
            subset=["ts", "underlying", "expiry", "leg", "strike_int"],
            keep="last",
        )

    keep = ["ts", "day", "underlying", "expiry", "leg", "strike_int", "close_f", "high_f", "spot_f"]
    return d[keep].copy()


# =============================================================================
# PASS-1 HELPERS: GLOBAL DAY / EXPIRY DISCOVERY
# =============================================================================
def discover_data_max_day(pickle_paths: List[str]) -> Optional[date]:
    """Find the maximum trading day present in the input pickles."""
    max_day: Optional[date] = None
    for p in pickle_paths:
        try:
            raw = pd.read_pickle(p)
            if not isinstance(raw, pd.DataFrame) or raw.empty:
                continue

            tcol = _pick_time_col(raw)
            if tcol in ("dt_ist", "timestamp_dt"):
                ts = ensure_ist(raw[tcol])
                ts = pd.to_datetime(ts, errors="coerce").dt.floor("min")
                day = ts.dt.date.max()
            else:
                dt_utc = pd.to_datetime(raw["timestamp"], unit="s", utc=True, errors="coerce")
                ts = dt_utc.dt.tz_convert("Asia/Kolkata").dt.floor("min")
                day = ts.dt.date.max()

            if day is not None:
                max_day = day if (max_day is None or day > max_day) else max_day
        except Exception:
            continue
    return max_day


def scan_pickles_pass1_dhan(
    pickle_paths: List[str],
    window_start: date,
    window_end: date,
) -> Tuple[Dict[Tuple[str, date], date], Optional[date], Optional[date]]:
    """
    Global pass-1 over all pickles.

    Returns:
      - min_expiry_map[(underlying, day)] = earliest expiry seen globally in the window
      - min_day_seen in filtered usable data
      - max_day_seen in filtered usable data

    This is the key correction over the earlier generated file. The nearest-expiry decision
    must be GLOBAL, not local per pickle, otherwise overlapping files can change outcomes.
    """
    min_expiry_map: Dict[Tuple[str, date], date] = {}
    min_day_seen: Optional[date] = None
    max_day_seen: Optional[date] = None

    for p in pickle_paths:
        try:
            raw = pd.read_pickle(p)
            if not isinstance(raw, pd.DataFrame) or raw.empty:
                continue

            d = _normalize_dhan_df(raw, os.path.basename(p))
            if d.empty:
                continue

            d = d[(d["day"] >= window_start) & (d["day"] <= window_end)]
            if d.empty:
                continue

            d["days_to_expiry"] = (pd.to_datetime(d["expiry"]) - pd.to_datetime(d["day"])).dt.days
            d = d[d["days_to_expiry"].isin([0, 1])]
            if d.empty:
                continue

            file_min_day = d["day"].min()
            file_max_day = d["day"].max()
            min_day_seen = file_min_day if (min_day_seen is None or file_min_day < min_day_seen) else min_day_seen
            max_day_seen = file_max_day if (max_day_seen is None or file_max_day > max_day_seen) else max_day_seen

            grp = d.groupby(["underlying", "day"], sort=False)["expiry"].min()
            for (und, dy), ex in grp.items():
                key = (und, dy)
                if key not in min_expiry_map or ex < min_expiry_map[key]:
                    min_expiry_map[key] = ex

            print(f"[PASS1 OK] {os.path.basename(p)} usable_days={d['day'].nunique()}")
        except Exception as e:
            msg = f"[PASS1 WARN] {os.path.basename(p)} failed: {e}"
            if FAIL_ON_PICKLE_ERROR:
                raise RuntimeError(msg) from e
            print(msg)

    return min_expiry_map, min_day_seen, max_day_seen


def pick_actual_underlying_by_day(min_expiry_map: Dict[Tuple[str, date], date]) -> Dict[date, str]:
    """
    Pick the ONE underlying to be traded for each day.

    Rule:
      - consider only the global nearest-expiry candidates already contained in min_expiry_map
      - earliest expiry wins
      - if tied, prefer NIFTY over SENSEX
    """
    by_day: Dict[date, List[Tuple[date, str]]] = {}

    for (und, dy), ex in min_expiry_map.items():
        if und not in TRADEABLE:
            continue
        dte = int((ex - dy).days)
        if dte not in (0, 1):
            continue
        by_day.setdefault(dy, []).append((ex, und))

    out: Dict[date, str] = {}
    for dy, lst in by_day.items():
        lst_sorted = sorted(lst, key=lambda t: (t[0], 0 if t[1] == "NIFTY" else 1))
        out[dy] = lst_sorted[0][1]

    return out


# =============================================================================
# SERIES BUILDERS / SMALL HELPERS
# =============================================================================
def _build_underlying_series_from_spot(day_opt: pd.DataFrame, idx_all: pd.DatetimeIndex) -> pd.Series:
    """Collapse spot_f to one value per minute and forward-fill."""
    sub = day_opt[["ts", "spot_f"]].dropna()
    if sub.empty:
        return pd.Series(index=idx_all, dtype="float64")
    sub = sub.sort_values("ts").groupby("ts", as_index=True)["spot_f"].last()
    return sub.reindex(idx_all).ffill()


def _build_leg_series_fixed_strike(
    day_opt: pd.DataFrame,
    idx_all: pd.DatetimeIndex,
    strike: int,
    leg: str,
    value_col: str,
    do_ffill: bool,
) -> pd.Series:
    """
    Build a minute-aligned series for a fixed strike and leg.

    - do_ffill=False is used where exact entry print matters.
    - do_ffill=True is used for post-entry MTM / EOD handling.
    """
    sub = day_opt[(day_opt["strike_int"] == strike) & (day_opt["leg"] == leg)][["ts", value_col]].dropna()
    if sub.empty:
        return pd.Series(index=idx_all, dtype="float64")
    sub = sub.sort_values("ts").groupby("ts", as_index=True)[value_col].last()
    s = sub.reindex(idx_all)
    return s.ffill() if do_ffill else s


def _missing_streak_minutes(s: pd.Series) -> int:
    """Return maximum consecutive NaN streak length."""
    is_na = s.isna().to_numpy()
    if not is_na.any():
        return 0
    best = 0
    cur = 0
    for v in is_na:
        if v:
            cur += 1
            best = max(best, cur)
        else:
            cur = 0
    return best


def _first_hit_ts(series_bool: pd.Series) -> Optional[pd.Timestamp]:
    """Return timestamp of first True, else None."""
    if series_bool.empty or not bool(series_bool.any()):
        return None
    return series_bool.index[series_bool.to_numpy().argmax()]


def _series_value(s: pd.Series, ts: pd.Timestamp) -> float:
    """Read one series value safely as float or NaN."""
    v = s.loc[ts]
    return float(v) if pd.notna(v) else float("nan")


def _classify_final_exit_reason(pe_reason: str, ce_reason: str) -> str:
    """Compact pair-level exit label."""
    if pe_reason == "EOD" and ce_reason == "EOD":
        return "BOTH_EOD"
    if pe_reason == "LEG_SL" and ce_reason == "LEG_SL":
        return "BOTH_LEG_SL"
    if pe_reason == "LEG_SL" and ce_reason == "EOD":
        return "PE_LEG_SL_CE_EOD"
    if pe_reason == "EOD" and ce_reason == "LEG_SL":
        return "CE_LEG_SL_PE_EOD"
    return f"PE_{pe_reason}_CE_{ce_reason}"


def _minutes_between(a: pd.Timestamp, b: pd.Timestamp) -> int:
    """Return integer minute difference between two timestamps."""
    return int((b - a).total_seconds() // 60)


def _next_attempt_timestamp(
    *,
    current_trade_seq: int,
    base_ts: pd.Timestamp,
    session_end_ts: pd.Timestamp,
) -> Optional[pd.Timestamp]:
    """
    Return the next allowed attempt timestamp or None if no further attempts are allowed.
    """
    max_total_attempts = 1 + MAX_REATTEMPTS
    if current_trade_seq >= max_total_attempts:
        return None

    next_ts = pd.Timestamp(base_ts) + pd.Timedelta(minutes=REENTRY_DELAY_MINUTES)
    if next_ts >= session_end_ts:
        return None
    return next_ts


def _simulate_leg_exit(
    *,
    entry_ts: pd.Timestamp,
    session_end_ts: pd.Timestamp,
    entry_price: float,
    close_series_ffill: pd.Series,
    high_series_raw: pd.Series,
    trigger_price: float,
) -> Tuple[pd.Timestamp, str, float]:
    """
    Simulate one short-leg exit.

    Logic:
      - Start monitoring from entry_ts + 1 minute.
      - If HIGH >= trigger_price, exit at trigger_price on that minute.
      - Else exit at EOD using last forward-filled close.

    This matches the reference script's continuous-monitoring approximation.
    """
    monitor_idx = close_series_ffill.loc[entry_ts + pd.Timedelta(minutes=1): session_end_ts].index

    # Rare corner case: if no post-entry minute exists, exit flat at entry.
    if len(monitor_idx) == 0:
        return entry_ts, "EOD", float(entry_price)

    high_monitor = high_series_raw.reindex(monitor_idx)
    close_monitor = close_series_ffill.reindex(monitor_idx)

    # If HIGH is missing for a minute, fall back to close.
    observed_high = high_monitor.combine_first(close_monitor)

    hit_mask = observed_high >= trigger_price
    hit_ts = _first_hit_ts(hit_mask)
    if hit_ts is not None:
        return hit_ts, "LEG_SL", float(trigger_price)

    eod_ts = monitor_idx[-1]
    return eod_ts, "EOD", _series_value(close_series_ffill, eod_ts)


# =============================================================================
# CORE STRATEGY SIMULATION FOR ONE (UNDERLYING, DAY, EXPIRY)
# =============================================================================
def simulate_day_multi_trades_dhan_legwise(
    *,
    und: str,
    dy: date,
    expiry: date,
    day_opt: pd.DataFrame,
    source_pickle: str,
) -> Tuple[List[TradeRow], List[Dict[str, Any]]]:
    """
    Simulate one day for one underlying and one expiry.

    Semantics are intentionally aligned to file B:
      - exact entry-minute close required for both legs
      - failed entry attempt still consumes an attempt slot
      - reattempt starts only after the later of CE/PE exits
      - later surviving leg continues after first leg exits
    """
    results: List[TradeRow] = []
    skipped: List[Dict[str, Any]] = []

    idx_all = build_minute_index(dy, SESSION_START_IST, SESSION_END_IST)
    session_end_ts = idx_all[-1]

    qty = int(QTY_UNITS[und])
    step = int(STRIKE_STEP[und])
    spot_s = _build_underlying_series_from_spot(day_opt, idx_all)

    trade_seq = 1
    cur_entry_ts = pd.Timestamp(datetime.combine(dy, ENTRY_TIME), tz=ist_tz())

    while trade_seq <= (1 + MAX_REATTEMPTS) and cur_entry_ts < session_end_ts:
        if cur_entry_ts not in idx_all:
            skipped.append({
                "day": dy,
                "underlying": und,
                "expiry": expiry,
                "trade_seq": trade_seq,
                "source_pickle": source_pickle,
                "entry_time": cur_entry_ts.strftime("%H:%M"),
                "reason": "Entry timestamp not in session index",
            })
            next_entry = _next_attempt_timestamp(
                current_trade_seq=trade_seq,
                base_ts=cur_entry_ts,
                session_end_ts=session_end_ts,
            )
            if next_entry is None:
                break
            trade_seq += 1
            cur_entry_ts = next_entry
            continue

        u_px = spot_s.loc[cur_entry_ts] if cur_entry_ts in spot_s.index else float("nan")
        if pd.isna(u_px):
            skipped.append({
                "day": dy,
                "underlying": und,
                "expiry": expiry,
                "trade_seq": trade_seq,
                "source_pickle": source_pickle,
                "entry_time": cur_entry_ts.strftime("%H:%M"),
                "reason": f"No underlying spot at entry {cur_entry_ts.strftime('%H:%M')}",
            })
            next_entry = _next_attempt_timestamp(
                current_trade_seq=trade_seq,
                base_ts=cur_entry_ts,
                session_end_ts=session_end_ts,
            )
            if next_entry is None:
                break
            trade_seq += 1
            cur_entry_ts = next_entry
            continue

        atm = round_to_step(float(u_px), step)
        pe_strike, ce_strike = compute_otm_strikes(atm, step, OTM_DISTANCE_STEPS)

        # Exact entry-minute close series.
        pe_close_raw = _build_leg_series_fixed_strike(day_opt, idx_all, pe_strike, "PE", "close_f", False)
        ce_close_raw = _build_leg_series_fixed_strike(day_opt, idx_all, ce_strike, "CE", "close_f", False)

        # Forward-filled close series for post-entry holding / EOD MTM.
        pe_close = pe_close_raw.ffill()
        ce_close = ce_close_raw.ffill()

        # HIGH is used for stop detection.
        pe_high = _build_leg_series_fixed_strike(day_opt, idx_all, pe_strike, "PE", "high_f", False)
        ce_high = _build_leg_series_fixed_strike(day_opt, idx_all, ce_strike, "CE", "high_f", False)

        if STRICT_STRIKE_PRESENCE:
            # Reject if the selected strike disappears too much after entry.
            pe_post = pe_close_raw.loc[cur_entry_ts:]
            ce_post = ce_close_raw.loc[cur_entry_ts:]
            max_miss = max(_missing_streak_minutes(pe_post), _missing_streak_minutes(ce_post))
            if max_miss > MAX_MISSING_STREAK_MIN:
                skipped.append({
                    "day": dy,
                    "underlying": und,
                    "expiry": expiry,
                    "trade_seq": trade_seq,
                    "entry_time": cur_entry_ts.strftime("%H:%M"),
                    "atm_strike": atm,
                    "pe_strike": pe_strike,
                    "ce_strike": ce_strike,
                    "source_pickle": source_pickle,
                    "reason": (
                        "Selected OTM strike series missing too much after entry "
                        f"(max_missing_streak={max_miss}m)"
                    ),
                })
                next_entry = _next_attempt_timestamp(
                    current_trade_seq=trade_seq,
                    base_ts=cur_entry_ts,
                    session_end_ts=session_end_ts,
                )
                if next_entry is None:
                    break
                trade_seq += 1
                cur_entry_ts = next_entry
                continue

        # Exact-entry rule: both legs must have a real close at the entry minute.
        pe_entry = pe_close_raw.loc[cur_entry_ts]
        ce_entry = ce_close_raw.loc[cur_entry_ts]
        if pd.isna(pe_entry) or pd.isna(ce_entry):
            skipped.append({
                "day": dy,
                "underlying": und,
                "expiry": expiry,
                "trade_seq": trade_seq,
                "entry_time": cur_entry_ts.strftime("%H:%M"),
                "atm_strike": atm,
                "pe_strike": pe_strike,
                "ce_strike": ce_strike,
                "source_pickle": source_pickle,
                "reason": "No exact PE/CE close available at entry timestamp",
            })
            next_entry = _next_attempt_timestamp(
                current_trade_seq=trade_seq,
                base_ts=cur_entry_ts,
                session_end_ts=session_end_ts,
            )
            if next_entry is None:
                break
            trade_seq += 1
            cur_entry_ts = next_entry
            continue

        pe_entry_f = float(pe_entry)
        ce_entry_f = float(ce_entry)
        pe_trigger = compute_leg_exit_trigger(pe_entry_f)
        ce_trigger = compute_leg_exit_trigger(ce_entry_f)

        # Leg-wise independent exits.
        pe_exit_ts, pe_exit_reason, pe_exit_price = _simulate_leg_exit(
            entry_ts=cur_entry_ts,
            session_end_ts=session_end_ts,
            entry_price=pe_entry_f,
            close_series_ffill=pe_close,
            high_series_raw=pe_high,
            trigger_price=pe_trigger,
        )
        ce_exit_ts, ce_exit_reason, ce_exit_price = _simulate_leg_exit(
            entry_ts=cur_entry_ts,
            session_end_ts=session_end_ts,
            entry_price=ce_entry_f,
            close_series_ffill=ce_close,
            high_series_raw=ce_high,
            trigger_price=ce_trigger,
        )

        # Leg-wise and combined gross PnL.
        pe_pnl_gross = (pe_entry_f - pe_exit_price) * qty
        ce_pnl_gross = (ce_entry_f - ce_exit_price) * qty
        exit_pnl_gross = pe_pnl_gross + ce_pnl_gross

        # What if the original pair had just been held till EOD?
        eod_pe = _series_value(pe_close, session_end_ts)
        eod_ce = _series_value(ce_close, session_end_ts)
        if pd.isna(eod_pe) or pd.isna(eod_ce):
            eod_pnl_if_held = float("nan")
        else:
            eod_pnl_if_held = (pe_entry_f - eod_pe) * qty + (ce_entry_f - eod_ce) * qty

        txn_charges = compute_trade_charges(
            entry_ce=ce_entry_f,
            entry_pe=pe_entry_f,
            exit_ce=ce_exit_price,
            exit_pe=pe_exit_price,
            qty=qty,
        )
        exit_pnl = exit_pnl_gross - txn_charges

        final_exit_ts = max(pe_exit_ts, ce_exit_ts)
        final_exit_reason = _classify_final_exit_reason(pe_exit_reason, ce_exit_reason)
        dte = int((expiry - dy).days)

        # Dhan rolling-option files generally do not expose a stable single tradingsymbol per fixed strike,
        # so we synthesize a readable identifier.
        pe_sym = f"{und}_{expiry.strftime('%Y%m%d')}_{pe_strike}_PE"
        ce_sym = f"{und}_{expiry.strftime('%Y%m%d')}_{ce_strike}_CE"

        results.append(
            TradeRow(
                day=dy,
                underlying=und,
                trade_seq=trade_seq,
                expiry=expiry,
                days_to_expiry=dte,
                qty_units=qty,
                entry_time=cur_entry_ts.strftime("%H:%M"),
                final_exit_time=final_exit_ts.strftime("%H:%M"),
                final_exit_reason=final_exit_reason,
                entry_underlying=float(u_px),
                atm_strike=int(atm),
                otm_distance_steps=int(OTM_DISTANCE_STEPS),
                pe_strike=int(pe_strike),
                ce_strike=int(ce_strike),
                pe_symbol=pe_sym,
                ce_symbol=ce_sym,
                entry_pe=pe_entry_f,
                entry_ce=ce_entry_f,
                pe_exit_time=pe_exit_ts.strftime("%H:%M"),
                ce_exit_time=ce_exit_ts.strftime("%H:%M"),
                pe_exit_reason=pe_exit_reason,
                ce_exit_reason=ce_exit_reason,
                pe_exit_price=float(pe_exit_price),
                ce_exit_price=float(ce_exit_price),
                pe_exit_trigger=float(pe_trigger),
                ce_exit_trigger=float(ce_trigger),
                pe_minutes_held=_minutes_between(cur_entry_ts, pe_exit_ts),
                ce_minutes_held=_minutes_between(cur_entry_ts, ce_exit_ts),
                pe_pnl_gross=float(pe_pnl_gross),
                ce_pnl_gross=float(ce_pnl_gross),
                exit_pnl_gross=float(exit_pnl_gross),
                txn_charges=float(txn_charges),
                exit_pnl=float(exit_pnl),
                eod_pnl_if_held=float(eod_pnl_if_held) if pd.notna(eod_pnl_if_held) else float("nan"),
                source_pickle=source_pickle,
            )
        )

        # No reattempt is possible if the position effectively lived to session end.
        if final_exit_ts >= session_end_ts:
            break

        next_entry = _next_attempt_timestamp(
            current_trade_seq=trade_seq,
            base_ts=final_exit_ts,
            session_end_ts=session_end_ts,
        )
        if next_entry is None:
            break

        trade_seq += 1
        cur_entry_ts = next_entry

    return results, skipped


# =============================================================================
# PASS-2: PROCESS PICKLES AND SIMULATE ONLY THE GLOBALLY CHOSEN CANDIDATES
# =============================================================================
def process_pickles_generate_trades_dhan(
    pickle_paths: List[str],
    min_expiry_map: Dict[Tuple[str, date], date],
    actual_underlying_by_day: Dict[date, str],
    window_start: date,
    window_end: date,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Pass-2 performs actual simulation, but only for the globally chosen strategy candidates:
      - nearest expiry for that (underlying, day)
      - chosen underlying for that day

    This mirrors the spirit of the reference file much more faithfully than simulating everything
    first and filtering later.
    """
    all_trades: List[Dict[str, Any]] = []
    skipped_rows: List[Dict[str, Any]] = []

    # Prevent double counting when the same (underlying, day, expiry) exists in multiple pickles.
    processed_day_keys: Set[Tuple[str, date, date]] = set()

    for p in pickle_paths:
        src = os.path.basename(p)
        try:
            raw = pd.read_pickle(p)
            if not isinstance(raw, pd.DataFrame) or raw.empty:
                continue

            d = _normalize_dhan_df(raw, src)
            if d.empty:
                continue

            d = d[(d["day"] >= window_start) & (d["day"] <= window_end)]
            if d.empty:
                continue

            d["days_to_expiry"] = (pd.to_datetime(d["expiry"]) - pd.to_datetime(d["day"])).dt.days
            d = d[d["days_to_expiry"].isin([0, 1])]
            if d.empty:
                continue

            for (und, dy, ex), g in d.groupby(["underlying", "day", "expiry"], sort=False):
                key_ud = (und, dy)
                if key_ud not in min_expiry_map:
                    continue

                # Enforce GLOBAL nearest-expiry rule.
                if min_expiry_map[key_ud] != ex:
                    continue

                # Enforce ONE underlying per day.
                if actual_underlying_by_day.get(dy) != und:
                    continue

                day_key = (und, dy, ex)
                if day_key in processed_day_keys:
                    skipped_rows.append({
                        "day": dy,
                        "underlying": und,
                        "expiry": ex,
                        "source_pickle": src,
                        "reason": "Duplicate (underlying,day,expiry) encountered in multiple pickles; skipped to avoid double-count",
                    })
                    continue
                processed_day_keys.add(day_key)

                trades, skips = simulate_day_multi_trades_dhan_legwise(
                    und=und,
                    dy=dy,
                    expiry=ex,
                    day_opt=g,
                    source_pickle=src,
                )
                all_trades.extend([t.__dict__ for t in trades])
                skipped_rows.extend(skips)

            print(f"[PASS2 OK] {src} processed")
        except Exception as e:
            msg = f"[PASS2 WARN] {src} failed: {e}"
            if FAIL_ON_PICKLE_ERROR:
                raise RuntimeError(msg) from e
            print(msg)
            skipped_rows.append({"source_pickle": src, "reason": str(e)})

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
# DEDUP ACROSS PICKLES (HYGIENE BACKSTOP)
# =============================================================================
def dedup_trades_across_pickles(trades: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Final dedup backstop. Even though pass-2 already prevents duplicate day keys,
    this stays as a second line of defense.
    """
    if trades.empty:
        return trades, pd.DataFrame()

    key_cols = ["underlying", "day", "expiry", "trade_seq", "entry_time"]
    before = len(trades)

    trades_sorted = trades.sort_values(key_cols + ["source_pickle"]).reset_index(drop=True)
    deduped = trades_sorted.drop_duplicates(subset=key_cols, keep="first").reset_index(drop=True)

    removed = before - len(deduped)
    report = pd.DataFrame()
    if removed > 0:
        report = pd.DataFrame([{
            "source_pickle": "__aggregate__",
            "reason": f"Dedup across pickles removed {removed} duplicate trade rows (key={key_cols})",
        }])

    return deduped, report


# =============================================================================
# ACTUAL TRADES
# =============================================================================
def build_actual_trades_df(
    all_trades_df: pd.DataFrame,
    actual_underlying_by_day: Dict[date, str],
) -> pd.DataFrame:
    """
    Build the actual-trades view using the globally chosen underlying per day.

    This is stricter and more correct than selecting from only the completed trade rows.
    If the chosen underlying has no completed trade on a day, that day simply remains absent
    rather than incorrectly switching to another underlying.
    """
    if all_trades_df.empty:
        return pd.DataFrame()

    out = all_trades_df.copy()
    out["actual_underlying_for_day"] = out["day"].map(actual_underlying_by_day)
    out = out[out["actual_underlying_for_day"].notna()]
    out = out[out["underlying"] == out["actual_underlying_for_day"]]
    out = out.drop(columns=["actual_underlying_for_day"])
    out = out.sort_values(["day", "trade_seq", "source_pickle"]).reset_index(drop=True)
    out["success"] = (pd.to_numeric(out["exit_pnl"], errors="coerce") > 0).astype(int)
    return out


# =============================================================================
# DAILY / MONTHLY SUMMARY
# =============================================================================
def build_daily_and_monthly_summary(actual_trades_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build daily and monthwise summaries from ACTUAL trades.

    Kept simple and useful:
      - daily net PnL across all attempts of the chosen underlying/day
      - monthly totals, averages, win/loss days
      - average loss on loss days and maximum loss in a day
    """
    if actual_trades_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    daily = (
        actual_trades_df.groupby(["day"], as_index=False)
        .agg(
            underlying=("underlying", "first"),
            expiry=("expiry", "min"),
            net_exit_pnl=("exit_pnl", "sum"),
            trades_taken=("trade_seq", "count"),
            gross_exit_pnl=("exit_pnl_gross", "sum"),
            total_txn_charges=("txn_charges", "sum"),
        )
        .sort_values("day")
        .reset_index(drop=True)
    )

    daily["month"] = pd.to_datetime(daily["day"]).dt.to_period("M").astype(str)

    monthwise = (
        daily.groupby("month", as_index=False)
        .agg(
            trading_days=("day", "count"),
            total_pnl=("net_exit_pnl", "sum"),
            avg_pnl_per_day=("net_exit_pnl", "mean"),
            win_days=("net_exit_pnl", lambda s: int((s > 0).sum())),
            loss_days=("net_exit_pnl", lambda s: int((s < 0).sum())),
            best_day_pnl=("net_exit_pnl", "max"),
            worst_day_pnl=("net_exit_pnl", "min"),
            avg_loss_on_loss_days=(
                "net_exit_pnl",
                lambda s: float(s[s < 0].mean()) if (s < 0).any() else 0.0,
            ),
            max_loss_in_a_day=("net_exit_pnl", "min"),
        )
        .sort_values("month")
        .reset_index(drop=True)
    )

    monthwise["win_rate_pct"] = (100.0 * monthwise["win_days"] / monthwise["trading_days"]).round(2)

    # Add an overall row for convenience.
    overall = pd.DataFrame([{
        "month": "__overall__",
        "trading_days": int(len(daily)),
        "total_pnl": float(daily["net_exit_pnl"].sum()),
        "avg_pnl_per_day": float(daily["net_exit_pnl"].mean()),
        "win_days": int((daily["net_exit_pnl"] > 0).sum()),
        "loss_days": int((daily["net_exit_pnl"] < 0).sum()),
        "best_day_pnl": float(daily["net_exit_pnl"].max()),
        "worst_day_pnl": float(daily["net_exit_pnl"].min()),
        "avg_loss_on_loss_days": float(daily.loc[daily["net_exit_pnl"] < 0, "net_exit_pnl"].mean()) if (daily["net_exit_pnl"] < 0).any() else 0.0,
        "max_loss_in_a_day": float(daily["net_exit_pnl"].min()),
        "win_rate_pct": round(float(100.0 * (daily["net_exit_pnl"] > 0).mean()), 2),
    }])
    monthwise = pd.concat([monthwise, overall], ignore_index=True)

    return daily, monthwise


# =============================================================================
# INSTRUMENT SUMMARY
# =============================================================================
def build_instrument_summary(all_trades_df: pd.DataFrame) -> pd.DataFrame:
    """Build simple instrument-wise summary over all simulated trades."""
    if all_trades_df.empty:
        return pd.DataFrame()

    inst = all_trades_df.copy()
    inst["any_leg_sl"] = (
        inst["pe_exit_reason"].astype(str).eq("LEG_SL")
        | inst["ce_exit_reason"].astype(str).eq("LEG_SL")
    )
    inst["both_legs_sl"] = (
        inst["pe_exit_reason"].astype(str).eq("LEG_SL")
        & inst["ce_exit_reason"].astype(str).eq("LEG_SL")
    )

    out = (
        inst.groupby("underlying", as_index=False)
        .agg(
            trades=("exit_pnl", "count"),
            total_exit_pnl=("exit_pnl", "sum"),
            avg_exit_pnl=("exit_pnl", "mean"),
            win_rate_exit_pct=("exit_pnl", lambda s: 100.0 * (s > 0).mean()),
            any_leg_sl_rate_pct=("any_leg_sl", lambda s: 100.0 * s.mean()),
            both_legs_sl_rate_pct=("both_legs_sl", lambda s: 100.0 * s.mean()),
        )
        .sort_values("total_exit_pnl", ascending=False)
        .reset_index(drop=True)
    )
    return out


# =============================================================================
# EXCEL OUTPUT
# =============================================================================
def _autosize_columns_safe(ws) -> None:
    """Best-effort autosizing. Never allow sizing failure to kill the run."""
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
        return


def write_excel(
    all_trades_df: pd.DataFrame,
    actual_trades_df: pd.DataFrame,
    daily_pnl_df: pd.DataFrame,
    monthwise_df: pd.DataFrame,
    instrument_summary_df: pd.DataFrame,
    skipped_df: pd.DataFrame,
) -> None:
    """Write final workbook."""
    out_dir = os.path.dirname(os.path.abspath(OUTPUT_XLSX))
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as xw:
        all_trades_df.to_excel(xw, sheet_name="all_trades_backtested", index=False)
        actual_trades_df.to_excel(xw, sheet_name="actual_trades", index=False)
        daily_pnl_df.to_excel(xw, sheet_name="daily_pnl_actual", index=False)
        monthwise_df.to_excel(xw, sheet_name="monthwise_summary", index=False)
        instrument_summary_df.to_excel(xw, sheet_name="instrument_summary", index=False)
        skipped_df.to_excel(xw, sheet_name="skipped", index=False)

        wb = xw.book
        for ws in wb.worksheets:
            ws.freeze_panes = "A2"
            _autosize_columns_safe(ws)

    print(f"[DONE] Excel written: {OUTPUT_XLSX}")


# =============================================================================
# MAIN
# =============================================================================
def main():
    validate_user_config()

    paths = sorted(glob.glob(os.path.join(PICKLES_DIR, "*.pkl")) + glob.glob(os.path.join(PICKLES_DIR, "*.pickle")))
    if not paths:
        raise FileNotFoundError(f"No .pkl/.pickle files found in: {PICKLES_DIR}")

    # Step 1: determine window end.
    if WINDOW_END_MODE == "today":
        end_day = date.today()
    else:
        max_day = discover_data_max_day(paths)
        end_day = max_day if max_day is not None else date.today()

    window_start = compute_window_start(end_day, LOOKBACK_MONTHS)

    print(f"[INFO] Pickles found: {len(paths)}")
    print(f"[INFO] WindowEndMode={WINDOW_END_MODE} => end_day={end_day} | Window: {window_start} -> {end_day}")
    print(f"[INFO] Entry time: {ENTRY_TIME_IST}")
    print(f"[INFO] OTM distance steps: {OTM_DISTANCE_STEPS}")
    print(f"[INFO] Leg premium rise exit pct: {LEG_PREMIUM_RISE_EXIT_PCT}")
    print(f"[INFO] Max reattempts: {MAX_REATTEMPTS} | Total attempt slots/day: {1 + MAX_REATTEMPTS} | Re-entry delay min: {REENTRY_DELAY_MINUTES}")
    print(f"[INFO] Strict strike presence: {STRICT_STRIKE_PRESENCE} (max_missing_streak={MAX_MISSING_STREAK_MIN}m)")
    print(f"[INFO] Dedup within pickle: {DEDUP_WITHIN_PICKLE} | Dedup across pickles: {DEDUP_ACROSS_PICKLES}")
    print(f"[INFO] Include transaction costs: {INCLUDE_TRANSACTION_COSTS}")
    print(f"[INFO] Output: {OUTPUT_XLSX}")

    # Step 2: GLOBAL pass-1 to decide the true nearest expiry and actual underlying per day.
    min_expiry_map, min_day_seen, max_day_seen_filtered = scan_pickles_pass1_dhan(paths, window_start, end_day)
    if not min_expiry_map:
        raise RuntimeError("No usable 0-DTE / 1-DTE Dhan option data found in the requested window.")

    actual_underlying_by_day = pick_actual_underlying_by_day(min_expiry_map)

    print(f"[INFO] Filtered usable day-range: {min_day_seen} -> {max_day_seen_filtered}")
    print(f"[INFO] Global eligible (underlying,day) pairs: {len(min_expiry_map)}")
    print(f"[INFO] Actual-trade days selected: {len(actual_underlying_by_day)}")

    # Step 3: GLOBAL pass-2 simulation.
    all_trades_df, skipped_df = process_pickles_generate_trades_dhan(
        pickle_paths=paths,
        min_expiry_map=min_expiry_map,
        actual_underlying_by_day=actual_underlying_by_day,
        window_start=window_start,
        window_end=end_day,
    )

    # Final dedup safety net.
    if not all_trades_df.empty and DEDUP_ACROSS_PICKLES:
        all_trades_df, dedup_report = dedup_trades_across_pickles(all_trades_df)
        if not dedup_report.empty:
            skipped_df = pd.concat([skipped_df, dedup_report], ignore_index=True)

    if not all_trades_df.empty:
        all_trades_df = all_trades_df.sort_values(["day", "underlying", "trade_seq", "source_pickle"]).reset_index(drop=True)

    actual_trades_df = build_actual_trades_df(all_trades_df, actual_underlying_by_day)
    daily_pnl_df, monthwise_df = build_daily_and_monthly_summary(actual_trades_df)
    instrument_summary_df = build_instrument_summary(all_trades_df)

    write_excel(
        all_trades_df=all_trades_df,
        actual_trades_df=actual_trades_df,
        daily_pnl_df=daily_pnl_df,
        monthwise_df=monthwise_df,
        instrument_summary_df=instrument_summary_df,
        skipped_df=skipped_df,
    )

    if all_trades_df.empty:
        print("[WARN] No completed trades. Check 'skipped' sheet for reasons.")
    else:
        print(all_trades_df.groupby("underlying")[["exit_pnl"]].describe())


if __name__ == "__main__":
    main()
