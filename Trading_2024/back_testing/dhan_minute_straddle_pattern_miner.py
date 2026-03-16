"""
DHAN MINUTE-BY-MINUTE SHORT-STRADDLE PATTERN MINER
===================================================

Purpose
-------
This is a *research* script, not a live-trading script.

It scans Dhan rolling-options pickles and does the following:
1) Normalizes the pickle schema into a stable intraday format.
2) For every trading day and every candidate entry minute in a configurable
   entry window, reconstructs the actual ATM short straddle using the nearest
   available expiry in that pickle.
3) Simulates the trade with the same broad mechanics as the existing Dhan
   backtester:
      - sell ATM CE + ATM PE at the candidate minute
      - stoploss on combined premium (with optional rupee cap)
      - profit-protect on combined premium
      - else exit at EOD
4) Computes *pre-entry* features using only information available up to the
   entry minute.
5) Writes candidate-level trade outcomes plus summary sheets so you can study
   what underlying states tend to produce better or worse short-straddle
   expectancy.

Why this script is different from the fixed-time backtester
-----------------------------------------------------------
The existing script backtests one entry time per day and optionally reattempts.
This script does *not* do reattempts. Instead, it treats each candidate minute
as its own hypothetical first entry and records the result.

That makes it suitable for pattern discovery:
- which time windows are better?
- does a large move from open help or hurt?
- does low path efficiency help?
- does chop help?
- does rich premium versus underlying movement help?

Important statistical caution
-----------------------------
Candidate rows from the same day are highly correlated. Do not interpret
200,000 minute-level candidates as 200,000 independent observations. Use this
script to discover structure, then validate any promising rule at the day or
regime level.

Example usage
-------------
Windows CMD:
    set DHAN_PICKLES_DIR=G:\\My Drive\\Trading\\Dhan_Historical_Options_Data
    set ENTRY_START_IST=09:20
    set ENTRY_END_IST=13:30
    set ENTRY_STEP_MINUTES=1
    python dhan_minute_straddle_pattern_miner.py

PowerShell:
    $env:DHAN_PICKLES_DIR="G:\\My Drive\\Trading\\Dhan_Historical_Options_Data"
    $env:ENTRY_START_IST="09:20"
    $env:ENTRY_END_IST="13:30"
    $env:ENTRY_STEP_MINUTES="1"
    python dhan_minute_straddle_pattern_miner.py
"""

import os
import glob
import time
from dataclasses import dataclass, asdict
from datetime import datetime, date, time as dtime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

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
# CONFIG
# =============================================================================
PICKLES_DIR = os.getenv("DHAN_PICKLES_DIR", r"G:\My Drive\Trading\Dhan_Historical_Options_Data")

ENTRY_START_IST = os.getenv("ENTRY_START_IST", "09:20")
ENTRY_END_IST = os.getenv("ENTRY_END_IST", "13:30")
ENTRY_STEP_MINUTES = int(os.getenv("ENTRY_STEP_MINUTES", "1"))

LOSS_LIMIT_PCT = float(os.getenv("LOSS_LIMIT_PCT", "0.20"))
PROFIT_PROTECT_TRIGGER_PCT = float(os.getenv("PROFIT_PROTECT_TRIGGER_PCT", "0.30"))
MAX_STOPLOSS_RUPEES = abs(float(os.getenv("MAX_STOPLOSS_RUPEES", "3000")))

WINDOW_END_MODE = os.getenv("WINDOW_END_MODE", "data").strip().lower()
LOOKBACK_MONTHS = int(os.getenv("LOOKBACK_MONTHS", "36"))

QTY_UNITS = {"NIFTY": 325, "SENSEX": 100}
TRADEABLE = set(QTY_UNITS.keys())
STRIKE_STEP = {"NIFTY": 50, "SENSEX": 100}

# Zerodha-like cost model carried forward from the reference script.
BROKERAGE_PER_ORDER = 20.0
ORDERS_PER_TRADE = 4
STT_SELL_PCT = 0.001
EXCHANGE_TXN_PCT = 0.0003553
SEBI_PER_CRORE = 10.0
STAMP_BUY_PCT = 0.00003
IPFT_PER_CRORE = 0.010
GST_PCT = 0.18
INCLUDE_TRANSACTION_COSTS = os.getenv("INCLUDE_TRANSACTION_COSTS", "1").strip() not in ("0", "false", "False")

SESSION_START_IST = dtime(9, 15)
SESSION_END_IST = dtime(15, 30)
FAIL_ON_PICKLE_ERROR = os.getenv("FAIL_ON_PICKLE_ERROR", "0").strip() == "1"
STRICT_STRIKE_PRESENCE = os.getenv("STRICT_STRIKE_PRESENCE", "0").strip() == "1"
MAX_MISSING_STREAK_MIN = int(os.getenv("MAX_MISSING_STREAK_MIN", "10"))
DEDUP_WITHIN_PICKLE = os.getenv("DEDUP_WITHIN_PICKLE", "1").strip() not in ("0", "false", "False")
DEDUP_ACROSS_PICKLES = os.getenv("DEDUP_ACROSS_PICKLES", "1").strip() not in ("0", "false", "False")

BIN_QUANTILES = int(os.getenv("BIN_QUANTILES", "5"))
WRITE_PARQUET = os.getenv("WRITE_PARQUET", "1").strip() not in ("0", "false", "False")


def _safe_fname_part(s: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in s)


_DEFAULT_XLSX = (
    Path.home()
    / "Downloads"
    / f"dhan_minute_straddle_pattern_miner_{_safe_fname_part(ENTRY_START_IST)}_to_{_safe_fname_part(ENTRY_END_IST)}_step_{ENTRY_STEP_MINUTES}m.xlsx"
)
OUTPUT_XLSX = os.getenv("OUTPUT_XLSX", str(_DEFAULT_XLSX))

_DEFAULT_PARQUET = (
    Path.home()
    / "Downloads"
    / f"dhan_minute_straddle_pattern_candidates_{_safe_fname_part(ENTRY_START_IST)}_to_{_safe_fname_part(ENTRY_END_IST)}_step_{ENTRY_STEP_MINUTES}m.parquet"
)
OUTPUT_PARQUET = os.getenv("OUTPUT_PARQUET", str(_DEFAULT_PARQUET))


# =============================================================================
# TIME HELPERS
# =============================================================================
def parse_hhmm(s: str) -> dtime:
    hh, mm = s.strip().split(":")
    return dtime(int(hh), int(mm))


ENTRY_START = parse_hhmm(ENTRY_START_IST)
ENTRY_END = parse_hhmm(ENTRY_END_IST)


def ist_tz():
    if ZoneInfo is not None:
        return ZoneInfo("Asia/Kolkata")
    if pytz is not None:
        return pytz.timezone("Asia/Kolkata")
    return "Asia/Kolkata"


def ensure_ist(x):
    dt = pd.to_datetime(x, errors="coerce")
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
    tz = ist_tz()
    start = pd.Timestamp(datetime.combine(day_d, start_t), tz=tz)
    end = pd.Timestamp(datetime.combine(day_d, end_t), tz=tz)
    return pd.date_range(start=start, end=end, freq="1min")


def build_candidate_index(day_d: date) -> pd.DatetimeIndex:
    tz = ist_tz()
    start = pd.Timestamp(datetime.combine(day_d, ENTRY_START), tz=tz)
    end = pd.Timestamp(datetime.combine(day_d, ENTRY_END), tz=tz)
    return pd.date_range(start=start, end=end, freq=f"{ENTRY_STEP_MINUTES}min")


def compute_window_start(end_day: date, months: int) -> date:
    if relativedelta is not None:
        return (pd.Timestamp(end_day) - relativedelta(months=months)).date()
    return (pd.Timestamp(end_day) - pd.Timedelta(days=30 * months)).date()


def round_to_step(x: float, step: int) -> int:
    return int(round(x / step) * step)


# =============================================================================
# SMALL UTILITIES
# =============================================================================
def safe_div(a: Any, b: Any, default: float = np.nan) -> float:
    try:
        if b in (0, 0.0) or pd.isna(b):
            return float(default)
        return float(a) / float(b)
    except Exception:
        return float(default)


def sign_change_count(arr: np.ndarray) -> float:
    x = np.asarray(arr, dtype=float)
    x = x[~np.isnan(x)]
    if x.size <= 1:
        return 0.0
    signs = np.sign(x)
    signs = signs[signs != 0]
    if signs.size <= 1:
        return 0.0
    return float(np.sum(signs[1:] != signs[:-1]))


def max_consecutive_nan(s: pd.Series) -> int:
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


def profit_factor(pnls: pd.Series) -> float:
    pnls = pd.to_numeric(pnls, errors="coerce").dropna().astype(float)
    gp = pnls[pnls > 0].sum()
    gl = -pnls[pnls < 0].sum()
    if gl > 0:
        return float(gp / gl)
    return float("inf") if gp > 0 else 0.0


# =============================================================================
# TRANSACTION CHARGES
# =============================================================================
def compute_trade_charges(entry_ce: float, entry_pe: float, exit_ce: float, exit_pe: float, qty: int) -> float:
    if not INCLUDE_TRANSACTION_COSTS:
        return 0.0

    entry_turnover = (entry_ce + entry_pe) * qty
    exit_turnover = (exit_ce + exit_pe) * qty
    total_turnover = entry_turnover + exit_turnover

    brokerage = BROKERAGE_PER_ORDER * ORDERS_PER_TRADE
    stt = entry_turnover * STT_SELL_PCT
    txn_charges = total_turnover * EXCHANGE_TXN_PCT
    sebi = total_turnover * SEBI_PER_CRORE / 1_00_00_000
    stamp = exit_turnover * STAMP_BUY_PCT
    ipft = total_turnover * IPFT_PER_CRORE / 1_00_00_000
    gst = (brokerage + txn_charges + sebi) * GST_PCT

    return round(brokerage + stt + txn_charges + sebi + stamp + ipft + gst, 2)


# =============================================================================
# OUTPUT ROW
# =============================================================================
@dataclass
class CandidateTradeRow:
    day: date
    underlying: str
    expiry: date
    days_to_expiry: int
    source_pickle: str

    entry_time: str
    exit_time: str
    exit_reason: str
    atm_strike: int
    qty_units: int

    entry_underlying: float
    entry_ce: float
    entry_pe: float
    entry_premium_points: float
    entry_premium_rupees: float
    ce_pe_imbalance_pct: float
    premium_change_5m_points: float
    premium_change_10m_points: float
    premium_vs_range_10: float
    premium_vs_range_15: float

    exit_ce: float
    exit_pe: float
    exit_pnl_gross: float
    txn_charges: float
    exit_pnl: float
    eod_pnl_gross: float
    eod_pnl_net: float
    max_profit_gross: float
    max_loss_gross: float
    sl_effective_rupees: float
    profit_protect_rupees: float

    is_profitable: int
    stoploss_hit: int
    profit_protect_hit: int

    minutes_since_open: int
    move_from_open_pts: float
    move_from_open_abs_pts: float
    move_from_open_pct: float
    range_from_open_pts: float
    move_5m_pts: float
    move_10m_pts: float
    move_15m_pts: float
    move_5m_abs_pts: float
    move_10m_abs_pts: float
    move_15m_abs_pts: float
    range_5m_pts: float
    range_10m_pts: float
    range_15m_pts: float
    rv_5_bps: float
    rv_10_bps: float
    rv_20_bps: float
    path_len_5m: float
    path_len_10m: float
    path_len_15m: float
    path_eff_5m: float
    path_eff_10m: float
    path_eff_15m: float
    sign_changes_5m: float
    sign_changes_10m: float
    sign_changes_15m: float
    close_loc_10m: float
    close_loc_15m: float
    dist_from_ma_10_pts: float
    dist_from_ma_20_pts: float
    chop_excess_10m: float
    chop_excess_15m: float


# =============================================================================
# DHAN PICKLE NORMALIZATION
# =============================================================================
def _pick_time_col(df: pd.DataFrame) -> str:
    if "dt_ist" in df.columns:
        return "dt_ist"
    if "timestamp_dt" in df.columns:
        return "timestamp_dt"
    if "timestamp" in df.columns:
        return "timestamp"
    raise ValueError("No usable time column found (expected dt_ist / timestamp_dt / timestamp).")


def _normalize_dhan_df(df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    needed = ["symbol", "leg", "strike", "close", "spot", "target_expiry_date"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(f"{source_name}: missing columns {missing}")

    d = df.copy()
    tcol = _pick_time_col(d)

    if tcol in ("dt_ist", "timestamp_dt"):
        d["ts"] = ensure_ist(d[tcol])
    else:
        dt_utc = pd.to_datetime(d["timestamp"], unit="s", utc=True, errors="coerce")
        d["ts"] = dt_utc.dt.tz_convert("Asia/Kolkata")

    d["ts"] = d["ts"].dt.floor("min")

    if "date_ist" in d.columns:
        d["day"] = pd.to_datetime(d["date_ist"], errors="coerce").dt.date
    else:
        d["day"] = d["ts"].dt.date

    d["underlying"] = d["symbol"].astype(str).str.upper().str.strip()
    d = d[d["underlying"].isin(TRADEABLE)]

    d["expiry"] = pd.to_datetime(d["target_expiry_date"], errors="coerce").dt.date
    d["strike_num"] = pd.to_numeric(d["strike"], errors="coerce")
    d["strike_int"] = d["strike_num"].round().astype("Int64")

    d["close_f"] = pd.to_numeric(d["close"], errors="coerce")
    d["high_f"] = pd.to_numeric(d["high"] if "high" in d.columns else d["close"], errors="coerce")
    d["low_f"] = pd.to_numeric(d["low"] if "low" in d.columns else d["close"], errors="coerce")
    d["spot_f"] = pd.to_numeric(d["spot"], errors="coerce")

    d["high_f"] = d["high_f"].fillna(d["close_f"])
    d["low_f"] = d["low_f"].fillna(d["close_f"])

    d["leg"] = d["leg"].astype(str).str.upper().str.strip()
    d = d[d["leg"].isin(["CE", "PE"])]

    d = d.dropna(subset=["ts", "day", "expiry", "strike_int", "close_f", "high_f", "low_f", "spot_f"])
    d["strike_int"] = d["strike_int"].astype(int)
    d["close_f"] = d["close_f"].astype(float)
    d["high_f"] = d["high_f"].astype(float)
    d["low_f"] = d["low_f"].astype(float)
    d["spot_f"] = d["spot_f"].astype(float)

    d = d[d["expiry"] >= d["day"]]

    if DEDUP_WITHIN_PICKLE and not d.empty:
        d = d.sort_values("ts").drop_duplicates(
            subset=["ts", "underlying", "expiry", "leg", "strike_int"], keep="last"
        )

    keep = ["ts", "day", "underlying", "expiry", "leg", "strike_int", "close_f", "high_f", "low_f", "spot_f"]
    return d[keep].copy()


# =============================================================================
# DATA DISCOVERY
# =============================================================================
def discover_data_max_day(pickle_paths: List[str]) -> Optional[date]:
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
                ts = pd.to_datetime(raw["timestamp"], unit="s", utc=True, errors="coerce")
                day = ts.dt.tz_convert("Asia/Kolkata").dt.floor("min").dt.date.max()
            if day is not None:
                max_day = day if (max_day is None or day > max_day) else max_day
        except Exception:
            continue
    return max_day


# =============================================================================
# SERIES BUILDERS
# =============================================================================
def _build_underlying_series_from_spot(day_opt: pd.DataFrame, idx_all: pd.DatetimeIndex) -> pd.Series:
    sub = day_opt[["ts", "spot_f"]].dropna()
    if sub.empty:
        return pd.Series(index=idx_all, dtype="float64")
    sub = sub.sort_values("ts").groupby("ts", as_index=True)["spot_f"].last()
    return sub.reindex(idx_all).ffill()


def _build_leg_series_fixed_strike(day_opt: pd.DataFrame, idx_all: pd.DatetimeIndex, strike: int, leg: str, value_col: str) -> pd.Series:
    sub = day_opt[(day_opt["strike_int"] == strike) & (day_opt["leg"] == leg)][["ts", value_col]].dropna()
    if sub.empty:
        return pd.Series(index=idx_all, dtype="float64")
    sub = sub.sort_values("ts").groupby("ts", as_index=True)[value_col].last()
    return sub.reindex(idx_all).ffill(limit=3)


def get_strike_cache(day_opt: pd.DataFrame, idx_all: pd.DatetimeIndex, strike: int, cache: Dict[int, Dict[str, pd.Series]]) -> Dict[str, pd.Series]:
    if strike not in cache:
        cache[strike] = {
            "ce_close": _build_leg_series_fixed_strike(day_opt, idx_all, strike, "CE", "close_f"),
            "pe_close": _build_leg_series_fixed_strike(day_opt, idx_all, strike, "PE", "close_f"),
            "ce_high": _build_leg_series_fixed_strike(day_opt, idx_all, strike, "CE", "high_f"),
            "pe_high": _build_leg_series_fixed_strike(day_opt, idx_all, strike, "PE", "high_f"),
            "ce_low": _build_leg_series_fixed_strike(day_opt, idx_all, strike, "CE", "low_f"),
            "pe_low": _build_leg_series_fixed_strike(day_opt, idx_all, strike, "PE", "low_f"),
        }
    return cache[strike]


# =============================================================================
# PRE-ENTRY FEATURES (HEAVILY WEIGHTED TOWARD UNDERLYING MOVEMENT)
# =============================================================================
def precompute_underlying_features(spot_s: pd.Series) -> pd.DataFrame:
    f = pd.DataFrame(index=spot_s.index)
    f["spot"] = pd.to_numeric(spot_s, errors="coerce")
    f["ret_1m_pts"] = f["spot"].diff()
    f["ret_1m_pct"] = f["spot"].pct_change()
    f["minutes_since_open"] = np.arange(len(f), dtype=int)

    first_valid = f["spot"].dropna()
    if first_valid.empty:
        return f

    open_px = float(first_valid.iloc[0])
    f["open_spot"] = open_px
    f["move_from_open_pts"] = f["spot"] - open_px
    f["move_from_open_abs_pts"] = f["move_from_open_pts"].abs()
    f["move_from_open_pct"] = 100.0 * f["move_from_open_pts"] / open_px
    f["cum_high"] = f["spot"].cummax()
    f["cum_low"] = f["spot"].cummin()
    f["range_from_open_pts"] = f["cum_high"] - f["cum_low"]

    for w in (5, 10, 15):
        f[f"move_{w}m_pts"] = f["spot"] - f["spot"].shift(w)
        f[f"move_{w}m_abs_pts"] = f[f"move_{w}m_pts"].abs()
        f[f"range_{w}m_pts"] = f["spot"].rolling(w, min_periods=max(2, w // 2)).max() - f["spot"].rolling(w, min_periods=max(2, w // 2)).min()
        f[f"rv_{w}_bps"] = f["ret_1m_pct"].rolling(w, min_periods=max(2, w // 2)).std() * 10000.0
        f[f"path_len_{w}m"] = f["ret_1m_pts"].abs().rolling(w, min_periods=max(2, w // 2)).sum()
        f[f"path_eff_{w}m"] = f[f"move_{w}m_abs_pts"] / f[f"path_len_{w}m"].replace(0.0, np.nan)
        f[f"close_loc_{w}m"] = (
            f["spot"] - f["spot"].rolling(w, min_periods=max(2, w // 2)).min()
        ) / (
            f["spot"].rolling(w, min_periods=max(2, w // 2)).max()
            - f["spot"].rolling(w, min_periods=max(2, w // 2)).min()
        ).replace(0.0, np.nan)
        f[f"sign_changes_{w}m"] = f["ret_1m_pts"].rolling(w, min_periods=max(3, w // 2)).apply(sign_change_count, raw=True)
        f[f"chop_excess_{w}m"] = f[f"path_len_{w}m"] - f[f"move_{w}m_abs_pts"]

    # rv_20 computed separately (used in dataclass but no other 20m features needed)
    f["rv_20_bps"] = f["ret_1m_pct"].rolling(20, min_periods=10).std() * 10000.0

    f["ma_10"] = f["spot"].rolling(10, min_periods=5).mean()
    f["ma_20"] = f["spot"].rolling(20, min_periods=10).mean()
    f["dist_from_ma_10_pts"] = f["spot"] - f["ma_10"]
    f["dist_from_ma_20_pts"] = f["spot"] - f["ma_20"]

    return f


# =============================================================================
# SINGLE-CANDIDATE SIMULATION
# =============================================================================
def simulate_single_candidate(
    *,
    und: str,
    dy: date,
    expiry: date,
    entry_ts: pd.Timestamp,
    source_pickle: str,
    day_opt: pd.DataFrame,
    idx_all: pd.DatetimeIndex,
    spot_s: pd.Series,
    feat_s: pd.DataFrame,
    strike_cache: Dict[int, Dict[str, pd.Series]],
) -> Tuple[Optional[CandidateTradeRow], Optional[Dict[str, Any]]]:
    if entry_ts not in idx_all:
        return None, {"source_pickle": source_pickle, "day": dy, "underlying": und, "expiry": expiry, "entry_time": entry_ts.strftime("%H:%M"), "reason": "Entry timestamp not in session grid"}

    spot = spot_s.loc[entry_ts]
    if pd.isna(spot):
        return None, {"source_pickle": source_pickle, "day": dy, "underlying": und, "expiry": expiry, "entry_time": entry_ts.strftime("%H:%M"), "reason": "No spot at candidate minute"}

    qty = int(QTY_UNITS[und])
    step = int(STRIKE_STEP[und])
    atm = round_to_step(float(spot), step)
    series = get_strike_cache(day_opt, idx_all, atm, strike_cache)

    ce_close = series["ce_close"]
    pe_close = series["pe_close"]
    ce_high = series["ce_high"]
    pe_high = series["pe_high"]
    ce_low = series["ce_low"]
    pe_low = series["pe_low"]

    ce_entry = ce_close.loc[entry_ts]
    pe_entry = pe_close.loc[entry_ts]
    if pd.isna(ce_entry) or pd.isna(pe_entry):
        return None, {"source_pickle": source_pickle, "day": dy, "underlying": und, "expiry": expiry, "entry_time": entry_ts.strftime("%H:%M"), "atm_strike": atm, "reason": "No CE/PE price at candidate minute"}

    if STRICT_STRIKE_PRESENCE:
        max_miss = max(max_consecutive_nan(ce_close.loc[entry_ts:]), max_consecutive_nan(pe_close.loc[entry_ts:]))
        if max_miss > MAX_MISSING_STREAK_MIN:
            return None, {"source_pickle": source_pickle, "day": dy, "underlying": und, "expiry": expiry, "entry_time": entry_ts.strftime("%H:%M"), "atm_strike": atm, "reason": f"Strike missing too much after entry (max_missing_streak={max_miss}m)"}

    if float(ce_entry) <= 0 or float(pe_entry) <= 0:
        return None, {"source_pickle": source_pickle, "day": dy, "underlying": und,
                       "expiry": expiry, "entry_time": entry_ts.strftime("%H:%M"),
                       "atm_strike": atm, "reason": "Non-positive CE/PE entry price"}

    premium_points = float(ce_entry) + float(pe_entry)
    premium_rupees = premium_points * qty
    loss_limit_rupees = premium_rupees * LOSS_LIMIT_PCT
    sl_effective_rupees = min(loss_limit_rupees, MAX_STOPLOSS_RUPEES) if MAX_STOPLOSS_RUPEES > 0 else loss_limit_rupees
    g_rupees = premium_rupees * PROFIT_PROTECT_TRIGGER_PCT
    profit_protect_enabled = g_rupees > 0

    # Combined premium series
    combined_close_all = ce_close + pe_close
    combined_high_all  = ce_high + pe_high

    # PnL from close prices (used for profit-protect and reporting)
    pnl_close_all = (premium_points - combined_close_all) * qty
    pnl_close = pnl_close_all.loc[entry_ts:].dropna()
    if pnl_close.empty:
        return None, {"source_pickle": source_pickle, "day": dy, "underlying": und,
                       "expiry": expiry, "entry_time": entry_ts.strftime("%H:%M"),
                       "atm_strike": atm, "reason": "PnL series empty after candidate entry"}

    # Conservative intrabar stoploss: use combined highs (worst case both legs spike)
    pnl_sl_all = (premium_points - combined_high_all) * qty
    pnl_sl = pnl_sl_all.loc[entry_ts:].dropna()

    stop_hit = pnl_sl <= -sl_effective_rupees
    stop_ts = pnl_sl.index[stop_hit.to_numpy().argmax()] if stop_hit.any() else None

    protect_ts = None
    if profit_protect_enabled:
        peak = pnl_close.cummax()
        armed = peak >= g_rupees
        trail = peak - g_rupees
        protect_hit = armed & (pnl_close <= trail)
        protect_ts = pnl_close.index[protect_hit.to_numpy().argmax()] if protect_hit.any() else None

    exit_ts = pnl_close.index[-1]
    exit_reason = "EOD"
    if stop_ts is not None and protect_ts is not None:
        if stop_ts <= protect_ts:
            exit_ts, exit_reason = stop_ts, "STOPLOSS"
        else:
            exit_ts, exit_reason = protect_ts, "PROFIT_PROTECT"
    elif stop_ts is not None:
        exit_ts, exit_reason = stop_ts, "STOPLOSS"
    elif protect_ts is not None:
        exit_ts, exit_reason = protect_ts, "PROFIT_PROTECT"

    exit_pnl_gross = float(pnl_close.loc[exit_ts])
    if exit_reason == "STOPLOSS" and exit_pnl_gross < -sl_effective_rupees:
        exit_pnl_gross = -float(sl_effective_rupees)

    exit_ce = float(ce_close.loc[exit_ts]) if pd.notna(ce_close.loc[exit_ts]) else float("nan")
    exit_pe = float(pe_close.loc[exit_ts]) if pd.notna(pe_close.loc[exit_ts]) else float("nan")
    txn_charges = compute_trade_charges(float(ce_entry), float(pe_entry), exit_ce if not pd.isna(exit_ce) else 0.0, exit_pe if not pd.isna(exit_pe) else 0.0, qty)
    exit_pnl_net = exit_pnl_gross - txn_charges

    eod_ts = pnl_close.index[-1]
    eod_pnl_gross = float(pnl_close.iloc[-1])
    eod_ce = float(ce_close.loc[eod_ts]) if pd.notna(ce_close.loc[eod_ts]) else 0.0
    eod_pe = float(pe_close.loc[eod_ts]) if pd.notna(pe_close.loc[eod_ts]) else 0.0
    eod_txn_charges = compute_trade_charges(float(ce_entry), float(pe_entry), eod_ce, eod_pe, qty)
    eod_pnl_net = eod_pnl_gross - eod_txn_charges

    lookup_5 = entry_ts - pd.Timedelta(minutes=5)
    lookup_10 = entry_ts - pd.Timedelta(minutes=10)
    prem_5 = (ce_close.loc[lookup_5] + pe_close.loc[lookup_5]) if lookup_5 in ce_close.index else np.nan
    prem_10 = (ce_close.loc[lookup_10] + pe_close.loc[lookup_10]) if lookup_10 in ce_close.index else np.nan

    dte = int((expiry - dy).days)
    fv = feat_s.loc[entry_ts]

    row = CandidateTradeRow(
        day=dy,
        underlying=und,
        expiry=expiry,
        days_to_expiry=dte,
        source_pickle=source_pickle,
        entry_time=entry_ts.strftime("%H:%M"),
        exit_time=pd.Timestamp(exit_ts).strftime("%H:%M"),
        exit_reason=exit_reason,
        atm_strike=int(atm),
        qty_units=qty,
        entry_underlying=float(spot),
        entry_ce=float(ce_entry),
        entry_pe=float(pe_entry),
        entry_premium_points=premium_points,
        entry_premium_rupees=premium_rupees,
        ce_pe_imbalance_pct=100.0 * safe_div(abs(float(ce_entry) - float(pe_entry)), premium_points),
        premium_change_5m_points=float(premium_points - prem_5) if pd.notna(prem_5) else np.nan,
        premium_change_10m_points=float(premium_points - prem_10) if pd.notna(prem_10) else np.nan,
        premium_vs_range_10=safe_div(premium_points, fv.get("range_10m_pts", np.nan)),
        premium_vs_range_15=safe_div(premium_points, fv.get("range_15m_pts", np.nan)),
        exit_ce=exit_ce,
        exit_pe=exit_pe,
        exit_pnl_gross=exit_pnl_gross,
        txn_charges=txn_charges,
        exit_pnl=exit_pnl_net,
        eod_pnl_gross=eod_pnl_gross,
        eod_pnl_net=eod_pnl_net,
        max_profit_gross=float(max(0.0, pnl_close.max())),
        max_loss_gross=float(min(0.0, pnl_close.min())),
        sl_effective_rupees=float(sl_effective_rupees),
        profit_protect_rupees=float(g_rupees),
        is_profitable=int(exit_pnl_net > 0),
        stoploss_hit=int(exit_reason == "STOPLOSS"),
        profit_protect_hit=int(exit_reason == "PROFIT_PROTECT"),
        minutes_since_open=int(fv.get("minutes_since_open", np.nan)),
        move_from_open_pts=float(fv.get("move_from_open_pts", np.nan)),
        move_from_open_abs_pts=float(fv.get("move_from_open_abs_pts", np.nan)),
        move_from_open_pct=float(fv.get("move_from_open_pct", np.nan)),
        range_from_open_pts=float(fv.get("range_from_open_pts", np.nan)),
        move_5m_pts=float(fv.get("move_5m_pts", np.nan)),
        move_10m_pts=float(fv.get("move_10m_pts", np.nan)),
        move_15m_pts=float(fv.get("move_15m_pts", np.nan)),
        move_5m_abs_pts=float(fv.get("move_5m_abs_pts", np.nan)),
        move_10m_abs_pts=float(fv.get("move_10m_abs_pts", np.nan)),
        move_15m_abs_pts=float(fv.get("move_15m_abs_pts", np.nan)),
        range_5m_pts=float(fv.get("range_5m_pts", np.nan)),
        range_10m_pts=float(fv.get("range_10m_pts", np.nan)),
        range_15m_pts=float(fv.get("range_15m_pts", np.nan)),
        rv_5_bps=float(fv.get("rv_5_bps", np.nan)),
        rv_10_bps=float(fv.get("rv_10_bps", np.nan)),
        rv_20_bps=float(fv.get("rv_20_bps", np.nan)),
        path_len_5m=float(fv.get("path_len_5m", np.nan)),
        path_len_10m=float(fv.get("path_len_10m", np.nan)),
        path_len_15m=float(fv.get("path_len_15m", np.nan)),
        path_eff_5m=float(fv.get("path_eff_5m", np.nan)),
        path_eff_10m=float(fv.get("path_eff_10m", np.nan)),
        path_eff_15m=float(fv.get("path_eff_15m", np.nan)),
        sign_changes_5m=float(fv.get("sign_changes_5m", np.nan)),
        sign_changes_10m=float(fv.get("sign_changes_10m", np.nan)),
        sign_changes_15m=float(fv.get("sign_changes_15m", np.nan)),
        close_loc_10m=float(fv.get("close_loc_10m", np.nan)),
        close_loc_15m=float(fv.get("close_loc_15m", np.nan)),
        dist_from_ma_10_pts=float(fv.get("dist_from_ma_10_pts", np.nan)),
        dist_from_ma_20_pts=float(fv.get("dist_from_ma_20_pts", np.nan)),
        chop_excess_10m=float(fv.get("chop_excess_10m", np.nan)),
        chop_excess_15m=float(fv.get("chop_excess_15m", np.nan)),
    )

    return row, None


# =============================================================================
# PER-PICKLE PROCESSING
# =============================================================================
def process_one_pickle(path: str, window_start: date, window_end: date) -> Tuple[pd.DataFrame, pd.DataFrame]:
    src = os.path.basename(path)
    raw = pd.read_pickle(path)
    if not isinstance(raw, pd.DataFrame) or raw.empty:
        return pd.DataFrame(), pd.DataFrame([{"source_pickle": src, "reason": "Empty or non-DataFrame pickle"}])

    d = _normalize_dhan_df(raw, src)
    if d.empty:
        return pd.DataFrame(), pd.DataFrame([{"source_pickle": src, "reason": "No usable rows after normalization"}])

    d = d[(d["day"] >= window_start) & (d["day"] <= window_end)]
    if d.empty:
        return pd.DataFrame(), pd.DataFrame([{"source_pickle": src, "reason": "No rows in requested date window"}])

    min_expiry_local: Dict[Tuple[str, date], date] = d.groupby(["underlying", "day"], sort=False)["expiry"].min().to_dict()

    rows: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []

    for (und, dy, ex), g in d.groupby(["underlying", "day", "expiry"], sort=False):
        if min_expiry_local.get((und, dy)) != ex:
            continue

        idx_all = build_minute_index(dy, SESSION_START_IST, SESSION_END_IST)
        spot_s = _build_underlying_series_from_spot(g, idx_all)
        feat_s = precompute_underlying_features(spot_s)
        strike_cache: Dict[int, Dict[str, pd.Series]] = {}

        for entry_ts in build_candidate_index(dy):
            row, skip = simulate_single_candidate(
                und=und,
                dy=dy,
                expiry=ex,
                entry_ts=entry_ts,
                source_pickle=src,
                day_opt=g,
                idx_all=idx_all,
                spot_s=spot_s,
                feat_s=feat_s,
                strike_cache=strike_cache,
            )
            if row is not None:
                rows.append(asdict(row))
            if skip is not None:
                skipped.append(skip)

    return pd.DataFrame(rows), pd.DataFrame(skipped)


# =============================================================================
# DEDUP AND SUMMARIES
# =============================================================================
def dedup_candidates_across_pickles(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty:
        return df, pd.DataFrame()
    key_cols = ["underlying", "day", "expiry", "entry_time"]
    before = len(df)
    out = df.sort_values(key_cols + ["source_pickle"]).drop_duplicates(subset=key_cols, keep="first").reset_index(drop=True)
    removed = before - len(out)
    if removed <= 0:
        return out, pd.DataFrame()
    rep = pd.DataFrame([{"source_pickle": "__aggregate__", "reason": f"Dedup across pickles removed {removed} duplicate candidate rows (key={key_cols})"}])
    return out, rep


def build_daily_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    x = df.copy()
    x["day"] = pd.to_datetime(x["day"])
    out = x.groupby(["day", "underlying"], as_index=False).agg(
        candidates=("exit_pnl", "count"),
        avg_exit_pnl=("exit_pnl", "mean"),
        median_exit_pnl=("exit_pnl", "median"),
        total_exit_pnl=("exit_pnl", "sum"),
        best_exit_pnl=("exit_pnl", "max"),
        worst_exit_pnl=("exit_pnl", "min"),
        profitable_candidates=("is_profitable", "sum"),
        stoploss_candidates=("stoploss_hit", "sum"),
        avg_entry_premium=("entry_premium_points", "mean"),
        first_profitable_time=("entry_time", lambda s: next((t for t, win in zip(s, x.loc[s.index, "is_profitable"]) if win == 1), None)),
    )
    out["win_rate_pct"] = 100.0 * out["profitable_candidates"] / out["candidates"]
    out["stoploss_rate_pct"] = 100.0 * out["stoploss_candidates"] / out["candidates"]
    out["month"] = out["day"].dt.to_period("M").astype(str)
    return out.sort_values(["day", "underlying"]).reset_index(drop=True)


def build_time_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    out = df.groupby(["underlying", "entry_time"], as_index=False).agg(
        candidates=("exit_pnl", "count"),
        avg_exit_pnl=("exit_pnl", "mean"),
        median_exit_pnl=("exit_pnl", "median"),
        total_exit_pnl=("exit_pnl", "sum"),
        best_exit_pnl=("exit_pnl", "max"),
        worst_exit_pnl=("exit_pnl", "min"),
        win_rate_pct=("is_profitable", lambda s: 100.0 * s.mean()),
        stoploss_rate_pct=("stoploss_hit", lambda s: 100.0 * s.mean()),
        avg_entry_premium=("entry_premium_points", "mean"),
        avg_move_from_open_abs_pts=("move_from_open_abs_pts", "mean"),
        avg_path_eff_10m=("path_eff_10m", "mean"),
        avg_rv_10_bps=("rv_10_bps", "mean"),
    )
    return out.sort_values(["underlying", "entry_time"]).reset_index(drop=True)


def summarize_feature_bins(df: pd.DataFrame, feature_cols: List[str], q: int = 5) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    rows: List[Dict[str, Any]] = []
    for feature in feature_cols:
        s = pd.to_numeric(df[feature], errors="coerce")
        valid = s.notna()
        if valid.sum() < max(20, q * 5):
            continue
        try:
            bins = pd.qcut(s[valid], q=q, duplicates="drop")
        except Exception:
            continue
        temp = df.loc[valid].copy()
        temp["feature_bin"] = bins.astype(str)
        grp = temp.groupby("feature_bin", as_index=False).agg(
            candidates=("exit_pnl", "count"),
            avg_exit_pnl=("exit_pnl", "mean"),
            median_exit_pnl=("exit_pnl", "median"),
            total_exit_pnl=("exit_pnl", "sum"),
            best_exit_pnl=("exit_pnl", "max"),
            worst_exit_pnl=("exit_pnl", "min"),
            win_rate_pct=("is_profitable", lambda x: 100.0 * x.mean()),
            stoploss_rate_pct=("stoploss_hit", lambda x: 100.0 * x.mean()),
            avg_feature_value=(feature, "mean"),
            min_feature_value=(feature, "min"),
            max_feature_value=(feature, "max"),
        )
        grp.insert(0, "feature", feature)
        rows.extend(grp.to_dict("records"))

    return pd.DataFrame(rows)


def build_overall_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    x = df.copy()
    x["day"] = pd.to_datetime(x["day"])
    daily = x.groupby(["day", "underlying"], as_index=False)["exit_pnl"].mean()
    rows = []
    for und, sub in x.groupby("underlying", sort=False):
        pnls = pd.to_numeric(sub["exit_pnl"], errors="coerce").dropna()
        daily_sub = daily[daily["underlying"] == und]["exit_pnl"]
        rows.append({
            "underlying": und,
            "candidates": int(len(sub)),
            "days": int(sub["day"].nunique()),
            "avg_exit_pnl": float(pnls.mean()),
            "median_exit_pnl": float(pnls.median()),
            "total_exit_pnl": float(pnls.sum()),
            "win_rate_pct": float(100.0 * sub["is_profitable"].mean()),
            "stoploss_rate_pct": float(100.0 * sub["stoploss_hit"].mean()),
            "profit_protect_rate_pct": float(100.0 * sub["profit_protect_hit"].mean()),
            "profit_factor": float(profit_factor(pnls)),
            "avg_daily_mean_pnl": float(pd.to_numeric(daily_sub, errors="coerce").mean()),
            "avg_entry_premium": float(pd.to_numeric(sub["entry_premium_points"], errors="coerce").mean()),
            "avg_move_from_open_abs_pts": float(pd.to_numeric(sub["move_from_open_abs_pts"], errors="coerce").mean()),
            "avg_path_eff_10m": float(pd.to_numeric(sub["path_eff_10m"], errors="coerce").mean()),
            "avg_rv_10_bps": float(pd.to_numeric(sub["rv_10_bps"], errors="coerce").mean()),
        })
    return pd.DataFrame(rows).sort_values("underlying").reset_index(drop=True)


# =============================================================================
# REGIME CLASSIFICATION (answers: "after what kind of movement?")
# =============================================================================
def classify_regime(df: pd.DataFrame) -> pd.Series:
    """
    Classify each candidate minute into an underlying movement regime.
    Uses ONLY pre-entry features. Returns a Series of regime labels.

    Regimes:
      POST_IMPULSE_SETTLING — big move already happened, but market is calming
      WAVY_CHOPPY           — lots of oscillation, low directional efficiency
      TRENDING              — strong directional move still in progress
      CALM_RANGE            — low volatility, small range, quiet market
      VOLATILE_UNCLEAR      — high vol but doesn't fit other categories
    """
    regimes = pd.Series("VOLATILE_UNCLEAR", index=df.index)

    move_abs = pd.to_numeric(df["move_from_open_abs_pts"], errors="coerce")
    path_eff = pd.to_numeric(df["path_eff_10m"], errors="coerce")
    sign_ch  = pd.to_numeric(df["sign_changes_10m"], errors="coerce")
    rv_10    = pd.to_numeric(df["rv_10_bps"], errors="coerce")
    range_10 = pd.to_numeric(df["range_10m_pts"], errors="coerce")
    chop_exc = pd.to_numeric(df["chop_excess_10m"], errors="coerce")

    # Use percentile ranks (adaptive to data, no hardcoded thresholds)
    move_pctile  = move_abs.rank(pct=True)
    eff_pctile   = path_eff.rank(pct=True)
    sign_pctile  = sign_ch.rank(pct=True)
    rv_pctile    = rv_10.rank(pct=True)
    range_pctile = range_10.rank(pct=True)
    chop_pctile  = chop_exc.rank(pct=True)

    # CALM_RANGE: low vol + low range + low move from open
    calm = (rv_pctile <= 0.30) & (range_pctile <= 0.30) & (move_pctile <= 0.40)
    regimes[calm] = "CALM_RANGE"

    # TRENDING: high path efficiency + large recent move
    trending = (eff_pctile >= 0.70) & (move_pctile >= 0.50)
    regimes[trending & ~calm] = "TRENDING"

    # POST_IMPULSE_SETTLING: large move from open + low recent path efficiency
    # (market moved a lot earlier, but right now it's settling)
    settling = (move_pctile >= 0.60) & (eff_pctile <= 0.35) & ~trending
    regimes[settling & ~calm] = "POST_IMPULSE_SETTLING"

    # WAVY_CHOPPY: many sign changes + low path efficiency + moderate chop excess
    wavy = (sign_pctile >= 0.60) & (eff_pctile <= 0.40) & (chop_pctile >= 0.50)
    regimes[wavy & ~calm & ~trending & ~settling] = "WAVY_CHOPPY"

    return regimes


def build_regime_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Performance breakdown by underlying movement regime."""
    if df.empty or "regime" not in df.columns:
        return pd.DataFrame()

    rows = []
    for regime, grp in df.groupby("regime", sort=False):
        pnls = pd.to_numeric(grp["exit_pnl"], errors="coerce").dropna()
        n_days = grp["day"].nunique()
        gp = pnls[pnls > 0].sum()
        gl = -pnls[pnls < 0].sum()
        pf = gp / gl if gl > 0 else (99.99 if gp > 0 else 0.0)
        rows.append({
            "regime": regime,
            "candidates": len(grp),
            "unique_days": n_days,
            "candidates_per_day": round(len(grp) / max(n_days, 1), 1),
            "avg_exit_pnl": round(float(pnls.mean()), 0),
            "median_exit_pnl": round(float(pnls.median()), 0),
            "total_exit_pnl": round(float(pnls.sum()), 0),
            "win_rate_pct": round(float((pnls > 0).mean() * 100), 1),
            "stoploss_rate_pct": round(float(grp["stoploss_hit"].mean() * 100), 1),
            "profit_factor": round(min(pf, 99.99), 2),
            "avg_move_from_open_abs": round(float(pd.to_numeric(grp["move_from_open_abs_pts"], errors="coerce").mean()), 1),
            "avg_path_eff_10m": round(float(pd.to_numeric(grp["path_eff_10m"], errors="coerce").mean()), 3),
            "avg_sign_changes_10m": round(float(pd.to_numeric(grp["sign_changes_10m"], errors="coerce").mean()), 1),
            "avg_rv_10_bps": round(float(pd.to_numeric(grp["rv_10_bps"], errors="coerce").mean()), 1),
            "avg_premium_points": round(float(pd.to_numeric(grp["entry_premium_points"], errors="coerce").mean()), 1),
        })

    result = pd.DataFrame(rows).sort_values("avg_exit_pnl", ascending=False).reset_index(drop=True)
    return result


# =============================================================================
# EXCEL OUTPUT
# =============================================================================
def _autosize_columns_safe(ws) -> None:
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


def write_excel(candidate_df: pd.DataFrame, daily_df: pd.DataFrame, time_df: pd.DataFrame, feature_bins_df: pd.DataFrame, regime_df: pd.DataFrame, overall_df: pd.DataFrame, skipped_df: pd.DataFrame) -> None:
    out_dir = os.path.dirname(os.path.abspath(OUTPUT_XLSX))
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as xw:
        # Cap Excel to 50k rows; full data in Parquet
        cdf_excel = candidate_df.head(50000) if len(candidate_df) > 50000 else candidate_df
        cdf_excel.to_excel(xw, sheet_name="candidate_trades", index=False)
        if len(candidate_df) > 50000:
            print(f"  [NOTE] candidate_trades capped at 50,000 rows (full: {len(candidate_df):,} in Parquet)")
        daily_df.to_excel(xw, sheet_name="daily_summary", index=False)
        time_df.to_excel(xw, sheet_name="time_summary", index=False)
        regime_df.to_excel(xw, sheet_name="regime_summary", index=False)
        feature_bins_df.to_excel(xw, sheet_name="feature_bins", index=False)
        overall_df.to_excel(xw, sheet_name="overall_summary", index=False)
        skipped_df.to_excel(xw, sheet_name="skipped", index=False)

        wb = xw.book
        for ws in wb.worksheets:
            ws.freeze_panes = "A2"
            _autosize_columns_safe(ws)

    print(f"[DONE] Excel written: {OUTPUT_XLSX}")


# =============================================================================
# MAIN
# =============================================================================
def main() -> None:
    paths = sorted(glob.glob(os.path.join(PICKLES_DIR, "*.pkl")) + glob.glob(os.path.join(PICKLES_DIR, "*.pickle")))
    if not paths:
        raise FileNotFoundError(f"No .pkl/.pickle files found in: {PICKLES_DIR}")

    end_day = date.today() if WINDOW_END_MODE == "today" else (discover_data_max_day(paths) or date.today())
    window_start = compute_window_start(end_day, LOOKBACK_MONTHS)

    print(f"[INFO] Pickles found: {len(paths)}")
    print(f"[INFO] WindowEndMode={WINDOW_END_MODE} => end_day={end_day} | Window: {window_start} -> {end_day}")
    print(f"[INFO] Candidate window: {ENTRY_START_IST} -> {ENTRY_END_IST} | step={ENTRY_STEP_MINUTES}m")
    print(f"[INFO] Stoploss={LOSS_LIMIT_PCT:.1%} of premium | ProfitProtect={PROFIT_PROTECT_TRIGGER_PCT:.1%} of premium | MaxSL={MAX_STOPLOSS_RUPEES}")
    print(f"[INFO] Strict strike presence: {STRICT_STRIKE_PRESENCE} (max_missing_streak={MAX_MISSING_STREAK_MIN}m)")
    print(f"[INFO] Dedup within pickle: {DEDUP_WITHIN_PICKLE} | Dedup across pickles: {DEDUP_ACROSS_PICKLES}")
    print(f"[INFO] Output XLSX: {OUTPUT_XLSX}")
    if WRITE_PARQUET:
        print(f"[INFO] Output Parquet: {OUTPUT_PARQUET}")

    candidate_parts: List[pd.DataFrame] = []
    skipped_parts: List[pd.DataFrame] = []
    t_start = time.time()

    for i, p in enumerate(paths):
        try:
            cdf, sdf = process_one_pickle(p, window_start, end_day)
            if cdf is not None and not cdf.empty:
                candidate_parts.append(cdf)
            if sdf is not None and not sdf.empty:
                if "source_pickle" not in sdf.columns:
                    sdf["source_pickle"] = os.path.basename(p)
                skipped_parts.append(sdf)
            elapsed = time.time() - t_start
            done = i + 1
            rate = elapsed / done if done > 0 else 1
            eta = rate * (len(paths) - done) / 60
            total_cands = sum(len(c) for c in candidate_parts)
            print(f"[OK] {os.path.basename(p)}  cands={len(cdf) if cdf is not None else 0}  "
                  f"[{done}/{len(paths)}  {elapsed/60:.1f}m  ETA={eta:.0f}m  total={total_cands:,}]")
        except Exception as e:
            msg = f"[WARN] {os.path.basename(p)} failed: {e}"
            if FAIL_ON_PICKLE_ERROR:
                raise RuntimeError(msg) from e
            print(msg)
            skipped_parts.append(pd.DataFrame([{"source_pickle": os.path.basename(p), "reason": str(e)}]))

    candidate_df = pd.concat(candidate_parts, ignore_index=True) if candidate_parts else pd.DataFrame()
    skipped_df = pd.concat(skipped_parts, ignore_index=True) if skipped_parts else pd.DataFrame()

    if not candidate_df.empty and DEDUP_ACROSS_PICKLES:
        candidate_df, rep = dedup_candidates_across_pickles(candidate_df)
        if not rep.empty:
            skipped_df = pd.concat([skipped_df, rep], ignore_index=True)

    if candidate_df.empty:
        print("[WARN] No completed candidate trades. See skipped output for reasons.")
        write_excel(candidate_df, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), skipped_df)
        return

    candidate_df = candidate_df.sort_values(["day", "underlying", "entry_time", "source_pickle"]).reset_index(drop=True)

    # Classify each candidate minute into an underlying movement regime
    print("[ANALYSIS] Classifying movement regimes ...")
    candidate_df["regime"] = classify_regime(candidate_df)

    daily_df = build_daily_summary(candidate_df)
    time_df = build_time_summary(candidate_df)
    regime_df = build_regime_summary(candidate_df)
    feature_bins_df = summarize_feature_bins(
        candidate_df,
        feature_cols=[
            "move_from_open_abs_pts",
            "range_from_open_pts",
            "move_10m_abs_pts",
            "range_10m_pts",
            "rv_10_bps",
            "path_eff_10m",
            "sign_changes_10m",
            "chop_excess_10m",
            "entry_premium_points",
            "premium_vs_range_10",
            "ce_pe_imbalance_pct",
        ],
        q=BIN_QUANTILES,
    )
    overall_df = build_overall_summary(candidate_df)

    write_excel(candidate_df, daily_df, time_df, feature_bins_df, regime_df, overall_df, skipped_df)

    if WRITE_PARQUET:
        candidate_df.to_parquet(OUTPUT_PARQUET, index=False)
        print(f"[DONE] Parquet written: {OUTPUT_PARQUET}")

    # Print regime summary prominently
    print("\n" + "=" * 90)
    print("  REGIME PERFORMANCE (which underlying movement patterns favor short straddles?)")
    print("=" * 90)
    if not regime_df.empty:
        print(regime_df[["regime", "candidates", "unique_days", "avg_exit_pnl",
                         "win_rate_pct", "stoploss_rate_pct", "profit_factor",
                         "avg_path_eff_10m", "avg_sign_changes_10m"]].to_string(index=False))
    print()

    print("[OVERALL SUMMARY]")
    print(overall_df.to_string(index=False))


if __name__ == "__main__":
    main()
