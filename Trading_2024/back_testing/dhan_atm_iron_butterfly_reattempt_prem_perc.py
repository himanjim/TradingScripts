"""
DHAN EXPIRY-DAY IRON BUTTERFLY / SHORT IRON CONDOR BACKTESTER

What this script does:
1) Loads Dhan rolling-option minute-data pickles produced by DhanExpiredOptionsDataFetcher.py.
2) Processes each pickle independently (no dependency on other pickles for signal generation).
3) Trades ONLY on weekly expiry day (days_to_expiry == 0), and only for NIFTY / SENSEX.
4) Enters a 4-leg option credit structure:
      - Short CE at configurable offset from ATM
      - Short PE at configurable offset from ATM
      - Long CE hedge at configurable offset from ATM
      - Long PE hedge at configurable offset from ATM

   Leg offsets are defined in "number of strike steps away from ATM":
      - For CE side, strike = ATM + offset * step
      - For PE side, strike = ATM - offset * step

   Therefore:
      - Iron butterfly:
          SHORT_CE_OFFSET_STEPS = 0
          SHORT_PE_OFFSET_STEPS = 0
          LONG_CE_OFFSET_STEPS  > 0
          LONG_PE_OFFSET_STEPS  > 0
      - Short iron condor:
          SHORT_CE_OFFSET_STEPS > 0 and/or SHORT_PE_OFFSET_STEPS > 0
          LONG_* offsets farther out than short_* offsets

5) Calculates entry net credit points:
      net_credit_points = (short CE + short PE) - (long CE + long PE)

6) Monitors MTM PnL minute-by-minute and exits:
      - STOPLOSS: when loss reaches configured % of entry net credit points
                  (optionally capped by MAX_STOPLOSS_POINTS)
      - Else at configured EXIT_TIME_IST

7) Uses intraminute stress scenarios for stoploss detection:
      - Close-based PnL
      - "Up move" scenario: CE prices use HIGH, PE prices use LOW
      - "Down move" scenario: CE prices use LOW,  PE prices use HIGH
   The worst (minimum) PnL among these is used for STOPLOSS triggering.

8) Writes an Excel workbook:
      - all_trades_backtested
      - actual_trades
      - monthwise_summary
      - skipped

Notes:
- This script assumes your Dhan pickles contain: close, high, low, strike, leg, target_expiry_date, spot, and dt/timestamp.
- Because the source is a rolling strike-band dataset, your configured offsets must remain inside the fetched band
  (typically ATM ± 10 if that is what your downloader fetched).
"""

import os
import glob
from dataclasses import dataclass
from datetime import datetime, date, time as dtime
from typing import Dict, List, Tuple, Optional, Any

import pandas as pd

# Optional timezone backends (Windows-safe)
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
# Folder with Dhan rolling-option pickles
PICKLES_DIR = os.getenv("DHAN_PICKLES_DIR", r"G:\My Drive\Trading\Dhan_Historical_Options_Data")

# Time window
ENTRY_TIME_IST = os.getenv("ENTRY_TIME_IST", "09:30")  # HH:MM
EXIT_TIME_IST = os.getenv("EXIT_TIME_IST", "15:20")    # HH:MM

# Underlyings supported
QTY_UNITS = {"NIFTY": 325, "SENSEX": 100}
TRADEABLE = set(QTY_UNITS.keys())
STRIKE_STEP = {"NIFTY": 50, "SENSEX": 100}

# Session boundaries (used to build minute grid)
SESSION_START_IST = dtime(9, 15)
SESSION_END_IST = dtime(15, 30)

# -----------------------------------------------------------------------------
# Strategy structure: all offsets are in "strike steps away from ATM"
# -----------------------------------------------------------------------------
# CE side strike = ATM + offset * step
# PE side strike = ATM - offset * step

# Default = classic iron butterfly
SHORT_CE_OFFSET_STEPS = int(os.getenv("SHORT_CE_OFFSET_STEPS", "0"))
SHORT_PE_OFFSET_STEPS = int(os.getenv("SHORT_PE_OFFSET_STEPS", "0"))
LONG_CE_OFFSET_STEPS = int(os.getenv("LONG_CE_OFFSET_STEPS", "10"))
LONG_PE_OFFSET_STEPS = int(os.getenv("LONG_PE_OFFSET_STEPS", "10"))

# -----------------------------------------------------------------------------
# Stoploss
# -----------------------------------------------------------------------------
# Stoploss is a % of entry net credit points receivable.
# Example:
#   If entry net credit = 50 points and STOPLOSS_PCT_OF_NET_CREDIT = 0.50,
#   then stoploss = 25 points loss.
STOPLOSS_PCT_OF_NET_CREDIT = float(os.getenv("STOPLOSS_PCT_OF_NET_CREDIT", "0.50"))

# Optional absolute cap in points (0 => disabled)
# Effective stoploss points = min(entry_credit_points * pct, MAX_STOPLOSS_POINTS)
MAX_STOPLOSS_POINTS = float(os.getenv("MAX_STOPLOSS_POINTS", "10000"))

# -----------------------------------------------------------------------------
# Data window selection
# -----------------------------------------------------------------------------
WINDOW_END_MODE = os.getenv("WINDOW_END_MODE", "data").strip().lower()  # "data" or "today"
LOOKBACK_MONTHS = int(os.getenv("LOOKBACK_MONTHS", "36"))

# -----------------------------------------------------------------------------
# Operational controls
# -----------------------------------------------------------------------------
FAIL_ON_PICKLE_ERROR = os.getenv("FAIL_ON_PICKLE_ERROR", "0").strip() == "1"
DEDUP_WITHIN_PICKLE = os.getenv("DEDUP_WITHIN_PICKLE", "1").strip().lower() not in ("0", "false", "no", "off")
DEDUP_ACROSS_PICKLES = os.getenv("DEDUP_ACROSS_PICKLES", "1").strip().lower() not in ("0", "false", "no", "off")

# Optional: reject a day if any one of the 4 legs disappears too much after entry
STRICT_STRIKE_PRESENCE = os.getenv("STRICT_STRIKE_PRESENCE", "0").strip() == "1"
MAX_MISSING_STREAK_MIN = int(os.getenv("MAX_MISSING_STREAK_MIN", "10"))

# Output
def _safe_fname_part(s: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in s)

_DEFAULT_OUT = (
    rf"C:\Users\Local User\Downloads\dhan_ironfly_condor_backtest_"
    rf"{_safe_fname_part(ENTRY_TIME_IST)}_to_{_safe_fname_part(EXIT_TIME_IST)}_"
    rf"SCE_{SHORT_CE_OFFSET_STEPS}_SPE_{SHORT_PE_OFFSET_STEPS}_"
    rf"LCE_{LONG_CE_OFFSET_STEPS}_LPE_{LONG_PE_OFFSET_STEPS}_"
    rf"SLPCT_{STOPLOSS_PCT_OF_NET_CREDIT}.xlsx"
)
OUTPUT_XLSX = os.getenv("OUTPUT_XLSX", _DEFAULT_OUT)


# =============================================================================
# TIME HELPERS
# =============================================================================
def parse_hhmm(s: str) -> dtime:
    hh, mm = s.strip().split(":")
    return dtime(int(hh), int(mm))

ENTRY_TIME = parse_hhmm(ENTRY_TIME_IST)
EXIT_TIME = parse_hhmm(EXIT_TIME_IST)


def ist_tz():
    if ZoneInfo is not None:
        return ZoneInfo("Asia/Kolkata")
    if pytz is not None:
        return pytz.timezone("Asia/Kolkata")
    return "Asia/Kolkata"


def ensure_ist(x):
    """
    Convert scalar/Series to tz-aware IST timestamps.
    """
    tz = ist_tz()
    dt = pd.to_datetime(x, errors="coerce")

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


def compute_window_start(end_day: date, months: int) -> date:
    if relativedelta is not None:
        return (pd.Timestamp(end_day) - relativedelta(months=months)).date()
    return (pd.Timestamp(end_day) - pd.Timedelta(days=30 * months)).date()


def round_to_step(x: float, step: int) -> int:
    return int(round(x / step) * step)


# =============================================================================
# CONFIG VALIDATION
# =============================================================================
def validate_config() -> None:
    """
    Guardrails so you don't accidentally define an unhedged or inverted structure.
    """
    if STOPLOSS_PCT_OF_NET_CREDIT <= 0:
        raise ValueError("STOPLOSS_PCT_OF_NET_CREDIT must be > 0.")

    # Long hedge must be same-side and not closer than short on each side
    if LONG_CE_OFFSET_STEPS < SHORT_CE_OFFSET_STEPS:
        raise ValueError("LONG_CE_OFFSET_STEPS must be >= SHORT_CE_OFFSET_STEPS.")
    if LONG_PE_OFFSET_STEPS < SHORT_PE_OFFSET_STEPS:
        raise ValueError("LONG_PE_OFFSET_STEPS must be >= SHORT_PE_OFFSET_STEPS.")

    # Entry must be before exit
    if ENTRY_TIME >= EXIT_TIME:
        raise ValueError("ENTRY_TIME_IST must be earlier than EXIT_TIME_IST.")

    # For Dhan rollingoption dataset, typical fetched band is ATM±10
    max_offset = max(SHORT_CE_OFFSET_STEPS, SHORT_PE_OFFSET_STEPS, LONG_CE_OFFSET_STEPS, LONG_PE_OFFSET_STEPS)
    if max_offset > 10:
        print("[WARN] One or more offsets exceed 10 strike-steps. Ensure your Dhan pickles actually contain those strikes.")


# =============================================================================
# OUTPUT STRUCTURE
# =============================================================================
@dataclass
class TradeRow:
    day: date
    underlying: str
    expiry: date
    days_to_expiry: int
    qty_units: int

    entry_time: str
    exit_time: str
    exit_reason: str  # STOPLOSS / TIME_EXIT

    entry_underlying: float
    atm_strike: int

    short_ce_strike: int
    short_pe_strike: int
    long_ce_strike: int
    long_pe_strike: int

    short_ce_entry: float
    short_pe_entry: float
    long_ce_entry: float
    long_pe_entry: float

    net_credit_points: float
    stoploss_points: float

    exit_short_ce: float
    exit_short_pe: float
    exit_long_ce: float
    exit_long_pe: float

    exit_pnl: float
    eod_pnl: float
    max_profit: float
    max_loss: float

    success: int
    source_pickle: str


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
    """
    Normalize the Dhan rolling-option DataFrame into a minimal, stable schema.

    Expected source columns:
      symbol, leg, strike, close, high, low, spot, target_expiry_date, dt/timestamp
    """
    needed = ["symbol", "leg", "strike", "close", "spot", "target_expiry_date"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(f"{source_name}: missing columns {missing}")

    tcol = _pick_time_col(df)
    d = df.copy()

    # Canonical timestamp in IST, floored to minute
    if tcol in ("dt_ist", "timestamp_dt"):
        d["ts"] = ensure_ist(d[tcol])
    else:
        dt_utc = pd.to_datetime(d["timestamp"], unit="s", utc=True, errors="coerce")
        d["ts"] = dt_utc.dt.tz_convert("Asia/Kolkata")

    d["ts"] = d["ts"].dt.floor("min")

    # Canonical day
    if "date_ist" in d.columns:
        d["day"] = pd.to_datetime(d["date_ist"], errors="coerce").dt.date
    else:
        d["day"] = d["ts"].dt.date

    # Underlying filter
    d["underlying"] = d["symbol"].astype(str).str.upper().str.strip()
    d = d[d["underlying"].isin(TRADEABLE)]

    # Expiry date
    d["expiry"] = pd.to_datetime(d["target_expiry_date"], errors="coerce").dt.date

    # Numeric fields
    d["strike_num"] = pd.to_numeric(d["strike"], errors="coerce")
    d["strike_int"] = d["strike_num"].round().astype("Int64")

    d["close_f"] = pd.to_numeric(d["close"], errors="coerce")
    d["high_f"] = pd.to_numeric(d["high"] if "high" in d.columns else d["close"], errors="coerce")
    d["low_f"] = pd.to_numeric(d["low"] if "low" in d.columns else d["close"], errors="coerce")
    d["spot_f"] = pd.to_numeric(d["spot"], errors="coerce")

    # If old pickles have missing high/low, fall back to close
    d["high_f"] = d["high_f"].fillna(d["close_f"])
    d["low_f"] = d["low_f"].fillna(d["close_f"])

    # Canonical leg
    d["leg"] = d["leg"].astype(str).str.upper().str.strip()
    d = d[d["leg"].isin(["CE", "PE"])]

    # Drop unusable rows
    d = d.dropna(subset=["ts", "day", "expiry", "strike_int", "close_f", "high_f", "low_f", "spot_f"])
    d["strike_int"] = d["strike_int"].astype(int)
    d["close_f"] = d["close_f"].astype(float)
    d["high_f"] = d["high_f"].astype(float)
    d["low_f"] = d["low_f"].astype(float)
    d["spot_f"] = d["spot_f"].astype(float)

    # Expiry must not be before trade day
    d = d[d["expiry"] >= d["day"]]

    # Optional dedup within a pickle at the candle level
    if DEDUP_WITHIN_PICKLE and not d.empty:
        d = d.sort_values("ts").drop_duplicates(
            subset=["ts", "underlying", "expiry", "leg", "strike_int"],
            keep="last"
        )

    keep = ["ts", "day", "underlying", "expiry", "leg", "strike_int", "close_f", "high_f", "low_f", "spot_f"]
    return d[keep].copy()


# =============================================================================
# SERIES BUILDERS
# =============================================================================
def _build_underlying_series_from_spot(day_opt: pd.DataFrame, idx_all: pd.DatetimeIndex) -> pd.Series:
    """
    One spot value per minute, forward-filled across the session.
    """
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
    value_col: str = "close_f",
) -> pd.Series:
    """
    Build a 1-minute series for a fixed strike and leg, using the selected value column.
    """
    sub = day_opt[(day_opt["strike_int"] == strike) & (day_opt["leg"] == leg)][["ts", value_col]].dropna()
    if sub.empty:
        return pd.Series(index=idx_all, dtype="float64")
    sub = sub.sort_values("ts").groupby("ts", as_index=True)[value_col].last()
    return sub.reindex(idx_all).ffill()


def _missing_streak_minutes(s: pd.Series) -> int:
    """
    Maximum consecutive NaN streak length in minutes.
    """
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


# =============================================================================
# CORE STRATEGY SIMULATION
# =============================================================================
def simulate_one_day_iron_structure(
    *,
    und: str,
    dy: date,
    expiry: date,
    day_opt: pd.DataFrame,
    source_pickle: str,
) -> Tuple[List[TradeRow], List[Dict[str, Any]]]:
    """
    Simulate ONE expiry-day iron butterfly / iron condor trade.

    Rules:
    - Enter once at ENTRY_TIME_IST
    - Exit on STOPLOSS or EXIT_TIME_IST
    - No re-entry in this strategy version
    """
    results: List[TradeRow] = []
    skipped: List[Dict[str, Any]] = []

    idx_all = build_minute_index(dy, SESSION_START_IST, SESSION_END_IST)
    qty = int(QTY_UNITS[und])
    step = int(STRIKE_STEP[und])

    entry_ts = pd.Timestamp(datetime.combine(dy, ENTRY_TIME), tz=ist_tz())
    target_exit_ts = pd.Timestamp(datetime.combine(dy, EXIT_TIME), tz=ist_tz())

    if entry_ts not in idx_all or target_exit_ts not in idx_all:
        skipped.append({
            "day": dy,
            "underlying": und,
            "expiry": expiry,
            "source_pickle": source_pickle,
            "reason": "Entry or exit timestamp not in session minute grid"
        })
        return results, skipped

    # Build spot series first so ATM can be chosen at entry
    spot_s = _build_underlying_series_from_spot(day_opt, idx_all)
    u_px = float(spot_s.loc[entry_ts]) if pd.notna(spot_s.loc[entry_ts]) else float("nan")
    if pd.isna(u_px):
        skipped.append({
            "day": dy,
            "underlying": und,
            "expiry": expiry,
            "source_pickle": source_pickle,
            "reason": f"No underlying spot at entry {entry_ts.strftime('%H:%M')}"
        })
        return results, skipped

    atm = round_to_step(float(u_px), step)

    # Calculate all 4 leg strikes from ATM
    short_ce_strike = atm + SHORT_CE_OFFSET_STEPS * step
    short_pe_strike = atm - SHORT_PE_OFFSET_STEPS * step
    long_ce_strike = atm + LONG_CE_OFFSET_STEPS * step
    long_pe_strike = atm - LONG_PE_OFFSET_STEPS * step

    # Build minute series for all 4 legs
    # Close series (entry, reporting, close-based PnL)
    sce_close = _build_leg_series_fixed_strike(day_opt, idx_all, short_ce_strike, "CE", "close_f")
    spe_close = _build_leg_series_fixed_strike(day_opt, idx_all, short_pe_strike, "PE", "close_f")
    lce_close = _build_leg_series_fixed_strike(day_opt, idx_all, long_ce_strike, "CE", "close_f")
    lpe_close = _build_leg_series_fixed_strike(day_opt, idx_all, long_pe_strike, "PE", "close_f")

    # High/Low series (used only for stoploss stress testing)
    sce_high = _build_leg_series_fixed_strike(day_opt, idx_all, short_ce_strike, "CE", "high_f")
    sce_low = _build_leg_series_fixed_strike(day_opt, idx_all, short_ce_strike, "CE", "low_f")
    spe_high = _build_leg_series_fixed_strike(day_opt, idx_all, short_pe_strike, "PE", "high_f")
    spe_low = _build_leg_series_fixed_strike(day_opt, idx_all, short_pe_strike, "PE", "low_f")

    lce_high = _build_leg_series_fixed_strike(day_opt, idx_all, long_ce_strike, "CE", "high_f")
    lce_low = _build_leg_series_fixed_strike(day_opt, idx_all, long_ce_strike, "CE", "low_f")
    lpe_high = _build_leg_series_fixed_strike(day_opt, idx_all, long_pe_strike, "PE", "high_f")
    lpe_low = _build_leg_series_fixed_strike(day_opt, idx_all, long_pe_strike, "PE", "low_f")

    # Entry prices
    short_ce_entry = sce_close.loc[entry_ts]
    short_pe_entry = spe_close.loc[entry_ts]
    long_ce_entry = lce_close.loc[entry_ts]
    long_pe_entry = lpe_close.loc[entry_ts]

    if any(pd.isna(x) for x in [short_ce_entry, short_pe_entry, long_ce_entry, long_pe_entry]):
        skipped.append({
            "day": dy,
            "underlying": und,
            "expiry": expiry,
            "source_pickle": source_pickle,
            "atm_strike": atm,
            "reason": "One or more leg prices missing at entry"
        })
        return results, skipped

    # Optional strictness: reject the day if any leg disappears too much after entry
    if STRICT_STRIKE_PRESENCE:
        series_to_check = [
            sce_close.loc[entry_ts:],
            spe_close.loc[entry_ts:],
            lce_close.loc[entry_ts:],
            lpe_close.loc[entry_ts:],
        ]
        max_miss = max(_missing_streak_minutes(s) for s in series_to_check)
        if max_miss > MAX_MISSING_STREAK_MIN:
            skipped.append({
                "day": dy,
                "underlying": und,
                "expiry": expiry,
                "source_pickle": source_pickle,
                "atm_strike": atm,
                "reason": f"One or more leg series missing too much after entry (max_missing_streak={max_miss}m)"
            })
            return results, skipped

    # Entry net credit in points
    net_credit_points = (
        float(short_ce_entry) + float(short_pe_entry)
        - float(long_ce_entry) - float(long_pe_entry)
    )

    if net_credit_points <= 0:
        skipped.append({
            "day": dy,
            "underlying": und,
            "expiry": expiry,
            "source_pickle": source_pickle,
            "atm_strike": atm,
            "reason": f"Non-positive entry net credit ({net_credit_points:.4f} points)"
        })
        return results, skipped

    # Stoploss in points
    sl_points_pct = net_credit_points * STOPLOSS_PCT_OF_NET_CREDIT
    stoploss_points = sl_points_pct
    if MAX_STOPLOSS_POINTS > 0:
        stoploss_points = min(stoploss_points, MAX_STOPLOSS_POINTS)

    stoploss_rupees = stoploss_points * qty

    # -------------------------------------------------------------------------
    # PnL calculation
    # -------------------------------------------------------------------------
    # Close-based close-out cost:
    #   buy back shorts - sell longs
    close_cost_points = (
        sce_close + spe_close
        - lce_close - lpe_close
    )
    pnl_close_all = (net_credit_points - close_cost_points) * qty

    # Directional stress scenario A (up-move):
    #   CE uses HIGH, PE uses LOW
    #   Same mapping is applied to both short and long legs on that side
    close_cost_up_points = (
        sce_high + spe_low
        - lce_high - lpe_low
    )
    pnl_up_all = (net_credit_points - close_cost_up_points) * qty

    # Directional stress scenario B (down-move):
    #   CE uses LOW, PE uses HIGH
    close_cost_down_points = (
        sce_low + spe_high
        - lce_low - lpe_high
    )
    pnl_down_all = (net_credit_points - close_cost_down_points) * qty

    # Worst-case PnL per minute for STOPLOSS evaluation
    pnl_sl_all = pd.concat([pnl_close_all, pnl_up_all, pnl_down_all], axis=1).min(axis=1)

    # Limit evaluation window to entry -> exit
    pnl_close = pnl_close_all.loc[entry_ts:target_exit_ts].dropna()
    pnl_sl = pnl_sl_all.loc[entry_ts:target_exit_ts].dropna()

    if pnl_close.empty or pnl_sl.empty:
        skipped.append({
            "day": dy,
            "underlying": und,
            "expiry": expiry,
            "source_pickle": source_pickle,
            "reason": "PnL series empty after entry"
        })
        return results, skipped

    # End-of-day (configured time) metrics
    eod_ts = pnl_close.index[-1]
    eod_pnl = float(pnl_close.iloc[-1])
    max_profit = float(max(0.0, pnl_close.max()))
    max_loss = float(min(0.0, pnl_close.min()))

    # Stoploss trigger: first time worst-case loss reaches threshold
    stop_hit = pnl_sl <= -stoploss_rupees
    stop_ts = pnl_sl.index[stop_hit.to_numpy().argmax()] if stop_hit.any() else None

    # Final exit decision
    exit_ts = eod_ts
    exit_reason = "TIME_EXIT"
    if stop_ts is not None:
        exit_ts = stop_ts
        exit_reason = "STOPLOSS"

    # Reporting uses close-based exit PnL at the chosen timestamp
    exit_pnl = float(pnl_close.loc[exit_ts])

    # Cap STOPLOSS exit PnL to the configured stoploss approximation
    # (This makes the report behave like "stop monitored tighter than minute bars".)
    if exit_reason == "STOPLOSS" and exit_pnl < -stoploss_rupees:
        exit_pnl = -float(stoploss_rupees)

    exit_short_ce = float(sce_close.loc[exit_ts]) if pd.notna(sce_close.loc[exit_ts]) else float("nan")
    exit_short_pe = float(spe_close.loc[exit_ts]) if pd.notna(spe_close.loc[exit_ts]) else float("nan")
    exit_long_ce = float(lce_close.loc[exit_ts]) if pd.notna(lce_close.loc[exit_ts]) else float("nan")
    exit_long_pe = float(lpe_close.loc[exit_ts]) if pd.notna(lpe_close.loc[exit_ts]) else float("nan")

    dte = int((expiry - dy).days)

    results.append(
        TradeRow(
            day=dy,
            underlying=und,
            expiry=expiry,
            days_to_expiry=dte,
            qty_units=qty,

            entry_time=pd.Timestamp(entry_ts).strftime("%H:%M"),
            exit_time=pd.Timestamp(exit_ts).strftime("%H:%M"),
            exit_reason=exit_reason,

            entry_underlying=float(u_px),
            atm_strike=int(atm),

            short_ce_strike=int(short_ce_strike),
            short_pe_strike=int(short_pe_strike),
            long_ce_strike=int(long_ce_strike),
            long_pe_strike=int(long_pe_strike),

            short_ce_entry=float(short_ce_entry),
            short_pe_entry=float(short_pe_entry),
            long_ce_entry=float(long_ce_entry),
            long_pe_entry=float(long_pe_entry),

            net_credit_points=float(net_credit_points),
            stoploss_points=float(stoploss_points),

            exit_short_ce=exit_short_ce,
            exit_short_pe=exit_short_pe,
            exit_long_ce=exit_long_ce,
            exit_long_pe=exit_long_pe,

            exit_pnl=float(exit_pnl),
            eod_pnl=float(eod_pnl),
            max_profit=float(max_profit),
            max_loss=float(max_loss),

            success=int(exit_pnl > 0),
            source_pickle=source_pickle,
        )
    )

    return results, skipped


# =============================================================================
# PER-PICKLE PROCESSOR
# =============================================================================
def process_one_pickle(p: str, window_start: date, window_end: date) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Process one pickle independently.

    Logic:
    - Normalize
    - Filter to requested date window
    - For each (underlying, day), select nearest expiry inside THIS file
    - Trade ONLY if the selected expiry is the same as the day (DTE=0)
    """
    src = os.path.basename(p)

    raw = pd.read_pickle(p)
    if not isinstance(raw, pd.DataFrame) or raw.empty:
        return pd.DataFrame(), pd.DataFrame([{"source_pickle": src, "reason": "Empty or non-DataFrame pickle"}])

    d = _normalize_dhan_df(raw, src)
    if d.empty:
        return pd.DataFrame(), pd.DataFrame([{"source_pickle": src, "reason": "No usable rows after normalization"}])

    d = d[(d["day"] >= window_start) & (d["day"] <= window_end)]
    if d.empty:
        return pd.DataFrame(), pd.DataFrame([{"source_pickle": src, "reason": "No rows in requested date window"}])

    # Nearest expiry per (underlying, day) within this file
    min_expiry_local: Dict[Tuple[str, date], date] = (
        d.groupby(["underlying", "day"], sort=False)["expiry"].min().to_dict()
    )

    trades_out: List[Dict[str, Any]] = []
    skipped_out: List[Dict[str, Any]] = []

    for (und, dy, ex), g in d.groupby(["underlying", "day", "expiry"], sort=False):
        # Only the nearest expiry for that day
        if min_expiry_local.get((und, dy)) != ex:
            continue

        # Only trade expiry day (weekly expiry day)
        if ex != dy:
            continue

        trades, skips = simulate_one_day_iron_structure(
            und=und,
            dy=dy,
            expiry=ex,
            day_opt=g,
            source_pickle=src,
        )
        trades_out.extend([t.__dict__ for t in trades])
        skipped_out.extend(skips)

    return pd.DataFrame(trades_out), pd.DataFrame(skipped_out)


# =============================================================================
# DEDUP ACROSS PICKLES
# =============================================================================
def dedup_trades_across_pickles(trades: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Remove duplicate trade rows that can arise from overlapping pickle coverage.
    """
    if trades.empty:
        return trades, pd.DataFrame()

    key_cols = [
        "underlying", "day", "expiry", "entry_time",
        "short_ce_strike", "short_pe_strike", "long_ce_strike", "long_pe_strike"
    ]
    before = len(trades)

    trades_sorted = trades.sort_values(key_cols + ["source_pickle"]).reset_index(drop=True)
    deduped = trades_sorted.drop_duplicates(subset=key_cols, keep="first").reset_index(drop=True)

    removed = before - len(deduped)
    report = pd.DataFrame()
    if removed > 0:
        report = pd.DataFrame([{
            "source_pickle": "__aggregate__",
            "reason": f"Dedup across pickles removed {removed} duplicate trade rows (key={key_cols})"
        }])

    return deduped, report


# =============================================================================
# ACTUAL TRADES
# =============================================================================
def build_actual_trades_df(all_trades_df: pd.DataFrame) -> pd.DataFrame:
    """
    Choose one underlying per day:
      - earliest expiry wins
      - tie-break: NIFTY preferred over SENSEX
    """
    if all_trades_df.empty:
        return pd.DataFrame()

    tmp = (
        all_trades_df.groupby(["day", "underlying"], as_index=False)["expiry"].min()
        .sort_values(["day", "expiry", "underlying"])
    )

    chosen: Dict[date, str] = {}
    for dy, sub in tmp.groupby("day", sort=False):
        sub = sub.sort_values(["expiry", "underlying"])
        min_ex = sub["expiry"].iloc[0]
        sub_min = sub[sub["expiry"] == min_ex]
        chosen[dy] = "NIFTY" if "NIFTY" in set(sub_min["underlying"].tolist()) else sub_min["underlying"].iloc[0]

    out = all_trades_df.copy()
    out["chosen_underlying"] = out["day"].map(chosen)
    out = out[out["underlying"] == out["chosen_underlying"]].drop(columns=["chosen_underlying"])
    out = out.sort_values(["day", "source_pickle"]).reset_index(drop=True)

    # This strategy is expiry-day only by design, but keep the filter explicit.
    dte = pd.to_numeric(out.get("days_to_expiry"), errors="coerce")
    out = out[dte == 0].copy()

    return out


# =============================================================================
# SUMMARIES
# =============================================================================
def _max_streak_days(pnls: List[float], mode: str) -> int:
    """
    Compute max consecutive streak length in days.
    mode:
      - "profit": pnl > 0
      - "loss": pnl < 0
    """
    if mode not in ("profit", "loss"):
        raise ValueError("mode must be 'profit' or 'loss'")
    best = 0
    cur = 0
    for v in pnls:
        ok = (v > 0) if mode == "profit" else (v < 0)
        if ok:
            cur += 1
            best = max(best, cur)
        else:
            cur = 0
    return best


def build_monthly_summary(actual_trades_df: pd.DataFrame) -> pd.DataFrame:
    """
    Monthly summary based on actual_trades.
    """
    if actual_trades_df.empty:
        return pd.DataFrame()

    daily = (
        actual_trades_df.groupby(["day"], as_index=False)
        .agg(
            underlying=("underlying", "first"),
            expiry=("expiry", "min"),
            net_exit_pnl=("exit_pnl", "sum"),
            trades=("exit_pnl", "count"),
            avg_credit_points=("net_credit_points", "mean"),
            avg_stoploss_points=("stoploss_points", "mean"),
        )
        .sort_values("day")
        .reset_index(drop=True)
    )

    daily["month"] = pd.to_datetime(daily["day"]).dt.to_period("M").astype(str)

    rows = []
    for m, sub in daily.groupby("month", sort=False):
        pnls = sub["net_exit_pnl"].astype(float).tolist()
        rows.append({
            "month": m,
            "trading_days": int(len(sub)),
            "total_pnl": float(sub["net_exit_pnl"].sum()),
            "avg_pnl_per_day": float(sub["net_exit_pnl"].mean()),
            "win_days": int((sub["net_exit_pnl"] > 0).sum()),
            "loss_days": int((sub["net_exit_pnl"] < 0).sum()),
            "win_rate_pct": float(100.0 * (sub["net_exit_pnl"] > 0).mean()),
            "best_day_pnl": float(sub["net_exit_pnl"].max()),
            "worst_day_pnl": float(sub["net_exit_pnl"].min()),
            "max_profit_streak_days": int(_max_streak_days(pnls, "profit")),
            "max_loss_streak_days": int(_max_streak_days(pnls, "loss")),
            "avg_credit_points": float(sub["avg_credit_points"].mean()),
            "avg_stoploss_points": float(sub["avg_stoploss_points"].mean()),
        })

    monthwise = pd.DataFrame(rows).sort_values("month").reset_index(drop=True)

    all_pnls = daily["net_exit_pnl"].astype(float).tolist()
    overall = pd.DataFrame([{
        "month": "__overall__",
        "trading_days": int(len(daily)),
        "total_pnl": float(daily["net_exit_pnl"].sum()),
        "avg_pnl_per_day": float(daily["net_exit_pnl"].mean()),
        "win_days": int((daily["net_exit_pnl"] > 0).sum()),
        "loss_days": int((daily["net_exit_pnl"] < 0).sum()),
        "win_rate_pct": float(100.0 * (daily["net_exit_pnl"] > 0).mean()),
        "best_day_pnl": float(daily["net_exit_pnl"].max()),
        "worst_day_pnl": float(daily["net_exit_pnl"].min()),
        "max_profit_streak_days": int(_max_streak_days(all_pnls, "profit")),
        "max_loss_streak_days": int(_max_streak_days(all_pnls, "loss")),
        "avg_credit_points": float(daily["avg_credit_points"].mean()),
        "avg_stoploss_points": float(daily["avg_stoploss_points"].mean()),
    }])

    return pd.concat([monthwise, overall], ignore_index=True)


# =============================================================================
# EXCEL OUTPUT
# =============================================================================
def _autosize_columns_safe(ws) -> None:
    """
    Best-effort column autosizing, capped for performance.
    """
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
    monthwise_df: pd.DataFrame,
    skipped_df: pd.DataFrame,
) -> None:
    """
    Write the output workbook.
    """
    out_dir = os.path.dirname(os.path.abspath(OUTPUT_XLSX))
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as xw:
        all_trades_df.to_excel(xw, sheet_name="all_trades_backtested", index=False)
        actual_trades_df.to_excel(xw, sheet_name="actual_trades", index=False)
        monthwise_df.to_excel(xw, sheet_name="monthwise_summary", index=False)
        skipped_df.to_excel(xw, sheet_name="skipped", index=False)

        wb = xw.book
        for ws in wb.worksheets:
            ws.freeze_panes = "A2"
            _autosize_columns_safe(ws)

    print(f"[DONE] Excel written: {OUTPUT_XLSX}")


# =============================================================================
# WINDOW END DISCOVERY
# =============================================================================
def discover_data_max_day(pickle_paths: List[str]) -> Optional[date]:
    """
    Find the latest trade day present in the dataset.
    """
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


# =============================================================================
# MAIN
# =============================================================================
def main():
    validate_config()

    paths = sorted(
        glob.glob(os.path.join(PICKLES_DIR, "*.pkl")) +
        glob.glob(os.path.join(PICKLES_DIR, "*.pickle"))
    )
    if not paths:
        raise FileNotFoundError(f"No .pkl/.pickle files found in: {PICKLES_DIR}")

    # Choose end_day
    if WINDOW_END_MODE == "today":
        end_day = date.today()
    else:
        max_day = discover_data_max_day(paths)
        end_day = max_day if max_day is not None else date.today()

    window_start = compute_window_start(end_day, LOOKBACK_MONTHS)

    print(f"[INFO] Pickles found: {len(paths)}")
    print(f"[INFO] WindowEndMode={WINDOW_END_MODE} => end_day={end_day} | Window: {window_start} -> {end_day}")
    print(f"[INFO] Entry={ENTRY_TIME_IST} | Exit={EXIT_TIME_IST}")
    print(
        "[INFO] Structure offsets (in strike-steps from ATM): "
        f"SHORT_CE={SHORT_CE_OFFSET_STEPS}, SHORT_PE={SHORT_PE_OFFSET_STEPS}, "
        f"LONG_CE={LONG_CE_OFFSET_STEPS}, LONG_PE={LONG_PE_OFFSET_STEPS}"
    )
    print(
        "[INFO] Stoploss: "
        f"{STOPLOSS_PCT_OF_NET_CREDIT:.1%} of entry net credit points"
        + (f" (capped at {MAX_STOPLOSS_POINTS} points)" if MAX_STOPLOSS_POINTS > 0 else "")
    )
    print(f"[INFO] Strict strike presence: {STRICT_STRIKE_PRESENCE} (max_missing_streak={MAX_MISSING_STREAK_MIN}m)")
    print(f"[INFO] Dedup within pickle: {DEDUP_WITHIN_PICKLE} | Dedup across pickles: {DEDUP_ACROSS_PICKLES}")
    print(f"[INFO] Output: {OUTPUT_XLSX}")

    all_trades_list: List[pd.DataFrame] = []
    skipped_list: List[pd.DataFrame] = []

    for p in paths:
        try:
            tdf, sdf = process_one_pickle(p, window_start, end_day)

            if tdf is not None and not tdf.empty:
                all_trades_list.append(tdf)

            if sdf is not None and not sdf.empty:
                if "source_pickle" not in sdf.columns:
                    sdf["source_pickle"] = os.path.basename(p)
                skipped_list.append(sdf)

            print(
                f"[OK] processed {os.path.basename(p)} "
                f"trades={len(tdf) if tdf is not None else 0} "
                f"skipped={len(sdf) if sdf is not None else 0}"
            )

        except Exception as e:
            msg = f"[WARN] {os.path.basename(p)} failed: {e}"
            if FAIL_ON_PICKLE_ERROR:
                raise RuntimeError(msg) from e
            print(msg)
            skipped_list.append(pd.DataFrame([{
                "source_pickle": os.path.basename(p),
                "reason": str(e)
            }]))

    all_trades_df = pd.concat(all_trades_list, ignore_index=True) if all_trades_list else pd.DataFrame()
    skipped_df = pd.concat(skipped_list, ignore_index=True) if skipped_list else pd.DataFrame()

    # Optional dedup across pickles
    if not all_trades_df.empty and DEDUP_ACROSS_PICKLES:
        all_trades_df, dedup_report = dedup_trades_across_pickles(all_trades_df)
        if not dedup_report.empty:
            skipped_df = pd.concat([skipped_df, dedup_report], ignore_index=True)

    if not all_trades_df.empty:
        all_trades_df = all_trades_df.sort_values(["day", "underlying", "source_pickle"]).reset_index(drop=True)

    # One underlying per day
    actual_trades_df = build_actual_trades_df(all_trades_df)

    # Monthwise summary
    monthwise_df = build_monthly_summary(actual_trades_df)

    # Write Excel
    write_excel(all_trades_df, actual_trades_df, monthwise_df, skipped_df)

    if all_trades_df.empty:
        print("[WARN] No completed trades. See 'skipped' sheet.")
    else:
        print(all_trades_df.groupby("underlying")[["exit_pnl"]].describe())


if __name__ == "__main__":
    main()