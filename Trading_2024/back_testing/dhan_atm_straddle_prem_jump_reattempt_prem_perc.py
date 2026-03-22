"""
DHAN ROLLING-OPTIONS SHORT-STRADDLE BACKTESTER (re-attempt + profit-protect)

What this script does (end-to-end):
1) Loads Dhan “rollingoption” minute-data pickles produced by DhanExpiredOptionsDataFetcher.py.
   - Each pickle contains minute candles for CE/PE across a rolling strike-band (ATM, ATM±1..±10) and includes
     the underlying spot ("spot") at each minute.
2) For each pickle INDEPENDENTLY (i.e., no requirement that other pickles exist):
   - Normalizes columns into a stable schema: ts (IST, minute), day, underlying, expiry, leg, strike, close, spot.
   - For each (underlying, trading day), determines the nearest expiry available within that pickle.
   - Simulates the same short-straddle logic as your Zerodha backtester:
       • Enter at ENTRY_TIME_IST at the ATM strike computed from underlying spot at entry.
       • Mark-to-market PnL minute-by-minute using CE/PE prices of that fixed numeric strike.
       • Exit rules:
           - STOPLOSS: pnl <= -LOSS_LIMIT_RUPEES
           - PROFIT_PROTECT: once peak pnl >= G, exit when pnl <= (peak - G)
           - Else exit at EOD
       • Re-entry: after STOPLOSS/PROFIT_PROTECT, allow MAX_REATTEMPTS reattempt(s) after REENTRY_DELAY_MINUTES.
3) Aggregates results across all pickles and writes an Excel workbook with:
   - all_trades_backtested (all simulated trades, including re-entries, with source_pickle for traceability)
   - actual_trades (one underlying per day: earliest expiry wins; tie -> NIFTY)
   - daily_pnl_actual (daily net P/L based on actual_trades)
   - monthwise_summary (month-level P/L + max profit/loss streaks in days + overall row)
   - pivots and summaries similar to your existing backtester
   - skipped (reasons for missing trades/data + dedup report)
"""

import os
import glob
from dataclasses import dataclass
from datetime import datetime, date, time as dtime
from typing import Dict, List, Tuple, Optional, Any
from pathlib import Path
import pandas as pd

# Optional timezone backends (works on Windows too)
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
# CONFIG (aligns with your Zerodha backtester semantics)
# =============================================================================
PICKLES_DIR = os.getenv("DHAN_PICKLES_DIR", r"G:\My Drive\Trading\Dhan_Historical_Options_Data")

ENTRY_TIME_IST = os.getenv("ENTRY_TIME_IST", "09:45")  # HH:MM
# Risk is a % of the entry premium (CE+PE) in rupees.
# Defaults chosen to roughly match old fixed values:
#   If premium_sum_rupees ~ 50,000 => SL 10% ≈ 5,000
#   If premium_sum_rupees ~ 55,000 => PP 18% ≈ 9,900 ~ 10,000
LOSS_LIMIT_PCT = float(os.getenv("LOSS_LIMIT_PCT", "0.20"))                  # 20% of premium sum (rupees)
PROFIT_PROTECT_TRIGGER_PCT = float(os.getenv("PROFIT_PROTECT_TRIGGER_PCT", "0.3"))  # 18% of premium sum (rupees)
MAX_STOPLOSS_RUPEES = abs(float(os.getenv("MAX_STOPLOSS_RUPEES", "3000")))

# Profit-protect configuration
# Mode:
#   - "off"       : disable profit-protect completely
#   - "pct_trail" : legacy mode (arm at G, trail by G)
#   - "lock_curve": new flexible lock-in profile based on peak profit
PROFIT_PROTECT_MODE = os.getenv("PROFIT_PROTECT_MODE", "lock_curve").strip().lower()

# Cap for the profit-protect "G" computed from PROFIT_PROTECT_TRIGGER_PCT.
# 0 => no cap.
MAX_PROFIT_PROTECT_RUPEES = abs(float(os.getenv("MAX_PROFIT_PROTECT_RUPEES", "10000")))

# Flexible profit lock curve (all values in rupees)
PROFIT_LOCK_ENABLED = os.getenv("PROFIT_LOCK_ENABLED", "1").strip() not in ("0", "false", "False")

PROFIT_LOCK_P1_PROFIT = float(os.getenv("PROFIT_LOCK_P1_PROFIT", "5000"))
PROFIT_LOCK_P1_LOCK   = float(os.getenv("PROFIT_LOCK_P1_LOCK", "1000"))

PROFIT_LOCK_P2_PROFIT = float(os.getenv("PROFIT_LOCK_P2_PROFIT", "10000"))
PROFIT_LOCK_P2_LOCK   = float(os.getenv("PROFIT_LOCK_P2_LOCK", "3000"))

PROFIT_LOCK_P3_PROFIT = float(os.getenv("PROFIT_LOCK_P3_PROFIT", "15000"))
# After P3, maximum profit giveback allowed is this amount:
PROFIT_LOCK_MAX_GIVEBACK_RUPEES = float(os.getenv("PROFIT_LOCK_MAX_GIVEBACK_RUPEES", "10000"))

MAX_REATTEMPTS = int(os.getenv("MAX_REATTEMPTS", "3"))  # "1" => allow one re-entry
REENTRY_DELAY_MINUTES = int(os.getenv("REENTRY_DELAY_MINUTES", "15"))

# Window selection:
# - "data": end_day = max day present in pickles (recommended; avoids empty results when data is old)
# - "today": end_day = date.today()
WINDOW_END_MODE = os.getenv("WINDOW_END_MODE", "data").strip().lower()
LOOKBACK_MONTHS = int(os.getenv("LOOKBACK_MONTHS", "36"))

# Contract quantities and ATM rounding steps
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
IPFT_PER_CRORE            = 0.010       # ₹0.01 per crore (on premium)
GST_PCT                   = 0.18       # 18% on (brokerage + txn charges + SEBI)
INCLUDE_TRANSACTION_COSTS = True       # set False to disable

# Session boundaries (IST)
SESSION_START_IST = dtime(9, 15)
SESSION_END_IST = dtime(15, 30)

# Operational controls
FAIL_ON_PICKLE_ERROR = os.getenv("FAIL_ON_PICKLE_ERROR", "0").strip() == "1"

# Optional strictness for rolling-band datasets (reject days where fixed strike disappears too much)
STRICT_STRIKE_PRESENCE = os.getenv("STRICT_STRIKE_PRESENCE", "0").strip() == "1"
MAX_MISSING_STREAK_MIN = int(os.getenv("MAX_MISSING_STREAK_MIN", "10"))

# Dedup controls:
DEDUP_WITHIN_PICKLE = os.getenv("DEDUP_WITHIN_PICKLE", "1").strip() not in ("0", "false", "False")
DEDUP_ACROSS_PICKLES = os.getenv("DEDUP_ACROSS_PICKLES", "1").strip() not in ("0", "false", "False")

# Output Excel
def _safe_fname_part(s: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in s)

_DEFAULT_OUT = rf"{Path.home()}\Downloads\dhan_short_straddle_backtest_reattempt_{_safe_fname_part(ENTRY_TIME_IST)}_LL_{LOSS_LIMIT_PCT}_PPT_{PROFIT_PROTECT_TRIGGER_PCT}_RDM_{REENTRY_DELAY_MINUTES}_MSR_{MAX_STOPLOSS_RUPEES}_MR_{MAX_REATTEMPTS}.xlsx"
OUTPUT_XLSX = os.getenv("OUTPUT_XLSX", _DEFAULT_OUT)


# =============================================================================
# TIME HELPERS
# =============================================================================
def parse_hhmm(s: str) -> dtime:
    """Parse 'HH:MM' into datetime.time."""
    hh, mm = s.strip().split(":")
    return dtime(int(hh), int(mm))

ENTRY_TIME = parse_hhmm(ENTRY_TIME_IST)

def ist_tz():
    """Return an IST tzinfo implementation available on this Python."""
    if ZoneInfo is not None:
        return ZoneInfo("Asia/Kolkata")
    if pytz is not None:
        return pytz.timezone("Asia/Kolkata")
    return "Asia/Kolkata"  # pandas can often handle string tz names

def ensure_ist(x):
    """
    Convert scalar/Series to tz-aware IST timestamps.
    - If naive, localize to IST.
    - If tz-aware, convert to IST.
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
    """Build a 1-minute IST grid for the trading session."""
    tz = ist_tz()
    start = pd.Timestamp(datetime.combine(day_d, start_t), tz=tz)
    end = pd.Timestamp(datetime.combine(day_d, end_t), tz=tz)
    return pd.date_range(start=start, end=end, freq="1min")

def compute_window_start(end_day: date, months: int) -> date:
    """Compute window start date as end_day - months."""
    if relativedelta is not None:
        return (pd.Timestamp(end_day) - relativedelta(months=months)).date()
    return (pd.Timestamp(end_day) - pd.Timedelta(days=30 * months)).date()

def round_to_step(x: float, step: int) -> int:
    """Round to nearest strike step (e.g., 50 for NIFTY, 100 for SENSEX)."""
    return int(round(x / step) * step)

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
    exit_turnover  = (exit_ce + exit_pe) * qty      # buy side
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
# OUTPUT STRUCTURE
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
    exit_reason: str  # STOPLOSS / PROFIT_PROTECT / EOD

    entry_underlying: float
    ce_symbol: str
    pe_symbol: str
    entry_ce: float
    entry_pe: float
    exit_ce: float
    exit_pe: float

    exit_pnl_gross: float  # P&L before charges
    txn_charges: float  # total transaction charges for this attempt
    exit_pnl: float  # net P&L after deducting charges
    eod_pnl: float
    max_profit: float
    max_loss: float

    source_pickle: str


# =============================================================================
# DHAN PICKLE NORMALIZATION
# =============================================================================
def _pick_time_col(df: pd.DataFrame) -> str:
    """
    Prefer tz-aware datetime columns if present.
    Downloader typically provides dt_ist and/or timestamp_dt.
    """
    if "dt_ist" in df.columns:
        return "dt_ist"
    if "timestamp_dt" in df.columns:
        return "timestamp_dt"
    if "timestamp" in df.columns:
        return "timestamp"
    raise ValueError("No usable time column found (expected dt_ist / timestamp_dt / timestamp).")

def _normalize_dhan_df(df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    """
    Normalize Dhan RollingOption pickle into minimal schema required for backtest.

    Developer notes:
    - RollingOption is a rolling strike band; we reconstruct a FIXED strike series by filtering strike==ATM_entry_strike.
    - Duplicate rows can appear (same strike can come via multiple strikeSelector snapshots).
      DEDUP_WITHIN_PICKLE drops duplicates for the same (ts, underlying, expiry, leg, strike_int).
    """
    needed = ["symbol", "leg", "strike", "close", "spot", "target_expiry_date"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(f"{source_name}: missing columns {missing}")

    tcol = _pick_time_col(df)
    d = df.copy()

    # Canonical timestamp (IST) and force onto minute grid
    if tcol in ("dt_ist", "timestamp_dt"):
        d["ts"] = ensure_ist(d[tcol])
    else:
        dt_utc = pd.to_datetime(d["timestamp"], unit="s", utc=True, errors="coerce")
        d["ts"] = dt_utc.dt.tz_convert("Asia/Kolkata")

    # Critical: avoid off-minute timestamps breaking entry alignment
    d["ts"] = d["ts"].dt.floor("min")

    # Canonical day
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
    # If some old pickles lack high/low, fall back to close
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
    d["spot_f"] = d["spot_f"].astype(float)

    # Sanity: expiry must not be before trading day
    d = d[d["expiry"] >= d["day"]]

    if DEDUP_WITHIN_PICKLE and not d.empty:
        # Keep last observation for duplicate keys
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
    """Collapse spot_f to one value per minute and forward-fill across the session."""
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
    sub = day_opt[(day_opt["strike_int"] == strike) & (day_opt["leg"] == leg)][["ts", value_col]].dropna()
    if sub.empty:
        return pd.Series(index=idx_all, dtype="float64")
    sub = sub.sort_values("ts").groupby("ts", as_index=True)[value_col].last()
    return sub.reindex(idx_all).ffill()


def _missing_streak_minutes(s: pd.Series) -> int:
    """Max consecutive NaN streak length in minutes."""
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

def profit_lock_floor_series(peak: pd.Series) -> pd.Series:
    """
    Convert a peak-PnL series (rupees) into a locked-profit "floor" series (rupees).

    Profile (default):
      peak= 5k  -> floor=1k
      peak=10k  -> floor=3k
      peak=15k  -> floor=5k  (implied by linear interpolation)
      peak>15k  -> floor = peak - 10k (max giveback 10k)

    Below PROFIT_LOCK_P1_PROFIT => floor = 0 (no lock).
    """
    floor = pd.Series(0.0, index=peak.index)

    p1, f1 = PROFIT_LOCK_P1_PROFIT, PROFIT_LOCK_P1_LOCK
    p2, f2 = PROFIT_LOCK_P2_PROFIT, PROFIT_LOCK_P2_LOCK
    p3     = PROFIT_LOCK_P3_PROFIT
    gb_max = PROFIT_LOCK_MAX_GIVEBACK_RUPEES

    # Guard against bad config
    if p2 <= p1:
        return floor
    m = (f2 - f1) / (p2 - p1)  # slope

    seg1 = (peak >= p1) & (peak < p2)
    floor.loc[seg1] = f1 + m * (peak.loc[seg1] - p1)

    # Continue same slope up to p3 (gives floor(p3)=f2 + m*(p3-p2))
    seg2 = (peak >= p2) & (peak < p3)
    floor.loc[seg2] = f2 + m * (peak.loc[seg2] - p2)

    # After p3: cap giveback
    seg3 = (peak >= p3)
    floor.loc[seg3] = peak.loc[seg3] - gb_max

    return floor

# =============================================================================
# CORE STRATEGY SIMULATION (same semantics as your Zerodha backtester)
# =============================================================================
def simulate_day_multi_trades_dhan(
    *,
    und: str,
    dy: date,
    expiry: date,
    day_opt: pd.DataFrame,
    source_pickle: str,
) -> Tuple[List[TradeRow], List[Dict[str, Any]], List[TradeRow]]:
    results: List[TradeRow] = []
    skipped: List[Dict[str, Any]] = []
    pess_results: List[TradeRow] = []

    idx_all = build_minute_index(dy, SESSION_START_IST, SESSION_END_IST)
    session_end_ts = idx_all[-1]

    qty = int(QTY_UNITS[und])
    step = int(STRIKE_STEP[und])

    spot_s = _build_underlying_series_from_spot(day_opt, idx_all)

    cur_entry_ts = pd.Timestamp(datetime.combine(dy, ENTRY_TIME), tz=ist_tz())
    trade_seq = 1

    while cur_entry_ts <= session_end_ts:
        if cur_entry_ts not in idx_all:
            skipped.append({
                "day": dy, "underlying": und, "expiry": expiry, "trade_seq": trade_seq,
                "source_pickle": source_pickle,
                "reason": "Entry timestamp not in session minute grid"
            })
            break

        u_px = float(spot_s.loc[cur_entry_ts]) if pd.notna(spot_s.loc[cur_entry_ts]) else float("nan")
        if pd.isna(u_px):
            skipped.append({
                "day": dy, "underlying": und, "expiry": expiry, "trade_seq": trade_seq,
                "source_pickle": source_pickle,
                "reason": f"No underlying spot at entry {cur_entry_ts.strftime('%H:%M')}"
            })
            break

        atm = round_to_step(float(u_px), step)

        # Close series (entry pricing, profit-protect tracking, reporting)
        ce_close = _build_leg_series_fixed_strike(day_opt, idx_all, atm, "CE", "close_f")
        pe_close = _build_leg_series_fixed_strike(day_opt, idx_all, atm, "PE", "close_f")

        # High/Low series (STOPLOSS intraminute extremes)
        ce_high = _build_leg_series_fixed_strike(day_opt, idx_all, atm, "CE", "high_f")
        ce_low = _build_leg_series_fixed_strike(day_opt, idx_all, atm, "CE", "low_f")
        pe_high = _build_leg_series_fixed_strike(day_opt, idx_all, atm, "PE", "high_f")
        pe_low = _build_leg_series_fixed_strike(day_opt, idx_all, atm, "PE", "low_f")

        ce_entry = ce_close.loc[cur_entry_ts]
        pe_entry = pe_close.loc[cur_entry_ts]
        if pd.isna(ce_entry) or pd.isna(pe_entry):
            skipped.append({
                "day": dy, "underlying": und, "expiry": expiry, "trade_seq": trade_seq,
                "atm_strike": atm,
                "source_pickle": source_pickle,
                "reason": "No CE/PE price at entry (strike missing in band or data gap)"
            })
            break

        # Per-attempt thresholds computed from entry premium sum (rupees)
        premium_sum_points = float(ce_entry) + float(pe_entry)  # points
        premium_sum_rupees = premium_sum_points * qty  # rupees

        loss_limit_rupees = premium_sum_rupees * LOSS_LIMIT_PCT

        # Effective stoploss is the tighter of:
        #   (a) premium-based SL
        #   (b) MAX_STOPLOSS_RUPEES (if enabled)
        effective_loss_limit_rupees = loss_limit_rupees
        if MAX_STOPLOSS_RUPEES and MAX_STOPLOSS_RUPEES > 0:
            effective_loss_limit_rupees = min(loss_limit_rupees, MAX_STOPLOSS_RUPEES)

        # Profit protect can be disabled entirely via PROFIT_PROTECT_MODE=off
        profit_protect_enabled = (PROFIT_PROTECT_MODE not in ("off", "0", "false")) and (PROFIT_PROTECT_TRIGGER_PCT > 0)

        # "G" in rupees (used by pct_trail mode and for reporting)
        G = premium_sum_rupees * PROFIT_PROTECT_TRIGGER_PCT
        if MAX_PROFIT_PROTECT_RUPEES and MAX_PROFIT_PROTECT_RUPEES > 0:
            G = min(G, MAX_PROFIT_PROTECT_RUPEES)

        if STRICT_STRIKE_PRESENCE:
            ce_post = ce_close.loc[cur_entry_ts:]
            pe_post = pe_close.loc[cur_entry_ts:]
            max_miss = max(_missing_streak_minutes(ce_post), _missing_streak_minutes(pe_post))
            if max_miss > MAX_MISSING_STREAK_MIN:
                skipped.append({
                    "day": dy, "underlying": und, "expiry": expiry, "trade_seq": trade_seq,
                    "atm_strike": atm,
                    "source_pickle": source_pickle,
                    "reason": f"Strike series missing too much after entry (max_missing_streak={max_miss}m)"
                })
                break

        # Short straddle MTM PnL series (entry - current) * qty for each leg
        pnl_close_all = (float(ce_entry) - ce_close) * qty + (float(pe_entry) - pe_close) * qty
        pnl = pnl_close_all.loc[cur_entry_ts:].dropna()  # keep close-based pnl for profit protect

        # Pessimistic PnL series: assume buyback cost is CE_high + PE_high each minute
        pnl_pess_all = (float(ce_entry) - ce_high) * qty + (float(pe_entry) - pe_high) * qty
        pnl_pess = pnl_pess_all.loc[cur_entry_ts:].dropna()

        pnl_ceHigh_peLow_all = (float(ce_entry) - ce_high) * qty + (float(pe_entry) - pe_low) * qty
        pnl_ceLow_peHigh_all = (float(ce_entry) - ce_low) * qty + (float(pe_entry) - pe_high) * qty

        pnl_sl_all = pd.concat([pnl_close_all, pnl_ceHigh_peLow_all, pnl_ceLow_peHigh_all], axis=1).min(axis=1)
        pnl_sl = pnl_sl_all.loc[cur_entry_ts:].dropna()
        if pnl.empty:
            skipped.append({
                "day": dy, "underlying": und, "expiry": expiry, "trade_seq": trade_seq,
                "atm_strike": atm,
                "source_pickle": source_pickle,
                "reason": "PnL series empty after entry"
            })
            break

        eod_ts = pnl.index[-1]
        eod_pnl = float(pnl.iloc[-1])

        max_profit = float(max(0.0, pnl.max()))
        max_loss = float(min(0.0, pnl.min()))

        # Reporting cap: in live you enforce MAX_STOPLOSS_RUPEES; don't show max_loss worse than that
        if MAX_STOPLOSS_RUPEES and MAX_STOPLOSS_RUPEES > 0:
            max_loss = max(max_loss, -float(MAX_STOPLOSS_RUPEES))

        # STOPLOSS: first time pnl crosses <= -LOSS_LIMIT_RUPEES
        stop_hit = pnl_sl <= -effective_loss_limit_rupees
        stop_ts = pnl_sl.index[stop_hit.to_numpy().argmax()] if stop_hit.any() else None

        # PROFIT_PROTECT: arm once peak >= G; then exit when pnl <= peak - G
        # PROFIT_PROTECT (two modes):
        #  - pct_trail  : legacy (arm at G, trail by G)
        #  - lock_curve : flexible lock-in profile based on peak profit
        protect_ts = None
        if profit_protect_enabled:
            peak = pnl.cummax()

            if PROFIT_PROTECT_MODE == "pct_trail":
                if G > 0:
                    armed = peak >= G
                    trail = peak - G
                    protect_hit = armed & (pnl <= trail)
                    protect_ts = pnl.index[protect_hit.to_numpy().argmax()] if protect_hit.any() else None

            elif PROFIT_PROTECT_MODE == "lock_curve":
                if PROFIT_LOCK_ENABLED:
                    floor = profit_lock_floor_series(peak)
                    armed = peak >= PROFIT_LOCK_P1_PROFIT  # start locking only after first milestone
                    protect_hit = armed & (pnl <= floor)
                    protect_ts = pnl.index[protect_hit.to_numpy().argmax()] if protect_hit.any() else None

            else:
                raise ValueError(f"Unknown PROFIT_PROTECT_MODE={PROFIT_PROTECT_MODE!r}")

        exit_ts = eod_ts
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

        exit_pnl_gross = float(pnl.loc[exit_ts])

        # Cap STOPLOSS exit P/L so it never shows worse than the configured stoploss
        if exit_reason == "STOPLOSS" and exit_pnl_gross < -effective_loss_limit_rupees:
            exit_pnl_gross = -float(effective_loss_limit_rupees)

        exit_ce = float(ce_close.loc[exit_ts]) if pd.notna(ce_close.loc[exit_ts]) else float("nan")
        exit_pe = float(pe_close.loc[exit_ts]) if pd.notna(pe_close.loc[exit_ts]) else float("nan")

        # Transaction charges
        txn_charges = compute_trade_charges(
            entry_ce=float(ce_entry), entry_pe=float(pe_entry),
            exit_ce=exit_ce if not pd.isna(exit_ce) else 0.0,
            exit_pe=exit_pe if not pd.isna(exit_pe) else 0.0,
            qty=qty,
        )
        exit_pnl = exit_pnl_gross - txn_charges

        dte = int((expiry - dy).days)

        # --- Pessimistic trajectory exits (same rules, but on pnl_pess) ---
        eod_ts_pess = pnl_pess.index[-1]
        eod_pnl_pess = float(pnl_pess.iloc[-1])

        stop_hit_pess = pnl_pess <= -effective_loss_limit_rupees
        stop_ts_pess = pnl_pess.index[stop_hit_pess.to_numpy().argmax()] if stop_hit_pess.any() else None

        protect_ts_pess = None
        if profit_protect_enabled:
            peak_pess = pnl_pess.cummax()

            if PROFIT_PROTECT_MODE == "pct_trail":
                if G > 0:
                    armed_pess = peak_pess >= G
                    trail_pess = peak_pess - G
                    protect_hit_pess = armed_pess & (pnl_pess <= trail_pess)
                    protect_ts_pess = pnl_pess.index[
                        protect_hit_pess.to_numpy().argmax()] if protect_hit_pess.any() else None

            elif PROFIT_PROTECT_MODE == "lock_curve":
                if PROFIT_LOCK_ENABLED:
                    floor_pess = profit_lock_floor_series(peak_pess)
                    armed_pess = peak_pess >= PROFIT_LOCK_P1_PROFIT
                    protect_hit_pess = armed_pess & (pnl_pess <= floor_pess)
                    protect_ts_pess = pnl_pess.index[
                        protect_hit_pess.to_numpy().argmax()] if protect_hit_pess.any() else None

        # Choose earliest exit among triggers, else EOD
        exit_ts_pess = eod_ts_pess
        exit_reason_pess = "EOD"
        if stop_ts_pess is not None and protect_ts_pess is not None:
            if stop_ts_pess <= protect_ts_pess:
                exit_ts_pess, exit_reason_pess = stop_ts_pess, "STOPLOSS"
            else:
                exit_ts_pess, exit_reason_pess = protect_ts_pess, "PROFIT_PROTECT"
        elif stop_ts_pess is not None:
            exit_ts_pess, exit_reason_pess = stop_ts_pess, "STOPLOSS"
        elif protect_ts_pess is not None:
            exit_ts_pess, exit_reason_pess = protect_ts_pess, "PROFIT_PROTECT"

        exit_pnl_pess = float(pnl_pess.loc[exit_ts_pess])

        # Cap pessimistic STOPLOSS report (same philosophy)
        if exit_reason_pess == "STOPLOSS" and exit_pnl_pess < -effective_loss_limit_rupees:
            exit_pnl_pess = -float(effective_loss_limit_rupees)

        # Rolling dataset has no per-strike tradingsymbol; store synthetic identifiers
        ce_sym = f"{und}_{expiry.strftime('%Y%m%d')}_{atm}_CE"
        pe_sym = f"{und}_{expiry.strftime('%Y%m%d')}_{atm}_PE"

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
                source_pickle=source_pickle,
            )
        )

        # --- build pessimistic TradeRow for this SAME attempt (does not change reattempt control-flow) ---

        # Use CE/PE HIGH at pessimistic exit time so charges and exits are consistent with pessimistic assumption
        exit_ce_pess = float(ce_high.loc[exit_ts_pess]) if pd.notna(ce_high.loc[exit_ts_pess]) else float("nan")
        exit_pe_pess = float(pe_high.loc[exit_ts_pess]) if pd.notna(pe_high.loc[exit_ts_pess]) else float("nan")

        txn_charges_pess = compute_trade_charges(
            entry_ce=float(ce_entry), entry_pe=float(pe_entry),
            exit_ce=exit_ce_pess if not pd.isna(exit_ce_pess) else 0.0,
            exit_pe=exit_pe_pess if not pd.isna(exit_pe_pess) else 0.0,
            qty=qty,
        )
        exit_pnl_net_pess = float(exit_pnl_pess) - txn_charges_pess

        max_profit_pess = float(max(0.0, pnl_pess.max()))
        max_loss_pess = float(min(0.0, pnl_pess.min()))
        if MAX_STOPLOSS_RUPEES and MAX_STOPLOSS_RUPEES > 0:
            max_loss_pess = max(max_loss_pess, -float(MAX_STOPLOSS_RUPEES))

        pess_results.append(
            TradeRow(
                day=dy,
                underlying=und,
                trade_seq=trade_seq,
                expiry=expiry,
                days_to_expiry=dte,
                atm_strike=int(atm),
                qty_units=qty,
                entry_time=pd.Timestamp(cur_entry_ts).strftime("%H:%M"),
                exit_time=pd.Timestamp(exit_ts_pess).strftime("%H:%M"),
                exit_reason=exit_reason_pess,
                entry_underlying=float(u_px),
                ce_symbol=ce_sym,
                pe_symbol=pe_sym,
                entry_ce=float(ce_entry),
                entry_pe=float(pe_entry),
                exit_ce=exit_ce_pess,
                exit_pe=exit_pe_pess,
                exit_pnl_gross=float(exit_pnl_pess),
                txn_charges=txn_charges_pess,
                exit_pnl=exit_pnl_net_pess,
                eod_pnl=float(eod_pnl_pess),
                max_profit=max_profit_pess,
                max_loss=max_loss_pess,
                source_pickle=source_pickle,
            )
        )

        # Reattempt logic
        if exit_reason in ("STOPLOSS", "PROFIT_PROTECT") and (trade_seq - 1) < MAX_REATTEMPTS:
            trade_seq += 1
            cur_entry_ts = pd.Timestamp(exit_ts) + pd.Timedelta(minutes=REENTRY_DELAY_MINUTES)
            if cur_entry_ts > session_end_ts:
                break
            continue

        break

    return results, skipped, pess_results


# =============================================================================
# PER-PICKLE PROCESSOR (each pickle is an independent unit)
# =============================================================================
def process_one_pickle(p: str, window_start: date, window_end: date) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Independent processing:
    - Normalize
    - Filter to (window_start..window_end)
    - Find nearest expiry per (underlying, day) inside THIS file only
    - Simulate trades for those (underlying, day, expiry)
    """
    src = os.path.basename(p)
    pess_trades_out: List[Dict[str, Any]] = []

    raw = pd.read_pickle(p)
    if not isinstance(raw, pd.DataFrame) or raw.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame([{"source_pickle": src, "reason": "Empty or non-DataFrame pickle"}])

    d = _normalize_dhan_df(raw, src)
    if d.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame([{"source_pickle": src, "reason": "No usable rows after normalization"}])

    d = d[(d["day"] >= window_start) & (d["day"] <= window_end)]
    if d.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame([{"source_pickle": src, "reason": "No rows in requested date window"}])

    # Nearest expiry within this pickle
    min_expiry_local: Dict[Tuple[str, date], date] = (
        d.groupby(["underlying", "day"], sort=False)["expiry"].min().to_dict()
    )

    trades_out: List[Dict[str, Any]] = []
    skipped_out: List[Dict[str, Any]] = []

    # Grouping ensures we only trade one expiry per day (nearest expiry) within this file
    for (und, dy, ex), g in d.groupby(["underlying", "day", "expiry"], sort=False):
        if min_expiry_local.get((und, dy)) != ex:
            continue

        trades, skips, pess_trades = simulate_day_multi_trades_dhan(
            und=und,
            dy=dy,
            expiry=ex,
            day_opt=g,
            source_pickle=src,
        )
        trades_out.extend([t.__dict__ for t in trades])
        pess_trades_out.extend([t.__dict__ for t in pess_trades])
        skipped_out.extend(skips)

    return pd.DataFrame(trades_out), pd.DataFrame(pess_trades_out), pd.DataFrame(skipped_out)


# =============================================================================
# DEDUP LOGIC (trade-row level, across pickles)
# =============================================================================
def dedup_trades_across_pickles(trades: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Duplicate trade rows can arise when the same (underlying, day, expiry) appears in multiple pickles
    (batch overlap, holiday shifts). We dedup on a stable trade identity key.

    Key choice rationale:
    - underlying/day/expiry/trade_seq/entry_time uniquely identifies "attempt #k for that day+expiry".
    - source_pickle is excluded from the key (because that's what changes across duplicates).
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
            "reason": f"Dedup across pickles removed {removed} duplicate trade rows (key={key_cols})"
        }])

    return deduped, report


# =============================================================================
# ACTUAL TRADES (one underlying/day selection)
# =============================================================================
def build_actual_trades_df(all_trades_df: pd.DataFrame) -> pd.DataFrame:
    """
    Choose one underlying per day:
      - earliest expiry wins
      - tie-break: NIFTY preferred over SENSEX
    Keep all reattempts for that underlying/day.
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
    out = out.sort_values(["day", "trade_seq", "source_pickle"]).reset_index(drop=True)

    # (2) Keep only Expiry day (D0) and Expiry-1 (D-1) trades in the "actual_trades" output
    dte = pd.to_numeric(out.get("days_to_expiry"), errors="coerce")
    out = out[dte.isin([0, 1])].copy()

    # (1) success flag for the "actual_trades" sheet
    out["success"] = (pd.to_numeric(out["exit_pnl"], errors="coerce") > 0).astype(int)

    return out


# =============================================================================
# MONTHWISE SUMMARY + STREAKS
# =============================================================================
def _max_streak_days(pnls: List[float], mode: str) -> int:
    """
    Compute max consecutive streak length in days.
    mode:
      - "profit": pnl > 0
      - "loss": pnl < 0
    Zero pnl breaks both streaks.
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

def build_daily_and_monthly_summary(actual_trades_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    daily_pnl: one row per day:
      net_exit_pnl = sum(exit_pnl across attempts for that day)
    monthwise_summary: total/avg PnL + win/loss days + max profit/loss streaks (in trading days)
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
            best_trade_max_profit=("max_profit", "max"),
            worst_trade_max_loss=("max_loss", "min"),
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
    }])

    monthwise = pd.concat([monthwise, overall], ignore_index=True)
    return daily, monthwise

def _profit_factor(pnls: pd.Series) -> float:
    pnls = pnls.astype(float)
    gp = pnls[pnls > 0].sum()
    gl = -pnls[pnls < 0].sum()
    if gl > 0:
        return float(gp / gl)
    return float("inf") if gp > 0 else 0.0


def build_expiry_attempt_summaries(actual_trades_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Detailed performance by:
      - expiry bucket (D0 expiry day vs D-1 vs OTHER)
      - attempt (trade_seq 1 vs 2)
    Output: monthwise, yearwise, overall.

    "Accuracy" = win_rate_exit_pct (exit_pnl > 0).
    """
    if actual_trades_df.empty:
        empty = pd.DataFrame()
        return empty, empty, empty

    t = actual_trades_df.copy()

    # Period keys
    t["month"] = pd.to_datetime(t["day"]).dt.to_period("M").astype(str)
    t["year"] = pd.to_datetime(t["day"]).dt.year.astype(int)

    # Expiry bucket (D0 / D-1 / OTHER)
    dte = pd.to_numeric(t.get("days_to_expiry"), errors="coerce")
    t["expiry_bucket"] = "OTHER"
    t.loc[dte == 0, "expiry_bucket"] = "D0_EXPIRY"
    t.loc[dte == 1, "expiry_bucket"] = "D-1"

    # Per-trade premium + thresholds (for “useful details”)
    t["entry_premium_points"] = pd.to_numeric(t["entry_ce"], errors="coerce") + pd.to_numeric(t["entry_pe"], errors="coerce")
    t["entry_premium_rupees"] = t["entry_premium_points"] * pd.to_numeric(t["qty_units"], errors="coerce")

    t["sl_premium_rupees"] = t["entry_premium_rupees"] * float(LOSS_LIMIT_PCT)
    t["sl_effective_rupees"] = t["sl_premium_rupees"]
    if MAX_STOPLOSS_RUPEES and MAX_STOPLOSS_RUPEES > 0:
        t["sl_effective_rupees"] = t["sl_premium_rupees"].clip(upper=float(MAX_STOPLOSS_RUPEES))

    t["G_rupees"] = t["entry_premium_rupees"] * float(PROFIT_PROTECT_TRIGGER_PCT)
    if MAX_PROFIT_PROTECT_RUPEES and MAX_PROFIT_PROTECT_RUPEES > 0:
        t["G_rupees"] = t["G_rupees"].clip(upper=float(MAX_PROFIT_PROTECT_RUPEES))
    if PROFIT_PROTECT_MODE in ("off", "0", "false"):
        t["G_rupees"] = 0.0

    # Outcome flags (rates)
    t["is_win_exit"] = pd.to_numeric(t["exit_pnl"], errors="coerce") > 0
    er = t["exit_reason"].astype(str).str.upper()
    t["is_stoploss"] = er.eq("STOPLOSS")
    t["is_profit_protect"] = er.eq("PROFIT_PROTECT")
    t["is_eod"] = er.eq("EOD")

    def _summarize(g: pd.DataFrame) -> pd.Series:
        pnls = pd.to_numeric(g["exit_pnl"], errors="coerce").astype(float)
        return pd.Series({
            "trades": int(len(g)),
            "trading_days": int(g["day"].nunique()),
            "total_exit_pnl": float(pnls.sum()),
            "avg_exit_pnl": float(pnls.mean()),
            "median_exit_pnl": float(pnls.median()),
            "win_rate_exit_pct": float(100.0 * g["is_win_exit"].mean()),
            "profit_factor": float(_profit_factor(pnls)),
            "stoploss_rate_pct": float(100.0 * g["is_stoploss"].mean()),
            "profit_protect_rate_pct": float(100.0 * g["is_profit_protect"].mean()),
            "eod_rate_pct": float(100.0 * g["is_eod"].mean()),
            "avg_entry_premium_rupees": float(pd.to_numeric(g["entry_premium_rupees"], errors="coerce").mean()),
            "avg_sl_effective_rupees": float(pd.to_numeric(g["sl_effective_rupees"], errors="coerce").mean()),
            "avg_G_rupees": float(pd.to_numeric(g["G_rupees"], errors="coerce").mean()),
        })

    # Monthwise
    monthwise = (
        t.groupby(["month", "expiry_bucket", "trade_seq"], as_index=False)
         .apply(_summarize)
         .reset_index()
         .drop(columns=["index"], errors="ignore")
         .sort_values(["month", "expiry_bucket", "trade_seq"])
         .reset_index(drop=True)
    )
    # Add share of trades within month (useful)
    month_tot = monthwise.groupby("month")["trades"].transform("sum").astype(float)
    monthwise["pct_of_trades_in_month"] = (100.0 * monthwise["trades"] / month_tot).round(2)

    # Yearwise
    yearwise = (
        t.groupby(["year", "expiry_bucket", "trade_seq"], as_index=False)
         .apply(_summarize)
         .reset_index()
         .drop(columns=["index"], errors="ignore")
         .sort_values(["year", "expiry_bucket", "trade_seq"])
         .reset_index(drop=True)
    )
    year_tot = yearwise.groupby("year")["trades"].transform("sum").astype(float)
    yearwise["pct_of_trades_in_year"] = (100.0 * yearwise["trades"] / year_tot).round(2)

    # Overall
    overall = (
        t.groupby(["expiry_bucket", "trade_seq"], as_index=False)
         .apply(_summarize)
         .reset_index()
         .drop(columns=["index"], errors="ignore")
         .sort_values(["expiry_bucket", "trade_seq"])
         .reset_index(drop=True)
    )

    return monthwise, yearwise, overall

# =============================================================================
# EXCEL OUTPUT
# =============================================================================
def _autosize_columns_safe(ws) -> None:
    """Best-effort auto width; capped for performance."""
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
    all_trades_pess_df: pd.DataFrame,
    actual_trades_pess_df: pd.DataFrame,
    daily_pnl_df: pd.DataFrame,
    monthwise_df: pd.DataFrame,
    exp_month_df: pd.DataFrame,
    exp_year_df: pd.DataFrame,
    exp_overall_df: pd.DataFrame,
    skipped_df: pd.DataFrame,
) -> None:
    """Write workbook (trade tables, pivots, summaries)."""
    out_dir = os.path.dirname(os.path.abspath(OUTPUT_XLSX))
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    piv_exit = pd.DataFrame()
    piv_first_trade_eod = pd.DataFrame()
    instrument_summary = pd.DataFrame()

    if not all_trades_df.empty:
        piv_exit = all_trades_df.pivot_table(
            index="day", columns="underlying", values="exit_pnl", aggfunc="sum"
        ).reset_index()

        # "First attempt only" EOD PnL pivot (kept for parity with your older outputs)
        first = all_trades_df[all_trades_df["trade_seq"] == 1].copy()
        if not first.empty:
            piv_first_trade_eod = first.pivot_table(
                index="day", columns="underlying", values="eod_pnl", aggfunc="sum"
            ).reset_index()

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

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as xw:
        all_trades_df.to_excel(xw, sheet_name="all_trades_backtested", index=False)
        actual_trades_df.to_excel(xw, sheet_name="actual_trades", index=False)
        all_trades_pess_df.to_excel(xw, sheet_name="all_trades_pessimistic", index=False)
        actual_trades_pess_df.to_excel(xw, sheet_name="actual_trades_pessimistic", index=False)
        daily_pnl_df.to_excel(xw, sheet_name="daily_pnl_actual", index=False)
        monthwise_df.to_excel(xw, sheet_name="monthwise_summary", index=False)

        exp_month_df.to_excel(xw, sheet_name="expiry_attempt_monthwise", index=False)
        exp_year_df.to_excel(xw, sheet_name="expiry_attempt_yearwise", index=False)
        exp_overall_df.to_excel(xw, sheet_name="expiry_attempt_overall", index=False)

        piv_exit.to_excel(xw, sheet_name="exit_pnl_pivot", index=False)
        piv_first_trade_eod.to_excel(xw, sheet_name="eod_pnl_first_trade_pivot", index=False)
        instrument_summary.to_excel(xw, sheet_name="instrument_summary", index=False)

        skipped_df.to_excel(xw, sheet_name="skipped", index=False)

        wb = xw.book
        for ws in wb.worksheets:
            ws.freeze_panes = "A2"
            _autosize_columns_safe(ws)

    print(f"[DONE] Excel written: {OUTPUT_XLSX}")


# =============================================================================
# WINDOW END DISCOVERY (optional but recommended when data is historical)
# =============================================================================
def discover_data_max_day(pickle_paths: List[str]) -> Optional[date]:
    """
    Find max trading day present in the dataset.
    This avoids the common pitfall: if data is from 2024 but today is 2026, LOOKBACK_MONTHS would select no rows.
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
            # Non-fatal; window end can still be derived from others
            continue

    return max_day


# =============================================================================
# MAIN
# =============================================================================
def main():
    paths = sorted(glob.glob(os.path.join(PICKLES_DIR, "*.pkl")) + glob.glob(os.path.join(PICKLES_DIR, "*.pickle")))
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
    print(f"[INFO] Stoploss: {LOSS_LIMIT_PCT:.1%} of premium | ProfitProtect(G): {PROFIT_PROTECT_TRIGGER_PCT:.1%} of premium | Re-entry delay: {REENTRY_DELAY_MINUTES}m")
    print(f"[INFO] Strict strike presence: {STRICT_STRIKE_PRESENCE} (max_missing_streak={MAX_MISSING_STREAK_MIN}m)")
    print(f"[INFO] Dedup within pickle: {DEDUP_WITHIN_PICKLE} | Dedup across pickles: {DEDUP_ACROSS_PICKLES}")
    print(f"[INFO] Output: {OUTPUT_XLSX}")

    all_trades_list: List[pd.DataFrame] = []
    all_trades_pess_list: List[pd.DataFrame] = []
    skipped_list: List[pd.DataFrame] = []

    # Each pickle is processed independently
    for p in paths:
        try:
            tdf, pdf, sdf = process_one_pickle(p, window_start, end_day)

            if tdf is not None and not tdf.empty:
                all_trades_list.append(tdf)

            if pdf is not None and not pdf.empty:
                all_trades_pess_list.append(pdf)

            if sdf is not None and not sdf.empty:
                if "source_pickle" not in sdf.columns:
                    sdf["source_pickle"] = os.path.basename(p)
                skipped_list.append(sdf)

            print(f"[OK] processed {os.path.basename(p)} trades={len(tdf) if tdf is not None else 0} skipped={len(sdf) if sdf is not None else 0}")

        except Exception as e:
            msg = f"[WARN] {os.path.basename(p)} failed: {e}"
            if FAIL_ON_PICKLE_ERROR:
                raise RuntimeError(msg) from e
            print(msg)
            skipped_list.append(pd.DataFrame([{"source_pickle": os.path.basename(p), "reason": str(e)}]))

    all_trades_df = pd.concat(all_trades_list, ignore_index=True) if all_trades_list else pd.DataFrame()
    all_trades_pess_df = pd.concat(all_trades_pess_list, ignore_index=True) if all_trades_pess_list else pd.DataFrame()
    skipped_df = pd.concat(skipped_list, ignore_index=True) if skipped_list else pd.DataFrame()

    # Optional dedup across pickles (aggregation hygiene; does NOT affect per-pickle independence)
    if not all_trades_df.empty and DEDUP_ACROSS_PICKLES:
        all_trades_df, dedup_report = dedup_trades_across_pickles(all_trades_df)
        if not dedup_report.empty:
            skipped_df = pd.concat([skipped_df, dedup_report], ignore_index=True)

    if not all_trades_df.empty:
        all_trades_df = all_trades_df.sort_values(["day", "underlying", "trade_seq", "source_pickle"]).reset_index(drop=True)


    if not all_trades_pess_df.empty and DEDUP_ACROSS_PICKLES:
        all_trades_pess_df, dedup_report = dedup_trades_across_pickles(all_trades_pess_df)
        if not dedup_report.empty:
            skipped_df = pd.concat([skipped_df, dedup_report], ignore_index=True)

    if not all_trades_pess_df.empty:
        all_trades_pess_df = all_trades_pess_df.sort_values(["day", "underlying", "trade_seq", "source_pickle"]).reset_index(drop=True)

    # Choose one underlying per day (actual trades)
    actual_trades_df = build_actual_trades_df(all_trades_df)

    actual_trades_pess_df = build_actual_trades_df(all_trades_pess_df)

    # Monthwise + streak summaries computed from ACTUAL daily net P/L
    daily_pnl_df, monthwise_df = build_daily_and_monthly_summary(actual_trades_df)

    exp_month_df, exp_year_df, exp_overall_df = build_expiry_attempt_summaries(actual_trades_df)

    write_excel(
        all_trades_df, actual_trades_df, all_trades_pess_df, actual_trades_pess_df,
        daily_pnl_df, monthwise_df,
        exp_month_df, exp_year_df, exp_overall_df,
        skipped_df
    )

    if all_trades_df.empty:
        print("[WARN] No completed trades. See 'skipped' sheet (and verify WINDOW_END_MODE / LOOKBACK_MONTHS).")
    else:
        print(all_trades_df.groupby("underlying")[["exit_pnl"]].describe())


if __name__ == "__main__":
    main()