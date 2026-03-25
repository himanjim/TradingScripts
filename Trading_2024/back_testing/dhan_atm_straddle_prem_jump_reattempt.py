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

ENTRY_TIME_IST = os.getenv("ENTRY_TIME_IST", "09:40")  # HH:MM
LOSS_LIMIT_RUPEES = int(os.getenv("LOSS_LIMIT_RUPEES", "3000"))
PROFIT_PROTECT_TRIGGER_RUPEES = int(os.getenv("PROFIT_PROTECT_TRIGGER_RUPEES", "20000"))

MAX_REATTEMPTS = int(os.getenv("MAX_REATTEMPTS", "10"))  # 1 = only one re-entry
REENTRY_DELAY_MINUTES = int(os.getenv("REENTRY_DELAY_MINUTES", "30"))
LOOKBACK_MONTHS = int(os.getenv("LOOKBACK_MONTHS", "12"))

# Window selection:
# - "data": end_day = max day present in pickles (recommended; avoids empty results when data is old)
# - "today": end_day = date.today()
WINDOW_END_MODE = os.getenv("WINDOW_END_MODE", "data").strip().lower()

# Contract quantities and ATM rounding steps
QTY_UNITS = {"NIFTY": 325, "SENSEX": 100}
TRADEABLE = set(QTY_UNITS.keys())
STRIKE_STEP = {"NIFTY": 50, "SENSEX": 100}

# =============================================================================
# TRANSACTION CHARGES (same logic as script A)
# =============================================================================
BROKERAGE_PER_ORDER       = 20.0
ORDERS_PER_TRADE          = 4
STT_SELL_PCT              = 0.001
EXCHANGE_TXN_PCT          = 0.0003553
SEBI_PER_CRORE            = 10.0
STAMP_BUY_PCT             = 0.00003
IPFT_PER_CRORE            = 0.010
GST_PCT                   = 0.18
INCLUDE_TRANSACTION_COSTS = True

def compute_trade_charges(
    entry_ce: float, entry_pe: float,
    exit_ce: float, exit_pe: float,
    qty: int,
) -> float:
    if not INCLUDE_TRANSACTION_COSTS:
        return 0.0

    entry_turnover = (entry_ce + entry_pe) * qty
    exit_turnover  = (exit_ce + exit_pe) * qty
    total_turnover = entry_turnover + exit_turnover

    brokerage = BROKERAGE_PER_ORDER * ORDERS_PER_TRADE
    stt = entry_turnover * STT_SELL_PCT
    txn_charges = total_turnover * EXCHANGE_TXN_PCT
    sebi = total_turnover * SEBI_PER_CRORE / 1_00_00_000
    stamp = exit_turnover * STAMP_BUY_PCT
    ipft = total_turnover * IPFT_PER_CRORE / 1_00_00_000
    gst = (brokerage + txn_charges + sebi) * GST_PCT

    total_charges = brokerage + stt + txn_charges + sebi + stamp + ipft + gst
    return round(total_charges, 2)

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

_DEFAULT_OUT = rf"C:\Users\himan\Downloads\dhan_short_straddle_backtest_reattempt_{_safe_fname_part(ENTRY_TIME_IST)}_LL_{LOSS_LIMIT_RUPEES}_PPT_{PROFIT_PROTECT_TRIGGER_RUPEES}_RDM_{REENTRY_DELAY_MINUTES}_MR_{MAX_REATTEMPTS}.xlsx"
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
# OUTPUT STRUCTURE
# =============================================================================
@dataclass
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

    exit_pnl_gross: float
    txn_charges: float
    exit_pnl: float
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
    Keeps close/high/low so STOPLOSS logic matches script A.
    """
    needed = ["symbol", "leg", "strike", "close", "spot", "target_expiry_date"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(f"{source_name}: missing columns {missing}")

    tcol = _pick_time_col(df)
    d = df.copy()

    if tcol in ("dt_ist", "timestamp_dt"):
        d["ts"] = ensure_ist(d[tcol])
    else:
        dt_utc = pd.to_datetime(d["timestamp"], unit="s", utc=True)
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
    d["high_f"] = pd.to_numeric(d["high"], errors="coerce") if "high" in d.columns else d["close_f"]
    d["low_f"] = pd.to_numeric(d["low"], errors="coerce") if "low" in d.columns else d["close_f"]
    d["spot_f"] = pd.to_numeric(d["spot"], errors="coerce")

    d["leg"] = d["leg"].astype(str).str.upper().str.strip()
    d = d[d["leg"].isin(["CE", "PE"])]

    d = d.dropna(subset=["ts", "day", "expiry", "strike_int", "close_f", "spot_f"])
    d["strike_int"] = d["strike_int"].astype(int)
    d["close_f"] = d["close_f"].astype(float)
    d["high_f"] = d["high_f"].fillna(d["close_f"]).astype(float)
    d["low_f"] = d["low_f"].fillna(d["close_f"]).astype(float)
    d["spot_f"] = d["spot_f"].astype(float)

    d = d[d["expiry"] >= d["day"]]

    if DEDUP_WITHIN_PICKLE and not d.empty:
        d = d.sort_values("ts").drop_duplicates(
            subset=["ts", "underlying", "expiry", "leg", "strike_int"],
            keep="last"
        )

    keep = [
        "ts", "day", "underlying", "expiry", "leg", "strike_int",
        "close_f", "high_f", "low_f", "spot_f"
    ]
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
    price_col: str = "close_f",
    do_ffill: bool = True,
) -> pd.Series:
    sub = day_opt[
        (day_opt["strike_int"] == strike) & (day_opt["leg"] == leg)
    ][["ts", price_col]].dropna()

    if sub.empty:
        return pd.Series(index=idx_all, dtype="float64")

    sub = sub.sort_values("ts").groupby("ts", as_index=True)[price_col].last()
    s = sub.reindex(idx_all)
    return s.ffill() if do_ffill else s

def _missing_streak_minutes(s: pd.Series) -> int:
    """Return the maximum consecutive NaN streak length in the series."""
    is_na = s.isna().to_numpy()
    if not is_na.any():
        return 0

    best = 0
    cur = 0
    for v in is_na:
        if v:
            cur += 1
            if cur > best:
                best = cur
        else:
            cur = 0
    return best

def scan_pickles_pass1_dhan(pickle_paths: List[str]) -> Tuple[date, Dict[Tuple[str, date], date], date]:
    max_day_seen: Optional[date] = None
    min_day_seen: Optional[date] = None
    min_expiry_map: Dict[Tuple[str, date], date] = {}

    for p in pickle_paths:
        try:
            raw = pd.read_pickle(p)
            if not isinstance(raw, pd.DataFrame) or raw.empty:
                continue

            d = _normalize_dhan_df(raw, os.path.basename(p))
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

            print(f"[PASS1 OK] {os.path.basename(p)} option_days={d['day'].nunique()}")

        except Exception as e:
            msg = f"[PASS1 WARN] {os.path.basename(p)} failed: {e}"
            if FAIL_ON_PICKLE_ERROR:
                raise RuntimeError(msg) from e
            print(msg)

    if max_day_seen is None or min_day_seen is None:
        raise RuntimeError("No usable option data found in pickles (PASS1) for tradeable underlyings.")

    return max_day_seen, min_expiry_map, min_day_seen


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
) -> Tuple[List[TradeRow], List[Dict[str, Any]]]:
    results: List[TradeRow] = []
    skipped: List[Dict[str, Any]] = []

    idx_all = build_minute_index(dy, SESSION_START_IST, SESSION_END_IST)
    session_end_ts = idx_all[-1]

    qty = int(QTY_UNITS[und])
    step = int(STRIKE_STEP[und])

    G = float(PROFIT_PROTECT_TRIGGER_RUPEES)
    profit_protect_enabled = G > 0

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

        ce_close_raw = _build_leg_series_fixed_strike(day_opt, idx_all, atm, "CE", "close_f", do_ffill=False)
        pe_close_raw = _build_leg_series_fixed_strike(day_opt, idx_all, atm, "PE", "close_f", do_ffill=False)

        ce_close = ce_close_raw.ffill()
        pe_close = pe_close_raw.ffill()

        ce_high = _build_leg_series_fixed_strike(day_opt, idx_all, atm, "CE", "high_f", do_ffill=False)
        ce_low  = _build_leg_series_fixed_strike(day_opt, idx_all, atm, "CE", "low_f", do_ffill=False)
        pe_high = _build_leg_series_fixed_strike(day_opt, idx_all, atm, "PE", "high_f", do_ffill=False)
        pe_low  = _build_leg_series_fixed_strike(day_opt, idx_all, atm, "PE", "low_f", do_ffill=False)

        ce_entry = ce_close_raw.loc[cur_entry_ts]
        pe_entry = pe_close_raw.loc[cur_entry_ts]

        if pd.isna(ce_entry) or pd.isna(pe_entry):
            skipped.append({
                "day": dy, "underlying": und, "expiry": expiry, "trade_seq": trade_seq,
                "atm_strike": atm,
                "source_pickle": source_pickle,
                "reason": "No CE/PE price at entry"
            })
            break

        if STRICT_STRIKE_PRESENCE:
            ce_post = ce_close_raw.loc[cur_entry_ts:]
            pe_post = pe_close_raw.loc[cur_entry_ts:]
            max_miss = max(_missing_streak_minutes(ce_post), _missing_streak_minutes(pe_post))
            if max_miss > MAX_MISSING_STREAK_MIN:
                skipped.append({
                    "day": dy, "underlying": und, "expiry": expiry, "trade_seq": trade_seq,
                    "atm_strike": atm,
                    "source_pickle": source_pickle,
                    "reason": f"Strike series missing too much after entry (max_missing_streak={max_miss}m)"
                })
                break

        monitor_start_ts = pd.Timestamp(cur_entry_ts) + pd.Timedelta(minutes=1)
        if monitor_start_ts > session_end_ts:
            break

        pnl_close_all = (float(ce_entry) - ce_close) * qty + (float(pe_entry) - pe_close) * qty
        pnl = pnl_close_all.loc[monitor_start_ts:].dropna()

        pnl_ceHigh_peLow_all = (float(ce_entry) - ce_high) * qty + (float(pe_entry) - pe_low) * qty
        pnl_ceLow_peHigh_all = (float(ce_entry) - ce_low) * qty + (float(pe_entry) - pe_high) * qty

        pnl_sl_all = pd.concat([pnl_close_all, pnl_ceHigh_peLow_all, pnl_ceLow_peHigh_all], axis=1).min(axis=1)
        pnl_sl = pnl_sl_all.loc[monitor_start_ts:].dropna()

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

        stop_hit = pnl_sl <= -LOSS_LIMIT_RUPEES
        stop_ts = pnl_sl.index[stop_hit.to_numpy().argmax()] if stop_hit.any() else None

        protect_ts = None
        if profit_protect_enabled:
            peak = pnl.cummax()
            armed = peak >= G
            trail = peak - G
            protect_hit = armed & (pnl <= trail)
            protect_ts = pnl.index[protect_hit.to_numpy().argmax()] if protect_hit.any() else None

        exit_ts = eod_ts
        exit_reason = "EOD"
        if stop_ts is not None and protect_ts is not None:
            if stop_ts < protect_ts:
                exit_ts, exit_reason = stop_ts, "STOPLOSS"
            elif protect_ts < stop_ts:
                exit_ts, exit_reason = protect_ts, "PROFIT_PROTECT"
            else:
                exit_ts, exit_reason = stop_ts, "STOPLOSS"
        elif stop_ts is not None:
            exit_ts, exit_reason = stop_ts, "STOPLOSS"
        elif protect_ts is not None:
            exit_ts, exit_reason = protect_ts, "PROFIT_PROTECT"

        if exit_reason == "STOPLOSS":
            exit_pnl_gross = -float(LOSS_LIMIT_RUPEES)
        else:
            exit_pnl_gross = float(pnl.loc[exit_ts])

        exit_ce = float(ce_close.loc[exit_ts]) if pd.notna(ce_close.loc[exit_ts]) else float("nan")
        exit_pe = float(pe_close.loc[exit_ts]) if pd.notna(pe_close.loc[exit_ts]) else float("nan")

        txn_charges = compute_trade_charges(
            entry_ce=float(ce_entry), entry_pe=float(pe_entry),
            exit_ce=exit_ce if not pd.isna(exit_ce) else 0.0,
            exit_pe=exit_pe if not pd.isna(exit_pe) else 0.0,
            qty=qty,
        )
        exit_pnl = exit_pnl_gross - txn_charges

        dte = int((expiry - dy).days)

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

        if exit_reason in ("STOPLOSS", "PROFIT_PROTECT") and (trade_seq - 1) < MAX_REATTEMPTS:
            trade_seq += 1
            cur_entry_ts = pd.Timestamp(exit_ts) + pd.Timedelta(minutes=REENTRY_DELAY_MINUTES)
            if cur_entry_ts > session_end_ts:
                break
            continue

        break

    return results, skipped

# =============================================================================
# PER-PICKLE PROCESSOR (each pickle is an independent unit)
# =============================================================================
def process_one_pickle(
    p: str,
    min_expiry_map: Dict[Tuple[str, date], date],
    processed_day_keys: set,
    window_start: date,
    window_end: date,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
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

    trades_out: List[Dict[str, Any]] = []
    skipped_out: List[Dict[str, Any]] = []

    for (und, dy, ex), g in d.groupby(["underlying", "day", "expiry"], sort=False):
        key_ud = (und, dy)
        if key_ud not in min_expiry_map:
            continue
        if min_expiry_map[key_ud] != ex:
            continue

        day_key = (und, dy, ex)
        if day_key in processed_day_keys:
            skipped_out.append({
                "day": dy,
                "underlying": und,
                "expiry": ex,
                "source_pickle": src,
                "reason": "Duplicate (underlying, day, expiry) encountered in multiple pickles; skipped to avoid double-count"
            })
            continue

        processed_day_keys.add(day_key)

        trades, skips = simulate_day_multi_trades_dhan(
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

def pick_actual_underlying_by_day(min_expiry_map: Dict[Tuple[str, date], date]) -> Dict[date, str]:
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
# ACTUAL TRADES (one underlying/day selection)
# =============================================================================
def build_actual_trades_df(all_trades_df: pd.DataFrame, min_expiry_map: Dict[Tuple[str, date], date]) -> pd.DataFrame:
    if all_trades_df.empty:
        return pd.DataFrame()

    actual_underlying = pick_actual_underlying_by_day(min_expiry_map)

    m = all_trades_df.copy()
    m["actual_underlying_for_day"] = m["day"].map(actual_underlying)

    m = m[m["actual_underlying_for_day"].notna()]
    m = m[m["underlying"] == m["actual_underlying_for_day"]]
    m = m[m["days_to_expiry"].isin([0, 1])]

    m = m.drop(columns=["actual_underlying_for_day"])
    m = m.sort_values(["day", "trade_seq"]).reset_index(drop=True)

    m["is_exit_pnl_positive"] = (m["exit_pnl"] > 0).astype(int)
    return m


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

def write_excel(all_trades_df: pd.DataFrame, actual_trades_df: pd.DataFrame, skipped_df: pd.DataFrame) -> None:
    out_dir = os.path.dirname(os.path.abspath(OUTPUT_XLSX))
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    piv_exit = pd.DataFrame()
    piv_eod_first = pd.DataFrame()
    monthwise_summary = pd.DataFrame()

    if not all_trades_df.empty:
        piv_exit = all_trades_df.pivot_table(
            index="day", columns="underlying", values="exit_pnl", aggfunc="sum"
        ).reset_index()

        first = all_trades_df[all_trades_df["trade_seq"] == 1]
        piv_eod_first = first.pivot_table(
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
    else:
        instrument_summary = pd.DataFrame()

    if not actual_trades_df.empty:
        tmp = actual_trades_df.copy()
        tmp["month"] = pd.to_datetime(tmp["day"]).dt.to_period("M").astype(str)

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

    print(f"[INFO] Pickles found: {len(paths)}")

    data_end_day, min_expiry_map, min_day_seen = scan_pickles_pass1_dhan(paths)
    end_day = date.today() if WINDOW_END_MODE == "today" else data_end_day
    window_start = compute_window_start(end_day, LOOKBACK_MONTHS)

    print(f"[INFO] Data day-range seen: {min_day_seen} -> {data_end_day}")
    print(f"[INFO] WindowEndMode={WINDOW_END_MODE} | Effective window: {window_start} -> {end_day}")
    print(f"[INFO] Stoploss: -{LOSS_LIMIT_RUPEES} | ProfitProtect giveback: {PROFIT_PROTECT_TRIGGER_RUPEES} | Re-entry delay min: {REENTRY_DELAY_MINUTES}")
    print(f"[INFO] Tradeables: {sorted(TRADEABLE)}")
    print(f"[INFO] Output: {OUTPUT_XLSX}")

    all_trades_list: List[pd.DataFrame] = []
    skipped_list: List[pd.DataFrame] = []
    processed_day_keys: set = set()

    for p in paths:
        try:
            tdf, sdf = process_one_pickle(
                p=p,
                min_expiry_map=min_expiry_map,
                processed_day_keys=processed_day_keys,
                window_start=window_start,
                window_end=end_day,
            )

            if tdf is not None and not tdf.empty:
                all_trades_list.append(tdf)

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
    skipped_df = pd.concat(skipped_list, ignore_index=True) if skipped_list else pd.DataFrame()

    if not all_trades_df.empty:
        all_trades_df = all_trades_df.sort_values(
            ["day", "underlying", "trade_seq", "source_pickle"]
        ).reset_index(drop=True)

    actual_trades_df = build_actual_trades_df(all_trades_df, min_expiry_map)

    write_excel(all_trades_df, actual_trades_df, skipped_df)

    if not all_trades_df.empty:
        print(all_trades_df.groupby("underlying")[["exit_pnl"]].describe())
    else:
        print("[WARN] No completed trades. Check 'skipped' sheet for reasons.")


if __name__ == "__main__":
    main()