"""
Directional-strategy backtest on ACTUAL option premiums.
=========================================================

Run LOCALLY (needs Kite for the underlying minute series + your option pickles):

    python backtest_directional_options.py

What it does
------------
For each trading day it:
  1. Builds the NIFTY/SENSEX underlying 1-min series (same as your straddle script).
  2. Runs OUR directional signal on the UNDERLYING (EMA-fan + ADX + breakout
     entry; tiered-trail / breakeven / no-progress / optional profit-lock exit).
  3. When a signal fires, BUYS a single ITM option one strike inside the money:
        up signal   -> buy CE at (ATM - 1 step)   [ITM call]
        down signal -> buy PE at (ATM + 1 step)    [ITM put]
     and tracks the ACTUAL premium to compute real rupee P&L.
  4. Exits the option when the underlying signal says exit (or stop/eod), priced
     on the real premium, then deducts Zerodha charges for a 2-leg option trade
     (buy entry + sell exit).

Why ITM: a ~1-strike ITM option has higher delta (~0.6-0.7), so it tracks the
underlying move more closely than ATM/OTM, which suits a directional bet and
reduces the share of premium that is pure time value.

Reference: data schema, IST handling, strike resolution and the charge
components are taken from atm_straddle_prem_jump_reattempt.py. The straddle
logic itself is NOT reused -- this is a single-leg directional trade, so the
cost model is reduced from 4 orders to 2 (STT on the SELL/exit side for a long
option, which is where STT applies for option buyers).
"""

import os
import glob
import time
from dataclasses import dataclass, asdict
from datetime import datetime, date, time as dtime, timedelta
from typing import Dict, List, Tuple, Optional, Any

import numpy as np
import pandas as pd

import Trading_2024.OptionTradeUtils as oUtils

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None
try:
    import pytz
except Exception:
    pytz = None
try:
    from dateutil.relativedelta import relativedelta
except Exception:
    relativedelta = None


# =============================================================================
# CONFIG
# =============================================================================
PICKLES_DIR = r"G:\My Drive\Trading\Historical_Options_Data"
OUTPUT_XLSX = os.path.join(os.path.expanduser("~"), "Downloads",
                           "directional_options_backtest.xlsx")

LOOKBACK_MONTHS = int(os.getenv("LOOKBACK_MONTHS", "12"))
SESSION_START_IST = dtime(9, 15)
SESSION_END_IST = dtime(15, 30)
SQUAREOFF_IST = dtime(15, 20)            # force-exit any open position by here

QTY_UNITS = {"NIFTY": 325, "SENSEX": 100}  # 1 lot. Adjust to your lot size.
TRADEABLE = set(QTY_UNITS.keys())
STRIKE_STEP = {"NIFTY": 50, "SENSEX": 100}
ITM_STEPS = 1                             # how many strikes ITM to buy (ATM-+1)

UNDERLYING_KITE = {
    "NIFTY": {"exchange": "NSE", "tradingsymbol": "NIFTY 50"},
    "SENSEX": {"exchange": "BSE", "tradingsymbol": "SENSEX"},
}

# ---- Strategy parameters (walk-forward optimized on 5yr NIFTY index, 2021-2026) ----
# Consensus from 7 OOS windows: EMA_FAST=9 (100%), MID=21 (71%), SLOW=50 (best
# performer of the 50/60 split), fan=0.0008 (100%), breakout=20 (71%),
# ADX=25 (best of 20/25 split), trail aggr=1.0 (86%). OOS: 40/42 months green.
EMA_FAST, EMA_MID, EMA_SLOW = 9, 21, 50
SLOPE_LOOKBACK = 5
BREAKOUT_LOOKBACK = 20          # was 30; optimizer chose 20 in 71% of windows
ATR_PERIOD = 14
TRAIL_TIERS = [(0.0100, 1.2), (0.0060, 1.6), (0.0035, 2.2), (0.0000, 3.0)]  # trail_aggr=1.0
MAX_LOSS_PCT = 0.004
MIN_TREND_BARS = 10
COOLDOWN_BARS = 5
ADX_PERIOD = 14
MIN_ADX = 25.0                 # was 20; stricter trend filter performed better OOS
MIN_FAN_PCT = 0.0008
ATR_EXPANSION = 1.10
ATR_EXP_LOOKBACK = 30
PROGRESS_BARS = 8
MIN_PROGRESS_PCT = 0.0010
BREAKEVEN_AFTER_PCT = 0.0015
# profit-lock OFF by default (testing showed it reduced index-level returns).
PROFIT_LOCK_TRIGGER_PTS = 1e9
PROFIT_LOCK_FLOOR_PTS = 5.0

# =============================================================================
# TRANSACTION CHARGES (Zerodha F&O Options, single long option = 2 orders)
# =============================================================================
# Buyer of an option: STT applies on the SELL side (exit) at 0.1% in current
# rules for options is on premium of sell side; we mirror the reference file's
# rates. Two executed orders: buy (entry) + sell (exit).
BROKERAGE_PER_ORDER = 20.0
ORDERS_PER_TRADE = 2
STT_SELL_PCT = 0.001        # 0.1% on sell-side (exit) premium for option seller side
EXCHANGE_TXN_PCT = 0.0003553
SEBI_PER_CRORE = 10.0
STAMP_BUY_PCT = 0.00003     # buy side (entry)
IPFT_PER_CRORE = 0.010
GST_PCT = 0.18
INCLUDE_TRANSACTION_COSTS = True

MAX_DAYS_PER_CHUNK = 25
MAX_ATTEMPTS = 5
SLEEP_BETWEEN_CALLS_SEC = 0.20
FAIL_ON_PICKLE_ERROR = os.getenv("FAIL_ON_PICKLE_ERROR", "0").strip() == "1"


# =============================================================================
# IST / TIME HELPERS  (from reference)
# =============================================================================
def ist_tz():
    if ZoneInfo is not None:
        return ZoneInfo("Asia/Kolkata")
    if pytz is not None:
        return pytz.timezone("Asia/Kolkata")
    return "Asia/Kolkata"


def ensure_ist(series_or_scalar):
    tz = ist_tz()
    dt = pd.to_datetime(series_or_scalar, errors="coerce")
    if isinstance(dt, pd.Series):
        return dt.dt.tz_localize(tz) if dt.dt.tz is None else dt.dt.tz_convert(tz)
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


def build_minute_index(day_d: date) -> pd.DatetimeIndex:
    tz = ist_tz()
    start = pd.Timestamp(datetime.combine(day_d, SESSION_START_IST), tz=tz)
    end = pd.Timestamp(datetime.combine(day_d, SESSION_END_IST), tz=tz)
    return pd.date_range(start=start, end=end, freq="1min")


def compute_window_start(end_day: date, months: int) -> date:
    if relativedelta is not None:
        return (pd.Timestamp(end_day) - relativedelta(months=months)).date()
    return (pd.Timestamp(end_day) - pd.Timedelta(days=30 * months)).date()


# =============================================================================
# TRANSACTION COST  (single long option, 2 orders)
# =============================================================================
def compute_trade_charges(entry_prem: float, exit_prem: float, qty: int) -> float:
    if not INCLUDE_TRANSACTION_COSTS:
        return 0.0
    entry_turnover = entry_prem * qty      # buy side
    exit_turnover = exit_prem * qty        # sell side
    total_turnover = entry_turnover + exit_turnover

    brokerage = BROKERAGE_PER_ORDER * ORDERS_PER_TRADE
    stt = exit_turnover * STT_SELL_PCT                 # STT on sell (exit) for option
    txn = total_turnover * EXCHANGE_TXN_PCT
    sebi = total_turnover * SEBI_PER_CRORE / 1_00_00_000
    stamp = entry_turnover * STAMP_BUY_PCT             # stamp on buy (entry)
    ipft = total_turnover * IPFT_PER_CRORE / 1_00_00_000
    gst = (brokerage + txn + sebi) * GST_PCT
    return round(brokerage + stt + txn + sebi + stamp + ipft + gst, 2)


# =============================================================================
# KITE underlying download  (from reference, condensed)
# =============================================================================
def _iter_chunks(from_dt, to_dt, days):
    out = []
    cur, end_d = from_dt.date(), to_dt.date()
    while cur <= end_d:
        ce = min(cur + timedelta(days=days - 1), end_d)
        cf = from_dt if cur == from_dt.date() else datetime.combine(cur, SESSION_START_IST)
        ct = to_dt if ce == end_d else datetime.combine(ce, SESSION_END_IST)
        out.append((cf, ct))
        cur = ce + timedelta(days=1)
    return out


def _instruments_cached(kite, exchange, cache):
    ex = exchange.upper().strip()
    if ex not in cache:
        cache[ex] = kite.instruments(ex)
    return cache[ex]


def _token(kite, exchange, tradingsymbol, cache):
    wanted = tradingsymbol.strip().upper()
    for r in _instruments_cached(kite, exchange, cache):
        if str(r.get("tradingsymbol", "")).upper() == wanted:
            return int(r["instrument_token"])
    raise ValueError(f"Instrument not found on {exchange}: {tradingsymbol}")


def _fetch_minute(kite, token, from_dt, to_dt, label):
    rows_all = []
    for i, (cf, ct) in enumerate(_iter_chunks(from_dt, to_dt, MAX_DAYS_PER_CHUNK), 1):
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                rows_all.extend(kite.historical_data(token, cf, ct, "minute", False, False))
                break
            except Exception as e:
                if attempt == MAX_ATTEMPTS:
                    print(f"[ERROR] {label} chunk {i} failed: {e}")
                time.sleep(min(8.0, 1.5 * attempt))
        time.sleep(SLEEP_BETWEEN_CALLS_SEC)
    return rows_all


def download_underlyings(kite, day_start, day_end):
    cache = {}
    fdt = datetime.combine(day_start, SESSION_START_IST)
    tdt = datetime.combine(day_end, SESSION_END_IST)
    out = {}
    for und, meta in UNDERLYING_KITE.items():
        if und not in TRADEABLE:
            continue
        tok = _token(kite, meta["exchange"], meta["tradingsymbol"], cache)
        rows = _fetch_minute(kite, tok, fdt, tdt, f"{meta['exchange']}:{meta['tradingsymbol']}")
        if not rows:
            out[und] = pd.DataFrame(columns=["date", "open", "high", "low", "close"])
            continue
        df = pd.DataFrame(rows)
        df["date"] = ensure_ist(df["date"])
        df = df.drop_duplicates("date").sort_values("date").reset_index(drop=True)
        df["day"] = df["date"].dt.tz_convert(ist_tz()).dt.date
        out[und] = df
        print(f"[UNDERLYING OK] {und}: {len(df)} candles, {df['day'].nunique()} days")
    return out


# =============================================================================
# SIGNAL INDICATORS on the underlying (per day)
# =============================================================================
def add_indicators(g: pd.DataFrame) -> pd.DataFrame:
    g = g.copy()
    c = g["close"]
    g["ema_f"] = c.ewm(span=EMA_FAST, adjust=False).mean()
    g["ema_m"] = c.ewm(span=EMA_MID, adjust=False).mean()
    g["ema_s"] = c.ewm(span=EMA_SLOW, adjust=False).mean()
    g["slope_s"] = g["ema_s"].diff(SLOPE_LOOKBACK)
    prev_c = c.shift(1)
    tr = pd.concat([g["high"] - g["low"], (g["high"] - prev_c).abs(),
                    (g["low"] - prev_c).abs()], axis=1).max(axis=1)
    g["atr"] = tr.ewm(span=ATR_PERIOD, adjust=False).mean()
    g["roll_hi"] = g["high"].rolling(BREAKOUT_LOOKBACK).max()
    g["roll_lo"] = g["low"].rolling(BREAKOUT_LOOKBACK).min()
    g["fan_pct"] = (g["ema_f"] - g["ema_s"]).abs() / c
    g["atr_avg"] = g["atr"].rolling(ATR_EXP_LOOKBACK).mean()
    g["atr_ratio"] = g["atr"] / g["atr_avg"]
    up_move = g["high"].diff()
    down_move = -g["low"].diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    atr_w = tr.ewm(alpha=1 / ADX_PERIOD, adjust=False).mean()
    pdi = 100 * pd.Series(plus_dm, index=g.index).ewm(alpha=1 / ADX_PERIOD, adjust=False).mean() / atr_w
    mdi = 100 * pd.Series(minus_dm, index=g.index).ewm(alpha=1 / ADX_PERIOD, adjust=False).mean() / atr_w
    dx = 100 * (pdi - mdi).abs() / (pdi + mdi).replace(0, np.nan)
    g["adx"] = dx.ewm(alpha=1 / ADX_PERIOD, adjust=False).mean()
    return g


# =============================================================================
# OPTION premium series builder (per day, one strike+type)
# =============================================================================
def build_leg(day_opt: pd.DataFrame, idx_all, strike: int, opt_type: str, col: str, ffill: bool):
    sub = day_opt[(day_opt["strike_int"] == strike) & (day_opt["option_type"] == opt_type)][["date", col]].dropna()
    if sub.empty:
        return pd.Series(index=idx_all, dtype="float64")
    sub = sub.copy()
    sub["date"] = ensure_ist(sub["date"])
    sub = sub.sort_values("date").drop_duplicates("date", keep="last").set_index("date")
    s = sub[col].astype(float).reindex(idx_all)
    return s.ffill() if ffill else s


# =============================================================================
# Per-day directional simulation on underlying, priced on option premium
# =============================================================================
@dataclass
class OptTrade:
    day: date
    underlying: str
    direction: str
    option_type: str
    strike: int
    qty: int
    entry_time: str
    exit_time: str
    exit_reason: str
    entry_underlying: float
    exit_underlying: float
    underlying_pts: float
    entry_premium: float
    exit_premium: float
    premium_move: float
    gross_pnl: float
    charges: float
    net_pnl: float
    peak_premium: float
    peak_gross_pnl: float
    bars_held: int


def simulate_day(und, dy, expiry, day_opt, uday) -> List[OptTrade]:
    idx_all = build_minute_index(dy)
    g = uday[uday["day"] == dy].copy()
    if len(g) < BREAKOUT_LOOKBACK + EMA_SLOW:
        return []
    g["date"] = ensure_ist(g["date"])
    g = g.sort_values("date").drop_duplicates("date").reset_index(drop=True)
    g = add_indicators(g).reset_index(drop=True)
    n = len(g)
    step = STRIKE_STEP[und]
    qty = QTY_UNITS[und]
    trades: List[OptTrade] = []

    i = BREAKOUT_LOOKBACK + SLOPE_LOOKBACK
    last_exit = -10_000
    sq_ts = pd.Timestamp(datetime.combine(dy, SQUAREOFF_IST), tz=ist_tz())

    while i < n - 1:
        row = g.iloc[i]
        if i - last_exit < COOLDOWN_BARS:
            i += 1; continue
        if pd.isna(row["slope_s"]) or pd.isna(row["roll_hi"]) or pd.isna(row["adx"]) or pd.isna(row["atr_ratio"]):
            i += 1; continue
        regime = (row["adx"] >= MIN_ADX) and (row["fan_pct"] >= MIN_FAN_PCT) and (row["atr_ratio"] >= ATR_EXPANSION)
        up = regime and (row["ema_f"] > row["ema_m"] > row["ema_s"]) and (row["slope_s"] > 0) and (row["close"] >= g.iloc[i - 1]["roll_hi"])
        down = regime and (row["ema_f"] < row["ema_m"] < row["ema_s"]) and (row["slope_s"] < 0) and (row["close"] <= g.iloc[i - 1]["roll_lo"])
        if not (up or down):
            i += 1; continue

        direction = "up" if up else "down"
        # ITM strike selection
        atm = round_to_step(float(row["close"]), step)
        if direction == "up":
            opt_type, strike = "CE", atm - ITM_STEPS * step    # ITM call
        else:
            opt_type, strike = "PE", atm + ITM_STEPS * step    # ITM put

        prem_close = build_leg(day_opt, idx_all, strike, opt_type, "close", ffill=True)
        entry_ts = ensure_ist(row["date"])
        if entry_ts not in prem_close.index or pd.isna(prem_close.loc[entry_ts]):
            i += 1; continue
        entry_prem = float(prem_close.loc[entry_ts])
        if entry_prem <= 0:
            i += 1; continue

        entry_px = row["close"]
        extreme = entry_px
        peak_prem = entry_prem
        be_armed = False
        lock_armed = False
        exit_idx, exit_reason = None, None

        j = i + 1
        while j < n:
            r = g.iloc[j]
            held = j - i
            ts = ensure_ist(r["date"])
            if ts >= sq_ts:
                exit_idx, exit_reason = j, "session_squareoff"; break
            if direction == "up":
                extreme = max(extreme, r["high"])
                fav = (extreme - entry_px) / entry_px
            else:
                extreme = min(extreme, r["low"])
                fav = (entry_px - extreme) / entry_px
            if fav >= BREAKEVEN_AFTER_PCT:
                be_armed = True
            atr_mult = next(m for thr, m in TRAIL_TIERS if fav >= thr)
            if direction == "up":
                trail = extreme - atr_mult * r["atr"]
                fav_pts = extreme - entry_px
                if fav_pts >= PROFIT_LOCK_TRIGGER_PTS:
                    lock_armed = True
                if held >= PROGRESS_BARS and fav < MIN_PROGRESS_PCT:
                    exit_idx, exit_reason = j, "no_progress"; break
                if lock_armed and r["low"] <= entry_px + PROFIT_LOCK_FLOOR_PTS:
                    exit_idx, exit_reason = j, "profit_lock"; break
                if be_armed and r["close"] <= entry_px:
                    exit_idx, exit_reason = j, "breakeven"; break
                if r["close"] <= entry_px * (1 - MAX_LOSS_PCT):
                    exit_idx, exit_reason = j, "max_loss"; break
                if r["close"] < trail:
                    exit_idx, exit_reason = j, "trail_stop"; break
                if r["ema_f"] < r["ema_m"]:
                    exit_idx, exit_reason = j, "ema_break"; break
            else:
                trail = extreme + atr_mult * r["atr"]
                fav_pts = entry_px - extreme
                if fav_pts >= PROFIT_LOCK_TRIGGER_PTS:
                    lock_armed = True
                if held >= PROGRESS_BARS and fav < MIN_PROGRESS_PCT:
                    exit_idx, exit_reason = j, "no_progress"; break
                if lock_armed and r["high"] >= entry_px - PROFIT_LOCK_FLOOR_PTS:
                    exit_idx, exit_reason = j, "profit_lock"; break
                if be_armed and r["close"] >= entry_px:
                    exit_idx, exit_reason = j, "breakeven"; break
                if r["close"] >= entry_px * (1 + MAX_LOSS_PCT):
                    exit_idx, exit_reason = j, "max_loss"; break
                if r["close"] > trail:
                    exit_idx, exit_reason = j, "trail_stop"; break
                if r["ema_f"] > r["ema_m"]:
                    exit_idx, exit_reason = j, "ema_break"; break
            # track option peak for reporting
            pts = ensure_ist(r["date"])
            if pts in prem_close.index and not pd.isna(prem_close.loc[pts]):
                peak_prem = max(peak_prem, float(prem_close.loc[pts]))
            j += 1

        if exit_idx is None:
            exit_idx, exit_reason = n - 1, "data_end"

        ex = g.iloc[exit_idx]
        bars = exit_idx - i
        if bars < MIN_TREND_BARS:
            last_exit = exit_idx
            i = exit_idx + 1
            continue

        exit_ts = ensure_ist(ex["date"])
        exit_prem = float(prem_close.loc[exit_ts]) if exit_ts in prem_close.index and not pd.isna(prem_close.loc[exit_ts]) else entry_prem
        # long option: profit = (exit_prem - entry_prem) * qty
        gross = (exit_prem - entry_prem) * qty
        charges = compute_trade_charges(entry_prem, exit_prem, qty)
        net = gross - charges
        u_pts = (ex["close"] - entry_px) if direction == "up" else (entry_px - ex["close"])

        trades.append(OptTrade(
            day=dy, underlying=und, direction=direction, option_type=opt_type, strike=strike, qty=qty,
            entry_time=entry_ts.strftime("%H:%M"), exit_time=exit_ts.strftime("%H:%M"), exit_reason=exit_reason,
            entry_underlying=round(entry_px, 2), exit_underlying=round(float(ex["close"]), 2),
            underlying_pts=round(u_pts, 2),
            entry_premium=round(entry_prem, 2), exit_premium=round(exit_prem, 2),
            premium_move=round(exit_prem - entry_prem, 2),
            gross_pnl=round(gross, 2), charges=round(charges, 2), net_pnl=round(net, 2),
            peak_premium=round(peak_prem, 2), peak_gross_pnl=round((peak_prem - entry_prem) * qty, 2),
            bars_held=bars,
        ))
        last_exit = exit_idx
        i = exit_idx + 1

    return trades


# =============================================================================
# PICKLE PASS 1: nearest expiry per (underlying, day)
# =============================================================================
def scan_pass1(paths):
    max_day = min_day = None
    min_expiry = {}
    for p in paths:
        try:
            df = pd.read_pickle(p)
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue
            for c in ("date", "name", "expiry", "type"):
                if c not in df.columns:
                    raise ValueError(f"missing {c}")
            d2 = df[df["type"].astype(str).str.upper().eq("OPTION")][["date", "name", "expiry"]].copy()
            if d2.empty:
                continue
            d2["date"] = ensure_ist(d2["date"])
            d2["day"] = d2["date"].dt.date
            d2["underlying"] = d2["name"].astype(str).map(normalize_underlying)
            d2["expiry_date"] = pd.to_datetime(d2["expiry"], errors="coerce").dt.date
            d2 = d2.dropna(subset=["underlying", "day", "expiry_date"])
            d2 = d2[d2["underlying"].isin(TRADEABLE) & (d2["expiry_date"] >= d2["day"])]
            if d2.empty:
                continue
            mx, mn = d2["day"].max(), d2["day"].min()
            max_day = mx if max_day is None or mx > max_day else max_day
            min_day = mn if min_day is None or mn < min_day else min_day
            for (und, dy), ex in d2.groupby(["underlying", "day"])["expiry_date"].min().items():
                k = (und, dy)
                if k not in min_expiry or ex < min_expiry[k]:
                    min_expiry[k] = ex
            print(f"[PASS1] {os.path.basename(p)} days={d2['day'].nunique()}")
        except Exception as e:
            if FAIL_ON_PICKLE_ERROR:
                raise
            print(f"[PASS1 WARN] {os.path.basename(p)}: {e}")
    if max_day is None:
        raise RuntimeError("No usable option data in pickles.")
    return max_day, min_expiry, min_day


# =============================================================================
# Per-day underlying selection: trade the index CLOSEST TO EXPIRY that day.
# min_expiry maps (underlying, day) -> nearest expiry date for that underlying.
# For each day we compute days-to-expiry per available underlying and keep the
# smallest. Ties break to NIFTY (mirrors the reference script's preference).
# Only days where the chosen underlying is 0- or 1-DTE are traded, matching how
# you actually trade the near-expiry instrument.
# =============================================================================
MAX_DTE_TO_TRADE = int(os.getenv("MAX_DTE_TO_TRADE", "1"))  # trade only 0/1 DTE


def pick_underlying_by_day(min_expiry: Dict[Tuple[str, date], date]) -> Dict[date, str]:
    by_day: Dict[date, List[Tuple[int, int, str]]] = {}
    for (und, dy), ex in min_expiry.items():
        if und not in TRADEABLE:
            continue
        dte = (ex - dy).days
        if dte < 0 or dte > MAX_DTE_TO_TRADE:
            continue
        pref = 0 if und == "NIFTY" else 1   # tie-break preference
        by_day.setdefault(dy, []).append((dte, pref, und))
    out: Dict[date, str] = {}
    for dy, lst in by_day.items():
        lst.sort()                          # smallest DTE, then NIFTY first
        out[dy] = lst[0][2]
    return out


# =============================================================================
# PICKLE PASS 2: generate trades
# =============================================================================
def process_pickles(paths, min_expiry, underlying_data, w_start, w_end, day_underlying):
    all_trades, skipped = [], []
    processed = set()
    for p in paths:
        try:
            df = pd.read_pickle(p)
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue
            need = ["date", "name", "type", "option_type", "strike", "expiry", "instrument", "high", "low", "close"]
            miss = [c for c in need if c not in df.columns]
            if miss:
                raise ValueError(f"missing {miss}")
            d2 = df[df["type"].astype(str).str.upper().eq("OPTION")][need].copy()
            if d2.empty:
                continue
            d2["date"] = ensure_ist(d2["date"])
            d2["day"] = d2["date"].dt.date
            d2["underlying"] = d2["name"].astype(str).map(normalize_underlying)
            d2 = d2[d2["underlying"].isin(TRADEABLE)]
            if d2.empty:
                continue
            d2["expiry_date"] = pd.to_datetime(d2["expiry"], errors="coerce").dt.date
            d2["strike_int"] = pd.to_numeric(d2["strike"], errors="coerce").round().astype("Int64")
            d2["option_type"] = d2["option_type"].astype(str).str.upper()
            d2 = d2.dropna(subset=["day", "underlying", "expiry_date", "strike_int", "close"])
            d2["strike_int"] = d2["strike_int"].astype(int)
            d2 = d2[(d2["expiry_date"] >= d2["day"]) & (d2["day"] >= w_start) & (d2["day"] <= w_end)]
            if d2.empty:
                continue
            for (und, dy, ex), gopt in d2.groupby(["underlying", "day", "expiry_date"], sort=False):
                # only trade the index selected for this day (closest to expiry)
                if day_underlying.get(dy) != und:
                    continue
                if min_expiry.get((und, dy)) != ex:
                    continue
                key = (und, dy, ex)
                if key in processed:
                    continue
                processed.add(key)
                uday = underlying_data.get(und)
                if uday is None or uday[uday["day"] == dy].empty:
                    skipped.append({"day": dy, "underlying": und, "reason": "underlying missing"})
                    continue
                trades = simulate_day(und, dy, ex, gopt, uday)
                all_trades.extend([asdict(t) for t in trades])
            print(f"[PASS2] {os.path.basename(p)} done")
        except Exception as e:
            if FAIL_ON_PICKLE_ERROR:
                raise
            print(f"[PASS2 WARN] {os.path.basename(p)}: {e}")
    return pd.DataFrame(all_trades), pd.DataFrame(skipped)


# =============================================================================
# OUTPUT
# =============================================================================
def write_output(trades_df: pd.DataFrame, skipped_df: pd.DataFrame):
    os.makedirs(os.path.dirname(os.path.abspath(OUTPUT_XLSX)), exist_ok=True)
    monthly = pd.DataFrame()
    by_underlying = pd.DataFrame()
    if not trades_df.empty:
        t = trades_df.copy()
        t["month"] = pd.to_datetime(t["day"]).dt.to_period("M").astype(str)
        t["win"] = t["net_pnl"] > 0
        monthly = t.groupby("month", as_index=False).agg(
            trades=("net_pnl", "count"),
            net_pnl=("net_pnl", "sum"),
            gross_pnl=("gross_pnl", "sum"),
            charges=("charges", "sum"),
            avg_net=("net_pnl", "mean"),
            win_rate_pct=("win", lambda s: round(100 * s.mean(), 1)),
        )
        by_underlying = t.groupby("underlying", as_index=False).agg(
            trades=("net_pnl", "count"),
            net_pnl=("net_pnl", "sum"),
            charges=("charges", "sum"),
            avg_net=("net_pnl", "mean"),
            win_rate_pct=("win", lambda s: round(100 * s.mean(), 1)),
        )
        print("\n=== MONTHLY (net of charges) ===")
        print(monthly.to_string(index=False))
        print("\n=== BY UNDERLYING ===")
        print(by_underlying.to_string(index=False))
        print("\n=== TOTALS (all traded instruments, near-expiry selection) ===")
        print(f"Trades: {len(t)}  Win rate: {100*t['win'].mean():.1f}%")
        print(f"Gross PnL: Rs {t['gross_pnl'].sum():,.0f}   Charges: Rs {t['charges'].sum():,.0f}   "
              f"Net PnL: Rs {t['net_pnl'].sum():,.0f}")
        print(f"Avg net/trade: Rs {t['net_pnl'].mean():,.0f}   Median: Rs {t['net_pnl'].median():,.0f}")
        prof_months = (monthly["net_pnl"] > 0).sum()
        print(f"Months profitable: {prof_months}/{len(monthly)}")

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as xw:
        if not trades_df.empty:
            trades_df.to_excel(xw, sheet_name="trades", index=False)
        else:
            pd.DataFrame({"note": ["no trades"]}).to_excel(xw, sheet_name="trades", index=False)
        if not monthly.empty:
            monthly.to_excel(xw, sheet_name="monthly_summary", index=False)
        else:
            pd.DataFrame({"note": ["no monthly data"]}).to_excel(xw, sheet_name="monthly_summary", index=False)
        if not by_underlying.empty:
            by_underlying.to_excel(xw, sheet_name="by_underlying", index=False)
        else:
            pd.DataFrame({"note": ["no data"]}).to_excel(xw, sheet_name="by_underlying", index=False)
        if not skipped_df.empty:
            skipped_df.to_excel(xw, sheet_name="skipped", index=False)
        else:
            pd.DataFrame({"note": ["nothing skipped"]}).to_excel(xw, sheet_name="skipped", index=False)
    print(f"\n[DONE] {OUTPUT_XLSX}")


# =============================================================================
# MAIN
# =============================================================================
def main():
    paths = sorted(glob.glob(os.path.join(PICKLES_DIR, "*.pkl")) +
                   glob.glob(os.path.join(PICKLES_DIR, "*.pickle")))
    if not paths:
        raise FileNotFoundError(f"No pickles in {PICKLES_DIR}")
    print(f"[INFO] {len(paths)} pickles")

    end_day, min_expiry, min_day = scan_pass1(paths)
    w_start = compute_window_start(end_day, LOOKBACK_MONTHS)
    print(f"[INFO] Data {min_day} -> {end_day}; window {w_start} -> {end_day}")

    # choose the index closest to expiry for each day (0/1 DTE), NIFTY on ties
    day_underlying = pick_underlying_by_day(min_expiry)
    from collections import Counter
    pick_counts = Counter(day_underlying.values())
    print(f"[INFO] Days selected by nearest expiry: {dict(pick_counts)}")

    kite = oUtils.intialize_kite_api()
    underlying_data = download_underlyings(kite, w_start, end_day)

    trades_df, skipped_df = process_pickles(paths, min_expiry, underlying_data, w_start, end_day, day_underlying)
    write_output(trades_df, skipped_df)


if __name__ == "__main__":
    main()
