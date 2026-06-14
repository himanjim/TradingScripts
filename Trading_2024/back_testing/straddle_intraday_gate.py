"""
straddle_intraday_gate.py  -  STANDALONE short-straddle backtester
==================================================================

Self-contained. Does NOT import the older backtester. Only external dependency
is the Kite API, used solely to pull the underlying's 1-minute series (needed to
read intraday trend direction). All option pricing comes from the local pickles.

THE MODEL (kept deliberately simple)
------------------------------------
Short ATM straddle is a MULTIPLE-TRY regime. Two intraday signals gate entry,
both computed only from candles up to the entry minute (no look-ahead, no
multi-day baselines):

  1. PREMIUM SWELLED  - the ATM straddle premium (CE+PE close) is RISING:
     premium(now) > premium(now - N candles), and also above the window's
     midpoint candle, so it's a genuine swell and not a one-bar spike. A swell
     means richer premium to sell into; such entries tend to survive longer.

  2. TREND IS SAFE    - avoid selling INTO a live directional move; only enter
     once it cools. Concretely:
         safe = flat/quiet  OR  (a trend was running AND the last 2 candles
                                  printed against that trend's direction)
     A trend currently running with NO reversal yet is BLOCKED. Flat/quiet days
     (no trend to fear) are allowed on the premium signal alone.

  ENTER  =  PREMIUM SWELLED  AND  TREND IS SAFE

MANAGEMENT (multiple-try)
-------------------------
On entry: sell ATM CE + ATM PE. Exit on per-trade stoploss (rupees), optional
profit-protect trail, or EOD. After a stoploss/profit-protect exit, wait a delay
and re-evaluate the gate; if it passes again, re-enter. Repeat up to a daily
re-entry cap. A daily loss cap stops all re-entries once the day is deep enough
in the red (this is the brake the earlier version lacked).

Edit the CONFIG block, then run:  python straddle_intraday_gate.py
"""

from __future__ import annotations

import os
import glob
import time
from dataclasses import dataclass, asdict
from datetime import datetime, date, time as dtime, timedelta
from typing import Dict, List, Tuple, Optional, Any

import pandas as pd

# Underlying series source (the ONLY external dependency).
import Trading_2024.OptionTradeUtils as oUtils

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None
try:
    import pytz
except Exception:
    pytz = None


# =============================================================================
# CONFIG
# =============================================================================
PICKLES_DIR = r"G:\My Drive\Trading\Historical_Options_Data"

ENTRY_TIME_IST   = os.getenv("ENTRY_TIME_IST", "10:00")   # first eligible entry
SESSION_START    = dtime(9, 15)
SESSION_END      = dtime(15, 30)

# --- Signal 1: premium swell ---
SWELL_LOOKBACK_N = int(os.getenv("SWELL_LOOKBACK_N", "5"))     # candles
SWELL_MIN_RISE_PCT = float(os.getenv("SWELL_MIN_RISE_PCT", "0.0"))  # min % rise vs N ago

# --- Signal 2: trend cooling ---
TREND_LOOKBACK_N = int(os.getenv("TREND_LOOKBACK_N", "10"))    # candles to judge a trend
TREND_MIN_RUN    = int(os.getenv("TREND_MIN_RUN", "4"))        # >= this many net same-dir
REVERSE_CANDLES  = int(os.getenv("REVERSE_CANDLES", "2"))      # candles against trend to allow
# A move is "directional" if net displacement over TREND_LOOKBACK_N exceeds this
# fraction of the window's total path (else it's chop = flat/quiet = safe).
TREND_DIRECTIONALITY = float(os.getenv("TREND_DIRECTIONALITY", "0.5"))

# --- Management ---
LOSS_LIMIT_RUPEES            = int(os.getenv("LOSS_LIMIT_RUPEES", "10000"))      # per-trade SL
PROFIT_PROTECT_TRIGGER       = int(os.getenv("PROFIT_PROTECT_TRIGGER", "10000")) # trail giveback; 0=off
MAX_REENTRIES                = int(os.getenv("MAX_REENTRIES", "3"))
REENTRY_DELAY_MIN            = int(os.getenv("REENTRY_DELAY_MIN", "10"))
DAILY_LOSS_CAP_RUPEES        = int(os.getenv("DAILY_LOSS_CAP_RUPEES", "20000"))  # stop day; 0=off

# --- Universe ---
QTY_UNITS   = {"NIFTY": 325, "SENSEX": 100}     # lot * lots; adjust to your sizing
STRIKE_STEP = {"NIFTY": 50, "SENSEX": 100}
TRADEABLE   = set(QTY_UNITS.keys())

UNDERLYING_KITE = {
    "NIFTY":  {"exchange": "NSE", "tradingsymbol": "NIFTY 50"},
    "SENSEX": {"exchange": "BSE", "tradingsymbol": "SENSEX"},
}

LOOKBACK_MONTHS = int(os.getenv("LOOKBACK_MONTHS", "12"))

# --- Transaction costs (Zerodha F&O options) ---
INCLUDE_TXN_COSTS   = True
BROKERAGE_PER_ORDER = 20.0
ORDERS_PER_TRADE    = 4
STT_SELL_PCT        = 0.001
EXCH_TXN_PCT        = 0.0003553
SEBI_PER_CRORE      = 10.0
STAMP_BUY_PCT       = 0.00003
IPFT_PER_CRORE      = 0.010
GST_PCT             = 0.18

# Kite fetch tuning
MAX_DAYS_PER_CHUNK = 25
MAX_ATTEMPTS = 5
SLEEP_BETWEEN_CALLS = 0.20

FAIL_ON_PICKLE_ERROR = os.getenv("FAIL_ON_PICKLE_ERROR", "0").strip() == "1"


def _downloads() -> str:
    from pathlib import Path
    d = Path.home() / "Downloads"
    return str(d if d.exists() else Path.home())


def _fname(s: str) -> str:
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in s)


OUTPUT_XLSX = os.getenv("OUTPUT_XLSX", os.path.join(
    _downloads(),
    f"straddle_intraday_gate_entry{_fname(ENTRY_TIME_IST)}"
    f"_swellN{SWELL_LOOKBACK_N}_rev{REVERSE_CANDLES}"
    f"_SL{LOSS_LIMIT_RUPEES}_DLC{DAILY_LOSS_CAP_RUPEES}.xlsx"
))


# =============================================================================
# TIME / TZ HELPERS
# =============================================================================
def ist_tz():
    if ZoneInfo is not None:
        return ZoneInfo("Asia/Kolkata")
    if pytz is not None:
        return pytz.timezone("Asia/Kolkata")
    return "Asia/Kolkata"


def ensure_ist(x) -> Any:
    tz = ist_tz()
    dt = pd.to_datetime(x, errors="coerce")
    if isinstance(dt, pd.Series):
        return dt.dt.tz_localize(tz) if dt.dt.tz is None else dt.dt.tz_convert(tz)
    if getattr(dt, "tzinfo", None) is None:
        return dt.tz_localize(tz)
    return dt.tz_convert(tz)


def parse_hhmm(s: str) -> dtime:
    hh, mm = s.strip().split(":")
    return dtime(int(hh), int(mm))


ENTRY_TIME = parse_hhmm(ENTRY_TIME_IST)


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


def minute_index(day_d: date, a: dtime, b: dtime) -> pd.DatetimeIndex:
    tz = ist_tz()
    return pd.date_range(pd.Timestamp(datetime.combine(day_d, a), tz=tz),
                         pd.Timestamp(datetime.combine(day_d, b), tz=tz), freq="1min")


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


def window_start(end_day: date, months: int) -> date:
    try:
        from dateutil.relativedelta import relativedelta
        return (pd.Timestamp(end_day) - relativedelta(months=months)).date()
    except Exception:
        return (pd.Timestamp(end_day) - pd.Timedelta(days=30 * months)).date()


# =============================================================================
# TRANSACTION COSTS
# =============================================================================
def trade_charges(entry_ce, entry_pe, exit_ce, exit_pe, qty) -> float:
    if not INCLUDE_TXN_COSTS:
        return 0.0
    entry_turn = (entry_ce + entry_pe) * qty
    exit_turn = (exit_ce + exit_pe) * qty
    total = entry_turn + exit_turn
    brokerage = BROKERAGE_PER_ORDER * ORDERS_PER_TRADE
    stt = entry_turn * STT_SELL_PCT
    txn = total * EXCH_TXN_PCT
    sebi = total * SEBI_PER_CRORE / 1_00_00_000
    stamp = exit_turn * STAMP_BUY_PCT
    ipft = total * IPFT_PER_CRORE / 1_00_00_000
    gst = (brokerage + txn + sebi) * GST_PCT
    return round(brokerage + stt + txn + sebi + stamp + ipft + gst, 2)


# =============================================================================
# KITE underlying download
# =============================================================================
def _chunks(from_dt, to_dt, days):
    cur, end_d = from_dt.date(), to_dt.date()
    out = []
    while cur <= end_d:
        ce = min(cur + timedelta(days=days - 1), end_d)
        cf = from_dt if cur == from_dt.date() else datetime.combine(cur, SESSION_START)
        ct = to_dt if ce == end_d else datetime.combine(ce, SESSION_END)
        out.append((cf, ct))
        cur = ce + timedelta(days=1)
    return out


def _instruments(kite, exchange, cache):
    ex = exchange.upper().strip()
    if ex not in cache:
        cache[ex] = kite.instruments(ex)
    return cache[ex]


def _token(kite, exchange, tradingsymbol, cache):
    want = tradingsymbol.strip().upper()
    for r in _instruments(kite, exchange, cache):
        if str(r.get("tradingsymbol", "")).upper() == want:
            return int(r["instrument_token"])
    raise ValueError(f"Instrument not found {exchange}:{tradingsymbol}")


def _fetch_minute(kite, token, from_dt, to_dt, label):
    rows_all = []
    for i, (cf, ct) in enumerate(_chunks(from_dt, to_dt, MAX_DAYS_PER_CHUNK), 1):
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                rows = kite.historical_data(token, cf, ct, "minute", continuous=False, oi=False)
                rows_all.extend(rows)
                break
            except Exception as e:
                if attempt == MAX_ATTEMPTS:
                    print(f"[ERR] {label} chunk {i}: {e}")
                time.sleep(min(8.0, 1.5 * attempt))
        time.sleep(SLEEP_BETWEEN_CALLS)
    return rows_all


def download_underlyings(kite, d0, d1):
    cache = {}
    out = {}
    f, t = datetime.combine(d0, SESSION_START), datetime.combine(d1, SESSION_END)
    for und, meta in UNDERLYING_KITE.items():
        if und not in TRADEABLE:
            continue
        tok = _token(kite, meta["exchange"], meta["tradingsymbol"], cache)
        rows = _fetch_minute(kite, tok, f, t, f"{meta['exchange']}:{meta['tradingsymbol']}")
        if not rows:
            out[und] = pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
            continue
        df = pd.DataFrame(rows)
        df["date"] = ensure_ist(df["date"])
        df = df.drop_duplicates("date", keep="last").sort_values("date").reset_index(drop=True)
        df["day"] = df["date"].dt.tz_convert(ist_tz()).dt.date
        out[und] = df
        print(f"[UND OK] {und}: {len(df)} candles, {df['day'].nunique()} days")
    return out


# =============================================================================
# THE TWO SIGNALS  (computed from candles up to the entry minute only)
# =============================================================================
def premium_swelled(straddle_series: pd.Series, entry_ts: pd.Timestamp) -> Tuple[bool, Dict[str, float]]:
    """
    straddle_series: index = minute ts, value = CE+PE close (ffilled), for today.
    Swell = premium(now) > premium(now - N)  AND  premium(now) > midpoint candle,
    with an optional minimum % rise vs N-ago. Uses only data at/under entry_ts.
    """
    s = straddle_series.loc[:entry_ts].dropna()
    if len(s) < SWELL_LOOKBACK_N + 1:
        return False, {"reason": "not_enough_candles"}
    now = float(s.iloc[-1])
    past = float(s.iloc[-(SWELL_LOOKBACK_N + 1)])
    mid = float(s.iloc[-(SWELL_LOOKBACK_N // 2 + 1)])
    rise_pct = (now - past) / past * 100.0 if past > 0 else 0.0
    swelled = (now > past) and (now >= mid) and (rise_pct >= SWELL_MIN_RISE_PCT)
    return bool(swelled), {"prem_now": now, "prem_past": past, "prem_mid": mid,
                           "rise_pct": round(rise_pct, 3)}


def trend_is_safe(spot_series: pd.Series, entry_ts: pd.Timestamp) -> Tuple[bool, Dict[str, Any]]:
    """
    spot_series: index = minute ts, value = underlying close, for today.
    Returns (safe, info). Logic:
      - Take the last TREND_LOOKBACK_N closes up to entry.
      - directionality = |net displacement| / total path  (0=pure chop, 1=clean run)
      - If directionality < TREND_DIRECTIONALITY  -> flat/quiet -> SAFE.
      - Else a trend is running in sign(net). Check the last REVERSE_CANDLES
        candle returns: if ALL of them are against the trend direction -> cooled
        -> SAFE. Otherwise the trend is live and un-reversed -> NOT safe.
    Uses only data at/under entry_ts.
    """
    s = spot_series.loc[:entry_ts].dropna()
    if len(s) < TREND_LOOKBACK_N + 1:
        return True, {"reason": "not_enough_candles_assume_safe"}

    win = s.iloc[-(TREND_LOOKBACK_N + 1):]
    diffs = win.diff().dropna()
    net = float(win.iloc[-1] - win.iloc[0])
    total_path = float(diffs.abs().sum())
    if total_path <= 0:
        return True, {"reason": "flat", "directionality": 0.0}

    directionality = abs(net) / total_path
    if directionality < TREND_DIRECTIONALITY:
        return True, {"reason": "chop_or_quiet", "directionality": round(directionality, 3),
                      "net": round(net, 2)}

    trend_dir = 1.0 if net > 0 else -1.0
    last = diffs.iloc[-REVERSE_CANDLES:]
    # 'against trend': sign opposite to trend_dir
    all_reverse = bool((last * trend_dir < 0).all()) and len(last) == REVERSE_CANDLES
    safe = all_reverse
    return safe, {"reason": "trend_running",
                  "directionality": round(directionality, 3),
                  "trend_dir": "up" if trend_dir > 0 else "down",
                  "reverse_candles_ok": all_reverse,
                  "net": round(net, 2)}


# =============================================================================
# PER-DAY OPTION FRAME -> ATM leg series
# =============================================================================
def pick_symbol(day_opt, strike, opt_type):
    sub = day_opt[(day_opt["strike_int"] == strike) & (day_opt["option_type"] == opt_type)]
    if sub.empty:
        return None
    syms = sorted(sub["instrument"].astype(str).unique().tolist())
    return syms[0] if syms else None


def leg_series(day_opt, idx, strike, opt_type, symbol, col, ffill=True):
    sub = day_opt[(day_opt["strike_int"] == strike) &
                  (day_opt["option_type"] == opt_type) &
                  (day_opt["instrument"].astype(str) == symbol)][["date", col]].dropna()
    if sub.empty:
        return pd.Series(index=idx, dtype="float64")
    sub = sub.copy()
    sub["date"] = ensure_ist(sub["date"])
    sub = sub.sort_values("date").drop_duplicates("date", keep="last").set_index("date")
    s = sub[col].astype(float).reindex(idx)
    return s.ffill() if ffill else s


# =============================================================================
# DATA STRUCTURE
# =============================================================================
@dataclass
class TradeRow:
    day: date
    underlying: str
    trade_seq: int
    expiry: date
    days_to_expiry: int
    atm_strike: int
    qty: int
    entry_time: str
    exit_time: str
    exit_reason: str
    entry_spot: float
    entry_ce: float
    entry_pe: float
    entry_premium: float
    exit_ce: float
    exit_pe: float
    gross_pnl: float
    txn_charges: float
    net_pnl: float
    eod_pnl: float
    max_profit: float
    max_loss: float
    swell_rise_pct: float
    trend_reason: str
    trend_directionality: float


# =============================================================================
# SIMULATE ONE DAY  (multiple-try with gate + daily loss cap)
# =============================================================================
def simulate_day(*, und, dy, expiry, day_opt, underlying_day) -> Tuple[List[TradeRow], List[Dict[str, Any]]]:
    results, skipped = [], []
    idx = minute_index(dy, SESSION_START, SESSION_END)
    session_end_ts = idx[-1]
    qty = int(QTY_UNITS[und])
    step = int(STRIKE_STEP[und])
    G = float(PROFIT_PROTECT_TRIGGER)

    # Build today's underlying close series on the minute grid.
    ud = underlying_day[["date", "close"]].dropna().copy()
    ud["date"] = ensure_ist(ud["date"])
    spot_series = ud.sort_values("date").drop_duplicates("date", keep="last").set_index("date")["close"].reindex(idx).ffill()

    cur_entry = pd.Timestamp(datetime.combine(dy, ENTRY_TIME), tz=ist_tz())
    seq = 1
    day_realized = 0.0

    while cur_entry <= session_end_ts:
        # Daily loss cap brake.
        if DAILY_LOSS_CAP_RUPEES > 0 and day_realized <= -float(DAILY_LOSS_CAP_RUPEES):
            skipped.append({"day": dy, "underlying": und, "trade_seq": seq,
                            "reason": f"DAILY_LOSS_CAP hit ({day_realized:.0f})"})
            break

        u_px = asof_close(underlying_day, cur_entry)
        if pd.isna(u_px):
            skipped.append({"day": dy, "underlying": und, "trade_seq": seq,
                            "reason": "no underlying at entry"})
            break
        atm = round_to_step(float(u_px), step)
        ce_sym = pick_symbol(day_opt, atm, "CE")
        pe_sym = pick_symbol(day_opt, atm, "PE")
        if not ce_sym or not pe_sym:
            skipped.append({"day": dy, "underlying": und, "trade_seq": seq,
                            "atm": atm, "reason": "ATM CE/PE missing"})
            break

        ce_raw = leg_series(day_opt, idx, atm, "CE", ce_sym, "close", ffill=False)
        pe_raw = leg_series(day_opt, idx, atm, "PE", pe_sym, "close", ffill=False)
        ce_close = ce_raw.ffill()
        pe_close = pe_raw.ffill()
        straddle = (ce_close + pe_close)

        if cur_entry not in idx:
            break

        # ---- GATE ----
        swelled, sw_info = premium_swelled(straddle, cur_entry)
        safe, tr_info = trend_is_safe(spot_series, cur_entry)
        if not (swelled and safe):
            reason = []
            if not swelled:
                reason.append("PREMIUM_NOT_SWELLED")
            if not safe:
                reason.append("TREND_NOT_COOLED")
            skipped.append({"day": dy, "underlying": und, "trade_seq": seq,
                            "atm": atm, "reason": "GATE:" + "+".join(reason),
                            "swell_rise_pct": sw_info.get("rise_pct"),
                            "trend_reason": tr_info.get("reason"),
                            "directionality": tr_info.get("directionality")})
            # gate failed this minute: advance by delay and re-test (regime is try-again)
            cur_entry = cur_entry + pd.Timedelta(minutes=REENTRY_DELAY_MIN)
            continue

        ce_entry = ce_raw.loc[cur_entry]
        pe_entry = pe_raw.loc[cur_entry]
        if pd.isna(ce_entry) or pd.isna(pe_entry):
            skipped.append({"day": dy, "underlying": und, "trade_seq": seq,
                            "atm": atm, "reason": "no CE/PE price at entry"})
            cur_entry = cur_entry + pd.Timedelta(minutes=REENTRY_DELAY_MIN)
            continue

        ce_entry = float(ce_entry)
        pe_entry = float(pe_entry)
        entry_prem = ce_entry + pe_entry

        mon_start = cur_entry + pd.Timedelta(minutes=1)
        if mon_start > session_end_ts:
            break

        # PnL (short straddle): premium collected minus current value.
        pnl_close = (ce_entry - ce_close) * qty + (pe_entry - pe_close) * qty
        pnl = pnl_close.loc[mon_start:].dropna()
        if pnl.empty:
            break

        # Worst-case intraminute for stoploss using highs.
        ce_high = leg_series(day_opt, idx, atm, "CE", ce_sym, "high", ffill=False)
        pe_high = leg_series(day_opt, idx, atm, "PE", pe_sym, "high", ffill=False)
        pnl_worst = (ce_entry - ce_high) * qty + (pe_entry - pe_high) * qty
        pnl_sl = pnl_worst.loc[mon_start:].dropna()

        eod_ts = pnl.index[-1]
        eod_pnl = float(pnl.iloc[-1])
        max_profit = float(max(0.0, pnl.max()))
        max_loss = float(min(0.0, pnl.min()))

        stop_hit = pnl_sl <= -LOSS_LIMIT_RUPEES
        stop_ts = pnl_sl.index[stop_hit.to_numpy().argmax()] if stop_hit.any() else None

        protect_ts = None
        if G > 0:
            peak = pnl.cummax()
            armed = peak >= G
            protect_hit = armed & (pnl <= (peak - G))
            protect_ts = pnl.index[protect_hit.to_numpy().argmax()] if protect_hit.any() else None

        exit_ts, exit_reason = eod_ts, "EOD"
        if stop_ts is not None and (protect_ts is None or stop_ts <= protect_ts):
            exit_ts, exit_reason = stop_ts, "STOPLOSS"
        elif protect_ts is not None:
            exit_ts, exit_reason = protect_ts, "PROFIT_PROTECT"

        gross = -float(LOSS_LIMIT_RUPEES) if exit_reason == "STOPLOSS" else float(pnl.loc[exit_ts])
        exit_ce = float(ce_close.loc[exit_ts]) if pd.notna(ce_close.loc[exit_ts]) else 0.0
        exit_pe = float(pe_close.loc[exit_ts]) if pd.notna(pe_close.loc[exit_ts]) else 0.0
        charges = trade_charges(ce_entry, pe_entry, exit_ce, exit_pe, qty)
        net = gross - charges
        day_realized += net

        results.append(TradeRow(
            day=dy, underlying=und, trade_seq=seq, expiry=expiry,
            days_to_expiry=int((expiry - dy).days), atm_strike=int(atm), qty=qty,
            entry_time=cur_entry.strftime("%H:%M"), exit_time=pd.Timestamp(exit_ts).strftime("%H:%M"),
            exit_reason=exit_reason, entry_spot=float(u_px),
            entry_ce=ce_entry, entry_pe=pe_entry, entry_premium=entry_prem,
            exit_ce=exit_ce, exit_pe=exit_pe,
            gross_pnl=gross, txn_charges=charges, net_pnl=net, eod_pnl=eod_pnl,
            max_profit=max_profit, max_loss=max_loss,
            swell_rise_pct=float(sw_info.get("rise_pct", 0.0)),
            trend_reason=str(tr_info.get("reason", "")),
            trend_directionality=float(tr_info.get("directionality", 0.0)),
        ))

        if exit_reason in ("STOPLOSS", "PROFIT_PROTECT") and seq <= MAX_REENTRIES:
            seq += 1
            cur_entry = pd.Timestamp(exit_ts) + pd.Timedelta(minutes=REENTRY_DELAY_MIN)
            continue
        break

    return results, skipped


# =============================================================================
# PASS 1: nearest expiry per (underlying, day)
# =============================================================================
def scan_pass1(paths):
    max_day = min_day = None
    min_expiry: Dict[Tuple[str, date], date] = {}
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
            mn, mx = d2["day"].min(), d2["day"].max()
            max_day = mx if max_day is None or mx > max_day else max_day
            min_day = mn if min_day is None or mn < min_day else min_day
            for (u, dd), ex in d2.groupby(["underlying", "day"])["expiry_date"].min().items():
                k = (u, dd)
                if k not in min_expiry or ex < min_expiry[k]:
                    min_expiry[k] = ex
        except Exception as e:
            if FAIL_ON_PICKLE_ERROR:
                raise
            print(f"[PASS1 WARN] {os.path.basename(p)}: {e}")
    if max_day is None:
        raise RuntimeError("No usable option data.")
    return max_day, min_expiry, min_day


# =============================================================================
# PASS 2: simulate
# =============================================================================
def process(paths, min_expiry, underlying_data, w0, w1):
    all_trades, skipped = [], []
    done: set = set()
    for p in paths:
        try:
            df = pd.read_pickle(p)
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue
            need = ["date", "name", "type", "option_type", "strike", "expiry", "instrument", "high", "low", "close"]
            if any(c not in df.columns for c in need):
                raise ValueError("missing columns")
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
            d2 = d2[(d2["expiry_date"] >= d2["day"]) & (d2["day"] >= w0) & (d2["day"] <= w1)]
            if d2.empty:
                continue
            for (u, dd, ex), g in d2.groupby(["underlying", "day", "expiry_date"], sort=False):
                if min_expiry.get((u, dd)) != ex:
                    continue
                key = (u, dd, ex)
                if key in done:
                    continue
                done.add(key)
                uday = underlying_data.get(u)
                if uday is None:
                    continue
                uday = uday[uday["day"] == dd]
                if uday.empty:
                    skipped.append({"day": dd, "underlying": u, "reason": "no underlying for day"})
                    continue
                tr, sk = simulate_day(und=u, dy=dd, expiry=ex, day_opt=g, underlying_day=uday)
                all_trades.extend([asdict(t) for t in tr])
                skipped.extend(sk)
        except Exception as e:
            if FAIL_ON_PICKLE_ERROR:
                raise
            print(f"[PASS2 WARN] {os.path.basename(p)}: {e}")

    tdf = pd.DataFrame(all_trades)
    if not tdf.empty:
        tdf = tdf.sort_values(["day", "underlying", "trade_seq"]).reset_index(drop=True)
    sdf = pd.DataFrame(skipped)
    return tdf, sdf


# =============================================================================
# OUTPUT
# =============================================================================
def write_excel(trades, skipped):
    out_dir = os.path.dirname(os.path.abspath(OUTPUT_XLSX))
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    daywise = monthwise = instrument = first_trade = pd.DataFrame()
    if not trades.empty:
        t = trades.copy()
        t["month"] = pd.to_datetime(t["day"]).dt.to_period("M").astype(str)
        daywise = t.groupby(["day", "underlying"], as_index=False).agg(
            trades=("net_pnl", "count"), day_net_pnl=("net_pnl", "sum"),
            stops=("exit_reason", lambda s: (s == "STOPLOSS").sum()))
        monthwise = t.groupby("month", as_index=False).agg(
            trades=("net_pnl", "count"), net_pnl=("net_pnl", "sum"),
            wins=("net_pnl", lambda s: (s > 0).sum()))
        monthwise["win_rate_pct"] = (100 * monthwise["wins"] / monthwise["trades"]).round(1)
        instrument = t.groupby("underlying", as_index=False).agg(
            trades=("net_pnl", "count"), net_pnl=("net_pnl", "sum"),
            avg_pnl=("net_pnl", "mean"),
            win_rate_pct=("net_pnl", lambda s: round(100 * (s > 0).mean(), 1)),
            stop_rate_pct=("exit_reason", lambda s: round(100 * (s == "STOPLOSS").mean(), 1)),
            avg_max_profit=("max_profit", "mean"), worst_max_loss=("max_loss", "min"))
        first_trade = t[t["trade_seq"] == 1].groupby("underlying", as_index=False).agg(
            first_try_trades=("net_pnl", "count"), first_try_net=("net_pnl", "sum"),
            first_try_win_rate=("net_pnl", lambda s: round(100 * (s > 0).mean(), 1)))

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as xw:
        (trades if not trades.empty else pd.DataFrame({"info": ["no trades"]})).to_excel(xw, sheet_name="trades", index=False)
        (daywise if not daywise.empty else pd.DataFrame({"info": ["no trades"]})).to_excel(xw, sheet_name="daywise", index=False)
        (monthwise if not monthwise.empty else pd.DataFrame({"info": ["no trades"]})).to_excel(xw, sheet_name="monthwise", index=False)
        (instrument if not instrument.empty else pd.DataFrame({"info": ["no trades"]})).to_excel(xw, sheet_name="instrument_summary", index=False)
        (first_trade if not first_trade.empty else pd.DataFrame({"info": ["no trades"]})).to_excel(xw, sheet_name="first_trade_only", index=False)
        (skipped if not skipped.empty else pd.DataFrame({"info": ["none"]})).to_excel(xw, sheet_name="skipped", index=False)
        for ws in xw.book.worksheets:
            ws.freeze_panes = "A2"
    print(f"[DONE] {OUTPUT_XLSX}")


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
    w0 = window_start(end_day, LOOKBACK_MONTHS)
    print(f"[INFO] window {w0} -> {end_day}  (data from {min_day})")
    print(f"[INFO] entry={ENTRY_TIME_IST} swellN={SWELL_LOOKBACK_N} "
          f"trendN={TREND_LOOKBACK_N} reverse={REVERSE_CANDLES} "
          f"SL={LOSS_LIMIT_RUPEES} dailycap={DAILY_LOSS_CAP_RUPEES} reentries={MAX_REENTRIES}")

    kite = oUtils.intialize_kite_api()
    underlying_data = download_underlyings(kite, w0, end_day)

    trades, skipped = process(paths, min_expiry, underlying_data, w0, end_day)
    write_excel(trades, skipped)

    if not trades.empty:
        print(f"\n[RESULT] trades={len(trades)} net_pnl={trades['net_pnl'].sum():,.0f} "
              f"win_rate={100*(trades['net_pnl']>0).mean():.1f}%")
        print(trades.groupby("underlying")["net_pnl"].agg(["count", "sum", "mean"]))
    else:
        print("[RESULT] No trades. Check 'skipped' for GATE reasons.")


if __name__ == "__main__":
    main()
