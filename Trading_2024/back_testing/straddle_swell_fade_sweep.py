"""
straddle_swell_fade_sweep.py  -  STANDALONE
===========================================

ONE simple idea, researched across thresholds:

  Watch the ATM straddle premium intraday. When it SWELLS by X% versus N candles
  ago, SELL the straddle to capture the FALLBACK (premium reverting toward its
  pre-swell level). Take profit when premium falls back to the pre-swell level;
  stoploss if it keeps swelling against us; else exit EOD.

  Research question: WHICH swell magnitudes are most profitable to fade?
  -> We sweep a grid of X% thresholds and report PnL/win-rate per bucket.

No trend filter. No fixed entry time. No multi-day baselines. Pure intraday
mean-reversion on the premium. Only external dependency: Kite, used ONLY to map
ATM strike from spot (the option pickles supply all premium data).

Run:  python straddle_swell_fade_sweep.py
Output: one Excel with a 'swell_buckets' sheet (the answer) + per-trade detail.
"""

from __future__ import annotations
import os, glob, time
from dataclasses import dataclass, asdict
from datetime import datetime, date, time as dtime, timedelta
from typing import Dict, List, Tuple, Optional, Any
import pandas as pd
import numpy as np

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

SESSION_START = dtime(9, 15)
SESSION_END   = dtime(15, 30)
# Don't open new fade positions after this (need room for fallback before EOD).
LAST_ENTRY    = dtime(15, 0)

# --- Swell detection ---
SWELL_LOOKBACK_N = int(os.getenv("SWELL_LOOKBACK_N", "10"))   # candles back for "pre-swell"
# Threshold grid to sweep (% rise vs N candles ago). Each trade is tagged with
# the HIGHEST bucket it cleared, so buckets are mutually exclusive in reporting.
SWELL_THRESHOLDS = [5, 7.5, 10, 12.5, 15, 20, 25, 30]        # percent

# --- ENTRY: arm on the rise, fire on the turn ---
# When premium has risen >= a threshold vs N ago, we ARM (start watching the
# peak) but do NOT enter. We enter only once premium turns DOWN from its peak,
# confirming the swell is reverting. The turn is confirmed when EITHER:
#   - premium falls >= PEAK_GIVEBACK_PCT below the running peak, OR
#   - REVERSE_CANDLES consecutive falling candles print after the peak.
PEAK_GIVEBACK_PCT = float(os.getenv("PEAK_GIVEBACK_PCT", "2.0"))   # % off the peak
REVERSE_CANDLES   = int(os.getenv("REVERSE_CANDLES", "2"))         # falling candles to confirm
# Abandon an armed-but-unfired swell if premium falls all the way back to the
# pre-swell level before we ever caught a turn (nothing left to fade).
ABANDON_AT_PRESWELL = True

# --- Fade management ---
# EXIT MODEL: hold to EOD to capture full theta decay, protected by:
#   - a stoploss (premium makes a new high beyond the peak), and
#   - an optional profit-protect TRAIL (lock gains once in profit).
# The old "fallback take-profit" (exit when premium returns to pre-swell) exited
# far too early per the avg_left_on_table analysis, so it is OFF by default.
USE_FALLBACK_TP = os.getenv("USE_FALLBACK_TP", "0").strip() == "1"   # default OFF
TP_AT_FRAC      = float(os.getenv("TP_AT_FRAC", "1.0"))             # only if TP on

# Profit-protect trail (rupees). Once the trade's running profit reaches
# PROFIT_PROTECT_TRIGGER, arm a trail; exit if profit gives back PROFIT_PROTECT_GIVEBACK
# from its peak profit. Set TRIGGER=0 to disable (pure hold-to-EOD + stoploss).
PROFIT_PROTECT_TRIGGER  = int(os.getenv("PROFIT_PROTECT_TRIGGER", "10000"))
PROFIT_PROTECT_GIVEBACK = int(os.getenv("PROFIT_PROTECT_GIVEBACK", "5000"))

# Stoploss: FIXED RUPEE loss per trade. The trade is held (riding the decay,
# protected by the profit-protect trail and EOD) until the running loss reaches
# -SL_RUPEES, at which point it is stopped. This is the primary stop.
SL_RUPEES       = int(os.getenv("SL_RUPEES", "5000"))
# Legacy percent-above-peak stop. Kept available but OFF by default now that the
# stop is a fixed rupee amount. Set > 0 to also stop on a new high beyond peak.
SL_ABOVE_PEAK_PCT = float(os.getenv("SL_ABOVE_PEAK_PCT", "0"))  # 0 = disabled

# --- Re-attempt control (per underlying per day) ---
# After ANY exit, a fresh swell may not arm until BOTH cool-downs pass:
#   (1) PRICE: premium has fallen back to near the prior pre-swell level
#       (<= COOLDOWN_TO_PRESWELL_FRAC * pre_swell), so the prior swell has truly
#       deflated rather than re-firing on its own elevated tail.
#   (2) TIME: at least REENTRY_WAIT_MIN minutes have elapsed since the exit.
# This stops the 2-3 minute re-entries that were just fading the same swell.
COOLDOWN_TO_PRESWELL_FRAC = float(os.getenv("COOLDOWN_TO_PRESWELL_FRAC", "1.05"))
REENTRY_WAIT_MIN          = int(os.getenv("REENTRY_WAIT_MIN", "10"))
# Cap how many fade attempts we make in one day, so a relentlessly volatile day
# can't stack a string of stoplosses (the overtrading failure mode).
MAX_TRADES_PER_DAY   = int(os.getenv("MAX_TRADES_PER_DAY", "4"))
# Stop opening new fades once the day's realized PnL falls below -cap (0 = off).
DAILY_LOSS_CAP_RUPEES = int(os.getenv("DAILY_LOSS_CAP_RUPEES", "15000"))
# One open position per underlying at a time.

# --- Universe / sizing ---
QTY_UNITS   = {"NIFTY": 325, "SENSEX": 100}    # set to your real sizing
STRIKE_STEP = {"NIFTY": 50, "SENSEX": 100}
TRADEABLE   = set(QTY_UNITS.keys())
# When two underlyings tie on days-to-expiry, this order breaks the tie.
TRADEABLE_PRIORITY = ["NIFTY", "SENSEX"]
# If True, trade only ONE underlying per day: the one nearest its expiry across
# both. If False, each underlying trades its own nearest expiry every day.
SINGLE_NEAREST_PER_DAY = os.getenv("SINGLE_NEAREST_PER_DAY", "1").strip() == "1"
UNDERLYING_KITE = {
    "NIFTY":  {"exchange": "NSE", "tradingsymbol": "NIFTY 50"},
    "SENSEX": {"exchange": "BSE", "tradingsymbol": "SENSEX"},
}
LOOKBACK_MONTHS = int(os.getenv("LOOKBACK_MONTHS", "12"))

# --- Costs (Zerodha F&O options) ---
INCLUDE_TXN_COSTS = True
BROKERAGE_PER_ORDER = 20.0; ORDERS_PER_TRADE = 4
STT_SELL_PCT = 0.001; EXCH_TXN_PCT = 0.0003553; SEBI_PER_CRORE = 10.0
STAMP_BUY_PCT = 0.00003; IPFT_PER_CRORE = 0.010; GST_PCT = 0.18

MAX_DAYS_PER_CHUNK = 25; MAX_ATTEMPTS = 5; SLEEP_BETWEEN_CALLS = 0.20
FAIL_ON_PICKLE_ERROR = os.getenv("FAIL_ON_PICKLE_ERROR", "0") == "1"


def _downloads():
    from pathlib import Path
    d = Path.home() / "Downloads"
    return str(d if d.exists() else Path.home())

def _fname(s): return "".join(c if c.isalnum() or c in "-_." else "_" for c in str(s))

OUTPUT_XLSX = os.getenv("OUTPUT_XLSX", os.path.join(
    _downloads(),
    f"swell_fade_sweep_N{SWELL_LOOKBACK_N}_giveback{_fname(PEAK_GIVEBACK_PCT)}"
    f"_PP{_fname(PROFIT_PROTECT_TRIGGER)}-{_fname(PROFIT_PROTECT_GIVEBACK)}"
    f"_SLrs{SL_RUPEES}_max{MAX_TRADES_PER_DAY}_dlc{DAILY_LOSS_CAP_RUPEES}_EOD.xlsx"))


# =============================================================================
# TZ / TIME
# =============================================================================
def ist_tz():
    if ZoneInfo is not None: return ZoneInfo("Asia/Kolkata")
    if pytz is not None: return pytz.timezone("Asia/Kolkata")
    return "Asia/Kolkata"

def ensure_ist(x):
    tz = ist_tz(); dt = pd.to_datetime(x, errors="coerce")
    if isinstance(dt, pd.Series):
        return dt.dt.tz_localize(tz) if dt.dt.tz is None else dt.dt.tz_convert(tz)
    if getattr(dt, "tzinfo", None) is None: return dt.tz_localize(tz)
    return dt.tz_convert(tz)

def normalize_underlying(name):
    if not isinstance(name, str): return None
    u = name.upper().strip()
    if "SENSEX" in u: return "SENSEX"
    if "BANKNIFTY" in u or "NIFTY BANK" in u: return "BANKNIFTY"
    if "NIFTY" in u: return "NIFTY"
    return None

def round_to_step(x, step): return int(round(x / step) * step)

def minute_index(day_d, a, b):
    tz = ist_tz()
    return pd.date_range(pd.Timestamp(datetime.combine(day_d, a), tz=tz),
                         pd.Timestamp(datetime.combine(day_d, b), tz=tz), freq="1min")

def asof_close(df, ts):
    if df.empty: return float("nan")
    d = df[["date", "close"]].dropna().copy()
    d["date"] = ensure_ist(d["date"]); d = d.sort_values("date").set_index("date")
    loc = d.index.get_indexer([ts], method="pad")
    return float("nan") if loc[0] == -1 else float(d.iloc[loc[0]]["close"])

def window_start(end_day, months):
    try:
        from dateutil.relativedelta import relativedelta
        return (pd.Timestamp(end_day) - relativedelta(months=months)).date()
    except Exception:
        return (pd.Timestamp(end_day) - pd.Timedelta(days=30 * months)).date()


# =============================================================================
# COSTS
# =============================================================================
def trade_charges(entry_ce, entry_pe, exit_ce, exit_pe, qty):
    if not INCLUDE_TXN_COSTS: return 0.0
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
# KITE underlying (for ATM mapping)
# =============================================================================
def _chunks(f, t, days):
    cur, end_d = f.date(), t.date(); out = []
    while cur <= end_d:
        ce = min(cur + timedelta(days=days - 1), end_d)
        cf = f if cur == f.date() else datetime.combine(cur, SESSION_START)
        ct = t if ce == end_d else datetime.combine(ce, SESSION_END)
        out.append((cf, ct)); cur = ce + timedelta(days=1)
    return out

def _instruments(kite, ex, cache):
    ex = ex.upper().strip()
    if ex not in cache: cache[ex] = kite.instruments(ex)
    return cache[ex]

def _token(kite, ex, ts, cache):
    want = ts.strip().upper()
    for r in _instruments(kite, ex, cache):
        if str(r.get("tradingsymbol", "")).upper() == want:
            return int(r["instrument_token"])
    raise ValueError(f"Instrument not found {ex}:{ts}")

def _fetch_minute(kite, tok, f, t, label):
    rows = []
    for i, (cf, ct) in enumerate(_chunks(f, t, MAX_DAYS_PER_CHUNK), 1):
        for a in range(1, MAX_ATTEMPTS + 1):
            try:
                rows.extend(kite.historical_data(tok, cf, ct, "minute", continuous=False, oi=False)); break
            except Exception as e:
                if a == MAX_ATTEMPTS: print(f"[ERR] {label} chunk {i}: {e}")
                time.sleep(min(8.0, 1.5 * a))
        time.sleep(SLEEP_BETWEEN_CALLS)
    return rows

def download_underlyings(kite, d0, d1):
    cache, out = {}, {}
    f, t = datetime.combine(d0, SESSION_START), datetime.combine(d1, SESSION_END)
    for und, meta in UNDERLYING_KITE.items():
        if und not in TRADEABLE: continue
        tok = _token(kite, meta["exchange"], meta["tradingsymbol"], cache)
        rows = _fetch_minute(kite, tok, f, t, f"{meta['exchange']}:{meta['tradingsymbol']}")
        if not rows:
            out[und] = pd.DataFrame(columns=["date", "close", "day"]); continue
        df = pd.DataFrame(rows); df["date"] = ensure_ist(df["date"])
        df = df.drop_duplicates("date", keep="last").sort_values("date").reset_index(drop=True)
        df["day"] = df["date"].dt.tz_convert(ist_tz()).dt.date
        out[und] = df
        print(f"[UND OK] {und}: {len(df)} candles, {df['day'].nunique()} days")
    return out


# =============================================================================
# OPTION leg series
# =============================================================================
def pick_symbol(day_opt, strike, opt):
    sub = day_opt[(day_opt["strike_int"] == strike) & (day_opt["option_type"] == opt)]
    if sub.empty: return None
    syms = sorted(sub["instrument"].astype(str).unique().tolist())
    return syms[0] if syms else None

def leg_series(day_opt, idx, strike, opt, symbol, col, ffill=True):
    sub = day_opt[(day_opt["strike_int"] == strike) & (day_opt["option_type"] == opt) &
                  (day_opt["instrument"].astype(str) == symbol)][["date", col]].dropna()
    if sub.empty: return pd.Series(index=idx, dtype="float64")
    sub = sub.copy(); sub["date"] = ensure_ist(sub["date"])
    sub = sub.sort_values("date").drop_duplicates("date", keep="last").set_index("date")
    s = sub[col].astype(float).reindex(idx)
    return s.ffill() if ffill else s


def which_bucket(rise_pct: float) -> Optional[float]:
    """Highest threshold the rise clears; None if below the smallest."""
    cleared = [th for th in SWELL_THRESHOLDS if rise_pct >= th]
    return max(cleared) if cleared else None


# =============================================================================
# TRADE ROW
# =============================================================================
@dataclass
class TradeRow:
    day: date
    underlying: str
    expiry: date
    days_to_expiry: int
    atm_strike: int
    qty: int
    entry_time: str
    exit_time: str
    exit_reason: str
    swell_bucket: float          # which % threshold it cleared (the ANSWER axis)
    rise_pct: float              # actual measured rise at entry
    entry_premium: float
    pre_swell_premium: float     # target to fall back to
    tp_level: float
    sl_level: float
    entry_ce: float
    entry_pe: float
    exit_premium: float
    gross_pnl: float
    txn_charges: float
    net_pnl: float
    eod_premium: float
    minutes_held: int
    # Max favorable excursion: best PnL the SHORT straddle would have reached if
    # held from entry to EOD (premium's lowest point after entry). Shows how much
    # an early take-profit leaves on the table.
    max_profit_if_held: float
    min_premium_after_entry: float
    eod_pnl_if_held: float


# =============================================================================
# SIMULATE ONE DAY  (swell-fade, rolling detection)
# =============================================================================
def simulate_day(*, und, dy, expiry, day_opt, underlying_day) -> List[TradeRow]:
    """
    State machine per day:
      IDLE   -> premium rises >= a swell threshold vs N candles ago -> ARMED
      ARMED  -> track running peak. Enter when premium TURNS DOWN from the peak
                (>= PEAK_GIVEBACK_PCT off peak, OR REVERSE_CANDLES falling bars).
                Abandon if premium sinks back to pre-swell before any turn.
      IN_TRADE -> short straddle. Exit on fallback-to-pre-swell TP, new-high
                  stoploss above the peak, or EOD. Then back to IDLE.
    The swell_bucket recorded is the highest threshold the rise cleared at the
    moment of ARMING (i.e. the size of the swell we're fading).
    """
    out: List[TradeRow] = []
    idx = minute_index(dy, SESSION_START, SESSION_END)
    if len(idx) == 0: return out
    end_ts = idx[-1]
    qty = int(QTY_UNITS[und]); step = int(STRIKE_STEP[und])
    last_entry_ts = pd.Timestamp(datetime.combine(dy, LAST_ENTRY), tz=ist_tz())

    open_ts = idx[0]
    spot0 = asof_close(underlying_day, open_ts)
    if pd.isna(spot0): return out
    atm = round_to_step(float(spot0), step)
    ce_sym = pick_symbol(day_opt, atm, "CE"); pe_sym = pick_symbol(day_opt, atm, "PE")
    if not ce_sym or not pe_sym: return out

    ce = leg_series(day_opt, idx, atm, "CE", ce_sym, "close", ffill=True)
    pe = leg_series(day_opt, idx, atm, "PE", pe_sym, "close", ffill=True)
    prem = (ce + pe)
    if prem.dropna().empty: return out

    pvals = prem.values
    n = len(pvals)

    state = "IDLE"
    pre_swell = peak = 0.0
    bucket = None
    falling_streak = 0
    prev_p = None
    trades_today = 0       # re-attempt counter (per underlying per day)
    day_realized = 0.0     # running realized PnL for the daily loss cap
    # After each exit, the next swell is measured from a FRESH local floor (the
    # lowest premium since the last exit), not only the rolling N-candles-ago
    # value. Without this, a stop leaves premium near its high, so a new "rise
    # vs N ago" rarely forms and re-attempts silently never fire. None until the
    # first exit (before that, the rolling-N reference is used as before).
    post_exit_floor = None
    # Cool-down after an exit: until premium falls back near the prior pre-swell
    # level AND the wait elapses, no fresh swell may arm. None = not cooling.
    cooldown_target = None     # premium level to cool back to (<=)
    cooldown_until_ts = None   # earliest ts a new entry may arm

    i = SWELL_LOOKBACK_N
    while i < n:
        ts = idx[i]
        now = pvals[i]
        past = pvals[i - SWELL_LOOKBACK_N]
        if not np.isfinite(now):
            i += 1; continue

        if state == "IDLE":
            # If cooling down, check whether BOTH gates have cleared.
            if cooldown_target is not None:
                price_cooled = now <= cooldown_target
                time_cooled = (cooldown_until_ts is None) or (ts >= cooldown_until_ts)
                if price_cooled and time_cooled:
                    # cool-down complete: reset and allow fresh swell-watching
                    cooldown_target = None
                    cooldown_until_ts = None
                    post_exit_floor = float(now)
                else:
                    # still cooling: track floor but do NOT arm
                    if post_exit_floor is not None and np.isfinite(now):
                        post_exit_floor = min(post_exit_floor, float(now))
                    i += 1
                    continue

            # Track the local floor since the last exit (for re-arming).
            if post_exit_floor is not None and np.isfinite(now):
                post_exit_floor = min(post_exit_floor, float(now))

            # Reference for "swell": the rolling N-candles-ago value, AND (after
            # an exit) the post-exit floor. We take whichever reference gives the
            # LOWER base, so a fresh swell can form from the local trough even
            # when premium is still below its earlier peak.
            refs = []
            if np.isfinite(past) and past > 0:
                refs.append(float(past))
            if post_exit_floor is not None and post_exit_floor > 0:
                refs.append(float(post_exit_floor))
            if refs and np.isfinite(now):
                base_ref = min(refs)
                rise_pct = (now / base_ref - 1.0) * 100.0
                b = which_bucket(rise_pct)
                if b is not None and ts <= last_entry_ts:
                    # ARM: start watching this swell for a turn.
                    state = "ARMED"
                    pre_swell = float(base_ref)
                    peak = float(now)
                    bucket = float(b)
                    falling_streak = 0
                    prev_p = float(now)
            i += 1
            continue

        if state == "ARMED":
            # update peak
            if now > peak:
                peak = float(now)
                falling_streak = 0
            else:
                if prev_p is not None and now < prev_p:
                    falling_streak += 1
                else:
                    falling_streak = 0
            prev_p = float(now)

            # abandon if it sank back to pre-swell with no tradeable turn
            if ABANDON_AT_PRESWELL and now <= pre_swell:
                state = "IDLE"; bucket = None; i += 1; continue
            # don't open too late
            if ts > last_entry_ts:
                state = "IDLE"; bucket = None; i += 1; continue
            # re-attempt cap: too many fades already today -> stop for the day
            if MAX_TRADES_PER_DAY > 0 and trades_today >= MAX_TRADES_PER_DAY:
                break
            # daily loss cap: day already too deep in the red -> stop for the day
            if DAILY_LOSS_CAP_RUPEES > 0 and day_realized <= -float(DAILY_LOSS_CAP_RUPEES):
                break

            giveback_ok = peak > 0 and now <= peak * (1.0 - PEAK_GIVEBACK_PCT / 100.0)
            candles_ok = falling_streak >= REVERSE_CANDLES
            if giveback_ok or candles_ok:
                # ---- ENTER the fade here (premium turned down from peak) ----
                entry_prem = float(now)
                ce_e = float(ce.iloc[i]); pe_e = float(pe.iloc[i])
                # Stoploss levels. Fixed rupee stop is primary: stop when the
                # running loss reaches -SL_RUPEES. The percent-above-peak stop is
                # only active if SL_ABOVE_PEAK_PCT > 0 (legacy/optional).
                use_pct_stop = SL_ABOVE_PEAK_PCT > 0
                sl_level = peak * (1.0 + SL_ABOVE_PEAK_PCT / 100.0) if use_pct_stop else float("inf")
                # Optional fallback TP (off by default now).
                tp_level = peak - TP_AT_FRAC * (peak - pre_swell)

                exit_ts = end_ts; exit_reason = "EOD"; exit_prem = float(pvals[-1])
                running_peak_profit = 0.0
                protect_armed = False
                j = i + 1
                while j < n:
                    p = pvals[j]
                    if not np.isfinite(p): j += 1; continue
                    profit = (entry_prem - p) * qty   # short straddle profit at this minute

                    # 1) Stoploss: fixed rupee loss (primary), or percent-above-peak if enabled.
                    hit_sl = (SL_RUPEES > 0 and profit <= -float(SL_RUPEES)) or (use_pct_stop and p >= sl_level)
                    if hit_sl:
                        exit_ts = idx[j]; exit_reason = "STOPLOSS"; exit_prem = float(p); break

                    # 2) Profit-protect trail: arm once profit hits trigger, then
                    #    exit if profit gives back GIVEBACK from its running peak.
                    if PROFIT_PROTECT_TRIGGER > 0:
                        if profit > running_peak_profit:
                            running_peak_profit = profit
                        if not protect_armed and running_peak_profit >= PROFIT_PROTECT_TRIGGER:
                            protect_armed = True
                        if protect_armed and profit <= running_peak_profit - PROFIT_PROTECT_GIVEBACK:
                            exit_ts = idx[j]; exit_reason = "PROFIT_PROTECT"; exit_prem = float(p); break

                    # 3) Optional legacy fallback TP (only if explicitly enabled).
                    if USE_FALLBACK_TP and p <= tp_level:
                        exit_ts = idx[j]; exit_reason = "FALLBACK_TP"; exit_prem = float(p); break

                    j += 1
                # else: fall through to EOD (hold the full session for theta decay)

                gross = (entry_prem - exit_prem) * qty
                exit_ce = float(ce.loc[exit_ts]) if np.isfinite(ce.loc[exit_ts]) else 0.0
                exit_pe = float(pe.loc[exit_ts]) if np.isfinite(pe.loc[exit_ts]) else 0.0
                charges = trade_charges(ce_e, pe_e, exit_ce, exit_pe, qty)
                net = gross - charges

                # Max favorable excursion if held from entry to EOD: the short
                # straddle profits most at the LOWEST premium after entry.
                fwd = pvals[i + 1:]
                fwd = fwd[np.isfinite(fwd)]
                if fwd.size:
                    min_prem_after = float(fwd.min())
                    eod_prem_val = float(pvals[-1])
                else:
                    min_prem_after = entry_prem
                    eod_prem_val = entry_prem
                max_profit_if_held = (entry_prem - min_prem_after) * qty
                eod_pnl_if_held = (entry_prem - eod_prem_val) * qty

                # Bucket by the PEAK swell size (the actual rise we faded), not
                # the threshold that first armed us. This is the research axis.
                peak_rise_pct = (peak / pre_swell - 1.0) * 100.0
                peak_bucket = which_bucket(peak_rise_pct)
                peak_bucket = float(peak_bucket) if peak_bucket is not None else float(bucket)

                out.append(TradeRow(
                    day=dy, underlying=und, expiry=expiry,
                    days_to_expiry=int((expiry - dy).days), atm_strike=int(atm), qty=qty,
                    entry_time=ts.strftime("%H:%M"), exit_time=pd.Timestamp(exit_ts).strftime("%H:%M"),
                    exit_reason=exit_reason, swell_bucket=float(peak_bucket),
                    rise_pct=round(peak_rise_pct, 2),
                    entry_premium=round(entry_prem, 2), pre_swell_premium=round(pre_swell, 2),
                    tp_level=round(tp_level, 2), sl_level=round(sl_level, 2),
                    entry_ce=round(ce_e, 2), entry_pe=round(pe_e, 2),
                    exit_premium=round(exit_prem, 2),
                    gross_pnl=round(gross, 2), txn_charges=charges, net_pnl=round(net, 2),
                    eod_premium=round(float(pvals[-1]), 2),
                    minutes_held=int((pd.Timestamp(exit_ts) - ts).seconds // 60),
                    max_profit_if_held=round(max_profit_if_held, 2),
                    min_premium_after_entry=round(min_prem_after, 2),
                    eod_pnl_if_held=round(eod_pnl_if_held, 2),
                ))

                # update per-day trackers for the caps
                trades_today += 1
                day_realized += net

                # back to IDLE, resume after exit. Seed the post-exit floor at
                # the exit premium so the NEXT swell is measured from this fresh
                # local level (enables reliable re-attempts after a stoploss).
                state = "IDLE"; bucket = None; prev_p = None; falling_streak = 0
                post_exit_floor = float(exit_prem)
                # Arm the cool-down: a fresh swell may not start until premium
                # falls back near the prior pre-swell level AND the wait elapses.
                cooldown_target = float(pre_swell) * COOLDOWN_TO_PRESWELL_FRAC
                cooldown_until_ts = pd.Timestamp(exit_ts) + pd.Timedelta(minutes=REENTRY_WAIT_MIN)
                exit_pos = idx.get_indexer([exit_ts])[0]
                i = max(i + 1, exit_pos + 1)
                continue
            i += 1
            continue

    return out


# =============================================================================
# PASS 1 / PASS 2
# =============================================================================
def scan_pass1(paths):
    max_day = min_day = None; min_expiry = {}
    for p in paths:
        try:
            df = pd.read_pickle(p)
            if not isinstance(df, pd.DataFrame) or df.empty: continue
            for c in ("date", "name", "expiry", "type"):
                if c not in df.columns: raise ValueError(f"missing {c}")
            d2 = df[df["type"].astype(str).str.upper().eq("OPTION")][["date", "name", "expiry"]].copy()
            if d2.empty: continue
            d2["date"] = ensure_ist(d2["date"]); d2["day"] = d2["date"].dt.date
            d2["underlying"] = d2["name"].astype(str).map(normalize_underlying)
            d2["expiry_date"] = pd.to_datetime(d2["expiry"], errors="coerce").dt.date
            d2 = d2.dropna(subset=["underlying", "day", "expiry_date"])
            d2 = d2[d2["underlying"].isin(TRADEABLE) & (d2["expiry_date"] >= d2["day"])]
            if d2.empty: continue
            mn, mx = d2["day"].min(), d2["day"].max()
            max_day = mx if max_day is None or mx > max_day else max_day
            min_day = mn if min_day is None or mn < min_day else min_day
            for (u, dd), ex in d2.groupby(["underlying", "day"])["expiry_date"].min().items():
                k = (u, dd)
                if k not in min_expiry or ex < min_expiry[k]: min_expiry[k] = ex
        except Exception as e:
            if FAIL_ON_PICKLE_ERROR: raise
            print(f"[PASS1 WARN] {os.path.basename(p)}: {e}")
    if max_day is None: raise RuntimeError("No usable option data.")

    # Choose ONE underlying per day: smallest days-to-expiry across both.
    # Tie -> order in TRADEABLE_PRIORITY (NIFTY first). This makes each day
    # trade only the instrument nearest its expiry.
    chosen: Dict[date, str] = {}
    by_day: Dict[date, List[Tuple[str, date]]] = {}
    for (u, dd), ex in min_expiry.items():
        by_day.setdefault(dd, []).append((u, ex))
    for dd, lst in by_day.items():
        def sort_key(item):
            u, ex = item
            dte = (ex - dd).days
            pri = TRADEABLE_PRIORITY.index(u) if u in TRADEABLE_PRIORITY else 99
            return (dte, pri)
        lst_sorted = sorted(lst, key=sort_key)
        chosen[dd] = lst_sorted[0][0]
    return max_day, min_expiry, min_day, chosen

def process(paths, min_expiry, underlying_data, w0, w1, chosen=None):
    all_trades = []; done = set()
    for p in paths:
        try:
            df = pd.read_pickle(p)
            if not isinstance(df, pd.DataFrame) or df.empty: continue
            need = ["date", "name", "type", "option_type", "strike", "expiry", "instrument", "high", "low", "close"]
            if any(c not in df.columns for c in need): raise ValueError("missing columns")
            d2 = df[df["type"].astype(str).str.upper().eq("OPTION")][need].copy()
            if d2.empty: continue
            d2["date"] = ensure_ist(d2["date"]); d2["day"] = d2["date"].dt.date
            d2["underlying"] = d2["name"].astype(str).map(normalize_underlying)
            d2 = d2[d2["underlying"].isin(TRADEABLE)]
            if d2.empty: continue
            d2["expiry_date"] = pd.to_datetime(d2["expiry"], errors="coerce").dt.date
            d2["strike_int"] = pd.to_numeric(d2["strike"], errors="coerce").round().astype("Int64")
            d2["option_type"] = d2["option_type"].astype(str).str.upper()
            d2 = d2.dropna(subset=["day", "underlying", "expiry_date", "strike_int", "close"])
            d2["strike_int"] = d2["strike_int"].astype(int)
            d2 = d2[(d2["expiry_date"] >= d2["day"]) & (d2["day"] >= w0) & (d2["day"] <= w1)]
            if d2.empty: continue
            for (u, dd, ex), g in d2.groupby(["underlying", "day", "expiry_date"], sort=False):
                if min_expiry.get((u, dd)) != ex: continue
                # Single-nearest-per-day: skip if this underlying isn't the chosen one.
                if SINGLE_NEAREST_PER_DAY and chosen is not None and chosen.get(dd) != u:
                    continue
                key = (u, dd, ex)
                if key in done: continue
                done.add(key)
                uday = underlying_data.get(u)
                if uday is None: continue
                uday = uday[uday["day"] == dd]
                if uday.empty: continue
                all_trades.extend([asdict(t) for t in simulate_day(
                    und=u, dy=dd, expiry=ex, day_opt=g, underlying_day=uday)])
        except Exception as e:
            if FAIL_ON_PICKLE_ERROR: raise
            print(f"[PASS2 WARN] {os.path.basename(p)}: {e}")
    tdf = pd.DataFrame(all_trades)
    if not tdf.empty:
        tdf = tdf.sort_values(["day", "underlying", "entry_time"]).reset_index(drop=True)
    return tdf


# =============================================================================
# THE ANSWER: bucket analysis
# =============================================================================
def build_bucket_summary(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty: return pd.DataFrame()
    def agg(g):
        return pd.Series({
            "trades": len(g),
            "net_pnl": g["net_pnl"].sum(),
            "avg_pnl": round(g["net_pnl"].mean(), 1),
            "median_pnl": round(g["net_pnl"].median(), 1),
            "win_rate_pct": round(100 * (g["net_pnl"] > 0).mean(), 1),
            "tp_rate_pct": round(100 * (g["exit_reason"] == "FALLBACK_TP").mean(), 1),
            "sl_rate_pct": round(100 * (g["exit_reason"] == "STOPLOSS").mean(), 1),
            "avg_minutes": round(g["minutes_held"].mean(), 0),
            "avg_rise_pct": round(g["rise_pct"].mean(), 1),
            # How much we LEFT on the table: avg best-PnL-if-held vs realized.
            "avg_max_if_held": round(g["max_profit_if_held"].mean(), 0),
            "avg_eod_if_held": round(g["eod_pnl_if_held"].mean(), 0),
            "avg_left_on_table": round((g["max_profit_if_held"] - g["net_pnl"]).mean(), 0),
        })
    by_bucket = trades.groupby("swell_bucket", group_keys=False).apply(agg).reset_index()
    by_bucket_und = (trades.groupby(["underlying", "swell_bucket"], group_keys=False)
                     .apply(agg).reset_index())
    return by_bucket, by_bucket_und


def write_excel(trades):
    out_dir = os.path.dirname(os.path.abspath(OUTPUT_XLSX))
    if out_dir and not os.path.exists(out_dir): os.makedirs(out_dir, exist_ok=True)
    placeholder = pd.DataFrame({"info": ["no trades"]})
    if trades.empty:
        buckets = buckets_und = month = placeholder
    else:
        buckets, buckets_und = build_bucket_summary(trades)
        t = trades.copy(); t["month"] = pd.to_datetime(t["day"]).dt.to_period("M").astype(str)
        month = t.groupby(["month", "swell_bucket"], as_index=False).agg(
            trades=("net_pnl", "count"), net_pnl=("net_pnl", "sum"),
            win_rate_pct=("net_pnl", lambda s: round(100 * (s > 0).mean(), 1)))
    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as xw:
        (buckets if not buckets.empty else placeholder).to_excel(xw, sheet_name="swell_buckets", index=False)
        (buckets_und if not buckets_und.empty else placeholder).to_excel(xw, sheet_name="buckets_by_underlying", index=False)
        (month if not month.empty else placeholder).to_excel(xw, sheet_name="bucket_by_month", index=False)
        (trades if not trades.empty else placeholder).to_excel(xw, sheet_name="trades", index=False)
        for ws in xw.book.worksheets: ws.freeze_panes = "A2"
    print(f"[DONE] {OUTPUT_XLSX}")


def main():
    paths = sorted(glob.glob(os.path.join(PICKLES_DIR, "*.pkl")) +
                   glob.glob(os.path.join(PICKLES_DIR, "*.pickle")))
    if not paths: raise FileNotFoundError(f"No pickles in {PICKLES_DIR}")
    print(f"[INFO] {len(paths)} pickles")
    end_day, min_expiry, min_day, chosen = scan_pass1(paths)
    w0 = window_start(end_day, LOOKBACK_MONTHS)
    print(f"[INFO] window {w0} -> {end_day} (from {min_day})")
    print(f"[INFO] swellN={SWELL_LOOKBACK_N} thresholds={SWELL_THRESHOLDS} "
          f"giveback%={PEAK_GIVEBACK_PCT} reverse={REVERSE_CANDLES} "
          f"EXIT=hold-to-EOD protect={PROFIT_PROTECT_TRIGGER}/{PROFIT_PROTECT_GIVEBACK} "
          f"SL_rupees={SL_RUPEES} SL_above_peak%={SL_ABOVE_PEAK_PCT} maxtrades/day={MAX_TRADES_PER_DAY} "
          f"daily_loss_cap={DAILY_LOSS_CAP_RUPEES} single_nearest={SINGLE_NEAREST_PER_DAY}")
    kite = oUtils.intialize_kite_api()
    underlying_data = download_underlyings(kite, w0, end_day)
    trades = process(paths, min_expiry, underlying_data, w0, end_day, chosen=chosen)
    write_excel(trades)
    if not trades.empty:
        print(f"\n[RESULT] trades={len(trades)} net={trades['net_pnl'].sum():,.0f} "
              f"win%={100*(trades['net_pnl']>0).mean():.1f}")
        bs, _ = build_bucket_summary(trades)
        print("\n[SWELL BUCKETS]")
        print(bs.to_string(index=False))
    else:
        print("[RESULT] No trades fired.")


if __name__ == "__main__":
    main()
