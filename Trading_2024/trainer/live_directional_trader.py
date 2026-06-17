"""
LIVE / PAPER multi-stock directional trader (Kite WebSocket).
=============================================================================
Pipeline:
  1. Load symbols from fno_symbols.txt.
  2. Live-screen them for liquidity (tight spread + Rs 10L depth); keep the most
     liquid MAX_MONITOR names.
  3. Subscribe to those via Kite WebSocket (KiteTicker), build 1-min bars from
     the tick stream (seeded with today's history so no 70-min warmup).
  4. Run the optimized directional signal per stock. Take ONE trade at a time --
     while in a position, ignore all new signals until it exits.
  5. Log everything, incl. SLIPPAGE measurement (signal price vs realistic fill).

============================ SAFETY ============================
  PAPER TRADING IS ON BY DEFAULT. No real order is sent unless you set
  LIVE_TRADING=True in this file AND pass --live AND type the confirm phrase.
  Run paper for several sessions; the slippage log is the key output -- it
  tells you your REAL per-side slippage to drop into the backtest's
  sensitivity table.
===============================================================

Run:
    python live_directional_trader.py            # paper (safe)
    python live_directional_trader.py --live      # live (still confirms)
"""

import os
import sys
import time
import threading
import logging
from dataclasses import dataclass
from datetime import datetime, date, time as dtime
from typing import Optional, Dict, List

import numpy as np
import pandas as pd
import pytz

import Trading_2024.OptionTradeUtils as oUtils
try:
    from kiteconnect import KiteTicker
except Exception:
    KiteTicker = None


# ============================================================
# SAFETY GATES
# ============================================================
LIVE_TRADING = False
CONFIRM_PHRASE = "TRADE LIVE"

# ============================================================
# CONFIG
# ============================================================
IST = pytz.timezone("Asia/Kolkata")
SESSION_START = dtime(9, 15)
SESSION_END = dtime(15, 30)
SQUAREOFF = dtime(15, 20)
NO_NEW_TRADES_AFTER = dtime(15, 0)

SYMBOLS_FILE = r"C:\Users\Local User\PycharmProjects\TradingScripts\Trading_2024\historic_data_fetcher\fno_symbols.txt"
MAX_MONITOR = 50                # monitor the N most liquid names
ORDER_VALUE_RS = 10_00_000
MAX_SPREAD_PCT = 0.0010
MAX_DEPTH_SLIPPAGE_PCT = 0.0015
QUOTE_THROTTLE_SEC = 0.34
WARMUP_BARS = 60                 # need >= EMA_SLOW + lookbacks

# ---- Strategy parameters (Bayesian-optimized; match scan_stocks_directional) ----
EMA_FAST, EMA_MID, EMA_SLOW = 8, 25, 48
SLOPE_LOOKBACK = 5
BREAKOUT_LOOKBACK = 28
ATR_PERIOD = 14
ATR_EXP_LOOKBACK = 30
ATR_EXPANSION = 1.04
ADX_PERIOD = 14
MIN_ADX = 20.0
MIN_FAN_PCT = 0.0013
TRAIL_TIERS = [(0.0100, 1.38), (0.0060, 1.84), (0.0035, 2.53), (0.0000, 3.45)]
MAX_LOSS_PCT = 0.0034
MIN_TREND_BARS = 10
COOLDOWN_BARS = 5
PROGRESS_BARS = 8
MIN_PROGRESS_PCT = 0.0010
BREAKEVEN_AFTER_PCT = 0.0015

# Order execution (live path)
OPTION_TICK = 0.05
ORDER_STATUS_POLL_SECONDS = 0.5
ORDER_STATUS_MAX_POLLS = 8
MARKET_PROTECTION = -1


# ============================================================
# LOGGING
# ============================================================
def setup_logger():
    os.makedirs("live_logs", exist_ok=True)
    fn = os.path.join("live_logs", f"stocktrader_{datetime.now(IST):%Y%m%d_%H%M%S}.log")
    lg = logging.getLogger("strader"); lg.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    fh = logging.FileHandler(fn); fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout); sh.setFormatter(fmt)
    lg.addHandler(fh); lg.addHandler(sh)
    lg.info(f"Log file: {fn}")
    return lg, fn


log, LOG_FILE = setup_logger()
SLIP_LOG = LOG_FILE.replace(".log", "_slippage.csv")


# ============================================================
# INDICATORS (match the backtest exactly)
# ============================================================
def add_indicators(g: pd.DataFrame) -> pd.DataFrame:
    g = g.copy(); c = g["close"]
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
    up_move = g["high"].diff(); down_move = -g["low"].diff()
    pdm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    mdm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    atr_w = tr.ewm(alpha=1 / ADX_PERIOD, adjust=False).mean()
    pdi = 100 * pd.Series(pdm, index=g.index).ewm(alpha=1 / ADX_PERIOD, adjust=False).mean() / atr_w
    mdi = 100 * pd.Series(mdm, index=g.index).ewm(alpha=1 / ADX_PERIOD, adjust=False).mean() / atr_w
    dx = 100 * (pdi - mdi).abs() / (pdi + mdi).replace(0, np.nan)
    g["adx"] = dx.ewm(alpha=1 / ADX_PERIOD, adjust=False).mean()
    return g


# ============================================================
# LIQUIDITY SCREEN (pick most liquid names)
# ============================================================
def load_symbols():
    if not os.path.exists(SYMBOLS_FILE):
        log.error(f"{SYMBOLS_FILE} not found."); sys.exit(1)
    with open(SYMBOLS_FILE) as f:
        return [ln.strip().upper() for ln in f if ln.strip() and not ln.startswith("#")]


def depth_absorbs(depth_side, mid, target_value):
    filled = 0.0; worst = 0.0
    for lvl in depth_side:
        px = float(lvl.get("price") or 0); qty = int(lvl.get("quantity") or 0)
        if px <= 0 or qty <= 0:
            continue
        filled += px * qty; worst = abs(px - mid) / mid
        if filled >= target_value:
            return worst
    return None


def screen_and_rank(kite, symbols):
    scored = []
    for k, sym in enumerate(symbols, 1):
        key = f"NSE:{sym}"
        try:
            q = kite.quote(key)[key]
        except Exception:
            time.sleep(QUOTE_THROTTLE_SEC); continue
        time.sleep(QUOTE_THROTTLE_SEC)
        depth = q.get("depth", {}) or {}
        bids = depth.get("buy", []); asks = depth.get("sell", [])
        bb = float(bids[0]["price"]) if bids and bids[0].get("price") else 0
        ba = float(asks[0]["price"]) if asks and asks[0].get("price") else 0
        if bb <= 0 or ba <= 0:
            continue
        mid = (bb + ba) / 2; spread = (ba - bb) / mid
        if spread > MAX_SPREAD_PCT:
            continue
        bs = depth_absorbs(asks, mid, ORDER_VALUE_RS)
        ss = depth_absorbs(bids, mid, ORDER_VALUE_RS)
        if bs is None or ss is None:
            continue
        worst = max(bs, ss)
        if worst > MAX_DEPTH_SLIPPAGE_PCT:
            continue
        token = int(q.get("instrument_token") or 0)
        scored.append({"symbol": sym, "token": token, "spread": spread,
                       "depth_slip": worst, "score": spread + worst})
        log.info(f"[SCREEN {k}/{len(symbols)}] {sym}: spread {spread*100:.3f}% "
                 f"depth_slip {worst*100:.3f}% PASS")
    scored.sort(key=lambda x: x["score"])     # tightest first
    return scored[:MAX_MONITOR]


# ============================================================
# THREAD-SAFE PER-STOCK BAR BUILDER
# ============================================================
class StockBars:
    def __init__(self):
        self.bars: List[Dict] = []
        self._m = None; self._o = self._h = self._l = self._c = None

    def seed(self, hist):
        self.bars.extend(hist)

    def update(self, ts, ltp):
        m = ts.replace(second=0, microsecond=0)
        if self._m is None:
            self._m = m; self._o = self._h = self._l = self._c = ltp; return False
        if m != self._m:
            self.bars.append({"date": self._m, "open": self._o, "high": self._h,
                              "low": self._l, "close": self._c})
            self._m = m; self._o = self._h = self._l = self._c = ltp
            return True            # a bar just closed
        self._h = max(self._h, ltp); self._l = min(self._l, ltp); self._c = ltp
        return False

    def df(self):
        if not self.bars:
            return pd.DataFrame(columns=["date", "open", "high", "low", "close"])
        return pd.DataFrame(self.bars)


# ============================================================
# POSITION (one at a time, global)
# ============================================================
@dataclass
class Position:
    symbol: str
    direction: str
    qty: int
    entry_signal_px: float       # underlying close at signal bar (for slippage)
    entry_fill_px: float         # realistic fill (ask for buy / bid for short)
    entry_bar: int
    entry_time: datetime
    extreme: float
    be_armed: bool = False


# ============================================================
# SIGNAL EVAL (on latest closed bar)
# ============================================================
def eval_entry(ind, last_exit_bar):
    n = len(ind); i = n - 1
    if i < BREAKOUT_LOOKBACK + SLOPE_LOOKBACK:
        return None
    if i - last_exit_bar < COOLDOWN_BARS:
        return None
    r = ind.iloc[i]
    if pd.isna(r["slope_s"]) or pd.isna(r["roll_hi"]) or pd.isna(r["adx"]) or pd.isna(r["atr_ratio"]):
        return None
    if not ((r["adx"] >= MIN_ADX) and (r["fan_pct"] >= MIN_FAN_PCT) and (r["atr_ratio"] >= ATR_EXPANSION)):
        return None
    if (r["ema_f"] > r["ema_m"] > r["ema_s"]) and (r["slope_s"] > 0) and (r["close"] >= ind.iloc[i - 1]["roll_hi"]):
        return "up"
    if (r["ema_f"] < r["ema_m"] < r["ema_s"]) and (r["slope_s"] < 0) and (r["close"] <= ind.iloc[i - 1]["roll_lo"]):
        return "down"
    return None


def eval_exit(ind, pos):
    n = len(ind); i = n - 1; r = ind.iloc[i]; held = i - pos.entry_bar
    ep = pos.entry_signal_px
    if pos.direction == "up":
        pos.extreme = max(pos.extreme, r["high"]); fav = (pos.extreme - ep) / ep
    else:
        pos.extreme = min(pos.extreme, r["low"]); fav = (ep - pos.extreme) / ep
    if fav >= BREAKEVEN_AFTER_PCT:
        pos.be_armed = True
    atr_mult = next(m for thr, m in TRAIL_TIERS if fav >= thr)
    if pos.direction == "up":
        trail = pos.extreme - atr_mult * r["atr"]
        if held >= PROGRESS_BARS and fav < MIN_PROGRESS_PCT: return "no_progress"
        if pos.be_armed and r["close"] <= ep: return "breakeven"
        if r["close"] <= ep * (1 - MAX_LOSS_PCT): return "max_loss"
        if r["close"] < trail: return "trail_stop"
        if r["ema_f"] < r["ema_m"]: return "ema_break"
    else:
        trail = pos.extreme + atr_mult * r["atr"]
        if held >= PROGRESS_BARS and fav < MIN_PROGRESS_PCT: return "no_progress"
        if pos.be_armed and r["close"] >= ep: return "breakeven"
        if r["close"] >= ep * (1 + MAX_LOSS_PCT): return "max_loss"
        if r["close"] > trail: return "trail_stop"
        if r["ema_f"] > r["ema_m"]: return "ema_break"
    return None


# ============================================================
# QUOTE HELPERS for realistic fills + slippage
# ============================================================
def bid_ask(kite, sym):
    key = f"NSE:{sym}"
    q = kite.quote(key)[key]
    d = q.get("depth", {}) or {}
    bids = d.get("buy", []); asks = d.get("sell", [])
    ltp = float(q.get("last_price") or 0)
    bid = float(bids[0]["price"]) if bids and bids[0].get("price") else ltp
    ask = float(asks[0]["price"]) if asks and asks[0].get("price") else ltp
    return bid, ask, ltp


def log_slippage(row: Dict):
    hdr = not os.path.exists(SLIP_LOG)
    pd.DataFrame([row]).to_csv(SLIP_LOG, mode="a", header=hdr, index=False)


# ============================================================
# GLOBAL STATE (shared between ws thread + main)
# ============================================================
STATE = {
    "bars": {},            # sym -> StockBars
    "last_exit_bar": {},   # sym -> int
    "position": None,      # single Position or None
    "pending": {},         # sym -> {"dir","signal_px","minute","bar_idx"} latest fresh signal
    "lock": threading.Lock(),
    "token2sym": {},
    "realized": 0.0,
    "trade_no": 0,
    "paper": True,
    "kite": None,
    "stop": False,
}

# A recorded signal is only takeable if it fired on the most recent minute
# (within SIGNAL_FRESHNESS_SEC of now). Older recordings are discarded as stale.
SIGNAL_FRESHNESS_SEC = 75      # ~1 bar; a signal older than this is "the move already happened"


def _do_entry(sym, sig, ind, bar_idx, now):
    """Open a position on sym in direction sig. Assumes slot is free + caller holds lock."""
    kite = STATE["kite"]; paper = STATE["paper"]
    signal_px = ind.iloc[bar_idx]["close"]
    try:
        bid, ask, ltp = bid_ask(kite, sym)
    except Exception:
        bid = ask = signal_px
    fill_px = ask if sig == "up" else bid          # buy at ask / short at bid
    qty = max(1, int(ORDER_VALUE_RS / signal_px))
    if not paper:
        ok, fp = place_order(kite, sym, is_buy=(sig == "up"), qty=qty)
        if not ok:
            log.error(f"ENTRY order failed {sym}"); return False
        fill_px = fp
    entry_slip = abs(fill_px - signal_px) / signal_px
    STATE["trade_no"] += 1
    STATE["position"] = Position(symbol=sym, direction=sig, qty=qty,
                                 entry_signal_px=signal_px, entry_fill_px=fill_px,
                                 entry_bar=bar_idx, entry_time=now, extreme=signal_px)
    STATE["pending"].pop(sym, None)                # consumed
    log_slippage({"time": now.isoformat(), "symbol": sym, "event": "entry",
                  "reason": sig, "signal_px": round(signal_px, 2),
                  "fill_px": round(fill_px, 2), "slip_pct": round(entry_slip * 100, 4),
                  "pnl": 0})
    log.info(f"ENTRY #{STATE['trade_no']} [{sig}] {sym} | signal {signal_px:.2f} "
             f"fill {fill_px:.2f} slip {entry_slip*100:.3f}% qty {qty}")
    return True


def _try_enter_freshest(now):
    """Slot is free: among recorded signals fresh within SIGNAL_FRESHNESS_SEC,
    enter the MOST RECENTLY fired one. Discards stale recordings."""
    if now.time() >= NO_NEW_TRADES_AFTER:
        STATE["pending"].clear(); return
    candidates = []
    for s, rec in list(STATE["pending"].items()):
        age = (now - rec["stamp"]).total_seconds()
        if age <= SIGNAL_FRESHNESS_SEC:
            candidates.append((rec["stamp"], s, rec))
        else:
            STATE["pending"].pop(s, None)          # stale -> drop
    if not candidates:
        return
    # freshest = latest stamp
    candidates.sort(key=lambda x: x[0], reverse=True)
    _, s, rec = candidates[0]
    # re-derive current indicators for that stock to confirm signal still valid NOW
    sb = STATE["bars"][s]
    df = sb.df()
    if len(df) < WARMUP_BARS:
        return
    df = df.copy(); df["day"] = pd.to_datetime(df["date"]).dt.date
    ind = add_indicators(df).reset_index(drop=True)
    bar_idx = len(ind) - 1
    sig = eval_entry(ind, STATE["last_exit_bar"].get(s, -10_000))
    if sig == rec["dir"]:
        _do_entry(s, sig, ind, bar_idx, now)
    else:
        STATE["pending"].pop(s, None)              # no longer valid


def handle_closed_bar(sym):
    """Called when a stock's 1-min bar closes. Thread-safe via STATE['lock']."""
    kite = STATE["kite"]; paper = STATE["paper"]
    sb = STATE["bars"][sym]
    df = sb.df()
    if len(df) < WARMUP_BARS:
        return
    df = df.copy()
    df["day"] = pd.to_datetime(df["date"]).dt.date
    ind = add_indicators(df).reset_index(drop=True)
    bar_idx = len(ind) - 1
    now = datetime.now(IST); tnow = now.time()
    pos = STATE["position"]

    # ====== A position is open ======
    if pos is not None:
        if pos.symbol == sym:
            # manage OUR open position
            force = tnow >= SQUAREOFF
            reason = "session_squareoff" if force else eval_exit(ind, pos)
            if reason:
                try:
                    bid, ask, ltp = bid_ask(kite, sym)
                except Exception:
                    bid = ask = ind.iloc[bar_idx]["close"]
                signal_px = ind.iloc[bar_idx]["close"]
                fill_px = bid if pos.direction == "up" else ask
                if not paper:
                    ok, fp = place_order(kite, sym, is_buy=(pos.direction == "down"), qty=pos.qty)
                    if ok:
                        fill_px = fp
                if pos.direction == "up":
                    pnl = (fill_px - pos.entry_fill_px) * pos.qty
                else:
                    pnl = (pos.entry_fill_px - fill_px) * pos.qty
                STATE["realized"] += pnl
                exit_slip = abs(fill_px - signal_px) / signal_px
                log_slippage({"time": now.isoformat(), "symbol": sym, "event": "exit",
                              "reason": reason, "signal_px": round(signal_px, 2),
                              "fill_px": round(fill_px, 2), "slip_pct": round(exit_slip * 100, 4),
                              "pnl": round(pnl, 2)})
                log.info(f"EXIT [{reason}] {sym} {pos.direction} | signal {signal_px:.2f} "
                         f"fill {fill_px:.2f} slip {exit_slip*100:.3f}% | PnL Rs {pnl:,.0f} "
                         f"| session Rs {STATE['realized']:,.0f}")
                STATE["last_exit_bar"][sym] = bar_idx
                STATE["position"] = None
                # slot just freed -> immediately take the freshest pending signal
                _try_enter_freshest(now)
        else:
            # OTHER stock closed a bar while we hold a position:
            # keep monitoring -> RECORD its signal (timestamped) instead of ignoring it
            if tnow < NO_NEW_TRADES_AFTER:
                sig = eval_entry(ind, STATE["last_exit_bar"].get(sym, -10_000))
                if sig is not None:
                    STATE["pending"][sym] = {"dir": sig, "stamp": now,
                                             "signal_px": ind.iloc[bar_idx]["close"]}
                else:
                    STATE["pending"].pop(sym, None)   # signal gone -> clear stale record
        return

    # ====== No position open ======
    if tnow >= NO_NEW_TRADES_AFTER:
        return
    sig = eval_entry(ind, STATE["last_exit_bar"].get(sym, -10_000))
    if sig is not None:
        _do_entry(sym, sig, ind, bar_idx, now)


# ============================================================
# LIVE ORDER (only used if not paper)
# ============================================================
def place_order(kite, sym, is_buy, qty):
    try:
        q = bid_ask(kite, sym)
        price = q[1] if is_buy else q[0]
        oid = kite.place_order(
            tradingsymbol=sym, variety=kite.VARIETY_REGULAR, exchange="NSE",
            transaction_type=(kite.TRANSACTION_TYPE_BUY if is_buy else kite.TRANSACTION_TYPE_SELL),
            quantity=qty, order_type=kite.ORDER_TYPE_MARKET, product=kite.PRODUCT_MIS,
            tag="DIRX", market_protection=MARKET_PROTECTION)
        for _ in range(ORDER_STATUS_MAX_POLLS):
            time.sleep(ORDER_STATUS_POLL_SECONDS)
            for o in reversed(kite.orders()):
                if o.get("order_id") == oid and str(o.get("status", "")).upper() == "COMPLETE":
                    return True, float(o.get("average_price") or price)
        return True, price
    except Exception as e:
        log.error(f"[LIVE] order failed {sym}: {e}")
        return False, None


# ============================================================
# WEBSOCKET CALLBACKS
# ============================================================
def on_ticks(ws, ticks):
    if STATE["stop"]:
        return
    now = datetime.now(IST)
    with STATE["lock"]:
        for t in ticks:
            sym = STATE["token2sym"].get(t.get("instrument_token"))
            if sym is None:
                continue
            ltp = float(t.get("last_price") or 0)
            if ltp <= 0:
                continue
            closed = STATE["bars"][sym].update(now.replace(tzinfo=None), ltp)
            if closed:
                try:
                    handle_closed_bar(sym)
                except Exception as e:
                    log.error(f"handle_closed_bar {sym}: {e}")


def on_connect(ws, response):
    tokens = list(STATE["token2sym"].keys())
    ws.subscribe(tokens)
    ws.set_mode(ws.MODE_FULL, tokens)        # FULL gives depth + ohlc
    log.info(f"WebSocket connected. Subscribed {len(tokens)} tokens (MODE_FULL).")


def on_close(ws, code, reason):
    log.warning(f"WebSocket closed: {code} {reason}")


def on_error(ws, code, reason):
    log.warning(f"WebSocket error: {code} {reason}")


# ============================================================
# HISTORICAL SEED
# ============================================================
def seed_today(kite, sym, token):
    start = datetime.now(IST).replace(hour=9, minute=15, second=0, microsecond=0)
    now = datetime.now(IST)
    try:
        rows = kite.historical_data(token, start, now, "minute", False, False)
    except Exception as e:
        log.warning(f"seed {sym} failed: {e}"); return []
    cur = now.replace(second=0, microsecond=0, tzinfo=None)
    out = []
    for r in rows:
        ts = pd.to_datetime(r["date"])
        if ts.tzinfo is not None:
            ts = ts.tz_convert(IST).tz_localize(None)
        ts = ts.replace(second=0, microsecond=0)
        if ts >= cur:
            continue
        out.append({"date": ts, "open": float(r["open"]), "high": float(r["high"]),
                    "low": float(r["low"]), "close": float(r["close"])})
    return out


# ============================================================
# MAIN
# ============================================================
def main():
    paper = True
    if "--live" in sys.argv:
        if not LIVE_TRADING:
            log.error("Refusing --live: set LIVE_TRADING=True first."); sys.exit(1)
        if input(f'Type "{CONFIRM_PHRASE}" to ARM live orders: ').strip() != CONFIRM_PHRASE:
            log.error("Confirm mismatch; staying PAPER.")
        else:
            paper = False; log.warning("LIVE TRADING ARMED.")
    STATE["paper"] = paper
    log.info(f"Mode: {'LIVE' if not paper else 'PAPER (no real orders)'}")

    if KiteTicker is None:
        log.error("kiteconnect KiteTicker not available."); sys.exit(1)

    kite = oUtils.intialize_kite_api()
    STATE["kite"] = kite

    now_t = datetime.now(IST).time()
    if not (SESSION_START <= now_t <= SESSION_END):
        log.error("Market not open. Run during 09:15-15:30 IST."); sys.exit(1)

    symbols = load_symbols()
    log.info(f"Screening {len(symbols)} symbols for liquidity...")
    top = screen_and_rank(kite, symbols)
    if not top:
        log.error("No liquid names passed screen."); sys.exit(1)
    log.info(f"Monitoring {len(top)} most-liquid: {[t['symbol'] for t in top]}")

    # seed bars + register tokens
    for t in top:
        sym, token = t["symbol"], t["token"]
        sb = StockBars()
        seeded = seed_today(kite, sym, token)
        sb.seed(seeded)
        STATE["bars"][sym] = sb
        STATE["last_exit_bar"][sym] = -10_000
        STATE["token2sym"][token] = sym
        log.info(f"  {sym}: seeded {len(seeded)} bars, token {token}")

    # websocket
    api_key, access_token = oUtils.get_ws_credentials(kite) if hasattr(oUtils, "get_ws_credentials") else (None, None)
    if api_key is None:
        # fallback: many setups expose these on the kite object / env
        api_key = os.getenv("KITE_API_KEY", oUtils.KITE_API_KEY); access_token = os.getenv("KITE_ACCESS_TOKEN", oUtils.KITE_ACCESS_CODE)
    if not api_key or not access_token:
        log.error("Need Kite api_key + access_token for WebSocket. Expose via "
                  "oUtils.get_ws_credentials(kite) or KITE_API_KEY/KITE_ACCESS_TOKEN env.")
        sys.exit(1)

    kws = KiteTicker(api_key, access_token)
    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.on_close = on_close
    kws.on_error = on_error

    log.info("Connecting WebSocket... (Ctrl+C to stop)")
    # run until session end in a background thread; main thread watches the clock
    kws.connect(threaded=True)
    try:
        while True:
            if datetime.now(IST).time() >= SESSION_END:
                break
            time.sleep(2)
    except KeyboardInterrupt:
        log.info("Interrupted by user.")
    finally:
        STATE["stop"] = True
        # close any open paper position at last known price
        with STATE["lock"]:
            pos = STATE["position"]
            if pos is not None:
                try:
                    bid, ask, ltp = bid_ask(kite, pos.symbol)
                    fill = bid if pos.direction == "up" else ask
                except Exception:
                    fill = pos.entry_fill_px
                pnl = ((fill - pos.entry_fill_px) if pos.direction == "up"
                       else (pos.entry_fill_px - fill)) * pos.qty
                STATE["realized"] += pnl
                log.info(f"EOD close {pos.symbol}: PnL Rs {pnl:,.0f}")
        try:
            kws.close()
        except Exception:
            pass
        log.info(f"=== DONE. Trades: {STATE['trade_no']}  Realized Rs {STATE['realized']:,.0f} "
                 f"({'PAPER' if paper else 'LIVE'}) ===")
        log.info(f"Slippage log: {SLIP_LOG}")


if __name__ == "__main__":
    main()
