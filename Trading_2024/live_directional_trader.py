"""
LIVE / PAPER directional ITM options trader.
============================================================================

Watches the index with the NEAREST EXPIRY, runs the SAME directional signal as
the backtest (EMA-fan + ADX + breakout entry; tiered-trail / breakeven /
no-progress exit), and on a trigger BUYS a 1-strike ITM option:
    up   signal -> buy CE one strike ITM (strike below spot)
    down signal -> buy PE one strike ITM (strike above spot)

================  READ THIS BEFORE RUNNING  ================
* DEFAULTS TO PAPER TRADING. No real order is sent unless you set
  LIVE_TRADING = True *and* pass --live on the command line *and* type the
  confirmation phrase when prompted. Three independent gates, on purpose.
* This is experimental. Our edge is validated on 3yr index data but only ~6
  months of options. Start in paper mode for several sessions and compare the
  logged fills/PnL against expectation before risking a single rupee.
* Live order execution is adapted from your ShortStraddleOrdersPlacer.py
  (marketable-limit -> convert-to-market, naked-leg safety).
===========================================================================

Run:
    python live_directional_trader.py            # paper (safe)
    python live_directional_trader.py --live     # live (still asks to confirm)
"""

import os
import sys
import time
import logging
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta, time as dtime
from typing import Optional, List, Dict

import numpy as np
import pandas as pd
import pytz

import OptionTradeUtils as oUtils


# ============================================================
# SAFETY GATES
# ============================================================
LIVE_TRADING = False          # master switch. Must be True AND --live AND typed confirm.
CONFIRM_PHRASE = "TRADE LIVE"  # must be typed exactly to arm live mode

# ============================================================
# CONFIG
# ============================================================
IST = pytz.timezone("Asia/Kolkata")
SESSION_START = dtime(9, 15)
SESSION_END = dtime(15, 30)
SQUAREOFF = dtime(15, 20)       # force-exit any open position at/after this
NO_NEW_TRADES_AFTER = dtime(15, 0)   # don't open fresh trades late in session

POLL_SECONDS = 5                # how often to poll the underlying quote
BAR_SECONDS = 60                # 1-minute bars
WARMUP_BARS = 70                # need >= EMA_SLOW + lookbacks before trading

NO_OF_LOTS_DEFAULT = 1          # overridden by get_instruments if available
ITM_STEPS = 1

# ---- Strategy parameters (walk-forward optimized; match the backtest) ----
EMA_FAST, EMA_MID, EMA_SLOW = 9, 21, 50
SLOPE_LOOKBACK = 5
BREAKOUT_LOOKBACK = 20
ATR_PERIOD = 14
ATR_EXP_LOOKBACK = 30
ATR_EXPANSION = 1.10
ADX_PERIOD = 14
MIN_ADX = 25.0
MIN_FAN_PCT = 0.0008
TRAIL_TIERS = [(0.0100, 1.2), (0.0060, 1.6), (0.0035, 2.2), (0.0000, 3.0)]
MAX_LOSS_PCT = 0.004
MIN_TREND_BARS = 10
COOLDOWN_BARS = 5
PROGRESS_BARS = 8
MIN_PROGRESS_PCT = 0.0010
BREAKEVEN_AFTER_PCT = 0.0015

# Order execution (from reference)
OPTION_TICK = 0.05
ORDER_STATUS_POLL_SECONDS = 0.5
ORDER_STATUS_MAX_POLLS = 8
MARKET_PROTECTION = -1


# ============================================================
# LOGGING
# ============================================================
def setup_logger():
    os.makedirs("live_logs", exist_ok=True)
    fn = os.path.join("live_logs", f"trader_{datetime.now(IST):%Y%m%d_%H%M%S}.log")
    logger = logging.getLogger("trader")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    fh = logging.FileHandler(fn); fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout); sh.setFormatter(fmt)
    logger.addHandler(fh); logger.addHandler(sh)
    logger.info(f"Log file: {fn}")
    return logger


log = setup_logger()


# ============================================================
# INDICATORS  (identical math to the backtest)
# ============================================================
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
# NEAREST-EXPIRY INSTRUMENT RESOLUTION
# ============================================================
def normalize_underlying(name: str) -> Optional[str]:
    u = str(name).upper()
    if "SENSEX" in u:
        return "SENSEX"
    if "NIFTY" in u and "BANK" not in u:
        return "NIFTY"
    return None


def get_option_universe(kite, options_exchange: str) -> pd.DataFrame:
    """All option instruments on the exchange as a DataFrame."""
    inst = pd.DataFrame(kite.instruments(options_exchange))
    inst = inst[inst["instrument_type"].isin(["CE", "PE"])].copy()
    inst["expiry"] = pd.to_datetime(inst["expiry"], errors="coerce").dt.date
    return inst


def nearest_expiry_for(inst: pd.DataFrame, name_part: str, today: date) -> Optional[date]:
    sub = inst[inst["name"].astype(str).str.upper() == name_part.upper()]
    fut = sorted([e for e in sub["expiry"].dropna().unique() if e >= today])
    return fut[0] if fut else None


def find_option_symbol(inst: pd.DataFrame, name_part: str, expiry: date,
                       strike: int, opt_type: str) -> Optional[str]:
    row = inst[(inst["name"].astype(str).str.upper() == name_part.upper())
               & (inst["expiry"] == expiry)
               & (inst["strike"].round().astype("Int64") == strike)
               & (inst["instrument_type"] == opt_type)]
    if row.empty:
        return None
    return str(row.iloc[0]["tradingsymbol"])


def fetch_todays_bars(kite, ul_exchange: str, ul_tradingsymbol: str) -> List[Dict]:
    """Fetch today's 1-min underlying bars so far, to seed the bar builder.
    Returns CLOSED bars only (drops the in-progress current minute)."""
    # resolve underlying token from the exchange instrument dump
    token = None
    want = ul_tradingsymbol.strip().upper()
    for r in kite.instruments(ul_exchange):
        if str(r.get("tradingsymbol", "")).upper() == want:
            token = int(r["instrument_token"]); break
    if token is None:
        log.warning(f"Could not resolve token for {ul_exchange}:{ul_tradingsymbol}; "
                    f"falling back to live warmup.")
        return []
    now = datetime.now(IST)
    start = now.replace(hour=SESSION_START.hour, minute=SESSION_START.minute,
                        second=0, microsecond=0)
    try:
        rows = kite.historical_data(token, start, now, "minute", False, False)
    except Exception as e:
        log.warning(f"historical_data seed failed: {e}; falling back to live warmup.")
        return []
    cur_minute = now.replace(second=0, microsecond=0)
    out = []
    for r in rows:
        ts = pd.to_datetime(r["date"])
        if ts.tzinfo is not None:
            ts = ts.tz_convert(IST).tz_localize(None)
        ts = ts.replace(second=0, microsecond=0)
        # skip the still-forming current minute
        if ts >= cur_minute.replace(tzinfo=None):
            continue
        out.append({"date": ts, "open": float(r["open"]), "high": float(r["high"]),
                    "low": float(r["low"]), "close": float(r["close"])})
    log.info(f"Seeded {len(out)} historical 1-min bars for today (up to {cur_minute.time()}).")
    return out


# ============================================================
# QUOTE / PRICING  (from reference)
# ============================================================
def round_to_tick(price, tick=OPTION_TICK):
    price = max(float(price), tick)
    return round(round(price / tick) * tick, 2)


def get_quote(kite, exchange, tradingsymbol):
    key = f"{exchange}:{tradingsymbol}"
    return kite.quote(key)[key]


def marketable_limit_price(kite, exchange, tradingsymbol, is_buy: bool):
    q = get_quote(kite, exchange, tradingsymbol)
    depth = q.get("depth", {})
    ltp = float(q.get("last_price") or 0.0)
    if is_buy:
        asks = depth.get("sell", [])
        px = float(asks[0]["price"]) if asks and asks[0].get("price") else ltp * 1.005
    else:
        bids = depth.get("buy", [])
        px = float(bids[0]["price"]) if bids and bids[0].get("price") else ltp * 0.995
    return round_to_tick(px)


def option_mid_and_spread(kite, exchange, tradingsymbol):
    """For paper fills: return (mid, bid, ask) from depth."""
    q = get_quote(kite, exchange, tradingsymbol)
    depth = q.get("depth", {})
    ltp = float(q.get("last_price") or 0.0)
    bids = depth.get("buy", []); asks = depth.get("sell", [])
    bid = float(bids[0]["price"]) if bids and bids[0].get("price") else ltp
    ask = float(asks[0]["price"]) if asks and asks[0].get("price") else ltp
    mid = (bid + ask) / 2 if (bid and ask) else ltp
    return mid, bid, ask


# ============================================================
# POSITION STATE
# ============================================================
@dataclass
class Position:
    direction: str
    opt_type: str
    strike: int
    tradingsymbol: str
    qty: int
    entry_underlying: float
    entry_premium: float
    entry_bar: int
    entry_time: datetime
    extreme: float                  # underlying extreme since entry
    be_armed: bool = False
    peak_premium: float = 0.0


# ============================================================
# LIVE ORDER EXECUTION  (adapted from reference, single leg)
# ============================================================
def _snapshot(kite, order_id):
    for o in reversed(kite.orders()):
        if o.get("order_id") == order_id:
            return o
    return None


def place_single_option(kite, exchange, tradingsymbol, is_buy, qty, paper, paper_price):
    """Returns (ok, fill_price). In paper mode simulates a fill at paper_price."""
    txn = "BUY" if is_buy else "SELL"
    if paper:
        log.info(f"[PAPER] {txn} {qty} {tradingsymbol} @ ~{paper_price} (simulated)")
        return True, paper_price
    # ---- LIVE ----
    price = marketable_limit_price(kite, exchange, tradingsymbol, is_buy)
    try:
        oid = kite.place_order(
            tradingsymbol=tradingsymbol, variety=kite.VARIETY_REGULAR, exchange=exchange,
            transaction_type=(kite.TRANSACTION_TYPE_BUY if is_buy else kite.TRANSACTION_TYPE_SELL),
            quantity=qty, order_type=kite.ORDER_TYPE_LIMIT, price=price,
            product=kite.PRODUCT_NRML, tag=getattr(oUtils, "SS_ORDER_TAG", "DIRX"),
        )
        log.info(f"[LIVE] Placed {txn} LIMIT {tradingsymbol} @ {price}, id={oid}")
    except Exception as e:
        log.error(f"[LIVE] place_order failed for {tradingsymbol}: {e}")
        return False, None

    market_modified = False
    for _ in range(ORDER_STATUS_MAX_POLLS):
        time.sleep(ORDER_STATUS_POLL_SECONDS)
        row = _snapshot(kite, oid)
        if row is None:
            continue
        status = str(row.get("status", "")).upper()
        pend = int(row.get("pending_quantity") or 0)
        if status == "COMPLETE" and pend == 0:
            fp = float(row.get("average_price") or price)
            log.info(f"[LIVE] COMPLETE {tradingsymbol} @ {fp}")
            return True, fp
        if status in {"REJECTED", "CANCELLED"}:
            log.error(f"[LIVE] {status} {tradingsymbol}")
            return False, None
        if pend > 0 and not market_modified:
            try:
                kite.modify_order(variety=kite.VARIETY_REGULAR, order_id=oid,
                                  order_type=kite.ORDER_TYPE_MARKET, market_protection=MARKET_PROTECTION)
                market_modified = True
                log.warning(f"[LIVE] {tradingsymbol} pending -> modified to MARKET")
            except Exception as e:
                log.warning(f"[LIVE] modify->MARKET failed {tradingsymbol}: {e}")
    row = _snapshot(kite, oid)
    if row and str(row.get("status", "")).upper() == "COMPLETE":
        return True, float(row.get("average_price") or price)
    log.error(f"[LIVE] {tradingsymbol} unresolved after polling")
    return False, None


# ============================================================
# SIGNAL EVALUATION on the latest closed bar
# ============================================================
def evaluate_entry(ind: pd.DataFrame, last_exit_bar: int) -> Optional[str]:
    n = len(ind)
    i = n - 1                       # latest CLOSED bar
    if i < BREAKOUT_LOOKBACK + SLOPE_LOOKBACK:
        return None
    if i - last_exit_bar < COOLDOWN_BARS:
        return None
    row = ind.iloc[i]
    if pd.isna(row["slope_s"]) or pd.isna(row["roll_hi"]) or pd.isna(row["adx"]) or pd.isna(row["atr_ratio"]):
        return None
    regime = (row["adx"] >= MIN_ADX) and (row["fan_pct"] >= MIN_FAN_PCT) and (row["atr_ratio"] >= ATR_EXPANSION)
    if not regime:
        return None
    up = (row["ema_f"] > row["ema_m"] > row["ema_s"]) and (row["slope_s"] > 0) and (row["close"] >= ind.iloc[i - 1]["roll_hi"])
    down = (row["ema_f"] < row["ema_m"] < row["ema_s"]) and (row["slope_s"] < 0) and (row["close"] <= ind.iloc[i - 1]["roll_lo"])
    if up:
        return "up"
    if down:
        return "down"
    return None


def evaluate_exit(ind: pd.DataFrame, pos: Position) -> Optional[str]:
    """Returns an exit reason string or None. Mirrors backtest exit logic."""
    n = len(ind)
    i = n - 1
    row = ind.iloc[i]
    held = i - pos.entry_bar
    entry_px = pos.entry_underlying
    if pos.direction == "up":
        pos.extreme = max(pos.extreme, row["high"])
        fav = (pos.extreme - entry_px) / entry_px
    else:
        pos.extreme = min(pos.extreme, row["low"])
        fav = (entry_px - pos.extreme) / entry_px
    if fav >= BREAKEVEN_AFTER_PCT:
        pos.be_armed = True
    atr_mult = next(m for thr, m in TRAIL_TIERS if fav >= thr)
    if pos.direction == "up":
        trail = pos.extreme - atr_mult * row["atr"]
        if held >= PROGRESS_BARS and fav < MIN_PROGRESS_PCT:
            return "no_progress"
        if pos.be_armed and row["close"] <= entry_px:
            return "breakeven"
        if row["close"] <= entry_px * (1 - MAX_LOSS_PCT):
            return "max_loss"
        if row["close"] < trail:
            return "trail_stop"
        if row["ema_f"] < row["ema_m"]:
            return "ema_break"
    else:
        trail = pos.extreme + atr_mult * row["atr"]
        if held >= PROGRESS_BARS and fav < MIN_PROGRESS_PCT:
            return "no_progress"
        if pos.be_armed and row["close"] >= entry_px:
            return "breakeven"
        if row["close"] >= entry_px * (1 + MAX_LOSS_PCT):
            return "max_loss"
        if row["close"] > trail:
            return "trail_stop"
        if row["ema_f"] > row["ema_m"]:
            return "ema_break"
    return None


# ============================================================
# BAR AGGREGATION from quotes
# ============================================================
class BarBuilder:
    """Builds 1-min OHLC bars of the underlying from periodic LTP polls."""
    def __init__(self):
        self.bars: List[Dict] = []
        self._cur_minute = None
        self._o = self._h = self._l = self._c = None

    def update(self, ts: datetime, ltp: float):
        minute = ts.replace(second=0, microsecond=0)
        if self._cur_minute is None:
            self._cur_minute = minute
            self._o = self._h = self._l = self._c = ltp
        elif minute != self._cur_minute:
            # close previous bar
            self.bars.append({"date": self._cur_minute, "open": self._o,
                              "high": self._h, "low": self._l, "close": self._c})
            self._cur_minute = minute
            self._o = self._h = self._l = self._c = ltp
        else:
            self._h = max(self._h, ltp); self._l = min(self._l, ltp); self._c = ltp

    def seed(self, hist_bars: List[Dict]):
        """Preload already-closed historical bars (e.g. today's session so far)
        so indicators have history immediately and we can trade from the next
        live bar instead of waiting out a 70-minute warmup."""
        self.bars.extend(hist_bars)

    def closed_df(self) -> pd.DataFrame:
        if not self.bars:
            return pd.DataFrame(columns=["date", "open", "high", "low", "close"])
        df = pd.DataFrame(self.bars)
        # normalize to tz-naive IST so seeded + live bars never mix tz-awareness
        dts = pd.to_datetime(df["date"], utc=False, errors="coerce")
        try:
            if getattr(dts.dt, "tz", None) is not None:
                dts = dts.dt.tz_convert(IST).dt.tz_localize(None)
        except (TypeError, AttributeError):
            pass
        df["date"] = dts
        return df


# ============================================================
# MAIN LOOP
# ============================================================
def main():
    paper = True
    if "--live" in sys.argv:
        if not LIVE_TRADING:
            log.error("Refusing --live: set LIVE_TRADING=True in the file first (gate 1 of 3).")
            sys.exit(1)
        typed = input(f'Type "{CONFIRM_PHRASE}" to ARM LIVE order placement: ').strip()
        if typed != CONFIRM_PHRASE:
            log.error("Confirmation phrase mismatch. Staying in PAPER mode.")
        else:
            paper = False
            log.warning("LIVE TRADING ARMED. Real orders WILL be placed.")
    log.info(f"Mode: {'LIVE' if not paper else 'PAPER (no real orders)'}")

    kite = oUtils.intialize_kite_api()

    # instrument config (reuse your helper if present)
    try:
        (ul_exch, underlying, opt_exch, part_symbol, no_of_lots, strike_mult,
         *_rest) = oUtils.get_instruments(kite)
        part_symbol = part_symbol.replace(":", "")
    except Exception as e:
        log.error(f"get_instruments failed ({e}); set config manually below.")
        ul_exch, underlying = "NSE", ":NIFTY 50"
        opt_exch, part_symbol, no_of_lots, strike_mult = "NFO", "NIFTY", NO_OF_LOTS_DEFAULT, 50

    under_symbol = ul_exch + underlying          # e.g. "NSE:NIFTY 50"
    name_part = normalize_underlying(part_symbol) or part_symbol
    today = datetime.now(IST).date()

    inst = get_option_universe(kite, opt_exch)
    expiry = nearest_expiry_for(inst, name_part, today)
    if expiry is None:
        log.error(f"No future expiry found for {name_part} on {opt_exch}")
        sys.exit(1)
    log.info(f"Underlying {under_symbol} | options {name_part} {opt_exch} | "
             f"nearest expiry {expiry} | lots(qty)={no_of_lots} | strike_mult={strike_mult}")

    bb = BarBuilder()
    # seed with today's bars so far -> trade from the next live bar, no 70min wait
    ul_tsym = underlying.lstrip(":").strip()      # e.g. "NIFTY 50"
    seed_bars = fetch_todays_bars(kite, ul_exch, ul_tsym)
    if seed_bars:
        bb.seed(seed_bars)
    pos: Optional[Position] = None
    last_exit_bar = -10_000
    realized_pnl = 0.0
    trade_no = 0

    log.info("Warming up... collecting bars. No trades until enough history.")
    while True:
        now = datetime.now(IST)
        tnow = now.time()
        if tnow < SESSION_START:
            time.sleep(POLL_SECONDS); continue
        if tnow >= SESSION_END:
            log.info("Session ended.")
            break

        try:
            q = kite.quote(under_symbol)[under_symbol]
            ltp = float(q["last_price"])
        except Exception as e:
            log.warning(f"underlying quote failed: {e}")
            time.sleep(POLL_SECONDS); continue

        prev_bar_count = len(bb.bars)
        bb.update(now.replace(tzinfo=None), ltp)   # tz-naive to match seeded bars
        new_bar_closed = len(bb.bars) > prev_bar_count
        if not new_bar_closed:
            time.sleep(POLL_SECONDS); continue

        # a 1-min bar just closed -> evaluate on closed bars only
        df = bb.closed_df()
        if len(df) < WARMUP_BARS:
            log.info(f"warmup {len(df)}/{WARMUP_BARS} bars (last close {ltp})")
            continue
        df["day"] = pd.to_datetime(df["date"]).dt.date
        ind = add_indicators(df).reset_index(drop=True)
        bar_idx = len(ind) - 1
        last = ind.iloc[bar_idx]

        # ---- manage open position ----
        if pos is not None:
            # square-off time
            force = tnow >= SQUAREOFF
            reason = "session_squareoff" if force else evaluate_exit(ind, pos)
            # track option peak
            try:
                mid, bid, ask = option_mid_and_spread(kite, opt_exch, pos.tradingsymbol)
                pos.peak_premium = max(pos.peak_premium, mid)
            except Exception:
                mid = bid = ask = pos.entry_premium
            if reason:
                # SELL to close. paper fill at bid (conservative), live = marketable.
                ok, fill = place_single_option(kite, opt_exch, pos.tradingsymbol,
                                                is_buy=False, qty=pos.qty, paper=paper,
                                                paper_price=bid)
                if ok:
                    pnl = (fill - pos.entry_premium) * pos.qty
                    realized_pnl += pnl
                    log.info(f"EXIT [{reason}] {pos.tradingsymbol} entry {pos.entry_premium} "
                             f"exit {fill} qty {pos.qty} -> PnL Rs {pnl:,.0f} | "
                             f"session realized Rs {realized_pnl:,.0f}")
                    last_exit_bar = bar_idx
                    pos = None
                else:
                    log.error("EXIT order failed; will retry next bar.")
            else:
                # status line
                upnl = (mid - pos.entry_premium) * pos.qty
                log.info(f"HOLD {pos.tradingsymbol} {pos.direction} | underlying {last['close']:.1f} "
                         f"prem {mid:.2f} uPnL Rs {upnl:,.0f} | bars held {bar_idx - pos.entry_bar}")
            continue

        # ---- look for new entry ----
        if tnow >= NO_NEW_TRADES_AFTER:
            log.info(f"after {NO_NEW_TRADES_AFTER}, no new entries. underlying {last['close']:.1f} "
                     f"adx {last['adx']:.1f}")
            continue
        sig = evaluate_entry(ind, last_exit_bar)
        log.info(f"bar {bar_idx} close {last['close']:.1f} adx {last['adx']:.1f} "
                 f"fan {last['fan_pct']*100:.3f}% atr_ratio {last['atr_ratio']:.2f} "
                 f"slope {last['slope_s']:.2f} -> signal {sig or 'none'}")
        if sig is None:
            continue

        # resolve ITM strike + symbol
        spot = float(last["close"])
        atm = round(spot / strike_mult) * strike_mult
        if sig == "up":
            opt_type = "CE"; strike = int(atm - ITM_STEPS * strike_mult)
        else:
            opt_type = "PE"; strike = int(atm + ITM_STEPS * strike_mult)
        tsym = find_option_symbol(inst, name_part, expiry, strike, opt_type)
        if tsym is None:
            log.warning(f"No option symbol for {name_part} {expiry} {strike}{opt_type}; skipping.")
            continue

        try:
            mid, bid, ask = option_mid_and_spread(kite, opt_exch, tsym)
        except Exception as e:
            log.warning(f"option quote failed {tsym}: {e}; skipping.")
            continue

        # BUY to open. paper fill at ask (conservative), live = marketable.
        ok, fill = place_single_option(kite, opt_exch, tsym, is_buy=True,
                                       qty=no_of_lots, paper=paper, paper_price=ask)
        if not ok:
            log.error(f"ENTRY order failed for {tsym}.")
            continue
        trade_no += 1
        pos = Position(direction=sig, opt_type=opt_type, strike=strike, tradingsymbol=tsym,
                       qty=no_of_lots, entry_underlying=spot, entry_premium=fill,
                       entry_bar=bar_idx, entry_time=now, extreme=spot, peak_premium=mid)
        log.info(f"ENTRY #{trade_no} [{sig}] {tsym} @ {fill} qty {no_of_lots} "
                 f"(spot {spot:.1f}, ITM {opt_type} {strike})")

    # ---- end of session: close any open position ----
    if pos is not None:
        try:
            mid, bid, ask = option_mid_and_spread(kite, opt_exch, pos.tradingsymbol)
        except Exception:
            bid = pos.entry_premium
        ok, fill = place_single_option(kite, opt_exch, pos.tradingsymbol, is_buy=False,
                                       qty=pos.qty, paper=paper, paper_price=bid)
        if ok:
            pnl = (fill - pos.entry_premium) * pos.qty
            realized_pnl += pnl
            log.info(f"EOD CLOSE {pos.tradingsymbol} -> PnL Rs {pnl:,.0f}")
    log.info(f"=== SESSION DONE. Trades: {trade_no}  Realized PnL: Rs {realized_pnl:,.0f} "
             f"({'PAPER' if paper else 'LIVE'}) ===")


if __name__ == "__main__":
    main()
