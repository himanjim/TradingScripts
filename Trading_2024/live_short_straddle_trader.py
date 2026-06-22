"""
============================================================================
live_short_straddle_trader.py
============================================================================
LIVE / PAPER trader that executes the ATM short-straddle re-attempt strategy
defined by the backtester `atm_straddle_claude_reattempt_pct_risk.py` ("B").

It reproduces B's per-day state machine in real time:

  * Enter a SHORT ATM straddle at ENTRY_TIME on an allowed-DTE day.
  * Risk per attempt (all measured against the premium collected on THAT entry):
        - STOP-LOSS  = min( STOP_PCT[attempt] * premium , STOP_CAP_RUPEES )
        - PROFIT-PROTECT: arms when peak profit >= G, exits on give-back of G,
                          where G = PROFIT_PROTECT_PCT * premium.
        - PROFIT-TARGET = PROFIT_TARGET_PCT * premium -> exits AND ends the day.
  * On a STOPLOSS or PROFIT_PROTECT exit, wait REENTRY_DELAY[attempt] minutes
    and re-enter at the NEW ATM (up to MAX_REATTEMPTS). PROFIT_TARGET and EOD
    end the day with no further trades.
  * A daily circuit-breaker stops the day once cumulative realized P&L reaches
    -MAX_DAILY_LOSS_RUPEES.
  * Square everything off at EOD.

Order placement is modelled on `ShortStraddleOrdersPlacer.py` ("A"):
marketable LIMIT on both legs -> convert any unfilled leg to MARKET ->
naked-leg square-off safety net.

P&L is monitored from a Kite WebSocket (KiteTicker) tick feed on the two legs.

PAPER TRADING IS ON BY DEFAULT (PAPER_TRADING=True). In paper mode no real
orders are sent; "fills" are simulated from the live tick price (with a small
configurable slippage) so you can dry-run against the real market safely.

Every trading "epoch" (startup, entry, fills, monitoring heartbeat, each exit
type, re-entry, circuit-breaker, EOD, shutdown) is logged to BOTH a rotating
log file and the console.

NOTE: this is operational trading code. Read it, set PAPER_TRADING and the
parameters to match your backtest, and test in paper mode across several
expiry days before ever setting PAPER_TRADING=False.
============================================================================
"""

import os
import sys
import time
import math
import threading
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, date, time as dtime, timedelta

import pytz

# Your existing helper module (same one A and B import).
import OptionTradeUtils as oUtils

# KiteTicker is the Kite WebSocket client.
try:
    from kiteconnect import KiteTicker
except Exception as _e:  # pragma: no cover
    KiteTicker = None  # surfaced at runtime with a clear message


# ===========================================================================
# 1) CONFIGURATION  (mirror these to your backtest "B" exactly)
# ===========================================================================
# --- SAFETY: paper trading is ON by default. Set to False to send real orders.
PAPER_TRADING = os.getenv("PAPER_TRADING", "1").strip() != "0"

# --- Timing (IST) ----------------------------------------------------------
IST = pytz.timezone("Asia/Calcutta")
ENTRY_TIME_IST = os.getenv("ENTRY_TIME_IST", "11:50")   # B default 11:50
SESSION_START_IST = dtime(9, 15)
SESSION_END_IST = dtime(15, 30)
# Live square-off a little before the close to avoid end-of-day illiquidity.
SQUAREOFF_TIME_IST = os.getenv("SQUAREOFF_TIME_IST", "15:25")

# --- Per-attempt STOP-LOSS as a fraction of premium collected (B's list) ---
# Effective stop = min(STOP_PCT[attempt] * premium, STOP_CAP_RUPEES).
STOP_PCT_BY_ATTEMPT = [0.2978, 0.2999, 0.3019, 0.3040, 0.3061, 0.3081,
                       0.3102, 0.3122, 0.3143, 0.3164, 0.3184]
STOP_CAP_RUPEES = float(os.getenv("STOP_CAP_RUPEES", "3000"))   # 0 disables the cap

# --- Profit-protect: arm + trailing give-back, as a fraction of premium ----
PROFIT_PROTECT_PCT = float(os.getenv("PROFIT_PROTECT_PCT", "0.6660"))  # 0 disables

# --- Per-day profit target as a fraction of premium (ends the day) ---------
PROFIT_TARGET_PCT = float(os.getenv("PROFIT_TARGET_PCT", "0.3593"))    # 0 disables

# --- Per-attempt re-entry gap in minutes (index 0 = before 1st re-entry) ---
REENTRY_DELAY_BY_ATTEMPT = [4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24]

MAX_REATTEMPTS = int(os.getenv("MAX_REATTEMPTS", "10"))   # 1 = only one re-entry

# --- Daily circuit-breaker: stop the day at this cumulative realized loss ---
MAX_DAILY_LOSS_RUPEES = float(os.getenv("MAX_DAILY_LOSS_RUPEES", "30000"))  # 0 disables

# --- Allowed days-to-expiry (B default [0] = expiry day only) --------------
ALLOWED_DTE = [int(x) for x in os.getenv("ALLOWED_DTE", "0").split(",") if x.strip() != ""]
# If we cannot reliably resolve today's DTE, should we still trade?
ENFORCE_DTE = os.getenv("ENFORCE_DTE", "0").strip() == "1"

# --- Live execution / feed knobs -------------------------------------------
MONITOR_POLL_SECONDS = float(os.getenv("MONITOR_POLL_SECONDS", "0.5"))   # how often we evaluate exits
MONITOR_HEARTBEAT_SECONDS = float(os.getenv("MONITOR_HEARTBEAT_SECONDS", "15"))  # P&L log cadence
PAPER_SLIPPAGE_TICKS = int(os.getenv("PAPER_SLIPPAGE_TICKS", "1"))       # simulated adverse fill, in ticks
OPTION_TICK = 0.05

LOG_FILE = os.getenv("LOG_FILE", os.path.join(os.path.expanduser("~"),
                                              "short_straddle_live.log"))


# ===========================================================================
# 2) LOGGING  (file + console; every epoch is recorded)
# ===========================================================================
def _build_logger() -> logging.Logger:
    lg = logging.getLogger("ss_live")
    lg.setLevel(logging.INFO)
    lg.propagate = False
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    # Rotating file handler so long-running sessions don't grow unbounded.
    fh = RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=5)
    fh.setFormatter(fmt)
    # Console handler.
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    if not lg.handlers:           # avoid duplicate handlers on re-import
        lg.addHandler(fh)
        lg.addHandler(ch)
    return lg


log = _build_logger()


# ===========================================================================
# 3) SMALL HELPERS  (faithful to B's parameter semantics)
# ===========================================================================
def now_ist() -> datetime:
    """Current wall-clock time in IST."""
    return datetime.now(IST)


def parse_hhmm(s: str) -> dtime:
    """'HH:MM' -> datetime.time."""
    hh, mm = s.strip().split(":")
    return dtime(int(hh), int(mm))


def round_to_tick(price: float, tick: float = OPTION_TICK) -> float:
    """Round a price to the nearest exchange tick (>= one tick)."""
    price = max(float(price), tick)
    return round(round(price / tick) * tick, 2)


def round_to_step(x: float, step: int) -> int:
    """Round an underlying price to the nearest strike step (B's ATM rule)."""
    return int(round(float(x) / step) * step)


def stop_pct_for_attempt(attempt_idx: int) -> float:
    """STOP_PCT for attempt index (0 = first entry); reuse last beyond list."""
    s = STOP_PCT_BY_ATTEMPT
    if not s:
        return 0.0
    return float(s[attempt_idx]) if attempt_idx < len(s) else float(s[-1])


def reentry_delay_for_attempt(attempt_idx: int) -> int:
    """Re-entry gap (minutes) before the next attempt; reuse last beyond list."""
    s = REENTRY_DELAY_BY_ATTEMPT
    if not s:
        return 0
    return int(s[attempt_idx]) if attempt_idx < len(s) else int(s[-1])


def effective_stop_rupees(attempt_idx: int, premium_sum_rupees: float) -> float:
    """min(STOP_PCT * premium, STOP_CAP) -- exactly B's capped stop."""
    uncapped = stop_pct_for_attempt(attempt_idx) * premium_sum_rupees
    if STOP_CAP_RUPEES > 0:
        return float(min(uncapped, STOP_CAP_RUPEES))
    return float(uncapped)


# ===========================================================================
# 4) PRICE FEED  (Kite WebSocket -> latest LTP per instrument token)
# ===========================================================================
class PriceFeed:
    """
    Thin wrapper over KiteTicker. Maintains the latest traded price per
    instrument token in a dict that the strategy thread reads each poll.
    Supports dynamic (un)subscription because each re-entry uses new strikes.
    """

    def __init__(self, api_key: str, access_token: str):
        if KiteTicker is None:
            raise RuntimeError("kiteconnect is not installed: pip install kiteconnect")
        self.ticker = KiteTicker(api_key, access_token)
        self.ltp = {}                      # token -> last_price
        self._subscribed = set()
        self._connected = threading.Event()

        # Bind callbacks.
        self.ticker.on_ticks = self._on_ticks
        self.ticker.on_connect = self._on_connect
        self.ticker.on_close = self._on_close
        self.ticker.on_error = self._on_error

    # --- websocket callbacks (run on the ticker's own thread) ---
    def _on_ticks(self, ws, ticks):
        for t in ticks:
            tok = t.get("instrument_token")
            px = t.get("last_price")
            if tok is not None and px is not None:
                self.ltp[tok] = float(px)

    def _on_connect(self, ws, response):
        log.info("[WS] Connected to Kite ticker.")
        self._connected.set()
        if self._subscribed:               # re-subscribe after a reconnect
            ws.subscribe(list(self._subscribed))
            ws.set_mode(ws.MODE_LTP, list(self._subscribed))

    def _on_close(self, ws, code, reason):
        log.warning(f"[WS] Closed (code={code}, reason={reason}).")

    def _on_error(self, ws, code, reason):
        log.warning(f"[WS] Error (code={code}, reason={reason}).")

    # --- control ---
    def start(self, timeout: float = 15.0):
        log.info("[WS] Starting ticker (threaded) ...")
        self.ticker.connect(threaded=True)
        if not self._connected.wait(timeout=timeout):
            log.warning("[WS] Ticker not confirmed connected within timeout; continuing.")

    def stop(self):
        try:
            self.ticker.close()
        except Exception:
            pass

    def subscribe(self, tokens):
        tokens = [int(t) for t in tokens]
        self._subscribed.update(tokens)
        try:
            self.ticker.subscribe(tokens)
            self.ticker.set_mode(self.ticker.MODE_LTP, tokens)
            log.info(f"[WS] Subscribed tokens: {tokens}")
        except Exception as e:
            log.warning(f"[WS] subscribe failed for {tokens}: {e}")

    def unsubscribe(self, tokens):
        tokens = [int(t) for t in tokens]
        self._subscribed.difference_update(tokens)
        try:
            self.ticker.unsubscribe(tokens)
        except Exception as e:
            log.warning(f"[WS] unsubscribe failed for {tokens}: {e}")

    def wait_for(self, tokens, timeout: float = 10.0) -> bool:
        """Block until every token has at least one tick (or timeout)."""
        deadline = time.time() + timeout
        tokens = [int(t) for t in tokens]
        while time.time() < deadline:
            if all(t in self.ltp for t in tokens):
                return True
            time.sleep(0.1)
        return all(t in self.ltp for t in tokens)

    def get(self, token):
        return self.ltp.get(int(token))


# ===========================================================================
# 5) BROKER  (paper + live order placement; live flow adapted from A)
# ===========================================================================
class Broker:
    """
    Places and squares off the two straddle legs.

    In PAPER mode nothing is sent to the exchange; fills are simulated from the
    live tick price plus an adverse slippage, so dry-runs still react to the
    real market. In LIVE mode it follows A's pattern: marketable LIMIT on both
    legs, convert any unfilled leg to MARKET, and a naked-leg square-off net.
    """

    ORDER_STATUS_POLL_SECONDS = 0.5
    ORDER_STATUS_MAX_POLLS = 8
    MARKET_PROTECTION = -1

    def __init__(self, kite, feed: PriceFeed, options_exchange: str, paper: bool):
        self.kite = kite
        self.feed = feed
        self.exchange = options_exchange
        self.paper = paper

    # ----- paper helpers -----
    def _paper_fill(self, token: float, side: str) -> float:
        """
        Simulate a fill from the latest tick. Selling fills slightly below and
        buying slightly above LTP by PAPER_SLIPPAGE_TICKS (conservative).
        """
        ltp = self.feed.get(token)
        if ltp is None:
            raise RuntimeError("No tick yet for paper fill")
        slip = PAPER_SLIPPAGE_TICKS * OPTION_TICK
        px = ltp - slip if side == "SELL" else ltp + slip
        return round_to_tick(px)

    # ----- live helpers (mirrors A) -----
    def _marketable_limit_price(self, tradingsymbol: str, transaction_type) -> float:
        q = self.kite.quote(f"{self.exchange}:{tradingsymbol}")[f"{self.exchange}:{tradingsymbol}"]
        depth = q.get("depth", {})
        buy_depth = depth.get("buy", [])
        sell_depth = depth.get("sell", [])
        ltp = float(q.get("last_price") or 0.0)
        if transaction_type == self.kite.TRANSACTION_TYPE_SELL:
            px = float(buy_depth[0]["price"]) if (buy_depth and buy_depth[0].get("price")) else ltp * 0.995
        else:
            px = float(sell_depth[0]["price"]) if (sell_depth and sell_depth[0].get("price")) else ltp * 1.005
        return round_to_tick(px)

    def _order_snapshot(self, order_id):
        for o in reversed(self.kite.orders()):
            if o.get("order_id") == order_id:
                return o
        return None

    def _ensure_filled_or_market(self, order_id, tradingsymbol) -> tuple:
        """Wait for fill; convert pending leg to MARKET. Returns (ok, avg_price)."""
        market_modified = False
        for _ in range(self.ORDER_STATUS_MAX_POLLS):
            time.sleep(self.ORDER_STATUS_POLL_SECONDS)
            row = self._order_snapshot(order_id)
            if row is None:
                continue
            status = str(row.get("status", "")).upper()
            pending = int(row.get("pending_quantity") or 0)
            avg = float(row.get("average_price") or 0.0)
            if status == "COMPLETE" and pending == 0:
                log.info(f"[ORDER] {tradingsymbol}: COMPLETE @ {avg}")
                return True, avg
            if status in {"REJECTED", "CANCELLED"}:
                log.error(f"[ORDER] {tradingsymbol}: {status}")
                return False, avg
            if pending > 0 and not market_modified:
                try:
                    self.kite.modify_order(
                        variety=self.kite.VARIETY_REGULAR, order_id=order_id,
                        order_type=self.kite.ORDER_TYPE_MARKET,
                        market_protection=self.MARKET_PROTECTION,
                    )
                    market_modified = True
                    log.warning(f"[ORDER] {tradingsymbol}: pending -> converted to MARKET.")
                except Exception as e:
                    log.warning(f"[ORDER] {tradingsymbol}: modify-to-MARKET failed: {e}")
        row = self._order_snapshot(order_id)
        if row and str(row.get("status", "")).upper() == "COMPLETE":
            return True, float(row.get("average_price") or 0.0)
        log.warning(f"[ORDER] {tradingsymbol}: unresolved final state {row.get('status') if row else 'NA'}")
        return False, float(row.get("average_price") or 0.0) if row else 0.0

    def _square_off_naked(self, tradingsymbol):
        for p in self.kite.positions()["net"]:
            if p["tradingsymbol"] == tradingsymbol and int(p["quantity"]) != 0:
                net = int(p["quantity"])
                txn = self.kite.TRANSACTION_TYPE_BUY if net < 0 else self.kite.TRANSACTION_TYPE_SELL
                try:
                    self.kite.place_order(
                        tradingsymbol=tradingsymbol, variety=self.kite.VARIETY_REGULAR,
                        exchange=self.exchange, transaction_type=txn, quantity=abs(net),
                        order_type=self.kite.ORDER_TYPE_MARKET, product=self.kite.PRODUCT_NRML,
                        tag=oUtils.SS_ORDER_TAG, market_protection=self.MARKET_PROTECTION,
                    )
                    log.warning(f"[SAFETY] Squared off naked leg {tradingsymbol} qty={abs(net)}")
                except Exception as e:
                    log.error(f"[SAFETY] Failed to square off {tradingsymbol}: {e}")
                break

    # ----- public API used by the strategy -----
    def open_short_straddle(self, pe_sym, ce_sym, pe_tok, ce_tok, qty) -> dict:
        """SELL both legs. Returns {'pe_fill':p, 'ce_fill':p, 'ok':bool}."""
        if self.paper:
            pe_fill = self._paper_fill(pe_tok, "SELL")
            ce_fill = self._paper_fill(ce_tok, "SELL")
            log.info(f"[PAPER] SELL {pe_sym} @ {pe_fill} | SELL {ce_sym} @ {ce_fill} (qty={qty})")
            return {"pe_fill": pe_fill, "ce_fill": ce_fill, "ok": True}

        txn = self.kite.TRANSACTION_TYPE_SELL
        pe_px = self._marketable_limit_price(pe_sym, txn)
        ce_px = self._marketable_limit_price(ce_sym, txn)
        pe_id = self.kite.place_order(tradingsymbol=pe_sym, variety=self.kite.VARIETY_REGULAR,
                                      exchange=self.exchange, transaction_type=txn, quantity=qty,
                                      order_type=self.kite.ORDER_TYPE_LIMIT, price=pe_px,
                                      product=self.kite.PRODUCT_NRML, tag=oUtils.SS_ORDER_TAG)
        ce_id = self.kite.place_order(tradingsymbol=ce_sym, variety=self.kite.VARIETY_REGULAR,
                                      exchange=self.exchange, transaction_type=txn, quantity=qty,
                                      order_type=self.kite.ORDER_TYPE_LIMIT, price=ce_px,
                                      product=self.kite.PRODUCT_NRML, tag=oUtils.SS_ORDER_TAG)
        log.info(f"[LIVE] SELL LIMIT {pe_sym}@{pe_px}, {ce_sym}@{ce_px} (qty={qty})")
        pe_ok, pe_fill = self._ensure_filled_or_market(pe_id, pe_sym)
        ce_ok, ce_fill = self._ensure_filled_or_market(ce_id, ce_sym)
        if not (pe_ok and ce_ok):           # naked-leg guard
            self._square_off_naked(pe_sym)
            self._square_off_naked(ce_sym)
        return {"pe_fill": pe_fill, "ce_fill": ce_fill, "ok": pe_ok and ce_ok}

    def close_short_straddle(self, pe_sym, ce_sym, pe_tok, ce_tok, qty) -> dict:
        """BUY back both legs to flatten. Returns {'pe_fill','ce_fill','ok'}."""
        if self.paper:
            pe_fill = self._paper_fill(pe_tok, "BUY")
            ce_fill = self._paper_fill(ce_tok, "BUY")
            log.info(f"[PAPER] BUY {pe_sym} @ {pe_fill} | BUY {ce_sym} @ {ce_fill} (qty={qty})")
            return {"pe_fill": pe_fill, "ce_fill": ce_fill, "ok": True}

        txn = self.kite.TRANSACTION_TYPE_BUY
        pe_px = self._marketable_limit_price(pe_sym, txn)
        ce_px = self._marketable_limit_price(ce_sym, txn)
        pe_id = self.kite.place_order(tradingsymbol=pe_sym, variety=self.kite.VARIETY_REGULAR,
                                      exchange=self.exchange, transaction_type=txn, quantity=qty,
                                      order_type=self.kite.ORDER_TYPE_LIMIT, price=pe_px,
                                      product=self.kite.PRODUCT_NRML, tag=oUtils.SS_ORDER_TAG)
        ce_id = self.kite.place_order(tradingsymbol=ce_sym, variety=self.kite.VARIETY_REGULAR,
                                      exchange=self.exchange, transaction_type=txn, quantity=qty,
                                      order_type=self.kite.ORDER_TYPE_LIMIT, price=ce_px,
                                      product=self.kite.PRODUCT_NRML, tag=oUtils.SS_ORDER_TAG)
        log.info(f"[LIVE] BUY LIMIT {pe_sym}@{pe_px}, {ce_sym}@{ce_px} (qty={qty})")
        pe_ok, pe_fill = self._ensure_filled_or_market(pe_id, pe_sym)
        ce_ok, ce_fill = self._ensure_filled_or_market(ce_id, ce_sym)
        return {"pe_fill": pe_fill, "ce_fill": ce_fill, "ok": pe_ok and ce_ok}


# ===========================================================================
# 6) STRATEGY ENGINE  (B's per-day state machine, tick-driven)
# ===========================================================================
class LiveStraddleTrader:
    def __init__(self, kite, feed: PriceFeed, broker: Broker, *,
                 underlying_quote_key: str, part_symbol: str,
                 strike_step: int, qty: int):
        self.kite = kite
        self.feed = feed
        self.broker = broker
        self.underlying_quote_key = underlying_quote_key   # e.g. "NSE:NIFTY 50"
        self.part_symbol = part_symbol                     # e.g. "NIFTY24D05" prefix
        self.strike_step = int(strike_step)
        self.qty = int(qty)

        self.entry_time = parse_hhmm(ENTRY_TIME_IST)
        self.squareoff_time = parse_hhmm(SQUAREOFF_TIME_IST)
        self.daily_realized_pnl = 0.0

    # ----- token / symbol resolution -----
    def _underlying_ltp(self) -> float:
        q = self.kite.ltp([self.underlying_quote_key])[self.underlying_quote_key]
        return float(q["last_price"])

    def _resolve_option(self, tradingsymbol: str):
        """Return (instrument_token, ltp) for an option tradingsymbol."""
        key = f"{self.broker.exchange}:{tradingsymbol}"
        info = self.kite.ltp([key])[key]
        return int(info["instrument_token"]), float(info["last_price"])

    # ----- waiting utilities (with EOD guard) -----
    def _sleep_until(self, target: dtime, label: str) -> bool:
        """Sleep until wall-clock reaches target time. False if EOD passed."""
        log.info(f"[WAIT] Waiting until {target.strftime('%H:%M')} ({label}) ...")
        while True:
            t = now_ist().time()
            if t >= self.squareoff_time:
                return False
            if t >= target:
                return True
            time.sleep(1.0)

    # ----- one attempt: enter, monitor, exit -----
    def run_attempt(self, attempt_idx: int) -> str:
        """
        Place one short straddle and manage it until an exit fires.
        Returns the exit reason: STOPLOSS | PROFIT_TARGET | PROFIT_PROTECT | EOD.
        Updates self.daily_realized_pnl.
        """
        # --- choose ATM from live underlying ---
        ul = self._underlying_ltp()
        atm = round_to_step(ul, self.strike_step)
        pe_sym = f"{self.part_symbol}{atm}PE"
        ce_sym = f"{self.part_symbol}{atm}CE"
        log.info(f"[ENTRY] attempt #{attempt_idx+1}: underlying={ul:.2f} -> ATM={atm} "
                 f"| {ce_sym} / {pe_sym}")

        # --- resolve tokens, subscribe to the feed, wait for first ticks ---
        try:
            pe_tok, _ = self._resolve_option(pe_sym)
            ce_tok, _ = self._resolve_option(ce_sym)
        except Exception as e:
            log.error(f"[ENTRY] Could not resolve option tokens ({e}); skipping attempt.")
            return "EOD"
        self.feed.subscribe([pe_tok, ce_tok])
        if not self.feed.wait_for([pe_tok, ce_tok], timeout=10):
            log.error("[ENTRY] No ticks for legs; skipping attempt.")
            self.feed.unsubscribe([pe_tok, ce_tok])
            return "EOD"

        # --- open the short straddle ---
        fills = self.broker.open_short_straddle(pe_sym, ce_sym, pe_tok, ce_tok, self.qty)
        ce_entry, pe_entry = fills["ce_fill"], fills["pe_fill"]
        premium_sum = (ce_entry + pe_entry) * self.qty

        # --- per-attempt risk thresholds (rupees), exactly as in B ---
        stop_rupees = effective_stop_rupees(attempt_idx, premium_sum)
        target_rupees = PROFIT_TARGET_PCT * premium_sum if PROFIT_TARGET_PCT > 0 else None
        G = PROFIT_PROTECT_PCT * premium_sum if PROFIT_PROTECT_PCT > 0 else None
        log.info(f"[ENTRY] filled CE={ce_entry} PE={pe_entry} premium=Rs{premium_sum:,.0f} "
                 f"| stop=Rs{stop_rupees:,.0f} "
                 f"| target={'Rs%.0f' % target_rupees if target_rupees else 'off'} "
                 f"| protectG={'Rs%.0f' % G if G else 'off'}")

        # --- monitor the position on the tick feed ---
        peak = 0.0
        armed = False
        last_hb = 0.0
        exit_reason = "EOD"
        while True:
            now_t = now_ist().time()
            ce_ltp = self.feed.get(ce_tok)
            pe_ltp = self.feed.get(pe_tok)

            # EOD square-off check first.
            if now_t >= self.squareoff_time:
                exit_reason = "EOD"
                break

            if ce_ltp is None or pe_ltp is None:
                time.sleep(MONITOR_POLL_SECONDS)
                continue

            # Live MTM profit of the short straddle (rupees).
            pnl = (ce_entry - ce_ltp) * self.qty + (pe_entry - pe_ltp) * self.qty
            peak = max(peak, pnl)
            if G is not None and not armed and peak >= G:
                armed = True
                log.info(f"[PROTECT] armed: peak=Rs{peak:,.0f} >= G=Rs{G:,.0f}")

            # Heartbeat P&L log.
            if time.time() - last_hb >= MONITOR_HEARTBEAT_SECONDS:
                log.info(f"[MONITOR] pnl=Rs{pnl:,.0f} peak=Rs{peak:,.0f} "
                         f"(CE {ce_ltp} / PE {pe_ltp}) armed={armed}")
                last_hb = time.time()

            # --- exit checks; same priority as B: STOP > TARGET > PROTECT ---
            if pnl <= -stop_rupees:
                exit_reason = "STOPLOSS"
                break
            if target_rupees is not None and pnl >= target_rupees:
                exit_reason = "PROFIT_TARGET"
                break
            if armed and G is not None and pnl <= (peak - G):
                exit_reason = "PROFIT_PROTECT"
                break

            time.sleep(MONITOR_POLL_SECONDS)

        # --- close the position ---
        close = self.broker.close_short_straddle(pe_sym, ce_sym, pe_tok, ce_tok, self.qty)
        ce_exit, pe_exit = close["ce_fill"], close["pe_fill"]
        gross = (ce_entry - ce_exit) * self.qty + (pe_entry - pe_exit) * self.qty
        self.daily_realized_pnl += gross
        log.info(f"[EXIT] {exit_reason}: CE {ce_entry}->{ce_exit}, PE {pe_entry}->{pe_exit} "
                 f"| gross=Rs{gross:,.0f} | day_realized=Rs{self.daily_realized_pnl:,.0f}")

        self.feed.unsubscribe([pe_tok, ce_tok])
        return exit_reason

    # ----- full trading day -----
    def run_day(self):
        log.info("=" * 70)
        log.info(f"[DAY] {date.today()} | mode={'PAPER' if self.broker.paper else 'LIVE'} "
                 f"| entry={ENTRY_TIME_IST} | squareoff={SQUAREOFF_TIME_IST}")
        log.info(f"[DAY] qty={self.qty} step={self.strike_step} "
                 f"max_reattempts={MAX_REATTEMPTS} daily_loss_cap=Rs{MAX_DAILY_LOSS_RUPEES:,.0f}")

        # Wait for the configured entry time.
        if now_ist().time() < self.entry_time:
            if not self._sleep_until(self.entry_time, "entry time"):
                log.info("[DAY] Square-off time reached before entry; nothing to do.")
                return
        elif now_ist().time() >= self.squareoff_time:
            log.info("[DAY] Past square-off time at startup; nothing to do.")
            return
        else:
            log.warning("[DAY] Started AFTER entry time; entering immediately on this run.")

        attempt_idx = 0
        while True:
            # Daily circuit-breaker (checked before each entry, as in B).
            if MAX_DAILY_LOSS_RUPEES > 0 and self.daily_realized_pnl <= -MAX_DAILY_LOSS_RUPEES:
                log.warning(f"[BREAKER] Daily loss cap hit "
                            f"(realized=Rs{self.daily_realized_pnl:,.0f}); no more trades today.")
                break
            if now_ist().time() >= self.squareoff_time:
                log.info("[DAY] Square-off time reached; ending the day.")
                break

            exit_reason = self.run_attempt(attempt_idx)

            # Re-entry rule mirrors B: only STOPLOSS / PROFIT_PROTECT re-enter.
            if exit_reason in ("STOPLOSS", "PROFIT_PROTECT") and attempt_idx < MAX_REATTEMPTS:
                delay = reentry_delay_for_attempt(attempt_idx)
                attempt_idx += 1
                target_t = (now_ist() + timedelta(minutes=delay)).time()
                log.info(f"[REENTRY] {exit_reason} -> wait {delay} min, next attempt #{attempt_idx+1}")
                if not self._sleep_until(target_t, f"re-entry #{attempt_idx+1}"):
                    log.info("[DAY] Square-off reached during re-entry wait; ending day.")
                    break
                continue

            # PROFIT_TARGET or EOD -> the day is done.
            log.info(f"[DAY] No re-entry after {exit_reason}; day complete.")
            break

        log.info(f"[DAY DONE] total realized P&L = Rs{self.daily_realized_pnl:,.0f}")


# ===========================================================================
# 7) MAIN
# ===========================================================================
def main():
    log.info("#" * 70)
    log.info("[BOOT] Short-straddle live trader starting "
             f"({'PAPER' if PAPER_TRADING else 'LIVE'} mode).")
    if not PAPER_TRADING:
        log.warning("[BOOT] LIVE MODE: real orders WILL be placed. Ctrl+C to abort.")

    # --- Kite session ---
    kite = oUtils.intialize_kite_api()
    log.info("[BOOT] Kite session initialised.")

    # --- Instrument config (same source A uses) ---
    (UNDER_LYING_EXCHANGE, UNDERLYING, OPTIONS_EXCHANGE, PART_SYMBOL,
     NO_OF_LOTS, STRIKE_MULTIPLE, STOPLOSS_POINTS, MINIMUM_LOTS,
     LONG_STRADDLE_STRIKE_DISTANCE) = oUtils.get_instruments(kite)
    PART_SYMBOL = PART_SYMBOL.replace(":", "")
    underlying_quote_key = UNDER_LYING_EXCHANGE + UNDERLYING
    log.info(f"[BOOT] Instruments: underlying='{underlying_quote_key}', "
             f"options_exchange={OPTIONS_EXCHANGE}, part='{PART_SYMBOL}', "
             f"qty={NO_OF_LOTS}, strike_step={STRIKE_MULTIPLE}")
    log.info(f"[BOOT] Strategy params: stop_pct[0]={STOP_PCT_BY_ATTEMPT[0]:.4f} "
             f"cap=Rs{STOP_CAP_RUPEES:,.0f} | protect={PROFIT_PROTECT_PCT:.4f} "
             f"| target={PROFIT_TARGET_PCT:.4f} | reentry_gaps={REENTRY_DELAY_BY_ATTEMPT} "
             f"| allowed_dte={ALLOWED_DTE}")

    # --- WebSocket feed (KiteTicker needs api_key + access_token) ---
    api_key = getattr(kite, "api_key", None) or getattr(oUtils, "API_KEY", None)
    access_token = getattr(kite, "access_token", None) or getattr(oUtils, "ACCESS_TOKEN", None)
    if not api_key or not access_token:
        log.error("[BOOT] Could not obtain api_key/access_token for KiteTicker. "
                  "Expose them via oUtils.API_KEY / oUtils.ACCESS_TOKEN and retry.")
        return
    feed = PriceFeed(api_key, access_token)
    feed.start()

    broker = Broker(kite, feed, OPTIONS_EXCHANGE, paper=PAPER_TRADING)
    trader = LiveStraddleTrader(
        kite, feed, broker,
        underlying_quote_key=underlying_quote_key,
        part_symbol=PART_SYMBOL,
        strike_step=int(STRIKE_MULTIPLE),
        qty=int(NO_OF_LOTS),
    )

    try:
        trader.run_day()
    except KeyboardInterrupt:
        log.warning("[SHUTDOWN] Interrupted by user.")
    except Exception as e:
        log.exception(f"[FATAL] Unhandled error: {e}")
    finally:
        feed.stop()
        log.info("[SHUTDOWN] Feed closed. Bye.")


if __name__ == "__main__":
    main()
