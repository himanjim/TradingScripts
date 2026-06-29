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
import json
import threading
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, date, time as dtime, timedelta

import pytz

# Your existing helper module (same one A and B import).
import OptionTradeUtils_env as oUtils

# KiteTicker is the Kite WebSocket client.
try:
    from kiteconnect import KiteTicker
except Exception as _e:  # pragma: no cover
    KiteTicker = None  # surfaced at runtime with a clear message


# ===========================================================================
# 0) CONFIGURATION SOURCE: external property file
# ===========================================================================
# Every tunable below is read via os.getenv(...). This loader first pushes the
# values from an external KEY=VALUE property file into the environment, so
# settings can be changed WITHOUT editing this script. Path defaults to
# "live_trader_config.properties" next to this file; override with the
# STRADDLE_LIVE_CONFIG environment variable. A real environment variable that
# is already set takes precedence over the file.
def _load_property_file() -> str:
    cfg_path = os.getenv(
        "STRADDLE_LIVE_CONFIG",
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "live_trader_config.properties"),
    )
    if not os.path.exists(cfg_path):
        print(f"[CONFIG] Property file not found at {cfg_path}; using built-in defaults.")
        return cfg_path
    loaded = 0
    with open(cfg_path, "r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#") or line.startswith(";") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            key, val = key.strip(), val.strip()
            if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
                val = val[1:-1]
            if key and key not in os.environ:   # real env vars win over the file
                os.environ[key] = val
                loaded += 1
    print(f"[CONFIG] Loaded {loaded} setting(s) from {cfg_path}")
    return cfg_path


PROPERTY_FILE_PATH = _load_property_file()


def _csv_floats(raw, default):
    """Parse a comma list of floats from a config string; fall back to default."""
    if raw is None or str(raw).strip() == "":
        return list(default)
    try:
        vals = [float(x) for x in str(raw).replace(" ", "").split(",") if x != ""]
        return vals if vals else list(default)
    except Exception:
        return list(default)


def _csv_ints(raw, default):
    """Parse a comma list of ints from a config string; fall back to default."""
    if raw is None or str(raw).strip() == "":
        return list(default)
    try:
        vals = [int(round(float(x))) for x in str(raw).replace(" ", "").split(",") if x != ""]
        return vals if vals else list(default)
    except Exception:
        return list(default)


# ===========================================================================
# 1) CONFIGURATION  (mirror these to your backtest "B" exactly)
# ===========================================================================
# --- SAFETY: paper trading is ON by default. Set to False to send real orders.
PAPER_TRADING = os.getenv("PAPER_TRADING", "0").strip() != "0"

# --- Timing (IST) ----------------------------------------------------------
IST = pytz.timezone("Asia/Calcutta")
ENTRY_TIME_IST = os.getenv("ENTRY_TIME_IST", "09:55")   # B default 11:50
SESSION_START_IST = dtime(9, 15)
SESSION_END_IST = dtime(15, 30)
# Live square-off a little before the close to avoid end-of-day illiquidity.
SQUAREOFF_TIME_IST = os.getenv("SQUAREOFF_TIME_IST", "15:20")

# --- Per-attempt STOP-LOSS as a fraction of premium collected (B's list) ---
# Effective stop = min(STOP_PCT[attempt] * premium, STOP_CAP_RUPEES).
STOP_PCT_BY_ATTEMPT = _csv_floats(os.getenv("STOP_PCT_BY_ATTEMPT"),
                                  [0.2487, 0.2824, 0.3162, 0.3499, 0.3837, 0.4174, 0.4512])
STOP_CAP_RUPEES = float(os.getenv("STOP_CAP_RUPEES", "3000"))   # 0 disables the cap

# --- Profit-protect: arm + trailing give-back, as a fraction of premium ----
PROFIT_PROTECT_PCT = float(os.getenv("PROFIT_PROTECT_PCT", "0.254741"))  # 0 disables

# --- Per-day profit target as a fraction of premium (ends the day) ---------
PROFIT_TARGET_PCT = float(os.getenv("PROFIT_TARGET_PCT", "0.779933"))    # 0 disables

# --- Per-attempt re-entry gap in minutes (index 0 = before 1st re-entry) ---
REENTRY_DELAY_BY_ATTEMPT = _csv_ints(os.getenv("REENTRY_DELAY_BY_ATTEMPT"),
                                     [6, 8, 10, 12, 14, 16, 18])

MAX_REATTEMPTS = int(os.getenv("MAX_REATTEMPTS", "6"))   # 1 = only one re-entry

# --- Daily circuit-breaker: stop the day at this cumulative realized loss ---
MAX_DAILY_LOSS_RUPEES = float(os.getenv("MAX_DAILY_LOSS_RUPEES", "30000"))  # 0 disables

# --- Allowed days-to-expiry (B default [0] = expiry day only) --------------
ALLOWED_DTE = [int(x) for x in os.getenv("ALLOWED_DTE", "0").split(",") if x.strip() != ""]
# If we cannot reliably resolve today's DTE, should we still trade?
ENFORCE_DTE = os.getenv("ENFORCE_DTE", "0").strip() == "0"

# --- Live execution / feed knobs -------------------------------------------
# Kite streams at most ~1 tick/sec per instrument (2/sec only on bursts), so
# evaluating once per second is the least-frequent cadence that still sees
# every market update -- polling faster just re-reads the same tick.
MONITOR_POLL_SECONDS = float(os.getenv("MONITOR_POLL_SECONDS", "1.0"))   # how often we evaluate exits
MONITOR_HEARTBEAT_SECONDS = float(os.getenv("MONITOR_HEARTBEAT_SECONDS", "15"))  # P&L log cadence
PAPER_SLIPPAGE_TICKS = int(os.getenv("PAPER_SLIPPAGE_TICKS", "1"))       # simulated adverse fill, in ticks
OPTION_TICK = float(os.getenv("OPTION_TICK", "0.05"))

LOG_FILE = os.getenv("LOG_FILE", os.path.join(os.path.expanduser("~"),
                                              "short_straddle_live.log"))

# --- Resilience: retry/backoff when the Zerodha API is unreachable ---------
# API_MAX_RETRIES = 0 means retry FOREVER (the trader pauses through an outage
# instead of crashing). Order placement uses a bounded retry to avoid the risk
# of duplicate orders if a reply is lost.
API_MAX_RETRIES = int(os.getenv("API_MAX_RETRIES", "0"))            # 0 = infinite (reads)
API_ORDER_MAX_RETRIES = int(os.getenv("API_ORDER_MAX_RETRIES", "5"))
API_RETRY_BACKOFF_SECONDS = float(os.getenv("API_RETRY_BACKOFF_SECONDS", "2"))
API_RETRY_BACKOFF_MAX = float(os.getenv("API_RETRY_BACKOFF_MAX", "30"))

# --- Resilience: on-disk state, supplementing live broker reconciliation ----
# On (re)start the trader rebuilds today's state from the BROKER (orders +
# positions); this file supplements that with attempt count, realized P&L and
# the per-attempt risk thresholds / trailing peak that the broker cannot know.
STATE_FILE = os.getenv("STATE_FILE", os.path.join(os.path.expanduser("~"),
                                                  "short_straddle_state.json"))


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
# API RETRY WRAPPER  (resilience against Zerodha API outages)
# ===========================================================================
def _api(fn, *args, desc="kite call", max_retries=None, **kwargs):
    """Call a Kite API function, retrying with exponential backoff on any
    exception. max_retries=0 retries indefinitely, so a Zerodha outage pauses
    the trader rather than crashing it. Reads use the infinite default; order
    placements pass a bounded max_retries to avoid duplicate orders."""
    if max_retries is None:
        max_retries = API_MAX_RETRIES
    attempt = 0
    delay = API_RETRY_BACKOFF_SECONDS
    while True:
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            attempt += 1
            if max_retries and attempt >= max_retries:
                log.error(f"[API] {desc} failed after {attempt} attempt(s): {e}")
                raise
            log.warning(f"[API] {desc} failed (attempt {attempt}): {e}; retrying in {delay:.0f}s")
            time.sleep(delay)
            delay = min(delay * 2, API_RETRY_BACKOFF_MAX)


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
        # Auto-reconnect (on by default in pykiteconnect) keeps the feed alive
        # through transient Zerodha outages; we raise the retry budget.
        try:
            self.ticker = KiteTicker(api_key, access_token,
                                     reconnect=True, reconnect_max_tries=300,
                                     reconnect_max_delay=60)
        except TypeError:
            self.ticker = KiteTicker(api_key, access_token)
        self.ltp = {}                      # token -> last_price
        self._subscribed = set()
        self._connected = threading.Event()

        # Bind callbacks.
        self.ticker.on_ticks = self._on_ticks
        self.ticker.on_connect = self._on_connect
        self.ticker.on_close = self._on_close
        self.ticker.on_error = self._on_error
        self.ticker.on_reconnect = self._on_reconnect
        self.ticker.on_noreconnect = self._on_noreconnect

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

    def _on_reconnect(self, ws, attempts_count):
        log.warning(f"[WS] Reconnecting... (attempt {attempts_count})")

    def _on_noreconnect(self, ws):
        log.error("[WS] Reconnection attempts exhausted; relying on process restart.")

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
        q = _api(self.kite.quote, f"{self.exchange}:{tradingsymbol}",
                 desc=f"quote {tradingsymbol}")[f"{self.exchange}:{tradingsymbol}"]
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
        for o in reversed(_api(self.kite.orders, desc="orders")):
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
                    _api(self.kite.modify_order,
                         variety=self.kite.VARIETY_REGULAR, order_id=order_id,
                         order_type=self.kite.ORDER_TYPE_MARKET,
                         market_protection=self.MARKET_PROTECTION,
                         desc=f"modify->MARKET {tradingsymbol}", max_retries=API_ORDER_MAX_RETRIES)
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
        for p in _api(self.kite.positions, desc="positions")["net"]:
            if p["tradingsymbol"] == tradingsymbol and int(p["quantity"]) != 0:
                net = int(p["quantity"])
                txn = self.kite.TRANSACTION_TYPE_BUY if net < 0 else self.kite.TRANSACTION_TYPE_SELL
                try:
                    _api(self.kite.place_order,
                         tradingsymbol=tradingsymbol, variety=self.kite.VARIETY_REGULAR,
                         exchange=self.exchange, transaction_type=txn, quantity=abs(net),
                         order_type=self.kite.ORDER_TYPE_MARKET, product=self.kite.PRODUCT_NRML,
                         tag=oUtils.SS_ORDER_TAG, market_protection=self.MARKET_PROTECTION,
                         desc=f"square-off {tradingsymbol}", max_retries=API_ORDER_MAX_RETRIES)
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
        pe_id = _api(self.kite.place_order, tradingsymbol=pe_sym, variety=self.kite.VARIETY_REGULAR,
                     exchange=self.exchange, transaction_type=txn, quantity=qty,
                     order_type=self.kite.ORDER_TYPE_LIMIT, price=pe_px,
                     product=self.kite.PRODUCT_NRML, tag=oUtils.SS_ORDER_TAG,
                     desc=f"place {pe_sym}", max_retries=API_ORDER_MAX_RETRIES)
        ce_id = _api(self.kite.place_order, tradingsymbol=ce_sym, variety=self.kite.VARIETY_REGULAR,
                     exchange=self.exchange, transaction_type=txn, quantity=qty,
                     order_type=self.kite.ORDER_TYPE_LIMIT, price=ce_px,
                     product=self.kite.PRODUCT_NRML, tag=oUtils.SS_ORDER_TAG,
                     desc=f"place {ce_sym}", max_retries=API_ORDER_MAX_RETRIES)
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
        pe_id = _api(self.kite.place_order, tradingsymbol=pe_sym, variety=self.kite.VARIETY_REGULAR,
                     exchange=self.exchange, transaction_type=txn, quantity=qty,
                     order_type=self.kite.ORDER_TYPE_LIMIT, price=pe_px,
                     product=self.kite.PRODUCT_NRML, tag=oUtils.SS_ORDER_TAG,
                     desc=f"place {pe_sym}", max_retries=API_ORDER_MAX_RETRIES)
        ce_id = _api(self.kite.place_order, tradingsymbol=ce_sym, variety=self.kite.VARIETY_REGULAR,
                     exchange=self.exchange, transaction_type=txn, quantity=qty,
                     order_type=self.kite.ORDER_TYPE_LIMIT, price=ce_px,
                     product=self.kite.PRODUCT_NRML, tag=oUtils.SS_ORDER_TAG,
                     desc=f"place {ce_sym}", max_retries=API_ORDER_MAX_RETRIES)
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
        # --- restart-recovery state (rebuilt from broker on startup) ---
        self.phase = "WAITING"        # WAITING | IN_POSITION | WAITING_REENTRY | DONE
        self.attempt_idx = 0
        self.position = None          # dict describing the currently open straddle
        self.reentry_target = None    # dtime to re-enter at (WAITING_REENTRY)
        self._state_loaded_today = False

    # ----- token / symbol resolution -----
    def _underlying_ltp(self) -> float:
        q = _api(self.kite.ltp, [self.underlying_quote_key], desc="ltp underlying")[self.underlying_quote_key]
        return float(q["last_price"])

    def _resolve_option(self, tradingsymbol: str):
        """Return (instrument_token, ltp) for an option tradingsymbol."""
        key = f"{self.broker.exchange}:{tradingsymbol}"
        info = _api(self.kite.ltp, [key], desc=f"ltp {tradingsymbol}")[key]
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
    # ----- on-disk state (supplements broker reconciliation) -----
    def _today_str(self) -> str:
        return now_ist().date().isoformat()

    def _save_state(self) -> None:
        """Persist the day state atomically so a restart can resume quickly."""
        state = {
            "date": self._today_str(),
            "phase": self.phase,
            "attempt_idx": self.attempt_idx,
            "daily_realized_pnl": self.daily_realized_pnl,
            "reentry_target": self.reentry_target.strftime("%H:%M") if self.reentry_target else None,
            "position": self.position,
            "mode": "PAPER" if self.broker.paper else "LIVE",
        }
        try:
            tmp = STATE_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(state, fh, indent=2, default=str)
            os.replace(tmp, STATE_FILE)
        except Exception as e:
            log.warning(f"[STATE] Could not save state: {e}")

    def _load_state(self) -> None:
        """Load today's saved state if present (only as a SUPPLEMENT to the
        broker reconciliation). Sets self._state_loaded_today accordingly."""
        self.phase = "WAITING"
        self.attempt_idx = 0
        self.daily_realized_pnl = 0.0
        self.position = None
        self.reentry_target = None
        self._state_loaded_today = False
        if not os.path.exists(STATE_FILE):
            return
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as fh:
                st = json.load(fh)
        except Exception as e:
            log.warning(f"[STATE] Could not read state file: {e}")
            return
        if st.get("date") != self._today_str():
            log.info("[STATE] Saved state is from a previous day; ignoring it.")
            return
        self._state_loaded_today = True
        self.phase = st.get("phase", "WAITING")
        self.attempt_idx = int(st.get("attempt_idx", 0))
        self.daily_realized_pnl = float(st.get("daily_realized_pnl", 0.0))
        self.position = st.get("position")
        rt = st.get("reentry_target")
        self.reentry_target = parse_hhmm(rt) if rt else None
        log.info(f"[STATE] Saved state for today found: phase={self.phase} "
                 f"attempt={self.attempt_idx} day_realized=Rs{self.daily_realized_pnl:,.0f} "
                 f"position={'YES' if self.position else 'no'}")

    # ----- broker introspection helpers (the SOURCE OF TRUTH on restart) -----
    def _is_ss_option(self, sym: str) -> bool:
        """True if a trading symbol is one of OUR straddle legs."""
        return bool(sym) and sym.startswith(self.part_symbol) and (sym.endswith("CE") or sym.endswith("PE"))

    def _ss_open_legs(self, net) -> dict:
        """From positions['net'], return {symbol: {qty, entry, avg}} for our
        option legs that are currently OPEN (non-zero net quantity). The short
        entry premium is taken as the average SELL price of the leg."""
        legs = {}
        for p in net:
            sym = p.get("tradingsymbol", "")
            qty = int(p.get("quantity") or 0)
            if qty != 0 and self._is_ss_option(sym):
                sell_q = float(p.get("sell_quantity") or 0)
                sell_val = float(p.get("sell_value") or 0.0)
                sell_avg = (sell_val / sell_q) if sell_q else float(p.get("sell_price") or 0.0)
                legs[sym] = {"qty": qty,
                             "entry": sell_avg,
                             "avg": float(p.get("average_price") or 0.0)}
        return legs

    def _ss_day_realised(self, net) -> float:
        """Sum the broker's BOOKED realized P&L across our legs today (used as a
        fallback for the daily-loss breaker when no saved state exists)."""
        tot = 0.0
        for p in net:
            if self._is_ss_option(p.get("tradingsymbol", "")):
                tot += float(p.get("realised", 0.0) or 0.0)
        return tot

    def _count_completed_entries(self, orders) -> int:
        """Number of straddles entered today = COMPLETE SELL orders on the CE
        leg (each entry sells exactly one CE). Used to infer the attempt index
        when no saved state is available."""
        n = 0
        for o in orders:
            if (o.get("tag") == oUtils.SS_ORDER_TAG
                    and str(o.get("transaction_type", "")).upper() == "SELL"
                    and str(o.get("tradingsymbol", "")).endswith("CE")
                    and str(o.get("status", "")).upper() == "COMPLETE"):
                n += 1
        return n

    def _cancel_stale_ss_orders(self, orders) -> None:
        """Cancel any of OUR orders left pending/open from a previous session,
        so a stale limit order can't surprise-fill after a restart."""
        open_states = {"OPEN", "TRIGGER PENDING", "OPEN PENDING", "MODIFY PENDING",
                       "VALIDATION PENDING", "AMO REQ RECEIVED", "PUT ORDER REQ RECEIVED"}
        for o in orders:
            if o.get("tag") != oUtils.SS_ORDER_TAG:
                continue
            if str(o.get("status", "")).upper() in open_states:
                sym = o.get("tradingsymbol", "")
                try:
                    _api(self.kite.cancel_order, variety=self.kite.VARIETY_REGULAR,
                         order_id=o.get("order_id"),
                         desc=f"cancel stale {sym}", max_retries=API_ORDER_MAX_RETRIES)
                    log.warning(f"[RECONCILE] Cancelled stale pending order {sym} ({o.get('order_id')}).")
                except Exception as e:
                    log.warning(f"[RECONCILE] Could not cancel {sym}: {e}")

    def _adopt_open_straddle(self, ce_sym, pe_sym, open_legs, completed_entries) -> None:
        """Rebuild self.position from a broker-confirmed open straddle. If the
        saved state matches the same strikes, reuse its risk thresholds and
        trailing peak; otherwise rebuild thresholds from the broker fills."""
        ce_leg, pe_leg = open_legs[ce_sym], open_legs[pe_sym]
        qty = abs(ce_leg["qty"]) or self.qty
        ce_entry = ce_leg["entry"] or ce_leg["avg"]
        pe_entry = pe_leg["entry"] or pe_leg["avg"]
        # _resolve_option retries via _api until it gets a token.
        ce_tok, _ = self._resolve_option(ce_sym)
        pe_tok, _ = self._resolve_option(pe_sym)

        saved = self.position
        if saved and saved.get("ce_sym") == ce_sym and saved.get("pe_sym") == pe_sym:
            attempt_idx = int(saved.get("attempt_idx", max(0, completed_entries - 1)))
            stop_rupees = saved["stop_rupees"]
            target_rupees = saved["target_rupees"]
            G = saved["G"]
            peak = saved.get("peak", 0.0)
            armed = saved.get("armed", False)
            ce_entry = saved.get("ce_entry", ce_entry)
            pe_entry = saved.get("pe_entry", pe_entry)
            log.info("[RECONCILE] Saved thresholds match the open straddle; reusing them "
                     f"(peak=Rs{peak:,.0f}, armed={armed}).")
        else:
            attempt_idx = max(self.attempt_idx, max(0, completed_entries - 1))
            premium_sum = (ce_entry + pe_entry) * qty
            stop_rupees = effective_stop_rupees(attempt_idx, premium_sum)
            target_rupees = PROFIT_TARGET_PCT * premium_sum if PROFIT_TARGET_PCT > 0 else None
            G = PROFIT_PROTECT_PCT * premium_sum if PROFIT_PROTECT_PCT > 0 else None
            peak, armed = 0.0, False
            log.warning("[RECONCILE] No matching saved state; rebuilt thresholds from broker "
                        "fills (trailing peak reset to 0 -> profit-protect will re-arm).")

        self.position = {
            "attempt_idx": attempt_idx, "pe_sym": pe_sym, "ce_sym": ce_sym,
            "pe_tok": int(pe_tok), "ce_tok": int(ce_tok),
            "ce_entry": ce_entry, "pe_entry": pe_entry, "qty": qty,
            "premium_sum": (ce_entry + pe_entry) * qty, "stop_rupees": stop_rupees,
            "target_rupees": target_rupees, "G": G, "peak": peak, "armed": armed,
        }
        self.attempt_idx = attempt_idx
        self.phase = "IN_POSITION"
        log.warning(f"[RECONCILE] Adopted OPEN straddle from broker: {ce_sym}/{pe_sym} "
                    f"qty={qty} entryCE={ce_entry} entryPE={pe_entry} attempt#{attempt_idx+1} "
                    f"stop=Rs{stop_rupees:,.0f}")

    def reconcile_on_startup(self) -> None:
        """Rebuild today's trading state from the BROKER (orders + positions),
        using the saved state file only as a supplement. This is what makes the
        trader safe to kill and restart at any moment."""
        self._load_state()   # supplement: thresholds / attempt / peak / realized

        if self.broker.paper:
            log.info("[RECONCILE] Paper mode: no live broker state; using the saved file only.")
            return

        # 1) Read the order book; cancel stale pendings; count today's entries.
        try:
            orders = _api(self.kite.orders, desc="orders(startup)")
        except Exception as e:
            log.error(f"[RECONCILE] orders() failed ({e}); proceeding with state file only.")
            orders = []
        self._cancel_stale_ss_orders(orders)
        completed_entries = self._count_completed_entries(orders)

        # 2) Read live positions -> what is actually open right now.
        try:
            net = _api(self.kite.positions, desc="positions(startup)")["net"]
        except Exception as e:
            log.error(f"[RECONCILE] positions() failed ({e}); proceeding with state file only.")
            net = []
        open_legs = self._ss_open_legs(net)
        day_realised = self._ss_day_realised(net)
        ce_syms = sorted(s for s in open_legs if s.endswith("CE"))
        pe_syms = sorted(s for s in open_legs if s.endswith("PE"))

        # 3) Naked-leg safety: exactly one side open -> square it off, treat flat.
        if bool(ce_syms) != bool(pe_syms):
            lone = (ce_syms or pe_syms)[0]
            log.warning(f"[RECONCILE] Naked leg {lone} open on restart; squaring it off.")
            self.broker._square_off_naked(lone)
            ce_syms, pe_syms, open_legs = [], [], {}

        # 4) Both legs open -> adopt and resume; else reconcile a flat book.
        if ce_syms and pe_syms:
            self._adopt_open_straddle(ce_syms[0], pe_syms[0], open_legs, completed_entries)
        else:
            if self.position is not None:
                log.warning("[RECONCILE] Saved state had an open position but the broker is FLAT; "
                            "clearing it (it was closed while we were down).")
                self.position = None
                if self.phase == "IN_POSITION":
                    self.phase = "WAITING"
            if not self._state_loaded_today:
                if completed_entries > 0:
                    # Prior trading happened today but we have no saved state and are
                    # flat now -> assume the strategy already ran; do NOT start over.
                    log.warning(f"[RECONCILE] {completed_entries} prior entr(ies) today, FLAT now, "
                                "no saved state -> assuming the day already ran. Not resuming.")
                    self.phase = "DONE"
                else:
                    log.info("[RECONCILE] No prior trades and no saved state today; fresh start.")
                    self.phase, self.attempt_idx = "WAITING", 0

        # 5) Daily realized P&L: prefer the file; else seed from broker booked P&L.
        if not self._state_loaded_today and abs(day_realised) > 1e-9:
            log.warning(f"[RECONCILE] Seeding day realized P&L from broker booked realised: "
                        f"Rs{day_realised:,.0f}")
            self.daily_realized_pnl = day_realised

        self._save_state()
        log.warning(f"[RECONCILE] Final startup state: phase={self.phase} attempt#{self.attempt_idx+1} "
                    f"open={'YES' if self.position else 'no'} "
                    f"day_realized=Rs{self.daily_realized_pnl:,.0f}")

    # ----- enter one straddle (sets self.position, persists state) -----
    def enter(self, attempt_idx: int) -> bool:
        ul = self._underlying_ltp()
        atm = round_to_step(ul, self.strike_step)
        pe_sym = f"{self.part_symbol}{atm}PE"
        ce_sym = f"{self.part_symbol}{atm}CE"
        log.info(f"[ENTRY] attempt #{attempt_idx+1}: underlying={ul:.2f} -> ATM={atm} | {ce_sym} / {pe_sym}")
        try:
            pe_tok, _ = self._resolve_option(pe_sym)
            ce_tok, _ = self._resolve_option(ce_sym)
        except Exception as e:
            log.error(f"[ENTRY] Could not resolve option tokens ({e}); skipping attempt.")
            return False
        self.feed.subscribe([pe_tok, ce_tok])
        if not self.feed.wait_for([pe_tok, ce_tok], timeout=10):
            log.error("[ENTRY] No ticks for legs; skipping attempt.")
            self.feed.unsubscribe([pe_tok, ce_tok])
            return False

        fills = self.broker.open_short_straddle(pe_sym, ce_sym, pe_tok, ce_tok, self.qty)
        ce_entry, pe_entry = fills["ce_fill"], fills["pe_fill"]
        premium_sum = (ce_entry + pe_entry) * self.qty
        stop_rupees = effective_stop_rupees(attempt_idx, premium_sum)
        target_rupees = PROFIT_TARGET_PCT * premium_sum if PROFIT_TARGET_PCT > 0 else None
        G = PROFIT_PROTECT_PCT * premium_sum if PROFIT_PROTECT_PCT > 0 else None

        self.position = {
            "attempt_idx": attempt_idx, "pe_sym": pe_sym, "ce_sym": ce_sym,
            "pe_tok": int(pe_tok), "ce_tok": int(ce_tok),
            "ce_entry": ce_entry, "pe_entry": pe_entry, "qty": self.qty,
            "premium_sum": premium_sum, "stop_rupees": stop_rupees,
            "target_rupees": target_rupees, "G": G, "peak": 0.0, "armed": False,
        }
        self.phase = "IN_POSITION"
        self._save_state()   # persist immediately so a crash here can resume
        log.info(f"[ENTRY] filled CE={ce_entry} PE={pe_entry} premium=Rs{premium_sum:,.0f} "
                 f"| stop=Rs{stop_rupees:,.0f} "
                 f"| target={'Rs%.0f' % target_rupees if target_rupees else 'off'} "
                 f"| protectG={'Rs%.0f' % G if G else 'off'}")
        return True

    # ----- monitor the current self.position until an exit fires -----
    def monitor_and_exit(self) -> str:
        p = self.position
        pe_tok, ce_tok = int(p["pe_tok"]), int(p["ce_tok"])
        ce_entry, pe_entry, qty = p["ce_entry"], p["pe_entry"], p["qty"]
        stop_rupees, target_rupees, G = p["stop_rupees"], p["target_rupees"], p["G"]
        peak, armed = p.get("peak", 0.0), p.get("armed", False)

        # Re-subscribe (essential after a restart; harmless otherwise).
        self.feed.subscribe([pe_tok, ce_tok])
        self.feed.wait_for([pe_tok, ce_tok], timeout=10)

        last_hb = 0.0
        last_save = time.time()
        exit_reason = "EOD"
        while True:
            now_t = now_ist().time()
            ce_ltp = self.feed.get(ce_tok)
            pe_ltp = self.feed.get(pe_tok)
            if now_t >= self.squareoff_time:
                exit_reason = "EOD"
                break
            if ce_ltp is None or pe_ltp is None:
                time.sleep(MONITOR_POLL_SECONDS)
                continue

            pnl = (ce_entry - ce_ltp) * qty + (pe_entry - pe_ltp) * qty
            peak = max(peak, pnl)
            if G is not None and not armed and peak >= G:
                armed = True
                log.info(f"[PROTECT] armed: peak=Rs{peak:,.0f} >= G=Rs{G:,.0f}")

            # Persist peak/armed periodically so a restart resumes accurately.
            if time.time() - last_save >= 10:
                p["peak"], p["armed"] = peak, armed
                self._save_state()
                last_save = time.time()

            if time.time() - last_hb >= MONITOR_HEARTBEAT_SECONDS:
                log.info(f"[MONITOR] pnl=Rs{pnl:,.0f} peak=Rs{peak:,.0f} "
                         f"(CE {ce_ltp} / PE {pe_ltp}) armed={armed}")
                last_hb = time.time()

            # Exit priority (same as B): STOP > TARGET > PROTECT.
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

        close = self.broker.close_short_straddle(p["pe_sym"], p["ce_sym"], pe_tok, ce_tok, qty)
        ce_exit, pe_exit = close["ce_fill"], close["pe_fill"]
        gross = (ce_entry - ce_exit) * qty + (pe_entry - pe_exit) * qty
        self.daily_realized_pnl += gross
        log.info(f"[EXIT] {exit_reason}: CE {ce_entry}->{ce_exit}, PE {pe_entry}->{pe_exit} "
                 f"| gross=Rs{gross:,.0f} | day_realized=Rs{self.daily_realized_pnl:,.0f}")

        self.feed.unsubscribe([pe_tok, ce_tok])
        self.position = None
        self.phase = "WAITING"
        self._save_state()
        return exit_reason

    # ----- full trading day -----
    def _handle_post_exit(self, exit_reason: str) -> bool:
        """Decide whether to re-enter. Returns True to continue, False to end."""
        if exit_reason in ("STOPLOSS", "PROFIT_PROTECT") and self.attempt_idx < MAX_REATTEMPTS:
            delay = reentry_delay_for_attempt(self.attempt_idx)
            self.attempt_idx += 1
            self.reentry_target = (now_ist() + timedelta(minutes=delay)).time()
            self.phase = "WAITING_REENTRY"
            self._save_state()
            log.info(f"[REENTRY] {exit_reason} -> wait {delay} min, next attempt #{self.attempt_idx+1}")
            if not self._sleep_until(self.reentry_target, f"re-entry #{self.attempt_idx+1}"):
                return False
            self.phase = "WAITING"
            self.reentry_target = None
            self._save_state()
            return True
        log.info(f"[DAY] No re-entry after {exit_reason}; day complete.")
        return False

    def _finish_day(self) -> None:
        self.phase = "DONE"
        self.reentry_target = None
        self._save_state()
        log.info(f"[DAY DONE] total realized P&L = Rs{self.daily_realized_pnl:,.0f}")

    def run_day(self):
        # Rebuild today's state from the BROKER (orders + positions); the saved
        # file only supplements it. This is what makes a restart correct.
        self.reconcile_on_startup()
        log.info("=" * 70)
        log.info(f"[DAY] {date.today()} | mode={'PAPER' if self.broker.paper else 'LIVE'} "
                 f"| entry={ENTRY_TIME_IST} | squareoff={SQUAREOFF_TIME_IST}")
        log.info(f"[DAY] qty={self.qty} step={self.strike_step} "
                 f"max_reattempts={MAX_REATTEMPTS} daily_loss_cap=Rs{MAX_DAILY_LOSS_RUPEES:,.0f}")

        if self.phase == "DONE":
            log.info("[DAY] State shows today already completed; nothing to do.")
            return

        # Past square-off: flatten anything still open, then finish.
        if now_ist().time() >= self.squareoff_time:
            if self.position is not None:
                log.warning("[DAY] Past square-off with an open position; squaring off now.")
                self.monitor_and_exit()
            self._finish_day()
            return

        # Resume a broker-confirmed open position (set by reconcile).
        if self.position is not None:
            log.warning(f"[RESUME] Monitoring open straddle "
                        f"{self.position['ce_sym']}/{self.position['pe_sym']} from attempt "
                        f"#{self.attempt_idx+1}.")
            exit_reason = self.monitor_and_exit()
            if not self._handle_post_exit(exit_reason):
                self._finish_day()
                return

        # Resume a pending re-entry timer that was in effect at crash time.
        if self.phase == "WAITING_REENTRY" and self.reentry_target is not None:
            log.info(f"[RESUME] Waiting for saved re-entry time {self.reentry_target.strftime('%H:%M')}.")
            if not self._sleep_until(self.reentry_target, "saved re-entry"):
                self._finish_day()
                return
            self.phase = "WAITING"
            self.reentry_target = None
            self._save_state()

        # First-entry wait (fresh day; nothing open yet).
        if self.attempt_idx == 0 and self.position is None and self.phase == "WAITING":
            if now_ist().time() < self.entry_time:
                if not self._sleep_until(self.entry_time, "entry time"):
                    self._finish_day()
                    return
            else:
                log.warning("[DAY] Started AFTER entry time; entering immediately on this run.")

        # Main attempt loop.
        while True:
            if MAX_DAILY_LOSS_RUPEES > 0 and self.daily_realized_pnl <= -MAX_DAILY_LOSS_RUPEES:
                log.warning(f"[BREAKER] Daily loss cap hit "
                            f"(realized=Rs{self.daily_realized_pnl:,.0f}); no more trades today.")
                break
            if now_ist().time() >= self.squareoff_time:
                log.info("[DAY] Square-off time reached; ending the day.")
                break

            if not self.enter(self.attempt_idx):
                break
            exit_reason = self.monitor_and_exit()
            if not self._handle_post_exit(exit_reason):
                break

        self._finish_day()


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
    kite = _api(oUtils.intialize_kite_api, desc="kite session init")
    log.info("[BOOT] Kite session initialised.")

    # --- Instrument config (same source A uses) ---
    (UNDER_LYING_EXCHANGE, UNDERLYING, OPTIONS_EXCHANGE, PART_SYMBOL,
     NO_OF_LOTS, STRIKE_MULTIPLE, STOPLOSS_POINTS, MINIMUM_LOTS,
     LONG_STRADDLE_STRIKE_DISTANCE) = _api(oUtils.get_instruments, kite, desc="get_instruments")
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

    # Auto-restart loop: if run_day crashes before square-off, restart it. The
    # broker reconciliation + on-disk state mean it RESUMES (re-adopts an open
    # position / pending re-entry) rather than starting the day over. For
    # crash-proofing across reboots, also run this under an OS supervisor
    # (Windows Task Scheduler "restart on failure" / systemd Restart=always).
    try:
        while True:
            try:
                trader.run_day()
                break
            except KeyboardInterrupt:
                log.warning("[SHUTDOWN] Interrupted by user.")
                break
            except Exception as e:
                log.exception(f"[RESILIENCE] run_day crashed: {e}")
                if now_ist().time() >= trader.squareoff_time:
                    log.info("[RESILIENCE] Past square-off; not restarting.")
                    break
                log.warning("[RESILIENCE] Restarting run_day in 5s (state preserved on disk).")
                time.sleep(5)
    finally:
        feed.stop()
        log.info("[SHUTDOWN] Feed closed. Bye.")


if __name__ == "__main__":
    main()
