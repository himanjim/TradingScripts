# epoch_trigger_intraday_equity.py
# Polls kite.quote() every second and places an intraday ENTRY when epoch sequence completes.
# Rules:
# 1) Epochs are PRICE levels and must be crossed in order.
# 2) Each epoch trigger must occur in a DIFFERENT 1-min candle (IST).
# 3) Before a new entry: square-off open MIS positions of same TAG (best-effort) + cancel open TAG orders.
# 4) Qty is sized to risk ₹3000: qty = floor(3000 / abs(entry - SL_PRICE)).
# 5) Places ENTRY as SMART LIMIT (best bid/ask depth) and waits ENTRY_WAIT_SEC.
#    If not filled in time -> cancel pending tag orders and exit.
# 6) After entry fill, places SL-M (trigger rounded to tick in conservative direction).
# 7) If quote/orders/positions fetching fails, retries every 2 seconds.

import time
import math
import datetime as dt
import pytz
import Trading_2024.OptionTradeUtils as oUtils

# ---------- CONFIG ----------
SYMBOL = "NSE:ICICIBANK"
EPOCHS = [1348.68]

POLL_SEC = 1
RETRY_SEC = 2  # retry delay for quote/orders/positions

ABS_RISK = 3000.0
SL_PRICE = 1354.69

PRODUCT = "MIS"
PAPER = False
TAG = oUtils.STOCK_INTRADAY_TAG
ONLY_TODAY_TAGGED = True  # prevents old tagged symbols from being used for square-off

# Tick size + entry behavior
TICK_SIZE = 0.10
ENTRY_WAIT_SEC = 10

SMART_LIMIT_MODE = "CROSS"   # "CROSS" or "JOIN"
# CROSS: buy at best_ask, sell at best_bid (higher fill probability)
# JOIN : buy at best_bid, sell at best_ask (better price, lower fill probability)
SLIPPAGE_TICKS = 0           # extra ticks to cross more aggressively (e.g., 1)

INDIA_TZ = pytz.timezone("Asia/Kolkata")


# --- beep helper (Windows: winsound; others: terminal bell) ---
def beep():
    try:
        import winsound
        winsound.Beep(1200, 300)
    except Exception:
        print("\a", end="", flush=True)


# ---------- Tick helpers ----------
def round_to_tick(price: float, tick: float = TICK_SIZE) -> float:
    return round(round(price / tick) * tick, 2)


def round_to_tick_dir(price: float, tick: float, direction: str) -> float:
    """
    direction: 'DOWN' or 'UP'
    DOWN => floor to tick, UP => ceil to tick
    """
    x = price / tick
    v = math.floor(x) if direction == "DOWN" else math.ceil(x)
    return round(v * tick, 2)


# ---------- Retry wrappers ----------
def safe_quote(kite, symbol: str) -> dict:
    while True:
        try:
            return kite.quote([symbol])
        except Exception as e:
            print(f"[WARN] quote() failed for {symbol}: {e}. Retrying in {RETRY_SEC}s...")
            time.sleep(RETRY_SEC)


def safe_orders(kite) -> list:
    while True:
        try:
            return kite.orders()
        except Exception as e:
            print(f"[WARN] orders() failed: {e}. Retrying in {RETRY_SEC}s...")
            time.sleep(RETRY_SEC)


def safe_positions(kite) -> dict:
    while True:
        try:
            return kite.positions()
        except Exception as e:
            print(f"[WARN] positions() failed: {e}. Retrying in {RETRY_SEC}s...")
            time.sleep(RETRY_SEC)


# ---------- Smart LIMIT pricing ----------
def safe_best_bid_ask(q_item: dict):
    """
    Returns (best_bid, best_ask) if depth exists; else (None, None).
    """
    try:
        d = q_item.get("depth") or {}
        bids = d.get("buy") or []
        asks = d.get("sell") or []
        best_bid = float(bids[0]["price"]) if bids else None
        best_ask = float(asks[0]["price"]) if asks else None
        return best_bid, best_ask
    except Exception:
        return None, None


def smart_limit_price_from_quote(q_item: dict, side: str, kite) -> float:
    """
    Choose a smart LIMIT price using best bid/ask depth. Falls back to LTP.
    Applies SLIPPAGE_TICKS in the fill-favoring direction.
    """
    ltp = float(q_item["last_price"])
    best_bid, best_ask = safe_best_bid_ask(q_item)

    # If depth missing, fallback to LTP
    if best_bid is None and best_ask is None:
        base = ltp
    else:
        mode = SMART_LIMIT_MODE.upper().strip()
        if mode == "JOIN":
            # better price, lower fill probability
            base = (best_bid if best_bid is not None else ltp) if side == kite.TRANSACTION_TYPE_BUY else (best_ask if best_ask is not None else ltp)
        else:
            # CROSS (default): higher fill probability
            base = (best_ask if best_ask is not None else ltp) if side == kite.TRANSACTION_TYPE_BUY else (best_bid if best_bid is not None else ltp)

    slip = float(SLIPPAGE_TICKS) * float(TICK_SIZE)
    px = base + slip if side == kite.TRANSACTION_TYPE_BUY else base - slip
    return round_to_tick(px, TICK_SIZE)


# ---------- EPOCH ENGINE (compact) ----------
def _candle_id_ist(now_utc): return now_utc.astimezone(INDIA_TZ).strftime("%Y-%m-%d %H:%M")
def _arm_dir(cur, lvl): return "UP" if cur < lvl else "DOWN"
def _crossed(prev, cur, lvl, d): return (prev < lvl <= cur) if d == "UP" else (prev > lvl >= cur)


def run_epochs(prev_val, cur_val, idx, armed_lvl, armed_dir, last_candle, cur_candle, levels):
    """
    Returns updated (idx, armed_lvl, armed_dir, last_candle, hit_dir, done)
    hit_dir is direction of the epoch that just triggered (or None).
    """
    hit_dir = None
    if _crossed(prev_val, cur_val, armed_lvl, armed_dir):
        if last_candle is None or cur_candle != last_candle:  # enforce separate 1-min candle
            hit_dir = armed_dir
            last_candle = cur_candle
            idx += 1
            if idx >= len(levels):
                return idx, armed_lvl, armed_dir, last_candle, hit_dir, True
            armed_lvl = float(levels[idx])
            armed_dir = _arm_dir(cur_val, armed_lvl)
    return idx, armed_lvl, armed_dir, last_candle, hit_dir, False


# ---------- ORDER HELPERS ----------
def qty_from_risk(entry, sl):
    r = abs(entry - sl)
    if r <= 0:
        raise ValueError(f"Bad SL: entry={entry}, sl={sl}")
    return max(1, int(math.floor(ABS_RISK / r)))


def _is_today_ist(order_ts: str) -> bool:
    try:
        d = order_ts.split(" ")[0]
        today = dt.datetime.now(INDIA_TZ).strftime("%Y-%m-%d")
        return d == today
    except Exception:
        return True


def tagged_symbols_and_open_orders(kite):
    """
    Returns (symbols_set, open_order_ids) for this TAG.
    """
    syms, open_oids = set(), []
    for o in safe_orders(kite):
        if o.get("tag") != TAG:
            continue
        if ONLY_TODAY_TAGGED and not _is_today_ist(str(o.get("order_timestamp", ""))):
            continue
        syms.add(f'{o["exchange"]}:{o["tradingsymbol"]}')
        if str(o.get("status", "")).upper() in ("OPEN", "TRIGGER PENDING", "VALIDATION PENDING", "PUT ORDER REQ RECEIVED", "PARTIAL"):
            open_oids.append(o["order_id"])
    return syms, open_oids


def cancel_open_tag_orders(kite):
    if PAPER:
        print("[PAPER] Would cancel OPEN/TRIGGER PENDING orders for TAG.")
        return
    _, open_oids = tagged_symbols_and_open_orders(kite)
    for oid in open_oids:
        try:
            kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=oid)
            print(f"[LIVE] Cancelled tagged order: {oid}")
        except Exception as e:
            print(f"[WARN] Could not cancel {oid}: {e}")


def squareoff_tag_positions(kite):
    """
    Best-effort square-off: positions don't store tag, so use symbols from TAG orders.
    """
    syms, _ = tagged_symbols_and_open_orders(kite)
    if not syms:
        print(f"[INFO] No tagged symbols found for TAG={TAG}.")
        return

    if PAPER:
        print(f"[PAPER] Would square-off open {PRODUCT} positions for TAG symbols={sorted(syms)}")
        return

    net = safe_positions(kite).get("net", [])
    for p in net:
        sym = f'{p["exchange"]}:{p["tradingsymbol"]}'
        if sym not in syms or p.get("product") != PRODUCT:
            continue
        netq = int(p.get("quantity") or 0)  # +long / -short
        if netq == 0:
            continue
        side = kite.TRANSACTION_TYPE_SELL if netq > 0 else kite.TRANSACTION_TYPE_BUY
        try:
            kite.place_order(
                variety=kite.VARIETY_REGULAR,
                exchange=p["exchange"],
                tradingsymbol=p["tradingsymbol"],
                transaction_type=side,
                quantity=abs(netq),
                product=PRODUCT,
                order_type=kite.ORDER_TYPE_MARKET,
                validity=kite.VALIDITY_DAY,
                tag=TAG
            )
            print(f"[LIVE] Square-off {sym} net_qty={netq} -> {side} {abs(netq)}")
        except Exception as e:
            print(f"[WARN] Square-off failed for {sym}: {e}")


def get_order_status(kite, order_id: str) -> str:
    if PAPER:
        return "COMPLETE"
    for o in safe_orders(kite):
        if o.get("order_id") == order_id:
            return str(o.get("status", "UNKNOWN")).upper()
    return "UNKNOWN"


def place_entry_and_slm(kite, side, qty, entry_quote_item: dict):
    """
    Places ENTRY as SMART LIMIT, waits ENTRY_WAIT_SEC for fill.
    If not filled -> cancels pending tag orders and returns (None, None).
    If filled -> places SL-M and returns (entry_oid, sl_oid).
    """
    exch, tsym = SYMBOL.split(":")
    exit_side = kite.TRANSACTION_TYPE_SELL if side == kite.TRANSACTION_TYPE_BUY else kite.TRANSACTION_TYPE_BUY

    # SL trigger rounding: long SL (SELL) -> round DOWN; short SL (BUY) -> round UP
    dirn = "DOWN" if exit_side == kite.TRANSACTION_TYPE_SELL else "UP"
    trig = round_to_tick_dir(float(SL_PRICE), TICK_SIZE, dirn)

    entry_px = smart_limit_price_from_quote(entry_quote_item, side, kite)

    if PAPER:
        print(f"[PAPER] ENTRY LIMIT {side} {SYMBOL} qty={qty} price={entry_px}")
        print(f"[PAPER] SL-M        {exit_side} {SYMBOL} qty={qty} trigger={trig}")
        return "PAPER_ENTRY", "PAPER_SL"

    # ---- ENTRY LIMIT ----
    entry_oid = kite.place_order(
        variety=kite.VARIETY_REGULAR,
        exchange=exch,
        tradingsymbol=tsym,
        transaction_type=side,
        quantity=qty,
        product=PRODUCT,
        order_type=kite.ORDER_TYPE_LIMIT,
        price=entry_px,
        validity=kite.VALIDITY_DAY,
        tag=TAG
    )
    print(f"[LIVE] Entry LIMIT placed @ {entry_px}, order_id={entry_oid}. Waiting {ENTRY_WAIT_SEC}s...")

    # ---- Wait for fill ----
    deadline = time.time() + ENTRY_WAIT_SEC
    filled = False
    while time.time() < deadline:
        st = get_order_status(kite, entry_oid)
        if st == "COMPLETE":
            filled = True
            break
        if st in ("CANCELLED", "REJECTED"):
            break
        time.sleep(1)

    if not filled:
        st = get_order_status(kite, entry_oid)
        if st in ("OPEN", "TRIGGER PENDING", "PARTIAL", "VALIDATION PENDING", "PUT ORDER REQ RECEIVED"):
            try:
                kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=entry_oid)
                print(f"[LIVE] Entry not filled in time. Cancelled entry order {entry_oid}.")
            except Exception as e:
                print(f"[WARN] Failed to cancel entry order {entry_oid}: {e}")

        cancel_open_tag_orders(kite)
        return None, None

    # ---- After entry fill: place SL-M ----
    sl_oid = kite.place_order(
        variety=kite.VARIETY_REGULAR,
        exchange=exch,
        tradingsymbol=tsym,
        transaction_type=exit_side,
        quantity=qty,
        product=PRODUCT,
        order_type=kite.ORDER_TYPE_SLM,
        trigger_price=trig,
        validity=kite.VALIDITY_DAY,
        tag=TAG
    )
    return entry_oid, sl_oid


# ---------- MAIN ----------
def main():
    kite = oUtils.intialize_kite_api()

    prev = None
    idx = 0
    armed_lvl = None
    armed_dir = None
    last_candle = None
    final_dir = None

    print(f"ENTRY-SCAN {SYMBOL} EPOCHS={EPOCHS} | SL={SL_PRICE} | risk=₹{ABS_RISK} | tag={TAG}")
    while True:
        q = safe_quote(kite, SYMBOL)
        ltp = float(q[SYMBOL]["last_price"])
        now_utc = dt.datetime.now(dt.timezone.utc)
        cur_candle = _candle_id_ist(now_utc)

        if prev is None:
            prev = ltp
            armed_lvl = float(EPOCHS[idx])
            armed_dir = _arm_dir(ltp, armed_lvl)
            print(f"[ARM] idx={idx} lvl={armed_lvl} dir={armed_dir} ltp={ltp} candle={cur_candle}")
            time.sleep(POLL_SEC)
            continue

        idx, armed_lvl, armed_dir, last_candle, hit_dir, done = run_epochs(
            prev, ltp, idx, armed_lvl, armed_dir, last_candle, cur_candle, EPOCHS
        )

        if hit_dir:
            print(f"[HIT] idx={idx-1} lvl={EPOCHS[idx-1]} dir={hit_dir} ltp={ltp} candle={cur_candle}")
            final_dir = hit_dir
            if not done:
                print(f"[ARM] idx={idx} lvl={armed_lvl} dir={armed_dir} ltp={ltp} candle={cur_candle}")

        if done:
            side = kite.TRANSACTION_TYPE_BUY if final_dir == "UP" else kite.TRANSACTION_TYPE_SELL
            qty = qty_from_risk(ltp, SL_PRICE)
            print(f"[DONE] epochs complete => {side} qty={qty} entry≈{ltp}")

            # Clean old stuff for tag: cancel pending + square-off open positions
            cancel_open_tag_orders(kite)
            squareoff_tag_positions(kite)

            entry_oid, sl_oid = place_entry_and_slm(kite, side, qty, entry_quote_item=q[SYMBOL])
            if entry_oid and sl_oid:
                beep()
                print(f"entry_order_id={entry_oid} sl_order_id={sl_oid}")
            else:
                print("[INFO] Entry did not fill within time window. All pending orders cancelled.")
            return

        prev = ltp
        time.sleep(POLL_SEC)


if __name__ == "__main__":
    main()
