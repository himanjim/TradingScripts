# epoch_quote_scanner_1s.py
# Polls kite.quote() every second and places an intraday ENTRY when epoch sequence completes.
# Rules:
# 1) Epochs are PRICE levels and must be crossed in order.
# 2) Each epoch trigger must occur in a DIFFERENT 1-min candle (IST).
# 3) Before a new entry: square-off open MIS positions of same TAG (best-effort) + cancel open TAG orders.
# 4) Qty is sized to risk ₹3000: qty = floor(3000 / abs(entry - SL_PRICE)).
# 5) Places ENTRY MARKET + SL-M (trigger at SL_PRICE). Paper mode supported.

import time, math, datetime as dt, pytz
import Trading_2024.OptionTradeUtils as oUtils
import math
# ---------- CONFIG ----------
SYMBOL = "NSE:HDFCBANK"
EPOCHS = [920.77]

POLL_SEC = 1
ABS_RISK = 3000.0
SL_PRICE = 923.84
TICK_SIZE = 0.10  # NSE cash equities are usually 0.05 or 0.10 depending on script

PRODUCT = "MIS"
PAPER = False
TAG = oUtils.STOCK_INTRADAY_TAG
ONLY_TODAY_TAGGED = True  # prevents old tagged symbols from being used for square-off

INDIA_TZ = pytz.timezone("Asia/Kolkata")


# --- beep helper (Windows: winsound; others: terminal bell) ---
def beep():
    try:
        import winsound
        winsound.Beep(1200, 300)  # frequency Hz, duration ms
    except Exception:
        print("\a", end="", flush=True)

def round_to_tick_dir(price: float, tick: float, direction: str) -> float:
    """
    direction: 'DOWN' or 'UP'
    DOWN => floor to tick, UP => ceil to tick
    """
    x = price / tick
    v = math.floor(x) if direction == "DOWN" else math.ceil(x)
    return round(v * tick, 2)



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
        # else: crossing in same candle => ignore
    return idx, armed_lvl, armed_dir, last_candle, hit_dir, False


# ---------- ORDER HELPERS ----------
def qty_from_risk(entry, sl):
    r = abs(entry - sl)
    if r <= 0: raise ValueError(f"Bad SL: entry={entry}, sl={sl}")
    return max(1, int(math.floor(ABS_RISK / r)))

def _is_today_ist(order_ts: str) -> bool:
    """
    order_ts from Kite is typically like '2026-01-16 09:16:01'.
    Treat it as IST local time string (Zerodha orders are in local time).
    """
    try:
        d = order_ts.split(" ")[0]
        today = dt.datetime.now(INDIA_TZ).strftime("%Y-%m-%d")
        return d == today
    except Exception:
        return True  # fail open (better than skipping incorrectly)

def tagged_symbols_and_open_orders(kite):
    """
    Returns (symbols_set, open_order_ids) for this TAG.
    If ONLY_TODAY_TAGGED=True, limits to today's tagged orders.
    """
    syms, open_oids = set(), []
    for o in kite.orders():
        if o.get("tag") != TAG:
            continue
        if ONLY_TODAY_TAGGED and not _is_today_ist(str(o.get("order_timestamp", ""))):
            continue
        syms.add(f'{o["exchange"]}:{o["tradingsymbol"]}')
        if o.get("status") in ("OPEN", "TRIGGER PENDING", "VALIDATION PENDING", "PUT ORDER REQ RECEIVED"):
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

    for p in kite.positions().get("net", []):
        sym = f'{p["exchange"]}:{p["tradingsymbol"]}'
        if sym not in syms or p.get("product") != PRODUCT:
            continue
        netq = int(p.get("quantity") or 0)  # +long / -short
        if netq == 0:
            continue
        side = kite.TRANSACTION_TYPE_SELL if netq > 0 else kite.TRANSACTION_TYPE_BUY
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

def place_entry_and_slm(kite, side, qty):
    exch, tsym = SYMBOL.split(":")
    exit_side = kite.TRANSACTION_TYPE_SELL if side == kite.TRANSACTION_TYPE_BUY else kite.TRANSACTION_TYPE_BUY

    # exit_side is SELL for long SL, BUY for short SL
    dirn = "DOWN" if exit_side == kite.TRANSACTION_TYPE_SELL else "UP"
    trig = round_to_tick_dir(float(SL_PRICE), TICK_SIZE, dirn)

    if PAPER:
        print(f"[PAPER] ENTRY {side} {SYMBOL} qty={qty}")
        print(f"[PAPER] SL-M  {exit_side} {SYMBOL} qty={qty} trigger={SL_PRICE}")
        return None, None

    entry_oid = kite.place_order(
        variety=kite.VARIETY_REGULAR, exchange=exch, tradingsymbol=tsym,
        transaction_type=side, quantity=qty, product=PRODUCT,
        order_type=kite.ORDER_TYPE_MARKET, validity=kite.VALIDITY_DAY, tag=TAG
    )
    sl_oid = kite.place_order(
        variety=kite.VARIETY_REGULAR, exchange=exch, tradingsymbol=tsym,
        transaction_type=exit_side, quantity=qty, product=PRODUCT,
        order_type=kite.ORDER_TYPE_SLM, trigger_price=trig,
        validity=kite.VALIDITY_DAY, tag=TAG
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
        try:
            q = kite.quote([SYMBOL])
            ltp = float(q[SYMBOL]["last_price"])
        except Exception as e:
            print(f"[WARN] quote() failed for {SYMBOL}: {e}. Retrying in 2s...")
            time.sleep(2)
            continue

        now_utc = dt.datetime.now(dt.timezone.utc)
        cur_candle = _candle_id_ist(now_utc)

        if prev is None:
            prev = ltp
            armed_lvl = float(EPOCHS[idx])
            armed_dir = _arm_dir(ltp, armed_lvl)
            print(f"[ARM] idx={idx} lvl={armed_lvl} dir={armed_dir} ltp={ltp} candle={cur_candle}")
            time.sleep(POLL_SEC); continue

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

            entry_oid, sl_oid = place_entry_and_slm(kite, side, qty)
            beep()  # <-- beep on order placement
            print(f"entry_order_id={entry_oid} sl_order_id={sl_oid}")
            return

        prev = ltp
        time.sleep(POLL_SEC)


if __name__ == "__main__":
    main()
