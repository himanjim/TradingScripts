# pnl_guard_by_tag.py
# Exit guard for TAG based on STOCK PRICE EPOCHS (not money/PnL):
# - Polls LTP via kite.quote() every second for SYMBOL
# - Applies the same epoch crossing logic as entry scanner
# - Enforces: each epoch trigger must occur in a DIFFERENT 1-min candle (IST)
# - When epochs complete => cancel open TAG orders + square-off open MIS positions for TAG symbols

import time, datetime as dt, pytz
import Trading_2024.OptionTradeUtils as oUtils

# --- beep helper (Windows: winsound; others: terminal bell) ---
def beep():
    try:
        import winsound
        winsound.Beep(1200, 300)
    except Exception:
        print("\a", end="", flush=True)

def has_open_tag_positions(kite) -> bool:
    """
    True if there is any open net MIS position for symbols seen under TAG orders.
    If none exist, guard exits immediately (your requirement).
    """
    syms, _ = tagged_symbols_and_open_orders(kite)
    if not syms:
        return False
    for p in kite.positions().get("net", []):
        if p.get("product") != PRODUCT:
            continue
        sym = f'{p["exchange"]}:{p["tradingsymbol"]}'
        if sym in syms and int(p.get("quantity") or 0) != 0:
            return True
    return False


# ---------- CONFIG ----------
SYMBOL = "NSE:IRFC"
EXIT_EPOCHS = [114.3]

POLL_SEC = 1
PRODUCT = "MIS"
PAPER = False
TAG = oUtils.STOCK_INTRADAY_TAG
ONLY_TODAY_TAGGED = True

INDIA_TZ = pytz.timezone("Asia/Kolkata")


# ---------- EPOCH ENGINE ----------
def _candle_id_ist(now_utc): return now_utc.astimezone(INDIA_TZ).strftime("%Y-%m-%d %H:%M")
def _arm_dir(cur, lvl): return "UP" if cur < lvl else "DOWN"
def _crossed(prev, cur, lvl, d): return (prev < lvl <= cur) if d == "UP" else (prev > lvl >= cur)

def run_epochs(prev_val, cur_val, idx, armed_lvl, armed_dir, last_candle, cur_candle, levels):
    hit_dir = None
    if _crossed(prev_val, cur_val, armed_lvl, armed_dir):
        if last_candle is None or cur_candle != last_candle:
            hit_dir = armed_dir
            last_candle = cur_candle
            idx += 1
            if idx >= len(levels):
                return idx, armed_lvl, armed_dir, last_candle, hit_dir, True
            armed_lvl = float(levels[idx])
            armed_dir = _arm_dir(cur_val, armed_lvl)
    return idx, armed_lvl, armed_dir, last_candle, hit_dir, False


# ---------- TAG CLEANUP / EXIT ----------
def _is_today_ist(order_ts: str) -> bool:
    try:
        d = order_ts.split(" ")[0]
        today = dt.datetime.now(INDIA_TZ).strftime("%Y-%m-%d")
        return d == today
    except Exception:
        return True

def tagged_symbols_and_open_orders(kite):
    syms, open_oids = set(), []
    for o in kite.orders():
        if o.get("tag") != TAG:
            continue
        if ONLY_TODAY_TAGGED and not _is_today_ist(str(o.get("order_timestamp", ""))):
            continue
        syms.add(f'{o["exchange"]}:{o["tradingsymbol"]}')
        if o.get("status") in ("OPEN", "TRIGGER PENDING", "VALIDATION PENDING", "PUT ORDER REQ RECEIVED", "PARTIAL"):
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
        netq = int(p.get("quantity") or 0)
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


# ---------- MAIN ----------
def main():
    kite = oUtils.intialize_kite_api()

    # If there is no open position corresponding to this TAG, nothing to guard -> exit.
    if not has_open_tag_positions(kite):
        print(f"[EXIT] No open {PRODUCT} positions found for TAG={TAG}. Exiting.")
        return

    prev = None
    idx = 0
    armed_lvl = None
    armed_dir = None
    last_candle = None
    final_dir = None

    print(f"EXIT-GUARD {SYMBOL} EXIT_EPOCHS={EXIT_EPOCHS} (separate 1-min candle) tag={TAG}")
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

        # If position already got closed (e.g., SL hit elsewhere), stop guarding and exit.
        if not has_open_tag_positions(kite):
            print(f"[EXIT] No open {PRODUCT} positions found for TAG={TAG}. Exiting.")
            cancel_open_tag_orders(kite)   # cleanup any remaining open orders
            return

        if prev is None:
            prev = ltp
            armed_lvl = float(EXIT_EPOCHS[idx])
            armed_dir = _arm_dir(ltp, armed_lvl)
            print(f"[ARM] idx={idx} lvl={armed_lvl} dir={armed_dir} ltp={ltp} candle={cur_candle}")
            time.sleep(POLL_SEC); continue

        idx, armed_lvl, armed_dir, last_candle, hit_dir, done = run_epochs(
            prev, ltp, idx, armed_lvl, armed_dir, last_candle, cur_candle, EXIT_EPOCHS
        )
        if hit_dir:
            print(f"[HIT] idx={idx-1} lvl={EXIT_EPOCHS[idx-1]} dir={hit_dir} ltp={ltp} candle={cur_candle}")
            final_dir = hit_dir
            if not done:
                print(f"[ARM] idx={idx} lvl={armed_lvl} dir={armed_dir} ltp={ltp} candle={cur_candle}")

        if done:
            print("[DONE] Exit epochs complete -> square-off TAG positions, then cancel open TAG orders.")
            squareoff_tag_positions(kite)

            # Give the square-off order(s) a moment to reach OMS, then cancel any leftover open orders (e.g., SL/target).
            time.sleep(1)
            cancel_open_tag_orders(kite)

            beep()  # <-- beep on exit action
            return

        prev = ltp
        time.sleep(POLL_SEC)


if __name__ == "__main__":
    main()
