import time as tm
from datetime import datetime
import pytz
import OptionTradeUtils as oUtils

# winsound is Windows-only; guard so script doesn't crash on Linux/macOS
try:
    import winsound  # type: ignore
except Exception:
    winsound = None


def beep():
    try:
        if winsound:
            winsound.Beep(2000, 2000)
    except Exception:
        pass


def _parse_ts(s):
    """
    Kite timestamps vary. Parse defensively.
    Returns naive datetime; ordering still works for same-day strings.
    """
    if not s:
        return datetime.min
    try:
        return datetime.fromisoformat(s)
    except Exception:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(s, fmt)
            except Exception:
                pass
    return datetime.min


def exit_trade(kite, position):
    kite.place_order(
        tradingsymbol=position["tradingsymbol"],
        variety=kite.VARIETY_REGULAR,
        exchange=position["exchange"],
        transaction_type=(
            kite.TRANSACTION_TYPE_BUY
            if position["type"] == kite.TRANSACTION_TYPE_SELL
            else kite.TRANSACTION_TYPE_SELL
        ),
        quantity=position["quantity"],
        order_type=kite.ORDER_TYPE_MARKET,
        product=position["product"],
        tag=oUtils.SS_ORDER_TAG,
    )


def _get_live_pos_map(kite):
    """
    Map tradingsymbol -> live position row (day positions).
    """
    pos = kite.positions()
    day = pos.get("day", [])
    mp = {}
    for p in day:
        ts = p.get("tradingsymbol")
        if ts:
            mp[ts] = p
    return mp


def _get_tagged_orders(kite):
    """
    Return tagged orders (OPEN/COMPLETE) for MIS/NRML regular variety.
    """
    out = []
    try:
        for o in kite.orders():
            if (
                o.get("product") in ("NRML", "MIS")
                and o.get("variety") == "regular"
                and str(o.get("tag", "")) == str(oUtils.SS_ORDER_TAG)
                and o.get("status") in ("OPEN", "COMPLETE")
            ):
                # Must have some fill, otherwise avg price is junk
                filled = int(o.get("filled_quantity") or 0)
                if filled > 0:
                    out.append(o)
    except Exception:
        pass

    # Sort by exchange_timestamp/order_timestamp for consistent "latest"
    out.sort(key=lambda x: _parse_ts(x.get("exchange_timestamp") or x.get("order_timestamp")))
    return out


def pick_latest_open_straddle_from_orders(kite, max_leg_gap_sec=180):
    """
    OLD-STYLE selection rule, but corrected:
    - Choose latest SELL-SELL CE+PE pair from ORDERS (tagged)
    - Ensure BOTH legs are OPEN in live positions (qty != 0 and net short)
      so we never compute PNL from a closed straddle.

    Returns:
      (positions, key)
      positions = [ {exchange, tradingsymbol, quantity, price, product, type}, ... ]  (2 legs)
      key = tuple identifying the straddle attempt
    """
    orders = _get_tagged_orders(kite)
    if not orders:
        return [], None

    live = _get_live_pos_map(kite)

    # Iterate from newest to oldest to find the "latest straddle" by latest orders
    for i in range(len(orders) - 1, -1, -1):
        o1 = orders[i]
        if o1.get("transaction_type") != kite.TRANSACTION_TYPE_SELL:
            continue

        sym1 = o1.get("tradingsymbol")
        if not sym1 or not (sym1.endswith("CE") or sym1.endswith("PE")):
            continue

        base = sym1[:-2]
        sym2 = base + ("PE" if sym1.endswith("CE") else "CE")

        # Find the most recent SELL order for the other leg *before or around* this time
        t1 = _parse_ts(o1.get("exchange_timestamp") or o1.get("order_timestamp"))
        o2 = None
        t2 = None

        for j in range(i - 1, -1, -1):
            cand = orders[j]
            if cand.get("transaction_type") != kite.TRANSACTION_TYPE_SELL:
                continue
            if cand.get("tradingsymbol") != sym2:
                continue
            t2 = _parse_ts(cand.get("exchange_timestamp") or cand.get("order_timestamp"))
            o2 = cand
            break

        if not o2:
            continue

        # Guard against pairing with an older attempt (same symbol reused)
        if abs((t1 - t2).total_seconds()) > max_leg_gap_sec:
            continue

        # Validate BOTH legs are OPEN and SHORT in live positions (exclude closed straddles)
        p1 = live.get(sym1)
        p2 = live.get(sym2)
        if not p1 or not p2:
            continue

        q1 = int(p1.get("quantity", 0))
        q2 = int(p2.get("quantity", 0))
        if q1 == 0 or q2 == 0:
            continue

        # Must be net short for a short straddle
        if q1 > 0 or q2 > 0:
            continue

        # Build positions using:
        # - entry price from latest SELL orders (avg price)
        # - quantity from LIVE positions (abs net qty) so exit qty is correct
        pos = [
            {
                "exchange": p1["exchange"],
                "tradingsymbol": sym1,
                "quantity": abs(q1),
                "price": float(o1.get("average_price") or 0.0),
                "product": p1["product"],
                "type": kite.TRANSACTION_TYPE_SELL,
            },
            {
                "exchange": p2["exchange"],
                "tradingsymbol": sym2,
                "quantity": abs(q2),
                "price": float(o2.get("average_price") or 0.0),
                "product": p2["product"],
                "type": kite.TRANSACTION_TYPE_SELL,
            },
        ]

        # Stable key: base + both entry timestamps + symbols
        key = (
            base,
            sym1,
            sym2,
            (o1.get("exchange_timestamp") or o1.get("order_timestamp") or ""),
            (o2.get("exchange_timestamp") or o2.get("order_timestamp") or ""),
        )

        # Ensure consistent ordering CE then PE for printing
        pos.sort(key=lambda d: d["tradingsymbol"])
        return pos, key

    return [], None


if __name__ == "__main__":
    # === HONOR THESE EXACTLY ===
    MAX_PROFIT = 10000
    MAX_LOSS = -5000
    MAX_PROFIT_EROSION = 10000

    max_profit_set = None  # e.g. 14500.0 if restarting and you already saw peak profit ~14500

    BASE_SLEEP = 2.0
    FAST_SLEEP = 0.75  # don't hammer API

    indian_timezone = pytz.timezone("Asia/Calcutta")
    kite = oUtils.intialize_kite_api()

    # Keep your bootstrap (your utils may rely on it)
    (
        UNDER_LYING_EXCHANGE,
        UNDERLYING,
        OPTIONS_EXCHANGE,
        PART_SYMBOL,
        NO_OF_LOTS,
        STRIKE_MULTIPLE,
        STOPLOSS_POINTS,
        MIN_LOTS,
        LONG_STRADDLE_STRIKE_DISTANCE,
    ) = oUtils.get_instruments(kite)

    current_key = None
    positions = []
    symbols = []

    max_pl = 0.0
    min_pl = 0.0
    sleep_time = BASE_SLEEP

    print(f"CONFIG: MAX_PROFIT={MAX_PROFIT} MAX_LOSS={MAX_LOSS} MAX_PROFIT_EROSION={MAX_PROFIT_EROSION}")

    while True:
        try:
            now = datetime.now(indian_timezone)
            now_t = now.time()

            if now_t > oUtils.MARKET_END_TIME:
                print("Market is closed. Exiting tracker.")
                break

            # Re-pick latest OPEN straddle using latest orders, ignoring closed ones
            new_positions, new_key = pick_latest_open_straddle_from_orders(kite)

            if not new_positions:
                print("No OPEN tagged short straddle found (latest may be closed).")
                break

            # Reset tracking when a new straddle attempt is detected
            if new_key != current_key:
                current_key = new_key
                positions = new_positions
                symbols = [p["exchange"] + ":" + p["tradingsymbol"] for p in positions]
                max_pl = 0.0
                min_pl = 0.0
                print(f"Tracking latest OPEN straddle (orders-based): {symbols}")

            # LTP fetch (lighter than quote)
            ltp_map = kite.ltp(symbols)

            net_pl = 0.0
            for p in positions:
                key = p["exchange"] + ":" + p["tradingsymbol"]
                ltp = float(ltp_map[key]["last_price"])
                entry = float(p["price"])
                qty = int(p["quantity"])

                # short P/L
                p["pl"] = (entry - ltp) * qty
                net_pl += p["pl"]

            if net_pl > max_pl:
                max_pl = net_pl

            if max_profit_set is not None and float(max_profit_set) > max_pl:
                max_pl = float(max_profit_set)

            if net_pl < min_pl:
                min_pl = net_pl

            drawdown = max_pl - net_pl

            # Sleep tuning (based on CURRENT net_pl, not min_pl history)
            if net_pl <= (MAX_LOSS * 0.9):      # <= -4500
                sleep_time = FAST_SLEEP
            elif net_pl <= (MAX_LOSS * 0.6):    # <= -3000
                sleep_time = 1.0
            else:
                sleep_time = BASE_SLEEP

            # Exit logic (ONLY these)
            exit_reason = None
            if net_pl >= MAX_PROFIT:
                exit_reason = "MAX_PROFIT"
            elif net_pl <= MAX_LOSS:
                exit_reason = "STOPLOSS"
            elif max_pl > 0 and drawdown >= MAX_PROFIT_EROSION:
                exit_reason = "MAX_PROFIT_EROSION"

            print(
                f"Net P/L={net_pl:.2f} | Peak={max_pl:.2f} | Min={min_pl:.2f} | "
                f"Drawdown={drawdown:.2f} | MAX_PROFIT={MAX_PROFIT} | {now_t}"
            )

            if exit_reason:
                print(
                    f"EXIT_TRIGGER={exit_reason} | net_pl={net_pl:.2f} | peak={max_pl:.2f} | drawdown={drawdown:.2f} | MAX_PROFIT={MAX_PROFIT}"
                )

                # IMPORTANT: re-check live positions once more and exit only if still open
                live_map = _get_live_pos_map(kite)
                for p in positions:
                    lp = live_map.get(p["tradingsymbol"])
                    if not lp:
                        continue
                    q = int(lp.get("quantity", 0))
                    if q == 0:
                        continue
                    # adjust to true open qty
                    p["quantity"] = abs(q)
                    p["product"] = lp["product"]
                    p["exchange"] = lp["exchange"]
                    p["type"] = kite.TRANSACTION_TYPE_SELL if q < 0 else kite.TRANSACTION_TYPE_BUY
                    exit_trade(kite, p)
                    print(f"Exited {p['tradingsymbol']} | leg P/L={p.get('pl', 0.0):.2f}")

                beep()

                # Optional: match your old behavior
                # oUtils.cancel_all_open_orders(kite)

                break

            tm.sleep(sleep_time)

        except Exception as e:
            print(f"An error occurred: {e}")
            tm.sleep(2.0)
            continue