from datetime import datetime
import pytz
import OptionTradeUtils as oUtils
import time

OPTION_TICK = 0.05
ORDER_STATUS_POLL_SECONDS = 0.5
ORDER_STATUS_MAX_POLLS = 8
MARKET_PROTECTION = -1   # automatic market protection by Zerodha

def _round_to_tick(price, tick=OPTION_TICK):
    price = max(float(price), tick)
    return round(round(price / tick) * tick, 2)


def _marketable_limit_price(kite, exchange, tradingsymbol, transaction_type):
    q = kite.quote(f"{exchange}:{tradingsymbol}")[f"{exchange}:{tradingsymbol}"]

    depth = q.get("depth", {})
    buy_depth = depth.get("buy", [])
    sell_depth = depth.get("sell", [])
    ltp = float(q.get("last_price") or 0.0)

    if transaction_type == kite.TRANSACTION_TYPE_SELL:
        # For sell orders, use best bid if available.
        if buy_depth and buy_depth[0].get("price"):
            px = float(buy_depth[0]["price"])
        else:
            px = ltp * 0.995
    else:
        # For buy orders, use best ask if available.
        if sell_depth and sell_depth[0].get("price"):
            px = float(sell_depth[0]["price"])
        else:
            px = ltp * 1.005

    return _round_to_tick(px)

def _get_order_snapshot(kite, order_id):
    """
    Return the latest order-book snapshot for the given order_id.
    """
    orders = kite.orders()
    for o in reversed(orders):
        if o.get("order_id") == order_id:
            return o
    return None


def _is_order_complete(order_row):
    return (
        order_row is not None
        and str(order_row.get("status", "")).upper() == "COMPLETE"
        and int(order_row.get("pending_quantity") or 0) == 0
    )


def _is_order_dead(order_row):
    return (
        order_row is not None
        and str(order_row.get("status", "")).upper() in {"REJECTED", "CANCELLED"}
    )


def _ensure_filled_or_convert_to_market(kite, order_id, tradingsymbol):
    """
    Wait briefly for LIMIT order completion.
    If still pending/open, modify it to MARKET.
    Then keep polling until terminal state.
    """
    market_modified = False

    for _ in range(ORDER_STATUS_MAX_POLLS):
        time.sleep(ORDER_STATUS_POLL_SECONDS)

        order_row = _get_order_snapshot(kite, order_id)
        if order_row is None:
            continue

        status = str(order_row.get("status", "")).upper()
        pending_qty = int(order_row.get("pending_quantity") or 0)
        filled_qty = int(order_row.get("filled_quantity") or 0)

        if _is_order_complete(order_row):
            print(f"✅ {tradingsymbol}: COMPLETE (filled={filled_qty})")
            return True

        if _is_order_dead(order_row):
            print(f"❌ {tradingsymbol}: {status} (filled={filled_qty}, pending={pending_qty})")
            return False

        # Any non-terminal state with pending qty means order is not fully done yet.
        if pending_qty > 0 and not market_modified:
            try:
                kite.modify_order(
                    variety=kite.VARIETY_REGULAR,
                    order_id=order_id,
                    order_type=kite.ORDER_TYPE_MARKET,
                    market_protection=MARKET_PROTECTION,
                )
                market_modified = True
                print(
                    f"⚠ {tradingsymbol}: not complete yet (status={status}, pending={pending_qty}). "
                    f"Modified to MARKET."
                )
            except Exception as e:
                print(f"WARNING: Could not modify {tradingsymbol} to MARKET yet: {e}")

    # Final check after polling loop
    order_row = _get_order_snapshot(kite, order_id)
    if _is_order_complete(order_row):
        print(f"✅ {tradingsymbol}: COMPLETE after follow-up polling.")
        return True

    if order_row:
        print(
            f"⚠ {tradingsymbol}: final state unresolved. "
            f"status={order_row.get('status')}, "
            f"filled={order_row.get('filled_quantity')}, "
            f"pending={order_row.get('pending_quantity')}"
        )
    else:
        print(f"⚠ {tradingsymbol}: order snapshot not found in order book.")

    return False


def _square_off_single_leg_if_naked(kite, tradingsymbol, exchange):
    """
    Safety fallback:
    If only one leg exists as an open position, square it off with MARKET.
    """
    positions = kite.positions()["net"]
    for p in positions:
        if p["tradingsymbol"] == tradingsymbol and int(p["quantity"]) != 0:
            net_qty = int(p["quantity"])

            if net_qty < 0:
                txn = kite.TRANSACTION_TYPE_BUY
            else:
                txn = kite.TRANSACTION_TYPE_SELL

            try:
                kite.place_order(
                    tradingsymbol=tradingsymbol,
                    variety=kite.VARIETY_REGULAR,
                    exchange=exchange,
                    transaction_type=txn,
                    quantity=abs(net_qty),
                    order_type=kite.ORDER_TYPE_MARKET,
                    product=kite.PRODUCT_NRML,
                    tag=oUtils.SS_ORDER_TAG,
                    market_protection=MARKET_PROTECTION,
                )
                print(f"🚨 Squared off naked leg {tradingsymbol} with MARKET {txn}, qty={abs(net_qty)}")
            except Exception as e:
                print(f"WARNING: Failed to square off naked leg {tradingsymbol}: {e}")

            break


def place_order(_pe, _ce, _transaction, _lots, _exchange):
    positions = kite.positions()
    net_positions = positions["net"]
    open_positions = [p["tradingsymbol"] for p in net_positions if p["quantity"] != 0]

    if _pe in open_positions or _ce in open_positions:
        existing = _pe if _pe in open_positions else _ce
        print(f"⛔ Trade skipped: Existing position found in {existing}\n")
        return

    pe_price = _marketable_limit_price(kite, _exchange, _pe, _transaction)
    ce_price = _marketable_limit_price(kite, _exchange, _ce, _transaction)

    pe_order_id = None
    ce_order_id = None

    try:
        pe_order_id = kite.place_order(
            tradingsymbol=_pe,
            variety=kite.VARIETY_REGULAR,
            exchange=_exchange,
            transaction_type=_transaction,
            quantity=_lots,
            order_type=kite.ORDER_TYPE_LIMIT,
            price=pe_price,
            product=kite.PRODUCT_NRML,
            tag=oUtils.SS_ORDER_TAG,
        )

        ce_order_id = kite.place_order(
            tradingsymbol=_ce,
            variety=kite.VARIETY_REGULAR,
            exchange=_exchange,
            transaction_type=_transaction,
            quantity=_lots,
            order_type=kite.ORDER_TYPE_LIMIT,
            price=ce_price,
            product=kite.PRODUCT_NRML,
            tag=oUtils.SS_ORDER_TAG,
        )

        print(
            f"Placed {_transaction} LIMIT orders for: {_pe} @ {pe_price} and {_ce} @ {ce_price} "
            f"at {datetime.now(indian_timezone).time()}."
        )

        # Verify both legs; if not complete, convert pending/open leg to MARKET.
        pe_ok = _ensure_filled_or_convert_to_market(kite, pe_order_id, _pe)
        ce_ok = _ensure_filled_or_convert_to_market(kite, ce_order_id, _ce)

        # Final safety: if one leg exists naked, square it off.
        if not (pe_ok and ce_ok):
            latest_positions = kite.positions()["net"]
            pe_net = next((int(p["quantity"]) for p in latest_positions if p["tradingsymbol"] == _pe), 0)
            ce_net = next((int(p["quantity"]) for p in latest_positions if p["tradingsymbol"] == _ce), 0)

            if pe_net != 0 and ce_net == 0:
                print(f"🚨 Naked PE leg detected in {_pe}. Squaring off.")
                _square_off_single_leg_if_naked(kite, _pe, _exchange)

            elif ce_net != 0 and pe_net == 0:
                print(f"🚨 Naked CE leg detected in {_ce}. Squaring off.")
                _square_off_single_leg_if_naked(kite, _ce, _exchange)

            elif pe_net != 0 and ce_net != 0:
                print("ℹ Both legs exist in positions. No naked-leg square-off required.")
            else:
                print("ℹ No open leg found after verification.")

    except Exception as e:
        print(f"Order placement failed: {e}")

        # If first leg got placed but second failed, try to neutralize first leg.
        if pe_order_id is not None:
            try:
                hedge_txn = (
                    kite.TRANSACTION_TYPE_BUY
                    if _transaction == kite.TRANSACTION_TYPE_SELL
                    else kite.TRANSACTION_TYPE_SELL
                )

                kite.place_order(
                    tradingsymbol=_pe,
                    variety=kite.VARIETY_REGULAR,
                    exchange=_exchange,
                    transaction_type=hedge_txn,
                    quantity=_lots,
                    order_type=kite.ORDER_TYPE_MARKET,
                    product=kite.PRODUCT_NRML,
                    tag=oUtils.SS_ORDER_TAG,
                    market_protection=MARKET_PROTECTION,
                )
                print(f"Neutralized first leg {_pe} with MARKET {hedge_txn}")
            except Exception as e2:
                print(f"WARNING: Failed to neutralize first leg {_pe}: {e2}")

if __name__ == '__main__':

    indian_timezone = pytz.timezone('Asia/Calcutta')

    kite = oUtils.intialize_kite_api()

    UNDER_LYING_EXCHANGE, UNDERLYING, OPTIONS_EXCHANGE, PART_SYMBOL, NO_OF_LOTS, STRIKE_MULTIPLE, STOPLOSS_POINTS, MINIMUM_LOTS, LONG_STRADDLE_STRIKE_DISTANCE = oUtils.get_instruments(kite)
    PART_SYMBOL = PART_SYMBOL.replace(':', '')


    ###############################
    while True:
        # under_lying_symbol = kite.EXCHANGE_NSE + ':NIFTY 50'
        under_lying_symbol = UNDER_LYING_EXCHANGE + UNDERLYING

        input(f"Press Enter to place SHORT straddle for {PART_SYMBOL}")

        ul_live_quote = kite.quote(under_lying_symbol)

        ul_ltp = ul_live_quote[under_lying_symbol]['last_price']

        # nifty_ltp_round_50 = round(nifty_ltp / 50) * 50
        ul_ltp_round = round(ul_ltp / STRIKE_MULTIPLE) * STRIKE_MULTIPLE

        option_pe = PART_SYMBOL + str(ul_ltp_round) + 'PE'
        option_ce = PART_SYMBOL + str(ul_ltp_round) + 'CE'

        place_order(option_pe, option_ce, kite.TRANSACTION_TYPE_SELL, NO_OF_LOTS, OPTIONS_EXCHANGE)

