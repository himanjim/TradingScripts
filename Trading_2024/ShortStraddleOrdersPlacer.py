from datetime import datetime
import pytz
import OptionTradeUtils as oUtils


OPTION_TICK = 0.05


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

        kite.place_order(
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
                pe_exit_price = _marketable_limit_price(kite, _exchange, _pe, hedge_txn)

                kite.place_order(
                    tradingsymbol=_pe,
                    variety=kite.VARIETY_REGULAR,
                    exchange=_exchange,
                    transaction_type=hedge_txn,
                    quantity=_lots,
                    order_type=kite.ORDER_TYPE_LIMIT,
                    price=pe_exit_price,
                    product=kite.PRODUCT_NRML,
                    tag=oUtils.SS_ORDER_TAG,
                )
                print(f"Neutralized first leg {_pe} with {hedge_txn} LIMIT @ {pe_exit_price}")
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

