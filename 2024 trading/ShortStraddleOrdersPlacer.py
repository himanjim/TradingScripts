from datetime import datetime
import pytz
import OptionTradeUtils as oUtils


def place_order(_pe, _ce, _transaction, _lots, _exchange):
    kite.place_order(tradingsymbol=_pe,
                     variety=kite.VARIETY_REGULAR,
                     exchange=_exchange,
                     transaction_type=_transaction,
                     quantity=_lots,
                     order_type=kite.ORDER_TYPE_MARKET,
                     product=kite.PRODUCT_NRML,
                     )

    kite.place_order(tradingsymbol=_ce,
                     variety=kite.VARIETY_REGULAR,
                     exchange=_exchange,
                     transaction_type=_transaction,
                     quantity=_lots,
                     order_type=kite.ORDER_TYPE_MARKET,
                     product=kite.PRODUCT_NRML,
                     )
    print(f"Placed {_transaction} order for : {_pe} and {_ce} at {datetime.now(indian_timezone).time()}.")


if __name__ == '__main__':

    indian_timezone = pytz.timezone('Asia/Calcutta')

    kite = oUtils.intialize_kite_api()

    UNDER_LYING_EXCHANGE, UNDERLYING, OPTIONS_EXCHANGE, PART_SYMBOL, NO_OF_LOTS, STRIKE_MULTIPLE = oUtils.get_instruments(kite)
    PART_SYMBOL = PART_SYMBOL.replace(':', '')


    ###############################

    # under_lying_symbol = kite.EXCHANGE_NSE + ':NIFTY 50'
    under_lying_symbol = UNDER_LYING_EXCHANGE + UNDERLYING

    input(f"Press Enter to place shrot straddle for {PART_SYMBOL}")

    ul_live_quote = kite.quote(under_lying_symbol)

    ul_ltp = ul_live_quote[under_lying_symbol]['last_price']

    # nifty_ltp_round_50 = round(nifty_ltp / 50) * 50
    ul_ltp_round = round(ul_ltp / STRIKE_MULTIPLE) * STRIKE_MULTIPLE

    option_pe = PART_SYMBOL + str(ul_ltp_round) + 'PE'
    option_ce = PART_SYMBOL + str(ul_ltp_round) + 'CE'

    place_order(option_pe, option_ce, kite.TRANSACTION_TYPE_SELL, NO_OF_LOTS, OPTIONS_EXCHANGE)

