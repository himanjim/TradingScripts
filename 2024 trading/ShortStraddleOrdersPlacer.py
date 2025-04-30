import Utils as util
import time as tm
from datetime import datetime
import pytz


def place_order(_pe, _ce, _transaction, _lots, _exchange):
    kite.place_order(tradingsymbol=_pe,
                     variety=kite.VARIETY_REGULAR,
                     exchange=_exchange,
                     transaction_type=_transaction,
                     quantity=_lots,
                     order_type=kite.ORDER_TYPE_MARKET,
                     product=kite.PRODUCT_MIS,
                     )

    kite.place_order(tradingsymbol=_ce,
                     variety=kite.VARIETY_REGULAR,
                     exchange=_exchange,
                     transaction_type=_transaction,
                     quantity=_lots,
                     order_type=kite.ORDER_TYPE_MARKET,
                     product=kite.PRODUCT_MIS,
                     )
    print(f"Placed {_transaction} order for : {_pe} and {_ce} at {datetime.now(indian_timezone).time()}.")


if __name__ == '__main__':

    indian_timezone = pytz.timezone('Asia/Calcutta')

    kite = util.intialize_kite_api()

    choice = 1

    ###############################
    if choice == 1:
        # NIFTY24D1924700PE
        ###############################
        # UNDER_LYING_EXCHANGE = kite.EXCHANGE_BSE
        UNDER_LYING_EXCHANGE = kite.EXCHANGE_NSE
        UNDERLYING = ':NIFTY 50'
        OPTIONS_EXCHANGE = kite.EXCHANGE_NFO
        # PART_SYMBOL = 'NIFTY25123'
        # PART_SYMBOL = 'NIFTY25220'
        PART_SYMBOL = 'NIFTY25430'
        NO_OF_LOTS = 300
        STRIKE_MULTIPLE = 50
    elif choice == 2:
        UNDER_LYING_EXCHANGE = kite.EXCHANGE_BSE
        UNDERLYING = ':SENSEX'
        OPTIONS_EXCHANGE = kite.EXCHANGE_BFO
        PART_SYMBOL = 'SENSEX25225'
        # PART_SYMBOL = 'SENSEX25JAN'
        NO_OF_LOTS = 100
        STRIKE_MULTIPLE = 100

    else:
        UNDER_LYING_EXCHANGE = kite.EXCHANGE_NSE
        UNDERLYING = ':NIFTY BANK'
        OPTIONS_EXCHANGE = kite.EXCHANGE_NFO
        PART_SYMBOL = 'BANKNIFTY25APR'
        NO_OF_LOTS = 120
        STRIKE_MULTIPLE = 100

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

