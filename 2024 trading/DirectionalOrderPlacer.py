import Utils as util
import time as tm
from datetime import datetime
import pytz


if __name__ == '__main__':

    indian_timezone = pytz.timezone('Asia/Calcutta')

    kite = util.intialize_kite_api()

    choice = 2

    ###############################
    if choice == 1:
        # NIFTY24D1924700PE
        ###############################
        # UNDER_LYING_EXCHANGE = kite.EXCHANGE_BSE
        UNDER_LYING_EXCHANGE = kite.EXCHANGE_NSE
        UNDERLYING = ':NIFTY 50'
        OPTIONS_EXCHANGE = kite.EXCHANGE_NFO
        # PART_SYMBOL = ':NIFTY25123'
        PART_SYMBOL = ':NIFTY25213'
        NO_OF_LOTS = 300
        STRIKE_MULTIPLE = 50
        trigger_value = None
        transaction_type = kite.TRANSACTION_TYPE_SELL
        option_type = 'PE'
    elif choice == 2:
        UNDER_LYING_EXCHANGE = kite.EXCHANGE_BSE
        UNDERLYING = ':SENSEX'
        OPTIONS_EXCHANGE = kite.EXCHANGE_BFO
        PART_SYMBOL = ':SENSEX25218'
        # PART_SYMBOL = ':SENSEX25JAN'
        NO_OF_LOTS = 100
        STRIKE_MULTIPLE = 100
        trigger_value = None
        transaction_type = kite.TRANSACTION_TYPE_SELL
        option_type = 'PE'
    else:
        UNDER_LYING_EXCHANGE = kite.EXCHANGE_NSE
        UNDERLYING = ':NIFTY BANK'
        OPTIONS_EXCHANGE = kite.EXCHANGE_NFO
        PART_SYMBOL = ':BANKNIFTY25JAN'
        NO_OF_LOTS = 105
        STRIKE_MULTIPLE = 100
        trigger_value = None
        transaction_type = kite.TRANSACTION_TYPE_SELL
        option_type = 'PE'

    ###############################

    # under_lying_symbol = kite.EXCHANGE_NSE + ':NIFTY 50'
    under_lying_symbol = UNDER_LYING_EXCHANGE + UNDERLYING

    while datetime.now(indian_timezone).time() < util.TRADE_START_TIME:
        pass

    if datetime.now(indian_timezone).time() > util.MARKET_END_TIME:
        print(f"Market is closed. Hence exiting.")
        exit(0)

    while True:

        ul_live_quote = kite.quote(under_lying_symbol)

        ul_ltp = ul_live_quote[under_lying_symbol]['last_price']

        # nifty_ltp_round_50 = round(nifty_ltp / 50) * 50
        ul_ltp_round = round(ul_ltp / STRIKE_MULTIPLE) * STRIKE_MULTIPLE

        option = OPTIONS_EXCHANGE + PART_SYMBOL + str(ul_ltp_round) + option_type

        if (transaction_type ==kite.TRANSACTION_TYPE_SELL and (option_type == 'PE' and  ul_ltp > trigger_value) or (option_type == 'CE' and  ul_ltp < trigger_value)) or (transaction_type ==kite.TRANSACTION_TYPE_BUY and (option_type == 'PE' and  ul_ltp < trigger_value) or (option_type == 'CE' and  ul_ltp > trigger_value)):
            order_details = kite.place_order(tradingsymbol=option,
                             variety=kite.VARIETY_REGULAR,
                             exchange=OPTIONS_EXCHANGE,
                             transaction_type=transaction_type,
                             quantity=NO_OF_LOTS,
                             order_type=kite.ORDER_TYPE_MARKET,
                             product=kite.PRODUCT_MIS,
                             )
            print(f"Placed {order_details}: {transaction_type} order for : {option} at {datetime.now(indian_timezone).time()}.")

        tm.sleep(1)
