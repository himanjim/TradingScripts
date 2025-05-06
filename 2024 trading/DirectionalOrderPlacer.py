import OptionTradeUtils as oUtils
import time as tm
from datetime import datetime
import pytz


if __name__ == '__main__':

    indian_timezone = pytz.timezone('Asia/Calcutta')

    kite = oUtils.intialize_kite_api()

    UNDER_LYING_EXCHANGE, UNDERLYING, OPTIONS_EXCHANGE, PART_SYMBOL, NO_OF_LOTS, STRIKE_MULTIPLE = oUtils.get_instruments(
        kite)
    PART_SYMBOL = PART_SYMBOL.replace(':', '')
    # under_lying_symbol = kite.EXCHANGE_NSE + ':NIFTY 50'
    under_lying_symbol = UNDER_LYING_EXCHANGE + UNDERLYING
    option_type = 'PE'
    transaction_type = kite.TRANSACTION_TYPE_SELL
    trigger_value = None

    while datetime.now(indian_timezone).time() < oUtils.TRADE_START_TIME:
        pass

    while True:

        if datetime.now(indian_timezone).time() > oUtils.MARKET_END_TIME:
            print(f"Market is closed. Hence exiting.")
            exit(0)

        ul_live_quote = kite.quote(under_lying_symbol)

        ul_ltp = ul_live_quote[under_lying_symbol]['last_price']

        # nifty_ltp_round_50 = round(nifty_ltp / 50) * 50
        ul_ltp_round = round(ul_ltp / STRIKE_MULTIPLE) * STRIKE_MULTIPLE

        option = PART_SYMBOL + str(ul_ltp_round) + option_type

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
            exit(0)

        tm.sleep(1)
