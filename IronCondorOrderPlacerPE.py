import time as tm
from datetime import datetime

import pytz

import Utils as util

if __name__ == '__main__':
    LIMIT_PTS = 100
    NO_OF_LOTS = 15

    kite = util.intialize_kite_api()
    indian_timezone = pytz.timezone('Asia/Calcutta')
    today_date = datetime.now(indian_timezone).date()
    testing = False

    symbol = 'BANKNIFTY2461249600PE'
    nse_symbol = kite.EXCHANGE_NFO + ':' + symbol

    while datetime.now(indian_timezone).time() < util.MARKET_START_TIME and testing is False:
        pass

    start_time = tm.time()

    stocks_live_data = kite.quote(nse_symbol)
    order_id = kite.place_order(tradingsymbol=symbol,
                                variety=kite.VARIETY_REGULAR,
                                exchange=kite.EXCHANGE_NFO,
                                transaction_type=kite.TRANSACTION_TYPE_SELL,
                                quantity=NO_OF_LOTS,
                                order_type=kite.ORDER_TYPE_LIMIT,
                                product=kite.PRODUCT_MIS,
                                price=stocks_live_data[nse_symbol]['last_price'] + LIMIT_PTS,
                                )

    end_time = tm.time()

    print('Orders executed in time(secs):', (end_time - start_time), ' at time: ', datetime.now(indian_timezone).time())

    code = input('Press ENTER to modify orders as MARKET')

    if order_id is not None:
        kite.modify_order(kite.VARIETY_REGULAR, order_id, order_type=kite.ORDER_TYPE_MARKET)
