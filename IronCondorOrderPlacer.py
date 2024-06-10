from datetime import datetime, time
import pytz
import concurrent.futures
import time as tm
import Utils as util


def order1():
    order_no = 0
    stocks_live_data = kite.quote(nse_symbols[order_no])
    kite.place_order(tradingsymbol=symbols[order_no],
                     variety=kite.VARIETY_REGULAR,
                     exchange=kite.EXCHANGE_NFO,
                     transaction_type=kite.TRANSACTION_TYPE_SELL,
                     quantity=NO_OF_LOTS,
                     order_type=kite.ORDER_TYPE_LIMIT,
                     product=kite.PRODUCT_MIS,
                     price=stocks_live_data[nse_symbols[order_no]]['last_price'] - LIMIT_PTS,
                     )


def order2():
    order_no = 1
    stocks_live_data = kite.quote(nse_symbols[order_no])
    kite.place_order(tradingsymbol=symbols[order_no],
                     variety=kite.VARIETY_REGULAR,
                     exchange=kite.EXCHANGE_NFO,
                     transaction_type=kite.TRANSACTION_TYPE_SELL,
                     quantity=NO_OF_LOTS,
                     order_type=kite.ORDER_TYPE_LIMIT,
                     product=kite.PRODUCT_MIS,
                     price=stocks_live_data[nse_symbols[order_no]]['last_price'] - LIMIT_PTS,
                     )


def execute_parallel_orders():
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit the tasks to the executor
        future1 = executor.submit(order1)
        future2 = executor.submit(order2)

        # Wait for the tasks to complete
        concurrent.futures.wait([future1, future2])


if __name__ == '__main__':
    LIMIT_PTS = 10
    NO_OF_LOTS = 105

    kite = util.intialize_kite_api()
    indian_timezone = pytz.timezone('Asia/Calcutta')
    today_date = datetime.now(indian_timezone).date()
    testing = False

    symbols = ['BANKNIFTY2461249800CE', 'BANKNIFTY2461249800PE']
    nse_symbols = [kite.EXCHANGE_NFO + ':' + symbols[0], kite.EXCHANGE_NFO + ':' + symbols[1]]

    while datetime.now(indian_timezone).time() < util.MARKET_START_TIME and testing is False:
        pass

    start_time = tm.time()
    execute_parallel_orders()
    end_time = tm.time()

    print('Orders executed in time(secs):', (end_time - start_time))

    orders = kite.orders()

    for order in orders:
        kite.modify_order(kite.VARIETY_REGULAR, order['order_id'], order_type=kite.ORDER_TYPE_MARKET)