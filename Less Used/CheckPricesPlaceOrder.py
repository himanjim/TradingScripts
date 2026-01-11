import asyncio
import traceback
from datetime import time, datetime

from Orders import *

# import time as sleep_time

MARKET_START_TIME_AFTER_VOLATILITY = time (9, 15, 1, 0)

testing = False

today_date = datetime.today ().date ()

orders_placed_stocks = []
stocks_put_to_execution = []

orders_placed = kite.orders()
for order_placed in orders_placed:
    symbol = order_placed['tradingsymbol'].upper()
    if order_placed['status'].upper () != 'REJECTED' and order_placed['status'].upper () != 'CANCELLED':
        if symbol not in orders_placed_stocks:
            orders_placed_stocks.append(symbol)

stocks_to_trade = []
for formed_order in orders:
    stocks_to_trade.append(formed_order['nse_symbol'])


async def check_price_place_order(order, stocks_live_data):
    try:
        stock_id = order['symbol']

        if stock_id not in orders_placed_stocks:
            stock_data = stocks_live_data[order['nse_symbol']]

            if testing is False and (util.MARKET_START_TIME > stock_data['last_trade_time'].time() or today_date > stock_data['last_trade_time'].date()):
                print('Old data:', stock_data, stock_id)
                return

            open_price = stock_data['ohlc']['open']
            ltp = stock_data['last_price']

            if open_price == ltp:
                return

            if len(stocks_put_to_execution) < (order['priority'] - 1):
                return

            if order['symbol'] not in stocks_put_to_execution:
                stocks_put_to_execution.append (order['symbol'])

            if 'action' not in order:
                action = [kite.TRANSACTION_TYPE_SELL, kite.TRANSACTION_TYPE_BUY][ltp > open_price]
            else:
                action = [kite.TRANSACTION_TYPE_SELL, kite.TRANSACTION_TYPE_BUY][order['action'] == 1]

            price = ltp + [-order['trigger_price_pts'], order['trigger_price_pts']][action == kite.TRANSACTION_TYPE_BUY]
            trigger_price = open_price + [order['trigger_price_pts'], -order['trigger_price_pts']][
                    action == kite.TRANSACTION_TYPE_BUY]

            kite.place_order(tradingsymbol=order['symbol'],
                            variety=kite.VARIETY_CO ,
                            exchange=kite.EXCHANGE_NSE,
                            transaction_type=kite.TRANSACTION_TYPE_BUY,
                            quantity=int(order['future_lot'] / 2),
                            order_type=kite.ORDER_TYPE_LIMIT,
                            product=kite.PRODUCT_CO,
                            price=price,
                            trigger_price=trigger_price)

            print ('Placed %s order for stock:%s at %s. Data:%s' % (
            str (action), stock_id, str (datetime.now ()), str (stock_data)))

            orders_placed_stocks.append (stock_id)

    except Exception:
        print(traceback.format_exc())


async def check_prices_place_orders(stocks_live_data):
    tasks = list ()
    for od in orders:
        tasks.append(asyncio.ensure_future(check_price_place_order(od, stocks_live_data)))

    await asyncio.gather (*tasks)


while datetime.now().time() < MARKET_START_TIME_AFTER_VOLATILITY and testing is False:
    pass

if len (orders_placed_stocks) > 0:
    print ('%d orders already placed: %s' % (len (orders_placed_stocks), str (orders_placed_stocks)))

    orders_not_placed_stocks = []
    for od in orders:
        if od['symbol'].upper () not in orders_placed_stocks:
            orders_not_placed_stocks.append (od['symbol'].upper ())

    print ('%d orders still to be placed: %s' % (len (orders_not_placed_stocks), str (orders_not_placed_stocks)))

    if len (orders_not_placed_stocks) == 0:
        exit (0)

print ('Started at:', datetime.now ())


while True:
    stocks_live_data = kite.quote (stocks_to_trade)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(check_prices_place_orders(stocks_live_data))



exit (0)
