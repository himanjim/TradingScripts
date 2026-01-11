import asyncio
import traceback

import Utils as util
from Orders import *
from pytz import timezone

max_loss = 2000
kite = util.intialize_kite_api()
indian_timezone = timezone('Asia/Calcutta')
today_date = datetime.now(indian_timezone).date()
testing = False


async def place_order(the_order, stocks_live_data):

    ltp = stocks_live_data[the_order['nse_symbol']]['last_price']
    lot = int(max_loss / abs(ltp - the_order['stop_loss']))

    if today_date > the_order['execution_date']:
        return

    transaction_type = [kite.TRANSACTION_TYPE_SELL, kite.TRANSACTION_TYPE_BUY][the_order['transaction_type'] == 1]

    try:
        kite.place_order (tradingsymbol=the_order['symbol'],
                          variety=kite.VARIETY_REGULAR,
                          exchange=kite.EXCHANGE_NSE,
                          transaction_type=transaction_type,
                          quantity=lot,
                          order_type=kite.ORDER_TYPE_MARKET,
                          product=kite.PRODUCT_CNC,
                          price=None,
                          trigger_price=None,
                          tag=None)
    except Exception:
        print (traceback.format_exc () + ' in Stock:' + str (the_order))


async def place_sl_order(the_order, stocks_live_data):

    ltp = stocks_live_data[the_order['nse_symbol']]['last_price']
    lot = int(max_loss / abs(ltp - the_order['stop_loss']))

    transaction_type = [kite.TRANSACTION_TYPE_BUY, kite.TRANSACTION_TYPE_SELL][the_order['transaction_type'] == 1]

    try:
        kite.place_order (tradingsymbol=the_order['symbol'],
                          variety=kite.VARIETY_REGULAR,
                          exchange=kite.EXCHANGE_NSE,
                          transaction_type=transaction_type,
                          quantity=lot,
                          order_type=kite.ORDER_TYPE_SLM,
                          product=kite.PRODUCT_CNC,
                          price=None,
                          trigger_price=the_order['stop_loss'],
                          tag=None)
    except Exception:
        print (traceback.format_exc () + ' in Stock:' + str (the_order))


async def place_orders(stocks_live_data):
    tasks = list ()
    for order in orders:
        tasks.append(asyncio.ensure_future(place_order(order, stocks_live_data)))

    await asyncio.gather (*tasks)


async def place_sl_orders(stocks_live_data):
    tasks = list ()
    for order in orders:
        tasks.append(asyncio.ensure_future(place_sl_order(order, stocks_live_data)))

    await asyncio.gather (*tasks)

symbols = []
for order in orders:
    symbols.append(order['nse_symbol'])

while datetime.now (indian_timezone).time () < util.MARKET_START_TIME and testing is False:
    pass

stocks_live_data = kite.quote (symbols)
while testing is False and today_date > stocks_live_data[orders[0]['nse_symbol']]['last_trade_time'].date ():
    print ('Old data:' + str (stocks_live_data))
    stocks_live_data = kite.quote (symbols)


loop = asyncio.get_event_loop()
loop.run_until_complete(place_orders(stocks_live_data))

loop = asyncio.get_event_loop()
loop.run_until_complete(place_sl_orders(stocks_live_data))

exit (0)
