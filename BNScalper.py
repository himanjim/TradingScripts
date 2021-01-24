import datetime as p_datetime
import logging
import os

import math
from kiteconnect import KiteTicker
from TradingScripts.BNScalperUtils import *

kite = util.intialize_kite_api()
kws = KiteTicker("your_api_key", "your_access_token")

testing = False
parent_orders = {}
break_even = 0
max_target_profit = 10000
INSTRUMENT = 'NSE:BANKNIFTY'
# Logging config
today_date_str = datetime.now(indian_timezone).strftime ('%Y-%m-%d')
logFormatter = logging.Formatter("%(asctime)s [%(module)s] [%(levelname)-5.5s]  %(message)s")
logger = logging.getLogger('Scalper')
logger.setLevel(logging.INFO)

fileHandler = logging.FileHandler(['G:', 'F:'][os.path.exists('F:')] + '/Trading_Responses/logs/Scalping_logs_' + today_date_str + '.log')
fileHandler.setFormatter(logFormatter)
logger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)

logging.Formatter.converter = custom_time

old_low = None
min_pts_up = 50



max_target_profit_reached = False

while datetime.now(indian_timezone).time () < p_datetime.time(9, 15, 50) and testing is False:
    pass

def on_ticks(ws, ticks):
    # Callback to receive ticks.
    logging.debug("Ticks: {}".format(ticks))
    ticks[0]['timestamp']

def on_connect(ws, response):
    # Callback on successful connect.
    # Subscribe to a list of instrument_tokens (RELIANCE and ACC here).
    ws.subscribe([738561, 5633])

    # Set RELIANCE to tick in `full` mode.
    ws.set_mode(ws.MODE_FULL, [738561])

def on_close(ws, code, reason):
    # On connection close stop the event loop.
    # Reconnection will not happen after executing `ws.stop()`
    ws.stop()

# Assign the callbacks.
kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close

# Infinite loop on the main thread. Nothing after this will run.
# You have to use the pre-defined callbacks to manage subscriptions.
kws.connect()

while True:
    try:
        orders = kite.orders ()
        positions = kite.positions ()['day']

        orders, positions, stocks_live_data = place_next_order (orders, positions, kite, logger)

        total_pnl = 0

        broker_orders_count = len(kite.orders()) / 2

        for position in positions:
            total_pnl += (position['pnl'] * (max_amount_for_future_lot / [position['buy_price'], position['sell_price']][position['buy_price'] == 0]) * .5)

        total_pnl = total_pnl - (broker_orders_count * 120)

        if datetime.now(indian_timezone).time().second % 5 == 0:
            print('P/L:%f' %(total_pnl))

        if total_pnl > max_target_profit:
            logger.info('Max target profit:%d reached.' %(max_target_profit))
            max_target_profit_reached = True

        if max_target_profit_reached and (max_target_profit - total_pnl) > 1000:
            logger.info('Max target profit:%d reached. Cancelling all open orders.' % (max_target_profit))
            for order in orders:
                if order['parent_order_id'] is not None and order['status'].upper() == 'TRIGGER PENDING':
                    kite.cancel_order(kite.VARIETY_CO, order['order_id'], order['parent_order_id'])
                    logger.info('Cancelled SL order:%s' % (str(order['order_id'])))
                if order['parent_order_id'] is None and order['status'].upper() == 'OPEN':
                    kite.cancel_order(kite.VARIETY_CO, order['order_id'])
                    logger.info('Cancelled open order:%s' % (str(order['order_id'])))
            exit(0)



    except Exception:
        logger.error (traceback.format_exc ())

