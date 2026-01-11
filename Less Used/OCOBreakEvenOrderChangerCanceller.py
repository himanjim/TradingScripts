import logging
import os
import sys
# add your project directory to the sys.path
project_home = '/home/himanjim/.local/lib/python2.7/site-packages/'
if project_home not in sys.path:
    sys.path.insert(0, project_home)

import UpDownGapUtils as upDnUtils
from NextAttemptOrderPlacerUtils import *

kite = util.intialize_kite_api()

testing = False
find_up_down_gap = False

break_even = 0
max_loss_to_bear = -1000

parent_orders = {}

# Logging config
today_date_str = datetime.now(indian_timezone).strftime ('%Y-%m-%d')
logFormatter = logging.Formatter("%(asctime)s [%(module)s] [%(levelname)-5.5s]  %(message)s")
logger = logging.getLogger('Trading')
logger.setLevel(logging.INFO)

log_directory = None
linux_log_dir = '/home/himanjim/'
office_log_dir = 'F:'
home_log_dir = 'G:'

if os.path.exists(linux_log_dir):
    log_directory = linux_log_dir
elif os.path.exists(office_log_dir):
    log_directory = office_log_dir
elif os.path.exists(home_log_dir):
    log_directory = home_log_dir
else:
    print('No log directory configured.')
    exit(0)

co_bo_stocks_dir = None
linux_co_bo_stocks_dir = '/home/himanjim/'
office_co_bo_stocks_dir = 'C:/Users/Admin/Desktop/'
home_co_bo_stocks_dir = 'C:/Users/Admin/Desktop/'

if os.path.exists(linux_co_bo_stocks_dir):
    co_bo_stocks_dir = linux_co_bo_stocks_dir
elif os.path.exists(office_co_bo_stocks_dir):
    co_bo_stocks_dir = office_co_bo_stocks_dir
elif os.path.exists(home_co_bo_stocks_dir):
    co_bo_stocks_dir = home_co_bo_stocks_dir
else:
    print('No co bo stock directory configured.')
    exit(0)


fileHandler = logging.FileHandler(log_directory + '/Trading_Responses/logs/Trading_logs_' + today_date_str + '.log')
fileHandler.setFormatter(logFormatter)
logger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)

logging.Formatter.converter = custom_time

if find_up_down_gap:
    fo_stock_ids = upDnUtils.get_fo_stock_ids(co_bo_stocks_dir)

first_run = False
while datetime.now(indian_timezone).time () < util.MARKET_START_TIME and testing is False:
    first_run = True

if find_up_down_gap:
    formed_orders.extend(upDnUtils.find_up_down_gap_orders(fo_stock_ids, kite, formed_orders, logger, testing))

while True:
    try:
        if first_run is False:
            orders = kite.orders ()
            positions = kite.positions ()['day']
        else:
            orders = []
            positions = []

        first_run = False

        orders, positions, stocks_live_data = place_next_order (orders, positions, kite, logger, max_loss_to_bear, testing)

        for order in orders:
            if order['parent_order_id'] is None and (order['status'].upper() == 'COMPLETE' or order['status'].upper() == 'OPEN'):
                parent_orders[order['order_id']] = order

        if len (parent_orders) == 0:
            continue

        for order in orders:

            if order['parent_order_id'] is None and order['status'].upper () == 'OPEN' and ((datetime.now(indian_timezone) - order['order_timestamp'].replace(tzinfo=indian_timezone)).seconds - 1380) >= 10:
                kite.cancel_order (kite.VARIETY_CO, order['order_id'])
                logger.info ('Cancelled open parent order:%s after 10 secs.' % (str (order['order_id'])))

            order_position = None
            for position in positions:
                if order['tradingsymbol'].upper() == position['tradingsymbol'].upper():
                    order_position = position
                    break

            if order['parent_order_id'] is not None and order['status'].upper () == 'TRIGGER PENDING' and order_position is not None and order_position['quantity'] != 0:
                if order['parent_order_id'] not in parent_orders:
                    logger.error ('No parent order for stock:%s and parent_order:%s.', order['tradingsymbol'], str (order['parent_order_id']))
                    continue

                parent_order = parent_orders[order['parent_order_id']]

                for formed_order in formed_orders:
                    if order['tradingsymbol'].upper () == formed_order['symbol'].upper ():
                        break

                ltp = stocks_live_data[formed_order['nse_symbol']]['last_price']

                unrealized_profit = (ltp - parent_order['average_price']) * parent_order['filled_quantity'] * [-1, 1][parent_order['transaction_type'].upper () == 'BUY']

                half_future_lot = formed_order['future_lot'] / 2
                lot_multi_factor = [1, parent_order['filled_quantity'] / half_future_lot][parent_order['filled_quantity'] < half_future_lot]

                if parent_order['tradingsymbol'].upper () == formed_order['symbol'].upper ():
                    if unrealized_profit >= (lot_multi_factor * 5000):
                        try:
                            target = round ((lot_multi_factor * 1000) * math.floor ((unrealized_profit - (lot_multi_factor * 4000)) / (lot_multi_factor * 1000)), 0)
                        except Exception:
                            logger.error (traceback.format_exc () + str(order) + str(unrealized_profit) + str(lot_multi_factor) + str(stocks_live_data[formed_order['nse_symbol']]))
                            continue

                    elif ((datetime.now(indian_timezone) - parent_order['exchange_timestamp'].replace(tzinfo=indian_timezone)).seconds - 1380) >= 300 and unrealized_profit > 0:
                        target = break_even
                    else:
                        continue

                    points_change = round (util.min_tick_size * round ((target / parent_order['filled_quantity']) / util.min_tick_size), 2)

                    points_change = [points_change, util.min_tick_size][abs(points_change) < util.min_tick_size]

                    transaction_type = parent_order['transaction_type'].upper ()

                    changed_price = util.round_to_tick(parent_order['average_price'] + [-points_change, points_change][transaction_type == 'BUY'])

                    if ( transaction_type == 'BUY' and order['trigger_price'] >= changed_price) or (transaction_type == 'SELL' and order['trigger_price'] <= changed_price):
                        continue

                    kite.modify_order(kite.VARIETY_CO, order['order_id'],parent_order_id=order['parent_order_id'], trigger_price=changed_price)

                    logger.info ('Order id:%s of stock:%s trigger price changed from %f to %f. P/L:%f. Child order:%s. Parent order:%s. Quote:%s.', str (order['order_id']), order['tradingsymbol'], order['trigger_price'], changed_price, unrealized_profit, str(order), str(parent_order), str(stocks_live_data[formed_order['nse_symbol']]))
                else:
                    logger.error('Wrong formed_order:%s fetched for parent_order:%s.', str(formed_order), str(parent_order))

    except Exception:
        logger.error (traceback.format_exc ())
