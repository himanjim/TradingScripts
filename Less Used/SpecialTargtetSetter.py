import traceback
from datetime import datetime, time

import Utils as util

kite = util.intialize_kite_api()

testing = False

special_target = 6000
special_target_stock_id = 'BHEL'

START_TIME_FOR_BREAKEVEN = time (9, 15, 40, 1)

while datetime.now ().time () < util.MARKET_START_TIME and testing is False:
    pass

parent_orders = {}
while True:
    try:
        orders = kite.orders()
        positions = kite.positions()['day']

        for order in orders:
            if order['parent_order_id'] is None and order['tradingsymbol'].upper () == special_target_stock_id and (order['status'].upper() == 'COMPLETE' or order['status'].upper() == 'OPEN'):
                parent_orders[order['order_id']] = order

        if len (parent_orders) == 0:
            continue

        for order in orders:

            order_position = None
            for position in positions:
                if order['tradingsymbol'].upper () == position['tradingsymbol'].upper ():
                    order_position = position
                    break

            if order['tradingsymbol'].upper () == special_target_stock_id and order['parent_order_id'] is not None and order['status'].upper () == 'TRIGGER PENDING':
                parent_order = parent_orders[order['parent_order_id']]

                points_change = round (util.min_tick_size * round (
                    (special_target / parent_order['filled_quantity']) / util.min_tick_size), 2)
                points_change = [points_change, util.min_tick_size][abs(points_change) < util.min_tick_size]

                changed_price = parent_order['average_price'] + [-points_change, points_change][
                    parent_order['transaction_type'].upper () == 'BUY']

                changed_price = util.round_to_tick(changed_price)

                kite.modify_order(kite.VARIETY_CO, order['order_id'], parent_order_id=order['parent_order_id'],trigger_price=changed_price)

                print ('Order id:%s of stock:%s trigger price changed from %f to %f at time:%s.' % (
                    str (order['order_id']), order['tradingsymbol'], order['trigger_price'], changed_price,
                    str (datetime.now ())))

        exit (0)

    except Exception:
        print (traceback.format_exc ())
