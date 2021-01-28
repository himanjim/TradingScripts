import traceback
from datetime import datetime, time
import face_recognition
import time as sleep_time

import ScrapUtils as nse_bse
import Utils as util

upstox_api = util.intialize_upstox_api ([nse_bse.NSE])

testing = False

START_TIME_FOR_SCRIPT = time (9, 15, 0, 1)
max_m2m = 0.0
max_loss_to_bear = -1500

while datetime.now ().time () < START_TIME_FOR_SCRIPT and testing is False:
    pass

print ('Starting at:', datetime.now ())

while True:
    try:
        orders = upstox_api.get_order_history ()
        positions = upstox_api.get_positions()

        for order in orders:
            order_position = None
            for position in positions:
                if order['symbol'].upper() == position['symbol'].upper():
                    order_position = position
                    break

            if order_position is None:
                continue

            if util.is_number(order['parent_order_id']) is False and util.is_number (order_position['unrealized_profit']):

                unrealized_profit = float (order_position['unrealized_profit'])

                if unrealized_profit < max_loss_to_bear or (unrealized_profit > 0 and unrealized_profit < (.75 * max_m2m)):
                    upstox_api.cancel_order(order['order_id'])

                if unrealized_profit > max_m2m:
                    print ('M2m changed from:%f to :%f' %(max_m2m, unrealized_profit))
                    max_m2m = unrealized_profit

        sleep_time.sleep (2.0)

    except Exception:
        print (traceback.format_exc ())
