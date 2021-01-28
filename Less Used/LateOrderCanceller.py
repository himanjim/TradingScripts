import traceback
from datetime import datetime, time

import time as sleep_time

import ScrapUtils as nse_bse
import Utils as util

upstox_api = util.intialize_upstox_api([nse_bse.NSE])

testing = False

START_TIME_FOR_SCRIPT = time(9, 15, 30, 1)

while datetime.now().time() < START_TIME_FOR_SCRIPT and testing is False:
    pass

while True:
    try:
        orders = upstox_api.get_order_history()

        for order in orders:
            if util.is_number(order['parent_order_id']) is False and order['status'].upper() == 'OPEN':
                upstox_api.cancel_order(order['order_id'])
                print('Cancelled order of symbol:%s and id:%d.' % (order['symbol'], order['order_id']))

        sleep_time.sleep(2.0)

    except Exception:
        print(traceback.format_exc())
