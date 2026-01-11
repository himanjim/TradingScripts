import os
import pickle
import traceback

import time

import ScrapUtils as nse_bse
import Utils as util

LATEST_M2M_FILE = 'm2m_file'
upstox_api = util.intialize_upstox_api ([nse_bse.NSE])

m2m = {}
if os.path.exists (LATEST_M2M_FILE) is False:
    pickle.dump (m2m, open (LATEST_M2M_FILE, 'wb+'))
else:
    m2m = pickle.load (open (LATEST_M2M_FILE, 'rb'))

counter = 0

while True:
    try:
        if counter % 5 == 0:
            orders = upstox_api.get_order_history ()

        positions = upstox_api.get_positions ()
        if positions is not None and len (positions) > 0:

            for position in positions:
                if util.is_number (position['unrealized_profit']) is False:
                    continue

                unrealized_profit = float (position['unrealized_profit'])

                if position['symbol'] not in m2m:
                    if unrealized_profit >= 2000:
                        m2m[position['symbol']] = unrealized_profit

                elif position['symbol'] in m2m:
                    if unrealized_profit < 1000:
                        for order in orders:
                            if order['parent_order_id'] != 'NA' and order['status'].upper () == 'OPEN' and order[
                                'symbol'].upper () == position['symbol'].upper ():
                                # upstox_api.cancel_order (order['order_id'])
                                print ('Order id:%s cancelled of stock:%s at p/l:%s and max m2m:%s.' % (
                                str (order['order_id']), position['symbol'], str (unrealized_profit),
                                str (m2m[position['symbol']])))

                    elif unrealized_profit > m2m[position['symbol']]:
                        m2m[position['symbol']] = unrealized_profit

        pickle.dump (m2m, open (LATEST_M2M_FILE, 'wb'))
        counter += 1

        time.sleep (1.5)

    except Exception:
        print (traceback.format_exc ())
