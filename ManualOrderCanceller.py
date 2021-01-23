import traceback

import ScrapUtils as nse_bse
import Utils as util

upstox_api = util.intialize_upstox_api([nse_bse.NSE])

while True:
    try:
        orders = upstox_api.get_order_history()

        for order in orders:
            if 'CANCELLED' not in order['status'].upper():
                input ('Cancel order of symbol:%s and order id:%d.' %(order['symbol'].upper(), order['order_id']))
                upstox_api.cancel_order(order['order_id'])
                print('Cancelled order of symbol:%s and id:%d.' % (order['symbol'], order['order_id']))

    except Exception:
        print(traceback.format_exc())
