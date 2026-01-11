import json
import time as sleep_time
import traceback
from datetime import datetime, time
from urllib.request import Request, urlopen

import ScrapUtils as nse_bse
import Utils as util
from Orders import orders
# import time as sleep_time
from bs4 import BeautifulSoup

upstox_api = util.intialize_upstox_api([nse_bse.NSE])

testing = False

START_TIME_FOR_SCRIPT = time(9, 15, 30, 1)
INVESTING_COM_HEADERS = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://in.investing.com'}


def fetch_investing_com_data(live_data_url):
    page = urlopen(Request(live_data_url, headers=INVESTING_COM_HEADERS))
    # parse the html using beautiful soup and store in variable `soup`
    soup = BeautifulSoup(page, 'html.parser')

    live_data = json.loads(soup.string.strip())

    stock_live_data = {'open': util.round_to_tick (live_data['o'][-1]), 'ltp': util.round_to_tick (live_data['c'][-1]),
                       'date': util.get_date_from_timestamp (int (live_data['t'][-1]) * 1000).date ()}

    return stock_live_data


while datetime.now().time() < START_TIME_FOR_SCRIPT and testing is False:
    pass


while True:
    try:
        upstox_orders = upstox_api.get_order_history()

        for upstox_order in upstox_orders:
            if util.is_number(upstox_order['parent_order_id']) is False and upstox_order['status'].upper() == 'TRIGGER PENDING':
                for order in orders:
                    if order['symbol'].upper() == upstox_order['symbol'].upper():
                        break

                stock_data = fetch_investing_com_data(order['live_data_url'])

                price = stock_data['open'] + [-order['reverse_price_pts'], order['reverse_price_pts']][upstox_order['transaction_type'] == 'B']

                if upstox_order['trigger_price'] != stock_data['open']:
                    upstox_api.modify_order(upstox_order['order_id'], trigger_price=stock_data['open'], price=price)

                    print('Changed trigger price of order of symbol:%s and id:%d from %f to %f' % (upstox_order['symbol'], upstox_order['order_id'], upstox_order['trigger_price'], stock_data['open']))

                    print('Changed price of order of symbol:%s and id:%d from %f to %f' % (upstox_order['symbol'], upstox_order['order_id'], upstox_order['price'], price))

        sleep_time.sleep(2.0)

    except Exception:
        print(traceback.format_exc())
