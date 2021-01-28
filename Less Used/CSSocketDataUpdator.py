import pickle
import tempfile
import traceback
from datetime import datetime
from io import BytesIO

import DerivativeUtils as d_util
import ScrapUtils as nse_bse
import Utils as util
from dateutil.relativedelta import relativedelta
from upstox_api.api import *

upstox_api = util.intialize_upstox_api([nse_bse.NSE_FO, nse_bse.NSE])
# upstox_api = None

# futures = nse_bse.get_all_nse_stocks_ids ()
futures = nse_bse.get_nse_fo_stocks()
# futures = nse_bse.get_indices ()
# futures = nse_bse.get_nifty50_stocks_latest_info ()
# futures = nse_bse.get_nifty100_stocks_latest_info ()
futures = [{nse_bse.STOCK_ID: 'IBULHSGFIN', nse_bse.EXCHANGE: nse_bse.NSE_FO}]

fetched_futures = {}
symbols_to_subscribe = []

today_date = datetime.today().date()
current_month_last_thurs = d_util.get_last_thurday_of_month(today_date.month, today_date.year)
near_month_last_thurs = current_month_last_thurs + relativedelta(months=+1)

for future in futures:
    stock_id = d_util.get_stock_id(future[nse_bse.STOCK_ID])

    future_cur_mon_symbol = d_util.get_future_symbol(stock_id, current_month_last_thurs)
    future_near_mon_symbol = d_util.get_future_symbol(stock_id, near_month_last_thurs)

    near_future = upstox_api.get_instrument_by_symbol(nse_bse.NSE_FO, future_near_mon_symbol)
    curr_future = upstox_api.get_instrument_by_symbol(nse_bse.NSE_FO, future_cur_mon_symbol)

    if near_future is not None and curr_future is not None:
        symbols_to_subscribe.append(curr_future)
        symbols_to_subscribe.append(near_future)

if len(symbols_to_subscribe) > 0:
    util.unsubscribe_symbols(upstox_api)

    upstox_api.subscribe(symbols_to_subscribe, LiveFeedType.Full, nse_bse.NSE_FO)
else:
    print('No symbols to subscribe.')
    exit(0)

if os.path.exists(util.get_instrument_latest_data_file_name()):
    os.remove(util.get_instrument_latest_data_file_name())

temp = tempfile.NamedTemporaryFile(mode='w+b', delete=False)
open(util.INSTRUMENT_LATEST_DATA_FILE, 'w+').close()
with open(util.INSTRUMENT_LATEST_DATA_FILE, 'a') as the_file:
    the_file.write(temp.name)

print('Writing to temp file', temp.name)

instrument_latest_data = {}


def write_to_temp():
    obj_bytes = BytesIO()
    pickle.dump(instrument_latest_data, obj_bytes)
    temp.seek(0)
    temp.write(obj_bytes.getvalue())
    temp.flush()


write_to_temp()

testing = True


def event_handler_quote_update(live_quote):
    try:
        stock = live_quote['symbol']

        ltp = float(live_quote['ltp'])
        open_price = float (live_quote['open'])
        date_from_timestamp = util.get_date_from_timestamp(int(live_quote['timestamp']))

        instrument_latest_data[stock] = {'timestamp': date_from_timestamp, 'ltp': ltp, 'open': open_price,
                                         'high': float (live_quote['high']), 'low': float (live_quote['low']),
                                         'close': float (live_quote['close']), 'vtt': float (live_quote['vtt']),
                                         'bids': live_quote['bids'], 'asks': live_quote['asks']}

        write_to_temp()

    except Exception:
        print(traceback.format_exc() + ' in Stock:' + live_quote)


while util.is_market_open() is False and testing is False:
    pass

upstox_api.set_on_quote_update(event_handler_quote_update)
print('Starting websocket at:', datetime.now())
upstox_api.start_websocket(False)
