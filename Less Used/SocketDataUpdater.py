import os
import pickle
import tempfile
import time as sleeptime
import traceback
from datetime import datetime
from io import BytesIO

from Orders import *
from upstox_api.api import LiveFeedType


def get_symbols_to_subscribe():
    symbols_to_subscribe = []
    for order in orders:
        symbols_to_subscribe.append(order[nse_bse.INSTRUMENT])

    return symbols_to_subscribe


symbols_to_subscribe_first = get_symbols_to_subscribe()

if len (symbols_to_subscribe_first) > 0:
    # util.unsubscribe_symbols(upstox_api)
    print ('Subscribing %d symbols' % (len (symbols_to_subscribe_first)))

    # upstox_api.subscribe (symbols_to_subscribe_first, LiveFeedType.Full, nse_bse.NSE)

    for symbol_to_subscribe in symbols_to_subscribe_first:
        upstox_api.subscribe(symbol_to_subscribe, LiveFeedType.Full)
        sleeptime.sleep (.5)

    print('Subscribed %d symbols' % (len(upstox_api.get_subscriptions()['FULL'])))
else:
    print('No symbols to subscribe.')
    exit(0)

if os.path.exists (util.get_instrument_latest_data_file_name ()):
    os.remove (util.get_instrument_latest_data_file_name ())

temp = tempfile.NamedTemporaryFile(mode='w+b', delete=False)
open(util.INSTRUMENT_LATEST_DATA_FILE, 'w+').close()
with open(util.INSTRUMENT_LATEST_DATA_FILE, 'a') as the_file:
    the_file.write(temp.name)

print('Writing to temp file', temp.name)

instrument_latest_data = {}


def write_to_temp():
    obj_bytes = BytesIO ()
    pickle.dump (instrument_latest_data, obj_bytes)
    temp.seek (0)
    temp.write (obj_bytes.getvalue ())
    temp.flush ()


write_to_temp ()
testing = False
open_prices = {}


def event_handler_quote_update(live_quote):
    try:
        stock_id = live_quote['symbol']

        ltp = float (live_quote['ltp'])
        open = float (live_quote['open'])
        date_from_timestamp = util.get_date_from_timestamp(int(live_quote['timestamp']))

        instrument_latest_data[stock_id] = {'timestamp': date_from_timestamp, 'ltp': ltp, 'open': open,
                                            'high': float(live_quote['high']), 'low': float(live_quote['low']),
                                            'close': float(live_quote['close']), 'vtt': float(live_quote['vtt'])}

        write_to_temp()
        open_prices[stock_id.lower()] = open
        print('Updated at time:%s. \n Open prices:%s' % (datetime.now(), open_prices))
        sleeptime.sleep(1)

    except Exception:
        print(traceback.format_exc() + ' in Stock:' + live_quote)


def event_handler_socket_disconnect(err):
    print("Socket Disconnected", err)
    upstox_api.start_websocket(False)


upstox_api.set_on_quote_update(event_handler_quote_update)
upstox_api.set_on_disconnect(event_handler_socket_disconnect)

while util.is_market_open() is False and testing is False:
    pass

print('Starting websocket at:', datetime.now())
upstox_api.start_websocket(False)
