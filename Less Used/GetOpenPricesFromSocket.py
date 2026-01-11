import traceback

import time as sleeptime
from upstox_api.api import LiveFeedType

from Orders import *


def get_symbols_to_subscribe():
    symbols_to_subscribe = []
    for order in orders:
        symbols_to_subscribe.append (order[nse_bse.INSTRUMENT])

    return symbols_to_subscribe


symbols_to_subscribe_first = get_symbols_to_subscribe ()

if len (symbols_to_subscribe_first) > 0:
    # util.unsubscribe_symbols(upstox_api)
    print ('Subscribing %d symbols' % (len (symbols_to_subscribe_first)))

    # upstox_api.subscribe (symbols_to_subscribe_first, LiveFeedType.Full, nse_bse.NSE)

    for symbol_to_subscribe in symbols_to_subscribe_first:
        upstox_api.subscribe (symbol_to_subscribe, LiveFeedType.Full)
        sleeptime.sleep (.5)

    print ('Subscribed %d symbols' % (len (upstox_api.get_subscriptions ()['FULL'])))
else:
    print ('No symbols to subscribe.')
    exit (0)

instrument_latest_data = {}
open_prices = {}

testing = False


def event_handler_quote_update(live_quote):
    try:
        stock_id = live_quote['symbol']

        ltp = float (live_quote['ltp'])
        open = float (live_quote['open'])
        date_from_timestamp = util.get_date_from_timestamp (int (live_quote['timestamp']))

        instrument_latest_data[stock_id] = {'timestamp': date_from_timestamp, 'ltp': ltp, 'open': open,
                                            'high': float (live_quote['high']), 'low': float (live_quote['low']),
                                            'close': float (live_quote['close']), 'vtt': float (live_quote['vtt'])}

        open_prices[stock_id.lower ()] = open
        print ('Open prices:', open_prices)
        print ('Time:', datetime.now ())
        sleeptime.sleep (.5)

    except Exception:
        print (traceback.format_exc () + ' in Stock:' + live_quote)


while util.is_market_open () is False and testing is False:
    pass

upstox_api.set_on_quote_update (event_handler_quote_update)
print ('Starting websocket at:', datetime.now ())
upstox_api.start_websocket (False)
