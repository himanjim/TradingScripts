import math

import Utils as util
import time as tm
from datetime import datetime
import pytz

if __name__ == '__main__':
    MAX_PROFIT = 15000
    MAX_LOSS = -10000
    indian_timezone = pytz.timezone('Asia/Calcutta')

    kite = util.intialize_kite_api()

    positions = kite.positions()
    symbols = []
    sell_price = 0
    for position in positions['day']:
        if position['average_price'] != 0:
            symbols.append(position['exchange'] + ':' + position['tradingsymbol'])
            sell_price += (position['sell_quantity'] * position['sell_price'])

    max_pl = None
    min_pl = None

    if len(symbols) == 0:
        print(f"No active position. Hence exiting.")
        exit(0)

    while True:

        if datetime.now(indian_timezone).time() > util.MARKET_END_TIME:
            print(f"Market is closed. Hence exiting.")
            exit(0)

        live_quotes = kite.quote(symbols)

        present_value = 0

        for trading_symbol, live_quote in live_quotes.items():
            present_value += (live_quote['last_price'] * position['sell_quantity'])

        net_pl = sell_price - present_value

        if max_pl is None:
            max_pl = net_pl
        elif net_pl > max_pl:
            max_pl = net_pl

        if min_pl is None:
            min_pl = net_pl
        elif net_pl < min_pl:
            min_pl = net_pl

        print(f"Net P/L: {net_pl}. Maximum Profit: {max_pl}. Maximum Loss: {min_pl}.")

        if net_pl >= MAX_PROFIT or net_pl <= MAX_LOSS:
            # if net_pl >= MAX_PROFIT:

            orders = kite.orders()

            for position in positions['day']:
                if position['average_price'] != 0:
                    kite.place_order(tradingsymbol=position['tradingsymbol'],
                                     variety=kite.VARIETY_REGULAR,
                                     exchange=kite.EXCHANGE_NFO,
                                     transaction_type=kite.TRANSACTION_TYPE_BUY,
                                     quantity=position['sell_quantity'],
                                     order_type=kite.ORDER_TYPE_MARKET,
                                     product=kite.PRODUCT_MIS,

                                     )
                    print(f"Position of instrument {position['tradingsymbol']} exited.")

            print(f"All postions exited at P/L {net_pl} at {datetime.now(indian_timezone).time()}")

            break

        else:
            tm.sleep(1)
