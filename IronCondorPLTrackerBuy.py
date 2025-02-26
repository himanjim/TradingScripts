import math

import Utils as util
import time as tm
from datetime import datetime
import pytz

if __name__ == '__main__':
    MAX_PROFIT = 8000
    MAX_LOSS = -5000
    sleep_time = 2

    indian_timezone = pytz.timezone('Asia/Calcutta')

    kite = util.intialize_kite_api()

    under_lying_symbol = kite.EXCHANGE_NSE + ':NIFTY BANK'

    positions = kite.positions()
    symbols = []
    sell_price = 0
    for position in positions['day']:
        if position['average_price'] != 0:
            symbols.append(position['exchange'] + ':' + position['tradingsymbol'])
            sell_price += (position['buy_quantity'] * position['buy_price'])

    max_pl = 0
    min_pl = 0

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
            present_value += (live_quote['last_price'] * position['buy_quantity'])

        net_pl = present_value - sell_price

        if net_pl > 0 and net_pl > max_pl:
            max_pl = net_pl

        if net_pl < 0 and net_pl < min_pl:
            min_pl = net_pl

        print(f"Net P/L: {net_pl}. Maximum Profit: {max_pl}. Maximum Loss: {min_pl} at {datetime.now(indian_timezone).time()}.")

        if min_pl < -5000:
            sleep_time = .5
        elif min_pl < -8000:
            sleep_time = .25

        if net_pl >= MAX_PROFIT or net_pl <= MAX_LOSS:

            for position in positions['day']:
                if position['average_price'] != 0:
                    kite.place_order(tradingsymbol=position['tradingsymbol'],
                                     variety=kite.VARIETY_REGULAR,
                                     exchange=position['exchange'],
                                     transaction_type=kite.TRANSACTION_TYPE_SELL,
                                     quantity=position['buy_quantity'],
                                     order_type=kite.ORDER_TYPE_MARKET,
                                     product=position['product'],
                                     )

                    print(f"Position of instrument {position['tradingsymbol']} exited.")

            print(f"All postions exited at P/L {net_pl} at {datetime.now(indian_timezone).time()}")

            underlying_live_data = kite.quote(under_lying_symbol)
            underlying_live_ltp = underlying_live_data[under_lying_symbol]['last_price']

            print(f"Under_lying is {underlying_live_ltp} at {datetime.now(indian_timezone).time()}")

            break

        else:
            tm.sleep(sleep_time)
