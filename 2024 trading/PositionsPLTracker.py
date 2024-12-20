import Utils as util
import time as tm
from datetime import datetime
import pytz

if __name__ == '__main__':
    MAX_PROFIT = 30000
    MAX_LOSS = -5000
    MAX_PROFIT_EROSION = 7000
    sleep_time = 2
    max_profit_set = None

    indian_timezone = pytz.timezone('Asia/Calcutta')

    kite = util.intialize_kite_api()

    # print(kite.positions())
    #
    # exit(0)

    # positions = kite.positions()

    positions = [{'exchange': 'BFO', 'tradingsymbol': 'SENSEX24D2079500CE', 'quantity': 10, 'price': 1.65, 'product': 'MIS', 'type': 'BUY'}, {'exchange': 'BFO', 'tradingsymbol': 'SENSEX24D2079500CE', 'quantity': 10, 'price': 1.5, 'product': 'MIS', 'type': 'BUY'}]

    symbols = []
    traded_value = 0
    for position in positions:
        if position['price'] != 0:
            symbols.append(position['exchange'] + ':' + position['tradingsymbol'])
            traded_value += (position['quantity'] * position['price'])

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
            present_value += (live_quote['last_price'] * position['quantity'])

        if positions[0]['type'] == kite.TRANSACTION_TYPE_BUY:
            net_pl = present_value - traded_value
        else:
            net_pl = traded_value - present_value

        if net_pl > 0 and net_pl > max_pl:
            max_pl = net_pl

        if net_pl < 0 and net_pl < min_pl:
            min_pl = net_pl

        print(f"Net P/L: {net_pl}. Maximum Profit: {max_pl}. Maximum Loss: {min_pl} at {datetime.now(indian_timezone).time()}.")

        if min_pl < (MAX_LOSS * .5):
            sleep_time = .5
        elif min_pl < (MAX_LOSS * .8):
            sleep_time = .25

        if max_profit_set and max_profit_set > max_pl:
            max_pl = max_pl

        if net_pl >= MAX_PROFIT or net_pl <= MAX_LOSS or (max_pl - net_pl) > MAX_PROFIT_EROSION:

            for position in positions:
                if position['price'] != 0:
                    kite.place_order(tradingsymbol=position['tradingsymbol'],
                                     variety=kite.VARIETY_REGULAR,
                                     exchange=position['exchange'],
                                     transaction_type=[kite.TRANSACTION_TYPE_BUY if position['quantity'] is kite.TRANSACTION_TYPE_SELL else kite.TRANSACTION_TYPE_SELL],
                                     quantity=position['quantity'],
                                     order_type=kite.ORDER_TYPE_MARKET,
                                     product=position['product'],
                                     )

                    print(f"Position of instrument {position['tradingsymbol']} exited.")

            print(f"All postions exited at P/L {net_pl} at {datetime.now(indian_timezone).time()}")

            break

        else:
            tm.sleep(sleep_time)
