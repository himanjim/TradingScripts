import time as tm
import datetime as dt
from datetime import datetime
import pytz
from kiteconnect import KiteConnect

KITE_API_KEY = '453dipfh64qcl484'
KITE_ACCESS_CODE = 'G2wk8dtjk8LaeCNGcc6SnXjy1i4mmtDv'
MARKET_START_TIME = dt.time (9, 15, 0, 100)
MARKET_END_TIME = dt.time (15, 25, 0)
TRADE_START_TIME = dt.time (9, 15, 30)


def intialize_kite_api():
    kite = KiteConnect (api_key=KITE_API_KEY)

    try:

        kite.set_access_token(KITE_ACCESS_CODE)
    except Exception as e:
        print("Authentication failed", str(e))
        raise

    return kite


if __name__ == '__main__':
    MAX_PROFIT = 40000
    MAX_LOSS = -4000
    MAX_PROFIT_EROSION = 9000
    sleep_time = 2
    max_profit_set = None

    indian_timezone = pytz.timezone('Asia/Calcutta')

    kite = intialize_kite_api()

    # print(kite.positions())
    #
    # exit(0)

    # positions = kite.positions()

    positions = [{'exchange': 'NFO', 'tradingsymbol': 'NIFTY2510223400PE', 'quantity': 300, 'price': 44.1, 'product': 'MIS', 'type': 'BUY'}, {'exchange': 'NFO', 'tradingsymbol': 'NIFTY2510223600PE', 'quantity': 300, 'price': 110.6, 'product': 'MIS', 'type': 'SELL'}]

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

        if datetime.now(indian_timezone).time() > MARKET_END_TIME:
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
                                     transaction_type=[kite.TRANSACTION_TYPE_BUY if position['type'] is kite.TRANSACTION_TYPE_SELL else kite.TRANSACTION_TYPE_SELL],
                                     quantity=position['quantity'],
                                     order_type=kite.ORDER_TYPE_MARKET,
                                     product=position['product'],
                                     )

                    print(f"Position of instrument {position['tradingsymbol']} exited.")

            print(f"All postions exited at P/L {net_pl} at {datetime.now(indian_timezone).time()}")

            break

        else:
            tm.sleep(sleep_time)
