import time as tm
import datetime as dt
from datetime import datetime
import pytz
from kiteconnect import KiteConnect

KITE_API_KEY = '453dipfh64qcl484'
KITE_ACCESS_CODE = 'acXxhbUVaqEVPTCUXHeKrSwGcIpytYVE'
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


def exit_trade(_position):
    kite.place_order(tradingsymbol=_position['tradingsymbol'],
                     variety=kite.VARIETY_REGULAR,
                     exchange=_position['exchange'],
                     transaction_type=[kite.TRANSACTION_TYPE_BUY if _position[
                                                                        'type'] is kite.TRANSACTION_TYPE_SELL else kite.TRANSACTION_TYPE_SELL],
                     quantity=_position['quantity'],
                     order_type=kite.ORDER_TYPE_MARKET,
                     product=_position['product'],
                     )


if __name__ == '__main__':
    MAX_PROFIT = 20000
    MAX_LOSS = -5000
    MAX_PROFIT_EROSION = 8000
    sleep_time = 2
    max_profit_set = None

    indian_timezone = pytz.timezone('Asia/Calcutta')

    kite = intialize_kite_api()

    # print(kite.positions())
    #
    # exit(0)

    # positions = kite.positions()

    positions = [{'exchange': 'NFO', 'tradingsymbol': 'NIFTY2520623750PE', 'quantity': 300, 'price': 97.05, 'product': 'MIS', 'type': 'SELL'},
{'exchange': 'NFO', 'tradingsymbol': 'NIFTY2520623750CE', 'quantity': 300, 'price': 80.75, 'product': 'MIS', 'type': 'SELL'}]

    symbols = []
    for position in positions:
        if position['price'] != 0:
            symbols.append(position['exchange'] + ':' + position['tradingsymbol'])

    max_pl = 0
    min_pl = 0

    if len(symbols) == 0:
        print(f"No active position. Hence exiting.")
        exit(0)

    while True:
        try:

            if datetime.now(indian_timezone).time() > MARKET_END_TIME:
                print(f"Market is closed. Hence exiting.")
                exit(0)

            live_quotes = kite.quote(symbols)

            net_pl = 0

            for position in positions:
                if position['type'] is kite.TRANSACTION_TYPE_SELL:
                    position ['pl'] =  ((position['price'] - live_quotes[position['exchange'] + ':' + position['tradingsymbol']]['last_price']) * position['quantity'])
                else:
                    position['pl'] = ((live_quotes[position['exchange'] + ':' + position['tradingsymbol']]['last_price'] -
                                position['price']) * position['quantity'])

                net_pl += position['pl']

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
                max_pl = max_profit_set

            if net_pl >= MAX_PROFIT:

                for position in positions:
                    if position['price'] != 0:
                        exit_trade(position)
                        print(f"Position of instrument {position['tradingsymbol']} exited at p/l {position['pl']} at {datetime.now(indian_timezone).time()}.")
                break
            elif net_pl <= MAX_LOSS or (max_pl - net_pl) > MAX_PROFIT_EROSION:

                for position in positions:
                    if position['price'] != 0 and position['pl'] < 0:
                        exit_trade(position)
                        print(f"Position of instrument {position['tradingsymbol']} exited at p/l {position['pl']} at {datetime.now(indian_timezone).time()}.")

                break

            else:
                tm.sleep(sleep_time)

        except Exception as e:
            # This will catch any exception and print the error message
            print(f"An error occurred: {e}")
            tm.sleep(2)
            continue
