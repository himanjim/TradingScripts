import Utils as util
import time as tm
from datetime import datetime
import pytz

if __name__ == '__main__':
    MAX_PROFIT = 10000
    MAX_LOSS = -10000
    indian_timezone = pytz.timezone('Asia/Calcutta')

    kite = util.intialize_kite_api()

    positions = kite.positions()
    symbols = []
    sell_price = 0
    for position in positions['day']:
        symbols.append(position['exchange'] + ':' + position['tradingsymbol'])
        sell_price += (position['sell_quantity'] * position['sell_price'])

    while True:

        live_quotes = kite.quote(symbols)

        present_value = 0

        for trading_symbol, live_quote in live_quotes.items():
            present_value += (live_quote['last_price'] * position['sell_quantity'])

        net_pl = sell_price - present_value
        print('Net P/L:', net_pl)

        if net_pl >= MAX_PROFIT or net_pl <= MAX_LOSS:
        # if net_pl >= MAX_PROFIT:

            orders = kite.orders()

            for position in positions['day']:
                kite.place_order(tradingsymbol=position['tradingsymbol'],
                                 variety=kite.VARIETY_REGULAR,
                                 exchange=kite.EXCHANGE_NFO,
                                 transaction_type=kite.TRANSACTION_TYPE_BUY,
                                 quantity=position['sell_quantity'],
                                 order_type=kite.ORDER_TYPE_MARKET,
                                 product=kite.PRODUCT_MIS,

                                 )
                print(f"Position of instrument {position['tradingsymbol']} exited.")

            print(f"All orders exited at P/L {net_pl} at {datetime.now(indian_timezone).time()}")

            break

        else:
            tm.sleep(1)