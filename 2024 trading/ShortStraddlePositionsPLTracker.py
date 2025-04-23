import time as tm
from datetime import datetime
import pytz
import OptionTradeUtils as oUtils
import re
import pandas as pd


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
    MAX_PROFIT = 10000
    MAX_LOSS = -5000
    MAX_PROFIT_EROSION = 5000
    sleep_time = 2
    max_profit_set = 4197

    second_trade_executed =  True

    indian_timezone = pytz.timezone('Asia/Calcutta')

    kite = oUtils.intialize_kite_api()

    # print(kite.positions())
    #
    # exit(0)

    # positions = kite.positions()

#     positions = [{'exchange': 'NFO', 'tradingsymbol': 'BANKNIFTY25APR55300PE', 'quantity': 120, 'price': 199.025, 'product': 'NRML', 'type': 'SELL'},
# {'exchange': 'NFO', 'tradingsymbol': 'BANKNIFTY25APR55300CE', 'quantity': 120, 'price': 173.45, 'product': 'NRML', 'type': 'SELL'}]

    orders = kite.orders()
    # Create pandas DataFrame from the list of orders
    df = pd.DataFrame(orders)
    positions = []
    # Iterate over each row in the filtered DataFrame
    for index, row in df.iterrows():
        positions.append(
            {'exchange': row['exchange'], 'tradingsymbol': row['tradingsymbol'], 'quantity': row['quantity'],
             'price': row['average_price'], 'product': row['product'], 'type': row['transaction_type']})
    positions = positions[-2:]

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

            if datetime.now(indian_timezone).time() > oUtils.MARKET_END_TIME:
                print(f"Market is closed. Hence exiting.")
                exit(0)

            live_quotes = kite.quote(symbols)

            positions_live = kite.positions()

            all_positions_closed = all(item['average_price'] == 0 for item in positions_live['day'])

            if all_positions_closed:
                print("No active positions.")
                break

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
                    if position['pl'] < 0 and position['price'] != 0:
                        exit_trade(position)
                        print(f"Position of instrument {position['tradingsymbol']} exited at p/l {position['pl']} at {datetime.now(indian_timezone).time()}.")

                break

            elif net_pl <= MAX_LOSS or (max_pl - net_pl) > MAX_PROFIT_EROSION:

                for position in positions:
                    # if position['pl'] < 0 and position['price'] != 0:
                    exit_trade(position)
                    print(f"Position of instrument {position['tradingsymbol']} exited at p/l {position['pl']} at {datetime.now(indian_timezone).time()}.")

                if all(item['pl'] < 0 for item in positions):
                    print(f"Both positions are in loss. Hence not initiating any directional position at {datetime.now(indian_timezone).time()}.")
                    break

                if second_trade_executed is False:
                    for position in positions:
                        if position['pl'] < 0:
                            ul_ltp_round = oUtils.get_underlying_value(kite, position)

                            modified_symbol = re.sub(r'(\d{5})(?=CE|PE)', str(ul_ltp_round), position['tradingsymbol'])
                            modified_symbol = modified_symbol.replace('CE', 'TEMP').replace('PE', 'CE').replace('TEMP', 'PE')
                            # modified_symbol = position['exchange'] + ':' + modified_symbol

                            kite.place_order(tradingsymbol=modified_symbol,
                                             variety=kite.VARIETY_REGULAR,
                                             exchange=position['exchange'],
                                             transaction_type=kite.TRANSACTION_TYPE_SELL,
                                             quantity=position['quantity'],
                                             order_type=kite.ORDER_TYPE_MARKET,
                                             product=position['product'],
                                             )

                            print(f"Placed second order of symbol {modified_symbol} at {datetime.now(indian_timezone).time()}.")

                            orders = kite.orders()

                            # Create pandas DataFrame from the list of orders
                            orders_df = pd.DataFrame(orders)

                            orders_df_row = orders_df.iloc[[-1]]
                            print(
                                f"Fetched second order of symbol {orders_df_row} at {datetime.now(indian_timezone).time()}.")

                            positions = [{'exchange': orders_df_row['exchange'], 'tradingsymbol': orders_df_row['tradingsymbol'], 'quantity': orders_df_row['quantity'], 'price': orders_df_row['average_price'],          'product': orders_df_row['product'], 'type': orders_df_row['transaction_type']}]

                            symbols = [modified_symbol]

                            MAX_PROFIT = 10000
                            MAX_LOSS = -2000
                            MAX_PROFIT_EROSION = 5000

                            second_trade_executed = True
                else:
                    break

            else:
                tm.sleep(sleep_time)

        except Exception as e:
            # This will catch any exception and print the error message
            print(f"An error occurred: {e}")
            tm.sleep(2)
            continue
