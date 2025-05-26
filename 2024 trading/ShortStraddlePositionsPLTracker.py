import time as tm
from datetime import datetime
import pytz
import OptionTradeUtils as oUtils
import pandas as pd
import winsound  # Use only on Windows


def exit_trade(_position):
    kite.place_order(tradingsymbol=_position['tradingsymbol'],
                     variety=kite.VARIETY_REGULAR,
                     exchange=_position['exchange'],
                     transaction_type=kite.TRANSACTION_TYPE_BUY if _position[
                                                                        'type'] == kite.TRANSACTION_TYPE_SELL else kite.TRANSACTION_TYPE_SELL,
                     quantity=_position['quantity'],
                     order_type=kite.ORDER_TYPE_MARKET,
                     product=_position['product'],
                     )

def get_positions_from_orders(kite_):
    orders = kite_.orders()
    # Create pandas DataFrame from the list of orders
    df = pd.DataFrame(orders)
    _all_positions = []
    # Iterate over each row in the filtered DataFrame
    for index, row in df.iterrows():
        if row['product'] in ('NRML', 'MIS') and row['variety'] in ('regular'):
            _all_positions.append(
                {'exchange': row['exchange'], 'tradingsymbol': row['tradingsymbol'], 'quantity': row['quantity'],
                 'price': row['average_price'], 'product': row['product'], 'type': row['transaction_type']})

    return _all_positions

def any_active_positions(kite_):
    positions_live = kite_.positions()

    return all(
        item['average_price'] == 0
        for item in positions_live['day']
        if item['product'] in ('NRML', 'MIS')
    )


if __name__ == '__main__':
    MAX_PROFIT = 10000
    MAX_LOSS = -3000
    MAX_PROFIT_EROSION = 10000
    sleep_time = 2
    max_profit_set = None
    # second_trade_execute = False

    indian_timezone = pytz.timezone('Asia/Calcutta')

    kite = oUtils.intialize_kite_api()
    UNDER_LYING_EXCHANGE, UNDERLYING, OPTIONS_EXCHANGE, PART_SYMBOL, NO_OF_LOTS, STRIKE_MULTIPLE = oUtils.get_instruments(kite)
    PART_SYMBOL = PART_SYMBOL.replace(':', '')
    under_lying_symbol = UNDER_LYING_EXCHANGE + UNDERLYING

    # print(kite.positions())
    #
    # exit(0)

    # positions = kite.positions()

#     positions = [{'exchange': 'NFO', 'tradingsymbol': 'BANKNIFTY25APR55300PE', 'quantity': 120, 'price': 82.525, 'product': 'NRML', 'type': 'SELL'},
# {'exchange': 'NFO', 'tradingsymbol': 'BANKNIFTY25APR55300CE', 'quantity': 120, 'price': 148.6875, 'product': 'NRML', 'type': 'SELL'}]
#
#     [{'exchange': 'NFO', 'tradingsymbol': 'BANKNIFTY25APR55300PE', 'quantity': 120, 'price': 82.525, 'product': 'NRML', 'type': 'SELL'}, {'exchange': 'NFO', 'tradingsymbol': 'BANKNIFTY25APR55300CE', 'quantity': 120, 'price': 148.6875, 'product': 'NRML', 'type': 'SELL'}]

    all_positions = get_positions_from_orders(kite)
    positions = all_positions[-2:]

    print(positions)

#     positions = [{'exchange': 'NFO', 'tradingsymbol': 'NIFTY2551524600PE', 'quantity': 300, 'price': 100.2125, 'product': 'NRML', 'type': 'SELL'},
# {'exchange': 'NFO', 'tradingsymbol': 'NIFTY2551524600CE', 'quantity': 300, 'price': 103.775, 'product': 'NRML', 'type': 'SELL'},]

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

            if any_active_positions(kite):
                print("No active positions.")
                break

            live_quotes = kite.quote(symbols)


            net_pl = 0

            for position in positions:
                if position['type'] == kite.TRANSACTION_TYPE_SELL:
                    position ['pl'] =  ((position['price'] - live_quotes[position['exchange'] + ':' + position['tradingsymbol']]['last_price']) * position['quantity'])
                else:
                    position['pl'] = ((live_quotes[position['exchange'] + ':' + position['tradingsymbol']]['last_price'] -
                                position['price']) * position['quantity'])

                net_pl += position['pl']

            if net_pl > 0 and net_pl > max_pl:
                max_pl = net_pl

            if net_pl < 0 and net_pl < min_pl:
                min_pl = net_pl

            if max_pl > 5000:
                MAX_LOSS = 0
                MAX_PROFIT_EROSION = 5000
            elif max_pl > 10000:
                MAX_LOSS = 7000
                MAX_PROFIT_EROSION = 10000

            print(f"Net P/L: {net_pl}. Maximum Profit: {max_pl}. Maximum Loss: {min_pl} at {datetime.now(indian_timezone).time()}.")

            if min_pl < (MAX_LOSS * .5):
                sleep_time = .5
            elif min_pl < (MAX_LOSS * .8):
                sleep_time = .25

            if max_profit_set and max_profit_set > max_pl:
                max_pl = max_profit_set

            if net_pl >= MAX_PROFIT:

                for position in positions:
                    # if position['pl'] < 0 and position['price'] != 0:
                    exit_trade(position)
                    print(f"Position of instrument {position['tradingsymbol']} exited at p/l {position['pl']} at {datetime.now(indian_timezone).time()}.")
                winsound.Beep(2000, 2000)

                break

            elif net_pl <= MAX_LOSS or (max_pl - net_pl) > MAX_PROFIT_EROSION:

                for position in positions:
                    # if position['pl'] < 0 and position['price'] != 0:
                    exit_trade(position)
                    print(f"Position of instrument {position['tradingsymbol']} exited at p/l {position['pl']} at {datetime.now(indian_timezone).time()}.")
                winsound.Beep(2000, 2000)

                # # Code of 2nd trade after loss
                # if second_trade_execute and net_pl <= MAX_LOSS:
                #     under_lying_symbol = UNDER_LYING_EXCHANGE + UNDERLYING
                #     ul_live_quote = kite.quote(under_lying_symbol)
                #
                #     ul_ltp = ul_live_quote[under_lying_symbol]['last_price']
                #
                #     # nifty_ltp_round_50 = round(nifty_ltp / 50) * 50
                #     ul_ltp_round = round(ul_ltp / STRIKE_MULTIPLE) * STRIKE_MULTIPLE
                #     for position in positions:
                #         if position['pl'] > 0 and position['price'] != 0:
                #             if 'CE' in position['tradingsymbol']:
                #                 option_symbol = PART_SYMBOL + str(ul_ltp_round) + 'CE'
                #             else:
                #                 option_symbol = PART_SYMBOL + str(ul_ltp_round) + 'PE'
                #
                #             kite.place_order(tradingsymbol=option_symbol,
                #                              variety=kite.VARIETY_REGULAR,
                #                              exchange=position['exchange'],
                #                              transaction_type=kite.TRANSACTION_TYPE_SELL,
                #                              quantity=position['quantity'],
                #                              order_type=kite.ORDER_TYPE_MARKET,
                #                              product=position['product'],
                #                              )
                #             print(
                #                 f"2nd order placed of instrument {option_symbol} at {datetime.now(indian_timezone).time()}.")
                #
                #             tm.sleep(1)
                #             if any_active_positions(kite):
                #                 print("No active positions. Place order manually")
                #                 break
                #
                #             all_positions = get_positions_from_orders(kite)
                #             last_position = all_positions[-1:][0]
                #
                #             if last_position and last_position['tradingsymbol'] == option_symbol and last_position['price'] != 0:
                #                 kite.place_order(tradingsymbol=option_symbol,
                #                                  variety=kite.VARIETY_REGULAR,
                #                                  exchange=position['exchange'],
                #                                  transaction_type=kite.TRANSACTION_TYPE_BUY,
                #                                  quantity=position['quantity'],
                #                                  order_type=kite.ORDER_TYPE_SL,
                #                                  product=position['product'],
                #                                  price= last_position['price'] + 31,
                #                                  trigger_price = last_position['price'] + 30
                #                                  )
                #
                #             break
                # # Code of 2nd trade after loss

                break

            else:
                tm.sleep(sleep_time)

        except Exception as e:
            # This will catch any exception and print the error message
            print(f"An error occurred: {e}")
            tm.sleep(2)
            continue
