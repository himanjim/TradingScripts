import os
from datetime import datetime

import GenericStatPrinter as gstats
import Utils as util
from NextAttemptOrderPlacerUtils import min_lot_percent_of_future_lot
from Orders import orders as formed_orders

max_loss_to_bear = -1000

kite = util.intialize_kite_api()

trades = [['TIME', 'SYMBOL', 'SELL/BUY', 'BUY PRICE', 'SELL PRICE', 'QUANTITY', 'FUT. LOT', 'TGR PT.', 'MAX HIGH PRICE', 'MIN LOW PRICE', 'P/L', 'TAG']]
detailed_orders = [['TYPE', 'SYMBOL', 'ID', 'P. TIME', 'E. TIME', 'SELL/BUY', 'QUANTITY', 'TRIGGER PRICE', 'PRICE', 'AVERAGE PRICE', 'TAG']]

today_date = datetime.now ().strftime ('%Y-%m-%d')
trades_excel_location = ['G:', 'F:'][os.path.exists('F:')] + '/Trading_Responses/Trades_' + str (today_date) + '.xlsx'
detailed_orders_excel_location = ['G:', 'F:'][os.path.exists('F:')] + '/Trading_Responses/Detailed_orders_' + str (today_date) + '.xlsx'

stocks_to_trade = []
for formed_order in formed_orders:
    stocks_to_trade.append (formed_order['nse_symbol'])

if len(stocks_to_trade) > 0:
    stocks_live_data = kite.quote(stocks_to_trade)

broker_orders = kite.orders ()

for parent_order in broker_orders:
    if parent_order['parent_order_id'] is None and parent_order['status'].upper () == 'CANCELLED' and (parent_order['average_price'] is None or parent_order['average_price'] == 0):
        detailed_orders.append (
            ['PARENT', parent_order['tradingsymbol'], str (parent_order['order_id']), parent_order['exchange_timestamp'], parent_order['order_timestamp'], parent_order['transaction_type'], parent_order['quantity'], parent_order['trigger_price'], parent_order['price'], parent_order['average_price'], parent_order['tag']])
        detailed_orders.append (['-', parent_order['tradingsymbol'], '-', '-', '-', '-', '-', '-', '-', '-'])
        continue

    if parent_order['parent_order_id'] is None and (parent_order['status'].upper () == 'COMPLETE' or parent_order['status'].upper () == 'CANCELLED') and (parent_order['average_price'] is not None and parent_order['average_price'] != 0):
        detailed_orders.append(['PARENT', parent_order['tradingsymbol'], str(parent_order['order_id']), parent_order['exchange_timestamp'], parent_order['order_timestamp'], parent_order['transaction_type'], parent_order['filled_quantity'], parent_order['trigger_price'], parent_order['price'], parent_order['average_price'], parent_order['tag']])

        total_price = 0
        total_quantity = 0

        for child_order in broker_orders:
            if child_order['parent_order_id'] is not None and child_order['parent_order_id'] == parent_order['order_id'] and child_order['status'].upper () == 'COMPLETE':
                detailed_orders.append (['CHILD', child_order['tradingsymbol'], str(child_order['order_id']), child_order['exchange_timestamp'], child_order['order_timestamp'], child_order['transaction_type'], child_order['filled_quantity'], child_order['trigger_price'], child_order['price'], child_order['average_price']])
                total_price += (child_order['filled_quantity'] * child_order['average_price'])
                total_quantity += child_order['filled_quantity']

        if total_quantity == 0:
            print('0 total quantity for parent order:%s' %(str(parent_order)))
            continue

        buy_price = [round(total_price / total_quantity, 2), parent_order['average_price']][parent_order['transaction_type'] == 'BUY' ]
        sell_price = [round (total_price / total_quantity, 2), parent_order['average_price']][parent_order['transaction_type'] == 'SELL']

        formed_order_exits = False

        for formed_order in formed_orders:
            if formed_order['symbol'].upper () == parent_order['tradingsymbol'].upper ():
                formed_order_exits = True
                break

        profit_loss = round ((sell_price - buy_price) * abs (parent_order['filled_quantity']), 2)

        detailed_orders.append(['-', parent_order['tradingsymbol'], '-', '-', '-', '-', '-', '-', '-', profit_loss])

        if formed_order_exits:

            max_high_price = stocks_live_data[formed_order['nse_symbol']]['ohlc']['open'] - formed_order['trigger_price_pts'] + (abs(max_loss_to_bear) / (min_lot_percent_of_future_lot * formed_order['future_lot']))
            min_low_price = stocks_live_data[formed_order['nse_symbol']]['ohlc']['open'] + formed_order['trigger_price_pts'] - (abs(max_loss_to_bear) / (min_lot_percent_of_future_lot * formed_order['future_lot']))

            trades.append ([parent_order['exchange_timestamp'], parent_order['tradingsymbol'].upper (), parent_order['transaction_type'], buy_price, sell_price, abs (parent_order['filled_quantity']), formed_order['future_lot'], formed_order['trigger_price_pts'],  max_high_price, min_low_price, profit_loss, parent_order['tag']])

        else:
            nse_stock_id = 'NSE:' + parent_order['tradingsymbol'].upper()
            open_price = kite.quote(nse_stock_id)[nse_stock_id]['ohlc']['open']

            trades.append([parent_order['exchange_timestamp'], parent_order['tradingsymbol'].upper(), parent_order['transaction_type'], buy_price, sell_price, abs(parent_order['filled_quantity']), int(500000 / open_price), None, None, None, profit_loss, parent_order['tag']])

positions = kite.positions ()

for formed_order in formed_orders:
    position_exist = False
    for position in positions['day']:
        if formed_order['symbol'].upper() == position['tradingsymbol'].upper():
            position_exist = True
    if position_exist is False:
        max_high_price = stocks_live_data[formed_order['nse_symbol']]['ohlc']['open'] - formed_order[
            'trigger_price_pts'] + (abs (max_loss_to_bear) / (
                    min_lot_percent_of_future_lot * formed_order['future_lot']))
        min_low_price = stocks_live_data[formed_order['nse_symbol']]['ohlc']['open'] + formed_order[
            'trigger_price_pts'] - (abs (max_loss_to_bear) / (
                    min_lot_percent_of_future_lot * formed_order['future_lot']))
        trades.append(
            [today_date, formed_order['symbol'].upper(), 'NA', 0, 0, 0, formed_order['future_lot'], formed_order['trigger_price_pts'], max_high_price, min_low_price, 0])

gstats.print_statistics (trades, trades_excel_location)

# detailed_orders.sort(key = lambda x: x[3])
gstats.print_statistics (detailed_orders, detailed_orders_excel_location)
