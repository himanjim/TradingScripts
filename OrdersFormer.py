import csv
import locale
from datetime import datetime

import math
import time
from pytz import timezone

import ScrapUtils as nse_bse
import Utils as util

indian_timezone = timezone('Asia/Calcutta')
today_date_str = datetime.now(indian_timezone).strftime ('%b-%d-%Y')


def round_to_5(x, base=5):
    if x < base:
        return x
    return base * round (x / base)


co_bo_blocked_stocks = []

with open ('C:/Users/Admin/Desktop/CM_Margin_'+ today_date_str + '.csv') as csv_file:
    csv_reader = csv.reader (csv_file, delimiter=',')
    for row in csv_reader:

        if len(row[0].strip()) > 0:
            co_bo_blocked_stocks.append(row[0].strip ())

stock_ids = [
    'SAIL',
    'PAGEIND',
    'GLENMARK',
    'ONGC',
    'BALKRISIND',
    'NBCC',
    'BHARTIARTL',
    'CESC',
    'UNIONBANK',
    'APOLLOHOSP',
]

orders = []

option_lots = nse_bse.get_nse_fo_lots ()

priority = 1
kite = util.intialize_kite_api()
for stock_id in stock_ids:
    if stock_id in co_bo_blocked_stocks:
        print('CO/BO blocked for stock_id:%s' %(stock_id))
        continue
    nse_stock_id = 'NSE:' + stock_id
    last_close = kite.quote (nse_stock_id)[nse_stock_id]['last_price']

    if stock_id in option_lots:
        future_lot = option_lots[stock_id]
    else:
        future_lot = None

    orig_future_lot = math.ceil (500000 / last_close)

    locale.setlocale(locale.LC_ALL, '')

    lot = int (orig_future_lot / 2)
    order = {
        'symbol': stock_id,
        'nse_symbol': 'NSE:' + stock_id,
        'future_lot': future_lot,
        'orig_future_lot': orig_future_lot,
        'priority': priority,
        'trigger_price_pts': round (util.min_tick_size * round ((200 / lot) / util.min_tick_size), 2),
        'traded_amount': locale.currency(last_close * lot, grouping=True),
        'last_close': last_close,
        'tag': None
             }
    orders.append (order)
    priority += 1
    time.sleep (1)

print ('[')
for order in orders:
    print (' {')
    for k, v in order.items ():
        print ("   '" + k + "':", [str (v), "'" + str (v) + "'"][k == 'symbol' or k == 'nse_symbol' or k == 'name' or k == 'traded_amount'] + ",")
    print (' },')
print (']')
