import os
from datetime import datetime
import ScrapUtils as sutils
import time

import GenericStatPrinter as gstats
import ScrapUtils as nse_bse
import Utils as util
import csv

today_date = datetime.now ().strftime ('%Y-%m-%d')
excel_location = ['G:', 'F:'][os.path.exists('F:')] + '/Trading_Responses/UpDownBackTest_' + str (today_date) + '.xlsx'
start_time = time.time()
updowngap_responses = [['Date', 'Symbol', 'Change', 'Up/Down', 'PERF. Trend', 'max P/L', 'Close P/L', 'Half F. Lot', 'Max move', 'Max Volat.', 'P/L', 'TAG']]

co_bo_stocks = []
co_bo_blocked_stocks = []
with open ('C:/Users/Admin/Desktop/CM_Margin_Sep-19-2019.csv') as csv_file:
    csv_reader = csv.reader (csv_file, delimiter=',')
    for row in csv_reader:
        if len(row[0].strip ()) > 0:
            co_bo_stocks.append (row[0].strip ())

        if len(row[1].strip()) > 0:
            co_bo_blocked_stocks.append(row[1].strip ())

fo_stock_ids = []

# for co_bo_stock in co_bo_stocks:
#     if co_bo_stock not in fo_stock_ids and co_bo_stock not in co_bo_blocked_stocks:
#         fo_stock_ids.append(co_bo_stock)

fo_stocks = sutils.get_nse_fo_stocks()
for fo_stock in fo_stocks:
    fo_stock_ids.append(fo_stock[sutils.STOCK_ID])
highest_lowest = {}

date_wise_stock_data = {}

for fo_stock_id in fo_stock_ids:

    stock_datas = util.get_equity_historical_data (fo_stock_id)

    high_low_diff = []

    for stock_data in stock_datas:
        if stock_data['date'] not in date_wise_stock_data:
            date_wise_stock_data[stock_data['date']] = [stock_data]
        else:
            date_wise_stock_data[stock_data['date']].append(stock_data)

for s_date, stock_datas in date_wise_stock_data.items():
    ascent_stocks = {}
    descent_stocks = {}
    for stock_data in stock_datas:
        open_price = stock_data['open']
        prev_close = stock_data['prev_close']
        high = stock_data['high']
        low = stock_data['low']
        close = stock_data['close']
        symbol = stock_data['symbol']

        future_lot = 500000 / open_price
        max_move = 1000 / (future_lot / 3)
        max_volat = 4000 / (future_lot / 2)

        half_future_lot = future_lot / 2

        if (high - open_price) > (open_price - low):
            max_pl = (high - open_price) * half_future_lot
        else:
            max_pl = (open_price - low) * half_future_lot

        close_pl = abs(open_price - close) * half_future_lot

        ascent = 0
        descent = 0

        if open_price > prev_close:
            ascent = abs ((open_price - prev_close) / prev_close)
            ascent_stocks[symbol] = [str(s_date), symbol, ascent, 'UP', (open_price == low or open_price == high), max_pl, close_pl, half_future_lot, max_move, max_volat]
        elif open_price < prev_close:
            descent = abs ((open_price - prev_close) / prev_close)
            descent_stocks[symbol] = [str (s_date), symbol, descent, 'DN', (open_price == low or open_price == high), max_pl, close_pl, half_future_lot, max_move, max_volat]

    ascent_stocks = [(k[0], ascent_stocks[k[0]]) for k in sorted (ascent_stocks.items (), key=lambda x: x[1][2])]
    descent_stocks = [(k[0], descent_stocks[k[0]]) for k in sorted (descent_stocks.items (), key=lambda x: x[1][2])]

    if len(descent_stocks) > 0:
        updowngap_responses.append (descent_stocks[int (len(descent_stocks) / 2)][1])
    else:
        print('No descent stocks on %s' %(str(s_date)))

    if len(ascent_stocks) > 0:
        updowngap_responses.append (ascent_stocks[int (len(ascent_stocks) / 2)][1])
    else:
        print('No ascent stocks on %s' %(str(s_date)))

updowngap_responses = sorted (updowngap_responses, key=lambda x: x[0], reverse=True)
gstats.print_statistics (updowngap_responses, excel_location)


print("---Script executed in %s seconds ---" % (time.time() - start_time))
