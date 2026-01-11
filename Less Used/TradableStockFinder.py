import traceback
import urllib.parse
import GenericStatPrinter as gstats
import DerivativeUtils as outil
import TradingScripts.ScrapUtils as sutils
import TradingScripts.Utils as util
import os
from datetime import datetime

stocks = sutils.get_nse_fo_stocks()
nse_fo_stocks = []
for stock in stocks:
    stock_id = urllib.parse.quote (outil.get_stock_id (stock[sutils.STOCK_ID]))
    nse_fo_stocks.append(stock_id)

today_date = datetime.now ().strftime ('%Y-%m-%d')
excel_location = ['G:', 'F:'][os.path.exists('F:')] + '/Trading_Responses/TradableStocks_' + str (today_date) + '.xlsx'

stocks = sutils.get_all_nse_stocks_ids()
# stocks = sutils.get_nse_fo_stocks()
# stocks = [{sutils.STOCK_ID: 'ASIANPAINT'}]
max_duration_to_consider = 180
min_daily_order_size = 500000
stock_market_cap = {}


for stock in stocks:
    try:
        stock_id = outil.get_stock_id(stock[sutils.STOCK_ID])
        stock_id = urllib.parse.quote(stock_id)

        stock_datas = util.get_equity_historical_data(stock_id)[-max_duration_to_consider:]
        min_daily_order_size_criteria_failed = False

        total_order_size = 0
        for stock_data in stock_datas:
            order_size = stock_data['close'] * stock_data['volume']

            if order_size < min_daily_order_size:
                print('Min daily order size criteria failed for stock:' + stock_id)
                min_daily_order_size_criteria_failed = True
                break

            total_order_size += order_size

        if min_daily_order_size_criteria_failed:
            continue

        stock_market_cap[stock_id] = total_order_size / max_duration_to_consider

    except Exception as e:
        print(str(traceback.format_exc()) + '\nError in stock:' + stock_id)

stock_market_cap = {k: v for k, v in sorted(stock_market_cap.items(), key=lambda item: item[1], reverse= True)}

res_to_print = [['STOCK', 'MARKET CAP', 'FO']]
for stock_id, market_cap in stock_market_cap.items():
    res_to_print.append([stock_id, market_cap, stock_id in nse_fo_stocks])

gstats.print_statistics(res_to_print, excel_location)