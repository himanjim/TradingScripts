import time
import traceback

import GenericStatPrinter as gstats
import ScrapUtils as sutils
import Utils as util

no_of_prev_sessions = 5

# zerodha_MIS_stocks = sutils.get_tradable_stocks_ids()
nse_fo_stocks = sutils.get_nse_fo_stocks()
nse_fo_stock_lots = sutils.get_nse_fo_lots()

res = []
for stock in nse_fo_stocks:
    try:
        stock_datas = util.get_equity_historical_data (stock[sutils.STOCK_ID])

        lots = nse_fo_stock_lots[stock[sutils.STOCK_ID]]

        if stock_datas[- 1]['close'] < stock_datas[- no_of_prev_sessions]['close'] and (stock_datas[-1]['close'] > stock_datas[-2]['open'] > stock_datas[-2]['close']) and stock_datas[-1]['open'] < stock_datas[-1]['close']:

            res.append([stock_datas[-1]['date'], stock[sutils.STOCK_ID], 'BULL', lots])

        elif stock_datas[- 1]['close'] > stock_datas[- no_of_prev_sessions]['close'] and (stock_datas[-1]['close'] < stock_datas[-2]['open'] < stock_datas[-2]['close']) and stock_datas[-1]['open'] > stock_datas[-1]['close']:

            res.append([stock_datas[-1]['date'], stock[sutils.STOCK_ID], 'BEAR', lots])
    except Exception:
        print (traceback.format_exc ())

for r in res:
    print(r)