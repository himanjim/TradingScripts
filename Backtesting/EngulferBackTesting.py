import time
import traceback

import GenericStatPrinter as gstats
import ScrapUtils as sutils
import Utils as util

no_of_prev_sessions = 5
stoploss = .05
max_loss = -20000
current_time = time.strftime ("%Y_%m_%d#%H_%M_%S")
excel_location = 'F:/Trading_Responses/Engulfer_Backtest_excel_' + str (current_time) + '.xlsx'

# zerodha_MIS_stocks = sutils.get_tradable_stocks_ids()
nse_fo_stocks = sutils.get_nse_fo_stocks()
nse_fo_stock_lots = sutils.get_nse_fo_lots()
# nse_fo_stocks_ids = []
# for nse_fo_stock in nse_fo_stocks:
#     nse_fo_stocks_ids.append(nse_fo_stock[sutils.STOCK_ID])

res = []
for stock in nse_fo_stocks:
    try:
        stock_datas = util.get_equity_historical_data (stock[sutils.STOCK_ID])

        for i in range (no_of_prev_sessions, len (stock_datas) - 1):
            total_vol = 0
            for j in range (i - no_of_prev_sessions, i):
                total_vol += stock_datas[j]['volume']

            rel_vol = [0, 1][stock_datas[i]['volume'] > (total_vol / no_of_prev_sessions)]
            # lots = nse_fo_stock_lots[stock[sutils.STOCK_ID]]
            lots = int(500000 / stock_datas[i]['open'])

            if stock_datas[i - 1]['close'] < stock_datas[i - no_of_prev_sessions]['close'] and (stock_datas[i]['close'] > stock_datas[i - 1]['open'] > stock_datas[i - 1]['close'] > stock_datas[i]['open']) :
                dev = (stock_datas[i]['close'] - stock_datas[i]['open']) / stock_datas[i]['open']

                perfect_high_trend = 1
                for j in range(i - no_of_prev_sessions, i):
                    if stock_datas[j]['close'] < stock_datas[i]['low']:
                        perfect_high_trend = 0
                        break

                long_range = 0
                if ((stock_datas[i]['low'] - stock_datas[i + 1]['open']) * lots) < max_loss:
                    long_range = 1

                min_sl = stock_datas[i]['low']
                # if long_range == 1:
                max_buy_price = stock_datas[i + 1]['open']
                succ = 0
                pl = 0
                exit_price = 0
                cat = 0

                for j in range (i + 1, len (stock_datas)):
                    nxt_max_loss = (stock_datas[j]['low'] - stock_datas[j]['open']) * lots
                    if pl > 0:
                        exit_price = stock_datas[j]['close']
                        if abs(nxt_max_loss) > (pl / 2):
                            pl -= (pl / 2)
                            cat = -1
                            break
                        elif nxt_max_loss < (max_loss / 2):
                            pl += (max_loss / 2)
                            cat = -2
                            break

                    if nxt_max_loss < max_loss:
                        pl += max_loss
                        exit_price = stock_datas[j]['close']
                        cat = -3
                        break
                    if (nxt_max_loss + pl) < max_loss:
                        pl = max_loss
                        exit_price = stock_datas[j]['close']
                        cat = -4
                        break
                    if stock_datas[j]['low'] <= min_sl:
                        pl += ((min_sl - stock_datas[j]['open']) * lots)
                        exit_price = min_sl
                        cat = -5
                        break
                    if ((stock_datas[j]['high'] - stock_datas[j]['open']) * lots) > abs(max_loss):
                        pl += abs(max_loss)
                        exit_price = stock_datas[j]['close']
                        cat = 1
                        continue

                    pl += ((stock_datas[j]['close'] - stock_datas[j]['open']) * lots)

                if pl > 0:
                    succ = 1

                res.append([stock_datas[i]['date'], stock[sutils.STOCK_ID], 'BULL', succ, pl, rel_vol, lots, long_range, dev, stock_datas[j]['date'], perfect_high_trend, max_buy_price, exit_price, [0, 1][max_buy_price > stock_datas[i]['close']], cat])

            elif stock_datas[i - 1]['close'] > stock_datas[i - no_of_prev_sessions]['close'] and (stock_datas[i]['close'] < stock_datas[i - 1]['open'] < stock_datas[i - 1]['close'] < stock_datas[i]['open']):
                dev = (stock_datas[i]['open'] - stock_datas[i]['close']) / stock_datas[i]['open']
                # succ = [0, 1][stock_datas[i + 1]['close'] < stock_datas[i + 1]['open']]
                # lots = int (abs (max_loss) / (stoploss * stock_datas[i + 1]['open']))
                perfect_low_trend = 1
                for j in range(i - no_of_prev_sessions, i):
                    if stock_datas[j]['close'] > stock_datas[i]['high']:
                        perfect_low_trend = 0
                        break

                long_range = 0
                if ((stock_datas[i + 1]['open'] - stock_datas[i]['high']) * lots) < max_loss:
                    long_range = 1

                min_sl = stock_datas[i]['high']
                # if long_range == 1:
                #     min_sl = stock_datas[i + 1]['open'] + (abs(max_loss) / lots)
                max_sell_price = stock_datas[i + 1]['open']
                succ = 0
                pl = 0
                exit_price = 0
                cat = 0

                for j in range (i + 1, len (stock_datas)):
                    nxt_max_loss = (stock_datas[j]['open'] - stock_datas[j]['high']) * lots
                    if pl > 0:
                        exit_price = stock_datas[j]['close']
                        if abs (nxt_max_loss) > (pl / 2):
                            pl -= (pl / 2)
                            cat = -1
                            break
                        elif nxt_max_loss < (max_loss / 2):
                            pl += (max_loss / 2)
                            cat = -2
                            break

                    if nxt_max_loss < max_loss:
                        pl += max_loss
                        exit_price = stock_datas[j]['close']
                        cat = -3
                        break
                    if (nxt_max_loss + pl) < max_loss:
                        pl = max_loss
                        exit_price = stock_datas[j]['close']
                        cat = -4
                        break
                    if stock_datas[j]['high'] >= min_sl:
                        pl += ((stock_datas[j]['open'] - min_sl) * lots)
                        exit_price = min_sl
                        cat = -5
                        break
                    if ((stock_datas[j]['open'] - stock_datas[j]['low']) * lots) > abs (max_loss):
                        pl += abs(max_loss)
                        exit_price = stock_datas[j]['close']
                        cat = 1
                        continue

                    pl += ((stock_datas[j]['open'] - stock_datas[j]['close']) * lots)

                if pl > 0:
                    succ = 1

                res.append([stock_datas[i]['date'], stock[sutils.STOCK_ID], 'BEAR', succ, pl, rel_vol, lots, long_range, dev, stock_datas[j]['date'], perfect_low_trend, max_sell_price, exit_price, [0, 1][max_sell_price < stock_datas[i]['close']], cat])
    except Exception:
        print (traceback.format_exc ())
res.insert (0, ['DATE', 'STOCK', 'TYPE', 'SUCCESS', 'PL', 'PL_CL', 'REL_VOL', 'LOTS', 'LONG RANGE', 'DEV', 'EXIT ON', 'TREND', 'ENTRY', 'EXIT', 'CAT'])

gstats.print_statistics(res, excel_location)