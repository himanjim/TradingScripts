import pickle
import traceback
from datetime import timedelta

import datetime
import time
from upstox_api.api import *

import GenericStatPrinter as gstats
import ScrapUtils as nse_bse
import Utils as util

current_time = time.strftime ("%Y_%m_%d_%H_%M_%S")
scalper_responses_excel_location = 'F:/Trading_Responses/Scalper_excel_backtest' + str (current_time) + '.xlsx'

start_time = time.time ()

upstox_api = util.intialize_upstox_api ([nse_bse.NSE, nse_bse.NSE_INDEX])

today_date = datetime.datetime.today ()
start_date = datetime.datetime.now () - timedelta (days=7)
end_date = datetime.datetime.now () - timedelta (days=1)

# stocks_latest_info = nse_bse.get_all_nse_stocks_ids()
stocks = nse_bse.get_nse_fo_stocks ()
# stocks_latest_info = nse_bse.get_nifty50_stocks_latest_info()
# stocks_latest_info = nse_bse.get_nifty100_stocks_latest_info ()
# stocks = [{nse_bse.STOCK_ID: 'TCS', nse_bse.EXCHANGE: nse_bse.NSE}]
# stocks[:] = stocks[40: 60]

option_lots = nse_bse.get_nse_fo_lots ()
max_loss = 3000
breakeven = 500

scalper_responses = []
MARKET_START_TIME = datetime.time (9, 15, 0)
MARKET_END_TIME = datetime.time (15, 30, 0)

stocks_data_obj = pickle.load (open (util.STOCK_DATA_OBJ_FILE, 'rb'))
for stock in stocks:
    try:
        # print ("---Fetching historic data for stock:" + stock_latest_info[nse_bse.STOCK_ID])
        stock_id = stock[nse_bse.STOCK_ID]
        stock_data = util.get_stock_latest_data (stock_id, upstox_api, start_date, end_date, stock[nse_bse.EXCHANGE],
                                                 stocks_data_obj, False, OHLCInterval.Minute_1)

        for i in range (len (stock_data)):
            data = stock_data[i]
            close = float (data['close'])
            open = float (data['open'])
            high = float (data['high'])
            low = float (data['low'])

            stock_time = util.get_date_from_timestamp (int (data['timestamp']))

            cover_points = max_loss / option_lots[stock_id]
            breakeven_points = breakeven / option_lots[stock_id]
            addition_buy_pts = cover_points / 2

            if stock_time.time () == MARKET_START_TIME:
                if open == low:
                    buy_pt = open + addition_buy_pts
                    stop_loss = buy_pt - cover_points
                    target = buy_pt + cover_points + breakeven_points

                    scalper_response = list ()
                    scalper_response.append (stock_id)
                    scalper_response.append (stock_time)
                    scalper_response.append (buy_pt)
                    scalper_response.append (target)
                    scalper_response.append (stop_loss)
                    scalper_response.append (1)

                    if high >= target:
                        scalper_response.append (high)
                        scalper_response.append ((high - buy_pt) * option_lots[stock_id])
                        scalper_response.append (1)
                        scalper_responses.append (scalper_response)
                        continue

                    for j in range (i + 1, len (stock_data)):
                        next_data = stock_data[j]
                        next_close = float (next_data['close'])
                        next_high = float (next_data['high'])
                        next_low = float (next_data['low'])

                        next_stock_time = util.get_date_from_timestamp (int (next_data['timestamp']))
                        if next_stock_time.time () == MARKET_END_TIME:
                            scalper_response.append (next_close)

                            if next_close < buy_pt:
                                scalper_response.append ((next_close - buy_pt) * option_lots[stock_id])
                                scalper_response.append (0)
                            else:
                                scalper_response.append ((next_close - buy_pt) * option_lots[stock_id])
                                scalper_response.append (1)
                            break
                        else:
                            if next_low <= stop_loss:
                                scalper_response.append (stop_loss)
                                scalper_response.append ((stop_loss - buy_pt) * option_lots[stock_id])
                                scalper_response.append (0)
                                break
                            elif next_high >= target:
                                scalper_response.append (next_high)
                                scalper_response.append ((next_high - buy_pt) * option_lots[stock_id])
                                scalper_response.append (1)
                                break

                    scalper_responses.append (scalper_response)

                elif open == high:

                    buy_pt = open - addition_buy_pts
                    stop_loss = buy_pt + cover_points
                    target = buy_pt - cover_points - breakeven_points

                    scalper_response = list ()
                    scalper_response.append (stock_id)
                    scalper_response.append (stock_time)
                    scalper_response.append (buy_pt)
                    scalper_response.append (target)
                    scalper_response.append (stop_loss)
                    scalper_response.append (0)

                    if low <= target:
                        scalper_response.append (low)
                        scalper_response.append ((buy_pt - low) * option_lots[stock_id])
                        scalper_response.append (1)
                        scalper_responses.append (scalper_response)
                        continue

                    for j in range (i + 1, len (stock_data)):
                        next_data = stock_data[j]
                        next_close = float (next_data['close'])
                        next_high = float (next_data['high'])
                        next_low = float (next_data['low'])

                        next_stock_time = util.get_date_from_timestamp (int (next_data['timestamp']))
                        if next_stock_time.time () == MARKET_END_TIME:
                            scalper_response.append (next_close)

                            if next_close > buy_pt:
                                scalper_response.append ((buy_pt - next_close) * option_lots[stock_id])
                                scalper_response.append (0)
                            else:
                                scalper_response.append ((buy_pt - next_close) * option_lots[stock_id])
                                scalper_response.append (1)
                            break
                        else:
                            if next_high >= stop_loss:
                                scalper_response.append (stop_loss)
                                scalper_response.append ((buy_pt - stop_loss) * option_lots[stock_id])
                                scalper_response.append (0)
                                break
                            elif next_low <= target:
                                scalper_response.append (next_low)
                                scalper_response.append ((buy_pt - next_low) * option_lots[stock_id])
                                scalper_response.append (1)
                                break

                    scalper_responses.append (scalper_response)

        time.sleep (.5)

    except Exception as e:
        print (traceback.format_exc ())

if len (scalper_responses) > 0:
    scalper_responses.insert (0, ['STOCK', 'BUY DATE', 'BUY PRICE', 'TARGET', 'STOPLOSS', 'LONG/SHORT', 'SELL PRICE',
                                  'EARNING', 'SUCC'])
    gstats.print_statistics (scalper_responses, scalper_responses_excel_location)
else:
    print ('No results.')

print ("---Script executed in %s seconds ---" % (time.time () - start_time))
