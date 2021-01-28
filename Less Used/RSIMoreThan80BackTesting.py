import traceback
from datetime import datetime, timedelta

import time

import GenericStatPrinter as gstats
import Indicators as ind
import ScrapUtils as nse_bse
import Utils as util

diff_between_start_end_date = 1200
no_of_sessions_to_skip_from_start = 50
no_of_sessions_to_buffer_from_end = 10
no_of_sessions_to_scan_for_RSI = 4
upper_limit_for_rsi = 65
lower_limit_for_rsi = 10

current_time = time.strftime ("%Y_%m_%d#%H_%M_%S")
rsi80_excel_location = 'D:/Trading_Responses/RSI80_excel_back_test' + str (current_time) + '.xlsx'

start_time = time.time ()

upstox_api = util.intialize_upstox_api (['NSE_INDEX', 'NSE_EQ'])

today_date = datetime.today ().date ()
start_date = datetime.now () - timedelta (days=diff_between_start_end_date)
end_date = datetime.now () - timedelta (days=1)

# stocks_latest_info = nse_bse.get_all_nse_stocks_ids ()
# stocks_latest_info = nse_bse.get_nifty50_stocks_latest_info ()
# stocks_latest_info = nse_bse.get_nse_fo_stocks ()
stocks_latest_info = nse_bse.get_indices ()
# stocks_latest_info = nse_bse.get_nifty100_stocks_latest_info ()
# stocks_latest_info=[{nse_bse.STOCK_ID:'HDFC',nse_bse.EXCHANGE:nse_bse.NSE}]

rsi_more_than_80_responses = [
    ['STOCK', 'STRATEGY', 'BGHT ON', 'SLD ON', 'BUY PRICE', 'EXIT PRICE', 'PROFIT/LOSS %', 'VOL']]

no_of_successful_cases = 0
no_of_failed_cases = 0

for stock_latest_info in stocks_latest_info:
    try:
        stock_latest_data = util.get_stock_latest_data (stock_latest_info[nse_bse.STOCK_ID], upstox_api, start_date,
                                                        end_date, stock_latest_info[nse_bse.EXCHANGE])

        stock_data_closing_prices_series = util.get_panda_series_of_stock_closing_prices (stock_latest_data)

        rsi_series = ind.rsi (stock_data_closing_prices_series, no_of_sessions_to_scan_for_RSI)

        i = no_of_sessions_to_skip_from_start

        while i + no_of_sessions_to_buffer_from_end < len (stock_latest_data):
            pivot = 1

            if rsi_series.iloc[i] <= lower_limit_for_rsi:
                for j in range (i + 2, len (stock_latest_data)):
                    pivot = j
                    # print(stock_latest_data[i])
                    # exit(0)
                    if rsi_series.iloc[j - 1] > upper_limit_for_rsi:
                        if stock_latest_data[j - 1]['close'] > stock_latest_data[i + 1]['open']:
                            no_of_successful_cases += 1
                        else:
                            no_of_failed_cases += 1

                        break

                    if rsi_series.iloc[j] < rsi_series.iloc[j - 1]:
                        if stock_latest_data[j]['close'] > stock_latest_data[i + 1]['open']:
                            no_of_successful_cases += 1
                        else:
                            no_of_failed_cases += 1

                        break

                rsi_more_than_80_responses.append ([stock_latest_info[nse_bse.STOCK_ID], 'RSI>80',
                                                    util.get_date_from_timestamp (
                                                        stock_latest_data[i + 1]['timestamp']),
                                                    util.get_date_from_timestamp (stock_latest_data[j]['timestamp']),
                                                    stock_latest_data[i + 1]['open'], stock_latest_data[j]['close'], ((
                                                                                                                                  stock_latest_data[
                                                                                                                                      i + 1][
                                                                                                                                      'open'] -
                                                                                                                                  stock_latest_data[
                                                                                                                                      j][
                                                                                                                                      'close']) * 100) /
                                                    stock_latest_data[i + 1]['open'], stock_latest_data[i]['volume']])

            i = pivot + i




    except Exception as e:
        print (traceback.format_exc ())

print ("Successful RSI>80 cases: %s" % ((no_of_successful_cases * 100) / (no_of_successful_cases + no_of_failed_cases)))

gstats.print_statistics (rsi_more_than_80_responses, rsi80_excel_location)

print ("---Script executed in %s seconds ---" % (time.time () - start_time))
