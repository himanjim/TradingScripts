import traceback
from datetime import datetime, timedelta

import time

import MAUtils as mautil
import MovingAverageTradeBacktestingStats as stat
import ScrapUtils as nse_bse
import Utils as util

diff_between_start_end_date = 1500
no_of_sessions_from_start_to_begin = 300
no_of_buffer_sessions_to_keep = 100
max_no_of_back_days_tolerate = 2
current_time = time.strftime ("%Y_%m_%d#%H_%M_%S")
exception_error_file_location = 'F:/Trading_Responses/MA_backtest_errors_' + str (current_time) + '.txt'
response_file_location = 'F:/Trading_Responses/MA_backtest_response_' + str (current_time) + '.txt'
ma_strategy_excel_location = 'F:/Trading_Responses/MA_backtest_excel_' + str (current_time) + '.xlsx'

start_time = time.time ()

upstox_api = util.intialize_upstox_api ([nse_bse.NSE_INDEX, nse_bse.NSE])

today_date = datetime.today ().date ()
start_date = datetime.now () - timedelta (days=diff_between_start_end_date)
end_date = datetime.now () - timedelta (days=1)

# stocks_latest_info=nse_bse.get_all_nse_stocks_ids()
stocks_latest_info = nse_bse.get_nifty50_stocks_latest_info ()
indices = nse_bse.get_indices ()
stocks_latest_info = nse_bse.get_nse_fo_stocks ()
stocks_latest_info.extend (indices)
# stocks_latest_info=stocks_latest_info[-200:-100]
# stocks_latest_info = [{nse_bse.STOCK_ID: 'ITC', nse_bse.EXCHANGE: nse_bse.NSE}]

all_moving_average_strategy_responses = []
exception_errors = []

tradable_moving_average_strategy_responses = []
for stock_latest_info in stocks_latest_info:
    try:
        # print ("---Fetching historic data for stock:" + stock_latest_info[nse_bse.STOCK_ID])
        stock_latest_data = util.get_stock_latest_data (stock_latest_info[nse_bse.STOCK_ID], upstox_api, start_date,
                                                        end_date, stock_latest_info[nse_bse.EXCHANGE])
        # print ("---Fetched historic data of sessions:" + str (stock_latest_data_len) + " for stock:" + future[nse_bse.STOCK_ID])
        stock_latest_data_len = len (stock_latest_data)

        i = 0

        while (no_of_sessions_from_start_to_begin + i + no_of_buffer_sessions_to_keep) < stock_latest_data_len:
            moving_average_strategy_responses = []
            mautil.test_ma_strategies (stock_latest_data[:no_of_sessions_from_start_to_begin + i], stock_latest_info,
                                       moving_average_strategy_responses, exception_errors, True)


            for moving_average_strategy_response in moving_average_strategy_responses:

                all_moving_average_strategy_responses.append (moving_average_strategy_response)

                if moving_average_strategy_response.is_strategy_tradable () and moving_average_strategy_response.days_back_when_stock_price_less_than_sma <= max_no_of_back_days_tolerate:
                    re_test_ma_strategy_res = mautil.re_test_ma_strategy (stock_latest_data,
                                                                          moving_average_strategy_response,
                                                                          (no_of_sessions_from_start_to_begin + i+1), True)

                    if re_test_ma_strategy_res:

                        tradable_moving_average_strategy_responses.append(
                            {'stock_id': moving_average_strategy_response.stock_id,
                             'strategy': moving_average_strategy_response.ma_strategy_name.name,
                             'bought_when': moving_average_strategy_response.fetch_date,
                             'bought_at': moving_average_strategy_response.current_day_current_price,
                             'bought_smas': moving_average_strategy_response.sma,
                             'bought_lmas': moving_average_strategy_response.lma,
                             'sold_when': util.get_date_from_timestamp (re_test_ma_strategy_res['timestamp']),
                             'sold_at': re_test_ma_strategy_res['close'], 'profit': (re_test_ma_strategy_res[
                                                                                         'close'] - moving_average_strategy_response.current_day_current_price),
                             'sold_mas': re_test_ma_strategy_res['sold_mas'],
                             'prev_sma_low': moving_average_strategy_response.prev_sma_less_than_lma,
                             'when_sma_last_low': moving_average_strategy_response.days_back_when_stock_price_less_than_sma,
                             'macd_slope': moving_average_strategy_response.macd_high_slope,
                             'success': (re_test_ma_strategy_res[
                                             'close'] - moving_average_strategy_response.current_day_current_price) > 0})

            i += 1

    except Exception as e:
        print (traceback.format_exc ())
        exception_errors.append (str (traceback.format_exc ()))

stat.print_statistics (tradable_moving_average_strategy_responses, ma_strategy_excel_location)

all_moving_average_strategy_responses.sort (key=lambda x: (x.stock_id, x.days_back_when_stock_price_less_than_sma))

open (response_file_location, 'w+').close ()
with open (response_file_location, 'a') as the_file:
    for moving_average_strategy_response in all_moving_average_strategy_responses:
        if moving_average_strategy_response.is_strategy_tradable ():
            the_file.write ("\n\nCorrect moving average strategy response start******************")
            the_file.write (str (moving_average_strategy_response))
            the_file.write ("Correct moving average strategy match response end******************\n\n")

error_count = 1
open (exception_error_file_location, 'w+').close ()
with open (exception_error_file_location, 'a') as the_file:
    for exception_error in exception_errors:
        the_file.write (str (error_count) + ': ' + str (exception_error) + '\n')
        error_count += 1

print ("---Script executed in %s seconds ---" % (time.time () - start_time))
