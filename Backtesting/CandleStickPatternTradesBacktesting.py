import traceback
from datetime import datetime, timedelta

import time

import CSUtils as csutil
import CandleStickPatternTradesBacktestingStats as csBackStat
import PatternRecognition as pr
import ScrapUtils as nse_bse
import SupportResistance as sr
import Utils as util

no_of_sessions_to_scan_forstocks = 200
diff_between_start_end_date = 1600
no_of_sessions_to_skip_for_sr_from_start = sr.no_of_sessions_to_scan
no_of_sessions_to_buffer_from_end = 5
no_of_sessions_for_previous_market_trend = 10
no_of_sessions_to_scan_for_RSI = 14
no_of_days_for_volatility_stop_loss = pr.no_of_days_for_volatility_stop_loss
no_of_sessions_to_scan_for_volatility = pr.no_of_sessions_to_scan_for_volatility
desired_risk_reward_ratio = pr.acceptable_risk_reward_ratio
current_time = time.strftime ("%Y_%m_%d#%H_%M_%S")
exception_error_file_location = 'F:/Trading_Responses/CS_errors_back_test' + str (current_time) + '.txt'
response_file_location = 'F:/Trading_Responses/CS_response_back_test' + str (current_time) + '.txt'
correct_cs_pattern_excel_location = 'F:/Trading_Responses/CS_excel_back_test' + str (current_time) + '.xlsx'

start_time = time.time()

upstox_api = util.intialize_upstox_api(['NSE_INDEX','NSE_EQ'])

today_date=datetime.today().date()
start_date=datetime.now() - timedelta(days=diff_between_start_end_date)
end_date=datetime.now() - timedelta(days=1)

market_historic_data = util.get_stock_latest_data('NIFTY_50', upstox_api, start_date, end_date, 'NSE_INDEX')

# stocks_latest_info = nse_bse.get_all_nse_stocks_ids ()
stocks_latest_info = nse_bse.get_nifty50_stocks_latest_info ()
# stocks_latest_info = nse_bse.get_nse_fo_stocks ()
# stocks_latest_info = nse_bse.get_nse_fo_stocks ()
# stocks_latest_info = nse_bse.get_indices ()
#stocks_latest_info = nse_bse.get_nifty100_stocks_latest_info ()
#stocks_latest_info=[{nse_bse.STOCK_ID:'HDFC',nse_bse.EXCHANGE:nse_bse.NSE}]

all_stocks_pattern_recognition_responses = []
pattern_recognition_results = []

lots = nse_bse.get_nse_fo_lots ()
exception_errors=[]
for stock_latest_info in stocks_latest_info:
    try:
        #print ("---Fetching historic data for stock:" + stock_latest_info[nse_bse.STOCK_ID])
        stock_latest_data = util.get_stock_latest_data (stock_latest_info[nse_bse.STOCK_ID], upstox_api, start_date,
                                                        end_date, stock_latest_info[nse_bse.EXCHANGE], None, False)

        #print ("---Fetched historic data of sessions:" + str (len (stock_latest_data)) + " for stock:" + stock_latest_info[nse_bse.STOCK_ID])
        i = no_of_sessions_to_skip_for_sr_from_start

        while i + no_of_sessions_to_buffer_from_end < len (stock_latest_data):
            stocks_pattern_recognition_responses = []

            market_previous_trend = pr.check_previous_trend(market_historic_data[i - no_of_sessions_for_previous_market_trend - 1:i])

            csutil.getCSResAndErrors (stock_latest_info, stock_latest_data[:i], stocks_pattern_recognition_responses,
                                      exception_errors, market_previous_trend, no_of_sessions_to_scan_forstocks,
                                      no_of_sessions_to_scan_for_RSI, no_of_sessions_to_scan_for_volatility,
                                      no_of_days_for_volatility_stop_loss)

            for stocks_pattern_recognition_response in stocks_pattern_recognition_responses:
                if stocks_pattern_recognition_response.pattern_match:
                    all_stocks_pattern_recognition_responses.append(stocks_pattern_recognition_response)

                    pattern_recognition_result = csutil.get_pattern_recognition_response_result (
                        stocks_pattern_recognition_response, stock_latest_data, desired_risk_reward_ratio, i,
                        lots[stock_latest_info[nse_bse.STOCK_ID]])

                    pattern_recognition_results.append(pattern_recognition_result)

            i += 1

    except Exception as e:
        print (traceback.format_exc ())
        exception_errors.append(str(traceback.format_exc()))


all_stocks_pattern_recognition_responses.sort (key=lambda x: -x.points)

open(response_file_location, 'w+').close()
with open(response_file_location, 'a') as the_file:

    csBackStat.print_statistics(pattern_recognition_results, market_previous_trend, the_file, correct_cs_pattern_excel_location)

    for stocks_pattern_recognition_response in all_stocks_pattern_recognition_responses:
        if stocks_pattern_recognition_response.is_pattern_tradable():
            the_file.write("Correct candlestick match response start******************")
            the_file.write(str(stocks_pattern_recognition_response))
            the_file.write("Correct candlestick match response end*******************")


all_stocks_pattern_recognition_responses.sort (key=lambda x: len(x.errors))

with open(response_file_location, 'a') as the_file:
    for stocks_pattern_recognition_response in all_stocks_pattern_recognition_responses:
        if stocks_pattern_recognition_response.is_pattern_tradable() == False and stocks_pattern_recognition_response.pattern_match:
            the_file.write(str(stocks_pattern_recognition_response))


with open(response_file_location, 'a') as the_file:
    for stocks_pattern_recognition_response in all_stocks_pattern_recognition_responses:
        if stocks_pattern_recognition_response.is_pattern_tradable() == False and stocks_pattern_recognition_response.pattern_match == False:
            the_file.write(str(stocks_pattern_recognition_response))


error_count=1
open(exception_error_file_location, 'w+').close()
with open(exception_error_file_location, 'a') as the_file:
    for exception_error in exception_errors:
        the_file.write(str(error_count)+': '+str(exception_error)+'\n')
        error_count+=1

print("---Script executed in %s seconds ---" % (time.time() - start_time))