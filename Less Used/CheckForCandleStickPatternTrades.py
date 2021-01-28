import pickle
import traceback
from datetime import datetime, timedelta

import time

import CSUtils as csutil
import CheckForCandleStickPatternTradesStats as stat
import DerivativeUtils as outil
import PatternRecognition as pr
import ScrapUtils as nse_bse
import Utils as util

no_of_sessions_to_scan_forstocks = 14
diff_between_start_end_date = 20
no_of_sessions_for_previous_market_trend = 10
no_of_sessions_to_scan_for_RSI = 14
no_of_days_for_volatility_stop_loss = pr.no_of_days_for_volatility_stop_loss
no_of_sessions_to_scan_for_volatility = pr.no_of_sessions_to_scan_for_volatility
current_time = time.strftime ("%Y_%m_%d_%H_%M_%S")
exception_error_file_location = 'F:/Trading_Responses/CS_errors_' + str(current_time) + '.txt'
response_file_location = 'F:/Trading_Responses/CS_response_' + str(current_time) + '.txt'
correct_cs_pattern_excel_location = 'F:/Trading_Responses/CS_excel_' + str(current_time) + '.xlsx'

start_time = time.time()

upstox_api = util.intialize_upstox_api([nse_bse.NSE, nse_bse.NSE_INDEX, nse_bse.NSE_FO])

today_date=datetime.today().date()
start_date=datetime.now() - timedelta(days=diff_between_start_end_date)
end_date=datetime.now() - timedelta(days=1)

market_start_date = datetime.now () - timedelta (days=(no_of_sessions_for_previous_market_trend + 10))
market_end_date=datetime.now()

stocks_data_obj = pickle.load(open(util.STOCK_DATA_OBJ_FILE, 'rb'))

market_historic_data = util.get_stock_latest_data ('NIFTY_50', upstox_api, market_start_date, market_end_date,
                                                   'NSE_INDEX', stocks_data_obj)

market_historic_data = market_historic_data[-no_of_sessions_for_previous_market_trend:]
market_previous_trend=pr.check_previous_trend(market_historic_data)
# stocks_latest_info = nse_bse.get_all_nse_stocks_ids()
stocks_latest_info = nse_bse.get_nse_fo_stocks()
nifty_50_stocks = nse_bse.get_nifty50_stocks_latest_info ()
# stocks_latest_info = nse_bse.get_nifty50_stocks_latest_info()
# stocks_latest_info = nse_bse.get_nifty100_stocks_latest_info ()
# stocks_latest_info = [{nse_bse.STOCK_ID: 'SAIL', nse_bse.EXCHANGE: nse_bse.NSE}, {nse_bse.STOCK_ID: 'VOLTAS', nse_bse.EXCHANGE: nse_bse.NSE}, {nse_bse.STOCK_ID: 'TCS', nse_bse.EXCHANGE: nse_bse.NSE}, {nse_bse.STOCK_ID: 'COALINDIA', nse_bse.EXCHANGE: nse_bse.NSE}]
# stocks_latest_info = [{nse_bse.STOCK_ID: 'RBLBANK', nse_bse.EXCHANGE: nse_bse.NSE}]

required_margins = outil.get_future_margins (stocks_latest_info)
if util.is_market_open ():
    available_margin = upstox_api.get_balance ()['equity']['available_margin']
    stocks_latest_info[:] = [x for x in stocks_latest_info if
                             required_margins[outil.get_stock_id (x[nse_bse.STOCK_ID])] <= (.95 * available_margin)]

stocks_pattern_recognition_responses=[]
option_lots = nse_bse.get_nse_fo_lots()
current_month_last_thurs = outil.get_last_thurday_of_month(datetime.now().month, datetime.now().year)

no_of_days_till_last_thurs = current_month_last_thurs.day - today_date.day + 1

# fetched_stocks_data = {}
# util.run_fetch_stocks_data(stocks_latest_info, fetched_stocks_data, upstox_api, start_date, end_date, stocks_data_obj,
#                            True)

exception_errors=[]
for stock_latest_info in stocks_latest_info:
    try:
        #print ("---Fetching historic data for stock:" + stock_latest_info[nse_bse.STOCK_ID])
        stock_latest_data = util.get_stock_latest_data (stock_latest_info[nse_bse.STOCK_ID], upstox_api, start_date,
                                                        end_date, nse_bse.NSE, stocks_data_obj, True)
        time.sleep(1)
        #print ("---Fetched historic data of sessions:" + str (len (stock_latest_data)) + " for stock:" + stock_latest_info[nse_bse.STOCK_ID])

        csutil.getCSResAndErrors (stock_latest_info, stock_latest_data, stocks_pattern_recognition_responses,
                                  exception_errors, market_previous_trend, no_of_sessions_to_scan_forstocks,
                                  no_of_sessions_to_scan_for_RSI, no_of_sessions_to_scan_for_volatility,
                                  no_of_days_for_volatility_stop_loss)

    except Exception as e:
        print(traceback.format_exc())
        exception_errors.append(str(traceback.format_exc()))

# stocks_pattern_recognition_responses_to_exec = [x for x in stocks_pattern_recognition_responses if
#                                                 x.is_pattern_tradable()]
# stocks_pattern_recognition_responses_to_exec.sort(key=lambda x: -x.points)
#
# for s_res in stocks_pattern_recognition_responses_to_exec:
#     print(s_res)
#     execute = input("Execute?")
#     if int(execute) == 1:
#
#         if no_of_days_till_last_thurs >= 10:
#             future_symbol = outil.get_future_symbol(outil.get_stock_id(s_res.stock_id), current_month_last_thurs)
#         else:
#             future_symbol = outil.get_future_symbol(outil.get_stock_id(s_res.stock_id),
#                                                     current_month_last_thurs + relativedelta(months=+1))
#
#         if s_res.action.value == pr.Action.LONG.value:
#             outil.buy_instrument(upstox_api, future_symbol, nse_bse.NSE_FO, None, option_lots[s_res.stock_id],
#                                  OrderType.Market)
#         elif s_res.action.value == pr.Action.SHORT.value:
#             outil.sell_instrument(upstox_api, future_symbol, nse_bse.NSE_FO, None, option_lots[s_res.stock_id],
#                                   OrderType.Market)


open(response_file_location, 'w+').close()
with open(response_file_location, 'a') as the_file:
    the_file.write ("\n---Market trend:" + str (market_previous_trend) + " " + str (market_historic_data[0]) + " : " + str (
        market_historic_data[-1]))
    stat.print_statistics (stocks_pattern_recognition_responses, market_previous_trend, the_file,
                           correct_cs_pattern_excel_location, nifty_50_stocks, option_lots, required_margins)
    for stocks_pattern_recognition_response in stocks_pattern_recognition_responses:
        if stocks_pattern_recognition_response.is_pattern_tradable():
            the_file.write("Correct candlestick match response start******************")
            the_file.write(str(stocks_pattern_recognition_response))
            the_file.write("Correct candlestick match response end*******************")


stocks_pattern_recognition_responses.sort (key=lambda x: len(x.errors))

with open(response_file_location, 'a') as the_file:
    for stocks_pattern_recognition_response in stocks_pattern_recognition_responses:
        if stocks_pattern_recognition_response.is_pattern_tradable() == False and stocks_pattern_recognition_response.pattern_match:
            the_file.write(str(stocks_pattern_recognition_response))


with open(response_file_location, 'a') as the_file:
    for stocks_pattern_recognition_response in stocks_pattern_recognition_responses:
        if stocks_pattern_recognition_response.is_pattern_tradable() == False and stocks_pattern_recognition_response.pattern_match == False:
            the_file.write(str(stocks_pattern_recognition_response))


error_count=1
open(exception_error_file_location, 'w+').close()
with open(exception_error_file_location, 'a') as the_file:
    for exception_error in exception_errors:
        the_file.write(str(error_count)+': '+str(exception_error)+'\n')
        error_count+=1

print("---Script executed in %s seconds ---" % (time.time() - start_time))