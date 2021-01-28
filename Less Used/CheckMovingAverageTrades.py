import pickle
import time
import traceback
from datetime import datetime, timedelta

import CheckMovingAverageTradesStats as stat
import MAUtils as mautil
import ScrapUtils as nse_bse
import Utils as util

diff_between_start_end_date=1800
current_time = time.strftime ("%Y_%m_%d#%H_%M_%S")
exception_error_file_location = 'F:/Trading_Responses/MA_errors_' + str(current_time) + '.txt'
response_file_location = 'F:/Trading_Responses/MA_response_' + str(current_time) + '.txt'
ma_strategy_excel_location = 'F:/Trading_Responses/MA_excel_' + str(current_time) + '.xlsx'

start_time = time.time()

upstox_api = util.intialize_upstox_api([nse_bse.BSE,nse_bse.NSE])

today_date=datetime.today().date()
start_date=datetime.now() - timedelta(days=diff_between_start_end_date)
end_date=datetime.now() - timedelta(days=1)

# stocks_latest_info=nse_bse.get_all_nse_stocks_ids()
stocks_latest_info = nse_bse.get_nse_fo_stocks()
#stocks_latest_info=stocks_latest_info[-200:-100]
#stocks_latest_info=[{nse_bse.STOCK_ID:'BATAINDIA',nse_bse.EXCHANGE:nse_bse.NSE}]
stocks_data_obj = pickle.load(open(util.STOCK_DATA_OBJ_FILE, 'rb'))

moving_average_strategy_responses=[]

exception_errors=[]
for stock_latest_info in stocks_latest_info:
     try:
        #print ("---Fetching historic data for stock:" + stock_latest_info[nse_bse.STOCK_ID])
        stock_latest_data = util.get_stock_latest_data(stock_latest_info[nse_bse.STOCK_ID], upstox_api, start_date,
                                                       end_date, stock_latest_info[nse_bse.EXCHANGE], None,
                                                       True)

        #print ("---Fetched historic data of sessions:" + str (stock_latest_data_len) + " for stock:" + stock_latest_info[nse_bse.STOCK_ID])
        mautil.test_ma_strategies(stock_latest_data, stock_latest_info, moving_average_strategy_responses, exception_errors)

     except Exception as e:
           #logging.error (traceback.format_exc ())
            exception_errors.append(str(traceback.format_exc ()))

moving_average_strategy_responses.sort (key=lambda x: (x.stock_id,x.days_back_when_stock_price_less_than_sma))

open(response_file_location, 'w+').close()
with open(response_file_location, 'a') as the_file:
    stat.print_statistics(moving_average_strategy_responses,the_file, ma_strategy_excel_location)
    for moving_average_strategy_response in moving_average_strategy_responses:
        if moving_average_strategy_response.is_strategy_tradable():
            the_file.write ("\n\nCorrect moving average strategy response start******************")
            the_file.write (str(moving_average_strategy_response))
            the_file.write ("Correct moving average strategy match response end******************\n\n")

moving_average_strategy_responses.sort (key=lambda x: (len(x.errors)))
with open(response_file_location, 'a') as the_file:
    for moving_average_strategy_response in moving_average_strategy_responses:
        if moving_average_strategy_response.is_strategy_tradable() == False:
            the_file.write (str(moving_average_strategy_response))

error_count=1
open(exception_error_file_location, 'w+').close()
with open(exception_error_file_location, 'a') as the_file:
    for exception_error in exception_errors:
        the_file.write(str(error_count)+': '+str(exception_error)+'\n')
        error_count+=1

print("---Script executed in %s seconds ---" % (time.time() - start_time))