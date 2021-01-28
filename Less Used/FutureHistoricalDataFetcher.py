import traceback
from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta

import CalendarSpreadUtils as cal_util
import DerivativeUtils as outil
import ScrapUtils as nse_bse
import Utils as util


def write_data_to_file(file, data):
    open(file, 'w+').close()
    with open(file, 'a', encoding='utf-8') as the_file:
        the_file.write(str(data))


diff_between_start_end_date = 730
today_date = datetime.today().date()
start_date = datetime.now() - timedelta(days=diff_between_start_end_date)
end_date = datetime.now() - timedelta(days=1)
future_file_location = 'F:/Trading_Responses/Future_data/'

indices = nse_bse.get_indices()
futures = nse_bse.get_nse_fo_stocks()
futures.extend(indices)

kite = util.intialize_kite_api()
instruments = kite.instruments()

current_month_last_thurs = outil.get_last_thurday_of_month(today_date.month, today_date.year)

near_month_last_thurs = current_month_last_thurs + relativedelta(months=+1)

far_month_last_thurs = near_month_last_thurs + relativedelta(months=+1)

for future in futures:
    try:
        stock_id = outil.get_stock_id(future[nse_bse.STOCK_ID])

        future_current_month_symbol = outil.get_future_symbol(stock_id, current_month_last_thurs)
        future_near_month_symbol = outil.get_future_symbol(stock_id, near_month_last_thurs)
        future_far_month_symbol = outil.get_future_symbol(stock_id, far_month_last_thurs)

        current_month = kite.historical_data(cal_util.get_instrument_token(future_current_month_symbol, instruments),
                                             start_date,
                                             end_date, 'day', continuous=True)
        near_month = kite.historical_data(cal_util.get_instrument_token(future_near_month_symbol, instruments),
                                          start_date,
                                          end_date, 'day', continuous=True)
        far_month = kite.historical_data(cal_util.get_instrument_token(future_far_month_symbol, instruments),
                                         start_date,
                                         end_date, 'day', continuous=True)

        future_file = future_file_location + stock_id + '_fut_current_month' + '.txt'
        write_data_to_file(future_file, current_month)

        future_file = future_file_location + stock_id + '_fut_near_month' + '.txt'
        write_data_to_file(future_file, near_month)

        future_file = future_file_location + stock_id + '_fut_far_month' + '.txt'
        write_data_to_file(future_file, far_month)

    except Exception as e:
        print(str(traceback.format_exc()) + '\nError in stock:' + stock_id)
