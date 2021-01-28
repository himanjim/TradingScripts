import time
import traceback

import CalendarSpreadUtils as c_spread
import DerivativeUtils as outil
import ScrapUtils as nse_bse
import Utils as util

start_time = time.time ()
# futures = nse_bse.get_all_nse_stocks_ids ()
# indices = nse_bse.get_indices ()
futures = nse_bse.get_nse_fo_stocks ()
# futures.extend (indices)
# futures = nse_bse.get_indices ()
# futures = nse_bse.get_nifty50_stocks_latest_info ()
# futures = nse_bse.get_nifty100_stocks_latest_info ()
# futures = [{nse_bse.STOCK_ID: 'TATAPOWER', nse_bse.EXCHANGE: nse_bse.NSE_FO}]
# futures.extend(indices)

future_limits = {}

for future in futures:
    try:
        stock_id = outil.get_stock_id (future[nse_bse.STOCK_ID])
        future_current_month_historical_data = []
        future_near_month_historical_data = []
        future_far_month_historical_data = []

        util.get_future_historical_data (stock_id, future_current_month_historical_data,
                                         future_near_month_historical_data, future_far_month_historical_data)

        future_near_month_historical_data = util.get_equity_historical_data (stock_id)

        current_near_months_diffs = c_spread.check_get_calendar_spread_month_diffs (
            future_current_month_historical_data, future_near_month_historical_data)

        current_far_months_diffs = c_spread.check_get_calendar_spread_month_diffs (future_current_month_historical_data,
                                                                                   future_far_month_historical_data)

        if len (current_near_months_diffs) >= c_spread.min_no_of_sessions_for_spread:

            current_near_months_range_upper_limit_for_buy, current_near_months_range_upper_limit_for_sell = c_spread.get_calendar_spread_upper_limit (
                current_near_months_diffs[-c_spread.min_no_of_sessions_for_spread:], c_spread.max_std_multiple_for_buy,
                c_spread.max_std_multiple_for_sell)

            current_far_months_range_upper_limit_for_buy, current_far_months_range_upper_limit_for_sell = c_spread.get_calendar_spread_upper_limit (
                current_far_months_diffs[-c_spread.min_no_of_sessions_for_spread:], c_spread.max_std_multiple_for_buy,
                c_spread.max_std_multiple_for_sell)

            future_limits[stock_id] = [current_near_months_range_upper_limit_for_buy,
                                       current_near_months_range_upper_limit_for_sell,
                                       current_far_months_range_upper_limit_for_buy,
                                       current_far_months_range_upper_limit_for_sell]
        else:
            print ('No data for stock:' + stock_id)


    except Exception as e:
        print (traceback.format_exc ())

print (future_limits)

print ("---Script executed in %s seconds ---" % (time.time () - start_time))
