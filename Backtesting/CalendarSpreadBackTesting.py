import traceback
from datetime import datetime, timedelta

import time

import CalendarSpreadUtils as c_spread
import DerivativeUtils as outil
import GenericStatPrinter as gstats
import ScrapUtils as nse_bse
import Utils as util

diff_between_start_end_date = 730
min_no_of_sessions_for_spread = c_spread.min_no_of_sessions_for_spread
buffer_from_end = 2
min_return_monthly = 10
current_time = time.strftime ("%Y_%m_%d#%H_%M_%S")
cspread_backtest_excel_location = 'F:/Trading_Responses/CalendarSpread_back_test_excel_' + str (current_time) + '.xlsx'

start_time = time.time ()

today_date = datetime.today ().date ()
start_date = datetime.now () - timedelta (days=diff_between_start_end_date)
end_date = datetime.now () - timedelta (days=1)
futures = []
# futures = nse_bse.get_all_nse_stocks_ids ()
indices = nse_bse.get_indices ()
# futures = nse_bse.get_nse_fo_stocks ()
futures.extend (indices)
futures = [{nse_bse.STOCK_ID: 'SBIN', nse_bse.EXCHANGE: nse_bse.NSE_FO}]

cs_responses = []

option_lots = nse_bse.get_nse_fo_lots ()

for future in futures:
    try:
        stock_id = outil.get_stock_id (future[nse_bse.STOCK_ID])
        future_current_month_historical_data = []
        future_near_month_historical_data = []
        future_far_month_historical_data = []

        util.get_future_historical_data(stock_id, future_current_month_historical_data,
                                        future_near_month_historical_data, future_far_month_historical_data)

        future_near_month_historical_data = util.get_equity_historical_data(stock_id)

        current_near_months_diffs = c_spread.check_get_calendar_spread_month_diffs(future_current_month_historical_data,
                                                                                   future_near_month_historical_data)

        # current_far_months_diffs = c_spread.check_get_calendar_spread_month_diffs(future_current_month_historical_data, future_far_month_historical_data)

        k = min_no_of_sessions_for_spread
        while k + buffer_from_end < len (current_near_months_diffs):

            current_near_months_range_upper_limit_for_buy, current_near_months_range_upper_limit_for_sell = c_spread.get_calendar_spread_upper_limit (
                current_near_months_diffs[k - min_no_of_sessions_for_spread: k + 1], c_spread.max_std_multiple_for_buy,
                c_spread.max_std_multiple_for_sell)

            # current_far_months_range_upper_limit_for_buy, current_far_months_range_upper_limit_for_sell = c_spread.get_calendar_spread_upper_limit (
            #     current_far_months_diffs[k - min_no_of_sessions_for_spread: k + 1], c_spread.max_std_multiple_for_buy,
            #     c_spread.max_std_multiple_for_sell)

            margin = outil.get_margin (stock_id, today_date.month, today_date.year)

            spread = future_current_month_historical_data[k]['ltp'] - future_near_month_historical_data[k]['ltp']

            if spread > current_near_months_range_upper_limit_for_buy:
                next_day_spread = c_spread.get_next_calendar_spread (future_current_month_historical_data,
                                                                     future_near_month_historical_data, k,
                                                                     current_near_months_range_upper_limit_for_sell)

                cs_responses.append (
                    [stock_id, future_near_month_historical_data[k]['date'], next_day_spread['date'],
                     current_near_months_range_upper_limit_for_buy, current_near_months_range_upper_limit_for_sell,
                     spread, next_day_spread['spread'],
                     spread / current_near_months_range_upper_limit_for_buy,
                     (spread - next_day_spread['spread']) * option_lots[stock_id], margin,
                     [0, 1][next_day_spread['spread'] <= current_near_months_range_upper_limit_for_sell],
                     future_near_month_historical_data[k]['close'], future_current_month_historical_data[k]['close'],
                     next_day_spread['near_far_fut_price'], next_day_spread['current_fut_price'],
                     (future_current_month_historical_data[k]['expiry'] -
                      future_current_month_historical_data[k]['date']).days])

            # spread = future_far_month_historical_data[k]['close'] - future_current_month_historical_data[k]['close']

            # if spread > current_far_months_range_upper_limit_for_buy:
            #     next_day_spread = c_spread.get_next_calendar_spread (future_current_month_historical_data,
            #                                                          future_far_month_historical_data, k,
            #                                                          current_far_months_range_upper_limit_for_sell)
            #
            #     cs_responses.append (
            #         [stock_id, future_far_month_historical_data[k]['date'], next_day_spread['date'],
            #          current_far_months_range_upper_limit_for_buy, current_far_months_range_upper_limit_for_sell,
            #          spread, next_day_spread['spread'],
            #          spread / current_far_months_range_upper_limit_for_buy,
            #          (spread - next_day_spread['spread']) * option_lots[stock_id], margin,
            #          [0, 1][next_day_spread['spread'] <= current_far_months_range_upper_limit_for_sell],
            #          future_far_month_historical_data[k]['close'], future_current_month_historical_data[k]['close'],
            #          next_day_spread['near_far_fut_price'], next_day_spread['current_fut_price'],
            #          (future_current_month_historical_data[k]['expiry'] -
            #           future_current_month_historical_data[k]['date']).days])

            k += 1

    except Exception as e:
        print (traceback.format_exc ())

# cs_responses.sort (key=lambda x: (-x[6]))

if len (cs_responses) > 0:
    cs_responses.insert (0,
                         ['STOCK', 'BUY DATE', 'SELL DATE', 'UPPER RANGE(B)', 'UPPER RANGE(S)', 'SPREAD(B)',
                          'SPREAD(S)', '%DIFF',
                          'EARNING', 'MARGIN',
                          'SUCCESS', 'NEAR/FAR PREM(B)', 'CURRENT PREM(B)', 'NEAR/FAR PREM(S)',
                          'CURRENT PREM(S)', 'EXP. DAYS'])

    gstats.print_statistics (cs_responses, cspread_backtest_excel_location)
else:
    print ('No results.')

print ("---Script executed in %s seconds ---" % (time.time () - start_time))
