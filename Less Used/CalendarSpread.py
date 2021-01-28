import traceback
from datetime import datetime

import time
from dateutil.relativedelta import relativedelta

import CalculatedCalendarSpreadsLimits as calcul_limit
import CalendarSpreadUtils as clds_utils
import DerivativeUtils as outil
import GenericStatPrinter as gstats
import ScrapUtils as nse_bse
import Utils as util

diff_between_start_end_date = 365
min_return_monthly = 10
current_time = time.strftime ("%Y_%m_%d#%H_%M_%S")
otm_excel_location = 'F:/Trading_Responses/CalendarSpread_excel_' + str (current_time) + '.xlsx'
debug = True

start_time = time.time ()

upstox_api = util.intialize_upstox_api ([nse_bse.NSE_FO])
# upstox_api = None


# futures = nse_bse.get_all_nse_stocks_ids ()
indices = nse_bse.get_indices ()
futures = nse_bse.get_nse_fo_stocks ()
futures.extend (indices)
# futures = nse_bse.get_indices ()
# futures = nse_bse.get_nifty50_stocks_latest_info ()
# futures = nse_bse.get_nifty100_stocks_latest_info ()
# futures = [{nse_bse.STOCK_ID: 'BANKBARODA', nse_bse.EXCHANGE: nse_bse.NSE_FO}]

current_month_last_thurs = outil.get_last_thurday_of_month (datetime.now ().month, datetime.now ().year)

near_month_last_thurs = current_month_last_thurs + relativedelta (months=+1)

far_month_last_thurs = near_month_last_thurs + relativedelta (months=+1)

cs_responses = []

option_lots = nse_bse.get_nse_fo_lots ()

required_margins = outil.get_future_margins (futures)

available_margin = upstox_api.get_balance ()['equity']['available_margin']

# futures[:] = [x for x in futures if required_margins[outil.get_stock_id (x[nse_bse.STOCK_ID])] <= (.95 * available_margin)]

current_month_last_thurs_yr = current_month_last_thurs.strftime ('%g')
near_month_last_thurs_mn = near_month_last_thurs.strftime ('%b').upper ()
far_month_last_thurs_mn = far_month_last_thurs.strftime ('%b').upper ()

fetched_futures = {}
clds_utils.run_fetch_futures (futures, fetched_futures, current_month_last_thurs_yr, near_month_last_thurs_mn,
                              far_month_last_thurs_mn)

i = 0
for future in futures:
    try:
        stock_id = outil.get_stock_id (future[nse_bse.STOCK_ID])
        cs_responses.extend (
            clds_utils.get_calendar_spread_responses (option_lots, stock_id, current_month_last_thurs,
                                                      near_month_last_thurs, far_month_last_thurs,
                                                      fetched_futures[stock_id], required_margins[stock_id], debug))

        if debug:
            i += 1
            print("---Script executed in %s seconds ---" % (time.time() - start_time) + str(i))

    except Exception as e:
        print (traceback.format_exc () + ' in Stock:' + future[nse_bse.STOCK_ID])


cs_responses.sort (key=lambda x: (-x[7]))

for cs_response in cs_responses:
    try:
        stock_id = cs_response[0].split (current_month_last_thurs_yr)[0]

        bids, asks, ltp1, spot = outil.get_all_bids(stock_id, current_month_last_thurs)

        if all(ltp1 < i['price'] for i in asks) is False or True:

            if outil.is_instrument_liquid (bids, asks, clds_utils.future_liquidity_margin,
                                           clds_utils.min_no_of_bids_asks):
                if cs_response[1].split (current_month_last_thurs_yr)[1].startswith (near_month_last_thurs_mn):
                    bids, asks, ltp2 = outil.get_all_bids (stock_id, near_month_last_thurs)

                    if all(ltp2 > i['price'] for i in bids) is False or True:
                        if outil.is_instrument_liquid (bids, asks, clds_utils.future_liquidity_margin,
                                                       clds_utils.min_no_of_bids_asks):
                            if (ltp2 - ltp1) > calcul_limit.calculated_calendar_spread_limits[stock_id][0]:
                                # outil.sell_future (upstox_api, cs_response[1], nse_bse.NSE_FO, ltp2,
                                #                    option_lots[stock_id])
                                # outil.buy_future (upstox_api, cs_response[0], nse_bse.NSE_FO, ltp1, option_lots[stock_id])
                                print ('Executed:', cs_response)
                                print('B,A.L:', bids, asks, ltp1)
                                break
                        else:
                            print ('Illiquid future 2:', bids, asks, stock_id)
                    else:
                        print ('Too high ltp for future 2:', bids, stock_id, ltp2)

                elif cs_response[1].split (current_month_last_thurs_yr)[1].startswith (far_month_last_thurs_mn):
                    bids, asks, ltp2 = outil.get_all_bids (stock_id, far_month_last_thurs)

                    if all(ltp2 > i['price'] for i in bids) is False and True:
                        if outil.is_instrument_liquid (bids, asks, clds_utils.future_liquidity_margin,
                                                       clds_utils.min_no_of_bids_asks):
                            if (ltp2 - ltp1) > calcul_limit.calculated_calendar_spread_limits[stock_id][2]:
                                # outil.sell_future (upstox_api, cs_response[1], nse_bse.NSE_FO, ltp2,
                                #                    option_lots[stock_id])
                                # outil.buy_future (upstox_api, cs_response[0], nse_bse.NSE_FO, ltp1, option_lots[stock_id])
                                print ('Executed:', cs_response)
                                print('B,A.L:', bids, asks, ltp1)
                                break

                        else:
                            print ('Illiquid future 3:', bids, asks, stock_id)
                    else:
                        print ('Too high ltp for future 3:', bids, stock_id, ltp2)
            else:
                print ('Illiquid future 1:', bids, asks, stock_id)
        else:
            print ('Too low ltp for future 1:', asks, stock_id, ltp1)

    except Exception as e:
        print (traceback.format_exc () + ' in Stock:' + stock_id)

if len (cs_responses) > 0:
    cs_responses.insert (0,
                         ['CURRENT FUT', 'NEAR/FAR FUT', 'CURRENT PREM', 'NEAR/FAR PREM', 'UPPER RANGE(B)',
                          'UPPER RANGE(S)', 'SPREAD', '%DIFF',
                          'EARNING', 'MARGIN', 'LOTS'])
    gstats.print_statistics (cs_responses, otm_excel_location)
else:
    print ('No results.')
print ("---Script executed in %s seconds ---" % (time.time () - start_time))
