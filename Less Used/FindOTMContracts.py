import pickle
import time
import traceback
from datetime import datetime, timedelta

import DerivativeUtils as outil
import GenericStatPrinter as gstats
import PatternRecognition as pr
import ScrapUtils as nse_bse
import Utils as util

diff_between_start_end_date = 365
min_return_monthly = 10
no_of_sessions_to_scan_for_volatility = pr.no_of_sessions_to_scan_for_volatility
current_time = time.strftime ("%Y_%m_%d#%H_%M_%S")
otm_excel_location = 'F:/Trading_Responses/OTM_excel_' + str(current_time) + '.xlsx'
max_loss_to_bear = 1500

start_time = time.time ()

upstox_api = util.intialize_upstox_api ([nse_bse.NSE_FO, nse_bse.NSE, nse_bse.NSE_INDEX])

today_date = datetime.today ().date ()
start_date = datetime.now () - timedelta (days=diff_between_start_end_date)
end_date = datetime.now () - timedelta (days=1)

# stocks_latest_info = nse_bse.get_all_nse_stocks_ids ()
indices = nse_bse.get_indices ()
stocks_latest_info = nse_bse.get_nse_fo_stocks ()
stocks_latest_info.extend (indices)
# stocks_latest_info = nse_bse.get_indices ()
# stocks_latest_info = nse_bse.get_nifty50_stocks_latest_info ()
# stocks_latest_info = nse_bse.get_nifty100_stocks_latest_info ()
# stocks_latest_info = [{nse_bse.STOCK_ID: 'JETAIRWAYS', nse_bse.EXCHANGE: nse_bse.NSE_INDEX}]

last_thurs = outil.get_last_thurday_of_month (today_date.month, today_date.year)

no_of_days_till_last_thurs = last_thurs.day - today_date.day + 1

required_margins = outil.get_future_margins(stocks_latest_info)

if util.is_market_open():
    available_margin = upstox_api.get_balance()['equity']['available_margin']

    stocks_latest_info[:] = [x for x in stocks_latest_info if
                             required_margins[outil.get_stock_id(x[nse_bse.STOCK_ID])] <= (.95 * available_margin)]

option_lots = nse_bse.get_nse_fo_lots()

fetched_options = {}
outil.run_fetch_options (stocks_latest_info, fetched_options, today_date)

stocks_data_obj = pickle.load (open (util.STOCK_DATA_OBJ_FILE, 'rb'))
fetched_stocks_data = {}
util.run_fetch_stocks_data (stocks_latest_info, fetched_stocks_data, upstox_api, start_date, end_date, stocks_data_obj,
                            False)

otm_contract_responses = []

for stock_latest_info in stocks_latest_info:
    try:

        stock_latest_data = fetched_stocks_data[stock_latest_info[nse_bse.STOCK_ID]]

        stock_id = outil.get_stock_id (stock_latest_info[nse_bse.STOCK_ID])

        stock_price_range = outil.get_range (stock_latest_data[-1]['close'],
                                             stock_latest_data[-no_of_sessions_to_scan_for_volatility:-1],
                                             no_of_days_till_last_thurs)

        stock_options = fetched_options[stock_id]

        stock_options[:] = [x for x in stock_options if not outil.is_option_without_premium(x.ltp)]

        stock_options[:] = [x for x in stock_options if not outil.is_option_without_oi (x.oi)]

        resistance = max(stock_options, key=lambda x: x.oi).strike_price

        max_pain = outil.get_max_pain_strike (stock_options)
        pcr = outil.get_pcr (stock_options)

        stock_options[:] = [x for x in stock_options if outil.is_call (x.symbol)]

        stock_options[:] = [x for x in stock_options if
                            outil.is_option_outside_1_sd (stock_price_range, x.strike_price)]

        stock_options[:] = [x for x in stock_options if outil.is_otm (x.symbol, x.strike_price, x.spot_price)]

        stock_options[:] = [x for x in stock_options if x.liquidity]

        stock_volatility = outil.get_volatility (outil.get_daily_volatility (
            outil.get_daily_returns (stock_latest_data[-no_of_sessions_to_scan_for_volatility:-1])),
            no_of_sessions_to_scan_for_volatility)

        for stock_option in stock_options:
            option_implied_volatility = 0
            if stock_option.implied_volatility is not None and util.is_number (stock_volatility):
                option_implied_volatility = float (stock_option.implied_volatility)

            stock_option.ltp = max(x['price'] for x in stock_option.bids)

            otm_contract_responses.append (
                [stock_option.symbol, stock_option.strike_price, stock_option.spot_price, stock_price_range[0],
                 stock_volatility * 100, option_implied_volatility, stock_option.ltp,
                 stock_option.ltp * option_lots[stock_id], required_margins[stock_id], stock_option.liquidity, max_pain,
                 pcr, outil.is_call(stock_option.symbol), stock_option.strike_price > max_pain,
                 option_implied_volatility > (stock_volatility * 100), pcr < outil.lower_pcr_value_for_shorting,
                 ((stock_option.strike_price + stock_option.ltp - stock_option.spot_price) / stock_option.spot_price),
                 resistance, stock_option.strike_price > resistance, outil.is_stock_physically_settled(stock_id),
                 option_lots[stock_id]])

    except Exception as e:
        print (traceback.format_exc ())

otm_contract_responses.sort(key=lambda x: (-x[16]))


if len (otm_contract_responses) > 0:
    otm_contract_responses.insert (0,
                                   ['OPTION', 'STRIKE', 'SPOT', '1 SD', 'STOCK VOLA', 'IV', 'PREM', 'EARNING', 'MARGIN',
                                    'LIQUID', 'MAX_PAIN', 'PCR', 'CALL', '> PAIN', 'IV high', 'GOOD PCR', 'DEVI',
                                    'RESIS', '>RESIS',
                                    'PHYSIC. SETT.', 'LOTS'])
    gstats.print_statistics (otm_contract_responses, otm_excel_location)
else:
    print ('No results')
print ("---Script executed in %s seconds ---" % (time.time () - start_time))

