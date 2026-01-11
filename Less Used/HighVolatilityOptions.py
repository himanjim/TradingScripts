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
high_volat_options_excel_location = 'F:/Trading_Responses/High_Volat_Option_excel_' + str (current_time) + '.xlsx'

start_time = time.time()

today_date = datetime.today().date()
start_date = datetime.now() - timedelta(days=diff_between_start_end_date)
end_date = datetime.now() - timedelta(days=1)

last_thurs = outil.get_last_thurday_of_month (today_date.month, today_date.year)

no_of_days_till_last_thurs = last_thurs.day - today_date.day + 1

upstox_api = util.intialize_upstox_api ([nse_bse.NSE_FO, nse_bse.NSE, nse_bse.NSE_INDEX])

# stocks_latest_info = nse_bse.get_all_nse_stocks_ids ()
indices = nse_bse.get_indices()
stocks_latest_info = nse_bse.get_nse_fo_stocks()
stocks_latest_info.extend(indices)
# stocks_latest_info = nse_bse.get_indices ()
# stocks_latest_info = nse_bse.get_nifty50_stocks_latest_info ()
# stocks_latest_info = nse_bse.get_nifty100_stocks_latest_info ()
# stocks_latest_info = [{nse_bse.STOCK_ID: 'BHEL', nse_bse.EXCHANGE: nse_bse.NSE}]

high_volat_contract_responses = []

option_lots = nse_bse.get_nse_fo_lots()

required_margins = outil.get_future_margins(stocks_latest_info)

if util.is_market_open():
    available_margin = upstox_api.get_balance()['equity']['available_margin']

    stocks_latest_info[:] = [x for x in stocks_latest_info if
                             required_margins[outil.get_stock_id(x[nse_bse.STOCK_ID])] <= (.95 * available_margin)]

stocks_data_obj = pickle.load(open(util.STOCK_DATA_OBJ_FILE, 'rb'))

fetched_options = {}
outil.run_fetch_options (stocks_latest_info, fetched_options, today_date)

fetched_stocks_data = {}
util.run_fetch_stocks_data(stocks_latest_info, fetched_stocks_data, upstox_api, start_date, end_date, stocks_data_obj,
                           False)

for stock_latest_info in stocks_latest_info:
    try:
        stock_latest_data = fetched_stocks_data[stock_latest_info[nse_bse.STOCK_ID]]

        stock_id = outil.get_stock_id (stock_latest_info[nse_bse.STOCK_ID])

        stock_options = fetched_options[stock_id]

        stock_options[:] = [x for x in stock_options if not outil.is_option_without_premium(x.ltp)]

        stock_options[:] = [x for x in stock_options if not outil.is_option_without_oi(x.oi)]

        max_pain = outil.get_max_pain_strike(stock_options)
        pcr = outil.get_pcr(stock_options)

        stock_options[:] = [x for x in stock_options if outil.is_call(x.symbol)]

        stock_options[:] = [x for x in stock_options if
                            (x.implied_volatility is not None and util.is_number(x.implied_volatility))]

        stock_volatility = outil.get_volatility(outil.get_daily_volatility(
            outil.get_daily_returns(stock_latest_data[-no_of_sessions_to_scan_for_volatility:-1])),
                                                no_of_sessions_to_scan_for_volatility)

        # stock_options[:] = [x for x in stock_options if (x.implied_volatility > (stock_volatility * 100))]

        stock_options[:] = [x for x in stock_options if x.liquidity]

        for stock_option in stock_options:
            stock_option.ltp = max (x['price'] for x in stock_option.bids)
            high_volat_contract_responses.append (
                [stock_option.symbol, stock_option.strike_price, stock_option.spot_price, stock_volatility * 100,
                 stock_option.implied_volatility, stock_option.ltp, stock_option.ltp * option_lots[stock_id],
                 stock_option.liquidity, max_pain, pcr, outil.is_call(stock_option.symbol),
                 stock_option.strike_price > max_pain, pcr < outil.lower_pcr_value_for_shorting,
                 required_margins[stock_id], option_lots[stock_id],
                 stock_option.implied_volatility > (stock_volatility * 100)])

    except Exception as e:
        print(traceback.format_exc())

high_volat_contract_responses.sort(key=lambda x: (-((x[1] + x[5]) / x[2])))

if len (high_volat_contract_responses) > 0:
    high_volat_contract_responses.insert (0,
                                          ['OPTION', 'STRIKE', 'SPOT', 'STOCK VOLA', 'IV', 'PREM', 'EARNING', 'LIQUID',
                                           'MAX_PAIN', 'PCR', 'CALL', '> PAIN', 'GOOD PCR', 'MARGIN', 'LOT',
                                           'HIGH. VOLA.'])
    gstats.print_statistics (high_volat_contract_responses, high_volat_options_excel_location)
else:
    print('No results.')
print("---Script executed in %s seconds ---" % (time.time() - start_time))
