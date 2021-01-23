import pickle
import time
import traceback
from datetime import datetime, timedelta

import DerivativeUtils as outil
import GenericStatPrinter as gstats
import OptionStrategies as o_strat
import PatternRecognition as pr
import ScrapUtils as nse_bse
import Utils as util

current_time = time.strftime("%Y_%m_%d#%H_%M_%S")
otm_excel_location = 'F:/Trading_Responses/Option_Strategy_excel_' + str(current_time) + '.xlsx'
diff_between_start_end_date = 365

start_time = time.time()
today_date = datetime.today().date()
start_date = datetime.now() - timedelta(days=diff_between_start_end_date)
end_date = datetime.now() - timedelta(days=1)

no_of_sessions_to_scan_for_volatility = pr.no_of_sessions_to_scan_for_volatility

# stocks_latest_info = nse_bse.get_all_nse_stocks_ids ()
indices = nse_bse.get_indices()
stocks_latest_info = nse_bse.get_nse_fo_stocks()
stocks_latest_info.extend(indices)
# stocks_latest_info = nse_bse.get_indices ()
# stocks_latest_info = nse_bse.get_nifty50_stocks_latest_info ()
# stocks_latest_info = nse_bse.get_nifty100_stocks_latest_info ()
# stocks_latest_info = [{nse_bse.STOCK_ID: 'HDFC', nse_bse.EXCHANGE: nse_bse.NSE}]

last_thurs = outil.get_last_thurday_of_month(today_date.month, today_date.year)

fetched_options = {}
outil.run_fetch_options(stocks_latest_info, fetched_options, today_date)

fetched_stocks_data = {}
stocks_data_obj = pickle.load(open(util.STOCK_DATA_OBJ_FILE, 'rb'))
upstox_api = util.intialize_upstox_api([nse_bse.NSE_FO, nse_bse.NSE, nse_bse.NSE_INDEX])
util.run_fetch_stocks_data(stocks_latest_info, fetched_stocks_data, upstox_api, start_date, end_date, stocks_data_obj,
                           False)

otm_strategy_responses = []
option_lots = nse_bse.get_nse_fo_lots()


def append_to_otm_strategy_response(stock_id, itm_option, otm_option, strategy_name, net_credit, net_debit,
                                    stock_volatility):
    if itm_option.implied_volatility is None:
        itm_option.implied_volatility = 0

    otm_strategy_responses.append(
        [stock_id, strategy_name, itm_option.strike_price, otm_option.strike_price, itm_option.ltp, otm_option.ltp,
         itm_option.spot_price, net_credit, net_debit, stock_volatility, itm_option.implied_volatility,
         otm_option.implied_volatility, [1, 0][stock_volatility > itm_option.implied_volatility],
         outil.is_stock_physically_settled(stock_id), option_lots[stock_id]])


for stock_latest_info in stocks_latest_info:
    try:

        stock_id = outil.get_stock_id(stock_latest_info[nse_bse.STOCK_ID])

        stock_options = fetched_options[stock_id]

        stock_latest_data = fetched_stocks_data[stock_id]
        stock_volatility = outil.get_volatility(outil.get_daily_volatility(
            outil.get_daily_returns(stock_latest_data[-no_of_sessions_to_scan_for_volatility:-1])),
                                                no_of_sessions_to_scan_for_volatility) * 100

        stock_options[:] = [x for x in stock_options if not outil.is_option_without_premium(x.ltp)]

        stock_options[:] = [x for x in stock_options if not outil.is_option_without_oi(x.oi)]

        itm_option, otm_option, strategy_name, net_credit, net_debit = o_strat.get_bear_call_spreads(stock_options)
        if itm_option is not None and otm_option is not None:
            append_to_otm_strategy_response(stock_id, itm_option, otm_option, strategy_name.name, net_credit, net_debit,
                                            stock_volatility)
        else:
            print('None options', itm_option, otm_option)

        itm_option, otm_option, strategy_name, net_credit, net_debit = o_strat.get_bear_put_spreads(stock_options)
        if itm_option is not None and otm_option is not None:
            append_to_otm_strategy_response(stock_id, itm_option, otm_option, strategy_name.name, net_credit, net_debit,
                                            stock_volatility)
        else:
            print('None options', itm_option, otm_option)

        itm_option, otm_option, strategy_name, net_credit, net_debit = o_strat.get_bull_call_spreads(stock_options)
        if itm_option is not None and otm_option is not None:
            append_to_otm_strategy_response(stock_id, itm_option, otm_option, strategy_name.name, net_credit, net_debit,
                                            stock_volatility)
        else:
            print('None options', itm_option, otm_option)

        itm_option, otm_option, strategy_name, net_credit, net_debit = o_strat.get_bull_put_spreads(stock_options)
        if itm_option is not None and otm_option is not None:
            append_to_otm_strategy_response(stock_id, itm_option, otm_option, strategy_name.name, net_credit, net_debit,
                                            stock_volatility)
        else:
            print('None options', itm_option, otm_option)

    except Exception as e:
        print(traceback.format_exc())

if len(otm_strategy_responses) > 0:
    otm_strategy_responses.insert(0,
                                  ['STOCK', 'STRATEGY', 'ITM(STRIKE)', 'OTM(STRIKE)', 'ITM(LTP)', 'OTM(LTP)', 'SPOT',
                                   'NET CREDIT', 'NET DEBIT', 'STOCK VOLA', 'IV(ITM)', 'IV(OTM)', 'IV high',
                                   'PHYSIC. SETT.', 'LOTS'])
    gstats.print_statistics(otm_strategy_responses, otm_excel_location)
else:
    print('No results')
print("---Script executed in %s seconds ---" % (time.time() - start_time))
