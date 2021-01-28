import csv
from datetime import timedelta

import time
from dateutil import parser

import DerivativeUtils as outil
import GenericStatPrinter as gstats
import PatternRecognition as pr
import ScrapUtils as nse_bse
import Utils as util


def get_stock_data_set(stock_data, date):
    stock_data_subset = []

    for data in stock_data:
        if (date - util.get_date_from_timestamp (data['timestamp'])).days <= 0:
            break

        stock_data_subset.append (data)

    return stock_data_subset


def check_for_duplicate(otm_contract_responses, option_hist_data):
    for res in otm_contract_responses:
        if option_hist_data['symbol'] == res[0] and option_hist_data['strike'] == res[1] and option_hist_data[
            'expiry'] == res[6]:
            return True

    return False


upstox_api = util.intialize_upstox_api ([nse_bse.NSE, nse_bse.NSE_INDEX])

diff_between_start_end_date = 365
no_of_sessions_to_scan_for_volatility = pr.no_of_sessions_to_scan_for_volatility
current_time = time.strftime ("%Y_%m_%d#%H_%M_%S")
otm_excel_location = 'F:/Trading_Responses/OTM_Backtest_excel_' + str (current_time) + '.xlsx'

start_time = time.time()

# stocks_latest_info = nse_bse.get_all_nse_stocks_ids ()
indices = nse_bse.get_indices ()
stocks_latest_info = nse_bse.get_nse_fo_stocks ()
stocks_latest_info.extend (indices)
# stocks_latest_info = nse_bse.get_indices()
# stocks_latest_info = nse_bse.get_nifty50_stocks_latest_info ()
# stocks_latest_info = nse_bse.get_nifty100_stocks_latest_info ()
#stocks_latest_info = [{nse_bse.STOCK_ID: 'NIFTY_BANK', nse_bse.EXCHANGE: nse_bse.NSE_INDEX}]

otm_contract_responses = []

option_lots = nse_bse.get_nse_fo_lots()

for stock_latest_info in stocks_latest_info:
    stock_id = outil.get_stock_id(stock_latest_info[nse_bse.STOCK_ID])

    option_hist_data = []
    with open(outil.OPTION_FILE_LOCATION + stock_id + outil.OPTION_FILE_SUFFIX) as csvfile:
        options_data = csv.reader(csvfile, delimiter=',', quotechar='"')
        row_count = 0

        for option_data in options_data:
            if row_count > 0:
                close = 0
                if util.is_number (util.remove_non_no_chars (option_data[8].strip ())):
                    close = float (util.remove_non_no_chars (option_data[8].strip ()))
                else:
                    print ('Fetching prev close', stock_id, option_data)
                    close = option_hist_data[-1]['close']

                spot = 0
                if util.is_number (util.remove_non_no_chars (option_data[16].strip ())):
                    spot = float (util.remove_non_no_chars (option_data[16].strip ()))
                else:
                    # print ('Fetching prev spot', stock_id, option_data)
                    spot = option_hist_data[-1]['spot']


                option_hist_data.append (
                    {'symbol': option_data[0].strip (), 'close': float (option_data[8].strip ()),
                     'strike': float (option_data[4].strip ()), 'date': parser.parse (option_data[1].strip ()),
                     'expiry': parser.parse (option_data[2].strip ()), 'spot': spot})

            row_count += 1

    start_date = option_hist_data[0]['date'] - timedelta (days=diff_between_start_end_date)
    end_date = option_hist_data[-1]['date'] - timedelta (days=diff_between_start_end_date)
    stock_data = util.get_stock_latest_data (stock_latest_info[nse_bse.STOCK_ID], upstox_api, start_date, end_date,
                                             stock_latest_info[nse_bse.EXCHANGE])

    k = 0
    while k < len(option_hist_data):

        if option_hist_data[k]['strike'] > option_hist_data[k]['spot'] and (
                option_hist_data[k]['expiry'] - option_hist_data[k]['date']).days <= 4:

            if check_for_duplicate (otm_contract_responses, option_hist_data[k]) is False:

                stock_data_set = get_stock_data_set (stock_data, option_hist_data[k]['date'])

                stock_price_range = outil.get_range (stock_data_set[-1]['close'],
                                                     stock_data_set[-no_of_sessions_to_scan_for_volatility:-1],
                                                     (option_hist_data[k]['expiry'] - option_hist_data[k]['date']).days)

                if option_hist_data[k]['strike'] > stock_price_range[0]:

                    i = k
                    while (option_hist_data[i]['expiry'] - option_hist_data[i]['date']).days > 0:
                        i += 1

                    otm_contract_responses.append (
                        [option_hist_data[k]['symbol'], option_hist_data[k]['strike'], option_hist_data[k]['spot'],
                         option_hist_data[k]['date'], stock_price_range[0], option_hist_data[i]['spot'],
                         option_hist_data[i]['date'], option_hist_data[k]['close'],
                         option_hist_data[k]['close'] * option_lots[stock_id],
                         [0, 1][option_hist_data[i]['spot'] <= option_hist_data[k]['strike']]])

        k += 1

otm_contract_responses.insert (0, ['OPTION', 'STRIKE', 'SPOT(B)', 'DATE(B)', '1 SD', 'SPOT(S)', 'DATE(S)', 'PREM',
                                   'EARNING', 'SUCCESS'])

gstats.print_statistics(otm_contract_responses, otm_excel_location)
print("---Script executed in %s seconds ---" % (time.time() - start_time))
