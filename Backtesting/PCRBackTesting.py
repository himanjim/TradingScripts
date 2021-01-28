import csv
import traceback
from datetime import datetime

import time

import DerivativeUtils as outil
import GenericStatPrinter as gstats
import ScrapUtils as nse_bse
import Utils as util


def parse_date(date_str):
    return datetime.strptime(date_str, '%d-%b-%Y')


def get_atm_option(options):
    min_diff = None
    atm_option = None

    for option in options:
        if min_diff is None or abs (option['spot'] - option['strike']) < min_diff:
            atm_option = option
            min_diff = abs (option['spot'] - option['strike'])

    return atm_option


current_time = time.strftime ("%Y_%m_%d#%H_%M_%S")
pcp_excel_location = 'F:/Trading_Responses/PCP_backtest_excel_' + str(current_time) + '.xlsx'

start_time = time.time ()

# stocks_latest_info = nse_bse.get_all_nse_stocks_ids ()
# indices = nse_bse.get_indices ()
stocks_latest_info = nse_bse.get_nse_fo_stocks ()
# stocks_latest_info.extend (indices)
# stocks_latest_info = nse_bse.get_indices ()
# stocks_latest_info = nse_bse.get_nifty50_stocks_latest_info ()
# stocks_latest_info = nse_bse.get_nifty100_stocks_latest_info ()
# stocks_latest_info = [{nse_bse.STOCK_ID: 'HDFCBANK', nse_bse.EXCHANGE: nse_bse.NSE}]


pcr_responses = []

option_lots = nse_bse.get_nse_fo_lots ()

for stock_latest_info in stocks_latest_info:
    try:
        stock_id = outil.get_stock_id (stock_latest_info[nse_bse.STOCK_ID])

        option_ce_hist_data = []
        with open(outil.OPTION_FILE_LOCATION + stock_id + 'CE' + outil.OPTION_FILE_SUFFIX) as csvfile:
            options_data = csv.reader (csvfile, delimiter=',', quotechar='"')
            row_count = 0

            for option_data in options_data:
                if row_count > 0:
                    close = 0
                    if util.is_number (util.remove_non_no_chars (option_data[8].strip ())):
                        close = float (util.remove_non_no_chars (option_data[8].strip ()))
                    else:
                        print ('Fetching prev close', stock_id, option_data)
                        close = option_ce_hist_data[-1]['close']

                    ltp = 0
                    if util.is_number (util.remove_non_no_chars (option_data[9].strip ())):
                        ltp = float (util.remove_non_no_chars (option_data[9].strip ()))

                    spot = 0
                    if util.is_number (util.remove_non_no_chars (option_data[16].strip ())):
                        spot = float (util.remove_non_no_chars (option_data[16].strip ()))
                    else:
                        # print ('Fetching prev spot', stock_id, option_data)
                        spot = option_ce_hist_data[-1]['spot']

                    option_ce_hist_data.append (
                        {'symbol': option_data[0].strip (), 'close': close, 'ltp': ltp,
                         'strike': float(option_data[4].strip()), 'date': parse_date(option_data[1].strip()),
                         'expiry': parse_date(option_data[2].strip()), 'spot': spot})

                row_count += 1

        option_pe_hist_data = []
        with open (outil.OPTION_FILE_LOCATION + stock_id + 'PE' + outil.OPTION_FILE_SUFFIX) as csvfile:
            options_data = csv.reader (csvfile, delimiter=',', quotechar='"')
            row_count = 0

            for option_data in options_data:
                if row_count > 0:
                    close = 0
                    if util.is_number (util.remove_non_no_chars (option_data[8].strip ())):
                        close = float (util.remove_non_no_chars (option_data[8].strip ()))
                    else:
                        print ('Fetching prev close', stock_id, option_data)
                        close = option_pe_hist_data[-1]['close']

                    ltp = 0
                    if util.is_number (util.remove_non_no_chars (option_data[9].strip ())):
                        ltp = float (util.remove_non_no_chars (option_data[9].strip ()))

                    spot = 0
                    if util.is_number (util.remove_non_no_chars (option_data[16].strip ())):
                        spot = float (util.remove_non_no_chars (option_data[16].strip ()))
                    else:
                        # print ('Fetching prev spot', stock_id, option_data)
                        spot = option_pe_hist_data[-1]['spot']

                    option_pe_hist_data.append (
                        {'symbol': option_data[0].strip (), 'close': close, 'ltp': ltp,
                         'strike': float(option_data[4].strip()), 'date': parse_date(option_data[1].strip()),
                         'expiry': parse_date(option_data[2].strip()), 'spot': spot})

                row_count += 1

        future_current_month_historical_data = []
        with open (outil.FUTURE_FILE_LOCATION + stock_id + outil.FUTURE_FILE_SUFFIX) as csvfile:
            futures_data = csv.reader (csvfile, delimiter=',', quotechar='"')
            row_count = 0

            for future_data in futures_data:
                if row_count > 0:
                    spot = 0
                    if util.is_number (util.remove_non_no_chars (future_data[13].strip ())):
                        spot = float (util.remove_non_no_chars (future_data[13].strip ()))
                    else:
                        # print ('Fetching prev spot', stock_id, option_data)
                        spot = future_current_month_historical_data[-1]['spot']

                    data_set = {'close': float (future_data[6].strip ()), 'high': float (future_data[4].strip ()),
                                'low': float (future_data[5].strip ()), 'ltp': float (future_data[7].strip ()),
                                'spot': spot,
                                'date': parse_date(future_data[1].strip()),
                                'expiry': parse_date(future_data[2].strip())}
                    if row_count % 3 == 1:
                        future_current_month_historical_data.append (data_set)

                row_count += 1

        pivot = 0
        for future in future_current_month_historical_data:
            options_ce = [x for x in option_ce_hist_data if
                          x['date'] == future['date'] and x['expiry'] == future['expiry']]
            # atm_option_ce = get_atm_option (options_ce)

            options_pe = [x for x in option_pe_hist_data if
                          x['date'] == future['date'] and x['expiry'] == future['expiry']]
            # atm_option_pe = get_atm_option (options_pe)

            for option in options_ce:
                atm_option_ce = option
                atm_option_pe = [x for x in options_pe if x['strike'] == atm_option_ce['strike']][0]

                if atm_option_ce is not None and atm_option_pe is not None and atm_option_pe['ltp'] > 0 and \
                        atm_option_ce[
                            'ltp'] > 0 and future['ltp'] > 0:

                    net_premium = atm_option_pe['ltp'] - atm_option_ce['ltp'] + future['ltp'] - atm_option_ce['strike']

                    earning = option_lots[stock_id] * net_premium

                    if earning > 100:

                        expiry_future = [x for x in future_current_month_historical_data if
                                         x['date'] == future['expiry']]

                        if len(expiry_future) > 0:
                            expiry_spot = expiry_future[0]['spot']

                            expiry_premium = future['ltp'] - expiry_spot + (
                                [0, expiry_spot - atm_option_ce['strike']][expiry_spot > atm_option_ce['strike']]) - \
                                             atm_option_ce['ltp'] + ([expiry_spot - atm_option_pe['strike'], 0][
                                expiry_spot > atm_option_pe['strike']]) + atm_option_pe['ltp']

                            if round(expiry_premium, 2) != round(net_premium, 2):
                                print('Not matching premiums:', net_premium, expiry_premium, stock_id)

                            if atm_option_ce['date'] != future['date'] or atm_option_pe['date'] != future['date']:
                                print('Dates mismatch:', atm_option_ce['date'], atm_option_pe['date'], future['date'])

                            pcr_responses.append(
                                [stock_id, atm_option_pe['strike'], atm_option_ce['ltp'], atm_option_pe['ltp'],
                                 future['ltp'], atm_option_ce['spot'],
                                 earning, future['date'], future['expiry']])

                else:
                    print('No options:', future['date'], stock_id)

            pivot += 1
    except Exception as e:
        print (str (traceback.format_exc ()) + '\nError in stock:' + stock_id)

if len (pcr_responses) > 0:
    pcr_responses.sort(key=lambda x: (-x[6]))
    pcr_responses.insert(0, ['STOCK ID', 'STRIKE', 'CALL LTP', 'PUT LTP', 'FUT LTP', 'STOCK LTP', 'EARNING', 'BUY DATE',
                             'SELL DATE'])
    gstats.print_statistics(pcr_responses, pcp_excel_location)
else:
    print ('No results.')

print ("---Script executed in %s seconds ---" % (time.time () - start_time))
