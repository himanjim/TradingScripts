import csv
from datetime import datetime

import time

import DerivativeUtils as outil
import GenericStatPrinter as gstats
import Utils as util

current_time = time.strftime ("%Y_%m_%d#%H_%M_%S")
banknifty_excel_location = 'F:/Trading_Responses/BANKNIFTY_backtest_excel_' + str (current_time) + '.xlsx'


def parse_date(date_str):
    return datetime.strptime (date_str, '%d-%b-%Y')


def get_atm_option(options):
    min_diff = None
    atm_option = None

    for option in options:
        if min_diff is None or abs (option['spot'] - option['strike']) < min_diff:
            atm_option = option
            min_diff = abs (option['spot'] - option['strike'])

    return atm_option


start_time = time.time ()

stock_id = 'BANKNIFTY'
min_breakeven = 5

lot = 20
bank_nifty_responses = []
option_ce_hist_data = []
with open (outil.OPTION_FILE_LOCATION + stock_id + 'CE' + outil.OPTION_FILE_SUFFIX) as csvfile:
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

            open_price = 0
            if util.is_number (util.remove_non_no_chars (option_data[5].strip ())):
                open_price = float (util.remove_non_no_chars (option_data[5].strip ()))
            else:
                print ('Fetching prev open', stock_id, option_data)
                open_price = option_ce_hist_data[-1]['open']

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
                {'symbol': option_data[0].strip (), 'open': open_price, 'close': close, 'ltp': ltp,
                 'strike': float (option_data[4].strip ()), 'date': parse_date (option_data[1].strip ()),
                 'expiry': parse_date (option_data[2].strip ()), 'spot': spot})

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

            open_price = 0
            if util.is_number (util.remove_non_no_chars (option_data[5].strip ())):
                open_price = float (util.remove_non_no_chars (option_data[5].strip ()))
            else:
                print ('Fetching prev open', stock_id, option_data)
                open_price = option_pe_hist_data[-1]['open']

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
                {'symbol': option_data[0].strip (), 'open': open_price, 'close': close, 'ltp': ltp,
                 'strike': float (option_data[4].strip ()), 'date': parse_date (option_data[1].strip ()),
                 'expiry': parse_date (option_data[2].strip ()), 'spot': spot})

        row_count += 1

for option_pe in option_pe_hist_data:
    options_ce = [x for x in option_ce_hist_data if
                  x['date'] == option_pe['date'] and x['expiry'] == option_pe['expiry']]
    if len (options_ce) > 0:
        atm_ce_option = get_atm_option (options_ce)
        if atm_ce_option['strike'] == option_pe['strike']:
            pl = atm_ce_option['close'] + option_pe['close'] - atm_ce_option['open'] - option_pe['open'] - min_breakeven
            bank_nifty_responses.append (
                [option_pe['strike'], option_pe['spot'], atm_ce_option['open'], option_pe['open'],
                 atm_ce_option['close'], option_pe['close'], pl, pl * lot, [0, 1][pl > 0], option_pe['date'],
                 option_pe['date']])

            bank_nifty_responses_len = len (bank_nifty_responses)
            if bank_nifty_responses_len > 400:
                break
            if bank_nifty_responses_len % 100 == 0:
                print (bank_nifty_responses_len)
    # else:
    #     print('No CE option for date: %s' %(option_pe['date']))

if len (bank_nifty_responses) > 0:
    bank_nifty_responses.sort (key=lambda x: (-x[6]))
    bank_nifty_responses.insert (0, ['STRIKE', 'SPOT', 'CALL LTP(B)', 'PUT LTP(B)', 'CALL LTP(S)', 'PUT LTP(S)', 'P/L',
                                     'EARNING', 'SUCC', 'BUY DATE', 'SELL DATE'])
    gstats.print_statistics (bank_nifty_responses, banknifty_excel_location)
else:
    print ('No results.')

print ("---Script executed in %s seconds ---" % (time.time () - start_time))
