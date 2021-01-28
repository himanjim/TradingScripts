import csv
import datetime
import math
import time
import traceback
import urllib.parse

import DerivativeUtils as outil
import GenericStatPrinter as gstats
import Utils as util
from dateutil import parser

current_time = time.strftime ("%Y_%m_%d#%H_%M_%S")
excel_location = 'F:/Trading_Responses/BankNifty_Straddle_data_' + str (current_time) + '.xlsx'

bank_nifty_spot = {}
with open ('F:/Trading_Responses/Bank_Nifty/NIFTYBANK.csv') as csvfile:
    spot_data_rows = csv.reader (csvfile, delimiter=',', quotechar='"')

    for spot_data_row in spot_data_rows:
        bank_nifty_spot[parser.parse(spot_data_row[0].strip())] = float(util.remove_non_no_chars(spot_data_row[4].strip()))


def get_bank_nifty_spot(ref_date):
    prev_day = ref_date - datetime.timedelta (days=1)
    while prev_day not in bank_nifty_spot:
        prev_day = prev_day - datetime.timedelta (days=1)

    return bank_nifty_spot[prev_day]


def set_pe_ce_data(options_data):
    data = {}
    first_row_excluded = False

    spot_start_of_month = None
    spot_at_expiry = None
    for option_data in options_data:
        if not first_row_excluded:
            first_row_excluded = True
            continue

        curr_date = parser.parse(option_data[1].strip())
        expiry_date = parser.parse(option_data[2].strip())

        if spot_start_of_month is None:
            spot_start_of_month = get_bank_nifty_spot (curr_date)

        last_thurs = outil.get_last_thurday_of_month(curr_date.month, curr_date.year)

        if curr_date.month != expiry_date.month or abs((expiry_date - last_thurs).days) > 1:
            continue

        if spot_at_expiry is None:
            spot_at_expiry = get_bank_nifty_spot (expiry_date)

        strike = float(util.remove_non_no_chars(option_data[4].strip()))

        if 'expiry_date' not in data:
            data = {'spot': spot_start_of_month, 'spot_at_expiry': spot_at_expiry, 'atm_strike': strike, 'prem_buy':  float(util.remove_non_no_chars(option_data[5].strip())), 'buy_date': curr_date, 'expiry_date': expiry_date, 'dates': {}, 'prem_buy_high':  float(util.remove_non_no_chars(option_data[6].strip())), 'prem_buy_low':  float(util.remove_non_no_chars(option_data[7].strip())), 'prem_buy_close':  float(util.remove_non_no_chars(option_data[8].strip()))}

        elif abs(strike - data['spot']) < abs(data['atm_strike'] - data['spot']) and curr_date <= data['buy_date']:
            data.update({'atm_strike': strike, 'prem_buy':  float(util.remove_non_no_chars(option_data[5].strip())), 'buy_date': curr_date, 'prem_buy_high':  float(util.remove_non_no_chars(option_data[6].strip())), 'prem_buy_low':  float(util.remove_non_no_chars(option_data[7].strip())), 'prem_buy_close':  float(util.remove_non_no_chars(option_data[8].strip()))})

        if expiry_date == curr_date and data['atm_strike'] == strike:
            data.update({'exit_premium': float(util.remove_non_no_chars(option_data[8].strip())), 'exit_premium_high':  float(util.remove_non_no_chars(option_data[6].strip())), 'exit_premium_low':  float(util.remove_non_no_chars(option_data[7].strip())), 'exit_premium_open':  float(util.remove_non_no_chars(option_data[5].strip()))})

        if data['atm_strike'] == strike and curr_date != data['buy_date'] and curr_date != data['expiry_date']:
            data['dates'].update({curr_date: {'prem_open':  float(util.remove_non_no_chars(option_data[5].strip())), 'prem_high':  float(util.remove_non_no_chars(option_data[6].strip())), 'prem_low':  float(util.remove_non_no_chars(option_data[7].strip())), 'prem_close':  float(util.remove_non_no_chars(option_data[8].strip()))}})

    return data


FILES_LOCATION = 'F:/Trading_Responses/Bank_Nifty/'
today = datetime.date.today()
first = today.replace(day=1)

responses = []
stocks = ['BANKNIFTY']
for stock_id in stocks:
    while True:
        try:
            stock_id = urllib.parse.quote (stock_id)

            lastMonth = first - datetime.timedelta (days=1)

            if lastMonth.year == 2010:
                break

            first = lastMonth.replace (day=1)

            with open (FILES_LOCATION + urllib.parse.unquote (stock_id) + lastMonth.strftime ("%b-%Y") + 'CE' + outil.OPTION_FILE_SUFFIX) as csvfile:
                ce_data = set_pe_ce_data(csv.reader (csvfile, delimiter=',', quotechar='"'))

            with open (FILES_LOCATION + urllib.parse.unquote (stock_id) + lastMonth.strftime ("%b-%Y") + 'PE' + outil.OPTION_FILE_SUFFIX) as csvfile:
                pe_data = set_pe_ce_data (csv.reader (csvfile, delimiter=',', quotechar='"'))

            responses.append([stock_id, ce_data['spot'], ce_data['spot_at_expiry'], ce_data['atm_strike']])

            responses.append ([ce_data['buy_date'], ce_data['prem_buy'], pe_data['prem_buy'], '', '', '', '', ce_data['prem_buy_high'], ce_data['prem_buy_low'], ce_data['prem_buy_close'], '', pe_data['prem_buy_high'], pe_data['prem_buy_low'], pe_data['prem_buy_close']])

            for date, c_cent_data in ce_data['dates'].items():
                p_cent_data = pe_data['dates'][date]
                responses.append ([date, c_cent_data['prem_close'], p_cent_data['prem_close'], '', '', '', '', c_cent_data['prem_high'], c_cent_data['prem_low'], c_cent_data['prem_open'], '', p_cent_data['prem_high'], p_cent_data['prem_low'], p_cent_data['prem_open']])

            responses.append ([ce_data['expiry_date'], ce_data['exit_premium'], pe_data['exit_premium'], '', '', '', '', ce_data['exit_premium_high'], ce_data['exit_premium_low'], ce_data['exit_premium_open'], '', pe_data['exit_premium_high'], pe_data['exit_premium_low'], pe_data['exit_premium_open']])

            responses.append(['-', '-', '-'])
        except Exception:
            print (traceback.format_exc (), stock_id, lastMonth.strftime ("%b-%Y"))

responses.insert (0, ['DATE', 'CE', 'PE', '', '', '', '', 'CE(H)', 'CE(L)', 'CE(O/C)', '', 'PE(H)', 'PE(L)', 'PE(O/C)'])
gstats.print_statistics(responses, excel_location)
