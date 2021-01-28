import csv
import datetime
import time
import traceback
import urllib.parse

import DerivativeUtils as outil
import GenericStatPrinter as gstats
import Utils as util
from dateutil import parser

current_time = time.strftime ("%Y_%m_%d#%H_%M_%S")
excel_location = 'F:/Trading_Responses/BankNiftyATM_Backtest_excel_' + str (current_time) + '.xlsx'

bank_nifty_spot = {}
with open ('F:/Trading_Responses/Bank_Nifty/NIFTYBANK.csv') as csvfile:
    spot_data_rows = csv.reader (csvfile, delimiter=',', quotechar='"')

    for spot_data_row in spot_data_rows:
        bank_nifty_spot[parser.parse(spot_data_row[0].strip())] = float(util.remove_non_no_chars(spot_data_row[1].strip()))


def set_pe_ce_data(options_data):
    data = {}
    first_row_excluded = False

    for option_data in options_data:
        if not first_row_excluded:
            first_row_excluded = True
            continue

        curr_date = parser.parse(option_data[1].strip())
        expiry_date = parser.parse(option_data[2].strip())

        spot = bank_nifty_spot[curr_date]

        last_thurs = outil.get_last_thurday_of_month(curr_date.month, curr_date.year)

        if curr_date.month != expiry_date.month or abs((expiry_date - last_thurs).days) > 1:
            continue

        strike = float(util.remove_non_no_chars(option_data[4].strip()))

        if curr_date not in data :
            data[curr_date] = {'spot': spot, 'atm_strike': strike, 'prem_open':  float(util.remove_non_no_chars(option_data[5].strip())), 'prem_high':  float(util.remove_non_no_chars(option_data[6].strip())), 'prem_low':  float(util.remove_non_no_chars(option_data[7].strip())), 'prem_close':  float(util.remove_non_no_chars(option_data[8].strip())), 'buy_date': curr_date, 'expiry_date': expiry_date}

        elif abs(strike - data[curr_date]['spot']) < abs(data[curr_date]['atm_strike'] - data[curr_date]['spot']):
            data[curr_date].update({'atm_strike': strike, 'prem_open': float (util.remove_non_no_chars (option_data[5].strip ())),  'prem_high': float (util.remove_non_no_chars (option_data[6].strip ())), 'prem_low': float (util.remove_non_no_chars (option_data[7].strip ())),  'prem_close': float (util.remove_non_no_chars (option_data[8].strip ()))})

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

            if lastMonth.year == 2014:
                break

            first = lastMonth.replace (day=1)

            with open (FILES_LOCATION + urllib.parse.unquote (stock_id) + lastMonth.strftime ("%b-%Y") + 'CE' + outil.OPTION_FILE_SUFFIX) as csvfile:
                ce_data = set_pe_ce_data(csv.reader (csvfile, delimiter=',', quotechar='"'))

            with open (FILES_LOCATION + urllib.parse.unquote (stock_id) + lastMonth.strftime ("%b-%Y") + 'PE' + outil.OPTION_FILE_SUFFIX) as csvfile:
                pe_data = set_pe_ce_data (csv.reader (csvfile, delimiter=',', quotechar='"'))

            for date, ce_d in ce_data.items():
                responses.append ([stock_id, ce_d['spot'], ce_d['atm_strike'], ce_d['buy_date'], ce_d['expiry_date'], ce_d['prem_open'], ce_d['prem_high'], ce_d['prem_low'], ce_d['prem_close'], pe_data[date]['atm_strike'], pe_data[date]['prem_open'], pe_data[date]['prem_high'], pe_data[date]['prem_low'], pe_data[date]['prem_close']])

        except Exception:
            print (traceback.format_exc (), stock_id, lastMonth.strftime ("%b-%Y"))

responses.insert (0, ['STOCK', 'SPOT', 'CE(STRIKE)', 'BUY DATE', 'EXPIRY', 'CE(OPEN)', 'CE(HIGH)', 'CE(LOW)', 'CE(CLOSE)', 'PE(STRIKE)', 'PE(OPEN)', 'PE(HIGH)', 'PE(LOW)', 'PE(CLOSE)'])

gstats.print_statistics(responses, excel_location)
