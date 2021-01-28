import time
import traceback
import os
import GenericStatPrinter as gstats
import Utils as util
import DerivativeUtils as outil
import csv
from dateutil import parser

current_time = time.strftime ("%Y_%m_%d#%H_%M_%S")
excel_location = 'F:/Trading_Responses/Banknifty_CallSpreadRatio_Backtest_excel_' + str (current_time) + '.xlsx'

files = []
for r, d, f in os.walk('F:/Trading_Responses/Bank_Nifty/'):
    for file in f:
        if '.txt' in file:
            files.append(os.path.join(r, file))

responses = []
files = ['F:/Trading_Responses/Bank_Nifty/BANKNIFTYMar-2018CE_option_csv.txt']
for file in files:
    try:

        with open (file) as csvfile:
            options_data = csv.reader (csvfile, delimiter=',', quotechar='"')

            first_row_excluded = False

            itm = {'strike': None, 'start_date': None}

            otm = {'strike': None, 'start_date': None}

            spot_start_of_month = None
            for option_data in options_data:
                if not first_row_excluded:
                    first_row_excluded = True
                    continue

                curr_date = parser.parse (option_data[1].strip ())
                expiry_date = parser.parse (option_data[2].strip ())

                if spot_start_of_month is None:
                    spot_start_of_month = float (util.remove_non_no_chars (option_data[16].strip ()))

                last_thurs = outil.get_last_thurday_of_month(curr_date.month, curr_date.year)

                if curr_date.month != expiry_date.month or abs((expiry_date - last_thurs).days) > 1 :
                    continue

                strike = float (util.remove_non_no_chars (option_data[4].strip ()))

                if otm['start_date'] is None or otm['start_date'] >= curr_date:
                    if strike > spot_start_of_month and (otm['strike'] is None or (strike - spot_start_of_month) < (otm['strike'] - spot_start_of_month)):
                        otm = {'strike': strike, 'start_date': curr_date, 'start_premium': float (util.remove_non_no_chars (option_data[5].strip ())), 'exit_date': expiry_date}

                if itm['start_date'] is None or itm['start_date'] >= curr_date:
                    if strike < spot_start_of_month and (itm['strike'] is None or (spot_start_of_month - strike) < (spot_start_of_month - itm['strike'])):
                        itm = {'strike': strike, 'start_date': curr_date,'start_premium': float (util.remove_non_no_chars (option_data[5].strip ())), 'exit_date': expiry_date, 'exit_premium': None}

                if abs((expiry_date - curr_date).days) <= 1 and otm['strike'] == strike:
                    otm.update({'exit_premium': float (util.remove_non_no_chars (option_data[8].strip ()))})

                if abs((expiry_date - curr_date).days) <= 1 and itm['strike'] == strike:
                    itm.update({'exit_premium': float (util.remove_non_no_chars (option_data[8].strip ()))})

            pl = (2 * (otm['exit_premium'] - otm['start_premium'])) + (itm['start_premium'] - itm['exit_premium'])
            responses.append([option_data[0], spot_start_of_month, otm['exit_date'], otm['strike'], itm['strike'], otm['start_premium'], itm['start_premium'], otm['exit_premium'], itm['exit_premium'], pl])

    except Exception:
        print (traceback.format_exc () + str(option_data))

responses.insert (0, ['INDEX', 'SPOT', 'MONTH', 'OTM(STRIKE)', 'ITM(STRIKE)', 'OTM(B)', 'ITM(S)', 'OTM(S)', 'ITM(B)', 'P/L'])

gstats.print_statistics(responses, excel_location)