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
excel_location = 'F:/Trading_Responses/HDFCBANK_Straddle_Backtest_excel_' + str (current_time) + '.xlsx'

bank_nifty_spot = {}
# with open ('F:/Trading_Responses/Bank_Nifty/NIFTYBANK.csv') as csvfile:
#     spot_data_rows = csv.reader (csvfile, delimiter=',', quotechar='"')
#
#     for spot_data_row in spot_data_rows:
#         bank_nifty_spot[parser.parse(spot_data_row[0].strip())] = float(util.remove_non_no_chars(spot_data_row[4].strip()))


def get_bank_nifty_spot(ref_date):
    prev_day = ref_date - datetime.timedelta (days=1)
    while prev_day not in bank_nifty_spot:
        prev_day = prev_day - datetime.timedelta (days=1)

    return bank_nifty_spot[prev_day]


def set_pl(def_pl, pl_limit, ce_d, pe_d):
    new_pl = def_pl
    if ce_d['min_prem'] is not None and (ce_d['min_prem'] + pe_d['min_prem']) < ((1 - pl_limit) * (ce_d['prem_buy'] + pe_d['prem_buy'])):
        new_pl = -pl_limit * (ce_d['prem_buy'] + pe_d['prem_buy'])

    return new_pl


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

        # if spot_start_of_month is None:
        #     spot_start_of_month = get_bank_nifty_spot (curr_date)

        if spot_start_of_month is None:
            spot_start_of_month = float(util.remove_non_no_chars(option_data[16].strip()))

        last_thurs = outil.get_last_thurday_of_month(curr_date.month, curr_date.year)

        if curr_date.month != expiry_date.month or abs((expiry_date - last_thurs).days) > 1:
            continue

        # if spot_at_expiry is None:
        #     spot_at_expiry = get_bank_nifty_spot (expiry_date)

        if spot_at_expiry is None and curr_date == expiry_date:
            spot_at_expiry = float(util.remove_non_no_chars(option_data[16].strip()))

        strike = float(util.remove_non_no_chars(option_data[4].strip()))

        if 'expiry_date' not in data:
            data = {'spot': spot_start_of_month, 'spot_at_expiry': spot_at_expiry, 'atm_strike': strike, 'prem_buy':  float(util.remove_non_no_chars(option_data[5].strip())), 'buy_date': curr_date, 'expiry_date': expiry_date, 'dates': {}}

        elif abs(strike - data['spot']) < abs(data['atm_strike'] - data['spot']) and curr_date <= data['buy_date']:
            data.update({'atm_strike': strike, 'prem_buy':  float(util.remove_non_no_chars(option_data[5].strip())), 'buy_date': curr_date})

        if expiry_date == curr_date and data['atm_strike'] == strike:
            data.update({'exit_premium': float(util.remove_non_no_chars(option_data[8].strip()))})

        if data['atm_strike'] == strike and curr_date != data['buy_date'] and curr_date != data['expiry_date']:
            data['dates'].update({curr_date: float(util.remove_non_no_chars(option_data[8].strip()))})

    return data


FILES_LOCATION = 'F:/Trading_Responses/EQ_Options_Data_for_Straddle/'
today = datetime.date.today()
first = today.replace(day=1)

responses = []
stocks = ['HDFCBANK']
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

            # for date, prem in ce_data['dates'].items ():
            #     if (ce_data['prem_buy'] + pe_data['prem_buy']) > 1000:
            #         if (ce_data['dates'][date] + pe_data['dates'][date]) <= 1000:
            #             ce_data['prem_buy'] = ce_data['dates'][date]
            #             pe_data['prem_buy'] = pe_data['dates'][date]
            #             ce_data['buy_date'] = pe_data['buy_date'] = date
            #             break

            response = [stock_id, ce_data['spot'], ce_data['spot_at_expiry'], ce_data['atm_strike'], ce_data['expiry_date'], ce_data['buy_date'], ce_data['prem_buy'], pe_data['prem_buy'], ce_data['prem_buy'] + pe_data['prem_buy'], ce_data['exit_premium'], pe_data['exit_premium'], ce_data['exit_premium'] + pe_data['exit_premium'], 100]

            max_prem = -math.inf
            min_prem = math.inf
            min_prem_date = None

            ce_data['min_prem'] = pe_data['min_prem'] = pe_data['max_prem'] = ce_data['max_prem'] = min_prem_date = None
            for date, prem in ce_data['dates'].items():
                if (prem + pe_data['dates'][date]) > max_prem and ce_data['buy_date'] < date <= ce_data['expiry_date']:
                    max_prem = prem + pe_data['dates'][date]
                    ce_data['max_prem'] = prem
                    pe_data['max_prem'] = pe_data['dates'][date]

                if (prem + pe_data['dates'][date]) < min_prem and ce_data['buy_date'] < date <= ce_data['expiry_date']:
                    min_prem = prem + pe_data['dates'][date]
                    ce_data['min_prem'] = prem
                    pe_data['min_prem'] = pe_data['dates'][date]
                    min_prem_date = date

            pl = (ce_data['exit_premium'] + pe_data['exit_premium']) - (ce_data['prem_buy'] + pe_data['prem_buy'])

            pl_20 = set_pl(pl, .2, ce_data, pe_data)
            pl_30 = set_pl (pl, .3, ce_data, pe_data)
            pl_40 = set_pl (pl, .4, ce_data, pe_data)
            pl_50 = set_pl (pl, .5, ce_data, pe_data)

            response.extend([ce_data['min_prem'], pe_data['min_prem'], min_prem_date, ce_data['min_prem'] + pe_data['min_prem'], ce_data['max_prem'], pe_data['max_prem'], ce_data['max_prem'] + pe_data['max_prem'], pl, pl_20, pl_30, pl_40, pl_50])

            responses.append(response)
        except Exception:
            print (traceback.format_exc (), stock_id, lastMonth.strftime ("%b-%Y"))

responses.insert (0, ['STOCK', 'SPOT', 'SPOT(EXP)','STRIKE', 'EXPIRY', 'BUY DATE', 'CE(B)', 'PE(B)', 'SUM', 'CE(E)', 'PE(E)', 'SUM', 'LOTS', 'CE(MIN)', 'PE(MIN)', 'MIN DATE', 'SUM', 'CE(MAX)', 'PE(MAX)', 'SUM', 'PL', 'PL20', 'PL30', 'PL40', 'PL50'])

gstats.print_statistics(responses, excel_location)
