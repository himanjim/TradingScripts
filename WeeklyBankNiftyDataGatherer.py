import csv
from datetime import datetime, timedelta
import time, math
import traceback

import DerivativeUtils as outil
import GenericStatPrinter as gstats
import Utils as util
from dateutil import parser

SPOT = 'BANKNIFTY'
DATE_FORMAT = '%b-%Y'
FILES_LOCATION = 'F:/Trading_Responses/Bank_Nifty/'

current_time = time.strftime ("%Y_%m_%d#%H_%M_%S")
excel_location = 'F:/Trading_Responses/Weekly_BankNifty_Data_' + str (current_time) + '.xlsx'

max_capital_per_option = 50000

bank_nifty_spot = {}
with open ('F:/Trading_Responses/Bank_Nifty/NIFTYBANK.csv') as csvfile:
    spot_data_rows = csv.reader (csvfile, delimiter=',', quotechar='"')

    for spot_data_row in spot_data_rows:
        bank_nifty_spot[parser.parse(spot_data_row[0].strip()).date()] = {'open': float(util.remove_non_no_chars(spot_data_row[1].strip())), 'high': float(util.remove_non_no_chars(spot_data_row[2].strip())), 'low': float(util.remove_non_no_chars(spot_data_row[3].strip())), 'close': float(util.remove_non_no_chars(spot_data_row[4].strip()))}


def get_bank_nifty_spot(ref_date):
    while ref_date not in bank_nifty_spot:
        ref_date = ref_date - timedelta (days=1)

    return ref_date, bank_nifty_spot[ref_date]


def set_pe_ce_data(options_data, ref_expiry_date, spot, option_type):

    first_row_excluded = False

    atm_strike = int(round(spot['open'] / 100.0) * 100)

    upper_strike_type = ['ITM', 'OTM'][option_type == 'CE']
    lower_strike_type = ['OTM', 'ITM'][option_type == 'CE']

    strikes = {atm_strike - 200: lower_strike_type + '2', atm_strike - 100: lower_strike_type + '1', atm_strike: 'ATM', atm_strike + 100: upper_strike_type + '1', atm_strike + 200: upper_strike_type + '2'}

    data = {}

    for option_data in options_data:
        if not first_row_excluded:
            first_row_excluded = True
            continue

        curr_date = parser.parse(option_data[1].strip()).date()
        expiry_date = parser.parse(option_data[2].strip()).date()

        if ref_expiry_date != expiry_date or ref_expiry_date != curr_date:
            continue

        strike = float(util.remove_non_no_chars(option_data[4].strip()))

        if strike not in strikes.keys():
            continue

        data.update({strike: {'prem_open':  float(util.remove_non_no_chars(option_data[5].strip())), 'prem_high':  float(util.remove_non_no_chars(option_data[6].strip())), 'prem_low':  float(util.remove_non_no_chars(option_data[7].strip())), 'prem_close':  float(util.remove_non_no_chars(option_data[8].strip())), 'strike_type': strikes[strike]}})

    return data


def get_pl(sp, bp, lots):
    pl = round(lots * (sp - bp))
    return [pl, -25000][pl < -25000]


def calc_pl(open, high, low, close):
    if high < (2 * open) and low < 1:
        return [-25000, -25000, -25000]

    lots_open = max_capital_per_option / open

    lots_low = max_capital_per_option / low

    if low < 1 or close < open:
        lots_low = 0

    return [get_pl(close, open, lots_open), get_pl(high, open, lots_open), get_pl(high, low, lots_low)]


start_date = datetime (2020, 5, 7).date ()
end_date = datetime (2020, 6, 25).date ()

responses = []

while start_date <= end_date:
    try:
        expiry_date, spot = get_bank_nifty_spot(start_date)

        prev_day, spot_prev = get_bank_nifty_spot (expiry_date - timedelta (days=1))

        start_date = start_date + timedelta (days=7)

        with open (FILES_LOCATION + SPOT + expiry_date.strftime (DATE_FORMAT) + 'CE' + outil.OPTION_FILE_SUFFIX) as csvfile:
            ce_data = set_pe_ce_data (csv.reader (csvfile, delimiter=',', quotechar='"'), expiry_date, spot, 'CE')

        with open (FILES_LOCATION + SPOT + expiry_date.strftime (DATE_FORMAT) + 'PE' + outil.OPTION_FILE_SUFFIX) as csvfile:
            pe_data = set_pe_ce_data (csv.reader (csvfile, delimiter=',', quotechar='"'), expiry_date, spot, 'PE')

        responses.append ([prev_day, spot_prev['open'], spot_prev['high'], spot_prev['low'], spot_prev['close'], round(((spot_prev['high'] - spot_prev['low']) / spot_prev['close']) * 100, 2)])
        responses.append ([expiry_date, spot['open'], spot['high'], spot['low'], spot['close']])

        for strike in sorted(ce_data):
            ce_d = ce_data[strike]
            response = [strike, ce_d['prem_open'], ce_d['prem_high'], ce_d['prem_low'], ce_d['prem_close'], ce_d['strike_type']]
            for p_strike, value in pe_data.items():
                if value['strike_type'] == ce_data[strike]['strike_type']:
                    pe_d = value
                    response.extend ([p_strike, pe_d['prem_open'], pe_d['prem_high'], pe_d['prem_low'], pe_d['prem_close'], '-'])
                    break

            response.extend (calc_pl(ce_d['prem_open'], ce_d['prem_high'], ce_d['prem_low'], ce_d['prem_close']))

            response.append('-')

            response.extend (calc_pl (pe_d['prem_open'], pe_d['prem_high'], pe_d['prem_low'], pe_d['prem_close']))

            responses.append (response)

        responses.append(['-', '-', '-'])
    except Exception:
        print (traceback.format_exc (), str(start_date))

responses.insert (0, ['STRIKE(CE)/DATE', 'CE(O)', 'CE(H)', 'CE(L)', 'CE(C)', 'CHANGE/TYPE', 'STRIKE(PE)','PE(O)', 'PE(H)', 'PE(L)', 'PE(C)', '-', 'CE(OC)', 'CE(OH)', 'CE(LH)', '-', 'PE(OC)', 'PE(OH)', 'PE(LH)'])
gstats.print_statistics(responses, excel_location)
