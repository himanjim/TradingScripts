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
excel_location = 'F:/Trading_Responses/Weekly_BankNifty_Data_Full' + str (current_time) + '.xlsx'

bank_nifty_spot = {}
with open ('F:/Trading_Responses/Bank_Nifty/NIFTYBANK.csv') as csvfile:
    spot_data_rows = csv.reader (csvfile, delimiter=',', quotechar='"')

    for spot_data_row in spot_data_rows:
        bank_nifty_spot[parser.parse(spot_data_row[0].strip()).date()] = {'open': float(util.remove_non_no_chars(spot_data_row[1].strip())), 'high': float(util.remove_non_no_chars(spot_data_row[2].strip())), 'low': float(util.remove_non_no_chars(spot_data_row[3].strip())), 'close': float(util.remove_non_no_chars(spot_data_row[4].strip()))}


def get_bank_nifty_spot(ref_date):
    while ref_date not in bank_nifty_spot:
        ref_date = ref_date - timedelta (days=1)

    return ref_date, bank_nifty_spot[ref_date]


def set_pe_ce_data(options_data, ref_trade_start_date, ref_expiry_date, atm_strike):

    first_row_excluded = False

    data = {}

    for option_data in options_data:
        if not first_row_excluded:
            first_row_excluded = True
            continue

        curr_date = parser.parse(option_data[1].strip()).date()
        expiry_date = parser.parse(option_data[2].strip()).date()

        if ref_expiry_date != expiry_date or ref_trade_start_date > curr_date:
            continue

        strike = float(util.remove_non_no_chars(option_data[4].strip()))

        if strike != atm_strike:
            continue

        data.update({curr_date: {'prem_open':  float(util.remove_non_no_chars(option_data[5].strip())), 'prem_high':  float(util.remove_non_no_chars(option_data[6].strip())), 'prem_low':  float(util.remove_non_no_chars(option_data[7].strip())), 'prem_close':  float(util.remove_non_no_chars(option_data[8].strip()))}})

    return data


start_date = datetime(2020, 5, 7).date ()
end_date = datetime(2020, 6, 25).date ()

responses = []

while start_date <= end_date:
    try:
        expiry_date, spot_expiry = get_bank_nifty_spot(start_date)

        trade_start_date = start_date - timedelta (days=6)
        while trade_start_date not in bank_nifty_spot:
            trade_start_date = trade_start_date + timedelta (days=1)

        spot = bank_nifty_spot[trade_start_date]
        atm_strike = int (round (spot['open'] / 100.0) * 100)

        start_date = start_date + timedelta (days=7)

        with open (FILES_LOCATION + SPOT + expiry_date.strftime (DATE_FORMAT) + 'CE' + outil.OPTION_FILE_SUFFIX) as csvfile:
            ce_data = set_pe_ce_data (csv.reader (csvfile, delimiter=',', quotechar='"'), trade_start_date, expiry_date, atm_strike)

        with open (FILES_LOCATION + SPOT + expiry_date.strftime (DATE_FORMAT) + 'PE' + outil.OPTION_FILE_SUFFIX) as csvfile:
            pe_data = set_pe_ce_data (csv.reader (csvfile, delimiter=',', quotechar='"'), trade_start_date, expiry_date, atm_strike)

        responses.append ([SPOT, spot['open'], spot_expiry['close'], atm_strike])

        for date, c_cent_data in ce_data.items ():
            p_cent_data = pe_data[date]
            responses.append (
                [date, c_cent_data['prem_open'], c_cent_data['prem_high'], c_cent_data['prem_low'], c_cent_data['prem_close'], '', p_cent_data['prem_open'], p_cent_data['prem_high'], p_cent_data['prem_low'], p_cent_data['prem_close']])

        responses.append (['-', '-', '-'])
    except Exception:
        print (traceback.format_exc (), str(start_date))

responses.insert (0, ['DATE', 'CE(O)', 'CE(H)', 'CE(L)', 'CE(C)', '', 'PE(O)', 'PE(H)', 'PE(L)', 'PE(C)'])
gstats.print_statistics(responses, excel_location)
