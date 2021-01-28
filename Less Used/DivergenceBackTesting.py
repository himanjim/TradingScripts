import traceback
from datetime import datetime, timedelta

import numpy as np
import time
from scipy.signal import argrelmax, argrelmin

import Indicators as ind
import ScrapUtils as nse_bse
import Utils as util


def positive_divergence_exist(stock_data, rsi_series):
    stock_data_closing_prices = []
    for stock in stock_data:
        stock_data_closing_prices.append (stock['close'])

    stock_maximas_minimas = get_maximas_minimas (stock_data_closing_prices)

    stock_minima_indices = stock_maximas_minimas[1][0]

    stock_minima_indices[::-1].sort ()

    if len (stock_minima_indices) < 2:
        return False
    else:
        for index in range (0, len (stock_minima_indices) - 1):
            if stock_data_closing_prices[stock_minima_indices[index]] < stock_data_closing_prices[
                stock_minima_indices[index + 1]]:
                return False

    rsi_values = []
    for index in range (0, len (rsi_series)):
        rsi_values.append (rsi_series.iloc[index])

    rsi_maximas_minimas = get_maximas_minimas (rsi_values)

    rsi_minima_indices = rsi_maximas_minimas[1][0]

    rsi_minima_indices[::-1].sort ()

    if len (rsi_minima_indices) < 2:
        return False
    else:
        if rsi_values[rsi_minima_indices[0]] > rsi_values[rsi_minima_indices[1]]:
            return False

        for index in range (1, len (rsi_minima_indices) - 1):
            if rsi_values[rsi_minima_indices[index]] < rsi_values[rsi_minima_indices[index + 1]]:
                return False

    return True


def negative_divergence_exist(stock_data, rsi_series):
    stock_data_closing_prices = []
    for stock in stock_data:
        stock_data_closing_prices.append (stock['close'])

    stock_maximas_minimas = get_maximas_minimas (stock_data_closing_prices)

    stock_maxima_indices = stock_maximas_minimas[0][0]

    stock_maxima_indices[::-1].sort ()

    if len (stock_maxima_indices) < 2:
        return False
    else:
        for index in range (0, len (stock_maxima_indices) - 1):
            if stock_data_closing_prices[stock_maxima_indices[index]] > stock_data_closing_prices[
                stock_maxima_indices[index + 1]]:
                return False

    rsi_values = []
    for index in range (0, len (rsi_series)):
        rsi_values.append (rsi_series.iloc[index])

    rsi_maximas_minimas = get_maximas_minimas (rsi_values)

    rsi_maxima_indices = rsi_maximas_minimas[0][0]

    rsi_maxima_indices[::-1].sort ()

    if len (rsi_maxima_indices) < 2:
        return False
    else:
        if rsi_values[rsi_maxima_indices[0]] < rsi_values[rsi_maxima_indices[1]]:
            return False

        for index in range (1, len (rsi_maxima_indices) - 1):
            if rsi_values[rsi_maxima_indices[index]] > rsi_values[rsi_maxima_indices[index + 1]]:
                return False

    return True


def get_maximas_minimas(data_set):
    data_set_array = np.array (data_set)

    maximas = argrelmax (data_set_array, order=1)
    minimas = argrelmin (data_set_array, order=1)

    return maximas, minimas


diff_between_start_end_date = 800
no_of_sessions_to_skip_from_start = 50
no_of_sessions_to_buffer_from_end = 10
no_of_sessions_to_scan_for_RSI = 14

current_time = time.strftime ("%Y_%m_%d#%H_%M_%S")
rsi80_excel_location = 'D:/Trading_Responses/Divergence_excel_back_test' + str (current_time) + '.xlsx'
rsi_sma60_excel_location = 'D:/Trading_Responses/Divergence_excel_back_test' + str (current_time) + '.xlsx'

start_time = time.time ()

upstox_api = util.intialize_upstox_api (['NSE_INDEX', 'NSE_EQ'])

today_date = datetime.today ().date ()
start_date = datetime.now () - timedelta (days=diff_between_start_end_date)
end_date = datetime.now () - timedelta (days=1)

# stocks_latest_info = nse_bse.get_all_nse_stocks_ids ()
# stocks_latest_info = nse_bse.get_nifty50_stocks_latest_info ()
# stocks_latest_info = nse_bse.get_nse_fo_stocks ()
# stocks_latest_info = nse_bse.get_nifty100_stocks_latest_info ()
stocks_latest_info = [{nse_bse.STOCK_ID: 'HDFC', nse_bse.EXCHANGE: nse_bse.NSE}]

# no_of_successful_rsi_more_than_80_cases = 0
# no_of_failed_rsi_more_than_80_cases = 0
# no_of_successful_rsi_sma_more_than_60_cases = 0
# no_of_failed_rsi_sma_more_than_60_cases = 0
#
# rsi_more_than_80_responses = [
#     ['STOCK', 'STRATEGY', 'BGHT ON', 'SLD ON', 'BUY PRICE', 'EXIT PRICE', 'PROFIT/LOSS %', 'VOL']]
#
# rsi_sma_more_than_60_responses = [
#     ['STOCK', 'STRATEGY', 'BGHT ON', 'SLD ON', 'BUY PRICE', 'EXIT PRICE', 'PROFIT/LOSS %', 'VOL']]

for stock_latest_info in stocks_latest_info:
    try:
        stock_latest_data = util.get_stock_latest_data (stock_latest_info[nse_bse.STOCK_ID], upstox_api, start_date,
                                                        end_date, stock_latest_info[nse_bse.EXCHANGE])

        stock_data_closing_prices_series = util.get_panda_series_of_stock_closing_prices (stock_latest_data)

        rsi_series = ind.rsi (stock_data_closing_prices_series, no_of_sessions_to_scan_for_RSI)

        i = no_of_sessions_to_skip_from_start

        while i + no_of_sessions_to_buffer_from_end < len (stock_latest_data):

            for j in range (5, 11):
                if positive_divergence_exist (stock_latest_data[i:i + j], rsi_series.iloc[i:i + j]):
                    for stock in stock_latest_data[i:i + j]:
                        print (stock['close'])

                    print (rsi_series.iloc[i:i + j])

                    print ('+ve ' + str (j) + ':' + str (
                        util.get_date_from_timestamp (stock_latest_data[i + j]['timestamp'])))
                    exit (0)
                if negative_divergence_exist (stock_latest_data[i:i + j], rsi_series.iloc[i:i + j]):
                    print (
                        '-ve ' + str (j) + str (util.get_date_from_timestamp (stock_latest_data[i + j]['timestamp'])))
                    exit (0)

            i += 1


    except Exception as e:
        print (traceback.format_exc ())

# print ("Successful RSI>80 cases: %s" % ((no_of_successful_rsi_more_than_80_cases * 100) / (
#             no_of_successful_rsi_more_than_80_cases + no_of_failed_rsi_more_than_80_cases)))
#
# print ("Successful RSI SMA>60 cases: %s" % ((no_of_successful_rsi_sma_more_than_60_cases * 100) / (
#             no_of_successful_rsi_sma_more_than_60_cases + no_of_failed_rsi_sma_more_than_60_cases)))
#
# gstats.print_statistics (rsi_more_than_80_responses, rsi80_excel_location)
# gstats.print_statistics (rsi_sma_more_than_60_responses, rsi_sma60_excel_location)

print ("---Script executed in %s seconds ---" % (time.time () - start_time))
