import numpy as np
from scipy.signal import argrelmax, argrelmin

import Utils as util

min_duration_between_two_price_zones_in_days = 20
max_support_resistance_variation_percent = 2
min_no_of_price_zones = 3
no_of_sessions_to_scan = 200
min_no_surrounding_points_for_maxima_minima = 5


def are_timestamps_properly_spaced(timestamp1, timestamp2):
    if (abs (timestamp1 - timestamp2) / (1000 * 86400) < min_duration_between_two_price_zones_in_days):
        return False

    return True


def get_supports_resistances(original_stock_data, sessions_to_scan=no_of_sessions_to_scan):
    # stock_data_high_prices_series = util.get_panda_series_of_stock_closing_prices (original_stock_data, 'high')
    # stock_data_low_prices_series = util.get_panda_series_of_stock_closing_prices (original_stock_data, 'low')
    #
    # _89_day_high_EMA_series = ind.ema (stock_data_high_prices_series, 89)
    # _89_day_low_EMA_series = ind.ema (stock_data_low_prices_series, 89)
    #
    # supports_resistances = [{'close':_89_day_high_EMA_series.iloc[-1], 'timestamp':original_stock_data[-2]['timestamp']},{'close':_89_day_low_EMA_series.iloc[-1], 'timestamp':original_stock_data[-2]['timestamp']}]

    stock_data = original_stock_data[-([len(original_stock_data), sessions_to_scan][len(original_stock_data) >= sessions_to_scan]):]

    stock_data_closing_prices = []
    for stock in stock_data:
        stock_data_closing_prices.append (stock['close'])

    stock_data_closing_prices = np.array (stock_data_closing_prices)

    maximas = argrelmax (stock_data_closing_prices, order=min_no_surrounding_points_for_maxima_minima)
    minimas = argrelmin (stock_data_closing_prices, order=min_no_surrounding_points_for_maxima_minima)

    maximas_minimas = []
    for maxima_index in maximas[0]:
        maximas_minimas.append (stock_data[maxima_index])

    for minima_index in minimas[0]:
        maximas_minimas.append (stock_data[minima_index])

    maximas_minimas.sort (key=lambda x: (-x['close']))

    supports_resistances = []

    for i in range (0, len (maximas_minimas)):
        supports_resistances_count = 1
        temp_srs=[{'close':maximas_minimas[i]['close'],'timestamp':maximas_minimas[i]['timestamp']}]
        for j in range (i + 1, len (maximas_minimas)):
            if (supports_resistances_count >= min_no_of_price_zones):
                supports_resistances.append ({'close':maximas_minimas[i]['close'],'timestamp':maximas_minimas[i]['timestamp']})
                break

            if (util.nearly_equal (maximas_minimas[i]['close'], maximas_minimas[j]['close'],
                              max_support_resistance_variation_percent)):
                is_timestamps_properly_spaced=True
                for temp_sr in temp_srs:
                    if(are_timestamps_properly_spaced(temp_sr['timestamp'], maximas_minimas[j]['timestamp']))==False:
                        is_timestamps_properly_spaced = False
                        break;


                if is_timestamps_properly_spaced:
                    temp_srs.append({'close':maximas_minimas[j]['close'], 'timestamp':maximas_minimas[j]['timestamp']})
                    supports_resistances_count += 1
            else:
                break

    return supports_resistances
