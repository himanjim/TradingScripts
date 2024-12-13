import math

import Utils as util
import time as tm
from datetime import datetime
import pytz
import csv


# Function to read a specific column from CSV, append 'NSE:', and return as a list
def read_csv_column_with_prefix(file_path, column_name, prefix='NSE:'):
    column_data = []

    # Open the CSV file
    with open(file_path, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)

        # Iterate through each row and append the desired column value with prefix to the list
        for row in reader:
            column_data.append(prefix + row[column_name])

    return column_data


# Function to find entries where 'open' equals 'low' or 'open' equals 'high'
def find_matching_open(data):
    result = {}

    for symbol, details in data.items():
        ohlc = details['ohlc']
        open_price = ohlc['open']
        low_price = ohlc['low']
        high_price = ohlc['high']

        # Check if open equals low or open equals high
        if open_price == low_price or open_price == high_price:
            result[symbol] = details

    return result


# Function to find the stock with highest 'open-low' ratio and 'high-open' ratio
def find_highest_movement_separately(data):
    max_diff_low = None
    max_diff_high = None
    stock_with_highest_low = None
    stock_with_highest_high = None

    for symbol, details in data.items():
        ohlc = details['ohlc']
        open_price = ohlc['open']
        low_price = ohlc['low']
        high_price = ohlc['high']

        # Calculate ('open' - 'low') / 'open' and ('high' - 'open') / 'open'
        diff_low = (open_price - low_price) / open_price
        diff_high = (high_price - open_price) / open_price

        # Track the stock with the highest ('open' - 'low') / 'open'
        if max_diff_low is None or diff_low > max_diff_low:
            max_diff_low = diff_low
            stock_with_highest_low = (symbol, diff_low)

        # Track the stock with the highest ('high' - 'open') / 'open'
        if max_diff_high is None or diff_high > max_diff_high:
            max_diff_high = diff_high
            stock_with_highest_high = (symbol, diff_high)

    return stock_with_highest_low, stock_with_highest_high


# Function to find stocks that meet the criteria
def find_stocks_with_inverted_movement(data):
    stocks_low = []
    stocks_high = []

    for symbol, details in data.items():
        ohlc = details['ohlc']
        open_price = ohlc['open']
        low_price = ohlc['low']
        high_price = ohlc['high']
        last_price = details['last_price']

        # Case 1: Check if low < open and last_price > open
        if low_price < open_price < last_price:
            diff_low = (open_price - low_price) / open_price
            # Add the stock and its corresponding diff to the list
            stocks_low.append((symbol, diff_low))

        # Case 2: Check if high > open and last_price < open
        if high_price > open_price > last_price:
            diff_high = (high_price - open_price) / open_price
            # Add the stock and its corresponding diff to the list
            stocks_high.append((symbol, diff_high))

    stocks_low.sort(key=lambda x: x[1], reverse=True)
    stocks_high.sort(key=lambda x: x[1], reverse=True)

    return stocks_low, stocks_high


if __name__ == '__main__':
    file_path = 'C:/Users/USER/Downloads/ind_nifty50list.csv'
    column_name = 'Symbol'  # Replace with the actual column name
    data_list = read_csv_column_with_prefix(file_path, column_name)

    # print(data_list)
    #
    # exit(0)

    indian_timezone = pytz.timezone('Asia/Calcutta')

    kite = util.intialize_kite_api()

    print(kite.ohlc(data_list))

    exit(0)

    # print(kite.ohlc(data_list))
    ohlc_data = kite.ohlc(data_list)

    # Example usage
    matching_entries = find_matching_open(ohlc_data)

    print(matching_entries)

    # Example usage
    stocks_low, stocks_high = find_stocks_with_inverted_movement(ohlc_data)

    # Output for stock with highest ('open' - 'low') / 'open'
    if stocks_low:
        print(f"Stock with highest ('open' - 'low') / 'open': {stocks_low}")
    else:
        print("No stock meets the criteria for ('open' - 'low') / 'open'.")

    # Output for stock with highest ('high' - 'open') / 'open'
    if stocks_high:
        print(f"Stock with highest ('high' - 'open') / 'open': {stocks_high}")
    else:
        print("No stock meets the criteria for ('high' - 'open') / 'open'.")

