import pandas as pd
import pytz

import Utils as util

if __name__ == '__main__':
    file_path = 'C:/Users/USER/Downloads/ind_nifty50list.csv'
    df = pd.read_csv(file_path)

    symbols_with_prefix = ['NSE:' + symbol for symbol in df['Symbol']]

    # print(data_list)
    #
    # exit(0)

    indian_timezone = pytz.timezone('Asia/Calcutta')

    kite = util.intialize_kite_api()
    #
    # print(kite.ohlc(data_list))
    # print(kite.instruments(exchange='NFO'))
    # exit(0)

    # print(kite.ohlc(data_list))
    ohlc_data = kite.ohlc(symbols_with_prefix)

    stockOptionsPricesMap = {}

    # Iterate through the dictionary and round the last_price to nearest 20
    for stock, data in ohlc_data.items():
        stock_name = stock.split(':')[1]  # Extract stock name
        last_price = data['last_price']

        df_symbol = df[df['Symbol'] == stock_name]

        strike_multiple = df_symbol['STRIKE MULTIPLE'].values[0]
        # Round the stock price to the nearest 20
        rounded_price = round(last_price / strike_multiple) * strike_multiple

        # print(f"Stock: {stock_name}, Rounded Price: {rounded_price}")

        peOptionSymbol = 'NFO:' + stock_name + '24DEC' + str(rounded_price) + 'PE'
        ceOptionSymbol = 'NFO:' + stock_name + '24DEC' + str(rounded_price) + 'CE'

        optionsPrices = kite.ohlc([peOptionSymbol, ceOptionSymbol])

        margin = df_symbol['MARGIN'].values[0]
        lots = df_symbol['LOTS'].values[0]

        stockOptionsPricesMap[stock] = {'return' : ((optionsPrices[peOptionSymbol]['last_price'] + optionsPrices[ceOptionSymbol]['last_price']) * lots) / margin, 'margin' : margin, 'lots' : lots, 'lots' : lots}
        # exit(0)

    # Sorting the dictionary by values in descending order
    sorted_dict = dict(sorted(stockOptionsPricesMap.items(), key=lambda item: item[1]['return'], reverse=True))

    # Display the sorted dictionary
    for option, data in sorted_dict.items():
        print(option + ':' + str(data))