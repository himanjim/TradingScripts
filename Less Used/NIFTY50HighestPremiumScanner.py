import pandas as pd
import pytz

import Utils as util

if __name__ == '__main__':
    file_path = 'C:/Users/himan/Downloads/ind_nifty50list.csv'
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

    oldStockOptionsPricesMap = {'NSE:ADANIENT': {'NFO:ADANIENT24DEC2460PE': 48.25, 'NFO:ADANIENT24DEC2460CE': 48.25, 'lot': 900}, 'NSE:ADANIPORTS': {'NFO:ADANIPORTS24DEC1220PE': 25.4, 'NFO:ADANIPORTS24DEC1220CE': 25.4, 'lot': 2400}, 'NSE:APOLLOHOSP': {'NFO:APOLLOHOSP24DEC7250PE': 87.35, 'NFO:APOLLOHOSP24DEC7250CE': 87.35, 'lot': 500}, 'NSE:ASIANPAINT': {'NFO:ASIANPAINT24DEC2340PE': 26.55, 'NFO:ASIANPAINT24DEC2340CE': 26.55, 'lot': 1600}, 'NSE:AXISBANK': {'NFO:AXISBANK24DEC1120PE': 12, 'NFO:AXISBANK24DEC1120CE': 12, 'lot': 3125}, 'NSE:BAJAJ-AUTO': {'NFO:BAJAJ-AUTO24DEC9000PE': 143, 'NFO:BAJAJ-AUTO24DEC9000CE': 143, 'lot': 450}, 'NSE:BAJAJFINSV': {'NFO:BAJAJFINSV24DEC1640PE': 25.6, 'NFO:BAJAJFINSV24DEC1640CE': 25.6, 'lot': 2000}, 'NSE:BAJFINANCE': {'NFO:BAJFINANCE24DEC7100PE': 97, 'NFO:BAJFINANCE24DEC7100CE': 97, 'lot': 500}, 'NSE:BEL': {'NFO:BEL24DEC300PE': 2.8, 'NFO:BEL24DEC300CE': 2.8, 'lot': 8550}, 'NSE:BHARTIARTL': {'NFO:BHARTIARTL24DEC1600PE': 16.3, 'NFO:BHARTIARTL24DEC1600CE': 16.3, 'lot': 1900}, 'NSE:BPCL': {'NFO:BPCL24DEC290PE': 4.9, 'NFO:BPCL24DEC290CE': 4.9, 'lot': 10800}, 'NSE:BRITANNIA': {'NFO:BRITANNIA24DEC4800PE': 66.75, 'NFO:BRITANNIA24DEC4800CE': 66.75, 'lot': 700}, 'NSE:CIPLA': {'NFO:CIPLA24DEC1480PE': 23.25, 'NFO:CIPLA24DEC1480CE': 23.25, 'lot': 2600}, 'NSE:COALINDIA': {'NFO:COALINDIA24DEC395PE': 5.1, 'NFO:COALINDIA24DEC395CE': 5.1, 'lot': 7350}, 'NSE:DRREDDY': {'NFO:DRREDDY24DEC1280PE': 20.4, 'NFO:DRREDDY24DEC1280CE': 20.4, 'lot': 3125}, 'NSE:EICHERMOT': {'NFO:EICHERMOT24DEC4750PE': 52.55, 'NFO:EICHERMOT24DEC4750CE': 52.55, 'lot': 875}, 'NSE:GRASIM': {'NFO:GRASIM24DEC2600PE': 34.65, 'NFO:GRASIM24DEC2600CE': 34.65, 'lot': 1500}, 'NSE:HCLTECH': {'NFO:HCLTECH24DEC1960PE': 25.6, 'NFO:HCLTECH24DEC1960CE': 25.6, 'lot': 1750}, 'NSE:HDFCBANK': {'NFO:HDFCBANK24DEC1810PE': 17.05, 'NFO:HDFCBANK24DEC1810CE': 17.05, 'lot': 2200}, 'NSE:HDFCLIFE': {'NFO:HDFCLIFE24DEC620PE': 5.95, 'NFO:HDFCLIFE24DEC620CE': 5.95, 'lot': 6600}, 'NSE:HEROMOTOCO': {'NFO:HEROMOTOCO24DEC4400PE': 61.2, 'NFO:HEROMOTOCO24DEC4400CE': 61.2, 'lot': 900}, 'NSE:HINDALCO': {'NFO:HINDALCO24DEC630PE': 8.1, 'NFO:HINDALCO24DEC630CE': 8.1, 'lot': 5600}, 'NSE:HINDUNILVR': {'NFO:HINDUNILVR24DEC2360PE': 25.65, 'NFO:HINDUNILVR24DEC2360CE': 25.65, 'lot': 1500}, 'NSE:ICICIBANK': {'NFO:ICICIBANK24DEC1310PE': 10.95, 'NFO:ICICIBANK24DEC1310CE': 10.95, 'lot': 3500}, 'NSE:INDUSINDBK': {'NFO:INDUSINDBK24DEC960PE': 11.15, 'NFO:INDUSINDBK24DEC960CE': 11.15, 'lot': 2500}, 'NSE:INFY': {'NFO:INFY24DEC1980PE': 23.15, 'NFO:INFY24DEC1980CE': 23.15, 'lot': 2000}, 'NSE:ITC': {'NFO:ITC24DEC470PE': 4.95, 'NFO:ITC24DEC470CE': 4.95, 'lot': 8000}, 'NSE:JSWSTEEL': {'NFO:JSWSTEEL24DEC950PE': 14.5, 'NFO:JSWSTEEL24DEC950CE': 14.5, 'lot': 3375}, 'NSE:KOTAKBANK': {'NFO:KOTAKBANK24DEC1780PE': 20.05, 'NFO:KOTAKBANK24DEC1780CE': 20.05, 'lot': 2000}, 'NSE:LT': {'NFO:LT24DEC3750PE': 40.8, 'NFO:LT24DEC3750CE': 40.8, 'lot': 900}, 'NSE:M&M': {'NFO:M&M24DEC3050PE': 47.75, 'NFO:M&M24DEC3050CE': 47.75, 'lot': 1225}, 'NSE:MARUTI': {'NFO:MARUTI24DEC11000PE': 122.65, 'NFO:MARUTI24DEC11000CE': 122.65, 'lot': 350}, 'NSE:NESTLEIND': {'NFO:NESTLEIND24DEC2180PE': 19.5, 'NFO:NESTLEIND24DEC2180CE': 19.5, 'lot': 1600}, 'NSE:NTPC': {'NFO:NTPC24DEC340PE': 3.2, 'NFO:NTPC24DEC340CE': 3.2, 'lot': 10500}, 'NSE:ONGC': {'NFO:ONGC24DEC245PE': 3.7, 'NFO:ONGC24DEC245CE': 3.7, 'lot': 13475}, 'NSE:POWERGRID': {'NFO:POWERGRID24DEC320PE': 3.05, 'NFO:POWERGRID24DEC320CE': 3.05, 'lot': 12600}, 'NSE:RELIANCE': {'NFO:RELIANCE24DEC1250PE': 11, 'NFO:RELIANCE24DEC1250CE': 11, 'lot': 3500}, 'NSE:SBILIFE': {'NFO:SBILIFE24DEC1400PE': 18.55, 'NFO:SBILIFE24DEC1400CE': 18.55, 'lot': 2625}, 'NSE:SBIN': {'NFO:SBIN24DEC840PE': 9.55, 'NFO:SBIN24DEC840CE': 9.55, 'lot': 5250}, 'NSE:SHRIRAMFIN': {'NFO:SHRIRAMFIN24DEC2950PE': 62.95, 'NFO:SHRIRAMFIN24DEC2950CE': 62.95, 'lot': 1050}, 'NSE:SUNPHARMA': {'NFO:SUNPHARMA24DEC1800PE': 16.8, 'NFO:SUNPHARMA24DEC1800CE': 16.8, 'lot': 2450}, 'NSE:TATACONSUM': {'NFO:TATACONSUM24DEC910PE': 13.05, 'NFO:TATACONSUM24DEC910CE': 13.05, 'lot': 4560}, 'NSE:TATAMOTORS': {'NFO:TATAMOTORS24DEC760PE': 13.65, 'NFO:TATAMOTORS24DEC760CE': 13.65, 'lot': 4950}, 'NSE:TATASTEEL': {'NFO:TATASTEEL24DEC145PE': 2.35, 'NFO:TATASTEEL24DEC145CE': 2.35, 'lot': 27500}, 'NSE:TCS': {'NFO:TCS24DEC4350PE': 54.7, 'NFO:TCS24DEC4350CE': 54.7, 'lot': 875}, 'NSE:TECHM': {'NFO:TECHM24DEC1780PE': 26.25, 'NFO:TECHM24DEC1780CE': 26.25, 'lot': 2400}, 'NSE:TITAN': {'NFO:TITAN24DEC3400PE': 36.8, 'NFO:TITAN24DEC3400CE': 36.8, 'lot': 1225}, 'NSE:TRENT': {'NFO:TRENT24DEC7100PE': 125, 'NFO:TRENT24DEC7100CE': 125, 'lot': 500}, 'NSE:ULTRACEMCO': {'NFO:ULTRACEMCO24DEC11800PE': 173.65, 'NFO:ULTRACEMCO24DEC11800CE': 173.65, 'lot': 350}, 'NSE:WIPRO': {'NFO:WIPRO24DEC315PE': 6.25, 'NFO:WIPRO24DEC315CE': 6.25, 'lot': 15000}}

    newStockOptionsPricesMap = {}

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

        newStockOptionsPricesMap[stock] = {peOptionSymbol: optionsPrices[peOptionSymbol]['last_price'], ceOptionSymbol: optionsPrices[peOptionSymbol]['last_price'], 'lot': lots}

        # stockOptionsPricesMap[stock] = {'return' : ((optionsPrices[peOptionSymbol]['last_price'] + optionsPrices[ceOptionSymbol]['last_price']) * lots) / margin, 'margin' : margin, 'lots' : lots, 'lots' : lots}
        # exit(0)

    print(newStockOptionsPricesMap)

    stocksOptionsPriceRiseMap = {}
    for stock, newOptionsData in newStockOptionsPricesMap.items():
        newOptionsPricesSum = 0

        for option, newOptionPrice in newOptionsData.items():
            newOptionsPricesSum += newOptionPrice

        oldOptionsData = oldStockOptionsPricesMap[stock]

        oldOptionsPricesSum = 0
        for option, oldOptionPrice in oldOptionsData.items():
            oldOptionsPricesSum += oldOptionPrice

        stocksOptionsPriceRiseMap[stock] = (newOptionsPricesSum - oldOptionsPricesSum) / oldOptionsPricesSum


    print(dict(sorted(stocksOptionsPriceRiseMap.items(), reverse=True)))
    # # Sorting the dictionary by values in descending order
    # sorted_dict = dict(sorted(stockOptionsPricesMap.items(), key=lambda item: item[1]['return'], reverse=True))
    #
    # # Display the sorted dictionary
    # for option, data in sorted_dict.items():
    #     print(option + ':' + str(data))