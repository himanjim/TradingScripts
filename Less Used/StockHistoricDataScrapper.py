import datetime
import os
import time
import traceback
import urllib.parse
import ScrapUtils as scrap
import DerivativeUtils as sutils
import requests
from StraddleBackTestingStocks import stocks as tradable_stocks
from bs4 import BeautifulSoup

# period = '24month'
# period = '15days'
NSE_EQ_HIST_DATA_URL = 'https://www1.nseindia.com/products/dynaContent/common/productsSymbolMapping.jsp?symbol={0}&segmentLink=3&symbolCount=2&series=EQ&dateRange=12month&fromDate=&toDate=&dataType=PRICEVOLUMEDELIVERABLE'
# stocks = sutils.get_nse_fo_stocks()

# indices = sutils.get_indices()
tradable_stocks = scrap.get_nse_fo_stocks()
# tradable_stocks = sutils.get_all_nse_stocks_ids()
# tradable_stocks = sutils.get_tradable_stocks_ids()
# top_20_traded_stocks = sutils.get_top_20_nse_traded_stocks(tradable_stocks)
# tradable_stocks.extend(top_20_traded_stocks)

# stocks.extend(indices)
# stocks = [{sutils.STOCK_ID: 'ESCORTS', sutils.EXCHANGE: sutils.NSE_FO}]

for stock in tradable_stocks:
    try:
        stock_id = sutils.get_stock_id(stock[scrap.STOCK_ID])
        # stock_id = urllib.parse.quote(stock_id)

        # if stock_id == 'NBVENTURES':
        #     start = True
        #
        # if not start:
        #     continue

        option_file = sutils.EQ_FILE_LOCATION + urllib.parse.unquote (stock_id) + sutils.EQ_FILE_SUFFIX

        if os.path.exists (option_file):
            print (stock_id + ' option data already exists.')
            continue

        url = NSE_EQ_HIST_DATA_URL.format(stock_id)

        response = requests.get (url, headers=sutils.FUTURE_HEADERS)
        # parse the html using beautiful soup and store in variable `soup`
        soup = BeautifulSoup (response.text, 'html.parser')
        option_data = soup.findAll ('div', attrs={'id': 'csvContentDiv'})

        if option_data is not None and len(option_data) > 0:

            open(option_file, 'w+').close()
            with open(option_file, 'a', encoding='utf-8') as the_file:
                the_file.write(str(option_data[0].string.replace(':', '\n')))
            print ('Fetched data:', stock_id)
        else:
            print ('No equity data for stock:', stock_id)

    except Exception as e:
        print(str(traceback.format_exc()) + '\nError in stock:' + stock_id)