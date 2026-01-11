import datetime
import os
import time
import traceback
import urllib.parse

import DerivativeUtils as outil
import requests
from StraddleBackTestingStocks import stocks as tradable_stocks
from bs4 import BeautifulSoup

# period = '24month'
# period = '15days'
NSE_EQ_HIST_DATA_URL = 'https://www1.nseindia.com/products/dynaContent/common/productsSymbolMapping.jsp?symbol={0}&segmentLink=3&symbolCount=2&series=EQ&dateRange=&fromDate={1}&toDate={2}&dataType=PRICEVOLUMEDELIVERABLE'
FILES_LOCATION = 'F:/Trading_Responses/EQ_data_multi/'
# stocks = sutils.get_nse_fo_stocks()

# indices = sutils.get_indices()
# tradable_stocks = sutils.get_nse_fo_stocks()
# tradable_stocks = sutils.get_all_nse_stocks_ids()
# tradable_stocks = sutils.get_tradable_stocks_ids()
# top_20_traded_stocks = sutils.get_top_20_nse_traded_stocks(tradable_stocks)
# tradable_stocks.extend(top_20_traded_stocks)

# stocks.extend(indices)
# stocks = [{sutils.STOCK_ID: 'ESCORTS', sutils.EXCHANGE: sutils.NSE_FO}]

today = datetime.date.today()
for stock in tradable_stocks:
    last = today
    first = last - datetime.timedelta(days=365)

    try:
        while first.year != 2014:

            first_day = first.strftime("%d-%m-%Y")
            last_day = last.strftime("%d-%m-%Y")

            last = first - datetime.timedelta(days=1)
            first = last - datetime.timedelta(days=365)

            stock_id = urllib.parse.quote(outil.get_stock_id(stock))

            option_file = FILES_LOCATION + '_' + urllib.parse.unquote(stock_id) + '_' + first_day + '_' + last_day + outil.EQ_FILE_SUFFIX
            if os.path.exists (option_file):
                print (stock_id + ' data already exists.')
                continue

            # url = NSE_EQ_HIST_DATA_URL.format(stock_id, period)
            url = NSE_EQ_HIST_DATA_URL.format(stock_id, first_day, last_day)

            response = requests.get(url, headers=outil.FUTURE_HEADERS)
            # parse the html using beautiful soup and store in variable `soup`
            soup = BeautifulSoup(response.text, 'html.parser')
            option_data = soup.findAll('div', attrs={'id': 'csvContentDiv'})

            if option_data is not None and len(option_data) > 0:

                open(option_file, 'w+').close()
                with open(option_file, 'a', encoding='utf-8') as the_file:
                    the_file.write(str(option_data[0].string.replace(':', '\n')))
                print('Fetched stock:', stock_id, first_day, last_day)
            else:
                print ('No data for stock:', stock_id, first_day, last_day)

            time.sleep(1)

    except Exception as e:
        print(str(traceback.format_exc()) + '\nError in stock:' + stock_id)
