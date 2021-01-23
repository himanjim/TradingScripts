import traceback
import urllib.parse
import urllib.parse
import requests
import os
import DerivativeUtils as outil
import ScrapUtils as sutils
from bs4 import BeautifulSoup
from StraddleBackTestingStocks import stocks

NSE_OPTION_HIST_DATA_URL = 'https://www1.nseindia.com/products/dynaContent/common/productsSymbolMapping.jsp?instrumentType={0}&symbol={1}&expiryDate=select&optionType={2}&strikePrice=&dateRange=24month&fromDate=&toDate=&segmentLink=9&symbolCount='

options = []
# indices = sutils.get_indices()
# options = sutils.get_nse_fo_stocks()
options = sutils.get_all_nse_stocks_ids()
# options.extend(indices)
# options = [{sutils.STOCK_ID: 'BANKNIFTY', sutils.EXCHANGE: sutils.NSE_FO}]

optionType = 'CE'

start = False

for stock_id in stocks:
    try:
        # stock_id = outil.get_stock_id(option[sutils.STOCK_ID])
        stock_id = urllib.parse.quote(stock_id)

        # if stock_id == 'NBVENTURES':
        #     start = True
        #
        # if not start:
        #     continue

        option_file = outil.OPTION_FILE_LOCATION + urllib.parse.unquote (stock_id) + optionType + outil.OPTION_FILE_SUFFIX

        if os.path.exists (option_file):
            print (stock_id + ' option data already exists.')
            continue

        if stock_id == sutils.NIFTY_50_NSE_SYMBOL:
            url = NSE_OPTION_HIST_DATA_URL.format(outil.OPTIDX, stock_id, optionType)
        elif stock_id == sutils.NIFTY_BANK_NSE_SYMBOL:
            url = NSE_OPTION_HIST_DATA_URL.format(outil.OPTIDX, stock_id, optionType)
        else:
            url = NSE_OPTION_HIST_DATA_URL.format(outil.OPTION_STRIKE, stock_id, optionType)

        response = requests.get (url, headers=outil.FUTURE_HEADERS)
        # parse the html using beautiful soup and store in variable `soup`
        soup = BeautifulSoup (response.text, 'html.parser')
        option_data = soup.findAll ('div', attrs={'id': 'csvContentDiv'})

        if option_data is not None and len(option_data) > 0:

            open(option_file, 'w+').close()
            with open(option_file, 'a', encoding='utf-8') as the_file:
                the_file.write(str(option_data[0].string.replace(':', '\n')))
            print ('Fetched option', optionType, stock_id)
        else:
            print ('No option data for stock:', optionType, stock_id)

    except Exception as e:
        print(str(traceback.format_exc()) + '\nError in stock:' + stock_id)