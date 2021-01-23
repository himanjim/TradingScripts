import datetime
import os
import traceback
import urllib.parse

import DerivativeUtils as outil
import requests
from bs4 import BeautifulSoup

NSE_OPTION_HIST_DATA_URL = 'https://www1.nseindia.com/products/dynaContent/common/productsSymbolMapping.jsp?instrumentType=OPTIDX&symbol={0}&expiryDate=select&optionType={1}&strikePrice=&dateRange=&fromDate={2}&toDate={3}&segmentLink=9&symbolCount='

# NSE_OPTION_HIST_DATA_URL = 'https://www1.nseindia.com/products/dynaContent/common/productsSymbolMapping.jsp?instrumentType=OPTSTK&symbol={0}&expiryDate=select&optionType={1}&strikePrice=&dateRange=&fromDate={2}&toDate={3}&segmentLink=9&symbolCount='

optionType = 'PE'

start = False

today = datetime.date.today()

FILES_LOCATION = 'F:/Trading_Responses/Bank_Nifty/'
stocks = ['BANKNIFTY']
for stock in stocks:
    first = (today + datetime.timedelta(days=31)).replace(day=1)
    while True:
        try:
            # stock_id = outil.get_stock_id(option[sutils.STOCK_ID])
            stock_id = urllib.parse.quote(stock)

            lastMonth = first - datetime.timedelta(days=1)

            first_day_of_month = lastMonth.replace(day=1).strftime ("%d-%b-%Y")
            last_day_of_month = lastMonth.strftime("%d-%b-%Y")

            if lastMonth.year == 2010:
                break

            option_file = FILES_LOCATION + urllib.parse.unquote (stock_id) + lastMonth.strftime("%b-%Y") + optionType + outil.OPTION_FILE_SUFFIX

            first = lastMonth.replace (day=1)

            if os.path.exists (option_file):
                print ('Option data already exists:', optionType, stock_id, lastMonth.strftime("%b-%Y"))
                continue

            url = NSE_OPTION_HIST_DATA_URL.format(stock_id, optionType, first_day_of_month, last_day_of_month)

            response = requests.get (url, headers=outil.FUTURE_HEADERS)
            # parse the html using beautiful soup and store in variable `soup`
            soup = BeautifulSoup (response.text, 'html.parser')
            option_data = soup.findAll ('div', attrs={'id': 'csvContentDiv'})

            if option_data is not None and len(option_data) > 0:

                open(option_file, 'w+').close()
                with open(option_file, 'a', encoding='utf-8') as the_file:
                    the_file.write(str(option_data[0].string.replace(':', '\n')))
                print ('Fetched option', optionType, stock_id, lastMonth.strftime("%b-%Y"))
            else:
                print ('No option data for stock:', optionType, stock_id, lastMonth.strftime("%b-%Y"))

        except Exception as e:
            print(str(traceback.format_exc()) + '\nError in stock:' + stock_id)