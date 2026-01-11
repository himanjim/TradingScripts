import traceback
import urllib.parse
from urllib.request import Request, urlopen

import DerivativeUtils as outil
import ScrapUtils as sutils
from bs4 import BeautifulSoup
period = '12month'
# period = '24month'
NSE_FUTURE_HIST_DATA_URL = 'https://www.nseindia.com/products/dynaContent/common/productsSymbolMapping.jsp?instrumentType={0}&symbol={1}&expiryDate=select&optionType=select&strikePrice=&dateRange={2}&fromDate=&toDate=&segmentLink=9&symbolCount='

futures = []
# indices = sutils.get_indices()
futures = sutils.get_nse_fo_stocks ()
# futures.extend(indices)
# futures = [{sutils.STOCK_ID: 'SBIN', sutils.EXCHANGE: sutils.NSE_FO}]

for future in futures:
    try:
        stock_id = outil.get_stock_id(future[sutils.STOCK_ID])
        stock_id = urllib.parse.quote (stock_id)

        if stock_id == sutils.NIFTY_50_NSE_SYMBOL:
            url = NSE_FUTURE_HIST_DATA_URL.format (outil.FUTIDX, stock_id, period)
        elif stock_id == sutils.NIFTY_BANK_NSE_SYMBOL:
            url = NSE_FUTURE_HIST_DATA_URL.format (outil.FUTIDX, stock_id, period)
        else:
            url = NSE_FUTURE_HIST_DATA_URL.format (outil.FUTSTK, stock_id, period)

        page = urlopen (Request (url, headers=outil.FUTURE_HEADERS))
        # parse the html using beautiful soup and store in variable `soup`
        soup = BeautifulSoup(page, 'html.parser')
        future_data = soup.findAll('div', attrs={'id': 'csvContentDiv'})

        if future_data is not None and len(future_data) > 0:

            future_file = outil.FUTURE_FILE_LOCATION + urllib.parse.unquote (stock_id) + outil.FUTURE_FILE_SUFFIX

            open(future_file, 'w+').close()
            with open(future_file, 'a', encoding='utf-8') as the_file:
                the_file.write(str(future_data[0].string.replace(':', '\n')))

            print('Fetched future data for stock:' + stock_id)
        else:
            print('No future data for stock:' + stock_id)

    except Exception as e:
        print(str(traceback.format_exc()) + '\nError in stock:' + stock_id)
