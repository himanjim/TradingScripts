import json
from datetime import datetime
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup

import ScrapUtils as nse_bse
import Utils as util

today_timestamp = int (
    datetime (datetime.today ().year, datetime.today ().month, datetime.today ().day, 15, 58, 56).timestamp ())

# stocks = nse_bse.get_nse_fo_stocks()
stocks = [{nse_bse.STOCK_ID: 'M&M',
           'live_url': 'https://tvc4.forexpros.com/783a9d5e01ae63dabd6a68480bb0f3e4/1556685636/56/56/23/history?symbol=18014&resolution=D&from=1525704109&to=' + str (
               today_timestamp)}]

INVESTING_COM_HEADERS = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://in.investing.com'}


def fetch_investing_com_data(live_data_url):
    page = urlopen (Request (live_data_url, headers=INVESTING_COM_HEADERS))
    # parse the html using beautiful soup and store in variable `soup`
    soup = BeautifulSoup (page, 'html.parser')

    live_data = json.loads (soup.string.strip ())

    stock_live_data = {'open': util.round_to_tick (live_data['o'][-1]), 'ltp': util.round_to_tick (live_data['c'][-1]),
                       'date': util.get_date_from_timestamp (int (live_data['t'][-1]) * 1000).date ()}

    return stock_live_data


for stock in stocks:
    stock_data = fetch_investing_com_data (stock)
