import asyncio
import json
from datetime import datetime
from urllib.request import Request, urlopen

import time as sleep_time
from bs4 import BeautifulSoup
from upstox_api.api import TransactionType, OrderType, DurationType, ProductType

import ScrapUtils as nse_bse
import Utils as util

today_timestamp = int (
    datetime (datetime.today ().year, datetime.today ().month, datetime.today ().day, 15, 58, 56).timestamp ())

upstox_api = util.intialize_upstox_api ([nse_bse.NSE_FO, nse_bse.NSE])
current_fut = {'symbol': 'NMDC19MAYFUT',
               'live_url': 'https://tvc4.forexpros.com/783a9d5e01ae63dabd6a68480bb0f3e4/1556685636/56/56/23/history?symbol=998312&resolution=D&from=1525704109&to=' + str (
                   today_timestamp)}
equity = {'symbol': 'NMDC',
          'live_url': 'https://tvc4.forexpros.com/783a9d5e01ae63dabd6a68480bb0f3e4/1556685636/56/56/23/history?symbol=18309&resolution=D&from=1525704109&to=' + str (
              today_timestamp)}
lot = 6000

INVESTING_COM_HEADERS = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://in.investing.com'}

current_fut_ins = upstox_api.get_instrument_by_symbol (nse_bse.NSE_FO, current_fut['symbol'])
equity_ins = upstox_api.get_instrument_by_symbol (nse_bse.NSE, equity['symbol'])


def fetch_investing_com_data(live_data_url):
    page = urlopen (Request (live_data_url, headers=INVESTING_COM_HEADERS))
    # parse the html using beautiful soup and store in variable `soup`
    soup = BeautifulSoup (page, 'html.parser')

    live_data = json.loads (soup.string.strip ())

    stock_live_data = {'open': util.round_to_tick (live_data['o'][-1]), 'ltp': util.round_to_tick (live_data['c'][-1]),
                       'date': util.get_date_from_timestamp (int (live_data['t'][-1]) * 1000).date ()}

    return stock_live_data


async def order_c():
    upstox_api.place_order (TransactionType.Buy, current_fut_ins, lot, OrderType.Market, ProductType.Delivery, None,
                            None, None, DurationType.DAY, None, None)


async def order_e():
    upstox_api.place_order (TransactionType.Sell, equity_ins, lot, OrderType.Market, ProductType.Delivery, None, None,
                            None, DurationType.DAY, None, None)


async def place_orders(f_ltp, e_ltp):
    tasks = list ()
    tasks.append (asyncio.ensure_future (order_c (f_ltp)))
    tasks.append (asyncio.ensure_future (order_e (e_ltp)))

    await asyncio.gather (*tasks)


async def fetch_future_live_price_data(future_live_price_data):
    future_live_price_data.update (fetch_investing_com_data (current_fut['live_url']))


async def fetch_equity_live_price_data(equity_live_price_data):
    equity_live_price_data.update (fetch_investing_com_data (equity['live_url']))


async def fetch_live_data():
    future_live_price_data = {}
    equity_live_price_data = {}

    tasks = list ()
    tasks.append (asyncio.ensure_future (fetch_future_live_price_data (future_live_price_data)))
    tasks.append (asyncio.ensure_future (fetch_equity_live_price_data (equity_live_price_data)))

    await asyncio.gather (*tasks)

    if (future_live_price_data['ltp'] - equity_live_price_data['ltp']) > .65:
        # loop = asyncio.get_event_loop ()
        # loop.run_until_complete (place_orders (future_live_price_data['ltp'], equity_live_price_data['ltp']))
        print ('####Diff:%f. Sold %s at market price. Bought %s at market price. Current time:%s.' % (
            (future_live_price_data['ltp'] - equity_live_price_data['ltp']), str (future_live_price_data['ltp']),
            str (equity_live_price_data['ltp']), str (datetime.now ())))

    # if (future_live_price_data['ltp'] - equity_live_price_data['ltp']) < 1.3:
    #     # loop = asyncio.get_event_loop ().
    #     # loop.run_until_complete (place_orders (future_live_price_data['ltp'], equity_live_price_data['ltp']))
    #     print ('****Diff:%f.Bought %s at market price. Sold %s at market price. Current time:%s.' % (
    #         (future_live_price_data['ltp'] - equity_live_price_data['ltp']), str (future_live_price_data['ltp']), str (equity_live_price_data['ltp']), str (datetime.now ())))

testing = True


while util.is_market_open () is False and testing is False:
    pass

while True:
    loop = asyncio.get_event_loop ()
    loop.run_until_complete (fetch_live_data ())

    sleep_time.sleep (1)

exit (0)
