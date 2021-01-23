import asyncio
import csv
import datetime
import io
import json
import math
import os
import statistics
import time
import traceback
import urllib.parse
from urllib.request import Request, urlopen

import ScrapUtils as sutils
import Utils as util
import requests
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta, TH
from upstox_api.api import TransactionType, OrderType, ProductType, DurationType

max_pain_safety_buffer = .05
max_liquidity_margin = .2
upper_pcr_value_for_buying = 1.3
lower_pcr_value_for_shorting = .5
NO_OF_LOTS_UPPER_LIMIT = 10000
current_time = time.strftime ('%b-%d-%Y')
MARGINS_FILE = 'C:/Users/Admin/Desktop/F&O_Margin_' + current_time + '.csv'
NSE_OPTIONS_LIVE_URL = 'https://www.nseindia.com/live_market/dynaContent/live_watch/option_chain/optionKeys.jsp?segmentLink=17&instrument={0}&symbol={1}&date={2}'

NSE_FUTURE_LIVE_URL = 'https://www.nseindia.com/live_market/dynaContent/live_watch/fomwatchsymbol.jsp?key={0}&Fut_Opt=Futures'

NSE_BIDS_LIVE_URL = 'https://nseindia.com/live_market/dynaContent/live_watch/get_quote/ajaxFOGetQuoteJSON.jsp?underlying={0}&instrument={1}&expiry={2}&type=SELECT&strike=SELECT'

NSE_EQ_LIVE_URL = 'https://www.nseindia.com/live_market/dynaContent/live_watch/get_quote/ajaxGetQuoteJSON.jsp?symbol={0}&series=EQ'

PHYSICAL_SETTLEMENT_CSV = 'physical_settlement.csv'

OPTION_STRIKE = 'OPTSTK'
OPTIDX = 'OPTIDX'
FUTIDX = 'FUTIDX'
FUTSTK = 'FUTSTK'

FUTURE_HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36', 'Referer': 'https://www1.nseindia.com/products/content/equities/equities/eq_security.htm', 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9', 'Accept-Encoding': 'gzip, deflate, br', 'Accept-Language': 'en-GB,en;q=0.9,en-US;q=0.8,hi;q=0.7', 'Cache-Control': 'max-age=0', 'Connection': 'keep-alive', 'Host': 'www1.nseindia.com', 'Sec-Fetch-Mode': 'navigate', 'Sec-Fetch-Site': 'none', 'Sec-Fetch-User': '?1', 'Upgrade-Insecure-Requests': '1'}

FUTURE_FILE_LOCATION = ['G:', 'F:'][os.path.exists('F:')] + '/Trading_Responses/Future_data/'
FUTURE_FILE_SUFFIX = '_fut_csv.txt'

OPTION_FILE_LOCATION = ['G:', 'F:'][os.path.exists('F:')] + '/Trading_Responses/Option_data/'
OPTION_FILE_SUFFIX = '_option_csv.txt'

EQ_FILE_LOCATION = ['G:', 'F:'][os.path.exists('F:')] + '/Trading_Responses/EQ_data/'
EQ_FILE_SUFFIX = '_eq_csv.txt'

buy_sell_enabled = False


class Futures:
    def __init__(self):
        self.stock_id = None
        self.symbol = None
        self.ltp = None
        self.spot_price = None
        self.liquidity = False
        self.volume = None
        self.lot_size = None
        self.expiry = None
        self.bids = None
        self.asks = None
        self.high = None
        self.low = None
        self.open = None

    def __str__(self):
        res_str = "Stock id:" + str (self.stock_id) + '\n'
        res_str = "Symbol:" + str (self.symbol) + '\n'
        res_str += "Ltp:" + str(self.ltp) + '\n'
        res_str += "Spot:" + str(self.spot_price) + '\n'
        res_str += "Liquidity:" + str(self.liquidity) + '\n'
        res_str += "Volume:" + str(self.volume) + '\n'
        res_str += "Lot size:" + str(self.lot_size) + '\n'
        res_str += "Bids:" + str (self.bids) + '\n'
        res_str += "Asks:" + str(self.asks) + '\n'
        res_str += "High:" + str (self.high) + '\n'
        res_str += "Low:" + str (self.low) + '\n'
        res_str += "Open:" + str (self.open) + '\n'

        return res_str


class Options:
    def __init__(self):
        self.symbol = None
        self.ltp = None
        self.spot_price = None
        self.liquidity = False
        self.oi = None
        self.strike_price = False
        self.lot_size = None
        self.tick_size = False
        self.expiry = None
        self.bids = None
        self.asks = None
        self.implied_volatility = None
        self.is_call = None
        self.volume = None

    def __str__(self):
        res_str = "Symbol:" + self.symbol + '\n'
        res_str += "Ltp:" + str (self.ltp) + '\n'
        res_str += "Spot:" + str (self.spot_price) + '\n'
        res_str += "Liquidity:" + str (self.liquidity) + '\n'
        res_str += "Oi:" + str (self.oi) + '\n'
        res_str += "Strike price:" + str (self.strike_price) + '\n'
        res_str += "Lot size:" + str (self.lot_size) + '\n'
        res_str += "Bids" + str (self.bids) + '\n'
        res_str += "Asks:" + str (self.asks) + '\n'
        res_str += "Implied volatility:" + str (self.implied_volatility) + '\n'
        res_str += "Call:" + str (self.is_call) + '\n'
        res_str += "Volume:" + str (self.volume) + '\n'
        return res_str


def get_nse_fo_lots_on_future(last_thurs):
    nse_fo_stocks_lots = {}
    req = Request (sutils.NSE_FO_STOCKS_URL, headers=sutils.HEADERS)

    with urlopen (req) as csv_file:
        csv_reader = csv.reader (io.StringIO (csv_file.read ().decode ('utf-8')), delimiter=',')
        line_count = 0
        for row in csv_reader:
            if util.is_number (row[2].strip ()):
                future_symbol = get_future_symbol ((row[1]).strip (), last_thurs)
                nse_fo_stocks_lots[future_symbol] = int ((row[2]).strip ())

            line_count += 1

    return nse_fo_stocks_lots


def is_stock_physically_settled(stock_id):
    with open (PHYSICAL_SETTLEMENT_CSV) as csv_file:
        csv_reader = csv.reader (csv_file, delimiter=',')
        for row in csv_reader:
            if row[0].strip () == stock_id:
                if int (row[1].strip ()) > datetime.datetime.now ().month:
                    return False
                else:
                    return True

    print ('No physical settlement status found for stock:', stock_id)
    return True


async def fetch_future_with_orderbook(stock_id, fetched_futures, last_thurs):
    bids, asks, ltp, spot = get_all_bids (stock_id, last_thurs)
    future = Futures ()
    future.bids = bids
    future.asks = asks
    future.ltp = ltp
    future.spot_price = spot
    future.stock_id = stock_id
    future.symbol = get_future_symbol (stock_id, last_thurs)

    fetched_futures[stock_id] = future


async def fetch_futures_with_orderbook(futures, fetched_futures, current_month_last_thurs):
    tasks = []
    for future in futures:
        stock_id = get_stock_id (future[sutils.STOCK_ID])
        tasks.append (
            asyncio.ensure_future (fetch_future_with_orderbook (stock_id, fetched_futures, current_month_last_thurs)))

    await asyncio.gather (*tasks)


def run_fetch_futures_with_orderbook(futures, fetched_futures, current_month_last_thurs):
    loop = asyncio.get_event_loop ()

    loop.run_until_complete (
        fetch_futures_with_orderbook (futures, fetched_futures, current_month_last_thurs))


async def fetch_option(stock_id, fetched_options, today_date):
    fetched_options[stock_id] = get_all_strikes (stock_id, today_date.month, today_date.year)


async def fetch_options(options, fetched_options, today_date):
    tasks = []
    for option in options:
        stock_id = get_stock_id (option[sutils.STOCK_ID])
        tasks.append (asyncio.ensure_future (fetch_option (stock_id, fetched_options, today_date)))

    await asyncio.gather (*tasks)


def run_fetch_options(options, fetched_options, today_date):
    loop = asyncio.get_event_loop ()
    loop.run_until_complete(fetch_options(options, fetched_options, today_date))


def buy_instrument(upstox_api, symbol, exchange, price, lot_size, orderType=OrderType.Limit, stoploss=None):
    if buy_sell_enabled:
        upstox_api.place_order(TransactionType.Buy, upstox_api.get_instrument_by_symbol(exchange, symbol), lot_size,
                               orderType, ProductType.Delivery, price, None, None, DurationType.DAY, stoploss, None)


def sell_instrument(upstox_api, symbol, exchange, price, lot_size, orderType=OrderType.Limit, stoploss=None):
    if buy_sell_enabled:
        upstox_api.place_order(TransactionType.Sell, upstox_api.get_instrument_by_symbol(exchange, symbol), lot_size,
                               orderType, ProductType.Delivery, price, None, None, DurationType.DAY, stoploss, None)


def is_option_outside_1_sd(stock_price_range, strike_price):
    return stock_price_range[0] < strike_price or strike_price < stock_price_range[1]


def is_option_without_premium(premium):
    if premium is None or util.is_number(premium) is False:
        return True
    elif premium <= 0:
        return True
    else:
        return False


def is_option_without_oi(oi):
    if oi is None or util.is_number (oi) is False:
        return True
    elif oi <= 0:
        return True
    else:
        return False


def is_otm(symbol, strike_price, spot_price):
    if is_call (symbol) and strike_price > spot_price:
        return True
    elif is_call (symbol) is False and strike_price < spot_price:
        return True
    else:
        return False


def get_future_margins(futures):
    required_margins = {}
    for future in futures:
        stock_id = get_stock_id (future[sutils.STOCK_ID])
        required_margins[stock_id] = get_margin (stock_id, datetime.datetime.now ().month,
                                                 datetime.datetime.now ().year)

    return required_margins


def get_margin(stock_id, month, year):
    last_thur = get_last_thurday_of_month (month, year)
    expiry = last_thur.strftime ('%Y-%m-%d')

    try:
        with open (MARGINS_FILE) as csv_file:
            csv_reader = csv.reader (csv_file, delimiter=',')
            for row in csv_reader:
                if row[0].strip () == stock_id:
                    if row[1].strip () == expiry:
                        return float (row[4]) + float (row[5])
                    else:
                        print ('Stock expiry not matching for:', stock_id, expiry)
                        return float (row[4]) + float (row[5])
    except Exception as e:
        print (traceback.format_exc ())

    return None


def is_instrument_liquid(bids, asks, liquidity_margin=max_liquidity_margin, min_length=1):
    if bids is None or len(bids) < min_length or asks is None or len(asks) < min_length:
        return False

    av_bid_price = sum ((item['price'] * item['quantity']) for item in bids) / sum (item['quantity'] for item in bids)
    av_ask_price = sum ((item['price'] * item['quantity']) for item in asks) / sum (item['quantity'] for item in asks)

    if av_bid_price == 0 or av_ask_price == 0:
        return False

    spread = abs (av_bid_price - av_ask_price) / ((av_bid_price + av_ask_price) / 2)

    if spread > liquidity_margin:
        return False
    else:
        return True


def get_all_strikes(stock_id, month, year):
    last_thurs = get_last_thurday_of_month(month, year)

    last_thurs_str = last_thurs.strftime("%d") + last_thurs.strftime("%b").upper() + last_thurs.strftime("%Y")

    stock_id = urllib.parse.quote (stock_id)
    if stock_id == sutils.NIFTY_50 or stock_id == sutils.NIFTY_50_NSE_SYMBOL:
        url = NSE_OPTIONS_LIVE_URL.format (OPTIDX, sutils.NIFTY_50_NSE_SYMBOL, last_thurs_str)
    elif stock_id == sutils.NIFTY_BANK or stock_id == sutils.NIFTY_BANK_NSE_SYMBOL:
        url = NSE_OPTIONS_LIVE_URL.format (OPTIDX, sutils.NIFTY_BANK_NSE_SYMBOL, last_thurs_str)
    else:
        url = NSE_OPTIONS_LIVE_URL.format (OPTION_STRIKE, stock_id, last_thurs_str)

    page = urlopen(Request(url, headers=sutils.HEADERS))
    # parse the html using beautiful soup and store in variable `soup`
    soup = BeautifulSoup(page, "html.parser")

    ltp_rows = soup.findAll ('b', attrs={'style': 'font-size:1.2em;'})

    if len(ltp_rows) > 0 and util.is_number(util.remove_non_no_chars(util.convert_nav_str_to_str(ltp_rows[0].string))):
        ltp = float (util.remove_non_no_chars (util.convert_nav_str_to_str (ltp_rows[0].string)))
    else:
        print('No ltp for stock:', stock_id)

    stock_price_rows = soup.findAll("table", attrs={'id': 'octable'})

    options = []

    for stock_price_row in stock_price_rows:
        for child in stock_price_row.find_all("tr"):
            cells = child.find_all ("td")
            if len (cells) > 21:
                links = cells[5].find_all("a")

                if len(links) > 0 and util.is_number(
                        util.remove_non_no_chars(util.convert_nav_str_to_str(links[0].string))):
                    option = Options ()
                    option.ltp = float(util.remove_non_no_chars(util.convert_nav_str_to_str(links[0].string)))
                    option.strike_price = round(
                        float(util.remove_non_no_chars(util.convert_nav_str_to_str(cells[11].string))))
                    if util.is_number(util.remove_non_no_chars(util.convert_nav_str_to_str(cells[1].string))):
                        option.oi = int(util.remove_non_no_chars(util.convert_nav_str_to_str(cells[1].string)))

                    if util.is_number (util.remove_non_no_chars (util.convert_nav_str_to_str (cells[3].string))):
                        option.volume = int (
                            util.remove_non_no_chars (util.convert_nav_str_to_str (cells[3].string)))

                    if util.is_number(util.remove_non_no_chars(util.convert_nav_str_to_str(cells[4].string))):
                        option.implied_volatility = float(
                            util.remove_non_no_chars(util.convert_nav_str_to_str(cells[4].string)))

                    if util.is_number(
                            util.remove_non_no_chars(util.convert_nav_str_to_str(cells[7].string))) and util.is_number(
                            util.remove_non_no_chars(util.convert_nav_str_to_str(cells[10].string))):
                        option.bids = []
                        option.bids.append ({
                            'quantity': int (util.remove_non_no_chars (util.convert_nav_str_to_str (cells[7].string))),
                            'price': float (util.remove_non_no_chars (util.convert_nav_str_to_str (cells[8].string)))})

                        option.asks = []
                        option.asks.append ({
                            'quantity': int (util.remove_non_no_chars (util.convert_nav_str_to_str (cells[10].string))),
                            'price': float (util.remove_non_no_chars (util.convert_nav_str_to_str (cells[9].string)))})

                    option.symbol = stock_id + last_thurs.strftime ("%g") + last_thurs.strftime ("%b").upper () + str (
                        option.strike_price) + 'CE'
                    option.is_call = True
                    option.expiry = last_thurs
                    option.spot_price = ltp
                    option.liquidity = is_instrument_liquid (option.bids, option.asks)

                    options.append(option)

                links = cells[17].find_all("a")

                if len(links) > 0 and util.is_number(
                        util.remove_non_no_chars(util.convert_nav_str_to_str(links[0].string))):
                    option = Options()
                    option.ltp = float(util.remove_non_no_chars(util.convert_nav_str_to_str(links[0].string)))
                    option.strike_price = round(
                        float(util.remove_non_no_chars(util.convert_nav_str_to_str(cells[11].string))))

                    if util.is_number(util.remove_non_no_chars(util.convert_nav_str_to_str(cells[21].string))):
                        option.oi = int(util.remove_non_no_chars(util.convert_nav_str_to_str(cells[21].string)))

                    if util.is_number(util.remove_non_no_chars(util.convert_nav_str_to_str(cells[18].string))):
                        option.implied_volatility = float(
                            util.remove_non_no_chars(util.convert_nav_str_to_str(cells[18].string)))

                    if util.is_number (util.remove_non_no_chars (util.convert_nav_str_to_str (cells[19].string))):
                        option.volume = int (
                            util.remove_non_no_chars (util.convert_nav_str_to_str (cells[19].string)))

                    if util.is_number(
                            util.remove_non_no_chars(util.convert_nav_str_to_str(cells[12].string))) and util.is_number(
                        util.remove_non_no_chars(util.convert_nav_str_to_str(cells[15].string))):
                        option.bids = []
                        option.bids.append ({
                            'quantity': int (util.remove_non_no_chars (util.convert_nav_str_to_str (cells[12].string))),
                            'price': float (util.remove_non_no_chars (util.convert_nav_str_to_str (cells[13].string)))})

                        option.asks = []
                        option.asks.append ({
                            'quantity': int (util.remove_non_no_chars (util.convert_nav_str_to_str (cells[15].string))),
                            'price': float (util.remove_non_no_chars (util.convert_nav_str_to_str (cells[14].string)))})

                    option.symbol = stock_id + last_thurs.strftime("%g") + last_thurs.strftime("%b").upper() + str(
                        option.strike_price) + 'PE'
                    option.expiry = last_thurs
                    option.is_call = False
                    option.spot_price = ltp
                    option.liquidity = is_instrument_liquid (option.bids, option.asks)

                    options.append (option)

    return options


def get_all_futures(stock_id, current_month_last_thurs_yr, near_month_last_thurs_mn, far_month_last_thurs_mn):
    stock_id = urllib.parse.quote (stock_id)
    url = NSE_FUTURE_LIVE_URL.format(stock_id)

    page = urlopen(Request(url, headers=sutils.HEADERS))
    # parse the html using beautiful soup and store in variable `soup`
    soup = BeautifulSoup(page, 'html.parser')
    stock_price_rows = soup.findAll('div', attrs={'class': 'tabular_data_live_analysis'})

    futures = []

    for stock_price_row in stock_price_rows:
        for child in stock_price_row.find_all('tr'):
            cells = child.find_all('td')
            if len(cells) > 5:
                future = Futures()
                future.expiry = datetime.datetime.strptime(util.convert_nav_str_to_str(cells[2].string), '%d%b%Y')
                future.symbol = get_future_symbol(stock_id, future.expiry)

                if util.is_number (util.remove_non_no_chars (util.convert_nav_str_to_str (cells[9].string))):
                    future.ltp = float (util.remove_non_no_chars (util.convert_nav_str_to_str (cells[9].string)))

                if util.is_number (util.remove_non_no_chars (util.convert_nav_str_to_str (cells[10].string))):
                    future.volume = float (util.remove_non_no_chars (util.convert_nav_str_to_str (cells[10].string)))

                if util.is_number (util.remove_non_no_chars (util.convert_nav_str_to_str (cells[12].string))):
                    future.spot_price = float (
                        util.remove_non_no_chars (util.convert_nav_str_to_str (cells[12].string)))

                if future.symbol.split (current_month_last_thurs_yr)[1].startswith (far_month_last_thurs_mn):
                    futures.insert (2, future)
                elif future.symbol.split (current_month_last_thurs_yr)[1].startswith (near_month_last_thurs_mn):
                    futures.insert (1, future)
                else:
                    futures.insert (0, future)

    return futures


def get_equity_live_ltp(stock_id):
    stock_id = urllib.parse.quote(stock_id)
    if stock_id == sutils.NIFTY_50 or stock_id == sutils.NIFTY_50_NSE_SYMBOL:
        url = NSE_EQ_LIVE_URL.format(sutils.NIFTY_50_NSE_SYMBOL)
    elif stock_id == sutils.NIFTY_BANK or stock_id == sutils.NIFTY_BANK_NSE_SYMBOL:
        url = NSE_EQ_LIVE_URL.format(sutils.NIFTY_BANK_NSE_SYMBOL)
    else:
        url = NSE_EQ_LIVE_URL.format(stock_id)

    response = requests.get (url, headers=FUTURE_HEADERS)
    # parse the html using beautiful soup and store in variable `soup`
    soup = BeautifulSoup(response.text, 'html.parser')

    js = json.loads(soup.string.strip())

    if 'data' in js and len (js['data']) > 0:
        price_data = js['data'][0]

        ltp = float (util.remove_non_no_chars (price_data['lastPrice']))

        prev_close = float (util.remove_non_no_chars (price_data['previousClose']))
        open = float (util.remove_non_no_chars (price_data['open']))
        high = float (util.remove_non_no_chars (price_data['dayHigh']))
        low = float (util.remove_non_no_chars (price_data['dayLow']))
        close = float (util.remove_non_no_chars (price_data['closePrice']))
        volume = float (util.remove_non_no_chars (price_data['totalTradedVolume']))

        return ltp, open, high, low, close, prev_close, volume

    else:
        print ('No data for stock:', stock_id)
        return None, None, None, None, None, None, None


def get_all_bids(stock_id, expiry):
    last_thurs_str = expiry.strftime ("%d") + expiry.strftime ("%b").upper () + expiry.strftime ("%Y")
    stock_id = urllib.parse.quote (stock_id)
    if stock_id == sutils.NIFTY_50 or stock_id == sutils.NIFTY_50_NSE_SYMBOL:
        url = NSE_BIDS_LIVE_URL.format (sutils.NIFTY_50_NSE_SYMBOL, FUTIDX, last_thurs_str)
    elif stock_id == sutils.NIFTY_BANK or stock_id == sutils.NIFTY_BANK_NSE_SYMBOL:
        url = NSE_BIDS_LIVE_URL.format (sutils.NIFTY_BANK_NSE_SYMBOL, FUTIDX, last_thurs_str)
    else:
        url = NSE_BIDS_LIVE_URL.format (stock_id, FUTSTK, last_thurs_str)

    page = urlopen (Request (url, headers=FUTURE_HEADERS))
    # parse the html using beautiful soup and store in variable `soup`
    soup = BeautifulSoup (page, 'html.parser')

    js = json.loads (soup.string.strip ())

    price_data = js['data'][0]

    asks = []

    i = 1
    while True:
        sq_str = 'sellQuantity' + str (i)
        sp_str = 'sellPrice' + str (i)
        if sq_str in price_data and sp_str in price_data:
            if util.is_number (util.remove_non_no_chars (price_data[sp_str])):
                asks.append ({'price': float (util.remove_non_no_chars (price_data[sp_str])),
                              'quantity': int (util.remove_non_no_chars (price_data[sq_str]))})
        else:
            break

        i += 1

    bids = []

    i = 1
    while True:
        bq_str = 'buyQuantity' + str (i)
        bp_str = 'buyPrice' + str (i)
        if bq_str in price_data and bp_str in price_data:
            if util.is_number (util.remove_non_no_chars (price_data[bp_str])):
                bids.append ({'price': float (util.remove_non_no_chars (price_data[bp_str])),
                              'quantity': int (util.remove_non_no_chars (price_data[bq_str]))})
        else:
            break

        i += 1

    ltp = ''
    if 'lastPrice' in price_data:
        ltp = price_data['lastPrice']
    if util.is_number (util.remove_non_no_chars (ltp)):
        ltp = float (util.remove_non_no_chars (ltp))
    else:
        return [], [], None, None, None, None, None, None

    spot = ''
    if 'underlyingValue' in price_data:
        spot = price_data['underlyingValue']
    if util.is_number (util.remove_non_no_chars (spot)):
        spot = float (util.remove_non_no_chars (spot))

    high = ''
    if 'highPrice' in price_data:
        high = price_data['highPrice']
    if util.is_number (util.remove_non_no_chars (high)):
        high = float (util.remove_non_no_chars (high))
    else:
        high = 0

    low = ''
    if 'lowPrice' in price_data:
        low = price_data['lowPrice']
    if util.is_number (util.remove_non_no_chars (low)):
        low = float (util.remove_non_no_chars (low))
    else:
        low = 0

    open_price = ''
    if 'openPrice' in price_data:
        open_price = price_data['openPrice']
    if util.is_number (util.remove_non_no_chars (open_price)):
        open_price = float (util.remove_non_no_chars (open_price))
    else:
        open_price = 0

    volume = ''
    if 'vwap' in price_data:
        volume = price_data['vwap']
    if util.is_number (util.remove_non_no_chars (volume)):
        volume = float (util.remove_non_no_chars (volume))
    else:
        volume = 0

    return bids, asks, ltp, spot, high, low, open_price, volume


def get_atm_strike(stock_options):
    min_diff = None
    atm_strike = None

    for option in stock_options:
        if min_diff is None or abs (option.spot_price - option.strike_price) < min_diff:
            atm_strike = option.strike_price
            min_diff = abs (option.spot_price - option.strike_price)

    return atm_strike


def get_last_thurday_of_month(cmon, year):
    todayte = datetime.datetime (year, cmon, 1)

    for i in range (1, 6):
        t = todayte + relativedelta (weekday=TH (i))
        if t.month != cmon:
            # since t is exceeded we need last one  which we can get by subtracting -2 since it is already a Thursday.
            t = t + relativedelta (weekday=TH (-2))
            break

    return t


def get_daily_returns(stock_data):
    log_returns = []
    for i in range (1, len (stock_data)):
        log_returns.append (math.log (stock_data[i]['close'] / stock_data[i - 1]['close']))

    return log_returns


def get_daily_average_returns(returns):
    return statistics.mean (returns)


def get_daily_volatility(returns):
    return statistics.stdev (returns)


def get_volatility(daily_volatility, duration):
    return daily_volatility * math.sqrt (duration)


def get_range(price, stock_data, duration):
    daily_returns = get_daily_returns (stock_data)
    sd = get_daily_volatility (daily_returns)

    d_mean = get_daily_average_returns (daily_returns) * duration
    d_sd = get_volatility (sd, duration)

    upper_range = price * (1 + d_mean + d_sd)
    lower_range = price * (1 + d_mean - d_sd)

    return upper_range, [0, lower_range][lower_range > 0]


def get_volatility_based_stoploss(price, daily_volatility, duration):
    d_volatility = get_volatility (daily_volatility, duration)
    return [price * (1 + d_volatility), price * (1 - d_volatility)]


def convert_daily_to_monthly_return(investment, pl, days):
    return math.pow ((1 + (pl / investment)), 30 / days) - 1


def convert_monthly_to_yearly_return(monthly_return):
    return math.pow ((1 + monthly_return), 12) - 1


def convert_daily_to_yearly_return(investment, pl, days):
    return math.pow ((1 + (pl / investment)), 365.25 / days) - 1


def get_all_options_strikes(stock_id, upstox_api, exchange, month, year, spot_price):
    instruments = upstox_api.search_instruments (exchange, stock_id + year + month)

    options = []

    for instrument in instruments:
        option = Options ()
        if instrument.instrument_type.upper () == OPTION_STRIKE:
            option.symbol = instrument.symbol
            option.strike_price = instrument.strike_price
            option.spot_price = spot_price
            option.lot_size = instrument.lot_size
            option.expiry = instrument.expiry
            option.tick_size = instrument.tick_size
            options.append (option)

    return options


def get_max_pain_strike(options):
    strikes = []

    for option in options:
        strikes.append (option.strike_price)

    strikes = set (strikes)

    money_lost_by_writers = []

    for strike in strikes:
        loss = 0
        for option in options:
            if is_call (option.symbol) and option.strike_price < strike and util.is_number (option.oi):
                loss += (option.oi * (strike - option.strike_price))
            elif is_put (option.symbol) and option.strike_price > strike and util.is_number (option.oi):
                loss += (option.oi * (option.strike_price - strike))

        money_lost_by_writers.append ({'strike': strike, 'loss': loss})

    money_lost_by_writers.sort (key=lambda x: x['loss'])

    if len (money_lost_by_writers) > 0:
        return money_lost_by_writers[0]['strike'] * (1 + max_pain_safety_buffer)
    else:
        return 0


def get_pcr(options):
    put_oi = 0
    call_oi = 1

    for option in options:
        if is_call (option.symbol) and util.is_number (option.oi):
            call_oi += option.oi
        elif is_put (option.symbol) and util.is_number (option.oi):
            put_oi += option.oi

    return put_oi / call_oi


def is_call(symbol):
    return symbol.strip ().upper ().endswith ('CE')


def is_put(symbol):
    return symbol.strip ().upper ().endswith ('PE')


def get_future_symbol(stock_id, last_thurs):
    return stock_id + last_thurs.strftime ('%g') + last_thurs.strftime ('%b').upper () + 'FUT'


def get_stock_id(stock_id):
    if stock_id == sutils.NIFTY_50:
        stock_id = sutils.NIFTY_50_NSE_SYMBOL
    elif stock_id == sutils.NIFTY_BANK:
        stock_id = sutils.NIFTY_BANK_NSE_SYMBOL

    return stock_id.strip ()