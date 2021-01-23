import csv
import io
import urllib

import DerivativeUtils as outil
import Utils as util
import requests
from urllib.request import Request, urlopen
import json
from bs4 import BeautifulSoup

NSE_CSV_FILE_URL='https://www1.nseindia.com/content/equities/EQUITY_L.csv'
BSE_CSV_FILE_LOCATION='C:/Users/Admin/Desktop/ListOfScrips.csv'
NIFTY100_STOCKS_URL='https://www.nseindia.com/content/indices/ind_nifty100list.csv'
NIFTY50_STOCKS_URL = 'https://www.nseindia.com/content/indices/ind_nifty50list.csv'
NSE_FO_STOCKS_URL = 'https://www1.nseindia.com/content/fo/fo_mktlots.csv'
TRADABLE_EXCEL_LOCATION = 'C:/Users/Admin/Desktop/TradableStocks.csv'
ZERODHA_MARGIN_URL = 'https://api.kite.trade/margins/equity'

DEFAULT_HEADERS = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://www.nse-india.com'}

STOCK_ID='stock_id'
STOCK_SYMBOL='stock_symbol'
EXCHANGE='exchange'
INSTRUMENT = 'instrument'
BSE='BSE_EQ'
NSE='NSE_EQ'
NSE_FO='NSE_FO'
NSE_INDEX = 'NSE_INDEX'
NIFTY_50 = 'NIFTY_50'
NIFTY_BANK = 'NIFTY_BANK'
NIFTY_50_NSE_SYMBOL = 'NIFTY'
NIFTY_BANK_NSE_SYMBOL = 'BANKNIFTY'
HEADERS = {'User-Agent': 'Mozilla/5.0'}


def remove_duplicate_stock_ids(bse_stock,nse_stocks):
    for nse_stock in nse_stocks:
        if nse_stock[STOCK_ID].strip () == bse_stock[STOCK_SYMBOL].strip ():
            return False

    return True


def get_zerodha_margin_stocks_ids():
    page = urlopen (Request (ZERODHA_MARGIN_URL, headers=DEFAULT_HEADERS))
    # parse the html using beautiful soup and store in variable `soup`
    zerodha_margin_data = json.loads (BeautifulSoup (page, 'html.parser').string.strip ())

    zerodha_MIS_stocks = []
    for data in zerodha_margin_data:
        zerodha_MIS_stocks.append ({STOCK_ID: (data['tradingsymbol']).strip (), EXCHANGE: NSE, NSE_FO: None})

    return zerodha_MIS_stocks


def get_tradable_stocks_ids():
    tradable_stocks = []
    with open (TRADABLE_EXCEL_LOCATION) as csv_file:
        csv_reader = csv.reader (csv_file, delimiter=',')
        for row in csv_reader:

            if len (row[0].strip ()) > 0:
                tradable_stocks.append ({STOCK_ID: (row[0]).strip (), EXCHANGE: NSE, NSE_FO: (row[2]).strip ()})

    return tradable_stocks


def get_top_20_nse_traded_stocks(tradable_stocks):
    tradable_stock_ids = []
    for stock in tradable_stocks:
        stock_id = urllib.parse.quote(outil.get_stock_id(stock[STOCK_ID]))
        tradable_stock_ids.append(stock_id)

    nse_stocks = get_all_nse_stocks_ids()

    nse_stock_ids = []

    for stock in nse_stocks:
        stock_id = urllib.parse.quote(outil.get_stock_id(stock[STOCK_ID]))
        if stock_id not in tradable_stock_ids:
            nse_stock_ids.append('NSE:' + stock_id)

    kite = util.intialize_kite_api()

    nse_stock_traded_values_map = {}
    quotes = kite.quote(nse_stock_ids[0:500])

    for stock_id, quote in quotes.items():
        nse_stock_traded_values_map[stock_id] = quote['volume'] * quote['average_price']

    quotes = kite.quote(nse_stock_ids[500:1000])

    for stock_id, quote in quotes.items():
        nse_stock_traded_values_map[stock_id] = quote['volume'] * quote['average_price']

    quotes = kite.quote(nse_stock_ids[1000:])

    for stock_id, quote in quotes.items():
        nse_stock_traded_values_map[stock_id] = quote['volume'] * quote['average_price']

    nse_stock_traded_values_map = {k: v for k, v in sorted(nse_stock_traded_values_map.items(), key=lambda item: item[1], reverse=True)}

    top_20_traded_stocks = []
    for stock_id in list(nse_stock_traded_values_map.keys())[:20]:
        top_20_traded_stocks.append ({STOCK_ID: stock_id.replace('NSE:', ''), EXCHANGE: NSE, NSE_FO: ''})

    return top_20_traded_stocks


def get_all_nse_stocks_ids():
    nse_stocks = []
    res = requests.get (NSE_CSV_FILE_URL, headers=outil.FUTURE_HEADERS)

    with res as csv_file:
        csv_reader = csv.reader (io.StringIO (csv_file.text), delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count > 0 and row[2] == 'EQ':
                nse_stocks.append ({STOCK_ID: (row[0]).strip (), EXCHANGE: NSE, NSE_FO: ''})

            line_count += 1

    return nse_stocks


def get_all_bse_stocks_ids():
    bse_stocks = []
    with open (BSE_CSV_FILE_LOCATION) as csv_file:
        csv_reader = csv.reader (csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count > 0:
                bse_stocks.append ({STOCK_ID: (row[0]).strip (), STOCK_SYMBOL: row[1], EXCHANGE: BSE})
            line_count += 1

    return bse_stocks


def get_all_indian_stock_ids():
    nse_stocks = get_all_nse_stocks_ids()

    bse_stocks = get_all_bse_stocks_ids()

    bse_stocks= list(filter(lambda a: remove_duplicate_stock_ids(a,nse_stocks), bse_stocks))

    indian_stocks=[]
    for nse_stock in nse_stocks:
        indian_stocks.append({STOCK_ID:nse_stock[STOCK_ID],EXCHANGE:NSE})

    for bse_stock in bse_stocks:
        indian_stocks.append({STOCK_ID:bse_stock[STOCK_ID],EXCHANGE:BSE})

    return indian_stocks


def get_nifty50_stocks_latest_info():
    nse_50_stocks = []
    req = Request (NIFTY50_STOCKS_URL, headers=HEADERS)

    with urlopen (req) as csv_file:
        csv_reader = csv.reader (io.StringIO (csv_file.read ().decode ('utf-8')), delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count > 0:
                nse_50_stocks.append ({STOCK_ID: (row[2]).strip (), EXCHANGE: NSE})

            line_count += 1

    return nse_50_stocks


def get_nse_fo():
    nse_fo_stocks = []
    req = Request (NSE_FO_STOCKS_URL, headers=HEADERS)

    with urlopen (req) as csv_file:
        csv_reader = csv.reader (io.StringIO (csv_file.read ().decode ('utf-8')), delimiter=',')
        for row in csv_reader:
             nse_fo_stocks.append ({STOCK_ID: (row[1]).strip (), EXCHANGE: NSE_FO})

    return nse_fo_stocks


def get_nse_fo_lots():
    nse_fo_stocks_lots = {}
    req = Request(NSE_FO_STOCKS_URL, headers=HEADERS)

    with urlopen(req) as csv_file:
        csv_reader = csv.reader(io.StringIO(csv_file.read().decode('utf-8')), delimiter=',')
        line_count = 0
        for row in csv_reader:
            if util.is_number (row[2].strip ()):
                nse_fo_stocks_lots[(row[1]).strip()] = int((row[2]).strip())

            line_count += 1

    return nse_fo_stocks_lots


def get_nse_fo_stocks():
    nse_fo_stocks = []
    req = requests.get(NSE_FO_STOCKS_URL, headers=outil.FUTURE_HEADERS)

    with req as csv_file:
        csv_reader = csv.reader (io.StringIO (csv_file.text), delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count > 10:
                nse_fo_stocks.append ({STOCK_ID: (row[1]).strip (), EXCHANGE: NSE})

            line_count += 1

    return nse_fo_stocks


def get_nifty100_stocks_latest_info():
    nse_100_stocks = []
    req = Request (NIFTY100_STOCKS_URL, headers=HEADERS)

    with urlopen (req) as csv_file:
        csv_reader = csv.reader (io.StringIO(csv_file.read().decode('utf-8')), delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count > 0:
                nse_100_stocks.append ({STOCK_ID: (row[2]).strip (), EXCHANGE: NSE})

            line_count += 1

    return nse_100_stocks


def get_indices():
    return [{STOCK_ID: 'NIFTY_50', EXCHANGE: NSE_INDEX}, {STOCK_ID: 'NIFTY_BANK', EXCHANGE: NSE_INDEX}]


def get_my_porfolio():
    return [{STOCK_ID:'RBLBANK',EXCHANGE:NSE},{STOCK_ID:'COSMOFILMS',EXCHANGE:NSE},{STOCK_ID:'CUPID',EXCHANGE:NSE},{STOCK_ID:'SINTEXPLAST',EXCHANGE:NSE},{STOCK_ID:'DBL',EXCHANGE:NSE},{STOCK_ID:'FCL',EXCHANGE:NSE},{STOCK_ID:'WABAG',EXCHANGE:NSE},{STOCK_ID:'CAPF',EXCHANGE:NSE},{STOCK_ID:'SCHAEFFLER',EXCHANGE:NSE}]


def get_special_stocks_to_be_observed():
    return [{STOCK_ID:'YESBANK',EXCHANGE:NSE},{STOCK_ID:'INFIBEAM',EXCHANGE:NSE},{STOCK_ID:'GOODYEAR',EXCHANGE:NSE},{STOCK_ID:'DHFL',EXCHANGE:NSE},{STOCK_ID:'MUTHOOTCAP',EXCHANGE:NSE},{STOCK_ID:'SRTRANSFIN',EXCHANGE:NSE},{STOCK_ID:'LICHSGFIN',EXCHANGE:NSE},{STOCK_ID:'MAGMA',EXCHANGE:NSE},{STOCK_ID:'L&TFH',EXCHANGE:NSE},{STOCK_ID:'SHRIRAMCIT',EXCHANGE:NSE},{STOCK_ID:'HDFC',EXCHANGE:NSE},{STOCK_ID:'M&MFIN',EXCHANGE:NSE}]

# print(get_all_nse_stocks_ids())
