import csv
import io
from urllib.request import Request, urlopen

import ScrapUtils as nse_bse

NIFTY100_STOCKS_URL='https://www.nseindia.com/content/indices/ind_nifty100list.csv'

def get_nifty100_stocks_latest_info():
    nse_100_stocks = []
    req = Request(NIFTY100_STOCKS_URL,headers={'User-Agent': 'Mozilla/5.0'})

    with urlopen (req) as csv_file:
        csv_reader = csv.reader (io.StringIO(csv_file.read().decode('utf-8')), delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count > 0:
                nse_100_stocks.append ({nse_bse.STOCK_ID:row[2],nse_bse.EXCHANGE:nse_bse.NSE})

            line_count += 1

    return nse_100_stocks

def get_my_porfolio():
    return [{nse_bse.STOCK_ID:'RBLBANK',nse_bse.EXCHANGE:nse_bse.NSE},{nse_bse.STOCK_ID:'COSMOFILMS',nse_bse.EXCHANGE:nse_bse.NSE},{nse_bse.STOCK_ID:'CUPID',nse_bse.EXCHANGE:nse_bse.NSE},{nse_bse.STOCK_ID:'SINTEXPLAST',nse_bse.EXCHANGE:nse_bse.NSE},{nse_bse.STOCK_ID:'DBL',nse_bse.EXCHANGE:nse_bse.NSE},{nse_bse.STOCK_ID:'FCL',nse_bse.EXCHANGE:nse_bse.NSE},{nse_bse.STOCK_ID:'WABAG',nse_bse.EXCHANGE:nse_bse.NSE},{nse_bse.STOCK_ID:'CAPF',nse_bse.EXCHANGE:nse_bse.NSE},{nse_bse.STOCK_ID:'SCHAEFFLER',nse_bse.EXCHANGE:nse_bse.NSE}]

def get_special_stocks_to_be_observed():
    return [{nse_bse.STOCK_ID:'YESBANK',nse_bse.EXCHANGE:nse_bse.NSE},{nse_bse.STOCK_ID:'INFIBEAM',nse_bse.EXCHANGE:nse_bse.NSE},{nse_bse.STOCK_ID:'GOODYEAR',nse_bse.EXCHANGE:nse_bse.NSE},{nse_bse.STOCK_ID:'DHFL',nse_bse.EXCHANGE:nse_bse.NSE},{nse_bse.STOCK_ID:'MUTHOOTCAP',nse_bse.EXCHANGE:nse_bse.NSE},{nse_bse.STOCK_ID:'SRTRANSFIN',nse_bse.EXCHANGE:nse_bse.NSE},{nse_bse.STOCK_ID:'LICHSGFIN',nse_bse.EXCHANGE:nse_bse.NSE},{nse_bse.STOCK_ID:'MAGMA',nse_bse.EXCHANGE:nse_bse.NSE},{nse_bse.STOCK_ID:'L&TFH',nse_bse.EXCHANGE:nse_bse.NSE},{nse_bse.STOCK_ID:'SHRIRAMCIT',nse_bse.EXCHANGE:nse_bse.NSE},{nse_bse.STOCK_ID:'HDFC',nse_bse.EXCHANGE:nse_bse.NSE},{nse_bse.STOCK_ID:'M&MFIN',nse_bse.EXCHANGE:nse_bse.NSE}]

#print(get_nifty100_stocks_latest_info())
