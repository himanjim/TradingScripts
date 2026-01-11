import json
import os
from datetime import datetime
from urllib.request import Request, urlopen

import math
from bs4 import BeautifulSoup

import GenericStatPrinter as gstats
import ScrapUtils as sUtils
import Utils as util

max_data_len = 10
today_date = datetime.now ().strftime ('%Y-%m-%d')
excel_location = ['G:', 'F:'][os.path.exists('F:')] + '/Trading_Responses/CO_BO_stocks_' + str (today_date) + '.xlsx'
low_vol_excel_location = ['G:', 'F:'][os.path.exists('F:')] + '/Trading_Responses/Low_Vol_CO_BO_stocks_' + str (today_date) + '.xlsx'
fo_stocks = sUtils.get_nse_fo_stocks()
min_aver_trade = math.inf
max_aver_trade = -math.inf
for fo_stock in fo_stocks:
    stock_datas = util.get_equity_historical_data (fo_stock[sUtils.STOCK_ID])
    stock_datas = stock_datas[-max_data_len:]
    total_trade = 0
    for stock_data in stock_datas:
        total_trade += (stock_data['close'] * stock_data['volume'])
    aver_trade = total_trade / len(stock_datas)
    if aver_trade < min_aver_trade:
        min_aver_trade = aver_trade
    if aver_trade > max_aver_trade:
        max_aver_trade = aver_trade
print('Min aver. trade:%f' % min_aver_trade)
print('Max aver. trade:%f' % max_aver_trade)

zerodha_margin_url = 'https://api.kite.trade/margins/equity'

page = urlopen(Request(zerodha_margin_url, headers={'User-Agent': 'Mozilla/5.0', 'Referer': 'https://www.nse-india.com'}))
        # parse the html using beautiful soup and store in variable `soup`
zerodha_margin_data = json.loads(BeautifulSoup(page, 'html.parser').string.strip())

tradable_co_bo_stocks = []
for data in zerodha_margin_data:
    tradable_co_bo_stocks.append (data['tradingsymbol'])

print('TR CO BO stocks:%d' %(len(tradable_co_bo_stocks)))

high_vol_tradable_co_bo_stocks = []
low_vol_tradable_co_bo_stocks = []
for tradable_co_bo_stock in tradable_co_bo_stocks:
    try:
        stock_datas = util.get_equity_historical_data (tradable_co_bo_stock)
        stock_datas = stock_datas[-max_data_len:]
    except Exception as e:
        print ('Stock:%s data not found:' %(tradable_co_bo_stock))
        continue
    total_trade = 0
    for stock_data in stock_datas:
        total_trade += (stock_data['close'] * stock_data['volume'])
    aver_trade = total_trade / len(stock_datas)
    if aver_trade >= (min_aver_trade / 2):
        high_vol_tradable_co_bo_stocks.append([tradable_co_bo_stock, aver_trade])
    else:
        low_vol_tradable_co_bo_stocks.append ([tradable_co_bo_stock, aver_trade])

print('HIGH VOL TR CO BO stocks:%d' %(len(high_vol_tradable_co_bo_stocks)))

low_vol_responses = [['STOCK', 'TRADE AMNT.']]
for low_vol_tradable_co_bo_stock in low_vol_tradable_co_bo_stocks:
    low_vol_responses.append (low_vol_tradable_co_bo_stock)
gstats.print_statistics (low_vol_responses, low_vol_excel_location)

responses = [['STOCK', 'TRADE AMNT.']]
for high_vol_tradable_co_bo_stock in high_vol_tradable_co_bo_stocks:
    responses.append(high_vol_tradable_co_bo_stock)

gstats.print_statistics (responses, excel_location)