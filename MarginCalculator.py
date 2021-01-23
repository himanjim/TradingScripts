import json
from urllib.request import Request, urlopen

import Utils as util
from Orders import orders as formed_orders
from bs4 import BeautifulSoup

kite = util.intialize_kite_api()

zerodha_margin_url = 'https://api.kite.trade/margins/equity'

page = urlopen(Request(zerodha_margin_url, headers={'User-Agent': 'Mozilla/5.0', 'Referer': 'https://www.nse-india.com'}))
        # parse the html using beautiful soup and store in variable `soup`
zerodha_margin_data = json.loads(BeautifulSoup(page, 'html.parser').string.strip())

zerodha_MIS_stocks = {}
for data in zerodha_margin_data:
    zerodha_MIS_stocks[data['tradingsymbol']] = float(data['mis_multiplier'])

available_margin = kite.margins()['equity']['net']

stock_wise_margins = {}
required_margin = 0
for formed_order in formed_orders:
    margin = (formed_order['last_close'] * (formed_order['future_lot'] / 2)) * (1 / 15.2)
    stock_wise_margins[formed_order['symbol']] = [zerodha_MIS_stocks[formed_order['symbol']], margin]
    required_margin += margin

print('Available margin:%f' %(available_margin))
print('Required margin:%f' %(required_margin))
if required_margin > available_margin:
    print('Short margin:%f' %(available_margin - required_margin))

for key, value in stock_wise_margins.items():
    print(key, value)