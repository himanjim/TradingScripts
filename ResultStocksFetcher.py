import os

import GenericStatPrinter as gstats
import ScrapUtils as sUtils
from BSEResultFetcher import *

results_excel_location = ['G:', 'F:'][os.path.exists('F:')] + '/Trading_Responses/Results_' + today_date_str + '.xlsx'

default_headers = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://www.nse-india.com'}

zerodha_margin_url = 'https://api.kite.trade/margins/equity'

page = urlopen(Request(zerodha_margin_url, headers=default_headers))
        # parse the html using beautiful soup and store in variable `soup`
zerodha_margin_data = json.loads(BeautifulSoup(page, 'html.parser').string.strip())

zerodha_MIS_stocks = {}
for data in zerodha_margin_data:
    zerodha_MIS_stocks[data['tradingsymbol']] = data['mis_multiplier']


fo_stocks = sUtils.get_nse_fo_stocks()
fo_stock_ids = []
for fo_stock in fo_stocks:
    fo_stock_ids.append(fo_stock[sUtils.STOCK_ID])

result_stocks = get_bse_result_stocks()

tradable_stocks = []
for result_stock in result_stocks:
    if result_stock[1] in zerodha_MIS_stocks:
        tradable_stock = [result_stock[1], zerodha_MIS_stocks[result_stock[1]]]
        if result_stock[1] in fo_stock_ids:
            tradable_stock.append(sUtils.NSE_FO)
        else:
            tradable_stock.append (None)

        tradable_stock.extend([result_stock[2], result_stock[3]])

        tradable_stocks.append (tradable_stock)

        nse_result_stats = get_nse_result_stats(tradable_stock[0])
        if nse_result_stats is not None:
            tradable_stock.extend(nse_result_stats)
            if len(nse_result_stats) > 4:
                tradable_stock.append (abs(nse_result_stats[0]) + abs(nse_result_stats[1]) + abs(nse_result_stats[2]) + abs(nse_result_stats[3]))
            else:
                tradable_stock.append (None)
        else:
            tradable_stock.extend ([None, None, None, None, None])

if len(tradable_stocks) > 0:
    tradable_stocks.insert(0, ['STOCK', 'LEVER.', 'FO', 'DECLARATION TIME', 'DECLARATION', 'REV. QOQ', 'REV. YOY', 'PROFIT QOQ', 'PROFIT YOY', 'TOTAL CHANGE'])
gstats.print_statistics (tradable_stocks, results_excel_location)