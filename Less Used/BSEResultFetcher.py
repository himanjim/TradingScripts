import json
import re
import traceback
from datetime import datetime
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup

today_date_str = datetime.now ().strftime ('%Y-%m-%d')
bse_url_headers = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://www.bseindia.com'}

nse_url_headers = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://www.nse-india.com'}

bse_stock_id_url = 'https://api.bseindia.com/BseIndiaAPI/api/PeerSmartSearch/w?Type=SS&text={0}'

from_date_str_bse = datetime.now ().strftime ('%Y%m%d')
to_date_str_bse = datetime.now ().strftime ('%Y%m%d')
from_date_str_bse = '20200211'
to_date_str_bse = '20200214'
bse_result_url = 'https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w?strCat=Result&strPrevDate={0}&strScrip=&strSearch=P&strToDate={1}&strType=C'

nse_stock_result_stats_url = 'https://www.nseindia.com/marketinfo/companyTracker/resultsCompare.jsp?symbol={0}'

quarter_result_period = '30-Jun-2019'


def get_nse_stock_id(bse_stock_id):
    try:
        return re.compile(r"<span>(\w+)").findall(str(BeautifulSoup (urlopen(Request(bse_stock_id_url.format (bse_stock_id), headers=bse_url_headers)), 'html.parser')).strip ())[0]
    except Exception:
        print (traceback.format_exc () + str (bse_stock_id))

    return None


def get_bse_result_stocks():
    try:
        results_json = json.loads(BeautifulSoup (urlopen(Request(bse_result_url.format (from_date_str_bse, to_date_str_bse), headers=bse_url_headers)), 'html.parser').text)

        result_stocks = []

        for result in results_json['Table']:
            result_stocks. append([result['SCRIP_CD'], get_nse_stock_id(result['SCRIP_CD']), result['NEWS_DT'], result['NEWSSUB']])
    except Exception:
        print (traceback.format_exc ())

    return result_stocks


def get_nse_result_stats(stock_id):
    # try:
    #     soup = BeautifulSoup (urlopen(Request(nse_stock_result_stats_url.format (stock_id), headers=nse_url_headers)), 'html.parser')
    #
    #     if soup.find_all ('td', attrs={'class': 'tabbgimg'})[1].text.strip() != quarter_result_period:
    #         return None
    #
    #     stats = []
    #
    #     for td in soup.find_all('td', attrs={'class': 'highlightedRow'}):
    #         if td.text.strip() == 'Net Sales/Income from Operations':
    #             present_earnings = float(td.parent.contents[2].text.strip())
    #             prev_quarter_earnings = float (td.parent.contents[4].text.strip ())
    #             prev_year_quarter_earnings = float (td.parent.contents[10].text.strip ())
    #             prev_quarter_change = round(((present_earnings - prev_quarter_earnings) / abs(prev_quarter_earnings)) * 100, 2)
    #             stats.append (prev_quarter_change)
    #
    #             prev_year_quarter_change = round(((present_earnings - prev_year_quarter_earnings) / abs(prev_year_quarter_earnings)) * 100, 2)
    #             stats.append(prev_year_quarter_change)
    #
    #         if td.text.strip () == 'Net Profit (+)/Loss (-) for the Period':
    #             present_profit = float (td.parent.contents[2].text.strip ())
    #             prev_quarter_profit = float (td.parent.contents[4].text.strip ())
    #             prev_year_quarter_profit = float (td.parent.contents[10].text.strip ())
    #             prev_quarter_change = round(((present_profit  - prev_quarter_profit) / abs(prev_quarter_profit)) * 100, 2)
    #             stats.append (prev_quarter_change)
    #
    #             prev_year_quarter_change = round(((present_profit - prev_year_quarter_profit) / abs(prev_year_quarter_profit)) * 100, 2)
    #             stats.append (prev_year_quarter_change)
    #
    #     return stats
    # except Exception:
    #     print (traceback.format_exc () + str (stock_id))

    return None

# print(get_nse_result_stats('IDFC'))
# print(get_bse_result_info(get_bse_stock_id('MOTHERSUMI')))
# print(get_nse_stock_id('524709'))