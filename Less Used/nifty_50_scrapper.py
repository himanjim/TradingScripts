from urllib.request import Request, urlopen

from bs4 import BeautifulSoup

# query the website and return the html to the variable ‘page’
implied_volatility_url = "https://www.nseindia.com/live_market/dynaContent/live_watch/option_chain/optionKeys.jsp?segmentLink=17&instrument=OPTSTK&symbol=HDFCBANK&date=27DEC2018"


def get_implied_volatility(stock_id, strike, month):
    hdr = {'User-Agent': 'Mozilla/5.0'}
    req = Request (implied_volatility_url, headers=hdr)
    page = urlopen (req)
    # parse the html using beautiful soup and store in variable `soup`
    soup = BeautifulSoup (page, "html.parser")
    stock_price_rows = soup.findAll ("tr")

    for stock_price_row in stock_price_rows:
        for child in stock_price_row.children:
            if child.string and child.string.strip ().startswith (strike):
                return (child.parent.find_all ("td")[18].string)
