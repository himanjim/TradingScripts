import pickle
import traceback
from datetime import datetime, timedelta

import ScrapUtils as nse_bse
import Utils as util
from upstox_api.api import OHLCInterval

diff_between_start_end_date = 20
start_date = datetime.now() - timedelta(days=diff_between_start_end_date)
end_date = datetime.now()

upstox_api = util.intialize_upstox_api([nse_bse.NSE, nse_bse.NSE_INDEX])

stocks = []
indices = nse_bse.get_indices()
stocks = nse_bse.get_nse_fo_stocks()
stocks.extend(indices)
stocks = [{nse_bse.STOCK_ID: 'TCS', nse_bse.EXCHANGE: nse_bse.NSE}]

stocks_data_obj = {}


def save_stock(s_id, s_data):
    stocks_data_obj_str = util.get_stock_date_str_for_pickle(s_id,
                                                             util.get_date_from_timestamp(int(s_data[-1]['timestamp'])))

    stocks_data_obj[stocks_data_obj_str] = s_data


for stock in stocks:
    try:
        stock_id = stock[nse_bse.STOCK_ID]

        stock_data = upstox_api.get_ohlc (upstox_api.get_instrument_by_symbol (stock[nse_bse.EXCHANGE], stock_id),
                                          OHLCInterval.Day_1, start_date, end_date)

        print('Fetched stock id:', stock_id)

        save_stock (stock_id, stock_data)

        if stock_id == nse_bse.NIFTY_50:
            save_stock (nse_bse.NIFTY_50_NSE_SYMBOL, stock_data)
        elif stock_id == nse_bse.NIFTY_BANK:
            save_stock (nse_bse.NIFTY_BANK_NSE_SYMBOL, stock_data)

    except Exception as e:
        print(str(traceback.format_exc()) + '\nError in stock:' + stock_id)

pickle.dump(stocks_data_obj, open(util.STOCK_DATA_OBJ_FILE, 'wb'))
