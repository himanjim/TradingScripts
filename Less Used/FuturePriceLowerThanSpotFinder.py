import traceback

from upstox_api.api import *

import DerivativeUtils as outil
import ScrapUtils as nsebse
import Utils as util

upstox_api = util.intialize_upstox_api ([nsebse.NSE_FO])

# futures = nse_bse.get_all_nse_stocks_ids ()
# indices = nse_bse.get_indices ()
# futures = nse_bse.get_nse_fo_stocks ()
# futures.extend (indices)
# stocks_latest_info = nsebse.get_indices ()
# futures = nse_bse.get_nifty50_stocks_latest_info ()
# futures = nse_bse.get_nifty100_stocks_latest_info ()
# futures = [{nse_bse.STOCK_ID: 'HDFC', nse_bse.EXCHANGE: nse_bse.NSE}]
indices = nsebse.get_indices ()
stocks_latest_info = nsebse.get_nse_fo_stocks()
stocks_latest_info.extend (indices)

last_thurs = outil.get_last_thurday_of_month (datetime.today ().month, datetime.today ().year)

futures_higher_than_spot = []

for stock_latest_info in stocks_latest_info:
    try:
        stock_id = outil.get_stock_id (stock_latest_info[nsebse.STOCK_ID])

        future = upstox_api.get_instrument_by_symbol (nsebse.NSE_FO, outil.get_future_symbol (stock_id, last_thurs))

        future_live_feed_data = upstox_api.get_live_feed(future, LiveFeedType.Full)

        if future_live_feed_data['ltp'] > future_live_feed_data['spot_price']:
            margin = (future.lot_size * future_live_feed_data['spot_price']) + outil.get_margin (stock_id,
                                                                                                 datetime.today ().month,
                                                                                                 datetime.today ().year)

            futures_higher_than_spot.append({'symbol': future.symbol, 'profit': (
                    future.lot_size * (future_live_feed_data['ltp'] - future_live_feed_data['spot_price'])), 'roi': (
                                                                                                                                future.lot_size * (
                                                                                                                                    future_live_feed_data[
                                                                                                                                        'ltp'] -
                                                                                                                                    future_live_feed_data[
                                                                                                                                        'spot_price'])) / (
                                                                                                                                margin / 100 / 12),
                                             'future': future, 'feed': future_live_feed_data, 'margin': margin})

    except Exception as e:
        print(traceback.format_exc())

futures_higher_than_spot.sort(key=lambda x: (-x['roi']))

print(futures_higher_than_spot[0])

futures_higher_than_spot.sort(key=lambda x: (x['margin']))

print(futures_higher_than_spot[0])
