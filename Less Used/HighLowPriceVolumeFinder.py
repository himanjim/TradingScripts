import asyncio
import traceback

import time

import DerivativeUtils as d_util
import HighLowPriceVolumeFinderUtils as hlutils
import ScrapUtils as nse_bse

start_time = time.time()

stocks_latest_info = nse_bse.get_all_nse_stocks_ids()
# stocks_latest_info = nse_bse.get_nifty50_stocks_latest_info ()
stocks_latest_info = nse_bse.get_nse_fo_stocks()
# stocks_latest_info = nse_bse.get_indices ()
# stocks_latest_info = nse_bse.get_nifty100_stocks_latest_info ()
# stocks_latest_info = [{nse_bse.STOCK_ID: 'TORNTPHARM', nse_bse.EXCHANGE: nse_bse.NSE}]

option_lots = nse_bse.get_nse_fo_lots ()

# stocks_latest_info[:] = [x for x in stocks_latest_info if
#                          d_util.get_stock_id(x[nse_bse.STOCK_ID]) not in option_lots or (
#                                      d_util.get_stock_id(x[nse_bse.STOCK_ID]) in option_lots and option_lots[
#                                  d_util.get_stock_id(x[nse_bse.STOCK_ID])] <= d_util.NO_OF_LOTS_UPPER_LIMIT)]

stock_live_data = {}


async def fetch_stock_live_price(stock_id, live_data):
    try:
        live_data[stock_id] = d_util.get_equity_live_ltp (stock_id)
    except Exception:
        print (traceback.format_exc (), stock_id)


async def fetch_stock_live_prices(stocks, live_data):
    tasks = []
    for stock in stocks:
        stock_id = d_util.get_stock_id(stock[nse_bse.STOCK_ID])
        tasks.append (asyncio.ensure_future (fetch_stock_live_price (stock_id, live_data)))

    await asyncio.gather (*tasks)


loop = asyncio.get_event_loop()
loop.run_until_complete (fetch_stock_live_prices (stocks_latest_info, stock_live_data))

for stock_latest_info in stocks_latest_info:
    try:

        stock_id = d_util.get_stock_id(stock_latest_info[nse_bse.STOCK_ID])

        ltp = stock_live_data[stock_id][0]

        if ltp is not None:
            open = stock_live_data[stock_id][1]
            high = stock_live_data[stock_id][2]
            low = stock_live_data[stock_id][3]
            prev_close = stock_live_data[stock_id][5]
            volume = stock_live_data[stock_id][6]

            # ltp = float(live_quote['ltp'])
            # open = float(live_quote['open'])
            # high = float(live_quote['high'])
            # low = float(live_quote['low'])
            # prev_close = float(live_quote['close'])
            # volume = int(live_quote['vtt'])
            # hlutils.update_prices(stock_id, ltp, open, high, low, prev_close, volume)

            hlutils.update_prices (stock_id, ltp, open, high, low, prev_close, volume)

    except Exception:
        print (traceback.format_exc (), stock_id)

print ('Gap up:', hlutils.max_price_rise_wrt_yester)
print ('Gap down:', hlutils.max_price_fall_wrt_yester)
print ('Prise rise today:', hlutils.max_price_rise_wrt_today)
print ('Prise down today:', hlutils.max_price_fall_wrt_today)
print ('Bullish marubuzo:', hlutils.bullish_marubuzo)
print ('Bearish marubuzo:', hlutils.bearish_marubuzo)
print ('Volume:', hlutils.max_vol_diff)

print("---Script executed in %s seconds ---" % (time.time() - start_time))
