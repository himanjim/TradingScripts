import asyncio
import time
import traceback

from CSPCPFinderUtils import *

diff_between_start_end_date = 365
min_return_monthly = 10

start_time = time.time ()

upstox_api = util.intialize_upstox_api([nse_bse.NSE_FO, nse_bse.NSE, nse_bse.NSE_INDEX])
# upstox_api = None

# stocks_latest_info = nse_bse.get_all_nse_stocks_ids ()
indices = nse_bse.get_indices ()
stocks_latest_info = nse_bse.get_nse_fo_stocks ()
stocks_latest_info.extend (indices)
# stocks_latest_info = nse_bse.get_indices ()
# stocks_latest_info = nse_bse.get_nifty50_stocks_latest_info ()
# stocks_latest_info = nse_bse.get_nifty100_stocks_latest_info ()
# stocks_latest_info = [{nse_bse.STOCK_ID: 'CADILAHC', nse_bse.EXCHANGE: nse_bse.NSE}, {nse_bse.STOCK_ID: 'UBL', nse_bse.EXCHANGE: nse_bse.NSE}]
# stocks_latest_info = [{nse_bse.STOCK_ID: 'BANKNIFTY', nse_bse.EXCHANGE: nse_bse.NSE_INDEX}]

required_margins = outil.get_future_margins (stocks_latest_info)

if upstox_api is not None:
    if util.is_market_open():
        available_margin = upstox_api.get_balance()['equity']['available_margin']

        stocks_latest_info[:] = [x for x in stocks_latest_info if
                                 required_margins[outil.get_stock_id(x[nse_bse.STOCK_ID])] <= (.95 * available_margin)]
else:
    print('Upstox API not initialized.')


today_date = datetime.today ().date ()

current_month_last_thurs = outil.get_last_thurday_of_month(today_date.month, today_date.year)

near_month_last_thurs = current_month_last_thurs + relativedelta(months=+1)

no_of_days_till_last_thurs = current_month_last_thurs.day - today_date.day + 1

pcr_responses = []
curr_month_illiquid_stocks = []
near_month_illiquid_stocks = []

option_lots = nse_bse.get_nse_fo_lots ()


async def fetch_options(stock_id, fetched_options, thurs):
    fetched_options.extend(outil.get_all_strikes(stock_id, thurs.month, thurs.year))


async def fetch_future(stock_id, futures, thurs):
    bids, asks, ltp, spot, high, low, open_price, volume = outil.get_all_bids (stock_id, thurs)
    future = outil.Futures ()
    future.bids = bids
    future.asks = asks
    future.ltp = ltp
    future.spot_price = spot
    future.stock_id = stock_id
    future.symbol = outil.get_future_symbol(stock_id, current_month_last_thurs)
    future.open = open_price
    future.high = high
    future.low = low
    future.volume = volume
    futures.append (future)


async def fetch_options_futures(stock_id, options1, options2, futures1, futures2):
    tasks = []

    if stock_id not in curr_month_illiquid_stocks:
        tasks.append(asyncio.ensure_future(fetch_options(stock_id, options1, current_month_last_thurs)))
        tasks.append(asyncio.ensure_future(fetch_future(stock_id, futures1, current_month_last_thurs)))
    else:
        print('Stock marked illiquid in current month', stock_id)

    if stock_id not in near_month_illiquid_stocks:
        tasks.append(asyncio.ensure_future(fetch_options(stock_id, options2, near_month_last_thurs)))
        tasks.append(asyncio.ensure_future(fetch_future(stock_id, futures2, near_month_last_thurs)))
    else:
        print('Stock marked illiquid in near month', stock_id)

    await asyncio.gather (*tasks)


def run_fetch_options_futures(stock_id, options1, options2, futures1, futures2):
    loop = asyncio.get_event_loop ()
    loop.run_until_complete(fetch_options_futures(stock_id, options1, options2, futures1, futures2))


max_earning = [{'earning': 0, 'symbol_ltps': None}]


while True:
    for stock_latest_info in stocks_latest_info:
        try:

            stock_id = outil.get_stock_id(stock_latest_info[nse_bse.STOCK_ID])

            stock_options_curr = []
            stock_options_near = []
            futures_curr = []
            futures_near = []
            print('###Fetching stock:', stock_id)
            run_fetch_options_futures(stock_id, stock_options_curr, stock_options_near, futures_curr, futures_near)

            if stock_id not in curr_month_illiquid_stocks:
                future_curr = None
                if len(futures_curr) > 0:
                    future_curr = futures_curr[0]

                    if check_pcr_future(future_curr):

                        stock_options_curr[:] = [x for x in stock_options_curr if check_pcr_option(x)]

                        if len(stock_options_curr) > 0:

                            for option in stock_options_curr:
                                if option.is_call:
                                    option_call_curr = option

                                    put_options = [x for x in stock_options_curr if
                                                   x.strike_price == option_call_curr.strike_price and x.is_call is False]

                                    if len(put_options) > 0:
                                        option_put_curr = put_options[0]

                                        future_curr.ltp = max (x['price'] for x in future_curr.bids)
                                        option_put_curr.ltp = max (x['price'] for x in option_put_curr.bids)
                                        option_call_curr.ltp = min (x['price'] for x in option_call_curr.asks)

                                        try_long_pcr (upstox_api, option_call_curr, option_put_curr, future_curr,
                                                      option_lots[stock_id], max_earning)

                                        future_curr.ltp = min (x['price'] for x in future_curr.asks)
                                        option_put_curr.ltp = min (x['price'] for x in option_put_curr.asks)
                                        option_call_curr.ltp = max (x['price'] for x in option_call_curr.bids)

                                        try_short_pcr (upstox_api, option_call_curr, option_put_curr, future_curr,
                                                       option_lots[stock_id], max_earning)
                        else:
                            curr_month_illiquid_stocks.append(stock_id)
                    else:
                        curr_month_illiquid_stocks.append(stock_id)
                else:
                    curr_month_illiquid_stocks.append (stock_id)

            if stock_id not in near_month_illiquid_stocks:
                future_near = None
                if len(futures_near) > 0:
                    future_near = futures_near[0]

                    if check_pcr_future(future_near):

                        stock_options_near[:] = [x for x in stock_options_near if check_pcr_option(x)]

                        if len(stock_options_near) > 0:

                            for option in stock_options_near:
                                if option.is_call:
                                    option_call_near = option

                                    put_options = [x for x in stock_options_near if
                                                   x.strike_price == option_call_near.strike_price and x.is_call is False]

                                    if len(put_options) > 0:
                                        option_put_near = put_options[0]

                                        future_near.ltp = max (x['price'] for x in future_near.bids)
                                        option_put_near.ltp = max (x['price'] for x in option_put_near.bids)
                                        option_call_near.ltp = min (x['price'] for x in option_call_near.asks)

                                        try_long_pcr (upstox_api, option_call_near, option_put_near, future_near,
                                                      option_lots[stock_id], max_earning)

                                        future_curr.ltp = min (x['price'] for x in future_curr.asks)
                                        option_put_curr.ltp = min (x['price'] for x in option_put_curr.asks)
                                        option_call_curr.ltp = max (x['price'] for x in option_call_curr.bids)

                                        try_short_pcr (upstox_api, option_call_near, option_put_near, future_near,
                                                       option_lots[stock_id], max_earning)
                        else:
                            near_month_illiquid_stocks.append(stock_id)
                    else:
                        near_month_illiquid_stocks.append(stock_id)
                else:
                    near_month_illiquid_stocks.append (stock_id)

        except Exception:
            print(traceback.format_exc())

    print(max_earning[0]['earning'])
    for x in max_earning[0]['symbol_ltps']:
        print(x)
    exit (0)


print ("---Script executed in %s seconds ---" % (time.time () - start_time))
