import asyncio
import statistics
import traceback

import CalculatedCalendarSpreadsLimits as calcul_limit
import DerivativeUtils as outil
import ScrapUtils as nse_bse

min_no_of_sessions_for_spread = 200
# 83% at 2 &1, 87% at 3 & 1.5
max_std_multiple_for_buy = 2
max_std_multiple_for_sell = 1
future_liquidity_margin = .1
min_no_of_bids_asks = 5


async def fetch_future_with_orderbook(stock_id, fetched_futures, last_thurs, index):
    bids, asks, ltp, spot = outil.get_all_bids(stock_id, last_thurs)
    future = outil.Futures()
    future.bids = bids
    future.asks = asks
    future.ltp = ltp
    future.stock_id = stock_id
    future.spot_price = spot
    future.symbol = outil.get_future_symbol (stock_id, last_thurs)

    fetched_futures.insert(index, future)


async def fetch_futures_with_orderbook(stock_id, fetched_futures, current_month_last_thurs, near_month_last_thurs,
                                       far_month_last_thurs):
    tasks = []
    tasks.append(asyncio.ensure_future(
        fetch_future_with_orderbook(stock_id, fetched_futures, current_month_last_thurs, 0)))
    tasks.append(asyncio.ensure_future(
        fetch_future_with_orderbook(stock_id, fetched_futures, near_month_last_thurs, 1)))
    # tasks.append(asyncio.ensure_future(
    #     fetch_future_with_orderbook(stock_id, fetched_futures, far_month_last_thurs, 2)))

    await asyncio.gather(*tasks)


def run_fetch_futures_with_orderbook(stock_id, fetched_futures, current_month_last_thurs, near_month_last_thurs,
                                     far_month_last_thurs):
    loop = asyncio.get_event_loop()

    loop.run_until_complete(
        fetch_futures_with_orderbook(stock_id, fetched_futures, current_month_last_thurs, near_month_last_thurs,
                                     far_month_last_thurs))


async def fetch_future(stock_id, fetched_futures, current_month_last_thurs_yr, near_month_last_thurs_mn,
                       far_month_last_thurs_mn):
    fetched_futures[stock_id] = outil.get_all_futures (stock_id, current_month_last_thurs_yr, near_month_last_thurs_mn,
                                                       far_month_last_thurs_mn)


async def fetch_futures(futures, fetched_futures, current_month_last_thurs_yr, near_month_last_thurs_mn,
                        far_month_last_thurs_mn):
    tasks = []
    for future in futures:
        stock_id = outil.get_stock_id (future[nse_bse.STOCK_ID])
        tasks.append (asyncio.ensure_future (
            fetch_future (stock_id, fetched_futures, current_month_last_thurs_yr, near_month_last_thurs_mn,
                          far_month_last_thurs_mn)))

    await asyncio.gather (*tasks)


def run_fetch_futures(futures, fetched_futures, current_month_last_thurs_yr, near_month_last_thurs_mn,
                      far_month_last_thurs_mn):
    loop = asyncio.get_event_loop ()
    try:
        loop.run_until_complete (
            fetch_futures (futures, fetched_futures, current_month_last_thurs_yr, near_month_last_thurs_mn,
                           far_month_last_thurs_mn))
    finally:
        loop.close ()


def get_instrument_token(tradingsymbol, instruments):
    for instrument in instruments:
        if instrument['tradingsymbol'] == tradingsymbol:
            return instrument['instrument_token']


def check_get_calendar_spread_month_diffs(current_month_historical_data, next_month_historical_data):
    diffs = []
    try:
        for i, j in zip (current_month_historical_data, next_month_historical_data):
            if j['date'] != i['date']:
                raise ValueError ('Dates mismtach' + str (j) + str (i))
            diffs.append(j['ltp'] - i['ltp'])
    except Exception as e:
        print (traceback.format_exc ())

    return diffs


def get_calendar_spread_upper_limit(data, std_multiple_for_buy, std_multiple_for_sell):
    mean = statistics.mean(data)
    std = statistics.stdev(data)

    upper_limit_for_buy = mean + (std_multiple_for_buy * std)
    upper_limit_for_sell = mean + (std_multiple_for_sell * std)

    return upper_limit_for_buy, upper_limit_for_sell


def get_next_calendar_spread(current_month_data, near_far_month_data, pivot, range_upper_limit):
    lm = pivot + 1

    near_far_month_data_type = 'ltp'
    current_month_data_type = 'ltp'

    next_day_spread = near_far_month_data[lm][near_far_month_data_type] - current_month_data[lm][
        current_month_data_type]

    while lm < len (current_month_data) and current_month_data[lm]['date'] < current_month_data[pivot]['expiry']:
        next_day_spread = near_far_month_data[lm][near_far_month_data_type] - current_month_data[lm][
            current_month_data_type]
        if next_day_spread < range_upper_limit:
            break
        lm += 1

    if lm >= len (current_month_data):
        lm -= 1

    return {'spread': next_day_spread, 'date': current_month_data[lm]['date'],
            'current_fut_price': current_month_data[lm][current_month_data_type],
            'near_far_fut_price': near_far_month_data[lm][near_far_month_data_type]}


def get_fair_future_value(spot, days_to_expiry, dividend):
    rbi_risk_free_rate = .083528

    return (spot * (1 + (rbi_risk_free_rate * (days_to_expiry / 365)))) - dividend


def non_0_none(value):
    return value is not 0 and value is not None


def get_calendar_spread_responses(option_lots, stock_id, current_month_last_thurs, near_month_last_thurs,
                                  far_month_last_thurs, futures, margin, debug=True):
    cs_responses = []

    future_current_month_symbol = outil.get_future_symbol (stock_id, current_month_last_thurs)

    future_near_month_symbol = outil.get_future_symbol (stock_id, near_month_last_thurs)

    future_far_month_symbol = outil.get_future_symbol (stock_id, far_month_last_thurs)

    current_near_months_range_upper_limit_for_buy = calcul_limit.calculated_calendar_spread_limits[stock_id][0]

    current_near_months_range_upper_limit_for_sell = calcul_limit.calculated_calendar_spread_limits[stock_id][1]

    current_far_months_range_upper_limit_for_buy = calcul_limit.calculated_calendar_spread_limits[stock_id][2]

    current_far_months_range_upper_limit_for_sell = calcul_limit.calculated_calendar_spread_limits[stock_id][3]

    if current_near_months_range_upper_limit_for_buy is None or current_far_months_range_upper_limit_for_buy is None:
        print ('No limits for stock:' + stock_id)
        return []

    if len (futures) > 1:
        if futures[0].ltp is not None and futures[0].ltp is not 0:

            if futures[1].ltp is not None and futures[1].ltp is not 0:
                spread = futures[1].ltp - futures[0].ltp
                if spread > current_near_months_range_upper_limit_for_buy:
                    # if True:

                    cs_responses.append (
                        [future_current_month_symbol, future_near_month_symbol, futures[1].ltp,
                         futures[0].ltp, current_near_months_range_upper_limit_for_buy,
                         current_near_months_range_upper_limit_for_sell, spread,
                         spread / current_near_months_range_upper_limit_for_buy,
                         (spread - current_near_months_range_upper_limit_for_sell) * option_lots[stock_id], margin,
                         option_lots[stock_id]])
            else:
                if debug:
                    print('No ltp for future:' + str(futures[1]))

            if len (futures) > 2:
                if futures[2].ltp is not None and futures[2].ltp is not 0:
                    spread = futures[2].ltp - futures[0].ltp
                    if spread > current_far_months_range_upper_limit_for_buy:
                        # if True:

                        cs_responses.append (
                            [future_current_month_symbol, future_far_month_symbol, futures[2].ltp,
                             futures[0].ltp, current_far_months_range_upper_limit_for_buy,
                             current_far_months_range_upper_limit_for_sell, spread,
                             spread / current_far_months_range_upper_limit_for_buy,
                             (spread - current_far_months_range_upper_limit_for_sell) * option_lots[stock_id], margin,
                             option_lots[stock_id]])
                else:
                    if debug:
                        print('No ltp for future:' + str(futures[2]))
            else:
                if debug:
                    print(*futures, 'No futures:')

        else:
            if debug:
                print('No ltp for future:' + str(futures[0]))
    else:
        if debug:
            print(*futures, 'No futures:')

    return cs_responses
