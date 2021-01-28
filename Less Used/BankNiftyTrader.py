import math
import statistics
from datetime import datetime, timedelta

import DerivativeUtils as outil
import ScrapUtils as sutil
import Utils as util
from upstox_api.api import LiveFeedType, OHLCInterval, OrderType

upstox_api = util.intialize_upstox_api ([sutil.NSE_FO, sutil.NSE_INDEX])

stock = {sutil.STOCK_ID: 'NIFTY_BANK', sutil.EXCHANGE: sutil.NSE_INDEX}

stock_id = outil.get_stock_id (stock[sutil.STOCK_ID])
std_multiple = .5

today = datetime.now ()
current_month_last_thurs = outil.get_last_thurday_of_month (today.month, today.year)
trading_symbol = outil.get_future_symbol (stock_id, current_month_last_thurs)

option_lots = sutil.get_nse_fo_lots ()
lot_multiple = 3

lot = option_lots[stock_id] * lot_multiple

required_margin = outil.get_margin (stock_id, today.month, today.year) * lot_multiple

if util.is_market_open ():
    available_margin = upstox_api.get_balance ()['equity']['available_margin']
    if required_margin > available_margin:
        print('Lower margin: %f than required: %f.' % (available_margin, required_margin))
        exit (0)

    util.unsubscribe_symbols (upstox_api)


def get_bollinger_range(prices):
    _20_session_average = statistics.mean (prices)
    std = std_multiple * statistics.stdev (prices)

    return [_20_session_average + std, _20_session_average - std]


end_date = util.trim_date (datetime.now ())
start_date = util.trim_date (end_date - timedelta (days=1))
bank_nifty = upstox_api.get_instrument_by_symbol(stock[sutil.EXCHANGE], stock[sutil.STOCK_ID])
nifty_prices_hist = upstox_api.get_ohlc(bank_nifty, OHLCInterval.Minute_30, start_date, end_date)

bank_nifty_prices = []
for nifty_price_hist in nifty_prices_hist:
    bank_nifty_prices.append (float (nifty_price_hist['close']))

bb_range = get_bollinger_range (bank_nifty_prices[-20:])

temp_nifty_prices = []
traded = {}

min_break_even_points = 5
target_points = 20
stoploss_points = 40
bank_nifty_min_len_for_bb_update = 2

bb_update_min = 30
bb_update_sec = 50


def event_handler_quote_update(bank_nifty):
    # print(bank_nifty)
    bank_nifty_date = util.get_date_from_timestamp(int(bank_nifty['timestamp']))

    ltp = float(bank_nifty['live_ltp'])

    if (bank_nifty_date.minute + 1) % bb_update_min == 0 and bank_nifty_date.second > bb_update_sec:
        temp_nifty_prices.append(ltp)
        print('updated temp_nifty_prices')

    if bank_nifty_date.minute % bb_update_min == 0 and len(temp_nifty_prices) > bank_nifty_min_len_for_bb_update:
        print('BB update started', bank_nifty)
        bank_nifty_prices.append (statistics.mean (temp_nifty_prices))
        temp_nifty_prices.clear ()
        bb_range[:] = get_bollinger_range (bank_nifty_prices[-20:])
        print('BB update doing', bank_nifty_prices)
        print ('BB updated at %s. BB = %s.' % (bank_nifty_date, bb_range))

    if 'price' not in traded:
        if 'prev_price' in traded:
            if ltp > bb_range[0] > traded['prev_price']:
                outil.buy_instrument(upstox_api, trading_symbol, sutil.NSE_FO, ltp, lot)
                traded.update (
                    {'price': ltp, 'action': 'bought', 'stoploss': ltp - stoploss_points,
                     'target': ltp + target_points, 'max': - math.inf})
                print ('Bought %s at price %f. Time = %s. BB = %s.' % (
                    stock_id, ltp, bank_nifty_date, bb_range))

            elif ltp < bb_range[1] < traded['prev_price']:
                outil.sell_instrument(upstox_api, trading_symbol, sutil.NSE_FO, ltp, lot)
                traded.update (
                    {'price': ltp, 'action': 'sold', 'stoploss': ltp + stoploss_points,
                     'target': ltp - target_points, 'min': math.inf})
                print ('Sold %s at price %f. Time = %s. BB = %s.' % (
                    stock_id, ltp, bank_nifty_date, bb_range))

    else:
        if traded['action'] == 'bought':
            if ltp < traded['stoploss']:
                outil.sell_instrument (upstox_api, trading_symbol, sutil.NSE_FO, None, lot, OrderType.Market)
                print ('Stoploss triggered for %s at price %f, bought at %f. Loss = %f. Time = %s.' % (
                    stock_id, ltp, traded['price'],
                    (traded['price'] - ltp + min_break_even_points) * lot, bank_nifty_date))
                traded.clear ()

            elif ltp > traded['max']:
                traded.update({'max': ltp})

            elif ltp >= traded['target']:
                outil.sell_instrument(upstox_api, trading_symbol, sutil.NSE_FO, ltp, lot)
                print ('Sold %s at price %f, bought at %f. Earned = %f. Time = %s.' % (
                    stock_id, ltp, traded['price'],
                    (ltp - traded['price'] - min_break_even_points) * lot, bank_nifty_date))
                traded.clear ()

        else:
            if ltp > traded['stoploss']:
                outil.buy_instrument (upstox_api, trading_symbol, sutil.NSE_FO, None, lot, OrderType.Market)
                print ('Stoploss triggered for %s at price %f, sold at %f. Loss = %f. Time = %s.' % (
                    stock_id, ltp, traded['price'],
                    (ltp - traded['price'] + min_break_even_points) * lot, bank_nifty_date))
                traded.clear ()

            elif ltp < traded['min']:
                traded.update({'min': ltp})

            elif ltp <= traded['target']:
                outil.buy_instrument(upstox_api, trading_symbol, sutil.NSE_FO, ltp, lot)
                print ('Bought %s at price %f, sold at %f. Earned = %f. Time = %s.' % (
                    stock_id, ltp, traded['price'],
                    (traded['price'] - ltp - min_break_even_points) * lot, bank_nifty_date))
                traded.clear ()

    traded.update({'prev_price': ltp})


if util.is_market_open ():
    upstox_api.subscribe(bank_nifty, LiveFeedType.Full, stock[sutil.EXCHANGE])
    upstox_api.set_on_quote_update (event_handler_quote_update)
    upstox_api.start_websocket (False)
else:
    print ('Market closed so not subscribing')
    nifty_prices_hist = upstox_api.get_ohlc (
        upstox_api.get_instrument_by_symbol (stock[sutil.EXCHANGE], stock[sutil.STOCK_ID]), OHLCInterval.Minute_15,
        datetime (2019, 2, 5).date (), datetime (2019, 2, 6).date ())

    bank_nifty_prices = []
    for nifty_price_hist in nifty_prices_hist:
        bank_nifty_prices.append (float (nifty_price_hist['close']))

    test_data = (
        upstox_api.get_ohlc (upstox_api.get_instrument_by_symbol ('NSE_INDEX', 'NIFTY_BANK'), OHLCInterval.Minute_5,
                             datetime (2019, 2, 7).date (), datetime (2019, 2, 8).date ()))

    bank_nifty_min_len_for_bb_update = 10

    for data in test_data:
        data.update ({'ltp': float (data['close'])})
        event_handler_quote_update (data)
