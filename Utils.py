# import asyncio
# import csv
import datetime
# import math
# import pickle
# import time
# import traceback
# from datetime import timedelta
# from kiteconnect import KiteTicker
# import DerivativeUtils as d_util
# # import Indicators as ind
# import ScrapUtils as sutils
# import pandas as pd
# from PatternRecognition import *
# from dateutil import parser
from kiteconnect import KiteConnect
# from upstox_api.api import *
import tempfile
NSE = 'NSE:'
NFO = 'NFO:'

UPSTOX_API_KEY = '5LfPWD6ZJh8MqTeHvikvU6USVnK6w1uk7imllV2z'
KITE_API_KEY = '453dipfh64qcl484'
KITE_API_SECRET = 'cnt30fp12ftbzk7s0a84ieqv8wbquer4'

STOCK_DATA_OBJ_FILE = 'F:/Trading_Responses/StockDataObj'
INSTRUMENT_LATEST_DATA_FILE = 'F:/IntrumentLatestDataFileName.txt'
UPSTOX_LATEST_ACCESS_CODE_FILE = 'UpstoxLatestAccessCode.txt'
KITE_LATEST_ACCESS_CODE_FILE = tempfile.gettempdir() + '/KiteLatestAccessCode.txt'
min_tick_size = .05
#
# TRADING_HOLIDAYS = [datetime.datetime (2019, 3, 4).date (), datetime.datetime (2019, 3, 21).date (),
#                     datetime.datetime (2019, 4, 17).date (), datetime.datetime (2019, 4, 19).date (),
#                     datetime.datetime (2019, 5, 1).date (), datetime.datetime (2019, 6, 5).date (),
#                     datetime.datetime (2019, 8, 12).date (), datetime.datetime (2019, 8, 15).date (),
#                     datetime.datetime (2019, 9, 2).date (), datetime.datetime (2019, 9, 10).date (),
#                     datetime.datetime (2019, 10, 2).date (), datetime.datetime (2019, 10, 8).date (),
#                     datetime.datetime (2019, 10, 28).date (), datetime.datetime (2019, 11, 12).date (),
#                     datetime.datetime (2019, 12, 25).date ()]
#
MARKET_START_TIME = datetime.time (9, 15, 0, 100)
MARKET_END_TIME = datetime.time (15, 30, 0)
TRADE_START_TIME = datetime.time (9, 16, 0)
# oco_future_expenses = 500
#
#
# def round_to_tick(x):
#     base = min_tick_size * 100
#     return round(int(base * math.ceil(float(x * 100) / base)) / 100, 2)
#
#
# def get_instrument_latest_data_file_name():
#     with open(INSTRUMENT_LATEST_DATA_FILE, 'r') as the_file:
#         return the_file.readline()
#
#     return None
#
#
# def get_break_even_points(lot):
#     return oco_future_expenses / lot
#
#
# def get_no_of_subscribed_symbols(upstox_api):
#     earlier_subs = upstox_api.get_subscriptions ()
#
#     if earlier_subs is not None and len (earlier_subs) > 0:
#         if 'FULL' in earlier_subs:
#             return len (earlier_subs['FULL'])
#
#         elif 'LTP' in earlier_subs:
#             return len (earlier_subs['LTP'])
#
#     return 0
#
#
# def unsubscribe_symbols(upstox_api):
#     earlier_subs = upstox_api.get_subscriptions()
#
#     if earlier_subs is not None and len(earlier_subs) > 0:
#         if 'FULL' in earlier_subs:
#             prev_subs = earlier_subs['FULL']
#             for prev_sub in prev_subs:
#                 upstox_api.unsubscribe(upstox_api.get_instrument_by_symbol(prev_sub['exchange'], prev_sub['symbol']),
#                                        LiveFeedType.Full)
#                 time.sleep(.5)
#
#         elif 'LTP' in earlier_subs:
#             prev_subs = earlier_subs['LTP']
#             for prev_sub in prev_subs:
#                 upstox_api.unsubscribe(upstox_api.get_instrument_by_symbol(prev_sub['exchange'], prev_sub['symbol']),
#                                        LiveFeedType.LTP)
#                 time.sleep(.5)
#
#
# def is_trade_time():
#     if datetime.datetime.now ().time () > TRADE_START_TIME:
#         return True
#
#     return False
#
#
# def is_market_open():
#     now = datetime.datetime.now()
#
#     if now.date() in TRADING_HOLIDAYS or now.date().weekday() > 4:
#         return False
#
#     if now.time() < MARKET_START_TIME or now.time() > MARKET_END_TIME:
#         return False
#
#     return True
#
#
# async def fetch_stock_data(stock_id, fetched_stocks_data, upstox_api, start_date, end_date, exchange, stocks_data_obj,
#                            fetch_livefeed):
#     time.sleep (.5)
#     try:
#         fetched_stocks_data[stock_id] = get_stock_latest_data(stock_id, upstox_api, start_date, end_date, exchange,
#                                                           stocks_data_obj, fetch_livefeed)
#     except Exception as e:
#         print(traceback.format_exc())
#
#
# async def fetch_stocks_data(stocks, fetched_stocks_data, upstox_api, start_date, end_date, stocks_data_obj,
#                             fetch_livefeed):
#     tasks = []
#     for stock in stocks:
#         tasks.append(asyncio.ensure_future(
#             fetch_stock_data(stock[sutils.STOCK_ID], fetched_stocks_data, upstox_api, start_date, end_date,
#                              stock[sutils.EXCHANGE], stocks_data_obj, fetch_livefeed)))
#
#     await asyncio.gather(*tasks)
#
#
# def run_fetch_stocks_data(stocks_latest_info, fetched_stocks_data, upstox_api, start_date, end_date, stocks_data_obj,
#                           fetch_livefeed):
#     loop = asyncio.get_event_loop()
#     try:
#         loop.run_until_complete(
#             fetch_stocks_data(stocks_latest_info, fetched_stocks_data, upstox_api, start_date, end_date,
#                               stocks_data_obj, fetch_livefeed))
#     finally:
#         loop.close()
#
#
# def get_stock_date_str_for_pickle(stock_id, end_date):
#     return stock_id + '#' + end_date.strftime("%Y_%m_%d")
#
#
# def cancel_order(upstox_api, symbol):
#     orders = upstox_api.get_order_history()
#
#     for order in orders:
#         if order['symbol'] == symbol:
#             upstox_api.cancel_order(order['order_id'])
#
#
# def check_if_instrument_in_positions(upstox_api, symbol, time_out):
#     start_time = time.time ()
#
#     while True:
#         for position in upstox_api.get_positions():
#             if position['symbol'] == symbol:
#                 return True
#             if (time.time() - start_time) > time_out:
#                 return False
#             time.sleep(2)
#
#     return False
#
#
# def not_too_long_or_short_candle(stock_data, last_10_day_stock_data):
#     # open = stock_data['open']
#     # close = [stock_data['close'], stock_data['cp']][stock_data['cp'] > stock_data['close']]
#     # range = (abs (close - open) / ((open + close) / 2)) * 100
#     # return 1 < range < 20
#     total_candle_length = 0
#
#     for stock in last_10_day_stock_data:
#         total_candle_length += abs (stock['close'] - stock['open'])
#
#     return abs (stock_data['close'] - stock_data['open']) >= total_candle_length / len (last_10_day_stock_data)
#
#
# def get_panda_series_of_stock_closing_prices(stock_data,type_of_price='close'):
#     stock_data_closing_prices = []
#     for stock_session_data in stock_data:
#         stock_data_closing_prices.append(stock_session_data[type_of_price])
#
#     return pd.Series(stock_data_closing_prices)
#
#
# def nearly_equal(var1, var2, variation_percent):
#     return math.isclose (var1, var2, rel_tol=variation_percent / 100)
#
#
# def get_current_day_current_price(stock_data):
#     stock_data_date = datetime.datetime.fromtimestamp(stock_data['timestamp'] / 1000)
#     if (stock_data_date.date () == datetime.datetime.today ().date ()) and is_market_open ():
#         return stock_data['cp']
#     else:
#         return stock_data['close']
#
#
# def calculate_last_10_days_average_volume(last_10_day_stock_data):
#     return sum (stock['volume'] for stock in last_10_day_stock_data) / len (last_10_day_stock_data)
#
#
# # def intialize_upstox_api(contracts):
# #     with open (UPSTOX_LATEST_ACCESS_CODE_FILE, 'r') as the_file:
# #         upstox_latest_access_code = the_file.readline ()
# #
# #     upstox_api = Upstox (UPSTOX_API_KEY, upstox_latest_access_code)
# #
# #     for contract in contracts:
# #         upstox_api.get_master_contract (contract)
# #
# #     return upstox_api
#
#
def intialize_kite_api():
    with open (KITE_LATEST_ACCESS_CODE_FILE, 'r') as the_file:
        kite_latest_access_code = the_file.readline ()

    kite = KiteConnect (api_key=KITE_API_KEY)

    try:

        kite.set_access_token(kite_latest_access_code)
    except Exception as e:
        print("Authentication failed", str(e))
        raise

    return kite
#
#
# def intialize_kite_ticker():
#     with open (KITE_LATEST_ACCESS_CODE_FILE, 'r') as the_file:
#         kite_latest_access_code = the_file.readline ()
#
#     kite = KiteConnect (api_key=KITE_API_KEY)
#
#     return KiteTicker(KITE_API_KEY, kite_latest_access_code)
#
#
# def trim_date(end_date):
#     new_date = end_date
#     while new_date in TRADING_HOLIDAYS:
#         new_date = new_date - timedelta (days=1)
#
#     while new_date.weekday () > 4:
#         new_date = new_date - timedelta (days=1)
#
#     return new_date
#
#
# # def get_stock_latest_data(stock_id, upstox_api, start_date, end_date, exchange, stocks_data_obj=None,
# #                           fetch_livefeed=True, interval=OHLCInterval.Day_1):
# #     stock_data = None
# #
# #     end_date = trim_date (end_date)
# #     if stocks_data_obj is not None:
# #         stocks_data_obj_key = get_stock_date_str_for_pickle(stock_id, end_date)
# #         if stocks_data_obj_key in stocks_data_obj:
# #             stock_data = stocks_data_obj[stocks_data_obj_key]
# #
# #     if stock_data is None:
# #         print('Empty cache for stock', stock_id)
# #         stock_data = upstox_api.get_ohlc (upstox_api.get_instrument_by_symbol (exchange, stock_id), interval,
# #                                           start_date, end_date)
# #
# #         if stocks_data_obj is not None:
# #             stocks_data_obj_key = get_stock_date_str_for_pickle (stock_id,
# #                                                                  get_date_from_timestamp(
# #                                                                      int(stock_data[-1]['timestamp'])))
# #             stocks_data_obj[stocks_data_obj_key] = stock_data
# #             pickle.dump (stocks_data_obj, open (STOCK_DATA_OBJ_FILE, 'wb'))
# #
# #     if fetch_livefeed:
# #         stock_live_feed_data = upstox_api.get_live_feed (upstox_api.get_instrument_by_symbol (exchange, stock_id),
# #                                                          LiveFeedType.Full)
# #
# #         live_feed_date = datetime.datetime.fromtimestamp(stock_live_feed_data['timestamp'] / 1000)
# #         if live_feed_date.date () == datetime.datetime.today ().date ():
# #             volume = 0
# #             if 'vtt' in stock_live_feed_data:
# #                 volume = stock_live_feed_data['vtt']
# #
# #             stock_data.append({'timestamp': stock_live_feed_data['timestamp'], 'open': stock_live_feed_data['open'],
# #                                'high': stock_live_feed_data['high'], 'low': stock_live_feed_data['low'],
# #                                'close': stock_live_feed_data['ltp'], 'volume': volume,
# #                                'cp': stock_live_feed_data['ltp'], 'yearly_low': stock_live_feed_data['yearly_low'],
# #                                'yearly_high': stock_live_feed_data['yearly_high']})
# #
# #         for stock in stock_data:
# #             stock.update(
# #                 {'yearly_low': stock_live_feed_data['yearly_low'], 'yearly_high': stock_live_feed_data['yearly_high']})
# #
# #     for stock in stock_data:
# #         stock['close'] = float(stock['close'])
# #         stock['open'] = float(stock['open'])
# #         stock['high'] = float(stock['high'])
# #         stock['low'] = float(stock['low'])
# #         stock['timestamp'] = int(stock['timestamp'])
# #         if 'volume' in stock:
# #             stock['volume'] = int(stock['volume'])
# #
# #     return stock_data
#
#
# def get_future_historical_data(stock_id, future_current_month_historical_data, future_near_month_historical_data,
#                                future_far_month_historical_data):
#
#     with open(d_util.FUTURE_FILE_LOCATION + stock_id + d_util.FUTURE_FILE_SUFFIX) as csvfile:
#         futures_data = csv.reader(csvfile, delimiter=',', quotechar='"')
#         row_count = 0
#
#         for future_data in futures_data:
#             if row_count > 0:
#                 try:
#                     data_set = {'close': float (future_data[6].strip ()), 'high': float (future_data[4].strip ()),
#                                 'low': float (future_data[5].strip ()), 'ltp': float (future_data[7].strip ()),
#                                 'date': parser.parse (future_data[1].strip ()),
#                                 'expiry': parser.parse (future_data[2].strip ())}
#                     if row_count % 3 == 1:
#                         future_current_month_historical_data.append (data_set)
#                     if row_count % 3 == 2:
#                         future_near_month_historical_data.append (data_set)
#                     if row_count % 3 == 0:
#                         future_far_month_historical_data.append (data_set)
#                 except Exception as e:
#                     print (traceback.format_exc (), ' in data:' + str (future_data))
#
#             row_count += 1
#
#
# def get_equity_historical_data(stock_id):
#     data = []
#
#     with open(d_util.EQ_FILE_LOCATION + stock_id + d_util.EQ_FILE_SUFFIX) as csvfile:
#         equity_datas = csv.reader (csvfile, delimiter=',', quotechar='"')
#         row_count = 0
#
#         for equity_data in equity_datas:
#             if row_count > 0:
#                 try:
#                     data_set = {'open': float (equity_data[4].strip ()), 'close': float (equity_data[8].strip ()),'high': float (equity_data[5].strip ()), 'low': float (equity_data[6].strip ()), 'ltp': float (equity_data[7].strip ()), 'date': parser.parse (equity_data[2].strip ()), 'symbol': equity_data[0].strip (), 'prev_close': float (equity_data[3].strip ()), 'volume': float (equity_data[10].strip ())}
#                     data.append (data_set)
#                 except Exception as e:
#                     print (traceback.format_exc (), ' in data:' + str (equity_data))
#
#             row_count += 1
#
#     return data
#
#
# # def get_rsi_14_9_period_SMA(stock_data_closing_prices_series):
# #     rsi_series = ind.rsi (stock_data_closing_prices_series, 14)
# #     sma = ind.sma (rsi_series, 9)
# #     return sma
#
#
# def get_date_from_timestamp(timestamp):
#     return datetime.datetime.fromtimestamp(timestamp / 1000)
#
#
# def is_number(s):
#     try:
#         float (s)
#         return True
#     except ValueError:
#         return False
#
#
# def remove_non_no_chars(s):
#     return re.sub('[^0-9\\.]', '', s)
#
#
# def remove_non_alphanum_chars(s):
#     return re.sub('[^0-9a-zA-Z]', '', s)
#
#
# def convert_nav_str_to_str(s):
#     if s is None:
#         return ''
#     return s.string
#
#
# def get_target(buy_price, stoploss, rrr, action):
#     if action.value == Action.LONG.value:
#         return (rrr * (buy_price - stoploss)) + buy_price
#     else:
#         return buy_price - (rrr * (stoploss - buy_price))
