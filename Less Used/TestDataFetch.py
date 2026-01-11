import csv
import statistics
from datetime import timedelta, datetime

import Utils as util
from PatternRecognition import Action
from upstox_api.api import *

upstox_api = util.intialize_upstox_api(['NSE_FO', 'NSE_INDEX'])
nifty_feb_fut = upstox_api.get_instrument_by_symbol('NSE_FO', 'NIFTY19FEBFUT')
nifty_feb_fut_live_feed = upstox_api.get_live_feed(nifty_feb_fut, LiveFeedType.Full)
market_price = float(nifty_feb_fut_live_feed['ltp']) - 2
upstox_api.place_order(TransactionType.Buy,  # transaction_type
                       nifty_feb_fut,  # instrument
                       75,  # quantity
                       OrderType.StopLossLimit,  # order_type
                       ProductType.OneCancelsOther,  # product_type
                       market_price,  # price
                       market_price,  # trigger_price
                       0,  # disclosed_quantity
                       DurationType.DAY,  # duration
                       1.0,  # stop_loss
                       5.0,  # square_off
                       20)  # trailing_ticks 20 * 0.05
exit(0)
# banknifty_fut = upstox_api.get_instrument_by_symbol ('NSE_FO', 'BANKNIFTY19FEBFUT')
# execute = input ("Execute?")
# if int (execute) == 1:
#     upstox_api.place_order (TransactionType.Buy, banknifty_fut, 60, OrderType.Market, ProductType.Delivery, None, None,
#                             None, DurationType.DAY, None, None)
# else:
#     upstox_api.place_order (TransactionType.Sell, banknifty_fut, 60, OrderType.Market, ProductType.Delivery, None, None,
#                             None, DurationType.DAY, None, None)
#
# exit (0)

# print(outil.sell_instrument(upstox_api, 'NIFTY19JANFUT', 'NSE_FO', 10808.0, 75))
# outil.buy_future(upstox_api, 'NIFTY19JANFUT', 'NSE_FO', 10807.0, 75)

banknifty_fut = upstox_api.get_instrument_by_symbol ('NSE_INDEX', 'NIFTY_BANK')
# print(banknifty_fut)
stock_test_data_day1 = (upstox_api.get_ohlc(banknifty_fut, OHLCInterval.Minute_15, datetime(2019, 2, 11).date(),
                                            datetime(2019, 2, 11).date()))
# banknifty_fut = upstox_api.get_instrument_by_symbol ('NSE_FO', 'NIFTY19FEBFUT')
print (stock_test_data_day1)
bank_nifty_prices = []
for nifty_price_hist in stock_test_data_day1:
    bank_nifty_prices.append(float(nifty_price_hist['close']))
bank_nifty_prices.append(27227.8)
bank_nifty_prices[:] = bank_nifty_prices[-20:]
_20_session_average = statistics.mean(bank_nifty_prices)
std = .5 * statistics.stdev(bank_nifty_prices)
print(_20_session_average)
print(std)
print(_20_session_average + std, _20_session_average - std)
exit (0)
# stock_test_data_day2 = (upstox_api.get_ohlc (banknifty_fut, OHLCInterval.Minute_5, datetime (2019, 1, 14).date (),
#                                              datetime (2019, 1, 14).date ()))
# print (stock_test_data_day)
# print (upstox_api.get_live_feed (banknifty_fut, LiveFeedType.Full))
# print (upstox_api.search_instruments ('NSE_FO', "hdfcbank18dec"))
# test = upstox_api.get_instrument_by_symbol('NSE_INDEX', 'NIFTY_MID50')
# print (test)
# stock_test_data_day=(upstox_api.get_ohlc(upstox_api.get_instrument_by_symbol('NSE_FO', 'jublfood18oct'), OHLCInterval.Day_1, datetime(2018,3, 1).date(), datetime(2018, 3, 26).date()))
# stock_live_feed_data = upstox_api.get_live_feed(banknifty_fut, LiveFeedType.Full)
# print(stock_live_feed_data)
# exit(0)
# stock_live_feed_data=upstox_api.get_live_feed(upstox_api.get_instrument_by_symbol('NSE_FO', 'HDFC18MARFUT'), LiveFeedType.Full)
# print(stock_live_feed_data)
# print(stock_test_data_day)
# buy = True
# sell = False
# for data1, data2 in zip (stock_test_data_day1, stock_test_data_day2):
#     if buy:
#         buy_spread = data2['cp'] - data1['cp']
#         if buy_spread > 22:
#             print ('Buy', data1['cp'], data2['cp'])
#             buy = False
#             sell = True
#
#     if sell:
#         sell_spread = data2['cp'] - data1['cp']
#         if (data2['cp'] - data1['cp']) < 22:
#             print ('Sell', data1['cp'], data2['cp'], (buy_spread - sell_spread) * 75)
#             buy = True
#             sell = False
#
# exit(0)

failed = []
success = []
with open ('C:/Users/Admin/Desktop/Technical analysis testing - Sheet1.csv') as csv_file:
    csv_reader = csv.reader (csv_file, delimiter=',')
    line_count = 0
    for row in csv_reader:
        if line_count > 0:
            stock_id = row[0].strip ()
            pattern = row[1].strip ()
            buy_date = datetime.strptime (row[2].strip (), '%d-%m-%Y')
            stop_loss = float (row[4].strip ())
            action = int (row[6].strip ())

            stock_data = upstox_api.get_ohlc (upstox_api.get_instrument_by_symbol ('NSE_EQ', stock_id),
                                              OHLCInterval.Day_1, buy_date, buy_date + timedelta (days=10))

            target = util.get_target (stock_data[0]['open'], stop_loss, .5, [Action.SHORT, Action.LONG][action == 1])

            to_append = [stock_id, buy_date, pattern, action, target, stop_loss]

            for data in stock_data:
                if (action == Action.LONG.value and data['low'] < stop_loss) or (
                        action == Action.SHORT.value and data['high'] > stop_loss):
                    failed.append (to_append)
                    break

                if (action == Action.LONG.value and data['high'] > target) or (
                        action == Action.SHORT.value and data['low'] < target):
                    success.append (to_append)
                    break

        line_count += 1

print ('Success:', len (success))
for suc in success:
    print (suc)

print ('Failed', len (failed))
for fai in failed:
    print (fai)
