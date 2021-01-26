from datetime import datetime
import logging
import os
import math
from TradingScripts.BNScalperUtils import *
import TradingScripts.Utils as util
from TradingScripts.GenericStaticPrinter import print_statistics

kite = util.intialize_kite_api()

results_excel_location = 'D:/BackTest/Bn_back_test.xlsx'

candles = {}
order_placed = False
target = None
stop_loss = None
trailed = False
trades = [['ENTRY TIME', 'EXIT TIME', 'SUCCESS', 'P/L']]
trade = []
DATE = 'date'

def get_candle_key(tick):
    hr = tick[DATE].hour
    min = tick[DATE].minute
    return int((hr * 60) + (int(min / 3) * 3))


def add_candle(hist_candle):
    candles[get_candle_key(hist_candle)] = {OPEN : hist_candle[OPEN], LOW : hist_candle[LOW], HIGH : hist_candle[HIGH], CLOSE : hist_candle[CLOSE]}


def check_and_place_order(hist_candle):
    global candles, order_placed, target, stop_loss, trade

    candle_key = get_candle_key(hist_candle)

    if (candle_key - 3) in candles and (candle_key - 9) in candles:
        prev_candle = candles[(candle_key - 3)]
        old_candle = candles[(candle_key - 9)]

        if prev_candle[LOW] < old_candle[LOW] and hist_candle[HIGH] > prev_candle[OPEN] and prev_candle[CLOSE] > prev_candle[OPEN] and (hist_candle[HIGH] - prev_candle[LOW]) > MIN_RISE:
            target = prev_candle[LOW] + MIN_RISE + TARGET
            stop_loss = prev_candle[LOW] + MIN_RISE - STOP_LOSS
            trade.append(hist_candle[DATE].strftime("%b %d %Y %H:%M:%S"))
            print('Order placed:' + str(hist_candle))
            order_placed = True


hist_candles = kite.historical_data(260105, datetime(2020, 10, 19, 0, 0, 0), datetime(2021, 1, 25, 0, 0, 0), '3minute')

for hist_candle in hist_candles:
    add_candle(hist_candle)

    if order_placed is False:
        check_and_place_order(hist_candle)

    if order_placed:
        if hist_candle[LOW] <=stop_loss:
            print('!!!Failed:' + str(hist_candle))

            trade.extend([hist_candle[DATE].strftime("%b %d %Y %H:%M:%S"), 0, [-500, 0][trailed]])
            trades.append(trade)

            order_placed = False
            trailed = False
            trade = []

        elif hist_candle[HIGH] >= target:
            print('!!!SUCCESS:' + str(hist_candle))

            trade.extend([hist_candle[DATE].strftime("%b %d %Y %H:%M:%S"), 1, 1000])
            trades.append(trade)

            order_placed = False
            trailed = False
            trade = []

        # elif hist_candle[HIGH] >= (target - TRAIL):
        #     trailed = True



print_statistics(trades, results_excel_location)



