from datetime import datetime
import logging
import os
import math
from TradingScripts.BNScalperUtils import *
import TradingScripts.Utils as util

kite = util.intialize_kite_api()
kws = util.intialize_kite_ticker()

testing = False
parent_orders = {}
# Logging config
today_date_str = datetime.now(indian_timezone).strftime ('%Y-%m-%d')
logFormatter = logging.Formatter("%(asctime)s [%(module)s] [%(levelname)-5.5s]  %(message)s")
logger = logging.getLogger('Scalper')
logger.setLevel(logging.INFO)

# fileHandler = logging.FileHandler(['G:', 'F:'][os.path.exists('F:')] + '/Trading_Responses/logs/Scalping_logs_' + today_date_str + '.log')
# fileHandler.setFormatter(logFormatter)
# logger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)

logging.Formatter.converter = custom_time

candles = {}
order_placed = False
target = None
stop_loss = None


def get_candle_key(tick):
    hr = tick['timestamp'].hour
    min = tick['timestamp'].minute
    return int((hr * 60) + (int(min / 3) * 3))


def add_candle(tick):
    candle_key = get_candle_key(tick)
    if candle_key not in candles:
        candles[candle_key] = {OPEN : tick[LAST_PRICE], LOW : tick[LAST_PRICE]}
    elif tick[LAST_PRICE] <  candles[candle_key][LOW]:
        candles[candle_key][LOW] = tick[LAST_PRICE]


def check_and_place_order(tick):
    global candles

    candle_key = get_candle_key(tick)

    if (candle_key - 3) in candles & (candle_key - 9) in candles:
        prev_candle = candles[(candle_key - 3)]
        old_candle = candles[(candle_key - 9)]

        lp = tick[LAST_PRICE]

        if prev_candle[LOW] < old_candle[LOW] and lp > prev_candle[OPEN] and (lp - prev_candle[LOW]) > MIN_RISE:
            target = lp + TARGET
            stop_loss = lp - STOP_LOSS
            # kite.place_order(tradingsymbol=TRADING_SYMBOL,
            #                  variety=kite.VARIETY_BO,
            #                  exchange=kite.EXCHANGE_NFO,
            #                  transaction_type=kite.TRANSACTION_TYPE_BUY,
            #                  quantity=LOTS,
            #                  order_type=kite.ORDER_TYPE_LIMIT,
            #                  product=kite.PRODUCT_BO,
            #                  price=lp + TRIGGER_POINT,
            #                  trigger_price=None,
            #                  stoploss=STOP_LOSS,
            #                  squareoff=TARGET,
            #                  tag=TAG)
            order_placed = True





while datetime.now(indian_timezone).time () < p_datetime.time(9, 15, 50) and testing is False:
    pass

def on_ticks(ws, ticks):
    # Callback to receive ticks.
    add_candle(ticks[0])

    if order_placed:
        if ticks[0][LAST_PRICE] <=stop_loss:
            print('!!!!FAILED' + str(ticks[0]))
        elif ticks[0][LAST_PRICE] >= target:
            print('@@@@SUCCESS' + str(ticks[0]))

        order_placed = False
    else:
        check_and_place_order(ticks[0])


def on_connect(ws, response):
    # Callback on successful connect.
    # Subscribe to a list of instrument_tokens (RELIANCE and ACC here).
    ws.subscribe([TRADING_SYMBOL])

    # Set RELIANCE to tick in `full` mode.
    ws.set_mode(ws.MODE_FULL, [TRADING_SYMBOL])

def on_close(ws, code, reason):
    # On connection close stop the event loop.
    # Reconnection will not happen after executing `ws.stop()`
    ws.stop()

def on_error(ws, code, reason):
    # On connection close stop the event loop.
    # Reconnection will not happen after executing `ws.stop()`
    ws.connect()


# Assign the callbacks.
kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close
kws.on_error = on_error

# Infinite loop on the main thread. Nothing after this will run.
# You have to use the pre-defined callbacks to manage subscriptions.
kws.connect()

