import asyncio
import pickle
from datetime import datetime

import math
import time

import DerivativeUtils as dutil
import ScrapUtils as nse_bse
import Utils as util

start_time = time.time()

upstox_api = util.intialize_upstox_api ([nse_bse.NSE])

max_bearable_loss = 3000
max_limit_sacrifice = 400

orders = []

today_date = datetime.today().date()
last_thurs = dutil.get_last_thurday_of_month(today_date.month, today_date.year)
# future_lots = dutil.get_nse_fo_lots_on_future(last_thurs)
future_lots = nse_bse.get_nse_fo_lots ()

PRICE_DIFF = 'price_diff'
max_price_rise_wrt_today = {nse_bse.STOCK_ID: None, PRICE_DIFF: -math.inf}
max_price_fall_wrt_today = {nse_bse.STOCK_ID: None, PRICE_DIFF: math.inf}


def update_prices(stock_id, ltp, open):
    price_change_wrt_today = (ltp - open) / open
    if price_change_wrt_today > 0 and price_change_wrt_today > max_price_rise_wrt_today[PRICE_DIFF]:
        max_price_rise_wrt_today[nse_bse.STOCK_ID] = stock_id
        max_price_rise_wrt_today[PRICE_DIFF] = price_change_wrt_today
    elif price_change_wrt_today < 0 and price_change_wrt_today < max_price_fall_wrt_today[PRICE_DIFF]:
        max_price_fall_wrt_today[nse_bse.STOCK_ID] = stock_id
        max_price_fall_wrt_today[PRICE_DIFF] = price_change_wrt_today


async def order1(data):
    stock = orders[0]  # Highest
    if stock[nse_bse.STOCK_ID] in data and data[stock[nse_bse.STOCK_ID]]['ltp'] > data[stock[nse_bse.STOCK_ID]]['open']:
        lot = future_lots[stock[nse_bse.STOCK_ID]]
        stop_loss = util.round_to_tick(max_bearable_loss / lot)
        target = stop_loss * 10

        price = data[stock[nse_bse.STOCK_ID]]['ltp'] + util.round_to_tick(max_limit_sacrifice / lot)
        # upstox_api.place_order (TransactionType.Buy,  # transaction_type
        #                         stock[nse_bse.INSTRUMENT],  # instrument
        #                         lot,  # quantity
        #                         OrderType.StopLossLimit,  # order_type
        #                         ProductType.OneCancelsOther,  # product_type
        #                         price,  # price
        #                         price,  # trigger_price
        #                         0,  # disclosed_quantity
        #                         DurationType.DAY,  # duration
        #                         stop_loss,  # stop_loss
        #                         target,  # square_off
        #                         20)  # trailing_ticks 20 * 0.05

        print('Bought highest: %s at %f with target:%f and stop loss:%f. Data:%s. Current time:%s.' % (
            stock[nse_bse.STOCK_ID], price, target, stop_loss, str(data[stock[nse_bse.STOCK_ID]]), str(datetime.now())))


async def order2(data):
    stock = orders[1]  # Lowest
    if stock[nse_bse.STOCK_ID] in data and data[stock[nse_bse.STOCK_ID]]['ltp'] < data[stock[nse_bse.STOCK_ID]]['open']:
        lot = future_lots[stock[nse_bse.STOCK_ID]]
        stop_loss = util.round_to_tick(max_bearable_loss / lot)
        target = stop_loss * 10

        price = data[stock[nse_bse.STOCK_ID]]['ltp'] - util.round_to_tick(max_limit_sacrifice / lot)
        # upstox_api.place_order (TransactionType.Sell,  # transaction_type
        #                         stock[nse_bse.INSTRUMENT],  # instrument
        #                         lot,  # quantity
        #                         OrderType.StopLossLimit,  # order_type
        #                         ProductType.OneCancelsOther,  # product_type
        #                         price,  # price
        #                         price,  # trigger_price
        #                         0,  # disclosed_quantity
        #                         DurationType.DAY,  # duration
        #                         stop_loss,  # stop_loss
        #                         target,  # square_off
        #                         20)  # trailing_ticks 20 * 0.05

        print('Sold lowest: %s at %f with target:%f and stop loss:%f. Data:%s. Current time:%s.' % (
            stock[nse_bse.STOCK_ID], price, target, stop_loss, str(data[stock[nse_bse.STOCK_ID]]), str(datetime.now())))


async def place_orders(data):
    tasks = list()
    tasks.append(asyncio.ensure_future(order1(data)))
    tasks.append(asyncio.ensure_future(order2(data)))

    await asyncio.gather(*tasks)


no_of_subscribed_symbols = util.get_no_of_subscribed_symbols(upstox_api)
print('No. of subscribed symbols:', no_of_subscribed_symbols)

stock_live_data = pickle.load(open(util.get_instrument_latest_data_file_name(), 'rb'))
while len (stock_live_data) < 150:
    stock_live_data = pickle.load(open(util.get_instrument_latest_data_file_name(), 'rb'))
# stock_live_data = {'TCS19MARFUT':{'ltp': 1.5, 'open': 2}, 'ULTRACEMCO19MARFUT':{'ltp': 1.5, 'open': 2}, 'AMBUJACEM':{'ltp': 1.5, 'open': 2}, 'RAMCOCEM19MARFUT':{'ltp': 1.5, 'open': 2}}
print('Got data. Placing orders at:', datetime.now())
for stock_id, live_quote in stock_live_data.items():
    # if stock_id in future_lots:
    update_prices (stock_id, live_quote['ltp'], live_quote['open'])

orders.append({nse_bse.STOCK_ID: max_price_rise_wrt_today[nse_bse.STOCK_ID],
               nse_bse.INSTRUMENT: upstox_api.get_instrument_by_symbol (nse_bse.NSE,
                                                                        max_price_rise_wrt_today[nse_bse.STOCK_ID])})

orders.append({nse_bse.STOCK_ID: max_price_fall_wrt_today[nse_bse.STOCK_ID],
               nse_bse.INSTRUMENT: upstox_api.get_instrument_by_symbol (nse_bse.NSE,
                                                                        max_price_fall_wrt_today[nse_bse.STOCK_ID])})

loop = asyncio.get_event_loop()
loop.run_until_complete(place_orders(stock_live_data))

print("---Script executed in %s seconds ---" % (time.time() - start_time))
