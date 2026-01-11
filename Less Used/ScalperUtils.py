import collections
import traceback
from datetime import datetime

from pytz import timezone, utc

import Utils as util
from UpDownGapUtils import get_fo_stock_ids

indian_timezone = timezone('Asia/Calcutta')
orders_count_local_cache = 0
min_lot_percent_of_future_lot = .3
min_trigger_up = 2
today_date = datetime.now(indian_timezone).date()
max_ltp_len_for_secon_order = 10
max_loss_to_bear = -400
trigger_price = 100
TAG = 'SCALP'

max_change = .03

prev_ltps = {}
fo_stock_ids = get_fo_stock_ids('C:/Users/Admin/Desktop/')

stocks_to_buy = []
stocks_to_sell = []
max_amount_for_future_lot = 500000
prev_ltps = {}


def place_next_order(broker_orders, broker_positions, kite, logger):
    try:
        global orders_count_local_cache
        global prev_ltps
        global stocks_to_buy
        global stocks_to_sell

        order_count_wrong = False
        while True:
            placed_order_count = 0
            for broker_order in broker_orders:
                if broker_order['parent_order_id'] is None and broker_order['order_timestamp'].time () > util.MARKET_START_TIME and broker_order['tag'] == TAG:
                    placed_order_count += 1

            if placed_order_count < orders_count_local_cache:
                logger.error ('Fetched orders count:%d not matching local count:%d.' , placed_order_count, orders_count_local_cache)
                broker_orders = kite.orders ()

                order_count_wrong = True
                continue
            else:
                break

        if order_count_wrong:
            logger.info('Fetching positions again.')
            broker_positions = kite.positions ()['day']

        open_order_stocks = []

        for broker_position in broker_positions:
            if broker_position['quantity'] != 0:
                open_order_stocks.append('NSE:' + broker_position['tradingsymbol'].upper ())

        stocks_live_data = None
        while True:
            stocks_live_data = kite.ohlc (fo_stock_ids)
            print (len (fo_stock_ids))
            print(len(stocks_live_data))

            stock_in_uptrend = None
            stock_in_downtrend = None
            for nse_stock_id, stock_live_data in stocks_live_data.items ():
                open_price = stock_live_data['ohlc']['open']
                ltp = stock_live_data['last_price']
                high_price = stock_live_data['ohlc']['high']
                low_price = stock_live_data['ohlc']['low']

                if open_price == 0 or open_price == high_price or open_price == low_price:
                    continue

                # if nse_stock_id not in prev_ltps:
                #     prev_ltps[nse_stock_id] = collections.deque(max_ltp_len_for_secon_order*[None], max_ltp_len_for_secon_order)
                # else:
                #     prev_ltps[nse_stock_id].appendleft(ltp)

                ascent = 0
                descent = 0

                if ltp > open_price and ltp >= high_price:
                    ascent = abs ((ltp - open_price) / open_price)
                elif ltp < open_price and ltp <= low_price:
                    descent = abs ((ltp - open_price) / open_price)

                if stock_in_uptrend is None or (ascent > max_change and ascent > stock_in_uptrend[1]):
                    stock_in_uptrend = [nse_stock_id, ascent, stock_live_data]

                if stock_in_downtrend is None or (descent > max_change and descent > stock_in_downtrend[1]):
                    stock_in_downtrend = [nse_stock_id, descent, stock_live_data]


            if stock_in_uptrend is not None and stock_in_uptrend[1] != 0:
                logger.info('UP:%s', str(stock_in_uptrend))

            if stock_in_downtrend is not None and stock_in_downtrend[1] != 0:
                logger.info ('DN:%s' ,str (stock_in_downtrend))
    except Exception:
        logger.error (traceback.format_exc ())
        if 'blocked' in str(traceback.format_exc ()):
            fo_stock_ids.remove(nse_stock_id)

    return broker_orders, broker_positions, stocks_live_data


def custom_time(*args):
    utc_dt = utc.localize(datetime.utcnow())
    my_tz = indian_timezone
    converted = utc_dt.astimezone(my_tz)
    return converted.timetuple()


def trend_is_correct(ltps_que, trend):
    for ltp in ltps_que:
        if ltp is not None and ((trend == 1 and ltps_que[0] < ltp) or (trend == 0 and ltps_que[0] > ltp)):
            return False

    return True