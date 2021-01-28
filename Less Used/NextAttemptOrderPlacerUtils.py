import collections
import datetime as parent_datetime
import math
import traceback
from datetime import datetime

import Utils as util
from Orders import orders as formed_orders
from pytz import timezone, utc

indian_timezone = timezone('Asia/Calcutta')
orders_count_local_cache = 0
min_lot_percent_of_future_lot = .3
min_trigger_up = 2
today_date = datetime.now(indian_timezone).date()
LAST_TIME_FOR_FIRST_ORDER = parent_datetime.time (9, 16, 58)
LAST_TIME_FOR_ALL_ORDERS = parent_datetime.time (11, 0, 0)
max_ltp_len_for_secon_order = 20

prev_ltps = {}


def place_next_order(broker_orders, broker_positions, kite, logger, max_loss_to_bear, testing):
    global orders_count_local_cache
    global prev_ltps
    stocks_live_data = {}

    order_count_wrong = False
    while True:
        placed_order_count = 0
        for broker_order in broker_orders:
            if broker_order['parent_order_id'] is None and broker_order['order_timestamp'].time () > util.MARKET_START_TIME:
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

    for formed_order in formed_orders:
        try:
            stock_live_data = kite.quote (formed_order['nse_symbol'])[formed_order['nse_symbol']]
            any_old_data = False
            if testing is False and today_date > stock_live_data['last_trade_time'].date ():
                logger.error ('Old data:' + str (stock_live_data) + formed_order['symbol'])
                any_old_data = True

            if any_old_data:
                break

            stocks_live_data[formed_order['nse_symbol']] = stock_live_data
            if formed_order['nse_symbol'] not in prev_ltps:
                prev_ltps[formed_order['nse_symbol']] = collections.deque(max_ltp_len_for_secon_order*[None], max_ltp_len_for_secon_order)
            else:
                prev_ltps[formed_order['nse_symbol']].appendleft(stock_live_data['last_price'])

            if testing is False and stock_live_data['timestamp'].time() >= LAST_TIME_FOR_ALL_ORDERS :
                continue

            open_position = False
            for broker_position in broker_positions:
                if formed_order['symbol'].upper () == broker_position['tradingsymbol'].upper () and broker_position is not None and broker_position['quantity'] != 0:
                    open_position = True
                    break

            if open_position is True:
                continue

            placed_buy_order_count = 0
            placed_sell_order_count = 0

            pending_order = False

            for broker_order in broker_orders:
                order_time = broker_order['order_timestamp']
                if formed_order['symbol'].upper () == broker_order['tradingsymbol'].upper ():
                    if broker_order['parent_order_id'] is None and order_time is not None and order_time.time() > util.MARKET_START_TIME:

                        if broker_order['status'].upper() == 'COMPLETE' or broker_order['status'].upper() == 'TRIGGER PENDING' or broker_order['status'].upper() == 'OPEN' or broker_order['status'].upper() == 'CANCELLED':
                            if broker_order['transaction_type'].upper () == 'BUY':
                                placed_buy_order_count += 1
                            else:
                                placed_sell_order_count += 1

                        if broker_order['status'].upper() == 'TRIGGER PENDING' or broker_order['status'].upper() == 'OPEN':
                             pending_order = True

            if pending_order is  True or (placed_buy_order_count >= 2 and placed_sell_order_count >= 2):
                continue

            open_price = stock_live_data['ohlc']['open']
            ltp = stock_live_data['last_price']
            high_price = stock_live_data['ohlc']['high']
            low_price = stock_live_data['ohlc']['low']

            if placed_buy_order_count < 2:

                if placed_buy_order_count == 1:
                    second_order_trigger = min_trigger_up * formed_order['trigger_price_pts']
                    high_price = open_price - formed_order['trigger_price_pts'] + (abs(max_loss_to_bear) / (min_lot_percent_of_future_lot * formed_order['future_lot']))

                if (placed_buy_order_count == 0 and ltp > (open_price + formed_order['trigger_price_pts'])) or (placed_buy_order_count == 1 and ltp > (high_price - second_order_trigger)):

                    if placed_buy_order_count == 1 and trend_is_correct(prev_ltps[formed_order['nse_symbol']], 1) is False:
                        logger.info ('No uptrend for stock:%s. Data:%s', formed_order['symbol'].upper (),str (prev_ltps[formed_order['nse_symbol']]))
                        continue

                    price = ltp + ([1, min_trigger_up][placed_buy_order_count == 0] * formed_order['trigger_price_pts'])
                    trigger_price = open_price - formed_order['trigger_price_pts']

                    lot = abs(max_loss_to_bear) / (ltp - trigger_price)
                    lot = math.ceil ([(formed_order['future_lot'] / 2), lot][lot < (formed_order['future_lot'] / 2)])

                    if lot < int(min_lot_percent_of_future_lot * formed_order['future_lot']):
                        logger.info ('Very low lot no:%d for stock:%s. Data:%s', lot, formed_order['symbol'].upper (), str (stock_live_data))
                        if (placed_buy_order_count == 1 and stock_live_data['timestamp'].time () >= LAST_TIME_FOR_FIRST_ORDER) or stock_live_data['timestamp'].time () < LAST_TIME_FOR_FIRST_ORDER:
                            continue
                        elif placed_buy_order_count == 0:
                            lot *= 2
                            lot = math.ceil ([(min_lot_percent_of_future_lot * formed_order['future_lot']), lot][lot < (min_lot_percent_of_future_lot * formed_order['future_lot'])])

                    kite.place_order (tradingsymbol=formed_order['symbol'],
                                      variety=kite.VARIETY_CO,
                                      exchange=kite.EXCHANGE_NSE,
                                      transaction_type=kite.TRANSACTION_TYPE_BUY,
                                      quantity=lot,
                                      order_type=kite.ORDER_TYPE_LIMIT,
                                      product=kite.PRODUCT_CO,
                                      price=price,
                                      trigger_price=trigger_price,
                                      tag=formed_order['tag'])

                    orders_count_local_cache += 1

                    logger.info ('Placed %d B order for stock:%s. Data:%s', (placed_buy_order_count + 1), formed_order['symbol'].upper (), str (stock_live_data))
                    continue

            if placed_sell_order_count < 2:

                if placed_sell_order_count == 1:
                    second_order_trigger = min_trigger_up * formed_order['trigger_price_pts']
                    low_price = open_price + formed_order['trigger_price_pts'] - (abs(max_loss_to_bear) / (min_lot_percent_of_future_lot * formed_order['future_lot']))

                if (placed_sell_order_count == 0 and ltp < (open_price - formed_order['trigger_price_pts'])) or (placed_sell_order_count == 1 and ltp < (low_price + second_order_trigger)):

                    if placed_sell_order_count == 1 and trend_is_correct(prev_ltps[formed_order['nse_symbol']], 0) is False:
                        logger.info ('No downtrend for stock:%s. Data:%s', formed_order['symbol'].upper (), str (prev_ltps[formed_order['nse_symbol']]))
                        continue

                    price = ltp - ([1, min_trigger_up][placed_sell_order_count == 0] * formed_order['trigger_price_pts'])
                    trigger_price = open_price + formed_order['trigger_price_pts']

                    lot = abs(max_loss_to_bear) / (trigger_price - ltp)
                    lot = math.ceil ([(formed_order['future_lot'] / 2), lot][lot < (formed_order['future_lot'] / 2)])

                    if lot < int(min_lot_percent_of_future_lot * formed_order['future_lot']):
                        logger.info ('Very low lot no:%d for stock:%s. Data:%s', lot, formed_order['symbol'].upper (), str (stock_live_data))

                        if (placed_sell_order_count == 1 and stock_live_data['timestamp'].time () >= LAST_TIME_FOR_FIRST_ORDER) or stock_live_data['timestamp'].time () < LAST_TIME_FOR_FIRST_ORDER:
                           continue
                        elif placed_sell_order_count == 0:
                            lot *= 2
                            lot = math.ceil ([(min_lot_percent_of_future_lot * formed_order['future_lot']), lot][lot < (min_lot_percent_of_future_lot * formed_order['future_lot'])])

                    kite.place_order (tradingsymbol=formed_order['symbol'],
                                      variety=kite.VARIETY_CO,
                                      exchange=kite.EXCHANGE_NSE,
                                      transaction_type=kite.TRANSACTION_TYPE_SELL,
                                      quantity=lot,
                                      order_type=kite.ORDER_TYPE_LIMIT,
                                      product=kite.PRODUCT_CO,
                                      price=price,
                                      trigger_price=trigger_price,
                                      tag=formed_order['tag'])

                    orders_count_local_cache += 1
                    logger.info ('Placed %d S order for stock:%s. Data:%s', (placed_sell_order_count + 1) ,formed_order['symbol'].upper (), str (stock_live_data))

        except Exception:
            logger.error (traceback.format_exc () + str(formed_order) + str(stocks_live_data))

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