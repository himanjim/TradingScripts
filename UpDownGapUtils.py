import csv
from datetime import datetime

import Utils as util
from pytz import timezone

indian_timezone = timezone('Asia/Calcutta')
today_date = datetime.now(indian_timezone).date()
today_date_str = datetime.now(indian_timezone).strftime ('%b-%d-%Y')

exclude_stocks = []
TAG = 'GAP'


def get_fo_stock_ids(co_bo_stocks_dir):
    co_bo_stocks = []
    co_bo_blocked_stocks = []

    with open (co_bo_stocks_dir + 'CM_Margin_'+ today_date_str + '.csv') as csv_file:
        csv_reader = csv.reader (csv_file, delimiter=',')
        for row in csv_reader:
            if len(row[0].strip ()) > 0:
                co_bo_stocks.append (row[0].strip ())

            if len(row[1].strip()) > 0:
                co_bo_blocked_stocks.append(row[1].strip ())

    fo_stock_ids = []

    for co_bo_stock in co_bo_stocks:
        if co_bo_stock not in fo_stock_ids and co_bo_stock not in exclude_stocks and co_bo_stock not in co_bo_blocked_stocks:
            fo_stock_ids.append(co_bo_stock)

    nse_fo_stock_ids = []
    for fo_stock_id in fo_stock_ids:
        nse_fo_stock_ids.append ('NSE:' + fo_stock_id)

    return nse_fo_stock_ids


def find_up_down_gap_orders(fo_stock_ids, kite, formed_orders, logger, testing):
    formed_order_ids = []
    for formed_order in formed_orders:
        if 'nse_symbol' in formed_order:
            formed_order_ids.append (formed_order['nse_symbol'])

    while True:
        stocks_live_data = kite.quote (fo_stock_ids)
        any_old_data = False
        for stock_live_data in stocks_live_data.values():
            if today_date == stock_live_data['last_trade_time'].date():
                any_old_data = False
                break
            else:
                any_old_data = True
                # logger.error('Old data:' + str(stock_live_data))

        if any_old_data and testing is False:
            continue
        else:
            break

    stocks_live_data = kite.quote (fo_stock_ids)
    stock_in_uptrend = None
    stock_in_downtrend = None
    orders = []

    for key, stock_live_data in stocks_live_data.items ():
        open_price = stock_live_data['ohlc']['open']
        prev_close = stock_live_data['ohlc']['close']

        if prev_close == 0 or open_price == 0:
            logger.error('0 prev. close/open price for stock:%s' %(key))
            continue

        if testing is False and today_date > stock_live_data['last_trade_time'].date():
            logger.error('Old data later:%s.' % (str(stock_live_data)))
            continue

        ascent = 0
        descent = 0

        if open_price > prev_close:
            ascent = abs ((open_price - prev_close) / prev_close)
        elif open_price < prev_close:
            descent = abs ((open_price - prev_close) / prev_close)

        lot = int (500000 / open_price)

        if stock_in_uptrend is None or ascent > stock_in_uptrend[1]:
            stock_in_uptrend = [key, ascent, lot]

        if stock_in_downtrend is None or descent > stock_in_downtrend[1]:
            stock_in_downtrend = [key, descent, lot]

    if stock_in_uptrend[1] != 0:
        if stock_in_uptrend[0] not in formed_order_ids:
            order = {
                'symbol': stock_in_uptrend[0].split(':')[1],
                'nse_symbol': stock_in_uptrend[0],
                'future_lot': stock_in_uptrend[2],
                'trigger_price_pts': round (util.min_tick_size * round ((200 / (stock_in_uptrend[2] / 2)) / util.min_tick_size), 2),
                'tag': TAG
            }
            orders.append (order)
            logger.info ('Max up stock:%s added in formed orders.', stock_in_uptrend[0])
        else:
            logger.info ('Max up stock:%s already in formed orders.', stock_in_uptrend[0])

    if stock_in_downtrend[1] != 0:
        if stock_in_downtrend[0] not in formed_order_ids:
            order = {
                'symbol': stock_in_downtrend[0].split (':')[1],
                'nse_symbol': stock_in_downtrend[0],
                'future_lot': stock_in_downtrend[2],
                'trigger_price_pts': round (util.min_tick_size * round ((200 / (stock_in_downtrend[2] / 2)) / util.min_tick_size), 2),
                'tag': TAG
            }
            orders.append (order)
            logger.info ('Max down stock:%s added in formed orders.', stock_in_downtrend[0])
        else:
            logger.info('Max down stock:%s already in formed orders.', stock_in_downtrend[0])

    return orders
