import time as sleep_time
from datetime import datetime

import CalculatedCalendarSpreadsLimits as calcul_limit
import DerivativeUtils as outil
import ScrapUtils as sUtils
import Utils as util
from dateutil.relativedelta import relativedelta


def isliquid(bids):
    if bids is None or len(bids) == 0:
        return False
    for bid in bids:
        if bid['price'] is None or bid['price'] is 0 or bid['quantity'] is None or bid['quantity'] is 0:
            return False

    return True


kite = util.intialize_kite_api()

current_month_last_thurs = outil.get_last_thurday_of_month (datetime.now ().month, datetime.now ().year)

near_month_last_thurs = current_month_last_thurs + relativedelta (months=+1)

fo_stocks = sUtils.get_nse_fo_stocks()
lots = sUtils.get_nse_fo_lots()
nse_futures = []

for fo_stock in fo_stocks:
    stock_id = fo_stock[sUtils.STOCK_ID]

    nse_futures.append(util.NFO + outil.get_future_symbol(stock_id, current_month_last_thurs))
    nse_futures.append(util.NFO + outil.get_future_symbol(stock_id, near_month_last_thurs))

exclude_stocks = []
stocks_to_observe = {}
while True:
    future_ltps = kite.quote(nse_futures)
    for fo_stock in fo_stocks:
        stock_id = fo_stock[sUtils.STOCK_ID]
        if stock_id in exclude_stocks:
            continue
        future_current_month_nse_symbol = util.NFO + outil.get_future_symbol(stock_id, current_month_last_thurs)
        future_near_month_nse_symbol = util.NFO + outil.get_future_symbol(stock_id, near_month_last_thurs)

        if stock_id in calcul_limit.calculated_calendar_spread_limits:
            if future_near_month_nse_symbol in future_ltps and future_current_month_nse_symbol in future_ltps:
                if isliquid((future_ltps[future_near_month_nse_symbol]['depth']['buy'])) is False or isliquid((future_ltps[future_current_month_nse_symbol]['depth']['sell'])) is False:
                    print('Illiquid future for stock:%s' % stock_id, str(future_ltps[future_near_month_nse_symbol]['depth']['buy']), str(future_ltps[future_current_month_nse_symbol]['depth']['sell']))
                    continue
                if (future_ltps[future_near_month_nse_symbol]['last_price'] - future_ltps[future_current_month_nse_symbol]['last_price']) >  calcul_limit.calculated_calendar_spread_limits[stock_id][0]:
                    if stock_id not in stocks_to_observe:
                        stocks_to_observe[stock_id] = [future_ltps[future_current_month_nse_symbol]['last_price'], future_ltps[future_near_month_nse_symbol]['last_price'], datetime.now(), datetime.now(), None, None, future_ltps[future_current_month_nse_symbol]['depth']['sell'], future_ltps[future_near_month_nse_symbol]['depth']['buy']]
                    else:
                        stocks_to_observe[stock_id] = [future_ltps[future_current_month_nse_symbol]['last_price'], future_ltps[future_near_month_nse_symbol]['last_price'], stocks_to_observe[stock_id][2], datetime.now (), (stocks_to_observe[stock_id][1] - stocks_to_observe[stock_id][0] - future_ltps[future_near_month_nse_symbol]['last_price'] + future_ltps[future_current_month_nse_symbol]['last_price']) * lots[stock_id], stocks_to_observe[stock_id][4], stocks_to_observe[stock_id][5], future_ltps[future_current_month_nse_symbol]['depth']['sell'], future_ltps[future_near_month_nse_symbol]['depth']['buy']]
                        sleep_time.sleep(1)
                        print(stocks_to_observe[stock_id])

                if stock_id in stocks_to_observe and (future_ltps[future_near_month_nse_symbol]['last_price'] - future_ltps[future_current_month_nse_symbol]['last_price']) <  calcul_limit.calculated_calendar_spread_limits[stock_id][1]:
                    print('Stock:%s initiated at %s [%f, %f] exited at %s [%f, %f]. Profit:%f.' %stock_id, str(stocks_to_observe[stock_id][2]), stocks_to_observe[stock_id][0], stocks_to_observe[stock_id][1], str(datetime.now()), future_ltps[future_near_month_nse_symbol]['last_price'],  future_ltps[future_current_month_nse_symbol]['last_price'], (stocks_to_observe[stock_id][1] - stocks_to_observe[stock_id][0] - future_ltps[future_near_month_nse_symbol]['last_price'] + future_ltps[future_current_month_nse_symbol]['last_price']) * lots[stock_id])
            else:
                print('Future:%s or %s not in retrieved quotes.' % (future_near_month_nse_symbol, future_current_month_nse_symbol))
                if future_near_month_nse_symbol in nse_futures:
                    nse_futures.remove(future_near_month_nse_symbol)
                if future_current_month_nse_symbol in nse_futures:
                    nse_futures.remove(future_current_month_nse_symbol)
                    if stock_id not in exclude_stocks:
                        exclude_stocks.append(stock_id)
        else:
            print('Stock:%s not in cs limits' %(stock_id))




