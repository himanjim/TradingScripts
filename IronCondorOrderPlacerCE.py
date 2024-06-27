import time as tm
from datetime import datetime
import IronCondorOrderPlacerUtils as ironCondorOrderPlacerUtils

import pytz

import Utils as util

if __name__ == '__main__':

    kite = util.intialize_kite_api()
    indian_timezone = pytz.timezone('Asia/Calcutta')

    testing = False

    underlying_open = 51900
    underlying_open_round = round(underlying_open / 100) * 100

    symbol = 'BANKNIFTY24JUN' + str(underlying_open_round) + 'CE'

    while datetime.now(indian_timezone).time() < util.TRADE_START_TIME and testing is False:
        pass

    ironCondorOrderPlacerUtils.check_trade_start_time_condition(kite, underlying_open_round)

    start_time = tm.time()

    order_id = ironCondorOrderPlacerUtils.order_placer(kite, symbol)

    end_time = tm.time()

    print('Orders executed in time(secs):', (end_time - start_time), ' at time: ', datetime.now(indian_timezone).time())

    tm.sleep(1.0)

    ironCondorOrderPlacerUtils.modify_order(kite, order_id, symbol)