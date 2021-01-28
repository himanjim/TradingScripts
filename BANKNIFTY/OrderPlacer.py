import math
import time
import traceback
from datetime import datetime

import Utils as util
from pytz import timezone

YEAR_MONTH = datetime.now().strftime("%y") + datetime.now().strftime("%b").upper()
ORDER_TAG = 'BN_STRADDLE' + YEAR_MONTH
SPOT = 'NSE:NIFTY BANK'
MAX_CAPITAL_PER_OPTION = 50000
STRIKE_PREFIX = 'NFO:BANKNIFTY' + YEAR_MONTH
MAX_PREM_SUM_FOR_BUY = 1200
MAX_PREM_SUM_FOR_SELL = 1500
PREM_LIMIT_PTS = 20


def place_buy_sell_order(instrument, limit_price, transaction_type, lots):
    try:
        kite.place_order (tradingsymbol=instrument.replace('NFO:', ''),
                          variety=kite.VARIETY_REGULAR,
                          exchange=kite.EXCHANGE_NSE,
                          transaction_type=transaction_type,
                          quantity=lots,
                          order_type=kite.ORDER_TYPE_MARKET,
                          product=kite.PRODUCT_CNC,
                          price=limit_price,
                          trigger_price=None,
                          tag=ORDER_TAG)
    except Exception:
        print (traceback.format_exc () + ' in instrument:' + str (instrument))


def get_lots(prem_buy_price):
    return int(math.ceil((MAX_CAPITAL_PER_OPTION / prem_buy_price) / 20.0) * 20)


def get_atm_strikes(l_spot_price):
    atm_strike = int(math.ceil(l_spot_price / 100.0) * 100)

    return [STRIKE_PREFIX + str (atm_strike) + 'CE', STRIKE_PREFIX + str (atm_strike) + 'PE']


# Initialise
kite = util.intialize_kite_api ()

indian_timezone = timezone ('Asia/Calcutta')
today_date = datetime.now(indian_timezone).date()
testing = True
while datetime.now (indian_timezone).time () < util.MARKET_START_TIME and testing is False:
    pass


while True:
    try:
        last_spot_price = kite.quote (SPOT)[SPOT]['last_price']
        strikes_to_trade = get_atm_strikes (last_spot_price)
        strikes_to_trade.append(SPOT)

        quotes = kite.quote (strikes_to_trade)

        old_data = False
        for quote in quotes.values():
            if 'last_trade_time' in quote and quote['last_trade_time'].date () < today_date and testing is False:
                old_data = True
                print ('Prev day data:' + str (quote))
                break

            if (datetime.now ()  - quote['timestamp']).seconds > 10 and testing is False:
                old_data = True
                print ('Old data:' + str (quote))
                break

        if old_data:
            time.sleep (1)
            continue

        prem1 = quotes[strikes_to_trade[0]]['last_price']
        prem2 = quotes[strikes_to_trade[1]]['last_price']
        prem_sum = prem1 + prem2

        if prem_sum < MAX_PREM_SUM_FOR_BUY:
            place_buy_sell_order (strikes_to_trade[0], prem1 + PREM_LIMIT_PTS, kite.TRANSACTION_TYPE_BUY, get_lots(prem1))
            place_buy_sell_order (strikes_to_trade[1], prem2 + PREM_LIMIT_PTS, kite.TRANSACTION_TYPE_BUY, get_lots(prem2))
            print('Placed buy order:' + strikes_to_trade[0] + ' at:' + str(prem1))
            print('Placed buy order:' + strikes_to_trade[1] + ' at:' + str (prem2))
        elif prem_sum > MAX_PREM_SUM_FOR_SELL:
            place_buy_sell_order (strikes_to_trade[0], prem1 - PREM_LIMIT_PTS, kite.TRANSACTION_TYPE_SELL, get_lots(prem1))
            place_buy_sell_order (strikes_to_trade[1], prem2 - PREM_LIMIT_PTS, kite.TRANSACTION_TYPE_SELL, get_lots(prem2))
            print ('Placed sell order:' + strikes_to_trade[0] + ' at:' + str (prem1))
            print ('Placed sell order:' + strikes_to_trade[1] + ' at:' + str (prem2))

        exit(0)

    except Exception:
        print (traceback.format_exc ())
        # orders_placed = fetch_positions_set_orders_placed (kite)

