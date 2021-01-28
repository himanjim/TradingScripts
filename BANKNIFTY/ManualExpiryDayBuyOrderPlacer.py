import math
import traceback
from datetime import datetime
import Utils as util
from pytz import timezone

MAX_CAPITAL_PER_OPTION = 10000
PREM_LIMIT_PTS = 10
MAX_LOTS_TO_SELL = 40

def place_buy_sell_order(instrument, limit_price, transaction_type, lots):
    try:
        kite.place_order (tradingsymbol=instrument.replace('NFO:', ''),
                          variety=kite.VARIETY_REGULAR,
                          exchange=kite.EXCHANGE_NFO,
                          transaction_type=transaction_type,
                          quantity=lots,
                          order_type=kite.ORDER_TYPE_LIMIT,
                          product=kite.PRODUCT_NRML,
                          price=limit_price,
                          trigger_price=None,
                          tag=None)
    except Exception:
        print (traceback.format_exc () + ' in instrument:' + str (instrument))


def get_lots(prem_buy_price):
    return int(math.floor((MAX_CAPITAL_PER_OPTION / prem_buy_price) / 20.0) * 20)


# Initialise
kite = util.intialize_kite_api ()

###############Configure every time you trade
spot = 21100
suffix = 'NFO:BANKNIFTY20611'
###############Configure every time you trade
atm_strike = int (round (spot / 100.0) * 100)

strikes_to_trade = [suffix + str(atm_strike + 200) + 'CE', suffix + str(atm_strike - 200) + 'PE']
indian_timezone = timezone ('Asia/Calcutta')
today_date = datetime.now(indian_timezone).date()
testing = False
while datetime.now (indian_timezone).time () < util.MARKET_START_TIME and testing is False:
    pass


while True:
    try:
        val = input ('(BBB)Enter 0 to buy:' + strikes_to_trade[0] + '\n(BBB)Enter 1 to buy:' + strikes_to_trade[1] + '\n(SSS)Enter 2 to sell:' + strikes_to_trade[0] + '\n(SSS)Enter 3 to sell:' + strikes_to_trade[1] + '\n')
        choice = int(val)
        strike_index = [choice - 2, choice][choice < 2]
        quote = kite.quote (strikes_to_trade[strike_index])

        prem = quote[strikes_to_trade[strike_index]]['last_price']

        transaction_t = [kite.TRANSACTION_TYPE_SELL, kite.TRANSACTION_TYPE_BUY][choice < 2]

        lots = [MAX_LOTS_TO_SELL, get_lots (prem)][transaction_t == kite.TRANSACTION_TYPE_BUY]

        net_prem = prem + [-PREM_LIMIT_PTS, PREM_LIMIT_PTS][choice < 2]

        place_buy_sell_order (strikes_to_trade[strike_index], net_prem, transaction_t, lots)
        print ('Placed ' + transaction_t + ' order:' + strikes_to_trade[strike_index] + ' at:' + str (net_prem) + '(' + str(prem) + ') and lots:' + str (lots))

    except Exception:
        print (traceback.format_exc ())

