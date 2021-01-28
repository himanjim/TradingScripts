import traceback
from datetime import datetime
import math
import Utils as util
from pytz import timezone

PREM_LIMIT_PTS = 20
MAX_LOTS_TO_SELL = 40
NFO = 'NFO:'
MAX_CAPITAL_BUYING_OPTION = 10000


def get_lots(prem_buy_price):
    return int(math.floor((MAX_CAPITAL_BUYING_OPTION / prem_buy_price) / 20.0) * 20)


def place_order(instrument, limit_price, transaction_type, lots):
    try:
        kite.place_order (tradingsymbol=instrument.replace(NFO, ''),
                          variety=kite.VARIETY_REGULAR,
                          exchange=kite.EXCHANGE_NFO,
                          transaction_type=transaction_type,
                          quantity=lots,
                          order_type=kite.ORDER_TYPE_LIMIT,
                          product=kite.PRODUCT_NRML,
                          price=limit_price,
                          trigger_price=None)
    except Exception:
        print (traceback.format_exc () + ' in instrument:' + str (instrument))


def get_max_prem(quotes):
    max_prem = 0
    max_prem_symbol = None

    for symbol, quote in quotes.items():
        if quote['last_price'] > max_prem:
            max_prem = quote['last_price']
            max_prem_symbol = symbol

    return max_prem, max_prem_symbol


# Initialise
kite = util.intialize_kite_api ()

###############Configure every time you trade
spot = 22100
suffix = 'NFO:BANKNIFTY20702'
# suffix = NFO + 'BANKNIFTY20611'
###############Configure every time you trade

atm_strike = int (round (spot / 100.0) * 100)

ce_strikes_to_trade = [suffix + str(atm_strike) + 'CE', suffix + str(atm_strike + 100) + 'CE', suffix + str(atm_strike + 200) + 'CE']
pe_strikes_to_trade = [suffix + str(atm_strike - 200) + 'PE', suffix + str(atm_strike - 100) + 'PE', suffix + str(atm_strike) + 'PE']

indian_timezone = timezone ('Asia/Calcutta')
today_date = datetime.now(indian_timezone).date()
testing = False
while datetime.now (indian_timezone).time () < util.MARKET_START_TIME and testing is False:
    pass


while True:
    quotes = kite.quote (ce_strikes_to_trade + pe_strikes_to_trade)

    old_data = False
    for quote in quotes.values():
        if 'last_trade_time' in quote and quote['last_trade_time'].date () < today_date and testing is False:
            old_data = True
            print ('Prev day data:' + str (quote))
            break

    if old_data:
        continue
    else:
        break

max_prem, max_prem_symbol = get_max_prem(quotes)

place_order (max_prem_symbol, max_prem - PREM_LIMIT_PTS, kite.TRANSACTION_TYPE_SELL, MAX_LOTS_TO_SELL)
print ('Placed sell order:' + max_prem_symbol + ' at:' + str (max_prem))

if max_prem_symbol in ce_strikes_to_trade:
    next_quotes = kite.quote (pe_strikes_to_trade)
else:
    next_quotes = kite.quote(ce_strikes_to_trade)

max_prem, max_prem_symbol = get_max_prem(next_quotes)
place_order(max_prem_symbol, max_prem - PREM_LIMIT_PTS, kite.TRANSACTION_TYPE_SELL, MAX_LOTS_TO_SELL)
print('Placed sell order:' + max_prem_symbol + ' at:' + str(max_prem))

otm2_ce_symbol = ce_strikes_to_trade[2]
otm2_ce_open = quotes[otm2_ce_symbol]['ohlc']['open']
otm2_buy_bet = util.round_to_tick(otm2_ce_open * .6)
otm2_buy_bet = [100, otm2_buy_bet][otm2_buy_bet < 100]
lots = get_lots(otm2_buy_bet)

place_order (otm2_ce_symbol, otm2_buy_bet, kite.TRANSACTION_TYPE_BUY, lots)
print ('Placed buy order:' + ce_strikes_to_trade[2] + ' at:' + str (otm2_buy_bet) + ' lots:' + str(lots))

exit(0)


