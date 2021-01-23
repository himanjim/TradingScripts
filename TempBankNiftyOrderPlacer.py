import Utils as util
from Orders import *
from pytz import timezone

kite = util.intialize_kite_api()
indian_timezone = timezone('Asia/Calcutta')
today_date = datetime.now(indian_timezone).date()
testing = False

symbols = ['BANKNIFTY20APR18800CE', 'BANKNIFTY20APR18800PE']
nse_symbols = [kite.EXCHANGE_NFO + ':' + symbols[0], kite.EXCHANGE_NFO + ':' + symbols[1]]

while datetime.now (indian_timezone).time () < util.MARKET_START_TIME and testing is False:
    pass

stocks_live_data = kite.quote (nse_symbols)
while testing is False and today_date > stocks_live_data[nse_symbols[0]]['last_trade_time'].date ():
    print ('Old data:' + str (stocks_live_data))
    stocks_live_data = kite.quote (nse_symbols)

trigger_pts = 75
limit_pts = trigger_pts + 10

for i in range(0, 1):
    kite.place_order (tradingsymbol=symbols[i],
                          variety=kite.VARIETY_REGULAR,
                          exchange=kite.EXCHANGE_NFO,
                          transaction_type=kite.TRANSACTION_TYPE_BUY,
                          quantity=20,
                          order_type=kite.ORDER_TYPE_SL,
                          product=kite.PRODUCT_NRML,
                          price=stocks_live_data[nse_symbols[i]]['last_price'] + limit_pts,
                          trigger_price=stocks_live_data[nse_symbols[i]]['last_price'] + trigger_pts,
                          tag=None)
