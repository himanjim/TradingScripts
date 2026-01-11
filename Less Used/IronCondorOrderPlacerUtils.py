import time as tm

LIMIT_PTS = 8
NO_OF_LOTS = 105


def check_trade_start_time_condition(kite, underlying_open_round):

    under_lying_symbol = kite.EXCHANGE_NSE + ':NIFTY BANK'

    while True:
        underlying_live_data = kite.quote(under_lying_symbol)
        underlying_live_ltp = underlying_live_data[under_lying_symbol]['last_price']

        if abs(underlying_live_ltp - underlying_open_round) < 50:
            return

        tm.sleep(1.0)


def order_placer(kite, symbol):
    nse_symbol = kite.EXCHANGE_NFO + ':' + symbol

    while True:
        try:
            stocks_live_data = kite.quote(nse_symbol)
            order_id = kite.place_order(tradingsymbol=symbol,
                                        variety=kite.VARIETY_REGULAR,
                                        exchange=kite.EXCHANGE_NFO,
                                        transaction_type=kite.TRANSACTION_TYPE_SELL,
                                        quantity=NO_OF_LOTS,
                                        order_type=kite.ORDER_TYPE_LIMIT,
                                        product=kite.PRODUCT_MIS,
                                        price=stocks_live_data[nse_symbol]['last_price'] - LIMIT_PTS,
                                        )

            return order_id

        except Exception as e:
            print(f"Order for {symbol} failed with error: {e}")
            tm.sleep(.1)


def modify_order(kite, order_id, symbol):
    order_modification_attempts = 0
    while True:
        if order_modification_attempts >= 8:
            break

        try:
            if order_id is not None:
                kite.modify_order(kite.VARIETY_REGULAR, order_id, order_type=kite.ORDER_TYPE_MARKET)
                break
            else:
                print('No order ID exists.')
                return
        except Exception as e:
            print(f"Order modification for {symbol} failed with error: {e}")
            tm.sleep(.5)
            order_modification_attempts += 1

    code = input('Press ENTER to modify orders as MARKET')

    if order_id is not None:
        kite.modify_order(kite.VARIETY_REGULAR, order_id, order_type=kite.ORDER_TYPE_MARKET)
