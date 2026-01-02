from kiteconnect import KiteConnect
import datetime as dt


KITE_API_KEY = '453dipfh64qcl484'
KITE_API_SECRET = 'cnt30fp12ftbzk7s0a84ieqv8wbquer4'


KITE_ACCESS_CODE = 'kb4q3uBgBiXu7eNQZsXaDRIVXscEHCiR'
MARKET_START_TIME = dt.time (9, 15, 0, 100)
MARKET_END_TIME = dt.time (15, 30, 0)
TRADE_START_TIME = dt.time (9, 15, 30)


def get_instruments(kite_):
    choice = 1

    if choice == 1:
        UNDER_LYING_EXCHANGE = kite_.EXCHANGE_NSE
        UNDERLYING = ':NIFTY 50'
        OPTIONS_EXCHANGE = kite_.EXCHANGE_NFO
        # PART_SYMBOL = ':NIFTY25508'
        # PART_SYMBOL = ':NIFTY25O20'
        # PART_SYMBOL = ':NIFTY25D16'
        PART_SYMBOL = ':NIFTY26106'
        NO_OF_LOTS = 325
        STRIKE_MULTIPLE = 50
        STOPLOSS_POINTS = 10
        MINIMUM_LOTS = 65
    elif choice == 2:
        UNDER_LYING_EXCHANGE = kite_.EXCHANGE_BSE
        UNDERLYING = ':SENSEX'
        OPTIONS_EXCHANGE = kite_.EXCHANGE_BFO
        # PART_SYMBOL = ':SENSEX25MAY'
        # PART_SYMBOL = ':SENSEX25506' # 6th May 2025
        # PART_SYMBOL = ':SENSEX25819'
        # PART_SYMBOL = ':SENSEX25N06'
        PART_SYMBOL = ':SENSEX26101'
        NO_OF_LOTS = 100
        STRIKE_MULTIPLE = 100
        STOPLOSS_POINTS = 30
        MINIMUM_LOTS = 20
    else:
        UNDER_LYING_EXCHANGE = kite_.EXCHANGE_NSE
        UNDERLYING = ':NIFTY BANK'
        OPTIONS_EXCHANGE = kite_.EXCHANGE_NFO
        # PART_SYMBOL = ':BANKNIFTY25APR'
        PART_SYMBOL = ':BANKNIFTY25JUL'
        NO_OF_LOTS = 105
        STRIKE_MULTIPLE = 100
        STOPLOSS_POINTS = 30
        MINIMUM_LOTS = 30

    return UNDER_LYING_EXCHANGE ,UNDERLYING, OPTIONS_EXCHANGE, PART_SYMBOL, NO_OF_LOTS, STRIKE_MULTIPLE, STOPLOSS_POINTS, MINIMUM_LOTS



def intialize_kite_api():
    kite = KiteConnect (api_key=KITE_API_KEY)

    try:

        kite.set_access_token(KITE_ACCESS_CODE)
    except Exception as e:
        print("Authentication failed", str(e))
        raise

    return kite


def get_underlying_value(_kite, _position):
    if 'NIFTY' in _position['tradingsymbol']:
        under_lying_symbol = _kite.EXCHANGE_NSE + ':NIFTY 50'
        strike_multiple = 50
    elif 'SENSEX' in _position['tradingsymbol']:
        under_lying_symbol = _kite.EXCHANGE_BSE + ':SENSEX'
        strike_multiple = 100
    else:
        under_lying_symbol = _kite.EXCHANGE_NSE + ':NIFTY BANK'
        strike_multiple = 100

    ul_live_quote = _kite.quote(under_lying_symbol)

    ul_ltp = ul_live_quote[under_lying_symbol]['last_price']

    # nifty_ltp_round_50 = round(nifty_ltp / 50) * 50
    ul_ltp_round = round(ul_ltp / strike_multiple) * strike_multiple

    return ul_ltp_round


def cancel_all_open_orders(kite):
    orders = kite.orders()
    open_orders = [order for order in orders if order['status'] == 'TRIGGER PENDING' or order['status'] == 'OPEN']

    for order in open_orders:
        try:
            kite.cancel_order(
                variety=order['variety'],
                order_id=order['order_id']
            )
            print(f"Cancelled order ID: {order['order_id']} for {order['tradingsymbol']}")
        except Exception as e:
            print(f"Failed to cancel order ID: {order['order_id']} - {str(e)}")
