from kiteconnect import KiteConnect
import datetime as dt


KITE_API_KEY = '453dipfh64qcl484'
KITE_API_SECRET = 'cnt30fp12ftbzk7s0a84ieqv8wbquer4'


KITE_ACCESS_CODE = 'zaMkKK5EGnM3YBVdWdcHSOC7Gpavmz9e'
MARKET_START_TIME = dt.time (9, 15, 0, 100)
MARKET_END_TIME = dt.time (15, 30, 0)
TRADE_START_TIME = dt.time (9, 15, 30)


def get_instruments(kite_):
    choice = 3

    if choice == 1:
        UNDER_LYING_EXCHANGE = kite_.EXCHANGE_NSE
        UNDERLYING = ':NIFTY 50'
        OPTIONS_EXCHANGE = kite_.EXCHANGE_NFO
        # PART_SYMBOL = ':NIFTY25508'
        PART_SYMBOL = ':NIFTY25522'
        NO_OF_LOTS = 300
        STRIKE_MULTIPLE = 50
    elif choice == 2:
        UNDER_LYING_EXCHANGE = kite_.EXCHANGE_BSE
        UNDERLYING = ':SENSEX'
        OPTIONS_EXCHANGE = kite_.EXCHANGE_BFO
        # PART_SYMBOL = ':SENSEX25MAY'
        PART_SYMBOL = ':SENSEX25506' # 6th May 2025
        PART_SYMBOL = ':SENSEX25MAY'
        NO_OF_LOTS = 100
        STRIKE_MULTIPLE = 100
    else:
        UNDER_LYING_EXCHANGE = kite_.EXCHANGE_NSE
        UNDERLYING = ':NIFTY BANK'
        OPTIONS_EXCHANGE = kite_.EXCHANGE_NFO
        # PART_SYMBOL = ':BANKNIFTY25APR'
        PART_SYMBOL = ':BANKNIFTY25MAY'
        NO_OF_LOTS = 120
        STRIKE_MULTIPLE = 100

    return UNDER_LYING_EXCHANGE ,UNDERLYING, OPTIONS_EXCHANGE, PART_SYMBOL, NO_OF_LOTS, STRIKE_MULTIPLE



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
