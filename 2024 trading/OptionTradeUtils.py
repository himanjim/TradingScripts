from kiteconnect import KiteConnect
import datetime as dt


KITE_API_KEY = '453dipfh64qcl484'
KITE_ACCESS_CODE = 'UgR02hYGZwuKXHu5eWEJ0VbsDAzCGHav'
MARKET_START_TIME = dt.time (9, 15, 0, 100)
MARKET_END_TIME = dt.time (15, 30, 0)
TRADE_START_TIME = dt.time (9, 15, 30)


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
