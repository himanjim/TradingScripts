from kiteconnect import KiteConnect
import datetime as dt


KITE_API_KEY = '453dipfh64qcl484'
KITE_API_SECRET = 'cnt30fp12ftbzk7s0a84ieqv8wbquer4'


KITE_ACCESS_CODE = 'h3WoOB4vjKHDvUdc7xVwou2uOgvVFhfy'


def get_instruments(kite_):
    choice = 1

    if choice == 1:
        UNDER_LYING_EXCHANGE = kite_.EXCHANGE_NSE
        UNDERLYING = ':NIFTY 50'
        OPTIONS_EXCHANGE = kite_.EXCHANGE_NFO
        # PART_SYMBOL = ':NIFTY25508'
        # PART_SYMBOL = ':NIFTY25O20'
        # PART_SYMBOL = ':NIFTY25D16'
        # PART_SYMBOL = ':NIFTY26217'
        # PART_SYMBOL = ':NIFTY26FEB'
        # PART_SYMBOL = ':NIFTY26519'
        # PART_SYMBOL = ':NIFTY26MAY'
        PART_SYMBOL = ':NIFTY26623'
        NO_OF_LOTS = 325
        STRIKE_MULTIPLE = 50
        STOPLOSS_POINTS = 10
        MINIMUM_LOTS = 65
        LONG_STRADDLE_STRIKE_DISTANCE= 1000
    elif choice == 2:
        UNDER_LYING_EXCHANGE = kite_.EXCHANGE_BSE
        UNDERLYING = ':SENSEX'
        OPTIONS_EXCHANGE = kite_.EXCHANGE_BFO
        # PART_SYMBOL = ':SENSEX25MAY'
        # PART_SYMBOL = ':SENSEX25506' # 6th May 2025
        # PART_SYMBOL = ':SENSEX25819'
        # PART_SYMBOL = ':SENSEX26219'
        # PART_SYMBOL = ':SENSEX26APR'
        # PART_SYMBOL = ':SENSEX26514'
        # PART_SYMBOL = ':SENSEX26MAY'
        PART_SYMBOL = ':SENSEX26618'
        NO_OF_LOTS = 100
        STRIKE_MULTIPLE = 100
        STOPLOSS_POINTS = 30
        MINIMUM_LOTS = 20
        LONG_STRADDLE_STRIKE_DISTANCE = 3000

    return UNDER_LYING_EXCHANGE ,UNDERLYING, OPTIONS_EXCHANGE, PART_SYMBOL, NO_OF_LOTS, STRIKE_MULTIPLE, STOPLOSS_POINTS, MINIMUM_LOTS, LONG_STRADDLE_STRIKE_DISTANCE



def intialize_kite_api():
    kite = KiteConnect (api_key=KITE_API_KEY)

    try:

        kite.set_access_token(KITE_ACCESS_CODE)
    except Exception as e:
        print("Authentication failed", str(e))
        raise

    return kite

