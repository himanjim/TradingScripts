import Utils as util
import time as tm
from datetime import datetime
import pytz

if __name__ == '__main__':

    indian_timezone = pytz.timezone('Asia/Calcutta')

    kite = util.intialize_kite_api()

    # NIFTY24D1924700PE
    ###############################
    # UNDER_LYING_EXCHANGE = kite.EXCHANGE_BSE
    UNDER_LYING_EXCHANGE = kite.EXCHANGE_NSE

    # UNDERLYING = ':NIFTY BANK'
    # UNDERLYING = ':SENSEX'
    UNDERLYING = ':NIFTY 50'
    # OPTIONS_EXCHANGE = kite.EXCHANGE_BFO
    OPTIONS_EXCHANGE = kite.EXCHANGE_NFO
    # PART_SYMBOL = ':BANKNIFTY24DEC'
    # PART_SYMBOL = ':SENSEX24D20'
    PART_SYMBOL = ':NIFTY25102'
    # PART_SYMBOL = ':SENSEX24DEC'

    # NO_OF_LOTS = 105
    NO_OF_LOTS = 300
    # NO_OF_LOTS = 80
    # STRIKE_MULTIPLE = 100
    STRIKE_MULTIPLE = 50
    ###############################

    # under_lying_symbol = kite.EXCHANGE_NSE + ':NIFTY 50'
    under_lying_symbol =UNDER_LYING_EXCHANGE + UNDERLYING

    while datetime.now(indian_timezone).time() < util.TRADE_START_TIME:
        pass

    if datetime.now(indian_timezone).time() > util.MARKET_END_TIME:
        print(f"Market is closed. Hence exiting.")
        exit(0)

    highest_options_premium_value = None
    while True:

        ul_live_quote = kite.quote(under_lying_symbol)

        ul_ltp = ul_live_quote[under_lying_symbol]['last_price']


        # nifty_ltp_round_50 = round(nifty_ltp / 50) * 50
        ul_ltp_round = round(ul_ltp / STRIKE_MULTIPLE) * STRIKE_MULTIPLE

        nifty_pe = OPTIONS_EXCHANGE + PART_SYMBOL + str(ul_ltp_round) + 'PE'
        nifty_ce = OPTIONS_EXCHANGE + PART_SYMBOL + str(ul_ltp_round) + 'CE'

        option_quotes = kite.quote([nifty_pe, nifty_ce])

        option_premium_value = 0
        for trading_symbol, live_quote in option_quotes.items():
            option_premium_value += (live_quote['last_price'] * NO_OF_LOTS)

        if highest_options_premium_value is None:
            highest_options_premium_value = option_premium_value
        elif option_premium_value > highest_options_premium_value:
            print(
                f"Premiuims has heated at : {option_premium_value} from: {highest_options_premium_value} at {datetime.now(indian_timezone).time()}.")
            highest_options_premium_value = option_premium_value

        elif (highest_options_premium_value - option_premium_value) > 500:
            print(f"Premiuims has cooled at : {ul_ltp_round}. CE: {option_quotes[nifty_ce]['last_price']}. PE: {option_quotes[nifty_pe]['last_price']} at {datetime.now(indian_timezone).time()}.")

        tm.sleep(2)

