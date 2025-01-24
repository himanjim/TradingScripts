import Utils as util
import time as tm
from datetime import datetime
import pytz

if __name__ == '__main__':

    indian_timezone = pytz.timezone('Asia/Calcutta')

    kite = util.intialize_kite_api()

    choice =2
    ###############################
    if choice == 1:
        # NIFTY24D1924700PE
        ###############################
        # UNDER_LYING_EXCHANGE = kite.EXCHANGE_BSE
        UNDER_LYING_EXCHANGE = kite.EXCHANGE_NSE
        UNDERLYING = ':NIFTY 50'
        OPTIONS_EXCHANGE = kite.EXCHANGE_NFO
        PART_SYMBOL = ':NIFTY25123'
        NO_OF_LOTS = 300
        STRIKE_MULTIPLE = 50
    elif choice == 2:
        UNDER_LYING_EXCHANGE = kite.EXCHANGE_BSE
        UNDERLYING = ':SENSEX'
        OPTIONS_EXCHANGE = kite.EXCHANGE_BFO
        PART_SYMBOL = ':SENSEX25114'
        PART_SYMBOL = ':SENSEX25JAN'
        NO_OF_LOTS = 100
        STRIKE_MULTIPLE = 100

    else:
        UNDER_LYING_EXCHANGE = kite.EXCHANGE_NSE
        UNDERLYING = ':NIFTY BANK'
        OPTIONS_EXCHANGE = kite.EXCHANGE_NFO
        PART_SYMBOL = ':BANKNIFTY24DEC'
        NO_OF_LOTS = 105
        STRIKE_MULTIPLE = 100

    ###############################

    # under_lying_symbol = kite.EXCHANGE_NSE + ':NIFTY 50'
    under_lying_symbol =UNDER_LYING_EXCHANGE + UNDERLYING

    while datetime.now(indian_timezone).time() < util.TRADE_START_TIME:
        pass

    if datetime.now(indian_timezone).time() > util.MARKET_END_TIME:
        print(f"Market is closed. Hence exiting.")
        exit(0)

    heated_options_premium_value = None
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

        if highest_options_premium_value is None or heated_options_premium_value > highest_options_premium_value:
            highest_options_premium_value = heated_options_premium_value

        if heated_options_premium_value is None:
            heated_options_premium_value = option_premium_value
        elif option_premium_value > heated_options_premium_value:
            print(
                f"Premiuims has heated at : {option_premium_value} from: {heated_options_premium_value} at {datetime.now(indian_timezone).time()}. Highest premiuim was: {highest_options_premium_value}")
            heated_options_premium_value = option_premium_value

        elif (heated_options_premium_value - option_premium_value) > 500:
            print(f"Premiuims has cooled at : {option_premium_value} from: {heated_options_premium_value} at UL: {ul_ltp_round}. CE: {option_quotes[nifty_ce]['last_price']}. PE: {option_quotes[nifty_pe]['last_price']} at {datetime.now(indian_timezone).time()}. Highest premiuim was: {highest_options_premium_value}")

        tm.sleep(2)

