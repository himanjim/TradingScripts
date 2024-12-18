import Utils as util
import time as tm
from datetime import datetime
import pytz

if __name__ == '__main__':

    # NIFTY24D1924700PE

    PART_SYMBOL = ':NIFTY24D19'
    NO_OF_LOTS = 250

    indian_timezone = pytz.timezone('Asia/Calcutta')

    kite = util.intialize_kite_api()

    under_lying_symbol = kite.EXCHANGE_NSE + ':NIFTY 50'

    while datetime.now(indian_timezone).time() < util.TRADE_START_TIME:
        pass

    if datetime.now(indian_timezone).time() > util.MARKET_END_TIME:
        print(f"Market is closed. Hence exiting.")
        exit(0)

    highest_options_premium_value = None
    while True:

        nifty_live_quote = kite.quote(under_lying_symbol)

        nifty_ltp = nifty_live_quote[under_lying_symbol]['last_price']

        nifty_ltp_round_50 = round(nifty_ltp / 50) * 50

        nifty_pe = kite.EXCHANGE_NFO + PART_SYMBOL + str(nifty_ltp_round_50) + 'PE'
        nifty_ce = kite.EXCHANGE_NFO + PART_SYMBOL + str(nifty_ltp_round_50) + 'CE'

        nifty_option_quotes = kite.quote([nifty_pe, nifty_ce])

        option_premium_value = 0
        for trading_symbol, live_quote in nifty_option_quotes.items():
            option_premium_value += (live_quote['last_price'] * NO_OF_LOTS)

        if highest_options_premium_value is None:
            highest_options_premium_value = option_premium_value
        elif option_premium_value > highest_options_premium_value:
            print(
                f"Premiuims has heated at : {option_premium_value} from: {highest_options_premium_value} at {datetime.now(indian_timezone).time()}.")
            highest_options_premium_value = option_premium_value

        elif (highest_options_premium_value - option_premium_value) > 1500:
            print(f"Premiuims has cooled at : {nifty_ltp_round_50}. CE: {nifty_option_quotes[nifty_ce]['last_price']}. PE: {nifty_option_quotes[nifty_pe]['last_price']} at {datetime.now(indian_timezone).time()}.")

        tm.sleep(2)

