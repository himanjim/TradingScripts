import Utils as util
import time as tm
from datetime import datetime
import pytz


def place_order(_pe, _ce, _transaction, _lots, _exchange):
    kite.place_order(tradingsymbol=_pe,
                     variety=kite.VARIETY_REGULAR,
                     exchange=_exchange,
                     transaction_type=_transaction,
                     quantity=_lots,
                     order_type=kite.ORDER_TYPE_MARKET,
                     product=kite.PRODUCT_MIS,
                     )

    kite.place_order(tradingsymbol=_ce,
                     variety=kite.VARIETY_REGULAR,
                     exchange=_exchange,
                     transaction_type=_transaction,
                     quantity=_lots,
                     order_type=kite.ORDER_TYPE_MARKET,
                     product=kite.PRODUCT_MIS,
                     )
    print(f"Placed {_transaction} order for : {_pe} and {_ce} at {datetime.now(indian_timezone).time()}.")


if __name__ == '__main__':

    indian_timezone = pytz.timezone('Asia/Calcutta')

    kite = util.intialize_kite_api()

    choice = 3

    premium_difference_for_action = 5000
    ###############################
    if choice == 1:
        # NIFTY24D1924700PE
        ###############################
        # UNDER_LYING_EXCHANGE = kite.EXCHANGE_BSE
        UNDER_LYING_EXCHANGE = kite.EXCHANGE_NSE
        UNDERLYING = ':NIFTY 50'
        OPTIONS_EXCHANGE = kite.EXCHANGE_NFO
        # PART_SYMBOL = ':NIFTY25123'
        PART_SYMBOL = ':NIFTY25220'
        NO_OF_LOTS = 300
        STRIKE_MULTIPLE = 50
    elif choice == 2:
        UNDER_LYING_EXCHANGE = kite.EXCHANGE_BSE
        UNDERLYING = ':SENSEX'
        OPTIONS_EXCHANGE = kite.EXCHANGE_BFO
        PART_SYMBOL = ':SENSEX25225'
        # PART_SYMBOL = ':SENSEX25JAN'
        NO_OF_LOTS = 100
        STRIKE_MULTIPLE = 100

    else:
        UNDER_LYING_EXCHANGE = kite.EXCHANGE_NSE
        UNDERLYING = ':NIFTY BANK'
        OPTIONS_EXCHANGE = kite.EXCHANGE_NFO
        PART_SYMBOL = ':BANKNIFTY25APR'
        NO_OF_LOTS = 120
        STRIKE_MULTIPLE = 100

    ###############################

    # under_lying_symbol = kite.EXCHANGE_NSE + ':NIFTY 50'
    under_lying_symbol = UNDER_LYING_EXCHANGE + UNDERLYING

    while datetime.now(indian_timezone).time() < util.MARKET_START_TIME:
        pass

    original_options_premium_value = None
    highest_options_premium_value = None
    while True:

        if datetime.now(indian_timezone).time() > util.MARKET_END_TIME:
            print(f"Market is closed. Hence exiting.")
            exit(0)

        ul_live_quote = kite.quote(under_lying_symbol)

        ul_ltp = ul_live_quote[under_lying_symbol]['last_price']

        # nifty_ltp_round_50 = round(nifty_ltp / 50) * 50
        ul_ltp_round = round(ul_ltp / STRIKE_MULTIPLE) * STRIKE_MULTIPLE

        option_pe = OPTIONS_EXCHANGE + PART_SYMBOL + str(ul_ltp_round) + 'PE'
        option_ce = OPTIONS_EXCHANGE + PART_SYMBOL + str(ul_ltp_round) + 'CE'

        option_quotes = kite.quote([option_pe, option_ce])

        option_premium_value = 0
        for trading_symbol, live_quote in option_quotes.items():
            option_premium_value += (live_quote['last_price'] * NO_OF_LOTS)

        if highest_options_premium_value is None or option_premium_value > highest_options_premium_value:
            highest_options_premium_value = option_premium_value

        if original_options_premium_value is None:
            original_options_premium_value = option_premium_value

        print(
            f"Strike:{ul_ltp_round}. Current PREM is: {option_premium_value}(CE:{option_quotes[option_ce]['last_price']} PE:{option_quotes[option_pe]['last_price']}),  original : {original_options_premium_value} and highest : {highest_options_premium_value} at {datetime.now(indian_timezone).time()}.")

        tm.sleep(2)
        continue

        if (option_premium_value - original_options_premium_value) > premium_difference_for_action:
            print(f"*******Difference: {option_premium_value - original_options_premium_value}. Current premium value is: {option_premium_value},  original premium value is : {original_options_premium_value} and highest premium value is: {highest_options_premium_value} at {datetime.now(indian_timezone).time()}. Highest premiuim was: {highest_options_premium_value}")
            place_order(option_pe, option_ce, kite.TRANSACTION_TYPE_SELL, NO_OF_LOTS, OPTIONS_EXCHANGE)
            exit(0)

        elif (original_options_premium_value  - option_premium_value) > premium_difference_for_action:
            print(f"^^^^^^Difference: {option_premium_value - original_options_premium_value}. Current premium value is : {option_premium_value},  original premium value is : {original_options_premium_value} and highest premium value is: {highest_options_premium_value} at {datetime.now(indian_timezone).time()}. Highest premiuim was: {highest_options_premium_value}")
            place_order(option_pe, option_ce, kite.TRANSACTION_TYPE_BUY, NO_OF_LOTS, OPTIONS_EXCHANGE)
            exit(0)
        else:
            print(f"Difference: {option_premium_value - original_options_premium_value}. Current premium value is : {option_premium_value},  original premium value is : {original_options_premium_value} and highest premium value is: {highest_options_premium_value} at {datetime.now(indian_timezone).time()}. Highest premiuim was: {highest_options_premium_value}")


