from datetime import datetime
import pytz
import OptionTradeUtils as oUtils

def place_single_order(symbol, transaction, lots, exchange):
    kite.place_order(
        tradingsymbol=symbol,
        variety=kite.VARIETY_REGULAR,
        exchange=exchange,
        transaction_type=transaction,
        quantity=lots,
        order_type=kite.ORDER_TYPE_MARKET,
        product=kite.PRODUCT_NRML,
    )
    print(f"Placed {transaction} order for: {symbol} at {datetime.now(indian_timezone).time()}.")


if __name__ == '__main__':

    indian_timezone = pytz.timezone('Asia/Calcutta')
    kite = oUtils.intialize_kite_api()

    UNDER_LYING_EXCHANGE, UNDERLYING, OPTIONS_EXCHANGE, PART_SYMBOL, NO_OF_LOTS, STRIKE_MULTIPLE = oUtils.get_instruments(kite)
    PART_SYMBOL = PART_SYMBOL.replace(':', '')

    while True:
        under_lying_symbol = UNDER_LYING_EXCHANGE + UNDERLYING
        user_input = input("Enter 'PE' or 'CE' to place SHORT order (case-insensitive): ").strip().upper()

        if user_input not in ['PE', 'CE']:
            print("Invalid input. Please enter only 'PE' or 'CE'.")
            continue

        ul_live_quote = kite.quote(under_lying_symbol)
        ul_ltp = ul_live_quote[under_lying_symbol]['last_price']
        ul_ltp_round = round(ul_ltp / STRIKE_MULTIPLE) * STRIKE_MULTIPLE

        option_symbol = PART_SYMBOL + str(ul_ltp_round) + user_input
        place_single_order(option_symbol, kite.TRANSACTION_TYPE_SELL, NO_OF_LOTS, OPTIONS_EXCHANGE)
