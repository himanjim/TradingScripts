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
        user_input = input("Enter option type (PE/CE) and optionally BUY/SELL (e.g. 'PE buy', 'CE', etc.): ").strip().lower()

        if not user_input:
            print("No input detected. Please enter a valid option type.")
            continue

        tokens = user_input.split()
        option_type = tokens[0].upper() if len(tokens) > 0 else None
        transaction_str = tokens[1].upper() if len(tokens) > 1 else "SELL"

        if option_type not in ["PE", "CE"]:
            print("Invalid option type. Please enter PE or CE.")
            continue

        if transaction_str not in ["BUY", "SELL"]:
            print("Invalid transaction type. Use 'buy' or 'sell'.")
            continue

        ul_live_quote = kite.quote(under_lying_symbol)
        ul_ltp = ul_live_quote[under_lying_symbol]['last_price']
        ul_ltp_round = round(ul_ltp / STRIKE_MULTIPLE) * STRIKE_MULTIPLE

        if transaction_str == "BUY":
            if option_type == "PE":
                ul_ltp_round = ul_ltp_round - (2* STRIKE_MULTIPLE)
            else:
                ul_ltp_round = ul_ltp_round + (2 * STRIKE_MULTIPLE)

        option_symbol = PART_SYMBOL + str(ul_ltp_round) + option_type
        transaction = kite.TRANSACTION_TYPE_BUY if transaction_str == "BUY" else kite.TRANSACTION_TYPE_SELL

        place_single_order(option_symbol, transaction, NO_OF_LOTS, OPTIONS_EXCHANGE)