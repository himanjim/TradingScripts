from datetime import datetime
import pytz
import OptionTradeUtils as oUtils

indian_timezone = pytz.timezone('Asia/Calcutta')
kite = oUtils.intialize_kite_api()


def place_market_order_with_stoploss(symbol, transaction_type, lots, exchange, stoploss_points):
    # Place market order
    order_id = kite.place_order(
        tradingsymbol=symbol,
        variety=kite.VARIETY_REGULAR,
        exchange=exchange,
        transaction_type=transaction_type,
        quantity=lots,
        order_type=kite.ORDER_TYPE_MARKET,
        product=kite.PRODUCT_NRML
    )

    print(f"Market order placed ({transaction_type}) for {symbol} at {datetime.now(indian_timezone).time()}. Order ID: {order_id}")

    # Fetch order details
    orders = kite.orders()
    executed_order = next((order for order in orders if order['order_id'] == order_id), None)

    if executed_order and executed_order['status'] == 'COMPLETE':
        traded_price = float(executed_order['average_price'])

        if transaction_type == kite.TRANSACTION_TYPE_BUY:
            stoploss_price = traded_price - stoploss_points
            sl_transaction = kite.TRANSACTION_TYPE_SELL
        else:
            stoploss_price = traded_price + stoploss_points
            sl_transaction = kite.TRANSACTION_TYPE_BUY

        stoploss_price = round(stoploss_price, 1)

        # Place stop-loss order
        sl_order_id = kite.place_order(
            tradingsymbol=symbol,
            variety=kite.VARIETY_REGULAR,
            exchange=exchange,
            transaction_type=sl_transaction,
            quantity=lots,
            order_type=kite.ORDER_TYPE_SL,
            product=kite.PRODUCT_NRML,
            trigger_price=stoploss_price,
            price=stoploss_price
        )

        print(f"Stoploss order placed at {stoploss_price} with Order ID: {sl_order_id}")

    else:
        print("Market order not completed or order not found. Cannot place stop-loss.")


# Example usage:
if __name__ == '__main__':
    transaction = input("Enter transaction type (BUY or SELL): ").strip().upper()
    if transaction not in ['BUY', 'SELL']:
        print("Invalid transaction type. Please enter BUY or SELL.")
        exit(1)

    instrument_symbol = 'SENSEX2560381200CE'  # Fixed symbol
    lots = 100  # Fixed lots
    stoploss_points = 30.0  # Fixed stoploss points
    exchange = 'BFO'

    transaction_type = kite.TRANSACTION_TYPE_BUY if transaction == 'BUY' else kite.TRANSACTION_TYPE_SELL

    place_market_order_with_stoploss(instrument_symbol, transaction_type, lots, exchange, stoploss_points)
