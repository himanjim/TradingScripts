from datetime import datetime
import pytz
import OptionTradeUtils as oUtils

indian_timezone = pytz.timezone('Asia/Calcutta')
kite = oUtils.intialize_kite_api()


def place_market_order_with_absolute_stoploss(symbol, transaction_type, lots, exchange, stoploss_absolute, stoploss_points):
    try:
        # Check if symbol already exists in open positions
        positions = kite.positions()
        net_positions = positions['net']
        matching_positions = [p for p in net_positions if p['tradingsymbol'] == symbol and p['quantity'] != 0]

        if matching_positions:
            print(
                f"⛔ Trade skipped: Open position already exists for {symbol} with quantity {matching_positions[0]['quantity']}.")
            return

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

        print(f"✅ Market order placed ({transaction_type}) for {symbol} at {datetime.now(indian_timezone).time()}. Order ID: {order_id}")

        sl_transaction = kite.TRANSACTION_TYPE_SELL if transaction_type == kite.TRANSACTION_TYPE_BUY else kite.TRANSACTION_TYPE_BUY

        # If absolute stoploss provided, use it directly
        if stoploss_absolute is not None:
            stoploss_price = round(stoploss_absolute, 1)
        else:
            # Fetch order details to determine traded price
            orders = kite.orders()
            executed_order = next((order for order in orders if order['order_id'] == order_id and order['status'] == 'COMPLETE'), None)
            if not executed_order:
                print("⚠️ Market order not completed or not found. Cannot place stop-loss.")
                return
            traded_price = float(executed_order['average_price'])
            stoploss_price = round(traded_price - stoploss_points if transaction_type == kite.TRANSACTION_TYPE_BUY else traded_price + stoploss_points, 1)

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

        print(f"📉 Stoploss order placed at {stoploss_price} with Order ID: {sl_order_id}")

    except Exception as e:
        print(f"❌ Error occurred while placing order: {e}")


# --- Main Execution Loop ---
if __name__ == '__main__':
    UNDER_LYING_EXCHANGE, UNDERLYING, OPTIONS_EXCHANGE, PART_SYMBOL, NO_OF_LOTS, STRIKE_MULTIPLE, STOPLOSS_POINTS = oUtils.get_instruments(
        kite)
    lots = NO_OF_LOTS
    exchange = OPTIONS_EXCHANGE

    while True:
        user_input = input("Enter strike+option, transaction type (B/S), and optional absolute stoploss (e.g., 52500CE B or 52500CE S 230): ").strip().upper().split()

        if len(user_input) < 2 or user_input[1] not in ['B', 'S']:
            print("❌ Invalid input. Format: 52500CE B [STOPLOSS] or 52500CE S [STOPLOSS]")
            continue

        suffix_symbol = user_input[0]
        transaction = user_input[1]

        instrument_symbol = PART_SYMBOL.replace(':', '') + suffix_symbol
        stoploss_absolute = None

        if len(user_input) == 3:
            try:
                stoploss_absolute = float(user_input[2])
            except ValueError:
                print("❌ Stoploss must be a numeric value.")
                continue

        transaction_type = kite.TRANSACTION_TYPE_BUY if transaction == 'B' else kite.TRANSACTION_TYPE_SELL

        place_market_order_with_absolute_stoploss(
            symbol=instrument_symbol,
            transaction_type=transaction_type,
            lots=lots,
            exchange=exchange,
            stoploss_absolute=stoploss_absolute,
            stoploss_points=STOPLOSS_POINTS
        )
