from datetime import datetime
import pytz
import OptionTradeUtils as oUtils
import time

indian_timezone = pytz.timezone('Asia/Calcutta')
kite = oUtils.intialize_kite_api()

def place_order(symbol, transaction_type, lots, exchange, stoploss_absolute, stoploss_points, is_limit=False, limit_price=None):
    try:
        # Check if symbol already exists in open positions
        positions = kite.positions()
        net_positions = positions['net']
        matching_positions = [p for p in net_positions if p['tradingsymbol'] == symbol and p['quantity'] != 0]

        if matching_positions:
            print(f"⛔ Trade skipped: Open position already exists for {symbol} with quantity {matching_positions[0]['quantity']}.")
            return

        order_type = kite.ORDER_TYPE_LIMIT if is_limit else kite.ORDER_TYPE_MARKET
        order_args = {
            "tradingsymbol": symbol,
            "variety": kite.VARIETY_REGULAR,
            "exchange": exchange,
            "transaction_type": transaction_type,
            "quantity": lots,
            "order_type": order_type,
            "product": kite.PRODUCT_NRML
        }

        if is_limit:
            order_args["price"] = limit_price

        # Place primary order
        order_id = kite.place_order(**order_args)
        print(f"✅ {'Limit' if is_limit else 'Market'} order placed ({transaction_type}) for {symbol} at {datetime.now(indian_timezone).time()}. Order ID: {order_id}")

        # Skip stoploss for limit orders
        if is_limit:
            return

        # Market order only: place stoploss
        sl_transaction = kite.TRANSACTION_TYPE_SELL if transaction_type == kite.TRANSACTION_TYPE_BUY else kite.TRANSACTION_TYPE_BUY

        if stoploss_absolute is not None:
            stoploss_price = round(stoploss_absolute, 1)
        else:
            executed_order = None
            max_retries = 3
            for attempt in range(1, max_retries + 1):
                orders = kite.orders()
                executed_order = next(
                    (order for order in orders if order['order_id'] == order_id and order['status'] == 'COMPLETE'),
                    None)

                if executed_order:
                    break
                else:
                    print(f"🔄 Attempt {attempt}: Executed order not yet found. Retrying in 1 second...")
                    time.sleep(1)

            if not executed_order:
                print("❌ Market order not completed or not found after 3 attempts. Cannot place stop-loss.")
                return

            traded_price = float(executed_order['average_price'])
            stoploss_price = round(traded_price - stoploss_points if transaction_type == kite.TRANSACTION_TYPE_BUY else traded_price + stoploss_points, 1)

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
    UNDER_LYING_EXCHANGE, UNDERLYING, OPTIONS_EXCHANGE, PART_SYMBOL, NO_OF_LOTS, STRIKE_MULTIPLE, STOPLOSS_POINTS = oUtils.get_instruments(kite)
    lots = NO_OF_LOTS
    exchange = OPTIONS_EXCHANGE

    while True:
        user_input = input("Enter (e.g., 52500CE B, 52500CE S 230, or 52500CE B L 450): ").strip().upper().split()

        if len(user_input) < 2:
            print("❌ Invalid input. Use: 52500CE B [STOPLOSS] or 52500CE B L [PRICE]")
            continue

        suffix_symbol = user_input[0]
        transaction = user_input[1]

        if transaction not in ['B', 'S']:
            print("❌ Transaction must be 'B' or 'S'")
            continue

        instrument_symbol = PART_SYMBOL.replace(':', '') + suffix_symbol
        transaction_type = kite.TRANSACTION_TYPE_BUY if transaction == 'B' else kite.TRANSACTION_TYPE_SELL

        is_limit = False
        limit_price = None
        stoploss_absolute = None

        if len(user_input) == 3:
            if user_input[2] == 'L':
                print("❌ Missing price for limit order.")
                continue
            try:
                stoploss_absolute = float(user_input[2])
            except ValueError:
                print("❌ Invalid stoploss price.")
                continue

        elif len(user_input) == 4:
            if user_input[2] != 'L':
                print("❌ Use 'L' for limit orders. Example: 52500CE B L 450")
                continue
            try:
                limit_price = float(user_input[3])
                is_limit = True
            except ValueError:
                print("❌ Invalid limit price.")
                continue

        place_order(
            symbol=instrument_symbol,
            transaction_type=transaction_type,
            lots=lots,
            exchange=exchange,
            stoploss_absolute=stoploss_absolute,
            stoploss_points=STOPLOSS_POINTS,
            is_limit=is_limit,
            limit_price=limit_price
        )
