from datetime import datetime
import pytz
import OptionTradeUtils as oUtils
import time

indian_timezone = pytz.timezone('Asia/Calcutta')
kite = oUtils.intialize_kite_api()

def cancel_all_open_orders():
    try:
        orders = kite.orders()
        cancellable_status = {
            "OPEN", "TRIGGER PENDING", "AMO REQ RECEIVED",
            "MODIFY PENDING", "CANCEL PENDING", "VALIDATION PENDING"
        }
        to_cancel = [o for o in orders if o.get("status") in cancellable_status]
        if not to_cancel:
            print("ℹ️ No open orders to cancel.")
            return

        for o in to_cancel:
            try:
                kite.cancel_order(variety=o["variety"], order_id=o["order_id"])
                print(f"🧹 Cancelled order {o['order_id']} ({o.get('tradingsymbol','')}, {o.get('status')})")
            except Exception as ce:
                print(f"❌ Could not cancel order {o.get('order_id')}: {ce}")
    except Exception as e:
        print(f"❌ Error while fetching/cancelling orders: {e}")

def square_off_all_positions():
    try:
        positions = kite.positions()
        net_positions = positions.get('net', [])
        live = [p for p in net_positions if p.get('quantity', 0) != 0]

        if not live:
            print("ℹ️ No live positions to square off.")
            return

        for p in live:
            try:
                qty = abs(int(p['quantity']))
                if qty == 0:
                    continue
                # Opposite transaction
                txn = kite.TRANSACTION_TYPE_SELL if p['quantity'] > 0 else kite.TRANSACTION_TYPE_BUY

                order_id = kite.place_order(
                    variety=kite.VARIETY_REGULAR,
                    exchange=p['exchange'],
                    tradingsymbol=p['tradingsymbol'],
                    transaction_type=txn,
                    quantity=qty,
                    product=p.get('product', kite.PRODUCT_NRML),
                    order_type=kite.ORDER_TYPE_MARKET
                )
                side = "SELL" if txn == kite.TRANSACTION_TYPE_SELL else "BUY"
                print(f"✅ Squared off {p['tradingsymbol']} | qty {qty} via {side} MKT. Order ID: {order_id}")
            except Exception as pe:
                print(f"❌ Failed to square off {p.get('tradingsymbol')}: {pe}")
    except Exception as e:
        print(f"❌ Error while squaring off positions: {e}")

def exit_all_positions_and_orders():
    print("🚪 EXIT requested: cancelling open orders and squaring off live positions...")
    cancel_all_open_orders()
    # Small pause to let cancellations reflect before placing exit orders
    time.sleep(0.5)
    square_off_all_positions()
    print("🏁 EXIT completed.")

def place_order(symbol, transaction_type, lots, exchange, stoploss_absolute, stoploss_points, is_limit=False, limit_price=None):
    try:
        # Check if symbol already exists in open positions
        positions = kite.positions()
        net_positions = positions['net']
        matching_positions = [p for p in net_positions if p['tradingsymbol'] == symbol and p['quantity'] != 0]

        if matching_positions:
            print(f"⛔ Trade skipped: Open position already exists for {symbol} with quantity {matching_positions[0]['quantity']}.")
            return

        if stoploss_absolute is not None and not is_limit:
            try:
                ltp_data = kite.ltp(f"{exchange}:{symbol}")
                entry_price = float(ltp_data[f"{exchange}:{symbol}"]['last_price'])
                risk_per_lot = abs(entry_price - stoploss_absolute)
                max_loss_allowed = NO_OF_LOTS * stoploss_points

                if risk_per_lot == 0:
                    print("❌ Stoploss equals entry price. Cannot calculate risk.")
                    return

                raw_affordable_lots = int(max_loss_allowed // risk_per_lot)
                max_affordable_lots = (raw_affordable_lots // MINIMUM_LOTS) * MINIMUM_LOTS

                if max_affordable_lots < MINIMUM_LOTS:
                    print(
                        f"❌ Risk too high. Even minimum lots ({MINIMUM_LOTS}) exceed allowed loss ({max_loss_allowed}).")
                    lots = max_affordable_lots

                if lots > max_affordable_lots:
                    print(
                        f"⚠️ Reducing lots from {lots} to {max_affordable_lots} to respect max loss limit of {max_loss_allowed}")
                    lots = max_affordable_lots

            except Exception as price_err:
                print(f"❌ Failed to fetch LTP or calculate risk: {price_err}")
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
            stoploss_price = round(
                traded_price - stoploss_points if transaction_type == kite.TRANSACTION_TYPE_BUY
                else traded_price + stoploss_points, 1
            )

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
    UNDER_LYING_EXCHANGE, UNDERLYING, OPTIONS_EXCHANGE, PART_SYMBOL, NO_OF_LOTS, STRIKE_MULTIPLE, STOPLOSS_POINTS, MINIMUM_LOTS = oUtils.get_instruments(kite)
    lots = NO_OF_LOTS
    exchange = OPTIONS_EXCHANGE

    while True:
        user_raw = input("Enter (e.g., 52500CE B, 52500CE S 230, 52500CE B L 450) or EXIT: ").strip().upper()
        if not user_raw:
            print("❌ Empty input.")
            continue

        # Global EXIT command
        if user_raw == "EXIT":
            exit_all_positions_and_orders()
            continue

        user_input = user_raw.split()

        if len(user_input) < 2:
            print("❌ Invalid input. Use: 52500CE B [STOPLOSS] or 52500CE B L [PRICE] or EXIT")
            continue

        suffix_symbol = user_input[0]
        transaction = user_input[1]

        if transaction not in ['B', 'S']:
            print("❌ Transaction must be 'B' or 'S' (or type EXIT)")
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

