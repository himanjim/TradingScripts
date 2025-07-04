from OptionTradeUtils import intialize_kite_api, get_instruments
from collections import defaultdict
from datetime import datetime
import pytz

# Initialize Kite API and timezone
kite = intialize_kite_api()
indian_timezone = pytz.timezone('Asia/Calcutta')

# Get stoploss point value (e.g., 30 for SENSEX, 10 for NIFTY)
_, _, options_exchange, _, _, _, STOPLOSS_POINTS = get_instruments(kite)


def get_active_directional_positions():
    """
    Identify PE/CE positions that are not part of a straddle (directional trades).
    """
    positions = kite.positions()['net']
    pe_positions = {}
    ce_positions = {}

    for pos in positions:
        symbol = pos['tradingsymbol']
        qty = pos['quantity']
        if qty == 0:
            continue
        if 'PE' in symbol:
            pe_positions[symbol] = pos
        elif 'CE' in symbol:
            ce_positions[symbol] = pos

    directional = {}
    for pe in pe_positions:
        ce_match = pe.replace('PE', 'CE')
        if ce_match not in ce_positions:
            directional[pe] = pe_positions[pe]

    for ce in ce_positions:
        pe_match = ce.replace('CE', 'PE')
        if pe_match not in pe_positions:
            directional[ce] = ce_positions[ce]

    return directional


def get_last_order_for_symbol(symbol, orders):
    """
    Get the most recent filled order for a symbol.
    """
    for order in reversed(orders):
        if order['tradingsymbol'] == symbol and order['status'] == 'COMPLETE':
            return order
    return None


def get_existing_sl_order(symbol, orders):
    """
    Return the active SL order if it exists.
    """
    for order in orders:
        if (
            order['tradingsymbol'] == symbol and
            order['order_type'] in ['SL', 'SL-M'] and
            order['status'] in ['TRIGGER PENDING', 'OPEN']
        ):
            return order
    return None


def calculate_trailing_sl(entry_price, pnl, direction, qty):
    """
    Determine new SL price based on profit thresholds.
    """
    if direction == 'SELL':
        if pnl >= 20000:
            return entry_price - (10000 / qty)
        elif pnl >= 10000:
            return entry_price
    elif direction == 'BUY':
        if pnl >= 20000:
            return entry_price + (10000 / qty)
        elif pnl >= 5000:
            return entry_price
    return None


def cancel_order(order_id):
    """
    Cancel a live order.
    """
    try:
        kite.cancel_order(
            variety=kite.VARIETY_REGULAR,
            order_id=order_id
        )
        print(f"❌ Cancelled old SL order ID: {order_id}")
    except Exception as e:
        print(f"⚠️ Failed to cancel order {order_id}: {e}")


def place_stoploss_order(symbol, direction, qty, sl_price):
    """
    Place new SL order (after cancelling old one if needed).
    """
    trigger_price = round(sl_price, 1)
    try:
        order_id = kite.place_order(
            tradingsymbol=symbol,
            variety=kite.VARIETY_REGULAR,
            exchange=options_exchange,
            transaction_type=kite.TRANSACTION_TYPE_SELL if direction == 'BUY' else kite.TRANSACTION_TYPE_BUY,
            quantity=qty,
            order_type=kite.ORDER_TYPE_SL,
            product=kite.PRODUCT_NRML,
            price=trigger_price,
            trigger_price=trigger_price
        )
        print(f"✅ SL order placed: {symbol} @ {trigger_price} ({direction}) | Order ID: {order_id}")
    except Exception as e:
        print(f"⚠️ Failed to place SL for {symbol}: {e}")


def manage_directional_trades():
    """
    Core function to detect directional trades and manage their SLs.
    """
    directional_positions = get_active_directional_positions()

    if len(directional_positions) == 0:
        return


    try:
        ltp_data = kite.ltp([f'{options_exchange}:{tradingsymbol}' for tradingsymbol in directional_positions])
    except Exception as e:
        print(f"❌ Failed to fetch LTPs: {e}")
        return

    for symbol, pos in directional_positions.items():
        orders = kite.orders()

        last_order = get_last_order_for_symbol(symbol, orders)
        if not last_order:
            print(f"⚠️ No order found for {symbol}")
            continue

        direction = last_order['transaction_type']
        qty = abs(pos['quantity'])
        entry_price = last_order['average_price']
        ltp = ltp_data[f'{options_exchange}:{symbol}']['last_price']
        pnl = (ltp - entry_price) * qty if direction == 'BUY' else (entry_price - ltp) * qty

        print(f"🟨 {symbol} | {direction} | Entry: {entry_price} | LTP: {ltp} | PnL: {pnl}")

        sl_order = get_existing_sl_order(symbol, orders)

        if not sl_order:
            # No SL yet, place initial
            sl_price = ltp - STOPLOSS_POINTS if direction == 'BUY' else ltp + STOPLOSS_POINTS
            place_stoploss_order(symbol, direction, qty, sl_price)
        else:
            # SL exists → check if trailing needed
            new_sl = calculate_trailing_sl(entry_price, pnl, direction, qty)
            if new_sl:
                current_trigger = float(sl_order['trigger_price'])
                if (direction == 'BUY' and new_sl > current_trigger) or \
                   (direction == 'SELL' and new_sl < current_trigger):
                    cancel_order(sl_order['order_id'])
                    place_stoploss_order(symbol, direction, qty, new_sl)
                else:
                    print(f"⏩ SL for {symbol} is already better or equal. No update needed.")

# 🔁 Run once
if __name__ == '__main__':
    manage_directional_trades()
