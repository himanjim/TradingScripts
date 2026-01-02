# one_keystroke_otm.py
from datetime import datetime
import pytz, math, time
from decimal import Decimal, ROUND_HALF_UP
import OptionTradeUtils as oUtils

indian_timezone = pytz.timezone('Asia/Calcutta')
kite = oUtils.intialize_kite_api()

# --- Exchange params from your utilities ---
# get_instruments must return:
# UNDER_LYING_EXCHANGE, UNDERLYING, OPTIONS_EXCHANGE, PART_SYMBOL,
# NO_OF_LOTS, STRIKE_MULTIPLE, STOPLOSS_POINTS, MINIMUM_LOTS
UNDER_LYING_EXCHANGE, UNDERLYING, OPTIONS_EXCHANGE, PART_SYMBOL, NO_OF_LOTS, STRIKE_MULTIPLE, STOPLOSS_POINTS, MINIMUM_LOTS = oUtils.get_instruments(kite)

# Adjust if your broker/exchange uses a different tick for options
TICK_SIZE = 0.05

def tick_round(price: float, tick: float = TICK_SIZE) -> float:
    """Round to nearest tick using half-up (0.05 for NSE options)."""
    dprice = Decimal(str(price))
    dtick = Decimal(str(tick))
    return float((dprice / dtick).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * dtick)

# ------------ Exit helpers ------------
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
                print(f"🧹 Cancelled {o['order_id']} ({o.get('tradingsymbol','')}, {o.get('status')})")
            except Exception as ce:
                print(f"❌ Could not cancel {o.get('order_id')}: {ce}")
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
    time.sleep(0.5)
    square_off_all_positions()
    print("🏁 EXIT completed.")

# ------------ Strike selection: ATM ± one strike (half-up rounding) ------------
def nearest_atm(price: float, step: int) -> int:
    # Half-up: add half step then floor-divide
    return int(math.floor((price + step / 2) / step) * step)

def compute_one_strike_otm(ltp: float, step: int, side: str) -> int:
    atm = nearest_atm(ltp, step)
    return atm + step if side == 'CE' else atm - step

# ------------ Order placement (SL derived from executed price) ------------
def place_order(symbol, transaction_type, qty, exchange, stoploss_points, is_limit=False, limit_price=None):
    try:
        positions = kite.positions()
        net_positions = positions.get('net', [])
        matching_positions = [p for p in net_positions if p.get('tradingsymbol') == symbol and p.get('quantity', 0) != 0]
        if matching_positions:
            print(f"⛔ Trade skipped: Open position already exists for {symbol} with quantity {matching_positions[0]['quantity']}.")
            return

        order_type = kite.ORDER_TYPE_LIMIT if is_limit else kite.ORDER_TYPE_MARKET
        args = {
            "tradingsymbol": symbol,
            "variety": kite.VARIETY_REGULAR,
            "exchange": exchange,
            "transaction_type": transaction_type,
            "quantity": qty,
            "order_type": order_type,
            "product": kite.PRODUCT_NRML
        }
        if is_limit:
            args["price"] = limit_price

        order_id = kite.place_order(**args)
        print(f"✅ {'Limit' if is_limit else 'Market'} order placed ({transaction_type}) for {symbol} at {datetime.now(indian_timezone).time()}. Order ID: {order_id}")

        if is_limit:
            return  # SL only for market flow

        # Derive SL from executed price (retry to obtain fill)
        executed_order = None
        for attempt in range(1, 4):
            orders = kite.orders()
            executed_order = next((o for o in orders if o.get('order_id') == order_id and o.get('status') == 'COMPLETE'), None)
            if executed_order:
                break
            print(f"🔄 Attempt {attempt}: Executed order not yet found. Retrying in 1 second...")
            time.sleep(1)
        if not executed_order:
            print("❌ Market order not completed or not found after 3 attempts. Cannot place stop-loss.")
            return

        traded_price = float(executed_order['average_price'])
        if transaction_type == kite.TRANSACTION_TYPE_BUY:
            raw_sl = traded_price - stoploss_points
            sl_txn = kite.TRANSACTION_TYPE_SELL
        else:
            raw_sl = traded_price + stoploss_points
            sl_txn = kite.TRANSACTION_TYPE_BUY

        stoploss_price = tick_round(raw_sl, TICK_SIZE)

        sl_order_id = kite.place_order(
            tradingsymbol=symbol,
            variety=kite.VARIETY_REGULAR,
            exchange=exchange,
            transaction_type=sl_txn,
            quantity=qty,
            order_type=kite.ORDER_TYPE_SL,   # use SL; change to ORDER_TYPE_SL_M if you prefer SL-M
            product=kite.PRODUCT_NRML,
            trigger_price=stoploss_price,
            price=stoploss_price
        )
        print(f"📉 Stoploss order placed at {stoploss_price:.2f} with Order ID: {sl_order_id}")

    except Exception as e:
        print(f"❌ Error occurred while placing order: {e}")

# ------------ Main loop (C, P, E) ------------
if __name__ == "__main__":
    print("🔹 Controls: [C] buy 1-strike OTM CE | [P] buy 1-strike OTM PE | [E] Exit (cancel orders + square off)")
    print(f"📈 Underlying: {UNDERLYING} on {UNDER_LYING_EXCHANGE} | Options root: {PART_SYMBOL} on {OPTIONS_EXCHANGE}")
    print(f"⚙️ Qty: {NO_OF_LOTS} | Strike step: {STRIKE_MULTIPLE} | SL points: {STOPLOSS_POINTS} | Tick: {TICK_SIZE}")

    while True:
        try:
            key = input("\nPress key [C/P/E]: ").strip().upper()
            if not key:
                continue

            if key == 'E':
                exit_all_positions_and_orders()
                continue

            if key not in ('C', 'P'):
                print("❌ Invalid key. Use C, P or E.")
                continue

            # 1) Underlying LTP
            try:
                token = f"{UNDER_LYING_EXCHANGE}{UNDERLYING}"
                ltp_obj = kite.ltp(token)
                ltp = float(ltp_obj[token]["last_price"])
            except Exception as e:
                print(f"❌ Could not fetch underlying LTP: {e}")
                continue

            side = 'CE' if key == 'C' else 'PE'
            step = int(STRIKE_MULTIPLE)
            strike = compute_one_strike_otm(ltp, step, side)

            # 2) Build option symbol
            opt_symbol = f"{PART_SYMBOL.replace(':','')}{strike}{side}"
            print(f"🧮 LTP={ltp:.2f} | ATM={nearest_atm(ltp, step)} → 1-strike OTM {side} = {strike} → {opt_symbol}")

            # 3) Market BUY + SL (derived)
            place_order(
                symbol=opt_symbol,
                transaction_type=kite.TRANSACTION_TYPE_BUY,
                qty=NO_OF_LOTS,                  # Assumed as actual exchange quantity per your utils
                exchange=OPTIONS_EXCHANGE,
                stoploss_points=STOPLOSS_POINTS,
                is_limit=False
            )

        except KeyboardInterrupt:
            print("\n👋 Exiting script (Ctrl+C).")
            break
        except Exception as e:
            print(f"❌ Unexpected error in main loop: {e}")
