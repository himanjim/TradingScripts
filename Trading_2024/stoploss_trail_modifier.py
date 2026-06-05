"""
stoploss_trail_modifier.py

Purpose
-------
Separate stoploss-modifier / trailing-stop script for an already open NSE MIS position.

This script does NOT place a fresh entry trade.

It only does this:
    1. Monitors one configured stock.
    2. Detects existing open MIS position.
    3. Detects existing active protective SL-M order.
    4. Monitors a configured list of [trigger_price, new_stoploss_price].
    5. When trigger_price is reached, modifies existing SL-M trigger to new_stoploss_price.
    6. If position becomes flat, assumes stoploss/manual exit happened and exits.

Configuration format
--------------------
STOCK = "RELIANCE"

STOPLOSS_MODIFICATION_RULES = [
    [trigger_price_1, new_stoploss_1],
    [trigger_price_2, new_stoploss_2],
    ...
]

Direction is inferred from the current open position:
    Net quantity > 0  => BUY / long position
    Net quantity < 0  => SELL / short position

Trigger direction
-----------------
For BUY / long position:
    Rule triggers when LTP >= trigger_price.
    New stoploss should normally be higher than existing stoploss and below current LTP.

For SELL / short position:
    Rule triggers when LTP <= trigger_price.
    New stoploss should normally be lower than existing stoploss and above current LTP.

Example for SELL position
-------------------------
Suppose entry was SELL at 1365 and original SL is 1370.

STOPLOSS_MODIFICATION_RULES = [
    [1360.00, 1365.00],   # when price falls to 1360, move SL to 1365
    [1355.00, 1360.00],   # when price falls to 1355, move SL to 1360
    [1350.00, 1355.00],   # when price falls to 1350, move SL to 1355
]

Example for BUY position
------------------------
Suppose entry was BUY at 1365 and original SL is 1360.

STOPLOSS_MODIFICATION_RULES = [
    [1370.00, 1365.00],   # when price rises to 1370, move SL to 1365
    [1375.00, 1370.00],   # when price rises to 1375, move SL to 1370
    [1380.00, 1375.00],   # when price rises to 1380, move SL to 1375
]

Important
---------
1. This script expects exactly one active protective SL-M order for the stock.
2. If multiple active stoploss orders are found, it stops for manual checking.
3. It does not worsen stoploss:
       BUY position  => new SL must be above current SL.
       SELL position => new SL must be below current SL.
4. It avoids modifying SL to a price that would immediately trigger, unless you explicitly allow it.
5. Do not run multiple copies of this script.
"""

from __future__ import annotations

import time
import traceback
from dataclasses import dataclass
from datetime import datetime, time as dtime
from typing import Any, Dict, List, Optional, Tuple

import pytz

import OptionTradeUtils as oUtils


# =============================================================================
# USER CONFIGURATION
# =============================================================================

IST = pytz.timezone("Asia/Kolkata")

# Stock to monitor.
STOCK = "RELIANCE"

# List of [trigger_price, new_stoploss_price].
# Direction is inferred from actual open position.
STOPLOSS_MODIFICATION_RULES: List[List[float]] = [
    # Example for SELL position:
    # [1360.00, 1365.00],
    # [1355.00, 1360.00],
    # [1350.00, 1355.00],

    # Example placeholders. Replace these with your actual levels.
    [1360.00, 1365.00],
    [1355.00, 1360.00],
]

EXCHANGE = "NSE"
PRODUCT = "MIS"
VALIDITY = "DAY"

# Fast polling. For one stock, 0.25 sec is acceptable.
POLL_SECONDS = 0.25

# Broker snapshot cache.
BROKER_SNAPSHOT_TTL_SECONDS = 0.75

# Safety: avoid modifying SL outside intraday session.
ENABLE_TIME_WINDOW = True
MONITOR_START_TIME = dtime(9, 15)
MONITOR_END_TIME = dtime(15, 20)

# NSE equity tick size.
TICK_SIZE = 0.05

# Zerodha market protection for SL-M modification.
MARKET_PROTECTION = -1

# Order tag used by your entry script.
# This script does not strictly require the tag, but it prefers tagged stoploss orders if available.
ORDER_TAG = "SIMPLE_ENTRY_SL"

# If True, the script allows modifying stoploss to a level that may trigger immediately.
# Safer default is False.
ALLOW_IMMEDIATE_TRIGGER_STOPLOSS = False

# Minimum price gap between LTP and new stoploss to avoid immediate trigger/rejection.
MIN_TRIGGER_GAP_TICKS = 1

# Order modification verification.
MODIFY_VERIFY_POLLS = 8
MODIFY_VERIFY_SLEEP_SECONDS = 0.25

# Periodic status print.
PRINT_STATUS_EVERY_SECONDS = 5.0


# =============================================================================
# GLOBAL RUNTIME STATE
# =============================================================================

STOP_REQUESTED = False
STOP_REASON = ""

runtime: Dict[str, Any] = {
    "last_status_print": 0.0,
    "last_modified_stoploss": None,
}


def request_stop(reason: str) -> None:
    """
    Requests script shutdown.
    """
    global STOP_REQUESTED, STOP_REASON
    STOP_REQUESTED = True
    STOP_REASON = reason
    print(f"🛑 Stop requested: {reason}")


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class PositionTruth:
    """
    Current broker position truth for the configured stock.
    """
    has_position: bool
    side: Optional[str]           # BUY for long, SELL for short
    quantity: int                 # absolute quantity
    raw_position: Optional[Dict[str, Any]] = None


@dataclass
class StoplossTruth:
    """
    Current active protective stoploss truth.
    """
    found: bool
    order_id: Optional[str]
    trigger_price: Optional[float]
    quantity: int
    raw_order: Optional[Dict[str, Any]] = None


# =============================================================================
# BASIC HELPERS
# =============================================================================

def now_ist() -> datetime:
    return datetime.now(IST)


def round_to_tick(price: float) -> float:
    """
    Rounds price to NSE tick size.
    """
    price = max(float(price), TICK_SIZE)
    return round(round(price / TICK_SIZE) * TICK_SIZE, 2)


def prices_equal(a: float, b: float) -> bool:
    """
    Compares prices within half tick tolerance.
    """
    return abs(float(a) - float(b)) <= (TICK_SIZE / 2)


def in_monitor_window() -> bool:
    """
    Returns whether the script is allowed to monitor/modify now.
    """
    if not ENABLE_TIME_WINDOW:
        return True

    t = now_ist().time()
    return MONITOR_START_TIME <= t <= MONITOR_END_TIME


def position_side_from_qty(qty: int) -> str:
    """
    Positive quantity = long / BUY position.
    Negative quantity = short / SELL position.
    """
    return "BUY" if qty > 0 else "SELL"


def trigger_reached(position_side: str, ltp: float, trigger_price: float) -> bool:
    """
    Determines whether a trailing rule trigger is reached.

    BUY / long:
        trigger when LTP >= trigger_price.

    SELL / short:
        trigger when LTP <= trigger_price.
    """
    if position_side == "BUY":
        return float(ltp) >= float(trigger_price)

    return float(ltp) <= float(trigger_price)


def is_stoploss_improvement(position_side: str, current_sl: float, new_sl: float) -> bool:
    """
    Prevents worsening stoploss.

    BUY / long:
        stoploss should move upward only.

    SELL / short:
        stoploss should move downward only.
    """
    if position_side == "BUY":
        return float(new_sl) > float(current_sl)

    return float(new_sl) < float(current_sl)


def stoploss_would_trigger_immediately(position_side: str, ltp: float, new_sl: float) -> bool:
    """
    Checks whether new stoploss is too close or beyond current LTP.

    BUY / long:
        protective SELL SL trigger should be below LTP.

    SELL / short:
        protective BUY SL trigger should be above LTP.
    """
    min_gap = TICK_SIZE * MIN_TRIGGER_GAP_TICKS

    if position_side == "BUY":
        return float(new_sl) >= float(ltp) - min_gap

    return float(new_sl) <= float(ltp) + min_gap


def normalize_order_type(order: Dict[str, Any]) -> str:
    """
    Normalizes Kite order type string.
    """
    return str(order.get("order_type", "")).upper().replace(" ", "").replace("_", "-")


# =============================================================================
# KITE HELPERS
# =============================================================================

def quote_ltp(kite, symbol: str) -> float:
    """
    Fetches live LTP from Kite.
    """
    key = f"{EXCHANGE}:{symbol}"
    return float(kite.quote(key)[key]["last_price"])


def opposite_transaction(kite, position_side: str) -> str:
    """
    Protective stoploss transaction is opposite of position side.
    """
    if position_side == "BUY":
        return kite.TRANSACTION_TYPE_SELL

    return kite.TRANSACTION_TYPE_BUY


# =============================================================================
# BROKER SNAPSHOT CACHE
# =============================================================================

broker_cache: Dict[str, Any] = {
    "ts": 0.0,
    "positions": [],
    "orders": [],
}


def invalidate_broker_cache() -> None:
    broker_cache["ts"] = 0.0


def broker_snapshot(kite, force: bool = False) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Returns cached positions/orders.

    Use force=True after order modification or when exact broker state is needed.
    """
    now = time.monotonic()

    if (
        force
        or broker_cache["ts"] <= 0.0
        or now - float(broker_cache["ts"]) >= BROKER_SNAPSHOT_TTL_SECONDS
    ):
        broker_cache["positions"] = kite.positions().get("net", [])
        broker_cache["orders"] = kite.orders()
        broker_cache["ts"] = now

    return broker_cache["positions"], broker_cache["orders"]


# =============================================================================
# ORDER HELPERS
# =============================================================================

TERMINAL_STATUSES = {"COMPLETE", "REJECTED", "CANCELLED"}


def order_status(order: Dict[str, Any]) -> str:
    return str(order.get("status", "")).upper().strip()


def is_terminal_order(order: Dict[str, Any]) -> bool:
    return order_status(order) in TERMINAL_STATUSES


def order_trigger_price(order: Dict[str, Any]) -> float:
    try:
        return float(order.get("trigger_price") or 0.0)
    except Exception:
        return 0.0


def order_quantity(order: Dict[str, Any]) -> int:
    """
    Returns active order quantity.

    For active stoploss orders, pending_quantity is usually the active quantity.
    """
    try:
        pending = int(order.get("pending_quantity") or 0)
        if pending > 0:
            return pending
    except Exception:
        pass

    try:
        return int(order.get("quantity") or 0)
    except Exception:
        return 0


def get_order_by_id(orders: List[Dict[str, Any]], order_id: str) -> Optional[Dict[str, Any]]:
    """
    Finds an order by order_id.
    """
    for order in reversed(orders):
        if str(order.get("order_id")) == str(order_id):
            return order

    return None


# =============================================================================
# POSITION AND STOPLOSS DETECTION
# =============================================================================

def get_position_truth(kite, symbol: str) -> PositionTruth:
    """
    Reads current MIS position for the configured stock.
    """
    positions, _ = broker_snapshot(kite)

    for p in positions:
        if p.get("tradingsymbol") != symbol:
            continue

        if p.get("exchange") != EXCHANGE:
            continue

        if p.get("product") != PRODUCT:
            continue

        qty = int(p.get("quantity") or 0)

        if qty == 0:
            continue

        return PositionTruth(
            has_position=True,
            side=position_side_from_qty(qty),
            quantity=abs(qty),
            raw_position=p,
        )

    return PositionTruth(
        has_position=False,
        side=None,
        quantity=0,
        raw_position=None,
    )


def is_active_protective_stoploss_order(
    kite,
    order: Dict[str, Any],
    symbol: str,
    position_side: str,
) -> bool:
    """
    Detects an active protective SL/SL-M order.

    Criteria:
        - same stock
        - NSE
        - MIS
        - opposite transaction to position side
        - non-terminal order
        - trigger_price > 0

    This avoids depending only on order_type text because brokers may show SL-M as
    'SL-M', 'SLM', or other variants.
    """
    expected_txn = opposite_transaction(kite, position_side)

    return (
        order.get("tradingsymbol") == symbol
        and order.get("exchange") == EXCHANGE
        and order.get("product") == PRODUCT
        and str(order.get("transaction_type")) == str(expected_txn)
        and not is_terminal_order(order)
        and order_trigger_price(order) > 0
    )


def get_active_stoploss_truth(kite, symbol: str, position_side: str) -> StoplossTruth:
    """
    Finds the active protective stoploss order for the open position.

    If multiple active stoploss orders are found, the script stops for manual checking
    instead of modifying the wrong one.
    """
    _, orders = broker_snapshot(kite, force=True)

    matches = [
        order for order in orders
        if is_active_protective_stoploss_order(kite, order, symbol, position_side)
    ]

    if not matches:
        return StoplossTruth(
            found=False,
            order_id=None,
            trigger_price=None,
            quantity=0,
            raw_order=None,
        )

    # Prefer tagged orders if available.
    tagged = [o for o in matches if str(o.get("tag", "")) == ORDER_TAG]

    if len(tagged) == 1:
        chosen = tagged[0]
    elif len(matches) == 1:
        chosen = matches[0]
    else:
        print(f"⚠ {symbol}: multiple active protective stoploss orders found:")
        for o in matches:
            print(
                f"  order_id={o.get('order_id')} "
                f"status={o.get('status')} "
                f"type={o.get('order_type')} "
                f"txn={o.get('transaction_type')} "
                f"qty={o.get('quantity')} "
                f"pending={o.get('pending_quantity')} "
                f"trigger={o.get('trigger_price')} "
                f"tag={o.get('tag')}"
            )
        request_stop(f"{symbol}: multiple active SL orders; manual check required")
        return StoplossTruth(
            found=False,
            order_id=None,
            trigger_price=None,
            quantity=0,
            raw_order=None,
        )

    return StoplossTruth(
        found=True,
        order_id=str(chosen.get("order_id")),
        trigger_price=round_to_tick(order_trigger_price(chosen)),
        quantity=order_quantity(chosen),
        raw_order=chosen,
    )


# =============================================================================
# RULE SELECTION
# =============================================================================

def choose_best_applicable_rule(
    position_side: str,
    ltp: float,
    current_sl: float,
) -> Optional[Tuple[float, float]]:
    """
    Chooses the best applicable [trigger_price, new_stoploss] rule.

    It may happen that price has already crossed multiple trigger levels before
    this script sees the tick. In that case, apply the strongest improvement.

    BUY / long:
        among triggered rules, choose highest new_stoploss.

    SELL / short:
        among triggered rules, choose lowest new_stoploss.
    """
    applicable: List[Tuple[float, float]] = []

    for raw_trigger, raw_new_sl in STOPLOSS_MODIFICATION_RULES:
        trigger = round_to_tick(float(raw_trigger))
        new_sl = round_to_tick(float(raw_new_sl))

        if not trigger_reached(position_side, ltp, trigger):
            continue

        if not is_stoploss_improvement(position_side, current_sl, new_sl):
            continue

        applicable.append((trigger, new_sl))

    if not applicable:
        return None

    if position_side == "BUY":
        # Highest improved stoploss is best for long.
        return max(applicable, key=lambda x: x[1])

    # Lowest improved stoploss is best for short.
    return min(applicable, key=lambda x: x[1])


# =============================================================================
# STOPLOSS MODIFICATION
# =============================================================================

def modify_stoploss_order(
    kite,
    order_id: str,
    symbol: str,
    new_stoploss: float,
) -> bool:
    """
    Modifies active SL-M order to new trigger price.

    This script assumes existing stoploss is SL-M, as placed by the simple entry script.

    If the existing order is actually SL-Limit, this function still attempts to
    preserve order_type as SL-M. For live use, use SL-M from the entry script.
    """
    new_trigger = round_to_tick(new_stoploss)

    print(
        f"{symbol}: modifying stoploss order {order_id} "
        f"to trigger={new_trigger}"
    )

    try:
        kite.modify_order(
            variety=kite.VARIETY_REGULAR,
            order_id=str(order_id),
            order_type=getattr(kite, "ORDER_TYPE_SLM", "SL-M"),
            trigger_price=new_trigger,
            market_protection=MARKET_PROTECTION,
            validity=VALIDITY,
        )
        invalidate_broker_cache()
    except Exception as e:
        print(f"❌ {symbol}: stoploss modification failed: {e}")
        return False

    # Verify modification in order book.
    for _ in range(MODIFY_VERIFY_POLLS):
        time.sleep(MODIFY_VERIFY_SLEEP_SECONDS)

        _, orders = broker_snapshot(kite, force=True)
        order = get_order_by_id(orders, str(order_id))

        if not order:
            continue

        st = order_status(order)

        if st in {"REJECTED", "CANCELLED"}:
            print(f"❌ {symbol}: modified SL order became {st}. Manual check required.")
            request_stop(f"{symbol}: SL order {st} after modification")
            return False

        if st == "COMPLETE":
            print(f"{symbol}: SL order completed during/after modification.")
            request_stop(f"{symbol}: SL completed after modification")
            return True

        actual_trigger = order_trigger_price(order)

        if prices_equal(actual_trigger, new_trigger):
            print(
                f"✅ {symbol}: stoploss modified successfully. "
                f"order_id={order_id}, trigger={actual_trigger}, status={st}"
            )
            runtime["last_modified_stoploss"] = new_trigger
            return True

    print(
        f"⚠ {symbol}: stoploss modification could not be verified. "
        f"Manual check required."
    )
    request_stop(f"{symbol}: SL modification verification failed")
    return False


# =============================================================================
# MAIN MONITORING LOGIC
# =============================================================================

def print_status(symbol: str, ltp: float, position: PositionTruth, sl: StoplossTruth) -> None:
    """
    Prints compact periodic status.
    """
    now = time.monotonic()

    if now - float(runtime.get("last_status_print") or 0.0) < PRINT_STATUS_EVERY_SECONDS:
        return

    runtime["last_status_print"] = now

    if not position.has_position:
        print(f"{symbol}: no open MIS position. LTP={ltp}")
        return

    print(
        f"{symbol}: LTP={ltp}, position={position.side}, "
        f"qty={position.quantity}, current_SL={sl.trigger_price}, "
        f"rules={STOPLOSS_MODIFICATION_RULES}"
    )


def monitor_and_modify_stoploss(kite) -> None:
    """
    Main single-cycle monitor logic.
    """
    symbol = STOCK
    ltp = quote_ltp(kite, symbol)

    position = get_position_truth(kite, symbol)

    if not position.has_position:
        print_status(symbol, ltp, position, StoplossTruth(False, None, None, 0, None))
        request_stop(f"{symbol}: no open MIS position")
        return

    sl = get_active_stoploss_truth(kite, symbol, position.side)

    if STOP_REQUESTED:
        return

    if not sl.found:
        print(
            f"⚠ {symbol}: open {position.side} position found, "
            f"but no active protective stoploss order found. Manual check required."
        )
        request_stop(f"{symbol}: open position without SL")
        return

    print_status(symbol, ltp, position, sl)

    if sl.quantity < position.quantity:
        print(
            f"⚠ {symbol}: SL quantity is short. "
            f"position_qty={position.quantity}, sl_qty={sl.quantity}. Manual check required."
        )
        request_stop(f"{symbol}: SL quantity short")
        return

    current_sl = float(sl.trigger_price)

    selected = choose_best_applicable_rule(
        position_side=position.side,
        ltp=ltp,
        current_sl=current_sl,
    )

    if selected is None:
        return

    trigger, new_sl = selected

    if stoploss_would_trigger_immediately(position.side, ltp, new_sl):
        msg = (
            f"{symbol}: rule [{trigger}, {new_sl}] is triggered, "
            f"but new SL may trigger immediately. LTP={ltp}, position={position.side}."
        )

        if not ALLOW_IMMEDIATE_TRIGGER_STOPLOSS:
            print(f"⚠ {msg} Modification skipped.")
            return

        print(f"⚠ {msg} Proceeding because ALLOW_IMMEDIATE_TRIGGER_STOPLOSS=True.")

    print(
        f"{symbol}: rule triggered. "
        f"LTP={ltp}, position={position.side}, "
        f"current_SL={current_sl}, trigger={trigger}, new_SL={new_sl}"
    )

    modify_stoploss_order(
        kite=kite,
        order_id=sl.order_id,
        symbol=symbol,
        new_stoploss=new_sl,
    )


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    # Basic config validation.
    if not STOPLOSS_MODIFICATION_RULES:
        raise ValueError("STOPLOSS_MODIFICATION_RULES cannot be empty.")

    print("Initializing Kite API...")
    kite = oUtils.intialize_kite_api()

    print("\nStoploss modifier started.")
    print(f"Stock: {STOCK}")
    print(f"Exchange: {EXCHANGE}")
    print(f"Product: {PRODUCT}")
    print(f"Polling interval: {POLL_SECONDS} sec")
    print(f"Rules: {STOPLOSS_MODIFICATION_RULES}")
    print("This script modifies existing protective SL-M order only.")
    print("Do not run multiple copies of this script.\n")

    while not STOP_REQUESTED:
        try:
            if not in_monitor_window():
                print("Outside monitoring window. Waiting...")
                time.sleep(5)
                continue

            monitor_and_modify_stoploss(kite)
            time.sleep(POLL_SECONDS)

        except KeyboardInterrupt:
            print("\nStopped by user.")
            break

        except Exception as e:
            print(f"Main loop error: {e}")
            traceback.print_exc()
            time.sleep(1)

    if STOP_REASON:
        print(f"Script exited: {STOP_REASON}")


if __name__ == "__main__":
    main()