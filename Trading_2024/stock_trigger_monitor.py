"""
simple_entry_stoploss_monitor.py

Simple live NSE equity entry + stop-loss monitor using Zerodha Kite.

Core logic
----------
For each configured stock:

1. Configure only:
      - entry price
      - stoploss price
      - maximum loss in rupees

2. BUY/SELL is inferred automatically:
      - if entry_price > stoploss_price  => BUY
      - if entry_price < stoploss_price  => SELL

3. Quantity is calculated as:
      quantity = floor(max_loss_rupees / abs(entry_price - stoploss_price))

4. Entry trigger:
      BUY  => place trade when LTP >= entry_price
      SELL => place trade when LTP <= entry_price

   This means if the entry point is already crossed when the script starts,
   the trade is placed immediately.

5. Entry order:
      - placed as MARKET order with market_protection
      - order status is monitored
      - if still pending, script tries to modify it to MARKET again

6. Stoploss:
      - placed only after entry order is COMPLETE
      - placed as broker-side SL-M order with market_protection
      - BUY entry  => protective SELL SL-M
      - SELL entry => protective BUY SL-M

7. After stoploss is hit or position becomes flat, script exits.

Important
---------
Do not run multiple copies of this script simultaneously.
Test with a small MAX_LOSS_RUPEES first.
"""

from __future__ import annotations

import math
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

# -------------------------------------------------------------------------
# Format:
#
# "SYMBOL": {
#     "entry": entry_price,
#     "stoploss": stoploss_price,
#     "loss": maximum_loss_in_rupees
# }
#
# Side is inferred:
#     entry > stoploss => BUY
#     entry < stoploss => SELL
# -------------------------------------------------------------------------
TRADE_SETUPS: Dict[str, Dict[str, float]] = {
    "RELIANCE": {
        "entry": 1365.20,
        "stoploss": 1370.00,
        "loss": 3000.00,
    },
}

EXCHANGE = "NSE"
PRODUCT = "MIS"
VALIDITY = "DAY"

# Polling interval for LTP checks.
# 0.25 sec is aggressive but acceptable for one or two stocks.
# Increase to 0.5 or 1.0 if monitoring many stocks.
POLL_SECONDS = 0.25

# Broker snapshot cache.
# This avoids hitting positions()/orders() on every 0.25 sec tick.
BROKER_SNAPSHOT_TTL_SECONDS = 0.75

# Entry window.
ENABLE_ENTRY_TIME_WINDOW = True
ENTRY_START_TIME = dtime(9, 15)
ENTRY_END_TIME = dtime(15, 10)

# Optional EOD stop. If position is still open at this time,
# the script will NOT square off by market order. It will stop with warning.
# This script is intentionally simple: entry + broker SL only.
ENABLE_EOD_WARNING = True
EOD_WARNING_TIME = dtime(15, 18)

# NSE tick size.
TICK_SIZE = 0.05

# Zerodha market protection.
# -1 means Zerodha's automatic market protection.
MARKET_PROTECTION = -1

# Order monitoring.
ORDER_STATUS_POLL_SECONDS = 0.25
ORDER_STATUS_MAX_POLLS = 20

# Zerodha order tag.
ORDER_TAG = "SIMPLE_ENTRY_SL"

# Optional cap to prevent accidental oversized orders.
# Set to None to disable.
MAX_QTY_CAP: Optional[int] = 500

# Status print frequency.
PRINT_WAITING_STATUS_EVERY_SECONDS = 5.0


# =============================================================================
# RUNTIME GLOBALS
# =============================================================================

STOP_REQUESTED = False
STOP_REASON = ""


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
class Setup:
    symbol: str
    entry: float
    stoploss: float
    max_loss_rupees: float
    side: str          # BUY or SELL
    quantity: int


@dataclass
class OrderResult:
    order_id: Optional[str]
    complete: bool
    status: str
    average_price: Optional[float]
    filled_quantity: int


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


def in_entry_window() -> bool:
    """
    Returns whether fresh entry is allowed at current time.
    """
    if not ENABLE_ENTRY_TIME_WINDOW:
        return True

    t = now_ist().time()
    return ENTRY_START_TIME <= t <= ENTRY_END_TIME


def eod_warning_time_reached() -> bool:
    if not ENABLE_EOD_WARNING:
        return False

    return now_ist().time() >= EOD_WARNING_TIME


def infer_side(entry: float, stoploss: float) -> str:
    """
    Infers trade side from entry and stoploss.

    entry > stoploss => BUY
    entry < stoploss => SELL
    """
    if entry > stoploss:
        return "BUY"

    if entry < stoploss:
        return "SELL"

    raise ValueError("Entry price and stoploss price cannot be the same.")


def entry_condition_met(setup: Setup, ltp: float) -> bool:
    """
    BUY:
        enter when LTP >= entry

    SELL:
        enter when LTP <= entry
    """
    if setup.side == "BUY":
        return float(ltp) >= setup.entry

    return float(ltp) <= setup.entry


def position_side_from_quantity(quantity: int) -> str:
    """
    Positive broker net quantity means long/BUY position.
    Negative broker net quantity means short/SELL position.
    """
    return "BUY" if quantity > 0 else "SELL"


# =============================================================================
# SETUP PARSING
# =============================================================================

def parse_setup(symbol: str, raw: Dict[str, float]) -> Setup:
    """
    Parses one configured trade setup.
    """
    entry = round_to_tick(float(raw["entry"]))
    stoploss = round_to_tick(float(raw["stoploss"]))
    max_loss = float(raw["loss"])

    if max_loss <= 0:
        raise ValueError(f"{symbol}: loss must be positive.")

    side = infer_side(entry, stoploss)

    risk_per_share = abs(entry - stoploss)

    if risk_per_share <= 0:
        raise ValueError(f"{symbol}: invalid risk/share.")

    qty = int(math.floor(max_loss / risk_per_share))

    if qty <= 0:
        raise ValueError(
            f"{symbol}: calculated quantity is 0. "
            f"loss={max_loss}, risk/share={risk_per_share}"
        )

    if MAX_QTY_CAP is not None and qty > MAX_QTY_CAP:
        print(f"{symbol}: quantity capped from {qty} to {MAX_QTY_CAP}")
        qty = MAX_QTY_CAP

    return Setup(
        symbol=symbol,
        entry=entry,
        stoploss=stoploss,
        max_loss_rupees=max_loss,
        side=side,
        quantity=qty,
    )


def load_setups() -> Dict[str, Setup]:
    """
    Loads and validates all configured setups.
    """
    return {
        symbol: parse_setup(symbol, raw)
        for symbol, raw in TRADE_SETUPS.items()
    }


# =============================================================================
# KITE HELPERS
# =============================================================================

def entry_transaction(kite, side: str) -> str:
    """
    Returns Zerodha transaction type for entry.
    """
    return kite.TRANSACTION_TYPE_BUY if side == "BUY" else kite.TRANSACTION_TYPE_SELL


def exit_transaction(kite, side: str) -> str:
    """
    Returns opposite transaction type for stoploss/exit.
    """
    return kite.TRANSACTION_TYPE_SELL if side == "BUY" else kite.TRANSACTION_TYPE_BUY


def quote_ltp(kite, symbol: str) -> float:
    """
    Fetches live LTP from Kite quote.
    """
    key = f"{EXCHANGE}:{symbol}"
    return float(kite.quote(key)[key]["last_price"])


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

    Force refresh after order placement, modification, cancellation, or whenever
    accurate broker state is required.
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


def order_pending_quantity(order: Dict[str, Any]) -> int:
    try:
        return int(order.get("pending_quantity") or 0)
    except Exception:
        return 0


def order_filled_quantity(order: Dict[str, Any]) -> int:
    try:
        return int(order.get("filled_quantity") or 0)
    except Exception:
        return 0


def order_quantity(order: Dict[str, Any]) -> int:
    try:
        return int(order.get("quantity") or 0)
    except Exception:
        return 0


def order_active_quantity(order: Dict[str, Any]) -> int:
    """
    For open SL orders, pending_quantity is the active quantity.
    Fallback to quantity if pending_quantity is missing.
    """
    pending = order_pending_quantity(order)

    if pending > 0:
        return pending

    return order_quantity(order)


def get_order_by_id(orders: List[Dict[str, Any]], order_id: str) -> Optional[Dict[str, Any]]:
    """
    Finds an order row by Zerodha order ID.
    """
    for order in reversed(orders):
        if str(order.get("order_id")) == str(order_id):
            return order

    return None


def average_price(order: Optional[Dict[str, Any]]) -> Optional[float]:
    if not order:
        return None

    try:
        px = float(order.get("average_price") or 0.0)
        return px if px > 0 else None
    except Exception:
        return None


# =============================================================================
# POSITION HELPERS
# =============================================================================

def find_position(positions: List[Dict[str, Any]], symbol: str) -> Optional[Dict[str, Any]]:
    """
    Finds non-zero MIS position for a symbol.
    """
    for p in positions:
        if p.get("tradingsymbol") != symbol:
            continue

        if p.get("exchange") != EXCHANGE:
            continue

        if p.get("product") != PRODUCT:
            continue

        if int(p.get("quantity") or 0) != 0:
            return p

    return None


def get_position_quantity(kite, symbol: str) -> int:
    """
    Returns current net MIS quantity for symbol.
    """
    positions, _ = broker_snapshot(kite, force=True)
    pos = find_position(positions, symbol)

    if not pos:
        return 0

    return int(pos.get("quantity") or 0)


def has_matching_position(kite, setup: Setup) -> bool:
    """
    Checks whether an existing position matches the configured direction.
    """
    qty = get_position_quantity(kite, setup.symbol)

    if qty == 0:
        return False

    return position_side_from_quantity(qty) == setup.side


# =============================================================================
# PROTECTIVE SL DETECTION
# =============================================================================

def is_active_protective_sl_order(kite, order: Dict[str, Any], setup: Setup) -> bool:
    """
    Detects active protective SL/SL-M orders for this setup.

    Detection is intentionally based on:
      - same symbol
      - same exchange/product
      - opposite transaction type
      - non-terminal order status
      - trigger_price > 0

    This is safer than relying only on order_type text.
    """
    expected_txn = exit_transaction(kite, setup.side)

    return (
        order.get("tradingsymbol") == setup.symbol
        and order.get("exchange") == EXCHANGE
        and order.get("product") == PRODUCT
        and str(order.get("transaction_type")) == str(expected_txn)
        and not is_terminal_order(order)
        and order_trigger_price(order) > 0
    )


def active_protective_sl_quantity(kite, setup: Setup, orders: List[Dict[str, Any]]) -> int:
    """
    Returns total active protective SL quantity.
    """
    total = 0

    for order in orders:
        if is_active_protective_sl_order(kite, order, setup):
            total += order_active_quantity(order)

    return total


def cancel_stale_tagged_sl_if_no_position(kite, setup: Setup) -> None:
    """
    If position is flat, cancel any active tagged SL order left behind by the script.
    """
    qty = get_position_quantity(kite, setup.symbol)

    if qty != 0:
        return

    _, orders = broker_snapshot(kite, force=True)

    for order in orders:
        if order.get("tradingsymbol") != setup.symbol:
            continue

        if order.get("exchange") != EXCHANGE:
            continue

        if order.get("product") != PRODUCT:
            continue

        if str(order.get("tag", "")) != ORDER_TAG:
            continue

        if is_terminal_order(order):
            continue

        if order_trigger_price(order) <= 0:
            continue

        try:
            kite.cancel_order(
                variety=kite.VARIETY_REGULAR,
                order_id=str(order.get("order_id")),
            )
            invalidate_broker_cache()
            print(f"{setup.symbol}: cancelled stale SL order {order.get('order_id')}")
        except Exception as e:
            print(f"WARNING: could not cancel stale SL order {order.get('order_id')}: {e}")


# =============================================================================
# ENTRY ORDER PLACEMENT WITH MARKET PROTECTION
# =============================================================================

def monitor_market_order_until_complete(kite, order_id: str, symbol: str) -> OrderResult:
    """
    Monitors an entry/exit MARKET order.

    If the order remains pending, tries to modify it again to MARKET with
    market protection.

    MARKET orders normally complete quickly, but this function makes the
    behaviour explicit and safer.
    """
    modified_once = False
    last_status = "UNKNOWN"
    last_avg_price: Optional[float] = None
    last_filled_qty = 0

    for _ in range(ORDER_STATUS_MAX_POLLS):
        time.sleep(ORDER_STATUS_POLL_SECONDS)

        orders = kite.orders()
        order = get_order_by_id(orders, order_id)

        if not order:
            continue

        st = order_status(order)
        pending_qty = order_pending_quantity(order)
        filled_qty = order_filled_quantity(order)

        last_status = st
        last_avg_price = average_price(order)
        last_filled_qty = filled_qty

        if st == "COMPLETE" and pending_qty == 0:
            invalidate_broker_cache()
            return OrderResult(
                order_id=order_id,
                complete=True,
                status=st,
                average_price=last_avg_price,
                filled_quantity=filled_qty,
            )

        if st in {"REJECTED", "CANCELLED"}:
            invalidate_broker_cache()
            return OrderResult(
                order_id=order_id,
                complete=False,
                status=st,
                average_price=last_avg_price,
                filled_quantity=filled_qty,
            )

        # If the order is still pending/open, try to force it to MARKET again.
        if pending_qty > 0 and not modified_once:
            try:
                kite.modify_order(
                    variety=kite.VARIETY_REGULAR,
                    order_id=order_id,
                    order_type=kite.ORDER_TYPE_MARKET,
                    market_protection=MARKET_PROTECTION,
                )
                modified_once = True
                print(f"{symbol}: pending order modified to MARKET with market protection")
                invalidate_broker_cache()
            except Exception as e:
                print(f"WARNING: could not modify pending order {order_id}: {e}")

    invalidate_broker_cache()

    return OrderResult(
        order_id=order_id,
        complete=False,
        status=last_status,
        average_price=last_avg_price,
        filled_quantity=last_filled_qty,
    )


def place_market_order_with_protection(
    kite,
    setup: Setup,
    transaction_type: str,
    quantity: int,
    purpose: str,
) -> OrderResult:
    """
    Places a MARKET order with Zerodha market protection and monitors completion.
    """
    print(
        f"{setup.symbol}: placing {purpose} MARKET order. "
        f"txn={transaction_type}, qty={quantity}, product={PRODUCT}, "
        f"market_protection={MARKET_PROTECTION}"
    )

    order_id = kite.place_order(
        tradingsymbol=setup.symbol,
        variety=kite.VARIETY_REGULAR,
        exchange=EXCHANGE,
        transaction_type=transaction_type,
        quantity=quantity,
        order_type=kite.ORDER_TYPE_MARKET,
        product=PRODUCT,
        validity=VALIDITY,
        market_protection=MARKET_PROTECTION,
        tag=ORDER_TAG,
    )

    invalidate_broker_cache()

    return monitor_market_order_until_complete(
        kite=kite,
        order_id=str(order_id),
        symbol=setup.symbol,
    )


# =============================================================================
# STOPLOSS PLACEMENT
# =============================================================================

def place_stoploss_after_entry_complete(kite, setup: Setup, quantity: int) -> Optional[str]:
    """
    Places broker-side protective SL-M order after entry is complete.

    BUY entry:
        protective order = SELL SL-M at stoploss

    SELL entry:
        protective order = BUY SL-M at stoploss
    """
    # Check existing active SL quantity first. This prevents duplicate SL orders.
    _, orders = broker_snapshot(kite, force=True)
    existing_sl_qty = active_protective_sl_quantity(kite, setup, orders)

    if existing_sl_qty >= quantity:
        print(f"{setup.symbol}: protective SL already exists for qty={existing_sl_qty}")
        return None

    deficit_qty = quantity - existing_sl_qty

    sl_txn = exit_transaction(kite, setup.side)
    trigger_price = round_to_tick(setup.stoploss)

    print(
        f"{setup.symbol}: placing protective SL-M. "
        f"txn={sl_txn}, qty={deficit_qty}, trigger={trigger_price}, "
        f"market_protection={MARKET_PROTECTION}"
    )

    order_id = kite.place_order(
        tradingsymbol=setup.symbol,
        variety=kite.VARIETY_REGULAR,
        exchange=EXCHANGE,
        transaction_type=sl_txn,
        quantity=deficit_qty,
        order_type=getattr(kite, "ORDER_TYPE_SLM", "SL-M"),
        trigger_price=trigger_price,
        product=PRODUCT,
        validity=VALIDITY,
        market_protection=MARKET_PROTECTION,
        tag=ORDER_TAG,
    )

    invalidate_broker_cache()

    # Confirm that SL order is visible and not rejected/cancelled.
    for _ in range(ORDER_STATUS_MAX_POLLS):
        time.sleep(ORDER_STATUS_POLL_SECONDS)

        _, fresh_orders = broker_snapshot(kite, force=True)
        order = get_order_by_id(fresh_orders, str(order_id))

        if not order:
            continue

        st = order_status(order)

        if st in {"REJECTED", "CANCELLED"}:
            print(f"❌ {setup.symbol}: SL-M order {st}. Manual action required.")
            request_stop(f"{setup.symbol}: SL-M rejected/cancelled")
            return None

        if st == "COMPLETE":
            print(f"{setup.symbol}: SL-M completed immediately.")
            return str(order_id)

        # Any non-terminal status with trigger price is acceptable:
        # OPEN, TRIGGER PENDING, OPEN PENDING, etc.
        if not is_terminal_order(order) and order_trigger_price(order) > 0:
            print(f"{setup.symbol}: protective SL-M active. order_id={order_id}, status={st}")
            return str(order_id)

    print(f"⚠ {setup.symbol}: SL-M order not confirmed in order book. Manual check required.")
    request_stop(f"{setup.symbol}: SL-M confirmation timeout")
    return str(order_id)


def ensure_stoploss_exists_for_open_position(kite, setup: Setup) -> None:
    """
    If position is open but protective SL is missing/short, place missing SL quantity.
    """
    qty = get_position_quantity(kite, setup.symbol)

    if qty == 0:
        return

    actual_side = position_side_from_quantity(qty)

    if actual_side != setup.side:
        print(
            f"⚠ {setup.symbol}: open position side={actual_side}, "
            f"but setup side={setup.side}. Script will not manage this position."
        )
        request_stop(f"{setup.symbol}: unmanaged opposite-side position")
        return

    position_qty = abs(qty)

    _, orders = broker_snapshot(kite, force=True)
    sl_qty = active_protective_sl_quantity(kite, setup, orders)

    if sl_qty >= position_qty:
        return

    print(
        f"⚠ {setup.symbol}: SL missing/short. "
        f"position_qty={position_qty}, active_sl_qty={sl_qty}"
    )

    place_stoploss_after_entry_complete(kite, setup, position_qty - sl_qty)


# =============================================================================
# MAIN STRATEGY
# =============================================================================

def print_waiting_status(setup: Setup, ltp: float) -> None:
    """
    Prints compact waiting status periodically.
    """
    st = state_for_symbol(setup.symbol)
    now = time.monotonic()

    if now - float(st.get("last_status_print") or 0.0) < PRINT_WAITING_STATUS_EVERY_SECONDS:
        return

    st["last_status_print"] = now

    condition = ">=" if setup.side == "BUY" else "<="

    print(
        f"{setup.symbol}: waiting. LTP={ltp}, "
        f"entry condition: LTP {condition} {setup.entry}, "
        f"side={setup.side}, qty={setup.quantity}, stoploss={setup.stoploss}"
    )


# Separate simple state dict for status printing.
simple_runtime: Dict[str, Dict[str, Any]] = {}


def state_for_symbol(symbol: str) -> Dict[str, Any]:
    if symbol not in simple_runtime:
        simple_runtime[symbol] = {
            "entry_done": False,
            "entry_order_id": None,
            "sl_order_id": None,
            "last_status_print": 0.0,
        }

    return simple_runtime[symbol]


def handle_existing_position_on_startup(kite, setup: Setup) -> bool:
    """
    If script is restarted while position is already open, adopt/manage it.

    Returns:
        True  => position already exists and is now being managed.
        False => no position exists.
    """
    qty = get_position_quantity(kite, setup.symbol)

    if qty == 0:
        cancel_stale_tagged_sl_if_no_position(kite, setup)
        return False

    actual_side = position_side_from_quantity(qty)

    if actual_side != setup.side:
        print(
            f"⚠ {setup.symbol}: existing position side={actual_side}, "
            f"setup side={setup.side}. Script will not manage this."
        )
        request_stop(f"{setup.symbol}: unmanaged existing position")
        return True

    print(
        f"{setup.symbol}: existing {actual_side} position detected on startup. "
        f"qty={abs(qty)}. Ensuring stoploss."
    )

    state_for_symbol(setup.symbol)["entry_done"] = True
    ensure_stoploss_exists_for_open_position(kite, setup)

    return True


def place_entry_if_triggered(kite, setup: Setup, ltp: float) -> None:
    """
    Checks entry condition and places entry if triggered/crossed.
    """
    rt = state_for_symbol(setup.symbol)

    if rt["entry_done"]:
        return

    if not in_entry_window():
        print_waiting_status(setup, ltp)
        return

    if not entry_condition_met(setup, ltp):
        print_waiting_status(setup, ltp)
        return

    print(
        f"{setup.symbol}: entry condition met. "
        f"LTP={ltp}, entry={setup.entry}, side={setup.side}"
    )

    # Check again that no position exists before entry.
    qty_now = get_position_quantity(kite, setup.symbol)

    if qty_now != 0:
        print(f"{setup.symbol}: position already exists before entry. Managing existing position.")
        rt["entry_done"] = True
        ensure_stoploss_exists_for_open_position(kite, setup)
        return

    txn = entry_transaction(kite, setup.side)

    result = place_market_order_with_protection(
        kite=kite,
        setup=setup,
        transaction_type=txn,
        quantity=setup.quantity,
        purpose="ENTRY",
    )

    rt["entry_order_id"] = result.order_id

    if not result.complete:
        print(
            f"❌ {setup.symbol}: entry order not complete. "
            f"status={result.status}, filled_qty={result.filled_quantity}. "
            f"Manual check required."
        )

        # If partial fill happened, protect whatever got filled.
        if result.filled_quantity > 0:
            print(f"⚠ {setup.symbol}: partial fill detected. Placing SL for filled qty={result.filled_quantity}")
            rt["entry_done"] = True
            sl_id = place_stoploss_after_entry_complete(kite, setup, result.filled_quantity)
            rt["sl_order_id"] = sl_id

        request_stop(f"{setup.symbol}: entry order incomplete/manual check")
        return

    filled_qty = result.filled_quantity if result.filled_quantity > 0 else setup.quantity

    print(
        f"{setup.symbol}: entry COMPLETE. "
        f"order_id={result.order_id}, avg_price={result.average_price}, filled_qty={filled_qty}"
    )

    rt["entry_done"] = True

    # Stoploss is placed only after entry is complete.
    sl_id = place_stoploss_after_entry_complete(kite, setup, filled_qty)
    rt["sl_order_id"] = sl_id


def monitor_open_position(kite, setup: Setup) -> None:
    """
    After entry, keeps position protected and exits script when position is flat.
    """
    rt = state_for_symbol(setup.symbol)

    if not rt["entry_done"]:
        return

    qty = get_position_quantity(kite, setup.symbol)

    if qty == 0:
        cancel_stale_tagged_sl_if_no_position(kite, setup)
        request_stop(f"{setup.symbol}: position is flat; stoploss/manual exit likely completed")
        return

    actual_side = position_side_from_quantity(qty)

    if actual_side != setup.side:
        print(
            f"⚠ {setup.symbol}: position side changed to {actual_side}. "
            f"Expected {setup.side}. Manual check required."
        )
        request_stop(f"{setup.symbol}: unexpected opposite position")
        return

    ensure_stoploss_exists_for_open_position(kite, setup)

    if eod_warning_time_reached():
        print(
            f"⚠ {setup.symbol}: EOD warning time reached with open position. "
            f"Position is still protected by SL, but manual review is advised."
        )
        request_stop(f"{setup.symbol}: EOD warning with open position")
        return


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    setups = load_setups()

    print("Initializing Kite API...")
    kite = oUtils.intialize_kite_api()

    print("\nSimple entry + stoploss monitor started.")
    print(f"Exchange: {EXCHANGE}")
    print(f"Product: {PRODUCT}")
    print(f"Polling interval: {POLL_SECONDS} sec")
    print("Do not run multiple copies of this script.\n")

    for setup in setups.values():
        risk_per_share = abs(setup.entry - setup.stoploss)
        condition = ">=" if setup.side == "BUY" else "<="

        print(
            f"{setup.symbol}: side={setup.side}, entry={setup.entry}, "
            f"stoploss={setup.stoploss}, risk/share={risk_per_share:.2f}, "
            f"loss={setup.max_loss_rupees:.2f}, qty={setup.quantity}, "
            f"entry condition: LTP {condition} {setup.entry}"
        )

        # Restart safety: adopt open position if present.
        handle_existing_position_on_startup(kite, setup)

    print("Monitoring...\n")

    while not STOP_REQUESTED:
        try:
            for setup in setups.values():
                ltp = quote_ltp(kite, setup.symbol)

                place_entry_if_triggered(kite, setup, ltp)
                monitor_open_position(kite, setup)

                if STOP_REQUESTED:
                    break

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