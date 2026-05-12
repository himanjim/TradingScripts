"""
stock_trigger_monitor_restart_safe.py

Restart-safe live NSE equity trigger monitor using Zerodha Kite.

Main behaviour
--------------
1. Restarting the script starts fresh monitoring.
2. Old completed SL/entry/exit orders from earlier script runs are ignored.
3. If an open position exists on restart, the script adopts it and manages it.
4. If an active protective SL already exists, the script adopts that SL order ID.
5. If target / software exit-level / EOD is hit:
      - cancel active protective SL first;
      - refresh position;
      - place only one software exit if position still exists;
      - stop script after exit.
6. If broker-side hard SL fires:
      - detect completed SL for current session/adopted SL;
      - stop script.
7. Fast polling.
8. Soft SL-Limit:
      SELL position -> BUY SL:
          trigger = hard_sl
          price   = hard_sl + buffer

      BUY position -> SELL SL:
          trigger = hard_sl
          price   = hard_sl - buffer
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

# Format:
# "SYMBOL": [[entry_levels], "BUY/SELL", target, [software_exit_levels], hard_stoploss_trigger]
TRADE_SETUPS: Dict[str, List[Any]] = {
    "RELIANCE": [[1386.00], "SELL", 1384.00, [1388.00], 1388.00],
}

MAX_STOPLOSS_RUPEES = 10.0
MAX_QTY_CAP: Optional[int] = 500

EXCHANGE = "NSE"
PRODUCT = "MIS"
VALIDITY = "DAY"

POLL_SECONDS = 0.25
BROKER_SNAPSHOT_TTL_SECONDS = 0.75

ENABLE_ENTRY_TIME_WINDOW = True
ENTRY_START_TIME = dtime(9, 15)
ENTRY_END_TIME = dtime(15, 10)

ENABLE_EOD_EXIT = True
EOD_EXIT_TIME = dtime(15, 18)

# Keep False if you want fresh monitoring after restart.
BLOCK_IF_TAGGED_ENTRY_DONE_TODAY = False

# If True, entry is skipped if price has already crossed target.
SKIP_ENTRY_IF_TARGET_ALREADY_CROSSED = False

STOP_SCRIPT_AFTER_TRADE_DONE = True

TICK_SIZE = 0.05

ORDER_STATUS_POLL_SECONDS = 0.25
ORDER_STATUS_MAX_POLLS = 12

MARKET_PROTECTION = -1
ORDER_TAG = "STK_TRG_MON_SAFE"

SL_LIMIT_BUFFER_VALUE = 0.50
SL_PLACEMENT_GUARD_SECONDS = 20

PRINT_WAITING_STATUS_EVERY_SECONDS = 5.0


# =============================================================================
# SESSION STATE
# =============================================================================

SESSION_START_TS: Optional[datetime] = None
STOP_REQUESTED = False
STOP_REASON = ""


def request_stop(reason: str) -> None:
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
    entry_levels: List[float]
    side: str
    target: float
    exit_levels: List[float]
    hard_sl: float


@dataclass
class BrokerTruth:
    has_position: bool
    managed: bool
    side: Optional[str]
    quantity: int
    active_sl_order_ids: List[str]
    active_sl_qty: int
    position: Optional[Dict[str, Any]] = None
    unmanaged_reason: Optional[str] = None


# =============================================================================
# RUNTIME STATE — NOT PERSISTED
# =============================================================================

runtime: Dict[str, Dict[str, Any]] = {}


def state(symbol: str) -> Dict[str, Any]:
    if symbol not in runtime:
        runtime[symbol] = {
            "entry_trigger_minutes": {},
            "exit_trigger_minutes": {},
            "last_entry_reference": None,
            "entry_in_progress": False,
            "exit_in_progress": False,
            "recent_sl_order_id": None,
            "sl_guard_until": 0.0,
            "last_status_print": 0.0,
        }
    return runtime[symbol]


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
# BASIC HELPERS
# =============================================================================

def now_ist() -> datetime:
    return datetime.now(IST)


def round_to_tick(price: float) -> float:
    price = max(float(price), TICK_SIZE)
    return round(round(price / TICK_SIZE) * TICK_SIZE, 2)


def level_key(level: float) -> str:
    return f"{float(level):.2f}"


def to_ist_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None

    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except Exception:
            return None

    if dt.tzinfo is None:
        return IST.localize(dt)

    return dt.astimezone(IST)


def is_after_session_start(order: Dict[str, Any]) -> bool:
    """
    Returns True only if order belongs to this script session.
    Old completed orders from earlier script runs are ignored.
    """
    if SESSION_START_TS is None:
        return False

    dt = to_ist_datetime(
        order.get("exchange_timestamp")
        or order.get("order_timestamp")
        or order.get("exchange_update_timestamp")
    )

    return bool(dt and dt >= SESSION_START_TS)


def in_entry_window() -> bool:
    if not ENABLE_ENTRY_TIME_WINDOW:
        return True

    t = now_ist().time()
    return ENTRY_START_TIME <= t <= ENTRY_END_TIME


def is_eod_exit_time() -> bool:
    if not ENABLE_EOD_EXIT:
        return False

    return now_ist().time() >= EOD_EXIT_TIME


def side_from_quantity(qty: int) -> str:
    return "BUY" if qty > 0 else "SELL"


# =============================================================================
# SETUP PARSING
# =============================================================================

def parse_setup(symbol: str) -> Setup:
    raw = TRADE_SETUPS[symbol]

    if not isinstance(raw, list) or len(raw) != 5:
        raise ValueError(
            f"{symbol}: setup must be [[entry_levels], 'BUY/SELL', target, [exit_levels], hard_stoploss]"
        )

    entry_levels, side, target, exit_levels, hard_sl = raw

    if not isinstance(entry_levels, list) or not entry_levels:
        raise ValueError(f"{symbol}: entry_levels must be non-empty list")

    if not isinstance(exit_levels, list) or not exit_levels:
        raise ValueError(f"{symbol}: exit_levels must be non-empty list")

    side = str(side).upper().strip()

    if side not in {"BUY", "SELL"}:
        raise ValueError(f"{symbol}: side must be BUY or SELL")

    return Setup(
        symbol=symbol,
        entry_levels=[round_to_tick(float(x)) for x in entry_levels],
        side=side,
        target=round_to_tick(float(target)),
        exit_levels=[round_to_tick(float(x)) for x in exit_levels],
        hard_sl=round_to_tick(float(hard_sl)),
    )


def load_setups() -> Dict[str, Setup]:
    return {symbol: parse_setup(symbol) for symbol in TRADE_SETUPS}


# =============================================================================
# STRATEGY DIRECTION LOGIC
# =============================================================================

def entry_level_hit(setup: Setup, ltp: float, level: float) -> bool:
    if setup.side == "BUY":
        return float(ltp) >= float(level)
    return float(ltp) <= float(level)


def exit_level_hit(setup: Setup, ltp: float, level: float) -> bool:
    if setup.side == "BUY":
        return float(ltp) <= float(level)
    return float(ltp) >= float(level)


def target_hit(setup: Setup, ltp: float) -> bool:
    if setup.side == "BUY":
        return float(ltp) >= setup.target
    return float(ltp) <= setup.target


# =============================================================================
# KITE HELPERS
# =============================================================================

def entry_transaction(kite, side: str) -> str:
    return kite.TRANSACTION_TYPE_BUY if side == "BUY" else kite.TRANSACTION_TYPE_SELL


def exit_transaction(kite, side: str) -> str:
    return kite.TRANSACTION_TYPE_SELL if side == "BUY" else kite.TRANSACTION_TYPE_BUY


def quote_ltp(kite, symbol: str) -> float:
    key = f"{EXCHANGE}:{symbol}"
    return float(kite.quote(key)[key]["last_price"])


def protective_sl_prices(setup: Setup, position_side: str) -> Tuple[float, float]:
    trigger = round_to_tick(setup.hard_sl)

    if position_side == "BUY":
        limit_price = round_to_tick(trigger - SL_LIMIT_BUFFER_VALUE)
    else:
        limit_price = round_to_tick(trigger + SL_LIMIT_BUFFER_VALUE)

    return trigger, limit_price


def effective_sl_price_for_risk(setup: Setup) -> float:
    _, limit_price = protective_sl_prices(setup, setup.side)
    return limit_price


def validate_stoploss_direction(setup: Setup, entry_reference_price: float) -> bool:
    if setup.side == "BUY" and setup.hard_sl >= entry_reference_price:
        print(
            f"⛔ {setup.symbol}: BUY blocked. "
            f"hard_sl={setup.hard_sl} must be below entry_reference={entry_reference_price}"
        )
        return False

    if setup.side == "SELL" and setup.hard_sl <= entry_reference_price:
        print(
            f"⛔ {setup.symbol}: SELL blocked. "
            f"hard_sl={setup.hard_sl} must be above entry_reference={entry_reference_price}"
        )
        return False

    return True


def calculate_quantity(setup: Setup, entry_reference_price: float) -> int:
    effective_sl = effective_sl_price_for_risk(setup)
    risk_per_share = abs(float(entry_reference_price) - float(effective_sl))

    if risk_per_share <= 0:
        print(
            f"⛔ {setup.symbol}: invalid risk/share. "
            f"entry_ref={entry_reference_price}, effective_sl={effective_sl}"
        )
        return 0

    qty = int(MAX_STOPLOSS_RUPEES // risk_per_share)

    if qty <= 0:
        print(
            f"⛔ {setup.symbol}: calculated qty is 0. "
            f"MAX_STOPLOSS_RUPEES={MAX_STOPLOSS_RUPEES}, "
            f"risk/share={risk_per_share:.2f}"
        )
        return 0

    if MAX_QTY_CAP is not None and qty > MAX_QTY_CAP:
        print(f"{setup.symbol}: qty capped from {qty} to {MAX_QTY_CAP}")
        qty = MAX_QTY_CAP

    return qty


def marketable_limit_price(kite, symbol: str, transaction_type: str) -> float:
    key = f"{EXCHANGE}:{symbol}"
    q = kite.quote(key)[key]

    depth = q.get("depth", {})
    buy_depth = depth.get("buy", [])
    sell_depth = depth.get("sell", [])
    ltp = float(q.get("last_price") or 0.0)

    if transaction_type == kite.TRANSACTION_TYPE_BUY:
        price = float(sell_depth[0]["price"]) if sell_depth and sell_depth[0].get("price") else ltp * 1.005
    else:
        price = float(buy_depth[0]["price"]) if buy_depth and buy_depth[0].get("price") else ltp * 0.995

    return round_to_tick(price)


# =============================================================================
# ORDER / POSITION HELPERS
# =============================================================================

TERMINAL_STATUSES = {"COMPLETE", "REJECTED", "CANCELLED"}


def order_status(order: Dict[str, Any]) -> str:
    return str(order.get("status", "")).upper().strip()


def is_terminal(order: Dict[str, Any]) -> bool:
    return order_status(order) in TERMINAL_STATUSES


def order_trigger_price(order: Dict[str, Any]) -> float:
    try:
        return float(order.get("trigger_price") or 0.0)
    except Exception:
        return 0.0


def order_qty(order: Dict[str, Any]) -> int:
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


def find_position(positions: List[Dict[str, Any]], setup: Setup) -> Optional[Dict[str, Any]]:
    for p in positions:
        if p.get("tradingsymbol") != setup.symbol:
            continue
        if p.get("exchange") != EXCHANGE:
            continue
        if p.get("product") != PRODUCT:
            continue
        if int(p.get("quantity") or 0) != 0:
            return p
    return None


def is_protective_sl_order(kite, order: Dict[str, Any], setup: Setup, position_side: str) -> bool:
    expected_txn = exit_transaction(kite, position_side)

    return (
        order.get("tradingsymbol") == setup.symbol
        and order.get("exchange") == EXCHANGE
        and order.get("product") == PRODUCT
        and str(order.get("transaction_type")) == str(expected_txn)
        and not is_terminal(order)
        and order_trigger_price(order) > 0
    )


def is_completed_protective_sl_order(kite, order: Dict[str, Any], setup: Setup) -> bool:
    expected_txn = exit_transaction(kite, setup.side)

    return (
        order.get("tradingsymbol") == setup.symbol
        and order.get("exchange") == EXCHANGE
        and order.get("product") == PRODUCT
        and str(order.get("transaction_type")) == str(expected_txn)
        and order_status(order) == "COMPLETE"
        and order_trigger_price(order) > 0
    )


def derive_broker_truth(
    kite,
    setup: Setup,
    positions: List[Dict[str, Any]],
    orders: List[Dict[str, Any]],
) -> BrokerTruth:
    pos = find_position(positions, setup)

    if not pos:
        return BrokerTruth(
            has_position=False,
            managed=True,
            side=None,
            quantity=0,
            active_sl_order_ids=[],
            active_sl_qty=0,
        )

    net_qty = int(pos.get("quantity") or 0)
    actual_side = side_from_quantity(net_qty)
    qty_abs = abs(net_qty)

    if actual_side != setup.side:
        return BrokerTruth(
            has_position=True,
            managed=False,
            side=actual_side,
            quantity=qty_abs,
            active_sl_order_ids=[],
            active_sl_qty=0,
            position=pos,
            unmanaged_reason=(
                f"Existing position side={actual_side}, configured side={setup.side}. "
                f"Not managing it."
            ),
        )

    sl_orders = [
        order for order in orders
        if is_protective_sl_order(kite, order, setup, actual_side)
    ]

    return BrokerTruth(
        has_position=True,
        managed=True,
        side=actual_side,
        quantity=qty_abs,
        active_sl_order_ids=[str(o.get("order_id")) for o in sl_orders],
        active_sl_qty=sum(order_qty(o) for o in sl_orders),
        position=pos,
    )


def adopt_active_sl_if_any(kite, setup: Setup, truth: BrokerTruth) -> None:
    """
    On restart, if an open position already has an active SL, adopt its order ID.
    This allows later hard-SL completion detection without treating old completed SLs as current.
    """
    if not truth.has_position or not truth.managed:
        return

    if not truth.active_sl_order_ids:
        return

    st = state(setup.symbol)

    if not st.get("recent_sl_order_id"):
        st["recent_sl_order_id"] = truth.active_sl_order_ids[-1]
        print(f"{setup.symbol}: adopted existing active SL order {st['recent_sl_order_id']}")


# =============================================================================
# ORDER PLACEMENT
# =============================================================================

def ensure_filled_or_convert_to_market(kite, order_id: str, symbol: str) -> Tuple[bool, Optional[float]]:
    converted = False

    for _ in range(ORDER_STATUS_MAX_POLLS):
        time.sleep(ORDER_STATUS_POLL_SECONDS)

        order = get_order_by_id(kite.orders(), order_id)

        if not order:
            continue

        st = order_status(order)
        pending_qty = int(order.get("pending_quantity") or 0)

        if st == "COMPLETE" and pending_qty == 0:
            invalidate_broker_cache()
            return True, average_price(order)

        if st in {"REJECTED", "CANCELLED"}:
            invalidate_broker_cache()
            print(f"❌ {symbol}: order {st}")
            return False, average_price(order)

        if pending_qty > 0 and not converted:
            kite.modify_order(
                variety=kite.VARIETY_REGULAR,
                order_id=order_id,
                order_type=kite.ORDER_TYPE_MARKET,
                market_protection=MARKET_PROTECTION,
            )
            converted = True
            print(f"{symbol}: pending LIMIT converted to MARKET")

    final_order = get_order_by_id(kite.orders(), order_id)
    invalidate_broker_cache()

    if final_order and order_status(final_order) == "COMPLETE":
        return True, average_price(final_order)

    return False, average_price(final_order)


def place_entry_or_exit_order(
    kite,
    setup: Setup,
    transaction_type: str,
    quantity: int,
) -> Tuple[bool, Optional[float]]:
    price = marketable_limit_price(kite, setup.symbol, transaction_type)

    print(
        f"Placing {transaction_type} LIMIT: {EXCHANGE}:{setup.symbol}, "
        f"qty={quantity}, price={price}, product={PRODUCT}"
    )

    order_id = kite.place_order(
        tradingsymbol=setup.symbol,
        variety=kite.VARIETY_REGULAR,
        exchange=EXCHANGE,
        transaction_type=transaction_type,
        quantity=quantity,
        order_type=kite.ORDER_TYPE_LIMIT,
        price=price,
        product=PRODUCT,
        validity=VALIDITY,
        tag=ORDER_TAG,
    )

    invalidate_broker_cache()
    return ensure_filled_or_convert_to_market(kite, order_id, setup.symbol)


def recent_sl_blocks_new_sl(orders: List[Dict[str, Any]], setup: Setup) -> bool:
    st = state(setup.symbol)
    recent_id = st.get("recent_sl_order_id")

    if not recent_id:
        return False

    order = get_order_by_id(orders, str(recent_id))

    if order:
        return order_status(order) not in {"REJECTED", "CANCELLED", "COMPLETE"}

    return time.time() < float(st.get("sl_guard_until") or 0.0)


def place_protective_sl(kite, setup: Setup, position_side: str, quantity: int) -> Optional[str]:
    _, orders = broker_snapshot(kite, force=True)

    if recent_sl_blocks_new_sl(orders, setup):
        return state(setup.symbol).get("recent_sl_order_id")

    sl_txn = exit_transaction(kite, position_side)
    trigger_price, limit_price = protective_sl_prices(setup, position_side)

    print(
        f"Placing SOFT SL LIMIT: {EXCHANGE}:{setup.symbol}, txn={sl_txn}, "
        f"qty={quantity}, trigger={trigger_price}, price={limit_price}, product={PRODUCT}"
    )

    order_id = kite.place_order(
        tradingsymbol=setup.symbol,
        variety=kite.VARIETY_REGULAR,
        exchange=EXCHANGE,
        transaction_type=sl_txn,
        quantity=quantity,
        order_type=getattr(kite, "ORDER_TYPE_SL", "SL"),
        price=limit_price,
        trigger_price=trigger_price,
        product=PRODUCT,
        validity=VALIDITY,
        tag=ORDER_TAG,
    )

    st = state(setup.symbol)
    st["recent_sl_order_id"] = str(order_id)
    st["sl_guard_until"] = time.time() + SL_PLACEMENT_GUARD_SECONDS

    invalidate_broker_cache()
    return str(order_id)


def cancel_order_safely(kite, order_id: str) -> bool:
    try:
        kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=str(order_id))
        invalidate_broker_cache()
        return True
    except Exception as e:
        print(f"WARNING: could not cancel order {order_id}: {e}")
        return False


def cancel_active_protective_sl_orders(kite, setup: Setup, position_side: str) -> bool:
    _, orders = broker_snapshot(kite, force=True)

    active_sl_orders = [
        order for order in orders
        if is_protective_sl_order(kite, order, setup, position_side)
    ]

    if not active_sl_orders:
        return True

    for order in active_sl_orders:
        oid = str(order.get("order_id"))

        if cancel_order_safely(kite, oid):
            print(f"{setup.symbol}: cancelled protective SL before software exit: {oid}")

    _, fresh_orders = broker_snapshot(kite, force=True)

    remaining = [
        order for order in fresh_orders
        if is_protective_sl_order(kite, order, setup, position_side)
    ]

    if remaining:
        print(
            f"⚠ {setup.symbol}: protective SL still active after cancel attempt. "
            f"Software exit aborted to avoid double exit."
        )
        return False

    return True


def ensure_protective_sl_exists(kite, setup: Setup) -> None:
    positions, orders = broker_snapshot(kite)
    truth = derive_broker_truth(kite, setup, positions, orders)

    if not truth.has_position:
        return

    if not truth.managed:
        print(f"{setup.symbol}: unmanaged position. {truth.unmanaged_reason}")
        return

    adopt_active_sl_if_any(kite, setup, truth)

    if truth.active_sl_qty >= truth.quantity:
        return

    if recent_sl_blocks_new_sl(orders, setup):
        return

    deficit = truth.quantity - truth.active_sl_qty

    print(
        f"{setup.symbol}: protective SL missing/short. "
        f"position_qty={truth.quantity}, active_sl_qty={truth.active_sl_qty}, "
        f"placing_deficit={deficit}"
    )

    place_protective_sl(kite, setup, truth.side, deficit)


def cancel_stale_tagged_sl_orders_if_flat(kite, setup: Setup) -> None:
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
        if is_terminal(order):
            continue
        if order_trigger_price(order) <= 0:
            continue

        oid = str(order.get("order_id"))

        if cancel_order_safely(kite, oid):
            print(f"{setup.symbol}: cancelled stale SL order after flat position: {oid}")


def hard_sl_completed_current_session(kite, setup: Setup, orders: List[Dict[str, Any]]) -> bool:
    """
    Detects hard SL completion only for:
    1. the current run's recent/adopted SL order; OR
    2. tagged protective SL orders placed after this script started.

    Old completed SL orders from earlier runs are ignored.
    """
    recent_id = state(setup.symbol).get("recent_sl_order_id")

    if recent_id:
        recent_order = get_order_by_id(orders, str(recent_id))

        if recent_order and is_completed_protective_sl_order(kite, recent_order, setup):
            return True

    for order in orders:
        if str(order.get("tag", "")) != ORDER_TAG:
            continue

        if not is_after_session_start(order):
            continue

        if is_completed_protective_sl_order(kite, order, setup):
            return True

    return False


# =============================================================================
# ENTRY / EXIT TRIGGER STATE
# =============================================================================

def apply_entry_trigger_from_ltp(setup: Setup, ltp: float, minute_id: str) -> None:
    st = state(setup.symbol)
    triggered = st["entry_trigger_minutes"]
    used_minutes = set(triggered.values())

    for level in setup.entry_levels:
        key = level_key(level)

        if key in triggered:
            continue

        if entry_level_hit(setup, ltp, level):
            if minute_id in used_minutes:
                return

            triggered[key] = minute_id
            st["last_entry_reference"] = level

            print(f"{setup.symbol}: entry trigger {level} confirmed by LTP={ltp} at {minute_id}")
            return


def apply_exit_trigger_from_ltp(setup: Setup, ltp: float, minute_id: str) -> None:
    triggered = state(setup.symbol)["exit_trigger_minutes"]

    for level in setup.exit_levels:
        key = level_key(level)

        if key in triggered:
            continue

        if exit_level_hit(setup, ltp, level):
            triggered[key] = minute_id
            print(f"{setup.symbol}: software exit level {level} confirmed by LTP={ltp} at {minute_id}")


def entry_triggers_done(setup: Setup) -> bool:
    triggered = state(setup.symbol)["entry_trigger_minutes"]
    return all(level_key(x) in triggered for x in setup.entry_levels)


def exit_triggers_done(setup: Setup) -> bool:
    triggered = state(setup.symbol)["exit_trigger_minutes"]
    return all(level_key(x) in triggered for x in setup.exit_levels)


# =============================================================================
# TRADE ACTIONS
# =============================================================================

def place_entry(kite, setup: Setup) -> None:
    st = state(setup.symbol)

    if st["entry_in_progress"]:
        return

    st["entry_in_progress"] = True

    try:
        positions, orders = broker_snapshot(kite, force=True)
        truth = derive_broker_truth(kite, setup, positions, orders)

        if truth.has_position:
            print(f"{setup.symbol}: position already exists. Entry skipped.")
            return

        live_ltp = quote_ltp(kite, setup.symbol)

        if SKIP_ENTRY_IF_TARGET_ALREADY_CROSSED and target_hit(setup, live_ltp):
            print(
                f"{setup.symbol}: entry skipped because target is already crossed. "
                f"ltp={live_ltp}, target={setup.target}"
            )
            return

        entry_reference = st.get("last_entry_reference") or setup.entry_levels[-1]

        if not validate_stoploss_direction(setup, entry_reference):
            return

        qty = calculate_quantity(setup, entry_reference)

        if qty <= 0:
            return

        txn = entry_transaction(kite, setup.side)
        effective_sl = effective_sl_price_for_risk(setup)

        print(
            f"{setup.symbol}: placing {setup.side} entry. "
            f"live_ltp={live_ltp}, qty_ref={entry_reference}, "
            f"target={setup.target}, hard_sl_trigger={setup.hard_sl}, "
            f"soft_sl_limit={effective_sl}, qty={qty}, "
            f"configured_risk≈{abs(entry_reference - effective_sl) * qty:.2f}"
        )

        ok, avg = place_entry_or_exit_order(kite, setup, txn, qty)

        if not ok:
            print(f"{setup.symbol}: entry order unresolved/failed.")
            return

        place_protective_sl(kite, setup, setup.side, qty)
        st["exit_trigger_minutes"] = {}

        print(
            f"{setup.symbol}: entry complete. "
            f"avg_price={avg}, target={setup.target}, hard_sl_trigger={setup.hard_sl}"
        )

    finally:
        st["entry_in_progress"] = False


def exit_position(kite, setup: Setup, reason: str) -> None:
    """
    Software exit path.

    It cancels broker protective SL before placing software exit order to prevent double exit.
    """
    st = state(setup.symbol)

    if st["exit_in_progress"]:
        return

    st["exit_in_progress"] = True

    try:
        positions, orders = broker_snapshot(kite, force=True)
        truth = derive_broker_truth(kite, setup, positions, orders)

        if not truth.has_position:
            cancel_stale_tagged_sl_orders_if_flat(kite, setup)

            if hard_sl_completed_current_session(kite, setup, orders):
                request_stop(f"{setup.symbol}: broker hard SL completed")

            return

        if not truth.managed:
            print(f"{setup.symbol}: unmanaged position. {truth.unmanaged_reason}")
            request_stop(f"{setup.symbol}: unmanaged position detected")
            return

        cancelled = cancel_active_protective_sl_orders(kite, setup, truth.side)

        positions, orders = broker_snapshot(kite, force=True)
        truth = derive_broker_truth(kite, setup, positions, orders)

        if not truth.has_position:
            cancel_stale_tagged_sl_orders_if_flat(kite, setup)

            if hard_sl_completed_current_session(kite, setup, orders):
                request_stop(f"{setup.symbol}: broker hard SL completed during software-exit attempt")
            else:
                request_stop(f"{setup.symbol}: position already flat during software-exit attempt")

            return

        if not cancelled or truth.active_sl_qty > 0:
            print(
                f"⚠ {setup.symbol}: software exit aborted because protective SL is still active. "
                f"This avoids duplicate exit orders."
            )
            return

        txn = exit_transaction(kite, truth.side)

        print(f"{setup.symbol}: software exit. reason={reason}, qty={truth.quantity}")

        ok, avg = place_entry_or_exit_order(kite, setup, txn, truth.quantity)

        if not ok:
            print(f"{setup.symbol}: software exit order unresolved/failed.")
            return

        cancel_stale_tagged_sl_orders_if_flat(kite, setup)

        st["entry_trigger_minutes"] = {}
        st["exit_trigger_minutes"] = {}
        st["last_entry_reference"] = None
        st["recent_sl_order_id"] = None
        st["sl_guard_until"] = 0.0

        print(f"{setup.symbol}: exited. reason={reason}, exit_price={avg}")

        if STOP_SCRIPT_AFTER_TRADE_DONE:
            request_stop(f"{setup.symbol}: trade completed by {reason}")

    finally:
        st["exit_in_progress"] = False


# =============================================================================
# MAIN STRATEGY PROCESSING
# =============================================================================

def print_waiting_status(setup: Setup, ltp: float, reason: str = "") -> None:
    st = state(setup.symbol)
    now = time.monotonic()

    if now - float(st.get("last_status_print") or 0.0) < PRINT_WAITING_STATUS_EVERY_SECONDS:
        return

    st["last_status_print"] = now

    triggered = st["entry_trigger_minutes"]
    pending = [x for x in setup.entry_levels if level_key(x) not in triggered]

    print(
        f"{setup.symbol}: LTP={ltp}, side={setup.side}, "
        f"pending_entry_levels={pending}, target={setup.target}, hard_sl={setup.hard_sl}"
        + (f", reason={reason}" if reason else "")
    )


def tagged_entry_done_after_session_start(orders: List[Dict[str, Any]], kite, setup: Setup) -> bool:
    """
    Optional same-session entry blocker.

    Old entries before restart are ignored.
    """
    entry_txn = entry_transaction(kite, setup.side)

    for order in orders:
        if order.get("tradingsymbol") != setup.symbol:
            continue
        if order.get("exchange") != EXCHANGE:
            continue
        if order.get("product") != PRODUCT:
            continue
        if str(order.get("tag", "")) != ORDER_TAG:
            continue
        if str(order.get("transaction_type")) != str(entry_txn):
            continue
        if order_status(order) != "COMPLETE":
            continue
        if is_after_session_start(order):
            return True

    return False


def process_symbol(kite, setup: Setup, ltp: float, minute_id: str) -> None:
    positions, orders = broker_snapshot(kite)
    truth = derive_broker_truth(kite, setup, positions, orders)

    # No open position: fresh monitoring is allowed after restart.
    if not truth.has_position:
        cancel_stale_tagged_sl_orders_if_flat(kite, setup)

        if hard_sl_completed_current_session(kite, setup, orders):
            request_stop(f"{setup.symbol}: broker hard SL completed")
            return

        if BLOCK_IF_TAGGED_ENTRY_DONE_TODAY and tagged_entry_done_after_session_start(orders, kite, setup):
            print_waiting_status(setup, ltp, "blocked because tagged entry completed in current session")
            return

        if not in_entry_window():
            print_waiting_status(setup, ltp, "outside entry window")
            return

        apply_entry_trigger_from_ltp(setup, ltp, minute_id)

        if entry_triggers_done(setup):
            place_entry(kite, setup)
        else:
            print_waiting_status(setup, ltp)

        return

    # Open position exists but it is not the configured side.
    if not truth.managed:
        print(f"{setup.symbol}: unmanaged position. {truth.unmanaged_reason}")
        request_stop(f"{setup.symbol}: unmanaged position detected")
        return

    adopt_active_sl_if_any(kite, setup, truth)

    # Target first.
    if target_hit(setup, ltp):
        print(f"{setup.symbol}: target hit. ltp={ltp}, target={setup.target}")
        exit_position(kite, setup, "TARGET")
        return

    # Software exit levels.
    apply_exit_trigger_from_ltp(setup, ltp, minute_id)

    if exit_triggers_done(setup):
        exit_position(kite, setup, "EXIT_LEVELS")
        return

    # EOD.
    if is_eod_exit_time():
        exit_position(kite, setup, "EOD_EXIT")
        return

    # Hard SL protection.
    ensure_protective_sl_exists(kite, setup)


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    global SESSION_START_TS

    SESSION_START_TS = now_ist()

    setups = load_setups()

    print("Initializing Kite API...")
    kite = oUtils.intialize_kite_api()

    print("\nMonitor started.")
    print(f"Session start: {SESSION_START_TS}")
    print(f"Symbols: {', '.join(setups.keys())}")
    print(f"Polling interval: {POLL_SECONDS} sec")
    print(f"Fixed max stop-loss per trade: ₹{MAX_STOPLOSS_RUPEES}")
    print(f"Soft SL buffer: ₹{SL_LIMIT_BUFFER_VALUE}")
    print("Old completed orders before this script start will be ignored.")
    print("Do not run multiple copies of this script.\n")

    while not STOP_REQUESTED:
        try:
            ts = now_ist()
            minute_id = ts.replace(second=0, microsecond=0).isoformat()

            for setup in setups.values():
                ltp = quote_ltp(kite, setup.symbol)
                process_symbol(kite, setup, ltp, minute_id)

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