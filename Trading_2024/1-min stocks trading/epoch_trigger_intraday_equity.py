# epoch_trigger_intraday_equity_multi.py
# ============================================================
# Multi-stock, multi-epoch intraday trigger (1-min candle constraint)
# ============================================================
# INPUT (dict-like strings; parsed safely via ast.literal_eval):
#   {'stock': 'HDFCBANK', 'epoch': [1551.10, 1547.00, 1551.10], 'stoploss': 3.93, 'target': 1568.40}
#
# CORE IDEA (same as your older single-stock epoch engine):
# - For each stock, 'epoch' is a LIST of price levels to be crossed IN ORDER.
# - For each level, direction is armed automatically:
#       if current_price < level -> wait for UP cross
#       else                     -> wait for DOWN cross
# - A level is considered "hit" only if the cross happens in a DIFFERENT 1-min IST candle
#   than the previous hit. (Prevents multiple steps in same candle.)
# - When the final level is hit, the trade is executed and the script exits.
#
# TRADE DIRECTION:
# - Determined by the LAST epoch hit direction:
#       last hit "UP"   => BUY
#       last hit "DOWN" => SELL
#
# STOPLOSS FIELD:
# - stoploss is interpreted as "points" away from FINAL epoch level:
#       BUY  => SL = final_epoch - stoploss_points
#       SELL => SL = final_epoch + stoploss_points
#
# QTY SIZING:
# - Risk per trade is ABS_RISK (₹). With stoploss_points as points:
#       qty = floor(ABS_RISK / stoploss_points)
# - Clamped to [1, MAX_QTY]
#
# ORDERING:
# - ENTRY is SMART LIMIT (uses best bid/ask if available; falls back to LTP)
# - If spread is wide (> MAX_SPREAD_TICKS), automatically switches to JOIN to avoid paying spread.
# - Wait ENTRY_WAIT_SEC seconds for fill; if not filled => cancel entry + cancel other open TAG orders; exit.
# - On fill => place SL-M (trigger rounded to tick conservatively) + optional TARGET LIMIT.
#
# ROBUSTNESS:
# - Retries quote()/orders()/positions() every RETRY_SEC seconds if Kite API temporarily fails.
# - Uses ONE kite.quote([...]) call per polling loop for all symbols (efficient).
# - Clears open TAG orders + squares-off open TAG positions before placing new entry.
#
# NOTES:
# - Tick size is assumed constant (TICK_SIZE). You requested NOT to vary per stock.
# - If you see tick-size exceptions, set TICK_SIZE appropriately for the universe you trade.

import time
import math
import datetime as dt
import pytz
import ast
from typing import Dict, Any, List, Optional, Tuple

import Trading_2024.OptionTradeUtils as oUtils


# ============================================================
# USER CONFIG
# ============================================================
STRATEGY_STRS = [
    "{'stock': 'HDFCBANK', 'epoch': [1551.10, 1547.00, 1551.10], 'stoploss': 3.93, 'target': 1568.40}",
    # "{'stock': 'ICICIBANK', 'epoch': [1120.0, 1115.0, 1120.0], 'stoploss': 3.0, 'target': 1132.0}",
]

EXCHANGE = "NSE"
PRODUCT = "MIS"
PAPER = False
TAG = oUtils.STOCK_INTRADAY_TAG
ONLY_TODAY_TAGGED = True

ABS_RISK = 3000.0
MAX_QTY = 5000

POLL_SEC = 1
RETRY_SEC = 2

TICK_SIZE = 0.10
ENTRY_WAIT_SEC = 10

SMART_LIMIT_MODE = "CROSS"     # baseline: "CROSS" or "JOIN"
SLIPPAGE_TICKS = 0             # extra ticks in fill-favoring direction (0/1)
MAX_SPREAD_TICKS = 2           # spread > this => auto JOIN

INDIA_TZ = pytz.timezone("Asia/Kolkata")


# ============================================================
# SOUND
# ============================================================
def beep():
    """Beep on Windows (winsound) else terminal bell."""
    try:
        import winsound
        winsound.Beep(1200, 300)
    except Exception:
        print("\a", end="", flush=True)


# ============================================================
# PRICE/TICK HELPERS
# ============================================================
def round_to_tick(price: float, tick: float = TICK_SIZE) -> float:
    """Round to nearest tick."""
    return round(round(price / tick) * tick, 2)


def round_to_tick_dir(price: float, tick: float, direction: str) -> float:
    """
    Round to tick in a specific direction:
      direction='DOWN' => floor to tick
      direction='UP'   => ceil to tick
    Used to ensure SL trigger is valid & conservative.
    """
    x = price / tick
    v = math.floor(x) if direction == "DOWN" else math.ceil(x)
    return round(v * tick, 2)


# ============================================================
# TIME / CANDLE HELPERS
# ============================================================
def candle_id_ist(now_utc: dt.datetime) -> str:
    """Return 1-minute candle ID in IST, e.g., '2026-02-04 09:16'."""
    return now_utc.astimezone(INDIA_TZ).strftime("%Y-%m-%d %H:%M")


# ============================================================
# EPOCH LOGIC (same spirit as your older script)
# ============================================================
def arm_dir(cur: float, lvl: float) -> str:
    """Arm direction for an epoch level based on current price."""
    return "UP" if cur < lvl else "DOWN"


def crossed(prev: float, cur: float, lvl: float, d: str) -> bool:
    """True if (prev -> cur) crosses lvl in direction d."""
    return (prev < lvl <= cur) if d == "UP" else (prev > lvl >= cur)


def run_epochs(
    prev_val: float,
    cur_val: float,
    idx: int,
    armed_lvl: float,
    armed_dir: str,
    last_hit_candle: Optional[str],
    cur_candle: str,
    levels: List[float],
) -> Tuple[int, float, str, Optional[str], Optional[str], bool]:
    """
    Attempt to progress an epoch sequence.

    Enforces 1-min candle separation:
    - A hit is valid only if cur_candle != last_hit_candle

    Returns:
      (new_idx, new_armed_lvl, new_armed_dir, new_last_hit_candle, hit_dir, done)
    """
    hit_dir = None
    done = False

    if crossed(prev_val, cur_val, armed_lvl, armed_dir):
        # enforce "different 1-min candle" rule
        if last_hit_candle is None or cur_candle != last_hit_candle:
            hit_dir = armed_dir
            last_hit_candle = cur_candle
            idx += 1

            if idx >= len(levels):
                done = True
            else:
                armed_lvl = float(levels[idx])
                armed_dir = arm_dir(cur_val, armed_lvl)

    return idx, armed_lvl, armed_dir, last_hit_candle, hit_dir, done


# ============================================================
# STRATEGY PARSING
# ============================================================
def parse_strategy_strings(strategy_strs: List[str]) -> List[Dict[str, Any]]:
    """Parse dict-like strings safely; validate required keys and epoch list."""
    out = []
    for s in strategy_strs:
        d = ast.literal_eval(s.strip())
        if not isinstance(d, dict):
            raise ValueError(f"Strategy must be dict-like: {s}")
        for k in ("stock", "epoch", "stoploss"):
            if k not in d:
                raise ValueError(f"Missing '{k}' in: {s}")
        if not isinstance(d["epoch"], list) or len(d["epoch"]) == 0:
            raise ValueError(f"'epoch' must be a non-empty list in: {s}")
        out.append(d)
    return out


def to_kite_symbol(stock_field: str) -> str:
    """Convert 'HDFCBANK' -> 'NSE:HDFCBANK' unless already 'EXCH:SYM'."""
    sf = str(stock_field).strip().upper()
    return sf if ":" in sf else f"{EXCHANGE}:{sf}"


# ============================================================
# KITE API RETRY WRAPPERS
# ============================================================
def safe_quote(kite, symbols: List[str]) -> Dict[str, Any]:
    """kitesdk quote() with retry."""
    while True:
        try:
            return kite.quote(symbols)
        except Exception as e:
            print(f"[WARN] quote() failed: {e}. Retrying in {RETRY_SEC}s...")
            time.sleep(RETRY_SEC)


def safe_orders(kite) -> list:
    """kitesdk orders() with retry."""
    while True:
        try:
            return kite.orders()
        except Exception as e:
            print(f"[WARN] orders() failed: {e}. Retrying in {RETRY_SEC}s...")
            time.sleep(RETRY_SEC)


def safe_positions(kite) -> dict:
    """kitesdk positions() with retry."""
    while True:
        try:
            return kite.positions()
        except Exception as e:
            print(f"[WARN] positions() failed: {e}. Retrying in {RETRY_SEC}s...")
            time.sleep(RETRY_SEC)


# ============================================================
# SMART LIMIT PRICING
# ============================================================
def best_bid_ask(q_item: dict) -> Tuple[Optional[float], Optional[float]]:
    """Extract best bid/ask from quote depth if present."""
    try:
        depth = q_item.get("depth") or {}
        bids = depth.get("buy") or []
        asks = depth.get("sell") or []
        best_bid = float(bids[0]["price"]) if bids else None
        best_ask = float(asks[0]["price"]) if asks else None
        return best_bid, best_ask
    except Exception:
        return None, None


def smart_limit_price_from_quote(q_item: dict, side: str, kite) -> float:
    """
    Choose a LIMIT price using depth if possible.
    - baseline mode from SMART_LIMIT_MODE
    - if spread too wide, auto JOIN (avoid paying wide spread)
    - apply SLIPPAGE_TICKS in fill-favoring direction
    """
    lp = q_item.get("last_price")
    if lp is None:
        # should be rare; caller already guards, but keep safe.
        raise ValueError("Quote missing last_price")

    ltp = float(lp)
    bid, ask = best_bid_ask(q_item)

    mode = SMART_LIMIT_MODE.upper().strip()

    # Spread sanity: if spread is wide, prefer JOIN
    if bid is not None and ask is not None:
        spread = ask - bid
        if spread > MAX_SPREAD_TICKS * TICK_SIZE:
            mode = "JOIN"

    # Choose base level for entry price
    if bid is None and ask is None:
        base = ltp
    else:
        if mode == "JOIN":
            # Better price (may miss fills)
            base = (bid if bid is not None else ltp) if side == kite.TRANSACTION_TYPE_BUY else (ask if ask is not None else ltp)
        else:
            # Higher fill probability
            base = (ask if ask is not None else ltp) if side == kite.TRANSACTION_TYPE_BUY else (bid if bid is not None else ltp)

    # Optional slippage ticks to be more aggressive
    slip = float(SLIPPAGE_TICKS) * float(TICK_SIZE)
    px = base + slip if side == kite.TRANSACTION_TYPE_BUY else base - slip

    return round_to_tick(px, TICK_SIZE)


# ============================================================
# TAG CLEANUP (cancel open orders, square-off tagged positions)
# ============================================================
def is_today_ist(order_ts: str) -> bool:
    """Restrict tagging to today to avoid touching old positions/orders."""
    try:
        d = order_ts.split(" ")[0]
        today = dt.datetime.now(INDIA_TZ).strftime("%Y-%m-%d")
        return d == today
    except Exception:
        return True


def tagged_symbols_and_open_orders(kite) -> Tuple[set, List[str]]:
    """Return (tagged_symbols_set, open_order_ids) for this TAG."""
    syms, open_oids = set(), []
    for o in safe_orders(kite):
        if o.get("tag") != TAG:
            continue
        if ONLY_TODAY_TAGGED and not is_today_ist(str(o.get("order_timestamp", ""))):
            continue
        syms.add(f'{o["exchange"]}:{o["tradingsymbol"]}')
        if str(o.get("status", "")).upper() in (
            "OPEN",
            "TRIGGER PENDING",
            "VALIDATION PENDING",
            "PUT ORDER REQ RECEIVED",
            "PARTIAL",
        ):
            open_oids.append(o["order_id"])
    return syms, open_oids


def cancel_open_tag_orders(kite):
    """Cancel all open/pending orders with TAG."""
    if PAPER:
        print("[PAPER] Would cancel OPEN/TRIGGER-PENDING TAG orders.")
        return

    _, open_oids = tagged_symbols_and_open_orders(kite)
    for oid in open_oids:
        try:
            kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=oid)
            print(f"[LIVE] Cancelled tagged order: {oid}")
        except Exception as e:
            print(f"[WARN] Could not cancel {oid}: {e}")


def squareoff_tag_positions(kite):
    """
    Best-effort square-off:
    Kite positions do not store tag; so we infer which symbols belong to TAG by looking at TAG orders.
    """
    syms, _ = tagged_symbols_and_open_orders(kite)
    if not syms:
        print(f"[INFO] No tagged symbols found for TAG={TAG}.")
        return

    if PAPER:
        print(f"[PAPER] Would square-off open {PRODUCT} positions for TAG symbols={sorted(syms)}")
        return

    net = safe_positions(kite).get("net", [])
    for p in net:
        sym = f'{p["exchange"]}:{p["tradingsymbol"]}'
        if sym not in syms or p.get("product") != PRODUCT:
            continue

        netq = int(p.get("quantity") or 0)
        if netq == 0:
            continue

        side = kite.TRANSACTION_TYPE_SELL if netq > 0 else kite.TRANSACTION_TYPE_BUY
        try:
            kite.place_order(
                variety=kite.VARIETY_REGULAR,
                exchange=p["exchange"],
                tradingsymbol=p["tradingsymbol"],
                transaction_type=side,
                quantity=abs(netq),
                product=PRODUCT,
                order_type=kite.ORDER_TYPE_MARKET,
                validity=kite.VALIDITY_DAY,
                tag=TAG,
            )
            print(f"[LIVE] Square-off {sym} net_qty={netq} -> {side} {abs(netq)}")
        except Exception as e:
            print(f"[WARN] Square-off failed for {sym}: {e}")


def get_order_status(kite, order_id: str) -> str:
    """Fetch status for a given order_id (retry-safe)."""
    if PAPER:
        return "COMPLETE"
    for o in safe_orders(kite):
        if o.get("order_id") == order_id:
            return str(o.get("status", "UNKNOWN")).upper()
    return "UNKNOWN"


# ============================================================
# ORDER PLACEMENT
# ============================================================
def qty_from_stoploss_pts(stoploss_pts: float) -> int:
    """Risk sizing based on points to SL."""
    if stoploss_pts <= 0:
        raise ValueError(f"stoploss must be > 0, got {stoploss_pts}")
    q = int(math.floor(ABS_RISK / stoploss_pts))
    return max(1, min(MAX_QTY, q))


def place_entry_sl_target(
    kite,
    kite_symbol: str,
    side: str,
    final_epoch: float,
    stoploss_pts: float,
    target: Optional[float],
    entry_quote_item: dict,
):
    """
    Place entry LIMIT (smart), then SL-M, then optional target LIMIT.
    Exits early if entry not filled within ENTRY_WAIT_SEC.
    """
    exch, tsym = kite_symbol.split(":")
    exit_side = kite.TRANSACTION_TYPE_SELL if side == kite.TRANSACTION_TYPE_BUY else kite.TRANSACTION_TYPE_BUY

    # SL absolute from FINAL epoch +/- stoploss_pts
    sl_price = (final_epoch - stoploss_pts) if side == kite.TRANSACTION_TYPE_BUY else (final_epoch + stoploss_pts)

    # SL trigger rounding:
    # long SL is SELL => round DOWN; short SL is BUY => round UP
    trig_dir = "DOWN" if exit_side == kite.TRANSACTION_TYPE_SELL else "UP"
    sl_trig = round_to_tick_dir(sl_price, TICK_SIZE, trig_dir)

    qty = qty_from_stoploss_pts(stoploss_pts)

    # Smart LIMIT entry price
    entry_px = smart_limit_price_from_quote(entry_quote_item, side, kite)

    if PAPER:
        print(f"[PAPER] ENTRY {kite_symbol} {side} qty={qty} limit={entry_px} | SL_trig={sl_trig} | target={target}")
        beep()
        return

    # -------------------------
    # ENTRY LIMIT
    # -------------------------
    entry_oid = kite.place_order(
        variety=kite.VARIETY_REGULAR,
        exchange=exch,
        tradingsymbol=tsym,
        transaction_type=side,
        quantity=qty,
        product=PRODUCT,
        order_type=kite.ORDER_TYPE_LIMIT,
        price=entry_px,
        validity=kite.VALIDITY_DAY,
        tag=TAG,
    )
    print(f"[LIVE] Entry LIMIT placed {kite_symbol} @ {entry_px}, oid={entry_oid}. Waiting {ENTRY_WAIT_SEC}s...")

    # Wait for fill
    deadline = time.time() + ENTRY_WAIT_SEC
    filled = False
    while time.time() < deadline:
        st = get_order_status(kite, entry_oid)
        if st == "COMPLETE":
            filled = True
            break
        if st in ("CANCELLED", "REJECTED"):
            break
        time.sleep(1)

    # If not filled: cancel entry + cancel other open tagged orders and stop.
    if not filled:
        st = get_order_status(kite, entry_oid)
        if st in ("OPEN", "TRIGGER PENDING", "PARTIAL", "VALIDATION PENDING", "PUT ORDER REQ RECEIVED"):
            try:
                kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=entry_oid)
                print(f"[LIVE] Entry not filled; cancelled oid={entry_oid}")
            except Exception as e:
                print(f"[WARN] cancel entry failed {entry_oid}: {e}")
        cancel_open_tag_orders(kite)
        print("[EXIT] Entry not filled in time. Exiting without SL/Target.")
        return

    # -------------------------
    # SL-M after entry fill
    # -------------------------
    sl_oid = kite.place_order(
        variety=kite.VARIETY_REGULAR,
        exchange=exch,
        tradingsymbol=tsym,
        transaction_type=exit_side,
        quantity=qty,
        product=PRODUCT,
        order_type=kite.ORDER_TYPE_SLM,
        trigger_price=sl_trig,
        validity=kite.VALIDITY_DAY,
        tag=TAG,
    )
    print(f"[LIVE] SL-M placed trigger={sl_trig}, oid={sl_oid}")

    # -------------------------
    # Optional TARGET LIMIT
    # -------------------------
    tgt_oid = None
    if target is not None:
        tgt_px = round_to_tick(float(target), TICK_SIZE)
        try:
            tgt_oid = kite.place_order(
                variety=kite.VARIETY_REGULAR,
                exchange=exch,
                tradingsymbol=tsym,
                transaction_type=exit_side,
                quantity=qty,
                product=PRODUCT,
                order_type=kite.ORDER_TYPE_LIMIT,
                price=tgt_px,
                validity=kite.VALIDITY_DAY,
                tag=TAG,
            )
            print(f"[LIVE] TARGET LIMIT placed @ {tgt_px}, oid={tgt_oid}")
        except Exception as e:
            print(f"[WARN] Target order failed: {e}")

    beep()
    print(f"[DONE] Entered {kite_symbol}. entry_oid={entry_oid} sl_oid={sl_oid} target_oid={tgt_oid}")


# ============================================================
# MAIN LOOP
# ============================================================
def main():
    kite = oUtils.intialize_kite_api()
    strategies = parse_strategy_strings(STRATEGY_STRS)

    # Build per-stock plans with epoch state
    plans = []
    for s in strategies:
        sym = to_kite_symbol(s["stock"])
        epochs = [float(x) for x in s["epoch"]]
        stoploss_pts = float(s["stoploss"])
        target = s.get("target", None)
        target = float(target) if target is not None else None

        plans.append({
            "symbol": sym,
            "epochs": epochs,
            "stoploss_pts": stoploss_pts,
            "target": target,

            # epoch runtime state
            "prev": None,
            "idx": 0,
            "armed_lvl": float(epochs[0]),
            "armed_dir": None,
            "last_hit_candle": None,
            "final_dir": None,
        })

    symbols = [p["symbol"] for p in plans]

    print(f"[START] scanning {len(symbols)} symbols with multi-epoch sequences.")
    for p in plans:
        print(f"  - {p['symbol']} epochs={p['epochs']} stoploss_pts={p['stoploss_pts']} target={p['target']}")

    while True:
        q = safe_quote(kite, symbols)
        now_utc = dt.datetime.now(dt.timezone.utc)
        cur_candle = candle_id_ist(now_utc)

        # Process each stock plan
        for p in plans:
            sym = p["symbol"]
            item = q.get(sym) or {}
            lp = item.get("last_price")
            if lp is None:
                continue

            ltp = float(lp)

            # Initialize arming on first valid tick
            if p["prev"] is None:
                p["prev"] = ltp
                p["armed_lvl"] = float(p["epochs"][p["idx"]])
                p["armed_dir"] = arm_dir(ltp, p["armed_lvl"])
                print(f"[ARM] {sym} idx=0 lvl={p['armed_lvl']} dir={p['armed_dir']} ltp={ltp} candle={cur_candle}")
                continue

            # Try to advance the epoch sequence
            p["idx"], p["armed_lvl"], p["armed_dir"], p["last_hit_candle"], hit_dir, done = run_epochs(
                prev_val=p["prev"],
                cur_val=ltp,
                idx=p["idx"],
                armed_lvl=p["armed_lvl"],
                armed_dir=p["armed_dir"],
                last_hit_candle=p["last_hit_candle"],
                cur_candle=cur_candle,
                levels=p["epochs"],
            )

            # Log hits
            if hit_dir:
                hit_level = p["epochs"][p["idx"] - 1]  # because idx already incremented
                print(f"[HIT] {sym} hit_idx={p['idx']-1} lvl={hit_level} dir={hit_dir} ltp={ltp} candle={cur_candle}")
                p["final_dir"] = hit_dir
                if not done:
                    print(f"[ARM] {sym} next_idx={p['idx']} next_lvl={p['armed_lvl']} dir={p['armed_dir']} ltp={ltp}")

            # If finished, execute trade and exit script
            if done:
                side = kite.TRANSACTION_TYPE_BUY if p["final_dir"] == "UP" else kite.TRANSACTION_TYPE_SELL
                final_epoch = float(p["epochs"][-1])

                print(f"[DONE] {sym} epochs complete => {side} | final_epoch={final_epoch} | stoploss_pts={p['stoploss_pts']} | target={p['target']}")
                print("[CLEANUP] cancel open TAG orders + square-off TAG positions...")

                cancel_open_tag_orders(kite)
                squareoff_tag_positions(kite)

                place_entry_sl_target(
                    kite=kite,
                    kite_symbol=sym,
                    side=side,
                    final_epoch=final_epoch,
                    stoploss_pts=p["stoploss_pts"],
                    target=p["target"],
                    entry_quote_item=item,
                )
                return  # exit entire script after first trade

            # Update prev for next tick
            p["prev"] = ltp

        time.sleep(POLL_SEC)


if __name__ == "__main__":
    main()
