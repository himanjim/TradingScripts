from __future__ import annotations

import math
from datetime import datetime
import pytz

import OptionTradeUtils as oUtils



# "Bit on the higher side" for LIMIT BUY:
# Example: LTP 1.25 -> LIMIT 1.30 (one tick up)
TICK_SIZE = 0.05
PAD_PCT = 0.01  # 1% pad for bigger premiums; tick pad dominates for small premiums


def _normalize_exchange(ex: str) -> str:
    # Kite expects "NFO", "NSE" etc in exchange=..., and "NFO:SYMBOL" in ltp keys.
    return (ex or "").replace(":", "").strip()


def round_up_to_tick(price: float, tick: float = TICK_SIZE) -> float:
    if price <= 0:
        return 0.0
    # integer-tick rounding avoids float drift
    ticks = int(math.ceil((price + 1e-12) / tick))
    return round(ticks * tick, 2)


def limit_buy_price_from_ltp(ltp: float) -> float:
    if ltp <= 0:
        return 0.0
    pad = max(TICK_SIZE, ltp * PAD_PCT)
    return round_up_to_tick(ltp + pad)


def _has_open_orders_for_symbols(kite, exchange: str, symbols: list[str]) -> bool:
    """
    Avoid double-entry if previous limit orders are still OPEN/TRIGGER PENDING.
    """
    ex = _normalize_exchange(exchange)
    wanted = set(symbols)
    try:
        for o in kite.orders():
            if (
                o.get("exchange") == ex
                and o.get("tradingsymbol") in wanted
                and o.get("status") in ("OPEN", "TRIGGER PENDING")
            ):
                return True
    except Exception:
        # If orders() fails, don't block trading; just proceed.
        return False
    return False


def place_long_otm_orders(kite, pe_symbol: str, ce_symbol: str, qty: int, exchange: str, product: str) -> None:
    ex = _normalize_exchange(exchange)

    # 1) Block if positions already open
    try:
        positions = kite.positions()
        net_positions = positions.get("net", [])
        open_pos = {p.get("tradingsymbol") for p in net_positions if p.get("quantity", 0) != 0}
    except Exception as e:
        print(f"⚠️ Could not fetch positions(): {e}. Proceeding anyway.")
        open_pos = set()

    if pe_symbol in open_pos or ce_symbol in open_pos:
        existing = pe_symbol if pe_symbol in open_pos else ce_symbol
        print(f"⛔ Trade skipped: Existing open position found in {existing}\n")
        return

    # 2) Block if previous limit orders are still open
    if _has_open_orders_for_symbols(kite, ex, [pe_symbol, ce_symbol]):
        print("⛔ Trade skipped: Found OPEN/TRIGGER-PENDING orders for the same symbols.\n")
        return

    # 3) Fetch LTPs (lightweight)
    pe_key = f"{ex}:{pe_symbol}"
    ce_key = f"{ex}:{ce_symbol}"

    try:
        ltp_map = kite.ltp([pe_key, ce_key])
        pe_ltp = ltp_map[pe_key]["last_price"]
        ce_ltp = ltp_map[ce_key]["last_price"]
    except Exception as e:
        print(f"❌ LTP fetch failed for {pe_key}/{ce_key}: {e}")
        return

    if not pe_ltp or not ce_ltp:
        print(f"❌ Illiquid/invalid LTPs: PE={pe_ltp}, CE={ce_ltp}. Not placing orders.")
        return

    pe_price = limit_buy_price_from_ltp(float(pe_ltp))
    ce_price = limit_buy_price_from_ltp(float(ce_ltp))

    print(f"PE {pe_symbol} LTP={pe_ltp} -> LIMIT={pe_price}")
    print(f"CE {ce_symbol} LTP={ce_ltp} -> LIMIT={ce_price}")

    # 4) Place LIMIT BUY orders (separate try blocks)
    try:
        kite.place_order(
            tradingsymbol=pe_symbol,
            variety=kite.VARIETY_REGULAR,
            exchange=ex,
            transaction_type=kite.TRANSACTION_TYPE_BUY,
            quantity=qty,
            order_type=kite.ORDER_TYPE_LIMIT,
            price=pe_price,
            product=product,
            validity=kite.VALIDITY_DAY,
            tag=oUtils.LS_ORDER_TAG,
        )
        print(f"✅ Placed LIMIT BUY for {pe_symbol} @ {pe_price}")
    except Exception as e:
        print(f"❌ Failed PE order for {pe_symbol}: {e}")

    try:
        kite.place_order(
            tradingsymbol=ce_symbol,
            variety=kite.VARIETY_REGULAR,
            exchange=ex,
            transaction_type=kite.TRANSACTION_TYPE_BUY,
            quantity=qty,
            order_type=kite.ORDER_TYPE_LIMIT,
            price=ce_price,
            product=product,
            validity=kite.VALIDITY_DAY,
            tag=oUtils.LS_ORDER_TAG,
        )
        print(f"✅ Placed LIMIT BUY for {ce_symbol} @ {ce_price}")
    except Exception as e:
        print(f"❌ Failed CE order for {ce_symbol}: {e}")

    print(f"Done at {datetime.now(indian_timezone).time()}\n")


def round_to_strike(ltp: float, strike_multiple: int) -> int:
    # Round-half-up to nearest strike multiple
    return int(math.floor((ltp + strike_multiple / 2) / strike_multiple) * strike_multiple)


if __name__ == "__main__":
    indian_timezone = pytz.timezone("Asia/Kolkata")
    kite = oUtils.intialize_kite_api()

    vals = oUtils.get_instruments(kite)

    # Backward/forward compatible unpacking:
    # Old scripts: 8 items; newer: 9 items including LONG_STRADDLE_STRIKE_DISTANCE
    if isinstance(vals, (list, tuple)) and len(vals) >= 8:
        UNDER_LYING_EXCHANGE, UNDERLYING, OPTIONS_EXCHANGE, PART_SYMBOL, QTY, STRIKE_MULTIPLE, _, _ = vals[:8]
        LONG_STRADDLE_STRIKE_DISTANCE = vals[8] if len(vals) >= 9 else getattr(oUtils, "LONG_STRADDLE_STRIKE_DISTANCE", 0)
    else:
        raise RuntimeError("get_instruments() did not return expected tuple/list")

    ex_ul = _normalize_exchange(UNDER_LYING_EXCHANGE)
    ex_opt = _normalize_exchange(OPTIONS_EXCHANGE)

    # PART_SYMBOL in your utils is like ':NIFTY25D16'
    PART_SYMBOL = (PART_SYMBOL or "").lstrip(":")

    # Product (NRML is fine for long options; switch to MIS if you want intraday)
    PRODUCT = kite.PRODUCT_NRML

    # UNDERLYING already includes leading ':', e.g. ':NIFTY 50'
    under_lying_symbol = f"{ex_ul}{UNDERLYING}"

    while True:
        input(
            f"Press Enter to place LONG OTM straddle (actually strangle) for {PART_SYMBOL} | dist={LONG_STRADDLE_STRIKE_DISTANCE}"
        )

        # Underlying LTP
        try:
            ul_ltp = kite.ltp([under_lying_symbol])[under_lying_symbol]["last_price"]
        except Exception as e:
            print(f"❌ Underlying LTP fetch failed for {under_lying_symbol}: {e}")
            continue

        atm = round_to_strike(float(ul_ltp), int(STRIKE_MULTIPLE))

        dist = int(LONG_STRADDLE_STRIKE_DISTANCE)
        if dist % int(STRIKE_MULTIPLE) != 0:
            dist = int(round(dist / int(STRIKE_MULTIPLE)) * int(STRIKE_MULTIPLE))
            print(f"⚠️ LONG_STRADDLE_STRIKE_DISTANCE adjusted to {dist} (multiple of {STRIKE_MULTIPLE})")

        ce_strike = atm + dist
        pe_strike = atm - dist

        option_pe = f"{PART_SYMBOL}{pe_strike}PE"
        option_ce = f"{PART_SYMBOL}{ce_strike}CE"

        print(f"Underlying {under_lying_symbol} LTP={ul_ltp} ATM={atm} -> PE={pe_strike}, CE={ce_strike}")
        place_long_otm_orders(kite, option_pe, option_ce, int(QTY), ex_opt, PRODUCT)
