# pnl_guard_by_tag.py
# ============================================================
# EXIT GUARD for a TAG using:
#   (A) EXIT_EPOCHS sequence (each epoch in a separate 1-min IST candle), AND
#   (B) TIMEOUT_EPOCH rule after N minutes from ENTRY time:
#         - LONG  => if LTP goes BELOW TIMEOUT_EPOCH after TIMEOUT_MINUTES => exit
#         - SHORT => if LTP goes ABOVE TIMEOUT_EPOCH after TIMEOUT_MINUTES => exit
#
# This script:
# - Polls LTP via kite.quote() every second (retry-safe).
# - Exits immediately if there are no open positions for this TAG (your requirement).
# - On exit condition:
#       1) Place market square-off for all open MIS positions belonging to symbols inferred from TAG orders
#       2) Cancel all open/pending TAG orders
#       3) Beep and exit
#
# Key robustness optimizations:
# - All Kite API calls (quote/orders/positions) are wrapped with retries.
# - Orders/positions are NOT fetched redundantly on every check:
#     * tagged_symbols_and_open_orders() is called once per exit action, not repeatedly
#     * open position check is lightweight and retry-safe
# - ENTRY time is computed once, but re-evaluated if not found on startup.
#
# Notes:
# - Kite positions do not store tag, so we infer "TAG symbols" from orders having this TAG.
# - If you run multiple strategies sharing same TAG, this script may touch them too.
#   Use a dedicated TAG for safety.

import time
import datetime as dt
import pytz
import Trading_2024.OptionTradeUtils as oUtils
from typing import Optional


# ============================================================
# CONFIG
# ============================================================
SYMBOL = "NSE:TITAN"

# (A) Exit by epochs (stock PRICE levels, not money) — each hit in separate 1-min candle
EXIT_EPOCHS = [4160]

# (B) Timeout epoch exit after N minutes from entry
ENABLE_TIMEOUT_EPOCH_EXIT = True
TIMEOUT_MINUTES = 20
TIMEOUT_EPOCH = 4100.0

# Polling / retries
POLL_SEC = 1
RETRY_SEC = 2

PRODUCT = "MIS"
PAPER = False
TAG = oUtils.STOCK_INTRADAY_TAG
ONLY_TODAY_TAGGED = True

INDIA_TZ = pytz.timezone("Asia/Kolkata")


# ============================================================
# BEEP
# ============================================================
def beep():
    try:
        import winsound
        winsound.Beep(1200, 300)
    except Exception:
        print("\a", end="", flush=True)


# ============================================================
# RETRY-SAFE KITE CALLS
# ============================================================
def safe_quote_ltp(kite, sym: str) -> float:
    """Return LTP for sym, retrying on transient failures."""
    while True:
        try:
            q = kite.quote([sym])
            item = q.get(sym) or {}
            lp = item.get("last_price")
            if lp is None:
                raise RuntimeError("last_price missing")
            return float(lp)
        except Exception as e:
            print(f"[WARN] quote() failed for {sym}: {e}. Retrying in {RETRY_SEC}s...")
            time.sleep(RETRY_SEC)


def safe_orders(kite) -> list:
    """Return kite.orders(), retrying on transient failures."""
    while True:
        try:
            return kite.orders()
        except Exception as e:
            print(f"[WARN] orders() failed: {e}. Retrying in {RETRY_SEC}s...")
            time.sleep(RETRY_SEC)


def safe_positions(kite) -> dict:
    """Return kite.positions(), retrying on transient failures."""
    while True:
        try:
            return kite.positions()
        except Exception as e:
            print(f"[WARN] positions() failed: {e}. Retrying in {RETRY_SEC}s...")
            time.sleep(RETRY_SEC)


# ============================================================
# EPOCH ENGINE HELPERS
# ============================================================
def candle_id_ist(now_utc: dt.datetime) -> str:
    """1-min candle key in IST."""
    return now_utc.astimezone(INDIA_TZ).strftime("%Y-%m-%d %H:%M")


def arm_dir(cur: float, lvl: float) -> str:
    """If cur is below level => expect UP cross; else expect DOWN cross."""
    return "UP" if cur < lvl else "DOWN"


def crossed(prev: float, cur: float, lvl: float, d: str) -> bool:
    """Directional cross check."""
    return (prev < lvl <= cur) if d == "UP" else (prev > lvl >= cur)


def run_epochs(prev_val, cur_val, idx, armed_lvl, armed_dir, last_candle, cur_candle, levels):
    """
    Progress EXIT_EPOCHS sequence.
    Enforces: each epoch hit must occur in a different 1-min IST candle.
    """
    hit_dir = None
    if crossed(prev_val, cur_val, armed_lvl, armed_dir):
        if last_candle is None or cur_candle != last_candle:
            hit_dir = armed_dir
            last_candle = cur_candle
            idx += 1
            if idx >= len(levels):
                return idx, armed_lvl, armed_dir, last_candle, hit_dir, True
            armed_lvl = float(levels[idx])
            armed_dir = arm_dir(cur_val, armed_lvl)
    return idx, armed_lvl, armed_dir, last_candle, hit_dir, False


# ============================================================
# TAG / ORDER / POSITION HELPERS
# ============================================================
def is_today_ist(order_ts: str) -> bool:
    """Return True if order timestamp belongs to today in IST."""
    try:
        d = order_ts.split(" ")[0]
        today = dt.datetime.now(INDIA_TZ).strftime("%Y-%m-%d")
        return d == today
    except Exception:
        return True


def tagged_symbols_and_open_orders(kite):
    """
    Returns:
      syms: set of 'EXCH:TRADINGSYMBOL' that appear under TAG orders
      open_oids: list of open/pending order_ids under TAG
    """
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


def has_open_tag_positions(kite) -> bool:
    """
    True if any open MIS position exists for any symbol inferred from TAG orders.
    If TAG doesn't exist in today's orders, treat as no positions to guard.
    """
    syms, _ = tagged_symbols_and_open_orders(kite)
    if not syms:
        return False

    for p in safe_positions(kite).get("net", []):
        if p.get("product") != PRODUCT:
            continue
        sym = f'{p.get("exchange")}:{p.get("tradingsymbol")}'
        if sym in syms and int(p.get("quantity") or 0) != 0:
            return True

    return False


def net_qty_for_symbol(kite, symbol: str) -> int:
    """Return net qty for SYMBOL in PRODUCT: >0 long, <0 short, 0 flat."""
    exch, tsym = symbol.split(":")
    for p in safe_positions(kite).get("net", []):
        if p.get("product") != PRODUCT:
            continue
        if p.get("exchange") == exch and p.get("tradingsymbol") == tsym:
            return int(p.get("quantity") or 0)
    return 0


# ============================================================
# ENTRY TIME DETECTION (for TIMEOUT_EPOCH logic)
# ============================================================
def parse_kite_order_ts_ist(ts: str) -> dt.datetime:
    """
    Kite often returns 'YYYY-MM-DD HH:MM:SS' in local timezone.
    We interpret it as IST (as you run from India).
    """
    try:
        x = dt.datetime.strptime(ts[:19], "%Y-%m-%d %H:%M:%S")
        return INDIA_TZ.localize(x)
    except Exception:
        return dt.datetime.now(INDIA_TZ)


def get_latest_entry_time_ist(kite, symbol: str) -> Optional[dt.datetime]:
    """
    Find latest COMPLETE tagged order for this SYMBOL that looks like an entry:
    - status COMPLETE
    - order_type != SLM (exclude stoploss orders)
    Returns None if not found.
    """
    exch, tsym = symbol.split(":")
    best = None
    for o in safe_orders(kite):
        if o.get("tag") != TAG:
            continue
        if ONLY_TODAY_TAGGED and not is_today_ist(str(o.get("order_timestamp", ""))):
            continue
        if o.get("exchange") != exch or o.get("tradingsymbol") != tsym:
            continue
        if str(o.get("status", "")).upper() != "COMPLETE":
            continue
        if str(o.get("order_type", "")).upper() == "SLM":
            continue

        ts = str(o.get("order_timestamp") or o.get("exchange_timestamp") or "")
        t = parse_kite_order_ts_ist(ts) if ts else dt.datetime.now(INDIA_TZ)
        if best is None or t > best:
            best = t
    return best


# ============================================================
# EXIT ACTIONS
# ============================================================
def cancel_open_tag_orders(kite):
    """Cancel all open/pending orders for TAG."""
    if PAPER:
        print("[PAPER] Would cancel OPEN/TRIGGER PENDING TAG orders.")
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
    Square-off open MIS positions for symbols inferred from TAG orders.
    """
    syms, _ = tagged_symbols_and_open_orders(kite)
    if not syms:
        print(f"[INFO] No tagged symbols found for TAG={TAG}.")
        return

    if PAPER:
        print(f"[PAPER] Would square-off open {PRODUCT} positions for TAG symbols={sorted(syms)}")
        return

    for p in safe_positions(kite).get("net", []):
        sym = f'{p.get("exchange")}:{p.get("tradingsymbol")}'
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


def do_full_exit(kite, reason: str):
    """
    Standard exit behavior:
      1) Square-off positions
      2) Cancel remaining open orders
      3) Beep
    """
    print(f"[EXIT-ACTION] {reason}")
    squareoff_tag_positions(kite)
    time.sleep(1)  # small buffer for OMS updates
    cancel_open_tag_orders(kite)
    beep()


# ============================================================
# MAIN
# ============================================================
def main():
    kite = oUtils.intialize_kite_api()

    # If there is no open position corresponding to this TAG, nothing to guard -> exit.
    if not has_open_tag_positions(kite):
        print(f"[EXIT] No open {PRODUCT} positions found for TAG={TAG}. Exiting.")
        return

    # Determine entry time for TIMEOUT_EPOCH logic.
    entry_time_ist = get_latest_entry_time_ist(kite, SYMBOL)
    if entry_time_ist is None:
        # Fallback: if cannot find entry order timestamp, start timer from now.
        entry_time_ist = dt.datetime.now(INDIA_TZ)
        print("[WARN] Could not detect entry time from orders; using now() as entry time.")

    timeout_at_ist = entry_time_ist + dt.timedelta(minutes=TIMEOUT_MINUTES)
    print(
        f"[INFO] EntryTime(IST)={entry_time_ist.strftime('%Y-%m-%d %H:%M:%S')} | "
        f"TimeoutAt={timeout_at_ist.strftime('%Y-%m-%d %H:%M:%S')} | "
        f"TIMEOUT_EPOCH={TIMEOUT_EPOCH}"
    )

    # Epoch state
    prev = None
    idx = 0
    armed_lvl = None
    armed_dir = None
    last_candle = None

    print(f"[START] EXIT-GUARD {SYMBOL} EXIT_EPOCHS={EXIT_EPOCHS} (separate 1-min candle) tag={TAG}")

    while True:
        # If position already closed (target/SL/manual), stop guarding and exit.
        if not has_open_tag_positions(kite):
            print(f"[EXIT] No open {PRODUCT} positions found for TAG={TAG}. Cleaning open orders (if any) and exiting.")
            cancel_open_tag_orders(kite)
            return

        # Get LTP (retry-safe)
        ltp = safe_quote_ltp(kite, SYMBOL)

        # Time/candle tracking
        now_utc = dt.datetime.now(dt.timezone.utc)
        cur_candle = candle_id_ist(now_utc)
        now_ist = dt.datetime.now(INDIA_TZ)

        # ----------------------------------------------------
        # (B) TIMEOUT_EPOCH rule (after TIMEOUT_MINUTES)
        # ----------------------------------------------------
        if ENABLE_TIMEOUT_EPOCH_EXIT and now_ist >= timeout_at_ist:
            netq = net_qty_for_symbol(kite, SYMBOL)  # >0 long, <0 short, 0 flat
            if netq > 0 and ltp < float(TIMEOUT_EPOCH):
                do_full_exit(kite, f"TimeoutEpoch: LONG and LTP({ltp}) < TIMEOUT_EPOCH({TIMEOUT_EPOCH}) after {TIMEOUT_MINUTES}m")
                return
            if netq < 0 and ltp > float(TIMEOUT_EPOCH):
                do_full_exit(kite, f"TimeoutEpoch: SHORT and LTP({ltp}) > TIMEOUT_EPOCH({TIMEOUT_EPOCH}) after {TIMEOUT_MINUTES}m")
                return

        # ----------------------------------------------------
        # (A) EXIT_EPOCHS rule
        # ----------------------------------------------------
        if prev is None:
            prev = ltp
            armed_lvl = float(EXIT_EPOCHS[idx])
            armed_dir = arm_dir(ltp, armed_lvl)
            print(f"[ARM] idx={idx} lvl={armed_lvl} dir={armed_dir} ltp={ltp} candle={cur_candle}")
            time.sleep(POLL_SEC)
            continue

        idx, armed_lvl, armed_dir, last_candle, hit_dir, done = run_epochs(
            prev, ltp, idx, armed_lvl, armed_dir, last_candle, cur_candle, EXIT_EPOCHS
        )

        if hit_dir:
            print(f"[HIT] idx={idx-1} lvl={EXIT_EPOCHS[idx-1]} dir={hit_dir} ltp={ltp} candle={cur_candle}")
            if not done:
                print(f"[ARM] idx={idx} lvl={armed_lvl} dir={armed_dir} ltp={ltp} candle={cur_candle}")

        if done:
            do_full_exit(kite, "Exit epochs complete")
            return

        prev = ltp
        time.sleep(POLL_SEC)


if __name__ == "__main__":
    main()
