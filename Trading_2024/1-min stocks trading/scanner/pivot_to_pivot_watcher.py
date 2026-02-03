# pivot_to_pivot_watcher.py
# ==============================================================================
# WHAT THIS SCRIPT DOES
# ------------------------------------------------------------------------------
# This script continuously scans the SQLite database produced by your writer
# (liquid_universe_ltp_cache.py) and detects when a stock's LTP completes a move
# from one pivot level to the *next adjacent* pivot level (upwards or downwards).
#
# IMPORTANT: "pivot1 might have been crossed much earlier"
# ------------------------------------------------------------------------------
# This is explicitly handled:
#   - On startup, for each stock we read the latest tick from DB.
#   - We set the "anchor pivot" (pivot1) based on the current LTP:
#       anchor = highest pivot level <= current LTP
#     meaning pivot1 can indeed be from hours earlier.
#   - From that point onward, we only look for pivot2 crossings in the future.
#
# WHAT COUNTS AS "MOVE COMPLETE"?
# ------------------------------------------------------------------------------
# A "pivot-to-pivot move" is complete when:
#   - You are anchored at pivot1 (the last confirmed level), and
#   - LTP crosses pivot2 (the next adjacent pivot in the move direction).
#
# When pivot2 is crossed, we print:
#   1) Kite chart URL:
#        https://kite.zerodha.com/markets/ext/chart/web/ciq/{EXCHANGE}/{SYMBOL}/{TOKEN}
#   2) A line showing the pivot values crossed:
#        CROSS UP: P(1950.25) -> CPR_high(1962.10)
#   3) A dict-like string you requested:
#        {'stock': 'SYMBOL', 'epoch': pivot2_value,
#         'stoploss': 0.5*abs(pivot2-pivot1),
#         'target': pivot3_value}
#   4) A beep sound on match.
#
# DATA MODEL ASSUMPTIONS (from writer script)
# ------------------------------------------------------------------------------
# The DB should contain:
#   - universe(instrument, exchange, tradingsymbol, instrument_token, ...)
#   - pivots(instrument, P, CPR_lower, CPR_upper, R1, S1, ...)
#   - ticks(instrument, ts_ms INTEGER, ltp REAL, PRIMARY KEY (instrument, ts_ms))
#
# PERFORMANCE NOTES
# ------------------------------------------------------------------------------
# - This watcher is read-only. It opens SQLite in query_only mode.
# - It performs per-instrument incremental reads using the PK index:
#       SELECT ts_ms, ltp FROM ticks
#         WHERE instrument=? AND ts_ms>? ORDER BY ts_ms ASC
#   This is efficient and does NOT require a global ts index.
# - With ~100 stocks and 1 tick/sec per stock, this is small workload.
#
# EDGE CASES HANDLED
# ------------------------------------------------------------------------------
# - CPR band collapse: CPR_lower may equal CPR_upper (or equal P).
#   We dedupe pivot values so you don't get zero-magnitude events.
# - Gap jumps: If LTP jumps across multiple pivots in one tick, the script can:
#     (a) emit every intermediate crossing, or
#     (b) emit only the final crossing,
#   controlled by EMIT_INTERMEDIATE_CROSSES.
# ==============================================================================

import os
import time
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pytz

# ==============================================================================
# USER CONFIG (EDIT ONLY THIS SECTION)
# ==============================================================================

# Timezone used only for logging timestamps (your DB timestamps are epoch ms)
TZ = pytz.timezone("Asia/Kolkata")

# Folder where your writer stores cache_YYYY-MM-DD.sqlite
CACHE_DIR = "./live_cache"

# If you want to hardcode a DB path, set it here. If None, the script auto-picks
# the most recent cache_*.sqlite from CACHE_DIR.
DB_PATH = None

# Main polling loop sleep time. 1.0s is enough since the writer inserts once/sec.
POLL_INTERVAL_SEC = 1.0

# Debounce (per stock) to prevent repeated printing of identical pivot events
# during whipsaws. Applies to same (pivot1, pivot2, direction) signature.
EVENT_COOLDOWN_SEC = 30

# Number formatting in printed output
PRINT_DECIMALS = 2

# If True: emit intermediate pivot crossings if LTP jumps over multiple levels
# in a single tick. If False: emit only the final pivot crossed in that tick.
EMIT_INTERMEDIATE_CROSSES = True

# Optional strictness: require pivot1 and pivot2 crossing to happen in different
# 1-minute buckets. OFF by default.
REQUIRE_DIFFERENT_MINUTE = False

# ==============================================================================
# BEEP SUPPORT (Windows + fallback)
# ==============================================================================

# On Windows, winsound.Beep works reliably.
# On non-Windows, fallback prints ASCII bell '\a' (may beep depending on terminal).
try:
    import winsound  # Windows only

    def beep():
        # Frequency (Hz), Duration (ms)
        winsound.Beep(1200, 180)

except Exception:
    def beep():
        print("\a", end="", flush=True)


# ==============================================================================
# SMALL UTILITIES
# ==============================================================================

def log(level: str, msg: str) -> None:
    """Console logging with IST timestamps for readability."""
    now = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
    print(f"{now} [{level}] {msg}")


def pick_latest_db(cache_dir: str) -> str:
    """
    Choose the newest cache_*.sqlite in CACHE_DIR.
    Filenames are cache_YYYY-MM-DD.sqlite which sorts lexicographically.
    """
    files = [f for f in os.listdir(cache_dir) if f.startswith("cache_") and f.endswith(".sqlite")]
    if not files:
        raise FileNotFoundError(f"No cache_*.sqlite found in {cache_dir}")
    files.sort()
    return os.path.join(cache_dir, files[-1])


def kite_chart_url(exchange: str, symbol: str, token: int) -> str:
    """
    Exact URL format you asked for.
    Example:
      https://kite.zerodha.com/markets/ext/chart/web/ciq/NSE/HDFCBANK/341249
    """
    return f"https://kite.zerodha.com/markets/ext/chart/web/ciq/{exchange}/{symbol}/{token}"


def fmt(x: Optional[float]) -> str:
    """Format floats with fixed decimals; represent None as 'None'."""
    if x is None:
        return "None"
    return f"{x:.{PRINT_DECIMALS}f}"


def minute_bucket(ts_ms: int) -> int:
    """Convert epoch milliseconds to integer 'minute bucket'."""
    return ts_ms // 60000


# ==============================================================================
# DATA STRUCTURES (per-stock state)
# ==============================================================================

@dataclass
class Level:
    """
    One pivot level in the ladder.
    name: label like S1, CPR_low, P, CPR_high, R1
    value: numeric price level
    """
    name: str
    value: float


@dataclass
class StockCtx:
    """
    Per-stock tracking context.

    levels:
      Sorted unique pivot levels ascending by value. Unique is important because
      CPR_lower may equal CPR_upper or P. We remove duplicates to avoid 0-move.

    anchor_idx:
      Index into levels of the "pivot1" anchor. This is the last confirmed level.
      On startup, anchor_idx is derived from current LTP (pivot1 could be old).

      anchor_idx = -1 means price is below the lowest level (no pivot1 inside list).

    anchor_minute:
      Minute bucket when anchor was last set (used only if REQUIRE_DIFFERENT_MINUTE)

    last_ts_ms / last_ltp:
      Last processed tick for this stock.

    last_event_sig / last_event_ts_ms:
      Used to debounce repeated same events during whipsaw.
    """
    exchange: str
    symbol: str
    token: int
    levels: List[Level]

    anchor_idx: int
    anchor_minute: Optional[int]

    last_ts_ms: int
    last_ltp: Optional[float]

    last_event_sig: Optional[Tuple[int, int, int]]  # (p1_idx, p2_idx, dir)
    last_event_ts_ms: int


# ==============================================================================
# PIVOT LADDER CONSTRUCTION (handles duplicates)
# ==============================================================================

def build_unique_levels(S1: float, CPR_low: float, P: float, CPR_high: float, R1: float) -> List[Level]:
    """
    Create a pivot ladder with unique values.

    Why unique?
      Sometimes CPR collapses and CPR_low == CPR_high (or equals P).
      If we keep duplicates, we'd generate zero-magnitude events and stoploss=0.

    How duplicates are resolved:
      - Sort by value ascending
      - For equal values, keep the one with deterministic label priority
    """
    raw = [
        Level("S1", float(S1)),
        Level("CPR_low", float(CPR_low)),
        Level("P", float(P)),
        Level("CPR_high", float(CPR_high)),
        Level("R1", float(R1)),
    ]

    # Lower priority number = preferred label if duplicates occur
    priority = {"S1": 0, "CPR_low": 1, "P": 2, "CPR_high": 3, "R1": 4}
    raw_sorted = sorted(raw, key=lambda x: (x.value, priority.get(x.name, 9)))

    uniq: List[Level] = []
    eps = 1e-9  # float equality tolerance
    for lv in raw_sorted:
        if not uniq or abs(lv.value - uniq[-1].value) > eps:
            uniq.append(lv)

    return uniq


def init_anchor_from_ltp(levels: List[Level], ltp: float) -> int:
    """
    Determine anchor pivot index from current LTP:
      anchor = highest level with value <= ltp.
    If ltp is below the lowest pivot => anchor = -1.
    """
    vals = [lv.value for lv in levels]
    if ltp < vals[0]:
        return -1

    # Walk from the top down to find highest level <= ltp
    for i in range(len(vals) - 1, -1, -1):
        if ltp >= vals[i]:
            return i

    return -1  # defensive fallback


# ==============================================================================
# LOAD UNIVERSE + PIVOTS FROM SQLITE
# ==============================================================================

def load_universe_and_pivots(conn: sqlite3.Connection) -> Dict[str, StockCtx]:
    """
    Load all instruments + pivots and build per-stock contexts.

    We join universe and pivots on instrument. This requires that the writer has
    already stored both tables (liquidity scan + pivot calculation done).
    """
    conn.row_factory = sqlite3.Row
    q = """
        SELECT
            u.instrument, u.exchange, u.tradingsymbol, u.instrument_token,
            p.P, p.CPR_lower, p.CPR_upper, p.R1, p.S1
        FROM universe u
        JOIN pivots p ON p.instrument = u.instrument
    """
    rows = conn.execute(q).fetchall()
    if not rows:
        raise RuntimeError("No universe+pivots rows found yet (writer not ready).")

    ctx_map: Dict[str, StockCtx] = {}
    for r in rows:
        inst = r["instrument"]
        exch = (r["exchange"] or "").upper()
        sym = r["tradingsymbol"]
        token = int(r["instrument_token"])

        # Build the pivot ladder for this stock
        levels = build_unique_levels(
            S1=float(r["S1"]),
            CPR_low=float(r["CPR_lower"]),
            P=float(r["P"]),
            CPR_high=float(r["CPR_upper"]),
            R1=float(r["R1"]),
        )

        ctx_map[inst] = StockCtx(
            exchange=exch,
            symbol=sym,
            token=token,
            levels=levels,
            anchor_idx=-1,
            anchor_minute=None,
            last_ts_ms=0,
            last_ltp=None,
            last_event_sig=None,
            last_event_ts_ms=0,
        )

    log("INFO", f"Loaded {len(ctx_map)} instruments with pivot ladders.")
    return ctx_map


# ==============================================================================
# EVENT EMISSION (prints URL + pivots crossed + dict + beep)
# ==============================================================================

def maybe_emit_event(ctx: StockCtx, p1_idx: int, p2_idx: int, direction: int, ts_ms: int) -> None:
    """
    Print and beep for a completed pivot-to-pivot movement.

    Debounce:
      Uses (pivot1_idx, pivot2_idx, direction) + time cooldown to avoid spamming
      the same event during whipsaw around pivot2.

    Optional strictness:
      If REQUIRE_DIFFERENT_MINUTE=True, require pivot2 crossing in a different
      minute bucket than anchor_minute.
    """
    sig = (p1_idx, p2_idx, direction)

    # Cooldown / dedup check
    if ctx.last_event_sig == sig and (ts_ms - ctx.last_event_ts_ms) < EVENT_COOLDOWN_SEC * 1000:
        return

    # Optional: different minute constraint
    if REQUIRE_DIFFERENT_MINUTE:
        if ctx.anchor_minute is not None and minute_bucket(ts_ms) == ctx.anchor_minute:
            return

    p1 = ctx.levels[p1_idx]
    p2 = ctx.levels[p2_idx]

    # If something degenerates to same value, skip
    mag = abs(p2.value - p1.value)
    if mag < 1e-9:
        return

    stoploss = 0.5 * mag

    # Target pivot is pivot3: next level after pivot2 in the same direction
    target_idx = p2_idx + 1 if direction > 0 else p2_idx - 1
    target = ctx.levels[target_idx] if 0 <= target_idx < len(ctx.levels) else None

    # 1) Print Kite URL
    print(kite_chart_url(ctx.exchange, ctx.symbol, ctx.token))

    # 2) Print pivot values crossed (requested)
    dir_s = "UP" if direction > 0 else "DOWN"
    print(f"CROSS {dir_s}: {p1.name}({fmt(p1.value)}) -> {p2.name}({fmt(p2.value)})")

    # 3) Print dict-like string in your exact style
    print(
        "{"
        f"'stock': '{ctx.symbol}', "
        f"'epoch': {fmt(p2.value)}, "
        f"'stoploss': {fmt(stoploss)}, "
        f"'target': {fmt(target.value) if target else 'None'}"
        "}"
    )

    # 4) Beep
    beep()

    # Update debounce state
    ctx.last_event_sig = sig
    ctx.last_event_ts_ms = ts_ms


# ==============================================================================
# TICK PROCESSING (core crossing detection)
# ==============================================================================

def process_tick(ctx: StockCtx, ts_ms: int, ltp: float) -> None:
    """
    Incorporate one new tick and detect pivot crossings.

    Direction:
      direction = +1 if price increased vs previous tick
      direction = -1 if price decreased vs previous tick

    Adjacent crossing logic:
      Upward crossing of next pivot v happens if:
        prev < v <= cur
      Downward crossing happens if:
        prev > v >= cur

    Gap jumps:
      If LTP jumps across multiple pivot levels in one tick, we step the anchor
      across each adjacent pivot that got crossed. Depending on
      EMIT_INTERMEDIATE_CROSSES, we may emit:
        - each crossing, or
        - only the final crossing.
    """
    # First tick for this instrument: initialize anchor from current LTP.
    if ctx.last_ltp is None:
        ctx.anchor_idx = init_anchor_from_ltp(ctx.levels, ltp)
        ctx.anchor_minute = minute_bucket(ts_ms)
        ctx.last_ltp = ltp
        ctx.last_ts_ms = ts_ms
        return

    prev = ctx.last_ltp
    cur = ltp

    # No movement, no crossing
    if cur == prev:
        ctx.last_ltp = cur
        ctx.last_ts_ms = ts_ms
        return

    direction = 1 if cur > prev else -1

    # Normalize anchor bounds defensively
    if ctx.anchor_idx < -1:
        ctx.anchor_idx = -1
    if ctx.anchor_idx >= len(ctx.levels):
        ctx.anchor_idx = len(ctx.levels) - 1

    # If we are suppressing intermediate crosses, store the last one only.
    last_cross: Optional[Tuple[int, int, int]] = None  # (p1, p2, dir)

    while True:
        if direction > 0:
            # Moving up: next pivot is anchor+1
            nxt = ctx.anchor_idx + 1
            if nxt >= len(ctx.levels):
                break  # already above top level

            v = ctx.levels[nxt].value

            # Upward crossing condition
            if prev < v <= cur:
                p1 = ctx.anchor_idx
                p2 = nxt

                # Emit only if pivot1 is a real level (anchor -1 means below lowest)
                if 0 <= p1 < len(ctx.levels):
                    last_cross = (p1, p2, direction)
                    if EMIT_INTERMEDIATE_CROSSES:
                        maybe_emit_event(ctx, p1, p2, direction, ts_ms)

                # Update anchor to pivot2 (we have now "arrived" at pivot2)
                ctx.anchor_idx = p2
                ctx.anchor_minute = minute_bucket(ts_ms)
                continue

            # Not crossed
            break

        else:
            # Moving down: next pivot is anchor-1
            nxt = ctx.anchor_idx - 1
            if nxt < 0:
                break  # already below lowest level in ladder

            v = ctx.levels[nxt].value

            # Downward crossing condition
            if prev > v >= cur:
                p1 = ctx.anchor_idx
                p2 = nxt

                if 0 <= p1 < len(ctx.levels):
                    last_cross = (p1, p2, direction)
                    if EMIT_INTERMEDIATE_CROSSES:
                        maybe_emit_event(ctx, p1, p2, direction, ts_ms)

                ctx.anchor_idx = p2
                ctx.anchor_minute = minute_bucket(ts_ms)
                continue

            break

    # If intermediate crosses were suppressed, emit the final crossing only
    if (not EMIT_INTERMEDIATE_CROSSES) and last_cross is not None:
        p1, p2, d = last_cross
        maybe_emit_event(ctx, p1, p2, d, ts_ms)

    # Persist last tick
    ctx.last_ltp = cur
    ctx.last_ts_ms = ts_ms


# ==============================================================================
# MAIN LOOP: incremental DB scan
# ==============================================================================

def main():
    # Resolve DB to use
    db_path = DB_PATH or pick_latest_db(CACHE_DIR)
    log("INFO", f"Using DB: {db_path}")

    # Open DB in read-only-ish mode:
    # - query_only prevents accidental writes
    # - busy_timeout allows waiting when writer is committing
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA query_only=1;")
    conn.execute("PRAGMA busy_timeout=3000;")

    # If watcher starts before writer has created pivots, wait and retry.
    while True:
        try:
            ctx_map = load_universe_and_pivots(conn)
            break
        except Exception as e:
            log("WARN", f"Waiting for universe+pivots to be ready: {e}")
            time.sleep(2.0)

    # Initialize each stock context from the latest tick so we don't replay history.
    # This ensures "pivot1 may have been crossed much earlier" is naturally handled.
    init_q = "SELECT ts_ms, ltp FROM ticks WHERE instrument=? ORDER BY ts_ms DESC LIMIT 1"
    for inst, ctx in ctx_map.items():
        row = conn.execute(init_q, (inst,)).fetchone()
        if row:
            ts_ms = int(row[0])
            ltp = float(row[1])
            ctx.last_ts_ms = ts_ms
            ctx.last_ltp = ltp
            ctx.anchor_idx = init_anchor_from_ltp(ctx.levels, ltp)
            ctx.anchor_minute = minute_bucket(ts_ms)
        else:
            # No ticks yet -> the writer isn't producing ticks yet
            ctx.last_ts_ms = 0
            ctx.last_ltp = None
            ctx.anchor_idx = -1
            ctx.anchor_minute = None

    log("INFO", "Initialization complete. Starting scan loop...")

    # Per-instrument incremental query:
    # Uses the PK index on ticks(instrument, ts_ms).
    q_new = "SELECT ts_ms, ltp FROM ticks WHERE instrument=? AND ts_ms>? ORDER BY ts_ms ASC"

    try:
        while True:
            any_new = False

            for inst, ctx in ctx_map.items():
                try:
                    # Stream rows without fetchall() to avoid list allocation
                    for ts_ms, ltp in conn.execute(q_new, (inst, ctx.last_ts_ms)):
                        any_new = True
                        process_tick(ctx, int(ts_ms), float(ltp))
                except sqlite3.OperationalError as e:
                    # Most common transient issue: DB busy/locked while writer commits
                    log("WARN", f"SQLite OperationalError: {e}")
                    continue

            # If no new ticks arrived, sleep slightly longer to reduce CPU churn
            time.sleep(POLL_INTERVAL_SEC if any_new else min(1.5, POLL_INTERVAL_SEC + 0.3))

    except KeyboardInterrupt:
        log("WARN", "Stopped by user.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
