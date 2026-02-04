# pivot_to_pivot_watcher.py
# ==============================================================================
# PURPOSE
# ------------------------------------------------------------------------------
# Continuously scans the SQLite DB produced by liquid_universe_ltp_cache.py and
# detects when LTP completes a move from one pivot level (pivot1) to the NEXT
# ADJACENT pivot level (pivot2) in the move direction.
#
# When pivot2 is crossed, it prints:
#   1) Kite chart URL:
#        https://kite.zerodha.com/markets/ext/chart/web/ciq/NSE/HDFCBANK/341249
#   2) Pivot values crossed:
#        CROSS UP: CPR_low(2436.67) -> P(2447.33)
#   3) Dict-like payload (with your latest required formatting):
#        {'stock': 'ASIANPAINT', 'epoch': [2447.33], 'stoploss': 2442.00, 'target': 2458.00}
#   4) A beep sound
#
# IMPORTANT REQUIREMENT HANDLED:
# ------------------------------------------------------------------------------
# "pivot1 might have been crossed much earlier; only pivot2 is in the future"
#
# We handle this by:
#   - On startup, for each instrument we read ONLY the latest tick from DB
#   - We set the anchor pivot (pivot1) based on current LTP
#   - We start scanning ONLY ticks newer than that latest tick timestamp
#
# ALSO:
# ------------------------------------------------------------------------------
# - "Don't repeat stocks already highlighted" -> enforced (one alert per stock)
# - Stoploss is MEAN of pivot1 and pivot2 (midpoint price)
# - Epoch printed as a LIST
# - Robust SQLite handling and retries
# ==============================================================================

import os
import time
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pytz
from bisect import bisect_right


# ==============================================================================
# USER CONFIG (EDIT ONLY THIS SECTION)
# ==============================================================================

TZ = pytz.timezone("Asia/Kolkata")

# Folder where your writer stores cache_YYYY-MM-DD.sqlite
CACHE_DIR = "./live_cache"

# If you want to hardcode a DB path, set it here. If None, auto-picks latest cache_*.sqlite
DB_PATH = None

# Main polling loop cadence. Writer inserts ~1 tick/sec, so 1.0 sec is fine.
POLL_INTERVAL_SEC = 1.0

# Printed number decimals
PRINT_DECIMALS = 2

# If True: emit intermediate pivot crossings when LTP jumps over multiple levels in one tick.
# If False: emit only the FINAL crossing in that tick.
# Since you only want one highlight per stock anyway, keeping this False reduces noise.
EMIT_INTERMEDIATE_CROSSES = False

# Optional: if True, requires pivot2 crossing to happen in a different 1-minute bucket
# compared to when anchor was last set (helps reduce some whipsaw noise).
REQUIRE_DIFFERENT_MINUTE = False

# SQLite busy timeout (ms). Higher helps when writer is committing.
SQLITE_BUSY_TIMEOUT_MS = 5000

# How many times to retry when SQLite is locked/busy before skipping that cycle.
SQLITE_RETRIES = 5

# Backoff base for locked DB (seconds). Grows per attempt.
SQLITE_BACKOFF_BASE_SEC = 0.08

# Print pivot session date from pivots table on event (useful to verify prev-day logic)
PRINT_PIVOT_SESSION_DATE = True


# ==============================================================================
# BEEP SUPPORT (Windows + fallback)
# ==============================================================================

try:
    import winsound  # Windows only

    def beep():
        winsound.Beep(1200, 180)  # frequency Hz, duration ms

except Exception:
    def beep():
        # Terminal bell (may or may not beep depending on your terminal settings)
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
    Filenames are cache_YYYY-MM-DD.sqlite which sort lexicographically.
    """
    files = [f for f in os.listdir(cache_dir) if f.startswith("cache_") and f.endswith(".sqlite")]
    if not files:
        raise FileNotFoundError(f"No cache_*.sqlite found in {cache_dir}")
    files.sort()
    return os.path.join(cache_dir, files[-1])


def kite_chart_url(exchange: str, symbol: str, token: int) -> str:
    """Exact URL format you asked for."""
    return f"https://kite.zerodha.com/markets/ext/chart/web/ciq/{exchange}/{symbol}/{token}"


def fmt_num(x: Optional[float]) -> str:
    """Format float with fixed decimals; represent None as 'None'."""
    if x is None:
        return "None"
    return f"{x:.{PRINT_DECIMALS}f}"


def minute_bucket(ts_ms: int) -> int:
    """Convert epoch milliseconds to integer minute-bucket."""
    return ts_ms // 60000


def is_transient_sqlite_lock(err: Exception) -> bool:
    """
    Identify the common transient SQLite errors when writer is committing:
    - 'database is locked'
    - 'database is busy'
    """
    s = str(err).lower()
    return ("database is locked" in s) or ("database is busy" in s) or ("locked" in s) or ("busy" in s)


# ==============================================================================
# DATA STRUCTURES
# ==============================================================================

@dataclass
class Level:
    name: str
    value: float


@dataclass
class StockCtx:
    """
    Per-stock tracking state.
    """
    exchange: str
    symbol: str
    token: int

    # Pivot ladder sorted ascending by value (unique values)
    levels: List[Level]

    # Pivot session date stored by writer (prev session used for CPR)
    prev_session_date: Optional[str]

    # Index in levels representing the current anchor (pivot1).
    # anchor_idx = -1 means LTP is below the lowest level.
    anchor_idx: int

    # Minute bucket when anchor was last set
    anchor_minute: Optional[int]

    # Last processed tick
    last_ts_ms: int
    last_ltp: Optional[float]

    # One-and-done: if True, we never alert this stock again in this run
    done: bool


# ==============================================================================
# PIVOT LADDER CONSTRUCTION
# ==============================================================================

def build_unique_levels(S1: float, CPR_low: float, P: float, CPR_high: float, R1: float) -> List[Level]:
    """
    Build pivot ladder using the 5 lines you aligned to Kite chart display:
      R1, CPR_high, P, CPR_low, S1  (chart displays top to bottom)
    Internally we sort ascending for easier crossing logic.

    We also DEDUPE equal values to avoid zero-magnitude movements.
    """
    raw = [
        Level("S1", float(S1)),
        Level("CPR_low", float(CPR_low)),
        Level("P", float(P)),
        Level("CPR_high", float(CPR_high)),
        Level("R1", float(R1)),
    ]

    # If duplicates exist, keep deterministic label priority
    priority = {"S1": 0, "CPR_low": 1, "P": 2, "CPR_high": 3, "R1": 4}
    raw_sorted = sorted(raw, key=lambda lv: (lv.value, priority.get(lv.name, 9)))

    uniq: List[Level] = []
    eps = 1e-9
    for lv in raw_sorted:
        if not uniq or abs(lv.value - uniq[-1].value) > eps:
            uniq.append(lv)

    return uniq


def init_anchor_from_ltp(levels: List[Level], ltp: float) -> int:
    """
    anchor = index of highest pivot level <= current LTP
    If ltp < lowest pivot => anchor = -1
    Uses bisect for speed.
    """
    vals = [lv.value for lv in levels]
    if ltp < vals[0]:
        return -1

    # bisect_right returns insertion point to keep sorted order;
    # subtract 1 gives highest index where vals[idx] <= ltp
    idx = bisect_right(vals, ltp) - 1
    return max(-1, min(idx, len(vals) - 1))


# ==============================================================================
# SQLITE LOADERS
# ==============================================================================

def load_universe_and_pivots(conn: sqlite3.Connection) -> Dict[str, StockCtx]:
    """
    Load instruments + pivots and build per-stock contexts.
    Requires writer to have already stored:
      - universe
      - pivots
    """
    conn.row_factory = sqlite3.Row
    q = """
        SELECT
            u.instrument, u.exchange, u.tradingsymbol, u.instrument_token,
            p.prev_session_date,
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
            prev_session_date=r["prev_session_date"],
            anchor_idx=-1,
            anchor_minute=None,
            last_ts_ms=0,
            last_ltp=None,
            done=False,
        )

    log("INFO", f"Loaded {len(ctx_map)} instruments with pivot ladders.")
    return ctx_map


def init_from_latest_ticks(conn: sqlite3.Connection, ctx_map: Dict[str, StockCtx]) -> None:
    """
    Initialize each ctx from the latest tick in DB so we do NOT replay history.
    This is critical for your requirement: pivot1 may be from much earlier;
    only pivot2 in the future should be detected.
    """
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
            # No ticks yet for this instrument
            ctx.last_ts_ms = 0
            ctx.last_ltp = None
            ctx.anchor_idx = -1
            ctx.anchor_minute = None


# ==============================================================================
# EVENT EMISSION
# ==============================================================================

def emit_event_once(ctx: StockCtx, p1_idx: int, p2_idx: int, direction: int, ts_ms: int) -> None:
    """
    Emits exactly one alert per stock (ctx.done=True afterwards).

    stoploss (as you requested): mean(midpoint) of pivot1 and pivot2 prices.
    epoch printed as LIST.
    """
    if ctx.done:
        return  # absolutely no repeats for this stock in this run

    # Optional: require crossing in a different minute than anchor set
    if REQUIRE_DIFFERENT_MINUTE:
        if ctx.anchor_minute is not None and minute_bucket(ts_ms) == ctx.anchor_minute:
            return

    # Defensive: pivot indices must be valid
    if not (0 <= p1_idx < len(ctx.levels) and 0 <= p2_idx < len(ctx.levels)):
        return

    p1 = ctx.levels[p1_idx]
    p2 = ctx.levels[p2_idx]

    # Skip degenerate zero move
    if abs(p2.value - p1.value) < 1e-9:
        return

    # stoploss as MEAN of two pivot prices (midpoint price)
    stoploss_price = (p1.value + p2.value) / 2.0

    # target is the next pivot after pivot2 in the same direction (pivot3)
    target_idx = p2_idx + 1 if direction > 0 else p2_idx - 1
    target = ctx.levels[target_idx] if 0 <= target_idx < len(ctx.levels) else None

    # 1) Kite URL
    print(kite_chart_url(ctx.exchange, ctx.symbol, ctx.token))

    # Helpful debug: show which session pivots belong to (verifies "previous day")
    if PRINT_PIVOT_SESSION_DATE and ctx.prev_session_date:
        print(f"PIVOT_SESSION_DATE: {ctx.prev_session_date}")

    # 2) Pivots crossed
    dir_s = "UP" if direction > 0 else "DOWN"
    print(f"CROSS {dir_s}: {p1.name}({fmt_num(p1.value)}) -> {p2.name}({fmt_num(p2.value)})")

    # 3) Dict-like string in your required format:
    #    - epoch as list
    #    - stoploss as midpoint price
    epoch_list_str = f"[{fmt_num(p2.value)}]"
    target_str = fmt_num(target.value) if target else "None"

    print(
        "{"
        f"'stock': '{ctx.symbol}', "
        f"'epoch': {epoch_list_str}, "
        f"'stoploss': {fmt_num(stoploss_price)}, "
        f"'target': {target_str}"
        "}"
    )

    # 4) Beep
    beep()

    # Mark stock as done => never repeat
    ctx.done = True


# ==============================================================================
# CORE CROSSING LOGIC
# ==============================================================================

def process_tick(ctx: StockCtx, ts_ms: int, ltp: float) -> None:
    """
    Incorporate one new tick and detect when LTP crosses the next adjacent pivot.

    Adjacent crossing conditions:
      - Upward cross of level v:   prev < v <= cur
      - Downward cross of level v: prev > v >= cur

    Gap jumps:
      If LTP jumps across multiple pivot levels in one tick, we advance the anchor
      across each crossed adjacent level.
      - If EMIT_INTERMEDIATE_CROSSES=True, it could emit multiple (but we still stop
        at first emit because ctx.done=True).
      - If EMIT_INTERMEDIATE_CROSSES=False, it emits only the final crossing.
    """
    # If stock already alerted, we can skip all work
    if ctx.done:
        ctx.last_ts_ms = ts_ms
        ctx.last_ltp = ltp
        return

    # First tick for this stock (should be rare because we init from latest tick)
    if ctx.last_ltp is None:
        ctx.anchor_idx = init_anchor_from_ltp(ctx.levels, ltp)
        ctx.anchor_minute = minute_bucket(ts_ms)
        ctx.last_ltp = ltp
        ctx.last_ts_ms = ts_ms
        return

    prev = ctx.last_ltp
    cur = ltp

    # No change => no crossing
    if cur == prev:
        ctx.last_ts_ms = ts_ms
        ctx.last_ltp = cur
        return

    direction = 1 if cur > prev else -1

    # Normalize anchor bounds defensively
    if ctx.anchor_idx < -1:
        ctx.anchor_idx = -1
    if ctx.anchor_idx >= len(ctx.levels):
        ctx.anchor_idx = len(ctx.levels) - 1

    # If we suppress intermediate crosses, we remember the last crossing only.
    last_cross: Optional[Tuple[int, int, int]] = None  # (p1_idx, p2_idx, direction)

    while True:
        if ctx.done:
            break  # once done, stop processing for this stock immediately

        if direction > 0:
            # next pivot up is anchor+1
            nxt = ctx.anchor_idx + 1
            if nxt >= len(ctx.levels):
                break

            v = ctx.levels[nxt].value
            if prev < v <= cur:
                p1 = ctx.anchor_idx
                p2 = nxt

                # Only emit if pivot1 is a real pivot level
                # (anchor=-1 means "below the ladder")
                if 0 <= p1 < len(ctx.levels):
                    last_cross = (p1, p2, direction)
                    if EMIT_INTERMEDIATE_CROSSES:
                        emit_event_once(ctx, p1, p2, direction, ts_ms)

                # Update anchor to pivot2 (we have "arrived" at pivot2)
                ctx.anchor_idx = p2
                ctx.anchor_minute = minute_bucket(ts_ms)
                continue

            break

        else:
            # next pivot down is anchor-1
            nxt = ctx.anchor_idx - 1
            if nxt < 0:
                break

            v = ctx.levels[nxt].value
            if prev > v >= cur:
                p1 = ctx.anchor_idx
                p2 = nxt

                if 0 <= p1 < len(ctx.levels):
                    last_cross = (p1, p2, direction)
                    if EMIT_INTERMEDIATE_CROSSES:
                        emit_event_once(ctx, p1, p2, direction, ts_ms)

                ctx.anchor_idx = p2
                ctx.anchor_minute = minute_bucket(ts_ms)
                continue

            break

    # If intermediate crosses suppressed, emit only the final crossing (if any)
    if (not EMIT_INTERMEDIATE_CROSSES) and (last_cross is not None) and (not ctx.done):
        p1, p2, d = last_cross
        emit_event_once(ctx, p1, p2, d, ts_ms)

    # Persist last tick
    ctx.last_ts_ms = ts_ms
    ctx.last_ltp = cur


# ==============================================================================
# SQLITE ITERATION WITH RETRIES (important for writer+watcher concurrency)
# ==============================================================================

def iter_new_ticks_with_retries(conn: sqlite3.Connection, inst: str, ctx: StockCtx) -> Tuple[bool, int]:
    """
    Iterates new ticks for (inst) with retries on transient SQLite locks.

    Returns:
      (had_any_rows, rows_processed)

    Key detail:
      We re-read ctx.last_ts_ms on each retry to avoid duplicates if a lock happened
      mid-stream after we already processed some ticks.
    """
    q_new = "SELECT ts_ms, ltp FROM ticks WHERE instrument=? AND ts_ms>? ORDER BY ts_ms ASC"

    for attempt in range(1, SQLITE_RETRIES + 1):
        try:
            had_any = False
            n = 0

            # IMPORTANT: use the *current* ctx.last_ts_ms each attempt
            cursor = conn.execute(q_new, (inst, ctx.last_ts_ms))
            for ts_ms, ltp in cursor:
                had_any = True
                n += 1
                process_tick(ctx, int(ts_ms), float(ltp))
                if ctx.done:
                    # Once done, no need to drain remaining rows for this stock
                    break

            return had_any, n

        except sqlite3.OperationalError as e:
            if is_transient_sqlite_lock(e):
                # backoff and retry
                sleep_s = SQLITE_BACKOFF_BASE_SEC * attempt
                time.sleep(sleep_s)
                continue
            # non-transient error: raise
            raise

    # If we exhausted retries, skip this instrument this cycle
    return False, 0


# ==============================================================================
# MAIN
# ==============================================================================

def open_sqlite_readonly(db_path: str) -> sqlite3.Connection:
    """
    Open SQLite in read-only mode using URI.
    This reduces risk of accidental writes and typically plays nicer with WAL.

    If URI open fails (some environments), fallback to normal connect + query_only.
    """
    try:
        uri = f"file:{os.path.abspath(db_path)}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
    except Exception:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.execute("PRAGMA query_only=1;")

    conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS};")
    return conn


def main():
    db_path = DB_PATH or pick_latest_db(CACHE_DIR)
    log("INFO", f"Using DB: {db_path}")

    conn = open_sqlite_readonly(db_path)

    # Wait until universe+pivots exist (writer may still be preparing)
    while True:
        try:
            ctx_map = load_universe_and_pivots(conn)
            break
        except Exception as e:
            log("WARN", f"Waiting for universe+pivots to be ready: {e}")
            time.sleep(2.0)

    # Initialize from latest ticks so we don't replay history
    init_from_latest_ticks(conn, ctx_map)
    log("INFO", "Initialization complete. Starting scan loop...")

    try:
        while True:
            any_new = False

            # Loop over all instruments; skip those already "done" to save CPU/DB work
            for inst, ctx in ctx_map.items():
                if ctx.done:
                    continue
                try:
                    had_any, _ = iter_new_ticks_with_retries(conn, inst, ctx)
                    any_new = any_new or had_any
                except sqlite3.OperationalError as e:
                    # non-transient OperationalError
                    log("ERROR", f"SQLite OperationalError on {inst}: {e}")

            # Sleep: if nothing new, slightly longer to reduce CPU churn
            if any_new:
                time.sleep(POLL_INTERVAL_SEC)
            else:
                time.sleep(min(1.5, POLL_INTERVAL_SEC + 0.3))

    except KeyboardInterrupt:
        log("WARN", "Stopped by user (KeyboardInterrupt).")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
