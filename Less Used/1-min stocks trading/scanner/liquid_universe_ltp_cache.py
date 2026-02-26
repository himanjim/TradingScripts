# liquid_universe_ltp_cache.py
# ==============================================================================
# PURPOSE
#   Build an intraday cache for a liquid universe:
#
#   1) Read a CSV (~1000 stocks): exchange, tradingsymbol [optional instrument_token]
#   2) Around ~09:20 IST, select TOP_N most liquid by bid-ask spread:
#        - take QUOTE_SNAPSHOTS snapshots via kite.quote()
#        - compute spread_bps each snapshot
#        - aggregate per stock using MEDIAN spread_bps
#        - rank: spread_bps ASC, then turnover DESC, then total_depth_qty DESC
#   3) Compute CPR pivots from previous session OHLC (needs daily candles via historical API).
#   4) Backfill today's 1-min candles (09:15 -> now) via historical API to cover "missed interval".
#      (This does NOT recreate per-second ticks; historical is only minute granularity.)
#   5) Live loop: every 1 second, call kite.ltp() ONCE for all TOP_N and store ticks:
#        ticks(instrument, ts_ms, ltp)
#   6) On restart, reset the DB and rebuild from scratch.
#
# STORAGE
#   SQLite WAL DB (fast reads for another scanning script).
#
# RATE LIMITS (guidance)
#   - Bulk LTP / Quote supports up to 1000 instruments per request.  (docs)  [we use only 100]
#   - Historical: commonly capped at ~3 req/sec. (forum guidance)
#   - LTP: commonly 1 request per second. (forum guidance)
# ==============================================================================

import os
import time
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import Dict, Any, List, Optional, Tuple, Iterable

import pandas as pd
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed

from kiteconnect import KiteConnect, exceptions as kite_ex

# Your helper used in your existing scripts (auth/session creation).
import Trading_2024.OptionTradeUtils as oUtils


# ==============================================================================
# USER CONFIG (edit only this section)
# ==============================================================================

INPUT_CSV = r"C:\Users\Local User\Downloads\stock_list.csv"

TOP_N = 100

# Liquidity scan (quote snapshots)
QUOTE_SNAPSHOTS = 5            # 3–5 is robust
QUOTE_GAP_SEC = 1.5            # pause between snapshots
QUOTE_BATCH_SIZE = 250         # safe; avoids huge URLs in some clients
QUOTE_RPS = 1.0                # conservative

# Historical fetching (global limiter)
HIST_RPS = 3.0                 # global requests/sec for historical_data
HIST_MAX_WORKERS = 10          # threads; limiter enforces actual RPS

# Live polling
LTP_INTERVAL_SEC = 1.0         # one poll per second
LTP_RPS = 1.0                  # one ltp() request per second

# Market time (IST)
TZ = pytz.timezone("Asia/Kolkata")
MARKET_OPEN_HHMM = (9, 15)
MARKET_CLOSE_HHMM = (15, 30)

# Storage
OUTPUT_DIR = "live_cache"
RESET_ON_START = True

# SQLite tick flush batching
TICK_FLUSH_EVERY_N_ROWS = 600   # 100 ticks/sec => ~6 sec batches
TICK_FLUSH_EVERY_SEC = 6.0

# Resilience / retries
MAX_RETRIES = 3
RETRY_SLEEP_BASE_SEC = 2.0


# ==============================================================================
# LOGGING
# ==============================================================================

def log(level: str, msg: str) -> None:
    now = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
    print(f"{now} [{level}] {msg}")


# ==============================================================================
# GLOBAL RATE LIMITER (thread-safe)
# ==============================================================================

class RateLimiter:
    """
    Enforces a minimum interval between calls, globally across threads.
    Example:
      rps=3 -> min_interval ~0.333s between requests.
    """
    def __init__(self, rps: float):
        self.min_interval = 1.0 / max(rps, 0.1)
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.time()
            elapsed = now - self._last
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self._last = time.time()


# ==============================================================================
# API call wrapper with retries
# ==============================================================================

def api_call_with_retries(fn, *args, limiter: Optional[RateLimiter] = None, **kwargs):
    """
    Execute a Kite API call with:
      - optional global rate limiting
      - retries on transient failures (network/timeouts/general temporary errors)
      - TokenException is raised immediately (auth/session invalid)
    """
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if limiter:
                limiter.wait()
            return fn(*args, **kwargs)
        except (kite_ex.NetworkException, kite_ex.TimeoutException) as e:
            last_err = e
            log("WARN", f"{fn.__name__} network/timeout attempt {attempt}/{MAX_RETRIES}: {e}")
        except kite_ex.TokenException:
            raise
        except Exception as e:
            last_err = e
            log("WARN", f"{fn.__name__} error attempt {attempt}/{MAX_RETRIES}: {e}")

        time.sleep(RETRY_SLEEP_BASE_SEC * attempt)

    raise RuntimeError(f"{fn.__name__} failed after {MAX_RETRIES} retries. Last error: {last_err}")


# ==============================================================================
# CSV loading + instrument token mapping
# ==============================================================================

def load_universe(csv_path: str) -> pd.DataFrame:
    """
    Required CSV columns (case-insensitive):
      exchange, tradingsymbol
    Optional:
      instrument_token  (recommended)
    """
    df = pd.read_csv(csv_path, dtype=str)
    cols = {c.lower(): c for c in df.columns}

    if "exchange" not in cols or "tradingsymbol" not in cols:
        raise ValueError("CSV must have columns: exchange, tradingsymbol (case-insensitive).")

    df["exchange"] = df[cols["exchange"]].astype(str).str.upper().str.strip()
    df["tradingsymbol"] = df[cols["tradingsymbol"]].astype(str).str.strip()

    if "instrument_token" in cols:
        df["instrument_token"] = pd.to_numeric(df[cols["instrument_token"]], errors="coerce").astype("Int64")
    else:
        df["instrument_token"] = pd.NA

    df = df.dropna(subset=["exchange", "tradingsymbol"])
    df = df.drop_duplicates(subset=["exchange", "tradingsymbol"]).reset_index(drop=True)

    df["instrument"] = df["exchange"] + ":" + df["tradingsymbol"]
    log("INFO", f"Loaded universe rows: {len(df)}")
    return df[["exchange", "tradingsymbol", "instrument", "instrument_token"]]


def ensure_tokens(kite: KiteConnect, df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure instrument_token exists for every row.
    If missing for any row, fetch kite.instruments() once and map by "EXCHANGE:TRADINGSYMBOL".
    """
    missing = int(df["instrument_token"].isna().sum())
    if missing == 0:
        df["instrument_token"] = df["instrument_token"].astype(int)
        log("INFO", "instrument_token present for all rows (from CSV).")
        return df

    log("STEP", f"instrument_token missing for {missing} rows. Fetching kite.instruments() once for mapping...")
    inst = kite.instruments()
    inst_df = pd.DataFrame(inst)
    inst_df["exchange"] = inst_df["exchange"].astype(str).str.upper()
    inst_df["tradingsymbol"] = inst_df["tradingsymbol"].astype(str)
    inst_df["instrument"] = inst_df["exchange"] + ":" + inst_df["tradingsymbol"]

    key_to_token = dict(zip(inst_df["instrument"], inst_df["instrument_token"]))

    mask = df["instrument_token"].isna()
    df.loc[mask, "instrument_token"] = df.loc[mask, "instrument"].map(key_to_token)

    before = len(df)
    df = df.dropna(subset=["instrument_token"]).reset_index(drop=True)
    after = len(df)

    df["instrument_token"] = df["instrument_token"].astype(int)
    log("INFO", f"Mapped tokens. Kept {after}/{before} rows (dropped {before-after} unmapped).")
    return df


# ==============================================================================
# Liquidity scan (spread-first)
# ==============================================================================

def chunked(lst: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def _best_price(depth_side: List[Dict[str, Any]]) -> Optional[float]:
    if not depth_side:
        return None
    p = depth_side[0].get("price")
    return float(p) if p is not None else None


def _sum_qty(depth_side: List[Dict[str, Any]]) -> int:
    return int(sum(int(level.get("quantity", 0) or 0) for level in depth_side))


def compute_spread_bps(symbol: str, q: Dict[str, Any]) -> Dict[str, Any]:
    """
    spread_bps = (ask - bid)/mid * 10000
    Also capture turnover and depth for tie-breakers.
    """
    exch, ts = symbol.split(":", 1)
    last_price = float(q.get("last_price") or 0.0)
    volume = int(q.get("volume") or 0)

    depth = q.get("depth") or {}
    buy_depth = depth.get("buy") or []
    sell_depth = depth.get("sell") or []

    best_bid = _best_price(buy_depth)
    best_ask = _best_price(sell_depth)

    bid_qty = _sum_qty(buy_depth)
    ask_qty = _sum_qty(sell_depth)
    total_depth_qty = bid_qty + ask_qty

    if best_bid is not None and best_ask is not None and best_bid > 0 and best_ask > 0 and best_ask >= best_bid:
        mid = (best_bid + best_ask) / 2.0
        spread_bps = ((best_ask - best_bid) / mid) * 10_000.0
    else:
        spread_bps = float("inf")

    turnover = last_price * float(volume)  # proxy; use as tie-breaker only

    return {
        "instrument": symbol,
        "exchange": exch,
        "tradingsymbol": ts,
        "spread_bps": spread_bps,
        "turnover": turnover,
        "total_depth_qty": total_depth_qty,
        "last_price": last_price,
        "volume": volume,
    }


def liquidity_scan_topN(kite: KiteConnect, universe_keys: List[str], top_n: int) -> pd.DataFrame:
    """
    Take QUOTE_SNAPSHOTS snapshots and compute MEDIAN spread_bps per instrument.
    Rank: spread ASC, turnover DESC, depth DESC.
    """
    quote_limiter = RateLimiter(QUOTE_RPS)
    rows: List[Dict[str, Any]] = []

    for snap in range(1, QUOTE_SNAPSHOTS + 1):
        log("INFO", f"Liquidity snapshot {snap}/{QUOTE_SNAPSHOTS}")

        for batch in chunked(universe_keys, QUOTE_BATCH_SIZE):
            quotes = api_call_with_retries(kite.quote, batch, limiter=quote_limiter)

            for sym in batch:
                q = quotes.get(sym)
                if not q:
                    rows.append({
                        "instrument": sym,
                        "exchange": sym.split(":")[0],
                        "tradingsymbol": sym.split(":")[1],
                        "spread_bps": float("inf"),
                        "turnover": 0.0,
                        "total_depth_qty": 0,
                        "last_price": 0.0,
                        "volume": 0,
                    })
                else:
                    rows.append(compute_spread_bps(sym, q))

        if snap < QUOTE_SNAPSHOTS:
            time.sleep(QUOTE_GAP_SEC)

    df = pd.DataFrame(rows)
    agg = df.groupby("instrument", as_index=False).agg(
        exchange=("exchange", "first"),
        tradingsymbol=("tradingsymbol", "first"),
        spread_bps=("spread_bps", "median"),
        turnover=("turnover", "median"),
        total_depth_qty=("total_depth_qty", "median"),
        last_price=("last_price", "median"),
        volume=("volume", "median"),
    )

    agg = agg.sort_values(
        by=["spread_bps", "turnover", "total_depth_qty"],
        ascending=[True, False, False],
    ).reset_index(drop=True)

    top = agg.head(top_n).copy()
    log("INFO", f"Selected top {len(top)} liquid symbols.")
    return top


# ==============================================================================
# CPR pivots (previous session OHLC)
# ==============================================================================

@dataclass
class CPR:
    P: float
    BC: float
    TC: float
    R1: float
    S1: float
    CPR_lower: float
    CPR_upper: float


def compute_cpr(H: float, L: float, C: float) -> CPR:
    P = (H + L + C) / 3.0
    BC = (H + L) / 2.0
    TC = 2.0 * P - BC
    R1 = 2.0 * P - L
    S1 = 2.0 * P - H
    return CPR(P, BC, TC, R1, S1, min(BC, TC), max(BC, TC))


def market_open_dt(d: date) -> datetime:
    hh, mm = MARKET_OPEN_HHMM
    return TZ.localize(datetime(d.year, d.month, d.day, hh, mm, 0))


def market_close_dt(d: date) -> datetime:
    hh, mm = MARKET_CLOSE_HHMM
    return TZ.localize(datetime(d.year, d.month, d.day, hh, mm, 0))


def prev_trading_day_ohlc(kite: KiteConnect, token: int, today_ist: date, hist_limiter: RateLimiter) -> Tuple[date, float, float, float]:
    """
    Fetch last ~12 calendar days daily candles and pick most recent candle < today_ist.
    """
    end_dt = TZ.localize(datetime(today_ist.year, today_ist.month, today_ist.day, 23, 59, 0))
    start_dt = end_dt - timedelta(days=12)

    candles = api_call_with_retries(
        kite.historical_data,
        token,
        start_dt,
        end_dt,
        "day",
        continuous=False,
        oi=False,
        limiter=hist_limiter
    )

    usable = []
    for c in candles or []:
        dtc = c.get("date")
        if not dtc:
            continue
        if dtc.tzinfo is None:
            dtc = TZ.localize(dtc)
        else:
            dtc = dtc.astimezone(TZ)

        if dtc.date() < today_ist:
            usable.append((dtc, c))

    if not usable:
        raise RuntimeError(f"No previous-session daily candle for token={token}")

    usable.sort(key=lambda x: x[0])
    dtc, c = usable[-1]
    return dtc.date(), float(c["high"]), float(c["low"]), float(c["close"])


# ==============================================================================
# SQLite storage
# ==============================================================================

def init_db(db_path: str, reset: bool) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    if reset and os.path.exists(db_path):
        os.remove(db_path)

    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA cache_size=-200000;")
    conn.execute("PRAGMA busy_timeout=3000;")  # wait up to 3s if DB locked

    conn.execute("CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS universe (
            instrument TEXT PRIMARY KEY,
            exchange TEXT,
            tradingsymbol TEXT,
            instrument_token INTEGER,
            spread_bps REAL,
            turnover REAL,
            total_depth_qty REAL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS pivots (
            instrument TEXT PRIMARY KEY,
            prev_session_date TEXT,
            P REAL, BC REAL, TC REAL, R1 REAL, S1 REAL,
            CPR_lower REAL, CPR_upper REAL
        )
    """)

    # Optional minute backfill table (missed interval)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS candles_1m_hist (
            instrument TEXT,
            ts TEXT,
            o REAL, h REAL, l REAL, c REAL,
            v REAL,
            PRIMARY KEY (instrument, ts)
        )
    """)

    # Tick table: epoch milliseconds for performance + compactness
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ticks (
            instrument TEXT,
            ts_ms INTEGER,
            ltp REAL,
            PRIMARY KEY (instrument, ts_ms)
        )
    """)

    # IMPORTANT:
    # We do NOT add additional indexes on ticks(ts_ms) by default to reduce write overhead.
    # The PK index (instrument, ts_ms) already supports fast per-instrument range scans.

    conn.commit()
    return conn


def upsert_meta(conn: sqlite3.Connection, k: str, v: str) -> None:
    conn.execute("INSERT OR REPLACE INTO meta(k,v) VALUES (?,?)", (k, v))


# ==============================================================================
# Historical backfill (today 1-minute candles)
# ==============================================================================

def backfill_today_minutes(
    kite: KiteConnect,
    conn: sqlite3.Connection,
    selected: pd.DataFrame,
    start_dt: datetime,
    end_dt: datetime,
    hist_limiter: RateLimiter
) -> None:
    """
    Backfill today's minute candles for each selected instrument: start_dt -> end_dt.
    """
    if end_dt <= start_dt:
        log("INFO", "Backfill window empty; skipping.")
        return

    log("STEP", f"Backfilling 1-min candles: {start_dt} -> {end_dt} for {len(selected)} symbols")

    def fetch_one(token: int):
        return api_call_with_retries(
            kite.historical_data,
            token,
            start_dt,
            end_dt,
            "minute",
            continuous=False,
            oi=False,
            limiter=hist_limiter
        )

    # Insert incrementally as futures complete to avoid holding all rows in memory
    inserted_symbols = 0
    with ThreadPoolExecutor(max_workers=HIST_MAX_WORKERS) as ex:
        future_map = {
            ex.submit(fetch_one, int(r["instrument_token"])): r["instrument"]
            for _, r in selected.iterrows()
        }

        for fut in as_completed(future_map):
            inst = future_map[fut]
            try:
                candles = fut.result() or []
            except Exception as e:
                log("ERROR", f"Minute backfill failed for {inst}: {e}")
                continue

            rows = []
            for c in candles:
                dtc = c.get("date")
                if not dtc:
                    continue
                if dtc.tzinfo is None:
                    dtc = TZ.localize(dtc)
                else:
                    dtc = dtc.astimezone(TZ)

                ts = dtc.strftime("%Y-%m-%d %H:%M:%S%z")
                rows.append((
                    inst, ts,
                    float(c["open"]), float(c["high"]), float(c["low"]), float(c["close"]),
                    float(c.get("volume", 0.0) or 0.0)
                ))

            if rows:
                conn.executemany(
                    "INSERT OR REPLACE INTO candles_1m_hist(instrument, ts, o, h, l, c, v) VALUES (?,?,?,?,?,?,?)",
                    rows
                )
                conn.commit()

            inserted_symbols += 1
            if inserted_symbols % 10 == 0:
                log("INFO", f"Backfill progress: {inserted_symbols}/{len(selected)}")

    log("INFO", "Backfill complete.")


# ==============================================================================
# Pivot computation (parallel under HIST_RPS limiter)
# ==============================================================================

def compute_and_store_pivots(
    kite: KiteConnect,
    conn: sqlite3.Connection,
    selected: pd.DataFrame,
    hist_limiter: RateLimiter
) -> None:
    """
    Compute pivots for all selected instruments in parallel but globally rate-limited.
    """
    today_ist = datetime.now(TZ).date()
    log("STEP", f"Computing pivots for {len(selected)} instruments (parallel, rate-limited)...")

    def work(inst: str, token: int):
        prev_d, H, L, C = prev_trading_day_ohlc(kite, token, today_ist, hist_limiter)
        cpr = compute_cpr(H, L, C)
        return (
            inst, prev_d.isoformat(),
            cpr.P, cpr.BC, cpr.TC, cpr.R1, cpr.S1, cpr.CPR_lower, cpr.CPR_upper
        )

    rows = []
    with ThreadPoolExecutor(max_workers=HIST_MAX_WORKERS) as ex:
        futures = [
            ex.submit(work, r["instrument"], int(r["instrument_token"]))
            for _, r in selected.iterrows()
        ]

        done = 0
        for fut in as_completed(futures):
            try:
                rows.append(fut.result())
            except Exception as e:
                log("ERROR", f"Pivot computation failed: {e}")

            done += 1
            if done % 20 == 0:
                log("INFO", f"Pivot progress: {done}/{len(selected)}")

    if rows:
        conn.executemany(
            "INSERT OR REPLACE INTO pivots(instrument, prev_session_date, P, BC, TC, R1, S1, CPR_lower, CPR_upper) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            rows
        )
        conn.commit()

    log("INFO", "Pivots stored.")


def ltp_with_retries(kite: KiteConnect, instruments: List[str], limiter: RateLimiter):
    last_err = None
    for attempt in range(1, 3):  # only 2 attempts; we don't want to stall the 1s cadence too long
        try:
            limiter.wait()
            return kite.ltp(instruments)
        except (kite_ex.NetworkException, kite_ex.TimeoutException) as e:
            last_err = e
            log("WARN", f"ltp network/timeout attempt {attempt}/2: {e}")
        except kite_ex.TokenException:
            raise
        except Exception as e:
            last_err = e
            log("WARN", f"ltp error attempt {attempt}/2: {e}")
        time.sleep(1.0 * attempt)
    raise RuntimeError(f"ltp failed after 2 retries. Last error: {last_err}")


# ==============================================================================
# Live LTP loop (ticks only)
# ==============================================================================

def run_live_ltp_loop(kite: KiteConnect, conn: sqlite3.Connection, instruments: List[str]) -> None:
    """
    Poll kite.ltp(instruments) once per second and store ticks:
      ticks(instrument, ts_ms, ltp)

    Scheduling:
      Uses monotonic time to avoid drift over long sessions.
    """
    limiter = RateLimiter(LTP_RPS)

    tick_buffer: List[Tuple[str, int, float]] = []
    last_flush = time.monotonic()

    today_ist = datetime.now(TZ).date()
    open_dt = market_open_dt(today_ist)
    close_dt = market_close_dt(today_ist)

    log("STEP", f"Starting LTP loop for {len(instruments)} instruments.")
    log("INFO", f"Market window: {open_dt.strftime('%H:%M:%S')} -> {close_dt.strftime('%H:%M:%S')} IST")

    next_fire = time.monotonic()  # monotonic schedule anchor

    try:
        while True:
            now_ist = datetime.now(TZ)

            # Before market open: just wait
            if now_ist < open_dt:
                time.sleep(1.0)
                continue

            # Stop after close (+ buffer)
            if now_ist >= close_dt + timedelta(minutes=1):
                log("INFO", "Market close reached. Stopping LTP loop.")
                break

            limiter.wait()
            ts_ms = int(now_ist.timestamp() * 1000)  # epoch milliseconds (UTC-based)

            try:
                data = ltp_with_retries(kite, instruments, limiter)
            except Exception as e:
                log("WARN", f"kite.ltp failed: {e}. Backing off 2s.")
                time.sleep(2.0)
                continue

            # Append ticks
            for inst in instruments:
                q = data.get(inst)
                if not q:
                    continue
                ltp = float(q.get("last_price") or 0.0)
                if ltp > 0:
                    tick_buffer.append((inst, ts_ms, ltp))

            # Flush if needed (batch size or elapsed time)
            now_m = time.monotonic()
            if len(tick_buffer) >= TICK_FLUSH_EVERY_N_ROWS or (now_m - last_flush) >= TICK_FLUSH_EVERY_SEC:
                try:
                    conn.executemany(
                        "INSERT OR IGNORE INTO ticks(instrument, ts_ms, ltp) VALUES (?,?,?)",
                        tick_buffer
                    )
                    conn.commit()
                    tick_buffer.clear()
                    last_flush = now_m
                except sqlite3.OperationalError as e:
                    log("ERROR", f"SQLite OperationalError on flush: {e}. Keeping buffer; retry next cycle.")
                    # keep buffer; do NOT clear
                except Exception as e:
                    log("ERROR", f"SQLite error on flush: {e}. Keeping buffer; retry next cycle.")

            # Drift-free sleep until next second tick
            next_fire += LTP_INTERVAL_SEC
            sleep_s = next_fire - time.monotonic()
            if sleep_s > 0:
                time.sleep(sleep_s)

    except KeyboardInterrupt:
        log("WARN", "KeyboardInterrupt: stopping LTP loop gracefully...")

    # Final flush
    if tick_buffer:
        conn.executemany("INSERT OR IGNORE INTO ticks(instrument, ts_ms, ltp) VALUES (?,?,?)", tick_buffer)
        conn.commit()

    log("INFO", "LTP loop finished; ticks flushed.")


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    today_str = datetime.now(TZ).strftime("%Y-%m-%d")
    db_path = os.path.join(OUTPUT_DIR, f"cache_{today_str}.sqlite")

    log("STEP", "Initializing Kite API via OptionTradeUtils.intialize_kite_api() ...")
    kite = oUtils.intialize_kite_api()
    log("INFO", "Kite API initialized.")

    # Load and token-map universe
    uni = load_universe(INPUT_CSV)
    uni = ensure_tokens(kite, uni)
    universe_keys = uni["instrument"].tolist()

    # Init DB
    conn = init_db(db_path, reset=RESET_ON_START)
    upsert_meta(conn, "created_at", datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S%z"))
    upsert_meta(conn, "source_csv", INPUT_CSV)
    conn.commit()

    # Liquidity scan to pick TOP_N
    log("STEP", f"Running liquidity scan on {len(universe_keys)} instruments...")
    top = liquidity_scan_topN(kite, universe_keys, TOP_N)

    # Join tokens back (needed for historical calls)
    top = top.merge(uni[["instrument", "instrument_token"]], on="instrument", how="left")
    top = top.dropna(subset=["instrument_token"]).reset_index(drop=True)
    top["instrument_token"] = top["instrument_token"].astype(int)

    # Store selected universe
    conn.executemany(
        "INSERT OR REPLACE INTO universe(instrument, exchange, tradingsymbol, instrument_token, spread_bps, turnover, total_depth_qty) "
        "VALUES (?,?,?,?,?,?,?)",
        [
            (
                r["instrument"], r["exchange"], r["tradingsymbol"], int(r["instrument_token"]),
                float(r["spread_bps"]), float(r["turnover"]), float(r["total_depth_qty"])
            )
            for _, r in top.iterrows()
        ]
    )
    conn.commit()

    # Save for inspection
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    top_csv = os.path.join(OUTPUT_DIR, f"top_{TOP_N}_liquid_{today_str}.csv")
    top.to_csv(top_csv, index=False)
    log("INFO", f"Saved top list: {top_csv}")

    # Historical limiter shared across pivots + minute backfill
    hist_limiter = RateLimiter(HIST_RPS)

    # Compute pivots (parallel)
    compute_and_store_pivots(kite, conn, top, hist_limiter)

    # Backfill today's minute candles (09:15 -> now) if already past open
    today_ist = datetime.now(TZ).date()
    open_dt = market_open_dt(today_ist)
    now_dt = datetime.now(TZ)
    if now_dt > open_dt:
        backfill_today_minutes(kite, conn, top, open_dt, now_dt, hist_limiter)
    else:
        log("INFO", "Now is before market open; skipping minute backfill.")

    # Live LTP loop: 1 call/sec for all TOP_N
    run_live_ltp_loop(kite, conn, top["instrument"].tolist())

    conn.close()
    log("INFO", f"Done. SQLite cache: {db_path}")


if __name__ == "__main__":
    main()
