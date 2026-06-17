"""
Download 1-minute historical data for a list of NSE stocks (F&O universe),
3 workers in parallel, with a SHARED global rate limiter so the AGGREGATE
request rate stays within Kite's 3 requests/second on historical_data.

Run LOCALLY (needs your Kite session). One parquet per stock under OUTPUT_DIR.

WHY A SHARED LIMITER (important):
  Kite's 3 req/sec on historical_data is a GLOBAL limit for your session, not
  per-connection. If 3 threads each fire freely you'd hit ~3x the cap and get
  throttled/blocked. So all workers draw from ONE token bucket that releases at
  most RATE_PER_SEC permits/second across the whole process. Parallelism here
  buys overlap of network/wait time, not a higher request rate.

RESUME:
  A stock is considered done iff its .parquet already exists. Completed files
  are skipped before any work is queued, so an interrupted run resumes cleanly.
  Files are written tmp-then-rename, so a partial file never looks complete.

CAVEATS (unchanged): survivorship bias if using today's F&O list; stock 1-min
data has real volume (used downstream for liquidity filtering).
"""

import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date, timedelta, time as dtime
from typing import List, Tuple

import pandas as pd
import Trading_2024.OptionTradeUtils as oUtils

# ============================================================
# CONFIG
# ============================================================
EXCHANGE = "NSE"
OUTPUT_DIR = "./stocks_1min_history"
LOOKBACK_YEARS = 5
SESSION_START = dtime(9, 15)
SESSION_END = dtime(15, 30)
MAX_DAYS_PER_CHUNK = 25
MAX_ATTEMPTS = 5

NUM_WORKERS = 3            # parallel stocks
RATE_PER_SEC = 3.0        # GLOBAL cap on historical_data calls/sec (Kite limit)
SAFETY_FACTOR = 0.9       # stay a touch under the cap to be safe

SYMBOLS_FILE = "fno_symbols.txt"
SYMBOLS_INLINE = [
    "RELIANCE", "HDFCBANK", "ICICIBANK", "INFY", "TCS", "SBIN", "AXISBANK",
    "KOTAKBANK", "LT", "ITC", "BHARTIARTL", "HINDUNILVR", "BAJFINANCE",
]


# ============================================================
# SHARED RATE LIMITER (thread-safe, serializes call spacing)
# ============================================================
class RateLimiter:
    """At most `rate` acquisitions/second across ALL threads. Each acquire()
    blocks until its scheduled slot. Spacing is serialized under a lock so the
    aggregate rate is correct no matter how many workers call concurrently."""
    def __init__(self, rate_per_sec: float):
        self.min_interval = 1.0 / rate_per_sec
        self._lock = threading.Lock()
        self._next_time = time.monotonic()

    def acquire(self):
        with self._lock:
            now = time.monotonic()
            if self._next_time > now:
                time.sleep(self._next_time - now)
                now = time.monotonic()
            self._next_time = max(now, self._next_time) + self.min_interval


LIMITER = RateLimiter(RATE_PER_SEC * SAFETY_FACTOR)
_print_lock = threading.Lock()


def log(msg):
    with _print_lock:
        print(msg, flush=True)


# ============================================================
# HELPERS
# ============================================================
def load_symbols() -> List[str]:
    if os.path.exists(SYMBOLS_FILE):
        with open(SYMBOLS_FILE) as f:
            syms = [ln.strip().upper() for ln in f if ln.strip() and not ln.startswith("#")]
        log(f"[INFO] Loaded {len(syms)} symbols from {SYMBOLS_FILE}")
        return syms
    log(f"[INFO] {SYMBOLS_FILE} not found; using {len(SYMBOLS_INLINE)} inline symbols")
    return [s.upper() for s in SYMBOLS_INLINE]


def iter_chunks(from_dt, to_dt, days) -> List[Tuple[datetime, datetime]]:
    out, cur, end_d = [], from_dt.date(), to_dt.date()
    while cur <= end_d:
        ce = min(cur + timedelta(days=days - 1), end_d)
        cf = from_dt if cur == from_dt.date() else datetime.combine(cur, SESSION_START)
        ct = to_dt if ce == end_d else datetime.combine(ce, SESSION_END)
        out.append((cf, ct)); cur = ce + timedelta(days=1)
    return out


def resolve_tokens(kite, symbols) -> dict:
    inst = kite.instruments(EXCHANGE)        # 1 call, not rate-critical
    want = {s.upper() for s in symbols}
    tok = {}
    for r in inst:
        ts = str(r.get("tradingsymbol", "")).upper()
        if ts in want and str(r.get("instrument_type", "")).upper() == "EQ":
            tok[ts] = int(r["instrument_token"])
    missing = want - set(tok)
    if missing:
        log(f"[WARN] {len(missing)} symbols not resolved: {sorted(missing)[:10]}...")
    return tok


def fetch_one(kite, token, from_dt, to_dt, label) -> pd.DataFrame:
    rows = []
    for i, (cf, ct) in enumerate(iter_chunks(from_dt, to_dt, MAX_DAYS_PER_CHUNK), 1):
        for attempt in range(1, MAX_ATTEMPTS + 1):
            LIMITER.acquire()                 # <-- global throttle, every call
            try:
                rows.extend(kite.historical_data(token, cf, ct, "minute", False, False))
                break
            except Exception as e:
                if attempt == MAX_ATTEMPTS:
                    log(f"  [ERR] {label} chunk {i}: {e}")
                time.sleep(min(8.0, 1.5 * attempt))
    if not rows:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    try:
        if df["date"].dt.tz is not None:
            df["date"] = df["date"].dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    except Exception:
        pass
    return df.drop_duplicates("date").sort_values("date").reset_index(drop=True)[
        ["date", "open", "high", "low", "close", "volume"]]


def worker(kite, sym, token, from_dt, to_dt, idx, total) -> str:
    out_path = os.path.join(OUTPUT_DIR, f"{sym}.parquet")
    tmp_path = out_path + ".tmp"
    try:
        df = fetch_one(kite, token, from_dt, to_dt, sym)
        if df.empty:
            log(f"[{idx}/{total}] {sym}: no data")
            return sym
        df.to_parquet(tmp_path, index=False)   # write tmp then atomic rename
        os.replace(tmp_path, out_path)
        log(f"[{idx}/{total}] {sym}: {len(df)} candles -> {out_path}")
    except Exception as e:
        log(f"[{idx}/{total}] {sym}: FAILED {e}")
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
    return sym


# ============================================================
# MAIN
# ============================================================
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    symbols = load_symbols()
    kite = oUtils.intialize_kite_api()
    tokens = resolve_tokens(kite, symbols)
    log(f"[INFO] Resolved {len(tokens)} tokens")

    to_dt = datetime.combine(date.today(), SESSION_END)
    from_dt = datetime.combine(date.today() - timedelta(days=365 * LOOKBACK_YEARS), SESSION_START)

    # filter out already-done BEFORE queuing (resume support)
    todo = []
    for sym, token in sorted(tokens.items()):
        if os.path.exists(os.path.join(OUTPUT_DIR, f"{sym}.parquet")):
            log(f"[SKIP] {sym}: already downloaded")
            continue
        todo.append((sym, token))
    total = len(todo)
    log(f"[INFO] {total} stocks to download, {NUM_WORKERS} workers, "
        f"global cap {RATE_PER_SEC*SAFETY_FACTOR:.1f} req/s")

    if not todo:
        log("[DONE] nothing to download (all present).")
        return

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as ex:
        futures = {ex.submit(worker, kite, sym, token, from_dt, to_dt, i, total): sym
                   for i, (sym, token) in enumerate(todo, 1)}
        done = 0
        for fut in as_completed(futures):
            done += 1
            if done % 10 == 0:
                el = time.time() - t0
                log(f"[PROGRESS] {done}/{total} done, {el/60:.1f} min elapsed, "
                    f"~{el/done*(total-done)/60:.1f} min remaining")
    log(f"[SUCCESS] {total} stocks in {(time.time()-t0)/60:.1f} min")


if __name__ == "__main__":
    main()
