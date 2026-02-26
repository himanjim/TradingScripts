import os
import time
from collections import deque
from datetime import datetime, date, time as dtime, timedelta
from typing import List, Dict, Tuple, Optional

import threading
import pandas as pd
from pandas.api.types import is_datetime64tz_dtype
from kiteconnect import exceptions as kite_ex

import Trading_2024.OptionTradeUtils as oUtils


# ========== USER CONFIG ==========

# CSV file with list of stocks
# Columns required: exchange, tradingsymbol
INPUT_CSV = "top_500_liquid_stocks.csv"

# Date range for historical data (DD-MM-YYYY)
START_DATE_STR = "09-12-2022"   # from this date (inclusive)
END_DATE_STR   = "09-12-2025"   # to this date (inclusive)

# Trading session times (IST)
SESSION_START = dtime(9, 15, 0)
SESSION_END   = dtime(15, 30, 0)

# Output folder for Parquet files (one per stock)
OUTPUT_DIR = "./stock_history_parquet"

# Historical API chunk size (1-min limit is ~60 days)
DAYS_PER_CHUNK = 60

# Zerodha historical API rate limit: 3 requests / second
HIST_MAX_CALLS_PER_SEC = 3

# Number of stocks to hit in parallel (threaded)
MAX_PARALLEL_STOCKS = 3  # make this smaller if you want to be conservative


# ========== LOGGING WITH TIMESTAMPS ==========

def log(level: str, msg: str):
    """
    Simple logger that prefixes messages with current timestamp.
    level: 'INFO', 'STEP', 'WARN', 'ERROR', 'WORKER', 'DEMO', etc.
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{now} [{level}] {msg}")


# ========== RATE LIMITER (THREAD-SAFE) ==========

class RateLimiter:
    """Simple thread-safe rate limiter: at most max_calls per 'per_seconds' window."""

    def __init__(self, max_calls: int, per_seconds: float):
        self.max_calls = max_calls
        self.per_seconds = per_seconds
        self.calls = deque()
        self.lock = threading.Lock()

    def wait(self):
        """Block until a slot is available, then record the call."""
        while True:
            with self.lock:
                now = time.time()
                # Drop old calls outside window
                while self.calls and self.calls[0] <= now - self.per_seconds:
                    self.calls.popleft()

                if len(self.calls) < self.max_calls:
                    self.calls.append(now)
                    return

                # Need to wait for oldest call to expire
                wait_for = self.per_seconds - (now - self.calls[0]) + 0.01

            if wait_for > 0:
                time.sleep(wait_for)


rate_limiter = RateLimiter(HIST_MAX_CALLS_PER_SEC, 1.0)


# ========== HELPER FUNCTIONS ==========

def parse_date_dmy(dstr: str) -> date:
    """Parse DD-MM-YYYY into date."""
    return datetime.strptime(dstr, "%d-%m-%Y").date()


def date_chunks(start_dt: datetime, end_dt: datetime, days_per_chunk: int = 30) -> List[Tuple[datetime, datetime]]:
    """Split [start_dt, end_dt] into inclusive chunks of at most days_per_chunk days."""
    chunks: List[Tuple[datetime, datetime]] = []
    cur_start = start_dt
    one_day = timedelta(days=1)
    while cur_start <= end_dt:
        cur_end = min(cur_start + timedelta(days=days_per_chunk) - one_day, end_dt)
        chunks.append((cur_start, cur_end))
        cur_start = cur_end + one_day
    return chunks


def load_stock_list(csv_path: str) -> pd.DataFrame:
    """
    Load list of stocks from CSV.
    Required columns: exchange, tradingsymbol
    """
    log("STEP", f"Reading stock list from CSV: {csv_path}")
    df = pd.read_csv(csv_path)
    if "exchange" not in df.columns or "tradingsymbol" not in df.columns:
        raise ValueError("CSV must contain columns: 'exchange', 'tradingsymbol'")
    df["exchange"] = df["exchange"].astype(str).str.upper().str.strip()
    df["tradingsymbol"] = df["tradingsymbol"].astype(str).str.strip()

    # Drop duplicates
    before = len(df)
    df = df.drop_duplicates(subset=["exchange", "tradingsymbol"]).reset_index(drop=True)
    after = len(df)
    log("INFO", f"Loaded {after} unique instruments (from {before} rows).")
    return df


def fetch_instruments_map(kite, exchanges: List[str]) -> Dict[str, List[Dict]]:
    """
    Fetch instruments for all required exchanges once.
    Returns dict: exchange -> list of instrument dicts.
    """
    inst_map: Dict[str, List[Dict]] = {}
    for ex in sorted(set(exchanges)):
        log("STEP", f"Fetching instruments for exchange={ex} ...")
        inst_map[ex] = kite.instruments(ex)
        log("INFO", f"Instruments on {ex}: {len(inst_map[ex])}")
    return inst_map


def resolve_instrument_token(inst_map: Dict[str, List[Dict]], exchange: str, tradingsymbol: str) -> int:
    """
    Resolve instrument_token for (exchange, tradingsymbol) using pre-fetched inst_map.
    """
    ex = exchange.upper().strip()
    ts = tradingsymbol.strip()
    instruments = inst_map.get(ex, [])
    matches = [r for r in instruments if str(r.get("tradingsymbol", "")).upper() == ts.upper()]
    if not matches:
        raise ValueError(f"Instrument not found: {ex}:{ts}")
    row = matches[0]
    token = int(row["instrument_token"])
    log("INFO", f"Resolved {ex}:{ts} → token={token}")
    return token


def safe_historical_data(
    kite,
    instrument_token: int,
    from_dt: datetime,
    to_dt: datetime,
    interval: str = "minute",
    label: str = "",
    max_retries: int = 5,
):
    """
    Historical call with global rate limiting + retries.
    Respects Zerodha's ~3 historical calls per second.
    """
    for attempt in range(1, max_retries + 1):
        rate_limiter.wait()  # ensure we never exceed max calls/sec

        try:
            return kite.historical_data(
                instrument_token=instrument_token,
                from_date=from_dt,
                to_date=to_dt,
                interval=interval,
                continuous=False,
                oi=False,
            )
        except kite_ex.NetworkException as e:
            wait = attempt * 2
            log("WARN", f"NetworkException on {label} (attempt {attempt}): {e}. Backing off {wait}s ...")
            time.sleep(wait)
        except Exception as e:
            wait = attempt * 2
            log("WARN", f"Error on {label} (attempt {attempt}): {e}. Backing off {wait}s ...")
            time.sleep(wait)

    raise RuntimeError(f"[ERROR] Failed to fetch historical data for {label} after {max_retries} attempts.")


def fetch_history_minute(
    kite,
    instrument_token: int,
    from_dt: datetime,
    to_dt: datetime,
    label: str = ""
) -> List[Dict]:
    """
    Fetch 1-minute historical data between from_dt and to_dt (inclusive), chunked to avoid limits.
    Uses safe_historical_data() with global rate limiter.
    """
    chunks = date_chunks(from_dt, to_dt, days_per_chunk=DAYS_PER_CHUNK)
    all_rows: List[Dict] = []

    log("INFO", f"Fetching 1-min candles for {label} in {len(chunks)} chunk(s).")

    for idx, (c_from, c_to) in enumerate(chunks, start=1):
        log("INFO", f"  [CHUNK {idx}/{len(chunks)}] {label} {c_from} → {c_to}")
        rows = safe_historical_data(
            kite,
            instrument_token=instrument_token,
            from_dt=c_from,
            to_dt=c_to,
            interval="minute",
            label=f"{label} [{c_from} - {c_to}]",
        )
        log("INFO", f"    Retrieved {len(rows)} rows for {label} in this chunk.")
        all_rows.extend(rows)

    log("INFO", f"Total rows for {label}: {len(all_rows)}")
    return all_rows


# ========== DATE NORMALIZATION HELPERS ==========

def normalize_date_series(s: pd.Series, ctx: str = "") -> pd.Series:
    """
    Normalize any 'date' series to tz-naive pandas.Timestamp.

    - Accepts Python datetimes (tz-aware or naive) or strings.
    - If tz-aware, drops timezone (keeps wall-clock).
    """
    s = pd.to_datetime(s)
    if is_datetime64tz_dtype(s):
        tz_info = s.dt.tz
        log("INFO", f"{ctx}: 'date' is tz-aware ({tz_info}); converting to tz-naive.")
        s = s.dt.tz_convert(None)
    return s


def _to_naive_ts(ts) -> pd.Timestamp:
    """
    Normalise any datetime-like scalar to a tz-naive pandas.Timestamp.
    """
    ts = pd.to_datetime(ts)
    if getattr(ts, "tz", None) is not None:
        ts = ts.tz_convert(None)
    return ts


# ========== DATAFRAME CONVERSION ==========

def rows_to_dataframe(rows: List[Dict]) -> pd.DataFrame:
    """Convert historical rows to a sorted DataFrame with the usual OHLC columns."""
    if not rows:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
    df = pd.DataFrame(rows)
    for col in ["date", "open", "high", "low", "close", "volume"]:
        if col not in df.columns:
            df[col] = None

    df["date"] = normalize_date_series(df["date"], ctx="rows_to_dataframe")
    df = df.sort_values("date").reset_index(drop=True)
    return df


# ---------- PARQUET HELPERS (PER-STOCK) ----------

def symbol_parquet_path(exchange: str, tradingsymbol: str) -> str:
    """Return the Parquet file path for a given stock."""
    safe_ex = exchange.upper()
    safe_ts = tradingsymbol.replace(" ", "_").replace(":", "_").replace("/", "_")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    return os.path.join(OUTPUT_DIR, f"{safe_ex}_{safe_ts}.parquet")


def load_symbol_df_if_exists(exchange: str, tradingsymbol: str) -> pd.DataFrame:
    """
    Load existing Parquet for a symbol if it exists; otherwise return empty DF.
    """
    path = symbol_parquet_path(exchange, tradingsymbol)
    if not os.path.exists(path):
        log("INFO", f"No existing Parquet for {exchange}:{tradingsymbol}. Will fetch full range.")
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    df = pd.read_parquet(path)
    if "date" not in df.columns:
        raise ValueError(f"Parquet file {path} missing 'date' column.")

    df["date"] = normalize_date_series(
        df["date"],
        ctx=f"load_symbol_df_if_exists {exchange}:{tradingsymbol}"
    )
    df = df.sort_values("date").reset_index(drop=True)

    log(
        "INFO",
        f"Existing Parquet for {exchange}:{tradingsymbol}: "
        f"{len(df)} rows ({df['date'].min()} → {df['date'].max()})"
    )
    return df


def compute_missing_ranges_for_symbol(
    existing_df: pd.DataFrame,
    desired_from: datetime,
    desired_to: datetime,
) -> List[Tuple[datetime, datetime]]:
    """
    For a given symbol and desired [desired_from, desired_to], figure out which
    datetime ranges are missing, based on existing_df (already on disk).
    """

    desired_from_n = _to_naive_ts(desired_from)
    desired_to_n   = _to_naive_ts(desired_to)

    if existing_df.empty:
        log(
            "INFO",
            f"compute_missing_ranges: no existing data; full range missing "
            f"{desired_from_n} → {desired_to_n}"
        )
        return [(desired_from_n, desired_to_n)]

    idx = normalize_date_series(existing_df["date"], ctx="compute_missing_ranges (existing)")
    existing_min = idx.min()
    existing_max = idx.max()

    log(
        "INFO",
        f"compute_missing_ranges: desired={desired_from_n} → {desired_to_n}, "
        f"existing={existing_min} → {existing_max}"
    )

    # If existing completely covers desired range, nothing to do
    if existing_min <= desired_from_n and existing_max >= desired_to_n:
        log("INFO", "compute_missing_ranges: existing range fully covers desired range → no missing ranges.")
        return []

    missing_ranges: List[Tuple[datetime, datetime]] = []
    one_min = timedelta(minutes=1)

    # Case: existing is completely before or after desired
    if existing_max < desired_from_n or existing_min > desired_to_n:
        log("INFO", "compute_missing_ranges: existing range is disjoint from desired → full desired missing.")
        return [(desired_from_n, desired_to_n)]

    # Missing before existing_min?
    if desired_from_n < existing_min:
        end = min(existing_min - one_min, desired_to_n)
        if desired_from_n <= end:
            missing_ranges.append((desired_from_n, end))

    # Missing after existing_max?
    if desired_to_n > existing_max:
        start = max(existing_max + one_min, desired_from_n)
        if start <= desired_to_n:
            missing_ranges.append((start, desired_to_n))

    log("INFO", f"compute_missing_ranges: missing ranges count = {len(missing_ranges)}")
    for i, (s, e) in enumerate(missing_ranges, 1):
        log("INFO", f"  missing[{i}] {s} → {e}")

    return missing_ranges


def save_symbol_parquet(exchange: str, tradingsymbol: str, df: pd.DataFrame):
    """
    Save the given DataFrame (date, OHLCV) to this symbol's Parquet file.
    """
    path = symbol_parquet_path(exchange, tradingsymbol)
    df = df.copy()
    df["date"] = normalize_date_series(
        df["date"],
        ctx=f"save_symbol_parquet {exchange}:{tradingsymbol}"
    )
    df = df.sort_values("date").reset_index(drop=True)
    df.to_parquet(path, index=False)
    log("STEP", f"Saved {len(df)} rows for {exchange}:{tradingsymbol} → {os.path.abspath(path)}")


# ========== PARALLEL WORKER (FETCH + MERGE + SAVE) ==========

def process_symbol_task(
    kite,
    exchange: str,
    tradingsymbol: str,
    instrument_token: int,
    missing_ranges: List[Tuple[datetime, datetime]],
) -> int:
    """
    Worker function executed in parallel threads for each symbol.

    - Loads existing Parquet for symbol
    - Fetches data only for missing_ranges
    - Merges, dedupes, saves Parquet
    - Returns number of *new* rows added
    """
    label = f"{exchange}:{tradingsymbol}"
    log("WORKER", f"Starting {label} with {len(missing_ranges)} missing range(s).")

    # Load current on-disk data for this symbol
    existing_df = load_symbol_df_if_exists(exchange, tradingsymbol)
    existing_rows_before = len(existing_df)

    all_dfs = []

    for i, (rng_from, rng_to) in enumerate(missing_ranges, start=1):
        log("WORKER", f"  [RANGE {i}/{len(missing_ranges)}] {label} {rng_from} → {rng_to}")
        rows = fetch_history_minute(
            kite,
            instrument_token=instrument_token,
            from_dt=rng_from,
            to_dt=rng_to,
            label=label,
        )
        df = rows_to_dataframe(rows)
        if df.empty:
            log("WORKER", f"    No data returned for {label} in this range, skipping.")
            continue

        all_dfs.append(df)
        log("WORKER", f"    Candles fetched for {label} in this range: {len(df)}")

    if not all_dfs:
        log("WORKER", f"No new data for {label} across all missing ranges; nothing to save.")
        return 0

    new_df = pd.concat(all_dfs, ignore_index=True)
    log("WORKER", f"Total new candles fetched for {label}: {len(new_df)}")

    # Merge with existing data
    combined = pd.concat([existing_df, new_df], ignore_index=True)
    combined["date"] = normalize_date_series(
        combined["date"],
        ctx=f"process_symbol_task {label}"
    )
    combined = combined.drop_duplicates(subset=["date"]).sort_values("date").reset_index(drop=True)

    new_total_rows = len(combined)
    newly_added = new_total_rows - existing_rows_before

    if newly_added <= 0:
        log("WORKER", f"{label}: after merge/dedup, no net new rows to save.")
        return 0

    save_symbol_parquet(exchange, tradingsymbol, combined)
    log("WORKER", f"{label}: existing_rows={existing_rows_before}, new_total_rows={new_total_rows}, newly_added={newly_added}")
    return newly_added


# ========== DEMO SAMPLE READER ==========

def demo_read_sample(
    exchange: str,
    tradingsymbol: str,
    start_dt_str: Optional[str] = None,
    end_dt_str: Optional[str] = None,
    n: int = 5,
):
    """
    Show a sample of data for a symbol from its Parquet file.
    """
    path = symbol_parquet_path(exchange, tradingsymbol)
    if not os.path.exists(path):
        log("DEMO", f"No Parquet file found for {exchange}:{tradingsymbol} at {path}")
        return

    log("DEMO", f"Loading Parquet for {exchange}:{tradingsymbol} from {path}")
    df = pd.read_parquet(path)

    if "date" not in df.columns:
        raise ValueError(f"Parquet file {path} missing 'date' column.")

    df["date"] = normalize_date_series(
        df["date"],
        ctx=f"demo_read_sample {exchange}:{tradingsymbol}"
    )
    df = df.sort_values("date").reset_index(drop=True)

    if not start_dt_str and not end_dt_str:
        log("DEMO", f"No datetime range provided. Showing last {n} candles:")
        print(df.tail(n))
        return

    start_dt = pd.to_datetime(start_dt_str) if start_dt_str else None
    end_dt = pd.to_datetime(end_dt_str) if end_dt_str else None

    if start_dt and end_dt:
        log("DEMO", f"Applying datetime range: {start_dt} → {end_dt}")
        sub = df[(df["date"] >= start_dt) & (df["date"] <= end_dt)]
    elif start_dt:
        log("DEMO", f"Applying datetime range from {start_dt} onwards")
        sub = df[df["date"] >= start_dt]
    elif end_dt:
        log("DEMO", f"Applying datetime range up to {end_dt}")
        sub = df[df["date"] <= end_dt]
    else:
        sub = df

    if sub.empty:
        log("DEMO", "No rows found in the specified datetime range.")
        return

    log("DEMO", f"Rows in specified range: {len(sub)}")
    if len(sub) <= n:
        print(sub)
    else:
        print("[DEMO] First few rows:")
        print(sub.head(min(n, 5)))
        print("\n[DEMO] Last few rows:")
        print(sub.tail(min(n, 5)))


# ========== MAIN SCRIPT ==========

def main():
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # Parse date range
    start_date = parse_date_dmy(START_DATE_STR)
    end_date = parse_date_dmy(END_DATE_STR)

    if start_date > end_date:
        raise ValueError("START_DATE must be on or before END_DATE")

    desired_from = datetime.combine(start_date, SESSION_START)
    desired_to = datetime.combine(end_date, SESSION_END)

    print("========================================================")
    log("CONFIG", f"Date range start (inclusive): {desired_from}")
    log("CONFIG", f"Date range end   (inclusive): {desired_to}")
    log("CONFIG", f"Input CSV: {INPUT_CSV}")
    log("CONFIG", f"Parquet output dir: {os.path.abspath(OUTPUT_DIR)}")
    log("CONFIG", f"Parallel symbols: {MAX_PARALLEL_STOCKS}")
    print("========================================================")

    # Initialise Kite
    log("STEP", "Initializing Kite API via OptionTradeUtils.intialize_kite_api() ...")
    kite = oUtils.intialize_kite_api()
    log("INFO", "Kite API initialized.")

    # Load stock list
    stock_df = load_stock_list(INPUT_CSV)
    total_universe = len(stock_df)

    # Fetch instruments once per exchange
    exchanges = stock_df["exchange"].unique().tolist()
    inst_map = fetch_instruments_map(kite, exchanges)

    # Build tasks only for symbols with missing ranges
    tasks = []
    skipped_full_covered = 0

    for idx, row in stock_df.iterrows():
        ex = row["exchange"]
        ts = row["tradingsymbol"]
        tag = f"{ex}:{ts}"

        existing_df = load_symbol_df_if_exists(ex, ts)
        missing_ranges = compute_missing_ranges_for_symbol(
            existing_df,
            desired_from,
            desired_to,
        )

        if not missing_ranges:
            log("INFO", f"[SKIP] {tag} already fully covered for {desired_from} → {desired_to}")
            skipped_full_covered += 1
            continue

        log("INFO", f"[TASK] {tag} requires {len(missing_ranges)} missing range(s).")

        try:
            token = resolve_instrument_token(inst_map, ex, ts)
        except Exception as e:
            log("ERROR", f"Skipping {tag}: {e}")
            continue

        tasks.append((ex, ts, token, missing_ranges))

    total_to_download = len(tasks)
    log(
        "STEP",
        f"Symbols needing downloads: {total_to_download} (out of {total_universe}). "
        f"Fully covered & skipped: {skipped_full_covered}"
    )

    saved_symbols_count = 0
    any_updates = False

    if tasks:
        log("STEP", "Starting ThreadPoolExecutor for symbol downloads ...")

        from concurrent.futures import ThreadPoolExecutor, as_completed

        with ThreadPoolExecutor(max_workers=MAX_PARALLEL_STOCKS) as executor:
            future_to_meta = {}
            for ex, ts, token, missing_ranges in tasks:
                tag = f"{ex}:{ts}"
                future = executor.submit(
                    process_symbol_task,
                    kite,
                    ex,
                    ts,
                    token,
                    missing_ranges,
                )
                future_to_meta[future] = (ex, ts, tag)

            for future in as_completed(future_to_meta):
                ex, ts, tag = future_to_meta[future]
                try:
                    newly_added = future.result()
                except Exception as e:
                    import traceback
                    log("ERROR", f"Worker failed for {tag}: {e}\n{traceback.format_exc()}")
                    continue

                if newly_added <= 0:
                    log("WARN", f"No new rows saved for {tag}.")
                    continue

                any_updates = True
                saved_symbols_count += 1
                remaining = total_to_download - saved_symbols_count
                log(
                    "INFO",
                    f"[SUMMARY] {tag}: newly_added={newly_added}. "
                    f"Stocks saved so far: {saved_symbols_count}/{total_to_download}. "
                    f"Remaining: {remaining}"
                )

    if not any_updates:
        log("INFO", "No new data downloaded. Existing Parquet files (if any) remain unchanged.")
        return

    log("INFO", f"Run complete. Total symbols updated (saved this run): {saved_symbols_count}")


if __name__ == "__main__":
    main()
    # Example demo after some data:
    # demo_read_sample('NSE', 'KAYNES', '2024-11-27 09:25:00', '2024-11-27 10:25:00')
