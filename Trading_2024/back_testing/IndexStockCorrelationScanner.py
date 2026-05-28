"""
IndexStockCorrelationScanner.py
================================

Purpose
-------
Download the last 1 year of historical OHLCV data for:
    1) NIFTY 50 index
    2) SENSEX index
    3) BANKNIFTY / NIFTY BANK index
    4) All NIFTY 50 stocks

Then compute pairwise return-correlation metrics among all instruments.
With 3 indices + 50 stocks, this creates 53 instruments and 53C2 = 1,378 pairs.

This script follows the style of your existing Kite downloader:
    - initializes Kite through Trading_2024.OptionTradeUtils.intialize_kite_api()
    - caches Kite instrument dumps
    - resolves instrument_token by exchange + tradingsymbol
    - downloads historical data in chunks
    - retries failed API calls
    - de-duplicates candles by timestamp
    - saves reusable local candle files
    - writes a final Excel/CSV report

Important trading-statistics point
----------------------------------
Correlation is computed on LOG RETURNS, not raw prices.
Raw price correlation is often misleading because two rising instruments can show
high price correlation even when the tradable return relationship is unstable.

Default interval
----------------
The default interval is 'day' because it is much faster and is sufficient for the
first screening pass.

For intraday research, set:
    INTERVAL=minute
or:
    INTERVAL=5minute

One year of 1-minute data for 53 instruments means many API calls. The script has
resume support, so interrupted runs can continue without re-downloading completed
symbols.

Typical commands
----------------
Daily screening:
    python IndexStockCorrelationScanner.py

1-minute screening:
    set INTERVAL=minute
    set ROLLING_WINDOW=375
    python IndexStockCorrelationScanner.py

5-minute screening:
    set INTERVAL=5minute
    set ROLLING_WINDOW=75
    python IndexStockCorrelationScanner.py

Use a custom NIFTY 50 constituents CSV:
    set NIFTY50_CSV_PATH=C:\\path\\to\\nifty50.csv
    python IndexStockCorrelationScanner.py

The custom CSV must contain either a 'Symbol' column or a 'tradingsymbol' column.

Outputs
-------
By default, files are written under:
    ./pair_correlation_output

Main outputs:
    candles/                         reusable per-instrument candle files
    wide_close_<interval>.pkl         aligned close-price matrix
    wide_returns_<interval>.pkl       aligned log-return matrix
    pair_correlation_metrics.csv      all pairwise metrics
    pair_correlation_report.xlsx      Excel report with multiple sheets

Dependencies
------------
Required:
    pandas, numpy, openpyxl for Excel output

Optional:
    pyarrow or fastparquet for parquet output. If missing, the script uses pickle.

Kite dependency
---------------
Your project must have:
    Trading_2024.OptionTradeUtils.intialize_kite_api()

This is intentionally the same initialization style used in your uploaded reference code.
"""

import os
import time
import math
import warnings
from dataclasses import dataclass
from datetime import datetime, date, time as dtime, timedelta
from typing import Dict, List, Tuple, Optional, Iterable

import numpy as np
import pandas as pd

# Same initialization pattern as your existing downloader.
import Trading_2024.OptionTradeUtils as oUtils

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


# =============================================================================
# CONFIGURATION
# =============================================================================

# Indian cash-market session. Used for intraday from/to datetimes.
SESSION_START = dtime(9, 15, 0)
SESSION_END = dtime(15, 30, 0)

# Historical download retry settings.
MAX_ATTEMPTS = int(os.environ.get("MAX_ATTEMPTS", "5"))
SLEEP_BETWEEN_CALLS_SEC = float(os.environ.get("SLEEP_BETWEEN_CALLS_SEC", "0.25"))

# Output directory.
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "./pair_correlation_output")

# Historical interval: 'day', 'minute', '3minute', '5minute', '10minute', '15minute', etc.
# Zerodha Kite supports standard historical_data intervals.
INTERVAL = os.environ.get("INTERVAL", "minute").strip()

# Lookback window in calendar days. For exactly last one year, keep 365.
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "365"))

# End date. If not provided, today's IST date is used.
# Format: YYYY-MM-DD or DD-MM-YYYY.
END_DATE_ENV = os.environ.get("END_DATE", "").strip()

# Rolling correlation window in number of candles, not days.
# Good defaults:
#   day      -> 60 candles = approx 3 trading months
#   minute   -> 375 candles = approx one trading day
#   5minute  -> 75 candles = approx one trading day
ROLLING_WINDOW = int(os.environ.get("ROLLING_WINDOW", "375"))

# Minimum common returns needed to calculate pair metrics.
MIN_COMMON_RETURNS = int(os.environ.get("MIN_COMMON_RETURNS", "100"))

# Set FORCE_DOWNLOAD=1 to re-download even if local candle file exists.
FORCE_DOWNLOAD = os.environ.get("FORCE_DOWNLOAD", "0").strip() == "1"

# Set SAVE_CSV_CANDLES=1 if you also want one CSV per instrument.
SAVE_CSV_CANDLES = os.environ.get("SAVE_CSV_CANDLES", "0").strip() == "1"

# Optional custom NIFTY 50 constituents CSV.
# CSV must contain one of these columns: Symbol, SYMBOL, tradingsymbol, Tradingsymbol.
NIFTY50_CSV_PATH = os.environ.get("NIFTY50_CSV_PATH", "").strip()

# Attempt to fetch the official constituents CSV first. If it fails, fallback is used.
# Set FETCH_NIFTY50_FROM_WEB=0 if you want only the hardcoded fallback/custom CSV.
FETCH_NIFTY50_FROM_WEB = os.environ.get("FETCH_NIFTY50_FROM_WEB", "1").strip() == "1"

# NSE's widely used NIFTY 50 constituents CSV endpoint.
NIFTY50_CONSTITUENTS_URL = os.environ.get(
    "NIFTY50_CONSTITUENTS_URL",
    "https://archives.nseindia.com/content/indices/ind_nifty50list.csv",
).strip()

# If INCLUDE_NIFTY50_STOCKS_ONLY=1, indices are excluded and only 50 stocks are analyzed.
# Default is 0 because the user asked for 3 indices + NIFTY 50 stocks.
INCLUDE_NIFTY50_STOCKS_ONLY = os.environ.get("INCLUDE_NIFTY50_STOCKS_ONLY", "0").strip() == "1"

# If TOP_N_SHEETS=50, the Excel report will include top 50 positive/negative/stable pairs.
TOP_N_SHEETS = int(os.environ.get("TOP_N_SHEETS", "50"))


# Fallback list. NIFTY 50 composition changes periodically.
# The script tries custom CSV / NSE CSV first; this list is only the fallback.
# If a symbol is not in your Kite instrument dump, the script skips it and reports the reason.
FALLBACK_NIFTY50_SYMBOLS = [
    "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
    "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV", "BEL", "BHARTIARTL",
    "CIPLA", "COALINDIA", "DRREDDY", "EICHERMOT", "ETERNAL",
    "GRASIM", "HCLTECH", "HDFCBANK", "HDFCLIFE", "HEROMOTOCO",
    "HINDALCO", "HINDUNILVR", "ICICIBANK", "ITC", "INDUSINDBK",
    "INFY", "JSWSTEEL", "JIOFIN", "KOTAKBANK", "LT",
    "M&M", "MARUTI", "NTPC", "NESTLEIND", "ONGC",
    "POWERGRID", "RELIANCE", "SBILIFE", "SHRIRAMFIN", "SBIN",
    "SUNPHARMA", "TCS", "TATACONSUM", "TATAMOTORS", "TATASTEEL",
    "TECHM", "TITAN", "TRENT", "ULTRACEMCO", "WIPRO",
]


@dataclass(frozen=True)
class InstrumentSpec:
    """One instrument to download and analyze."""

    label: str              # Clean column name used in matrices/reports.
    exchange: str           # NSE or BSE.
    tradingsymbol: str      # Kite tradingsymbol.
    kind: str               # INDEX or STOCK.


# =============================================================================
# DATE HELPERS
# =============================================================================


def ist_today() -> date:
    """Return today's date in Asia/Kolkata, with a safe fallback."""
    if ZoneInfo is not None:
        try:
            return datetime.now(ZoneInfo("Asia/Kolkata")).date()
        except Exception:
            pass
    return datetime.now().date()


def parse_date_env(raw: str, default: date) -> date:
    """Parse YYYY-MM-DD or DD-MM-YYYY date string; return default if blank."""
    raw = (raw or "").strip()
    if not raw:
        return default
    for fmt in ("%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Date must be YYYY-MM-DD or DD-MM-YYYY, got: {raw!r}")


def make_from_to_datetimes(end_d: date, lookback_days: int, interval: str) -> Tuple[datetime, datetime]:
    """
    Convert date settings to Kite historical from/to datetimes.

    For intraday intervals, use 09:15 to 15:30.
    For daily interval, using datetimes is still accepted by Kite and keeps the
    function signature uniform.
    """
    start_d = end_d - timedelta(days=lookback_days)
    from_dt = datetime.combine(start_d, SESSION_START)
    to_dt = datetime.combine(end_d, SESSION_END)
    return from_dt, to_dt


# =============================================================================
# KITE / DATA-DOWNLOAD HELPERS
# =============================================================================


def days_per_chunk_for_interval(interval: str) -> int:
    """
    Conservative chunk size by interval.

    Kite historical_data calls can fail if too much minute data is requested in
    one call. Your reference code used 25 days for minute data; this script keeps
    that safe default for all intraday intervals.
    """
    interval_u = interval.lower().strip()
    if interval_u == "day":
        return 500
    if interval_u in {"minute", "3minute", "5minute", "10minute", "15minute", "30minute", "60minute"}:
        return 25
    # Unknown intervals are treated conservatively.
    return 25


def iter_chunks_by_date(from_dt: datetime, to_dt: datetime, days_per_chunk: int) -> List[Tuple[datetime, datetime]]:
    """
    Chunk a datetime range without losing intraday candles.

    This mirrors the key safety idea from your reference downloader: chunk ends
    should use SESSION_END, not the start time, otherwise an entire chunk-end day
    after 09:15 can be lost.
    """
    if from_dt > to_dt:
        raise ValueError("from_dt must be <= to_dt")

    chunks: List[Tuple[datetime, datetime]] = []
    start_d = from_dt.date()
    end_d = to_dt.date()
    cur = start_d

    while cur <= end_d:
        chunk_end_d = min(cur + timedelta(days=days_per_chunk - 1), end_d)
        c_from = from_dt if cur == start_d else datetime.combine(cur, SESSION_START)
        c_to = to_dt if chunk_end_d == end_d else datetime.combine(chunk_end_d, SESSION_END)
        chunks.append((c_from, c_to))
        cur = chunk_end_d + timedelta(days=1)

    return chunks


def kite_instruments_cached(kite, exchange: str, cache: Dict[str, List[Dict]]) -> List[Dict]:
    """Load and cache Kite instrument dump for the given exchange."""
    ex = exchange.upper().strip()
    if ex not in cache:
        print(f"[STEP] Loading instruments dump for {ex} ...")
        cache[ex] = kite.instruments(ex)
        print(f"[INFO] Total instruments on {ex}: {len(cache[ex])}")
    return cache[ex]


def get_instrument_token(
    kite,
    exchange: str,
    tradingsymbol: str,
    cache: Dict[str, List[Dict]],
) -> Tuple[int, str, Dict]:
    """
    Resolve instrument_token for a Kite tradingsymbol.

    Returns:
        (instrument_token, real_exchange, full_instrument_row)
    """
    ex = exchange.upper().strip()
    wanted = tradingsymbol.upper().strip()
    instruments = kite_instruments_cached(kite, ex, cache)

    for row in instruments:
        if str(row.get("tradingsymbol", "")).upper().strip() == wanted:
            return int(row["instrument_token"]), str(row.get("exchange", ex)), row

    raise ValueError(f"Instrument not found on {ex}: {tradingsymbol!r}")


def fetch_history(
    kite,
    instrument_token: int,
    from_dt: datetime,
    to_dt: datetime,
    interval: str,
    label: str,
) -> List[Dict]:
    """Fetch historical data using chunking and retries."""
    days_per_chunk = days_per_chunk_for_interval(interval)
    chunks = iter_chunks_by_date(from_dt, to_dt, days_per_chunk=days_per_chunk)

    print(
        f"[INFO] Fetching {interval} data for {label} token={instrument_token} "
        f"from {from_dt} to {to_dt} in {len(chunks)} chunk(s)."
    )

    all_rows: List[Dict] = []

    for idx, (c_from, c_to) in enumerate(chunks, start=1):
        print(f"  [CHUNK {idx}/{len(chunks)}] {c_from} -> {c_to}")
        last_err: Optional[Exception] = None

        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                rows = kite.historical_data(
                    instrument_token=instrument_token,
                    from_date=c_from,
                    to_date=c_to,
                    interval=interval,
                    continuous=False,
                    oi=False,
                )
                print(f"    [OK] Retrieved {len(rows)} candles on attempt {attempt}.")
                all_rows.extend(rows)
                last_err = None
                break
            except Exception as exc:
                last_err = exc
                wait = min(10.0, 1.5 * attempt)
                print(
                    f"    [WARN] {label} attempt {attempt}/{MAX_ATTEMPTS} failed: {exc}. "
                    f"Sleeping {wait:.1f}s"
                )
                time.sleep(wait)

        if last_err is not None:
            print(f"    [ERROR] Giving up on chunk {idx}/{len(chunks)} for {label}: {last_err}")

        time.sleep(SLEEP_BETWEEN_CALLS_SEC)

    return all_rows


def normalize_kite_datetime(series: pd.Series) -> pd.Series:
    """
    Normalize Kite datetime column.

    Kite often returns timezone-aware datetimes. For joining instruments, we only
    need consistent timestamps, so timezone-aware values are converted to naive
    Asia/Kolkata timestamps.
    """
    s = pd.to_datetime(series, errors="coerce")

    # If the whole Series has a timezone-aware dtype, convert cleanly.
    try:
        tz = s.dt.tz
        if tz is not None:
            return s.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    except Exception:
        pass

    # If dtype is object with mixed tz-aware Python datetimes, normalize row-wise.
    def _one(x):
        if pd.isna(x):
            return pd.NaT
        try:
            ts = pd.Timestamp(x)
            if ts.tzinfo is not None:
                ts = ts.tz_convert("Asia/Kolkata").tz_localize(None)
            return ts
        except Exception:
            return pd.NaT

    return s.map(_one)


def rows_to_dataframe(rows: List[Dict], spec: InstrumentSpec) -> pd.DataFrame:
    """Convert Kite historical rows to a clean sorted OHLCV DataFrame."""
    if not rows:
        return pd.DataFrame(
            columns=[
                "date", "open", "high", "low", "close", "volume",
                "label", "exchange", "tradingsymbol", "kind",
            ]
        )

    df = pd.DataFrame(rows)

    # Ensure a stable schema even if Kite omits any field.
    for col in ["date", "open", "high", "low", "close", "volume"]:
        if col not in df.columns:
            df[col] = np.nan

    df["date"] = normalize_kite_datetime(df["date"])
    df = df.dropna(subset=["date", "close"])

    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = (
        df.drop_duplicates(subset=["date"], keep="last")
        .sort_values("date")
        .reset_index(drop=True)
    )

    # Metadata columns make later debugging easier.
    df.insert(0, "label", spec.label)
    df.insert(1, "exchange", spec.exchange)
    df.insert(2, "tradingsymbol", spec.tradingsymbol)
    df.insert(3, "kind", spec.kind)

    return df


# =============================================================================
# FILE I/O HELPERS
# =============================================================================


def safe_filename(text: str) -> str:
    """Create a filesystem-safe filename fragment."""
    return (
        text.replace(" ", "_")
        .replace("/", "_")
        .replace("\\", "_")
        .replace(":", "_")
        .replace("&", "AND")
    )


def candle_file_path(output_dir: str, spec: InstrumentSpec, interval: str) -> str:
    """Return local cache path for one instrument's candle file."""
    candles_dir = os.path.join(output_dir, "candles")
    os.makedirs(candles_dir, exist_ok=True)
    fname = f"{safe_filename(spec.label)}__{spec.exchange}__{safe_filename(spec.tradingsymbol)}__{interval}.pkl"
    return os.path.join(candles_dir, fname)


def save_candles(df: pd.DataFrame, path: str) -> None:
    """Save candles as pickle; optionally also save CSV for inspection."""
    df.to_pickle(path)
    if SAVE_CSV_CANDLES:
        csv_path = path[:-4] + ".csv" if path.endswith(".pkl") else path + ".csv"
        df.to_csv(csv_path, index=False)


def load_candles_if_available(path: str) -> Optional[pd.DataFrame]:
    """Load cached candles if available and not forcing re-download."""
    if FORCE_DOWNLOAD:
        return None
    if os.path.exists(path):
        try:
            df = pd.read_pickle(path)
            if not df.empty:
                print(f"[CACHE] Using cached file: {path} rows={len(df)}")
                return df
        except Exception as exc:
            print(f"[WARN] Could not read cached file {path}: {exc}. Will re-download.")
    return None


# =============================================================================
# NIFTY 50 CONSTITUENT HELPERS
# =============================================================================


def extract_symbols_from_csv(df: pd.DataFrame) -> List[str]:
    """Extract symbols from a constituents CSV with flexible column naming."""
    candidate_cols = ["Symbol", "SYMBOL", "symbol", "tradingsymbol", "Tradingsymbol", "TRADINGSYMBOL"]
    found_col = None
    for col in candidate_cols:
        if col in df.columns:
            found_col = col
            break

    if found_col is None:
        raise ValueError(f"Could not find a symbol column in CSV. Columns={list(df.columns)}")

    symbols = (
        df[found_col]
        .astype(str)
        .str.strip()
        .str.upper()
        .replace("", np.nan)
        .dropna()
        .drop_duplicates()
        .tolist()
    )
    return symbols


def load_nifty50_symbols() -> List[str]:
    """
    Load NIFTY 50 stock symbols.

    Order of preference:
        1) user-supplied CSV via NIFTY50_CSV_PATH
        2) NSE constituents CSV URL
        3) hardcoded fallback list
    """
    if NIFTY50_CSV_PATH:
        print(f"[STEP] Loading NIFTY50 constituents from custom CSV: {NIFTY50_CSV_PATH}")
        df = pd.read_csv(NIFTY50_CSV_PATH)
        symbols = extract_symbols_from_csv(df)
        print(f"[INFO] Loaded {len(symbols)} symbols from custom CSV.")
        return symbols

    if FETCH_NIFTY50_FROM_WEB:
        try:
            print(f"[STEP] Attempting to fetch NIFTY50 constituents from: {NIFTY50_CONSTITUENTS_URL}")
            # NSE endpoints sometimes need a user-agent. pandas supports storage_options for URLs.
            df = pd.read_csv(
                NIFTY50_CONSTITUENTS_URL,
                storage_options={"User-Agent": "Mozilla/5.0"},
            )
            symbols = extract_symbols_from_csv(df)
            if len(symbols) >= 45:
                print(f"[INFO] Loaded {len(symbols)} symbols from web CSV.")
                return symbols
            print(f"[WARN] Web CSV returned only {len(symbols)} symbols. Using fallback list.")
        except Exception as exc:
            print(f"[WARN] Could not fetch NIFTY50 web CSV: {exc}. Using fallback list.")

    print(f"[INFO] Using fallback NIFTY50 symbol list with {len(FALLBACK_NIFTY50_SYMBOLS)} symbols.")
    return list(FALLBACK_NIFTY50_SYMBOLS)


def build_instrument_universe() -> List[InstrumentSpec]:
    """Build the requested universe: 3 indices + NIFTY 50 stocks."""
    specs: List[InstrumentSpec] = []

    if not INCLUDE_NIFTY50_STOCKS_ONLY:
        specs.extend(
            [
                # Kite usually exposes these as NSE/BSE index tradingsymbols.
                InstrumentSpec(label="NIFTY", exchange="NSE", tradingsymbol="NIFTY 50", kind="INDEX"),
                InstrumentSpec(label="BANKNIFTY", exchange="NSE", tradingsymbol="NIFTY BANK", kind="INDEX"),
                InstrumentSpec(label="SENSEX", exchange="BSE", tradingsymbol="SENSEX", kind="INDEX"),
            ]
        )

    for sym in load_nifty50_symbols():
        specs.append(InstrumentSpec(label=sym, exchange="NSE", tradingsymbol=sym, kind="STOCK"))

    # Remove accidental duplicates by label while preserving order.
    seen = set()
    unique_specs: List[InstrumentSpec] = []
    for spec in specs:
        key = spec.label.upper()
        if key not in seen:
            unique_specs.append(spec)
            seen.add(key)

    print(f"[INFO] Universe size after de-duplication: {len(unique_specs)}")
    return unique_specs


# =============================================================================
# DOWNLOAD PIPELINE
# =============================================================================


def download_one_instrument(
    kite,
    spec: InstrumentSpec,
    from_dt: datetime,
    to_dt: datetime,
    interval: str,
    output_dir: str,
    instruments_cache: Dict[str, List[Dict]],
) -> Tuple[InstrumentSpec, Optional[pd.DataFrame], Optional[str]]:
    """
    Resolve token, download or load cached candles, and return clean DataFrame.

    Returns:
        (spec, dataframe_or_none, error_message_or_none)
    """
    path = candle_file_path(output_dir, spec, interval)

    cached = load_candles_if_available(path)
    if cached is not None:
        return spec, cached, None

    try:
        token, real_exchange, inst_row = get_instrument_token(
            kite=kite,
            exchange=spec.exchange,
            tradingsymbol=spec.tradingsymbol,
            cache=instruments_cache,
        )
        print(
            f"\n[DOWNLOAD] {spec.label} | {real_exchange}:{spec.tradingsymbol} | "
            f"token={token} | kind={spec.kind}"
        )

        rows = fetch_history(
            kite=kite,
            instrument_token=token,
            from_dt=from_dt,
            to_dt=to_dt,
            interval=interval,
            label=f"{real_exchange}:{spec.tradingsymbol}",
        )

        df = rows_to_dataframe(rows, spec)
        if df.empty:
            return spec, None, "No historical candles returned"

        save_candles(df, path)
        print(f"[SAVED] {spec.label}: rows={len(df)} -> {path}")
        return spec, df, None

    except Exception as exc:
        return spec, None, str(exc)


def download_universe(
    kite,
    specs: List[InstrumentSpec],
    from_dt: datetime,
    to_dt: datetime,
    interval: str,
    output_dir: str,
) -> Tuple[Dict[str, pd.DataFrame], pd.DataFrame]:
    """
    Download all instruments and return:
        - dict[label] = candles DataFrame
        - coverage/errors DataFrame
    """
    instruments_cache: Dict[str, List[Dict]] = {}
    downloaded: Dict[str, pd.DataFrame] = {}
    coverage_rows: List[Dict] = []

    total = len(specs)
    for i, spec in enumerate(specs, start=1):
        print("\n" + "=" * 90)
        print(f"[INSTRUMENT {i}/{total}] {spec.label} ({spec.exchange}:{spec.tradingsymbol})")
        print("=" * 90)

        spec, df, err = download_one_instrument(
            kite=kite,
            spec=spec,
            from_dt=from_dt,
            to_dt=to_dt,
            interval=interval,
            output_dir=output_dir,
            instruments_cache=instruments_cache,
        )

        if df is not None and not df.empty:
            downloaded[spec.label] = df
            coverage_rows.append(
                {
                    "label": spec.label,
                    "exchange": spec.exchange,
                    "tradingsymbol": spec.tradingsymbol,
                    "kind": spec.kind,
                    "status": "OK",
                    "rows": len(df),
                    "first_date": df["date"].min(),
                    "last_date": df["date"].max(),
                    "error": "",
                }
            )
        else:
            print(f"[SKIP] {spec.label}: {err}")
            coverage_rows.append(
                {
                    "label": spec.label,
                    "exchange": spec.exchange,
                    "tradingsymbol": spec.tradingsymbol,
                    "kind": spec.kind,
                    "status": "FAILED",
                    "rows": 0,
                    "first_date": pd.NaT,
                    "last_date": pd.NaT,
                    "error": err or "Unknown error",
                }
            )

    coverage_df = pd.DataFrame(coverage_rows)
    return downloaded, coverage_df


# =============================================================================
# CORRELATION / METRICS HELPERS
# =============================================================================


def build_wide_close(downloaded: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Convert per-instrument candle DataFrames to one wide close-price matrix.

    Index:
        date
    Columns:
        instrument labels
    """
    series_list: List[pd.Series] = []

    for label, df in downloaded.items():
        if df.empty or "close" not in df.columns:
            continue

        s = (
            df[["date", "close"]]
            .dropna(subset=["date", "close"])
            .drop_duplicates(subset=["date"], keep="last")
            .sort_values("date")
            .set_index("date")["close"]
            .astype(float)
        )
        s.name = label
        series_list.append(s)

    if not series_list:
        return pd.DataFrame()

    wide = pd.concat(series_list, axis=1).sort_index()
    wide = wide.loc[:, ~wide.columns.duplicated()]
    return wide


def compute_log_returns(wide_close: pd.DataFrame) -> pd.DataFrame:
    """Compute log returns for all columns in a close-price matrix."""
    close = wide_close.astype(float).replace(0, np.nan)
    returns = np.log(close / close.shift(1))
    returns = returns.replace([np.inf, -np.inf], np.nan)
    return returns


def safe_float(x) -> Optional[float]:
    """Convert numpy/pandas scalar to normal float or None for clean reports."""
    try:
        if x is None or pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None


def beta_y_on_x(y: pd.Series, x: pd.Series) -> Optional[float]:
    """
    OLS beta of y on x using returns:
        y = alpha + beta * x + error

    For pair trading, beta is useful as a rough hedge-ratio starting point.
    """
    df = pd.concat([y, x], axis=1).dropna()
    if len(df) < 3:
        return None

    yv = df.iloc[:, 0].values.astype(float)
    xv = df.iloc[:, 1].values.astype(float)
    var_x = np.var(xv, ddof=1)
    if var_x == 0 or np.isnan(var_x):
        return None

    cov_yx = np.cov(yv, xv, ddof=1)[0, 1]
    return float(cov_yx / var_x)


def annualization_factor(interval: str) -> float:
    """
    Approximate number of return observations per year.

    Used only for volatility comparison. It is not used in correlation.
    """
    i = interval.lower().strip()
    if i == "day":
        return 252.0
    if i == "minute":
        return 252.0 * 375.0
    if i == "3minute":
        return 252.0 * 125.0
    if i == "5minute":
        return 252.0 * 75.0
    if i == "10minute":
        return 252.0 * 38.0
    if i == "15minute":
        return 252.0 * 25.0
    if i == "30minute":
        return 252.0 * 13.0
    if i == "60minute":
        return 252.0 * 7.0
    return 252.0


def compute_pair_metrics(
    returns: pd.DataFrame,
    interval: str,
    rolling_window: int,
    min_common_returns: int,
) -> pd.DataFrame:
    """
    Compute pairwise return-correlation metrics for every instrument pair.

    Each pair is aligned independently using common non-null return timestamps.
    This is better than dropping rows where any one of 53 instruments is missing.
    """
    labels = list(returns.columns)
    rows: List[Dict] = []
    ann = annualization_factor(interval)

    pair_count = len(labels) * (len(labels) - 1) // 2
    done = 0

    for i in range(len(labels)):
        a = labels[i]
        for j in range(i + 1, len(labels)):
            b = labels[j]
            done += 1

            pair = returns[[a, b]].dropna()
            n = len(pair)

            if n < min_common_returns:
                rows.append(
                    {
                        "instrument_a": a,
                        "instrument_b": b,
                        "common_returns": n,
                        "status": "INSUFFICIENT_DATA",
                    }
                )
                continue

            ra = pair[a].astype(float)
            rb = pair[b].astype(float)

            full_corr = ra.corr(rb)
            rolling_corr = ra.rolling(rolling_window).corr(rb).dropna()

            # Rolling metrics are useful to check stability. A high full-period
            # correlation with unstable rolling correlation is suspicious.
            if len(rolling_corr) > 0:
                roll_mean = rolling_corr.mean()
                roll_min = rolling_corr.min()
                roll_max = rolling_corr.max()
                roll_std = rolling_corr.std()
                roll_last = rolling_corr.iloc[-1]
                roll_p05 = rolling_corr.quantile(0.05)
                roll_p25 = rolling_corr.quantile(0.25)
                roll_p50 = rolling_corr.quantile(0.50)
                roll_p75 = rolling_corr.quantile(0.75)
                roll_p95 = rolling_corr.quantile(0.95)
                pct_above_0_50 = float((rolling_corr > 0.50).mean() * 100.0)
                pct_above_0_70 = float((rolling_corr > 0.70).mean() * 100.0)
                pct_above_0_80 = float((rolling_corr > 0.80).mean() * 100.0)
            else:
                roll_mean = roll_min = roll_max = roll_std = roll_last = None
                roll_p05 = roll_p25 = roll_p50 = roll_p75 = roll_p95 = None
                pct_above_0_50 = pct_above_0_70 = pct_above_0_80 = None

            beta_a_b = beta_y_on_x(ra, rb)
            beta_b_a = beta_y_on_x(rb, ra)

            rows.append(
                {
                    "instrument_a": a,
                    "instrument_b": b,
                    "common_returns": n,
                    "first_common_timestamp": pair.index.min(),
                    "last_common_timestamp": pair.index.max(),
                    "status": "OK",

                    # Main requested metric.
                    "full_period_return_correlation": safe_float(full_corr),

                    # Rolling stability metrics.
                    "rolling_window_candles": rolling_window,
                    "rolling_corr_mean": safe_float(roll_mean),
                    "rolling_corr_min": safe_float(roll_min),
                    "rolling_corr_max": safe_float(roll_max),
                    "rolling_corr_std": safe_float(roll_std),
                    "rolling_corr_last": safe_float(roll_last),
                    "rolling_corr_p05": safe_float(roll_p05),
                    "rolling_corr_p25": safe_float(roll_p25),
                    "rolling_corr_median": safe_float(roll_p50),
                    "rolling_corr_p75": safe_float(roll_p75),
                    "rolling_corr_p95": safe_float(roll_p95),
                    "rolling_corr_gt_0_50_pct": safe_float(pct_above_0_50),
                    "rolling_corr_gt_0_70_pct": safe_float(pct_above_0_70),
                    "rolling_corr_gt_0_80_pct": safe_float(pct_above_0_80),

                    # Similar/useful pair-screening metrics.
                    "r_squared_from_full_corr": safe_float(full_corr * full_corr if pd.notna(full_corr) else None),
                    "beta_a_on_b_returns": safe_float(beta_a_b),
                    "beta_b_on_a_returns": safe_float(beta_b_a),
                    "ann_vol_a_pct": safe_float(ra.std(ddof=1) * math.sqrt(ann) * 100.0),
                    "ann_vol_b_pct": safe_float(rb.std(ddof=1) * math.sqrt(ann) * 100.0),
                    "mean_abs_return_diff_bps": safe_float((ra - rb).abs().mean() * 10000.0),
                }
            )

            if done % 100 == 0 or done == pair_count:
                print(f"[METRICS] Completed {done}/{pair_count} pairs")

    metrics = pd.DataFrame(rows)

    if not metrics.empty and "full_period_return_correlation" in metrics.columns:
        metrics["abs_full_corr"] = metrics["full_period_return_correlation"].abs()
        metrics["corr_stability_score"] = (
            metrics["rolling_corr_mean"].fillna(-999.0)
            - metrics["rolling_corr_std"].fillna(999.0)
        )
        metrics = metrics.sort_values(
            by=["status", "full_period_return_correlation", "rolling_corr_mean"],
            ascending=[True, False, False],
        ).reset_index(drop=True)

    return metrics


def correlation_matrix(returns: pd.DataFrame) -> pd.DataFrame:
    """Full-period return correlation matrix."""
    return returns.corr()


def rolling_corr_mean_matrix(returns: pd.DataFrame, rolling_window: int) -> pd.DataFrame:
    """
    Matrix where each cell is the mean rolling correlation for the pair.

    This is slower than returns.corr(), but useful for a compact matrix view of
    correlation stability.
    """
    labels = list(returns.columns)
    mat = pd.DataFrame(index=labels, columns=labels, dtype=float)

    for a in labels:
        mat.loc[a, a] = 1.0

    for i, a in enumerate(labels):
        for j in range(i + 1, len(labels)):
            b = labels[j]
            pair = returns[[a, b]].dropna()
            if len(pair) >= rolling_window:
                rc = pair[a].rolling(rolling_window).corr(pair[b]).dropna()
                val = float(rc.mean()) if len(rc) else np.nan
            else:
                val = np.nan
            mat.loc[a, b] = val
            mat.loc[b, a] = val

    return mat


# =============================================================================
# REPORTING
# =============================================================================


def write_reports(
    output_dir: str,
    interval: str,
    wide_close: pd.DataFrame,
    returns: pd.DataFrame,
    metrics: pd.DataFrame,
    coverage: pd.DataFrame,
    rolling_window: int,
) -> None:
    """Write pickle/CSV/Excel outputs."""
    os.makedirs(output_dir, exist_ok=True)

    close_path = os.path.join(output_dir, f"wide_close_{interval}.pkl")
    returns_path = os.path.join(output_dir, f"wide_returns_{interval}.pkl")
    metrics_csv_path = os.path.join(output_dir, "pair_correlation_metrics.csv")
    coverage_csv_path = os.path.join(output_dir, "instrument_coverage.csv")
    excel_path = os.path.join(output_dir, "pair_correlation_report.xlsx")

    wide_close.to_pickle(close_path)
    returns.to_pickle(returns_path)
    metrics.to_csv(metrics_csv_path, index=False)
    coverage.to_csv(coverage_csv_path, index=False)

    print(f"[SAVED] Wide close matrix: {close_path}")
    print(f"[SAVED] Wide return matrix: {returns_path}")
    print(f"[SAVED] Pair metrics CSV: {metrics_csv_path}")
    print(f"[SAVED] Coverage CSV: {coverage_csv_path}")

    # Excel report. If openpyxl/xlsxwriter is unavailable, CSVs are still saved.
    try:
        corr_mat = correlation_matrix(returns)
        roll_mean_mat = rolling_corr_mean_matrix(returns, rolling_window=rolling_window)

        ok_metrics = metrics[metrics.get("status", "") == "OK"].copy() if not metrics.empty else metrics

        top_positive = ok_metrics.sort_values("full_period_return_correlation", ascending=False).head(TOP_N_SHEETS)
        top_negative = ok_metrics.sort_values("full_period_return_correlation", ascending=True).head(TOP_N_SHEETS)
        top_abs = ok_metrics.sort_values("abs_full_corr", ascending=False).head(TOP_N_SHEETS)
        top_stable = ok_metrics.sort_values("corr_stability_score", ascending=False).head(TOP_N_SHEETS)

        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            coverage.to_excel(writer, sheet_name="instrument_coverage", index=False)
            metrics.to_excel(writer, sheet_name="all_pair_metrics", index=False)
            top_positive.to_excel(writer, sheet_name="top_positive_corr", index=False)
            top_negative.to_excel(writer, sheet_name="top_negative_corr", index=False)
            top_abs.to_excel(writer, sheet_name="top_abs_corr", index=False)
            top_stable.to_excel(writer, sheet_name="top_stable_rolling", index=False)
            corr_mat.to_excel(writer, sheet_name="corr_matrix")
            roll_mean_mat.to_excel(writer, sheet_name="rolling_corr_mean_matrix")

            # Compact run configuration sheet for reproducibility.
            config_df = pd.DataFrame(
                [
                    {"parameter": "interval", "value": interval},
                    {"parameter": "lookback_days", "value": LOOKBACK_DAYS},
                    {"parameter": "rolling_window_candles", "value": rolling_window},
                    {"parameter": "min_common_returns", "value": MIN_COMMON_RETURNS},
                    {"parameter": "output_dir", "value": output_dir},
                    {"parameter": "force_download", "value": FORCE_DOWNLOAD},
                    {"parameter": "fetch_nifty50_from_web", "value": FETCH_NIFTY50_FROM_WEB},
                    {"parameter": "nifty50_csv_path", "value": NIFTY50_CSV_PATH},
                ]
            )
            config_df.to_excel(writer, sheet_name="run_config", index=False)

        print(f"[SAVED] Excel report: {excel_path}")

    except Exception as exc:
        print(f"[WARN] Could not write Excel report: {exc}")
        print("[INFO] CSV and pickle outputs have still been saved.")


def print_summary(metrics: pd.DataFrame, coverage: pd.DataFrame) -> None:
    """Print a concise console summary after report generation."""
    print("\n" + "=" * 90)
    print("SUMMARY")
    print("=" * 90)

    ok_inst = coverage[coverage["status"] == "OK"] if not coverage.empty else pd.DataFrame()
    failed_inst = coverage[coverage["status"] != "OK"] if not coverage.empty else pd.DataFrame()

    print(f"Downloaded instruments OK: {len(ok_inst)}")
    print(f"Failed/skipped instruments: {len(failed_inst)}")

    if len(failed_inst) > 0:
        print("\nFailed/skipped instruments:")
        cols = ["label", "exchange", "tradingsymbol", "error"]
        print(failed_inst[cols].to_string(index=False))

    if metrics.empty:
        print("\nNo pair metrics computed.")
        return

    ok = metrics[metrics["status"] == "OK"].copy()
    print(f"\nPairs with OK metrics: {len(ok)}")

    if not ok.empty:
        print("\nTop 15 pairs by full-period return correlation:")
        cols = [
            "instrument_a", "instrument_b", "common_returns",
            "full_period_return_correlation", "rolling_corr_mean", "rolling_corr_min",
            "rolling_corr_std", "beta_a_on_b_returns",
        ]
        print(ok.sort_values("full_period_return_correlation", ascending=False).head(15)[cols].to_string(index=False))

        print("\nTop 15 pairs by rolling stability score = rolling_corr_mean - rolling_corr_std:")
        cols2 = [
            "instrument_a", "instrument_b", "common_returns",
            "full_period_return_correlation", "rolling_corr_mean", "rolling_corr_std",
            "corr_stability_score",
        ]
        print(ok.sort_values("corr_stability_score", ascending=False).head(15)[cols2].to_string(index=False))

    print("=" * 90)


# =============================================================================
# ENTRYPOINT
# =============================================================================


def main() -> None:
    warnings.simplefilter(action="ignore", category=FutureWarning)

    end_d = parse_date_env(END_DATE_ENV, default=ist_today())
    from_dt, to_dt = make_from_to_datetimes(
        end_d=end_d,
        lookback_days=LOOKBACK_DAYS,
        interval=INTERVAL,
    )

    print("=" * 90)
    print("INDEX / STOCK CORRELATION SCANNER")
    print("=" * 90)
    print(f"Interval: {INTERVAL}")
    print(f"Lookback days: {LOOKBACK_DAYS}")
    print(f"Date range: {from_dt} -> {to_dt}")
    print(f"Rolling window: {ROLLING_WINDOW} candles")
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"Force download: {FORCE_DOWNLOAD}")
    print("=" * 90)

    print("[STEP] Building instrument universe ...")
    specs = build_instrument_universe()

    print("\n[STEP] Initializing Kite API ...")
    kite = oUtils.intialize_kite_api()
    print("[INFO] Kite API initialized.")

    print("\n[STEP] Downloading historical data / reading cache ...")
    downloaded, coverage = download_universe(
        kite=kite,
        specs=specs,
        from_dt=from_dt,
        to_dt=to_dt,
        interval=INTERVAL,
        output_dir=OUTPUT_DIR,
    )

    if len(downloaded) < 2:
        raise RuntimeError("Need at least two successfully downloaded instruments to compute correlation.")

    print("\n[STEP] Building wide close matrix ...")
    wide_close = build_wide_close(downloaded)
    if wide_close.empty or wide_close.shape[1] < 2:
        raise RuntimeError("Wide close matrix has fewer than two instruments.")

    print(f"[INFO] Wide close shape: rows={wide_close.shape[0]}, instruments={wide_close.shape[1]}")

    print("\n[STEP] Computing log returns ...")
    returns = compute_log_returns(wide_close)
    print(f"[INFO] Return matrix shape: rows={returns.shape[0]}, instruments={returns.shape[1]}")

    print("\n[STEP] Computing pairwise correlation metrics ...")
    metrics = compute_pair_metrics(
        returns=returns,
        interval=INTERVAL,
        rolling_window=ROLLING_WINDOW,
        min_common_returns=MIN_COMMON_RETURNS,
    )

    print("\n[STEP] Writing reports ...")
    write_reports(
        output_dir=OUTPUT_DIR,
        interval=INTERVAL,
        wide_close=wide_close,
        returns=returns,
        metrics=metrics,
        coverage=coverage,
        rolling_window=ROLLING_WINDOW,
    )

    print_summary(metrics=metrics, coverage=coverage)


if __name__ == "__main__":
    main()
