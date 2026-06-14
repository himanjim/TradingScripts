#!/usr/bin/env python3
"""
stock_intraday_combined_candidate_selector.py

Purpose
-------
Rank NSE stocks for one-stock intraday trading using a combined movement model:

    1. Same-day high-low range
    2. Same-day open-close body movement
    3. Body efficiency
    4. Liquidity

This is intended for a momentum-style intraday trader who wants stocks that:

    - move enough intraday,
    - have meaningful directional body movement,
    - do not only create large wicks,
    - have enough traded value to enter/exit cleanly.

Core formulas
-------------
For every daily candle:

    high_low_range_abs = high - low
    high_low_range_pct = (high - low) / close * 100

    body_abs = abs(close - open)
    body_pct = abs(close - open) / open * 100

    body_efficiency_pct = abs(close - open) / (high - low) * 100

Interpretation
--------------
High-low range:
    Measures total intraday opportunity.

Open-close body:
    Measures net directional movement from open to close.

Body efficiency:
    Measures how much of the day's high-low range became directional body.

    Example:
        open=100, high=106, low=99, close=105

        high-low range = 7
        body = 5
        body efficiency = 5 / 7 * 100 = 71.43%

    High body efficiency means the stock did not merely make wicks; it produced
    a cleaner directional candle.

Liquidity
---------
Liquidity remains a prerequisite.

Approximate traded value is calculated from Kite daily candles:

    typical_price = (high + low + close) / 3
    traded_value_rs = typical_price * volume
    traded_value_cr = traded_value_rs / 1e7

This is not exact NSE turnover, but it is good enough for ranking/filtering.

Input CSV
---------
CSV containing NSE stock symbols. Accepted column names:

    SYMBOL, symbol, TRADINGSYMBOL, TICKER, STOCK,
    UNDERLYING, UNDERLYING_SYMBOL, NAME

Output Excel sheets
-------------------
1. ranked_stocks
2. top_candidates
3. daily_features
4. missing_or_failed
5. config

Environment variables
---------------------
INPUT_CSV="C:\\Users\\Local User\\Downloads\\fo_mktlots.csv"
OUTPUT_EXCEL="stock_intraday_combined_candidate_selector.xlsx"
DATA_CACHE_DIR="./stock_daily_cache"
FORCE_REFRESH="0" or "1"
TOP_N_TO_PRINT="10"

Liquidity thresholds:
MIN_AVG_TRADED_VALUE_CR="100"
MIN_MEDIAN_TRADED_VALUE_CR="75"
MIN_P25_TRADED_VALUE_CR="50"
MAX_SCORE_IF_LIQUIDITY_FAIL="49"

Dependency
----------
Uses your existing Kite login helper:

    Trading_2024.OptionTradeUtils.intialize_kite_api()

Fallback:
    OptionTradeUtils.intialize_kite_api()

Important limitation
--------------------
This script is a daily-candle selector. It does not prove a specific 1-minute
or 5-minute entry setup. After this selector, test MAE/MFE and target-before-stop
behaviour on the top 10-20 candidates.
"""

from __future__ import annotations

import math
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd

try:
    import Trading_2024.OptionTradeUtils as oUtils  # type: ignore
except Exception:
    try:
        import OptionTradeUtils as oUtils  # type: ignore
    except Exception as exc:  # pragma: no cover
        oUtils = None  # type: ignore
        print(f"[WARN] Could not import OptionTradeUtils: {exc}")

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


# ============================================================
# CONFIG
# ============================================================

EXCHANGE = "NSE"

INPUT_CSV = os.environ.get(
    "INPUT_CSV",
    r"C:\Users\himan\Downloads\fo_mktlots.csv",
).strip()

OUTPUT_EXCEL = os.environ.get(
    "OUTPUT_EXCEL",
    "stock_intraday_combined_candidate_selector.xlsx",
).strip()

DATA_CACHE_DIR = os.environ.get("DATA_CACHE_DIR", "./stock_daily_cache").strip()
FORCE_REFRESH = os.environ.get("FORCE_REFRESH", "0").strip() == "1"

TOP_N_TO_PRINT = int(os.environ.get("TOP_N_TO_PRINT", "10"))
TOP_N_TO_PRINT = max(5, min(10, TOP_N_TO_PRINT))

# Historical analysis windows.
WINDOW_2Y_DAYS = 365 * 2
WINDOW_1Y_DAYS = 365
WINDOW_3M_DAYS = 92
DATE_BUFFER_DAYS = 10

# Minimum data coverage expected in each window.
MIN_DAYS_2Y = int(os.environ.get("MIN_DAYS_2Y", "350"))
MIN_DAYS_1Y = int(os.environ.get("MIN_DAYS_1Y", "180"))
MIN_DAYS_3M = int(os.environ.get("MIN_DAYS_3M", "40"))

# Useful diagnostic thresholds.
HL_RANGE_ACTIVE_1PCT = float(os.environ.get("HL_RANGE_ACTIVE_1PCT", "1.00"))
BODY_ACTIVE_1PCT = float(os.environ.get("BODY_ACTIVE_1PCT", "1.00"))
HL_RANGE_EXTREME_4PCT = float(os.environ.get("HL_RANGE_EXTREME_4PCT", "4.00"))
BODY_EXTREME_4PCT = float(os.environ.get("BODY_EXTREME_4PCT", "4.00"))

# Liquidity prerequisite thresholds.
MIN_AVG_TRADED_VALUE_CR = float(os.environ.get("MIN_AVG_TRADED_VALUE_CR", "100"))
MIN_MEDIAN_TRADED_VALUE_CR = float(os.environ.get("MIN_MEDIAN_TRADED_VALUE_CR", "75"))
MIN_P25_TRADED_VALUE_CR = float(os.environ.get("MIN_P25_TRADED_VALUE_CR", "50"))
MAX_SCORE_IF_LIQUIDITY_FAIL = float(os.environ.get("MAX_SCORE_IF_LIQUIDITY_FAIL", "49"))

# Severe liquidity penalties. Cumulative.
PENALTY_AVG_LIQUIDITY_FAIL = float(os.environ.get("PENALTY_AVG_LIQUIDITY_FAIL", "40"))
PENALTY_MEDIAN_LIQUIDITY_FAIL = float(os.environ.get("PENALTY_MEDIAN_LIQUIDITY_FAIL", "30"))
PENALTY_P25_LIQUIDITY_FAIL = float(os.environ.get("PENALTY_P25_LIQUIDITY_FAIL", "20"))

# Kite API and retry behaviour.
MAX_DAYS_PER_CHUNK = 365
MAX_ATTEMPTS = 5
SLEEP_BETWEEN_CALLS_SEC = 0.25


# ============================================================
# DATA STRUCTURES
# ============================================================

@dataclass
class StockMetrics:
    """All computed metrics for one stock."""

    symbol: str
    instrument_token: int

    first_date_2y: Optional[date]
    last_date_2y: Optional[date]

    days_2y: int
    days_1y: int
    days_3m: int

    # High-low range metrics.
    median_hl_range_pct_1y: float
    median_hl_range_pct_2y: float
    median_hl_range_pct_3m: float
    avg_hl_range_pct_1y: float
    avg_hl_range_pct_2y: float
    avg_hl_range_pct_3m: float
    p75_hl_range_pct_1y: float
    p75_hl_range_pct_2y: float
    p75_hl_range_pct_3m: float
    p90_hl_range_pct_1y: float
    p90_hl_range_pct_2y: float
    p90_hl_range_pct_3m: float

    # Open-close body metrics.
    median_body_pct_1y: float
    median_body_pct_2y: float
    median_body_pct_3m: float
    avg_body_pct_1y: float
    avg_body_pct_2y: float
    avg_body_pct_3m: float
    p75_body_pct_1y: float
    p75_body_pct_2y: float
    p75_body_pct_3m: float
    p90_body_pct_1y: float
    p90_body_pct_2y: float
    p90_body_pct_3m: float

    # Body efficiency.
    median_body_efficiency_pct_1y: float
    median_body_efficiency_pct_2y: float
    median_body_efficiency_pct_3m: float
    avg_body_efficiency_pct_1y: float
    avg_body_efficiency_pct_2y: float
    avg_body_efficiency_pct_3m: float
    p75_body_efficiency_pct_1y: float
    p75_body_efficiency_pct_2y: float
    p75_body_efficiency_pct_3m: float

    # Activity diagnostics.
    active_hl_day_pct_ge_1_1y: float
    active_hl_day_pct_ge_1_2y: float
    active_hl_day_pct_ge_1_3m: float
    active_body_day_pct_ge_1_1y: float
    active_body_day_pct_ge_1_2y: float
    active_body_day_pct_ge_1_3m: float

    extreme_hl_day_pct_gt_4_1y: float
    extreme_hl_day_pct_gt_4_2y: float
    extreme_hl_day_pct_gt_4_3m: float
    extreme_body_day_pct_gt_4_1y: float
    extreme_body_day_pct_gt_4_2y: float
    extreme_body_day_pct_gt_4_3m: float

    # Liquidity metrics.
    avg_traded_value_cr_1y: float
    avg_traded_value_cr_2y: float
    avg_traded_value_cr_3m: float
    median_traded_value_cr_1y: float
    p25_traded_value_cr_1y: float

    latest_close: float
    avg_close_1y: float

    liquidity_pass: bool
    data_warning: str = ""


# ============================================================
# DATE HELPERS
# ============================================================

def ist_today() -> date:
    """Return current date in Asia/Kolkata."""
    if ZoneInfo is not None:
        try:
            return datetime.now(ZoneInfo("Asia/Kolkata")).date()
        except Exception:
            pass
    return datetime.now().date()


def compute_master_date_range() -> Tuple[date, date, date, date, date]:
    """
    Compute the download start date and three analysis-window starts.

    Returns:
        from_date_buffered, start_2y, start_1y, start_3m, to_date
    """
    to_date = ist_today()
    start_2y = to_date - timedelta(days=WINDOW_2Y_DAYS)
    start_1y = to_date - timedelta(days=WINDOW_1Y_DAYS)
    start_3m = to_date - timedelta(days=WINDOW_3M_DAYS)
    from_date_buffered = start_2y - timedelta(days=DATE_BUFFER_DAYS)
    return from_date_buffered, start_2y, start_1y, start_3m, to_date


def iter_date_chunks(from_date: date, to_date: date, days_per_chunk: int) -> List[Tuple[date, date]]:
    """Split a date range into API-friendly chunks."""
    if from_date > to_date:
        raise ValueError("from_date must be <= to_date")

    chunks: List[Tuple[date, date]] = []
    cur = from_date
    while cur <= to_date:
        chunk_end = min(cur + timedelta(days=days_per_chunk - 1), to_date)
        chunks.append((cur, chunk_end))
        cur = chunk_end + timedelta(days=1)
    return chunks


# ============================================================
# INPUT CSV
# ============================================================

def read_stock_symbols(csv_path: str) -> List[str]:
    """
    Read stock symbols from a CSV with flexible column names.

    Rows that look like index names or bad headers are ignored.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(
            f"Input CSV not found: {csv_path}\n"
            "Use INPUT_CSV env var or place a CSV with column SYMBOL/symbol."
        )

    df = pd.read_csv(csv_path)
    if df.empty:
        raise ValueError(f"Input CSV is empty: {csv_path}")

    normalized_map = {str(c).strip().upper(): c for c in df.columns}
    possible_cols = [
        "SYMBOL",
        "TRADINGSYMBOL",
        "TICKER",
        "STOCK",
        "UNDERLYING",
        "UNDERLYING_SYMBOL",
        "NAME",
    ]

    selected_col: Optional[str] = None
    for col in possible_cols:
        if col in normalized_map:
            selected_col = normalized_map[col]
            break

    if selected_col is None and len(df.columns) == 1:
        selected_col = df.columns[0]

    if selected_col is None:
        raise ValueError(
            f"Could not identify symbol column in {csv_path}. "
            f"Use one of: {', '.join(possible_cols)}"
        )

    ignore_values = {
        "",
        "NAN",
        "SYMBOL",
        "STOCK",
        "TRADINGSYMBOL",
        "NIFTY",
        "NIFTY50",
        "NIFTY 50",
        "NIFTYNXT50",
        "NIFTY NEXT 50",
        "NIFTY BANK",
        "BANKNIFTY",
        "FINNIFTY",
        "MIDCPNIFTY",
        "SENSEX",
    }

    out: List[str] = []
    for raw in df[selected_col].dropna().tolist():
        s = str(raw).strip().upper()
        s = s.replace(".NS", "")
        s = s.replace("NSE:", "")
        s = s.replace("EQ:", "")
        s = " ".join(s.split())

        if s in ignore_values:
            continue
        out.append(s)

    # Preserve order but remove duplicates.
    seen = set()
    cleaned: List[str] = []
    for s in out:
        if s not in seen:
            cleaned.append(s)
            seen.add(s)

    if not cleaned:
        raise ValueError(f"No valid stock symbols found in {csv_path}")

    return cleaned


# ============================================================
# KITE HELPERS
# ============================================================

def initialize_kite():
    """Initialize Kite using the user's existing utility function."""
    if oUtils is None:
        raise RuntimeError(
            "OptionTradeUtils is not importable. Ensure Trading_2024.OptionTradeUtils "
            "or OptionTradeUtils.py is available."
        )

    kite = oUtils.intialize_kite_api()
    if kite is None:
        raise RuntimeError("intialize_kite_api() returned None. Check Kite login.")
    return kite


def kite_instruments_cached(kite, exchange: str, cache: Dict[str, List[Dict]]) -> List[Dict]:
    """Load Kite instruments dump once per exchange."""
    ex = exchange.upper().strip()
    if ex not in cache:
        print(f"[STEP] Loading instruments dump for {ex} ...")
        cache[ex] = kite.instruments(ex)
        print(f"[INFO] Instrument rows on {ex}: {len(cache[ex]):,}")
    return cache[ex]


def get_equity_token(
    kite,
    symbol: str,
    exchange: str,
    instruments_cache: Dict[str, List[Dict]],
) -> Tuple[int, Dict]:
    """Resolve NSE equity instrument token for a symbol."""
    symbol_u = symbol.upper().strip()
    instruments = kite_instruments_cached(kite, exchange, instruments_cache)

    matches = []
    for inst in instruments:
        tsym = str(inst.get("tradingsymbol", "")).upper().strip()
        if tsym == symbol_u:
            matches.append(inst)

    if not matches:
        raise ValueError(f"Instrument not found on {exchange}: {symbol}")

    def priority(inst: Dict) -> int:
        instrument_type = str(inst.get("instrument_type", "")).upper()
        segment = str(inst.get("segment", "")).upper()
        exchange_value = str(inst.get("exchange", "")).upper()
        if instrument_type == "EQ" and exchange_value == exchange.upper():
            return 0
        if instrument_type == "EQ":
            return 1
        if segment == exchange.upper():
            return 2
        return 3

    matches.sort(key=priority)
    selected = matches[0]
    return int(selected["instrument_token"]), selected


# ============================================================
# HISTORICAL DATA AND CACHE
# ============================================================

def fetch_history_day(kite, instrument_token: int, from_date: date, to_date: date, label: str) -> List[Dict]:
    """Fetch daily historical candles from Kite with retries."""
    chunks = iter_date_chunks(from_date, to_date, MAX_DAYS_PER_CHUNK)
    print(
        f"[INFO] Fetching day data for {label} token={instrument_token} "
        f"from {from_date} to {to_date} in {len(chunks)} chunk(s)."
    )

    all_rows: List[Dict] = []
    for idx, (c_from, c_to) in enumerate(chunks, start=1):
        print(f"  [CHUNK {idx}/{len(chunks)}] {c_from} -> {c_to}")
        last_err = None

        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                rows = kite.historical_data(
                    instrument_token=instrument_token,
                    from_date=c_from,
                    to_date=c_to,
                    interval="day",
                    continuous=False,
                    oi=False,
                )
                print(f"    [OK] {len(rows)} daily candles on attempt {attempt}")
                all_rows.extend(rows)
                last_err = None
                break
            except Exception as exc:
                last_err = exc
                wait = min(8.0, 1.5 * attempt)
                print(f"    [WARN] {label} attempt {attempt}/{MAX_ATTEMPTS} failed: {exc}. Sleeping {wait:.1f}s")
                time.sleep(wait)

        if last_err is not None:
            print(f"    [ERROR] Giving up on chunk {idx} for {label}: {last_err}")

        time.sleep(SLEEP_BETWEEN_CALLS_SEC)

    return all_rows


def rows_to_dataframe(rows: List[Dict]) -> pd.DataFrame:
    """Convert Kite candle rows to clean daily OHLCV dataframe."""
    columns = ["date", "open", "high", "low", "close", "volume"]
    if not rows:
        return pd.DataFrame(columns=columns)

    df = pd.DataFrame(rows)
    for col in columns:
        if col not in df.columns:
            df[col] = None

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df["trade_date"] = df["date"].dt.date

    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["volume"] = df["volume"].fillna(0)

    df = (
        df.dropna(subset=["open", "high", "low", "close"])
        .drop_duplicates(subset=["trade_date"], keep="last")
        .sort_values("trade_date")
        .reset_index(drop=True)
    )
    return df


def cache_path_for_symbol(symbol: str) -> str:
    """Create a safe cache path for one symbol."""
    safe = (
        symbol.replace("/", "_")
        .replace("\\", "_")
        .replace(":", "_")
        .replace("&", "AND")
    )
    return os.path.join(DATA_CACHE_DIR, f"{safe}_daily.pkl")


def normalize_cached_df(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize a cached dataframe loaded from pickle."""
    if df.empty:
        return df
    df = df.copy()
    if "trade_date" not in df.columns and "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["trade_date"] = df["date"].dt.date
    return df


def load_cached_if_usable(symbol: str, from_date: date, to_date: date) -> Optional[pd.DataFrame]:
    """Load cached daily data if it covers the required date range."""
    if FORCE_REFRESH:
        return None

    path = cache_path_for_symbol(symbol)
    if not os.path.exists(path):
        return None

    try:
        df = normalize_cached_df(pd.read_pickle(path))
        if df.empty or "trade_date" not in df.columns:
            return None

        min_d = min(df["trade_date"])
        max_d = max(df["trade_date"])

        # Allow end to be a few calendar days behind for weekend/holiday/today candle.
        if min_d <= from_date and max_d >= to_date - timedelta(days=7):
            print(f"[CACHE] Using {symbol}: {path}")
            return df

        print(f"[CACHE] Stale/incomplete for {symbol}: {min_d} -> {max_d}. Re-downloading.")
        return None
    except Exception as exc:
        print(f"[WARN] Could not read cache for {symbol}: {exc}")
        return None


def save_cache(symbol: str, df: pd.DataFrame) -> None:
    """Save raw daily data to pickle cache."""
    os.makedirs(DATA_CACHE_DIR, exist_ok=True)
    df.to_pickle(cache_path_for_symbol(symbol))


# ============================================================
# FEATURE ENGINEERING AND METRICS
# ============================================================

def add_daily_features(symbol: str, instrument_token: int, raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Add high-low range, open-close body, body efficiency, and liquidity features.

    Scoring-relevant movement columns:
        high_low_range_pct
        body_pct
        body_efficiency_pct
    """
    if raw_df.empty:
        raise ValueError(f"No data for {symbol}")

    df = raw_df.copy()
    df["symbol"] = symbol
    df["instrument_token"] = instrument_token

    # Same-day high-low range: total intraday movement/opportunity.
    df["high_low_range_abs"] = df["high"] - df["low"]
    df["high_low_range_pct"] = (df["high_low_range_abs"] / df["close"]) * 100.0
    df["high_low_range_pct_vs_low"] = ((df["high"] / df["low"]) - 1.0) * 100.0

    # Same-day open-close body: directional body movement, irrespective of direction.
    df["body_abs"] = (df["close"] - df["open"]).abs()
    df["body_pct"] = (df["body_abs"] / df["open"]) * 100.0

    # Signed body is kept for diagnostics only. Positive means close > open.
    df["body_signed_pct"] = ((df["close"] - df["open"]) / df["open"]) * 100.0

    # Body efficiency: how much of the day's high-low range became candle body.
    # It identifies stocks that do not merely make large wicks.
    df["body_efficiency_pct"] = 0.0
    valid_range = df["high_low_range_abs"] > 0
    df.loc[valid_range, "body_efficiency_pct"] = (
        df.loc[valid_range, "body_abs"] / df.loc[valid_range, "high_low_range_abs"]
    ) * 100.0

    # Defensive clip. In clean OHLC data, body_abs <= high_low_range_abs, so
    # body_efficiency_pct should be 0..100. Clip prevents bad OHLC rows from
    # dominating the score.
    df["body_efficiency_pct"] = df["body_efficiency_pct"].clip(lower=0.0, upper=100.0)

    # Approximate traded value from daily candles.
    df["typical_price"] = (df["high"] + df["low"] + df["close"]) / 3.0
    df["traded_value_rs"] = df["typical_price"] * df["volume"]
    df["traded_value_cr"] = df["traded_value_rs"] / 1e7

    # Gap diagnostics only. Not used in score.
    df["prev_close"] = df["close"].shift(1)
    df["gap_pct"] = ((df["open"] - df["prev_close"]).abs() / df["prev_close"]) * 100.0

    # Remove impossible/bad rows.
    df = df[
        (df["open"] > 0)
        & (df["high"] > 0)
        & (df["low"] > 0)
        & (df["close"] > 0)
        & (df["high"] >= df["low"])
        & (df["high_low_range_abs"] > 0)
        & (df["high_low_range_pct"] > 0)
        & (df["high_low_range_pct"] < 25)
        & (df["body_pct"] >= 0)
        & (df["body_pct"] < 25)
    ].copy()

    if df.empty:
        raise ValueError(f"No valid daily feature rows for {symbol}")

    return df


def window_slice(df: pd.DataFrame, start_date: date, to_date: date) -> pd.DataFrame:
    """Return rows inside one analysis window."""
    return df[(df["trade_date"] >= start_date) & (df["trade_date"] <= to_date)].copy()


def safe_mean(s: pd.Series) -> float:
    return float(s.dropna().mean()) if s.notna().any() else 0.0


def safe_median(s: pd.Series) -> float:
    return float(s.dropna().median()) if s.notna().any() else 0.0


def safe_quantile(s: pd.Series, q: float) -> float:
    return float(s.dropna().quantile(q)) if s.notna().any() else 0.0


def calc_window_metrics(w: pd.DataFrame, label: str) -> Dict[str, float]:
    """Calculate movement/liquidity metrics for one window."""
    if w.empty:
        return {
            f"days_{label}": 0,

            f"median_hl_range_pct_{label}": 0.0,
            f"avg_hl_range_pct_{label}": 0.0,
            f"p75_hl_range_pct_{label}": 0.0,
            f"p90_hl_range_pct_{label}": 0.0,

            f"median_body_pct_{label}": 0.0,
            f"avg_body_pct_{label}": 0.0,
            f"p75_body_pct_{label}": 0.0,
            f"p90_body_pct_{label}": 0.0,

            f"median_body_efficiency_pct_{label}": 0.0,
            f"avg_body_efficiency_pct_{label}": 0.0,
            f"p75_body_efficiency_pct_{label}": 0.0,

            f"active_hl_day_pct_ge_1_{label}": 0.0,
            f"active_body_day_pct_ge_1_{label}": 0.0,
            f"extreme_hl_day_pct_gt_4_{label}": 0.0,
            f"extreme_body_day_pct_gt_4_{label}": 0.0,

            f"avg_traded_value_cr_{label}": 0.0,
            f"median_traded_value_cr_{label}": 0.0,
            f"p25_traded_value_cr_{label}": 0.0,
            f"avg_close_{label}": 0.0,
        }

    return {
        f"days_{label}": int(len(w)),

        f"median_hl_range_pct_{label}": safe_median(w["high_low_range_pct"]),
        f"avg_hl_range_pct_{label}": safe_mean(w["high_low_range_pct"]),
        f"p75_hl_range_pct_{label}": safe_quantile(w["high_low_range_pct"], 0.75),
        f"p90_hl_range_pct_{label}": safe_quantile(w["high_low_range_pct"], 0.90),

        f"median_body_pct_{label}": safe_median(w["body_pct"]),
        f"avg_body_pct_{label}": safe_mean(w["body_pct"]),
        f"p75_body_pct_{label}": safe_quantile(w["body_pct"], 0.75),
        f"p90_body_pct_{label}": safe_quantile(w["body_pct"], 0.90),

        f"median_body_efficiency_pct_{label}": safe_median(w["body_efficiency_pct"]),
        f"avg_body_efficiency_pct_{label}": safe_mean(w["body_efficiency_pct"]),
        f"p75_body_efficiency_pct_{label}": safe_quantile(w["body_efficiency_pct"], 0.75),

        f"active_hl_day_pct_ge_1_{label}": float((w["high_low_range_pct"] >= HL_RANGE_ACTIVE_1PCT).mean() * 100.0),
        f"active_body_day_pct_ge_1_{label}": float((w["body_pct"] >= BODY_ACTIVE_1PCT).mean() * 100.0),
        f"extreme_hl_day_pct_gt_4_{label}": float((w["high_low_range_pct"] > HL_RANGE_EXTREME_4PCT).mean() * 100.0),
        f"extreme_body_day_pct_gt_4_{label}": float((w["body_pct"] > BODY_EXTREME_4PCT).mean() * 100.0),

        f"avg_traded_value_cr_{label}": safe_mean(w["traded_value_cr"]),
        f"median_traded_value_cr_{label}": safe_median(w["traded_value_cr"]),
        f"p25_traded_value_cr_{label}": safe_quantile(w["traded_value_cr"], 0.25),
        f"avg_close_{label}": safe_mean(w["close"]),
    }


def calculate_stock_metrics(
    symbol: str,
    instrument_token: int,
    featured_df: pd.DataFrame,
    start_2y: date,
    start_1y: date,
    start_3m: date,
    to_date: date,
) -> Tuple[StockMetrics, pd.DataFrame]:
    """Calculate all stock metrics needed for final ranking."""
    w2y = window_slice(featured_df, start_2y, to_date)
    w1y = window_slice(featured_df, start_1y, to_date)
    w3m = window_slice(featured_df, start_3m, to_date)

    if w2y.empty:
        raise ValueError(f"No 2Y data available for {symbol}")

    m2 = calc_window_metrics(w2y, "2y")
    m1 = calc_window_metrics(w1y, "1y")
    m3 = calc_window_metrics(w3m, "3m")

    warnings: List[str] = []
    if int(m2["days_2y"]) < MIN_DAYS_2Y:
        warnings.append(f"2Y low coverage: {int(m2['days_2y'])} < {MIN_DAYS_2Y}")
    if int(m1["days_1y"]) < MIN_DAYS_1Y:
        warnings.append(f"1Y low coverage: {int(m1['days_1y'])} < {MIN_DAYS_1Y}")
    if int(m3["days_3m"]) < MIN_DAYS_3M:
        warnings.append(f"3M low coverage: {int(m3['days_3m'])} < {MIN_DAYS_3M}")

    avg_liq = float(m1["avg_traded_value_cr_1y"])
    median_liq = float(m1["median_traded_value_cr_1y"])
    p25_liq = float(m1["p25_traded_value_cr_1y"])

    liquidity_pass = (
        avg_liq >= MIN_AVG_TRADED_VALUE_CR
        and median_liq >= MIN_MEDIAN_TRADED_VALUE_CR
        and p25_liq >= MIN_P25_TRADED_VALUE_CR
    )

    if avg_liq < MIN_AVG_TRADED_VALUE_CR:
        warnings.append(f"1Y avg traded value low: {avg_liq:.1f} < {MIN_AVG_TRADED_VALUE_CR:.1f} Cr")
    if median_liq < MIN_MEDIAN_TRADED_VALUE_CR:
        warnings.append(f"1Y median traded value low: {median_liq:.1f} < {MIN_MEDIAN_TRADED_VALUE_CR:.1f} Cr")
    if p25_liq < MIN_P25_TRADED_VALUE_CR:
        warnings.append(f"1Y p25 traded value low: {p25_liq:.1f} < {MIN_P25_TRADED_VALUE_CR:.1f} Cr")

    latest_close = float(featured_df["close"].iloc[-1]) if not featured_df.empty else 0.0

    result = StockMetrics(
        symbol=symbol,
        instrument_token=instrument_token,

        first_date_2y=min(w2y["trade_date"]) if not w2y.empty else None,
        last_date_2y=max(w2y["trade_date"]) if not w2y.empty else None,

        days_2y=int(m2["days_2y"]),
        days_1y=int(m1["days_1y"]),
        days_3m=int(m3["days_3m"]),

        median_hl_range_pct_1y=float(m1["median_hl_range_pct_1y"]),
        median_hl_range_pct_2y=float(m2["median_hl_range_pct_2y"]),
        median_hl_range_pct_3m=float(m3["median_hl_range_pct_3m"]),
        avg_hl_range_pct_1y=float(m1["avg_hl_range_pct_1y"]),
        avg_hl_range_pct_2y=float(m2["avg_hl_range_pct_2y"]),
        avg_hl_range_pct_3m=float(m3["avg_hl_range_pct_3m"]),
        p75_hl_range_pct_1y=float(m1["p75_hl_range_pct_1y"]),
        p75_hl_range_pct_2y=float(m2["p75_hl_range_pct_2y"]),
        p75_hl_range_pct_3m=float(m3["p75_hl_range_pct_3m"]),
        p90_hl_range_pct_1y=float(m1["p90_hl_range_pct_1y"]),
        p90_hl_range_pct_2y=float(m2["p90_hl_range_pct_2y"]),
        p90_hl_range_pct_3m=float(m3["p90_hl_range_pct_3m"]),

        median_body_pct_1y=float(m1["median_body_pct_1y"]),
        median_body_pct_2y=float(m2["median_body_pct_2y"]),
        median_body_pct_3m=float(m3["median_body_pct_3m"]),
        avg_body_pct_1y=float(m1["avg_body_pct_1y"]),
        avg_body_pct_2y=float(m2["avg_body_pct_2y"]),
        avg_body_pct_3m=float(m3["avg_body_pct_3m"]),
        p75_body_pct_1y=float(m1["p75_body_pct_1y"]),
        p75_body_pct_2y=float(m2["p75_body_pct_2y"]),
        p75_body_pct_3m=float(m3["p75_body_pct_3m"]),
        p90_body_pct_1y=float(m1["p90_body_pct_1y"]),
        p90_body_pct_2y=float(m2["p90_body_pct_2y"]),
        p90_body_pct_3m=float(m3["p90_body_pct_3m"]),

        median_body_efficiency_pct_1y=float(m1["median_body_efficiency_pct_1y"]),
        median_body_efficiency_pct_2y=float(m2["median_body_efficiency_pct_2y"]),
        median_body_efficiency_pct_3m=float(m3["median_body_efficiency_pct_3m"]),
        avg_body_efficiency_pct_1y=float(m1["avg_body_efficiency_pct_1y"]),
        avg_body_efficiency_pct_2y=float(m2["avg_body_efficiency_pct_2y"]),
        avg_body_efficiency_pct_3m=float(m3["avg_body_efficiency_pct_3m"]),
        p75_body_efficiency_pct_1y=float(m1["p75_body_efficiency_pct_1y"]),
        p75_body_efficiency_pct_2y=float(m2["p75_body_efficiency_pct_2y"]),
        p75_body_efficiency_pct_3m=float(m3["p75_body_efficiency_pct_3m"]),

        active_hl_day_pct_ge_1_1y=float(m1["active_hl_day_pct_ge_1_1y"]),
        active_hl_day_pct_ge_1_2y=float(m2["active_hl_day_pct_ge_1_2y"]),
        active_hl_day_pct_ge_1_3m=float(m3["active_hl_day_pct_ge_1_3m"]),
        active_body_day_pct_ge_1_1y=float(m1["active_body_day_pct_ge_1_1y"]),
        active_body_day_pct_ge_1_2y=float(m2["active_body_day_pct_ge_1_2y"]),
        active_body_day_pct_ge_1_3m=float(m3["active_body_day_pct_ge_1_3m"]),

        extreme_hl_day_pct_gt_4_1y=float(m1["extreme_hl_day_pct_gt_4_1y"]),
        extreme_hl_day_pct_gt_4_2y=float(m2["extreme_hl_day_pct_gt_4_2y"]),
        extreme_hl_day_pct_gt_4_3m=float(m3["extreme_hl_day_pct_gt_4_3m"]),
        extreme_body_day_pct_gt_4_1y=float(m1["extreme_body_day_pct_gt_4_1y"]),
        extreme_body_day_pct_gt_4_2y=float(m2["extreme_body_day_pct_gt_4_2y"]),
        extreme_body_day_pct_gt_4_3m=float(m3["extreme_body_day_pct_gt_4_3m"]),

        avg_traded_value_cr_1y=avg_liq,
        avg_traded_value_cr_2y=float(m2["avg_traded_value_cr_2y"]),
        avg_traded_value_cr_3m=float(m3["avg_traded_value_cr_3m"]),
        median_traded_value_cr_1y=median_liq,
        p25_traded_value_cr_1y=p25_liq,

        latest_close=latest_close,
        avg_close_1y=float(m1["avg_close_1y"]),

        liquidity_pass=liquidity_pass,
        data_warning="; ".join(warnings),
    )

    keep_cols = [
        "symbol",
        "instrument_token",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "high_low_range_abs",
        "high_low_range_pct",
        "high_low_range_pct_vs_low",
        "body_abs",
        "body_pct",
        "body_signed_pct",
        "body_efficiency_pct",
        "typical_price",
        "traded_value_rs",
        "traded_value_cr",
        "prev_close",
        "gap_pct",
    ]
    return result, featured_df[keep_cols].copy()


# ============================================================
# RANKING MODEL
# ============================================================

def pct_rank_higher_better(s: pd.Series) -> pd.Series:
    """Percentile rank where higher value is better."""
    return s.rank(pct=True, method="average") * 100.0


def build_ranking(results: List[StockMetrics]) -> pd.DataFrame:
    """
    Build final combined ranking.

    Score components:

        High-low range            = 35%
            20% 1Y median high-low range
            10% 2Y median high-low range
             5% 3M median high-low range

        Open-close body           = 35%
            20% 1Y median body
            10% 2Y median body
             5% 3M median body

        Body efficiency           = 20%
            15% 1Y median body efficiency
             5% 3M median body efficiency

        Liquidity rank            = 10%
            10% 1Y average traded value rank

    Liquidity treatment:
        Liquidity is still a prerequisite. If avg/median/p25 traded-value
        thresholds fail, severe penalties are applied and final score is capped.
    """
    if not results:
        return pd.DataFrame()

    df = pd.DataFrame([r.__dict__ for r in results])

    # High-low range ranks.
    df["rank_1y_median_hl_range_pct"] = pct_rank_higher_better(df["median_hl_range_pct_1y"])
    df["rank_2y_median_hl_range_pct"] = pct_rank_higher_better(df["median_hl_range_pct_2y"])
    df["rank_3m_median_hl_range_pct"] = pct_rank_higher_better(df["median_hl_range_pct_3m"])

    # Open-close body ranks.
    df["rank_1y_median_body_pct"] = pct_rank_higher_better(df["median_body_pct_1y"])
    df["rank_2y_median_body_pct"] = pct_rank_higher_better(df["median_body_pct_2y"])
    df["rank_3m_median_body_pct"] = pct_rank_higher_better(df["median_body_pct_3m"])

    # Body efficiency ranks.
    df["rank_1y_median_body_efficiency_pct"] = pct_rank_higher_better(df["median_body_efficiency_pct_1y"])
    df["rank_3m_median_body_efficiency_pct"] = pct_rank_higher_better(df["median_body_efficiency_pct_3m"])

    # Traded value is log-ranked so ultra-high-turnover names do not dominate.
    df["log_avg_traded_value_cr_1y"] = df["avg_traded_value_cr_1y"].clip(lower=0).apply(lambda x: math.log1p(float(x)))
    df["rank_1y_avg_traded_value"] = pct_rank_higher_better(df["log_avg_traded_value_cr_1y"])

    df["raw_combined_score"] = (
        # High-low range = 35%
        0.20 * df["rank_1y_median_hl_range_pct"]
        + 0.10 * df["rank_2y_median_hl_range_pct"]
        + 0.05 * df["rank_3m_median_hl_range_pct"]

        # Open-close body = 35%
        + 0.20 * df["rank_1y_median_body_pct"]
        + 0.10 * df["rank_2y_median_body_pct"]
        + 0.05 * df["rank_3m_median_body_pct"]

        # Body efficiency = 20%
        + 0.15 * df["rank_1y_median_body_efficiency_pct"]
        + 0.05 * df["rank_3m_median_body_efficiency_pct"]

        # Liquidity rank = 10%
        + 0.10 * df["rank_1y_avg_traded_value"]
    )

    # Liquidity failure penalties. Independent and cumulative.
    df["avg_liquidity_fail"] = df["avg_traded_value_cr_1y"] < MIN_AVG_TRADED_VALUE_CR
    df["median_liquidity_fail"] = df["median_traded_value_cr_1y"] < MIN_MEDIAN_TRADED_VALUE_CR
    df["p25_liquidity_fail"] = df["p25_traded_value_cr_1y"] < MIN_P25_TRADED_VALUE_CR

    df["liquidity_penalty"] = (
        df["avg_liquidity_fail"].astype(float) * PENALTY_AVG_LIQUIDITY_FAIL
        + df["median_liquidity_fail"].astype(float) * PENALTY_MEDIAN_LIQUIDITY_FAIL
        + df["p25_liquidity_fail"].astype(float) * PENALTY_P25_LIQUIDITY_FAIL
    )

    # Data coverage penalties.
    df["data_coverage_penalty"] = (
        (df["days_2y"] < MIN_DAYS_2Y).astype(float) * 12.0
        + (df["days_1y"] < MIN_DAYS_1Y).astype(float) * 8.0
        + (df["days_3m"] < MIN_DAYS_3M).astype(float) * 5.0
    )

    df["selection_score_pre_cap"] = (
        df["raw_combined_score"]
        - df["liquidity_penalty"]
        - df["data_coverage_penalty"]
    ).clip(lower=0, upper=100)

    # Liquidity prerequisite cap.
    df["selection_score"] = df["selection_score_pre_cap"]
    df.loc[~df["liquidity_pass"], "selection_score"] = df.loc[
        ~df["liquidity_pass"], "selection_score"
    ].clip(upper=MAX_SCORE_IF_LIQUIDITY_FAIL)

    def classify(row) -> str:
        """Final verdict."""
        if row["days_2y"] < MIN_DAYS_2Y or row["days_1y"] < MIN_DAYS_1Y:
            return "REJECT_LOW_DATA"

        if not bool(row["liquidity_pass"]):
            return "REJECT_LOW_LIQUIDITY"

        if row["selection_score"] >= 80:
            return "STRONG_COMBINED_CANDIDATE"
        if row["selection_score"] >= 70:
            return "GOOD_COMBINED_CANDIDATE"
        if row["selection_score"] >= 60:
            return "WATCHLIST_COMBINED"
        return "REJECT_WEAK_COMBINED"

    df["verdict"] = df.apply(classify, axis=1)

    ordered_cols = [
        "symbol",
        "verdict",
        "selection_score",
        "raw_combined_score",
        "liquidity_penalty",
        "data_coverage_penalty",
        "selection_score_pre_cap",
        "liquidity_pass",

        # Main criteria.
        "median_hl_range_pct_1y",
        "median_hl_range_pct_2y",
        "median_hl_range_pct_3m",
        "median_body_pct_1y",
        "median_body_pct_2y",
        "median_body_pct_3m",
        "median_body_efficiency_pct_1y",
        "median_body_efficiency_pct_2y",
        "median_body_efficiency_pct_3m",
        "avg_traded_value_cr_1y",

        # Liquidity diagnostics.
        "median_traded_value_cr_1y",
        "p25_traded_value_cr_1y",
        "avg_traded_value_cr_2y",
        "avg_traded_value_cr_3m",
        "avg_liquidity_fail",
        "median_liquidity_fail",
        "p25_liquidity_fail",

        # Supporting high-low diagnostics.
        "avg_hl_range_pct_1y",
        "avg_hl_range_pct_2y",
        "avg_hl_range_pct_3m",
        "p75_hl_range_pct_1y",
        "p75_hl_range_pct_2y",
        "p75_hl_range_pct_3m",
        "p90_hl_range_pct_1y",
        "p90_hl_range_pct_2y",
        "p90_hl_range_pct_3m",
        "active_hl_day_pct_ge_1_1y",
        "active_hl_day_pct_ge_1_2y",
        "active_hl_day_pct_ge_1_3m",
        "extreme_hl_day_pct_gt_4_1y",
        "extreme_hl_day_pct_gt_4_2y",
        "extreme_hl_day_pct_gt_4_3m",

        # Supporting body diagnostics.
        "avg_body_pct_1y",
        "avg_body_pct_2y",
        "avg_body_pct_3m",
        "p75_body_pct_1y",
        "p75_body_pct_2y",
        "p75_body_pct_3m",
        "p90_body_pct_1y",
        "p90_body_pct_2y",
        "p90_body_pct_3m",
        "active_body_day_pct_ge_1_1y",
        "active_body_day_pct_ge_1_2y",
        "active_body_day_pct_ge_1_3m",
        "extreme_body_day_pct_gt_4_1y",
        "extreme_body_day_pct_gt_4_2y",
        "extreme_body_day_pct_gt_4_3m",

        # Body efficiency diagnostics.
        "avg_body_efficiency_pct_1y",
        "avg_body_efficiency_pct_2y",
        "avg_body_efficiency_pct_3m",
        "p75_body_efficiency_pct_1y",
        "p75_body_efficiency_pct_2y",
        "p75_body_efficiency_pct_3m",

        # Coverage and price.
        "days_2y",
        "days_1y",
        "days_3m",
        "first_date_2y",
        "last_date_2y",
        "latest_close",
        "avg_close_1y",

        # Score component ranks.
        "rank_1y_median_hl_range_pct",
        "rank_2y_median_hl_range_pct",
        "rank_3m_median_hl_range_pct",
        "rank_1y_median_body_pct",
        "rank_2y_median_body_pct",
        "rank_3m_median_body_pct",
        "rank_1y_median_body_efficiency_pct",
        "rank_3m_median_body_efficiency_pct",
        "rank_1y_avg_traded_value",

        "instrument_token",
        "data_warning",
    ]

    df = df[ordered_cols].sort_values(
        by=[
            "selection_score",
            "raw_combined_score",
            "median_hl_range_pct_1y",
            "median_body_pct_1y",
            "median_body_efficiency_pct_1y",
            "avg_traded_value_cr_1y",
        ],
        ascending=[False, False, False, False, False, False],
    ).reset_index(drop=True)

    df.insert(0, "rank", range(1, len(df) + 1))
    return df


# ============================================================
# EXCEL OUTPUT
# ============================================================

def autosize_excel_columns(writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame) -> None:
    """Auto-size worksheet columns."""
    ws = writer.sheets[sheet_name]
    for idx, col in enumerate(df.columns):
        series = df[col].astype(str) if not df.empty else pd.Series([], dtype=str)
        max_len = max([len(str(col))] + [len(x) for x in series.head(500).tolist()])
        ws.set_column(idx, idx, min(max_len + 2, 50))


def write_excel_report(
    ranking_df: pd.DataFrame,
    daily_features_df: pd.DataFrame,
    failed_df: pd.DataFrame,
    config_df: pd.DataFrame,
    output_path: str,
) -> None:
    """Write final Excel report."""
    top_df = ranking_df.head(TOP_N_TO_PRINT).copy() if not ranking_df.empty else pd.DataFrame()

    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        ranking_df.to_excel(writer, index=False, sheet_name="ranked_stocks")
        top_df.to_excel(writer, index=False, sheet_name="top_candidates")
        daily_features_df.to_excel(writer, index=False, sheet_name="daily_features")
        failed_df.to_excel(writer, index=False, sheet_name="missing_or_failed")
        config_df.to_excel(writer, index=False, sheet_name="config")

        workbook = writer.book
        fmt_num = workbook.add_format({"num_format": "0.00"})
        fmt_score = workbook.add_format({"num_format": "0.00"})

        for sheet_name, df in [
            ("ranked_stocks", ranking_df),
            ("top_candidates", top_df),
            ("daily_features", daily_features_df),
            ("missing_or_failed", failed_df),
            ("config", config_df),
        ]:
            autosize_excel_columns(writer, sheet_name, df)
            ws = writer.sheets[sheet_name]
            ws.freeze_panes(1, 0)

            if not df.empty:
                ws.autofilter(0, 0, len(df), len(df.columns) - 1)

                for col_name in df.columns:
                    idx = df.columns.get_loc(col_name)
                    lower = col_name.lower()
                    if (
                        "pct" in lower
                        or "score" in lower
                        or "rank_" in lower
                        or "value_cr" in lower
                        or "efficiency" in lower
                    ):
                        ws.set_column(idx, idx, 17, fmt_num)

                    if col_name in {"selection_score", "raw_combined_score"}:
                        ws.set_column(idx, idx, 19, fmt_score)


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    """Script entrypoint."""
    print("============================================================")
    print("STOCK INTRADAY COMBINED CANDIDATE SELECTOR")
    print("HIGH-LOW RANGE + OPEN-CLOSE BODY + BODY EFFICIENCY + LIQUIDITY")
    print("============================================================")

    from_date_buffered, start_2y, start_1y, start_3m, to_date = compute_master_date_range()

    print(f"[CONFIG] Input CSV                      : {INPUT_CSV}")
    print(f"[CONFIG] Output Excel                   : {OUTPUT_EXCEL}")
    print(f"[CONFIG] Exchange                       : {EXCHANGE}")
    print(f"[CONFIG] Download from                  : {from_date_buffered}")
    print(f"[CONFIG] Analysis 2Y start              : {start_2y}")
    print(f"[CONFIG] Analysis 1Y start              : {start_1y}")
    print(f"[CONFIG] Analysis 3M start              : {start_3m}")
    print(f"[CONFIG] Analysis end                   : {to_date}")
    print(f"[CONFIG] Min avg traded value 1Y        : {MIN_AVG_TRADED_VALUE_CR:.1f} Cr")
    print(f"[CONFIG] Min median traded value 1Y     : {MIN_MEDIAN_TRADED_VALUE_CR:.1f} Cr")
    print(f"[CONFIG] Min p25 traded value 1Y        : {MIN_P25_TRADED_VALUE_CR:.1f} Cr")
    print(f"[CONFIG] Max score if liquidity fails   : {MAX_SCORE_IF_LIQUIDITY_FAIL:.1f}")
    print(f"[CONFIG] Force refresh                  : {FORCE_REFRESH}")
    print("============================================================")

    print("[STEP] Reading stock symbols ...")
    symbols = read_stock_symbols(INPUT_CSV)
    print(f"[INFO] Symbols loaded: {len(symbols):,}")
    print(", ".join(symbols[:80]) + (" ..." if len(symbols) > 80 else ""))

    print("\n[STEP] Initializing Kite API ...")
    kite = initialize_kite()
    print("[INFO] Kite API initialized.")

    instruments_cache: Dict[str, List[Dict]] = {}
    results: List[StockMetrics] = []
    daily_frames: List[pd.DataFrame] = []
    failed_rows: List[Dict] = []

    for idx, symbol in enumerate(symbols, start=1):
        print("\n------------------------------------------------------------")
        print(f"[STOCK {idx}/{len(symbols)}] {symbol}")
        print("------------------------------------------------------------")

        try:
            token, _inst = get_equity_token(kite, symbol, EXCHANGE, instruments_cache)
            print(f"[INFO] Token: {token}")

            raw_df = load_cached_if_usable(symbol, from_date_buffered, to_date)
            if raw_df is None:
                rows = fetch_history_day(kite, token, from_date_buffered, to_date, f"{EXCHANGE}:{symbol}")
                raw_df = rows_to_dataframe(rows)
                if raw_df.empty:
                    raise RuntimeError(f"No daily candles returned for {symbol}")
                save_cache(symbol, raw_df)

            raw_df = raw_df[(raw_df["trade_date"] >= from_date_buffered) & (raw_df["trade_date"] <= to_date)].copy()
            featured_df = add_daily_features(symbol, token, raw_df)

            result, daily_df = calculate_stock_metrics(
                symbol=symbol,
                instrument_token=token,
                featured_df=featured_df,
                start_2y=start_2y,
                start_1y=start_1y,
                start_3m=start_3m,
                to_date=to_date,
            )

            results.append(result)
            daily_frames.append(daily_df)

            print(
                f"[OK] {symbol}: "
                f"HL_1Y={result.median_hl_range_pct_1y:.2f}%, "
                f"Body_1Y={result.median_body_pct_1y:.2f}%, "
                f"Eff_1Y={result.median_body_efficiency_pct_1y:.1f}%, "
                f"Value_1Y={result.avg_traded_value_cr_1y:.1f} Cr, "
                f"liquidity_pass={result.liquidity_pass}"
            )

            if result.data_warning:
                print(f"[WARN] {result.data_warning}")

        except Exception as exc:
            print(f"[ERROR] {symbol} failed: {exc}")
            failed_rows.append({"symbol": symbol, "error": str(exc)})

    print("\n[STEP] Building combined ranking ...")
    ranking_df = build_ranking(results)

    if daily_frames:
        daily_features_df = pd.concat(daily_frames, ignore_index=True)
        daily_features_df = daily_features_df.sort_values(["symbol", "trade_date"]).reset_index(drop=True)
    else:
        daily_features_df = pd.DataFrame()

    failed_df = pd.DataFrame(failed_rows)

    config_df = pd.DataFrame(
        [
            {"parameter": "INPUT_CSV", "value": INPUT_CSV},
            {"parameter": "OUTPUT_EXCEL", "value": OUTPUT_EXCEL},
            {"parameter": "EXCHANGE", "value": EXCHANGE},
            {"parameter": "FROM_DATE_BUFFERED", "value": str(from_date_buffered)},
            {"parameter": "START_2Y", "value": str(start_2y)},
            {"parameter": "START_1Y", "value": str(start_1y)},
            {"parameter": "START_3M", "value": str(start_3m)},
            {"parameter": "TO_DATE", "value": str(to_date)},
            {"parameter": "HL_RANGE_FORMULA", "value": "high_low_range_pct=(high-low)/close*100"},
            {"parameter": "BODY_FORMULA", "value": "body_pct=abs(close-open)/open*100"},
            {"parameter": "BODY_EFFICIENCY_FORMULA", "value": "body_efficiency_pct=abs(close-open)/(high-low)*100"},
            {"parameter": "TRADED_VALUE_FORMULA", "value": "typical_price=(high+low+close)/3; traded_value_cr=typical_price*volume/1e7"},
            {"parameter": "HL_RANGE_ACTIVE_1PCT", "value": HL_RANGE_ACTIVE_1PCT},
            {"parameter": "BODY_ACTIVE_1PCT", "value": BODY_ACTIVE_1PCT},
            {"parameter": "HL_RANGE_EXTREME_4PCT", "value": HL_RANGE_EXTREME_4PCT},
            {"parameter": "BODY_EXTREME_4PCT", "value": BODY_EXTREME_4PCT},
            {"parameter": "MIN_AVG_TRADED_VALUE_CR", "value": MIN_AVG_TRADED_VALUE_CR},
            {"parameter": "MIN_MEDIAN_TRADED_VALUE_CR", "value": MIN_MEDIAN_TRADED_VALUE_CR},
            {"parameter": "MIN_P25_TRADED_VALUE_CR", "value": MIN_P25_TRADED_VALUE_CR},
            {"parameter": "MAX_SCORE_IF_LIQUIDITY_FAIL", "value": MAX_SCORE_IF_LIQUIDITY_FAIL},
            {"parameter": "PENALTY_AVG_LIQUIDITY_FAIL", "value": PENALTY_AVG_LIQUIDITY_FAIL},
            {"parameter": "PENALTY_MEDIAN_LIQUIDITY_FAIL", "value": PENALTY_MEDIAN_LIQUIDITY_FAIL},
            {"parameter": "PENALTY_P25_LIQUIDITY_FAIL", "value": PENALTY_P25_LIQUIDITY_FAIL},
            {"parameter": "MIN_DAYS_2Y", "value": MIN_DAYS_2Y},
            {"parameter": "MIN_DAYS_1Y", "value": MIN_DAYS_1Y},
            {"parameter": "MIN_DAYS_3M", "value": MIN_DAYS_3M},
            {"parameter": "TOP_N_TO_PRINT", "value": TOP_N_TO_PRINT},
            {"parameter": "DATA_CACHE_DIR", "value": DATA_CACHE_DIR},
            {"parameter": "FORCE_REFRESH", "value": FORCE_REFRESH},
            {
                "parameter": "COMBINED_SCORING_WEIGHTS",
                "value": (
                    "35% high-low range: 20% 1Y + 10% 2Y + 5% 3M; "
                    "35% open-close body: 20% 1Y + 10% 2Y + 5% 3M; "
                    "20% body efficiency: 15% 1Y + 5% 3M; "
                    "10% 1Y avg traded value; liquidity failures penalized and capped"
                ),
            },
        ]
    )

    print("[STEP] Writing Excel report ...")
    write_excel_report(ranking_df, daily_features_df, failed_df, config_df, OUTPUT_EXCEL)

    print("\n==================== FINAL RESULT ====================")
    if ranking_df.empty:
        print("[ERROR] No stocks could be ranked.")
    else:
        top_cols = [
            "rank",
            "symbol",
            "verdict",
            "selection_score",
            "raw_combined_score",
            "liquidity_pass",
            "median_hl_range_pct_1y",
            "median_body_pct_1y",
            "median_body_efficiency_pct_1y",
            "avg_traded_value_cr_1y",
            "median_traded_value_cr_1y",
            "p25_traded_value_cr_1y",
        ]
        print(f"Top {TOP_N_TO_PRINT} candidates:")
        print(ranking_df[top_cols].head(TOP_N_TO_PRINT).to_string(index=False))

    print(f"\n[DONE] Excel saved: {OUTPUT_EXCEL}")
    print("======================================================")


if __name__ == "__main__":
    main()