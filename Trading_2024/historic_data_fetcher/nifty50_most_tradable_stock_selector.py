#!/usr/bin/env python3
"""
index_underlying_3yr_movement_analyzer.py

Purpose
-------
Analyze the UNDERLYING/SPOT movement of:

    1. NIFTY
    2. SENSEX
    3. BANKNIFTY

over the last 3 years.

This script intentionally ignores options.

It answers:

    Which underlying index gives the best intraday movement profile?

It calculates for every daily candle:

    1. High-low range
    2. Open-close body movement
    3. Body efficiency
    4. Gap from previous close
    5. Close location inside daily range
    6. True range / ATR
    7. Month-wise and year-wise movement behaviour

Output Excel sheets
-------------------
1. ranked_indices
2. daily_features
3. month_summary
4. yearly_summary
5. weekday_summary
6. month_range_distribution
7. month_body_distribution
8. missing_or_failed
9. config

Core formulas
-------------
High-low range:

    high_low_range_abs = high - low
    high_low_range_pct = (high - low) / close * 100

Open-close body:

    body_abs = abs(close - open)
    body_pct = abs(close - open) / open * 100
    body_signed_pct = (close - open) / open * 100

Body efficiency:

    body_efficiency_pct = abs(close - open) / (high - low) * 100

Close location:

    close_location_pct = (close - low) / (high - low) * 100

Interpretation:
    close_location_pct near 100 = closed near high
    close_location_pct near 0   = closed near low
    close_location_pct near 50  = closed near middle

Run
---
    python index_underlying_3yr_movement_analyzer.py

Optional overrides
------------------
    set INDEX_SYMBOLS=NIFTY,SENSEX,BANKNIFTY
    set LOOKBACK_YEARS=3
    set FORCE_REFRESH=1
    set OUTPUT_EXCEL=index_underlying_3yr_movement_analysis.xlsx
"""

from __future__ import annotations

import os
import time
import traceback
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

try:
    import Trading_2024.OptionTradeUtils as oUtils  # type: ignore
except Exception:
    try:
        import OptionTradeUtils as oUtils  # type: ignore
    except Exception as exc:
        oUtils = None  # type: ignore
        print(f"[WARN] Could not import OptionTradeUtils: {exc}")

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # type: ignore


# ============================================================
# CONFIG
# ============================================================

INDEX_SYMBOLS = [
    x.strip().upper()
    for x in os.environ.get("INDEX_SYMBOLS", "NIFTY,SENSEX,BANKNIFTY").split(",")
    if x.strip()
]

LOOKBACK_YEARS = int(os.environ.get("LOOKBACK_YEARS", "3"))

OUTPUT_EXCEL = os.environ.get(
    "OUTPUT_EXCEL",
    "index_underlying_3yr_movement_analysis.xlsx",
).strip()

DATA_CACHE_DIR = os.environ.get(
    "DATA_CACHE_DIR",
    "./index_underlying_daily_cache",
).strip()

FORCE_REFRESH = os.environ.get("FORCE_REFRESH", "0").strip() == "1"

# By default, historical analysis ends at previous completed day.
# This avoids today's incomplete daily candle.
USE_TODAY_DAILY_CANDLE = os.environ.get("USE_TODAY_DAILY_CANDLE", "0").strip() == "1"

# API chunking/retry.
MAX_DAYS_PER_CHUNK = 365
MAX_ATTEMPTS = 5
SLEEP_BETWEEN_CALLS_SEC = 0.25

# Movement thresholds for index underlyings.
# These are lower than stock thresholds because indices move less than stocks.
HL_ACTIVE_THRESHOLD_PCT = float(os.environ.get("HL_ACTIVE_THRESHOLD_PCT", "0.75"))
HL_STRONG_THRESHOLD_PCT = float(os.environ.get("HL_STRONG_THRESHOLD_PCT", "1.25"))
BODY_ACTIVE_THRESHOLD_PCT = float(os.environ.get("BODY_ACTIVE_THRESHOLD_PCT", "0.35"))
BODY_STRONG_THRESHOLD_PCT = float(os.environ.get("BODY_STRONG_THRESHOLD_PCT", "0.75"))

# Rolling windows.
ROLLING_SHORT = 5
ROLLING_MEDIUM = 20
ATR_PERIOD = 14

# Buckets for monthly distribution.
RANGE_BUCKETS = [-np.inf, 0.5, 0.75, 1.0, 1.25, 1.5, 2.0, np.inf]
RANGE_BUCKET_LABELS = ["<=0.5%", "0.5-0.75%", "0.75-1%", "1-1.25%", "1.25-1.5%", "1.5-2%", ">2%"]

BODY_BUCKETS = [-np.inf, 0.25, 0.35, 0.50, 0.75, 1.00, 1.50, np.inf]
BODY_BUCKET_LABELS = ["<=0.25%", "0.25-0.35%", "0.35-0.5%", "0.5-0.75%", "0.75-1%", "1-1.5%", ">1.5%"]


# ============================================================
# INDEX CONFIG
# ============================================================

INDEX_CONFIG: Dict[str, Dict[str, Any]] = {
    "NIFTY": {
        "display": "NIFTY",
        "spot_exchange": "NSE",
        "spot_tradingsymbols": ["NIFTY 50", "NIFTY50", "NIFTY"],
    },
    "BANKNIFTY": {
        "display": "BANKNIFTY",
        "spot_exchange": "NSE",
        "spot_tradingsymbols": ["NIFTY BANK", "BANKNIFTY", "NIFTYBANK"],
    },
    "SENSEX": {
        "display": "SENSEX",
        "spot_exchange": "BSE",
        "spot_tradingsymbols": ["SENSEX"],
    },
}


# ============================================================
# DATA CLASSES
# ============================================================

@dataclass
class SpotIndexInfo:
    """Resolved spot index metadata."""

    index_symbol: str
    display: str
    spot_exchange: str
    spot_tradingsymbol: str
    spot_kite_key: str
    spot_token: int


@dataclass
class IndexRankMetrics:
    """Final index-level ranking metrics."""

    index_symbol: str
    spot_kite_key: str
    spot_token: int

    first_date: Optional[date]
    last_date: Optional[date]
    trading_days: int

    # 3Y/full-period movement.
    median_hl_range_pct_3y: float
    avg_hl_range_pct_3y: float
    p75_hl_range_pct_3y: float
    p90_hl_range_pct_3y: float

    median_body_pct_3y: float
    avg_body_pct_3y: float
    p75_body_pct_3y: float
    p90_body_pct_3y: float

    median_body_efficiency_pct_3y: float
    avg_body_efficiency_pct_3y: float

    active_hl_day_pct_3y: float
    strong_hl_day_pct_3y: float
    active_body_day_pct_3y: float
    strong_body_day_pct_3y: float
    strong_directional_day_pct_3y: float

    # 1Y movement.
    median_hl_range_pct_1y: float
    median_body_pct_1y: float
    median_body_efficiency_pct_1y: float
    active_hl_day_pct_1y: float
    active_body_day_pct_1y: float
    strong_directional_day_pct_1y: float

    # 3M movement.
    median_hl_range_pct_3m: float
    median_body_pct_3m: float
    median_body_efficiency_pct_3m: float
    active_hl_day_pct_3m: float
    active_body_day_pct_3m: float
    strong_directional_day_pct_3m: float

    # Directional behaviour.
    green_day_pct_3y: float
    red_day_pct_3y: float
    avg_abs_gap_pct_3y: float
    median_true_range_pct_3y: float
    avg_atr14_pct_3y: float

    latest_close: float
    data_warning: str


# ============================================================
# TIME HELPERS
# ============================================================

def ist_today() -> date:
    """Return current IST date."""
    if ZoneInfo is not None:
        try:
            return datetime.now(ZoneInfo("Asia/Kolkata")).date()
        except Exception:
            pass
    return datetime.now().date()


def to_ist_naive_timestamp(value: Any) -> pd.Timestamp:
    """Convert Kite/Python/Pandas datetime to timezone-naive IST timestamp."""
    try:
        ts = pd.Timestamp(value)

        if pd.isna(ts):
            return pd.NaT

        if ts.tzinfo is not None:
            ts = ts.tz_convert("Asia/Kolkata").tz_localize(None)

        return pd.Timestamp(ts)

    except Exception:
        return pd.NaT


def make_excel_safe_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make dataframe safe for Excel.

    Excel does not support timezone-aware datetimes.
    """
    if df is None or df.empty:
        return df.copy() if df is not None else pd.DataFrame()

    out = df.copy()
    out = out.replace([np.inf, -np.inf], np.nan)

    for col in out.columns:
        s = out[col]

        if pd.api.types.is_datetime64_any_dtype(s):
            try:
                if getattr(s.dt, "tz", None) is not None:
                    out[col] = s.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
                else:
                    out[col] = pd.to_datetime(s, errors="coerce")
            except Exception:
                out[col] = pd.to_datetime(s.astype(str), errors="coerce")

        elif s.dtype == "object":
            sample = s.dropna().head(10).tolist()
            if any(isinstance(x, (datetime, date, pd.Timestamp)) for x in sample):
                out[col] = pd.to_datetime(
                    [to_ist_naive_timestamp(x) for x in s.tolist()],
                    errors="coerce",
                )

    return out


def compute_date_range() -> Tuple[date, date, date, date]:
    """
    Compute date windows.

    Returns:
        from_date_buffered, analysis_start_3y, start_1y, start_3m, to_date
    """
    today = ist_today()
    to_date = today if USE_TODAY_DAILY_CANDLE else today - timedelta(days=1)

    analysis_start_3y = to_date - timedelta(days=365 * LOOKBACK_YEARS)
    start_1y = to_date - timedelta(days=365)
    start_3m = to_date - timedelta(days=92)
    from_date_buffered = analysis_start_3y - timedelta(days=10)

    return from_date_buffered, analysis_start_3y, start_1y, start_3m, to_date


def iter_date_chunks(from_date: date, to_date: date, days_per_chunk: int) -> List[Tuple[date, date]]:
    """Split date range into API-friendly chunks."""
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
# KITE HELPERS
# ============================================================

def initialize_kite():
    """Initialize Kite API using your existing helper."""
    if oUtils is None:
        raise RuntimeError(
            "OptionTradeUtils is not importable. Ensure Trading_2024.OptionTradeUtils "
            "or OptionTradeUtils.py is available."
        )

    kite = oUtils.intialize_kite_api()

    if kite is None:
        raise RuntimeError("intialize_kite_api() returned None. Check Kite login.")

    return kite


def kite_instruments_cached(
    kite,
    exchange: str,
    cache: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    """Load Kite instruments dump once per exchange."""
    ex = exchange.upper().strip()

    if ex not in cache:
        print(f"[STEP] Loading instruments dump for {ex} ...")
        cache[ex] = kite.instruments(ex)
        print(f"[INFO] Instrument rows on {ex}: {len(cache[ex]):,}")

    return cache[ex]


def resolve_spot_index(
    kite,
    index_symbol: str,
    instruments_cache: Dict[str, List[Dict[str, Any]]],
) -> SpotIndexInfo:
    """
    Resolve spot index token.

    Expected mappings:
        NIFTY     -> NSE:NIFTY 50
        BANKNIFTY -> NSE:NIFTY BANK
        SENSEX    -> BSE:SENSEX
    """
    index_symbol = index_symbol.upper().strip()

    if index_symbol not in INDEX_CONFIG:
        raise ValueError(f"Unsupported index symbol: {index_symbol}")

    cfg = INDEX_CONFIG[index_symbol]
    exchange = cfg["spot_exchange"]
    candidates = [x.upper() for x in cfg["spot_tradingsymbols"]]

    instruments = kite_instruments_cached(kite, exchange, instruments_cache)

    matches: List[Dict[str, Any]] = []

    for inst in instruments:
        tsym = str(inst.get("tradingsymbol", "")).upper().strip()
        name = str(inst.get("name", "")).upper().strip()

        if tsym in candidates or name in candidates:
            matches.append(inst)

    if not matches:
        raise ValueError(
            f"Could not resolve {index_symbol}. Tried {exchange}:{cfg['spot_tradingsymbols']}"
        )

    def priority(inst: Dict[str, Any]) -> int:
        tsym = str(inst.get("tradingsymbol", "")).upper().strip()
        segment = str(inst.get("segment", "")).upper().strip()
        instrument_type = str(inst.get("instrument_type", "")).upper().strip()

        if tsym in candidates and segment == "INDICES":
            return 0
        if tsym in candidates:
            return 1
        if segment == "INDICES":
            return 2
        if instrument_type == "EQ":
            return 3
        return 4

    matches.sort(key=priority)
    selected = matches[0]

    spot_tradingsymbol = str(selected.get("tradingsymbol")).strip()
    spot_token = int(selected["instrument_token"])

    return SpotIndexInfo(
        index_symbol=index_symbol,
        display=cfg["display"],
        spot_exchange=exchange,
        spot_tradingsymbol=spot_tradingsymbol,
        spot_kite_key=f"{exchange}:{spot_tradingsymbol}",
        spot_token=spot_token,
    )


# ============================================================
# HISTORICAL DOWNLOAD / CACHE
# ============================================================

def cache_path_for_index(index_symbol: str) -> str:
    """Return cache path for one index."""
    safe = index_symbol.replace("/", "_").replace("\\", "_").replace(":", "_")
    return os.path.join(DATA_CACHE_DIR, f"{safe}_underlying_daily.pkl")


def normalize_cached_df(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize cached dataframe."""
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()

    if "date" in out.columns:
        out["date"] = pd.to_datetime(
            [to_ist_naive_timestamp(x) for x in out["date"].tolist()],
            errors="coerce",
        )

    if "trade_date" not in out.columns and "date" in out.columns:
        out["trade_date"] = out["date"].dt.date

    return out


def load_cached_if_usable(index_symbol: str, from_date: date, to_date: date) -> Optional[pd.DataFrame]:
    """Load cached data if it covers the required date range."""
    if FORCE_REFRESH:
        return None

    path = cache_path_for_index(index_symbol)

    if not os.path.exists(path):
        return None

    try:
        df = normalize_cached_df(pd.read_pickle(path))

        if df.empty or "trade_date" not in df.columns:
            return None

        min_d = min(pd.to_datetime(df["trade_date"]).dt.date)
        max_d = max(pd.to_datetime(df["trade_date"]).dt.date)

        # Allow last trading day to lag because of weekends/holidays.
        if min_d <= from_date and max_d >= to_date - timedelta(days=7):
            print(f"[CACHE] Using {index_symbol}: {path}")
            return df

        print(f"[CACHE] Stale/incomplete for {index_symbol}: {min_d} -> {max_d}. Re-downloading.")
        return None

    except Exception as exc:
        print(f"[WARN] Could not read cache for {index_symbol}: {exc}")
        return None


def save_cache(index_symbol: str, df: pd.DataFrame) -> None:
    """Save dataframe cache."""
    os.makedirs(DATA_CACHE_DIR, exist_ok=True)
    df.to_pickle(cache_path_for_index(index_symbol))


def fetch_history_day(
    kite,
    instrument_token: int,
    from_date: date,
    to_date: date,
    label: str,
) -> List[Dict[str, Any]]:
    """Fetch daily historical candles from Kite with retries."""
    chunks = iter_date_chunks(from_date, to_date, MAX_DAYS_PER_CHUNK)

    print(
        f"[INFO] Fetching daily data for {label} token={instrument_token} "
        f"from {from_date} to {to_date} in {len(chunks)} chunk(s)."
    )

    all_rows: List[Dict[str, Any]] = []

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

                print(f"    [OK] {len(rows)} candles on attempt {attempt}")
                all_rows.extend(rows)
                last_err = None
                break

            except Exception as exc:
                last_err = exc
                wait = min(8.0, 1.5 * attempt)
                print(f"    [WARN] Attempt {attempt}/{MAX_ATTEMPTS} failed: {exc}. Sleeping {wait:.1f}s")
                time.sleep(wait)

        if last_err is not None:
            print(f"    [ERROR] Giving up on chunk {idx}: {last_err}")

        time.sleep(SLEEP_BETWEEN_CALLS_SEC)

    return all_rows


def rows_to_dataframe(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    """Convert Kite historical rows to clean OHLC dataframe."""
    cols = ["date", "open", "high", "low", "close", "volume"]

    if not rows:
        return pd.DataFrame(columns=cols)

    df = pd.DataFrame(rows)

    for col in cols:
        if col not in df.columns:
            df[col] = np.nan

    df["date"] = pd.to_datetime(
        [to_ist_naive_timestamp(x) for x in df["date"].tolist()],
        errors="coerce",
    )

    df = df.dropna(subset=["date"]).copy()
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


# ============================================================
# FEATURE ENGINEERING
# ============================================================

def add_daily_features(index_symbol: str, info: SpotIndexInfo, raw_df: pd.DataFrame) -> pd.DataFrame:
    """Add underlying daily movement features."""
    if raw_df.empty:
        raise ValueError(f"No data for {index_symbol}")

    df = raw_df.copy()

    df["date"] = pd.to_datetime(
        [to_ist_naive_timestamp(x) for x in df["date"].tolist()],
        errors="coerce",
    )
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date

    df = df.dropna(subset=["date", "trade_date"]).copy()

    df["index_symbol"] = index_symbol
    df["spot_exchange"] = info.spot_exchange
    df["spot_tradingsymbol"] = info.spot_tradingsymbol
    df["spot_kite_key"] = info.spot_kite_key
    df["spot_token"] = info.spot_token

    # Calendar columns.
    df["trade_date_dt"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df["year"] = df["trade_date_dt"].dt.year
    df["month"] = df["trade_date_dt"].dt.month
    df["month_name"] = df["trade_date_dt"].dt.strftime("%b")
    df["year_month"] = df["trade_date_dt"].dt.to_period("M").astype(str)
    df["weekday"] = df["trade_date_dt"].dt.day_name()

    # Previous close and gap.
    df["prev_close"] = df["close"].shift(1)
    df["gap_abs"] = df["open"] - df["prev_close"]
    df["gap_pct"] = (df["gap_abs"] / df["prev_close"]) * 100.0
    df["abs_gap_pct"] = df["gap_pct"].abs()

    # Close-to-close return.
    df["close_to_close_ret_pct"] = ((df["close"] / df["prev_close"]) - 1.0) * 100.0

    # High-low range.
    df["high_low_range_abs"] = df["high"] - df["low"]
    df["high_low_range_pct"] = (df["high_low_range_abs"] / df["close"]) * 100.0
    df["high_low_range_pct_vs_low"] = ((df["high"] / df["low"]) - 1.0) * 100.0

    # Open-close body.
    df["body_signed_abs"] = df["close"] - df["open"]
    df["body_signed_pct"] = (df["body_signed_abs"] / df["open"]) * 100.0
    df["body_abs"] = df["body_signed_abs"].abs()
    df["body_pct"] = (df["body_abs"] / df["open"]) * 100.0

    # Body efficiency.
    df["body_efficiency_pct"] = np.where(
        df["high_low_range_abs"] > 0,
        (df["body_abs"] / df["high_low_range_abs"]) * 100.0,
        0.0,
    )
    df["body_efficiency_pct"] = df["body_efficiency_pct"].clip(lower=0.0, upper=100.0)

    # Close/open location inside the candle range.
    df["close_location_pct"] = np.where(
        df["high_low_range_abs"] > 0,
        ((df["close"] - df["low"]) / df["high_low_range_abs"]) * 100.0,
        50.0,
    )
    df["open_location_pct"] = np.where(
        df["high_low_range_abs"] > 0,
        ((df["open"] - df["low"]) / df["high_low_range_abs"]) * 100.0,
        50.0,
    )

    df["close_location_pct"] = df["close_location_pct"].clip(0.0, 100.0)
    df["open_location_pct"] = df["open_location_pct"].clip(0.0, 100.0)

    # Candle direction.
    df["day_type"] = np.select(
        [df["body_signed_pct"] > 0.05, df["body_signed_pct"] < -0.05],
        ["GREEN", "RED"],
        default="DOJI_OR_FLAT",
    )

    # Activity flags.
    df["active_hl_day"] = df["high_low_range_pct"] >= HL_ACTIVE_THRESHOLD_PCT
    df["strong_hl_day"] = df["high_low_range_pct"] >= HL_STRONG_THRESHOLD_PCT

    df["active_body_day"] = df["body_pct"] >= BODY_ACTIVE_THRESHOLD_PCT
    df["strong_body_day"] = df["body_pct"] >= BODY_STRONG_THRESHOLD_PCT

    df["strong_directional_day"] = (
        (df["body_efficiency_pct"] >= 50.0)
        & (df["body_pct"] >= BODY_ACTIVE_THRESHOLD_PCT)
    )

    # True range and ATR.
    tr1 = df["high"] - df["low"]
    tr2 = (df["high"] - df["prev_close"]).abs()
    tr3 = (df["low"] - df["prev_close"]).abs()

    df["true_range_abs"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df["true_range_pct"] = (df["true_range_abs"] / df["close"]) * 100.0

    df["atr14_abs"] = df["true_range_abs"].rolling(ATR_PERIOD, min_periods=5).mean()
    df["atr14_pct"] = (df["atr14_abs"] / df["close"]) * 100.0

    # Rolling movement.
    df["hl_range_5d_avg_pct"] = df["high_low_range_pct"].rolling(ROLLING_SHORT, min_periods=2).mean()
    df["hl_range_20d_avg_pct"] = df["high_low_range_pct"].rolling(ROLLING_MEDIUM, min_periods=5).mean()

    df["body_5d_avg_pct"] = df["body_pct"].rolling(ROLLING_SHORT, min_periods=2).mean()
    df["body_20d_avg_pct"] = df["body_pct"].rolling(ROLLING_MEDIUM, min_periods=5).mean()

    df["body_eff_5d_avg_pct"] = df["body_efficiency_pct"].rolling(ROLLING_SHORT, min_periods=2).mean()
    df["body_eff_20d_avg_pct"] = df["body_efficiency_pct"].rolling(ROLLING_MEDIUM, min_periods=5).mean()

    # Buckets.
    df["range_bucket"] = pd.cut(
        df["high_low_range_pct"],
        bins=RANGE_BUCKETS,
        labels=RANGE_BUCKET_LABELS,
    )

    df["body_bucket"] = pd.cut(
        df["body_pct"],
        bins=BODY_BUCKETS,
        labels=BODY_BUCKET_LABELS,
    )

    # Defensive cleaning.
    df = df[
        (df["open"] > 0)
        & (df["high"] > 0)
        & (df["low"] > 0)
        & (df["close"] > 0)
        & (df["high"] >= df["low"])
        & (df["high_low_range_abs"] > 0)
        & (df["high_low_range_pct"] > 0)
        & (df["high_low_range_pct"] < 15)
        & (df["body_pct"] >= 0)
        & (df["body_pct"] < 15)
    ].copy()

    if df.empty:
        raise ValueError(f"No valid movement rows for {index_symbol}")

    return df


# ============================================================
# METRIC HELPERS
# ============================================================

def safe_mean(s: pd.Series) -> float:
    return float(s.dropna().mean()) if s.notna().any() else 0.0


def safe_median(s: pd.Series) -> float:
    return float(s.dropna().median()) if s.notna().any() else 0.0


def safe_quantile(s: pd.Series, q: float) -> float:
    return float(s.dropna().quantile(q)) if s.notna().any() else 0.0


def pct_true(s: pd.Series) -> float:
    if len(s) == 0:
        return 0.0
    return float(s.mean() * 100.0)


def window_slice(df: pd.DataFrame, start_date: date, to_date: date) -> pd.DataFrame:
    """Return rows between start_date and to_date inclusive."""
    trade_dates = pd.to_datetime(df["trade_date"], errors="coerce").dt.date
    return df[(trade_dates >= start_date) & (trade_dates <= to_date)].copy()


# ============================================================
# SUMMARY TABLES
# ============================================================

def calculate_rank_metrics(
    info: SpotIndexInfo,
    daily_3y: pd.DataFrame,
    start_1y: date,
    start_3m: date,
    to_date: date,
) -> IndexRankMetrics:
    """Calculate index-level ranking metrics."""
    w3y = daily_3y.copy()
    w1y = window_slice(daily_3y, start_1y, to_date)
    w3m = window_slice(daily_3y, start_3m, to_date)

    warnings: List[str] = []

    if len(w3y) < 600:
        warnings.append(f"3Y low coverage: {len(w3y)}")
    if len(w1y) < 180:
        warnings.append(f"1Y low coverage: {len(w1y)}")
    if len(w3m) < 40:
        warnings.append(f"3M low coverage: {len(w3m)}")

    return IndexRankMetrics(
        index_symbol=info.index_symbol,
        spot_kite_key=info.spot_kite_key,
        spot_token=info.spot_token,

        first_date=min(pd.to_datetime(w3y["trade_date"]).dt.date) if not w3y.empty else None,
        last_date=max(pd.to_datetime(w3y["trade_date"]).dt.date) if not w3y.empty else None,
        trading_days=int(len(w3y)),

        median_hl_range_pct_3y=safe_median(w3y["high_low_range_pct"]),
        avg_hl_range_pct_3y=safe_mean(w3y["high_low_range_pct"]),
        p75_hl_range_pct_3y=safe_quantile(w3y["high_low_range_pct"], 0.75),
        p90_hl_range_pct_3y=safe_quantile(w3y["high_low_range_pct"], 0.90),

        median_body_pct_3y=safe_median(w3y["body_pct"]),
        avg_body_pct_3y=safe_mean(w3y["body_pct"]),
        p75_body_pct_3y=safe_quantile(w3y["body_pct"], 0.75),
        p90_body_pct_3y=safe_quantile(w3y["body_pct"], 0.90),

        median_body_efficiency_pct_3y=safe_median(w3y["body_efficiency_pct"]),
        avg_body_efficiency_pct_3y=safe_mean(w3y["body_efficiency_pct"]),

        active_hl_day_pct_3y=pct_true(w3y["active_hl_day"]),
        strong_hl_day_pct_3y=pct_true(w3y["strong_hl_day"]),
        active_body_day_pct_3y=pct_true(w3y["active_body_day"]),
        strong_body_day_pct_3y=pct_true(w3y["strong_body_day"]),
        strong_directional_day_pct_3y=pct_true(w3y["strong_directional_day"]),

        median_hl_range_pct_1y=safe_median(w1y["high_low_range_pct"]),
        median_body_pct_1y=safe_median(w1y["body_pct"]),
        median_body_efficiency_pct_1y=safe_median(w1y["body_efficiency_pct"]),
        active_hl_day_pct_1y=pct_true(w1y["active_hl_day"]),
        active_body_day_pct_1y=pct_true(w1y["active_body_day"]),
        strong_directional_day_pct_1y=pct_true(w1y["strong_directional_day"]),

        median_hl_range_pct_3m=safe_median(w3m["high_low_range_pct"]),
        median_body_pct_3m=safe_median(w3m["body_pct"]),
        median_body_efficiency_pct_3m=safe_median(w3m["body_efficiency_pct"]),
        active_hl_day_pct_3m=pct_true(w3m["active_hl_day"]),
        active_body_day_pct_3m=pct_true(w3m["active_body_day"]),
        strong_directional_day_pct_3m=pct_true(w3m["strong_directional_day"]),

        green_day_pct_3y=pct_true(w3y["day_type"] == "GREEN"),
        red_day_pct_3y=pct_true(w3y["day_type"] == "RED"),
        avg_abs_gap_pct_3y=safe_mean(w3y["abs_gap_pct"]),
        median_true_range_pct_3y=safe_median(w3y["true_range_pct"]),
        avg_atr14_pct_3y=safe_mean(w3y["atr14_pct"]),

        latest_close=float(w3y["close"].iloc[-1]) if not w3y.empty else 0.0,
        data_warning="; ".join(warnings),
    )


def make_month_summary(daily: pd.DataFrame) -> pd.DataFrame:
    """Create month-wise movement summary for all indices."""
    if daily.empty:
        return pd.DataFrame()

    rows: List[Dict[str, Any]] = []

    grouped = daily.groupby(["index_symbol", "year_month"], sort=True)

    for (index_symbol, ym), g in grouped:
        g = g.sort_values("trade_date").copy()
        first = g.iloc[0]
        last = g.iloc[-1]

        month_open = float(first["open"])
        month_close = float(last["close"])
        month_high = float(g["high"].max())
        month_low = float(g["low"].min())

        rows.append(
            {
                "index_symbol": index_symbol,
                "year_month": ym,
                "year": int(first["year"]),
                "month": int(first["month"]),
                "month_name": str(first["month_name"]),
                "trading_days": int(len(g)),
                "first_date": first["trade_date"],
                "last_date": last["trade_date"],

                "month_open": month_open,
                "month_high": month_high,
                "month_low": month_low,
                "month_close": month_close,

                "month_return_pct_open_to_close": ((month_close / month_open) - 1.0) * 100.0 if month_open > 0 else 0.0,
                "month_high_low_range_pct": ((month_high / month_low) - 1.0) * 100.0 if month_low > 0 else 0.0,

                "median_hl_range_pct": safe_median(g["high_low_range_pct"]),
                "avg_hl_range_pct": safe_mean(g["high_low_range_pct"]),
                "p75_hl_range_pct": safe_quantile(g["high_low_range_pct"], 0.75),
                "p90_hl_range_pct": safe_quantile(g["high_low_range_pct"], 0.90),

                "median_body_pct": safe_median(g["body_pct"]),
                "avg_body_pct": safe_mean(g["body_pct"]),
                "p75_body_pct": safe_quantile(g["body_pct"], 0.75),
                "p90_body_pct": safe_quantile(g["body_pct"], 0.90),

                "median_body_efficiency_pct": safe_median(g["body_efficiency_pct"]),
                "avg_body_efficiency_pct": safe_mean(g["body_efficiency_pct"]),

                "active_hl_day_pct": pct_true(g["active_hl_day"]),
                "strong_hl_day_pct": pct_true(g["strong_hl_day"]),
                "active_body_day_pct": pct_true(g["active_body_day"]),
                "strong_body_day_pct": pct_true(g["strong_body_day"]),
                "strong_directional_day_pct": pct_true(g["strong_directional_day"]),

                "green_day_pct": pct_true(g["day_type"] == "GREEN"),
                "red_day_pct": pct_true(g["day_type"] == "RED"),

                "avg_abs_gap_pct": safe_mean(g["abs_gap_pct"]),
                "median_true_range_pct": safe_median(g["true_range_pct"]),
                "avg_atr14_pct": safe_mean(g["atr14_pct"]),

                "best_day_close_to_close_ret_pct": safe_mean(pd.Series([g["close_to_close_ret_pct"].max()])),
                "worst_day_close_to_close_ret_pct": safe_mean(pd.Series([g["close_to_close_ret_pct"].min()])),
                "avg_close_to_close_ret_pct": safe_mean(g["close_to_close_ret_pct"]),
            }
        )

    out = pd.DataFrame(rows)

    if not out.empty:
        out["monthly_movement_quality_score"] = (
            0.30 * out["median_hl_range_pct"].rank(pct=True)
            + 0.25 * out["median_body_pct"].rank(pct=True)
            + 0.20 * out["median_body_efficiency_pct"].rank(pct=True)
            + 0.15 * out["strong_directional_day_pct"].rank(pct=True)
            + 0.10 * out["active_hl_day_pct"].rank(pct=True)
        ) * 100.0

    return out.sort_values(["index_symbol", "year_month"]).reset_index(drop=True)


def make_yearly_summary(daily: pd.DataFrame) -> pd.DataFrame:
    """Create year-wise movement summary."""
    if daily.empty:
        return pd.DataFrame()

    rows: List[Dict[str, Any]] = []

    for (index_symbol, year), g in daily.groupby(["index_symbol", "year"], sort=True):
        g = g.sort_values("trade_date").copy()
        first = g.iloc[0]
        last = g.iloc[-1]

        year_open = float(first["open"])
        year_close = float(last["close"])
        year_high = float(g["high"].max())
        year_low = float(g["low"].min())

        rows.append(
            {
                "index_symbol": index_symbol,
                "year": int(year),
                "trading_days": int(len(g)),
                "first_date": first["trade_date"],
                "last_date": last["trade_date"],
                "year_open": year_open,
                "year_high": year_high,
                "year_low": year_low,
                "year_close": year_close,
                "year_return_pct_open_to_close": ((year_close / year_open) - 1.0) * 100.0 if year_open > 0 else 0.0,
                "year_high_low_range_pct": ((year_high / year_low) - 1.0) * 100.0 if year_low > 0 else 0.0,
                "median_hl_range_pct": safe_median(g["high_low_range_pct"]),
                "median_body_pct": safe_median(g["body_pct"]),
                "median_body_efficiency_pct": safe_median(g["body_efficiency_pct"]),
                "active_hl_day_pct": pct_true(g["active_hl_day"]),
                "active_body_day_pct": pct_true(g["active_body_day"]),
                "strong_directional_day_pct": pct_true(g["strong_directional_day"]),
                "avg_atr14_pct": safe_mean(g["atr14_pct"]),
                "green_day_pct": pct_true(g["day_type"] == "GREEN"),
                "red_day_pct": pct_true(g["day_type"] == "RED"),
            }
        )

    return pd.DataFrame(rows)


def make_weekday_summary(daily: pd.DataFrame) -> pd.DataFrame:
    """Create weekday-wise movement summary."""
    if daily.empty:
        return pd.DataFrame()

    weekday_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
    rows: List[Dict[str, Any]] = []

    for index_symbol in sorted(daily["index_symbol"].unique()):
        idx_df = daily[daily["index_symbol"] == index_symbol].copy()

        for wd in weekday_order:
            g = idx_df[idx_df["weekday"] == wd].copy()

            if g.empty:
                continue

            rows.append(
                {
                    "index_symbol": index_symbol,
                    "weekday": wd,
                    "trading_days": int(len(g)),
                    "median_hl_range_pct": safe_median(g["high_low_range_pct"]),
                    "avg_hl_range_pct": safe_mean(g["high_low_range_pct"]),
                    "median_body_pct": safe_median(g["body_pct"]),
                    "avg_body_pct": safe_mean(g["body_pct"]),
                    "median_body_efficiency_pct": safe_median(g["body_efficiency_pct"]),
                    "active_hl_day_pct": pct_true(g["active_hl_day"]),
                    "active_body_day_pct": pct_true(g["active_body_day"]),
                    "strong_directional_day_pct": pct_true(g["strong_directional_day"]),
                    "green_day_pct": pct_true(g["day_type"] == "GREEN"),
                    "red_day_pct": pct_true(g["day_type"] == "RED"),
                }
            )

    return pd.DataFrame(rows)


def make_month_bucket_distribution(daily: pd.DataFrame, bucket_col: str) -> pd.DataFrame:
    """Create monthly bucket distribution for range/body buckets."""
    if daily.empty:
        return pd.DataFrame()

    counts = (
        daily.groupby(["index_symbol", "year_month", bucket_col], observed=False)
        .size()
        .reset_index(name="days")
    )

    pivot = (
        counts
        .pivot_table(
            index=["index_symbol", "year_month"],
            columns=bucket_col,
            values="days",
            fill_value=0,
            aggfunc="sum",
            observed=False,
        )
        .reset_index()
    )

    return pivot


# ============================================================
# FINAL RANKING
# ============================================================

def rank_higher_better(s: pd.Series) -> pd.Series:
    """Percentile rank where higher is better."""
    x = pd.to_numeric(s, errors="coerce").fillna(0.0)

    if x.nunique(dropna=False) <= 1:
        return pd.Series(50.0, index=s.index)

    return x.rank(pct=True, method="average") * 100.0


def build_ranked_indices(metrics: List[IndexRankMetrics]) -> pd.DataFrame:
    """
    Build final underlying-only ranking.

    Scoring:
        40% 3Y stable movement
        35% 1Y recent movement
        25% 3M current-regime movement

    No option-depth, no option spread, no order-book scoring.
    """
    if not metrics:
        return pd.DataFrame()

    df = pd.DataFrame([m.__dict__ for m in metrics])

    # 3Y stable movement ranks.
    df["rank_3y_hl"] = rank_higher_better(df["median_hl_range_pct_3y"])
    df["rank_3y_body"] = rank_higher_better(df["median_body_pct_3y"])
    df["rank_3y_eff"] = rank_higher_better(df["median_body_efficiency_pct_3y"])
    df["rank_3y_active_hl"] = rank_higher_better(df["active_hl_day_pct_3y"])
    df["rank_3y_active_body"] = rank_higher_better(df["active_body_day_pct_3y"])

    df["score_3y_stability"] = (
        0.12 * df["rank_3y_hl"]
        + 0.10 * df["rank_3y_body"]
        + 0.06 * df["rank_3y_eff"]
        + 0.06 * df["rank_3y_active_hl"]
        + 0.06 * df["rank_3y_active_body"]
    )

    # 1Y recent movement ranks.
    df["rank_1y_hl"] = rank_higher_better(df["median_hl_range_pct_1y"])
    df["rank_1y_body"] = rank_higher_better(df["median_body_pct_1y"])
    df["rank_1y_eff"] = rank_higher_better(df["median_body_efficiency_pct_1y"])
    df["rank_1y_directional"] = rank_higher_better(df["strong_directional_day_pct_1y"])

    df["score_1y_recent"] = (
        0.12 * df["rank_1y_hl"]
        + 0.10 * df["rank_1y_body"]
        + 0.06 * df["rank_1y_eff"]
        + 0.07 * df["rank_1y_directional"]
    )

    # 3M current regime ranks.
    df["rank_3m_hl"] = rank_higher_better(df["median_hl_range_pct_3m"])
    df["rank_3m_body"] = rank_higher_better(df["median_body_pct_3m"])
    df["rank_3m_eff"] = rank_higher_better(df["median_body_efficiency_pct_3m"])
    df["rank_3m_directional"] = rank_higher_better(df["strong_directional_day_pct_3m"])

    df["score_3m_current"] = (
        0.09 * df["rank_3m_hl"]
        + 0.07 * df["rank_3m_body"]
        + 0.04 * df["rank_3m_eff"]
        + 0.05 * df["rank_3m_directional"]
    )

    df["raw_underlying_score"] = (
        df["score_3y_stability"]
        + df["score_1y_recent"]
        + df["score_3m_current"]
    )

    df["data_penalty"] = 0.0
    df.loc[df["trading_days"] < 600, "data_penalty"] += 10.0

    df["final_underlying_score"] = (
        df["raw_underlying_score"] - df["data_penalty"]
    ).clip(lower=0, upper=100)

    def classify(row: pd.Series) -> str:
        if row["trading_days"] < 600:
            return "REJECT_LOW_DATA"
        if row["final_underlying_score"] >= 80:
            return "STRONG_UNDERLYING_MOVER"
        if row["final_underlying_score"] >= 70:
            return "GOOD_UNDERLYING_MOVER"
        if row["final_underlying_score"] >= 60:
            return "WATCHLIST_UNDERLYING_MOVER"
        return "WEAK_UNDERLYING_MOVER"

    df["underlying_verdict"] = df.apply(classify, axis=1)

    ordered_cols = [
        "index_symbol",
        "underlying_verdict",
        "final_underlying_score",
        "raw_underlying_score",
        "score_3y_stability",
        "score_1y_recent",
        "score_3m_current",
        "data_penalty",

        "median_hl_range_pct_3y",
        "avg_hl_range_pct_3y",
        "p75_hl_range_pct_3y",
        "p90_hl_range_pct_3y",

        "median_body_pct_3y",
        "avg_body_pct_3y",
        "p75_body_pct_3y",
        "p90_body_pct_3y",

        "median_body_efficiency_pct_3y",
        "avg_body_efficiency_pct_3y",

        "active_hl_day_pct_3y",
        "strong_hl_day_pct_3y",
        "active_body_day_pct_3y",
        "strong_body_day_pct_3y",
        "strong_directional_day_pct_3y",

        "median_hl_range_pct_1y",
        "median_body_pct_1y",
        "median_body_efficiency_pct_1y",
        "active_hl_day_pct_1y",
        "active_body_day_pct_1y",
        "strong_directional_day_pct_1y",

        "median_hl_range_pct_3m",
        "median_body_pct_3m",
        "median_body_efficiency_pct_3m",
        "active_hl_day_pct_3m",
        "active_body_day_pct_3m",
        "strong_directional_day_pct_3m",

        "green_day_pct_3y",
        "red_day_pct_3y",
        "avg_abs_gap_pct_3y",
        "median_true_range_pct_3y",
        "avg_atr14_pct_3y",

        "trading_days",
        "first_date",
        "last_date",
        "latest_close",
        "spot_kite_key",
        "spot_token",
        "data_warning",
    ]

    existing_cols = [c for c in ordered_cols if c in df.columns]

    df = df[existing_cols].sort_values(
        by=[
            "final_underlying_score",
            "score_3m_current",
            "score_1y_recent",
            "score_3y_stability",
        ],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)

    df.insert(0, "rank", range(1, len(df) + 1))
    return df


# ============================================================
# EXCEL OUTPUT
# ============================================================

def autosize_excel_columns(writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame) -> None:
    """Auto-size Excel columns."""
    ws = writer.sheets[sheet_name]

    for idx, col in enumerate(df.columns):
        series = df[col].astype(str) if not df.empty else pd.Series([], dtype=str)
        max_len = max([len(str(col))] + [len(x) for x in series.head(500).tolist()])
        ws.set_column(idx, idx, min(max_len + 2, 55))


def write_excel_report(
    ranked_df: pd.DataFrame,
    daily_features_df: pd.DataFrame,
    month_summary_df: pd.DataFrame,
    yearly_summary_df: pd.DataFrame,
    weekday_summary_df: pd.DataFrame,
    month_range_distribution_df: pd.DataFrame,
    month_body_distribution_df: pd.DataFrame,
    failed_df: pd.DataFrame,
    config_df: pd.DataFrame,
    output_path: str,
) -> None:
    """Write full Excel workbook."""
    ranked_df = make_excel_safe_df(ranked_df)
    daily_features_df = make_excel_safe_df(daily_features_df)
    month_summary_df = make_excel_safe_df(month_summary_df)
    yearly_summary_df = make_excel_safe_df(yearly_summary_df)
    weekday_summary_df = make_excel_safe_df(weekday_summary_df)
    month_range_distribution_df = make_excel_safe_df(month_range_distribution_df)
    month_body_distribution_df = make_excel_safe_df(month_body_distribution_df)
    failed_df = make_excel_safe_df(failed_df)
    config_df = make_excel_safe_df(config_df)

    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        ranked_df.to_excel(writer, index=False, sheet_name="ranked_indices")
        daily_features_df.to_excel(writer, index=False, sheet_name="daily_features")
        month_summary_df.to_excel(writer, index=False, sheet_name="month_summary")
        yearly_summary_df.to_excel(writer, index=False, sheet_name="yearly_summary")
        weekday_summary_df.to_excel(writer, index=False, sheet_name="weekday_summary")
        month_range_distribution_df.to_excel(writer, index=False, sheet_name="month_range_distribution")
        month_body_distribution_df.to_excel(writer, index=False, sheet_name="month_body_distribution")
        failed_df.to_excel(writer, index=False, sheet_name="missing_or_failed")
        config_df.to_excel(writer, index=False, sheet_name="config")

        sheets = {
            "ranked_indices": ranked_df,
            "daily_features": daily_features_df,
            "month_summary": month_summary_df,
            "yearly_summary": yearly_summary_df,
            "weekday_summary": weekday_summary_df,
            "month_range_distribution": month_range_distribution_df,
            "month_body_distribution": month_body_distribution_df,
            "missing_or_failed": failed_df,
            "config": config_df,
        }

        workbook = writer.book
        fmt_num = workbook.add_format({"num_format": "0.00"})
        fmt_int = workbook.add_format({"num_format": "0"})
        fmt_date = workbook.add_format({"num_format": "yyyy-mm-dd"})

        for sheet_name, df in sheets.items():
            ws = writer.sheets[sheet_name]
            ws.freeze_panes(1, 0)

            if not df.empty:
                ws.autofilter(0, 0, len(df), len(df.columns) - 1)

            autosize_excel_columns(writer, sheet_name, df)

            for col_name in df.columns:
                idx = df.columns.get_loc(col_name)
                lower = col_name.lower()

                if "date" in lower:
                    ws.set_column(idx, idx, 14, fmt_date)
                elif "days" in lower or lower in {"year", "month", "trading_days"}:
                    ws.set_column(idx, idx, 12, fmt_int)
                elif (
                    "pct" in lower
                    or "score" in lower
                    or "range" in lower
                    or "body" in lower
                    or "atr" in lower
                    or lower in {"open", "high", "low", "close", "latest_close"}
                ):
                    ws.set_column(idx, idx, 16, fmt_num)


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    """Main entrypoint."""
    print("============================================================")
    print("INDEX UNDERLYING 3-YEAR MOVEMENT ANALYZER")
    print("NIFTY + SENSEX + BANKNIFTY")
    print("Underlying only. No options. No option depth.")
    print("============================================================")

    from_date_buffered, analysis_start_3y, start_1y, start_3m, to_date = compute_date_range()

    print(f"[CONFIG] Index symbols           : {INDEX_SYMBOLS}")
    print(f"[CONFIG] Lookback years          : {LOOKBACK_YEARS}")
    print(f"[CONFIG] Download from           : {from_date_buffered}")
    print(f"[CONFIG] Analysis from           : {analysis_start_3y}")
    print(f"[CONFIG] 1Y start                : {start_1y}")
    print(f"[CONFIG] 3M start                : {start_3m}")
    print(f"[CONFIG] Historical end          : {to_date}")
    print(f"[CONFIG] Output Excel            : {OUTPUT_EXCEL}")
    print(f"[CONFIG] Use today daily candle  : {USE_TODAY_DAILY_CANDLE}")
    print(f"[CONFIG] Force refresh           : {FORCE_REFRESH}")
    print("============================================================")

    print("[STEP] Initializing Kite ...")
    kite = initialize_kite()
    print("[OK] Kite initialized.")

    instruments_cache: Dict[str, List[Dict[str, Any]]] = {}

    infos: List[SpotIndexInfo] = []
    rank_metrics: List[IndexRankMetrics] = []
    daily_frames: List[pd.DataFrame] = []
    failed_rows: List[Dict[str, Any]] = []

    for idx, index_symbol in enumerate(INDEX_SYMBOLS, start=1):
        print("\n------------------------------------------------------------")
        print(f"[INDEX {idx}/{len(INDEX_SYMBOLS)}] {index_symbol}")
        print("------------------------------------------------------------")

        try:
            info = resolve_spot_index(kite, index_symbol, instruments_cache)
            infos.append(info)

            print(f"[INFO] Resolved: {info.spot_kite_key}, token={info.spot_token}")

            raw_df = load_cached_if_usable(index_symbol, from_date_buffered, to_date)

            if raw_df is None:
                rows = fetch_history_day(
                    kite=kite,
                    instrument_token=info.spot_token,
                    from_date=from_date_buffered,
                    to_date=to_date,
                    label=info.spot_kite_key,
                )

                raw_df = rows_to_dataframe(rows)

                if raw_df.empty:
                    raise RuntimeError(f"No daily candles returned for {index_symbol}")

                save_cache(index_symbol, raw_df)

            raw_df = normalize_cached_df(raw_df)
            raw_df["trade_date"] = pd.to_datetime(raw_df["trade_date"], errors="coerce").dt.date

            raw_df = raw_df[
                (raw_df["trade_date"] >= from_date_buffered)
                & (raw_df["trade_date"] <= to_date)
            ].copy()

            featured = add_daily_features(index_symbol, info, raw_df)

            # Exact 3-year analysis period.
            featured_dates = pd.to_datetime(featured["trade_date"], errors="coerce").dt.date
            daily_3y = featured[
                (featured_dates >= analysis_start_3y)
                & (featured_dates <= to_date)
            ].copy()

            if daily_3y.empty:
                raise RuntimeError(f"No rows in exact 3Y analysis window for {index_symbol}")

            metrics = calculate_rank_metrics(
                info=info,
                daily_3y=daily_3y,
                start_1y=start_1y,
                start_3m=start_3m,
                to_date=to_date,
            )

            rank_metrics.append(metrics)
            daily_frames.append(daily_3y)

            print(
                f"[OK] {index_symbol}: "
                f"3Y median HL={metrics.median_hl_range_pct_3y:.2f}%, "
                f"3Y median body={metrics.median_body_pct_3y:.2f}%, "
                f"1Y median HL={metrics.median_hl_range_pct_1y:.2f}%, "
                f"3M median HL={metrics.median_hl_range_pct_3m:.2f}%"
            )

            if metrics.data_warning:
                print(f"[WARN] {metrics.data_warning}")

        except Exception as exc:
            print(f"[ERROR] {index_symbol} failed: {exc}")
            traceback.print_exc()
            failed_rows.append(
                {
                    "index_symbol": index_symbol,
                    "stage": "historical_underlying",
                    "error": str(exc),
                }
            )

    print("\n[STEP] Building underlying-only ranking ...")
    ranked_df = build_ranked_indices(rank_metrics)

    if daily_frames:
        daily_features_df = pd.concat(daily_frames, ignore_index=True)
        daily_features_df = daily_features_df.sort_values(["index_symbol", "trade_date"]).reset_index(drop=True)
    else:
        daily_features_df = pd.DataFrame()

    print("[STEP] Building month summary ...")
    month_summary_df = make_month_summary(daily_features_df)

    print("[STEP] Building yearly summary ...")
    yearly_summary_df = make_yearly_summary(daily_features_df)

    print("[STEP] Building weekday summary ...")
    weekday_summary_df = make_weekday_summary(daily_features_df)

    print("[STEP] Building bucket distributions ...")
    month_range_distribution_df = make_month_bucket_distribution(daily_features_df, "range_bucket")
    month_body_distribution_df = make_month_bucket_distribution(daily_features_df, "body_bucket")

    failed_df = pd.DataFrame(failed_rows)

    config_df = pd.DataFrame(
        [
            {"parameter": "INDEX_SYMBOLS", "value": ",".join(INDEX_SYMBOLS)},
            {"parameter": "LOOKBACK_YEARS", "value": LOOKBACK_YEARS},
            {"parameter": "FROM_DATE_BUFFERED", "value": str(from_date_buffered)},
            {"parameter": "ANALYSIS_START_3Y", "value": str(analysis_start_3y)},
            {"parameter": "START_1Y", "value": str(start_1y)},
            {"parameter": "START_3M", "value": str(start_3m)},
            {"parameter": "TO_DATE", "value": str(to_date)},
            {"parameter": "USE_TODAY_DAILY_CANDLE", "value": USE_TODAY_DAILY_CANDLE},
            {"parameter": "OUTPUT_EXCEL", "value": OUTPUT_EXCEL},
            {"parameter": "DATA_CACHE_DIR", "value": DATA_CACHE_DIR},
            {"parameter": "FORCE_REFRESH", "value": FORCE_REFRESH},
            {"parameter": "HL_RANGE_FORMULA", "value": "(high-low)/close*100"},
            {"parameter": "BODY_FORMULA", "value": "abs(close-open)/open*100"},
            {"parameter": "BODY_SIGNED_FORMULA", "value": "(close-open)/open*100"},
            {"parameter": "BODY_EFFICIENCY_FORMULA", "value": "abs(close-open)/(high-low)*100"},
            {"parameter": "CLOSE_LOCATION_FORMULA", "value": "(close-low)/(high-low)*100"},
            {"parameter": "HL_ACTIVE_THRESHOLD_PCT", "value": HL_ACTIVE_THRESHOLD_PCT},
            {"parameter": "HL_STRONG_THRESHOLD_PCT", "value": HL_STRONG_THRESHOLD_PCT},
            {"parameter": "BODY_ACTIVE_THRESHOLD_PCT", "value": BODY_ACTIVE_THRESHOLD_PCT},
            {"parameter": "BODY_STRONG_THRESHOLD_PCT", "value": BODY_STRONG_THRESHOLD_PCT},
            {
                "parameter": "FINAL_SCORING",
                "value": (
                    "Underlying only: 40% 3Y stable movement + "
                    "35% 1Y recent movement + 25% 3M current-regime movement. "
                    "No option-depth or option-spread scoring."
                ),
            },
        ]
    )

    print("[STEP] Writing Excel report ...")
    write_excel_report(
        ranked_df=ranked_df,
        daily_features_df=daily_features_df,
        month_summary_df=month_summary_df,
        yearly_summary_df=yearly_summary_df,
        weekday_summary_df=weekday_summary_df,
        month_range_distribution_df=month_range_distribution_df,
        month_body_distribution_df=month_body_distribution_df,
        failed_df=failed_df,
        config_df=config_df,
        output_path=OUTPUT_EXCEL,
    )

    print("\n==================== FINAL RESULT ====================")

    if ranked_df.empty:
        print("[ERROR] No indices ranked.")
    else:
        top_cols = [
            "rank",
            "index_symbol",
            "underlying_verdict",
            "final_underlying_score",
            "score_3y_stability",
            "score_1y_recent",
            "score_3m_current",
            "median_hl_range_pct_3y",
            "median_body_pct_3y",
            "median_hl_range_pct_1y",
            "median_body_pct_1y",
            "median_hl_range_pct_3m",
            "median_body_pct_3m",
            "active_hl_day_pct_3y",
            "active_body_day_pct_3y",
        ]

        top_cols = [c for c in top_cols if c in ranked_df.columns]
        print(ranked_df[top_cols].to_string(index=False))

        best = ranked_df.iloc[0]
        print("\nBest underlying mover:")
        print(f"Index                    : {best['index_symbol']}")
        print(f"Score                    : {best['final_underlying_score']:.2f}")
        print(f"Verdict                  : {best['underlying_verdict']}")
        print(f"3Y median HL range        : {best['median_hl_range_pct_3y']:.2f}%")
        print(f"3Y median body            : {best['median_body_pct_3y']:.2f}%")
        print(f"1Y median HL range        : {best['median_hl_range_pct_1y']:.2f}%")
        print(f"1Y median body            : {best['median_body_pct_1y']:.2f}%")
        print(f"3M median HL range        : {best['median_hl_range_pct_3m']:.2f}%")
        print(f"3M median body            : {best['median_body_pct_3m']:.2f}%")

    print(f"\n[DONE] Excel saved: {OUTPUT_EXCEL}")
    print("=======================================================")


if __name__ == "__main__":
    main()