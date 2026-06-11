#!/usr/bin/env python3
"""
index_intraday_tradability_selector.py

Purpose
-------
Check intraday tradability of index-option underlyings:

    1. NIFTY
    2. SENSEX
    3. BANKNIFTY

Important difference from stock selector
----------------------------------------
For stocks, live execution can be checked using stock bid/ask depth.

For indices, the index itself is not directly traded like equity. Therefore,
this script evaluates:

    A. Historical SPOT INDEX movement
       - Daily high-low range
       - Daily open-close body
       - Body efficiency

    B. Live SPOT INDEX movement
       - Current high-low range so far
       - Current open-to-LTP body
       - Body efficiency so far
       - Day change %

    C. Live ATM OPTION execution quality
       - Nearest expiry ATM CE and PE
       - CE/PE bid-ask spread
       - CE/PE top-5 bid/ask quantity
       - CE/PE top-5 bid/ask lots
       - Safe slice size in lots

This is more relevant for your trading style because you trade index options,
not the spot index.

Output Excel sheets
-------------------
1. ranked_indices
2. daily_features
3. live_underlying
4. atm_option_depth
5. selected_atm_options
6. missing_or_failed
7. config

Default output:
    index_intraday_tradability_selector.xlsx

Run
---
    python index_intraday_tradability_selector.py

Recommended during live market:
    python index_intraday_tradability_selector.py

Force fresh historical download:
    set FORCE_REFRESH=1
    python index_intraday_tradability_selector.py

Optional overrides:
    set INDEX_SYMBOLS=NIFTY,SENSEX,BANKNIFTY
    set OUTPUT_EXCEL=index_intraday_tradability_selector.xlsx
    set MIN_ATM_TOP5_LOTS_EACH_SIDE=100
    set MIN_SAFE_SLICE_LOTS=10
    set MAX_ATM_OPTION_SPREAD_PCT=3.0
"""

from __future__ import annotations

import math
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

OUTPUT_EXCEL = os.environ.get(
    "OUTPUT_EXCEL",
    "index_intraday_tradability_selector.xlsx",
).strip()

DATA_CACHE_DIR = os.environ.get(
    "DATA_CACHE_DIR",
    "./index_daily_cache",
).strip()

FORCE_REFRESH = os.environ.get("FORCE_REFRESH", "0").strip() == "1"

TOP_N_TO_PRINT = int(os.environ.get("TOP_N_TO_PRINT", "3"))

# Historical windows.
WINDOW_2Y_DAYS = int(os.environ.get("WINDOW_2Y_DAYS", str(365 * 2)))
WINDOW_1Y_DAYS = int(os.environ.get("WINDOW_1Y_DAYS", "365"))
WINDOW_3M_DAYS = int(os.environ.get("WINDOW_3M_DAYS", "92"))
DATE_BUFFER_DAYS = 10

# Use previous completed day by default.
# This avoids today's incomplete daily candle contaminating historical stats.
USE_TODAY_DAILY_CANDLE = os.environ.get("USE_TODAY_DAILY_CANDLE", "0").strip() == "1"

# Diagnostic thresholds for index spot movement.
HL_ACTIVE_THRESHOLD_PCT = float(os.environ.get("HL_ACTIVE_THRESHOLD_PCT", "0.75"))
BODY_ACTIVE_THRESHOLD_PCT = float(os.environ.get("BODY_ACTIVE_THRESHOLD_PCT", "0.35"))

# Live ATM option execution thresholds.
MIN_ATM_TOP5_LOTS_EACH_SIDE = float(os.environ.get("MIN_ATM_TOP5_LOTS_EACH_SIDE", "100"))
MIN_SAFE_SLICE_LOTS = float(os.environ.get("MIN_SAFE_SLICE_LOTS", "10"))
MAX_ATM_OPTION_SPREAD_PCT = float(os.environ.get("MAX_ATM_OPTION_SPREAD_PCT", "3.0"))

# Kite API safety.
MAX_DAYS_PER_CHUNK = 365
MAX_ATTEMPTS = 5
SLEEP_BETWEEN_CALLS_SEC = 0.25


# ============================================================
# INDEX CONFIG
# ============================================================

INDEX_CONFIG: Dict[str, Dict[str, Any]] = {
    "NIFTY": {
        "display": "NIFTY",
        "spot_exchange": "NSE",
        "spot_tradingsymbols": ["NIFTY 50", "NIFTY50", "NIFTY"],
        "derivative_exchange": "NFO",
        "derivative_name": "NIFTY",
        "strike_step": 50,
    },
    "BANKNIFTY": {
        "display": "BANKNIFTY",
        "spot_exchange": "NSE",
        "spot_tradingsymbols": ["NIFTY BANK", "BANKNIFTY", "NIFTYBANK"],
        "derivative_exchange": "NFO",
        "derivative_name": "BANKNIFTY",
        "strike_step": 100,
    },
    "SENSEX": {
        "display": "SENSEX",
        "spot_exchange": "BSE",
        "spot_tradingsymbols": ["SENSEX"],
        "derivative_exchange": "BFO",
        "derivative_name": "SENSEX",
        "strike_step": 100,
    },
}


# ============================================================
# DATA CLASSES
# ============================================================

@dataclass
class SpotIndexInfo:
    index_symbol: str
    display: str
    spot_exchange: str
    spot_tradingsymbol: str
    spot_kite_key: str
    spot_token: int
    derivative_exchange: str
    derivative_name: str
    strike_step: int


@dataclass
class IndexHistoricalMetrics:
    index_symbol: str
    spot_token: int
    spot_kite_key: str

    first_date: Optional[date]
    last_date: Optional[date]

    days_2y: int
    days_1y: int
    days_3m: int

    median_hl_range_pct_1y: float
    median_hl_range_pct_2y: float
    median_hl_range_pct_3m: float

    median_body_pct_1y: float
    median_body_pct_2y: float
    median_body_pct_3m: float

    median_body_efficiency_pct_1y: float
    median_body_efficiency_pct_2y: float
    median_body_efficiency_pct_3m: float

    active_hl_day_pct_1y: float
    active_body_day_pct_1y: float

    p75_hl_range_pct_1y: float
    p90_hl_range_pct_1y: float
    p75_body_pct_1y: float
    p90_body_pct_1y: float

    latest_close: float
    data_warning: str


@dataclass
class LiveUnderlyingMetrics:
    index_symbol: str
    spot_kite_key: str

    ltp: float
    day_open: float
    day_high: float
    day_low: float
    prev_close: float

    day_change_pct: float
    live_hl_range_pct: float
    live_body_pct: float
    live_body_signed_pct: float
    live_body_efficiency_pct: float

    live_warning: str


@dataclass
class OptionLegDepth:
    index_symbol: str
    leg: str

    derivative_exchange: str
    tradingsymbol: str
    kite_key: str
    instrument_token: int
    expiry: date
    strike: float
    lot_size: int

    ltp: float
    day_volume: int
    oi: int

    best_bid: float
    best_ask: float
    spread_abs: float
    spread_pct: float

    top5_bid_qty: int
    top5_ask_qty: int
    top5_bid_lots: float
    top5_ask_lots: float
    top5_min_side_lots: float

    bid_orders_top5: int
    ask_orders_top5: int

    safe_buy_slice_lots_10pct_ask: float
    safe_sell_slice_lots_10pct_bid: float
    safe_two_way_slice_lots: float

    depth_imbalance_pct: float

    leg_depth_pass: bool
    leg_spread_pass: bool
    leg_warning: str


# ============================================================
# TIME HELPERS
# ============================================================

def ist_today() -> date:
    """Return current date in Asia/Kolkata."""
    if ZoneInfo is not None:
        try:
            return datetime.now(ZoneInfo("Asia/Kolkata")).date()
        except Exception:
            pass
    return datetime.now().date()


def to_ist_naive_timestamp(value: Any) -> pd.Timestamp:
    """Convert timestamp to timezone-naive IST pandas Timestamp."""
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
    """Remove timezone-aware datetimes and infinities before Excel writing."""
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


def compute_master_date_range() -> Tuple[date, date, date, date, date]:
    """Compute historical analysis windows."""
    today = ist_today()

    if USE_TODAY_DAILY_CANDLE:
        to_date = today
    else:
        to_date = today - timedelta(days=1)

    start_2y = to_date - timedelta(days=WINDOW_2Y_DAYS)
    start_1y = to_date - timedelta(days=WINDOW_1Y_DAYS)
    start_3m = to_date - timedelta(days=WINDOW_3M_DAYS)
    from_date_buffered = start_2y - timedelta(days=DATE_BUFFER_DAYS)

    return from_date_buffered, start_2y, start_1y, start_3m, to_date


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


def parse_expiry_date(value: Any) -> Optional[date]:
    """Parse Kite expiry field to date."""
    try:
        if value is None or pd.isna(value):
            return None
        ts = pd.Timestamp(value)
        if pd.isna(ts):
            return None
        return ts.date()
    except Exception:
        return None


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
    Resolve spot index instrument token.

    Examples:
        NIFTY     -> NSE:NIFTY 50
        BANKNIFTY -> NSE:NIFTY BANK
        SENSEX    -> BSE:SENSEX
    """
    if index_symbol not in INDEX_CONFIG:
        raise ValueError(f"Unsupported index symbol: {index_symbol}")

    cfg = INDEX_CONFIG[index_symbol]
    exchange = cfg["spot_exchange"]
    candidates = [x.upper() for x in cfg["spot_tradingsymbols"]]

    instruments = kite_instruments_cached(kite, exchange, instruments_cache)

    exact_matches = []

    for inst in instruments:
        tsym = str(inst.get("tradingsymbol", "")).upper().strip()
        name = str(inst.get("name", "")).upper().strip()

        if tsym in candidates or name in candidates:
            exact_matches.append(inst)

    if not exact_matches:
        raise ValueError(
            f"Could not resolve spot index for {index_symbol}. "
            f"Tried {exchange}:{cfg['spot_tradingsymbols']}"
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

    exact_matches.sort(key=priority)
    selected = exact_matches[0]

    spot_tradingsymbol = str(selected.get("tradingsymbol")).strip()
    spot_token = int(selected["instrument_token"])

    return SpotIndexInfo(
        index_symbol=index_symbol,
        display=cfg["display"],
        spot_exchange=exchange,
        spot_tradingsymbol=spot_tradingsymbol,
        spot_kite_key=f"{exchange}:{spot_tradingsymbol}",
        spot_token=spot_token,
        derivative_exchange=cfg["derivative_exchange"],
        derivative_name=cfg["derivative_name"],
        strike_step=int(cfg["strike_step"]),
    )


# ============================================================
# HISTORICAL DATA AND CACHE
# ============================================================

def cache_path_for_index(index_symbol: str) -> str:
    """Return cache path for spot index daily candles."""
    safe = index_symbol.replace("/", "_").replace("\\", "_").replace(":", "_")
    return os.path.join(DATA_CACHE_DIR, f"{safe}_spot_daily.pkl")


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
    """Load cached daily data if available and adequate."""
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

        if min_d <= from_date and max_d >= to_date - timedelta(days=7):
            print(f"[CACHE] Using {index_symbol}: {path}")
            return df

        print(f"[CACHE] Stale/incomplete for {index_symbol}: {min_d} -> {max_d}. Re-downloading.")
        return None

    except Exception as exc:
        print(f"[WARN] Could not read cache for {index_symbol}: {exc}")
        return None


def save_cache(index_symbol: str, df: pd.DataFrame) -> None:
    """Save daily candles to cache."""
    os.makedirs(DATA_CACHE_DIR, exist_ok=True)
    df.to_pickle(cache_path_for_index(index_symbol))


def fetch_history_day(
    kite,
    instrument_token: int,
    from_date: date,
    to_date: date,
    label: str,
) -> List[Dict[str, Any]]:
    """Fetch daily historical candles from Kite."""
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
    """Convert Kite historical rows to clean dataframe."""
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

def add_daily_features(index_symbol: str, spot_token: int, raw_df: pd.DataFrame) -> pd.DataFrame:
    """Add daily spot-index movement features."""
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
    df["spot_token"] = spot_token

    # High-low range: total intraday movement of the index.
    df["high_low_range_abs"] = df["high"] - df["low"]
    df["high_low_range_pct"] = (df["high_low_range_abs"] / df["close"]) * 100.0
    df["high_low_range_pct_vs_low"] = ((df["high"] / df["low"]) - 1.0) * 100.0

    # Open-close body: net directional movement from open to close.
    df["body_abs"] = (df["close"] - df["open"]).abs()
    df["body_pct"] = (df["body_abs"] / df["open"]) * 100.0
    df["body_signed_pct"] = ((df["close"] - df["open"]) / df["open"]) * 100.0

    # Body efficiency: how much of the day’s range became body.
    df["body_efficiency_pct"] = np.where(
        df["high_low_range_abs"] > 0,
        (df["body_abs"] / df["high_low_range_abs"]) * 100.0,
        0.0,
    )
    df["body_efficiency_pct"] = df["body_efficiency_pct"].clip(lower=0.0, upper=100.0)

    # Gap diagnostics.
    df["prev_close"] = df["close"].shift(1)
    df["gap_pct"] = ((df["open"] - df["prev_close"]).abs() / df["prev_close"]) * 100.0

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
        raise ValueError(f"No valid feature rows for {index_symbol}")

    return df


def window_slice(df: pd.DataFrame, start_date: date, to_date: date) -> pd.DataFrame:
    """Return rows inside a date window."""
    return df[(df["trade_date"] >= start_date) & (df["trade_date"] <= to_date)].copy()


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


def calculate_index_metrics(
    info: SpotIndexInfo,
    featured_df: pd.DataFrame,
    start_2y: date,
    start_1y: date,
    start_3m: date,
    to_date: date,
) -> Tuple[IndexHistoricalMetrics, pd.DataFrame]:
    """Calculate historical movement metrics for one index."""
    w2y = window_slice(featured_df, start_2y, to_date)
    w1y = window_slice(featured_df, start_1y, to_date)
    w3m = window_slice(featured_df, start_3m, to_date)

    if w2y.empty:
        raise ValueError(f"No 2Y data for {info.index_symbol}")

    warnings: List[str] = []

    if len(w2y) < 350:
        warnings.append(f"2Y low coverage: {len(w2y)}")
    if len(w1y) < 180:
        warnings.append(f"1Y low coverage: {len(w1y)}")
    if len(w3m) < 40:
        warnings.append(f"3M low coverage: {len(w3m)}")

    result = IndexHistoricalMetrics(
        index_symbol=info.index_symbol,
        spot_token=info.spot_token,
        spot_kite_key=info.spot_kite_key,

        first_date=min(w2y["trade_date"]) if not w2y.empty else None,
        last_date=max(w2y["trade_date"]) if not w2y.empty else None,

        days_2y=int(len(w2y)),
        days_1y=int(len(w1y)),
        days_3m=int(len(w3m)),

        median_hl_range_pct_1y=safe_median(w1y["high_low_range_pct"]),
        median_hl_range_pct_2y=safe_median(w2y["high_low_range_pct"]),
        median_hl_range_pct_3m=safe_median(w3m["high_low_range_pct"]),

        median_body_pct_1y=safe_median(w1y["body_pct"]),
        median_body_pct_2y=safe_median(w2y["body_pct"]),
        median_body_pct_3m=safe_median(w3m["body_pct"]),

        median_body_efficiency_pct_1y=safe_median(w1y["body_efficiency_pct"]),
        median_body_efficiency_pct_2y=safe_median(w2y["body_efficiency_pct"]),
        median_body_efficiency_pct_3m=safe_median(w3m["body_efficiency_pct"]),

        active_hl_day_pct_1y=pct_true(w1y["high_low_range_pct"] >= HL_ACTIVE_THRESHOLD_PCT),
        active_body_day_pct_1y=pct_true(w1y["body_pct"] >= BODY_ACTIVE_THRESHOLD_PCT),

        p75_hl_range_pct_1y=safe_quantile(w1y["high_low_range_pct"], 0.75),
        p90_hl_range_pct_1y=safe_quantile(w1y["high_low_range_pct"], 0.90),
        p75_body_pct_1y=safe_quantile(w1y["body_pct"], 0.75),
        p90_body_pct_1y=safe_quantile(w1y["body_pct"], 0.90),

        latest_close=float(featured_df["close"].iloc[-1]),
        data_warning="; ".join(warnings),
    )

    keep_cols = [
        "index_symbol",
        "spot_token",
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
        "prev_close",
        "gap_pct",
    ]

    return result, featured_df[keep_cols].copy()


# ============================================================
# LIVE UNDERLYING
# ============================================================

def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        v = float(x)
        if math.isnan(v):
            return default
        return v
    except Exception:
        return default


def safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(float(x))
    except Exception:
        return default


def extract_live_underlying(info: SpotIndexInfo, quote: Dict[str, Any]) -> LiveUnderlyingMetrics:
    """Extract live spot-index movement from quote."""
    ltp = safe_float(quote.get("last_price"))

    ohlc = quote.get("ohlc") or {}

    day_open = safe_float(ohlc.get("open"))
    day_high = safe_float(ohlc.get("high"))
    day_low = safe_float(ohlc.get("low"))
    prev_close = safe_float(ohlc.get("close"))

    warnings: List[str] = []

    if ltp <= 0:
        warnings.append("missing ltp")
    if day_open <= 0:
        warnings.append("missing day open")
    if day_high <= 0:
        warnings.append("missing day high")
    if day_low <= 0:
        warnings.append("missing day low")
    if prev_close <= 0:
        warnings.append("missing previous close")

    day_change_pct = ((ltp - prev_close) / prev_close) * 100.0 if prev_close > 0 else 0.0

    live_hl_range_abs = day_high - day_low if day_high > 0 and day_low > 0 else 0.0
    live_hl_range_pct = (live_hl_range_abs / ltp) * 100.0 if ltp > 0 else 0.0

    live_body_abs = abs(ltp - day_open) if day_open > 0 and ltp > 0 else 0.0
    live_body_pct = (live_body_abs / day_open) * 100.0 if day_open > 0 else 0.0
    live_body_signed_pct = ((ltp - day_open) / day_open) * 100.0 if day_open > 0 else 0.0

    live_body_efficiency_pct = (
        (live_body_abs / live_hl_range_abs) * 100.0
        if live_hl_range_abs > 0
        else 0.0
    )
    live_body_efficiency_pct = max(0.0, min(100.0, live_body_efficiency_pct))

    return LiveUnderlyingMetrics(
        index_symbol=info.index_symbol,
        spot_kite_key=info.spot_kite_key,
        ltp=ltp,
        day_open=day_open,
        day_high=day_high,
        day_low=day_low,
        prev_close=prev_close,
        day_change_pct=day_change_pct,
        live_hl_range_pct=live_hl_range_pct,
        live_body_pct=live_body_pct,
        live_body_signed_pct=live_body_signed_pct,
        live_body_efficiency_pct=live_body_efficiency_pct,
        live_warning="; ".join(warnings),
    )


def fetch_live_underlying(kite, infos: List[SpotIndexInfo]) -> pd.DataFrame:
    """Fetch live spot-index quotes."""
    keys = [info.spot_kite_key for info in infos]

    print(f"[STEP] Fetching live spot-index quotes: {keys}")
    quotes = kite.quote(keys)

    rows: List[Dict[str, Any]] = []

    for info in infos:
        q = quotes.get(info.spot_kite_key, {}) or {}
        metrics = extract_live_underlying(info, q)
        rows.append(metrics.__dict__)

    return pd.DataFrame(rows)


# ============================================================
# OPTION INSTRUMENT SELECTION
# ============================================================

def nearest_strike(price: float, strike_step: int) -> float:
    """Return nearest ATM strike."""
    if price <= 0:
        return 0.0
    return round(price / strike_step) * strike_step


def get_option_universe_for_index(
    kite,
    info: SpotIndexInfo,
    instruments_cache: Dict[str, List[Dict[str, Any]]],
) -> pd.DataFrame:
    """Load and filter option instruments for one index."""
    rows = kite_instruments_cached(kite, info.derivative_exchange, instruments_cache)

    filtered: List[Dict[str, Any]] = []
    derivative_name = info.derivative_name.upper()

    for row in rows:
        inst_type = str(row.get("instrument_type", "")).upper().strip()
        name = str(row.get("name", "")).upper().strip()
        tradingsymbol = str(row.get("tradingsymbol", "")).upper().strip()

        if inst_type not in {"CE", "PE"}:
            continue

        # Kite usually sets name as NIFTY / BANKNIFTY / SENSEX.
        # Fallback: tradingsymbol starts with derivative name.
        if name != derivative_name and not tradingsymbol.startswith(derivative_name):
            continue

        expiry = parse_expiry_date(row.get("expiry"))
        if expiry is None:
            continue

        strike = safe_float(row.get("strike"), 0.0)
        if strike <= 0:
            continue

        out = dict(row)
        out["expiry_date"] = expiry
        out["strike_float"] = strike
        out["lot_size_int"] = safe_int(row.get("lot_size"), 0)
        filtered.append(out)

    if not filtered:
        raise ValueError(f"No option instruments found for {info.index_symbol} on {info.derivative_exchange}")

    return pd.DataFrame(filtered)


def select_atm_options(
    kite,
    infos: List[SpotIndexInfo],
    live_underlying_df: pd.DataFrame,
    instruments_cache: Dict[str, List[Dict[str, Any]]],
) -> pd.DataFrame:
    """
    Select nearest-expiry ATM CE and PE for each index.

    Uses live LTP from live_underlying_df. If unavailable, uses latest spot close
    from historical data indirectly through ltp = 0 handling outside scoring.
    """
    selected_rows: List[Dict[str, Any]] = []

    live_map = {
        str(row["index_symbol"]).upper(): row
        for _, row in live_underlying_df.iterrows()
    }

    today = ist_today()

    for info in infos:
        try:
            live_row = live_map.get(info.index_symbol, {})
            underlying_ltp = safe_float(live_row.get("ltp"), 0.0)

            if underlying_ltp <= 0:
                raise ValueError(f"Live spot LTP unavailable for {info.index_symbol}")

            atm = nearest_strike(underlying_ltp, info.strike_step)

            opt_df = get_option_universe_for_index(kite, info, instruments_cache)
            opt_df = opt_df[opt_df["expiry_date"] >= today].copy()

            if opt_df.empty:
                raise ValueError(f"No non-expired options found for {info.index_symbol}")

            # Nearest expiry.
            nearest_expiry = sorted(opt_df["expiry_date"].unique())[0]
            exp_df = opt_df[opt_df["expiry_date"] == nearest_expiry].copy()

            strikes = sorted(exp_df["strike_float"].unique())
            if not strikes:
                raise ValueError(f"No strikes found for {info.index_symbol} expiry {nearest_expiry}")

            selected_strike = min(strikes, key=lambda x: abs(float(x) - atm))

            for leg in ["CE", "PE"]:
                leg_df = exp_df[
                    (exp_df["strike_float"] == selected_strike)
                    & (exp_df["instrument_type"].astype(str).str.upper() == leg)
                ].copy()

                if leg_df.empty:
                    raise ValueError(
                        f"{info.index_symbol} {nearest_expiry} {selected_strike} {leg} not found"
                    )

                row = leg_df.iloc[0].to_dict()
                row_out = {
                    "index_symbol": info.index_symbol,
                    "underlying_ltp": underlying_ltp,
                    "atm_strike_calc": atm,
                    "selected_strike": selected_strike,
                    "leg": leg,
                    "derivative_exchange": info.derivative_exchange,
                    "tradingsymbol": row["tradingsymbol"],
                    "kite_key": f"{info.derivative_exchange}:{row['tradingsymbol']}",
                    "instrument_token": int(row["instrument_token"]),
                    "expiry": nearest_expiry,
                    "lot_size": int(row.get("lot_size_int", 0) or 0),
                    "selection_warning": "",
                }
                selected_rows.append(row_out)

        except Exception as exc:
            selected_rows.append(
                {
                    "index_symbol": info.index_symbol,
                    "underlying_ltp": 0.0,
                    "atm_strike_calc": 0.0,
                    "selected_strike": 0.0,
                    "leg": "",
                    "derivative_exchange": info.derivative_exchange,
                    "tradingsymbol": "",
                    "kite_key": "",
                    "instrument_token": 0,
                    "expiry": pd.NaT,
                    "lot_size": 0,
                    "selection_warning": str(exc),
                }
            )

    return pd.DataFrame(selected_rows)


# ============================================================
# OPTION DEPTH
# ============================================================

def sum_option_depth(depth_rows: List[Dict[str, Any]], lot_size: int) -> Tuple[int, float, int]:
    """
    Sum top-5 option depth.

    Returns:
        total_qty_units, total_lots, total_orders
    """
    total_qty = 0
    total_orders = 0

    for row in depth_rows or []:
        total_qty += safe_int(row.get("quantity"))
        total_orders += safe_int(row.get("orders"))

    lots = total_qty / lot_size if lot_size > 0 else 0.0
    return total_qty, lots, total_orders


def extract_option_leg_depth(row: pd.Series, quote: Dict[str, Any]) -> OptionLegDepth:
    """Extract ATM option CE/PE depth metrics."""
    index_symbol = str(row.get("index_symbol", "")).upper()
    leg = str(row.get("leg", "")).upper()
    derivative_exchange = str(row.get("derivative_exchange", "")).upper()
    tradingsymbol = str(row.get("tradingsymbol", ""))
    kite_key = str(row.get("kite_key", ""))

    lot_size = safe_int(row.get("lot_size"), 0)
    strike = safe_float(row.get("selected_strike"), 0.0)
    expiry_value = row.get("expiry")
    expiry = parse_expiry_date(expiry_value) or ist_today()

    instrument_token = safe_int(row.get("instrument_token"), 0)

    ltp = safe_float(quote.get("last_price"), 0.0)
    day_volume = safe_int(quote.get("volume"), 0)
    oi = safe_int(quote.get("oi"), 0)

    depth = quote.get("depth") or {}
    buy_rows = depth.get("buy") or []
    sell_rows = depth.get("sell") or []

    best_bid = safe_float(buy_rows[0].get("price")) if buy_rows else 0.0
    best_ask = safe_float(sell_rows[0].get("price")) if sell_rows else 0.0

    if best_bid > 0 and best_ask > 0:
        mid = (best_bid + best_ask) / 2.0
        spread_abs = best_ask - best_bid
        spread_pct = (spread_abs / mid) * 100.0 if mid > 0 else 999.0
    else:
        spread_abs = 0.0
        spread_pct = 999.0

    top5_bid_qty, top5_bid_lots, bid_orders = sum_option_depth(buy_rows[:5], lot_size)
    top5_ask_qty, top5_ask_lots, ask_orders = sum_option_depth(sell_rows[:5], lot_size)

    top5_min_side_lots = min(top5_bid_lots, top5_ask_lots)

    safe_buy_slice_lots = top5_ask_lots * 0.10
    safe_sell_slice_lots = top5_bid_lots * 0.10
    safe_two_way_slice_lots = min(safe_buy_slice_lots, safe_sell_slice_lots)

    denom = top5_bid_lots + top5_ask_lots
    depth_imbalance_pct = (
        ((top5_bid_lots - top5_ask_lots) / denom) * 100.0
        if denom > 0
        else 0.0
    )

    leg_depth_pass = (
        top5_min_side_lots >= MIN_ATM_TOP5_LOTS_EACH_SIDE
        and safe_two_way_slice_lots >= MIN_SAFE_SLICE_LOTS
    )

    leg_spread_pass = spread_pct <= MAX_ATM_OPTION_SPREAD_PCT

    warnings: List[str] = []

    if not buy_rows or not sell_rows:
        warnings.append("depth missing")
    if ltp <= 0:
        warnings.append("ltp missing")
    if lot_size <= 0:
        warnings.append("lot size missing")
    if not leg_depth_pass:
        warnings.append(
            f"low depth: min top5 lots={top5_min_side_lots:.1f}, "
            f"safe lots={safe_two_way_slice_lots:.1f}"
        )
    if not leg_spread_pass:
        warnings.append(f"wide spread: {spread_pct:.2f}%")

    return OptionLegDepth(
        index_symbol=index_symbol,
        leg=leg,
        derivative_exchange=derivative_exchange,
        tradingsymbol=tradingsymbol,
        kite_key=kite_key,
        instrument_token=instrument_token,
        expiry=expiry,
        strike=strike,
        lot_size=lot_size,
        ltp=ltp,
        day_volume=day_volume,
        oi=oi,
        best_bid=best_bid,
        best_ask=best_ask,
        spread_abs=spread_abs,
        spread_pct=spread_pct,
        top5_bid_qty=top5_bid_qty,
        top5_ask_qty=top5_ask_qty,
        top5_bid_lots=top5_bid_lots,
        top5_ask_lots=top5_ask_lots,
        top5_min_side_lots=top5_min_side_lots,
        bid_orders_top5=bid_orders,
        ask_orders_top5=ask_orders,
        safe_buy_slice_lots_10pct_ask=safe_buy_slice_lots,
        safe_sell_slice_lots_10pct_bid=safe_sell_slice_lots,
        safe_two_way_slice_lots=safe_two_way_slice_lots,
        depth_imbalance_pct=depth_imbalance_pct,
        leg_depth_pass=leg_depth_pass,
        leg_spread_pass=leg_spread_pass,
        leg_warning="; ".join(warnings),
    )


def fetch_option_depth(kite, selected_options_df: pd.DataFrame) -> pd.DataFrame:
    """Fetch live option depth for selected ATM CE/PE rows."""
    valid = selected_options_df[
        selected_options_df["kite_key"].astype(str).str.len() > 0
    ].copy()

    if valid.empty:
        return pd.DataFrame()

    keys = valid["kite_key"].dropna().astype(str).unique().tolist()

    print(f"[STEP] Fetching ATM option quotes/depth for {len(keys)} instruments ...")
    quotes = kite.quote(keys)

    rows: List[Dict[str, Any]] = []

    for _, opt_row in valid.iterrows():
        key = str(opt_row["kite_key"])
        q = quotes.get(key, {}) or {}

        try:
            depth = extract_option_leg_depth(opt_row, q)
            rows.append(depth.__dict__)
        except Exception as exc:
            rows.append(
                {
                    "index_symbol": opt_row.get("index_symbol"),
                    "leg": opt_row.get("leg"),
                    "kite_key": key,
                    "tradingsymbol": opt_row.get("tradingsymbol"),
                    "leg_depth_pass": False,
                    "leg_spread_pass": False,
                    "leg_warning": f"option depth extraction failed: {exc}",
                }
            )

    return pd.DataFrame(rows)


# ============================================================
# FINAL SCORING
# ============================================================

def rank_higher_better(s: pd.Series) -> pd.Series:
    """Percentile rank where higher is better."""
    x = pd.to_numeric(s, errors="coerce").fillna(0.0)

    if x.nunique(dropna=False) <= 1:
        return pd.Series(50.0, index=s.index)

    return x.rank(pct=True, method="average") * 100.0


def rank_lower_better(s: pd.Series) -> pd.Series:
    """Percentile rank where lower is better."""
    x = pd.to_numeric(s, errors="coerce").fillna(999999.0)

    if x.nunique(dropna=False) <= 1:
        return pd.Series(50.0, index=s.index)

    return x.rank(pct=True, method="average", ascending=False) * 100.0


def summarize_option_depth(option_depth_df: pd.DataFrame) -> pd.DataFrame:
    """Convert CE/PE leg rows into one row per index."""
    if option_depth_df.empty:
        return pd.DataFrame()

    rows: List[Dict[str, Any]] = []

    for index_symbol, g in option_depth_df.groupby("index_symbol"):
        ce = g[g["leg"] == "CE"].copy()
        pe = g[g["leg"] == "PE"].copy()

        ce_row = ce.iloc[0].to_dict() if not ce.empty else {}
        pe_row = pe.iloc[0].to_dict() if not pe.empty else {}

        ce_spread = safe_float(ce_row.get("spread_pct"), 999.0)
        pe_spread = safe_float(pe_row.get("spread_pct"), 999.0)

        ce_min_lots = safe_float(ce_row.get("top5_min_side_lots"), 0.0)
        pe_min_lots = safe_float(pe_row.get("top5_min_side_lots"), 0.0)

        ce_safe_lots = safe_float(ce_row.get("safe_two_way_slice_lots"), 0.0)
        pe_safe_lots = safe_float(pe_row.get("safe_two_way_slice_lots"), 0.0)

        lot_size = safe_int(ce_row.get("lot_size") or pe_row.get("lot_size"), 0)

        pair_min_top5_lots = min(ce_min_lots, pe_min_lots)
        pair_safe_slice_lots = min(ce_safe_lots, pe_safe_lots)
        avg_spread_pct = (ce_spread + pe_spread) / 2.0

        ce_pass = bool(ce_row.get("leg_depth_pass", False)) and bool(ce_row.get("leg_spread_pass", False))
        pe_pass = bool(pe_row.get("leg_depth_pass", False)) and bool(pe_row.get("leg_spread_pass", False))

        option_execution_pass = (
            ce_pass
            and pe_pass
            and pair_min_top5_lots >= MIN_ATM_TOP5_LOTS_EACH_SIDE
            and pair_safe_slice_lots >= MIN_SAFE_SLICE_LOTS
            and avg_spread_pct <= MAX_ATM_OPTION_SPREAD_PCT
        )

        rows.append(
            {
                "index_symbol": index_symbol,
                "expiry": ce_row.get("expiry") or pe_row.get("expiry"),
                "strike": ce_row.get("strike") or pe_row.get("strike"),
                "lot_size": lot_size,

                "ce_tradingsymbol": ce_row.get("tradingsymbol", ""),
                "pe_tradingsymbol": pe_row.get("tradingsymbol", ""),

                "ce_ltp": safe_float(ce_row.get("ltp"), 0.0),
                "pe_ltp": safe_float(pe_row.get("ltp"), 0.0),

                "ce_spread_pct": ce_spread,
                "pe_spread_pct": pe_spread,
                "avg_atm_spread_pct": avg_spread_pct,

                "ce_top5_min_side_lots": ce_min_lots,
                "pe_top5_min_side_lots": pe_min_lots,
                "pair_min_top5_lots": pair_min_top5_lots,

                "ce_safe_slice_lots": ce_safe_lots,
                "pe_safe_slice_lots": pe_safe_lots,
                "pair_safe_slice_lots": pair_safe_slice_lots,

                "ce_depth_pass": bool(ce_row.get("leg_depth_pass", False)),
                "pe_depth_pass": bool(pe_row.get("leg_depth_pass", False)),
                "ce_spread_pass": bool(ce_row.get("leg_spread_pass", False)),
                "pe_spread_pass": bool(pe_row.get("leg_spread_pass", False)),
                "option_execution_pass": option_execution_pass,

                "ce_warning": ce_row.get("leg_warning", ""),
                "pe_warning": pe_row.get("leg_warning", ""),
            }
        )

    return pd.DataFrame(rows)


def build_final_ranking(
    historical_df: pd.DataFrame,
    live_underlying_df: pd.DataFrame,
    option_summary_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build final ranking across NIFTY / SENSEX / BANKNIFTY.

    Weights:
        Historical spot movement     : 40%
        Live spot movement           : 20%
        ATM option execution quality : 40%
    """
    if historical_df.empty:
        return pd.DataFrame()

    df = historical_df.copy()

    df = df.merge(live_underlying_df, on="index_symbol", how="left")
    df = df.merge(option_summary_df, on="index_symbol", how="left")

    numeric_defaults = {
        "live_hl_range_pct": 0.0,
        "live_body_pct": 0.0,
        "live_body_efficiency_pct": 0.0,
        "day_change_pct": 0.0,
        "pair_min_top5_lots": 0.0,
        "pair_safe_slice_lots": 0.0,
        "avg_atm_spread_pct": 999.0,
    }

    for col, default in numeric_defaults.items():
        if col not in df.columns:
            df[col] = default
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(default)

    if "option_execution_pass" not in df.columns:
        df["option_execution_pass"] = False

    # Historical spot movement: 40%.
    df["rank_hist_hl_1y"] = rank_higher_better(df["median_hl_range_pct_1y"])
    df["rank_hist_body_1y"] = rank_higher_better(df["median_body_pct_1y"])
    df["rank_hist_hl_3m"] = rank_higher_better(df["median_hl_range_pct_3m"])
    df["rank_hist_body_3m"] = rank_higher_better(df["median_body_pct_3m"])
    df["rank_hist_active_body"] = rank_higher_better(df["active_body_day_pct_1y"])

    df["historical_movement_score"] = (
        0.12 * df["rank_hist_hl_1y"]
        + 0.10 * df["rank_hist_body_1y"]
        + 0.06 * df["rank_hist_hl_3m"]
        + 0.06 * df["rank_hist_body_3m"]
        + 0.06 * df["rank_hist_active_body"]
    )

    # Live spot movement: 20%.
    df["rank_live_hl"] = rank_higher_better(df["live_hl_range_pct"])
    df["rank_live_body"] = rank_higher_better(df["live_body_pct"])
    df["rank_live_eff"] = rank_higher_better(df["live_body_efficiency_pct"])
    df["rank_live_abs_change"] = rank_higher_better(df["day_change_pct"].abs())

    df["live_underlying_score"] = (
        0.08 * df["rank_live_hl"]
        + 0.06 * df["rank_live_body"]
        + 0.03 * df["rank_live_eff"]
        + 0.03 * df["rank_live_abs_change"]
    )

    # ATM option execution: 40%.
    df["rank_option_depth"] = rank_higher_better(df["pair_min_top5_lots"])
    df["rank_safe_slice"] = rank_higher_better(df["pair_safe_slice_lots"])
    df["rank_option_spread"] = rank_lower_better(df["avg_atm_spread_pct"])

    df["option_availability_score"] = np.where(df["option_execution_pass"].astype(bool), 100.0, 0.0)

    df["option_execution_score"] = (
        0.15 * df["rank_option_depth"]
        + 0.10 * df["rank_safe_slice"]
        + 0.10 * df["rank_option_spread"]
        + 0.05 * df["option_availability_score"]
    )

    df["raw_tradability_score"] = (
        df["historical_movement_score"]
        + df["live_underlying_score"]
        + df["option_execution_score"]
    )

    # Penalties.
    df["data_penalty"] = 0.0
    df.loc[df["days_2y"] < 350, "data_penalty"] += 10.0
    df.loc[df["days_1y"] < 180, "data_penalty"] += 8.0
    df.loc[df["days_3m"] < 40, "data_penalty"] += 5.0

    df["option_penalty"] = 0.0
    df.loc[df["pair_min_top5_lots"] < MIN_ATM_TOP5_LOTS_EACH_SIDE, "option_penalty"] += 15.0
    df.loc[df["pair_safe_slice_lots"] < MIN_SAFE_SLICE_LOTS, "option_penalty"] += 10.0
    df.loc[df["avg_atm_spread_pct"] > MAX_ATM_OPTION_SPREAD_PCT, "option_penalty"] += 10.0
    df.loc[~df["option_execution_pass"].astype(bool), "option_penalty"] += 10.0

    df["final_tradability_score"] = (
        df["raw_tradability_score"] - df["data_penalty"] - df["option_penalty"]
    ).clip(lower=0, upper=100)

    def classify(row: pd.Series) -> str:
        if row["days_2y"] < 350 or row["days_1y"] < 180:
            return "REJECT_LOW_DATA"

        if not bool(row.get("option_execution_pass", False)):
            if row["pair_min_top5_lots"] <= 0:
                return "REJECT_OPTION_DEPTH_UNAVAILABLE"
            return "WATCH_OPTION_EXECUTION"

        if row["final_tradability_score"] >= 80:
            return "STRONG_INDEX_OPTION_TRADABLE"
        if row["final_tradability_score"] >= 70:
            return "GOOD_INDEX_OPTION_TRADABLE"
        if row["final_tradability_score"] >= 60:
            return "WATCHLIST_INDEX_OPTION_TRADABLE"

        return "REJECT_WEAK_INDEX_TRADABILITY"

    df["tradability_verdict"] = df.apply(classify, axis=1)

    ordered_cols = [
        "index_symbol",
        "tradability_verdict",
        "final_tradability_score",
        "raw_tradability_score",
        "historical_movement_score",
        "live_underlying_score",
        "option_execution_score",
        "data_penalty",
        "option_penalty",

        # Historical spot movement.
        "median_hl_range_pct_1y",
        "median_body_pct_1y",
        "median_body_efficiency_pct_1y",
        "active_hl_day_pct_1y",
        "active_body_day_pct_1y",
        "median_hl_range_pct_3m",
        "median_body_pct_3m",
        "p75_hl_range_pct_1y",
        "p90_hl_range_pct_1y",
        "p75_body_pct_1y",
        "p90_body_pct_1y",

        # Live spot movement.
        "ltp",
        "day_open",
        "day_high",
        "day_low",
        "prev_close",
        "day_change_pct",
        "live_hl_range_pct",
        "live_body_pct",
        "live_body_signed_pct",
        "live_body_efficiency_pct",

        # ATM option execution.
        "expiry",
        "strike",
        "lot_size",
        "ce_tradingsymbol",
        "pe_tradingsymbol",
        "ce_ltp",
        "pe_ltp",
        "ce_spread_pct",
        "pe_spread_pct",
        "avg_atm_spread_pct",
        "ce_top5_min_side_lots",
        "pe_top5_min_side_lots",
        "pair_min_top5_lots",
        "ce_safe_slice_lots",
        "pe_safe_slice_lots",
        "pair_safe_slice_lots",
        "option_execution_pass",

        # Coverage.
        "days_2y",
        "days_1y",
        "days_3m",
        "first_date",
        "last_date",
        "spot_kite_key",
        "spot_token",
        "data_warning",
        "live_warning",
        "ce_warning",
        "pe_warning",
    ]

    existing = [c for c in ordered_cols if c in df.columns]

    df = df[existing].sort_values(
        by=[
            "final_tradability_score",
            "option_execution_score",
            "historical_movement_score",
            "live_underlying_score",
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
    ranking_df: pd.DataFrame,
    daily_features_df: pd.DataFrame,
    live_underlying_df: pd.DataFrame,
    option_depth_df: pd.DataFrame,
    selected_options_df: pd.DataFrame,
    failed_df: pd.DataFrame,
    config_df: pd.DataFrame,
    output_path: str,
) -> None:
    """Write Excel workbook."""
    ranking_df = make_excel_safe_df(ranking_df)
    daily_features_df = make_excel_safe_df(daily_features_df)
    live_underlying_df = make_excel_safe_df(live_underlying_df)
    option_depth_df = make_excel_safe_df(option_depth_df)
    selected_options_df = make_excel_safe_df(selected_options_df)
    failed_df = make_excel_safe_df(failed_df)
    config_df = make_excel_safe_df(config_df)

    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        ranking_df.to_excel(writer, index=False, sheet_name="ranked_indices")
        daily_features_df.to_excel(writer, index=False, sheet_name="daily_features")
        live_underlying_df.to_excel(writer, index=False, sheet_name="live_underlying")
        option_depth_df.to_excel(writer, index=False, sheet_name="atm_option_depth")
        selected_options_df.to_excel(writer, index=False, sheet_name="selected_atm_options")
        failed_df.to_excel(writer, index=False, sheet_name="missing_or_failed")
        config_df.to_excel(writer, index=False, sheet_name="config")

        sheets = {
            "ranked_indices": ranking_df,
            "daily_features": daily_features_df,
            "live_underlying": live_underlying_df,
            "atm_option_depth": option_depth_df,
            "selected_atm_options": selected_options_df,
            "missing_or_failed": failed_df,
            "config": config_df,
        }

        workbook = writer.book
        fmt_num = workbook.add_format({"num_format": "0.00"})
        fmt_int = workbook.add_format({"num_format": "0"})

        for sheet_name, df in sheets.items():
            ws = writer.sheets[sheet_name]
            ws.freeze_panes(1, 0)

            if not df.empty:
                ws.autofilter(0, 0, len(df), len(df.columns) - 1)

            autosize_excel_columns(writer, sheet_name, df)

            for col_name in df.columns:
                idx = df.columns.get_loc(col_name)
                lower = col_name.lower()

                if (
                    "pct" in lower
                    or "score" in lower
                    or "price" in lower
                    or "spread" in lower
                    or "ltp" in lower
                    or lower in {"open", "high", "low", "close", "strike"}
                ):
                    ws.set_column(idx, idx, 16, fmt_num)

                if (
                    "qty" in lower
                    or "volume" in lower
                    or "orders" in lower
                    or "lots" in lower
                    or "lot_size" in lower
                ):
                    ws.set_column(idx, idx, 14, fmt_int)


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    """Main entrypoint."""
    print("============================================================")
    print("INDEX INTRADAY TRADABILITY SELECTOR")
    print("NIFTY + SENSEX + BANKNIFTY")
    print("Spot Movement + Live Spot + ATM Option Depth")
    print("============================================================")

    from_date_buffered, start_2y, start_1y, start_3m, to_date = compute_master_date_range()

    print(f"[CONFIG] Index symbols                    : {INDEX_SYMBOLS}")
    print(f"[CONFIG] Output Excel                     : {OUTPUT_EXCEL}")
    print(f"[CONFIG] Download from                    : {from_date_buffered}")
    print(f"[CONFIG] Historical end                   : {to_date}")
    print(f"[CONFIG] Use today daily candle           : {USE_TODAY_DAILY_CANDLE}")
    print(f"[CONFIG] Min ATM top5 lots each side      : {MIN_ATM_TOP5_LOTS_EACH_SIDE:.0f}")
    print(f"[CONFIG] Min safe slice lots              : {MIN_SAFE_SLICE_LOTS:.0f}")
    print(f"[CONFIG] Max ATM option spread            : {MAX_ATM_OPTION_SPREAD_PCT:.2f}%")
    print(f"[CONFIG] Force refresh                    : {FORCE_REFRESH}")
    print("============================================================")

    print("[STEP] Initializing Kite ...")
    kite = initialize_kite()
    print("[OK] Kite initialized.")

    instruments_cache: Dict[str, List[Dict[str, Any]]] = {}

    infos: List[SpotIndexInfo] = []
    historical_results: List[IndexHistoricalMetrics] = []
    daily_frames: List[pd.DataFrame] = []
    failed_rows: List[Dict[str, Any]] = []

    for idx, index_symbol in enumerate(INDEX_SYMBOLS, start=1):
        print("\n------------------------------------------------------------")
        print(f"[INDEX {idx}/{len(INDEX_SYMBOLS)}] {index_symbol}")
        print("------------------------------------------------------------")

        try:
            info = resolve_spot_index(kite, index_symbol, instruments_cache)
            infos.append(info)

            print(
                f"[INFO] Spot={info.spot_kite_key} token={info.spot_token}; "
                f"Options={info.derivative_exchange}:{info.derivative_name}"
            )

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

            featured_df = add_daily_features(index_symbol, info.spot_token, raw_df)

            metrics, daily_df = calculate_index_metrics(
                info=info,
                featured_df=featured_df,
                start_2y=start_2y,
                start_1y=start_1y,
                start_3m=start_3m,
                to_date=to_date,
            )

            historical_results.append(metrics)
            daily_frames.append(daily_df)

            print(
                f"[OK] {index_symbol}: "
                f"HL_1Y={metrics.median_hl_range_pct_1y:.2f}%, "
                f"Body_1Y={metrics.median_body_pct_1y:.2f}%, "
                f"Eff_1Y={metrics.median_body_efficiency_pct_1y:.1f}%, "
                f"ActiveBody={metrics.active_body_day_pct_1y:.1f}%"
            )

            if metrics.data_warning:
                print(f"[WARN] {metrics.data_warning}")

        except Exception as exc:
            print(f"[ERROR] {index_symbol} failed: {exc}")
            traceback.print_exc()
            failed_rows.append({"index_symbol": index_symbol, "stage": "historical", "error": str(exc)})

    historical_df = pd.DataFrame([x.__dict__ for x in historical_results])

    if daily_frames:
        daily_features_df = pd.concat(daily_frames, ignore_index=True)
        daily_features_df = daily_features_df.sort_values(["index_symbol", "trade_date"]).reset_index(drop=True)
    else:
        daily_features_df = pd.DataFrame()

    print("\n[STEP] Fetching live underlying index movement ...")
    try:
        live_underlying_df = fetch_live_underlying(kite, infos)
    except Exception as exc:
        print(f"[WARN] Live underlying fetch failed: {exc}")
        live_underlying_df = pd.DataFrame(
            [{"index_symbol": info.index_symbol, "live_warning": f"live fetch failed: {exc}"} for info in infos]
        )

    print("\n[STEP] Selecting nearest-expiry ATM CE/PE options ...")
    try:
        selected_options_df = select_atm_options(
            kite=kite,
            infos=infos,
            live_underlying_df=live_underlying_df,
            instruments_cache=instruments_cache,
        )
    except Exception as exc:
        print(f"[WARN] ATM option selection failed: {exc}")
        selected_options_df = pd.DataFrame()

    print("\n[STEP] Fetching ATM option live depth ...")
    try:
        option_depth_df = fetch_option_depth(kite, selected_options_df)
    except Exception as exc:
        print(f"[WARN] Option depth fetch failed: {exc}")
        option_depth_df = pd.DataFrame()

    print("\n[STEP] Building option depth summary ...")
    option_summary_df = summarize_option_depth(option_depth_df)

    print("[STEP] Building final ranking ...")
    ranking_df = build_final_ranking(
        historical_df=historical_df,
        live_underlying_df=live_underlying_df,
        option_summary_df=option_summary_df,
    )

    failed_df = pd.DataFrame(failed_rows)

    config_df = pd.DataFrame(
        [
            {"parameter": "INDEX_SYMBOLS", "value": ",".join(INDEX_SYMBOLS)},
            {"parameter": "OUTPUT_EXCEL", "value": OUTPUT_EXCEL},
            {"parameter": "FROM_DATE_BUFFERED", "value": str(from_date_buffered)},
            {"parameter": "START_2Y", "value": str(start_2y)},
            {"parameter": "START_1Y", "value": str(start_1y)},
            {"parameter": "START_3M", "value": str(start_3m)},
            {"parameter": "TO_DATE", "value": str(to_date)},
            {"parameter": "USE_TODAY_DAILY_CANDLE", "value": USE_TODAY_DAILY_CANDLE},
            {"parameter": "HL_RANGE_FORMULA", "value": "(high-low)/close*100"},
            {"parameter": "BODY_FORMULA", "value": "abs(close-open)/open*100"},
            {"parameter": "BODY_EFFICIENCY_FORMULA", "value": "abs(close-open)/(high-low)*100"},
            {"parameter": "HL_ACTIVE_THRESHOLD_PCT", "value": HL_ACTIVE_THRESHOLD_PCT},
            {"parameter": "BODY_ACTIVE_THRESHOLD_PCT", "value": BODY_ACTIVE_THRESHOLD_PCT},
            {"parameter": "MIN_ATM_TOP5_LOTS_EACH_SIDE", "value": MIN_ATM_TOP5_LOTS_EACH_SIDE},
            {"parameter": "MIN_SAFE_SLICE_LOTS", "value": MIN_SAFE_SLICE_LOTS},
            {"parameter": "MAX_ATM_OPTION_SPREAD_PCT", "value": MAX_ATM_OPTION_SPREAD_PCT},
            {
                "parameter": "FINAL_SCORING",
                "value": (
                    "40% historical spot movement + 20% live spot movement + "
                    "40% ATM option execution quality; penalties for weak ATM depth, "
                    "wide ATM spread, and low historical data coverage"
                ),
            },
            {"parameter": "DATA_CACHE_DIR", "value": DATA_CACHE_DIR},
            {"parameter": "FORCE_REFRESH", "value": FORCE_REFRESH},
        ]
    )

    print("[STEP] Writing Excel report ...")
    write_excel_report(
        ranking_df=ranking_df,
        daily_features_df=daily_features_df,
        live_underlying_df=live_underlying_df,
        option_depth_df=option_depth_df,
        selected_options_df=selected_options_df,
        failed_df=failed_df,
        config_df=config_df,
        output_path=OUTPUT_EXCEL,
    )

    print("\n==================== FINAL RESULT ====================")

    if ranking_df.empty:
        print("[ERROR] No indices ranked.")
    else:
        top_cols = [
            "rank",
            "index_symbol",
            "tradability_verdict",
            "final_tradability_score",
            "historical_movement_score",
            "live_underlying_score",
            "option_execution_score",
            "median_hl_range_pct_1y",
            "median_body_pct_1y",
            "live_hl_range_pct",
            "live_body_pct",
            "avg_atm_spread_pct",
            "pair_min_top5_lots",
            "pair_safe_slice_lots",
            "option_execution_pass",
        ]
        top_cols = [c for c in top_cols if c in ranking_df.columns]

        print(ranking_df[top_cols].head(TOP_N_TO_PRINT).to_string(index=False))

        best = ranking_df.iloc[0]
        print("\nBest current index-option tradability candidate:")
        print(f"Index                      : {best['index_symbol']}")
        print(f"Score                      : {best['final_tradability_score']:.2f}")
        print(f"Verdict                    : {best['tradability_verdict']}")
        print(f"1Y median HL range         : {best['median_hl_range_pct_1y']:.2f}%")
        print(f"1Y median body             : {best['median_body_pct_1y']:.2f}%")
        print(f"Live HL range              : {best['live_hl_range_pct']:.2f}%")
        print(f"Live body                  : {best['live_body_pct']:.2f}%")
        print(f"ATM avg spread             : {best['avg_atm_spread_pct']:.2f}%")
        print(f"ATM pair min top5 lots     : {best['pair_min_top5_lots']:.1f}")
        print(f"ATM safe slice lots        : {best['pair_safe_slice_lots']:.1f}")

    print(f"\n[DONE] Excel saved: {OUTPUT_EXCEL}")
    print("=======================================================")


if __name__ == "__main__":
    main()