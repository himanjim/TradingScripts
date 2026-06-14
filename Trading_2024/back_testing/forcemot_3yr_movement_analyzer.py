#!/usr/bin/env python3
"""
forcemot_3yr_movement_analyzer.py

Purpose
-------
Analyze FORCEMOT daily movement in depth over the last 3 years.

This script downloads daily OHLCV candles from Zerodha Kite and generates
an Excel report with:

    1. daily_analysis
       Day-wise movement statistics for every trading day.

    2. month_summary
       Month-wise summary of movement, body, efficiency, gap, volume,
       traded value, and returns.

    3. month_range_distribution
       Monthly count of days by high-low range bucket.

    4. month_body_distribution
       Monthly count of days by open-close body bucket.

    5. weekday_summary
       Behaviour by weekday.

    6. yearly_summary
       Year-wise summary.

    7. config
       Run configuration and formulas used.

Core daily movement formulas
----------------------------
Same-day high-low range:

    high_low_range_abs = high - low
    high_low_range_pct_close = (high - low) / close * 100
    high_low_range_pct_low   = (high / low - 1) * 100

Open-close body movement:

    body_abs = abs(close - open)
    body_pct = abs(close - open) / open * 100
    body_signed_pct = (close - open) / open * 100

Body efficiency:

    body_efficiency_pct = abs(close - open) / (high - low) * 100

Close location inside day's range:

    close_location_pct = (close - low) / (high - low) * 100

Interpretation:
    close_location_pct near 100 = closed near high
    close_location_pct near 0   = closed near low
    close_location_pct near 50  = closed near middle

Approximate traded value:
    typical_price = (high + low + close) / 3
    traded_value_cr = typical_price * volume / 1e7

Important
---------
This script uses daily candles. It answers:

    "How does FORCEMOT move day by day and month by month?"

It does NOT test a specific intraday entry/exit strategy.
For strategy testing, use 1-minute or 5-minute data later.

Dependency
----------
Uses your existing Kite login helper:

    Trading_2024.OptionTradeUtils.intialize_kite_api()

Fallback:

    OptionTradeUtils.intialize_kite_api()

Run
---
    python forcemot_3yr_movement_analyzer.py

Optional environment overrides
------------------------------
    set SYMBOL=FORCEMOT
    set LOOKBACK_YEARS=3
    set OUTPUT_EXCEL=FORCEMOT_3yr_movement_analysis.xlsx
    set FORCE_REFRESH=1
"""

from __future__ import annotations

import os
import time
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

EXCHANGE = "NSE"

SYMBOL = os.environ.get("SYMBOL", "FORCEMOT").strip().upper()

LOOKBACK_YEARS = int(os.environ.get("LOOKBACK_YEARS", "3"))

OUTPUT_EXCEL = os.environ.get(
    "OUTPUT_EXCEL",
    f"{SYMBOL}_3yr_movement_analysis.xlsx",
).strip()

DATA_CACHE_DIR = os.environ.get("DATA_CACHE_DIR", "./stock_daily_cache").strip()

FORCE_REFRESH = os.environ.get("FORCE_REFRESH", "0").strip() == "1"

# Daily candles can be fetched in large chunks, but chunking improves retry safety.
MAX_DAYS_PER_CHUNK = 365

MAX_ATTEMPTS = 5

SLEEP_BETWEEN_CALLS_SEC = 0.25

# Rolling windows for diagnostics.
ROLLING_SHORT = 5
ROLLING_MEDIUM = 20

# Range buckets for distribution analysis.
RANGE_BUCKETS = [-np.inf, 1, 2, 3, 4, 5, np.inf]
RANGE_BUCKET_LABELS = ["<=1%", "1-2%", "2-3%", "3-4%", "4-5%", ">5%"]

BODY_BUCKETS = [-np.inf, 0.5, 1, 1.5, 2, 3, np.inf]
BODY_BUCKET_LABELS = ["<=0.5%", "0.5-1%", "1-1.5%", "1.5-2%", "2-3%", ">3%"]


# ============================================================
# DATA STRUCTURES
# ============================================================

@dataclass(frozen=True)
class InstrumentInfo:
    """Resolved Kite instrument details."""

    symbol: str
    exchange: str
    instrument_token: int
    name: str


# ============================================================
# DATE / TIME HELPERS
# ============================================================

def ist_today() -> date:
    """Return today's date in Asia/Kolkata."""
    if ZoneInfo is not None:
        try:
            return datetime.now(ZoneInfo("Asia/Kolkata")).date()
        except Exception:
            pass
    return datetime.now().date()


def compute_date_range() -> Tuple[date, date]:
    """
    Compute the last N years date range.

    Adds a small buffer of 10 days so rolling/previous-close features have
    enough earlier rows. Final filtering is done later.
    """
    to_date = ist_today()
    from_date = to_date - timedelta(days=365 * LOOKBACK_YEARS + 10)
    return from_date, to_date


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
    """Initialize Kite using the user's existing helper."""
    if oUtils is None:
        raise RuntimeError(
            "OptionTradeUtils is not importable. Ensure Trading_2024.OptionTradeUtils "
            "or OptionTradeUtils.py is available."
        )

    kite = oUtils.intialize_kite_api()

    if kite is None:
        raise RuntimeError("intialize_kite_api() returned None. Check Kite login.")

    return kite


def resolve_equity_token(kite, symbol: str, exchange: str = EXCHANGE) -> InstrumentInfo:
    """
    Resolve NSE equity instrument token for the given symbol.

    This scans the NSE instruments dump and prioritizes normal EQ rows.
    """
    print(f"[STEP] Loading {exchange} instruments dump ...")
    instruments = kite.instruments(exchange)
    print(f"[INFO] Instrument rows received: {len(instruments):,}")

    symbol_u = symbol.strip().upper()
    matches: List[Dict[str, Any]] = []

    for inst in instruments:
        tsym = str(inst.get("tradingsymbol", "")).strip().upper()
        if tsym == symbol_u:
            matches.append(inst)

    if not matches:
        raise ValueError(f"Instrument not found on {exchange}: {symbol_u}")

    def priority(inst: Dict[str, Any]) -> int:
        instrument_type = str(inst.get("instrument_type", "")).upper()
        segment = str(inst.get("segment", "")).upper()
        exchange_value = str(inst.get("exchange", "")).upper()

        if instrument_type == "EQ" and segment == exchange and exchange_value == exchange:
            return 0
        if instrument_type == "EQ":
            return 1
        if segment == exchange:
            return 2
        return 3

    matches.sort(key=priority)
    selected = matches[0]

    return InstrumentInfo(
        symbol=symbol_u,
        exchange=exchange,
        instrument_token=int(selected["instrument_token"]),
        name=str(selected.get("name", symbol_u)),
    )


# ============================================================
# CACHE
# ============================================================

def cache_path_for_symbol(symbol: str) -> str:
    """Return safe pickle cache path for one symbol."""
    safe = (
        symbol.replace("/", "_")
        .replace("\\", "_")
        .replace(":", "_")
        .replace("&", "AND")
    )
    return os.path.join(DATA_CACHE_DIR, f"{safe}_daily_3yr_analysis.pkl")


def normalize_cached_df(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize dataframe loaded from cache."""
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()

    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce")

    if "trade_date" not in out.columns and "date" in out.columns:
        out["trade_date"] = out["date"].dt.date

    return out


def load_cached_if_usable(symbol: str, from_date: date, to_date: date) -> Optional[pd.DataFrame]:
    """
    Load cached daily data if it covers the requested date range.

    The end date may be a few days behind because today may not have a completed
    daily candle and weekends/holidays naturally create gaps.
    """
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

        if min_d <= from_date and max_d >= to_date - timedelta(days=7):
            print(f"[CACHE] Using cached data: {path}")
            return df

        print(f"[CACHE] Stale/incomplete cache: {min_d} -> {max_d}. Re-downloading.")
        return None

    except Exception as exc:
        print(f"[WARN] Could not read cache: {exc}")
        return None


def save_cache(symbol: str, df: pd.DataFrame) -> None:
    """Save raw daily data to pickle cache."""
    os.makedirs(DATA_CACHE_DIR, exist_ok=True)
    df.to_pickle(cache_path_for_symbol(symbol))


# ============================================================
# HISTORICAL DOWNLOAD
# ============================================================

def fetch_history_day(
    kite,
    instrument_token: int,
    from_date: date,
    to_date: date,
    label: str,
) -> List[Dict[str, Any]]:
    """Fetch daily historical candles from Kite with retries and chunking."""
    chunks = iter_date_chunks(from_date, to_date, MAX_DAYS_PER_CHUNK)

    print(
        f"[INFO] Fetching daily data for {label} "
        f"token={instrument_token} from {from_date} to {to_date} "
        f"in {len(chunks)} chunk(s)."
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

                print(f"    [OK] Retrieved {len(rows)} candles on attempt {attempt}")
                all_rows.extend(rows)
                last_err = None
                break

            except Exception as exc:
                last_err = exc
                wait = min(8.0, 1.5 * attempt)
                print(
                    f"    [WARN] Attempt {attempt}/{MAX_ATTEMPTS} failed: {exc}. "
                    f"Sleeping {wait:.1f}s"
                )
                time.sleep(wait)

        if last_err is not None:
            print(f"    [ERROR] Giving up on chunk {idx}: {last_err}")

        time.sleep(SLEEP_BETWEEN_CALLS_SEC)

    return all_rows


def rows_to_dataframe(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    """Convert Kite daily candle rows into clean dataframe."""
    cols = ["date", "open", "high", "low", "close", "volume"]

    if not rows:
        return pd.DataFrame(columns=cols)

    df = pd.DataFrame(rows)

    for col in cols:
        if col not in df.columns:
            df[col] = np.nan

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
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

def add_daily_movement_features(df: pd.DataFrame, symbol: str, instrument_token: int) -> pd.DataFrame:
    """
    Add detailed daily movement features for FORCEMOT.

    Main analysis areas:
        - High-low range
        - Open-close body
        - Body efficiency
        - Gaps
        - True range / ATR
        - Close location
        - Volume and traded value
        - Rolling movement averages
        - Bucket classifications
    """
    if df.empty:
        raise ValueError("No data supplied to add_daily_movement_features().")

    out = df.copy()

    out["symbol"] = symbol
    out["instrument_token"] = instrument_token

    # Calendar columns.
    out["trade_date"] = pd.to_datetime(out["trade_date"])
    out["year"] = out["trade_date"].dt.year
    out["month"] = out["trade_date"].dt.month
    out["month_name"] = out["trade_date"].dt.strftime("%b")
    out["year_month"] = out["trade_date"].dt.to_period("M").astype(str)
    out["weekday"] = out["trade_date"].dt.day_name()

    # Previous close.
    out["prev_close"] = out["close"].shift(1)

    # Gap from previous close to today's open.
    out["gap_abs"] = out["open"] - out["prev_close"]
    out["gap_pct"] = (out["gap_abs"] / out["prev_close"]) * 100.0

    out["gap_direction"] = np.select(
        [
            out["gap_pct"] > 0.10,
            out["gap_pct"] < -0.10,
        ],
        [
            "GAP_UP",
            "GAP_DOWN",
        ],
        default="FLAT_OPEN",
    )

    # Close-to-close return.
    out["close_to_close_ret_pct"] = ((out["close"] / out["prev_close"]) - 1.0) * 100.0

    # Intraday signed body: close versus open.
    out["body_signed_abs"] = out["close"] - out["open"]
    out["body_signed_pct"] = (out["body_signed_abs"] / out["open"]) * 100.0

    # Absolute body size.
    out["body_abs"] = out["body_signed_abs"].abs()
    out["body_pct"] = (out["body_abs"] / out["open"]) * 100.0

    # Same-day high-low range.
    out["high_low_range_abs"] = out["high"] - out["low"]

    # Two versions are useful:
    # 1. versus close: consistent with earlier selector
    # 2. versus low: intuitive "from low to high" intraday move
    out["high_low_range_pct_close"] = (out["high_low_range_abs"] / out["close"]) * 100.0
    out["high_low_range_pct_low"] = ((out["high"] / out["low"]) - 1.0) * 100.0

    # Upper and lower wick sizes.
    out["upper_wick_abs"] = out["high"] - out[["open", "close"]].max(axis=1)
    out["lower_wick_abs"] = out[["open", "close"]].min(axis=1) - out["low"]
    out["total_wick_abs"] = out["upper_wick_abs"] + out["lower_wick_abs"]

    out["upper_wick_pct"] = (out["upper_wick_abs"] / out["open"]) * 100.0
    out["lower_wick_pct"] = (out["lower_wick_abs"] / out["open"]) * 100.0
    out["total_wick_pct"] = (out["total_wick_abs"] / out["open"]) * 100.0

    # Body efficiency:
    # How much of the day's high-low range became open-close body.
    out["body_efficiency_pct"] = np.where(
        out["high_low_range_abs"] > 0,
        (out["body_abs"] / out["high_low_range_abs"]) * 100.0,
        0.0,
    )
    out["body_efficiency_pct"] = out["body_efficiency_pct"].clip(lower=0.0, upper=100.0)

    # Close location inside high-low range:
    # 0 = close near low, 100 = close near high.
    out["close_location_pct"] = np.where(
        out["high_low_range_abs"] > 0,
        ((out["close"] - out["low"]) / out["high_low_range_abs"]) * 100.0,
        50.0,
    )
    out["close_location_pct"] = out["close_location_pct"].clip(lower=0.0, upper=100.0)

    # Open location inside high-low range.
    out["open_location_pct"] = np.where(
        out["high_low_range_abs"] > 0,
        ((out["open"] - out["low"]) / out["high_low_range_abs"]) * 100.0,
        50.0,
    )
    out["open_location_pct"] = out["open_location_pct"].clip(lower=0.0, upper=100.0)

    # Candle colour / day type.
    out["day_type"] = np.select(
        [
            out["body_signed_pct"] > 0.10,
            out["body_signed_pct"] < -0.10,
        ],
        [
            "GREEN",
            "RED",
        ],
        default="DOJI_OR_FLAT",
    )

    # Strong directional day:
    # Body efficiency above 50 and body movement above 1%.
    out["strong_directional_day"] = (
        (out["body_efficiency_pct"] >= 50.0)
        & (out["body_pct"] >= 1.0)
    )

    # Strong range day:
    # High-low movement above 3%.
    out["strong_range_day"] = out["high_low_range_pct_close"] >= 3.0

    # Very large range day:
    # High-low movement above 5%.
    out["very_large_range_day"] = out["high_low_range_pct_close"] >= 5.0

    # True range and ATR.
    tr1 = out["high"] - out["low"]
    tr2 = (out["high"] - out["prev_close"]).abs()
    tr3 = (out["low"] - out["prev_close"]).abs()

    out["true_range_abs"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    out["true_range_pct"] = (out["true_range_abs"] / out["close"]) * 100.0

    out["atr14_abs"] = out["true_range_abs"].rolling(14, min_periods=5).mean()
    out["atr14_pct"] = (out["atr14_abs"] / out["close"]) * 100.0

    # Approximate traded value.
    out["typical_price"] = (out["high"] + out["low"] + out["close"]) / 3.0
    out["traded_value_rs"] = out["typical_price"] * out["volume"]
    out["traded_value_cr"] = out["traded_value_rs"] / 1e7

    # Rolling liquidity / participation.
    out["volume_5d_avg"] = out["volume"].rolling(ROLLING_SHORT, min_periods=2).mean()
    out["volume_20d_avg"] = out["volume"].rolling(ROLLING_MEDIUM, min_periods=5).mean()

    out["value_5d_avg_cr"] = out["traded_value_cr"].rolling(ROLLING_SHORT, min_periods=2).mean()
    out["value_20d_avg_cr"] = out["traded_value_cr"].rolling(ROLLING_MEDIUM, min_periods=5).mean()

    out["volume_vs_20d_x"] = out["volume"] / out["volume_20d_avg"]
    out["value_vs_20d_x"] = out["traded_value_cr"] / out["value_20d_avg_cr"]

    # Rolling movement averages.
    out["hl_range_5d_avg_pct"] = out["high_low_range_pct_close"].rolling(ROLLING_SHORT, min_periods=2).mean()
    out["hl_range_20d_avg_pct"] = out["high_low_range_pct_close"].rolling(ROLLING_MEDIUM, min_periods=5).mean()

    out["body_5d_avg_pct"] = out["body_pct"].rolling(ROLLING_SHORT, min_periods=2).mean()
    out["body_20d_avg_pct"] = out["body_pct"].rolling(ROLLING_MEDIUM, min_periods=5).mean()

    out["body_eff_5d_avg_pct"] = out["body_efficiency_pct"].rolling(ROLLING_SHORT, min_periods=2).mean()
    out["body_eff_20d_avg_pct"] = out["body_efficiency_pct"].rolling(ROLLING_MEDIUM, min_periods=5).mean()

    # Buckets for monthly distribution.
    out["range_bucket"] = pd.cut(
        out["high_low_range_pct_close"],
        bins=RANGE_BUCKETS,
        labels=RANGE_BUCKET_LABELS,
    )

    out["body_bucket"] = pd.cut(
        out["body_pct"],
        bins=BODY_BUCKETS,
        labels=BODY_BUCKET_LABELS,
    )

    # Clean impossible rows defensively.
    out = out[
        (out["open"] > 0)
        & (out["high"] > 0)
        & (out["low"] > 0)
        & (out["close"] > 0)
        & (out["high"] >= out["low"])
        & (out["high_low_range_abs"] >= 0)
    ].copy()

    return out


# ============================================================
# SUMMARY TABLES
# ============================================================

def pct_true(s: pd.Series) -> float:
    """Return percentage of True values in a boolean series."""
    if len(s) == 0:
        return 0.0
    return float(s.mean() * 100.0)


def make_month_summary(daily: pd.DataFrame) -> pd.DataFrame:
    """Create month-wise movement summary."""
    if daily.empty:
        return pd.DataFrame()

    grouped = daily.groupby("year_month", sort=True)

    rows: List[Dict[str, Any]] = []

    for ym, g in grouped:
        g = g.sort_values("trade_date").copy()

        first = g.iloc[0]
        last = g.iloc[-1]

        month_open = float(first["open"])
        month_close = float(last["close"])
        month_high = float(g["high"].max())
        month_low = float(g["low"].min())

        month_ret_pct = ((month_close / month_open) - 1.0) * 100.0 if month_open > 0 else 0.0
        month_hl_range_pct = ((month_high / month_low) - 1.0) * 100.0 if month_low > 0 else 0.0

        rows.append(
            {
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

                "month_return_pct_open_to_close": month_ret_pct,
                "month_high_low_range_pct": month_hl_range_pct,

                "green_day_pct": pct_true(g["day_type"] == "GREEN"),
                "red_day_pct": pct_true(g["day_type"] == "RED"),
                "doji_flat_day_pct": pct_true(g["day_type"] == "DOJI_OR_FLAT"),

                "gap_up_day_pct": pct_true(g["gap_direction"] == "GAP_UP"),
                "gap_down_day_pct": pct_true(g["gap_direction"] == "GAP_DOWN"),

                "avg_gap_pct": float(g["gap_pct"].mean()),
                "median_gap_pct": float(g["gap_pct"].median()),
                "avg_abs_gap_pct": float(g["gap_pct"].abs().mean()),

                "avg_hl_range_pct": float(g["high_low_range_pct_close"].mean()),
                "median_hl_range_pct": float(g["high_low_range_pct_close"].median()),
                "p75_hl_range_pct": float(g["high_low_range_pct_close"].quantile(0.75)),
                "p90_hl_range_pct": float(g["high_low_range_pct_close"].quantile(0.90)),

                "avg_body_pct": float(g["body_pct"].mean()),
                "median_body_pct": float(g["body_pct"].median()),
                "p75_body_pct": float(g["body_pct"].quantile(0.75)),
                "p90_body_pct": float(g["body_pct"].quantile(0.90)),

                "avg_body_signed_pct": float(g["body_signed_pct"].mean()),
                "median_body_signed_pct": float(g["body_signed_pct"].median()),

                "avg_body_efficiency_pct": float(g["body_efficiency_pct"].mean()),
                "median_body_efficiency_pct": float(g["body_efficiency_pct"].median()),
                "p75_body_efficiency_pct": float(g["body_efficiency_pct"].quantile(0.75)),

                "avg_close_location_pct": float(g["close_location_pct"].mean()),
                "median_close_location_pct": float(g["close_location_pct"].median()),

                "strong_range_day_pct_hl_ge_3": pct_true(g["strong_range_day"]),
                "very_large_range_day_pct_hl_ge_5": pct_true(g["very_large_range_day"]),
                "strong_directional_day_pct": pct_true(g["strong_directional_day"]),

                "avg_true_range_pct": float(g["true_range_pct"].mean()),
                "median_true_range_pct": float(g["true_range_pct"].median()),
                "avg_atr14_pct": float(g["atr14_pct"].mean()),

                "avg_volume": float(g["volume"].mean()),
                "median_volume": float(g["volume"].median()),
                "total_volume": float(g["volume"].sum()),

                "avg_traded_value_cr": float(g["traded_value_cr"].mean()),
                "median_traded_value_cr": float(g["traded_value_cr"].median()),
                "p25_traded_value_cr": float(g["traded_value_cr"].quantile(0.25)),
                "total_traded_value_cr": float(g["traded_value_cr"].sum()),

                "best_day_return_pct": float(g["close_to_close_ret_pct"].max()),
                "worst_day_return_pct": float(g["close_to_close_ret_pct"].min()),
                "avg_close_to_close_ret_pct": float(g["close_to_close_ret_pct"].mean()),
                "std_close_to_close_ret_pct": float(g["close_to_close_ret_pct"].std(ddof=0)),
            }
        )

    out = pd.DataFrame(rows)

    if not out.empty:
        out["movement_quality_score"] = (
            0.30 * out["median_hl_range_pct"].rank(pct=True)
            + 0.25 * out["median_body_pct"].rank(pct=True)
            + 0.20 * out["median_body_efficiency_pct"].rank(pct=True)
            + 0.15 * out["strong_directional_day_pct"].rank(pct=True)
            + 0.10 * out["avg_traded_value_cr"].rank(pct=True)
        ) * 100.0

        out = out.sort_values("year_month").reset_index(drop=True)

    return out


def make_yearly_summary(daily: pd.DataFrame) -> pd.DataFrame:
    """Create year-wise movement summary."""
    if daily.empty:
        return pd.DataFrame()

    grouped = daily.groupby("year", sort=True)

    rows: List[Dict[str, Any]] = []

    for yr, g in grouped:
        g = g.sort_values("trade_date").copy()

        first = g.iloc[0]
        last = g.iloc[-1]

        year_open = float(first["open"])
        year_close = float(last["close"])
        year_high = float(g["high"].max())
        year_low = float(g["low"].min())

        rows.append(
            {
                "year": int(yr),
                "trading_days": int(len(g)),
                "first_date": first["trade_date"],
                "last_date": last["trade_date"],
                "year_open": year_open,
                "year_high": year_high,
                "year_low": year_low,
                "year_close": year_close,
                "year_return_pct_open_to_close": ((year_close / year_open) - 1.0) * 100.0 if year_open > 0 else 0.0,
                "year_high_low_range_pct": ((year_high / year_low) - 1.0) * 100.0 if year_low > 0 else 0.0,
                "median_hl_range_pct": float(g["high_low_range_pct_close"].median()),
                "median_body_pct": float(g["body_pct"].median()),
                "median_body_efficiency_pct": float(g["body_efficiency_pct"].median()),
                "strong_directional_day_pct": pct_true(g["strong_directional_day"]),
                "strong_range_day_pct_hl_ge_3": pct_true(g["strong_range_day"]),
                "very_large_range_day_pct_hl_ge_5": pct_true(g["very_large_range_day"]),
                "avg_traded_value_cr": float(g["traded_value_cr"].mean()),
                "median_traded_value_cr": float(g["traded_value_cr"].median()),
                "total_traded_value_cr": float(g["traded_value_cr"].sum()),
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

    rows = []

    for wd in weekday_order:
        g = daily[daily["weekday"] == wd].copy()

        if g.empty:
            continue

        rows.append(
            {
                "weekday": wd,
                "trading_days": int(len(g)),
                "median_hl_range_pct": float(g["high_low_range_pct_close"].median()),
                "avg_hl_range_pct": float(g["high_low_range_pct_close"].mean()),
                "median_body_pct": float(g["body_pct"].median()),
                "avg_body_pct": float(g["body_pct"].mean()),
                "median_body_efficiency_pct": float(g["body_efficiency_pct"].median()),
                "avg_body_efficiency_pct": float(g["body_efficiency_pct"].mean()),
                "strong_directional_day_pct": pct_true(g["strong_directional_day"]),
                "strong_range_day_pct_hl_ge_3": pct_true(g["strong_range_day"]),
                "very_large_range_day_pct_hl_ge_5": pct_true(g["very_large_range_day"]),
                "green_day_pct": pct_true(g["day_type"] == "GREEN"),
                "red_day_pct": pct_true(g["day_type"] == "RED"),
                "avg_traded_value_cr": float(g["traded_value_cr"].mean()),
                "median_traded_value_cr": float(g["traded_value_cr"].median()),
            }
        )

    return pd.DataFrame(rows)


def make_month_bucket_distribution(daily: pd.DataFrame, bucket_col: str) -> pd.DataFrame:
    """
    Create monthly bucket distribution for range/body buckets.

    bucket_col:
        "range_bucket" or "body_bucket"
    """
    if daily.empty:
        return pd.DataFrame()

    counts = (
        daily.groupby(["year_month", bucket_col], observed=False)
        .size()
        .reset_index(name="days")
    )

    pivot = counts.pivot(index="year_month", columns=bucket_col, values="days").fillna(0).reset_index()

    return pivot


# ============================================================
# EXCEL OUTPUT
# ============================================================

def autosize_excel_columns(writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame) -> None:
    """Auto-size Excel columns."""
    worksheet = writer.sheets[sheet_name]

    for idx, col in enumerate(df.columns):
        series = df[col].astype(str) if not df.empty else pd.Series([], dtype=str)
        max_len = max([len(str(col))] + [len(x) for x in series.head(500).tolist()])
        worksheet.set_column(idx, idx, min(max_len + 2, 45))


def add_excel_charts(writer: pd.ExcelWriter, month_summary: pd.DataFrame) -> None:
    """
    Add simple charts to Excel.

    Charts are deliberately basic and use month_summary.
    """
    if month_summary.empty:
        return

    workbook = writer.book

    if "month_summary" not in writer.sheets:
        return

    ws = writer.sheets["month_summary"]

    # Find columns by name.
    col_map = {name: idx for idx, name in enumerate(month_summary.columns)}

    required_cols = [
        "year_month",
        "median_hl_range_pct",
        "median_body_pct",
        "median_body_efficiency_pct",
        "avg_traded_value_cr",
    ]

    if not all(c in col_map for c in required_cols):
        return

    nrows = len(month_summary)

    # Chart 1: median high-low range and body.
    chart1 = workbook.add_chart({"type": "line"})
    chart1.add_series(
        {
            "name": "Median HL Range %",
            "categories": ["month_summary", 1, col_map["year_month"], nrows, col_map["year_month"]],
            "values": ["month_summary", 1, col_map["median_hl_range_pct"], nrows, col_map["median_hl_range_pct"]],
        }
    )
    chart1.add_series(
        {
            "name": "Median Body %",
            "categories": ["month_summary", 1, col_map["year_month"], nrows, col_map["year_month"]],
            "values": ["month_summary", 1, col_map["median_body_pct"], nrows, col_map["median_body_pct"]],
        }
    )
    chart1.set_title({"name": f"{SYMBOL}: Monthly Movement"})
    chart1.set_x_axis({"name": "Month"})
    chart1.set_y_axis({"name": "%"})
    chart1.set_legend({"position": "bottom"})

    ws.insert_chart("BI2", chart1, {"x_scale": 1.5, "y_scale": 1.2})

    # Chart 2: body efficiency.
    chart2 = workbook.add_chart({"type": "line"})
    chart2.add_series(
        {
            "name": "Median Body Efficiency %",
            "categories": ["month_summary", 1, col_map["year_month"], nrows, col_map["year_month"]],
            "values": ["month_summary", 1, col_map["median_body_efficiency_pct"], nrows, col_map["median_body_efficiency_pct"]],
        }
    )
    chart2.set_title({"name": f"{SYMBOL}: Monthly Body Efficiency"})
    chart2.set_x_axis({"name": "Month"})
    chart2.set_y_axis({"name": "%"})
    chart2.set_legend({"position": "bottom"})

    ws.insert_chart("BI25", chart2, {"x_scale": 1.5, "y_scale": 1.2})

    # Chart 3: average traded value.
    chart3 = workbook.add_chart({"type": "column"})
    chart3.add_series(
        {
            "name": "Avg Traded Value Cr",
            "categories": ["month_summary", 1, col_map["year_month"], nrows, col_map["year_month"]],
            "values": ["month_summary", 1, col_map["avg_traded_value_cr"], nrows, col_map["avg_traded_value_cr"]],
        }
    )
    chart3.set_title({"name": f"{SYMBOL}: Monthly Avg Traded Value"})
    chart3.set_x_axis({"name": "Month"})
    chart3.set_y_axis({"name": "₹ Cr"})
    chart3.set_legend({"position": "bottom"})

    ws.insert_chart("BI48", chart3, {"x_scale": 1.5, "y_scale": 1.2})


def write_excel_report(
    daily: pd.DataFrame,
    month_summary: pd.DataFrame,
    yearly_summary: pd.DataFrame,
    weekday_summary: pd.DataFrame,
    month_range_distribution: pd.DataFrame,
    month_body_distribution: pd.DataFrame,
    config_df: pd.DataFrame,
    output_path: str,
) -> None:
    """Write all analysis sheets to Excel."""
    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        daily.to_excel(writer, index=False, sheet_name="daily_analysis")
        month_summary.to_excel(writer, index=False, sheet_name="month_summary")
        yearly_summary.to_excel(writer, index=False, sheet_name="yearly_summary")
        weekday_summary.to_excel(writer, index=False, sheet_name="weekday_summary")
        month_range_distribution.to_excel(writer, index=False, sheet_name="month_range_distribution")
        month_body_distribution.to_excel(writer, index=False, sheet_name="month_body_distribution")
        config_df.to_excel(writer, index=False, sheet_name="config")

        sheets = {
            "daily_analysis": daily,
            "month_summary": month_summary,
            "yearly_summary": yearly_summary,
            "weekday_summary": weekday_summary,
            "month_range_distribution": month_range_distribution,
            "month_body_distribution": month_body_distribution,
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
                elif "days" in lower or "volume" == lower:
                    ws.set_column(idx, idx, 14, fmt_int)
                elif (
                    "pct" in lower
                    or "abs" in lower
                    or "cr" in lower
                    or "price" in lower
                    or lower in {"open", "high", "low", "close"}
                ):
                    ws.set_column(idx, idx, 16, fmt_num)

        add_excel_charts(writer, month_summary)


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    """Script entrypoint."""
    print("============================================================")
    print(f"{SYMBOL} 3-YEAR DAILY MOVEMENT ANALYZER")
    print("============================================================")

    from_date, to_date = compute_date_range()
    analysis_start = to_date - timedelta(days=365 * LOOKBACK_YEARS)

    print(f"[CONFIG] Symbol         : {SYMBOL}")
    print(f"[CONFIG] Exchange       : {EXCHANGE}")
    print(f"[CONFIG] Lookback years : {LOOKBACK_YEARS}")
    print(f"[CONFIG] Download from  : {from_date}")
    print(f"[CONFIG] Analysis from  : {analysis_start}")
    print(f"[CONFIG] To date        : {to_date}")
    print(f"[CONFIG] Output Excel   : {OUTPUT_EXCEL}")
    print(f"[CONFIG] Force refresh  : {FORCE_REFRESH}")
    print("============================================================")

    print("[STEP] Initializing Kite API ...")
    kite = initialize_kite()
    print("[OK] Kite initialized.")

    print("[STEP] Resolving instrument token ...")
    inst = resolve_equity_token(kite, SYMBOL, EXCHANGE)
    print(f"[OK] {inst.exchange}:{inst.symbol} token={inst.instrument_token} name={inst.name}")

    raw_df = load_cached_if_usable(SYMBOL, from_date, to_date)

    if raw_df is None:
        print("[STEP] Downloading daily history ...")
        rows = fetch_history_day(
            kite=kite,
            instrument_token=inst.instrument_token,
            from_date=from_date,
            to_date=to_date,
            label=f"{EXCHANGE}:{SYMBOL}",
        )

        raw_df = rows_to_dataframe(rows)

        if raw_df.empty:
            raise RuntimeError(f"No daily candles returned for {SYMBOL}")

        save_cache(SYMBOL, raw_df)

    # Restrict to download window.
    raw_df["trade_date"] = pd.to_datetime(raw_df["trade_date"]).dt.date
    raw_df = raw_df[
        (raw_df["trade_date"] >= from_date)
        & (raw_df["trade_date"] <= to_date)
    ].copy()

    print(f"[INFO] Raw daily candles: {len(raw_df)}")

    print("[STEP] Calculating daily movement features ...")
    daily = add_daily_movement_features(
        df=raw_df,
        symbol=SYMBOL,
        instrument_token=inst.instrument_token,
    )

    # Final analysis period: exact 3 years, but previous rows were used for prev_close/rolling features.
    daily["trade_date"] = pd.to_datetime(daily["trade_date"])
    daily = daily[daily["trade_date"].dt.date >= analysis_start].copy()
    daily = daily.sort_values("trade_date").reset_index(drop=True)

    print(f"[INFO] Analysis daily rows: {len(daily)}")
    print(f"[INFO] Date range: {daily['trade_date'].min().date()} -> {daily['trade_date'].max().date()}")

    print("[STEP] Building month-wise summary ...")
    month_summary = make_month_summary(daily)

    print("[STEP] Building yearly summary ...")
    yearly_summary = make_yearly_summary(daily)

    print("[STEP] Building weekday summary ...")
    weekday_summary = make_weekday_summary(daily)

    print("[STEP] Building bucket distributions ...")
    month_range_distribution = make_month_bucket_distribution(daily, "range_bucket")
    month_body_distribution = make_month_bucket_distribution(daily, "body_bucket")

    config_df = pd.DataFrame(
        [
            {"parameter": "SYMBOL", "value": SYMBOL},
            {"parameter": "EXCHANGE", "value": EXCHANGE},
            {"parameter": "INSTRUMENT_TOKEN", "value": inst.instrument_token},
            {"parameter": "INSTRUMENT_NAME", "value": inst.name},
            {"parameter": "LOOKBACK_YEARS", "value": LOOKBACK_YEARS},
            {"parameter": "DOWNLOAD_FROM", "value": str(from_date)},
            {"parameter": "ANALYSIS_FROM", "value": str(analysis_start)},
            {"parameter": "TO_DATE", "value": str(to_date)},
            {"parameter": "OUTPUT_EXCEL", "value": OUTPUT_EXCEL},
            {"parameter": "FORCE_REFRESH", "value": FORCE_REFRESH},
            {"parameter": "HIGH_LOW_RANGE_PCT_CLOSE", "value": "(high-low)/close*100"},
            {"parameter": "HIGH_LOW_RANGE_PCT_LOW", "value": "(high/low-1)*100"},
            {"parameter": "BODY_PCT", "value": "abs(close-open)/open*100"},
            {"parameter": "BODY_SIGNED_PCT", "value": "(close-open)/open*100"},
            {"parameter": "BODY_EFFICIENCY_PCT", "value": "abs(close-open)/(high-low)*100"},
            {"parameter": "CLOSE_LOCATION_PCT", "value": "(close-low)/(high-low)*100"},
            {"parameter": "TRADED_VALUE_CR", "value": "((high+low+close)/3)*volume/1e7"},
            {"parameter": "RANGE_BUCKETS", "value": ", ".join(RANGE_BUCKET_LABELS)},
            {"parameter": "BODY_BUCKETS", "value": ", ".join(BODY_BUCKET_LABELS)},
        ]
    )

    print("[STEP] Writing Excel report ...")
    write_excel_report(
        daily=daily,
        month_summary=month_summary,
        yearly_summary=yearly_summary,
        weekday_summary=weekday_summary,
        month_range_distribution=month_range_distribution,
        month_body_distribution=month_body_distribution,
        config_df=config_df,
        output_path=OUTPUT_EXCEL,
    )

    print("\n==================== SUMMARY ====================")

    if not daily.empty:
        print(f"Trading days analyzed      : {len(daily)}")
        print(f"Median daily HL range %    : {daily['high_low_range_pct_close'].median():.2f}%")
        print(f"Median daily body %        : {daily['body_pct'].median():.2f}%")
        print(f"Median body efficiency %   : {daily['body_efficiency_pct'].median():.2f}%")
        print(f"Days with HL range >= 3%   : {pct_true(daily['strong_range_day']):.2f}%")
        print(f"Days with HL range >= 5%   : {pct_true(daily['very_large_range_day']):.2f}%")
        print(f"Strong directional days    : {pct_true(daily['strong_directional_day']):.2f}%")
        print(f"Avg traded value / day     : ₹{daily['traded_value_cr'].mean():.2f} Cr")
        print(f"Median traded value / day  : ₹{daily['traded_value_cr'].median():.2f} Cr")

    if not month_summary.empty:
        best_month = month_summary.sort_values("movement_quality_score", ascending=False).iloc[0]
        print("\nBest movement-quality month:")
        print(f"Month                     : {best_month['year_month']}")
        print(f"Movement quality score    : {best_month['movement_quality_score']:.2f}")
        print(f"Median HL range %         : {best_month['median_hl_range_pct']:.2f}%")
        print(f"Median body %             : {best_month['median_body_pct']:.2f}%")
        print(f"Median body efficiency %  : {best_month['median_body_efficiency_pct']:.2f}%")
        print(f"Avg traded value          : ₹{best_month['avg_traded_value_cr']:.2f} Cr")

    print(f"\n[DONE] Excel saved: {OUTPUT_EXCEL}")
    print("=================================================")


if __name__ == "__main__":
    main()