#!/usr/bin/env python3
"""
live_5_stock_insight_scanner.py

Purpose
-------
Live scanner for the 5 shortlisted stocks:

    BSE
    FORCEMOT
    KAYNES
    MCX
    IDEA

This script gives live intraday insights only. It does NOT place orders.

It scans:
    - LTP
    - % change from previous close
    - Today's high-low range %
    - Today's open-current body %
    - Body efficiency %
    - VWAP and distance from VWAP
    - 15-minute opening range breakout status
    - EMA 9 / EMA 21 trend
    - Last 5-minute and 15-minute movement
    - CPR / pivot position
    - Volume pace versus previous day's volume
    - A rough comparative live momentum score

Important fix in this version
-----------------------------
The previous version crashed with:

    AttributeError: Can only use .dt accessor with datetimelike values

Reason:
    Pandas sometimes keeps mixed timezone-aware / timezone-naive timestamps
    as object dtype. Then .dt.date fails.

Fix:
    All timestamps are explicitly normalized into timezone-naive IST pandas
    datetime64[ns] before .dt is used.

Data source
-----------
Uses Zerodha Kite:

    1. historical_data() for today's 1-minute candles
    2. historical_data() for previous trading day's daily candle
    3. quote() for live polling

Dependency
----------
Uses your existing Kite login helper:

    Trading_2024.OptionTradeUtils.intialize_kite_api()

Fallback:

    OptionTradeUtils.intialize_kite_api()

Install dependencies if needed:

    pip install pandas numpy kiteconnect

Run
---
    python live_5_stock_insight_scanner.py

Optional:

    python live_5_stock_insight_scanner.py --refresh-sec 5

    python live_5_stock_insight_scanner.py --symbols BSE,FORCEMOT,KAYNES,MCX,IDEA

    python live_5_stock_insight_scanner.py --export-csv live_5_stock_scan.csv

    python live_5_stock_insight_scanner.py --no-clear

Notes
-----
- The live current-minute candle is approximate between full historical refreshes.
- The script corrects candles periodically using Kite historical_data().
- The live_score is a comparative scanner score, not a buy/sell signal.
"""

from __future__ import annotations

import argparse
import math
import os
import time
import traceback
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta
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
# USER CONFIG
# ============================================================

EXCHANGE = "NSE"

DEFAULT_SYMBOLS = ["BSE", "FORCEMOT", "KAYNES", "MCX", "IDEA"]

SESSION_START = dtime(9, 15)
SESSION_END = dtime(15, 30)

# Opening range window:
# 09:15 to 09:29:59 = first 15 minutes.
OPENING_RANGE_MINUTES = 15

# Quote refresh interval.
DEFAULT_REFRESH_SEC = 5

# Full 1-minute historical correction interval.
# Quote polling is live but approximate. Historical refresh cleans the data.
DEFAULT_HIST_REFRESH_SEC = 60

# EMA periods for live trend reading.
EMA_FAST = 9
EMA_SLOW = 21

# ATR period on 1-minute candles.
ATR_PERIOD = 14


# ============================================================
# DATA CLASSES
# ============================================================

@dataclass
class StockMeta:
    """Static metadata for one stock."""

    symbol: str
    exchange: str
    kite_key: str
    instrument_token: int
    name: str


@dataclass
class PreviousDayLevels:
    """Previous trading day OHLC and classic CPR/pivot levels."""

    source_day: date
    high: float
    low: float
    close: float
    volume: int

    p: float
    bc: float
    tc: float
    r1: float
    s1: float
    r2: float
    s2: float


@dataclass
class LiveState:
    """
    Runtime state.

    candles:
        Symbol -> today's 1-minute OHLCV dataframe.

    last_cum_volume:
        Symbol -> last seen cumulative volume from Kite quote().
        Used to estimate current-minute volume delta.

    last_quote:
        Symbol -> latest Kite quote payload.

    last_hist_refresh_ts:
        UNIX timestamp of last historical refresh.
    """

    candles: Dict[str, pd.DataFrame]
    last_cum_volume: Dict[str, int]
    last_quote: Dict[str, Dict[str, Any]]
    last_hist_refresh_ts: float


# ============================================================
# ARGUMENTS
# ============================================================

def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Live insight scanner for shortlisted NSE stocks."
    )

    parser.add_argument(
        "--symbols",
        default=",".join(DEFAULT_SYMBOLS),
        help="Comma-separated NSE symbols. Default: BSE,FORCEMOT,KAYNES,MCX,IDEA",
    )

    parser.add_argument(
        "--refresh-sec",
        type=float,
        default=DEFAULT_REFRESH_SEC,
        help="Quote refresh interval in seconds. Default: 5",
    )

    parser.add_argument(
        "--hist-refresh-sec",
        type=float,
        default=DEFAULT_HIST_REFRESH_SEC,
        help="Full historical 1-minute correction interval in seconds. Default: 60",
    )

    parser.add_argument(
        "--export-csv",
        default="live_5_stock_scan.csv",
        help="CSV file where the latest scan table is repeatedly saved.",
    )

    parser.add_argument(
        "--no-clear",
        action="store_true",
        help="Do not clear terminal between refreshes.",
    )

    return parser.parse_args()


# ============================================================
# TIME HELPERS
# ============================================================

def now_ist() -> datetime:
    """
    Return timezone-aware current datetime in IST.

    If zoneinfo is unavailable, falls back to system local time.
    """
    if ZoneInfo is not None:
        return datetime.now(ZoneInfo("Asia/Kolkata"))
    return datetime.now()


def today_ist() -> date:
    """Return today's date in IST."""
    return now_ist().date()


def session_start_dt(day: date) -> datetime:
    """
    Return session start datetime as timezone-naive IST clock time.

    Kite historical_data generally accepts naive Indian-market datetimes.
    """
    return datetime.combine(day, SESSION_START)


def session_end_dt(day: date) -> datetime:
    """Return session end datetime as timezone-naive IST clock time."""
    return datetime.combine(day, SESSION_END)


def is_market_session_now() -> bool:
    """Return True if current IST time is inside regular NSE cash-market session."""
    t = now_ist().time()
    return SESSION_START <= t <= SESSION_END


def normalize_kite_datetime(value: Any) -> pd.Timestamp:
    """
    Normalize Kite/Pandas/Python timestamp into timezone-naive IST pandas Timestamp.

    Why timezone-naive IST?
    -----------------------
    Pandas can create object dtype when a column has mixed:
        - timezone-aware timestamps
        - timezone-naive timestamps
        - Python datetime objects
        - pandas Timestamp objects

    That object dtype breaks .dt accessors.

    This function converts every valid timestamp to:
        pandas Timestamp, timezone-naive, Asia/Kolkata clock time

    Example:
        2026-06-04 10:15:00+05:30  -> 2026-06-04 10:15:00
    """
    try:
        ts = pd.Timestamp(value)

        if pd.isna(ts):
            return pd.NaT

        # If timestamp is timezone-aware, convert to IST and remove timezone.
        if ts.tzinfo is not None:
            ts = ts.tz_convert("Asia/Kolkata").tz_localize(None)

        # If timestamp is timezone-naive, treat it as already IST/local.
        return pd.Timestamp(ts)

    except Exception:
        return pd.NaT


# ============================================================
# KITE INITIALIZATION AND INSTRUMENTS
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


def load_nse_equity_instruments(kite) -> Dict[str, Dict[str, Any]]:
    """
    Load NSE instrument dump and retain only EQ instruments.

    Returns:
        symbol -> instrument row
    """
    print("[STEP] Loading NSE instruments dump ...")
    rows = kite.instruments(EXCHANGE)
    print(f"[INFO] NSE instrument rows received: {len(rows):,}")

    out: Dict[str, Dict[str, Any]] = {}

    for row in rows:
        symbol = str(row.get("tradingsymbol", "")).strip().upper()
        segment = str(row.get("segment", "")).strip().upper()
        instrument_type = str(row.get("instrument_type", "")).strip().upper()
        exchange = str(row.get("exchange", "")).strip().upper()

        if not symbol:
            continue

        if exchange != EXCHANGE:
            continue

        if segment != EXCHANGE:
            continue

        if instrument_type != "EQ":
            continue

        out[symbol] = row

    print(f"[INFO] NSE EQ symbols retained: {len(out):,}")
    return out


def resolve_symbols(symbols: List[str], equity_map: Dict[str, Dict[str, Any]]) -> List[StockMeta]:
    """Resolve requested symbols to Kite instrument tokens."""
    metas: List[StockMeta] = []

    for symbol in symbols:
        s = symbol.strip().upper()
        if not s:
            continue

        row = equity_map.get(s)

        if row is None:
            raise ValueError(f"Symbol not found as NSE EQ instrument: {s}")

        metas.append(
            StockMeta(
                symbol=s,
                exchange=EXCHANGE,
                kite_key=f"{EXCHANGE}:{s}",
                instrument_token=int(row["instrument_token"]),
                name=str(row.get("name", s)),
            )
        )

    if not metas:
        raise ValueError("No symbols resolved.")

    return metas


# ============================================================
# OHLCV CLEANING AND HISTORICAL DATA
# ============================================================

def empty_ohlcv_df() -> pd.DataFrame:
    """Return an empty OHLCV dataframe with standard columns."""
    return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])


def clean_ohlcv_df(df: pd.DataFrame, only_today: Optional[date] = None) -> pd.DataFrame:
    """
    Clean OHLCV dataframe returned by Kite historical_data() or built from live quote rows.

    Expected columns:
        date, open, high, low, close, volume

    Important fix:
    --------------
    The date column is explicitly rebuilt as datetime64[ns] after element-wise
    timestamp normalization. This prevents the pandas error:

        AttributeError: Can only use .dt accessor with datetimelike values
    """
    if df is None or df.empty:
        return empty_ohlcv_df()

    d = df.copy()

    if "date" not in d.columns:
        return empty_ohlcv_df()

    # Robust timestamp normalization.
    # Do not rely on plain apply() alone because it can leave object dtype.
    normalized_dates = [normalize_kite_datetime(x) for x in d["date"].tolist()]
    d["date"] = pd.to_datetime(normalized_dates, errors="coerce")

    # Remove bad timestamps before using .dt.
    d = d.dropna(subset=["date"]).copy()

    if d.empty:
        return empty_ohlcv_df()

    # Convert OHLC columns.
    for col in ["open", "high", "low", "close"]:
        if col not in d.columns:
            d[col] = np.nan
        d[col] = pd.to_numeric(d[col], errors="coerce")

    # Convert volume.
    if "volume" not in d.columns:
        d["volume"] = 0

    d["volume"] = pd.to_numeric(d["volume"], errors="coerce").fillna(0).astype(float)

    # Remove rows with invalid OHLC.
    d = d.dropna(subset=["open", "high", "low", "close"]).copy()

    if d.empty:
        return empty_ohlcv_df()

    # Now .dt is safe because date is guaranteed datetime64[ns].
    if only_today is not None:
        d = d[d["date"].dt.date == only_today].copy()

    if d.empty:
        return empty_ohlcv_df()

    # Floor to minute and remove duplicate minute rows.
    d["date"] = d["date"].dt.floor("min")

    d = (
        d.drop_duplicates(subset=["date"], keep="last")
        .sort_values("date")
        .reset_index(drop=True)
    )

    return d[["date", "open", "high", "low", "close", "volume"]]


def fetch_today_1m(kite, meta: StockMeta) -> pd.DataFrame:
    """
    Fetch today's 1-minute candles from market open to now.

    Used:
        1. at startup;
        2. periodically to correct quote-derived live candle approximations.
    """
    day = today_ist()

    start = session_start_dt(day)
    end = now_ist().replace(tzinfo=None)

    # Clamp end to regular session end if script runs after close.
    end = min(end, session_end_dt(day))

    if end <= start:
        return empty_ohlcv_df()

    rows = kite.historical_data(
        instrument_token=meta.instrument_token,
        from_date=start,
        to_date=end,
        interval="minute",
        continuous=False,
        oi=False,
    )

    if not rows:
        return empty_ohlcv_df()

    return clean_ohlcv_df(pd.DataFrame(rows), only_today=day)


def merge_candles(old: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    """
    Merge old and new candle frames.

    Last row per minute is retained. This allows historical correction to
    overwrite approximate quote-derived rows.
    """
    parts = []

    if old is not None and not old.empty:
        parts.append(old)

    if new is not None and not new.empty:
        parts.append(new)

    if not parts:
        return empty_ohlcv_df()

    merged = pd.concat(parts, ignore_index=True)
    merged = clean_ohlcv_df(merged, only_today=today_ist())

    return merged


def fetch_previous_day_levels(kite, meta: StockMeta) -> Optional[PreviousDayLevels]:
    """
    Fetch previous trading day's daily candle and compute CPR/pivot levels.

    CPR:
        P  = (H + L + C) / 3
        BC = (H + L) / 2
        TC = 2P - BC

    Pivots:
        R1 = 2P - L
        S1 = 2P - H
        R2 = P + (H - L)
        S2 = P - (H - L)
    """
    today = today_ist()
    from_date = today - timedelta(days=20)
    to_date = today

    rows = kite.historical_data(
        instrument_token=meta.instrument_token,
        from_date=from_date,
        to_date=to_date,
        interval="day",
        continuous=False,
        oi=False,
    )

    if not rows:
        return None

    df = pd.DataFrame(rows)

    if df.empty:
        return None

    df["date"] = pd.to_datetime([normalize_kite_datetime(x) for x in df["date"].tolist()], errors="coerce")
    df = df.dropna(subset=["date"]).copy()
    df["trade_date"] = df["date"].dt.date

    # Keep only completed days before today.
    df = df[df["trade_date"] < today].copy()

    if df.empty:
        return None

    last = df.sort_values("trade_date").iloc[-1]

    h = float(last["high"])
    l = float(last["low"])
    c = float(last["close"])
    v = int(float(last.get("volume", 0) or 0))

    p = (h + l + c) / 3.0
    bc = (h + l) / 2.0
    tc = 2.0 * p - bc
    r1 = 2.0 * p - l
    s1 = 2.0 * p - h
    r2 = p + (h - l)
    s2 = p - (h - l)

    return PreviousDayLevels(
        source_day=last["trade_date"],
        high=h,
        low=l,
        close=c,
        volume=v,
        p=p,
        bc=bc,
        tc=tc,
        r1=r1,
        s1=s1,
        r2=r2,
        s2=s2,
    )


# ============================================================
# LIVE QUOTE HANDLING
# ============================================================

def quote_all(kite, metas: List[StockMeta]) -> Dict[str, Dict[str, Any]]:
    """Fetch quotes for all tracked stocks in one Kite quote() call."""
    keys = [m.kite_key for m in metas]
    return kite.quote(keys)


def safe_float(value: Any, default: float = 0.0) -> float:
    """Safely convert to float."""
    try:
        if value is None:
            return default
        v = float(value)
        if math.isnan(v):
            return default
        return v
    except Exception:
        return default


def safe_int(value: Any, default: int = 0) -> int:
    """Safely convert to int."""
    try:
        if value is None:
            return default
        return int(float(value))
    except Exception:
        return default


def update_current_minute_from_quote(state: LiveState, meta: StockMeta, quote: Dict[str, Any]) -> None:
    """
    Update/append current 1-minute candle using live quote.

    Kite quote() gives:
        - latest LTP
        - cumulative day volume

    It does not give every tick. Therefore, current minute candle is approximate
    between full historical refreshes.
    """
    symbol = meta.symbol

    ltp = safe_float(quote.get("last_price"), 0.0)
    if ltp <= 0:
        return

    cum_volume = safe_int(quote.get("volume"), 0)
    prev_cum_volume = state.last_cum_volume.get(symbol)

    if prev_cum_volume is None:
        volume_delta = 0
    else:
        volume_delta = max(0, cum_volume - prev_cum_volume)

    state.last_cum_volume[symbol] = cum_volume
    state.last_quote[symbol] = quote

    # Use quote timestamp if available, else current time.
    q_ts_raw = quote.get("timestamp") or quote.get("last_trade_time") or now_ist()
    q_ts = normalize_kite_datetime(q_ts_raw)

    # Important safety check:
    # Avoid calling .date() on NaT.
    if pd.isna(q_ts) or q_ts.date() != today_ist():
        q_ts = pd.Timestamp(now_ist().replace(tzinfo=None))

    minute_ts = q_ts.floor("min")

    df = state.candles.get(symbol)

    if df is None or df.empty:
        state.candles[symbol] = pd.DataFrame(
            [
                {
                    "date": minute_ts,
                    "open": ltp,
                    "high": ltp,
                    "low": ltp,
                    "close": ltp,
                    "volume": volume_delta,
                }
            ]
        )
        return

    df = clean_ohlcv_df(df, only_today=today_ist())

    match = df["date"] == minute_ts

    if match.any():
        idx = df.index[match][-1]
        df.loc[idx, "high"] = max(float(df.loc[idx, "high"]), ltp)
        df.loc[idx, "low"] = min(float(df.loc[idx, "low"]), ltp)
        df.loc[idx, "close"] = ltp
        df.loc[idx, "volume"] = float(df.loc[idx, "volume"]) + float(volume_delta)
    else:
        new_row = pd.DataFrame(
            [
                {
                    "date": minute_ts,
                    "open": ltp,
                    "high": ltp,
                    "low": ltp,
                    "close": ltp,
                    "volume": volume_delta,
                }
            ]
        )
        df = pd.concat([df, new_row], ignore_index=True)

    state.candles[symbol] = clean_ohlcv_df(df, only_today=today_ist())


# ============================================================
# INDICATORS
# ============================================================

def calc_vwap(df: pd.DataFrame) -> float:
    """
    Calculate intraday VWAP from 1-minute candles.

    Uses typical price:
        (high + low + close) / 3
    """
    if df.empty or "volume" not in df.columns:
        return 0.0

    d = df.copy()
    d["typical_price"] = (d["high"] + d["low"] + d["close"]) / 3.0
    total_volume = float(d["volume"].sum())

    if total_volume <= 0:
        return 0.0

    return float((d["typical_price"] * d["volume"]).sum() / total_volume)


def calc_ema(series: pd.Series, period: int) -> pd.Series:
    """Calculate exponential moving average."""
    return series.ewm(span=period, adjust=False).mean()


def calc_atr_1m(df: pd.DataFrame, period: int = ATR_PERIOD) -> float:
    """
    Calculate approximate 1-minute ATR.

    For intraday noise reading, this helps judge whether a stop is smaller
    than normal candle noise.
    """
    if df.empty or len(df) < 3:
        return 0.0

    d = df.copy()
    d["prev_close"] = d["close"].shift(1)

    tr1 = d["high"] - d["low"]
    tr2 = (d["high"] - d["prev_close"]).abs()
    tr3 = (d["low"] - d["prev_close"]).abs()

    d["tr"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    return float(d["tr"].tail(period).mean())


def pct_change(current: float, base: float) -> float:
    """Return percentage change."""
    if base == 0:
        return 0.0
    return (current - base) * 100.0 / base


def abs_pct_change(current: float, base: float) -> float:
    """Return absolute percentage change."""
    if base == 0:
        return 0.0
    return abs(current - base) * 100.0 / base


def get_opening_range(df: pd.DataFrame) -> Tuple[float, float, float]:
    """
    Return opening-range high, low, and range %.

    Uses first OPENING_RANGE_MINUTES candles from 09:15.
    """
    if df.empty:
        return 0.0, 0.0, 0.0

    day = today_ist()
    start = pd.Timestamp(session_start_dt(day))
    end = start + pd.Timedelta(minutes=OPENING_RANGE_MINUTES)

    orb = df[(df["date"] >= start) & (df["date"] < end)].copy()

    if orb.empty:
        return 0.0, 0.0, 0.0

    high = float(orb["high"].max())
    low = float(orb["low"].min())

    range_pct = abs_pct_change(high, low) if low > 0 else 0.0

    return high, low, range_pct


def nearest_level(price: float, levels: PreviousDayLevels) -> Tuple[str, float, float]:
    """
    Find nearest pivot/CPR/previous-day level and distance percentage.
    """
    candidates = {
        "R2": levels.r2,
        "R1": levels.r1,
        "TC": levels.tc,
        "P": levels.p,
        "BC": levels.bc,
        "S1": levels.s1,
        "S2": levels.s2,
        "PDH": levels.high,
        "PDL": levels.low,
        "PDC": levels.close,
    }

    best_name = ""
    best_value = 0.0
    best_dist = 999.0

    for name, value in candidates.items():
        dist = abs_pct_change(price, value)
        if dist < best_dist:
            best_name = name
            best_value = float(value)
            best_dist = float(dist)

    return best_name, best_value, best_dist


# ============================================================
# INSIGHT CALCULATION
# ============================================================

def compute_stock_insight(
    meta: StockMeta,
    df: pd.DataFrame,
    quote: Dict[str, Any],
    levels: Optional[PreviousDayLevels],
) -> Dict[str, Any]:
    """
    Compute one row of scanner output for one stock.
    """
    symbol = meta.symbol

    df = clean_ohlcv_df(df, only_today=today_ist())

    ltp = safe_float(quote.get("last_price"), 0.0)

    if ltp <= 0 and not df.empty:
        ltp = float(df["close"].iloc[-1])

    if df.empty or ltp <= 0:
        return {
            "symbol": symbol,
            "status": "NO_DATA",
            "live_score": 0.0,
            "insight": "NO_DATA",
        }

    day_open = float(df["open"].iloc[0])
    day_high = float(max(df["high"].max(), ltp))
    day_low = float(min(df["low"].min(), ltp))
    last_close = float(df["close"].iloc[-1])

    prev_close = 0.0
    prev_day_volume = 0

    if levels is not None:
        prev_close = float(levels.close)
        prev_day_volume = int(levels.volume)
    else:
        ohlc = quote.get("ohlc") or {}
        prev_close = safe_float(ohlc.get("close"), 0.0)

    day_change_pct = pct_change(ltp, prev_close) if prev_close > 0 else 0.0

    # Same-day high-low range.
    high_low_range_abs = day_high - day_low
    high_low_range_pct = high_low_range_abs * 100.0 / ltp if ltp > 0 else 0.0

    # Same-day open-current body movement.
    body_abs = abs(ltp - day_open)
    body_pct = body_abs * 100.0 / day_open if day_open > 0 else 0.0
    body_signed_pct = pct_change(ltp, day_open) if day_open > 0 else 0.0

    # Body efficiency.
    if high_low_range_abs > 0:
        body_efficiency_pct = body_abs * 100.0 / high_low_range_abs
    else:
        body_efficiency_pct = 0.0

    body_efficiency_pct = max(0.0, min(100.0, body_efficiency_pct))

    # VWAP.
    vwap = calc_vwap(df)
    dist_vwap_pct = pct_change(ltp, vwap) if vwap > 0 else 0.0

    # Opening range.
    or_high, or_low, or_range_pct = get_opening_range(df)

    if or_high > 0 and ltp > or_high:
        orb_status = "ABOVE_ORH"
    elif or_low > 0 and ltp < or_low:
        orb_status = "BELOW_ORL"
    elif or_high > 0 and or_low > 0:
        orb_status = "INSIDE_OR"
    else:
        orb_status = "OR_NOT_READY"

    # EMA trend.
    close_series = df["close"].astype(float)
    ema_fast = calc_ema(close_series, EMA_FAST).iloc[-1] if len(close_series) >= EMA_FAST else 0.0
    ema_slow = calc_ema(close_series, EMA_SLOW).iloc[-1] if len(close_series) >= EMA_SLOW else 0.0

    if ema_fast > 0 and ema_slow > 0:
        if ltp > ema_fast > ema_slow:
            ema_trend = "UP"
        elif ltp < ema_fast < ema_slow:
            ema_trend = "DOWN"
        else:
            ema_trend = "MIXED"
    else:
        ema_trend = "NA"

    # Last 5-minute and 15-minute movement.
    last_5m_pct = 0.0
    last_15m_pct = 0.0

    if len(df) >= 6:
        ref_5 = float(df["close"].iloc[-6])
        last_5m_pct = pct_change(ltp, ref_5)

    if len(df) >= 16:
        ref_15 = float(df["close"].iloc[-16])
        last_15m_pct = pct_change(ltp, ref_15)

    # ATR / current noise estimate.
    atr_1m = calc_atr_1m(df, ATR_PERIOD)
    atr_1m_pct = atr_1m * 100.0 / ltp if ltp > 0 else 0.0

    # Volume pace versus previous day.
    today_volume = safe_int(quote.get("volume"), 0)

    if today_volume <= 0:
        today_volume = int(float(df["volume"].sum()))

    # Estimate elapsed fraction of trading day.
    now_ts = now_ist().replace(tzinfo=None)
    start_ts = session_start_dt(today_ist())
    end_ts = session_end_dt(today_ist())

    elapsed_sec = max(0.0, min((now_ts - start_ts).total_seconds(), (end_ts - start_ts).total_seconds()))
    total_sec = (end_ts - start_ts).total_seconds()
    elapsed_fraction = elapsed_sec / total_sec if total_sec > 0 else 0.0

    expected_volume_now = prev_day_volume * elapsed_fraction if prev_day_volume > 0 else 0.0
    volume_pace_x = today_volume / expected_volume_now if expected_volume_now > 0 else 0.0

    # CPR / pivot status.
    cpr_position = "NA"
    nearest = ""
    nearest_value = 0.0
    nearest_dist_pct = 0.0

    if levels is not None:
        cpr_top = max(levels.tc, levels.bc)
        cpr_bottom = min(levels.tc, levels.bc)

        if ltp > cpr_top:
            cpr_position = "ABOVE_CPR"
        elif ltp < cpr_bottom:
            cpr_position = "BELOW_CPR"
        else:
            cpr_position = "INSIDE_CPR"

        nearest, nearest_value, nearest_dist_pct = nearest_level(ltp, levels)

    # Directional bias.
    if body_signed_pct > 0:
        direction = "UP"
    elif body_signed_pct < 0:
        direction = "DOWN"
    else:
        direction = "FLAT"

    # Rough comparative live momentum score.
    # This is not a trading signal.
    range_score = min(25.0, high_low_range_pct / 3.0 * 25.0)
    body_score = min(25.0, body_pct / 1.5 * 25.0)
    efficiency_score = min(20.0, body_efficiency_pct / 55.0 * 20.0)
    volume_score = min(15.0, volume_pace_x / 1.5 * 15.0)

    trend_score = 0.0
    if ema_trend in {"UP", "DOWN"}:
        trend_score += 8.0
    if orb_status in {"ABOVE_ORH", "BELOW_ORL"}:
        trend_score += 7.0

    live_score = range_score + body_score + efficiency_score + volume_score + trend_score
    live_score = max(0.0, min(100.0, live_score))

    # Human-readable setup classification.
    if live_score >= 75 and orb_status == "ABOVE_ORH" and dist_vwap_pct > 0 and ema_trend == "UP":
        insight = "STRONG_UP_MOMENTUM"
    elif live_score >= 75 and orb_status == "BELOW_ORL" and dist_vwap_pct < 0 and ema_trend == "DOWN":
        insight = "STRONG_DOWN_MOMENTUM"
    elif live_score >= 60 and abs(dist_vwap_pct) < 0.25 and orb_status == "INSIDE_OR":
        insight = "ACTIVE_BUT_CHOPPY"
    elif live_score >= 60:
        insight = "ACTIVE_WATCH"
    else:
        insight = "LOW_PRIORITY"

    return {
        "symbol": symbol,
        "ltp": round(ltp, 2),
        "direction": direction,
        "day_change_pct": round(day_change_pct, 2),

        "day_open": round(day_open, 2),
        "day_high": round(day_high, 2),
        "day_low": round(day_low, 2),

        "hl_range_pct": round(high_low_range_pct, 2),
        "body_pct": round(body_pct, 2),
        "body_signed_pct": round(body_signed_pct, 2),
        "body_eff_pct": round(body_efficiency_pct, 2),

        "vwap": round(vwap, 2),
        "dist_vwap_pct": round(dist_vwap_pct, 2),

        "or_high": round(or_high, 2),
        "or_low": round(or_low, 2),
        "or_range_pct": round(or_range_pct, 2),
        "orb_status": orb_status,

        "ema9": round(float(ema_fast), 2) if ema_fast else 0.0,
        "ema21": round(float(ema_slow), 2) if ema_slow else 0.0,
        "ema_trend": ema_trend,

        "last_5m_pct": round(last_5m_pct, 2),
        "last_15m_pct": round(last_15m_pct, 2),

        "atr_1m": round(atr_1m, 2),
        "atr_1m_pct": round(atr_1m_pct, 3),

        "today_volume": today_volume,
        "prev_day_volume": prev_day_volume,
        "volume_pace_x": round(volume_pace_x, 2),

        "cpr_position": cpr_position,
        "nearest_level": nearest,
        "nearest_level_value": round(nearest_value, 2),
        "nearest_level_dist_pct": round(nearest_dist_pct, 2),

        "live_score": round(live_score, 2),
        "insight": insight,
    }


# ============================================================
# DISPLAY
# ============================================================

def clear_terminal() -> None:
    """Clear terminal screen."""
    os.system("cls" if os.name == "nt" else "clear")


def print_scan_table(scan_df: pd.DataFrame) -> None:
    """Print compact live scanner table."""
    if scan_df.empty:
        print("[WARN] No scan rows.")
        return

    display_cols = [
        "symbol",
        "ltp",
        "direction",
        "day_change_pct",
        "hl_range_pct",
        "body_pct",
        "body_eff_pct",
        "dist_vwap_pct",
        "orb_status",
        "ema_trend",
        "last_5m_pct",
        "last_15m_pct",
        "volume_pace_x",
        "cpr_position",
        "nearest_level",
        "nearest_level_dist_pct",
        "live_score",
        "insight",
    ]

    available_cols = [c for c in display_cols if c in scan_df.columns]

    out = scan_df[available_cols].copy()

    if "live_score" in out.columns:
        out = out.sort_values("live_score", ascending=False)

    print(out.to_string(index=False))


# ============================================================
# MAIN LOOP
# ============================================================

def main() -> None:
    """Main entrypoint."""
    args = parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]

    print("============================================================")
    print("LIVE 5-STOCK INTRADAY INSIGHT SCANNER")
    print("============================================================")
    print(f"[CONFIG] Symbols              : {symbols}")
    print(f"[CONFIG] Refresh seconds      : {args.refresh_sec}")
    print(f"[CONFIG] Hist refresh seconds : {args.hist_refresh_sec}")
    print(f"[CONFIG] Export CSV           : {args.export_csv}")
    print("============================================================")

    print("[STEP] Initializing Kite ...")
    kite = initialize_kite()
    print("[OK] Kite initialized.")

    equity_map = load_nse_equity_instruments(kite)
    metas = resolve_symbols(symbols, equity_map)

    print("[STEP] Resolved symbols:")
    for meta in metas:
        print(f"  {meta.symbol:<10} token={meta.instrument_token} key={meta.kite_key}")

    print("[STEP] Fetching previous-day CPR/pivots ...")
    previous_levels: Dict[str, PreviousDayLevels] = {}

    for meta in metas:
        try:
            levels = fetch_previous_day_levels(kite, meta)
            if levels is not None:
                previous_levels[meta.symbol] = levels
                print(
                    f"  {meta.symbol:<10} prev={levels.source_day} "
                    f"P={levels.p:.2f} BC={levels.bc:.2f} TC={levels.tc:.2f} "
                    f"R1={levels.r1:.2f} S1={levels.s1:.2f}"
                )
            else:
                print(f"  {meta.symbol:<10} previous levels not found")
        except Exception as exc:
            print(f"  {meta.symbol:<10} pivot fetch failed: {exc}")

        time.sleep(0.20)

    print("[STEP] Fetching today's 1-minute candles ...")
    candles: Dict[str, pd.DataFrame] = {}

    for meta in metas:
        try:
            df = fetch_today_1m(kite, meta)
            candles[meta.symbol] = df
            print(f"  {meta.symbol:<10} candles={len(df)}")
        except Exception as exc:
            print(f"  {meta.symbol:<10} 1m fetch failed: {exc}")
            candles[meta.symbol] = empty_ohlcv_df()

        time.sleep(0.20)

    # Initial quote fetch, mainly to seed cumulative volume.
    print("[STEP] Fetching initial live quotes ...")
    initial_quotes = quote_all(kite, metas)

    last_cum_volume: Dict[str, int] = {}
    last_quote: Dict[str, Dict[str, Any]] = {}

    for meta in metas:
        q = initial_quotes.get(meta.kite_key, {}) or {}
        last_quote[meta.symbol] = q
        last_cum_volume[meta.symbol] = safe_int(q.get("volume"), 0)

    state = LiveState(
        candles=candles,
        last_cum_volume=last_cum_volume,
        last_quote=last_quote,
        last_hist_refresh_ts=time.time(),
    )

    print("[START] Live scanner running. Press Ctrl+C to stop.")
    time.sleep(1.0)

    while True:
        try:
            # Poll live quotes in one request.
            quotes = quote_all(kite, metas)

            # Update current minute candles from quote snapshots.
            for meta in metas:
                q = quotes.get(meta.kite_key, {}) or {}
                if q:
                    update_current_minute_from_quote(state, meta, q)

            # Periodically correct with actual 1-minute historical candles.
            now_ts = time.time()

            if now_ts - state.last_hist_refresh_ts >= float(args.hist_refresh_sec):
                for meta in metas:
                    try:
                        fresh = fetch_today_1m(kite, meta)
                        state.candles[meta.symbol] = merge_candles(state.candles.get(meta.symbol), fresh)
                    except Exception as exc:
                        print(f"[WARN] Historical refresh failed for {meta.symbol}: {exc}")

                    time.sleep(0.15)

                state.last_hist_refresh_ts = now_ts

            # Build insight rows.
            rows: List[Dict[str, Any]] = []

            for meta in metas:
                q = state.last_quote.get(meta.symbol, {})
                df = state.candles.get(meta.symbol, empty_ohlcv_df())
                levels = previous_levels.get(meta.symbol)
                row = compute_stock_insight(meta, df, q, levels)
                rows.append(row)

            scan_df = pd.DataFrame(rows)

            # Save latest scan to CSV.
            if args.export_csv:
                scan_df.to_csv(args.export_csv, index=False)

            # Display.
            if not args.no_clear:
                clear_terminal()

            print("============================================================")
            print("LIVE 5-STOCK INTRADAY INSIGHT SCANNER")
            print(f"Time: {now_ist().strftime('%Y-%m-%d %H:%M:%S IST')}")
            print("Score is comparative only, not a trade signal.")
            print("============================================================")
            print_scan_table(scan_df)
            print("============================================================")
            print(f"CSV saved: {args.export_csv}")
            print("Press Ctrl+C to stop.")
            print("============================================================")

            time.sleep(float(args.refresh_sec))

        except KeyboardInterrupt:
            print("\n[STOPPED] User interrupted.")
            break

        except Exception as exc:
            print("[ERROR] Main loop error:", exc)
            traceback.print_exc()
            time.sleep(float(args.refresh_sec))


if __name__ == "__main__":
    main()