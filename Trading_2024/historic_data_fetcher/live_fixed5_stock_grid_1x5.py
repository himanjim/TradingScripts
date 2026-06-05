#!/usr/bin/env python3
"""
Zerodha Fixed 5 Live Stock Grid
===============================

Purpose
-------
This Dash application shows only the following five NSE equity stocks:

    1. IDEA
    2. BSE
    3. MCX
    4. FORCEMOT
    5. KAYNES

It does the following:

1. Matches the fixed watchlist with Zerodha/Kite NSE equity instruments.
2. Backfills today's missing 1-minute candles for these five stocks using
   Kite historical_data().
3. Fetches the previous trading day's daily candle and computes CPR/pivot levels.
4. Displays the five charts in a 1 x 5 layout, meaning one column and five rows.
5. Refreshes all five charts every second by polling all instruments in a single
   kite.quote([...]) call.
6. Keeps the small + / − button in each card for client-side expand/minify.

Why polling is used here
------------------------
The user specifically requested kite.quote polling. WebSocket ticks are usually
better for sub-second market data, but this script deliberately uses quote polling
so all five selected instruments refresh together once per second.

Prerequisites
-------------
    pip install pandas numpy pytz plotly dash kiteconnect

Your existing trading environment should expose one of these helpers:
    Trading_2024.OptionTradeUtils.intialize_kite_api()
    OptionTradeUtils.intialize_kite_api()

This is the same Kite initialization pattern used in your existing scripts.

Example
-------
    python live_fixed5_stock_grid.py --port 8050

Optional examples
-----------------
    python live_fixed5_stock_grid.py --refresh-ms 1000
    python live_fixed5_stock_grid.py --cache-dir ./live_stock_cache
    python live_fixed5_stock_grid.py --symbols IDEA,BSE,MCX,FORCEMOT,KAYNES

Important practical note
------------------------
Kite's historical API is not bulk. Startup backfill is therefore one historical
request per selected stock, plus one daily historical request per selected stock
for pivots. The code throttles these requests to reduce the probability of
429/rate-limit errors.
"""

from __future__ import annotations

import argparse
import math
import os
import re
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import pytz
from dash import Dash, Input, Output, dcc, html


# =============================================================================
# Optional imports from the user's trading environment
# =============================================================================
try:
    import Trading_2024.OptionTradeUtils as oUtils  # type: ignore
except Exception:
    try:
        import OptionTradeUtils as oUtils  # type: ignore
    except Exception:
        oUtils = None
        print("[WARN] OptionTradeUtils.py not found. Kite login will fail unless it is importable.")

try:
    from kiteconnect import KiteConnect
except Exception:
    KiteConnect = None  # type: ignore[assignment]
    print("[WARN] kiteconnect not installed. Run: pip install kiteconnect")


# =============================================================================
# User-level constants
# =============================================================================
IST = pytz.timezone("Asia/Kolkata")

# NSE equity cash market timings.
SESSION_START_IST = dtime(9, 15)
SESSION_END_IST = dtime(15, 30)

# Quote API accepts many instruments in one call. This fixed-watchlist version
# polls only 5 instruments in the live loop, but the helper remains chunk-safe.
QUOTE_CHUNK_SIZE = 500

# Conservative pause between startup quote chunks. Quote endpoint is still an
# HTTP endpoint; keep this non-zero to avoid hammering the API during startup.
STARTUP_QUOTE_CHUNK_PAUSE_SEC = 1.05

# Historical data is fetched one instrument at a time. Keep this high enough to
# avoid repeated 429 errors. You can reduce it if your app/key tolerates it.
HISTORICAL_CALL_PAUSE_SEC = 0.38

# Chart sizing. With one column, a taller chart is more useful than the earlier
# 5-column dashboard mini-chart height.
DEFAULT_CHART_HEIGHT_PX = 420

# Height used when one chart is expanded through the small + button.
# Keep this below the typical browser viewport height so that the enlarged chart
# remains usable without forcing excessive vertical scrolling.
DEFAULT_EXPANDED_CHART_HEIGHT_PX = 760

# Number of intraday candles to show in each chart. Use 390 to show the full
# NSE cash-market day. Use 120 if you want a cleaner last-2-hour dashboard.
DEFAULT_MAX_CANDLES_SHOWN = 390

# Files are saved as CSV to avoid a pyarrow dependency for parquet.
CACHE_DATE_FORMAT = "%Y-%m-%d"

# Fixed watchlist requested by the user. The layout is one column and five rows,
# in exactly this order.
DEFAULT_FIXED_SYMBOLS = ("IDEA", "BSE", "MCX", "FORCEMOT", "KAYNES")


# =============================================================================
# Data classes
# =============================================================================
@dataclass(frozen=True)
class StockMeta:
    """Static metadata for one selected Zerodha stock instrument."""

    rank: int
    symbol: str
    exchange: str
    kite_key: str
    instrument_token: int
    name: str
    initial_volume: int
    initial_ltp: float
    initial_turnover: float


@dataclass(frozen=True)
class PivotLevels:
    """Classic floor-pivot and CPR levels computed from previous trading day."""

    source_day: date
    high: float
    low: float
    close: float
    p: float
    bc: float
    tc: float
    r1: float
    s1: float
    r2: float
    s2: float


@dataclass
class LiveState:
    """Mutable in-memory state updated every second by quote polling."""

    # Symbol -> current-day 1-minute OHLCV dataframe.
    candles: Dict[str, pd.DataFrame]

    # Symbol -> previous cumulative day volume seen from quote(). This lets us
    # estimate per-minute volume deltas for live candles.
    last_cum_volume: Dict[str, int]

    # Symbol -> last complete quote payload returned by Kite.
    last_quote: Dict[str, Dict[str, Any]]

    # Symbol -> last error text, if any.
    errors: Dict[str, str]

    # Last time CSV caches were flushed to disk.
    last_cache_save_ts: float


# =============================================================================
# CLI parsing
# =============================================================================
def parse_args() -> argparse.Namespace:
    """Parse command-line arguments.

    This fixed-watchlist version does not need a CSV. The older ``--symbols-csv``
    and ``--top-n`` arguments are retained as ignored compatibility arguments so
    old run commands do not immediately fail under argparse.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Live Zerodha dashboard for IDEA, BSE, MCX, FORCEMOT, and KAYNES "
            "in a 1 x 5 Plotly/Dash grid."
        )
    )
    parser.add_argument(
        "--symbols",
        default=",".join(DEFAULT_FIXED_SYMBOLS),
        help=(
            "Comma-separated NSE symbols to show, in display order. "
            "Default: IDEA,BSE,MCX,FORCEMOT,KAYNES."
        ),
    )
    parser.add_argument(
        "--symbols-csv",
        default=None,
        help=(
            "Ignored in this fixed-watchlist version. Retained only so older "
            "run commands with --symbols-csv do not fail."
        ),
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=5,
        help=(
            "Ignored in this fixed-watchlist version. Exactly the symbols in "
            "--symbols are displayed."
        ),
    )
    parser.add_argument(
        "--exchange",
        default="NSE",
        choices=["NSE"],
        help="Exchange to use. Currently NSE equity is supported. Default: NSE.",
    )
    parser.add_argument(
        "--refresh-ms",
        type=int,
        default=1000,
        help="Dash refresh interval in milliseconds. Default: 1000.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("PORT", "8050")),
        help="Dash server port. Default: 8050 or PORT environment variable.",
    )
    parser.add_argument(
        "--cache-dir",
        default="live_stock_grid_cache",
        help="Folder where today's intraday candle CSV caches are stored.",
    )
    parser.add_argument(
        "--chart-height",
        type=int,
        default=DEFAULT_CHART_HEIGHT_PX,
        help=f"Height of each chart in pixels. Default: {DEFAULT_CHART_HEIGHT_PX}.",
    )
    parser.add_argument(
        "--expanded-chart-height",
        type=int,
        default=DEFAULT_EXPANDED_CHART_HEIGHT_PX,
        help=(
            "Height of the chart after clicking the + button. "
            f"Default: {DEFAULT_EXPANDED_CHART_HEIGHT_PX}."
        ),
    )
    parser.add_argument(
        "--max-candles-shown",
        type=int,
        default=DEFAULT_MAX_CANDLES_SHOWN,
        help=f"Candles shown per chart. Default: {DEFAULT_MAX_CANDLES_SHOWN}.",
    )
    parser.add_argument(
        "--skip-backfill",
        action="store_true",
        help="Skip historical_data() startup backfill and start only from live quote ticks.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Run Dash in debug mode.",
    )
    return parser.parse_args()


# =============================================================================
# Basic helpers
# =============================================================================
def now_ist() -> datetime:
    """Return current time in Asia/Kolkata timezone."""
    return datetime.now(tz=IST)


def market_day() -> date:
    """Return today's date in IST."""
    return now_ist().date()


def session_start_dt(day: date) -> datetime:
    """Return timezone-aware NSE cash-market session start for a date."""
    return IST.localize(datetime.combine(day, SESSION_START_IST))


def session_end_dt(day: date) -> datetime:
    """Return timezone-aware NSE cash-market session end for a date."""
    return IST.localize(datetime.combine(day, SESSION_END_IST))


def clamp_to_session(ts: datetime, day: date) -> datetime:
    """Clamp a timestamp to the regular NSE cash-market session window."""
    start = session_start_dt(day)
    end = session_end_dt(day)
    if ts < start:
        return start
    if ts > end:
        return end
    return ts


def is_within_session(ts: datetime) -> bool:
    """Return True if a timestamp falls inside regular cash-market hours."""
    t = ts.astimezone(IST).time()
    return SESSION_START_IST <= t <= SESSION_END_IST


def floor_to_minute(ts: datetime) -> pd.Timestamp:
    """Round/floor a timestamp down to its minute open."""
    return pd.Timestamp(ts.astimezone(IST)).floor("min")


def ensure_ist_datetime(value: Any) -> pd.Timestamp:
    """Parse any Kite/Pandas timestamp and convert/localize it to IST."""
    ts = pd.Timestamp(value)
    if pd.isna(ts):
        return pd.NaT
    if ts.tzinfo is None:
        # Kite usually returns timezone-aware timestamps. If it does not, treat
        # the value as IST because Zerodha Indian-market data is in IST context.
        ts = ts.tz_localize(IST)
    else:
        ts = ts.tz_convert(IST)
    return ts


def to_ist_series(values: pd.Series) -> pd.Series:
    """Convert a timestamp Series to timezone-aware IST safely.

    This intentionally avoids a single vectorized ``pd.to_datetime(series)`` call
    because cached CSV rows and Kite rows can be a mixture of strings, Python
    datetimes, pandas Timestamps, timezone-aware values, and timezone-naive
    values. Pandas may raise or create object dtype for such mixtures, which then
    breaks ``.dt`` operations inside Dash callbacks. Element-wise conversion is
    slower but stable for a 50-stock dashboard.

    Functional fix in this version:
        The earlier implementation called ``pd.to_datetime`` on an already mixed
        Series. In some Pandas versions this may still return object dtype. This
        version constructs a DatetimeIndex from already-normalized IST timestamps
        and then converts it back to a Series with the original index, giving a
        reliable ``datetime64[ns, Asia/Kolkata]`` dtype for downstream ``.dt`` use.
    """
    converted: List[pd.Timestamp] = []
    for value in values.tolist():
        if pd.isna(value):
            converted.append(pd.NaT)
        else:
            converted.append(ensure_ist_datetime(value))

    # DatetimeIndex preserves the timezone when all non-null timestamps are IST.
    # This is safer for Dash callbacks than letting Pandas infer object dtype.
    return pd.Series(pd.DatetimeIndex(converted), index=values.index)


def clean_symbol(raw: Any) -> str:
    """Normalize symbol strings from the input CSV.

    Handles values such as 'NSE:RELIANCE' by stripping the exchange prefix.
    """
    s = str(raw).strip().upper()
    if not s or s == "NAN":
        return ""
    if ":" in s:
        s = s.split(":", 1)[1].strip()
    return s


def parse_symbol_list(raw_symbols: str) -> List[str]:
    """Parse a comma-separated symbol list while preserving order.

    This lets you keep the default five-stock watchlist or temporarily override
    it from the command line, for example:
        --symbols IDEA,BSE,MCX,FORCEMOT,KAYNES
    """
    if not raw_symbols:
        return list(DEFAULT_FIXED_SYMBOLS)

    cleaned = [clean_symbol(x) for x in str(raw_symbols).split(",")]
    cleaned = [x for x in cleaned if x]

    # Remove duplicates without changing the requested display order.
    seen = set()
    out: List[str] = []
    for symbol in cleaned:
        if symbol not in seen:
            out.append(symbol)
            seen.add(symbol)

    if not out:
        raise ValueError("No valid symbols found in --symbols.")
    return out


def safe_file_part(value: str) -> str:
    """Create a safe string for filenames and HTML ids."""
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value.strip().upper())


def chunked(items: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    """Yield fixed-size chunks from a sequence."""
    for i in range(0, len(items), size):
        yield items[i : i + size]


def numeric(value: Any, default: float = 0.0) -> float:
    """Convert a value to float safely."""
    try:
        if value is None:
            return default
        v = float(value)
        if math.isnan(v):
            return default
        return v
    except Exception:
        return default


def integer(value: Any, default: int = 0) -> int:
    """Convert a value to int safely."""
    try:
        if value is None:
            return default
        return int(float(value))
    except Exception:
        return default


# =============================================================================
# Kite initialization and instrument matching
# =============================================================================
def init_kite() -> "KiteConnect":
    """Initialize Kite through the same helper used by the user's scripts."""
    if KiteConnect is None:
        raise RuntimeError("kiteconnect is not installed. Run: pip install kiteconnect")
    if oUtils is None:
        raise RuntimeError(
            "OptionTradeUtils.py is required because this script calls "
            "oUtils.intialize_kite_api(). Make sure your trading utilities are importable."
        )

    kite = oUtils.intialize_kite_api()
    if kite is None:
        raise RuntimeError("oUtils.intialize_kite_api() returned None. Check Kite authentication.")
    return kite


def load_symbols_from_csv(path: str) -> List[str]:
    """Read the user CSV and return de-duplicated cleaned symbols."""
    csv_path = Path(path)
    if not csv_path.exists():
        raise FileNotFoundError(f"Symbols CSV not found: {path}")

    df = pd.read_csv(csv_path)
    lower_cols = {c.lower().strip(): c for c in df.columns}
    if "symbol" not in lower_cols:
        raise ValueError(
            f"CSV must contain a column named 'symbol'. Found columns: {list(df.columns)}"
        )

    col = lower_cols["symbol"]
    symbols = [clean_symbol(x) for x in df[col].tolist()]
    symbols = [s for s in symbols if s]

    # Preserve CSV order but remove duplicates.
    seen = set()
    unique: List[str] = []
    for s in symbols:
        if s not in seen:
            unique.append(s)
            seen.add(s)

    if not unique:
        raise ValueError("No usable symbols found in the CSV symbol column.")
    return unique


def build_nse_equity_map(kite: "KiteConnect", exchange: str) -> Dict[str, Dict[str, Any]]:
    """Download Zerodha instrument dump and map NSE equity symbols to rows.

    Only pure equity instruments are retained. This avoids indices, futures,
    options, ETFs/other segments where the instrument_type/segment differ.
    """
    print(f"[KITE] Loading {exchange} instrument dump ...")
    rows = kite.instruments(exchange)
    print(f"[KITE] Instrument rows received for {exchange}: {len(rows):,}")

    equity_map: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        symbol = str(row.get("tradingsymbol", "")).strip().upper()
        segment = str(row.get("segment", "")).strip().upper()
        inst_type = str(row.get("instrument_type", "")).strip().upper()

        # For NSE stocks, Zerodha commonly has segment=NSE and instrument_type=EQ.
        if not symbol:
            continue
        if segment != exchange.upper():
            continue
        if inst_type != "EQ":
            continue
        equity_map[symbol] = row

    print(f"[KITE] NSE equity symbols retained: {len(equity_map):,}")
    return equity_map


def match_csv_symbols_to_kite(
    csv_symbols: Sequence[str],
    equity_map: Dict[str, Dict[str, Any]],
    exchange: str,
) -> List[Dict[str, Any]]:
    """Return Kite instrument rows for symbols that exist in the instrument dump."""
    matched: List[Dict[str, Any]] = []
    missing: List[str] = []

    for symbol in csv_symbols:
        row = equity_map.get(symbol)
        if row is None:
            missing.append(symbol)
            continue
        row2 = dict(row)
        row2["kite_key"] = f"{exchange}:{symbol}"
        matched.append(row2)

    if missing:
        print(f"[WARN] Symbols not found as {exchange} equity instruments: {len(missing)}")
        print("       First missing symbols:", ", ".join(missing[:30]))

    if not matched:
        raise RuntimeError("None of the CSV symbols matched Zerodha NSE equity instruments.")
    return matched


def select_fixed_watchlist_stocks(
    kite: "KiteConnect",
    equity_map: Dict[str, Dict[str, Any]],
    exchange: str,
    requested_symbols: Sequence[str],
) -> List[StockMeta]:
    """Build StockMeta rows for the fixed watchlist in the requested order.

    Unlike the earlier top-50 version, this function does not rank symbols by
    volume. It validates the exact requested NSE symbols, fetches one quote
    snapshot for those symbols, and keeps the user's order:
        IDEA, BSE, MCX, FORCEMOT, KAYNES
    """
    if not requested_symbols:
        raise ValueError("requested_symbols cannot be empty.")

    missing = [s for s in requested_symbols if s not in equity_map]
    if missing:
        raise RuntimeError(
            "These requested symbols were not found as NSE EQ instruments in "
            f"Zerodha's instrument dump: {', '.join(missing)}"
        )

    rows = [equity_map[symbol] for symbol in requested_symbols]
    kite_keys = [f"{exchange}:{symbol}" for symbol in requested_symbols]

    print(f"[STEP] Fetching initial quote snapshot for fixed watchlist: {', '.join(requested_symbols)}")
    quotes = kite_quote_many(kite, kite_keys, chunk_size=QUOTE_CHUNK_SIZE, pause_between_chunks=0.0)
    print(f"[STEP] Initial quote rows received: {len(quotes):,}/{len(kite_keys):,}")

    selected: List[StockMeta] = []
    for rank, (symbol, row) in enumerate(zip(requested_symbols, rows), start=1):
        kite_key = f"{exchange}:{symbol}"
        q = quotes.get(kite_key, {}) or {}
        volume = integer(q.get("volume"), 0)
        ltp = numeric(q.get("last_price"), 0.0)
        turnover = float(volume) * float(ltp)
        selected.append(
            StockMeta(
                rank=rank,
                symbol=symbol,
                exchange=exchange,
                kite_key=kite_key,
                instrument_token=int(row["instrument_token"]),
                name=str(row.get("name", symbol)),
                initial_volume=int(volume),
                initial_ltp=float(ltp),
                initial_turnover=float(turnover),
            )
        )

    print("[WATCHLIST] Fixed display order:")
    for s in selected:
        print(
            f"  {s.rank:02d}. {s.symbol:<10} "
            f"volume={s.initial_volume:>12,} ltp={s.initial_ltp:>10.2f} "
            f"turnover≈{s.initial_turnover:,.0f}"
        )

    return selected


# =============================================================================
# Quote scanning and top-liquidity selection
# =============================================================================
def kite_quote_many(
    kite: "KiteConnect",
    kite_keys: Sequence[str],
    chunk_size: int = QUOTE_CHUNK_SIZE,
    pause_between_chunks: float = STARTUP_QUOTE_CHUNK_PAUSE_SEC,
) -> Dict[str, Dict[str, Any]]:
    """Fetch quotes for many instruments using chunked quote() calls.

    For the live top-50 loop, this function is called with <=50 instruments and
    therefore makes exactly one quote request.
    """
    all_quotes: Dict[str, Dict[str, Any]] = {}

    for idx, keys_chunk in enumerate(chunked(list(kite_keys), chunk_size), start=1):
        try:
            data = kite.quote(list(keys_chunk))
            if data:
                all_quotes.update(data)
        except Exception as exc:
            print(f"[QUOTE WARN] quote chunk {idx} failed: {exc}")

        # Sleep only between chunks, not after the last chunk.
        if idx * chunk_size < len(kite_keys):
            time.sleep(pause_between_chunks)

    return all_quotes


def select_top_liquid_stocks(
    kite: "KiteConnect",
    matched_rows: Sequence[Dict[str, Any]],
    exchange: str,
    top_n: int,
) -> List[StockMeta]:
    """Select top-N symbols by current live traded volume.

    Ranking field:
        quote['volume'] = cumulative traded quantity for the current day.

    Tie-breakers:
        1. Volume descending
        2. Estimated turnover descending = volume * last_price
        3. Symbol ascending
    """
    if top_n <= 0:
        raise ValueError("top_n must be positive.")

    kite_keys = [str(r["kite_key"]) for r in matched_rows]
    print(f"[STEP] Scanning live quotes for {len(kite_keys):,} symbols ...")
    quotes = kite_quote_many(kite, kite_keys)
    print(f"[STEP] Quote rows received: {len(quotes):,}")

    scored: List[Tuple[int, float, str, Dict[str, Any], Dict[str, Any]]] = []
    row_by_key = {str(r["kite_key"]): r for r in matched_rows}

    for kite_key, row in row_by_key.items():
        q = quotes.get(kite_key, {}) or {}
        symbol = str(row.get("tradingsymbol", "")).strip().upper()
        volume = integer(q.get("volume"), 0)
        ltp = numeric(q.get("last_price"), 0.0)
        turnover = float(volume) * float(ltp)
        scored.append((volume, turnover, symbol, row, q))

    scored.sort(key=lambda x: (-x[0], -x[1], x[2]))
    selected = scored[: min(top_n, len(scored))]

    out: List[StockMeta] = []
    for rank, (volume, turnover, symbol, row, q) in enumerate(selected, start=1):
        out.append(
            StockMeta(
                rank=rank,
                symbol=symbol,
                exchange=exchange,
                kite_key=f"{exchange}:{symbol}",
                instrument_token=int(row["instrument_token"]),
                name=str(row.get("name", symbol)),
                initial_volume=int(volume),
                initial_ltp=float(numeric(q.get("last_price"), 0.0)),
                initial_turnover=float(turnover),
            )
        )

    print("[TOP 50] Highest live-volume stocks:")
    for s in out:
        print(
            f"  {s.rank:02d}. {s.symbol:<14} "
            f"volume={s.initial_volume:>12,} ltp={s.initial_ltp:>10.2f} "
            f"turnover≈{s.initial_turnover:,.0f}"
        )

    zero_volume_count = sum(1 for s in out if s.initial_volume <= 0)
    if zero_volume_count == len(out):
        print(
            "[WARN] All selected stocks have zero live volume. "
            "This usually means the market is closed or quotes are not providing today's traded volume yet."
        )
    return out


# =============================================================================
# Candle cache and historical backfill
# =============================================================================
def candle_cache_path(cache_dir: str, exchange: str, symbol: str, day: date) -> Path:
    """Return the CSV cache path for one symbol/day."""
    base = Path(cache_dir) / exchange.upper() / day.strftime(CACHE_DATE_FORMAT)
    base.mkdir(parents=True, exist_ok=True)
    return base / f"{safe_file_part(symbol)}_1min.csv"


def normalize_candle_df(df: pd.DataFrame, day: Optional[date] = None) -> pd.DataFrame:
    """Return a clean 1-minute OHLCV dataframe with IST timestamps."""
    if df is None or df.empty:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    d = df.copy()
    if "date" not in d.columns:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    # Robust conversion. Do not use one vectorized pd.to_datetime() before this;
    # mixed tz-aware/tz-naive inputs can make Dash callbacks fail and leave blank
    # default Plotly grids in the browser.
    d["date"] = to_ist_series(d["date"])

    for col in ["open", "high", "low", "close"]:
        if col not in d.columns:
            d[col] = np.nan
        d[col] = pd.to_numeric(d[col], errors="coerce")

    if "volume" not in d.columns:
        d["volume"] = 0
    d["volume"] = pd.to_numeric(d["volume"], errors="coerce").fillna(0).astype(float)

    d = d.dropna(subset=["date", "open", "high", "low", "close"]).copy()
    if day is not None:
        d = d[d["date"].dt.date == day].copy()

    # Keep only regular session candles.
    d = d[(d["date"].dt.time >= SESSION_START_IST) & (d["date"].dt.time <= SESSION_END_IST)].copy()

    # Collapse duplicate minute rows by keeping the last row. This is safe for a
    # cache merge, because live quote updates continuously rewrite the latest
    # candle.
    d["date"] = d["date"].dt.floor("min")
    d = d.drop_duplicates(subset=["date"], keep="last")
    d = d.sort_values("date").reset_index(drop=True)
    return d[["date", "open", "high", "low", "close", "volume"]]


def load_cached_candles(cache_dir: str, meta: StockMeta, day: date) -> pd.DataFrame:
    """Load one symbol's cached current-day minute candles, if available."""
    path = candle_cache_path(cache_dir, meta.exchange, meta.symbol, day)
    if not path.exists():
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    try:
        raw = pd.read_csv(path)
        return normalize_candle_df(raw, day=day)
    except Exception as exc:
        print(f"[CACHE WARN] Could not read cache for {meta.symbol}: {exc}")
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])


def save_cached_candles(cache_dir: str, meta: StockMeta, day: date, df: pd.DataFrame) -> None:
    """Write one symbol's minute candles to CSV cache."""
    try:
        path = candle_cache_path(cache_dir, meta.exchange, meta.symbol, day)
        d = normalize_candle_df(df, day=day)
        # Save ISO timestamps with timezone. This makes reloading deterministic.
        out = d.copy()
        out["date"] = out["date"].map(lambda x: pd.Timestamp(x).isoformat())
        out.to_csv(path, index=False)
    except Exception as exc:
        print(f"[CACHE WARN] Could not save cache for {meta.symbol}: {exc}")


def merge_candles(old: pd.DataFrame, new: pd.DataFrame, day: date) -> pd.DataFrame:
    """Merge historical/cached/live candle rows for one symbol."""
    parts = []
    if old is not None and not old.empty:
        parts.append(old)
    if new is not None and not new.empty:
        parts.append(new)
    if not parts:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
    return normalize_candle_df(pd.concat(parts, ignore_index=True), day=day)


def fetch_intraday_backfill(
    kite: "KiteConnect",
    meta: StockMeta,
    day: date,
    to_dt: datetime,
) -> pd.DataFrame:
    """Fetch today's 1-minute historical candles for one stock.

    Kite historical_data() returns candles with Timestamp, Open, High, Low,
    Close, Volume. This function normalizes them into our local format.
    """
    start_dt = session_start_dt(day)
    end_dt = clamp_to_session(to_dt, day)

    if end_dt <= start_dt:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    rows = kite.historical_data(
        instrument_token=int(meta.instrument_token),
        from_date=start_dt,
        to_date=end_dt,
        interval="minute",
        continuous=False,
        oi=False,
    )
    if not rows:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
    return normalize_candle_df(pd.DataFrame(rows), day=day)


def startup_backfill_all(
    kite: "KiteConnect",
    selected: Sequence[StockMeta],
    cache_dir: str,
    skip_backfill: bool,
) -> Dict[str, pd.DataFrame]:
    """Load cache and backfill missing current-day candles for selected stocks."""
    day = market_day()
    current_time = now_ist()
    candles: Dict[str, pd.DataFrame] = {}

    print(f"[STEP] Loading candle cache/backfill for {len(selected)} stocks, day={day} ...")
    for idx, meta in enumerate(selected, start=1):
        cached = load_cached_candles(cache_dir, meta, day)
        merged = cached

        if not skip_backfill:
            try:
                hist = fetch_intraday_backfill(kite, meta, day, current_time)
                merged = merge_candles(cached, hist, day)
                save_cached_candles(cache_dir, meta, day, merged)
                print(
                    f"[BACKFILL {idx:02d}/{len(selected):02d}] {meta.symbol:<14} "
                    f"cache={len(cached):>3} hist={len(hist):>3} final={len(merged):>3}"
                )
            except Exception as exc:
                print(f"[BACKFILL WARN] {meta.symbol}: {exc}. Using cache/live ticks only.")
                merged = cached
            time.sleep(HISTORICAL_CALL_PAUSE_SEC)
        else:
            print(f"[CACHE {idx:02d}/{len(selected):02d}] {meta.symbol:<14} cached candles={len(cached):>3}")

        candles[meta.symbol] = normalize_candle_df(merged, day=day)

    return candles


# =============================================================================
# Pivot calculation
# =============================================================================
def compute_pivots_from_ohlc(source_day: date, high: float, low: float, close: float) -> PivotLevels:
    """Compute classic floor pivots and CPR from previous day OHLC.

    Formulae:
        P  = (H + L + C) / 3
        BC = (H + L) / 2
        TC = 2*P - BC
        R1 = 2*P - L
        S1 = 2*P - H
        R2 = P + (H - L)
        S2 = P - (H - L)
    """
    h = float(high)
    l = float(low)
    c = float(close)
    p = (h + l + c) / 3.0
    bc = (h + l) / 2.0
    tc = 2.0 * p - bc
    r1 = 2.0 * p - l
    s1 = 2.0 * p - h
    r2 = p + (h - l)
    s2 = p - (h - l)
    return PivotLevels(source_day=source_day, high=h, low=l, close=c, p=p, bc=bc, tc=tc, r1=r1, s1=s1, r2=r2, s2=s2)


def fetch_previous_day_pivots(kite: "KiteConnect", meta: StockMeta, today: date) -> Optional[PivotLevels]:
    """Fetch previous trading day's daily candle and compute pivots."""
    # Ask for enough calendar days to survive weekends and holidays.
    from_dt = IST.localize(datetime.combine(today - timedelta(days=15), dtime(0, 0)))
    to_dt = IST.localize(datetime.combine(today, dtime(0, 0)))

    rows = kite.historical_data(
        instrument_token=int(meta.instrument_token),
        from_date=from_dt,
        to_date=to_dt,
        interval="day",
        continuous=False,
        oi=False,
    )
    if not rows:
        return None

    df = pd.DataFrame(rows)
    if df.empty:
        return None
    df["date"] = to_ist_series(df["date"])
    df = df.dropna(subset=["date"]).copy()
    df["day"] = df["date"].dt.date
    df = df[df["day"] < today].copy()
    if df.empty:
        return None

    last = df.sort_values("day").iloc[-1]
    return compute_pivots_from_ohlc(
        source_day=last["day"],
        high=numeric(last.get("high")),
        low=numeric(last.get("low")),
        close=numeric(last.get("close")),
    )


def fetch_all_pivots(kite: "KiteConnect", selected: Sequence[StockMeta]) -> Dict[str, PivotLevels]:
    """Fetch previous-day pivots for all selected symbols."""
    today = market_day()
    pivots: Dict[str, PivotLevels] = {}

    print(f"[STEP] Fetching previous-day pivots for {len(selected)} stocks ...")
    for idx, meta in enumerate(selected, start=1):
        try:
            pv = fetch_previous_day_pivots(kite, meta, today)
            if pv is not None:
                pivots[meta.symbol] = pv
                print(
                    f"[PIVOT {idx:02d}/{len(selected):02d}] {meta.symbol:<14} "
                    f"prev={pv.source_day} P={pv.p:.2f} BC={pv.bc:.2f} TC={pv.tc:.2f} "
                    f"R1={pv.r1:.2f} S1={pv.s1:.2f}"
                )
            else:
                print(f"[PIVOT WARN] {meta.symbol}: no previous daily candle found")
        except Exception as exc:
            print(f"[PIVOT WARN] {meta.symbol}: {exc}")
        time.sleep(HISTORICAL_CALL_PAUSE_SEC)

    return pivots


# =============================================================================
# Live quote -> minute candle update
# =============================================================================
def quote_timestamp_or_now(q: Dict[str, Any]) -> datetime:
    """Extract the best available timestamp from a Kite quote payload."""
    for key in ("timestamp", "last_trade_time"):
        value = q.get(key)
        if value is not None:
            try:
                return ensure_ist_datetime(value).to_pydatetime()
            except Exception:
                pass
    return now_ist()


def update_symbol_from_quote(state: LiveState, meta: StockMeta, q: Dict[str, Any]) -> None:
    """Update one symbol's current 1-minute candle using a live quote.

    quote() is not a tick stream. It gives the latest LTP and cumulative day
    volume. Therefore this function approximates the current minute candle using
    consecutive quote snapshots. This is appropriate for a live dashboard, but
    not for exact tick-level backtesting.

    Functional fixes in this version:
        1. If the script is running outside regular cash-market time, it stores
           the quote but does not keep rewriting the last market candle.
        2. The current-minute volume update is now explicit addition of the
           quote-derived volume delta, instead of the previous redundant
           max(current, current + delta) expression.
    """
    symbol = meta.symbol
    ltp = numeric(q.get("last_price"), 0.0)
    if ltp <= 0:
        state.errors[symbol] = "No LTP in quote"
        return

    dashboard_now = now_ist()
    state.last_quote[symbol] = q

    # Outside the regular cash session, do not manufacture new candles from
    # stale last_trade_time values. This keeps the dashboard honest after close
    # and before open while still showing the latest LTP in the title.
    if not is_within_session(dashboard_now):
        return

    q_ts = quote_timestamp_or_now(q)
    if q_ts.date() != market_day():
        # Some quote timestamps may be stale outside market hours. During the
        # live session, use current IST timestamp only for dashboard continuity.
        q_ts = dashboard_now

    if not is_within_session(q_ts):
        return

    minute_ts = floor_to_minute(q_ts)
    cum_volume = integer(q.get("volume"), 0)
    prev_cum = state.last_cum_volume.get(symbol)
    volume_delta = max(0, cum_volume - prev_cum) if prev_cum is not None else 0
    state.last_cum_volume[symbol] = cum_volume
    state.errors.pop(symbol, None)

    df = state.candles.get(symbol)
    if df is None or df.empty:
        new_row = pd.DataFrame(
            [{"date": minute_ts, "open": ltp, "high": ltp, "low": ltp, "close": ltp, "volume": volume_delta}]
        )
        state.candles[symbol] = normalize_candle_df(new_row, day=market_day())
        return

    df = normalize_candle_df(df, day=market_day())
    match = df["date"] == minute_ts

    if match.any():
        idx = df.index[match][-1]
        df.loc[idx, "high"] = max(float(df.loc[idx, "high"]), ltp)
        df.loc[idx, "low"] = min(float(df.loc[idx, "low"]), ltp)
        df.loc[idx, "close"] = ltp
        df.loc[idx, "volume"] = float(df.loc[idx, "volume"]) + float(volume_delta)
    else:
        # New minute. Use LTP as O/H/L/C until additional snapshots arrive.
        new_row = pd.DataFrame(
            [{"date": minute_ts, "open": ltp, "high": ltp, "low": ltp, "close": ltp, "volume": volume_delta}]
        )
        df = pd.concat([df, new_row], ignore_index=True)

    state.candles[symbol] = normalize_candle_df(df, day=market_day())

def poll_watchlist_once(kite: "KiteConnect", selected: Sequence[StockMeta], state: LiveState) -> Tuple[int, Optional[str]]:
    """Poll all fixed-watchlist instruments in one quote() call and update live state."""
    keys = [s.kite_key for s in selected]
    try:
        quotes = kite.quote(keys)  # Exactly one call when len(keys) <= 50.
    except Exception as exc:
        err = f"quote() failed: {exc}"
        for meta in selected:
            state.errors[meta.symbol] = err
        return 0, err

    updated = 0
    for meta in selected:
        q = quotes.get(meta.kite_key)
        if not q:
            state.errors[meta.symbol] = "Missing quote row"
            continue
        update_symbol_from_quote(state, meta, q)
        updated += 1
    return updated, None


# =============================================================================
# Plotting
# =============================================================================
def last_price_from_state(meta: StockMeta, state: LiveState) -> float:
    """Return latest usable price for a symbol."""
    q = state.last_quote.get(meta.symbol, {})
    q_ltp = numeric(q.get("last_price"), 0.0)
    if q_ltp > 0:
        return q_ltp
    df = state.candles.get(meta.symbol)
    if df is not None and not df.empty:
        return float(df["close"].iloc[-1])
    return meta.initial_ltp


def day_change_text(q: Dict[str, Any]) -> str:
    """Return compact day change text from quote payload."""
    ltp = numeric(q.get("last_price"), 0.0)
    ohlc = q.get("ohlc") or {}
    prev_close = numeric(ohlc.get("close"), 0.0)
    if ltp <= 0 or prev_close <= 0:
        return ""
    abs_change = ltp - prev_close
    pct_change = abs_change * 100.0 / prev_close
    sign = "+" if abs_change >= 0 else ""
    return f"{sign}{abs_change:.2f} ({sign}{pct_change:.2f}%)"


def visible_y_range(df: pd.DataFrame, pivots: Optional[PivotLevels], fallback_price: float = 0.0) -> Tuple[float, float]:
    """Compute a readable y-axis range without over-compressing candles.

    When no candles are available yet, use LTP or nearby pivot levels. This
    prevents the browser from showing Plotly's default 0..6/0..4 empty axes.
    """
    if df.empty:
        base = float(fallback_price) if float(fallback_price or 0.0) > 0 else 1.0
        lo = base * 0.995
        hi = base * 1.005
        if pivots is not None:
            for y in [pivots.p, pivots.bc, pivots.tc, pivots.r1, pivots.s1]:
                if abs(float(y) - base) <= max(base * 0.08, 1.0):
                    lo = min(lo, float(y))
                    hi = max(hi, float(y))
        if hi <= lo:
            hi = lo + max(base * 0.01, 1.0)
        span = hi - lo
        return max(0.0, lo - span * 0.08), hi + span * 0.08

    lo = float(df["low"].min())
    hi = float(df["high"].max())

    # Include nearby pivot/CPR levels if they are not absurdly far from price.
    if pivots is not None:
        candle_mid = (lo + hi) / 2.0 if hi > lo else float(df["close"].iloc[-1])
        near_band = max(candle_mid * 0.08, hi - lo, 1.0)
        for y in [pivots.p, pivots.bc, pivots.tc, pivots.r1, pivots.s1]:
            if abs(float(y) - candle_mid) <= near_band:
                lo = min(lo, float(y))
                hi = max(hi, float(y))

    span = max(hi - lo, 0.05)
    pad = max(span * 0.08, max(abs(hi), 1.0) * 0.002)
    return max(0.0, lo - pad), hi + pad


def add_pivot_lines(fig: go.Figure, pivots: Optional[PivotLevels], y_min: float, y_max: float) -> None:
    """Draw CPR/pivot lines on a mini chart.

    User-requested change:
        All pivot/CPR lines are now solid. Earlier versions used dash/dot
        styles for R1/S1 and TC/BC, which made the grid visually noisy and also
        harder to read in the magnified view.

    Labels are intentionally short because 50 mini charts must remain readable.
    """
    if pivots is None:
        return

    # Keep colors differentiated, but make every level a solid line.
    # The fourth tuple value is retained as line_dash for clarity, but it is
    # deliberately always "solid".
    levels = [
        ("R1", pivots.r1, "rgba(183, 28, 28, 0.62)", "solid"),
        ("TC", pivots.tc, "rgba(30, 136, 229, 0.62)", "solid"),
        ("P", pivots.p, "rgba(0, 0, 0, 0.70)", "solid"),
        ("BC", pivots.bc, "rgba(30, 136, 229, 0.62)", "solid"),
        ("S1", pivots.s1, "rgba(27, 94, 32, 0.62)", "solid"),
    ]

    for label, y, color, dash in levels:
        y_float = float(y)
        if y_float < y_min or y_float > y_max:
            continue
        fig.add_hline(
            y=y_float,
            line_width=0.9,
            line_dash=dash,
            line_color=color,
            annotation_text=f"{label} {y_float:.1f}",
            annotation_position="right",
            annotation_font_size=8,
        )

def make_error_figure(title: str, message: str, chart_height: int) -> go.Figure:
    """Render a visible error card instead of leaving a blank Plotly grid."""
    fig = go.Figure()
    fig.add_annotation(
        text=message[:500],
        x=0.5,
        y=0.5,
        xref="paper",
        yref="paper",
        showarrow=False,
        font=dict(size=11),
        align="left",
    )
    fig.update_layout(
        title=dict(text=title, font=dict(size=10), x=0.01, xanchor="left"),
        template="plotly_white",
        height=int(chart_height),
        margin=dict(l=8, r=8, t=34, b=18),
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        paper_bgcolor="white",
        plot_bgcolor="white",
        showlegend=False,
    )
    return fig


def make_stock_figure(
    meta: StockMeta,
    df_full: pd.DataFrame,
    pivots: Optional[PivotLevels],
    state: LiveState,
    chart_height: int,
    max_candles_shown: int,
) -> go.Figure:
    """Create one compact candlestick figure for the 5x10 grid."""
    q = state.last_quote.get(meta.symbol, {})
    df = normalize_candle_df(df_full, day=market_day())
    if max_candles_shown > 0 and len(df) > max_candles_shown:
        df = df.tail(max_candles_shown).copy()

    fig = go.Figure()

    if not df.empty:
        fig.add_trace(
            go.Candlestick(
                x=df["date"],
                open=df["open"],
                high=df["high"],
                low=df["low"],
                close=df["close"],
                name=meta.symbol,
                increasing=dict(line=dict(color="#26a69a", width=0.8), fillcolor="#26a69a"),
                decreasing=dict(line=dict(color="#ef5350", width=0.8), fillcolor="#ef5350"),
                whiskerwidth=0.35,
                hovertemplate=(
                    "<b>%{x|%H:%M}</b><br>"
                    "O %{open:.2f}<br>H %{high:.2f}<br>L %{low:.2f}<br>C %{close:.2f}<extra></extra>"
                ),
            )
        )
        last_close = float(df["close"].iloc[-1])
    else:
        last_close = last_price_from_state(meta, state)
        # Add an invisible point so the chart renders cleanly even before market open.
        fig.add_trace(
            go.Scatter(
                x=[session_start_dt(market_day())],
                y=[last_close if last_close > 0 else 1.0],
                mode="markers",
                marker=dict(size=1, opacity=0),
                hoverinfo="skip",
                showlegend=False,
            )
        )

    y_min, y_max = visible_y_range(df, pivots, fallback_price=last_close)
    add_pivot_lines(fig, pivots, y_min, y_max)

    volume = integer(q.get("volume"), meta.initial_volume)
    change = day_change_text(q)
    error = state.errors.get(meta.symbol, "")
    suffix = f" | Vol {volume:,}"
    if change:
        suffix += f" | {change}"
    if error:
        suffix += " | quote err"

    title = f"#{meta.rank} {meta.symbol}  {last_close:.2f}{suffix}"

    fig.update_layout(
        title=dict(text=title, font=dict(size=10), x=0.01, xanchor="left"),
        template="plotly_white",
        height=int(chart_height),
        margin=dict(l=6, r=42, t=34, b=18),
        showlegend=False,
        hovermode="x",
        dragmode="pan",
        xaxis_rangeslider_visible=False,
        uirevision=f"{meta.symbol}-{market_day()}",
        paper_bgcolor="white",
        plot_bgcolor="white",
        font=dict(size=9),
    )

    fig.update_xaxes(
        showgrid=True,
        gridwidth=0.4,
        gridcolor="rgba(0,0,0,0.06)",
        zeroline=False,
        tickformat="%H:%M",
        nticks=4,
        range=[session_start_dt(market_day()), session_end_dt(market_day())],
    )
    fig.update_yaxes(
        side="right",
        showgrid=True,
        gridwidth=0.4,
        gridcolor="rgba(0,0,0,0.06)",
        zeroline=False,
        nticks=4,
        range=[y_min, y_max],
        tickformat=".1f",
    )
    return fig


# =============================================================================
# Dash application
# =============================================================================
def make_app(
    kite: "KiteConnect",
    selected: Sequence[StockMeta],
    pivots: Dict[str, PivotLevels],
    state: LiveState,
    cache_dir: str,
    refresh_ms: int,
    chart_height: int,
    expanded_chart_height: int,
    max_candles_shown: int,
) -> Dash:
    """Create the Dash app containing a 1 x 5 scrollable chart grid.

    Important design decision in v4:
        Expand/minify is handled entirely in browser-side JavaScript/CSS, not
        through Dash callbacks. This avoids Dash callback-signature/state
        mismatches such as:

            KeyError: Callback function not found for output ...
            IndexError: list index out of range

        The only server-side Dash callback is the one-second refresh callback:
            Input:  live-interval.n_intervals
            Output: status-line.children + all chart figures

        Therefore expanding or minifying a chart does not consume any
        kite.quote() call and cannot break the quote-refresh callback map.
    """
    app = Dash(__name__)
    app.title = "Zerodha Fixed 5 Live Stock Grid"

    # CSS and the tiny expand/minify JavaScript are embedded so that the script
    # stays completely standalone. The JavaScript listens for clicks on buttons
    # with class="expand-btn" and toggles the nearest .chart-card. It does not
    # interact with Dash callbacks.
    index_template = """
    <!DOCTYPE html>
    <html>
        <head>
            {%metas%}
            <title>{%title%}</title>
            {%favicon%}
            {%css%}
            <style>
                html, body {
                    margin: 0;
                    padding: 0;
                    font-family: Arial, sans-serif;
                    background: #f6f7f9;
                }
                .topbar {
                    position: sticky;
                    top: 0;
                    z-index: 999;
                    background: white;
                    border-bottom: 1px solid #ddd;
                    padding: 8px 12px;
                    box-shadow: 0 1px 4px rgba(0,0,0,0.08);
                }
                .title-row {
                    display: flex;
                    align-items: baseline;
                    justify-content: space-between;
                    gap: 12px;
                    flex-wrap: wrap;
                }
                .main-title {
                    font-weight: 700;
                    font-size: 18px;
                }
                .status-line {
                    font-size: 12px;
                    color: #444;
                    margin-top: 3px;
                }
                .grid {
                    display: grid;
                    grid-template-columns: minmax(520px, 1fr);
                    gap: 10px;
                    padding: 10px;
                    align-items: start;
                    max-width: 1500px;
                    margin: 0 auto;
                }
                .chart-card {
                    position: relative;
                    background: white;
                    border: 1px solid #e0e0e0;
                    border-radius: 6px;
                    overflow: hidden;
                    box-shadow: 0 1px 2px rgba(0,0,0,0.05);
                }
                .chart-card .dash-graph {
                    width: 100%;
                }
                .expand-btn {
                    position: absolute;
                    top: 5px;
                    right: 5px;
                    z-index: 20;
                    width: 22px;
                    height: 22px;
                    border-radius: 50%;
                    border: 1px solid #bdbdbd;
                    background: rgba(255,255,255,0.94);
                    color: #111;
                    font-size: 16px;
                    font-weight: 700;
                    line-height: 18px;
                    padding: 0;
                    cursor: pointer;
                    box-shadow: 0 1px 3px rgba(0,0,0,0.18);
                }
                .expand-btn:hover {
                    background: #f1f1f1;
                    border-color: #666;
                }
                .expanded-card {
                    position: fixed;
                    top: 54px;
                    left: 8px;
                    right: 8px;
                    bottom: 8px;
                    z-index: 2000;
                    overflow: auto;
                    border: 2px solid #222;
                    border-radius: 8px;
                    box-shadow: 0 8px 28px rgba(0,0,0,0.34);
                }
                .expanded-card .dash-graph {
                    height: calc(100vh - 72px) !important;
                    min-height: 560px;
                }
                .expanded-card .expand-btn {
                    width: 28px;
                    height: 28px;
                    font-size: 20px;
                    line-height: 22px;
                    right: 8px;
                    top: 8px;
                }
                @media (max-width: 620px) {
                    .grid {
                        grid-template-columns: 1fr;
                        min-width: 0;
                        padding: 6px;
                    }
                }
            </style>
        </head>
        <body>
            {%app_entry%}
            <footer>
                {%config%}
                {%scripts%}
                {%renderer%}
                <script>
                (function () {
                    "use strict";

                    const MINI_HEIGHT = __MINI_CHART_HEIGHT__;
                    const EXPANDED_HEIGHT = __EXPANDED_CHART_HEIGHT__;

                    function getPlotlyDiv(card) {
                        if (!card) return null;
                        return card.querySelector(".js-plotly-plot");
                    }

                    function relayoutCard(card, expanded) {
                        const gd = getPlotlyDiv(card);
                        if (!gd || !window.Plotly) return;
                        const h = expanded ? Math.max(EXPANDED_HEIGHT, window.innerHeight - 82) : MINI_HEIGHT;
                        try {
                            window.Plotly.relayout(gd, {height: h, autosize: true});
                        } catch (e) {
                            // Ignore transient Plotly relayout errors during Dash redraw.
                        }
                    }

                    function collapseOtherCards(activeCard) {
                        document.querySelectorAll(".chart-card.expanded-card").forEach(function (card) {
                            if (card !== activeCard) {
                                card.classList.remove("expanded-card");
                                const btn = card.querySelector(".expand-btn");
                                if (btn) btn.textContent = "+";
                                relayoutCard(card, false);
                            }
                        });
                    }

                    function toggleCard(card) {
                        if (!card) return;
                        const willExpand = !card.classList.contains("expanded-card");
                        collapseOtherCards(card);

                        if (willExpand) {
                            card.classList.add("expanded-card");
                        } else {
                            card.classList.remove("expanded-card");
                        }

                        const btn = card.querySelector(".expand-btn");
                        if (btn) btn.textContent = willExpand ? "−" : "+";

                        // Let the CSS position/height apply, then resize Plotly.
                        setTimeout(function () { relayoutCard(card, willExpand); }, 40);
                        setTimeout(function () { relayoutCard(card, willExpand); }, 250);
                    }

                    function attachExpandHandlers() {
                        document.querySelectorAll(".expand-btn").forEach(function (btn) {
                            if (btn.__zerodhaGridExpandAttached === true) return;
                            btn.__zerodhaGridExpandAttached = true;
                            btn.addEventListener("click", function (event) {
                                event.preventDefault();
                                event.stopPropagation();
                                toggleCard(btn.closest(".chart-card"));
                            });
                        });
                    }

                    function maintainExpandedCharts() {
                        attachExpandHandlers();
                        document.querySelectorAll(".chart-card.expanded-card").forEach(function (card) {
                            const btn = card.querySelector(".expand-btn");
                            if (btn) btn.textContent = "−";
                            relayoutCard(card, true);
                        });
                    }

                    document.addEventListener("DOMContentLoaded", attachExpandHandlers);
                    window.addEventListener("resize", maintainExpandedCharts, {passive: true});

                    // Dash may replace graph internals on every callback. Reattach handlers
                    // and reapply expanded height after redraws without using any Dash callback.
                    setInterval(maintainExpandedCharts, 700);
                })();
                </script>
            </footer>
        </body>
    </html>
    """
    app.index_string = (
        index_template
        .replace("__MINI_CHART_HEIGHT__", str(int(chart_height)))
        .replace("__EXPANDED_CHART_HEIGHT__", str(int(expanded_chart_height)))
    )

    def initial_graph(meta: StockMeta) -> go.Figure:
        """Build the initial chart for one stock before the first callback."""
        return make_stock_figure(
            meta=meta,
            df_full=state.candles.get(meta.symbol, pd.DataFrame()),
            pivots=pivots.get(meta.symbol),
            state=state,
            chart_height=chart_height,
            max_candles_shown=max_candles_shown,
        )

    app.layout = html.Div(
        [
            dcc.Interval(id="live-interval", interval=int(refresh_ms), n_intervals=0),
            html.Div(
                [
                    html.Div(
                        [
                            html.Div("Zerodha Fixed 5 Stock Grid", className="main-title"),
                            html.Div(
                                (
                                    f"{len(selected)} charts | refresh={refresh_ms} ms | "
                                    "one kite.quote() call for all 5 symbols per refresh | "
                                    "click + to expand a chart"
                                ),
                                style={"fontSize": "12px", "color": "#666"},
                            ),
                        ],
                        className="title-row",
                    ),
                    html.Div(id="status-line", className="status-line"),
                ],
                className="topbar",
            ),
            html.Div(
                [
                    html.Div(
                        [
                            html.Button(
                                "+",
                                className="expand-btn",
                                title=f"Expand {safe_file_part(_meta.symbol)}",
                            ),
                            dcc.Graph(
                                id=f"chart-{i}",
                                figure=initial_graph(_meta),
                                config={
                                    "displayModeBar": False,
                                    "displaylogo": False,
                                    "responsive": True,
                                    "scrollZoom": False,
                                },
                                style={"height": f"{int(chart_height)}px"},
                            ),
                        ],
                        id=f"card-{i}",
                        className="chart-card",
                    )
                    for i, _meta in enumerate(selected)
                ],
                className="grid",
            ),
        ]
    )

    outputs = [Output("status-line", "children")] + [Output(f"chart-{i}", "figure") for i in range(len(selected))]

    @app.callback(outputs, Input("live-interval", "n_intervals"))
    def refresh_dashboard(n_intervals: int):
        """Poll quote once, update in-memory candles, and redraw all charts.

        This is the only Dash callback in the app. Expand/minify is client-side
        only and never changes the server callback signature.
        """
        try:
            updated, quote_error = poll_watchlist_once(kite, selected, state)

            # Flush live candles to disk roughly once per minute. This makes
            # restarts faster and avoids writing on every one-second tick.
            ts = time.time()
            if ts - state.last_cache_save_ts >= 60:
                today = market_day()
                for meta in selected:
                    save_cached_candles(cache_dir, meta, today, state.candles.get(meta.symbol, pd.DataFrame()))
                state.last_cache_save_ts = ts

            now_str = now_ist().strftime("%Y-%m-%d %H:%M:%S IST")
            if quote_error:
                status = f"Last refresh: {now_str} | quote error: {quote_error} | showing last available candles"
            else:
                non_empty = sum(1 for meta in selected if not state.candles.get(meta.symbol, pd.DataFrame()).empty)
                status = (
                    f"Last refresh: {now_str} | updated quotes: {updated}/{len(selected)} | "
                    f"charts with candles: {non_empty}/{len(selected)} | fixed watchlist"
                )

            figures = [
                make_stock_figure(
                    meta=meta,
                    df_full=state.candles.get(meta.symbol, pd.DataFrame()),
                    pivots=pivots.get(meta.symbol),
                    state=state,
                    chart_height=chart_height,
                    max_candles_shown=max_candles_shown,
                )
                for meta in selected
            ]
            return [status] + figures

        except Exception as exc:
            tb = traceback.format_exc()
            print("[DASH CALLBACK ERROR]", tb, file=sys.stderr)
            now_str = now_ist().strftime("%Y-%m-%d %H:%M:%S IST")
            status = f"Last refresh: {now_str} | DASH CALLBACK ERROR: {exc}. See PyCharm terminal."
            err_figs = [make_error_figure(f"#{m.rank} {m.symbol}", str(exc), chart_height) for m in selected]
            return [status] + err_figs

    return app


# =============================================================================
# Main
# =============================================================================
def main() -> None:
    """Main entry point."""
    args = parse_args()
    requested_symbols = parse_symbol_list(args.symbols)

    print("[CONFIG] fixed_symbols     =", ", ".join(requested_symbols))
    print("[CONFIG] exchange          =", args.exchange)
    print("[CONFIG] refresh_ms        =", args.refresh_ms)
    print("[CONFIG] cache_dir         =", args.cache_dir)
    print("[CONFIG] chart_height      =", args.chart_height)
    print("[CONFIG] expanded_height   =", args.expanded_chart_height)
    print("[CONFIG] skip_backfill     =", args.skip_backfill)

    if args.symbols_csv:
        print(
            "[INFO] --symbols-csv was supplied but is ignored in this fixed-watchlist version. "
            "Use --symbols to override the five symbols."
        )
    if args.top_n != len(requested_symbols):
        print(
            "[INFO] --top-n is ignored in this fixed-watchlist version. "
            f"Displaying exactly {len(requested_symbols)} requested symbols."
        )

    print("[STEP] Initializing Kite ...")
    kite = init_kite()
    print("[OK] Kite initialized.")

    equity_map = build_nse_equity_map(kite, args.exchange)

    selected = select_fixed_watchlist_stocks(
        kite=kite,
        equity_map=equity_map,
        exchange=args.exchange,
        requested_symbols=requested_symbols,
    )
    if not selected:
        raise RuntimeError("No selected stocks after fixed-watchlist matching.")

    candles = startup_backfill_all(
        kite=kite,
        selected=selected,
        cache_dir=args.cache_dir,
        skip_backfill=bool(args.skip_backfill),
    )

    pivots = fetch_all_pivots(kite, selected)

    non_empty_startup = sum(1 for df in candles.values() if df is not None and not df.empty)
    total_startup_candles = sum(len(df) for df in candles.values() if df is not None)
    print(
        f"[DIAG] Startup candle data: non_empty_symbols={non_empty_startup}/{len(selected)}, "
        f"total_candles={total_startup_candles:,}."
    )
    print(
        "[DIAG] If this is 0 during market hours, historical backfill is not returning data. "
        "The UI will still show LTP-only cards after the first quote refresh."
    )

    state = LiveState(
        candles=candles,
        last_cum_volume={s.symbol: s.initial_volume for s in selected},
        last_quote={},
        errors={},
        last_cache_save_ts=0.0,
    )

    app = make_app(
        kite=kite,
        selected=selected,
        pivots=pivots,
        state=state,
        cache_dir=args.cache_dir,
        refresh_ms=args.refresh_ms,
        chart_height=args.chart_height,
        expanded_chart_height=args.expanded_chart_height,
        max_candles_shown=args.max_candles_shown,
    )

    print("\nOpen this URL in your browser:")
    print(f"http://127.0.0.1:{args.port}")
    print(f"\nLive loop: one kite.quote([{len(selected)} fixed NSE symbols]) call per refresh.")
    print("Grid layout: 1 column x 5 rows. Press Ctrl+C in this terminal to stop.\n")

    # Disable the Flask reloader. Otherwise debug mode can start the script twice,
    # causing duplicate Kite logins/API calls and confusing blank-page symptoms.
    app.run(debug=bool(args.debug), port=int(args.port), host="127.0.0.1", use_reloader=False)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[STOPPED] User interrupted.")
    except Exception as exc:
        print(f"\n[FATAL] {exc}", file=sys.stderr)
        raise
