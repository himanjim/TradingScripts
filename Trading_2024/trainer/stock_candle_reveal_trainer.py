"""
NIFTY/SENSEX 1-Minute Candle Reveal Trainer
========================================

Purpose
-------
Browser-based training tool for reading NIFTY and SENSEX candles one candle at a time.
The script uses Zerodha Kite for historical OHLCV data, Dash for the web UI,
and Plotly for a high-quality interactive candlestick chart.

What this version changes
-------------------------
1. Pivot/CPR lines are solid, not dashed or dotted.
2. The chart is intentionally narrower and taller, so candles are easier to read
   on a desktop/laptop screen.
3. The top button ribbon is sticky/fixed at the top while the page scrolls.
4. The Right Arrow key is captured at the full-page level, not through an empty
   placeholder div. This makes keyboard stepping work after clicking anywhere
   inside the app, including the chart or the buttons.
5. Disk caching is reduced to exactly one pickle file:

       stock_reveal_cache/shown_stock_dates.pkl

   This pickle stores only the stock name and date combinations already shown.
   Candle data, daily data, and Kite instrument metadata are kept in memory only
   during the current run and are not written as many separate cache files.
6. The chart uses a sleek crosshair mouse cursor.
7. When the mouse is inside the chart area, only a compact price label is shown
   near the mouse pointer. No horizontal/vertical overlay lines are drawn.

Core features
-------------
1. Accepts a Zerodha NSE stock instrument token through --stock-id.
2. Accepts an NSE tradingsymbol through --symbol, e.g. RELIANCE.
3. Accepts a specific date through --date in YYYY-MM-DD format.
4. If no stock/date is supplied, randomly picks either NIFTY or SENSEX and a random weekday date within
   the last 3 years.
5. If only stock is supplied, randomizes only the date.
6. If only date is supplied, randomizes only the stock.
7. Avoids repeating already-shown stock/date pairs in random mode.
8. Reveals one new candle per button click or Right Arrow key press.
9. Draws Zerodha-style CPR/pivot levels from the immediately previous trading
   session's daily H/L/C: R2, R1, CPR_upper, P, CPR_lower, S1, S2.
10. Resistance lines are red, support lines are green, and CPR/pivot lines are
    blue/purple.
11. Shows only a compact live mouse-price label near the pointer and uses a
    crosshair cursor for training-style chart reading.

Prerequisites
-------------
pip install pandas numpy pytz plotly dash dash-bootstrap-components dash-extensions kiteconnect

Also required:
- Your existing OptionTradeUtils.py must be importable.
- It must expose intialize_kite_api(), same as your existing Kite scripts.

Examples
--------
Random NIFTY/SENSEX + random date:
    python stock_candle_reveal_trainer_mouse_price_crosshair.py

Specific NSE symbol + specific date:
    python stock_candle_reveal_trainer_mouse_price_crosshair.py --symbol RELIANCE --date 2024-04-01

Specific Zerodha instrument token + specific date:
    python stock_candle_reveal_trainer_mouse_price_crosshair.py --stock-id 738561 --date 2024-04-01

Specific stock, random unused date:
    python stock_candle_reveal_trainer_mouse_price_crosshair.py --symbol RELIANCE

Random stock, specific date:
    python stock_candle_reveal_trainer_mouse_price_crosshair.py --date 2024-04-01

Reset shown stock/date history:
    python stock_candle_reveal_trainer_mouse_price_crosshair.py --reset-shown-cache
"""

from __future__ import annotations

import argparse
import os
import pickle
import random
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta
from typing import Dict, List, Optional, Sequence, Set, Tuple

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import pytz
from dash import Dash, Input, Output, State, ctx, dcc, html, no_update
import dash_bootstrap_components as dbc
from dash_extensions import EventListener

# -----------------------------------------------------------------------------
# Optional imports from your Kite/trading environment
# -----------------------------------------------------------------------------
# Your older scripts sometimes import OptionTradeUtils as a top-level file and
# sometimes from Trading_2024. This two-stage import supports both layouts.
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

# -----------------------------------------------------------------------------
# Global configuration
# -----------------------------------------------------------------------------
IST = pytz.timezone("Asia/Kolkata")

# Kite intervals.
INTERVAL = "minute"
DAILY_INTERVAL = "day"

# NSE regular cash-market session.
TRADING_START = dtime(9, 15)
TRADING_END = dtime(15, 30)

# Random date range. Kite historical availability depends on your Kite plan and
# instrument; this is only the candidate date range.
RANDOM_LOOKBACK_DAYS = 365 * 3
MAX_RANDOM_ATTEMPTS = 220

# First candle visible at app start.
INITIAL_STEP = 1

# Keyboard handling.
# When True, holding down the Right Arrow key will not rapidly skip many candles.
# Each physical press advances only one candle. Set this to False if you want
# key-hold auto-repeat to advance candles quickly.
IGNORE_HELD_KEY_REPEAT = True

# Chart proportions.
# The previous version filled the full browser width, which made the chart too
# wide and compressed vertically. This version constrains width and allocates
# more height.
CHART_MAX_WIDTH_PX = 1080
CHART_HEIGHT_PX = 900
CANDLE_LINE_WIDTH = 1.25

# Sticky top ribbon configuration.
# `position: sticky` keeps the control bar visible while the page scrolls, but
# unlike `position: fixed`, it does not remove the bar from the page layout.
# This prevents the chart/status area from sliding underneath the buttons.
TOP_RIBBON_STYLE = {
    "position": "sticky",
    "top": "0",
    "zIndex": 1100,
    "backgroundColor": "white",
    "boxShadow": "0 1px 4px rgba(0,0,0,0.10)",
}

# Y-axis padding. The y-range uses revealed candles plus prior-day pivot levels,
# not the current day's future high/low.
Y_PADDING_PCT = 0.075
MIN_Y_PADDING_PCT_OF_PRICE = 0.002
INCLUDE_PIVOTS_IN_AUTO_Y_RANGE = True

# Pivot/CPR plotting order from top to bottom in normal situations. Actual price
# order may vary in rare cases, but all lines are horizontal at their prices.
PIVOT_DISPLAY_ORDER = ["R2", "R1", "CPR_upper", "P", "CPR_lower", "S1", "S2"]
PIVOT_LABELS = {
    "R2": "R2",
    "R1": "R1",
    "CPR_upper": "CPR U",
    "P": "P",
    "CPR_lower": "CPR L",
    "S1": "S1",
    "S2": "S2",
}

# Solid colour lines as requested. No dashes/dots for pivot levels.
PIVOT_LINE_DASH = {key: "solid" for key in PIVOT_DISPLAY_ORDER}
PIVOT_LINE_COLOR = {
    "R2": "#d32f2f",          # resistance: red
    "R1": "#d32f2f",          # resistance: red
    "CPR_upper": "#3949ab",   # CPR: blue/purple
    "P": "#1a237e",           # central pivot: darker blue
    "CPR_lower": "#3949ab",   # CPR: blue/purple
    "S1": "#2e7d32",          # support: green
    "S2": "#2e7d32",          # support: green
}
PIVOT_LINE_WIDTH = {
    "R2": 1.2,
    "R1": 1.2,
    "CPR_upper": 1.35,
    "P": 1.6,
    "CPR_lower": 1.35,
    "S1": 1.2,
    "S2": 1.2,
}
CPR_BAND_FILL_COLOR = "rgba(57, 73, 171, 0.055)"

# Single local cache file. No candle-cache files, daily-cache files, or
# instrument-cache files are created in this version.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(BASE_DIR, "stock_reveal_cache")
SHOWN_CACHE_PATH = os.path.join(CACHE_DIR, "shown_stock_dates.pkl")
os.makedirs(CACHE_DIR, exist_ok=True)

# Random training universe.
#
# The requirement now is to show NIFTY and SENSEX on a random basis.
# Therefore the random universe is deliberately restricted to two index
# instruments:
#   1. NSE:NIFTY 50  -> displayed as NIFTY
#   2. BSE:SENSEX    -> displayed as SENSEX
#
# Important implementation detail:
# - NIFTY 50 is an NSE index.
# - SENSEX is a BSE index.
# So the script downloads both NSE and BSE instrument dumps from Kite during the
# current run and resolves the correct token from the correct exchange.
#
# Fallback tokens are used only if the instrument dump does not resolve the
# index. In normal usage, Kite's current instrument dump is the source of truth.
RANDOM_INDEX_UNIVERSE: List[Dict[str, object]] = [
    {
        "display": "NIFTY",
        "symbol": "NIFTY 50",
        "exchange": "NSE",
        "fallback_token": 256265,  # widely used Kite token for NSE NIFTY 50 index
    },
    {
        "display": "SENSEX",
        "symbol": "SENSEX",
        "exchange": "BSE",
        "fallback_token": 265,     # fallback only; current Kite dump is preferred
    },
]

# Helpful aliases for manual CLI use.
# Examples:
#   --symbol NIFTY
#   --symbol NIFTY50
#   --symbol SENSEX
SYMBOL_ALIASES: Dict[str, str] = {
    "NIFTY": "NIFTY 50",
    "NIFTY50": "NIFTY 50",
    "NIFTY 50": "NIFTY 50",
    "SENSEX": "SENSEX",
}

# Default exchange for the two index symbols. This prevents accidental lookup of
# SENSEX in NSE or NIFTY in BSE when --exchange is not supplied.
DEFAULT_EXCHANGE_BY_SYMBOL: Dict[str, str] = {
    "NIFTY 50": "NSE",
    "SENSEX": "BSE",
}


# -----------------------------------------------------------------------------
# Data classes
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class StockIdentity:
    """Resolved Kite instrument identity.

    The earlier version assumed NSE equities only. For NIFTY/SENSEX random
    training we need both exchange and symbol, because:
    - NIFTY 50 belongs to NSE.
    - SENSEX belongs to BSE.

    `display_name` is a clean UI label. For example, the Kite tradingsymbol is
    "NIFTY 50", but the chart can display "NIFTY".
    """

    symbol: str
    token: int
    name: str = ""
    exchange: str = ""
    display_name: str = ""

    @property
    def display_label(self) -> str:
        """Return the label shown in the chart title and status line."""
        return (self.display_name or self.symbol).upper().strip()

    @property
    def cache_stock_name(self) -> str:
        """Stock/index name stored in the single shown-history pickle.

        Include exchange in the key so NSE:NIFTY and BSE:SENSEX remain distinct
        and future symbols with the same name on different exchanges do not
        collide.
        """
        symbol_part = self.display_label
        exchange_part = self.exchange.upper().strip()
        return f"{exchange_part}:{symbol_part}" if exchange_part else symbol_part


@dataclass(frozen=True)
class SessionSelection:
    """One training session: one stock and one trading date."""

    stock: StockIdentity
    session_date: date

    def key(self) -> Tuple[str, str]:
        """Return the exact pair used to avoid repeated random sessions."""
        return self.stock.cache_stock_name, self.session_date.isoformat()

    def to_store(self, step: int = INITIAL_STEP) -> Dict[str, object]:
        """Serialize selection into a Dash dcc.Store-compatible dictionary."""
        return {
            "symbol": self.stock.symbol,
            "token": int(self.stock.token),
            "name": self.stock.name,
            "exchange": self.stock.exchange,
            "display_name": self.stock.display_name,
            "date": self.session_date.isoformat(),
            "step": int(step),
        }


@dataclass(frozen=True)
class PivotLevels:
    """CPR/pivot levels derived from the previous session's daily H/L/C.

    Formulas:
    P  = (H + L + C) / 3
    BC = (H + L) / 2
    TC = 2*P - BC
    R1 = 2*P - L
    S1 = 2*P - H
    R2 = P + (H - L)
    S2 = P - (H - L)

    Important: TC can be below BC. For display, normalize CPR edges as:
    CPR_upper = max(BC, TC)
    CPR_lower = min(BC, TC)
    """

    previous_session_date: date
    previous_high: float
    previous_low: float
    previous_close: float
    p: float
    bc: float
    tc: float
    cpr_upper: float
    cpr_lower: float
    r1: float
    s1: float
    r2: float
    s2: float

    def values_for_plot(self) -> Dict[str, float]:
        """Return plot-ready pivot levels."""
        return {
            "R2": self.r2,
            "R1": self.r1,
            "CPR_upper": self.cpr_upper,
            "P": self.p,
            "CPR_lower": self.cpr_lower,
            "S1": self.s1,
            "S2": self.s2,
        }

    def summary(self) -> str:
        """Compact status text."""
        return (
            f"Prev daily OHLC {self.previous_session_date}: "
            f"H {self.previous_high:.2f}, L {self.previous_low:.2f}, C {self.previous_close:.2f} | "
            f"R2 {self.r2:.2f}, R1 {self.r1:.2f}, CPR U {self.cpr_upper:.2f}, "
            f"P {self.p:.2f}, CPR L {self.cpr_lower:.2f}, S1 {self.s1:.2f}, S2 {self.s2:.2f}"
        )


# -----------------------------------------------------------------------------
# CLI handling
# -----------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    """Parse command-line options."""
    parser = argparse.ArgumentParser(
        description="NIFTY/SENSEX 1-minute candle reveal trainer using Zerodha Kite."
    )
    parser.add_argument(
        "--stock-id",
        type=int,
        default=None,
        help="Zerodha instrument_token. If supplied with --symbol, token takes priority.",
    )
    parser.add_argument(
        "--symbol",
        type=str,
        default=None,
        help="NSE tradingsymbol, e.g. RELIANCE.",
    )
    parser.add_argument(
        "--exchange",
        type=str,
        default=None,
        choices=["NSE", "BSE", "nse", "bse"],
        help=(
            "Optional exchange for --symbol. Use NSE for NIFTY 50/NSE stocks "
            "and BSE for SENSEX. If omitted, the script auto-selects NSE for "
            "NIFTY 50 and BSE for SENSEX."
        ),
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Trading date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("PORT", "8050")),
        help="Dash server port. Default: 8050 or PORT environment variable.",
    )
    parser.add_argument(
        "--reset-shown-cache",
        action="store_true",
        help="Clear the single shown stock/date pickle before starting.",
    )
    return parser.parse_args()


def parse_iso_date(value: str) -> date:
    """Parse YYYY-MM-DD safely."""
    try:
        return datetime.strptime(value.strip(), "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"Invalid date '{value}'. Use YYYY-MM-DD, e.g. 2024-04-01.") from exc


def is_weekday(session_day: date) -> bool:
    """Return True for Monday-Friday calendar days."""
    return session_day.weekday() < 5


def validate_requested_date(session_day: date) -> None:
    """Reject impossible dates before calling Kite."""
    today_ist = datetime.now(IST).date()
    if session_day >= today_ist:
        raise ValueError(
            f"Date {session_day} is not valid for historical training. "
            f"Use a completed past trading date before {today_ist}."
        )
    if not is_weekday(session_day):
        raise ValueError(f"Date {session_day} is a weekend. Use an NSE trading date.")


# -----------------------------------------------------------------------------
# Kite initialization and instrument resolution
# -----------------------------------------------------------------------------
def init_kite() -> "KiteConnect":
    """Initialize Kite through your existing helper."""
    if KiteConnect is None:
        raise RuntimeError("kiteconnect is not installed. Run: pip install kiteconnect")
    if oUtils is None:
        raise RuntimeError(
            "OptionTradeUtils.py is required because this script calls "
            "oUtils.intialize_kite_api()."
        )

    kite = oUtils.intialize_kite_api()
    if kite is None:
        raise RuntimeError("oUtils.intialize_kite_api() returned None. Check Kite authentication.")
    return kite


def normalize_instruments_df(df: pd.DataFrame, default_exchange: str = "") -> pd.DataFrame:
    """Normalize Kite instrument dump columns used by the script.

    The same function is used for both NSE and BSE dumps. `default_exchange`
    is applied when the dump does not explicitly carry the exchange value.
    """
    if df.empty:
        return df

    required_cols = ["tradingsymbol", "instrument_token"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"Kite instrument dump missing columns: {missing}")

    df = df.copy()
    df["tradingsymbol"] = df["tradingsymbol"].astype(str).str.upper().str.strip()
    df["instrument_token"] = pd.to_numeric(df["instrument_token"], errors="coerce").astype("Int64")

    # Make optional fields predictable.
    for col in ["name", "instrument_type", "exchange", "segment"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].astype(str).str.upper().str.strip()

    # Some Kite dumps may have blank exchange values in individual rows. Fill
    # them with the exchange from which the dump was downloaded.
    default_exchange = default_exchange.upper().strip()
    if default_exchange:
        blank_exchange = df["exchange"].isin(["", "NAN", "NONE"])
        df.loc[blank_exchange, "exchange"] = default_exchange

    df = df.dropna(subset=["instrument_token"]).copy()
    return df


def load_market_instruments(kite: "KiteConnect") -> pd.DataFrame:
    """Download NSE and BSE instrument metadata for the current run only.

    The earlier code downloaded only NSE instruments. That is why random mode
    effectively stayed around NIFTY/NSE. SENSEX is on BSE, so we must load both
    exchanges and then resolve each index from its proper exchange.

    No instrument file is written to disk. This preserves the single-cache-file
    design: only shown_stock_dates.pkl is persisted.
    """
    frames: List[pd.DataFrame] = []

    for exchange in ("NSE", "BSE"):
        print(f"[FETCH] Downloading Kite {exchange} instrument dump for this run...")
        raw = kite.instruments(exchange)
        df = normalize_instruments_df(pd.DataFrame(raw), default_exchange=exchange)
        if df.empty:
            print(f"[WARN] Kite returned an empty {exchange} instrument dump.")
            continue
        frames.append(df)

    if not frames:
        raise RuntimeError("Kite returned empty instrument dumps for both NSE and BSE.")

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=["exchange", "tradingsymbol", "instrument_token"])
    return combined


def canonical_symbol(symbol: str) -> str:
    """Normalize user-entered symbol aliases.

    Examples:
    - NIFTY   -> NIFTY 50
    - NIFTY50 -> NIFTY 50
    - SENSEX  -> SENSEX
    """
    raw = symbol.upper().strip()
    return SYMBOL_ALIASES.get(raw, raw)


def resolve_stock_by_symbol(
    instruments_df: pd.DataFrame,
    symbol: str,
    exchange: Optional[str] = None,
    display_name: str = "",
) -> StockIdentity:
    """Resolve a tradingsymbol/index symbol to a Kite instrument token.

    This function supports both:
    - NSE symbols, e.g. NIFTY 50 or RELIANCE
    - BSE symbols, e.g. SENSEX

    For NIFTY/SENSEX, exchange is auto-selected if not supplied.
    For ordinary stocks, NSE is preferred when both NSE and BSE have the symbol.
    """
    symbol = canonical_symbol(symbol)
    requested_exchange = (exchange or DEFAULT_EXCHANGE_BY_SYMBOL.get(symbol, "")).upper().strip()

    rows = instruments_df[instruments_df["tradingsymbol"] == symbol].copy()

    if requested_exchange:
        rows = rows[rows["exchange"] == requested_exchange].copy()

    if rows.empty:
        exchange_text = f" on {requested_exchange}" if requested_exchange else ""
        raise ValueError(f"Could not resolve symbol '{symbol}'{exchange_text} from Kite instruments.")

    # Prioritise in a way that works for both index and stock use:
    # 1. NSE/BSE priority: if no exchange was explicitly supplied, prefer NSE
    #    for ordinary stocks because this trainer is primarily for Indian index/
    #    NSE-style chart reading. SENSEX is protected by DEFAULT_EXCHANGE_BY_SYMBOL.
    # 2. Instrument type: INDEX first for NIFTY/SENSEX, then EQ for stocks.
    exchange_priority = {"NSE": 0, "BSE": 1}
    type_priority = {"INDEX": 0, "EQ": 1}

    rows["_exchange_priority"] = rows["exchange"].map(exchange_priority).fillna(9)
    rows["_type_priority"] = rows["instrument_type"].map(type_priority).fillna(5)
    rows = rows.sort_values(["_exchange_priority", "_type_priority", "instrument_token"])

    row = rows.iloc[0]
    resolved_exchange = str(row.get("exchange", requested_exchange) or requested_exchange).upper().strip()
    resolved_symbol = str(row.get("tradingsymbol", symbol) or symbol).upper().strip()

    return StockIdentity(
        symbol=resolved_symbol,
        token=int(row["instrument_token"]),
        name=str(row.get("name", "") or ""),
        exchange=resolved_exchange,
        display_name=(display_name or resolved_symbol),
    )


def resolve_stock_by_token(instruments_df: pd.DataFrame, token: int) -> StockIdentity:
    """Resolve a Kite token to symbol/exchange/name if present in the dumps.

    If the token is not present, historical_data can still work with the token.
    In that case we create a TOKEN_<id> display identity.
    """
    token = int(token)
    rows = instruments_df[instruments_df["instrument_token"].astype("Int64") == token]

    if rows.empty:
        return StockIdentity(
            symbol=f"TOKEN_{token}",
            token=token,
            name="",
            exchange="",
            display_name=f"TOKEN_{token}",
        )

    row = rows.iloc[0]
    symbol = str(row.get("tradingsymbol", f"TOKEN_{token}")).upper().strip()
    exchange = str(row.get("exchange", "") or "").upper().strip()
    return StockIdentity(
        symbol=symbol,
        token=token,
        name=str(row.get("name", "") or ""),
        exchange=exchange,
        display_name=symbol,
    )


def resolve_requested_stock(args: argparse.Namespace, instruments_df: pd.DataFrame) -> Optional[StockIdentity]:
    """Resolve instrument from CLI args, or return None for random mode."""
    if args.stock_id is not None:
        return resolve_stock_by_token(instruments_df, int(args.stock_id))
    if args.symbol:
        return resolve_stock_by_symbol(
            instruments_df=instruments_df,
            symbol=str(args.symbol),
            exchange=str(args.exchange).upper().strip() if args.exchange else None,
            display_name=str(args.symbol).upper().strip(),
        )
    return None


# -----------------------------------------------------------------------------
# Single pickle: shown stock/date persistence
# -----------------------------------------------------------------------------
def load_shown_combinations() -> Set[Tuple[str, str]]:
    """Load shown stock/date pairs from the single pickle file.

    Current pickle format:
        {"shown_stock_dates": [{"stock": "RELIANCE", "date": "2024-04-01"}, ...]}

    Only stock and date are semantically used. A little backward compatibility is
    included for older set/list tuple formats, but the file is saved back in the
    clean current format.
    """
    if not os.path.exists(SHOWN_CACHE_PATH):
        return set()

    try:
        with open(SHOWN_CACHE_PATH, "rb") as f:
            data = pickle.load(f)

        pairs: Set[Tuple[str, str]] = set()

        if isinstance(data, dict):
            records = data.get("shown_stock_dates", data.get("shown", []))
            for item in records:
                if isinstance(item, dict):
                    stock = str(item.get("stock", item.get("symbol", ""))).upper().strip()
                    day = str(item.get("date", "")).strip()
                    if stock and day:
                        pairs.add((stock, day))
            return pairs

        if isinstance(data, set):
            for item in data:
                if isinstance(item, tuple) and len(item) >= 2:
                    stock_raw, day_raw = item[0], item[1]
                    stock = f"TOKEN_{stock_raw}" if isinstance(stock_raw, int) else str(stock_raw).upper().strip()
                    day = str(day_raw).strip()
                    if stock and day:
                        pairs.add((stock, day))
            return pairs

        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    stock = str(item.get("stock", item.get("symbol", ""))).upper().strip()
                    day = str(item.get("date", "")).strip()
                elif isinstance(item, (tuple, list)) and len(item) >= 2:
                    stock = str(item[0]).upper().strip()
                    day = str(item[1]).strip()
                else:
                    continue
                if stock and day:
                    pairs.add((stock, day))
            return pairs

    except Exception as exc:
        print(f"[WARN] Could not read shown stock/date pickle: {exc}")

    return set()


def save_shown_combinations(shown: Set[Tuple[str, str]]) -> None:
    """Save shown stock/date pairs to the single pickle file."""
    records = [
        {"stock": stock, "date": day}
        for stock, day in sorted(shown, key=lambda x: (x[0], x[1]))
    ]
    payload = {"shown_stock_dates": records}

    tmp_path = SHOWN_CACHE_PATH + ".tmp"
    try:
        with open(tmp_path, "wb") as f:
            pickle.dump(payload, f)
        os.replace(tmp_path, SHOWN_CACHE_PATH)
    except Exception as exc:
        print(f"[WARN] Could not save shown stock/date pickle: {exc}")


def mark_combination_shown(selection: SessionSelection) -> None:
    """Mark the current stock/date as already shown."""
    shown = load_shown_combinations()
    shown.add(selection.key())
    save_shown_combinations(shown)


def reset_shown_cache() -> None:
    """Delete the single shown-history pickle."""
    if os.path.exists(SHOWN_CACHE_PATH):
        os.remove(SHOWN_CACHE_PATH)
    print(f"[CACHE] Reset: {SHOWN_CACHE_PATH}")


# -----------------------------------------------------------------------------
# Historical data download and normalization. No candle/daily disk caching.
# -----------------------------------------------------------------------------
def ist_datetime(session_day: date, t: dtime) -> datetime:
    """Create an IST-aware datetime for Kite historical_data calls."""
    return IST.localize(datetime.combine(session_day, t))


def normalize_intraday_candles(raw: Sequence[dict], session_day: date) -> pd.DataFrame:
    """Normalize Kite 1-minute candle response into a clean IST DataFrame."""
    base_cols = ["date", "open", "high", "low", "close", "volume", "session_date", "session_time"]
    if not raw:
        return pd.DataFrame(columns=base_cols)

    df = pd.DataFrame(raw)
    required = ["date", "open", "high", "low", "close"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"Kite intraday response missing columns: {missing}")

    df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
    df = df.dropna(subset=["date"]).copy()
    df["date"] = df["date"].dt.tz_convert(IST)

    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "volume" not in df.columns:
        df["volume"] = 0

    df = df.dropna(subset=["open", "high", "low", "close"]).copy()
    df = df.sort_values("date").reset_index(drop=True)
    df["session_date"] = df["date"].dt.date
    df["session_time"] = df["date"].dt.time

    # Keep only the requested NSE regular session.
    df = df[
        (df["session_date"] == session_day)
        & (df["session_time"] >= TRADING_START)
        & (df["session_time"] <= TRADING_END)
    ].copy()

    # Deduplicate possible repeated timestamps.
    df = df.drop_duplicates(subset=["date"], keep="last").sort_values("date").reset_index(drop=True)
    return df


def download_one_day_candles(kite: "KiteConnect", stock: StockIdentity, session_day: date) -> pd.DataFrame:
    """Download one stock's one-day 1-minute candles from Kite."""
    from_dt = ist_datetime(session_day, TRADING_START)
    to_dt = ist_datetime(session_day, TRADING_END)

    last_exc: Optional[Exception] = None
    for attempt in range(1, 4):
        try:
            raw = kite.historical_data(
                instrument_token=int(stock.token),
                from_date=from_dt,
                to_date=to_dt,
                interval=INTERVAL,
                continuous=False,
                oi=False,
            )
            df = normalize_intraday_candles(raw, session_day)
            if df.empty:
                print(f"[WARN] No candles for {stock.symbol} on {session_day}. Holiday or unavailable data.")
            return df
        except Exception as exc:
            last_exc = exc
            wait_s = 1 + attempt * 2
            print(
                f"[WARN] historical_data failed for {stock.symbol} {session_day} "
                f"attempt {attempt}/3: {exc}. Retrying in {wait_s}s..."
            )
            time.sleep(wait_s)

    raise RuntimeError(f"Could not download intraday candles after retries: {last_exc}")


def normalize_daily_candles(raw: Sequence[dict]) -> pd.DataFrame:
    """Normalize Kite daily candles into one row per trading session."""
    base_cols = ["date", "open", "high", "low", "close", "volume", "session_date"]
    if not raw:
        return pd.DataFrame(columns=base_cols)

    df = pd.DataFrame(raw)
    required = ["date", "open", "high", "low", "close"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"Kite daily response missing columns: {missing}")

    df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
    df = df.dropna(subset=["date"]).copy()
    df["date"] = df["date"].dt.tz_convert(IST)

    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "volume" not in df.columns:
        df["volume"] = 0

    df = df.dropna(subset=["open", "high", "low", "close"]).copy()
    df["session_date"] = df["date"].dt.date
    df = df.drop_duplicates(subset=["session_date"], keep="last")
    df = df.sort_values("session_date").reset_index(drop=True)
    return df


def download_recent_daily_candles(
    kite: "KiteConnect",
    stock: StockIdentity,
    session_day: date,
    lookback_calendar_days: int = 30,
) -> pd.DataFrame:
    """Download recent daily candles before the selected session.

    We cannot assume the previous calendar day was a trading session. This pulls
    a short daily range and later selects the latest daily candle with date less
    than the selected session date.
    """
    from_day = session_day - timedelta(days=lookback_calendar_days)
    to_day = session_day - timedelta(days=1)
    from_dt = IST.localize(datetime.combine(from_day, dtime(0, 0)))
    to_dt = IST.localize(datetime.combine(to_day, dtime(23, 59)))

    last_exc: Optional[Exception] = None
    for attempt in range(1, 4):
        try:
            raw = kite.historical_data(
                instrument_token=int(stock.token),
                from_date=from_dt,
                to_date=to_dt,
                interval=DAILY_INTERVAL,
                continuous=False,
                oi=False,
            )
            return normalize_daily_candles(raw)
        except Exception as exc:
            last_exc = exc
            wait_s = 1 + attempt * 2
            print(
                f"[WARN] daily historical_data failed for {stock.symbol} before {session_day} "
                f"attempt {attempt}/3: {exc}. Retrying in {wait_s}s..."
            )
            time.sleep(wait_s)

    raise RuntimeError(f"Could not download daily candles after retries: {last_exc}")


# -----------------------------------------------------------------------------
# CPR/pivot calculation
# -----------------------------------------------------------------------------
def compute_pivot_levels_from_previous_session(daily_df: pd.DataFrame, session_day: date) -> Optional[PivotLevels]:
    """Compute CPR/pivots from the immediately previous trading session."""
    if daily_df.empty:
        return None

    prev_rows = daily_df[daily_df["session_date"] < session_day].copy()
    if prev_rows.empty:
        return None

    prev = prev_rows.sort_values("session_date").iloc[-1]
    prev_day = prev["session_date"]
    h = float(prev["high"])
    l = float(prev["low"])
    c = float(prev["close"])

    p = (h + l + c) / 3.0
    bc = (h + l) / 2.0
    tc = 2.0 * p - bc
    r1 = 2.0 * p - l
    s1 = 2.0 * p - h
    r2 = p + (h - l)
    s2 = p - (h - l)

    return PivotLevels(
        previous_session_date=prev_day,
        previous_high=h,
        previous_low=l,
        previous_close=c,
        p=p,
        bc=bc,
        tc=tc,
        cpr_upper=max(bc, tc),
        cpr_lower=min(bc, tc),
        r1=r1,
        s1=s1,
        r2=r2,
        s2=s2,
    )


def get_pivot_levels_for_session(
    kite: "KiteConnect",
    stock: StockIdentity,
    session_day: date,
) -> Optional[PivotLevels]:
    """Fetch daily candles and compute previous-session pivot levels."""
    daily_df = download_recent_daily_candles(kite, stock, session_day)
    return compute_pivot_levels_from_previous_session(daily_df, session_day)


# -----------------------------------------------------------------------------
# Session selection
# -----------------------------------------------------------------------------
def random_candidate_date() -> date:
    """Pick a random weekday date within the last 3 years, excluding today."""
    today = datetime.now(IST).date()
    latest = today - timedelta(days=1)
    earliest = today - timedelta(days=RANDOM_LOOKBACK_DAYS)

    for _ in range(60):
        offset = random.randint(0, (latest - earliest).days)
        candidate = earliest + timedelta(days=offset)
        if is_weekday(candidate):
            return candidate

    # Deterministic fallback.
    candidate = latest
    while candidate >= earliest:
        if is_weekday(candidate):
            return candidate
        candidate -= timedelta(days=1)

    raise RuntimeError("Could not generate a valid weekday date.")


def random_stock_from_universe(instruments_df: pd.DataFrame) -> StockIdentity:
    """Randomly choose either NIFTY or SENSEX and resolve its Kite token.

    The name is kept as `random_stock_from_universe` to minimize changes in the
    rest of the script, but in this version the random universe is index-based,
    not stock-based.
    """
    spec = random.choice(RANDOM_INDEX_UNIVERSE)

    symbol = str(spec["symbol"]).upper().strip()
    exchange = str(spec["exchange"]).upper().strip()
    display = str(spec.get("display", symbol)).upper().strip()

    try:
        return resolve_stock_by_symbol(
            instruments_df=instruments_df,
            symbol=symbol,
            exchange=exchange,
            display_name=display,
        )
    except Exception as exc:
        # Fallback is deliberately last-resort. The current Kite instrument dump
        # should normally resolve both index tokens.
        fallback_token = spec.get("fallback_token")
        if fallback_token is None:
            raise

        print(
            f"[WARN] Could not resolve {exchange}:{symbol} from Kite instruments: {exc}. "
            f"Using fallback token {fallback_token}."
        )
        return StockIdentity(
            symbol=symbol,
            token=int(fallback_token),
            name=display,
            exchange=exchange,
            display_name=display,
        )


def pick_session_with_constraints(
    kite: "KiteConnect",
    instruments_df: pd.DataFrame,
    fixed_stock: Optional[StockIdentity],
    fixed_date: Optional[date],
) -> Tuple[SessionSelection, pd.DataFrame]:
    """Pick a stock/date pair while respecting provided constraints."""
    if fixed_date is not None:
        validate_requested_date(fixed_date)

    # Exact manual pair. Do not randomize anything.
    if fixed_stock is not None and fixed_date is not None:
        selection = SessionSelection(stock=fixed_stock, session_date=fixed_date)
        df = download_one_day_candles(kite, fixed_stock, fixed_date)
        if df.empty:
            raise RuntimeError(f"No candles found for {fixed_stock.symbol} on {fixed_date}.")
        mark_combination_shown(selection)
        print(f"[SELECTED] Manual: {fixed_stock.symbol} | {fixed_date} | candles={len(df)}")
        return selection, df

    shown = load_shown_combinations()

    for attempt in range(1, MAX_RANDOM_ATTEMPTS + 1):
        stock = fixed_stock if fixed_stock is not None else random_stock_from_universe(instruments_df)
        session_day = fixed_date if fixed_date is not None else random_candidate_date()

        selection = SessionSelection(stock=stock, session_date=session_day)
        if selection.key() in shown:
            continue

        df = download_one_day_candles(kite, stock, session_day)
        if df.empty:
            continue

        mark_combination_shown(selection)
        print(
            f"[SELECTED] Randomized: {stock.symbol} | {session_day} | "
            f"candles={len(df)} | attempt={attempt}"
        )
        return selection, df

    fixed_bits = []
    if fixed_stock is not None:
        fixed_bits.append(f"stock={fixed_stock.symbol}")
    if fixed_date is not None:
        fixed_bits.append(f"date={fixed_date}")
    detail = ", ".join(fixed_bits) if fixed_bits else "no fixed stock/date"

    raise RuntimeError(
        f"Could not find an unused random session with available candles ({detail}). "
        "Run with --reset-shown-cache or provide a different stock/date."
    )


def select_start_session(
    kite: "KiteConnect",
    instruments_df: pd.DataFrame,
    args: argparse.Namespace,
) -> Tuple[SessionSelection, pd.DataFrame]:
    """Resolve CLI constraints and select/download the first session."""
    fixed_stock = resolve_requested_stock(args, instruments_df)
    fixed_date = parse_iso_date(args.date) if args.date else None
    return pick_session_with_constraints(kite, instruments_df, fixed_stock, fixed_date)


# -----------------------------------------------------------------------------
# Plotting helpers
# -----------------------------------------------------------------------------
def build_partial_candle_arrays(
    df_full: pd.DataFrame,
    step: int,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Hide candles after `step` while preserving full-day candle spacing."""
    x = df_full["date"].to_numpy()
    open_arr = df_full["open"].to_numpy(dtype=float).astype(object)
    high_arr = df_full["high"].to_numpy(dtype=float).astype(object)
    low_arr = df_full["low"].to_numpy(dtype=float).astype(object)
    close_arr = df_full["close"].to_numpy(dtype=float).astype(object)

    step = max(1, min(int(step), len(df_full)))
    if step < len(df_full):
        open_arr[step:] = None
        high_arr[step:] = None
        low_arr[step:] = None
        close_arr[step:] = None

    return x, open_arr, high_arr, low_arr, close_arr


def visible_y_range(
    df_full: pd.DataFrame,
    step: int,
    pivot_levels: Optional[PivotLevels],
) -> Tuple[float, float]:
    """Compute y-axis range from revealed candles plus prior-day pivots."""
    step = max(1, min(int(step), len(df_full)))
    visible = df_full.iloc[:step]

    y_values: List[float] = [
        float(visible["low"].min()),
        float(visible["high"].max()),
    ]

    if INCLUDE_PIVOTS_IN_AUTO_Y_RANGE and pivot_levels is not None:
        y_values.extend(float(v) for v in pivot_levels.values_for_plot().values())

    lo = min(y_values)
    hi = max(y_values)
    span = max(hi - lo, 1e-9)
    mid_price = max((hi + lo) / 2.0, 1.0)
    pad = max(span * Y_PADDING_PCT, mid_price * MIN_Y_PADDING_PCT_OF_PRICE)

    return lo - pad, hi + pad


def add_cpr_band(fig: go.Figure, pivot_levels: Optional[PivotLevels], x_start: datetime, x_end: datetime) -> None:
    """Add faint CPR band between CPR lower and CPR upper."""
    if pivot_levels is None:
        return
    fig.add_shape(
        type="rect",
        x0=x_start,
        x1=x_end,
        y0=float(pivot_levels.cpr_lower),
        y1=float(pivot_levels.cpr_upper),
        xref="x",
        yref="y",
        line=dict(width=0),
        fillcolor=CPR_BAND_FILL_COLOR,
        layer="below",
    )


def add_pivot_lines(fig: go.Figure, pivot_levels: Optional[PivotLevels], x_start: datetime, x_end: datetime) -> None:
    """Draw solid CPR/pivot lines with right-edge labels."""
    if pivot_levels is None:
        return

    levels = pivot_levels.values_for_plot()
    for key in PIVOT_DISPLAY_ORDER:
        y_value = levels.get(key)
        if y_value is None:
            continue

        y_float = float(y_value)
        label = PIVOT_LABELS.get(key, key)
        color = PIVOT_LINE_COLOR.get(key, "#424242")
        width = PIVOT_LINE_WIDTH.get(key, 1.2)

        fig.add_trace(
            go.Scatter(
                x=[x_start, x_end],
                y=[y_float, y_float],
                mode="lines",
                line=dict(width=width, dash="solid", color=color),
                # Suppress pivot hover tooltips; mouse movement should show only
                # the pointer price label.
                hoverinfo="skip",
                showlegend=False,
                name=label,
            )
        )

        fig.add_annotation(
            x=x_end,
            y=y_float,
            text=f"{label} {y_float:.2f}",
            showarrow=False,
            xanchor="left",
            yanchor="middle",
            font=dict(size=10, color=color),
            bgcolor="rgba(255,255,255,0.82)",
            bordercolor="rgba(0,0,0,0.16)",
            borderwidth=1,
        )


def make_candle_figure(
    df_full: pd.DataFrame,
    selection: SessionSelection,
    step: int,
    pivot_levels: Optional[PivotLevels],
) -> go.Figure:
    """Build the progressive high-quality candlestick figure."""
    fig = go.Figure()
    if df_full.empty:
        fig.update_layout(template="plotly_white", title="No data")
        return fig

    step = max(1, min(int(step), len(df_full)))
    x, open_arr, high_arr, low_arr, close_arr = build_partial_candle_arrays(df_full, step)

    fig.add_trace(
        go.Candlestick(
            x=x,
            open=open_arr,
            high=high_arr,
            low=low_arr,
            close=close_arr,
            name=f"{selection.stock.display_label} 1m",
            increasing=dict(line=dict(color="#26a69a", width=CANDLE_LINE_WIDTH), fillcolor="#26a69a"),
            decreasing=dict(line=dict(color="#ef5350", width=CANDLE_LINE_WIDTH), fillcolor="#ef5350"),
            whiskerwidth=0.45,
            # Suppress Plotly's default OHLC hover tooltip.
            # The custom client-side price label is the only mouse readout.
            hoverinfo="skip",
        )
    )

    day_start = ist_datetime(selection.session_date, TRADING_START)
    day_end = ist_datetime(selection.session_date, TRADING_END)
    y_min, y_max = visible_y_range(df_full, step, pivot_levels)

    last = df_full.iloc[step - 1]
    last_close = float(last["close"])
    last_open = float(last["open"])
    last_high = float(last["high"])
    last_low = float(last["low"])
    last_time = pd.Timestamp(last["date"]).strftime("%H:%M")

    fig.update_layout(
        title=(
            f"{selection.stock.display_label} | {selection.session_date} | "
            f"Candle {step}/{len(df_full)} | {last_time} | "
            f"O {last_open:.2f} H {last_high:.2f} L {last_low:.2f} C {last_close:.2f}"
        ),
        template="plotly_white",
        height=CHART_HEIGHT_PX,
        margin=dict(l=45, r=84, t=58, b=38),
        # Disable Plotly's built-in hover box.
        # We show our own compact mouse-price label instead. This prevents the
        # large OHLC hover overlay from covering the chart while training.
        hovermode=False,
        dragmode="pan",
        xaxis_rangeslider_visible=False,
        uirevision=f"{selection.stock.token}-{selection.session_date}",
        font=dict(size=13),
        plot_bgcolor="white",
        paper_bgcolor="white",
        showlegend=False,
        hoverdistance=60,
        spikedistance=-1,
    )

    fig.update_xaxes(
        title_text="Time (IST)",
        range=[day_start, day_end],
        showgrid=True,
        gridwidth=0.5,
        gridcolor="rgba(0,0,0,0.08)",
        zeroline=False,
        tickformat="%H:%M",
        ticks="outside",
        # Built-in spike lines are disabled because the requirement is to show
        # only the price at the mouse pointer, not extra overlay lines.
        showspikes=False,
    )

    fig.update_yaxes(
        title_text="Price",
        range=[y_min, y_max],
        fixedrange=False,
        showgrid=True,
        gridwidth=0.5,
        gridcolor="rgba(0,0,0,0.08)",
        zeroline=False,
        ticks="outside",
        side="right",
        # Built-in spike lines are disabled because the requirement is to show
        # only the price at the mouse pointer, not extra overlay lines.
        showspikes=False,
    )

    # Prior-day CPR/pivot references.
    add_cpr_band(fig, pivot_levels, day_start, day_end)
    add_pivot_lines(fig, pivot_levels, day_start, day_end)

    # Current price guide remains dotted. It is not a pivot line.
    fig.add_hline(
        y=last_close,
        line_width=1,
        line_dash="dot",
        line_color="rgba(0,0,0,0.45)",
        annotation_text=f"{last_close:.2f}",
        annotation_position="right",
    )

    return fig




def install_custom_mouse_crosshair(app: Dash) -> None:
    """Install a compact mouse-price label and a crosshair cursor.

    Earlier version drew a horizontal line, a vertical line, and a right-axis
    badge. That became visually heavy and could feel like a large overlay.

    This version intentionally does only one thing while the mouse is inside the
    actual Plotly plot area:
    - convert the pointer's y-pixel position into the corresponding stock price,
    - show that price in a small floating label near the mouse pointer,
    - hide the label as soon as the pointer leaves the plot area.

    It does not draw horizontal lines, vertical lines, rectangles, or any other
    overlay. It also disables Plotly's large default hover box through the figure
    layout (`hovermode=False`) in `make_candle_figure()`.

    The code is injected through Dash's HTML template, so no extra JavaScript or
    CSS files are created. The only disk cache used by the script remains the
    shown stock/date pickle.
    """

    app.index_string = """
    <!DOCTYPE html>
    <html>
        <head>
            {%metas%}
            <title>{%title%}</title>
            {%favicon%}
            {%css%}
            <style>
                /*
                   Sleek plus/crosshair cursor on the Plotly chart surface.
                   Plotly uses nested SVG/drag layers, so the rule is applied to
                   the graph container and the common inner plot layers.
                */
                #candle-graph,
                #candle-graph .js-plotly-plot,
                #candle-graph .main-svg,
                #candle-graph .draglayer,
                #candle-graph .nsewdrag {
                    cursor: crosshair !important;
                }

                /*
                   Compact floating price label.
                   This is the only mouse overlay. It follows the pointer and
                   displays the exact price represented by the pointer's vertical
                   position on the y-axis.
                */
                .trainer-mouse-price-label {
                    position: absolute;
                    padding: 2px 6px;
                    border-radius: 4px;
                    background: rgba(20, 20, 20, 0.88);
                    color: #ffffff;
                    font-family: Arial, sans-serif;
                    font-size: 11px;
                    line-height: 16px;
                    text-align: center;
                    pointer-events: none;
                    z-index: 1000;
                    display: none;
                    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.22);
                    white-space: nowrap;
                    user-select: none;
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

                    const GRAPH_ID = "candle-graph";
                    const ATTACH_CHECK_INTERVAL_MS = 700;

                    function formatPrice(value) {
                        if (!isFinite(value)) {
                            return "";
                        }
                        // NSE cash prices are conventionally shown with two decimals.
                        return Number(value).toFixed(2);
                    }

                    function getGraphOuter() {
                        return document.getElementById(GRAPH_ID);
                    }

                    function getPlotlyGraphDiv() {
                        const outer = getGraphOuter();
                        if (!outer) {
                            return null;
                        }
                        return outer.querySelector(".js-plotly-plot");
                    }

                    function ensurePriceLabel(outer) {
                        // Insert one small label into the Dash Graph container.
                        // It is a sibling of Plotly's SVG layers and survives
                        // ordinary Plotly redraws.
                        outer.style.position = "relative";

                        let label = outer.querySelector(".trainer-mouse-price-label");
                        if (!label) {
                            label = document.createElement("div");
                            label.className = "trainer-mouse-price-label";
                            outer.appendChild(label);
                        }
                        return label;
                    }

                    function hidePriceLabel(outer) {
                        if (!outer) {
                            return;
                        }
                        const label = outer.querySelector(".trainer-mouse-price-label");
                        if (label) {
                            label.style.display = "none";
                        }
                    }

                    function clamp(value, minValue, maxValue) {
                        return Math.max(minValue, Math.min(maxValue, value));
                    }

                    function attachMousePriceLabel() {
                        const outer = getGraphOuter();
                        const gd = getPlotlyGraphDiv();

                        if (!outer || !gd || !gd._fullLayout) {
                            return false;
                        }

                        // Dash/Plotly may rebuild the inner graph div after a
                        // figure update. Attach once per actual Plotly graph div.
                        if (gd.__trainerMousePriceOnlyAttached === true) {
                            return true;
                        }
                        gd.__trainerMousePriceOnlyAttached = true;

                        ensurePriceLabel(outer);

                        gd.addEventListener("mousemove", function (event) {
                            const fullLayout = gd._fullLayout;
                            if (!fullLayout || !fullLayout.yaxis || !fullLayout._size) {
                                hidePriceLabel(outer);
                                return;
                            }

                            const size = fullLayout._size;
                            const graphRect = gd.getBoundingClientRect();
                            const outerRect = outer.getBoundingClientRect();

                            // Pointer coordinates inside the Plotly plotting rectangle.
                            const xInPlot = event.clientX - graphRect.left - size.l;
                            const yInPlot = event.clientY - graphRect.top - size.t;

                            // Do not show anything over the title, axes, margins, or modebar.
                            if (xInPlot < 0 || xInPlot > size.w || yInPlot < 0 || yInPlot > size.h) {
                                hidePriceLabel(outer);
                                return;
                            }

                            let price;
                            try {
                                // Plotly yaxis.p2d converts plot-area y-pixel into data price.
                                price = fullLayout.yaxis.p2d(yInPlot);
                            } catch (e) {
                                hidePriceLabel(outer);
                                return;
                            }

                            if (!isFinite(price)) {
                                hidePriceLabel(outer);
                                return;
                            }

                            const label = ensurePriceLabel(outer);
                            label.textContent = formatPrice(price);

                            // Position the label close to the pointer. It is deliberately
                            // not attached to the right axis, because that felt like a large
                            // overlay in the earlier version.
                            const labelWidth = 64;
                            const labelHeight = 20;
                            const mouseX = event.clientX - outerRect.left;
                            const mouseY = event.clientY - outerRect.top;

                            let left = mouseX + 10;
                            let top = mouseY - 10;

                            // Keep the label inside the graph container.
                            left = clamp(left, 2, outerRect.width - labelWidth - 2);
                            top = clamp(top, 2, outerRect.height - labelHeight - 2);

                            label.style.left = left + "px";
                            label.style.top = top + "px";
                            label.style.display = "block";
                        });

                        gd.addEventListener("mouseleave", function () {
                            hidePriceLabel(outer);
                        });

                        // Hide the label during scroll/resize/relayout so it never
                        // remains at a stale screen position.
                        window.addEventListener("scroll", function () { hidePriceLabel(outer); }, { passive: true });
                        window.addEventListener("resize", function () { hidePriceLabel(outer); }, { passive: true });

                        if (gd.on) {
                            gd.on("plotly_relayout", function () { hidePriceLabel(outer); });
                            gd.on("plotly_afterplot", function () { ensurePriceLabel(outer); });
                        }

                        return true;
                    }

                    document.addEventListener("DOMContentLoaded", function () {
                        attachMousePriceLabel();
                    });

                    // Dash updates can replace the inner Plotly graph. A light
                    // periodic check keeps the label active after candle advances
                    // and new random stock/day selections.
                    setInterval(attachMousePriceLabel, ATTACH_CHECK_INTERVAL_MS);
                })();
                </script>
            </footer>
        </body>
    </html>
    """


# -----------------------------------------------------------------------------
# Dash application
# -----------------------------------------------------------------------------
def make_app(
    kite: "KiteConnect",
    instruments_df: pd.DataFrame,
    initial_selection: SessionSelection,
    initial_df: pd.DataFrame,
) -> Dash:
    """Create the Dash app.

    Candle data and pivot data are cached only in memory while the app is open.
    Nothing except shown_stock_dates.pkl is written to disk.
    """
    candle_data_by_key: Dict[Tuple[str, str], pd.DataFrame] = {initial_selection.key(): initial_df}
    pivot_levels_by_key: Dict[Tuple[str, str], Optional[PivotLevels]] = {}

    def selection_from_store(store: Dict[str, object]) -> SessionSelection:
        """Deserialize Dash store into SessionSelection."""
        stock = StockIdentity(
            symbol=str(store["symbol"]),
            token=int(store["token"]),
            name=str(store.get("name", "") or ""),
            exchange=str(store.get("exchange", "") or ""),
            display_name=str(store.get("display_name", "") or ""),
        )
        return SessionSelection(stock=stock, session_date=parse_iso_date(str(store["date"])))

    def get_df_for_selection(selection: SessionSelection) -> pd.DataFrame:
        """Get intraday candles from memory or Kite."""
        key = selection.key()
        if key not in candle_data_by_key:
            candle_data_by_key[key] = download_one_day_candles(
                kite=kite,
                stock=selection.stock,
                session_day=selection.session_date,
            )
        return candle_data_by_key[key]

    def get_pivots_for_selection(selection: SessionSelection) -> Optional[PivotLevels]:
        """Get pivot levels from memory or Kite daily data."""
        key = selection.key()
        if key not in pivot_levels_by_key:
            pivot_levels_by_key[key] = get_pivot_levels_for_session(
                kite=kite,
                stock=selection.stock,
                session_day=selection.session_date,
            )
        return pivot_levels_by_key[key]

    app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
    app.title = "NIFTY/SENSEX Candle Reveal Trainer"
    install_custom_mouse_crosshair(app)

    # Keyboard event listener.
    #
    # The earlier pattern used EventListener around an empty placeholder div.
    # That is unreliable because keydown events only bubble through the element
    # that currently has browser focus. An empty placeholder almost never has
    # focus, so the callback may not fire.
    #
    # The corrected pattern wraps the whole app page inside EventListener. A
    # tiny client-side callback focuses the wrapper after the page loads, so the
    # Right Arrow key works without first clicking the page. If the user later
    # clicks the chart or buttons, keydown events still bubble through the same
    # wrapper to the listener.
    keyboard_events = [
        {
            "event": "keydown",
            "props": ["key", "code", "repeat", "ctrlKey", "altKey", "metaKey", "shiftKey"],
        }
    ]

    page = dbc.Container(
        [
            dcc.Store(id="session-store", data=initial_selection.to_store(step=INITIAL_STEP)),

            # Used only by a tiny client-side callback that focuses the
            # keyboard-capture wrapper after the page loads and after session
            # changes. This is necessary because Dash html.Div does not accept
            # an autoFocus prop in Dash 3.x.
            dcc.Store(id="keyboard-focus-store"),
            dcc.Interval(
                id="keyboard-focus-interval",
                interval=350,
                n_intervals=0,
                max_intervals=1,
            ),

            html.Div(
                [
                    html.Div(
                        [
                            html.H4("NIFTY/SENSEX 1-Minute Candle Reveal Trainer", className="mb-0"),
                            html.Div(
                                "Press → or click Next. Move mouse over chart to see only the exact price at pointer. Solid red = R1/R2, green = S1/S2.",
                                className="text-muted small",
                            ),
                        ],
                        className="me-auto",
                    ),
                    dbc.Button("New random NIFTY/SENSEX day", id="new-random-btn", color="primary", className="me-2"),
                    dbc.Button("Next candle (→)", id="next-btn", color="success", className="me-2"),
                    dbc.Button("Reset to first candle", id="reset-step-btn", color="secondary", outline=True),
                ],
                className="d-flex align-items-center flex-wrap gap-2 px-3 py-2 border-bottom",
                # Keep this controls ribbon visible while scrolling through the
                # tall chart. The buttons remain accessible at all times.
                style=TOP_RIBBON_STYLE,
            ),
            html.Div(
                id="status-line",
                className="px-3 py-2 fw-semibold",
                style={"maxWidth": f"{CHART_MAX_WIDTH_PX}px", "margin": "0 auto"},
            ),
            html.Div(
                dcc.Graph(
                    id="candle-graph",
                    config={
                        "displayModeBar": True,
                        "displaylogo": False,
                        "responsive": True,
                        "scrollZoom": True,
                        "modeBarButtonsToRemove": ["select2d", "lasso2d"],
                    },
                    style={
                        "height": "calc(100vh - 132px)",
                        "minHeight": "760px",
                        "width": "100%",
                        "cursor": "crosshair",
                    },
                ),
                style={
                    "maxWidth": f"{CHART_MAX_WIDTH_PX}px",
                    "margin": "0 auto",
                    "padding": "0 8px 12px 8px",
                },
            ),
        ],
        fluid=True,
        className="p-0",
    )

    app.layout = EventListener(
        id="key-listener",
        events=keyboard_events,
        # Full-page focusable wrapper. tabIndex makes the div keyboard-focusable.
        # Do NOT use autoFocus here: Dash html.Div does not support that prop in
        # Dash 3.x and raises: unexpected keyword argument 'autoFocus'.
        # A small client-side callback below focuses this wrapper safely.
        children=html.Div(
            page,
            id="keyboard-capture-root",
            tabIndex=0,
            style={"outline": "none", "minHeight": "100vh"},
        ),
    )

    # Focus the keyboard wrapper from JavaScript.
    #
    # Why this is needed:
    # - Browser keydown events are delivered to the currently focused element.
    # - A plain html.Div can be focusable only when tabIndex is set.
    # - Dash 3.x does not allow autoFocus on html.Div, so the focus must be
    #   requested through a client-side callback.
    #
    # The callback runs once shortly after page load and again whenever the
    # session-store changes. The second trigger keeps keyboard navigation alive
    # after changing session or revealing candles, without requiring the user to
    # click the page first.
    app.clientside_callback(
        """
        function(n_intervals, session_data) {
            const el = document.getElementById('keyboard-capture-root');
            if (!el) {
                return window.dash_clientside.no_update;
            }

            const active = document.activeElement;
            const tag = active && active.tagName ? active.tagName.toUpperCase() : '';

            // Never steal focus from text-entry controls. This script currently
            // has no text inputs in the layout, but this guard keeps future
            // changes safe.
            if (['INPUT', 'TEXTAREA', 'SELECT'].includes(tag)) {
                return Date.now();
            }

            try {
                el.focus({preventScroll: true});
            } catch (e) {
                el.focus();
            }
            return Date.now();
        }
        """,
        Output("keyboard-focus-store", "data"),
        Input("keyboard-focus-interval", "n_intervals"),
        Input("session-store", "data"),
    )

    @app.callback(
        Output("session-store", "data"),
        Input("new-random-btn", "n_clicks"),
        prevent_initial_call=True,
    )
    def choose_new_random(_: Optional[int]) -> Dict[str, object]:
        """Choose a completely new random stock/date session."""
        selection, df = pick_session_with_constraints(
            kite=kite,
            instruments_df=instruments_df,
            fixed_stock=None,
            fixed_date=None,
        )
        candle_data_by_key[selection.key()] = df
        return selection.to_store(step=INITIAL_STEP)

    @app.callback(
        Output("session-store", "data", allow_duplicate=True),
        Input("next-btn", "n_clicks"),
        Input("key-listener", "n_events"),
        Input("reset-step-btn", "n_clicks"),
        State("key-listener", "event"),
        State("session-store", "data"),
        prevent_initial_call=True,
    )
    def advance_or_reset(
        next_clicks: Optional[int],
        key_events: Optional[int],
        reset_clicks: Optional[int],
        last_key_event: Optional[Dict[str, object]],
        store: Dict[str, object],
    ) -> Dict[str, object]:
        """Advance one candle, handle Right Arrow, or reset to candle 1."""
        del next_clicks, key_events, reset_clicks

        if not store:
            return no_update

        trigger = ctx.triggered_id
        new_store = dict(store)

        if trigger == "reset-step-btn":
            new_store["step"] = INITIAL_STEP
            return new_store

        if trigger == "key-listener":
            event = last_key_event or {}
            key = event.get("key")
            code = event.get("code")

            # Modern browsers report key="ArrowRight" and code="ArrowRight".
            # Some older browser/event wrappers may report key="Right".
            is_right_arrow = key in ("ArrowRight", "Right") or code == "ArrowRight"
            if not is_right_arrow:
                return no_update

            # Optional safety: do not reveal many candles if the key is held
            # down and the browser starts sending repeated keydown events.
            if IGNORE_HELD_KEY_REPEAT and bool(event.get("repeat", False)):
                return no_update

        if trigger not in ("next-btn", "key-listener"):
            return no_update

        selection = selection_from_store(store)
        df = get_df_for_selection(selection)
        if df.empty:
            return no_update

        current_step = int(store.get("step", INITIAL_STEP))
        new_store["step"] = min(current_step + 1, len(df))
        return new_store

    @app.callback(
        Output("status-line", "children"),
        Output("candle-graph", "figure"),
        Input("session-store", "data"),
    )
    def render_chart(store: Dict[str, object]) -> Tuple[str, go.Figure]:
        """Render status text and chart."""
        if not store:
            return "No stock/date selected.", go.Figure()

        selection = selection_from_store(store)
        df = get_df_for_selection(selection)
        if df.empty:
            return f"No candles found for {selection.stock.display_label} on {selection.session_date}.", go.Figure()

        step = max(1, min(int(store.get("step", INITIAL_STEP)), len(df)))
        pivot_levels = get_pivots_for_selection(selection)
        fig = make_candle_figure(df, selection, step, pivot_levels)

        first_ts = pd.Timestamp(df["date"].iloc[0]).strftime("%H:%M")
        last_ts = pd.Timestamp(df["date"].iloc[-1]).strftime("%H:%M")
        stock_name = f" - {selection.stock.name}" if selection.stock.name else ""
        exchange_text = f" | Exchange: {selection.stock.exchange}" if selection.stock.exchange else ""
        pivot_text = pivot_levels.summary() if pivot_levels else "Pivots unavailable: previous daily candle not found"

        status = (
            f"Instrument: {selection.stock.display_label}{stock_name}{exchange_text} | "
            f"Token: {selection.stock.token} | Date: {selection.session_date} | "
            f"Candles shown: {step}/{len(df)} | Session: {first_ts}-{last_ts} IST | "
            f"{pivot_text}"
        )
        return status, fig

    return app


# -----------------------------------------------------------------------------
# Main entry point
# -----------------------------------------------------------------------------
def main() -> None:
    """Run the trainer."""
    args = parse_args()

    if args.reset_shown_cache:
        reset_shown_cache()

    kite = init_kite()
    instruments_df = load_market_instruments(kite)
    selection, df = select_start_session(kite, instruments_df, args)

    app = make_app(kite, instruments_df, selection, df)

    print("\nOpen this URL in your browser:")
    print(f"http://127.0.0.1:{args.port}")
    print("\nControls: click 'Next candle' or press the Right Arrow key. Move mouse over chart to show only the exact price at pointer.")
    print("Random mode now alternates between NSE:NIFTY 50 and BSE:SENSEX.")
    print(f"Only one disk cache file is used: {SHOWN_CACHE_PATH}\n")

    app.run(debug=True, port=args.port)


if __name__ == "__main__":
    main()
