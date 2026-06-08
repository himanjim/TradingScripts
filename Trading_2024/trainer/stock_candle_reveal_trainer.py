"""
FORCEMOT / KAYNES / IDEA 1-Minute Candle Reveal Trainer
=======================================================

Purpose
-------
Browser-based training tool for reading one of three NSE stock charts one
candle at a time:

    1. FORCEMOT
    2. KAYNES
    3. IDEA

The script uses Zerodha Kite for historical OHLCV data, Dash for the web UI,
and Plotly for a high-quality interactive candlestick chart.

What this version changes
-------------------------
1. Random mode is restricted to exactly three NSE stocks: FORCEMOT, KAYNES, IDEA.
2. The stock selection follows a persistent cycle:
       IDEA -> FORCEMOT -> KAYNES -> IDEA -> ...
   Therefore, if IDEA was shown last, the next random session will use FORCEMOT;
   after FORCEMOT it will use KAYNES; after KAYNES it will return to IDEA.
3. The date is selected randomly from the last 3 years, but dates already shown
   are not repeated. This date non-repetition is global across the three stocks.
4. Pivot/CPR lines are solid, not dashed or dotted.
5. The chart is intentionally narrower and taller, so candles are easier to read
   on a desktop/laptop screen.
6. The top button ribbon is sticky/fixed at the top while the page scrolls.
7. The Right Arrow key is captured at the full-page level.
8. Disk caching is reduced to exactly one pickle file:

       stock_reveal_cache/shown_stock_dates.pkl

   This pickle stores shown stock/date pairs, shown dates, and the last stock in
   the rotation. Candle data, daily data, and Kite instrument metadata are kept
   in memory only during the current run.
9. The chart uses a sleek crosshair mouse cursor.
10. When the mouse is inside the chart area, only a compact price label is shown
    near the mouse pointer. No horizontal/vertical overlay lines are drawn.

Core features
-------------
1. Accepts a specific NSE tradingsymbol through --symbol. In this version, it
   must be one of FORCEMOT, KAYNES, or IDEA.
2. Accepts a specific date through --date in YYYY-MM-DD format.
3. If no stock/date is supplied, the stock is selected by the persistent cycle
   and the date is selected randomly from the last 3 years without date repeat.
4. If only stock is supplied, only the date is randomized.
5. If only date is supplied, only the stock is selected from the cycle.
6. Reveals one new candle per button click or Right Arrow key press.
7. Draws Zerodha-style CPR/pivot levels from the immediately previous trading
   session's daily H/L/C: R2, R1, CPR_upper, P, CPR_lower, S1, S2.
8. Resistance lines are red, support lines are green, and CPR/pivot lines are
   blue/purple.
9. Shows only a compact live mouse-price label near the pointer and uses a
   crosshair cursor for training-style chart reading.

Prerequisites
-------------
pip install pandas numpy pytz plotly dash dash-bootstrap-components dash-extensions kiteconnect

Also required:
- Your existing OptionTradeUtils.py must be importable.
- It must expose intialize_kite_api(), same as your existing Kite scripts.

Examples
--------
Cycle stock + random unused date:
    python stock_candle_reveal_trainer_three_stock_cycle.py

Specific allowed stock + random unused date:
    python stock_candle_reveal_trainer_three_stock_cycle.py --symbol IDEA

Specific allowed stock + specific date:
    python stock_candle_reveal_trainer_three_stock_cycle.py --symbol FORCEMOT --date 2024-04-01

Cycle stock + specific date:
    python stock_candle_reveal_trainer_three_stock_cycle.py --date 2024-04-01

Reset shown stock/date/date-history and cycle state:
    python stock_candle_reveal_trainer_three_stock_cycle.py --reset-shown-cache
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

# Three-stock training universe.
#
# Requirement implemented here:
# - Only these three NSE stocks are used in random/cycle mode:
#     1. FORCEMOT
#     2. KAYNES
#     3. IDEA
# - The stock does not get chosen independently at random every time. Instead,
#   the script remembers the last successfully shown stock and selects the next
#   stock in this persistent rotation:
#
#       IDEA -> FORCEMOT -> KAYNES -> IDEA -> ...
#
# - If there is no cache history yet, the first stock is selected randomly from
#   these three names. From then onward the fixed rotation is followed.
# - Dates are selected randomly from the last 3 years but are not repeated.
#   Date non-repetition is global across the three stocks.
THREE_STOCK_CYCLE: List[str] = ["IDEA", "FORCEMOT", "KAYNES"]
ALLOWED_STOCKS: Set[str] = set(THREE_STOCK_CYCLE)

# Helpful aliases for manual CLI use.
# Add aliases here if your personal naming differs from Kite's NSE symbol.
SYMBOL_ALIASES: Dict[str, str] = {
    "IDEA": "IDEA",
    "VODAFONEIDEA": "IDEA",
    "VODAFONE IDEA": "IDEA",
    "FORCEMOT": "FORCEMOT",
    "FORCE MOTORS": "FORCEMOT",
    "KAYNES": "KAYNES",
}

# All three instruments are NSE cash-market equities.
DEFAULT_EXCHANGE_BY_SYMBOL: Dict[str, str] = {
    "RELIANCE": "NSE",
    "FORCEMOT": "NSE",
    "HDFCBANK": "NSE",
}


# -----------------------------------------------------------------------------
# Data classes
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class StockIdentity:
    """Resolved Kite instrument identity.

    This version is designed for three NSE cash-market stocks only:
    FORCEMOT, KAYNES, and IDEA. `display_name` is kept as a separate field so
    the UI and cache can remain stable even if Kite metadata uses a longer name.
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
        """Stock name stored in the single shown-history pickle.

        The exchange prefix keeps the key explicit and future-proof even though
        this version currently uses only NSE symbols.
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
        description="FORCEMOT/KAYNES/IDEA 1-minute candle reveal trainer using Zerodha Kite."
    )
    parser.add_argument(
        "--stock-id",
        type=int,
        default=None,
        help="Zerodha instrument_token. Use only if you deliberately want to override symbol resolution.",
    )
    parser.add_argument(
        "--symbol",
        type=str,
        default=None,
        help="Allowed NSE tradingsymbol: FORCEMOT, KAYNES, or IDEA.",
    )
    parser.add_argument(
        "--exchange",
        type=str,
        default=None,
        choices=["NSE", "nse"],
        help="Optional exchange for --symbol. This three-stock trainer uses NSE only.",
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

    The function is currently used for the NSE dump. `default_exchange` is
    applied when the dump does not explicitly carry the exchange value.
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
    """Download NSE instrument metadata for the current run only.

    FORCEMOT, KAYNES, and IDEA are NSE cash-market equities. Therefore this
    version downloads only the NSE instrument dump. Nothing is written to disk,
    preserving the single-cache-file design: only shown_stock_dates.pkl is
    persisted.
    """
    print("[FETCH] Downloading Kite NSE instrument dump for this run...")
    raw = kite.instruments("NSE")
    df = normalize_instruments_df(pd.DataFrame(raw), default_exchange="NSE")
    if df.empty:
        raise RuntimeError("Kite returned an empty NSE instrument dump.")
    return df

def canonical_symbol(symbol: str) -> str:
    """Normalize user-entered symbol aliases.

    Examples:
    - VODAFONEIDEA -> IDEA
    - FORCE MOTORS -> FORCEMOT
    - KAYNES       -> KAYNES
    """
    raw = symbol.upper().strip()
    return SYMBOL_ALIASES.get(raw, raw)


def resolve_stock_by_symbol(
    instruments_df: pd.DataFrame,
    symbol: str,
    exchange: Optional[str] = None,
    display_name: str = "",
) -> StockIdentity:
    """Resolve an allowed NSE tradingsymbol to a Kite instrument token.

    Random/cycle mode is restricted to FORCEMOT, KAYNES, and IDEA. Manual
    --symbol use is also restricted to those names or their aliases, so the app
    cannot accidentally show unrelated stocks.
    """
    symbol = canonical_symbol(symbol)
    if symbol not in ALLOWED_STOCKS:
        raise ValueError(
            f"This trainer is restricted to {', '.join(THREE_STOCK_CYCLE)}. "
            f"Received symbol: {symbol}"
        )

    requested_exchange = (exchange or DEFAULT_EXCHANGE_BY_SYMBOL.get(symbol, "NSE")).upper().strip()
    if requested_exchange != "NSE":
        raise ValueError("This three-stock trainer supports only NSE instruments.")

    rows = instruments_df[instruments_df["tradingsymbol"] == symbol].copy()

    if requested_exchange:
        rows = rows[rows["exchange"] == requested_exchange].copy()

    if rows.empty:
        exchange_text = f" on {requested_exchange}" if requested_exchange else ""
        raise ValueError(f"Could not resolve symbol '{symbol}'{exchange_text} from Kite instruments.")

    # All allowed instruments are NSE equities. Still sort defensively in case
    # Kite returns multiple rows after symbol filtering.
    exchange_priority = {"NSE": 0}
    type_priority = {"EQ": 0, "INDEX": 1}

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
        raise ValueError(
            "This trainer is restricted to NSE symbols FORCEMOT, KAYNES, and IDEA. "
            "The supplied token was not found in the NSE instrument dump, so it cannot be validated."
        )

    row = rows.iloc[0]
    symbol = str(row.get("tradingsymbol", f"TOKEN_{token}")).upper().strip()
    exchange = str(row.get("exchange", "") or "").upper().strip()

    if symbol not in ALLOWED_STOCKS or exchange != "NSE":
        raise ValueError(
            f"This trainer is restricted to NSE symbols {', '.join(THREE_STOCK_CYCLE)}. "
            f"Token {token} resolved to {exchange}:{symbol}."
        )

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
            display_name=canonical_symbol(str(args.symbol)),
        )
    return None


# -----------------------------------------------------------------------------
# Single pickle: shown stock/date/date-cycle persistence
# -----------------------------------------------------------------------------
def empty_history_payload() -> Dict[str, object]:
    """Return the normalized in-memory shape of the single cache payload."""
    return {
        "shown_stock_dates": set(),  # Set[Tuple[str, str]], e.g. ("NSE:IDEA", "2024-04-01")
        "shown_dates": set(),        # Set[str], e.g. "2024-04-01"; used for global date non-repeat
        "last_stock": None,          # Last successful cycle stock, e.g. "IDEA"
    }


def normalize_stock_key_for_cache(stock: str) -> str:
    """Normalize stock keys read from old/new cache formats."""
    stock = str(stock).upper().strip()
    if not stock:
        return ""

    # Older/newer versions may store keys as NSE:IDEA. The stock part after the
    # colon is what matters for cycle-state logic.
    if ":" in stock:
        stock = stock.split(":", 1)[1].strip()

    return canonical_symbol(stock)


def load_history_payload() -> Dict[str, object]:
    """Load and normalize the single pickle file.

    Current saved format:
        {
            "shown_stock_dates": [{"stock": "NSE:IDEA", "date": "2024-04-01"}, ...],
            "shown_dates": ["2024-04-01", ...],
            "last_stock": "IDEA"
        }

    Backward compatibility is intentionally included for the older formats used
    by previous trainer versions:
    - set/list of (stock, date) tuples
    - dict with only "shown_stock_dates"

    Date non-repetition is global. Therefore, even if an older cache only has
    stock/date pairs, `shown_dates` is reconstructed from the dates in those
    pairs.
    """
    payload = empty_history_payload()

    if not os.path.exists(SHOWN_CACHE_PATH):
        return payload

    try:
        with open(SHOWN_CACHE_PATH, "rb") as f:
            data = pickle.load(f)
    except Exception as exc:
        print(f"[WARN] Could not read shown history pickle: {exc}")
        return payload

    shown_pairs: Set[Tuple[str, str]] = set()
    shown_dates: Set[str] = set()
    last_stock: Optional[str] = None

    def add_pair(stock_value: object, date_value: object) -> None:
        stock_raw = str(stock_value).upper().strip()
        day = str(date_value).strip()
        if not stock_raw or not day:
            return

        # Preserve explicit exchange in pair keys when present; otherwise use NSE.
        if ":" in stock_raw:
            exchange, raw_symbol = stock_raw.split(":", 1)
            symbol = normalize_stock_key_for_cache(raw_symbol)
            stock_key = f"{exchange.upper().strip()}:{symbol}"
        else:
            symbol = normalize_stock_key_for_cache(stock_raw)
            stock_key = f"NSE:{symbol}" if symbol in ALLOWED_STOCKS else stock_raw

        shown_pairs.add((stock_key, day))
        shown_dates.add(day)

    try:
        if isinstance(data, dict):
            records = data.get("shown_stock_dates", data.get("shown", []))
            for item in records:
                if isinstance(item, dict):
                    add_pair(item.get("stock", item.get("symbol", "")), item.get("date", ""))
                elif isinstance(item, (tuple, list)) and len(item) >= 2:
                    add_pair(item[0], item[1])

            # Explicit shown_dates, if present, is merged with dates reconstructed
            # from shown_stock_dates.
            for day in data.get("shown_dates", []):
                day = str(day).strip()
                if day:
                    shown_dates.add(day)

            raw_last = data.get("last_stock")
            if raw_last:
                candidate = normalize_stock_key_for_cache(str(raw_last))
                if candidate in ALLOWED_STOCKS:
                    last_stock = candidate

        elif isinstance(data, set):
            for item in data:
                if isinstance(item, tuple) and len(item) >= 2:
                    add_pair(item[0], item[1])

        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    add_pair(item.get("stock", item.get("symbol", "")), item.get("date", ""))
                elif isinstance(item, (tuple, list)) and len(item) >= 2:
                    add_pair(item[0], item[1])

    except Exception as exc:
        print(f"[WARN] Could not normalize shown history pickle: {exc}")

    payload["shown_stock_dates"] = shown_pairs
    payload["shown_dates"] = shown_dates
    payload["last_stock"] = last_stock
    return payload


def save_history_payload(payload: Dict[str, object]) -> None:
    """Save the normalized history payload to the single pickle file."""
    shown_pairs = payload.get("shown_stock_dates", set())
    shown_dates = payload.get("shown_dates", set())
    last_stock = payload.get("last_stock")

    records = [
        {"stock": stock, "date": day}
        for stock, day in sorted(shown_pairs, key=lambda x: (x[0], x[1]))
    ]

    clean_payload = {
        "shown_stock_dates": records,
        "shown_dates": sorted(str(d) for d in shown_dates),
        "last_stock": str(last_stock).upper().strip() if last_stock else None,
    }

    tmp_path = SHOWN_CACHE_PATH + ".tmp"
    try:
        with open(tmp_path, "wb") as f:
            pickle.dump(clean_payload, f)
        os.replace(tmp_path, SHOWN_CACHE_PATH)
    except Exception as exc:
        print(f"[WARN] Could not save shown history pickle: {exc}")


def load_shown_combinations() -> Set[Tuple[str, str]]:
    """Load shown stock/date pairs from the single pickle file."""
    return set(load_history_payload().get("shown_stock_dates", set()))


def load_shown_dates() -> Set[str]:
    """Load globally shown dates from the single pickle file."""
    return set(load_history_payload().get("shown_dates", set()))


def load_last_cycle_stock() -> Optional[str]:
    """Load the last successfully shown stock in the IDEA/FORCEMOT/KAYNES cycle."""
    value = load_history_payload().get("last_stock")
    if not value:
        return None
    candidate = normalize_stock_key_for_cache(str(value))
    return candidate if candidate in ALLOWED_STOCKS else None


def mark_combination_shown(selection: SessionSelection, update_last_stock: bool = True) -> None:
    """Mark the current stock/date and date as already shown.

    `shown_dates` is global: once a date is shown for any of the three stocks,
    random mode will not show that date again for any stock.
    """
    payload = load_history_payload()
    shown_pairs: Set[Tuple[str, str]] = set(payload.get("shown_stock_dates", set()))
    shown_dates: Set[str] = set(payload.get("shown_dates", set()))

    shown_pairs.add(selection.key())
    shown_dates.add(selection.session_date.isoformat())

    if update_last_stock:
        stock_symbol = normalize_stock_key_for_cache(selection.stock.display_label)
        if stock_symbol in ALLOWED_STOCKS:
            payload["last_stock"] = stock_symbol

    payload["shown_stock_dates"] = shown_pairs
    payload["shown_dates"] = shown_dates
    save_history_payload(payload)


def reset_shown_cache() -> None:
    """Delete the single shown-history pickle, including cycle state."""
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
def random_candidate_date(excluded_dates: Optional[Set[str]] = None) -> date:
    """Pick a random unused weekday date within the last 3 years.

    `excluded_dates` contains ISO dates that have already been shown. This makes
    date non-repetition global across FORCEMOT, KAYNES, and IDEA.
    """
    excluded_dates = excluded_dates or set()

    today = datetime.now(IST).date()
    latest = today - timedelta(days=1)
    earliest = today - timedelta(days=RANDOM_LOOKBACK_DAYS)

    candidate_pool: List[date] = []
    candidate = earliest
    while candidate <= latest:
        if is_weekday(candidate) and candidate.isoformat() not in excluded_dates:
            candidate_pool.append(candidate)
        candidate += timedelta(days=1)

    if not candidate_pool:
        raise RuntimeError(
            "No unused weekday dates remain in the last 3 years. "
            "Run with --reset-shown-cache if you want to restart training history."
        )

    return random.choice(candidate_pool)


def next_cycle_symbol(last_stock: Optional[str]) -> str:
    """Return the next stock in the persistent three-stock cycle.

    Required sequence:
        IDEA -> FORCEMOT -> KAYNES -> IDEA -> ...

    If there is no previous stock in the cache, the first stock is selected
    randomly from the three-stock universe. After that, the deterministic cycle
    is followed.
    """
    if not last_stock:
        return random.choice(THREE_STOCK_CYCLE)

    last_stock = normalize_stock_key_for_cache(last_stock)
    if last_stock not in THREE_STOCK_CYCLE:
        return random.choice(THREE_STOCK_CYCLE)

    idx = THREE_STOCK_CYCLE.index(last_stock)
    return THREE_STOCK_CYCLE[(idx + 1) % len(THREE_STOCK_CYCLE)]


def cycle_stock_from_universe(instruments_df: pd.DataFrame, last_stock: Optional[str]) -> StockIdentity:
    """Resolve the next stock in the IDEA/FORCEMOT/KAYNES cycle."""
    symbol = next_cycle_symbol(last_stock)
    return resolve_stock_by_symbol(
        instruments_df=instruments_df,
        symbol=symbol,
        exchange="NSE",
        display_name=symbol,
    )



def pick_session_with_constraints(
    kite: "KiteConnect",
    instruments_df: pd.DataFrame,
    fixed_stock: Optional[StockIdentity],
    fixed_date: Optional[date],
) -> Tuple[SessionSelection, pd.DataFrame]:
    """Pick a stock/date pair while respecting provided constraints.

    Behaviour in this three-stock version:
    - If fixed_stock is missing, select the next stock from the persistent cycle.
    - If fixed_date is missing, select a random date from the last 3 years that
      has not been shown before.
    - Dates are not repeated globally in random-date mode.
    - After a successful session, both the stock/date pair and the date are
      stored in the single pickle. The last successful stock updates the cycle.
    """
    if fixed_date is not None:
        validate_requested_date(fixed_date)

    history = load_history_payload()
    shown_pairs: Set[Tuple[str, str]] = set(history.get("shown_stock_dates", set()))
    shown_dates: Set[str] = set(history.get("shown_dates", set()))
    last_stock: Optional[str] = history.get("last_stock") if history.get("last_stock") else None

    # Determine the stock once per session. If the randomly chosen date later
    # turns out to be a holiday/unavailable day, we retry with a different date
    # for the same stock. We do not advance the stock cycle until a valid chart
    # is successfully shown.
    stock = fixed_stock if fixed_stock is not None else cycle_stock_from_universe(instruments_df, last_stock)

    # Exact manual pair. Do not randomize anything. Manual dates are allowed even
    # if already shown, but they are still recorded again in the cache state.
    if fixed_date is not None:
        selection = SessionSelection(stock=stock, session_date=fixed_date)
        df = download_one_day_candles(kite, stock, fixed_date)
        if df.empty:
            raise RuntimeError(f"No candles found for {stock.display_label} on {fixed_date}.")
        mark_combination_shown(selection)
        print(f"[SELECTED] Manual/fixed-date: {stock.display_label} | {fixed_date} | candles={len(df)}")
        return selection, df

    # Random-date mode. Date must not repeat globally. The selected stock remains
    # fixed for this call; only the date changes across attempts.
    attempted_dates: Set[str] = set()
    for attempt in range(1, MAX_RANDOM_ATTEMPTS + 1):
        try:
            session_day = random_candidate_date(excluded_dates=shown_dates | attempted_dates)
        except RuntimeError:
            raise

        attempted_dates.add(session_day.isoformat())
        selection = SessionSelection(stock=stock, session_date=session_day)

        # This check is secondary because shown_dates already blocks date reuse,
        # but it protects against old cache payloads that may have stock/date
        # pairs without a reconstructed shown_dates entry.
        if selection.key() in shown_pairs:
            continue

        df = download_one_day_candles(kite, stock, session_day)
        if df.empty:
            # Exchange holiday or unavailable data. Do not mark the date as
            # shown; simply try another random unused date.
            continue

        mark_combination_shown(selection)
        print(
            f"[SELECTED] Cycle/random-date: {stock.display_label} | {session_day} | "
            f"candles={len(df)} | attempt={attempt}"
        )
        return selection, df

    raise RuntimeError(
        f"Could not find an unused random date with available candles for {stock.display_label}. "
        "Run with --reset-shown-cache or try a specific --date."
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
    app.title = "Three-Stock Candle Reveal Trainer"
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
                            html.H4("FORCEMOT / KAYNES / IDEA Candle Reveal Trainer", className="mb-0"),
                            html.Div(
                                "Press → or click Next. Move mouse over chart to see only the exact price at pointer. Solid red = R1/R2, green = S1/S2.",
                                className="text-muted small",
                            ),
                        ],
                        className="me-auto",
                    ),
                    dbc.Button("Next stock + random date", id="new-random-btn", color="primary", className="me-2"),
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
        """Choose the next cycle stock with a random unused date."""
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
    print("Random mode cycles IDEA -> FORCEMOT -> KAYNES -> IDEA, with globally non-repeated random dates.")
    print(f"Only one disk cache file is used: {SHOWN_CACHE_PATH}\n")

    app.run(debug=True, port=args.port)


if __name__ == "__main__":
    main()
