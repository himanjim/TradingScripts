"""
Option ATM Candle Reveal Trainer
================================

Purpose
-------
Browser-based 1-minute candle reveal trainer for historical NIFTY/SENSEX
options already saved as pickle files.

This script is intentionally modelled on your existing stock candle trainer:
- Dash + Plotly candlestick chart.
- One-candle-at-a-time reveal.
- Right Arrow key support.
- Sleek crosshair cursor.
- Compact mouse-price label only, without heavy hover overlays.

Key difference from the stock trainer
-------------------------------------
Instead of downloading stock candles directly from Kite, this script reads
option candles from the pickle folder used by your backtesting script:

    G:\\My Drive\\Trading\\Historical_Options_Data

Selection logic
---------------
For a selected/random date and underlying:

1. Use only NIFTY and SENSEX.
2. Read the available option pickles.
3. Determine the nearest valid expiry for that underlying and date.
4. Fetch the underlying's 1-minute candles from Kite only to get the
   underlying opening price for that date.
5. Round the opening price to the nearest ATM strike:
       NIFTY  -> nearest 50
       SENSEX -> nearest 100
6. Choose the ATM option at the nearest expiry.
7. Show either CE, PE, or a random side, controlled by --option-type.

Assumption resolved in code
---------------------------
The prompt says "the option chosen" but does not specify CE or PE. Therefore,
the default is --option-type RANDOM. You can force CE or PE from the command line.

Prerequisites
-------------
pip install pandas numpy pytz plotly dash dash-bootstrap-components dash-extensions kiteconnect

Also required:
- Trading_2024.OptionTradeUtils or OptionTradeUtils must be importable.
- It must expose intialize_kite_api(), same as your existing scripts.

Examples
--------
Random NIFTY/SENSEX, random date, random CE/PE:
    python option_atm_candle_reveal_trainer.py

Random NIFTY only:
    python option_atm_candle_reveal_trainer.py --underlying NIFTY

Specific date, random NIFTY/SENSEX:
    python option_atm_candle_reveal_trainer.py --date 2024-04-01

Specific date + underlying + CE:
    python option_atm_candle_reveal_trainer.py --underlying NIFTY --date 2024-04-01 --option-type CE

Use a different pickle folder:
    python option_atm_candle_reveal_trainer.py --pickles-dir "D:\\OptionsData"

Reset already-shown history:
    python option_atm_candle_reveal_trainer.py --reset-shown-cache
"""

from __future__ import annotations

import argparse
import glob
import os
import pickle
import random
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dtime
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import pytz
from dash import Dash, Input, Output, State, ctx, dcc, html, no_update
import dash_bootstrap_components as dbc
from dash_extensions import EventListener


# =============================================================================
# Optional imports from your existing trading environment
# =============================================================================
try:
    import Trading_2024 as oUtils
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
# USER CONFIG
# =============================================================================
IST = pytz.timezone("Asia/Kolkata")

# Same default folder as the attached atm_straddle_prem_jump_reattempt.py.
DEFAULT_PICKLES_DIR = r"G:\My Drive\Trading\Historical_Options_Data"

# Index option session timing.
SESSION_START_IST = dtime(9, 15)
SESSION_END_IST = dtime(15, 30)

# Only these two underlyings are allowed, as requested.
TRADEABLE_UNDERLYINGS = {"NIFTY", "SENSEX"}

# ATM strike rounding used in your backtesting script.
STRIKE_STEP = {"NIFTY": 50, "SENSEX": 100}

# Kite symbols used only to fetch the underlying opening price.
UNDERLYING_KITE = {
    "NIFTY": {"exchange": "NSE", "tradingsymbol": "NIFTY 50"},
    "SENSEX": {"exchange": "BSE", "tradingsymbol": "SENSEX"},
}

# Chart layout.
INITIAL_STEP = 1
CHART_MAX_WIDTH_PX = 1100
CHART_HEIGHT_PX = 900
CANDLE_LINE_WIDTH = 1.25
Y_PADDING_PCT = 0.075
MIN_Y_PADDING_PCT_OF_PRICE = 0.01

# Keyboard handling.
IGNORE_HELD_KEY_REPEAT = True

# Random-search protection.
MAX_RANDOM_ATTEMPTS = 350

# Local cache: only shown selections. No candle data is written to disk.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(BASE_DIR, "option_reveal_cache")
SHOWN_CACHE_PATH = os.path.join(CACHE_DIR, "shown_option_sessions.pkl")
os.makedirs(CACHE_DIR, exist_ok=True)

TOP_RIBBON_STYLE = {
    "position": "sticky",
    "top": "0",
    "zIndex": 1100,
    "backgroundColor": "white",
    "boxShadow": "0 1px 4px rgba(0,0,0,0.10)",
}


# =============================================================================
# Data classes
# =============================================================================
@dataclass(frozen=True)
class OptionSelection:
    """One chart session: one ATM option for one underlying/date/expiry."""

    underlying: str
    session_date: date
    expiry_date: date
    days_to_expiry: int
    underlying_open: float
    underlying_open_time: str
    atm_strike: int
    option_type: str
    instrument: str
    source_files: Tuple[str, ...]

    def key(self) -> Tuple[str, str, str, int, str, str]:
        """Stable key used to avoid repeating the same trainer session."""
        return (
            self.underlying,
            self.session_date.isoformat(),
            self.expiry_date.isoformat(),
            int(self.atm_strike),
            self.option_type,
            self.instrument,
        )

    def to_store(self, step: int = INITIAL_STEP) -> Dict[str, object]:
        """Serialize selection for Dash dcc.Store."""
        return {
            "underlying": self.underlying,
            "session_date": self.session_date.isoformat(),
            "expiry_date": self.expiry_date.isoformat(),
            "days_to_expiry": int(self.days_to_expiry),
            "underlying_open": float(self.underlying_open),
            "underlying_open_time": self.underlying_open_time,
            "atm_strike": int(self.atm_strike),
            "option_type": self.option_type,
            "instrument": self.instrument,
            "source_files": list(self.source_files),
            "step": int(step),
        }


@dataclass
class Catalog:
    """Metadata discovered from the option pickle folder.

    min_expiry_map:
        (underlying, trading_day) -> nearest expiry date

    paths_by_key:
        (underlying, trading_day, expiry_date) -> pickle paths that contain
        data for that group
    """

    min_day_seen: date
    max_day_seen: date
    min_expiry_map: Dict[Tuple[str, date], date]
    paths_by_key: Dict[Tuple[str, date, date], Set[str]]

    def candidates(
        self,
        fixed_underlying: Optional[str],
        fixed_date: Optional[date],
    ) -> List[Tuple[str, date, date]]:
        """Return candidate (underlying, day, nearest_expiry) rows."""
        out: List[Tuple[str, date, date]] = []
        for (und, dy), ex in self.min_expiry_map.items():
            if fixed_underlying is not None and und != fixed_underlying:
                continue
            if fixed_date is not None and dy != fixed_date:
                continue
            out.append((und, dy, ex))
        return out


# =============================================================================
# CLI
# =============================================================================
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Dash candle reveal trainer for ATM NIFTY/SENSEX options from local pickle files."
    )
    parser.add_argument(
        "--pickles-dir",
        type=str,
        default=os.getenv("PICKLES_DIR", DEFAULT_PICKLES_DIR),
        help=f"Folder containing .pkl/.pickle option files. Default: {DEFAULT_PICKLES_DIR}",
    )
    parser.add_argument(
        "--underlying",
        type=str,
        default=None,
        choices=["NIFTY", "SENSEX"],
        help="Optional fixed underlying. If omitted, random NIFTY/SENSEX is used.",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Optional fixed trading date in YYYY-MM-DD format. If omitted, random date is used.",
    )
    parser.add_argument(
        "--option-type",
        type=str,
        default="RANDOM",
        choices=["CE", "PE", "RANDOM"],
        help="Which ATM option side to show. Default: RANDOM.",
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
        help="Clear already-shown option sessions before starting.",
    )
    parser.add_argument(
        "--fail-on-pickle-error",
        action="store_true",
        help="Stop immediately if any pickle has missing/bad columns. Default skips bad files with warnings.",
    )
    return parser.parse_args()


def parse_iso_date(value: str) -> date:
    """Parse YYYY-MM-DD safely."""
    try:
        return datetime.strptime(value.strip(), "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"Invalid date '{value}'. Use YYYY-MM-DD, e.g. 2024-04-01.") from exc


# =============================================================================
# Timezone and data normalization helpers
# =============================================================================
def ist_tz():
    """Return pytz timezone used consistently across the script."""
    return IST


def ensure_ist(series_or_scalar) -> Any:
    """Convert a pandas Series/scalar datetime to Asia/Kolkata.

    Pickles may store timezone-aware or timezone-naive timestamps. For naive
    timestamps, the script treats them as IST because your historical option
    pickles are already Indian market data.
    """
    dt = pd.to_datetime(series_or_scalar, errors="coerce")

    if isinstance(dt, pd.Series):
        if dt.dt.tz is None:
            return dt.dt.tz_localize(IST)
        return dt.dt.tz_convert(IST)

    if getattr(dt, "tzinfo", None) is None:
        return dt.tz_localize(IST)
    return dt.tz_convert(IST)


def normalize_underlying(value: str) -> Optional[str]:
    """Normalize index name from Dhan/Zerodha-style fields."""
    if not isinstance(value, str):
        return None
    u = value.upper().strip()

    # Important: detect SENSEX before NIFTY.
    if "SENSEX" in u:
        return "SENSEX"

    # Explicitly exclude Bank Nifty as requested.
    if "BANKNIFTY" in u or "NIFTY BANK" in u or "BANK NIFTY" in u:
        return None

    # Keep only NIFTY index, not individual NSE stocks.
    if "NIFTY" in u:
        return "NIFTY"

    return None


def infer_option_type_from_symbol(symbol: str) -> Optional[str]:
    """Fallback option-side parser if option_type column is absent/bad."""
    s = str(symbol).upper().strip()
    if s.endswith("CE") or " CE" in s:
        return "CE"
    if s.endswith("PE") or " PE" in s:
        return "PE"
    return None


def round_to_step(x: float, step: int) -> int:
    """Round price to nearest valid index-option strike interval."""
    return int(round(float(x) / int(step)) * int(step))


def build_minute_index(day_d: date) -> pd.DatetimeIndex:
    """Full 1-minute market-session index for one date."""
    start = pd.Timestamp(datetime.combine(day_d, SESSION_START_IST), tz=IST)
    end = pd.Timestamp(datetime.combine(day_d, SESSION_END_IST), tz=IST)
    return pd.date_range(start=start, end=end, freq="1min")


def list_pickle_paths(pickles_dir: str) -> List[str]:
    """Find .pkl and .pickle files in the configured folder."""
    paths = sorted(
        glob.glob(os.path.join(pickles_dir, "*.pkl"))
        + glob.glob(os.path.join(pickles_dir, "*.pickle"))
    )
    if not paths:
        raise FileNotFoundError(f"No .pkl/.pickle files found in: {pickles_dir}")
    return paths


def pick_symbol_with_most_rows(day_opt: pd.DataFrame, strike: int, opt_type: str) -> Optional[str]:
    """Choose the most complete instrument for strike/type.

    If multiple symbols are present for the same strike/type because of duplicate
    files, the symbol with the highest row count is preferred. Ties are broken
    alphabetically for reproducibility.
    """
    sub = day_opt[(day_opt["strike_int"] == int(strike)) & (day_opt["option_type"] == opt_type)]
    if sub.empty:
        return None

    counts = (
        sub.groupby("instrument", dropna=True)
        .size()
        .reset_index(name="rows")
        .sort_values(["rows", "instrument"], ascending=[False, True])
    )
    if counts.empty:
        return None
    return str(counts.iloc[0]["instrument"])


# =============================================================================
# Shown-session cache
# =============================================================================
def load_shown_selections() -> Set[Tuple[str, str, str, int, str, str]]:
    """Load previously shown option sessions from the local pickle."""
    if not os.path.exists(SHOWN_CACHE_PATH):
        return set()

    try:
        with open(SHOWN_CACHE_PATH, "rb") as f:
            data = pickle.load(f)

        out: Set[Tuple[str, str, str, int, str, str]] = set()
        records = data.get("shown_option_sessions", []) if isinstance(data, dict) else []

        for item in records:
            if not isinstance(item, dict):
                continue
            try:
                out.add(
                    (
                        str(item["underlying"]).upper().strip(),
                        str(item["session_date"]),
                        str(item["expiry_date"]),
                        int(item["atm_strike"]),
                        str(item["option_type"]).upper().strip(),
                        str(item["instrument"]).upper().strip(),
                    )
                )
            except Exception:
                continue

        return out
    except Exception as exc:
        print(f"[WARN] Could not read shown-session cache: {exc}")
        return set()


def save_shown_selections(shown: Set[Tuple[str, str, str, int, str, str]]) -> None:
    """Save shown option sessions to disk."""
    records = [
        {
            "underlying": und,
            "session_date": dy,
            "expiry_date": ex,
            "atm_strike": int(strike),
            "option_type": opt_type,
            "instrument": instrument,
        }
        for und, dy, ex, strike, opt_type, instrument in sorted(shown)
    ]

    tmp = SHOWN_CACHE_PATH + ".tmp"
    with open(tmp, "wb") as f:
        pickle.dump({"shown_option_sessions": records}, f)
    os.replace(tmp, SHOWN_CACHE_PATH)


def mark_selection_shown(selection: OptionSelection) -> None:
    """Record current selection so random mode does not repeat it."""
    shown = load_shown_selections()
    shown.add(selection.key())
    save_shown_selections(shown)


def reset_shown_cache() -> None:
    """Delete shown-session cache."""
    if os.path.exists(SHOWN_CACHE_PATH):
        os.remove(SHOWN_CACHE_PATH)
    print(f"[CACHE] Reset: {SHOWN_CACHE_PATH}")


# =============================================================================
# Pickle scanning and loading
# =============================================================================
def normalize_option_frame_for_scan(df: pd.DataFrame, source_path: str) -> pd.DataFrame:
    """Return small normalized DataFrame for metadata scanning.

    Required scan columns are deliberately minimal to keep scanning cheaper:
    date, name/instrument, expiry, type.
    """
    if df.empty:
        return pd.DataFrame()

    if "date" not in df.columns or "expiry" not in df.columns:
        raise ValueError("Missing required columns: date/expiry")

    # Some files use name to indicate underlying; instrument is a useful fallback.
    if "name" not in df.columns and "instrument" not in df.columns and "tradingsymbol" not in df.columns:
        raise ValueError("Missing required underlying source column: name/instrument/tradingsymbol")

    # Respect the original script's type == OPTION filter when available.
    d = df.copy()
    if "type" in d.columns:
        d = d[d["type"].astype(str).str.upper().eq("OPTION")].copy()

    if d.empty:
        return pd.DataFrame()

    symbol_col = "instrument" if "instrument" in d.columns else ("tradingsymbol" if "tradingsymbol" in d.columns else "name")
    name_col = "name" if "name" in d.columns else symbol_col

    out = pd.DataFrame()
    out["date"] = ensure_ist(d["date"])
    out["day"] = out["date"].dt.date
    out["source_name"] = d[name_col].astype(str)
    out["instrument"] = d[symbol_col].astype(str).str.upper().str.strip()
    out["underlying"] = out["source_name"].map(normalize_underlying)
    out["expiry_date"] = pd.to_datetime(d["expiry"], errors="coerce").dt.date
    out["source_path"] = source_path

    out = out.dropna(subset=["day", "underlying", "expiry_date"])
    out = out[out["underlying"].isin(TRADEABLE_UNDERLYINGS)]
    out = out[out["expiry_date"] >= out["day"]]
    return out


def scan_option_pickles(pickle_paths: Sequence[str], fail_on_error: bool = False) -> Catalog:
    """Pass-1 scan: identify available NIFTY/SENSEX days and nearest expiries."""
    min_day_seen: Optional[date] = None
    max_day_seen: Optional[date] = None
    min_expiry_map: Dict[Tuple[str, date], date] = {}
    paths_by_key: Dict[Tuple[str, date, date], Set[str]] = {}

    for p in pickle_paths:
        try:
            df = pd.read_pickle(p)
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue

            d = normalize_option_frame_for_scan(df, p)
            if d.empty:
                continue

            file_min = d["day"].min()
            file_max = d["day"].max()
            min_day_seen = file_min if min_day_seen is None or file_min < min_day_seen else min_day_seen
            max_day_seen = file_max if max_day_seen is None or file_max > max_day_seen else max_day_seen

            # Map path membership for later exact day/expiry loading.
            for (und, dy, ex), _g in d.groupby(["underlying", "day", "expiry_date"], sort=False):
                paths_by_key.setdefault((und, dy, ex), set()).add(p)

            # Nearest expiry for each underlying/day.
            grp = d.groupby(["underlying", "day"], sort=False)["expiry_date"].min()
            for (und, dy), ex in grp.items():
                key = (und, dy)
                if key not in min_expiry_map or ex < min_expiry_map[key]:
                    min_expiry_map[key] = ex

            print(f"[SCAN OK] {os.path.basename(p)} option_days={d['day'].nunique()} groups={len(grp)}")

        except Exception as exc:
            msg = f"[SCAN WARN] {os.path.basename(p)} failed: {exc}"
            if fail_on_error:
                raise RuntimeError(msg) from exc
            print(msg)

    if min_day_seen is None or max_day_seen is None or not min_expiry_map:
        raise RuntimeError("No usable NIFTY/SENSEX option data found in the pickle folder.")

    return Catalog(
        min_day_seen=min_day_seen,
        max_day_seen=max_day_seen,
        min_expiry_map=min_expiry_map,
        paths_by_key=paths_by_key,
    )


def normalize_option_frame_for_chart(
    df: pd.DataFrame,
    source_path: str,
    underlying: str,
    session_day: date,
    expiry_date: date,
) -> pd.DataFrame:
    """Normalize one pickle's option rows for charting."""
    if df.empty:
        return pd.DataFrame()

    # Flexible instrument column handling.
    if "instrument" in df.columns:
        instrument_col = "instrument"
    elif "tradingsymbol" in df.columns:
        instrument_col = "tradingsymbol"
    elif "symbol" in df.columns:
        instrument_col = "symbol"
    else:
        raise ValueError("Missing instrument/tradingsymbol/symbol column")

    # B's pickle processing requires high/low/close; candlestick needs open too.
    # If open is missing, use close as a last-resort fallback and warn.
    required_price_cols = ["high", "low", "close"]
    missing_price = [c for c in required_price_cols if c not in df.columns]
    if missing_price:
        raise ValueError(f"Missing price columns {missing_price}")

    d = df.copy()
    if "type" in d.columns:
        d = d[d["type"].astype(str).str.upper().eq("OPTION")].copy()
    if d.empty:
        return pd.DataFrame()

    if "open" not in d.columns:
        print(f"[WARN] {os.path.basename(source_path)} has no 'open' column. Using close as open for candlestick bodies.")
        d["open"] = d["close"]

    if "option_type" not in d.columns:
        d["option_type"] = d[instrument_col].astype(str).map(infer_option_type_from_symbol)
    else:
        d["option_type"] = d["option_type"].astype(str).str.upper().str.strip()
        missing_side = ~d["option_type"].isin(["CE", "PE"])
        if missing_side.any():
            d.loc[missing_side, "option_type"] = d.loc[missing_side, instrument_col].astype(str).map(infer_option_type_from_symbol)

    if "name" in d.columns:
        underlying_source = d["name"].astype(str)
    else:
        underlying_source = d[instrument_col].astype(str)

    out = pd.DataFrame()
    out["date"] = ensure_ist(d["date"])
    out["day"] = out["date"].dt.date
    out["underlying"] = underlying_source.map(normalize_underlying)
    out["expiry_date"] = pd.to_datetime(d["expiry"], errors="coerce").dt.date
    out["instrument"] = d[instrument_col].astype(str).str.upper().str.strip()
    out["option_type"] = d["option_type"].astype(str).str.upper().str.strip()
    out["strike_num"] = pd.to_numeric(d["strike"], errors="coerce")
    out["strike_int"] = out["strike_num"].round().astype("Int64")

    for col in ["open", "high", "low", "close"]:
        out[col] = pd.to_numeric(d[col], errors="coerce")

    if "volume" in d.columns:
        out["volume"] = pd.to_numeric(d["volume"], errors="coerce").fillna(0)
    else:
        out["volume"] = 0

    out["source_path"] = source_path

    out = out.dropna(
        subset=[
            "date",
            "day",
            "underlying",
            "expiry_date",
            "instrument",
            "option_type",
            "strike_int",
            "open",
            "high",
            "low",
            "close",
        ]
    ).copy()

    out["strike_int"] = out["strike_int"].astype(int)

    out = out[
        (out["underlying"] == underlying)
        & (out["day"] == session_day)
        & (out["expiry_date"] == expiry_date)
        & (out["option_type"].isin(["CE", "PE"]))
        & (out["expiry_date"] >= out["day"])
    ].copy()

    # Keep only regular market session.
    out["session_time"] = out["date"].dt.time
    out = out[
        (out["session_time"] >= SESSION_START_IST)
        & (out["session_time"] <= SESSION_END_IST)
    ].copy()

    return out.drop(columns=["session_time"], errors="ignore")


def load_day_options_from_paths(
    paths: Sequence[str],
    underlying: str,
    session_day: date,
    expiry_date: date,
    fail_on_error: bool = False,
) -> pd.DataFrame:
    """Load all option rows for one underlying/day/expiry from relevant pickles."""
    frames: List[pd.DataFrame] = []

    for p in sorted(set(paths)):
        try:
            raw = pd.read_pickle(p)
            if not isinstance(raw, pd.DataFrame) or raw.empty:
                continue
            d = normalize_option_frame_for_chart(raw, p, underlying, session_day, expiry_date)
            if not d.empty:
                frames.append(d)
        except Exception as exc:
            msg = f"[LOAD WARN] {os.path.basename(p)} failed for {underlying} {session_day} {expiry_date}: {exc}"
            if fail_on_error:
                raise RuntimeError(msg) from exc
            print(msg)

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)

    # Deduplicate identical minute/instrument rows. If duplicate pickles exist,
    # the row from the lexicographically latest path wins because of the sort.
    out = (
        out.sort_values(["instrument", "date", "source_path"])
        .drop_duplicates(subset=["instrument", "date"], keep="last")
        .sort_values(["instrument", "date"])
        .reset_index(drop=True)
    )
    return out


def build_selected_option_candles(
    day_opt: pd.DataFrame,
    strike: int,
    option_type: str,
    instrument: str,
) -> pd.DataFrame:
    """Extract clean OHLCV series for the selected option instrument."""
    sub = day_opt[
        (day_opt["strike_int"] == int(strike))
        & (day_opt["option_type"] == option_type)
        & (day_opt["instrument"].astype(str).str.upper().str.strip() == instrument.upper().strip())
    ][["date", "open", "high", "low", "close", "volume"]].copy()

    if sub.empty:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    sub["date"] = ensure_ist(sub["date"])
    for col in ["open", "high", "low", "close", "volume"]:
        sub[col] = pd.to_numeric(sub[col], errors="coerce")

    sub = sub.dropna(subset=["date", "open", "high", "low", "close"]).copy()
    sub = sub.drop_duplicates(subset=["date"], keep="last").sort_values("date").reset_index(drop=True)

    # Keep only market session. This is repeated deliberately because duplicate
    # files may contain odd ticks outside normal hours.
    sub["session_time"] = sub["date"].dt.time
    sub = sub[
        (sub["session_time"] >= SESSION_START_IST)
        & (sub["session_time"] <= SESSION_END_IST)
    ].drop(columns=["session_time"])

    return sub.reset_index(drop=True)


# =============================================================================
# Kite underlying-open helpers
# =============================================================================
def init_kite() -> "KiteConnect":
    """Initialize Kite through the same helper used by your trading scripts."""
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


def _kite_instruments_cached(kite: "KiteConnect", exchange: str, cache: Dict[str, List[Dict]]) -> List[Dict]:
    """Download/cache Kite instrument dump in memory only."""
    ex = exchange.upper().strip()
    if ex not in cache:
        print(f"[KITE] Loading instruments dump for {ex} ...")
        cache[ex] = kite.instruments(ex)
        print(f"[KITE] {ex} instruments: {len(cache[ex])}")
    return cache[ex]


def get_instrument_token(kite: "KiteConnect", exchange: str, tradingsymbol: str, cache: Dict[str, List[Dict]]) -> int:
    """Resolve a Kite tradingsymbol to instrument_token."""
    wanted = tradingsymbol.strip().upper()
    for row in _kite_instruments_cached(kite, exchange, cache):
        if str(row.get("tradingsymbol", "")).upper().strip() == wanted:
            return int(row["instrument_token"])
    raise ValueError(f"Instrument not found on {exchange}: {tradingsymbol}")


def fetch_underlying_day(kite: "KiteConnect", und: str, dy: date, instrument_cache: Dict[str, List[Dict]]) -> pd.DataFrame:
    """Fetch one day's underlying minute candles from Kite."""
    meta = UNDERLYING_KITE[und]
    token = get_instrument_token(kite, meta["exchange"], meta["tradingsymbol"], instrument_cache)

    from_dt = IST.localize(datetime.combine(dy, SESSION_START_IST))
    to_dt = IST.localize(datetime.combine(dy, SESSION_END_IST))

    last_err: Optional[Exception] = None
    for attempt in range(1, 4):
        try:
            rows = kite.historical_data(
                instrument_token=int(token),
                from_date=from_dt,
                to_date=to_dt,
                interval="minute",
                continuous=False,
                oi=False,
            )
            if not rows:
                return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

            df = pd.DataFrame(rows)
            df["date"] = ensure_ist(df["date"])
            for col in ["open", "high", "low", "close", "volume"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            df = df.dropna(subset=["date", "open", "high", "low", "close"]).copy()
            df["day"] = df["date"].dt.date
            df = df[df["day"] == dy].copy()
            df = df.drop_duplicates(subset=["date"], keep="last").sort_values("date").reset_index(drop=True)
            return df

        except Exception as exc:
            last_err = exc
            wait_s = min(6, attempt * 1.5)
            print(f"[KITE WARN] Underlying fetch failed {und} {dy} attempt {attempt}/3: {exc}. Retrying in {wait_s}s...")
            time.sleep(wait_s)

    raise RuntimeError(f"Could not fetch underlying candles for {und} {dy}: {last_err}")


def get_underlying_open(
    kite: "KiteConnect",
    und: str,
    dy: date,
    instrument_cache: Dict[str, List[Dict]],
    underlying_cache: Dict[Tuple[str, date], pd.DataFrame],
) -> Tuple[float, str]:
    """Return first available underlying open price for the date."""
    key = (und, dy)
    if key not in underlying_cache:
        underlying_cache[key] = fetch_underlying_day(kite, und, dy, instrument_cache)

    df = underlying_cache[key]
    if df.empty:
        raise RuntimeError(f"No underlying Kite candles available for {und} on {dy}")

    first = df.sort_values("date").iloc[0]
    open_px = float(first["open"])
    open_time = pd.Timestamp(first["date"]).strftime("%H:%M")
    return open_px, open_time


# =============================================================================
# Session selection
# =============================================================================
def selection_from_store(store: Dict[str, object]) -> OptionSelection:
    """Deserialize Dash store into OptionSelection."""
    return OptionSelection(
        underlying=str(store["underlying"]).upper().strip(),
        session_date=parse_iso_date(str(store["session_date"])),
        expiry_date=parse_iso_date(str(store["expiry_date"])),
        days_to_expiry=int(store["days_to_expiry"]),
        underlying_open=float(store["underlying_open"]),
        underlying_open_time=str(store["underlying_open_time"]),
        atm_strike=int(store["atm_strike"]),
        option_type=str(store["option_type"]).upper().strip(),
        instrument=str(store["instrument"]).upper().strip(),
        source_files=tuple(str(x) for x in store.get("source_files", [])),
    )


def choose_option_type(day_opt: pd.DataFrame, atm: int, requested: str) -> Optional[Tuple[str, str]]:
    """Choose CE/PE instrument for the ATM strike.

    Returns:
        (option_type, instrument), or None if no valid instrument is available.
    """
    requested = requested.upper().strip()

    available: List[Tuple[str, str]] = []
    for side in ["CE", "PE"]:
        symbol = pick_symbol_with_most_rows(day_opt, atm, side)
        if symbol:
            available.append((side, symbol))

    if not available:
        return None

    if requested in ("CE", "PE"):
        for side, symbol in available:
            if side == requested:
                return side, symbol
        return None

    return random.choice(available)


def try_build_selection(
    *,
    kite: "KiteConnect",
    catalog: Catalog,
    und: str,
    dy: date,
    nearest_expiry: date,
    option_type_request: str,
    fail_on_error: bool,
    instrument_cache: Dict[str, List[Dict]],
    underlying_cache: Dict[Tuple[str, date], pd.DataFrame],
) -> Tuple[Optional[OptionSelection], Optional[pd.DataFrame], str]:
    """Attempt to construct one valid ATM option selection."""
    source_paths = sorted(catalog.paths_by_key.get((und, dy, nearest_expiry), set()))
    if not source_paths:
        return None, None, "No pickle path mapped for nearest expiry"

    try:
        underlying_open, open_time = get_underlying_open(kite, und, dy, instrument_cache, underlying_cache)
    except Exception as exc:
        return None, None, f"Underlying open unavailable: {exc}"

    atm = round_to_step(underlying_open, STRIKE_STEP[und])

    day_opt = load_day_options_from_paths(
        source_paths,
        underlying=und,
        session_day=dy,
        expiry_date=nearest_expiry,
        fail_on_error=fail_on_error,
    )
    if day_opt.empty:
        return None, None, "No option rows loaded for nearest expiry"

    chosen = choose_option_type(day_opt, atm, option_type_request)
    if not chosen:
        return None, None, f"ATM {atm} {option_type_request} not available"

    option_type, instrument = chosen
    option_df = build_selected_option_candles(day_opt, atm, option_type, instrument)
    if option_df.empty:
        return None, None, f"Selected instrument has no candles: {instrument}"

    # Reject pathological selections with only a few candles; those are poor
    # trainer sessions and usually indicate incomplete data.
    if len(option_df) < 20:
        return None, None, f"Too few option candles ({len(option_df)})"

    selection = OptionSelection(
        underlying=und,
        session_date=dy,
        expiry_date=nearest_expiry,
        days_to_expiry=int((nearest_expiry - dy).days),
        underlying_open=float(underlying_open),
        underlying_open_time=open_time,
        atm_strike=int(atm),
        option_type=option_type,
        instrument=instrument,
        source_files=tuple(source_paths),
    )
    return selection, option_df, "OK"


def pick_session_with_constraints(
    *,
    kite: "KiteConnect",
    catalog: Catalog,
    fixed_underlying: Optional[str],
    fixed_date: Optional[date],
    option_type_request: str,
    fail_on_error: bool,
    instrument_cache: Dict[str, List[Dict]],
    underlying_cache: Dict[Tuple[str, date], pd.DataFrame],
) -> Tuple[OptionSelection, pd.DataFrame]:
    """Pick a valid random/manual ATM option session."""
    candidates = catalog.candidates(fixed_underlying, fixed_date)
    if not candidates:
        fixed = []
        if fixed_underlying:
            fixed.append(f"underlying={fixed_underlying}")
        if fixed_date:
            fixed.append(f"date={fixed_date}")
        detail = ", ".join(fixed) if fixed else "no fixed filter"
        raise RuntimeError(f"No option candidates found for {detail}")

    random.shuffle(candidates)
    shown = load_shown_selections()

    # Match the behaviour of the stock trainer: an exact manual request should
    # be shown even if it was shown earlier. The shown-cache is primarily for
    # random practice mode.
    exact_manual_request = fixed_underlying is not None and fixed_date is not None

    failures: List[str] = []

    attempts = 0
    for und, dy, ex in candidates:
        attempts += 1
        if attempts > MAX_RANDOM_ATTEMPTS and fixed_date is None and fixed_underlying is None:
            break

        selection, option_df, reason = try_build_selection(
            kite=kite,
            catalog=catalog,
            und=und,
            dy=dy,
            nearest_expiry=ex,
            option_type_request=option_type_request,
            fail_on_error=fail_on_error,
            instrument_cache=instrument_cache,
            underlying_cache=underlying_cache,
        )

        if selection is None or option_df is None:
            failures.append(f"{und} {dy} {ex}: {reason}")
            continue

        if not exact_manual_request and selection.key() in shown:
            failures.append(f"{und} {dy} {ex}: already shown")
            continue

        mark_selection_shown(selection)
        print(
            f"[SELECTED] {selection.underlying} {selection.session_date} "
            f"exp={selection.expiry_date} DTE={selection.days_to_expiry} "
            f"open={selection.underlying_open:.2f}@{selection.underlying_open_time} "
            f"ATM={selection.atm_strike} {selection.option_type} {selection.instrument} "
            f"candles={len(option_df)}"
        )
        return selection, option_df

    # If exact manual filter was used, it is better to show recent failures.
    tail = "\n".join(f"  - {x}" for x in failures[-15:])
    raise RuntimeError(
        "Could not construct a valid ATM option trainer session.\n"
        f"Failures checked: {len(failures)}\n"
        f"{tail}\n\n"
        "Try --reset-shown-cache, switch --option-type, or check whether your pickle folder contains the ATM strike."
    )


# =============================================================================
# Plotting helpers
# =============================================================================
def build_partial_candle_arrays(
    df_full: pd.DataFrame,
    step: int,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Hide candles after `step` while keeping the x-axis fixed."""
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


def visible_y_range(df_full: pd.DataFrame, step: int) -> Tuple[float, float]:
    """Compute y-range from revealed option candles only."""
    step = max(1, min(int(step), len(df_full)))
    visible = df_full.iloc[:step]

    lo = float(visible["low"].min())
    hi = float(visible["high"].max())
    span = max(hi - lo, 1e-9)
    mid = max((hi + lo) / 2.0, 1.0)
    pad = max(span * Y_PADDING_PCT, mid * MIN_Y_PADDING_PCT_OF_PRICE)
    return max(0.0, lo - pad), hi + pad


def make_candle_figure(
    df_full: pd.DataFrame,
    selection: OptionSelection,
    step: int,
) -> go.Figure:
    """Build the progressive option candlestick chart."""
    fig = go.Figure()
    if df_full.empty:
        fig.update_layout(template="plotly_white", title="No option data")
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
            name=selection.instrument,
            increasing=dict(line=dict(color="#26a69a", width=CANDLE_LINE_WIDTH), fillcolor="#26a69a"),
            decreasing=dict(line=dict(color="#ef5350", width=CANDLE_LINE_WIDTH), fillcolor="#ef5350"),
            whiskerwidth=0.45,
            hoverinfo="skip",
        )
    )

    day_start = IST.localize(datetime.combine(selection.session_date, SESSION_START_IST))
    day_end = IST.localize(datetime.combine(selection.session_date, SESSION_END_IST))
    y_min, y_max = visible_y_range(df_full, step)

    last = df_full.iloc[step - 1]
    last_time = pd.Timestamp(last["date"]).strftime("%H:%M")
    last_open = float(last["open"])
    last_high = float(last["high"])
    last_low = float(last["low"])
    last_close = float(last["close"])

    title = (
        f"{selection.underlying} ATM {selection.atm_strike} {selection.option_type} | "
        f"{selection.instrument} | {selection.session_date} | "
        f"Exp {selection.expiry_date} | DTE {selection.days_to_expiry} | "
        f"Candle {step}/{len(df_full)} | {last_time} | "
        f"O {last_open:.2f} H {last_high:.2f} L {last_low:.2f} C {last_close:.2f}"
    )

    fig.update_layout(
        title=title,
        template="plotly_white",
        height=CHART_HEIGHT_PX,
        margin=dict(l=45, r=84, t=70, b=38),
        hovermode=False,
        dragmode="pan",
        xaxis_rangeslider_visible=False,
        uirevision=selection.instrument + "-" + selection.session_date.isoformat(),
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
        showspikes=False,
    )

    fig.update_yaxes(
        title_text="Option premium",
        range=[y_min, y_max],
        fixedrange=False,
        showgrid=True,
        gridwidth=0.5,
        gridcolor="rgba(0,0,0,0.08)",
        zeroline=False,
        ticks="outside",
        side="right",
        showspikes=False,
    )

    # Current revealed close guide.
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
    """Install compact mouse-price label and crosshair cursor.

    This is intentionally the same behaviour as the stock trainer: only a small
    price label follows the mouse. No vertical/horizontal overlay lines are
    drawn by JavaScript.
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
                #candle-graph,
                #candle-graph .js-plotly-plot,
                #candle-graph .main-svg,
                #candle-graph .draglayer,
                #candle-graph .nsewdrag {
                    cursor: crosshair !important;
                }

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

                            const xInPlot = event.clientX - graphRect.left - size.l;
                            const yInPlot = event.clientY - graphRect.top - size.t;

                            if (xInPlot < 0 || xInPlot > size.w || yInPlot < 0 || yInPlot > size.h) {
                                hidePriceLabel(outer);
                                return;
                            }

                            let price;
                            try {
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

                            const labelWidth = 64;
                            const labelHeight = 20;
                            const mouseX = event.clientX - outerRect.left;
                            const mouseY = event.clientY - outerRect.top;

                            let left = mouseX + 10;
                            let top = mouseY - 10;

                            left = clamp(left, 2, outerRect.width - labelWidth - 2);
                            top = clamp(top, 2, outerRect.height - labelHeight - 2);

                            label.style.left = left + "px";
                            label.style.top = top + "px";
                            label.style.display = "block";
                        });

                        gd.addEventListener("mouseleave", function () {
                            hidePriceLabel(outer);
                        });

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

                    setInterval(attachMousePriceLabel, ATTACH_CHECK_INTERVAL_MS);
                })();
                </script>
            </footer>
        </body>
    </html>
    """


# =============================================================================
# Dash application
# =============================================================================
def make_app(
    *,
    kite: "KiteConnect",
    catalog: Catalog,
    initial_selection: OptionSelection,
    initial_df: pd.DataFrame,
    option_type_request: str,
    fixed_underlying: Optional[str],
    fixed_date: Optional[date],
    fail_on_error: bool,
    instrument_cache: Dict[str, List[Dict]],
    underlying_cache: Dict[Tuple[str, date], pd.DataFrame],
) -> Dash:
    """Create the Dash UI."""
    candle_data_by_key: Dict[Tuple[str, str, str, int, str, str], pd.DataFrame] = {
        initial_selection.key(): initial_df
    }

    def get_df_for_selection(selection: OptionSelection) -> pd.DataFrame:
        """Get option candles from memory or reload from mapped source files."""
        key = selection.key()
        if key not in candle_data_by_key:
            day_opt = load_day_options_from_paths(
                selection.source_files,
                underlying=selection.underlying,
                session_day=selection.session_date,
                expiry_date=selection.expiry_date,
                fail_on_error=fail_on_error,
            )
            candle_data_by_key[key] = build_selected_option_candles(
                day_opt,
                selection.atm_strike,
                selection.option_type,
                selection.instrument,
            )
        return candle_data_by_key[key]

    app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
    app.title = "ATM Option Candle Reveal Trainer"
    install_custom_mouse_crosshair(app)

    keyboard_events = [
        {
            "event": "keydown",
            "props": ["key", "code", "repeat", "ctrlKey", "altKey", "metaKey", "shiftKey"],
        }
    ]

    page = dbc.Container(
        [
            dcc.Store(id="session-store", data=initial_selection.to_store(step=INITIAL_STEP)),
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
                            html.H4("ATM Option Candle Reveal Trainer", className="mb-0"),
                            html.Div(
                                "Press → or click Next. Move mouse over chart to see only the exact option premium at pointer.",
                                className="text-muted small",
                            ),
                        ],
                        className="me-auto",
                    ),
                    dbc.Button("New random ATM option", id="new-random-btn", color="primary", className="me-2"),
                    dbc.Button("Next candle (→)", id="next-btn", color="success", className="me-2"),
                    dbc.Button("Reset to first candle", id="reset-step-btn", color="secondary", outline=True),
                ],
                className="d-flex align-items-center flex-wrap gap-2 px-3 py-2 border-bottom",
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
        children=html.Div(
            page,
            id="keyboard-capture-root",
            tabIndex=0,
            style={"outline": "none", "minHeight": "100vh"},
        ),
    )

    app.clientside_callback(
        """
        function(n_intervals, session_data) {
            const el = document.getElementById('keyboard-capture-root');
            if (!el) {
                return window.dash_clientside.no_update;
            }

            const active = document.activeElement;
            const tag = active && active.tagName ? active.tagName.toUpperCase() : '';
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
        """Choose a new session. Fixed CLI filters are respected."""
        selection, option_df = pick_session_with_constraints(
            kite=kite,
            catalog=catalog,
            fixed_underlying=fixed_underlying,
            fixed_date=fixed_date,
            option_type_request=option_type_request,
            fail_on_error=fail_on_error,
            instrument_cache=instrument_cache,
            underlying_cache=underlying_cache,
        )
        candle_data_by_key[selection.key()] = option_df
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
        """Advance one candle, handle Right Arrow, or reset."""
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

            is_right_arrow = key in ("ArrowRight", "Right") or code == "ArrowRight"
            if not is_right_arrow:
                return no_update

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
            return "No option selected.", go.Figure()

        selection = selection_from_store(store)
        df = get_df_for_selection(selection)
        if df.empty:
            return f"No candles found for {selection.instrument}.", go.Figure()

        step = max(1, min(int(store.get("step", INITIAL_STEP)), len(df)))
        fig = make_candle_figure(df, selection, step)

        first_ts = pd.Timestamp(df["date"].iloc[0]).strftime("%H:%M")
        last_ts = pd.Timestamp(df["date"].iloc[-1]).strftime("%H:%M")
        source_count = len(selection.source_files)

        status = (
            f"{selection.underlying} | Date: {selection.session_date} | "
            f"Nearest expiry: {selection.expiry_date} | DTE: {selection.days_to_expiry} | "
            f"Underlying open: {selection.underlying_open:.2f} at {selection.underlying_open_time} | "
            f"ATM: {selection.atm_strike} | Option: {selection.option_type} | "
            f"Instrument: {selection.instrument} | Candles shown: {step}/{len(df)} | "
            f"Option session: {first_ts}-{last_ts} IST | Source pickle files: {source_count}"
        )
        return status, fig

    return app


# =============================================================================
# Main
# =============================================================================
def main() -> None:
    args = parse_args()

    if args.reset_shown_cache:
        reset_shown_cache()

    fixed_underlying = args.underlying.upper().strip() if args.underlying else None
    fixed_date = parse_iso_date(args.date) if args.date else None
    option_type_request = args.option_type.upper().strip()

    pickle_paths = list_pickle_paths(args.pickles_dir)
    print(f"[INFO] Pickle files found: {len(pickle_paths)}")
    print(f"[INFO] Pickles dir: {args.pickles_dir}")
    print(f"[INFO] Option side request: {option_type_request}")
    print(f"[INFO] Fixed underlying: {fixed_underlying or 'RANDOM'}")
    print(f"[INFO] Fixed date: {fixed_date or 'RANDOM'}")

    catalog = scan_option_pickles(pickle_paths, fail_on_error=args.fail_on_pickle_error)
    print(f"[INFO] Option data day-range seen: {catalog.min_day_seen} -> {catalog.max_day_seen}")
    print(f"[INFO] Candidate underlying/day groups: {len(catalog.min_expiry_map)}")

    print("[STEP] Initializing Kite for underlying opening price ...")
    kite = init_kite()
    print("[OK] Kite ready.")

    instrument_cache: Dict[str, List[Dict]] = {}
    underlying_cache: Dict[Tuple[str, date], pd.DataFrame] = {}

    selection, option_df = pick_session_with_constraints(
        kite=kite,
        catalog=catalog,
        fixed_underlying=fixed_underlying,
        fixed_date=fixed_date,
        option_type_request=option_type_request,
        fail_on_error=args.fail_on_pickle_error,
        instrument_cache=instrument_cache,
        underlying_cache=underlying_cache,
    )

    app = make_app(
        kite=kite,
        catalog=catalog,
        initial_selection=selection,
        initial_df=option_df,
        option_type_request=option_type_request,
        fixed_underlying=fixed_underlying,
        fixed_date=fixed_date,
        fail_on_error=args.fail_on_pickle_error,
        instrument_cache=instrument_cache,
        underlying_cache=underlying_cache,
    )

    print("\nOpen this URL in your browser:")
    print(f"http://127.0.0.1:{args.port}")
    print("\nControls: click 'Next candle' or press the Right Arrow key.")
    print("Move mouse over chart to show only the exact option premium at pointer.")
    print(f"Shown-session cache: {SHOWN_CACHE_PATH}\n")

    app.run(debug=True, port=args.port)


if __name__ == "__main__":
    main()
