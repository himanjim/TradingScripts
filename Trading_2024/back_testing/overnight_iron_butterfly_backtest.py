#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Overnight Iron Butterfly Backtester for Zerodha/Dhan-style option pickle files.

Strategy requested
------------------
1. Read option minute-data pickle files from a folder.
2. Enter an Iron Butterfly at 15:29 IST.
3. Exit the same option legs at 09:16 IST on the next available trading day.
4. Test:
   - NIFTY and SENSEX only on expiry day D-2 and D-1.
   - BANKNIFTY on all available pre-expiry days except expiry day D.
5. Write Excel output with:
   - all_trades_backtested: every completed eligible trade found in the pickles.
   - actual_trades: only NIFTY/SENSEX, one trade per calendar day, nearest to expiry.
   - summary sheets and skipped diagnostics.

Important assumptions
---------------------
- One pickle may contain one expiry or multiple expiries. The script handles both.
- Required option columns are broadly compatible with your existing scripts:
  date, name, type, option_type, strike, expiry, instrument, close.
- high/low/open/volume are not required because this is a fixed entry/exit overnight test.
- The ATM strike is selected using spot/underlying price if present in the pickle.
  If no spot column exists, ATM is inferred from the option chain at entry time by
  choosing the strike where |CE premium - PE premium| is minimum.
- Buy wings can be enabled/disabled. If disabled, the strategy becomes an overnight
  short straddle, but the output still clearly marks the missing buy legs.

Run
---
1. Set PICKLES_DIR below or pass it via environment variable.
2. Run:
      python overnight_iron_butterfly_backtest.py

Environment overrides examples on Windows PowerShell
----------------------------------------------------
$env:PICKLES_DIR='G:\\My Drive\\Trading\\Historical_Options_Data'
$env:ENABLE_BUY_WINGS='1'
$env:WING_DISTANCE_STEPS='2'
$env:ENTRY_TIME_IST='15:29'
$env:EXIT_TIME_IST='09:16'
python overnight_iron_butterfly_backtest.py
"""

from __future__ import annotations

import glob
import math
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, time as dtime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:  # pragma: no cover - fallback for older environments
    ZoneInfo = None  # type: ignore

try:
    import pytz  # type: ignore
except Exception:  # pragma: no cover - pytz is optional
    pytz = None  # type: ignore


# =============================================================================
# USER CONFIG
# =============================================================================

# Folder containing .pkl/.pickle option minute-data files.
# Override from environment: set PICKLES_DIR=...
PICKLES_DIR = os.getenv(
    "PICKLES_DIR",
    r"G:\My Drive\Trading\Historical_Options_Data",
)

# Entry and exit times requested by you.
# Entry is on trade day. Exit is on the next available trading day in the same
# option-expiry dataset.
ENTRY_TIME_IST = os.getenv("ENTRY_TIME_IST", "15:29")
EXIT_TIME_IST = os.getenv("EXIT_TIME_IST", "09:16")

# Time lookup mode:
# - "exact"       : require the exact minute candle.
# - "pad"         : use the latest candle at or before the target time.
# - "backfill"    : use the earliest candle at or after the target time.
# - "nearest"     : use nearest candle within MAX_TIME_LOOKUP_TOLERANCE_MINUTES.
# For a clean backtest, keep exact. If your pickles occasionally miss one minute,
# change EXIT_PRICE_LOOKUP_MODE to nearest/backfill, but record that in results.
ENTRY_PRICE_LOOKUP_MODE = os.getenv("ENTRY_PRICE_LOOKUP_MODE", "exact").strip().lower()
EXIT_PRICE_LOOKUP_MODE = os.getenv("EXIT_PRICE_LOOKUP_MODE", "exact").strip().lower()
MAX_TIME_LOOKUP_TOLERANCE_MINUTES = int(os.getenv("MAX_TIME_LOOKUP_TOLERANCE_MINUTES", "2"))

# Iron Butterfly buy wings.
# ENABLE_BUY_WINGS=1 means:
#   Sell ATM CE + Sell ATM PE
#   Buy OTM CE at ATM + distance
#   Buy OTM PE at ATM - distance
# ENABLE_BUY_WINGS=0 means only the short ATM CE/PE legs are traded.
ENABLE_BUY_WINGS = os.getenv("ENABLE_BUY_WINGS", "1").strip() == "1"

# Wing distance can be configured in either steps or absolute points.
# If BUY_CE_DISTANCE_POINTS or BUY_PE_DISTANCE_POINTS is set to a positive value,
# that exact point distance is used for that side. Otherwise WING_DISTANCE_STEPS
# multiplied by the instrument's strike step is used.
WING_DISTANCE_STEPS = int(os.getenv("WING_DISTANCE_STEPS", "7"))
BUY_CE_DISTANCE_POINTS = int(os.getenv("BUY_CE_DISTANCE_POINTS", "0"))
BUY_PE_DISTANCE_POINTS = int(os.getenv("BUY_PE_DISTANCE_POINTS", "0"))

# Quantity units to use for P&L. These are deliberately configurable because lot
# sizes and your intended number of lots can change. NIFTY/SENSEX defaults match
# your existing scripts. BANKNIFTY default is a placeholder; set it explicitly.
QTY_UNITS: Dict[str, int] = {
    "NIFTY": int(os.getenv("QTY_NIFTY", "325")),
    "SENSEX": int(os.getenv("QTY_SENSEX", "100")),
    "BANKNIFTY": int(os.getenv("QTY_BANKNIFTY", "35")),
}

# Strike step used for ATM/wings.
STRIKE_STEP: Dict[str, int] = {
    "NIFTY": int(os.getenv("STRIKE_STEP_NIFTY", "50")),
    "SENSEX": int(os.getenv("STRIKE_STEP_SENSEX", "100")),
    "BANKNIFTY": int(os.getenv("STRIKE_STEP_BANKNIFTY", "100")),
}

# Which underlyings to include in all_trades_backtested.
# actual_trades is restricted later to NIFTY/SENSEX only.
TRADEABLE: Tuple[str, ...] = tuple(
    x.strip().upper()
    for x in os.getenv("TRADEABLE", "NIFTY,SENSEX,BANKNIFTY").split(",")
    if x.strip()
)

# BANKNIFTY is now generally treated as a monthly-expiry index in your tests.
# If old historical files contain weekly BANKNIFTY expiries also, keep this as 1
# to retain only the last available BANKNIFTY expiry in each expiry month.
# Set BANKNIFTY_MONTHLY_EXPIRY_ONLY=0 if your folder already contains only the
# exact BANKNIFTY contracts you want to test.
BANKNIFTY_MONTHLY_EXPIRY_ONLY = os.getenv("BANKNIFTY_MONTHLY_EXPIRY_ONLY", "1").strip() == "1"

# Actual trades tie-breaker. If NIFTY and SENSEX are both equally near to expiry
# on the same day, the first symbol in this list is selected.
ACTUAL_TRADE_TIE_BREAKER: Tuple[str, ...] = tuple(
    x.strip().upper()
    for x in os.getenv("ACTUAL_TRADE_TIE_BREAKER", "NIFTY,SENSEX").split(",")
    if x.strip()
)

# Keep only the most recent N months of data from the available pickle range.
# Set 0 to use all available data.
LOOKBACK_MONTHS = int(os.getenv("LOOKBACK_MONTHS", "0"))

# If true, the script stops on a corrupt/bad pickle. If false, bad files are
# logged in the skipped sheet and processing continues.
FAIL_ON_PICKLE_ERROR = os.getenv("FAIL_ON_PICKLE_ERROR", "0").strip() == "1"

# Excel output path.
def _get_downloads_folder() -> str:
    downloads = Path.home() / "Downloads"
    return str(downloads if downloads.exists() else Path.home())


def _safe_fname_part(s: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in s)


_DEFAULT_OUT = os.path.join(
    _get_downloads_folder(),
    "overnight_iron_butterfly"
    f"_entry_{_safe_fname_part(ENTRY_TIME_IST)}"
    f"_exit_{_safe_fname_part(EXIT_TIME_IST)}"
    f"_wings_{int(ENABLE_BUY_WINGS)}"
    f"_steps_{WING_DISTANCE_STEPS}.xlsx",
)
OUTPUT_XLSX = os.getenv("OUTPUT_XLSX", _DEFAULT_OUT)


# =============================================================================
# TRANSACTION CHARGES CONFIG
# =============================================================================
# Zerodha-style options charges approximation. Keep these configurable because
# broker charges and statutory levies can change. This calculator handles both
# short and long legs. STT is applied on sell-side option premium only.

INCLUDE_TRANSACTION_COSTS = os.getenv("INCLUDE_TRANSACTION_COSTS", "1").strip() == "1"
BROKERAGE_PER_ORDER = float(os.getenv("BROKERAGE_PER_ORDER", "20.0"))
STT_SELL_PCT = float(os.getenv("STT_SELL_PCT", "0.001"))          # 0.1% on sell side
EXCHANGE_TXN_PCT = float(os.getenv("EXCHANGE_TXN_PCT", "0.0003553"))
SEBI_PER_CRORE = float(os.getenv("SEBI_PER_CRORE", "10.0"))
STAMP_BUY_PCT = float(os.getenv("STAMP_BUY_PCT", "0.00003"))      # 0.003% on buy side
IPFT_PER_CRORE = float(os.getenv("IPFT_PER_CRORE", "0.010"))
GST_PCT = float(os.getenv("GST_PCT", "0.18"))


# =============================================================================
# DATE/TIME HELPERS
# =============================================================================

def ist_tz():
    """Return an Asia/Kolkata tz object compatible with pandas."""
    if ZoneInfo is not None:
        return ZoneInfo("Asia/Kolkata")
    if pytz is not None:
        return pytz.timezone("Asia/Kolkata")
    return "Asia/Kolkata"


def parse_hhmm(s: str) -> dtime:
    """Parse HH:MM string into datetime.time."""
    hh, mm = s.strip().split(":")
    return dtime(int(hh), int(mm))


ENTRY_TIME = parse_hhmm(ENTRY_TIME_IST)
EXIT_TIME = parse_hhmm(EXIT_TIME_IST)


def ensure_ist(series_or_scalar) -> Any:
    """
    Convert a pandas Series/scalar datetime to timezone-aware Asia/Kolkata.

    Many historical pickle files contain timezone-naive timestamps. In trading
    data generated locally in India, those are usually already IST. Therefore,
    timezone-naive values are localized to IST rather than treated as UTC.
    """
    tz = ist_tz()
    dt = pd.to_datetime(series_or_scalar, errors="coerce")

    if isinstance(dt, pd.Series):
        if dt.dt.tz is None:
            return dt.dt.tz_localize(tz)
        return dt.dt.tz_convert(tz)

    if getattr(dt, "tzinfo", None) is None:
        return dt.tz_localize(tz)
    return dt.tz_convert(tz)


def make_ts(day: date, t: dtime) -> pd.Timestamp:
    """Build an IST timestamp for a trading day and clock time."""
    return pd.Timestamp(datetime.combine(day, t), tz=ist_tz())


def compute_window_start(end_day: date, months: int) -> Optional[date]:
    """Return lower date bound for LOOKBACK_MONTHS, or None when disabled."""
    if months <= 0:
        return None
    return (pd.Timestamp(end_day) - pd.DateOffset(months=months)).date()


# =============================================================================
# NORMALIZATION HELPERS
# =============================================================================

def normalize_underlying(name: Any) -> Optional[str]:
    """
    Map raw option name/symbol text to one of NIFTY, SENSEX, BANKNIFTY.

    Avoid treating FINNIFTY/MIDCPNIFTY as NIFTY. If you later want FINNIFTY,
    add it explicitly instead of relying on substring matching.
    """
    if not isinstance(name, str):
        return None

    u = name.upper().replace(" ", "").replace("-", "_")

    if "SENSEX" in u:
        return "SENSEX"
    if "BANKNIFTY" in u or "NIFTYBANK" in u:
        return "BANKNIFTY"
    if "FINNIFTY" in u or "MIDCPNIFTY" in u:
        return None
    if "NIFTY" in u:
        return "NIFTY"
    return None


def round_to_step(x: float, step: int) -> int:
    """Round spot/index value to the nearest legal strike."""
    if pd.isna(x):
        raise ValueError("Cannot round NaN to strike")
    return int(round(float(x) / step) * step)


def month_key(d: date) -> str:
    return pd.Timestamp(d).strftime("%Y-%m")


# =============================================================================
# DATA STRUCTURES
# =============================================================================

@dataclass
class Leg:
    """One option leg in the strategy."""

    leg_name: str              # short_atm_ce, short_atm_pe, long_ce, long_pe
    side: str                  # SELL at entry for shorts; BUY at entry for longs
    option_type: str           # CE or PE
    strike: int
    instrument: str
    entry_price: float
    exit_price: float
    entry_time_used: str
    exit_time_used: str
    price_lookup_note: str

    def pnl_per_unit(self) -> float:
        """Return leg P&L per one unit before multiplying by quantity."""
        if self.side == "SELL":
            return self.entry_price - self.exit_price
        if self.side == "BUY":
            return self.exit_price - self.entry_price
        raise ValueError(f"Unknown side: {self.side}")

    def entry_sell_turnover_per_unit(self) -> float:
        return self.entry_price if self.side == "SELL" else 0.0

    def entry_buy_turnover_per_unit(self) -> float:
        return self.entry_price if self.side == "BUY" else 0.0

    def exit_sell_turnover_per_unit(self) -> float:
        return self.exit_price if self.side == "BUY" else 0.0

    def exit_buy_turnover_per_unit(self) -> float:
        return self.exit_price if self.side == "SELL" else 0.0


@dataclass
class SimulationResult:
    """A completed trade row for Excel output."""

    entry_day: date
    exit_day: date
    underlying: str
    expiry: date
    days_to_expiry_at_entry: int
    days_to_expiry_at_exit: int
    strategy: str
    buy_wings_enabled: bool
    qty_units: int
    atm_selection_method: str
    atm_reference_value: float
    atm_strike: int
    wing_distance_steps: int
    ce_wing_distance_points: int
    pe_wing_distance_points: int
    entry_time_requested: str
    exit_time_requested: str
    entry_time_used: str
    exit_time_used: str
    short_ce_symbol: str
    short_pe_symbol: str
    long_ce_symbol: str
    long_pe_symbol: str
    short_ce_strike: int
    short_pe_strike: int
    long_ce_strike: Optional[int]
    long_pe_strike: Optional[int]
    short_ce_entry: float
    short_pe_entry: float
    long_ce_entry: Optional[float]
    long_pe_entry: Optional[float]
    short_ce_exit: float
    short_pe_exit: float
    long_ce_exit: Optional[float]
    long_pe_exit: Optional[float]
    entry_credit_per_unit: float
    exit_debit_per_unit: float
    gross_pnl_per_unit: float
    gross_pnl: float
    txn_charges: float
    net_pnl: float
    price_lookup_notes: str
    source_files: str


# =============================================================================
# PICKLE LOADING
# =============================================================================

REQUIRED_BASE_COLUMNS = [
    "date",
    "name",
    "type",
    "option_type",
    "strike",
    "expiry",
    "instrument",
    "close",
]

# Possible columns where your data may store spot/index/underlying values. The
# script tries these in order. If none exists, ATM is inferred from CE/PE premiums.
POSSIBLE_SPOT_COLUMNS = [
    "underlying_close",
    "underlying_price",
    "underlying_value",
    "underlying",
    "spot",
    "spot_price",
    "index_close",
    "index_value",
    "underlying_last_price",
]


def list_pickle_paths(pickles_dir: str) -> List[str]:
    """Return sorted .pkl/.pickle files from the configured directory."""
    paths = sorted(
        glob.glob(os.path.join(pickles_dir, "*.pkl"))
        + glob.glob(os.path.join(pickles_dir, "*.pickle"))
    )
    if not paths:
        raise FileNotFoundError(f"No .pkl/.pickle files found in: {pickles_dir}")
    return paths


def _read_one_pickle(path: str) -> pd.DataFrame:
    """Read one pickle and return a normalized, slim option DataFrame."""
    df = pd.read_pickle(path)
    if not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame()

    missing = [c for c in REQUIRED_BASE_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Keep required columns plus optional spot columns if present.
    keep_cols = list(REQUIRED_BASE_COLUMNS)
    keep_cols.extend([c for c in POSSIBLE_SPOT_COLUMNS if c in df.columns and c not in keep_cols])
    keep_cols = [c for c in keep_cols if c in df.columns]

    d = df[keep_cols].copy()

    # Keep only option rows.
    d = d[d["type"].astype(str).str.upper().eq("OPTION")].copy()
    if d.empty:
        return pd.DataFrame()

    d["date"] = ensure_ist(d["date"])
    d["day"] = d["date"].dt.date
    d["underlying"] = d["name"].map(normalize_underlying)
    d = d[d["underlying"].isin(TRADEABLE)].copy()
    if d.empty:
        return pd.DataFrame()

    d["expiry_date"] = pd.to_datetime(d["expiry"], errors="coerce").dt.date
    d["option_type"] = d["option_type"].astype(str).str.upper().str.strip()
    d["strike_num"] = pd.to_numeric(d["strike"], errors="coerce")
    d["strike_int"] = d["strike_num"].round().astype("Int64")
    d["close"] = pd.to_numeric(d["close"], errors="coerce")
    d["instrument"] = d["instrument"].astype(str)

    d = d.dropna(subset=["date", "day", "underlying", "expiry_date", "option_type", "strike_int", "close"])
    if d.empty:
        return pd.DataFrame()

    d["strike_int"] = d["strike_int"].astype(int)
    d = d[d["option_type"].isin(["CE", "PE"])].copy()

    # Ignore rows where the contract has already expired before the trading day.
    d = d[d["expiry_date"] >= d["day"]].copy()
    if d.empty:
        return pd.DataFrame()

    # Keep a source-file trail for debugging duplicate/merged data.
    d["source_file"] = os.path.basename(path)

    return d


def load_all_option_pickles(paths: Sequence[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load all pickles into one normalized option DataFrame.

    Returns
    -------
    options_df : pd.DataFrame
        Slim normalized option rows.
    skipped_df : pd.DataFrame
        File-level errors/warnings.
    """
    frames: List[pd.DataFrame] = []
    skipped: List[Dict[str, Any]] = []

    for i, path in enumerate(paths, start=1):
        try:
            d = _read_one_pickle(path)
            if not d.empty:
                frames.append(d)
                print(
                    f"[LOAD OK] {i}/{len(paths)} {os.path.basename(path)} "
                    f"rows={len(d):,} days={d['day'].nunique()} expiries={d['expiry_date'].nunique()}"
                )
            else:
                skipped.append({"scope": "file", "file": os.path.basename(path), "reason": "No usable option rows"})
                print(f"[LOAD SKIP] {i}/{len(paths)} {os.path.basename(path)} no usable rows")
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            skipped.append({"scope": "file", "file": os.path.basename(path), "reason": msg})
            print(f"[LOAD WARN] {i}/{len(paths)} {os.path.basename(path)} failed: {msg}")
            if FAIL_ON_PICKLE_ERROR:
                raise

    if not frames:
        raise RuntimeError("No usable option data found in the configured pickles.")

    options_df = pd.concat(frames, ignore_index=True)

    # De-duplicate exact duplicate candles. Multiple files may contain overlapping
    # rows; keeping the last row avoids double-counting without averaging prices.
    dedup_cols = ["date", "underlying", "expiry_date", "option_type", "strike_int", "instrument"]
    before = len(options_df)
    options_df = (
        options_df.sort_values(["date", "source_file"])
        .drop_duplicates(subset=dedup_cols, keep="last")
        .reset_index(drop=True)
    )
    after = len(options_df)
    if before != after:
        skipped.append(
            {
                "scope": "dedup",
                "file": "ALL",
                "reason": f"Dropped {before - after:,} exact duplicate option rows",
            }
        )

    return options_df, pd.DataFrame(skipped)


# =============================================================================
# PRICE LOOKUP AND ATM SELECTION
# =============================================================================

def lookup_price_for_leg(
    g: pd.DataFrame,
    *,
    instrument: str,
    strike: int,
    option_type: str,
    target_ts: pd.Timestamp,
    mode: str,
) -> Tuple[float, pd.Timestamp, str]:
    """
    Return close price for one leg at requested timestamp.

    The function first filters by instrument. If the instrument filter yields no
    rows, it falls back to strike+option_type. The fallback is useful when some
    pickle files have unstable instrument strings but stable strike/type columns.
    """
    sub = g[g["instrument"].astype(str).eq(str(instrument))].copy()
    if sub.empty:
        sub = g[(g["strike_int"] == int(strike)) & (g["option_type"] == option_type)].copy()

    if sub.empty:
        raise KeyError(f"No rows for {instrument} {strike}{option_type}")

    s = (
        sub[["date", "close"]]
        .dropna()
        .sort_values("date")
        .drop_duplicates(subset=["date"], keep="last")
        .set_index("date")["close"]
        .astype(float)
    )

    if s.empty:
        raise KeyError(f"No close prices for {instrument} {strike}{option_type}")

    mode = mode.lower().strip()

    # Exact candle match.
    if mode == "exact":
        if target_ts not in s.index:
            raise KeyError(f"Exact candle not found at {target_ts.strftime('%Y-%m-%d %H:%M')}")
        return float(s.loc[target_ts]), target_ts, "exact"

    # Latest candle at or before target.
    if mode == "pad":
        loc = s.index.get_indexer([target_ts], method="pad")
        if loc[0] == -1:
            raise KeyError(f"No candle at/before {target_ts.strftime('%Y-%m-%d %H:%M')}")
        used_ts = s.index[loc[0]]
        return float(s.iloc[loc[0]]), used_ts, f"pad:{used_ts.strftime('%H:%M')}"

    # First candle at or after target.
    if mode == "backfill":
        loc = s.index.get_indexer([target_ts], method="backfill")
        if loc[0] == -1:
            raise KeyError(f"No candle at/after {target_ts.strftime('%Y-%m-%d %H:%M')}")
        used_ts = s.index[loc[0]]
        return float(s.iloc[loc[0]]), used_ts, f"backfill:{used_ts.strftime('%H:%M')}"

    # Nearest candle within a tolerance.
    if mode == "nearest":
        tolerance = pd.Timedelta(minutes=MAX_TIME_LOOKUP_TOLERANCE_MINUTES)
        loc = s.index.get_indexer([target_ts], method="nearest", tolerance=tolerance)
        if loc[0] == -1:
            raise KeyError(
                f"No nearest candle within {MAX_TIME_LOOKUP_TOLERANCE_MINUTES} min of "
                f"{target_ts.strftime('%Y-%m-%d %H:%M')}"
            )
        used_ts = s.index[loc[0]]
        delta_min = abs((used_ts - target_ts).total_seconds()) / 60.0
        return float(s.iloc[loc[0]]), used_ts, f"nearest:{used_ts.strftime('%H:%M')},delta_min={delta_min:.1f}"

    raise ValueError(f"Invalid price lookup mode: {mode}")


def _find_spot_column(g: pd.DataFrame) -> Optional[str]:
    """Return the first usable spot/underlying column present in the data."""
    for col in POSSIBLE_SPOT_COLUMNS:
        if col in g.columns:
            vals = pd.to_numeric(g[col], errors="coerce")
            if vals.notna().any():
                return col
    return None


def pick_atm_from_spot(g: pd.DataFrame, entry_ts: pd.Timestamp, step: int) -> Optional[Tuple[int, float, str]]:
    """
    Try selecting ATM from spot/index price stored inside the option pickle.

    Returns (atm_strike, reference_value, method) or None if no usable spot value
    exists at the entry timestamp.
    """
    spot_col = _find_spot_column(g)
    if spot_col is None:
        return None

    # Use the first non-null value at the entry timestamp. Option rows often
    # repeat the same underlying/spot value across all strikes for that minute.
    same_min = g[g["date"].eq(entry_ts)].copy()
    if same_min.empty:
        return None

    vals = pd.to_numeric(same_min[spot_col], errors="coerce").dropna()
    if vals.empty:
        return None

    spot = float(vals.iloc[0])
    atm = round_to_step(spot, step)
    return atm, spot, f"spot_column:{spot_col}"


def pick_atm_from_chain(g: pd.DataFrame, entry_ts: pd.Timestamp) -> Tuple[int, float, str]:
    """
    Infer ATM from option chain when no spot value is available.

    It chooses the strike where CE and PE premiums are closest at the entry
    minute. This approximates the at-the-money/forward strike and is usually a
    robust fallback for backtests that only have option data.
    """
    chain = g[g["date"].eq(entry_ts)].copy()
    if chain.empty:
        raise KeyError(f"No option chain rows at entry timestamp {entry_ts.strftime('%Y-%m-%d %H:%M')}")

    piv = (
        chain.pivot_table(index="strike_int", columns="option_type", values="close", aggfunc="last")
        .dropna(subset=["CE", "PE"], how="any")
        .copy()
    )
    if piv.empty:
        raise KeyError("Cannot infer ATM: no strike has both CE and PE prices at entry time")

    piv["ce_pe_abs_diff"] = (piv["CE"] - piv["PE"]).abs()
    piv = piv.sort_values(["ce_pe_abs_diff", "strike_int"])
    atm = int(piv.index[0])
    ref = float(piv.iloc[0]["ce_pe_abs_diff"])
    return atm, ref, "chain_min_abs_CE_minus_PE"


def pick_atm_strike(g: pd.DataFrame, und: str, entry_ts: pd.Timestamp) -> Tuple[int, float, str]:
    """Pick ATM strike using spot column first, option chain fallback second."""
    step = int(STRIKE_STEP[und])
    spot_choice = pick_atm_from_spot(g, entry_ts, step)
    if spot_choice is not None:
        atm, ref, method = spot_choice
        # Validate that ATM strike actually has CE and PE rows. If not, fall back
        # to chain inference because the rounded spot may be outside available band.
        has_ce = not g[(g["date"].eq(entry_ts)) & (g["strike_int"].eq(atm)) & (g["option_type"].eq("CE"))].empty
        has_pe = not g[(g["date"].eq(entry_ts)) & (g["strike_int"].eq(atm)) & (g["option_type"].eq("PE"))].empty
        if has_ce and has_pe:
            return atm, ref, method

    return pick_atm_from_chain(g, entry_ts)


def pick_instrument(g: pd.DataFrame, strike: int, option_type: str, entry_ts: pd.Timestamp) -> str:
    """
    Pick one instrument string for a strike/type at the entry timestamp.

    If multiple symbols exist due to duplicated vendor data, the most common
    symbol in the full group is preferred; this is more stable than alphabetical
    order alone.
    """
    exact = g[(g["date"].eq(entry_ts)) & (g["strike_int"].eq(strike)) & (g["option_type"].eq(option_type))]
    if exact.empty:
        raise KeyError(f"No {strike}{option_type} candle at entry timestamp")

    counts = exact["instrument"].astype(str).value_counts()
    if counts.empty:
        raise KeyError(f"No instrument value for {strike}{option_type}")
    return str(counts.index[0])


# =============================================================================
# ELIGIBILITY RULES
# =============================================================================

def is_trade_eligible(underlying: str, entry_day: date, expiry: date) -> Tuple[bool, str]:
    """
    Apply the user's eligibility rule.

    - NIFTY/SENSEX: test only D-2 and D-1, not D.
    - BANKNIFTY: monthly expiry data; test all pre-expiry days except D.
    """
    dte = int((expiry - entry_day).days)
    if dte < 0:
        return False, "expired_contract"

    if underlying in ("NIFTY", "SENSEX"):
        if dte in (2, 1):
            return True, "NIFTY/SENSEX_D-2_or_D-1"
        return False, f"NIFTY/SENSEX only D-2/D-1; got D-{dte}"

    if underlying == "BANKNIFTY":
        if dte >= 1:
            return True, "BANKNIFTY_all_pre_expiry_days_except_D"
        return False, "BANKNIFTY expiry day D skipped"

    return False, f"Underlying not configured: {underlying}"


def next_available_trading_day(days: Sequence[date], entry_day: date) -> Optional[date]:
    """Return the next available day in the data after entry_day."""
    for d in sorted(set(days)):
        if d > entry_day:
            return d
    return None


# =============================================================================
# TRANSACTION COSTS
# =============================================================================

def compute_option_charges(legs: Sequence[Leg], qty: int) -> float:
    """
    Compute approximate all-in charges for the executed option legs.

    For each leg:
    - Entry has one order: SELL for short legs, BUY for long legs.
    - Exit has one order: BUY for short legs, SELL for long legs.

    STT applies only on sell-side premium. Stamp duty applies only on buy-side
    premium. Exchange/SEBI/IPFT apply on total premium turnover. GST applies on
    brokerage + exchange transaction charges + SEBI charges.
    """
    if not INCLUDE_TRANSACTION_COSTS:
        return 0.0

    order_count = len(legs) * 2

    entry_sell_turnover = sum(l.entry_sell_turnover_per_unit() for l in legs) * qty
    entry_buy_turnover = sum(l.entry_buy_turnover_per_unit() for l in legs) * qty
    exit_sell_turnover = sum(l.exit_sell_turnover_per_unit() for l in legs) * qty
    exit_buy_turnover = sum(l.exit_buy_turnover_per_unit() for l in legs) * qty

    sell_turnover = entry_sell_turnover + exit_sell_turnover
    buy_turnover = entry_buy_turnover + exit_buy_turnover
    total_turnover = sell_turnover + buy_turnover

    brokerage = BROKERAGE_PER_ORDER * order_count
    stt = sell_turnover * STT_SELL_PCT
    txn_charges = total_turnover * EXCHANGE_TXN_PCT
    sebi = total_turnover * SEBI_PER_CRORE / 1_00_00_000
    stamp = buy_turnover * STAMP_BUY_PCT
    ipft = total_turnover * IPFT_PER_CRORE / 1_00_00_000
    gst = (brokerage + txn_charges + sebi) * GST_PCT

    return round(brokerage + stt + txn_charges + sebi + stamp + ipft + gst, 2)


# =============================================================================
# STRATEGY SIMULATION
# =============================================================================

def wing_distances_for_underlying(und: str) -> Tuple[int, int]:
    """Return CE and PE wing distances in points for this underlying."""
    step = int(STRIKE_STEP[und])
    ce_dist = BUY_CE_DISTANCE_POINTS if BUY_CE_DISTANCE_POINTS > 0 else WING_DISTANCE_STEPS * step
    pe_dist = BUY_PE_DISTANCE_POINTS if BUY_PE_DISTANCE_POINTS > 0 else WING_DISTANCE_STEPS * step
    return int(ce_dist), int(pe_dist)


def build_iron_butterfly_legs(
    g: pd.DataFrame,
    *,
    und: str,
    atm: int,
    entry_ts: pd.Timestamp,
    exit_ts: pd.Timestamp,
) -> List[Leg]:
    """Create and price all required strategy legs."""
    legs: List[Leg] = []

    # Short ATM CE and PE are always present.
    core_specs: List[Tuple[str, str, str, int]] = [
        ("short_atm_ce", "SELL", "CE", atm),
        ("short_atm_pe", "SELL", "PE", atm),
    ]

    # Optional long wings convert the short straddle into an Iron Butterfly.
    if ENABLE_BUY_WINGS:
        ce_dist, pe_dist = wing_distances_for_underlying(und)
        core_specs.extend(
            [
                ("long_ce_wing", "BUY", "CE", atm + ce_dist),
                ("long_pe_wing", "BUY", "PE", atm - pe_dist),
            ]
        )

    for leg_name, side, option_type, strike in core_specs:
        instrument = pick_instrument(g, strike, option_type, entry_ts)

        entry_price, entry_used_ts, entry_note = lookup_price_for_leg(
            g,
            instrument=instrument,
            strike=strike,
            option_type=option_type,
            target_ts=entry_ts,
            mode=ENTRY_PRICE_LOOKUP_MODE,
        )
        exit_price, exit_used_ts, exit_note = lookup_price_for_leg(
            g,
            instrument=instrument,
            strike=strike,
            option_type=option_type,
            target_ts=exit_ts,
            mode=EXIT_PRICE_LOOKUP_MODE,
        )

        legs.append(
            Leg(
                leg_name=leg_name,
                side=side,
                option_type=option_type,
                strike=int(strike),
                instrument=instrument,
                entry_price=float(entry_price),
                exit_price=float(exit_price),
                entry_time_used=entry_used_ts.strftime("%Y-%m-%d %H:%M"),
                exit_time_used=exit_used_ts.strftime("%Y-%m-%d %H:%M"),
                price_lookup_note=f"{leg_name}:entry={entry_note};exit={exit_note}",
            )
        )

    return legs


def _get_leg(legs: Sequence[Leg], leg_name: str) -> Optional[Leg]:
    for leg in legs:
        if leg.leg_name == leg_name:
            return leg
    return None


def simulate_one_trade(
    g: pd.DataFrame,
    *,
    und: str,
    expiry: date,
    entry_day: date,
    exit_day: date,
) -> SimulationResult:
    """Simulate one overnight Iron Butterfly trade."""
    entry_ts = make_ts(entry_day, ENTRY_TIME)
    exit_ts = make_ts(exit_day, EXIT_TIME)

    atm, atm_ref, atm_method = pick_atm_strike(g, und, entry_ts)
    ce_dist, pe_dist = wing_distances_for_underlying(und)
    qty = int(QTY_UNITS[und])

    legs = build_iron_butterfly_legs(
        g,
        und=und,
        atm=atm,
        entry_ts=entry_ts,
        exit_ts=exit_ts,
    )

    short_ce = _get_leg(legs, "short_atm_ce")
    short_pe = _get_leg(legs, "short_atm_pe")
    long_ce = _get_leg(legs, "long_ce_wing")
    long_pe = _get_leg(legs, "long_pe_wing")

    if short_ce is None or short_pe is None:
        raise RuntimeError("Internal error: missing mandatory short CE/PE legs")

    # Entry credit = premium received from shorts minus premium paid for longs.
    entry_credit_per_unit = sum(
        (leg.entry_price if leg.side == "SELL" else -leg.entry_price) for leg in legs
    )

    # Exit debit = premium paid to close shorts minus premium received from selling longs.
    exit_debit_per_unit = sum(
        (leg.exit_price if leg.side == "SELL" else -leg.exit_price) for leg in legs
    )

    gross_pnl_per_unit = sum(leg.pnl_per_unit() for leg in legs)
    gross_pnl = gross_pnl_per_unit * qty
    charges = compute_option_charges(legs, qty)
    net_pnl = gross_pnl - charges

    # Use the first leg's timestamps for row-level columns. Per-leg notes are also
    # saved, so any difference caused by nearest/pad lookup remains visible.
    entry_time_used = legs[0].entry_time_used
    exit_time_used = legs[0].exit_time_used

    source_files = ",".join(sorted(g["source_file"].astype(str).unique().tolist()))
    price_notes = " | ".join(leg.price_lookup_note for leg in legs)

    return SimulationResult(
        entry_day=entry_day,
        exit_day=exit_day,
        underlying=und,
        expiry=expiry,
        days_to_expiry_at_entry=int((expiry - entry_day).days),
        days_to_expiry_at_exit=int((expiry - exit_day).days),
        strategy="IRON_BUTTERFLY" if ENABLE_BUY_WINGS else "SHORT_STRADDLE_WINGS_DISABLED",
        buy_wings_enabled=bool(ENABLE_BUY_WINGS),
        qty_units=qty,
        atm_selection_method=atm_method,
        atm_reference_value=float(atm_ref),
        atm_strike=int(atm),
        wing_distance_steps=int(WING_DISTANCE_STEPS),
        ce_wing_distance_points=int(ce_dist),
        pe_wing_distance_points=int(pe_dist),
        entry_time_requested=ENTRY_TIME_IST,
        exit_time_requested=EXIT_TIME_IST,
        entry_time_used=entry_time_used,
        exit_time_used=exit_time_used,
        short_ce_symbol=short_ce.instrument,
        short_pe_symbol=short_pe.instrument,
        long_ce_symbol=long_ce.instrument if long_ce else "DISABLED",
        long_pe_symbol=long_pe.instrument if long_pe else "DISABLED",
        short_ce_strike=short_ce.strike,
        short_pe_strike=short_pe.strike,
        long_ce_strike=long_ce.strike if long_ce else None,
        long_pe_strike=long_pe.strike if long_pe else None,
        short_ce_entry=short_ce.entry_price,
        short_pe_entry=short_pe.entry_price,
        long_ce_entry=long_ce.entry_price if long_ce else None,
        long_pe_entry=long_pe.entry_price if long_pe else None,
        short_ce_exit=short_ce.exit_price,
        short_pe_exit=short_pe.exit_price,
        long_ce_exit=long_ce.exit_price if long_ce else None,
        long_pe_exit=long_pe.exit_price if long_pe else None,
        entry_credit_per_unit=float(entry_credit_per_unit),
        exit_debit_per_unit=float(exit_debit_per_unit),
        gross_pnl_per_unit=float(gross_pnl_per_unit),
        gross_pnl=float(gross_pnl),
        txn_charges=float(charges),
        net_pnl=float(net_pnl),
        price_lookup_notes=price_notes,
        source_files=source_files,
    )


# =============================================================================
# TRADE GENERATION
# =============================================================================

def filter_banknifty_monthly_expiries(options_df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Keep only monthly BANKNIFTY expiries when BANKNIFTY_MONTHLY_EXPIRY_ONLY=1.

    The robust data-driven definition used here is: for BANKNIFTY, within each
    expiry calendar month, retain the maximum expiry_date present in the loaded
    data. This handles exchange-holiday shifts better than hard-coding weekday
    logic.
    """
    skipped: List[Dict[str, Any]] = []

    if not BANKNIFTY_MONTHLY_EXPIRY_ONLY or options_df.empty:
        return options_df, skipped

    bnf = options_df[options_df["underlying"].eq("BANKNIFTY")].copy()
    if bnf.empty:
        return options_df, skipped

    bnf["expiry_month"] = pd.to_datetime(bnf["expiry_date"]).dt.to_period("M").astype(str)
    monthly_expiry_by_month = bnf.groupby("expiry_month")["expiry_date"].max().to_dict()

    keep_mask = ~options_df["underlying"].eq("BANKNIFTY")
    bnf_month = pd.to_datetime(options_df["expiry_date"]).dt.to_period("M").astype(str)
    keep_banknifty_monthly = (
        options_df["underlying"].eq("BANKNIFTY")
        & options_df["expiry_date"].eq(bnf_month.map(monthly_expiry_by_month))
    )
    keep_mask = keep_mask | keep_banknifty_monthly

    dropped = int((~keep_mask & options_df["underlying"].eq("BANKNIFTY")).sum())
    if dropped > 0:
        skipped.append(
            {
                "scope": "BANKNIFTY_monthly_filter",
                "underlying": "BANKNIFTY",
                "reason": f"Dropped {dropped:,} BANKNIFTY rows belonging to non-monthly expiries",
            }
        )

    return options_df[keep_mask].copy(), skipped


def generate_all_trades(options_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Generate every eligible backtested trade.

    The grouping is by underlying and expiry. This is important for the overnight
    exit because the same option contract must be available on both entry day and
    next trading day.
    """
    all_rows: List[Dict[str, Any]] = []
    skipped_rows: List[Dict[str, Any]] = []

    if options_df.empty:
        return pd.DataFrame(), pd.DataFrame([{"reason": "options_df empty"}])

    # Optional data-driven monthly-expiry filter for BANKNIFTY.
    options_df, monthly_filter_skips = filter_banknifty_monthly_expiries(options_df)
    skipped_rows.extend(monthly_filter_skips)

    max_day = max(options_df["day"])
    window_start = compute_window_start(max_day, LOOKBACK_MONTHS)
    if window_start is not None:
        options_df = options_df[options_df["day"] >= window_start].copy()

    group_cols = ["underlying", "expiry_date"]
    groups = options_df.groupby(group_cols, sort=True)

    for (und, expiry), g in groups:
        try:
            if und not in TRADEABLE:
                continue

            g = g.sort_values("date").copy()
            available_days = sorted(set(g["day"].tolist()))

            for entry_day in available_days:
                eligible, rule_note = is_trade_eligible(str(und), entry_day, expiry)
                if not eligible:
                    # Keep skipped diagnostics for dates close to expiry and for
                    # BANKNIFTY expiry-day skips. Avoid filling Excel with thousands
                    # of irrelevant far-from-expiry NIFTY/SENSEX rows.
                    dte = int((expiry - entry_day).days)
                    if dte in (0, 1, 2, 3) or str(und) == "BANKNIFTY":
                        skipped_rows.append(
                            {
                                "scope": "eligibility",
                                "underlying": und,
                                "entry_day": entry_day,
                                "expiry": expiry,
                                "dte": dte,
                                "reason": rule_note,
                            }
                        )
                    continue

                exit_day = next_available_trading_day(available_days, entry_day)
                if exit_day is None:
                    skipped_rows.append(
                        {
                            "scope": "exit_day",
                            "underlying": und,
                            "entry_day": entry_day,
                            "expiry": expiry,
                            "dte": int((expiry - entry_day).days),
                            "reason": "No next available trading day in same expiry data",
                        }
                    )
                    continue

                # The exit must not be after the expiry day. D-1 exits on D are
                # valid. Exiting after D is impossible for an expired option.
                if exit_day > expiry:
                    skipped_rows.append(
                        {
                            "scope": "exit_day",
                            "underlying": und,
                            "entry_day": entry_day,
                            "exit_day": exit_day,
                            "expiry": expiry,
                            "dte": int((expiry - entry_day).days),
                            "reason": "Next available day is after expiry; cannot exit expired option",
                        }
                    )
                    continue

                try:
                    res = simulate_one_trade(
                        g,
                        und=str(und),
                        expiry=expiry,
                        entry_day=entry_day,
                        exit_day=exit_day,
                    )
                    row = res.__dict__.copy()
                    row["eligibility_rule"] = rule_note
                    all_rows.append(row)
                except Exception as e:
                    skipped_rows.append(
                        {
                            "scope": "simulation",
                            "underlying": und,
                            "entry_day": entry_day,
                            "exit_day": exit_day,
                            "expiry": expiry,
                            "dte": int((expiry - entry_day).days),
                            "reason": f"{type(e).__name__}: {e}",
                            "source_files": ",".join(sorted(g["source_file"].astype(str).unique().tolist())),
                        }
                    )

            print(f"[SIM OK] {und} expiry={expiry} days={len(available_days)}")

        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            skipped_rows.append(
                {
                    "scope": "group",
                    "underlying": und,
                    "expiry": expiry,
                    "reason": msg,
                }
            )
            print(f"[SIM WARN] {und} expiry={expiry} failed: {msg}")
            if FAIL_ON_PICKLE_ERROR:
                raise

    all_df = pd.DataFrame(all_rows)
    if not all_df.empty:
        all_df = all_df.sort_values(["entry_day", "underlying", "expiry"]).reset_index(drop=True)
        all_df["is_net_profit"] = (all_df["net_pnl"] > 0).astype(int)

    skipped_df = pd.DataFrame(skipped_rows)
    if not skipped_df.empty:
        sort_cols = [c for c in ["entry_day", "underlying", "expiry", "scope"] if c in skipped_df.columns]
        if sort_cols:
            skipped_df = skipped_df.sort_values(sort_cols, na_position="last").reset_index(drop=True)

    return all_df, skipped_df


# =============================================================================
# ACTUAL TRADES SELECTION
# =============================================================================

def _tie_rank(underlying: str) -> int:
    """Lower rank wins when two symbols are equally near expiry."""
    try:
        return ACTUAL_TRADE_TIE_BREAKER.index(underlying)
    except ValueError:
        return 999


def build_actual_trades_df(all_trades_df: pd.DataFrame) -> pd.DataFrame:
    """
    Select one actual trade per entry day.

    Rules requested:
    - Only SENSEX and NIFTY.
    - Only one trade per day.
    - Choose the trade nearest to expiry.

    Since the all-trades sheet may contain both NIFTY and SENSEX on the same day,
    ties are resolved using ACTUAL_TRADE_TIE_BREAKER.
    """
    if all_trades_df.empty:
        return pd.DataFrame()

    m = all_trades_df.copy()
    m = m[m["underlying"].isin(["NIFTY", "SENSEX"])].copy()
    if m.empty:
        return pd.DataFrame()

    # Only completed rows generated by the NIFTY/SENSEX rule should be present,
    # but keep this filter explicit for auditability.
    m = m[m["days_to_expiry_at_entry"].isin([1, 2])].copy()
    if m.empty:
        return pd.DataFrame()

    m["tie_rank"] = m["underlying"].map(_tie_rank)
    m = m.sort_values(
        [
            "entry_day",
            "days_to_expiry_at_entry",  # 1 is nearer than 2
            "expiry",
            "tie_rank",
            "underlying",
        ]
    ).copy()

    actual = m.groupby("entry_day", as_index=False).head(1).copy()
    actual = actual.drop(columns=["tie_rank"], errors="ignore")
    actual = actual.sort_values("entry_day").reset_index(drop=True)
    actual["actual_selection_note"] = "Selected one NIFTY/SENSEX trade per day with nearest expiry"
    return actual


# =============================================================================
# SUMMARIES AND EXCEL OUTPUT
# =============================================================================

def build_instrument_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Summarize all completed backtested trades by underlying."""
    if df.empty:
        return pd.DataFrame()

    tmp = df.copy()
    tmp["is_win"] = tmp["net_pnl"] > 0
    out = (
        tmp.groupby("underlying", as_index=False)
        .agg(
            trades=("net_pnl", "count"),
            total_gross_pnl=("gross_pnl", "sum"),
            total_txn_charges=("txn_charges", "sum"),
            total_net_pnl=("net_pnl", "sum"),
            avg_net_pnl=("net_pnl", "mean"),
            median_net_pnl=("net_pnl", "median"),
            win_rate_pct=("is_win", lambda s: round(100.0 * float(s.mean()), 2)),
            best_trade=("net_pnl", "max"),
            worst_trade=("net_pnl", "min"),
            avg_entry_credit_per_unit=("entry_credit_per_unit", "mean"),
            avg_gross_pnl_per_unit=("gross_pnl_per_unit", "mean"),
        )
        .sort_values("total_net_pnl", ascending=False)
        .reset_index(drop=True)
    )
    return out


def build_monthwise_summary(actual_df: pd.DataFrame) -> pd.DataFrame:
    """Build monthwise summary for actual_trades."""
    if actual_df.empty:
        return pd.DataFrame()

    tmp = actual_df.copy()
    tmp["month"] = pd.to_datetime(tmp["entry_day"]).dt.to_period("M").astype(str)
    tmp["is_win"] = tmp["net_pnl"] > 0

    out = (
        tmp.groupby("month", as_index=False)
        .agg(
            trades=("net_pnl", "count"),
            total_gross_pnl=("gross_pnl", "sum"),
            total_txn_charges=("txn_charges", "sum"),
            total_net_pnl=("net_pnl", "sum"),
            avg_net_pnl=("net_pnl", "mean"),
            median_net_pnl=("net_pnl", "median"),
            winning_trades=("is_win", "sum"),
            best_trade=("net_pnl", "max"),
            worst_trade=("net_pnl", "min"),
        )
        .reset_index(drop=True)
    )
    out["losing_trades"] = out["trades"] - out["winning_trades"]
    out["win_rate_pct"] = (100.0 * out["winning_trades"] / out["trades"]).round(2)
    return out


def build_daywise_pivot(actual_df: pd.DataFrame) -> pd.DataFrame:
    """Create compact daywise actual P&L sheet."""
    if actual_df.empty:
        return pd.DataFrame()

    cols = [
        "entry_day",
        "exit_day",
        "underlying",
        "expiry",
        "days_to_expiry_at_entry",
        "atm_strike",
        "entry_credit_per_unit",
        "gross_pnl",
        "txn_charges",
        "net_pnl",
        "is_net_profit",
    ]
    return actual_df[[c for c in cols if c in actual_df.columns]].copy()


def _autosize_columns_safe(ws) -> None:
    """Best-effort autosize for openpyxl worksheets."""
    try:
        for col_idx in range(1, (ws.max_column or 0) + 1):
            col_letter = ws.cell(row=1, column=col_idx).column_letter
            max_len = 0
            for row_idx in range(1, min(ws.max_row or 1, 2500) + 1):
                val = ws.cell(row=row_idx, column=col_idx).value
                if val is not None:
                    max_len = max(max_len, len(str(val)))
            ws.column_dimensions[col_letter].width = min(70, max(10, max_len + 2))
    except Exception:
        return


def write_excel(
    *,
    all_trades_df: pd.DataFrame,
    actual_trades_df: pd.DataFrame,
    skipped_df: pd.DataFrame,
    file_skipped_df: pd.DataFrame,
) -> None:
    """Write all requested sheets into one Excel workbook."""
    out_dir = os.path.dirname(os.path.abspath(OUTPUT_XLSX))
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    instrument_summary = build_instrument_summary(all_trades_df)
    monthwise_summary = build_monthwise_summary(actual_trades_df)
    actual_daywise = build_daywise_pivot(actual_trades_df)

    config_rows = [
        {"parameter": "PICKLES_DIR", "value": PICKLES_DIR},
        {"parameter": "ENTRY_TIME_IST", "value": ENTRY_TIME_IST},
        {"parameter": "EXIT_TIME_IST", "value": EXIT_TIME_IST},
        {"parameter": "ENTRY_PRICE_LOOKUP_MODE", "value": ENTRY_PRICE_LOOKUP_MODE},
        {"parameter": "EXIT_PRICE_LOOKUP_MODE", "value": EXIT_PRICE_LOOKUP_MODE},
        {"parameter": "MAX_TIME_LOOKUP_TOLERANCE_MINUTES", "value": MAX_TIME_LOOKUP_TOLERANCE_MINUTES},
        {"parameter": "ENABLE_BUY_WINGS", "value": ENABLE_BUY_WINGS},
        {"parameter": "WING_DISTANCE_STEPS", "value": WING_DISTANCE_STEPS},
        {"parameter": "BUY_CE_DISTANCE_POINTS", "value": BUY_CE_DISTANCE_POINTS},
        {"parameter": "BUY_PE_DISTANCE_POINTS", "value": BUY_PE_DISTANCE_POINTS},
        {"parameter": "QTY_UNITS", "value": str(QTY_UNITS)},
        {"parameter": "STRIKE_STEP", "value": str(STRIKE_STEP)},
        {"parameter": "TRADEABLE", "value": ",".join(TRADEABLE)},
        {"parameter": "BANKNIFTY_MONTHLY_EXPIRY_ONLY", "value": BANKNIFTY_MONTHLY_EXPIRY_ONLY},
        {"parameter": "ACTUAL_TRADE_TIE_BREAKER", "value": ",".join(ACTUAL_TRADE_TIE_BREAKER)},
        {"parameter": "LOOKBACK_MONTHS", "value": LOOKBACK_MONTHS},
        {"parameter": "INCLUDE_TRANSACTION_COSTS", "value": INCLUDE_TRANSACTION_COSTS},
        {"parameter": "BROKERAGE_PER_ORDER", "value": BROKERAGE_PER_ORDER},
        {"parameter": "STT_SELL_PCT", "value": STT_SELL_PCT},
        {"parameter": "EXCHANGE_TXN_PCT", "value": EXCHANGE_TXN_PCT},
        {"parameter": "STAMP_BUY_PCT", "value": STAMP_BUY_PCT},
        {"parameter": "GST_PCT", "value": GST_PCT},
        {
            "parameter": "NIFTY/SENSEX eligibility",
            "value": "Only D-2 and D-1; expiry day D excluded",
        },
        {
            "parameter": "BANKNIFTY eligibility",
            "value": "All available pre-expiry days; expiry day D excluded",
        },
        {
            "parameter": "ATM selection",
            "value": "Spot column if available; otherwise min abs(CE-PE) at entry",
        },
    ]
    config_df = pd.DataFrame(config_rows)

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as xw:
        all_trades_df.to_excel(xw, sheet_name="all_trades_backtested", index=False)
        actual_trades_df.to_excel(xw, sheet_name="actual_trades", index=False)
        actual_daywise.to_excel(xw, sheet_name="actual_daywise", index=False)
        monthwise_summary.to_excel(xw, sheet_name="monthwise_summary", index=False)
        instrument_summary.to_excel(xw, sheet_name="instrument_summary", index=False)
        skipped_df.to_excel(xw, sheet_name="skipped_trades", index=False)
        file_skipped_df.to_excel(xw, sheet_name="skipped_files", index=False)
        config_df.to_excel(xw, sheet_name="config", index=False)

        wb = xw.book
        for ws in wb.worksheets:
            ws.freeze_panes = "A2"
            _autosize_columns_safe(ws)

    print(f"[DONE] Excel written: {OUTPUT_XLSX}")


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    print("[INFO] Overnight Iron Butterfly backtest starting")
    print(f"[INFO] Pickles dir: {PICKLES_DIR}")
    print(f"[INFO] Entry: {ENTRY_TIME_IST} | Exit next trading day: {EXIT_TIME_IST}")
    print(f"[INFO] Buy wings enabled: {ENABLE_BUY_WINGS} | Wing steps: {WING_DISTANCE_STEPS}")
    print(f"[INFO] Tradeable underlyings: {TRADEABLE}")
    print(f"[INFO] BANKNIFTY monthly-expiry-only filter: {BANKNIFTY_MONTHLY_EXPIRY_ONLY}")
    print(f"[INFO] Output: {OUTPUT_XLSX}")

    paths = list_pickle_paths(PICKLES_DIR)
    print(f"[INFO] Pickle files found: {len(paths)}")

    options_df, file_skipped_df = load_all_option_pickles(paths)

    min_day = min(options_df["day"])
    max_day = max(options_df["day"])
    print(
        f"[INFO] Loaded rows={len(options_df):,} "
        f"days={options_df['day'].nunique()} range={min_day} -> {max_day} "
        f"underlyings={sorted(options_df['underlying'].unique().tolist())}"
    )

    if LOOKBACK_MONTHS > 0:
        window_start = compute_window_start(max_day, LOOKBACK_MONTHS)
        print(f"[INFO] LOOKBACK_MONTHS={LOOKBACK_MONTHS}; effective start={window_start}")

    all_trades_df, skipped_trades_df = generate_all_trades(options_df)
    actual_trades_df = build_actual_trades_df(all_trades_df)

    write_excel(
        all_trades_df=all_trades_df,
        actual_trades_df=actual_trades_df,
        skipped_df=skipped_trades_df,
        file_skipped_df=file_skipped_df,
    )

    if all_trades_df.empty:
        print("[WARN] No completed trades. Check skipped_trades and skipped_files sheets.")
        return

    print("\n[SUMMARY] All backtested trades by underlying:")
    summary = build_instrument_summary(all_trades_df)
    if not summary.empty:
        print(summary.to_string(index=False))

    if not actual_trades_df.empty:
        print("\n[SUMMARY] Actual trades:")
        print(
            actual_trades_df[["entry_day", "underlying", "expiry", "days_to_expiry_at_entry", "net_pnl"]]
            .to_string(index=False)
        )
    else:
        print("[WARN] No actual trades selected. Check whether NIFTY/SENSEX D-2/D-1 rows exist.")


if __name__ == "__main__":
    main()
