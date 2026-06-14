#!/usr/bin/env python3
"""
PairPriceDifferenceIntradayChart.py
===================================

Purpose
-------
Create an interactive intraday chart showing how the raw price difference
between two NSE stocks varies during a selected trading day.

Default pair:
    DENOMINATOR_SYMBOL = HDFCBANK
    NUMERATOR_SYMBOL   = ICICIBANK

Default difference:
    difference = NUMERATOR close - DENOMINATOR close

For default symbols:
    difference = ICICIBANK close - HDFCBANK close

Why this script exists
----------------------
This is meant to visually inspect the user's raw-difference hypothesis:

    "If the average difference between two related stocks diverges by 5/10/15
     points in either direction, does it come back?"

The script does NOT place orders and does NOT backtest. It only generates a
chart and a CSV for one input date.

What the chart shows
--------------------
1. Intraday difference line:
       NUMERATOR - DENOMINATOR

2. Same-day average difference line.

3. Previous trading day's average difference line, if available.

4. Optional +/- threshold bands around previous-day average difference.
   Example: if THRESHOLD_POINTS=10, it plots:
       prev_day_avg_diff + 10
       prev_day_avg_diff - 10

5. Markers where the current day's difference deviates from previous-day
   average by at least THRESHOLD_POINTS.

6. A second panel showing the two stock prices for sanity-checking.

Data handling
-------------
The script first tries to use cached candles. It can read:

    ./pair_difference_chart_output/candles/<SYMBOL>_1min_<YYYYMMDD>.pkl

It also searches common older cache folders such as:

    ./hdfc_icici_deviation_output/candles/
    ./hdfc_icici_difference_output/candles/

If data is not found, it downloads 1-minute historical data using your existing
Kite utility:

    Trading_2024.OptionTradeUtils.intialize_kite_api()

Dependencies
------------
pip install pandas numpy plotly python-dateutil kiteconnect

Your project dependency:
    Trading_2024.OptionTradeUtils

Typical Windows CMD usage
-------------------------
For default HDFCBANK / ICICIBANK:

    set CHART_DATE=2026-05-29
    set THRESHOLD_POINTS=10
    python PairPriceDifferenceIntradayChart.py

For another pair:

    set DENOMINATOR_SYMBOL=SBIN
    set NUMERATOR_SYMBOL=AXISBANK
    set CHART_DATE=2026-05-29
    set THRESHOLD_POINTS=5
    python PairPriceDifferenceIntradayChart.py

CLI usage is also supported:

    python PairPriceDifferenceIntradayChart.py --date 2026-05-29 --denom HDFCBANK --numer ICICIBANK --threshold 10

Output
------
Default output folder:
    ./pair_difference_chart_output

Files:
    charts/<NUMERATOR>_<DENOMINATOR>_difference_<YYYYMMDD>.html
    charts/<NUMERATOR>_<DENOMINATOR>_difference_<YYYYMMDD>.csv

Important interpretation note
-----------------------------
A raw point difference is easiest to interpret when you use equal share
quantities in both legs. For example, if the difference mean-reverts by 10
points and you use 700 shares on both legs, the gross pair PnL from the spread
movement is roughly:

    10 * 700 = Rs 7,000

If the two legs use different quantities, raw-difference logic becomes less
clean and you should adjust the chart/backtest accordingly.
"""

from __future__ import annotations

import argparse
import glob
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
except Exception as exc:  # pragma: no cover
    raise RuntimeError(
        "Plotly is required. Install it with: pip install plotly"
    ) from exc

try:
    import Trading_2024.OptionTradeUtils as oUtils
except Exception as exc:  # pragma: no cover
    raise RuntimeError(
        "Could not import Trading_2024.OptionTradeUtils. Run this script from your project "
        "environment where OptionTradeUtils is available."
    ) from exc


# =============================================================================
# CONFIGURATION
# =============================================================================

SESSION_START = dtime(9, 15, 0)
SESSION_END = dtime(15, 30, 0)

EXCHANGE = os.environ.get("EXCHANGE", "NSE").strip().upper()

# Difference is calculated as NUMERATOR - DENOMINATOR.
DENOMINATOR_SYMBOL = os.environ.get("DENOMINATOR_SYMBOL", "HDFCBANK").strip().upper()
NUMERATOR_SYMBOL = os.environ.get("NUMERATOR_SYMBOL", "ICICIBANK").strip().upper()

# Input date. Can also be supplied through --date.
CHART_DATE_ENV = os.environ.get("CHART_DATE", "2026-05-22").strip()

# Threshold in rupee points around previous-day average difference.
# Example: 10 means previous-day average diff +/- 10 points.
THRESHOLD_POINTS = float(os.environ.get("THRESHOLD_POINTS", "10"))

# Settlement band around previous-day average difference for visual reference.
# Example: 1 means +/- 1 point around previous-day average.
SETTLE_DIFF_POINTS = float(os.environ.get("SETTLE_DIFF_POINTS", "1"))

# Days to scan backwards to find the previous trading day with available data.
PREV_DAY_SEARCH_CALENDAR_DAYS = int(os.environ.get("PREV_DAY_SEARCH_CALENDAR_DAYS", "10"))

OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "./pair_difference_chart_output")

# If true, daily cache is ignored and data is downloaded again from Kite.
FORCE_DOWNLOAD = os.environ.get("FORCE_DOWNLOAD", "0").strip().lower() in {"1", "true", "yes", "y"}

# Kite historical-data robustness settings.
MAX_ATTEMPTS = int(os.environ.get("MAX_ATTEMPTS", "5"))
SLEEP_BETWEEN_CALLS_SEC = float(os.environ.get("SLEEP_BETWEEN_CALLS_SEC", "0.20"))

# Existing candle folders to search before downloading. Add your own folders via
# EXTRA_CANDLE_SEARCH_DIRS separated by semicolon on Windows or colon on Linux.
DEFAULT_CANDLE_SEARCH_DIRS = [
    "./pair_difference_chart_output/candles",
    "./hdfc_icici_difference_output/candles",
    "./hdfc_icici_deviation_output/candles",
    "./nifty_sensex_4y_deviation_output/candles",
]
EXTRA_CANDLE_SEARCH_DIRS_RAW = os.environ.get("EXTRA_CANDLE_SEARCH_DIRS", "").strip()


# =============================================================================
# DATA STRUCTURES
# =============================================================================

@dataclass(frozen=True)
class EquitySpec:
    """Minimal instrument description for NSE equity lookup."""

    exchange: str
    tradingsymbol: str


# =============================================================================
# BASIC HELPERS
# =============================================================================

def parse_date(raw: str) -> date:
    """Parse YYYY-MM-DD or DD-MM-YYYY date strings."""
    raw = str(raw).strip()
    for fmt in ("%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Could not parse date {raw!r}. Use YYYY-MM-DD or DD-MM-YYYY.")


def date_tag(d: date) -> str:
    """Return compact YYYYMMDD date tag."""
    return d.strftime("%Y%m%d")


def ensure_dirs() -> Dict[str, str]:
    """Create output folders and return their paths."""
    paths = {
        "root": OUTPUT_DIR,
        "candles": os.path.join(OUTPUT_DIR, "candles"),
        "charts": os.path.join(OUTPUT_DIR, "charts"),
    }
    for p in paths.values():
        os.makedirs(p, exist_ok=True)
    return paths


def normalize_datetime_series(s: pd.Series) -> pd.Series:
    """
    Convert date column to timezone-naive IST-like minute timestamps.

    Kite usually returns timezone-aware timestamps. This function converts them
    to Asia/Kolkata where possible and removes timezone metadata for easy merge.
    """
    out = pd.to_datetime(s, errors="coerce")
    try:
        if out.dt.tz is not None:
            out = out.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    except Exception:
        out = pd.to_datetime(s.astype(str), errors="coerce")
    return out.dt.floor("min")


def clean_ohlcv_df(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """Standardize candle DataFrame to date/open/high/low/close/volume."""
    out = df.copy()
    if "date" not in out.columns:
        raise ValueError(f"{symbol} data does not contain 'date' column.")

    # Some caches may have capitalized columns or adjusted names. Keep this
    # tolerant but require close price.
    lower_map = {c.lower(): c for c in out.columns}
    if "close" not in lower_map:
        raise ValueError(f"{symbol} data does not contain 'close' column.")

    out = out.rename(columns={lower_map["date"]: "date", lower_map["close"]: "close"})
    out["date"] = normalize_datetime_series(out["date"])
    out["close"] = pd.to_numeric(out["close"], errors="coerce")
    out = out.dropna(subset=["date", "close"])
    out = out[out["close"] > 0].copy()
    out = out.drop_duplicates("date", keep="last").sort_values("date")

    # Keep only normal market session. This also removes vendor odd rows.
    t = out["date"].dt.time
    out = out[(t >= SESSION_START) & (t <= SESSION_END)].copy()
    out = out[out["date"].dt.weekday < 5].copy()
    out["trading_date"] = out["date"].dt.date
    out["instrument"] = symbol

    keep_cols = ["date", "close", "trading_date", "instrument"]
    extra = [c for c in ["open", "high", "low", "volume"] if c in out.columns]
    return out[keep_cols + extra].reset_index(drop=True)


def search_dirs() -> List[str]:
    """Build the list of candle-cache folders to search."""
    dirs = list(DEFAULT_CANDLE_SEARCH_DIRS)
    if EXTRA_CANDLE_SEARCH_DIRS_RAW:
        # Support both Windows semicolon and Linux colon by replacing semicolon.
        raw = EXTRA_CANDLE_SEARCH_DIRS_RAW.replace(";", os.pathsep)
        dirs.extend([x.strip() for x in raw.split(os.pathsep) if x.strip()])
    # Preserve order, remove duplicates.
    return list(dict.fromkeys(dirs))


# =============================================================================
# KITE HELPERS
# =============================================================================

def kite_instruments_cached(kite, exchange: str, cache: Dict[str, List[Dict]]) -> List[Dict]:
    """Load and cache Kite instruments dump for an exchange."""
    ex = exchange.upper().strip()
    if ex not in cache:
        print(f"[STEP] Loading instruments dump for {ex} ...")
        cache[ex] = kite.instruments(ex)
        print(f"[INFO] {ex} instruments loaded: {len(cache[ex])}")
    return cache[ex]


def get_instrument_token(kite, spec: EquitySpec, cache: Dict[str, List[Dict]]) -> int:
    """Resolve NSE equity instrument token from tradingsymbol."""
    rows = kite_instruments_cached(kite, spec.exchange, cache)
    wanted = spec.tradingsymbol.upper().strip()

    matches = [r for r in rows if str(r.get("tradingsymbol", "")).upper().strip() == wanted]
    if not matches:
        raise ValueError(f"Instrument not found: {spec.exchange}:{spec.tradingsymbol}")

    # Prefer normal equity instruments when metadata is available.
    for r in matches:
        instrument_type = str(r.get("instrument_type", "")).upper().strip()
        segment = str(r.get("segment", "")).upper().strip()
        if instrument_type in {"EQ", ""} and "NSE" in segment:
            return int(r["instrument_token"])

    return int(matches[0]["instrument_token"])


def fetch_day_1min(kite, token: int, symbol: str, target_date: date) -> pd.DataFrame:
    """Download one trading day's 1-minute candles from Kite with retries."""
    from_dt = datetime.combine(target_date, SESSION_START)
    to_dt = datetime.combine(target_date, SESSION_END)

    last_err: Optional[Exception] = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            rows = kite.historical_data(
                instrument_token=token,
                from_date=from_dt,
                to_date=to_dt,
                interval="minute",
                continuous=False,
                oi=False,
            )
            df = pd.DataFrame(rows)
            if df.empty:
                return clean_ohlcv_df(pd.DataFrame(columns=["date", "close"]), symbol)
            return clean_ohlcv_df(df, symbol)
        except Exception as e:  # noqa: BLE001
            last_err = e
            wait = min(10.0, 1.5 * attempt)
            print(f"[WARN] {symbol} {target_date} attempt {attempt}/{MAX_ATTEMPTS} failed: {e}. Sleep {wait:.1f}s")
            time.sleep(wait)

    raise RuntimeError(f"Failed to download {symbol} for {target_date}: {last_err}")


# =============================================================================
# CACHE LOADING
# =============================================================================

def try_load_daily_cache(symbol: str, target_date: date, paths: Dict[str, str]) -> Optional[pd.DataFrame]:
    """Try exact daily cache first."""
    daily_path = os.path.join(paths["candles"], f"{symbol}_1min_{date_tag(target_date)}.pkl")
    if os.path.exists(daily_path) and not FORCE_DOWNLOAD:
        try:
            print(f"[CACHE] Loading daily cache: {daily_path}")
            df = pd.read_pickle(daily_path)
            df = clean_ohlcv_df(df, symbol)
            df = df[df["trading_date"] == target_date].copy()
            if not df.empty:
                return df.reset_index(drop=True)
        except Exception as e:  # noqa: BLE001
            print(f"[WARN] Failed reading daily cache {daily_path}: {e}")
    return None


def try_load_from_existing_range_cache(symbol: str, target_date: date) -> Optional[pd.DataFrame]:
    """
    Search existing range-cache pickles/CSVs and extract the target date.

    This allows the chart script to reuse 4-year data already downloaded by the
    backtest scripts without redownloading the same candles.
    """
    if FORCE_DOWNLOAD:
        return None

    patterns = []
    for folder in search_dirs():
        patterns.extend([
            os.path.join(folder, f"{symbol}_1min_*.pkl"),
            os.path.join(folder, f"{symbol}_1min_*.csv"),
            os.path.join(folder, f"{symbol.lower()}_1min*.pkl"),
            os.path.join(folder, f"{symbol.lower()}_1min*.csv"),
        ])

    files: List[str] = []
    for pat in patterns:
        files.extend(glob.glob(pat))

    # Prefer larger/date-range files after exact daily cache failed.
    files = list(dict.fromkeys(files))
    for path in files:
        try:
            if path.lower().endswith((".pkl", ".pickle")):
                df = pd.read_pickle(path)
            elif path.lower().endswith(".csv"):
                df = pd.read_csv(path)
            else:
                continue
            df = clean_ohlcv_df(df, symbol)
            day_df = df[df["trading_date"] == target_date].copy()
            if not day_df.empty:
                print(f"[CACHE] Found {symbol} {target_date} inside {path}")
                return day_df.reset_index(drop=True)
        except Exception:
            continue

    return None


def load_or_download_day(
    kite,
    symbol: str,
    target_date: date,
    paths: Dict[str, str],
    instruments_cache: Dict[str, List[Dict]],
) -> pd.DataFrame:
    """Load one day from cache/range-cache or download from Kite."""
    df = try_load_daily_cache(symbol, target_date, paths)
    if df is not None and not df.empty:
        return df

    df = try_load_from_existing_range_cache(symbol, target_date)
    if df is not None and not df.empty:
        # Also save a daily cache for next time.
        out_path = os.path.join(paths["candles"], f"{symbol}_1min_{date_tag(target_date)}.pkl")
        df.to_pickle(out_path)
        return df

    print(f"[DOWNLOAD] {symbol} {target_date} not found in cache. Downloading from Kite ...")
    token = get_instrument_token(kite, EquitySpec(EXCHANGE, symbol), instruments_cache)
    df = fetch_day_1min(kite, token, symbol, target_date)

    if not df.empty:
        out_path = os.path.join(paths["candles"], f"{symbol}_1min_{date_tag(target_date)}.pkl")
        df.to_pickle(out_path)
        df.to_csv(out_path.replace(".pkl", ".csv"), index=False)
        print(f"[DONE] Saved daily cache: {out_path} rows={len(df)}")

    return df


def align_pair_for_day(denom_df: pd.DataFrame, numer_df: pd.DataFrame, denom_symbol: str, numer_symbol: str) -> pd.DataFrame:
    """Align two daily candle DataFrames on common minute timestamps."""
    d = denom_df[["date", "close"]].rename(columns={"close": f"{denom_symbol}_close"}).copy()
    n = numer_df[["date", "close"]].rename(columns={"close": f"{numer_symbol}_close"}).copy()

    out = pd.merge(d, n, on="date", how="inner")
    out = out.dropna().sort_values("date").drop_duplicates("date", keep="last")
    out = out.reset_index(drop=True)
    if out.empty:
        return out

    out["difference"] = out[f"{numer_symbol}_close"] - out[f"{denom_symbol}_close"]
    out["trading_date"] = out["date"].dt.date
    return out


def find_previous_aligned_trading_day(
    kite,
    denom_symbol: str,
    numer_symbol: str,
    target_date: date,
    paths: Dict[str, str],
    instruments_cache: Dict[str, List[Dict]],
) -> Tuple[Optional[date], Optional[pd.DataFrame]]:
    """Find the previous date with aligned data for both symbols."""
    for offset in range(1, PREV_DAY_SEARCH_CALENDAR_DAYS + 1):
        d = target_date - timedelta(days=offset)
        if d.weekday() >= 5:
            continue
        denom_df = load_or_download_day(kite, denom_symbol, d, paths, instruments_cache)
        numer_df = load_or_download_day(kite, numer_symbol, d, paths, instruments_cache)
        aligned = align_pair_for_day(denom_df, numer_df, denom_symbol, numer_symbol)
        if not aligned.empty:
            return d, aligned
    return None, None


# =============================================================================
# CHART CREATION
# =============================================================================

def make_difference_chart(
    aligned: pd.DataFrame,
    denom_symbol: str,
    numer_symbol: str,
    target_date: date,
    prev_day_date: Optional[date],
    prev_day_avg_diff: Optional[float],
    threshold_points: float,
    settle_points: float,
    output_html: str,
) -> None:
    """Create and save an interactive Plotly HTML chart."""
    denom_col = f"{denom_symbol}_close"
    numer_col = f"{numer_symbol}_close"

    current_day_avg_diff = float(aligned["difference"].mean())
    current_day_min_diff = float(aligned["difference"].min())
    current_day_max_diff = float(aligned["difference"].max())

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        row_heights=[0.68, 0.32],
        subplot_titles=(
            f"{numer_symbol} - {denom_symbol} price difference",
            "Underlying stock prices",
        ),
    )

    # Main difference line.
    fig.add_trace(
        go.Scatter(
            x=aligned["date"],
            y=aligned["difference"],
            mode="lines",
            name=f"Difference ({numer_symbol} - {denom_symbol})",
            hovertemplate="Time=%{x|%H:%M}<br>Difference=%{y:.2f}<extra></extra>",
        ),
        row=1,
        col=1,
    )

    # Same-day average line.
    fig.add_hline(
        y=current_day_avg_diff,
        line_dash="dot",
        annotation_text=f"Day avg: {current_day_avg_diff:.2f}",
        annotation_position="top left",
        row=1,
        col=1,
    )

    # Previous-day average and bands.
    if prev_day_avg_diff is not None and np.isfinite(prev_day_avg_diff):
        fig.add_hline(
            y=prev_day_avg_diff,
            line_dash="dash",
            annotation_text=f"Prev day avg ({prev_day_date}): {prev_day_avg_diff:.2f}",
            annotation_position="bottom left",
            row=1,
            col=1,
        )
        fig.add_hline(
            y=prev_day_avg_diff + threshold_points,
            line_dash="dashdot",
            annotation_text=f"+{threshold_points:.2f} threshold",
            annotation_position="top right",
            row=1,
            col=1,
        )
        fig.add_hline(
            y=prev_day_avg_diff - threshold_points,
            line_dash="dashdot",
            annotation_text=f"-{threshold_points:.2f} threshold",
            annotation_position="bottom right",
            row=1,
            col=1,
        )
        fig.add_hline(
            y=prev_day_avg_diff + settle_points,
            line_dash="dot",
            annotation_text=f"+{settle_points:.2f} settle",
            annotation_position="top right",
            row=1,
            col=1,
        )
        fig.add_hline(
            y=prev_day_avg_diff - settle_points,
            line_dash="dot",
            annotation_text=f"-{settle_points:.2f} settle",
            annotation_position="bottom right",
            row=1,
            col=1,
        )

        # Mark threshold breach points.
        tmp = aligned.copy()
        tmp["dev_from_prev_avg"] = tmp["difference"] - prev_day_avg_diff
        breaches = tmp[tmp["dev_from_prev_avg"].abs() >= threshold_points].copy()
        if not breaches.empty:
            fig.add_trace(
                go.Scatter(
                    x=breaches["date"],
                    y=breaches["difference"],
                    mode="markers",
                    name="Threshold breach",
                    marker={"size": 7},
                    customdata=np.round(breaches["dev_from_prev_avg"].to_numpy(), 2),
                    hovertemplate="Time=%{x|%H:%M}<br>Difference=%{y:.2f}<br>Dev from prev avg=%{customdata:.2f}<extra></extra>",
                ),
                row=1,
                col=1,
            )

    # Underlying price lines.
    fig.add_trace(
        go.Scatter(
            x=aligned["date"],
            y=aligned[numer_col],
            mode="lines",
            name=numer_symbol,
            hovertemplate=f"Time=%{{x|%H:%M}}<br>{numer_symbol}=%{{y:.2f}}<extra></extra>",
        ),
        row=2,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=aligned["date"],
            y=aligned[denom_col],
            mode="lines",
            name=denom_symbol,
            hovertemplate=f"Time=%{{x|%H:%M}}<br>{denom_symbol}=%{{y:.2f}}<extra></extra>",
        ),
        row=2,
        col=1,
    )

    title = f"{numer_symbol} - {denom_symbol} intraday difference on {target_date}"
    subtitle = (
        f"Min diff {current_day_min_diff:.2f}, max diff {current_day_max_diff:.2f}, "
        f"day avg {current_day_avg_diff:.2f}"
    )

    fig.update_layout(
        title={"text": f"{title}<br><sup>{subtitle}</sup>", "x": 0.5},
        hovermode="x unified",
        height=850,
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "right", "x": 1},
        margin={"l": 70, "r": 40, "t": 110, "b": 60},
    )
    fig.update_yaxes(title_text="Price difference", row=1, col=1)
    fig.update_yaxes(title_text="Price", row=2, col=1)
    fig.update_xaxes(title_text="Time", row=2, col=1)

    fig.write_html(output_html, include_plotlyjs="cdn", full_html=True)


# =============================================================================
# MAIN
# =============================================================================

def parse_args() -> argparse.Namespace:
    """Parse command-line arguments while preserving environment defaults."""
    parser = argparse.ArgumentParser(description="Plot intraday raw price difference for two NSE stocks.")
    parser.add_argument("--date", dest="chart_date", default=CHART_DATE_ENV, help="Input date, YYYY-MM-DD or DD-MM-YYYY.")
    parser.add_argument("--denom", default=DENOMINATOR_SYMBOL, help="Denominator stock symbol, e.g. HDFCBANK.")
    parser.add_argument("--numer", default=NUMERATOR_SYMBOL, help="Numerator stock symbol, e.g. ICICIBANK.")
    parser.add_argument("--threshold", type=float, default=THRESHOLD_POINTS, help="Threshold points around previous-day average difference.")
    parser.add_argument("--settle", type=float, default=SETTLE_DIFF_POINTS, help="Settlement band points around previous-day average difference.")
    return parser.parse_args()


def main() -> None:
    """Run chart generation for one date and one stock pair."""
    args = parse_args()
    if not args.chart_date:
        raise ValueError("Provide input date through --date or CHART_DATE environment variable.")

    target_date = parse_date(args.chart_date)
    denom_symbol = str(args.denom).strip().upper()
    numer_symbol = str(args.numer).strip().upper()
    threshold_points = float(args.threshold)
    settle_points = float(args.settle)

    paths = ensure_dirs()

    print("============================================================")
    print("Pair price-difference intraday chart")
    print("============================================================")
    print(f"[CONFIG] Date        : {target_date}")
    print(f"[CONFIG] Difference  : {numer_symbol} - {denom_symbol}")
    print(f"[CONFIG] Threshold   : +/- {threshold_points} points from previous-day average")
    print(f"[CONFIG] Settle band : +/- {settle_points} points from previous-day average")
    print(f"[CONFIG] Output dir  : {OUTPUT_DIR}")

    print("\n[STEP] Initializing Kite API ...")
    kite = oUtils.intialize_kite_api()
    instruments_cache: Dict[str, List[Dict]] = {}
    print("[INFO] Kite API initialized.")

    print("\n[STEP] Loading/downloading target-date candles ...")
    denom_df = load_or_download_day(kite, denom_symbol, target_date, paths, instruments_cache)
    numer_df = load_or_download_day(kite, numer_symbol, target_date, paths, instruments_cache)
    aligned = align_pair_for_day(denom_df, numer_df, denom_symbol, numer_symbol)
    if aligned.empty:
        raise RuntimeError(f"No aligned candles found for {numer_symbol}-{denom_symbol} on {target_date}.")

    print(f"[INFO] Aligned target-date rows: {len(aligned)}")

    print("\n[STEP] Finding previous trading day baseline ...")
    prev_day_date, prev_aligned = find_previous_aligned_trading_day(
        kite=kite,
        denom_symbol=denom_symbol,
        numer_symbol=numer_symbol,
        target_date=target_date,
        paths=paths,
        instruments_cache=instruments_cache,
    )

    prev_day_avg_diff: Optional[float] = None
    if prev_aligned is not None and not prev_aligned.empty:
        prev_day_avg_diff = float(prev_aligned["difference"].mean())
        print(f"[INFO] Previous aligned trading day: {prev_day_date}; avg diff={prev_day_avg_diff:.2f}")
    else:
        print("[WARN] Previous trading day baseline not found. Chart will show only current-day average.")

    # Add diagnostic columns before saving CSV.
    aligned["day_avg_difference"] = float(aligned["difference"].mean())
    if prev_day_avg_diff is not None:
        aligned["prev_day_avg_difference"] = prev_day_avg_diff
        aligned["deviation_from_prev_day_avg"] = aligned["difference"] - prev_day_avg_diff
        aligned["abs_deviation_from_prev_day_avg"] = aligned["deviation_from_prev_day_avg"].abs()
        aligned["threshold_breach"] = aligned["abs_deviation_from_prev_day_avg"] >= threshold_points
    else:
        aligned["prev_day_avg_difference"] = np.nan
        aligned["deviation_from_prev_day_avg"] = np.nan
        aligned["abs_deviation_from_prev_day_avg"] = np.nan
        aligned["threshold_breach"] = False

    base_name = f"{numer_symbol.lower()}_{denom_symbol.lower()}_difference_{date_tag(target_date)}"
    csv_path = os.path.join(paths["charts"], f"{base_name}.csv")
    html_path = os.path.join(paths["charts"], f"{base_name}.html")

    aligned.to_csv(csv_path, index=False)
    make_difference_chart(
        aligned=aligned,
        denom_symbol=denom_symbol,
        numer_symbol=numer_symbol,
        target_date=target_date,
        prev_day_date=prev_day_date,
        prev_day_avg_diff=prev_day_avg_diff,
        threshold_points=threshold_points,
        settle_points=settle_points,
        output_html=html_path,
    )

    print("\n==================== OUTPUT ====================")
    print(f"CSV : {csv_path}")
    print(f"HTML: {html_path}")
    print("Open the HTML file in a browser to inspect the chart.")
    print("================================================")


if __name__ == "__main__":
    main()
