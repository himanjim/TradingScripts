#!/usr/bin/env python3
"""
NiftySensexDeepDeviation4YTester.py
===================================

Purpose
-------
This script tests a very specific hypothesis for the NIFTY-SENSEX pair:

    "When the normalized NIFTY-SENSEX deviation becomes large, does it
     settle down later? If yes, how long does it take and what interim
     loss would be faced by a futures trader?"

It deliberately avoids overcomplicating the first diagnostic with advanced
strategy rules. It does NOT try to optimize entry, stop-loss, trailing logic,
or brokerage assumptions. It simply creates one event-study report per z-score
threshold: |z| >= 2, |z| >= 3, |z| >= 4, |z| >= 5, etc.

For every threshold file, each row shows:
    - when the deviation entry happened,
    - whether SENSEX was rich or cheap versus NIFTY,
    - when |z| settled below the settlement threshold,
    - how many 1-minute bars it took to settle,
    - maximum adverse z-score faced while waiting,
    - maximum interim rupee loss faced while waiting,
    - final gross and net PnL using futures-like quantity assumptions.

Default futures quantity assumptions requested by user:
    - NIFTY_QTY  = 325
    - SENSEX_QTY = 100

Important simplification
------------------------
The PnL is calculated using index close levels as a proxy for futures prices.
For serious live-trade evaluation, replace index close with actual near-month
NIFTY futures and SENSEX futures prices, including rollover, basis, slippage,
charges, and execution delay.

Expected local dependency
-------------------------
This follows the user's existing code style and expects:

    import Trading_2024.OptionTradeUtils as oUtils

where oUtils.intialize_kite_api() returns an authenticated KiteConnect object.

Install dependencies if missing:

    pip install pandas numpy openpyxl python-dateutil

Typical run
-----------
    python NiftySensexDeepDeviation4YTester.py

Useful environment overrides on Windows CMD
-------------------------------------------
    set LOOKBACK_YEARS=4
    set Z_WINDOW=375
    set SETTLE_Z=0.5
    set MAX_WAIT_TRADING_DAYS=10
    set THRESHOLDS=2,3,4,5
    set NIFTY_QTY=325
    set SENSEX_QTY=100
    set FORCE_DOWNLOAD=0
    python NiftySensexDeepDeviation4YTester.py

Output
------
Default output directory:
    ./nifty_sensex_4y_deviation_output

Files created:
    candles/nifty_1min.pkl
    candles/sensex_1min.pkl
    nifty_sensex_aligned_1min.pkl
    combined_threshold_summary.xlsx
    z_ge_2/nifty_sensex_z_ge_2.xlsx
    z_ge_3/nifty_sensex_z_ge_3.xlsx
    z_ge_4/nifty_sensex_z_ge_4.xlsx
    z_ge_5/nifty_sensex_z_ge_5.xlsx

"""

from __future__ import annotations

import math
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

# Your existing project utility used in the reference downloader.
# It should return an authenticated KiteConnect object.
import Trading_2024.OptionTradeUtils as oUtils

try:
    from dateutil.relativedelta import relativedelta
except Exception:  # pragma: no cover
    relativedelta = None  # type: ignore

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


# =============================================================================
# CONFIGURATION
# =============================================================================

SESSION_START = dtime(9, 15, 0)
SESSION_END = dtime(15, 30, 0)

# 1-minute candles for many years should be downloaded in conservative chunks.
# The user's older downloader used 25-day chunks for minute data; we follow the
# same approach because it avoids Kite timeout/range issues in practice.
MAX_DAYS_PER_CHUNK = int(os.environ.get("MAX_DAYS_PER_CHUNK", "25"))
MAX_ATTEMPTS = int(os.environ.get("MAX_ATTEMPTS", "5"))
SLEEP_BETWEEN_CALLS_SEC = float(os.environ.get("SLEEP_BETWEEN_CALLS_SEC", "0.20"))

OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "nifty_sensex_4y_deviation_output")
INTERVAL = "minute"  # fixed by user request

LOOKBACK_YEARS = int(os.environ.get("LOOKBACK_YEARS", "4"))

# z-score window. For 1-minute candles, 375 bars roughly equals one full Indian
# trading session. You can test 750, 1125, etc. later.
Z_WINDOW = int(os.environ.get("Z_WINDOW", "225"))
MIN_PERIODS = int(os.environ.get("MIN_PERIODS", str(Z_WINDOW)))

# Settlement means deviation has normalized to this absolute z-score or lower.
SETTLE_Z = float(os.environ.get("SETTLE_Z", "0.5"))

# We allow multi-day settlement. 10 trading days is a diagnostic default, not a
# recommendation to carry futures positions blindly for 10 days.
MAX_WAIT_TRADING_DAYS = int(os.environ.get("MAX_WAIT_TRADING_DAYS", "10"))
INTRADAY_BARS_PER_DAY = int(os.environ.get("INTRADAY_BARS_PER_DAY", "375"))
MAX_LOOKAHEAD_BARS = int(os.environ.get("MAX_LOOKAHEAD_BARS", str(MAX_WAIT_TRADING_DAYS * INTRADAY_BARS_PER_DAY)))

# Entry thresholds. Separate Excel files are generated for each threshold.
THRESHOLDS = [float(x.strip()) for x in os.environ.get("THRESHOLDS", "2,3,4,5").split(",") if x.strip()]

# Futures-like quantities requested by the user.
NIFTY_QTY = int(os.environ.get("NIFTY_QTY", "325"))
SENSEX_QTY = int(os.environ.get("SENSEX_QTY", "100"))

# Optional flat cost per complete pair trade. Keep 0 for pure diagnostic gross PnL.
# Later, you can plug a realistic cost number after checking current brokerage,
# exchange charges, GST, STT, stamp duty, slippage, and spread.
COST_PER_TRADE_RUPEES = float(os.environ.get("COST_PER_TRADE_RUPEES", "0"))

# FORCE_DOWNLOAD=1 ignores cached candle files and downloads again.
FORCE_DOWNLOAD = os.environ.get("FORCE_DOWNLOAD", "0").strip().lower() in {"1", "true", "yes", "y"}

# Date override. If END_DATE is blank, IST today is used.
# START_DATE is optional; if blank, END_DATE - LOOKBACK_YEARS is used.
END_DATE_ENV = os.environ.get("END_DATE", "").strip()
START_DATE_ENV = os.environ.get("START_DATE", "").strip()

# Hedge beta mode:
#   FULL  : beta estimated on all available data. Good for historical diagnostic,
#           but uses look-ahead and should not be considered live-safe.
#   FIRST : beta estimated from the first BETA_TRAIN_DAYS trading days.
#
# For this simple deviation-settlement diagnostic, FULL is acceptable. For live
# strategy simulation, use FIRST or implement rolling beta.
BETA_MODE = os.environ.get("BETA_MODE", "FULL").strip().upper()
BETA_TRAIN_DAYS = int(os.environ.get("BETA_TRAIN_DAYS", "60"))

# If True, once a threshold event is detected, the scanner skips forward until
# that event settles or reaches the maximum wait window. This prevents counting
# multiple entries inside the same unresolved deviation episode.
SKIP_OVERLAPPING_EVENTS = os.environ.get("SKIP_OVERLAPPING_EVENTS", "1").strip().lower() in {"1", "true", "yes", "y"}


# =============================================================================
# DATA STRUCTURES
# =============================================================================

@dataclass(frozen=True)
class IndexSpec:
    """Minimal instrument description needed to locate the index token."""

    label: str
    exchange: str
    tradingsymbol: str


NIFTY_SPEC = IndexSpec(label="NIFTY", exchange="NSE", tradingsymbol="NIFTY 50")
SENSEX_SPEC = IndexSpec(label="SENSEX", exchange="BSE", tradingsymbol="SENSEX")


# =============================================================================
# DATE / CONFIG HELPERS
# =============================================================================

def ist_today() -> date:
    """Return today's date in India when zoneinfo is available."""
    if ZoneInfo is not None:
        try:
            return datetime.now(ZoneInfo("Asia/Kolkata")).date()
        except Exception:
            pass
    return datetime.now().date()


def parse_date(raw: str) -> date:
    """Parse YYYY-MM-DD or DD-MM-YYYY date strings."""
    raw = raw.strip()
    for fmt in ("%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Could not parse date: {raw!r}. Use YYYY-MM-DD or DD-MM-YYYY.")


def get_date_range() -> Tuple[date, date]:
    """Resolve start and end dates from env or defaults."""
    end_d = parse_date(END_DATE_ENV) if END_DATE_ENV else ist_today()

    if START_DATE_ENV:
        start_d = parse_date(START_DATE_ENV)
    else:
        if relativedelta is not None:
            start_d = end_d - relativedelta(years=LOOKBACK_YEARS)
        else:
            start_d = end_d - timedelta(days=365 * LOOKBACK_YEARS)

    if start_d >= end_d:
        raise ValueError(f"START_DATE must be earlier than END_DATE. Got {start_d} >= {end_d}")

    return start_d, end_d


def ensure_dirs() -> Dict[str, str]:
    """Create output folders and return their paths."""
    paths = {
        "root": OUTPUT_DIR,
        "candles": os.path.join(OUTPUT_DIR, "candles"),
    }
    for p in paths.values():
        os.makedirs(p, exist_ok=True)
    return paths


# =============================================================================
# KITE DOWNLOAD HELPERS
# =============================================================================

def iter_chunks_by_date(from_dt: datetime, to_dt: datetime, days_per_chunk: int) -> List[Tuple[datetime, datetime]]:
    """
    Split a datetime range into chunks while preserving intraday session times.

    This mirrors the important logic in the user's reference downloader: do not
    end intermediate chunks at the start time, otherwise most of the chunk-end
    day gets lost.
    """
    if from_dt > to_dt:
        raise ValueError("from_dt must be <= to_dt")

    chunks: List[Tuple[datetime, datetime]] = []
    cur = from_dt.date()
    end_d = to_dt.date()

    while cur <= end_d:
        chunk_end_d = min(cur + timedelta(days=days_per_chunk - 1), end_d)
        c_from = from_dt if cur == from_dt.date() else datetime.combine(cur, SESSION_START)
        c_to = to_dt if chunk_end_d == end_d else datetime.combine(chunk_end_d, SESSION_END)
        chunks.append((c_from, c_to))
        cur = chunk_end_d + timedelta(days=1)

    return chunks


def kite_instruments_cached(kite, exchange: str, cache: Dict[str, List[Dict]]) -> List[Dict]:
    """Load and cache Kite instruments dump for an exchange."""
    ex = exchange.upper().strip()
    if ex not in cache:
        print(f"[STEP] Loading instruments dump for {ex} ...")
        cache[ex] = kite.instruments(ex)
        print(f"[INFO] {ex} instruments loaded: {len(cache[ex])}")
    return cache[ex]


def get_instrument_token(kite, spec: IndexSpec, cache: Dict[str, List[Dict]]) -> Tuple[int, str]:
    """Resolve index instrument token by exchange and tradingsymbol."""
    rows = kite_instruments_cached(kite, spec.exchange, cache)
    wanted = spec.tradingsymbol.upper().strip()

    for r in rows:
        if str(r.get("tradingsymbol", "")).upper().strip() == wanted:
            return int(r["instrument_token"]), str(r.get("exchange", spec.exchange))

    raise ValueError(f"Instrument not found: {spec.exchange}:{spec.tradingsymbol}")


def fetch_history_1min(kite, instrument_token: int, from_dt: datetime, to_dt: datetime, label: str) -> List[Dict]:
    """Fetch 1-minute historical data using chunking and retry logic."""
    chunks = iter_chunks_by_date(from_dt, to_dt, MAX_DAYS_PER_CHUNK)
    print(f"[INFO] Fetching {label} token={instrument_token}, range={from_dt} to {to_dt}, chunks={len(chunks)}")

    all_rows: List[Dict] = []

    for idx, (c_from, c_to) in enumerate(chunks, start=1):
        print(f"  [CHUNK {idx:03d}/{len(chunks):03d}] {c_from} -> {c_to}")
        last_err: Optional[Exception] = None

        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                rows = kite.historical_data(
                    instrument_token=instrument_token,
                    from_date=c_from,
                    to_date=c_to,
                    interval=INTERVAL,
                    continuous=False,
                    oi=False,
                )
                print(f"    [OK] {len(rows)} candles")
                all_rows.extend(rows)
                last_err = None
                break
            except Exception as e:  # noqa: BLE001 - keep script robust for local API failures
                last_err = e
                wait = min(10.0, 1.5 * attempt)
                print(f"    [WARN] attempt {attempt}/{MAX_ATTEMPTS} failed: {e}. Sleeping {wait:.1f}s")
                time.sleep(wait)

        if last_err is not None:
            print(f"    [ERROR] Giving up on chunk {idx}/{len(chunks)} for {label}: {last_err}")

        time.sleep(SLEEP_BETWEEN_CALLS_SEC)

    return all_rows


def normalize_datetime_series(s: pd.Series) -> pd.Series:
    """
    Convert Kite date column to timezone-naive IST-like minute timestamps.

    Kite often returns timezone-aware timestamps. For joining NSE and BSE data,
    we convert to Asia/Kolkata where possible and remove timezone metadata.
    """
    out = pd.to_datetime(s)

    # If pandas detected a timezone-aware dtype, convert to IST and remove tz.
    try:
        if out.dt.tz is not None:
            out = out.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    except Exception:
        # If mixed/awkward dtype appears, fall back to parsing each value.
        out = pd.to_datetime(s.astype(str), errors="coerce")

    return out.dt.floor("min")


def rows_to_dataframe(rows: List[Dict], label: str) -> pd.DataFrame:
    """Convert Kite historical rows into a clean OHLCV DataFrame."""
    if not rows:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    df = pd.DataFrame(rows)
    for col in ["date", "open", "high", "low", "close", "volume"]:
        if col not in df.columns:
            df[col] = np.nan

    df["date"] = normalize_datetime_series(df["date"])
    df = df.dropna(subset=["date", "close"])
    df = df.drop_duplicates(subset=["date"], keep="last")
    df = df.sort_values("date").reset_index(drop=True)
    df["instrument"] = label

    # Keep only regular Indian market session minutes.
    times = df["date"].dt.time
    df = df[(times >= SESSION_START) & (times <= SESSION_END)].copy()

    # Keep weekdays only. Holidays are automatically absent because Kite returns no candles.
    df = df[df["date"].dt.weekday < 5].copy()

    return df.reset_index(drop=True)


def load_or_download_index(kite, spec: IndexSpec, start_d: date, end_d: date, paths: Dict[str, str], cache: Dict[str, List[Dict]]) -> pd.DataFrame:
    """Load cached 1-min index candles or download them from Kite."""
    out_path = os.path.join(paths["candles"], f"{spec.label.lower()}_1min.pkl")

    if os.path.exists(out_path) and not FORCE_DOWNLOAD:
        print(f"[CACHE] Loading {spec.label} candles from {out_path}")
        df = pd.read_pickle(out_path)
        df["date"] = pd.to_datetime(df["date"])
        return df

    token, real_ex = get_instrument_token(kite, spec, cache)
    from_dt = datetime.combine(start_d, SESSION_START)
    to_dt = datetime.combine(end_d, SESSION_END)

    rows = fetch_history_1min(kite, token, from_dt, to_dt, label=f"{real_ex}:{spec.tradingsymbol}")
    df = rows_to_dataframe(rows, label=spec.label)

    if df.empty:
        raise RuntimeError(f"No candle data returned for {spec.label}")

    df.to_pickle(out_path)
    df.to_csv(out_path.replace(".pkl", ".csv"), index=False)
    print(f"[DONE] Saved {spec.label} candles: {out_path} rows={len(df)}")
    return df


# =============================================================================
# SPREAD / Z-SCORE CALCULATION
# =============================================================================

def align_nifty_sensex(nifty_df: pd.DataFrame, sensex_df: pd.DataFrame) -> pd.DataFrame:
    """Inner-join NIFTY and SENSEX 1-minute closes on common timestamps."""
    n = nifty_df[["date", "close"]].copy().rename(columns={"close": "nifty_close"})
    s = sensex_df[["date", "close"]].copy().rename(columns={"close": "sensex_close"})

    n["date"] = pd.to_datetime(n["date"])
    s["date"] = pd.to_datetime(s["date"])

    df = pd.merge(n, s, on="date", how="inner")
    df = df.dropna(subset=["nifty_close", "sensex_close"])
    df = df[(df["nifty_close"] > 0) & (df["sensex_close"] > 0)]
    df = df.drop_duplicates(subset=["date"], keep="last").sort_values("date")
    df = df.reset_index(drop=True)

    df["trading_date"] = df["date"].dt.date
    return df


def estimate_beta(df: pd.DataFrame) -> float:
    """
    Estimate hedge beta in log-price space:

        log(SENSEX) = alpha + beta * log(NIFTY) + error

    The spread later becomes:

        spread = log(SENSEX) - beta * log(NIFTY)

    For this diagnostic, default beta mode is FULL. That is acceptable for a
    historical event study, but not live-safe because it uses future data.
    """
    tmp = df[["date", "ln_nifty", "ln_sensex"]].dropna().copy()

    if BETA_MODE == "FIRST":
        first_days = sorted(tmp["date"].dt.date.unique())[:BETA_TRAIN_DAYS]
        tmp = tmp[tmp["date"].dt.date.isin(first_days)].copy()
        if len(tmp) < 1000:
            raise RuntimeError("Too few rows for FIRST beta estimation. Use BETA_MODE=FULL or increase data.")
    elif BETA_MODE != "FULL":
        raise ValueError("BETA_MODE must be FULL or FIRST")

    x = tmp["ln_nifty"].to_numpy(dtype=float)
    y = tmp["ln_sensex"].to_numpy(dtype=float)

    x_var = float(np.var(x))
    if x_var == 0 or not math.isfinite(x_var):
        raise RuntimeError("Cannot estimate beta because NIFTY log-price variance is zero/invalid.")

    beta = float(np.cov(x, y, ddof=0)[0, 1] / x_var)
    return beta


def add_spread_and_zscore(df: pd.DataFrame) -> Tuple[pd.DataFrame, float]:
    """Add log prices, spread, rolling mean/std, z-score, and abs z-score."""
    out = df.copy()
    out["ln_nifty"] = np.log(out["nifty_close"])
    out["ln_sensex"] = np.log(out["sensex_close"])

    beta = estimate_beta(out)

    out["beta"] = beta
    out["spread"] = out["ln_sensex"] - beta * out["ln_nifty"]
    out["spread_mean"] = out["spread"].rolling(Z_WINDOW, min_periods=MIN_PERIODS).mean()
    out["spread_std"] = out["spread"].rolling(Z_WINDOW, min_periods=MIN_PERIODS).std(ddof=0)

    # Avoid division by zero.
    out.loc[out["spread_std"] <= 0, "spread_std"] = np.nan
    out["z"] = (out["spread"] - out["spread_mean"]) / out["spread_std"]
    out["abs_z"] = out["z"].abs()

    return out, beta


# =============================================================================
# EVENT STUDY / PNL LOGIC
# =============================================================================

def compute_pair_pnl_path(
    side: str,
    entry_nifty: float,
    entry_sensex: float,
    path_nifty: np.ndarray,
    path_sensex: np.ndarray,
) -> np.ndarray:
    """
    Compute futures-like rupee PnL path using index closes as futures proxy.

    side definitions:

    1) SENSEX_RICH_SHORT_SENSEX_LONG_NIFTY
       z is positive. SENSEX is high/rich relative to NIFTY.
       Trade: short SENSEX, long NIFTY.

       PnL = (entry_sensex - current_sensex) * SENSEX_QTY
             + (current_nifty - entry_nifty) * NIFTY_QTY

    2) SENSEX_CHEAP_LONG_SENSEX_SHORT_NIFTY
       z is negative. SENSEX is low/cheap relative to NIFTY.
       Trade: long SENSEX, short NIFTY.

       PnL = (current_sensex - entry_sensex) * SENSEX_QTY
             + (entry_nifty - current_nifty) * NIFTY_QTY
    """
    if side == "SENSEX_RICH_SHORT_SENSEX_LONG_NIFTY":
        return ((entry_sensex - path_sensex) * SENSEX_QTY) + ((path_nifty - entry_nifty) * NIFTY_QTY)

    if side == "SENSEX_CHEAP_LONG_SENSEX_SHORT_NIFTY":
        return ((path_sensex - entry_sensex) * SENSEX_QTY) + ((entry_nifty - path_nifty) * NIFTY_QTY)

    raise ValueError(f"Unknown side: {side}")


def infer_side(entry_z: float) -> str:
    """Map entry z-score sign to pair trade direction."""
    if entry_z > 0:
        return "SENSEX_RICH_SHORT_SENSEX_LONG_NIFTY"
    return "SENSEX_CHEAP_LONG_SENSEX_SHORT_NIFTY"


def build_events_for_threshold(df: pd.DataFrame, threshold: float) -> pd.DataFrame:
    """
    Build non-overlapping deviation-settlement events for a given |z| threshold.

    Entry condition:
        abs(z[i]) >= threshold AND abs(z[i-1]) < threshold

    Settlement condition:
        abs(z[j]) <= SETTLE_Z, j > i

    If settlement does not happen within MAX_LOOKAHEAD_BARS, the row is marked
    as FORCED_MAX_WAIT_EXIT.
    """
    required_cols = ["date", "trading_date", "nifty_close", "sensex_close", "z", "abs_z"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Work only with rows where z is available.
    work = df.dropna(subset=["z", "abs_z", "nifty_close", "sensex_close"]).copy().reset_index(drop=True)

    dates = work["date"].to_numpy()
    nifty = work["nifty_close"].to_numpy(dtype=float)
    sensex = work["sensex_close"].to_numpy(dtype=float)
    z = work["z"].to_numpy(dtype=float)
    abs_z = work["abs_z"].to_numpy(dtype=float)
    trading_dates = work["trading_date"].to_numpy()

    rows: List[Dict] = []
    n = len(work)
    i = 1
    event_id = 0

    while i < n:
        # Entry only at first crossing into this threshold zone.
        crossed = abs_z[i] >= threshold and abs_z[i - 1] < threshold

        if not crossed:
            i += 1
            continue

        event_id += 1
        entry_i = i
        entry_z = float(z[entry_i])
        entry_abs_z = float(abs_z[entry_i])
        side = infer_side(entry_z)

        # We scan forward for settlement up to the maximum wait window.
        max_j = min(n - 1, entry_i + MAX_LOOKAHEAD_BARS)
        settle_j: Optional[int] = None

        for j in range(entry_i + 1, max_j + 1):
            if abs_z[j] <= SETTLE_Z:
                settle_j = j
                break

        if settle_j is not None:
            exit_j = settle_j
            exit_reason = "SETTLED"
        else:
            exit_j = max_j
            exit_reason = "FORCED_MAX_WAIT_EXIT"

        # Path used to measure interim pain and final PnL.
        path_slice = slice(entry_i, exit_j + 1)
        path_nifty = nifty[path_slice]
        path_sensex = sensex[path_slice]
        path_z = z[path_slice]
        path_abs_z = abs_z[path_slice]
        path_dates = dates[path_slice]

        pnl_path = compute_pair_pnl_path(
            side=side,
            entry_nifty=float(nifty[entry_i]),
            entry_sensex=float(sensex[entry_i]),
            path_nifty=path_nifty,
            path_sensex=path_sensex,
        )

        gross_exit_pnl = float(pnl_path[-1])
        net_exit_pnl = gross_exit_pnl - COST_PER_TRADE_RUPEES

        # Maximum loss faced while waiting. If all path values are positive,
        # max_loss_rupees will be positive or zero; max_loss_abs_rupees clips it
        # to 0 for easier risk reading.
        min_pnl_idx = int(np.nanargmin(pnl_path))
        max_pnl_idx = int(np.nanargmax(pnl_path))
        max_loss_rupees = float(pnl_path[min_pnl_idx])
        max_profit_rupees = float(pnl_path[max_pnl_idx])
        max_loss_abs_rupees = abs(min(0.0, max_loss_rupees))

        max_abs_z_idx = int(np.nanargmax(path_abs_z))
        max_abs_z_value = float(path_abs_z[max_abs_z_idx])
        max_abs_z_time = pd.Timestamp(path_dates[max_abs_z_idx])

        # Direction-specific adverse z value. For positive z entry, worse means
        # z goes even more positive. For negative z entry, worse means z goes
        # even more negative.
        if entry_z > 0:
            adverse_z_value = float(np.nanmax(path_z))
            adverse_z_idx = int(np.nanargmax(path_z))
        else:
            adverse_z_value = float(np.nanmin(path_z))
            adverse_z_idx = int(np.nanargmin(path_z))
        adverse_z_time = pd.Timestamp(path_dates[adverse_z_idx])

        bars_held = int(exit_j - entry_i)
        calendar_minutes_held = float((pd.Timestamp(dates[exit_j]) - pd.Timestamp(dates[entry_i])).total_seconds() / 60.0)
        approx_trading_days_held = bars_held / float(INTRADAY_BARS_PER_DAY)

        rows.append(
            {
                "event_id": event_id,
                "threshold_abs_z": threshold,
                "entry_time": pd.Timestamp(dates[entry_i]),
                "entry_date": pd.Timestamp(dates[entry_i]).date(),
                "entry_trading_date": trading_dates[entry_i],
                "side": side,
                "entry_z": entry_z,
                "entry_abs_z": entry_abs_z,
                "entry_nifty_close": float(nifty[entry_i]),
                "entry_sensex_close": float(sensex[entry_i]),
                "settled": bool(settle_j is not None),
                "settle_time": pd.Timestamp(dates[settle_j]) if settle_j is not None else pd.NaT,
                "exit_time": pd.Timestamp(dates[exit_j]),
                "exit_reason": exit_reason,
                "exit_z": float(z[exit_j]),
                "exit_abs_z": float(abs_z[exit_j]),
                "exit_nifty_close": float(nifty[exit_j]),
                "exit_sensex_close": float(sensex[exit_j]),
                "bars_to_exit": bars_held,
                "bars_to_settle": bars_held if settle_j is not None else np.nan,
                "approx_trading_days_to_exit": approx_trading_days_held,
                "calendar_minutes_to_exit": calendar_minutes_held,
                "max_abs_z_during_wait": max_abs_z_value,
                "max_abs_z_time": max_abs_z_time,
                "directional_worst_z_during_wait": adverse_z_value,
                "directional_worst_z_time": adverse_z_time,
                "max_loss_rupees": max_loss_rupees,
                "max_loss_abs_rupees": max_loss_abs_rupees,
                "max_loss_time": pd.Timestamp(path_dates[min_pnl_idx]),
                "max_profit_rupees": max_profit_rupees,
                "max_profit_time": pd.Timestamp(path_dates[max_pnl_idx]),
                "gross_exit_pnl_rupees": gross_exit_pnl,
                "cost_rupees": COST_PER_TRADE_RUPEES,
                "net_exit_pnl_rupees": net_exit_pnl,
                "nifty_qty": NIFTY_QTY,
                "sensex_qty": SENSEX_QTY,
                "nifty_points_at_exit": float(nifty[exit_j] - nifty[entry_i]),
                "sensex_points_at_exit": float(sensex[exit_j] - sensex[entry_i]),
            }
        )

        if SKIP_OVERLAPPING_EVENTS:
            # Do not count another entry inside the same unresolved deviation.
            i = exit_j + 1
        else:
            i += 1

    return pd.DataFrame(rows)


# =============================================================================
# SUMMARY / EXCEL OUTPUT
# =============================================================================

def safe_percent(numer: float, denom: float) -> float:
    """Return percent safely."""
    return float(numer / denom * 100.0) if denom else np.nan


def summarize_events(events: pd.DataFrame, threshold: float, trading_day_count: int) -> pd.DataFrame:
    """Create a one-row summary for a threshold's event table."""
    if events.empty:
        summary = {
            "threshold_abs_z": threshold,
            "total_events": 0,
            "trading_days_in_dataset": trading_day_count,
            "events_per_trading_day": 0.0,
        }
        return pd.DataFrame([summary])

    settled = events[events["settled"] == True].copy()  # noqa: E712
    forced = events[events["settled"] == False].copy()  # noqa: E712
    pnl = events["net_exit_pnl_rupees"].astype(float)

    summary = {
        "threshold_abs_z": threshold,
        "total_events": int(len(events)),
        "trading_days_in_dataset": int(trading_day_count),
        "events_per_trading_day": float(len(events) / trading_day_count) if trading_day_count else np.nan,
        "settled_count": int(len(settled)),
        "not_settled_count": int(len(forced)),
        "settlement_rate_pct": safe_percent(len(settled), len(events)),
        "median_bars_to_settle": float(settled["bars_to_settle"].median()) if not settled.empty else np.nan,
        "p75_bars_to_settle": float(settled["bars_to_settle"].quantile(0.75)) if not settled.empty else np.nan,
        "p90_bars_to_settle": float(settled["bars_to_settle"].quantile(0.90)) if not settled.empty else np.nan,
        "p95_bars_to_settle": float(settled["bars_to_settle"].quantile(0.95)) if not settled.empty else np.nan,
        "max_bars_to_settle": float(settled["bars_to_settle"].max()) if not settled.empty else np.nan,
        "avg_max_loss_abs_rupees": float(events["max_loss_abs_rupees"].mean()),
        "median_max_loss_abs_rupees": float(events["max_loss_abs_rupees"].median()),
        "p90_max_loss_abs_rupees": float(events["max_loss_abs_rupees"].quantile(0.90)),
        "max_loss_abs_rupees_worst_case": float(events["max_loss_abs_rupees"].max()),
        "avg_max_abs_z_during_wait": float(events["max_abs_z_during_wait"].mean()),
        "p90_max_abs_z_during_wait": float(events["max_abs_z_during_wait"].quantile(0.90)),
        "max_abs_z_during_wait_worst_case": float(events["max_abs_z_during_wait"].max()),
        "gross_total_pnl_rupees": float(events["gross_exit_pnl_rupees"].sum()),
        "net_total_pnl_rupees": float(pnl.sum()),
        "avg_net_pnl_per_event": float(pnl.mean()),
        "median_net_pnl_per_event": float(pnl.median()),
        "win_count_net": int((pnl > 0).sum()),
        "loss_count_net": int((pnl <= 0).sum()),
        "win_rate_net_pct": safe_percent(int((pnl > 0).sum()), len(events)),
        "profit_factor_net": profit_factor(pnl),
        "nifty_qty": NIFTY_QTY,
        "sensex_qty": SENSEX_QTY,
        "settle_z": SETTLE_Z,
        "max_lookahead_bars": MAX_LOOKAHEAD_BARS,
        "max_wait_trading_days": MAX_WAIT_TRADING_DAYS,
        "z_window": Z_WINDOW,
        "beta_mode": BETA_MODE,
    }
    return pd.DataFrame([summary])


def profit_factor(pnl: pd.Series) -> float:
    """Gross profit divided by gross loss, using net PnL series."""
    wins = float(pnl[pnl > 0].sum())
    losses = float(-pnl[pnl < 0].sum())
    if losses == 0:
        return np.inf if wins > 0 else np.nan
    return wins / losses


def build_daily_counts(events: pd.DataFrame) -> pd.DataFrame:
    """Count entry opportunities per trading day for one threshold."""
    if events.empty:
        return pd.DataFrame(columns=["entry_date", "events"])
    out = events.groupby("entry_date", as_index=False).size().rename(columns={"size": "events"})
    return out.sort_values("entry_date").reset_index(drop=True)


def build_by_side_summary(events: pd.DataFrame) -> pd.DataFrame:
    """Summarize events separately for SENSEX-rich and SENSEX-cheap conditions."""
    if events.empty:
        return pd.DataFrame()

    rows = []
    for side, g in events.groupby("side"):
        settled_count = int(g["settled"].sum())
        pnl = g["net_exit_pnl_rupees"].astype(float)
        rows.append(
            {
                "side": side,
                "events": int(len(g)),
                "settled_count": settled_count,
                "settlement_rate_pct": safe_percent(settled_count, len(g)),
                "median_bars_to_settle": float(g.loc[g["settled"] == True, "bars_to_settle"].median()) if settled_count else np.nan,  # noqa: E712
                "avg_max_loss_abs_rupees": float(g["max_loss_abs_rupees"].mean()),
                "p90_max_loss_abs_rupees": float(g["max_loss_abs_rupees"].quantile(0.90)),
                "net_total_pnl_rupees": float(pnl.sum()),
                "avg_net_pnl_per_event": float(pnl.mean()),
                "win_rate_net_pct": safe_percent(int((pnl > 0).sum()), len(g)),
            }
        )
    return pd.DataFrame(rows)


def autosize_excel_columns(writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame, max_width: int = 45) -> None:
    """Auto-size columns for readability when using openpyxl engine."""
    try:
        ws = writer.sheets[sheet_name]
        for idx, col in enumerate(df.columns, start=1):
            values = [str(col)] + [str(x) for x in df[col].head(200).tolist()]
            width = min(max(len(x) for x in values) + 2, max_width)
            ws.column_dimensions[ws.cell(row=1, column=idx).column_letter].width = width
    except Exception:
        pass


def write_threshold_excel(threshold_dir: str, threshold: float, events: pd.DataFrame, summary: pd.DataFrame, config_df: pd.DataFrame) -> str:
    """Write one simple separate Excel workbook for a threshold."""
    os.makedirs(threshold_dir, exist_ok=True)

    # Use z_ge_2 instead of z_ge_2.0 in filenames.
    label = str(threshold).replace(".", "_").rstrip("0").rstrip("_")
    xlsx_path = os.path.join(threshold_dir, f"nifty_sensex_z_ge_{label}.xlsx")
    csv_path = os.path.join(threshold_dir, f"nifty_sensex_z_ge_{label}_events.csv")

    daily_counts = build_daily_counts(events)
    by_side = build_by_side_summary(events)

    # Important columns first for easy inspection.
    preferred_cols = [
        "event_id",
        "threshold_abs_z",
        "entry_time",
        "side",
        "entry_z",
        "entry_abs_z",
        "settled",
        "settle_time",
        "exit_time",
        "exit_reason",
        "bars_to_settle",
        "bars_to_exit",
        "approx_trading_days_to_exit",
        "entry_nifty_close",
        "entry_sensex_close",
        "exit_nifty_close",
        "exit_sensex_close",
        "max_loss_abs_rupees",
        "max_loss_rupees",
        "max_loss_time",
        "max_profit_rupees",
        "max_profit_time",
        "max_abs_z_during_wait",
        "max_abs_z_time",
        "directional_worst_z_during_wait",
        "directional_worst_z_time",
        "gross_exit_pnl_rupees",
        "cost_rupees",
        "net_exit_pnl_rupees",
        "nifty_points_at_exit",
        "sensex_points_at_exit",
        "nifty_qty",
        "sensex_qty",
    ]

    if not events.empty:
        other_cols = [c for c in events.columns if c not in preferred_cols]
        events_out = events[preferred_cols + other_cols].copy()
    else:
        events_out = events.copy()

    events_out.to_csv(csv_path, index=False)

    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        summary.to_excel(writer, sheet_name="summary", index=False)
        autosize_excel_columns(writer, "summary", summary)

        events_out.to_excel(writer, sheet_name="events", index=False)
        autosize_excel_columns(writer, "events", events_out)

        daily_counts.to_excel(writer, sheet_name="daily_counts", index=False)
        autosize_excel_columns(writer, "daily_counts", daily_counts)

        by_side.to_excel(writer, sheet_name="by_side", index=False)
        autosize_excel_columns(writer, "by_side", by_side)

        config_df.to_excel(writer, sheet_name="config", index=False)
        autosize_excel_columns(writer, "config", config_df)

    print(f"[DONE] Threshold |z| >= {threshold}: {len(events)} events -> {xlsx_path}")
    return xlsx_path


def make_config_df(start_d: date, end_d: date, beta: float, aligned_rows: int, trading_days: int) -> pd.DataFrame:
    """Write run configuration as key-value table."""
    rows = [
        ("start_date", str(start_d)),
        ("end_date", str(end_d)),
        ("interval", INTERVAL),
        ("aligned_rows", aligned_rows),
        ("trading_days", trading_days),
        ("z_window", Z_WINDOW),
        ("min_periods", MIN_PERIODS),
        ("settle_z", SETTLE_Z),
        ("thresholds", ",".join(str(x) for x in THRESHOLDS)),
        ("max_wait_trading_days", MAX_WAIT_TRADING_DAYS),
        ("max_lookahead_bars", MAX_LOOKAHEAD_BARS),
        ("nifty_qty", NIFTY_QTY),
        ("sensex_qty", SENSEX_QTY),
        ("cost_per_trade_rupees", COST_PER_TRADE_RUPEES),
        ("beta_mode", BETA_MODE),
        ("beta_train_days", BETA_TRAIN_DAYS),
        ("estimated_beta", beta),
        ("skip_overlapping_events", SKIP_OVERLAPPING_EVENTS),
        ("note", "Index close levels are used as futures proxy; replace with actual futures for live-grade backtest."),
    ]
    return pd.DataFrame(rows, columns=["parameter", "value"])


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    """Run the complete 4-year NIFTY-SENSEX deviation-depth test."""
    print("============================================================")
    print("NIFTY-SENSEX 4Y 1-min deviation settlement tester")
    print("============================================================")

    paths = ensure_dirs()
    start_d, end_d = get_date_range()

    print(f"[CONFIG] Date range       : {start_d} to {end_d}")
    print(f"[CONFIG] Output dir       : {OUTPUT_DIR}")
    print(f"[CONFIG] Thresholds       : {THRESHOLDS}")
    print(f"[CONFIG] Z_WINDOW         : {Z_WINDOW}")
    print(f"[CONFIG] SETTLE_Z         : {SETTLE_Z}")
    print(f"[CONFIG] Max wait bars    : {MAX_LOOKAHEAD_BARS} (~{MAX_WAIT_TRADING_DAYS} trading days)")
    print(f"[CONFIG] Futures qty      : NIFTY={NIFTY_QTY}, SENSEX={SENSEX_QTY}")
    print(f"[CONFIG] FORCE_DOWNLOAD   : {FORCE_DOWNLOAD}")

    print("\n[STEP] Initializing Kite API ...")
    kite = oUtils.intialize_kite_api()
    print("[INFO] Kite API initialized.")

    instruments_cache: Dict[str, List[Dict]] = {}

    print("\n[STEP] Loading/downloading NIFTY and SENSEX 1-min candles ...")
    nifty_df = load_or_download_index(kite, NIFTY_SPEC, start_d, end_d, paths, instruments_cache)
    sensex_df = load_or_download_index(kite, SENSEX_SPEC, start_d, end_d, paths, instruments_cache)

    print("\n[STEP] Aligning NIFTY and SENSEX candles ...")
    aligned = align_nifty_sensex(nifty_df, sensex_df)
    if aligned.empty:
        raise RuntimeError("No common NIFTY-SENSEX timestamps after alignment.")

    trading_days = int(aligned["trading_date"].nunique())
    print(f"[INFO] Aligned rows: {len(aligned):,}; trading days: {trading_days}")

    print("\n[STEP] Computing spread and z-score ...")
    aligned, beta = add_spread_and_zscore(aligned)
    valid_z_rows = int(aligned["z"].notna().sum())
    print(f"[INFO] Estimated beta: {beta:.6f}")
    print(f"[INFO] Valid z-score rows after rolling warmup: {valid_z_rows:,}")

    aligned_path = os.path.join(OUTPUT_DIR, "nifty_sensex_aligned_1min.pkl")
    aligned_csv_path = os.path.join(OUTPUT_DIR, "nifty_sensex_aligned_1min.csv")
    aligned.to_pickle(aligned_path)
    # CSV can be large, but it is useful for debugging; keep it optional.
    if os.environ.get("SAVE_ALIGNED_CSV", "0").strip().lower() in {"1", "true", "yes", "y"}:
        aligned.to_csv(aligned_csv_path, index=False)
    print(f"[DONE] Saved aligned dataset: {aligned_path}")

    config_df = make_config_df(start_d, end_d, beta, aligned_rows=len(aligned), trading_days=trading_days)

    all_summaries: List[pd.DataFrame] = []
    threshold_files: List[Dict] = []

    print("\n[STEP] Building threshold reports ...")
    for threshold in THRESHOLDS:
        label = str(threshold).replace(".", "_").rstrip("0").rstrip("_")
        threshold_dir = os.path.join(OUTPUT_DIR, f"z_ge_{label}")

        events = build_events_for_threshold(aligned, threshold=threshold)
        summary = summarize_events(events, threshold=threshold, trading_day_count=trading_days)
        all_summaries.append(summary)

        xlsx_path = write_threshold_excel(threshold_dir, threshold, events, summary, config_df)
        threshold_files.append({"threshold_abs_z": threshold, "events": len(events), "file": xlsx_path})

    combined_summary = pd.concat(all_summaries, ignore_index=True) if all_summaries else pd.DataFrame()
    files_df = pd.DataFrame(threshold_files)

    combined_path = os.path.join(OUTPUT_DIR, "combined_threshold_summary.xlsx")
    with pd.ExcelWriter(combined_path, engine="openpyxl") as writer:
        combined_summary.to_excel(writer, sheet_name="combined_summary", index=False)
        autosize_excel_columns(writer, "combined_summary", combined_summary)

        files_df.to_excel(writer, sheet_name="files", index=False)
        autosize_excel_columns(writer, "files", files_df)

        config_df.to_excel(writer, sheet_name="config", index=False)
        autosize_excel_columns(writer, "config", config_df)

    print("\n==================== FINAL SUMMARY ====================")
    if not combined_summary.empty:
        print(combined_summary[[
            "threshold_abs_z",
            "total_events",
            "events_per_trading_day",
            "settlement_rate_pct",
            "median_bars_to_settle",
            "p90_bars_to_settle",
            "max_loss_abs_rupees_worst_case",
            "net_total_pnl_rupees",
            "win_rate_net_pct",
        ]].to_string(index=False))
    print("-------------------------------------------------------")
    print(f"Combined summary: {combined_path}")
    print(f"Output directory : {OUTPUT_DIR}")
    print("=======================================================")


if __name__ == "__main__":
    main()
