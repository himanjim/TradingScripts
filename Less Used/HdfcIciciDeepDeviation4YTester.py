#!/usr/bin/env python3
"""
HdfcIciciDeepDeviation4YTester.py
=================================

Purpose
-------
Backtest a z-score mean-reversion event study for the HDFCBANK-ICICIBANK pair.

This is a stock-pair version of the earlier NIFTY-SENSEX deviation tester.
It tests the hypothesis:

    "When the normalized ICICIBANK/HDFCBANK statistical spread becomes large,
     does it settle later? If yes, how long does it take and what interim
     loss/profit would a pair-trade face?"

Default pair
------------
    Denominator / hedge leg : HDFCBANK
    Numerator / rich-cheap leg: ICICIBANK

The log spread is:

    spread = log(ICICIBANK) - beta * log(HDFCBANK)

The z-score is:

    z = (spread - rolling_mean(spread)) / rolling_std(spread)

Interpretation
--------------
If z > 0:
    ICICIBANK is rich versus HDFCBANK.
    Pair trade assumed:
        SHORT ICICIBANK
        LONG  HDFCBANK

If z < 0:
    ICICIBANK is cheap versus HDFCBANK.
    Pair trade assumed:
        LONG  ICICIBANK
        SHORT HDFCBANK

Data handling
-------------
The script first tries to load cached 1-minute candles from:

    ./hdfc_icici_deviation_output/candles/

If missing, or if FORCE_DOWNLOAD=1, it downloads 1-minute NSE historical data
using your existing Kite utility:

    Trading_2024.OptionTradeUtils.intialize_kite_api()

The cache filenames include the date range, so old data is less likely to be
accidentally reused after changing START_DATE / END_DATE / LOOKBACK_YEARS.

PnL modes
---------
For stock pairs, using fixed quantities blindly is usually poor. This script
therefore supports two quantity modes:

1. NOTIONAL_BALANCED  [default]
   At entry, both legs are sized approximately to the same rupee notional.

       denominator_qty = BASE_NOTIONAL_RUPEES / HDFCBANK price
       numerator_qty   = BASE_NOTIONAL_RUPEES / ICICIBANK price

   This is useful for diagnostic stock-pair testing.

2. FIXED
   Use explicitly configured quantities:

       DENOMINATOR_QTY_FIXED = HDFCBANK quantity
       NUMERATOR_QTY_FIXED   = ICICIBANK quantity

Trading-style exits now supported
----------------------------------
The original event study waited until the z-score settled or until a large
diagnostic lookahead window expired. This updated version can also exit earlier
using two configurable trading-style risk controls:

    HARD_EXIT_BARS      = maximum bars to hold after entry. Default: 60.
    STOP_LOSS_RUPEES    = maximum rupee loss tolerated. Default: 5000.

A trade exits on the first condition that occurs:

    1. PnL <= -STOP_LOSS_RUPEES, if STOP_LOSS_RUPEES > 0
    2. abs(z) <= SETTLE_Z
    3. bars held >= HARD_EXIT_BARS, if HARD_EXIT_BARS > 0
    4. MAX_LOOKAHEAD_BARS is reached

The stop-loss is checked on 1-minute close data. Therefore, the actual exit loss
can be worse than the configured stop if PnL jumps across the stop between bars.

Important limitations
---------------------
1. This is still a research/event-study script, not a live-trading system.
2. It uses equity close prices as proxy execution prices.
3. It does not include impact cost, bid-ask spread, taxes, or actual futures lots.
4. BETA_MODE=FULL uses future data and is useful only for diagnostics. For
   live-style testing, use BETA_MODE=FIRST.

Install dependencies
--------------------
    pip install pandas numpy openpyxl python-dateutil

Typical run
-----------
    python HdfcIciciDeepDeviation4YTester.py

Recommended Windows CMD run
---------------------------
    set LOOKBACK_YEARS=4
    set Z_WINDOW=225
    set THRESHOLDS=2,3,4,5
    set SETTLE_Z=0.5
    set QTY_MODE=NOTIONAL_BALANCED
    set BASE_NOTIONAL_RUPEES=1000000
    set HARD_EXIT_BARS=60
    set STOP_LOSS_RUPEES=5000
    set FORCE_DOWNLOAD=0
    python HdfcIciciDeepDeviation4YTester.py

Useful overrides
----------------
    set DENOMINATOR_SYMBOL=HDFCBANK
    set NUMERATOR_SYMBOL=ICICIBANK
    set DENOMINATOR_LABEL=HDFCBANK
    set NUMERATOR_LABEL=ICICIBANK

    set START_DATE=2022-05-29
    set END_DATE=2026-05-29
    set FORCE_DOWNLOAD=1

    set BETA_MODE=FIRST
    set BETA_TRAIN_DAYS=60

Output
------
Default output directory:

    ./hdfc_icici_deviation_output

Files created:
    candles/HDFCBANK_1min_<start>_<end>.pkl/csv
    candles/ICICIBANK_1min_<start>_<end>.pkl/csv
    hdfc_icici_aligned_1min.pkl
    combined_threshold_summary.xlsx
    z_ge_2/hdfc_icici_z_ge_2.xlsx
    z_ge_3/hdfc_icici_z_ge_3.xlsx
    ...

"""

from __future__ import annotations

import math
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# Your existing project utility used in earlier trading scripts.
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

# NSE equity symbols. You can override these to test any two NSE stocks.
DENOMINATOR_SYMBOL = os.environ.get("DENOMINATOR_SYMBOL", "HDFCBANK").strip().upper()
NUMERATOR_SYMBOL = os.environ.get("NUMERATOR_SYMBOL", "ICICIBANK").strip().upper()

DENOMINATOR_LABEL = os.environ.get("DENOMINATOR_LABEL", DENOMINATOR_SYMBOL).strip().upper()
NUMERATOR_LABEL = os.environ.get("NUMERATOR_LABEL", NUMERATOR_SYMBOL).strip().upper()

EXCHANGE = os.environ.get("EXCHANGE", "NSE").strip().upper()

# 1-minute candles for many years should be downloaded in conservative chunks.
MAX_DAYS_PER_CHUNK = int(os.environ.get("MAX_DAYS_PER_CHUNK", "25"))
MAX_ATTEMPTS = int(os.environ.get("MAX_ATTEMPTS", "5"))
SLEEP_BETWEEN_CALLS_SEC = float(os.environ.get("SLEEP_BETWEEN_CALLS_SEC", "0.20"))

OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "./hdfc_icici_deviation_output")
INTERVAL = "minute"

LOOKBACK_YEARS = int(os.environ.get("LOOKBACK_YEARS", "4"))

# For intraday stock pairs, 225 or 375 are sensible starting points.
# 225 = roughly 3.75 trading hours. 375 = full trading day.
Z_WINDOW = int(os.environ.get("Z_WINDOW", "225"))
MIN_PERIODS = int(os.environ.get("MIN_PERIODS", str(Z_WINDOW)))

# Settlement means z has normalized to this absolute level or lower.
SETTLE_Z = float(os.environ.get("SETTLE_Z", "0.5"))

# Diagnostic maximum wait. This is not a recommendation to hold blindly.
MAX_WAIT_TRADING_DAYS = int(os.environ.get("MAX_WAIT_TRADING_DAYS", "10"))
INTRADAY_BARS_PER_DAY = int(os.environ.get("INTRADAY_BARS_PER_DAY", "375"))
MAX_LOOKAHEAD_BARS = int(
    os.environ.get("MAX_LOOKAHEAD_BARS", str(MAX_WAIT_TRADING_DAYS * INTRADAY_BARS_PER_DAY))
)

# Entry thresholds. Separate Excel files are generated for each.
THRESHOLDS = [
    float(x.strip())
    for x in os.environ.get("THRESHOLDS", "2,3,4,5").split(",")
    if x.strip()
]

# Quantity mode:
#   NOTIONAL_BALANCED: both legs get approximately equal rupee notional.
#   FIXED: use fixed quantities configured below.
QTY_MODE = os.environ.get("QTY_MODE", "NOTIONAL_BALANCED").strip().upper()
BASE_NOTIONAL_RUPEES = float(os.environ.get("BASE_NOTIONAL_RUPEES", "1000000"))

# Fixed quantities, used only when QTY_MODE=FIXED.
# Denominator = HDFCBANK by default.
# Numerator   = ICICIBANK by default.
DENOMINATOR_QTY_FIXED = int(os.environ.get("DENOMINATOR_QTY_FIXED", "550"))
NUMERATOR_QTY_FIXED = int(os.environ.get("NUMERATOR_QTY_FIXED", "700"))

# Round notional-balanced share quantities to this step.
# For equity spot diagnostic, 1 is fine. For futures-style testing, set this to
# current lot size or manually use QTY_MODE=FIXED.
QTY_ROUND_STEP = int(os.environ.get("QTY_ROUND_STEP", "1"))

# Optional flat cost per complete pair trade. Keep 0 for pure diagnostic.
COST_PER_TRADE_RUPEES = float(os.environ.get("COST_PER_TRADE_RUPEES", "0"))

# -----------------------------------------------------------------------------
# Trading-style exits requested by user
# -----------------------------------------------------------------------------
# HARD_EXIT_BARS limits how long one pair trade is allowed to remain open after
# entry. Default 60 means: if z has not settled within 60 one-minute bars, exit
# at the 60th bar. Set 0 to disable this rule and use only MAX_LOOKAHEAD_BARS.
HARD_EXIT_BARS = int(os.environ.get("HARD_EXIT_BARS", "240"))

# STOP_LOSS_RUPEES is the maximum loss tolerated on the pair position. Default
# 5000 means: exit when current bar-close PnL is <= -5000. Set 0 to disable.
# Because data is 1-minute OHLC close based, actual loss can exceed this number
# if the pair jumps from above the stop to below the stop between two closes.
STOP_LOSS_RUPEES = float(os.environ.get("STOP_LOSS_RUPEES", "5000"))

# FORCE_DOWNLOAD=1 ignores cached candle files and downloads again.
FORCE_DOWNLOAD = os.environ.get("FORCE_DOWNLOAD", "0").strip().lower() in {"1", "true", "yes", "y"}

# Date override. If END_DATE is blank, IST today is used.
END_DATE_ENV = os.environ.get("END_DATE", "").strip()
START_DATE_ENV = os.environ.get("START_DATE", "").strip()

# Hedge beta mode:
#   FULL  : beta estimated on all available data. Diagnostic only; look-ahead.
#   FIRST : beta estimated from first BETA_TRAIN_DAYS trading days.
BETA_MODE = os.environ.get("BETA_MODE", "FULL").strip().upper()
BETA_TRAIN_DAYS = int(os.environ.get("BETA_TRAIN_DAYS", "60"))

# If True, one unresolved deviation episode is counted only once.
SKIP_OVERLAPPING_EVENTS = os.environ.get("SKIP_OVERLAPPING_EVENTS", "1").strip().lower() in {
    "1", "true", "yes", "y"
}

# Optional entry time filter. OFF by default for pure event study.
ENABLE_ENTRY_TIME_FILTER = os.environ.get("ENABLE_ENTRY_TIME_FILTER", "0").strip().lower() in {
    "1", "true", "yes", "y"
}
ENTRY_START_TIME = dtime.fromisoformat(os.environ.get("ENTRY_START_TIME", "09:30"))
LAST_ENTRY_TIME = dtime.fromisoformat(os.environ.get("LAST_ENTRY_TIME", "14:30"))


# =============================================================================
# DATA STRUCTURES
# =============================================================================

@dataclass(frozen=True)
class EquitySpec:
    """Minimal instrument description needed to locate an NSE equity token."""

    label: str
    exchange: str
    tradingsymbol: str


DENOMINATOR_SPEC = EquitySpec(label=DENOMINATOR_LABEL, exchange=EXCHANGE, tradingsymbol=DENOMINATOR_SYMBOL)
NUMERATOR_SPEC = EquitySpec(label=NUMERATOR_LABEL, exchange=EXCHANGE, tradingsymbol=NUMERATOR_SYMBOL)


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


def cache_label(start_d: date, end_d: date) -> str:
    """Return compact date-range label for candle cache filenames."""
    return f"{start_d.strftime('%Y%m%d')}_{end_d.strftime('%Y%m%d')}"


def round_qty(qty: float) -> int:
    """
    Round quantity to configured step and ensure at least one share.

    QTY_ROUND_STEP=1 gives share-level diagnostic sizing.
    For futures-style approximation, set QTY_ROUND_STEP to current lot size.
    """
    if not math.isfinite(qty) or qty <= 0:
        return 0
    step = max(1, int(QTY_ROUND_STEP))
    return max(step, int(round(qty / step) * step))


# =============================================================================
# KITE DOWNLOAD HELPERS
# =============================================================================

def iter_chunks_by_date(from_dt: datetime, to_dt: datetime, days_per_chunk: int) -> List[Tuple[datetime, datetime]]:
    """Split datetime range into chunks while preserving intraday session times."""
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


def get_instrument_token(kite, spec: EquitySpec, cache: Dict[str, List[Dict]]) -> Tuple[int, str]:
    """
    Resolve instrument token by exchange and tradingsymbol.

    For NSE equities, tradingsymbols are usually simple strings such as:
        HDFCBANK
        ICICIBANK
    """
    rows = kite_instruments_cached(kite, spec.exchange, cache)
    wanted = spec.tradingsymbol.upper().strip()

    exact_matches = [
        r for r in rows
        if str(r.get("tradingsymbol", "")).upper().strip() == wanted
    ]

    if not exact_matches:
        raise ValueError(f"Instrument not found: {spec.exchange}:{spec.tradingsymbol}")

    # Prefer normal EQ segment/equity instruments when metadata is present.
    for r in exact_matches:
        segment = str(r.get("segment", "")).upper()
        instrument_type = str(r.get("instrument_type", "")).upper()
        if "NSE" in segment and instrument_type in {"EQ", ""}:
            return int(r["instrument_token"]), str(r.get("exchange", spec.exchange))

    r = exact_matches[0]
    return int(r["instrument_token"]), str(r.get("exchange", spec.exchange))


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
            except Exception as e:  # noqa: BLE001
                last_err = e
                wait = min(10.0, 1.5 * attempt)
                print(f"    [WARN] attempt {attempt}/{MAX_ATTEMPTS} failed: {e}. Sleeping {wait:.1f}s")
                time.sleep(wait)

        if last_err is not None:
            print(f"    [ERROR] Giving up on chunk {idx}/{len(chunks)} for {label}: {last_err}")

        time.sleep(SLEEP_BETWEEN_CALLS_SEC)

    return all_rows


def normalize_datetime_series(s: pd.Series) -> pd.Series:
    """Convert Kite date column to timezone-naive IST-like minute timestamps."""
    out = pd.to_datetime(s)

    try:
        if out.dt.tz is not None:
            out = out.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    except Exception:
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

    times = df["date"].dt.time
    df = df[(times >= SESSION_START) & (times <= SESSION_END)].copy()
    df = df[df["date"].dt.weekday < 5].copy()

    return df.reset_index(drop=True)


def load_or_download_equity(
    kite,
    spec: EquitySpec,
    start_d: date,
    end_d: date,
    paths: Dict[str, str],
    cache: Dict[str, List[Dict]],
) -> pd.DataFrame:
    """
    Load cached 1-minute equity candles or download them from Kite.

    Cache filenames include start/end dates to prevent accidental reuse of old
    date ranges.
    """
    range_tag = cache_label(start_d, end_d)
    out_path = os.path.join(paths["candles"], f"{spec.label}_1min_{range_tag}.pkl")

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

def align_pair(denom_df: pd.DataFrame, numer_df: pd.DataFrame) -> pd.DataFrame:
    """Inner-join denominator and numerator 1-minute closes on common timestamps."""
    d = denom_df[["date", "close"]].copy().rename(columns={"close": "denom_close"})
    n = numer_df[["date", "close"]].copy().rename(columns={"close": "numer_close"})

    d["date"] = pd.to_datetime(d["date"])
    n["date"] = pd.to_datetime(n["date"])

    df = pd.merge(d, n, on="date", how="inner")
    df = df.dropna(subset=["denom_close", "numer_close"])
    df = df[(df["denom_close"] > 0) & (df["numer_close"] > 0)]
    df = df.drop_duplicates(subset=["date"], keep="last").sort_values("date")
    df = df.reset_index(drop=True)

    df["trading_date"] = df["date"].dt.date
    return df


def estimate_beta(df: pd.DataFrame) -> float:
    """
    Estimate hedge beta in log-price space:

        log(NUMERATOR) = alpha + beta * log(DENOMINATOR) + error

    The spread later becomes:

        spread = log(NUMERATOR) - beta * log(DENOMINATOR)

    FULL uses all data and is diagnostic only. FIRST uses the first
    BETA_TRAIN_DAYS trading days.
    """
    tmp = df[["date", "ln_denom", "ln_numer"]].dropna().copy()

    if BETA_MODE == "FIRST":
        first_days = sorted(tmp["date"].dt.date.unique())[:BETA_TRAIN_DAYS]
        tmp = tmp[tmp["date"].dt.date.isin(first_days)].copy()
        if len(tmp) < 1000:
            raise RuntimeError("Too few rows for FIRST beta estimation. Use BETA_MODE=FULL or increase data.")
    elif BETA_MODE != "FULL":
        raise ValueError("BETA_MODE must be FULL or FIRST")

    x = tmp["ln_denom"].to_numpy(dtype=float)
    y = tmp["ln_numer"].to_numpy(dtype=float)

    x_var = float(np.var(x))
    if x_var == 0 or not math.isfinite(x_var):
        raise RuntimeError(f"Cannot estimate beta because {DENOMINATOR_LABEL} log-price variance is zero/invalid.")

    return float(np.cov(x, y, ddof=0)[0, 1] / x_var)


def add_spread_and_zscore(df: pd.DataFrame) -> Tuple[pd.DataFrame, float]:
    """Add log prices, beta, spread, rolling mean/std, z-score and abs-z."""
    out = df.copy()
    out["ln_denom"] = np.log(out["denom_close"])
    out["ln_numer"] = np.log(out["numer_close"])

    beta = estimate_beta(out)

    out["beta"] = beta
    out["spread"] = out["ln_numer"] - beta * out["ln_denom"]
    out["spread_mean"] = out["spread"].rolling(Z_WINDOW, min_periods=MIN_PERIODS).mean()
    out["spread_std"] = out["spread"].rolling(Z_WINDOW, min_periods=MIN_PERIODS).std(ddof=0)

    out.loc[out["spread_std"] <= 0, "spread_std"] = np.nan
    out["z"] = (out["spread"] - out["spread_mean"]) / out["spread_std"]
    out["abs_z"] = out["z"].abs()

    return out, beta


# =============================================================================
# EVENT STUDY / PNL LOGIC
# =============================================================================

def compute_quantities(entry_denom: float, entry_numer: float) -> Tuple[int, int]:
    """
    Return denominator and numerator quantities for a trade entry.

    In NOTIONAL_BALANCED mode:
        denom_qty ≈ BASE_NOTIONAL_RUPEES / entry_denom_price
        numer_qty ≈ BASE_NOTIONAL_RUPEES / entry_numer_price

    In FIXED mode:
        use DENOMINATOR_QTY_FIXED and NUMERATOR_QTY_FIXED.
    """
    if QTY_MODE == "FIXED":
        return int(DENOMINATOR_QTY_FIXED), int(NUMERATOR_QTY_FIXED)

    if QTY_MODE == "NOTIONAL_BALANCED":
        denom_qty = round_qty(BASE_NOTIONAL_RUPEES / entry_denom)
        numer_qty = round_qty(BASE_NOTIONAL_RUPEES / entry_numer)
        return int(denom_qty), int(numer_qty)

    raise ValueError("QTY_MODE must be NOTIONAL_BALANCED or FIXED")


def compute_pair_pnl_path(
    side: str,
    entry_denom: float,
    entry_numer: float,
    path_denom: np.ndarray,
    path_numer: np.ndarray,
    denom_qty: int,
    numer_qty: int,
) -> np.ndarray:
    """
    Compute rupee PnL path.

    side definitions:

    1) NUMERATOR_RICH_SHORT_NUMERATOR_LONG_DENOMINATOR
       z is positive. NUMERATOR is rich relative to DENOMINATOR.
       Trade: short numerator, long denominator.

       PnL = (entry_numer - current_numer) * numer_qty
             + (current_denom - entry_denom) * denom_qty

    2) NUMERATOR_CHEAP_LONG_NUMERATOR_SHORT_DENOMINATOR
       z is negative. NUMERATOR is cheap relative to DENOMINATOR.
       Trade: long numerator, short denominator.

       PnL = (current_numer - entry_numer) * numer_qty
             + (entry_denom - current_denom) * denom_qty
    """
    if side == "NUMERATOR_RICH_SHORT_NUMERATOR_LONG_DENOMINATOR":
        return ((entry_numer - path_numer) * numer_qty) + ((path_denom - entry_denom) * denom_qty)

    if side == "NUMERATOR_CHEAP_LONG_NUMERATOR_SHORT_DENOMINATOR":
        return ((path_numer - entry_numer) * numer_qty) + ((entry_denom - path_denom) * denom_qty)

    raise ValueError(f"Unknown side: {side}")


def current_pair_pnl(
    side: str,
    entry_denom: float,
    entry_numer: float,
    current_denom: float,
    current_numer: float,
    denom_qty: int,
    numer_qty: int,
) -> float:
    """
    Compute current rupee PnL for one bar.

    This function is used inside the forward scan so that the script can exit
    immediately when the configured STOP_LOSS_RUPEES is breached. It uses the
    same side definitions as compute_pair_pnl_path(), but avoids rebuilding the
    whole path for every forward bar.
    """
    if side == "NUMERATOR_RICH_SHORT_NUMERATOR_LONG_DENOMINATOR":
        return ((entry_numer - current_numer) * numer_qty) + ((current_denom - entry_denom) * denom_qty)

    if side == "NUMERATOR_CHEAP_LONG_NUMERATOR_SHORT_DENOMINATOR":
        return ((current_numer - entry_numer) * numer_qty) + ((entry_denom - current_denom) * denom_qty)

    raise ValueError(f"Unknown side: {side}")


def infer_side(entry_z: float) -> str:
    """Map z-score sign to pair trade direction."""
    if entry_z > 0:
        return "NUMERATOR_RICH_SHORT_NUMERATOR_LONG_DENOMINATOR"
    return "NUMERATOR_CHEAP_LONG_NUMERATOR_SHORT_DENOMINATOR"


def is_time_allowed(ts: pd.Timestamp) -> bool:
    """Return True if optional entry-time filter allows the timestamp."""
    if not ENABLE_ENTRY_TIME_FILTER:
        return True
    t = ts.time()
    return ENTRY_START_TIME <= t <= LAST_ENTRY_TIME


def build_events_for_threshold(df: pd.DataFrame, threshold: float) -> pd.DataFrame:
    """
    Build non-overlapping deviation-settlement events for one |z| threshold.

    Entry:
        abs(z[i]) >= threshold AND abs(z[i-1]) < threshold

    Exit:
        first future row where one of these occurs:

        1. PnL <= -STOP_LOSS_RUPEES, if stop-loss is enabled
        2. abs(z[j]) <= SETTLE_Z
        3. bars held >= HARD_EXIT_BARS, if hard bar exit is enabled
        4. max lookahead if none of the above occurs

    Stop-loss is checked before z-settlement. This is conservative: if a bar
    both settles statistically and is already below the rupee stop, the row is
    classified as STOP_LOSS_RUPEES.
    """
    required_cols = ["date", "trading_date", "denom_close", "numer_close", "z", "abs_z"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    work = df.dropna(subset=["z", "abs_z", "denom_close", "numer_close"]).copy().reset_index(drop=True)

    dates = work["date"].to_numpy()
    denom = work["denom_close"].to_numpy(dtype=float)
    numer = work["numer_close"].to_numpy(dtype=float)
    z = work["z"].to_numpy(dtype=float)
    abs_z = work["abs_z"].to_numpy(dtype=float)
    trading_dates = work["trading_date"].to_numpy()

    rows: List[Dict] = []
    n = len(work)
    i = 1
    event_id = 0

    while i < n:
        crossed = abs_z[i] >= threshold and abs_z[i - 1] < threshold

        if not crossed:
            i += 1
            continue

        entry_time = pd.Timestamp(dates[i])
        if not is_time_allowed(entry_time):
            i += 1
            continue

        event_id += 1
        entry_i = i
        entry_z = float(z[entry_i])
        entry_abs_z = float(abs_z[entry_i])
        side = infer_side(entry_z)

        denom_qty, numer_qty = compute_quantities(float(denom[entry_i]), float(numer[entry_i]))

        # The large MAX_LOOKAHEAD_BARS remains a safety cap, but HARD_EXIT_BARS
        # is the practical trading-style time stop. If HARD_EXIT_BARS=60, the
        # scan will not look beyond 60 bars for this trade. Set HARD_EXIT_BARS=0
        # to restore the older diagnostic behaviour.
        max_j = min(n - 1, entry_i + MAX_LOOKAHEAD_BARS)
        if HARD_EXIT_BARS > 0:
            max_j = min(max_j, entry_i + HARD_EXIT_BARS)

        settle_j: Optional[int] = None
        exit_j: Optional[int] = None
        exit_reason = "FORCED_MAX_WAIT_EXIT"

        entry_denom = float(denom[entry_i])
        entry_numer = float(numer[entry_i])

        for j in range(entry_i + 1, max_j + 1):
            bars_held_now = j - entry_i
            pnl_j = current_pair_pnl(
                side=side,
                entry_denom=entry_denom,
                entry_numer=entry_numer,
                current_denom=float(denom[j]),
                current_numer=float(numer[j]),
                denom_qty=denom_qty,
                numer_qty=numer_qty,
            )

            # 1) Rupee stop-loss first. This protects capital and avoids marking
            # a trade as statistically settled when it has already breached the
            # configured maximum loss.
            if STOP_LOSS_RUPEES > 0 and pnl_j <= -abs(STOP_LOSS_RUPEES):
                exit_j = j
                exit_reason = "STOP_LOSS_RUPEES"
                break

            # 2) Normal z-score settlement.
            if abs_z[j] <= SETTLE_Z:
                settle_j = j
                exit_j = j
                exit_reason = "SETTLED"
                break

            # 3) Hard bar/time stop. This is especially important because your
            # results showed that trades taking too long generally lose quality.
            if HARD_EXIT_BARS > 0 and bars_held_now >= HARD_EXIT_BARS:
                exit_j = j
                exit_reason = "HARD_EXIT_BARS"
                break

        if exit_j is None:
            exit_j = max_j
            if HARD_EXIT_BARS > 0 and exit_j >= entry_i + HARD_EXIT_BARS:
                exit_reason = "HARD_EXIT_BARS"
            else:
                exit_reason = "FORCED_MAX_WAIT_EXIT"

        path_slice = slice(entry_i, exit_j + 1)
        path_denom = denom[path_slice]
        path_numer = numer[path_slice]
        path_z = z[path_slice]
        path_abs_z = abs_z[path_slice]
        path_dates = dates[path_slice]

        pnl_path = compute_pair_pnl_path(
            side=side,
            entry_denom=entry_denom,
            entry_numer=entry_numer,
            path_denom=path_denom,
            path_numer=path_numer,
            denom_qty=denom_qty,
            numer_qty=numer_qty,
        )

        gross_exit_pnl = float(pnl_path[-1])
        net_exit_pnl = gross_exit_pnl - COST_PER_TRADE_RUPEES

        min_pnl_idx = int(np.nanargmin(pnl_path))
        max_pnl_idx = int(np.nanargmax(pnl_path))
        max_loss_rupees = float(pnl_path[min_pnl_idx])
        max_profit_rupees = float(pnl_path[max_pnl_idx])
        max_loss_abs_rupees = abs(min(0.0, max_loss_rupees))

        max_abs_z_idx = int(np.nanargmax(path_abs_z))
        max_abs_z_value = float(path_abs_z[max_abs_z_idx])
        max_abs_z_time = pd.Timestamp(path_dates[max_abs_z_idx])

        if entry_z > 0:
            adverse_z_value = float(np.nanmax(path_z))
            adverse_z_idx = int(np.nanargmax(path_z))
        else:
            adverse_z_value = float(np.nanmin(path_z))
            adverse_z_idx = int(np.nanargmin(path_z))
        adverse_z_time = pd.Timestamp(path_dates[adverse_z_idx])

        bars_held = int(exit_j - entry_i)
        calendar_minutes_held = float(
            (pd.Timestamp(dates[exit_j]) - pd.Timestamp(dates[entry_i])).total_seconds() / 60.0
        )
        approx_trading_days_held = bars_held / float(INTRADAY_BARS_PER_DAY)

        rows.append(
            {
                "event_id": event_id,
                "threshold_abs_z": threshold,
                "entry_time": pd.Timestamp(dates[entry_i]),
                "entry_date": pd.Timestamp(dates[entry_i]).date(),
                "entry_trading_date": trading_dates[entry_i],
                "side": side,
                "denominator_symbol": DENOMINATOR_LABEL,
                "numerator_symbol": NUMERATOR_LABEL,
                "entry_z": entry_z,
                "entry_abs_z": entry_abs_z,
                "entry_denom_close": float(denom[entry_i]),
                "entry_numer_close": float(numer[entry_i]),
                "denom_qty": denom_qty,
                "numer_qty": numer_qty,
                "denom_entry_notional": float(denom[entry_i] * denom_qty),
                "numer_entry_notional": float(numer[entry_i] * numer_qty),
                "qty_mode": QTY_MODE,
                "base_notional_rupees": BASE_NOTIONAL_RUPEES if QTY_MODE == "NOTIONAL_BALANCED" else np.nan,
                "settled": bool(settle_j is not None),
                "settle_time": pd.Timestamp(dates[settle_j]) if settle_j is not None else pd.NaT,
                "exit_time": pd.Timestamp(dates[exit_j]),
                "exit_reason": exit_reason,
                "exit_z": float(z[exit_j]),
                "exit_abs_z": float(abs_z[exit_j]),
                "exit_denom_close": float(denom[exit_j]),
                "exit_numer_close": float(numer[exit_j]),
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
                "hard_exit_bars_config": HARD_EXIT_BARS,
                "stop_loss_rupees_config": STOP_LOSS_RUPEES,
                "denom_points_at_exit": float(denom[exit_j] - denom[entry_i]),
                "numer_points_at_exit": float(numer[exit_j] - numer[entry_i]),
            }
        )

        if SKIP_OVERLAPPING_EVENTS:
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


def profit_factor(pnl: pd.Series) -> float:
    """Gross profit divided by gross loss, using net PnL series."""
    wins = float(pnl[pnl > 0].sum())
    losses = float(-pnl[pnl < 0].sum())
    if losses == 0:
        return np.inf if wins > 0 else np.nan
    return wins / losses


def summarize_events(events: pd.DataFrame, threshold: float, trading_day_count: int) -> pd.DataFrame:
    """Create a one-row summary for a threshold's event table."""
    if events.empty:
        return pd.DataFrame([{
            "threshold_abs_z": threshold,
            "total_events": 0,
            "trading_days_in_dataset": trading_day_count,
            "events_per_trading_day": 0.0,
        }])

    settled = events[events["settled"] == True].copy()  # noqa: E712
    forced = events[events["settled"] == False].copy()  # noqa: E712
    pnl = events["net_exit_pnl_rupees"].astype(float)
    exit_reason_counts = events["exit_reason"].value_counts().to_dict()

    summary = {
        "threshold_abs_z": threshold,
        "total_events": int(len(events)),
        "trading_days_in_dataset": int(trading_day_count),
        "events_per_trading_day": float(len(events) / trading_day_count) if trading_day_count else np.nan,
        "settled_count": int(len(settled)),
        "not_settled_count": int(len(forced)),
        "exit_settled_count": int(exit_reason_counts.get("SETTLED", 0)),
        "exit_stop_loss_count": int(exit_reason_counts.get("STOP_LOSS_RUPEES", 0)),
        "exit_hard_bar_count": int(exit_reason_counts.get("HARD_EXIT_BARS", 0)),
        "exit_forced_max_wait_count": int(exit_reason_counts.get("FORCED_MAX_WAIT_EXIT", 0)),
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
        "denominator_symbol": DENOMINATOR_LABEL,
        "numerator_symbol": NUMERATOR_LABEL,
        "qty_mode": QTY_MODE,
        "base_notional_rupees": BASE_NOTIONAL_RUPEES if QTY_MODE == "NOTIONAL_BALANCED" else np.nan,
        "denominator_qty_fixed": DENOMINATOR_QTY_FIXED if QTY_MODE == "FIXED" else np.nan,
        "numerator_qty_fixed": NUMERATOR_QTY_FIXED if QTY_MODE == "FIXED" else np.nan,
        "settle_z": SETTLE_Z,
        "max_lookahead_bars": MAX_LOOKAHEAD_BARS,
        "max_wait_trading_days": MAX_WAIT_TRADING_DAYS,
        "hard_exit_bars": HARD_EXIT_BARS,
        "stop_loss_rupees": STOP_LOSS_RUPEES,
        "z_window": Z_WINDOW,
        "beta_mode": BETA_MODE,
        "cost_per_trade_rupees": COST_PER_TRADE_RUPEES,
    }
    return pd.DataFrame([summary])


def build_daily_counts(events: pd.DataFrame) -> pd.DataFrame:
    """Count entry opportunities per trading day for one threshold."""
    if events.empty:
        return pd.DataFrame(columns=["entry_date", "events"])
    out = events.groupby("entry_date", as_index=False).size().rename(columns={"size": "events"})
    return out.sort_values("entry_date").reset_index(drop=True)


def build_by_side_summary(events: pd.DataFrame) -> pd.DataFrame:
    """Summarize events separately for numerator-rich and numerator-cheap."""
    if events.empty:
        return pd.DataFrame()

    rows = []
    for side, g in events.groupby("side"):
        settled_count = int(g["settled"].sum())
        pnl = g["net_exit_pnl_rupees"].astype(float)
        rows.append({
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
            "profit_factor_net": profit_factor(pnl),
        })
    return pd.DataFrame(rows)


def build_exit_reason_summary(events: pd.DataFrame) -> pd.DataFrame:
    """Summarize trades by exit reason: settled, stop-loss, hard bar exit, etc."""
    if events.empty:
        return pd.DataFrame()

    rows = []
    for reason, g in events.groupby("exit_reason"):
        pnl = g["net_exit_pnl_rupees"].astype(float)
        rows.append({
            "exit_reason": reason,
            "events": int(len(g)),
            "settled_count": int(g["settled"].sum()),
            "net_total_pnl_rupees": float(pnl.sum()),
            "avg_net_pnl_per_event": float(pnl.mean()),
            "median_net_pnl_per_event": float(pnl.median()),
            "win_rate_net_pct": safe_percent(int((pnl > 0).sum()), len(g)),
            "profit_factor_net": profit_factor(pnl),
            "worst_exit_pnl": float(pnl.min()),
            "worst_max_loss_abs_rupees": float(g["max_loss_abs_rupees"].max()),
            "median_bars_to_exit": float(g["bars_to_exit"].median()),
        })
    return pd.DataFrame(rows).sort_values("events", ascending=False).reset_index(drop=True)


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


def write_threshold_excel(
    threshold_dir: str,
    threshold: float,
    events: pd.DataFrame,
    summary: pd.DataFrame,
    config_df: pd.DataFrame,
) -> str:
    """Write one simple separate Excel workbook for a threshold."""
    os.makedirs(threshold_dir, exist_ok=True)

    label = str(threshold).replace(".", "_").rstrip("0").rstrip("_")
    base_name = f"{NUMERATOR_LABEL.lower()}_{DENOMINATOR_LABEL.lower()}_z_ge_{label}"
    xlsx_path = os.path.join(threshold_dir, f"{base_name}.xlsx")
    csv_path = os.path.join(threshold_dir, f"{base_name}_events.csv")

    daily_counts = build_daily_counts(events)
    by_side = build_by_side_summary(events)
    exit_reason_summary = build_exit_reason_summary(events)

    preferred_cols = [
        "event_id", "threshold_abs_z", "entry_time", "side",
        "denominator_symbol", "numerator_symbol",
        "entry_z", "entry_abs_z",
        "entry_denom_close", "entry_numer_close",
        "denom_qty", "numer_qty", "denom_entry_notional", "numer_entry_notional",
        "settled", "settle_time", "exit_time", "exit_reason",
        "hard_exit_bars_config", "stop_loss_rupees_config",
        "exit_z", "exit_abs_z",
        "exit_denom_close", "exit_numer_close",
        "bars_to_settle", "bars_to_exit", "approx_trading_days_to_exit",
        "max_loss_abs_rupees", "max_loss_rupees", "max_loss_time",
        "max_profit_rupees", "max_profit_time",
        "max_abs_z_during_wait", "max_abs_z_time",
        "directional_worst_z_during_wait", "directional_worst_z_time",
        "gross_exit_pnl_rupees", "cost_rupees", "net_exit_pnl_rupees",
        "denom_points_at_exit", "numer_points_at_exit",
        "qty_mode", "base_notional_rupees",
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

        exit_reason_summary.to_excel(writer, sheet_name="exit_reason_summary", index=False)
        autosize_excel_columns(writer, "exit_reason_summary", exit_reason_summary)

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
        ("exchange", EXCHANGE),
        ("denominator_symbol", DENOMINATOR_SYMBOL),
        ("numerator_symbol", NUMERATOR_SYMBOL),
        ("spread_definition", f"log({NUMERATOR_LABEL}) - beta * log({DENOMINATOR_LABEL})"),
        ("aligned_rows", aligned_rows),
        ("trading_days", trading_days),
        ("z_window", Z_WINDOW),
        ("min_periods", MIN_PERIODS),
        ("settle_z", SETTLE_Z),
        ("thresholds", ",".join(str(x) for x in THRESHOLDS)),
        ("max_wait_trading_days", MAX_WAIT_TRADING_DAYS),
        ("max_lookahead_bars", MAX_LOOKAHEAD_BARS),
        ("hard_exit_bars", HARD_EXIT_BARS),
        ("stop_loss_rupees", STOP_LOSS_RUPEES),
        ("qty_mode", QTY_MODE),
        ("base_notional_rupees", BASE_NOTIONAL_RUPEES),
        ("denominator_qty_fixed", DENOMINATOR_QTY_FIXED),
        ("numerator_qty_fixed", NUMERATOR_QTY_FIXED),
        ("qty_round_step", QTY_ROUND_STEP),
        ("cost_per_trade_rupees", COST_PER_TRADE_RUPEES),
        ("beta_mode", BETA_MODE),
        ("beta_train_days", BETA_TRAIN_DAYS),
        ("estimated_beta", beta),
        ("skip_overlapping_events", SKIP_OVERLAPPING_EVENTS),
        ("enable_entry_time_filter", ENABLE_ENTRY_TIME_FILTER),
        ("entry_start_time", ENTRY_START_TIME.isoformat(timespec="minutes")),
        ("last_entry_time", LAST_ENTRY_TIME.isoformat(timespec="minutes")),
        ("note", "Equity close levels are used as proxy execution prices; add actual costs/slippage before trading."),
    ]
    return pd.DataFrame(rows, columns=["parameter", "value"])


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    """Run the complete HDFCBANK-ICICIBANK deviation settlement test."""
    pair_name = f"{NUMERATOR_LABEL}-{DENOMINATOR_LABEL}"
    print("============================================================")
    print(f"{pair_name} 1-min z-score deviation settlement tester")
    print("============================================================")

    paths = ensure_dirs()
    start_d, end_d = get_date_range()

    print(f"[CONFIG] Date range       : {start_d} to {end_d}")
    print(f"[CONFIG] Output dir       : {OUTPUT_DIR}")
    print(f"[CONFIG] Pair             : numerator={NUMERATOR_LABEL}, denominator={DENOMINATOR_LABEL}")
    print(f"[CONFIG] Spread           : log({NUMERATOR_LABEL}) - beta * log({DENOMINATOR_LABEL})")
    print(f"[CONFIG] Thresholds       : {THRESHOLDS}")
    print(f"[CONFIG] Z_WINDOW         : {Z_WINDOW}")
    print(f"[CONFIG] SETTLE_Z         : {SETTLE_Z}")
    print(f"[CONFIG] Max wait bars    : {MAX_LOOKAHEAD_BARS} (~{MAX_WAIT_TRADING_DAYS} trading days)")
    print(f"[CONFIG] Hard exit bars   : {HARD_EXIT_BARS} (0 disables)")
    print(f"[CONFIG] Stop loss        : Rs {STOP_LOSS_RUPEES:,.2f} (0 disables)")
    print(f"[CONFIG] Qty mode         : {QTY_MODE}")
    print(f"[CONFIG] FORCE_DOWNLOAD   : {FORCE_DOWNLOAD}")

    print("\n[STEP] Initializing Kite API ...")
    kite = oUtils.intialize_kite_api()
    print("[INFO] Kite API initialized.")

    instruments_cache: Dict[str, List[Dict]] = {}

    print(f"\n[STEP] Loading/downloading {DENOMINATOR_LABEL} and {NUMERATOR_LABEL} 1-min candles ...")
    denom_df = load_or_download_equity(kite, DENOMINATOR_SPEC, start_d, end_d, paths, instruments_cache)
    numer_df = load_or_download_equity(kite, NUMERATOR_SPEC, start_d, end_d, paths, instruments_cache)

    print(f"\n[STEP] Aligning {DENOMINATOR_LABEL} and {NUMERATOR_LABEL} candles ...")
    aligned = align_pair(denom_df, numer_df)
    if aligned.empty:
        raise RuntimeError(f"No common {DENOMINATOR_LABEL}-{NUMERATOR_LABEL} timestamps after alignment.")

    trading_days = int(aligned["trading_date"].nunique())
    print(f"[INFO] Aligned rows: {len(aligned):,}; trading days: {trading_days}")

    print("\n[STEP] Computing spread and z-score ...")
    aligned, beta = add_spread_and_zscore(aligned)
    valid_z_rows = int(aligned["z"].notna().sum())
    print(f"[INFO] Estimated beta: {beta:.6f}")
    print(f"[INFO] Valid z-score rows after rolling warmup: {valid_z_rows:,}")

    aligned_path = os.path.join(OUTPUT_DIR, f"{NUMERATOR_LABEL.lower()}_{DENOMINATOR_LABEL.lower()}_aligned_1min.pkl")
    aligned.to_pickle(aligned_path)
    if os.environ.get("SAVE_ALIGNED_CSV", "0").strip().lower() in {"1", "true", "yes", "y"}:
        aligned.to_csv(aligned_path.replace(".pkl", ".csv"), index=False)
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

    combined_path = os.path.join(OUTPUT_DIR, f"{NUMERATOR_LABEL.lower()}_{DENOMINATOR_LABEL.lower()}_combined_threshold_summary.xlsx")
    with pd.ExcelWriter(combined_path, engine="openpyxl") as writer:
        combined_summary.to_excel(writer, sheet_name="combined_summary", index=False)
        autosize_excel_columns(writer, "combined_summary", combined_summary)

        files_df.to_excel(writer, sheet_name="files", index=False)
        autosize_excel_columns(writer, "files", files_df)

        config_df.to_excel(writer, sheet_name="config", index=False)
        autosize_excel_columns(writer, "config", config_df)

    print("\n==================== FINAL SUMMARY ====================")
    if not combined_summary.empty:
        cols = [
            "threshold_abs_z",
            "total_events",
            "events_per_trading_day",
            "settlement_rate_pct",
            "exit_stop_loss_count",
            "exit_hard_bar_count",
            "median_bars_to_settle",
            "p90_bars_to_settle",
            "max_loss_abs_rupees_worst_case",
            "net_total_pnl_rupees",
            "win_rate_net_pct",
            "profit_factor_net",
        ]
        existing_cols = [c for c in cols if c in combined_summary.columns]
        print(combined_summary[existing_cols].to_string(index=False))
    print("-------------------------------------------------------")
    print(f"Combined summary: {combined_path}")
    print(f"Output directory : {OUTPUT_DIR}")
    print("=======================================================")


if __name__ == "__main__":
    main()
