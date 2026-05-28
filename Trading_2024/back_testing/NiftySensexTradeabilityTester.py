"""
NiftySensexTradeabilityTester.py
=================================

Purpose
-------
Test whether the NIFTY-SENSEX relationship is actually tradeable as a
statistical-arbitrage / pairs-trading setup.

This is the NEXT stage after correlation screening.

The earlier correlation scanner answers:
    "Do NIFTY and SENSEX move together?"

This script answers the more important question:
    "Is the NIFTY-SENSEX spread stable/mean-reverting enough to trade?"

What this script checks
-----------------------
1) Loads NIFTY and SENSEX close prices from your earlier scanner output OR
   downloads fresh data using Kite if local data is not found.

2) Aligns both instruments on common timestamps.

3) Uses log prices and log returns.

4) Estimates hedge ratio using OLS:
       log(SENSEX) = alpha + beta * log(NIFTY)

5) Builds spread:
       spread = log(SENSEX) - beta * log(NIFTY)

6) Tests statistical tradeability:
       - return correlation
       - rolling return correlation
       - Engle-Granger cointegration test, if statsmodels is installed
       - ADF stationarity test on spread, if statsmodels is installed
       - half-life of mean reversion
       - z-score stability and threshold crossings

7) Runs a simple, disciplined z-score backtest:
       - If z > +ENTRY_Z: SENSEX is rich vs NIFTY
             short spread = short SENSEX, long beta-adjusted NIFTY
       - If z < -ENTRY_Z: SENSEX is cheap vs NIFTY
             long spread = long SENSEX, short beta-adjusted NIFTY
       - Exit when |z| <= EXIT_Z
       - Stop if |z| >= STOP_Z
       - Time stop after MAX_HOLD_BARS

8) Adds transaction cost and slippage assumptions in basis points.

Important warning
-----------------
This script uses index spot/close prices as research proxies.
For live trading, the implementation instrument will likely be futures/options,
not the index itself. Therefore, before live use, replace spot index closes with
actual tradable futures prices and add true brokerage, STT/CTT, exchange charges,
GST, bid-ask spread, slippage, lot-size rounding, and margin effects.

Dependencies
------------
Required:
    pip install pandas numpy openpyxl

Optional but strongly recommended:
    pip install statsmodels

If statsmodels is unavailable, the script will still run but will skip
cointegration and ADF tests.

Typical usage
-------------
If you already ran IndexStockCorrelationScanner.py and have wide close output:
    python NiftySensexTradeabilityTester.py

Force a specific close matrix:
    set CLOSE_MATRIX_PATH=C:\\path\\to\\wide_close_minute.pkl
    python NiftySensexTradeabilityTester.py

Force download through Kite:
    set FORCE_DOWNLOAD=1
    set INTERVAL=minute
    set LOOKBACK_DAYS=365
    python NiftySensexTradeabilityTester.py

Tune strategy parameters:
    set ENTRY_Z=2.0
    set EXIT_Z=0.5
    set STOP_Z=3.0
    set ROLLING_Z_WINDOW=375
    set TRADE_NOTIONAL=100000
    set COST_BPS_PER_LEG=1.5
    set SLIPPAGE_BPS_PER_LEG=1.0
    python NiftySensexTradeabilityTester.py

Default outputs
---------------
./nifty_sensex_tradeability_output/
    nifty_sensex_tradeability_report.xlsx
    nifty_sensex_backtest_trades.csv
    nifty_sensex_equity_curve.csv
    nifty_sensex_aligned_data.pkl

Author note
-----------
The code is intentionally verbose and heavily commented so that you can modify
it later for BANKNIFTY-FINNIFTY, HDFCBANK-ICICIBANK, INFY-TCS, etc.
"""

import os
import time
from dataclasses import dataclass, asdict
from datetime import datetime, date, time as dtime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# Kite initialization is kept consistent with your existing downloader scripts.
# Your project already uses Trading_2024.OptionTradeUtils.intialize_kite_api().
try:
    import Trading_2024.OptionTradeUtils as oUtils
except Exception:  # pragma: no cover - allows non-Kite analysis from local files
    oUtils = None  # type: ignore

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

# statsmodels is optional but strongly recommended.
# It is required for Engle-Granger cointegration and ADF stationarity tests.
try:
    import statsmodels.api as sm
    from statsmodels.tsa.stattools import adfuller, coint
    STATSMODELS_AVAILABLE = True
except Exception:  # pragma: no cover
    sm = None  # type: ignore
    adfuller = None  # type: ignore
    coint = None  # type: ignore
    STATSMODELS_AVAILABLE = False


# ===================== CONFIGURATION =====================

# Indian cash-market session. Used only when downloading intraday data through Kite.
SESSION_START = dtime(9, 15, 0)
SESSION_END = dtime(15, 30, 0)

# Conservative chunk size for historical downloads.
# Kite historical API can fail on very large intervals, especially for minute data.
MAX_DAYS_PER_CHUNK = int(os.environ.get("MAX_DAYS_PER_CHUNK", "25"))

# Retry settings for Kite download.
MAX_ATTEMPTS = int(os.environ.get("MAX_ATTEMPTS", "5"))
SLEEP_BETWEEN_CALLS_SEC = float(os.environ.get("SLEEP_BETWEEN_CALLS_SEC", "0.20"))

# Main strategy/statistics parameters.
INTERVAL = os.environ.get("INTERVAL", "minute").strip()       # minute, 3minute, 5minute, 10minute, 15minute, day
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "365"))
ROLLING_CORR_WINDOW = int(os.environ.get("ROLLING_CORR_WINDOW", "375"))
ROLLING_Z_WINDOW = int(os.environ.get("ROLLING_Z_WINDOW", "375"))

# Split for out-of-sample testing.
# Hedge ratio is estimated on the train portion; backtest is run on the test portion.
TRAIN_FRACTION = float(os.environ.get("TRAIN_FRACTION", "0.60"))

# Z-score strategy parameters.
ENTRY_Z = float(os.environ.get("ENTRY_Z", "2.0"))
EXIT_Z = float(os.environ.get("EXIT_Z", "0.5"))
STOP_Z = float(os.environ.get("STOP_Z", "3.0"))
MAX_HOLD_BARS = int(os.environ.get("MAX_HOLD_BARS", "1500"))

# Cost model.
# This is intentionally configurable. For real futures trading, replace with exact broker/exchange charges.
TRADE_NOTIONAL = float(os.environ.get("TRADE_NOTIONAL", "100000"))
COST_BPS_PER_LEG = float(os.environ.get("COST_BPS_PER_LEG", "1.5"))
SLIPPAGE_BPS_PER_LEG = float(os.environ.get("SLIPPAGE_BPS_PER_LEG", "1.0"))

# File paths.
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "./nifty_sensex_tradeability_output").strip()
CLOSE_MATRIX_PATH = os.environ.get("CLOSE_MATRIX_PATH", "").strip()
FORCE_DOWNLOAD = os.environ.get("FORCE_DOWNLOAD", "0").strip().lower() in {"1", "true", "yes", "y"}

# Candidate local paths from the earlier correlation scanner.
# The script will try these automatically if CLOSE_MATRIX_PATH is not supplied.
LOCAL_CLOSE_CANDIDATES = [
    f"./pair_correlation_output/wide_close_{INTERVAL}.pkl",
    "./pair_correlation_output/wide_close_minute.pkl",
    "./pair_correlation_output/wide_close_5minute.pkl",
    "./pair_correlation_output/wide_close_day.pkl",
]


@dataclass
class PairDiagnostics:
    """Container for the main diagnostic results."""
    rows_total: int
    rows_train: int
    rows_test: int
    start_datetime: str
    end_datetime: str
    train_end_datetime: str
    interval: str
    train_fraction: float
    full_return_corr: float
    train_return_corr: float
    test_return_corr: float
    rolling_corr_mean: float
    rolling_corr_min: float
    rolling_corr_max: float
    rolling_corr_std: float
    rolling_corr_p05: float
    rolling_corr_p50: float
    rolling_corr_p95: float
    hedge_ratio_beta_train: float
    hedge_alpha_train: float
    hedge_ratio_beta_full: float
    hedge_alpha_full: float
    spread_mean_train: float
    spread_std_train: float
    spread_half_life_bars_train: Optional[float]
    spread_half_life_bars_full: Optional[float]
    coint_pvalue_train: Optional[float]
    coint_pvalue_full: Optional[float]
    adf_pvalue_train_spread: Optional[float]
    adf_pvalue_full_spread: Optional[float]
    z_cross_above_entry_count: int
    z_cross_below_entry_count: int
    z_abs_gt_entry_pct: float
    z_abs_gt_stop_pct: float


# ===================== SMALL UTILITY HELPERS =====================

def _ist_today() -> date:
    """Return today's date in Asia/Kolkata when zoneinfo is available."""
    if ZoneInfo is not None:
        try:
            return datetime.now(ZoneInfo("Asia/Kolkata")).date()
        except Exception:
            pass
    return datetime.now().date()


def ensure_output_dir() -> None:
    """Create the output directory if it does not already exist."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def safe_float(x) -> Optional[float]:
    """Convert values to clean Python floats for Excel/report output."""
    if x is None:
        return None
    try:
        if pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None


def _iter_chunks_by_date(from_dt: datetime, to_dt: datetime, days_per_chunk: int) -> List[Tuple[datetime, datetime]]:
    """
    Split a datetime range into date-based chunks.

    This follows the same idea used in your historical downloader:
    - avoid asking Kite for a very large minute-data range in one call
    - keep intraday session boundaries intact
    """
    if from_dt > to_dt:
        raise ValueError("from_dt must be <= to_dt")

    chunks: List[Tuple[datetime, datetime]] = []
    start_d = from_dt.date()
    end_d = to_dt.date()
    cur = start_d

    while cur <= end_d:
        chunk_end_d = min(cur + timedelta(days=days_per_chunk - 1), end_d)
        c_from = from_dt if cur == start_d else datetime.combine(cur, SESSION_START)
        c_to = to_dt if chunk_end_d == end_d else datetime.combine(chunk_end_d, SESSION_END)
        chunks.append((c_from, c_to))
        cur = chunk_end_d + timedelta(days=1)

    return chunks


# ===================== KITE DOWNLOAD HELPERS =====================

def _kite_instruments_cached(kite, exchange: str, cache: Dict[str, List[Dict]]) -> List[Dict]:
    """Load and cache Kite instrument dump for an exchange."""
    ex = exchange.upper().strip()
    if ex not in cache:
        print(f"[STEP] Loading instruments dump for {ex} ...")
        cache[ex] = kite.instruments(ex)
        print(f"[INFO] Total instruments on {ex}: {len(cache[ex])}")
    return cache[ex]


def get_instrument_token(kite, exchange: str, tradingsymbol: str, cache: Dict[str, List[Dict]]) -> Tuple[int, str]:
    """Resolve Kite instrument_token for a given exchange + tradingsymbol."""
    ex = exchange.upper().strip()
    wanted = tradingsymbol.strip().upper()

    instruments = _kite_instruments_cached(kite, ex, cache)
    for r in instruments:
        if str(r.get("tradingsymbol", "")).upper() == wanted:
            return int(r["instrument_token"]), str(r.get("exchange", ex))

    raise ValueError(f"Instrument not found on {ex}: {tradingsymbol}")


def fetch_history(kite, instrument_token: int, from_dt: datetime, to_dt: datetime, interval: str, label: str) -> List[Dict]:
    """
    Fetch historical candles from Kite with chunking and retries.

    For day interval, Kite still accepts datetime/date inputs; we pass datetime
    consistently to keep the code simple.
    """
    chunks = _iter_chunks_by_date(from_dt, to_dt, days_per_chunk=MAX_DAYS_PER_CHUNK)
    print(f"[INFO] Fetching {interval} data for {label} token={instrument_token} in {len(chunks)} chunk(s).")

    all_rows: List[Dict] = []
    for idx, (c_from, c_to) in enumerate(chunks, start=1):
        print(f"  [CHUNK {idx}/{len(chunks)}] {c_from} -> {c_to}")
        last_err = None

        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                rows = kite.historical_data(
                    instrument_token=instrument_token,
                    from_date=c_from,
                    to_date=c_to,
                    interval=interval,
                    continuous=False,
                    oi=False,
                )
                print(f"    [OK] Retrieved {len(rows)} candles on attempt {attempt}.")
                all_rows.extend(rows)
                last_err = None
                break
            except Exception as e:
                last_err = e
                wait = min(8.0, 1.5 * attempt)
                print(f"    [WARN] {label} attempt {attempt}/{MAX_ATTEMPTS} failed: {e}. Sleeping {wait:.1f}s")
                time.sleep(wait)

        if last_err is not None:
            print(f"    [ERROR] Giving up on chunk {idx}/{len(chunks)} for {label}: {last_err}")

        time.sleep(SLEEP_BETWEEN_CALLS_SEC)

    return all_rows


def rows_to_dataframe(rows: List[Dict]) -> pd.DataFrame:
    """Convert Kite rows into a clean OHLCV DataFrame."""
    if not rows:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    df = pd.DataFrame(rows)
    for col in ["date", "open", "high", "low", "close", "volume"]:
        if col not in df.columns:
            df[col] = np.nan

    df["date"] = pd.to_datetime(df["date"])
    df = df.drop_duplicates(subset=["date"], keep="last").sort_values("date").reset_index(drop=True)
    return df[["date", "open", "high", "low", "close", "volume"]]


def download_nifty_sensex_from_kite() -> pd.DataFrame:
    """
    Download NIFTY 50 and SENSEX closes from Kite and return a wide close matrix.

    NIFTY 50 is on NSE with tradingsymbol 'NIFTY 50'.
    SENSEX is on BSE with tradingsymbol 'SENSEX'.
    """
    if oUtils is None:
        raise RuntimeError(
            "Trading_2024.OptionTradeUtils could not be imported. "
            "Either run this from your TradingScripts environment or provide CLOSE_MATRIX_PATH."
        )

    print("[STEP] Initializing Kite API ...")
    kite = oUtils.intialize_kite_api()
    print("[INFO] Kite API initialized.")

    instruments_cache: Dict[str, List[Dict]] = {}

    end_date = _ist_today()
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)

    from_dt = datetime.combine(start_date, SESSION_START)
    to_dt = datetime.combine(end_date, SESSION_END)

    print(f"[CONFIG] Download date range: {from_dt} -> {to_dt}")

    nifty_token, nifty_ex = get_instrument_token(kite, "NSE", "NIFTY 50", instruments_cache)
    sensex_token, sensex_ex = get_instrument_token(kite, "BSE", "SENSEX", instruments_cache)

    nifty_rows = fetch_history(kite, nifty_token, from_dt, to_dt, INTERVAL, f"{nifty_ex}:NIFTY 50")
    sensex_rows = fetch_history(kite, sensex_token, from_dt, to_dt, INTERVAL, f"{sensex_ex}:SENSEX")

    nifty_df = rows_to_dataframe(nifty_rows)[["date", "close"]].rename(columns={"close": "NIFTY"})
    sensex_df = rows_to_dataframe(sensex_rows)[["date", "close"]].rename(columns={"close": "SENSEX"})

    wide = pd.merge(nifty_df, sensex_df, on="date", how="inner")
    wide = wide.set_index("date").sort_index()

    if wide.empty:
        raise RuntimeError("Downloaded NIFTY/SENSEX data is empty after timestamp alignment.")

    return wide


# ===================== DATA LOADING HELPERS =====================

def find_existing_close_matrix() -> Optional[str]:
    """Find an existing wide_close_*.pkl file from the previous scanner."""
    if CLOSE_MATRIX_PATH:
        return CLOSE_MATRIX_PATH if os.path.exists(CLOSE_MATRIX_PATH) else None

    for path in LOCAL_CLOSE_CANDIDATES:
        if os.path.exists(path):
            return path

    return None


def detect_column(columns: List[str], keyword: str, reject_keywords: Optional[List[str]] = None) -> Optional[str]:
    """
    Detect a column containing a keyword while excluding unwanted keywords.

    This makes the script robust to columns such as:
        'NIFTY', 'NIFTY 50', 'NSE:NIFTY 50'
    while avoiding:
        'BANKNIFTY', 'NIFTY BANK'
    """
    keyword_u = keyword.upper()
    reject_keywords = reject_keywords or []
    reject_u = [r.upper() for r in reject_keywords]

    for c in columns:
        cu = str(c).upper()
        if keyword_u in cu and not any(r in cu for r in reject_u):
            return c
    return None


def load_or_download_close_matrix() -> pd.DataFrame:
    """
    Load NIFTY/SENSEX close matrix from local file, unless FORCE_DOWNLOAD is enabled.
    If no local file is found, download from Kite.
    """
    ensure_output_dir()

    if not FORCE_DOWNLOAD:
        path = find_existing_close_matrix()
        if path:
            print(f"[STEP] Loading close matrix from: {path}")
            wide = pd.read_pickle(path)
            if not isinstance(wide.index, pd.DatetimeIndex):
                # Try to recover if date is a normal column.
                if "date" in wide.columns:
                    wide["date"] = pd.to_datetime(wide["date"])
                    wide = wide.set_index("date")
                elif "datetime" in wide.columns:
                    wide["datetime"] = pd.to_datetime(wide["datetime"])
                    wide = wide.set_index("datetime")
                else:
                    wide.index = pd.to_datetime(wide.index)

            cols = list(wide.columns)
            nifty_col = detect_column(cols, "NIFTY", reject_keywords=["BANK", "FIN"])
            sensex_col = detect_column(cols, "SENSEX")

            if nifty_col and sensex_col:
                out = wide[[nifty_col, sensex_col]].copy()
                out.columns = ["NIFTY", "SENSEX"]
                out = out.sort_index()
                print(f"[INFO] Loaded local NIFTY/SENSEX rows: {len(out)}")
                return out

            print("[WARN] Local close matrix found but NIFTY/SENSEX columns could not be detected.")
            print("[WARN] Falling back to Kite download.")

    print("[STEP] Downloading fresh NIFTY/SENSEX data from Kite ...")
    wide = download_nifty_sensex_from_kite()

    # Save the freshly downloaded matrix for future reuse.
    save_path = os.path.join(OUTPUT_DIR, f"downloaded_nifty_sensex_close_{INTERVAL}.pkl")
    wide.to_pickle(save_path)
    print(f"[DONE] Saved downloaded close matrix: {save_path}")
    return wide


def prepare_pair_data(wide_close: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and prepare aligned NIFTY/SENSEX close, log price, and log return data.
    """
    df = wide_close[["NIFTY", "SENSEX"]].copy()
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()

    # Remove bad/missing prices.
    df = df.replace([np.inf, -np.inf], np.nan).dropna()
    df = df[(df["NIFTY"] > 0) & (df["SENSEX"] > 0)]

    # Log prices are used for spread construction.
    df["log_nifty"] = np.log(df["NIFTY"])
    df["log_sensex"] = np.log(df["SENSEX"])

    # Log returns are used for correlation and PnL approximation.
    df["ret_nifty"] = df["log_nifty"].diff()
    df["ret_sensex"] = df["log_sensex"].diff()

    df = df.dropna().copy()

    if len(df) < max(ROLLING_Z_WINDOW, ROLLING_CORR_WINDOW) + 100:
        raise RuntimeError(
            f"Not enough aligned observations ({len(df)}). "
            "Need substantially more rows for rolling statistics/backtest."
        )

    return df


# ===================== STATISTICAL TEST HELPERS =====================

def ols_hedge_ratio(log_y: pd.Series, log_x: pd.Series) -> Tuple[float, float]:
    """
    Estimate OLS hedge ratio:
        log_y = alpha + beta * log_x

    Here:
        y = SENSEX
        x = NIFTY

    Returns:
        alpha, beta
    """
    y = pd.Series(log_y).astype(float)
    x = pd.Series(log_x).astype(float)

    aligned = pd.concat([y, x], axis=1).dropna()
    yv = aligned.iloc[:, 0].values
    xv = aligned.iloc[:, 1].values

    if STATSMODELS_AVAILABLE:
        X = sm.add_constant(xv)
        model = sm.OLS(yv, X).fit()
        alpha = float(model.params[0])
        beta = float(model.params[1])
        return alpha, beta

    # Fallback with numpy polyfit: y = beta*x + alpha
    beta, alpha = np.polyfit(xv, yv, deg=1)
    return float(alpha), float(beta)


def build_spread(df: pd.DataFrame, alpha: float, beta: float) -> pd.Series:
    """
    Build residual spread from the hedge relationship.

    Alpha is subtracted so that the spread is centered closer to zero:
        spread = log(SENSEX) - alpha - beta*log(NIFTY)
    """
    return df["log_sensex"] - alpha - beta * df["log_nifty"]


def compute_half_life(spread: pd.Series) -> Optional[float]:
    """
    Estimate half-life of mean reversion in bars.

    Regression model:
        delta_spread_t = a + phi * spread_{t-1} + error

    If phi < 0, the spread is mean-reverting.
    Half-life:
        -ln(2) / phi

    If phi is >= 0, half-life is not meaningful.
    """
    s = pd.Series(spread).dropna().astype(float)
    if len(s) < 50:
        return None

    lag = s.shift(1)
    delta = s - lag
    tmp = pd.concat([delta.rename("delta"), lag.rename("lag")], axis=1).dropna()

    if tmp.empty:
        return None

    y = tmp["delta"].values
    x = tmp["lag"].values

    try:
        if STATSMODELS_AVAILABLE:
            X = sm.add_constant(x)
            model = sm.OLS(y, X).fit()
            phi = float(model.params[1])
        else:
            # Fallback slope from simple linear regression.
            phi = float(np.polyfit(x, y, deg=1)[0])

        if phi >= 0:
            return None

        hl = -np.log(2.0) / phi
        if not np.isfinite(hl) or hl <= 0:
            return None
        return float(hl)
    except Exception:
        return None


def run_coint_test(log_y: pd.Series, log_x: pd.Series) -> Optional[float]:
    """Run Engle-Granger cointegration test and return p-value."""
    if not STATSMODELS_AVAILABLE:
        return None
    try:
        aligned = pd.concat([log_y, log_x], axis=1).dropna()
        _, pvalue, _ = coint(aligned.iloc[:, 0], aligned.iloc[:, 1])
        return safe_float(pvalue)
    except Exception as e:
        print(f"[WARN] Cointegration test failed: {e}")
        return None


def run_adf_test(series: pd.Series) -> Optional[float]:
    """Run Augmented Dickey-Fuller test on the spread and return p-value."""
    if not STATSMODELS_AVAILABLE:
        return None
    try:
        s = pd.Series(series).dropna().astype(float)
        result = adfuller(s, autolag="AIC")
        return safe_float(result[1])
    except Exception as e:
        print(f"[WARN] ADF test failed: {e}")
        return None


def add_zscore(df: pd.DataFrame, spread_col: str = "spread") -> pd.DataFrame:
    """
    Add rolling z-score of spread without lookahead bias.

    The rolling mean/std are shifted by one bar so the current signal uses only
    information available before the current bar.
    """
    out = df.copy()
    rolling_mean = out[spread_col].rolling(ROLLING_Z_WINDOW).mean().shift(1)
    rolling_std = out[spread_col].rolling(ROLLING_Z_WINDOW).std(ddof=0).shift(1)

    out["spread_roll_mean"] = rolling_mean
    out["spread_roll_std"] = rolling_std
    out["zscore"] = (out[spread_col] - rolling_mean) / rolling_std

    # Remove rows where rolling stats are not ready.
    out = out.replace([np.inf, -np.inf], np.nan).dropna(subset=["zscore"]).copy()
    return out


def count_threshold_crossings(z: pd.Series, threshold: float, direction: str) -> int:
    """
    Count actual threshold crossings rather than counting all bars above threshold.

    direction='above': count z moving from <= threshold to > threshold.
    direction='below': count z moving from >= -threshold to < -threshold.
    """
    z = pd.Series(z).dropna()
    prev = z.shift(1)

    if direction == "above":
        return int(((prev <= threshold) & (z > threshold)).sum())
    if direction == "below":
        return int(((prev >= -threshold) & (z < -threshold)).sum())
    raise ValueError("direction must be 'above' or 'below'")


# ===================== BACKTEST HELPERS =====================

def compute_trade_cost(beta: float) -> float:
    """
    Compute approximate round-trip cost for one pair trade.

    Structure:
        SENSEX leg notional = TRADE_NOTIONAL
        NIFTY leg notional  = beta * TRADE_NOTIONAL

    Each trade has entry + exit, and each event trades both legs.

    total_turnover = 2 * (sensex_notional + nifty_notional)

    cost_bps_per_leg and slippage_bps_per_leg are applied to turnover.
    """
    gross_leg_notional = TRADE_NOTIONAL * (1.0 + abs(beta))
    round_trip_turnover = 2.0 * gross_leg_notional
    total_bps = COST_BPS_PER_LEG + SLIPPAGE_BPS_PER_LEG
    return round_trip_turnover * total_bps / 10000.0


def backtest_zscore_strategy(df: pd.DataFrame, beta: float) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, float]]:
    """
    Backtest a simple z-score strategy on the test period.

    Position convention:
        position = +1 means LONG spread:
            long SENSEX notional, short beta-adjusted NIFTY notional
        position = -1 means SHORT spread:
            short SENSEX notional, long beta-adjusted NIFTY notional

    PnL approximation per bar while holding:
        pnl_bar = position * TRADE_NOTIONAL * (ret_sensex - beta*ret_nifty)

    This matches the spread definition:
        spread = log(SENSEX) - beta*log(NIFTY)

    Cost is subtracted once per completed trade as a round-trip estimate.
    """
    data = df.copy().sort_index()

    position = 0
    entry_time = None
    entry_z = None
    entry_spread = None
    entry_sensex = None
    entry_nifty = None
    entry_i = None
    cumulative_pnl = 0.0
    open_trade_pnl = 0.0

    round_trip_cost = compute_trade_cost(beta)

    trades: List[Dict] = []
    equity_rows: List[Dict] = []

    # We iterate row by row because pair-trade state matters.
    rows = list(data.iterrows())

    for i, (ts, row) in enumerate(rows):
        z = float(row["zscore"])
        spread = float(row["spread"])
        ret_sensex = float(row["ret_sensex"])
        ret_nifty = float(row["ret_nifty"])

        # Mark-to-market current open position using current bar's returns.
        # No PnL when no position is open.
        bar_pnl = 0.0
        if position != 0:
            bar_pnl = position * TRADE_NOTIONAL * (ret_sensex - beta * ret_nifty)
            open_trade_pnl += bar_pnl
            cumulative_pnl += bar_pnl

        exit_reason = None

        # Exit logic checked after updating current-bar mark-to-market.
        if position != 0:
            bars_held = i - int(entry_i)

            if abs(z) <= EXIT_Z:
                exit_reason = "MEAN_REVERSION_EXIT"
            elif abs(z) >= STOP_Z:
                exit_reason = "STOP_Z_EXIT"
            elif bars_held >= MAX_HOLD_BARS:
                exit_reason = "TIME_STOP_EXIT"

            if exit_reason is not None:
                # Subtract estimated round-trip cost on completed trade.
                net_pnl = open_trade_pnl - round_trip_cost
                cumulative_pnl -= round_trip_cost

                trades.append({
                    "entry_time": entry_time,
                    "exit_time": ts,
                    "direction": "LONG_SPREAD" if position == 1 else "SHORT_SPREAD",
                    "entry_z": entry_z,
                    "exit_z": z,
                    "entry_spread": entry_spread,
                    "exit_spread": spread,
                    "entry_sensex": entry_sensex,
                    "exit_sensex": float(row["SENSEX"]),
                    "entry_nifty": entry_nifty,
                    "exit_nifty": float(row["NIFTY"]),
                    "bars_held": bars_held,
                    "gross_pnl": open_trade_pnl,
                    "estimated_round_trip_cost": round_trip_cost,
                    "net_pnl": net_pnl,
                    "exit_reason": exit_reason,
                })

                # Reset position state.
                position = 0
                entry_time = None
                entry_z = None
                entry_spread = None
                entry_sensex = None
                entry_nifty = None
                entry_i = None
                open_trade_pnl = 0.0

        # Entry logic: only when flat.
        # We enter after exit check to avoid same-bar exit+entry churn.
        if position == 0:
            if z > ENTRY_Z:
                # Spread high: SENSEX rich relative to NIFTY.
                # Short spread = short SENSEX, long beta-adjusted NIFTY.
                position = -1
                entry_time = ts
                entry_z = z
                entry_spread = spread
                entry_sensex = float(row["SENSEX"])
                entry_nifty = float(row["NIFTY"])
                entry_i = i
                open_trade_pnl = 0.0
            elif z < -ENTRY_Z:
                # Spread low: SENSEX cheap relative to NIFTY.
                # Long spread = long SENSEX, short beta-adjusted NIFTY.
                position = 1
                entry_time = ts
                entry_z = z
                entry_spread = spread
                entry_sensex = float(row["SENSEX"])
                entry_nifty = float(row["NIFTY"])
                entry_i = i
                open_trade_pnl = 0.0

        equity_rows.append({
            "datetime": ts,
            "SENSEX": float(row["SENSEX"]),
            "NIFTY": float(row["NIFTY"]),
            "spread": spread,
            "zscore": z,
            "position": position,
            "bar_pnl": bar_pnl,
            "cumulative_pnl": cumulative_pnl,
        })

    # If a trade is still open at the end, close it at final bar.
    if position != 0 and entry_time is not None:
        ts, row = rows[-1]
        z = float(row["zscore"])
        spread = float(row["spread"])
        bars_held = len(rows) - 1 - int(entry_i)
        net_pnl = open_trade_pnl - round_trip_cost
        cumulative_pnl -= round_trip_cost

        trades.append({
            "entry_time": entry_time,
            "exit_time": ts,
            "direction": "LONG_SPREAD" if position == 1 else "SHORT_SPREAD",
            "entry_z": entry_z,
            "exit_z": z,
            "entry_spread": entry_spread,
            "exit_spread": spread,
            "entry_sensex": entry_sensex,
            "exit_sensex": float(row["SENSEX"]),
            "entry_nifty": entry_nifty,
            "exit_nifty": float(row["NIFTY"]),
            "bars_held": bars_held,
            "gross_pnl": open_trade_pnl,
            "estimated_round_trip_cost": round_trip_cost,
            "net_pnl": net_pnl,
            "exit_reason": "FORCED_END_EXIT",
        })

        # Update last equity row for forced cost deduction.
        if equity_rows:
            equity_rows[-1]["cumulative_pnl"] = cumulative_pnl

    trades_df = pd.DataFrame(trades)
    equity_df = pd.DataFrame(equity_rows)

    summary = summarize_backtest(trades_df, equity_df)
    return trades_df, equity_df, summary


def summarize_backtest(trades_df: pd.DataFrame, equity_df: pd.DataFrame) -> Dict[str, float]:
    """Create a compact numerical summary from trades and equity curve."""
    if equity_df.empty:
        return {
            "total_trades": 0,
            "net_pnl": 0.0,
            "win_rate_pct": 0.0,
            "profit_factor": 0.0,
            "max_drawdown": 0.0,
            "avg_net_pnl": 0.0,
            "median_net_pnl": 0.0,
            "avg_bars_held": 0.0,
        }

    if trades_df.empty:
        pnl = float(equity_df["cumulative_pnl"].iloc[-1]) if "cumulative_pnl" in equity_df else 0.0
        dd = compute_max_drawdown(equity_df["cumulative_pnl"])
        return {
            "total_trades": 0,
            "net_pnl": pnl,
            "win_rate_pct": 0.0,
            "profit_factor": 0.0,
            "max_drawdown": dd,
            "avg_net_pnl": 0.0,
            "median_net_pnl": 0.0,
            "avg_bars_held": 0.0,
        }

    net = trades_df["net_pnl"].astype(float)
    wins = net[net > 0]
    losses = net[net < 0]

    gross_profit = float(wins.sum()) if not wins.empty else 0.0
    gross_loss = abs(float(losses.sum())) if not losses.empty else 0.0
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else np.inf if gross_profit > 0 else 0.0

    return {
        "total_trades": int(len(trades_df)),
        "net_pnl": float(net.sum()),
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "win_rate_pct": float((net > 0).mean() * 100.0),
        "profit_factor": float(profit_factor) if np.isfinite(profit_factor) else 999.0,
        "max_drawdown": compute_max_drawdown(equity_df["cumulative_pnl"]),
        "avg_net_pnl": float(net.mean()),
        "median_net_pnl": float(net.median()),
        "avg_bars_held": float(trades_df["bars_held"].astype(float).mean()),
        "median_bars_held": float(trades_df["bars_held"].astype(float).median()),
        "mean_reversion_exit_count": int((trades_df["exit_reason"] == "MEAN_REVERSION_EXIT").sum()),
        "stop_z_exit_count": int((trades_df["exit_reason"] == "STOP_Z_EXIT").sum()),
        "time_stop_exit_count": int((trades_df["exit_reason"] == "TIME_STOP_EXIT").sum()),
        "forced_end_exit_count": int((trades_df["exit_reason"] == "FORCED_END_EXIT").sum()),
    }


def compute_max_drawdown(equity: pd.Series) -> float:
    """Compute max drawdown from an equity/PnL series."""
    s = pd.Series(equity).astype(float).fillna(method="ffill").fillna(0.0)
    running_max = s.cummax()
    drawdown = s - running_max
    return float(drawdown.min())


# ===================== MAIN ANALYSIS PIPELINE =====================

def analyze_tradeability(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict[str, float], PairDiagnostics]:
    """
    Run full diagnostics and backtest.

    Returns:
        full_df       : aligned full dataset with spread/zscore columns
        trades_df     : completed backtest trades
        equity_df     : bar-by-bar equity/PnL curve
        bt_summary    : dictionary of backtest metrics
        diagnostics   : PairDiagnostics dataclass
    """
    n = len(df)
    train_n = int(n * TRAIN_FRACTION)
    if train_n <= max(ROLLING_Z_WINDOW, ROLLING_CORR_WINDOW):
        raise RuntimeError("Training sample too small. Lower rolling windows or use more data.")
    if train_n >= n - 100:
        raise RuntimeError("Test sample too small. Lower TRAIN_FRACTION or use more data.")

    train = df.iloc[:train_n].copy()
    test = df.iloc[train_n:].copy()

    # Estimate train and full-sample hedge ratios.
    alpha_train, beta_train = ols_hedge_ratio(train["log_sensex"], train["log_nifty"])
    alpha_full, beta_full = ols_hedge_ratio(df["log_sensex"], df["log_nifty"])

    # For honest out-of-sample backtest, use train-estimated hedge ratio only.
    df = df.copy()
    df["spread"] = build_spread(df, alpha_train, beta_train)

    # Add rolling return correlation.
    df["rolling_return_corr"] = (
        df["ret_sensex"].rolling(ROLLING_CORR_WINDOW).corr(df["ret_nifty"])
    )

    # Add z-score using no-lookahead rolling stats.
    df_z = add_zscore(df, spread_col="spread")

    # Re-split after z-score because early rows are dropped.
    train_end_ts = train.index[-1]
    train_z = df_z[df_z.index <= train_end_ts].copy()
    test_z = df_z[df_z.index > train_end_ts].copy()

    if test_z.empty:
        raise RuntimeError("No test rows left after rolling z-score calculation.")

    # Stationarity/cointegration diagnostics.
    coint_train = run_coint_test(train["log_sensex"], train["log_nifty"])
    coint_full = run_coint_test(df["log_sensex"], df["log_nifty"])

    spread_train = build_spread(train, alpha_train, beta_train)
    spread_full = build_spread(df, alpha_full, beta_full)

    adf_train = run_adf_test(spread_train)
    adf_full = run_adf_test(spread_full)

    hl_train = compute_half_life(spread_train)
    hl_full = compute_half_life(spread_full)

    # Backtest only on the out-of-sample test portion.
    trades_df, equity_df, bt_summary = backtest_zscore_strategy(test_z, beta=beta_train)

    # Z-score diagnostics on test period.
    z_test = test_z["zscore"].dropna()
    z_cross_above = count_threshold_crossings(z_test, ENTRY_Z, direction="above")
    z_cross_below = count_threshold_crossings(z_test, ENTRY_Z, direction="below")

    rolling_corr = df["rolling_return_corr"].dropna()

    diagnostics = PairDiagnostics(
        rows_total=int(len(df)),
        rows_train=int(len(train)),
        rows_test=int(len(test)),
        start_datetime=str(df.index[0]),
        end_datetime=str(df.index[-1]),
        train_end_datetime=str(train_end_ts),
        interval=INTERVAL,
        train_fraction=TRAIN_FRACTION,
        full_return_corr=safe_float(df["ret_sensex"].corr(df["ret_nifty"])) or np.nan,
        train_return_corr=safe_float(train["ret_sensex"].corr(train["ret_nifty"])) or np.nan,
        test_return_corr=safe_float(test["ret_sensex"].corr(test["ret_nifty"])) or np.nan,
        rolling_corr_mean=safe_float(rolling_corr.mean()) or np.nan,
        rolling_corr_min=safe_float(rolling_corr.min()) or np.nan,
        rolling_corr_max=safe_float(rolling_corr.max()) or np.nan,
        rolling_corr_std=safe_float(rolling_corr.std()) or np.nan,
        rolling_corr_p05=safe_float(rolling_corr.quantile(0.05)) or np.nan,
        rolling_corr_p50=safe_float(rolling_corr.quantile(0.50)) or np.nan,
        rolling_corr_p95=safe_float(rolling_corr.quantile(0.95)) or np.nan,
        hedge_ratio_beta_train=beta_train,
        hedge_alpha_train=alpha_train,
        hedge_ratio_beta_full=beta_full,
        hedge_alpha_full=alpha_full,
        spread_mean_train=safe_float(spread_train.mean()) or np.nan,
        spread_std_train=safe_float(spread_train.std()) or np.nan,
        spread_half_life_bars_train=hl_train,
        spread_half_life_bars_full=hl_full,
        coint_pvalue_train=coint_train,
        coint_pvalue_full=coint_full,
        adf_pvalue_train_spread=adf_train,
        adf_pvalue_full_spread=adf_full,
        z_cross_above_entry_count=z_cross_above,
        z_cross_below_entry_count=z_cross_below,
        z_abs_gt_entry_pct=safe_float((z_test.abs() > ENTRY_Z).mean() * 100.0) or 0.0,
        z_abs_gt_stop_pct=safe_float((z_test.abs() > STOP_Z).mean() * 100.0) or 0.0,
    )

    return df_z, trades_df, equity_df, bt_summary, diagnostics


def make_decision_table(diagnostics: PairDiagnostics, bt_summary: Dict[str, float]) -> pd.DataFrame:
    """
    Create a rule-based decision table.

    This is deliberately conservative. It is a research gate, not a trading signal.
    """
    rows = []

    def add(rule: str, value, threshold: str, passed: bool, interpretation: str) -> None:
        rows.append({
            "rule": rule,
            "value": value,
            "preferred_threshold": threshold,
            "pass": bool(passed),
            "interpretation": interpretation,
        })

    add(
        "Return correlation",
        diagnostics.full_return_corr,
        ">= 0.85 for index-index pair",
        diagnostics.full_return_corr >= 0.85,
        "NIFTY-SENSEX should have very strong return co-movement.",
    )

    add(
        "Rolling correlation 5th percentile",
        diagnostics.rolling_corr_p05,
        ">= 0.60 preferred",
        diagnostics.rolling_corr_p05 >= 0.60,
        "Relationship should remain strong even in weaker windows.",
    )

    add(
        "Cointegration p-value, train",
        diagnostics.coint_pvalue_train,
        "< 0.05 preferred",
        diagnostics.coint_pvalue_train is not None and diagnostics.coint_pvalue_train < 0.05,
        "Lower p-value supports a stable long-run spread.",
    )

    add(
        "ADF p-value on train spread",
        diagnostics.adf_pvalue_train_spread,
        "< 0.05 preferred",
        diagnostics.adf_pvalue_train_spread is not None and diagnostics.adf_pvalue_train_spread < 0.05,
        "Lower p-value supports spread stationarity.",
    )

    hl = diagnostics.spread_half_life_bars_train
    add(
        "Half-life, train spread",
        hl,
        "10 to 1500 bars, depending interval",
        hl is not None and 10 <= hl <= 1500,
        "Too low can be noise; too high can be too slow for practical trading.",
    )

    add(
        "Backtest total trades",
        bt_summary.get("total_trades", 0),
        ">= 30 preferred for initial confidence",
        bt_summary.get("total_trades", 0) >= 30,
        "Too few trades means the result is not statistically meaningful.",
    )

    add(
        "Backtest net PnL after estimated costs",
        bt_summary.get("net_pnl", 0.0),
        "> 0",
        bt_summary.get("net_pnl", 0.0) > 0,
        "Negative net PnL after costs means no practical edge under these settings.",
    )

    add(
        "Profit factor",
        bt_summary.get("profit_factor", 0.0),
        ">= 1.30 preferred",
        bt_summary.get("profit_factor", 0.0) >= 1.30,
        "Profit factor should be comfortably above 1 after costs.",
    )

    add(
        "Win rate",
        bt_summary.get("win_rate_pct", 0.0),
        ">= 55% preferred",
        bt_summary.get("win_rate_pct", 0.0) >= 55.0,
        "Mean-reversion strategies should not rely only on rare large winners.",
    )

    return pd.DataFrame(rows)


def write_outputs(full_df: pd.DataFrame, trades_df: pd.DataFrame, equity_df: pd.DataFrame,
                  bt_summary: Dict[str, float], diagnostics: PairDiagnostics) -> None:
    """Write CSV, pickle, and Excel outputs."""
    ensure_output_dir()

    aligned_path = os.path.join(OUTPUT_DIR, "nifty_sensex_aligned_data.pkl")
    trades_path = os.path.join(OUTPUT_DIR, "nifty_sensex_backtest_trades.csv")
    equity_path = os.path.join(OUTPUT_DIR, "nifty_sensex_equity_curve.csv")
    excel_path = os.path.join(OUTPUT_DIR, "nifty_sensex_tradeability_report.xlsx")

    full_df.to_pickle(aligned_path)
    trades_df.to_csv(trades_path, index=False)
    equity_df.to_csv(equity_path, index=False)

    diagnostics_df = pd.DataFrame([asdict(diagnostics)])
    bt_summary_df = pd.DataFrame([bt_summary])
    decision_df = make_decision_table(diagnostics, bt_summary)

    config_rows = [
        {"parameter": "INTERVAL", "value": INTERVAL},
        {"parameter": "LOOKBACK_DAYS", "value": LOOKBACK_DAYS},
        {"parameter": "ROLLING_CORR_WINDOW", "value": ROLLING_CORR_WINDOW},
        {"parameter": "ROLLING_Z_WINDOW", "value": ROLLING_Z_WINDOW},
        {"parameter": "TRAIN_FRACTION", "value": TRAIN_FRACTION},
        {"parameter": "ENTRY_Z", "value": ENTRY_Z},
        {"parameter": "EXIT_Z", "value": EXIT_Z},
        {"parameter": "STOP_Z", "value": STOP_Z},
        {"parameter": "MAX_HOLD_BARS", "value": MAX_HOLD_BARS},
        {"parameter": "TRADE_NOTIONAL", "value": TRADE_NOTIONAL},
        {"parameter": "COST_BPS_PER_LEG", "value": COST_BPS_PER_LEG},
        {"parameter": "SLIPPAGE_BPS_PER_LEG", "value": SLIPPAGE_BPS_PER_LEG},
        {"parameter": "STATSMODELS_AVAILABLE", "value": STATSMODELS_AVAILABLE},
        {"parameter": "FORCE_DOWNLOAD", "value": FORCE_DOWNLOAD},
        {"parameter": "CLOSE_MATRIX_PATH", "value": CLOSE_MATRIX_PATH},
        {"parameter": "OUTPUT_DIR", "value": OUTPUT_DIR},
    ]
    config_df = pd.DataFrame(config_rows)

    # Keep Excel sheets manageable. Full minute-level dataframe can be very large.
    zscore_sample = full_df[[
        "NIFTY", "SENSEX", "ret_nifty", "ret_sensex", "spread", "rolling_return_corr", "zscore"
    ]].tail(5000).reset_index().rename(columns={"index": "datetime"})

    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        diagnostics_df.to_excel(writer, sheet_name="diagnostics", index=False)
        decision_df.to_excel(writer, sheet_name="decision_table", index=False)
        bt_summary_df.to_excel(writer, sheet_name="backtest_summary", index=False)
        trades_df.to_excel(writer, sheet_name="backtest_trades", index=False)
        equity_df.tail(5000).to_excel(writer, sheet_name="equity_curve_tail", index=False)
        zscore_sample.to_excel(writer, sheet_name="zscore_tail", index=False)
        config_df.to_excel(writer, sheet_name="run_config", index=False)

    print("\n==================== OUTPUTS ====================")
    print(f"Aligned data pickle : {aligned_path}")
    print(f"Trades CSV          : {trades_path}")
    print(f"Equity CSV          : {equity_path}")
    print(f"Excel report        : {excel_path}")
    print("=================================================")


def print_console_summary(diagnostics: PairDiagnostics, bt_summary: Dict[str, float]) -> None:
    """Print concise summary to console."""
    print("\n==================== DIAGNOSTICS SUMMARY ====================")
    print(f"Rows total / train / test : {diagnostics.rows_total} / {diagnostics.rows_train} / {diagnostics.rows_test}")
    print(f"Date range                : {diagnostics.start_datetime} -> {diagnostics.end_datetime}")
    print(f"Train end                 : {diagnostics.train_end_datetime}")
    print(f"Full return corr          : {diagnostics.full_return_corr:.4f}")
    print(f"Train return corr         : {diagnostics.train_return_corr:.4f}")
    print(f"Test return corr          : {diagnostics.test_return_corr:.4f}")
    print(f"Rolling corr mean/min/p05 : {diagnostics.rolling_corr_mean:.4f} / {diagnostics.rolling_corr_min:.4f} / {diagnostics.rolling_corr_p05:.4f}")
    print(f"Train hedge beta          : {diagnostics.hedge_ratio_beta_train:.6f}")
    print(f"Full hedge beta           : {diagnostics.hedge_ratio_beta_full:.6f}")
    print(f"Train coint p-value       : {diagnostics.coint_pvalue_train}")
    print(f"Train ADF spread p-value  : {diagnostics.adf_pvalue_train_spread}")
    print(f"Train spread half-life    : {diagnostics.spread_half_life_bars_train} bars")
    print(f"Z crosses +ENTRY / -ENTRY : {diagnostics.z_cross_above_entry_count} / {diagnostics.z_cross_below_entry_count}")

    print("\n==================== BACKTEST SUMMARY ====================")
    for k, v in bt_summary.items():
        print(f"{k:30s}: {v}")
    print("==========================================================")

    if not STATSMODELS_AVAILABLE:
        print("\n[IMPORTANT] statsmodels is not installed. Cointegration and ADF tests were skipped.")
        print("Install with: pip install statsmodels")


# ===================== ENTRYPOINT =====================

def main() -> None:
    print("========================================================")
    print("NIFTY-SENSEX Tradeability Tester")
    print("========================================================")
    print(f"INTERVAL              = {INTERVAL}")
    print(f"LOOKBACK_DAYS         = {LOOKBACK_DAYS}")
    print(f"ROLLING_CORR_WINDOW   = {ROLLING_CORR_WINDOW}")
    print(f"ROLLING_Z_WINDOW      = {ROLLING_Z_WINDOW}")
    print(f"TRAIN_FRACTION        = {TRAIN_FRACTION}")
    print(f"ENTRY_Z / EXIT_Z      = {ENTRY_Z} / {EXIT_Z}")
    print(f"STOP_Z                = {STOP_Z}")
    print(f"MAX_HOLD_BARS         = {MAX_HOLD_BARS}")
    print(f"TRADE_NOTIONAL        = {TRADE_NOTIONAL}")
    print(f"COST+SLIPPAGE bps/leg = {COST_BPS_PER_LEG + SLIPPAGE_BPS_PER_LEG}")
    print(f"STATSMODELS_AVAILABLE = {STATSMODELS_AVAILABLE}")
    print("========================================================")

    wide_close = load_or_download_close_matrix()
    pair_df = prepare_pair_data(wide_close)

    print(f"[INFO] Prepared aligned pair rows: {len(pair_df)}")
    print(f"[INFO] First timestamp: {pair_df.index[0]}")
    print(f"[INFO] Last timestamp : {pair_df.index[-1]}")

    full_df, trades_df, equity_df, bt_summary, diagnostics = analyze_tradeability(pair_df)

    print_console_summary(diagnostics, bt_summary)
    write_outputs(full_df, trades_df, equity_df, bt_summary, diagnostics)

    print("\n[DONE] NIFTY-SENSEX tradeability test completed.")


if __name__ == "__main__":
    main()
