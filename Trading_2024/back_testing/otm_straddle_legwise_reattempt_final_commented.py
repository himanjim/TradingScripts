"""
OTM short straddle backtest with:
1) Configurable entry time
2) Configurable OTM distance from ATM
3) Nearest-expiry contract selection for each underlying/day
4) Independent leg-wise stop logic:
   - Exit PE if PE premium rises by X% from PE entry
   - Exit CE if CE premium rises by X% from CE entry
   - After one leg exits, the other leg continues to be monitored until its own stop or EOD
5) Configurable reattempt gap and number of reattempts
6) Excel output with detailed trade logs and summaries

This script is intentionally derived from the user's reference straddle script, but the trade engine
is different from the reference in one major respect:
- The reference logic is straddle-PnL driven
- This script is leg-wise premium-rise driven

Key implementation assumptions
------------------------------
- Entry is taken only if both selected option legs have an exact close price at the configured entry minute.
  If either leg is missing an exact entry-minute close in the pickle, that attempt is treated as a failed
  attempt slot and the engine moves to the next allowed reattempt time, subject to MAX_REATTEMPTS.
  This is a conservative choice and avoids silently inventing entry prices.
- Stop detection is done using the candle HIGH of the option premium after entry.
  This is suitable for a short option stop based on premium rising.
- Once a stop is hit inside a minute, the exit price is assumed to be the configured trigger price,
  not the candle high. This reflects a continuous-monitoring approximation rather than worst-fill-at-high.
- Reattempt happens only after the whole straddle is finished, i.e. after the later of the two leg exits.
  Failed entry attempts also consume an attempt slot and move to the next scheduled reattempt time.

Environment variables supported
-------------------------------
ENTRY_TIME_IST                e.g. "09:30"
LOOKBACK_MONTHS               e.g. 6
MAX_REATTEMPTS                e.g. 3   (3 means 1 initial attempt + up to 3 reattempts)
REENTRY_DELAY_MINUTES         e.g. 5
OTM_DISTANCE_STEPS            e.g. 1   (NIFTY: 1 => 50 points, SENSEX: 1 => 100 points)
LEG_PREMIUM_RISE_EXIT_PCT     e.g. 30  (exit a leg if premium rises 30% from entry)
INCLUDE_TRANSACTION_COSTS     1/0
FAIL_ON_PICKLE_ERROR          1/0
OUTPUT_XLSX                   optional explicit output path
"""

import os
from pathlib import Path
import glob
import time
from dataclasses import dataclass
from datetime import datetime, date, time as dtime, timedelta
from typing import Dict, List, Tuple, Optional, Any

import pandas as pd

import Trading_2024.OptionTradeUtils as oUtils

try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:
    ZoneInfo = None  # type: ignore

try:
    import pytz  # type: ignore
except Exception:
    pytz = None  # type: ignore

try:
    from dateutil.relativedelta import relativedelta  # type: ignore
except Exception:
    relativedelta = None  # type: ignore


# =============================================================================
# USER CONFIG
# =============================================================================
PICKLES_DIR = r"G:\My Drive\Trading\Historical_Options_Data"

# Main strategy controls
ENTRY_TIME_IST = os.getenv("ENTRY_TIME_IST", "10:05")  # format: HH:MM
LOOKBACK_MONTHS = int(os.getenv("LOOKBACK_MONTHS", "7"))

# Reattempt controls
# MAX_REATTEMPTS = number of reattempts AFTER the first attempt.
# Example:
#   0 => only the first attempt
#   1 => first attempt + at most one reattempt
MAX_REATTEMPTS = int(os.getenv("MAX_REATTEMPTS", "4"))
REENTRY_DELAY_MINUTES = int(os.getenv("REENTRY_DELAY_MINUTES", "9"))

# OTM distance in STRIKE STEPS, not raw points.
# Example:
#   NIFTY step is 50, SENSEX step is 100
#   OTM_DISTANCE_STEPS=2 means:
#       NIFTY  => PE strike = ATM - 100, CE strike = ATM + 100
#       SENSEX => PE strike = ATM - 200, CE strike = ATM + 200
OTM_DISTANCE_STEPS = int(os.getenv("OTM_DISTANCE_STEPS", "1"))

# Stop threshold for EACH LEG independently.
# Example: 30 means exit a leg when premium >= entry_premium * 1.30
LEG_PREMIUM_RISE_EXIT_PCT = float(os.getenv("LEG_PREMIUM_RISE_EXIT_PCT", "13"))

# Operational flags
FAIL_ON_PICKLE_ERROR = os.getenv("FAIL_ON_PICKLE_ERROR", "0").strip() == "1"
INCLUDE_TRANSACTION_COSTS = os.getenv("INCLUDE_TRANSACTION_COSTS", "1").strip() == "1"

# Trading session bounds
SESSION_START_IST = dtime(9, 15)
SESSION_END_IST = dtime(15, 30)

# Quantity and strike-step settings
QTY_UNITS = {"NIFTY": 325, "SENSEX": 100}
TRADEABLE = set(QTY_UNITS.keys())
STRIKE_STEP = {"NIFTY": 50, "SENSEX": 100}

# Kite historical download throttling
MAX_DAYS_PER_CHUNK = 25
MAX_ATTEMPTS = 5
SLEEP_BETWEEN_CALLS_SEC = 0.20


# =============================================================================
# TRANSACTION CHARGES
# =============================================================================
# These are kept aligned with the user's earlier reference style.
# If you want broker/exchange-specific changes later, update here centrally.
BROKERAGE_PER_ORDER = 20.0
STT_SELL_PCT = 0.001
EXCHANGE_TXN_PCT = 0.0003553
SEBI_PER_CRORE = 10.0
STAMP_BUY_PCT = 0.00003
IPFT_PER_CRORE = 0.010
GST_PCT = 0.18


# =============================================================================
# PATH HELPERS
# =============================================================================
def _safe_fname_part(s: str) -> str:
    """Make a string safe for use inside a filename."""
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in s)


def _get_downloads_folder() -> str:
    """Return user's Downloads folder if it exists, else home folder."""
    downloads = Path.home() / "Downloads"
    return str(downloads if downloads.exists() else Path.home())


_DEFAULT_OUT = os.path.join(
    _get_downloads_folder(),
    f"otm_short_straddle_legwise_{_safe_fname_part(ENTRY_TIME_IST)}"
    f"_OTM_{OTM_DISTANCE_STEPS}"
    f"_SLPCT_{_safe_fname_part(str(LEG_PREMIUM_RISE_EXIT_PCT))}"
    f"_MR_{MAX_REATTEMPTS}"
    f"_RDM_{REENTRY_DELAY_MINUTES}.xlsx"
)
OUTPUT_XLSX = os.getenv("OUTPUT_XLSX", _DEFAULT_OUT)


# =============================================================================
# TIME / TZ HELPERS
# =============================================================================
def parse_hhmm(s: str) -> dtime:
    """Parse a HH:MM string into a time object."""
    hh, mm = s.strip().split(":")
    return dtime(int(hh), int(mm))


ENTRY_TIME = parse_hhmm(ENTRY_TIME_IST)


def ist_tz():
    """Return Asia/Kolkata timezone object in a way compatible with available libraries."""
    if ZoneInfo is not None:
        return ZoneInfo("Asia/Kolkata")
    if pytz is not None:
        return pytz.timezone("Asia/Kolkata")
    return "Asia/Kolkata"


def ensure_ist(series_or_scalar) -> Any:
    """
    Convert pandas datetime series/scalar to IST.
    If naive, localize to IST.
    If timezone-aware, convert to IST.
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


# =============================================================================
# GENERAL HELPERS
# =============================================================================
def normalize_underlying(name: str) -> Optional[str]:
    """
    Normalize various underlying names found in pickle data to our internal symbols.
    Non-tradeable or unrecognized names return None.
    """
    if not isinstance(name, str):
        return None
    u = name.upper().strip()
    if "SENSEX" in u:
        return "SENSEX"
    if "BANKNIFTY" in u or "NIFTY BANK" in u:
        return "BANKNIFTY"
    if "NIFTY" in u:
        return "NIFTY"
    return None


def round_to_step(x: float, step: int) -> int:
    """Round a price to the nearest strike step."""
    return int(round(x / step) * step)


def build_minute_index(day_d: date, start_t: dtime, end_t: dtime) -> pd.DatetimeIndex:
    """Build a one-minute IST session index for a given day."""
    tz = ist_tz()
    start = pd.Timestamp(datetime.combine(day_d, start_t), tz=tz)
    end = pd.Timestamp(datetime.combine(day_d, end_t), tz=tz)
    return pd.date_range(start=start, end=end, freq="1min")


def asof_close(df: pd.DataFrame, ts: pd.Timestamp) -> float:
    """
    Get underlying close at or before timestamp ts.
    This is used only for ATM determination, not option entry fill.
    """
    if df.empty:
        return float("nan")
    d = df[["date", "close"]].dropna().copy()
    d["date"] = ensure_ist(d["date"])
    d = d.sort_values("date").set_index("date")
    loc = d.index.get_indexer([ts], method="pad")
    if loc[0] == -1:
        return float("nan")
    return float(d.iloc[loc[0]]["close"])


def compute_window_start(end_day: date, months: int) -> date:
    """Compute lookback start date from the last data day."""
    if relativedelta is not None:
        return (pd.Timestamp(end_day) - relativedelta(months=months)).date()
    return (pd.Timestamp(end_day) - pd.Timedelta(days=30 * months)).date()


def compute_otm_strikes(atm: int, step: int, distance_steps: int) -> Tuple[int, int]:
    """
    Return OTM PE strike and OTM CE strike based on ATM and step size.
    """
    dist = int(step * distance_steps)
    return atm - dist, atm + dist


def compute_leg_exit_trigger(entry_price: float) -> float:
    """Return stop trigger price for a short leg based on entry premium."""
    return float(entry_price) * (1.0 + LEG_PREMIUM_RISE_EXIT_PCT / 100.0)


def validate_user_config() -> None:
    """
    Validate runtime configuration early, so the script fails fast on bad inputs.
    """
    if LOOKBACK_MONTHS < 0:
        raise ValueError("LOOKBACK_MONTHS must be >= 0")
    if MAX_REATTEMPTS < 0:
        raise ValueError("MAX_REATTEMPTS must be >= 0")
    if REENTRY_DELAY_MINUTES < 0:
        raise ValueError("REENTRY_DELAY_MINUTES must be >= 0")
    if OTM_DISTANCE_STEPS < 0:
        raise ValueError("OTM_DISTANCE_STEPS must be >= 0")
    if LEG_PREMIUM_RISE_EXIT_PCT <= 0:
        raise ValueError("LEG_PREMIUM_RISE_EXIT_PCT must be > 0")
    if ENTRY_TIME < SESSION_START_IST or ENTRY_TIME >= SESSION_END_IST:
        raise ValueError(
            f"ENTRY_TIME_IST must be within session [{SESSION_START_IST.strftime('%H:%M')}, "
            f"{SESSION_END_IST.strftime('%H:%M')})"
        )


# =============================================================================
# TRANSACTION COST CALCULATOR
# =============================================================================
def compute_trade_charges(
    entry_ce: float,
    entry_pe: float,
    exit_ce: float,
    exit_pe: float,
    qty: int,
) -> float:
    """
    Compute estimated transaction charges for one complete short straddle attempt:
    SELL CE + SELL PE + BUY CE + BUY PE

    Notes:
    - Charges are computed on premium turnover, in line with the earlier reference script.
    - If INCLUDE_TRANSACTION_COSTS is disabled, this returns 0.
    """
    if not INCLUDE_TRANSACTION_COSTS:
        return 0.0

    entry_turnover = (entry_ce + entry_pe) * qty
    exit_turnover = (exit_ce + exit_pe) * qty
    total_turnover = entry_turnover + exit_turnover

    brokerage = BROKERAGE_PER_ORDER * 4.0
    stt = entry_turnover * STT_SELL_PCT
    txn_charges = total_turnover * EXCHANGE_TXN_PCT
    sebi = total_turnover * SEBI_PER_CRORE / 1_00_00_000
    stamp = exit_turnover * STAMP_BUY_PCT
    ipft = total_turnover * IPFT_PER_CRORE / 1_00_00_000
    gst = (brokerage + txn_charges + sebi) * GST_PCT

    return round(brokerage + stt + txn_charges + sebi + stamp + ipft + gst, 2)


# =============================================================================
# KITE HISTORICAL HELPERS
# =============================================================================
def _iter_chunks_by_date(from_dt: datetime, to_dt: datetime, days_per_chunk: int) -> List[Tuple[datetime, datetime]]:
    """Split a long historical request into smaller date chunks."""
    if from_dt > to_dt:
        raise ValueError("from_dt must be <= to_dt")
    chunks: List[Tuple[datetime, datetime]] = []
    cur = from_dt.date()
    end_d = to_dt.date()
    while cur <= end_d:
        chunk_end = min(cur + timedelta(days=days_per_chunk - 1), end_d)
        c_from = from_dt if cur == from_dt.date() else datetime.combine(cur, SESSION_START_IST)
        c_to = to_dt if chunk_end == end_d else datetime.combine(chunk_end, SESSION_END_IST)
        chunks.append((c_from, c_to))
        cur = chunk_end + timedelta(days=1)
    return chunks


def _kite_instruments_cached(kite, exchange: str, cache: Dict[str, List[Dict]]) -> List[Dict]:
    """Cache full instrument dumps per exchange to avoid repeated API hits."""
    ex = exchange.upper().strip()
    if ex not in cache:
        print(f"[STEP] Loading instruments dump for {ex} ...")
        cache[ex] = kite.instruments(ex)
        print(f"[INFO] {ex} instruments: {len(cache[ex])}")
    return cache[ex]


def get_instrument_token(kite, exchange: str, tradingsymbol: str, cache: Dict[str, List[Dict]]) -> int:
    """Lookup instrument token for a given exchange + tradingsymbol."""
    ex = exchange.upper().strip()
    wanted = tradingsymbol.strip().upper()
    for r in _kite_instruments_cached(kite, ex, cache):
        if str(r.get("tradingsymbol", "")).upper() == wanted:
            return int(r["instrument_token"])
    raise ValueError(f"Instrument not found on {ex}: '{tradingsymbol}'")


def fetch_history_minute(kite, instrument_token: int, from_dt: datetime, to_dt: datetime, label: str) -> List[Dict]:
    """
    Download minute history in chunks with retries.
    """
    interval = "minute"
    chunks = _iter_chunks_by_date(from_dt, to_dt, MAX_DAYS_PER_CHUNK)
    rows_all: List[Dict] = []
    print(f"[INFO] Fetch {label} token={instrument_token} chunks={len(chunks)} {from_dt} -> {to_dt}")

    for i, (c_from, c_to) in enumerate(chunks, start=1):
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
                rows_all.extend(rows)
                last_err = None
                break
            except Exception as e:
                last_err = e
                time.sleep(min(8.0, 1.5 * attempt))

        if last_err is not None:
            print(f"[ERROR] {label} chunk {i}/{len(chunks)} failed: {c_from}->{c_to}: {last_err}")

        time.sleep(SLEEP_BETWEEN_CALLS_SEC)

    return rows_all


def rows_to_df(rows: List[Dict]) -> pd.DataFrame:
    """Convert Kite historical rows to a clean DataFrame."""
    if not rows:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
    df = pd.DataFrame(rows)
    df["date"] = ensure_ist(df["date"])
    df = df.drop_duplicates(subset=["date"], keep="last").sort_values("date").reset_index(drop=True)
    return df


# =============================================================================
# DATA STRUCTURES
# =============================================================================
@dataclass
class TradeRow:
    """
    One completed strategy attempt.

    final_exit_reason summarizes the pair of leg outcomes:
    - BOTH_EOD
    - BOTH_LEG_SL
    - PE_LEG_SL_CE_EOD
    - CE_LEG_SL_PE_EOD
    """
    day: date
    underlying: str
    trade_seq: int
    expiry: date
    days_to_expiry: int
    qty_units: int
    entry_time: str
    final_exit_time: str
    final_exit_reason: str
    entry_underlying: float
    atm_strike: int
    otm_distance_steps: int
    pe_strike: int
    ce_strike: int
    pe_symbol: str
    ce_symbol: str
    entry_pe: float
    entry_ce: float
    pe_exit_time: str
    ce_exit_time: str
    pe_exit_reason: str
    ce_exit_reason: str
    pe_exit_price: float
    ce_exit_price: float
    pe_exit_trigger: float
    ce_exit_trigger: float
    pe_minutes_held: int
    ce_minutes_held: int
    pe_pnl_gross: float
    ce_pnl_gross: float
    exit_pnl_gross: float
    txn_charges: float
    exit_pnl: float
    eod_pnl_if_held: float


# =============================================================================
# PASS-1: FIND NEAREST EXPIRY PER (UNDERLYING, DAY)
# =============================================================================
def scan_pickles_pass1(pickle_paths: List[str]) -> Tuple[date, Dict[Tuple[str, date], date], date]:
    """
    Pass 1 scans the pickles only to determine:
    - minimum available expiry for each (underlying, day)
    - overall min/max day seen

    This lets us enforce the user's rule that the traded instrument for a day must be
    the one nearest to expiry for that underlying/day.
    """
    max_day_seen: Optional[date] = None
    min_day_seen: Optional[date] = None
    min_expiry_map: Dict[Tuple[str, date], date] = {}

    for p in pickle_paths:
        try:
            df = pd.read_pickle(p)
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue

            for c in ("date", "name", "expiry", "type"):
                if c not in df.columns:
                    raise ValueError(f"Missing column '{c}' in {p}")

            d2 = df[df["type"].astype(str).str.upper().eq("OPTION")]
            if d2.empty:
                continue

            d2 = d2[["date", "name", "expiry"]].copy()
            d2["date"] = ensure_ist(d2["date"])
            d2["day"] = d2["date"].dt.date
            d2["underlying"] = d2["name"].astype(str).map(normalize_underlying)
            d2["expiry_date"] = pd.to_datetime(d2["expiry"], errors="coerce").dt.date
            d2 = d2.dropna(subset=["underlying", "day", "expiry_date"])

            d2 = d2[d2["underlying"].isin(TRADEABLE)]
            d2 = d2[d2["expiry_date"] >= d2["day"]]
            if d2.empty:
                continue

            file_min_day = d2["day"].min()
            file_max_day = d2["day"].max()
            max_day_seen = file_max_day if (max_day_seen is None or file_max_day > max_day_seen) else max_day_seen
            min_day_seen = file_min_day if (min_day_seen is None or file_min_day < min_day_seen) else min_day_seen

            grp = d2.groupby(["underlying", "day"], sort=False)["expiry_date"].min()
            for (und, dy), ex in grp.items():
                key = (und, dy)
                if key not in min_expiry_map or ex < min_expiry_map[key]:
                    min_expiry_map[key] = ex

            print(f"[PASS1 OK] {os.path.basename(p)} option_days={d2['day'].nunique()}")

        except Exception as e:
            msg = f"[PASS1 WARN] {os.path.basename(p)} failed: {e}"
            if FAIL_ON_PICKLE_ERROR:
                raise RuntimeError(msg) from e
            print(msg)

    if max_day_seen is None or min_day_seen is None:
        raise RuntimeError("No usable option data found in pickles (PASS1) for tradeable underlyings.")

    return max_day_seen, min_expiry_map, min_day_seen


# =============================================================================
# UNDERLYING DOWNLOAD
# =============================================================================
UNDERLYING_KITE = {
    "NIFTY": {"exchange": "NSE", "tradingsymbol": "NIFTY 50"},
    "SENSEX": {"exchange": "BSE", "tradingsymbol": "SENSEX"},
}


def download_underlyings(kite, day_start: date, day_end: date) -> Dict[str, pd.DataFrame]:
    """
    Download underlying minute data from Kite for the same window used in the backtest.
    """
    cache: Dict[str, List[Dict]] = {}
    from_dt = datetime.combine(day_start, SESSION_START_IST)
    to_dt = datetime.combine(day_end, SESSION_END_IST)

    out: Dict[str, pd.DataFrame] = {}
    for und, meta in UNDERLYING_KITE.items():
        token = get_instrument_token(kite, meta["exchange"], meta["tradingsymbol"], cache)
        rows = fetch_history_minute(kite, token, from_dt, to_dt, label=f"{meta['exchange']}:{meta['tradingsymbol']}")
        df = rows_to_df(rows)
        df["day"] = df["date"].dt.tz_convert(ist_tz()).dt.date
        out[und] = df
        print(f"[UNDERLYING OK] {und}: candles={len(df)} days={df['day'].nunique()}")
    return out


# =============================================================================
# SIMULATION HELPERS
# =============================================================================
def _pick_symbol(day_opt: pd.DataFrame, strike: int, opt_type: str) -> Optional[str]:
    """
    Pick a unique tradingsymbol for a strike/type within the day-expiry slice.
    If multiple symbols exist, choose lexicographically first.
    """
    sub = day_opt[(day_opt["strike_int"] == strike) & (day_opt["option_type"] == opt_type)]
    if sub.empty:
        return None
    syms = sorted(sub["instrument"].astype(str).unique().tolist())
    return syms[0] if syms else None


def _build_leg_series(
    day_opt: pd.DataFrame,
    idx_all: pd.DatetimeIndex,
    strike: int,
    opt_type: str,
    symbol: str,
    price_col: str,
    do_ffill: bool,
) -> pd.Series:
    """
    Build a minute-aligned option price series for one specific symbol.
    do_ffill=True is useful for mark-to-market style tracking.
    do_ffill=False is useful where exact presence matters, e.g. exact entry print.
    """
    sub = day_opt[
        (day_opt["strike_int"] == strike)
        & (day_opt["option_type"] == opt_type)
        & (day_opt["instrument"].astype(str) == symbol)
    ][["date", price_col]].dropna()

    if sub.empty:
        return pd.Series(index=idx_all, dtype="float64")

    sub = sub.copy()
    sub["date"] = ensure_ist(sub["date"])
    sub = sub.sort_values("date").drop_duplicates(subset=["date"], keep="last").set_index("date")
    s = sub[price_col].astype(float).reindex(idx_all)
    return s.ffill() if do_ffill else s


def _first_hit_ts(series_bool: pd.Series) -> Optional[pd.Timestamp]:
    """Return the timestamp of the first True value, else None."""
    if series_bool.empty or not bool(series_bool.any()):
        return None
    return series_bool.index[series_bool.to_numpy().argmax()]


def _series_value(s: pd.Series, ts: pd.Timestamp) -> float:
    """Read a series value safely as float or NaN."""
    v = s.loc[ts]
    return float(v) if pd.notna(v) else float("nan")


def _classify_final_exit_reason(pe_reason: str, ce_reason: str) -> str:
    """Create a compact human-readable final exit label for the whole straddle."""
    if pe_reason == "EOD" and ce_reason == "EOD":
        return "BOTH_EOD"
    if pe_reason == "LEG_SL" and ce_reason == "LEG_SL":
        return "BOTH_LEG_SL"
    if pe_reason == "LEG_SL" and ce_reason == "EOD":
        return "PE_LEG_SL_CE_EOD"
    if pe_reason == "EOD" and ce_reason == "LEG_SL":
        return "CE_LEG_SL_PE_EOD"
    return f"PE_{pe_reason}_CE_{ce_reason}"


def _minutes_between(a: pd.Timestamp, b: pd.Timestamp) -> int:
    """Return integer minute difference between two timestamps."""
    return int((b - a).total_seconds() // 60)


def _next_attempt_timestamp(
    *,
    current_trade_seq: int,
    base_ts: pd.Timestamp,
    session_end_ts: pd.Timestamp,
) -> Optional[pd.Timestamp]:
    """
    Return the next allowed attempt timestamp or None if no more attempts are possible.

    Rules:
    - total attempts allowed = 1 + MAX_REATTEMPTS
    - next attempt is base_ts + REENTRY_DELAY_MINUTES
    - next attempt must still be strictly before session end
    """
    max_total_attempts = 1 + MAX_REATTEMPTS
    if current_trade_seq >= max_total_attempts:
        return None

    next_ts = pd.Timestamp(base_ts) + pd.Timedelta(minutes=REENTRY_DELAY_MINUTES)
    if next_ts >= session_end_ts:
        return None
    return next_ts



def _simulate_leg_exit(
    *,
    entry_ts: pd.Timestamp,
    session_end_ts: pd.Timestamp,
    entry_price: float,
    close_series_ffill: pd.Series,
    high_series_raw: pd.Series,
    trigger_price: float,
) -> Tuple[pd.Timestamp, str, float]:
    """
    Simulate exit of one short option leg.

    Logic:
    - Start monitoring from entry_ts + 1 minute
    - If HIGH >= trigger_price on any minute, exit there at trigger_price
    - Else exit at EOD using the last forward-filled close

    Returns:
    - exit timestamp
    - exit reason ("LEG_SL" or "EOD")
    - exit price
    """
    monitor_idx = close_series_ffill.loc[entry_ts + pd.Timedelta(minutes=1): session_end_ts].index

    # If there is no post-entry monitor window, force same-price same-time exit.
    # This should be rare because entry validation already prevents ENTRY_TIME at/after session end.
    if len(monitor_idx) == 0:
        return entry_ts, "EOD", float(entry_price)

    high_monitor = high_series_raw.reindex(monitor_idx)
    close_monitor = close_series_ffill.reindex(monitor_idx)

    # If HIGH is missing for a minute, fall back to close for that minute.
    observed_high = high_monitor.combine_first(close_monitor)
    hit_mask = observed_high >= trigger_price
    hit_ts = _first_hit_ts(hit_mask)

    if hit_ts is not None:
        # Continuous monitoring approximation:
        # once trigger is reached, exit at the trigger price, not at candle high.
        return hit_ts, "LEG_SL", float(trigger_price)

    eod_ts = monitor_idx[-1]
    return eod_ts, "EOD", _series_value(close_series_ffill, eod_ts)


# =============================================================================
# CORE DAY SIMULATION
# =============================================================================
def simulate_day_multi_trades(
    *,
    und: str,
    dy: date,
    expiry: date,
    day_opt: pd.DataFrame,
    underlying_day: pd.DataFrame,
) -> Tuple[List[TradeRow], List[Dict[str, Any]]]:
    """
    Simulate one day for one underlying and one expiry.

    Important semantics
    -------------------
    - Only one underlying is simulated for a given trading day. That filtering is already done in pass-2.
    - MAX_REATTEMPTS means reattempts *after* the first attempt.
      Therefore total possible attempt slots in the day are 1 + MAX_REATTEMPTS.
    - A completed trade can be reattempted only after the later of the two leg exits.
    - A failed entry attempt (for example, exact entry-minute option close is missing) also consumes one
      attempt slot and shifts the engine to the next allowed entry time after REENTRY_DELAY_MINUTES.
    """
    results: List[TradeRow] = []
    skipped: List[Dict[str, Any]] = []

    idx_all = build_minute_index(dy, SESSION_START_IST, SESSION_END_IST)
    session_end_ts = idx_all[-1]

    qty = int(QTY_UNITS[und])
    step = int(STRIKE_STEP[und])

    trade_seq = 1
    cur_entry_ts = pd.Timestamp(datetime.combine(dy, ENTRY_TIME), tz=ist_tz())

    # Each loop iteration corresponds to one attempt slot.
    while trade_seq <= (1 + MAX_REATTEMPTS) and cur_entry_ts < session_end_ts:
        u_px = asof_close(underlying_day, cur_entry_ts)
        if pd.isna(u_px):
            skipped.append({
                "day": dy,
                "underlying": und,
                "expiry": expiry,
                "trade_seq": trade_seq,
                "entry_time": cur_entry_ts.strftime("%H:%M"),
                "reason": f"No underlying price at entry {cur_entry_ts.strftime('%H:%M')}",
            })
            next_entry = _next_attempt_timestamp(
                current_trade_seq=trade_seq,
                base_ts=cur_entry_ts,
                session_end_ts=session_end_ts,
            )
            if next_entry is None:
                break
            trade_seq += 1
            cur_entry_ts = next_entry
            continue

        # Compute ATM from the underlying and then derive the requested OTM strikes.
        atm = round_to_step(float(u_px), step)
        pe_strike, ce_strike = compute_otm_strikes(atm, step, OTM_DISTANCE_STEPS)

        # Select only the requested OTM strikes from the already-filtered nearest-expiry slice.
        pe_sym = _pick_symbol(day_opt, pe_strike, "PE")
        ce_sym = _pick_symbol(day_opt, ce_strike, "CE")
        if not pe_sym or not ce_sym:
            skipped.append({
                "day": dy,
                "underlying": und,
                "expiry": expiry,
                "trade_seq": trade_seq,
                "entry_time": cur_entry_ts.strftime("%H:%M"),
                "atm_strike": atm,
                "pe_strike": pe_strike,
                "ce_strike": ce_strike,
                "reason": "Requested OTM CE/PE not available in nearest-expiry pickle band",
            })
            next_entry = _next_attempt_timestamp(
                current_trade_seq=trade_seq,
                base_ts=cur_entry_ts,
                session_end_ts=session_end_ts,
            )
            if next_entry is None:
                break
            trade_seq += 1
            cur_entry_ts = next_entry
            continue

        # Build exact and forward-filled price series for both legs.
        # Exact series is used for validating the entry print.
        # Forward-filled series is used for post-entry mark-to-market and EOD marking.
        pe_close_raw = _build_leg_series(day_opt, idx_all, pe_strike, "PE", pe_sym, "close", False)
        ce_close_raw = _build_leg_series(day_opt, idx_all, ce_strike, "CE", ce_sym, "close", False)
        pe_close = pe_close_raw.ffill()
        ce_close = ce_close_raw.ffill()

        # HIGH is used for stop detection because this is a short-premium stop.
        pe_high = _build_leg_series(day_opt, idx_all, pe_strike, "PE", pe_sym, "high", False)
        ce_high = _build_leg_series(day_opt, idx_all, ce_strike, "CE", ce_sym, "high", False)

        if cur_entry_ts not in idx_all:
            skipped.append({
                "day": dy,
                "underlying": und,
                "expiry": expiry,
                "trade_seq": trade_seq,
                "entry_time": cur_entry_ts.strftime("%H:%M"),
                "reason": "Entry timestamp not in session index",
            })
            next_entry = _next_attempt_timestamp(
                current_trade_seq=trade_seq,
                base_ts=cur_entry_ts,
                session_end_ts=session_end_ts,
            )
            if next_entry is None:
                break
            trade_seq += 1
            cur_entry_ts = next_entry
            continue

        # Conservative entry rule:
        # both legs must have an exact entry-minute close.
        pe_entry = pe_close_raw.loc[cur_entry_ts]
        ce_entry = ce_close_raw.loc[cur_entry_ts]
        if pd.isna(pe_entry) or pd.isna(ce_entry):
            skipped.append({
                "day": dy,
                "underlying": und,
                "expiry": expiry,
                "trade_seq": trade_seq,
                "entry_time": cur_entry_ts.strftime("%H:%M"),
                "atm_strike": atm,
                "pe_strike": pe_strike,
                "ce_strike": ce_strike,
                "reason": "No exact PE/CE close available at entry timestamp",
            })
            next_entry = _next_attempt_timestamp(
                current_trade_seq=trade_seq,
                base_ts=cur_entry_ts,
                session_end_ts=session_end_ts,
            )
            if next_entry is None:
                break
            trade_seq += 1
            cur_entry_ts = next_entry
            continue

        pe_entry_f = float(pe_entry)
        ce_entry_f = float(ce_entry)

        # Independent stop triggers for the two short legs.
        pe_trigger = compute_leg_exit_trigger(pe_entry_f)
        ce_trigger = compute_leg_exit_trigger(ce_entry_f)

        # Simulate exits independently.
        # This correctly keeps the surviving leg alive after the first leg exits.
        pe_exit_ts, pe_exit_reason, pe_exit_price = _simulate_leg_exit(
            entry_ts=cur_entry_ts,
            session_end_ts=session_end_ts,
            entry_price=pe_entry_f,
            close_series_ffill=pe_close,
            high_series_raw=pe_high,
            trigger_price=pe_trigger,
        )
        ce_exit_ts, ce_exit_reason, ce_exit_price = _simulate_leg_exit(
            entry_ts=cur_entry_ts,
            session_end_ts=session_end_ts,
            entry_price=ce_entry_f,
            close_series_ffill=ce_close,
            high_series_raw=ce_high,
            trigger_price=ce_trigger,
        )

        # Leg-wise and combined gross PnL.
        pe_pnl_gross = (pe_entry_f - pe_exit_price) * qty
        ce_pnl_gross = (ce_entry_f - ce_exit_price) * qty
        exit_pnl_gross = pe_pnl_gross + ce_pnl_gross

        # EOD mark-to-market if the original position had been held till session end.
        eod_pe = _series_value(pe_close, session_end_ts)
        eod_ce = _series_value(ce_close, session_end_ts)
        if pd.isna(eod_pe) or pd.isna(eod_ce):
            eod_pnl_if_held = float("nan")
        else:
            eod_pnl_if_held = (pe_entry_f - eod_pe) * qty + (ce_entry_f - eod_ce) * qty

        txn_charges = compute_trade_charges(
            entry_ce=ce_entry_f,
            entry_pe=pe_entry_f,
            exit_ce=ce_exit_price,
            exit_pe=pe_exit_price,
            qty=qty,
        )
        exit_pnl = exit_pnl_gross - txn_charges

        final_exit_ts = max(pe_exit_ts, ce_exit_ts)
        final_exit_reason = _classify_final_exit_reason(pe_exit_reason, ce_exit_reason)
        dte = int((expiry - dy).days)

        results.append(
            TradeRow(
                day=dy,
                underlying=und,
                trade_seq=trade_seq,
                expiry=expiry,
                days_to_expiry=dte,
                qty_units=qty,
                entry_time=cur_entry_ts.strftime("%H:%M"),
                final_exit_time=final_exit_ts.strftime("%H:%M"),
                final_exit_reason=final_exit_reason,
                entry_underlying=float(u_px),
                atm_strike=int(atm),
                otm_distance_steps=int(OTM_DISTANCE_STEPS),
                pe_strike=int(pe_strike),
                ce_strike=int(ce_strike),
                pe_symbol=pe_sym,
                ce_symbol=ce_sym,
                entry_pe=pe_entry_f,
                entry_ce=ce_entry_f,
                pe_exit_time=pe_exit_ts.strftime("%H:%M"),
                ce_exit_time=ce_exit_ts.strftime("%H:%M"),
                pe_exit_reason=pe_exit_reason,
                ce_exit_reason=ce_exit_reason,
                pe_exit_price=float(pe_exit_price),
                ce_exit_price=float(ce_exit_price),
                pe_exit_trigger=float(pe_trigger),
                ce_exit_trigger=float(ce_trigger),
                pe_minutes_held=_minutes_between(cur_entry_ts, pe_exit_ts),
                ce_minutes_held=_minutes_between(cur_entry_ts, ce_exit_ts),
                pe_pnl_gross=float(pe_pnl_gross),
                ce_pnl_gross=float(ce_pnl_gross),
                exit_pnl_gross=float(exit_pnl_gross),
                txn_charges=float(txn_charges),
                exit_pnl=float(exit_pnl),
                eod_pnl_if_held=float(eod_pnl_if_held) if pd.notna(eod_pnl_if_held) else float("nan"),
            )
        )

        # No reattempt is possible once the whole position effectively lives until session end.
        if final_exit_ts >= session_end_ts:
            break

        next_entry = _next_attempt_timestamp(
            current_trade_seq=trade_seq,
            base_ts=final_exit_ts,
            session_end_ts=session_end_ts,
        )
        if next_entry is None:
            break

        trade_seq += 1
        cur_entry_ts = next_entry

    return results, skipped


# =============================================================================
# PASS-2: PROCESS PICKLES AND SIMULATE TRADES
# =============================================================================
def process_pickles_generate_trades(
    pickle_paths: List[str],
    min_expiry_map: Dict[Tuple[str, date], date],
    actual_underlying_by_day: Dict[date, str],
    underlying_data: Dict[str, pd.DataFrame],
    window_start: date,
    window_end: date,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Pass 2 performs the actual simulation, but only on the nearest-expiry slice for each underlying/day.
    """
    all_trades: List[Dict[str, Any]] = []
    skipped_rows: List[Dict[str, Any]] = []

    # Prevent double-counting if the same (underlying, day, expiry) exists in multiple files.
    processed_day_keys: set[Tuple[str, date, date]] = set()

    for p in pickle_paths:
        try:
            df = pd.read_pickle(p)
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue

            needed_cols = [
                "date",
                "name",
                "type",
                "option_type",
                "strike",
                "expiry",
                "instrument",
                "high",
                "close",
            ]
            missing = [c for c in needed_cols if c not in df.columns]
            if missing:
                raise ValueError(f"Missing columns {missing} in {p}")

            d2 = df[df["type"].astype(str).str.upper().eq("OPTION")][needed_cols].copy()
            if d2.empty:
                continue

            d2["date"] = ensure_ist(d2["date"])
            d2["day"] = d2["date"].dt.date
            d2["underlying"] = d2["name"].astype(str).map(normalize_underlying)
            d2 = d2[d2["underlying"].isin(TRADEABLE)]
            if d2.empty:
                continue

            d2["expiry_date"] = pd.to_datetime(d2["expiry"], errors="coerce").dt.date
            d2["strike_num"] = pd.to_numeric(d2["strike"], errors="coerce")
            d2["strike_int"] = d2["strike_num"].round().astype("Int64")
            d2["option_type"] = d2["option_type"].astype(str).str.upper()
            d2 = d2.dropna(subset=["day", "underlying", "expiry_date", "strike_int", "close"])
            d2["strike_int"] = d2["strike_int"].astype(int)

            # Ignore rows where expiry is already over.
            d2 = d2[d2["expiry_date"] >= d2["day"]]
            if d2.empty:
                continue

            # Keep only 0-DTE and 1-DTE rows.
            d2["days_to_expiry"] = (pd.to_datetime(d2["expiry_date"]) - pd.to_datetime(d2["day"])).dt.days
            d2 = d2[d2["days_to_expiry"].isin([0, 1])]
            if d2.empty:
                continue

            # Apply lookback window.
            d2 = d2[(d2["day"] >= window_start) & (d2["day"] <= window_end)]
            if d2.empty:
                continue

            # Group at (underlying, day, expiry) granularity.
            for (und, dy, ex), g in d2.groupby(["underlying", "day", "expiry_date"], sort=False):
                key_ud = (und, dy)
                if key_ud not in min_expiry_map:
                    continue

                # Enforce "nearest expiry" rule for that underlying/day.
                if min_expiry_map[key_ud] != ex:
                    continue

                # Enforce ONE underlying per trading day.
                if actual_underlying_by_day.get(dy) != und:
                    continue

                day_key = (und, dy, ex)
                if day_key in processed_day_keys:
                    skipped_rows.append({
                        "day": dy,
                        "underlying": und,
                        "expiry": ex,
                        "reason": "Duplicate (underlying,day,expiry) encountered in multiple pickles; skipped to avoid double-count",
                    })
                    continue
                processed_day_keys.add(day_key)

                uday = underlying_data.get(und)
                if uday is None:
                    skipped_rows.append({"day": dy, "underlying": und, "expiry": ex, "reason": "No underlying series downloaded"})
                    continue

                uday = uday[uday["day"] == dy]
                if uday.empty:
                    skipped_rows.append({"day": dy, "underlying": und, "expiry": ex, "reason": "Underlying missing for day"})
                    continue

                trades, skips = simulate_day_multi_trades(
                    und=und,
                    dy=dy,
                    expiry=ex,
                    day_opt=g,
                    underlying_day=uday,
                )
                all_trades.extend([t.__dict__ for t in trades])
                skipped_rows.extend(skips)

            print(f"[PASS2 OK] {os.path.basename(p)} processed")

        except Exception as e:
            msg = f"[PASS2 WARN] {os.path.basename(p)} failed: {e}"
            if FAIL_ON_PICKLE_ERROR:
                raise RuntimeError(msg) from e
            print(msg)

    all_df = pd.DataFrame(all_trades)
    if not all_df.empty:
        all_df = all_df.sort_values(["day", "underlying", "trade_seq"]).reset_index(drop=True)

    skip_df = pd.DataFrame(skipped_rows)
    if not skip_df.empty:
        if "day" not in skip_df.columns:
            skip_df["day"] = pd.NaT
        if "underlying" not in skip_df.columns:
            skip_df["underlying"] = pd.NA
        skip_df = skip_df.sort_values(["day", "underlying"], na_position="last").reset_index(drop=True)

    return all_df, skip_df


# =============================================================================
# ACTUAL TRADES: ONE UNDERLYING PER DAY, KEEP ALL REATTEMPTS FOR THAT UNDERLYING
# =============================================================================
def pick_actual_underlying_by_day(min_expiry_map: Dict[Tuple[str, date], date]) -> Dict[date, str]:
    """
    Pick the ONE underlying to be traded for each day.

    Rule:
    - consider only 0-DTE and 1-DTE candidates
    - choose the underlying whose expiry is earliest on that day
    - if tied, prefer NIFTY over SENSEX
    """
    by_day: Dict[date, List[Tuple[date, str]]] = {}

    for (und, dy), ex in min_expiry_map.items():
        if und not in TRADEABLE:
            continue

        dte = int((ex - dy).days)
        if dte not in (0, 1):
            continue

        by_day.setdefault(dy, []).append((ex, und))

    out: Dict[date, str] = {}
    for dy, lst in by_day.items():
        lst_sorted = sorted(lst, key=lambda t: (t[0], 0 if t[1] == "NIFTY" else 1))
        out[dy] = lst_sorted[0][1]

    return out


def build_actual_trades_df(all_trades_df: pd.DataFrame, min_expiry_map: Dict[Tuple[str, date], date]) -> pd.DataFrame:
    """
    Reduce the full backtest to one chosen underlying per day while keeping all reattempts for that chosen underlying.
    """
    if all_trades_df.empty:
        return pd.DataFrame()

    actual_underlying = pick_actual_underlying_by_day(min_expiry_map)
    m = all_trades_df.copy()
    m["actual_underlying_for_day"] = m["day"].map(actual_underlying)
    m = m[m["actual_underlying_for_day"].notna()]
    m = m[m["underlying"] == m["actual_underlying_for_day"]]
    m = m.drop(columns=["actual_underlying_for_day"])
    m = m.sort_values(["day", "trade_seq"]).reset_index(drop=True)
    m["is_exit_pnl_positive"] = (m["exit_pnl"] > 0).astype(int)
    return m


# =============================================================================
# EXCEL OUTPUT
# =============================================================================
def _autosize_columns_safe(ws) -> None:
    """
    Auto-size worksheet columns defensively.
    Never let column-sizing failure kill the run.
    """
    try:
        max_col = ws.max_column or 0
        if max_col <= 0:
            return
        for col_idx in range(1, max_col + 1):
            col_letter = ws.cell(row=1, column=col_idx).column_letter
            max_len = 0
            for row_idx in range(1, min(ws.max_row or 1, 2000) + 1):
                v = ws.cell(row=row_idx, column=col_idx).value
                if v is None:
                    continue
                max_len = max(max_len, len(str(v)))
            ws.column_dimensions[col_letter].width = min(60, max(10, max_len + 2))
    except Exception:
        return


def write_excel(all_trades_df: pd.DataFrame, actual_trades_df: pd.DataFrame, skipped_df: pd.DataFrame) -> None:
    """
    Write all outputs to Excel:
    - all_trades_backtested
    - actual_trades
    - monthwise_summary
    - instrument_summary
    - skipped
    """
    out_dir = os.path.dirname(os.path.abspath(OUTPUT_XLSX))
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    instrument_summary = pd.DataFrame()
    monthwise_summary = pd.DataFrame()

    if not all_trades_df.empty:
        inst = all_trades_df.copy()
        inst["any_leg_sl"] = (
            inst["pe_exit_reason"].astype(str).eq("LEG_SL")
            | inst["ce_exit_reason"].astype(str).eq("LEG_SL")
        )
        inst["both_legs_sl"] = (
            inst["pe_exit_reason"].astype(str).eq("LEG_SL")
            & inst["ce_exit_reason"].astype(str).eq("LEG_SL")
        )
        instrument_summary = (
            inst.groupby("underlying", as_index=False)
            .agg(
                trades=("exit_pnl", "count"),
                total_exit_pnl=("exit_pnl", "sum"),
                avg_exit_pnl=("exit_pnl", "mean"),
                win_rate_exit_pct=("exit_pnl", lambda s: 100.0 * (s > 0).mean()),
                any_leg_sl_rate_pct=("any_leg_sl", lambda s: 100.0 * s.mean()),
                both_legs_sl_rate_pct=("both_legs_sl", lambda s: 100.0 * s.mean()),
            )
            .sort_values("total_exit_pnl", ascending=False)
            .reset_index(drop=True)
        )

    if not actual_trades_df.empty:
        tmp = actual_trades_df.copy()
        tmp["month"] = pd.to_datetime(tmp["day"]).dt.to_period("M").astype(str)
        monthwise_summary = (
            tmp.groupby("month", as_index=False)
            .agg(
                trades=("exit_pnl", "count"),
                total_exit_pnl=("exit_pnl", "sum"),
                avg_exit_pnl=("exit_pnl", "mean"),
                winning_trades=("is_exit_pnl_positive", "sum"),
            )
        )
        monthwise_summary["losing_trades"] = monthwise_summary["trades"] - monthwise_summary["winning_trades"]
        monthwise_summary["win_rate_pct"] = (100.0 * monthwise_summary["winning_trades"] / monthwise_summary["trades"]).round(2)

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as xw:
        all_trades_df.to_excel(xw, sheet_name="all_trades_backtested", index=False)
        actual_trades_df.to_excel(xw, sheet_name="actual_trades", index=False)
        monthwise_summary.to_excel(xw, sheet_name="monthwise_summary", index=False)
        instrument_summary.to_excel(xw, sheet_name="instrument_summary", index=False)
        skipped_df.to_excel(xw, sheet_name="skipped", index=False)

        wb = xw.book
        for ws in wb.worksheets:
            ws.freeze_panes = "A2"
            _autosize_columns_safe(ws)

    print(f"[DONE] Excel written: {OUTPUT_XLSX}")


# =============================================================================
# MAIN
# =============================================================================
def main():
    """Entry point."""
    validate_user_config()

    paths = sorted(glob.glob(os.path.join(PICKLES_DIR, "*.pkl")) + glob.glob(os.path.join(PICKLES_DIR, "*.pickle")))
    if not paths:
        raise FileNotFoundError(f"No .pkl/.pickle files found in: {PICKLES_DIR}")

    print(f"[INFO] Pickles found: {len(paths)}")

    end_day, min_expiry_map, min_day_seen = scan_pickles_pass1(paths)
    window_start = compute_window_start(end_day, LOOKBACK_MONTHS)

    print(f"[INFO] Data day-range seen: {min_day_seen} -> {end_day}")
    print(f"[INFO] Window: {window_start} -> {end_day}")
    print(f"[INFO] Entry time: {ENTRY_TIME_IST}")
    print(f"[INFO] OTM distance steps: {OTM_DISTANCE_STEPS}")
    print(f"[INFO] Leg premium rise exit pct: {LEG_PREMIUM_RISE_EXIT_PCT}")
    print(f"[INFO] Max reattempts: {MAX_REATTEMPTS} | Total attempt slots/day: {1 + MAX_REATTEMPTS} | Re-entry delay min: {REENTRY_DELAY_MINUTES}")
    print(f"[INFO] Tradeables: {sorted(TRADEABLE)}")
    print(f"[INFO] Include transaction costs: {INCLUDE_TRANSACTION_COSTS}")
    print(f"[INFO] Output: {OUTPUT_XLSX}")

    print("[STEP] Initializing Kite ...")
    kite = oUtils.intialize_kite_api()
    print("[OK] Kite ready.")

    underlying_data = download_underlyings(kite, window_start, end_day)
    actual_underlying_by_day = pick_actual_underlying_by_day(min_expiry_map)

    all_trades_df, skipped_df = process_pickles_generate_trades(
        paths,
        min_expiry_map,
        actual_underlying_by_day,
        underlying_data,
        window_start,
        end_day,
    )

    actual_trades_df = build_actual_trades_df(all_trades_df, min_expiry_map)
    write_excel(all_trades_df, actual_trades_df, skipped_df)

    if not actual_trades_df.empty:
        print(actual_trades_df.groupby("underlying")[["exit_pnl"]].describe())
    else:
        print("[WARN] No completed trades. Check 'skipped' sheet for reasons.")


if __name__ == "__main__":
    main()
