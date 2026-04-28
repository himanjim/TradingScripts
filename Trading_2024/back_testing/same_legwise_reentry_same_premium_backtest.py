"""
Leg-wise OTM/ATM short option-pair backtest with independent re-entry.

Strategy implemented
--------------------
1. At ENTRY_TIME_IST, compute ATM from the underlying close available at or before entry time.
2. Sell one PE and one CE at configurable strike distance from ATM:
       OTM_DISTANCE_STEPS = 0  -> true ATM short straddle
       OTM_DISTANCE_STEPS > 0  -> PE below ATM and CE above ATM, i.e. OTM short strangle-style pair
3. Each leg is monitored independently:
       - if that leg's premium rises by LEG_PREMIUM_RISE_EXIT_PCT from its sold price, buy back only that leg
       - after exit, wait until the same option premium returns to the earlier sold premium
       - when it returns, sell that same option again
       - repeat until MAX_RETRIES_PER_LEG is exhausted, independently for PE and CE
4. Open legs, if any, are closed at SESSION_END_IST.
5. Both legs are monitored simultaneously through a minute-by-minute state machine.

Backtest fill assumptions
-------------------------
- Initial entry uses exact close at ENTRY_TIME_IST. If exact close is missing for CE or PE, the day is skipped.
- Stop-loss exit uses candle HIGH because the short option is harmed by premium rising.
- Stop-loss fill is assumed at the trigger price, not at candle high.
- Re-entry uses the candle range after the configured waiting period. In strict mode,
  the earlier sold premium must lie inside that candle's [LOW, HIGH] range.
- Re-entry fill is assumed at the earlier sold price only if the level is touched
  after the waiting period. A touch that happens during the waiting period is ignored.
- If LOW and HIGH both cross re-entry/stop levels in the same candle after re-entry, the code does NOT stop in the
  same candle. Stop monitoring starts from the next candle. This avoids imposing an unknowable intraminute sequence.

Expected option pickle columns
------------------------------
The script follows the same broad data assumptions as the user's reference code:
    date, name, type, option_type, strike, expiry, high, low, close, instrument/tradingsymbol

It also downloads underlying 1-minute data through Trading_2024.OptionTradeUtils.intialize_kite_api(), as in the
reference code, because ATM must be computed from underlying price rather than option data.
"""

from __future__ import annotations

import glob
import os
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

import Trading_2024.OptionTradeUtils as oUtils

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

try:
    import pytz  # type: ignore
except Exception:  # pragma: no cover
    pytz = None  # type: ignore

try:
    from dateutil.relativedelta import relativedelta  # type: ignore
except Exception:  # pragma: no cover
    relativedelta = None  # type: ignore


def parse_bool_env(name: str, default: bool) -> bool:
    """
    Parse common boolean environment variable values.

    Accepted true values: 1, true, yes, y, on
    Accepted false values: 0, false, no, n, off
    Any blank/unknown value falls back to the supplied default.
    """
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return bool(default)
    val = str(raw).strip().lower()
    if val in {"1", "true", "yes", "y", "on"}:
        return True
    if val in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


# =============================================================================
# USER CONFIGURATION
# =============================================================================

# Folder containing option historical data pickles.
PICKLES_DIR = r"G:\My Drive\Trading\Historical_Options_Data"

# Entry and backtest window.
ENTRY_TIME_IST = os.getenv("ENTRY_TIME_IST", "09:25")          # HH:MM
LOOKBACK_MONTHS = int(os.getenv("LOOKBACK_MONTHS", "10"))

# Strike distance from ATM in strike steps.
# NIFTY step = 50, SENSEX step = 100.
# 0 => true ATM short straddle.
# 1 => NIFTY PE = ATM - 50 and CE = ATM + 50; SENSEX PE = ATM - 100 and CE = ATM + 100.
OTM_DISTANCE_STEPS = int(os.getenv("OTM_DISTANCE_STEPS", "0"))

# Independent leg stop.
# Example: 20 means a leg sold at 100 exits when premium reaches 120.
LEG_PREMIUM_RISE_EXIT_PCT = float(os.getenv("LEG_PREMIUM_RISE_EXIT_PCT", "8"))

# Re-entry retry count PER LEG, not per straddle.
# Example: 2 means each leg can be re-sold up to 2 times after stop exit.
MAX_RETRIES_PER_LEG = int(os.getenv("MAX_RETRIES_PER_LEG", "5"))

# Minimum waiting time after a leg stop before that same leg is allowed to re-enter.
# Keep 0 if you want immediate next-candle eligibility when premium returns.
MIN_REENTRY_GAP_MINUTES = int(os.getenv("MIN_REENTRY_GAP_MINUTES", "14"))

# Transaction cost toggle.
INCLUDE_TRANSACTION_COSTS = parse_bool_env("INCLUDE_TRANSACTION_COSTS", True)

# Re-entry fill quality control.
# True  => re-entry at the earlier sold premium is allowed only if that premium lies inside
#          the candle range [LOW, HIGH]. This is stricter and avoids optimistic fills when
#          the whole candle is already below the earlier premium.
# False => re-entry is allowed as soon as LOW <= earlier sold premium, matching a looser
#          "premium came back below my level" interpretation.
REENTRY_REQUIRE_RANGE_TOUCH = parse_bool_env("REENTRY_REQUIRE_RANGE_TOUCH", True)


# Error handling.
# 0 => skip bad pickle and continue. 1 => fail immediately.
FAIL_ON_PICKLE_ERROR = parse_bool_env("FAIL_ON_PICKLE_ERROR", False)

# Output file. Default goes to Downloads.
OUTPUT_XLSX = os.getenv("OUTPUT_XLSX", "")

# Market session.
SESSION_START_IST = dtime(9, 15)
SESSION_END_IST = dtime(15, 30)

# Trade universe and lot quantities.
QTY_UNITS = {"NIFTY": 325, "SENSEX": 100}
TRADEABLE = set(QTY_UNITS.keys())
STRIKE_STEP = {"NIFTY": 50, "SENSEX": 100}

# Underlying symbols to download through Kite.
UNDERLYING_KITE = {
    "NIFTY": {"exchange": "NSE", "tradingsymbol": "NIFTY 50"},
    "SENSEX": {"exchange": "BSE", "tradingsymbol": "SENSEX"},
}

# Kite historical throttling.
MAX_DAYS_PER_CHUNK = 25
MAX_ATTEMPTS = 5
SLEEP_BETWEEN_CALLS_SEC = 0.20


# =============================================================================
# TRANSACTION COST SETTINGS
# =============================================================================
# Kept aligned with the user's earlier style. Update centrally if broker/exchange charges change.
BROKERAGE_PER_ORDER = 20.0
STT_SELL_PCT = 0.001
EXCHANGE_TXN_PCT = 0.0003553
SEBI_PER_CRORE = 10.0
STAMP_BUY_PCT = 0.00003
IPFT_PER_CRORE = 0.010
GST_PCT = 0.18


# =============================================================================
# BASIC HELPERS
# =============================================================================

def ist_tz():
    """Return Asia/Kolkata timezone object using zoneinfo if available, otherwise pytz."""
    if ZoneInfo is not None:
        return ZoneInfo("Asia/Kolkata")
    if pytz is not None:
        return pytz.timezone("Asia/Kolkata")
    return "Asia/Kolkata"


def ensure_ist(series_or_scalar) -> Any:
    """
    Convert pandas datetime series/scalar to IST.
    - naive timestamps are localized to IST
    - aware timestamps are converted to IST
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


def parse_hhmm(s: str) -> dtime:
    """Parse HH:MM string into datetime.time."""
    hh, mm = s.strip().split(":")
    return dtime(int(hh), int(mm))


ENTRY_TIME = parse_hhmm(ENTRY_TIME_IST)


def safe_fname_part(s: str) -> str:
    """Make a string safe for filenames."""
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in str(s))


def downloads_folder() -> str:
    """Return user's Downloads folder if present, else home folder."""
    d = Path.home() / "Downloads"
    return str(d if d.exists() else Path.home())


def default_output_path() -> str:
    """Build a descriptive default Excel output path."""
    return os.path.join(
        downloads_folder(),
        "legwise_reentry_same_premium"
        f"_{safe_fname_part(ENTRY_TIME_IST)}"
        f"_OTM{OTM_DISTANCE_STEPS}"
        f"_SL{safe_fname_part(str(LEG_PREMIUM_RISE_EXIT_PCT))}"
        f"_R{MAX_RETRIES_PER_LEG}"
        f"_GAP{MIN_REENTRY_GAP_MINUTES}"
        f"_TOUCH{int(REENTRY_REQUIRE_RANGE_TOUCH)}.xlsx",
    )


if not OUTPUT_XLSX:
    OUTPUT_XLSX = default_output_path()


def validate_config() -> None:
    """Fail early for invalid configuration."""
    if ENTRY_TIME < SESSION_START_IST or ENTRY_TIME >= SESSION_END_IST:
        raise ValueError(
            f"ENTRY_TIME_IST must be within [{SESSION_START_IST.strftime('%H:%M')}, "
            f"{SESSION_END_IST.strftime('%H:%M')}). Current: {ENTRY_TIME_IST}"
        )
    if LOOKBACK_MONTHS < 0:
        raise ValueError("LOOKBACK_MONTHS must be >= 0")
    if OTM_DISTANCE_STEPS < 0:
        raise ValueError("OTM_DISTANCE_STEPS must be >= 0")
    if LEG_PREMIUM_RISE_EXIT_PCT <= 0:
        raise ValueError("LEG_PREMIUM_RISE_EXIT_PCT must be > 0")
    if MAX_RETRIES_PER_LEG < 0:
        raise ValueError("MAX_RETRIES_PER_LEG must be >= 0")
    if MIN_REENTRY_GAP_MINUTES < 0:
        raise ValueError("MIN_REENTRY_GAP_MINUTES must be >= 0")


def normalize_underlying(name: str) -> Optional[str]:
    """
    Normalize names found in option pickle to internal underlying names.

    Important:
    - Do not classify FINNIFTY/MIDCPNIFTY/BANKNIFTY as NIFTY.
    - BANKNIFTY is recognized but excluded later because TRADEABLE currently contains only NIFTY/SENSEX.
    """
    if not isinstance(name, str):
        return None

    u = " ".join(name.upper().replace("_", " ").replace("-", " ").split())

    if "SENSEX" in u:
        return "SENSEX"

    if "BANKNIFTY" in u or "NIFTY BANK" in u:
        return "BANKNIFTY"

    # Avoid accidental FINNIFTY/MIDCPNIFTY matching.
    if "FINNIFTY" in u or "MIDCPNIFTY" in u or "NIFTY NEXT" in u or "NIFTY NEXT 50" in u:
        return None

    if u in {"NIFTY", "NIFTY 50"} or u.startswith("NIFTY "):
        return "NIFTY"

    return None


def round_to_step(x: float, step: int) -> int:
    """Round a price to nearest permitted strike step."""
    return int(round(float(x) / step) * step)


def compute_otm_strikes(atm: int, step: int, distance_steps: int) -> Tuple[int, int]:
    """
    Compute PE and CE strikes from ATM.
    distance_steps=0 gives PE=CE=ATM.
    distance_steps>0 gives OTM PE below ATM and OTM CE above ATM.
    """
    distance_points = step * int(distance_steps)
    return int(atm - distance_points), int(atm + distance_points)


def build_minute_index(day_d: date, start_t: dtime, end_t: dtime) -> pd.DatetimeIndex:
    """Build complete 1-minute IST index for a trading day."""
    tz = ist_tz()
    start = pd.Timestamp(datetime.combine(day_d, start_t), tz=tz)
    end = pd.Timestamp(datetime.combine(day_d, end_t), tz=tz)
    return pd.date_range(start=start, end=end, freq="1min")


def compute_window_start(end_day: date, months: int) -> date:
    """Compute start date for lookback window."""
    if relativedelta is not None:
        return (pd.Timestamp(end_day) - relativedelta(months=months)).date()
    return (pd.Timestamp(end_day) - pd.Timedelta(days=30 * months)).date()


def asof_close(df: pd.DataFrame, ts: pd.Timestamp) -> float:
    """Return underlying close at or before timestamp ts."""
    if df.empty:
        return float("nan")
    d = df[["date", "close"]].dropna().copy()
    d["date"] = ensure_ist(d["date"])
    d = d.sort_values("date").drop_duplicates("date", keep="last").set_index("date")
    loc = d.index.get_indexer([ts], method="pad")
    if loc[0] == -1:
        return float("nan")
    return float(d.iloc[loc[0]]["close"])


def series_value(s: pd.Series, ts: pd.Timestamp) -> float:
    """Read float from a Series at timestamp. Return NaN if unavailable."""
    if ts not in s.index:
        return float("nan")
    v = s.loc[ts]
    return float(v) if pd.notna(v) else float("nan")


def reentry_level_reached(
    *,
    low_raw: pd.Series,
    high_raw: pd.Series,
    close_ffill: pd.Series,
    ts: pd.Timestamp,
    target_price: float,
) -> bool:
    """
    Return True if the earlier sold premium can be treated as reached for re-entry.

    With minute OHLC data, a re-entry at an exact level is safest when the level lies
    inside the candle range: LOW <= target <= HIGH. If high/low is unavailable, the
    function falls back to close-based logic.

    This prevents a common optimistic backtest error:
    - target re-entry premium = 100
    - candle high = 90, low = 70
    - old logic using only LOW <= 100 would sell again at 100, although the candle never traded at 100
    """
    low_v = series_value(low_raw, ts)
    high_v = series_value(high_raw, ts)
    close_v = series_value(close_ffill, ts)

    if REENTRY_REQUIRE_RANGE_TOUCH:
        if pd.notna(low_v) and pd.notna(high_v):
            return float(low_v) <= float(target_price) <= float(high_v)
        if pd.notna(close_v):
            return float(close_v) <= float(target_price)
        return False

    # Looser mode: consider the level reached once premium is at/below the target.
    if pd.notna(low_v):
        return float(low_v) <= float(target_price)
    if pd.notna(close_v):
        return float(close_v) <= float(target_price)
    return False



def first_reentry_check_timestamp(stop_ts: pd.Timestamp) -> pd.Timestamp:
    """
    Return the first candle timestamp at which re-entry may be checked after a stop.

    This fixes an easy off-by-one confusion:
    - If a stop occurs at 10:00 and MIN_REENTRY_GAP_MINUTES = 0,
      re-entry can first be checked at 10:01 because the 10:00 candle sequence is unknowable.
    - If a stop occurs at 10:00 and MIN_REENTRY_GAP_MINUTES = 15,
      re-entry can first be checked at 10:15, not 10:16.

    The function only controls when checking resumes. It does not carry forward a touch
    that occurred during the waiting period. For a same-premium re-entry, the target
    premium must be touched again at or after this returned timestamp.
    """
    minutes_to_wait = max(1, int(MIN_REENTRY_GAP_MINUTES))
    return stop_ts + pd.Timedelta(minutes=minutes_to_wait)


def minutes_between(a: pd.Timestamp, b: pd.Timestamp) -> int:
    """Integer minute difference between two timestamps."""
    return int((b - a).total_seconds() // 60)


# =============================================================================
# KITE HISTORICAL DOWNLOAD HELPERS
# =============================================================================

def iter_chunks_by_date(from_dt: datetime, to_dt: datetime, days_per_chunk: int) -> Iterable[Tuple[datetime, datetime]]:
    """Split a long historical download request into date chunks."""
    if from_dt > to_dt:
        raise ValueError("from_dt must be <= to_dt")
    cur = from_dt.date()
    end_d = to_dt.date()
    while cur <= end_d:
        chunk_end = min(cur + timedelta(days=days_per_chunk - 1), end_d)
        c_from = from_dt if cur == from_dt.date() else datetime.combine(cur, SESSION_START_IST)
        c_to = to_dt if chunk_end == end_d else datetime.combine(chunk_end, SESSION_END_IST)
        yield c_from, c_to
        cur = chunk_end + timedelta(days=1)


def kite_instruments_cached(kite, exchange: str, cache: Dict[str, List[Dict]]) -> List[Dict]:
    """Cache Kite instruments by exchange."""
    ex = exchange.upper().strip()
    if ex not in cache:
        print(f"[STEP] Loading Kite instruments for {ex}...")
        cache[ex] = kite.instruments(ex)
        print(f"[OK] {ex} instruments loaded: {len(cache[ex])}")
    return cache[ex]


def get_instrument_token(kite, exchange: str, tradingsymbol: str, cache: Dict[str, List[Dict]]) -> int:
    """Find Kite instrument token by exchange and tradingsymbol."""
    ex = exchange.upper().strip()
    wanted = tradingsymbol.strip().upper()
    for row in kite_instruments_cached(kite, ex, cache):
        if str(row.get("tradingsymbol", "")).upper() == wanted:
            return int(row["instrument_token"])
    raise ValueError(f"Instrument not found on {ex}: {tradingsymbol}")


def fetch_history_minute(kite, instrument_token: int, from_dt: datetime, to_dt: datetime, label: str) -> List[Dict]:
    """Download 1-minute historical candles from Kite with retries."""
    rows_all: List[Dict] = []
    chunks = list(iter_chunks_by_date(from_dt, to_dt, MAX_DAYS_PER_CHUNK))
    print(f"[INFO] Fetching {label}, token={instrument_token}, chunks={len(chunks)}, {from_dt} -> {to_dt}")

    for i, (c_from, c_to) in enumerate(chunks, start=1):
        last_err: Optional[Exception] = None
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                rows = kite.historical_data(
                    instrument_token=instrument_token,
                    from_date=c_from,
                    to_date=c_to,
                    interval="minute",
                    continuous=False,
                    oi=False,
                )
                rows_all.extend(rows)
                last_err = None
                break
            except Exception as e:  # pragma: no cover - broker/API runtime path
                last_err = e
                print(f"[WARN] {label} chunk {i}/{len(chunks)} attempt {attempt} failed: {e}")
                time.sleep(min(8.0, 1.5 * attempt))

        if last_err is not None:
            print(f"[ERROR] {label} chunk {i}/{len(chunks)} failed finally: {last_err}")

        time.sleep(SLEEP_BETWEEN_CALLS_SEC)

    return rows_all


def rows_to_df(rows: List[Dict]) -> pd.DataFrame:
    """Convert Kite historical rows to clean minute DataFrame."""
    if not rows:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume", "day"])
    df = pd.DataFrame(rows)
    df["date"] = ensure_ist(df["date"])
    df = df.drop_duplicates(subset=["date"], keep="last").sort_values("date").reset_index(drop=True)
    df["day"] = df["date"].dt.date
    return df


def download_underlyings(kite, day_start: date, day_end: date) -> Dict[str, pd.DataFrame]:
    """Download underlying minute data for all configured underlyings."""
    cache: Dict[str, List[Dict]] = {}
    from_dt = datetime.combine(day_start, SESSION_START_IST)
    to_dt = datetime.combine(day_end, SESSION_END_IST)

    out: Dict[str, pd.DataFrame] = {}
    for und, meta in UNDERLYING_KITE.items():
        token = get_instrument_token(kite, meta["exchange"], meta["tradingsymbol"], cache)
        rows = fetch_history_minute(kite, token, from_dt, to_dt, f"{meta['exchange']}:{meta['tradingsymbol']}")
        df = rows_to_df(rows)
        out[und] = df
        print(f"[UNDERLYING OK] {und}: candles={len(df)}, days={df['day'].nunique() if not df.empty else 0}")
    return out


# =============================================================================
# DATA STANDARDIZATION
# =============================================================================

def standardize_option_df(df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    """
    Return a standardized option DataFrame with columns used by the strategy engine.

    Supports either 'instrument' or 'tradingsymbol' as option symbol column.
    Requires 'low' for best re-entry simulation; if low is missing, falls back to close.
    """
    required_base = ["date", "name", "type", "option_type", "strike", "expiry", "high", "close"]
    missing = [c for c in required_base if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns {missing} in {source_file}")

    if "instrument" in df.columns:
        symbol_col = "instrument"
    elif "tradingsymbol" in df.columns:
        symbol_col = "tradingsymbol"
    else:
        raise ValueError(f"Missing option symbol column: expected 'instrument' or 'tradingsymbol' in {source_file}")

    cols = required_base + [symbol_col]
    if "low" in df.columns:
        cols.append("low")

    d = df[df["type"].astype(str).str.upper().eq("OPTION")][cols].copy()
    if d.empty:
        return d

    d = d.rename(columns={symbol_col: "instrument"})
    if "low" not in d.columns:
        d["low"] = d["close"]

    d["date"] = ensure_ist(d["date"])
    d["day"] = d["date"].dt.date
    d["underlying"] = d["name"].astype(str).map(normalize_underlying)
    d = d[d["underlying"].isin(TRADEABLE)]

    d["expiry_date"] = pd.to_datetime(d["expiry"], errors="coerce").dt.date
    d["strike_num"] = pd.to_numeric(d["strike"], errors="coerce")
    d["strike_int"] = d["strike_num"].round().astype("Int64")
    d["option_type"] = d["option_type"].astype(str).str.upper().str.strip()
    d["option_type"] = d["option_type"].replace(
        {
            "CALL": "CE",
            "C": "CE",
            "PUT": "PE",
            "P": "PE",
        }
    )
    d = d[d["option_type"].isin(["CE", "PE"])]


    for price_col in ["high", "low", "close"]:
        d[price_col] = pd.to_numeric(d[price_col], errors="coerce")

    d = d.dropna(subset=["day", "underlying", "expiry_date", "strike_int", "option_type", "instrument", "close"])
    d["strike_int"] = d["strike_int"].astype(int)

    # Keep only valid, non-expired rows.
    d = d[d["expiry_date"] >= d["day"]]
    return d


# =============================================================================
# PASS 1: NEAREST EXPIRY SCAN
# =============================================================================

def scan_pickles_pass1(pickle_paths: List[str]) -> Tuple[date, date, Dict[Tuple[str, date], date]]:
    """
    Determine:
    - min and max data day seen
    - nearest expiry for each (underlying, day)

    The nearest-expiry rule prevents accidental trading of a farther expiry when 0/1-DTE also exists.
    """
    min_day_seen: Optional[date] = None
    max_day_seen: Optional[date] = None
    min_expiry_map: Dict[Tuple[str, date], date] = {}

    for p in pickle_paths:
        try:
            raw = pd.read_pickle(p)
            if not isinstance(raw, pd.DataFrame) or raw.empty:
                continue

            d = standardize_option_df(raw, p)
            if d.empty:
                continue

            file_min = d["day"].min()
            file_max = d["day"].max()
            min_day_seen = file_min if min_day_seen is None or file_min < min_day_seen else min_day_seen
            max_day_seen = file_max if max_day_seen is None or file_max > max_day_seen else max_day_seen

            grp = d.groupby(["underlying", "day"], sort=False)["expiry_date"].min()
            for (und, dy), ex in grp.items():
                key = (und, dy)
                if key not in min_expiry_map or ex < min_expiry_map[key]:
                    min_expiry_map[key] = ex

            print(f"[PASS1 OK] {os.path.basename(p)} days={d['day'].nunique()}")

        except Exception as e:
            msg = f"[PASS1 WARN] {os.path.basename(p)} failed: {e}"
            if FAIL_ON_PICKLE_ERROR:
                raise RuntimeError(msg) from e
            print(msg)

    if min_day_seen is None or max_day_seen is None:
        raise RuntimeError("No usable option data found in pickles.")

    return min_day_seen, max_day_seen, min_expiry_map


def pick_actual_underlying_by_day(min_expiry_map: Dict[Tuple[str, date], date]) -> Dict[date, str]:
    """
    Pick only one underlying per day for 'actual' trading.

    Rule:
    - only 0-DTE and 1-DTE candidates are eligible
    - choose earliest expiry
    - if NIFTY and SENSEX tie, prefer NIFTY
    """
    by_day: Dict[date, List[Tuple[date, str]]] = {}
    for (und, dy), ex in min_expiry_map.items():
        if und not in TRADEABLE:
            continue
        dte = int((ex - dy).days)
        if dte in (0, 1):
            by_day.setdefault(dy, []).append((ex, und))

    out: Dict[date, str] = {}
    for dy, candidates in by_day.items():
        candidates_sorted = sorted(candidates, key=lambda t: (t[0], 0 if t[1] == "NIFTY" else 1))
        out[dy] = candidates_sorted[0][1]
    return out


# =============================================================================
# TRANSACTION COSTS
# =============================================================================

def compute_leg_roundtrip_charges(entry_sell_price: float, exit_buy_price: float, qty: int) -> float:
    """
    Estimate charges for one short-option round trip:
    - SELL option at entry
    - BUY option at exit
    """
    if not INCLUDE_TRANSACTION_COSTS:
        return 0.0

    sell_turnover = float(entry_sell_price) * qty
    buy_turnover = float(exit_buy_price) * qty
    total_turnover = sell_turnover + buy_turnover

    brokerage = BROKERAGE_PER_ORDER * 2.0
    stt = sell_turnover * STT_SELL_PCT
    txn_charges = total_turnover * EXCHANGE_TXN_PCT
    sebi = total_turnover * SEBI_PER_CRORE / 1_00_00_000
    stamp = buy_turnover * STAMP_BUY_PCT
    ipft = total_turnover * IPFT_PER_CRORE / 1_00_00_000
    gst = (brokerage + txn_charges + sebi) * GST_PCT

    return round(brokerage + stt + txn_charges + sebi + stamp + ipft + gst, 2)


# =============================================================================
# LEG STATE MACHINE
# =============================================================================

@dataclass
class LegCycle:
    """One sell-buy cycle for one option leg."""
    day: date
    underlying: str
    expiry: date
    days_to_expiry: int
    leg: str
    symbol: str
    strike: int
    cycle_no: int
    retry_no: int
    entry_time: str
    exit_time: str
    entry_price: float
    exit_price: float
    stop_trigger: float
    exit_reason: str
    minutes_held: int
    qty_units: int
    pnl_gross: float
    txn_charges: float
    pnl_net: float


@dataclass
class StraddleSummary:
    """One row per day/underlying option pair."""
    day: date
    underlying: str
    expiry: date
    days_to_expiry: int
    qty_units: int
    entry_time: str
    entry_underlying: float
    atm_strike: int
    otm_distance_steps: int
    pe_strike: int
    ce_strike: int
    pe_symbol: str
    ce_symbol: str
    pe_initial_entry: float
    ce_initial_entry: float
    pe_stop_trigger: float
    ce_stop_trigger: float
    pe_cycles: int
    ce_cycles: int
    pe_retries_used: int
    ce_retries_used: int
    pe_stop_count: int
    ce_stop_count: int
    pe_net_pnl: float
    ce_net_pnl: float
    total_gross_pnl: float
    total_txn_charges: float
    total_net_pnl: float
    final_exit_time: str
    any_open_leg_till_eod: int
    eod_pnl_if_held_without_stops: float


def build_leg_price_series(
    day_opt: pd.DataFrame,
    idx_all: pd.DatetimeIndex,
    strike: int,
    option_type: str,
    symbol: str,
    price_col: str,
    ffill: bool,
) -> pd.Series:
    """
    Build a minute-aligned price series for one option leg and one price column.

    Duplicate rows within the same minute are aggregated defensively:
    - high  -> maximum high
    - low   -> minimum low
    - close -> last close by timestamp order

    This is safer than blindly keeping the last row because stop and re-entry decisions
    depend on the intraminute extremes.
    """
    sub = day_opt[
        (day_opt["strike_int"] == strike)
        & (day_opt["option_type"] == option_type)
        & (day_opt["instrument"].astype(str) == symbol)
    ][["date", price_col]].dropna()

    if sub.empty:
        return pd.Series(index=idx_all, dtype="float64")

    sub = sub.copy()
    sub["date"] = ensure_ist(sub["date"])
    sub[price_col] = pd.to_numeric(sub[price_col], errors="coerce")
    sub = sub.dropna(subset=["date", price_col]).sort_values("date")

    if price_col == "high":
        agg = sub.groupby("date", sort=True)[price_col].max()
    elif price_col == "low":
        agg = sub.groupby("date", sort=True)[price_col].min()
    else:
        agg = sub.groupby("date", sort=True)[price_col].last()

    s = agg.astype(float).reindex(idx_all)
    return s.ffill() if ffill else s


def pick_symbol(day_opt: pd.DataFrame, strike: int, option_type: str) -> Optional[str]:
    """Pick a symbol for strike/type from nearest-expiry day slice."""
    sub = day_opt[(day_opt["strike_int"] == strike) & (day_opt["option_type"] == option_type)]
    if sub.empty:
        return None
    syms = sorted(sub["instrument"].astype(str).unique().tolist())
    return syms[0] if syms else None


def simulate_one_leg(
    *,
    day: date,
    underlying: str,
    expiry: date,
    days_to_expiry: int,
    leg: str,
    symbol: str,
    strike: int,
    qty: int,
    entry_ts: pd.Timestamp,
    session_end_ts: pd.Timestamp,
    close_raw: pd.Series,
    close_ffill: pd.Series,
    high_raw: pd.Series,
    low_raw: pd.Series,
) -> Tuple[List[LegCycle], float, float, int, int, int, str]:
    """
    Simulate one option leg independently.

    State machine:
    - OPEN: short option is active. If high >= stop trigger, buy back at trigger.
    - WAIT_REENTRY: leg is flat after stop. After MIN_REENTRY_GAP_MINUTES has elapsed,
      sell again only if the original entry price is touched again, subject to retry limit.
    - DONE: retry limit exhausted after a stop. No more trades in this leg for the day.

    Return tuple:
    - cycles list
    - net pnl
    - gross pnl
    - retries used
    - stop count
    - number of cycles
    - final exit time as HH:MM
    """
    initial_entry_price = series_value(close_raw, entry_ts)
    if pd.isna(initial_entry_price):
        raise ValueError(f"No exact entry close for {symbol} at {entry_ts.strftime('%H:%M')}")

    stop_trigger = initial_entry_price * (1.0 + LEG_PREMIUM_RISE_EXIT_PCT / 100.0)

    state = "OPEN"
    current_entry_ts = entry_ts
    current_entry_price = float(initial_entry_price)
    cycles: List[LegCycle] = []
    retries_used = 0
    stop_count = 0
    cycle_no = 1
    next_reentry_allowed_ts: Optional[pd.Timestamp] = None

    # Monitor from the next minute after initial entry.
    monitor_index = close_ffill.loc[entry_ts + pd.Timedelta(minutes=1): session_end_ts].index

    for ts in monitor_index:
        if state == "OPEN":
            # Stop detection: use high if available; fall back to close.
            high_v = series_value(high_raw, ts)
            close_v = series_value(close_ffill, ts)
            observed_high = high_v if pd.notna(high_v) else close_v

            if pd.notna(observed_high) and observed_high >= stop_trigger:
                exit_price = float(stop_trigger)
                pnl_gross = (current_entry_price - exit_price) * qty
                charges = compute_leg_roundtrip_charges(current_entry_price, exit_price, qty)
                pnl_net = pnl_gross - charges
                stop_count += 1

                cycles.append(
                    LegCycle(
                        day=day,
                        underlying=underlying,
                        expiry=expiry,
                        days_to_expiry=days_to_expiry,
                        leg=leg,
                        symbol=symbol,
                        strike=int(strike),
                        cycle_no=cycle_no,
                        retry_no=retries_used,
                        entry_time=current_entry_ts.strftime("%H:%M"),
                        exit_time=ts.strftime("%H:%M"),
                        entry_price=float(current_entry_price),
                        exit_price=float(exit_price),
                        stop_trigger=float(stop_trigger),
                        exit_reason="LEG_SL",
                        minutes_held=minutes_between(current_entry_ts, ts),
                        qty_units=qty,
                        pnl_gross=float(pnl_gross),
                        txn_charges=float(charges),
                        pnl_net=float(pnl_net),
                    )
                )

                if retries_used >= MAX_RETRIES_PER_LEG:
                    state = "DONE"
                else:
                    state = "WAIT_REENTRY"
                    # The wait period starts from the stop candle timestamp. A target touch
                    # before next_reentry_allowed_ts is intentionally ignored because the
                    # strategy says to wait first, then re-enter only when the same premium
                    # is available again.
                    next_reentry_allowed_ts = first_reentry_check_timestamp(ts)

                cycle_no += 1
                continue

        elif state == "WAIT_REENTRY":
            if next_reentry_allowed_ts is not None and ts < next_reentry_allowed_ts:
                continue

            # Re-entry detection:
            # The target is the original sold premium. In strict mode, that level must
            # lie inside the candle range; otherwise the backtest may assume a fill at
            # a price that never traded during that minute.
            if reentry_level_reached(
                low_raw=low_raw,
                high_raw=high_raw,
                close_ffill=close_ffill,
                ts=ts,
                target_price=float(initial_entry_price),
            ):
                retries_used += 1
                current_entry_ts = ts
                current_entry_price = float(initial_entry_price)
                state = "OPEN"
                # Deliberately do not check stop in the same candle after re-entry.
                # The intraminute order of low/high is unknowable in 1-minute OHLC data.
                continue

        elif state == "DONE":
            # Retry limit exhausted. Nothing more to do for this leg.
            continue

    # End of session handling.
    if state == "OPEN":
        eod_exit_price = series_value(close_ffill, session_end_ts)
        if pd.isna(eod_exit_price):
            # Last resort: use current entry price if no EOD quote is available.
            eod_exit_price = current_entry_price

        pnl_gross = (current_entry_price - eod_exit_price) * qty
        charges = compute_leg_roundtrip_charges(current_entry_price, eod_exit_price, qty)
        pnl_net = pnl_gross - charges

        cycles.append(
            LegCycle(
                day=day,
                underlying=underlying,
                expiry=expiry,
                days_to_expiry=days_to_expiry,
                leg=leg,
                symbol=symbol,
                strike=int(strike),
                cycle_no=cycle_no,
                retry_no=retries_used,
                entry_time=current_entry_ts.strftime("%H:%M"),
                exit_time=session_end_ts.strftime("%H:%M"),
                entry_price=float(current_entry_price),
                exit_price=float(eod_exit_price),
                stop_trigger=float(stop_trigger),
                exit_reason="EOD",
                minutes_held=minutes_between(current_entry_ts, session_end_ts),
                qty_units=qty,
                pnl_gross=float(pnl_gross),
                txn_charges=float(charges),
                pnl_net=float(pnl_net),
            )
        )

    gross = sum(c.pnl_gross for c in cycles)
    net = sum(c.pnl_net for c in cycles)
    final_exit_time = cycles[-1].exit_time if cycles else entry_ts.strftime("%H:%M")

    return cycles, float(net), float(gross), int(retries_used), int(stop_count), int(len(cycles)), final_exit_time


# =============================================================================
# DAY SIMULATION
# =============================================================================

def simulate_day(
    *,
    und: str,
    dy: date,
    expiry: date,
    day_opt: pd.DataFrame,
    underlying_day: pd.DataFrame,
) -> Tuple[Optional[StraddleSummary], List[LegCycle], List[Dict[str, Any]]]:
    """Simulate one trading day for one selected underlying and its nearest expiry."""
    skipped: List[Dict[str, Any]] = []
    leg_cycles_all: List[LegCycle] = []

    idx_all = build_minute_index(dy, SESSION_START_IST, SESSION_END_IST)
    session_end_ts = idx_all[-1]
    entry_ts = pd.Timestamp(datetime.combine(dy, ENTRY_TIME), tz=ist_tz())

    qty = int(QTY_UNITS[und])
    step = int(STRIKE_STEP[und])
    dte = int((expiry - dy).days)

    u_px = asof_close(underlying_day, entry_ts)
    if pd.isna(u_px):
        skipped.append({"day": dy, "underlying": und, "expiry": expiry, "reason": "No underlying price at entry"})
        return None, leg_cycles_all, skipped

    atm = round_to_step(u_px, step)
    pe_strike, ce_strike = compute_otm_strikes(atm, step, OTM_DISTANCE_STEPS)

    pe_symbol = pick_symbol(day_opt, pe_strike, "PE")
    ce_symbol = pick_symbol(day_opt, ce_strike, "CE")
    if not pe_symbol or not ce_symbol:
        skipped.append(
            {
                "day": dy,
                "underlying": und,
                "expiry": expiry,
                "entry_underlying": u_px,
                "atm_strike": atm,
                "pe_strike": pe_strike,
                "ce_strike": ce_strike,
                "reason": "Requested PE/CE strike not available in nearest-expiry option data",
            }
        )
        return None, leg_cycles_all, skipped

    # Build PE series.
    pe_close_raw = build_leg_price_series(day_opt, idx_all, pe_strike, "PE", pe_symbol, "close", ffill=False)
    pe_close_ffill = pe_close_raw.ffill()
    pe_high_raw = build_leg_price_series(day_opt, idx_all, pe_strike, "PE", pe_symbol, "high", ffill=False)
    pe_low_raw = build_leg_price_series(day_opt, idx_all, pe_strike, "PE", pe_symbol, "low", ffill=False)

    # Build CE series.
    ce_close_raw = build_leg_price_series(day_opt, idx_all, ce_strike, "CE", ce_symbol, "close", ffill=False)
    ce_close_ffill = ce_close_raw.ffill()
    ce_high_raw = build_leg_price_series(day_opt, idx_all, ce_strike, "CE", ce_symbol, "high", ffill=False)
    ce_low_raw = build_leg_price_series(day_opt, idx_all, ce_strike, "CE", ce_symbol, "low", ffill=False)

    pe_entry = series_value(pe_close_raw, entry_ts)
    ce_entry = series_value(ce_close_raw, entry_ts)
    if pd.isna(pe_entry) or pd.isna(ce_entry):
        skipped.append(
            {
                "day": dy,
                "underlying": und,
                "expiry": expiry,
                "entry_time": ENTRY_TIME_IST,
                "pe_symbol": pe_symbol,
                "ce_symbol": ce_symbol,
                "reason": "Exact entry close missing for PE or CE",
            }
        )
        return None, leg_cycles_all, skipped

    pe_cycles, pe_net, pe_gross, pe_retries, pe_stops, pe_cycle_count, pe_final_time = simulate_one_leg(
        day=dy,
        underlying=und,
        expiry=expiry,
        days_to_expiry=dte,
        leg="PE",
        symbol=pe_symbol,
        strike=pe_strike,
        qty=qty,
        entry_ts=entry_ts,
        session_end_ts=session_end_ts,
        close_raw=pe_close_raw,
        close_ffill=pe_close_ffill,
        high_raw=pe_high_raw,
        low_raw=pe_low_raw,
    )

    ce_cycles, ce_net, ce_gross, ce_retries, ce_stops, ce_cycle_count, ce_final_time = simulate_one_leg(
        day=dy,
        underlying=und,
        expiry=expiry,
        days_to_expiry=dte,
        leg="CE",
        symbol=ce_symbol,
        strike=ce_strike,
        qty=qty,
        entry_ts=entry_ts,
        session_end_ts=session_end_ts,
        close_raw=ce_close_raw,
        close_ffill=ce_close_ffill,
        high_raw=ce_high_raw,
        low_raw=ce_low_raw,
    )

    leg_cycles_all.extend(pe_cycles)
    leg_cycles_all.extend(ce_cycles)

    # Original no-stop EOD comparison for reference.
    eod_pe = series_value(pe_close_ffill, session_end_ts)
    eod_ce = series_value(ce_close_ffill, session_end_ts)
    if pd.isna(eod_pe) or pd.isna(eod_ce):
        eod_pnl_if_held = float("nan")
    else:
        eod_pnl_if_held = (float(pe_entry) - eod_pe) * qty + (float(ce_entry) - eod_ce) * qty

    total_gross = pe_gross + ce_gross
    total_charges = sum(c.txn_charges for c in leg_cycles_all)
    total_net = pe_net + ce_net

    # Any leg held till EOD means its last cycle ended at EOD.
    any_open_till_eod = int(any(c.exit_reason == "EOD" and c.exit_time == session_end_ts.strftime("%H:%M") for c in leg_cycles_all))

    # Final exit time is the later leg cycle exit time. Since times are HH:MM in same day, string max is safe.
    final_exit_time = max(pe_final_time, ce_final_time)

    summary = StraddleSummary(
        day=dy,
        underlying=und,
        expiry=expiry,
        days_to_expiry=dte,
        qty_units=qty,
        entry_time=ENTRY_TIME_IST,
        entry_underlying=float(u_px),
        atm_strike=int(atm),
        otm_distance_steps=int(OTM_DISTANCE_STEPS),
        pe_strike=int(pe_strike),
        ce_strike=int(ce_strike),
        pe_symbol=pe_symbol,
        ce_symbol=ce_symbol,
        pe_initial_entry=float(pe_entry),
        ce_initial_entry=float(ce_entry),
        pe_stop_trigger=float(pe_entry) * (1.0 + LEG_PREMIUM_RISE_EXIT_PCT / 100.0),
        ce_stop_trigger=float(ce_entry) * (1.0 + LEG_PREMIUM_RISE_EXIT_PCT / 100.0),
        pe_cycles=int(pe_cycle_count),
        ce_cycles=int(ce_cycle_count),
        pe_retries_used=int(pe_retries),
        ce_retries_used=int(ce_retries),
        pe_stop_count=int(pe_stops),
        ce_stop_count=int(ce_stops),
        pe_net_pnl=float(pe_net),
        ce_net_pnl=float(ce_net),
        total_gross_pnl=float(total_gross),
        total_txn_charges=float(total_charges),
        total_net_pnl=float(total_net),
        final_exit_time=final_exit_time,
        any_open_leg_till_eod=any_open_till_eod,
        eod_pnl_if_held_without_stops=float(eod_pnl_if_held) if pd.notna(eod_pnl_if_held) else float("nan"),
    )

    return summary, leg_cycles_all, skipped


# =============================================================================
# PASS 2: PROCESS PICKLES AND RUN STRATEGY
# =============================================================================

def process_pickles(
    *,
    pickle_paths: List[str],
    min_expiry_map: Dict[Tuple[str, date], date],
    actual_underlying_by_day: Dict[date, str],
    underlying_data: Dict[str, pd.DataFrame],
    window_start: date,
    window_end: date,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Read option pickles and run strategy for selected underlying/day/nearest-expiry slices."""
    summaries: List[Dict[str, Any]] = []
    cycles: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []

    processed_day_keys: set[Tuple[str, date, date]] = set()

    for p in pickle_paths:
        try:
            raw = pd.read_pickle(p)
            if not isinstance(raw, pd.DataFrame) or raw.empty:
                continue

            d = standardize_option_df(raw, p)
            if d.empty:
                continue

            # Restrict to 0-DTE and 1-DTE because these are usually the actual tradable weekly/expiry choices
            # in the user's earlier logic.
            d["days_to_expiry"] = (pd.to_datetime(d["expiry_date"]) - pd.to_datetime(d["day"])).dt.days
            d = d[d["days_to_expiry"].isin([0, 1])]
            d = d[(d["day"] >= window_start) & (d["day"] <= window_end)]
            if d.empty:
                continue

            for (und, dy, ex), g in d.groupby(["underlying", "day", "expiry_date"], sort=False):
                # Only trade nearest expiry for that underlying/day.
                if min_expiry_map.get((und, dy)) != ex:
                    continue

                # Only one selected underlying per day, matching the user's prior actual-trade discipline.
                if actual_underlying_by_day.get(dy) != und:
                    continue

                day_key = (und, dy, ex)
                if day_key in processed_day_keys:
                    skipped.append(
                        {
                            "day": dy,
                            "underlying": und,
                            "expiry": ex,
                            "source_file": os.path.basename(p),
                            "reason": "Duplicate underlying/day/expiry already processed successfully; skipped to avoid double-counting",
                        }
                    )
                    continue

                uday = underlying_data.get(und, pd.DataFrame())
                uday = uday[uday["day"] == dy] if not uday.empty else uday
                if uday.empty:
                    skipped.append(
                        {
                            "day": dy,
                            "underlying": und,
                            "expiry": ex,
                            "source_file": os.path.basename(p),
                            "reason": "Underlying minute data missing for day",
                        }
                    )
                    continue

                summary, leg_cycles, skip_rows = simulate_day(
                    und=und,
                    dy=dy,
                    expiry=ex,
                    day_opt=g,
                    underlying_day=uday,
                )
                if summary is not None:
                    # Mark as processed only after a successful simulation. This allows a later
                    # pickle to rescue the day if an earlier partial/duplicate pickle lacked
                    # the required strikes or exact entry candles.
                    processed_day_keys.add(day_key)
                    summaries.append(asdict(summary))
                cycles.extend([asdict(c) for c in leg_cycles])
                skipped.extend(skip_rows)

            print(f"[PASS2 OK] {os.path.basename(p)}")

        except Exception as e:
            msg = f"[PASS2 WARN] {os.path.basename(p)} failed: {e}"
            if FAIL_ON_PICKLE_ERROR:
                raise RuntimeError(msg) from e
            print(msg)

    summary_df = pd.DataFrame(summaries)
    cycle_df = pd.DataFrame(cycles)
    skipped_df = pd.DataFrame(skipped)

    if not summary_df.empty:
        summary_df = summary_df.sort_values(["day", "underlying"]).reset_index(drop=True)
        summary_df["is_net_pnl_positive"] = (summary_df["total_net_pnl"] > 0).astype(int)

    if not cycle_df.empty:
        cycle_df = cycle_df.sort_values(["day", "underlying", "leg", "cycle_no"]).reset_index(drop=True)

    if not skipped_df.empty:
        if "day" not in skipped_df.columns:
            skipped_df["day"] = pd.NaT
        skipped_df = skipped_df.sort_values(["day"], na_position="last").reset_index(drop=True)

    return summary_df, cycle_df, skipped_df


# =============================================================================
# REPORTING
# =============================================================================

def build_monthwise_summary(summary_df: pd.DataFrame) -> pd.DataFrame:
    """Build month-wise strategy summary from one-row-per-day summary DataFrame."""
    if summary_df.empty:
        return pd.DataFrame()

    tmp = summary_df.copy()
    tmp["month"] = pd.to_datetime(tmp["day"]).dt.to_period("M").astype(str)

    out = (
        tmp.groupby("month", as_index=False)
        .agg(
            trading_days=("total_net_pnl", "count"),
            total_net_pnl=("total_net_pnl", "sum"),
            avg_net_pnl=("total_net_pnl", "mean"),
            winning_days=("is_net_pnl_positive", "sum"),
            max_profit_day=("total_net_pnl", "max"),
            max_loss_day=("total_net_pnl", "min"),
            avg_pe_retries=("pe_retries_used", "mean"),
            avg_ce_retries=("ce_retries_used", "mean"),
            total_pe_stops=("pe_stop_count", "sum"),
            total_ce_stops=("ce_stop_count", "sum"),
        )
    )
    out["losing_days"] = out["trading_days"] - out["winning_days"]
    out["win_rate_pct"] = (100.0 * out["winning_days"] / out["trading_days"]).round(2)

    # Average loss on loss days as a negative number. 0 if no losing days.
    loss_stats = (
        tmp.groupby("month", as_index=False)
        .agg(avg_loss_on_loss_days=("total_net_pnl", lambda s: float(s[s < 0].mean()) if (s < 0).any() else 0.0))
    )
    out = out.merge(loss_stats, on="month", how="left")
    return out


def build_instrument_summary(summary_df: pd.DataFrame) -> pd.DataFrame:
    """Build underlying-wise summary."""
    if summary_df.empty:
        return pd.DataFrame()

    return (
        summary_df.groupby("underlying", as_index=False)
        .agg(
            trading_days=("total_net_pnl", "count"),
            total_net_pnl=("total_net_pnl", "sum"),
            avg_net_pnl=("total_net_pnl", "mean"),
            win_rate_pct=("total_net_pnl", lambda s: 100.0 * (s > 0).mean()),
            avg_pe_retries=("pe_retries_used", "mean"),
            avg_ce_retries=("ce_retries_used", "mean"),
            total_pe_stops=("pe_stop_count", "sum"),
            total_ce_stops=("ce_stop_count", "sum"),
        )
        .sort_values("total_net_pnl", ascending=False)
        .reset_index(drop=True)
    )


def build_config_df() -> pd.DataFrame:
    """Return key runtime configuration as a two-column DataFrame."""
    config = {
        "PICKLES_DIR": PICKLES_DIR,
        "ENTRY_TIME_IST": ENTRY_TIME_IST,
        "LOOKBACK_MONTHS": LOOKBACK_MONTHS,
        "OTM_DISTANCE_STEPS": OTM_DISTANCE_STEPS,
        "LEG_PREMIUM_RISE_EXIT_PCT": LEG_PREMIUM_RISE_EXIT_PCT,
        "MAX_RETRIES_PER_LEG": MAX_RETRIES_PER_LEG,
        "MIN_REENTRY_GAP_MINUTES": MIN_REENTRY_GAP_MINUTES,
        "INCLUDE_TRANSACTION_COSTS": INCLUDE_TRANSACTION_COSTS,
        "REENTRY_REQUIRE_RANGE_TOUCH": REENTRY_REQUIRE_RANGE_TOUCH,
        "QTY_UNITS": str(QTY_UNITS),
        "TRADEABLE": sorted(TRADEABLE),
        "SESSION_START_IST": SESSION_START_IST.strftime("%H:%M"),
        "SESSION_END_IST": SESSION_END_IST.strftime("%H:%M"),
        "OUTPUT_XLSX": OUTPUT_XLSX,
    }
    return pd.DataFrame([{"parameter": k, "value": str(v)} for k, v in config.items()])


def autosize_columns_safe(ws) -> None:
    """Autosize Excel columns without letting formatting errors kill the run."""
    try:
        for col_idx in range(1, (ws.max_column or 0) + 1):
            col_letter = ws.cell(row=1, column=col_idx).column_letter
            max_len = 0
            for row_idx in range(1, min(ws.max_row or 1, 2000) + 1):
                v = ws.cell(row=row_idx, column=col_idx).value
                if v is not None:
                    max_len = max(max_len, len(str(v)))
            ws.column_dimensions[col_letter].width = min(60, max(10, max_len + 2))
    except Exception:
        return


def write_excel(summary_df: pd.DataFrame, cycle_df: pd.DataFrame, skipped_df: pd.DataFrame) -> None:
    """Write Excel report with summary, leg-cycle detail, monthly summary, and skipped rows."""
    out_dir = os.path.dirname(os.path.abspath(OUTPUT_XLSX))
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    monthwise_df = build_monthwise_summary(summary_df)
    instrument_df = build_instrument_summary(summary_df)
    config_df = build_config_df()

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as xw:
        summary_df.to_excel(xw, sheet_name="trade_summary", index=False)
        cycle_df.to_excel(xw, sheet_name="leg_cycles", index=False)
        monthwise_df.to_excel(xw, sheet_name="monthwise_summary", index=False)
        instrument_df.to_excel(xw, sheet_name="instrument_summary", index=False)
        skipped_df.to_excel(xw, sheet_name="skipped", index=False)
        config_df.to_excel(xw, sheet_name="config", index=False)

        wb = xw.book
        for ws in wb.worksheets:
            ws.freeze_panes = "A2"
            autosize_columns_safe(ws)

    print(f"[DONE] Excel written: {OUTPUT_XLSX}")


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    """Program entry point."""
    validate_config()

    if not os.path.isdir(PICKLES_DIR):
        raise FileNotFoundError(f"PICKLES_DIR does not exist or is not a folder: {PICKLES_DIR}")

    paths = sorted(glob.glob(os.path.join(PICKLES_DIR, "*.pkl")) + glob.glob(os.path.join(PICKLES_DIR, "*.pickle")))
    if not paths:
        raise FileNotFoundError(f"No .pkl/.pickle files found in: {PICKLES_DIR}")

    print(f"[INFO] Pickle files found: {len(paths)}")
    print(f"[INFO] Strategy: entry={ENTRY_TIME_IST}, OTM_DISTANCE_STEPS={OTM_DISTANCE_STEPS}, "
          f"leg_stop_pct={LEG_PREMIUM_RISE_EXIT_PCT}, retries_per_leg={MAX_RETRIES_PER_LEG}")

    min_day_seen, max_day_seen, min_expiry_map = scan_pickles_pass1(paths)
    window_start = compute_window_start(max_day_seen, LOOKBACK_MONTHS)
    window_end = max_day_seen

    print(f"[INFO] Data day-range seen: {min_day_seen} -> {max_day_seen}")
    print(f"[INFO] Backtest window: {window_start} -> {window_end}")

    # Same underlying selection discipline as the reference-style actual trade output:
    # one chosen underlying per day, nearest expiry, 0/1-DTE only.
    actual_underlying_by_day = pick_actual_underlying_by_day(min_expiry_map)
    print(f"[INFO] Actual-trade days selected: {len(actual_underlying_by_day)}")

    print("[STEP] Initializing Kite...")
    kite = oUtils.intialize_kite_api()
    print("[OK] Kite initialized.")

    underlying_data = download_underlyings(kite, window_start, window_end)

    summary_df, cycle_df, skipped_df = process_pickles(
        pickle_paths=paths,
        min_expiry_map=min_expiry_map,
        actual_underlying_by_day=actual_underlying_by_day,
        underlying_data=underlying_data,
        window_start=window_start,
        window_end=window_end,
    )

    write_excel(summary_df, cycle_df, skipped_df)

    if summary_df.empty:
        print("[WARN] No completed trades. Check the 'skipped' sheet in the output Excel.")
    else:
        print("\n[SUMMARY BY UNDERLYING]")
        print(build_instrument_summary(summary_df).to_string(index=False))
        print("\n[MONTHWISE SUMMARY]")
        print(build_monthwise_summary(summary_df).to_string(index=False))


if __name__ == "__main__":
    main()
