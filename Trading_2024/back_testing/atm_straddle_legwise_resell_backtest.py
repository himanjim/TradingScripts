"""
Leg-wise short straddle backtest with independent stop exits and re-sells.

Trading model
-------------
1. At a configurable entry time, the script sells ATM CE and ATM PE.
2. Each leg is managed independently after entry.
3. If a leg's premium rises by STOPLOSS_PCT from that leg's latest sell price,
   only that leg is bought back. The opposite leg remains live.
4. After a stop-out, that same leg can be sold again if price later returns to
   the most recent sell price used for that leg.
5. Re-sell attempts are capped independently for CE and PE.
6. Any still-open leg is bought back at end of day.

Important modelling assumptions
-------------------------------
- Entry is taken only if the option has an exact 1-minute close at ENTRY_TIME_IST.
  This avoids silently entering at a stale print.
- Stop detection uses the 1-minute HIGH of the option bar. Once HIGH reaches the
  stop level, exit is assumed at the exact trigger price.
- Re-entry detection uses the 1-minute LOW of the option bar. Once LOW touches
  the anchor sell price, re-entry is assumed at that exact sell price.
- A leg is not allowed to stop out and re-enter in the same 1-minute candle.
  This is deliberate because minute OHLC data does not reveal intrabar order.
- A leg re-entered on a given minute is only eligible for another stop from the
  next minute onward.

The script follows the data-loading style of the user's reference code:
option candles are read from local pickle files, while underlying minute candles
are downloaded through the same Kite helper module used in the reference script.
"""

import os
import glob
import time
from dataclasses import dataclass
from pathlib import Path
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
ENTRY_TIME_IST = os.getenv("ENTRY_TIME_IST", "09:20")
STOPLOSS_PCT = float(os.getenv("STOPLOSS_PCT", "0.30"))
MAX_RESELLS_PER_LEG = int(os.getenv("MAX_RESELLS_PER_LEG", "4"))
ALLOWED_DAYS_TO_EXPIRY = sorted({
    int(x.strip()) for x in os.getenv("ALLOWED_DAYS_TO_EXPIRY", "0,1,2").split(",") if x.strip()
})

ENABLE_PREMIUM_GIVEBACK_EXIT = os.getenv("ENABLE_PREMIUM_GIVEBACK_EXIT", "0").strip() == "1"
PREMIUM_LOCK_ACTIVATION_DROP_PCT = float(os.getenv("PREMIUM_LOCK_ACTIVATION_DROP_PCT", "0.50"))
PREMIUM_LOCK_REBOUND_PCT = float(os.getenv("PREMIUM_LOCK_REBOUND_PCT", "0.25"))

LOOKBACK_MONTHS = int(os.getenv("LOOKBACK_MONTHS", "6"))
FAIL_ON_PICKLE_ERROR = os.getenv("FAIL_ON_PICKLE_ERROR", "0").strip() == "1"
INCLUDE_TRANSACTION_COSTS = os.getenv("INCLUDE_TRANSACTION_COSTS", "1").strip() == "1"

# If True, a day is skipped unless both CE and PE have an exact candle at the
# configured entry minute. This is the safer default for a minute-level study.
REQUIRE_EXACT_ENTRY_PRINT = os.getenv("REQUIRE_EXACT_ENTRY_PRINT", "1").strip() == "1"

SESSION_START_IST = dtime(9, 15)
SESSION_END_IST = dtime(15, 30)

QTY_UNITS = {"NIFTY": 325, "SENSEX": 100}
TRADEABLE = set(QTY_UNITS.keys())
STRIKE_STEP = {"NIFTY": 50, "SENSEX": 100}

UNDERLYING_KITE = {
    "NIFTY": {"exchange": "NSE", "tradingsymbol": "NIFTY 50"},
    "SENSEX": {"exchange": "BSE", "tradingsymbol": "SENSEX"},
}

MAX_DAYS_PER_CHUNK = 25
MAX_ATTEMPTS = 5
SLEEP_BETWEEN_CALLS_SEC = 0.20

# Zerodha F&O options charges. This keeps the same simplified charging model as
# the reference code. It is direction-aware at the order level.
BROKERAGE_PER_ORDER = 20.0
STT_SELL_PCT = 0.001
EXCHANGE_TXN_PCT = 0.0003553
SEBI_PER_CRORE = 10.0
STAMP_BUY_PCT = 0.00003
IPFT_PER_CRORE = 0.010
GST_PCT = 0.18


def _safe_fname_part(s: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in s)


def _get_downloads_folder() -> str:
    downloads = Path.home() / "Downloads"
    return str(downloads if downloads.exists() else Path.home())


_DEFAULT_OUT = os.path.join(
    _get_downloads_folder(),
    f"short_straddle_legwise_resell_{_safe_fname_part(ENTRY_TIME_IST)}"
    f"_SLPCT_{_safe_fname_part(str(STOPLOSS_PCT))}"
f"_PLADP_{_safe_fname_part(str(PREMIUM_LOCK_ACTIVATION_DROP_PCT))}"
f"_PLRP_{_safe_fname_part(str(PREMIUM_LOCK_REBOUND_PCT))}"
     f"_EPEG_{_safe_fname_part(str(ENABLE_PREMIUM_GIVEBACK_EXIT))}"
    f"_RSL_{_safe_fname_part(str(MAX_RESELLS_PER_LEG))}.xlsx",
)
OUTPUT_XLSX = os.getenv("OUTPUT_XLSX", _DEFAULT_OUT)


# =============================================================================
# HELPERS
# =============================================================================
def parse_hhmm(s: str) -> dtime:
    """Parse an HH:MM string into a Python time object."""
    hh, mm = s.strip().split(":")
    out = dtime(int(hh), int(mm))
    return out


ENTRY_TIME = parse_hhmm(ENTRY_TIME_IST)


def validate_runtime_config() -> None:
    """
    Validate configuration early so the script fails with a precise message.
    """
    if STOPLOSS_PCT <= 0:
        raise ValueError("STOPLOSS_PCT must be > 0")
    if MAX_RESELLS_PER_LEG < 0:
        raise ValueError("MAX_RESELLS_PER_LEG must be >= 0")
    if not ALLOWED_DAYS_TO_EXPIRY:
        raise ValueError("ALLOWED_DAYS_TO_EXPIRY must not be empty")
    if any(d < 0 for d in ALLOWED_DAYS_TO_EXPIRY):
        raise ValueError("ALLOWED_DAYS_TO_EXPIRY values must be >= 0")
    if ENABLE_PREMIUM_GIVEBACK_EXIT:
        if not (0 < PREMIUM_LOCK_ACTIVATION_DROP_PCT < 1):
            raise ValueError("PREMIUM_LOCK_ACTIVATION_DROP_PCT must be in (0, 1)")
        if PREMIUM_LOCK_REBOUND_PCT <= 0:
            raise ValueError("PREMIUM_LOCK_REBOUND_PCT must be > 0")
    if LOOKBACK_MONTHS <= 0:
        raise ValueError("LOOKBACK_MONTHS must be > 0")
    if ENTRY_TIME < SESSION_START_IST or ENTRY_TIME > SESSION_END_IST:
        raise ValueError(
            f"ENTRY_TIME_IST={ENTRY_TIME_IST} must be within session "
            f"{SESSION_START_IST.strftime('%H:%M')} to {SESSION_END_IST.strftime('%H:%M')}"
        )


def ist_tz():
    if ZoneInfo is not None:
        return ZoneInfo("Asia/Kolkata")
    if pytz is not None:
        return pytz.timezone("Asia/Kolkata")
    return "Asia/Kolkata"


def ensure_ist(series_or_scalar) -> Any:
    """
    Convert a pandas Series or scalar timestamp to Asia/Kolkata.
    Naive timestamps are interpreted as already being in IST.
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


def normalize_underlying(name: str) -> Optional[str]:
    """
    Normalize instrument names to the supported underlying universe.
    BANKNIFTY is recognized but intentionally excluded from TRADEABLE.
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
    """Round the underlying spot to the nearest valid strike step."""
    return int(round(x / step) * step)


def build_minute_index(day_d: date, start_t: dtime, end_t: dtime) -> pd.DatetimeIndex:
    """Construct the full minute grid for one trading day in IST."""
    tz = ist_tz()
    start = pd.Timestamp(datetime.combine(day_d, start_t), tz=tz)
    end = pd.Timestamp(datetime.combine(day_d, end_t), tz=tz)
    return pd.date_range(start=start, end=end, freq="1min")


def asof_close(df: pd.DataFrame, ts: pd.Timestamp) -> float:
    """
    Return the latest close at or before ts from a minute series.
    This is used for the underlying spot at entry.
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
    """Compute the inclusive backtest start date from the latest day seen."""
    if relativedelta is not None:
        return (pd.Timestamp(end_day) - relativedelta(months=months)).date()
    return (pd.Timestamp(end_day) - pd.Timedelta(days=30 * months)).date()


# =============================================================================
# CHARGES
# =============================================================================
def compute_order_charge(side: str, premium: float, qty: int) -> float:
    """
    Compute charges for a single options order.

    The backtest opens and closes each leg separately, so charges are computed
    per SELL and per BUY order rather than per combined straddle trade.
    """
    if not INCLUDE_TRANSACTION_COSTS:
        return 0.0

    side = side.upper().strip()
    turnover = max(0.0, float(premium) * int(qty))

    brokerage = BROKERAGE_PER_ORDER
    stt = turnover * STT_SELL_PCT if side == "SELL" else 0.0
    txn = turnover * EXCHANGE_TXN_PCT
    sebi = turnover * SEBI_PER_CRORE / 1_00_00_000
    stamp = turnover * STAMP_BUY_PCT if side == "BUY" else 0.0
    ipft = turnover * IPFT_PER_CRORE / 1_00_00_000
    gst = (brokerage + txn + sebi) * GST_PCT
    return round(brokerage + stt + txn + sebi + stamp + ipft + gst, 2)


# =============================================================================
# KITE HISTORICAL HELPERS
# =============================================================================
def _iter_chunks_by_date(from_dt: datetime, to_dt: datetime, days_per_chunk: int) -> List[Tuple[datetime, datetime]]:
    """
    Split a large historical download request into smaller date chunks.
    This avoids oversized API pulls and mirrors the reference code pattern.
    """
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
    """Load the full instrument dump once per exchange and reuse it."""
    ex = exchange.upper().strip()
    if ex not in cache:
        print(f"[STEP] Loading instruments dump for {ex} ...")
        cache[ex] = kite.instruments(ex)
        print(f"[INFO] {ex} instruments: {len(cache[ex])}")
    return cache[ex]


def get_instrument_token(kite, exchange: str, tradingsymbol: str, cache: Dict[str, List[Dict]]) -> int:
    """Resolve instrument token for the underlying spot series download."""
    ex = exchange.upper().strip()
    wanted = tradingsymbol.strip().upper()
    for r in _kite_instruments_cached(kite, ex, cache):
        if str(r.get("tradingsymbol", "")).upper() == wanted:
            return int(r["instrument_token"])
    raise ValueError(f"Instrument not found on {ex}: '{tradingsymbol}'")


def fetch_history_minute(kite, instrument_token: int, from_dt: datetime, to_dt: datetime, label: str) -> List[Dict]:
    """
    Fetch minute history with retries and chunking.
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
    """Convert Kite historical rows to a clean minute DataFrame."""
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
class LegTradeRow:
    """One completed short cycle for one leg."""
    day: date
    underlying: str
    expiry: date
    days_to_expiry: int
    atm_strike: int
    qty_units: int
    leg: str
    cycle_no: int
    sell_time: str
    sell_price: float
    trigger_price: float
    buy_time: str
    buy_price: float
    exit_reason: str
    gross_pnl: float
    charges: float
    net_pnl: float
    resell_count_used: int
    ce_symbol: str
    pe_symbol: str


@dataclass
class DaySummaryRow:
    """Day-level summary combining both CE and PE activity."""
    day: date
    underlying: str
    expiry: date
    days_to_expiry: int
    atm_strike: int
    qty_units: int
    entry_time: str
    entry_underlying: float
    ce_symbol: str
    pe_symbol: str
    entry_ce: float
    entry_pe: float
    ce_cycles: int
    pe_cycles: int
    ce_resells_done: int
    pe_resells_done: int
    ce_stop_exits: int
    pe_stop_exits: int
    ce_final_exit_price: float
    pe_final_exit_price: float
    ce_final_exit_reason: str
    pe_final_exit_reason: str
    ce_gross_pnl: float
    pe_gross_pnl: float
    total_gross_pnl: float
    total_charges: float
    total_net_pnl: float


@dataclass
class LegState:
    """In-memory state for one live or stopped-out leg during intraday simulation."""
    leg: str
    symbol: str
    is_open: bool
    last_sell_price: float
    cycle_no: int
    resells_done: int
    entry_ts: pd.Timestamp
    stop_exits: int
    realized_gross: float
    realized_charges: float
    last_exit_price: float
    last_exit_reason: str
    best_low_since_entry: float


# =============================================================================
# PASS-1: NEAREST EXPIRY PER (UNDERLYING, DAY)
# =============================================================================
def scan_pickles_pass1(pickle_paths: List[str]) -> Tuple[date, Dict[Tuple[str, date], date], date]:
    """
    First pass over pickles.

    This identifies:
    - the latest day present in the local option archive,
    - the earliest day seen, and
    - the nearest valid expiry for each (underlying, trading day).
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
            d2["days_to_expiry"] = (pd.to_datetime(d2["expiry_date"]) - pd.to_datetime(d2["day"])).dt.days
            d2 = d2[d2["days_to_expiry"].isin(ALLOWED_DAYS_TO_EXPIRY)]
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
def download_underlyings(kite, day_start: date, day_end: date) -> Dict[str, pd.DataFrame]:
    """
    Download minute spot data for each supported underlying for the full test window.
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
# SERIES HELPERS
# =============================================================================
def _pick_symbol(day_opt: pd.DataFrame, strike: int, opt_type: str) -> Optional[str]:
    """Pick a unique option symbol for the selected strike and option type."""
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
    Build a minute-aligned series for one option leg and one price field.
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


# =============================================================================
# CORE SIMULATION
# =============================================================================
def _leg_exit_event(high_price: float, stop_trigger: float) -> bool:
    """Return True if the minute high has touched the stop trigger."""
    if pd.isna(high_price):
        return False
    return float(high_price) >= float(stop_trigger)


def _leg_reentry_event(
    last_exit_reason: str,
    low_price: float,
    high_price: float,
    anchor_sell_price: float,
) -> bool:
    """
    Return True if price has revisited the anchor sell price in the correct
    direction for the previous exit type.

    - After STOP_PCT, premium was above the sell price and must come back down,
      so re-entry is detected from the minute LOW.
    - After PREMIUM_GIVEBACK, premium was below the sell price and must rise back
      up to it, so re-entry is detected from the minute HIGH.
    """
    reason = str(last_exit_reason).upper().strip()

    if reason == "PREMIUM_GIVEBACK":
        if pd.isna(high_price):
            return False
        return float(high_price) >= float(anchor_sell_price)

    # Default behaviour for STOP_PCT and any other defensive fallback.
    if pd.isna(low_price):
        return False
    return float(low_price) <= float(anchor_sell_price)

def _leg_giveback_trigger(anchor_sell_price: float, best_low_since_entry: float) -> float:
    """
    Return the premium-lock exit trigger after a sufficiently deep favourable fall.

    Logic:
    1. Do nothing until premium has fallen by at least
       PREMIUM_LOCK_ACTIVATION_DROP_PCT from the sell price.
    2. Once armed, exit when premium rebounds by PREMIUM_LOCK_REBOUND_PCT
       from the best low seen since entry.

    Examples:
    - sell 320, best low 160, activation drop 50%, rebound 25%
      -> armed, trigger = 160 * 1.25 = 200
    - sell 320, best low 160, activation drop 50%, rebound 50%
      -> armed, trigger = 160 * 1.50 = 240
    """
    sell_px = float(anchor_sell_price)
    best_low = float(best_low_since_entry)

    if best_low >= sell_px:
        return float("nan")

    activation_low = sell_px * (1.0 - PREMIUM_LOCK_ACTIVATION_DROP_PCT)

    # Arm only after the premium has fallen deeply enough.
    if best_low > activation_low:
        return float("nan")

    trigger = best_low * (1.0 + PREMIUM_LOCK_REBOUND_PCT)

    # Rebound exit should not sit above the original sell price.
    return min(trigger, sell_px)


def _close_leg(
    state: LegState,
    exit_ts: pd.Timestamp,
    exit_price: float,
    exit_reason: str,
    trigger_price: float,
    qty: int,
    dy: date,
    und: str,
    expiry: date,
    atm: int,
    ce_symbol: str,
    pe_symbol: str,
) -> LegTradeRow:
    """
    Close one active short leg, update state, and return the completed cycle row.
    """
    gross = (float(state.last_sell_price) - float(exit_price)) * qty
    charges = compute_order_charge("SELL", state.last_sell_price, qty) + compute_order_charge("BUY", exit_price, qty)
    net = gross - charges

    state.realized_gross += gross
    state.realized_charges += charges
    state.is_open = False
    state.last_exit_price = float(exit_price)
    state.last_exit_reason = exit_reason
    if exit_reason == "STOP_PCT":
        state.stop_exits += 1

    return LegTradeRow(
        day=dy,
        underlying=und,
        expiry=expiry,
        days_to_expiry=int((expiry - dy).days),
        atm_strike=int(atm),
        qty_units=int(qty),
        leg=state.leg,
        cycle_no=state.cycle_no,
        sell_time=state.entry_ts.strftime("%H:%M"),
        sell_price=round(float(state.last_sell_price), 4),
        trigger_price=round(float(trigger_price), 4) if not pd.isna(trigger_price) else float("nan"),
        buy_time=exit_ts.strftime("%H:%M"),
        buy_price=round(float(exit_price), 4),
        exit_reason=exit_reason,
        gross_pnl=round(float(gross), 2),
        charges=round(float(charges), 2),
        net_pnl=round(float(net), 2),
        resell_count_used=int(state.resells_done),
        ce_symbol=ce_symbol,
        pe_symbol=pe_symbol,
    )


def _attempt_reentry(
    state: LegState,
    ts: pd.Timestamp,
    low_price: float,
    high_price: float,
) -> bool:
    """
    Re-open a previously exited leg if price revisits the anchor sell price in
    the correct direction for the last exit reason.
    """
    if state.is_open:
        return False
    if state.resells_done >= MAX_RESELLS_PER_LEG:
        return False
    if not _leg_reentry_event(
        state.last_exit_reason,
        low_price,
        high_price,
        state.last_sell_price,
    ):
        return False

    state.is_open = True
    state.entry_ts = ts
    state.cycle_no += 1
    state.resells_done += 1
    # In _attempt_reentry, after setting is_open = True:
    state.best_low_since_entry = float(state.last_sell_price)

    # Only carry the re-entry bar's low into the new cycle when the prior exit
    # was STOP_PCT. After PREMIUM_GIVEBACK, the re-entry is detected from HIGH,
    # so the same bar's LOW is pre-reentry information and must be ignored.
    if str(state.last_exit_reason).upper().strip() == "STOP_PCT" and not pd.isna(low_price):
        state.best_low_since_entry = min(state.best_low_since_entry, float(low_price))
    return True


def _get_entry_price(series_raw: pd.Series, series_ffill: pd.Series, entry_ts: pd.Timestamp) -> float:
    """
    Get entry premium according to the configured entry strictness.
    """
    if REQUIRE_EXACT_ENTRY_PRINT:
        return float(series_raw.loc[entry_ts]) if not pd.isna(series_raw.loc[entry_ts]) else float("nan")
    return float(series_ffill.loc[entry_ts]) if not pd.isna(series_ffill.loc[entry_ts]) else float("nan")


def simulate_day_legwise_resell(
    *,
    und: str,
    dy: date,
    expiry: date,
    day_opt: pd.DataFrame,
    underlying_day: pd.DataFrame,
) -> Tuple[List[LegTradeRow], Optional[DaySummaryRow], List[Dict[str, Any]]]:
    """
    Simulate one day for one underlying and its nearest valid expiry.
    """
    leg_trades: List[LegTradeRow] = []
    skipped: List[Dict[str, Any]] = []

    idx_all = build_minute_index(dy, SESSION_START_IST, SESSION_END_IST)
    session_end_ts = idx_all[-1]
    entry_ts = pd.Timestamp(datetime.combine(dy, ENTRY_TIME), tz=ist_tz())

    if entry_ts not in idx_all:
        skipped.append({"day": dy, "underlying": und, "expiry": expiry, "reason": "Entry timestamp not in session index"})
        return leg_trades, None, skipped

    qty = int(QTY_UNITS[und])
    step = int(STRIKE_STEP[und])

    u_px = asof_close(underlying_day, entry_ts)
    if pd.isna(u_px):
        skipped.append({"day": dy, "underlying": und, "expiry": expiry, "reason": f"No underlying price at entry {entry_ts.strftime('%H:%M')}"})
        return leg_trades, None, skipped

    atm = round_to_step(float(u_px), step)

    ce_sym = _pick_symbol(day_opt, atm, "CE")
    pe_sym = _pick_symbol(day_opt, atm, "PE")
    if not ce_sym or not pe_sym:
        skipped.append({"day": dy, "underlying": und, "expiry": expiry, "atm_strike": atm, "reason": "ATM CE/PE not available in pickle band"})
        return leg_trades, None, skipped

    ce_close_raw = _build_leg_series(day_opt, idx_all, atm, "CE", ce_sym, "close", do_ffill=False)
    pe_close_raw = _build_leg_series(day_opt, idx_all, atm, "PE", pe_sym, "close", do_ffill=False)
    ce_close = ce_close_raw.ffill()
    pe_close = pe_close_raw.ffill()
    ce_high = _build_leg_series(day_opt, idx_all, atm, "CE", ce_sym, "high", do_ffill=False)
    pe_high = _build_leg_series(day_opt, idx_all, atm, "PE", pe_sym, "high", do_ffill=False)
    ce_low = _build_leg_series(day_opt, idx_all, atm, "CE", ce_sym, "low", do_ffill=False)
    pe_low = _build_leg_series(day_opt, idx_all, atm, "PE", pe_sym, "low", do_ffill=False)

    ce_entry = _get_entry_price(ce_close_raw, ce_close, entry_ts)
    pe_entry = _get_entry_price(pe_close_raw, pe_close, entry_ts)
    if pd.isna(ce_entry) or pd.isna(pe_entry):
        why = "No exact CE/PE close available at entry timestamp" if REQUIRE_EXACT_ENTRY_PRINT else "No CE/PE price available at entry timestamp"
        skipped.append({"day": dy, "underlying": und, "expiry": expiry, "atm_strike": atm, "reason": why})
        return leg_trades, None, skipped

    ce_state = LegState(
        leg="CE",
        symbol=ce_sym,
        is_open=True,
        last_sell_price=float(ce_entry),
        cycle_no=1,
        resells_done=0,
        entry_ts=entry_ts,
        stop_exits=0,
        realized_gross=0.0,
        realized_charges=0.0,
        last_exit_price=float("nan"),
        last_exit_reason="",
        best_low_since_entry=float(ce_entry),
    )
    pe_state = LegState(
        leg="PE",
        symbol=pe_sym,
        is_open=True,
        last_sell_price=float(pe_entry),
        cycle_no=1,
        resells_done=0,
        entry_ts=entry_ts,
        stop_exits=0,
        realized_gross=0.0,
        realized_charges=0.0,
        last_exit_price=float("nan"),
        last_exit_reason="",
        best_low_since_entry=float(pe_entry),
    )


    # Scan minute bars after entry. Exit is checked before re-entry, and the same
    # leg cannot both exit and re-enter in a single bar.
    for ts in idx_all[idx_all.get_loc(entry_ts) + 1:]:
        ce_closed_this_bar = False
        pe_closed_this_bar = False

        if ce_state.is_open:
            ce_stop_trigger = float(ce_state.last_sell_price) * (1.0 + STOPLOSS_PCT)

            # 1) adverse stop-loss exit
            if _leg_exit_event(ce_high.loc[ts], ce_stop_trigger):
                leg_trades.append(
                    _close_leg(
                        ce_state,
                        ts,
                        ce_stop_trigger,
                        "STOP_PCT",
                        ce_stop_trigger,
                        qty,
                        dy,
                        und,
                        expiry,
                        atm,
                        ce_sym,
                        pe_sym,
                    )
                )
                ce_closed_this_bar = True

            # 2) profit-protect / premium giveback exit
            elif ENABLE_PREMIUM_GIVEBACK_EXIT:
                ce_giveback_trigger = _leg_giveback_trigger(
                    ce_state.last_sell_price,
                    ce_state.best_low_since_entry,
                )
                if (not pd.isna(ce_giveback_trigger)) and _leg_exit_event(ce_high.loc[ts], ce_giveback_trigger):
                    leg_trades.append(
                        _close_leg(
                            ce_state,
                            ts,
                            ce_giveback_trigger,
                            "PREMIUM_GIVEBACK",
                            ce_giveback_trigger,
                            qty,
                            dy,
                            und,
                            expiry,
                            atm,
                            ce_sym,
                            pe_sym,
                        )
                    )
                    ce_closed_this_bar = True

            # only after exit checks, update the best low using this bar's low
            if (not ce_closed_this_bar) and (not pd.isna(ce_low.loc[ts])):
                ce_state.best_low_since_entry = min(
                    float(ce_state.best_low_since_entry),
                    float(ce_low.loc[ts]),
                )

        if pe_state.is_open:
            pe_stop_trigger = float(pe_state.last_sell_price) * (1.0 + STOPLOSS_PCT)

            # 1) adverse stop-loss exit
            if _leg_exit_event(pe_high.loc[ts], pe_stop_trigger):
                leg_trades.append(
                    _close_leg(
                        pe_state,
                        ts,
                        pe_stop_trigger,
                        "STOP_PCT",
                        pe_stop_trigger,
                        qty,
                        dy,
                        und,
                        expiry,
                        atm,
                        ce_sym,
                        pe_sym,
                    )
                )
                pe_closed_this_bar = True

            # 2) profit-protect / premium giveback exit
            elif ENABLE_PREMIUM_GIVEBACK_EXIT:
                pe_giveback_trigger = _leg_giveback_trigger(
                    pe_state.last_sell_price,
                    pe_state.best_low_since_entry,
                )
                if (not pd.isna(pe_giveback_trigger)) and _leg_exit_event(pe_high.loc[ts], pe_giveback_trigger):
                    leg_trades.append(
                        _close_leg(
                            pe_state,
                            ts,
                            pe_giveback_trigger,
                            "PREMIUM_GIVEBACK",
                            pe_giveback_trigger,
                            qty,
                            dy,
                            und,
                            expiry,
                            atm,
                            ce_sym,
                            pe_sym,
                        )
                    )
                    pe_closed_this_bar = True

            # only after exit checks, update the best low using this bar's low
            if (not pe_closed_this_bar) and (not pd.isna(pe_low.loc[ts])):
                pe_state.best_low_since_entry = min(
                    float(pe_state.best_low_since_entry),
                    float(pe_low.loc[ts]),
                )

        if (not ce_closed_this_bar) and (not ce_state.is_open):
            _attempt_reentry(ce_state, ts, ce_low.loc[ts], ce_high.loc[ts])

        if (not pe_closed_this_bar) and (not pe_state.is_open):
            _attempt_reentry(pe_state, ts, pe_low.loc[ts], pe_high.loc[ts])

    # End-of-day square-off for any leg still open.
    ce_eod_price = float("nan")
    pe_eod_price = float("nan")

    if ce_state.is_open:
        ce_eod_px = ce_close.loc[session_end_ts]
        if pd.isna(ce_eod_px):
            skipped.append({"day": dy, "underlying": und, "expiry": expiry, "reason": "CE EOD close missing"})
            return leg_trades, None, skipped
        ce_eod_price = float(ce_eod_px)
        leg_trades.append(
            _close_leg(ce_state, session_end_ts, ce_eod_price, "EOD", float("nan"), qty, dy, und, expiry, atm, ce_sym, pe_sym)
        )
    else:
        ce_eod_price = float(ce_state.last_exit_price)

    if pe_state.is_open:
        pe_eod_px = pe_close.loc[session_end_ts]
        if pd.isna(pe_eod_px):
            skipped.append({"day": dy, "underlying": und, "expiry": expiry, "reason": "PE EOD close missing"})
            return leg_trades, None, skipped
        pe_eod_price = float(pe_eod_px)
        leg_trades.append(
            _close_leg(pe_state, session_end_ts, pe_eod_price, "EOD", float("nan"), qty, dy, und, expiry, atm, ce_sym, pe_sym)
        )
    else:
        pe_eod_price = float(pe_state.last_exit_price)

    summary = DaySummaryRow(
        day=dy,
        underlying=und,
        expiry=expiry,
        days_to_expiry=int((expiry - dy).days),
        atm_strike=int(atm),
        qty_units=int(qty),
        entry_time=entry_ts.strftime("%H:%M"),
        entry_underlying=round(float(u_px), 4),
        ce_symbol=ce_sym,
        pe_symbol=pe_sym,
        entry_ce=round(float(ce_entry), 4),
        entry_pe=round(float(pe_entry), 4),
        ce_cycles=int(ce_state.cycle_no),
        pe_cycles=int(pe_state.cycle_no),
        ce_resells_done=int(ce_state.resells_done),
        pe_resells_done=int(pe_state.resells_done),
        ce_stop_exits=int(ce_state.stop_exits),
        pe_stop_exits=int(pe_state.stop_exits),
        ce_final_exit_price=round(float(ce_eod_price), 4),
        pe_final_exit_price=round(float(pe_eod_price), 4),
        ce_final_exit_reason=str(ce_state.last_exit_reason),
        pe_final_exit_reason=str(pe_state.last_exit_reason),
        ce_gross_pnl=round(float(ce_state.realized_gross), 2),
        pe_gross_pnl=round(float(pe_state.realized_gross), 2),
        total_gross_pnl=round(float(ce_state.realized_gross + pe_state.realized_gross), 2),
        total_charges=round(float(ce_state.realized_charges + pe_state.realized_charges), 2),
        total_net_pnl=round(float((ce_state.realized_gross + pe_state.realized_gross) - (ce_state.realized_charges + pe_state.realized_charges)), 2),
    )
    return leg_trades, summary, skipped


# =============================================================================
# PASS-2: CONSOLIDATE PICKLES AND SIMULATE
# =============================================================================
def _prepare_option_rows(df: pd.DataFrame, source_path: str) -> pd.DataFrame:
    """
    Standardize one pickle to the option columns required by the simulator.
    """
    needed_cols = ["date", "name", "type", "option_type", "strike", "expiry", "instrument", "high", "low", "close"]
    missing = [c for c in needed_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns {missing} in {source_path}")

    d2 = df[df["type"].astype(str).str.upper().eq("OPTION")][needed_cols].copy()
    if d2.empty:
        return d2

    d2["date"] = ensure_ist(d2["date"])
    d2["day"] = d2["date"].dt.date
    d2["underlying"] = d2["name"].astype(str).map(normalize_underlying)
    d2 = d2[d2["underlying"].isin(TRADEABLE)]
    if d2.empty:
        return d2

    d2["expiry_date"] = pd.to_datetime(d2["expiry"], errors="coerce").dt.date
    d2["strike_num"] = pd.to_numeric(d2["strike"], errors="coerce")
    d2["strike_int"] = d2["strike_num"].round().astype("Int64")
    d2["option_type"] = d2["option_type"].astype(str).str.upper()
    d2 = d2.dropna(subset=["day", "underlying", "expiry_date", "strike_int", "close"])
    d2["strike_int"] = d2["strike_int"].astype(int)
    d2["days_to_expiry"] = (pd.to_datetime(d2["expiry_date"]) - pd.to_datetime(d2["day"])).dt.days
    d2 = d2[d2["days_to_expiry"].isin(ALLOWED_DAYS_TO_EXPIRY)]
    return d2


def process_pickles_generate_results(
    pickle_paths: List[str],
    min_expiry_map: Dict[Tuple[str, date], date],
    underlying_data: Dict[str, pd.DataFrame],
    window_start: date,
    window_end: date,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Second pass over pickles.

    Correction over the earlier draft:
    all fragments belonging to the same (underlying, day, expiry) are first
    consolidated and deduplicated before simulation. This avoids the failure mode
    where the first pickle fragment is simulated and later fragments for the same
    day are skipped as duplicates.
    """
    all_leg_trades: List[Dict[str, Any]] = []
    all_day_summaries: List[Dict[str, Any]] = []
    skipped_rows: List[Dict[str, Any]] = []

    day_bucket_map: Dict[Tuple[str, date, date], List[pd.DataFrame]] = {}

    for p in pickle_paths:
        try:
            df = pd.read_pickle(p)
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue

            d2 = _prepare_option_rows(df, p)
            if d2.empty:
                continue

            d2 = d2[(d2["day"] >= window_start) & (d2["day"] <= window_end)]
            if d2.empty:
                continue

            for (und, dy, ex), g in d2.groupby(["underlying", "day", "expiry_date"], sort=False):
                key_ud = (und, dy)
                if key_ud not in min_expiry_map or min_expiry_map[key_ud] != ex:
                    continue
                day_bucket_map.setdefault((und, dy, ex), []).append(g.copy())

            print(f"[PASS2 OK] {os.path.basename(p)} staged")

        except Exception as e:
            msg = f"[PASS2 WARN] {os.path.basename(p)} failed: {e}"
            if FAIL_ON_PICKLE_ERROR:
                raise RuntimeError(msg) from e
            print(msg)

    for (und, dy, ex), parts in sorted(day_bucket_map.items(), key=lambda x: (x[0][1], x[0][0], x[0][2])):
        g = pd.concat(parts, ignore_index=True)

        # Deduplicate after cross-file consolidation. The chosen key keeps one row
        # per timestamp per option contract and preserves the latest encountered row.
        dedup_cols = ["date", "instrument", "option_type", "strike_int", "expiry_date"]
        g = g.sort_values(["date", "instrument"]).drop_duplicates(subset=dedup_cols, keep="last")

        uday = underlying_data.get(und)
        if uday is None:
            skipped_rows.append({"day": dy, "underlying": und, "expiry": ex, "reason": "No underlying series downloaded"})
            continue

        uday = uday[uday["day"] == dy]
        if uday.empty:
            skipped_rows.append({"day": dy, "underlying": und, "expiry": ex, "reason": "Underlying missing for day"})
            continue

        leg_rows, day_summary, skips = simulate_day_legwise_resell(
            und=und,
            dy=dy,
            expiry=ex,
            day_opt=g,
            underlying_day=uday,
        )
        # Only persist rows for fully completed days. If the day returns no
        # summary, it is treated as invalid and its partial cycles are discarded.
        if day_summary is not None:
            all_leg_trades.extend([r.__dict__ for r in leg_rows])
            all_day_summaries.append(day_summary.__dict__)
        skipped_rows.extend(skips)

    leg_df = pd.DataFrame(all_leg_trades)
    if not leg_df.empty:
        leg_df = leg_df.sort_values(["day", "underlying", "leg", "cycle_no"]).reset_index(drop=True)

    day_df = pd.DataFrame(all_day_summaries)
    if not day_df.empty:
        day_df = day_df.sort_values(["day", "underlying"]).reset_index(drop=True)

    skip_df = pd.DataFrame(skipped_rows)
    if not skip_df.empty:
        if "day" not in skip_df.columns:
            skip_df["day"] = pd.NaT
        if "underlying" not in skip_df.columns:
            skip_df["underlying"] = pd.NA
        skip_df = skip_df.sort_values(["day", "underlying"], na_position="last").reset_index(drop=True)

    return leg_df, day_df, skip_df


# =============================================================================
# ACTUAL TRADES: ONE UNDERLYING PER DAY
# =============================================================================
def pick_actual_underlying_by_day(min_expiry_map: Dict[Tuple[str, date], date]) -> Dict[date, str]:
    """
    Pick a single 'actual' underlying for each day.

    If both supported underlyings exist on a day, the nearest expiry is preferred.
    On a tie, NIFTY is preferred over SENSEX to stay aligned with the reference
    script's one-underlying-per-day reporting convention.
    """
    by_day: Dict[date, List[Tuple[date, str]]] = {}
    for (und, dy), ex in min_expiry_map.items():
        if und not in TRADEABLE:
            continue
        by_day.setdefault(dy, []).append((ex, und))

    out: Dict[date, str] = {}
    for dy, lst in by_day.items():
        lst_sorted = sorted(lst, key=lambda t: (t[0], 0 if t[1] == "NIFTY" else 1))
        out[dy] = lst_sorted[0][1]
    return out


def build_actual_day_df(day_df: pd.DataFrame, min_expiry_map: Dict[Tuple[str, date], date]) -> pd.DataFrame:
    """Filter day summaries down to one selected underlying per date."""
    if day_df.empty:
        return pd.DataFrame()
    actual_underlying = pick_actual_underlying_by_day(min_expiry_map)
    m = day_df.copy()
    m["actual_underlying_for_day"] = m["day"].map(actual_underlying)
    m = m[m["underlying"] == m["actual_underlying_for_day"]].drop(columns=["actual_underlying_for_day"])
    m = m.sort_values(["day"]).reset_index(drop=True)
    m["is_net_profit_day"] = (m["total_net_pnl"] > 0).astype(int)
    return m


def build_actual_leg_df(leg_df: pd.DataFrame, actual_day_df: pd.DataFrame) -> pd.DataFrame:
    """Filter cycle rows down to the one-underlying-per-day view."""
    if leg_df.empty or actual_day_df.empty:
        return pd.DataFrame()
    keys = actual_day_df[["day", "underlying"]].drop_duplicates().copy()
    out = leg_df.merge(keys, on=["day", "underlying"], how="inner")
    out = out.sort_values(["day", "leg", "cycle_no"]).reset_index(drop=True)
    out["is_net_profit_cycle"] = (out["net_pnl"] > 0).astype(int)
    return out


# =============================================================================
# EXCEL OUTPUT
# =============================================================================
def _autosize_columns_safe(ws) -> None:
    """Best-effort autosizing for openpyxl worksheets."""
    try:
        max_col = ws.max_column or 0
        if max_col <= 0:
            return
        for col_idx in range(1, max_col + 1):
            col_letter = ws.cell(row=1, column=col_idx).column_letter
            max_len = 0
            for row_idx in range(1, min(ws.max_row or 1, 3000) + 1):
                v = ws.cell(row=row_idx, column=col_idx).value
                if v is None:
                    continue
                max_len = max(max_len, len(str(v)))
            ws.column_dimensions[col_letter].width = min(60, max(10, max_len + 2))
    except Exception:
        return


def build_monthwise_summary(actual_day_df: pd.DataFrame) -> pd.DataFrame:
    """Build month-wise P&L summary from the actual-trades day sheet."""
    if actual_day_df.empty:
        return pd.DataFrame()
    tmp = actual_day_df.copy()
    tmp["month"] = pd.to_datetime(tmp["day"]).dt.to_period("M").astype(str)
    out = (
        tmp.groupby("month", as_index=False)
        .agg(
            trading_days=("total_net_pnl", "count"),
            profitable_days=("is_net_profit_day", "sum"),
            total_gross_pnl=("total_gross_pnl", "sum"),
            total_charges=("total_charges", "sum"),
            total_net_pnl=("total_net_pnl", "sum"),
            avg_net_pnl_per_day=("total_net_pnl", "mean"),
            ce_stop_exits=("ce_stop_exits", "sum"),
            pe_stop_exits=("pe_stop_exits", "sum"),
        )
        .sort_values("month")
        .reset_index(drop=True)
    )
    out["losing_days"] = out["trading_days"] - out["profitable_days"]
    out["win_rate_pct"] = (100.0 * out["profitable_days"] / out["trading_days"]).round(2)
    return out


def build_leg_summary(leg_df: pd.DataFrame) -> pd.DataFrame:
    """Summarize cycle-level behaviour by underlying and leg side."""
    if leg_df.empty:
        return pd.DataFrame()
    tmp = leg_df.copy()
    tmp["is_stop_exit"] = tmp["exit_reason"].astype(str).str.upper().eq("STOP_PCT")
    tmp["is_eod_exit"] = tmp["exit_reason"].astype(str).str.upper().eq("EOD")
    tmp["is_giveback_exit"] = tmp["exit_reason"].astype(str).str.upper().eq("PREMIUM_GIVEBACK")

    out = (
        tmp.groupby(["underlying", "leg"], as_index=False)
        .agg(
            cycles=("net_pnl", "count"),
            stop_exits=("is_stop_exit", "sum"),
            giveback_exits=("is_giveback_exit", "sum"),
            eod_exits=("is_eod_exit", "sum"),
            total_gross_pnl=("gross_pnl", "sum"),
            total_charges=("charges", "sum"),
            total_net_pnl=("net_pnl", "sum"),
            avg_net_pnl=("net_pnl", "mean"),
            max_cycle_profit=("net_pnl", "max"),
            max_cycle_loss=("net_pnl", "min"),
        )
        .sort_values(["underlying", "leg"])
        .reset_index(drop=True)
    )
    return out


def write_excel(
    leg_df: pd.DataFrame,
    day_df: pd.DataFrame,
    actual_day_df: pd.DataFrame,
    actual_leg_df: pd.DataFrame,
    skipped_df: pd.DataFrame,
) -> None:
    """
    Write the final Excel workbook.

    Sheet meanings:
    - all_days_backtested: day-level summaries for every valid simulated day/underlying
    - all_leg_cycles: every completed CE/PE cycle across all simulated days
    - actual_days: one selected underlying per day, matching the reference style
    - actual_leg_cycles: cycle rows for the selected one-underlying-per-day view
    - monthwise_summary: month-wise summary from actual_days
    - leg_summary: aggregate cycle summary by underlying and leg
    - skipped: all skipped days with explicit reasons
    """
    out_dir = os.path.dirname(os.path.abspath(OUTPUT_XLSX))
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    month_df = build_monthwise_summary(actual_day_df)
    leg_summary_df = build_leg_summary(actual_leg_df)

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as xw:
        day_df.to_excel(xw, sheet_name="all_days_backtested", index=False)
        leg_df.to_excel(xw, sheet_name="all_leg_cycles", index=False)
        actual_day_df.to_excel(xw, sheet_name="actual_days", index=False)
        actual_leg_df.to_excel(xw, sheet_name="actual_leg_cycles", index=False)
        month_df.to_excel(xw, sheet_name="monthwise_summary", index=False)
        leg_summary_df.to_excel(xw, sheet_name="leg_summary", index=False)
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
    """Entry point for the backtest."""
    validate_runtime_config()

    paths = sorted(glob.glob(os.path.join(PICKLES_DIR, "*.pkl")) + glob.glob(os.path.join(PICKLES_DIR, "*.pickle")))
    if not paths:
        raise FileNotFoundError(f"No .pkl/.pickle files found in: {PICKLES_DIR}")

    print(f"[INFO] Pickles found: {len(paths)}")

    end_day, min_expiry_map, min_day_seen = scan_pickles_pass1(paths)
    window_start = compute_window_start(end_day, LOOKBACK_MONTHS)

    print(f"[INFO] Data day-range seen: {min_day_seen} -> {end_day}")
    print(f"[INFO] Window: {window_start} -> {end_day}")
    print(f"[INFO] Entry time: {ENTRY_TIME_IST}")
    print(f"[INFO] Leg stop percent: {STOPLOSS_PCT:.2%}")
    print(f"[INFO] Max re-sells per leg: {MAX_RESELLS_PER_LEG}")
    print(f"[INFO] Require exact entry print: {REQUIRE_EXACT_ENTRY_PRINT}")
    print(f"[INFO] Tradeables: {sorted(TRADEABLE)}")
    print(f"[INFO] Output: {OUTPUT_XLSX}")

    print("[STEP] Initializing Kite ...")
    kite = oUtils.intialize_kite_api()
    print("[OK] Kite ready.")

    underlying_data = download_underlyings(kite, window_start, end_day)

    leg_df, day_df, skipped_df = process_pickles_generate_results(
        paths, min_expiry_map, underlying_data, window_start, end_day
    )

    actual_day_df = build_actual_day_df(day_df, min_expiry_map)
    actual_leg_df = build_actual_leg_df(leg_df, actual_day_df)

    write_excel(leg_df, day_df, actual_day_df, actual_leg_df, skipped_df)

    if not day_df.empty:
        print(day_df.groupby("underlying")[["total_net_pnl"]].describe())
    else:
        print("[WARN] No completed trading days. Check 'skipped' sheet for reasons.")


if __name__ == "__main__":
    main()
