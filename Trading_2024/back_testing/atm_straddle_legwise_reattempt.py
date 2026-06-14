import os
from pathlib import Path
import glob
import time
from dataclasses import dataclass, asdict
from datetime import datetime, date, time as dtime, timedelta
from typing import Dict, List, Tuple, Optional, Any

import pandas as pd

# Same project utility used in the reference script.
# Change this import only if your project structure is different.
import Trading_2024.OptionTradeUtils as oUtils

try:
    from zoneinfo import ZoneInfo
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

ENTRY_TIME_IST = os.getenv("ENTRY_TIME_IST", "11:30")  # HH:MM

# Leg-wise premium stop.
# Example: CE entry premium = 100, LEG_STOP_PCT = 30 => CE stop price = 130.
LEG_STOP_PCT = float(os.getenv("LEG_STOP_PCT", "20"))

# Separate same-leg reattempt limits.
# The count increases only after the stopped leg is actually re-entered.
MAX_CE_REATTEMPTS = int(os.getenv("MAX_CE_REATTEMPTS", "4"))
MAX_PE_REATTEMPTS = int(os.getenv("MAX_PE_REATTEMPTS", "4"))

# No REENTRY_DELAY_MINUTES is used anywhere in this script.
# Re-entry is driven only by full-cover reversal candles in the underlying.

SESSION_START_IST = dtime(9, 15)
SESSION_END_IST = dtime(15, 30)

LOOKBACK_MONTHS = int(os.getenv("LOOKBACK_MONTHS", "8"))

QTY_UNITS = {
    "NIFTY": 325,
    "SENSEX": 100,
}
TRADEABLE = set(QTY_UNITS.keys())

STRIKE_STEP = {
    "NIFTY": 50,
    "SENSEX": 100,
}

FAIL_ON_PICKLE_ERROR = os.getenv("FAIL_ON_PICKLE_ERROR", "0").strip() == "1"

UNDERLYING_KITE = {
    "NIFTY": {"exchange": "NSE", "tradingsymbol": "NIFTY 50"},
    "SENSEX": {"exchange": "BSE", "tradingsymbol": "SENSEX"},
}

MAX_DAYS_PER_CHUNK = 25
MAX_ATTEMPTS = 5
SLEEP_BETWEEN_CALLS_SEC = 0.20


# =============================================================================
# TRANSACTION CHARGES — ZERODHA-STYLE F&O OPTIONS, PER LEG
# =============================================================================
# The reference script computes charges for a full short-straddle attempt.
# This strategy enters and exits CE/PE independently, so charges are computed
# per completed short-option leg.
#
# For each completed short leg:
#   Entry = SELL option.
#   Exit  = BUY option.
#
# Assumptions:
#   1. Brokerage: flat per executed order. One completed leg has two orders.
#   2. STT: on sell-side premium only. For a short leg, this is entry.
#   3. Exchange transaction charge: on entry + exit turnover.
#   4. SEBI charge: on entry + exit turnover.
#   5. Stamp duty: on buy-side premium only. For a short leg, this is exit.
#   6. IPFT: on entry + exit turnover.
#   7. GST: on brokerage + exchange transaction charge + SEBI charge.

BROKERAGE_PER_ORDER = 20.0
STT_SELL_PCT = 0.001
EXCHANGE_TXN_PCT = 0.0003553
SEBI_PER_CRORE = 10.0
STAMP_BUY_PCT = 0.00003
IPFT_PER_CRORE = 0.010
GST_PCT = 0.18
INCLUDE_TRANSACTION_COSTS = True


# =============================================================================
# OUTPUT FILE
# =============================================================================

def _safe_fname_part(s: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in str(s))


def _get_downloads_folder() -> str:
    downloads = Path.home() / "Downloads"
    return str(downloads if downloads.exists() else Path.home())


_DEFAULT_OUT = os.path.join(
    _get_downloads_folder(),
    f"legwise_atm_short_straddle_reversal_{_safe_fname_part(ENTRY_TIME_IST)}"
    f"_LSP_{_safe_fname_part(str(LEG_STOP_PCT))}"
    f"_CE_{_safe_fname_part(str(MAX_CE_REATTEMPTS))}"
    f"_PE_{_safe_fname_part(str(MAX_PE_REATTEMPTS))}.xlsx",
)

OUTPUT_XLSX = os.getenv("OUTPUT_XLSX", _DEFAULT_OUT)


# =============================================================================
# TIMEZONE AND GENERAL HELPERS
# =============================================================================

def parse_hhmm(s: str) -> dtime:
    hh, mm = s.strip().split(":")
    return dtime(int(hh), int(mm))


ENTRY_TIME = parse_hhmm(ENTRY_TIME_IST)


def ist_tz():
    if ZoneInfo is not None:
        return ZoneInfo("Asia/Kolkata")
    if pytz is not None:
        return pytz.timezone("Asia/Kolkata")
    return "Asia/Kolkata"


def make_ist_ts(day_d: date, t: dtime) -> pd.Timestamp:
    return pd.Timestamp(datetime.combine(day_d, t)).tz_localize(ist_tz())


def ensure_ist(series_or_scalar) -> Any:
    """
    Convert/localise timestamps to IST.

    - Naive timestamps are treated as IST.
    - Timezone-aware timestamps are converted to IST.
    """
    dt = pd.to_datetime(series_or_scalar, errors="coerce")
    tz = ist_tz()

    if isinstance(dt, pd.Series):
        if dt.empty:
            return dt
        if dt.dt.tz is None:
            return dt.dt.tz_localize(tz)
        return dt.dt.tz_convert(tz)

    if pd.isna(dt):
        return dt

    if getattr(dt, "tzinfo", None) is None:
        return dt.tz_localize(tz)

    return dt.tz_convert(tz)


def normalize_underlying(name: str) -> Optional[str]:
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
    """
    ATM strike selection.

    The ATM strike is the nearest valid strike step to the underlying close/as-of
    price at initial entry or reversal-based re-entry.
    """
    return int(round(float(x) / step) * step)


def build_minute_index(day_d: date, start_t: dtime, end_t: dtime) -> pd.DatetimeIndex:
    start = make_ist_ts(day_d, start_t)
    end = make_ist_ts(day_d, end_t)
    return pd.date_range(start=start, end=end, freq="1min")


def compute_window_start(end_day: date, months: int) -> date:
    if relativedelta is not None:
        return (pd.Timestamp(end_day) - relativedelta(months=months)).date()
    return (pd.Timestamp(end_day) - pd.Timedelta(days=30 * months)).date()


def valid_price(x: Any) -> bool:
    try:
        v = float(x)
        return pd.notna(v) and v > 0
    except Exception:
        return False


# =============================================================================
# TRANSACTION COST CALCULATOR
# =============================================================================

def compute_short_option_leg_charges(entry_sell_price: float, exit_buy_price: float, qty: int) -> float:
    """
    Compute charges for one completed short-option leg.

    Entry: SELL option.
    Exit : BUY option.

    Legs are independent. Therefore this function is called once for every
    closed CE/PE leg row.
    """
    if not INCLUDE_TRANSACTION_COSTS:
        return 0.0

    if not valid_price(entry_sell_price) or not valid_price(exit_buy_price) or qty <= 0:
        return 0.0

    sell_turnover = float(entry_sell_price) * qty
    buy_turnover = float(exit_buy_price) * qty
    total_turnover = sell_turnover + buy_turnover

    brokerage = BROKERAGE_PER_ORDER * 2
    stt = sell_turnover * STT_SELL_PCT
    exchange_txn = total_turnover * EXCHANGE_TXN_PCT
    sebi = total_turnover * SEBI_PER_CRORE / 1_00_00_000
    stamp = buy_turnover * STAMP_BUY_PCT
    ipft = total_turnover * IPFT_PER_CRORE / 1_00_00_000
    gst = (brokerage + exchange_txn + sebi) * GST_PCT

    total = brokerage + stt + exchange_txn + sebi + stamp + ipft + gst
    return round(total, 2)


# =============================================================================
# KITE HISTORICAL HELPERS FOR UNDERLYING DATA
# =============================================================================

def _iter_chunks_by_date(
    from_dt: datetime,
    to_dt: datetime,
    days_per_chunk: int,
) -> List[Tuple[datetime, datetime]]:
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
    ex = exchange.upper().strip()

    if ex not in cache:
        print(f"[STEP] Loading instruments dump for {ex} ...")
        cache[ex] = kite.instruments(ex)
        print(f"[INFO] {ex} instruments: {len(cache[ex])}")

    return cache[ex]


def get_instrument_token(
    kite,
    exchange: str,
    tradingsymbol: str,
    cache: Dict[str, List[Dict]],
) -> int:
    ex = exchange.upper().strip()
    wanted = tradingsymbol.strip().upper()

    for row in _kite_instruments_cached(kite, ex, cache):
        if str(row.get("tradingsymbol", "")).upper() == wanted:
            return int(row["instrument_token"])

    raise ValueError(f"Instrument not found on {ex}: '{tradingsymbol}'")


def fetch_history_minute(
    kite,
    instrument_token: int,
    from_dt: datetime,
    to_dt: datetime,
    label: str,
) -> List[Dict]:
    rows_all: List[Dict] = []
    chunks = _iter_chunks_by_date(from_dt, to_dt, MAX_DAYS_PER_CHUNK)

    print(f"[INFO] Fetch {label} token={instrument_token} chunks={len(chunks)} {from_dt} -> {to_dt}")

    for i, (c_from, c_to) in enumerate(chunks, start=1):
        last_err = None

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
            except Exception as e:
                last_err = e
                time.sleep(min(8.0, 1.5 * attempt))

        if last_err is not None:
            print(f"[ERROR] {label} chunk {i}/{len(chunks)} failed: {c_from}->{c_to}: {last_err}")

        time.sleep(SLEEP_BETWEEN_CALLS_SEC)

    return rows_all


def rows_to_df(rows: List[Dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    df = pd.DataFrame(rows)
    df["date"] = ensure_ist(df["date"])

    for c in ["open", "high", "low", "close"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.drop_duplicates(subset=["date"], keep="last")
    df = df.sort_values("date").reset_index(drop=True)

    return df


def download_underlyings(kite, day_start: date, day_end: date) -> Dict[str, pd.DataFrame]:
    """
    Download 1-minute underlying OHLC data for NIFTY/SENSEX.

    Option candles are read from pickles. Underlying candles are required for:
    - ATM strike calculation;
    - reversal-candle detection;
    - exit_underlying reporting.
    """
    cache: Dict[str, List[Dict]] = {}
    from_dt = datetime.combine(day_start, SESSION_START_IST)
    to_dt = datetime.combine(day_end, SESSION_END_IST)

    out: Dict[str, pd.DataFrame] = {}

    for und, meta in UNDERLYING_KITE.items():
        token = get_instrument_token(kite, meta["exchange"], meta["tradingsymbol"], cache)

        rows = fetch_history_minute(
            kite,
            token,
            from_dt,
            to_dt,
            label=f"{meta['exchange']}:{meta['tradingsymbol']}",
        )

        df = rows_to_df(rows)

        if not df.empty:
            df["day"] = df["date"].dt.tz_convert(ist_tz()).dt.date
        else:
            df["day"] = pd.Series(dtype="object")

        out[und] = df

        print(
            f"[UNDERLYING OK] {und}: "
            f"candles={len(df)} days={df['day'].nunique() if not df.empty else 0}"
        )

    return out


# =============================================================================
# DATA STRUCTURES
# =============================================================================

@dataclass
class ActiveLeg:
    """
    One currently open short option leg.

    CE and PE are tracked independently. After same-leg re-entry, CE and PE may
    have different strikes and different symbols.
    """
    day: date
    underlying: str
    expiry: date
    days_to_expiry: int
    option_type: str
    leg_seq: int
    reattempt_no: int
    strike: int
    symbol: str
    qty_units: int
    entry_time_ts: pd.Timestamp
    entry_time: str
    entry_underlying: float
    entry_premium: float
    stop_price: float
    monitor_from_ts: pd.Timestamp
    reversal_wait_minutes: Optional[int] = None
    reversal_direction: Optional[str] = None
    reversal_candle_time: Optional[str] = None


@dataclass
class PendingReentry:
    """
    A stopped leg waiting for its required underlying reversal candle.

    CE stop waits for bearish reversal.
    PE stop waits for bullish reversal.
    """
    option_type: str
    stopped_at_ts: pd.Timestamp
    required_direction: str
    failed_log_keys: set
    logged_missing_underlying: bool = False


@dataclass
class LegTradeRow:
    day: date
    underlying: str
    expiry: date
    days_to_expiry: int
    option_type: str
    leg_seq: int
    reattempt_no: int
    strike: int
    symbol: str
    qty_units: int
    entry_time: str
    entry_underlying: float
    entry_premium: float
    exit_time: str
    exit_underlying: float
    exit_premium: float
    exit_reason: str
    leg_pnl_gross: float
    txn_charges: float
    leg_pnl_net: float
    reversal_wait_minutes: Optional[int]
    reversal_direction: Optional[str]
    reversal_candle_time: Optional[str]
    stop_price: float
    stop_candle_open: Optional[float]
    stop_candle_high: Optional[float]


# =============================================================================
# PICKLE PASS-1 — NEAREST VALID EXPIRY PER UNDERLYING/DAY
# =============================================================================

def scan_pickles_pass1(pickle_paths: List[str]) -> Tuple[date, Dict[Tuple[str, date], date], date]:
    """
    First pass over pickles.

    Builds:
        (underlying, trading_day) -> nearest valid expiry on/after trading_day

    This preserves the reference script's nearest-expiry convention.
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

            min_day_seen = file_min_day if min_day_seen is None or file_min_day < min_day_seen else min_day_seen
            max_day_seen = file_max_day if max_day_seen is None or file_max_day > max_day_seen else max_day_seen

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
        raise RuntimeError("No usable option data found in pickles for tradeable underlyings.")

    return max_day_seen, min_expiry_map, min_day_seen


# =============================================================================
# OPTION DATA PREPARATION
# =============================================================================

def prepare_option_day_frame(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise option rows from one pickle file.

    Required columns follow the reference script plus open if available:
        date, name, type, option_type, strike, expiry, instrument, high, close

    open is optional. If present, it improves conservative stop execution by
    handling gap-through-stop candles. If absent, stop exit uses stop_price.
    """
    required_cols = [
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

    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required option columns: {missing}")

    cols = required_cols.copy()
    if "open" in df.columns:
        cols.append("open")

    d2 = df[df["type"].astype(str).str.upper().eq("OPTION")][cols].copy()

    if d2.empty:
        return d2

    if "open" not in d2.columns:
        d2["open"] = pd.NA

    d2["date"] = ensure_ist(d2["date"])
    d2["day"] = d2["date"].dt.date
    d2["underlying"] = d2["name"].astype(str).map(normalize_underlying)
    d2["expiry_date"] = pd.to_datetime(d2["expiry"], errors="coerce").dt.date

    d2["strike_num"] = pd.to_numeric(d2["strike"], errors="coerce")
    d2["strike_int"] = d2["strike_num"].round().astype("Int64")

    d2["option_type"] = d2["option_type"].astype(str).str.upper().str.strip()
    d2["instrument"] = d2["instrument"].astype(str)

    for c in ["open", "high", "close"]:
        d2[c] = pd.to_numeric(d2[c], errors="coerce")

    d2 = d2.dropna(
        subset=[
            "day",
            "underlying",
            "expiry_date",
            "strike_int",
            "option_type",
            "instrument",
            "close",
        ]
    )

    d2 = d2[d2["underlying"].isin(TRADEABLE)]
    d2 = d2[d2["option_type"].isin(["CE", "PE"])]
    d2 = d2[d2["expiry_date"] >= d2["day"]]

    if d2.empty:
        return d2

    d2["strike_int"] = d2["strike_int"].astype(int)

    # Defensive duplicate handling.
    d2 = d2.drop_duplicates(
        subset=["date", "instrument", "option_type", "strike_int"],
        keep="last",
    )

    return d2.sort_values(["date", "instrument"]).reset_index(drop=True)


# =============================================================================
# DAY MARKET CONTEXT WITH SERIES CACHE
# =============================================================================

class DayMarketContext:
    """
    Cached per-day market data accessor.

    Optimization over the previous draft:
    option price series are built once per (strike, option_type, symbol, column,
    ffill) and then reused. This prevents repeated DataFrame filtering inside the
    minute-by-minute simulation loop.
    """

    def __init__(
        self,
        *,
        day_opt: pd.DataFrame,
        underlying_day: pd.DataFrame,
        idx_all: pd.DatetimeIndex,
    ) -> None:
        self.day_opt = day_opt
        self.idx_all = idx_all
        self.underlying_idx = self._build_underlying_indexed(underlying_day)
        self._series_cache: Dict[Tuple[int, str, str, str, bool], pd.Series] = {}

    def _build_underlying_indexed(self, underlying_day: pd.DataFrame) -> pd.DataFrame:
        cols = ["date", "open", "high", "low", "close"]
        missing = [c for c in cols if c not in underlying_day.columns]

        if missing:
            raise ValueError(f"Underlying missing OHLC columns: {missing}")

        u = underlying_day[cols].dropna(subset=["date"]).copy()
        u["date"] = ensure_ist(u["date"])

        for c in ["open", "high", "low", "close"]:
            u[c] = pd.to_numeric(u[c], errors="coerce")

        u = u.dropna(subset=["open", "high", "low", "close"])
        u = (
            u.drop_duplicates(subset=["date"], keep="last")
            .sort_values("date")
            .set_index("date")
        )

        # Reindex to session minutes but do not forward-fill OHLC. Reversal
        # detection must use actual previous/current 1-minute candles.
        return u.reindex(self.idx_all)

    def pick_symbol(self, strike: int, opt_type: str) -> Optional[str]:
        sub = self.day_opt[
            (self.day_opt["strike_int"] == int(strike))
            & (self.day_opt["option_type"] == opt_type)
        ]

        if sub.empty:
            return None

        syms = sorted(sub["instrument"].dropna().astype(str).unique().tolist())
        return syms[0] if syms else None

    def option_series(
        self,
        *,
        strike: int,
        opt_type: str,
        symbol: str,
        price_col: str,
        ffill: bool,
    ) -> pd.Series:
        key = (int(strike), str(opt_type), str(symbol), str(price_col), bool(ffill))

        if key in self._series_cache:
            return self._series_cache[key]

        if price_col not in self.day_opt.columns:
            s = pd.Series(index=self.idx_all, dtype="float64")
            self._series_cache[key] = s
            return s

        sub = self.day_opt[
            (self.day_opt["strike_int"] == int(strike))
            & (self.day_opt["option_type"] == opt_type)
            & (self.day_opt["instrument"].astype(str) == str(symbol))
        ][["date", price_col]].dropna()

        if sub.empty:
            s = pd.Series(index=self.idx_all, dtype="float64")
            self._series_cache[key] = s
            return s

        sub = sub.copy()
        sub["date"] = ensure_ist(sub["date"])
        sub[price_col] = pd.to_numeric(sub[price_col], errors="coerce")
        sub = sub.dropna(subset=[price_col])

        sub = (
            sub.sort_values("date")
            .drop_duplicates(subset=["date"], keep="last")
            .set_index("date")
        )

        s = sub[price_col].astype(float).reindex(self.idx_all)
        if ffill:
            s = s.ffill()

        self._series_cache[key] = s
        return s

    def exact_option_price(
        self,
        *,
        strike: int,
        opt_type: str,
        symbol: str,
        price_col: str,
        ts: pd.Timestamp,
        ffill: bool = False,
    ) -> float:
        s = self.option_series(
            strike=strike,
            opt_type=opt_type,
            symbol=symbol,
            price_col=price_col,
            ffill=ffill,
        )

        if ts not in s.index:
            return float("nan")

        v = s.loc[ts]
        return float(v) if valid_price(v) else float("nan")

    def asof_option_price(
        self,
        *,
        strike: int,
        opt_type: str,
        symbol: str,
        price_col: str,
        ts: pd.Timestamp,
    ) -> float:
        s = self.option_series(
            strike=strike,
            opt_type=opt_type,
            symbol=symbol,
            price_col=price_col,
            ffill=True,
        )

        s2 = s.loc[:ts].dropna()
        if s2.empty:
            return float("nan")

        v = s2.iloc[-1]
        return float(v) if valid_price(v) else float("nan")

    def asof_underlying_close(self, ts: pd.Timestamp) -> float:
        if self.underlying_idx.empty or "close" not in self.underlying_idx.columns:
            return float("nan")

        s = self.underlying_idx["close"].loc[:ts].dropna()
        if s.empty:
            return float("nan")

        return float(s.iloc[-1])

    def underlying_candle(self, ts: pd.Timestamp) -> Optional[pd.Series]:
        if ts not in self.underlying_idx.index:
            return None
        row = self.underlying_idx.loc[ts]
        if row[["open", "close"]].isna().any():
            return None
        return row


# =============================================================================
# REVERSAL CANDLE DETECTION
# =============================================================================

def is_full_cover_reversal(
    prev_candle: pd.Series,
    curr_candle: pd.Series,
    required_direction: str,
) -> bool:
    """
    Full-cover reversal candle definition.

    Bullish reversal:
        current_open  <= previous_close
        current_close >= previous_open
        current_close >  current_open

    Bearish reversal:
        current_open  >= previous_close
        current_close <= previous_open
        current_close <  current_open
    """
    required_direction = str(required_direction).upper().strip()

    for c in ["open", "close"]:
        if c not in prev_candle or c not in curr_candle:
            return False
        if not valid_price(prev_candle[c]) or not valid_price(curr_candle[c]):
            return False

    po = float(prev_candle["open"])
    pc = float(prev_candle["close"])
    co = float(curr_candle["open"])
    cc = float(curr_candle["close"])

    if required_direction == "BULLISH":
        return co <= pc and cc >= po and cc > co

    if required_direction == "BEARISH":
        return co >= pc and cc <= po and cc < co

    raise ValueError(f"Invalid required_direction: {required_direction}")


def reversal_needed_after_stop(option_type: str) -> str:
    """
    Re-entry direction mapping.

    PE stop normally indicates underlying fell. Re-enter PE only after bullish
    full-cover reversal.

    CE stop normally indicates underlying rose. Re-enter CE only after bearish
    full-cover reversal.
    """
    option_type = option_type.upper()

    if option_type == "PE":
        return "BULLISH"

    if option_type == "CE":
        return "BEARISH"

    raise ValueError(f"Unsupported option_type: {option_type}")


def max_reattempts_for(option_type: str) -> int:
    return MAX_CE_REATTEMPTS if option_type.upper() == "CE" else MAX_PE_REATTEMPTS


# =============================================================================
# LEG OPEN/CLOSE HELPERS
# =============================================================================

def open_short_leg(
    *,
    und: str,
    dy: date,
    expiry: date,
    ctx: DayMarketContext,
    option_type: str,
    entry_ts: pd.Timestamp,
    leg_seq: int,
    reattempt_no: int,
    reversal_wait_minutes: Optional[int],
    reversal_direction: Optional[str],
    reversal_candle_time: Optional[str],
    skipped: List[Dict[str, Any]],
) -> Optional[ActiveLeg]:
    """
    Open one fresh ATM short option leg.

    Used for:
    - initial CE/PE entry at ENTRY_TIME_IST;
    - later same-type CE/PE re-entry after reversal confirmation.

    ATM is recalculated at every fresh entry, allowing CE and PE to have
    different active strikes after re-entry.
    """
    qty = int(QTY_UNITS[und])
    step = int(STRIKE_STEP[und])

    entry_underlying = ctx.asof_underlying_close(entry_ts)

    if not valid_price(entry_underlying):
        skipped.append(
            {
                "day": dy,
                "underlying": und,
                "expiry": expiry,
                "option_type": option_type,
                "event_time": entry_ts.strftime("%H:%M"),
                "reason": "Cannot open leg: no underlying close/as-of price",
            }
        )
        return None

    strike = round_to_step(entry_underlying, step)
    symbol = ctx.pick_symbol(strike, option_type)

    if not symbol:
        skipped.append(
            {
                "day": dy,
                "underlying": und,
                "expiry": expiry,
                "option_type": option_type,
                "event_time": entry_ts.strftime("%H:%M"),
                "strike": strike,
                "reason": "Cannot open leg: ATM option symbol missing in nearest-expiry pickle data",
            }
        )
        return None

    # Entry uses exact close of the option candle at the entry/reversal minute.
    entry_premium = ctx.exact_option_price(
        strike=strike,
        opt_type=option_type,
        symbol=symbol,
        price_col="close",
        ts=entry_ts,
        ffill=False,
    )

    if not valid_price(entry_premium):
        skipped.append(
            {
                "day": dy,
                "underlying": und,
                "expiry": expiry,
                "option_type": option_type,
                "event_time": entry_ts.strftime("%H:%M"),
                "strike": strike,
                "symbol": symbol,
                "reason": "Cannot open leg: option close missing/invalid at exact entry/reversal time",
            }
        )
        return None

    stop_price = round(float(entry_premium) * (1.0 + LEG_STOP_PCT / 100.0), 4)

    return ActiveLeg(
        day=dy,
        underlying=und,
        expiry=expiry,
        days_to_expiry=int((expiry - dy).days),
        option_type=option_type,
        leg_seq=leg_seq,
        reattempt_no=reattempt_no,
        strike=int(strike),
        symbol=str(symbol),
        qty_units=qty,
        entry_time_ts=entry_ts,
        entry_time=entry_ts.strftime("%H:%M"),
        entry_underlying=float(entry_underlying),
        entry_premium=float(entry_premium),
        stop_price=float(stop_price),
        monitor_from_ts=entry_ts + pd.Timedelta(minutes=1),
        reversal_wait_minutes=reversal_wait_minutes,
        reversal_direction=reversal_direction,
        reversal_candle_time=reversal_candle_time,
    )


def close_short_leg(
    *,
    leg: ActiveLeg,
    exit_ts: pd.Timestamp,
    exit_reason: str,
    exit_premium: float,
    exit_underlying: float,
    stop_candle_open: Optional[float] = None,
    stop_candle_high: Optional[float] = None,
) -> LegTradeRow:
    """
    Close one short leg and calculate P&L.

    Short option P&L:
        (entry premium - exit premium) * quantity
    """
    qty = int(leg.qty_units)

    gross = (float(leg.entry_premium) - float(exit_premium)) * qty
    charges = compute_short_option_leg_charges(leg.entry_premium, exit_premium, qty)
    net = gross - charges

    return LegTradeRow(
        day=leg.day,
        underlying=leg.underlying,
        expiry=leg.expiry,
        days_to_expiry=leg.days_to_expiry,
        option_type=leg.option_type,
        leg_seq=leg.leg_seq,
        reattempt_no=leg.reattempt_no,
        strike=leg.strike,
        symbol=leg.symbol,
        qty_units=qty,
        entry_time=leg.entry_time,
        entry_underlying=round(float(leg.entry_underlying), 2),
        entry_premium=round(float(leg.entry_premium), 2),
        exit_time=exit_ts.strftime("%H:%M"),
        exit_underlying=round(float(exit_underlying), 2) if valid_price(exit_underlying) else float("nan"),
        exit_premium=round(float(exit_premium), 2),
        exit_reason=exit_reason,
        leg_pnl_gross=round(float(gross), 2),
        txn_charges=round(float(charges), 2),
        leg_pnl_net=round(float(net), 2),
        reversal_wait_minutes=leg.reversal_wait_minutes,
        reversal_direction=leg.reversal_direction,
        reversal_candle_time=leg.reversal_candle_time,
        stop_price=round(float(leg.stop_price), 2),
        stop_candle_open=round(float(stop_candle_open), 2) if valid_price(stop_candle_open) else None,
        stop_candle_high=round(float(stop_candle_high), 2) if valid_price(stop_candle_high) else None,
    )


# =============================================================================
# DAY SIMULATION — LEG-WISE STATE MACHINE
# =============================================================================

def simulate_day_legwise(
    *,
    und: str,
    dy: date,
    expiry: date,
    day_opt: pd.DataFrame,
    underlying_day: pd.DataFrame,
) -> Tuple[List[LegTradeRow], List[Dict[str, Any]]]:
    """
    Simulate one underlying/day/nearest-expiry.

    State model:
      1. At ENTRY_TIME_IST, open ATM CE and ATM PE as independent short legs.
      2. Monitor each open leg independently using option candle high.
      3. If one leg hits its premium stop, close only that leg.
      4. A stopped PE waits for bullish full-cover reversal.
      5. A stopped CE waits for bearish full-cover reversal.
      6. On reversal confirmation, re-enter only the same option type.
      7. ATM is recalculated at re-entry using latest underlying close.
      8. CE and PE may have different active strikes.
      9. All still-open legs are closed at SESSION_END_IST.
    """
    trades: List[LegTradeRow] = []
    skipped: List[Dict[str, Any]] = []

    idx_all = build_minute_index(dy, SESSION_START_IST, SESSION_END_IST)
    entry_ts = make_ist_ts(dy, ENTRY_TIME)
    session_end_ts = make_ist_ts(dy, SESSION_END_IST)

    if entry_ts not in idx_all:
        skipped.append(
            {
                "day": dy,
                "underlying": und,
                "expiry": expiry,
                "reason": "ENTRY_TIME_IST outside session index",
            }
        )
        return trades, skipped

    if entry_ts >= session_end_ts:
        skipped.append(
            {
                "day": dy,
                "underlying": und,
                "expiry": expiry,
                "reason": "ENTRY_TIME_IST is at/after SESSION_END_IST; no meaningful intraday trade window",
            }
        )
        return trades, skipped

    try:
        ctx = DayMarketContext(day_opt=day_opt, underlying_day=underlying_day, idx_all=idx_all)
    except Exception as e:
        skipped.append(
            {
                "day": dy,
                "underlying": und,
                "expiry": expiry,
                "reason": f"Day market context preparation failed: {e}",
            }
        )
        return trades, skipped

    if not valid_price(ctx.asof_underlying_close(entry_ts)):
        skipped.append(
            {
                "day": dy,
                "underlying": und,
                "expiry": expiry,
                "reason": "No underlying close/as-of price at ENTRY_TIME_IST",
            }
        )
        return trades, skipped

    active_legs: Dict[str, ActiveLeg] = {}
    pending_reentries: Dict[str, PendingReentry] = {}
    reattempt_counts = {"CE": 0, "PE": 0}
    leg_seq_counter = 0
    missing_high_logged: set = set()

    # Initial ATM short straddle, represented as two independent short legs.
    for opt_type in ["CE", "PE"]:
        leg_seq_counter += 1

        leg = open_short_leg(
            und=und,
            dy=dy,
            expiry=expiry,
            ctx=ctx,
            option_type=opt_type,
            entry_ts=entry_ts,
            leg_seq=leg_seq_counter,
            reattempt_no=0,
            reversal_wait_minutes=None,
            reversal_direction=None,
            reversal_candle_time=None,
            skipped=skipped,
        )

        if leg is not None:
            active_legs[opt_type] = leg

    if not active_legs:
        skipped.append(
            {
                "day": dy,
                "underlying": und,
                "expiry": expiry,
                "reason": "Initial ATM CE/PE legs could not be opened",
            }
        )
        return trades, skipped

    # Monitor minute-by-minute after entry.
    start_pos = idx_all.get_loc(entry_ts)

    for ts in idx_all[start_pos + 1:]:
        stopped_this_minute: List[str] = []

        # ------------------------------------------------------------------
        # 1. Leg-wise stop detection using option HIGH.
        # ------------------------------------------------------------------
        for opt_type, leg in list(active_legs.items()):
            if ts < leg.monitor_from_ts:
                continue

            high_price = ctx.exact_option_price(
                strike=leg.strike,
                opt_type=leg.option_type,
                symbol=leg.symbol,
                price_col="high",
                ts=ts,
                ffill=False,
            )

            if not valid_price(high_price):
                log_key = (leg.leg_seq, leg.option_type, leg.symbol, leg.strike)
                if log_key not in missing_high_logged:
                    missing_high_logged.add(log_key)
                    skipped.append(
                        {
                            "day": dy,
                            "underlying": und,
                            "expiry": expiry,
                            "option_type": opt_type,
                            "leg_seq": leg.leg_seq,
                            "symbol": leg.symbol,
                            "strike": leg.strike,
                            "event_time": ts.strftime("%H:%M"),
                            "reason": "One or more option high candles are missing; stop could not be tested for those minute(s)",
                        }
                    )
                continue

            if float(high_price) >= float(leg.stop_price):
                # Conservative stop execution assumption:
                # - If candle high touches/exceeds stop_price, stop is considered hit.
                # - If option candle open is already above stop_price, exit at open.
                # - Otherwise exit at configured stop_price.
                open_price = ctx.exact_option_price(
                    strike=leg.strike,
                    opt_type=leg.option_type,
                    symbol=leg.symbol,
                    price_col="open",
                    ts=ts,
                    ffill=False,
                )

                if valid_price(open_price) and float(open_price) > float(leg.stop_price):
                    exit_premium = float(open_price)
                    exit_reason = "LEG_STOP_GAP_OPEN"
                else:
                    exit_premium = float(leg.stop_price)
                    exit_reason = "LEG_STOP"

                exit_underlying = ctx.asof_underlying_close(ts)

                trades.append(
                    close_short_leg(
                        leg=leg,
                        exit_ts=ts,
                        exit_reason=exit_reason,
                        exit_premium=exit_premium,
                        exit_underlying=exit_underlying,
                        stop_candle_open=open_price if valid_price(open_price) else None,
                        stop_candle_high=high_price,
                    )
                )

                stopped_this_minute.append(opt_type)

                if reattempt_counts[opt_type] < max_reattempts_for(opt_type) and ts < session_end_ts:
                    pending_reentries[opt_type] = PendingReentry(
                        option_type=opt_type,
                        stopped_at_ts=ts,
                        required_direction=reversal_needed_after_stop(opt_type),
                        failed_log_keys=set(),
                    )
                else:
                    skipped.append(
                        {
                            "day": dy,
                            "underlying": und,
                            "expiry": expiry,
                            "option_type": opt_type,
                            "symbol": leg.symbol,
                            "strike": leg.strike,
                            "event_time": ts.strftime("%H:%M"),
                            "reason": f"{opt_type} stopped but no re-entry scheduled because limit exhausted or session ended",
                        }
                    )

        for opt_type in stopped_this_minute:
            active_legs.pop(opt_type, None)

        # ------------------------------------------------------------------
        # 2. Re-entry trigger: underlying full-cover reversal after stop.
        # ------------------------------------------------------------------
        # A reversal is confirmed at candle close. To avoid opening and closing
        # a new leg at the same final minute, do not re-enter at SESSION_END_IST.
        if ts >= session_end_ts:
            continue

        for opt_type, pending in list(pending_reentries.items()):
            if ts <= pending.stopped_at_ts:
                continue

            prev_ts = ts - pd.Timedelta(minutes=1)
            prev_candle = ctx.underlying_candle(prev_ts)
            curr_candle = ctx.underlying_candle(ts)

            if prev_candle is None or curr_candle is None:
                if not pending.logged_missing_underlying:
                    pending.logged_missing_underlying = True
                    skipped.append(
                        {
                            "day": dy,
                            "underlying": und,
                            "expiry": expiry,
                            "option_type": opt_type,
                            "event_time": ts.strftime("%H:%M"),
                            "reason": "Underlying candle missing while waiting for reversal; that minute could not be evaluated",
                        }
                    )
                continue

            if not is_full_cover_reversal(prev_candle, curr_candle, pending.required_direction):
                continue

            if reattempt_counts[opt_type] >= max_reattempts_for(opt_type):
                pending_reentries.pop(opt_type, None)
                skipped.append(
                    {
                        "day": dy,
                        "underlying": und,
                        "expiry": expiry,
                        "option_type": opt_type,
                        "event_time": ts.strftime("%H:%M"),
                        "reason": f"{opt_type} reversal found but reattempt limit exhausted",
                    }
                )
                continue

            next_reattempt_no = reattempt_counts[opt_type] + 1
            wait_minutes = int((ts - pending.stopped_at_ts).total_seconds() // 60)
            leg_seq_counter += 1

            new_leg = open_short_leg(
                und=und,
                dy=dy,
                expiry=expiry,
                ctx=ctx,
                option_type=opt_type,
                entry_ts=ts,
                leg_seq=leg_seq_counter,
                reattempt_no=next_reattempt_no,
                reversal_wait_minutes=wait_minutes,
                reversal_direction=pending.required_direction,
                reversal_candle_time=ts.strftime("%H:%M"),
                skipped=skipped,
            )

            if new_leg is None:
                # Keep waiting for a later reversal. Avoid repeated identical logs.
                log_key = (ts.strftime("%H:%M"), pending.required_direction)
                if log_key not in pending.failed_log_keys:
                    pending.failed_log_keys.add(log_key)
                    skipped.append(
                        {
                            "day": dy,
                            "underlying": und,
                            "expiry": expiry,
                            "option_type": opt_type,
                            "event_time": ts.strftime("%H:%M"),
                            "required_reversal": pending.required_direction,
                            "reason": "Reversal found, but re-entry could not be executed due to missing option symbol/price",
                        }
                    )
                continue

            reattempt_counts[opt_type] = next_reattempt_no
            active_legs[opt_type] = new_leg
            pending_reentries.pop(opt_type, None)

    # ----------------------------------------------------------------------
    # 3. End-of-day handling: close every still-open leg.
    # ----------------------------------------------------------------------
    for opt_type, leg in list(active_legs.items()):
        eod_price = ctx.asof_option_price(
            strike=leg.strike,
            opt_type=leg.option_type,
            symbol=leg.symbol,
            price_col="close",
            ts=session_end_ts,
        )

        exit_reason = "EOD"

        if not valid_price(eod_price):
            # Ensure no open position is left in the final output.
            # The assumption is explicitly visible in both output and skipped sheet.
            eod_price = float(leg.entry_premium)
            exit_reason = "EOD_EXIT_PRICE_MISSING_ASSUMED_ENTRY"

            skipped.append(
                {
                    "day": dy,
                    "underlying": und,
                    "expiry": expiry,
                    "option_type": opt_type,
                    "leg_seq": leg.leg_seq,
                    "symbol": leg.symbol,
                    "strike": leg.strike,
                    "event_time": session_end_ts.strftime("%H:%M"),
                    "reason": "EOD option close missing; assumed exit at entry premium to avoid open leg",
                }
            )

        exit_underlying = ctx.asof_underlying_close(session_end_ts)

        trades.append(
            close_short_leg(
                leg=leg,
                exit_ts=session_end_ts,
                exit_reason=exit_reason,
                exit_premium=float(eod_price),
                exit_underlying=exit_underlying,
            )
        )

    for opt_type, pending in pending_reentries.items():
        skipped.append(
            {
                "day": dy,
                "underlying": und,
                "expiry": expiry,
                "option_type": opt_type,
                "event_time": session_end_ts.strftime("%H:%M"),
                "required_reversal": pending.required_direction,
                "reason": f"{opt_type} stopped and waited for reversal, but no executable re-entry occurred before EOD",
            }
        )

    trades.sort(key=lambda r: (r.day, r.underlying, r.entry_time, r.option_type, r.leg_seq))
    return trades, skipped


# =============================================================================
# PASS-2 — PROCESS PICKLES AND SIMULATE DAYS
# =============================================================================

def process_pickles_generate_trades(
    pickle_paths: List[str],
    min_expiry_map: Dict[Tuple[str, date], date],
    underlying_data: Dict[str, pd.DataFrame],
    window_start: date,
    window_end: date,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    all_trades: List[Dict[str, Any]] = []
    skipped_rows: List[Dict[str, Any]] = []

    # Prevent double-counting if the same day/expiry appears in more than one pickle.
    processed_day_keys: set[Tuple[str, date, date]] = set()

    for p in pickle_paths:
        try:
            df = pd.read_pickle(p)

            if not isinstance(df, pd.DataFrame) or df.empty:
                continue

            d2 = prepare_option_day_frame(df)

            if d2.empty:
                continue

            d2 = d2[(d2["day"] >= window_start) & (d2["day"] <= window_end)]

            if d2.empty:
                continue

            for (und, dy, ex), g in d2.groupby(["underlying", "day", "expiry_date"], sort=False):
                if min_expiry_map.get((und, dy)) != ex:
                    continue

                day_key = (und, dy, ex)

                if day_key in processed_day_keys:
                    skipped_rows.append(
                        {
                            "day": dy,
                            "underlying": und,
                            "expiry": ex,
                            "source_file": os.path.basename(p),
                            "reason": "Duplicate (underlying, day, expiry) encountered in multiple pickles; skipped to avoid double-count",
                        }
                    )
                    continue

                processed_day_keys.add(day_key)

                uday = underlying_data.get(und)

                if uday is None or uday.empty:
                    skipped_rows.append(
                        {
                            "day": dy,
                            "underlying": und,
                            "expiry": ex,
                            "source_file": os.path.basename(p),
                            "reason": "No downloaded underlying data",
                        }
                    )
                    continue

                uday = uday[uday["day"] == dy].copy()

                if uday.empty:
                    skipped_rows.append(
                        {
                            "day": dy,
                            "underlying": und,
                            "expiry": ex,
                            "source_file": os.path.basename(p),
                            "reason": "Underlying candles missing for day",
                        }
                    )
                    continue

                trades, skips = simulate_day_legwise(
                    und=und,
                    dy=dy,
                    expiry=ex,
                    day_opt=g.copy(),
                    underlying_day=uday,
                )

                for row in skips:
                    row.setdefault("source_file", os.path.basename(p))

                all_trades.extend([asdict(t) for t in trades])
                skipped_rows.extend(skips)

            print(f"[PASS2 OK] {os.path.basename(p)} processed")

        except Exception as e:
            msg = f"[PASS2 WARN] {os.path.basename(p)} failed: {e}"
            if FAIL_ON_PICKLE_ERROR:
                raise RuntimeError(msg) from e
            print(msg)

    all_df = pd.DataFrame(all_trades)

    if not all_df.empty:
        all_df = order_leg_columns(all_df)
        all_df = all_df.sort_values(
            ["day", "underlying", "entry_time", "option_type", "leg_seq"]
        ).reset_index(drop=True)

    skipped_df = pd.DataFrame(skipped_rows)

    if not skipped_df.empty:
        for c in ["day", "underlying", "expiry", "reason"]:
            if c not in skipped_df.columns:
                skipped_df[c] = pd.NA

        skipped_df = skipped_df.sort_values(
            ["day", "underlying"],
            na_position="last",
        ).reset_index(drop=True)

    return all_df, skipped_df


# =============================================================================
# ACTUAL TRADES — ONE UNDERLYING PER DAY, NEAREST 0/1 DTE
# =============================================================================

def pick_actual_underlying_by_day(
    min_expiry_map: Dict[Tuple[str, date], date],
) -> Dict[date, str]:
    """
    Preserve the reference convention:
    - actual trades include only 0/1 DTE;
    - one underlying per day;
    - nearest expiry wins;
    - if tied, prefer NIFTY before SENSEX.
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

    for dy, candidates in by_day.items():
        candidates = sorted(candidates, key=lambda t: (t[0], 0 if t[1] == "NIFTY" else 1))
        out[dy] = candidates[0][1]

    return out


def build_actual_legs_df(
    all_legs_df: pd.DataFrame,
    min_expiry_map: Dict[Tuple[str, date], date],
) -> pd.DataFrame:
    if all_legs_df.empty:
        return pd.DataFrame()

    actual_underlying = pick_actual_underlying_by_day(min_expiry_map)

    m = all_legs_df.copy()
    m["actual_underlying_for_day"] = m["day"].map(actual_underlying)

    m = m[m["actual_underlying_for_day"].notna()]
    m = m[m["underlying"] == m["actual_underlying_for_day"]]
    m = m[m["days_to_expiry"].isin([0, 1])]

    m = m.drop(columns=["actual_underlying_for_day"])

    return m.sort_values(
        ["day", "entry_time", "option_type", "leg_seq"]
    ).reset_index(drop=True)


def build_actual_skipped_df(
    skipped_df: pd.DataFrame,
    min_expiry_map: Dict[Tuple[str, date], date],
) -> pd.DataFrame:
    if skipped_df.empty:
        return pd.DataFrame()

    actual_underlying = pick_actual_underlying_by_day(min_expiry_map)

    s = skipped_df.copy()
    s["actual_underlying_for_day"] = s["day"].map(actual_underlying)
    s = s[s["actual_underlying_for_day"].notna()]
    s = s[s["underlying"] == s["actual_underlying_for_day"]]
    s = s.drop(columns=["actual_underlying_for_day"])

    return s.reset_index(drop=True)


# =============================================================================
# REPORTING
# =============================================================================

def order_leg_columns(df: pd.DataFrame) -> pd.DataFrame:
    wanted = [
        "day",
        "underlying",
        "expiry",
        "days_to_expiry",
        "option_type",
        "leg_seq",
        "reattempt_no",
        "strike",
        "symbol",
        "qty_units",
        "entry_time",
        "entry_underlying",
        "entry_premium",
        "exit_time",
        "exit_underlying",
        "exit_premium",
        "exit_reason",
        "leg_pnl_gross",
        "txn_charges",
        "leg_pnl_net",
        "reversal_wait_minutes",
        "reversal_direction",
        "reversal_candle_time",
        "stop_price",
        "stop_candle_open",
        "stop_candle_high",
    ]

    for c in wanted:
        if c not in df.columns:
            df[c] = pd.NA

    extra = [c for c in df.columns if c not in wanted]
    return df[wanted + extra]


def build_daily_summary(
    legs_df: pd.DataFrame,
    skipped_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Daily summary with required reporting fields and skipped/error reasons.
    Includes skip-only days as well, if present.
    """
    base_cols = [
        "day",
        "underlying",
        "daily_gross_pnl",
        "daily_net_pnl",
        "number_of_ce_entries",
        "number_of_pe_entries",
        "number_of_ce_stops",
        "number_of_pe_stops",
        "number_of_ce_reattempts",
        "number_of_pe_reattempts",
        "skip_count",
        "skipped/error reasons",
    ]

    leg_summary = pd.DataFrame()

    if not legs_df.empty:
        m = legs_df.copy()

        m["is_ce"] = m["option_type"].eq("CE")
        m["is_pe"] = m["option_type"].eq("PE")
        m["is_stop"] = m["exit_reason"].astype(str).str.upper().str.startswith("LEG_STOP")
        m["reattempt_no_num"] = pd.to_numeric(m["reattempt_no"], errors="coerce").fillna(0).astype(int)

        m["ce_entry"] = m["is_ce"].astype(int)
        m["pe_entry"] = m["is_pe"].astype(int)
        m["ce_stop"] = (m["is_ce"] & m["is_stop"]).astype(int)
        m["pe_stop"] = (m["is_pe"] & m["is_stop"]).astype(int)
        m["ce_reattempt"] = (m["is_ce"] & (m["reattempt_no_num"] > 0)).astype(int)
        m["pe_reattempt"] = (m["is_pe"] & (m["reattempt_no_num"] > 0)).astype(int)

        leg_summary = (
            m.groupby(["day", "underlying"], as_index=False)
            .agg(
                daily_gross_pnl=("leg_pnl_gross", "sum"),
                daily_net_pnl=("leg_pnl_net", "sum"),
                number_of_ce_entries=("ce_entry", "sum"),
                number_of_pe_entries=("pe_entry", "sum"),
                number_of_ce_stops=("ce_stop", "sum"),
                number_of_pe_stops=("pe_stop", "sum"),
                number_of_ce_reattempts=("ce_reattempt", "sum"),
                number_of_pe_reattempts=("pe_reattempt", "sum"),
            )
        )

    skip_summary = pd.DataFrame()

    if not skipped_df.empty and {"day", "underlying", "reason"}.issubset(skipped_df.columns):
        sk = skipped_df.copy()
        sk = sk.dropna(subset=["day", "underlying"])
        sk["reason"] = sk["reason"].astype(str)

        if not sk.empty:
            skip_summary = (
                sk.groupby(["day", "underlying"], as_index=False)
                .agg(
                    skip_count=("reason", "count"),
                    **{
                        "skipped/error reasons": (
                            "reason",
                            lambda x: " | ".join(sorted(set(x)))[:30000],
                        )
                    },
                )
            )

    if leg_summary.empty and skip_summary.empty:
        return pd.DataFrame(columns=base_cols)

    if leg_summary.empty:
        summary = skip_summary.copy()
    elif skip_summary.empty:
        summary = leg_summary.copy()
    else:
        summary = leg_summary.merge(skip_summary, on=["day", "underlying"], how="outer")

    fill_zero_cols = [
        "daily_gross_pnl",
        "daily_net_pnl",
        "number_of_ce_entries",
        "number_of_pe_entries",
        "number_of_ce_stops",
        "number_of_pe_stops",
        "number_of_ce_reattempts",
        "number_of_pe_reattempts",
        "skip_count",
    ]

    for c in fill_zero_cols:
        if c not in summary.columns:
            summary[c] = 0
        summary[c] = summary[c].fillna(0)

    count_cols = [c for c in fill_zero_cols if c not in ("daily_gross_pnl", "daily_net_pnl")]
    for c in count_cols:
        summary[c] = summary[c].astype(int)

    for c in ["daily_gross_pnl", "daily_net_pnl"]:
        summary[c] = summary[c].astype(float).round(2)

    if "skipped/error reasons" not in summary.columns:
        summary["skipped/error reasons"] = ""
    summary["skipped/error reasons"] = summary["skipped/error reasons"].fillna("")

    return summary[base_cols].sort_values(["day", "underlying"]).reset_index(drop=True)


def build_instrument_summary(legs_df: pd.DataFrame) -> pd.DataFrame:
    if legs_df.empty:
        return pd.DataFrame()

    m = legs_df.copy()

    m["is_win"] = m["leg_pnl_net"] > 0
    m["is_stop"] = m["exit_reason"].astype(str).str.upper().str.startswith("LEG_STOP")
    m["is_eod"] = m["exit_reason"].astype(str).str.upper().str.startswith("EOD")
    m["is_ce"] = m["option_type"].eq("CE")
    m["is_pe"] = m["option_type"].eq("PE")
    m["reattempt_no_num"] = pd.to_numeric(m["reattempt_no"], errors="coerce").fillna(0).astype(int)
    m["ce_entry"] = m["is_ce"].astype(int)
    m["pe_entry"] = m["is_pe"].astype(int)
    m["ce_reattempt"] = (m["is_ce"] & (m["reattempt_no_num"] > 0)).astype(int)
    m["pe_reattempt"] = (m["is_pe"] & (m["reattempt_no_num"] > 0)).astype(int)

    out = (
        m.groupby("underlying", as_index=False)
        .agg(
            legs=("leg_pnl_net", "count"),
            total_gross_pnl=("leg_pnl_gross", "sum"),
            total_net_pnl=("leg_pnl_net", "sum"),
            avg_net_pnl=("leg_pnl_net", "mean"),
            win_rate_pct=("is_win", lambda s: 100.0 * s.mean()),
            stop_rate_pct=("is_stop", lambda s: 100.0 * s.mean()),
            eod_exit_rate_pct=("is_eod", lambda s: 100.0 * s.mean()),
            ce_entries=("ce_entry", "sum"),
            pe_entries=("pe_entry", "sum"),
            ce_reattempts=("ce_reattempt", "sum"),
            pe_reattempts=("pe_reattempt", "sum"),
        )
        .sort_values("total_net_pnl", ascending=False)
        .reset_index(drop=True)
    )

    for c in [
        "total_gross_pnl",
        "total_net_pnl",
        "avg_net_pnl",
        "win_rate_pct",
        "stop_rate_pct",
        "eod_exit_rate_pct",
    ]:
        out[c] = out[c].round(2)

    return out


def build_monthwise_summary(actual_daily_df: pd.DataFrame) -> pd.DataFrame:
    if actual_daily_df.empty:
        return pd.DataFrame()

    m = actual_daily_df.copy()
    m["month"] = pd.to_datetime(m["day"]).dt.to_period("M").astype(str)
    m["is_win_day"] = m["daily_net_pnl"] > 0

    out = (
        m.groupby("month", as_index=False)
        .agg(
            trading_days=("day", "count"),
            total_net_pnl=("daily_net_pnl", "sum"),
            avg_net_pnl=("daily_net_pnl", "mean"),
            win_rate_pct=("is_win_day", lambda s: 100.0 * s.mean()),
            avg_loss_on_loss_days=(
                "daily_net_pnl",
                lambda s: float(s[s < 0].mean()) if (s < 0).any() else 0.0,
            ),
            max_loss_in_a_day=("daily_net_pnl", "min"),
            total_ce_entries=("number_of_ce_entries", "sum"),
            total_pe_entries=("number_of_pe_entries", "sum"),
            total_ce_stops=("number_of_ce_stops", "sum"),
            total_pe_stops=("number_of_pe_stops", "sum"),
            total_ce_reattempts=("number_of_ce_reattempts", "sum"),
            total_pe_reattempts=("number_of_pe_reattempts", "sum"),
        )
        .reset_index(drop=True)
    )

    for c in [
        "total_net_pnl",
        "avg_net_pnl",
        "win_rate_pct",
        "avg_loss_on_loss_days",
        "max_loss_in_a_day",
    ]:
        out[c] = out[c].round(2)

    return out


def _autosize_columns_safe(ws) -> None:
    try:
        for col_idx in range(1, (ws.max_column or 0) + 1):
            col_letter = ws.cell(row=1, column=col_idx).column_letter
            max_len = 0

            for row_idx in range(1, min(ws.max_row or 1, 2000) + 1):
                value = ws.cell(row=row_idx, column=col_idx).value
                if value is None:
                    continue
                max_len = max(max_len, len(str(value)))

            ws.column_dimensions[col_letter].width = min(60, max(10, max_len + 2))
    except Exception:
        return


def write_excel(
    all_legs_df: pd.DataFrame,
    actual_legs_df: pd.DataFrame,
    skipped_df: pd.DataFrame,
    actual_skipped_df: pd.DataFrame,
) -> None:
    out_dir = os.path.dirname(os.path.abspath(OUTPUT_XLSX))

    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    daily_summary = build_daily_summary(all_legs_df, skipped_df)
    actual_daily_summary = build_daily_summary(actual_legs_df, actual_skipped_df)
    instrument_summary = build_instrument_summary(all_legs_df)
    monthwise_summary = build_monthwise_summary(actual_daily_summary)

    config_df = pd.DataFrame(
        [
            {"parameter": "PICKLES_DIR", "value": PICKLES_DIR},
            {"parameter": "ENTRY_TIME_IST", "value": ENTRY_TIME_IST},
            {"parameter": "SESSION_START_IST", "value": SESSION_START_IST.strftime("%H:%M")},
            {"parameter": "SESSION_END_IST", "value": SESSION_END_IST.strftime("%H:%M")},
            {"parameter": "LEG_STOP_PCT", "value": LEG_STOP_PCT},
            {"parameter": "MAX_CE_REATTEMPTS", "value": MAX_CE_REATTEMPTS},
            {"parameter": "MAX_PE_REATTEMPTS", "value": MAX_PE_REATTEMPTS},
            {"parameter": "QTY_UNITS", "value": str(QTY_UNITS)},
            {"parameter": "STRIKE_STEP", "value": str(STRIKE_STEP)},
            {"parameter": "LOOKBACK_MONTHS", "value": LOOKBACK_MONTHS},
            {"parameter": "STOP_DETECTION", "value": "Option candle high >= stop_price"},
            {"parameter": "STOP_EXECUTION", "value": "max(stop_price, option candle open if open > stop_price)"},
            {"parameter": "REENTRY_LOGIC", "value": "No fixed delay; same-leg re-entry only after full-cover underlying reversal candle"},
            {"parameter": "INCLUDE_TRANSACTION_COSTS", "value": INCLUDE_TRANSACTION_COSTS},
            {"parameter": "BROKERAGE_PER_ORDER", "value": BROKERAGE_PER_ORDER},
            {"parameter": "STT_SELL_PCT", "value": STT_SELL_PCT},
            {"parameter": "EXCHANGE_TXN_PCT", "value": EXCHANGE_TXN_PCT},
            {"parameter": "SEBI_PER_CRORE", "value": SEBI_PER_CRORE},
            {"parameter": "STAMP_BUY_PCT", "value": STAMP_BUY_PCT},
            {"parameter": "IPFT_PER_CRORE", "value": IPFT_PER_CRORE},
            {"parameter": "GST_PCT", "value": GST_PCT},
            {"parameter": "OUTPUT_XLSX", "value": OUTPUT_XLSX},
        ]
    )

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as xw:
        all_legs_df.to_excel(xw, sheet_name="all_legs_backtested", index=False)
        actual_legs_df.to_excel(xw, sheet_name="actual_legs", index=False)
        daily_summary.to_excel(xw, sheet_name="daily_summary", index=False)
        actual_daily_summary.to_excel(xw, sheet_name="actual_daily_summary", index=False)
        monthwise_summary.to_excel(xw, sheet_name="monthwise_summary", index=False)
        instrument_summary.to_excel(xw, sheet_name="instrument_summary", index=False)
        skipped_df.to_excel(xw, sheet_name="skipped", index=False)
        actual_skipped_df.to_excel(xw, sheet_name="actual_skipped", index=False)
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
    paths = sorted(
        glob.glob(os.path.join(PICKLES_DIR, "*.pkl"))
        + glob.glob(os.path.join(PICKLES_DIR, "*.pickle"))
    )

    if not paths:
        raise FileNotFoundError(f"No .pkl/.pickle files found in: {PICKLES_DIR}")

    print(f"[INFO] Pickles found: {len(paths)}")
    print("[INFO] No fixed re-entry delay is used. Re-entry is reversal-candle based only.")
    print(
        f"[INFO] Entry={ENTRY_TIME_IST} | "
        f"LegStopPct={LEG_STOP_PCT} | "
        f"MaxCE={MAX_CE_REATTEMPTS} | "
        f"MaxPE={MAX_PE_REATTEMPTS}"
    )
    print(f"[INFO] Output={OUTPUT_XLSX}")

    end_day, min_expiry_map, min_day_seen = scan_pickles_pass1(paths)
    window_start = compute_window_start(end_day, LOOKBACK_MONTHS)

    print(f"[INFO] Data day-range seen: {min_day_seen} -> {end_day}")
    print(f"[INFO] Backtest window: {window_start} -> {end_day}")
    print(f"[INFO] Tradeables: {sorted(TRADEABLE)}")

    print("[STEP] Initializing Kite ...")
    kite = oUtils.intialize_kite_api()
    print("[OK] Kite ready.")

    underlying_data = download_underlyings(kite, window_start, end_day)

    all_legs_df, skipped_df = process_pickles_generate_trades(
        paths,
        min_expiry_map,
        underlying_data,
        window_start,
        end_day,
    )

    actual_legs_df = build_actual_legs_df(all_legs_df, min_expiry_map)
    actual_skipped_df = build_actual_skipped_df(skipped_df, min_expiry_map)

    write_excel(all_legs_df, actual_legs_df, skipped_df, actual_skipped_df)

    if not all_legs_df.empty:
        console_summary = (
            all_legs_df.groupby("underlying", as_index=False)
            .agg(
                legs=("leg_pnl_net", "count"),
                total_net_pnl=("leg_pnl_net", "sum"),
                avg_net_pnl=("leg_pnl_net", "mean"),
            )
        )
        console_summary["total_net_pnl"] = console_summary["total_net_pnl"].round(2)
        console_summary["avg_net_pnl"] = console_summary["avg_net_pnl"].round(2)
        print(console_summary)
    else:
        print("[WARN] No completed legs. Check the skipped sheet for reasons.")


if __name__ == "__main__":
    main()