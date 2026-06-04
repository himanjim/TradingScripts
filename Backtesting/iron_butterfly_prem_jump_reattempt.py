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
# Strategy: Iron Butterfly
# Entry:  SELL ATM CE + SELL ATM PE, and BUY OTM CE + BUY OTM PE as hedge wings.
# Exit:   Square off all four legs together.
#
# Wing distance can be configured in two ways:
#   1) Common strike-step distance for all instruments:
#        IRON_BFLY_WING_DISTANCE_STEPS=5
#      This means 5 strike intervals away from ATM. Example:
#        NIFTY  step=50  -> 250 points away
#        SENSEX step=100 -> 500 points away
#
#   2) Per-underlying point override:
#        NIFTY_WING_DISTANCE_POINTS=300
#        SENSEX_WING_DISTANCE_POINTS=700
#      If the override is not a clean strike multiple, it is rounded to nearest
#      valid strike step during simulation.
# =============================================================================
PICKLES_DIR = r"G:\My Drive\Trading\Historical_Options_Data"
ENTRY_TIME_IST = os.getenv("ENTRY_TIME_IST", "09:20")  # "HH:MM"

def _safe_fname_part(s: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in s)

def _get_downloads_folder() -> str:
    """
    Returns the current user's default Downloads folder.
    Falls back to home directory if Downloads is not found.
    """
    downloads = Path.home() / "Downloads"
    return str(downloads if downloads.exists() else Path.home())

LOSS_LIMIT_RUPEES = int(os.getenv("LOSS_LIMIT_RUPEES", "20000"))
PROFIT_PROTECT_TRIGGER_RUPEES = int(os.getenv("PROFIT_PROTECT_TRIGGER_RUPEES", "40000"))
MAX_REATTEMPTS = int(os.getenv("MAX_REATTEMPTS", "4"))  # 1 = only one re-entry after first trade
REENTRY_DELAY_MINUTES = int(os.getenv("REENTRY_DELAY_MINUTES", "10"))

# Default hedge-wing distance = this many strike steps away from ATM.
# Example with default 5: NIFTY = 5*50 = 250 points; SENSEX = 5*100 = 500 points.
IRON_BFLY_WING_DISTANCE_STEPS = int(os.getenv("IRON_BFLY_WING_DISTANCE_STEPS", "5"))

_DEFAULT_OUT = os.path.join(
    _get_downloads_folder(),
    f"iron_butterfly_checked_backtest_reattempt{_safe_fname_part(ENTRY_TIME_IST)}"
    f"_WSTEPS_{_safe_fname_part(str(IRON_BFLY_WING_DISTANCE_STEPS))}"
    f"_LL_{_safe_fname_part(str(LOSS_LIMIT_RUPEES))}"
    f"_PPT_{_safe_fname_part(str(PROFIT_PROTECT_TRIGGER_RUPEES))}"
    f"_MR_{_safe_fname_part(str(MAX_REATTEMPTS))}"
    f"_RDM_{_safe_fname_part(str(REENTRY_DELAY_MINUTES))}.xlsx"
)

OUTPUT_XLSX = os.getenv("OUTPUT_XLSX", _DEFAULT_OUT)

FAIL_ON_PICKLE_ERROR = os.getenv("FAIL_ON_PICKLE_ERROR", "0").strip() == "1"

SESSION_START_IST = dtime(9, 15)
SESSION_END_IST = dtime(15, 30)

LOOKBACK_MONTHS = int(os.getenv("LOOKBACK_MONTHS", "12"))

QTY_UNITS = {"NIFTY": 325 * 7, "SENSEX": 100 * 7}
TRADEABLE = set(QTY_UNITS.keys())

STRIKE_STEP = {"NIFTY": 50, "SENSEX": 100}

# =============================================================================
# TRANSACTION CHARGES (Zerodha F&O Options — NSE)
# =============================================================================
# Each Iron Butterfly attempt has 4 legs at entry + 4 legs at exit = 8 executed orders.
BROKERAGE_PER_ORDER       = 20.0       # ₹20 flat per executed order
ORDERS_PER_TRADE          = 8          # entry 4 legs + exit 4 legs
STT_SELL_PCT              = 0.001      # 0.1% on sell-side premium
EXCHANGE_TXN_PCT          = 0.0003553  # 0.03553% on premium (NSE options)
SEBI_PER_CRORE            = 10.0       # ₹10 per crore of turnover
STAMP_BUY_PCT             = 0.00003    # 0.003% on buy-side premium
IPFT_PER_CRORE            = 0.010      # ₹0.01 per crore (on premium)
GST_PCT                   = 0.18       # 18% on (brokerage + txn charges + SEBI)
INCLUDE_TRANSACTION_COSTS = True       # set False to disable

UNDERLYING_KITE = {
    "NIFTY": {"exchange": "NSE", "tradingsymbol": "NIFTY 50"},
    "SENSEX": {"exchange": "BSE", "tradingsymbol": "SENSEX"},
}

MAX_DAYS_PER_CHUNK = 25
MAX_ATTEMPTS = 5
SLEEP_BETWEEN_CALLS_SEC = 0.20


# =============================================================================
# HELPERS
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

def ensure_ist(series_or_scalar) -> Any:
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
    return int(round(x / step) * step)

def get_wing_distance_points(underlying: str) -> int:
    """
    Return the configured Iron Butterfly wing distance in index points.

    Priority:
      1. <UNDERLYING>_WING_DISTANCE_POINTS, e.g. NIFTY_WING_DISTANCE_POINTS=300
      2. IRON_BFLY_WING_DISTANCE_STEPS * STRIKE_STEP[underlying]

    The final distance is rounded to the nearest valid strike step and never
    allowed to be less than one strike step.
    """
    und = underlying.upper().strip()
    step = int(STRIKE_STEP[und])

    override = os.getenv(f"{und}_WING_DISTANCE_POINTS", "").strip()
    raw_distance = int(override) if override else int(IRON_BFLY_WING_DISTANCE_STEPS) * step

    # Keep the wing on a valid listed strike.
    rounded = round_to_step(abs(float(raw_distance)), step)
    return max(step, int(rounded))

def build_minute_index(day_d: date, start_t: dtime, end_t: dtime) -> pd.DatetimeIndex:
    tz = ist_tz()
    start = pd.Timestamp(datetime.combine(day_d, start_t), tz=tz)
    end = pd.Timestamp(datetime.combine(day_d, end_t), tz=tz)
    return pd.date_range(start=start, end=end, freq="1min")

def asof_close(df: pd.DataFrame, ts: pd.Timestamp) -> float:
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
    if relativedelta is not None:
        return (pd.Timestamp(end_day) - relativedelta(months=months)).date()
    return (pd.Timestamp(end_day) - pd.Timedelta(days=30 * months)).date()

# =============================================================================
# TRANSACTION COST CALCULATOR
# =============================================================================
def compute_trade_charges(
    *,
    entry_short_ce: float,
    entry_short_pe: float,
    entry_long_ce: float,
    entry_long_pe: float,
    exit_short_ce: float,
    exit_short_pe: float,
    exit_long_ce: float,
    exit_long_pe: float,
    qty: int,
) -> float:
    """
    Compute total Zerodha-style transaction charges for one Iron Butterfly attempt.

    Iron Butterfly legs:
      Entry:
        SELL short ATM CE
        SELL short ATM PE
        BUY  long OTM CE hedge wing
        BUY  long OTM PE hedge wing

      Exit:
        BUY  back short ATM CE
        BUY  back short ATM PE
        SELL long OTM CE hedge wing
        SELL long OTM PE hedge wing

    Important cost treatment:
      - Brokerage: 8 executed orders.
      - STT: on sell-side premium only. For Iron Butterfly, sell-side includes
        entry short legs and exit sale of long hedge wings.
      - Stamp duty: on buy-side premium only. For Iron Butterfly, buy-side includes
        entry long hedge wings and exit buyback of short legs.
      - Exchange transaction charge, SEBI, IPFT: on total premium turnover.
      - GST: on brokerage + exchange transaction charge + SEBI charge.

    Returns total charges in rupees (always positive).
    """
    if not INCLUDE_TRANSACTION_COSTS:
        return 0.0

    # Premium received on sell orders.
    sell_turnover = (
        entry_short_ce + entry_short_pe + exit_long_ce + exit_long_pe
    ) * qty

    # Premium paid on buy orders.
    buy_turnover = (
        entry_long_ce + entry_long_pe + exit_short_ce + exit_short_pe
    ) * qty

    total_turnover = sell_turnover + buy_turnover

    brokerage = BROKERAGE_PER_ORDER * ORDERS_PER_TRADE
    stt = sell_turnover * STT_SELL_PCT
    txn_charges = total_turnover * EXCHANGE_TXN_PCT
    sebi = total_turnover * SEBI_PER_CRORE / 1_00_00_000
    stamp = buy_turnover * STAMP_BUY_PCT
    ipft = total_turnover * IPFT_PER_CRORE / 1_00_00_000
    gst = (brokerage + txn_charges + sebi) * GST_PCT

    total_charges = brokerage + stt + txn_charges + sebi + stamp + ipft + gst
    return round(float(total_charges), 2)

# =============================================================================
# Kite historical helpers
# =============================================================================
def _iter_chunks_by_date(from_dt: datetime, to_dt: datetime, days_per_chunk: int) -> List[Tuple[datetime, datetime]]:
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

def get_instrument_token(kite, exchange: str, tradingsymbol: str, cache: Dict[str, List[Dict]]) -> int:
    ex = exchange.upper().strip()
    wanted = tradingsymbol.strip().upper()
    for r in _kite_instruments_cached(kite, ex, cache):
        if str(r.get("tradingsymbol", "")).upper() == wanted:
            return int(r["instrument_token"])
    raise ValueError(f"Instrument not found on {ex}: '{tradingsymbol}'")

def fetch_history_minute(kite, instrument_token: int, from_dt: datetime, to_dt: datetime, label: str) -> List[Dict]:
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
    day: date
    underlying: str
    trade_seq: int
    expiry: date
    days_to_expiry: int

    # Iron Butterfly structure
    atm_strike: int
    wing_distance_points: int
    buy_pe_strike: int
    buy_ce_strike: int
    qty_units: int

    # Timing and exit classification
    entry_time: str
    exit_time: str
    exit_reason: str
    entry_underlying: float

    # Four option instruments
    short_ce_symbol: str
    short_pe_symbol: str
    long_ce_symbol: str
    long_pe_symbol: str

    # Entry prices
    entry_short_ce: float
    entry_short_pe: float
    entry_long_ce: float
    entry_long_pe: float

    # Exit prices
    exit_short_ce: float
    exit_short_pe: float
    exit_long_ce: float
    exit_long_pe: float

    # Iron Butterfly economics, before transaction charges
    entry_net_credit: float
    exit_net_debit: float
    max_profit_possible: float
    max_loss_possible: float

    # Diagnostics:
    # - exit_pnl_close_based is the P&L obtained from the stored exit close prices.
    # - stoploss_trigger_pnl is the worst directional intraminute P&L that triggered SL.
    # These prevent confusion when STOPLOSS rows are booked exactly at LOSS_LIMIT_RUPEES.
    exit_pnl_close_based: float
    stoploss_trigger_pnl: float

    # Backtest results
    exit_pnl_gross: float   # Booked gross P&L before charges
    txn_charges: float      # total transaction charges for this attempt
    exit_pnl: float         # net P&L after deducting charges
    eod_pnl: float
    max_profit: float
    max_loss: float


# =============================================================================
# PASS-1: nearest expiry per (underlying, day)
# =============================================================================
def scan_pickles_pass1(pickle_paths: List[str]) -> Tuple[date, Dict[Tuple[str, date], date], date]:
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
# Underlying download
# =============================================================================
def download_underlyings(kite, day_start: date, day_end: date) -> Dict[str, pd.DataFrame]:
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
# Simulation helpers
# =============================================================================
def _pick_symbol(day_opt: pd.DataFrame, strike: int, opt_type: str) -> Optional[str]:
    sub = day_opt[(day_opt["strike_int"] == strike) & (day_opt["option_type"] == opt_type)]
    if sub.empty:
        return None
    syms = sorted(sub["instrument"].astype(str).unique().tolist())
    return syms[0] if syms else None

def _build_leg_series(day_opt: pd.DataFrame, idx_all: pd.DatetimeIndex,
                      strike: int, opt_type: str, symbol: str,
                      price_col: str = "close", do_ffill: bool = True) -> pd.Series:
    sub = day_opt[
        (day_opt["strike_int"] == strike) &
        (day_opt["option_type"] == opt_type) &
        (day_opt["instrument"].astype(str) == symbol)
    ][["date", price_col]].dropna()

    if sub.empty:
        return pd.Series(index=idx_all, dtype="float64")

    sub = sub.copy()
    sub["date"] = ensure_ist(sub["date"])
    sub = sub.sort_values("date").drop_duplicates(subset=["date"], keep="last").set_index("date")
    s = sub[price_col].astype(float).reindex(idx_all)
    return s.ffill() if do_ffill else s

def simulate_day_multi_trades(
    *,
    und: str,
    dy: date,
    expiry: date,
    day_opt: pd.DataFrame,
    underlying_day: pd.DataFrame,
) -> Tuple[List[TradeRow], List[Dict[str, Any]]]:
    """
    Simulate one or more intraday Iron Butterfly attempts for one underlying/day.

    Entry rule:
      - Find the underlying close at ENTRY_TIME_IST.
      - Round it to nearest strike to select ATM.
      - SELL ATM CE and ATM PE.
      - BUY OTM CE at ATM + configured wing distance.
      - BUY OTM PE at ATM - configured wing distance.

    Exit rule:
      - Exit all 4 legs together at the earliest of:
          1) configured gross stop-loss,
          2) profit-protection giveback after reaching configured profit,
          3) EOD.
      - If STOPLOSS or PROFIT_PROTECT occurs, re-enter after REENTRY_DELAY_MINUTES,
        up to MAX_REATTEMPTS.

    P&L convention:
      - Short leg P&L = entry premium - current premium.
      - Long leg P&L  = current premium - entry premium.
      - Net gross P&L = sum of all four leg P&Ls * qty.
    """

    results: List[TradeRow] = []
    skipped: List[Dict[str, Any]] = []

    idx_all = build_minute_index(dy, SESSION_START_IST, SESSION_END_IST)
    session_end_ts = idx_all[-1]

    qty = int(QTY_UNITS[und])
    step = int(STRIKE_STEP[und])
    wing_distance_points = get_wing_distance_points(und)

    G = float(PROFIT_PROTECT_TRIGGER_RUPEES)
    profit_protect_enabled = G > 0

    cur_entry_ts = pd.Timestamp(datetime.combine(dy, ENTRY_TIME), tz=ist_tz())
    trade_seq = 1

    while cur_entry_ts <= session_end_ts:
        u_px = asof_close(underlying_day, cur_entry_ts)
        if pd.isna(u_px):
            skipped.append({"day": dy, "underlying": und, "expiry": expiry, "trade_seq": trade_seq,
                            "reason": f"No underlying price at entry {cur_entry_ts.strftime('%H:%M')}"})
            break

        atm = round_to_step(float(u_px), step)
        buy_ce_strike = int(atm + wing_distance_points)
        buy_pe_strike = int(atm - wing_distance_points)

        if buy_pe_strike <= 0:
            skipped.append({"day": dy, "underlying": und, "expiry": expiry, "trade_seq": trade_seq,
                            "atm_strike": atm, "wing_distance_points": wing_distance_points,
                            "reason": "Configured wing distance makes PE strike <= 0"})
            break

        # Pick exact option symbols for all four Iron Butterfly legs.
        short_ce_sym = _pick_symbol(day_opt, atm, "CE")
        short_pe_sym = _pick_symbol(day_opt, atm, "PE")
        long_ce_sym = _pick_symbol(day_opt, buy_ce_strike, "CE")
        long_pe_sym = _pick_symbol(day_opt, buy_pe_strike, "PE")

        missing_legs = []
        if not short_ce_sym:
            missing_legs.append(f"short ATM CE {atm}")
        if not short_pe_sym:
            missing_legs.append(f"short ATM PE {atm}")
        if not long_ce_sym:
            missing_legs.append(f"long CE wing {buy_ce_strike}")
        if not long_pe_sym:
            missing_legs.append(f"long PE wing {buy_pe_strike}")

        if missing_legs:
            skipped.append({
                "day": dy,
                "underlying": und,
                "expiry": expiry,
                "trade_seq": trade_seq,
                "atm_strike": atm,
                "wing_distance_points": wing_distance_points,
                "buy_pe_strike": buy_pe_strike,
                "buy_ce_strike": buy_ce_strike,
                "reason": "Iron Butterfly legs not available in pickle band: " + ", ".join(missing_legs),
            })
            break

        # Raw close series: used as the base series.
        short_ce_close_raw = _build_leg_series(day_opt, idx_all, atm, "CE", short_ce_sym, "close", do_ffill=False)
        short_pe_close_raw = _build_leg_series(day_opt, idx_all, atm, "PE", short_pe_sym, "close", do_ffill=False)
        long_ce_close_raw = _build_leg_series(day_opt, idx_all, buy_ce_strike, "CE", long_ce_sym, "close", do_ffill=False)
        long_pe_close_raw = _build_leg_series(day_opt, idx_all, buy_pe_strike, "PE", long_pe_sym, "close", do_ffill=False)

        # Forward-filled close series: used for entry pricing, tracking, and reporting.
        # This avoids skipping a trade only because an OTM hedge wing did not trade
        # exactly at ENTRY_TIME_IST. It does not use future prices; it only carries
        # forward the last available traded close up to that minute.
        short_ce_close = short_ce_close_raw.ffill()
        short_pe_close = short_pe_close_raw.ffill()
        long_ce_close = long_ce_close_raw.ffill()
        long_pe_close = long_pe_close_raw.ffill()

        # High/low series for conservative intraminute stop-loss detection.
        # IMPORTANT FIX:
        #   Do NOT combine short CE high + short PE high + long CE low + long PE low
        #   in one synthetic candle. CE high and PE high usually belong to opposite
        #   market directions and may not occur at the same instant. That creates a
        #   fake worst-case loss and can trigger false STOPLOSS rows.
        #
        # We therefore build two directionally consistent candidates:
        #   1) Up-move candidate: CE premiums at high, PE premiums at low.
        #   2) Down-move candidate: CE premiums at low, PE premiums at high.
        #
        # Missing high/low values are replaced with forward-filled close to keep the
        # stop-loss check robust for illiquid hedge-wing minutes.
        short_ce_high = _build_leg_series(day_opt, idx_all, atm, "CE", short_ce_sym, "high", do_ffill=False).combine_first(short_ce_close)
        short_ce_low  = _build_leg_series(day_opt, idx_all, atm, "CE", short_ce_sym, "low",  do_ffill=False).combine_first(short_ce_close)
        short_pe_high = _build_leg_series(day_opt, idx_all, atm, "PE", short_pe_sym, "high", do_ffill=False).combine_first(short_pe_close)
        short_pe_low  = _build_leg_series(day_opt, idx_all, atm, "PE", short_pe_sym, "low",  do_ffill=False).combine_first(short_pe_close)

        long_ce_high = _build_leg_series(day_opt, idx_all, buy_ce_strike, "CE", long_ce_sym, "high", do_ffill=False).combine_first(long_ce_close)
        long_ce_low  = _build_leg_series(day_opt, idx_all, buy_ce_strike, "CE", long_ce_sym, "low",  do_ffill=False).combine_first(long_ce_close)
        long_pe_high = _build_leg_series(day_opt, idx_all, buy_pe_strike, "PE", long_pe_sym, "high", do_ffill=False).combine_first(long_pe_close)
        long_pe_low  = _build_leg_series(day_opt, idx_all, buy_pe_strike, "PE", long_pe_sym, "low",  do_ffill=False).combine_first(long_pe_close)

        if cur_entry_ts not in idx_all:
            skipped.append({"day": dy, "underlying": und, "expiry": expiry, "trade_seq": trade_seq,
                            "reason": "Entry timestamp not in session index"})
            break

        entry_short_ce = short_ce_close.loc[cur_entry_ts]
        entry_short_pe = short_pe_close.loc[cur_entry_ts]
        entry_long_ce = long_ce_close.loc[cur_entry_ts]
        entry_long_pe = long_pe_close.loc[cur_entry_ts]

        monitor_start_ts = pd.Timestamp(cur_entry_ts) + pd.Timedelta(minutes=1)
        if monitor_start_ts > session_end_ts:
            break

        if any(pd.isna(x) for x in [entry_short_ce, entry_short_pe, entry_long_ce, entry_long_pe]):
            skipped.append({
                "day": dy,
                "underlying": und,
                "expiry": expiry,
                "trade_seq": trade_seq,
                "atm_strike": atm,
                "wing_distance_points": wing_distance_points,
                "buy_pe_strike": buy_pe_strike,
                "buy_ce_strike": buy_ce_strike,
                "reason": "No price at or before entry for one or more Iron Butterfly legs",
            })
            break

        entry_short_ce = float(entry_short_ce)
        entry_short_pe = float(entry_short_pe)
        entry_long_ce = float(entry_long_ce)
        entry_long_pe = float(entry_long_pe)

        # Net credit received per unit at entry.
        # For a normal credit Iron Butterfly this should be positive.
        entry_net_credit = (entry_short_ce + entry_short_pe) - (entry_long_ce + entry_long_pe)

        # Theoretical gross limits for a symmetric Iron Butterfly at expiry.
        # Actual intraday MTM can differ due to IV, spreads, and wing liquidity.
        max_profit_possible = entry_net_credit * qty
        max_loss_possible = max(0.0, (float(wing_distance_points) - entry_net_credit) * qty)

        # Close-based P&L series used for profit-protection, EOD P&L, and reporting.
        pnl_close_all = (
            (entry_short_ce - short_ce_close) +
            (entry_short_pe - short_pe_close) +
            (long_ce_close - entry_long_ce) +
            (long_pe_close - entry_long_pe)
        ) * qty
        pnl = pnl_close_all.loc[monitor_start_ts:].dropna()

        # Conservative but directionally consistent STOPLOSS P&L.
        # Candidate A: underlying moves up inside the minute.
        #   CE premiums are marked at high; PE premiums are marked at low.
        pnl_sl_up_all = (
            (entry_short_ce - short_ce_high) +
            (entry_short_pe - short_pe_low) +
            (long_ce_high - entry_long_ce) +
            (long_pe_low - entry_long_pe)
        ) * qty

        # Candidate B: underlying moves down inside the minute.
        #   CE premiums are marked at low; PE premiums are marked at high.
        pnl_sl_down_all = (
            (entry_short_ce - short_ce_low) +
            (entry_short_pe - short_pe_high) +
            (long_ce_low - entry_long_ce) +
            (long_pe_high - entry_long_pe)
        ) * qty

        # Use the worst among close-based P&L and the two directional candidates.
        # This remains conservative but avoids impossible CE-high + PE-high mixing.
        pnl_sl_all = pd.concat([pnl_close_all, pnl_sl_up_all, pnl_sl_down_all], axis=1).min(axis=1)
        pnl_sl = pnl_sl_all.loc[monitor_start_ts:].dropna()

        if pnl.empty:
            skipped.append({"day": dy, "underlying": und, "expiry": expiry, "trade_seq": trade_seq,
                            "atm_strike": atm, "wing_distance_points": wing_distance_points,
                            "reason": "PnL series empty after entry"})
            break

        eod_ts = pnl.index[-1]
        eod_pnl = float(pnl.iloc[-1])

        max_profit = float(max(0.0, pnl.max()))
        max_loss = float(min(0.0, pnl.min()))

        stop_hit = pnl_sl <= -LOSS_LIMIT_RUPEES
        stop_ts = pnl_sl.index[stop_hit.to_numpy().argmax()] if stop_hit.any() else None

        protect_ts = None
        if profit_protect_enabled:
            peak = pnl.cummax()
            armed = peak >= G
            trail = peak - G
            protect_hit = armed & (pnl <= trail)
            protect_ts = pnl.index[protect_hit.to_numpy().argmax()] if protect_hit.any() else None

        exit_ts = eod_ts
        exit_reason = "EOD"
        if stop_ts is not None and protect_ts is not None:
            if stop_ts < protect_ts:
                exit_ts, exit_reason = stop_ts, "STOPLOSS"
            elif protect_ts < stop_ts:
                exit_ts, exit_reason = protect_ts, "PROFIT_PROTECT"
            else:
                exit_ts, exit_reason = stop_ts, "STOPLOSS"
        elif stop_ts is not None:
            exit_ts, exit_reason = stop_ts, "STOPLOSS"
        elif protect_ts is not None:
            exit_ts, exit_reason = protect_ts, "PROFIT_PROTECT"

        # Close-based P&L at the selected exit timestamp. This is exactly what
        # the stored exit leg prices imply. For STOPLOSS rows, the booked P&L may
        # intentionally differ from this value because minute OHLC does not reveal
        # the exact fill price at the stop threshold.
        exit_pnl_close_based = float(pnl.loc[exit_ts])
        stoploss_trigger_pnl = float(pnl_sl.loc[exit_ts]) if exit_ts in pnl_sl.index else float("nan")

        if exit_reason == "STOPLOSS":
            # Conservative stop-fill convention: once the directional intraminute
            # worst-case breaches the stop, book gross P&L exactly at the configured
            # stop amount. See exit_pnl_close_based and stoploss_trigger_pnl columns
            # for diagnostics.
            exit_pnl_gross = -float(LOSS_LIMIT_RUPEES)
        else:
            exit_pnl_gross = exit_pnl_close_based

        exit_short_ce = float(short_ce_close.loc[exit_ts]) if pd.notna(short_ce_close.loc[exit_ts]) else float("nan")
        exit_short_pe = float(short_pe_close.loc[exit_ts]) if pd.notna(short_pe_close.loc[exit_ts]) else float("nan")
        exit_long_ce = float(long_ce_close.loc[exit_ts]) if pd.notna(long_ce_close.loc[exit_ts]) else float("nan")
        exit_long_pe = float(long_pe_close.loc[exit_ts]) if pd.notna(long_pe_close.loc[exit_ts]) else float("nan")

        # Exit debit per unit. A negative value is possible if the long wings are
        # worth more than the short ATM legs at exit.
        exit_net_debit = (exit_short_ce + exit_short_pe) - (exit_long_ce + exit_long_pe)

        txn_charges = compute_trade_charges(
            entry_short_ce=entry_short_ce,
            entry_short_pe=entry_short_pe,
            entry_long_ce=entry_long_ce,
            entry_long_pe=entry_long_pe,
            exit_short_ce=exit_short_ce if not pd.isna(exit_short_ce) else 0.0,
            exit_short_pe=exit_short_pe if not pd.isna(exit_short_pe) else 0.0,
            exit_long_ce=exit_long_ce if not pd.isna(exit_long_ce) else 0.0,
            exit_long_pe=exit_long_pe if not pd.isna(exit_long_pe) else 0.0,
            qty=qty,
        )
        exit_pnl = exit_pnl_gross - txn_charges

        dte = int((expiry - dy).days)

        results.append(
            TradeRow(
                day=dy,
                underlying=und,
                trade_seq=trade_seq,
                expiry=expiry,
                days_to_expiry=dte,
                atm_strike=int(atm),
                wing_distance_points=int(wing_distance_points),
                buy_pe_strike=int(buy_pe_strike),
                buy_ce_strike=int(buy_ce_strike),
                qty_units=qty,
                entry_time=pd.Timestamp(cur_entry_ts).strftime("%H:%M"),
                exit_time=pd.Timestamp(exit_ts).strftime("%H:%M"),
                exit_reason=exit_reason,
                entry_underlying=float(u_px),
                short_ce_symbol=short_ce_sym,
                short_pe_symbol=short_pe_sym,
                long_ce_symbol=long_ce_sym,
                long_pe_symbol=long_pe_sym,
                entry_short_ce=entry_short_ce,
                entry_short_pe=entry_short_pe,
                entry_long_ce=entry_long_ce,
                entry_long_pe=entry_long_pe,
                exit_short_ce=exit_short_ce,
                exit_short_pe=exit_short_pe,
                exit_long_ce=exit_long_ce,
                exit_long_pe=exit_long_pe,
                entry_net_credit=float(entry_net_credit),
                exit_net_debit=float(exit_net_debit),
                max_profit_possible=float(max_profit_possible),
                max_loss_possible=float(max_loss_possible),
                exit_pnl_close_based=float(exit_pnl_close_based),
                stoploss_trigger_pnl=float(stoploss_trigger_pnl),
                exit_pnl_gross=float(exit_pnl_gross),
                txn_charges=float(txn_charges),
                exit_pnl=float(exit_pnl),
                eod_pnl=float(eod_pnl),
                max_profit=float(max_profit),
                max_loss=float(max_loss),
            )
        )

        if exit_reason in ("STOPLOSS", "PROFIT_PROTECT") and (trade_seq - 1) < MAX_REATTEMPTS:
            trade_seq += 1
            cur_entry_ts = pd.Timestamp(exit_ts) + pd.Timedelta(minutes=REENTRY_DELAY_MINUTES)
            if cur_entry_ts > session_end_ts:
                break
            continue

        break

    return results, skipped


# =============================================================================
# PASS-2: process each pickle and simulate trades for days where this expiry is nearest
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

    # IMPORTANT: prevent double-count if same (und,day,expiry) appears in multiple files
    processed_day_keys: set[Tuple[str, date, date]] = set()

    for p in pickle_paths:
        try:
            df = pd.read_pickle(p)
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue

            needed_cols = ["date", "name", "type", "option_type", "strike", "expiry", "instrument", "high", "low", "close"]
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
            d2["strike_int"] = d2["strike_num"].round().astype("Int64")  # safer than truncation
            d2["option_type"] = d2["option_type"].astype(str).str.upper()

            d2 = d2.dropna(subset=["day", "underlying", "expiry_date", "strike_int", "close"])
            d2["strike_int"] = d2["strike_int"].astype(int)

            # SAFETY: ignore stale rows where expiry is already before the trading day
            d2 = d2[d2["expiry_date"] >= d2["day"]]
            if d2.empty:
                continue

            # window filter
            d2 = d2[(d2["day"] >= window_start) & (d2["day"] <= window_end)]
            if d2.empty:
                continue

            # group by (und, day, expiry)
            for (und, dy, ex), g in d2.groupby(["underlying", "day", "expiry_date"], sort=False):
                key_ud = (und, dy)
                if key_ud not in min_expiry_map:
                    continue
                if min_expiry_map[key_ud] != ex:
                    continue

                day_key = (und, dy, ex)
                if day_key in processed_day_keys:
                    skipped_rows.append({
                        "day": dy, "underlying": und, "expiry": ex,
                        "reason": "Duplicate (underlying,day,expiry) encountered in multiple pickles; skipped to avoid double-count"
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
# Actual trades: one underlying per day (nearest expiry), include all re-entries for that underlying/day
# =============================================================================
def pick_actual_underlying_by_day(min_expiry_map: Dict[Tuple[str, date], date]) -> Dict[date, str]:
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
        # nearest expiry first; if tied, prefer NIFTY
        lst_sorted = sorted(lst, key=lambda t: (t[0], 0 if t[1] == "NIFTY" else 1))
        out[dy] = lst_sorted[0][1]
    return out

def build_actual_trades_df(all_trades_df: pd.DataFrame, min_expiry_map: Dict[Tuple[str, date], date]) -> pd.DataFrame:
    if all_trades_df.empty:
        return pd.DataFrame()

    actual_underlying = pick_actual_underlying_by_day(min_expiry_map)

    m = all_trades_df.copy()
    m["actual_underlying_for_day"] = m["day"].map(actual_underlying)

    # keep only days for which a 0/1-DTE actual underlying exists
    m = m[m["actual_underlying_for_day"].notna()]

    # keep only the selected underlying for that day
    m = m[m["underlying"] == m["actual_underlying_for_day"]]

    # keep only 0- and 1-DTE rows
    m = m[m["days_to_expiry"].isin([0, 1])]

    # keep all reattempts for the one selected underlying on that day
    m = m.drop(columns=["actual_underlying_for_day"])
    m = m.sort_values(["day", "trade_seq"]).reset_index(drop=True)

    # 1 if net exit PnL is positive, else 0
    m["is_exit_pnl_positive"] = (m["exit_pnl"] > 0).astype(int)

    return m


# =============================================================================
# Excel output
# =============================================================================
def _autosize_columns_safe(ws) -> None:
    # Safe autosize even when the sheet is "empty-ish"
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
        # Never fail the whole run just because autosize misbehaved
        return

def write_excel(all_trades_df: pd.DataFrame, actual_trades_df: pd.DataFrame, skipped_df: pd.DataFrame) -> None:
    out_dir = os.path.dirname(os.path.abspath(OUTPUT_XLSX))
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    piv_exit = pd.DataFrame()
    piv_eod_first = pd.DataFrame()
    monthwise_summary = pd.DataFrame()
    if not all_trades_df.empty:
        piv_exit = all_trades_df.pivot_table(index="day", columns="underlying", values="exit_pnl", aggfunc="sum").reset_index()

        first = all_trades_df[all_trades_df["trade_seq"] == 1]
        piv_eod_first = first.pivot_table(index="day", columns="underlying", values="eod_pnl", aggfunc="sum").reset_index()

        inst = all_trades_df.copy()
        inst["is_win_exit"] = inst["exit_pnl"] > 0
        inst["is_stoploss"] = inst["exit_reason"].astype(str).str.upper().eq("STOPLOSS")
        inst["is_profit_protect"] = inst["exit_reason"].astype(str).str.upper().eq("PROFIT_PROTECT")
        instrument_summary = (
            inst.groupby("underlying", as_index=False)
            .agg(
                trades=("exit_pnl", "count"),
                total_exit_pnl=("exit_pnl", "sum"),
                avg_exit_pnl=("exit_pnl", "mean"),
                win_rate_exit_pct=("is_win_exit", lambda s: 100.0 * s.mean()),
                stoploss_rate_pct=("is_stoploss", lambda s: 100.0 * s.mean()),
                profit_protect_rate_pct=("is_profit_protect", lambda s: 100.0 * s.mean()),
                avg_max_profit=("max_profit", "mean"),
                avg_max_loss=("max_loss", "mean"),
                worst_max_loss=("max_loss", "min"),
            )
            .sort_values("total_exit_pnl", ascending=False)
            .reset_index(drop=True)
        )
    else:
        instrument_summary = pd.DataFrame()

    if not actual_trades_df.empty:
        tmp = actual_trades_df.copy()
        tmp["month"] = pd.to_datetime(tmp["day"]).dt.to_period("M").astype(str)

        # Existing trade-level monthly summary
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
        monthwise_summary["win_rate_pct"] = (
                100.0 * monthwise_summary["winning_trades"] / monthwise_summary["trades"]
        ).round(2)

        # New: daily PnL inside each month
        daily_tmp = (
            tmp.groupby(["month", "day"], as_index=False)
            .agg(daily_pnl=("exit_pnl", "sum"))
        )

        loss_day_stats = (
            daily_tmp.groupby("month", as_index=False)
            .agg(
                avg_loss_on_loss_days=(
                    "daily_pnl",
                    lambda s: float(s[s < 0].mean()) if (s < 0).any() else 0.0
                ),
                max_loss_in_a_day=(
                    "daily_pnl",
                    lambda s: float(s.min()) if len(s) else 0.0
                ),
            )
        )

        monthwise_summary = monthwise_summary.merge(
            loss_day_stats,
            on="month",
            how="left",
        )
    else:
        monthwise_summary = pd.DataFrame()

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as xw:
        all_trades_df.to_excel(xw, sheet_name="all_trades_backtested", index=False)
        actual_trades_df.to_excel(xw, sheet_name="actual_trades", index=False)
        monthwise_summary.to_excel(xw, sheet_name="monthwise_summary", index=False)
        piv_exit.to_excel(xw, sheet_name="exit_pnl_pivot", index=False)
        piv_eod_first.to_excel(xw, sheet_name="eod_pnl_first_trade_pivot", index=False)
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
    paths = sorted(glob.glob(os.path.join(PICKLES_DIR, "*.pkl")) + glob.glob(os.path.join(PICKLES_DIR, "*.pickle")))
    if not paths:
        raise FileNotFoundError(f"No .pkl/.pickle files found in: {PICKLES_DIR}")

    print(f"[INFO] Pickles found: {len(paths)}")

    end_day, min_expiry_map, min_day_seen = scan_pickles_pass1(paths)
    window_start = compute_window_start(end_day, LOOKBACK_MONTHS)

    print(f"[INFO] Data day-range seen: {min_day_seen} -> {end_day}")
    print(f"[INFO] Window: {window_start} -> {end_day}")
    print(f"[INFO] Strategy: Iron Butterfly | Wing distance steps: {IRON_BFLY_WING_DISTANCE_STEPS}")
    for _und in sorted(TRADEABLE):
        print(f"[INFO] {_und} wing distance: {get_wing_distance_points(_und)} points")
    print(f"[INFO] Stoploss: -{LOSS_LIMIT_RUPEES} | ProfitProtect giveback: {PROFIT_PROTECT_TRIGGER_RUPEES} | Re-entry delay min: {REENTRY_DELAY_MINUTES}")
    print(f"[INFO] Tradeables: {sorted(TRADEABLE)}")
    print(f"[INFO] Output: {OUTPUT_XLSX}")

    print("[STEP] Initializing Kite ...")
    kite = oUtils.intialize_kite_api()
    print("[OK] Kite ready.")

    underlying_data = download_underlyings(kite, window_start, end_day)

    all_trades_df, skipped_df = process_pickles_generate_trades(
        paths, min_expiry_map, underlying_data, window_start, end_day
    )

    actual_trades_df = build_actual_trades_df(all_trades_df, min_expiry_map)

    write_excel(all_trades_df, actual_trades_df, skipped_df)

    if not all_trades_df.empty:
        print(all_trades_df.groupby("underlying")[["exit_pnl"]].describe())
    else:
        print("[WARN] No completed trades. Check 'skipped' sheet for reasons.")


if __name__ == "__main__":
    main()