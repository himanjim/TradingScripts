import os
import glob
import time
from dataclasses import dataclass
from datetime import datetime, date, time as dtime, timedelta
from typing import Dict, List, Tuple, Optional, Any, Set

import numpy as np
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
PICKLES_DIR = r"G:\My Drive\Trading\Historical_Options_Data"            # <-- change

SCAN_START_TIME_IST = os.getenv("SCAN_START_TIME_IST", "09:30").strip() or os.getenv("ENTRY_TIME_IST", "09:30").strip()

# Jump is ALWAYS RUPEES: (CE+PE)*qty
PREMIUM_JUMP_RUPEES = float(os.getenv("PREMIUM_JUMP_RUPEES", "5000"))

# If exact ATM strike missing at a minute, search nearest strikes within N steps (0 = strict ATM only)
MAX_STRIKE_DISTANCE_STEPS = int(os.getenv("MAX_STRIKE_DISTANCE_STEPS", "0"))

DEBUG_ENTRY_SCAN = os.getenv("DEBUG_ENTRY_SCAN", "0").strip() == "1"

LOSS_LIMIT_RUPEES = float(os.getenv("LOSS_LIMIT_RUPEES", "10000"))
PROFIT_PROTECT_TRIGGER_RUPEES = float(os.getenv("PROFIT_PROTECT_TRIGGER_RUPEES", "10000"))

def _safe_fname_part(s: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in s)

_DEFAULT_OUT = rf"C:\Users\himan\Downloads\short_straddle_backtest_prem_jump{_safe_fname_part(SCAN_START_TIME_IST)}_LL_{_safe_fname_part(str(LOSS_LIMIT_RUPEES))}_PPT_{_safe_fname_part(str(PROFIT_PROTECT_TRIGGER_RUPEES))}_PJR_{_safe_fname_part(str(PREMIUM_JUMP_RUPEES))}.xlsx"
OUTPUT_XLSX = os.getenv("OUTPUT_XLSX", _DEFAULT_OUT)  # if overriding with Windows path, use raw string or \\

FAIL_ON_PICKLE_ERROR = os.getenv("FAIL_ON_PICKLE_ERROR", "0").strip() == "1"

SESSION_START_IST = dtime(9, 15)
SESSION_END_IST = dtime(15, 30)

LOOKBACK_MONTHS = int(os.getenv("LOOKBACK_MONTHS", "6"))

QTY_UNITS = {"NIFTY": 325, "SENSEX": 100}  # BANKNIFTY excluded
TRADEABLE = set(QTY_UNITS.keys())

STRIKE_STEP = {"NIFTY": 50, "SENSEX": 100}

UNDERLYING_KITE = {
    "NIFTY": {"exchange": "NSE", "tradingsymbol": "NIFTY 50"},
    "SENSEX": {"exchange": "BSE", "tradingsymbol": "SENSEX"},
}

MAX_DAYS_PER_CHUNK = 25
MAX_ATTEMPTS = 5
SLEEP_BETWEEN_CALLS_SEC = 0.20

WRITE_MINUTE_DETAIL_SHEET = False


# =============================================================================
# HELPERS
# =============================================================================
def parse_hhmm(s: str) -> dtime:
    hh, mm = s.strip().split(":")
    return dtime(int(hh), int(mm))

SCAN_START_TIME = parse_hhmm(SCAN_START_TIME_IST)

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

def build_minute_index(day_d: date, start_t: dtime, end_t: dtime) -> pd.DatetimeIndex:
    tz = ist_tz()
    start = pd.Timestamp(datetime.combine(day_d, start_t), tz=tz)
    end = pd.Timestamp(datetime.combine(day_d, end_t), tz=tz)
    return pd.date_range(start=start, end=end, freq="1min")


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
class TradePlan:
    underlying: str
    day: date
    expiry: date
    qty: int
    needed_strikes: Set[int]
    ce_symbol_by_strike: Dict[int, str]
    pe_symbol_by_strike: Dict[int, str]
    ce_rows_by_strike: Dict[int, List[Tuple[pd.Timestamp, float]]]
    pe_rows_by_strike: Dict[int, List[Tuple[pd.Timestamp, float]]]

@dataclass
class TradeResult:
    day: date
    underlying: str
    expiry: date
    days_to_expiry: int
    atm_strike: int
    qty_units: int
    scan_start_time: str
    entry_time: str
    exit_time: str
    exit_reason: str
    entry_underlying: float
    entry_ce_symbol: str
    entry_pe_symbol: str
    entry_ce: float
    entry_pe: float
    exit_ce: float
    exit_pe: float
    exit_pnl: float
    eod_pnl: float
    max_profit: float
    max_loss: float
    # rupee trigger audit
    entry_atm_premium_rupees: float
    min_atm_premium_rupees_so_far: float
    entry_jump_rupees: float


# =============================================================================
# PASS-1
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

def compute_window_start(end_day: date, months: int) -> date:
    if relativedelta is not None:
        return (pd.Timestamp(end_day) - relativedelta(months=months)).date()
    return (pd.Timestamp(end_day) - pd.Timedelta(days=30 * months)).date()


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
# Build plans (needed strikes from SCAN_START_TIME onward, expanded for fallback)
# =============================================================================
def build_trade_plans(
    min_expiry_map: Dict[Tuple[str, date], date],
    window_start: date,
    window_end: date,
    underlying_data: Dict[str, pd.DataFrame],
) -> Dict[Tuple[str, date], TradePlan]:
    plans: Dict[Tuple[str, date], TradePlan] = {}

    for (und, dy), expiry in min_expiry_map.items():
        if dy < window_start or dy > window_end:
            continue
        if und not in underlying_data:
            continue
        if expiry < dy:
            continue

        uday = underlying_data[und]
        uday = uday[uday["day"] == dy]
        if uday.empty:
            continue

        idx_all = build_minute_index(dy, SESSION_START_IST, SESSION_END_IST)

        u2 = uday[["date", "close"]].dropna().copy()
        u2["date"] = ensure_ist(u2["date"])
        u2 = u2.sort_values("date").drop_duplicates(subset=["date"], keep="last").set_index("date")
        u_all = u2.reindex(idx_all).ffill()
        if u_all["close"].isna().all():
            continue

        scan_ts = pd.Timestamp(datetime.combine(dy, SCAN_START_TIME), tz=ist_tz())
        if scan_ts not in idx_all:
            continue
        u_scan = u_all.loc[scan_ts:]
        if u_scan.empty or u_scan["close"].isna().all():
            continue

        step = STRIKE_STEP[und]
        base_strikes = set(round_to_step(float(x), step) for x in u_scan["close"].dropna().astype(float).values)
        if not base_strikes:
            continue

        # IMPORTANT: if fallback enabled, include neighbors so PASS-2 actually loads those legs
        strikes = set(base_strikes)
        if MAX_STRIKE_DISTANCE_STEPS > 0:
            for s in list(base_strikes):
                for k in range(1, MAX_STRIKE_DISTANCE_STEPS + 1):
                    strikes.add(s - k * step)
                    strikes.add(s + k * step)

        plans[(und, dy)] = TradePlan(
            underlying=und,
            day=dy,
            expiry=expiry,
            qty=int(QTY_UNITS[und]),
            needed_strikes=strikes,
            ce_symbol_by_strike={},
            pe_symbol_by_strike={},
            ce_rows_by_strike={},
            pe_rows_by_strike={},
        )

    print(f"[INFO] Trade plans created (ALL): {len(plans)}")
    return plans


# =============================================================================
# Choose actual trades: per day pick instrument with nearest expiry
# =============================================================================
def choose_actual_plan_keys(plans: Dict[Tuple[str, date], TradePlan]) -> Set[Tuple[str, date]]:
    by_day: Dict[date, List[Tuple[str, date]]] = {}
    for k, p in plans.items():
        by_day.setdefault(p.day, []).append(k)

    chosen: Set[Tuple[str, date]] = set()
    for dy, keys in by_day.items():
        keys_sorted = sorted(
            keys,
            key=lambda kk: (plans[kk].expiry, 0 if plans[kk].underlying == "NIFTY" else 1),
        )
        chosen.add(keys_sorted[0])
    print(f"[INFO] Actual-trade keys selected (1/day): {len(chosen)}")
    return chosen


# =============================================================================
# PASS-2: collect CE/PE legs for strikes that can be ATM that day
# =============================================================================
def collect_legs_pass2(pickle_paths: List[str], plans: Dict[Tuple[str, date], TradePlan]) -> None:
    if not plans:
        return

    need_keys_mi = pd.MultiIndex.from_tuples(list(plans.keys()))
    need_expiry = {k: p.expiry for k, p in plans.items()}
    wanted_exp = pd.Series(list(need_expiry.values()), index=pd.MultiIndex.from_tuples(list(need_expiry.keys())))

    for pth in pickle_paths:
        try:
            df = pd.read_pickle(pth)
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue

            needed_cols = ["date", "name", "type", "option_type", "strike", "expiry", "instrument", "close"]
            missing = [c for c in needed_cols if c not in df.columns]
            if missing:
                raise ValueError(f"Missing columns {missing} in {pth}")

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
            d2["option_type"] = d2["option_type"].astype(str).str.upper()
            d2 = d2.dropna(subset=["underlying", "day", "expiry_date", "strike_num", "close"])
            d2 = d2[d2["expiry_date"] >= d2["day"]]
            if d2.empty:
                continue

            key_idx = pd.MultiIndex.from_arrays([d2["underlying"], d2["day"]])
            d2 = d2[key_idx.isin(need_keys_mi)]
            if d2.empty:
                continue

            key_idx2 = pd.MultiIndex.from_arrays([d2["underlying"], d2["day"]])
            wexp = wanted_exp.reindex(key_idx2).to_numpy()
            d2 = d2[d2["expiry_date"].to_numpy() == wexp]
            if d2.empty:
                continue

            d2["strike_int"] = d2["strike_num"].round().astype(int)
            d2 = d2[d2["option_type"].isin(["CE", "PE"])]
            if d2.empty:
                continue

            matched_rows = 0
            for (und, dy), g in d2.groupby(["underlying", "day"], sort=False):
                plan = plans.get((und, dy))
                if plan is None:
                    continue

                gg = g[g["strike_int"].isin(plan.needed_strikes)]
                if gg.empty:
                    continue
                matched_rows += len(gg)

                for strike_i, gg2 in gg.groupby("strike_int", sort=False):
                    strike_i = int(strike_i)
                    for opt_type, gg3 in gg2.groupby("option_type", sort=False):
                        sym_first = str(gg3["instrument"].iloc[0])

                        if opt_type == "CE":
                            plan.ce_symbol_by_strike.setdefault(strike_i, sym_first)
                            sym = plan.ce_symbol_by_strike[strike_i]
                            gsym = gg3[gg3["instrument"].astype(str) == sym]
                            lst = plan.ce_rows_by_strike.setdefault(strike_i, [])
                            for ts, cl in gsym[["date", "close"]].itertuples(index=False, name=None):
                                lst.append((ts, float(cl)))
                        else:
                            plan.pe_symbol_by_strike.setdefault(strike_i, sym_first)
                            sym = plan.pe_symbol_by_strike[strike_i]
                            gsym = gg3[gg3["instrument"].astype(str) == sym]
                            lst = plan.pe_rows_by_strike.setdefault(strike_i, [])
                            for ts, cl in gsym[["date", "close"]].itertuples(index=False, name=None):
                                lst.append((ts, float(cl)))

            print(f"[PASS2 OK] {os.path.basename(pth)} matched_rows={matched_rows}")

        except Exception as e:
            msg = f"[PASS2 WARN] {os.path.basename(pth)} failed: {e}"
            if FAIL_ON_PICKLE_ERROR:
                raise RuntimeError(msg) from e
            print(msg)


# =============================================================================
# Compute: dynamic ATM premium scan (rupees), running-min trigger, then exit logic
# =============================================================================
def _rows_to_reindexed_arrays(rows: List[Tuple[pd.Timestamp, float]], idx_all: pd.DatetimeIndex) -> Optional[np.ndarray]:
    if not rows:
        return None
    df = pd.DataFrame(rows, columns=["date", "close"]).dropna()
    if df.empty:
        return None
    df["date"] = ensure_ist(df["date"])
    df = df.sort_values("date").drop_duplicates(subset=["date"], keep="last")
    ser = df.set_index("date").reindex(idx_all).ffill()["close"].astype(float)
    return ser.to_numpy(dtype=float)

def compute_trade_results(
    plans: Dict[Tuple[str, date], TradePlan],
    underlying_data: Dict[str, pd.DataFrame],
) -> Tuple[pd.DataFrame, pd.DataFrame, Optional[pd.DataFrame]]:
    results: List[TradeResult] = []
    skipped_rows: List[Dict[str, Any]] = []
    minute_details: List[pd.DataFrame] = []

    giveback_G = float(PROFIT_PROTECT_TRIGGER_RUPEES)

    for _, plan in sorted(plans.items(), key=lambda kv: (kv[1].day, kv[1].underlying)):
        und = plan.underlying
        dy = plan.day

        uday = underlying_data.get(und)
        if uday is None:
            skipped_rows.append({"day": dy, "underlying": und, "reason": "Missing underlying minute data"})
            continue
        uday = uday[uday["day"] == dy]
        if uday.empty:
            skipped_rows.append({"day": dy, "underlying": und, "reason": "No underlying candles for day"})
            continue

        idx_all = build_minute_index(dy, SESSION_START_IST, SESSION_END_IST)
        eod_ts = idx_all[-1]

        u2 = uday[["date", "close"]].dropna().copy()
        u2["date"] = ensure_ist(u2["date"])
        u2 = u2.sort_values("date").drop_duplicates(subset=["date"], keep="last").set_index("date")
        u_all = u2.reindex(idx_all).ffill()
        if u_all["close"].isna().all():
            skipped_rows.append({"day": dy, "underlying": und, "reason": "Underlying close all-NaN after ffill"})
            continue

        scan_start_ts = pd.Timestamp(datetime.combine(dy, SCAN_START_TIME), tz=ist_tz())
        if scan_start_ts not in idx_all:
            skipped_rows.append({"day": dy, "underlying": und, "reason": "Scan start timestamp not in index"})
            continue

        start_pos = int(idx_all.get_loc(scan_start_ts))
        u_close = u_all["close"].to_numpy(dtype=float)

        qty = int(plan.qty)
        step = STRIKE_STEP[und]

        # Cache strike -> (ce_np, pe_np)
        leg_cache: Dict[int, Tuple[np.ndarray, np.ndarray]] = {}

        def get_leg_arrays_exact(strike_i: int) -> Optional[Tuple[np.ndarray, np.ndarray]]:
            if strike_i in leg_cache:
                return leg_cache[strike_i]
            ce_rows = plan.ce_rows_by_strike.get(strike_i, [])
            pe_rows = plan.pe_rows_by_strike.get(strike_i, [])
            if not ce_rows or not pe_rows:
                return None
            ce_np = _rows_to_reindexed_arrays(ce_rows, idx_all)
            pe_np = _rows_to_reindexed_arrays(pe_rows, idx_all)
            if ce_np is None or pe_np is None:
                return None
            leg_cache[strike_i] = (ce_np, pe_np)
            return leg_cache[strike_i]

        def get_leg_arrays_with_fallback(atm_strike: int) -> Optional[Tuple[int, Tuple[np.ndarray, np.ndarray]]]:
            legs = get_leg_arrays_exact(atm_strike)
            if legs is not None:
                return atm_strike, legs
            if MAX_STRIKE_DISTANCE_STEPS <= 0:
                return None
            for k in range(1, MAX_STRIKE_DISTANCE_STEPS + 1):
                for s in (atm_strike - k * step, atm_strike + k * step):
                    legs2 = get_leg_arrays_exact(s)
                    if legs2 is not None:
                        return s, legs2
            return None

        # ENTRY scan (rupees)
        min_rupees_so_far: Optional[float] = None

        entry_pos: Optional[int] = None
        entry_strike: Optional[int] = None
        entry_ce: Optional[float] = None
        entry_pe: Optional[float] = None
        entry_under: Optional[float] = None

        entry_prem_rupees: Optional[float] = None
        entry_min_rupees_before: Optional[float] = None
        entry_jump_rupees: Optional[float] = None

        misses = 0
        seen = 0
        best_jump_seen = float("-inf")

        for i in range(start_pos, len(idx_all)):
            under_px = float(u_close[i])
            if pd.isna(under_px):
                continue

            atm = round_to_step(under_px, step)
            got = get_leg_arrays_with_fallback(atm)
            if got is None:
                misses += 1
                continue

            strike_used, (ce_np, pe_np) = got

            ce_px = float(ce_np[i])
            pe_px = float(pe_np[i])
            if pd.isna(ce_px) or pd.isna(pe_px):
                misses += 1
                continue

            prem_rupees = float((ce_px + pe_px) * qty)

            seen += 1
            if min_rupees_so_far is None or prem_rupees < min_rupees_so_far:
                min_rupees_so_far = prem_rupees

            jump_now = prem_rupees - float(min_rupees_so_far)
            best_jump_seen = max(best_jump_seen, jump_now)

            if jump_now >= PREMIUM_JUMP_RUPEES:
                entry_pos = i
                entry_strike = int(strike_used)
                entry_ce = ce_px
                entry_pe = pe_px
                entry_under = under_px

                entry_prem_rupees = prem_rupees
                entry_min_rupees_before = float(min_rupees_so_far)
                entry_jump_rupees = float(jump_now)
                break

        if entry_pos is None or entry_strike is None or entry_ce is None or entry_pe is None or entry_under is None:
            skipped_rows.append({
                "day": dy,
                "underlying": und,
                "expiry": plan.expiry,
                "reason": (
                    f"No entry: jump not hit | jump={PREMIUM_JUMP_RUPEES} rupees | "
                    f"seen={seen} misses={misses} best_jump_seen={best_jump_seen:.2f}"
                )
            })
            if DEBUG_ENTRY_SCAN:
                print(f"[DEBUG] {und} {dy} no-entry: seen={seen} misses={misses} best_jump_seen={best_jump_seen:.2f}")
            continue

        legs_exact = get_leg_arrays_exact(entry_strike)
        if legs_exact is None:
            skipped_rows.append({"day": dy, "underlying": und, "reason": "Entry strike series missing unexpectedly"})
            continue
        ce_np, pe_np = legs_exact

        # Full pnl series from entry->EOD on the FIXED strike
        pnl_arr = (entry_ce - ce_np[entry_pos:]) * qty + (entry_pe - pe_np[entry_pos:]) * qty
        pnl_idx = idx_all[entry_pos:]
        pnl = pd.Series(pnl_arr, index=pnl_idx)

        # EOD pnl forced at 15:30 (index last)
        eod_pnl = float(pnl.iloc[-1])

        max_profit = float(max(0.0, pnl.max()))
        max_loss = float(min(0.0, pnl.min()))

        stop_hit = pnl <= -LOSS_LIMIT_RUPEES
        stop_ts = pnl.index[stop_hit.to_numpy().argmax()] if stop_hit.any() else None

        peak_series = pnl.cummax()
        armed = peak_series >= giveback_G
        trail_level = peak_series - giveback_G
        protect_hit = armed & (pnl <= trail_level)
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

        exit_pos = int(idx_all.get_loc(exit_ts))
        exit_ce = float(ce_np[exit_pos])
        exit_pe = float(pe_np[exit_pos])
        exit_pnl = float(pnl.at[exit_ts])

        dte = int((plan.expiry - dy).days)
        ce_sym = plan.ce_symbol_by_strike.get(entry_strike, "")
        pe_sym = plan.pe_symbol_by_strike.get(entry_strike, "")

        results.append(
            TradeResult(
                day=dy,
                underlying=und,
                expiry=plan.expiry,
                days_to_expiry=dte,
                atm_strike=int(entry_strike),
                qty_units=qty,
                scan_start_time=SCAN_START_TIME_IST,
                entry_time=idx_all[entry_pos].strftime("%H:%M"),
                exit_time=pd.Timestamp(exit_ts).strftime("%H:%M"),
                exit_reason=exit_reason,
                entry_underlying=float(entry_under),
                entry_ce_symbol=ce_sym,
                entry_pe_symbol=pe_sym,
                entry_ce=float(entry_ce),
                entry_pe=float(entry_pe),
                exit_ce=exit_ce,
                exit_pe=exit_pe,
                exit_pnl=exit_pnl,
                eod_pnl=eod_pnl,
                max_profit=max_profit,
                max_loss=max_loss,
                entry_atm_premium_rupees=float(entry_prem_rupees),
                min_atm_premium_rupees_so_far=float(entry_min_rupees_before),
                entry_jump_rupees=float(entry_jump_rupees),
            )
        )

        if WRITE_MINUTE_DETAIL_SHEET:
            md = pd.DataFrame({"datetime": pnl.index, "pnl": pnl.values})
            md.insert(0, "underlying", und)
            md.insert(1, "day", dy)
            md.insert(2, "expiry", plan.expiry)
            md.insert(3, "days_to_expiry", dte)
            md.insert(4, "atm_strike", int(entry_strike))
            minute_details.append(md)

    all_df = pd.DataFrame([r.__dict__ for r in results])
    if not all_df.empty:
        all_df = all_df.sort_values(["day", "underlying"]).reset_index(drop=True)

    skip_df = pd.DataFrame(skipped_rows)
    if not skip_df.empty:
        if "day" not in skip_df.columns:
            skip_df["day"] = pd.NaT
        if "underlying" not in skip_df.columns:
            skip_df["underlying"] = pd.NA
        skip_df = skip_df.sort_values(["day", "underlying"], na_position="last").reset_index(drop=True)

    md_df = None
    if WRITE_MINUTE_DETAIL_SHEET and minute_details:
        md_df = pd.concat(minute_details, ignore_index=True)

    return all_df, skip_df, md_df


# =============================================================================
# Actual trades sheet (1/day)
# =============================================================================
def build_actual_trades_sheet(
    plans: Dict[Tuple[str, date], TradePlan],
    actual_keys: Set[Tuple[str, date]],
    all_trades_df: pd.DataFrame,
    skipped_df: pd.DataFrame
) -> pd.DataFrame:
    res_map: Dict[Tuple[str, date], Dict[str, Any]] = {}
    if not all_trades_df.empty:
        for r in all_trades_df.to_dict(orient="records"):
            res_map[(r["underlying"], r["day"])] = r

    skip_reason_map: Dict[Tuple[str, date], str] = {}
    if not skipped_df.empty and "reason" in skipped_df.columns:
        for r in skipped_df.to_dict(orient="records"):
            k = (r.get("underlying"), r.get("day"))
            if k not in skip_reason_map and r.get("reason"):
                skip_reason_map[k] = str(r.get("reason"))

    rows: List[Dict[str, Any]] = []
    for k in sorted(actual_keys, key=lambda kk: plans[kk].day):
        und, dy = k
        if (und, dy) in res_map:
            row = dict(res_map[(und, dy)])
            row["skip_reason"] = ""
            rows.append(row)
        else:
            p = plans[k]
            rows.append({
                "day": dy,
                "underlying": und,
                "expiry": p.expiry,
                "days_to_expiry": int((p.expiry - dy).days),
                "atm_strike": "",
                "qty_units": p.qty,
                "scan_start_time": SCAN_START_TIME_IST,
                "entry_time": "",
                "exit_time": "",
                "exit_reason": "SKIPPED",
                "entry_underlying": "",
                "entry_ce_symbol": "",
                "entry_pe_symbol": "",
                "entry_ce": "",
                "entry_pe": "",
                "exit_ce": "",
                "exit_pe": "",
                "exit_pnl": "",
                "eod_pnl": "",
                "max_profit": "",
                "max_loss": "",
                "entry_atm_premium_rupees": "",
                "min_atm_premium_rupees_so_far": "",
                "entry_jump_rupees": "",
                "skip_reason": skip_reason_map.get((und, dy), "No result row (see skipped sheet)"),
            })

    return pd.DataFrame(rows)


# =============================================================================
# Excel output (includes instrument summary + pivots)
# =============================================================================
def write_excel(all_trades_df: pd.DataFrame, actual_trades_df: pd.DataFrame, skipped_df: pd.DataFrame, md_df: Optional[pd.DataFrame]) -> None:
    out_dir = os.path.dirname(os.path.abspath(OUTPUT_XLSX))
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    if not all_trades_df.empty:
        inst = all_trades_df.copy()
        inst["is_win_exit"] = inst["exit_pnl"] > 0
        inst["is_win_eod"] = inst["eod_pnl"] > 0
        inst["is_stoploss"] = inst["exit_reason"].astype(str).str.upper().eq("STOPLOSS")
        inst["is_profit_protect"] = inst["exit_reason"].astype(str).str.upper().eq("PROFIT_PROTECT")

        instrument_summary = (
            inst.groupby("underlying", as_index=False)
            .agg(
                trades=("exit_pnl", "count"),
                total_exit_pnl=("exit_pnl", "sum"),
                total_eod_pnl=("eod_pnl", "sum"),
                avg_exit_pnl=("exit_pnl", "mean"),
                avg_eod_pnl=("eod_pnl", "mean"),
                win_rate_exit_pct=("is_win_exit", lambda s: 100.0 * s.mean()),
                win_rate_eod_pct=("is_win_eod", lambda s: 100.0 * s.mean()),
                stoploss_rate_pct=("is_stoploss", lambda s: 100.0 * s.mean()),
                profit_protect_rate_pct=("is_profit_protect", lambda s: 100.0 * s.mean()),
                avg_max_profit=("max_profit", "mean"),
                avg_max_loss=("max_loss", "mean"),
                worst_max_loss=("max_loss", "min"),
            )
            .sort_values("total_exit_pnl", ascending=False)
            .reset_index(drop=True)
        )

        piv_exit = all_trades_df.pivot_table(index="day", columns="underlying", values="exit_pnl", aggfunc="sum").reset_index()
        piv_eod = all_trades_df.pivot_table(index="day", columns="underlying", values="eod_pnl", aggfunc="sum").reset_index()
    else:
        instrument_summary = pd.DataFrame()
        piv_exit = pd.DataFrame()
        piv_eod = pd.DataFrame()

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as xw:
        all_trades_df.to_excel(xw, sheet_name="all_trades_backtested", index=False)
        actual_trades_df.to_excel(xw, sheet_name="actual_trades", index=False)
        piv_exit.to_excel(xw, sheet_name="exit_pnl_pivot", index=False)
        piv_eod.to_excel(xw, sheet_name="eod_pnl_pivot", index=False)
        instrument_summary.to_excel(xw, sheet_name="instrument_summary", index=False)
        skipped_df.to_excel(xw, sheet_name="skipped", index=False)
        if md_df is not None:
            md_df.to_excel(xw, sheet_name="minute_pnl", index=False)

        wb = xw.book
        for sheet in wb.worksheets:
            sheet.freeze_panes = "A2"
            for col in sheet.columns:
                max_len = 0
                col_letter = col[0].column_letter
                for cell in col[:2000]:
                    if cell.value is None:
                        continue
                    max_len = max(max_len, len(str(cell.value)))
                sheet.column_dimensions[col_letter].width = min(60, max(10, max_len + 2))

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
    print(f"[INFO] Scan start: {SCAN_START_TIME_IST} | Jump={PREMIUM_JUMP_RUPEES} rupees")
    print(f"[INFO] ATM fallback steps: {MAX_STRIKE_DISTANCE_STEPS}")
    print(f"[INFO] Stoploss: -{LOSS_LIMIT_RUPEES} | ProfitProtect giveback: {PROFIT_PROTECT_TRIGGER_RUPEES}")
    print(f"[INFO] Tradeables: {sorted(TRADEABLE)} (BANKNIFTY excluded)")
    print(f"[INFO] Output: {OUTPUT_XLSX}")

    print("[STEP] Initializing Kite ...")
    kite = oUtils.intialize_kite_api()
    print("[OK] Kite ready.")

    underlying_data = download_underlyings(kite, window_start, end_day)

    plans = build_trade_plans(min_expiry_map, window_start, end_day, underlying_data)
    if not plans:
        raise RuntimeError("No trade plans created (NIFTY/SENSEX).")

    actual_keys = choose_actual_plan_keys(plans)

    collect_legs_pass2(paths, plans)

    all_trades_df, skipped_df, md_df = compute_trade_results(plans, underlying_data)
    actual_trades_df = build_actual_trades_sheet(plans, actual_keys, all_trades_df, skipped_df)

    write_excel(all_trades_df, actual_trades_df, skipped_df, md_df)

    if all_trades_df.empty:
        print("[WARN] No completed trades. Check 'skipped' sheet.")
    else:
        print(all_trades_df.groupby("underlying")[["exit_pnl", "eod_pnl"]].describe())


if __name__ == "__main__":
    main()