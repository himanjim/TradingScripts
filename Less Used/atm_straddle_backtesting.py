import os
import glob
import time
from dataclasses import dataclass
from datetime import datetime, date, time as dtime, timedelta
from typing import Dict, List, Tuple, Optional, Any, Set

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
ENTRY_TIME_IST = os.getenv("ENTRY_TIME_IST", "12:30")  # "HH:MM"

def _safe_fname_part(s: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in s)

LOSS_LIMIT_RUPEES = int(os.getenv("LOSS_LIMIT_RUPEES", "10000"))
PROFIT_PROTECT_TRIGGER_RUPEES = int(os.getenv("PROFIT_PROTECT_TRIGGER_RUPEES", "10000"))

_DEFAULT_OUT = rf"C:\Users\himan\Downloads\short_straddle_backtest_{_safe_fname_part(ENTRY_TIME_IST)}_LL_{_safe_fname_part(str(LOSS_LIMIT_RUPEES))}_PPT_{_safe_fname_part(str(PROFIT_PROTECT_TRIGGER_RUPEES))}.xlsx"
OUTPUT_XLSX = os.getenv("OUTPUT_XLSX", _DEFAULT_OUT)  # if overriding with Windows path, use raw string or \\

FAIL_ON_PICKLE_ERROR = os.getenv("FAIL_ON_PICKLE_ERROR", "0").strip() == "1"

SESSION_START_IST = dtime(9, 15)
SESSION_END_IST = dtime(15, 30)

LOOKBACK_MONTHS = int(os.getenv("LOOKBACK_MONTHS", "6"))

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

WRITE_MINUTE_DETAIL_SHEET = False


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
    atm_strike: Optional[int] = None
    entry_underlying: Optional[float] = None
    ce_symbol: Optional[str] = None
    pe_symbol: Optional[str] = None
    ce_rows: List[Tuple[pd.Timestamp, float]] = None
    pe_rows: List[Tuple[pd.Timestamp, float]] = None

    def __post_init__(self):
        self.ce_rows = self.ce_rows or []
        self.pe_rows = self.pe_rows or []

@dataclass
class TradeResult:
    day: date
    underlying: str
    expiry: date
    days_to_expiry: int
    atm_strike: int
    qty_units: int
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
    exit_pnl: float     # PnL at actual exit timestamp
    eod_pnl: float      # PnL at 15:30 regardless of exit
    max_profit: float   # >= 0
    max_loss: float     # <= 0


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
# Build plans (ALL backtested)
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

        entry_ts = pd.Timestamp(datetime.combine(dy, ENTRY_TIME), tz=ist_tz())
        entry_under = asof_close(uday, entry_ts)
        if pd.isna(entry_under):
            continue

        atm = round_to_step(float(entry_under), STRIKE_STEP[und])
        qty = int(QTY_UNITS[und])

        plans[(und, dy)] = TradePlan(
            underlying=und,
            day=dy,
            expiry=expiry,
            qty=qty,
            atm_strike=int(atm),
            entry_underlying=float(entry_under),
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
# PASS-2: collect CE/PE legs for ALL plans (optimized; no merge)
# =============================================================================
def collect_legs_pass2(pickle_paths: List[str], plans: Dict[Tuple[str, date], TradePlan]) -> None:
    if not plans:
        return

    # Wanted per (underlying, day)
    need_expiry: Dict[Tuple[str, date], date] = {}
    need_strike: Dict[Tuple[str, date], int] = {}
    need_keys: Set[Tuple[str, date]] = set()

    for k, p in plans.items():
        if p.atm_strike is None:
            continue
        need_keys.add(k)
        need_expiry[k] = p.expiry
        need_strike[k] = int(p.atm_strike)

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

            # Key filter first
            key_series = pd.Series(list(zip(d2["underlying"], d2["day"])), index=d2.index)
            mask_key = key_series.isin(need_keys)
            d2 = d2[mask_key]
            key_series = key_series[mask_key]
            if d2.empty:
                continue

            d2["wanted_expiry"] = key_series.map(need_expiry)
            d2["wanted_strike"] = key_series.map(need_strike)
            d2["strike_int"] = d2["strike_num"].astype(int)

            d2 = d2[
                (d2["expiry_date"] == d2["wanted_expiry"]) &
                (d2["strike_int"] == d2["wanted_strike"]) &
                (d2["option_type"].isin(["CE", "PE"]))
            ]
            if d2.empty:
                continue

            for (und, dy, opt_type), g in d2.groupby(["underlying", "day", "option_type"], sort=False):
                plan = plans.get((und, dy))
                if plan is None:
                    continue

                sym_first = str(g["instrument"].iloc[0])
                if opt_type == "CE":
                    if plan.ce_symbol is None:
                        plan.ce_symbol = sym_first
                    g2 = g[g["instrument"].astype(str) == plan.ce_symbol]
                    for ts, cl in g2[["date", "close"]].itertuples(index=False, name=None):
                        plan.ce_rows.append((ts, float(cl)))
                else:
                    if plan.pe_symbol is None:
                        plan.pe_symbol = sym_first
                    g2 = g[g["instrument"].astype(str) == plan.pe_symbol]
                    for ts, cl in g2[["date", "close"]].itertuples(index=False, name=None):
                        plan.pe_rows.append((ts, float(cl)))

            print(f"[PASS2 OK] {os.path.basename(pth)} matched_rows={len(d2)}")

        except Exception as e:
            msg = f"[PASS2 WARN] {os.path.basename(pth)} failed: {e}"
            if FAIL_ON_PICKLE_ERROR:
                raise RuntimeError(msg) from e
            print(msg)


# =============================================================================
# Compute PnL + exits + EOD
# =============================================================================
def compute_trade_results(plans: Dict[Tuple[str, date], TradePlan]) -> Tuple[pd.DataFrame, pd.DataFrame, Optional[pd.DataFrame]]:
    results: List[TradeResult] = []
    skipped_rows: List[Dict[str, Any]] = []
    minute_details: List[pd.DataFrame] = []

    G = float(PROFIT_PROTECT_TRIGGER_RUPEES)

    for _, plan in sorted(plans.items(), key=lambda kv: (kv[1].day, kv[1].underlying)):
        und = plan.underlying
        dy = plan.day

        if plan.atm_strike is None or plan.entry_underlying is None:
            skipped_rows.append({"day": dy, "underlying": und, "reason": "Missing ATM/entry_underlying"})
            continue

        if not plan.ce_symbol or not plan.pe_symbol:
            skipped_rows.append({"day": dy, "underlying": und, "expiry": plan.expiry, "atm_strike": plan.atm_strike,
                                 "reason": "Missing CE/PE symbol"})
            continue

        if len(plan.ce_rows) < 5 or len(plan.pe_rows) < 5:
            skipped_rows.append({"day": dy, "underlying": und, "expiry": plan.expiry, "atm_strike": plan.atm_strike,
                                 "ce_rows": len(plan.ce_rows), "pe_rows": len(plan.pe_rows),
                                 "reason": "Insufficient minute data for CE/PE"})
            continue

        ce_df = pd.DataFrame(plan.ce_rows, columns=["date", "close"]).dropna()
        pe_df = pd.DataFrame(plan.pe_rows, columns=["date", "close"]).dropna()
        ce_df["date"] = ensure_ist(ce_df["date"])
        pe_df["date"] = ensure_ist(pe_df["date"])
        ce_df = ce_df.sort_values("date").drop_duplicates(subset=["date"], keep="last")
        pe_df = pe_df.sort_values("date").drop_duplicates(subset=["date"], keep="last")

        idx_all = build_minute_index(dy, SESSION_START_IST, SESSION_END_IST)
        entry_ts = pd.Timestamp(datetime.combine(dy, ENTRY_TIME), tz=ist_tz())

        ce_all = ce_df.set_index("date").reindex(idx_all).ffill()
        pe_all = pe_df.set_index("date").reindex(idx_all).ffill()

        if entry_ts not in ce_all.index or entry_ts not in pe_all.index:
            skipped_rows.append({"day": dy, "underlying": und, "reason": "Entry timestamp not in index"})
            continue

        entry_ce = float(ce_all.at[entry_ts, "close"]) if pd.notna(ce_all.at[entry_ts, "close"]) else float("nan")
        entry_pe = float(pe_all.at[entry_ts, "close"]) if pd.notna(pe_all.at[entry_ts, "close"]) else float("nan")
        if pd.isna(entry_ce) or pd.isna(entry_pe):
            skipped_rows.append({"day": dy, "underlying": und, "expiry": plan.expiry, "atm_strike": plan.atm_strike,
                                 "reason": "No CE/PE price at entry"})
            continue

        qty = int(plan.qty)

        pnl_all = (entry_ce - ce_all["close"]) * qty + (entry_pe - pe_all["close"]) * qty
        pnl = pnl_all.loc[entry_ts:].dropna()
        if pnl.empty:
            skipped_rows.append({"day": dy, "underlying": und, "reason": "PnL series empty after entry"})
            continue

        # EOD
        eod_ts = pnl.index[-1]
        eod_pnl = float(pnl.iloc[-1])

        # Day extremes from entry->EOD
        max_profit = float(max(0.0, pnl.max()))
        max_loss = float(min(0.0, pnl.min()))  # IMPORTANT: never positive

        # STOPLOSS
        stop_hit = pnl <= -LOSS_LIMIT_RUPEES
        stop_ts = pnl.index[stop_hit.to_numpy().argmax()] if stop_hit.any() else None

        # PROFIT PROTECT (giveback G)
        peak_series = pnl.cummax()
        armed = peak_series >= G
        trail_level = peak_series - G
        protect_hit = armed & (pnl <= trail_level)
        protect_ts = pnl.index[protect_hit.to_numpy().argmax()] if protect_hit.any() else None

        # earliest exit; STOPLOSS tie-break
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

        exit_ce = float(ce_all.at[exit_ts, "close"]) if pd.notna(ce_all.at[exit_ts, "close"]) else float("nan")
        exit_pe = float(pe_all.at[exit_ts, "close"]) if pd.notna(pe_all.at[exit_ts, "close"]) else float("nan")
        exit_pnl = float(pnl.at[exit_ts])

        dte = int((plan.expiry - dy).days)

        results.append(
            TradeResult(
                day=dy,
                underlying=und,
                expiry=plan.expiry,
                days_to_expiry=dte,
                atm_strike=int(plan.atm_strike),
                qty_units=qty,
                entry_time=entry_ts.strftime("%H:%M"),
                exit_time=pd.Timestamp(exit_ts).strftime("%H:%M"),
                exit_reason=exit_reason,
                entry_underlying=float(plan.entry_underlying),
                entry_ce_symbol=plan.ce_symbol,
                entry_pe_symbol=plan.pe_symbol,
                entry_ce=entry_ce,
                entry_pe=entry_pe,
                exit_ce=exit_ce,
                exit_pe=exit_pe,
                exit_pnl=exit_pnl,
                eod_pnl=eod_pnl,
                max_profit=max_profit,
                max_loss=max_loss,
            )
        )

        if WRITE_MINUTE_DETAIL_SHEET:
            md = pd.DataFrame({"datetime": pnl.index, "pnl": pnl.values})
            md.insert(0, "underlying", und)
            md.insert(1, "day", dy)
            md.insert(2, "expiry", plan.expiry)
            md.insert(3, "days_to_expiry", dte)
            md.insert(4, "atm_strike", int(plan.atm_strike))
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
                "atm_strike": p.atm_strike,
                "qty_units": p.qty,
                "entry_time": ENTRY_TIME_IST,
                "exit_time": "",
                "exit_reason": "SKIPPED",
                "entry_underlying": p.entry_underlying,
                "entry_ce_symbol": p.ce_symbol or "",
                "entry_pe_symbol": p.pe_symbol or "",
                "entry_ce": "",
                "entry_pe": "",
                "exit_ce": "",
                "exit_pe": "",
                "exit_pnl": "",
                "eod_pnl": "",
                "max_profit": "",
                "max_loss": "",
                "skip_reason": skip_reason_map.get((und, dy), "No result row (see skipped sheet)"),
            })

    return pd.DataFrame(rows)


# =============================================================================
# Excel output
# =============================================================================
def write_excel(all_trades_df: pd.DataFrame, actual_trades_df: pd.DataFrame, skipped_df: pd.DataFrame, md_df: Optional[pd.DataFrame]) -> None:
    # ensure output directory exists
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
    else:
        instrument_summary = pd.DataFrame()

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as xw:
        all_trades_df.to_excel(xw, sheet_name="all_trades_backtested", index=False)
        actual_trades_df.to_excel(xw, sheet_name="actual_trades", index=False)

        if not all_trades_df.empty:
            piv_exit = all_trades_df.pivot_table(index="day", columns="underlying", values="exit_pnl", aggfunc="sum").reset_index()
            piv_eod = all_trades_df.pivot_table(index="day", columns="underlying", values="eod_pnl", aggfunc="sum").reset_index()
        else:
            piv_exit = pd.DataFrame()
            piv_eod = pd.DataFrame()

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

    all_trades_df, skipped_df, md_df = compute_trade_results(plans)
    actual_trades_df = build_actual_trades_sheet(plans, actual_keys, all_trades_df, skipped_df)

    write_excel(all_trades_df, actual_trades_df, skipped_df, md_df)

    if not all_trades_df.empty:
        print(all_trades_df.groupby("underlying")[["exit_pnl", "eod_pnl"]].describe())
    else:
        print("[WARN] No completed trades. Check 'skipped' sheet for reasons.")


if __name__ == "__main__":
    main()