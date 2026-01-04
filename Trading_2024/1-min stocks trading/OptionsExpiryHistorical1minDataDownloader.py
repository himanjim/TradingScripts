import os
import time
from dataclasses import dataclass
from datetime import datetime, date, time as dtime, timedelta
from typing import Dict, List, Tuple

import pandas as pd

import Trading_2024.OptionTradeUtils as oUtils

try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

try:
    from dateutil.relativedelta import relativedelta  # type: ignore
except Exception:  # pragma: no cover
    relativedelta = None  # type: ignore


# ===================== AUTO CONFIG =====================
# Assumption: you run this script ON the expiry trading day.
#
# Expiry rules (holiday shifts handled implicitly by checking actual expiries in instruments dump):
#   - NIFTY weekly expiry: Tuesday (or previous trading day if Tue holiday)
#   - BANKNIFTY monthly expiry: last Tuesday of the month (or previous trading day if Tue holiday)
#   - SENSEX weekly expiry: Thursday (or previous trading day if Thu holiday)
#
# EXPIRY_DATE = run date (IST by default; override via RUN_DATE).
# START_DATE:
#   - NIFTY/SENSEX: trading day on/before (expiry - 7 calendar days)
#   - BANKNIFTY: trading day on/before (expiry - 1 calendar month)
#
# Optional env overrides:
#   RUN_DATE="YYYY-MM-DD" or "DD-MM-YYYY"
#   TARGET_INDEX="NIFTY" | "BANKNIFTY" | "SENSEX"
#   OUTPUT_DIR="..."
#   OUTPUT_BASENAME="..."
#
# Collision note (last Tuesday): defaults to BANKNIFTY monthly.
# To force NIFTY that day: set TARGET_INDEX=NIFTY


@dataclass(frozen=True)
class AutoConfig:
    label: str
    index_exchange: str
    index_tradingsymbol: str
    option_exchange: str
    option_ts_prefix: str
    strike_step: int
    lookback: str  # "WEEK" or "MONTH"


ALLOWED_OPTION_TYPES = ("CE", "PE")

# Trading session times (IST)
SESSION_START = dtime(9, 15, 0)
SESSION_END = dtime(15, 30, 0)

# Conservative chunk size for minute data
MAX_DAYS_PER_CHUNK = 25

# Retry tuning
MAX_ATTEMPTS = 5
SLEEP_BETWEEN_CALLS_SEC = 0.20


# ========== HELPERS ==========
def normalize_expiry(e) -> date:
    """Normalize expiry field from instruments dump to a date object."""
    if isinstance(e, date) and not isinstance(e, datetime):
        return e
    if isinstance(e, datetime):
        return e.date()
    if isinstance(e, str):
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(e, fmt).date()
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(e).date()
        except Exception:
            pass
    raise ValueError(f"Cannot parse expiry value: {e!r}")


def _iter_chunks_by_date(from_dt: datetime, to_dt: datetime, days_per_chunk: int) -> List[Tuple[datetime, datetime]]:
    """
    Chunk a datetime range by date boundaries without losing intraday minutes.

    IMPORTANT bugfix: do NOT end a chunk at the chunk's start-time (e.g., 09:15),
    otherwise you lose the entire day after 09:15 for each chunk end-date.
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


def get_instrument_token(kite, exchange: str, tradingsymbol: str) -> Tuple[int, str]:
    ex = (exchange or "").upper().strip()
    wanted = tradingsymbol.strip().upper()
    instruments = kite.instruments(ex)
    for r in instruments:
        if str(r.get("tradingsymbol", "")).upper() == wanted:
            return int(r["instrument_token"]), str(r.get("exchange", ex))
    raise ValueError(f"Instrument not found on {ex}: '{tradingsymbol}'")


def fetch_history_minute(kite, instrument_token: int, from_dt: datetime, to_dt: datetime, label: str = "") -> List[Dict]:
    """Fetch 1-minute historical data between from_dt and to_dt, chunked and retried."""
    interval = "minute"
    chunks = _iter_chunks_by_date(from_dt, to_dt, days_per_chunk=MAX_DAYS_PER_CHUNK)

    print(f"[INFO] Fetching {interval} data for {label} (token={instrument_token}) "
          f"from {from_dt} to {to_dt} in {len(chunks)} chunk(s).")

    all_rows: List[Dict] = []
    for idx, (c_from, c_to) in enumerate(chunks, start=1):
        print(f"  [CHUNK {idx}/{len(chunks)}] {c_from} → {c_to}")
        last_err = None
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                rows = kite.historical_data(
                    instrument_token=instrument_token,
                    from_date=c_from,
                    to_date=c_to,
                    interval=interval,
                    continuous=False,
                    oi=False
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
    """Convert historical rows to a sorted DataFrame with OHLCV; de-duplicate by timestamp."""
    if not rows:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    df = pd.DataFrame(rows)
    for col in ["date", "open", "high", "low", "close", "volume"]:
        if col not in df.columns:
            df[col] = None

    df["date"] = pd.to_datetime(df["date"])
    df = df.drop_duplicates(subset=["date"], keep="last").sort_values("date").reset_index(drop=True)
    return df


def detect_option_type(tradingsymbol: str) -> str:
    s = tradingsymbol.upper()
    if s.endswith("CE"):
        return "CE"
    if s.endswith("PE"):
        return "PE"
    return ""


# ---------- AUTO CONFIG HELPERS ----------
def _ist_today() -> date:
    if ZoneInfo is not None:
        try:
            return datetime.now(ZoneInfo("Asia/Kolkata")).date()
        except Exception:
            pass
    return datetime.now().date()


def _parse_run_date_env() -> date:
    raw = (os.environ.get("RUN_DATE") or "").strip()
    if not raw:
        return _ist_today()
    for fmt in ("%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"RUN_DATE must be YYYY-MM-DD or DD-MM-YYYY (got: {raw!r})")


def _last_weekday_of_month(y: int, m: int, weekday: int) -> date:
    if m == 12:
        last = date(y + 1, 1, 1) - timedelta(days=1)
    else:
        last = date(y, m + 1, 1) - timedelta(days=1)
    while last.weekday() != weekday:
        last -= timedelta(days=1)
    return last


def _is_banknifty_monthly_symbol(tsym: str, expiry_date: date) -> bool:
    """
    Zerodha convention:
      - Monthly: BANKNIFTY + YY + MMM + STRIKE + CE/PE
      - Weekly : BANKNIFTY + YY + MMM + DD + STRIKE + CE/PE
    """
    u = tsym.upper()
    yy = expiry_date.strftime("%y")
    mmm = expiry_date.strftime("%b").upper()
    dd = expiry_date.strftime("%d")
    base = f"BANKNIFTY{yy}{mmm}"
    return u.startswith(base) and not u.startswith(base + dd)


def _load_instruments_cached(kite, exchange: str, cache: Dict[str, List[Dict]]) -> List[Dict]:
    ex = exchange.upper().strip()
    if ex not in cache:
        print(f"[STEP] Loading instruments dump for {ex} ...")
        cache[ex] = kite.instruments(ex)
        print(f"[INFO] Total instruments on {ex}: {len(cache[ex])}")
    return cache[ex]


def _count_options_for_expiry(instruments: List[Dict], ts_prefix: str, expiry_date: date) -> Tuple[int, int]:
    """Return (total_count, banknifty_monthly_count)."""
    prefix_u = ts_prefix.upper().strip()
    total = 0
    monthly = 0
    for inst in instruments:
        try:
            tsym = str(inst.get("tradingsymbol", "")).upper()
            if not tsym.startswith(prefix_u):
                continue
            itype = str(inst.get("instrument_type", "")).upper()
            if itype not in ALLOWED_OPTION_TYPES:
                continue
            exp = normalize_expiry(inst.get("expiry"))
            if exp != expiry_date:
                continue
            total += 1
            if prefix_u == "BANKNIFTY" and _is_banknifty_monthly_symbol(tsym, expiry_date):
                monthly += 1
        except Exception:
            continue
    return total, monthly


def pick_autoconfig_for_date(kite, run_date: date) -> AutoConfig:
    forced = (os.environ.get("TARGET_INDEX") or "").strip().upper()
    if forced and forced not in {"NIFTY", "BANKNIFTY", "SENSEX"}:
        raise ValueError("TARGET_INDEX must be NIFTY, BANKNIFTY, or SENSEX")

    cfg_nifty = AutoConfig("NIFTY_WEEKLY", "NSE", "NIFTY 50", "NFO", "NIFTY", 50, "WEEK")
    cfg_bank = AutoConfig("BANKNIFTY_MONTHLY", "NSE", "NIFTY BANK", "NFO", "BANKNIFTY", 100, "MONTH")
    cfg_sensex = AutoConfig("SENSEX_WEEKLY", "BSE", "SENSEX", "BFO", "SENSEX", 100, "WEEK")

    if forced == "NIFTY":
        return cfg_nifty
    if forced == "BANKNIFTY":
        return cfg_bank
    if forced == "SENSEX":
        return cfg_sensex

    cache: Dict[str, List[Dict]] = {}
    nfo = _load_instruments_cached(kite, "NFO", cache)
    try:
        bfo = _load_instruments_cached(kite, "BFO", cache)
    except Exception:
        bfo = []

    sensex_total, _ = _count_options_for_expiry(bfo, "SENSEX", run_date) if bfo else (0, 0)
    bank_total, bank_monthly = _count_options_for_expiry(nfo, "BANKNIFTY", run_date)
    nifty_total, _ = _count_options_for_expiry(nfo, "NIFTY", run_date)

    if sensex_total > 0:
        return cfg_sensex

    if bank_monthly > 0:
        return cfg_bank

    # heuristic last Tuesday (or Monday if last Tuesday is holiday and expiry shifted)
    last_tue = _last_weekday_of_month(run_date.year, run_date.month, weekday=1)
    if bank_total > 0 and (run_date == last_tue or (run_date.weekday() == 0 and run_date + timedelta(days=1) == last_tue)):
        return cfg_bank

    if nifty_total > 0:
        return cfg_nifty

    if bank_total > 0:
        return cfg_bank

    raise RuntimeError(
        f"Could not auto-detect target index for run_date={run_date}. "
        "Set TARGET_INDEX=NIFTY/BANKNIFTY/SENSEX."
    )


def _is_trading_day_probe(kite, idx_token: int, d: date) -> bool:
    if d.weekday() >= 5:
        return False
    try:
        rows = kite.historical_data(
            instrument_token=idx_token,
            from_date=d,
            to_date=d,
            interval="day",
            continuous=False,
            oi=False,
        )
        return bool(rows)
    except Exception:
        return False


def _previous_trading_day(kite, idx_token: int, anchor: date, max_back: int) -> date:
    d = anchor
    for _ in range(max_back):
        if _is_trading_day_probe(kite, idx_token, d):
            return d
        d -= timedelta(days=1)
    raise RuntimeError(f"Unable to find a trading day on/before {anchor} within {max_back} days.")


def compute_start_date(kite, idx_token: int, expiry_date: date, lookback: str) -> date:
    lookback_u = (lookback or "").upper().strip()
    if lookback_u == "WEEK":
        anchor = expiry_date - timedelta(days=7)
        return _previous_trading_day(kite, idx_token, anchor, max_back=15)
    if lookback_u == "MONTH":
        if relativedelta is not None:
            anchor = expiry_date - relativedelta(months=1)
        else:
            anchor = expiry_date - timedelta(days=30)
        return _previous_trading_day(kite, idx_token, anchor, max_back=45)
    raise ValueError(f"Unknown lookback: {lookback!r}")


# ========== CORE LOGIC ==========
def main():
    print("[STEP] Initializing Kite API ...")
    kite = oUtils.intialize_kite_api()
    print("[INFO] Kite API initialized.")

    run_date = _parse_run_date_env()
    expiry_date = run_date
    cfg = pick_autoconfig_for_date(kite, run_date)

    print("========================================================")
    print("[CONFIG] Auto-selected target:", cfg.label)
    print("[CONFIG] Underlying:", f"{cfg.index_exchange}:{cfg.index_tradingsymbol}")
    print("[CONFIG] Options:", f"{cfg.option_exchange} prefix={cfg.option_ts_prefix} step={cfg.strike_step}")
    print("[CONFIG] Expiry date:", expiry_date)
    print("========================================================")

    idx_token, idx_ex = get_instrument_token(kite, cfg.index_exchange, cfg.index_tradingsymbol)

    start_date = compute_start_date(kite, idx_token, expiry_date, cfg.lookback)

    from_dt = datetime.combine(start_date, SESSION_START)
    to_dt = datetime.combine(expiry_date, SESSION_END)

    output_dir = (os.environ.get("OUTPUT_DIR") or "").strip() or f"./{cfg.option_ts_prefix}_{expiry_date:%Y%m%d}_expiry_history"
    output_basename = (os.environ.get("OUTPUT_BASENAME") or "").strip() or f"{cfg.option_ts_prefix}_{expiry_date:%Y%m%d}_minute"

    print("[CONFIG] Date range:", from_dt, "→", to_dt)
    print("[CONFIG] Output:", output_dir, "/", output_basename + ".pkl")

    # Underlying
    print("\n[STEP] Fetching underlying index minute data ...")
    idx_rows = fetch_history_minute(kite, idx_token, from_dt, to_dt, label=f"{idx_ex}:{cfg.index_tradingsymbol}")
    idx_df = rows_to_dataframe(idx_rows)
    if idx_df.empty:
        raise RuntimeError("No underlying data returned for selected range.")

    low_price = float(idx_df["low"].min())
    high_price = float(idx_df["high"].max())
    print(f"[INFO] Underlying LOW/HIGH in period: {low_price:.2f} / {high_price:.2f}")

    # Strike band
    step = cfg.strike_step
    min_strike_base = int(low_price // step * step)
    max_strike_base = int((high_price + step - 1) // step * step)
    strike_min = max(min_strike_base - step, 0)
    strike_max = max_strike_base + step
    print(f"[INFO] Strike band: {strike_min} → {strike_max} (step {step})")

    idx_df.insert(0, "instrument", cfg.index_tradingsymbol)
    idx_df.insert(1, "exchange", idx_ex)
    idx_df.insert(2, "name", cfg.index_tradingsymbol)
    idx_df.insert(3, "type", "UNDERLYING")
    idx_df.insert(4, "option_type", "")
    idx_df.insert(5, "strike", None)
    idx_df.insert(6, "expiry", expiry_date)

    all_dfs = [idx_df]

    # Options
    print("\n[STEP] Loading option instruments:", cfg.option_exchange)
    all_opts = kite.instruments(cfg.option_exchange)
    print(f"[INFO] Instruments on {cfg.option_exchange}: {len(all_opts)}")

    prefix_u = cfg.option_ts_prefix.upper()
    filtered = []
    expiry_set = set()

    for inst in all_opts:
        try:
            tsym = str(inst.get("tradingsymbol", "")).upper()
            if not tsym.startswith(prefix_u):
                continue
            itype = str(inst.get("instrument_type", "")).upper()
            if itype not in ALLOWED_OPTION_TYPES:
                continue
            exp = normalize_expiry(inst.get("expiry"))
            expiry_set.add(exp)
            if exp != expiry_date:
                continue
            strike = int(float(inst.get("strike") or 0))
            if strike_min <= strike <= strike_max:
                inst["__exp_date__"] = exp
                inst["__strike_i__"] = strike
                filtered.append(inst)
        except Exception:
            continue

    if expiry_date not in expiry_set:
        print("[ERROR] No options found with expiry equal to run date.")
        print("        Run date:", expiry_date)
        print("        Available expiries:", sorted(expiry_set))
        print("        If you're backfilling, set RUN_DATE=YYYY-MM-DD and rerun.")
        return

    if not filtered:
        print("[WARN] No options found for expiry + strike band. Consider widening strike band.")
        return

    filtered.sort(key=lambda r: (r["__strike_i__"], r.get("tradingsymbol", "")))
    print(f"[INFO] Options to download: {len(filtered)}")

    print("\n[STEP] Fetching 1-min history for options ...")
    total = len(filtered)
    for i, inst in enumerate(filtered, start=1):
        token = int(inst["instrument_token"])
        sym = inst["tradingsymbol"]
        ex = inst["exchange"]
        strike = inst["__strike_i__"]
        exp = inst["__exp_date__"]
        name = inst.get("name")
        opt_type = detect_option_type(sym)

        print(f"\n  [OPTION {i}/{total}] {ex}:{sym} strike={strike} type={opt_type}")
        rows = fetch_history_minute(kite, token, from_dt, to_dt, label=f"{ex}:{sym}")
        df = rows_to_dataframe(rows)
        if df.empty:
            print("    [SKIP] no candles")
            continue

        df.insert(0, "instrument", sym)
        df.insert(1, "exchange", ex)
        df.insert(2, "name", name)
        df.insert(3, "type", "OPTION")
        df.insert(4, "option_type", opt_type)
        df.insert(5, "strike", strike)
        df.insert(6, "expiry", exp)

        all_dfs.append(df)

    print("\n[STEP] Concatenating & saving ...")
    master_df = pd.concat(all_dfs, ignore_index=True)
    master_df["date"] = pd.to_datetime(master_df["date"])

    os.makedirs(output_dir, exist_ok=True)
    pickle_path = os.path.join(output_dir, f"{output_basename}.pkl")
    master_df.to_pickle(pickle_path)

    print("[DONE] Saved:", pickle_path)
    print("Rows:", len(master_df))


if __name__ == "__main__":
    main()
