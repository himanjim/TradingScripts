import os
import time
from datetime import datetime, date, time as dtime, timedelta
from typing import List, Dict, Tuple

import pandas as pd

import Trading_2024.OptionTradeUtils as oUtils


# ========== USER CONFIG ==========
# Underlying index whose options you want (cash/index symbol on NSE/BSE)
# INDEX_EXCHANGE = "NSE"              # e.g. "NSE" for NIFTY/BANKNIFTY indices
INDEX_EXCHANGE = "BSE"
# INDEX_TRADINGSYMBOL = "NIFTY BANK"  # BANKNIFTY index symbol on NSE cash
# INDEX_TRADINGSYMBOL = "NIFTY 50"
INDEX_TRADINGSYMBOL = "SENSEX"
# Options segment details
# OPTION_EXCHANGE = "NFO"                 # "NFO" for index options
OPTION_EXCHANGE = "BFO"
# OPTION_TS_PREFIX = "BANKNIFTY"          # tradingsymbol prefix for this index's options
# OPTION_TS_PREFIX = "NIFTY"
OPTION_TS_PREFIX = "SENSEX"
ALLOWED_OPTION_TYPES = ("CE", "PE")     # Kite option types

# Strike step for this underlying (BANKNIFTY = 100, NIFTY = 50)
STRIKE_STEP = 100
# STRIKE_STEP = 50

# Expiry date for which you want all strikes (BANKNIFTY monthly expiry)
# BANKNIFTY monthly expiry is the LAST TUESDAY of the month.
EXPIRY_DATE_STR = "18-12-2025"          # DD-MM-YYYY  (set this to actual monthly expiry)

# Start date: day after previous monthly expiry
START_DATE_STR = "12-12-2025"

# Output folder and filename
# OUTPUT_DIR = "./BANKNIFTY_20251125_expiry_history"
# OUTPUT_BASENAME = "BANKNIFTY_20251125_minute"   # used as pickle filename
OUTPUT_DIR = "./NIFTY_20251125_expiry_history"
OUTPUT_BASENAME = "SENSEX_20251218_minute"   # used as pickle filename

# Trading session times (IST)
SESSION_START = dtime(9, 15, 0)
SESSION_END   = dtime(15, 30, 0)


# ========== HELPERS ==========
def parse_date_dmy(dstr: str) -> date:
    """Parse DD-MM-YYYY (your config format)."""
    return datetime.strptime(dstr, "%d-%m-%Y").date()


def normalize_expiry(e) -> date:
    """
    Normalize expiry from instruments dump to a date object.
    It can be a date, datetime, or string in ISO / DMY format.
    """
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
        # last resort: try fromisoformat
        try:
            return datetime.fromisoformat(e).date()
        except Exception:
            pass
    raise ValueError(f"Cannot parse expiry value: {e!r}")


def date_chunks(start_dt: datetime, end_dt: datetime, days_per_chunk: int = 30) -> List[Tuple[datetime, datetime]]:
    """Split [start_dt, end_dt] into inclusive chunks of at most days_per_chunk days."""
    chunks: List[Tuple[datetime, datetime]] = []
    cur_start = start_dt
    one_day = timedelta(days=1)
    while cur_start <= end_dt:
        cur_end = min(cur_start + timedelta(days=days_per_chunk) - one_day, end_dt)
        chunks.append((cur_start, cur_end))
        cur_start = cur_end + one_day
    return chunks


def get_instrument_token(kite, exchange: str, tradingsymbol: str) -> Tuple[int, str]:
    """Return (instrument_token, real_exchange) for a given tradingsymbol on a specific exchange."""
    ex = (exchange or "").upper().strip()
    wanted = tradingsymbol.strip()

    print(f"[INFO] Fetching instruments for exchange={ex} to resolve '{wanted}' ...")
    instruments = kite.instruments(ex)
    matches = [r for r in instruments if str(r.get("tradingsymbol", "")).upper() == wanted.upper()]
    if not matches:
        raise ValueError(f"Instrument not found on {ex}: '{wanted}'")
    row = matches[0]
    print(f"[INFO] Resolved {wanted} → token={row['instrument_token']} on exchange={row['exchange']}")
    return int(row["instrument_token"]), row["exchange"]


def fetch_history_minute(kite, instrument_token: int, from_dt: datetime, to_dt: datetime, label: str = "") -> List[Dict]:
    """
    Fetch 1-minute historical data between from_dt and to_dt (inclusive), chunked to avoid limits.
    label: human-readable instrument name for logging.
    """
    interval = "minute"
    sleep_between_calls_sec = 0.25
    chunks = date_chunks(from_dt, to_dt, days_per_chunk=30)

    print(f"[INFO] Fetching {interval} data for {label} (token={instrument_token}) "
          f"from {from_dt} to {to_dt} in {len(chunks)} chunk(s).")

    all_rows: List[Dict] = []
    for idx, (c_from, c_to) in enumerate(chunks, start=1):
        print(f"  [CHUNK {idx}/{len(chunks)}] {c_from} → {c_to}")
        for attempt in range(1, 6):  # up to 5 attempts with simple backoff
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
                break
            except Exception as e:
                wait = attempt * 1.5
                print(f"    [WARN] Error on attempt {attempt} for {label}: {e}. "
                      f"Retrying in {wait:.1f}s ...")
                time.sleep(wait)
        time.sleep(sleep_between_calls_sec)

    print(f"[INFO] Total candles fetched for {label}: {len(all_rows)}")
    return all_rows


def rows_to_dataframe(rows: List[Dict]) -> pd.DataFrame:
    """Convert historical rows to a sorted DataFrame with the usual OHLC columns."""
    if not rows:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
    df = pd.DataFrame(rows)
    for col in ["date", "open", "high", "low", "close", "volume"]:
        if col not in df.columns:
            df[col] = None
    df = df.sort_values("date").reset_index(drop=True)
    return df


def detect_option_type(tradingsymbol: str) -> str:
    """Return 'CE', 'PE', or '' based on the tradingsymbol suffix."""
    s = tradingsymbol.upper()
    if s.endswith("CE"):
        return "CE"
    if s.endswith("PE"):
        return "PE"
    return ""


# ========== CORE LOGIC ==========
def main():
    # --- Parse dates and build datetime range ---
    expiry_date = parse_date_dmy(EXPIRY_DATE_STR)
    start_date = parse_date_dmy(START_DATE_STR)

    if start_date > expiry_date:
        raise ValueError("START_DATE must be on or before EXPIRY_DATE")

    from_dt = datetime.combine(start_date, SESSION_START)
    to_dt = datetime.combine(expiry_date, SESSION_END)

    print("========================================================")
    print("[CONFIG] Date range:")
    print(f"         Start (inclusive): {from_dt}")
    print(f"         End   (inclusive): {to_dt}")
    print("[CONFIG] Underlying index:")
    print(f"         {INDEX_EXCHANGE}:{INDEX_TRADINGSYMBOL}")
    print("[CONFIG] Options universe:")
    print(f"         Exchange={OPTION_EXCHANGE}, ts_prefix={OPTION_TS_PREFIX}, "
          f"allowed_types={ALLOWED_OPTION_TYPES}, strike step={STRIKE_STEP}")
    print(f"[CONFIG] Target expiry date: {expiry_date}")
    print("========================================================")

    # --- Initialise Kite ---
    print("[STEP] Initializing Kite API via OptionTradeUtils.intialize_kite_api() ...")
    kite = oUtils.intialize_kite_api()
    print("[INFO] Kite API initialized.")

    # --- Fetch underlying index history and compute min/max ---
    print("\n[STEP] Resolving underlying index instrument token ...")
    idx_token, idx_ex = get_instrument_token(kite, INDEX_EXCHANGE, INDEX_TRADINGSYMBOL)

    print("\n[STEP] Fetching underlying index historical data ...")
    idx_rows = fetch_history_minute(
        kite,
        idx_token,
        from_dt,
        to_dt,
        label=f"{idx_ex}:{INDEX_TRADINGSYMBOL}"
    )
    idx_df = rows_to_dataframe(idx_rows)

    if idx_df.empty:
        raise RuntimeError("No historical data returned for underlying index in the selected range.")

    low_price = float(idx_df["low"].min())
    high_price = float(idx_df["high"].max())

    print(f"[INFO] Underlying LOW in period : {low_price:.2f}")
    print(f"[INFO] Underlying HIGH in period: {high_price:.2f}")

    # --- Derive strike range: one strike below min, one strike above max ---
    min_strike_base = int(low_price // STRIKE_STEP * STRIKE_STEP)
    max_strike_base = int((high_price + STRIKE_STEP - 1) // STRIKE_STEP * STRIKE_STEP)

    strike_min = max(min_strike_base - STRIKE_STEP, 0)
    strike_max = max_strike_base + STRIKE_STEP

    print(f"[INFO] Strike range (one below min, one above max): {strike_min} → {strike_max} (step {STRIKE_STEP})")

    # Add metadata columns to underlying df
    idx_df.insert(0, "instrument", INDEX_TRADINGSYMBOL)
    idx_df.insert(1, "exchange", idx_ex)
    idx_df.insert(2, "name", INDEX_TRADINGSYMBOL)
    idx_df.insert(3, "type", "UNDERLYING")
    idx_df.insert(4, "option_type", "")
    idx_df.insert(5, "strike", None)
    idx_df.insert(6, "expiry", expiry_date)

    all_dfs = [idx_df]

    # --- Discover all BANKNIFTY options and list expiries ---
    print("\n[STEP] Loading instruments for options from exchange:", OPTION_EXCHANGE)
    all_nfo = kite.instruments(OPTION_EXCHANGE)
    print(f"[INFO] Total instruments on {OPTION_EXCHANGE}: {len(all_nfo)}")

    print(f"[STEP] Filtering instruments for prefix '{OPTION_TS_PREFIX}', types={ALLOWED_OPTION_TYPES} ...")
    bnf_opts = []
    for inst in all_nfo:
        try:
            tsym = str(inst.get("tradingsymbol", "")).upper()
            if not tsym.startswith(OPTION_TS_PREFIX.upper()):
                continue
            itype = str(inst.get("instrument_type", "")).upper()
            if itype not in ALLOWED_OPTION_TYPES:
                # skip futures etc.
                continue
            exp = normalize_expiry(inst.get("expiry"))
            inst["__exp_date__"] = exp
            bnf_opts.append(inst)
        except Exception:
            continue

    if not bnf_opts:
        print(f"[ERROR] No option instruments (types={ALLOWED_OPTION_TYPES}) in {OPTION_EXCHANGE} "
              f"starting with '{OPTION_TS_PREFIX}'.")
        return

    expiry_set = sorted({inst["__exp_date__"] for inst in bnf_opts})
    print(f"[INFO] Available expiries for {OPTION_TS_PREFIX} options on {OPTION_EXCHANGE}:")
    for d in expiry_set:
        print("       ", d)

    if expiry_date not in expiry_set:
        print("\n[ERROR] Your configured EXPIRY_DATE_STR does NOT match any available option expiry.")
        print(f"        Configured: {EXPIRY_DATE_STR} → {expiry_date}")
        print("        Pick one of the above dates and update EXPIRY_DATE_STR, then rerun.")
        return

    print(f"[INFO] Using expiry date {expiry_date} which is present in option instruments.")

    # --- Narrow down to this expiry and strike band ---
    print(f"\n[STEP] Filtering BANKNIFTY options for expiry={expiry_date} and strikes in [{strike_min}, {strike_max}] ...")
    option_instruments = []
    for inst in bnf_opts:
        if inst["__exp_date__"] != expiry_date:
            continue
        try:
            strike = int(float(inst.get("strike") or 0))
        except Exception:
            continue
        if strike < strike_min or strike > strike_max:
            continue
        option_instruments.append(inst)

    if not option_instruments:
        print("[WARN] No option instruments found for the given expiry and strike range.")
        print("       Try widening the strike band or double-checking STRIKE_STEP.")
        return

    option_instruments.sort(key=lambda r: (int(float(r.get("strike", 0))), r.get("tradingsymbol", "")))

    print(f"[INFO] Filtered options count: {len(option_instruments)}")
    strikes = sorted(set(int(float(r.get("strike", 0))) for r in option_instruments))
    print(f"[INFO] Unique strikes in band: {len(strikes)}")
    if len(strikes) <= 30:
        print("       Strikes:", strikes)
    else:
        print("       First 10 strikes:", strikes[:10], "... Last 10 strikes:", strikes[-10:])

    # --- Fetch and accumulate history for each option strike ---
    print("\n[STEP] Fetching historical data for each option instrument in the band ...")
    total_opts = len(option_instruments)
    for idx, inst in enumerate(option_instruments, start=1):
        token = int(inst["instrument_token"])
        sym = inst["tradingsymbol"]
        ex = inst["exchange"]
        strike = int(float(inst.get("strike") or 0))
        exp = inst["__exp_date__"]
        name = inst.get("name")
        opt_type = detect_option_type(sym)

        print(f"\n  [OPTION {idx}/{total_opts}] {ex}:{sym}, strike={strike}, expiry={exp}, type={opt_type}")
        rows = fetch_history_minute(
            kite,
            token,
            from_dt,
            to_dt,
            label=f"{ex}:{sym}"
        )
        df = rows_to_dataframe(rows)

        if df.empty:
            print(f"    [SKIP] No data returned for {sym} in selected range.")
            continue

        # Add metadata columns
        df.insert(0, "instrument", sym)
        df.insert(1, "exchange", ex)
        df.insert(2, "name", name)
        df.insert(3, "type", "OPTION")
        df.insert(4, "option_type", opt_type)
        df.insert(5, "strike", strike)
        df.insert(6, "expiry", exp)

        print(f"    [INFO] Candles for {sym}: {len(df)}")
        all_dfs.append(df)

    # --- Concatenate all into a single DataFrame and save as pickle ---
    print("\n[STEP] Concatenating all instruments into a single DataFrame ...")
    if not all_dfs:
        print("[ERROR] No dataframes to concatenate. Nothing to save.")
        return

    master_df = pd.concat(all_dfs, ignore_index=True)
    print(f"[INFO] Master DataFrame shape: {master_df.shape[0]} rows × {master_df.shape[1]} columns")

    # Ensure date column is datetime
    if "date" in master_df.columns:
        master_df["date"] = pd.to_datetime(master_df["date"])

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    pickle_path = os.path.join(OUTPUT_DIR, f"{OUTPUT_BASENAME}.pkl")

    print(f"[STEP] Saving master DataFrame to pickle: {pickle_path}")
    master_df.to_pickle(pickle_path)
    print("[DONE] Saved successfully.")

    print("\n[HOW TO USE LATER]")
    print("  import pandas as pd")
    print(f"  df = pd.read_pickle(r'{pickle_path}')")
    print("  # Now filter by instrument / type / strike / expiry as needed.")


if __name__ == "__main__":
    main()
