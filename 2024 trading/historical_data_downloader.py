"""
Download 1-minute historical candles from Zerodha Kite for one or more instruments.
Now supports loading symbols from a CSV file as well.

USAGE (interactive):
    python download_kite_history.py

You will be prompted for:
- Exchange (NFO/NSE/BSE/CDS/MCX or AUTO)
- Instrument names (comma-separated)  [optional if CSV is provided]
- CSV path with symbols               [optional]
- From datetime (e.g., "22-08-2025 09:15:00")
- To datetime   (e.g., "26-08-2025 15:30:00")
- Output folder (e.g., "C:/Users/you/Downloads")

Notes:
- Interval is "minute" (1-min candles).
- Auto-chunks long ranges (30 days per request).
- Requires OptionTradeUtils.py with intialize_kite_api().
"""

import os
import time
import csv
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional

import pytz
import pandas as pd

# --- Your utility that returns an authenticated KiteConnect instance ---
import OptionTradeUtils as oUtils


# ========= Helpers =========
def parse_datetime_ist(dt_str: str) -> datetime:
    """Parse 'DD-MM-YYYY HH:MM:SS' into naive IST datetime (as Kite expects)."""
    ist = pytz.timezone('Asia/Calcutta')
    dt = datetime.strptime(dt_str.strip(), "%d-%m-%Y %H:%M:%S")
    dt_aware = ist.localize(dt)
    return dt_aware.replace(tzinfo=None)


def date_chunks(start_dt: datetime, end_dt: datetime, days_per_chunk: int = 30) -> List[Tuple[datetime, datetime]]:
    """Split [start_dt, end_dt] into inclusive chunks of days_per_chunk."""
    chunks = []
    cur_start = start_dt
    one_day = timedelta(days=1)
    while cur_start <= end_dt:
        cur_end = min(cur_start + timedelta(days=days_per_chunk) - one_day, end_dt)
        chunks.append((cur_start, cur_end))
        cur_start = cur_end + one_day
    return chunks


def load_symbols_from_csv(csv_path: str, column_hint: Optional[str] = None) -> List[str]:
    """
    Load symbols from a CSV file. Tries to auto-detect a reasonable column if not provided.
    Priority order: column_hint, 'tradingsymbol', 'symbol', 'instrument', 'Instrument', first column.
    """
    if not os.path.isfile(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)
    if df.empty:
        return []

    candidates = []
    if column_hint and column_hint in df.columns:
        candidates.append(column_hint)

    # common column names people use
    common_cols = ["tradingsymbol", "symbol", "instrument", "Instrument", "TradingSymbol", "Symbol"]
    for c in common_cols:
        if c in df.columns and c not in candidates:
            candidates.append(c)

    # fallback to first column
    if not candidates:
        candidates = [df.columns[0]]

    col = candidates[0]
    vals = (
        df[col]
        .dropna()
        .astype(str)
        .map(str.strip)
        .tolist()
    )
    # remove empties and dedupe preserving order
    seen = set()
    cleaned = []
    for v in vals:
        if v and v not in seen:
            seen.add(v)
            cleaned.append(v)
    return cleaned


def get_instrument_token(kite, exchange: str, tradingsymbol: str, _cache: Dict[str, List[Dict]] = {}) -> Tuple[int, str]:
    """
    Return (instrument_token, real_exchange). If exchange is 'AUTO' or invalid,
    search across all exchanges loaded by kite.instruments().
    """
    wanted = tradingsymbol.strip()
    ex = (exchange or "").upper().strip()

    def load_all():
        if "_ALL_" not in _cache:
            print("Fetching instruments across all exchanges ...")
            _cache["_ALL_"] = kite.instruments()  # all exchanges
            print(f"Loaded {_cache['_ALL_'].__len__()} total rows.")
        return _cache["_ALL_"]

    candidates = []
    if ex in ("NSE", "NFO", "BSE", "CDS", "MCX"):
        if ex not in _cache:
            print(f"Fetching instruments for exchange: {ex} ...")
            _cache[ex] = kite.instruments(ex)
            print(f"Loaded {len(_cache[ex])} instruments for {ex}.")
        arr = _cache[ex]
        candidates = [r for r in arr if r.get("tradingsymbol") == wanted] or \
                     [r for r in arr if str(r.get("tradingsymbol","")).upper() == wanted.upper()]
    else:
        arr = load_all()
        candidates = [r for r in arr if r.get("tradingsymbol") == wanted] or \
                     [r for r in arr if str(r.get("tradingsymbol","")).upper() == wanted.upper()]

    if not candidates:
        arr = _cache.get(ex, _cache.get("_ALL_", load_all()))
        near = [r for r in arr if str(r.get("tradingsymbol","")).upper().startswith(wanted.upper()[:6])]
        hints = ", ".join(sorted({r["exchange"]+":"+r["tradingsymbol"] for r in near})[:8])
        raise ValueError(
            f"Instrument not found: tradingsymbol='{wanted}' (exchange='{ex or 'AUTO'}'). "
            f"{'Zerodha historical does not cover BSE SENSEX options; use NFO (NIFTY/BANKNIFTY/FINNIFTY/MIDCPNIFTY).' if 'SENSEX' in wanted.upper() else ''} "
            f"{'Nearby: ' + hints if hints else 'No nearby symbols found.'}"
        )

    r = candidates[0]
    return int(r["instrument_token"]), r["exchange"]


def fetch_history_minute(kite, instrument_token: int, from_dt: datetime, to_dt: datetime) -> List[Dict]:
    """Pull 1-min history across chunks safely; returns list of OHLC dicts with 'date' key."""
    interval = "minute"
    sleep_between_calls_sec = 0.25
    chunks = date_chunks(from_dt, to_dt, days_per_chunk=30)

    all_rows: List[Dict] = []
    for (c_from, c_to) in chunks:
        print(f"  → Fetching {interval} data: {c_from} to {c_to}")
        for attempt in range(1, 6):  # retry with simple backoff
            try:
                rows = kite.historical_data(
                    instrument_token=instrument_token,
                    from_date=c_from,
                    to_date=c_to,
                    interval=interval,
                    continuous=False,
                    oi=False
                )
                all_rows.extend(rows)
                break
            except Exception as e:
                wait = attempt * 1.5
                print(f"    ! Error on attempt {attempt}: {e}. Retrying in {wait:.1f}s ...")
                time.sleep(wait)
        time.sleep(sleep_between_calls_sec)

    return all_rows


def rows_to_dataframe(rows: List[Dict]) -> pd.DataFrame:
    """Convert Kite historical rows into a tidy DataFrame."""
    if not rows:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
    df = pd.DataFrame(rows)
    for col in ["date", "open", "high", "low", "close", "volume"]:
        if col not in df.columns:
            df[col] = None
    df = df.sort_values("date").reset_index(drop=True)
    return df


def safe_filename(s: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in s)


def save_csv(df: pd.DataFrame, out_dir: str, tradingsymbol: str, from_dt: datetime, to_dt: datetime, interval: str = "minute") -> str:
    os.makedirs(out_dir, exist_ok=True)
    fn = f"{safe_filename(tradingsymbol)}_{interval}_{from_dt.strftime('%Y%m%d%H%M%S')}_{to_dt.strftime('%Y%m%d%H%M%S')}.csv"
    path = os.path.join(out_dir, fn)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    df.to_csv(path, index=False, quoting=csv.QUOTE_MINIMAL)
    return path


# ========= Main (interactive prompts) =========
def main():
    print("=== Zerodha Kite 1-minute Historical Downloader ===\n")

    # TIP: Keep exchange as AUTO unless you know the exact venue
    exchange = input("Exchange [NFO/NSE/BSE/CDS/MCX or AUTO]: ").strip() or "AUTO"

    # Optional CSV file with symbols
    csv_path = input("CSV path with symbols (optional, press Enter to skip): ").strip()
    csv_col_hint = ""
    if csv_path:
        csv_col_hint = input("Column name in CSV for symbols (optional, auto-detect if blank): ").strip()

    # Optional manual symbols
    instruments_raw = input(
        "Instrument name(s), comma-separated (exact 'tradingsymbol'). [optional if CSV is given]\n"
        "Examples (NFO):  NIFTY25AUG2481000CE, BANKNIFTY25AUG2447000PE\n"
        "Examples (NSE):  RELIANCE, INFY, TCS\n"
        "Enter: "
    ).strip()

    from_str = input("From datetime (DD-MM-YYYY HH:MM:SS), e.g., 22-08-2025 09:15:00: ").strip()
    to_str   = input("To   datetime (DD-MM-YYYY HH:MM:SS), e.g., 26-08-2025 15:30:00: ").strip()
    out_dir  = input("Output folder [default: current folder]: ").strip() or os.getcwd()

    try:
        from_dt = parse_datetime_ist(from_str)
        to_dt   = parse_datetime_ist(to_str)
    except Exception as e:
        print(f"Invalid datetime input: {e}")
        sys.exit(1)
    if to_dt < from_dt:
        print("Error: 'to' datetime is earlier than 'from' datetime.")
        sys.exit(1)

    # Build symbol list
    instruments: List[str] = []
    if csv_path:
        try:
            from_csv = load_symbols_from_csv(csv_path, csv_col_hint if csv_col_hint else None)
            print(f"Loaded {len(from_csv)} symbol(s) from CSV.")
            instruments.extend(from_csv)
        except Exception as e:
            print(f"Warning: could not load CSV symbols: {e}")

    if instruments_raw:
        manual_syms = [s.strip() for s in instruments_raw.split(",") if s.strip()]
        instruments.extend(manual_syms)

    # Deduplicate preserving order
    seen = set()
    instruments = [s for s in instruments if not (s in seen or seen.add(s))]

    if not instruments:
        print("No instruments provided (CSV and manual both empty). Exiting.")
        sys.exit(1)

    print("\nInitializing Kite API via OptionTradeUtils ...")
    kite = oUtils.intialize_kite_api()
    print("Kite initialized.\n")

    for sym in instruments:
        print(f"=== {exchange}:{sym} ===")
        try:
            token, real_ex = get_instrument_token(kite, exchange, sym)
            print(f"Match → {real_ex}:{sym} (token {token})")
        except Exception as e:
            print(f"Skipping '{sym}': {e}")
            continue

        rows = fetch_history_minute(kite, token, from_dt, to_dt)
        df = rows_to_dataframe(rows)

        if df.empty:
            print(f"No data returned for {sym} in the selected range.")
            continue

        df.insert(0, "instrument", sym)
        df.insert(1, "exchange", real_ex)

        out_path = save_csv(df, out_dir, sym, from_dt, to_dt, interval="minute")
        print(f"Saved: {out_path}\n")

    print("Done.")


if __name__ == "__main__":
    main()
