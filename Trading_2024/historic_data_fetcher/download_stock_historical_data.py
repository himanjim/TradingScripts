import os
import time
from datetime import datetime, date, time as dtime, timedelta
from typing import List, Dict, Tuple, Optional

import pandas as pd

import Trading_2024.OptionTradeUtils as oUtils


# ============================================================
# USER CONFIGURATION
# ============================================================
# Set all inputs here. No command-line arguments are required.

# ---------- Instrument selection ----------
EXCHANGE = "NSE"                 # Example: NSE, BSE
TRADINGSYMBOL = "RELIANCE"       # Example: RELIANCE, INFY, TCS

# Optional disambiguation filters.
# Keep blank ("") to ignore.
SEGMENT = ""                     # Example: NSE, BSE, NFO-OPT, etc.
INSTRUMENT_TYPE = ""             # Example: EQ, ETF, FUT, CE, PE

# ---------- Date/time range ----------
FROM_DATE = "2022-04-01"         # YYYY-MM-DD or DD-MM-YYYY
TO_DATE = "2026-04-10"           # YYYY-MM-DD or DD-MM-YYYY
FROM_TIME = "09:15:00"           # HH:MM or HH:MM:SS
TO_TIME = "15:30:00"             # HH:MM or HH:MM:SS

# ---------- Output ----------
OUTPUT_DIR = "./stock_1min_history"
OUTPUT_BASENAME = r"C:\Users\himan\Documents\Audacity"             # Leave blank for auto-generated name

# ---------- Save options ----------
SAVE_CSV = False
SAVE_FULL_PICKLE = True
SAVE_SAMPLE_PICKLE = False
SAMPLE_ROWS = 10                 # Number of top rows to include in sample pickle

# ---------- Timestamp handling ----------
# If True:
#   - timezone-aware timestamps are converted to Asia/Kolkata and made tz-naive
# If False:
#   - timestamps are preserved as received
NORMALIZE_TO_IST_NAIVE = False

# ---------- Download behaviour ----------
MAX_DAYS_PER_CHUNK = 25
MAX_ATTEMPTS = 5
SLEEP_BETWEEN_CALLS_SEC = 0.25

# Default market session times used for intermediate chunks
DEFAULT_SESSION_START = dtime(9, 15, 0)
DEFAULT_SESSION_END = dtime(15, 30, 0)


# ============================================================
# DATE/TIME PARSING HELPERS
# ============================================================

def parse_date_str(value: str) -> date:
    """
    Parse a date string in either:
      - YYYY-MM-DD
      - DD-MM-YYYY
    """
    value = value.strip()
    for fmt in ("%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            pass
    raise ValueError(f"Invalid date format: {value!r}. Use YYYY-MM-DD or DD-MM-YYYY")


def parse_time_str(value: str) -> dtime:
    """
    Parse a time string in either:
      - HH:MM
      - HH:MM:SS
    """
    value = value.strip()
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime(value, fmt).time()
        except ValueError:
            pass
    raise ValueError(f"Invalid time format: {value!r}. Use HH:MM or HH:MM:SS")


def build_datetime_range(
    from_date_str: str,
    to_date_str: str,
    from_time_str: str,
    to_time_str: str,
) -> Tuple[datetime, datetime]:
    """
    Build start/end datetimes from configured date and time strings.
    """
    from_d = parse_date_str(from_date_str)
    to_d = parse_date_str(to_date_str)
    from_t = parse_time_str(from_time_str)
    to_t = parse_time_str(to_time_str)

    from_dt = datetime.combine(from_d, from_t)
    to_dt = datetime.combine(to_d, to_t)

    if from_dt > to_dt:
        raise ValueError("FROM_DATE/FROM_TIME must be <= TO_DATE/TO_TIME")

    return from_dt, to_dt


# ============================================================
# CHUNKING LOGIC
# ============================================================

def iter_chunks_by_date(
    from_dt: datetime,
    to_dt: datetime,
    days_per_chunk: int,
) -> List[Tuple[datetime, datetime]]:
    """
    Split a long datetime range into date-based chunks.

    Important detail:
    Intermediate chunk end must be the session end time, not the chunk's
    start time, otherwise intraday candles on the chunk-end date may be lost.
    """
    if from_dt > to_dt:
        raise ValueError("from_dt must be <= to_dt")

    chunks: List[Tuple[datetime, datetime]] = []
    current_date = from_dt.date()
    final_date = to_dt.date()

    while current_date <= final_date:
        chunk_end_date = min(current_date + timedelta(days=days_per_chunk - 1), final_date)

        chunk_from = from_dt if current_date == from_dt.date() else datetime.combine(current_date, DEFAULT_SESSION_START)
        chunk_to = to_dt if chunk_end_date == final_date else datetime.combine(chunk_end_date, DEFAULT_SESSION_END)

        chunks.append((chunk_from, chunk_to))
        current_date = chunk_end_date + timedelta(days=1)

    return chunks


# ============================================================
# INSTRUMENT RESOLUTION
# ============================================================

def load_instruments_for_exchange(kite, exchange: str) -> List[Dict]:
    """
    Load the instruments dump for a given exchange.
    """
    exchange = exchange.upper().strip()
    print(f"[STEP] Loading instruments for exchange: {exchange}")
    instruments = kite.instruments(exchange)
    print(f"[INFO] Total instruments loaded on {exchange}: {len(instruments)}")
    return instruments


def _match_optional_filter(value: str, expected: str) -> bool:
    """
    Case-insensitive optional string filter.
    If expected is blank, treat it as a match.
    """
    if not expected.strip():
        return True
    return str(value).upper().strip() == expected.upper().strip()


def resolve_instrument_by_tradingsymbol(
    instruments: List[Dict],
    exchange: str,
    tradingsymbol: str,
    segment: str = "",
    instrument_type: str = "",
) -> Dict:
    """
    Resolve a unique instrument row using tradingsymbol and optional filters.

    Improvement over the earlier version:
    - does not silently choose the first row when multiple rows remain
    - throws a clear ambiguity error and prints candidates
    """
    exchange = exchange.upper().strip()
    tradingsymbol = tradingsymbol.upper().strip()

    exact_matches = []
    for inst in instruments:
        if str(inst.get("tradingsymbol", "")).upper().strip() != tradingsymbol:
            continue
        if str(inst.get("exchange", "")).upper().strip() != exchange:
            continue
        if not _match_optional_filter(str(inst.get("segment", "")), segment):
            continue
        if not _match_optional_filter(str(inst.get("instrument_type", "")), instrument_type):
            continue
        exact_matches.append(inst)

    if not exact_matches:
        raise ValueError(
            f"No instrument found for tradingsymbol='{tradingsymbol}', "
            f"exchange='{exchange}', segment='{segment}', instrument_type='{instrument_type}'."
        )

    if len(exact_matches) > 1:
        print("[ERROR] Instrument lookup is ambiguous. Matching rows:")
        for i, row in enumerate(exact_matches[:20], start=1):
            print(
                f"  {i}. exchange={row.get('exchange')}, "
                f"segment={row.get('segment')}, "
                f"instrument_type={row.get('instrument_type')}, "
                f"tradingsymbol={row.get('tradingsymbol')}, "
                f"token={row.get('instrument_token')}, "
                f"name={row.get('name')}"
            )
        raise ValueError(
            "Multiple instruments matched the same tradingsymbol. "
            "Set SEGMENT and/or INSTRUMENT_TYPE in the config block."
        )

    return exact_matches[0]


# ============================================================
# HISTORICAL DATA FETCH
# ============================================================

def fetch_history_minute(
    kite,
    instrument_token: int,
    from_dt: datetime,
    to_dt: datetime,
    label: str,
) -> List[Dict]:
    """
    Fetch 1-minute historical candles for a single instrument token.

    The range is downloaded in chunks with retries.
    """
    chunks = iter_chunks_by_date(from_dt, to_dt, days_per_chunk=MAX_DAYS_PER_CHUNK)

    print(
        f"[INFO] Fetching minute candles for {label} "
        f"(token={instrument_token}) from {from_dt} to {to_dt} "
        f"in {len(chunks)} chunk(s)."
    )

    all_rows: List[Dict] = []

    for idx, (chunk_from, chunk_to) in enumerate(chunks, start=1):
        print(f"[CHUNK {idx}/{len(chunks)}] {chunk_from} -> {chunk_to}")

        last_error: Optional[Exception] = None

        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                rows = kite.historical_data(
                    instrument_token=instrument_token,
                    from_date=chunk_from,
                    to_date=chunk_to,
                    interval="minute",
                    continuous=False,
                    oi=False,
                )
                print(f"  [OK] Retrieved {len(rows)} candles on attempt {attempt}")
                all_rows.extend(rows)
                last_error = None
                break

            except Exception as exc:
                last_error = exc
                wait_seconds = min(8.0, 1.5 * attempt)
                print(
                    f"  [WARN] Attempt {attempt}/{MAX_ATTEMPTS} failed: {exc}. "
                    f"Sleeping {wait_seconds:.1f}s"
                )
                time.sleep(wait_seconds)

        if last_error is not None:
            raise RuntimeError(
                f"Failed to fetch chunk {idx}/{len(chunks)} for token {instrument_token}: {last_error}"
            )

        time.sleep(SLEEP_BETWEEN_CALLS_SEC)

    return all_rows


# ============================================================
# DATAFRAME POST-PROCESSING
# ============================================================

def normalize_datetime_series_if_needed(series: pd.Series, make_ist_naive: bool) -> pd.Series:
    """
    Optionally normalize timezone-aware timestamps to Asia/Kolkata and then
    strip timezone information.

    If timestamps are already timezone-naive, they are left unchanged.
    """
    series = pd.to_datetime(series, errors="raise")

    if not make_ist_naive:
        return series

    # For timezone-aware dtype
    try:
        if series.dt.tz is not None:
            return series.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    except Exception:
        pass

    # Already naive
    return series


def rows_to_dataframe(
    rows: List[Dict],
    instrument_row: Dict,
    normalize_to_ist_naive: bool,
) -> pd.DataFrame:
    """
    Convert raw historical rows into a clean DataFrame with metadata columns.
    """
    base_columns = [
        "instrument_token",
        "tradingsymbol",
        "exchange",
        "segment",
        "instrument_type",
        "name",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]

    if not rows:
        return pd.DataFrame(columns=base_columns)

    df = pd.DataFrame(rows)

    # Ensure expected columns exist even if API omits some field unexpectedly
    for col in ["date", "open", "high", "low", "close", "volume"]:
        if col not in df.columns:
            df[col] = None

    df["date"] = normalize_datetime_series_if_needed(df["date"], make_ist_naive=normalize_to_ist_naive)

    # De-duplicate by timestamp and sort
    df = df.drop_duplicates(subset=["date"], keep="last").sort_values("date").reset_index(drop=True)

    # Add metadata columns so the pickle is self-describing
    df.insert(0, "instrument_token", int(instrument_row["instrument_token"]))
    df.insert(1, "tradingsymbol", str(instrument_row.get("tradingsymbol", "")).upper())
    df.insert(2, "exchange", str(instrument_row.get("exchange", "")).upper())
    df.insert(3, "segment", str(instrument_row.get("segment", "")))
    df.insert(4, "instrument_type", str(instrument_row.get("instrument_type", "")))
    df.insert(5, "name", str(instrument_row.get("name", "")))

    df = df[base_columns]
    return df


# ============================================================
# OUTPUT
# ============================================================

def build_output_basename(
    configured_basename: str,
    instrument_row: Dict,
    from_dt: datetime,
    to_dt: datetime,
) -> str:
    """
    Build a stable output base name if user did not explicitly provide one.
    """
    if configured_basename.strip():
        return configured_basename.strip()

    symbol = str(instrument_row.get("tradingsymbol", "")).upper()
    return (
        f"{symbol}_1min_"
        f"{from_dt.strftime('%Y%m%d_%H%M%S')}_to_{to_dt.strftime('%Y%m%d_%H%M%S')}"
    )


def save_outputs(
    df: pd.DataFrame,
    instrument_row: Dict,
    from_dt: datetime,
    to_dt: datetime,
    output_dir: str,
    output_basename: str,
    save_csv: bool,
    save_full_pickle: bool,
    save_sample_pickle: bool,
    sample_rows: int,
) -> None:
    """
    Save the full output and the sample output.
    """
    os.makedirs(output_dir, exist_ok=True)

    basename = build_output_basename(output_basename, instrument_row, from_dt, to_dt)

    if save_csv:
        csv_path = os.path.join(output_dir, f"{basename}.csv")
        df.to_csv(csv_path, index=False)
        print(f"[DONE] CSV saved: {csv_path}")

    if save_full_pickle:
        full_pickle_path = os.path.join(output_dir, f"{basename}.pkl")
        df.to_pickle(full_pickle_path)
        print(f"[DONE] Full pickle saved: {full_pickle_path}")

    if save_sample_pickle:
        sample_df = df.head(sample_rows).copy()
        sample_pickle_path = os.path.join(output_dir, f"{basename}_sample_{len(sample_df)}_rows.pkl")
        sample_df.to_pickle(sample_pickle_path)
        print(f"[DONE] Sample pickle saved: {sample_pickle_path}")

        print(f"\n[INFO] First {len(sample_df)} rows written to sample pickle:")
        print(sample_df.to_string(index=False))


# ============================================================
# MAIN
# ============================================================

def main():
    print("[STEP] Initializing Kite API...")
    kite = oUtils.intialize_kite_api()
    print("[INFO] Kite API initialized.")

    from_dt, to_dt = build_datetime_range(
        from_date_str=FROM_DATE,
        to_date_str=TO_DATE,
        from_time_str=FROM_TIME,
        to_time_str=TO_TIME,
    )

    print("[INFO] Configuration:")
    print(f"       EXCHANGE            : {EXCHANGE}")
    print(f"       TRADINGSYMBOL       : {TRADINGSYMBOL}")
    print(f"       SEGMENT             : {SEGMENT or '(ignored)'}")
    print(f"       INSTRUMENT_TYPE     : {INSTRUMENT_TYPE or '(ignored)'}")
    print(f"       FROM_DT             : {from_dt}")
    print(f"       TO_DT               : {to_dt}")
    print(f"       NORMALIZE_TO_IST    : {NORMALIZE_TO_IST_NAIVE}")
    print(f"       OUTPUT_DIR          : {OUTPUT_DIR}")

    # Load instruments once for the configured exchange
    instruments = load_instruments_for_exchange(kite, EXCHANGE)

    # Resolve the exact instrument row
    instrument_row = resolve_instrument_by_tradingsymbol(
        instruments=instruments,
        exchange=EXCHANGE,
        tradingsymbol=TRADINGSYMBOL,
        segment=SEGMENT,
        instrument_type=INSTRUMENT_TYPE,
    )

    instrument_token = int(instrument_row["instrument_token"])
    label = f"{instrument_row.get('exchange')}:{instrument_row.get('tradingsymbol')}"

    print("[INFO] Resolved instrument:")
    print(f"       token              : {instrument_token}")
    print(f"       exchange           : {instrument_row.get('exchange')}")
    print(f"       segment            : {instrument_row.get('segment')}")
    print(f"       instrument_type    : {instrument_row.get('instrument_type')}")
    print(f"       name               : {instrument_row.get('name')}")

    rows = fetch_history_minute(
        kite=kite,
        instrument_token=instrument_token,
        from_dt=from_dt,
        to_dt=to_dt,
        label=label,
    )

    df = rows_to_dataframe(
        rows=rows,
        instrument_row=instrument_row,
        normalize_to_ist_naive=NORMALIZE_TO_IST_NAIVE,
    )

    if df.empty:
        print("[WARN] No data returned for the configured symbol and date range.")
        return

    print(f"[INFO] Total candles fetched: {len(df)}")
    print(f"[INFO] First candle: {df.iloc[0]['date']}")
    print(f"[INFO] Last candle : {df.iloc[-1]['date']}")

    save_outputs(
        df=df,
        instrument_row=instrument_row,
        from_dt=from_dt,
        to_dt=to_dt,
        output_dir=OUTPUT_DIR,
        output_basename=OUTPUT_BASENAME,
        save_csv=SAVE_CSV,
        save_full_pickle=SAVE_FULL_PICKLE,
        save_sample_pickle=SAVE_SAMPLE_PICKLE,
        sample_rows=SAMPLE_ROWS,
    )

    print("[SUCCESS] Download completed.")


if __name__ == "__main__":
    main()