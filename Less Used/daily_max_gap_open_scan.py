import os
import glob
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import Dict, Tuple

import pandas as pd


# ================== USER CONFIG ==================

PARQUET_DIR = "1-min stocks trading/stock_history_parquet"
OUTPUT_CSV  = "daily_max_gap_open.csv"

# Time interpretation of stored parquet timestamps:
# "AUTO": if session seems to start at ~03:45, shift +5:30 (UTC->IST)
# "IST" : assume already IST-naive (09:15 etc.)
# "UTC" : assume UTC-naive, always shift +5:30
DATA_TIME_MODE = "AUTO"  # "AUTO" | "IST" | "UTC"
IST_OFFSET = timedelta(hours=5, minutes=30)

LOG_EVERY_N_FILES = 25


# ================== LOGGING ==================

def log(level: str, msg: str):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{now} [{level}] {msg}")


# ================== FILE HELPERS ==================

def list_parquet_files(parquet_dir: str):
    if not os.path.isdir(parquet_dir):
        raise FileNotFoundError(f"Parquet directory not found: {parquet_dir}")
    files = sorted(glob.glob(os.path.join(parquet_dir, "*.parquet")))
    if not files:
        raise FileNotFoundError(f"No parquet files found in: {parquet_dir}")
    return files


def parse_exchange_symbol_from_filename(path: str) -> Tuple[str, str]:
    base = os.path.splitext(os.path.basename(path))[0]
    if "_" in base:
        ex, sym = base.split("_", 1)
    else:
        ex, sym = "NSE", base
    return ex.upper(), sym


# ================== DATETIME NORMALIZATION ==================

def normalize_date_series(s: pd.Series) -> pd.Series:
    def _drop_tz_python(x):
        if isinstance(x, datetime) and x.tzinfo is not None:
            return x.replace(tzinfo=None)
        return x

    s = s.apply(_drop_tz_python)
    return pd.to_datetime(s, errors="coerce")


def auto_shift_to_ist_if_needed(df: pd.DataFrame, ctx: str = "") -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    df["date"] = normalize_date_series(df["date"])
    df = df.dropna(subset=["date"])

    if DATA_TIME_MODE == "IST":
        return df

    if DATA_TIME_MODE == "UTC":
        df["date"] = df["date"] + IST_OFFSET
        return df

    # AUTO detection: check earliest time on first available day
    first_day = df["date"].dt.date.min()
    day_df = df[df["date"].dt.date == first_day]
    if day_df.empty:
        return df

    t0 = day_df["date"].min().time()
    if t0.hour == 3 and 35 <= t0.minute <= 55:
        log("INFO", f"{ctx}: AUTO TZ detected UTC-like session start ({t0}). Shifting +05:30 to IST.")
        df["date"] = df["date"] + IST_OFFSET

    return df


# ================== DAILY OHLC FROM 1-MIN ==================

def compute_daily_ohlc_from_minute(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["day", "open", "high", "low", "close"])

    df = df.sort_values("date").reset_index(drop=True)
    df["day"] = df["date"].dt.date

    daily = df.groupby("day", as_index=False).agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
    )
    return daily


# ================== CORE LOGIC ==================

@dataclass
class BestGap:
    abs_gap_pct: float
    gap_pct: float
    symbol: str
    day: date
    oc_pct: float
    lh_pct: float


def update_best_by_day(best_by_day: Dict[date, BestGap], candidate: BestGap):
    prev = best_by_day.get(candidate.day)
    if prev is None or candidate.abs_gap_pct > prev.abs_gap_pct:
        best_by_day[candidate.day] = candidate


def scan_all_files_and_find_daily_best_gap(parquet_dir: str) -> Dict[date, BestGap]:
    files = list_parquet_files(parquet_dir)
    log("STEP", f"Found {len(files)} parquet files in {parquet_dir}")

    best_by_day: Dict[date, BestGap] = {}

    for i, path in enumerate(files, start=1):
        ex, sym = parse_exchange_symbol_from_filename(path)
        tag = f"{ex}:{sym}"

        try:
            df = pd.read_parquet(
                path,
                columns=["date", "open", "high", "low", "close"],
                engine="pyarrow",
            )

            df = auto_shift_to_ist_if_needed(df, ctx=tag)
            if df.empty:
                continue

            daily = compute_daily_ohlc_from_minute(df)
            if daily.empty or len(daily) < 2:
                continue

            daily = daily.sort_values("day").reset_index(drop=True)
            daily["prev_close"] = daily["close"].shift(1)

            # ✅ Selection metric: % gap at open vs previous close
            daily["gap_pct"] = (daily["open"] - daily["prev_close"]) / daily["prev_close"] * 100.0

            # Output metrics
            daily["oc_pct"] = (daily["close"] - daily["open"]) / daily["open"] * 100.0
            daily["lh_pct"] = (daily["high"] - daily["low"]) / daily["low"] * 100.0

            valid = daily.dropna(subset=["prev_close", "gap_pct", "oc_pct", "lh_pct"])
            for _, r in valid.iterrows():
                d = r["day"]
                gap = float(r["gap_pct"])
                cand = BestGap(
                    abs_gap_pct=float(abs(gap)),
                    gap_pct=gap,
                    symbol=sym,
                    day=d,
                    oc_pct=float(r["oc_pct"]),
                    lh_pct=float(r["lh_pct"]),
                )
                update_best_by_day(best_by_day, cand)

        except Exception as e:
            log("ERROR", f"Failed for {tag}: {e}")

        if i % LOG_EVERY_N_FILES == 0:
            log("STEP", f"Processed {i}/{len(files)} files. Days captured so far: {len(best_by_day)}")

    log("INFO", f"Scan complete. Total unique days with a best gapper: {len(best_by_day)}")
    return best_by_day


def write_output_csv(best_by_day: Dict[date, BestGap], out_csv: str):
    rows = []
    for d in sorted(best_by_day.keys()):
        b = best_by_day[d]
        rows.append({
            "Stock name": b.symbol,
            "date": d.isoformat(),
            "% change (open-close)": round(b.oc_pct, 6),
            "% change(low-high)": round(b.lh_pct, 6),
            # If you later want to keep the gap itself, uncomment:
            # "% gap(open-prevClose)": round(b.gap_pct, 6),
        })

    out_df = pd.DataFrame(rows, columns=[
        "Stock name",
        "date",
        "% change (open-close)",
        "% change(low-high)",
    ])

    out_df.to_csv(out_csv, index=False)
    log("INFO", f"Saved CSV: {os.path.abspath(out_csv)}  (rows={len(out_df)})")


# ================== MAIN ==================

def main():
    log("CONFIG", f"PARQUET_DIR={PARQUET_DIR}")
    log("CONFIG", f"OUTPUT_CSV={OUTPUT_CSV}")
    log("CONFIG", f"DATA_TIME_MODE={DATA_TIME_MODE}")

    best_by_day = scan_all_files_and_find_daily_best_gap(PARQUET_DIR)
    write_output_csv(best_by_day, OUTPUT_CSV)


if __name__ == "__main__":
    main()
