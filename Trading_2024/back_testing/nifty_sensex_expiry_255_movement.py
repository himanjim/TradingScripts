#!/usr/bin/env python3
"""
Download four years of NIFTY 50 and SENSEX 1-minute index data from Kite
and create an Excel event study of movement after 14:55 on:

    D0   = the weekly expiry session
    D-1  = the previous trading session
    D-2  = the second previous trading session

The weekly-expiry weekday regimes and holiday-backshift concept are based on
DhanExpiredOptionsDataFetcher.py. This script downloads only the underlying
indices through Kite; it does not download option candles.

Movement definitions
--------------------
Baseline:
    Close of the 14:55 candle (or the first available candle before 15:00).

Close movement:
    Final regular-session candle close minus the baseline.

Maximum upside/downside:
    Highest high / lowest low in candles strictly after the baseline candle,
    measured relative to the baseline close.

Required packages
-----------------
    pip install kiteconnect pandas numpy xlsxwriter

Kite initialization
-------------------
Kite is initialized through the project's existing helper, matching
download_stocks_1min.py:

    import Trading_2024.OptionTradeUtils as oUtils
    kite = oUtils.intialize_kite_api()

Authentication and token handling therefore remain entirely inside
OptionTradeUtils.

Examples (PowerShell)
---------------------
    python .\\nifty_sensex_expiry_255_movement.py

    python .\\nifty_sensex_expiry_255_movement.py `
        --end-date 2026-07-22 `
        --output .\\expiry_movement.xlsx

The per-chunk cache makes interrupted downloads resumable. Use
--force-download to replace cached Kite responses.
"""

from __future__ import annotations

import argparse
import bisect
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import Trading_2024.OptionTradeUtils as oUtils

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - Python 3.8 fallback
    ZoneInfo = None  # type: ignore[assignment]


# =============================================================================
# CONFIGURATION
# =============================================================================

IST = "Asia/Kolkata"
SESSION_START = dtime(9, 15)
MOVEMENT_START = dtime(14, 55)
LATEST_ALLOWED_ENTRY = dtime(14, 59)
SESSION_END = dtime(15, 30)
EXPECTED_LAST_CANDLE = dtime(15, 25)

DEFAULT_LOOKBACK_YEARS = 4
MAX_DAYS_PER_CHUNK = 25
MAX_ATTEMPTS = 5
SLEEP_BETWEEN_CALLS_SEC = 0.40  # safely below Kite's usual 3 req/sec pace
MAX_EXPIRY_SHIFT_BACK_DAYS = 7

NIFTY_CUTOVER = date(2025, 9, 1)       # Thursday -> Tuesday
SENSEX_CUTOVER_1 = date(2025, 1, 1)    # Friday -> Tuesday
SENSEX_CUTOVER_2 = date(2025, 9, 1)    # Tuesday -> Thursday
SENSEX_WEEKLY_START = date(2023, 5, 1)

DETAIL_COLUMNS = [
    "symbol",
    "scheduled_expiry",
    "actual_expiry",
    "expiry_shift_days",
    "day_role",
    "day_offset",
    "trading_date",
    "entry_timestamp",
    "entry_close",
    "exit_timestamp",
    "exit_close",
    "close_change_points",
    "close_change_pct",
    "absolute_close_change_points",
    "absolute_close_change_pct",
    "max_up_points",
    "max_up_pct",
    "max_up_timestamp",
    "max_down_points",
    "max_down_pct",
    "max_down_timestamp",
    "largest_excursion_points",
    "largest_excursion_pct",
    "largest_excursion_direction",
    "close_direction",
    "observed_minutes",
    "data_status",
]


@dataclass(frozen=True)
class IndexSpec:
    label: str
    exchange: str
    tradingsymbol: str


INDEX_SPECS = (
    IndexSpec("NIFTY", "NSE", "NIFTY 50"),
    IndexSpec("SENSEX", "BSE", "SENSEX"),
)


# =============================================================================
# DATE AND EXPIRY HELPERS
# =============================================================================

def ist_now() -> datetime:
    if ZoneInfo is not None:
        return datetime.now(ZoneInfo(IST))
    return datetime.now()


def default_complete_end_date() -> date:
    """Avoid treating an unfinished current session as complete."""
    now = ist_now()
    if now.time().replace(tzinfo=None) < SESSION_END:
        return now.date() - timedelta(days=1)
    return now.date()


def subtract_years(d: date, years: int) -> date:
    try:
        return d.replace(year=d.year - years)
    except ValueError:  # 29 February -> 28 February
        return d.replace(year=d.year - years, month=2, day=28)


def week_monday(d: date) -> date:
    return d - timedelta(days=d.weekday())


def expiry_weekday(symbol: str, monday: date) -> int:
    """Return Monday=0 ... Sunday=6 using the supplied reference regimes."""
    if symbol == "NIFTY":
        return 3 if monday < NIFTY_CUTOVER else 1
    if symbol == "SENSEX":
        if monday < SENSEX_CUTOVER_1:
            return 4
        if monday < SENSEX_CUTOVER_2:
            return 1
        return 3
    raise KeyError(symbol)


def scheduled_weekly_expiries(symbol: str, start: date, end: date) -> List[date]:
    if symbol == "SENSEX":
        start = max(start, SENSEX_WEEKLY_START)
    out: List[date] = []
    monday = week_monday(start)
    while monday <= end:
        candidate = monday + timedelta(days=expiry_weekday(symbol, monday))
        if start <= candidate <= end:
            out.append(candidate)
        monday += timedelta(days=7)
    return out


def iter_date_chunks(start: date, end: date, days_per_chunk: int) -> Iterable[Tuple[date, date]]:
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=days_per_chunk - 1), end)
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


# =============================================================================
# KITE DOWNLOAD
# =============================================================================

def initialize_kite() -> Any:
    """Initialize Kite using the same project helper as download_stocks_1min.py."""
    return oUtils.intialize_kite_api()


def resolve_instrument_token(kite: Any, spec: IndexSpec) -> int:
    wanted = spec.tradingsymbol.upper()
    rows = kite.instruments(spec.exchange)
    matches = [
        row
        for row in rows
        if str(row.get("tradingsymbol", "")).strip().upper() == wanted
    ]
    if not matches:
        raise RuntimeError(
            f"Kite instrument not found: {spec.exchange}:{spec.tradingsymbol}"
        )
    return int(matches[0]["instrument_token"])


def normalize_kite_rows(rows: Sequence[Mapping[str, Any]]) -> pd.DataFrame:
    columns = ["timestamp", "open", "high", "low", "close", "volume", "trading_date"]
    if not rows:
        return pd.DataFrame(columns=columns)

    df = pd.DataFrame(rows).rename(columns={"date": "timestamp"})
    required = ["timestamp", "open", "high", "low", "close"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise RuntimeError(f"Kite response is missing columns: {missing}")

    parsed = pd.to_datetime(df["timestamp"], errors="coerce")
    try:
        if parsed.dt.tz is not None:
            parsed = parsed.dt.tz_convert(IST).dt.tz_localize(None)
    except (AttributeError, TypeError):
        parsed = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
        parsed = parsed.dt.tz_convert(IST).dt.tz_localize(None)
    df["timestamp"] = parsed

    if "volume" not in df.columns:
        df["volume"] = np.nan
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["timestamp", "open", "high", "low", "close"])
    df = df.drop_duplicates(subset=["timestamp"], keep="last").sort_values("timestamp")
    times = df["timestamp"].dt.time
    df = df[(times >= MOVEMENT_START) & (times <= SESSION_END)].copy()
    df["trading_date"] = df["timestamp"].dt.date
    return df[columns].reset_index(drop=True)


def fetch_kite_chunk(
    kite: Any,
    instrument_token: int,
    chunk_start: date,
    chunk_end: date,
    label: str,
) -> pd.DataFrame:
    from_dt = datetime.combine(chunk_start, SESSION_START)
    to_dt = datetime.combine(chunk_end, SESSION_END)
    last_error: Optional[Exception] = None

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            rows = kite.historical_data(
                instrument_token=instrument_token,
                from_date=from_dt,
                to_date=to_dt,
                interval="minute",
                continuous=False,
                oi=False,
            )
            return normalize_kite_rows(rows)
        except Exception as exc:  # API exception classes vary by kiteconnect version.
            last_error = exc
            wait = min(12.0, 1.5 * (2 ** (attempt - 1)))
            print(
                f"    [WARN] {label} attempt {attempt}/{MAX_ATTEMPTS} failed: "
                f"{exc}; retrying in {wait:.1f}s"
            )
            time.sleep(wait)

    raise RuntimeError(
        f"Kite download failed for {label}, {chunk_start}..{chunk_end}"
    ) from last_error


def download_late_session_history(
    kite: Any,
    spec: IndexSpec,
    start: date,
    end: date,
    cache_dir: Path,
    force_download: bool,
) -> pd.DataFrame:
    token = resolve_instrument_token(kite, spec)
    symbol_cache = cache_dir / spec.label.lower()
    symbol_cache.mkdir(parents=True, exist_ok=True)
    chunks = list(iter_date_chunks(start, end, MAX_DAYS_PER_CHUNK))
    parts: List[pd.DataFrame] = []

    print(
        f"[INFO] {spec.label}: token={token}, {start}..{end}, "
        f"chunks={len(chunks)}"
    )
    for number, (chunk_start, chunk_end) in enumerate(chunks, start=1):
        cache_file = symbol_cache / (
            f"{chunk_start:%Y%m%d}_{chunk_end:%Y%m%d}_1455.pkl"
        )
        if cache_file.exists() and not force_download:
            part = pd.read_pickle(cache_file)
            print(f"  [CACHE {number:02d}/{len(chunks):02d}] {cache_file.name}")
        else:
            print(
                f"  [FETCH {number:02d}/{len(chunks):02d}] "
                f"{chunk_start} -> {chunk_end}"
            )
            part = fetch_kite_chunk(
                kite, token, chunk_start, chunk_end, spec.label
            )
            temp_file = cache_file.with_suffix(".tmp")
            part.to_pickle(temp_file)
            os.replace(temp_file, cache_file)
            time.sleep(SLEEP_BETWEEN_CALLS_SEC)
        parts.append(part)

    if not parts:
        return normalize_kite_rows([])
    result = pd.concat(parts, ignore_index=True)
    result = (
        result.drop_duplicates(subset=["timestamp"], keep="last")
        .sort_values("timestamp")
        .reset_index(drop=True)
    )
    if result.empty:
        raise RuntimeError(f"No late-session candles returned for {spec.label}")
    return result


# =============================================================================
# EXPIRY RESOLUTION AND MOVEMENT CALCULATION
# =============================================================================

def resolve_expiry_calendar(
    symbol: str,
    scheduled: Sequence[date],
    trading_dates: Sequence[date],
) -> pd.DataFrame:
    available = sorted(set(trading_dates))
    rows: List[Dict[str, Any]] = []
    seen_actual: set[date] = set()

    for scheduled_expiry in scheduled:
        pos = bisect.bisect_right(available, scheduled_expiry) - 1
        actual: Optional[date] = None
        if pos >= 0:
            candidate = available[pos]
            if (scheduled_expiry - candidate).days <= MAX_EXPIRY_SHIFT_BACK_DAYS:
                actual = candidate

        if actual is None:
            rows.append(
                {
                    "symbol": symbol,
                    "scheduled_expiry": scheduled_expiry,
                    "actual_expiry": pd.NaT,
                    "expiry_shift_days": np.nan,
                    "D-1": pd.NaT,
                    "D-2": pd.NaT,
                    "calendar_status": "NO_TRADING_SESSION_FOUND",
                }
            )
            continue

        if actual in seen_actual:
            rows.append(
                {
                    "symbol": symbol,
                    "scheduled_expiry": scheduled_expiry,
                    "actual_expiry": actual,
                    "expiry_shift_days": (scheduled_expiry - actual).days,
                    "D-1": pd.NaT,
                    "D-2": pd.NaT,
                    "calendar_status": "DUPLICATE_ACTUAL_EXPIRY",
                }
            )
            continue
        seen_actual.add(actual)

        actual_pos = bisect.bisect_left(available, actual)
        d_minus_1 = available[actual_pos - 1] if actual_pos >= 1 else None
        d_minus_2 = available[actual_pos - 2] if actual_pos >= 2 else None
        status = "OK" if d_minus_2 is not None else "INSUFFICIENT_PRIOR_SESSIONS"
        rows.append(
            {
                "symbol": symbol,
                "scheduled_expiry": scheduled_expiry,
                "actual_expiry": actual,
                "expiry_shift_days": (scheduled_expiry - actual).days,
                "D-1": d_minus_1 if d_minus_1 is not None else pd.NaT,
                "D-2": d_minus_2 if d_minus_2 is not None else pd.NaT,
                "calendar_status": status,
            }
        )

    return pd.DataFrame(rows)


def empty_movement(status: str) -> Dict[str, Any]:
    return {
        "entry_timestamp": pd.NaT,
        "entry_close": np.nan,
        "exit_timestamp": pd.NaT,
        "exit_close": np.nan,
        "close_change_points": np.nan,
        "close_change_pct": np.nan,
        "absolute_close_change_points": np.nan,
        "absolute_close_change_pct": np.nan,
        "max_up_points": np.nan,
        "max_up_pct": np.nan,
        "max_up_timestamp": pd.NaT,
        "max_down_points": np.nan,
        "max_down_pct": np.nan,
        "max_down_timestamp": pd.NaT,
        "largest_excursion_points": np.nan,
        "largest_excursion_pct": np.nan,
        "largest_excursion_direction": "",
        "close_direction": "",
        "observed_minutes": np.nan,
        "data_status": status,
    }


def analyze_session(session: pd.DataFrame) -> Dict[str, Any]:
    if session.empty:
        return empty_movement("NO_LATE_SESSION_DATA")

    session = session.sort_values("timestamp").reset_index(drop=True)
    exact = session[session["timestamp"].dt.time == MOVEMENT_START]
    if exact.empty:
        candidates = session[
            (session["timestamp"].dt.time > MOVEMENT_START)
            & (session["timestamp"].dt.time <= LATEST_ALLOWED_ENTRY)
        ]
        if candidates.empty:
            return empty_movement("NO_ENTRY_CANDLE_1455_TO_1459")
        entry = candidates.iloc[0]
        status_parts = ["ENTRY_CANDLE_SHIFTED"]
    else:
        entry = exact.iloc[0]
        status_parts = []

    entry_ts = pd.Timestamp(entry["timestamp"])
    entry_close = float(entry["close"])
    after = session[session["timestamp"] > entry_ts].copy()
    if after.empty:
        return empty_movement("NO_CANDLES_AFTER_ENTRY")

    exit_row = after.iloc[-1]
    exit_ts = pd.Timestamp(exit_row["timestamp"])
    exit_close = float(exit_row["close"])
    if exit_ts.time() < EXPECTED_LAST_CANDLE:
        status_parts.append("INCOMPLETE_OR_EARLY_CLOSE")

    high_idx = after["high"].idxmax()
    low_idx = after["low"].idxmin()
    high_row = after.loc[high_idx]
    low_row = after.loc[low_idx]

    close_change = exit_close - entry_close
    close_change_pct = close_change / entry_close
    # Excursions are directional magnitudes. If price never traded above/below
    # the baseline, that side is zero rather than a misleading opposite sign.
    max_up = max(0.0, float(high_row["high"]) - entry_close)
    max_down = min(0.0, float(low_row["low"]) - entry_close)
    largest_is_up = max_up >= abs(max_down)
    largest_points = max_up if largest_is_up else abs(max_down)
    if largest_points == 0:
        largest_direction = "FLAT"
    else:
        largest_direction = "UP" if largest_is_up else "DOWN"

    return {
        "entry_timestamp": entry_ts,
        "entry_close": entry_close,
        "exit_timestamp": exit_ts,
        "exit_close": exit_close,
        "close_change_points": close_change,
        "close_change_pct": close_change_pct,
        "absolute_close_change_points": abs(close_change),
        "absolute_close_change_pct": abs(close_change_pct),
        "max_up_points": max_up,
        "max_up_pct": max_up / entry_close,
        "max_up_timestamp": pd.Timestamp(high_row["timestamp"]),
        "max_down_points": max_down,
        "max_down_pct": max_down / entry_close,
        "max_down_timestamp": pd.Timestamp(low_row["timestamp"]),
        "largest_excursion_points": largest_points,
        "largest_excursion_pct": largest_points / entry_close,
        "largest_excursion_direction": largest_direction,
        "close_direction": (
            "UP" if close_change > 0 else "DOWN" if close_change < 0 else "FLAT"
        ),
        "observed_minutes": (exit_ts - entry_ts).total_seconds() / 60.0,
        "data_status": "|".join(status_parts) if status_parts else "OK",
    }


def build_detail(
    symbol: str,
    late_history: pd.DataFrame,
    calendar: pd.DataFrame,
) -> pd.DataFrame:
    sessions = {
        session_date: group.reset_index(drop=True)
        for session_date, group in late_history.groupby("trading_date", sort=True)
    }
    rows: List[Dict[str, Any]] = []

    for _, source in calendar.iterrows():
        if source["calendar_status"] not in {
            "OK",
            "INSUFFICIENT_PRIOR_SESSIONS",
        }:
            continue
        role_dates = (
            ("D0", 0, source["actual_expiry"]),
            ("D-1", -1, source["D-1"]),
            ("D-2", -2, source["D-2"]),
        )

        for role, offset, trading_day in role_dates:
            if pd.isna(trading_day):
                lookup_day: Optional[date] = None
            elif isinstance(trading_day, pd.Timestamp):
                lookup_day = trading_day.date()
            elif isinstance(trading_day, datetime):
                lookup_day = trading_day.date()
            else:
                lookup_day = trading_day
            base = {
                "symbol": symbol,
                "scheduled_expiry": source["scheduled_expiry"],
                "actual_expiry": source["actual_expiry"],
                "expiry_shift_days": source["expiry_shift_days"],
                "day_role": role,
                "day_offset": offset,
                "trading_date": lookup_day if lookup_day is not None else pd.NaT,
            }
            if lookup_day is None:
                movement = empty_movement("TRADING_DAY_UNAVAILABLE")
            else:
                movement = analyze_session(
                    sessions.get(lookup_day, pd.DataFrame())
                )
            rows.append({**base, **movement})

    return pd.DataFrame(rows, columns=DETAIL_COLUMNS)


def build_summary(detail: pd.DataFrame) -> pd.DataFrame:
    valid = detail[detail["entry_close"].notna()].copy()
    columns = [
        "symbol",
        "day_role",
        "observations",
        "up_close_pct",
        "average_close_change_points",
        "average_close_change_pct",
        "average_abs_close_change_points",
        "average_abs_close_change_pct",
        "median_abs_close_change_pct",
        "p90_abs_close_change_pct",
        "maximum_abs_close_change_pct",
        "average_max_up_pct",
        "average_max_down_abs_pct",
        "average_largest_excursion_pct",
        "maximum_largest_excursion_pct",
    ]
    if valid.empty:
        return pd.DataFrame(columns=columns)

    rows: List[Dict[str, Any]] = []
    role_order = {"D0": 0, "D-1": 1, "D-2": 2}
    for (symbol, role), group in valid.groupby(["symbol", "day_role"], sort=False):
        rows.append(
            {
                "symbol": symbol,
                "day_role": role,
                "observations": int(len(group)),
                "up_close_pct": float((group["close_change_points"] > 0).mean()),
                "average_close_change_points": group["close_change_points"].mean(),
                "average_close_change_pct": group["close_change_pct"].mean(),
                "average_abs_close_change_points": group[
                    "absolute_close_change_points"
                ].mean(),
                "average_abs_close_change_pct": group[
                    "absolute_close_change_pct"
                ].mean(),
                "median_abs_close_change_pct": group[
                    "absolute_close_change_pct"
                ].median(),
                "p90_abs_close_change_pct": group[
                    "absolute_close_change_pct"
                ].quantile(0.90),
                "maximum_abs_close_change_pct": group[
                    "absolute_close_change_pct"
                ].max(),
                "average_max_up_pct": group["max_up_pct"].mean(),
                "average_max_down_abs_pct": (-group["max_down_pct"]).mean(),
                "average_largest_excursion_pct": group[
                    "largest_excursion_pct"
                ].mean(),
                "maximum_largest_excursion_pct": group[
                    "largest_excursion_pct"
                ].max(),
            }
        )
    summary = pd.DataFrame(rows, columns=columns)
    summary["_role_order"] = summary["day_role"].map(role_order)
    return (
        summary.sort_values(["symbol", "_role_order"])
        .drop(columns="_role_order")
        .reset_index(drop=True)
    )


# =============================================================================
# EXCEL OUTPUT
# =============================================================================

def add_dataframe_table(
    writer: pd.ExcelWriter,
    sheet_name: str,
    df: pd.DataFrame,
    table_name: str,
    startrow: int = 0,
) -> None:
    df.to_excel(
        writer,
        sheet_name=sheet_name,
        index=False,
        header=False,
        startrow=startrow + 1,
    )
    sheet = writer.sheets[sheet_name]
    if df.empty:
        for col, name in enumerate(df.columns):
            sheet.write(startrow, col, name)
        return
    sheet.add_table(
        startrow,
        0,
        startrow + len(df),
        len(df.columns) - 1,
        {
            "name": table_name,
            "columns": [{"header": str(col)} for col in df.columns],
            "style": "Table Style Medium 2",
        },
    )


def set_useful_widths(sheet: Any, df: pd.DataFrame, max_width: int = 34) -> None:
    for col_idx, column in enumerate(df.columns):
        sample = df[column].head(300).astype(str) if not df.empty else pd.Series(dtype=str)
        content_width = int(sample.map(len).max()) if not sample.empty else 0
        width = min(max(len(str(column)) + 2, content_width + 2, 11), max_width)
        sheet.set_column(col_idx, col_idx, width)


def write_workbook(
    output_path: Path,
    detail: pd.DataFrame,
    calendar: pd.DataFrame,
    summary: pd.DataFrame,
    start_date: date,
    end_date: date,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(
        output_path,
        engine="xlsxwriter",
        datetime_format="yyyy-mm-dd hh:mm",
        date_format="yyyy-mm-dd",
    ) as writer:
        workbook = writer.book
        title_fmt = workbook.add_format(
            {
                "bold": True,
                "font_size": 16,
                "font_color": "#FFFFFF",
                "bg_color": "#17365D",
                "align": "left",
                "valign": "vcenter",
            }
        )
        label_fmt = workbook.add_format(
            {"bold": True, "font_color": "#17365D", "bg_color": "#D9EAF7"}
        )
        note_fmt = workbook.add_format(
            {"font_color": "#404040", "text_wrap": True, "valign": "top"}
        )
        pct_fmt = workbook.add_format({"num_format": "0.00%"})
        point_fmt = workbook.add_format({"num_format": "#,##0.00"})
        date_fmt = workbook.add_format({"num_format": "yyyy-mm-dd"})
        datetime_fmt = workbook.add_format({"num_format": "yyyy-mm-dd hh:mm"})

        # Summary
        summary_sheet = workbook.add_worksheet("Summary")
        writer.sheets["Summary"] = summary_sheet
        summary_sheet.merge_range("A1:O1", "Expiry-week movement after 14:55", title_fmt)
        metadata = [
            ("Underlying data", "Kite 1-minute NIFTY 50 and SENSEX index candles"),
            ("Analysis range", f"{start_date.isoformat()} to {end_date.isoformat()}"),
            ("Days studied", "Weekly expiry (D0), previous session (D-1), second previous session (D-2)"),
            ("Baseline", "Close of 14:55 candle; first available candle up to 14:59 is fallback"),
            ("Excursions", "High/low of candles strictly after the baseline candle"),
        ]
        for row, (label, value) in enumerate(metadata, start=2):
            summary_sheet.write(row, 0, label, label_fmt)
            summary_sheet.merge_range(row, 1, row, 14, value, note_fmt)
        add_dataframe_table(writer, "Summary", summary, "SummaryTable", startrow=8)
        set_useful_widths(summary_sheet, summary)
        summary_sheet.set_column(0, 0, 18)
        summary_sheet.set_column(1, 1, 12)
        if not summary.empty:
            summary_sheet.set_column(2, 2, 13)
            summary_sheet.set_column(3, 3, 13, pct_fmt)
            summary_sheet.set_column(4, 4, 20, point_fmt)
            summary_sheet.set_column(5, 14, 20, pct_fmt)
        summary_sheet.freeze_panes(9, 2)

        # Detail
        add_dataframe_table(
            writer, "Expiry Movement", detail, "ExpiryMovementTable"
        )
        detail_sheet = writer.sheets["Expiry Movement"]
        set_useful_widths(detail_sheet, detail)
        detail_sheet.freeze_panes(1, 7)
        for name in ["scheduled_expiry", "actual_expiry", "trading_date"]:
            detail_sheet.set_column(detail.columns.get_loc(name), detail.columns.get_loc(name), 13, date_fmt)
        for name in [
            "entry_timestamp",
            "exit_timestamp",
            "max_up_timestamp",
            "max_down_timestamp",
        ]:
            detail_sheet.set_column(detail.columns.get_loc(name), detail.columns.get_loc(name), 18, datetime_fmt)
        for name in [
            "close_change_pct",
            "absolute_close_change_pct",
            "max_up_pct",
            "max_down_pct",
            "largest_excursion_pct",
        ]:
            detail_sheet.set_column(detail.columns.get_loc(name), detail.columns.get_loc(name), 15, pct_fmt)
        point_columns = [
            "entry_close",
            "exit_close",
            "close_change_points",
            "absolute_close_change_points",
            "max_up_points",
            "max_down_points",
            "largest_excursion_points",
        ]
        for name in point_columns:
            detail_sheet.set_column(detail.columns.get_loc(name), detail.columns.get_loc(name), 15, point_fmt)
        if not detail.empty:
            change_col = detail.columns.get_loc("close_change_points")
            detail_sheet.conditional_format(
                1,
                change_col,
                len(detail),
                change_col,
                {
                    "type": "3_color_scale",
                    "min_color": "#F8696B",
                    "mid_color": "#FFEB84",
                    "max_color": "#63BE7B",
                },
            )

        # Expiry calendar
        calendar_out = calendar.copy()
        add_dataframe_table(
            writer, "Expiry Calendar", calendar_out, "ExpiryCalendarTable"
        )
        calendar_sheet = writer.sheets["Expiry Calendar"]
        set_useful_widths(calendar_sheet, calendar_out)
        calendar_sheet.freeze_panes(1, 2)
        for name in ["scheduled_expiry", "actual_expiry", "D-1", "D-2"]:
            calendar_sheet.set_column(
                calendar_out.columns.get_loc(name),
                calendar_out.columns.get_loc(name),
                14,
                date_fmt,
            )

        # Methodology / audit notes
        method_sheet = workbook.add_worksheet("Methodology")
        writer.sheets["Methodology"] = method_sheet
        method_sheet.merge_range("A1:F1", "Methodology and assumptions", title_fmt)
        notes = [
            ("Purpose", "Measure late-session underlying-index movement on weekly expiry, D-1, and D-2."),
            ("D0 resolution", "Generate the scheduled weekday, then shift backward to the latest available index trading session within seven calendar days."),
            ("D-1 / D-2", "The two prior trading sessions found in that index's downloaded Kite data; weekends and exchange holidays are therefore skipped."),
            ("14:55 baseline", "Close of the candle timestamped 14:55. If absent, the first candle from 14:56 through 14:59 is used and flagged."),
            ("Close movement", "Final available regular-session close minus the baseline close."),
            ("Maximum upside", "Highest high strictly after the baseline candle minus the baseline close."),
            ("Maximum downside", "Lowest low strictly after the baseline candle minus the baseline close; it is normally negative."),
            ("Largest excursion", "Larger absolute value of maximum upside and maximum downside."),
            ("NIFTY expiry rule", "Thursday before the week of 2025-09-01; Tuesday thereafter, exactly as encoded in the supplied reference."),
            ("SENSEX expiry rule", "Friday from weekly launch in May 2023 through 2024; Tuesday Jan-Aug 2025; Thursday from Sep 2025, exactly as encoded in the supplied reference."),
            ("Data quality", "Rows with a shifted entry candle, incomplete close, or missing data are explicitly flagged in data_status."),
            ("Scope", "Underlying spot-index movement only. No option premium, execution price, charges, slippage, or tradability assumption is included."),
        ]
        for row, (label, text) in enumerate(notes, start=2):
            method_sheet.write(row, 0, label, label_fmt)
            method_sheet.merge_range(row, 1, row, 5, text, note_fmt)
        method_sheet.set_column("A:A", 22)
        method_sheet.set_column("B:F", 22)
        method_sheet.set_default_row(34)
        method_sheet.set_row(0, 24)

    print(f"[DONE] Excel written: {output_path.resolve()}")


# =============================================================================
# CLI
# =============================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze NIFTY/SENSEX movement after 14:55 around weekly expiries."
    )
    parser.add_argument(
        "--end-date",
        type=date.fromisoformat,
        default=None,
        help="Inclusive YYYY-MM-DD end date. Default: latest complete IST session.",
    )
    parser.add_argument(
        "--years",
        type=int,
        default=DEFAULT_LOOKBACK_YEARS,
        help="Number of calendar years to analyze (default: 4).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("nifty_sensex_expiry_255_movement.xlsx"),
        help="Output Excel path.",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path(".kite_expiry_movement_cache"),
        help="Directory for resumable per-chunk candle caches.",
    )
    parser.add_argument(
        "--force-download",
        action="store_true",
        help="Ignore and replace all matching cached candle chunks.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.years < 1:
        raise ValueError("--years must be at least 1")
    if args.output.suffix.lower() != ".xlsx":
        args.output = args.output.with_suffix(".xlsx")

    end_date = args.end_date or default_complete_end_date()
    analysis_start = subtract_years(end_date, args.years)
    # Extra history is only to identify D-1 and D-2 for the first expiry.
    download_start = analysis_start - timedelta(days=10)

    print("=" * 72)
    print("NIFTY / SENSEX weekly-expiry movement after 14:55")
    print(f"Analysis: {analysis_start} -> {end_date}")
    print(f"Cache:    {args.cache_dir.resolve()}")
    print(f"Output:   {args.output.resolve()}")
    print("=" * 72)

    kite = initialize_kite()
    all_details: List[pd.DataFrame] = []
    all_calendars: List[pd.DataFrame] = []

    for spec in INDEX_SPECS:
        late_history = download_late_session_history(
            kite,
            spec,
            download_start,
            end_date,
            args.cache_dir,
            args.force_download,
        )
        scheduled = scheduled_weekly_expiries(
            spec.label, analysis_start, end_date
        )
        calendar = resolve_expiry_calendar(
            spec.label,
            scheduled,
            late_history["trading_date"].tolist(),
        )
        detail = build_detail(spec.label, late_history, calendar)
        all_calendars.append(calendar)
        all_details.append(detail)
        print(
            f"[INFO] {spec.label}: scheduled expiries={len(scheduled)}, "
            f"analysis rows={len(detail)}"
        )

    combined_detail = pd.concat(all_details, ignore_index=True)
    combined_calendar = pd.concat(all_calendars, ignore_index=True)
    combined_detail = combined_detail.sort_values(
        ["symbol", "actual_expiry", "day_offset"],
        ascending=[True, False, False],
        na_position="last",
    ).reset_index(drop=True)
    combined_calendar = combined_calendar.sort_values(
        ["symbol", "scheduled_expiry"], ascending=[True, False]
    ).reset_index(drop=True)
    summary = build_summary(combined_detail)

    write_workbook(
        args.output,
        combined_detail,
        combined_calendar,
        summary,
        analysis_start,
        end_date,
    )


if __name__ == "__main__":
    main()
