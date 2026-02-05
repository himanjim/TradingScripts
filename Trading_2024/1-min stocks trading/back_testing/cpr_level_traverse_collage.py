# cpr_traverse_extreme_scan_v4.py
# ------------------------------------------------------------
# SCAN LOGIC:
#  (A) UP: cross BC (upper CPR boundary) -> cross R1,
#          AND the R1-cross candle is the HIGHEST HIGH in scan window (09:15 -> cutoff).
#  (B) DOWN: cross TC (lower CPR boundary) -> cross S1,
#            AND the S1-cross candle is the LOWEST LOW in scan window (09:15 -> cutoff).
#
# PLOT REQUIREMENT:
#   - Plot COMPLETE session-day chart (09:15 -> 15:30 IST) for each match
#   - Save HTML and automatically open it in browser
#
# KEY FIX:
#   - For full-day plotting, DO NOT rely on Parquet predicate filters (can truncate on tz-aware/auto).
#     Instead: read column-pruned WITHOUT time filters, convert to IST-naive, then clamp in pandas.
#
# REQUIREMENTS:
#   pip install pandas numpy plotly pyarrow
# ------------------------------------------------------------

import os
import glob
import webbrowser
from datetime import datetime, date, time as dtime, timedelta
from typing import Optional, Dict, List, Tuple

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import pyarrow.parquet as pq


# ===================== CONFIG =====================

PARQUET_DIR = "../stock_history_parquet"
OUT_HTML = "cpr_traverse_extreme_collage.html"
ERROR_LOG = "cpr_traverse_extreme_errors.log"

SESSION_START_IST = dtime(9, 15)
SESSION_END_IST = dtime(15, 30)  # NSE session end (normal)

LOOKBACK_DAYS_FOR_PIVOT = 14

# Timestamp mode for stored Parquet timestamps:
#   "IST"  : tz-naive IST timestamps
#   "UTC"  : tz-naive UTC timestamps (shift +5:30)
#   "AUTO" : unknown tz-naive basis; we will not use Parquet time filters (safer),
#            and we detect 03:45-ish start to shift +5:30.
#
# NOTE: If Parquet contains tz-aware timestamps, we always convert them to IST-naive.
DATA_TIME_MODE = "AUTO"  # "IST" | "UTC" | "AUTO"
IST_OFFSET = timedelta(hours=5, minutes=30)

# Crossing definition:
#   "hl"    : wick-based crossings (recommended)
#   "close" : close-to-close crossings (strict)
CROSSING_MODE = "hl"  # "hl" | "close"

EPS_PRICE = 0.01
TOP_N = 30
PROGRESS_EVERY = 200

OPEN_HTML_IN_BROWSER = True

# Whether to try Parquet predicate filters by default.
# For AUTO, filters are risky (can truncate), so default is False.
DEFAULT_USE_PARQUET_FILTERS = (DATA_TIME_MODE in ("IST", "UTC"))

# ==================================================


# ---------- logging ----------

def log(msg: str):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}")


def log_error(tag: str, ex: Exception):
    log(f"[ERROR] {tag} | {type(ex).__name__}: {ex}")
    try:
        with open(ERROR_LOG, "a", encoding="utf-8") as f:
            f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {tag} | {type(ex).__name__}: {ex}\n")
    except Exception:
        pass


# ---------- file helpers ----------

def parse_exchange_symbol(path: str) -> Tuple[str, str]:
    base = os.path.splitext(os.path.basename(path))[0]
    if "_" in base:
        ex, sym = base.split("_", 1)
    else:
        ex, sym = "NSE", base
    return ex, sym


_SCHEMA_CACHE: Dict[str, set] = {}

def parquet_columns(path: str) -> set:
    cols = _SCHEMA_CACHE.get(path)
    if cols is None:
        pf = pq.ParquetFile(path)
        cols = set(pf.schema.names)
        _SCHEMA_CACHE[path] = cols
    return cols


# ---------- datetime normalization (tz-safe) ----------

def normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").drop_duplicates(subset=["date"]).reset_index(drop=True)
    return df


def to_ist_naive(df: pd.DataFrame, session_date: date) -> pd.DataFrame:
    """
    Convert df['date'] to IST timezone-naive.

    Handles:
      1) tz-aware series: tz_convert('Asia/Kolkata') then tz_localize(None)
      2) tz-naive series: adjust per DATA_TIME_MODE (IST/UTC/AUTO)
    """
    if df.empty:
        return df

    df = normalize_dates(df)
    s = df["date"]

    # tz-aware series
    if hasattr(s.dt, "tz") and s.dt.tz is not None:
        df["date"] = s.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
        return df

    # tz-naive series
    if DATA_TIME_MODE == "IST":
        return df
    if DATA_TIME_MODE == "UTC":
        df["date"] = df["date"] + IST_OFFSET
        return df

    # AUTO heuristic: if session_date seems to start around 03:45, treat as UTC-naive and shift
    day = df[df["date"].dt.date == session_date]
    if not day.empty:
        t0 = day["date"].min().time()
        if t0.hour == 3 and 35 <= t0.minute <= 55:
            df["date"] = df["date"] + IST_OFFSET

    return df


def read_parquet_window(
    path: str,
    start_ist: datetime,
    end_ist: datetime,
    session_date: date,
    wanted_cols: List[str],
    use_filters: Optional[bool] = None,
) -> pd.DataFrame:
    """
    Read a time window and return IST-naive timestamps.

    Strategy:
      - If use_filters is True: try Parquet predicate filters (fast). If it returns empty,
        fall back to unfiltered column-pruned read (robust).
      - If use_filters is False: read column-pruned without time filters, then clamp in pandas.
    """
    if use_filters is None:
        use_filters = DEFAULT_USE_PARQUET_FILTERS

    cols = parquet_columns(path)
    if "date" not in cols:
        return pd.DataFrame()

    use_cols = [c for c in wanted_cols if c in cols]

    def _read_unfiltered() -> pd.DataFrame:
        df0 = pd.read_parquet(path, engine="pyarrow", columns=use_cols)
        if df0.empty:
            return df0
        df0 = normalize_dates(df0)
        df0 = to_ist_naive(df0, session_date)
        return df0[(df0["date"] >= start_ist) & (df0["date"] <= end_ist)].copy().reset_index(drop=True)

    if not use_filters:
        # Robust path: no predicate pushdown
        return _read_unfiltered()

    # Attempt filtered read (fast)
    if DATA_TIME_MODE == "UTC":
        q_start = start_ist - IST_OFFSET
        q_end = end_ist - IST_OFFSET
    else:
        q_start = start_ist
        q_end = end_ist

    try:
        df = pd.read_parquet(
            path,
            engine="pyarrow",
            columns=use_cols,
            filters=[("date", ">=", q_start), ("date", "<=", q_end)],
        )
        # If filtered read yields empty, fall back to unfiltered (common with tz-aware parquet)
        if df.empty:
            return _read_unfiltered()

        df = normalize_dates(df)
        df = to_ist_naive(df, session_date)
        return df[(df["date"] >= start_ist) & (df["date"] <= end_ist)].copy().reset_index(drop=True)

    except Exception:
        # Any filter-related issues -> robust fallback
        return _read_unfiltered()


# ---------- pivot helpers ----------

def prev_trading_day_ohlc(df_lb: pd.DataFrame, session_date: date) -> Optional[Tuple[date, float, float, float]]:
    if df_lb.empty:
        return None
    days = sorted({d.date() for d in df_lb["date"]})
    prev_days = [d for d in days if d < session_date]
    if not prev_days:
        return None

    prev = prev_days[-1]
    ddf = df_lb[df_lb["date"].dt.date == prev]
    if ddf.empty:
        return None

    H = float(ddf["high"].max())
    L = float(ddf["low"].min())
    C = float(ddf.sort_values("date")["close"].iloc[-1])
    return prev, H, L, C


def pivots_from_prev_day(df_lb: pd.DataFrame, session_date: date) -> Optional[Dict[str, float]]:
    out = prev_trading_day_ohlc(df_lb, session_date)
    if not out:
        return None

    prev_date, H, L, C = out
    P = (H + L + C) / 3.0
    BC_raw = (H + L) / 2.0
    TC_raw = 2.0 * P - BC_raw
    R1 = 2.0 * P - L
    S1 = 2.0 * P - H

    # Ordered CPR boundaries (robust)
    BC = max(BC_raw, TC_raw)  # upper boundary
    TC = min(BC_raw, TC_raw)  # lower boundary

    return {
        "prev_date": prev_date,
        "P": P,
        "BC_raw": BC_raw,
        "TC_raw": TC_raw,
        "BC": BC,
        "TC": TC,
        "R1": R1,
        "S1": S1,
    }


# ---------- crossing logic (vectorized) ----------

def cross_up_flags(df: pd.DataFrame, level: float) -> np.ndarray:
    n = len(df)
    if n < 2:
        return np.zeros(n, dtype=bool)

    if CROSSING_MODE == "close":
        curr = df["close"].to_numpy(dtype=float)
        prev = np.roll(curr, 1)
        flags = (prev < level) & (curr >= level)
    else:
        curr = df["high"].to_numpy(dtype=float)
        prev = np.roll(curr, 1)
        flags = (prev < level) & (curr >= level)

    flags[0] = False
    return flags


def cross_down_flags(df: pd.DataFrame, level: float) -> np.ndarray:
    n = len(df)
    if n < 2:
        return np.zeros(n, dtype=bool)

    if CROSSING_MODE == "close":
        curr = df["close"].to_numpy(dtype=float)
        prev = np.roll(curr, 1)
        flags = (prev > level) & (curr <= level)
    else:
        curr = df["low"].to_numpy(dtype=float)
        prev = np.roll(curr, 1)
        flags = (prev > level) & (curr <= level)

    flags[0] = False
    return flags


def find_two_step(first_flags: np.ndarray, second_flags: np.ndarray) -> Optional[Tuple[int, int]]:
    idx1s = np.flatnonzero(first_flags)
    if idx1s.size == 0:
        return None
    idx1 = int(idx1s[0])

    idx2s = np.flatnonzero(second_flags[idx1:])
    if idx2s.size == 0:
        return None
    idx2 = idx1 + int(idx2s[0])
    return idx1, idx2


# ---------- extreme checks (within scan window) ----------

def is_highest_at(df: pd.DataFrame, idx: int) -> bool:
    day_high = float(df["high"].max())
    return abs(float(df["high"].iloc[idx]) - day_high) <= EPS_PRICE


def is_lowest_at(df: pd.DataFrame, idx: int) -> bool:
    day_low = float(df["low"].min())
    return abs(float(df["low"].iloc[idx]) - day_low) <= EPS_PRICE


# ---------- plotting (FORCE full-session axis) ----------

def make_full_day_chart(
    df_full: pd.DataFrame,
    title: str,
    lv: Dict[str, float],
    t1: datetime,
    t2: datetime,
    cutoff_dt: datetime,
    x_start: datetime,
    x_end: datetime,
) -> str:
    fig = go.Figure()
    fig.add_trace(go.Candlestick(
        x=df_full["date"],
        open=df_full["open"],
        high=df_full["high"],
        low=df_full["low"],
        close=df_full["close"]
    ))

    for k in ["R1", "BC", "P", "TC", "S1"]:
        y = float(lv[k])
        fig.add_hline(
            y=y,
            line_dash="dot",
            opacity=0.85,
            annotation_text=f"{k}:{y:.2f}",
            annotation_position="top left"
        )

    # Event/cutoff markers
    fig.add_vline(x=t1, line_dash="dot", opacity=0.65)
    fig.add_vline(x=t2, line_dash="dot", opacity=0.95)
    fig.add_vline(x=cutoff_dt, line_dash="dash", opacity=0.35)

    # FORCE x-axis range to show full session day
    fig.update_xaxes(range=[x_start, x_end])

    fig.update_layout(
        title=title,
        template="plotly_white",
        xaxis_rangeslider_visible=False,
        height=520,
        margin=dict(l=30, r=10, t=55, b=30),
    )
    return fig.to_html(full_html=False, include_plotlyjs=False, config={"responsive": True})


def build_html(items: List[Dict], out_path: str, session_date: date, cutoff_dt: datetime):
    chunks = []
    for it in items:
        chunks.append(f"<h3>{it['title']}</h3>")
        chunks.append(f"<div style='font-size:12px; margin-bottom:6px;'>{it['meta']}</div>")
        chunks.append(it["chart"])
        chunks.append("<hr/>")

    html = f"""<!doctype html>
<html>
<head>
<meta charset="utf-8"/>
<title>CPR Traverse + Extreme — {session_date}</title>
<script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
<style>
body {{ font-family: Arial, sans-serif; }}
.wrap {{ max-width: 1200px; margin: 0 auto; }}
</style>
</head>
<body>
<div class="wrap">
<h2>CPR Traverse + Extreme Scan (Full-day charts)</h2>
<p>
<b>Date:</b> {session_date} &nbsp;&nbsp;
<b>Scan window:</b> {SESSION_START_IST.strftime('%H:%M')}–{cutoff_dt.strftime('%H:%M')} IST &nbsp;&nbsp;
<b>Chart window:</b> {SESSION_START_IST.strftime('%H:%M')}–{SESSION_END_IST.strftime('%H:%M')} IST &nbsp;&nbsp;
<b>Crossing:</b> {CROSSING_MODE} &nbsp;&nbsp;
<b>Top N:</b> {TOP_N}
</p>
{''.join(chunks) if chunks else "<p><b>No matches</b></p>"}
</div>
</body>
</html>"""

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

    abs_path = os.path.abspath(out_path)
    log(f"[OK] Saved HTML: {abs_path}")

    if OPEN_HTML_IN_BROWSER:
        try:
            url = "file:///" + abs_path.replace(os.sep, "/")
            webbrowser.open(url)
            log("[OK] Opened HTML in browser.")
        except Exception as e:
            log_error("BROWSER", e)


# ---------- main scan ----------

def main():
    date_str = input("Enter analysis date (YYYY-MM-DD) [IST]: ").strip()
    cutoff_str = input("Enter cutoff time (HH:MM) [IST]: ").strip()

    session_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    cutoff_time = datetime.strptime(cutoff_str, "%H:%M").time()

    scan_start = datetime.combine(session_date, SESSION_START_IST)
    cutoff_dt = datetime.combine(session_date, cutoff_time)
    chart_start = datetime.combine(session_date, SESSION_START_IST)
    chart_end = datetime.combine(session_date, SESSION_END_IST)

    if cutoff_dt < scan_start:
        raise ValueError("Cutoff must be >= 09:15 IST")

    files = sorted(glob.glob(os.path.join(PARQUET_DIR, "*.parquet")))
    log(f"[INFO] Files: {len(files)} in {PARQUET_DIR}")
    log(f"[INFO] Scan window:  {scan_start.strftime('%H:%M')} -> {cutoff_dt.strftime('%H:%M')} IST")
    log(f"[INFO] Chart window: {chart_start.strftime('%H:%M')} -> {chart_end.strftime('%H:%M')} IST")
    log(f"[INFO] DATA_TIME_MODE={DATA_TIME_MODE} CROSSING_MODE={CROSSING_MODE}")
    log(f"[INFO] DEFAULT_USE_PARQUET_FILTERS={DEFAULT_USE_PARQUET_FILTERS} (plotting forces unfiltered reads)")
    log(f"[INFO] Errors printed to console and appended to: {os.path.abspath(ERROR_LOG)}")

    # Store only metadata for hits.
    # Each hit: (move_pct, tag, path, piv, t1, t2)
    up_hits: List[Tuple[float, str, str, Dict[str, float], datetime, datetime]] = []
    down_hits: List[Tuple[float, str, str, Dict[str, float], datetime, datetime]] = []

    for i, path in enumerate(files, start=1):
        ex, sym = parse_exchange_symbol(path)
        tag = f"{ex}:{sym}"

        try:
            # Lookback window for previous-day pivots (minimal columns)
            lb_start = datetime.combine(session_date - timedelta(days=LOOKBACK_DAYS_FOR_PIVOT), dtime(0, 0))
            lb_end = datetime.combine(session_date, dtime(0, 0))
            df_lb = read_parquet_window(
                path, lb_start, lb_end, session_date,
                wanted_cols=["date", "high", "low", "close"],
                use_filters=DEFAULT_USE_PARQUET_FILTERS
            )
            if df_lb.empty:
                continue

            piv = pivots_from_prev_day(df_lb, session_date)
            if not piv:
                continue

            # Scan window (09:15 -> cutoff)
            df_scan = read_parquet_window(
                path, scan_start, cutoff_dt, session_date,
                wanted_cols=["date", "open", "high", "low", "close"],
                use_filters=DEFAULT_USE_PARQUET_FILTERS
            )
            if df_scan.empty or len(df_scan) < 5:
                continue

            hi = float(df_scan["high"].max())
            lo = float(df_scan["low"].min())

            # UP: BC -> R1 and idx2 is highest in scan window
            if hi + EPS_PRICE >= piv["R1"]:
                step = find_two_step(cross_up_flags(df_scan, piv["BC"]), cross_up_flags(df_scan, piv["R1"]))
                if step:
                    idx1, idx2 = step
                    if is_highest_at(df_scan, idx2):
                        c1 = float(df_scan["close"].iloc[idx1])
                        c2 = float(df_scan["close"].iloc[idx2])
                        move_pct = abs(c2 - c1) / (abs(c1) + 1e-12) * 100.0
                        t1 = pd.to_datetime(df_scan["date"].iloc[idx1]).to_pydatetime()
                        t2 = pd.to_datetime(df_scan["date"].iloc[idx2]).to_pydatetime()
                        up_hits.append((move_pct, tag, path, piv, t1, t2))

            # DOWN: TC -> S1 and idx2 is lowest in scan window
            if lo - EPS_PRICE <= piv["S1"]:
                step = find_two_step(cross_down_flags(df_scan, piv["TC"]), cross_down_flags(df_scan, piv["S1"]))
                if step:
                    idx1, idx2 = step
                    if is_lowest_at(df_scan, idx2):
                        c1 = float(df_scan["close"].iloc[idx1])
                        c2 = float(df_scan["close"].iloc[idx2])
                        move_pct = abs(c2 - c1) / (abs(c1) + 1e-12) * 100.0
                        t1 = pd.to_datetime(df_scan["date"].iloc[idx1]).to_pydatetime()
                        t2 = pd.to_datetime(df_scan["date"].iloc[idx2]).to_pydatetime()
                        down_hits.append((move_pct, tag, path, piv, t1, t2))

        except Exception as e:
            log_error(tag, e)

        if i % PROGRESS_EVERY == 0:
            log(f"[PROGRESS] {i}/{len(files)} scanned | UP={len(up_hits)} DOWN={len(down_hits)}")

    # Rank + cut
    up_hits.sort(key=lambda x: -x[0])
    down_hits.sort(key=lambda x: -x[0])
    up_hits = up_hits[:TOP_N]
    down_hits = down_hits[:TOP_N]

    log(f"[RESULT] UP (BC->R1 & highest): {len(up_hits)}")
    log(f"[RESULT] DOWN (TC->S1 & lowest): {len(down_hits)}")

    for k, (mv, tag, *_rest) in enumerate(up_hits, start=1):
        log(f"[UP]   #{k} {tag} move={mv:.2f}%")
    for k, (mv, tag, *_rest) in enumerate(down_hits, start=1):
        log(f"[DOWN] #{k} {tag} move={mv:.2f}%")

    # Build charts: FULL session-day chart (force unfiltered read for correctness)
    items: List[Dict] = []

    def add_item(prefix: str, mv: float, tag: str, path: str, piv: Dict[str, float], t1: datetime, t2: datetime):
        df_full = read_parquet_window(
            path,
            chart_start,
            chart_end,
            session_date,
            wanted_cols=["date", "open", "high", "low", "close"],
            use_filters=False  # <-- critical: force robust full-day read
        )
        if df_full.empty:
            log(f"[WARN] Full-day data empty for {tag} (cannot plot).")
            return

        # Debug: confirm full-day coverage in console
        log(f"[PLOT] {tag} full-day range: {df_full['date'].iloc[0].strftime('%H:%M')} -> {df_full['date'].iloc[-1].strftime('%H:%M')} (rows={len(df_full)})")

        title = f"{prefix} {tag} — move={mv:.2f}%"
        meta = (
            f"Prev={piv['prev_date']} | "
            f"Cross times: t1={t1.strftime('%H:%M')}, t2={t2.strftime('%H:%M')} | "
            f"Cutoff={cutoff_dt.strftime('%H:%M')} | "
            f"R1/BC/P/TC/S1={piv['R1']:.2f}/{piv['BC']:.2f}/{piv['P']:.2f}/{piv['TC']:.2f}/{piv['S1']:.2f} | "
            f"Raw CPR: BC_raw={piv['BC_raw']:.2f}, TC_raw={piv['TC_raw']:.2f}"
        )

        chart = make_full_day_chart(df_full, title, piv, t1, t2, cutoff_dt, chart_start, chart_end)
        items.append({"title": title, "meta": meta, "chart": chart})

    for mv, tag, path, piv, t1, t2 in up_hits:
        add_item("[UP BC->R1 & HIGHEST]", mv, tag, path, piv, t1, t2)

    for mv, tag, path, piv, t1, t2 in down_hits:
        add_item("[DOWN TC->S1 & LOWEST]", mv, tag, path, piv, t1, t2)

    build_html(items, OUT_HTML, session_date, cutoff_dt)


if __name__ == "__main__":
    main()
