import os
import glob
import datetime as dt
from datetime import datetime, date, time as dtime, timedelta
from typing import List, Dict, Tuple, Optional

import numpy as np
import pandas as pd


import plotly.graph_objects as go


# ================== CONFIG ==================

PARQUET_DIR = "./stock_history_parquet"
OUTPUT_HTML = "pattern_scan_collage.html"

# Read only this many calendar days back to reliably find prev trading day
LOOKBACK_DAYS_FOR_PIVOT = 12

# Pattern B "flatness" threshold (total band of last 20 bars as % of mid price)
PATTERN_B_MAX_BAND_PCT = 0.25  # tighten/loosen as needed

# Time interpretation:
# "AUTO": detect UTC-like session (03:45) and shift +5:30 to IST
# "IST": assume stored in IST-naive already
# "UTC": assume stored UTC-naive; shift +5:30 to IST-naive
DATA_TIME_MODE = "AUTO"  # "AUTO" | "IST" | "UTC"
IST_OFFSET = timedelta(hours=5, minutes=30)

# Plot sizes
MINI_HEIGHT = 320
FULL_HEIGHT = 430


# ================== LOGGING ==================

def log(level: str, msg: str):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{now} [{level}] {msg}")


# ================== PARQUET HELPERS ==================

def list_parquet_files() -> List[str]:
    if not os.path.isdir(PARQUET_DIR):
        raise FileNotFoundError(f"Parquet directory not found: {PARQUET_DIR}")
    return sorted(glob.glob(os.path.join(PARQUET_DIR, "*.parquet")))


def parse_exchange_symbol_from_filename(path: str) -> Tuple[str, str]:
    fname = os.path.basename(path)
    base = os.path.splitext(fname)[0]
    if "_" in base:
        ex, ts = base.split("_", 1)
    else:
        ex, ts = "NSE", base
    return ex, ts


def normalize_date_series(s: pd.Series, ctx: str = "") -> pd.Series:
    """
    Convert any 'date' series into pandas datetime64[ns] (tz-naive).
    Handles tz-aware objects safely.
    """
    def _drop_tz_python(x):
        if isinstance(x, dt.datetime) and x.tzinfo is not None:
            return x.replace(tzinfo=None)
        return x

    s = s.apply(_drop_tz_python)
    s = pd.to_datetime(s, errors="coerce")

    if isinstance(s.dtype, pd.DatetimeTZDtype):
        tz_info = s.dt.tz
        log("INFO", f"{ctx}: tz-aware detected ({tz_info}); dropping tz.")
        s = s.dt.tz_convert(None)

    return s


def read_symbol_window(path: str, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    """
    Read only a small window from a large per-stock Parquet using filters (pyarrow).
    Big speed win vs reading full 3-year file.
    """
    cols = ["date", "open", "high", "low", "close", "volume"]

    try:
        df = pd.read_parquet(
            path,
            columns=cols,
            engine="pyarrow",
            filters=[
                ("date", ">=", start_dt),
                ("date", "<=", end_dt),
            ],
        )
    except TypeError:
        # Fallback for environments where datetime filters act up
        df = pd.read_parquet(path, columns=cols, engine="pyarrow")

    df = df.copy()
    df["date"] = normalize_date_series(df["date"], ctx=f"read_symbol_window {os.path.basename(path)}")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    df = df[(df["date"] >= start_dt) & (df["date"] <= end_dt)]
    return df


def to_ist_naive(df: pd.DataFrame, session_date: date) -> pd.DataFrame:
    """
    Ensure df['date'] behaves like IST-naive.

    If data was stored as UTC-naive (03:45–10:00), shift +5:30.
    AUTO mode detects this using earliest timestamp on session_date.
    """
    if df.empty:
        return df

    df = df.copy()
    df["date"] = normalize_date_series(df["date"], ctx="to_ist_naive")

    if DATA_TIME_MODE == "IST":
        return df
    if DATA_TIME_MODE == "UTC":
        df["date"] = df["date"] + IST_OFFSET
        return df

    # AUTO detection on the requested session date
    day = df[df["date"].dt.date == session_date]
    if day.empty:
        return df

    t0 = day["date"].min().time()
    # NSE 09:15 IST corresponds to 03:45 UTC
    if (t0.hour == 3 and 35 <= t0.minute <= 55):
        log("INFO", "AUTO TZ: looks like UTC-naive session (03:45). Shifting +05:30 to IST.")
        df["date"] = df["date"] + IST_OFFSET
    return df


# ================== CPR / PIVOT LOGIC ==================
#   P  = (H + L + C) / 3
#   BC = (H + L) / 2
#   TC = 2P - BC
#   R1 = 2P - L
#   S1 = 2P - H

def compute_pivots_from_prev_day(df_win: pd.DataFrame, session_date: date) -> Optional[Dict[str, float]]:
    if df_win.empty:
        return None

    all_dates = sorted({d.date() for d in df_win["date"]})
    prev_dates = [d for d in all_dates if d < session_date]
    if not prev_dates:
        return None
    prev_date = prev_dates[-1]

    prev_df = df_win[df_win["date"].dt.date == prev_date]
    if prev_df.empty:
        return None

    H = float(prev_df["high"].max())
    L = float(prev_df["low"].min())
    C = float(prev_df.sort_values("date")["close"].iloc[-1])

    P = (H + L + C) / 3.0
    BC = (H + L) / 2.0
    TC = 2 * P - BC
    R1 = 2 * P - L
    S1 = 2 * P - H

    return {"P": P, "BC": BC, "TC": TC, "R1": R1, "S1": S1, "prev_date": prev_date}


# ================== PATTERNS ==================

def detect_pattern_A(df_day_upto: pd.DataFrame) -> Optional[Dict]:
    """
    Pattern A (pressure building):
      last 10 candles => smallest mean absolute % change between consecutive closes.
      This normalizes across different price levels.
    """
    if len(df_day_upto) < 11:
        return None

    last = df_day_upto.tail(11).copy()  # need 10 consecutive diffs
    closes = last["close"].astype(float).values

    # pct change between consecutive closes (absolute)
    pct = np.abs(np.diff(closes) / closes[:-1]) * 100.0

    # primary score: mean abs % move (lower is tighter)
    mean_abs_pct = float(np.mean(pct))
    # tie-break: max abs % move (lower is more consistently tight)
    max_abs_pct = float(np.max(pct))

    return {
        "score": mean_abs_pct,
        "tie": max_abs_pct,
        "last_time": last["date"].iloc[-1],
        "last_close": float(closes[-1]),
    }


def _compress_signs(signs: np.ndarray) -> List[int]:
    seq = []
    for s in signs:
        if s == 0:
            continue
        if not seq or int(s) != seq[-1]:
            seq.append(int(s))
    return seq


def _whipsaw_across_level(closes: np.ndarray, level: float) -> bool:
    """
    Returns True if closes whipsaw across the given level:
      below->above->below OR above->below->above within the window.
    """
    diffs = closes - level
    signs = np.sign(diffs)  # -1, 0, +1
    seq = _compress_signs(signs)

    if len(seq) < 3:
        return False

    # Look for -1,+1,-1 or +1,-1,+1 anywhere (not necessarily consecutive originally,
    # but consecutive after compression indicates direction flips happened).
    for i in range(len(seq) - 2):
        a, b, c = seq[i], seq[i + 1], seq[i + 2]
        if a == -1 and b == 1 and c == -1:
            return True
        if a == 1 and b == -1 and c == 1:
            return True

    return False


def detect_pattern_B(df_day_upto: pd.DataFrame, pivots: Dict[str, float]) -> Optional[Dict]:
    """
    Pattern B:
      last 20 candles:
        - tight band (flat tops & bottoms)
        - whipsaw across ANY of the 5 CPR lines: P, BC, TC, R1, S1
    """
    if len(df_day_upto) < 20:
        return None

    last20 = df_day_upto.tail(20)

    h_max = float(last20["high"].max())
    l_min = float(last20["low"].min())
    mid = (h_max + l_min) / 2.0
    if mid <= 0:
        return None

    band_pct = (h_max - l_min) / mid * 100.0
    if band_pct > PATTERN_B_MAX_BAND_PCT:
        return None

    closes = last20["close"].astype(float).values

    levels = {k: float(pivots[k]) for k in ["P", "BC", "TC", "R1", "S1"]}
    crossed = [k for k, lvl in levels.items() if _whipsaw_across_level(closes, lvl)]
    if not crossed:
        return None

    last_close = float(last20["close"].iloc[-1])

    # tie-break: distance to nearest crossed line (smaller better)
    nearest_dist = min(abs(last_close - levels[k]) for k in crossed)

    return {
        "score": float(band_pct),                 # lower better
        "tie1": -len(crossed),                    # more crossed lines better => negative
        "tie2": float(nearest_dist),              # smaller better
        "band_pct": float(band_pct),
        "crossed_lines": crossed,                 # e.g. ["P","TC"]
        "last_time": last20["date"].iloc[-1],
        "last_close": last_close,
    }


# ================== CHARTING ==================

def add_pivot_lines(fig: go.Figure, pivots: Optional[Dict[str, float]]):
    if not pivots:
        return
    for k in ["P", "BC", "TC", "R1", "S1"]:
        y = pivots.get(k)
        if y is None:
            continue
        fig.add_hline(
            y=y,
            line_dash="dot",
            opacity=0.85,
            annotation_text=f"{k}: {y:.2f}",
            annotation_position="top left",
        )


def make_candle_fig(df_plot: pd.DataFrame, title: str, pivots: Optional[Dict[str, float]], height: int) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(
        go.Candlestick(
            x=df_plot["date"],
            open=df_plot["open"],
            high=df_plot["high"],
            low=df_plot["low"],
            close=df_plot["close"],
            name="",
        )
    )
    add_pivot_lines(fig, pivots)
    fig.update_layout(
        title=title,
        xaxis_title="Time",
        yaxis_title="Price",
        xaxis_rangeslider_visible=False,
        template="plotly_white",
        height=height,
        margin=dict(l=30, r=5, t=45, b=30),
        showlegend=False,
    )
    fig.update_xaxes(automargin=True)
    fig.update_yaxes(automargin=True)

    return fig


def fig_to_html_div(fig: go.Figure, static: bool) -> str:
    config = {"displayModeBar": not static, "staticPlot": static, "scrollZoom": not static}
    return fig.to_html(full_html=False, include_plotlyjs=False, config=config)


def build_collage_html(top_results: List[Dict], session_date: date, cutoff_dt: datetime, out_path: str):
    if not top_results:
        log("WARN", "No results to plot.")
        return

    mini_blocks = []
    full_blocks = []

    for rank, r in enumerate(top_results, start=1):
        ex = r["exchange"]
        ts = r["symbol"]
        path = r["path"]
        anchor = f"{ex}_{ts}_full".replace(":", "_").replace(" ", "_")

        win_start = datetime.combine(session_date - timedelta(days=LOOKBACK_DAYS_FOR_PIVOT), dtime(0, 0))
        win_end = datetime.combine(session_date + timedelta(days=1), dtime(0, 0))
        df_win = read_symbol_window(path, win_start, win_end)
        df_win = to_ist_naive(df_win, session_date)

        piv = compute_pivots_from_prev_day(df_win, session_date)

        df_day = df_win[df_win["date"].dt.date == session_date].copy()
        if df_day.empty:
            continue
        df_day = df_day.sort_values("date")
        df_upto = df_day[df_day["date"] <= cutoff_dt]
        if df_upto.empty:
            continue

        mini_title = f"{rank}. {ex}:{ts} — till {cutoff_dt.strftime('%H:%M')}"
        full_title = f"{rank}. {ex}:{ts} — full day {session_date}"

        fig_mini = make_candle_fig(df_upto, mini_title, piv, MINI_HEIGHT)
        fig_full = make_candle_fig(df_day, full_title, piv, FULL_HEIGHT)

        # Mini = static so the button click is always reliable
        mini_div = fig_to_html_div(fig_mini, static=True)
        full_div = fig_to_html_div(fig_full, static=False)

        extra = ""
        if "crossed_lines" in r:
            extra = f"<div style='font-size:12px;margin:4px 0;'><b>Crossed:</b> {', '.join(r['crossed_lines'])}</div>"

        mini_blocks.append(f"""
<div class="mini-chart">
  <div class="mini-head">
    <div class="mini-title">{mini_title}</div>
    <a class="btn" href="#{anchor}">Open full-day</a>
  </div>
  {extra}
  {mini_div}
</div>
""")

        full_blocks.append(f"""
<h2 id="{anchor}">{full_title}</h2>
{full_div}
<hr />
""")

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8" />
<title>Pattern Scan — {session_date}</title>
<script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
<style>
body {{ font-family: Arial, sans-serif; }}
.grid {{
  display: grid;
  grid-template-columns: repeat(2, minmax(420px, 520px));
  gap: 14px;
  justify-content: center;
}}

.mini-chart {{
  border: 1px solid #ddd;
  padding: 8px;
  background: #fafafa;
}}
.mini-head {{
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 6px;
}}
.mini-title {{
  font-weight: 700;
  font-size: 14px;
}}
.btn {{
  font-size: 12px;
  text-decoration: none;
  padding: 6px 10px;
  border: 1px solid #444;
  border-radius: 6px;
  color: #111;
  background: #fff;
}}
.btn:hover {{ background: #eee; }}
</style>
</head>
<body>
<h1>Pattern Scan</h1>
<p><b>Date:</b> {session_date} &nbsp;&nbsp; <b>Cutoff:</b> {cutoff_dt.strftime('%H:%M')} &nbsp;&nbsp;
<b>Pattern B band:</b> {PATTERN_B_MAX_BAND_PCT:.2f}%</p>

<h2>Top matches (collage)</h2>
<div class="grid">
{''.join(mini_blocks)}
</div>

<hr />
<h2>Full-day charts</h2>
{''.join(full_blocks)}

</body>
</html>
"""
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

    log("INFO", f"Saved collage HTML: {os.path.abspath(out_path)}")


# ================== MAIN ==================

def main():
    date_str = input("Enter analysis date (YYYY-MM-DD): ").strip()
    time_str = input("Enter cutoff time (HH:MM, IST) [default 15:30]: ").strip() or "15:30"
    pattern_type = input("Enter pattern type (A or B): ").strip().upper()

    if pattern_type not in ("A", "B"):
        log("ERROR", "Pattern type must be A or B.")
        return

    session_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    cutoff_time = datetime.strptime(time_str, "%H:%M").time()
    cutoff_dt = datetime.combine(session_date, cutoff_time)

    files = list_parquet_files()
    log("STEP", f"Found {len(files)} Parquet files.")

    win_start = datetime.combine(session_date - timedelta(days=LOOKBACK_DAYS_FOR_PIVOT), dtime(0, 0))
    win_end = datetime.combine(session_date + timedelta(days=1), dtime(0, 0))

    results = []
    scanned = 0

    for path in files:
        ex, ts = parse_exchange_symbol_from_filename(path)
        tag = f"{ex}:{ts}"

        try:
            df_win = read_symbol_window(path, win_start, win_end)
            df_win = to_ist_naive(df_win, session_date)

            df_day = df_win[df_win["date"].dt.date == session_date].copy()
            if df_day.empty:
                continue

            df_day_upto = df_day[df_day["date"] <= cutoff_dt].sort_values("date")
            if df_day_upto.empty:
                continue

            if pattern_type == "A":
                pr = detect_pattern_A(df_day_upto)
                if pr:
                    pr.update({"exchange": ex, "symbol": ts, "path": path})
                    results.append(pr)
            else:
                piv = compute_pivots_from_prev_day(df_win, session_date)
                if not piv:
                    continue
                pr = detect_pattern_B(df_day_upto, piv)
                if pr:
                    pr.update({"exchange": ex, "symbol": ts, "path": path, "pivots": piv})
                    results.append(pr)

        except Exception as e:
            log("ERROR", f"{tag}: failed: {e}")
            continue

        scanned += 1
        if scanned % 50 == 0:
            log("STEP", f"Scanned {scanned}/{len(files)}; matches: {len(results)}")

    if not results:
        log("WARN", "No matches found.")
        return

    if pattern_type == "A":
        results_sorted = sorted(results, key=lambda r: (r["score"], r["tie"]))
    else:
        # score asc (band), tie1 asc (negative means more lines crossed), tie2 asc (distance)
        results_sorted = sorted(results, key=lambda r: (r["score"], r["tie1"], r["tie2"]))

    top = results_sorted[:10]

    log("INFO", f"Matches: {len(results)} | Top selected: {len(top)}")
    for i, r in enumerate(top, start=1):
        if pattern_type == "A":
            log("INFO", f"#{i} {r['exchange']}:{r['symbol']} meanRange={r['score']:.4f} maxRange={r['tie']:.4f}")
        else:
            log("INFO", f"#{i} {r['exchange']}:{r['symbol']} band%={r['score']:.4f} crossed={r['crossed_lines']}")

    build_collage_html(top, session_date, cutoff_dt, OUTPUT_HTML)


if __name__ == "__main__":
    main()
