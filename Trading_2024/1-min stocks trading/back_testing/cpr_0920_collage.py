# cpr_0920_collage.py
# Browser collage (Plotly) scanner:
# Shows ONLY top 5 stocks where:
#   (A) Stock has TOUCHED or CROSSED the CPR BAND (BC–TC) between 09:15–09:20 IST
#   (B) The 09:15–09:20 window is TRENDING (not wavy) using:
#       - Efficiency Ratio (ER) threshold
#       - Max direction changes threshold
#       - Min net move %
# Ranked by: abs((PP - TC) / TC) * 100 (minimum CPR width first)
#
# Requires: pandas, numpy, plotly, pyarrow
# pip install pandas numpy plotly pyarrow

import os
import glob
import argparse
import datetime as dt
from datetime import datetime, date, time as dtime, timedelta
from typing import List, Dict, Tuple, Optional

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import webbrowser


# ================== DEFAULT CONFIG ==================

DEFAULT_PARQUET_DIR = "../stock_history_parquet"
DEFAULT_OUTPUT_HTML = "cpr_0920_collage.html"

LOOKBACK_DAYS_FOR_PIVOT = 12

# Time interpretation:
# "AUTO": detect UTC-like session (03:45) and shift +5:30 to IST
# "IST": assume stored in IST-naive already
# "UTC": assume stored UTC-naive; shift +5:30 to IST-naive
DATA_TIME_MODE = "AUTO"  # "AUTO" | "IST" | "UTC"
IST_OFFSET = timedelta(hours=5, minutes=30)

SESSION_START = dtime(9, 15)  # IST
DEFAULT_CUTOFF = dtime(9, 20)  # IST inclusive

TOP_N_DEFAULT = 5

# Trend filters for 09:15–09:20
TREND_EFF_MIN_DEFAULT = 0.35
MAX_DIR_CHANGES_DEFAULT = 3
MIN_NET_MOVE_PCT_DEFAULT = 0.00
MIN_ABS_STEPS_DEFAULT = 0.0001  # avoids ER being "high" on nearly flat series (absolute price units)

# Plot sizes + page width
MINI_HEIGHT = 300
FULL_HEIGHT = 430
PAGE_MAX_WIDTH_PX = 980

REQUIRED_COLS = {"date", "open", "high", "low", "close"}


# ================== LOGGING ==================

def log(level: str, msg: str):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{now} [{level}] {msg}")


# ================== FILE + PARQUET HELPERS ==================

def list_parquet_files(parquet_dir: str) -> List[str]:
    if not os.path.isdir(parquet_dir):
        raise FileNotFoundError(f"Parquet directory not found: {parquet_dir}")
    return sorted(glob.glob(os.path.join(parquet_dir, "*.parquet")))


def parse_exchange_symbol_from_filename(path: str) -> Tuple[str, str]:
    fname = os.path.basename(path)
    base = os.path.splitext(fname)[0]
    if "_" in base:
        ex, ts = base.split("_", 1)
    else:
        ex, ts = "NSE", base
    return ex, ts


def normalize_date_series(s: pd.Series, ctx: str = "") -> pd.Series:
    s = s.astype("object")
    s = s.map(lambda x: x.replace(tzinfo=None) if isinstance(x, dt.datetime) and x.tzinfo else x)
    s = pd.to_datetime(s, errors="coerce")
    if isinstance(s.dtype, pd.DatetimeTZDtype):
        log("INFO", f"{ctx}: tz-aware detected ({s.dt.tz}); dropping tz.")
        s = s.dt.tz_convert(None)
    return s


def to_ist_naive(df: pd.DataFrame, session_date: date) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    df["date"] = normalize_date_series(df["date"], ctx="to_ist_naive")

    if DATA_TIME_MODE == "IST":
        return df
    if DATA_TIME_MODE == "UTC":
        df["date"] = df["date"] + IST_OFFSET
        return df

    # AUTO detect (03:45-ish => UTC-naive)
    day = df[df["date"].dt.date == session_date]
    if day.empty:
        return df

    t0 = day["date"].min().time()
    if (t0.hour == 3 and 35 <= t0.minute <= 55):
        df["date"] = df["date"] + IST_OFFSET
    return df


def read_symbol_window(path: str, start_dt: datetime, end_dt: datetime, session_date_for_tz: date) -> pd.DataFrame:
    cols = ["date", "open", "high", "low", "close", "volume"]
    try:
        df = pd.read_parquet(
            path,
            columns=cols,
            engine="pyarrow",
            filters=[("date", ">=", start_dt), ("date", "<=", end_dt)],
        )
    except Exception:
        df = pd.read_parquet(path, columns=cols, engine="pyarrow")

    if df.empty:
        return df

    df = df.copy()
    df["date"] = normalize_date_series(df["date"], ctx=f"read_symbol_window {os.path.basename(path)}")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

    # constrain to requested window
    df = df[(df["date"] >= start_dt) & (df["date"] <= end_dt)]

    # normalize to IST-naive
    df = to_ist_naive(df, session_date_for_tz)

    # protect against duplicates (can break cross/trend logic)
    df = df.sort_values("date").drop_duplicates(subset=["date"]).reset_index(drop=True)
    return df


def read_symbol_day(path: str, session_date: date) -> pd.DataFrame:
    cols = ["date", "open", "high", "low", "close", "volume"]
    start_dt = datetime.combine(session_date, dtime(0, 0))
    end_dt = datetime.combine(session_date + timedelta(days=1), dtime(0, 0))

    try:
        df = pd.read_parquet(
            path,
            columns=cols,
            engine="pyarrow",
            filters=[("date", ">=", start_dt), ("date", "<", end_dt)],
        )
    except Exception:
        df = pd.read_parquet(path, columns=cols, engine="pyarrow")

    if df.empty:
        return df

    df = df.copy()
    df["date"] = normalize_date_series(df["date"], ctx=f"read_symbol_day {os.path.basename(path)}")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    df = to_ist_naive(df, session_date)

    df = df[df["date"].dt.date == session_date].copy()
    df = df.sort_values("date").drop_duplicates(subset=["date"]).reset_index(drop=True)
    return df


def has_required_cols(df: pd.DataFrame) -> bool:
    return REQUIRED_COLS.issubset(set(df.columns))


# ================== CPR ==================
# PP/P = (H + L + C)/3
# BC   = (H + L)/2
# TC   = 2P - BC

def compute_cpr_from_prev_day(df_win: pd.DataFrame, session_date: date) -> Optional[Dict[str, float]]:
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

    return {"PP": P, "P": P, "BC": BC, "TC": TC, "prev_date": prev_date}


def _cpr_band(BC: float, TC: float) -> Tuple[float, float]:
    return (min(BC, TC), max(BC, TC))


# ================== TOUCH / CROSS (BAND AS ZONE) ==================

def band_touched_by_wicks(df: pd.DataFrame, band_lo: float, band_hi: float) -> bool:
    # any candle overlaps the band
    return bool(((df["low"] <= band_hi) & (df["high"] >= band_lo)).any())


def _band_state(close_px: float, band_lo: float, band_hi: float) -> int:
    # -1 BELOW, 0 INSIDE (inclusive), +1 ABOVE
    if close_px < band_lo:
        return -1
    if close_px > band_hi:
        return 1
    return 0


def crossed_band_by_close(df: pd.DataFrame, band_lo: float, band_hi: float) -> Tuple[bool, Optional[datetime], str]:
    """
    Crossed CPR band as a zone = any CLOSE-to-CLOSE state transition:
      BELOW <-> INSIDE, ABOVE <-> INSIDE, or BELOW <-> ABOVE (gap-through).
    """
    if len(df) < 2:
        return False, None, ""

    closes = df["close"].astype(float).values
    states = [_band_state(c, band_lo, band_hi) for c in closes]

    def sname(s: int) -> str:
        return "BELOW" if s == -1 else ("ABOVE" if s == 1 else "INSIDE")

    for i in range(1, len(states)):
        if states[i - 1] != states[i]:
            return True, df["date"].iloc[i], f"{sname(states[i - 1])}->{sname(states[i])}"

    return False, None, ""


# ================== TREND FILTER (NOT WAVY) ==================

def trend_metrics(df: pd.DataFrame) -> Dict[str, float]:
    c = df["close"].astype(float).values
    if len(c) < 2:
        return {"er": 0.0, "dir_changes": 999.0, "net_move_pct": 0.0, "abs_steps_sum": 0.0}

    steps = np.diff(c)
    abs_steps_sum = float(np.sum(np.abs(steps)))
    net_move = float(c[-1] - c[0])

    er = abs(net_move) / (abs_steps_sum + 1e-12)

    # direction changes (ignore zeros)
    signs = np.sign(steps)
    signs = signs[signs != 0]
    dir_changes = 0
    if len(signs) >= 2:
        dir_changes = int(np.sum(signs[1:] != signs[:-1]))

    net_move_pct = abs(net_move) / (abs(c[0]) + 1e-12) * 100.0

    return {"er": float(er), "dir_changes": float(dir_changes), "net_move_pct": float(net_move_pct), "abs_steps_sum": abs_steps_sum}


def is_trending_window(df: pd.DataFrame, er_min: float, max_dir_changes: int, min_net_move_pct: float, min_abs_steps: float) -> Tuple[bool, Dict[str, float]]:
    m = trend_metrics(df)
    ok = (
        (m["abs_steps_sum"] >= min_abs_steps) and
        (m["er"] >= er_min) and
        (m["dir_changes"] <= max_dir_changes) and
        (m["net_move_pct"] >= min_net_move_pct)
    )
    return ok, m


# ================== SCAN ONE SYMBOL ==================

def scan_one_symbol(path: str,
                    session_date: date,
                    cutoff_dt: datetime,
                    er_min: float,
                    max_dir_changes: int,
                    min_net_move_pct: float,
                    min_abs_steps: float) -> Optional[Dict]:
    # OPTIMIZATION: end the scan window at cutoff_dt (not end-of-day)
    win_start = datetime.combine(session_date - timedelta(days=LOOKBACK_DAYS_FOR_PIVOT), dtime(0, 0))
    win_end = cutoff_dt

    df_win = read_symbol_window(path, win_start, win_end, session_date)
    if df_win.empty or (not has_required_cols(df_win)):
        return None

    piv = compute_cpr_from_prev_day(df_win, session_date)
    if not piv:
        return None

    PP = float(piv["PP"])
    BC = float(piv["BC"])
    TC = float(piv["TC"])
    band_lo, band_hi = _cpr_band(BC, TC)

    df_day = df_win[df_win["date"].dt.date == session_date].copy()
    if df_day.empty:
        return None

    df_upto = df_day[
        (df_day["date"] >= datetime.combine(session_date, SESSION_START)) &
        (df_day["date"] <= cutoff_dt)
    ].copy().sort_values("date").drop_duplicates(subset=["date"]).reset_index(drop=True)

    # Need enough candles to judge trend; with 1-min, 6 candles expected
    if len(df_upto) < 4:
        return None

    # Must have touched or crossed band in the window
    touched = band_touched_by_wicks(df_upto, band_lo, band_hi)
    crossed, first_cross, transition = crossed_band_by_close(df_upto, band_lo, band_hi)
    if not (touched or crossed):
        return None

    # Must be trending (not wavy) in the window
    trending_ok, tmet = is_trending_window(df_upto, er_min, max_dir_changes, min_net_move_pct, min_abs_steps)
    if not trending_ok:
        return None

    # Width metric (your original; but safe denom)
    denom = max(abs(TC), 1e-12)
    width_pct_signed = ((PP - TC) / denom) * 100.0
    width_pct_abs = abs(width_pct_signed)

    last = df_upto.iloc[-1]
    last_close = float(last["close"])
    last_time = last["date"]

    cutoff_state = "BELOW" if last_close < band_lo else ("ABOVE" if last_close > band_hi else "INSIDE")

    ex, sym = parse_exchange_symbol_from_filename(path)

    return {
        "exchange": ex,
        "symbol": sym,
        "path": path,
        "pivots": piv,  # reuse in plotting
        "prev_date": piv["prev_date"],
        "PP": PP,
        "BC": BC,
        "TC": TC,
        "CPR_LOW": band_lo,
        "CPR_HIGH": band_hi,
        "touched_band": bool(touched),
        "crossed_band": bool(crossed),
        "band_transition": transition,
        "first_cross_time": first_cross.strftime("%H:%M") if first_cross else "",
        "cutoff_time": last_time.strftime("%H:%M"),
        "cutoff_close": last_close,
        "cutoff_state": cutoff_state,
        "width_pct_signed": float(width_pct_signed),
        "width_pct_abs": float(width_pct_abs),
        "trend_er": float(tmet["er"]),
        "trend_dir_changes": int(tmet["dir_changes"]),
        "trend_net_move_pct": float(tmet["net_move_pct"]),
    }


# ================== PLOTTING (browser collage) ==================

def add_cpr_lines(fig: go.Figure, pivots: Optional[Dict[str, float]]):
    if not pivots:
        return

    PP = float(pivots["PP"])
    BC = float(pivots["BC"])
    TC = float(pivots["TC"])
    lo, hi = _cpr_band(BC, TC)

    fig.add_hrect(y0=lo, y1=hi, opacity=0.12, line_width=0)

    for k in ["PP", "BC", "TC"]:
        y = float(pivots[k])
        fig.add_hline(
            y=y,
            line_dash="dot",
            opacity=0.8,
            annotation_text=f"{k}: {y:.2f}",
            annotation_position="top left",
        )


def make_candle_fig(df_plot: pd.DataFrame, title: str, pivots: Optional[Dict[str, float]],
                    height: int, cutoff_dt: Optional[datetime] = None) -> go.Figure:
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

    add_cpr_lines(fig, pivots)

    if cutoff_dt is not None:
        fig.add_vline(x=cutoff_dt, line_dash="dot", opacity=0.6)

    fig.update_layout(
        title=title,
        xaxis_title="Time",
        yaxis_title="Price",
        xaxis_rangeslider_visible=False,
        template="plotly_white",
        height=height,
        margin=dict(l=30, r=6, t=45, b=30),
        showlegend=False,
    )
    return fig


def fig_to_html_div(fig: go.Figure, static: bool) -> str:
    config = {"displayModeBar": not static, "staticPlot": static, "scrollZoom": not static, "responsive": True}
    return fig.to_html(full_html=False, include_plotlyjs=False, config=config)


def build_collage_html(top_results: List[Dict],
                       session_date: date,
                       cutoff_dt: datetime,
                       out_path: str,
                       open_browser: bool):
    if not top_results:
        log("WARN", "No results to plot.")
        return

    mini_blocks, full_blocks = [], []

    for rank, r in enumerate(top_results, start=1):
        ex = r["exchange"]
        ts = r["symbol"]
        path = r["path"]
        piv = r.get("pivots")

        anchor = f"CPR_{ex}_{ts}_full".replace(":", "_").replace(" ", "_")

        # only re-read for top-5 (full-day chart)
        df_day = read_symbol_day(path, session_date)
        if df_day.empty or (not has_required_cols(df_day)) or (not piv):
            continue

        df_upto = df_day[
            (df_day["date"] >= datetime.combine(session_date, SESSION_START)) &
            (df_day["date"] <= cutoff_dt)
        ].copy()
        if df_upto.empty:
            continue

        mini_title = f"{rank}. {ex}:{ts} — touch/cross CPR — width={r['width_pct_abs']:.4f}%"
        full_title = f"{rank}. {ex}:{ts} — full day {session_date}"

        extra = (
            f"<div class='meta'>"
            f"<b>Touched:</b> {r['touched_band']} &nbsp; "
            f"<b>Crossed:</b> {r['crossed_band']} &nbsp; "
            f"<b>Transition:</b> {r.get('band_transition','-') or '-'} &nbsp; "
            f"<b>First cross:</b> {r.get('first_cross_time','-') or '-'} &nbsp; "
            f"<b>Cutoff:</b> {r['cutoff_close']:.2f} @ {r['cutoff_time']} ({r['cutoff_state']}) &nbsp; "
            f"<b>Trend ER:</b> {r['trend_er']:.2f} &nbsp; "
            f"<b>DirChanges:</b> {r['trend_dir_changes']} &nbsp; "
            f"<b>NetMove%:</b> {r['trend_net_move_pct']:.2f}% &nbsp; "
            f"<b>PP/BC/TC:</b> {r['PP']:.2f}/{r['BC']:.2f}/{r['TC']:.2f}"
            f"</div>"
        )

        fig_mini = make_candle_fig(df_upto, mini_title, piv, MINI_HEIGHT, cutoff_dt=cutoff_dt)
        fig_full = make_candle_fig(df_day, full_title, piv, FULL_HEIGHT, cutoff_dt=cutoff_dt)

        mini_div = fig_to_html_div(fig_mini, static=True)
        full_div = fig_to_html_div(fig_full, static=False)

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
{extra}
{full_div}
<hr />
""")

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8" />
<title>CPR 09:20 Trend Scan — {session_date}</title>
<script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
<style>
body {{ font-family: Arial, sans-serif; }}
.wrap {{ max-width: {PAGE_MAX_WIDTH_PX}px; margin: 0 auto; }}
.grid {{
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 12px;
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
  font-size: 13px;
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
.meta {{
  font-size: 12px;
  margin: 4px 0 6px 0;
}}
</style>
</head>
<body>
<div class="wrap">
<h1>CPR Scan (touch/cross CPR band + trending window)</h1>
<p>
<b>Date:</b> {session_date} &nbsp;&nbsp;
<b>Window:</b> {SESSION_START.strftime('%H:%M')}–{cutoff_dt.strftime('%H:%M')} IST &nbsp;&nbsp;
<b>Top N:</b> {len(top_results)} &nbsp;&nbsp;
<b>Rank metric:</b> abs((PP - TC)/TC)*100
</p>

<h2>Top matches (collage)</h2>
<div class="grid">
{''.join(mini_blocks)}
</div>

<hr />
<h2>Full-day charts</h2>
{''.join(full_blocks)}
</div>
</body>
</html>
"""
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

    abs_path = os.path.abspath(out_path)
    log("INFO", f"Saved collage HTML: {abs_path}")

    if open_browser:
        try:
            webbrowser.open(f"file:///{abs_path.replace(os.sep, '/')}")
        except Exception:
            pass


# ================== MAIN ==================

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=False, help="Analysis date YYYY-MM-DD (IST session)")
    ap.add_argument("--cutoff", default=DEFAULT_CUTOFF.strftime("%H:%M"), help="Cutoff time HH:MM (IST)")
    ap.add_argument("--top", type=int, default=TOP_N_DEFAULT, help="Top N to show (default 5)")
    ap.add_argument("--parquet-dir", default=DEFAULT_PARQUET_DIR)
    ap.add_argument("--out", default=DEFAULT_OUTPUT_HTML)
    ap.add_argument("--no-open", action="store_true", help="Do not open browser")

    # Trend filter knobs
    ap.add_argument("--trend-eff-min", type=float, default=TREND_EFF_MIN_DEFAULT, help="Min Efficiency Ratio (ER)")
    ap.add_argument("--max-dir-changes", type=int, default=MAX_DIR_CHANGES_DEFAULT, help="Max direction changes")
    ap.add_argument("--min-net-move-pct", type=float, default=MIN_NET_MOVE_PCT_DEFAULT, help="Min net move % in window")
    ap.add_argument("--min-abs-steps", type=float, default=MIN_ABS_STEPS_DEFAULT, help="Min sum(abs(steps)) in window")

    # Optional speed
    ap.add_argument("--workers", type=int, default=1, help="Threaded scan workers (I/O bound)")

    args = ap.parse_args()

    date_str = args.date or input("Enter analysis date (YYYY-MM-DD): ").strip()
    session_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    cutoff_time = datetime.strptime(args.cutoff, "%H:%M").time()
    cutoff_dt = datetime.combine(session_date, cutoff_time)

    files = list_parquet_files(args.parquet_dir)
    log("STEP", f"Found {len(files)} Parquet files in: {args.parquet_dir}")
    log("STEP", f"Scanning {session_date} window {SESSION_START.strftime('%H:%M')}–{cutoff_dt.strftime('%H:%M')} IST")
    log("STEP", f"Trend: ER>={args.trend_eff_min}, dirChanges<={args.max_dir_changes}, netMove%>={args.min_net_move_pct}, absSteps>={args.min_abs_steps}")

    results: List[Dict] = []

    if args.workers and args.workers > 1:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=int(args.workers)) as ex:
            futs = [
                ex.submit(
                    scan_one_symbol, p, session_date, cutoff_dt,
                    args.trend_eff_min, args.max_dir_changes, args.min_net_move_pct, args.min_abs_steps
                )
                for p in files
            ]
            done = 0
            for f in as_completed(futs):
                done += 1
                try:
                    r = f.result()
                    if r:
                        results.append(r)
                except Exception:
                    pass
                if done % 100 == 0:
                    log("STEP", f"Scanned {done}/{len(files)} | matches={len(results)}")
    else:
        for i, p in enumerate(files, start=1):
            try:
                r = scan_one_symbol(
                    p, session_date, cutoff_dt,
                    args.trend_eff_min, args.max_dir_changes, args.min_net_move_pct, args.min_abs_steps
                )
                if r:
                    results.append(r)
            except Exception:
                pass
            if i % 100 == 0:
                log("STEP", f"Scanned {i}/{len(files)} | matches={len(results)}")

    if not results:
        log("WARN", "No matches found.")
        return

    # Rank by minimum CPR width (your metric)
    results_sorted = sorted(results, key=lambda r: (r["width_pct_abs"], r["symbol"]))
    top = results_sorted[: int(args.top)]

    log("INFO", f"Matches total: {len(results)} | Showing top {len(top)}")
    for i, r in enumerate(top, start=1):
        log(
            "INFO",
            f"#{i} {r['exchange']}:{r['symbol']} width={r['width_pct_abs']:.4f}% "
            f"touch={r['touched_band']} cross={r['crossed_band']} "
            f"ER={r['trend_er']:.2f} dirChg={r['trend_dir_changes']} netMove%={r['trend_net_move_pct']:.2f} "
            f"cutoff={r['cutoff_close']:.2f}@{r['cutoff_time']}({r['cutoff_state']})"
        )

    build_collage_html(top, session_date, cutoff_dt, args.out, open_browser=(not args.no_open))


if __name__ == "__main__":
    main()
