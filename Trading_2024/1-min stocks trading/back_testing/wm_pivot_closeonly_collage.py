# wm_pivot_closeonly_collage.py
# -----------------------------------------------------------------------------
# Scans per-stock 1-min Parquet OHLCV and detects "trend -> W / inverted-W (M)
# reversal around a pivot line" BEFORE a cutoff time (default 10:00 IST).
#
# Detection uses ONLY the CLOSE series (simpler).
# Pivot lines computed from previous trading day (Kite CPR):
#   P  = (H + L + C) / 3
#   BC = (H + L) / 2
#   TC = 2P - BC
#   R1 = 2P - L
#   S1 = 2P - H
# -----------------------------------------------------------------------------

import os
import glob
import argparse
from dataclasses import dataclass
from datetime import datetime, date, time as dtime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import webbrowser


# =============================================================================
# CONFIG (ALL TUNABLES HERE — no runtime prompts for parameters)
# =============================================================================

PARQUET_DIR = "../stock_history_parquet"
OUTPUT_HTML = "wm_pivot_closeonly_collage.html"

# Time interpretation:
# "AUTO": if earliest candle on session_date is ~03:45, assume stored UTC-naive and shift +5:30
# "IST": timestamps already IST-naive
# "UTC": timestamps are UTC-naive (always shift +5:30)
DATA_TIME_MODE = "AUTO"
IST_OFFSET = timedelta(hours=5, minutes=30)

SESSION_START = dtime(9, 15)
CUTOFF_TIME = dtime(9, 40)

LOOKBACK_DAYS = 12

TOP_N = 5
WORKERS = 8

PAGE_MAX_WIDTH_PX = 980
MINI_HEIGHT = 310
FULL_HEIGHT = 440

# ---- Pivot proximity ----
# Bottoms/tops must be within +/- this % of the pivot level
PIVOT_TOUCH_TOL_PCT = 0.25

# ---- NEW: same-side requirement for W upper tips / M lower tips ----
# We classify a value v relative to pivot P as:
#   ABOVE if v > P + eps
#   BELOW if v < P - eps
#   NEAR  otherwise (treated as neither side; candidate rejected for strictness)
PIVOT_SIDE_EPS_PCT = 0.03  # raise slightly (0.05) if too strict

# Extremum detection smoothing on closes (still close-only)
SMOOTH_ROLL = 3

MIN_SEP_BARS = 3
MAX_SEP_BARS = 28

# ---- Trend leg (start -> first test) ----
MIN_LEG_MOVE_PCT = 0.40
TREND_ER_MIN = 0.40
TREND_MAX_DIR_CHANGES = 4
MIN_LEG_BARS = 6

# ---- Reversal confirmation ----
BREAK_CONFIRM_PCT = 0.00
REVERSAL_MUST_HAPPEN_BEFORE_CUTOFF = True

# =============================================================================
# END CONFIG
# =============================================================================


def log(level: str, msg: str):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{now} [{level}] {msg}")


# ================== Parquet helpers ==================

def list_parquet_files(parquet_dir: str) -> List[str]:
    if not os.path.isdir(parquet_dir):
        raise FileNotFoundError(f"Parquet directory not found: {parquet_dir}")
    return sorted(glob.glob(os.path.join(parquet_dir, "*.parquet")))


def parse_exchange_symbol_from_filename(path: str) -> Tuple[str, str]:
    base = os.path.splitext(os.path.basename(path))[0]
    if "_" in base:
        ex, sym = base.split("_", 1)
    else:
        ex, sym = "NSE", base
    return ex, sym


def normalize_date_series(s: pd.Series) -> pd.Series:
    s = s.astype("object")
    s = s.map(lambda x: x.replace(tzinfo=None) if isinstance(x, datetime) and x.tzinfo else x)
    s = pd.to_datetime(s, errors="coerce")
    if isinstance(s.dtype, pd.DatetimeTZDtype):
        s = s.dt.tz_convert(None)
    return s


def to_ist_naive(df: pd.DataFrame, session_date: date) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    df["date"] = normalize_date_series(df["date"])

    if DATA_TIME_MODE == "IST":
        return df
    if DATA_TIME_MODE == "UTC":
        df["date"] = df["date"] + IST_OFFSET
        return df

    # AUTO detect
    day = df[df["date"].dt.date == session_date]
    if not day.empty:
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
    df["date"] = normalize_date_series(df["date"])
    df = df.dropna(subset=["date"]).sort_values("date")

    df = df[(df["date"] >= start_dt) & (df["date"] <= end_dt)]
    df = to_ist_naive(df, session_date_for_tz)
    df = df.sort_values("date").drop_duplicates(subset=["date"]).reset_index(drop=True)
    return df


def read_symbol_day(path: str, session_date: date) -> pd.DataFrame:
    start_dt = datetime.combine(session_date, dtime(0, 0))
    end_dt = datetime.combine(session_date + timedelta(days=1), dtime(0, 0))
    cols = ["date", "open", "high", "low", "close", "volume"]

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
    df["date"] = normalize_date_series(df["date"])
    df = df.dropna(subset=["date"]).sort_values("date")
    df = to_ist_naive(df, session_date)
    df = df[df["date"].dt.date == session_date].copy()
    df = df.sort_values("date").drop_duplicates(subset=["date"]).reset_index(drop=True)
    return df


# ================== Pivot logic (Kite CPR) ==================

def compute_kite_pivots_from_prev_day(df_win: pd.DataFrame, session_date: date) -> Optional[Dict[str, float]]:
    if df_win.empty:
        return None

    dates = sorted({d.date() for d in df_win["date"]})
    prev_dates = [d for d in dates if d < session_date]
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
    TC = 2.0 * P - BC
    R1 = 2.0 * P - L
    S1 = 2.0 * P - H

    return {"prev_date": prev_date, "P": P, "BC": BC, "TC": TC, "R1": R1, "S1": S1}


# ================== Close-only helpers ==================

def rolling_smooth(x: np.ndarray, w: int) -> np.ndarray:
    if w <= 1 or len(x) < w:
        return x
    return pd.Series(x).rolling(w, center=True, min_periods=max(2, w // 2)).mean().to_numpy()


def local_extrema_indices(x: np.ndarray) -> Tuple[List[int], List[int]]:
    mins, maxs = [], []
    if len(x) < 5:
        return mins, maxs
    for i in range(2, len(x) - 2):
        if np.isnan(x[i - 2:i + 3]).any():
            continue
        if x[i] < x[i - 1] and x[i] < x[i + 1] and x[i] <= x[i - 2] and x[i] <= x[i + 2]:
            mins.append(i)
        if x[i] > x[i - 1] and x[i] > x[i + 1] and x[i] >= x[i - 2] and x[i] >= x[i + 2]:
            maxs.append(i)
    return mins, maxs


def efficiency_ratio(closes: np.ndarray) -> float:
    if len(closes) < 2:
        return 0.0
    steps = np.diff(closes)
    denom = float(np.sum(np.abs(steps))) + 1e-12
    return float(abs(closes[-1] - closes[0]) / denom)


def direction_changes(closes: np.ndarray) -> int:
    if len(closes) < 3:
        return 0
    steps = np.diff(closes)
    s = np.sign(steps)
    s = s[s != 0]
    if len(s) < 2:
        return 0
    return int(np.sum(s[1:] != s[:-1]))


def pct_diff(a: float, b: float) -> float:
    return abs(a - b) / max(abs(b), 1e-12) * 100.0


def first_cross_up(closes: np.ndarray, level: float, start_idx: int, confirm_pct: float) -> Optional[int]:
    thr = level * (1.0 + confirm_pct / 100.0)
    for i in range(start_idx, len(closes)):
        if closes[i] >= thr:
            return i
    return None


def first_cross_down(closes: np.ndarray, level: float, start_idx: int, confirm_pct: float) -> Optional[int]:
    thr = level * (1.0 - confirm_pct / 100.0)
    for i in range(start_idx, len(closes)):
        if closes[i] <= thr:
            return i
    return None


def side_of_pivot(v: float, pivot: float) -> int:
    """
    Returns:
      +1 => ABOVE pivot
      -1 => BELOW pivot
       0 => NEAR pivot (within eps band)
    """
    eps = abs(pivot) * (PIVOT_SIDE_EPS_PCT / 100.0)
    if v > pivot + eps:
        return 1
    if v < pivot - eps:
        return -1
    return 0


# ================== Pattern detection ==================

@dataclass
class PatternCandidate:
    pattern_type: str          # "W" or "M"
    pivot_name: str            # "P", "BC", "TC", "R1", "S1"
    pivot_level: float

    move_pct: float            # ranking metric: max move % before reversal confirm

    # key points (window-relative indices)
    p1_idx: int
    p2_idx: int
    mid1_idx: int              # W: upper tip 1 (neckline between bottoms); M: lower tip 1 (trough between tops)
    mid2_idx: int              # W: upper tip 2 (max close from p2..rev); M: lower tip 2 (min close from p2..rev)
    rev_idx: int

    # timestamps (used for plotting in any df)
    p1_time: datetime
    p2_time: datetime
    mid1_time: datetime
    mid2_time: datetime
    rev_time: datetime

    # prices
    start_close: float
    p1_close: float
    p2_close: float
    mid1_close: float
    mid2_close: float
    rev_close: float


def detect_best_pattern_close_only(df_win: pd.DataFrame, pivots: Dict[str, float]) -> Optional[PatternCandidate]:
    """
    Adds NEW constraint:
      - W: the two UPPER tips (mid1 and mid2) must be on the SAME SIDE of the pivot (above/below)
      - M: the two LOWER tips (mid1 and mid2) must be on the SAME SIDE of the pivot (above/below)
    """
    if df_win.empty or len(df_win) < 18:
        return None

    w = df_win.reset_index(drop=True)
    closes = w["close"].astype(float).to_numpy()
    start_close = float(closes[0])
    if start_close <= 0:
        return None

    sm = rolling_smooth(closes, SMOOTH_ROLL)
    mins, maxs = local_extrema_indices(sm)

    best: Optional[PatternCandidate] = None

    pivot_lines = [("R1", pivots["R1"]), ("BC", pivots["BC"]), ("P", pivots["P"]), ("TC", pivots["TC"]), ("S1", pivots["S1"])]

    def leg_ok(is_up: bool, end_idx: int) -> bool:
        if end_idx < MIN_LEG_BARS:
            return False
        seg = closes[: end_idx + 1]
        er = efficiency_ratio(seg)
        dc = direction_changes(seg)
        move = (seg[-1] - seg[0]) / max(abs(seg[0]), 1e-12) * 100.0
        if is_up:
            if move < MIN_LEG_MOVE_PCT:
                return False
        else:
            if -move < MIN_LEG_MOVE_PCT:
                return False
        return (er >= TREND_ER_MIN) and (dc <= TREND_MAX_DIR_CHANGES)

    # ------------------ W (double bottom near pivot) ------------------
    for pname, plevel in pivot_lines:
        for i in range(len(mins) - 1):
            for j in range(i + 1, len(mins)):
                p1, p2 = mins[i], mins[j]
                sep = p2 - p1
                if sep < MIN_SEP_BARS or sep > MAX_SEP_BARS:
                    continue

                c1, c2 = float(closes[p1]), float(closes[p2])
                if pct_diff(c1, plevel) > PIVOT_TOUCH_TOL_PCT or pct_diff(c2, plevel) > PIVOT_TOUCH_TOL_PCT:
                    continue

                if not leg_ok(is_up=False, end_idx=p1):
                    continue

                # mid1 = upper tip between bottoms (neckline peak on closes)
                seg = closes[p1:p2 + 1]
                mid1_rel = int(np.argmax(seg))
                mid1_idx = p1 + mid1_rel
                mid1_close = float(closes[mid1_idx])

                # reversal confirm: breakout above mid1
                rev_idx = first_cross_up(closes, mid1_close, p2, BREAK_CONFIRM_PCT)
                if rev_idx is None:
                    continue
                if REVERSAL_MUST_HAPPEN_BEFORE_CUTOFF and rev_idx >= len(closes):
                    continue

                # mid2 = upper tip after p2 before/at reversal (max close in [p2..rev_idx])
                seg2 = closes[p2:rev_idx + 1]
                mid2_rel = int(np.argmax(seg2))
                mid2_idx = p2 + mid2_rel
                mid2_close = float(closes[mid2_idx])

                # NEW: both upper tips (mid1, mid2) must be on same side of pivot
                s1 = side_of_pivot(mid1_close, plevel)
                s2 = side_of_pivot(mid2_close, plevel)
                if s1 == 0 or s2 == 0 or s1 != s2:
                    continue

                # ranking metric: maximum adverse move (down) before reversal
                min_before = float(np.min(closes[: rev_idx + 1]))
                move_pct = (start_close - min_before) / max(abs(start_close), 1e-12) * 100.0

                cand = PatternCandidate(
                    pattern_type="W",
                    pivot_name=pname,
                    pivot_level=float(plevel),
                    move_pct=float(move_pct),
                    p1_idx=p1,
                    p2_idx=p2,
                    mid1_idx=mid1_idx,
                    mid2_idx=mid2_idx,
                    rev_idx=rev_idx,
                    p1_time=w.loc[p1, "date"],
                    p2_time=w.loc[p2, "date"],
                    mid1_time=w.loc[mid1_idx, "date"],
                    mid2_time=w.loc[mid2_idx, "date"],
                    rev_time=w.loc[rev_idx, "date"],
                    start_close=start_close,
                    p1_close=c1,
                    p2_close=c2,
                    mid1_close=mid1_close,
                    mid2_close=mid2_close,
                    rev_close=float(closes[rev_idx]),
                )

                if (best is None) or (cand.move_pct > best.move_pct):
                    best = cand

    # ------------------ M (double top near pivot) ------------------
    for pname, plevel in pivot_lines:
        for i in range(len(maxs) - 1):
            for j in range(i + 1, len(maxs)):
                p1, p2 = maxs[i], maxs[j]
                sep = p2 - p1
                if sep < MIN_SEP_BARS or sep > MAX_SEP_BARS:
                    continue

                c1, c2 = float(closes[p1]), float(closes[p2])
                if pct_diff(c1, plevel) > PIVOT_TOUCH_TOL_PCT or pct_diff(c2, plevel) > PIVOT_TOUCH_TOL_PCT:
                    continue

                if not leg_ok(is_up=True, end_idx=p1):
                    continue

                # mid1 = lower tip between tops (trough on closes)
                seg = closes[p1:p2 + 1]
                mid1_rel = int(np.argmin(seg))
                mid1_idx = p1 + mid1_rel
                mid1_close = float(closes[mid1_idx])

                # reversal confirm: breakdown below mid1
                rev_idx = first_cross_down(closes, mid1_close, p2, BREAK_CONFIRM_PCT)
                if rev_idx is None:
                    continue

                # mid2 = lower tip after p2 before/at reversal (min close in [p2..rev_idx])
                seg2 = closes[p2:rev_idx + 1]
                mid2_rel = int(np.argmin(seg2))
                mid2_idx = p2 + mid2_rel
                mid2_close = float(closes[mid2_idx])

                # NEW: both lower tips (mid1, mid2) must be on same side of pivot
                s1 = side_of_pivot(mid1_close, plevel)
                s2 = side_of_pivot(mid2_close, plevel)
                if s1 == 0 or s2 == 0 or s1 != s2:
                    continue

                # ranking metric: maximum favourable move (up) before reversal
                max_before = float(np.max(closes[: rev_idx + 1]))
                move_pct = (max_before - start_close) / max(abs(start_close), 1e-12) * 100.0

                cand = PatternCandidate(
                    pattern_type="M",
                    pivot_name=pname,
                    pivot_level=float(plevel),
                    move_pct=float(move_pct),
                    p1_idx=p1,
                    p2_idx=p2,
                    mid1_idx=mid1_idx,
                    mid2_idx=mid2_idx,
                    rev_idx=rev_idx,
                    p1_time=w.loc[p1, "date"],
                    p2_time=w.loc[p2, "date"],
                    mid1_time=w.loc[mid1_idx, "date"],
                    mid2_time=w.loc[mid2_idx, "date"],
                    rev_time=w.loc[rev_idx, "date"],
                    start_close=start_close,
                    p1_close=c1,
                    p2_close=c2,
                    mid1_close=mid1_close,
                    mid2_close=mid2_close,
                    rev_close=float(closes[rev_idx]),
                )

                if (best is None) or (cand.move_pct > best.move_pct):
                    best = cand

    return best


# ================== Plotting (timestamp-safe) ==================

def nearest_idx_by_time(df: pd.DataFrame, t: datetime) -> int:
    if df.empty:
        return 0
    # vectorized nearest search
    s = (df["date"] - t).abs()
    return int(s.idxmin())


def fig_to_html_div(fig: go.Figure, static: bool) -> str:
    config = {"displayModeBar": not static, "staticPlot": static, "scrollZoom": not static, "responsive": True}
    return fig.to_html(full_html=False, include_plotlyjs=False, config=config)


def add_pattern_marks(fig: go.Figure, df_plot: pd.DataFrame, cand: PatternCandidate):
    df_plot = df_plot.reset_index(drop=True)

    i_p1 = nearest_idx_by_time(df_plot, cand.p1_time)
    i_p2 = nearest_idx_by_time(df_plot, cand.p2_time)
    i_m1 = nearest_idx_by_time(df_plot, cand.mid1_time)
    i_m2 = nearest_idx_by_time(df_plot, cand.mid2_time)
    i_rev = nearest_idx_by_time(df_plot, cand.rev_time)

    # pivot line
    fig.add_hline(
        y=cand.pivot_level,
        line_dash="dot",
        opacity=0.9,
        annotation_text=f"{cand.pivot_name}: {cand.pivot_level:.2f}",
        annotation_position="top left",
    )

    # mid1 line (neckline/trough)
    fig.add_hline(
        y=cand.mid1_close,
        line_dash="dash",
        opacity=0.65,
        annotation_text=f"mid1: {cand.mid1_close:.2f}",
        annotation_position="bottom left",
    )

    # reversal vertical
    fig.add_vline(x=df_plot.loc[i_rev, "date"], line_dash="dot", opacity=0.7)

    def mark(idx: int, label: str):
        fig.add_trace(go.Scatter(
            x=[df_plot.loc[idx, "date"]],
            y=[df_plot.loc[idx, "close"]],
            mode="markers+text",
            text=[label],
            textposition="top center",
            marker=dict(size=10, symbol="circle"),
            hoverinfo="skip",
            showlegend=False
        ))

    # P1/P2 tests near pivot
    mark(i_p1, "P1")
    mark(i_p2, "P2")

    # Tips: W => U1/U2, M => L1/L2
    if cand.pattern_type == "W":
        mark(i_m1, "U1")
        mark(i_m2, "U2")
    else:
        mark(i_m1, "L1")
        mark(i_m2, "L2")


def make_fig(df_plot: pd.DataFrame, title: str, height: int, cand: Optional[PatternCandidate]) -> go.Figure:
    fig = go.Figure()

    fig.add_trace(go.Candlestick(
        x=df_plot["date"],
        open=df_plot["open"],
        high=df_plot["high"],
        low=df_plot["low"],
        close=df_plot["close"],
        name=""
    ))

    # close overlay (explicit, since detection uses close)
    fig.add_trace(go.Scatter(
        x=df_plot["date"],
        y=df_plot["close"],
        mode="lines",
        opacity=0.7,
        showlegend=False
    ))

    if cand is not None:
        add_pattern_marks(fig, df_plot, cand)

    fig.update_layout(
        title=title,
        xaxis_title="Time",
        yaxis_title="Price",
        xaxis_rangeslider_visible=False,
        template="plotly_white",
        height=height,
        margin=dict(l=30, r=6, t=45, b=30),
        showlegend=False
    )
    return fig


def build_collage_html(rows: List[Dict], session_date: date, cutoff_dt: datetime, out_path: str):
    mini_blocks, full_blocks = [], []

    for rank, r in enumerate(rows, start=1):
        ex, sym, path = r["exchange"], r["symbol"], r["path"]
        cand: PatternCandidate = r["cand"]

        anchor = f"WM_{ex}_{sym}_full".replace(":", "_").replace(" ", "_")

        df_day = read_symbol_day(path, session_date)
        if df_day.empty:
            continue

        df_win = df_day[
            (df_day["date"] >= datetime.combine(session_date, SESSION_START)) &
            (df_day["date"] <= cutoff_dt)
        ].copy().reset_index(drop=True)

        if df_win.empty or len(df_win) < 10:
            continue

        extra = (
            f"<div class='meta'>"
            f"<b>Type:</b> {cand.pattern_type} &nbsp; "
            f"<b>Pivot:</b> {cand.pivot_name} ({cand.pivot_level:.2f}) &nbsp; "
            f"<b>Move before reversal:</b> {cand.move_pct:.2f}% &nbsp; "
            f"<b>Reversal:</b> {cand.rev_time.strftime('%H:%M')} ({cand.rev_close:.2f})"
            f"</div>"
        )

        mini_title = f"{rank}. {ex}:{sym} — {cand.pattern_type} near {cand.pivot_name} — move={cand.move_pct:.2f}% (to {cutoff_dt.strftime('%H:%M')})"
        full_title = f"{rank}. {ex}:{sym} — full day {session_date}"

        fig_mini = make_fig(df_win, mini_title, MINI_HEIGHT, cand)
        fig_full = make_fig(df_day, full_title, FULL_HEIGHT, cand)

        mini_blocks.append(f"""
<div class="mini-chart">
  <div class="mini-head">
    <div class="mini-title">{mini_title}</div>
    <a class="btn" href="#{anchor}">Open full-day</a>
  </div>
  {extra}
  {fig_to_html_div(fig_mini, static=True)}
</div>
""")

        full_blocks.append(f"""
<h2 id="{anchor}">{full_title}</h2>
{extra}
{fig_to_html_div(fig_full, static=False)}
<hr />
""")

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8" />
<title>W/M Pivot (Close-only) — {session_date}</title>
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
<h1>W / Inverted-W (M) Reversal Around Pivot Lines (Close-only detection)</h1>
<p>
<b>Date:</b> {session_date} &nbsp;&nbsp;
<b>Window:</b> {SESSION_START.strftime('%H:%M')}–{cutoff_dt.strftime('%H:%M')} IST &nbsp;&nbsp;
<b>Rank:</b> max move % before reversal
</p>

<h2>Top matches</h2>
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
    log("INFO", f"Saved HTML: {abs_path}")
    webbrowser.open(f"file:///{abs_path.replace(os.sep, '/')}")


# ================== Scan ==================

def scan_one_file(path: str, session_date: date, cutoff_dt: datetime) -> Optional[Dict]:
    start_dt = datetime.combine(session_date - timedelta(days=LOOKBACK_DAYS), dtime(0, 0))
    df = read_symbol_window(path, start_dt, cutoff_dt, session_date)
    if df.empty:
        return None

    piv = compute_kite_pivots_from_prev_day(df, session_date)
    if not piv:
        return None

    df_win = df[
        (df["date"].dt.date == session_date) &
        (df["date"] >= datetime.combine(session_date, SESSION_START)) &
        (df["date"] <= cutoff_dt)
    ].copy().reset_index(drop=True)

    if len(df_win) < 18:
        return None

    cand = detect_best_pattern_close_only(df_win, piv)
    if cand is None:
        return None

    ex, sym = parse_exchange_symbol_from_filename(path)
    return {"exchange": ex, "symbol": sym, "path": path, "cand": cand}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date",default="2025-12-01", help="Analysis date YYYY-MM-DD (IST)")
    args = ap.parse_args()

    session_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    cutoff_dt = datetime.combine(session_date, CUTOFF_TIME)

    files = list_parquet_files(PARQUET_DIR)
    log("STEP", f"Found {len(files)} parquet files in: {PARQUET_DIR}")
    log("STEP", f"Scanning {session_date} window {SESSION_START.strftime('%H:%M')}–{CUTOFF_TIME.strftime('%H:%M')} IST")

    results: List[Dict] = []

    if WORKERS and WORKERS > 1:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=int(WORKERS)) as ex:
            futs = [ex.submit(scan_one_file, p, session_date, cutoff_dt) for p in files]
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
                r = scan_one_file(p, session_date, cutoff_dt)
                if r:
                    results.append(r)
            except Exception:
                pass
            if i % 100 == 0:
                log("STEP", f"Scanned {i}/{len(files)} | matches={len(results)}")

    if not results:
        log("WARN", "No matches. Relax CONFIG: PIVOT_TOUCH_TOL_PCT, PIVOT_SIDE_EPS_PCT, MIN_LEG_MOVE_PCT, TREND_ER_MIN.")
        return

    results_sorted = sorted(results, key=lambda r: (-r["cand"].move_pct, r["symbol"]))
    top = results_sorted[:TOP_N]

    log("INFO", f"Matches total: {len(results)} | Showing top {len(top)}")
    for i, r in enumerate(top, start=1):
        c = r["cand"]
        log("INFO", f"#{i} {r['exchange']}:{r['symbol']} {c.pattern_type} near {c.pivot_name} "
                    f"move={c.move_pct:.2f}% U/L same-side enforced")

    build_collage_html(top, session_date, cutoff_dt, OUTPUT_HTML)


if __name__ == "__main__":
    main()
