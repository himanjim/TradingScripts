"""
scan_wm_live.py

Reads today's SQLite candle cache and finds W / inverted-W (M) in last 60 candles.

Output: Plotly HTML page with charts of matching stocks.

This is intentionally fast:
- SQL query pulls only last 60 candles per symbol for detection.
- For chart display, we pull full-day candles for that symbol (still cheap in SQLite).
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import webbrowser


# ================= CONFIG =================

CACHE_ROOT = r"./live_cache"
WINDOW = 60
TOP_SHOW = 25
OUTPUT_HTML = "wm_live_scan.html"

# W/M detection parameters (sane defaults; tune later)
SMOOTH_ROLL = 5
BOTTOM_TOP_TOL_PCT = 0.35
MIN_SEP_BARS = 6
MAX_SEP_BARS = 55
MIN_DEPTH_PCT = 0.25
MIN_HEIGHT_PCT = 0.25
FORMING_MAX_DIST_TO_LEVEL_PCT = 0.35
MIN_REBOUND_PCT = 0.35

CHART_HEIGHT = 340


# ================= HELPERS =================

def log(level: str, msg: str):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{now} [{level}] {msg}")

def today_ist() -> date:
    return datetime.now().date()

def day_dir(d: date) -> str:
    return os.path.join(CACHE_ROOT, d.isoformat())

def db_path(d: date) -> str:
    return os.path.join(day_dir(d), "candles.sqlite")

def open_db(path: str) -> sqlite3.Connection:
    if not os.path.isfile(path):
        raise FileNotFoundError(f"DB not found: {path} (run live_market_cache.py first)")
    conn = sqlite3.connect(path)
    return conn

def db_symbols(conn: sqlite3.Connection) -> List[str]:
    cur = conn.execute("SELECT DISTINCT symbol FROM candles;")
    return [r[0] for r in cur.fetchall()]

def db_last_n(conn: sqlite3.Connection, symbol: str, n: int) -> pd.DataFrame:
    cur = conn.execute("""
        SELECT ts, open, high, low, close, volume
        FROM candles
        WHERE symbol=?
        ORDER BY ts DESC
        LIMIT ?;
    """, (symbol, int(n)))
    rows = cur.fetchall()
    if not rows:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
    df = pd.DataFrame(rows, columns=["date", "open", "high", "low", "close", "volume"])
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    return df

def db_full_day(conn: sqlite3.Connection, symbol: str) -> pd.DataFrame:
    cur = conn.execute("""
        SELECT ts, open, high, low, close, volume
        FROM candles
        WHERE symbol=?
        ORDER BY ts ASC;
    """, (symbol,))
    rows = cur.fetchall()
    if not rows:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
    df = pd.DataFrame(rows, columns=["date", "open", "high", "low", "close", "volume"])
    df["date"] = pd.to_datetime(df["date"])
    return df


# ================= W/M DETECTION =================

def _swing_points_from_smooth(x: np.ndarray, roll: int = 5) -> Tuple[List[int], List[int]]:
    if len(x) < max(roll, 5):
        return [], []
    s = pd.Series(x).rolling(roll, center=True, min_periods=max(2, roll // 2)).mean().to_numpy()
    mins, maxs = [], []
    for i in range(2, len(s) - 2):
        if np.isnan(s[i - 2:i + 3]).any():
            continue
        if s[i] < s[i - 1] and s[i] < s[i + 1] and s[i] <= s[i - 2] and s[i] <= s[i + 2]:
            mins.append(i)
        if s[i] > s[i - 1] and s[i] > s[i + 1] and s[i] >= s[i - 2] and s[i] >= s[i + 2]:
            maxs.append(i)
    return mins, maxs

def _quality_score(strength_avg: float, tol: float, dist_to_level_pct: float) -> float:
    return strength_avg - (tol * 0.8) - (dist_to_level_pct * 1.2)

def detect_best_WM(df: pd.DataFrame) -> Optional[Dict]:
    if len(df) < WINDOW:
        return None

    w = df.tail(WINDOW).reset_index(drop=True)
    closes = w["close"].astype(float).to_numpy()
    highs = w["high"].astype(float).to_numpy()
    lows = w["low"].astype(float).to_numpy()
    last_close = float(closes[-1])

    mins, _ = _swing_points_from_smooth(lows, roll=SMOOTH_ROLL)
    _, maxs = _swing_points_from_smooth(highs, roll=SMOOTH_ROLL)

    best = None

    # ---- W: double-bottom ----
    if len(mins) >= 2:
        for i in range(len(mins) - 1):
            for j in range(i + 1, len(mins)):
                a, b = mins[i], mins[j]
                sep = b - a
                if sep < MIN_SEP_BARS or sep > MAX_SEP_BARS:
                    continue

                b1, b2 = float(lows[a]), float(lows[b])
                if b1 <= 0 or b2 <= 0:
                    continue

                tol = abs(b2 - b1) / ((b1 + b2) / 2.0) * 100.0
                if tol > BOTTOM_TOP_TOL_PCT:
                    continue

                level = float(np.max(highs[a:b + 1]))  # neckline
                level_idx = a + int(np.argmax(highs[a:b + 1]))

                depth1 = (level - b1) / level * 100.0
                depth2 = (level - b2) / level * 100.0
                if min(depth1, depth2) < MIN_DEPTH_PCT:
                    continue

                post = closes[b:]
                if len(post) < 3:
                    continue
                post_max = float(np.max(post))
                rebound_pct = (post_max - b2) / max(b2, 1e-9) * 100.0
                if rebound_pct < MIN_REBOUND_PCT:
                    continue

                dist_to_level_pct = abs(last_close - level) / level * 100.0
                forming_ok = (last_close >= level) or (dist_to_level_pct <= FORMING_MAX_DIST_TO_LEVEL_PCT)
                if not forming_ok:
                    continue

                q = _quality_score((depth1 + depth2) / 2.0, tol, dist_to_level_pct)
                cand = {
                    "type": "W",
                    "score": -q,
                    "tie": dist_to_level_pct,
                    "level": level,
                    "p1_idx": a,
                    "p2_idx": b,
                    "level_idx": level_idx,
                    "dist_to_level_pct": dist_to_level_pct,
                    "tol_pct": tol,
                }
                if best is None or (cand["score"], cand["tie"]) < (best["score"], best["tie"]):
                    best = cand

    # ---- M: double-top (inverted W) ----
    if len(maxs) >= 2:
        for i in range(len(maxs) - 1):
            for j in range(i + 1, len(maxs)):
                a, b = maxs[i], maxs[j]
                sep = b - a
                if sep < MIN_SEP_BARS or sep > MAX_SEP_BARS:
                    continue

                t1, t2 = float(highs[a]), float(highs[b])
                if t1 <= 0 or t2 <= 0:
                    continue

                tol = abs(t2 - t1) / ((t1 + t2) / 2.0) * 100.0
                if tol > BOTTOM_TOP_TOL_PCT:
                    continue

                level = float(np.min(lows[a:b + 1]))  # trough
                level_idx = a + int(np.argmin(lows[a:b + 1]))

                height1 = (t1 - level) / max(level, 1e-9) * 100.0
                height2 = (t2 - level) / max(level, 1e-9) * 100.0
                if min(height1, height2) < MIN_HEIGHT_PCT:
                    continue

                post = closes[b:]
                if len(post) < 3:
                    continue
                post_min = float(np.min(post))
                drop_pct = (t2 - post_min) / max(t2, 1e-9) * 100.0
                if drop_pct < MIN_REBOUND_PCT:
                    continue

                dist_to_level_pct = abs(last_close - level) / max(level, 1e-9) * 100.0
                forming_ok = (last_close <= level) or (dist_to_level_pct <= FORMING_MAX_DIST_TO_LEVEL_PCT)
                if not forming_ok:
                    continue

                q = _quality_score((height1 + height2) / 2.0, tol, dist_to_level_pct)
                cand = {
                    "type": "M",
                    "score": -q,
                    "tie": dist_to_level_pct,
                    "level": level,
                    "p1_idx": a,
                    "p2_idx": b,
                    "level_idx": level_idx,
                    "dist_to_level_pct": dist_to_level_pct,
                    "tol_pct": tol,
                }
                if best is None or (cand["score"], cand["tie"]) < (best["score"], best["tie"]):
                    best = cand

    return best


# ================= CHARTING =================

def make_fig(full_df: pd.DataFrame, best: Dict, title: str) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Candlestick(
        x=full_df["date"],
        open=full_df["open"],
        high=full_df["high"],
        low=full_df["low"],
        close=full_df["close"],
        name=""
    ))

    level = float(best["level"])
    fig.add_hline(
        y=level,
        line_dash="dot",
        opacity=0.8,
        annotation_text=f"{best['type']} level {level:.2f}",
        annotation_position="top left",
    )

    # mark the points in last-window coordinates
    w = full_df.tail(WINDOW).reset_index(drop=True)
    p1, p2, li = int(best["p1_idx"]), int(best["p2_idx"]), int(best["level_idx"])
    p1 = max(0, min(len(w) - 1, p1))
    p2 = max(0, min(len(w) - 1, p2))
    li = max(0, min(len(w) - 1, li))

    if best["type"] == "W":
        y1, y2, yL = float(w.loc[p1, "low"]), float(w.loc[p2, "low"]), float(w.loc[li, "high"])
        t1, t2, tL = "B1", "B2", "Neck"
    else:
        y1, y2, yL = float(w.loc[p1, "high"]), float(w.loc[p2, "high"]), float(w.loc[li, "low"])
        t1, t2, tL = "T1", "T2", "Tr"

    fig.add_trace(go.Scatter(
        x=[w.loc[p1, "date"], w.loc[p2, "date"]],
        y=[y1, y2],
        mode="markers+text",
        text=[t1, t2],
        textposition="top center",
        showlegend=False
    ))
    fig.add_trace(go.Scatter(
        x=[w.loc[li, "date"]],
        y=[yL],
        mode="markers+text",
        text=[tL],
        textposition="top center",
        showlegend=False
    ))

    fig.update_layout(
        title=title,
        xaxis_rangeslider_visible=False,
        template="plotly_white",
        height=CHART_HEIGHT,
        margin=dict(l=30, r=10, t=50, b=30),
        showlegend=False
    )
    return fig

def fig_to_div(fig: go.Figure) -> str:
    return fig.to_html(full_html=False, include_plotlyjs=False, config={"responsive": True})

def build_html(results: List[Dict], out_path: str, d: date) -> None:
    blocks = []
    for i, r in enumerate(results, start=1):
        sym = r["symbol"]
        b = r["best"]
        meta = f"{b['type']}  dist%={b['dist_to_level_pct']:.3f}  tol%={b['tol_pct']:.3f}"
        title = f"{i}. {sym} — {meta}"
        fig = make_fig(r["full_df"], b, title)
        blocks.append(f"<div class='card'>{fig_to_div(fig)}</div>")

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8" />
<title>W/M Scan — {d}</title>
<script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
<style>
body {{ font-family: Arial, sans-serif; }}
.wrap {{ max-width: 1100px; margin: 0 auto; }}
.grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }}
.card {{ border: 1px solid #ddd; padding: 8px; background: #fafafa; }}
</style>
</head>
<body>
<div class="wrap">
<h1>W / inverted-W scan</h1>
<p><b>Date:</b> {d} &nbsp; <b>Window:</b> last {WINDOW} candles &nbsp; <b>Matches:</b> {len(results)}</p>
<div class="grid">
{''.join(blocks)}
</div>
</div>
</body>
</html>
"""
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

    abs_path = os.path.abspath(out_path)
    log("INFO", f"Saved HTML: {abs_path}")
    try:
        webbrowser.open(f"file:///{abs_path.replace(os.sep, '/')}")
    except Exception:
        pass


# ================= MAIN =================

def main():
    d = today_ist()
    conn = open_db(db_path(d))

    syms = db_symbols(conn)
    if not syms:
        log("ERROR", "No symbols found in DB yet. Start live_market_cache.py and wait for data.")
        return

    log("INFO", f"Found {len(syms)} symbols in DB. Scanning last {WINDOW} candles...")

    results = []
    for sym in syms:
        df = db_last_n(conn, sym, WINDOW)
        if len(df) < WINDOW:
            continue

        best = detect_best_WM(df)
        if not best:
            continue

        full_df = db_full_day(conn, sym)
        results.append({"symbol": sym, "best": best, "full_df": full_df})

    if not results:
        log("WARN", "No W/M matches found.")
        return

    results = sorted(results, key=lambda r: (r["best"]["score"], r["best"]["tie"]))[:TOP_SHOW]

    for i, r in enumerate(results, start=1):
        b = r["best"]
        log("INFO", f"#{i} {r['symbol']}  {b['type']}  dist%={b['dist_to_level_pct']:.3f} tol%={b['tol_pct']:.3f}")

    build_html(results, OUTPUT_HTML, d)

if __name__ == "__main__":
    main()
