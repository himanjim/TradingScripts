"""
scan_wm_live.py (FIXED: freshness selection + continuous 80-candle scan)

Key fixes vs your previous version:
- Builds a continuous last-80-minute window (80 candles) per symbol (gap-filled).
- Detects ALL W/M occurrences in that 80-candle window.
- Filters by freshness first (P2 age <= MAX_PATTERN_AGE_MIN), THEN picks best.
  (This avoids dropping a symbol just because the "best" pattern is older.)
- Enforces:
    W: day_open > LTP
    M: day_open < LTP
- Prints symbol + formed time + age(min) + SL (W bottom / M top)
- Shows CPR lines (P, BC, TC, R1, S1) computed from previous day H/L/C (standard Zerodha formulas).
"""

from __future__ import annotations

import os
import json
import sqlite3
from datetime import datetime, date, time as dtime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
import webbrowser

import Trading_2024.OptionTradeUtils as oUtils


# ================= CONFIG =================

CACHE_ROOT = "./live_cache"
OUTPUT_HTML = "wm_live_top3.html"

SCAN_BARS = 80                  # scan only last 80 candles (continuous 80 minutes)
DISPLAY_BARS = 240              # chart shows last N minutes (continuous grid)
TOP_PER_TYPE = 3

MAX_PATTERN_AGE_MIN = 15        # patterns older than this are rejected

PLOTLY_JS_MODE = "inline"

SESSION_START = dtime(9, 15)
SESSION_END = dtime(15, 30)

# --- W/M params (same as kite_day_wm_scan.py) ---
SMOOTH_ROLL = 5
BOTTOM_TOP_TOL_PCT = 0.35
MIN_SEP_BARS = 6
MAX_SEP_BARS = 70
MIN_DEPTH_PCT = 0.25
MIN_HEIGHT_PCT = 0.25
MIN_REBOUND_PCT = 0.35
LOOKAHEAD_BARS_VALIDATE = 20


# ================= LOG =================

def log(level: str, msg: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{now} [{level}] {msg}")


# ================= PATHS =================

def today_ist() -> date:
    return datetime.now().date()

def day_dir(d: date) -> str:
    return os.path.join(CACHE_ROOT, d.isoformat())

def db_path(d: date) -> str:
    return os.path.join(day_dir(d), "candles.sqlite")

def day_open_path(d: date) -> str:
    return os.path.join(day_dir(d), "day_open.json")


# ================= DB READ (read-only) =================

def open_db_ro(path: str) -> sqlite3.Connection:
    if not os.path.isfile(path):
        raise FileNotFoundError(f"DB not found: {path} (run live_market_cache.py)")
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)

def db_symbols(conn: sqlite3.Connection) -> List[str]:
    cur = conn.execute("SELECT DISTINCT symbol FROM candles;")
    return [r[0] for r in cur.fetchall()]

def db_last_ts_close(conn: sqlite3.Connection, symbol: str) -> Optional[Tuple[datetime, float]]:
    cur = conn.execute("""
        SELECT ts, close FROM candles
        WHERE symbol=?
        ORDER BY ts DESC
        LIMIT 1;
    """, (symbol,))
    row = cur.fetchone()
    if not row:
        return None
    return datetime.fromisoformat(row[0]), float(row[1])

def db_first_open(conn: sqlite3.Connection, symbol: str) -> Optional[Tuple[datetime, float]]:
    cur = conn.execute("""
        SELECT ts, open FROM candles
        WHERE symbol=?
        ORDER BY ts ASC
        LIMIT 1;
    """, (symbol,))
    row = cur.fetchone()
    if not row:
        return None
    return datetime.fromisoformat(row[0]), float(row[1])

def db_range(conn: sqlite3.Connection, symbol: str, start_ts: datetime, end_ts: datetime) -> pd.DataFrame:
    cur = conn.execute("""
        SELECT ts, open, high, low, close, volume
        FROM candles
        WHERE symbol=? AND ts>=? AND ts<=?
        ORDER BY ts ASC;
    """, (symbol, start_ts.isoformat(), end_ts.isoformat()))
    rows = cur.fetchall()
    return pd.DataFrame(rows, columns=["date", "open", "high", "low", "close", "volume"])

def db_last_before(conn: sqlite3.Connection, symbol: str, ts: datetime) -> Optional[Tuple[datetime, float]]:
    cur = conn.execute("""
        SELECT ts, close FROM candles
        WHERE symbol=? AND ts < ?
        ORDER BY ts DESC
        LIMIT 1;
    """, (symbol, ts.isoformat()))
    row = cur.fetchone()
    if not row:
        return None
    return datetime.fromisoformat(row[0]), float(row[1])

def sanitize_candles(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["date", "open", "high", "low", "close"])
    df = df.drop_duplicates(subset=["date"], keep="last")
    df = df.sort_values("date").reset_index(drop=True)
    df["volume"] = df["volume"].fillna(0).astype(int)
    return df


# ================= CONTINUOUS WINDOW (gap-filled) =================

def make_continuous_minute_window(
    conn: sqlite3.Connection,
    symbol: str,
    end_ts: datetime,
    minutes: int,
) -> pd.DataFrame:
    """
    Continuous 1-minute grid for [end_ts-minutes+1, end_ts],
    filling missing minutes with flat candles (close carried forward), vol=0.
    """
    end_ts = end_ts.replace(second=0, microsecond=0)
    start_ts = end_ts - timedelta(minutes=minutes - 1)

    df = sanitize_candles(db_range(conn, symbol, start_ts, end_ts))
    df = df.set_index("date") if not df.empty else pd.DataFrame().set_index(pd.DatetimeIndex([], name="date"))

    idx = pd.date_range(start_ts, end_ts, freq="1min")
    df = df.reindex(idx)

    seed = db_last_before(conn, symbol, start_ts)
    seed_close = seed[1] if seed else None

    if seed_close is not None and "close" in df.columns and pd.isna(df["close"].iloc[0]):
        df.iloc[0, df.columns.get_loc("close")] = seed_close

    df["close"] = df["close"].ffill()

    for c in ["open", "high", "low"]:
        df[c] = df[c].where(df[c].notna(), df["close"])

    df["volume"] = df["volume"].fillna(0).astype(int)

    df = df.reset_index().rename(columns={"index": "date"})
    return df


# ================= CPR (Zerodha standard) =================

def compute_cpr_from_prev_day_hlc(H: float, L: float, C: float) -> Dict[str, float]:
    P = (H + L + C) / 3.0
    BC = (H + L) / 2.0
    TC = 2.0 * P - BC
    R1 = 2.0 * P - L
    S1 = 2.0 * P - H
    return {"P": P, "BC": BC, "TC": TC, "R1": R1, "S1": S1}


# ================= W/M DETECTION (same as kite_day_wm_scan.py) =================

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

def _dedup_occ(occ: List[Dict], new: Dict) -> bool:
    for o in occ:
        if o["type"] != new["type"]:
            continue
        if abs(int(o["p2_idx"]) - int(new["p2_idx"])) <= 3:
            lvl = float(new["level"])
            if abs(float(o["level"]) - lvl) / max(lvl, 1e-9) * 100.0 <= BOTTOM_TOP_TOL_PCT:
                return True
    return False

def detect_all_WM(df_day: pd.DataFrame) -> List[Dict]:
    if df_day is None or df_day.empty or len(df_day) < 60:
        return []

    d = df_day.reset_index(drop=True)
    closes = d["close"].astype(float).to_numpy()
    highs = d["high"].astype(float).to_numpy()
    lows = d["low"].astype(float).to_numpy()

    mins, _ = _swing_points_from_smooth(lows, roll=SMOOTH_ROLL)
    _, maxs = _swing_points_from_smooth(highs, roll=SMOOTH_ROLL)

    occ: List[Dict] = []
    N = len(d)

    # W
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

                seg = highs[a:b + 1]
                level = float(np.max(seg))
                level_idx = a + int(np.argmax(seg))

                depth1 = (level - b1) / level * 100.0
                depth2 = (level - b2) / level * 100.0
                if min(depth1, depth2) < MIN_DEPTH_PCT:
                    continue

                end_val = min(b + LOOKAHEAD_BARS_VALIDATE, N - 1)
                post = closes[b:end_val + 1]
                if len(post) < 3:
                    continue

                rebound_pct = (float(np.max(post)) - b2) / max(b2, 1e-9) * 100.0
                if rebound_pct < MIN_REBOUND_PCT:
                    continue

                breakout_idx = None
                for k in range(b, N):
                    if float(closes[k]) >= level:
                        breakout_idx = k
                        break

                close_eval = float(closes[end_val])
                dist_to_level_pct = abs(close_eval - level) / max(level, 1e-9) * 100.0
                q = _quality_score((depth1 + depth2) / 2.0, tol, dist_to_level_pct)

                new = {
                    "type": "W",
                    "score": -q,
                    "tie": dist_to_level_pct,
                    "level": level,
                    "tol_pct": tol,
                    "p1_idx": a,
                    "p2_idx": b,
                    "level_idx": level_idx,
                    "breakout_idx": breakout_idx,
                }
                if not _dedup_occ(occ, new):
                    occ.append(new)

    # M
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

                seg = lows[a:b + 1]
                level = float(np.min(seg))
                level_idx = a + int(np.argmin(seg))

                height1 = (t1 - level) / max(level, 1e-9) * 100.0
                height2 = (t2 - level) / max(level, 1e-9) * 100.0
                if min(height1, height2) < MIN_HEIGHT_PCT:
                    continue

                end_val = min(b + LOOKAHEAD_BARS_VALIDATE, N - 1)
                post = closes[b:end_val + 1]
                if len(post) < 3:
                    continue

                drop_pct = (t2 - float(np.min(post))) / max(t2, 1e-9) * 100.0
                if drop_pct < MIN_REBOUND_PCT:
                    continue

                breakout_idx = None
                for k in range(b, N):
                    if float(closes[k]) <= level:
                        breakout_idx = k
                        break

                close_eval = float(closes[end_val])
                dist_to_level_pct = abs(close_eval - level) / max(level, 1e-9) * 100.0
                q = _quality_score((height1 + height2) / 2.0, tol, dist_to_level_pct)

                new = {
                    "type": "M",
                    "score": -q,
                    "tie": dist_to_level_pct,
                    "level": level,
                    "tol_pct": tol,
                    "p1_idx": a,
                    "p2_idx": b,
                    "level_idx": level_idx,
                    "breakout_idx": breakout_idx,
                }
                if not _dedup_occ(occ, new):
                    occ.append(new)

    return sorted(occ, key=lambda r: (r["score"], r["tie"]))


def pick_best_recent(occ: List[Dict], df_scan: pd.DataFrame, now_ref: datetime, max_age_min: int, typ: str) -> Optional[Dict]:
    """
    Pick the best (lowest score,tie) among occurrences whose P2 is <= max_age_min old.
    """
    cand = []
    for o in occ:
        if o["type"] != typ:
            continue
        p2 = int(o["p2_idx"])
        if p2 < 0 or p2 >= len(df_scan):
            continue
        p2_time = pd.to_datetime(df_scan.loc[p2, "date"]).to_pydatetime().replace(second=0, microsecond=0)
        age_min = int((now_ref - p2_time).total_seconds() // 60)
        if age_min < 0:
            age_min = 0
        if age_min <= max_age_min:
            o2 = dict(o)
            o2["_p2_time"] = p2_time
            o2["_age_min"] = age_min
            cand.append(o2)

    if not cand:
        return None
    cand.sort(key=lambda r: (r["score"], r["tie"]))
    return cand[0]


# ================= PLOT HELPERS =================

def add_hline(fig: go.Figure, y: float, text: str):
    fig.add_shape(
        type="line",
        xref="paper", x0=0, x1=1,
        yref="y", y0=y, y1=y,
        line=dict(width=1, dash="dot"),
        opacity=0.75,
    )
    fig.add_annotation(
        x=0, xref="paper",
        y=y, yref="y",
        text=text,
        showarrow=False,
        xanchor="left",
        bgcolor="rgba(255,255,255,0.65)",
        font=dict(size=10),
    )

def add_vline(fig: go.Figure, x_dt: datetime, text: str, dash: str = "solid", opacity: float = 0.5):
    fig.add_shape(
        type="line",
        xref="x", x0=x_dt, x1=x_dt,
        yref="paper", y0=0, y1=1,
        line=dict(width=1, dash=dash),
        opacity=opacity,
    )
    fig.add_annotation(
        x=x_dt, xref="x",
        y=0.98, yref="paper",
        text=text,
        showarrow=False,
        xanchor="left",
        bgcolor="rgba(255,255,255,0.65)",
        font=dict(size=10),
    )

def plot_symbol(df_plot: pd.DataFrame, pivots: Optional[Dict[str, float]], best: Dict, title: str) -> go.Figure:
    fig = go.Figure()

    fig.add_trace(go.Candlestick(
        x=df_plot["date"],
        open=df_plot["open"],
        high=df_plot["high"],
        low=df_plot["low"],
        close=df_plot["close"],
        name=""
    ))

    if pivots:
        for k in ["R1", "BC", "P", "TC", "S1"]:
            add_hline(fig, float(pivots[k]), f"{k}: {pivots[k]:.2f}")

    p1_t = best["p1_time"]
    p2_t = best["p2_time"]
    li_t = best["level_time"]
    level = float(best["level"])

    fig.add_shape(
        type="line",
        xref="x", x0=p1_t, x1=p2_t,
        yref="y", y0=level, y1=level,
        line=dict(width=1, dash="dot"),
        opacity=0.8,
    )

    fig.add_trace(go.Scatter(
        x=[p1_t, p2_t],
        y=[best["p1_y"], best["p2_y"]],
        mode="markers+text",
        text=[best["p1_label"], best["p2_label"]],
        textposition="top center",
        showlegend=False
    ))
    fig.add_trace(go.Scatter(
        x=[li_t],
        y=[best["level_y"]],
        mode="markers+text",
        text=[best["level_label"]],
        textposition="top center",
        showlegend=False
    ))

    add_vline(fig, p2_t, f"{best['type']} complete {p2_t.strftime('%H:%M')}", dash="solid", opacity=0.45)
    if best.get("breakout_time") is not None:
        add_vline(fig, best["breakout_time"], f"break {best['breakout_time'].strftime('%H:%M')}", dash="dash", opacity=0.75)

    fig.update_layout(
        title=title,
        xaxis_rangeslider_visible=False,
        template="plotly_white",
        height=520,
        margin=dict(l=30, r=10, t=70, b=30),
        showlegend=False
    )
    return fig


# ================= MAIN =================

def load_day_open_map(d: date) -> Dict[str, float]:
    p = day_open_path(d)
    if not os.path.isfile(p):
        return {}
    with open(p, "r", encoding="utf-8") as f:
        data = json.load(f) or {}
    return {str(k): float(v) for k, v in data.items()}

def main():
    d = today_ist()
    conn = open_db_ro(db_path(d))

    syms = db_symbols(conn)
    if not syms:
        log("ERROR", "No symbols in DB yet. Start live_market_cache.py and wait.")
        return

    day_open_map = load_day_open_map(d)
    log("INFO", f"Symbols in DB: {len(syms)} | day_open cached: {len(day_open_map)}")

    now_ref = datetime.now().replace(second=0, microsecond=0)

    # debug counters
    cnt_seen = 0
    cnt_have_last = 0
    cnt_scan_ready = 0
    cnt_any_occ = 0
    cnt_recent_W = 0
    cnt_recent_M = 0
    cnt_pass_rule_W = 0
    cnt_pass_rule_M = 0

    candidates_W = []
    candidates_M = []

    for sym in syms:
        cnt_seen += 1

        last = db_last_ts_close(conn, sym)
        if not last:
            continue
        cnt_have_last += 1
        end_ts, ltp = last

        # Open (prefer cached day_open)
        day_open = day_open_map.get(sym)
        if day_open is None:
            fo = db_first_open(conn, sym)
            if not fo:
                continue
            day_open = fo[1]

        # ---- scan window: continuous last 80 minutes (80 candles) ----
        df_scan = make_continuous_minute_window(conn, sym, end_ts=end_ts, minutes=SCAN_BARS)
        df_scan = sanitize_candles(df_scan)
        if len(df_scan) < 60:
            continue
        cnt_scan_ready += 1

        occ = detect_all_WM(df_scan)
        if not occ:
            continue
        cnt_any_occ += 1

        # pick best RECENT W and/or M (freshness filter first)
        bestW = pick_best_recent(occ, df_scan, now_ref, MAX_PATTERN_AGE_MIN, "W")
        bestM = pick_best_recent(occ, df_scan, now_ref, MAX_PATTERN_AGE_MIN, "M")

        # For display window
        df_plot = make_continuous_minute_window(conn, sym, end_ts=end_ts, minutes=DISPLAY_BARS)
        df_plot = sanitize_candles(df_plot)
        if df_plot.empty or len(df_plot) < 30:
            continue

        # helper to package record
        def make_rec(best: Dict, typ: str) -> Dict:
            p1 = int(best["p1_idx"]); p2 = int(best["p2_idx"]); li = int(best["level_idx"])
            p1 = max(0, min(len(df_scan) - 1, p1))
            p2 = max(0, min(len(df_scan) - 1, p2))
            li = max(0, min(len(df_scan) - 1, li))

            p2_time = best["_p2_time"]
            age_min = int(best["_age_min"])

            p1_time = pd.to_datetime(df_scan.loc[p1, "date"]).to_pydatetime().replace(second=0, microsecond=0)
            li_time = pd.to_datetime(df_scan.loc[li, "date"]).to_pydatetime().replace(second=0, microsecond=0)

            # Stoploss
            if typ == "W":
                b1 = float(df_scan.loc[p1, "low"])
                b2v = float(df_scan.loc[p2, "low"])
                stoploss = min(b1, b2v)     # bottom of W
            else:
                t1 = float(df_scan.loc[p1, "high"])
                t2v = float(df_scan.loc[p2, "high"])
                stoploss = max(t1, t2v)     # top of M

            diff_pct = abs(day_open - ltp) / max(day_open, 1e-9) * 100.0

            best2 = dict(best)
            best2["p1_time"] = p1_time
            best2["p2_time"] = p2_time
            best2["level_time"] = li_time

            if typ == "W":
                best2["p1_y"] = float(df_scan.loc[p1, "low"])
                best2["p2_y"] = float(df_scan.loc[p2, "low"])
                best2["level_y"] = float(df_scan.loc[li, "high"])
                best2["p1_label"] = "B1"
                best2["p2_label"] = "B2"
                best2["level_label"] = "Neck"
            else:
                best2["p1_y"] = float(df_scan.loc[p1, "high"])
                best2["p2_y"] = float(df_scan.loc[p2, "high"])
                best2["level_y"] = float(df_scan.loc[li, "low"])
                best2["p1_label"] = "T1"
                best2["p2_label"] = "T2"
                best2["level_label"] = "Tr"

            bo = best.get("breakout_idx")
            if bo is not None:
                bo = int(bo)
                bo = max(0, min(len(df_scan) - 1, bo))
                best2["breakout_time"] = pd.to_datetime(df_scan.loc[bo, "date"]).to_pydatetime().replace(second=0, microsecond=0)
            else:
                best2["breakout_time"] = None

            return {
                "symbol": sym,
                "type": typ,
                "diff_pct": diff_pct,
                "open": float(day_open),
                "ltp": float(ltp),
                "formed_time": p2_time,
                "age_min": age_min,
                "stoploss": float(stoploss),
                "best": best2,
                "df_plot": df_plot,
            }

        # ---- apply your Open vs LTP direction rule ----
        if bestW is not None:
            cnt_recent_W += 1
            if float(day_open) > float(ltp):  # W rule
                cnt_pass_rule_W += 1
                candidates_W.append(make_rec(bestW, "W"))

        if bestM is not None:
            cnt_recent_M += 1
            if float(day_open) < float(ltp):  # M rule
                cnt_pass_rule_M += 1
                candidates_M.append(make_rec(bestM, "M"))

    # show debug summary so you can see which filter is killing results
    log("INFO", f"Scan summary: seen={cnt_seen} have_last={cnt_have_last} scan_ready={cnt_scan_ready} any_occ={cnt_any_occ} "
                f"recentW={cnt_recent_W} recentM={cnt_recent_M} passRuleW={cnt_pass_rule_W} passRuleM={cnt_pass_rule_M}")

    if not candidates_W and not candidates_M:
        log("WARN", f"No candidates after filters (fresh <= {MAX_PATTERN_AGE_MIN}m AND Open/LTP rule).")
        return

    # Top 3 by |Open-LTP|%
    candidates_W = sorted(candidates_W, key=lambda r: r["diff_pct"], reverse=True)[:TOP_PER_TYPE]
    candidates_M = sorted(candidates_M, key=lambda r: r["diff_pct"], reverse=True)[:TOP_PER_TYPE]

    print("\n========== TOP W ==========")
    for i, r in enumerate(candidates_W, 1):
        print(f"{i}) {r['symbol']} | formed={r['formed_time'].strftime('%H:%M')} (age={r['age_min']}m) | "
              f"Open={r['open']:.2f} LTP={r['ltp']:.2f} Δ%={r['diff_pct']:.2f} | SL={r['stoploss']:.2f}")

    print("\n========== TOP M ==========")
    for i, r in enumerate(candidates_M, 1):
        print(f"{i}) {r['symbol']} | formed={r['formed_time'].strftime('%H:%M')} (age={r['age_min']}m) | "
              f"Open={r['open']:.2f} LTP={r['ltp']:.2f} Δ%={r['diff_pct']:.2f} | SL={r['stoploss']:.2f}")

    # CPR pivots only for final displayed symbols (small API usage)
    log("STEP", "Computing CPR pivots for top picks (small Kite usage)...")
    kite = oUtils.intialize_kite_api()

    def get_token(sym_key: str) -> int:
        q = kite.quote([sym_key]) or {}
        d0 = q.get(sym_key) or {}
        tok = d0.get("instrument_token")
        if tok is None:
            raise RuntimeError(f"instrument_token not found for {sym_key}")
        return int(tok)

    def prev_day_hlc(token: int, target_day: date) -> Tuple[date, float, float, float]:
        # use daily candles and pick last day before target_day
        from_dt = datetime.combine(target_day - timedelta(days=20), dtime(0, 0))
        to_dt = datetime.combine(target_day, dtime(0, 0))
        candles = kite.historical_data(token, from_dt, to_dt, "day", continuous=False, oi=False) or []
        picked = None
        for c in candles:
            cdate = c["date"].date() if hasattr(c["date"], "date") else pd.to_datetime(c["date"]).date()
            if cdate < target_day:
                picked = (cdate, float(c["high"]), float(c["low"]), float(c["close"]))
        if picked is None:
            raise RuntimeError("prev day candle not found")
        return picked

    final_list = candidates_W + candidates_M

    blocks = []
    for i, r in enumerate(final_list, start=1):
        sym = r["symbol"]

        piv = None
        piv_from = None
        try:
            tok = get_token(sym)
            pday, H, L, C = prev_day_hlc(tok, d)
            piv = compute_cpr_from_prev_day_hlc(H, L, C)
            piv_from = pday
        except Exception as e:
            log("WARN", f"CPR fetch failed for {sym}: {e}. Chart will be shown without CPR lines.")

        title = (f"{sym} | {r['type']} | formed {r['formed_time'].strftime('%H:%M')} (age {r['age_min']}m) | "
                 f"Open={r['open']:.2f} LTP={r['ltp']:.2f} Δ%={r['diff_pct']:.2f} | SL={r['stoploss']:.2f}")
        if piv_from:
            title += f" | CPR from {piv_from}"

        fig = plot_symbol(r["df_plot"], piv, r["best"], title)

        include_js = PLOTLY_JS_MODE if i == 1 else False
        div = pio.to_html(fig, full_html=False, include_plotlyjs=include_js, config={"responsive": True})
        blocks.append(f"<div class='card'>{div}</div>")

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8" />
<title>WM Top3 — {d}</title>
<style>
body {{ font-family: Arial, sans-serif; }}
.wrap {{ max-width: 1400px; margin: 0 auto; }}
.grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }}
.card {{ border: 1px solid #ddd; padding: 8px; background: #fafafa; overflow: visible; }}
</style>
</head>
<body>
<div class="wrap">
<h1>Top {TOP_PER_TYPE} W + Top {TOP_PER_TYPE} M (ranked by |Open-LTP|%)</h1>
<p>
<b>Date:</b> {d}
&nbsp; <b>Scan bars:</b> {SCAN_BARS}
&nbsp; <b>Max pattern age:</b> {MAX_PATTERN_AGE_MIN} min
&nbsp; <b>Display minutes:</b> {DISPLAY_BARS}
</p>
<div class="grid">
{''.join(blocks)}
</div>
</div>
</body>
</html>
"""
    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)

    outp = os.path.abspath(OUTPUT_HTML)
    log("INFO", f"Saved: {outp}")
    try:
        webbrowser.open(f"file:///{outp.replace(os.sep, '/')}")
    except Exception:
        pass


if __name__ == "__main__":
    main()
