"""
kite_day_wm_scan.py

- Input: stock symbol + date (YYYY-MM-DD)
- Fetch 1-min candles for:
    (A) target day (scan W / inverted-W anywhere in that day)
    (B) previous trading day (for CPR pivots)
- Compute CPR pivots from previous day's 1-min session data (H/L/C)
- Detect W and M occurrences across the target day
- Plot target day candlestick with:
    - pivot lines: P, BC, TC, R1, S1
    - pattern points (B1/B2/Neck or T1/T2/Trough)
    - vertical line at completion time (P2)
    - vertical line at breakout time (if it happens later in the same day)

Assumes your Kite init:
  import Trading_2024.OptionTradeUtils as oUtils
  kite = oUtils.intialize_kite_api()
"""

from __future__ import annotations

import os
import argparse
from datetime import datetime, date, time as dtime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import webbrowser

from kiteconnect import KiteConnect
import Trading_2024.OptionTradeUtils as oUtils  # your helper


# ===================== CONFIG =====================

SESSION_START = dtime(9, 15)
SESSION_END = dtime(15, 30)

IST_OFFSET = timedelta(hours=5, minutes=30)
PREV_TRADING_DAY_MAX_BACK_DAYS = 12

SMOOTH_ROLL = 5
BOTTOM_TOP_TOL_PCT = 0.35
MIN_SEP_BARS = 6
MAX_SEP_BARS = 70
MIN_DEPTH_PCT = 0.25
MIN_HEIGHT_PCT = 0.25
MIN_REBOUND_PCT = 0.35
LOOKAHEAD_BARS_VALIDATE = 20

MAX_ANNOTATIONS_PER_TYPE = 10

INSTR_CACHE_DIR = "./kite_instruments_cache"
INSTR_CACHE_TTL_DAYS = 7


# ===================== LOG =====================

def log(level: str, msg: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{now} [{level}] {msg}")


# ===================== TIME HELPERS =====================

def floor_minute(ts: datetime) -> datetime:
    return ts.replace(second=0, microsecond=0)

def strip_tz(ts):
    if ts is None:
        return ts
    if hasattr(ts, "to_pydatetime"):
        ts = ts.to_pydatetime()
    if getattr(ts, "tzinfo", None) is not None:
        ts = ts.replace(tzinfo=None)
    return ts

def to_py_dt(x):
    """Always return a python datetime (not pandas Timestamp)."""
    if x is None:
        return None
    if isinstance(x, pd.Timestamp):
        return x.to_pydatetime()
    if hasattr(x, "to_pydatetime"):
        return x.to_pydatetime()
    return x

def in_session(ts: datetime, session_day: date) -> bool:
    if ts.date() != session_day:
        return False
    t = ts.time()
    return (t >= SESSION_START) and (t <= SESSION_END)

def looks_like_utc_naive_session(ts_min: datetime, ts_max: datetime) -> bool:
    tmin = ts_min.time()
    tmax = ts_max.time()
    return (dtime(3, 15) <= tmin <= dtime(4, 45)) and (dtime(9, 30) <= tmax <= dtime(10, 30))

def auto_shift_to_ist_if_needed(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    ts_min = to_py_dt(df["date"].min())
    ts_max = to_py_dt(df["date"].max())
    if looks_like_utc_naive_session(ts_min, ts_max):
        df = df.copy()
        df["date"] = df["date"] + IST_OFFSET
    return df


# ===================== CPR PIVOTS =====================

def compute_cpr_pivots(prev_day_1m: pd.DataFrame) -> Dict[str, float]:
    if prev_day_1m.empty:
        raise ValueError("prev_day_1m is empty; cannot compute pivots.")

    H = float(prev_day_1m["high"].max())
    L = float(prev_day_1m["low"].min())
    C = float(prev_day_1m.sort_values("date")["close"].iloc[-1])

    P = (H + L + C) / 3.0
    BC = (H + L) / 2.0
    TC = 2.0 * P - BC
    R1 = 2.0 * P - L
    S1 = 2.0 * P - H

    return {"P": P, "BC": BC, "TC": TC, "R1": R1, "S1": S1}


# ===================== SYMBOL/TOKEN =====================

def resolve_symbol_exchange(symbol_in: str, exchange_in: Optional[str]) -> Tuple[str, str]:
    s = symbol_in.strip()
    if ":" in s:
        ex, ts = s.split(":", 1)
        return ex.upper().strip(), ts.strip()
    ex = (exchange_in or "NSE").upper().strip()
    return ex, s

def _cache_is_fresh(path: str, ttl_days: int) -> bool:
    if not os.path.isfile(path):
        return False
    mtime = datetime.fromtimestamp(os.path.getmtime(path))
    return (datetime.now() - mtime).days <= ttl_days

def _instr_cache_file(exchange: str) -> str:
    os.makedirs(INSTR_CACHE_DIR, exist_ok=True)
    return os.path.join(INSTR_CACHE_DIR, f"instruments_{exchange.upper()}.csv")

def load_instruments_df(kite: KiteConnect, exchange: str) -> pd.DataFrame:
    cache = _instr_cache_file(exchange)
    if _cache_is_fresh(cache, INSTR_CACHE_TTL_DAYS):
        df = pd.read_csv(cache)
        df["exchange"] = df["exchange"].astype(str).str.upper()
        df["tradingsymbol"] = df["tradingsymbol"].astype(str).str.strip()
        return df

    log("STEP", f"Downloading instruments dump for {exchange} (fallback; cached {INSTR_CACHE_TTL_DAYS}d)")
    inst = kite.instruments(exchange)
    df = pd.DataFrame(inst)
    keep = ["exchange", "tradingsymbol", "instrument_token"]
    df = df[[c for c in keep if c in df.columns]].copy()
    df["exchange"] = df["exchange"].astype(str).str.upper()
    df["tradingsymbol"] = df["tradingsymbol"].astype(str).str.strip()
    df.to_csv(cache, index=False)
    return df

def get_instrument_token_fast(kite: KiteConnect, exchange: str, tradingsymbol: str) -> int:
    sym_key = f"{exchange}:{tradingsymbol}"
    try:
        q = kite.quote([sym_key]) or {}
        d = q.get(sym_key) or {}
        tok = d.get("instrument_token")
        if tok is not None:
            return int(tok)
    except Exception:
        pass

    df = load_instruments_df(kite, exchange)
    hit = df[(df["exchange"] == exchange.upper()) & (df["tradingsymbol"] == tradingsymbol.strip())]
    if hit.empty:
        raise ValueError(f"Instrument not found: {exchange}:{tradingsymbol}")
    return int(hit["instrument_token"].iloc[0])


# ===================== HISTORICAL FETCH (1-min only) =====================

def fetch_1m_session(kite: KiteConnect, token: int, day: date) -> pd.DataFrame:
    from_dt = datetime(day.year, day.month, day.day, 0, 0, 0)
    to_dt = from_dt + timedelta(days=1)

    candles = kite.historical_data(
        instrument_token=token,
        from_date=from_dt,
        to_date=to_dt,
        interval="minute",
        continuous=False,
        oi=False,
    ) or []

    if not candles:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    df = pd.DataFrame(candles)

    df["date"] = pd.to_datetime(df["date"].map(strip_tz))
    df["date"] = df["date"].map(floor_minute)
    df = df.sort_values("date").reset_index(drop=True)

    df = auto_shift_to_ist_if_needed(df)

    df = df[df["date"].map(lambda x: in_session(to_py_dt(x), day))].copy()
    df = df.drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)

    for c in ["open", "high", "low", "close"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["volume"] = pd.to_numeric(df.get("volume", 0), errors="coerce").fillna(0).astype(int)
    df = df.dropna(subset=["open", "high", "low", "close"]).reset_index(drop=True)

    return df

def fetch_prev_trading_day_1m(kite: KiteConnect, token: int, target_day: date) -> Tuple[date, pd.DataFrame]:
    d0 = target_day - timedelta(days=1)
    df0 = fetch_1m_session(kite, token, d0)
    if not df0.empty:
        return d0, df0

    for i in range(2, PREV_TRADING_DAY_MAX_BACK_DAYS + 1):
        d = target_day - timedelta(days=i)
        df = fetch_1m_session(kite, token, d)
        if not df.empty:
            return d, df

    raise RuntimeError(f"Could not find previous trading day within last {PREV_TRADING_DAY_MAX_BACK_DAYS} days.")


# ===================== W / M DETECTION =====================

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

                post_max = float(np.max(post))
                rebound_pct = (post_max - b2) / max(b2, 1e-9) * 100.0
                if rebound_pct < MIN_REBOUND_PCT:
                    continue

                breakout_idx = None
                for k in range(b, N):
                    if float(closes[k]) >= level:
                        breakout_idx = k
                        break

                close_eval = float(closes[end_val])
                dist_to_level_pct = abs(close_eval - level) / level * 100.0
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
                    "validate_end_idx": end_val,
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

                post_min = float(np.min(post))
                drop_pct = (t2 - post_min) / max(t2, 1e-9) * 100.0
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
                    "validate_end_idx": end_val,
                    "breakout_idx": breakout_idx,
                }
                if not _dedup_occ(occ, new):
                    occ.append(new)

    return sorted(occ, key=lambda r: (r["score"], r["tie"]))


# ===================== PLOTTING (FIXED: no add_vline with annotation_text) =====================

def add_hline_with_label(fig: go.Figure, y: float, text: str):
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
        bgcolor="rgba(255,255,255,0.6)",
    )

def add_vline_with_label(fig: go.Figure, x_dt: datetime, text: str, dash: str, opacity: float, y_paper: float = 0.98):
    x_dt = to_py_dt(x_dt)
    fig.add_shape(
        type="line",
        xref="x", x0=x_dt, x1=x_dt,
        yref="paper", y0=0, y1=1,
        line=dict(width=1, dash=dash),
        opacity=opacity,
    )
    fig.add_annotation(
        x=x_dt, xref="x",
        y=y_paper, yref="paper",
        text=text,
        showarrow=False,
        xanchor="left",
        bgcolor="rgba(255,255,255,0.6)",
    )

def add_pivot_lines(fig: go.Figure, pivots: Dict[str, float]) -> None:
    for k in ["P", "BC", "TC", "R1", "S1"]:
        y = float(pivots[k])
        add_hline_with_label(fig, y, f"{k}: {y:.2f}")

def plot_day_with_patterns(df_day: pd.DataFrame, pivots: Dict[str, float], occ: List[Dict], title: str) -> go.Figure:
    # Avoid FutureWarning by passing numpy array of python datetimes, not pandas Series
    x_arr = np.array(df_day["date"].dt.to_pydatetime())

    fig = go.Figure()
    fig.add_trace(go.Candlestick(
        x=x_arr,
        open=df_day["open"].to_numpy(),
        high=df_day["high"].to_numpy(),
        low=df_day["low"].to_numpy(),
        close=df_day["close"].to_numpy(),
        name=""
    ))

    add_pivot_lines(fig, pivots)

    w_list = [o for o in occ if o["type"] == "W"][:MAX_ANNOTATIONS_PER_TYPE]
    m_list = [o for o in occ if o["type"] == "M"][:MAX_ANNOTATIONS_PER_TYPE]

    w_i = 0
    m_i = 0
    ann_y = 0.98  # stack labels a bit so they don’t overlap completely

    for o in (w_list + m_list):
        typ = o["type"]
        if typ == "W":
            w_i += 1
            label = f"W#{w_i}"
        else:
            m_i += 1
            label = f"M#{m_i}"

        p1 = int(o["p1_idx"]); p2 = int(o["p2_idx"]); li = int(o["level_idx"])
        level = float(o["level"])
        breakout_idx = o.get("breakout_idx")

        t_p1 = to_py_dt(df_day.loc[p1, "date"])
        t_p2 = to_py_dt(df_day.loc[p2, "date"])
        t_li = to_py_dt(df_day.loc[li, "date"])

        # Level line over pattern span (p1..p2)
        fig.add_shape(
            type="line",
            xref="x", x0=t_p1, x1=t_p2,
            yref="y", y0=level, y1=level,
            line=dict(width=1, dash="dot"),
            opacity=0.75,
        )

        if typ == "W":
            y1 = float(df_day.loc[p1, "low"])
            y2 = float(df_day.loc[p2, "low"])
            yL = float(df_day.loc[li, "high"])
            txt1, txt2, txtL = "B1", "B2", "Neck"
        else:
            y1 = float(df_day.loc[p1, "high"])
            y2 = float(df_day.loc[p2, "high"])
            yL = float(df_day.loc[li, "low"])
            txt1, txt2, txtL = "T1", "T2", "Tr"

        fig.add_trace(go.Scatter(
            x=[t_p1, t_p2],
            y=[y1, y2],
            mode="markers+text",
            text=[txt1, txt2],
            textposition="top center",
            marker=dict(size=9, symbol="circle"),
            showlegend=False
        ))
        fig.add_trace(go.Scatter(
            x=[t_li],
            y=[yL],
            mode="markers+text",
            text=[txtL],
            textposition="top center",
            marker=dict(size=9, symbol="diamond"),
            showlegend=False
        ))

        # Completion line at P2 (solid)
        add_vline_with_label(
            fig, t_p2,
            text=f"{label} complete @ {t_p2.strftime('%H:%M')}",
            dash="solid",
            opacity=0.45,
            y_paper=ann_y
        )
        ann_y = max(0.70, ann_y - 0.04)

        # Breakout line later in the day (dashed)
        if breakout_idx is not None and int(breakout_idx) != p2:
            t_bo = to_py_dt(df_day.loc[int(breakout_idx), "date"])
            add_vline_with_label(
                fig, t_bo,
                text=f"{label} break @ {t_bo.strftime('%H:%M')}",
                dash="dash",
                opacity=0.70,
                y_paper=ann_y
            )
            ann_y = max(0.70, ann_y - 0.04)

    fig.update_layout(
        title=title,
        xaxis_rangeslider_visible=False,
        template="plotly_white",
        height=720,
        margin=dict(l=30, r=10, t=60, b=30),
        showlegend=False,
    )
    return fig


# ===================== CLI =====================

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", type=str, default="", help="e.g. INFY or NSE:INFY")
    ap.add_argument("--exchange", type=str, default="NSE", help="Default NSE if symbol doesn't include EX:SYM")
    ap.add_argument("--date", type=str, default="", help="YYYY-MM-DD")
    ap.add_argument("--no-open", action="store_true", help="Do not auto-open the HTML")
    return ap.parse_args()

def prompt_if_empty(args: argparse.Namespace) -> Tuple[str, str]:
    sym = args.symbol.strip() or input("Enter stock symbol (e.g., INFY or NSE:INFY): ").strip()
    dstr = args.date.strip() or input("Enter date (YYYY-MM-DD): ").strip()
    return sym, dstr


# ===================== MAIN =====================

def main():
    args = parse_args()
    sym_in, dstr = prompt_if_empty(args)

    target_day = datetime.strptime(dstr, "%Y-%m-%d").date()
    exchange, tradingsymbol = resolve_symbol_exchange(sym_in, args.exchange)

    log("STEP", "Initializing Kite via OptionTradeUtils.intialize_kite_api() ...")
    kite = oUtils.intialize_kite_api()
    log("INFO", "Kite initialized.")

    log("STEP", f"Resolving instrument token for {exchange}:{tradingsymbol} ...")
    token = get_instrument_token_fast(kite, exchange, tradingsymbol)
    log("INFO", f"instrument_token={token}")

    log("STEP", f"Fetching target day 1-min session candles: {target_day} ...")
    df_day = fetch_1m_session(kite, token, target_day)
    if df_day.empty:
        log("ERROR", f"No session candles found for {exchange}:{tradingsymbol} on {target_day}")
        return

    log("STEP", "Fetching previous trading day 1-min session candles (for pivots) ...")
    prev_day, df_prev = fetch_prev_trading_day_1m(kite, token, target_day)

    pivots = compute_cpr_pivots(df_prev)
    log("INFO", f"Prev trading day = {prev_day} | " +
        ", ".join([f"{k}={pivots[k]:.2f}" for k in ["P", "BC", "TC", "R1", "S1"]])
    )

    log("STEP", "Scanning full target day for W / inverted-W (M) patterns ...")
    occ = detect_all_WM(df_day)
    if not occ:
        log("WARN", "No W/M pattern found for this day.")
        return

    for i, o in enumerate(occ[:min(10, len(occ))], start=1):
        p2_t = to_py_dt(df_day.loc[int(o["p2_idx"]), "date"])
        bo = o.get("breakout_idx")
        bo_t = to_py_dt(df_day.loc[int(bo), "date"]).strftime("%H:%M") if bo is not None else "NA"
        log("INFO", f"#{i} {o['type']}  complete@{p2_t.strftime('%H:%M')}  break@{bo_t}  "
                    f"tol%={o['tol_pct']:.3f}  level={o['level']:.2f}")

    title = f"{exchange}:{tradingsymbol} — {target_day} (W/M scan) | pivots from {prev_day}"
    fig = plot_day_with_patterns(df_day, pivots, occ, title)

    out = f"kite_wm_{exchange}_{tradingsymbol}_{target_day}.html".replace(":", "_").replace(" ", "_")
    fig.write_html(out, include_plotlyjs="cdn", full_html=True)

    abs_out = os.path.abspath(out)
    log("INFO", f"Saved chart: {abs_out}")

    if not args.no_open:
        try:
            webbrowser.open(f"file:///{abs_out.replace(os.sep, '/')}")
        except Exception:
            pass


if __name__ == "__main__":
    main()


# Test cases
# 1. ASIANPAINT 2025-11-27
# 2. ASIANPAINT 2025-11-28
# 3. ASIANPAINT 2025-11-21
# 4. ASIANPAINT 2025-11-17
# 5. ASIANPAINT 2025-11-11
# 6. ASIANPAINT 2025-09-23
# 7. HDFCLIFE 2025-12-11