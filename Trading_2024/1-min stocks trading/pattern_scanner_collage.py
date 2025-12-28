import os
import glob
import datetime as dt
from datetime import datetime, date, time as dtime, timedelta
from typing import List, Dict, Tuple, Optional

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import webbrowser


# ================== CONFIG ==================

PARQUET_DIR = "./stock_history_parquet"
OUTPUT_HTML = "pattern_scan_collage.html"

# Read only this many calendar days back to reliably find prev trading day for CPR pivots (Pattern B)
LOOKBACK_DAYS_FOR_PIVOT = 12

# Pattern B "flatness" threshold (total band of last 20 bars as % of mid price)
PATTERN_B_MAX_BAND_PCT = 0.25  # tighten/loosen as needed

# Time interpretation:
# "AUTO": detect UTC-like session (03:45) and shift +5:30 to IST
# "IST": assume stored in IST-naive already
# "UTC": assume stored UTC-naive; shift +5:30 to IST-naive
DATA_TIME_MODE = "AUTO"  # "AUTO" | "IST" | "UTC"
IST_OFFSET = timedelta(hours=5, minutes=30)

# NSE session times (IST)
SESSION_START = dtime(9, 15)
SESSION_END = dtime(15, 30)

# ---- Pattern C: W / inverted-W detection on LAST N candles till cutoff ----
PATTERN_C_WINDOW = 80

# ---- Pattern D: find ALL W / inverted-W occurrences in the day up to cutoff ----
PATTERN_D_LOOKAHEAD = 20  # evaluate rebound/approach within next N candles after 2nd bottom/top
MAX_ANNOTATIONS_PER_STOCK_PER_TYPE = 8  # to avoid unreadable charts

# W/M detection internal params
SMOOTH_ROLL = 5
BOTTOM_TOP_TOL_PCT = 0.35             # bottoms/tops closeness (percent)
MIN_SEP_BARS = 6
MAX_SEP_BARS = 70
MIN_DEPTH_PCT = 0.25                  # W: bottoms must be at least this % below neckline
MIN_HEIGHT_PCT = 0.25                 # M: tops must be at least this % above trough
FORMING_MAX_DIST_TO_LEVEL_PCT = 0.35  # allow "forming" if close is near neckline/trough
MIN_REBOUND_PCT = 0.35                # after 2nd bottom/top, must rebound/fall by this % (vs bottom/top)

# Plot sizes + page width (keeps charts from being too wide)
MINI_HEIGHT = 300
FULL_HEIGHT = 430
PAGE_MAX_WIDTH_PX = 980

# Collage count
TOP_N = 10


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
    Handles tz-aware objects safely (no deprecated checks).
    """
    s = s.astype("object")
    s = s.map(lambda x: x.replace(tzinfo=None) if isinstance(x, dt.datetime) and x.tzinfo else x)
    s = pd.to_datetime(s, errors="coerce")
    if isinstance(s.dtype, pd.DatetimeTZDtype):
        log("INFO", f"{ctx}: tz-aware detected ({s.dt.tz}); dropping tz.")
        s = s.dt.tz_convert(None)
    return s


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


def read_symbol_window(path: str, start_dt: datetime, end_dt: datetime, session_date_for_tz: date) -> pd.DataFrame:
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
    except Exception:
        # Fallback to full read if filters fail in your environment
        df = pd.read_parquet(path, columns=cols, engine="pyarrow")

    df = df.copy()
    df["date"] = normalize_date_series(df["date"], ctx=f"read_symbol_window {os.path.basename(path)}")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

    # constrain to requested window
    df = df[(df["date"] >= start_dt) & (df["date"] <= end_dt)]
    df = to_ist_naive(df, session_date_for_tz)
    return df


def read_symbol_day(path: str, session_date: date) -> pd.DataFrame:
    """
    Efficiently read only the session date data (using filters), then normalize to IST-naive.
    """
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
    df = df.sort_values("date").reset_index(drop=True)
    return df


# ================== CPR / PIVOT LOGIC (Pattern B) ==================
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


# ================== PATTERN A ==================

def detect_pattern_A(df_day_upto: pd.DataFrame) -> Optional[Dict]:
    """
    Pattern A (pressure building / tightness normalized by price):
      last 10 candles => smallest mean absolute % change between consecutive closes.
    """
    if len(df_day_upto) < 11:
        return None

    last = df_day_upto.tail(11).copy()  # need 10 consecutive diffs
    closes = last["close"].astype(float).values
    pct = np.abs(np.diff(closes) / np.maximum(closes[:-1], 1e-9)) * 100.0

    mean_abs_pct = float(np.mean(pct))  # lower better
    max_abs_pct = float(np.max(pct))    # tie-break: lower better

    return {
        "score": mean_abs_pct,
        "tie": max_abs_pct,
        "last_time": last["date"].iloc[-1],
        "last_close": float(closes[-1]),
    }


# ================== PATTERN B ==================

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
    True if closes whipsaw across the given level:
      below->above->below OR above->below->above within the window.
    """
    diffs = closes - level
    signs = np.sign(diffs)  # -1, 0, +1
    seq = _compress_signs(signs)
    if len(seq) < 3:
        return False
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
    nearest_dist = min(abs(last_close - levels[k]) for k in crossed)

    return {
        "score": float(band_pct),        # lower better
        "tie1": -len(crossed),           # more crossed lines better => negative
        "tie2": float(nearest_dist),     # smaller better
        "band_pct": float(band_pct),
        "crossed_lines": crossed,
        "last_time": last20["date"].iloc[-1],
        "last_close": last_close,
    }


# ================== W / M ENGINE (Pattern C and D) ==================

def _swing_points_from_smooth(x: np.ndarray, roll: int = 5) -> Tuple[List[int], List[int]]:
    """
    Swing detection on smoothed series:
      returns indices of local minima and maxima.
    """
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


def _dedup_occurrences(occ: List[Dict], new: Dict) -> bool:
    """
    Return True if 'new' is a near-duplicate of an existing occurrence.
    Duplicate heuristic: same type, p2 within 3 bars, level within tolerance band.
    """
    typ = new["type"]
    p2 = new["p2_idx"]
    level = new["level"]
    for o in occ:
        if o["type"] != typ:
            continue
        if abs(o["p2_idx"] - p2) <= 3:
            # level closeness measured as % of level
            if abs(o["level"] - level) / max(level, 1e-9) * 100.0 <= BOTTOM_TOP_TOL_PCT:
                return True
    return False


def _quality_score(depth_or_height_avg: float, tol: float, dist_to_level_pct: float) -> float:
    """
    Higher is better quality; we will store score = -quality for sorting ascending.
    """
    return depth_or_height_avg - (tol * 0.8) - (dist_to_level_pct * 1.2)


def detect_best_WM_in_last_window(df_day_upto: pd.DataFrame, window: int) -> Optional[Dict]:
    """
    Pattern C:
      look at last `window` candles up to cutoff, return the best single W or M candidate.
    """
    if len(df_day_upto) < window:
        return None

    w = df_day_upto.tail(window).reset_index(drop=True)
    closes = w["close"].astype(float).values
    highs = w["high"].astype(float).values
    lows = w["low"].astype(float).values

    mins, maxs = _swing_points_from_smooth(lows, roll=SMOOTH_ROLL)
    last_close = float(closes[-1])
    best: Optional[Dict] = None

    # --- W candidates ---
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

                seg_highs = highs[a:b + 1]
                level = float(np.max(seg_highs))
                level_idx = a + int(np.argmax(seg_highs))

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
                    "dist_to_level_pct": dist_to_level_pct,
                    "tol_pct": tol,
                    "p1_idx": a,
                    "p2_idx": b,
                    "level_idx": level_idx,
                }
                if best is None or (cand["score"], cand["tie"]) < (best["score"], best["tie"]):
                    best = cand

    # --- M candidates ---
    if len(maxs) >= 2:
        maxs2 = maxs
        for i in range(len(maxs2) - 1):
            for j in range(i + 1, len(maxs2)):
                a, b = maxs2[i], maxs2[j]
                sep = b - a
                if sep < MIN_SEP_BARS or sep > MAX_SEP_BARS:
                    continue
                t1, t2 = float(highs[a]), float(highs[b])
                if t1 <= 0 or t2 <= 0:
                    continue
                tol = abs(t2 - t1) / ((t1 + t2) / 2.0) * 100.0
                if tol > BOTTOM_TOP_TOL_PCT:
                    continue

                seg_lows = lows[a:b + 1]
                level = float(np.min(seg_lows))   # trough level
                level_idx = a + int(np.argmin(seg_lows))

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
                    "dist_to_level_pct": dist_to_level_pct,
                    "tol_pct": tol,
                    "p1_idx": a,
                    "p2_idx": b,
                    "level_idx": level_idx,
                }
                if best is None or (cand["score"], cand["tie"]) < (best["score"], best["tie"]):
                    best = cand

    return best


def detect_all_WM_in_day(df_day_upto: pd.DataFrame) -> List[Dict]:
    """
    Pattern D:
      Find ALL W and M occurrences in the day (up to cutoff).
      We detect using swing points over the whole day and validate each pair with a lookahead window.
    """
    if len(df_day_upto) < 60:
        return []

    d = df_day_upto.reset_index(drop=True)
    closes = d["close"].astype(float).values
    highs = d["high"].astype(float).values
    lows = d["low"].astype(float).values

    mins, maxs = _swing_points_from_smooth(lows, roll=SMOOTH_ROLL)
    mins2, maxs2 = mins, _swing_points_from_smooth(highs, roll=SMOOTH_ROLL)[1]

    occ: List[Dict] = []

    # --- W occurrences ---
    if len(mins2) >= 2:
        for i in range(len(mins2) - 1):
            for j in range(i + 1, len(mins2)):
                a, b = mins2[i], mins2[j]
                sep = b - a
                if sep < MIN_SEP_BARS or sep > MAX_SEP_BARS:
                    continue

                b1, b2 = float(lows[a]), float(lows[b])
                if b1 <= 0 or b2 <= 0:
                    continue
                tol = abs(b2 - b1) / ((b1 + b2) / 2.0) * 100.0
                if tol > BOTTOM_TOP_TOL_PCT:
                    continue

                seg_highs = highs[a:b + 1]
                level = float(np.max(seg_highs))  # neckline
                level_idx = a + int(np.argmax(seg_highs))

                depth1 = (level - b1) / level * 100.0
                depth2 = (level - b2) / level * 100.0
                if min(depth1, depth2) < MIN_DEPTH_PCT:
                    continue

                end = min(b + PATTERN_D_LOOKAHEAD, len(closes) - 1)
                post = closes[b:end + 1]
                if len(post) < 3:
                    continue

                post_max = float(np.max(post))
                rebound_pct = (post_max - b2) / max(b2, 1e-9) * 100.0
                if rebound_pct < MIN_REBOUND_PCT:
                    continue

                close_eval = float(closes[end])
                dist_to_level_pct = abs(close_eval - level) / level * 100.0
                forming_ok = (post_max >= level) or (dist_to_level_pct <= FORMING_MAX_DIST_TO_LEVEL_PCT)
                if not forming_ok:
                    continue

                q = _quality_score((depth1 + depth2) / 2.0, tol, dist_to_level_pct)
                new = {
                    "type": "W",
                    "score": -q,
                    "tie": dist_to_level_pct,
                    "level": level,
                    "dist_to_level_pct": dist_to_level_pct,
                    "tol_pct": tol,
                    "p1_idx": a,
                    "p2_idx": b,
                    "level_idx": level_idx,
                    "eval_end_idx": end,
                }
                if not _dedup_occurrences(occ, new):
                    occ.append(new)

    # --- M occurrences ---
    if len(maxs2) >= 2:
        for i in range(len(maxs2) - 1):
            for j in range(i + 1, len(maxs2)):
                a, b = maxs2[i], maxs2[j]
                sep = b - a
                if sep < MIN_SEP_BARS or sep > MAX_SEP_BARS:
                    continue

                t1, t2 = float(highs[a]), float(highs[b])
                if t1 <= 0 or t2 <= 0:
                    continue
                tol = abs(t2 - t1) / ((t1 + t2) / 2.0) * 100.0
                if tol > BOTTOM_TOP_TOL_PCT:
                    continue

                seg_lows = lows[a:b + 1]
                level = float(np.min(seg_lows))  # trough
                level_idx = a + int(np.argmin(seg_lows))

                height1 = (t1 - level) / max(level, 1e-9) * 100.0
                height2 = (t2 - level) / max(level, 1e-9) * 100.0
                if min(height1, height2) < MIN_HEIGHT_PCT:
                    continue

                end = min(b + PATTERN_D_LOOKAHEAD, len(closes) - 1)
                post = closes[b:end + 1]
                if len(post) < 3:
                    continue

                post_min = float(np.min(post))
                drop_pct = (t2 - post_min) / max(t2, 1e-9) * 100.0
                if drop_pct < MIN_REBOUND_PCT:
                    continue

                close_eval = float(closes[end])
                dist_to_level_pct = abs(close_eval - level) / max(level, 1e-9) * 100.0
                forming_ok = (post_min <= level) or (dist_to_level_pct <= FORMING_MAX_DIST_TO_LEVEL_PCT)
                if not forming_ok:
                    continue

                q = _quality_score((height1 + height2) / 2.0, tol, dist_to_level_pct)
                new = {
                    "type": "M",
                    "score": -q,
                    "tie": dist_to_level_pct,
                    "level": level,
                    "dist_to_level_pct": dist_to_level_pct,
                    "tol_pct": tol,
                    "p1_idx": a,
                    "p2_idx": b,
                    "level_idx": level_idx,
                    "eval_end_idx": end,
                }
                if not _dedup_occurrences(occ, new):
                    occ.append(new)

    # Sort by best quality first
    occ = sorted(occ, key=lambda r: (r["score"], r["tie"]))
    return occ


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
            opacity=0.75,
            annotation_text=f"{k}: {y:.2f}",
            annotation_position="top left",
        )


def _add_WM_markers(fig: go.Figure, df_plot: pd.DataFrame, occ: List[Dict]):
    """
    Highlight all W/M occurrences on the full-day chart (limited to prevent clutter).
    Each occurrence draws:
      - a horizontal level line between p1 and eval_end
      - point markers at p1, p2, and level_idx
    """
    if not occ:
        return

    df_plot = df_plot.reset_index(drop=True)
    n = len(df_plot)

    # Limit annotations per type
    w_list = [o for o in occ if o["type"] == "W"][:MAX_ANNOTATIONS_PER_STOCK_PER_TYPE]
    m_list = [o for o in occ if o["type"] == "M"][:MAX_ANNOTATIONS_PER_STOCK_PER_TYPE]
    occ2 = w_list + m_list

    for o in occ2:
        typ = o["type"]
        p1 = int(o["p1_idx"]); p2 = int(o["p2_idx"]); li = int(o["level_idx"])
        end = int(o.get("eval_end_idx", p2))
        p1 = max(0, min(n - 1, p1))
        p2 = max(0, min(n - 1, p2))
        li = max(0, min(n - 1, li))
        end = max(0, min(n - 1, end))

        level = float(o["level"])

        # Draw limited-span horizontal line
        fig.add_shape(
            type="line",
            x0=df_plot.loc[p1, "date"],
            x1=df_plot.loc[end, "date"],
            y0=level,
            y1=level,
            line=dict(width=1, dash="dot"),
            opacity=0.7,
        )

        if typ == "W":
            y1 = float(df_plot.loc[p1, "low"])
            y2 = float(df_plot.loc[p2, "low"])
            yL = float(df_plot.loc[li, "high"])
            t1, t2, tL = "B1", "B2", "Neck"
        else:
            y1 = float(df_plot.loc[p1, "high"])
            y2 = float(df_plot.loc[p2, "high"])
            yL = float(df_plot.loc[li, "low"])
            t1, t2, tL = "T1", "T2", "Tr"

        fig.add_trace(go.Scatter(
            x=[df_plot.loc[p1, "date"], df_plot.loc[p2, "date"]],
            y=[y1, y2],
            mode="markers+text",
            text=[t1, t2],
            textposition="top center",
            marker=dict(size=9, symbol="circle"),
            hoverinfo="skip",
            showlegend=False
        ))
        fig.add_trace(go.Scatter(
            x=[df_plot.loc[li, "date"]],
            y=[yL],
            mode="markers+text",
            text=[tL],
            textposition="top center",
            marker=dict(size=9, symbol="diamond"),
            hoverinfo="skip",
            showlegend=False
        ))


def make_candle_fig(
    df_plot: pd.DataFrame,
    title: str,
    pivots: Optional[Dict[str, float]],
    height: int,
    wm_occ: Optional[List[Dict]] = None
) -> go.Figure:
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
    if wm_occ:
        _add_WM_markers(fig, df_plot, wm_occ)

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
    fig.update_xaxes(automargin=True)
    fig.update_yaxes(automargin=True)
    return fig


def fig_to_html_div(fig: go.Figure, static: bool) -> str:
    config = {"displayModeBar": not static, "staticPlot": static, "scrollZoom": not static, "responsive": True}
    return fig.to_html(full_html=False, include_plotlyjs=False, config=config)


def pattern_hint_text() -> str:
    return (
        "<ul>"
        "<li><b>A</b>: “Pressure building” — last 10 candles have smallest mean absolute % change between consecutive closes.</li>"
        "<li><b>B</b>: “CPR whipsaw + flat band” — last 20 candles are tight and price whipsaws across any of CPR lines (P, BC, TC, R1, S1).</li>"
        f"<li><b>C</b>: “W / inverted-W forming (last {PATTERN_C_WINDOW})” — best W or M found only in the last window till cutoff.</li>"
        "<li><b>D</b>: “All W / inverted-W in the day” — finds and highlights all W and M occurrences in that day (up to cutoff) and ranks stocks by best quality.</li>"
        "</ul>"
    )


def build_collage_html(top_results: List[Dict], session_date: date, cutoff_dt: datetime, out_path: str, pattern_type: str):
    if not top_results:
        log("WARN", "No results to plot.")
        return

    mini_blocks = []
    full_blocks = []

    for rank, r in enumerate(top_results, start=1):
        ex = r["exchange"]
        ts = r["symbol"]
        path = r["path"]
        anchor = f"{pattern_type}_{ex}_{ts}_full".replace(":", "_").replace(" ", "_")

        # Read required data for plotting
        if pattern_type == "B":
            win_start = datetime.combine(session_date - timedelta(days=LOOKBACK_DAYS_FOR_PIVOT), dtime(0, 0))
            win_end = datetime.combine(session_date + timedelta(days=1), dtime(0, 0))
            df_win = read_symbol_window(path, win_start, win_end, session_date)
            piv = compute_pivots_from_prev_day(df_win, session_date)
            df_day = df_win[df_win["date"].dt.date == session_date].copy().sort_values("date")
        else:
            df_day = read_symbol_day(path, session_date)
            piv = None

        if df_day.empty:
            continue

        df_upto = df_day[df_day["date"] <= cutoff_dt].copy()
        if df_upto.empty:
            continue

        wm_occ = None
        extra = ""
        if pattern_type == "B" and "crossed_lines" in r:
            extra = f"<div class='meta'><b>Crossed CPR:</b> {', '.join(r['crossed_lines'])} | <b>Band:</b> {r['band_pct']:.3f}%</div>"

        if pattern_type in ("C", "D"):
            wm_occ = r.get("wm_occ")
            if pattern_type == "D":
                w_cnt = sum(1 for o in (wm_occ or []) if o["type"] == "W")
                m_cnt = sum(1 for o in (wm_occ or []) if o["type"] == "M")
                extra = f"<div class='meta'><b>W count:</b> {w_cnt} &nbsp; <b>M count:</b> {m_cnt}</div>"

        # Mini plot: keep compact
        if pattern_type in ("C", "D"):
            mini_df = df_upto.tail(PATTERN_C_WINDOW).copy() if len(df_upto) >= PATTERN_C_WINDOW else df_upto.copy()
        else:
            mini_df = df_upto.copy()

        mini_title = f"{rank}. {ex}:{ts} — till {cutoff_dt.strftime('%H:%M')}"
        full_title = f"{rank}. {ex}:{ts} — full day {session_date}"

        fig_mini = make_candle_fig(mini_df, mini_title, piv, MINI_HEIGHT, wm_occ=None if pattern_type != "D" else wm_occ)
        fig_full = make_candle_fig(df_day, full_title, piv, FULL_HEIGHT, wm_occ=wm_occ)

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
<title>Pattern Scan — {session_date}</title>
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
<h1>Pattern Scan</h1>
<p>
<b>Date:</b> {session_date} &nbsp;&nbsp;
<b>Cutoff:</b> {cutoff_dt.strftime('%H:%M')} &nbsp;&nbsp;
<b>Pattern:</b> {pattern_type} &nbsp;&nbsp;
<b>Pattern B band:</b> {PATTERN_B_MAX_BAND_PCT:.2f}%
</p>

<h3>What each option means</h3>
{pattern_hint_text()}

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
    try:
        webbrowser.open(f"file:///{abs_path.replace(os.sep, '/')}")
    except Exception:
        pass


# ================== MAIN ==================

def main():
    date_str = input("Enter analysis date (YYYY-MM-DD): ").strip()
    time_str = input("Enter cutoff time (HH:MM, IST) [default 15:30]: ").strip() or "15:30"
    pattern_type = input("Enter pattern type (A / B / C / D): ").strip().upper()

    if pattern_type not in ("A", "B", "C", "D"):
        log("ERROR", "Pattern type must be A, B, C or D.")
        return

    session_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    cutoff_time = datetime.strptime(time_str, "%H:%M").time()
    cutoff_dt = datetime.combine(session_date, cutoff_time)

    files = list_parquet_files()
    log("STEP", f"Found {len(files)} Parquet files.")
    log("STEP", f"Scanning pattern {pattern_type} for {session_date} till {cutoff_dt.strftime('%H:%M')}")

    # For B, precompute window bounds once
    win_start_B = datetime.combine(session_date - timedelta(days=LOOKBACK_DAYS_FOR_PIVOT), dtime(0, 0))
    win_end_B = datetime.combine(session_date + timedelta(days=1), dtime(0, 0))

    results: List[Dict] = []
    scanned = 0

    for path in files:
        ex, ts = parse_exchange_symbol_from_filename(path)
        tag = f"{ex}:{ts}"

        try:
            if pattern_type == "B":
                df_win = read_symbol_window(path, win_start_B, win_end_B, session_date)
                df_day = df_win[df_win["date"].dt.date == session_date].copy()
                if df_day.empty:
                    continue
                df_day = df_day.sort_values("date")
                df_upto = df_day[df_day["date"] <= cutoff_dt].copy()
                if df_upto.empty:
                    continue

                piv = compute_pivots_from_prev_day(df_win, session_date)
                if not piv:
                    continue
                pr = detect_pattern_B(df_upto, piv)
                if pr:
                    pr.update({"exchange": ex, "symbol": ts, "path": path})
                    results.append(pr)

            else:
                df_day = read_symbol_day(path, session_date)
                if df_day.empty:
                    continue
                df_upto = df_day[df_day["date"] <= cutoff_dt].copy()
                if df_upto.empty:
                    continue

                if pattern_type == "A":
                    pr = detect_pattern_A(df_upto)
                    if pr:
                        pr.update({"exchange": ex, "symbol": ts, "path": path})
                        results.append(pr)

                elif pattern_type == "C":
                    pr = detect_best_WM_in_last_window(df_upto, PATTERN_C_WINDOW)
                    if pr:
                        pr.update({"exchange": ex, "symbol": ts, "path": path})
                        # store for plotting (full-day uses only one best, so keep occ list as [best])
                        pr["wm_occ"] = [pr]
                        results.append(pr)

                elif pattern_type == "D":
                    occ = detect_all_WM_in_day(df_upto)
                    if not occ:
                        continue
                    best = occ[0]
                    # rank by best quality, then by more patterns
                    pr = {
                        "exchange": ex,
                        "symbol": ts,
                        "path": path,
                        "score": float(best["score"]),
                        "tie": float(best["tie"]),
                        "wm_occ": occ,  # all occurrences for full-day chart
                        "w_count": sum(1 for o in occ if o["type"] == "W"),
                        "m_count": sum(1 for o in occ if o["type"] == "M"),
                    }
                    results.append(pr)

        except Exception as e:
            log("ERROR", f"{tag}: failed: {e}")

        scanned += 1
        if scanned % 50 == 0:
            log("STEP", f"Scanned {scanned}/{len(files)}; matches so far: {len(results)}")

    if not results:
        log("WARN", "No matches found.")
        return

    # Sort results per pattern
    if pattern_type == "A":
        results_sorted = sorted(results, key=lambda r: (r["score"], r["tie"]))
    elif pattern_type == "B":
        results_sorted = sorted(results, key=lambda r: (r["score"], r["tie1"], r["tie2"]))
    elif pattern_type == "C":
        results_sorted = sorted(results, key=lambda r: (r["score"], r["tie"]))
    else:
        # D: best score first, then more total patterns
        results_sorted = sorted(results, key=lambda r: (r["score"], -(r["w_count"] + r["m_count"]), r["tie"]))

    top = results_sorted[:TOP_N]

    log("INFO", f"Matches total: {len(results)} | Top selected: {len(top)}")
    for i, r in enumerate(top, start=1):
        if pattern_type == "A":
            log("INFO", f"#{i} {r['exchange']}:{r['symbol']} meanAbs%={r['score']:.5f} maxAbs%={r['tie']:.5f}")
        elif pattern_type == "B":
            log("INFO", f"#{i} {r['exchange']}:{r['symbol']} band%={r['score']:.4f} crossed={r['crossed_lines']}")
        elif pattern_type == "C":
            log("INFO", f"#{i} {r['exchange']}:{r['symbol']} {r['type']} dist%={r['dist_to_level_pct']:.3f} tol%={r['tol_pct']:.3f}")
        else:
            log("INFO", f"#{i} {r['exchange']}:{r['symbol']} bestScore={r['score']:.4f} W={r['w_count']} M={r['m_count']}")

    build_collage_html(top, session_date, cutoff_dt, OUTPUT_HTML, pattern_type)


if __name__ == "__main__":
    main()
