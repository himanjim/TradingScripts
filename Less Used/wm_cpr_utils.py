"""
wm_cpr_utils.py

Reusable utilities for:
- W / M detection on 1-min candles
- CPR computation from previous day H/L/C (with BC/TC swap safety)
- CPR band crossing detection

Expected candle DF columns:
['date','open','high','low','close','volume']
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


# =========================
# W / M detection parameters
# =========================

@dataclass(frozen=True)
class WMParams:
    smooth_roll: int = 5
    bottom_top_tol_pct: float = 0.35
    min_sep_bars: int = 6
    max_sep_bars: int = 70
    min_depth_pct: float = 0.25
    min_height_pct: float = 0.25
    min_rebound_pct: float = 0.35
    lookahead_bars_validate: int = 20


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


def _dedup_occ(occ: List[Dict], new: Dict, tol_pct: float) -> bool:
    for o in occ:
        if o["type"] != new["type"]:
            continue
        if abs(int(o["p2_idx"]) - int(new["p2_idx"])) <= 3:
            lvl = float(new["level"])
            if abs(float(o["level"]) - lvl) / max(lvl, 1e-9) * 100.0 <= tol_pct:
                return True
    return False


def detect_all_WM(df_day: pd.DataFrame, params: WMParams = WMParams()) -> List[Dict]:
    """
    Returns ALL detected W/M occurrences in df_day, sorted by (score, tie).
    score is negative quality (lower is better), tie is dist_to_level_pct.
    """
    if df_day is None or df_day.empty or len(df_day) < 60:
        return []

    d = df_day.reset_index(drop=True)

    closes = d["close"].astype(float).to_numpy()
    highs = d["high"].astype(float).to_numpy()
    lows = d["low"].astype(float).to_numpy()
    N = len(d)

    mins, _ = _swing_points_from_smooth(lows, roll=params.smooth_roll)
    _, maxs = _swing_points_from_smooth(highs, roll=params.smooth_roll)

    occ: List[Dict] = []

    # ---------- W ----------
    if len(mins) >= 2:
        for i in range(len(mins) - 1):
            for j in range(i + 1, len(mins)):
                a, b = mins[i], mins[j]
                sep = b - a
                if sep < params.min_sep_bars or sep > params.max_sep_bars:
                    continue

                b1, b2 = float(lows[a]), float(lows[b])
                if b1 <= 0 or b2 <= 0:
                    continue

                tol = abs(b2 - b1) / ((b1 + b2) / 2.0) * 100.0
                if tol > params.bottom_top_tol_pct:
                    continue

                seg = highs[a:b + 1]
                level = float(np.max(seg))
                level_idx = a + int(np.argmax(seg))

                depth1 = (level - b1) / max(level, 1e-9) * 100.0
                depth2 = (level - b2) / max(level, 1e-9) * 100.0
                if min(depth1, depth2) < params.min_depth_pct:
                    continue

                end_val = min(b + params.lookahead_bars_validate, N - 1)
                post = closes[b:end_val + 1]
                if len(post) < 3:
                    continue

                rebound_pct = (float(np.max(post)) - b2) / max(b2, 1e-9) * 100.0
                if rebound_pct < params.min_rebound_pct:
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
                if not _dedup_occ(occ, new, params.bottom_top_tol_pct):
                    occ.append(new)

    # ---------- M ----------
    if len(maxs) >= 2:
        for i in range(len(maxs) - 1):
            for j in range(i + 1, len(maxs)):
                a, b = maxs[i], maxs[j]
                sep = b - a
                if sep < params.min_sep_bars or sep > params.max_sep_bars:
                    continue

                t1, t2 = float(highs[a]), float(highs[b])
                if t1 <= 0 or t2 <= 0:
                    continue

                tol = abs(t2 - t1) / ((t1 + t2) / 2.0) * 100.0
                if tol > params.bottom_top_tol_pct:
                    continue

                seg = lows[a:b + 1]
                level = float(np.min(seg))
                level_idx = a + int(np.argmin(seg))

                height1 = (t1 - level) / max(level, 1e-9) * 100.0
                height2 = (t2 - level) / max(level, 1e-9) * 100.0
                if min(height1, height2) < params.min_height_pct:
                    continue

                end_val = min(b + params.lookahead_bars_validate, N - 1)
                post = closes[b:end_val + 1]
                if len(post) < 3:
                    continue

                drop_pct = (t2 - float(np.min(post))) / max(t2, 1e-9) * 100.0
                if drop_pct < params.min_rebound_pct:
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
                if not _dedup_occ(occ, new, params.bottom_top_tol_pct):
                    occ.append(new)

    return sorted(occ, key=lambda r: (r["score"], r["tie"]))


def pick_best_recent(
    occ: List[Dict],
    df_scan: pd.DataFrame,
    now_ref: datetime,
    max_age_min: int,
    typ: str,
) -> Optional[Dict]:
    """
    Pick the best (lowest score,tie) among occurrences whose P2 is <= max_age_min old.
    Adds: _p2_time, _age_min
    """
    if not occ:
        return None

    cand = []
    for o in occ:
        if o.get("type") != typ:
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


# =========================
# CPR
# =========================

def compute_cpr_from_prev_day_hlc(H: float, L: float, C: float) -> Dict[str, float]:
    P = (H + L + C) / 3.0
    BC = (H + L) / 2.0
    TC = 2.0 * P - BC
    R1 = 2.0 * P - L
    S1 = 2.0 * P - H

    # Ensure band order
    if BC > TC:
        BC, TC = TC, BC

    return {"P": float(P), "BC": float(BC), "TC": float(TC), "R1": float(R1), "S1": float(S1)}


def cpr_width_pct(pivots: Dict[str, float]) -> float:
    P = float(pivots["P"])
    return abs(float(pivots["TC"]) - float(pivots["BC"])) / max(abs(P), 1e-9) * 100.0


# =========================
# CPR band crossings
# =========================

def detect_cpr_band_crossings(df_1m: pd.DataFrame, pivots: Dict[str, float]) -> List[Dict]:
    """
    Band is [BC, TC]. Detect:
      bottom_to_up: close <= BC at some point, later close >= TC
      up_to_bottom: close >= TC at some point, later close <= BC
    """
    if df_1m is None or df_1m.empty:
        return []

    d = df_1m.reset_index(drop=True).copy()
    d["date"] = pd.to_datetime(d["date"])
    closes = d["close"].astype(float).to_numpy()

    band_low = min(float(pivots["BC"]), float(pivots["TC"]))
    band_high = max(float(pivots["BC"]), float(pivots["TC"]))

    events: List[Dict] = []
    last_below_idx: Optional[int] = None
    last_above_idx: Optional[int] = None

    for i in range(len(d)):
        c = float(closes[i])

        if c <= band_low:
            last_below_idx = i
        if c >= band_high:
            last_above_idx = i

        if last_below_idx is not None and i > last_below_idx and c >= band_high:
            events.append({
                "direction": "bottom_to_up",
                "cross_time": pd.to_datetime(d.loc[i, "date"]).to_pydatetime().replace(second=0, microsecond=0),
                "cross_price": c,
                "band_low": band_low,
                "band_high": band_high,
            })
            last_below_idx = None

        if last_above_idx is not None and i > last_above_idx and c <= band_low:
            events.append({
                "direction": "up_to_bottom",
                "cross_time": pd.to_datetime(d.loc[i, "date"]).to_pydatetime().replace(second=0, microsecond=0),
                "cross_price": c,
                "band_low": band_low,
                "band_high": band_high,
            })
            last_above_idx = None

    return events


def latest_recent_cpr_cross(
    df_1m: pd.DataFrame,
    pivots: Dict[str, float],
    now_ref: datetime,
    max_age_min: int,
) -> Optional[Dict]:
    ev = detect_cpr_band_crossings(df_1m, pivots)
    if not ev:
        return None
    last = ev[-1]
    age_min = int((now_ref - last["cross_time"]).total_seconds() // 60)
    if age_min < 0:
        age_min = 0
    if age_min > max_age_min:
        return None
    out = dict(last)
    out["_age_min"] = age_min
    return out
