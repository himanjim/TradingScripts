"""
scan_wm_live.py (OPTIMIZED + FIXED)

Fixes/Optimizations:
- Builds df_plot only for FINAL candidates (big speedup).
- Handles DB lag: uses per-symbol reference time (end_ts) when lag > 2 min.
- Continuous window prefix NaNs fixed (no more silent <60 bars).
- CPR pivots are fetched ON-DEMAND only (uses cache file, avoids filling all 500 every run).
"""

from __future__ import annotations

import os
import json
import time
import sqlite3
from datetime import datetime, date, time as dtime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
import webbrowser

import Trading_2024.OptionTradeUtils as oUtils

from wm_cpr_utils import (
    WMParams,
    detect_all_WM,
    pick_best_recent,
    compute_cpr_from_prev_day_hlc,
    cpr_width_pct,
    latest_recent_cpr_cross,
)

# ================= CONFIG =================

CACHE_ROOT = "./live_cache"
OUTPUT_HTML = "wm_live_top3.html"

SCAN_BARS = 80
DISPLAY_BARS = 240

TOP_PER_TYPE = 3
MAX_PATTERN_AGE_MIN = 15

CPR_CROSS_LOOKBACK_BARS = 30
MAX_CPR_CROSS_AGE_MIN = 15
TOP_CPR = 3

DATA_LAG_REF_SWITCH_MIN = 2  # if DB lag > this, use end_ts as ref time for freshness checks

PLOTLY_JS_MODE = "inline"

INSTRUMENTS_CACHE_DIR = "./kite_instruments_cache"
INSTRUMENTS_CACHE_TTL_DAYS = 7

CPR_PIVOTS_JSON = "cpr_pivots.json"
PIVOT_SLEEP_SEC = 0.45
PIVOT_SAVE_EVERY = 10

WM_PARAMS = WMParams(
    smooth_roll=5,
    bottom_top_tol_pct=0.35,
    min_sep_bars=6,
    max_sep_bars=70,
    min_depth_pct=0.25,
    min_height_pct=0.25,
    min_rebound_pct=0.35,
    lookahead_bars_validate=20,
)

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

def cpr_pivots_path(d: date) -> str:
    return os.path.join(day_dir(d), CPR_PIVOTS_JSON)

def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

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
    df = df.dropna(subset=["date"])
    for c in ["open", "high", "low", "close", "volume"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["open", "high", "low", "close"])
    df = df.drop_duplicates(subset=["date"], keep="last")
    df = df.sort_values("date").reset_index(drop=True)
    df["volume"] = df["volume"].fillna(0).astype(int)
    return df

def make_continuous_minute_window(
    conn: sqlite3.Connection,
    symbol: str,
    end_ts: datetime,
    minutes: int,
) -> pd.DataFrame:
    """
    Continuous 1-min grid ending at end_ts inclusive.
    Fix: handles prefix NaNs (seed from last_before, else seed from first valid close in window).
    """
    end_ts = end_ts.replace(second=0, microsecond=0)
    start_ts = (end_ts - timedelta(minutes=minutes - 1)).replace(second=0, microsecond=0)

    raw = db_range(conn, symbol, start_ts, end_ts)
    if raw.empty:
        return raw

    raw = sanitize_candles(raw)
    if raw.empty:
        return raw

    grid = pd.date_range(start=start_ts, end=end_ts, freq="1min")
    df = raw.set_index("date").reindex(grid)

    # Seed first close
    if pd.isna(df["close"].iloc[0]):
        seed = db_last_before(conn, symbol, start_ts)
        if seed is not None:
            df.iloc[0, df.columns.get_loc("close")] = float(seed[1])
        else:
            # seed from first available close inside the window (prefix only)
            first_valid = df["close"].dropna()
            if not first_valid.empty:
                df.iloc[0, df.columns.get_loc("close")] = float(first_valid.iloc[0])

    df["close"] = df["close"].ffill()

    # Fill OHLC with close where missing
    for col in ["open", "high", "low"]:
        df[col] = df[col].where(df[col].notna(), df["close"])

    df["volume"] = df["volume"].fillna(0).astype(int)

    df = df.reset_index().rename(columns={"index": "date"})
    return df

# ================= Day Open =================

def load_day_open_map(d: date) -> Dict[str, float]:
    p = day_open_path(d)
    if not os.path.isfile(p):
        return {}
    with open(p, "r", encoding="utf-8") as f:
        data = json.load(f) or {}
    return {str(k): float(v) for k, v in data.items()}

# ================= Instruments (token map) =================

def _instruments_cache_file(exchange: str) -> str:
    return os.path.join(INSTRUMENTS_CACHE_DIR, f"instruments_{exchange.upper()}.parquet")

def _cache_is_fresh(path: str, ttl_days: int) -> bool:
    if not os.path.isfile(path):
        return False
    mtime = datetime.fromtimestamp(os.path.getmtime(path))
    return (datetime.now() - mtime).days <= ttl_days

def load_instruments_df(kite, exchange: str) -> pd.DataFrame:
    ensure_dir(INSTRUMENTS_CACHE_DIR)
    cache = _instruments_cache_file(exchange)

    if _cache_is_fresh(cache, INSTRUMENTS_CACHE_TTL_DAYS):
        return pd.read_parquet(cache, engine="pyarrow")

    log("STEP", f"Downloading instruments dump for {exchange} (cached {INSTRUMENTS_CACHE_TTL_DAYS} days)...")
    inst = kite.instruments(exchange)
    df = pd.DataFrame(inst)
    keep = ["exchange", "tradingsymbol", "instrument_token"]
    df = df[[c for c in keep if c in df.columns]].copy()
    df.to_parquet(cache, engine="pyarrow", index=False)
    return df

def build_token_map(kite, sym_keys: List[str]) -> Dict[str, int]:
    by_ex: Dict[str, List[str]] = {}
    for s in sym_keys:
        if ":" not in s:
            continue
        ex, ts = s.split(":", 1)
        by_ex.setdefault(ex.upper(), []).append(ts.strip())

    out: Dict[str, int] = {}
    for ex, tss in by_ex.items():
        df = load_instruments_df(kite, ex)
        df["exchange"] = df["exchange"].astype(str).str.upper()
        df["tradingsymbol"] = df["tradingsymbol"].astype(str).str.strip()

        sub = df[df["tradingsymbol"].isin(set(tss))]
        for r in sub.itertuples(index=False):
            key = f"{r.exchange}:{r.tradingsymbol}"
            out[key] = int(r.instrument_token)

    miss = [s for s in sym_keys if s not in out]
    if miss:
        log("WARN", f"Missing instrument_token for {len(miss)} symbols (first 10): {miss[:10]}")
    return out

# ================= CPR pivots cache (on-demand) =================

def load_cpr_cache(d: date) -> Dict:
    p = cpr_pivots_path(d)
    if os.path.isfile(p):
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    return {"day": d.isoformat(), "pivots": {}}

def save_cpr_cache(d: date, cache: Dict) -> None:
    ensure_dir(day_dir(d))
    with open(cpr_pivots_path(d), "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2)

def prev_day_hlc(kite, token: int, target_day: date) -> Tuple[date, float, float, float]:
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

class CPRPivotProvider:
    """
    On-demand CPR pivot fetcher with JSON cache.
    Avoids filling all symbols upfront.
    """
    def __init__(self, kite, d: date, token_map: Dict[str, int]):
        self.kite = kite
        self.d = d
        self.token_map = token_map
        self.cache = load_cpr_cache(d)
        self.piv = self.cache.setdefault("pivots", {})
        self._fills_since_save = 0

    def get(self, sym: str) -> Optional[Dict[str, float]]:
        rec = self.piv.get(sym)
        if rec:
            return {k: float(rec[k]) for k in ["P", "BC", "TC", "R1", "S1"] if k in rec}
        return None

    def get_width_pct(self, sym: str) -> Optional[float]:
        rec = self.piv.get(sym)
        if not rec:
            return None
        w = rec.get("width_pct")
        return float(w) if w is not None else None

    def ensure(self, sym: str) -> Optional[Dict[str, float]]:
        """
        Ensure pivots are available for sym, fetch via Kite if needed.
        """
        got = self.get(sym)
        if got:
            return got

        tok = self.token_map.get(sym)
        if tok is None:
            return None

        try:
            pday, H, L, C = prev_day_hlc(self.kite, tok, self.d)
            pivots = compute_cpr_from_prev_day_hlc(H, L, C)
            self.piv[sym] = {
                "prev_day": pday.isoformat(),
                **{k: float(v) for k, v in pivots.items()},
                "width_pct": float(cpr_width_pct(pivots)),
            }
            self._fills_since_save += 1
            if self._fills_since_save >= PIVOT_SAVE_EVERY:
                save_cpr_cache(self.d, self.cache)
                self._fills_since_save = 0
            time.sleep(PIVOT_SLEEP_SEC)
            return pivots
        except Exception:
            return None

    def flush(self) -> None:
        save_cpr_cache(self.d, self.cache)

# ================= Plot helpers =================

def add_hline(fig: go.Figure, y: float, text: str):
    fig.add_shape(type="line", xref="paper", x0=0, x1=1, yref="y", y0=y, y1=y,
                  line=dict(width=1, dash="dot"), opacity=0.75)
    fig.add_annotation(x=0, xref="paper", y=y, yref="y", text=text, showarrow=False,
                       xanchor="left", bgcolor="rgba(255,255,255,0.65)", font=dict(size=10))

def add_vline(fig: go.Figure, x_dt: datetime, text: str, dash: str = "solid", opacity: float = 0.5):
    fig.add_shape(type="line", xref="x", x0=x_dt, x1=x_dt, yref="paper", y0=0, y1=1,
                  line=dict(width=1, dash=dash), opacity=opacity)
    fig.add_annotation(x=x_dt, xref="x", y=0.98, yref="paper", text=text, showarrow=False,
                       xanchor="left", bgcolor="rgba(255,255,255,0.65)", font=dict(size=10))

def plot_symbol(df_plot: pd.DataFrame, pivots: Optional[Dict[str, float]], wm_best: Optional[Dict],
                cpr_event: Optional[Dict], title: str) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Candlestick(
        x=df_plot["date"], open=df_plot["open"], high=df_plot["high"],
        low=df_plot["low"], close=df_plot["close"], name=""
    ))

    if pivots:
        for k in ["R1", "BC", "P", "TC", "S1"]:
            if k in pivots:
                add_hline(fig, float(pivots[k]), f"{k}: {pivots[k]:.2f}")

    if wm_best:
        p1_t, p2_t, li_t = wm_best["p1_time"], wm_best["p2_time"], wm_best["level_time"]
        level = float(wm_best["level"])
        fig.add_shape(type="line", xref="x", x0=p1_t, x1=p2_t, yref="y", y0=level, y1=level,
                      line=dict(width=1, dash="dot"), opacity=0.8)

        fig.add_trace(go.Scatter(
            x=[p1_t, p2_t], y=[wm_best["p1_y"], wm_best["p2_y"]],
            mode="markers+text", text=[wm_best["p1_label"], wm_best["p2_label"]],
            textposition="top center", showlegend=False
        ))
        fig.add_trace(go.Scatter(
            x=[li_t], y=[wm_best["level_y"]],
            mode="markers+text", text=[wm_best["level_label"]],
            textposition="top center", showlegend=False
        ))

        add_vline(fig, p2_t, f"{wm_best['type']} complete {p2_t.strftime('%H:%M')}", dash="solid", opacity=0.45)
        if wm_best.get("breakout_time"):
            add_vline(fig, wm_best["breakout_time"], f"break {wm_best['breakout_time'].strftime('%H:%M')}",
                      dash="dash", opacity=0.75)

    if cpr_event:
        ct = cpr_event["cross_time"]
        add_vline(fig, ct, f"CPR {cpr_event['direction']} {ct.strftime('%H:%M')}", dash="dash", opacity=0.85)

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

def main():
    d = today_ist()
    conn = open_db_ro(db_path(d))
    syms = db_symbols(conn)
    if not syms:
        log("ERROR", "No symbols in DB yet. Start live_market_cache.py and wait.")
        return

    day_open_map = load_day_open_map(d)
    now_global = datetime.now().replace(second=0, microsecond=0)

    log("STEP", "Initializing Kite API...")
    kite = oUtils.intialize_kite_api()
    log("INFO", "Kite initialized.")

    token_map = build_token_map(kite, syms)
    pivot_provider = CPRPivotProvider(kite, d, token_map)

    candidates_W, candidates_M, candidates_CPR = [], [], []

    # Debug counters
    cnt_scan_ready = cnt_any_occ = 0
    cnt_w_recent = cnt_m_recent = 0
    cnt_w_rule = cnt_m_rule = 0
    cnt_cpr_recent = 0

    for sym in syms:
        last = db_last_ts_close(conn, sym)
        if not last:
            continue
        end_ts, ltp = last
        end_ts = end_ts.replace(second=0, microsecond=0)

        data_lag_min = int((now_global - end_ts).total_seconds() // 60)
        if data_lag_min < 0:
            data_lag_min = 0

        # Per-symbol ref time to avoid freshness filters killing everything when DB lags
        now_ref = end_ts if data_lag_min > DATA_LAG_REF_SWITCH_MIN else now_global

        day_open = day_open_map.get(sym)
        if day_open is None:
            fo = db_first_open(conn, sym)
            if not fo:
                continue
            day_open = fo[1]

        df_scan = make_continuous_minute_window(conn, sym, end_ts=end_ts, minutes=SCAN_BARS)
        df_scan = sanitize_candles(df_scan)
        if len(df_scan) < 60:
            continue
        cnt_scan_ready += 1

        occ = detect_all_WM(df_scan, params=WM_PARAMS)
        if occ:
            cnt_any_occ += 1

        bestW = pick_best_recent(occ, df_scan, now_ref, MAX_PATTERN_AGE_MIN, "W") if occ else None
        bestM = pick_best_recent(occ, df_scan, now_ref, MAX_PATTERN_AGE_MIN, "M") if occ else None

        def build_wm_rec(best: Dict, typ: str) -> Dict:
            p1 = max(0, min(len(df_scan) - 1, int(best["p1_idx"])))
            p2 = max(0, min(len(df_scan) - 1, int(best["p2_idx"])))
            li = max(0, min(len(df_scan) - 1, int(best["level_idx"])))

            p2_time = best["_p2_time"]
            age_min = int(best["_age_min"])
            p1_time = pd.to_datetime(df_scan.loc[p1, "date"]).to_pydatetime().replace(second=0, microsecond=0)
            li_time = pd.to_datetime(df_scan.loc[li, "date"]).to_pydatetime().replace(second=0, microsecond=0)

            if typ == "W":
                sl = min(float(df_scan.loc[p1, "low"]), float(df_scan.loc[p2, "low"]))
            else:
                sl = max(float(df_scan.loc[p1, "high"]), float(df_scan.loc[p2, "high"]))

            diff_pct = abs(day_open - ltp) / max(day_open, 1e-9) * 100.0

            best2 = dict(best)
            best2["p1_time"], best2["p2_time"], best2["level_time"] = p1_time, p2_time, li_time

            if typ == "W":
                best2.update({
                    "p1_y": float(df_scan.loc[p1, "low"]),
                    "p2_y": float(df_scan.loc[p2, "low"]),
                    "level_y": float(df_scan.loc[li, "high"]),
                    "p1_label": "B1", "p2_label": "B2", "level_label": "Neck",
                })
            else:
                best2.update({
                    "p1_y": float(df_scan.loc[p1, "high"]),
                    "p2_y": float(df_scan.loc[p2, "high"]),
                    "level_y": float(df_scan.loc[li, "low"]),
                    "p1_label": "T1", "p2_label": "T2", "level_label": "Tr",
                })

            bo = best.get("breakout_idx")
            if bo is not None:
                bo = max(0, min(len(df_scan) - 1, int(bo)))
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
                "stoploss": float(sl),
                "best": best2,
                "end_ts": end_ts,
                "data_lag_min": data_lag_min,
            }

        # Apply Open/LTP direction rule
        if bestW is not None:
            cnt_w_recent += 1
            if float(day_open) > float(ltp):
                cnt_w_rule += 1
                candidates_W.append(build_wm_rec(bestW, "W"))

        if bestM is not None:
            cnt_m_recent += 1
            if float(day_open) < float(ltp):
                cnt_m_rule += 1
                candidates_M.append(build_wm_rec(bestM, "M"))

        # CPR cross detection (needs pivots). Fetch pivots on-demand.
        piv = pivot_provider.ensure(sym)
        if piv:
            df_cpr = df_scan.tail(CPR_CROSS_LOOKBACK_BARS)
            ev = latest_recent_cpr_cross(df_cpr, piv, now_ref=now_ref, max_age_min=MAX_CPR_CROSS_AGE_MIN)
            if ev:
                cnt_cpr_recent += 1
                width = pivot_provider.get_width_pct(sym)
                if width is None:
                    width = float(cpr_width_pct(piv))

                sl = float(ev["band_low"]) if ev["direction"] == "bottom_to_up" else float(ev["band_high"])

                candidates_CPR.append({
                    "symbol": sym,
                    "cross_time": ev["cross_time"],
                    "age_min": int(ev["_age_min"]),
                    "direction": ev["direction"],
                    "width_pct": float(width),
                    "stoploss": float(sl),
                    "end_ts": end_ts,
                    "data_lag_min": data_lag_min,
                })

    pivot_provider.flush()

    log("INFO", f"Scan summary: scan_ready={cnt_scan_ready} any_occ={cnt_any_occ} "
                f"W_recent={cnt_w_recent} W_passRule={cnt_w_rule} "
                f"M_recent={cnt_m_recent} M_passRule={cnt_m_rule} "
                f"CPR_recent={cnt_cpr_recent}")

    # Rank
    topW = sorted(candidates_W, key=lambda r: (r["diff_pct"], -r["age_min"]), reverse=True)[:TOP_PER_TYPE]
    topM = sorted(candidates_M, key=lambda r: (r["diff_pct"], -r["age_min"]), reverse=True)[:TOP_PER_TYPE]
    topCPR = sorted(candidates_CPR, key=lambda r: (r["width_pct"], r["age_min"]))[:TOP_CPR]

    # Print
    print("\n========== TOP W ==========")
    if topW:
        for i, r in enumerate(topW, 1):
            print(f"{i}) {r['symbol']} | formed={r['formed_time'].strftime('%H:%M')} (age={r['age_min']}m) | "
                  f"Open={r['open']:.2f} LTP={r['ltp']:.2f} Δ%={r['diff_pct']:.2f} | SL={r['stoploss']:.2f} | lag={r['data_lag_min']}m")
    else:
        print("(none)")

    print("\n========== TOP M ==========")
    if topM:
        for i, r in enumerate(topM, 1):
            print(f"{i}) {r['symbol']} | formed={r['formed_time'].strftime('%H:%M')} (age={r['age_min']}m) | "
                  f"Open={r['open']:.2f} LTP={r['ltp']:.2f} Δ%={r['diff_pct']:.2f} | SL={r['stoploss']:.2f} | lag={r['data_lag_min']}m")
    else:
        print("(none)")

    print("\n========== TOP CPR CROSSES (thin CPR) ==========")
    if topCPR:
        for i, r in enumerate(topCPR, 1):
            print(f"{i}) {r['symbol']} | cross={r['cross_time'].strftime('%H:%M')} (age={r['age_min']}m) | "
                  f"dir={r['direction']} | CPR_width%={r['width_pct']:.3f} | SL={r['stoploss']:.2f} | lag={r['data_lag_min']}m")
    else:
        print("(none)")

    # Build plots only for final candidates (BIG speedup)
    finals = []
    finals.extend([("WM", r) for r in topW])
    finals.extend([("WM", r) for r in topM])
    finals.extend([("CPR", r) for r in topCPR])

    if not finals:
        return

    # Load pivots from cache for plotting
    piv_cache = load_cpr_cache(d).get("pivots", {})

    def get_piv(sym: str) -> Optional[Dict[str, float]]:
        rec = piv_cache.get(sym)
        if not rec:
            return None
        return {k: float(rec[k]) for k in ["P", "BC", "TC", "R1", "S1"] if k in rec}

    blocks = []
    for idx, (kind, rec) in enumerate(finals, start=1):
        sym = rec["symbol"]
        end_ts = rec["end_ts"]
        df_plot = make_continuous_minute_window(conn, sym, end_ts=end_ts, minutes=DISPLAY_BARS)
        df_plot = sanitize_candles(df_plot)
        piv = get_piv(sym)

        if kind == "WM":
            title = (f"{sym} | {rec['type']} | formed {rec['formed_time'].strftime('%H:%M')} (age {rec['age_min']}m) | "
                     f"Open={rec['open']:.2f} LTP={rec['ltp']:.2f} Δ%={rec['diff_pct']:.2f} | SL={rec['stoploss']:.2f}")
            fig = plot_symbol(df_plot, piv, rec["best"], None, title)
        else:
            title = (f"{sym} | CPR cross ({rec['direction']}) @ {rec['cross_time'].strftime('%H:%M')} (age {rec['age_min']}m) | "
                     f"CPR_width%={rec['width_pct']:.3f} | SL={rec['stoploss']:.2f}")
            fig = plot_symbol(df_plot, piv, None, rec, title)

        include_js = PLOTLY_JS_MODE if idx == 1 else False
        div = pio.to_html(fig, full_html=False, include_plotlyjs=include_js, config={"responsive": True})
        blocks.append(f"<div class='card'>{div}</div>")

    cols = 3 if len(finals) >= 3 else 2

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8" />
<title>Live Scan — {d}</title>
<style>
body {{ font-family: Arial, sans-serif; }}
.wrap {{ max-width: 1600px; margin: 0 auto; }}
.grid {{ display: grid; grid-template-columns: repeat({cols}, 1fr); gap: 12px; }}
.card {{ border: 1px solid #ddd; padding: 8px; background: #fafafa; overflow: visible; }}
.small {{ color: #444; font-size: 13px; }}
</style>
</head>
<body>
<div class="wrap">
<h1>Live scan: Top W + Top M + Top CPR-cross (thin CPR)</h1>
<p class="small">
<b>Date:</b> {d}
&nbsp; <b>Scan bars:</b> {SCAN_BARS}
&nbsp; <b>Max W/M age:</b> {MAX_PATTERN_AGE_MIN} min
&nbsp; <b>CPR-cross age:</b> {MAX_CPR_CROSS_AGE_MIN} min
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
