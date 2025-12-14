"""
NIFTY 1-min browser app with CPR + step-by-step candle reveal (SIDE-BY-SIDE)

• Two charts side by side: Previous day (full, left) and Current day (progressive reveal, right).
• Zerodha-style CPR: for day D, CPR (TC/P/BC) uses previous day’s H/L/C; drawn fully across the day.
• 7 traditional pivot levels (P, R1–R3, S1–S3), from previous day’s H/L/C.
• Right-Arrow (→) initializes a random day (if none) and advances one candle each press.
• Larger, clearer charts (~90vh; thicker candles; unified hover; scroll zoom; edge-to-edge layout).
• Avoid duplicate dates per bucket (High/Low) via cache/used_dates.json; auto-resets when exhausted.
• Avoids .to_pydatetime warnings by passing NumPy arrays/Series directly.

Prereqs:
pip install pandas numpy pyarrow pytz plotly dash dash-bootstrap-components dash-extensions kiteconnect
"""
from __future__ import annotations

import os, json, time, random
from datetime import datetime, timedelta, date, time as dtime
from typing import Optional, Tuple, Dict, List

import numpy as np
import pandas as pd
import pytz

import plotly.graph_objects as go
from dash import Dash, html, dcc, Input, Output, State, ctx, no_update
import dash_bootstrap_components as dbc
from dash_extensions import EventListener

# --- Utils / Kite ---
try:
    import OptionTradeUtils as oUtils
except Exception:
    oUtils = None
    print("[WARN] OptionTradeUtils not found; need it for authenticated Kite.")

try:
    from kiteconnect import KiteConnect
except Exception:
    KiteConnect = None

IST = pytz.timezone("Asia/Kolkata")

# -------------------------------
# Config
# -------------------------------
SYMBOL_NAME   = "NIFTY 50"
DEFAULT_TOKEN = 256265      # Zerodha instrument token for NIFTY 50
INTERVAL      = "minute"
TRADING_START = dtime(9, 15)
TRADING_END   = dtime(15, 30)

BASE_DIR = os.path.dirname(__file__)
CACHE_DIR = os.path.join(BASE_DIR, "cache")
os.makedirs(CACHE_DIR, exist_ok=True)
USED_DATES_PATH = os.path.join(CACHE_DIR, "used_dates.json")

TODAY_IST = datetime.now(IST).date()
TO_DATE   = TODAY_IST - timedelta(days=1)
FROM_DATE = TO_DATE - timedelta(days=730)

# -------------------------------
# Used-dates persistence
# -------------------------------
def load_used_dates() -> Dict[str, List[str]]:
    if os.path.exists(USED_DATES_PATH):
        try:
            with open(USED_DATES_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"High": [], "Low": []}

def save_used_dates(store: Dict[str, List[str]]) -> None:
    try:
        with open(USED_DATES_PATH, "w", encoding="utf-8") as f:
            json.dump(store, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[WARN] Could not save used dates: {e}")

# -------------------------------
# Kite helpers
# -------------------------------
def init_kite() -> KiteConnect:
    if oUtils is None:
        raise RuntimeError("OptionTradeUtils is required & must be authenticated.")
    kite = oUtils.intialize_kite_api()
    if kite is None:
        raise RuntimeError("oUtils.intialize_kite_api() returned None.")
    return kite

def discover_nifty_token(kite: KiteConnect) -> int:
    return DEFAULT_TOKEN

# -------------------------------
# Historical fetch + cache
# -------------------------------
def cache_path(symbol: str, token: int, from_dt: date, to_dt: date, interval: str) -> str:
    key = f"{symbol.replace(' ','')}_{token}_{interval}_{from_dt.strftime('%Y%m%d')}_{to_dt.strftime('%Y%m%d')}.parquet"
    return os.path.join(CACHE_DIR, key)

def fetch_historical_minute_df(
    kite: KiteConnect, token: int, from_dt: date, to_dt: date, interval: str = INTERVAL
) -> pd.DataFrame:
    start = datetime.combine(from_dt, dtime(0, 0)).astimezone(IST)
    end   = datetime.combine(to_dt, dtime(23, 59)).astimezone(IST)

    def _fetch_chunk(s: datetime, e: datetime, retries: int = 3) -> List[dict]:
        for attempt in range(retries):
            try:
                return kite.historical_data(
                    instrument_token=token, from_date=s, to_date=e,
                    interval=interval, continuous=False, oi=False
                )
            except Exception as ex:
                wait = 1 + attempt * 2
                print(f"[WARN] historical_data error: {ex} | retrying in {wait}s …")
                time.sleep(wait)
        raise

    frames: List[pd.DataFrame] = []
    cur = start
    while cur < end:
        chunk_end = min(cur + timedelta(days=60), end)
        raw = _fetch_chunk(cur, chunk_end)
        if raw:
            frames.append(pd.DataFrame(raw))
        cur = chunk_end
        time.sleep(0.2)

    if not frames:
        raise RuntimeError("No candles returned.")

    df = pd.concat(frames, ignore_index=True)

    # Robust datetime + numeric normalization (no .to_pydatetime)
    df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
    if df["date"].isna().any():
        bad = int(df["date"].isna().sum())
        print(f"[WARN] Dropping {bad} rows with unparsable dates")
        df = df.dropna(subset=["date"])
    df["date"] = df["date"].dt.tz_convert(IST)

    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["open", "high", "low", "close"])

    df = df.sort_values("date").reset_index(drop=True)
    df["session_date"] = df["date"].dt.date
    df["session_time"] = df["date"].dt.time

    df = df[(df["session_time"] >= TRADING_START) & (df["session_time"] <= TRADING_END)].copy()
    return df

def get_cached_or_download(kite: KiteConnect, symbol: str, token: int, from_dt: date, to_dt: date, interval: str) -> pd.DataFrame:
    path = cache_path(symbol, token, from_dt, to_dt, interval)
    if os.path.exists(path):
        print(f"[CACHE] {path}")
        return pd.read_parquet(path)
    print("[FETCH] Downloading candles from Kite …")
    df = fetch_historical_minute_df(kite, token, from_dt, to_dt, interval)
    df.to_parquet(path, index=False)
    print(f"[CACHE] Saved -> {path}")
    return df

# -------------------------------
# Analytics: daily OHLC + CPR + Pivots
# -------------------------------
def daily_ohlc(df: pd.DataFrame) -> pd.DataFrame:
    g = df.groupby("session_date")
    daily = g.agg(high=("high", "max"), low=("low", "min"), close=("close", "last")).reset_index()
    daily["volatility"] = (daily["high"] - daily["low"]).abs() / daily["low"].replace(0, np.nan)
    return daily.dropna()

def cpr_from_prev_day(daily_df: pd.DataFrame, day: date) -> Optional[Tuple[float, float, float]]:
    dates = sorted(daily_df["session_date"].tolist())
    if day not in dates:
        return None
    idx = dates.index(day)
    if idx == 0:
        return None
    prev_day = dates[idx - 1]
    r = daily_df.loc[daily_df["session_date"] == prev_day].iloc[0]
    H, L, C = float(r["high"]), float(r["low"]), float(r["close"])
    P  = (H + L + C) / 3.0
    BC = (H + L) / 2.0
    TC = 2.0 * P - BC
    if BC > TC:
        BC, TC = TC, BC
    return BC, P, TC

def pivot_levels_from_prev_day(daily_df: pd.DataFrame, day: date) -> Optional[Dict[str, float]]:
    dates = sorted(daily_df["session_date"].tolist())
    if day not in dates:
        return None
    idx = dates.index(day)
    if idx == 0:
        return None
    prev_day = dates[idx - 1]
    r = daily_df.loc[daily_df["session_date"] == prev_day].iloc[0]
    H, L, C = float(r["high"]), float(r["low"]), float(r["close"])
    P  = (H + L + C) / 3.0
    R1 = 2 * P - L
    S1 = 2 * P - H
    R2 = P + (H - L)
    S2 = P - (H - L)
    R3 = H + 2 * (P - L)
    S3 = L - 2 * (H - P)
    return {"P": P, "R1": R1, "R2": R2, "R3": R3, "S1": S1, "S2": S2, "S3": S3}

# -------------------------------
# Plotting helpers (side-by-side)
# -------------------------------
def _build_partial_candle_arrays(df_full: pd.DataFrame, step: int):
    """
    Return (x, o, h, l, c) for a candlestick where only the first `step`
    candles are visible and the rest are None. Keeps normal bar width from step 1.
    """
    x = df_full["date"].to_numpy()
    o = df_full["open"].to_numpy(dtype=float).astype(object)
    h = df_full["high"].to_numpy(dtype=float).astype(object)
    l = df_full["low"].to_numpy(dtype=float).astype(object)
    c = df_full["close"].to_numpy(dtype=float).astype(object)
    step = int(max(1, min(step, len(x))))
    if step < len(x):
        o[step:] = None; h[step:] = None; l[step:] = None; c[step:] = None
    return x, o, h, l, c

def make_day_fig(
    df_day: pd.DataFrame,
    cpr_lines: Optional[Tuple[float, float, float]],
    pivot_levels: Optional[Dict[str, float]],
    title: str,
    full_df_for_axes: Optional[pd.DataFrame] = None,
    step: Optional[int] = None
) -> go.Figure:
    fig = go.Figure()
    if (df_day is None or df_day.empty) and full_df_for_axes is None:
        fig.update_layout(template="plotly_white")
        return fig

    # Determine candles (partial vs full)
    if step is not None and full_df_for_axes is not None:
        x, o, h, l, c = _build_partial_candle_arrays(full_df_for_axes, step)
        base_df = full_df_for_axes
    else:
        x = df_day["date"].to_numpy()
        o = df_day["open"].to_numpy(dtype=float)
        h = df_day["high"].to_numpy(dtype=float)
        l = df_day["low"].to_numpy(dtype=float)
        c = df_day["close"].to_numpy(dtype=float)
        base_df = df_day

    # Candles (thicker for clarity)
    fig.add_trace(go.Candlestick(
        x=x, open=o, high=h, low=l, close=c,
        increasing=dict(line=dict(width=2)),
        decreasing=dict(line=dict(width=2)),
        name="NIFTY 1m"
    ))

    # Use full-day x-extent for lines
    x_start = base_df["date"].iloc[0]
    x_end   = base_df["date"].iloc[-1]

    # 7 Pivot levels: P, R1–R3, S1–S3 (from previous day)
    if pivot_levels is not None:
        order  = ["S3", "S2", "S1", "P", "R1", "R2", "R3"]
        dashes = {"P": "solid", "R1": "dot", "R2": "dash", "R3": "dot", "S1": "dot", "S2": "dash", "S3": "dot"}
        for key in order:
            y = pivot_levels.get(key)
            if y is None:
                continue
            y = float(y)
            fig.add_trace(go.Scatter(
                x=[x_start, x_end], y=[y, y], mode="lines",
                line=dict(width=1, dash=dashes.get(key, "dot")),
                hoverinfo="skip", showlegend=False, name=key
            ))
            fig.add_annotation(x=x_end, y=y, text=f"{key} {y:.2f}", showarrow=False, font=dict(size=10), xanchor="left")

    # CPR (BC & TC only; P duplicates pivot P)
    if cpr_lines is not None:
        BC, P_cpr, TC = cpr_lines
        for label, y, dash in [("BC", BC, "dot"), ("TC", TC, "dot")]:
            if y is None:
                continue
            yv = float(y)
            fig.add_trace(go.Scatter(
                x=[x_start, x_end], y=[yv, yv], mode="lines",
                line=dict(width=1, dash=dash), hoverinfo="skip", showlegend=False
            ))
            fig.add_annotation(x=x_end, y=yv, text=f"{label} {yv:.2f}", showarrow=False, font=dict(size=10), xanchor="left")

    # Fix x-range to the full trading session for visual consistency
    try:
        d0 = IST.localize(datetime.combine(base_df["date"].iloc[0].date(), TRADING_START))
        d1 = IST.localize(datetime.combine(base_df["date"].iloc[0].date(), TRADING_END))
        fig.update_xaxes(range=[d0, d1])
    except Exception:
        pass

    # Stable y-range: include day's H/L plus pivots + CPR
    try:
        ymins = [float(np.nanmin(base_df["low"]))]; ymaxs = [float(np.nanmax(base_df["high"]))]
        if pivot_levels:
            for k in ["S3","S2","S1","P","R1","R2","R3"]:
                if k in pivot_levels and pivot_levels[k] is not None:
                    val = float(pivot_levels[k]); ymins.append(val); ymaxs.append(val)
        if cpr_lines:
            BC, P_cpr, TC = cpr_lines
            for val in [BC, P_cpr, TC]:
                if val is not None:
                    v = float(val); ymins.append(v); ymaxs.append(v)
        lo = min(ymins); hi = max(ymaxs); span = max(1e-6, hi - lo)
        pad = max(span * 0.03, hi * 0.001)
        fig.update_yaxes(range=[lo - pad, hi + pad])
    except Exception:
        pass

    fig.update_layout(
        title=title,
        xaxis_title="Time (IST)", yaxis_title="Price",
        xaxis_rangeslider_visible=False,
        template="plotly_white",
        margin=dict(l=10, r=10, t=35, b=10),
        hovermode="x unified",
    )
    return fig

# -------------------------------
# Build data
# -------------------------------
print("[INIT] Initializing Kite & preparing data …")
KITE  = init_kite()
TOKEN = discover_nifty_token(KITE)
ALL_MIN_DF = get_cached_or_download(KITE, SYMBOL_NAME, TOKEN, FROM_DATE, TO_DATE, INTERVAL)
DAILY_DF   = daily_ohlc(ALL_MIN_DF)
MEDIAN_VOL = DAILY_DF["volatility"].median()
ALL_DATES  = sorted(DAILY_DF["session_date"].tolist())

def previous_trading_day(d: date) -> Optional[date]:
    if d not in ALL_DATES:
        return None
    i = ALL_DATES.index(d)
    if i == 0:
        return None
    return ALL_DATES[i - 1]

def pick_random_date(vol_kind: str) -> Optional[date]:
    used_store = load_used_dates()
    used_list = set(used_store.get(vol_kind, []))

    if vol_kind == "High":
        pool = DAILY_DF[DAILY_DF["volatility"] >= MEDIAN_VOL]["session_date"].tolist()
    else:
        pool = DAILY_DF[DAILY_DF["volatility"] < MEDIAN_VOL]["session_date"].tolist()

    pool = [d for d in sorted(pool) if d in ALL_DATES and ALL_DATES.index(d) > 0]
    fresh = [d for d in pool if d.isoformat() not in used_list]
    if not fresh:
        used_store[vol_kind] = []
        save_used_dates(used_store)
        fresh = pool[:]
    if not fresh:
        return None
    return random.choice(fresh)

def mark_date_used(vol_kind: str, d: date):
    store = load_used_dates()
    arr = store.get(vol_kind, [])
    iso = d.isoformat()
    if iso not in arr:
        arr.append(iso)
        store[vol_kind] = arr
        save_used_dates(store)

# -------------------------------
# Dash UI
# -------------------------------
external_stylesheets = [dbc.themes.BOOTSTRAP]
app = Dash(__name__, external_stylesheets=external_stylesheets)
app.title = "NIFTY CPR Reveal (Side-by-Side)"

keyboard_events = [{"event": "keydown", "props": ["key"]}]

app.layout = dbc.Container([
    html.Div([
        html.Div([
            html.Label("Volatility type:", className="me-2"),
            dcc.RadioItems(
                id="vol-choice",
                options=[{"label": "High", "value": "High"}, {"label": "Low", "value": "Low"}],
                value="High", inline=True
            ),
            dbc.Button("Pick random day", id="pick-btn", color="primary", className="ms-3"),
            dbc.Button("Next candle (→)", id="next-btn", color="secondary", className="ms-2"),
            html.Span(id="selection-info", className="ms-3 fw-semibold"),
        ], className="d-flex align-items-center flex-wrap py-2 px-2"),

        EventListener(id="key-listener", events=keyboard_events, children=html.Div(id="listener-anchor")),
        dcc.Store(id="sel-store"),

        # Side-by-side charts
        dbc.Row([
            dbc.Col(
                dcc.Graph(
                    id="prev-graph", figure=go.Figure(),
                    config={"displayModeBar": True, "displaylogo": False, "responsive": True, "scrollZoom": True},
                    style={"height": "90vh", "width": "100%"}
                ),
                md=6
            ),
            dbc.Col(
                dcc.Graph(
                    id="curr-graph", figure=go.Figure(),
                    config={"displayModeBar": True, "displaylogo": False, "responsive": True, "scrollZoom": True},
                    style={"height": "90vh", "width": "100%"}
                ),
                md=6
            ),
        ], className="g-0 my-0 px-2"),
    ], style={"maxWidth": "100%", "padding": "0"})
], fluid=True, className="p-0")

# -------------------------------
# Callbacks
# -------------------------------
# Initialize / repick via button or radio change
@app.callback(
    Output("sel-store", "data"),
    Input("vol-choice", "value"),
    Input("pick-btn", "n_clicks"),
    prevent_initial_call=True
)
def set_random_selection(vol_choice, _):
    if vol_choice is None:
        vol_choice = "High"
    d = pick_random_date(vol_choice)
    if d is None:
        return no_update
    prev_d = previous_trading_day(d)
    mark_date_used(vol_choice, d)
    return {"vol": vol_choice, "day": d.isoformat(), "prev": prev_d.isoformat() if prev_d else None, "step": 1}

# Right-arrow and button: initialize if missing; otherwise advance step
@app.callback(
    Output("sel-store", "data", allow_duplicate=True),
    Input("key-listener", "n_events"),
    State("key-listener", "event"),
    Input("next-btn", "n_clicks"),
    State("vol-choice", "value"),
    State("sel-store", "data"),
    prevent_initial_call=True
)
def advance_step(n_events, last_event, next_clicks, vol_choice, sel):
    trig = ctx.triggered_id

    # Initialize on Right-Arrow if no selection yet
    if (sel is None or not sel) and trig in ("key-listener", "next-btn"):
        if trig == "key-listener":
            key = (last_event or {}).get("key")
            if key not in ("ArrowRight", "Right"):
                return no_update
        if vol_choice is None:
            vol_choice = "High"
        d = pick_random_date(vol_choice)
        if d is None:
            return no_update
        prev_d = previous_trading_day(d)
        mark_date_used(vol_choice, d)
        return {"vol": vol_choice, "day": d.isoformat(), "prev": prev_d.isoformat() if prev_d else None, "step": 1}

    if not sel:
        return no_update

    inc = False
    if trig == "next-btn" and next_clicks:
        inc = True
    elif trig == "key-listener" and n_events:
        key = (last_event or {}).get("key")
        if key in ("ArrowRight", "Right"):
            inc = True

    if not inc:
        return no_update

    d = date.fromisoformat(sel.get("day")) if sel.get("day") else None
    if d is None:
        return no_update

    day_df = ALL_MIN_DF[ALL_MIN_DF["session_date"] == d]
    max_step = len(day_df)
    step = min(int(sel.get("step", 1)) + 1, max_step)

    new_sel = dict(sel)
    new_sel["step"] = step
    return new_sel

# Build both charts
@app.callback(
    Output("selection-info", "children"),
    Output("prev-graph", "figure"),
    Output("curr-graph", "figure"),
    Input("sel-store", "data")
)
def update_charts(sel):
    if not sel:
        return ("Pick a day to begin (or press →).", go.Figure(), go.Figure())

    vol_choice = sel.get("vol", "High")
    d_iso      = sel.get("day")
    p_iso      = sel.get("prev")
    step       = int(sel.get("step", 1))

    if not d_iso or not p_iso:
        return ("Selection incomplete.", go.Figure(), go.Figure())

    d = date.fromisoformat(d_iso)
    prev_d = date.fromisoformat(p_iso)

    prev_df     = ALL_MIN_DF[ALL_MIN_DF["session_date"] == prev_d]
    day_df_full = ALL_MIN_DF[ALL_MIN_DF["session_date"] == d]
    step        = max(1, min(step, len(day_df_full)))
    day_df      = day_df_full.iloc[:step]

    prev_cpr = cpr_from_prev_day(DAILY_DF, prev_d)
    day_cpr  = cpr_from_prev_day(DAILY_DF, d)

    prev_piv = pivot_levels_from_prev_day(DAILY_DF, prev_d)
    day_piv  = pivot_levels_from_prev_day(DAILY_DF, d)

    prev_fig = make_day_fig(prev_df, prev_cpr, prev_piv, title=f"Previous Day: {prev_d}", full_df_for_axes=prev_df)
    curr_fig = make_day_fig(day_df, day_cpr, day_piv,
                            title=f"Selected Day: {d}  (shown {len(day_df)}/{len(day_df_full)})",
                            full_df_for_axes=day_df_full, step=len(day_df))

    info = f"Volatility: {vol_choice} | Selected: {d} | Previous: {prev_d} | Press → to reveal next candle"
    return info, prev_fig, curr_fig

# -------------------------------
# Main
# -------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8050"))
    print(f"Open your browser at: http://127.0.0.1:{port}")
    app.run(debug=True, port=port)
