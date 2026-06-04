# kite_20stocks_plotly_grid_4x5_fast.py
# First 20 symbols from your 50-stock list. 4 columns x 5 rows grid, fixed boxes + borders,
# per-chart axes, bold names, page zoom slider, Plotly zoom per chart.
#
# Optimizations:
# - Layout updates applied in one shot (fewer dict assignments).
# - Incremental minute fetching (cache) to cut API + speed refresh.
# - Pivots as scatter lines (faster than shapes).
# - Robust IST-naive datetime for Kite historical API (avoids invalid from date).

import os
import time
import datetime as dt
from dataclasses import dataclass
from collections import deque
from typing import Dict, Tuple

import pandas as pd
from kiteconnect import exceptions as kite_ex
import plotly.graph_objects as go

import Trading_2024.OptionTradeUtils as oUtils


# =========================
# CONFIG
# =========================

# First 20 from your earlier 50-list:
SYMBOLS_20 = [
    "RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK",
    "SBIN", "LT", "ITC", "HINDUNILVR", "BHARTIARTL",
    "AXISBANK", "KOTAKBANK", "ASIANPAINT", "MARUTI", "M&M",
    "TITAN", "SUNPHARMA", "NTPC", "ONGC", "POWERGRID"
]
SYMBOL_SET = set(SYMBOLS_20)

EXCHANGE = "NSE"

GRID_COLS = 4
GRID_ROWS = 5
assert GRID_COLS * GRID_ROWS == 20

LOOKBACK_MINUTES = 120
REFRESH_SECONDS = 25

OUTPUT_HTML = os.path.abspath("kite_20stocks_4x5_plotly.html")
OPEN_BROWSER = True

IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
SESSION_START = dt.time(9, 15)
SESSION_END = dt.time(15, 30)

HIST_MAX_CALLS_PER_SEC = 3
MAX_RETRIES = 5
RETRY_BACKOFF_BASE_SEC = 1.8
NETWORK_JITTER_SEC = 0.10

EXIT_AFTER_MARKET_CLOSE_SNAPSHOT = True

# Make boxes near-square: reduced width + increased height
FIG_WIDTH_PX = 1180
FIG_HEIGHT_PX = 1550

# Increase separation
PAD_X = 0.032
PAD_Y = 0.036

# Pivot colors + width
PIV_COLORS = {"R1": "orange", "TC": "purple", "P": "black", "BC": "blue", "S1": "green"}
PIV_WIDTH = {"R1": 1.0, "TC": 1.1, "P": 1.6, "BC": 1.1, "S1": 1.0}

# Grid border styling
CELL_BORDER_COLOR = "rgba(0,0,0,0.35)"
CELL_BORDER_WIDTH = 1

# =========================
# LOG
# =========================
def log(level: str, msg: str):
    now = dt.datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    print(f"{now} [{level}] {msg}")


# =========================
# Rate limiter
# =========================
class RateLimiter:
    def __init__(self, max_calls: int, per_seconds: float = 1.0):
        self.max_calls = max_calls
        self.per_seconds = per_seconds
        self.calls = deque()

    def wait(self):
        while True:
            now = time.time()
            while self.calls and self.calls[0] <= now - self.per_seconds:
                self.calls.popleft()
            if len(self.calls) < self.max_calls:
                self.calls.append(now)
                return
            time.sleep(max(0.01, self.per_seconds - (now - self.calls[0]) + 0.01))

rate_limiter = RateLimiter(HIST_MAX_CALLS_PER_SEC, 1.0)


# =========================
# Pivots
# =========================
@dataclass(frozen=True)
class PivotLevels:
    P: float
    BC: float
    TC: float
    R1: float
    S1: float
    ref_day: dt.date

def compute_pivots(H: float, L: float, C: float, ref_day: dt.date) -> PivotLevels:
    P = (H + L + C) / 3.0
    BC = (H + L) / 2.0
    TC = 2.0 * P - BC
    R1 = 2.0 * P - L
    S1 = 2.0 * P - H
    return PivotLevels(P=P, BC=BC, TC=TC, R1=R1, S1=S1, ref_day=ref_day)


# =========================
# Time helpers
# =========================
def now_ist() -> dt.datetime:
    return dt.datetime.now(IST)

def market_bounds(d: dt.date) -> Tuple[dt.datetime, dt.datetime]:
    return (
        dt.datetime.combine(d, SESSION_START, tzinfo=IST),
        dt.datetime.combine(d, SESSION_END, tzinfo=IST),
    )

def is_market_open(ts: dt.datetime) -> bool:
    s, e = market_bounds(ts.date())
    return s <= ts <= e

def floor_to_minute(t: dt.datetime) -> dt.datetime:
    return t.replace(second=0, microsecond=0)

def to_ist_naive(t: dt.datetime) -> dt.datetime:
    # Zerodha historical often behaves better with tz-naive local time
    if t.tzinfo is None:
        return floor_to_minute(t)
    return floor_to_minute(t.astimezone(IST).replace(tzinfo=None))

def clamp_intraday_window(end_dt: dt.datetime, lookback_min: int) -> Tuple[dt.datetime, dt.datetime]:
    s, e = market_bounds(end_dt.date())
    to_dt = min(end_dt, e)
    from_dt = to_dt - dt.timedelta(minutes=lookback_min)
    if from_dt < s:
        from_dt = s
    return from_dt, to_dt


# =========================
# Kite helpers
# =========================
def safe_historical_data(kite, token: int, from_dt: dt.datetime, to_dt: dt.datetime, interval: str, label: str):
    f = to_ist_naive(from_dt)
    t = to_ist_naive(to_dt)
    if f >= t:
        return []

    for attempt in range(1, MAX_RETRIES + 1):
        rate_limiter.wait()
        try:
            rows = kite.historical_data(
                instrument_token=token,
                from_date=f,
                to_date=t,
                interval=interval,
                continuous=False,
                oi=False,
            )
            time.sleep(NETWORK_JITTER_SEC)
            return rows
        except kite_ex.NetworkException as e:
            wait = (RETRY_BACKOFF_BASE_SEC ** attempt)
            log("WARN", f"NetworkException {label} {attempt}/{MAX_RETRIES}: {e} | sleep={wait:.1f}s")
            time.sleep(wait)
        except Exception as e:
            wait = (RETRY_BACKOFF_BASE_SEC ** attempt)
            log("WARN", f"Error {label} {attempt}/{MAX_RETRIES}: {e} | sleep={wait:.1f}s")
            time.sleep(wait)
    raise RuntimeError(f"Historical fetch failed after {MAX_RETRIES} retries: {label}")

def rows_to_df(rows) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    return df

def normalize_minute_df_ist(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    s = pd.to_datetime(df["date"], errors="coerce")
    if getattr(s.dt, "tz", None) is None:
        s = s.dt.tz_localize(IST)
    else:
        s = s.dt.tz_convert(IST)
    df = df.copy()
    df["date"] = s
    return df.dropna(subset=["date"]).reset_index(drop=True)


# =========================
# Pivot day logic
# =========================
def determine_last_td_and_ref_day(kite, probe_token: int) -> Tuple[dt.date, dt.date]:
    end_dt = now_ist()
    start_dt = end_dt - dt.timedelta(days=60)
    rows = safe_historical_data(kite, probe_token, start_dt, end_dt, "day", "probe calendar")
    df = rows_to_df(rows)
    if df.empty:
        d = end_dt.date()
        return d, d
    df["d"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df.dropna(subset=["d"]).reset_index(drop=True)
    last_td = df["d"].iloc[-1]
    prevs = df[df["d"] < last_td]
    ref = prevs["d"].iloc[-1] if not prevs.empty else last_td
    return last_td, ref

def load_pivots_for_ref_day(kite, sym_to_token: Dict[str, int], ref_day: dt.date) -> Dict[str, PivotLevels]:
    end_dt = now_ist()
    start_dt = end_dt - dt.timedelta(days=80)
    out: Dict[str, PivotLevels] = {}
    for sym in SYMBOLS_20:
        tok = sym_to_token[sym]
        rows = safe_historical_data(kite, tok, start_dt, end_dt, "day", f"{sym} day pivots")
        df = rows_to_df(rows)
        if df.empty:
            continue
        df["d"] = pd.to_datetime(df["date"], errors="coerce").dt.date
        row = df[df["d"] == ref_day]
        if row.empty:
            continue
        r = row.iloc[-1]
        out[sym] = compute_pivots(r["high"], r["low"], r["close"], ref_day=ref_day)
    return out


# =========================
# Plotly grid builder
# =========================
def build_plotly_grid(symbol_to_df: Dict[str, pd.DataFrame],
                      pivots: Dict[str, PivotLevels],
                      title: str) -> go.Figure:
    fig = go.Figure()

    cell_w = (1.0 - PAD_X * (GRID_COLS + 1)) / GRID_COLS
    cell_h = (1.0 - PAD_Y * (GRID_ROWS + 1)) / GRID_ROWS

    layout_updates = {}
    shapes = []
    annotations = []

    for idx, sym in enumerate(SYMBOLS_20, start=1):
        r = (idx - 1) // GRID_COLS  # 0..4 (top->bottom)
        c = (idx - 1) % GRID_COLS   # 0..3

        y_top = 1.0 - PAD_Y - r * (cell_h + PAD_Y)
        y_bottom = y_top - cell_h
        x_left = PAD_X + c * (cell_w + PAD_X)
        x_right = x_left + cell_w

        # Border rectangle
        shapes.append(dict(
            type="rect",
            x0=x_left, x1=x_right,
            y0=y_bottom, y1=y_top,
            xref="paper", yref="paper",
            line=dict(color=CELL_BORDER_COLOR, width=CELL_BORDER_WIDTH),
            fillcolor="rgba(0,0,0,0)",
            layer="below"
        ))

        layout_updates[f"xaxis{idx}"] = dict(
            domain=[x_left, x_right],
            anchor=f"y{idx}",
            showgrid=True,
            ticks="outside",
            tickfont=dict(size=10),
            rangeslider=dict(visible=False),
            zeroline=False,
        )
        layout_updates[f"yaxis{idx}"] = dict(
            domain=[y_bottom, y_top],
            anchor=f"x{idx}",
            showgrid=True,
            ticks="outside",
            tickfont=dict(size=10),
            zeroline=False,
        )

        # Bold stock label (fixed position in cell)
        annotations.append(dict(
            x=x_left + 0.006,
            y=y_top - 0.006,
            xref="paper",
            yref="paper",
            text=f"<b>{sym}</b>",
            showarrow=False,
            xanchor="left",
            yanchor="top",
            font=dict(size=12),
            bgcolor="rgba(255,255,255,0.85)",
            bordercolor="rgba(0,0,0,0.15)",
            borderwidth=1
        ))

        df = symbol_to_df.get(sym)
        if df is None or df.empty:
            continue

        # Candlestick
        fig.add_trace(go.Candlestick(
            x=df["date"],
            open=df["open"], high=df["high"], low=df["low"], close=df["close"],
            increasing_line_width=1,
            decreasing_line_width=1,
            xaxis=f"x{idx}",
            yaxis=f"y{idx}",
            showlegend=False,
            name=sym
        ))

        # Pivots as fast line traces
        piv = pivots.get(sym)
        if piv:
            x0 = df["date"].iloc[0]
            x1 = df["date"].iloc[-1]
            levels = {"R1": piv.R1, "TC": piv.TC, "P": piv.P, "BC": piv.BC, "S1": piv.S1}
            for name, y in levels.items():
                fig.add_trace(go.Scatter(
                    x=[x0, x1],
                    y=[y, y],
                    mode="lines",
                    line=dict(color=PIV_COLORS[name], width=PIV_WIDTH.get(name, 1.1)),
                    hoverinfo="skip",
                    xaxis=f"x{idx}",
                    yaxis=f"y{idx}",
                    showlegend=False
                ))

    fig.update_layout(
        **layout_updates,
        shapes=shapes,
        annotations=annotations,
        title=dict(text=title, x=0.01),
        width=FIG_WIDTH_PX,
        height=FIG_HEIGHT_PX,
        margin=dict(l=20, r=20, t=70, b=20),
        template="plotly_white",
        dragmode="zoom",
        showlegend=False
    )
    return fig


def write_html(fig: go.Figure, out_html: str, refresh_sec: int):
    plot_html = fig.to_html(include_plotlyjs="cdn", full_html=False)
    html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta http-equiv="refresh" content="{refresh_sec}">
  <title>Kite 20 Stocks 4x5</title>
  <style>
    body {{ margin:0; font-family: Arial, sans-serif; }}
    .topbar {{
      position: sticky; top: 0; background: #fff; border-bottom: 1px solid #ddd;
      padding: 8px 10px; z-index: 9999; display:flex; gap:16px; align-items:center;
    }}
    .wrap {{ padding: 10px; }}
    #scaleWrap {{ transform-origin: 0 0; }}
    input[type=range] {{ width: 220px; }}
  </style>
</head>
<body>
  <div class="topbar">
    <div><b>4×5 NSE Dashboard (20 stocks)</b> (auto-refresh {refresh_sec}s)</div>
    <div>Page zoom:
      <input id="zoom" type="range" min="60" max="180" value="100" />
      <span id="zv">100%</span>
    </div>
    <div style="opacity:0.7;">Drag inside any chart to zoom; double-click to reset.</div>
  </div>

  <div class="wrap">
    <div id="scaleWrap">{plot_html}</div>
  </div>

<script>
(function() {{
  var zoom = document.getElementById('zoom');
  var zv = document.getElementById('zv');
  var scaleWrap = document.getElementById('scaleWrap');

  function apply() {{
    var s = Number(zoom.value) / 100;
    scaleWrap.style.transform = "scale(" + s + ")";
    zv.textContent = zoom.value + '%';
  }}
  zoom.addEventListener('input', apply);
  apply();
}})();
</script>
</body>
</html>"""
    tmp = out_html + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(html)
    os.replace(tmp, out_html)


# =========================
# Main
# =========================
def main():
    log("STEP", "Initializing Kite ...")
    kite = oUtils.intialize_kite_api()
    log("INFO", "Kite initialized.")

    log("STEP", "Loading NSE instruments once ...")
    inst = kite.instruments(EXCHANGE)
    sym_to_token: Dict[str, int] = {}
    for r in inst:
        ts = str(r.get("tradingsymbol", "")).upper()
        if ts in SYMBOL_SET:
            sym_to_token[ts] = int(r["instrument_token"])

    missing = [s for s in SYMBOLS_20 if s not in sym_to_token]
    if missing:
        raise ValueError(f"Symbols not found in NSE instruments: {missing}")

    probe_token = sym_to_token[SYMBOLS_20[0]]
    last_td, ref_day = determine_last_td_and_ref_day(kite, probe_token)
    log("INFO", f"last_trading_day={last_td} | pivot_ref_day={ref_day}")

    log("STEP", "Loading pivots ...")
    pivots = load_pivots_for_ref_day(kite, sym_to_token, ref_day)
    log("INFO", f"Pivots ready for {len(pivots)}/20 symbols.")

    import webbrowser
    if OPEN_BROWSER:
        if not os.path.exists(OUTPUT_HTML):
            with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
                f.write("<html><body>Starting...</body></html>")
        webbrowser.open("file://" + OUTPUT_HTML)

    cache_df: Dict[str, pd.DataFrame] = {}
    last_dt: Dict[str, dt.datetime] = {}

    while True:
        t0 = time.time()
        asof = now_ist()

        if not is_market_open(asof) and EXIT_AFTER_MARKET_CLOSE_SNAPSHOT:
            # full-day snapshot and exit
            day_start, day_end = market_bounds(last_td)
            symbol_to_df: Dict[str, pd.DataFrame] = {}
            for i, sym in enumerate(SYMBOLS_20, start=1):
                tok = sym_to_token[sym]
                rows = safe_historical_data(kite, tok, day_start, day_end, "minute", f"{sym} full {i}/20")
                df = normalize_minute_df_ist(rows_to_df(rows))
                symbol_to_df[sym] = df

            title = f"FULL DAY {last_td} | Pivots {ref_day} | {asof.strftime('%Y-%m-%d %H:%M IST')}"
            fig = build_plotly_grid(symbol_to_df, pivots, title)
            write_html(fig, OUTPUT_HTML, refresh_sec=9999)
            log("INFO", "Rendered full-day snapshot. Exiting.")
            return

        window_from, window_to = clamp_intraday_window(asof, LOOKBACK_MINUTES)
        symbol_to_df: Dict[str, pd.DataFrame] = {}

        for i, sym in enumerate(SYMBOLS_20, start=1):
            tok = sym_to_token[sym]
            try:
                if sym not in cache_df or cache_df[sym].empty:
                    rows = safe_historical_data(kite, tok, window_from, window_to, "minute", f"{sym} init {i}/20")
                    df = normalize_minute_df_ist(rows_to_df(rows))
                    cache_df[sym] = df
                    if not df.empty:
                        last_dt[sym] = df["date"].iloc[-1]
                    symbol_to_df[sym] = df
                    continue

                inc_from = (last_dt.get(sym) + dt.timedelta(minutes=1)) if last_dt.get(sym) else window_from
                if inc_from < window_from:
                    inc_from = window_from
                if inc_from >= window_to:
                    symbol_to_df[sym] = cache_df[sym]
                    continue

                rows = safe_historical_data(kite, tok, inc_from, window_to, "minute", f"{sym} inc {i}/20")
                inc = normalize_minute_df_ist(rows_to_df(rows))

                if not inc.empty:
                    df = pd.concat([cache_df[sym], inc], ignore_index=True)
                    df = df.drop_duplicates(subset=["date"], keep="last").sort_values("date").reset_index(drop=True)

                    latest = df["date"].iloc[-1]
                    cutoff = latest - pd.Timedelta(minutes=LOOKBACK_MINUTES)
                    df = df[df["date"] >= cutoff].reset_index(drop=True)

                    cache_df[sym] = df
                    last_dt[sym] = df["date"].iloc[-1]

                symbol_to_df[sym] = cache_df[sym]

            except Exception as e:
                log("ERROR", f"{sym}: {e}")
                symbol_to_df[sym] = cache_df.get(sym, pd.DataFrame(columns=["date","open","high","low","close","volume"]))

        title = f"INTRADAY LIVE | 4×5 | {LOOKBACK_MINUTES}m | Pivots {ref_day} | {asof.strftime('%H:%M:%S IST')}"
        fig = build_plotly_grid(symbol_to_df, pivots, title)
        write_html(fig, OUTPUT_HTML, refresh_sec=REFRESH_SECONDS)

        elapsed = time.time() - t0
        sleep_for = max(1.0, REFRESH_SECONDS - elapsed)
        log("INFO", f"Cycle={elapsed:.1f}s | sleep={sleep_for:.1f}s")
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()
