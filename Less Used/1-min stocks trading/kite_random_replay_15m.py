"""
kite_random_replay_15m_cpr_prevday.py

- Random stock + random date
- Fetch 1-min session candles (09:15-15:30 IST)
- Replay: initially up to 09:30, then +15m per click
- Overlay CPR lines (R1, BC, P, TC, S1) computed from previous trading day's H/L/C
- Also show Previous Day chart (full day) with its CPR (computed from the day before it)
- Persistent used-combo cache (SQLite)
- Fix candle width feel: keep x-axis fixed + use max-width centered container
"""

from __future__ import annotations

import os
import json
import random
import argparse
import webbrowser
import sqlite3
from datetime import datetime, date, time as dtime, timedelta
from typing import Optional, Tuple, List, Set, Dict

import pandas as pd
import plotly.graph_objects as go
from kiteconnect import KiteConnect

SESSION_START = dtime(9, 15)
SESSION_END = dtime(15, 30)
INITIAL_REVEAL_END = dtime(9, 30)
STEP_MINUTES = 15

IST_OFFSET = timedelta(hours=5, minutes=30)

INSTR_CACHE_DIR = "./kite_instruments_cache"
INSTR_CACHE_TTL_DAYS = 7

DEFAULT_USED_DB = "./kite_random_replay_used.sqlite"
PREV_TRADING_DAY_MAX_BACK_DAYS = 12


def log(level: str, msg: str) -> None:
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} [{level}] {msg}")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="stock_list.csv", help="CSV path (default: ./stock_list.csv)")
    ap.add_argument("--exchange", default="NSE", help="Default exchange if not present in symbol (NSE/BSE)")
    ap.add_argument("--days-back", type=int, default=180, help="Random date range (calendar days back from today)")
    ap.add_argument("--tries", type=int, default=120, help="Max attempts to find a stock+date with data")
    ap.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility")
    ap.add_argument("--symbol", default="", help="Override random pick. e.g. INFY or NSE:INFY")
    ap.add_argument("--date", default="", help="Override random date. YYYY-MM-DD")
    ap.add_argument("--no-open", action="store_true", help="Do not auto-open the HTML in browser")
    ap.add_argument("--out", default="", help="Output HTML filename (optional)")
    ap.add_argument("--used-db", default=DEFAULT_USED_DB, help="SQLite DB file to store used combos")
    ap.add_argument("--reset-used", action="store_true", help="Clear used-combo cache and exit")
    ap.add_argument("--allow-repeat", action="store_true", help="Allow selecting combos even if already used")
    ap.add_argument("--min-candles", type=int, default=120, help="Minimum candles required to accept a day")
    ap.add_argument("--gap-fill", action="store_true", help="Fill missing 1-min candles with flat vol=0 bars")
    return ap.parse_args()


def resolve_symbol_exchange(symbol_in: str, exchange_in: Optional[str]) -> Tuple[str, str]:
    s = symbol_in.strip()
    if ":" in s:
        ex, ts = s.split(":", 1)
        return ex.upper().strip(), ts.strip()
    ex = (exchange_in or "NSE").upper().strip()
    return ex, s


def read_symbols_from_csv(path: str) -> List[str]:
    df = pd.read_csv(path)
    candidates = ["symbol", "tradingsymbol", "ticker", "stock", "scrip", "instrument"]
    col = next((c for c in candidates if c in df.columns), None)
    if col is None:
        col = df.columns[0]
    syms = df[col].astype(str).map(lambda x: x.strip()).tolist()
    return [s for s in syms if s and s.lower() not in ("nan", "none")]


def is_weekend(d: date) -> bool:
    return d.weekday() >= 5


def random_date_within(days_back: int) -> date:
    today = datetime.now().date()
    start = today - timedelta(days=days_back)
    delta = (today - start).days
    return start + timedelta(days=random.randint(0, max(delta, 0)))


def used_key(exchange: str, tradingsymbol: str, day: date) -> str:
    return f"{exchange.upper()}:{tradingsymbol.strip()}|{day.isoformat()}"


def init_used_db(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(os.path.abspath(db_path)) or ".", exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS used_combos (
            exchange TEXT NOT NULL,
            tradingsymbol TEXT NOT NULL,
            day TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (exchange, tradingsymbol, day)
        )
        """
    )
    conn.commit()
    return conn


def load_used_set(conn: sqlite3.Connection) -> Set[str]:
    rows = conn.execute("SELECT exchange, tradingsymbol, day FROM used_combos").fetchall()
    return {f"{str(ex).upper()}:{str(ts).strip()}|{str(dy)}" for ex, ts, dy in rows}


def mark_used(conn: sqlite3.Connection, exchange: str, tradingsymbol: str, day: date, used_set: Set[str]) -> None:
    ex = exchange.upper()
    ts = tradingsymbol.strip()
    dy = day.isoformat()
    conn.execute(
        "INSERT OR IGNORE INTO used_combos(exchange, tradingsymbol, day, created_at) VALUES (?,?,?,?)",
        (ex, ts, dy, datetime.now().isoformat(timespec="seconds")),
    )
    conn.commit()
    used_set.add(f"{ex}:{ts}|{dy}")


def reset_used(conn: sqlite3.Connection) -> None:
    conn.execute("DELETE FROM used_combos")
    conn.commit()


def strip_tz(ts):
    if ts is None:
        return ts
    if hasattr(ts, "to_pydatetime"):
        ts = ts.to_pydatetime()
    if getattr(ts, "tzinfo", None) is not None:
        ts = ts.replace(tzinfo=None)
    return ts


def floor_minute(ts: datetime) -> datetime:
    return ts.replace(second=0, microsecond=0)


def looks_like_utc_naive_session(ts_min: datetime, ts_max: datetime) -> bool:
    tmin = ts_min.time()
    tmax = ts_max.time()
    return (dtime(3, 15) <= tmin <= dtime(4, 45)) and (dtime(9, 30) <= tmax <= dtime(10, 30))


def auto_shift_to_ist_if_needed(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    ts_min = df["date"].min().to_pydatetime()
    ts_max = df["date"].max().to_pydatetime()
    if looks_like_utc_naive_session(ts_min, ts_max):
        df = df.copy()
        df["date"] = df["date"] + IST_OFFSET
    return df


def gap_fill_minutes(df: pd.DataFrame, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    if df.empty:
        return df
    full_idx = pd.date_range(start_dt, end_dt, freq="min")
    df2 = df.set_index("date").reindex(full_idx)

    for c in ["open", "high", "low", "close", "volume"]:
        if c not in df2.columns:
            df2[c] = pd.NA

    df2["close"] = pd.to_numeric(df2["close"], errors="coerce").ffill()

    miss = df2["open"].isna() | df2["high"].isna() | df2["low"].isna()
    df2.loc[miss, "open"] = df2.loc[miss, "close"]
    df2.loc[miss, "high"] = df2.loc[miss, "close"]
    df2.loc[miss, "low"] = df2.loc[miss, "close"]

    df2["volume"] = pd.to_numeric(df2["volume"], errors="coerce").fillna(0).astype(int)

    df2 = df2.dropna(subset=["close"]).copy()
    df2.index.name = "date"
    df2 = df2.reset_index()

    for c in ["open", "high", "low", "close"]:
        df2[c] = pd.to_numeric(df2[c], errors="coerce")
    df2 = df2.dropna(subset=["open", "high", "low", "close"]).reset_index(drop=True)
    return df2


def init_kite() -> KiteConnect:
    try:
        import Trading_2024.OptionTradeUtils as oUtils
        log("STEP", "Initializing Kite via Trading_2024.OptionTradeUtils.intialize_kite_api() ...")
        kite = oUtils.intialize_kite_api()
        log("INFO", "Kite initialized via OptionTradeUtils.")
        return kite
    except Exception as e:
        log("WARN", f"OptionTradeUtils init not available ({e}). Falling back to env vars.")

    api_key = os.environ.get("KITE_API_KEY", "").strip()
    access_token = os.environ.get("KITE_ACCESS_TOKEN", "").strip()
    if not api_key or not access_token:
        raise RuntimeError(
            "Kite init failed. Provide Trading_2024.OptionTradeUtils.intialize_kite_api(), "
            "or set env vars KITE_API_KEY and KITE_ACCESS_TOKEN."
        )
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    log("INFO", "Kite initialized via env vars.")
    return kite


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

    log("STEP", f"Downloading instruments dump for {exchange} (cached {INSTR_CACHE_TTL_DAYS}d) ...")
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


def fetch_1m_session(kite: KiteConnect, token: int, day: date, gap_fill: bool) -> pd.DataFrame:
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

    start_dt = datetime.combine(day, SESSION_START)
    end_dt = datetime.combine(day, SESSION_END)

    df = df[df["date"].between(start_dt, end_dt)].copy()
    df = df.drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)

    for c in ["open", "high", "low", "close"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["volume"] = pd.to_numeric(df.get("volume", 0), errors="coerce").fillna(0).astype(int)
    df = df.dropna(subset=["open", "high", "low", "close"]).reset_index(drop=True)

    if gap_fill and not df.empty:
        df = gap_fill_minutes(df, start_dt, end_dt)

    return df


def fetch_prev_trading_day_1m(kite: KiteConnect, token: int, target_day: date, gap_fill: bool) -> Tuple[date, pd.DataFrame]:
    for i in range(1, PREV_TRADING_DAY_MAX_BACK_DAYS + 1):
        d = target_day - timedelta(days=i)
        df = fetch_1m_session(kite, token, d, gap_fill=gap_fill)
        if not df.empty:
            return d, df
    raise RuntimeError(f"Could not find previous trading day within last {PREV_TRADING_DAY_MAX_BACK_DAYS} days.")


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
        bgcolor="rgba(255,255,255,0.65)",
    )


def add_cpr_lines(fig: go.Figure, pivots: Dict[str, float]) -> None:
    for k in ["R1", "BC", "P", "TC", "S1"]:
        y = float(pivots[k])
        add_hline_with_label(fig, y, f"{k}: {y:.2f}")


def build_day_figure(
    df: pd.DataFrame,
    day: date,
    title: str,
    pivots: Optional[Dict[str, float]] = None,
    fixed_session_range: bool = True,
) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Candlestick(
        x=df["date"].to_list(),
        open=df["open"].to_list(),
        high=df["high"].to_list(),
        low=df["low"].to_list(),
        close=df["close"].to_list(),
        name="",
        increasing_line_width=1,
        decreasing_line_width=1,
    ))

    if pivots:
        add_cpr_lines(fig, pivots)

    start_dt = datetime.combine(day, SESSION_START)
    end_dt = datetime.combine(day, SESSION_END)

    xaxis = dict(
        type="date",
        rangeslider=dict(visible=False),
        showgrid=True,
        tickformat="%H:%M",
    )
    if fixed_session_range:
        xaxis["range"] = [start_dt, end_dt]

    fig.update_layout(
        title=title,
        xaxis=xaxis,
        yaxis=dict(showgrid=True, ticks="outside", side="right", fixedrange=False),
        margin=dict(l=18, r=18, t=56, b=18),
        showlegend=False,
        template="plotly_white",
        hovermode="x unified",
        autosize=True,
        uirevision="KEEP",  # keeps zoom/pan if you interact and then click Next
    )
    return fig


def write_html_two_charts(
    df_target_full: pd.DataFrame,
    target_day: date,
    pivots_target: Dict[str, float],
    prev_day: date,
    df_prev_full: pd.DataFrame,
    pivots_prev: Optional[Dict[str, float]],
    exchange: str,
    tradingsymbol: str,
    out_html: str,
) -> str:
    session_start_dt = datetime.combine(target_day, SESSION_START)
    initial_end_dt = datetime.combine(target_day, INITIAL_REVEAL_END)

    offsets = ((df_target_full["date"] - pd.Timestamp(session_start_dt)).dt.total_seconds() // 60).astype(int).tolist()
    initial_end_offset = int((initial_end_dt - session_start_dt).total_seconds() // 60)

    init_n = 0
    for m in offsets:
        if m <= initial_end_offset:
            init_n += 1
        else:
            break
    init_n = max(init_n, 1)

    df_target_init = df_target_full.iloc[:init_n].copy()

    title_target = f"{exchange}:{tradingsymbol} — {target_day} (Replay +{STEP_MINUTES}m) | CPR from {prev_day}"
    fig_target = build_day_figure(df_target_init, target_day, title_target, pivots_target, fixed_session_range=True)

    title_prev = (
        f"{exchange}:{tradingsymbol} — {prev_day} (Full day) | CPR from its prev day"
        if pivots_prev else
        f"{exchange}:{tradingsymbol} — {prev_day} (Full day)"
    )
    fig_prev = build_day_figure(df_prev_full, prev_day, title_prev, pivots_prev, fixed_session_range=True)

    # IMPORTANT: Use strings for x in replay as well (no epoch Date objects -> no IST/UTC drift)
    x_str = df_target_full["date"].dt.strftime("%Y-%m-%d %H:%M:%S").tolist()

    payload = {
        "x": x_str,
        "open": df_target_full["open"].astype(float).to_list(),
        "high": df_target_full["high"].astype(float).to_list(),
        "low": df_target_full["low"].astype(float).to_list(),
        "close": df_target_full["close"].astype(float).to_list(),
        "offset_min": offsets,
        "init_n": init_n,
        "initial_end_offset": initial_end_offset,
        "step_minutes": STEP_MINUTES,
        "session_start": SESSION_START.strftime("%H:%M"),
    }

    fig_target_json = fig_target.to_json()
    fig_prev_json = fig_prev.to_json()

    html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{exchange}:{tradingsymbol} {target_day} Replay + CPR</title>
  <script src="https://cdn.plot.ly/plotly-2.30.0.min.js"></script>
  <style>
    :root {{
      --maxw: 1400px;  /* reduce if you still feel candles are wide */
    }}
    body {{
      margin: 0;
      font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
      background: #ffffff;
    }}
    .wrap {{
      height: 100vh;
      display: flex;
      justify-content: center;
    }}
    .container {{
      width: min(100vw, var(--maxw));
      height: 100vh;
      display: grid;
      grid-template-rows: auto 1fr 1fr;
    }}
    .topbar {{
      display: flex;
      align-items: center;
      gap: 10px;
      padding: 10px 12px;
      border-bottom: 1px solid #eaeaea;
      flex-wrap: wrap;
    }}
    .title {{
      font-weight: 800;
      font-size: 14px;
      white-space: nowrap;
    }}
    .pill {{
      padding: 6px 10px;
      border: 1px solid #e0e0e0;
      border-radius: 999px;
      font-size: 12px;
      background: #fafafa;
    }}
    .status {{
      font-size: 13px;
      color: #444;
      margin-left: auto;
    }}
    button {{
      padding: 8px 12px;
      border: 1px solid #d0d0d0;
      border-radius: 8px;
      background: #f7f7f7;
      cursor: pointer;
      font-weight: 700;
    }}
    button:disabled {{
      opacity: 0.5;
      cursor: not-allowed;
    }}
    #chartTarget {{
      width: 100%;
      height: 100%;
    }}
    #chartPrev {{
      width: 100%;
      height: 100%;
      border-top: 1px solid #eaeaea;
    }}
  </style>
</head>

<body>
  <div class="wrap">
    <div class="container">
      <div class="topbar">
        <div class="title">{exchange}:{tradingsymbol}</div>
        <div class="pill">Target: {target_day} | CPR from {prev_day}</div>
        <button id="btnNext">Next +{STEP_MINUTES}m</button>
        <button id="btnReset">Reset</button>
        <div class="status" id="status"></div>
      </div>

      <div id="chartTarget"></div>
      <div id="chartPrev"></div>
    </div>
  </div>

  <script>
    const figTarget = {fig_target_json};
    const figPrev = {fig_prev_json};
    const payload = {json.dumps(payload)};

    const chartTarget = document.getElementById("chartTarget");
    const chartPrev = document.getElementById("chartPrev");
    const btnNext = document.getElementById("btnNext");
    const btnReset = document.getElementById("btnReset");
    const status = document.getElementById("status");

    const fullX = payload.x;
    const fullO = payload.open;
    const fullH = payload.high;
    const fullL = payload.low;
    const fullC = payload.close;
    const offsets = payload.offset_min;

    const initN = payload.init_n;
    const stepMin = payload.step_minutes;
    const initialEndOffset = payload.initial_end_offset;

    let currentEndOffset = initialEndOffset;

    function fmtHHMMFromOffset(m) {{
      const parts = payload.session_start.split(":").map(Number);
      const baseMin = parts[0]*60 + parts[1];
      const t = baseMin + m;
      const hh = String(Math.floor(t/60)).padStart(2, "0");
      const mm = String(t%60).padStart(2, "0");
      return hh + ":" + mm;
    }}

    function computeNForEndOffset(endOff) {{
      let n = 0;
      while (n < offsets.length && offsets[n] <= endOff) n++;
      return Math.max(n, 1);
    }}

    function updateStatus(n) {{
      const lastOff = offsets[Math.min(n-1, offsets.length-1)];
      const toTime = fmtHHMMFromOffset(lastOff);
      status.textContent =
        "Showing: " + payload.session_start + " → " + toTime +
        "  |  Candles: " + n + "/" + offsets.length;

      if (n >= offsets.length || lastOff >= offsets[offsets.length-1]) {{
        btnNext.disabled = true;
        status.textContent += "  |  Done ✅";
      }} else {{
        btnNext.disabled = false;
      }}
    }}

    const config = {{
      responsive: true,
      displaylogo: false,
      scrollZoom: true
    }};

    Plotly.newPlot(chartTarget, figTarget.data, figTarget.layout, config).then(() => {{
      updateStatus(initN);
    }});
    Plotly.newPlot(chartPrev, figPrev.data, figPrev.layout, config);

    btnNext.addEventListener("click", () => {{
      currentEndOffset += stepMin;
      const n = computeNForEndOffset(currentEndOffset);

      Plotly.restyle(chartTarget, {{
        x: [fullX.slice(0, n)],
        open: [fullO.slice(0, n)],
        high: [fullH.slice(0, n)],
        low: [fullL.slice(0, n)],
        close: [fullC.slice(0, n)]
      }}, [0]).then(() => {{
        updateStatus(n);
      }});
    }});

    btnReset.addEventListener("click", () => {{
      currentEndOffset = initialEndOffset;
      const n = computeNForEndOffset(currentEndOffset);

      Plotly.restyle(chartTarget, {{
        x: [fullX.slice(0, n)],
        open: [fullO.slice(0, n)],
        high: [fullH.slice(0, n)],
        low: [fullL.slice(0, n)],
        close: [fullC.slice(0, n)]
      }}, [0]).then(() => {{
        btnNext.disabled = false;
        updateStatus(n);
      }});
    }});
  </script>
</body>
</html>
"""
    with open(out_html, "w", encoding="utf-8") as f:
        f.write(html)

    return os.path.abspath(out_html)


def main():
    args = parse_args()
    if args.seed is not None:
        random.seed(args.seed)

    used_conn = init_used_db(args.used_db)
    try:
        if args.reset_used:
            reset_used(used_conn)
            log("INFO", f"Used-combo cache cleared: {os.path.abspath(args.used_db)}")
            return

        if not args.symbol.strip() and not args.csv.strip():
            raise RuntimeError("Provide --csv (stock list) or force a --symbol.")

        used_set = load_used_set(used_conn)
        log("INFO", f"Loaded used combos: {len(used_set)} from {os.path.abspath(args.used_db)}")

        syms: List[str] = []
        if not args.symbol.strip():
            syms = read_symbols_from_csv(args.csv)
            if not syms:
                raise RuntimeError("No symbols found in CSV.")

        kite = init_kite()

        forced_symbol = args.symbol.strip()
        forced_date = args.date.strip()

        for attempt in range(1, args.tries + 1):
            sym_in = forced_symbol if forced_symbol else random.choice(syms)
            exchange, tradingsymbol = resolve_symbol_exchange(sym_in, args.exchange)

            if forced_date:
                target_day = datetime.strptime(forced_date, "%Y-%m-%d").date()
            else:
                target_day = random_date_within(args.days_back)

            if not forced_date and is_weekend(target_day):
                continue

            k = used_key(exchange, tradingsymbol, target_day)
            if (not args.allow_repeat) and (k in used_set):
                log("INFO", f"Skipping already-used combo: {k}")
                if forced_symbol or forced_date:
                    raise RuntimeError(f"Forced combo already used: {k}. Use --allow-repeat or change inputs.")
                continue

            try:
                log("STEP", f"[{attempt}/{args.tries}] Picked {exchange}:{tradingsymbol} on {target_day}")
                token = get_instrument_token_fast(kite, exchange, tradingsymbol)

                df_target = fetch_1m_session(kite, token, target_day, gap_fill=args.gap_fill)
                if df_target.empty or len(df_target) < args.min_candles:
                    log("WARN", f"Insufficient target candles ({len(df_target)}). Retrying...")
                    continue

                prev_day, df_prev = fetch_prev_trading_day_1m(kite, token, target_day, gap_fill=args.gap_fill)
                pivots_target = compute_cpr_pivots(df_prev)

                pivots_prev = None
                try:
                    _, df_prev_prev = fetch_prev_trading_day_1m(kite, token, prev_day, gap_fill=args.gap_fill)
                    pivots_prev = compute_cpr_pivots(df_prev_prev)
                except Exception as e:
                    log("WARN", f"Could not compute prev-day CPR (still showing prev day chart): {e}")

                out_html = args.out.strip()
                if not out_html:
                    out_html = f"replay_cpr_{exchange}_{tradingsymbol}_{target_day}.html".replace(":", "_")

                abs_out = write_html_two_charts(
                    df_target_full=df_target,
                    target_day=target_day,
                    pivots_target=pivots_target,
                    prev_day=prev_day,
                    df_prev_full=df_prev,
                    pivots_prev=pivots_prev,
                    exchange=exchange,
                    tradingsymbol=tradingsymbol,
                    out_html=out_html,
                )

                mark_used(used_conn, exchange, tradingsymbol, target_day, used_set)

                log("INFO", f"Saved HTML: {abs_out}")
                log("INFO", f"Marked used combo: {k}")

                if not args.no_open:
                    try:
                        webbrowser.open(f"file:///{abs_out.replace(os.sep, '/')}")
                    except Exception:
                        pass
                return

            except Exception as e:
                log("WARN", f"Attempt failed: {e}")

        raise RuntimeError(
            "Could not find a valid *unused* stock+date with intraday candles. "
            "Try increasing --tries/--days-back, reduce --min-candles, or use --allow-repeat."
        )
    finally:
        try:
            used_conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
