# kite_50stocks_5x10_cpr_matplotlib.py
# 50 NSE stocks 1-min candlesticks in a 5x10 Matplotlib window + CPR pivots (P, BC, TC, R1, S1).
#
# BEHAVIOR
# - During market hours: refresh in a loop, incrementally fetching only new 1-min candles (fast).
# - Outside market hours: fetch FULL DAY 1-min candles for the LAST TRADING DAY, render ONCE, and EXIT.
#
# FIXES / IMPROVEMENTS (per your feedback)
# - Draw in a Matplotlib window (no browser/HTML).
# - Candles are clearly visible (colored bodies + wicks).
# - Pivots always visible: y-limits expanded to include pivot levels.
# - Much faster rendering: show x tick labels only on bottom row, y tick labels only on left column;
#   sparse x ticks (every 30 min).
# - Removed unnecessary daily "probe" calls in intraday loop (pivots won't change during session).
# - Atomic + stable: no blank "Starting..." page possible.
#
# NOTES
# - 50 panels is heavy; keep LOOKBACK_MINUTES reasonable (60–150).
# - If you want an even faster version, ask for "artist reuse" optimization.

import time
import datetime as dt
from dataclasses import dataclass
from collections import deque
from typing import Dict, List, Tuple, Optional

import pandas as pd
from kiteconnect import exceptions as kite_ex

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.lines import Line2D
from matplotlib.patches import Rectangle

# Your reference init
import Trading_2024.OptionTradeUtils as oUtils


# ==========================================================
# USER CONFIG
# ==========================================================

SYMBOLS_50 = [
    "RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK",
    "SBIN", "LT", "ITC", "HINDUNILVR", "BHARTIARTL",
    "AXISBANK", "KOTAKBANK", "ASIANPAINT", "MARUTI", "M&M",
    "TITAN", "SUNPHARMA", "NTPC", "ONGC", "POWERGRID",
    "BAJFINANCE", "BAJAJFINSV", "HCLTECH", "WIPRO", "TECHM",
    "ULTRACEMCO", "ADANIPORTS", "TATASTEEL", "JSWSTEEL", "HINDALCO",
    "COALINDIA", "DRREDDY", "APOLLOHOSP", "DIVISLAB", "CIPLA",
    "NESTLEIND", "BRITANNIA", "SBILIFE", "HDFCLIFE", "INDUSINDBK",
    "GRASIM", "SBIN", "HEROMOTOCO", "EICHERMOT", "BAJAJ-AUTO",
    "BPCL", "TATACONSUM", "SHRIRAMFIN", "ADANIENT", "PIDILITIND"
]
SYMBOL_SET = set(SYMBOLS_50)

EXCHANGE = "NSE"

ROWS = 10
COLS = 5

# During-market view window
LOOKBACK_MINUTES = 120

# Refresh cadence during market (real cadence limited by API calls)
REFRESH_SECONDS = 25

# Historical API safety (commonly ~3 req/sec for historical)
HIST_MAX_CALLS_PER_SEC = 3

MAX_RETRIES = 5
RETRY_BACKOFF_BASE_SEC = 1.8
NETWORK_JITTER_SEC = 0.12

# Market session (IST)
IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
SESSION_START = dt.time(9, 15)
SESSION_END = dt.time(15, 30)

# Drawing: candle width in days (1 minute = 1/(24*60))
CANDLE_WIDTH = (1.0 / (24 * 60)) * 0.70

# Styling
UP_COLOR = "#1a7f37"     # green-ish
DOWN_COLOR = "#d1242f"   # red-ish
WICK_ALPHA = 0.85
BODY_ALPHA = 0.70

# X ticks sparsity
XTICK_MINUTE_INTERVAL = 30


# ==========================================================
# Logging
# ==========================================================

def log(level: str, msg: str):
    now = dt.datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    print(f"{now} [{level}] {msg}")


# ==========================================================
# Rate limiter
# ==========================================================

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
            wait_for = self.per_seconds - (now - self.calls[0]) + 0.01
            time.sleep(max(0.01, wait_for))


rate_limiter = RateLimiter(HIST_MAX_CALLS_PER_SEC, 1.0)


# ==========================================================
# Pivots
# ==========================================================

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


# ==========================================================
# Time helpers
# ==========================================================

def now_ist() -> dt.datetime:
    return dt.datetime.now(IST)

def market_session_bounds(d: dt.date) -> Tuple[dt.datetime, dt.datetime]:
    return (
        dt.datetime.combine(d, SESSION_START, tzinfo=IST),
        dt.datetime.combine(d, SESSION_END, tzinfo=IST),
    )

def is_market_open(ts: dt.datetime) -> bool:
    s, e = market_session_bounds(ts.date())
    return s <= ts <= e

def clamp_intraday_window(end_dt: dt.datetime, lookback_min: int) -> Tuple[dt.datetime, dt.datetime]:
    s, e = market_session_bounds(end_dt.date())
    to_dt = min(end_dt, e)
    from_dt = to_dt - dt.timedelta(minutes=lookback_min)
    if from_dt < s:
        from_dt = s
    return from_dt, to_dt

def full_day_window(d: dt.date) -> Tuple[dt.datetime, dt.datetime]:
    return market_session_bounds(d)


# ==========================================================
# Kite helpers
# ==========================================================

def safe_historical_data(kite, token: int, from_dt: dt.datetime, to_dt: dt.datetime, interval: str, label: str):
    for attempt in range(1, MAX_RETRIES + 1):
        rate_limiter.wait()
        try:
            rows = kite.historical_data(
                instrument_token=token,
                from_date=from_dt,
                to_date=to_dt,
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


# ==========================================================
# Matplotlib candlestick + pivots
# ==========================================================

def draw_candles(ax, df: pd.DataFrame):
    # Convert times to matplotlib date float
    x = mdates.date2num(df["date"].dt.tz_convert(IST).dt.to_pydatetime())
    o = df["open"].to_numpy()
    h = df["high"].to_numpy()
    l = df["low"].to_numpy()
    c = df["close"].to_numpy()

    for xi, oi, hi, li, ci in zip(x, o, h, l, c):
        up = ci >= oi
        col = UP_COLOR if up else DOWN_COLOR

        # wick
        ax.add_line(Line2D([xi, xi], [li, hi], color=col, linewidth=0.55, alpha=WICK_ALPHA))

        # body
        y0 = min(oi, ci)
        height = abs(ci - oi)
        if height < 1e-9:
            height = 1e-9
        rect = Rectangle(
            (xi - CANDLE_WIDTH / 2.0, y0),
            CANDLE_WIDTH,
            height,
            facecolor=col,
            edgecolor=col,
            linewidth=0.35,
            alpha=BODY_ALPHA
        )
        ax.add_patch(rect)

    ax.set_xlim(x[0], x[-1])

def draw_pivots(ax, piv: PivotLevels):
    # Dotted lines. Always draw all 5.
    ax.axhline(piv.R1, linewidth=0.7, linestyle=":", alpha=0.9)
    ax.axhline(piv.TC, linewidth=0.7, linestyle=":", alpha=0.9)
    ax.axhline(piv.P,  linewidth=0.7, linestyle=":", alpha=0.9)
    ax.axhline(piv.BC, linewidth=0.7, linestyle=":", alpha=0.9)
    ax.axhline(piv.S1, linewidth=0.7, linestyle=":", alpha=0.9)

def set_ylim_include_pivots(ax, df: pd.DataFrame, piv: Optional[PivotLevels]):
    lo = float(df["low"].min())
    hi = float(df["high"].max())
    if piv is not None:
        lo = min(lo, piv.S1, piv.BC, piv.P, piv.TC, piv.R1)
        hi = max(hi, piv.S1, piv.BC, piv.P, piv.TC, piv.R1)
    if hi <= lo:
        hi = lo + 1.0
    pad = (hi - lo) * 0.06
    ax.set_ylim(lo - pad, hi + pad)


# ==========================================================
# Pivot day logic
# ==========================================================

def determine_last_trading_day(kite, probe_token: int) -> dt.date:
    end_dt = now_ist()
    start_dt = end_dt - dt.timedelta(days=30)
    rows = safe_historical_data(kite, probe_token, start_dt, end_dt, "day", "probe day lastTD")
    df = rows_to_df(rows)
    if df.empty:
        return now_ist().date()
    df["d"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    return df["d"].dropna().iloc[-1]

def determine_ref_day_for_pivots(kite, probe_token: int, last_td: dt.date) -> dt.date:
    end_dt = now_ist()
    start_dt = end_dt - dt.timedelta(days=40)
    rows = safe_historical_data(kite, probe_token, start_dt, end_dt, "day", "probe day refDay")
    df = rows_to_df(rows)
    if df.empty:
        return last_td
    df["d"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    prevs = df[df["d"] < last_td]
    if prevs.empty:
        return last_td
    return prevs["d"].iloc[-1]

def load_pivots_for_ref_day(kite, sym_to_token: Dict[str, int], ref_day: dt.date) -> Dict[str, PivotLevels]:
    end_dt = now_ist()
    start_dt = end_dt - dt.timedelta(days=45)
    out: Dict[str, PivotLevels] = {}
    for sym in SYMBOLS_50:
        token = sym_to_token[sym]
        rows = safe_historical_data(kite, token, start_dt, end_dt, "day", f"{sym} day pivots")
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


# ==========================================================
# Rendering controller
# ==========================================================

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

    missing = [s for s in SYMBOLS_50 if s not in sym_to_token]
    if missing:
        raise ValueError(f"Symbols not found in NSE instruments: {missing}")

    probe_token = sym_to_token[SYMBOLS_50[0]]

    asof = now_ist()
    market_open = is_market_open(asof)

    # Determine last trading day + pivot ref day
    last_td = determine_last_trading_day(kite, probe_token)
    ref_day = determine_ref_day_for_pivots(kite, probe_token, last_td)
    log("INFO", f"last_trading_day={last_td} | pivot_ref_day={ref_day}")

    log("STEP", "Loading pivots for all symbols (once) ...")
    pivots = load_pivots_for_ref_day(kite, sym_to_token, ref_day)
    log("INFO", f"Pivots ready for {len(pivots)}/{len(SYMBOLS_50)} symbols.")

    # Matplotlib setup (single window)
    plt.ion()
    fig, axes = plt.subplots(ROWS, COLS, figsize=(24, 13), constrained_layout=True)

    # Pre-set tick locators/formatters once (faster than per-refresh)
    locator = mdates.MinuteLocator(interval=XTICK_MINUTE_INTERVAL)
    formatter = mdates.DateFormatter("%H:%M", tz=IST)

    for i in range(ROWS * COLS):
        ax = axes[i // COLS][i % COLS]
        ax.xaxis.set_major_locator(locator)
        ax.xaxis.set_major_formatter(formatter)

    def render(symbol_to_df: Dict[str, pd.DataFrame], title_line: str):
        fig.suptitle(title_line, fontsize=14)

        for i, sym in enumerate(SYMBOLS_50):
            ax = axes[i // COLS][i % COLS]
            ax.cla()  # slightly lighter than clear() in practice

            df = symbol_to_df.get(sym)
            ax.set_title(sym, fontsize=8, pad=1)

            if df is None or df.empty:
                ax.set_xticks([])
                ax.set_yticks([])
                continue

            draw_candles(ax, df)

            piv = pivots.get(sym)
            if piv is not None:
                draw_pivots(ax, piv)
                set_ylim_include_pivots(ax, df, piv)
            else:
                set_ylim_include_pivots(ax, df, None)

            ax.grid(True, linewidth=0.25, alpha=0.6)

            # Show x labels only on bottom row; y labels only on left column
            is_bottom = (i // COLS) == (ROWS - 1)
            is_left = (i % COLS) == 0
            ax.tick_params(axis="x", labelsize=7, labelbottom=is_bottom)
            ax.tick_params(axis="y", labelsize=7, labelleft=is_left)

            # Apply pre-created locator/formatter (cla() resets them)
            ax.xaxis.set_major_locator(locator)
            ax.xaxis.set_major_formatter(formatter)

        fig.canvas.draw()
        fig.canvas.flush_events()

    # ----------------------------------------------------------
    # MARKET CLOSED MODE: full-day snapshot once, then wait for close
    # ----------------------------------------------------------
    if not market_open:
        day_from, day_to = full_day_window(last_td)
        log("INFO", f"Market closed now. Fetching FULL DAY once for {last_td} and exiting after window close.")

        symbol_to_df: Dict[str, pd.DataFrame] = {}
        for idx, sym in enumerate(SYMBOLS_50, start=1):
            token = sym_to_token[sym]
            try:
                rows = safe_historical_data(
                    kite, token, day_from, day_to, "minute",
                    f"{sym} full-day minute {idx}/{len(SYMBOLS_50)}"
                )
                df = normalize_minute_df_ist(rows_to_df(rows))
                symbol_to_df[sym] = df
            except Exception as e:
                log("ERROR", f"{sym}: full-day fetch failed: {e}")
                symbol_to_df[sym] = pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

        title = f"FULL DAY | Day={last_td} | Pivots from={ref_day} | As-of={asof.strftime('%Y-%m-%d %H:%M IST')}"
        render(symbol_to_df, title)

        log("INFO", "Close the Matplotlib window to exit.")
        plt.ioff()
        plt.show()
        return

    # ----------------------------------------------------------
    # MARKET OPEN MODE: incremental loop
    # ----------------------------------------------------------
    log("INFO", "Market open. Starting refresh loop in Matplotlib window.")

    cache_df: Dict[str, pd.DataFrame] = {}
    last_candle_dt: Dict[str, dt.datetime] = {}

    while True:
        loop_start = time.time()
        asof = now_ist()

        if not is_market_open(asof):
            log("INFO", "Market closed during run. Exiting. Re-run after close for full-day snapshot.")
            break

        window_from, window_to = clamp_intraday_window(asof, LOOKBACK_MINUTES)
        symbol_to_df: Dict[str, pd.DataFrame] = {}

        for idx, sym in enumerate(SYMBOLS_50, start=1):
            token = sym_to_token[sym]
            try:
                # First fetch for symbol
                if sym not in cache_df or cache_df[sym].empty:
                    rows = safe_historical_data(
                        kite, token, window_from, window_to, "minute",
                        f"{sym} init minute {idx}/{len(SYMBOLS_50)}"
                    )
                    df = normalize_minute_df_ist(rows_to_df(rows))
                    cache_df[sym] = df
                    if not df.empty:
                        last_candle_dt[sym] = df["date"].iloc[-1]
                    symbol_to_df[sym] = df
                    continue

                last_dt = last_candle_dt.get(sym)
                inc_from = (last_dt + dt.timedelta(minutes=1)) if last_dt else window_from

                if inc_from < window_to:
                    rows = safe_historical_data(
                        kite, token, inc_from, window_to, "minute",
                        f"{sym} inc minute {idx}/{len(SYMBOLS_50)}"
                    )
                    inc = normalize_minute_df_ist(rows_to_df(rows))

                    if not inc.empty:
                        df = pd.concat([cache_df[sym], inc], ignore_index=True)
                        df = df.drop_duplicates(subset=["date"], keep="last").sort_values("date").reset_index(drop=True)

                        # Trim to last LOOKBACK_MINUTES
                        latest = df["date"].iloc[-1]
                        cutoff = latest - pd.Timedelta(minutes=LOOKBACK_MINUTES)
                        df = df[df["date"] >= cutoff].reset_index(drop=True)

                        cache_df[sym] = df
                        last_candle_dt[sym] = df["date"].iloc[-1]

                symbol_to_df[sym] = cache_df[sym]

            except Exception as e:
                log("ERROR", f"{sym}: minute fetch failed: {e}")
                symbol_to_df[sym] = cache_df.get(
                    sym, pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
                )

        title = f"INTRADAY LIVE | Window={LOOKBACK_MINUTES}m | Pivots from={ref_day} | As-of={asof.strftime('%H:%M:%S IST')}"
        render(symbol_to_df, title)

        elapsed = time.time() - loop_start
        sleep_for = max(1.0, REFRESH_SECONDS - elapsed)
        log("INFO", f"Cycle={elapsed:.1f}s | sleep={sleep_for:.1f}s")
        time.sleep(sleep_for)

    plt.ioff()
    plt.show()


if __name__ == "__main__":
    main()
