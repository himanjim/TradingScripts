#!/usr/bin/env python3
"""
Master-DF cache (Feather) for ultra-fast scans + Plotly charts.
Interactive loop: enter FROM and TO repeatedly to generate fresh charts
without restarting the script.

Adds rule:
- Gainers must have (first-candle-in-window) low == open
- Losers  must have (first-candle-in-window) high == open

DEPENDENCIES:
  pip install pyarrow plotly
"""

import os
import sys
import json
import glob
import hashlib
from typing import List, Tuple, Optional
from datetime import time
from time import perf_counter
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import numpy as np

# Plotly opens charts in your default browser (1 tab per chart)
import plotly.graph_objects as go
import plotly.io as pio
pio.renderers.default = "browser"

# ---------- CONFIG ----------
DATA_DIR       = r"1_min_data"            # folder with CSVs
TOP_N          = 10                       # leaderboard rows per window
CHARTS_TOP_N   = 3                        # charts to open per window
CHARTS_MODE    = "abs"                    # "gainers" | "losers" | "abs"
MAX_WORKERS    = min(12, (os.cpu_count() or 4) * 2)  # parallel CSV reads
MASTER_DIRNAME = ".master_cache"          # cache folder under DATA_DIR

# Float equality tolerance for low==open / high==open checks
PRICE_ATOL     = 1e-4
PRICE_RTOL     = 1e-6
# ----------------------------------------

# Require pyarrow (Feather backend)
try:
    import pyarrow  # noqa: F401
except Exception:
    print("Please install dependencies:\n  pip install pyarrow plotly", file=sys.stderr)
    sys.exit(1)

# Timezone (Asia/Kolkata)
try:
    from zoneinfo import ZoneInfo
    IST = ZoneInfo("Asia/Kolkata")
except Exception:
    import pytz
    IST = pytz.timezone("Asia/Kolkata")

# ---------- logging ----------
_T0 = perf_counter()
def log(msg: str) -> None:
    print(f"[{perf_counter()-_T0:6.2f}s] {msg}")
# -----------------------------

# CSV schema
USECOLS = ["instrument", "exchange", "date", "open", "high", "low", "close", "volume"]
DTYPES  = {"instrument": "string", "exchange": "string",
           "open": "float64", "high": "float64", "low": "float64", "close": "float64"}

# ----- I/O + master cache -----

def list_csvs(folder: str) -> List[str]:
    files = []
    for ext in ("*.csv", "*.CSV"):
        files.extend(glob.glob(os.path.join(folder, ext)))
    # de-dupe (Windows case-insensitive)
    seen, out = set(), []
    for p in files:
        k = os.path.normcase(os.path.abspath(p))
        if k not in seen:
            seen.add(k); out.append(k)
    return out

def folder_hash(csv_paths: List[str]) -> str:
    h = hashlib.sha1()
    for p in sorted(csv_paths):
        try:
            st = os.stat(p)
            h.update(p.encode("utf-8"))
            h.update(str(int(st.st_mtime)).encode("ascii"))
            h.update(str(st.st_size).encode("ascii"))
        except FileNotFoundError:
            h.update(p.encode("utf-8")); h.update(b"-1"); h.update(b"-1")
    return h.hexdigest()

def master_cache_paths(root: str, h: str) -> Tuple[str, str]:
    cache_dir = os.path.join(root, MASTER_DIRNAME)
    os.makedirs(cache_dir, exist_ok=True)
    return os.path.join(cache_dir, f"master_{h}.feather"), os.path.join(cache_dir, f"master_{h}.meta.json")

def prune_old_master_cache(root: str, keep_hash: str) -> None:
    cache_dir = os.path.join(root, MASTER_DIRNAME)
    if not os.path.isdir(cache_dir): return
    for fn in os.listdir(cache_dir):
        if fn.startswith("master_") and not fn.startswith(f"master_{keep_hash}"):
            try: os.remove(os.path.join(cache_dir, fn))
            except Exception: pass

def read_one_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, usecols=USECOLS, dtype=DTYPES, parse_dates=["date"], engine="c")
    # tz normalize
    if pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = df["date"].dt.tz_localize(IST) if df["date"].dt.tz is None else df["date"].dt.tz_convert(IST)
    else:
        df["date"] = pd.to_datetime(df["date"], utc=True).dt.tz_convert(IST)
    # numerics
    for c in ["open", "high", "low", "close", "volume"]:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")
    # drop unusable rows
    df = df.dropna(subset=["instrument", "exchange", "date", "open", "high", "low", "close"])
    return df.sort_values("date").reset_index(drop=True)

def build_master_df(csv_paths: List[str]) -> pd.DataFrame:
    log(f"Building master DataFrame from {len(csv_paths)} CSV(s) using {MAX_WORKERS} workers...")
    parts: List[pd.DataFrame] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = [ex.submit(read_one_csv, p) for p in csv_paths]
        for fut in as_completed(futs):
            try: parts.append(fut.result())
            except Exception as e: print(f"⚠️  Skipping a CSV due to: {e}", file=sys.stderr)
    if not parts:
        return pd.DataFrame(columns=USECOLS)

    df = pd.concat(parts, axis=0, ignore_index=True)
    # optimize dtypes
    df["instrument"] = df["instrument"].astype("category")
    df["exchange"]   = df["exchange"].astype("string")
    for c in ["open", "high", "low", "close", "volume"]:
        if c in df.columns: df[c] = df[c].astype("float32")
    # global sort for fast groupby-first/last
    df = df.sort_values(["instrument", "date"], kind="mergesort", ignore_index=True)
    log(f"Master DF ready: rows={len(df):,}, instruments={df['instrument'].nunique()}")
    return df

def save_master(df: pd.DataFrame, feather_path: str, meta_path: str, manifest_hash: str) -> None:
    df.to_feather(feather_path)
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump({"hash": manifest_hash, "rows": int(len(df)),
                   "instruments": int(df["instrument"].nunique()), "cols": list(map(str, df.columns))}, f)
    log(f"Saved master cache → {feather_path}")

def load_master(feather_path: str) -> pd.DataFrame:
    df = pd.read_feather(feather_path)
    df["instrument"] = df["instrument"].astype("category")
    df["exchange"]   = df["exchange"].astype("string")
    for c in ["open", "high", "low", "close", "volume"]:
        if c in df.columns: df[c] = df[c].astype("float32")
    return df

def ensure_master(DATA_DIR: str) -> Tuple[pd.DataFrame, str]:
    csvs = list_csvs(DATA_DIR)
    if not csvs:
        print("No CSVs found.", file=sys.stderr); sys.exit(1)
    log(f"Found {len(csvs)} CSV file(s).")
    h = folder_hash(csvs)
    feather_path, meta_path = master_cache_paths(DATA_DIR, h)
    if os.path.isfile(feather_path):
        log("Cache hit: loading master DataFrame from Feather...")
        return load_master(feather_path), h
    # rebuild
    df = build_master_df(csvs)
    if df.empty:
        print("Master DataFrame is empty after reading CSVs.", file=sys.stderr); sys.exit(1)
    save_master(df, feather_path, meta_path, h)
    prune_old_master_cache(DATA_DIR, h)
    return df, h

# ----- analytics: movers & charts -----

def compute_movers(df: pd.DataFrame, from_ts: pd.Timestamp, to_ts: pd.Timestamp) -> pd.DataFrame:
    """
    Vectorized movers within [from_ts, to_ts]; also compute the first-candle low/high
    inside the window so we can enforce:
      - gainers: low == open on first window candle
      - losers : high == open on first window candle
    """
    m = (df["date"] >= from_ts) & (df["date"] <= to_ts)
    w = df.loc[m, ["instrument", "date", "open", "high", "low", "close"]]
    if w.empty:
        return pd.DataFrame(columns=[
            "instrument","open_window","ltp_window","%chg",
            "first_ts","last_ts","rows",
            "first_low","first_high","lo_eq_open","hi_eq_open"
        ])

    agg = (w.groupby("instrument", observed=True)
             .agg(open_window=("open", "first"),
                  ltp_window=("close", "last"),
                  first_ts=("date", "first"),
                  last_ts=("date", "last"),
                  rows=("close", "size"),
                  first_low=("low", "first"),
                  first_high=("high", "first"))
           ).reset_index()

    agg["%chg"] = (agg["ltp_window"] - agg["open_window"]) / agg["open_window"] * 100.0
    agg["lo_eq_open"] = np.isclose(agg["first_low"],  agg["open_window"], atol=PRICE_ATOL, rtol=PRICE_RTOL)
    agg["hi_eq_open"] = np.isclose(agg["first_high"], agg["open_window"], atol=PRICE_ATOL, rtol=PRICE_RTOL)
    # Sort by % change desc (gainers first, losers later)
    agg = agg.sort_values("%chg", ascending=False, ignore_index=True)
    return agg

def enforce_open_wick_rule(df_moves: pd.DataFrame, mode: Optional[str] = None) -> pd.DataFrame:
    """
    Apply the rule:
      - gainers must have lo==open on first window candle
      - losers  must have hi==open on first window candle
    mode:
      'gainers' → keep only positive movers with lo_eq_open
      'losers'  → keep only negative movers with hi_eq_open
      None or 'abs' → keep both sides with their respective rules
    """
    if df_moves.empty:
        return df_moves

    if mode and mode.lower() == "gainers":
        return df_moves[(df_moves["%chg"] > 0) & (df_moves["lo_eq_open"])].copy()
    if mode and mode.lower() == "losers":
        return df_moves[(df_moves["%chg"] < 0) & (df_moves["hi_eq_open"])].copy()

    # abs / None -> keep both with their own rule
    pos = df_moves[(df_moves["%chg"] > 0) & (df_moves["lo_eq_open"])]
    neg = df_moves[(df_moves["%chg"] < 0) & (df_moves["hi_eq_open"])]
    out = pd.concat([pos, neg], ignore_index=True)
    # Keep gainers first by default (desc), losers will be at the bottom
    out = out.sort_values("%chg", ascending=False, ignore_index=True)
    return out

def pick_charts(df_moves: pd.DataFrame, mode: str, k: int) -> pd.DataFrame:
    if df_moves.empty or k <= 0:
        return df_moves.iloc[0:0]
    mode = (mode or "abs").lower()
    if mode == "gainers":
        return df_moves.head(k)
    if mode == "losers":
        return df_moves.sort_values("%chg", ascending=True).head(k)
    # absolute movers from the rule-enforced set
    return (df_moves.assign(_abs=np.abs(df_moves["%chg"]))
                    .sort_values("_abs", ascending=False)
                    .head(k).drop(columns="_abs"))

def day_bounds(ts: pd.Timestamp) -> Tuple[pd.Timestamp, pd.Timestamp]:
    d = ts.tz_convert(IST).date()
    start = pd.Timestamp.combine(d, time(9, 15)).tz_localize(IST)
    end   = pd.Timestamp.combine(d, time(15, 30)).tz_localize(IST)
    return start, end

def make_fig(day_df: pd.DataFrame, title: str,
             from_ts: pd.Timestamp, to_ts: pd.Timestamp) -> Optional[go.Figure]:
    if day_df.empty: return None
    x = day_df["date"].dt.tz_convert(IST).dt.tz_localize(None)
    from_local = from_ts.tz_convert(IST).tz_localize(None).to_pydatetime()
    to_local   = to_ts.tz_convert(IST).tz_localize(None).to_pydatetime()
    day_start_local = x.iloc[0].to_pydatetime().replace(second=0, microsecond=0)
    day_end_local   = x.iloc[-1].to_pydatetime().replace(second=0, microsecond=0)
    fig = go.Figure([go.Candlestick(
        x=x, open=day_df["open"], high=day_df["high"], low=day_df["low"], close=day_df["close"], name=title
    )])
    fig.add_vrect(x0=from_local, x1=to_local, fillcolor="LightSkyBlue",
                  opacity=0.25, line_width=0, annotation_text="Window",
                  annotation_position="top left")
    fig.update_layout(
        title=title, xaxis_title="Time (Asia/Kolkata)", yaxis_title="Price",
        xaxis_range=[day_start_local, day_end_local],
        xaxis_rangeslider_visible=False, template="plotly_white",
        margin=dict(l=40, r=20, t=60, b=40))
    return fig

def open_charts_for_window(df_master: pd.DataFrame, picks: pd.DataFrame,
                           from_ts: pd.Timestamp, to_ts: pd.Timestamp) -> None:
    if picks.empty: return
    day_start, day_end = day_bounds(from_ts)
    for _, row in picks.iterrows():
        inst = row["instrument"]
        day_df = df_master[(df_master["instrument"] == inst) &
                           (df_master["date"] >= day_start) &
                           (df_master["date"] <= day_end)].copy()
        fig = make_fig(day_df, f"{inst} — Δ {row['%chg']:+.2f}% (window)", from_ts, to_ts)
        if fig is not None: fig.show()

# ----- interactive loop -----

def prompt_one_window(prev_from: Optional[pd.Timestamp], prev_to: Optional[pd.Timestamp]) -> Optional[Tuple[pd.Timestamp, pd.Timestamp]]:
    """
    Returns (from_ts, to_ts) or None to exit.
    - If user presses Enter on FROM, reuse previous FROM (if available).
    - If user presses Enter on TO, reuse previous TO (if available).
    - Typing 'done'/'q' on FROM exits.
    """
    print("\nEnter a date window (Asia/Kolkata). Format: YYYY-MM-DD HH:MM")
    pf = f" [{prev_from.strftime('%Y-%m-%d %H:%M')}]" if prev_from is not None else ""
    pt = f" [{prev_to.strftime('%Y-%m-%d %H:%M')}]"   if prev_to   is not None else ""

    line_from = input(f"  From{pf}: ").strip()
    if line_from.lower() in {"done", "q", "quit", "exit"}:
        return None
    if line_from == "":
        if prev_from is None:
            print("  ⚠️  No previous FROM. Please enter a value or type 'done'.")
            return prompt_one_window(prev_from, prev_to)
        f_ts = prev_from
    else:
        try:
            f_ts = pd.to_datetime(line_from, format="%Y-%m-%d %H:%M").tz_localize(IST)
        except Exception as e:
            print(f"  ⚠️  Invalid FROM ({e}). Try again.")
            return prompt_one_window(prev_from, prev_to)

    line_to = input(f"  To  {pt}: ").strip()
    if line_to == "":
        if prev_to is None:
            print("  ⚠️  No previous TO. Please enter a value.")
            return prompt_one_window(f_ts, prev_to)
        t_ts = prev_to
    else:
        try:
            t_ts = pd.to_datetime(line_to, format="%Y-%m-%d %H:%M").tz_localize(IST)
        except Exception as e:
            print(f"  ⚠️  Invalid TO ({e}). Try again.")
            return prompt_one_window(f_ts, prev_to)

    if t_ts < f_ts:
        print("  ⚠️  TO must be >= FROM. Try again.")
        return prompt_one_window(prev_from, prev_to)

    print(f"  ✅ Using window: {f_ts} → {t_ts}")
    return f_ts, t_ts

# ----- main -----

def main():
    log("=== Master-DF scanner (interactive loop + open-wick rule) started ===")
    if not os.path.isdir(DATA_DIR):
        print(f"DATA_DIR not found: {DATA_DIR}", file=sys.stderr); sys.exit(1)

    # Load or build the master DF ONCE
    t0 = perf_counter()
    master, h = ensure_master(DATA_DIR)
    log(f"Master load/build completed in {perf_counter()-t0:0.2f}s. (hash={h[:12]}…)\n")

    # REPL loop: ask window → compute → chart → ask again
    prev_from: Optional[pd.Timestamp] = None
    prev_to:   Optional[pd.Timestamp] = None

    while True:
        res = prompt_one_window(prev_from, prev_to)
        if res is None:
            log("Exiting.")
            break
        from_ts, to_ts = res
        prev_from, prev_to = from_ts, to_ts

        # Compute movers
        t1 = perf_counter()
        movers_raw = compute_movers(master, from_ts, to_ts)
        movers = enforce_open_wick_rule(movers_raw, mode=None if CHARTS_MODE.lower()=="abs" else CHARTS_MODE)
        log(f"Mover computation: {perf_counter()-t1:0.2f}s; eligible instruments={len(movers)}")

        if movers.empty:
            print("No instruments meet the rule within this window (low=open for gainers, high=open for losers).")
            continue

        # Leaderboard (show if each side met the rule)
        top_n = int(TOP_N) if TOP_N and TOP_N > 0 else 10
        disp_cols = ["instrument","open_window","ltp_window","%chg","first_ts","last_ts","rows","lo_eq_open","hi_eq_open"]
        print(f"\n=== Leaderboard (Top {top_n}) — rule applied (L=O for +ve, H=O for -ve) ===")
        pd.set_option("display.width", 180); pd.set_option("display.max_columns", None)
        df_print = movers.loc[:, disp_cols].copy()
        # Convert booleans to short flags for readability
        df_print["lo_eq_open"] = df_print["lo_eq_open"].map({True:"Y", False:""})
        df_print["hi_eq_open"] = df_print["hi_eq_open"].map({True:"Y", False:""})
        print(df_print.head(top_n).to_string(index=False,
              justify="left", float_format=lambda x: f"{x:.2f}"))

        # Charts for this window
        # For chart picks we still honor CHARTS_MODE ordering
        chart_base = enforce_open_wick_rule(movers_raw, mode=CHARTS_MODE)
        picks = pick_charts(chart_base, CHARTS_MODE, CHARTS_TOP_N)
        if CHARTS_TOP_N > 0 and not picks.empty:
            log(f"Opening {len(picks)} chart(s) in browser…")
            open_charts_for_window(master, picks, from_ts, to_ts)
        elif CHARTS_TOP_N > 0:
            log("No eligible symbols to chart for the selected mode.")

        # Quick summary
        best = movers.iloc[0]
        worst = movers.iloc[movers["%chg"].idxmin()]
        print(f"\nTop Gainer (eligible): {best['instrument']} ({best['%chg']:+.2f}%)")
        print(f"Top Loser  (eligible): {worst['instrument']} ({worst['%chg']:+.2f}%)")
        print("\n— Enter another window or type 'done' to quit —")

if __name__ == "__main__":
    main()
