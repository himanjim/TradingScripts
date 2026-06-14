"""
Scan 3 years of NIFTY 50 1-minute data for sustained DIRECTIONAL moves
(like the chart you showed) and simulate how a trend-following exit would
have performed.

Run LOCALLY:
    python scan_directional_trends.py NIFTY50_1min_YYYYMMDD_to_YYYYMMDD.parquet

It reads the parquet/csv produced by download_nifty50_1min.py and writes:
    - <input>_trends.csv        one row per detected directional episode
    - <input>_summary.txt       aggregate stats
Upload those two back here and I'll tune the parameters.

------------------------------------------------------------------
DEFINITIONS (iteration 1 -- deliberately simple, easy to reason about)
------------------------------------------------------------------
This is intraday only: every episode lives inside a single trading day
(no overnight carry), because that matches your 1-min chart use case.

Entry (a directional move is considered "started") when, on a bar:
  * EMA stack is ordered and fanning in one direction
      up:   ema_fast > ema_mid > ema_slow  and  slope(ema_slow) > 0
      down: ema_fast < ema_mid < ema_slow  and  slope(ema_slow) < 0
  * price has broken the rolling high/low of the last BREAKOUT_LOOKBACK bars
    (confirms momentum, not just drift)
  * the move isn't already old (we only enter near the start of a fan)

Exit (reversal signs) -- whichever triggers first:
  1. Trailing stop: close crosses back through a chandelier-style trail
     (extreme price minus ATR_MULT * ATR).
  2. EMA stack breaks: ema_fast crosses ema_mid against the trade.
  3. Hard time stop at session end (square off before close).
  4. Max-loss stop from entry (safety).

Everything is vectorised where cheap and looped where state is needed.
Parameters are at the top so we can sweep them next round.
"""

import sys
import os
import numpy as np
import pandas as pd


# ============================================================
# PARAMETERS  (iteration 2)
# ------------------------------------------------------------
# Iteration 1 fired 4148 times but 78% had MFE<0.20% -- mostly noise/drift,
# and median capture of the favorable move was only ~4%. Meanwhile the 33
# genuine big runs (MFE>0.8%) captured ~72%. So: filter HARD for real momentum
# at entry, and let the trail breathe so big runs aren't choked off.
# ============================================================
EMA_FAST = 9
EMA_MID = 21
EMA_SLOW = 50

SLOPE_LOOKBACK = 5          # bars used to measure ema_slow slope
BREAKOUT_LOOKBACK = 20      # walk-forward optimized (chosen in 71% of OOS windows)
ATR_PERIOD = 14
# --- tiered trailing stop (iteration 3) ---
# Problem: a flat 3.0x ATR trail captured only 35% of peak on trail_stop exits
# (avg giveback 0.19%). Solution: start loose so young trades aren't shaken out,
# then tighten the trail as unrealized profit grows, locking in more of the run.
# Each tuple: (favorable_profit_pct_threshold, atr_multiplier_to_use)
# evaluated top-down; first row whose threshold <= current favorable profit wins.
TRAIL_TIERS = [
    (0.0100, 1.2),   # in >=1.0% profit: trail very tight, protect the big win
    (0.0060, 1.6),   # >=0.6%
    (0.0035, 2.2),   # >=0.35%
    (0.0000, 3.0),   # default early on: loose, let it develop
]
MAX_LOSS_PCT = 0.004        # 0.4% hard stop from entry
MIN_TREND_BARS = 10         # ignore episodes shorter than this (noise)
COOLDOWN_BARS = 5           # bars to wait after an exit before re-entering

# --- new entry-quality gates (iteration 2) ---
ADX_PERIOD = 14
MIN_ADX = 25.0              # walk-forward optimized (stricter filter; was 20)
MIN_FAN_PCT = 0.0008        # ema_fast vs ema_slow must be separated >=0.08% (fan, not flat)
ATR_EXPANSION = 1.10        # current ATR must be >=1.10x its own 30-bar average (volatility kicking in)
ATR_EXP_LOOKBACK = 30

# --- early-cull rule (iteration 2): losers reveal themselves fast ---
# Diagnosis: trades that never reach MIN_PROGRESS_PCT within PROGRESS_BARS
# almost always bleed out. Cut them early instead of feeding the trail.
PROGRESS_BARS = 8
MIN_PROGRESS_PCT = 0.0010   # need +0.10% favorable by bar 8, else exit
BREAKEVEN_AFTER_PCT = 0.0015  # once +0.15% reached, move stop to entry (no give-back to red)

SESSION_START = "09:15"
SESSION_END = "15:30"
SQUAREOFF_TIME = "15:20"    # force-exit before the close

# --- iteration 4: realism + validation ---
# Per-trade round-trip cost as a fraction of notional, subtracted from each
# trade's pnl_pct. Covers brokerage + STT + exchange/GST + slippage.
# 0.03% is a reasonable mid estimate for an intraday NIFTY futures round trip;
# options or thinner fills can be higher. Tune to your actual broker.
COST_PCT_PER_TRADE = 0.0003     # 0.03% round trip

# Out-of-sample split. Trades with entry date < TRAIN_TEST_SPLIT are "train"
# (the period whose behaviour informed the rules); on/after is "test" (unseen).
# The RULES ARE IDENTICAL for both -- we only report them separately so we can
# see whether the edge survives on data that didn't shape the parameters.
TRAIN_TEST_SPLIT = "2025-01-01"


# ============================================================
# LOADING
# ============================================================
def load(path: str) -> pd.DataFrame:
    if path.lower().endswith(".parquet"):
        df = pd.read_parquet(path)
    elif path.lower().endswith((".csv", ".txt")):
        df = pd.read_csv(path)
    elif path.lower().endswith((".pkl", ".pickle")):
        df = pd.read_pickle(path)
    else:
        raise ValueError("Unsupported file. Use .parquet, .csv, or .pkl")
    df.columns = [c.lower() for c in df.columns]
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").drop_duplicates("date").reset_index(drop=True)
    df["day"] = df["date"].dt.date
    df["t"] = df["date"].dt.strftime("%H:%M")
    return df


# ============================================================
# INDICATORS (per-day to avoid bleeding across sessions)
# ============================================================
def add_indicators(g: pd.DataFrame) -> pd.DataFrame:
    g = g.copy()
    c = g["close"]
    g["ema_f"] = c.ewm(span=EMA_FAST, adjust=False).mean()
    g["ema_m"] = c.ewm(span=EMA_MID, adjust=False).mean()
    g["ema_s"] = c.ewm(span=EMA_SLOW, adjust=False).mean()
    g["slope_s"] = g["ema_s"].diff(SLOPE_LOOKBACK)

    prev_c = c.shift(1)
    tr = pd.concat([
        g["high"] - g["low"],
        (g["high"] - prev_c).abs(),
        (g["low"] - prev_c).abs(),
    ], axis=1).max(axis=1)
    g["atr"] = tr.ewm(span=ATR_PERIOD, adjust=False).mean()

    g["roll_hi"] = g["high"].rolling(BREAKOUT_LOOKBACK).max()
    g["roll_lo"] = g["low"].rolling(BREAKOUT_LOOKBACK).min()

    # --- fan width: separation between fast and slow EMA, as fraction of price ---
    g["fan_pct"] = (g["ema_f"] - g["ema_s"]).abs() / c

    # --- ATR expansion: current ATR vs its own recent average ---
    g["atr_avg"] = g["atr"].rolling(ATR_EXP_LOOKBACK).mean()
    g["atr_ratio"] = g["atr"] / g["atr_avg"]

    # --- ADX (Wilder) for trend-strength regime filter ---
    up_move = g["high"].diff()
    down_move = -g["low"].diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    atr_w = tr.ewm(alpha=1 / ADX_PERIOD, adjust=False).mean()
    plus_di = 100 * pd.Series(plus_dm, index=g.index).ewm(alpha=1 / ADX_PERIOD, adjust=False).mean() / atr_w
    minus_di = 100 * pd.Series(minus_dm, index=g.index).ewm(alpha=1 / ADX_PERIOD, adjust=False).mean() / atr_w
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    g["adx"] = dx.ewm(alpha=1 / ADX_PERIOD, adjust=False).mean()
    return g


# ============================================================
# EPISODE DETECTION + EXIT SIMULATION (per day)
# ============================================================
def scan_day(g: pd.DataFrame):
    g = add_indicators(g).reset_index(drop=True)
    n = len(g)
    episodes = []
    i = BREAKOUT_LOOKBACK + SLOPE_LOOKBACK
    last_exit_idx = -10_000

    while i < n - 1:
        row = g.iloc[i]
        if i - last_exit_idx < COOLDOWN_BARS:
            i += 1
            continue
        if pd.isna(row["slope_s"]) or pd.isna(row["roll_hi"]) \
                or pd.isna(row["adx"]) or pd.isna(row["atr_ratio"]):
            i += 1
            continue

        # entry-quality gates shared by both directions
        regime_ok = (row["adx"] >= MIN_ADX) and (row["fan_pct"] >= MIN_FAN_PCT) \
            and (row["atr_ratio"] >= ATR_EXPANSION)

        up = regime_ok and (row["ema_f"] > row["ema_m"] > row["ema_s"]) and (row["slope_s"] > 0) \
            and (row["close"] >= g.iloc[i - 1]["roll_hi"])
        down = regime_ok and (row["ema_f"] < row["ema_m"] < row["ema_s"]) and (row["slope_s"] < 0) \
            and (row["close"] <= g.iloc[i - 1]["roll_lo"])

        if not (up or down):
            i += 1
            continue

        direction = "up" if up else "down"
        entry_idx = i
        entry_px = row["close"]
        entry_time = row["date"]
        extreme = entry_px            # for chandelier trail
        exit_idx = None
        exit_reason = None

        j = i + 1
        be_armed = False        # breakeven stop engaged once enough progress made
        while j < n:
            r = g.iloc[j]
            held = j - entry_idx
            # square-off time check
            if r["t"] >= SQUAREOFF_TIME:
                exit_idx, exit_reason = j, "session_squareoff"
                break

            if direction == "up":
                extreme = max(extreme, r["high"])
                fav = (extreme - entry_px) / entry_px
                if fav >= BREAKEVEN_AFTER_PCT:
                    be_armed = True
                atr_mult = next(m for thr, m in TRAIL_TIERS if fav >= thr)
                trail = extreme - atr_mult * r["atr"]
                # early cull: no progress by PROGRESS_BARS -> bail
                if held >= PROGRESS_BARS and fav < MIN_PROGRESS_PCT:
                    exit_idx, exit_reason = j, "no_progress"; break
                if be_armed and r["close"] <= entry_px:
                    exit_idx, exit_reason = j, "breakeven"; break
                if r["close"] <= entry_px * (1 - MAX_LOSS_PCT):
                    exit_idx, exit_reason = j, "max_loss"; break
                if r["close"] < trail:
                    exit_idx, exit_reason = j, "trail_stop"; break
                if r["ema_f"] < r["ema_m"]:
                    exit_idx, exit_reason = j, "ema_break"; break
            else:
                extreme = min(extreme, r["low"])
                fav = (entry_px - extreme) / entry_px
                if fav >= BREAKEVEN_AFTER_PCT:
                    be_armed = True
                atr_mult = next(m for thr, m in TRAIL_TIERS if fav >= thr)
                trail = extreme + atr_mult * r["atr"]
                if held >= PROGRESS_BARS and fav < MIN_PROGRESS_PCT:
                    exit_idx, exit_reason = j, "no_progress"; break
                if be_armed and r["close"] >= entry_px:
                    exit_idx, exit_reason = j, "breakeven"; break
                if r["close"] >= entry_px * (1 + MAX_LOSS_PCT):
                    exit_idx, exit_reason = j, "max_loss"; break
                if r["close"] > trail:
                    exit_idx, exit_reason = j, "trail_stop"; break
                if r["ema_f"] > r["ema_m"]:
                    exit_idx, exit_reason = j, "ema_break"; break
            j += 1

        if exit_idx is None:
            exit_idx, exit_reason = n - 1, "data_end"

        ex = g.iloc[exit_idx]
        bars = exit_idx - entry_idx
        if bars >= MIN_TREND_BARS:
            pnl_pts = (ex["close"] - entry_px) if direction == "up" else (entry_px - ex["close"])
            pnl_pct_gross = pnl_pts / entry_px
            pnl_pct = pnl_pct_gross - COST_PCT_PER_TRADE   # net of round-trip cost
            # max favourable / adverse excursion during the hold
            seg = g.iloc[entry_idx:exit_idx + 1]
            if direction == "up":
                peak_px = seg["high"].max()
                peak_idx = seg["high"].idxmax()
                mfe = (peak_px - entry_px) / entry_px
                mae = (seg["low"].min() - entry_px) / entry_px
                peak_pts = peak_px - entry_px
            else:
                peak_px = seg["low"].min()
                peak_idx = seg["low"].idxmin()
                mfe = (entry_px - peak_px) / entry_px
                mae = (entry_px - seg["high"].max()) / entry_px
                peak_pts = entry_px - peak_px
            bars_to_peak = int(peak_idx - entry_idx)
            capture = (pnl_pct_gross / mfe) if mfe > 0 else 0.0
            split = "train" if str(entry_time) < TRAIN_TEST_SPLIT else "test"
            episodes.append({
                "day": g.iloc[0]["day"],
                "split": split,
                "direction": direction,
                "entry_time": entry_time,
                "exit_time": ex["date"],
                "bars": bars,
                "entry_px": round(entry_px, 2),
                "exit_px": round(ex["close"], 2),
                "pnl_pts": round(pnl_pts, 2),
                "pnl_pct_gross": round(pnl_pct_gross * 100, 3),
                "pnl_pct": round(pnl_pct * 100, 3),   # NET of costs
                # --- highest profit reached during the trade (peak unrealized) ---
                "peak_profit_pts": round(peak_pts, 2),
                "peak_profit_pct": round(mfe * 100, 3),
                "bars_to_peak": bars_to_peak,
                "capture_ratio": round(capture, 3),   # realized / peak
                "mfe_pct": round(mfe * 100, 3),
                "mae_pct": round(mae * 100, 3),
                "adx_at_entry": round(row["adx"], 1),
                "exit_reason": exit_reason,
            })

        last_exit_idx = exit_idx
        i = exit_idx + 1

    return episodes


def main():
    if len(sys.argv) < 2:
        path = r"C:\Users\himan\PycharmProjects\TradingScripts\Trading_2024\back_testing\nifty50_1min_history\NIFTY50_1min_20210614_to_20260613.parquet"
        print("Usage: python scan_directional_trends.py <data_file.parquet|csv|pkl>")
        # sys.exit(1)
    else:
        path = sys.argv[1]
    df = load(path)
    print(f"[INFO] {len(df)} candles, {df['day'].nunique()} days, "
          f"{df['date'].min()} -> {df['date'].max()}")

    all_eps = []
    for _, g in df.groupby("day"):
        if len(g) < BREAKOUT_LOOKBACK + EMA_SLOW:
            continue
        all_eps.extend(scan_day(g))

    res = pd.DataFrame(all_eps)
    base = os.path.splitext(path)[0]
    trends_path = base + "_trends.csv"
    summary_path = base + "_summary.txt"

    if res.empty:
        print("[WARN] No episodes detected with current params.")
        open(summary_path, "w").write("No episodes detected.\n")
        return

    res = res.sort_values("entry_time").reset_index(drop=True)
    res.to_csv(trends_path, index=False)

    # ---- summary ----
    wins = res[res["pnl_pct"] > 0]
    lines = []
    lines.append(f"File: {path}")
    lines.append(f"Cost per trade applied: {COST_PCT_PER_TRADE*100:.3f}% (round trip). All pnl below is NET.")
    lines.append(f"Episodes detected: {len(res)}")
    lines.append(f"  up:   {(res['direction']=='up').sum()}")
    lines.append(f"  down: {(res['direction']=='down').sum()}")
    lines.append(f"Win rate (net): {len(wins)/len(res)*100:.1f}%")
    lines.append(f"Avg pnl %% (net):  {res['pnl_pct'].mean():.3f}   (gross {res['pnl_pct_gross'].mean():.3f})")
    lines.append(f"Median pnl %% (net): {res['pnl_pct'].median():.3f}")
    lines.append(f"Total pnl %% (sum, net): {res['pnl_pct'].sum():.2f}   (gross {res['pnl_pct_gross'].sum():.2f})")
    lines.append(f"Avg bars held: {res['bars'].mean():.1f}")
    lines.append(f"Avg MFE %: {res['mfe_pct'].mean():.3f}   Avg MAE %: {res['mae_pct'].mean():.3f}")
    lines.append("")

    # ---- OUT-OF-SAMPLE: train vs test ----
    lines.append(f"--- OUT-OF-SAMPLE VALIDATION (split at {TRAIN_TEST_SPLIT}) ---")
    lines.append("Same rules on both periods. 'test' data did NOT shape the parameters.")
    lines.append(f"{'period':6s} {'trades':>6s} {'win%':>6s} {'avg%':>7s} {'med%':>7s} {'sum%':>8s} {'expectancy':>10s}")
    for period in ["train", "test"]:
        s = res[res["split"] == period]
        if len(s) == 0:
            lines.append(f"{period:6s}  (no trades)")
            continue
        wr = (s["pnl_pct"] > 0).mean() * 100
        lines.append(f"{period:6s} {len(s):6d} {wr:6.0f} {s['pnl_pct'].mean():7.3f} "
                     f"{s['pnl_pct'].median():7.3f} {s['pnl_pct'].sum():8.2f} {s['pnl_pct'].mean():10.3f}")
    lines.append("")
    lines.append("--- HIGHEST PROFIT / CAPTURE ---")
    lines.append(f"Highest peak profit reached (single episode): {res['peak_profit_pct'].max():.3f}% "
                 f"({res['peak_profit_pts'].max():.1f} pts)")
    lines.append(f"Avg peak profit reached: {res['peak_profit_pct'].mean():.3f}%")
    lines.append(f"Median capture ratio (realized/peak): {res['capture_ratio'].median():.2f}")
    lines.append(f"Avg bars to peak: {res['bars_to_peak'].mean():.1f}  (vs avg bars held {res['bars'].mean():.1f})")
    bigmoves = res[res['mfe_pct'] > 0.8]
    lines.append(f"Big runs (peak>0.8%): {len(bigmoves)}  "
                 f"median capture {((bigmoves['pnl_pct']/bigmoves['mfe_pct']).median() if len(bigmoves) else 0):.2f}")
    lines.append("")
    lines.append("Exit reason breakdown:")
    for reason, cnt in res["exit_reason"].value_counts().items():
        sub = res[res["exit_reason"] == reason]
        lines.append(f"  {reason:18s} n={cnt:4d}  avg_pnl%={sub['pnl_pct'].mean():.3f}")
    lines.append("")
    lines.append("Best 5 episodes:")
    for _, r in res.nlargest(5, "pnl_pct").iterrows():
        lines.append(f"  {r['day']} {r['direction']:4s} {r['pnl_pct']:+.2f}% in {r['bars']} bars ({r['exit_reason']})")
    lines.append("Worst 5 episodes:")
    for _, r in res.nsmallest(5, "pnl_pct").iterrows():
        lines.append(f"  {r['day']} {r['direction']:4s} {r['pnl_pct']:+.2f}% in {r['bars']} bars ({r['exit_reason']})")

    # ---- monthly breakdown (is the edge sustainable month to month?) ----
    res["month"] = pd.to_datetime(res["entry_time"]).dt.to_period("M").astype(str)
    lines.append("")
    lines.append("--- MONTHLY SUMMARY ---")
    lines.append(f"{'month':9s} {'trades':>6s} {'win%':>6s} {'sum%':>8s} {'avg%':>7s} {'med%':>7s}")
    monthly = []
    for m, s in res.groupby("month"):
        wr = (s["pnl_pct"] > 0).mean() * 100
        lines.append(f"{m:9s} {len(s):6d} {wr:6.0f} {s['pnl_pct'].sum():8.2f} "
                     f"{s['pnl_pct'].mean():7.3f} {s['pnl_pct'].median():7.3f}")
        monthly.append(s["pnl_pct"].sum())
    monthly = pd.Series(monthly)
    lines.append("")
    lines.append(f"Months total: {len(monthly)}   profitable: {(monthly>0).sum()} "
                 f"({(monthly>0).mean()*100:.0f}%)   losing: {(monthly<=0).sum()}")
    lines.append(f"Monthly sum%  -> best {monthly.max():.2f}  worst {monthly.min():.2f}  "
                 f"avg {monthly.mean():.2f}  median {monthly.median():.2f}")
    lines.append(f"Std of monthly sum%: {monthly.std():.2f}  "
                 f"(consistency: lower is steadier)")

    summary = "\n".join(lines)
    open(summary_path, "w").write(summary + "\n")
    print(summary)
    print(f"\n[DONE] {trends_path}")
    print(f"[DONE] {summary_path}")


if __name__ == "__main__":
    main()
