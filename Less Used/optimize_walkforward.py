"""
Walk-forward optimizer for the directional NIFTY strategy.
==========================================================

Goal (per your choices):
  * Search the parameters that matter most: the 3 EMA lengths + ADX threshold,
    fan width, breakout lookback, and the trailing-stop aggressiveness.
  * Optimize for ACCURACY + ROBUSTNESS that repeats month after month --
    NOT raw total return. The score rewards consistent monthly profitability
    and penalises fragility.
  * Validate with WALK-FORWARD so every score is on data the optimizer never
    tuned on. This is the honest test of "does it work month after month".

WHY WALK-FORWARD (read this before trusting any number):
  A single optimisation over all 5 years will always find a gorgeous backtest --
  that is overfitting, and it dies live. Walk-forward instead does:
      optimise on [train window] -> lock params -> test on [next unseen window]
      roll forward, repeat.
  The strategy's REAL performance is the stitched-together OUT-OF-SAMPLE windows
  (the 'oos' results). If those are consistently green, you have something. If
  only the in-sample 'train' numbers look good, you have a curve fit.

Run LOCALLY:
    python optimize_walkforward.py NIFTY50_1min_5yr.parquet

Outputs:
    walkforward_oos_trades.csv   every out-of-sample trade (the honest record)
    walkforward_summary.txt      per-window chosen params + OOS monthly stats
    walkforward_param_stability.csv  how often each param value was chosen
                                     (stable picks = robust; jumpy = fragile)
"""

import sys
import itertools
import numpy as np
import pandas as pd
from dataclasses import dataclass


# ============================================================
# FIXED PARAMETERS (not searched -- kept at current values)
# ============================================================
FIXED = dict(
    SLOPE_LOOKBACK=5,
    ATR_PERIOD=14,
    ATR_EXP_LOOKBACK=30,
    ATR_EXPANSION=1.10,
    MAX_LOSS_PCT=0.004,
    MIN_TREND_BARS=10,
    COOLDOWN_BARS=5,
    ADX_PERIOD=14,
    PROGRESS_BARS=8,
    MIN_PROGRESS_PCT=0.0010,
    BREAKEVEN_AFTER_PCT=0.0015,
    COST_PCT_PER_TRADE=0.0003,
    SQUAREOFF_TIME="15:20",
)

# ============================================================
# SEARCH GRID (the parameters you said matter most)
# ------------------------------------------------------------
# Keep each axis small: total combos = product of all list lengths.
# EMA stack must stay ordered fast<mid<slow; invalid combos are skipped.
# trail_aggr scales ALL trail tiers together (one knob, not four) to keep
# the trail shape but tune how tight/loose it is overall.
# ============================================================
GRID = dict(
    EMA_FAST=[9, 12],
    EMA_MID=[21, 26],
    EMA_SLOW=[50, 60],
    MIN_ADX=[20.0, 25.0],
    MIN_FAN_PCT=[0.0008, 0.0012],
    BREAKOUT_LOOKBACK=[20, 30],
    TRAIL_AGGR=[1.0, 1.2],
)
BASE_TRAIL_TIERS = [(0.0100, 1.2), (0.0060, 1.6), (0.0035, 2.2), (0.0000, 3.0)]

# ============================================================
# WALK-FORWARD WINDOWS
# ------------------------------------------------------------
# TRAIN_MONTHS optimise, then TEST_MONTHS held-out test, then roll by TEST_MONTHS.
# With ~5yr data: 18mo train / 6mo test, rolling 6mo -> several OOS slices.
# ============================================================
TRAIN_MONTHS = 18
TEST_MONTHS = 6


# ============================================================
# CORE STRATEGY (parameterised -- no globals)
# ============================================================
def add_indicators(g, P):
    g = g.copy()
    c = g["close"]
    g["ema_f"] = c.ewm(span=P["EMA_FAST"], adjust=False).mean()
    g["ema_m"] = c.ewm(span=P["EMA_MID"], adjust=False).mean()
    g["ema_s"] = c.ewm(span=P["EMA_SLOW"], adjust=False).mean()
    g["slope_s"] = g["ema_s"].diff(P["SLOPE_LOOKBACK"])
    prev_c = c.shift(1)
    tr = pd.concat([g["high"] - g["low"], (g["high"] - prev_c).abs(),
                    (g["low"] - prev_c).abs()], axis=1).max(axis=1)
    g["atr"] = tr.ewm(span=P["ATR_PERIOD"], adjust=False).mean()
    g["roll_hi"] = g["high"].rolling(P["BREAKOUT_LOOKBACK"]).max()
    g["roll_lo"] = g["low"].rolling(P["BREAKOUT_LOOKBACK"]).min()
    g["fan_pct"] = (g["ema_f"] - g["ema_s"]).abs() / c
    g["atr_avg"] = g["atr"].rolling(P["ATR_EXP_LOOKBACK"]).mean()
    g["atr_ratio"] = g["atr"] / g["atr_avg"]
    up_move = g["high"].diff(); down_move = -g["low"].diff()
    pdm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    mdm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    atr_w = tr.ewm(alpha=1 / P["ADX_PERIOD"], adjust=False).mean()
    pdi = 100 * pd.Series(pdm, index=g.index).ewm(alpha=1 / P["ADX_PERIOD"], adjust=False).mean() / atr_w
    mdi = 100 * pd.Series(mdm, index=g.index).ewm(alpha=1 / P["ADX_PERIOD"], adjust=False).mean() / atr_w
    dx = 100 * (pdi - mdi).abs() / (pdi + mdi).replace(0, np.nan)
    g["adx"] = dx.ewm(alpha=1 / P["ADX_PERIOD"], adjust=False).mean()
    return g


def scan_day(g, P, trail_tiers):
    """Compatibility wrapper: compute indicators then scan."""
    gi = add_indicators(g, P).reset_index(drop=True)
    return scan_from_indicators(gi, P, trail_tiers)


def scan_from_indicators(g, P, trail_tiers):
    n = len(g)
    if n < 2:
        return []
    # pull to numpy for fast row access (iloc in a tight loop is the bottleneck)
    close = g["close"].to_numpy()
    high = g["high"].to_numpy()
    low = g["low"].to_numpy()
    atr = g["atr"].to_numpy()
    ema_f = g["ema_f"].to_numpy()
    ema_m = g["ema_m"].to_numpy()
    ema_s = g["ema_s"].to_numpy()
    slope_s = g["slope_s"].to_numpy()
    roll_hi = g["roll_hi"].to_numpy()
    roll_lo = g["roll_lo"].to_numpy()
    fan = g["fan_pct"].to_numpy()
    atr_ratio = g["atr_ratio"].to_numpy()
    adx = g["adx"].to_numpy()
    tarr = g["t"].to_numpy()
    day0 = g.iloc[0]["day"]
    date_arr = g["date"].to_numpy()

    eps = []
    i = P["BREAKOUT_LOOKBACK"] + P["SLOPE_LOOKBACK"]
    last_exit = -10_000
    sq = P["SQUAREOFF_TIME"]
    be_pct = P["BREAKEVEN_AFTER_PCT"]; prog_bars = P["PROGRESS_BARS"]; min_prog = P["MIN_PROGRESS_PCT"]
    max_loss = P["MAX_LOSS_PCT"]; min_bars = P["MIN_TREND_BARS"]; cost = P["COST_PCT_PER_TRADE"]
    cooldown = P["COOLDOWN_BARS"]; min_adx = P["MIN_ADX"]; min_fan = P["MIN_FAN_PCT"]; atr_exp = P["ATR_EXPANSION"]

    while i < n - 1:
        if i - last_exit < cooldown:
            i += 1; continue
        if np.isnan(slope_s[i]) or np.isnan(roll_hi[i]) or np.isnan(adx[i]) or np.isnan(atr_ratio[i]):
            i += 1; continue
        regime = (adx[i] >= min_adx) and (fan[i] >= min_fan) and (atr_ratio[i] >= atr_exp)
        up = regime and (ema_f[i] > ema_m[i] > ema_s[i]) and (slope_s[i] > 0) and (close[i] >= roll_hi[i - 1])
        down = regime and (ema_f[i] < ema_m[i] < ema_s[i]) and (slope_s[i] < 0) and (close[i] <= roll_lo[i - 1])
        if not (up or down):
            i += 1; continue
        direction = "up" if up else "down"
        entry_idx = i; entry_px = close[i]
        extreme = entry_px; exit_idx = None
        j = i + 1; be_armed = False
        while j < n:
            held = j - entry_idx
            if tarr[j] >= sq:
                exit_idx = j; break
            if direction == "up":
                if high[j] > extreme:
                    extreme = high[j]
                fav = (extreme - entry_px) / entry_px
                if fav >= be_pct:
                    be_armed = True
                atr_mult = next(m for thr, m in trail_tiers if fav >= thr)
                trail = extreme - atr_mult * atr[j]
                if held >= prog_bars and fav < min_prog:
                    exit_idx = j; break
                if be_armed and close[j] <= entry_px:
                    exit_idx = j; break
                if close[j] <= entry_px * (1 - max_loss):
                    exit_idx = j; break
                if close[j] < trail:
                    exit_idx = j; break
                if ema_f[j] < ema_m[j]:
                    exit_idx = j; break
            else:
                if low[j] < extreme:
                    extreme = low[j]
                fav = (entry_px - extreme) / entry_px
                if fav >= be_pct:
                    be_armed = True
                atr_mult = next(m for thr, m in trail_tiers if fav >= thr)
                trail = extreme + atr_mult * atr[j]
                if held >= prog_bars and fav < min_prog:
                    exit_idx = j; break
                if be_armed and close[j] >= entry_px:
                    exit_idx = j; break
                if close[j] >= entry_px * (1 + max_loss):
                    exit_idx = j; break
                if close[j] > trail:
                    exit_idx = j; break
                if ema_f[j] > ema_m[j]:
                    exit_idx = j; break
            j += 1
        if exit_idx is None:
            exit_idx = n - 1
        bars = exit_idx - entry_idx
        if bars >= min_bars:
            exit_px = close[exit_idx]
            pnl_pts = (exit_px - entry_px) if direction == "up" else (entry_px - exit_px)
            pnl_pct = pnl_pts / entry_px - cost
            eps.append({"day": day0, "entry_time": date_arr[entry_idx],
                        "direction": direction, "pnl_pct": pnl_pct * 100})
        last_exit = exit_idx
        i = exit_idx + 1
    return eps


def run_strategy(df, P, trail_tiers, _cache=None):
    """Run over all days in df, return trades DataFrame.
    Indicators depend only on EMA lengths + periods + breakout lookback, so when
    a cache dict is supplied we memoize the per-day indicator frames keyed on
    those, and reuse them across threshold-only param changes (MIN_ADX, MIN_FAN,
    TRAIL_AGGR), which is most of the grid. This is the main optimizer speedup."""
    all_eps = []
    ikey = (P["EMA_FAST"], P["EMA_MID"], P["EMA_SLOW"], P["SLOPE_LOOKBACK"],
            P["ATR_PERIOD"], P["ATR_EXP_LOOKBACK"], P["ADX_PERIOD"], P["BREAKOUT_LOOKBACK"])
    for day, g in df.groupby("day"):
        if len(g) < P["BREAKOUT_LOOKBACK"] + P["EMA_SLOW"]:
            continue
        gi = None
        if _cache is not None:
            gi = _cache.get((day, ikey))
        if gi is None:
            gi = add_indicators(g, P).reset_index(drop=True)
            if _cache is not None:
                _cache[(day, ikey)] = gi
        all_eps.extend(scan_from_indicators(gi, P, trail_tiers))
    return pd.DataFrame(all_eps)


# ============================================================
# ROBUSTNESS / ACCURACY SCORE
# ------------------------------------------------------------
# We do NOT score on total return. We score on month-after-month reliability:
#   * frac_profitable_months  (consistency)         -- biggest weight
#   * monthly mean / monthly std  (stability, Sharpe-like)
#   * win rate                (accuracy)
#   * a floor on trade count so we don't reward 3-trade flukes
# ============================================================
def score(trades, min_trades=20):
    if trades.empty or len(trades) < min_trades:
        return -1e9, {}
    t = trades.copy()
    t["month"] = pd.to_datetime(t["entry_time"]).dt.to_period("M").astype(str)
    monthly = t.groupby("month")["pnl_pct"].sum()
    if len(monthly) < 2:
        return -1e9, {}
    frac_green = (monthly > 0).mean()
    m_mean = monthly.mean()
    m_std = monthly.std() if monthly.std() > 1e-9 else 1e-9
    sharpe_m = m_mean / m_std                       # monthly stability
    win_rate = (t["pnl_pct"] > 0).mean()
    # combined score: consistency dominates, then stability, then accuracy.
    # m_mean kept as a mild multiplier so we don't pick a flat-but-consistent dud.
    s = (frac_green * 3.0) + (sharpe_m * 1.0) + (win_rate * 1.0)
    s *= max(0.1, m_mean)        # must actually make money to score well
    stats = dict(trades=len(t), frac_green=round(frac_green, 3),
                 monthly_mean=round(m_mean, 3), monthly_std=round(m_std, 3),
                 sharpe_m=round(sharpe_m, 3), win_rate=round(win_rate, 3),
                 total=round(monthly.sum(), 2))
    return s, stats


# ============================================================
# WALK-FORWARD ENGINE
# ============================================================
def build_param_combos():
    keys = list(GRID.keys())
    combos = []
    for vals in itertools.product(*[GRID[k] for k in keys]):
        d = dict(zip(keys, vals))
        if not (d["EMA_FAST"] < d["EMA_MID"] < d["EMA_SLOW"]):
            continue            # enforce ordered EMA stack
        combos.append(d)
    return combos


def make_params(combo):
    P = dict(FIXED)
    P.update({k: combo[k] for k in combo if k != "TRAIL_AGGR"})
    aggr = combo["TRAIL_AGGR"]
    tiers = [(thr, mult * aggr) for thr, mult in BASE_TRAIL_TIERS]
    return P, tiers


def month_floor(ts):
    return pd.Timestamp(ts).to_period("M").to_timestamp()


def walk_forward(df):
    df = df.copy()
    df["entry_time"] = pd.to_datetime(df["date"])
    months = sorted(pd.to_datetime(df["date"]).dt.to_period("M").unique())
    combos = build_param_combos()
    print(f"[INFO] {len(combos)} valid param combos, {len(months)} months of data")
    print(f"[INFO] Walk-forward: {TRAIN_MONTHS}mo train / {TEST_MONTHS}mo test, "
          f"~{max(0,(len(months)-TRAIN_MONTHS)//TEST_MONTHS)} OOS windows")

    oos_trades_all = []
    window_log = []
    param_picks = {k: [] for k in GRID}

    start = 0
    win_no = 0
    while start + TRAIN_MONTHS + TEST_MONTHS <= len(months):
        train_months = months[start:start + TRAIN_MONTHS]
        test_months = months[start + TRAIN_MONTHS:start + TRAIN_MONTHS + TEST_MONTHS]
        win_no += 1

        def in_months(d, mlist):
            p = pd.to_datetime(d["date"]).dt.to_period("M")
            return d[p.isin(mlist)]

        train_df = in_months(df, train_months)
        test_df = in_months(df, test_months)

        # --- optimise on train (cache indicators across combos in this window) ---
        best_s, best_combo, best_stats = -1e18, None, None
        ind_cache = {}
        for combo in combos:
            P, tiers = make_params(combo)
            tr = run_strategy(train_df, P, tiers, _cache=ind_cache)
            s, st = score(tr)
            if s > best_s:
                best_s, best_combo, best_stats = s, combo, st

        # --- apply locked params to UNSEEN test ---
        P, tiers = make_params(best_combo)
        test_tr = run_strategy(test_df, P, tiers)
        _, oos_stats = score(test_tr, min_trades=1)
        if not test_tr.empty:
            test_tr = test_tr.assign(window=win_no)
            oos_trades_all.append(test_tr)

        for k in GRID:
            param_picks[k].append(best_combo[k])

        tlabel = f"{str(train_months[0])}..{str(train_months[-1])}"
        olabel = f"{str(test_months[0])}..{str(test_months[-1])}"
        window_log.append(dict(window=win_no, train=tlabel, oos=olabel,
                               chosen=best_combo, train_stats=best_stats, oos_stats=oos_stats))
        print(f"[WIN {win_no}] train {tlabel} -> oos {olabel} | "
              f"chosen EMA {best_combo['EMA_FAST']}/{best_combo['EMA_MID']}/{best_combo['EMA_SLOW']} "
              f"ADX{best_combo['MIN_ADX']} | OOS total {oos_stats.get('total','?')}% "
              f"green {oos_stats.get('frac_green','?')}")
        start += TEST_MONTHS

    return oos_trades_all, window_log, param_picks


def main():
    if len(sys.argv) < 2:
        path = r"../Trading_2024/back_testing/nifty50_1min_history/NIFTY50_1min_20210614_to_20260613.parquet"
        print("Usage: python optimize_walkforward.py <nifty_1min.parquet|csv>")
        # sys.exit(1)
    else:
        path = sys.argv[1]
    if path.lower().endswith(".parquet"):
        df = pd.read_parquet(path)
    else:
        df = pd.read_csv(path)
    df.columns = [c.lower() for c in df.columns]
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").drop_duplicates("date").reset_index(drop=True)
    df["day"] = df["date"].dt.date
    df["t"] = df["date"].dt.strftime("%H:%M")
    print(f"[INFO] {len(df)} candles, {df['day'].nunique()} days, "
          f"{df['date'].min()} -> {df['date'].max()}")

    oos_list, window_log, param_picks = walk_forward(df)

    # ---- stitch OOS results: the honest, out-of-sample track record ----
    lines = []
    if oos_list:
        oos = pd.concat(oos_list, ignore_index=True)
        oos.to_csv("walkforward_oos_trades.csv", index=False)
        oos["month"] = pd.to_datetime(oos["entry_time"]).dt.to_period("M").astype(str)
        monthly = oos.groupby("month")["pnl_pct"].agg(["count", "sum"])
        frac_green = (monthly["sum"] > 0).mean()
        lines.append("=== STITCHED OUT-OF-SAMPLE (the number that matters) ===")
        lines.append(f"OOS trades: {len(oos)}")
        lines.append(f"OOS win rate: {(oos['pnl_pct']>0).mean()*100:.1f}%")
        lines.append(f"OOS months: {len(monthly)}  profitable: {(monthly['sum']>0).sum()} "
                     f"({frac_green*100:.0f}%)")
        lines.append(f"OOS total %: {monthly['sum'].sum():.2f}  "
                     f"avg/mo {monthly['sum'].mean():.3f}  std {monthly['sum'].std():.3f}")
        lines.append("")
        lines.append("OOS monthly:")
        for mo, r in monthly.iterrows():
            lines.append(f"  {mo}  n={int(r['count']):3d}  {r['sum']:+.2f}%")
    else:
        lines.append("No OOS windows produced (need more months of data).")

    lines.append("")
    lines.append("=== PER-WINDOW CHOSEN PARAMS ===")
    for w in window_log:
        c = w["chosen"]
        lines.append(f"W{w['window']} oos {w['oos']}: EMA {c['EMA_FAST']}/{c['EMA_MID']}/{c['EMA_SLOW']} "
                     f"ADX{c['MIN_ADX']} fan{c['MIN_FAN_PCT']} brk{c['BREAKOUT_LOOKBACK']} "
                     f"trail{c['TRAIL_AGGR']} | OOS {w['oos_stats'].get('total','?')}%")

    # ---- parameter stability: stable picks => robust, jumpy => fragile ----
    lines.append("")
    lines.append("=== PARAMETER STABILITY (how often each value was chosen) ===")
    stab_rows = []
    for k, picks in param_picks.items():
        vc = pd.Series(picks).value_counts()
        top = vc.index[0]; top_frac = vc.iloc[0] / len(picks)
        lines.append(f"{k:18s} most common: {top} ({top_frac*100:.0f}% of windows)  "
                     f"all: {dict(vc)}")
        stab_rows.append({"param": k, "most_common": top, "stability_pct": round(top_frac * 100, 1)})
    pd.DataFrame(stab_rows).to_csv("walkforward_param_stability.csv", index=False)

    txt = "\n".join(lines)
    open("walkforward_summary.txt", "w").write(txt + "\n")
    print("\n" + txt)
    print("\n[DONE] walkforward_summary.txt, walkforward_oos_trades.csv, walkforward_param_stability.csv")


if __name__ == "__main__":
    main()
