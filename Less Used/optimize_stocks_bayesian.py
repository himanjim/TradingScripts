"""
Bayesian (Optuna/TPE) WALK-FORWARD optimizer for the directional strategy on
the LIQUID stock universe, scoring net of real Zerodha costs + 0.05%/side slippage.
=============================================================================

WHY THIS DESIGN (read before trusting any number):
  * Bayesian search is efficient at finding the best parameters -- which also
    means efficient at OVERFITTING if let loose on all the data. So we wrap it
    in WALK-FORWARD: optimise on a train window, score the chosen params on the
    NEXT unseen window, roll forward. The honest result is the stitched
    out-of-sample performance, never the in-sample best.
  * The objective is NET PROFIT AFTER 0.05%/side slippage and full Zerodha
    intraday charges. This is deliberate: tuning entry/exit parameters can only
    grow the GROSS edge; it cannot fix slippage. By baking slippage into the
    score, the optimizer is rewarded only for parameters whose edge SURVIVES
    realistic execution -- not for a prettier no-slippage fantasy.

Run LOCALLY (needs the 86 liquid stock parquets; pip install optuna):
    python optimize_stocks_bayesian.py ./stocks_1min_history [liquid_universe.csv]

If liquid_universe.csv (from the scanner) is present, only those names are used;
otherwise all parquet files in the dir are used.

Outputs:
    stock_opt_summary.txt          per-window chosen params + stitched OOS net
    stock_opt_param_stability.csv  how often each value was chosen (robustness)
    stock_opt_trials.csv           all trials (for inspection)
"""

import sys
import os
import glob
import time
import numpy as np
import pandas as pd

try:
    import optuna
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    HAVE_OPTUNA = True
except Exception:
    HAVE_OPTUNA = False

# ============================================================
# FIXED (not searched)
# ============================================================
SLOPE_LOOKBACK = 5
ATR_PERIOD = 14
ATR_EXP_LOOKBACK = 30
ADX_PERIOD = 14
MIN_TREND_BARS = 10
COOLDOWN_BARS = 5
PROGRESS_BARS = 8
MIN_PROGRESS_PCT = 0.0010
BREAKEVEN_AFTER_PCT = 0.0015
SQUAREOFF_TIME = "15:20"
ORDER_VALUE_RS = 10_00_000
SLIPPAGE_PCT_PER_SIDE = 0.0005          # 0.05%/side baked into objective

# Zerodha intraday equity charges (verified 2026)
BROKERAGE_PCT = 0.0003; BROKERAGE_CAP = 20.0
STT_SELL_PCT = 0.00025; EXCH_TXN_PCT = 0.0000297
SEBI_PER_CRORE = 10.0; STAMP_BUY_PCT = 0.00003; GST_PCT = 0.18

# ============================================================
# SEARCH SPACE (the parameters that matter; small ranges = less overfit)
# ============================================================
SEARCH = dict(
    EMA_FAST=(7, 12),            # int
    EMA_MID=(18, 30),            # int
    EMA_SLOW=(40, 70),           # int
    MIN_ADX=(18.0, 30.0),        # float
    MIN_FAN_PCT=(0.0005, 0.0015),
    BREAKOUT_LOOKBACK=(15, 40),  # int
    ATR_EXPANSION=(1.0, 1.3),    # float
    TRAIL_AGGR=(0.8, 1.3),       # float, scales trail tiers
    MAX_LOSS_PCT=(0.003, 0.006), # float
)
BASE_TRAIL_TIERS = [(0.0100, 1.2), (0.0060, 1.6), (0.0035, 2.2), (0.0000, 3.0)]

N_TRIALS = int(os.getenv("N_TRIALS", "40"))   # Bayesian trials per train window
TRAIN_MONTHS = 18
TEST_MONTHS = 6
# Runtime reality: 86 stocks x 5yr x (trials x windows) backtests is ~35 HOURS at
# full scale. Optimizing on a representative SUBSET of liquid names is the sane
# default -- 15-20 stocks captures the cross-sectional signal at ~1/5 the time,
# and the chosen params are then validated on ALL names with the scanner anyway.
MAX_STOCKS = int(os.getenv("MAX_STOCKS", "20"))  # 0 = all (slow!); default 20 subset


# ============================================================
# CHARGES + indicators + scan  (reused logic, parameterized)
# ============================================================
def charges(buy_value, sell_value):
    b = min(BROKERAGE_PCT * buy_value, BROKERAGE_CAP) + min(BROKERAGE_PCT * sell_value, BROKERAGE_CAP)
    stt = STT_SELL_PCT * sell_value
    turn = buy_value + sell_value
    exch = EXCH_TXN_PCT * turn
    sebi = SEBI_PER_CRORE * turn / 1_00_00_000
    stamp = STAMP_BUY_PCT * buy_value
    gst = GST_PCT * (b + exch + sebi)
    return b + stt + exch + sebi + stamp + gst


def add_indicators_full(df, P):
    """Compute indicators across the WHOLE stock at once, but with per-DAY reset
    (no overnight bleed) via groupby-transform. ~30x faster than looping days,
    because pandas is called a handful of times instead of thousands."""
    g = df.copy()
    gb = g.groupby("day", sort=False)
    c = g["close"]
    g["ema_f"] = gb["close"].transform(lambda s: s.ewm(span=P["EMA_FAST"], adjust=False).mean())
    g["ema_m"] = gb["close"].transform(lambda s: s.ewm(span=P["EMA_MID"], adjust=False).mean())
    g["ema_s"] = gb["close"].transform(lambda s: s.ewm(span=P["EMA_SLOW"], adjust=False).mean())
    g["slope_s"] = gb["ema_s"].transform(lambda s: s.diff(SLOPE_LOOKBACK))
    prev_c = gb["close"].transform(lambda s: s.shift(1))
    tr = pd.concat([g["high"] - g["low"], (g["high"] - prev_c).abs(),
                    (g["low"] - prev_c).abs()], axis=1).max(axis=1)
    g["_tr"] = tr
    g["atr"] = gb["_tr"].transform(lambda s: s.ewm(span=ATR_PERIOD, adjust=False).mean())
    g["roll_hi"] = gb["high"].transform(lambda s: s.rolling(P["BREAKOUT_LOOKBACK"]).max())
    g["roll_lo"] = gb["low"].transform(lambda s: s.rolling(P["BREAKOUT_LOOKBACK"]).min())
    g["fan_pct"] = (g["ema_f"] - g["ema_s"]).abs() / c
    g["atr_avg"] = gb["atr"].transform(lambda s: s.rolling(ATR_EXP_LOOKBACK).mean())
    g["atr_ratio"] = g["atr"] / g["atr_avg"]
    up_move = gb["high"].transform(lambda s: s.diff())
    down_move = -gb["low"].transform(lambda s: s.diff())
    g["_pdm"] = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    g["_mdm"] = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    g["_atrw"] = gb["_tr"].transform(lambda s: s.ewm(alpha=1 / ADX_PERIOD, adjust=False).mean())
    pdi = 100 * gb["_pdm"].transform(lambda s: s.ewm(alpha=1 / ADX_PERIOD, adjust=False).mean()) / g["_atrw"]
    mdi = 100 * gb["_mdm"].transform(lambda s: s.ewm(alpha=1 / ADX_PERIOD, adjust=False).mean()) / g["_atrw"]
    dx = 100 * (pdi - mdi).abs() / (pdi + mdi).replace(0, np.nan)
    g["_dx"] = dx
    g["adx"] = gb["_dx"].transform(lambda s: s.ewm(alpha=1 / ADX_PERIOD, adjust=False).mean())
    return g


def add_indicators(g, P):
    """Per-day version kept for compatibility (single-day frame)."""
    g = g.copy(); c = g["close"]
    g["ema_f"] = c.ewm(span=P["EMA_FAST"], adjust=False).mean()
    g["ema_m"] = c.ewm(span=P["EMA_MID"], adjust=False).mean()
    g["ema_s"] = c.ewm(span=P["EMA_SLOW"], adjust=False).mean()
    g["slope_s"] = g["ema_s"].diff(SLOPE_LOOKBACK)
    prev_c = c.shift(1)
    tr = pd.concat([g["high"] - g["low"], (g["high"] - prev_c).abs(), (g["low"] - prev_c).abs()], axis=1).max(axis=1)
    g["atr"] = tr.ewm(span=ATR_PERIOD, adjust=False).mean()
    g["roll_hi"] = g["high"].rolling(P["BREAKOUT_LOOKBACK"]).max()
    g["roll_lo"] = g["low"].rolling(P["BREAKOUT_LOOKBACK"]).min()
    g["fan_pct"] = (g["ema_f"] - g["ema_s"]).abs() / c
    g["atr_avg"] = g["atr"].rolling(ATR_EXP_LOOKBACK).mean()
    g["atr_ratio"] = g["atr"] / g["atr_avg"]
    up_move = g["high"].diff(); down_move = -g["low"].diff()
    pdm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    mdm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    atr_w = tr.ewm(alpha=1 / ADX_PERIOD, adjust=False).mean()
    pdi = 100 * pd.Series(pdm, index=g.index).ewm(alpha=1 / ADX_PERIOD, adjust=False).mean() / atr_w
    mdi = 100 * pd.Series(mdm, index=g.index).ewm(alpha=1 / ADX_PERIOD, adjust=False).mean() / atr_w
    dx = 100 * (pdi - mdi).abs() / (pdi + mdi).replace(0, np.nan)
    g["adx"] = dx.ewm(alpha=1 / ADX_PERIOD, adjust=False).mean()
    return g


def scan_day(g, P, trail_tiers):
    g = add_indicators(g, P).reset_index(drop=True)
    n = len(g)
    if n < 2:
        return []
    close = g["close"].to_numpy(); high = g["high"].to_numpy(); low = g["low"].to_numpy()
    atr = g["atr"].to_numpy(); ema_f = g["ema_f"].to_numpy(); ema_m = g["ema_m"].to_numpy()
    ema_s = g["ema_s"].to_numpy(); slope_s = g["slope_s"].to_numpy()
    roll_hi = g["roll_hi"].to_numpy(); roll_lo = g["roll_lo"].to_numpy()
    fan = g["fan_pct"].to_numpy(); atr_ratio = g["atr_ratio"].to_numpy(); adx = g["adx"].to_numpy()
    tarr = g["t"].to_numpy()
    eps = []
    bl = P["BREAKOUT_LOOKBACK"]; min_adx = P["MIN_ADX"]; min_fan = P["MIN_FAN_PCT"]
    atr_exp = P["ATR_EXPANSION"]; max_loss = P["MAX_LOSS_PCT"]
    i = bl + SLOPE_LOOKBACK; last_exit = -10_000
    while i < n - 1:
        if i - last_exit < COOLDOWN_BARS:
            i += 1; continue
        if np.isnan(slope_s[i]) or np.isnan(roll_hi[i]) or np.isnan(adx[i]) or np.isnan(atr_ratio[i]):
            i += 1; continue
        regime = (adx[i] >= min_adx) and (fan[i] >= min_fan) and (atr_ratio[i] >= atr_exp)
        up = regime and (ema_f[i] > ema_m[i] > ema_s[i]) and (slope_s[i] > 0) and (close[i] >= roll_hi[i - 1])
        down = regime and (ema_f[i] < ema_m[i] < ema_s[i]) and (slope_s[i] < 0) and (close[i] <= roll_lo[i - 1])
        if not (up or down):
            i += 1; continue
        direction = "up" if up else "down"
        entry_idx = i; entry_px = close[i]; extreme = entry_px; exit_idx = None
        j = i + 1; be_armed = False
        while j < n:
            held = j - entry_idx
            if tarr[j] >= SQUAREOFF_TIME:
                exit_idx = j; break
            if direction == "up":
                if high[j] > extreme: extreme = high[j]
                fav = (extreme - entry_px) / entry_px
                if fav >= BREAKEVEN_AFTER_PCT: be_armed = True
                atr_mult = next(m for thr, m in trail_tiers if fav >= thr)
                trail = extreme - atr_mult * atr[j]
                if held >= PROGRESS_BARS and fav < MIN_PROGRESS_PCT: exit_idx = j; break
                if be_armed and close[j] <= entry_px: exit_idx = j; break
                if close[j] <= entry_px * (1 - max_loss): exit_idx = j; break
                if close[j] < trail: exit_idx = j; break
                if ema_f[j] < ema_m[j]: exit_idx = j; break
            else:
                if low[j] < extreme: extreme = low[j]
                fav = (entry_px - extreme) / entry_px
                if fav >= BREAKEVEN_AFTER_PCT: be_armed = True
                atr_mult = next(m for thr, m in trail_tiers if fav >= thr)
                trail = extreme + atr_mult * atr[j]
                if held >= PROGRESS_BARS and fav < MIN_PROGRESS_PCT: exit_idx = j; break
                if be_armed and close[j] >= entry_px: exit_idx = j; break
                if close[j] >= entry_px * (1 + max_loss): exit_idx = j; break
                if close[j] > trail: exit_idx = j; break
                if ema_f[j] > ema_m[j]: exit_idx = j; break
            j += 1
        if exit_idx is None:
            exit_idx = n - 1
        bars = exit_idx - entry_idx
        if bars >= MIN_TREND_BARS:
            exit_px = close[exit_idx]
            shares = max(1, int(ORDER_VALUE_RS / entry_px))
            if direction == "up":
                buy_val = entry_px * shares; sell_val = exit_px * shares
            else:
                sell_val = entry_px * shares; buy_val = exit_px * shares
            pnl_pts = (exit_px - entry_px) if direction == "up" else (entry_px - exit_px)
            gross_rs = pnl_pts * shares
            ch = charges(buy_val, sell_val)
            slip = SLIPPAGE_PCT_PER_SIDE * (buy_val + sell_val)
            net_rs = gross_rs - ch - slip
            eps.append({"net_rs": net_rs})
        last_exit = exit_idx
        i = exit_idx + 1
    return eps


def _scan_day_arrays(close, high, low, atr, ema_f, ema_m, ema_s, slope_s,
                     roll_hi, roll_lo, fan, atr_ratio, adx, tsec, P, trail_tiers,
                     squareoff_sec):
    """Pure-numpy single-day scan. Arrays are slices for one day. Returns list of net_rs."""
    n = len(close)
    if n < 2:
        return []
    eps = []
    bl = P["BREAKOUT_LOOKBACK"]; min_adx = P["MIN_ADX"]; min_fan = P["MIN_FAN_PCT"]
    atr_exp = P["ATR_EXPANSION"]; max_loss = P["MAX_LOSS_PCT"]
    i = bl + SLOPE_LOOKBACK; last_exit = -10_000
    while i < n - 1:
        if i - last_exit < COOLDOWN_BARS:
            i += 1; continue
        if np.isnan(slope_s[i]) or np.isnan(roll_hi[i]) or np.isnan(adx[i]) or np.isnan(atr_ratio[i]):
            i += 1; continue
        regime = (adx[i] >= min_adx) and (fan[i] >= min_fan) and (atr_ratio[i] >= atr_exp)
        up = regime and (ema_f[i] > ema_m[i] > ema_s[i]) and (slope_s[i] > 0) and (close[i] >= roll_hi[i - 1])
        down = regime and (ema_f[i] < ema_m[i] < ema_s[i]) and (slope_s[i] < 0) and (close[i] <= roll_lo[i - 1])
        if not (up or down):
            i += 1; continue
        direction_up = up
        entry_idx = i; entry_px = close[i]; extreme = entry_px; exit_idx = None
        j = i + 1; be_armed = False
        while j < n:
            held = j - entry_idx
            if tsec[j] >= squareoff_sec:
                exit_idx = j; break
            if direction_up:
                if high[j] > extreme: extreme = high[j]
                fav = (extreme - entry_px) / entry_px
                if fav >= BREAKEVEN_AFTER_PCT: be_armed = True
                atr_mult = next(m for thr, m in trail_tiers if fav >= thr)
                trail = extreme - atr_mult * atr[j]
                if held >= PROGRESS_BARS and fav < MIN_PROGRESS_PCT: exit_idx = j; break
                if be_armed and close[j] <= entry_px: exit_idx = j; break
                if close[j] <= entry_px * (1 - max_loss): exit_idx = j; break
                if close[j] < trail: exit_idx = j; break
                if ema_f[j] < ema_m[j]: exit_idx = j; break
            else:
                if low[j] < extreme: extreme = low[j]
                fav = (entry_px - extreme) / entry_px
                if fav >= BREAKEVEN_AFTER_PCT: be_armed = True
                atr_mult = next(m for thr, m in trail_tiers if fav >= thr)
                trail = extreme + atr_mult * atr[j]
                if held >= PROGRESS_BARS and fav < MIN_PROGRESS_PCT: exit_idx = j; break
                if be_armed and close[j] >= entry_px: exit_idx = j; break
                if close[j] >= entry_px * (1 + max_loss): exit_idx = j; break
                if close[j] > trail: exit_idx = j; break
                if ema_f[j] > ema_m[j]: exit_idx = j; break
            j += 1
        if exit_idx is None:
            exit_idx = n - 1
        bars = exit_idx - entry_idx
        if bars >= MIN_TREND_BARS:
            exit_px = close[exit_idx]
            shares = max(1, int(ORDER_VALUE_RS / entry_px))
            if direction_up:
                buy_val = entry_px * shares; sell_val = exit_px * shares
                pnl_pts = exit_px - entry_px
            else:
                sell_val = entry_px * shares; buy_val = exit_px * shares
                pnl_pts = entry_px - exit_px
            gross_rs = pnl_pts * shares
            ch = charges(buy_val, sell_val)
            slip = SLIPPAGE_PCT_PER_SIDE * (buy_val + sell_val)
            eps.append(gross_rs - ch - slip)
        last_exit = exit_idx
        i = exit_idx + 1
    return eps


_IND_CACHE = {}        # (stock_id, ema_f, ema_m, ema_s, breakout) -> arrays dict
_CACHE_HITS = [0, 0]   # [hits, misses] for diagnostics


def _stock_arrays(df, P, stock_id):
    """Return cached indicator arrays for this stock + indicator params.
    Only EMA lengths and BREAKOUT_LOOKBACK change the indicators; threshold
    params (ADX/fan/atr_exp/max_loss/trail) do NOT, so they are excluded from
    the cache key -> most trials reuse cached indicators entirely."""
    key = (stock_id, P["EMA_FAST"], P["EMA_MID"], P["EMA_SLOW"], P["BREAKOUT_LOOKBACK"])
    cached = _IND_CACHE.get(key)
    if cached is not None:
        _CACHE_HITS[0] += 1
        return cached
    _CACHE_HITS[1] += 1
    ind = add_indicators_full(df, P)
    ind = ind.assign(_tsec=ind["date"].dt.hour * 3600 + ind["date"].dt.minute * 60)
    cols = ["close", "high", "low", "atr", "ema_f", "ema_m", "ema_s", "slope_s",
            "roll_hi", "roll_lo", "fan_pct", "atr_ratio", "adx", "_tsec"]
    arrs = {c: ind[c].to_numpy() for c in cols}
    day_codes = ind["day"].to_numpy()
    bounds = np.where(day_codes[1:] != day_codes[:-1])[0] + 1
    arrs["_starts"] = np.concatenate([[0], bounds])
    arrs["_ends"] = np.concatenate([bounds, [len(day_codes)]])
    _IND_CACHE[key] = arrs
    return arrs


def run_universe(stock_dfs, P, trail_tiers):
    """Sum net_rs across all stocks/days, using cached indicators per stock."""
    total = 0.0; ntr = 0
    min_bars = P["BREAKOUT_LOOKBACK"] + P["EMA_SLOW"]
    sq_sec = 15 * 3600 + 20 * 60
    for stock_id, df in enumerate(stock_dfs):
        arrs = _stock_arrays(df, P, stock_id)
        starts = arrs["_starts"]; ends = arrs["_ends"]
        for s, e in zip(starts, ends):
            if e - s < min_bars:
                continue
            res = _scan_day_arrays(
                arrs["close"][s:e], arrs["high"][s:e], arrs["low"][s:e], arrs["atr"][s:e],
                arrs["ema_f"][s:e], arrs["ema_m"][s:e], arrs["ema_s"][s:e], arrs["slope_s"][s:e],
                arrs["roll_hi"][s:e], arrs["roll_lo"][s:e], arrs["fan_pct"][s:e],
                arrs["atr_ratio"][s:e], arrs["adx"][s:e], arrs["_tsec"][s:e],
                P, trail_tiers, sq_sec)
            total += sum(res); ntr += len(res)
    return total, ntr


def make_params(trial_or_dict):
    """Build P dict + trail tiers from an Optuna trial or a plain dict."""
    if HAVE_OPTUNA and not isinstance(trial_or_dict, dict):
        t = trial_or_dict
        P = dict(
            EMA_FAST=t.suggest_int("EMA_FAST", *SEARCH["EMA_FAST"]),
            EMA_MID=t.suggest_int("EMA_MID", *SEARCH["EMA_MID"]),
            EMA_SLOW=t.suggest_int("EMA_SLOW", *SEARCH["EMA_SLOW"]),
            MIN_ADX=t.suggest_float("MIN_ADX", *SEARCH["MIN_ADX"]),
            MIN_FAN_PCT=t.suggest_float("MIN_FAN_PCT", *SEARCH["MIN_FAN_PCT"]),
            BREAKOUT_LOOKBACK=t.suggest_int("BREAKOUT_LOOKBACK", *SEARCH["BREAKOUT_LOOKBACK"]),
            ATR_EXPANSION=t.suggest_float("ATR_EXPANSION", *SEARCH["ATR_EXPANSION"]),
            MAX_LOSS_PCT=t.suggest_float("MAX_LOSS_PCT", *SEARCH["MAX_LOSS_PCT"]),
        )
        aggr = t.suggest_float("TRAIL_AGGR", *SEARCH["TRAIL_AGGR"])
    else:
        d = trial_or_dict
        P = {k: d[k] for k in ["EMA_FAST", "EMA_MID", "EMA_SLOW", "MIN_ADX", "MIN_FAN_PCT",
                               "BREAKOUT_LOOKBACK", "ATR_EXPANSION", "MAX_LOSS_PCT"]}
        aggr = d["TRAIL_AGGR"]
    # enforce ordered EMA stack; if invalid, signal caller to skip
    if not (P["EMA_FAST"] < P["EMA_MID"] < P["EMA_SLOW"]):
        return None, None
    tiers = [(thr, mult * aggr) for thr, mult in BASE_TRAIL_TIERS]
    return P, tiers


def load_stock(path):
    df = pd.read_parquet(path) if path.endswith(".parquet") else pd.read_csv(path)
    df.columns = [c.lower() for c in df.columns]
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").drop_duplicates("date").reset_index(drop=True)
    df["day"] = df["date"].dt.date
    df["t"] = df["date"].dt.strftime("%H:%M")
    df["period"] = df["date"].dt.to_period("M").astype(str)
    return df


def main():
    if not HAVE_OPTUNA:
        print("[ERR] optuna not installed. Run: pip install optuna")
        return
    d = sys.argv[1] if len(sys.argv) > 1 else "../historic_data_fetcher/stocks_1min_history"
    universe_csv = sys.argv[2] if len(sys.argv) > 2 else os.path.join(os.getcwd(), "liquid_universe.csv")

    files = sorted(glob.glob(os.path.join(d, "*.parquet")) + glob.glob(os.path.join(d, "*.csv")))
    files = [f for f in files if not os.path.basename(f).startswith("_")]
    if os.path.exists(universe_csv):
        liq = set(pd.read_csv(universe_csv)["symbol"].astype(str))
        files = [f for f in files if os.path.splitext(os.path.basename(f))[0] in liq]
        print(f"[INFO] restricted to {len(files)} liquid names from {universe_csv}")
    else:
        print(f"[INFO] no liquid_universe.csv; using all {len(files)} files")
    if not files:
        print("[ERR] no files"); return

    if MAX_STOCKS > 0 and len(files) > MAX_STOCKS:
        # evenly sample across the (alpha-sorted) list for a representative subset
        idx = np.linspace(0, len(files) - 1, MAX_STOCKS).round().astype(int)
        files = [files[i] for i in sorted(set(idx))]
        print(f"[INFO] MAX_STOCKS={MAX_STOCKS}: optimizing on a {len(files)}-stock "
              f"representative subset (set MAX_STOCKS=0 for all 86, ~35h).")
    print("[INFO] loading stocks...")
    stocks = [load_stock(f) for f in files]
    all_months = sorted(pd.concat([s[["period"]] for s in stocks])["period"].unique())
    print(f"[INFO] {len(stocks)} stocks, {len(all_months)} months "
          f"({all_months[0]}..{all_months[-1]})")

    def subset(months):
        out = []
        mset = set(months)
        for s in stocks:
            sub = s[s["period"].isin(mset)]
            if not sub.empty:
                out.append(sub)
        return out

    window_log, oos_total, oos_trades = [], 0.0, 0
    param_picks = {k: [] for k in SEARCH}
    all_trials = []
    # count total windows up front so progress is meaningful
    total_windows = max(0, (len(all_months) - TRAIN_MONTHS - TEST_MONTHS) // TEST_MONTHS + 1)
    print(f"[INFO] {total_windows} walk-forward windows planned, "
          f"{N_TRIALS} trials each = {total_windows*N_TRIALS} backtests of {len(stocks)} stocks.")
    print("[INFO] First trial is the slowest (warms caches). Progress prints per trial.\n")
    run_t0 = time.time()
    start = win = 0
    while start + TRAIN_MONTHS + TEST_MONTHS <= len(all_months):
        win += 1
        tr_m = all_months[start:start + TRAIN_MONTHS]
        te_m = all_months[start + TRAIN_MONTHS:start + TRAIN_MONTHS + TEST_MONTHS]
        train_dfs = subset(tr_m); test_dfs = subset(te_m)
        _IND_CACHE.clear()      # train data differs per window; stale arrays invalid
        _CACHE_HITS[0] = _CACHE_HITS[1] = 0
        print(f"--- WINDOW {win}/{total_windows}: train {tr_m[0]}..{tr_m[-1]}, "
              f"oos {te_m[0]}..{te_m[-1]} ({N_TRIALS} trials) ---")

        win_t0 = time.time()

        def objective(trial):
            P, tiers = make_params(trial)
            if P is None:
                return -1e18
            net, ntr = run_universe(train_dfs, P, tiers)
            if ntr < 200:        # avoid degenerate low-trade params
                return -1e15
            return net

        def progress_cb(study, trial):
            n_done = trial.number + 1
            el = time.time() - win_t0
            per = el / n_done
            eta_win = per * (N_TRIALS - n_done)
            best_so_far = study.best_value if study.best_trial else float("nan")
            sys.stdout.write(
                f"\r  W{win}/{total_windows} trial {n_done}/{N_TRIALS} | "
                f"{per:.1f}s/trial | best net Rs {best_so_far:,.0f} | "
                f"win ETA {eta_win/60:.1f}m   ")
            sys.stdout.flush()
            if n_done == N_TRIALS:
                sys.stdout.write("\n")

        study = optuna.create_study(direction="maximize",
                                    sampler=optuna.samplers.TPESampler(seed=42))
        study.optimize(objective, n_trials=N_TRIALS, show_progress_bar=False,
                       callbacks=[progress_cb])
        best = study.best_params
        for k in SEARCH:
            param_picks[k].append(best[k])
        for t in study.trials:
            all_trials.append({"window": win, **t.params, "train_net": t.value})

        P, tiers = make_params(best)
        _IND_CACHE.clear()      # test_dfs reuse stock ids; must not hit train arrays
        te_net, te_ntr = run_universe(test_dfs, P, tiers)
        oos_total += te_net; oos_trades += te_ntr
        window_log.append({"window": win, "train": f"{tr_m[0]}..{tr_m[-1]}",
                           "oos": f"{te_m[0]}..{te_m[-1]}", "best": best,
                           "oos_net": te_net, "oos_trades": te_ntr})
        el_all = time.time() - run_t0
        eta_all = (el_all / win) * (total_windows - win)
        print(f"[WIN {win}/{total_windows} DONE in {(time.time()-win_t0)/60:.1f}m] "
              f"oos {te_m[0]}..{te_m[-1]} | "
              f"EMA {best['EMA_FAST']}/{best['EMA_MID']}/{best['EMA_SLOW']} "
              f"ADX{best['MIN_ADX']:.1f} brk{best['BREAKOUT_LOOKBACK']} | "
              f"OOS net Rs {te_net:,.0f} ({te_ntr} trades)")
        print(f"    [overall] {win}/{total_windows} windows done, "
              f"{el_all/60:.1f}m elapsed, ~{eta_all/60:.1f}m remaining\n")
        start += TEST_MONTHS

    # ---- report ----
    L = ["=== BAYESIAN WALK-FORWARD (stocks, net of charges + 0.05%/side slippage) ==="]
    L.append(f"Universe: {len(stocks)} liquid stocks | {N_TRIALS} trials/window | "
             f"{TRAIN_MONTHS}mo train / {TEST_MONTHS}mo test")
    L.append("")
    L.append("--- STITCHED OUT-OF-SAMPLE (the honest number) ---")
    L.append(f"OOS windows: {len(window_log)}")
    L.append(f"OOS total net: Rs {oos_total:,.0f}  over {oos_trades} trades")
    if oos_trades:
        L.append(f"OOS avg net/trade: Rs {oos_total/oos_trades:,.0f}")
    L.append("")
    L.append("Per-window OOS net:")
    for w in window_log:
        L.append(f"  W{w['window']} {w['oos']}: Rs {w['oos_net']:,.0f} ({w['oos_trades']} tr)  "
                 f"EMA {w['best']['EMA_FAST']}/{w['best']['EMA_MID']}/{w['best']['EMA_SLOW']} "
                 f"ADX{w['best']['MIN_ADX']:.1f}")
    L.append("")
    L.append("--- PARAMETER STABILITY (stable picks = robust; jumpy = fragile) ---")
    stab = []
    if not window_log:
        L.append("(no completed walk-forward windows -- need >= TRAIN_MONTHS+TEST_MONTHS "
                 f"= {TRAIN_MONTHS+TEST_MONTHS} months of data)")
    for k, picks in param_picks.items():
        if not picks:
            continue
        arr = pd.Series(picks)
        if arr.dtype.kind in "if":
            L.append(f"{k:18s} mean {arr.mean():.4f}  min {arr.min():.4f}  max {arr.max():.4f}  "
                     f"std {arr.std():.4f}")
            stab.append({"param": k, "mean": arr.mean(), "std": arr.std(),
                         "min": arr.min(), "max": arr.max()})
        else:
            vc = arr.value_counts()
            L.append(f"{k:18s} {dict(vc)}")
            stab.append({"param": k, "most_common": vc.index[0] if len(vc) else None})
    if stab:
        pd.DataFrame(stab).to_csv("stock_opt_param_stability.csv", index=False)
    if all_trials:
        pd.DataFrame(all_trials).to_csv("stock_opt_trials.csv", index=False)

    out = "\n".join(L)
    open("stock_opt_summary.txt", "w").write(out + "\n")
    print("\n" + out)
    print("\n[DONE] stock_opt_summary.txt, stock_opt_param_stability.csv, stock_opt_trials.csv")


if __name__ == "__main__":
    main()
