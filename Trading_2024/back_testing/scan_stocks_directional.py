"""
Multi-stock directional backtest on LIQUID, TRADEABLE names, with REAL Zerodha
intraday-equity costs logged per trade.
=============================================================================
Pipeline:
  1. Check the market is LIVE (needed to read live bid/ask spread & depth).
  2. For each stock file, query Kite live quote and keep only names where:
       - bid/ask spread is tight (<= MAX_SPREAD_PCT), and
       - the order book can absorb ORDER_VALUE_RS (~Rs 10 lakh) within
         MAX_DEPTH_SLIPPAGE_PCT of mid (i.e. Rs 10 lakh fills comfortably).
  3. Backtest the (unchanged) directional signal on the survivors.
  4. Log every trade with FULL Zerodha intraday equity charges (brokerage, STT,
     exchange txn, SEBI, stamp, GST) -> realistic net P&L in rupees.

Run LOCALLY (needs Kite session for the live liquidity screen):
    python scan_stocks_directional.py ./stocks_1min_history

Outputs:
    stocks_per_trade.csv     every trade with gross, charges, net (rupees + %)
    stocks_per_symbol.csv    per-stock net summary
    stocks_summary.txt       universe verdict + cost sensitivity
    liquid_universe.csv      the names that passed the live liquidity screen

NOTE: the live screen reflects liquidity AT RUN TIME (a snapshot). Run it during
active market hours (not pre-open / lunch lull) for a representative read.
"""

import sys
import os
import glob
import time
import numpy as np
import pandas as pd

try:
    import Trading_2024.OptionTradeUtils as oUtils
except Exception:
    try:
        import OptionTradeUtils as oUtils
    except Exception:
        oUtils = None

# ============================================================
# STRATEGY PARAMETERS (Bayesian walk-forward optimized on 20-stock subset,
# 7 OOS windows all profitable net of costs + 0.05%/side slippage).
# Converged params used directly (EMA_FAST, MIN_FAN, ATR_EXPANSION, MAX_LOSS);
# central values used where the optimizer was insensitive (EMA_SLOW, BREAKOUT).
# ============================================================
EMA_FAST, EMA_MID, EMA_SLOW = 8, 25, 48     # was 9/21/50
SLOPE_LOOKBACK = 5
BREAKOUT_LOOKBACK = 28                        # was 20 (optimizer mean ~28)
ATR_PERIOD = 14
ATR_EXP_LOOKBACK = 30
ATR_EXPANSION = 1.04                          # was 1.10
ADX_PERIOD = 14
MIN_ADX = 20.0                                # was 25.0
MIN_FAN_PCT = 0.0013                          # was 0.0008 (stronger fan filter)
# trail tiers scaled by optimized TRAIL_AGGR ~1.15
TRAIL_TIERS = [(0.0100, 1.38), (0.0060, 1.84), (0.0035, 2.53), (0.0000, 3.45)]
MAX_LOSS_PCT = 0.0034                         # was 0.004
MIN_TREND_BARS = 10
COOLDOWN_BARS = 5
PROGRESS_BARS = 8
MIN_PROGRESS_PCT = 0.0010
BREAKEVEN_AFTER_PCT = 0.0015
SQUAREOFF_TIME = "15:20"

# ============================================================
# LIVE LIQUIDITY SCREEN
# ============================================================
ORDER_VALUE_RS = 10_00_000          # the order size you want to place comfortably
MAX_SPREAD_PCT = 0.0010             # <=0.10% bid/ask spread to qualify
MAX_DEPTH_SLIPPAGE_PCT = 0.0015     # Rs 10L must fill within 0.15% of mid
QUOTE_THROTTLE_SEC = 0.34           # ~3 quote calls/sec (Kite global limit)
REQUIRE_LIVE_MARKET = True          # set False to bypass the market-hours gate (e.g. dry run)

# ============================================================
# ZERODHA INTRADAY EQUITY CHARGES (verified 2026)
#   brokerage : 0.03% of turnover or Rs 20, whichever LOWER, PER ORDER
#   STT       : 0.025% on SELL side only
#   exch txn  : 0.00297% (NSE) on turnover (both sides)
#   SEBI      : Rs 10 per crore on turnover
#   stamp     : 0.003% on BUY side only
#   GST       : 18% on (brokerage + exch txn + SEBI)
#   DP charges: not applicable to intraday
# ============================================================
BROKERAGE_PCT = 0.0003
BROKERAGE_CAP = 20.0
STT_SELL_PCT = 0.00025
EXCH_TXN_PCT = 0.0000297
SEBI_PER_CRORE = 10.0
STAMP_BUY_PCT = 0.00003
GST_PCT = 0.18


def intraday_equity_charges(buy_value: float, sell_value: float) -> float:
    """Full Zerodha intraday equity cost for one round trip (buy + sell)."""
    brokerage = min(BROKERAGE_PCT * buy_value, BROKERAGE_CAP) + \
                min(BROKERAGE_PCT * sell_value, BROKERAGE_CAP)
    stt = STT_SELL_PCT * sell_value
    turnover = buy_value + sell_value
    exch = EXCH_TXN_PCT * turnover
    sebi = SEBI_PER_CRORE * turnover / 1_00_00_000
    stamp = STAMP_BUY_PCT * buy_value
    gst = GST_PCT * (brokerage + exch + sebi)
    return brokerage + stt + exch + sebi + stamp + gst


# ============================================================
# MARKET-LIVE CHECK
# ============================================================
def market_is_live(kite) -> bool:
    """True if NSE equity is currently in normal trading session."""
    import datetime as dt
    try:
        import pytz
        now = dt.datetime.now(pytz.timezone("Asia/Kolkata"))
    except Exception:
        now = dt.datetime.now()
    if now.weekday() >= 5:                      # Sat/Sun
        return False
    t = now.time()
    if not (dt.time(9, 15) <= t <= dt.time(15, 30)):
        return False
    # best-effort holiday check: if a known liquid name returns a stale/zero quote
    try:
        q = kite.quote("NSE:RELIANCE")["NSE:RELIANCE"]
        if float(q.get("last_price") or 0) <= 0:
            return False
    except Exception:
        pass
    return True


# ============================================================
# LIVE LIQUIDITY SCREEN
# ============================================================
def depth_fill_slippage(depth_side, mid, target_value):
    """Walk the order-book side; return slippage % to fill target_value, or None
    if the visible depth can't absorb it."""
    filled_val = 0.0
    weighted_px = 0.0
    for level in depth_side:
        px = float(level.get("price") or 0)
        qty = int(level.get("quantity") or 0)
        if px <= 0 or qty <= 0:
            continue
        level_val = px * qty
        take = min(level_val, target_value - filled_val)
        weighted_px += (take / px) * px       # = take (value); track value-weighted px below
        filled_val += take
        if filled_val >= target_value:
            # value-weighted average price approximation
            return abs(px - mid) / mid          # conservative: use deepest touched level
    return None                                  # not enough visible depth


def screen_liquidity(kite, symbols):
    """Return list of symbols that pass spread + depth screen for ORDER_VALUE_RS."""
    passed, rejected = [], []
    for k, sym in enumerate(symbols, 1):
        key = f"NSE:{sym}"
        try:
            q = kite.quote(key)[key]
        except Exception as e:
            rejected.append({"symbol": sym, "reason": f"quote_err:{e}"});
            time.sleep(QUOTE_THROTTLE_SEC); continue
        ltp = float(q.get("last_price") or 0)
        depth = q.get("depth", {}) or {}
        bids = depth.get("buy", []); asks = depth.get("sell", [])
        best_bid = float(bids[0]["price"]) if bids and bids[0].get("price") else 0
        best_ask = float(asks[0]["price"]) if asks and asks[0].get("price") else 0
        time.sleep(QUOTE_THROTTLE_SEC)
        if best_bid <= 0 or best_ask <= 0 or ltp <= 0:
            rejected.append({"symbol": sym, "reason": "no_quote"}); continue
        mid = (best_bid + best_ask) / 2
        spread_pct = (best_ask - best_bid) / mid
        if spread_pct > MAX_SPREAD_PCT:
            rejected.append({"symbol": sym, "reason": f"wide_spread {spread_pct*100:.3f}%"})
            print(f"[SCREEN {k}] {sym}: REJECT spread {spread_pct*100:.3f}%")
            continue
        buy_slip = depth_fill_slippage(asks, mid, ORDER_VALUE_RS)
        sell_slip = depth_fill_slippage(bids, mid, ORDER_VALUE_RS)
        if buy_slip is None or sell_slip is None:
            rejected.append({"symbol": sym, "reason": "thin_depth"})
            print(f"[SCREEN {k}] {sym}: REJECT thin depth for Rs {ORDER_VALUE_RS:,.0f}")
            continue
        worst = max(buy_slip, sell_slip)
        if worst > MAX_DEPTH_SLIPPAGE_PCT:
            rejected.append({"symbol": sym, "reason": f"depth_slip {worst*100:.3f}%"})
            print(f"[SCREEN {k}] {sym}: REJECT depth slippage {worst*100:.3f}%")
            continue
        passed.append({"symbol": sym, "spread_pct": round(spread_pct*100, 4),
                       "depth_slip_pct": round(worst*100, 4), "ltp": ltp})
        print(f"[SCREEN {k}] {sym}: PASS spread {spread_pct*100:.3f}% depth_slip {worst*100:.3f}%")
    return passed, rejected


# ============================================================
# INDICATORS + SCAN  (unchanged signal)
# ============================================================
def add_indicators(g):
    g = g.copy()
    c = g["close"]
    g["ema_f"] = c.ewm(span=EMA_FAST, adjust=False).mean()
    g["ema_m"] = c.ewm(span=EMA_MID, adjust=False).mean()
    g["ema_s"] = c.ewm(span=EMA_SLOW, adjust=False).mean()
    g["slope_s"] = g["ema_s"].diff(SLOPE_LOOKBACK)
    prev_c = c.shift(1)
    tr = pd.concat([g["high"] - g["low"], (g["high"] - prev_c).abs(),
                    (g["low"] - prev_c).abs()], axis=1).max(axis=1)
    g["atr"] = tr.ewm(span=ATR_PERIOD, adjust=False).mean()
    g["roll_hi"] = g["high"].rolling(BREAKOUT_LOOKBACK).max()
    g["roll_lo"] = g["low"].rolling(BREAKOUT_LOOKBACK).min()
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


def scan_day(g, symbol, qty):
    g = add_indicators(g).reset_index(drop=True)
    n = len(g)
    if n < 2:
        return []
    close = g["close"].to_numpy(); high = g["high"].to_numpy(); low = g["low"].to_numpy()
    atr = g["atr"].to_numpy(); ema_f = g["ema_f"].to_numpy(); ema_m = g["ema_m"].to_numpy()
    ema_s = g["ema_s"].to_numpy(); slope_s = g["slope_s"].to_numpy()
    roll_hi = g["roll_hi"].to_numpy(); roll_lo = g["roll_lo"].to_numpy()
    fan = g["fan_pct"].to_numpy(); atr_ratio = g["atr_ratio"].to_numpy(); adx = g["adx"].to_numpy()
    tarr = g["t"].to_numpy(); date_arr = g["date"].to_numpy()
    day0 = g.iloc[0]["day"]
    eps = []
    i = BREAKOUT_LOOKBACK + SLOPE_LOOKBACK
    last_exit = -10_000
    while i < n - 1:
        if i - last_exit < COOLDOWN_BARS:
            i += 1; continue
        if np.isnan(slope_s[i]) or np.isnan(roll_hi[i]) or np.isnan(adx[i]) or np.isnan(atr_ratio[i]):
            i += 1; continue
        regime = (adx[i] >= MIN_ADX) and (fan[i] >= MIN_FAN_PCT) and (atr_ratio[i] >= ATR_EXPANSION)
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
                atr_mult = next(m for thr, m in TRAIL_TIERS if fav >= thr)
                trail = extreme - atr_mult * atr[j]
                if held >= PROGRESS_BARS and fav < MIN_PROGRESS_PCT: exit_idx = j; break
                if be_armed and close[j] <= entry_px: exit_idx = j; break
                if close[j] <= entry_px * (1 - MAX_LOSS_PCT): exit_idx = j; break
                if close[j] < trail: exit_idx = j; break
                if ema_f[j] < ema_m[j]: exit_idx = j; break
            else:
                if low[j] < extreme: extreme = low[j]
                fav = (entry_px - extreme) / entry_px
                if fav >= BREAKEVEN_AFTER_PCT: be_armed = True
                atr_mult = next(m for thr, m in TRAIL_TIERS if fav >= thr)
                trail = extreme + atr_mult * atr[j]
                if held >= PROGRESS_BARS and fav < MIN_PROGRESS_PCT: exit_idx = j; break
                if be_armed and close[j] >= entry_px: exit_idx = j; break
                if close[j] >= entry_px * (1 + MAX_LOSS_PCT): exit_idx = j; break
                if close[j] > trail: exit_idx = j; break
                if ema_f[j] > ema_m[j]: exit_idx = j; break
            j += 1
        if exit_idx is None:
            exit_idx = n - 1
        bars = exit_idx - entry_idx
        if bars >= MIN_TREND_BARS:
            exit_px = close[exit_idx]
            # qty sized to ORDER_VALUE_RS at entry price
            shares = max(1, int(ORDER_VALUE_RS / entry_px))
            if direction == "up":
                buy_val = entry_px * shares; sell_val = exit_px * shares
            else:
                # short intraday: sell at entry, buy back at exit
                sell_val = entry_px * shares; buy_val = exit_px * shares
            pnl_pts = (exit_px - entry_px) if direction == "up" else (entry_px - exit_px)
            gross_rs = pnl_pts * shares
            charges = intraday_equity_charges(buy_val, sell_val)
            net_rs = gross_rs - charges
            pnl_pct_gross = pnl_pts / entry_px * 100
            eps.append({"symbol": symbol, "day": day0, "entry_time": date_arr[entry_idx],
                        "direction": direction, "shares": shares,
                        "entry_px": round(entry_px, 2), "exit_px": round(exit_px, 2),
                        "pnl_pct_gross": round(pnl_pct_gross, 4),
                        "gross_rs": round(gross_rs, 2), "charges_rs": round(charges, 2),
                        "net_rs": round(net_rs, 2), "bars": bars})
        last_exit = exit_idx
        i = exit_idx + 1
    return eps


def load_stock(path):
    df = pd.read_parquet(path) if path.endswith(".parquet") else pd.read_csv(path)
    df.columns = [c.lower() for c in df.columns]
    if "volume" not in df.columns:
        df["volume"] = 0
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").drop_duplicates("date").reset_index(drop=True)
    df["day"] = df["date"].dt.date
    df["t"] = df["date"].dt.strftime("%H:%M")
    return df


def main():
    d = sys.argv[1] if len(sys.argv) > 1 else "../historic_data_fetcher/stocks_1min_history"
    files = sorted(glob.glob(os.path.join(d, "*.parquet")) + glob.glob(os.path.join(d, "*.csv")))
    files = [f for f in files if not os.path.basename(f).startswith("_")]
    print(f"[INFO] {len(files)} stock files in {d}")
    if not files:
        print("[ERR] no files"); return
    all_syms = [os.path.splitext(os.path.basename(f))[0] for f in files]

    # ---- 1. market live + 2. live liquidity screen ----
    if oUtils is None:
        print("[ERR] OptionTradeUtils not importable; live screen needs Kite.")
        return
    kite = oUtils.intialize_kite_api()
    if REQUIRE_LIVE_MARKET and not market_is_live(kite):
        print("[ABORT] Market is not live. The liquidity screen needs live bid/ask/depth.")
        print("        Run during NSE hours (Mon-Fri 09:15-15:30 IST), or set "
              "REQUIRE_LIVE_MARKET=False to skip (screen results will be meaningless).")
        return
    print(f"[INFO] Market live. Screening {len(all_syms)} names for spread<= "
          f"{MAX_SPREAD_PCT*100:.2f}% and Rs {ORDER_VALUE_RS:,.0f} depth...")
    passed, rejected = screen_liquidity(kite, all_syms)
    liquid = pd.DataFrame(passed)
    liquid.to_csv("liquid_universe.csv", index=False)
    print(f"[INFO] {len(passed)} passed liquidity screen, {len(rejected)} rejected")
    if not passed:
        print("[ABORT] No names passed the liquidity screen."); return
    liquid_syms = set(liquid["symbol"])

    # ---- 3-4. backtest survivors with full intraday costs ----
    all_trades = []
    fmap = {os.path.splitext(os.path.basename(f))[0]: f for f in files}
    for k, sym in enumerate(sorted(liquid_syms), 1):
        try:
            df = load_stock(fmap[sym])
        except Exception as e:
            print(f"[{k}] {sym}: load error {e}"); continue
        eps = []
        for _, g in df.groupby("day"):
            if len(g) < BREAKOUT_LOOKBACK + EMA_SLOW:
                continue
            eps.extend(scan_day(g, sym, ORDER_VALUE_RS))
        all_trades.extend(eps)
        if eps:
            e = pd.DataFrame(eps)
            print(f"[{k}] {sym}: {len(e)} trades, win {100*(e.net_rs>0).mean():.0f}%, "
                  f"net Rs {e.net_rs.sum():,.0f} (charges Rs {e.charges_rs.sum():,.0f})")
    if not all_trades:
        print("[WARN] no trades"); return

    t = pd.DataFrame(all_trades)
    t.to_csv("stocks_per_trade.csv", index=False)

    per_sym = t.groupby("symbol").agg(
        trades=("net_rs", "count"),
        win_rate=("net_rs", lambda s: round(100 * (s > 0).mean(), 1)),
        gross_rs=("gross_rs", "sum"),
        charges_rs=("charges_rs", "sum"),
        net_rs=("net_rs", "sum"),
        avg_net_rs=("net_rs", "mean"),
    ).reset_index().sort_values("net_rs", ascending=False)
    per_sym.to_csv("stocks_per_symbol.csv", index=False)

    L = []
    L.append("=== LIQUID-UNIVERSE BACKTEST (real Zerodha intraday equity costs) ===")
    L.append(f"Order size per trade: Rs {ORDER_VALUE_RS:,.0f}")
    L.append(f"Liquidity screen: spread<={MAX_SPREAD_PCT*100:.2f}%, "
             f"Rs {ORDER_VALUE_RS:,.0f} fills within {MAX_DEPTH_SLIPPAGE_PCT*100:.2f}% of mid")
    L.append(f"Stocks passed screen: {len(liquid_syms)}")
    L.append(f"Total trades: {len(t)}")
    L.append("")
    L.append("--- NET RESULTS (after full charges) ---")
    L.append(f"Overall win rate (net): {100*(t['net_rs']>0).mean():.1f}%")
    L.append(f"Gross Rs {t['gross_rs'].sum():,.0f}  Charges Rs {t['charges_rs'].sum():,.0f}  "
             f"Net Rs {t['net_rs'].sum():,.0f}")
    L.append(f"Avg net/trade Rs {t['net_rs'].mean():,.0f}  Median Rs {t['net_rs'].median():,.0f}")
    L.append(f"Charges as % of gross: {100*t['charges_rs'].sum()/max(1,t['gross_rs'].sum()):.1f}%")
    prof = (per_sym["net_rs"] > 0)
    L.append(f"Stocks net-profitable: {prof.sum()}/{len(per_sym)} ({100*prof.mean():.0f}%)")
    L.append("")
    # spread-slippage sensitivity: subtract an extra per-trade slippage in % of notional
    L.append("--- SLIPPAGE SENSITIVITY (extra cost beyond charges, on top of fills) ---")
    L.append(f"{'slip%/side':>10s} {'net_total_Rs':>14s} {'win%':>7s} {'stocks_prof%':>13s}")
    for slip in [0.0, 0.0005, 0.0010, 0.0020]:
        extra = (t['entry_px'] * t['shares'] + t['exit_px'] * t['shares']) * slip
        net2 = t['net_rs'] - extra
        sp = 100 * (net2.groupby(t['symbol']).sum() > 0).mean()
        L.append(f"{slip*100:9.3f}% {net2.sum():14,.0f} {100*(net2>0).mean():6.1f} {sp:12.0f}%")
    L.append("")
    L.append("Top 10 by net:")
    for _, r in per_sym.head(10).iterrows():
        L.append(f"  {r['symbol']:14s} n={int(r['trades']):4d} win {r['win_rate']:.0f}% net Rs {r['net_rs']:,.0f}")
    L.append("Bottom 10 by net:")
    for _, r in per_sym.tail(10).iterrows():
        L.append(f"  {r['symbol']:14s} n={int(r['trades']):4d} win {r['win_rate']:.0f}% net Rs {r['net_rs']:,.0f}")

    out = "\n".join(L)
    open("stocks_summary.txt", "w").write(out + "\n")
    print("\n" + out)
    print("\n[DONE] stocks_per_trade.csv, stocks_per_symbol.csv, stocks_summary.txt, liquid_universe.csv")


if __name__ == "__main__":
    main()
