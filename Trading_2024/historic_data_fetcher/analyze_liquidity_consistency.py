"""
Liquidity CONSISTENCY analyzer.
=============================================================================
Finds stocks that are reliably liquid DAY AFTER DAY (not just on average),
using per-minute turnover (price * volume) from your 1-min history as a proxy
for spread tightness / depth. For scaling lots, you want names whose liquidity
holds up on their WORST days, not just their best -- so this ranks by the
low-percentile of daily turnover, not the mean.

Run LOCALLY:
    python analyze_liquidity_consistency.py ./stocks_1min_history

Outputs:
    liquidity_consistency.csv  per-stock liquidity stats, ranked
    (prints the top consistently-liquid names by sector if a sector map exists)

Why turnover as a proxy: high, STABLE per-minute turnover almost always means
tight spreads and deep books -- the conditions that keep slippage low. A stock
with high average turnover but high variability can be thin on quiet days,
which is exactly when scaled-up orders slip. So we reward HIGH and STEADY.
"""

import sys
import os
import glob
import numpy as np
import pandas as pd

# trading window for intraday liquidity (ignore pre/post)
SESSION_START = "09:15"
SESSION_END = "15:30"
# the order size you want to place; used to express turnover in "how many of my
# orders could the average minute absorb"
ORDER_VALUE_RS = 10_00_000


def load(path):
    df = pd.read_parquet(path) if path.endswith(".parquet") else pd.read_csv(path)
    df.columns = [c.lower() for c in df.columns]
    if "volume" not in df.columns:
        return None
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").drop_duplicates("date")
    df["t"] = df["date"].dt.strftime("%H:%M")
    df = df[(df["t"] >= SESSION_START) & (df["t"] <= SESSION_END)]
    df["day"] = df["date"].dt.date
    df["turnover"] = df["close"] * df["volume"]
    return df


def main():
    d = sys.argv[1] if len(sys.argv) > 1 else "./stocks_1min_history"
    files = sorted(glob.glob(os.path.join(d, "*.parquet")) + glob.glob(os.path.join(d, "*.csv")))
    files = [f for f in files if not os.path.basename(f).startswith("_")]
    print(f"[INFO] {len(files)} files in {d}")
    rows = []
    for k, f in enumerate(files, 1):
        sym = os.path.splitext(os.path.basename(f))[0]
        df = load(f)
        if df is None or df.empty:
            continue
        # per-DAY median minute turnover (typical liquidity that day)
        daily_med = df.groupby("day")["turnover"].median()
        if len(daily_med) < 30:
            continue
        # consistency metrics:
        #   median across days (typical liquidity)
        #   10th percentile across days (liquidity on a THIN day -- the risk case)
        #   coefficient of variation (lower = steadier)
        med = daily_med.median()
        p10 = daily_med.quantile(0.10)
        cv = daily_med.std() / daily_med.mean() if daily_med.mean() > 0 else np.nan
        # fraction of days where typical-minute turnover comfortably exceeds order size
        # (a minute that turns over >= ORDER_VALUE means your order is a fraction of 1 min)
        days_deep = (daily_med >= ORDER_VALUE_RS).mean()
        rows.append({
            "symbol": sym,
            "days": len(daily_med),
            "median_min_turnover": round(med),
            "p10_min_turnover": round(p10),       # thin-day liquidity (key for scaling)
            "turnover_cv": round(cv, 2),          # lower = more consistent
            "pct_days_deep": round(100 * days_deep, 1),
        })
        if k % 25 == 0:
            print(f"  ...{k}/{len(files)}")
    res = pd.DataFrame(rows)
    if res.empty:
        print("[WARN] no usable data (need volume column)"); return

    # CONSISTENT-LIQUIDITY SCORE:
    # high p10 turnover (liquid even on thin days) AND low CV (steady) AND deep most days
    res["s_p10"] = np.log1p(res["p10_min_turnover"])
    res["s_p10"] = (res["s_p10"] - res["s_p10"].min()) / (res["s_p10"].max() - res["s_p10"].min())
    res["s_cv"] = 1 - (res["turnover_cv"] - res["turnover_cv"].min()) / \
                      (res["turnover_cv"].max() - res["turnover_cv"].min())
    res["s_deep"] = res["pct_days_deep"] / 100
    res["liq_consistency"] = 0.5 * res["s_p10"] + 0.2 * res["s_cv"] + 0.3 * res["s_deep"]
    res = res.sort_values("liq_consistency", ascending=False)
    res.to_csv("liquidity_consistency.csv", index=False)

    print("\n=== TOP 30 CONSISTENTLY-LIQUID STOCKS ===")
    print("(ranked by liquidity on THIN days + steadiness, not just average)")
    print(f"{'symbol':12s}{'median_TO':>12}{'p10_TO(thin)':>14}{'cv':>6}{'%days_deep':>11}")
    for _, r in res.head(30).iterrows():
        print(f"{r['symbol']:12s}{r['median_min_turnover']:>12,}{r['p10_min_turnover']:>14,}"
              f"{r['turnover_cv']:>6.2f}{r['pct_days_deep']:>11.0f}")
    print(f"\n[DONE] liquidity_consistency.csv ({len(res)} stocks)")
    print("Read: p10_TO is per-minute turnover on a THIN (10th-pct) day. For Rs 10L")
    print("orders, you want p10 well above your order size so even quiet days absorb you.")


if __name__ == "__main__":
    main()
