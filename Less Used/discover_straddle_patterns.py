"""
SHORT-STRADDLE PATTERN DISCOVERY
=================================
Scans all pickle data to extract market conditions at entry time,
runs the backtest to get trade outcomes, then finds which conditions
predict profitable vs unprofitable short straddles.

Focuses on the two things you've observed matter most:
  1. Underlying movement (before and after entry)
  2. Premium characteristics

Place in the same directory as dhan_atm_straddle_prem_jump_reattempt_prem_perc.py

Usage:
    python discover_straddle_patterns.py
    python discover_straddle_patterns.py --lookback-months 12
    python discover_straddle_patterns.py --entry-time 09:25

Output:  ~/Downloads/straddle_patterns.xlsx
"""

import os, sys, glob, time, argparse, warnings
from datetime import date, time as dtime, datetime
from typing import Dict, List, Any, Optional
from pathlib import Path
from dataclasses import asdict

import pandas as pd
import numpy as np

warnings.filterwarnings("ignore")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

try:
    import dhan_atm_straddle_prem_jump_reattempt_prem_perc as bt
except ImportError as e:
    print(f"[ERROR] Cannot import backtester: {e}")
    sys.exit(1)


# =============================================================================
# FEATURE EXTRACTION — one row per (underlying, day, expiry)
# =============================================================================

def extract_features_and_outcome(
    und: str, dy: date, expiry: date,
    day_opt: pd.DataFrame, source_pickle: str,
    entry_time: dtime,
    prev_close_spot: Optional[float],
) -> Optional[Dict[str, Any]]:
    """
    Extract market features at entry time from minute-level data,
    then run the backtest simulation to get the trade outcome.
    Returns a single dict with features + outcome, or None if data is insufficient.
    """
    idx_all = bt.build_minute_index(dy, bt.SESSION_START_IST, bt.SESSION_END_IST)
    session_end_ts = idx_all[-1]
    qty = int(bt.QTY_UNITS[und])
    step = int(bt.STRIKE_STEP[und])
    dte = (expiry - dy).days

    # --- Build spot series for the full session ---
    spot_s = bt._build_underlying_series_from_spot(day_opt, idx_all)
    if spot_s.dropna().empty:
        return None

    entry_ts = pd.Timestamp(datetime.combine(dy, entry_time), tz=bt.ist_tz())
    if entry_ts not in idx_all:
        return None

    spot_at_entry = spot_s.loc[entry_ts]
    if pd.isna(spot_at_entry):
        return None
    spot_at_entry = float(spot_at_entry)

    # Session open (first available spot)
    spot_open_ts = spot_s.dropna().index[0]
    spot_open = float(spot_s.loc[spot_open_ts])

    # ATM strike and premiums at entry
    atm = bt.round_to_step(spot_at_entry, step)
    ce_close = bt._build_leg_series_fixed_strike(day_opt, idx_all, atm, "CE", "close_f")
    pe_close = bt._build_leg_series_fixed_strike(day_opt, idx_all, atm, "PE", "close_f")
    ce_entry = ce_close.loc[entry_ts]
    pe_entry = pe_close.loc[entry_ts]
    if pd.isna(ce_entry) or pd.isna(pe_entry):
        return None
    ce_entry = float(ce_entry)
    pe_entry = float(pe_entry)
    premium_sum = ce_entry + pe_entry

    if premium_sum <= 0 or spot_at_entry <= 0:
        return None

    # =====================================================================
    # FEATURE GROUP 1: UNDERLYING MOVEMENT (your primary driver)
    # =====================================================================

    # 1a. Gap from previous day's close
    gap_pct = np.nan
    if prev_close_spot is not None and prev_close_spot > 0:
        gap_pct = (spot_open - prev_close_spot) / prev_close_spot * 100

    # 1b. Pre-entry move: 09:15 open → entry time
    pre_entry_move_pct = (spot_at_entry - spot_open) / spot_open * 100
    pre_entry_abs_move_pct = abs(pre_entry_move_pct)

    # 1c. Pre-entry range (high - low before entry as % of open)
    pre_entry_spot = spot_s.loc[:entry_ts].dropna()
    if len(pre_entry_spot) >= 2:
        pre_entry_high = float(pre_entry_spot.max())
        pre_entry_low = float(pre_entry_spot.min())
        pre_entry_range_pct = (pre_entry_high - pre_entry_low) / spot_open * 100
    else:
        pre_entry_range_pct = 0.0

    # 1d. Spot momentum: rate of change in last 5 minutes before entry
    t_minus_5 = entry_ts - pd.Timedelta(minutes=5)
    spot_5m_ago = spot_s.loc[t_minus_5] if t_minus_5 in spot_s.index and pd.notna(spot_s.loc[t_minus_5]) else np.nan
    momentum_5m_pct = np.nan
    if not np.isnan(spot_5m_ago) and spot_5m_ago > 0:
        momentum_5m_pct = (spot_at_entry - float(spot_5m_ago)) / float(spot_5m_ago) * 100

    # 1e. Post-entry maximum move (how much did the underlying actually move after entry)
    post_entry_spot = spot_s.loc[entry_ts:].dropna()
    if len(post_entry_spot) >= 2:
        post_high = float(post_entry_spot.max())
        post_low = float(post_entry_spot.min())
        post_entry_max_move_up_pct = (post_high - spot_at_entry) / spot_at_entry * 100
        post_entry_max_move_down_pct = (spot_at_entry - post_low) / spot_at_entry * 100
        post_entry_max_abs_move_pct = max(post_entry_max_move_up_pct, post_entry_max_move_down_pct)
        post_entry_range_pct = (post_high - post_low) / spot_at_entry * 100
        # Spot at EOD
        spot_eod = float(post_entry_spot.iloc[-1])
        spot_eod_move_pct = (spot_eod - spot_at_entry) / spot_at_entry * 100
    else:
        post_entry_max_move_up_pct = 0.0
        post_entry_max_move_down_pct = 0.0
        post_entry_max_abs_move_pct = 0.0
        post_entry_range_pct = 0.0
        spot_eod_move_pct = 0.0

    # 1f. Full-day intraday range
    full_day_spot = spot_s.dropna()
    if len(full_day_spot) >= 2:
        full_day_range_pct = (float(full_day_spot.max()) - float(full_day_spot.min())) / spot_open * 100
    else:
        full_day_range_pct = 0.0

    # =====================================================================
    # FEATURE GROUP 2: PREMIUM CHARACTERISTICS
    # =====================================================================

    # 2a. Premium as % of spot (IV proxy)
    premium_pct_of_spot = premium_sum / spot_at_entry * 100

    # 2b. CE/PE skew (>1 means CE is more expensive = market expects upside)
    ce_pe_ratio = ce_entry / pe_entry if pe_entry > 0 else np.nan

    # 2c. Premium decay rate: how fast did premium drop in first 5 min after entry
    ce_5m_later = ce_close.loc[entry_ts + pd.Timedelta(minutes=5)] if (entry_ts + pd.Timedelta(minutes=5)) in ce_close.index else np.nan
    pe_5m_later = pe_close.loc[entry_ts + pd.Timedelta(minutes=5)] if (entry_ts + pd.Timedelta(minutes=5)) in pe_close.index else np.nan
    early_decay_pct = np.nan
    if pd.notna(ce_5m_later) and pd.notna(pe_5m_later):
        premium_5m = float(ce_5m_later) + float(pe_5m_later)
        early_decay_pct = (premium_sum - premium_5m) / premium_sum * 100  # positive = decaying (good)

    # =====================================================================
    # FEATURE GROUP 3: TIME/CALENDAR
    # =====================================================================
    day_of_week = pd.Timestamp(dy).dayofweek  # 0=Mon, 4=Fri
    day_of_week_name = pd.Timestamp(dy).day_name()
    week_of_month = (pd.Timestamp(dy).day - 1) // 7 + 1
    is_expiry_day = 1 if dte == 0 else 0
    month = pd.Timestamp(dy).month

    # =====================================================================
    # RUN BACKTEST TO GET OUTCOME (first trade only)
    # =====================================================================
    try:
        trades, _ = bt.simulate_day_multi_trades_dhan(
            und=und, dy=dy, expiry=expiry,
            day_opt=day_opt, source_pickle=source_pickle,
        )
    except Exception:
        return None

    if not trades:
        return None

    first_trade = trades[0]  # first attempt
    t = asdict(first_trade)

    # Also compute net daily PnL across all attempts
    daily_pnl = sum(asdict(tr)["exit_pnl"] for tr in trades)
    n_attempts = len(trades)
    any_eod = any(asdict(tr)["exit_reason"] == "EOD" for tr in trades)

    return {
        # --- Identifiers ---
        "day": dy,
        "underlying": und,
        "expiry": expiry,
        "days_to_expiry": dte,
        "source_pickle": source_pickle,

        # --- Feature Group 1: Underlying movement ---
        "spot_open": spot_open,
        "spot_at_entry": spot_at_entry,
        "gap_from_prev_close_pct": round(gap_pct, 4) if not np.isnan(gap_pct) else np.nan,
        "abs_gap_pct": round(abs(gap_pct), 4) if not np.isnan(gap_pct) else np.nan,
        "pre_entry_move_pct": round(pre_entry_move_pct, 4),
        "pre_entry_abs_move_pct": round(pre_entry_abs_move_pct, 4),
        "pre_entry_range_pct": round(pre_entry_range_pct, 4),
        "momentum_5m_pct": round(momentum_5m_pct, 4) if not np.isnan(momentum_5m_pct) else np.nan,
        "abs_momentum_5m_pct": round(abs(momentum_5m_pct), 4) if not np.isnan(momentum_5m_pct) else np.nan,
        "post_entry_max_up_pct": round(post_entry_max_move_up_pct, 4),
        "post_entry_max_down_pct": round(post_entry_max_move_down_pct, 4),
        "post_entry_max_abs_move_pct": round(post_entry_max_abs_move_pct, 4),
        "post_entry_range_pct": round(post_entry_range_pct, 4),
        "spot_eod_move_pct": round(spot_eod_move_pct, 4),
        "full_day_range_pct": round(full_day_range_pct, 4),

        # --- Feature Group 2: Premiums ---
        "entry_ce": ce_entry,
        "entry_pe": pe_entry,
        "premium_sum_points": round(premium_sum, 2),
        "premium_pct_of_spot": round(premium_pct_of_spot, 4),
        "ce_pe_ratio": round(ce_pe_ratio, 4) if not np.isnan(ce_pe_ratio) else np.nan,
        "early_decay_pct": round(early_decay_pct, 4) if not np.isnan(early_decay_pct) else np.nan,

        # --- Feature Group 3: Calendar ---
        "day_of_week": day_of_week,
        "day_of_week_name": day_of_week_name,
        "week_of_month": week_of_month,
        "is_expiry_day": is_expiry_day,
        "month": month,

        # --- Outcome (first trade) ---
        "first_trade_exit_reason": t["exit_reason"],
        "first_trade_exit_pnl": t["exit_pnl"],
        "first_trade_max_profit": t["max_profit"],
        "first_trade_max_loss": t["max_loss"],

        # --- Outcome (full day, all attempts) ---
        "daily_net_pnl": daily_pnl,
        "n_attempts": n_attempts,
        "reached_eod": 1 if any_eod else 0,
        "daily_success": 1 if daily_pnl > 0 else 0,
    }


# =============================================================================
# ANALYSIS ENGINE
# =============================================================================

def _bucket_analysis(df: pd.DataFrame, feature: str, outcome: str,
                     n_buckets: int = 5) -> pd.DataFrame:
    """Split feature into equal-frequency buckets, compute stats per bucket."""
    col = pd.to_numeric(df[feature], errors="coerce")
    valid = df[col.notna()].copy()
    if len(valid) < n_buckets * 5:
        return pd.DataFrame()

    valid["_bucket"] = pd.qcut(col[col.notna()], q=n_buckets, duplicates="drop")
    if valid["_bucket"].nunique() < 2:
        return pd.DataFrame()

    result = valid.groupby("_bucket", observed=True).agg(
        trades=(outcome, "count"),
        win_rate=(outcome, lambda x: (pd.to_numeric(x, errors="coerce") > 0).mean() * 100),
        avg_pnl=(outcome, lambda x: pd.to_numeric(x, errors="coerce").mean()),
        total_pnl=(outcome, lambda x: pd.to_numeric(x, errors="coerce").sum()),
        median_pnl=(outcome, lambda x: pd.to_numeric(x, errors="coerce").median()),
        avg_feature=(feature, lambda x: pd.to_numeric(x, errors="coerce").mean()),
    ).reset_index()
    result.columns = ["bucket", "trades", "win_rate_pct", "avg_pnl", "total_pnl",
                       "median_pnl", f"avg_{feature}"]
    result["feature"] = feature
    return result


def _correlation_analysis(df: pd.DataFrame, features: List[str],
                          outcome: str) -> pd.DataFrame:
    """Compute correlation + rank-correlation of each feature with outcome."""
    rows = []
    outcome_vals = pd.to_numeric(df[outcome], errors="coerce")
    for f in features:
        f_vals = pd.to_numeric(df[f], errors="coerce")
        mask = f_vals.notna() & outcome_vals.notna()
        if mask.sum() < 30:
            continue
        x = f_vals[mask]
        y = outcome_vals[mask]

        pearson = x.corr(y)
        spearman = x.rank().corr(y.rank())

        # Win rate in top vs bottom quintile
        top_20 = y[x >= x.quantile(0.80)]
        bot_20 = y[x <= x.quantile(0.20)]
        top_wr = (top_20 > 0).mean() * 100 if len(top_20) > 5 else np.nan
        bot_wr = (bot_20 > 0).mean() * 100 if len(bot_20) > 5 else np.nan
        wr_spread = top_wr - bot_wr if not (np.isnan(top_wr) or np.isnan(bot_wr)) else np.nan

        top_avg = top_20.mean() if len(top_20) > 5 else np.nan
        bot_avg = bot_20.mean() if len(bot_20) > 5 else np.nan

        rows.append({
            "feature": f,
            "pearson_corr": round(pearson, 4),
            "spearman_corr": round(spearman, 4),
            "top_20pct_win_rate": round(top_wr, 1) if not np.isnan(top_wr) else np.nan,
            "bottom_20pct_win_rate": round(bot_wr, 1) if not np.isnan(bot_wr) else np.nan,
            "win_rate_spread": round(wr_spread, 1) if not np.isnan(wr_spread) else np.nan,
            "top_20pct_avg_pnl": round(top_avg, 0) if not np.isnan(top_avg) else np.nan,
            "bottom_20pct_avg_pnl": round(bot_avg, 0) if not np.isnan(bot_avg) else np.nan,
            "n_valid": int(mask.sum()),
        })

    result = pd.DataFrame(rows)
    if not result.empty:
        result = result.sort_values("spearman_corr", key=abs, ascending=False).reset_index(drop=True)
    return result


def _categorical_analysis(df: pd.DataFrame, feature: str,
                          outcome: str) -> pd.DataFrame:
    """Win rate and PnL per category value."""
    valid = df[df[feature].notna()].copy()
    if len(valid) < 10:
        return pd.DataFrame()

    result = valid.groupby(feature).agg(
        trades=(outcome, "count"),
        win_rate=(outcome, lambda x: (pd.to_numeric(x, errors="coerce") > 0).mean() * 100),
        avg_pnl=(outcome, lambda x: pd.to_numeric(x, errors="coerce").mean()),
        total_pnl=(outcome, lambda x: pd.to_numeric(x, errors="coerce").sum()),
    ).reset_index()
    result.columns = [feature, "trades", "win_rate_pct", "avg_pnl", "total_pnl"]
    return result.sort_values("avg_pnl", ascending=False).reset_index(drop=True)


def run_analysis(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Run the full pattern analysis suite. Returns dict of sheet_name -> DataFrame."""
    sheets = {}
    outcome = "daily_net_pnl"

    # --- Features lists ---
    movement_features = [
        "gap_from_prev_close_pct", "abs_gap_pct",
        "pre_entry_move_pct", "pre_entry_abs_move_pct", "pre_entry_range_pct",
        "momentum_5m_pct",
        "abs_momentum_5m_pct",
        "post_entry_max_abs_move_pct", "post_entry_range_pct",
        "spot_eod_move_pct", "full_day_range_pct",
    ]
    premium_features = [
        "premium_sum_points", "premium_pct_of_spot",
        "ce_pe_ratio", "early_decay_pct",
    ]
    all_numeric_features = movement_features + premium_features
    categorical_features = ["day_of_week_name", "is_expiry_day", "days_to_expiry", "month"]

    # 1. RAW DATA (for user to explore)
    sheets["raw_data"] = df

    # 2. CORRELATION RANKING (the most important sheet)
    corr = _correlation_analysis(df, all_numeric_features, outcome)
    if not corr.empty:
        sheets["feature_correlations"] = corr

    # 3. BUCKET ANALYSIS for each important feature
    for f in all_numeric_features:
        bucket = _bucket_analysis(df, f, outcome, n_buckets=5)
        if not bucket.empty:
            safe_name = f[:20]
            sheets[f"bucket_{safe_name}"] = bucket

    # 4. CATEGORICAL ANALYSIS
    for f in categorical_features:
        cat = _categorical_analysis(df, f, outcome)
        if not cat.empty:
            sheets[f"cat_{f[:25]}"] = cat

    # 5. CROSS-TAB: underlying movement buckets × premium level
    if "post_entry_max_abs_move_pct" in df.columns and "premium_pct_of_spot" in df.columns:
        try:
            tmp = df.copy()
            tmp["move_bucket"] = pd.qcut(
                pd.to_numeric(tmp["post_entry_max_abs_move_pct"], errors="coerce"),
                q=4, labels=["low_move", "med_low", "med_high", "high_move"],
                duplicates="drop"
            )
            tmp["prem_bucket"] = pd.qcut(
                pd.to_numeric(tmp["premium_pct_of_spot"], errors="coerce"),
                q=3, labels=["low_prem", "med_prem", "high_prem"],
                duplicates="drop"
            )
            cross = tmp.groupby(["move_bucket", "prem_bucket"], observed=True).agg(
                trades=(outcome, "count"),
                win_rate=(outcome, lambda x: (pd.to_numeric(x, errors="coerce") > 0).mean() * 100),
                avg_pnl=(outcome, lambda x: pd.to_numeric(x, errors="coerce").mean()),
                total_pnl=(outcome, lambda x: pd.to_numeric(x, errors="coerce").sum()),
            ).reset_index()
            sheets["cross_move_x_premium"] = cross
        except Exception:
            pass

    # 6. SUMMARY TABLE: pre-entry features only (what you can observe before trading)
    pre_trade_features = [
        "gap_from_prev_close_pct", "abs_gap_pct",
        "pre_entry_move_pct", "pre_entry_abs_move_pct", "pre_entry_range_pct",
        "momentum_5m_pct",
        "abs_momentum_5m_pct",
        "premium_sum_points", "premium_pct_of_spot", "ce_pe_ratio",
    ]
    pre_corr = _correlation_analysis(df, pre_trade_features, outcome)
    if not pre_corr.empty:
        sheets["pre_entry_correlations"] = pre_corr

    # 7. KEY FINDING: "what if we skipped high-movement days?"
    try:
        col = pd.to_numeric(df["pre_entry_abs_move_pct"], errors="coerce")
        outcome_col = pd.to_numeric(df[outcome], errors="coerce")
        thresholds = [0.2, 0.3, 0.4, 0.5, 0.6, 0.8, 1.0, 1.5]
        skip_rows = []
        total_trades = len(df)
        total_wr = (outcome_col > 0).mean() * 100
        total_pnl = outcome_col.sum()
        skip_rows.append({
            "filter": "NO FILTER (all trades)",
            "trades": total_trades,
            "win_rate_pct": round(total_wr, 1),
            "avg_pnl": round(outcome_col.mean(), 0),
            "total_pnl": round(total_pnl, 0),
        })
        for t in thresholds:
            mask = col <= t
            if mask.sum() < 10:
                continue
            sub = outcome_col[mask]
            skip_rows.append({
                "filter": f"pre_entry_abs_move <= {t}%",
                "trades": int(mask.sum()),
                "skipped": total_trades - int(mask.sum()),
                "win_rate_pct": round((sub > 0).mean() * 100, 1),
                "avg_pnl": round(sub.mean(), 0),
                "total_pnl": round(sub.sum(), 0),
            })
        sheets["filter_pre_entry_move"] = pd.DataFrame(skip_rows)
    except Exception:
        pass

    # 8. KEY FINDING: "what if we skipped high-gap days?"
    try:
        col = pd.to_numeric(df["abs_gap_pct"], errors="coerce")
        outcome_col = pd.to_numeric(df[outcome], errors="coerce")
        thresholds = [0.2, 0.3, 0.5, 0.7, 1.0, 1.5]
        gap_rows = [{
            "filter": "NO FILTER (all trades)",
            "trades": len(df),
            "win_rate_pct": round((outcome_col > 0).mean() * 100, 1),
            "avg_pnl": round(outcome_col.mean(), 0),
            "total_pnl": round(outcome_col.sum(), 0),
        }]
        for t in thresholds:
            mask = col.notna() & (col <= t)
            if mask.sum() < 10:
                continue
            sub = outcome_col[mask]
            gap_rows.append({
                "filter": f"abs_gap <= {t}%",
                "trades": int(mask.sum()),
                "skipped": len(df) - int(mask.sum()),
                "win_rate_pct": round((sub > 0).mean() * 100, 1),
                "avg_pnl": round(sub.mean(), 0),
                "total_pnl": round(sub.sum(), 0),
            })
        sheets["filter_gap"] = pd.DataFrame(gap_rows)
    except Exception:
        pass

    # 9. KEY FINDING: "what if we only traded when premium is in a certain range?"
    try:
        col = pd.to_numeric(df["premium_pct_of_spot"], errors="coerce")
        outcome_col = pd.to_numeric(df[outcome], errors="coerce")
        valid_mask = col.notna() & outcome_col.notna()
        prem_rows = [{
            "filter": "NO FILTER (all trades)",
            "trades": int(valid_mask.sum()),
            "win_rate_pct": round((outcome_col[valid_mask] > 0).mean() * 100, 1),
            "avg_pnl": round(outcome_col[valid_mask].mean(), 0),
            "total_pnl": round(outcome_col[valid_mask].sum(), 0),
        }]
        # Premium floor: skip if premium too low (less theta to capture)
        for floor in [0.3, 0.5, 0.7, 1.0]:
            mask = valid_mask & (col >= floor)
            if mask.sum() >= 10:
                sub = outcome_col[mask]
                prem_rows.append({
                    "filter": f"premium >= {floor}% of spot",
                    "trades": int(mask.sum()),
                    "skipped": int(valid_mask.sum()) - int(mask.sum()),
                    "win_rate_pct": round((sub > 0).mean() * 100, 1),
                    "avg_pnl": round(sub.mean(), 0),
                    "total_pnl": round(sub.sum(), 0),
                })
        # Premium ceiling: skip if premium too high (market expecting big move)
        for ceil in [3.0, 2.0, 1.5, 1.0]:
            mask = valid_mask & (col <= ceil)
            if mask.sum() >= 10:
                sub = outcome_col[mask]
                prem_rows.append({
                    "filter": f"premium <= {ceil}% of spot",
                    "trades": int(mask.sum()),
                    "skipped": int(valid_mask.sum()) - int(mask.sum()),
                    "win_rate_pct": round((sub > 0).mean() * 100, 1),
                    "avg_pnl": round(sub.mean(), 0),
                    "total_pnl": round(sub.sum(), 0),
                })
        sheets["filter_premium"] = pd.DataFrame(prem_rows)
    except Exception:
        pass

    return sheets


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Short-straddle pattern discovery")
    parser.add_argument("--pickles-dir", default=None)
    parser.add_argument("--lookback-months", type=int, default=None)
    parser.add_argument("--entry-time", default=None, help="Override entry time (HH:MM)")
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    pickles_dir = args.pickles_dir or bt.PICKLES_DIR
    lookback = args.lookback_months or bt.LOOKBACK_MONTHS
    output_path = args.output or str(Path.home() / "Downloads" / "straddle_patterns.xlsx")

    if args.entry_time:
        bt.ENTRY_TIME_IST = args.entry_time
        bt.ENTRY_TIME = bt.parse_hhmm(args.entry_time)
    entry_time = bt.ENTRY_TIME

    print("=" * 70)
    print("  SHORT-STRADDLE PATTERN DISCOVERY")
    print("=" * 70)
    print(f"  Pickles:     {pickles_dir}")
    print(f"  Lookback:    {lookback} months")
    print(f"  Entry time:  {bt.ENTRY_TIME_IST}")
    print(f"  Output:      {output_path}")
    print()

    # --- Discover data range ---
    paths = sorted(glob.glob(os.path.join(pickles_dir, "*.pkl"))
                   + glob.glob(os.path.join(pickles_dir, "*.pickle")))
    if not paths:
        print(f"[ERROR] No pickles in: {pickles_dir}")
        sys.exit(1)

    end_day = bt.discover_data_max_day(paths) or date.today()
    window_start = bt.compute_window_start(end_day, lookback)
    print(f"  Data window: {window_start} -> {end_day}\n")

    # --- Load and normalize all pickles ---
    print("[STEP 1] Loading and normalizing pickles ...")
    t0 = time.time()

    all_groups = []
    for i, p in enumerate(paths):
        src = os.path.basename(p)
        try:
            raw = pd.read_pickle(p)
            if not isinstance(raw, pd.DataFrame) or raw.empty:
                continue
            d = bt._normalize_dhan_df(raw, src)
            if d.empty:
                continue
            d = d[(d["day"] >= window_start) & (d["day"] <= end_day)]
            if d.empty:
                continue
            min_expiry = d.groupby(["underlying", "day"], sort=False)["expiry"].min().to_dict()
            for (und, dy, ex), g in d.groupby(["underlying", "day", "expiry"], sort=False):
                if min_expiry.get((und, dy)) != ex:
                    continue
                all_groups.append({"und": und, "dy": dy, "expiry": ex,
                                   "day_opt": g.copy(), "source_pickle": src})
            if (i + 1) % 10 == 0 or (i + 1) == len(paths):
                print(f"  ... {i+1}/{len(paths)} pickles")
        except Exception as e:
            print(f"  [WARN] {src}: {e}")

    # Dedup
    all_groups.sort(key=lambda g: (g["und"], g["dy"], g["expiry"], g["source_pickle"]))
    seen = set()
    groups = []
    for g in all_groups:
        key = (g["und"], g["dy"], g["expiry"])
        if key not in seen:
            seen.add(key)
            groups.append(g)

    print(f"  Loaded {len(groups):,} groups in {time.time()-t0:.1f}s\n")

    if not groups:
        print("[ERROR] No data groups found.")
        sys.exit(1)

    # --- Build previous-close map for gap calculation ---
    print("[STEP 2] Building previous-close map ...")
    # For each (underlying, day), get the last available spot from raw data
    eod_spots = {}  # (und, day) -> last spot price
    for g in groups:
        try:
            spot_vals = g["day_opt"]["spot_f"].dropna()
            if len(spot_vals) > 0:
                # Sort by timestamp to get the latest spot
                ts_col = g["day_opt"].loc[spot_vals.index, "ts"]
                last_idx = ts_col.idxmax()
                eod_spots[(g["und"], g["dy"])] = float(g["day_opt"].loc[last_idx, "spot_f"])
        except Exception:
            pass

    # Build prev_close lookup: for each day, find the most recent prior day's EOD spot
    prev_close_map = {}  # (und, day) -> prev_day_eod_spot
    by_und = {}
    for (und, dy), spot in eod_spots.items():
        by_und.setdefault(und, []).append((dy, spot))
    for und, day_spots in by_und.items():
        day_spots.sort(key=lambda x: x[0])
        for i in range(1, len(day_spots)):
            prev_close_map[(und, day_spots[i][0])] = day_spots[i-1][1]

    print(f"  Built prev-close for {len(prev_close_map):,} (underlying, day) pairs\n")

    # --- Extract features + run backtest for each group ---
    print("[STEP 3] Extracting features and running backtests ...")
    t0 = time.time()
    rows = []
    skipped = 0

    for i, g in enumerate(groups):
        prev_close = prev_close_map.get((g["und"], g["dy"]), None)
        result = extract_features_and_outcome(
            und=g["und"], dy=g["dy"], expiry=g["expiry"],
            day_opt=g["day_opt"], source_pickle=g["source_pickle"],
            entry_time=entry_time,
            prev_close_spot=prev_close,
        )
        if result is not None:
            rows.append(result)
        else:
            skipped += 1

        if (i + 1) % 50 == 0 or (i + 1) == len(groups):
            print(f"  ... {i+1}/{len(groups)} groups  ({len(rows)} features extracted, {skipped} skipped)")

    print(f"  Done in {time.time()-t0:.1f}s\n")

    if not rows:
        print("[ERROR] No features extracted.")
        sys.exit(1)

    df = pd.DataFrame(rows)

    # --- Build actual_trades selection (one underlying per day) ---
    print(f"[STEP 4] Selecting actual trades (one underlying/day) ...")
    # Prefer NIFTY on tie, earliest expiry
    tmp = df.sort_values(["day", "expiry", "underlying"]).drop_duplicates(subset=["day"], keep="first")
    print(f"  {len(tmp)} unique trading days from {len(df)} total groups")
    df_actual = tmp.copy()

    # --- Run analysis ---
    print(f"\n[STEP 5] Running pattern analysis on {len(df_actual)} trading days ...\n")
    sheets = run_analysis(df_actual)

    # --- Write Excel ---
    print(f"[STEP 6] Writing Excel ...")
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    with pd.ExcelWriter(output_path, engine="openpyxl") as xw:
        # Write correlation sheet first (most important)
        for name in ["feature_correlations", "pre_entry_correlations",
                     "filter_pre_entry_move", "filter_gap", "filter_premium",
                     "cross_move_x_premium"]:
            if name in sheets:
                sheets[name].to_excel(xw, sheet_name=name[:31], index=False)

        # Categorical analyses
        for name, sdf in sheets.items():
            if name.startswith("cat_"):
                sdf.to_excel(xw, sheet_name=name[:31], index=False)

        # Bucket analyses
        for name, sdf in sheets.items():
            if name.startswith("bucket_"):
                sdf.to_excel(xw, sheet_name=name[:31], index=False)

        # Raw data last
        if "raw_data" in sheets:
            sheets["raw_data"].to_excel(xw, sheet_name="raw_data", index=False)

        for ws in xw.book.worksheets:
            ws.freeze_panes = "A2"

    print(f"\n[SAVED] {output_path}")

    # --- Console summary ---
    print("\n" + "=" * 70)
    print("  KEY FINDINGS")
    print("=" * 70)

    if "feature_correlations" in sheets:
        corr = sheets["feature_correlations"]
        print("\n  Top features correlated with daily P&L:")
        print("  (sorted by absolute Spearman rank-correlation)\n")
        for _, row in corr.head(8).iterrows():
            direction = "higher=WORSE" if row["spearman_corr"] < 0 else "higher=BETTER"
            print(f"    {row['feature']:35s}  r={row['spearman_corr']:+.3f}  "
                  f"top20%WR={row['top_20pct_win_rate']:5.1f}%  "
                  f"bot20%WR={row['bottom_20pct_win_rate']:5.1f}%  "
                  f"({direction})")

    if "pre_entry_correlations" in sheets:
        corr = sheets["pre_entry_correlations"]
        print("\n  Pre-entry features only (observable BEFORE you trade):")
        for _, row in corr.head(5).iterrows():
            direction = "higher=WORSE" if row["spearman_corr"] < 0 else "higher=BETTER"
            print(f"    {row['feature']:35s}  r={row['spearman_corr']:+.3f}  "
                  f"WR spread={row['win_rate_spread']:+.1f}%  ({direction})")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
