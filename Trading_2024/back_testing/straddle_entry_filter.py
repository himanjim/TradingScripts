"""
straddle_entry_filter.py
========================

Drop-in entry-gate layer for `atm_straddle_prem_jump_reattempt.py`.

WHAT IT ADDS
------------
An entry gate that fires a short straddle on a given day ONLY when BOTH:
  (1) PREMIUM SWELLED  - today's entry-minute straddle premium (as % of spot) is
      rich relative to that underlying's own trailing baseline, AND
  (2) TREND COOLED     - today's morning realized move (09:15 -> entry, as % of
      spot) is below that underlying's own trailing baseline (i.e. quiet).

NO LOOK-AHEAD
-------------
Both baselines use ONLY prior trading days. For day D we compute the median over
the previous N completed days (D excluded). Day D's own features are never part
of its own baseline. The first N days of data therefore never trade (no baseline
yet) - that is correct, not a bug.

HOW TO USE
----------
Place this file next to the original. Then run:

    python straddle_entry_filter.py

It imports the original module, builds the per-(underlying,day) feature table by
re-reading the pickles once, sweeps the baseline windows you list in
BASELINE_WINDOWS, and writes one Excel per window with two extra sheets:
  - entry_features : every candidate day, its features, baseline, and gate result
  - gate_summary   : traded vs skipped counts and PnL, per underlying and month

Tune the knobs in the CONFIG block below.
"""

from __future__ import annotations

import os
from datetime import date, datetime, time as dtime
from typing import Dict, List, Tuple, Any, Optional

import pandas as pd

# Import the original backtester as a module. We reuse its helpers and override
# two functions. Nothing in the original file is modified.
import atm_straddle_prem_jump_reattempt as base


# =============================================================================
# FILTER CONFIG  (the only things you normally touch)
# =============================================================================
# Baseline windows (in trading days) to sweep. One output file per value.
BASELINE_WINDOWS: List[int] = [10, 20, 30]

# A day is "premium swelled" if entry straddle premium (% of spot) is at least
# this multiple of its trailing-median baseline. 1.0 = at or above median.
PREMIUM_RICH_MULT: float = 1.10        # >= 110% of trailing median premium%

# A day is "trend cooled" only if its settle_ratio is at most this fraction of
# its trailing-median settle_ratio. <1.0 demands the recent window be
# MEANINGFULLY calmer than a typical day's recent window, not merely at par,
# so a day that is still chopping just as hard at entry is rejected.
TREND_QUIET_MULT: float = 0.85         # settle_ratio <= 0.85 * trailing median

# Minimum number of prior days required before the gate is allowed to act.
# Defaults to the baseline window itself if left as None.
MIN_HISTORY_DAYS: Optional[int] = None

# Where to write outputs (one file per baseline window).
OUT_DIR = base._get_downloads_folder()


# =============================================================================
# FEATURE PASS  (chronological, no look-ahead)
# =============================================================================
def _entry_ts_for(dy: date) -> pd.Timestamp:
    return pd.Timestamp(datetime.combine(dy, base.ENTRY_TIME), tz=base.ist_tz())


# How many minutes before entry count as "recent" (the settling window).
SETTLE_WINDOW_MIN: int = 15

# Minimum bars required in the EARLY window (09:15 -> entry-SETTLE) for the
# settle ratio to be defined. Below this the ratio is NaN, not a fake 1.0.
# With a 15-min settle window this effectively requires entry >= ~09:45.
MIN_PRE_SETTLE_BARS: int = 10

# A morning must have at least this much total path (% of spot) to be considered
# "it moved" at all. Dead-flat mornings are not 'cooled', they're just dead, and
# there's no swollen premium to sell. Below this floor -> not a valid cooled day.
MORNING_PATH_FLOOR_PCT: float = 0.15


def _path_len_pct(d: pd.DataFrame, spot_at_entry: float) -> float:
    """Sum of absolute minute-to-minute close moves over d, as % of spot."""
    if len(d) < 2 or not (spot_at_entry and spot_at_entry == spot_at_entry) or spot_at_entry <= 0:
        return float("nan")
    return float(d["close"].diff().abs().sum()) / spot_at_entry * 100.0


def _settle_features(underlying_day: pd.DataFrame, entry_ts: pd.Timestamp,
                     spot_at_entry: float) -> Tuple[float, float, float]:
    """
    Captures 'it moved AND is now settling' via realized PATH LENGTH (sum of
    |per-minute moves|), comparing two NON-OVERLAPPING windows:

      early window  : 09:15  -> (entry - SETTLE_WINDOW_MIN)   ('it moved' earlier)
      recent window : (entry - SETTLE_WINDOW_MIN) -> entry    ('is it calm now')

    Returns (settle_ratio, morning_path_pct, recent_path_pct):
      morning_path_pct : path length of the EARLY window, % of spot
      recent_path_pct  : path length of the RECENT window, % of spot
      settle_ratio     : recent_path_pct / early_path_pct     (low = cooled)

    Non-overlapping is deliberate: if 'recent' were a subset of 'morning' (as in
    a naive full-morning vs last-15 split), the recent travel would sit in both
    numerator and denominator and force the ratio toward 1.0 — which is exactly
    what kills the signal at early entry times. Here early and recent are
    disjoint, so the ratio is a clean before/after contrast.

    Requires at least MIN_PRE_SETTLE_BARS minutes in the early window; otherwise
    returns NaN ratio (the feature is undefined, not silently 1.0). This means a
    09:15+SETTLE entry or earlier yields no settle signal by construction — move
    entry later (>= ~09:45 for a 15-min settle window) for a usable ratio.

    All bars are at/under entry_ts -> no forward peeking.
    """
    if underlying_day.empty or not (spot_at_entry and spot_at_entry == spot_at_entry):
        return float("nan"), float("nan"), float("nan")
    d = underlying_day[["date", "close"]].dropna().copy()
    d["date"] = base.ensure_ist(d["date"])

    # Align entry/open to the SAME tz as the data so masks never go all-False.
    data_tz = d["date"].dt.tz
    e_ts = pd.Timestamp(entry_ts)
    if e_ts.tzinfo is None:
        e_ts = e_ts.tz_localize(data_tz) if data_tz is not None else e_ts
    else:
        e_ts = e_ts.tz_convert(data_tz) if data_tz is not None else e_ts.tz_localize(None)

    open_ts = pd.Timestamp(datetime.combine(e_ts.date(), base.SESSION_START_IST))
    if data_tz is not None:
        open_ts = open_ts.tz_localize(data_tz)

    split_ts = e_ts - pd.Timedelta(minutes=SETTLE_WINDOW_MIN)

    early = d[(d["date"] >= open_ts) & (d["date"] <= split_ts)].sort_values("date")
    recent = d[(d["date"] > split_ts) & (d["date"] <= e_ts)].sort_values("date")

    # Need enough bars in BOTH windows or the ratio is meaningless.
    if len(early) < MIN_PRE_SETTLE_BARS or len(recent) < 2:
        early_path = _path_len_pct(early, spot_at_entry) if len(early) >= 2 else float("nan")
        return float("nan"), early_path, float("nan")

    early_path = _path_len_pct(early, spot_at_entry)
    recent_path = _path_len_pct(recent, spot_at_entry)

    if pd.isna(early_path) or early_path <= 0 or pd.isna(recent_path):
        return float("nan"), early_path, recent_path
    return recent_path / early_path, early_path, recent_path


def _entry_straddle_pct(day_opt: pd.DataFrame, underlying_day: pd.DataFrame,
                        und: str, dy: date) -> Tuple[float, float, float, int]:
    """
    Returns (straddle_pct_of_spot, ce_entry, pe_entry, atm_strike) priced at the
    exact entry minute the same way the simulator does. NaNs if unavailable.
    """
    entry_ts = _entry_ts_for(dy)
    spot = base.asof_close(underlying_day, entry_ts)
    if pd.isna(spot):
        return float("nan"), float("nan"), float("nan"), -1

    step = int(base.STRIKE_STEP[und])
    atm = base.round_to_step(float(spot), step)

    idx_all = base.build_minute_index(dy, base.SESSION_START_IST, base.SESSION_END_IST)
    ce_sym = base._pick_symbol(day_opt, atm, "CE")
    pe_sym = base._pick_symbol(day_opt, atm, "PE")
    if not ce_sym or not pe_sym or entry_ts not in idx_all:
        return float("nan"), float("nan"), float("nan"), atm

    ce_raw = base._build_leg_series(day_opt, idx_all, atm, "CE", ce_sym, "close", do_ffill=False)
    pe_raw = base._build_leg_series(day_opt, idx_all, atm, "PE", pe_sym, "close", do_ffill=False)
    ce_e = ce_raw.loc[entry_ts]
    pe_e = pe_raw.loc[entry_ts]
    if pd.isna(ce_e) or pd.isna(pe_e) or spot <= 0:
        return float("nan"), float(ce_e) if pd.notna(ce_e) else float("nan"), \
               float(pe_e) if pd.notna(pe_e) else float("nan"), atm

    straddle_pct = (float(ce_e) + float(pe_e)) / spot * 100.0
    return straddle_pct, float(ce_e), float(pe_e), atm


def build_feature_table(
    pickle_paths: List[str],
    min_expiry_map: Dict[Tuple[str, date], date],
    underlying_data: Dict[str, pd.DataFrame],
    window_start: date,
    window_end: date,
) -> pd.DataFrame:
    """
    One row per (underlying, day) that is eligible to trade (nearest expiry on
    that day). Columns: raw features only. Baselines/flags are added later,
    per baseline-window, in attach_baseline_flags().
    """
    rows: List[Dict[str, Any]] = []
    processed: set[Tuple[str, date, date]] = set()

    for p in pickle_paths:
        try:
            df = pd.read_pickle(p)
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue
            needed = ["date", "name", "type", "option_type", "strike", "expiry",
                      "instrument", "high", "low", "close"]
            if any(c not in df.columns for c in needed):
                continue

            d2 = df[df["type"].astype(str).str.upper().eq("OPTION")][needed].copy()
            if d2.empty:
                continue
            d2["date"] = base.ensure_ist(d2["date"])
            d2["day"] = d2["date"].dt.date
            d2["underlying"] = d2["name"].astype(str).map(base.normalize_underlying)
            d2 = d2[d2["underlying"].isin(base.TRADEABLE)]
            if d2.empty:
                continue
            d2["expiry_date"] = pd.to_datetime(d2["expiry"], errors="coerce").dt.date
            d2["strike_num"] = pd.to_numeric(d2["strike"], errors="coerce")
            d2["strike_int"] = d2["strike_num"].round().astype("Int64")
            d2["option_type"] = d2["option_type"].astype(str).str.upper()
            d2 = d2.dropna(subset=["day", "underlying", "expiry_date", "strike_int", "close"])
            d2["strike_int"] = d2["strike_int"].astype(int)
            d2 = d2[d2["expiry_date"] >= d2["day"]]
            d2 = d2[(d2["day"] >= window_start) & (d2["day"] <= window_end)]
            if d2.empty:
                continue

            for (und, dy, ex), g in d2.groupby(["underlying", "day", "expiry_date"], sort=False):
                if min_expiry_map.get((und, dy)) != ex:
                    continue
                key = (und, dy, ex)
                if key in processed:
                    continue
                processed.add(key)

                uday = underlying_data.get(und)
                if uday is None:
                    continue
                uday = uday[uday["day"] == dy]
                if uday.empty:
                    continue

                straddle_pct, ce_e, pe_e, atm = _entry_straddle_pct(g, uday, und, dy)
                entry_ts = _entry_ts_for(dy)
                spot = base.asof_close(uday, entry_ts)
                spot_f = float(spot) if pd.notna(spot) else float("nan")
                settle_ratio, morning_path, recent_path = _settle_features(uday, entry_ts, spot_f)

                rows.append({
                    "underlying": und,
                    "day": dy,
                    "expiry": ex,
                    "atm_strike": atm,
                    "entry_spot": spot_f,
                    "entry_ce": ce_e,
                    "entry_pe": pe_e,
                    "straddle_pct_of_spot": straddle_pct,
                    "morning_path_pct": morning_path,
                    "recent_path_pct": recent_path,
                    "settle_ratio": settle_ratio,
                })
        except Exception as e:
            print(f"[FEATURE WARN] {os.path.basename(p)}: {e}")

    feat = pd.DataFrame(rows)
    if not feat.empty:
        feat = feat.sort_values(["underlying", "day"]).reset_index(drop=True)
    return feat


def attach_baseline_flags(feat: pd.DataFrame, baseline_window: int) -> pd.DataFrame:
    """
    Adds trailing-median baselines (prior N days, current day EXCLUDED via shift),
    the two boolean flags, and the combined gate. Per underlying, chronological.
    """
    if feat.empty:
        return feat.copy()

    min_hist = MIN_HISTORY_DAYS if MIN_HISTORY_DAYS is not None else baseline_window
    out = feat.sort_values(["underlying", "day"]).copy()

    def _per_und(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values("day").copy()
        # shift(1) so the rolling window ends on the PRIOR day -> today excluded.
        prem = g["straddle_pct_of_spot"]
        ratio = g["settle_ratio"]
        g["prem_baseline"] = prem.shift(1).rolling(baseline_window, min_periods=min_hist).median()
        g["settle_baseline"] = ratio.shift(1).rolling(baseline_window, min_periods=min_hist).median()

        # PREMIUM SWELLED: today's straddle% is rich vs trailing median.
        g["premium_swelled"] = (g["straddle_pct_of_spot"] >= PREMIUM_RICH_MULT * g["prem_baseline"])

        # TREND COOLED: it MOVED (morning path above floor) AND it's now SETTLING
        # (settle_ratio at/below its trailing baseline * mult -> recent calm
        #  relative to the morning, and calmer than a typical day).
        moved = g["morning_path_pct"] >= MORNING_PATH_FLOOR_PCT
        settling = g["settle_ratio"] <= TREND_QUIET_MULT * g["settle_baseline"]
        g["trend_cooled"] = moved & settling

        have_baseline = g["prem_baseline"].notna() & g["settle_baseline"].notna()
        feats_ok = (g["straddle_pct_of_spot"].notna() & g["settle_ratio"].notna()
                    & g["morning_path_pct"].notna())
        g["gate_enter"] = (have_baseline & feats_ok &
                           g["premium_swelled"].fillna(False) &
                           g["trend_cooled"].fillna(False))
        g["gate_reason"] = "ENTER"
        g.loc[~have_baseline, "gate_reason"] = "NO_BASELINE_YET"
        g.loc[have_baseline & ~feats_ok, "gate_reason"] = "MISSING_FEATURE"
        g.loc[have_baseline & feats_ok & ~g["premium_swelled"].fillna(False),
              "gate_reason"] = "PREMIUM_NOT_RICH"
        # Distinguish the two ways "cooled" can fail, for the audit.
        not_cooled = have_baseline & feats_ok & g["premium_swelled"].fillna(False) & ~g["trend_cooled"].fillna(False)
        g.loc[not_cooled & ~moved, "gate_reason"] = "MORNING_TOO_FLAT"
        g.loc[not_cooled & moved & ~settling.fillna(False), "gate_reason"] = "NOT_SETTLED"
        return g

    parts = [_per_und(g) for _, g in out.groupby("underlying", sort=False)]
    return pd.concat(parts, ignore_index=True) if parts else out


# =============================================================================
# GATE LOOKUP  +  PATCHED SIMULATOR
# =============================================================================
# Populated per baseline-window run, before the simulator runs.
_GATE: Dict[Tuple[str, date], Dict[str, Any]] = {}


def diagnose_features(feat: pd.DataFrame, flagged: pd.DataFrame, N: int) -> None:
    """Print where days drop out, so a 0-pass gate can be debugged at a glance."""
    print(f"\n--- DIAGNOSTIC (N={N}) ---")
    if feat.empty:
        print("  feature table EMPTY")
        return
    tot = len(feat)
    prem_ok = int(feat["straddle_pct_of_spot"].notna().sum())
    mp_ok = int(feat["morning_path_pct"].notna().sum())
    rp_ok = int(feat["recent_path_pct"].notna().sum())
    sr_ok = int(feat["settle_ratio"].notna().sum())
    print(f"  rows={tot} | premium%%_valid={prem_ok} morning_path_valid={mp_ok} "
          f"recent_path_valid={rp_ok} settle_ratio_valid={sr_ok}")
    # show a couple of sample rows so bad values are visible
    cols = ["underlying", "day", "entry_spot", "entry_ce", "entry_pe",
            "straddle_pct_of_spot", "morning_path_pct", "recent_path_pct", "settle_ratio"]
    cols = [c for c in cols if c in feat.columns]
    print("  sample feature rows:")
    print(feat[cols].head(3).to_string(index=False))
    if not flagged.empty:
        print("  gate_reason counts:")
        print(flagged["gate_reason"].value_counts().to_string())
        hb = int((flagged["prem_baseline"].notna() & flagged["settle_baseline"].notna()).sum())
        print(f"  days_with_baseline={hb}")
        prem_rich = int(flagged["premium_swelled"].fillna(False).sum())
        cooled = int(flagged["trend_cooled"].fillna(False).sum())
        print(f"  premium_swelled_days={prem_rich} trend_cooled_days={cooled}")
    print("--- END DIAGNOSTIC ---\n")


def _set_gate(flagged: pd.DataFrame) -> None:
    _GATE.clear()
    for _, r in flagged.iterrows():
        _GATE[(r["underlying"], r["day"])] = {
            "enter": bool(r["gate_enter"]),
            "reason": str(r["gate_reason"]),
            "straddle_pct": r["straddle_pct_of_spot"],
            "prem_baseline": r["prem_baseline"],
            "settle_ratio": r["settle_ratio"],
            "settle_baseline": r["settle_baseline"],
            "morning_path_pct": r["morning_path_pct"],
        }


# Keep a handle to the original so we can delegate when the gate says ENTER.
_orig_simulate = base.simulate_day_multi_trades


def patched_simulate_day_multi_trades(*, und, dy, expiry, day_opt, underlying_day):
    """
    Wraps the original simulator. If the gate rejects this (underlying, day),
    return zero trades and a single skip row explaining why. Otherwise delegate
    to the unmodified original logic, then stamp the gate features onto each
    resulting trade row for auditing.
    """
    info = _GATE.get((und, dy))
    if info is None or not info["enter"]:
        reason = info["reason"] if info else "NOT_IN_FEATURE_TABLE"
        return [], [{
            "day": dy, "underlying": und, "expiry": expiry,
            "reason": f"GATE_SKIP:{reason}",
            "straddle_pct_of_spot": None if info is None else info["straddle_pct"],
            "settle_ratio": None if info is None else info["settle_ratio"],
        }]

    trades, skips = _orig_simulate(
        und=und, dy=dy, expiry=expiry, day_opt=day_opt, underlying_day=underlying_day
    )
    # Stamp features onto each trade (as plain dict augmentation later).
    for t in trades:
        # TradeRow is a dataclass; attach via __dict__ post-hoc.
        t.__dict__["straddle_pct_of_spot"] = info["straddle_pct"]
        t.__dict__["prem_baseline"] = info["prem_baseline"]
        t.__dict__["settle_ratio"] = info["settle_ratio"]
        t.__dict__["settle_baseline"] = info["settle_baseline"]
        t.__dict__["morning_path_pct"] = info["morning_path_pct"]
    return trades, skips


# =============================================================================
# SUMMARY SHEET
# =============================================================================
def build_gate_summary(flagged: pd.DataFrame, actual_trades_df: pd.DataFrame) -> pd.DataFrame:
    if flagged.empty:
        return pd.DataFrame()
    g = flagged.copy()
    g["month"] = pd.to_datetime(g["day"]).dt.to_period("M").astype(str)
    eligible = g.groupby(["underlying", "month"], as_index=False).agg(
        eligible_days=("day", "nunique"),
        gate_entered_days=("gate_enter", "sum"),
    )
    if not actual_trades_df.empty:
        a = actual_trades_df.copy()
        a["month"] = pd.to_datetime(a["day"]).dt.to_period("M").astype(str)
        pnl = a.groupby(["underlying", "month"], as_index=False).agg(
            trades=("exit_pnl", "count"),
            total_exit_pnl=("exit_pnl", "sum"),
            win_rate_pct=("exit_pnl", lambda s: round(100.0 * (s > 0).mean(), 2)),
        )
        eligible = eligible.merge(pnl, on=["underlying", "month"], how="left")
    return eligible


# =============================================================================
# WRITE  (reuse original writer, then append our two sheets)
# =============================================================================
def write_with_filter_sheets(all_df, actual_df, skipped_df, flagged, out_path):
    prev = base.OUTPUT_XLSX
    base.OUTPUT_XLSX = out_path
    try:
        base.write_excel(all_df, actual_df, skipped_df)
    finally:
        base.OUTPUT_XLSX = prev

    gate_summary = build_gate_summary(flagged, actual_df)
    with pd.ExcelWriter(out_path, engine="openpyxl", mode="a",
                        if_sheet_exists="replace") as xw:
        flagged.to_excel(xw, sheet_name="entry_features", index=False)
        gate_summary.to_excel(xw, sheet_name="gate_summary", index=False)
    print(f"[DONE+FILTER] {out_path}")


# =============================================================================
# MAIN  (sweep baseline windows)
# =============================================================================
def main():
    paths = sorted(
        __import__("glob").glob(os.path.join(base.PICKLES_DIR, "*.pkl")) +
        __import__("glob").glob(os.path.join(base.PICKLES_DIR, "*.pickle"))
    )
    if not paths:
        raise FileNotFoundError(f"No .pkl/.pickle files in: {base.PICKLES_DIR}")

    end_day, min_expiry_map, min_day_seen = base.scan_pickles_pass1(paths)
    window_start = base.compute_window_start(end_day, base.LOOKBACK_MONTHS)
    print(f"[INFO] Window {window_start} -> {end_day}")

    # Guard: settle ratio needs enough early-window bars before the settle window.
    entry_minutes = (datetime.combine(date.today(), base.ENTRY_TIME)
                     - datetime.combine(date.today(), base.SESSION_START_IST)).seconds // 60
    early_bars_avail = entry_minutes - SETTLE_WINDOW_MIN
    if early_bars_avail < MIN_PRE_SETTLE_BARS:
        print(f"[WARN] Entry {base.ENTRY_TIME_IST} leaves only ~{early_bars_avail} early-window "
              f"minutes before the {SETTLE_WINDOW_MIN}-min settle window "
              f"(need >= {MIN_PRE_SETTLE_BARS}). The settle ratio will be NaN and "
              f"trend_cooled will never fire. Set ENTRY_TIME_IST later "
              f"(>= ~09:{15 + SETTLE_WINDOW_MIN + MIN_PRE_SETTLE_BARS - 60 if (15+SETTLE_WINDOW_MIN+MIN_PRE_SETTLE_BARS)>=60 else 15+SETTLE_WINDOW_MIN+MIN_PRE_SETTLE_BARS:02d}) "
              f"or reduce SETTLE_WINDOW_MIN / MIN_PRE_SETTLE_BARS.")

    kite = base.oUtils.intialize_kite_api()
    underlying_data = base.download_underlyings(kite, window_start, end_day)

    # Build raw features ONCE; baselines differ per window.
    feat = build_feature_table(paths, min_expiry_map, underlying_data,
                               window_start, end_day)
    if feat.empty:
        print("[WARN] No features built; nothing to gate.")
        return
    print(f"[INFO] Feature rows: {len(feat)} "
          f"({feat['underlying'].nunique()} underlyings, {feat['day'].nunique()} days)")

    # Patch the simulator for the whole sweep.
    base.simulate_day_multi_trades = patched_simulate_day_multi_trades

    for N in BASELINE_WINDOWS:
        print(f"\n========== BASELINE WINDOW N={N} ==========")
        flagged = attach_baseline_flags(feat, N)
        diagnose_features(feat, flagged, N)
        entered = int(flagged["gate_enter"].sum())
        print(f"[GATE] N={N}: {entered}/{len(flagged)} eligible days pass the gate")
        _set_gate(flagged)

        all_df, skipped_df = base.process_pickles_generate_trades(
            paths, min_expiry_map, underlying_data, window_start, end_day
        )
        actual_df = base.build_actual_trades_df(all_df, min_expiry_map)

        out_path = os.path.join(
            OUT_DIR,
            f"straddle_FILTERED_rich{PREMIUM_RICH_MULT:.2f}"
            f"_quiet{TREND_QUIET_MULT:.2f}_N{N}"
            f"_entry{base._safe_fname_part(base.ENTRY_TIME_IST)}.xlsx"
        )
        write_with_filter_sheets(all_df, actual_df, skipped_df, flagged, out_path)

        if not actual_df.empty:
            tot = actual_df["exit_pnl"].sum()
            wr = 100.0 * (actual_df["exit_pnl"] > 0).mean()
            print(f"[RESULT] N={N}: trades={len(actual_df)} "
                  f"net_pnl={tot:,.0f} win_rate={wr:.1f}%")
        else:
            print(f"[RESULT] N={N}: gate produced no trades.")


if __name__ == "__main__":
    main()
