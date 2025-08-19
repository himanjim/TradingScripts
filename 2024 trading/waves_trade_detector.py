# -*- coding: utf-8 -*-
# Simple Equal-Leg (Symmetry) Scanner — 1-min, Intraday, NSE stocks
# Pure price action: detects AB≈CD waves (continuations & reversals)
# Prints entry trigger (B-break), stop (below/above C), and measured-move target.

import os, time, datetime as dt, pytz
import pandas as pd
import numpy as np
import OptionTradeUtils as oUtils

# ---------------- Config ----------------
IST = pytz.timezone('Asia/Kolkata')

WATCHLIST_FILE     = r"C:\Users\USER\Downloads\NSE derivatives list.xlsx"  # Excel/CSV with column 'SYMBOL'
DEFAULT_EX_PREFIX  = "NSE:"
MAX_SYMBOLS        = 100        # trim very large lists to avoid rate limits
SLEEP_BETWEEN_CALLS= 0.15       # seconds between historical calls (gentle on API)

# Data window (intraday)
LOOKBACK_MINS      = 45         # last X minutes (today or last session if closed)

# Swings & symmetry thresholds (tweak lightly)
SWING_K            = 3          # swing lookback/forward bars
MIN_LEG_PCT        = 0.10       # minimum % size of each leg to ignore noise (e.g., 0.10%)
PX_TOLERANCE       = 0.20       # legs size tolerance (20%)
TIME_TOLERANCE     = 0.50       # legs time tolerance (50%)

TOP_N              = 12         # show top N signals
# ---------------------------------------


# ---------- Kite init ----------
def init_kite():
    return oUtils.intialize_kite_api()


# ---------- Watchlist ----------
def build_watch_from_excel(path):
    ext = os.path.splitext(path)[1].lower()
    if ext in (".xlsx", ".xls"):
        dfw = pd.read_excel(path)
    elif ext == ".csv":
        dfw = pd.read_csv(path)
    else:
        raise ValueError("Use .xlsx/.xls/.csv")
    if "SYMBOL" not in dfw.columns:
        raise ValueError("File must have a 'SYMBOL' column")

    symbols = (
        dfw["SYMBOL"].astype(str).str.strip().str.upper()
        .replace({"": np.nan}).dropna().unique().tolist()
    )
    symbols = symbols[:MAX_SYMBOLS]
    return [DEFAULT_EX_PREFIX + s for s in symbols]


# ---------- Token resolve (batched) ----------
def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def resolve_tokens_batch(kite, instruments, batch=150):
    tokens = {}
    for part in chunks(instruments, batch):
        try:
            resp = kite.ltp(part)  # returns { 'NSE:RELIANCE': {..., 'instrument_token': int}, ...}
            for k, v in resp.items():
                tokens[k] = v["instrument_token"]
        except Exception as e:
            print(f"⚠️ ltp batch failed ({len(part)}): {e}")
        time.sleep(0.05)
    return tokens


# ---------- Historical fetch (simple, robust) ----------
def _session_bounds(date_obj, now_ist):
    ss = IST.localize(dt.datetime.combine(date_obj, dt.time(9, 15)))
    se = IST.localize(dt.datetime.combine(date_obj, dt.time(15, 30)))
    # If inside today's session, end at now; else end at session close
    end = now_ist if (date_obj == now_ist.date() and ss <= now_ist <= se) else se
    start = max(ss, end - dt.timedelta(minutes=LOOKBACK_MINS))
    if start >= end:
        start = ss
    return start, end, ss, se

def _prev_weekday(d):
    d -= dt.timedelta(days=1)
    while d.weekday() >= 5:  # 5=Sat,6=Sun
        d -= dt.timedelta(days=1)
    return d

def fetch_1m_df(kite, token, interval="minute"):
    """
    Try today's session (trim to last LOOKBACK_MINS). If empty/early, fall back
    to the previous weekday session and take its last LOOKBACK_MINS.
    """
    now = dt.datetime.now(IST)

    # 1) Try today
    start, end, ss, se = _session_bounds(now.date(), now)
    try:
        data = kite.historical_data(token, start, end, interval)
    except Exception as e:
        # If rate/too many requests, a tiny sleep and one more try
        time.sleep(0.4)
        try:
            data = kite.historical_data(token, start, end, interval)
        except Exception as e2:
            print(f"⚠️ historical (today) token={token}: {e2}")
            data = []

    df = pd.DataFrame(data)
    if not df.empty and "date" in df.columns:
        df.set_index("date", inplace=True)
        return df[["open","high","low","close","volume"]].copy()

    # 2) Fallback: last weekday session (simple; ignores exchange holidays)
    prev = _prev_weekday(now.date())
    start2, end2, ss2, se2 = _session_bounds(prev, now)
    try:
        data2 = kite.historical_data(token, ss2, se2, interval)  # full day, then trim
    except Exception as e:
        print(f"⚠️ historical (prev) token={token}: {e}")
        data2 = []
    df2 = pd.DataFrame(data2)
    if not df2.empty and "date" in df2.columns:
        df2.set_index("date", inplace=True)
        cutoff = se2 - dt.timedelta(minutes=LOOKBACK_MINS)
        df2 = df2[(df2.index >= cutoff) & (df2.index <= se2)]
        return df2[["open","high","low","close","volume"]].copy()

    return pd.DataFrame()


# ---------- Swings & Equal-legs ----------
def find_swings(df, k=SWING_K):
    """
    Swing-high: bar 'i' high is max over [i-k, i+k]
    Swing-low : bar 'i' low  is min over [i-k, i+k]
    """
    H, L = df["high"].values, df["low"].values
    idxs = df.index.to_list()
    swings = []
    n = len(df)
    for i in range(k, n-k):
        is_high = H[i] == H[i-k:i+k+1].max()
        is_low  = L[i] == L[i-k:i+k+1].min()
        if is_high and not is_low:
            swings.append((idxs[i], float(H[i]), 'H'))
        elif is_low and not is_high:
            swings.append((idxs[i], float(L[i]), 'L'))
    return swings

def equal_leg_patterns(df,
                       k=SWING_K,
                       min_leg_pct=MIN_LEG_PCT,
                       px_tolerance=PX_TOLERANCE,
                       time_tolerance=TIME_TOLERANCE):
    """
    Finds AB≈CD with simple structure checks.
    Returns list of dicts with type, A/B/C/D, entry trigger, SL, TP, and a small 'score'.
    """
    out = []
    swings = find_swings(df, k=k)
    if len(swings) < 4:
        return out

    def pct(a, b):  # % change a->b
        return (b - a) / max(abs(a), 1e-9) * 100.0

    for i in range(len(swings) - 3):
        A, B, C, D = swings[i], swings[i+1], swings[i+2], swings[i+3]
        seq = ''.join([A[2], B[2], C[2], D[2]])
        tA,pA = A[0], A[1]
        tB,pB = B[0], B[1]
        tC,pC = C[0], C[1]
        tD,pD = D[0], D[1]

        dt1 = (tB - tA).total_seconds()/60.0
        dt2 = (tD - tC).total_seconds()/60.0
        if dt1 <= 0 or dt2 <= 0:
            continue

        leg1 = pct(pA, pB)
        leg2 = pct(pC, pD)

        # CONTINUATION LONG: L-H-L-H with both legs up, HH/HL structure
        if seq == "LHLH" and leg1 > 0 and leg2 > 0 and abs(leg1) >= min_leg_pct and abs(leg2) >= min_leg_pct:
            size_ok = abs(abs(leg2) - abs(leg1)) <= px_tolerance * abs(leg1)
            time_ok = abs(dt2 - dt1) <= time_tolerance * dt1
            if size_ok and time_ok and pC > pA and pD > pB:
                score = 100*(1 - min(1, abs(abs(leg2)-abs(leg1))/max(abs(leg1),1e-9)))*0.7 \
                        + 100*(1 - min(1, abs(dt2-dt1)/max(dt1,1e-9)))*0.3
                out.append({
                    "type": "CONT_LONG", "score": round(score,1),
                    "A": (tA,pA), "B": (tB,pB), "C": (tC,pC), "D": (tD,pD),
                    "entry": ("break_above", pB), "sl": ("below", pC),
                    "tp": ("measured", pC + (pB - pA))
                })

        # CONTINUATION SHORT: H-L-H-L with both legs down, LL/LH structure
        if seq == "HLHL" and leg1 < 0 and leg2 < 0 and abs(leg1) >= min_leg_pct and abs(leg2) >= min_leg_pct:
            size_ok = abs(abs(leg2) - abs(leg1)) <= px_tolerance * abs(leg1)
            time_ok = abs(dt2 - dt1) <= time_tolerance * dt1
            if size_ok and time_ok and pC < pA and pD < pB:
                score = 100*(1 - min(1, abs(abs(leg2)-abs(leg1))/max(abs(leg1),1e-9)))*0.7 \
                        + 100*(1 - min(1, abs(dt2-dt1)/max(dt1,1e-9)))*0.3
                out.append({
                    "type": "CONT_SHORT", "score": round(score,1),
                    "A": (tA,pA), "B": (tB,pB), "C": (tC,pC), "D": (tD,pD),
                    "entry": ("break_below", pB), "sl": ("above", pC),
                    "tp": ("measured", pC - (pA - pB))
                })

        # Optional: treat AB=CD against prior move as reversal; keep code minimal and skip here.

    return out


# ---------- Scanner ----------
def scan_equal_legs(kite, instruments):
    token_map = resolve_tokens_batch(kite, instruments)
    if not token_map:
        print("⚠️ No tokens resolved.")
        return pd.DataFrame()

    rows = []
    for instr, token in token_map.items():
        df = fetch_1m_df(kite, token, interval="minute")
        # polite pacing to avoid rate limits
        time.sleep(SLEEP_BETWEEN_CALLS)

        if df is None or df.empty or len(df) < 20:
            continue

        sigs = equal_leg_patterns(df,
                                  k=SWING_K,
                                  min_leg_pct=MIN_LEG_PCT,
                                  px_tolerance=PX_TOLERANCE,
                                  time_tolerance=TIME_TOLERANCE)
        if not sigs:
            continue

        # pick the latest (most recent) signal
        best = sigs[-1]
        tD = best["D"][0]
        mins_ago = (df.index[-1] - tD).total_seconds()/60.0
        rec_bonus = max(0.0, 1.0 - mins_ago/LOOKBACK_MINS)  # fresher gets a tiny boost

        rows.append({
            "instrument": instr,
            "type": best["type"],
            "score": round(best["score"] + 5*rec_bonus, 1),
            "A_t": best["A"][0], "A_p": round(best["A"][1],2),
            "B_t": best["B"][0], "B_p": round(best["B"][1],2),
            "C_t": best["C"][0], "C_p": round(best["C"][1],2),
            "D_t": best["D"][0], "D_p": round(best["D"][1],2),
            "entry": f"{best['entry'][0]} {round(best['entry'][1],2)}",
            "sl":    f"{best['sl'][0]} {round(best['sl'][1],2)}",
            "tp":    f"{best['tp'][0]} {round(best['tp'][1],2)}",
            "age_min": round(mins_ago, 1)
        })

    if not rows:
        return pd.DataFrame()

    out = (pd.DataFrame(rows)
           .sort_values(by=["score","age_min"], ascending=[False, True])
           .reset_index(drop=True))
    return out


# ---------- Main ----------
if __name__ == "__main__":
    kite = init_kite()
    watch = build_watch_from_excel(WATCHLIST_FILE)  # ['NSE:RELIANCE', ...]
    print(f"Scanning {len(watch)} symbols (1-min, last {LOOKBACK_MINS}m) for clean equal-legs...")

    ranked = scan_equal_legs(kite, watch)

    if ranked.empty:
        print("No clean equal-leg patterns found.")
    else:
        print(ranked.head(TOP_N).to_string(index=False))
