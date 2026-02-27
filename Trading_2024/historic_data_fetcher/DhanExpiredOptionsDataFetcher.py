import os
import csv
import time
import pickle
import requests
import pandas as pd
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

# =============================================================================
# CONFIG
# =============================================================================
# Where pickle files will be written
OUT_DIR = os.getenv(
    "DHAN_EXPIRED_OUTDIR",
    r"G:\My Drive\Trading\Dhan_Historical_Options_Data"
)

# How many expiries per output pickle file.
# Each expiry contributes D-1 and D0 data. So 2 expiries => up to 4 trading days worth.
BATCH_EXPIRIES = int(os.getenv("DHAN_BATCH_EXPIRIES", "2"))

# How far back to generate scheduled expiries
LOOKBACK_DAYS = int(os.getenv("DHAN_LOOKBACK_DAYS", str(365 * 2)))  # default 2 years

# If the scheduled expiry is a holiday, try shifting back up to this many days
MAX_SHIFT_BACK_DAYS = int(os.getenv("DHAN_MAX_SHIFT_BACK_DAYS", "7"))

# Throttle between API calls (Dhan will rate-limit if you go too fast)
SLEEP_BETWEEN_CALLS = float(os.getenv("DHAN_SLEEP_BETWEEN_CALLS", "0.15"))

# Used only for conversion of UNIX timestamps to readable IST datetimes
TIMEZONE_IST = "Asia/Kolkata"

# Process and write files newest → oldest
PROCESS_NEWEST_FIRST = os.getenv("DHAN_NEWEST_FIRST", "1").strip() not in ("0", "false", "False")

# -----------------------------------------------------------------------------
# Dhan endpoints
# -----------------------------------------------------------------------------
ROLLING_URL = "https://api.dhan.co/v2/charts/rollingoption"
IDX_I_URL = "https://api.dhan.co/v2/instrument/IDX_I"

# -----------------------------------------------------------------------------
# Credentials
# -----------------------------------------------------------------------------
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzcyMjUwNzE2LCJpYXQiOjE3NzIxNjQzMTYsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA4NTg4OTMyIn0.KmDhXJiK3lemNmsy_PFv9mxGpmVDW9csAnb_hLO07313dW6F7Mx2nmwH_W51o7-XgtlKfLGurW-_8Mgj-erDbQ").strip()
if not ACCESS_TOKEN:
    raise SystemExit("Missing DHAN_ACCESS_TOKEN env var.")

CLIENT_ID = os.getenv("DHAN_CLIENT_ID", "1108588932").strip()
if not CLIENT_ID:
    raise SystemExit("Missing DHAN_CLIENT_ID env var.")
# -----------------------------------------------------------------------------
# Strike selection:
# RollingOption supports strike selectors like: ATM, ATM+1 ... ATM+10, ATM-1 ... ATM-10
# For index options, the usual max band is 10. We implement "all available (via this API)"d
# by fetching ATM plus/minus STRIKE_BAND.
# -----------------------------------------------------------------------------
STRIKE_BAND = int(os.getenv("DHAN_STRIKE_BAND", "10"))  # default: ATM±10 for indices

def _build_strike_selectors(band: int) -> List[str]:
    """
    Build selectors in an intuitive order:
      ATM, ATM+1, ATM-1, ATM+2, ATM-2, ...
    """
    out = ["ATM"]
    for k in range(1, band + 1):
        out.append(f"ATM+{k}")
        out.append(f"ATM-{k}")
    return out

STRIKE_SELECTORS = _build_strike_selectors(STRIKE_BAND)

# Data fields supported by RollingOption; "spot" is important for underlying tracking
REQUIRED_DATA = ["open", "high", "low", "close", "iv", "volume", "strike", "oi", "spot"]

# ExpiryCode enum: 0=Near, 1=Next, 2=Far (as per Dhan annexure)
EXPIRY_CODE = int(os.getenv("DHAN_EXPIRY_CODE", "1"))

# -----------------------------------------------------------------------------
# Weekly expiry weekday rules (weekday: Mon=0 ... Sun=6), keyed by week-start (Monday).
# CUTOVER captures the regime change date you used:
#   NIFTY weekly: Thu -> Tue after CUTOVER
#   SENSEX weekly: Tue -> Thu after CUTOVER
# -----------------------------------------------------------------------------
CUTOVER = date(2025, 9, 1)

WEEKDAY_RULES = {
    "NIFTY": {
        "exchangeSegment": "NSE_FNO",
        "underlyingName": "NIFTY 50",
        "weekday_pre": 3,   # Thu
        "weekday_post": 1,  # Tue
    },
    "SENSEX": {
        "exchangeSegment": "BSE_FNO",
        "underlyingName": "SENSEX",
        "weekday_pre": 1,   # Tue
        "weekday_post": 3,  # Thu
    }
}

# =============================================================================
# HELPERS
# =============================================================================
def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def _norm(s: str) -> str:
    s = (s or "").upper().strip()
    return "".join(ch for ch in s if ch.isalnum() or ch.isspace()).strip()

def _safe_int(x) -> int:
    return int(float(str(x).strip()))

def _detect_cols(fieldnames):
    """
    Dhan IDX_I CSV column names sometimes vary. We detect likely columns.
    """
    f = [c.strip() for c in (fieldnames or [])]

    def pick_exact(cands):
        for c in cands:
            if c in f:
                return c
        return None

    def pick_contains(substrs):
        for c in f:
            lc = c.lower()
            if any(ss.lower() in lc for ss in substrs):
                return c
        return None

    sec_col  = pick_exact(["SECURITY_ID", "security_id"]) or pick_contains(["security", "smst_security"])
    name_col = pick_exact(["DISPLAY_NAME", "SYMBOL_NAME", "TRADING_SYMBOL"]) or pick_contains(["display", "symbol", "trading"])
    exch_col = pick_exact(["EXCH_ID"]) or pick_contains(["exch_id", "exchange"])
    return sec_col, name_col, exch_col

def _score_row(target_key: str, row_name: str) -> float:
    """
    Minimal scoring to reliably pick NIFTY 50 and SENSEX from IDX_I.
    """
    n = _norm(row_name)
    score = 0.0

    if target_key == "NIFTY":
        if n == "NIFTY 50":
            score += 1000
        if "NIFTY" in n:
            score += 200
        if "50" in n:
            score += 150
        for bad in ["BANK", "FIN", "MID", "SMALL", "IT", "NEXT", "100", "200", "500"]:
            if bad in n:
                score -= 60

    elif target_key == "SENSEX":
        if n == "SENSEX":
            score += 1000
        if "SENSEX" in n:
            score += 300
        if "BSE" in n:
            score += 50

    score -= max(0, len(n) - 10) * 0.5
    return score

def _is_weekend(d: date) -> bool:
    return d.weekday() >= 5

def _prev_weekday_trading_day(d: date) -> date:
    """
    For our purposes, "D-1" is previous weekday (skip Sat/Sun).
    We do NOT maintain a full exchange holiday calendar here; holiday handling
    is done by shifting the expiry backward when the API yields no data.
    """
    x = d - timedelta(days=1)
    while _is_weekend(x):
        x -= timedelta(days=1)
    return x

def _week_monday(d: date) -> date:
    return d - timedelta(days=d.weekday())

def _nifty_or_sensex_expiry_weekday(sym: str, week_start_monday: date) -> int:
    rule = WEEKDAY_RULES[sym]
    return rule["weekday_pre"] if week_start_monday < CUTOVER else rule["weekday_post"]

def _generate_scheduled_weekly_expiries(sym: str, start: date, end: date) -> List[date]:
    """
    Generates scheduled weekly expiry dates (not holiday-adjusted).
    Holiday-adjustment is handled later by probing the API and shifting back.
    """
    expiries: List[date] = []
    monday = _week_monday(start)
    while monday <= end:
        wd = _nifty_or_sensex_expiry_weekday(sym, monday)
        cand = monday + timedelta(days=wd)
        if start <= cand <= end:
            expiries.append(cand)
        monday += timedelta(days=7)
    return expiries

def _batch_pairs(expiries: List[date], k: int) -> List[List[date]]:
    return [expiries[i:i+k] for i in range(0, len(expiries), k)]

def _batch_filename(sym: str, batch_expiries: List[date]) -> str:
    """
    Filename encodes: symbol, expiryCode, strike band, and expiry range.
    Note: we keep the tag as "oldest_newest" inside the batch for stability,
    even if we write files newest-first.
    """
    if len(batch_expiries) == 1:
        tag = batch_expiries[0].strftime("%Y%m%d")
    else:
        tag = f"{batch_expiries[0].strftime('%Y%m%d')}_{batch_expiries[-1].strftime('%Y%m%d')}"
    return f"{sym}_W_EXP{EXPIRY_CODE}_ATMpm{STRIKE_BAND}_{tag}.pkl"

def _parse_strike_selector(selector: str) -> int:
    """
    Convert selector -> signed offset for sorting.
      ATM     -> 0
      ATM+3   -> +3
      ATM-10  -> -10
    """
    s = selector.strip().upper()
    if s == "ATM":
        return 0
    if s.startswith("ATM+") and s[4:].isdigit():
        return int(s[4:])
    if s.startswith("ATM-") and s[4:].isdigit():
        return -int(s[4:])
    # fallback: put unknowns at end
    return 10**9

def _post_json(
    session: requests.Session,
    url: str,
    headers: dict,
    payload: dict,
    retries: int = 6,
    base_sleep: float = 0.5
) -> dict:
    """
    Robust POST with exponential backoff on:
      - 429 / rate limit
      - DH-904 (rate limit)
      - 5xx
    """
    for attempt in range(retries):
        r = session.post(url, headers=headers, json=payload, timeout=60)
        if r.status_code == 200:
            return r.json()

        # Try to parse an error response
        try:
            j = r.json()
        except Exception:
            j = {"raw": r.text[:500]}

        err_code = None
        if isinstance(j, dict):
            # Typical Dhan error shape: {errorType, errorCode, errorMessage}
            if j.get("errorCode"):
                err_code = j.get("errorCode")
            # Alternate shape: {status:'failed', data:{'813':'Invalid SecurityId'}}
            elif j.get("status") == "failed" and isinstance(j.get("data"), dict) and j["data"]:
                err_code = next(iter(j["data"].keys()))

        transient = (r.status_code in (429, 500, 502, 503, 504)) or (err_code == "DH-904")
        if transient:
            time.sleep(base_sleep * (2 ** attempt))
            continue

        raise RuntimeError(f"HTTP {r.status_code}: {j}")

    raise RuntimeError(f"Failed after retries: {url}")

def _json_to_df(
    j: dict,
    leg: str,
    sym: str,
    exch_seg: str,
    target_expiry: date,
    strike_selector: str
) -> pd.DataFrame:
    """
    Convert one RollingOption response leg into a row-wise DataFrame.

    Output is 1 row per minute candle (timestamp), for either CE or PE, and for
    the chosen strike selector (ATM / ATM±k).
    """
    data = (j.get("data") or {})
    series = data.get("ce" if leg == "CALL" else "pe") or {}
    ts = series.get("timestamp") or []
    if not ts:
        return pd.DataFrame()

    df = pd.DataFrame({
        "timestamp": ts,                 # UNIX seconds
        "open": series.get("open"),
        "high": series.get("high"),
        "low": series.get("low"),
        "close": series.get("close"),
        "volume": series.get("volume"),
        "oi": series.get("oi"),
        "iv": series.get("iv"),
        "strike": series.get("strike"),
        "spot": series.get("spot"),      # underlying (important!)
    })

    # Defensive check: if spot missing, something is wrong with requiredData
    if "spot" not in df.columns or df["spot"].isna().all():
        raise RuntimeError("Spot (underlying) not returned. Ensure 'spot' is included in requiredData.")

    # ---- Time conversions (readable) ----
    # dt_ist: timezone-aware datetime in Asia/Kolkata
    dt_utc = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["dt_ist"] = dt_utc.dt.tz_convert(TIMEZONE_IST)

    # Additional "readable" column explicitly requested:
    # - timestamp_dt: datetime object (IST)
    # - timestamp_str: formatted string (IST)
    df["timestamp_dt"] = df["dt_ist"]
    df["timestamp_str"] = df["dt_ist"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # Date-only convenience column
    df["date_ist"] = df["dt_ist"].dt.date

    # ---- Metadata ----
    df["symbol"] = sym
    df["exchangeSegment"] = exch_seg
    df["expiryFlag"] = "WEEK"
    df["expiryCode"] = EXPIRY_CODE

    df["strikeSelector"] = strike_selector
    df["strike_offset"] = _parse_strike_selector(strike_selector)

    df["leg"] = "CE" if leg == "CALL" else "PE"

    # target_expiry_date = the actual expiry date we ended up using (holiday-adjusted)
    # --- HARD GUARD 1: never allow rows after the expiry-hint date ---
    df = df[df["date_ist"] <= target_expiry].reset_index(drop=True)
    if df.empty:
        return df

    # --- Determine D0 and D-1 from the data itself (holiday-safe) ---
    d0 = df["date_ist"].max()
    prev_dates = df.loc[df["date_ist"] < d0, "date_ist"]
    d_minus_1 = prev_dates.max() if not prev_dates.empty else None

    # Store the *actual* expiry day present in data (not just the hint)
    df["target_expiry_date"] = d0

    def _role(d):
        if d == d0:
            return "D0"
        if d_minus_1 is not None and d == d_minus_1:
            return "D-1"
        return "OTHER"

    df["day_role"] = df["date_ist"].map(_role)
    df = df[df["day_role"].isin(["D-1", "D0"])].reset_index(drop=True)
    return df

# =============================================================================
# CORE FETCHER
# =============================================================================
@dataclass(frozen=True)
class SymbolCfg:
    symbol: str
    exchange_segment: str
    underlying_display_name: str
    underlying_security_id: int

class RollingExpiryDownloader:
    def __init__(self):
        self.session = requests.Session()
        self.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "access-token": ACCESS_TOKEN,
            "client-id": CLIENT_ID,
        }

    def resolve_underlying_ids(self) -> Dict[str, SymbolCfg]:
        """
        Resolve underlying securityId for NIFTY 50 and SENSEX from the IDX_I instrument CSV.
        """
        r = self.session.get(
            IDX_I_URL,
            headers={"access-token": ACCESS_TOKEN, "client-id": CLIENT_ID},
            timeout=60
        )
        r.raise_for_status()

        rows = list(csv.DictReader(r.text.splitlines()))
        if not rows:
            raise RuntimeError("IDX_I returned no rows.")

        sec_col, name_col, _ = _detect_cols(rows[0].keys())
        if not sec_col or not name_col:
            raise RuntimeError(f"IDX_I CSV columns not detected. cols={list(rows[0].keys())}")

        out: Dict[str, SymbolCfg] = {}

        for sym in ("NIFTY", "SENSEX"):
            best = None
            best_sc = -1e9
            for row in rows:
                nm = row.get(name_col, "")
                sc = _score_row(sym, nm)
                if sc > best_sc:
                    best_sc = sc
                    best = row

            if not best or best_sc < 100:
                raise RuntimeError(f"Could not resolve securityId for {sym} from IDX_I list.")

            out[sym] = SymbolCfg(
                symbol=sym,
                exchange_segment=WEEKDAY_RULES[sym]["exchangeSegment"],
                underlying_display_name=WEEKDAY_RULES[sym]["underlyingName"],
                underlying_security_id=_safe_int(best[sec_col]),
            )

        return out

    def fetch_window_for_exact_expiry(
        self,
        cfg: SymbolCfg,
        expiry: date,
        strike_selector: str
    ) -> pd.DataFrame:
        """
        Fetch both legs (CALL+PUT) for a given expiry date and strike selector,
        covering D-1 and D0 (via fromDate=prev_weekday, toDate=expiry+1).
        """
        # prev_day = _prev_weekday_trading_day(expiry)
        # from_date = prev_day.isoformat()
        from_date = (expiry - timedelta(days=7)).isoformat()
        to_date = (expiry + timedelta(days=1)).isoformat()  # non-inclusive boundary

        base_payload = {
            "exchangeSegment": cfg.exchange_segment,
            "interval": "1",
            "securityId": cfg.underlying_security_id,
            "instrument": "OPTIDX",
            "expiryFlag": "WEEK",
            "expiryCode": EXPIRY_CODE,
            "strike": strike_selector,
            "requiredData": REQUIRED_DATA,
            "fromDate": from_date,
            "toDate": to_date,
        }

        # CALL leg
        payload = dict(base_payload)
        payload["drvOptionType"] = "CALL"
        j_call = _post_json(self.session, ROLLING_URL, self.headers, payload)
        time.sleep(SLEEP_BETWEEN_CALLS)

        # PUT leg
        payload = dict(base_payload)
        payload["drvOptionType"] = "PUT"
        j_put = _post_json(self.session, ROLLING_URL, self.headers, payload)

        df_call = _json_to_df(j_call, "CALL", cfg.symbol, cfg.exchange_segment, expiry, strike_selector)
        df_put = _json_to_df(j_put, "PUT", cfg.symbol, cfg.exchange_segment, expiry, strike_selector)

        return pd.concat([df_call, df_put], ignore_index=True)

    def resolve_actual_expiry_with_atm(
        self,
        cfg: SymbolCfg,
        scheduled_expiry: date
    ) -> Tuple[Optional[date], pd.DataFrame]:
        """
        If scheduled expiry is a holiday/non-trading day, the API often returns
        empty/failure. This function probes by shifting backwards until data exists.

        We return:
          - actual_expiry date (holiday-adjusted)
          - the already-fetched ATM dataframe for that actual expiry
            (so we don't re-fetch ATM again)
        """
        for shift in range(0, MAX_SHIFT_BACK_DAYS + 1):
            expiry = scheduled_expiry - timedelta(days=shift)

            # Quick weekend skip; holidays are handled by probing
            if _is_weekend(expiry):
                continue

            try:
                df_atm = self.fetch_window_for_exact_expiry(cfg, expiry, "ATM")
            except RuntimeError as e:
                # "no data / invalid params" -> keep shifting back
                msg = str(e)
                if "DH-905" in msg or "DH-907" in msg or "811" in msg or "812" in msg:
                    continue
                raise

            if not df_atm.empty:
                actual = df_atm["target_expiry_date"].max()
                return actual, df_atm

        return None, pd.DataFrame()

# =============================================================================
# MAIN
# =============================================================================
def main():
    _ensure_dir(OUT_DIR)

    end = date.today()
    start = end - timedelta(days=LOOKBACK_DAYS)

    dl = RollingExpiryDownloader()
    cfgs = dl.resolve_underlying_ids()

    for sym in ("NIFTY", "SENSEX"):
        cfg = cfgs[sym]

        # Scheduled expiries are generated deterministically by weekday rules
        scheduled = _generate_scheduled_weekly_expiries(sym, start, end)

        # Batch them into files
        batches = _batch_pairs(scheduled, BATCH_EXPIRIES)

        print(
            f"[INFO] {sym}: scheduled expiries={len(scheduled)} batches={len(batches)} "
            f"(output={os.path.abspath(OUT_DIR)}) strike_selectors={len(STRIKE_SELECTORS)}"
        )

        batch_iter = reversed(batches) if PROCESS_NEWEST_FIRST else batches

        for b in batch_iter:
            out_name = _batch_filename(sym, b)
            out_path = os.path.join(OUT_DIR, out_name)
            tmp_path = out_path + ".tmp"

            # Skip if already downloaded
            if os.path.exists(out_path):
                continue

            all_parts: List[pd.DataFrame] = []
            actual_expiries: List[date] = []

            for scheduled_expiry in b:
                # 1) Resolve actual expiry (holiday-adjusted) once, using ATM probe
                actual_expiry, df_atm = dl.resolve_actual_expiry_with_atm(cfg, scheduled_expiry)
                if actual_expiry is None or df_atm.empty:
                    print(f"[WARN] {sym}: no data for scheduled expiry {scheduled_expiry} (skipped)")
                    continue

                actual_expiries.append(actual_expiry)

                # 2) Keep the ATM data we already fetched
                all_parts.append(df_atm)

                # 3) Fetch all other supported strike selectors for the same actual expiry
                for strike_sel in STRIKE_SELECTORS:
                    if strike_sel == "ATM":
                        continue  # already fetched

                    try:
                        df = dl.fetch_window_for_exact_expiry(cfg, actual_expiry, strike_sel)
                    except RuntimeError as e:
                        # If that strike selector doesn't exist / returns no data for that date, skip
                        msg = str(e)
                        if "DH-905" in msg or "DH-907" in msg or "811" in msg or "812" in msg:
                            continue
                        raise

                    if not df.empty:
                        all_parts.append(df)

                    time.sleep(SLEEP_BETWEEN_CALLS)

            # Nothing collected for this batch => nothing to write
            if not all_parts:
                continue

            out_df = pd.concat(all_parts, ignore_index=True)

            # Batch meta (also encoded in filename)
            out_df["batch_expiries"] = ",".join([d.isoformat() for d in actual_expiries])

            # Sort so downstream analysis is consistent and "latest first"
            out_df = out_df.sort_values(
                ["target_expiry_date", "strike_offset", "leg", "timestamp"],
                ascending=[False, True, True, True]
            ).reset_index(drop=True)

            # Atomic-ish write: temp -> rename (prevents partial/corrupt files)
            with open(tmp_path, "wb") as f:
                pickle.dump(out_df, f, protocol=pickle.HIGHEST_PROTOCOL)
            os.replace(tmp_path, out_path)

            print(f"[OK] wrote {out_name} rows={len(out_df)} actual_expiries={actual_expiries}")

if __name__ == "__main__":
    main()