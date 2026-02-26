import os
import csv
import time
import json
import math
import pickle
import requests
import pandas as pd
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

# ============================
# CONFIG
# ============================
OUT_DIR = os.getenv("DHAN_EXPIRED_OUTDIR", "./dhan_expired_rolling_pickles")
BATCH_EXPIRIES = 2                 # 2 expiries per file => 4 days (D-1 + D0 for each expiry)
LOOKBACK_DAYS = 365 * 2            # last 2 years
MAX_SHIFT_BACK_DAYS = 7            # if scheduled expiry is holiday, shift back
SLEEP_BETWEEN_CALLS = 0.15         # polite throttle
TIMEZONE_IST = "Asia/Kolkata"

# RollingOption (Expired Options Data)
ROLLING_URL = "https://api.dhan.co/v2/charts/rollingoption"
IDX_I_URL = "https://api.dhan.co/v2/instrument/IDX_I"

# Required env var
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "").strip()
if not ACCESS_TOKEN:
    raise SystemExit("Missing DHAN_ACCESS_TOKEN env var.")

CLIENT_ID = os.getenv("DHAN_CLIENT_ID", "").strip()
if not CLIENT_ID:
    raise SystemExit("Missing DHAN_CLIENT_ID env var.")

# If you want ATM+/-k later, change this.
STRIKE_SELECTOR = "ATM"

# Data fields supported by RollingOption.
REQUIRED_DATA = ["open", "high", "low", "close", "iv", "volume", "strike", "oi", "spot"]

# ExpiryCode enum: 0=Current/Near, 1=Next, 2=Far (from Dhan Annexure).
EXPIRY_CODE = 0

# Weekly expiry weekday rules (weekday: Mon=0 ... Sun=6), keyed by week-start (Monday).
CUTOVER = date(2025, 9, 1)

# NIFTY weekly: Thu -> Tue
# SENSEX weekly: Tue -> Thu
# We determine the weekday by the Monday of that week.
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

# ============================
# Helpers (based on your reference script’s structure)
# ============================
def _norm(s: str) -> str:
    s = (s or "").upper().strip()
    return "".join(ch for ch in s if ch.isalnum() or ch.isspace()).strip()

def _safe_int(x) -> int:
    return int(float(str(x).strip()))

def _detect_cols(fieldnames):
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

def _score_row(target_key: str, row_name: str, row_exch: str) -> float:
    """
    Minimal scoring to reliably pick NIFTY 50 and SENSEX from IDX_I.
    Mirrors the approach in your reference file.
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

def _post_json(session: requests.Session, url: str, headers: dict, payload: dict,
              retries: int = 6, base_sleep: float = 0.5) -> dict:
    """
    Robust POST with exponential backoff on:
      - DH-904 (rate limit) / 429
      - 5xx
    Matches your reference script style, extended for DH-904.
    """
    for attempt in range(retries):
        r = session.post(url, headers=headers, json=payload, timeout=60)
        if r.status_code == 200:
            return r.json()

        # parse error
        try:
            j = r.json()
        except Exception:
            j = {"raw": r.text[:500]}

        err_code = None
        err_msg = None

        if isinstance(j, dict):
            # Dhan style {errorType, errorCode, errorMessage}
            if j.get("errorCode"):
                err_code = j.get("errorCode")
                err_msg = j.get("errorMessage")
            # Alternate {status:'failed', data:{'813':'Invalid SecurityId'}}
            elif j.get("status") == "failed" and isinstance(j.get("data"), dict) and j["data"]:
                err_code = next(iter(j["data"].keys()))
                err_msg = j["data"][err_code]

        transient = (r.status_code in (429, 500, 502, 503, 504)) or (err_code == "DH-904")
        if transient:
            sleep_s = base_sleep * (2 ** attempt)
            time.sleep(sleep_s)
            continue

        raise RuntimeError(f"HTTP {r.status_code}: {j}")

    raise RuntimeError(f"Failed after retries: {url}")

def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def _daterange_days(a: date, b_exclusive: date):
    d = a
    while d < b_exclusive:
        yield d
        d += timedelta(days=1)

def _is_weekend(d: date) -> bool:
    return d.weekday() >= 5

def _prev_weekday_trading_day(d: date) -> date:
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
    Generates *scheduled* weekly expiry dates (not holiday-adjusted),
    based on the week-start regime change.
    """
    expiries = []
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
    # Intuitive: includes expiry dates (not the D-1 dates)
    if len(batch_expiries) == 1:
        tag = batch_expiries[0].strftime("%Y%m%d")
    else:
        tag = f"{batch_expiries[0].strftime('%Y%m%d')}_{batch_expiries[-1].strftime('%Y%m%d')}"
    return f"{sym}_W_EXP{EXPIRY_CODE}_{STRIKE_SELECTOR}_{tag}.pkl"

def _json_to_df(j: dict, leg: str, sym: str, exch_seg: str, target_expiry: date) -> pd.DataFrame:
    """
    Convert RollingOption response leg into a row-wise DataFrame.
    """
    data = (j.get("data") or {})
    series = data.get("ce" if leg == "CALL" else "pe") or {}
    ts = series.get("timestamp") or []
    if not ts:
        return pd.DataFrame()

    df = pd.DataFrame({
        "timestamp": ts,
        "open": series.get("open"),
        "high": series.get("high"),
        "low": series.get("low"),
        "close": series.get("close"),
        "volume": series.get("volume"),
        "oi": series.get("oi"),
        "iv": series.get("iv"),
        "strike": series.get("strike"),
        "spot": series.get("spot"),
    })

    if "spot" not in df.columns or df["spot"].isna().all():
        raise RuntimeError("Spot (underlying) not returned. Ensure 'spot' is included in requiredData.")

    # time columns
    dt_utc = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["dt_ist"] = dt_utc.dt.tz_convert(TIMEZONE_IST)
    df["date_ist"] = df["dt_ist"].dt.date

    # metadata
    df["symbol"] = sym
    df["exchangeSegment"] = exch_seg
    df["expiryFlag"] = "WEEK"
    df["expiryCode"] = EXPIRY_CODE
    df["strikeSelector"] = STRIKE_SELECTOR
    df["leg"] = "CE" if leg == "CALL" else "PE"

    df["target_expiry_date"] = target_expiry
    df["day_role"] = df["date_ist"].apply(lambda x: "D0" if x == target_expiry else "D-1")

    # Keep only D-1 and D0 rows (defensive)
    df = df[df["day_role"].isin(["D-1", "D0"])].reset_index(drop=True)
    return df

# ============================
# Core fetcher
# ============================
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
        r = self.session.get(IDX_I_URL, headers={"access-token": ACCESS_TOKEN, "client-id": CLIENT_ID}, timeout=60)
        r.raise_for_status()
        rows = list(csv.DictReader(r.text.splitlines()))
        sec_col, name_col, exch_col = _detect_cols(rows[0].keys())
        if not sec_col or not name_col:
            raise RuntimeError(f"IDX_I CSV columns not detected. cols={list(rows[0].keys())}")

        out: Dict[str, SymbolCfg] = {}

        for sym in ("NIFTY", "SENSEX"):
            best = None
            best_sc = -1e9
            for row in rows:
                nm = row.get(name_col, "")
                sc = _score_row(sym, nm, row.get(exch_col, "") if exch_col else "")
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

    def fetch_window_for_expiry(self, cfg: SymbolCfg, scheduled_expiry: date) -> Tuple[Optional[date], pd.DataFrame]:
        """
        Fetch D-1 and D0 data for the weekly expiry around a scheduled expiry date.
        If scheduled expiry is a holiday, shift back until data exists.
        """
        for shift in range(0, MAX_SHIFT_BACK_DAYS + 1):
            expiry = scheduled_expiry - timedelta(days=shift)

            # quick weekend skip; if holiday, API will still fail/empty and we keep shifting
            if _is_weekend(expiry):
                continue

            prev_day = _prev_weekday_trading_day(expiry)

            from_date = prev_day.isoformat()
            to_date = (expiry + timedelta(days=1)).isoformat()  # non-inclusive boundary

            base_payload = {
                "exchangeSegment": cfg.exchange_segment,
                "interval": "1",
                "securityId": cfg.underlying_security_id,
                "instrument": "OPTIDX",
                "expiryFlag": "WEEK",
                "expiryCode": EXPIRY_CODE,
                "strike": STRIKE_SELECTOR,
                "requiredData": REQUIRED_DATA,
                "fromDate": from_date,
                "toDate": to_date,
            }

            try:
                # CALL
                payload = dict(base_payload)
                payload["drvOptionType"] = "CALL"
                j_call = _post_json(self.session, ROLLING_URL, self.headers, payload)
                time.sleep(SLEEP_BETWEEN_CALLS)

                # PUT
                payload = dict(base_payload)
                payload["drvOptionType"] = "PUT"
                j_put = _post_json(self.session, ROLLING_URL, self.headers, payload)

            except RuntimeError as e:
                # If parameters/no data, shift back. Otherwise fail fast.
                msg = str(e)
                if "DH-905" in msg or "DH-907" in msg or "811" in msg or "812" in msg:
                    continue
                raise

            df_call = _json_to_df(j_call, "CALL", cfg.symbol, cfg.exchange_segment, expiry)
            df_put  = _json_to_df(j_put,  "PUT",  cfg.symbol, cfg.exchange_segment, expiry)

            df = pd.concat([df_call, df_put], ignore_index=True)
            if not df.empty:
                return expiry, df

            # empty => non-trading day / no data => shift back
        return None, pd.DataFrame()

# ============================
# Main
# ============================
def main():
    _ensure_dir(OUT_DIR)

    end = date.today()
    start = end - timedelta(days=LOOKBACK_DAYS)

    dl = RollingExpiryDownloader()
    cfgs = dl.resolve_underlying_ids()

    for sym in ("NIFTY", "SENSEX"):
        cfg = cfgs[sym]

        scheduled = _generate_scheduled_weekly_expiries(sym, start, end)
        batches = _batch_pairs(scheduled, BATCH_EXPIRIES)

        print(f"[INFO] {sym}: scheduled expiries in range={len(scheduled)} batches={len(batches)} "
              f"(output={os.path.abspath(OUT_DIR)})")

        for b in batches:
            out_name = _batch_filename(sym, b)
            out_path = os.path.join(OUT_DIR, out_name)
            tmp_path = out_path + ".tmp"

            if os.path.exists(out_path):
                continue

            all_parts = []
            actual_expiries = []

            for scheduled_expiry in b:
                actual_expiry, df = dl.fetch_window_for_expiry(cfg, scheduled_expiry)
                if df.empty or actual_expiry is None:
                    print(f"[WARN] {sym}: no data for scheduled expiry {scheduled_expiry} (skipped)")
                    continue

                actual_expiries.append(actual_expiry)
                all_parts.append(df)

                time.sleep(SLEEP_BETWEEN_CALLS)

            if not all_parts:
                # Nothing to write
                continue

            out_df = pd.concat(all_parts, ignore_index=True)

            # sort for nicer downstream processing
            out_df = out_df.sort_values(["target_expiry_date", "leg", "timestamp"]).reset_index(drop=True)

            # Add a small batch-level meta column (also encoded in filename)
            out_df["batch_expiries"] = ",".join([d.isoformat() for d in actual_expiries])

            # Atomic-ish write: temp -> rename
            with open(tmp_path, "wb") as f:
                pickle.dump(out_df, f, protocol=pickle.HIGHEST_PROTOCOL)
            os.replace(tmp_path, out_path)

            print(f"[OK] wrote {out_name} rows={len(out_df)} actual_expiries={actual_expiries}")

if __name__ == "__main__":
    main()