import os
import csv
import time
import requests
from statistics import median
from datetime import datetime

# ----------------------------
# CONFIG
# ----------------------------
TARGET_DATE = "2026-02-24"
FROM_DT = f"{TARGET_DATE} 09:15:00"
TO_DT   = f"{TARGET_DATE} 15:30:00"   # keep inside session

ATM_BAND_STEPS = 10   # ATM +/- N strikes (set 10 for a clean sample)
MAX_CONTRACTS_PER_UNDERLYING = None   # optional hard cap for quick run

UNDERLYINGS = [
    {"name": "NIFTY",     "opt_exchange_segment": "NSE_FNO", "preferred_exch": "NSE"},
    {"name": "BANKNIFTY", "opt_exchange_segment": "NSE_FNO", "preferred_exch": "NSE"},
    {"name": "SENSEX",    "opt_exchange_segment": "BSE_FNO", "preferred_exch": "BSE"},
]

IDX_I_INSTRUMENT_LIST = "https://api.dhan.co/v2/instrument/IDX_I"
OPTIONCHAIN_EXPIRYLIST = "https://api.dhan.co/v2/optionchain/expirylist"
OPTIONCHAIN = "https://api.dhan.co/v2/optionchain"
INTRADAY = "https://api.dhan.co/v2/charts/intraday"

ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzcyMDkxODk1LCJpYXQiOjE3NzIwMDU0OTUsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA4NTg4OTMyIn0.EwSkuuRk4VNsrFfNYa2OzzrkyaoiIRQlgKZoMeYtldygO8qtdVvfEy8zD-bexbjtFJN-B4NQwnTZFNg6p6yUTA").strip()
CLIENT_ID = os.getenv("DHAN_CLIENT_ID", "1108588932").strip()

if not ACCESS_TOKEN:
    raise SystemExit("Missing DHAN_ACCESS_TOKEN env var.")
if not CLIENT_ID:
    raise SystemExit("Missing DHAN_CLIENT_ID env var (OptionChain requires client-id).")

# ----------------------------
# Helpers
# ----------------------------
def norm(s: str) -> str:
    s = (s or "").upper().strip()
    return "".join(ch for ch in s if ch.isalnum() or ch.isspace()).strip()

def safe_int(x):
    return int(float(str(x).strip()))

def detect_cols(fieldnames):
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

def score_row(target, row_name, preferred_exch, row_exch):
    n = norm(row_name)
    score = 0
    if preferred_exch and row_exch and norm(row_exch) == norm(preferred_exch):
        score += 50

    if target == "NIFTY":
        if n == "NIFTY 50": score += 1000
        if "NIFTY" in n: score += 200
        if "50" in n: score += 150
        for bad in ["BANK", "FIN", "MID", "SMALL", "IT", "NEXT", "100", "200", "500"]:
            if bad in n: score -= 60

    elif target == "BANKNIFTY":
        if n in ("NIFTY BANK", "BANK NIFTY", "BANKNIFTY"): score += 1000
        if "NIFTY" in n and "BANK" in n: score += 250
        if "BANKNIFTY" in n or "NIFTYBANK" in n: score += 250
        for bad in ["FIN", "MID", "SMALL", "IT", "NEXT"]:
            if bad in n: score -= 40

    elif target == "SENSEX":
        if n == "SENSEX": score += 1000
        if "SENSEX" in n: score += 300
        if "BSE" in n: score += 50

    score -= max(0, len(n) - 10) * 0.5
    return score

def pick_underlying_security_id(rows, sec_col, name_col, exch_col, target, preferred_exch):
    scored = []
    for r in rows:
        sc = score_row(target, r.get(name_col, ""), preferred_exch, r.get(exch_col, "") if exch_col else "")
        scored.append((sc, r))
    scored.sort(key=lambda x: x[0], reverse=True)
    best_sc, best_row = scored[0]
    if best_sc < 100:
        raise RuntimeError(f"Could not confidently resolve IDX_I securityId for {target}.")
    return safe_int(best_row[sec_col])

def post_json(url, headers, payload, retries=4):
    for attempt in range(retries):
        r = requests.post(url, headers=headers, json=payload, timeout=60)
        if r.status_code == 200:
            return r.json()

        # Parse JSON error
        try:
            j = r.json()
        except Exception:
            raise RuntimeError(f"HTTP {r.status_code}: {r.text[:500]}")

        # Dhan-style errors: {errorType, errorCode, errorMessage}
        if isinstance(j, dict) and j.get("errorCode"):
            code = j.get("errorCode")
            msg = j.get("errorMessage")
            raise RuntimeError(f"HTTP {r.status_code}: {code} {msg}")

        # Alternate error form: {"status":"failed","data":{"813":"Invalid SecurityId"}}
        if isinstance(j, dict) and j.get("status") == "failed" and isinstance(j.get("data"), dict) and j["data"]:
            code = next(iter(j["data"].keys()))
            msg = j["data"][code]
            raise RuntimeError(f"HTTP {r.status_code}: {code} {msg}")

        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(1.0 * (2 ** attempt))
            continue

        raise RuntimeError(f"HTTP {r.status_code}: {j}")

    raise RuntimeError(f"Failed after retries: {url}")

def pick_nearest_expiry(expiry_list, target_date_str):
    target = datetime.strptime(target_date_str, "%Y-%m-%d").date()
    dates = sorted(datetime.strptime(d, "%Y-%m-%d").date() for d in expiry_list)
    future = [d for d in dates if d >= target]
    return (future[0] if future else dates[-1]).strftime("%Y-%m-%d")

def infer_strike_step(sorted_strikes):
    diffs = [round(sorted_strikes[i+1] - sorted_strikes[i], 6) for i in range(len(sorted_strikes)-1)]
    diffs = [d for d in diffs if d > 0]
    return median(diffs) if diffs else None

# ----------------------------
# Main logic
# ----------------------------
def fetch_idx_i_rows():
    print("[STEP] Downloading IDX_I instrument list ...")
    resp = requests.get(IDX_I_INSTRUMENT_LIST, timeout=60)
    resp.raise_for_status()
    rdr = csv.DictReader(resp.text.splitlines())
    sec_col, name_col, exch_col = detect_cols(rdr.fieldnames)
    if not sec_col or not name_col:
        raise RuntimeError(f"Could not detect columns in IDX_I CSV. Columns={rdr.fieldnames}")
    return list(rdr), sec_col, name_col, exch_col

def resolve_underlyings(rows, sec_col, name_col, exch_col):
    ids = {}
    for u in UNDERLYINGS:
        ids[u["name"]] = pick_underlying_security_id(
            rows, sec_col, name_col, exch_col,
            target=u["name"], preferred_exch=u["preferred_exch"]
        )
    print("[INFO] Underlying securityIds for OptionChain:", ids)
    return ids

def fetch_option_chain(underlying_scrip_id, expiry):
    headers_oc = {
        "Content-Type": "application/json",
        "access-token": ACCESS_TOKEN,
        "client-id": CLIENT_ID,
    }
    # Option chain rate limit: 1 unique request per ~3 seconds
    time.sleep(3.2)
    return post_json(
        OPTIONCHAIN,
        headers_oc,
        {"UnderlyingScrip": underlying_scrip_id, "UnderlyingSeg": "IDX_I", "Expiry": expiry},
    )

def fetch_expiry_list(underlying_scrip_id):
    headers_oc = {
        "Content-Type": "application/json",
        "access-token": ACCESS_TOKEN,
        "client-id": CLIENT_ID,
    }
    return post_json(
        OPTIONCHAIN_EXPIRYLIST,
        headers_oc,
        {"UnderlyingScrip": underlying_scrip_id, "UnderlyingSeg": "IDX_I"},
    )

def build_contracts_from_chain(chain_json):
    data = (chain_json.get("data") or {})
    spot = float(data.get("last_price"))
    oc = data.get("oc") or {}

    strikes = sorted(float(s) for s in oc.keys())
    step = infer_strike_step(strikes)
    atm = min(strikes, key=lambda x: abs(x - spot))

    # Filter to ATM band
    if step is None:
        lo, hi = min(strikes), max(strikes)
    else:
        lo = atm - (ATM_BAND_STEPS * step)
        hi = atm + (ATM_BAND_STEPS * step)

    contracts = []
    for strike_str, node in oc.items():
        strike = float(strike_str)
        if strike < lo or strike > hi:
            continue
        ce = node.get("ce")
        pe = node.get("pe")
        if ce and "security_id" in ce:
            contracts.append({"strike": strike, "right": "CE", "security_id": int(ce["security_id"])})
        if pe and "security_id" in pe:
            contracts.append({"strike": strike, "right": "PE", "security_id": int(pe["security_id"])})

    return spot, atm, step, contracts

def fetch_intraday_series(option_security_id, opt_exchange_segment):
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "access-token": ACCESS_TOKEN,
    }
    payload = {
        "securityId": str(option_security_id),
        "exchangeSegment": opt_exchange_segment,  # NSE_FNO / BSE_FNO
        "instrument": "OPTIDX",
        "interval": "1",                          # docs example uses string
        "oi": True,
        "fromDate": FROM_DT,
        "toDate": TO_DT,
    }
    return post_json(INTRADAY, headers, payload)

def write_csv(underlying_name, expiry, opt_exchange_segment, contracts):
    out_path = f"{underlying_name}_OPTIDX_{expiry}_{TARGET_DATE}_1min_ATMpm{ATM_BAND_STEPS}.csv"
    print(f"[STEP] Writing {out_path}")

    ok = 0
    no_data = 0
    other_fail = 0

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "underlying", "expiry", "strike", "right", "option_security_id",
            "timestamp_epoch", "open", "high", "low", "close", "volume", "open_interest"
        ])

        for i, c in enumerate(contracts, 1):
            sid = c["security_id"]
            try:
                series = fetch_intraday_series(sid, opt_exchange_segment)
            except RuntimeError as e:
                msg = str(e)
                # Quietly treat DH-905 as "no data for that date" (very common)
                if "DH-905" in msg:
                    no_data += 1
                    continue
                other_fail += 1
                print(f"[WARN] {underlying_name} {c['right']} {c['strike']} sid={sid} failed: {msg}")
                continue

            ts = series.get("timestamp") or []
            o  = series.get("open") or []
            h  = series.get("high") or []
            l  = series.get("low") or []
            cl = series.get("close") or []
            v  = series.get("volume") or []
            oi = series.get("open_interest") or []

            if not ts:
                no_data += 1
                continue

            for k in range(len(ts)):
                w.writerow([
                    underlying_name, expiry, c["strike"], c["right"], sid,
                    ts[k],
                    o[k]  if k < len(o)  else "",
                    h[k]  if k < len(h)  else "",
                    l[k]  if k < len(l)  else "",
                    cl[k] if k < len(cl) else "",
                    v[k]  if k < len(v)  else "",
                    oi[k] if k < len(oi) else "",
                ])
            ok += 1

            if i % 20 == 0:
                print(f"[INFO] {underlying_name}: processed {i}/{len(contracts)} contracts...")

    print(f"[DONE] {out_path} | ok={ok} | dh905_no_data={no_data} | other_fail={other_fail}")

def main():
    rows, sec_col, name_col, exch_col = fetch_idx_i_rows()
    ids = resolve_underlyings(rows, sec_col, name_col, exch_col)

    for u in UNDERLYINGS:
        name = u["name"]
        opt_seg = u["opt_exchange_segment"]

        # expiry list
        exp = fetch_expiry_list(ids[name])
        if exp.get("status") != "success":
            raise RuntimeError(f"Expirylist failed for {name}: {exp}")
        expiry = pick_nearest_expiry(exp["data"], TARGET_DATE)

        # option chain (real-time)
        chain = fetch_option_chain(ids[name], expiry)
        if chain.get("status") != "success":
            raise RuntimeError(f"Optionchain failed for {name}: {chain}")

        spot, atm, step, contracts = build_contracts_from_chain(chain)
        if MAX_CONTRACTS_PER_UNDERLYING:
            contracts = contracts[:MAX_CONTRACTS_PER_UNDERLYING]

        print(f"[INFO] {name}: spot(LTP now)={spot:.2f} atm~{atm} step~{step} contracts_in_band={len(contracts)} expiry={expiry}")

        # Fetch + write
        write_csv(name, expiry, opt_seg, contracts)

if __name__ == "__main__":
    main()