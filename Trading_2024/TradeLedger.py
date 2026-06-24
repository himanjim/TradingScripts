# TradeLedger_live_modified.py
# -------------------------------------------------------------
# Purpose:
#   1. Login to Zerodha using your existing oUtils helper.
#   2. Download today's completed orders from kite.orders().
#   3. Download positions from kite.positions() to get current/expiry LTP.
#   4. Detect SHORT STRADDLE trades: SELL CE + SELL PE at same strike/expiry
#      within STRADDLE_TIME_WINDOW_SEC.
#   5. Match later BUY orders as exits using FIFO quantity consumption.
#   6. Export an Excel ledger in the exact format requested.
#
# Notes:
#   - P/L and Expiry PL are gross values before brokerage/taxes.
#   - SELL CALL/PUT EXPIRY PRICE is taken from Zerodha positions last_price.
#   - LOTS is calculated from Kite instrument-master lot_size where available.
#   - If instrument-master download fails, LOTS is left blank unless positions
#     already provide lot_size.
# -------------------------------------------------------------

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

# Keep the same import style as your reference DirectionalTradeLedger.py
import Trading_2024.OptionTradeUtils as oUtils


# -------------------------------------------------------------
# Config
# -------------------------------------------------------------
STRADDLE_TIME_WINDOW_SEC = 5
OUTPUT_XLSX = "short_straddle_trade_ledger_live.xlsx"

# Include both MIS and NRML so that no index option trade is missed.
# Set to None if you want to include every product.
PRODUCTS_ALLOWED: Optional[set[str]] = {"MIS", "NRML"}

# Zerodha option exchanges normally needed for index options.
# NFO = NSE F&O; BFO = BSE F&O.
INSTRUMENT_MASTER_EXCHANGES = ("NFO", "BFO")
DOWNLOAD_INSTRUMENT_MASTER = True

# If a straddle entry exists but full BUY exits are not found, keep it out of
# the final ledger. Set True only if you want open trades marked with LTP exits.
INCLUDE_OPEN_TRADES = False

# Exact output format requested by you.
OUTPUT_COLUMNS = [
    "S. NO.",
    "ENTRY DATE",
    "EXIT DATE",
    "EXPIRY DATE",
    "LOTS",
    "Days to Expiry",
    "Entry Time",
    "Time of exit",
    "SELL CALL STRIKE",
    "SELL CALL ENTRY PRICE",
    "SELL CALL EXIT PRICE",
    "SELL PUT STRIKE",
    "SELL PUT ENTRY PRICE",
    "SELL PUT EXIT  PRICE",  # kept with double space as requested
    "SELL CALL EXPIRY PRICE",
    "SELL PUT EXPIRY PRICE",
    "P/L",
    "Expiry PL",
]


# -------------------------------------------------------------
# Data containers
# -------------------------------------------------------------
@dataclass
class ExitDetails:
    qty: int
    avg_price: float
    last_time: pd.Timestamp
    used_order_indices: List[Tuple[int, int]]  # (order_index, qty_used)


# -------------------------------------------------------------
# Basic helpers
# -------------------------------------------------------------
def get_kite_client():
    """Return authenticated Kite client using the oUtils helper available in your project."""
    if hasattr(oUtils, "intialize_kite_api"):
        return oUtils.intialize_kite_api()
    if hasattr(oUtils, "initialize_kite_api"):
        return oUtils.initialize_kite_api()
    raise AttributeError(
        "oUtils does not contain intialize_kite_api() or initialize_kite_api(). "
        "Check Trading_2024.OptionTradeUtils."
    )


def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None or pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None


def safe_int(x: Any) -> Optional[int]:
    try:
        if x is None or pd.isna(x):
            return None
        return int(float(x))
    except Exception:
        return None


def round2(x: Any) -> Optional[float]:
    v = safe_float(x)
    if v is None:
        return None
    return round(v, 2)


def date_only(ts: Any) -> Optional[date]:
    if ts is None or pd.isna(ts):
        return None
    return pd.to_datetime(ts).date()


def time_only(ts: Any) -> Optional[str]:
    if ts is None or pd.isna(ts):
        return None
    return pd.to_datetime(ts).strftime("%H:%M:%S")


def format_date(d: Any) -> Optional[str]:
    if d is None or pd.isna(d):
        return None
    return pd.to_datetime(d).date().isoformat()


def get_downloads_folder() -> Path:
    """Return Windows Downloads folder where possible; fallback to current working directory."""
    candidates = []
    userprofile = os.environ.get("USERPROFILE")
    if userprofile:
        candidates.append(Path(userprofile) / "Downloads")
    candidates.append(Path.home() / "Downloads")
    candidates.append(Path.cwd())

    for p in candidates:
        try:
            p.mkdir(parents=True, exist_ok=True)
            if p.exists() and os.access(p, os.W_OK):
                return p
        except Exception:
            continue

    return Path.cwd()


# -------------------------------------------------------------
# Zerodha download functions
# -------------------------------------------------------------
def fetch_orders_from_zerodha(kite) -> pd.DataFrame:
    """
    Download completed Zerodha orders and normalize them into the columns needed
    by the ledger builder.
    """
    raw_orders = kite.orders()

    rows: List[Dict[str, Any]] = []
    for o in raw_orders:
        status = str(o.get("status", "")).upper().strip()
        if status != "COMPLETE":
            continue

        qty = safe_int(o.get("filled_quantity")) or 0
        avg_price = safe_float(o.get("average_price")) or 0.0
        if qty <= 0 or avg_price <= 0:
            continue

        product = str(o.get("product", "")).upper().strip()
        if PRODUCTS_ALLOWED is not None and product not in PRODUCTS_ALLOWED:
            continue

        order_time = o.get("exchange_timestamp") or o.get("order_timestamp")
        rows.append(
            {
                "Time": pd.to_datetime(order_time, errors="coerce"),
                "Type": str(o.get("transaction_type", "")).upper().strip(),
                "Instrument": str(o.get("tradingsymbol", "")).upper().strip(),
                "Product": product,
                "Exchange": str(o.get("exchange", "")).upper().strip(),
                "FilledQty": qty,
                "Avg. price": avg_price,
                "OrderID": str(o.get("order_id", "")),
                "Status": status,
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df = df.dropna(subset=["Time"])
    df = df[df["Type"].isin(["BUY", "SELL"])]
    df = df.sort_values("Time").reset_index(drop=True)
    return df


def fetch_positions_from_zerodha(kite) -> pd.DataFrame:
    """
    Download Zerodha positions. The same symbol can occur in day and net blocks;
    the function keeps the best non-null LTP/last_price available per symbol.
    """
    try:
        raw = kite.positions()
    except Exception as e:
        print(f"⚠️ Could not download positions: {e}")
        return pd.DataFrame(columns=["Instrument", "Exchange", "LTP", "LotSize"])

    position_rows: List[Dict[str, Any]] = []

    if isinstance(raw, dict):
        blocks: Iterable[Any] = list(raw.get("day", []) or []) + list(raw.get("net", []) or [])
    elif isinstance(raw, list):
        blocks = raw
    else:
        blocks = []

    for p in blocks:
        symbol = str(p.get("tradingsymbol", "")).upper().strip()
        if not symbol:
            continue

        ltp = safe_float(p.get("last_price"))
        if ltp is None:
            ltp = safe_float(p.get("LTP"))
        if ltp is None:
            ltp = safe_float(p.get("close_price"))

        position_rows.append(
            {
                "Instrument": symbol,
                "Exchange": str(p.get("exchange", "")).upper().strip(),
                "LTP": ltp,
                "LotSize": safe_int(p.get("lot_size")),
                "Quantity": safe_int(p.get("quantity")),
                "Product": str(p.get("product", "")).upper().strip(),
            }
        )

    df = pd.DataFrame(position_rows)
    if df.empty:
        return pd.DataFrame(columns=["Instrument", "Exchange", "LTP", "LotSize"])

    # Prefer rows where LTP is available. If duplicate symbols exist, keep first valid.
    df["_has_ltp"] = df["LTP"].notna().astype(int)
    df = df.sort_values(["Instrument", "_has_ltp"], ascending=[True, False])
    df = df.drop_duplicates(subset=["Instrument"], keep="first")
    df = df.drop(columns=["_has_ltp"])
    return df.reset_index(drop=True)


def fetch_instrument_master(kite) -> pd.DataFrame:
    """
    Download instrument master for NFO/BFO to get exact expiry date, strike and lot size.
    If this fails, symbol parsing fallback will still work for common weekly symbols.
    """
    if not DOWNLOAD_INSTRUMENT_MASTER:
        return pd.DataFrame()

    rows: List[Dict[str, Any]] = []
    for exch in INSTRUMENT_MASTER_EXCHANGES:
        try:
            instruments = kite.instruments(exch)
        except Exception as e:
            print(f"⚠️ Could not download instrument master for {exch}: {e}")
            continue

        for ins in instruments:
            symbol = str(ins.get("tradingsymbol", "")).upper().strip()
            ins_type = str(ins.get("instrument_type", "")).upper().strip()
            if not symbol or ins_type not in {"CE", "PE"}:
                continue

            rows.append(
                {
                    "Instrument": symbol,
                    "Exchange": exch,
                    "Underlying": str(ins.get("name", "")).upper().strip(),
                    "ExpiryDate": pd.to_datetime(ins.get("expiry"), errors="coerce"),
                    "Strike": safe_float(ins.get("strike")),
                    "OptionType": ins_type,
                    "LotSize": safe_int(ins.get("lot_size")),
                }
            )

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df = df.drop_duplicates(subset=["Instrument"], keep="first")
    return df.reset_index(drop=True)


# -------------------------------------------------------------
# Option-symbol enrichment
# -------------------------------------------------------------
MONTH_CODE_MAP = {
    "1": 1,
    "2": 2,
    "3": 3,
    "4": 4,
    "5": 5,
    "6": 6,
    "7": 7,
    "8": 8,
    "9": 9,
    "O": 10,
    "N": 11,
    "D": 12,
}

MONTH_NAME_MAP = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}


def parse_common_option_symbol(symbol: str) -> Dict[str, Any]:
    """
    Parse common Zerodha option tradingsymbols.

    Handles examples like:
      - NIFTY2582125100CE     -> YY=25, month=8, day=21, strike=25100, CE
      - BANKNIFTY25JUN50000PE -> YY=25, month=JUN, strike=50000, PE

    Monthly symbols do not always encode exact expiry day. For monthly symbols,
    expiry should preferably come from Kite instrument master.
    """
    s = str(symbol).upper().strip()

    # Weekly numeric/month-code format: UNDERLYING + YY + M + DD + STRIKE + CE/PE
    m = re.match(r"^(?P<underlying>[A-Z]+)(?P<yy>\d{2})(?P<month>[1-9OND])(?P<dd>\d{2})(?P<strike>\d+)(?P<option_type>CE|PE)$", s)
    if m:
        yy = int(m.group("yy"))
        year = 2000 + yy
        month = MONTH_CODE_MAP.get(m.group("month"))
        day = int(m.group("dd"))
        expiry = None
        if month is not None:
            try:
                expiry = pd.Timestamp(date(year, month, day))
            except ValueError:
                expiry = pd.NaT

        return {
            "Underlying_parsed": m.group("underlying"),
            "ExpiryDate_parsed": expiry,
            "Strike_parsed": safe_float(m.group("strike")),
            "OptionType_parsed": m.group("option_type"),
            "Root_parsed": f"{m.group('underlying')}{m.group('yy')}{m.group('month')}{m.group('dd')}",
        }

    # Monthly alpha format: UNDERLYING + YY + MON + STRIKE + CE/PE
    m = re.match(r"^(?P<underlying>[A-Z]+)(?P<yy>\d{2})(?P<mon>JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(?P<strike>\d+)(?P<option_type>CE|PE)$", s)
    if m:
        # Do not invent exact monthly expiry date here. Use instrument master.
        return {
            "Underlying_parsed": m.group("underlying"),
            "ExpiryDate_parsed": pd.NaT,
            "Strike_parsed": safe_float(m.group("strike")),
            "OptionType_parsed": m.group("option_type"),
            "Root_parsed": f"{m.group('underlying')}{m.group('yy')}{m.group('mon')}",
        }

    # Last-resort generic parse: strike before CE/PE.
    m = re.match(r"^(?P<root>.*?)(?P<strike>\d{4,6})(?P<option_type>CE|PE)$", s)
    if m:
        return {
            "Underlying_parsed": None,
            "ExpiryDate_parsed": pd.NaT,
            "Strike_parsed": safe_float(m.group("strike")),
            "OptionType_parsed": m.group("option_type"),
            "Root_parsed": m.group("root"),
        }

    return {
        "Underlying_parsed": None,
        "ExpiryDate_parsed": pd.NaT,
        "Strike_parsed": None,
        "OptionType_parsed": None,
        "Root_parsed": None,
    }


def enrich_orders_with_option_metadata(orders: pd.DataFrame, instruments: pd.DataFrame) -> pd.DataFrame:
    """Attach option type, strike, expiry, underlying and lot size to orders."""
    if orders.empty:
        return orders

    parsed = orders["Instrument"].apply(parse_common_option_symbol).apply(pd.Series)
    df = pd.concat([orders.copy(), parsed], axis=1)

    if not instruments.empty:
        inst_cols = ["Instrument", "Underlying", "ExpiryDate", "Strike", "OptionType", "LotSize"]
        inst_small = instruments[inst_cols].copy()
        df = df.merge(inst_small, on="Instrument", how="left")
    else:
        df["Underlying"] = None
        df["ExpiryDate"] = pd.NaT
        df["Strike"] = None
        df["OptionType"] = None
        df["LotSize"] = None

    # Prefer exact instrument-master values. Fall back to parsed values.
    df["Underlying"] = df["Underlying"].where(df["Underlying"].astype(str).str.len() > 0, df["Underlying_parsed"])
    df["ExpiryDate"] = df["ExpiryDate"].where(df["ExpiryDate"].notna(), df["ExpiryDate_parsed"])
    df["Strike"] = df["Strike"].where(df["Strike"].notna(), df["Strike_parsed"])
    df["OptionType"] = df["OptionType"].where(df["OptionType"].notna(), df["OptionType_parsed"])
    df["Root"] = df["Root_parsed"]

    df["IsOption"] = df["OptionType"].isin(["CE", "PE"]) & df["Strike"].notna()

    # SeriesKey ensures CE and PE are paired only within same expiry/series.
    # If exact expiry is unavailable, Root is used as fallback.
    def make_series_key(r: pd.Series) -> str:
        expiry = r.get("ExpiryDate")
        if pd.notna(expiry):
            return f"{r.get('Underlying') or ''}|{pd.to_datetime(expiry).date().isoformat()}"
        return str(r.get("Root") or "")

    df["SeriesKey"] = df.apply(make_series_key, axis=1)
    return df


# -------------------------------------------------------------
# Position LTP lookup
# -------------------------------------------------------------
def build_ltp_lookup(positions: pd.DataFrame) -> Dict[str, float]:
    if positions.empty:
        return {}
    out: Dict[str, float] = {}
    for _, r in positions.iterrows():
        symbol = str(r.get("Instrument", "")).upper().strip()
        ltp = safe_float(r.get("LTP"))
        if symbol and ltp is not None:
            out[symbol] = ltp
    return out


def build_position_lot_lookup(positions: pd.DataFrame) -> Dict[str, int]:
    if positions.empty or "LotSize" not in positions.columns:
        return {}
    out: Dict[str, int] = {}
    for _, r in positions.iterrows():
        symbol = str(r.get("Instrument", "")).upper().strip()
        lot = safe_int(r.get("LotSize"))
        if symbol and lot and lot > 0:
            out[symbol] = lot
    return out


# -------------------------------------------------------------
# Exit matching
# -------------------------------------------------------------
def get_exit_details(
    orders: pd.DataFrame,
    instrument: str,
    product: str,
    after_time: pd.Timestamp,
    qty_required: int,
    buy_remaining: Dict[int, int],
    consume: bool,
) -> Optional[ExitDetails]:
    """
    Find/consume BUY orders that close qty_required short quantity for an instrument.
    Uses weighted average exit price if multiple BUY orders close the leg.
    """
    if qty_required <= 0:
        return None

    candidates = orders[
        (orders["Instrument"] == instrument)
        & (orders["Product"] == product)
        & (orders["Type"] == "BUY")
        & (orders["Time"] > after_time)
    ].sort_values("Time")

    qty_left = qty_required
    value = 0.0
    used: List[Tuple[int, int]] = []
    last_time: Optional[pd.Timestamp] = None

    for idx, r in candidates.iterrows():
        available = int(buy_remaining.get(idx, 0))
        if available <= 0:
            continue

        take = min(qty_left, available)
        price = safe_float(r["Avg. price"])
        if price is None:
            continue

        value += take * price
        qty_left -= take
        used.append((idx, take))
        last_time = r["Time"]

        if qty_left == 0:
            break

    if qty_left > 0 or last_time is None:
        return None

    if consume:
        for idx, q in used:
            buy_remaining[idx] = int(buy_remaining.get(idx, 0)) - q

    return ExitDetails(
        qty=qty_required,
        avg_price=value / qty_required,
        last_time=last_time,
        used_order_indices=used,
    )


# -------------------------------------------------------------
# Straddle ledger builder
# -------------------------------------------------------------
def calculate_lots(qty: int, call_lot_size: Any, put_lot_size: Any, position_lot_lookup: Dict[str, int], call_symbol: str, put_symbol: str) -> Optional[float]:
    """Calculate lots from quantity and available lot size."""
    lot_size = safe_int(call_lot_size) or safe_int(put_lot_size)
    if not lot_size:
        lot_size = position_lot_lookup.get(call_symbol) or position_lot_lookup.get(put_symbol)

    if not lot_size or lot_size <= 0:
        return None

    lots = qty / lot_size
    return int(lots) if abs(lots - int(lots)) < 1e-9 else round(lots, 2)


def build_short_straddle_ledger(orders: pd.DataFrame, positions: pd.DataFrame) -> pd.DataFrame:
    """
    Build requested short-straddle ledger from completed Zerodha orders.
    Only trades with SELL CE + SELL PE at same strike/expiry are included.
    """
    if orders.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    ltp_lookup = build_ltp_lookup(positions)
    position_lot_lookup = build_position_lot_lookup(positions)

    opts = orders[orders["IsOption"]].copy()
    if opts.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    # Track remaining quantities so partial fills/exits are handled properly.
    sell_remaining: Dict[int, int] = {
        int(idx): int(r["FilledQty"])
        for idx, r in opts[opts["Type"] == "SELL"].iterrows()
    }
    buy_remaining: Dict[int, int] = {
        int(idx): int(r["FilledQty"])
        for idx, r in opts[opts["Type"] == "BUY"].iterrows()
    }

    rows: List[Dict[str, Any]] = []

    group_cols = ["Exchange", "SeriesKey", "Strike", "Product"]
    for _, g in opts[opts["Type"] == "SELL"].groupby(group_cols, dropna=False):
        g = g.sort_values("Time")

        for idx, r in g.iterrows():
            idx = int(idx)
            if sell_remaining.get(idx, 0) <= 0:
                continue

            while sell_remaining.get(idx, 0) > 0:
                this_type = str(r["OptionType"])
                opposite_type = "PE" if this_type == "CE" else "CE"

                candidates = []
                for cidx, c in g.iterrows():
                    cidx = int(cidx)
                    if cidx == idx or sell_remaining.get(cidx, 0) <= 0:
                        continue
                    if str(c["OptionType"]) != opposite_type:
                        continue

                    diff_sec = abs((pd.to_datetime(c["Time"]) - pd.to_datetime(r["Time"])).total_seconds())
                    if diff_sec <= STRADDLE_TIME_WINDOW_SEC:
                        candidates.append((diff_sec, c["Time"], cidx, c))

                if not candidates:
                    break

                candidates.sort(key=lambda x: (x[0], x[1]))
                _, _, opp_idx, opp = candidates[0]

                matched_qty = min(int(sell_remaining[idx]), int(sell_remaining[opp_idx]))
                if matched_qty <= 0:
                    break

                if this_type == "CE":
                    call_entry = r
                    call_idx = idx
                    put_entry = opp
                    put_idx = opp_idx
                else:
                    call_entry = opp
                    call_idx = opp_idx
                    put_entry = r
                    put_idx = idx

                call_symbol = str(call_entry["Instrument"])
                put_symbol = str(put_entry["Instrument"])
                product = str(call_entry["Product"])

                # First preview both exits. Consume BUY quantities only if both legs have exits.
                call_exit = get_exit_details(
                    opts,
                    call_symbol,
                    product,
                    pd.to_datetime(call_entry["Time"]),
                    matched_qty,
                    buy_remaining,
                    consume=False,
                )
                put_exit = get_exit_details(
                    opts,
                    put_symbol,
                    product,
                    pd.to_datetime(put_entry["Time"]),
                    matched_qty,
                    buy_remaining,
                    consume=False,
                )

                if call_exit is None or put_exit is None:
                    if not INCLUDE_OPEN_TRADES:
                        # Do not consume entry quantity if the straddle is not fully closed.
                        break

                    # Optional open-trade mode: use current LTP as exit proxy.
                    call_ltp = ltp_lookup.get(call_symbol)
                    put_ltp = ltp_lookup.get(put_symbol)
                    if call_ltp is None or put_ltp is None:
                        break
                    now_ts = pd.Timestamp.now()
                    call_exit = ExitDetails(matched_qty, float(call_ltp), now_ts, [])
                    put_exit = ExitDetails(matched_qty, float(put_ltp), now_ts, [])
                else:
                    # Now consume exits because both legs are valid.
                    get_exit_details(opts, call_symbol, product, pd.to_datetime(call_entry["Time"]), matched_qty, buy_remaining, consume=True)
                    get_exit_details(opts, put_symbol, product, pd.to_datetime(put_entry["Time"]), matched_qty, buy_remaining, consume=True)

                # Consume SELL entry quantities.
                sell_remaining[call_idx] = int(sell_remaining.get(call_idx, 0)) - matched_qty
                sell_remaining[put_idx] = int(sell_remaining.get(put_idx, 0)) - matched_qty

                entry_ts = min(pd.to_datetime(call_entry["Time"]), pd.to_datetime(put_entry["Time"]))
                exit_ts = max(pd.to_datetime(call_exit.last_time), pd.to_datetime(put_exit.last_time))
                expiry_dt = call_entry.get("ExpiryDate")
                if pd.isna(expiry_dt):
                    expiry_dt = put_entry.get("ExpiryDate")

                entry_date = entry_ts.date()
                expiry_date = pd.to_datetime(expiry_dt).date() if pd.notna(expiry_dt) else None
                days_to_expiry = (expiry_date - entry_date).days if expiry_date is not None else None

                call_entry_price = float(call_entry["Avg. price"])
                put_entry_price = float(put_entry["Avg. price"])
                call_exit_price = float(call_exit.avg_price)
                put_exit_price = float(put_exit.avg_price)

                call_expiry_price = ltp_lookup.get(call_symbol)
                put_expiry_price = ltp_lookup.get(put_symbol)

                gross_pl = matched_qty * (
                    (call_entry_price - call_exit_price)
                    + (put_entry_price - put_exit_price)
                )

                expiry_pl = None
                if call_expiry_price is not None and put_expiry_price is not None:
                    expiry_pl = matched_qty * (
                        (call_entry_price - float(call_expiry_price))
                        + (put_entry_price - float(put_expiry_price))
                    )

                lots = calculate_lots(
                    matched_qty,
                    call_entry.get("LotSize"),
                    put_entry.get("LotSize"),
                    position_lot_lookup,
                    call_symbol,
                    put_symbol,
                )

                rows.append(
                    {
                        "S. NO.": None,  # assigned after sorting
                        "ENTRY DATE": entry_date.isoformat(),
                        "EXIT DATE": exit_ts.date().isoformat(),
                        "EXPIRY DATE": expiry_date.isoformat() if expiry_date else None,
                        "LOTS": lots,
                        "Days to Expiry": days_to_expiry,
                        "Entry Time": entry_ts.strftime("%H:%M:%S"),
                        "Time of exit": exit_ts.strftime("%H:%M:%S"),
                        "SELL CALL STRIKE": int(call_entry["Strike"]) if pd.notna(call_entry["Strike"]) else None,
                        "SELL CALL ENTRY PRICE": round2(call_entry_price),
                        "SELL CALL EXIT PRICE": round2(call_exit_price),
                        "SELL PUT STRIKE": int(put_entry["Strike"]) if pd.notna(put_entry["Strike"]) else None,
                        "SELL PUT ENTRY PRICE": round2(put_entry_price),
                        "SELL PUT EXIT  PRICE": round2(put_exit_price),
                        "SELL CALL EXPIRY PRICE": round2(call_expiry_price),
                        "SELL PUT EXPIRY PRICE": round2(put_expiry_price),
                        "P/L": round2(gross_pl),
                        "Expiry PL": round2(expiry_pl),
                    }
                )

    ledger = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
    if ledger.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    ledger = ledger.sort_values(["ENTRY DATE", "Entry Time"]).reset_index(drop=True)
    ledger["S. NO."] = range(1, len(ledger) + 1)
    return ledger[OUTPUT_COLUMNS]


# -------------------------------------------------------------
# Excel writer
# -------------------------------------------------------------
def write_excel(ledger: pd.DataFrame, output_path: Path) -> None:
    """Write ledger to Excel with simple formatting and column autosizing."""
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        ledger.to_excel(writer, sheet_name="Trade Ledger", index=False)
        ws = writer.sheets["Trade Ledger"]
        ws.freeze_panes = "A2"

        # Autosize columns.
        for col_cells in ws.columns:
            max_len = 0
            col_letter = col_cells[0].column_letter
            for cell in col_cells:
                value = "" if cell.value is None else str(cell.value)
                max_len = max(max_len, len(value))
            ws.column_dimensions[col_letter].width = min(max(max_len + 2, 12), 28)

        # Apply numeric formats.
        price_cols = {
            "SELL CALL ENTRY PRICE",
            "SELL CALL EXIT PRICE",
            "SELL PUT ENTRY PRICE",
            "SELL PUT EXIT  PRICE",
            "SELL CALL EXPIRY PRICE",
            "SELL PUT EXPIRY PRICE",
            "P/L",
            "Expiry PL",
        }
        header_map = {cell.value: cell.column for cell in ws[1]}
        for col_name in price_cols:
            col_idx = header_map.get(col_name)
            if not col_idx:
                continue
            for row in range(2, ws.max_row + 1):
                ws.cell(row=row, column=col_idx).number_format = "#,##0.00"


def main() -> None:
    print("[STEP] Initialising Zerodha Kite API via oUtils ...")
    kite = get_kite_client()

    print("[STEP] Downloading completed orders ...")
    orders = fetch_orders_from_zerodha(kite)
    if orders.empty:
        out_path = get_downloads_folder() / OUTPUT_XLSX
        empty = pd.DataFrame(columns=OUTPUT_COLUMNS)
        write_excel(empty, out_path)
        print("No completed Zerodha orders found for selected filters.")
        print(f"✅ Empty ledger written to: {out_path}")
        return

    print(f"[INFO] Completed orders downloaded: {len(orders)}")

    print("[STEP] Downloading positions for expiry/LTP prices ...")
    positions = fetch_positions_from_zerodha(kite)
    print(f"[INFO] Positions downloaded: {len(positions)}")

    print("[STEP] Downloading instrument master for expiry/lot-size metadata ...")
    instruments = fetch_instrument_master(kite)
    if instruments.empty:
        print("⚠️ Instrument master unavailable. Expiry/lot-size will use symbol/positions fallback only.")
    else:
        print(f"[INFO] Option instruments downloaded: {len(instruments)}")

    print("[STEP] Enriching orders with option metadata ...")
    orders = enrich_orders_with_option_metadata(orders, instruments)

    print("[STEP] Building short-straddle ledger ...")
    ledger = build_short_straddle_ledger(orders, positions)

    out_path = get_downloads_folder() / OUTPUT_XLSX
    print("[STEP] Writing Excel report ...")
    write_excel(ledger, out_path)

    print(f"✅ Short-straddle trade ledger written to: {out_path}")
    print(f"[INFO] Ledger rows: {len(ledger)}")

    if ledger.empty:
        print("⚠️ No completed short straddle found. Check whether CE and PE sell orders are within STRADDLE_TIME_WINDOW_SEC and exits are complete.")
    else:
        print(ledger.to_string(index=False))


if __name__ == "__main__":
    main()
