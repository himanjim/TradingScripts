# generate_trade_ledger_equity.py
# Intraday EQUITY trade ledger from Zerodha orders.csv (+ optional positions.csv for LTP).
# - Uses FIFO pairing per symbol
# - Handles partial fills
# - Focus: MIS trades by default (change PRODUCTS_ALLOWED if needed)
# - Outputs: Ledger + OpenPositions sheets in trade_ledger.xlsx

import os
from collections import deque
import pandas as pd

# -----------------------------
# Config
# -----------------------------
ORDERS_CSV = r"C:\Users\Local User\Downloads\orders.csv"
POSITIONS_CSV = r"C:\Users\Local User\Downloads\positions.csv"
EXPORT_XLSX = r"C:\Users\Local User\Downloads\trade_ledger.xlsx"

PRODUCTS_ALLOWED = {"MIS"}  # set to None to include all products


# -----------------------------
# Path helper
# -----------------------------
def resolve_path(filename: str) -> str:
    # 1) current working directory
    p = os.path.join(os.getcwd(), filename)
    if os.path.exists(p):
        return p
    # 2) /mnt/data (if running in a sandbox / Colab-like env)
    p = os.path.join("/mnt/data", filename)
    if os.path.exists(p):
        return p
    # 3) Windows Downloads
    home = os.environ.get("USERPROFILE") or os.path.expanduser("~")
    p = os.path.join(home, "Downloads", filename)
    return p


def parse_filled_qty(x) -> int:
    """
    Zerodha 'Qty.' often like '115/115' or '0/600'. We use the left side as filled qty.
    """
    if pd.isna(x):
        return 0
    s = str(x).strip().replace('"', "")
    if not s:
        return 0
    if "/" in s:
        s = s.split("/", 1)[0].strip()
    try:
        return int(float(s))
    except Exception:
        return 0


def pick_col(df: pd.DataFrame, candidates) -> str:
    """
    Return the first existing column name from candidates, else raise.
    """
    for c in candidates:
        if c in df.columns:
            return c
    raise ValueError(f"Missing required column. Tried: {candidates}. Found: {list(df.columns)}")


def main():
    orders_path = resolve_path(ORDERS_CSV)
    pos_path = resolve_path(POSITIONS_CSV)

    orders = pd.read_csv(orders_path).loc[:, lambda d: ~d.columns.str.contains(r"^Unnamed")]
    positions = pd.read_csv(pos_path).loc[:, lambda d: ~d.columns.str.contains(r"^Unnamed")] if os.path.exists(pos_path) else pd.DataFrame()

    # --- tolerate column-name variations ---
    col_time = pick_col(orders, ["Time", "Order timestamp", "order_timestamp", "Timestamp"])
    col_type = pick_col(orders, ["Type", "Transaction type", "transaction_type"])
    col_inst = pick_col(orders, ["Instrument", "Trading symbol", "tradingsymbol"])
    col_prod = pick_col(orders, ["Product", "product"])
    col_qty  = pick_col(orders, ["Qty.", "Quantity", "quantity"])
    col_avg  = pick_col(orders, ["Avg. price", "Average price", "avg_price", "average_price"])
    col_stat = pick_col(orders, ["Status", "status"])
    col_exch = pick_col(orders, ["Exchange", "exchange"]) if "Exchange" in orders.columns or "exchange" in orders.columns else None

    # --- normalize ---
    orders = orders.copy()
    orders[col_time] = pd.to_datetime(orders[col_time], errors="coerce")
    orders[col_type] = orders[col_type].astype(str).str.upper().str.strip()      # BUY / SELL
    orders[col_prod] = orders[col_prod].astype(str).str.upper().str.strip()
    orders[col_stat] = orders[col_stat].astype(str).str.upper().str.strip()
    orders["FilledQty"] = orders[col_qty].apply(parse_filled_qty)
    orders["AvgPrice"] = pd.to_numeric(orders[col_avg], errors="coerce")

    # drop bad rows
    orders = orders.dropna(subset=[col_time, "AvgPrice"])
    orders = orders[(orders[col_stat] == "COMPLETE") & (orders["FilledQty"] > 0)].copy()

    if PRODUCTS_ALLOWED is not None:
        orders = orders[orders[col_prod].isin(PRODUCTS_ALLOWED)].copy()

    if orders.empty:
        print("No COMPLETE filled orders found after filters.")
        return

    # Sort: critical for FIFO correctness
    orders = orders.sort_values(col_time).reset_index(drop=True)

    # Symbol key: include exchange if present to avoid collisions
    def sym_key(r):
        if col_exch:
            return f"{str(r[col_exch]).upper().strip()}:{str(r[col_inst]).upper().strip()}"
        return str(r[col_inst]).upper().strip()

    # -----------------------------
    # FIFO matcher (correct + fast)
    # -----------------------------
    # For each symbol: maintain open BUY legs and open SELL legs separately.
    # Each leg: (qty_remaining, price, time)
    open_buys = {}
    open_sells = {}

    ledger_rows = []

    for _, r in orders.iterrows():
        sym = sym_key(r)
        side = r[col_type]               # BUY/SELL
        prod = r[col_prod]
        qty = int(r["FilledQty"])
        px = float(r["AvgPrice"])
        t = r[col_time]

        if sym not in open_buys:
            open_buys[sym] = deque()
            open_sells[sym] = deque()

        if side == "BUY":
            # BUY can close existing open sells (shorts) FIFO, else it opens a long leg.
            q_left = qty
            while q_left > 0 and open_sells[sym]:
                sq, spx, st = open_sells[sym][0]
                m = min(q_left, sq)

                # entry was SELL @ spx, exit is BUY @ px => short pnl = (entry - exit)*qty
                pnl = m * (spx - px)
                ledger_rows.append({
                    "SYMBOL": sym,
                    "PRODUCT": prod,
                    "SIDE": "SELL",  # short
                    "QTY": m,
                    "ENTRY_TIME": st,
                    "EXIT_TIME": t,
                    "ENTRY_PRICE": spx,
                    "EXIT_PRICE": px,
                    "P&L": pnl,
                })

                sq -= m
                q_left -= m
                if sq == 0:
                    open_sells[sym].popleft()
                else:
                    open_sells[sym][0] = (sq, spx, st)

            if q_left > 0:
                open_buys[sym].append((q_left, px, t))

        else:  # SELL
            # SELL can close existing open buys (longs) FIFO, else it opens a short leg.
            q_left = qty
            while q_left > 0 and open_buys[sym]:
                bq, bpx, bt = open_buys[sym][0]
                m = min(q_left, bq)

                # entry was BUY @ bpx, exit is SELL @ px => long pnl = (exit - entry)*qty
                pnl = m * (px - bpx)
                ledger_rows.append({
                    "SYMBOL": sym,
                    "PRODUCT": prod,
                    "SIDE": "BUY",  # long
                    "QTY": m,
                    "ENTRY_TIME": bt,
                    "EXIT_TIME": t,
                    "ENTRY_PRICE": bpx,
                    "EXIT_PRICE": px,
                    "P&L": pnl,
                })

                bq -= m
                q_left -= m
                if bq == 0:
                    open_buys[sym].popleft()
                else:
                    open_buys[sym][0] = (bq, bpx, bt)

            if q_left > 0:
                open_sells[sym].append((q_left, px, t))

    ledger = pd.DataFrame(ledger_rows)

    # Build open-legs sheet
    open_rows = []
    for sym in sorted(set(open_buys.keys()) | set(open_sells.keys())):
        for q, px, t in open_buys.get(sym, []):
            open_rows.append({"SYMBOL": sym, "OPEN_SIDE": "BUY", "OPEN_QTY": q, "OPEN_PRICE": px, "OPEN_TIME": t})
        for q, px, t in open_sells.get(sym, []):
            open_rows.append({"SYMBOL": sym, "OPEN_SIDE": "SELL", "OPEN_QTY": q, "OPEN_PRICE": px, "OPEN_TIME": t})
    open_df = pd.DataFrame(open_rows)

    if ledger.empty:
        print("No completed BUY↔SELL pairs found (ledger empty).")
    else:
        # Split date/time for Excel
        ledger["ENTRY_TIME"] = pd.to_datetime(ledger["ENTRY_TIME"])
        ledger["EXIT_TIME"] = pd.to_datetime(ledger["EXIT_TIME"])
        ledger["ENTRY_DATE"] = ledger["ENTRY_TIME"].dt.date
        ledger["EXIT_DATE"] = ledger["EXIT_TIME"].dt.date
        ledger["ENTRY_CLOCK"] = ledger["ENTRY_TIME"].dt.time
        ledger["EXIT_CLOCK"] = ledger["EXIT_TIME"].dt.time

        ledger = ledger[[
            "SYMBOL", "PRODUCT", "SIDE", "QTY",
            "ENTRY_DATE", "EXIT_DATE", "ENTRY_CLOCK", "EXIT_CLOCK",
            "ENTRY_PRICE", "EXIT_PRICE", "P&L",
            "ENTRY_TIME", "EXIT_TIME"
        ]].sort_values(["ENTRY_TIME", "SYMBOL"]).reset_index(drop=True)

        # Optional: add LTP from positions.csv if it has Instrument+LTP
        if not positions.empty and "Instrument" in positions.columns and "LTP" in positions.columns:
            ltp_map = positions.set_index("Instrument")["LTP"].to_dict()
            # positions.csv likely has plain symbol (e.g., SBIN); map both styles
            ledger["LTP (positions.csv)"] = ledger["SYMBOL"].apply(lambda s: ltp_map.get(s.split(":")[-1], None))

    # Save workbook
    out = resolve_path(EXPORT_XLSX)  # will point to Downloads if not present in cwd/mnt
    # If resolve_path returns non-existing file path, write there anyway:
    try:
        with pd.ExcelWriter(out, engine="openpyxl") as w:
            ledger.to_excel(w, sheet_name="Ledger", index=False)
            open_df.to_excel(w, sheet_name="OpenPositions", index=False)
        print(f"✅ Trade ledger written to: {out}")
    except Exception:
        # fallback to cwd
        out2 = os.path.join(os.getcwd(), EXPORT_XLSX)
        with pd.ExcelWriter(out2, engine="openpyxl") as w:
            ledger.to_excel(w, sheet_name="Ledger", index=False)
            open_df.to_excel(w, sheet_name="OpenPositions", index=False)
        print(f"✅ Trade ledger written to: {out2}")


if __name__ == "__main__":
    main()
