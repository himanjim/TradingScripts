import os
from collections import defaultdict, deque
import pandas as pd

ORDERS_CSV = r"C:\Users\Local User\Downloads\orders (1).csv"
OUTPUT_XLSX = r"C:\Users\Local User\Downloads\directional_trade_ledger.xlsx"

PRODUCTS_ALLOWED = {"MIS"}  # set None to include all products

OUTPUT_COLUMNS = [
    "SYMBOL", "PRODUCT", "SIDE", "QTY",
    "ENTRY_DATE", "EXIT_DATE",
    "ENTRY_CLOCK", "EXIT_CLOCK",
    "ENTRY_PRICE", "EXIT_PRICE", "P&L"
]


def resolve_path(filename: str) -> str:
    paths = [
        os.path.join(os.getcwd(), filename),
        os.path.join("/mnt/data", filename),
        os.path.join(os.environ.get("USERPROFILE", ""), "Downloads", filename),
    ]
    for p in paths:
        if os.path.exists(p):
            return p
    return paths[0]


def parse_filled_qty(x) -> int:
    """
    Zerodha Qty. format is usually '500/500'.
    Left side = filled quantity.
    """
    if pd.isna(x):
        return 0
    s = str(x).strip()
    if "/" in s:
        s = s.split("/", 1)[0].strip()
    try:
        return int(float(s))
    except Exception:
        return 0


def build_directional_ledger(orders: pd.DataFrame) -> pd.DataFrame:
    """
    FIFO directional ledger:
    - BUY followed by SELL => long trade, SIDE = BUY
    - SELL followed by BUY => short trade, SIDE = SELL
    - Handles partial fills safely.
    """
    ledger = []

    open_buys = defaultdict(deque)   # key -> deque of [qty, price, time]
    open_sells = defaultdict(deque)  # key -> deque of [qty, price, time]

    orders = orders.sort_values("Time").reset_index(drop=True)

    for _, r in orders.iterrows():
        symbol = str(r["Instrument"]).strip()
        product = str(r["Product"]).strip()
        side = str(r["Type"]).upper().strip()
        qty = int(r["FilledQty"])
        price = float(r["Avg. price"])
        t = r["Time"]

        key = (symbol, product)
        q_left = qty

        if side == "BUY":
            # BUY first closes earlier SELL/short positions.
            while q_left > 0 and open_sells[key]:
                sq, sp, st = open_sells[key][0]
                m = min(q_left, sq)

                ledger.append({
                    "SYMBOL": symbol,
                    "PRODUCT": product,
                    "SIDE": "SELL",
                    "QTY": m,
                    "ENTRY_DATE": st.date(),
                    "EXIT_DATE": t.date(),
                    "ENTRY_CLOCK": st.time(),
                    "EXIT_CLOCK": t.time(),
                    "ENTRY_PRICE": sp,
                    "EXIT_PRICE": price,
                    "P&L": round(m * (sp - price), 2),
                })

                sq -= m
                q_left -= m
                if sq == 0:
                    open_sells[key].popleft()
                else:
                    open_sells[key][0][0] = sq

            if q_left > 0:
                open_buys[key].append([q_left, price, t])

        elif side == "SELL":
            # SELL first closes earlier BUY/long positions.
            while q_left > 0 and open_buys[key]:
                bq, bp, bt = open_buys[key][0]
                m = min(q_left, bq)

                ledger.append({
                    "SYMBOL": symbol,
                    "PRODUCT": product,
                    "SIDE": "BUY",
                    "QTY": m,
                    "ENTRY_DATE": bt.date(),
                    "EXIT_DATE": t.date(),
                    "ENTRY_CLOCK": bt.time(),
                    "EXIT_CLOCK": t.time(),
                    "ENTRY_PRICE": bp,
                    "EXIT_PRICE": price,
                    "P&L": round(m * (price - bp), 2),
                })

                bq -= m
                q_left -= m
                if bq == 0:
                    open_buys[key].popleft()
                else:
                    open_buys[key][0][0] = bq

            if q_left > 0:
                open_sells[key].append([q_left, price, t])

    return pd.DataFrame(ledger, columns=OUTPUT_COLUMNS)


def main():
    orders_path = resolve_path(ORDERS_CSV)
    orders = pd.read_csv(orders_path)
    orders = orders.loc[:, ~orders.columns.str.contains(r"^Unnamed")]

    required = {"Time", "Type", "Instrument", "Product", "Qty.", "Avg. price", "Status"}
    missing = required - set(orders.columns)
    if missing:
        raise ValueError(f"orders CSV missing columns: {missing}")

    orders["Time"] = pd.to_datetime(orders["Time"], errors="coerce")
    orders["Status"] = orders["Status"].astype(str).str.upper().str.strip()
    orders["Type"] = orders["Type"].astype(str).str.upper().str.strip()
    orders["Product"] = orders["Product"].astype(str).str.upper().str.strip()
    orders["FilledQty"] = orders["Qty."].apply(parse_filled_qty)
    orders["Avg. price"] = pd.to_numeric(orders["Avg. price"], errors="coerce")

    orders = orders.dropna(subset=["Time", "Avg. price"])
    orders = orders[
        (orders["Status"] == "COMPLETE") &
        (orders["FilledQty"] > 0) &
        (orders["Type"].isin(["BUY", "SELL"]))
    ].copy()

    if PRODUCTS_ALLOWED is not None:
        orders = orders[orders["Product"].isin(PRODUCTS_ALLOWED)].copy()

    ledger = build_directional_ledger(orders)

    out_path = resolve_path(OUTPUT_XLSX)
    ledger.to_excel(out_path, index=False)

    print(f"✅ Directional trade ledger written to: {out_path}")
    print(ledger)


if __name__ == "__main__":
    main()