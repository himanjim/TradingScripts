# generate_directional_trade_ledger_live.py
# Fetches today's orders directly from Zerodha and creates directional trade ledger.

import os
from collections import defaultdict, deque
import pandas as pd
import Trading_2024.OptionTradeUtils as oUtils

OUTPUT_XLSX = "directional_trade_ledger_live.xlsx"
PRODUCTS_ALLOWED = {"MIS"}  # set None to include CNC etc.

OUTPUT_COLUMNS = [
    "SYMBOL", "PRODUCT", "SIDE", "QTY",
    "ENTRY_DATE", "EXIT_DATE",
    "ENTRY_CLOCK", "EXIT_CLOCK",
    "ENTRY_PRICE", "EXIT_PRICE", "P&L"
]


def build_directional_ledger(orders: pd.DataFrame) -> pd.DataFrame:
    ledger = []
    open_buys = defaultdict(deque)
    open_sells = defaultdict(deque)

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
            while q_left > 0 and open_sells[key]:
                sq, sp, st = open_sells[key][0]
                m = min(q_left, sq)
                ledger.append({
                    "SYMBOL": symbol, "PRODUCT": product, "SIDE": "SELL", "QTY": m,
                    "ENTRY_DATE": st.date(), "EXIT_DATE": t.date(),
                    "ENTRY_CLOCK": st.time(), "EXIT_CLOCK": t.time(),
                    "ENTRY_PRICE": sp, "EXIT_PRICE": price,
                    "P&L": round(m * (sp - price), 2),
                })
                sq -= m; q_left -= m
                if sq == 0: open_sells[key].popleft()
                else: open_sells[key][0][0] = sq

            if q_left > 0:
                open_buys[key].append([q_left, price, t])

        elif side == "SELL":
            while q_left > 0 and open_buys[key]:
                bq, bp, bt = open_buys[key][0]
                m = min(q_left, bq)
                ledger.append({
                    "SYMBOL": symbol, "PRODUCT": product, "SIDE": "BUY", "QTY": m,
                    "ENTRY_DATE": bt.date(), "EXIT_DATE": t.date(),
                    "ENTRY_CLOCK": bt.time(), "EXIT_CLOCK": t.time(),
                    "ENTRY_PRICE": bp, "EXIT_PRICE": price,
                    "P&L": round(m * (price - bp), 2),
                })
                bq -= m; q_left -= m
                if bq == 0: open_buys[key].popleft()
                else: open_buys[key][0][0] = bq

            if q_left > 0:
                open_sells[key].append([q_left, price, t])

    return pd.DataFrame(ledger, columns=OUTPUT_COLUMNS)


def fetch_orders_from_zerodha(kite) -> pd.DataFrame:
    raw = kite.orders()

    rows = []
    for o in raw:
        status = str(o.get("status", "")).upper()
        if status != "COMPLETE":
            continue

        qty = int(o.get("filled_quantity") or 0)
        avg_price = float(o.get("average_price") or 0)

        if qty <= 0 or avg_price <= 0:
            continue

        rows.append({
            "Time": pd.to_datetime(o.get("order_timestamp")),
            "Type": str(o.get("transaction_type", "")).upper(),
            "Instrument": str(o.get("tradingsymbol", "")).upper(),
            "Product": str(o.get("product", "")).upper(),
            "FilledQty": qty,
            "Avg. price": avg_price,
        })

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    df = df.dropna(subset=["Time", "Avg. price"])
    df = df[df["Type"].isin(["BUY", "SELL"])]

    if PRODUCTS_ALLOWED is not None:
        df = df[df["Product"].isin(PRODUCTS_ALLOWED)]

    return df.copy()


def main():
    kite = oUtils.intialize_kite_api()

    orders = fetch_orders_from_zerodha(kite)
    if orders.empty:
        print("No completed Zerodha orders found for selected filters.")
        return

    ledger = build_directional_ledger(orders)

    out_path = os.path.join(os.environ.get("USERPROFILE", os.getcwd()), "Downloads", OUTPUT_XLSX)
    ledger.to_excel(out_path, index=False)

    print(f"✅ Directional trade ledger written to: {out_path}")
    print(ledger)


if __name__ == "__main__":
    main()