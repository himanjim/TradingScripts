import pandas as pd
import datetime as dt
from kiteconnect import KiteConnect


KITE_API_KEY = '453dipfh64qcl484'
KITE_ACCESS_CODE = 'pgFjBt0Z8TOmTyKjRBwvW1uiAhbL8iXc'
MARKET_START_TIME = dt.time (9, 15, 0, 100)
MARKET_END_TIME = dt.time (15, 25, 0)
TRADE_START_TIME = dt.time (9, 15, 30)


def intialize_kite_api():
    kite = KiteConnect (api_key=KITE_API_KEY)

    try:

        kite.set_access_token(KITE_ACCESS_CODE)
    except Exception as e:
        print("Authentication failed", str(e))
        raise

    return kite


if __name__ == '__main__':

    kite = intialize_kite_api()

    orders = kite.orders();

    # Create pandas DataFrame from the list of orders
    df = pd.DataFrame(orders)

    # Separate BUY and SELL orders
    buy_orders = df[df['transaction_type'] == 'BUY'].sort_values(by='order_timestamp')
    sell_orders = df[df['transaction_type'] == 'SELL'].sort_values(by='order_timestamp')

    # Check if the number of BUY and SELL orders are the same
    if len(buy_orders) > len(sell_orders):
        # Drop the extra buy orders, keeping the oldest ones (exclude latest by time)
        buy_orders = buy_orders.iloc[:len(sell_orders)]
    elif len(sell_orders) > len(buy_orders):
        # Drop the extra sell orders, keeping the oldest ones (exclude latest by time)
        sell_orders = sell_orders.iloc[:len(buy_orders)]

    # Proceed only if counts match
    if len(buy_orders) == len(sell_orders):
        # Merge BUY and SELL orders based on matching 'tradingsymbol', 'quantity', and 'product'
        merged_orders = pd.merge(
            sell_orders, buy_orders,
            on=['tradingsymbol', 'quantity', 'product'],
            suffixes=('_sell', '_buy')
        )

    # Get the order IDs of both matching BUY and SELL orders
    order_ids_to_remove = pd.concat([merged_orders['order_id_sell'], merged_orders['order_id_buy']])

    # Drop both matching BUY and SELL orders
    df_filtered = df[~df['order_id'].isin(order_ids_to_remove)]

    # Show the resulting filtered DataFrame
    print(df_filtered)

    positions = []
    # Iterate over each row in the filtered DataFrame
    for index, row in df.iterrows():
        positions.append({'exchange': row['exchange'] , 'tradingsymbol': row['tradingsymbol'], 'quantity': row['quantity'], 'price': row['average_price'], 'product': row['product'], 'type':row['transaction_type']})

    for position in positions:
        print(position)
