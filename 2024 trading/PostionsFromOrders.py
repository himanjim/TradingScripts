import Utils as util
import pandas as pd
import pytz

if __name__ == '__main__':

    kite = util.intialize_kite_api()

    orders = kite.orders();

    # Create pandas DataFrame from the list of orders
    df = pd.DataFrame(orders)

    # Separate BUY and SELL orders
    buy_orders = df[df['transaction_type'] == 'BUY']
    sell_orders = df[df['transaction_type'] == 'SELL']

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
    for index, row in df_filtered.iterrows():
        positions.append({'exchange': row['exchange'] , 'tradingsymbol': row['tradingsymbol'], 'quantity': row['quantity'], 'price': row['average_price'], 'product': row['product'], 'type':row['transaction_type']})

    print(positions)
