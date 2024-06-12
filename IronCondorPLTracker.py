import Utils as util
import time as tm

if __name__ == '__main__':
    MAX_PROFIT = 10000
    MAX_LOSS = -10000

    kite = util.intialize_kite_api()

    while True:
        positions = kite.positions()
        net_pl = 0

        for position in positions['day']:
            net_pl += position['pnl']

        if net_pl >= MAX_PROFIT or net_pl <= MAX_LOSS:

            orders = kite.orders()

            for order in orders:
                kite.exit_order(order['variety'], order['order_id'])
                print(f"Order id {order['order_id']} exited.")

            print(f"All orders exited at P/L {net_pl}.")

            break

        else:
            tm.sleep(1)