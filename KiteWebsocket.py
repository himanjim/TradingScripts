from datetime import datetime
from KiteTickerInstruments import instruments
from kiteconnect import KiteTicker
import csv

current_day = datetime.today().strftime("%Y_%m_%d")
record_file = 'F:/Trading_Responses/instruments' + current_day + '.csv'

with open(record_file,'ab') as f:
    writer = csv.writer(f)

# Initialise
kws = KiteTicker("453dipfh64qcl484", "QFQyBzJT3iZQ6UIrbInqvG0qnhGgGfMg")


def get_min_sp(tick):
    if 'depth' in tick and 'sell' in tick['depth']:
        return min(item['price'] for item in tick['depth']['sell'])
    return -1


def on_ticks(ws, ticks):
    # Callback to receive ticks.
    records = {}
    curr_time = datetime.now()
    # Spot, Strike, CE, PE, Currtime, PE Timestamp, CE Timestamp
    spot = None
    for tick in ticks:
        if tick['instrument_token'] == instruments['underlying']:
            spot = tick['last_price']

        if instruments[tick['instrument_token']]['strike'] in records:
            records[instruments[tick['instrument_token']]['strike']].update({instruments[tick['instrument_token']]['instrument_type']: [get_min_sp(tick), tick['timestamp'], (curr_time - tick['timestamp']).seconds] })
        else:
            records[instruments[tick['instrument_token']]['strike']] = {instruments[tick['instrument_token']]['instrument_type']: [get_min_sp (tick), tick['timestamp'], (curr_time - tick['timestamp']).seconds]}

        for key, value in records.items():
            writer.writerow ([spot, key, value['CE'][0], value['PE'][0], curr_time, value['CE'][1], value['PE'][1], value['CE'][2], value['PE'][2]])


def on_connect(ws, response):
    # Callback on successful connect.
    # Subscribe to a list of instrument_tokens (RELIANCE and ACC here).
    ws.subscribe(instruments.keys())

    # Set RELIANCE to tick in `full` mode.
    ws.set_mode(ws.MODE_FULL, instruments.keys())


def on_close(ws, code, reason):
    # On connection close stop the main loop
    # Reconnection will not happen after executing `ws.stop()`
    print('Connection closed:', code, reason)


def on_error(ws, code, reason):
    # On connection close stop the main loop
    # Reconnection will not happen after executing `ws.stop()`
    print('Connection error:', code, reason)


# Assign the callbacks.
kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close
kws.on_error = on_error

# Infinite loop on the main thread. Nothing after this will run.
# You have to use the pre-defined callbacks to manage subscriptions.
kws.connect()