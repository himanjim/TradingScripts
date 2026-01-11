import csv
import math
import time
import traceback
from datetime import datetime

import Utils as util
from KiteTickerInstruments import instruments
from pytz import timezone


def get_min_sp(tick):
    if 'depth' in tick and 'sell' in tick['depth']:
        min_sp = math.inf
        for item in tick['depth']['sell']:
            if 0 < item['price'] < min_sp:
                min_sp = item['price']
        return min_sp
    return -1


def get_max_bp(tick):
    if 'depth' in tick and 'buy' in tick['depth']:
        max_bp = -math.inf
        for item in tick['depth']['buy']:
            if 0 < item['price'] > max_bp:
                max_bp = item['price']
        return max_bp
    return -1


current_day = datetime.today().strftime("%Y_%m_%d")
record_file = 'F:/Trading_Responses/instruments' + current_day + '.csv'

writer = csv.writer(open(record_file,'a', newline=''))
indian_timezone = timezone('Asia/Calcutta')
if datetime.now (indian_timezone).time () < util.MARKET_START_TIME:
    writer.writerow (['SPOT', 'STRIKE', 'CE(B)', 'PE(B)', 'CE(S)', 'PE(S)', 'CE(LTP)', 'PE(LTP)', 'CURR TIME', 'CE TIME DIFF', 'PE TIME DIFF'])
# Initialise
kite = util.intialize_kite_api()

while datetime.now (indian_timezone).time () < util.MARKET_START_TIME:
    pass


while True:
    try:
        quotes = kite.quote(instruments.keys())

        records = {}
        curr_time = datetime.now()
        # Spot, Strike, CE, PE, Currtime, PE Timestamp, CE Timestamp
        spot = None
        for key, quote in quotes.items():
            if key == 'NSE:NIFTY BANK':
                spot = quote['last_price']
                continue

            min_sp = get_min_sp(quote)

            # if min_sp == 0:
            #     print(quote)

            delay_in_fetch = [(quote['timestamp'] - curr_time).seconds, (curr_time - quote['timestamp']).seconds][curr_time > quote['timestamp']]

            if min_sp == -1 or min_sp == math.inf or delay_in_fetch > 10:
                continue

            max_bp = get_max_bp(quote)

            if instruments[key]['strike'] in records:
                records[instruments[key]['strike']].update({instruments[key]['instrument_type']: [min_sp, quote['timestamp'], delay_in_fetch, max_bp, quote['last_price']]})
            else:
                records[instruments[key]['strike']] = {instruments[key]['instrument_type']: [min_sp, quote['timestamp'], delay_in_fetch, max_bp, quote['last_price']]}

        for key, value in records.items():
            # if ((value['CE'][0] - value['PE'][0])/ value['CE'][0]) < .05:
            # if ((value['CE'][0] - value['PE'][0])/ value['CE'][0]) < .05:
            #     open('F:/Trading_Responses/instruments.txt', 'w').write(key + str(quote))
            #     open('F:/Trading_Responses/instruments.txt', 'w').write(key + str(quote))
            if 'CE' in value and 'PE' in value:
                writer.writerow ([spot, key, value['CE'][0], value['PE'][0], value['CE'][3], value['PE'][3], value['CE'][4], value['PE'][4], curr_time, value['CE'][2], value['PE'][2]])

        time.sleep(1.5)
    except Exception:
        print (traceback.format_exc () )

