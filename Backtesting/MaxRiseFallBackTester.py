import csv
import os
import time
import traceback

import GenericStatPrinter as gstats
from dateutil import parser


def get_rise_pl(level, rs, opn, hgh, cls, lots):
    if rs > level:
        sl = (1 + level) * opn

        if cls > sl or ((sl - hgh) * lots) < max_loss:
            succ = 0
        else:
            succ = 1

        p_l = (sl - cls) * lots

        if ((sl - hgh) * lots) < max_loss:
            p_l = max_loss

        return succ, p_l

    return None, None


def get_fall_pl(level, fl, opn, lw, cls, lots):
    if fl > level:
        buy = (1 - level) * opn

        if cls < buy or ((lw - buy) * lots) < max_loss:
            succ = 0
        else:
            succ = 1

        p_l = (cls - buy) * lots

        if ((lw - buy) * lots) < max_loss:
            p_l = max_loss

        return succ, p_l

    return None, None


current_time = time.strftime ("%Y_%m_%d#%H_%M_%S")
excel_location = 'F:/Trading_Responses/MaxRiseFall_Backtest_excel_' + str (current_time) + '.xlsx'

files = []
for r, d, f in os.walk('F:/Trading_Responses/EQ_data_multi/'):
    for file in f:
        if '.txt' in file:
            files.append(os.path.join(r, file))

max_loss = -5000
responses = []
level1 = .05
level2 = .10
level3 = .15
# files = ['F:/Trading_Responses/Bank_Nifty/BANKNIFTYMar-2018CE_option_csv.txt']
for file in files:
    try:

        with open (file) as csvfile:
            stocks_data = csv.reader (csvfile, delimiter=',', quotechar='"')

            first_row_excluded = False

            for stock_data in stocks_data:
                if not first_row_excluded:
                    first_row_excluded = True
                    continue

                close = float(stock_data[8].strip())
                high = float(stock_data[5].strip())
                low = float(stock_data[6].strip())
                open = float(stock_data[4].strip())

                lots = int(500000 / open)

                rise = (high - open) / open
                level1risesucc, level1risepl= get_rise_pl(level1, rise, open, high, close, lots)
                level2risesucc, level2risepl = get_rise_pl (level2, rise, open, high, close, lots)
                level3risesucc, level3risepl = get_rise_pl (level3, rise, open, high, close, lots)

                fall = (open - low) / open
                level1fallsucc, level1fallpl = get_fall_pl (level1, rise, open, low, close, lots)
                level2fallsucc, level2fallpl = get_fall_pl (level2, rise, open, low, close, lots)
                level3fallsucc, level3fallpl = get_fall_pl (level3, rise, open, low, close, lots)

                responses.append ([stock_data[0].strip (), parser.parse (stock_data[2].strip ()), open, high, low, close, [0, 1][close > open], rise, fall, level1risesucc, level1risepl, level2risesucc, level2risepl, level3risesucc, level3risepl, level1fallsucc, level1fallpl,level2fallsucc, level2fallpl, level3fallsucc, level3fallpl])

    except Exception:
        print (traceback.format_exc () + str(stocks_data) + file)

responses.insert (0, ['SPOT', 'DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'TYPE', 'RISE', 'FALL', 'L1SUCC(RISE)', 'L1PL(RISE)', 'L2SUCC(RISE)', 'L2PL(RISE)', 'L3SUCC(RISE)', 'L3PL(RISE)', 'L1SUCC(FALL)', 'L1PL(FALL)', 'L2SUCC(FALL)', 'L2PL(FALL)', 'L3SUCC(FALL)', 'L3PL(FALL)'])

gstats.print_statistics(responses, excel_location)