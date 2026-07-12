import pandas as pd
from datetime import datetime, timedelta
import glob
import traceback
from concurrent.futures import ThreadPoolExecutor


def get_date(value):
    return value.date()


if __name__ == '__main__':

    outputs = []

    DRIVE = 'D:'

    # **********************
    df_bn_5min = pd.read_pickle(DRIVE + '/BN 5 min/BN_5_min.pkl')

    df_bn_5min['Date_Only'] = df_bn_5min['Date'].apply(get_date)

    trading_dates = df_bn_5min['Date_Only'].unique()

    for trading_date in trading_dates:
        df_bn_5min_part_ = df_bn_5min.loc[(df_bn_5min['Date_Only'] == trading_date)]

        date_time_obj_9_15 = datetime(year=trading_date.year, month=trading_date.month, day=trading_date.day, hour=9, minute=15)
        df_bn_5min_part_9_15 = df_bn_5min_part_.loc[(df_bn_5min_part_['Date'] == date_time_obj_9_15)]

        if df_bn_5min_part_9_15.empty:
            continue

        open_9_15 = df_bn_5min_part_9_15['Open'].iloc[0]

        date_time_obj_15_25 = datetime(year=trading_date.year, month=trading_date.month, day=trading_date.day, hour=15, minute=25)
        df_bn_5min_part_15_25 = df_bn_5min_part_.loc[(df_bn_5min_part_['Date'] == date_time_obj_15_25)]

        if df_bn_5min_part_15_25.empty:
            continue

        close_15_25 = df_bn_5min_part_15_25['Close'].iloc[0]

        date_time_obj_14_55 = datetime(year=trading_date.year, month=trading_date.month, day=trading_date.day, hour=14, minute=55)
        close_14_55 = df_bn_5min_part_.loc[(df_bn_5min_part_['Date'] == date_time_obj_14_55)]['Close'].iloc[0]

        profit = 0

        if (open_9_15 > close_14_55) and (close_14_55 < close_15_25):
            profit = 1
        elif (open_9_15 < close_14_55) and (close_14_55 > close_15_25):
            profit = 1

        outputs.append([trading_date, open_9_15, close_14_55, close_15_25, profit, (close_15_25 - close_14_55) if open_9_15 > close_14_55 else (close_14_55 - close_15_25)])

    if len(outputs) > 0:
        outputs.insert(0, ['DATE', 'OPEN', '3PM', 'CLOSE', 'PROFIT', 'POINTS'])

        excel_df = pd.DataFrame(outputs[1:], columns=outputs[0])

        excel_df.to_excel(DRIVE + '/BN 5 min/BN_5min_test.xlsx', index=False)
        # print_statistics(trading_outputs, DRIVE +"/" + UNDERLYING + "_IronCondor.xlsx")
    else:
        print('No results')

    exit(0)



    pass
