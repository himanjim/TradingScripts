import pandas as pd
from datetime import datetime, timedelta
from openpyxl import Workbook
import glob
import traceback

def print_statistics(results, excel_location):
    wb = Workbook ()

    # grab the active worksheet
    ws = wb.active

    row_count = 1
    for result in results:

        start_char_ascii = 65

        for value in result:
            ws[chr (start_char_ascii) + str (row_count)] = value
            start_char_ascii += 1

        row_count += 1

    wb.save (excel_location)


def get_trading_date(expiryDateObj):

    tradingDateObj = expiryDateObj - timedelta(days=NO_DAYS_TO_EXPIRY)

    while tradingDateObj.strftime('%d-%b-%Y') not in all_dates:
        tradingDateObj = tradingDateObj - timedelta(days=1)
        if (expiryDateObj - tradingDateObj).days >= 10:
            print('No trading date found for expiry date:', expiryDateObj)
            tradingDateObj = None
            break

    return tradingDateObj



def date_str_to_obj1(value):
    return datetime.strptime(value, '%d-%b-%Y')


def date_str_to_obj2(value):
    return datetime.strptime(value, '%d %b %Y')


if __name__ == '__main__':
    # print(datetime.strptime('31 Jan 2024', '%d %b %Y'))
    # exit(0)
    ####################################
    DRIVE = 'E:'
    files_pattern = DRIVE + '/BN OLD DATA/*.csv'
    underlying_file = DRIVE + '/NIFTY BANK_Historical_PR_01042015to20032024.csv'
    UNDERLYING = 'BN'
    NO_DAYS_TO_EXPIRY = 2
    HEDGE_STRIKE_DIFFERENCE = 1000
    ####################################

    # Get a list of all CSV files matching the pattern
    csv_files = glob.glob(files_pattern)

    # Initialize an empty list to store DataFrames
    dfs = []

    # Loop through each CSV file and read it into a DataFrame
    for file in csv_files:
        dfs.append(pd.read_csv(file))

    # Concatenate all DataFrames into a single DataFrame
    df = pd.concat(dfs, ignore_index=True)

    underlying_df = pd.read_csv(underlying_file)

    df['Strike Price  '] = df['Strike Price  '].round(0).astype(int)

    df['date_obj'] = df['Date  '].apply(date_str_to_obj1)
    underlying_df['date_obj'] = underlying_df['Date'].apply(date_str_to_obj2)

    expiry_dates = df['Expiry  '].unique()

    all_dates = df['Date  '].unique()

    trading_outputs = []

    for expiry_date_str in expiry_dates:
        expiry_date_obj = datetime.strptime(expiry_date_str, '%d-%b-%Y')
        trading_date_obj = get_trading_date(expiry_date_obj)

        if trading_date_obj is not None:
            trading_date_str = trading_date_obj.strftime('%d-%b-%Y')
            df_part = df.loc[(df['Date  '] == trading_date_str) & (df['Expiry  '] == expiry_date_str)]

            if df_part.empty:
                print('No data for expiry date: ', expiry_date_str, ' and expiry date: ', expiry_date_str)
                continue

            try:
                under_lying_value = underlying_df.loc[underlying_df['date_obj'] == trading_date_obj]['Open'].iloc[0]
                under_lying_value_rnd = round(under_lying_value / 100) * 100

                buy_put_strike = under_lying_value_rnd - HEDGE_STRIKE_DIFFERENCE
                buy_call_strike = under_lying_value_rnd + HEDGE_STRIKE_DIFFERENCE

                buy_put_entry_price = float(df_part.loc[(df_part['Strike Price  '] == buy_put_strike) & (df_part['Option type  '] == 'PE')]['Close  '].iloc[0])
                buy_call_entry_price = float(df_part.loc[(df_part['Strike Price  '] == buy_call_strike) & (df_part['Option type  '] == 'CE')]['Close  '].iloc[0])

                df_part = df.loc[(df['Date  '] == expiry_date_str) & (df['Expiry  '] == expiry_date_str)]
                buy_put_exit_price = float(df_part.loc[(df_part['Strike Price  '] == buy_put_strike) & (df_part['Option type  '] == 'PE')]['Close  '].iloc[0])
                buy_call_exit_price = float(df_part.loc[(df_part['Strike Price  '] == buy_call_strike) & (df_part['Option type  '] == 'CE')]['Close  '].iloc[0])

                p_l = (buy_call_exit_price + buy_put_exit_price) - (buy_put_entry_price + buy_call_entry_price)

                trading_outputs.append([UNDERLYING, under_lying_value, trading_date_obj, expiry_date_obj, buy_put_strike, buy_put_entry_price, buy_put_exit_price, buy_call_strike, buy_call_entry_price, buy_call_exit_price, p_l, 1 if p_l > 0 else 0])

            except Exception as e:
                print(traceback.format_exc())
                continue

    if len(trading_outputs) > 0:
        trading_outputs = sorted(trading_outputs, key=lambda x: x[3])
        trading_outputs.insert(0, ['UNDERLYING', 'VALUE', 'TRADING DATE', 'EXPIRY DATE', 'BUY PUT', 'BUY PUT(EN)', 'BUY PUT(EX)', 'BUY CALL', 'BUY CALL(EN)', 'BUY CALL(EX)', 'P/L', 'PROFIT'])
        print_statistics(trading_outputs, DRIVE +"/" + UNDERLYING + "_Straddle_Hedge.xlsx")
    else:
        print('No results')

    exit(0)
