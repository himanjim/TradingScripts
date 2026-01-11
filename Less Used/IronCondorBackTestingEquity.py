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
    return datetime.strptime(value, '%d-%b-%y')


if __name__ == '__main__':
    # print(datetime.strptime('31 Jan 2024', '%d %b %Y'))
    # exit(0)
    ####################################
    DRIVE = 'E:'
    files_pattern = DRIVE + '/HDFCBANK OLD DATA/*.csv'
    underlying_file = DRIVE + '/Quote-Equity-HDFCBANK-EQ-05-03-2023-to-04-03-2024.csv'
    UNDERLYING = 'HDFCBANK'
    BUY_OPTION_MULTIPLE = 2
    STRIKE_DIFFERENCE = 20
    BUY_STRIKE_DIFFERENCE = 20
    NO_DAYS_TO_EXPIRY = 3
    hedge = False
    maximum_beareable_loss = -300
    open_or_close = 'Open'
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

    # df = df[df['Underlying Value  '] != '-']

    df['Strike Price  '] = df['Strike Price  '].round(0).astype(int)
    # df['Underlying Value  '] = df['Underlying Value  '].astype(float).astype(int)
    # df['Open  '] = df['Open  '].astype(float)
    # df['Close  '] = df['Close  '].astype(float)

    df['date_obj'] = df['Date  '].apply(date_str_to_obj1)
    underlying_df['date_obj'] = underlying_df['Date '].apply(date_str_to_obj2)

    # for index, row in df.iterrows():
    #     print(row['Date  '], row['Expiry  '])
    #     break

    # newdf = df.loc[(df['Date  '] == '01-Nov-2023') & (df['Expiry  '] == '02-Nov-2023')]

    # print(newdf['Underlying Value  '])

    # print(df['Expiry  '].unique())
    expiry_dates = df['Expiry  '].unique()
    # print(df['Date  '].unique())
    all_dates = df['Date  '].unique()

    trading_outputs = []

    for expiry_date_str in expiry_dates:
        expiry_date_obj = datetime.strptime(expiry_date_str, '%d-%b-%Y')
        trading_date_obj = get_trading_date(expiry_date_obj)

        # print('Expiry Date:', expiry_date_obj)
        # print('Trading Date:', trading_date_obj)

        if trading_date_obj is not None:
            trading_date_str = trading_date_obj.strftime('%d-%b-%Y')
            df_part = df.loc[(df['Date  '] == trading_date_str) & (df['Expiry  '] == expiry_date_str)]

            if df_part.empty:
                print('No data for trading date: ', trading_date_str, ' and expiry date: ', expiry_date_str)
                continue
            try:
                # under_lying_value = df_part['Underlying Value  '].iloc[0]
                under_lying_value = underlying_df.loc[underlying_df['date_obj'] == trading_date_obj][open_or_close].iloc[0]
                under_lying_value_rnd = round(float(under_lying_value.replace(',', '')) / 100) * 100

                buy_put_strike = under_lying_value_rnd - (BUY_OPTION_MULTIPLE * BUY_STRIKE_DIFFERENCE)
                sell_put_strike = under_lying_value_rnd - STRIKE_DIFFERENCE
                sell_call_strike = under_lying_value_rnd + STRIKE_DIFFERENCE
                buy_call_strike = under_lying_value_rnd + (BUY_OPTION_MULTIPLE * BUY_STRIKE_DIFFERENCE)

                df_part_1 = df_part.loc[(df_part['Strike Price  '] == buy_put_strike) & (df_part['Option type  '] == 'PE')]
                if df_part_1.empty:
                    print('Incomplete data for trading date: ', trading_date_str, ' and expiry date: ', expiry_date_str)
                    continue

                buy_put_entry_price = float(df_part.loc[(df_part['Strike Price  '] == buy_put_strike) & (df_part['Option type  '] == 'PE')][open_or_close + '  '].iloc[0])
                sell_put_entry_price = float(df_part.loc[(df_part['Strike Price  '] == sell_put_strike) & (df_part['Option type  '] == 'PE')][open_or_close + '  '].iloc[0])
                sell_call_entry_price = float(df_part.loc[(df_part['Strike Price  '] == sell_call_strike) & (df_part['Option type  '] == 'CE')][open_or_close + '  '].iloc[0])
                buy_call_entry_price = float(df_part.loc[(df_part['Strike Price  '] == buy_call_strike) & (df_part['Option type  '] == 'CE')][open_or_close + '  '].iloc[0])
            except Exception as e:
                print(traceback.format_exc())
                continue

            df_part = df.loc[(df['Date  '] == expiry_date_str) & (df['Expiry  '] == expiry_date_str)]
            if df_part.empty:
                print('No data for trading date: ', expiry_date_str, ' and expiry date: ', expiry_date_str)
                continue

            df_part_1 = df_part.loc[(df_part['Strike Price  '] == buy_put_strike) & (df_part['Option type  '] == 'PE')]
            if df_part_1.empty:
                print('Incomplete data for trading date: ', expiry_date_str, ' and expiry date: ', expiry_date_str)
                continue

            try:
                buy_put_exit_price = float(df_part.loc[(df_part['Strike Price  '] == buy_put_strike) & (df_part['Option type  '] == 'PE')]['Close  ' ].iloc[0])
                sell_put_exit_price = float(df_part.loc[(df_part['Strike Price  '] == sell_put_strike) & (df_part['Option type  '] == 'PE')]['Close  '].iloc[0])
                sell_call_exit_price = float(df_part.loc[(df_part['Strike Price  '] == sell_call_strike) & (df_part['Option type  '] == 'CE')]['Close  '].iloc[0])
                buy_call_exit_price = float(df_part.loc[(df_part['Strike Price  '] == buy_call_strike) & (df_part['Option type  '] == 'CE')]['Close  '].iloc[0])

                if hedge:
                    p_l = (sell_put_entry_price + sell_call_entry_price + buy_call_exit_price + buy_put_exit_price) - (buy_put_entry_price + buy_call_entry_price + sell_put_exit_price + sell_call_exit_price)
                else:
                    p_l = (sell_put_entry_price + sell_call_entry_price) - (sell_put_exit_price + sell_call_exit_price)

                # if expiry_date_str == '24-Feb-2022':
                #     print(expiry_date_str)

                maximum_ce_short_premium = max(df.loc[(df['Strike Price  '] == sell_call_strike) & (df['Option type  '] == 'CE') & (df['Expiry  '] == expiry_date_str) & (df['date_obj'] >= trading_date_obj)]['High  '].apply(float))

                maximum_pe_short_premium = max(df.loc[(df['Strike Price  '] == sell_put_strike) & (df['Option type  '] == 'PE') & (df['Expiry  '] == expiry_date_str) & (df['date_obj'] >= trading_date_obj)]['High  '].apply(float))

                maximum_loss =  (sell_call_entry_price + sell_put_entry_price - maximum_ce_short_premium) if maximum_ce_short_premium >= maximum_pe_short_premium else (sell_call_entry_price + sell_put_entry_price - maximum_pe_short_premium)

            except Exception as e:
                print(traceback.format_exc())
                continue

            max_short_premium =  maximum_ce_short_premium if maximum_ce_short_premium >= maximum_pe_short_premium else maximum_pe_short_premium

            max_short_premium = max_short_premium if (max_short_premium > (sell_call_entry_price + sell_put_entry_price)) else 0

            maximum_loss = maximum_loss if max_short_premium != 0 else 0

            max_premium_type = 'CE' if maximum_ce_short_premium >= maximum_pe_short_premium else 'PE'

            managed_profit = p_l if maximum_loss > maximum_beareable_loss else maximum_beareable_loss

            # managed_profit = p_l if p_l > maximum_beareable_loss else maximum_beareable_loss

            trading_outputs.append([UNDERLYING, under_lying_value, trading_date_str, expiry_date_str, buy_put_strike, buy_put_entry_price, buy_put_exit_price, sell_put_strike, sell_put_entry_price, sell_put_exit_price, sell_call_strike, sell_call_entry_price, sell_call_exit_price, buy_call_strike, buy_call_entry_price, buy_call_exit_price, p_l, 1 if p_l > 0 else 0, managed_profit, maximum_loss, max_short_premium, max_premium_type])
            # rounded_number = round(number / 100) * 100
            # exit(0)

    if len(trading_outputs) > 0:
        trading_outputs.insert(0, ['UNDERLYING', 'VALUE', 'TRADE DATE', 'EXPIRY DATE', 'BUY PUT', 'BUY PUT(EN)', 'BUY PUT(EX)', 'SELL PUT', 'SELL PUT(EN)', 'SELL PUT(EX)', 'SELL CALL', 'SELL CALL(EN)', 'SELL CALL(EX)', 'BUY CALL', 'BUY CALL(EN)', 'BUY CALL(EX)', 'P/L', 'PROFIT', 'MGD PROFIT', 'MAX LOSS', 'MAX PREM.', 'MAX PREM. TYPE'])
        print_statistics(trading_outputs, DRIVE +"/" + UNDERLYING + "_IronCondor.xlsx")
    else:
        print('No results')

    exit(0)

date_string = '16-Nov-2023'
date_object = datetime.strptime(date_string, '%d-%b-%Y')

print(date_object)

date_string = date_object.strftime('%d-%b-%Y')

print(date_string)

# Subtract 5 days from the datetime object
result_date = date_object - timedelta(days=5)

print(result_date)

exit(0)

import requests
URL = "https://www.nseindia.com/api/historical/foCPV?from=01-01-2024&to=31-01-2024&instrumentType=OPTIDX&symbol=BANKNIFTY&year=2024&expiryDate=31-Jan-2024&optionType=CE&strikePrice=49000"
headers = {'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246"}
# Here the user agent is for Edge browser on windows 10. You can find your browser user agent from the above given link.
r = requests.get(url=URL, headers=headers)
print(r.content)

exit(0)
