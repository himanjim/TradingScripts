import pandas as pd
from datetime import datetime, timedelta
import glob
import traceback

def date_str_to_obj1(value):
    return datetime.strptime(value, '%d-%b-%Y')


def date_str_to_obj2(value):
    return datetime.strptime(value, '%d %b %Y')


def nearest_expiry(df, current_trading_date_obj):
    # Filter rows where Expiry_date is greater than the given Trading_date
    filtered_df = df[df['expiry_date_obj'] > current_trading_date_obj]

    # Sort by Expiry_date to get the nearest future date
    filtered_df = filtered_df.sort_values(by='expiry_date_obj')

    # Select the first row which will have the nearest Expiry_date
    if not filtered_df.empty:
        return filtered_df.iloc[0]['expiry_date_obj']
    else:
        return None  # In case there is no valid Expiry


if __name__ == '__main__':

    ####################################
    DRIVE = 'D:'
    files_pattern = DRIVE + '/BN OLD DATA/*.csv'
    underlying_file = DRIVE + '/NIFTY BANK_Historical_PR_01042017to08052024.csv'
    UNDERLYING = 'BN'
    STRIKE_DIFF_PERCENT = 0.00
    NO_DAYS_TO_EXPIRY = 2
    maximum_beareable_loss_per = -0.00416666666
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

    df['Strike Price  '] = df['Strike Price  '].round(0).astype(int)

    df['date_obj'] = df['Date  '].apply(date_str_to_obj1)
    df['expiry_date_obj'] = df['Expiry  '].apply(date_str_to_obj1)
    underlying_df['date_obj'] = underlying_df['Date'].apply(date_str_to_obj2)

    expiry_dates = df['Expiry  '].unique()

    all_dates = df['Date  '].unique()

    trading_outputs = []

    for trading_date in all_dates:
        trading_date_obj = datetime.strptime(trading_date, '%d-%b-%Y')
        expiry_date_obj = nearest_expiry(df, trading_date_obj)

        if expiry_date_obj is not None:
            df_part = df.loc[(df['date_obj'] == trading_date_obj) & (df['expiry_date_obj'] == expiry_date_obj)]

            if df_part.empty:
                print('No data for trading date: ', trading_date_obj, ' and expiry date: ', expiry_date_obj)
                continue
            try:
                # under_lying_value = df_part['Underlying Value  '].iloc[0]
                under_lying_value = underlying_df.loc[underlying_df['date_obj'] == trading_date_obj][open_or_close].iloc[0]

                strike_difference = round((STRIKE_DIFF_PERCENT * under_lying_value) / 100) * 100

                under_lying_value_rnd = round(under_lying_value / 100) * 100

                sell_put_strike = under_lying_value_rnd - strike_difference
                sell_call_strike = under_lying_value_rnd + strike_difference

                sell_put_entry_price = float(df_part.loc[(df_part['Strike Price  '] == sell_put_strike) & (df_part['Option type  '] == 'PE')][open_or_close + '  '].iloc[0])
                sell_call_entry_price = float(df_part.loc[(df_part['Strike Price  '] == sell_call_strike) & (df_part['Option type  '] == 'CE')][open_or_close + '  '].iloc[0])

                sell_put_exit_price = float(df_part.loc[(df_part['Strike Price  '] == sell_put_strike) & (df_part['Option type  '] == 'PE')]['Close  '].iloc[0])
                sell_call_exit_price = float(df_part.loc[(df_part['Strike Price  '] == sell_call_strike) & (df_part['Option type  '] == 'CE')]['Close  '].iloc[0])

                p_l = (sell_put_entry_price + sell_call_entry_price) - (sell_put_exit_price + sell_call_exit_price)

                maximum_ce_short_premium = float(df_part.loc[(df['Strike Price  '] == sell_call_strike) & (df['Option type  '] == 'CE')]['High  '].iloc[0])

                maximum_pe_short_premium = float(df_part.loc[(df['Strike Price  '] == sell_put_strike) & (df['Option type  '] == 'PE')]['High  '].iloc[0])

                maximum_loss =  (sell_call_entry_price + sell_put_entry_price - maximum_ce_short_premium) if maximum_ce_short_premium >= maximum_pe_short_premium else (sell_call_entry_price + sell_put_entry_price - maximum_pe_short_premium)

            except Exception as e:
                print(traceback.format_exc())
                continue

            maximum_beareable_loss = maximum_beareable_loss_per * under_lying_value/2

            max_short_premium =  maximum_ce_short_premium if maximum_ce_short_premium >= maximum_pe_short_premium else maximum_pe_short_premium

            max_premium_type = 'CE' if maximum_ce_short_premium >= maximum_pe_short_premium else 'PE'

            managed_profit = p_l if maximum_loss > maximum_beareable_loss else maximum_beareable_loss

            # if managed_profit < 0:
            #     unavoidable_loss, under_lying_value_strangle, strangle_call_open_price, strangle_call_exit_price, strangle_put_open_price, strangle_put_exit_price, strangle_p_l, full_strangle_p_l, second_trading_date, second_maximum_ce_short_premium, second_maximum_pe_short_premium, second_minimum_ce_short_premium, second_minimum_pe_short_premium = find_second_trade_pl(df, sell_call_strike, sell_call_entry_price, sell_put_strike, sell_put_entry_price, trading_date_obj, expiry_date_str, strike_difference, under_lying_value)
            # else:
            #     unavoidable_loss, under_lying_value_strangle, strangle_call_open_price, strangle_call_exit_price, strangle_put_open_price, strangle_put_exit_price, second_trading_date, second_maximum_ce_short_premium, second_maximum_pe_short_premium, second_minimum_ce_short_premium, second_minimum_pe_short_premium = [None] * 11
            #     strangle_p_l = 0
            #     full_strangle_p_l = 0
            #
            # if unavoidable_loss is not None:
            #     managed_profit = unavoidable_loss
            #     # managed_profit = p_l if p_l > maximum_beareable_loss else maximum_beareable_loss

            # trading_outputs.append([UNDERLYING, under_lying_value, trading_date_obj, expiry_date_obj, sell_put_strike, sell_put_entry_price, sell_put_exit_price, sell_call_strike, sell_call_entry_price, sell_call_exit_price, p_l, 1 if p_l > 0 else 0, managed_profit, 1 if managed_profit > 0 else 0, maximum_loss, max_short_premium, max_premium_type, under_lying_value_strangle, second_trading_date, strangle_call_open_price, strangle_call_exit_price, strangle_put_open_price, strangle_put_exit_price, strangle_p_l, 1 if (strangle_p_l !=None and strangle_p_l > 0) else 0, strangle_p_l, full_strangle_p_l, 1 if (full_strangle_p_l is not None and full_strangle_p_l > 0) > 0 else 0, full_strangle_p_l, 1 if unavoidable_loss is not None else 0, second_maximum_ce_short_premium, second_maximum_pe_short_premium, second_minimum_ce_short_premium, second_minimum_pe_short_premium])

            trading_outputs.append([UNDERLYING, under_lying_value, trading_date_obj, expiry_date_obj, sell_put_strike, sell_put_entry_price, sell_put_exit_price, sell_call_strike, sell_call_entry_price, sell_call_exit_price, p_l, 1 if p_l > 0 else 0, managed_profit, 1 if managed_profit > 0 else 0])


    if len(trading_outputs) > 0:
        trading_outputs = sorted(trading_outputs, key=lambda x: x[3])
        # trading_outputs.insert(0, ['UNDERLYING', 'VALUE', 'TRADE DATE', 'EXPIRY DATE', 'SELL PUT', 'SELL PUT(EN)', 'SELL PUT(EX)', 'SELL CALL', 'SELL CALL(EN)', 'SELL CALL(EX)', 'P/L', 'PROFIT', 'MGD PROFIT', 'MGD P/L', 'MAX LOSS', 'MAX PREM.', 'MAX PREM. TYPE', 'STRANGLE_UNDER', '2ND TRAD DATE', 'STRANGLE SELL CALL(EN)', 'STRANGLE SELL CALL(EX)', 'STRANGLE SELL PUT(EN)', 'STRANGLE SELL PUT(EX)', 'STRANGLE P/L', 'STRANGLE SELL PROFIT', 'STRANGLE P/L MGD', 'FULL STRANGLE P/L', 'FULL STRANGLE SELL PROFIT', 'FULL STRANGLE P/L MGD', 'UNAVOID. LOSS', '2ND MAX CE PRE', '2ND MAX PE PRE', '2ND MIN CE PRE', '2ND MIN PE PRE'])
        trading_outputs.insert(0, ['UNDERLYING', 'VALUE', 'TRADE DATE', 'EXPIRY DATE', 'SELL PUT', 'SELL PUT(EN)', 'SELL PUT(EX)', 'SELL CALL', 'SELL CALL(EN)', 'SELL CALL(EX)', 'P/L', 'PROFIT', 'MGD PROFIT', 'MGD P/L'])
        excel_df = pd.DataFrame(trading_outputs[1:], columns=trading_outputs[0])

        ###################
        # Group by year and sum the 'amount' column
        print('###################\n')
        print("Total no. of trades:", excel_df.shape[0])
        print("Strike Difference:", STRIKE_DIFF_PERCENT)
        print("Days to expiry:", NO_DAYS_TO_EXPIRY)

        print("\n P/L year wise \n:", excel_df.groupby(pd.Grouper(key='TRADE DATE', freq='YE'))['P/L'].sum().reset_index())
        print("Total P/L:", round(excel_df['P/L'].sum(), 1))
        print("Accuracy(P/L):", round(excel_df['PROFIT'].sum() / excel_df.shape[0], 3))

        # print("\n MGD PROFIT year wise: \n", excel_df.groupby(pd.Grouper(key='TRADE DATE', freq='YE'))['MGD PROFIT'].sum().reset_index())
        # print("MGD PROFIT:", round(excel_df['MGD PROFIT'].sum(), 1))
        # print("Accuracy(MGD PROFIT):", round(excel_df['MGD P/L'].sum() / excel_df.shape[0], 3))
        #
        # print("\n STRANGLE PROFIT:", excel_df['STRANGLE P/L'].sum())
        # print("Accuracy(STRANGLE PROFIT):", round(excel_df['STRANGLE SELL PROFIT'].sum() / len(excel_df[excel_df['MGD P/L'] == 0]), 3))
        # print("\n STRANGLE P/L MGD:", excel_df['STRANGLE P/L MGD'].sum())
        # print("\n STRANGLE PROFIT MGD year wise: \n", excel_df.groupby(pd.Grouper(key='TRADE DATE', freq='YE'))['STRANGLE P/L MGD'].sum().reset_index())
        #
        # print("\n FULL STRANGLE PROFIT:", excel_df['FULL STRANGLE P/L'].sum())
        # print("Accuracy(FULL STRANGLE PROFIT):", round(excel_df['FULL STRANGLE SELL PROFIT'].sum() / len(excel_df[excel_df['MGD P/L'] == 0]), 3))
        # print("\n FULL STRANGLE P/L MGD:", excel_df['FULL STRANGLE P/L MGD'].sum())
        # print("\n FULL STRANGLE PROFIT MGD year wise: \n", excel_df.groupby(pd.Grouper(key='TRADE DATE', freq='YE'))['FULL STRANGLE P/L MGD'].sum().reset_index())
        #
        # print("\n Total Unavoidable losses:", excel_df['UNAVOID. LOSS'].sum())
        print('\n###################')
        #############

        excel_df.to_excel(DRIVE + "".join(("/", str(UNDERLYING), "_", str(STRIKE_DIFF_PERCENT), "_", str(NO_DAYS_TO_EXPIRY), "_IronCondor_Same_Day.xlsx")), index=False)
        # print_statistics(trading_outputs, DRIVE +"/" + UNDERLYING + "_IronCondor.xlsx")
    else:
        print('No results')

    exit(0)


