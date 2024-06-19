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
    filtered_df = df[(df['expiry_date_obj'] >= current_trading_date_obj) & (df['date_obj'] == current_trading_date_obj)]

    # Sort by Expiry_date to get the nearest future date
    filtered_df = filtered_df.sort_values(by='expiry_date_obj')

    # Select the first row which will have the nearest Expiry_date
    if not filtered_df.empty:
        return filtered_df.iloc[0]['expiry_date_obj']
    else:
        return None  # In case there is no valid Expiry


def get_trading_outputs(df_part_, under_lying_value_rnd_, second_trade_, trading_days_diff_):

    sell_call_strike_ = round((under_lying_value_rnd_ + (STRIKE_DIFF_PERCENT * under_lying_value_rnd_)) / 100) * 100
    sell_put_strike_ = round((under_lying_value_rnd_ - (STRIKE_DIFF_PERCENT * under_lying_value_rnd_)) / 100) * 100

    if second_trade_ is False:
        sell_put_entry_price_ = float(df_part_.loc[(df_part_['Strike Price  '] == sell_put_strike_) & (df_part_['Option type  '] == 'PE')]['Open  '].iloc[0])
        sell_call_entry_price_ = float(df_part_.loc[(df_part_['Strike Price  '] == sell_call_strike_) & (df_part_['Option type  '] == 'CE')]['Open  '].iloc[0])
    else:
        sell_put_entry_price_ = sell_put_entry_price
        sell_call_entry_price_ = sell_call_entry_price

    sell_put_exit_price_ = float(df_part_.loc[(df_part_['Strike Price  '] == sell_put_strike_) & (df_part_['Option type  '] == 'PE')]['Close  '].iloc[0])
    sell_call_exit_price_ = float(df_part_.loc[(df_part_['Strike Price  '] == sell_call_strike_) & (df_part_['Option type  '] == 'CE')]['Close  '].iloc[0])

    p_l_ = (sell_put_entry_price_ + sell_call_entry_price_) - (sell_put_exit_price_ + sell_call_exit_price_)

    maximum_ce_short_premium_ = float(df_part_.loc[(df_part_['Strike Price  '] == sell_call_strike_) & (df_part_['Option type  '] == 'CE')]['High  '].iloc[0])
    minimum_ce_short_premium_ = float(df_part_.loc[(df_part_['Strike Price  '] == sell_call_strike_) & (df_part_['Option type  '] == 'CE')]['Low  '].iloc[0])

    maximum_pe_short_premium_ = float(df_part_.loc[(df_part_['Strike Price  '] == sell_put_strike_) & (df_part_['Option type  '] == 'PE')]['High  '].iloc[0])
    minimum_pe_short_premium_ = float(df_part_.loc[(df_part_['Strike Price  '] == sell_put_strike_) & (df_part_['Option type  '] == 'PE')]['Low  '].iloc[0])

    maximum_loss_ = (sell_call_entry_price_ + sell_put_entry_price_ - maximum_ce_short_premium_ - minimum_pe_short_premium_) if maximum_ce_short_premium_ >= maximum_pe_short_premium_ else (
                sell_call_entry_price_ + sell_put_entry_price_ - maximum_pe_short_premium_ - minimum_ce_short_premium_)

    if second_trade_ is False:
        maximum_beareable_loss_ = maximum_beareable_loss_per * under_lying_value_rnd_/2
    else:
        maximum_beareable_loss_ = maximum_beareable_loss_per * under_lying_value_rnd_/2

    # if (trading_days_diff_ == 0) or (trading_days_diff_ == 1):
    #     maximum_beareable_loss_ = maximum_beareable_loss_per * under_lying_value_rnd_/1

    max_premium_type_ = 'CE' if maximum_ce_short_premium_ >= maximum_pe_short_premium_ else 'PE'

    managed_profit_ = p_l_ if maximum_loss_ > maximum_beareable_loss_ else maximum_beareable_loss_

    return sell_put_strike_, sell_put_entry_price_, sell_put_exit_price_, sell_call_strike_, sell_call_entry_price_, sell_call_exit_price_, p_l_, managed_profit_, maximum_ce_short_premium_, minimum_ce_short_premium_, maximum_pe_short_premium_, minimum_pe_short_premium_, max_premium_type_, maximum_beareable_loss_, maximum_loss_


if __name__ == '__main__':

    ####################################
    DRIVE = 'D:'
    files_pattern = DRIVE + '/BN OLD DATA/*.csv'
    underlying_file = DRIVE + '/NIFTY BANK_Historical_PR_01042017to14062024.csv'
    UNDERLYING = 'BN'
    STRIKE_DIFF_PERCENT = 0.0000
    maximum_beareable_loss_per = -0.00416666666
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
                under_lying_value = underlying_df.loc[underlying_df['date_obj'] == trading_date_obj]['Open'].iloc[0]

                under_lying_value_rnd = round(under_lying_value / 100) * 100

                trading_days_diff = 0
                temp_trading_date_obj = trading_date_obj
                while temp_trading_date_obj < expiry_date_obj:
                    temp_trading_date_obj += timedelta(days=1)

                    # if trading_date_obj in df['date_obj'].values:
                    if df['date_obj'].isin([temp_trading_date_obj]).any():
                        trading_days_diff += 1

                sell_put_strike, sell_put_entry_price, sell_put_exit_price, sell_call_strike, sell_call_entry_price, sell_call_exit_price, p_l, managed_profit , max_short_premium_ce, min_short_premium_ce, max_short_premium_pe, min_short_premium_pe, max_premium_type, maximum_beareable_loss, maximum_loss = get_trading_outputs(df_part, under_lying_value_rnd, False, trading_days_diff)

                if trading_days_diff == 0:
                    second_trade_multiplying_factor = .75
                elif trading_days_diff == 1:
                    second_trade_multiplying_factor = .675
                elif trading_days_diff == 2:
                    second_trade_multiplying_factor = .50
                elif trading_days_diff == 3:
                    second_trade_multiplying_factor = .375
                elif trading_days_diff == 4:
                    second_trade_multiplying_factor = .25
                elif trading_days_diff == 5:
                    second_trade_multiplying_factor = .20

                if managed_profit <= maximum_beareable_loss:
                    if max_premium_type == 'CE':
                        second_trade_underlying = sell_call_strike + ((sell_put_entry_price + sell_call_entry_price - (maximum_beareable_loss * 2)) * second_trade_multiplying_factor)

                    else:
                        second_trade_underlying = sell_put_strike - ((sell_put_entry_price + sell_call_entry_price - (maximum_beareable_loss * 2)) * second_trade_multiplying_factor)

                    second_trade_underlying = round(second_trade_underlying / 100) * 100

                    second_trade_sell_put_strike, second_trade_put_open_price, second_trade_put_exit_price, second_trade_sell_call_strike, second_trade_call_open_price, second_trade_call_exit_price, second_trade_p_l, second_trade_managed_profit, second_trade_max_short_premium_ce, second_trade_min_short_premium_ce, second_trade_max_short_premium_pe, second_trade_min_short_premium_pe, second_trade_max_premium_type, second_trade_maximum_beareable_loss, second_trade_maximum_loss = get_trading_outputs(df_part, second_trade_underlying, True, trading_days_diff)
                else:
                    second_trade_sell_put_strike, second_trade_put_open_price, second_trade_put_exit_price, second_trade_sell_call_strike, second_trade_call_open_price, second_trade_call_exit_price, second_trade_p_l, second_trade_managed_profit, second_trade_max_short_premium_ce, second_trade_min_short_premium_ce, second_trade_max_short_premium_pe, second_trade_min_short_premium_pe, second_trade_max_premium_type, second_trade_underlying, second_trade_maximum_loss = [None] * 15

            except Exception as e:
                print(traceback.format_exc())
                continue

            trading_outputs.append([UNDERLYING, under_lying_value, trading_date_obj, expiry_date_obj, trading_days_diff, sell_put_strike, sell_put_entry_price, sell_put_exit_price, sell_call_strike, sell_call_entry_price, sell_call_exit_price, p_l, 1 if p_l > 0 else 0, managed_profit, 1 if managed_profit > 0 else 0, max_short_premium_ce, min_short_premium_ce, max_short_premium_pe, min_short_premium_pe, max_premium_type, maximum_loss, maximum_beareable_loss, second_trade_underlying, second_trade_call_open_price, second_trade_call_exit_price, second_trade_put_open_price, second_trade_put_exit_price, second_trade_p_l, 1 if (second_trade_p_l !=None and second_trade_p_l > 0) else 0, second_trade_managed_profit, 1 if (second_trade_managed_profit is not None and second_trade_managed_profit > 0) else 0, second_trade_max_short_premium_ce, second_trade_min_short_premium_ce, second_trade_max_short_premium_pe, second_trade_min_short_premium_pe, second_trade_max_premium_type, second_trade_maximum_loss])

    if len(trading_outputs) > 0:
        trading_outputs = sorted(trading_outputs, key=lambda x: x[3])
        trading_outputs.insert(0, ['UNDERLYING',  'VALUE', 'TRADE DATE', 'EXPIRY DATE', 'DIFF. DAYS', 'SELL PUT', 'SELL PUT(EN)', 'SELL PUT(EX)', 'SELL CALL', 'SELL CALL(EN)', 'SELL CALL(EX)', 'P/L', 'PROFIT', 'MGD PROFIT', 'MGD P/L', 'MAX PREM CE', 'MIN PREM CE', 'MAX PREM PE', 'MIN PREM PE', 'MAX PREM TYPE', 'MAX LOSS', 'MAX BEAR, LOSS', '2ND TRADE UNDERLYING', '2ND TRADE CALL OPEN', '2ND TRADE CALL EX', '2ND TRADE PUT OPEN', '2ND TRADE PUT EX', '2ND TRADE PL', '2ND TRADE P/L', '2ND TRADE MG PROFIT', '2ND TRADE MG P/L', '2ND TRADE MAX PREM CE', '2ND TRADE MIN PREM CE', '2ND TRADE MAX PREM PE', '2ND TRADE MIN PREM PE','2ND TRADE PREM TYPE', '2ND MAX LOSS'])

        excel_df = pd.DataFrame(trading_outputs[1:], columns=trading_outputs[0])

        ###################
        # Group by year and sum the 'amount' column
        print('###################\n')
        print("Total no. of trades:", excel_df.shape[0])

        print("\n P/L year wise \n:", excel_df.groupby([pd.Grouper(key='TRADE DATE', freq='YE'), 'DIFF. DAYS'])['P/L'].sum().reset_index())
        print("Total P/L:", round(excel_df['P/L'].sum(), 1))
        print("Accuracy(P/L):", round(excel_df['PROFIT'].sum() / excel_df.shape[0], 3))

        print("\n MGD PROFIT year wise(diff. days): \n", excel_df.groupby([pd.Grouper(key='TRADE DATE', freq='YE'), 'DIFF. DAYS'])['MGD PROFIT'].sum().reset_index())
        print("MGD PROFIT:", round(excel_df['MGD PROFIT'].sum(), 1))
        print("Accuracy(MGD PROFIT):", round(excel_df['MGD P/L'].sum() / excel_df.shape[0], 3))

        print("\n MGD PROFIT year wise: \n",
              excel_df.groupby(pd.Grouper(key='TRADE DATE', freq='YE'))['MGD PROFIT'].sum().reset_index())
        print("MGD PROFIT:", round(excel_df['MGD PROFIT'].sum(), 1))
        print("Accuracy(MGD PROFIT):", round(excel_df['MGD P/L'].sum() / excel_df.shape[0], 3))

        # print("\n 2ND TRADE PROFIT:", excel_df['2ND TRADE PL'].sum())
        # print("2ND TRADE P/L ACCURACY:", round(excel_df['2ND TRADE P/L'].sum() / len(excel_df[excel_df['MGD P/L'] == 0]), 3))
        # print("\n 2ND TRADE PROFIT year wise: \n", excel_df.groupby(pd.Grouper(key='TRADE DATE', freq='YE'))['2ND TRADE PL'].sum().reset_index())
        #
        # print("\n 2ND TRADE P/L MGD:", excel_df['2ND TRADE MG PROFIT'].sum())
        # print("\n 2ND TRADE PROFIT MGD year wise: \n", excel_df.groupby(pd.Grouper(key='TRADE DATE', freq='YE'))['2ND TRADE MG PROFIT'].sum().reset_index())

        print('\n###################')
        #############

        excel_df.to_excel(DRIVE + "".join(("/", str(UNDERLYING), '_', str(STRIKE_DIFF_PERCENT), str(maximum_beareable_loss), '_IronCondor_Same_Day_2_attempts.xlsx')), index=False)
        # print_statistics(trading_outputs, DRIVE +"/" + UNDERLYING + "_IronCondor.xlsx")
    else:
        print('No results')

    exit(0)


