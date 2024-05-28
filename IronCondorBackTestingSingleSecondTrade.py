import pandas as pd
from datetime import datetime, timedelta
import glob
import traceback


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


def calculate_expiry_day_strangle_pl(under_lying_value_orig_new__, df_part__, trading_date__, expiry_date_str__, max_prem_type__):
    try:
        under_lying_value_expiry_open_ = round(underlying_df.loc[underlying_df['date_obj'] == trading_date__]['Open'].iloc[0] / 100) * 100
        trading_date_obj__ = datetime.strptime(trading_date__, '%d-%b-%Y')

        sell_call_open_price_ = float(df_part__.loc[(df_part__['Strike Price  '] == under_lying_value_expiry_open_) & (df_part__['Option type  '] == 'CE') & (df_part__['Date  '] == trading_date__)]['Open  '].iloc[0])

        sell_put_open_price_ = float(df_part__.loc[(df_part__['Strike Price  '] == under_lying_value_expiry_open_) & (df_part__['Option type  '] == 'PE') & (df_part__['Date  '] == trading_date__)]['Open  '].iloc[0])

        sell_call_exit_price_ = float(df_part__.loc[(df_part__['Strike Price  '] == under_lying_value_orig_new__) & (df_part__['Option type  '] == 'CE') & (df_part__['Date  '] == expiry_date_str__)]['Close  '].iloc[0])

        sell_put_exit_price_ = float(df_part__.loc[(df_part__['Strike Price  '] == under_lying_value_orig_new__) & (df_part__['Option type  '] == 'PE') & (df_part__['Date  '] == expiry_date_str__)]['Close  '].iloc[0])

        if expiry_date_str__ == trading_date__:
            maximum_ce_short_premium__ = max(df_part__.loc[(df_part__['Strike Price  '] == under_lying_value_orig_new__) & (df_part__['Option type  '] == 'CE') & (df_part__['Expiry  '] == expiry_date_str__) & (df_part__['Date  '] == expiry_date_str__)]['High  '].apply(float))
        else:
            maximum_ce_short_premium__ = max(df_part__.loc[(df_part__['Strike Price  '] == under_lying_value_orig_new__) & (df_part__['Option type  '] == 'CE') & (df_part__['Expiry  '] == expiry_date_str__) & (df_part__['date_obj'] > trading_date_obj__)]['High  '].apply(float))

        minimum_ce_short_premium__ = min(df_part__.loc[(df_part__['Strike Price  '] == under_lying_value_orig_new__) & (df_part__['Option type  '] == 'CE') & (df_part__['Expiry  '] == expiry_date_str__) & (df_part__['date_obj'] >= trading_date_obj__)]['Low  '].apply(float))

        if expiry_date_str__ == trading_date__:
            maximum_pe_short_premium__ = max(df_part__.loc[(df_part__['Strike Price  '] == under_lying_value_orig_new__) & (df_part__['Option type  '] == 'PE') & (df_part__['Expiry  '] == expiry_date_str__) & (df_part__['Date  '] == expiry_date_str__)]['High  '].apply(float))
        else:
            maximum_pe_short_premium__ = max(df_part__.loc[(df_part__['Strike Price  '] == under_lying_value_orig_new__) & (df_part__['Option type  '] == 'PE') & (df_part__['Expiry  '] == expiry_date_str__) & (df_part__['date_obj'] > trading_date_obj__)]['High  '].apply(float))

        minimum_pe_short_premium__ = min(df_part__.loc[(df_part__['Strike Price  '] == under_lying_value_orig_new__) & (df_part__['Option type  '] == 'PE') & (df_part__['Expiry  '] == expiry_date_str__) & (df_part__['date_obj'] >= trading_date_obj__)]['Low  '].apply(float))

        if max_prem_type__ == 'CE':
            if (sell_put_open_price_ + sell_call_open_price_ - maximum_ce_short_premium__) < (maximum_beareable_loss / 2):
                p_l_ = (maximum_beareable_loss / 2)
            else:
                # p_l_ = sell_put_open_price_ - sell_put_exit_price_
                p_l_ = sell_call_open_price_ + sell_put_open_price_ - sell_call_exit_price_ - sell_put_exit_price_
        else:
            if (sell_call_open_price_ + sell_put_open_price_ - maximum_pe_short_premium__) < (maximum_beareable_loss / 2):
                p_l_ = (maximum_beareable_loss / 2)
            else:
                # p_l_ = sell_call_open_price_ - sell_call_exit_price_
                p_l_ = sell_call_open_price_ + sell_put_open_price_ - sell_call_exit_price_ - sell_put_exit_price_

        # p_l_ = sell_call_open_price_ + sell_put_open_price_ - sell_call_exit_price_ - sell_put_exit_price_

        return p_l_, sell_call_open_price_, sell_call_exit_price_, sell_put_open_price_, sell_put_exit_price_, maximum_ce_short_premium__, maximum_pe_short_premium__, minimum_ce_short_premium__, minimum_pe_short_premium__

    except Exception as e:
        print(traceback.format_exc())
        return None, None, None, None, None, None, None, None, None


def find_second_trade_pl(df_, sell_call_strike_, sell_call_entry_price_, sell_put_strike_, sell_put_entry_price_, trading_date_obj_, expiry_date_str_, strike_difference_, under_lying_value_orig_):
    trading_dates_ = df_.loc[(df['Expiry  '] == expiry_date_str_) & (df_['date_obj'] >= trading_date_obj_)]['Date  '].unique()

    df_part_ = df_.loc[(df['Expiry  '] == expiry_date_str_) & (df_['date_obj'] >= trading_date_obj_)]

    for trading_date_ in trading_dates_:

        sell_call_high_price = float(df_part_.loc[(df_part_['Strike Price  '] == sell_call_strike_) & (df_part_['Option type  '] == 'CE') & (df_part_['Date  '] == trading_date_)]['High  '].iloc[0])
        sell_call_low_price = float(df_part_.loc[(df_part_['Strike Price  '] == sell_call_strike_) & (df_part_['Option type  '] == 'CE') & (df_part_['Date  '] == trading_date_)]['Low  '].iloc[0])

        sell_put_high_price = float(df_part_.loc[(df_part_['Strike Price  '] == sell_put_strike_) & (df_part_['Option type  '] == 'PE') & (df_part_['Date  '] == trading_date_)]['High  '].iloc[0])
        sell_put_low_price = float(df_part_.loc[(df_part_['Strike Price  '] == sell_put_strike_) & (df_part_['Option type  '] == 'PE') & (df_part_['Date  '] == trading_date_)]['Low  '].iloc[0])

        if (trading_date_ == '26-Apr-2024'):
            pass

        if (sell_call_entry_price_ + sell_put_entry_price_ - sell_call_high_price - sell_put_low_price) < maximum_beareable_loss:
            under_lying_value_orig_new_ = sell_call_strike_ + (
                        (sell_put_entry_price + sell_call_entry_price - maximum_beareable_loss) * .9)
            under_lying_value_orig_new_ = round(under_lying_value_orig_new_ / 100) * 100
            p_l_f_strangle_, strangle_call_open_price_, strangle_call_exit_price_, strangle_put_open_price_, strangle_put_exit_price_, maximum_ce_short_premium_, maximum_pe_short_premium_, minimum_ce_short_premium_, minimum_pe_short_premium_ = calculate_expiry_day_strangle_pl(
                under_lying_value_orig_new_, df_part_, trading_date_, expiry_date_str_, 'CE')

            if p_l_f_strangle_ is not None:
                p_l_f_strangle_ = (maximum_beareable_loss / 2) if p_l_f_strangle_ < (maximum_beareable_loss / 2) else p_l_f_strangle_

            return None, under_lying_value_orig_new_, strangle_call_open_price_, strangle_call_exit_price_, strangle_put_open_price_, strangle_put_exit_price_, p_l_f_strangle_, p_l_f_strangle_, datetime.strptime(trading_date_, '%d-%b-%Y'), maximum_ce_short_premium_, maximum_pe_short_premium_, minimum_ce_short_premium_, minimum_pe_short_premium_

        if (sell_call_entry_price_ + sell_put_entry_price_ - sell_put_high_price - sell_call_low_price) < maximum_beareable_loss:
            # if trading_date_ == expiry_date_str_ and expiry_date_str_ == '17-Jan-2024':
            under_lying_value_orig_new_ = sell_put_strike_ - (
                        (sell_put_entry_price + sell_call_entry_price - maximum_beareable_loss) * .9)
            under_lying_value_orig_new_ = round(under_lying_value_orig_new_ / 100) * 100
            p_l_f_strangle_, strangle_call_open_price_, strangle_call_exit_price_, strangle_put_open_price_, strangle_put_exit_price_, maximum_ce_short_premium_, maximum_pe_short_premium_, minimum_ce_short_premium_, minimum_pe_short_premium_ = calculate_expiry_day_strangle_pl(
                under_lying_value_orig_new_, df_part_, trading_date_, expiry_date_str_, 'PE')

            if p_l_f_strangle_ is not None:
                p_l_f_strangle_ = (maximum_beareable_loss / 2) if p_l_f_strangle_ < (maximum_beareable_loss / 2) else p_l_f_strangle_
                # if p_l_f_strangle_ != p_l_:
                #     pass

            return None, under_lying_value_orig_new_, strangle_call_open_price_, strangle_call_exit_price_, strangle_put_open_price_, strangle_put_exit_price_, p_l_f_strangle_, p_l_f_strangle_, datetime.strptime(trading_date_, '%d-%b-%Y'), maximum_ce_short_premium_, maximum_pe_short_premium_, minimum_ce_short_premium_, minimum_pe_short_premium_

    return None, None, None, None, None, None, 0, 0, None, None, None, None, None


if __name__ == '__main__':

    ####################################
    DRIVE = 'D:'
    files_pattern = DRIVE + '/BN OLD DATA/*.csv'
    underlying_file = DRIVE + '/NIFTY BANK_Historical_PR_01042017to17052024.csv'
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
                print('No data for trading date: ', trading_date_str, ' and expiry date: ', expiry_date_str)
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

            except Exception as e:
                print(traceback.format_exc())
                continue

            df_part = df.loc[(df['Date  '] == expiry_date_str) & (df['Expiry  '] == expiry_date_str)]
            if df_part.empty:
                print('No data for trading date: ', expiry_date_str, ' and expiry date: ', expiry_date_str)
                continue

            try:

                sell_put_exit_price = float(df_part.loc[(df_part['Strike Price  '] == sell_put_strike) & (df_part['Option type  '] == 'PE')]['Close  '].iloc[0])
                sell_call_exit_price = float(df_part.loc[(df_part['Strike Price  '] == sell_call_strike) & (df_part['Option type  '] == 'CE')]['Close  '].iloc[0])

                p_l = (sell_put_entry_price + sell_call_entry_price) - (sell_put_exit_price + sell_call_exit_price)

                maximum_ce_short_premium = max(df.loc[(df['Strike Price  '] == sell_call_strike) & (df['Option type  '] == 'CE') & (df['Expiry  '] == expiry_date_str) & (df['date_obj'] >= trading_date_obj)]['High  '].apply(float))

                maximum_pe_short_premium = max(df.loc[(df['Strike Price  '] == sell_put_strike) & (df['Option type  '] == 'PE') & (df['Expiry  '] == expiry_date_str) & (df['date_obj'] >= trading_date_obj)]['High  '].apply(float))

                maximum_loss =  (sell_call_entry_price + sell_put_entry_price - maximum_ce_short_premium) if maximum_ce_short_premium >= maximum_pe_short_premium else (sell_call_entry_price + sell_put_entry_price - maximum_pe_short_premium)

            except Exception as e:
                print(traceback.format_exc())
                continue

            maximum_beareable_loss = maximum_beareable_loss_per * under_lying_value

            max_short_premium =  maximum_ce_short_premium if maximum_ce_short_premium >= maximum_pe_short_premium else maximum_pe_short_premium

            maximum_loss = maximum_loss if max_short_premium != 0 else 0

            max_premium_type = 'CE' if maximum_ce_short_premium >= maximum_pe_short_premium else 'PE'

            managed_profit = p_l if maximum_loss > maximum_beareable_loss else maximum_beareable_loss

            if managed_profit < 0:
                unavoidable_loss, under_lying_value_strangle, strangle_call_open_price, strangle_call_exit_price, strangle_put_open_price, strangle_put_exit_price, strangle_p_l, full_strangle_p_l, second_trading_date, second_maximum_ce_short_premium, second_maximum_pe_short_premium, second_minimum_ce_short_premium, second_minimum_pe_short_premium = find_second_trade_pl(df, sell_call_strike, sell_call_entry_price, sell_put_strike, sell_put_entry_price, trading_date_obj, expiry_date_str, strike_difference, under_lying_value)
            else:
                unavoidable_loss, under_lying_value_strangle, strangle_call_open_price, strangle_call_exit_price, strangle_put_open_price, strangle_put_exit_price, second_trading_date, second_maximum_ce_short_premium, second_maximum_pe_short_premium, second_minimum_ce_short_premium, second_minimum_pe_short_premium = [None] * 11
                strangle_p_l = 0
                full_strangle_p_l = 0

            if unavoidable_loss is not None:
                managed_profit = unavoidable_loss
                # managed_profit = p_l if p_l > maximum_beareable_loss else maximum_beareable_loss

            trading_outputs.append([UNDERLYING, under_lying_value, trading_date_obj, expiry_date_obj, sell_put_strike, sell_put_entry_price, sell_put_exit_price, sell_call_strike, sell_call_entry_price, sell_call_exit_price, p_l, 1 if p_l > 0 else 0, managed_profit, 1 if managed_profit > 0 else 0, maximum_loss, max_short_premium, max_premium_type, under_lying_value_strangle, second_trading_date, strangle_call_open_price, strangle_call_exit_price, strangle_put_open_price, strangle_put_exit_price, strangle_p_l, 1 if (strangle_p_l !=None and strangle_p_l > 0) else 0, strangle_p_l, full_strangle_p_l, 1 if (full_strangle_p_l is not None and full_strangle_p_l > 0) > 0 else 0, full_strangle_p_l, 1 if unavoidable_loss is not None else 0, second_maximum_ce_short_premium, second_maximum_pe_short_premium, second_minimum_ce_short_premium, second_minimum_pe_short_premium])


    if len(trading_outputs) > 0:
        trading_outputs = sorted(trading_outputs, key=lambda x: x[3])
        trading_outputs.insert(0, ['UNDERLYING', 'VALUE', 'TRADE DATE', 'EXPIRY DATE', 'SELL PUT', 'SELL PUT(EN)', 'SELL PUT(EX)', 'SELL CALL', 'SELL CALL(EN)', 'SELL CALL(EX)', 'P/L', 'PROFIT', 'MGD PROFIT', 'MGD P/L', 'MAX LOSS', 'MAX PREM.', 'MAX PREM. TYPE', 'STRANGLE_UNDER', '2ND TRAD DATE', 'STRANGLE SELL CALL(EN)', 'STRANGLE SELL CALL(EX)', 'STRANGLE SELL PUT(EN)', 'STRANGLE SELL PUT(EX)', 'STRANGLE P/L', 'STRANGLE SELL PROFIT', 'STRANGLE P/L MGD', 'FULL STRANGLE P/L', 'FULL STRANGLE SELL PROFIT', 'FULL STRANGLE P/L MGD', 'UNAVOID. LOSS', '2ND MAX CE PRE', '2ND MAX PE PRE', '2ND MIN CE PRE', '2ND MIN PE PRE'])
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

        print("\n MGD PROFIT year wise: \n", excel_df.groupby(pd.Grouper(key='TRADE DATE', freq='YE'))['MGD PROFIT'].sum().reset_index())
        print("MGD PROFIT:", round(excel_df['MGD PROFIT'].sum(), 1))
        print("Accuracy(MGD PROFIT):", round(excel_df['MGD P/L'].sum() / excel_df.shape[0], 3))

        print("\n STRANGLE PROFIT:", excel_df['STRANGLE P/L'].sum())
        print("Accuracy(STRANGLE PROFIT):", round(excel_df['STRANGLE SELL PROFIT'].sum() / len(excel_df[excel_df['MGD P/L'] == 0]), 3))
        print("\n STRANGLE P/L MGD:", excel_df['STRANGLE P/L MGD'].sum())
        print("\n STRANGLE PROFIT MGD year wise: \n", excel_df.groupby(pd.Grouper(key='TRADE DATE', freq='YE'))['STRANGLE P/L MGD'].sum().reset_index())

        print("\n FULL STRANGLE PROFIT:", excel_df['FULL STRANGLE P/L'].sum())
        print("Accuracy(FULL STRANGLE PROFIT):", round(excel_df['FULL STRANGLE SELL PROFIT'].sum() / len(excel_df[excel_df['MGD P/L'] == 0]), 3))
        print("\n FULL STRANGLE P/L MGD:", excel_df['FULL STRANGLE P/L MGD'].sum())
        print("\n FULL STRANGLE PROFIT MGD year wise: \n", excel_df.groupby(pd.Grouper(key='TRADE DATE', freq='YE'))['FULL STRANGLE P/L MGD'].sum().reset_index())

        print("\n Total Unavoidable losses:", excel_df['UNAVOID. LOSS'].sum())
        print('\n###################')
        #############

        excel_df.to_excel(DRIVE + "".join(("/", str(UNDERLYING), "_", str(STRIKE_DIFF_PERCENT), "_", str(NO_DAYS_TO_EXPIRY), "_IronCondor.xlsx")), index=False)
        # print_statistics(trading_outputs, DRIVE +"/" + UNDERLYING + "_IronCondor.xlsx")
    else:
        print('No results')

    exit(0)


