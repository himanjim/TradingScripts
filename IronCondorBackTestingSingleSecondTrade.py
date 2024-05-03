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


def calculate_expiry_day_strangle_pl(under_lying_value_orig_new__, df_part__, expiry_date_str__):
    try:
        under_lying_value_expiry_open_ = round(underlying_df.loc[underlying_df['date_obj'] == expiry_date_str__]['Open'].iloc[0] / 100) * 100

        sell_call_open_price_ = float(df_part__.loc[(df_part__['Strike Price  '] == under_lying_value_expiry_open_) & (df_part__['Option type  '] == 'CE') & (df_part__['Date  '] == expiry_date_str__)]['Open  '].iloc[0])

        sell_put_open_price_ = float(df_part__.loc[(df_part__['Strike Price  '] == under_lying_value_expiry_open_) & (df_part__['Option type  '] == 'PE') & (df_part__['Date  '] == expiry_date_str__)]['Open  '].iloc[0])

        sell_call_exit_price_ = float(df_part__.loc[(df_part__['Strike Price  '] == under_lying_value_orig_new__) & (df_part__['Option type  '] == 'CE') & (df_part__['Date  '] == expiry_date_str__)]['Close  '].iloc[0])

        sell_put_exit_price_ = float(df_part__.loc[(df_part__['Strike Price  '] == under_lying_value_orig_new__) & (df_part__['Option type  '] == 'PE') & (df_part__['Date  '] == expiry_date_str__)]['Close  '].iloc[0])

        p_l_ = sell_call_open_price_ + sell_put_open_price_ - sell_call_exit_price_ - sell_put_exit_price_

        return p_l_, sell_call_open_price_, sell_call_exit_price_, sell_put_open_price_, sell_put_exit_price_

    except Exception as e:
        print(traceback.format_exc())
        return None


def second_trade_pl_calculations(under_lying_value_, df_part__, trading_date__, expiry_date_str__, strike_difference__):

    # try:
    maximum_ce_short_premium__ = maximum_pe_short_premium__ = max_premium_type__ = None
    trading_date_obj__ = datetime.strptime(trading_date__, '%d-%b-%Y')

    if trading_date__ != expiry_date_str__:
        next_trading_date_obj__ = trading_date_obj__ + timedelta(days=0)
        while next_trading_date_obj__.strftime('%d-%b-%Y') not in all_dates:
            next_trading_date_obj__ = next_trading_date_obj__ + timedelta(days=1)

        next_trading_date_ = next_trading_date_obj__.strftime('%d-%b-%Y')
    else:
        next_trading_date_ = trading_date__

    open_or_close_ = 'Close  '

    sell_call_open_price_f_strangle_ = float(df_part__.loc[(df_part__['Strike Price  '] == under_lying_value_) & (df_part__['Option type  '] == 'CE') & (df_part__['Date  '] == next_trading_date_)][open_or_close_].iloc[0])
    sell_put_open_price_f_strangle_ = float(df_part__.loc[(df_part__['Strike Price  '] == under_lying_value_) & (df_part__['Option type  '] == 'PE') & (df_part__['Date  '] == next_trading_date_)][open_or_close_].iloc[0])

    ##### High calculations
    if trading_date__ != expiry_date_str__:
        maximum_ce_short_premium__ = max(df_part__.loc[(df_part__['Strike Price  '] == under_lying_value_) & (df_part__['Option type  '] == 'CE') & (df_part__['Expiry  '] == expiry_date_str__) & (df_part__['date_obj'] > trading_date_obj__)]['High  '].apply(float))

        maximum_pe_short_premium__ = max(df_part__.loc[(df_part__['Strike Price  '] == under_lying_value_) & (df_part__['Option type  '] == 'PE') & (df_part__['Expiry  '] == expiry_date_str__) & (df_part__['date_obj'] > trading_date_obj__)]['High  '].apply(float))

        maximum_loss__ = (
                sell_call_open_price_f_strangle_ + sell_put_open_price_f_strangle_ - maximum_ce_short_premium__) if maximum_ce_short_premium__ >= maximum_pe_short_premium__ else (
                    sell_call_open_price_f_strangle_ + sell_put_open_price_f_strangle_ - maximum_pe_short_premium__)

        max_premium_type__= 'CE'if maximum_ce_short_premium__ > maximum_pe_short_premium__ else 'PE'
    #####################

    sell_call_exit_price_f_strangle_ = float(df_part__.loc[(df_part__['Strike Price  '] == under_lying_value_) & (df_part__['Option type  '] == 'CE') & (df_part__['Date  '] == expiry_date_str__)]['Close  '].iloc[
                                                 0])
    sell_put_exit_price_f_strangle_ = float(df_part__.loc[(df_part__['Strike Price  '] == under_lying_value_) & (df_part__['Option type  '] == 'PE') & (df_part__['Date  '] == expiry_date_str__)]['Close  '].iloc[
                                                0])

    if trading_date__ != expiry_date_str__ and maximum_loss__ < maximum_beareable_loss:
        p_l_f_strangle__ = maximum_loss__
    else:
        p_l_f_strangle__ = sell_call_open_price_f_strangle_ + sell_put_open_price_f_strangle_ - sell_call_exit_price_f_strangle_ - sell_put_exit_price_f_strangle_

        p_l_f_strangle__ = round(p_l_f_strangle__, 0)

    # return p_l_f_strangle__, p_l__, sell_call_close_price_, sell_call_exit_price_, sell_put_close_price_, sell_put_exit_price_
    return p_l_f_strangle__, sell_call_open_price_f_strangle_, sell_call_exit_price_f_strangle_, sell_put_open_price_f_strangle_, sell_put_exit_price_f_strangle_, maximum_ce_short_premium__, maximum_pe_short_premium__, max_premium_type__

    # except Exception as e:
    #     print(traceback.format_exc())
    #     return 0, 0, None, None, None, None


def find_second_trade_pl(df_, sell_call_strike_, sell_call_entry_price_, sell_put_strike_, sell_put_entry_price_, trading_date_obj_, expiry_date_str_, strike_difference_, under_lying_value_orig_):
    trading_dates_ = df_.loc[(df['Expiry  '] == expiry_date_str_) & (df_['date_obj'] >= trading_date_obj_)]['Date  '].unique()

    df_part_ = df_.loc[(df['Expiry  '] == expiry_date_str_) & (df_['date_obj'] >= trading_date_obj_)]

    for trading_date_ in trading_dates_:
        sell_call_open_price = float(df_part_.loc[(df_part_['Strike Price  '] == sell_call_strike_) & (df_part_['Option type  '] == 'CE') & (df_part_['Date  '] == trading_date_)]['Open  '].iloc[0])
        sell_put_open_price = float(df_part_.loc[(df_part_['Strike Price  '] == sell_put_strike_) & (df_part_['Option type  '] == 'PE') & (df_part_['Date  '] == trading_date_)]['Open  '].iloc[0])

        under_lying_value_open = round(underlying_df.loc[underlying_df['date_obj'] == trading_date_]['Open'].iloc[0]/100) * 100

        if (sell_call_entry_price_ + sell_put_entry_price_ - sell_call_open_price - sell_put_open_price) < maximum_beareable_loss:
            unavoidable_loss = sell_call_entry_price_ + sell_put_entry_price_ - sell_call_open_price - sell_put_open_price
            p_l_f_strangle_, strangle_call_open_price_, strangle_call_exit_price_, strangle_put_open_price_, strangle_put_exit_price_, maximum_ce_short_premium_, maximum_pe_short_premium_, max_premium_type_ = second_trade_pl_calculations(under_lying_value_open, df_part_, trading_date_, expiry_date_str_, strike_difference_)

            return unavoidable_loss, under_lying_value_open, strangle_call_open_price_, strangle_call_exit_price_, strangle_put_open_price_, strangle_put_exit_price_, p_l_f_strangle_, datetime.strptime(trading_date_, '%d-%b-%Y'), maximum_ce_short_premium_, maximum_pe_short_premium_, max_premium_type_

        sell_call_high_price = float(df_part_.loc[(df_part_['Strike Price  '] == sell_call_strike_) & (df_part_['Option type  '] == 'CE') & (df_part_['Date  '] == trading_date_)]['High  '].iloc[0])
        sell_call_low_price = float(df_part_.loc[(df_part_['Strike Price  '] == sell_call_strike_) & (df_part_['Option type  '] == 'CE') & (df_part_['Date  '] == trading_date_)]['Low  '].iloc[0])

        sell_put_high_price = float(df_part_.loc[(df_part_['Strike Price  '] == sell_put_strike_) & (df_part_['Option type  '] == 'PE') & (df_part_['Date  '] == trading_date_)]['High  '].iloc[0])
        sell_put_low_price = float(df_part_.loc[(df_part_['Strike Price  '] == sell_put_strike_) & (df_part_['Option type  '] == 'PE') & (df_part_['Date  '] == trading_date_)]['Low  '].iloc[0])

        under_lying_value_close = round(
            underlying_df.loc[underlying_df['date_obj'] == trading_date_]['Close'].iloc[0] / 100) * 100

        if (sell_call_entry_price_ + sell_put_entry_price_ - sell_call_high_price - sell_put_low_price) < maximum_beareable_loss:

            if trading_date_ == expiry_date_str_:
                under_lying_value_orig_new_= sell_call_strike + ((sell_put_entry_price + sell_call_entry_price - maximum_beareable_loss) * .9)
                under_lying_value_orig_new_ = round(under_lying_value_orig_new_ / 100) * 100
                p_l_f_strangle_, strangle_call_open_price_, strangle_call_exit_price_, strangle_put_open_price_, strangle_put_exit_price_ =  calculate_expiry_day_strangle_pl(under_lying_value_orig_new_, df_part_, expiry_date_str_)
                p_l_f_strangle_ = maximum_beareable_loss if p_l_f_strangle_ < maximum_beareable_loss else p_l_f_strangle_
                under_lying_value_close = under_lying_value_orig_new_
                maximum_ce_short_premium_ = maximum_pe_short_premium_ = max_premium_type_ = None

                # if p_l_f_strangle_ != p_l_:
                #     pass
            else:
                p_l_f_strangle_, strangle_call_open_price_, strangle_call_exit_price_, strangle_put_open_price_, strangle_put_exit_price_, maximum_ce_short_premium_, maximum_pe_short_premium_, max_premium_type_ = second_trade_pl_calculations(
                    under_lying_value_close, df_part_, trading_date_, expiry_date_str_, strike_difference_)

            return None, under_lying_value_close, strangle_call_open_price_, strangle_call_exit_price_, strangle_put_open_price_, strangle_put_exit_price_, p_l_f_strangle_, datetime.strptime(trading_date_, '%d-%b-%Y'), maximum_ce_short_premium_, maximum_pe_short_premium_, max_premium_type_

        if (sell_call_entry_price_ + sell_put_entry_price_ - sell_put_high_price - sell_call_low_price) < maximum_beareable_loss:
            # if trading_date_ == expiry_date_str_ and expiry_date_str_ == '17-Jan-2024':
            if trading_date_ == expiry_date_str_:
                under_lying_value_orig_new_= sell_put_strike_ - ((sell_put_entry_price + sell_call_entry_price - maximum_beareable_loss) * .9)
                under_lying_value_orig_new_ = round(under_lying_value_orig_new_ / 100) * 100
                p_l_f_strangle_, strangle_call_open_price_, strangle_call_exit_price_, strangle_put_open_price_, strangle_put_exit_price_ =  calculate_expiry_day_strangle_pl(under_lying_value_orig_new_, df_part_, expiry_date_str_)

                p_l_f_strangle_ = maximum_beareable_loss if p_l_f_strangle_ < maximum_beareable_loss else p_l_f_strangle_
                under_lying_value_close = under_lying_value_orig_new_
                maximum_ce_short_premium_ = maximum_pe_short_premium_ = max_premium_type_ = None
            else:
                p_l_f_strangle_, strangle_call_open_price_, strangle_call_exit_price_, strangle_put_open_price_, strangle_put_exit_price_, maximum_ce_short_premium_, maximum_pe_short_premium_, max_premium_type_ = second_trade_pl_calculations(
                    under_lying_value_close, df_part_, trading_date_, expiry_date_str_, strike_difference_)

                # if p_l_f_strangle_ != p_l_:
                #     pass

            return None, under_lying_value_close, strangle_call_open_price_, strangle_call_exit_price_, strangle_put_open_price_, strangle_put_exit_price_, p_l_f_strangle_, datetime.strptime(trading_date_, '%d-%b-%Y'), maximum_ce_short_premium_, maximum_pe_short_premium_, max_premium_type_

    return None, None, None, None, None, None, 0, 0, None, None, None


if __name__ == '__main__':

    ####################################
    DRIVE = 'D:'
    files_pattern = DRIVE + '/BN OLD DATA/*.csv'
    underlying_file = DRIVE + '/NIFTY BANK_Historical_PR_01042017to30042024.csv'
    UNDERLYING = 'BN'
    STRIKE_DIFF_PERCENT = 0.00
    NO_DAYS_TO_EXPIRY = 2
    maximum_beareable_loss = -200
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

            max_short_premium =  maximum_ce_short_premium if maximum_ce_short_premium >= maximum_pe_short_premium else maximum_pe_short_premium

            maximum_loss = maximum_loss if max_short_premium != 0 else 0

            max_premium_type = 'CE' if maximum_ce_short_premium >= maximum_pe_short_premium else 'PE'

            managed_profit = p_l if maximum_loss > maximum_beareable_loss else maximum_beareable_loss

            if managed_profit < 0:
                unavoidable_loss, under_lying_value_strangle, strangle_call_open_price, strangle_call_exit_price, strangle_put_open_price, strangle_put_exit_price, full_strangle_p_l, second_trading_date,  maximum_2nd_ce_short_premium, maximum_2nd_pe_short_premium, max_2nd_premium_type = find_second_trade_pl(df, sell_call_strike, sell_call_entry_price, sell_put_strike, sell_put_entry_price, trading_date_obj, expiry_date_str, strike_difference, under_lying_value)
            else:
                unavoidable_loss, under_lying_value_strangle, strangle_call_open_price, strangle_call_exit_price, strangle_put_open_price, strangle_put_exit_price, second_trading_date, maximum_2nd_ce_short_premium, maximum_2nd_pe_short_premium, max_2nd_premium_type = [None] * 10
                strangle_p_l = 0
                full_strangle_p_l = 0

            if unavoidable_loss is not None:
                managed_profit = unavoidable_loss
                # managed_profit = p_l if p_l > maximum_beareable_loss else maximum_beareable_loss

            trading_outputs.append([UNDERLYING, under_lying_value, trading_date_obj, expiry_date_obj, sell_put_strike, sell_put_entry_price, sell_put_exit_price, sell_call_strike, sell_call_entry_price, sell_call_exit_price, p_l, 1 if p_l > 0 else 0, managed_profit, 1 if managed_profit > 0 else 0, maximum_loss, max_short_premium, max_premium_type, under_lying_value_strangle, second_trading_date, strangle_call_open_price, strangle_call_exit_price, strangle_put_open_price, strangle_put_exit_price, full_strangle_p_l, 1 if full_strangle_p_l > 0 else 0, maximum_beareable_loss if full_strangle_p_l < maximum_beareable_loss else full_strangle_p_l,1 if unavoidable_loss is not None else 0, maximum_2nd_ce_short_premium, maximum_2nd_pe_short_premium, max_2nd_premium_type])

    if len(trading_outputs) > 0:
        trading_outputs = sorted(trading_outputs, key=lambda x: x[3])
        trading_outputs.insert(0, ['UNDERLYING', 'VALUE', 'TRADE DATE', 'EXPIRY DATE', 'SELL PUT', 'SELL PUT(EN)', 'SELL PUT(EX)', 'SELL CALL', 'SELL CALL(EN)', 'SELL CALL(EX)', 'P/L', 'PROFIT', 'MGD PROFIT', 'MGD P/L', 'MAX LOSS', 'MAX PREM.', 'MAX PREM. TYPE', 'STRANGLE_UNDER', '2ND TRAD DATE', 'STRANGLE SELL CALL(EN)', 'STRANGLE SELL CALL(EX)', 'STRANGLE SELL PUT(EN)', 'STRANGLE SELL PUT(EX)', 'FULL STRANGLE P/L', 'FULL STRANGLE SELL PROFIT', 'FULL STRANGLE P/L MGD', 'UNAVOID. LOSS', 'MAX 2ND CE PREM', 'MAX 2ND PE PREM', 'MAX 2ND PREM TYPE'])
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


