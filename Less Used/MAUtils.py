import Indicators as ind
import MovingAverageStrategies as ma
import ScrapUtils as nse_bse
import Utils as util


def is_up_slopes_of_mas(*mas, pos):
    for maseries in mas:
        if ma.calculate_slope (maseries.iloc[:pos]).value == ma.Slope.DOWN.value:
            return False

    return True

def test_ma_strategies(stock_latest_data, stock_latest_info, moving_average_strategy_responses, exception_errors, ignore_min_session_len = False):
    stock_latest_data_len = len (stock_latest_data)
    # print ("---Testing various moving average strategies for stock:" + stock_latest_info[nse_bse.STOCK_ID])

    # if ignore_min_session_len or stock_latest_data_len > ma.MA_Strategy_Name._13_21_34_DAY_EMA.max_session_size:
    #     _13_21_34_DAY_EMA_res = ma._13_21_34_EMA_Abhijeet (stock_latest_data)
    #     _13_21_34_DAY_EMA_res.stock_id = stock_latest_info[nse_bse.STOCK_ID]
    #     _13_21_34_DAY_EMA_res.fetched_dataset = stock_latest_data
    #     moving_average_strategy_responses.append (_13_21_34_DAY_EMA_res)
    # else:
    #     exception_errors.append (
    #         "Stock data session size:" + str (stock_latest_data_len) + " less than max session size: " + str (
    #             ma.MA_Strategy_Name._13_21_34_DAY_EMA.max_session_size) + " required for strategy:" + str (
    #             ma.MA_Strategy_Name._13_21_34_DAY_EMA))
    #
    # if ignore_min_session_len or stock_latest_data_len > ma.MA_Strategy_Name.IDENTIFY_LONG_TERM_STOCK_BEFORE_RALLY.max_session_size:
    #     identify_long_term_stock_before_rally_res = ma.identify_long_term_stock_before_rally (stock_latest_data)
    #     identify_long_term_stock_before_rally_res.stock_id = stock_latest_info[nse_bse.STOCK_ID]
    #     identify_long_term_stock_before_rally_res.fetched_dataset = stock_latest_data
    #     moving_average_strategy_responses.append (identify_long_term_stock_before_rally_res)
    # else:
    #     exception_errors.append (
    #         "Stock data session size:" + str (stock_latest_data_len) + " less than max session size: " + str (
    #             ma.MA_Strategy_Name.IDENTIFY_LONG_TERM_STOCK_BEFORE_RALLY.max_session_size) + " required for strategy:" + str (
    #             ma.MA_Strategy_Name.IDENTIFY_LONG_TERM_STOCK_BEFORE_RALLY))
    #
    # if ignore_min_session_len or stock_latest_data_len > ma.MA_Strategy_Name.GUPPY_MULTIPLE_MOVING_AVERAGE_INDICATOR.max_session_size:
    #     guppy_multiple_moving_average_res = ma.guppy_multiple_moving_average_indicator (stock_latest_data)
    #     guppy_multiple_moving_average_res.stock_id = stock_latest_info[nse_bse.STOCK_ID]
    #     guppy_multiple_moving_average_res.fetched_dataset = stock_latest_data
    #     moving_average_strategy_responses.append (guppy_multiple_moving_average_res)
    # else:
    #     exception_errors.append (
    #         "Stock data session size:" + str (stock_latest_data_len) + " less than max session size: " + str (
    #             ma.MA_Strategy_Name.GUPPY_MULTIPLE_MOVING_AVERAGE_INDICATOR.max_session_size) + " required for strategy:" + str (
    #             ma.MA_Strategy_Name.GUPPY_MULTIPLE_MOVING_AVERAGE_INDICATOR))

    # if ignore_min_session_len or stock_latest_data_len > ma.MA_Strategy_Name._100_DAY_EMA_WITH_200_DAY_EMA.max_session_size:
    #     _100_day_EMA_with_200_Day_EMA_res = ma._100_day_EMA_with_200_Day_EMA (stock_latest_data)
    #     _100_day_EMA_with_200_Day_EMA_res.stock_id = stock_latest_info[nse_bse.STOCK_ID]
    #     _100_day_EMA_with_200_Day_EMA_res.fetched_dataset = stock_latest_data
    #     moving_average_strategy_responses.append (_100_day_EMA_with_200_Day_EMA_res)
    # else:
    #     exception_errors.append (
    #         "Stock data session size:" + str (stock_latest_data_len) + " less than max session size: " + str (
    #             ma.MA_Strategy_Name._100_DAY_EMA_WITH_200_DAY_EMA.max_session_size) + " required for strategy:" + str (
    #             ma.MA_Strategy_Name._100_DAY_EMA_WITH_200_DAY_EMA))

    if ignore_min_session_len or stock_latest_data_len > ma.MA_Strategy_Name._50_DAY_EMA_WITH_100_DAY_EMA.max_session_size:
        _50_day_EMA_with_100_Day_EMA_res = ma._50_day_EMA_with_100_Day_EMA (stock_latest_data)
        _50_day_EMA_with_100_Day_EMA_res.stock_id = stock_latest_info[nse_bse.STOCK_ID]
        _50_day_EMA_with_100_Day_EMA_res.fetched_dataset = stock_latest_data
        moving_average_strategy_responses.append (_50_day_EMA_with_100_Day_EMA_res)
    else:
        exception_errors.append (
            "Stock data session size:" + str (stock_latest_data_len) + " less than max session size: " + str (
                ma.MA_Strategy_Name._50_DAY_EMA_WITH_100_DAY_EMA.max_session_size) + " required for strategy:" + str (
                ma.MA_Strategy_Name._50_DAY_EMA_WITH_100_DAY_EMA))

    # if ignore_min_session_len or stock_latest_data_len > ma.MA_Strategy_Name.MOVING_AVERAGE_RIBBON.max_session_size:
    #     moving_average_ribbon_res = ma.moving_average_ribbon (stock_latest_data)
    #     moving_average_ribbon_res.stock_id = stock_latest_info[nse_bse.STOCK_ID]
    #     moving_average_ribbon_res.fetched_dataset = stock_latest_data
    #     moving_average_strategy_responses.append (moving_average_ribbon_res)
    # else:
    #     exception_errors.append (
    #         "Stock data session size:" + str (stock_latest_data_len) + " less than max session size: " + str (
    #             ma.MA_Strategy_Name.MOVING_AVERAGE_RIBBON.max_session_size) + " required for strategy:" + str (
    #             ma.MA_Strategy_Name.MOVING_AVERAGE_RIBBON))

    # if (stock_latest_data_len > ma.MA_Strategy_Name._50_WEEK_SMA_20_DAY_RSI.max_session_size):
    #     _50_week_SMA_20_day_RSI_res = ma._50_week_SMA_20_day_RSI (stock_latest_data)
    #     _50_week_SMA_20_day_RSI_res.stock_id = stock_latest_info[nse_bse.STOCK_ID]
    #     _50_week_SMA_20_day_RSI_res.fetched_dataset = stock_latest_data
    #     moving_average_strategy_responses.append (_50_week_SMA_20_day_RSI_res)
    # else:
    #     exception_errors.append (
    #         "Stock data session size:" + str (stock_latest_data_len) + " less than max session size: " + str (
    #             ma.MA_Strategy_Name._50_WEEK_SMA_20_DAY_RSI.max_session_size) + " required for strategy:" + str (
    #             ma.MA_Strategy_Name._50_WEEK_SMA_20_DAY_RSI))

def re_test_ma_strategy(stock_latest_data, mares, no_of_sessions_back_to_start_i, ignore_min_session_len = False):
    stock_latest_data_effective_len = len (stock_latest_data)

    sold_mas = None

    if (ignore_min_session_len or stock_latest_data_effective_len > ma.MA_Strategy_Name._13_21_34_DAY_EMA.max_session_size) and mares.ma_strategy_name.value==ma.MA_Strategy_Name._13_21_34_DAY_EMA.value:
        stock_data_closing_prices_series = util.get_panda_series_of_stock_closing_prices (stock_latest_data)
        _13_day_EMA_series = ind.ema (stock_data_closing_prices_series, 13)
        _21_day_EMA_series = ind.ema (stock_data_closing_prices_series, 21)
        _34_day_EMA_series = ind.ema (stock_data_closing_prices_series, 34)

        while _13_day_EMA_series.iloc[no_of_sessions_back_to_start_i] > _21_day_EMA_series.iloc[
            no_of_sessions_back_to_start_i] and _21_day_EMA_series.iloc[no_of_sessions_back_to_start_i] > \
                _34_day_EMA_series.iloc[no_of_sessions_back_to_start_i] and is_up_slopes_of_mas (_13_day_EMA_series,
                                                                                                 _21_day_EMA_series,
                                                                                                 _34_day_EMA_series,
                                                                                                 pos=no_of_sessions_back_to_start_i):
            no_of_sessions_back_to_start_i += 1
            if no_of_sessions_back_to_start_i >= stock_latest_data_effective_len:
                break

        if no_of_sessions_back_to_start_i < stock_latest_data_effective_len:
            sold_mas = [ _13_day_EMA_series.iloc[no_of_sessions_back_to_start_i], _21_day_EMA_series.iloc[no_of_sessions_back_to_start_i], _34_day_EMA_series.iloc[no_of_sessions_back_to_start_i]]

    elif (ignore_min_session_len or stock_latest_data_effective_len > ma.MA_Strategy_Name.IDENTIFY_LONG_TERM_STOCK_BEFORE_RALLY.max_session_size) and mares.ma_strategy_name.value==ma.MA_Strategy_Name.IDENTIFY_LONG_TERM_STOCK_BEFORE_RALLY.value:
        stock_data_closing_prices_series = util.get_panda_series_of_stock_closing_prices (stock_latest_data)
        _50_day_SMA_series = ind.sma (stock_data_closing_prices_series, 50)
        _150_day_SMA_series = ind.sma (stock_data_closing_prices_series, 150)
        _200_day_SMA_series = ind.sma (stock_data_closing_prices_series, 200)

        while _50_day_SMA_series.iloc[no_of_sessions_back_to_start_i] > _150_day_SMA_series.iloc[
            no_of_sessions_back_to_start_i] and _150_day_SMA_series.iloc[no_of_sessions_back_to_start_i] > \
                _200_day_SMA_series.iloc[no_of_sessions_back_to_start_i] and is_up_slopes_of_mas (_50_day_SMA_series,
                                                                                                  _150_day_SMA_series,
                                                                                                  pos=no_of_sessions_back_to_start_i):
            no_of_sessions_back_to_start_i += 1
            if no_of_sessions_back_to_start_i >= stock_latest_data_effective_len:
                break

        if no_of_sessions_back_to_start_i < stock_latest_data_effective_len:
            sold_mas = [_50_day_SMA_series.iloc[no_of_sessions_back_to_start_i],_150_day_SMA_series.iloc[no_of_sessions_back_to_start_i],_200_day_SMA_series.iloc[no_of_sessions_back_to_start_i]]

    elif (ignore_min_session_len or stock_latest_data_effective_len > ma.MA_Strategy_Name.GUPPY_MULTIPLE_MOVING_AVERAGE_INDICATOR.max_session_size) and mares.ma_strategy_name.value==ma.MA_Strategy_Name.GUPPY_MULTIPLE_MOVING_AVERAGE_INDICATOR.value:
        stock_data_closing_prices_series = util.get_panda_series_of_stock_closing_prices (stock_latest_data)
        _3_day_EMA_series = ind.ema (stock_data_closing_prices_series, 3)
        _5_day_EMA_series = ind.ema (stock_data_closing_prices_series, 5)
        _8_day_EMA_series = ind.ema (stock_data_closing_prices_series, 8)
        _10_day_EMA_series = ind.ema (stock_data_closing_prices_series, 10)
        _12_day_EMA_series = ind.ema (stock_data_closing_prices_series, 12)
        _18_day_EMA_series = ind.ema (stock_data_closing_prices_series, 18)

        _30_day_EMA_series = ind.ema (stock_data_closing_prices_series, 30)
        _35_day_EMA_series = ind.ema (stock_data_closing_prices_series, 35)
        _40_day_EMA_series = ind.ema (stock_data_closing_prices_series, 40)
        _45_day_EMA_series = ind.ema (stock_data_closing_prices_series, 45)
        _50_day_EMA_series = ind.ema (stock_data_closing_prices_series, 50)
        _60_day_EMA_series = ind.ema (stock_data_closing_prices_series, 60)

        while _3_day_EMA_series.iloc[no_of_sessions_back_to_start_i] > _5_day_EMA_series.iloc[
            no_of_sessions_back_to_start_i] and _5_day_EMA_series.iloc[no_of_sessions_back_to_start_i] > \
                _8_day_EMA_series.iloc[no_of_sessions_back_to_start_i] and _8_day_EMA_series.iloc[
            no_of_sessions_back_to_start_i] > _10_day_EMA_series.iloc[no_of_sessions_back_to_start_i] and \
                _10_day_EMA_series.iloc[no_of_sessions_back_to_start_i] > _12_day_EMA_series.iloc[
            no_of_sessions_back_to_start_i] and _12_day_EMA_series.iloc[no_of_sessions_back_to_start_i] > \
                _18_day_EMA_series.iloc[no_of_sessions_back_to_start_i] and _18_day_EMA_series.iloc[
            no_of_sessions_back_to_start_i] > _30_day_EMA_series.iloc[no_of_sessions_back_to_start_i] and \
                _30_day_EMA_series.iloc[no_of_sessions_back_to_start_i] > _35_day_EMA_series.iloc[
            no_of_sessions_back_to_start_i] and _35_day_EMA_series.iloc[no_of_sessions_back_to_start_i] > \
                _40_day_EMA_series.iloc[no_of_sessions_back_to_start_i] and _40_day_EMA_series.iloc[
            no_of_sessions_back_to_start_i] > _45_day_EMA_series.iloc[no_of_sessions_back_to_start_i] and \
                _45_day_EMA_series.iloc[no_of_sessions_back_to_start_i] > _50_day_EMA_series.iloc[
            no_of_sessions_back_to_start_i] and _50_day_EMA_series.iloc[no_of_sessions_back_to_start_i] > \
                _60_day_EMA_series.iloc[no_of_sessions_back_to_start_i] and is_up_slopes_of_mas (_3_day_EMA_series,
                                                                                                 _5_day_EMA_series,
                                                                                                 _8_day_EMA_series,
                                                                                                 _10_day_EMA_series,
                                                                                                 _12_day_EMA_series,
                                                                                                 _18_day_EMA_series,
                                                                                                 _30_day_EMA_series,
                                                                                                 _35_day_EMA_series,
                                                                                                 _40_day_EMA_series,
                                                                                                 _45_day_EMA_series,
                                                                                                 _50_day_EMA_series,
                                                                                                 _60_day_EMA_series,
                                                                                                 pos=no_of_sessions_back_to_start_i):
            no_of_sessions_back_to_start_i +=1
            if no_of_sessions_back_to_start_i >= stock_latest_data_effective_len:
                break

        if no_of_sessions_back_to_start_i < stock_latest_data_effective_len:
            sold_mas = [_3_day_EMA_series.iloc[no_of_sessions_back_to_start_i],_5_day_EMA_series.iloc[no_of_sessions_back_to_start_i],_8_day_EMA_series.iloc[no_of_sessions_back_to_start_i],_10_day_EMA_series.iloc[no_of_sessions_back_to_start_i],_12_day_EMA_series.iloc[no_of_sessions_back_to_start_i],                   _18_day_EMA_series.iloc[no_of_sessions_back_to_start_i]]

    elif (ignore_min_session_len or stock_latest_data_effective_len > ma.MA_Strategy_Name._100_DAY_EMA_WITH_200_DAY_EMA.max_session_size) and mares.ma_strategy_name.value == ma.MA_Strategy_Name._100_DAY_EMA_WITH_200_DAY_EMA.value:
        stock_data_closing_prices_series = util.get_panda_series_of_stock_closing_prices (stock_latest_data)
        _100_day_EMA_series = ind.ema (stock_data_closing_prices_series, 100)
        _200_day_EMA_series = ind.ema (stock_data_closing_prices_series, 200)

        while _100_day_EMA_series.iloc[no_of_sessions_back_to_start_i] > _200_day_EMA_series.iloc[
            no_of_sessions_back_to_start_i] and is_up_slopes_of_mas (_100_day_EMA_series, _200_day_EMA_series,
                                                                     pos=no_of_sessions_back_to_start_i):
            no_of_sessions_back_to_start_i += 1
            if no_of_sessions_back_to_start_i >= stock_latest_data_effective_len:
                break

        if no_of_sessions_back_to_start_i < stock_latest_data_effective_len:
            sold_mas = [_100_day_EMA_series.iloc[no_of_sessions_back_to_start_i],_200_day_EMA_series.iloc[no_of_sessions_back_to_start_i]]

    elif (ignore_min_session_len or stock_latest_data_effective_len > ma.MA_Strategy_Name._50_DAY_EMA_WITH_100_DAY_EMA.max_session_size) and mares.ma_strategy_name.value == ma.MA_Strategy_Name._50_DAY_EMA_WITH_100_DAY_EMA.value:
        stock_data_closing_prices_series = util.get_panda_series_of_stock_closing_prices (stock_latest_data)
        # _50_day_EMA_series = ind.ema (stock_data_closing_prices_series, 50)
        # _100_day_EMA_series = ind.ema (stock_data_closing_prices_series, 100)
        macd_series = ind.macd (stock_data_closing_prices_series)

        # while _50_day_EMA_series.iloc[no_of_sessions_back_to_start_i] > _100_day_EMA_series.iloc[
        #     no_of_sessions_back_to_start_i] and is_up_slopes_of_mas (_50_day_EMA_series, _100_day_EMA_series,
        #                                                              pos=no_of_sessions_back_to_start_i):
        while is_up_slopes_of_mas (macd_series, pos=no_of_sessions_back_to_start_i):
            no_of_sessions_back_to_start_i += 1
            if no_of_sessions_back_to_start_i >= stock_latest_data_effective_len:
                break

        if no_of_sessions_back_to_start_i < stock_latest_data_effective_len:
            # sold_mas = [_50_day_EMA_series.iloc[no_of_sessions_back_to_start_i], _100_day_EMA_series.iloc[no_of_sessions_back_to_start_i]]
            sold_mas = []

    elif (ignore_min_session_len or stock_latest_data_effective_len > ma.MA_Strategy_Name.MOVING_AVERAGE_RIBBON.max_session_size) and mares.ma_strategy_name.value == ma.MA_Strategy_Name.MOVING_AVERAGE_RIBBON.value:
        stock_data_closing_prices_series = util.get_panda_series_of_stock_closing_prices (stock_latest_data, 'close')
        _34_day_EMA_close_series = ind.ema (stock_data_closing_prices_series, 34)
        stock_data_high_prices_series = util.get_panda_series_of_stock_closing_prices (stock_latest_data, 'high')
        _34_day_EMA_high_series = ind.ema (stock_data_high_prices_series, 34)
        stock_data_low_prices_series = util.get_panda_series_of_stock_closing_prices (stock_latest_data, 'low')
        _34_day_EMA_low_series = ind.ema (stock_data_low_prices_series, 34)

        while stock_latest_data[no_of_sessions_back_to_start_i]['close'] > _34_day_EMA_close_series.iloc[
            no_of_sessions_back_to_start_i] and stock_latest_data[no_of_sessions_back_to_start_i]['close'] > \
                _34_day_EMA_high_series.iloc[no_of_sessions_back_to_start_i] and \
                stock_latest_data[no_of_sessions_back_to_start_i]['close'] > _34_day_EMA_low_series.iloc[
            no_of_sessions_back_to_start_i] and is_up_slopes_of_mas (_34_day_EMA_close_series, _34_day_EMA_high_series,
                                                                     _34_day_EMA_low_series,
                                                                     pos=no_of_sessions_back_to_start_i):
            no_of_sessions_back_to_start_i +=1
            if no_of_sessions_back_to_start_i >= stock_latest_data_effective_len:
                break

        if no_of_sessions_back_to_start_i < stock_latest_data_effective_len:
            sold_mas = [_34_day_EMA_high_series.iloc[no_of_sessions_back_to_start_i],_34_day_EMA_close_series.iloc[no_of_sessions_back_to_start_i], _34_day_EMA_low_series.iloc[no_of_sessions_back_to_start_i]]

    if no_of_sessions_back_to_start_i >= stock_latest_data_effective_len:
        return None
    else:
        stock_latest_data[no_of_sessions_back_to_start_i].update({'sold_mas':sold_mas})
        return stock_latest_data[no_of_sessions_back_to_start_i]

    # elif (stock_latest_data_effective_len > ma.MA_Strategy_Name._50_WEEK_SMA_20_DAY_RSI.max_session_size and mares.ma_strategy_name.value==ma.MA_Strategy_Name._50_WEEK_SMA_20_DAY_RSI.value):
    #     re_test_mares = ma._50_week_SMA_20_day_RSI(stock_latest_data)
    #     return re_test_mares.stock_price_greater_than_mas
