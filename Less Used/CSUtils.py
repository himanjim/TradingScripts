import math

import Indicators as ind
import PatternRecognition as pr
import ScrapUtils as nse_bse
import Utils as util


def get_stop_loss(buy_price, volatility_stop_loss, sup_or_resistance, action_value, lot):
    if action_value == pr.Action.SHORT.value:
        if volatility_stop_loss < buy_price:
            volatility_stop_loss = math.inf
        if sup_or_resistance < buy_price:
            sup_or_resistance = math.inf

        return min (buy_price + (pr.max_loss_to_bear_in_rs / lot), volatility_stop_loss, sup_or_resistance)

    if action_value == pr.Action.LONG.value:
        if volatility_stop_loss > buy_price:
            volatility_stop_loss = float ('-inf')
        if sup_or_resistance > buy_price:
            sup_or_resistance = float ('-inf')

        return max (buy_price - (pr.max_loss_to_bear_in_rs / lot), volatility_stop_loss, sup_or_resistance)



def getCSResAndErrors(stock_latest_info, stock_latest_data, stocks_pattern_recognition_responses, exception_errors,
                      market_previous_trend, no_of_sessions_to_scan_forstocks, no_of_sessions_to_scan_for_RSI,
                      no_of_sessions_to_scan_for_volatility, no_of_days_for_volatility_stop_loss):
    stock_data_closing_prices_series = util.get_panda_series_of_stock_closing_prices (stock_latest_data)

    # supports_resistances = sr.get_supports_resistances (stock_latest_data)
    supports_resistances = []

    # if len (supports_resistances) == 0:
    #     exception_errors.append (
    #         "******** No support or resistance found for stock:" + stock_latest_info[nse_bse.STOCK_ID])
    #     return
    # stock_latest_data_len = len (stock_latest_data)
    # if (stock_latest_data_len < no_of_sessions_to_scan_forstocks):
    #     exception_errors.append (
    #         "Stock data session size:" + str (stock_latest_data_len) + " less than required session size: " + str (
    #             no_of_sessions_to_scan_forstocks))
    #     return
    #
    rsi_series = ind.rsi (stock_data_closing_prices_series, no_of_sessions_to_scan_for_RSI)
    rsi = rsi_series.iloc[-1]
    # rsi_14_9_period_SMA_series = util.get_rsi_14_9_period_SMA (stock_data_closing_prices_series)
    # rsi_14_9_period_SMA = rsi_14_9_period_SMA_series.iloc[-1]
    #
    # volatility = outil.get_daily_volatility (
    #     outil.get_daily_returns (stock_latest_data[-no_of_sessions_to_scan_for_volatility: -1]))
    # volatility_stop_loss = outil.get_volatility_based_stoploss (stock_latest_data[-1]['close'], volatility,
    #                                                            no_of_days_for_volatility_stop_loss)
    volatility_stop_loss = 0
    rsi_14_9_period_SMA = 0

    # print ("---Testing various candlestick patterns for stock:" + stock_latest_info[nse_bse.STOCK_ID])
    evening_star_res = pr.Recognize_Evening_Star_pattern (stock_latest_data, supports_resistances, rsi,
                                                          rsi_14_9_period_SMA, market_previous_trend)
    evening_star_res.stock_id = stock_latest_info[nse_bse.STOCK_ID]
    evening_star_res.fetched_dataset = stock_latest_data
    evening_star_res.volatility_stop_loss = volatility_stop_loss
    stocks_pattern_recognition_responses.append (evening_star_res)

    morning_star_res = pr.Recognize_Morning_Star_pattern (stock_latest_data, supports_resistances, rsi,
                                                          rsi_14_9_period_SMA, market_previous_trend)
    morning_star_res.stock_id = stock_latest_info[nse_bse.STOCK_ID]
    morning_star_res.fetched_dataset = stock_latest_data
    morning_star_res.volatility_stop_loss = volatility_stop_loss
    stocks_pattern_recognition_responses.append (morning_star_res)

    bearish_harami_res = pr.Recognize_Bearish_Harami_pattern (stock_latest_data, supports_resistances, rsi,
                                                              rsi_14_9_period_SMA, market_previous_trend)
    bearish_harami_res.stock_id = stock_latest_info[nse_bse.STOCK_ID]
    bearish_harami_res.fetched_dataset = stock_latest_data
    bearish_harami_res.volatility_stop_loss = volatility_stop_loss
    stocks_pattern_recognition_responses.append (bearish_harami_res)

    bullish_harami_res = pr.Recognize_Bullish_Harami_pattern (stock_latest_data, supports_resistances, rsi,
                                                              rsi_14_9_period_SMA, market_previous_trend)
    bullish_harami_res.stock_id = stock_latest_info[nse_bse.STOCK_ID]
    bullish_harami_res.fetched_dataset = stock_latest_data
    bullish_harami_res.volatility_stop_loss = volatility_stop_loss
    stocks_pattern_recognition_responses.append (bullish_harami_res)

    bearish_engulfing_res = pr.Recognize_Bearish_Engulfing_pattern (stock_latest_data, supports_resistances, rsi,
                                                                    rsi_14_9_period_SMA, market_previous_trend)
    bearish_engulfing_res.stock_id = stock_latest_info[nse_bse.STOCK_ID]
    bearish_engulfing_res.fetched_dataset = stock_latest_data
    bearish_engulfing_res.volatility_stop_loss = volatility_stop_loss
    stocks_pattern_recognition_responses.append (bearish_engulfing_res)

    bullish_engulfing_res = pr.Recognize_Bullish_Engulfing_pattern (stock_latest_data, supports_resistances, rsi,
                                                                    rsi_14_9_period_SMA, market_previous_trend)
    bullish_engulfing_res.stock_id = stock_latest_info[nse_bse.STOCK_ID]
    bullish_engulfing_res.fetched_dataset = stock_latest_data
    bullish_engulfing_res.volatility_stop_loss = volatility_stop_loss
    stocks_pattern_recognition_responses.append (bullish_engulfing_res)

    shooting_star_res = pr.Recognize_Shooting_Star (stock_latest_data, supports_resistances, rsi, rsi_14_9_period_SMA,
                                                    market_previous_trend)
    shooting_star_res.stock_id = stock_latest_info[nse_bse.STOCK_ID]
    shooting_star_res.fetched_dataset = stock_latest_data
    shooting_star_res.volatility_stop_loss = volatility_stop_loss
    stocks_pattern_recognition_responses.append (shooting_star_res)

    hanging_man_res = pr.Recognize_Hanging_Man (stock_latest_data, supports_resistances, rsi, rsi_14_9_period_SMA,
                                                market_previous_trend)
    hanging_man_res.stock_id = stock_latest_info[nse_bse.STOCK_ID]
    hanging_man_res.fetched_dataset = stock_latest_data
    hanging_man_res.volatility_stop_loss = volatility_stop_loss
    stocks_pattern_recognition_responses.append (hanging_man_res)

    hammer_res = pr.Recognize_Hammer (stock_latest_data, supports_resistances, rsi, rsi_14_9_period_SMA,
                                      market_previous_trend)
    hammer_res.stock_id = stock_latest_info[nse_bse.STOCK_ID]
    hammer_res.fetched_dataset = stock_latest_data
    hammer_res.volatility_stop_loss = volatility_stop_loss
    stocks_pattern_recognition_responses.append (hammer_res)

    bearish_marubozo_res = pr.Recognize_Bearish_Marubozo (stock_latest_data, supports_resistances, rsi,
                                                          rsi_14_9_period_SMA, market_previous_trend)
    bearish_marubozo_res.stock_id = stock_latest_info[nse_bse.STOCK_ID]
    bearish_marubozo_res.fetched_dataset = stock_latest_data
    bearish_marubozo_res.volatility_stop_loss = volatility_stop_loss
    stocks_pattern_recognition_responses.append (bearish_marubozo_res)

    bullish_marubozo_res = pr.Recognize_Bullish_Marubozo (stock_latest_data, supports_resistances, rsi,
                                                          rsi_14_9_period_SMA, market_previous_trend)
    bullish_marubozo_res.stock_id = stock_latest_info[nse_bse.STOCK_ID]
    bullish_marubozo_res.fetched_dataset = stock_latest_data
    bullish_marubozo_res.volatility_stop_loss = volatility_stop_loss
    stocks_pattern_recognition_responses.append (bullish_marubozo_res)

    doji_res = pr.Recognize_Doji (stock_latest_data, supports_resistances, rsi,
                                  rsi_14_9_period_SMA, market_previous_trend)
    doji_res.stock_id = stock_latest_info[nse_bse.STOCK_ID]
    doji_res.fetched_dataset = stock_latest_data
    doji_res.volatility_stop_loss = volatility_stop_loss
    stocks_pattern_recognition_responses.append (doji_res)

    gap_up_down_res = pr.Recognize_Gap_Up_Down (stock_latest_data, supports_resistances, rsi,
                                                rsi_14_9_period_SMA, market_previous_trend)
    gap_up_down_res.stock_id = stock_latest_info[nse_bse.STOCK_ID]
    gap_up_down_res.fetched_dataset = stock_latest_data
    gap_up_down_res.volatility_stop_loss = volatility_stop_loss
    stocks_pattern_recognition_responses.append (gap_up_down_res)

    inverted_hammer_res = pr.Recognize_Inverted_Hammer (stock_latest_data, supports_resistances, rsi,
                                                        rsi_14_9_period_SMA, market_previous_trend)
    inverted_hammer_res.stock_id = stock_latest_info[nse_bse.STOCK_ID]
    inverted_hammer_res.fetched_dataset = stock_latest_data
    inverted_hammer_res.volatility_stop_loss = volatility_stop_loss
    stocks_pattern_recognition_responses.append (inverted_hammer_res)

    bullish_piercing_res = pr.Recognize_Bullish_Piercing_pattern (stock_latest_data, supports_resistances, rsi,
                                                                  rsi_14_9_period_SMA, market_previous_trend)
    bullish_piercing_res.stock_id = stock_latest_info[nse_bse.STOCK_ID]
    bullish_piercing_res.fetched_dataset = stock_latest_data
    bullish_piercing_res.volatility_stop_loss = volatility_stop_loss
    stocks_pattern_recognition_responses.append (bullish_piercing_res)

    bearish_piercing_res = pr.Recognize_Bearish_Piercing_pattern (stock_latest_data, supports_resistances, rsi,
                                                                  rsi_14_9_period_SMA, market_previous_trend)
    bearish_piercing_res.stock_id = stock_latest_info[nse_bse.STOCK_ID]
    bearish_piercing_res.fetched_dataset = stock_latest_data
    bearish_piercing_res.volatility_stop_loss = volatility_stop_loss
    stocks_pattern_recognition_responses.append (bearish_piercing_res)

    uptrend_res = pr.Recognize_Uptrend_pattern (stock_latest_data, supports_resistances, rsi,
                                                rsi_14_9_period_SMA, market_previous_trend)
    uptrend_res.stock_id = stock_latest_info[nse_bse.STOCK_ID]
    uptrend_res.fetched_dataset = stock_latest_data
    uptrend_res.volatility_stop_loss = volatility_stop_loss
    stocks_pattern_recognition_responses.append (uptrend_res)

    downtrend_res = pr.Recognize_Downtrend_pattern (stock_latest_data, supports_resistances, rsi,
                                                    rsi_14_9_period_SMA, market_previous_trend)
    downtrend_res.stock_id = stock_latest_info[nse_bse.STOCK_ID]
    downtrend_res.fetched_dataset = stock_latest_data
    downtrend_res.volatility_stop_loss = volatility_stop_loss
    stocks_pattern_recognition_responses.append (downtrend_res)


def get_pattern_recognition_response_result(stocks_pattern_recognition_response, stock_latest_data,
                                            desired_risk_reward_ratio, position_to_scan, lot):
    pattern_recognition_result = {'res': stocks_pattern_recognition_response, 'sl_trigged': 0, 'trigged_on': None,
                                  'tg_trigged': 0, 'exit_price': 0, 'exit_price_profit_%': 0, 'rsi_pat_ok': 1,
                                  'buy_on': util.get_date_from_timestamp (
                                      stock_latest_data[position_to_scan - 1]['timestamp']), 'earning': 0}

    stock_data_subset = stock_latest_data[position_to_scan:]

    buy_price = stock_latest_data[position_to_scan - 1]['close']

    stoploss = 0
    target = 0
    if stocks_pattern_recognition_response.action.value == pr.Action.LONG.value:

        stoploss = get_stop_loss (buy_price, stocks_pattern_recognition_response.volatility_stop_loss[0],
                                  stocks_pattern_recognition_response.support,
                                  stocks_pattern_recognition_response.action.value, lot)

        # stoploss = \
        #     [stocks_pattern_recognition_response.volatility_stop_loss[0], stocks_pattern_recognition_response.support][
        #         stocks_pattern_recognition_response.support < stocks_pattern_recognition_response.volatility_stop_loss[
        #             0]]

        target = (desired_risk_reward_ratio * (buy_price - stoploss)) + buy_price

    elif stocks_pattern_recognition_response.action.value == pr.Action.SHORT.value:
        stoploss = get_stop_loss (buy_price, stocks_pattern_recognition_response.volatility_stop_loss[1],
                                  stocks_pattern_recognition_response.resistance,
                                  stocks_pattern_recognition_response.action.value, lot)

        # stoploss = \
        #     [stocks_pattern_recognition_response.volatility_stop_loss[1],
        #      stocks_pattern_recognition_response.resistance][
        #         stocks_pattern_recognition_response.resistance >
        #         stocks_pattern_recognition_response.volatility_stop_loss[
        #             1]]

        target = buy_price - (desired_risk_reward_ratio * (stoploss - buy_price))


    rsis = ind.rsi(util.get_panda_series_of_stock_closing_prices(stock_latest_data), 14)
    current_position = position_to_scan

    for stock_data in stock_data_subset:

        if (stocks_pattern_recognition_response.action.value == pr.Action.LONG.value and stock_data[
            'low'] < stoploss) or (
                stocks_pattern_recognition_response.action.value == pr.Action.SHORT.value and stock_data[
            'high'] > stoploss):
            pattern_recognition_result.update (
                {'sl_trigged': 1, 'trigged_on': util.get_date_from_timestamp (stock_data['timestamp']), 'tg_trigged': 0,
                 'exit_price': [stock_data['high'], stock_data['low']][
                     stocks_pattern_recognition_response.action.value == pr.Action.LONG.value]})
            break

        if (stocks_pattern_recognition_response.action.value == pr.Action.LONG.value and stock_data[
            'high'] > target) or (
                stocks_pattern_recognition_response.action.value == pr.Action.SHORT.value and stock_data[
            'low'] < target):
            pattern_recognition_result.update (
                {'sl_trigged': 0, 'tg_trigged': 1, 'trigged_on': util.get_date_from_timestamp (stock_data['timestamp']),
                 'exit_price': [stock_data['low'], stock_data['high']][
                     stocks_pattern_recognition_response.action.value == pr.Action.LONG.value]})
            break

        current_position += 1

    rsis_list = []
    while position_to_scan <= current_position + 2:
        if position_to_scan < len (rsis):
            rsis_list.append (rsis.iloc[position_to_scan])
        position_to_scan += 1

    pattern_recognition_result.update (
        {'target': target,
         'stoploss': stoploss, 'rsi_smas': rsis_list, 'buy_price': buy_price})

    if pattern_recognition_result['trigged_on']:
        pattern_recognition_result.update (
            {'earning': (pattern_recognition_result['exit_price'] - buy_price) * lot * [1, -1][
                stocks_pattern_recognition_response.action.value == pr.Action.SHORT.value]})

    return pattern_recognition_result


