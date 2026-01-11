import enum

import Utils as util


class Trend (enum.Enum):
    uptrend = 1
    downtrend = -1
    notrend = 0
    weak_up_trend = .5
    weak_down_trend = -.5


class Action (enum.Enum):
    LONG = 1
    SHORT = -1


class Pattern_Name (enum.Enum):
    BULLISH_MARUBOZO = 1
    BEARISH_MARUBOZO = 2
    HAMMER = 3
    HANGING_MAN = 4
    SHOOTING_STAR = 5
    BULLISH_ENGULFING = 6
    BEARISH_ENGULFING = 7
    BULLISH_HARAMI = 8
    BEARISH_HARAMI = 9
    MORNING_STAR = 10
    EVENING_STAR = 11
    DOJI = 12
    GAP_UP_DOWN = 13
    INVERTED_HAMMER = 14
    BULLISH_PIERCING_PATTERN = 15
    BEARISH_PIERCING_PATTERN = 16
    UPTREND = 17
    DOWNTREND = 18


class PatternRecognitionResponse:
    def __init__(self):
        self.risk_reward_ratio = 0.0
        self.errors = []
        self.points = 0
        self.pattern_match = True
        self.previous_trend = None
        self.strong_correct_trend = True
        self.weak_correct_trend = True
        self.pattern_trend_same_as_market_trend = True
        self.high_volumes = True
        self.current_day_volumes = None
        self.last_10_day_average_volumes = None
        self.current_day_current_price = None
        self.correct_rsi = True
        self.correct_rsi_14_9_period_SMA = True
        self.correct_candle_length = True
        self.correct_support = None
        self.correct_resistance = None
        self.correct_risk_reward_ratio = True
        self.stock_id = None
        self.fetch_date = None
        self.pattern_name = None
        self.action = None
        self.fetched_dataset = []
        self.supports_resistances=[]
        self.support=None
        self.resistance=None
        self.volatility_stop_loss = None
        self.rsi=None
        self.rsi_14_9_period_SMA=None

    def __str__(self):
        res_str = ("\nPattern response start------------\n")
        res_str += "Stock id:" + str (self.stock_id) + '\n'
        res_str += "Pattern name:" + str (self.pattern_name) + '\n'
        if (len (self.errors) > 0):
            res_str += "Errors@@@@@@@@@@@@@@@@@@@@@@@@@@@:\n"
            for error in self.errors:
                res_str += str (error) + "\n"
            res_str += "Errors@@@@@@@@@@@@@@@@@@@@@@@@@@@:\n"
        res_str += "Fetch date:" + str (self.fetch_date) + '\n'
        res_str += "Points:" + str (self.points) + '\n'
        res_str += "Pattern match:" + str (self.pattern_match) + '\n'
        res_str += "Previous trend:" + str (self.previous_trend) + '\n'
        res_str += "Strong correct trend:" + str (self.strong_correct_trend) + '\n'
        res_str += "Weak correct trend:" + str (self.weak_correct_trend) + '\n'
        res_str += "Pattern trend same as market trend:" + str (self.pattern_trend_same_as_market_trend) + '\n'
        res_str += "High volumes:" + str (self.high_volumes) + '\n'
        res_str += "Current day volumes:" + str (self.current_day_volumes) + '\n'
        res_str += "Last 10 day average volumes:" + str (self.last_10_day_average_volumes) + '\n'
        res_str += "Current day current price:" + str (self.current_day_current_price) + '\n'
        res_str += "Correct rsi:" + str (self.correct_rsi) + '\n'
        res_str += "Correct rsi_14_9_period_SMA:" + str (self.correct_rsi_14_9_period_SMA) + '\n'
        res_str += "Correct candle length:" + str (self.correct_candle_length) + '\n'
        res_str += "Correct support:" + str (self.correct_support) + '\n'
        res_str += "Correct resistance:" + str (self.correct_resistance) + '\n'
        res_str += "Volatility stoploss:" + str (self.volatility_stop_loss) + '\n'
        res_str += "Correct risk reward ratio:" + str (self.correct_risk_reward_ratio) + '\n'
        res_str += "Action:" + str (self.action) + '\n'
        res_str += "Support:" + str (self.support) + '\n'
        res_str += "Resistance:" + str (self.resistance) + '\n'
        res_str += "Rsi:" + str (self.rsi) + '\n'
        res_str += "Rsi_14_9_period_SMA:" + str (self.rsi_14_9_period_SMA) + '\n'

        res_str += "Supports & resistances:\n"+str (self.supports_resistances)+ '\n'

        res_str += "Stock historic data(last 3 days)==============:\n"
        res_str += "Current day :"+str(self.fetched_dataset[-1])+ '\n'
        res_str += "Previous day :" + str (self.fetched_dataset[-2]) + '\n'
        res_str += "Previous to previous day :" + str (self.fetched_dataset[-3]) + '\n'
        res_str += "Last day :" + str(self.fetched_dataset[0]) + '\n'
        res_str += "Stock historic data(last 3 days)==============:\n"
        # for data in self.fetched_dataset:
        #     res_str += str(data) + "\n"

        res_str += ("Pattern response end------------\n")

        return res_str

    def is_pattern_tradable(self):
        return self.pattern_match and (self.previous_trend is None or (
                self.strong_correct_trend or self.weak_correct_trend)) and self.high_volumes and (
                       self.correct_resistance or self.correct_support)

    def is_perfect(self):
        return self.pattern_match and (self.previous_trend is None or (
                self.strong_correct_trend or self.weak_correct_trend)) and self.high_volumes and self.correct_rsi and self.correct_rsi_14_9_period_SMA and (
                self.correct_resistance or self.correct_support) and (
                       self.previous_trend is None or self.pattern_trend_same_as_market_trend) and self.correct_risk_reward_ratio and self.correct_candle_length


acceptable_risk_reward_ratio = 1
no_of_days_for_volatility_stop_loss = 5
no_of_sessions_to_scan_for_volatility = 252
high_low_variation_percent = .5
high_low_marubuzo_variation_percent = .2
high_low_shooting_star_lower_body_variation_percent = .2
support_resistance_variation_percent = 2
no_of_sessions_for_previous_trend = 5
up_down_trend_diff_percent = 5
min_average_volume_to_consider_for_patterns = 300000
min_volume_to_consider_for_patterns_ignore_prev_vol = 500000
max_loss_to_bear_in_rs = 20000
no_of_trend_errors_to_ignore = 1
upper_limit_for_rsi = 80
lower_limit_for_rsi = 20
upper_limit_for_rsi_14_9_period_SMA = 60
lower_limit_for_rsi_14_9_period_SMA = 40


def is_pattern_trend_same_as_market_trend(action, market_previous_trend):
    return (action.value > 0 and market_previous_trend.value > 0) or (
            action.value < 0 and market_previous_trend.value < 0)


def calculate_risk_reward_ratio_appropriate(current_price, target, stoploss):
    if current_price == stoploss:
        current_price += 1
    return (abs (target - current_price) / abs (current_price - stoploss))


def check_previous_trend(stock_data):
    last_price_in_trend = stock_data[-1]['close']

    no_of_trend_errors=0
    is_there_uptrend = True
    for x in range (len (stock_data) - 1):
        if (last_price_in_trend < stock_data[x]['close']):
            no_of_trend_errors+=1
            if(no_of_trend_errors>no_of_trend_errors_to_ignore):
                is_there_uptrend = False
                break

    if is_there_uptrend == False:
        no_of_trend_errors = 0
        is_there_downtrend = True
        for x in range (len (stock_data) - 1):
            if (last_price_in_trend > stock_data[x]['close']):
                no_of_trend_errors += 1
                if (no_of_trend_errors > no_of_trend_errors_to_ignore):
                    is_there_downtrend = False
                    break

        if is_there_downtrend == False:
            return Trend.notrend
        elif (util.nearly_equal (stock_data[0]['close'], last_price_in_trend, up_down_trend_diff_percent)):
            return Trend.weak_down_trend
        else:
            return Trend.downtrend
    elif (util.nearly_equal (stock_data[0]['close'], last_price_in_trend, up_down_trend_diff_percent)):
        return Trend.weak_up_trend
    else:
        return Trend.uptrend


def append_low_volume_error(res, last_1x_day_average_volume, stock_day_data):
    res.errors.append ("Very low volume than for last 1x day average volume:" + str (
        last_1x_day_average_volume) + " and/or min volume:" + str (min_average_volume_to_consider_for_patterns)+ " than for current day:" + str (stock_day_data['volume']))


def append_too_long_or_short_candle_error(res, stock_day_data):
    res.errors.append ("Very short or long candle with open:" + str (stock_day_data['open']) + " close:" + str (
        stock_day_data['close']))


def append_low_risk_reward_ratio_error(res, risk_reward_ratio, acceptable_risk_reward_ratio, current_price, target,
                                       stoploss):
    res.errors.append (
        "Calculated risk reward ratio:" + str (risk_reward_ratio) + " lower than acceptable ratio:" + str (
            acceptable_risk_reward_ratio) + " at current price:" + str (current_price) + " and target:" + str (
            target) + " and stoploss:" + str (stoploss))


def append_unacceptable_rsi_error(res, rsi, acceptable_rsi):
    res.errors.append ("Provided RSI value:" + str (rsi) + " not within acceptable range of:" + str (acceptable_rsi))


def append_unacceptable_rsi_14_9_period_SMA_error(res, rsi_14_9_period_SMA, acceptable_rsi_14_9_period_SMA):
    res.errors.append ("Provided RSI_14_9_period_SMA value:" + str (rsi_14_9_period_SMA) + " not within acceptable range of:" + str (acceptable_rsi_14_9_period_SMA))


def append_unacceptable_target_stoploss_variation_error(res, target_or_stop_loss):
    res.errors.append ("Target or stop loss:" + str (target_or_stop_loss) + " not within acceptable range")


def find_nearest_resistance_support(supports_resistances,price_to_scan,current_day_timestamp,is_support=True):
    if len(supports_resistances) == 0:
        return 0
    if is_support:
        nearest_support_resistance = supports_resistances[-1]
    else:
        nearest_support_resistance = supports_resistances[0]

    for support_resistance in supports_resistances:
        if(is_support==True and support_resistance['close']<price_to_scan and abs(current_day_timestamp-support_resistance['timestamp'])<=abs(current_day_timestamp-nearest_support_resistance['timestamp'])):
            nearest_support_resistance=support_resistance
        elif (is_support==False and support_resistance['close']>price_to_scan and abs(support_resistance['timestamp']-current_day_timestamp)<=abs(nearest_support_resistance['timestamp']-current_day_timestamp)):
            nearest_support_resistance = support_resistance

    return nearest_support_resistance['close']


def is_resistance_support_appropriate(supports_resistances,price_to_scan):
    for support_resistance in supports_resistances:
        if(util.nearly_equal(support_resistance['close'],price_to_scan,support_resistance_variation_percent)):
           return True

    return False


def is_volume_appropriate(stock_data, last_x_day_average_volume):
    return (stock_data['volume'] >= min_average_volume_to_consider_for_patterns and stock_data[
        'volume'] > last_x_day_average_volume) or stock_data[
               'volume'] >= min_volume_to_consider_for_patterns_ignore_prev_vol


def is_rsi_appropriate(rsi):
    return lower_limit_for_rsi < rsi < upper_limit_for_rsi


def Recognize_Bullish_Marubozo(stock_data, supports_resistances, rsi, rsi_14_9_period_SMA, market_trend):
    last_10_day_average_volume = util.calculate_last_10_days_average_volume (stock_data[-11:-1])
    current_day_data = stock_data[-1]
    current_day_current_price = util.get_current_day_current_price (stock_data[-1])

    resistance=find_nearest_resistance_support(supports_resistances,current_day_current_price,current_day_data['timestamp'],False)

    risk_reward_ratio = calculate_risk_reward_ratio_appropriate (current_day_current_price, resistance, current_day_data['low'])

    res = PatternRecognitionResponse ()
    res.risk_reward_ratio = risk_reward_ratio
    res.points = risk_reward_ratio + 1
    res.pattern_name = Pattern_Name.BULLISH_MARUBOZO
    res.action = Action.LONG
    res.previous_trend = None
    res.strong_correct_trend = None
    res.weak_correct_trend = None
    res.resistance=resistance
    res.support=current_day_data['low']
    res.rsi=rsi
    res.rsi_14_9_period_SMA = rsi_14_9_period_SMA
    res.fetch_date = util.get_date_from_timestamp(stock_data[-1]['timestamp'])
    res.supports_resistances = supports_resistances
    res.current_day_volumes = stock_data[-1]['volume']
    res.last_10_day_average_volumes = last_10_day_average_volume
    res.current_day_current_price = current_day_current_price

    if is_volume_appropriate(current_day_data, last_10_day_average_volume) == False:
        append_low_volume_error (res, last_10_day_average_volume, current_day_data)
        res.high_volumes = False

    if is_pattern_trend_same_as_market_trend (res.action, market_trend) == False:
        res.pattern_trend_same_as_market_trend = False
        res.errors.append ("Stock action:" + str (res.action) + " not same as market trend:" + str (market_trend.name))

    if (util.not_too_long_or_short_candle (current_day_data, stock_data[-11:-1])) == False:
        append_too_long_or_short_candle_error (res, current_day_data)
        res.correct_candle_length = False

    # if util.nearly_equal (current_day_current_price, current_day_data['high'],
    #                       high_low_marubuzo_variation_percent) == False:
    #     res.errors.append ("Current day current price:" + str (
    #         current_day_current_price) + " not nearly equal to current day high:" + str (current_day_data['high']))
    #     res.pattern_match = False

    if (current_day_data['open'] == current_day_data['low']) == False:
        res.errors.append (
            "Current day open:" + str (current_day_data['open']) + " not nearly equal to current day low:" + str (
                current_day_data['low']))
        res.pattern_match = False

    if (current_day_current_price > current_day_data['open']) == False:
        res.errors.append ("Current day current price:" + str (
            current_day_current_price) + " not greater than current day open:" + str (
            current_day_data['open']))
        res.pattern_match = False

    if is_rsi_appropriate (rsi) == False:
        append_unacceptable_rsi_error (res, rsi, lower_limit_for_rsi)
        res.correct_rsi = False

    if (rsi_14_9_period_SMA > lower_limit_for_rsi_14_9_period_SMA) == False:
        append_unacceptable_rsi_14_9_period_SMA_error (res, rsi_14_9_period_SMA, lower_limit_for_rsi_14_9_period_SMA)
        res.correct_rsi_14_9_period_SMA = False

    if is_resistance_support_appropriate(supports_resistances,current_day_data['low']) == False:
        append_unacceptable_target_stoploss_variation_error (res, current_day_data['low'])
        res.correct_support = False
    else:
        res.correct_support = True

    if (risk_reward_ratio >= acceptable_risk_reward_ratio) == False:
        append_low_risk_reward_ratio_error (res, risk_reward_ratio, acceptable_risk_reward_ratio,
                                            current_day_current_price, resistance,  current_day_data['low'])
        res.correct_risk_reward_ratio = False

    return res


def Recognize_Gap_Up_Down(stock_data, supports_resistances, rsi, rsi_14_9_period_SMA, market_trend):
    last_10_day_average_volume = util.calculate_last_10_days_average_volume (stock_data[-11:-1])
    current_day_data = stock_data[-1]
    previous_day_data = stock_data[-2]
    current_day_current_price = util.get_current_day_current_price (stock_data[-1])

    resistance = find_nearest_resistance_support (supports_resistances, current_day_current_price,
                                                  current_day_data['timestamp'], False)

    risk_reward_ratio = calculate_risk_reward_ratio_appropriate (current_day_current_price, resistance,
                                                                 current_day_data['low'])

    res = PatternRecognitionResponse ()
    res.risk_reward_ratio = risk_reward_ratio
    res.points = risk_reward_ratio + 1
    res.pattern_name = Pattern_Name.GAP_UP_DOWN
    res.action = Action.LONG
    res.previous_trend = None
    res.strong_correct_trend = None
    res.weak_correct_trend = None
    res.resistance = resistance
    res.support = current_day_data['low']
    res.rsi = rsi
    res.rsi_14_9_period_SMA = rsi_14_9_period_SMA
    res.fetch_date = util.get_date_from_timestamp (stock_data[-1]['timestamp'])
    res.supports_resistances = supports_resistances
    res.current_day_volumes = stock_data[-1]['volume']
    res.last_10_day_average_volumes = last_10_day_average_volume
    res.current_day_current_price = current_day_current_price

    if is_volume_appropriate (current_day_data, last_10_day_average_volume) == False:
        append_low_volume_error (res, last_10_day_average_volume, current_day_data)
        res.high_volumes = False

    if is_pattern_trend_same_as_market_trend (res.action, market_trend) == False:
        res.pattern_trend_same_as_market_trend = False
        res.errors.append ("Stock action:" + str (res.action) + " not same as market trend:" + str (market_trend.name))

    if (util.not_too_long_or_short_candle (current_day_data, stock_data[-11:-1])) == False:
        append_too_long_or_short_candle_error (res, current_day_data)
        res.correct_candle_length = False

    if ((current_day_data['low'] > previous_day_data['high'] and current_day_data['high'] > previous_day_data[
        'high'] and current_day_data['low'] > previous_day_data['low'] and current_day_data['high'] > previous_day_data[
             'low']) or (
                current_day_data['low'] < previous_day_data['high'] and current_day_data['high'] < previous_day_data[
            'high'] and current_day_data['low'] < previous_day_data['low'] and current_day_data['high'] <
                previous_day_data['low'])) == False:
        res.pattern_match = False

    if is_rsi_appropriate (rsi) == False:
        append_unacceptable_rsi_error (res, rsi, lower_limit_for_rsi)
        res.correct_rsi = False

    if (rsi_14_9_period_SMA > lower_limit_for_rsi_14_9_period_SMA) == False:
        append_unacceptable_rsi_14_9_period_SMA_error (res, rsi_14_9_period_SMA, lower_limit_for_rsi_14_9_period_SMA)
        res.correct_rsi_14_9_period_SMA = False

    if is_resistance_support_appropriate (supports_resistances, current_day_data['low']) == False:
        append_unacceptable_target_stoploss_variation_error (res, current_day_data['low'])
        res.correct_support = False
    else:
        res.correct_support = True

    if (risk_reward_ratio >= acceptable_risk_reward_ratio) == False:
        append_low_risk_reward_ratio_error (res, risk_reward_ratio, acceptable_risk_reward_ratio,
                                            current_day_current_price, resistance, current_day_data['low'])
        res.correct_risk_reward_ratio = False

    return res


def Recognize_Doji(stock_data, supports_resistances, rsi, rsi_14_9_period_SMA, market_trend):
    last_10_day_average_volume = util.calculate_last_10_days_average_volume (stock_data[-11:-1])
    current_day_data = stock_data[-1]
    current_day_current_price = util.get_current_day_current_price (stock_data[-1])

    resistance = find_nearest_resistance_support (supports_resistances, current_day_current_price,
                                                  current_day_data['timestamp'], False)

    risk_reward_ratio = calculate_risk_reward_ratio_appropriate (current_day_current_price, resistance,
                                                                 current_day_data['low'])

    res = PatternRecognitionResponse ()
    res.risk_reward_ratio = risk_reward_ratio
    res.points = risk_reward_ratio + 1
    res.pattern_name = Pattern_Name.DOJI
    res.action = Action.LONG
    res.previous_trend = None
    res.strong_correct_trend = None
    res.weak_correct_trend = None
    res.resistance = resistance
    res.support = current_day_data['low']
    res.rsi = rsi
    res.rsi_14_9_period_SMA = rsi_14_9_period_SMA
    res.fetch_date = util.get_date_from_timestamp (stock_data[-1]['timestamp'])
    res.supports_resistances = supports_resistances
    res.current_day_volumes = stock_data[-1]['volume']
    res.last_10_day_average_volumes = last_10_day_average_volume
    res.current_day_current_price = current_day_current_price

    if is_volume_appropriate (current_day_data, last_10_day_average_volume) == False:
        append_low_volume_error (res, last_10_day_average_volume, current_day_data)
        res.high_volumes = False

    if is_pattern_trend_same_as_market_trend (res.action, market_trend) == False:
        res.pattern_trend_same_as_market_trend = False
        res.errors.append ("Stock action:" + str (res.action) + " not same as market trend:" + str (market_trend.name))

    if (util.not_too_long_or_short_candle (current_day_data, stock_data[-11:-1])) == False:
        append_too_long_or_short_candle_error (res, current_day_data)
        res.correct_candle_length = False

    if util.nearly_equal (current_day_current_price, current_day_data['open'],
                          high_low_marubuzo_variation_percent) == False:
        res.errors.append ("Current day current price:" + str (
            current_day_current_price) + " not nearly equal to current day open:" + str (current_day_data['open']))
        res.pattern_match = False

    if (abs (current_day_data['low'] - current_day_data['high']) > (
            5 * abs (current_day_current_price - current_day_data['open']))) == False:
        res.errors.append ('Doji not long enough')
        res.pattern_match = False

    if is_rsi_appropriate (rsi) == False:
        append_unacceptable_rsi_error (res, rsi, lower_limit_for_rsi)
        res.correct_rsi = False

    if (rsi_14_9_period_SMA > lower_limit_for_rsi_14_9_period_SMA) == False:
        append_unacceptable_rsi_14_9_period_SMA_error (res, rsi_14_9_period_SMA, lower_limit_for_rsi_14_9_period_SMA)
        res.correct_rsi_14_9_period_SMA = False

    if is_resistance_support_appropriate (supports_resistances, current_day_data['low']) == False:
        append_unacceptable_target_stoploss_variation_error (res, current_day_data['low'])
        res.correct_support = False
    else:
        res.correct_support = True

    if (risk_reward_ratio >= acceptable_risk_reward_ratio) == False:
        append_low_risk_reward_ratio_error (res, risk_reward_ratio, acceptable_risk_reward_ratio,
                                            current_day_current_price, resistance, current_day_data['low'])
        res.correct_risk_reward_ratio = False

    return res


def Recognize_Bearish_Marubozo(stock_data, supports_resistances, rsi, rsi_14_9_period_SMA, market_trend):
    last_10_day_average_volume = util.calculate_last_10_days_average_volume (stock_data[-11:-1])
    current_day_data = stock_data[-1]
    current_day_current_price = util.get_current_day_current_price (stock_data[-1])

    support = find_nearest_resistance_support(supports_resistances,current_day_current_price,current_day_data['timestamp'])
    risk_reward_ratio = calculate_risk_reward_ratio_appropriate (current_day_current_price, support, current_day_data['high'])

    res = PatternRecognitionResponse ()
    res.risk_reward_ratio = risk_reward_ratio
    res.points = risk_reward_ratio + 1
    res.pattern_name = Pattern_Name.BEARISH_MARUBOZO
    res.action = Action.SHORT
    res.previous_trend = None
    res.strong_correct_trend = None
    res.weak_correct_trend = None
    res.resistance = current_day_data['high']
    res.support = support
    res.rsi = rsi
    res.rsi_14_9_period_SMA = rsi_14_9_period_SMA
    res.fetch_date = util.get_date_from_timestamp(stock_data[-1]['timestamp'])
    res.supports_resistances = supports_resistances
    res.current_day_volumes = stock_data[-1]['volume']
    res.last_10_day_average_volumes = last_10_day_average_volume
    res.current_day_current_price = current_day_current_price

    if is_volume_appropriate(current_day_data, last_10_day_average_volume) == False:
        append_low_volume_error (res, last_10_day_average_volume, current_day_data)
        res.high_volumes = False

    if is_pattern_trend_same_as_market_trend (res.action, market_trend) == False:
        res.pattern_trend_same_as_market_trend = False
        res.errors.append ("Stock action:" + str (res.action) + " not same as market trend:" + str (market_trend.name))

    if (util.not_too_long_or_short_candle (current_day_data, stock_data[-11:-1])) == False:
        append_too_long_or_short_candle_error (res, current_day_data)
        res.correct_candle_length = False

    # if util.nearly_equal (current_day_current_price, current_day_data['low'],
    #                       high_low_marubuzo_variation_percent) == False:
    #     res.errors.append ("Current day current price:" + str (
    #         current_day_current_price) + " not nearly equal to current day low:" + str (
    #         current_day_data['low']))
    #     res.pattern_match = False

    if (current_day_data['open'] == current_day_data['high']) == False:
        res.errors.append (
            "Current day open:" + str (current_day_data['open']) + " not nearly equal to current day high:" + str (
                current_day_data['high']))
        res.pattern_match = False

    if (current_day_current_price < current_day_data['open']) == False:
        res.errors.append (
            "Current day current price:" + str (current_day_current_price) + " not less than current day open:" + str (
                current_day_data['open']))
        res.pattern_match = False

    if is_rsi_appropriate (rsi) == False:
        append_unacceptable_rsi_error (res, rsi, upper_limit_for_rsi)
        res.correct_rsi = False

    if (rsi_14_9_period_SMA < upper_limit_for_rsi_14_9_period_SMA) == False:
        append_unacceptable_rsi_14_9_period_SMA_error (res, rsi_14_9_period_SMA, upper_limit_for_rsi_14_9_period_SMA)
        res.correct_rsi_14_9_period_SMA = False

    if is_resistance_support_appropriate(supports_resistances, current_day_data['high']) == False:
        append_unacceptable_target_stoploss_variation_error(res, current_day_data['high'])
        res.correct_resistance = False
    else:
        res.correct_resistance = True

    if (risk_reward_ratio >= acceptable_risk_reward_ratio) == False:
        append_low_risk_reward_ratio_error (res, risk_reward_ratio, acceptable_risk_reward_ratio,
                                            current_day_current_price, support, current_day_data['high'])
        res.correct_risk_reward_ratio = False

    return res


def Recognize_Hammer(stock_data, supports_resistances, rsi, rsi_14_9_period_SMA, market_trend):
    last_10_day_average_volume = util.calculate_last_10_days_average_volume (stock_data[-11:-1])
    last_11_day_average_volume = util.calculate_last_10_days_average_volume (stock_data[-12:-2])
    current_day_data = stock_data[-1]
    previous_day_data = stock_data[-2]
    current_day_current_price = util.get_current_day_current_price (stock_data[-1])

    resistance = find_nearest_resistance_support(supports_resistances,current_day_current_price,current_day_data['timestamp'], False)
    risk_reward_ratio = calculate_risk_reward_ratio_appropriate (current_day_current_price, resistance, current_day_data['low'])

    res = PatternRecognitionResponse ()
    res.risk_reward_ratio = risk_reward_ratio
    res.points = risk_reward_ratio + 2
    res.pattern_name = Pattern_Name.HAMMER
    res.action = Action.LONG
    res.resistance = resistance
    res.support = current_day_data['low']
    res.rsi = rsi
    res.rsi_14_9_period_SMA = rsi_14_9_period_SMA
    res.fetch_date = util.get_date_from_timestamp(stock_data[-1]['timestamp'])
    res.supports_resistances = supports_resistances
    res.current_day_volumes = stock_data[-1]['volume']
    res.last_10_day_average_volumes = last_10_day_average_volume
    res.current_day_current_price = current_day_current_price

    if is_volume_appropriate(current_day_data, last_10_day_average_volume) == False:
        append_low_volume_error (res, last_10_day_average_volume, current_day_data)
        res.high_volumes = False

    # if is_volume_appropriate(previous_day_data, last_11_day_average_volume) == True:
    #     append_low_volume_error (res, last_11_day_average_volume, previous_day_data)
    #     res.high_volumes = False

    previous_trend = check_previous_trend (stock_data[-no_of_sessions_for_previous_trend - 2: -2])
    res.previous_trend = previous_trend
    if (previous_trend.value == Trend.downtrend.value) == False:
        res.errors.append (
            "No clear downtrend." + str (stock_data[-no_of_sessions_for_previous_trend - 3]['close']) + " : " + str (
                stock_data[-3]['close']))
        res.strong_correct_trend = False
    else:
        res.weak_correct_trend = False

    if (previous_trend.value == Trend.weak_down_trend.value or previous_trend.value == Trend.downtrend.value) == False:
        res.weak_correct_trend = False
    elif (previous_trend.value == Trend.weak_down_trend.value) == True:
        res.errors.append ("Weak downtrend.")

    if is_pattern_trend_same_as_market_trend (res.action, market_trend) == False:
        res.pattern_trend_same_as_market_trend = False
        res.errors.append ("Stock action:" + str (res.action) + " not same as market trend:" + str (market_trend.name))

    if (util.not_too_long_or_short_candle (current_day_data, stock_data[-11:-1])) == False:
        append_too_long_or_short_candle_error (res, current_day_data)
        res.correct_candle_length = False

    if (util.not_too_long_or_short_candle (previous_day_data, stock_data[-12:-2])) == False:
        append_too_long_or_short_candle_error (res, previous_day_data)
        res.correct_candle_length = False

    # if (current_day_current_price >= current_day_data['open']) == False:
    #     res.errors.append ("Current day current price:" + str (
    #         current_day_current_price) + " not greater than current day open:" + str (
    #         current_day_data['open']))
    #     res.pattern_match = False

    if (previous_day_data['close'] < previous_day_data['open']) == False:
        res.errors.append (
            "Previous day close:" + str (previous_day_data['close']) + " not less than current day open:" + str (
                current_day_data['open']))
        res.pattern_match = False

    if ((util.nearly_equal (current_day_current_price, current_day_data['high'], high_low_variation_percent) and (
            abs (current_day_data['open'] - current_day_data['low']) > (
            2 * abs (current_day_current_price - current_day_data['open'])))) or (
                util.nearly_equal (current_day_data['open'], current_day_data['high'], high_low_variation_percent) and (
                abs (current_day_current_price - current_day_data['low']) > (
                2 * abs (current_day_data['open'] - current_day_current_price))))) == False:
        res.errors.append ("Current day current price:" + str (
            current_day_current_price) + " not nearly equal to current day high:" + str (
            current_day_data['high']))
        res.pattern_match = False

    # if ( or ) == False:
    #     res.errors.append ("Current day (open-low):" + str ((current_day_data['open'] - current_day_data[
    #         'low'])) + " not twice current day (current price-open):" + str (
    #         (current_day_current_price - current_day_data['open'])))
    #     res.pattern_match = False

    if is_rsi_appropriate (rsi) == False:
        append_unacceptable_rsi_error (res, rsi, lower_limit_for_rsi)
        res.correct_rsi = False

    if (rsi_14_9_period_SMA > lower_limit_for_rsi_14_9_period_SMA) == False:
        append_unacceptable_rsi_14_9_period_SMA_error (res, rsi_14_9_period_SMA, lower_limit_for_rsi_14_9_period_SMA)
        res.correct_rsi_14_9_period_SMA = False

    if is_resistance_support_appropriate(supports_resistances, current_day_data['low']) == False:
        append_unacceptable_target_stoploss_variation_error(res, current_day_data['low'])
        res.correct_support = False
    else:
        res.correct_support = True

    if (risk_reward_ratio >= acceptable_risk_reward_ratio) == False:
        append_low_risk_reward_ratio_error (res, risk_reward_ratio, acceptable_risk_reward_ratio,
                                            current_day_current_price, resistance, current_day_data['low'])
        res.correct_risk_reward_ratio = False

    return res


def Recognize_Hanging_Man(stock_data, supports_resistances, rsi, rsi_14_9_period_SMA, market_trend):
    last_10_day_average_volume = util.calculate_last_10_days_average_volume (stock_data[-11:-1])
    last_11_day_average_volume = util.calculate_last_10_days_average_volume (stock_data[-12:-2])
    current_day_data = stock_data[-1]
    previous_day_data = stock_data[-2]
    current_day_current_price = util.get_current_day_current_price (stock_data[-1])

    support = find_nearest_resistance_support(supports_resistances,current_day_current_price,current_day_data['timestamp'])
    risk_reward_ratio = calculate_risk_reward_ratio_appropriate (current_day_current_price, support, current_day_data['high'])

    res = PatternRecognitionResponse ()
    res.risk_reward_ratio = risk_reward_ratio
    res.points = risk_reward_ratio + 2
    res.pattern_name = Pattern_Name.HANGING_MAN
    res.action = Action.SHORT
    res.resistance = current_day_data['high']
    res.support = support
    res.rsi = rsi
    res.rsi_14_9_period_SMA = rsi_14_9_period_SMA
    res.fetch_date = util.get_date_from_timestamp(stock_data[-1]['timestamp'])
    res.supports_resistances = supports_resistances
    res.current_day_volumes = stock_data[-1]['volume']
    res.last_10_day_average_volumes = last_10_day_average_volume
    res.current_day_current_price = current_day_current_price

    if is_volume_appropriate(current_day_data, last_10_day_average_volume) == False:
        append_low_volume_error (res, last_10_day_average_volume, current_day_data)
        res.high_volumes = False

    # if is_volume_appropriate(previous_day_data, last_11_day_average_volume) == False:
    #     append_low_volume_error (res, last_11_day_average_volume, previous_day_data)
    #     res.high_volumes = False

    previous_trend = check_previous_trend (stock_data[-no_of_sessions_for_previous_trend - 2: -2])
    res.previous_trend = previous_trend
    if (previous_trend.value == Trend.uptrend.value) == False:
        res.errors.append (
            "No clear uptrend." + str (stock_data[-no_of_sessions_for_previous_trend - 3]['close']) + " : " + str (
                stock_data[-3]['close']))
        res.strong_correct_trend = False
    else:
        res.weak_correct_trend = False

    if (previous_trend.value == Trend.weak_up_trend.value or previous_trend.value == Trend.uptrend.value) == False:
        res.weak_correct_trend = False
    elif (previous_trend.value == Trend.weak_up_trend.value) == True:
        res.errors.append ("Weak uptrend.")

    if is_pattern_trend_same_as_market_trend (res.action, market_trend) == False:
        res.pattern_trend_same_as_market_trend = False
        res.errors.append ("Stock action:" + str (res.action) + " not same as market trend:" + str (market_trend.name))

    if (util.not_too_long_or_short_candle (current_day_data, stock_data[-11:-1])) == False:
        append_too_long_or_short_candle_error (res, current_day_data)
        res.correct_candle_length = False

    if (util.not_too_long_or_short_candle (previous_day_data, stock_data[-12:-2])) == False:
        append_too_long_or_short_candle_error (res, previous_day_data)
        res.correct_candle_length = False

    # if (current_day_current_price <= current_day_data['open']) == False:
    #     res.errors.append (
    #         "Current day current price:" + str (current_day_current_price) + " not less than current day open:" + str (
    #             current_day_data['open']))
    #     res.pattern_match = False

    if (previous_day_data['close'] > previous_day_data['open']) == False:
        res.errors.append (
            "Previous day close:" + str (previous_day_data['close']) + " not greater than previous day open:" + str (
                previous_day_data['open']))
        res.pattern_match = False

    if ((util.nearly_equal (current_day_current_price, current_day_data['high'], high_low_variation_percent) and (
            abs (current_day_data['open'] - current_day_data['low']) > (
            2 * abs (current_day_data['high'] - current_day_data['open'])))) or (
                util.nearly_equal (current_day_data['open'], current_day_data['high'], high_low_variation_percent) and (
                abs (current_day_current_price - current_day_data['low']) > (
                2 * abs (current_day_data['high'] - current_day_current_price))))) == False:
        res.errors.append ("Current day current price/open:" + str (
            current_day_current_price) + " not nearly equal to current day high:" + str (
            current_day_data['high']))
        res.pattern_match = False

    # if ( or  ) == False:
    #     res.errors.append ("Current day (cp-low):" + str ((current_day_current_price - current_day_data['low'])) +
    #                        " not twice current day (high-cp):" + str (
    #         (current_day_data['high'] - current_day_current_price)))
    #     res.pattern_match = False

    if is_rsi_appropriate (rsi) == False:
        append_unacceptable_rsi_error (res, rsi, upper_limit_for_rsi)
        res.correct_rsi = False

    if (rsi_14_9_period_SMA < upper_limit_for_rsi_14_9_period_SMA) == False:
        append_unacceptable_rsi_14_9_period_SMA_error (res, rsi_14_9_period_SMA, upper_limit_for_rsi_14_9_period_SMA)
        res.correct_rsi_14_9_period_SMA = False

    if is_resistance_support_appropriate(supports_resistances, current_day_data['high']) == False:
        append_unacceptable_target_stoploss_variation_error(res, current_day_data['high'])
        res.correct_resistance = False
    else:
        res.correct_resistance = True

    if (risk_reward_ratio >= acceptable_risk_reward_ratio) == False:
        append_low_risk_reward_ratio_error (res, risk_reward_ratio, acceptable_risk_reward_ratio,
                                            current_day_current_price, support, current_day_data['high'])
        res.correct_risk_reward_ratio = False

    return res


def Recognize_Shooting_Star(stock_data, supports_resistances, rsi, rsi_14_9_period_SMA, market_trend):
    last_10_day_average_volume = util.calculate_last_10_days_average_volume (stock_data[-11:-1])
    last_11_day_average_volume = util.calculate_last_10_days_average_volume (stock_data[-12:-2])
    current_day_data = stock_data[-1]
    previous_day_data = stock_data[-2]
    current_day_current_price = util.get_current_day_current_price (stock_data[-1])

    support = find_nearest_resistance_support(supports_resistances,current_day_current_price,current_day_data['timestamp'])
    risk_reward_ratio = calculate_risk_reward_ratio_appropriate (current_day_current_price, support, current_day_data['high'])

    res = PatternRecognitionResponse ()
    res.risk_reward_ratio = risk_reward_ratio
    res.points = risk_reward_ratio + 2
    res.pattern_name = Pattern_Name.SHOOTING_STAR
    res.action = Action.SHORT
    res.resistance = current_day_data['high']
    res.support = support
    res.rsi = rsi
    res.rsi_14_9_period_SMA = rsi_14_9_period_SMA
    res.fetch_date = util.get_date_from_timestamp(stock_data[-1]['timestamp'])
    res.supports_resistances = supports_resistances
    res.current_day_volumes = stock_data[-1]['volume']
    res.last_10_day_average_volumes = last_10_day_average_volume
    res.current_day_current_price = current_day_current_price

    if is_volume_appropriate(current_day_data, last_10_day_average_volume) == False:
        append_low_volume_error (res, last_10_day_average_volume, current_day_data)
        res.high_volumes = False

    # if is_volume_appropriate(previous_day_data, last_11_day_average_volume) == False:
    #     append_low_volume_error (res, last_11_day_average_volume, previous_day_data)
    #     res.high_volumes = False

    previous_trend = check_previous_trend (stock_data[-no_of_sessions_for_previous_trend - 2: -2])
    res.previous_trend = previous_trend
    if (previous_trend.value == Trend.uptrend.value) == False:
        res.errors.append (
            "No clear uptrend." + str (stock_data[-no_of_sessions_for_previous_trend - 3]['close']) + " : " + str (
                stock_data[-3]['close']))
        res.strong_correct_trend = False
    else:
        res.weak_correct_trend = False

    if (previous_trend.value == Trend.weak_up_trend.value or previous_trend.value == Trend.uptrend.value) == False:
        res.weak_correct_trend = False
    elif (previous_trend.value == Trend.weak_up_trend.value) == True:
        res.errors.append ("Weak uptrend.")

    if is_pattern_trend_same_as_market_trend (res.action, market_trend) == False:
        res.pattern_trend_same_as_market_trend = False
        res.errors.append ("Stock action:" + str (res.action) + " not same as market trend:" + str (market_trend.name))

    if (util.not_too_long_or_short_candle (current_day_data, stock_data[-11:-1])) == False:
        append_too_long_or_short_candle_error (res, current_day_data)
        res.correct_candle_length = False

    if (util.not_too_long_or_short_candle (previous_day_data, stock_data[-12:-2])) == False:
        append_too_long_or_short_candle_error (res, previous_day_data)
        res.correct_candle_length = False

    if (previous_day_data['close'] > previous_day_data['open']) == False:
        res.errors.append (
            "Previous day close:" + str (previous_day_data['close']) + " not greater than previous day open:" + str (
                previous_day_data['open']))
        res.pattern_match = False

    # in case of bullish & bearish candle
    if ((util.nearly_equal (current_day_data['open'], current_day_data['low'],
                            high_low_shooting_star_lower_body_variation_percent) and (
                 abs (current_day_data['high'] - current_day_current_price) > (
                 2 * abs (current_day_current_price - current_day_data['low'])))) or (
                util.nearly_equal (current_day_data['low'], current_day_current_price,
                                   high_low_shooting_star_lower_body_variation_percent) and (
                        abs (current_day_data['high'] - current_day_data['open']) > (
                        2 * abs (current_day_data['open'] - current_day_current_price))))) == False:
        res.errors.append ("Current day (open or low):(" + str (current_day_data['open']) + " or " + str (
            current_day_data['low']) + ") not nearly equal to current day (cp or low):(" + str (
            current_day_current_price) + " or " + str (current_day_data['low']) + ")")
        res.errors.append ("Current day (high-cp(or open)):(" + str (
            current_day_data['high'] - current_day_current_price) + " or " + str (
            current_day_data['high'] - current_day_data['open']) +
                           ") not twice current day ((cp-low) or (open-cp)):(" + str
                           (current_day_current_price - current_day_data['low']) + "or " + str (
            current_day_data['open'] - current_day_current_price))
        res.pattern_match = False

    if is_rsi_appropriate (rsi) == False:
        append_unacceptable_rsi_error (res, rsi, upper_limit_for_rsi)
        res.correct_rsi = False

    if (rsi_14_9_period_SMA < upper_limit_for_rsi_14_9_period_SMA) == False:
        append_unacceptable_rsi_14_9_period_SMA_error (res, rsi_14_9_period_SMA, upper_limit_for_rsi_14_9_period_SMA)
        res.correct_rsi_14_9_period_SMA = False

    if is_resistance_support_appropriate(supports_resistances, current_day_data['high']) == False:
        append_unacceptable_target_stoploss_variation_error(res, current_day_data['high'])
        res.correct_resistance = False
    else:
        res.correct_resistance = True

    if (risk_reward_ratio >= acceptable_risk_reward_ratio) == False:
        append_low_risk_reward_ratio_error (res, risk_reward_ratio, acceptable_risk_reward_ratio,
                                            current_day_current_price, support, current_day_data['high'])
        res.correct_risk_reward_ratio = False

    return res


def Recognize_Inverted_Hammer(stock_data, supports_resistances, rsi, rsi_14_9_period_SMA, market_trend):
    last_10_day_average_volume = util.calculate_last_10_days_average_volume (stock_data[-11:-1])
    last_11_day_average_volume = util.calculate_last_10_days_average_volume (stock_data[-12:-2])
    current_day_data = stock_data[-1]
    previous_day_data = stock_data[-2]
    current_day_current_price = util.get_current_day_current_price (stock_data[-1])

    support = find_nearest_resistance_support (supports_resistances, current_day_current_price,
                                               current_day_data['timestamp'])
    risk_reward_ratio = calculate_risk_reward_ratio_appropriate (current_day_current_price, support,
                                                                 current_day_data['high'])

    res = PatternRecognitionResponse ()
    res.risk_reward_ratio = risk_reward_ratio
    res.points = risk_reward_ratio + 2
    res.pattern_name = Pattern_Name.INVERTED_HAMMER
    res.action = Action.SHORT
    res.resistance = current_day_data['high']
    res.support = support
    res.rsi = rsi
    res.rsi_14_9_period_SMA = rsi_14_9_period_SMA
    res.fetch_date = util.get_date_from_timestamp (stock_data[-1]['timestamp'])
    res.supports_resistances = supports_resistances
    res.current_day_volumes = stock_data[-1]['volume']
    res.last_10_day_average_volumes = last_10_day_average_volume
    res.current_day_current_price = current_day_current_price

    if is_volume_appropriate (current_day_data, last_10_day_average_volume) == False:
        append_low_volume_error (res, last_10_day_average_volume, current_day_data)
        res.high_volumes = False

    # if is_volume_appropriate(previous_day_data, last_11_day_average_volume) == False:
    #     append_low_volume_error (res, last_11_day_average_volume, previous_day_data)
    #     res.high_volumes = False

    previous_trend = check_previous_trend (stock_data[-no_of_sessions_for_previous_trend - 2: -2])
    res.previous_trend = previous_trend
    if (previous_trend.value == Trend.downtrend.value) == False:
        res.errors.append (
            "No clear downtrend." + str (stock_data[-no_of_sessions_for_previous_trend - 3]['close']) + " : " + str (
                stock_data[-3]['close']))
        res.strong_correct_trend = False
    else:
        res.weak_correct_trend = False

    if (previous_trend.value == Trend.weak_down_trend.value or previous_trend.value == Trend.downtrend.value) == False:
        res.weak_correct_trend = False
    elif (previous_trend.value == Trend.downtrend.value) == True:
        res.errors.append ("Weak downtrend.")

    if is_pattern_trend_same_as_market_trend (res.action, market_trend) == False:
        res.pattern_trend_same_as_market_trend = False
        res.errors.append ("Stock action:" + str (res.action) + " not same as market trend:" + str (market_trend.name))

    if (util.not_too_long_or_short_candle (current_day_data, stock_data[-11:-1])) == False:
        append_too_long_or_short_candle_error (res, current_day_data)
        res.correct_candle_length = False

    if (util.not_too_long_or_short_candle (previous_day_data, stock_data[-12:-2])) == False:
        append_too_long_or_short_candle_error (res, previous_day_data)
        res.correct_candle_length = False

    if (previous_day_data['close'] < previous_day_data['open']) == False:
        res.errors.append (
            "Previous day close:" + str (previous_day_data['close']) + " not less than previous day open:" + str (
                previous_day_data['open']))
        res.pattern_match = False

    # in case of bullish & bearish candle
    if ((util.nearly_equal (current_day_data['open'], current_day_data['low'],
                            high_low_shooting_star_lower_body_variation_percent) and (
                 abs (current_day_data['high'] - current_day_current_price) > (
                 2 * abs (current_day_current_price - current_day_data['low'])))) or (
                util.nearly_equal (current_day_data['low'], current_day_current_price,
                                   high_low_shooting_star_lower_body_variation_percent) and (
                        abs (current_day_data['high'] - current_day_data['open']) > (
                        2 * abs (current_day_data['open'] - current_day_current_price))))) == False:
        res.errors.append ("Current day (open or low):(" + str (current_day_data['open']) + " or " + str (
            current_day_data['low']) + ") not nearly equal to current day (cp or low):(" + str (
            current_day_current_price) + " or " + str (current_day_data['low']) + ")")
        res.errors.append ("Current day (high-cp(or open)):(" + str (
            current_day_data['high'] - current_day_current_price) + " or " + str (
            current_day_data['high'] - current_day_data['open']) +
                           ") not twice current day ((cp-low) or (open-cp)):(" + str
                           (current_day_current_price - current_day_data['low']) + "or " + str (
            current_day_data['open'] - current_day_current_price))
        res.pattern_match = False

    if is_rsi_appropriate (rsi) == False:
        append_unacceptable_rsi_error (res, rsi, upper_limit_for_rsi)
        res.correct_rsi = False

    if (rsi_14_9_period_SMA < upper_limit_for_rsi_14_9_period_SMA) == False:
        append_unacceptable_rsi_14_9_period_SMA_error (res, rsi_14_9_period_SMA, upper_limit_for_rsi_14_9_period_SMA)
        res.correct_rsi_14_9_period_SMA = False

    if is_resistance_support_appropriate (supports_resistances, current_day_data['high']) == False:
        append_unacceptable_target_stoploss_variation_error (res, current_day_data['high'])
        res.correct_resistance = False
    else:
        res.correct_resistance = True

    if (risk_reward_ratio >= acceptable_risk_reward_ratio) == False:
        append_low_risk_reward_ratio_error (res, risk_reward_ratio, acceptable_risk_reward_ratio,
                                            current_day_current_price, support, current_day_data['high'])
        res.correct_risk_reward_ratio = False

    return res


def Recognize_Bullish_Engulfing_pattern(stock_data, supports_resistances, rsi, rsi_14_9_period_SMA, market_trend):
    last_10_day_average_volume = util.calculate_last_10_days_average_volume (stock_data[-11:-1])
    last_11_day_average_volume = util.calculate_last_10_days_average_volume (stock_data[-12:-2])
    current_day_data = stock_data[-1]
    previous_day_data = stock_data[-2]
    current_day_current_price = util.get_current_day_current_price (stock_data[-1])

    lowest_low = [previous_day_data['low'], current_day_data['low']][current_day_data['low'] < previous_day_data['low']]

    resistance = find_nearest_resistance_support(supports_resistances,current_day_current_price,current_day_data['timestamp'], False)
    risk_reward_ratio = calculate_risk_reward_ratio_appropriate (current_day_current_price, resistance, lowest_low)

    res = PatternRecognitionResponse ()
    res.risk_reward_ratio = risk_reward_ratio
    res.points = risk_reward_ratio + 2
    res.pattern_name = Pattern_Name.BULLISH_ENGULFING
    res.action = Action.LONG
    res.strong_correct_trend = None
    res.weak_correct_trend = None
    res.resistance = resistance
    res.support = lowest_low
    res.rsi = rsi
    res.rsi_14_9_period_SMA = rsi_14_9_period_SMA
    res.fetch_date = util.get_date_from_timestamp(stock_data[-1]['timestamp'])
    res.supports_resistances = supports_resistances
    res.current_day_volumes = stock_data[-1]['volume']
    res.last_10_day_average_volumes = last_10_day_average_volume
    res.current_day_current_price = current_day_current_price

    if is_volume_appropriate(current_day_data, last_10_day_average_volume) == False:
        append_low_volume_error (res, last_10_day_average_volume, current_day_data)
        res.high_volumes = False

    # if is_volume_appropriate(previous_day_data, last_11_day_average_volume)== False:
    #     append_low_volume_error (res, last_11_day_average_volume, previous_day_data)
    #     res.high_volumes = False

    # previous_trend = check_previous_trend (stock_data[-no_of_sessions_for_previous_trend - 2: -2])
    # res.previous_trend = previous_trend
    # if (previous_trend.value == Trend.downtrend.value) == False:
    #     res.errors.append (
    #         "No clear downtrend." + str (stock_data[-no_of_sessions_for_previous_trend - 3]['close']) + " : " + str (
    #             stock_data[-3]['close']))
    #     res.strong_correct_trend = False
    # else:
    #     res.weak_correct_trend = False
    #
    # if (previous_trend.value == Trend.weak_down_trend.value or previous_trend.value == Trend.downtrend.value) == False:
    #     res.weak_correct_trend = False
    # elif (previous_trend.value == Trend.weak_down_trend.value) == True:
    #     res.errors.append ("Weak downtrend.")

    if is_pattern_trend_same_as_market_trend (res.action, market_trend) == False:
        res.pattern_trend_same_as_market_trend = False
        res.errors.append ("Stock action:" + str (res.action) + " not same as market trend:" + str (market_trend.name))

    if (util.not_too_long_or_short_candle (current_day_data, stock_data[-11:-1])) == False:
        append_too_long_or_short_candle_error (res, current_day_data)
        res.correct_candle_length = False

    if (util.not_too_long_or_short_candle (previous_day_data, stock_data[-12:-2])) == False:
        append_too_long_or_short_candle_error (res, previous_day_data)
        res.correct_candle_length = False

    if (current_day_current_price > current_day_data['open']) == False:
        res.errors.append (
            "Current day cp:" + str (current_day_current_price) + " not greater than current day open:" + str (
                current_day_data['open']))
        res.pattern_match = False

    if (previous_day_data['close'] < previous_day_data['open']) == False:
        res.errors.append (
            "Previous day close:" + str (previous_day_data['close']) + " not less than previous day open:" + str (
                previous_day_data['open']))
        res.pattern_match = False

    if (current_day_current_price > previous_day_data['open']) == False:
        res.errors.append (
            "Current day cp:" + str (current_day_current_price) + " not greater than previous day open:" + str (
                previous_day_data['open']))
        res.pattern_match = False

    if (current_day_data['open'] < previous_day_data['close']) == False:
        res.errors.append (
            "Current day open:" + str (current_day_data['open']) + " not less than previous day close:" + str (
                previous_day_data['close']))
        res.pattern_match = False

    if is_rsi_appropriate (rsi) == False:
        append_unacceptable_rsi_error (res, rsi, lower_limit_for_rsi)
        res.correct_rsi = False

    if (rsi_14_9_period_SMA > lower_limit_for_rsi_14_9_period_SMA) == False:
        append_unacceptable_rsi_14_9_period_SMA_error (res, rsi_14_9_period_SMA, lower_limit_for_rsi_14_9_period_SMA)
        res.correct_rsi_14_9_period_SMA = False

    if is_resistance_support_appropriate(supports_resistances, lowest_low) == False:
        append_unacceptable_target_stoploss_variation_error(res, lowest_low)
        res.correct_support = False
    else:
        res.correct_support = True

    if (risk_reward_ratio >= acceptable_risk_reward_ratio) == False:
        append_low_risk_reward_ratio_error (res, risk_reward_ratio, acceptable_risk_reward_ratio,
                                            current_day_current_price, resistance, lowest_low)
        res.correct_risk_reward_ratio = False

    return res


def Recognize_Bullish_Piercing_pattern(stock_data, supports_resistances, rsi, rsi_14_9_period_SMA, market_trend):
    last_10_day_average_volume = util.calculate_last_10_days_average_volume (stock_data[-11:-1])
    last_11_day_average_volume = util.calculate_last_10_days_average_volume (stock_data[-12:-2])
    current_day_data = stock_data[-1]
    previous_day_data = stock_data[-2]
    current_day_current_price = util.get_current_day_current_price (stock_data[-1])

    lowest_low = [previous_day_data['low'], current_day_data['low']][current_day_data['low'] < previous_day_data['low']]

    resistance = find_nearest_resistance_support (supports_resistances, current_day_current_price,
                                                  current_day_data['timestamp'], False)
    risk_reward_ratio = calculate_risk_reward_ratio_appropriate (current_day_current_price, resistance, lowest_low)

    res = PatternRecognitionResponse ()
    res.risk_reward_ratio = risk_reward_ratio
    res.points = risk_reward_ratio + 2
    res.pattern_name = Pattern_Name.BULLISH_PIERCING_PATTERN
    res.action = Action.LONG
    res.resistance = resistance
    res.support = lowest_low
    res.rsi = rsi
    res.rsi_14_9_period_SMA = rsi_14_9_period_SMA
    res.fetch_date = util.get_date_from_timestamp (stock_data[-1]['timestamp'])
    res.supports_resistances = supports_resistances
    res.current_day_volumes = stock_data[-1]['volume']
    res.last_10_day_average_volumes = last_10_day_average_volume
    res.current_day_current_price = current_day_current_price

    if is_volume_appropriate (current_day_data, last_10_day_average_volume) == False:
        append_low_volume_error (res, last_10_day_average_volume, current_day_data)
        res.high_volumes = False

    # if is_volume_appropriate(previous_day_data, last_11_day_average_volume)== False:
    #     append_low_volume_error (res, last_11_day_average_volume, previous_day_data)
    #     res.high_volumes = False

    previous_trend = check_previous_trend (stock_data[-no_of_sessions_for_previous_trend - 2: -2])
    res.previous_trend = previous_trend
    if (previous_trend.value == Trend.downtrend.value) == False:
        res.errors.append (
            "No clear downtrend." + str (stock_data[-no_of_sessions_for_previous_trend - 3]['close']) + " : " + str (
                stock_data[-3]['close']))
        res.strong_correct_trend = False
    else:
        res.weak_correct_trend = False

    if (previous_trend.value == Trend.weak_down_trend.value or previous_trend.value == Trend.downtrend.value) == False:
        res.weak_correct_trend = False
    elif (previous_trend.value == Trend.weak_down_trend.value) == True:
        res.errors.append ("Weak downtrend.")

    if is_pattern_trend_same_as_market_trend (res.action, market_trend) == False:
        res.pattern_trend_same_as_market_trend = False
        res.errors.append ("Stock action:" + str (res.action) + " not same as market trend:" + str (market_trend.name))

    if (util.not_too_long_or_short_candle (current_day_data, stock_data[-11:-1])) == False:
        append_too_long_or_short_candle_error (res, current_day_data)
        res.correct_candle_length = False

    if (util.not_too_long_or_short_candle (previous_day_data, stock_data[-12:-2])) == False:
        append_too_long_or_short_candle_error (res, previous_day_data)
        res.correct_candle_length = False

    if (current_day_current_price > current_day_data['open']) == False:
        res.errors.append (
            "Current day cp:" + str (current_day_current_price) + " not greater than current day open:" + str (
                current_day_data['open']))
        res.pattern_match = False

    if (previous_day_data['close'] < previous_day_data['open']) == False:
        res.errors.append (
            "Previous day close:" + str (previous_day_data['close']) + " not less than previous day open:" + str (
                previous_day_data['open']))
        res.pattern_match = False

    previous_day_middle = previous_day_data['close'] + (previous_day_data['open'] - previous_day_data['close']) / 2

    if (current_day_current_price > previous_day_middle) == False:
        res.errors.append (
            "Current day cp:" + str (current_day_current_price) + " not greater than previous day open:" + str (
                previous_day_data['open']))
        res.pattern_match = False

    if (current_day_data['open'] < previous_day_data['close']) == False:
        res.errors.append (
            "Current day open:" + str (current_day_data['open']) + " not less than previous day close:" + str (
                previous_day_data['close']))
        res.pattern_match = False

    if is_rsi_appropriate (rsi) == False:
        append_unacceptable_rsi_error (res, rsi, lower_limit_for_rsi)
        res.correct_rsi = False

    if (rsi_14_9_period_SMA > lower_limit_for_rsi_14_9_period_SMA) == False:
        append_unacceptable_rsi_14_9_period_SMA_error (res, rsi_14_9_period_SMA, lower_limit_for_rsi_14_9_period_SMA)
        res.correct_rsi_14_9_period_SMA = False

    if is_resistance_support_appropriate (supports_resistances, lowest_low) == False:
        append_unacceptable_target_stoploss_variation_error (res, lowest_low)
        res.correct_support = False
    else:
        res.correct_support = True

    if (risk_reward_ratio >= acceptable_risk_reward_ratio) == False:
        append_low_risk_reward_ratio_error (res, risk_reward_ratio, acceptable_risk_reward_ratio,
                                            current_day_current_price, resistance, lowest_low)
        res.correct_risk_reward_ratio = False

    return res


def Recognize_Bearish_Engulfing_pattern(stock_data, supports_resistances, rsi, rsi_14_9_period_SMA, market_trend):
    last_10_day_average_volume = util.calculate_last_10_days_average_volume (stock_data[-11:-1])
    last_11_day_average_volume = util.calculate_last_10_days_average_volume (stock_data[-12:-2])
    current_day_data = stock_data[-1]
    previous_day_data = stock_data[-2]
    current_day_current_price = util.get_current_day_current_price (stock_data[-1])

    highest_high = [previous_day_data['high'], current_day_data['high']][
        current_day_data['high'] > previous_day_data['high']]

    support = find_nearest_resistance_support(supports_resistances,current_day_current_price,current_day_data['timestamp'])
    risk_reward_ratio = calculate_risk_reward_ratio_appropriate (current_day_current_price, support, highest_high)

    res = PatternRecognitionResponse ()
    res.risk_reward_ratio = risk_reward_ratio
    res.points = risk_reward_ratio + 2
    res.pattern_name = Pattern_Name.BEARISH_ENGULFING
    res.action = Action.SHORT
    res.strong_correct_trend = None
    res.weak_correct_trend = None
    res.resistance = highest_high
    res.support = support
    res.rsi = rsi
    res.rsi_14_9_period_SMA = rsi_14_9_period_SMA
    res.fetch_date = util.get_date_from_timestamp(stock_data[-1]['timestamp'])
    res.supports_resistances = supports_resistances
    res.current_day_volumes = stock_data[-1]['volume']
    res.last_10_day_average_volumes = last_10_day_average_volume
    res.current_day_current_price = current_day_current_price

    if is_volume_appropriate(current_day_data, last_10_day_average_volume) == False:
        append_low_volume_error (res, last_10_day_average_volume, current_day_data)
        res.high_volumes = False

    # if is_volume_appropriate(previous_day_data, last_11_day_average_volume) == False:
    #     append_low_volume_error (res, last_11_day_average_volume, previous_day_data)
    #     res.high_volumes = False

    # previous_trend = check_previous_trend (stock_data[-no_of_sessions_for_previous_trend - 2: -2])
    # res.previous_trend = previous_trend
    # if (previous_trend.value == Trend.uptrend.value) == False:
    #     res.errors.append (
    #         "No clear uptrend." + str (stock_data[-no_of_sessions_for_previous_trend - 3]['close']) + " : " + str (
    #             stock_data[-3]['close']))
    #     res.strong_correct_trend = False
    # else:
    #     res.weak_correct_trend = False
    #
    # if (previous_trend.value == Trend.weak_up_trend.value or previous_trend.value == Trend.uptrend.value) == False:
    #     res.weak_correct_trend = False
    # elif (previous_trend.value == Trend.weak_up_trend.value) == True:
    #     res.errors.append ("Weak uptrend.")

    if is_pattern_trend_same_as_market_trend (res.action, market_trend) == False:
        res.pattern_trend_same_as_market_trend = False
        res.errors.append ("Stock action:" + str (res.action) + " not same as market trend:" + str (market_trend.name))

    if (util.not_too_long_or_short_candle (current_day_data, stock_data[-11:-1])) == False:
        append_too_long_or_short_candle_error (res, current_day_data)
        res.correct_candle_length = False

    if (util.not_too_long_or_short_candle (previous_day_data, stock_data[-12:-2])) == False:
        append_too_long_or_short_candle_error (res, previous_day_data)
        res.correct_candle_length = False

    if (current_day_current_price < current_day_data['open']) == False:
        res.errors.append (
            "Current day cp:" + str (current_day_current_price) + " not less than current day open:" + str (
                current_day_data['open']))
        res.pattern_match = False

    if (previous_day_data['close'] > previous_day_data['open']) == False:
        res.errors.append (
            "Previous day close:" + str (previous_day_data['close']) + " not greater than previous day open:" + str (
                previous_day_data['open']))
        res.pattern_match = False

    if (current_day_current_price < previous_day_data['open']) == False:
        res.errors.append (
            "Current day cp:" + str (current_day_current_price) + " not less than previous day open:" + str (
                previous_day_data['open']))
        res.pattern_match = False

    if (current_day_data['open'] >= previous_day_data['close']) == False:
        res.errors.append (
            "Current day open:" + str (current_day_data['open']) + " not greater than previous day close:" + str (
                previous_day_data['close']))
        res.pattern_match = False

    if is_rsi_appropriate (rsi) == False:
        append_unacceptable_rsi_error (res, rsi, upper_limit_for_rsi)
        res.correct_rsi = False

    if (rsi_14_9_period_SMA < upper_limit_for_rsi_14_9_period_SMA) == False:
        append_unacceptable_rsi_14_9_period_SMA_error (res, rsi_14_9_period_SMA, upper_limit_for_rsi_14_9_period_SMA)
        res.correct_rsi_14_9_period_SMA = False

    if is_resistance_support_appropriate(supports_resistances, highest_high) == False:
        append_unacceptable_target_stoploss_variation_error(res, highest_high)
        res.correct_resistance = False
    else:
        res.correct_resistance = True

    if (risk_reward_ratio >= acceptable_risk_reward_ratio) == False:
        append_low_risk_reward_ratio_error (res, risk_reward_ratio, acceptable_risk_reward_ratio,
                                            current_day_current_price, support, highest_high)
        res.correct_risk_reward_ratio = False

    return res


def Recognize_Bearish_Piercing_pattern(stock_data, supports_resistances, rsi, rsi_14_9_period_SMA, market_trend):
    last_10_day_average_volume = util.calculate_last_10_days_average_volume (stock_data[-11:-1])
    last_11_day_average_volume = util.calculate_last_10_days_average_volume (stock_data[-12:-2])
    current_day_data = stock_data[-1]
    previous_day_data = stock_data[-2]
    current_day_current_price = util.get_current_day_current_price (stock_data[-1])

    highest_high = [previous_day_data['high'], current_day_data['high']][
        current_day_data['high'] > previous_day_data['high']]

    support = find_nearest_resistance_support (supports_resistances, current_day_current_price,
                                               current_day_data['timestamp'])
    risk_reward_ratio = calculate_risk_reward_ratio_appropriate (current_day_current_price, support, highest_high)

    res = PatternRecognitionResponse ()
    res.risk_reward_ratio = risk_reward_ratio
    res.points = risk_reward_ratio + 2
    res.pattern_name = Pattern_Name.BEARISH_PIERCING_PATTERN
    res.action = Action.SHORT
    res.resistance = highest_high
    res.support = support
    res.rsi = rsi
    res.rsi_14_9_period_SMA = rsi_14_9_period_SMA
    res.fetch_date = util.get_date_from_timestamp (stock_data[-1]['timestamp'])
    res.supports_resistances = supports_resistances
    res.current_day_volumes = stock_data[-1]['volume']
    res.last_10_day_average_volumes = last_10_day_average_volume
    res.current_day_current_price = current_day_current_price

    if is_volume_appropriate (current_day_data, last_10_day_average_volume) == False:
        append_low_volume_error (res, last_10_day_average_volume, current_day_data)
        res.high_volumes = False

    # if is_volume_appropriate(previous_day_data, last_11_day_average_volume) == False:
    #     append_low_volume_error (res, last_11_day_average_volume, previous_day_data)
    #     res.high_volumes = False

    previous_trend = check_previous_trend (stock_data[-no_of_sessions_for_previous_trend - 2: -2])
    res.previous_trend = previous_trend
    if (previous_trend.value == Trend.uptrend.value) == False:
        res.errors.append (
            "No clear uptrend." + str (stock_data[-no_of_sessions_for_previous_trend - 3]['close']) + " : " + str (
                stock_data[-3]['close']))
        res.strong_correct_trend = False
    else:
        res.weak_correct_trend = False

    if (previous_trend.value == Trend.weak_up_trend.value or previous_trend.value == Trend.uptrend.value) == False:
        res.weak_correct_trend = False
    elif (previous_trend.value == Trend.weak_up_trend.value) == True:
        res.errors.append ("Weak uptrend.")

    if is_pattern_trend_same_as_market_trend (res.action, market_trend) == False:
        res.pattern_trend_same_as_market_trend = False
        res.errors.append ("Stock action:" + str (res.action) + " not same as market trend:" + str (market_trend.name))

    if (util.not_too_long_or_short_candle (current_day_data, stock_data[-11:-1])) == False:
        append_too_long_or_short_candle_error (res, current_day_data)
        res.correct_candle_length = False

    if (util.not_too_long_or_short_candle (previous_day_data, stock_data[-12:-2])) == False:
        append_too_long_or_short_candle_error (res, previous_day_data)
        res.correct_candle_length = False

    if (current_day_current_price < current_day_data['open']) == False:
        res.errors.append (
            "Current day cp:" + str (current_day_current_price) + " not less than current day open:" + str (
                current_day_data['open']))
        res.pattern_match = False

    if (previous_day_data['close'] > previous_day_data['open']) == False:
        res.errors.append (
            "Previous day close:" + str (previous_day_data['close']) + " not greater than previous day open:" + str (
                previous_day_data['open']))
        res.pattern_match = False

    previous_day_middle = previous_day_data['open'] + (previous_day_data['close'] - previous_day_data['open']) / 2

    if (current_day_current_price < previous_day_middle) == False:
        res.errors.append (
            "Current day cp:" + str (current_day_current_price) + " not less than previous day middle:" + str (
                previous_day_middle))
        res.pattern_match = False

    if (current_day_data['open'] > previous_day_data['close']) == False:
        res.errors.append (
            "Current day open:" + str (current_day_data['open']) + " not greater than previous day close:" + str (
                previous_day_data['close']))
        res.pattern_match = False

    if is_rsi_appropriate (rsi) == False:
        append_unacceptable_rsi_error (res, rsi, upper_limit_for_rsi)
        res.correct_rsi = False

    if (rsi_14_9_period_SMA < upper_limit_for_rsi_14_9_period_SMA) == False:
        append_unacceptable_rsi_14_9_period_SMA_error (res, rsi_14_9_period_SMA, upper_limit_for_rsi_14_9_period_SMA)
        res.correct_rsi_14_9_period_SMA = False

    if is_resistance_support_appropriate (supports_resistances, highest_high) == False:
        append_unacceptable_target_stoploss_variation_error (res, highest_high)
        res.correct_resistance = False
    else:
        res.correct_resistance = True

    if (risk_reward_ratio >= acceptable_risk_reward_ratio) == False:
        append_low_risk_reward_ratio_error (res, risk_reward_ratio, acceptable_risk_reward_ratio,
                                            current_day_current_price, support, highest_high)
        res.correct_risk_reward_ratio = False

    return res


def Recognize_Bullish_Harami_pattern(stock_data, supports_resistances, rsi, rsi_14_9_period_SMA, market_trend):
    last_10_day_average_volume = util.calculate_last_10_days_average_volume (stock_data[-11:-1])
    last_11_day_average_volume = util.calculate_last_10_days_average_volume (stock_data[-12:-2])
    current_day_data = stock_data[-1]
    previous_day_data = stock_data[-2]
    current_day_current_price = util.get_current_day_current_price (stock_data[-1])

    lowest_low = [previous_day_data['low'], current_day_data['low']][current_day_data['low'] < previous_day_data['low']]

    resistance = find_nearest_resistance_support(supports_resistances,current_day_current_price,current_day_data['timestamp'], False)
    risk_reward_ratio = calculate_risk_reward_ratio_appropriate (current_day_current_price, resistance, lowest_low)

    res = PatternRecognitionResponse ()
    res.risk_reward_ratio = risk_reward_ratio
    res.points = risk_reward_ratio + 2
    res.pattern_name = Pattern_Name.BULLISH_HARAMI
    res.action = Action.LONG
    res.resistance = resistance
    res.support = lowest_low
    res.rsi = rsi
    res.rsi_14_9_period_SMA = rsi_14_9_period_SMA
    res.fetch_date = util.get_date_from_timestamp(stock_data[-1]['timestamp'])
    res.supports_resistances = supports_resistances
    res.current_day_volumes = stock_data[-1]['volume']
    res.last_10_day_average_volumes = last_10_day_average_volume
    res.current_day_current_price = current_day_current_price

    if is_volume_appropriate(current_day_data, last_10_day_average_volume) == False:
        append_low_volume_error (res, last_10_day_average_volume, current_day_data)
        res.high_volumes = False

    # if is_volume_appropriate(previous_day_data, last_11_day_average_volume) == False:
    #     append_low_volume_error (res, last_11_day_average_volume, previous_day_data)
    #     res.high_volumes = False

    previous_trend = check_previous_trend (stock_data[-no_of_sessions_for_previous_trend - 2: -2])
    res.previous_trend = previous_trend
    if (previous_trend.value == Trend.downtrend.value) == False:
        res.errors.append (
            "No clear downtrend." + str (stock_data[-no_of_sessions_for_previous_trend - 3]['close']) + " : " + str (
                stock_data[-3]['close']))
        res.strong_correct_trend = False
    else:
        res.weak_correct_trend = False

    if (previous_trend.value == Trend.weak_down_trend.value or previous_trend.value == Trend.downtrend.value) == False:
        res.weak_correct_trend = False
    elif (previous_trend.value == Trend.weak_down_trend.value) == True:
        res.errors.append ("Weak downtrend.")

    if is_pattern_trend_same_as_market_trend (res.action, market_trend) == False:
        res.pattern_trend_same_as_market_trend = False
        res.errors.append ("Stock action:" + str (res.action) + " not same as market trend:" + str (market_trend.name))

    if (util.not_too_long_or_short_candle (current_day_data, stock_data[-11:-1])) == False:
        append_too_long_or_short_candle_error (res, current_day_data)
        res.correct_candle_length = False

    if (util.not_too_long_or_short_candle (previous_day_data, stock_data[-12:-2])) == False:
        append_too_long_or_short_candle_error (res, previous_day_data)
        res.correct_candle_length = False

    if (current_day_current_price > current_day_data['open']) == False:
        res.errors.append (
            "Current day cp:" + str (current_day_current_price) + " not greater than current day open:" + str (
                current_day_data['open']))
        res.pattern_match = False

    if (previous_day_data['close'] < previous_day_data['open']) == False:
        res.errors.append (
            "Previous day close:" + str (previous_day_data['close']) + " not less than previous day open:" + str (
                previous_day_data['open']))
        res.pattern_match = False

    if (current_day_current_price < previous_day_data['open']) == False:
        res.errors.append (
            "Current day cp:" + str (current_day_current_price) + " not less than previous day open:" + str (
                previous_day_data['open']))
        res.pattern_match = False

    if (current_day_data['open'] >= previous_day_data['close']) == False:
        res.errors.append (
            "Current day open:" + str (current_day_data['open']) + " not greater than previous day close:" + str (
                previous_day_data['close']))
        res.pattern_match = False

    if is_rsi_appropriate (rsi) == False:
        append_unacceptable_rsi_error (res, rsi, lower_limit_for_rsi)
        res.correct_rsi = False

    if (rsi_14_9_period_SMA > lower_limit_for_rsi_14_9_period_SMA) == False:
        append_unacceptable_rsi_14_9_period_SMA_error (res, rsi_14_9_period_SMA, lower_limit_for_rsi_14_9_period_SMA)
        res.correct_rsi_14_9_period_SMA = False

    if is_resistance_support_appropriate(supports_resistances, lowest_low) == False:
        append_unacceptable_target_stoploss_variation_error(res, lowest_low)
        res.correct_support = False
    else:
        res.correct_support = True

    if (risk_reward_ratio >= acceptable_risk_reward_ratio) == False:
        append_low_risk_reward_ratio_error (res, risk_reward_ratio, acceptable_risk_reward_ratio,
                                            current_day_current_price, resistance, lowest_low)
        res.correct_risk_reward_ratio = False

    return res


def Recognize_Bearish_Harami_pattern(stock_data, supports_resistances, rsi, rsi_14_9_period_SMA, market_trend):
    last_10_day_average_volume = util.calculate_last_10_days_average_volume (stock_data[-11:-1])
    last_11_day_average_volume = util.calculate_last_10_days_average_volume (stock_data[-12:-2])
    current_day_data = stock_data[-1]
    previous_day_data = stock_data[-2]
    current_day_current_price = util.get_current_day_current_price (stock_data[-1])

    highest_high = [previous_day_data['high'], current_day_data['high']][
        current_day_data['high'] > previous_day_data['high']]

    support = find_nearest_resistance_support(supports_resistances,current_day_current_price,current_day_data['timestamp'])
    risk_reward_ratio = calculate_risk_reward_ratio_appropriate (current_day_current_price, support, highest_high)

    res = PatternRecognitionResponse ()
    res.risk_reward_ratio = risk_reward_ratio
    res.points = risk_reward_ratio + 2
    res.pattern_name = Pattern_Name.BEARISH_HARAMI
    res.action = Action.SHORT
    res.resistance = highest_high
    res.support = support
    res.rsi = rsi
    res.rsi_14_9_period_SMA = rsi_14_9_period_SMA
    res.fetch_date = util.get_date_from_timestamp(stock_data[-1]['timestamp'])
    res.supports_resistances = supports_resistances
    res.current_day_volumes = stock_data[-1]['volume']
    res.last_10_day_average_volumes = last_10_day_average_volume
    res.current_day_current_price = current_day_current_price

    if is_volume_appropriate(current_day_data, last_10_day_average_volume) == False:
        append_low_volume_error (res, last_10_day_average_volume, current_day_data)
        res.high_volumes = False

    # if is_volume_appropriate(previous_day_data, last_11_day_average_volume) == False:
    #     append_low_volume_error (res, last_11_day_average_volume, previous_day_data)
    #     res.high_volumes = False

    previous_trend = check_previous_trend (stock_data[-no_of_sessions_for_previous_trend - 2: -2])
    res.previous_trend = previous_trend
    if (previous_trend.value == Trend.uptrend.value) == False:
        res.errors.append (
            "No clear uptrend." + str (stock_data[-no_of_sessions_for_previous_trend - 3]['close']) + " : " + str (
                stock_data[-3]['close']))
        res.strong_correct_trend = False
    else:
        res.weak_correct_trend = False

    if (previous_trend.value == Trend.weak_up_trend.value or previous_trend.value == Trend.uptrend.value) == False:
        res.weak_correct_trend = False
    elif (previous_trend.value == Trend.weak_up_trend.value) == True:
        res.errors.append ("Weak uptrend.")

    if is_pattern_trend_same_as_market_trend (res.action, market_trend) == False:
        res.pattern_trend_same_as_market_trend = False
        res.errors.append ("Stock action:" + str (res.action) + " not same as market trend:" + str (market_trend.name))

    if (util.not_too_long_or_short_candle (current_day_data, stock_data[-11:-1])) == False:
        append_too_long_or_short_candle_error (res, current_day_data)
        res.correct_candle_length = False

    if (util.not_too_long_or_short_candle (previous_day_data, stock_data[-12:-2])) == False:
        append_too_long_or_short_candle_error (res, previous_day_data)
        res.correct_candle_length = False

    if (current_day_current_price < current_day_data['open']) == False:
        res.errors.append (
            "Current day cp:" + str (current_day_current_price) + " not less than current day open:" + str (
                current_day_data['open']))
        res.pattern_match = False

    if (previous_day_data['close'] > previous_day_data['open']) == False:
        res.errors.append (
            "Previous day cp:" + str (previous_day_data['close']) + " not greater than previous day open:" + str (
                previous_day_data['open']))
        res.pattern_match = False

    if (current_day_current_price > previous_day_data['open']) == False:
        res.errors.append (
            "Current day cp:" + str (current_day_current_price) + " not greater than previous day open:" + str (
                previous_day_data['open']))
        res.pattern_match = False

    if (current_day_data['open'] < previous_day_data['close']) == False:
        res.errors.append (
            "Current day open:" + str (current_day_data['open']) + " not less than previous day close:" + str (
                previous_day_data['close']))
        res.pattern_match = False

    if is_rsi_appropriate (rsi) == False:
        append_unacceptable_rsi_error (res, rsi, upper_limit_for_rsi)
        res.correct_rsi = False

    if (rsi_14_9_period_SMA < upper_limit_for_rsi_14_9_period_SMA) == False:
        append_unacceptable_rsi_14_9_period_SMA_error (res, rsi_14_9_period_SMA, upper_limit_for_rsi_14_9_period_SMA)
        res.correct_rsi_14_9_period_SMA = False

    if is_resistance_support_appropriate(supports_resistances, highest_high) == False:
        append_unacceptable_target_stoploss_variation_error(res, highest_high)
        res.correct_resistance = False
    else:
        res.correct_resistance = True

    if (risk_reward_ratio >= acceptable_risk_reward_ratio) == False:
        append_low_risk_reward_ratio_error (res, risk_reward_ratio, acceptable_risk_reward_ratio,
                                            current_day_current_price, support, highest_high)
        res.correct_risk_reward_ratio = False

    return res


def Recognize_Morning_Star_pattern(stock_data, supports_resistances, rsi, rsi_14_9_period_SMA, market_trend):
    last_10_day_average_volume = util.calculate_last_10_days_average_volume (stock_data[-11:-1])
    last_11_day_average_volume = util.calculate_last_10_days_average_volume (stock_data[-12:-2])
    last_12_day_average_volume = util.calculate_last_10_days_average_volume (stock_data[-13:-3])
    current_day_data = stock_data[-1]
    previous_day_data = stock_data[-2]
    previous_to_previous_day_data = stock_data[-3]
    current_day_current_price = util.get_current_day_current_price (stock_data[-1])

    lowest_low_prelim = [previous_to_previous_day_data['low'], previous_day_data['low']][
        previous_day_data['low'] < previous_to_previous_day_data['low']]
    lowest_low_final = [current_day_data['low'], lowest_low_prelim][lowest_low_prelim < current_day_data['low']]

    resistance = find_nearest_resistance_support(supports_resistances,current_day_current_price,current_day_data['timestamp'], False)
    risk_reward_ratio = calculate_risk_reward_ratio_appropriate (current_day_current_price, resistance, lowest_low_final)

    res = PatternRecognitionResponse ()
    res.risk_reward_ratio = risk_reward_ratio
    res.points = risk_reward_ratio + 3
    res.pattern_name = Pattern_Name.MORNING_STAR
    res.action = Action.LONG
    res.resistance = resistance
    res.support = lowest_low_final
    res.rsi = rsi
    res.rsi_14_9_period_SMA = rsi_14_9_period_SMA
    res.fetch_date = util.get_date_from_timestamp(stock_data[-1]['timestamp'])
    res.supports_resistances = supports_resistances
    res.current_day_volumes = stock_data[-1]['volume']
    res.last_10_day_average_volumes = last_10_day_average_volume
    res.current_day_current_price = current_day_current_price

    if is_volume_appropriate(current_day_data, last_10_day_average_volume) == False:
        append_low_volume_error (res, last_10_day_average_volume, current_day_data)
        res.high_volumes = False

    # if is_volume_appropriate(previous_day_data, last_11_day_average_volume) == False:
    #     append_low_volume_error (res, last_11_day_average_volume, previous_day_data)
    #     res.high_volumes = False
    #
    # if is_volume_appropriate(previous_to_previous_day_data, last_12_day_average_volume) == False:
    #     append_low_volume_error (res, last_12_day_average_volume, previous_to_previous_day_data)
    #     res.high_volumes = False

    previous_trend = check_previous_trend (stock_data[-no_of_sessions_for_previous_trend - 3: -3])
    res.previous_trend = previous_trend
    if (previous_trend.value == Trend.downtrend.value) == False:
        res.errors.append (
            "No clear downtrend." + str (stock_data[-no_of_sessions_for_previous_trend - 4]['close']) + " : " + str (
                stock_data[-4]['close']))
        res.strong_correct_trend = False
    else:
        res.weak_correct_trend = False

    if (previous_trend.value == Trend.weak_down_trend.value or previous_trend.value == Trend.downtrend.value) == False:
        res.weak_correct_trend = False
    elif (previous_trend.value == Trend.weak_down_trend.value) == True:
        res.errors.append ("Weak downtrend.")

    if is_pattern_trend_same_as_market_trend (res.action, market_trend) == False:
        res.pattern_trend_same_as_market_trend = False
        res.errors.append ("Stock action:" + str (res.action) + " not same as market trend:" + str (market_trend.name))

    if (util.not_too_long_or_short_candle (current_day_data, stock_data[-11:-1])) == False:
        append_too_long_or_short_candle_error (res, current_day_data)
        res.correct_candle_length = False

    if (util.not_too_long_or_short_candle (previous_day_data, stock_data[-12:-2])) == False:
        append_too_long_or_short_candle_error (res, previous_day_data)
        res.correct_candle_length = False

    if (util.not_too_long_or_short_candle (previous_to_previous_day_data, stock_data[-13:-3])) == False:
        append_too_long_or_short_candle_error (res, previous_to_previous_day_data)
        res.correct_candle_length = False

    if (current_day_current_price > current_day_data['open']) == False:
        res.errors.append (
            "Current day cp:" + str (current_day_current_price) + " not greater than current day open:" + str (
                current_day_data['open']))
        res.pattern_match = False

    # if util.nearly_equal (previous_day_data['close'], previous_day_data['open'], high_low_variation_percent) == False:
    #     res.errors.append (
    #         "Previous day close:" + str (previous_day_data['close']) + " not nearly equal to previous day open:" + str (
    #             previous_day_data['open']))
    #     res.pattern_match = False

    if (previous_to_previous_day_data['close'] < previous_to_previous_day_data['open']) == False:
        res.errors.append ("Previous to previous day close:" + str (
            previous_to_previous_day_data['close']) + " not less than previous to previous day open:" + str (
            previous_to_previous_day_data['open']))
        res.pattern_match = False

    previous_to_previous_day_middle = previous_to_previous_day_data['close'] + (
                previous_to_previous_day_data['open'] - previous_to_previous_day_data['close']) / 2

    if (previous_day_data['close'] < previous_to_previous_day_middle) == False:
        res.errors.append ("Previous day close::" + str (
            previous_day_data['close']) + " not less than previous to previous day middle:" + str (
            previous_to_previous_day_middle))
        res.pattern_match = False

    if (previous_day_data['open'] < previous_to_previous_day_middle) == False:
        res.errors.append ("Previous day open:" + str (
            previous_day_data['open']) + " not less than previous to previous day middle:" + str (
            previous_to_previous_day_middle))
        res.pattern_match = False

    if (current_day_current_price > previous_to_previous_day_data['open']) == False:
        res.errors.append ("Current day cp::" + str (
            current_day_current_price) + " not greater than previous to previous day open:" + str (
            previous_to_previous_day_data['open']))
        res.pattern_match = False

    current_day_middle = current_day_data['open'] + (current_day_data['close'] - current_day_data['open']) / 2

    if (current_day_middle > previous_day_data['close']) == False:
        res.errors.append (
            "Current day middle:" + str (current_day_middle) + " not greater than previous day close:" + str (
                previous_day_data['close']))
        res.pattern_match = False

    if (current_day_middle > previous_day_data['open']) == False:
        res.errors.append (
            "Current day middle:" + str (current_day_middle) + " not greater than previous day open:" + str (
                previous_day_data['open']))
        res.pattern_match = False

    if is_rsi_appropriate (rsi) == False:
        append_unacceptable_rsi_error (res, rsi, lower_limit_for_rsi)
        res.correct_rsi = False

    if (rsi_14_9_period_SMA > lower_limit_for_rsi_14_9_period_SMA) == False:
        append_unacceptable_rsi_14_9_period_SMA_error (res, rsi_14_9_period_SMA, lower_limit_for_rsi_14_9_period_SMA)
        res.correct_rsi_14_9_period_SMA = False

    if is_resistance_support_appropriate(supports_resistances, lowest_low_final) == False:
        append_unacceptable_target_stoploss_variation_error(res, lowest_low_final)
        res.correct_support = False
    else:
        res.correct_support = True

    if (risk_reward_ratio >= acceptable_risk_reward_ratio) == False:
        append_low_risk_reward_ratio_error (res, risk_reward_ratio, acceptable_risk_reward_ratio,
                                            current_day_current_price, resistance, lowest_low_final)
        res.correct_risk_reward_ratio = False

    return res


def Recognize_Evening_Star_pattern(stock_data, supports_resistances, rsi, rsi_14_9_period_SMA, market_trend):
    last_10_day_average_volume = util.calculate_last_10_days_average_volume (stock_data[-11:-1])
    last_11_day_average_volume = util.calculate_last_10_days_average_volume (stock_data[-12:-2])
    last_12_day_average_volume = util.calculate_last_10_days_average_volume (stock_data[-13:-3])
    current_day_data = stock_data[-1]
    previous_day_data = stock_data[-2]
    previous_to_previous_day_data = stock_data[-3]
    current_day_current_price = util.get_current_day_current_price (stock_data[-1])

    highest_high_premlin = [previous_day_data['high'], previous_to_previous_day_data['high']][
        previous_to_previous_day_data['high'] > previous_day_data['high']]
    highest_high_final = [current_day_data['high'], highest_high_premlin][highest_high_premlin > current_day_data['high']]

    support = find_nearest_resistance_support(supports_resistances,current_day_current_price,current_day_data['timestamp'])
    risk_reward_ratio = calculate_risk_reward_ratio_appropriate (current_day_current_price, support, highest_high_final)

    res = PatternRecognitionResponse ()
    res.risk_reward_ratio = risk_reward_ratio
    res.points = risk_reward_ratio + 3
    res.pattern_name = Pattern_Name.EVENING_STAR
    res.action = Action.SHORT
    res.resistance = highest_high_final
    res.support = support
    res.rsi = rsi
    res.rsi_14_9_period_SMA = rsi_14_9_period_SMA
    res.fetch_date = util.get_date_from_timestamp(stock_data[-1]['timestamp'])
    res.supports_resistances = supports_resistances
    res.current_day_volumes = stock_data[-1]['volume']
    res.last_10_day_average_volumes = last_10_day_average_volume
    res.current_day_current_price = current_day_current_price

    if is_volume_appropriate(current_day_data, last_10_day_average_volume) == False:
        append_low_volume_error (res, last_10_day_average_volume, current_day_data)
        res.high_volumes = False

    # if is_volume_appropriate(previous_day_data, last_11_day_average_volume) == False:
    #     append_low_volume_error (res, last_11_day_average_volume, previous_day_data)
    #     res.high_volumes = False
    #
    # if is_volume_appropriate(previous_to_previous_day_data, last_12_day_average_volume) == False:
    #     append_low_volume_error (res, last_12_day_average_volume, previous_to_previous_day_data)
    #     res.high_volumes = False

    previous_trend = check_previous_trend (stock_data[-no_of_sessions_for_previous_trend - 3: -3])
    res.previous_trend = previous_trend
    if (previous_trend.value == Trend.uptrend.value) == False:
        res.errors.append (
            "No clear uptrend." + str (stock_data[-no_of_sessions_for_previous_trend - 4]['close']) + " : " + str (
                stock_data[-4]['close']))
        res.strong_correct_trend = False
    else:
        res.weak_correct_trend = False

    if (previous_trend.value == Trend.weak_up_trend.value or previous_trend.value == Trend.uptrend.value) == False:
        res.weak_correct_trend = False
    elif (previous_trend.value == Trend.weak_up_trend.value) == True:
        res.errors.append ("Weak uptrend.")

    if is_pattern_trend_same_as_market_trend (res.action, market_trend) == False:
        res.pattern_trend_same_as_market_trend = False
        res.errors.append ("Stock action:" + str (res.action) + " not same as market trend:" + str (market_trend.name))

    if (util.not_too_long_or_short_candle (current_day_data, stock_data[-11:-1])) == False:
        append_too_long_or_short_candle_error (res, current_day_data)
        res.correct_candle_length = False

    if (util.not_too_long_or_short_candle (previous_day_data, stock_data[-12:-2])) == False:
        append_too_long_or_short_candle_error (res, previous_day_data)
        res.correct_candle_length = False

    if (util.not_too_long_or_short_candle (previous_to_previous_day_data, stock_data[-13:-3])) == False:
        append_too_long_or_short_candle_error (res, previous_to_previous_day_data)
        res.correct_candle_length = False

    if (current_day_current_price < current_day_data['open']) == False:
        res.errors.append (
            "Current day cp:" + str (current_day_current_price) + " not less than current day open:" + str (
                current_day_data['open']))
        res.pattern_match = False

    # if util.nearly_equal (previous_day_data['close'], previous_day_data['open'], high_low_variation_percent) == False:
    #     res.errors.append (
    #         "Previous day close:" + str (previous_day_data['close']) + " not nearly equal to previous day open:" + str (
    #             previous_day_data['open']))
    #     res.pattern_match = False

    if (previous_to_previous_day_data['close'] > previous_to_previous_day_data['open']) == False:
        res.errors.append ("Previous to previous day close:" + str (previous_to_previous_day_data['close']) +
                           " not greater than previous to previous day open:" + str (
            previous_to_previous_day_data['open']))
        res.pattern_match = False

    previous_to_previous_day_middle = previous_to_previous_day_data['open'] + (
            previous_to_previous_day_data['close'] - previous_to_previous_day_data['open']) / 2

    if (previous_day_data['close'] > previous_to_previous_day_middle) == False:
        res.errors.append ("Previous day close::" + str (
            previous_day_data['close']) + " not greater than previous to previous day close:" + str (
            previous_to_previous_day_data['close']))
        res.pattern_match = False

    if (previous_day_data['open'] > previous_to_previous_day_middle) == False:
        res.errors.append ("Previous day open:" + str (
            previous_day_data['open']) + " not greater than previous to previous day close:" + str (
            previous_to_previous_day_data['close']))
        res.pattern_match = False

    if (current_day_current_price < previous_to_previous_day_data['open']) == False:
        res.errors.append ("Current day cp::" + str (
            current_day_current_price) + " not less than previous to previous day open:" + str (
            previous_to_previous_day_data['open']))
        res.pattern_match = False

    current_day_middle = current_day_data['close'] + (current_day_data['open'] - current_day_data['close']) / 2

    if (current_day_middle < previous_day_data['close']) == False:
        res.errors.append (
            "Current day open:" + str (current_day_data['open']) + " not less than previous day close:" + str (
                previous_day_data['close']))
        res.pattern_match = False

    if (current_day_middle < previous_day_data['open']) == False:
        res.errors.append (
            "Current day open:" + str (current_day_data['open']) + " not less than previous day open:" + str (
                previous_day_data['open']))
        res.pattern_match = False

    if is_rsi_appropriate (rsi) == False:
        append_unacceptable_rsi_error (res, rsi, upper_limit_for_rsi)
        res.correct_rsi = False

    if (rsi_14_9_period_SMA < upper_limit_for_rsi_14_9_period_SMA) == False:
        append_unacceptable_rsi_14_9_period_SMA_error (res, rsi_14_9_period_SMA, upper_limit_for_rsi_14_9_period_SMA)
        res.correct_rsi_14_9_period_SMA = False

    if is_resistance_support_appropriate(supports_resistances, highest_high_final) == False:
        append_unacceptable_target_stoploss_variation_error(res, highest_high_final)
        res.correct_resistance = False
    else:
        res.correct_resistance = True

    if (risk_reward_ratio >= acceptable_risk_reward_ratio) == False:
        append_low_risk_reward_ratio_error (res, risk_reward_ratio, acceptable_risk_reward_ratio,
                                            current_day_current_price, support, highest_high_final)
        res.correct_risk_reward_ratio = False

    return res


def Recognize_Uptrend_pattern(stock_data, supports_resistances, rsi, rsi_14_9_period_SMA, market_trend):
    last_10_day_average_volume = util.calculate_last_10_days_average_volume (stock_data[-11:-1])
    last_11_day_average_volume = util.calculate_last_10_days_average_volume (stock_data[-12:-2])
    current_day_data = stock_data[-1]
    previous_day_data = stock_data[-2]
    current_day_current_price = util.get_current_day_current_price (stock_data[-1])

    highest_high = [previous_day_data['high'], current_day_data['high']][
        current_day_data['high'] > previous_day_data['high']]

    support = find_nearest_resistance_support (supports_resistances, current_day_current_price,
                                               current_day_data['timestamp'])
    risk_reward_ratio = calculate_risk_reward_ratio_appropriate (current_day_current_price, support, highest_high)

    res = PatternRecognitionResponse ()
    res.risk_reward_ratio = risk_reward_ratio
    res.points = risk_reward_ratio + 2
    res.pattern_name = Pattern_Name.UPTREND
    res.action = Action.LONG
    res.resistance = highest_high
    res.support = support
    res.rsi = rsi
    res.strong_correct_trend = None
    res.weak_correct_trend = None
    res.rsi_14_9_period_SMA = rsi_14_9_period_SMA
    res.fetch_date = util.get_date_from_timestamp (stock_data[-1]['timestamp'])
    res.supports_resistances = supports_resistances
    res.current_day_volumes = stock_data[-1]['volume']
    res.last_10_day_average_volumes = last_10_day_average_volume
    res.current_day_current_price = current_day_current_price

    if is_volume_appropriate (current_day_data, last_10_day_average_volume) == False:
        append_low_volume_error (res, last_10_day_average_volume, current_day_data)
        res.high_volumes = False

    # if is_volume_appropriate(previous_day_data, last_11_day_average_volume) == False:
    #     append_low_volume_error (res, last_11_day_average_volume, previous_day_data)
    #     res.high_volumes = False


    if is_pattern_trend_same_as_market_trend (res.action, market_trend) == False:
        res.pattern_trend_same_as_market_trend = False
        res.errors.append ("Stock action:" + str (res.action) + " not same as market trend:" + str (market_trend.name))

    if (util.not_too_long_or_short_candle (current_day_data, stock_data[-11:-1])) == False:
        append_too_long_or_short_candle_error (res, current_day_data)
        res.correct_candle_length = False

    if (util.not_too_long_or_short_candle (previous_day_data, stock_data[-12:-2])) == False:
        append_too_long_or_short_candle_error (res, previous_day_data)
        res.correct_candle_length = False

    if (current_day_current_price > current_day_data['open']) == False:
        res.errors.append (
            "Current day cp:" + str (current_day_current_price) + " not greater than current day open:" + str (
                current_day_data['open']))
        res.pattern_match = False

    if (previous_day_data['close'] > previous_day_data['open']) == False:
        res.errors.append (
            "Previous day close:" + str (previous_day_data['close']) + " not greater than previous day open:" + str (
                previous_day_data['open']))
        res.pattern_match = False

    if (current_day_data['open'] > previous_day_data['open']) == False:
        res.errors.append (
            "Current day open:" + str(current_day_data['open']) + " not greater than previous day open:" + str(
                previous_day_data['open']))
        res.pattern_match = False

    if is_rsi_appropriate (rsi) == False:
        append_unacceptable_rsi_error (res, rsi, upper_limit_for_rsi)
        res.correct_rsi = False

    if (rsi_14_9_period_SMA < upper_limit_for_rsi_14_9_period_SMA) == False:
        append_unacceptable_rsi_14_9_period_SMA_error (res, rsi_14_9_period_SMA, upper_limit_for_rsi_14_9_period_SMA)
        res.correct_rsi_14_9_period_SMA = False

    if is_resistance_support_appropriate (supports_resistances, highest_high) == False:
        append_unacceptable_target_stoploss_variation_error (res, highest_high)
        res.correct_resistance = False
    else:
        res.correct_resistance = True

    if (risk_reward_ratio >= acceptable_risk_reward_ratio) == False:
        append_low_risk_reward_ratio_error (res, risk_reward_ratio, acceptable_risk_reward_ratio,
                                            current_day_current_price, support, highest_high)
        res.correct_risk_reward_ratio = False

    return res


def Recognize_Downtrend_pattern(stock_data, supports_resistances, rsi, rsi_14_9_period_SMA, market_trend):
    last_10_day_average_volume = util.calculate_last_10_days_average_volume (stock_data[-11:-1])
    last_11_day_average_volume = util.calculate_last_10_days_average_volume (stock_data[-12:-2])
    current_day_data = stock_data[-1]
    previous_day_data = stock_data[-2]
    current_day_current_price = util.get_current_day_current_price (stock_data[-1])

    highest_high = [previous_day_data['high'], current_day_data['high']][
        current_day_data['high'] > previous_day_data['high']]

    support = find_nearest_resistance_support (supports_resistances, current_day_current_price,
                                               current_day_data['timestamp'])
    risk_reward_ratio = calculate_risk_reward_ratio_appropriate (current_day_current_price, support, highest_high)

    res = PatternRecognitionResponse ()
    res.risk_reward_ratio = risk_reward_ratio
    res.points = risk_reward_ratio + 2
    res.pattern_name = Pattern_Name.DOWNTREND
    res.action = Action.SHORT
    res.resistance = highest_high
    res.strong_correct_trend = None
    res.weak_correct_trend = None
    res.support = support
    res.rsi = rsi
    res.rsi_14_9_period_SMA = rsi_14_9_period_SMA
    res.fetch_date = util.get_date_from_timestamp (stock_data[-1]['timestamp'])
    res.supports_resistances = supports_resistances
    res.current_day_volumes = stock_data[-1]['volume']
    res.last_10_day_average_volumes = last_10_day_average_volume
    res.current_day_current_price = current_day_current_price

    if is_volume_appropriate (current_day_data, last_10_day_average_volume) == False:
        append_low_volume_error (res, last_10_day_average_volume, current_day_data)
        res.high_volumes = False

    # if is_volume_appropriate(previous_day_data, last_11_day_average_volume) == False:
    #     append_low_volume_error (res, last_11_day_average_volume, previous_day_data)
    #     res.high_volumes = False

    if is_pattern_trend_same_as_market_trend (res.action, market_trend) == False:
        res.pattern_trend_same_as_market_trend = False
        res.errors.append ("Stock action:" + str (res.action) + " not same as market trend:" + str (market_trend.name))

    if (util.not_too_long_or_short_candle (current_day_data, stock_data[-11:-1])) == False:
        append_too_long_or_short_candle_error (res, current_day_data)
        res.correct_candle_length = False

    if (util.not_too_long_or_short_candle (previous_day_data, stock_data[-12:-2])) == False:
        append_too_long_or_short_candle_error (res, previous_day_data)
        res.correct_candle_length = False

    if (current_day_current_price < current_day_data['open']) == False:
        res.errors.append (
            "Current day cp:" + str (current_day_current_price) + " not less than current day open:" + str (
                current_day_data['open']))
        res.pattern_match = False

    if (previous_day_data['close'] < previous_day_data['open']) == False:
        res.errors.append (
            "Previous day close:" + str (previous_day_data['close']) + " not less than previous day open:" + str (
                previous_day_data['open']))
        res.pattern_match = False

    if (current_day_data['open'] < previous_day_data['open']) == False:
        res.errors.append (
            "Current day open:" + str(current_day_data['open']) + " not less than previous day open:" + str(
                previous_day_data['open']))
        res.pattern_match = False

    if is_rsi_appropriate (rsi) == False:
        append_unacceptable_rsi_error (res, rsi, upper_limit_for_rsi)
        res.correct_rsi = False

    if (rsi_14_9_period_SMA < upper_limit_for_rsi_14_9_period_SMA) == False:
        append_unacceptable_rsi_14_9_period_SMA_error (res, rsi_14_9_period_SMA, upper_limit_for_rsi_14_9_period_SMA)
        res.correct_rsi_14_9_period_SMA = False

    if is_resistance_support_appropriate (supports_resistances, highest_high) == False:
        append_unacceptable_target_stoploss_variation_error (res, highest_high)
        res.correct_resistance = False
    else:
        res.correct_resistance = True

    if (risk_reward_ratio >= acceptable_risk_reward_ratio) == False:
        append_low_risk_reward_ratio_error (res, risk_reward_ratio, acceptable_risk_reward_ratio,
                                            current_day_current_price, support, highest_high)
        res.correct_risk_reward_ratio = False

    return res
