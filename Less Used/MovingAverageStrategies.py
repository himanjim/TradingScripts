import enum

import Indicators as ind
import Utils as util

min_no_of_values_to_scan_for_slope=5
min_no_of_values_to_scan_for_divergence=5
min_no_of_values_to_scan_for_89_ema_trend=5
min_average_volume_to_consider=500000
min_no_of_ma_values_to_append=2


class Slope (enum.Enum):
    UP = 1
    DOWN = -1


class Trend (enum.Enum):
    uptrend = 1
    notrend=0
    downtrend = -1


class MA_Strategy_Name (enum.Enum):
    _13_21_34_DAY_EMA = (0, 200)
    _50_DAY_EMA_WITH_100_DAY_EMA = (1,450)
    _100_DAY_EMA_WITH_200_DAY_EMA= (2,1200)
    IDENTIFY_LONG_TERM_STOCK_BEFORE_RALLY = (3,1200)
    GUPPY_MULTIPLE_MOVING_AVERAGE_INDICATOR = (4,300)
    _50_WEEK_SMA_20_DAY_RSI = (5,1500)
    MOVING_AVERAGE_RIBBON = (6, 200)

    def __init__(self, index, max_session_size):
        self.index = index
        self.max_session_size = max_session_size

class MASResponse:
    def __init__(self):
        self.stock_id = None
        self.ma_strategy_name=None
        self.sma_greater_than_lma = True
        self.prev_sma_less_than_lma = True
        self.high_volumes = True
        self.stock_price_greater_than_mas = True
        self.stock_price_appropriately_placed_between_yearly_highs_lows = None
        self.sma_high_slope = True
        self.lma_high_slope = True
        self.mas_diverging = None
        self.days_back_when_stock_price_less_than_sma = 9999
        self.macd_greater_than_9_day_ema = True
        self.macd_high_slope = True
        self.sma=[]
        self.lma=[]
        self.average_volume=None
        self.macd = []
        self.stoploss = None
        self.rsi = None
        self.correct_rsi = None
        self.trend = None
        self.correct_trend = True
        self._9_day_macd_ema = []
        self.current_day_current_price=None
        self.fetch_date = None
        self.fetched_dataset = []
        self.errors = []


    def __str__(self):
        res_str = ("\nMASResponse start------------\n")
        res_str += "Stock id:" + str (self.stock_id) + '\n'
        res_str += "MA strategy name:" + str (self.ma_strategy_name) + '\n'

        if(len(self.errors)>0):
            res_str += "Errors@@@@@@@@@@@@@@@@@@@@@@@@@@@:\n"
            for error in self.errors:
                res_str += str (error) + "\n"
            res_str += "Errors@@@@@@@@@@@@@@@@@@@@@@@@@@@:\n"
        res_str += "Days back when stock price less than prev SMA:" + str (
            self.days_back_when_stock_price_less_than_sma) + '\n'
        res_str += "SMA greater than LMA:" + str (self.sma_greater_than_lma) + '\n'
        res_str += "Prev SMA less than LMA:" + str (self.sma_greater_than_lma) + '\n'
        res_str += "High volumes:" + str (self.high_volumes) + '\n'
        res_str += "Stock price greater than MAs:" + str (self.stock_price_greater_than_mas) + '\n'
        res_str += "Stock price appropriately placed between yearly highs lows:" + str (self.stock_price_appropriately_placed_between_yearly_highs_lows) + '\n'
        res_str += "SMA high slope:" + str (self.sma_high_slope) + '\n'
        res_str += "LMA high slope:" + str (self.lma_high_slope) + '\n'
        res_str += "MAs diverging:" + str (self.mas_diverging) + '\n'
        res_str += "MACD greater than 9 day EMA:" + str (self.macd_greater_than_9_day_ema) + '\n'
        res_str += "MACD high slope:" + str (self.macd_high_slope) + '\n'

        res_str += "SMAs(short term moving averages)############################:\n"
        for ma in self.sma:
            res_str += str (ma) + "\n"
        res_str += "SMAs(short term moving averages)############################:\n"\

        res_str += "LMAs(long term moving averages)############################:\n"
        for ma in self.lma:
            res_str += str (ma) + "\n"
        res_str += "LMAs(long term moving averages)############################:\n"

        res_str += "Average volume:" + str (self.average_volume) + '\n'

        res_str += "MACD############################:\n"
        for mcd in self.macd:
            res_str += str (mcd) + "\n"
        res_str += "MACD############################:\n"

        res_str += "Stoploss:" + str (self.stoploss) + '\n'
        res_str += "RSI:" + str (self.rsi) + '\n'
        res_str += "Correct RSI:" + str (self.correct_rsi) + '\n'
        res_str += "Trend:" + str (self.trend) + '\n'
        res_str += "Correct trend:" + str (self.correct_trend) + '\n'
        res_str += "9 day EMA:" + str (self._9_day_macd_ema) + '\n'
        res_str += "Current day current price:" + str (self.current_day_current_price) + '\n'
        # for data in self.fetched_dataset:
        #     res_str += str(data) + "\n"

        res_str += "Stock historic data(last 3 days)==============:\n"
        res_str += "Current day :" + str (self.fetched_dataset[-1]) + '\n'
        res_str += "Previous day :" + str (self.fetched_dataset[-2]) + '\n'
        res_str += "Previous to previous day :" + str (self.fetched_dataset[-3]) + '\n'
        res_str += "Stock historic data(last 3 days)==============:\n"
        res_str += "Fetch date:" + str (self.fetch_date) + '\n'

        res_str += ("MASResponse end------------\n")

        return res_str
    
    def is_strategy_tradable(self):
        # and  moving_average_strategy_response.macd[0]['MACD'][0] > 0
        return (self.sma_greater_than_lma or self.sma_greater_than_lma is None) and (
                    self.correct_rsi is None or self.correct_rsi) and self.correct_trend and (
                           self.mas_diverging is None or self.mas_diverging) and (
                           self.stock_price_appropriately_placed_between_yearly_highs_lows is None or self.stock_price_appropriately_placed_between_yearly_highs_lows) and self.stock_price_greater_than_mas and (
                           self.lma_high_slope or self.lma_high_slope is None) and (
                           self.sma_high_slope or self.sma_high_slope is None) and self.high_volumes and self.macd_high_slope and self.macd_greater_than_9_day_ema

def get_89_ema_high_low_trend(stock_latest_data):
    stock_data_high_prices_series = util.get_panda_series_of_stock_closing_prices (stock_latest_data,'high')
    stock_data_low_prices_series = util.get_panda_series_of_stock_closing_prices (stock_latest_data, 'low')

    _89_day_high_EMA_series = ind.ema (stock_data_high_prices_series, 89)
    _89_day_low_EMA_series = ind.ema (stock_data_low_prices_series, 89)

    stock_data_closing_prices_series = util.get_panda_series_of_stock_closing_prices (stock_latest_data, 'close')

    trend = Trend.uptrend
    for i in range (1, min_no_of_values_to_scan_for_89_ema_trend):
        if (stock_data_closing_prices_series.iloc[-i] > _89_day_high_EMA_series.iloc[-i] and
            stock_data_closing_prices_series.iloc[-i] > _89_day_low_EMA_series.iloc[-i]) == False:
            trend = Trend.notrend
            break

    if(trend.value==Trend.uptrend.value):
        return trend
    else:
        trend = Trend.downtrend
        for i in range (1, min_no_of_values_to_scan_for_89_ema_trend):
            if (stock_data_closing_prices_series.iloc[-i] < _89_day_high_EMA_series.iloc[-i] and
                stock_data_closing_prices_series.iloc[-i] < _89_day_low_EMA_series.iloc[-i]) == False:
                trend = Trend.notrend
                break

        return  trend


def append_ma_series(ma_series_name,ma_series,no_of_values_to_append=min_no_of_ma_values_to_append):
    series_value_list = []
    for i in range(1,no_of_values_to_append):
        series_value_list.append(ma_series.iloc[-i])

    return {ma_series_name:series_value_list}


def calculate_slope(ema_series,no_of_values_to_scan_for_slope=min_no_of_values_to_scan_for_slope):
    ema_series = ema_series.iloc[-no_of_values_to_scan_for_slope:].iloc[::-1]

    prev_ema_value = None
    for key, ema in ema_series.iteritems():
        if prev_ema_value == None:
            prev_ema_value=ema

        elif(ema>prev_ema_value):
            return Slope.DOWN

        prev_ema_value=ema

    return Slope.UP

def divergence_exists(mares, ma_series_list,no_of_values_to_scan=min_no_of_values_to_scan_for_divergence):

    latest_divergence=None
    for i in range(0,no_of_values_to_scan):
        divergence = 0
        for m in range (1, len (ma_series_list)-1):
            divergence += (ma_series_list[m - 1].iloc[-1-i] - ma_series_list[m].iloc[-1-i])

        if(latest_divergence and divergence>latest_divergence):
            mares.errors.append("Latest divergence:"+str(latest_divergence)+" lesser than next divergence:"+str(divergence)+" at position:"+str(-1-i))
            return False
        latest_divergence=divergence

    return True

def _13_21_34_EMA_Abhijeet(stock_data, backtesting=True):
   mares = MASResponse ()
   mares.fetched_dataset=stock_data
   mares.ma_strategy_name = MA_Strategy_Name._13_21_34_DAY_EMA
   mares.fetch_date = util.get_date_from_timestamp(stock_data[-1]['timestamp'])

   stock_data_closing_prices_series=util.get_panda_series_of_stock_closing_prices(stock_data)
   _13_day_EMA_series = ind.ema (stock_data_closing_prices_series, 13)
   _21_day_EMA_series=ind.ema(stock_data_closing_prices_series,21)
   _34_day_EMA_series = ind.ema(stock_data_closing_prices_series, 34)

   average_volume=util.calculate_last_10_days_average_volume (stock_data[-10:])
   mares.average_volume = average_volume
   mares.stoploss = _13_day_EMA_series.iloc[-1]

   if (average_volume > min_average_volume_to_consider) == False:
       mares.errors.append ("Last 10 day average volume:" + str (average_volume) + " not greater than min average volume to consider:" + str (min_average_volume_to_consider))
       mares.high_volumes=False

   trend = get_89_ema_high_low_trend (stock_data)
   mares.trend = trend
   if (trend.value == Trend.uptrend.value) == False:
       mares.errors.append("No clear uptrend")
       mares.correct_trend = False

   current_day_current_price = util.get_current_day_current_price (stock_data[-1])
   mares.current_day_current_price=current_day_current_price

   if (calculate_slope (_13_day_EMA_series).value == Slope.UP.value) == False:
       mares.errors.append("_13_day_EMA_series not sloping upwards.")
       mares.sma_high_slope=False

   if (calculate_slope (_21_day_EMA_series).value == Slope.UP.value) == False:
       mares.errors.append ("_21_day_EMA_series not sloping upwards.")
       mares.sma_high_slope = False

   if (calculate_slope (_34_day_EMA_series).value == Slope.UP.value) == False:
       mares.errors.append ("_34_day_EMA_series not sloping upwards")
       mares.sma_high_slope = False

   mares.lma_high_slope = None

   if(_13_day_EMA_series.iloc[-1]>_21_day_EMA_series.iloc[-1] and _21_day_EMA_series.iloc[-1]>_34_day_EMA_series.iloc[-1])== False:
       mares.errors.append ("13 day EMA:"+str(_13_day_EMA_series.iloc[-1])+" not greater than 21 day SMA:"+str(_21_day_EMA_series.iloc[-1])+" not greater than 34 day EMA:"+str(_34_day_EMA_series.iloc[-1]))
       mares.sma_greater_than_lma=False

   if (_13_day_EMA_series.iloc[-2] < _21_day_EMA_series.iloc[-2] and _13_day_EMA_series.iloc[-2] <
       _34_day_EMA_series.iloc[-2]) == False:
       mares.errors.append (
           "Prev 13 day EMA:" + str (_13_day_EMA_series.iloc[-2]) + " not less than 21 day SMA:" + str (
               _21_day_EMA_series.iloc[-2]) + " and 34 day EMA:" + str (_34_day_EMA_series.iloc[-2]))
       mares.prev_sma_less_than_lma = False

   mares.sma.append (append_ma_series ('13_day_EMA', _13_day_EMA_series))
   mares.sma.append (append_ma_series ('21_day_EMA', _21_day_EMA_series))
   mares.sma.append (append_ma_series ('34_day_EMA', _34_day_EMA_series))

   if (current_day_current_price>_13_day_EMA_series.iloc[-1]) == False:
       mares.errors.append ("Current day current price:"+str(current_day_current_price)+" not greater than 13 day EMA:"+str(_13_day_EMA_series.iloc[-1]))
       mares.stock_price_greater_than_mas=False

   if backtesting:
       days_back_when_stock_price_less_than_sma=0
       while days_back_when_stock_price_less_than_sma < len (stock_data):
           if (_13_day_EMA_series.iloc[-2 - days_back_when_stock_price_less_than_sma] > _21_day_EMA_series.iloc[
               -2 - days_back_when_stock_price_less_than_sma]) == False:
               break
           days_back_when_stock_price_less_than_sma+=1

       mares.days_back_when_stock_price_less_than_sma = days_back_when_stock_price_less_than_sma

   macd_series = ind.macd (stock_data_closing_prices_series)

   mares.macd.append (append_ma_series ('MACD', macd_series))

   if (calculate_slope (macd_series).value == Slope.UP.value) == False:
       mares.errors.append ("MACD_series not sloping upwards.")
       mares.macd_high_slope = False

   if (macd_series.iloc[-1] > 0) == False:
       mares.errors.append ("MACD:" + str (macd_series.iloc[-1]) + "not greater than 0 ")
       mares.macd_greater_than_9_day_ema = False

   return mares


def _50_day_EMA_with_100_Day_EMA(stock_data, backtesting=True):
   mares = MASResponse ()
   mares.fetched_dataset=stock_data
   mares.ma_strategy_name=MA_Strategy_Name._50_DAY_EMA_WITH_100_DAY_EMA
   mares.fetch_date = util.get_date_from_timestamp(stock_data[-1]['timestamp'])

   stock_data_closing_prices_series=util.get_panda_series_of_stock_closing_prices(stock_data)
   _50_day_EMA_series=ind.ema(stock_data_closing_prices_series,50)
   _100_day_EMA_series = ind.ema(stock_data_closing_prices_series, 100)

   average_volume=util.calculate_last_10_days_average_volume (stock_data[-10:])
   mares.average_volume = average_volume

   if (average_volume > min_average_volume_to_consider) == False:
       mares.errors.append ("Last 10 day average volume:" + str (average_volume) + "not greater than min average volume to consider:" + str (min_average_volume_to_consider))
       mares.high_volumes=False

   trend=get_89_ema_high_low_trend(stock_data)
   mares.trend=trend
   if(trend.value==Trend.uptrend.value)== False:
        mares.errors.append("No clear uptrend")
        mares.correct_trend=False

   current_day_current_price = util.get_current_day_current_price (stock_data[-1])
   mares.current_day_current_price=current_day_current_price
   mares.stoploss=_50_day_EMA_series.iloc[-1]

   if (calculate_slope (_50_day_EMA_series).value == Slope.UP.value) == False:
       mares.errors.append("_50_day_EMA_series not sloping upwards.")
       mares.sma_high_slope=False

   if (calculate_slope (_100_day_EMA_series).value == Slope.UP.value) == False:
       mares.errors.append ("_100_day_EMA_series not sloping upwards.")
       mares.lma_high_slope = False

   if(_50_day_EMA_series.iloc[-1]>_100_day_EMA_series.iloc[-1])== False:
       mares.errors.append ("50 day EMA:"+str(_50_day_EMA_series.iloc[-1])+"not greater than 100 day EMA:"+str(_100_day_EMA_series.iloc[-1]))
       mares.sma_greater_than_lma=False

   if (_50_day_EMA_series.iloc[-2] < _100_day_EMA_series.iloc[-2]) == False:
       mares.errors.append (
           "Prev 50 day EMA:" + str (_50_day_EMA_series.iloc[-2]) + "not less than 100 day EMA:" + str (
               _100_day_EMA_series.iloc[-2]))
       mares.prev_sma_less_than_lma = False

   mares.sma.append(append_ma_series('50_day_EMA',_50_day_EMA_series))
   mares.lma.append(append_ma_series('100_day_EMA',_100_day_EMA_series))

   if (current_day_current_price>_50_day_EMA_series.iloc[-1] ) == False:
       mares.errors.append ("Current day current price:"+str(current_day_current_price)+" not greater than 50 day EMA:"+str(_50_day_EMA_series.iloc[-1]))
       mares.stock_price_greater_than_mas=False

   if backtesting:
       days_back_when_stock_price_less_than_sma=0
       while days_back_when_stock_price_less_than_sma<len(stock_data):
           if (_50_day_EMA_series.iloc[-2 - days_back_when_stock_price_less_than_sma] > _100_day_EMA_series.iloc[
               -2 - days_back_when_stock_price_less_than_sma]) == False:
               break
           days_back_when_stock_price_less_than_sma += 1

       mares.days_back_when_stock_price_less_than_sma = days_back_when_stock_price_less_than_sma

   macd_series = ind.macd (stock_data_closing_prices_series, n_fast=6, n_slow=13)

   mares.macd.append (append_ma_series ('MACD', macd_series))

   if (calculate_slope (macd_series).value == Slope.UP.value) == False:
       mares.errors.append ("MACD_series not sloping upwards.")
       mares.macd_high_slope = False

   if (macd_series.iloc[-1] > 0) == False:
       mares.errors.append ("MACD:" + str (macd_series.iloc[-1]) + "not greater than 0 ")
       mares.macd_greater_than_9_day_ema = False

   return mares


def _100_day_EMA_with_200_Day_EMA(stock_data, backtesting=True):
   mares = MASResponse ()
   mares.fetched_dataset=stock_data
   mares.ma_strategy_name = MA_Strategy_Name._100_DAY_EMA_WITH_200_DAY_EMA
   mares.fetch_date = util.get_date_from_timestamp(stock_data[-1]['timestamp'])

   stock_data_closing_prices_series=util.get_panda_series_of_stock_closing_prices(stock_data)
   _100_day_EMA_series=ind.ema(stock_data_closing_prices_series,100)
   _200_day_EMA_series = ind.ema(stock_data_closing_prices_series, 200)

   average_volume=util.calculate_last_10_days_average_volume (stock_data[-10:])
   mares.average_volume = average_volume
   mares.stoploss = _100_day_EMA_series.iloc[-1]

   trend = get_89_ema_high_low_trend (stock_data)
   mares.trend = trend
   if (trend.value == Trend.uptrend.value) == False:
       mares.errors.append("No clear uptrend")
       mares.correct_trend = False

   if (average_volume > min_average_volume_to_consider) == False:
       mares.errors.append ("Last 10 day average volume:" + str (average_volume) + " not greater than min average volume to consider:" + str (min_average_volume_to_consider))
       mares.high_volumes=False

   current_day_current_price = util.get_current_day_current_price (stock_data[-1])
   mares.current_day_current_price=current_day_current_price

   if (calculate_slope (_100_day_EMA_series).value == Slope.UP.value) == False:
       mares.errors.append("_100_day_EMA_series not sloping upwards.")
       mares.sma_high_slope=False

   if (calculate_slope (_200_day_EMA_series).value == Slope.UP.value) == False:
       mares.errors.append ("_200_day_EMA_series not sloping upwards.")
       mares.lma_high_slope = False

   if(_100_day_EMA_series.iloc[-1]>_200_day_EMA_series.iloc[-1])== False:
       mares.errors.append ("100 day EMA:"+str(_100_day_EMA_series.iloc[-1])+" not greater than 200 day EMA:"+str(_200_day_EMA_series.iloc[-1]))
       mares.sma_greater_than_lma=False

   if (_100_day_EMA_series.iloc[-2] < _200_day_EMA_series.iloc[-2]) == False:
       mares.errors.append (
           "Prev 100 day EMA:" + str (_100_day_EMA_series.iloc[-2]) + " not less than 200 day EMA:" + str (
               _200_day_EMA_series.iloc[-2]))
       mares.prev_sma_less_than_lma = False

   mares.sma.append (append_ma_series ('100_day_EMA', _100_day_EMA_series))
   mares.lma.append (append_ma_series ('200_day_EMA', _200_day_EMA_series))

   if (current_day_current_price>_100_day_EMA_series.iloc[-1]) == False:
       mares.errors.append ("Current day current price:"+str(current_day_current_price)+" not greater than 100 day SMA:"+str(_100_day_EMA_series.iloc[-1]))
       mares.stock_price_greater_than_mas=False

   if backtesting:
       days_back_when_stock_price_less_than_sma=0
       while days_back_when_stock_price_less_than_sma<len(stock_data):
           if (_100_day_EMA_series.iloc[-2 - days_back_when_stock_price_less_than_sma] > _200_day_EMA_series.iloc[
               -2 - days_back_when_stock_price_less_than_sma]) == False:
               break

           days_back_when_stock_price_less_than_sma+=1

       mares.days_back_when_stock_price_less_than_sma = days_back_when_stock_price_less_than_sma

   macd_series = ind.macd (stock_data_closing_prices_series)

   mares.macd.append (append_ma_series ('MACD', macd_series))

   if (calculate_slope (macd_series).value == Slope.UP.value) == False:
       mares.errors.append ("MACD_series not sloping upwards.")
       mares.macd_high_slope = False

   if (macd_series.iloc[-1] > 0) == False:
       mares.errors.append ("MACD:" + str (macd_series.iloc[-1]) + "not greater than 0 ")
       mares.macd_greater_than_9_day_ema = False

   return mares


def identify_long_term_stock_before_rally(stock_data, backtesting=True):
   mares = MASResponse ()
   mares.fetched_dataset=stock_data
   mares.ma_strategy_name = MA_Strategy_Name.IDENTIFY_LONG_TERM_STOCK_BEFORE_RALLY
   mares.fetch_date = util.get_date_from_timestamp(stock_data[-1]['timestamp'])

   stock_data_closing_prices_series=util.get_panda_series_of_stock_closing_prices(stock_data)
   _50_day_SMA_series = ind.sma (stock_data_closing_prices_series, 50)
   _150_day_SMA_series=ind.sma(stock_data_closing_prices_series,150)
   _200_day_SMA_series = ind.sma(stock_data_closing_prices_series, 200)

   average_volume=util.calculate_last_10_days_average_volume (stock_data[-10:])
   mares.average_volume = average_volume
   mares.stoploss = _50_day_SMA_series.iloc[-1]

   if (average_volume > min_average_volume_to_consider) == False:
       mares.errors.append ("Last 10 day average volume:" + str (average_volume) + " not greater than min average volume to consider:" + str (min_average_volume_to_consider))
       mares.high_volumes=False

   trend = get_89_ema_high_low_trend (stock_data)
   mares.trend = trend
   if (trend.value == Trend.uptrend.value) == False:
       mares.errors.append("No clear uptrend")
       mares.correct_trend = False

   current_day_current_price = util.get_current_day_current_price (stock_data[-1])
   mares.current_day_current_price=current_day_current_price

   if (calculate_slope (_50_day_SMA_series).value == Slope.UP.value) == False:
       mares.errors.append("_50_day_SMA_series not sloping upwards.")
       mares.sma_high_slope=False

   if (calculate_slope (_150_day_SMA_series).value == Slope.UP.value) == False:
       mares.errors.append ("_150_day_SMA_series not sloping upwards.")
       mares.lma_high_slope = False

   if (calculate_slope (_200_day_SMA_series, 30).value == Slope.UP.value) == False:
       mares.errors.append ("_200_day_SMA_series not sloping upwards for period:."+str(30))
       mares.lma_high_slope = False

   if(_50_day_SMA_series.iloc[-1]>_150_day_SMA_series.iloc[-1] and _150_day_SMA_series.iloc[-1]>_200_day_SMA_series.iloc[-1])== False:
       mares.errors.append ("50 day EMA:"+str(_50_day_SMA_series.iloc[-1])+" not greater than 150 day SMA:"+str(_150_day_SMA_series.iloc[-1])+" not greater than 200 day EMA:"+str(_200_day_SMA_series.iloc[-1]))
       mares.sma_greater_than_lma=False

   if (_50_day_SMA_series.iloc[-2] < _150_day_SMA_series.iloc[-2] and _50_day_SMA_series.iloc[-2] <
       _200_day_SMA_series.iloc[-2]) == False:
       mares.errors.append (
           "Prev 50 day EMA:" + str (_50_day_SMA_series.iloc[-2]) + " not less than 150 day SMA:" + str (
               _150_day_SMA_series.iloc[-2]) + " not less than 200 day EMA:" + str (_200_day_SMA_series.iloc[-2]))
       mares.prev_sma_less_than_lma = False

   mares.sma.append (append_ma_series ('50_day_SMA', _50_day_SMA_series))
   mares.lma.append (append_ma_series ('150_day_SMA', _150_day_SMA_series))
   mares.lma.append (append_ma_series ('200_day_SMA', _200_day_SMA_series))

   if (current_day_current_price > (_50_day_SMA_series.iloc[-1])) == False:
       mares.errors.append ("Current day current price:"+str(current_day_current_price)+" not greater than 50 day SMA:"+str(_50_day_SMA_series.iloc[-1]))
       mares.stock_price_greater_than_mas=False

   if backtesting:
       days_back_when_stock_price_less_than_sma=0
       while days_back_when_stock_price_less_than_sma<len(stock_data):
           if (_50_day_SMA_series.iloc[-2 - days_back_when_stock_price_less_than_sma] > _150_day_SMA_series.iloc[
               -2 - days_back_when_stock_price_less_than_sma]) == False:
               break

           days_back_when_stock_price_less_than_sma += 1

       mares.days_back_when_stock_price_less_than_sma = days_back_when_stock_price_less_than_sma

   macd_series = ind.macd (stock_data_closing_prices_series)

   mares.macd.append (append_ma_series ('MACD', macd_series))

   if (calculate_slope (macd_series).value == Slope.UP.value) == False:
       mares.errors.append ("MACD_series not sloping upwards.")
       mares.macd_high_slope = False

   if (macd_series.iloc[-1] > 0) == False:
       mares.errors.append ("MACD:" + str (macd_series.iloc[-1]) + "not greater than 0 ")
       mares.macd_greater_than_9_day_ema = False

   if (current_day_current_price>(1.3*float(stock_data[-1]['yearly_low'])))== False:
       mares.errors.append (
           "Current day current price:" + str (current_day_current_price) + " not greater than 30% of yearly low:" + str (
               stock_data[-1]['yearly_low']))
       mares.stock_price_appropriately_placed_between_yearly_highs_lows=False

   if (current_day_current_price>(.75*float(stock_data[-1]['yearly_high'])))== False:
       mares.errors.append (
           "Current day current price:" + str (
               current_day_current_price) + " not greater than 75% of yearly high:" + str (
               stock_data[-1]['yearly_high']))
       mares.stock_price_appropriately_placed_between_yearly_highs_lows = False

   if(mares.stock_price_appropriately_placed_between_yearly_highs_lows==None):
       mares.stock_price_appropriately_placed_between_yearly_highs_lows=True

   rsi_series = ind.rsi (stock_data_closing_prices_series, 14)
   mares.rsi=rsi_series.iloc[-1]
   if (mares.rsi >70) == False:
       mares.errors.append ("<70 RSI value"+str(mares.rsi))
       mares.correct_rsi=False

   if (mares.correct_rsi == None):
       mares.correct_rsi = True

   return mares

def guppy_multiple_moving_average_indicator(stock_data, backtesting=True):
   mares = MASResponse ()
   mares.fetched_dataset=stock_data
   mares.ma_strategy_name = MA_Strategy_Name.GUPPY_MULTIPLE_MOVING_AVERAGE_INDICATOR
   mares.fetch_date = util.get_date_from_timestamp(stock_data[-1]['timestamp'])

   stock_data_closing_prices_series=util.get_panda_series_of_stock_closing_prices(stock_data)
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

   average_volume=util.calculate_last_10_days_average_volume (stock_data[-10:])
   mares.average_volume = average_volume
   mares.stoploss = _3_day_EMA_series.iloc[-1]

   if (average_volume > min_average_volume_to_consider) == False:
       mares.errors.append ("Last 10 day average volume:" + str (average_volume) + "not greater than min average volume to consider:" + str (min_average_volume_to_consider))
       mares.high_volumes=False

   trend = get_89_ema_high_low_trend (stock_data)
   mares.trend = trend
   if (trend.value == Trend.uptrend.value) == False:
       mares.errors.append("No clear uptrend")
       mares.correct_trend = False

   current_day_current_price = util.get_current_day_current_price (stock_data[-1])
   mares.current_day_current_price=current_day_current_price

   if (calculate_slope (_3_day_EMA_series, 3).value == Slope.UP.value) == False:
       mares.errors.append("_3_day_EMA_series not sloping upwards.")
       mares.sma_high_slope=False

   if (calculate_slope (_5_day_EMA_series, 5).value == Slope.UP.value) == False:
       mares.errors.append ("_5_day_EMA_series not sloping upwards.")
       mares.sma_high_slope = False

   if (calculate_slope (_8_day_EMA_series).value == Slope.UP.value) == False:
       mares.errors.append ("_8_day_EMA_series not sloping upwards.")
       mares.sma_high_slope = False

   if (calculate_slope (_10_day_EMA_series).value == Slope.UP.value) == False:
       mares.errors.append ("_10_day_EMA_series not sloping upwards.")
       mares.sma_high_slope = False

   if (calculate_slope (_12_day_EMA_series).value == Slope.UP.value) == False:
       mares.errors.append ("_12_day_EMA_series not sloping upwards.")
       mares.sma_high_slope = False

   if (calculate_slope (_18_day_EMA_series).value == Slope.UP.value) == False:
       mares.errors.append ("_18_day_EMA_series not sloping upwards.")
       mares.sma_high_slope = False

   if (calculate_slope (_30_day_EMA_series).value == Slope.UP.value) == False:
       mares.errors.append ("_30_day_EMA_series not sloping upwards.")
       mares.lma_high_slope = False

   if (calculate_slope (_35_day_EMA_series).value == Slope.UP.value) == False:
       mares.errors.append ("_35_day_EMA_series not sloping upwards for period.")
       mares.lma_high_slope = False

   if (calculate_slope (_40_day_EMA_series).value == Slope.UP.value) == False:
       mares.errors.append ("_40_day_EMA_series not sloping upwards for period.")
       mares.lma_high_slope = False

   if (calculate_slope (_45_day_EMA_series).value == Slope.UP.value) == False:
       mares.errors.append ("_45_day_EMA_series not sloping upwards for period.")
       mares.lma_high_slope = False

   if (calculate_slope (_50_day_EMA_series).value == Slope.UP.value) == False:
       mares.errors.append ("_50_day_EMA_series not sloping upwards for period.")
       mares.lma_high_slope = False

   if (calculate_slope (_60_day_EMA_series).value == Slope.UP.value) == False:
       mares.errors.append ("_60_day_EMA_series not sloping upwards for period.")
       mares.lma_high_slope = False

   if(_3_day_EMA_series.iloc[-1]>_5_day_EMA_series.iloc[-1] and _5_day_EMA_series.iloc[-1]>_8_day_EMA_series.iloc[-1]and _8_day_EMA_series.iloc[-1]>_10_day_EMA_series.iloc[-1]and _10_day_EMA_series.iloc[-1]>_12_day_EMA_series.iloc[-1] and _12_day_EMA_series.iloc[-1]>_18_day_EMA_series.iloc[-1] and _18_day_EMA_series.iloc[-1]>_30_day_EMA_series.iloc[-1] and _30_day_EMA_series.iloc[-1]>_35_day_EMA_series.iloc[-1] and _35_day_EMA_series.iloc[-1]>_40_day_EMA_series.iloc[-1] and _40_day_EMA_series.iloc[-1]>_45_day_EMA_series.iloc[-1] and _45_day_EMA_series.iloc[-1]>_50_day_EMA_series.iloc[-1] and _50_day_EMA_series.iloc[-1]>_60_day_EMA_series.iloc[-1] )== False:
       mares.errors.append ("SEMAs & LEMAs not in increasing order:"+str(_3_day_EMA_series.iloc[-1])+", "+str(_5_day_EMA_series.iloc[-1])+", "+str(_8_day_EMA_series.iloc[-1])+", "+str(_10_day_EMA_series.iloc[-1])+", "+str(_12_day_EMA_series.iloc[-1])+", "+str(_18_day_EMA_series.iloc[-1])+", "+str(_30_day_EMA_series.iloc[-1])+", "+str(_35_day_EMA_series.iloc[-1])+", "+str(_40_day_EMA_series.iloc[-1])+", "+str(_45_day_EMA_series.iloc[-1])+", "+str(_50_day_EMA_series.iloc[-1])+", "+str(_60_day_EMA_series.iloc[-1])+".")
       mares.sma_greater_than_lma=False

   if (_3_day_EMA_series.iloc[-2] < _5_day_EMA_series.iloc[-2] and _3_day_EMA_series.iloc[-2] < _8_day_EMA_series.iloc[
       -2]) == False:
       mares.errors.append (
           "Prev 3 day EMA:" + str (_3_day_EMA_series.iloc[-2]) + " not less than 5 day SMA:" + str (
               _5_day_EMA_series.iloc[-2]) + " not less than 8 day EMA:" + str (_8_day_EMA_series.iloc[-2]))
       mares.prev_sma_less_than_lma = False

   if (divergence_exists(mares, [_3_day_EMA_series,_5_day_EMA_series,_8_day_EMA_series,_10_day_EMA_series,_12_day_EMA_series, _18_day_EMA_series, _30_day_EMA_series,_35_day_EMA_series,_40_day_EMA_series,_45_day_EMA_series, _50_day_EMA_series,_60_day_EMA_series]) == True) == False:
       mares.errors.append ("No upward divergence between MAs.")
       mares.mas_diverging = False

   if mares.mas_diverging==None:
       mares.mas_diverging = True

   mares.sma.append (append_ma_series ('3_day_EMA', _3_day_EMA_series))
   mares.sma.append (append_ma_series ('5_day_EMA', _5_day_EMA_series))
   mares.sma.append (append_ma_series ('8_day_EMA', _8_day_EMA_series))
   mares.sma.append (append_ma_series ('10_day_EMA', _10_day_EMA_series))
   mares.sma.append (append_ma_series ('12_day_EMA', _12_day_EMA_series))
   mares.sma.append (append_ma_series ('18_day_EMA', _18_day_EMA_series))

   mares.lma.append (append_ma_series ('30_day_EMA', _30_day_EMA_series))
   mares.lma.append (append_ma_series ('35_day_EMA', _35_day_EMA_series))
   mares.lma.append (append_ma_series ('40_day_EMA', _40_day_EMA_series))
   mares.lma.append (append_ma_series ('45_day_EMA', _45_day_EMA_series))
   mares.lma.append (append_ma_series ('50_day_EMA', _50_day_EMA_series))
   mares.lma.append (append_ma_series ('60_day_EMA', _60_day_EMA_series))


   if (current_day_current_price>_3_day_EMA_series.iloc[-1]) == False:
       mares.errors.append ("Current day current price:"+str(current_day_current_price)+" not greater than 3 day SMA:"+str(_3_day_EMA_series.iloc[-1]))
       mares.stock_price_greater_than_mas=False

   if backtesting:
       days_back_when_stock_price_less_than_sma=0
       while days_back_when_stock_price_less_than_sma<len(stock_data):
           if (_3_day_EMA_series.iloc[-2 - days_back_when_stock_price_less_than_sma] > _5_day_EMA_series.iloc[
               -2 - days_back_when_stock_price_less_than_sma]) == False:
               break

           days_back_when_stock_price_less_than_sma += 1

       mares.days_back_when_stock_price_less_than_sma = days_back_when_stock_price_less_than_sma

   macd_series = ind.macd (stock_data_closing_prices_series)

   mares.macd.append (append_ma_series ('MACD', macd_series))

   if (calculate_slope (macd_series).value == Slope.UP.value) == False:
       mares.errors.append ("MACD_series not sloping upwards.")
       mares.macd_high_slope = False

   if (macd_series.iloc[-1] > 0) == False:
       mares.errors.append ("MACD:" + str (macd_series.iloc[-1]) + "not greater than 0 ")
       mares.macd_greater_than_9_day_ema = False

   return mares

def _50_week_SMA_20_day_RSI(stock_data, backtesting=True):
   mares = MASResponse ()
   mares.fetched_dataset=stock_data
   mares.ma_strategy_name = MA_Strategy_Name._50_WEEK_SMA_20_DAY_RSI
   mares.fetch_date = util.get_date_from_timestamp(stock_data[-1]['timestamp'])

   stock_data_closing_prices_series=util.get_panda_series_of_stock_closing_prices(stock_data)
   _250_day_SMA_series=ind.sma(stock_data_closing_prices_series,250)

   mares.sma_greater_than_lma=None

   average_volume=util.calculate_last_10_days_average_volume (stock_data[-10:])
   mares.average_volume = average_volume
   mares.stoploss = _250_day_SMA_series.iloc[-1]

   if (average_volume > min_average_volume_to_consider) == False:
       mares.errors.append ("Last 10 day average volume:" + str (average_volume) + "not greater than min average volume to consider:" + str (min_average_volume_to_consider))
       mares.high_volumes=False

   trend = get_89_ema_high_low_trend (stock_data)
   mares.trend = trend
   if (trend.value == Trend.uptrend.value) == False:
       mares.errors.append("No clear uptrend")
       mares.correct_trend = False

   current_day_current_price = util.get_current_day_current_price (stock_data[-1])
   mares.current_day_current_price=current_day_current_price

   mares.sma_high_slope=None
   if (calculate_slope (_250_day_SMA_series).value == Slope.UP.value) == False:
       mares.errors.append("_250_day_SMA_series not sloping upwards.")
       mares.lma_high_slope=False

   mares.lma.append (append_ma_series ('250_day_SMA', _250_day_SMA_series))

   if (current_day_current_price>_250_day_SMA_series.iloc[-1] ) == False:
       mares.errors.append ("Current day current price:"+str(current_day_current_price)+" not greater than 250 day SMA:"+str(_250_day_SMA_series.iloc[-1]))
       mares.stock_price_greater_than_mas=False

   if backtesting:
       days_back_when_stock_price_less_than_sma=0
       while days_back_when_stock_price_less_than_sma<len(stock_data):
           earlier_mares=_50_week_SMA_20_day_RSI(stock_data[:-1-days_back_when_stock_price_less_than_sma],False)
           if earlier_mares.is_strategy_tradable()==False:
               break
           days_back_when_stock_price_less_than_sma+=1

       mares.days_back_when_stock_price_less_than_sma = days_back_when_stock_price_less_than_sma

   macd_series = ind.macd (stock_data_closing_prices_series)

   mares.macd.append (append_ma_series ('MACD', macd_series))

   if (calculate_slope (macd_series).value == Slope.UP.value) == False:
       mares.errors.append ("MACD_series not sloping upwards.")
       mares.macd_high_slope = False

   if (macd_series.iloc[-1] > 0) == False:
       mares.errors.append ("MACD:" + str (macd_series.iloc[-1]) + "not greater than 0 ")
       mares.macd_greater_than_9_day_ema = False

   rsi_series = ind.rsi (stock_data_closing_prices_series, 20)
   mares.rsi = rsi_series.iloc[-1]
   if (mares.rsi>50) == False:
       mares.errors.append ("<50 RSI value"+str(mares.rsi))
       mares.correct_rsi = False

   if (mares.correct_rsi == None):
       mares.correct_rsi = True

   return mares

def moving_average_ribbon(stock_data, backtesting=True):
    mares = MASResponse ()
    mares.fetched_dataset = stock_data
    mares.ma_strategy_name = MA_Strategy_Name.MOVING_AVERAGE_RIBBON
    mares.fetch_date = util.get_date_from_timestamp(stock_data[-1]['timestamp'])

    stock_data_closing_prices_series = util.get_panda_series_of_stock_closing_prices (stock_data,'close')
    _34_day_EMA_close_series = ind.ema (stock_data_closing_prices_series, 34)
    stock_data_high_prices_series = util.get_panda_series_of_stock_closing_prices (stock_data, 'high')
    _34_day_EMA_high_series = ind.ema (stock_data_high_prices_series, 34)
    stock_data_low_prices_series = util.get_panda_series_of_stock_closing_prices (stock_data, 'low')
    _34_day_EMA_low_series = ind.ema (stock_data_low_prices_series, 34)

    average_volume = util.calculate_last_10_days_average_volume (stock_data[-10:])
    mares.average_volume = average_volume
    mares.stoploss = _34_day_EMA_low_series.iloc[-1]

    if (average_volume > min_average_volume_to_consider) == False:
        mares.errors.append ("Last 10 day average volume:" + str (
            average_volume) + "not greater than min average volume to consider:" + str (min_average_volume_to_consider))
        mares.high_volumes = False

    trend = get_89_ema_high_low_trend (stock_data)
    mares.trend = trend
    if (trend.value == Trend.uptrend.value) == False:
        mares.errors.append("No clear uptrend")
        mares.correct_trend = False

    current_day_current_price = util.get_current_day_current_price (stock_data[-1])
    mares.current_day_current_price = current_day_current_price

    if (calculate_slope (_34_day_EMA_close_series).value == Slope.UP.value) == False:
        mares.errors.append ("_34_day_EMA_close_series not sloping upwards.")
        mares.sma_high_slope = False

    if (calculate_slope (_34_day_EMA_high_series).value == Slope.UP.value) == False:
        mares.errors.append ("_34_day_EMA_high_series not sloping upwards.")
        mares.sma_high_slope = False

    if (calculate_slope (_34_day_EMA_low_series).value == Slope.UP.value) == False:
        mares.errors.append ("_34_day_EMA_low_series not sloping upwards.")
        mares.sma_high_slope = False

    mares.lma_high_slope = None
    mares.sma_greater_than_lma=None

    mares.sma.append (append_ma_series ('34_day_EMA_close', _34_day_EMA_close_series))
    mares.sma.append (append_ma_series ('34_day_EMA_high', _34_day_EMA_high_series))
    mares.sma.append (append_ma_series ('34_day_EMA_low', _34_day_EMA_low_series))

    if (current_day_current_price > _34_day_EMA_close_series.iloc[-1] and current_day_current_price > _34_day_EMA_high_series.iloc[-1] and current_day_current_price > _34_day_EMA_low_series.iloc[-1]) == False:
        mares.errors.append (
            "Current day current price:" + str (current_day_current_price) + " not greater than three 34 day EMAs(high, low & close):" + str(_34_day_EMA_high_series.iloc[-1])+","+ str(_34_day_EMA_low_series.iloc[-1])+","+ str(_34_day_EMA_close_series.iloc[-1])+".")
        mares.stock_price_greater_than_mas = False

    if backtesting:
        days_back_when_stock_price_less_than_sma = 0
        while days_back_when_stock_price_less_than_sma < len(stock_data):
            if (stock_data[-2 - days_back_when_stock_price_less_than_sma]['close'] > _34_day_EMA_high_series.iloc[
                -2 - days_back_when_stock_price_less_than_sma]) == False:
                break
            days_back_when_stock_price_less_than_sma += 1

        mares.days_back_when_stock_price_less_than_sma = days_back_when_stock_price_less_than_sma

    macd_series = ind.macd (stock_data_closing_prices_series)

    mares.macd.append (append_ma_series ('MACD', macd_series))

    if (calculate_slope (macd_series).value == Slope.UP.value) == False:
        mares.errors.append ("MACD_series not sloping upwards.")
        mares.macd_high_slope = False

    if (macd_series.iloc[-1] > 0) == False:
        mares.errors.append ("MACD:" + str (macd_series.iloc[-1]) + "not greater than 0 ")
        mares.macd_greater_than_9_day_ema = False

    return mares
