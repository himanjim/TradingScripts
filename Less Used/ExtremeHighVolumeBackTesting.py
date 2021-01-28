import traceback
from datetime import datetime, timedelta

import time

import GenericStatPrinter as gstats
import ScrapUtils as nse_bse
import Utils as util
from PatternRecognition import Action, Trend

diff_between_start_end_date = 1200
no_of_sessions_to_skip_from_start = 15
no_of_sessions_to_buffer_from_end = 5
risk_reward_ratio = .5
max_days_to_wait = 7

current_time = time.strftime ("%Y_%m_%d#%H_%M_%S")
high_volume_back_test_excel_location = 'F:/Trading_Responses/High_volume_back_test' + str(current_time) + '.xlsx'

start_time = time.time ()

upstox_api = util.intialize_upstox_api (['NSE_EQ'])

today_date = datetime.today ().date ()
start_date = datetime.now () - timedelta (days=diff_between_start_end_date)
end_date = datetime.now () - timedelta (days=1)

# stocks_latest_info = nse_bse.get_all_nse_stocks_ids ()
# stocks_latest_info = nse_bse.get_nifty50_stocks_latest_info ()
stocks_latest_info = nse_bse.get_nse_fo_stocks()
# stocks_latest_info = nse_bse.get_indices ()
# stocks_latest_info = nse_bse.get_nifty100_stocks_latest_info ()
# stocks_latest_info = [{nse_bse.STOCK_ID: 'MGL', nse_bse.EXCHANGE: nse_bse.NSE}]

highest_vol_daily_responses = []

highest_vols_daily = {}

for stock_latest_info in stocks_latest_info:
    try:
        stock_latest_data = util.get_stock_latest_data (stock_latest_info[nse_bse.STOCK_ID], upstox_api, start_date,
                                                        end_date, stock_latest_info[nse_bse.EXCHANGE])

        i = no_of_sessions_to_skip_from_start

        while i + no_of_sessions_to_buffer_from_end < len (stock_latest_data):
            last_10_day_stock_data = stock_latest_data[i - 10: i]
            prev_10_day_aver_vol = sum (item['volume'] for item in last_10_day_stock_data) / len (
                last_10_day_stock_data)

            vol_rise = (stock_latest_data[i]['volume'] - prev_10_day_aver_vol) / prev_10_day_aver_vol

            if stock_latest_data[i]['volume'] > prev_10_day_aver_vol and (
                    stock_latest_data[i]['timestamp'] not in highest_vols_daily or vol_rise >
                    highest_vols_daily[stock_latest_data[i]['timestamp']]['vol_rise']):

                action = [Action.SHORT, Action.LONG][stock_latest_data[i]['close'] > stock_latest_data[i]['open']]

                stop_loss = [stock_latest_data[i]['high'], stock_latest_data[i]['low']][
                    action.value == Action.LONG.value]

                target = util.get_target (stock_latest_data[i + 1]['open'], stop_loss, risk_reward_ratio, action)

                trade_success = None

                for j in range (i + 1, len (stock_latest_data)):

                    count = 0

                    if count >= max_days_to_wait:
                        break

                    if action.value == Action.LONG.value and stock_latest_data[j]['low'] <= stop_loss:
                        trade_success = 0
                        break
                    elif action.value == Action.LONG.value and stock_latest_data[j]['high'] >= target:
                        trade_success = 1
                        break
                    elif action.value == Action.SHORT.value and stock_latest_data[j]['high'] >= stop_loss:
                        trade_success = 0
                        break
                    elif action.value == Action.SHORT.value and stock_latest_data[j]['low'] <= target:
                        trade_success = 1
                        break

                    count += 1

                trend = [Trend.downtrend, Trend.uptrend][
                    last_10_day_stock_data[-1]['close'] > last_10_day_stock_data[0]['close']]
                long_candle_length = [0, 1][
                    util.not_too_long_or_short_candle (stock_latest_data[-1], last_10_day_stock_data)]

                if trade_success is not None:
                    highest_vols_daily[stock_latest_data[i]['timestamp']] = {
                    nse_bse.STOCK_ID: stock_latest_info[nse_bse.STOCK_ID], 'vol_rise': vol_rise,
                    'av_vol': prev_10_day_aver_vol, 'trade_success': trade_success, 'action': action.name,
                        'stop_loss': stop_loss, 'target': target, 'buy_price': stock_latest_data[i + 1]['open'],
                        'sell_price': stock_latest_data[j]['close'],
                    'action': action.name, 'sell_day': stock_latest_data[j]['timestamp'],
                        'stock_data': stock_latest_data[i], 'trend': trend.name,
                        'long_candle_length': long_candle_length}

            i += 1

    except Exception as e:
        print (traceback.format_exc ())

for key, highest_vol_daily in highest_vols_daily.items():

    highest_vol_daily_responses.append (
        [highest_vol_daily[nse_bse.STOCK_ID], highest_vol_daily['stock_data']['volume'], highest_vol_daily['av_vol'],
         highest_vol_daily['vol_rise'], util.get_date_from_timestamp (highest_vol_daily['stock_data']['timestamp']),
         util.get_date_from_timestamp(highest_vol_daily['sell_day']), highest_vol_daily['buy_price'],
         highest_vol_daily['sell_price'],
         highest_vol_daily['target'], highest_vol_daily['stop_loss'], highest_vol_daily['action'],
         highest_vol_daily['trend'], highest_vol_daily['long_candle_length'], highest_vol_daily['trade_success']])

highest_vol_daily_responses.sort(key=lambda x: -x[3])

highest_vol_daily_responses.insert(0,
                                   ['STOCK', 'VOL', 'AV. VOL', 'VOL RISE', 'VOL DAY', 'SELL DAY', 'BUY PRICE',
                                    'SELL PRICE', 'TARGET',
                                    'SL', 'ACTION', 'TREND', 'CANDLE LEN', 'SUCCESS'])

gstats.print_statistics (highest_vol_daily_responses, high_volume_back_test_excel_location)

print ("---Script executed in %s seconds ---" % (time.time () - start_time))
