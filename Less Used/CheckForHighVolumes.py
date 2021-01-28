import traceback
from datetime import datetime, timedelta

import time

import ScrapUtils as nse_bse
import Utils as util
from PatternRecognition import Action, Trend

diff_between_start_end_date = 30
risk_reward_ratio = .5

start_time = time.time()

upstox_api = util.intialize_upstox_api ([nse_bse.NSE])

today_date = datetime.today().date()
start_date = datetime.now() - timedelta(days=diff_between_start_end_date)
end_date = datetime.now() - timedelta(days=1)

# stocks_latest_info = nse_bse.get_all_nse_stocks_ids ()
# stocks_latest_info = nse_bse.get_nifty50_stocks_latest_info ()
stocks_latest_info = nse_bse.get_nse_fo_stocks()
# stocks_latest_info = nse_bse.get_indices ()
# stocks_latest_info = nse_bse.get_nifty100_stocks_latest_info ()
# stocks_latest_info = [{nse_bse.STOCK_ID: 'MGL', nse_bse.EXCHANGE: nse_bse.NSE}]

# Volume rise > 12

highest_vols_today = []

prev_vol_rise = None

for stock_latest_info in stocks_latest_info:
    try:
        stock_latest_data = util.get_stock_latest_data(stock_latest_info[nse_bse.STOCK_ID], upstox_api, start_date,
                                                       end_date, stock_latest_info[nse_bse.EXCHANGE])

        last_10_day_stock_data = stock_latest_data[-11: -1]
        prev_10_day_aver_vol = sum(item['volume'] for item in last_10_day_stock_data) / len(
            last_10_day_stock_data)

        vol_rise = (stock_latest_data[-1]['volume'] - prev_10_day_aver_vol) / prev_10_day_aver_vol

        if stock_latest_data[-1]['volume'] > prev_10_day_aver_vol and (prev_vol_rise is None or vol_rise >
                                                                       prev_vol_rise):
            action = [Action.SHORT, Action.LONG][stock_latest_data[-1]['close'] > stock_latest_data[-1]['open']]

            stop_loss = [stock_latest_data[-1]['high'], stock_latest_data[-1]['low']][
                action.value == Action.LONG.value]

            trend = [Trend.downtrend, Trend.uptrend][
                last_10_day_stock_data[-1]['close'] > last_10_day_stock_data[0]['close']]
            long_candle_length = [0, 1][
                util.not_too_long_or_short_candle(stock_latest_data[-1], last_10_day_stock_data)]

            highest_vols_today = {
                nse_bse.STOCK_ID: stock_latest_info[nse_bse.STOCK_ID], 'vol_rise': vol_rise,
                'av_vol': prev_10_day_aver_vol, 'action': action.name,
                'stop_loss': stop_loss, 'trend': trend.name,
                'long_candle_length': long_candle_length}

            prev_vol_rise = vol_rise



    except Exception as e:
        print(traceback.format_exc())

print(highest_vols_today)

print("---Script executed in %s seconds ---" % (time.time() - start_time))
