import math

import ScrapUtils as nse_bse

PRICE_DIFF = 'price_diff'
VOL_DIFF = 'vol_diff'
max_price_rise_wrt_yester = {nse_bse.STOCK_ID: None, PRICE_DIFF: -math.inf}
max_price_fall_wrt_yester = {nse_bse.STOCK_ID: None, PRICE_DIFF: math.inf}
max_price_rise_wrt_today = {nse_bse.STOCK_ID: None, PRICE_DIFF: -math.inf}
max_price_fall_wrt_today = {nse_bse.STOCK_ID: None, PRICE_DIFF: math.inf}
bullish_marubuzo = {nse_bse.STOCK_ID: None, PRICE_DIFF: math.inf}
bearish_marubuzo = {nse_bse.STOCK_ID: None, PRICE_DIFF: math.inf}
max_vol_diff = {nse_bse.STOCK_ID: None, VOL_DIFF: -math.inf}


def update_prices(stock_id, ltp, open, high, low, prev_close, volume):
    price_diff_wrt_yester = (open - prev_close) / prev_close

    if price_diff_wrt_yester < 0 and price_diff_wrt_yester < max_price_fall_wrt_yester[PRICE_DIFF]:
        max_price_fall_wrt_yester[nse_bse.STOCK_ID] = stock_id
        max_price_fall_wrt_yester[PRICE_DIFF] = price_diff_wrt_yester
    elif price_diff_wrt_yester > 0 and price_diff_wrt_yester > max_price_rise_wrt_yester[PRICE_DIFF]:
        max_price_rise_wrt_yester[nse_bse.STOCK_ID] = stock_id
        max_price_rise_wrt_yester[PRICE_DIFF] = price_diff_wrt_yester

    price_change_wrt_today = (ltp - open) / open
    if price_change_wrt_today > 0 and price_change_wrt_today > max_price_rise_wrt_today[PRICE_DIFF]:
        max_price_rise_wrt_today[nse_bse.STOCK_ID] = stock_id
        max_price_rise_wrt_today[PRICE_DIFF] = price_change_wrt_today
    elif price_change_wrt_today < 0 and price_change_wrt_today < max_price_fall_wrt_today[PRICE_DIFF]:
        max_price_fall_wrt_today[nse_bse.STOCK_ID] = stock_id
        max_price_fall_wrt_today[PRICE_DIFF] = price_change_wrt_today

    low_open_diff = abs ((low - open) / open)
    if low_open_diff < bullish_marubuzo[PRICE_DIFF]:
        bullish_marubuzo[nse_bse.STOCK_ID] = stock_id
        bullish_marubuzo[PRICE_DIFF] = low_open_diff

    high_open_diff = abs ((high - open) / open)
    if high_open_diff < bearish_marubuzo[PRICE_DIFF]:
        bearish_marubuzo[nse_bse.STOCK_ID] = stock_id
        bearish_marubuzo[PRICE_DIFF] = high_open_diff

    if volume > max_vol_diff[VOL_DIFF]:
        max_vol_diff[nse_bse.STOCK_ID] = stock_id
        max_vol_diff[VOL_DIFF] = volume
