import ScrapUtils as nse_bse
import Utils as util

stocks = nse_bse.get_nse_fo_stocks ()
# stocks = [{nse_bse.STOCK_ID: 'SUZLON', nse_bse.EXCHANGE: nse_bse.NSE}]

max_rises = {}
max_falls = {}

for stock in stocks:

    stock_datas = util.get_equity_historical_data (stock[nse_bse.STOCK_ID])

    high_low_diff = []

    for stock_data in stock_datas:
        if stock_data['open'] > stock_data['prev_close']:
            rise = (stock_data['open'] - stock_data['prev_close']) / stock_data['prev_close']
            if stock_data['date'] not in max_rises:
                max_rises[stock_data['date']]= stock_data
            else:
                prev_max_rise_stock_data = max_rises[stock_data['date']]
                prev_rise = (prev_max_rise_stock_data['open'] - prev_max_rise_stock_data['prev_close']) / prev_max_rise_stock_data['prev_close']
                if rise > prev_rise:
                    max_rises[stock_data['date']] = stock_data
        else:
            fall = (stock_data['prev_close'] - stock_data['open']) / stock_data['prev_close']
            if stock_data['date'] not in max_falls:
                max_falls[stock_data['date']] = stock_data
            else:
                prev_max_fall_stock_data = max_falls[stock_data['date']]
                prev_fall = (prev_max_fall_stock_data['prev_close'] - prev_max_fall_stock_data['open']) / prev_max_fall_stock_data['prev_close']
                if fall > prev_fall:
                    max_falls[stock_data['date']] = stock_data


print('Max rises:' + str(max_rises))
print('Max falls:' + str(max_falls))