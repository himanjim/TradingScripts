import os
import traceback
from datetime import datetime, timedelta, date, time
import time
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import TradingScripts.ScrapUtils as sutils
import TradingScripts.Utils as util
from mpl_finance import candlestick_ohlc
from TradingScripts.ZerodhaInstMap import instruments_map

kite = util.intialize_kite_api()

max_data_len = 120
today_date_str = datetime.now().strftime ('%b-%d-%Y')
tradable_stocks = sutils.get_nse_fo_stocks()
# tradable_stocks = sutils.get_tradable_stocks_ids()
# top_20_traded_stocks = sutils.get_top_20_nse_traded_stocks(tradable_stocks)
# tradable_stocks.extend(top_20_traded_stocks)
# tradable_stocks = sutils.get_all_nse_stocks_ids()

candle_stick_chart_dir = ['G:', 'D:'][os.path.exists('D:')] + '/CandleSticks/'

counter = 1
for stock in tradable_stocks:
    try:
        file_path = candle_stick_chart_dir + str(counter) + '_' + stock[sutils.STOCK_ID] + '.png'
        counter += 1

        if os.path.exists(file_path):
            print(stock[sutils.STOCK_ID] + ' candles already printed.')
            continue

        if stock[sutils.STOCK_ID] not in instruments_map:
            print(stock[sutils.STOCK_ID] + ' not in map.')
            continue

        stock_datas = kite.historical_data(instruments_map[stock[sutils.STOCK_ID]], date.today() - timedelta(max_data_len), date.today(), 'day')

        lows = []
        opens = []
        highs = []
        closes = []
        timestamps = []

        for stock_data in stock_datas:
            timestamps.append(stock_data['date'])
            lows.append(stock_data['low'])
            opens.append(stock_data['open'])
            highs.append(stock_data['high'])
            closes.append(stock_data['close'])

        # if not(closes[-2] < closes[-10] and closes[-1] > opens[-2] and closes[-1] > closes[-2] and closes[-1] > opens[-1]):
        #     continue

        changed_dates = []
        for timestamp in timestamps:
            changed_dates.append(mdates.date2num(timestamp))

        lows = lows[-max_data_len:]
        opens = opens[-max_data_len:]
        highs = highs[-max_data_len:]
        closes = closes[-max_data_len:]
        changed_dates = changed_dates[-max_data_len:]
        # Plot candlestick.
        ##########################
        quotes = [tuple([changed_dates[i],
                         opens[i],
                         highs[i],
                         lows[i],
                         closes[i]]) for i in range(len(changed_dates))]
        fig, ax = plt.subplots(figsize=(20, 20))
        candlestick_ohlc(ax, quotes, width=0.6, colorup='g', colordown='r');

        # Customize graph.
        ##########################
        plt.rc('xtick', labelsize=20)
        plt.rc('ytick', labelsize=20)
        plt.xlabel('Date')
        plt.ylabel('Price')



        # Format time.
        ax.xaxis_date()
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
        ax.grid(b=True, axis='y')
        ax.xaxis.set_major_locator(mticker.MaxNLocator(10))

        plt.gcf().autofmt_xdate()  # Beautify the x-labels
        plt.autoscale(tight=True)
        plt.margins(.025, tight=True)

        fig.tight_layout()
        # Save graph to file. christropher.ko@cbsa-asfc.gc.ca

        plt.savefig(file_path)

        plt.clf()
        fig.clf()
        plt.close()
        ax.cla()

        time.sleep(1)

    except Exception:
        print(traceback.format_exc())
