# from scipy.signal import argrelmax,argrelmin
# import ta as tech_ana
code = input("Enter Python code to execute (or 'exit' to quit): ")

exit(0)
import csv
import datetime
import io
import math
import multiprocessing
import time
import time as sleep_time
import traceback
# from datetime import datetime
from urllib.request import Request, urlopen

# import face_recognition
import TradingScripts.DerivativeUtils as dutil
# import TradingScripts.GenericStatPrinter as gstats
import TradingScripts.ScrapUtils as sUtils
import TradingScripts.Utils as util
from TradingScripts.UpDownGapUtils import get_fo_stock_ids
from pytz import timezone, utc

x = datetime.datetime(2018, 9, 15)

print(x.strftime("%b %d %Y %H:%M:%S"))

exit(0)

kite = util.intialize_kite_api()
# print(kite.instruments())
print(kite.historical_data(260105, datetime.datetime(2021, 1, 22), datetime.datetime(2021, 1, 25), '3minute'))
exit(0)
#
# print(int(round(19249 / 100.0) * 100))

timestamp = datetime.datetime(2021, 1, 25, 15, 10, 6)
print(timestamp.hour)
print(timestamp.minute)
hr = 9
min =18
print(int((hr * 60) + (int(min/3) * 3)))
candles = {}
candles[123] = {'1': 12, 'LOW': 13}
print(candles)
candles[123]['LOW'] = 14
print(candles)
exit(0)

instruments_map = {}
with open('C:/Users/himan/Downloads/NSE_small.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    for row in csv_reader:
        instruments_map[row[1]] = row[0]

print(instruments_map)

exit(0)
# lines_to_write = '####Bought:' + ' at CE:' + ' and PE:'
# lines_to_write += '####Bought:'
# print(lines_to_write)
# exit(0)
# x = math.inf
#
# print(x == math.inf)
#
# exit(0)

instruments = kite.instruments()
# open('F:/Trading_Responses/instruments.txt','w').write(str(kite.instruments()))

tokens = {}
strikes = {}
for instrument in instruments:
    if instrument['name'] == 'BANKNIFTY' and instrument['expiry'] == datetime.date(2020, 4, 30) and instrument['segment'] == 'NFO-OPT':
        # print(instrument)
        tokens[instrument['exchange'] + ':' + instrument['tradingsymbol']] = {'tradingsymbol': instrument['tradingsymbol'], 'strike': instrument['strike'], 'instrument_type': instrument['instrument_type']}
        if instrument['strike'] not in strikes:
            strikes[instrument['strike']] = [instrument['tradingsymbol']]
        else:
            strikes[instrument['strike']].append(instrument['tradingsymbol'])
        # strikes.append(instrument['strike'])
    if instrument['name'] == 'NIFTY BANK' and instrument['segment'] == 'INDICES':
        # print (instrument)
        tokens[instrument['exchange'] + ':' + instrument['tradingsymbol']] = instrument['tradingsymbol']

print(tokens)
exit(0)

from TempData import CE, PE
from dateutil import parser
excel_location = 'F:/Trading_Responses/BankNiftyATM_Backtest_excel_' '.xlsx'

ce_data = {}
for candles in CE['data']['candles']:
    ce_data[parser.parse(candles[0])] = [candles[1], candles[2], candles[3], candles[4]]

pe_data = {}
for candles in PE['data']['candles']:
    pe_data[parser.parse(candles[0])] = [candles[1], candles[2], candles[3], candles[4]]

responses = []
for date, c_data in ce_data.items():
    if date in pe_data:
        responses.append([date, c_data[0], c_data[1], c_data[2], c_data[3], pe_data[date][0], pe_data[date][1], pe_data[date][2], pe_data[date][3]])

responses.insert (0, ['BUY DATETIME', 'CE(OPEN)', 'CE(HIGH)', 'CE(LOW)', 'CE(CLOSE)', 'PE(OPEN)', 'PE(HIGH)', 'PE(LOW)', 'PE(CLOSE)'])

gstats.print_statistics(responses, excel_location)

exit(0)
today = p_datetime.date.today()
first = today.replace(day=1)
lastMonth = first - p_datetime.timedelta(days=1)
print(lastMonth.strftime("%d-%b-%Y"))
print(lastMonth.replace(day=1).strftime ("%d-%b-%Y"))
exit(0)
arr = [1,2,3,4,5,6,7,8]
for i in range(5, len(arr)):
    print(arr[i])
print(arr[-1])
print(arr[-5])
print(arr[5:])
exit(0)
print(dutil.get_equity_live_ltp('HDFC'))

exit(0)
## Starting timer which keep track of the runtime
start_time = time.time()
image_dir = 'E:/RJIL_POS Crop Images Dec 2019/'

## Define the function which will be executed within the pool
def asyncProcess(data):
    print(face_recognition.face_encodings (face_recognition.load_image_file ('E:/RJIL_POS Crop Images Dec 2019/3729_60341259.jpg')))


## Define Pool Size of maximal concurrent processes
pool_size = 4

## Define an empty list where we store child processes
processes = []

if __name__ == '__main__':
    ## Define an empty pool whith maximal concurrent processes
    pool = multiprocessing.Pool (processes=pool_size)

    ## Firing in total of 10 processes
    pool.map(asyncProcess, [1, 2, 3, 4])

    print("---%d images encoded in %s seconds ---" % (pool_size, time.time() - start_time))

exit(0)
kite = util.intialize_kite_api()
# print(kite.margins())
print(kite.quote(['NSE:HDFC','NSE:HDFCBANK']))
exit(0)



print([1,2,3,4][-2:])
exit(0)
ascent_stocks = {}
ascent_stocks['a'] = ['a', 4]
ascent_stocks['b'] = ['b', 2]
ascent_stocks['c'] = ['c', 3]
print(ascent_stocks.items())
ascent_stocks = [(k[0], ascent_stocks[k[0]]) for k in sorted(ascent_stocks.items(), key=lambda x: x[1][1])]
print(ascent_stocks)



today_date = datetime.now().date()
print(kite.quote('NSE:HDFC')['NSE:HDFC']['timestamp'].date() == today_date)
exit(0)
fo_stock_ids = get_fo_stock_ids('C:/Users/Admin/Desktop/')
stocks_to_buy = []
stocks_to_sell = []
while datetime.now().time () < p_datetime.time(9, 15, 50):
    pass
ascent_stocks = {}
descent_stocks = {}

stocks_live_data = kite.quote (fo_stock_ids)
stock_in_uptrend = None
stock_in_downtrend = None

for key, stock_live_data in stocks_live_data.items ():
    open_price = stock_live_data['ohlc']['open']
    ltp = stock_live_data['last_price']
    high_price = stock_live_data['ohlc']['high']
    low_price = stock_live_data['ohlc']['low']
    close = stock_live_data['ohlc']['close']

    ascent = 0
    descent = 0

    if open_price > close:
        ascent = abs ((open_price - close) / close)
        ascent_stocks[key] = ascent
    elif open_price < close:
        descent = abs ((open_price - close) / close)
        descent_stocks[key] = descent

    if stock_in_uptrend is None or ascent > stock_in_uptrend[1]:
        stock_in_uptrend = [key, ascent]

    if stock_in_downtrend is None or descent > stock_in_downtrend[1]:
        stock_in_downtrend = [key, descent]

if stock_in_uptrend[1] != 0:
    print(stock_in_uptrend)
    if stock_in_uptrend[0] not in stocks_to_sell:
        stocks_to_sell.append(stock_in_uptrend[0])

if stock_in_downtrend[1] != 0:
    print (stock_in_downtrend)
    if stock_in_downtrend[0] not in stocks_to_buy:
        stocks_to_buy.append (stock_in_downtrend[0])

ascent_stocks = [(k, ascent_stocks[k]) for k in sorted (ascent_stocks, key=ascent_stocks.get, reverse=True)]
descent_stocks = [(k, descent_stocks[k]) for k in sorted (descent_stocks, key=descent_stocks.get, reverse=True)]

print ('Ascent_stocks:')
print(ascent_stocks)
print ('Descent_stocks:')
print (descent_stocks)

exit(0)


kite = util.intialize_kite_api()
today_date = datetime.now().date()
print(kite.quote('NSE:HDFC')['NSE:HDFC']['timestamp'].date() == today_date)
exit(0)

co_bo_stocks = []
try:
    with urlopen (Request ('http://www.sharecsv.com/s/9e419f5ff41230a35e9921b94edb039b/CM_Margin_Sep-13-2019.csv', headers={'User-Agent': 'Mozilla/5.0'})) as csv_file:
        csv_reader = csv.reader (io.StringIO (csv_file.read ().decode ('utf-8')), delimiter=',')
        line_count = 0
        for row in csv_reader:
            co_bo_stocks.append(row[0].strip ())
except Exception as e:
    print (traceback.format_exc ())
print('co_bo_stocks', len(co_bo_stocks))
exit(0)

print(kite.quote('NSE:HDFC'))
exit(0)
print(((datetime.now(indian_timezone) - kite.orders()[0]['order_timestamp'].replace(tzinfo=indian_timezone)).seconds - 1380))




a = {'r': 'q'}
a['p'] = ['q']
a['p'].append('r')

print(a)
print(list(a.values())[0])
print(a)
exit(0)

exclude = ['ABB', 'ABFRL', 'ALKEM', 'AUBANK', 'BAJAJHLDNG', 'BANDHANBNK', 'BBTC', 'COROMANDEL', 'CROMPTON', 'DBL', 'DMART', 'ENDURANCE', 'FCONSUMER', 'FRETAIL', 'GICRE', 'GODREJAGRO', 'GODREJIND', 'GSKCONS', 'GSPL', 'HUDCO', 'ICICIGI', 'JSWENERGY', 'LTI', 'LTTS', 'MPHASIS', 'MRPL', 'NATCOPHARM', 'NAUKRI', 'NHPC', 'NIACL', 'PFIZER', 'PGHH', 'PNBHOUSING', 'PRESTIGE', 'QUESS', 'RAJESHEXPO', 'RPOWER', 'SBILIFE', 'SPARC', 'SYNGENE', 'VARROC', 'VGUARD']
co_bo_stocks = []
try:
    with open ('http://www.sharecsv.com/s/9e419f5ff41230a35e9921b94edb039b/CM_Margin_Sep-13-2019.csv') as csv_file:
        csv_reader = csv.reader (csv_file, delimiter=',')
        for row in csv_reader:
            co_bo_stocks.append(row[0].strip ())
except Exception as e:\
    print (traceback.format_exc ())
print('co_bo_stocks', len(co_bo_stocks))
fo_stocks = sUtils.get_nse_fo_stocks()
fo_stock_ids = []
for fo_stock in fo_stocks:
    fo_stock_ids.append(fo_stock[sUtils.STOCK_ID])
print('fo_stock_ids', len(fo_stock_ids))

count = 1
for co_bo_stock in co_bo_stocks:
    if co_bo_stock not in fo_stock_ids:
        print('%d CO BO stock:%s not in FO stock' %(count, co_bo_stock))
        count += 1

exit(0)


print(kite.orders()[5]['exchange_timestamp'].replace(tzinfo=timezone('America/Los_Angeles')))
exit(0)

print(kite.quote('NSE:HDFC'))
exit(0)
lot_multi_factor =  .9
print(util.round_to_tick(200 * lot_multi_factor))
net_profit = 7100 - 4000
net_profit = round (1000 * math.floor (net_profit / 1000), 0)
print(net_profit)
print(util.round_to_tick(net_profit * lot_multi_factor))
print(round ((lot_multi_factor * 500) * math.floor ((2600 - (lot_multi_factor * 1000)) / (lot_multi_factor * 500)), 0))
exit(0)
# print((datetime.now(indian_timezone) - parent_order['exchange_timestamp']).seconds)

x = collections.deque(5*[None], 5)
x.appendleft(2.35)
x.appendleft(3)
print(x)
for ele in x:
    print(ele > 0)
exit(0)

logging.basicConfig(format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

def customTime(*args):
    utc_dt = utc.localize(datetime.utcnow())
    my_tz = timezone("US/Eastern")
    converted = utc_dt.astimezone(my_tz)
    return converted.timetuple()

logging.Formatter.converter = customTime
logger.error("customTime")

print(datetime.now(timezone('US/Eastern')))
exit(0)

lot_multi_factor = 32 / 200
print(round ((lot_multi_factor * 1000) * math.floor ((1 - (lot_multi_factor * 4000)) / (lot_multi_factor * 1000)), 0))
exit(0)

fo_lots = nse_bse.get_nse_fo_lots()

responses = []
with open ('C:/Users/Admin/Desktop/Technical analysis testing - Tradebook 1.csv') as csv_file:
    csv_reader = csv.reader (csv_file, delimiter=',')
    for row in csv_reader:
        stock_id = (row[1]).strip ()
        price = float((row[2]).strip ())
        if price != 0 and stock_id in fo_lots:
            responses.append([(row[0]).strip (), stock_id, fo_lots[stock_id], int(500000 / price)])

gstat.print_statistics (responses, 'C:/Users/Admin/Desktop/res.csv')
exit(0)

re_prodId = re.compile(r'Symbol:\"(\w+?)\"')
print(re_prodId.findall('{ success:true ,results:103,rows:[{Symbol:"CLNINDIA",CompanyName:"Clariant Chemicals (India) Limited",ISIN:"INE492A01029",Ind:"-",Purpose:"Financial Results",BoardMeetingDate:"08-Aug-2019",DisplayDate:"04-Jul-2019",seqId:"103630857",Details:"To consider and approve the financial results for the period ended Jun 30, 2019"},{Symbol:"COFFEEDAY",CompanyName:"Coffee Day Enterprises Limited",ISIN:"INE335K01011",'))
exit(0)

today_date_str = datetime.now ().strftime ('%Y-%m-%d')
logFormatter = logging.Formatter("%(asctime)s [%(module)s] [%(levelname)-5.5s]  %(message)s")
logger = logging.getLogger('Trading')
logger.setLevel(logging.INFO)

fileHandler = logging.FileHandler('F:/test.log')
fileHandler.setFormatter(logFormatter)
logger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)
start_time = datetime.now().microsecond
logger.info('test')
print((datetime.now().microsecond - start_time) )
exit(0)

lot = 1
lot *= 2
print(lot)
exit(0)
kite = util.intialize_kite_api()
print(kite.quote('NSE:BERGEPAINT'))
print(kite.quote('NSE:BERGEPAINT', 'NSE:TORNTPOWER'))
exit(0)
# print(datetime.strptime('2019-08-05 09:15:00.846397', '%Y-%m-%d %H:%M:%S.%f').time() > util.MARKET_START_TIME)
# print(494.5 - .35 + (abs(1000) / (.3 * 1100)))
print(494.5 - .35 + (abs(1000) / (.3 * 1100)))
print(1000 / (298.6 - 297.40))
half_future_lot = 1851 / 2
print([1, 690 / half_future_lot][690 < half_future_lot])
print(('SELL' == 'BUY' and (330.3351 - 330.55) >= 20) or ('SELL' == 'SELL' and (330.55 - 330.3351) >= 20) or 148.281 >= (0.7455429497568882 * 20000))

exit(0)

logFormatter = logging.Formatter("%(asctime)s [%(module)s] [%(levelname)-5.5s]  %(message)s")
rootLogger = logging.getLogger()

fileHandler = logging.FileHandler('F:test.txt')
fileHandler.setFormatter(logFormatter)
rootLogger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
rootLogger.addHandler(consoleHandler)

rootLogger.setLevel(logging.INFO)
rootLogger.info('Test info.')
rootLogger.info('Test error.')

exit(0)

half_future_lot = 1851 / 2
print([1, 690 / half_future_lot][690 < half_future_lot])
exit(0)
positions_day = kite.positions ()['day']
positions_net = kite.positions ()['net']
for position in positions_day:
    if position['tradingsymbol'].upper() == 'INFRATEL':
        print(position)
        print(position['unrealised'], position['realised'])
        break

for position in positions_net:
    if position['tradingsymbol'].upper() == 'INFRATEL':
        print(position)
        print(position['unrealised'], position['realised'])
        break


kite = util.intialize_kite_api()
orders = kite.orders()
kite.modify_order(kite.VARIETY_CO, '190729001773509', parent_order_id='190729001773508', trigger_price=252.0)

exit(0)

print(math.ceil(1.234))
exit(0)

def increment():
    global test
    test += 1

test = 0
print(test)
increment()
print(test)
exit(0)

kite = util.intialize_kite_api()
order_id = kite.place_order(tradingsymbol="JUBLFOOD",
                                variety=kite.VARIETY_CO ,
                                exchange=kite.EXCHANGE_NSE,
                                transaction_type=kite.TRANSACTION_TYPE_BUY,
                                quantity=1,
                                order_type=kite.ORDER_TYPE_LIMIT,
                                product=kite.PRODUCT_CO,
                            price=1165,
                            trigger_price=1160)

print(order_id)

exit(0)

logger = logging.getLogger('spam_application')
logger.setLevel(logging.INFO)
fh = logging.FileHandler('F:/test.txt')
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.info ('Cancelled order of symbol:%s and id:%d.' , 'LT', 12345)
logger.error('Test error.')

exit(0)

print(kite.orders())
print(kite.trades())

# for o in kite.instruments():
#     for i, (key, value) in enumerate(o.items()):
#         if i == 2 and value in ('INFRATEL', 'IDFCFIRSTB', 'ASIANPAINT', 'CANBK', 'JUBLFOOD', 'ICICIPRULI',):
#             print(o)
# start_time = datetime.now()
# print(kite.quote('NSE:JUBLFOOD', 'NSE:INFRATEL', 'NSE:IDFCFIRSTB', 'NSE:ICICIPRULI', 'NSE:ASIANPAINT', 'NSE:CANBK'))
# print(datetime.now(), (datetime.now() - start_time).seconds)



exit(0)
datetime.now().astimezone(pytz.timezone('Asia/Kolkata'))

stock_tickers = {}
stocks = []
print(util.is_number(str('190722000342580|1').split('|')[0]))
print ((datetime.now() > datetime.fromtimestamp(1563555469)))


try:
    with open ('C:/Users/Admin/Desktop/CM_Margin_Jun-28-2019.csv') as csv_file:
        csv_reader = csv.reader (csv_file, delimiter=',')
        for row in csv_reader:
            stocks.append(row[0].strip ())
except Exception as e:\
    print (traceback.format_exc ())

for stock in stocks:
    try:
        page = urlopen(Request(
            'https://tvc4.forexpros.com/783a9d5e01ae63dabd6a68480bb0f3e4/1556685636/56/56/23/search?limit=30&query={0}&type=&exchange='.format(stock), headers={'User-Agent': 'Mozilla/5.0', 'Referer': 'https://in.investing.com'}))
        # parse the html using beautiful soup and store in variable `soup`
        soup = BeautifulSoup(page, 'html.parser')

        datas = json.loads(soup.string.strip())

        stock_name = ''
        for data in datas:
            if data['exchange'] == 'NSE' and data['type'] == 'Stock' and stock[0].upper() == data['description'][0].upper():
                ticker = data['ticker']
                stock_name = data['description']
                stock_tickers[stock.upper()] = {'ticker': ticker, 'description': stock_name}
                break

        sleep_time.sleep(1)
    except Exception:
        print(traceback.format_exc())

print(stock_tickers)
exit(0)

MARKET_START_TIME_AFTER_VOLATILITY = time (9, 15, 1, 0)

while datetime.now().time() < MARKET_START_TIME_AFTER_VOLATILITY:
    pass

for i in range(1,5):
    FUTURE_HEADERS = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://kite.zerodha.com/chart/ext/ciq/NSE/ITC/424961'}
    today_timestamp = int(
        datetime(datetime.today().year, datetime.today().month, datetime.today().day, 15, 58, 56).timestamp())
    page = urlopen(Request(
        'https://kitecharts-aws.zerodha.com/api/chart/141569/day?public_token=JkPP1XVYS4DCfWYf2ifIeC65afq55Cy0&user_id=DH1355&oi=1&api_key=kitefront&access_token=&from=2019-06-24&to=2019-06-27&ciqrandom=1561600055480' + str(
            today_timestamp),
        headers=FUTURE_HEADERS))
    # parse the html using beautiful soup and store in variable `soup`
    soup = BeautifulSoup(page, 'html.parser')

    js = json.loads(soup.string.strip())

    print(js)
    print(datetime.now())

exit(0)

locale.setlocale(locale.LC_ALL, '')
print(locale.currency(100000, grouping=True))

exit(0)

print(round(util.min_tick_size * round(
    (1000 / 125) / util.min_tick_size), 2))
exit(0)



# from upstox_api.api import TransactionType
dates = ['2019-03-01', '2019-03-04', '2019-03-05', '2019-03-06', '2019-03-07']
changed_dates = []
for date in dates:
    changed_dates.append(mdates.date2num(datetime.strptime(date, '%Y-%m-%d')))

kurse_l = [172.889999, 173.970001, 174.539993, 173.940002, 172.020004]
kurse_o = [174.279999, 175.690002, 175.940002, 174.669998, 173.869995]
kurse_h = [175.149994, 177.750000, 176.000000, 175.490005, 174.440002]
kurse_c = [174.970001, 175.850006, 175.529999, 174.520004, 172.500000]
# Plot candlestick.
##########################
quotes = [tuple([changed_dates[i],
                 kurse_o[i],
                 kurse_h[i],
                 kurse_l[i],
                 kurse_c[i]]) for i in range(5)]
fig, ax = plt.subplots()
candlestick_ohlc(ax, quotes, width=0.5, colorup='g', colordown='r');

# Customize graph.
##########################
plt.xlabel('Date')
plt.ylabel('Price')
plt.title('Apple')

# Format time.
ax.xaxis_date()
ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))

plt.gcf().autofmt_xdate()  # Beautify the x-labels
plt.autoscale(tight=True)

# Save graph to file.
plt.savefig('F:/mpl_finance-apple.png')
exit(0)

print(util.round_to_tick(659.3045))
exit(0)

print(util.get_date_from_timestamp(1559533557379466 / 1000).time())
print((datetime.now() - util.get_date_from_timestamp(1559533557379466 / 1000)).seconds)
exit(0)

print (round (
    util.min_tick_size * round ((150 / 4000) / util.min_tick_size), 2))
exit (0)

lots = nse_bse.get_nse_fo_lots ()
stocks = nse_bse.get_nse_fo_stocks ()

earnings = {}

for stock in stocks:
    stock_id = stock[nse_bse.STOCK_ID]
    if stock_id in c_limit.calculated_calendar_spread_limits:
        earnings[stock_id] = lots[stock_id] * (c_limit.calculated_calendar_spread_limits[stock_id][0] -
                                               c_limit.calculated_calendar_spread_limits[stock_id][1])
    else:
        print ('No limit found for:', stock_id)

listofTuples = sorted (earnings.items (), reverse=True, key=lambda x: x[1])

# Iterate over the sorted sequence
for elem in listofTuples:
    print (elem[0], " ::", elem[1])

exit (0)

x = {}
x.update ({'1': '2'})
print (x)
exit (0)

x = [1, 3, 3.1, 3, 4, 2, 3, 4, 3, 2, 5]
print (x[-2:])
exit (0)

high_low_means = {}
high_low_means['test'] = 1.2
high_low_means['test1'] = 1.3



FUTURE_HEADERS = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://tvc-invdn-com.akamaized.net'}
today_timestamp = int(
    datetime(datetime.today().year, datetime.today().month, datetime.today().day, 15, 58, 56).timestamp())
page = urlopen(Request(
    'https://tvc4.forexpros.com/783a9d5e01ae63dabd6a68480bb0f3e4/1556685636/56/56/23/history?symbol=993246&resolution=D&from=1525585469&to=' + str(
        today_timestamp),
    headers=FUTURE_HEADERS))
# parse the html using beautiful soup and store in variable `soup`
soup = BeautifulSoup(page, 'html.parser')

js = json.loads(soup.string.strip())
print(util.round_to_tick(js['c'][-1]))
print(util.round_to_tick(js['o'][-1]))
print(js['t'][-1])
today_date = datetime.today().date()
dt = util.get_date_from_timestamp(int(js['t'][-1]) * 1000)

print(dt)
print(datetime.today())

exit(0)

orders = [{'lot': 455, 'stop_loss': 2.2, 'square_off': 6.6, 'square_off_secon': 4.4, 'trailing_ticks': 20, 'action': 0,
           'price_pts': 0.7, 'trigger_price_pts': 0.6, 'price_pts_secon': 0.9, 'trigger_price_pts_secon': 0.8,
           'live_data_url': "'https://tvc4.forexpros.com/783a9d5e01ae63dabd6a68480bb0f3e4/1556685636/56/56/23/history?symbol=18040&resolution=D&from=1525704109&to='+str (today_timestamp)",
           'instrument': "upstox_api.get_instrument_by_symbol (nse_bse.NSE, 'BPCL')"},
          {'lot': 455, 'stop_loss': 2.2, 'square_off': 6.6, 'square_off_secon': 4.4, 'trailing_ticks': 20, 'action': 0,
           'price_pts': 0.7, 'trigger_price_pts': 0.6, 'price_pts_secon': 0.9, 'trigger_price_pts_secon': 0.8,
           'live_data_url': "'https://tvc4.forexpros.com/783a9d5e01ae63dabd6a68480bb0f3e4/1556685636/56/56/23/history?symbol=18040&resolution=D&from=1525704109&to='+str (today_timestamp)",
           'instrument': "upstox_api.get_instrument_by_symbol (nse_bse.NSE, 'BPCL')"}]

print ('[')
for order in orders:
    print (' {')
    for k, v in order.items ():
        print ("   '" + k + "':", str (v) + ",")
    print (' },')
print (']')
exit (0)

print(int(datetime(datetime.today().year, datetime.today().month, datetime.today().day, 15, 58, 56).timestamp()))
exit(0)
# ind, price = '0#1234.5'.split('#')
# print(ind, price)
# exit(0)


upstox_api = util.intialize_upstox_api ([nse_bse.NSE_FO, nse_bse.NSE])
print (upstox_api.get_order_history ())

exit (0)

upstox_api = util.intialize_upstox_api([nse_bse.NSE_FO, nse_bse.NSE])
orders = upstox_api.get_order_history()
if orders is not None and len(orders) > 0:
    for order in orders:
        if order['parent_order_id'] == 'NA' and order['status'] == 'open':
            upstox_api.cancel_order(order['order_id'])
            print('Order cancelled:', order['order_id'])
else:
    print('No orders found')
print(upstox_api.get_order_history())
exit (0)
# [{'exchange': 'NSE_EQ', 'product': 'OCO', 'symbol': 'GODFRYPHLP', 'token': 1181, 'buy_amount': 0, 'sell_amount': 614163.55, 'buy_quantity': 0, 'sell_quantity': 539, 'cf_buy_amount': 0, 'cf_sell_amount': 0, 'cf_buy_quantity': 0, 'cf_sell_quantity': 0, 'avg_buy_price': '', 'avg_sell_price': 1139.45, 'net_quantity': -539, 'close_price': 1141.95, 'last_traded_price': 1136.8, 'realized_profit': '', 'unrealized_profit': 1428.350000000049, 'cf_avg_price': ''}, {'exchange': 'NSE_EQ', 'product': 'OCO', 'symbol': 'NBCC', 'token': 31415, 'buy_amount': 0, 'sell_amount': 259200, 'buy_quantity': 0, 'sell_quantity': 4000, 'cf_buy_amount': 0, 'cf_sell_amount': 0, 'cf_buy_quantity': 0, 'cf_sell_quantity': 0, 'avg_buy_price': '', 'avg_sell_price': 64.8, 'net_quantity': -4000, 'close_price': 65, 'last_traded_price': 65.05, 'realized_profit': '', 'unrealized_profit': -1000, 'cf_avg_price': ''}]
# print('a', 'b')
# exit(0)
# data = pickle.load(open(util.get_instrument_latest_data_file_name(), 'rb'))
#
# print(data)
#
# exit(0)


# while util.is_market_open () is False:
#     pass
#
# while True:
#     print (outil.get_equity_live_ltp ('TCS'))
#     print ('Curr. time:', datetime.datetime.now ())
#     time.sleep (.5)
#
# n = [{'a': 1, 'b': 2}]
# print (1 in x['a'] for x in n)
# exit (0)
upstox_api = util.intialize_upstox_api ([nse_bse.NSE_FO, nse_bse.NSE])
util.unsubscribe_symbols (upstox_api)
print (util.round_to_tick (6.9789768))
exit (0)
price_data = {}
price_data['test'] = []
price_data['test'].insert(0, {'t': 1, 'ltp': 2})
price_data['test'].insert (0, {'t': 2, 'ltp': 3})
# price_data['test'].pop()
print(price_data)
exit(0)
test = {}
test['test'] = {'test1': 1}
print('test' in test)
exit(0)

print('Curr(10)', datetime.datetime.now() - datetime.timedelta(seconds=10))
print('Curr', datetime.datetime.now())
exit(0)
data = pickle.load(open(util.get_instrument_latest_data_file_name(), 'rb'))

print(len(data))
timestamps = []
for stock_id, live_quote in data.items():
    timestamps.append(live_quote['timestamp'])

timestamps.sort(reverse=True)

# # print (data)
# exit (0)
for timestamp in timestamps:
    print(timestamp)

print((max(data[x]['timestamp'] for x in data)))
print((min(data[x]['timestamp'] for x in data)))
print('Curr', datetime.datetime.now())
exit(0)

# upstox_api = util.intialize_upstox_api ([nse_bse.NSE_FO, nse_bse.NSE])
# util.unsubscribe_symbols (upstox_api)
# print (util.round_to_tick (6.9789768))
# exit (0)

data = pickle.load (open (util.get_instrument_latest_data_file_name (), 'rb'))
print (data)
print (len (data))
# print (util.round_to_tick (6.9589768))
# print(datetime.datetime.now().date() == datetime.datetime(2019, 3, 4))
exit (0)

test = {}
test['test'] = {'test': 1}
print (test)
test['test'] = {'test': 2}
print (test)
exit (0)

# options = outil.get_all_strikes ('HDFC', datetime.datetime.now ().month, datetime.datetime.now ().year)
# print (ostrat.get_bear_call_spreads (options))
# print (ostrat.get_bear_put_spreads (options))
# print (ostrat.get_bull_call_spreads (options))
# print (ostrat.get_bull_put_spreads (options))

exit (0)

start_time = time.time ()
arr = []
for i in range (1, 2000):
    arr.append (i)
mean = statistics.mean (arr)
print (mean)
print (time.time () - start_time)
exit (0)
traded = {}
traded.update ({'prev_price': 5})
print (traded)
traded.update ({'prev5_price': 6})
print (traded)
traded.clear ()
print (traded)
exit (0)

x = - math.inf
print (0 > x)
exit (0)

now = datetime.datetime.now()
print(now.time())
print(util.is_market_open())
exit(0)
print (
    outil.get_last_thurday_of_month (datetime.datetime.today ().date ().month, datetime.datetime.today ().date ().year))
exit (0)
print (util.trim_date (datetime.datetime (2019, 3, 4)))
exit (0)

frequency = 2500  # Set Frequency To 2500 Hertz
duration = 1000  # Set Duration To 1000 ms == 1 second
winsound.Beep(frequency, 300)
exit(0)

print (round (79.89) == round (79.80))
exit (0)

x = [1, 3, 3.1, 3, 4, 2, 3, 4, 3, 2, 5]
print ([i for i in x if i == 4])
exit (0)

print(datetime.datetime.now())
exit(0)
# test = [{'exchange': 'NSE_FO', 'product': 'D', 'symbol': 'BANKBARODA19JANFUT', 'token': 52373, 'buy_amount': 0, 'sell_amount': 0, 'buy_quantity': 0, 'sell_quantity': 0, 'cf_buy_amount': 486800, 'cf_sell_amount': 0, 'cf_buy_quantity': 4000, 'cf_sell_quantity': 0, 'avg_buy_price': 121.7, 'avg_sell_price': '', 'net_quantity': 4000, 'close_price': 121.7, 'last_traded_price': 121.65, 'realized_profit': '', 'unrealized_profit': -199.99999999998863, 'cf_avg_price': '122.75'}]
# pickle_file1 = open('C:/Users/Admin/Desktop/New Text Document.txt','wb')
# pickle.dump(test, pickle_file1)
pickle_file1 = open('C:/Users/Admin/Desktop/New Text Document.txt', 'rb')
print(pickle.load(pickle_file1))
exit(0)

a = []
a.append('aaa')
print('aaa' in a)
exit(0)

a = {}
a.update({'aaa': 2.0})
print('aaa' in a)
print(a['aaa'])
exit(0)

age = input("What is your age? ")
print("Your age is: ", age)
exit(0)

# upstox_api = util.intialize_upstox_api([nse_bse.NSE_FO])
# print (upstox_api.get_positions()[0]['symbol'])
# print(upstox_api.get_trade_book())
# [{'exchange': 'NSE_FO', 'product': 'D', 'symbol': 'BANKBARODA19JANFUT', 'token': 52373, 'buy_amount': 0, 'sell_amount': 0, 'buy_quantity': 0, 'sell_quantity': 0, 'cf_buy_amount': 486800, 'cf_sell_amount': 0, 'cf_buy_quantity': 4000, 'cf_sell_quantity': 0, 'avg_buy_price': 121.7, 'avg_sell_price': '', 'net_quantity': 4000, 'close_price': 121.7, 'last_traded_price': 121.65, 'realized_profit': '', 'unrealized_profit': -199.99999999998863, 'cf_avg_price': '122.75'}]
exit(0)

# print(cutil.get_fair_future_value(7392.40, 7, 0))
exit(0)

x = [1, 3, 3.1, 3, 4, 2, 3, 4, 3, 2, 5]
print (all (3 > i for i in x) is False)
exit (0)

class Option1:
    def __init__(self, strike_price, oi, symbol):
        self.strike_price = strike_price
        self.oi = oi
        self.symbol = symbol

    def __str__(self):
        return str(self.strike_price)


def Option(strike_price, call_oi, put_oi, options):
    x = Option1(strike_price, call_oi, 'HDFC' + str(strike_price) + 'ce')
    y = Option1(strike_price, put_oi, 'HDFC' + str(strike_price) + 'PE')

    options.append(x)
    options.append(y)


print (urllib.parse.unquote ('M%26M'))
exit (0)

print ([1, 3, 3.1, 3, 4, 2, 3, 4, 3, 2, 5][0:4 + 1])
print ([1, 3, 3.1, 3, 4, 2, 3, 4, 3, 2, 5][-4:])
exit (0)

print (outil.get_all_bids ('HDFCBANK', datetime.datetime (2019, 2, 28).date ()))
# print(outil.get_all_bids('HDFC', datetime.datetime(2019, 1, 31).date()).strip())
exit (0)

# print (cutil.get_fair_future_value (2280.5, 80, 0))
exit (0)
date1 = parser.parse ('23-Feb-2018')
date2 = parser.parse ('24-Feb-2018')

print ((date2 - date1).days)
# for d in data.split(':'):
#     for e in d.split(','):
#         print(e.replace('"', ''))
exit(0)
exit(0)

options = outil.get_all_strikes('ITC', 1, 2019, '200')
for option in options:
    print(option)

exit(0)

array1 = [0, 10, 20, 40]
array2 = [0, 10, 20, 40, 50]
array3 = [0, 10, 20, 40, 50, 60]

for data1, data2, data3 in zip(reversed(array1), reversed(array2), reversed(array3)):
    print(data1, data2, data3)

reversed(array1)
for data1 in array1:
    print(data1)

exit(0)

# print(datetime.datetime.strptime('31JAN2019', '%d%b%Y'))
# exit(0)
futures = outil.get_all_futures('HDFC', 1, 19)

for future in futures:
    print(future)

exit(0)


kite = util.intialize_kite_api()
# instruments = (kite.instruments())
# with open('C:/Users/Admin/Desktop/intruments.txt', 'a') as the_file:
#     for instrument in instruments:
#         the_file.write(str(instrument)+'\n')

print(kite.historical_data(18362370, datetime.datetime(2018, 8, 21).date(), datetime.datetime(2019, 1, 1).date(),
                             'day', continuous=True))
exit(0)

print (parser.parse (
    '"HDFCBANK","29-Dec-2016","25-Jan-2017","1189.85","1208.95","1187.7","1206.7","1206.1","1206.7","15087","90380","28076500","3623000","'.split (
        ',')[1].split ('"')[1].strip ()))
exit (0)

matchObj = re.match (r'.*>(.*)<.*',
                     '<DT><A HREF="http://www.girls-mag.de/images/product_images/info_images/711_1.jpg" ADD_DATE="1487221468">   red cute </A>')
if matchObj:
    print ("matchObj.group(1) : ", matchObj.group (1))
    words = matchObj.group (1).split ()
    print (words)

exit (0)

future_data = {'timestamp': 1545991201000, 'exchange': 'NSE_FO', 'symbol': 'JPASSOCIAT19JANFUT', 'ltp': 7.7,
               'open': 7.75, 'high': 7.8, 'low': 7.6, 'close': 7.7, 'vtt': 22880000, 'atp': 7.7, 'oi': 111320000,
               'spot_price': 7.65, 'total_buy_qty': 9405000, 'total_sell_qty': 8305000, 'lower_circuit': 6.95,
               'upper_circuit': 8.5, 'yearly_low': '', 'yearly_high': '',
               'bids': [{'quantity': 990000, 'price': 7.65, 'orders': 18},
                        {'quantity': 1375000, 'price': 7.6, 'orders': 22},
                        {'quantity': 1100000, 'price': 7.55, 'orders': 4},
                        {'quantity': 1320000, 'price': 7.5, 'orders': 11},
                        {'quantity': 220000, 'price': 7.45, 'orders': 3}],
               'asks': [{'quantity': 1265000, 'price': 7.75, 'orders': 22},
                        {'quantity': 715000, 'price': 7.8, 'orders': 12},
                        {'quantity': 990000, 'price': 7.85, 'orders': 7},
                        {'quantity': 1045000, 'price': 7.9, 'orders': 7},
                        {'quantity': 935000, 'price': 7.95, 'orders': 6}], 'ltt': 1545991198000}
print (outil.is_instrument_liquid (future_data['bids'], future_data['asks']))
exit (0)

# s = ' ddd 2,000.00 '
# print(util.remove_non_no_chars(s))
# exit (0)

options = []
Option(7000, 1404300, 4087050, options)
Option(7100, 335700, 1029150, options)
Option(7200, 482100, 2977875, options)
Option(7300, 422475, 1975650, options)
Option(7400, 963900, 2336700, options)
Option(7500, 999975, 4548450, options)
Option(7600, 785550, 3690900, options)
Option(7700, 1823400, 5783025, options)
Option(7800, 3448575, 4864125, options)
Option(7900, 5367450, 2559375, options)
Option(8000, 6510975, 1447125, options)
Option(8100, 5900325, 310500, options)
Option(8200, 5113350, 248775, options)
Option(8300, 3844500, 355725, options)
Option(8400, 2135625, 255525, options)
Option(8500, 2252250, 488475, options)
Option(8600, 1083750, 58500, options)

print(outil.get_pcr(options))
exit(0)

print(util.is_number(8.998))

exit(0)

highest_vol_daily_responses = []

highest_vol_daily_responses.append(['a', 'b'])
highest_vol_daily_responses.append(['c', 'd'])
highest_vol_daily_responses.insert(0, ['e', 'f'])

print(highest_vol_daily_responses)

exit(0)

highest_vols_daily = {}

highest_vols_daily['1'] = {'a': 'b'}
highest_vols_daily['2'] = {'c': 'd'}

for key, highest_vol_daily in highest_vols_daily.items():
    print(highest_vol_daily)
exit(0)

t = [1, 3, 3.1, 3, 4, 2, 3, 4, 3, 2, 5]

i = 6

print (t[i - 3:i])
exit (0)

#
# today_date=datetime.today().date()
# print (today_date.strftime ("%y"))
# print (today_date.strftime ("%b").upper ())
# exit(0)
#
# options = []
# Option (7000, 1404300, 4087050, options)
# Option (7100, 335700, 1029150, options)
# Option (7200, 482100, 2977875, options)
# Option (7300, 422475, 1975650, options)
# Option (7400, 963900, 2336700, options)
# Option (7500, 999975, 4548450, options)
# Option (7600, 785550, 3690900, options)
# Option (7700, 1823400, 5783025, options)
# Option (7800, 3448575, 4864125, options)
# Option (7900, 5367450, 2559375, options)
# Option (8000, 6510975, 1447125, options)
# Option (8100, 5900325, 310500, options)
# Option (8200, 5113350, 248775, options)
# Option (8300, 3844500, 355725, options)
# Option (8400, 2135625, 255525, options)
# Option (8500, 2252250, 488475, options)
# Option (8600, 1083750, 58500, options)
#
# options[:] = [x for x in options if outil.is_option_outside_1_sd ([8050, 7650], x.strike_price)]
#
# for option_pe in options:
#     print (option_pe)
# exit (0)
#
# bids = [{'quantity': 500, 'price': 31.35, 'orders': 1}, {'quantity': 500, 'price': 31.1, 'orders': 1},
#         {'quantity': 500, 'price': 30.4, 'orders': 1}, {'quantity': 500, 'price': 30.2, 'orders': 1},
#         {'quantity': 1000, 'price': 26.4, 'orders': 1}]
#
# asks = [{'quantity': 500, 'price': 32.7, 'orders': 1}, {'quantity': 500, 'price': 32.75, 'orders': 1},
#      {'quantity': 1000, 'price': 33.9, 'orders': 1}, {'quantity': 500, 'price': 34, 'orders': 1},
#      {'quantity': 500, 'price': 34.5, 'orders': 1}]
#
# # t =[1, 3, 3.1, 3, 4, 2, 3, 4, 3, 2, 5]
#
# print (outil.is_option_liquid (bids, asks))
# exit (0)
#
# date = scutil.get_last_thurday_of_month (1, 2019)
# print (date.strftime ("%d"))
# print (date.strftime ("%b").upper ())
# print (scutil.get_implied_volatility ('HDFCBANK', 2100, 12, 2018))
# exit (0)
#
# print (util.convert_daily_to_yearly_return (100, 6, 100))
# print (util.convert_monthly_to_yearly_return (.02))
# exit (0)
#
# upstox_api = util.intialize_upstox_api (['NSE_INDEX'])
# future_current_month_latest_data = upstox_api.get_ohlc (upstox_api.get_instrument_by_symbol ('NSE_INDEX', 'NIFTY_50'),
#                                          OHLCInterval.Day_1, datetime (2017, 8, 21).date (),
#                                          datetime (2018, 8, 20).date ())
#
# mean = util.get_daily_average_returns (util.get_daily_returns (future_current_month_latest_data))
#
# sd = util.get_daily_volatility (util.get_daily_returns (future_current_month_latest_data))
#
# print (mean)
# print (sd)
#
# print (util.get_range (11551.75, future_current_month_latest_data, 7))
#
# exit (0)
#
# for i in range (1, 3):
#     print (i)
#
# print (i)
#
# exit (0)
# print ([1, 3, 3.1, 3, 4, 2, 3, 4, 3, 2, 5][-2:])
# exit (0)
# upstox_api = util.intialize_upstox_api(['NSE_EQ'])
#
# today_date=datetime.today().date()
# start_date=datetime.now() - timedelta(days=1800)
# end_date=datetime.now() - timedelta(days=1)
#
# future_current_month_latest_data=util.get_stock_latest_data('SBIN',upstox_api,start_date,end_date,today_date,'NSE_EQ')
# stock_data_closing_prices_series = util.get_panda_series_of_stock_closing_prices(future_current_month_latest_data)
#
# # 3,5,8,10,12,13,18,21,30,34(200)],34high low close],35,40,45,50,60(300)],89 high, 89 low],100(450)], 150,200(1200,1800)],250
# ema_series = ind.ema(stock_data_closing_prices_series,200)
# print (ema_series)
# print (ema_series.iloc[1215])
# print(len(future_current_month_latest_data))
# # rsis= ind.new_rsi(stock_data_closing_prices_series, 14)
# # print(rsis)
# # print(rsis[0].iloc[-1])
# # print(rsis[1].iloc[-1])
# # rsi=ind
# # .rsi(stock_data_closing_prices_series, 14)
# # print(rsi.iloc[-1])
# exit(0)
#
# upstox_api = util.intialize_upstox_api(['NSE_EQ'])
#
# today_date=datetime.today().date()
# start_date=datetime.now() - timedelta(days=500)
# end_date=datetime.now() - timedelta(days=1)
#
# future_current_month_latest_data=util.get_stock_latest_data('HDFC',upstox_api,start_date,end_date,today_date,'NSE_EQ')
# stock_data_closing_prices = []
# stock_data_timestamps = []
# for stock in future_current_month_latest_data:
#     stock_data_closing_prices.append(stock['close'])
#     stock_data_timestamps.append(stock['timestamp'])
#
# plt.plot(stock_data_timestamps,stock_data_closing_prices)
#
# supports_resistances=sr.get_supports_resistances(future_current_month_latest_data)
#
# for supports_resistance in supports_resistances:
#     plt.plot ([stock_data_timestamps[0], stock_data_timestamps[-1]],
#               [supports_resistance['close'], supports_resistance['close']], color='k', linestyle='-', linewidth=1)
#
#
#
# plt.show()
# exit(0)
#
#
# print(print(future_current_month_latest_data[-1]))
# rsi_series=ind.RSI(pd.Series(future_current_month_latest_data),14)
# print(rsi_series.iloc[-1])
# exit(0)
#
#
# supports_resistances=sr.get_supports_resistances(ssd.bajaj_rsi_data)
# # print(supports_resistances)
# # exit(0)
#
# stock_data_closing_prices = []
# stock_data_timestamps = []
# for stock in ssd.bajaj_rsi_data:
#     stock_data_closing_prices.append(stock['close'])
#     stock_data_timestamps.append(stock['timestamp'])
#
# plt.plot(stock_data_timestamps,stock_data_closing_prices)
#
# for supports_resistance in supports_resistances:
#     plt.plot ([stock_data_timestamps[0], stock_data_timestamps[-1]],
#               [supports_resistance, supports_resistance], color='k', linestyle='-', linewidth=1)
#
#
#
# plt.show()
# exit(0)
#
# #ssd.maruti_540_days_stock_data.sort (key=lambda x: (-x['timestamp']))
# stock_data_closing_prices = []
# for stock in ssd.bajaj_rsi_data:
#     stock_data_closing_prices.append(stock['close'])
#
#
# #print([1,3,3.1,3,4,2,3,4,3,2,5][-2:])
# stock_data_closing_prices_series=pd.Series(stock_data_closing_prices)
# rsi_series=ind.rsi(stock_data_closing_prices_series,28)
# # print(rsi_series)
# #rsi_series=rsi_series.iloc[::-1]
# rsi_series=rsi_series.iloc[-5:].iloc[::-1]
# for rsi in rsi_series.iteritems():
#     print(rsi)
#
# exit(0)
#
#
# live_feed_date=datetime.fromtimestamp(1537595813)
# print(live_feed_date.date())
# today_date=datetime.today().date()
# print(today_date)
# print(live_feed_date.date()==datetime.today().date())
#
# print([1,3,3.1,3,4,2,3,4,3,2,5][-3:-1])
# exit(0)
# print(util.get_current_day_current_price({'timestamp': 1537497910000, 'open': 68, 'high': 69, 'low': 66.55, 'close': 67.25, 'volume': 46539, 'cp': 67.55}))
# exit(0)
# print(pr.find_nearest_resistance_support([12,11,10,9,8,7,6,5,4,3],9,True))
# print(pr.find_nearest_resistance_support([12,11,10,9,8,7,6,5,4,3],9,False))
# print(pr.find_nearest_resistance_support([12,11,10,9,8,7,6,5,4,3],3,True))
# print(pr.find_nearest_resistance_support([12,11,10,9,8,7,6,5,4,3],3,False))
# print(pr.find_nearest_resistance_support([12,11,10,9,8,7,6,5,4,3],12,True))
# print(pr.find_nearest_resistance_support([12,11,10,9,8,7,6,5,4,3],12,False))
#
#
# #print(ind.relative_strength(np.array(stock_data_closing_prices)))
# #print(ind.macd(stock_data_closing_prices_series))
# #print(ind.get_rsi(np.array(stock_data_closing_prices),28))
# #print(ind.calculate_rsi(ssd.bajaj_rsi_data[-28:]))
#
#
# #print(ind.ema(stock_data_closing_prices,10))
# exit(0)
# x = np.array([1,3,3.1,3,4,2,3,4,3,2,5])
# print([1,3,3.1,3,4,2,3,4,3,2,5][:-1])
# exit(0)
#
# # for local maxima
#
#
# def supres(ltp, n):
#     """
#     This function takes a numpy array of last traded price
#     and returns a list of support and resistance levels
#     respectively. n is the number of entries to be scanned.
#     """
#     from scipy.signal import savgol_filter as smooth
#
#     # converting n to a nearest even number
#     if n % 2 != 0:
#         n += 1
#
#     n_ltp = ltp.shape[0]
#     print ("n_ltp", n_ltp)
#     print ("n", n)
#     # smoothening the curve
#     ltp_s = smooth (ltp, (n + 1), 3)
#
#     print("ltp_s",ltp_s)
#     # taking a simple derivative
#     ltp_d = np.zeros (n_ltp)
#     #print ("ltp_d", ltp_d)
#     ltp_d[1:] = np.subtract (ltp_s[1:], ltp_s[:-1])
#     #print ("ltp_d", ltp_d)
#
#     resistance = []
#     support = []
#
#     print ("n_ltp", n_ltp - n)
#     for i in list(range (n_ltp - n)):
#         arr_sl = ltp_d[i:(i + n)]
#         first = arr_sl[:n//2]  # first half
#         last = arr_sl[n//2:]  # second half
#
#         r_1 = np.sum (first > 0)
#         r_2 = np.sum (last < 0)
#
#         s_1 = np.sum (first < 0)
#         s_2 = np.sum (last > 0)
#
#         #print ("s_1", n)
#         #print ("r_1", r_1)
#         #print ("s_1", s_1)
#         # local maxima detection
#         if (r_1 == (n / 2)) and (r_2 == (n / 2)):
#             resistance.append (ltp[i + ((n / 2) - 1)])
#
#         # local minima detection
#         if (s_1 == (n / 2)) and (s_2 == (n / 2)):
#             support.append (ltp[i + ((n / 2) - 1)])
#
#     return support, resistance
#
# def gentrends(x, window=1/3.0, charts=False):
#     """
#     Returns a Pandas dataframe with support and resistance lines.
#     :param x: One-dimensional data set
#     :param window: How long the trendlines should be. If window < 1, then it
#                    will be taken as a percentage of the size of the data
#     :param charts: Boolean value saying whether to print chart to screen
#     """
#
#     import numpy as np
#     import pandas_datareader.data as pd
#
#     x = np.array(x)
#
#     if window < 1:
#         window = int(window * len(x))
#
#     max1 = np.where(x == max(x))[0][0]  # find the index of the abs max
#     min1 = np.where(x == min(x))[0][0]  # find the index of the abs min
#
#     # First the max
#     if max1 + window > len(x):
#         max2 = max(x[0:(max1 - window)])
#     else:
#         max2 = max(x[(max1 + window):])
#
#     # Now the min
#     if min1 - window < 0:
#         min2 = min(x[(min1 + window):])
#     else:
#         min2 = min(x[0:(min1 - window)])
#
#     # Now find the indices of the secondary extrema
#     max2 = np.where(x == max2)[0][0]  # find the index of the 2nd max
#     min2 = np.where(x == min2)[0][0]  # find the index of the 2nd min
#
#     # Create & extend the lines
#     maxslope = (x[max1] - x[max2]) / (max1 - max2)  # slope between max points
#     minslope = (x[min1] - x[min2]) / (min1 - min2)  # slope between min points
#     a_max = x[max1] - (maxslope * max1)  # y-intercept for max trendline
#     a_min = x[min1] - (minslope * min1)  # y-intercept for min trendline
#     b_max = x[max1] + (maxslope * (len(x) - max1))  # extend to last data pt
#     b_min = x[min1] + (minslope * (len(x) - min1))  # extend to last data point
#     maxline = np.linspace(a_max, b_max, len(x))  # Y values between max's
#     minline = np.linspace(a_min, b_min, len(x))  # Y values between min's
#
#     # OUTPUT
#     trends = np.transpose(np.array((x, maxline, minline)))
#     trends = pd.DataReader(trends, index=np.arange (0, len (x)),
#                            columns=['Data', 'Max Line', 'Min Line'])
#
#
#     return trends
#
# stock_data_closing_prices = []
# for stock in ssd.maruti_540_days_stock_data:
#     stock_data_closing_prices.append(stock['close'])
#
# stock_data_closing_prices=stock_data_closing_prices[-10:]
# print("stock_data_closing_prices",stock_data_closing_prices)
# print(supres(np.array(stock_data_closing_prices),5))
# exit(0)
#
# stock_sample_data_from_upstox=[{'timestamp': 1535567400000, 'open': 68, 'high': 69, 'low': 66.55, 'close': 67.25, 'volume': 46539, 'cp': 67.55}, {'timestamp': 1535653800000, 'open': 68.5, 'high': 68.5, 'low': 66.8, 'close': 67.05, 'volume': 44865, 'cp': 67.15}, {'timestamp': 1535913000000, 'open': 68.5, 'high': 70.5, 'low': 67.5, 'close': 68, 'volume': 148413, 'cp': 0}]
#
# #print(np.array(stock_sample_data_from_upstox)[-2,-1])
#
#
# print(len(stock_data_closing_prices))
# print(supres(np.array(stock_data_closing_prices),10))
# exit(0)
#
# print(math.isclose(658,664.5,rel_tol=.01))
# print(pr.calculate_last_10_days_average_volume(ssd.trend_check[-11:-1]))
# print(pr.Recognize_Morning_Star_pattern(ssd.trend_check,1,1,1))
# exit(0)
#
#
# print(ind.sma(stock_data_closing_prices,9))
# print(ind.ema(stock_data_closing_prices,50))
# exit(0)
# time_difference_in_secs_for_current_price=600
#
#
#
# def nearly_equal(var1, var2, variation_percent):
#     return math.isclose (var1, var2, rel_tol=variation_percent / 100)
#
# if ((nearly_equal (366.7, 364.8, 1) and (
#         (377.45 - 375.6) > (
#         2 * (375.6 - 	366.70)))) or (
#             nearly_equal (	366.70, 375.6, 1) and (
#             (377.45 - 	366.70) > (
#             2 * (366.7 - 375.6))))) == False:
#     print("Hello")
# print((nearly_equal (366.7, 364.8, 1) and (
#         (377.45 - 375.6) > (
#         2 * (375.6 - 	366.70)))))
# exit(0)
#
#
# print(get_open_high_low_close(ssd.maruti_540_days_stock_data))
# print([{'timestamp': 1536258600000, 'open': 237, 'high': 246.25, 'low': 234.25, 'close': 242.55, 'volume': 11021703, 'cp': 243.45}])
# exit(0)
#
# print(time.time())
# print(1490553000000/1000)
# stock_data={'timestamp': 1436474784000, 'open': 5977, 'high': 6020, 'low': 5965.65, 'close': 5985.8, 'volume': 340512, 'cp': 5985}
# print(get_current_day_current_price(stock_data))
#
#
# maruti_540_days_stock_data_closing_prices=[]
# for stock_data in ssd.maruti_540_days_stock_data:
#     maruti_540_days_stock_data_closing_prices.append(stock_data['close'])
#
# #maruti_540_days_stock_data_closing_prices=np.array(maruti_540_days_stock_data_closing_prices)
# #print(tech_ana.rsi(stock_closing_price,28))
#
#
# print (PPSR(ssd.maruti_540_days_stock_data))
#
# long_EMA_days=100
# short_EMA_days=50
# def Check_MA_Crossover_Bullish():
#  print(short_EMA_days+long_EMA_days)
#
# test_array=[1,2,3,4,5,6]
# stock_test_data=[
#      {
#       "timestamp": 1485973800000,
#       "open": 1050,
#       "high": 1050.8,
#       "low": 1038.25,
#       "close": 1043.5,
#       "volume": 2079910,
#       "cp": 1045.55
#     },
#     {
#       "timestamp": 1485973800000,
#       "open": 1050,
#       "high": 1050.8,
#       "low": 1038.25,
#       "close": 1043.5,
#       "volume": 2079908,
#       "cp": 1045.55
#     }
# ]
#
#
# #print((abs(107-114)/(107-102))>1.3)
#
# #Check_MA_Crossover_Bullish()
#
# today=datetime.today().date()
# yesterday=datetime.now() - timedelta(days=3)
#
# #print(today)
# #print(yesterday.date())
#
# #print (sum(stock['volume'] for stock in stock_test_data) / len(stock_test_data))
# #print(test_array[-3])
# #print([10, 9][9>10])
#
#
