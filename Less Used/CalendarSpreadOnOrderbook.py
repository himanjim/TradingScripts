import pickle
import traceback

import CalculatedCalendarSpreadsLimits as calcul_limit
import CalendarSpreadUtils as clds_utils
from CSPCPFinderUtils import *
from upstox_api.api import LiveFeedType

buffer_for_cs = 5

upstox_api = util.intialize_upstox_api([nse_bse.NSE_FO])
# upstox_api = None

# futures = nse_bse.get_all_nse_stocks_ids ()
futures = nse_bse.get_nse_fo_stocks()
# futures = nse_bse.get_indices ()
# futures = nse_bse.get_nifty50_stocks_latest_info ()
# futures = nse_bse.get_nifty100_stocks_latest_info ()
# futures = [{nse_bse.STOCK_ID: 'IGL', nse_bse.EXCHANGE: nse_bse.NSE_FO}]


option_lots = nse_bse.get_nse_fo_lots()

futures[:] = [x for x in futures if
              outil.get_stock_id(x[nse_bse.STOCK_ID]) in calcul_limit.calculated_calendar_spread_limits]

fetched_futures = {}

fetch_futures(futures, current_month_last_thurs, near_month_last_thurs, fetched_futures, calcul_limit, option_lots)

no_of_subscribed_symbols = util.get_no_of_subscribed_symbols(upstox_api)
print('No. of subscribed symbols:', no_of_subscribed_symbols)
stock_live_data = pickle.load(open(util.get_instrument_latest_data_file_name(), 'rb'))
while len(stock_live_data) < no_of_subscribed_symbols:
    stock_live_data = pickle.load(open(util.get_instrument_latest_data_file_name(), 'rb'))

while True:
    try:
        stock_live_data = pickle.load(open(util.get_instrument_latest_data_file_name(), 'rb'))
        for symbol, future in stock_live_data.items():

            if 'ltp' not in fetched_futures[symbol]:

                if future['ltp'] is not None:

                    near_fut = fetched_futures[fetched_futures[symbol]]

                    near_fut['c_ltp'] = future['ltp']

                    # near_fut['c_ltp'] = min(x['price'] for x in future['asks'])

                    near_fut['c_liquid'] = outil.is_instrument_liquid(future['bids'], future['asks'],
                                                                      clds_utils.future_liquidity_margin,
                                                                      clds_utils.min_no_of_bids_asks) and (
                                                       future['open'] > 0 and future['low'] > 0 and future[
                                                   'high'] > 0 and future['vtt'] > 0)


                else:
                    print('No ltp in future 0:', symbol)
                    upstox_api.unsubscribe(
                        upstox_api.get_instrument_by_symbol(future[nse_bse.EXCHANGE], symbol),
                        LiveFeedType.Full)
                    fetched_futures['future_status'][fetched_futures[symbol]['stock']]['curr_mon_enable'] = False

            elif fetched_futures[symbol]['c_ltp'] is not None:
                near_fut = fetched_futures[symbol]

                near_fut['ltp'] = future['ltp']
                lot = near_fut['lot']

                near_fut['liquid'] = outil.is_instrument_liquid(future['bids'], future['asks'],
                                                                clds_utils.future_liquidity_margin,
                                                                clds_utils.min_no_of_bids_asks) and (
                                                 future['open'] > 0 and future['low'] > 0 and future['high'] > 0 and
                                                 future['vtt'] > 0)

                if near_fut['liquid']:
                    if near_fut['c_liquid']:

                        # future['ltp'] = max(x['price'] for x in future['bids'])

                        spread = future['ltp'] - near_fut['c_ltp']
                        sd_buy_limit = near_fut['sd_buy']

                        if spread > sd_buy_limit:

                            sd_sell_limit = near_fut['sd_sell']

                            earning = spread * lot

                            print(symbol, future, near_fut, earning)
                            continue

                            if earning > minimum_cs_earning:

                                get_reduced_cs_ltps(earning, near_fut, lot)

                                print(outil.sell_instrument(upstox_api, symbol, nse_bse.NSE_FO, future['ltp'], lot))
                                print('Executed future 1:', symbol, future['ltp'], earning)
                                execute = input("Execute future 0?")
                                if int(execute) == 1:
                                    print(outil.buy_instrument(upstox_api, near_fut['c_symbol'], nse_bse.NSE_FO,
                                                               near_fut['c_ltp'], lot))
                                    print('Executed future 0:', near_fut['c_symbol'], near_fut['c_ltp'])

                                    print('Sell at:', sd_sell_limit, lot)
                                    exit(0)
                                # wait_and_buy_current_future(upstox_api, near_fut, lot)

                            else:
                                print('Low return', earning, symbol)
                    else:
                        pass
                        # print('Illiquid future 0:', near_fut['c_symbol'])
                        # upstox_api.unsubscribe(
                        #     upstox_api.get_instrument_by_symbol(nse_bse.NSE_FO, near_fut['c_symbol']),
                        #     LiveFeedType.Full)
                        # print('Unsubscribed future 0:', near_fut['c_symbol'])

                else:
                    pass
                    # print('Illiquid future 1:', symbol)
                    # upstox_api.unsubscribe(upstox_api.get_instrument_by_symbol(nse_bse.NSE_FO, symbol),
                    #                        LiveFeedType.Full)
                    # print('Unsubscribed future 1:', symbol)
            else:
                print('No current ltp in future 1:', symbol)

    except Exception:
        print(traceback.format_exc() + ' in Stock:' + str(future))


upstox_api.set_on_quote_update(event_handler_quote_update)
upstox_api.start_websocket(False)
