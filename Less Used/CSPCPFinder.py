import traceback

import time
from upstox_api.api import LiveFeedType

import CalculatedCalendarSpreadsLimits as calcul_limit
import CalendarSpreadUtils as clds_utils
from CSPCPFinderUtils import *

buffer_for_cs = 5

upstox_api = util.intialize_upstox_api([nse_bse.NSE_FO])
# upstox_api = None

# futures = nse_bse.get_all_nse_stocks_ids ()
futures = nse_bse.get_nse_fo_stocks()
# futures = nse_bse.get_indices ()
# futures = nse_bse.get_nifty50_stocks_latest_info ()
# futures = nse_bse.get_nifty100_stocks_latest_info ()
futures = [{nse_bse.STOCK_ID: 'HDFC', nse_bse.EXCHANGE: nse_bse.NSE_FO}]

no_of_days_till_last_thurs = current_month_last_thurs.day - today_date.day + 1

cs_enable = True
if no_of_days_till_last_thurs < buffer_for_cs:
    cs_enable = False

option_lots = nse_bse.get_nse_fo_lots()

required_margins = outil.get_future_margins(futures)

if util.is_market_open():
    available_margin = upstox_api.get_balance()['equity']['available_margin']
    futures[:] = [x for x in futures if
                  required_margins[outil.get_stock_id(x[nse_bse.STOCK_ID])] <= (.95 * available_margin)]

futures[:] = [x for x in futures if
              outil.get_stock_id(x[nse_bse.STOCK_ID]) in calcul_limit.calculated_calendar_spread_limits]

fetched_futures = {}
symbols_to_subscribe = []

fetch_futures(upstox_api, futures, current_month_last_thurs, near_month_last_thurs,
              symbols_to_subscribe, fetched_futures, calcul_limit, option_lots)

if len (symbols_to_subscribe) > 0:
    prev_subs = upstox_api.get_subscriptions()['FULL']
    for prev_sub in prev_subs:
        upstox_api.unsubscribe(upstox_api.get_instrument_by_symbol(prev_sub['exchange'], prev_sub['symbol']),
                               LiveFeedType.Full)
        time.sleep(.2)

    upstox_api.subscribe (symbols_to_subscribe, LiveFeedType.Full, nse_bse.NSE_FO)
else:
    print ('No symbols to subscribe.')
    exit (0)


def event_handler_quote_update(future):
    try:
        print('Helll')

        pickle.loads(open(FETCHED_FUTURES_FILE_OBJ_LOC, 'rb'))

        print('Helll')

        if datetime.now().minute % 3 == 0:
            pickle.dump(fetched_futures, open(FETCHED_FUTURES_FILE_OBJ_LOC, 'wb'))

        symbol = future['symbol']

        if 'ltp' not in fetched_futures[symbol]:

            if future['ltp'] is not None:

                near_fut = fetched_futures[fetched_futures[symbol]]

                lot = near_fut['lot']

                near_fut['c_ltp'] = future['ltp']

                # fetched_futures[fetched_futures[symbol]]['c_ltp'] = min(x['price'] for x in future['asks'])

                near_fut['c_liquid'] = outil.is_instrument_liquid (future['bids'], future['asks'],
                                                                   clds_utils.future_liquidity_margin,
                                                                   clds_utils.min_no_of_bids_asks)

                if fetched_futures['future_status'][near_fut['stock']][
                    'curr_mon_enable'] and 'atm_option_call' in near_fut:
                    try_long_pcr (upstox_api, near_fut['atm_option_call'], near_fut['atm_option_put'], future,
                                  near_fut['c_liquid'], lot,
                                  fetched_futures['future_status'][near_fut['stock']]['curr_mon_enable'])

            else:
                print ('No ltp in future 0:', symbol)
                upstox_api.unsubscribe (
                    upstox_api.get_instrument_by_symbol (future[nse_bse.EXCHANGE], symbol),
                    LiveFeedType.Full)
                fetched_futures['future_status'][fetched_futures[symbol]['stock']]['curr_mon_enable'] = False

        elif fetched_futures[symbol]['c_ltp'] is not None:
            near_fut = fetched_futures[symbol]

            near_fut['ltp'] = future['ltp']
            lot = near_fut['lot']

            near_fut['liquid'] = outil.is_instrument_liquid (future['bids'], future['asks'],
                                                                           clds_utils.future_liquidity_margin,
                                                                           clds_utils.min_no_of_bids_asks)

            if fetched_futures['future_status'][near_fut['stock']]['near_mon_enable'] and 'atm_option_call' in near_fut:
                try_long_pcr (upstox_api, near_fut['atm_option_call_near'], near_fut['atm_option_put_near'], future,
                              near_fut['liquid'], lot,
                              fetched_futures['future_status'][near_fut['stock']]['near_mon_enable'])

            if near_fut['liquid']:
                if near_fut['c_liquid']:
                    if cs_enable:

                        # future['ltp'] = max(x['price'] for x in future['bids'])

                        spread = future['ltp'] - near_fut['c_ltp']
                        sd_buy_limit = near_fut['sd_buy']

                        if spread > sd_buy_limit:

                            sd_sell_limit = near_fut['sd_sell']

                            earning = spread * lot

                            if earning > minimum_cs_earning:
                                get_reduced_cs_ltps (earning, near_fut, lot)

                                outil.sell_instrument (upstox_api, symbol, nse_bse.NSE_FO, future['ltp'], lot)

                                print ('Executed future 1:', symbol, future['ltp'])
                                print ('Sell at:', sd_sell_limit, lot)
                                wait_and_buy_current_future (upstox_api, near_fut, lot)
                            else:
                                print ('Low return', earning, symbol)
                    else:
                        print('CS disabled')
                else:
                    print ('Illiquid future 0:', near_fut['c_symbol'])
                    upstox_api.unsubscribe (
                        upstox_api.get_instrument_by_symbol (future[nse_bse.EXCHANGE], near_fut['c_symbol']),
                        LiveFeedType.Full)
                    fetched_futures['future_status'][near_fut['stock']]['curr_mon_enable'] = False

            else:
                print('Illiquid future 1:', symbol)
                upstox_api.unsubscribe(upstox_api.get_instrument_by_symbol(future[nse_bse.EXCHANGE], symbol),
                                       LiveFeedType.Full)
                fetched_futures['future_status'][near_fut['stock']]['near_mon_enable'] = False
        else:
            print ('No ltp in future 1:', symbol)
            upstox_api.unsubscribe (upstox_api.get_instrument_by_symbol (future[nse_bse.EXCHANGE], symbol),
                                    LiveFeedType.Full)
            fetched_futures['future_status'][fetched_futures[symbol]]['near_mon_enable'] = False


    except Exception as e:
        print(traceback.format_exc() + ' in Stock:' + future)


upstox_api.set_on_quote_update(event_handler_quote_update)
upstox_api.start_websocket(False)
