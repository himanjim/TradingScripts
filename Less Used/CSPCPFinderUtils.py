from datetime import datetime

import DerivativeUtils as outil
import ScrapUtils as nse_bse
import Utils as util
from dateutil.relativedelta import relativedelta

minimum_cs_earning = 6500
minimum_pcp_earning = 20000
max_timeout_for_trade = 180
cs_earning_sacrifical_ratio = .2
pcr_earning_sacrifical_ratio = .2
max_std_multiple_for_buy = 2
max_std_multiple_for_sell = 1
future_liquidity_margin = .1
min_no_of_bids_asks = 5

today_date = datetime.today().date()

current_month_last_thurs = outil.get_last_thurday_of_month(today_date.month, today_date.year)

near_month_last_thurs = current_month_last_thurs + relativedelta(months=+1)


def fetch_futures(futures, current_month_last_thurs, near_month_last_thurs, fetched_futures, calcul_limit, option_lots):
    future_status = {}
    for future in futures:
        stock_id = outil.get_stock_id (future[nse_bse.STOCK_ID])
        future_status[stock_id] = {'curr_mon_enable': True, 'near_mon_enable': True}

        future_cur_mon_symbol = outil.get_future_symbol (stock_id, current_month_last_thurs)
        future_near_mon_symbol = outil.get_future_symbol (stock_id, near_month_last_thurs)

        fetched_futures[future_cur_mon_symbol] = future_near_mon_symbol

        fetched_futures[future_near_mon_symbol] = {'ltp': None, 'liquid': False, 'c_ltp': None, 'c_liquid': False,
                                                   'c_symbol': future_cur_mon_symbol,
                                                   'sd_buy': calcul_limit.calculated_calendar_spread_limits[stock_id][
                                                       0],
                                                   'sd_sell': calcul_limit.calculated_calendar_spread_limits[stock_id][
                                                       1], 'lot': option_lots[stock_id], 'stock': stock_id}

        fetched_futures['future_status'] = future_status
        fetched_futures['futures'] = futures


def try_long_pcr(upstox_api, atm_option_call, atm_option_put, future, lot, max_earning):
    net_premium = atm_option_put.ltp - atm_option_call.ltp + future.ltp - atm_option_call.strike_price
    earning = net_premium * lot

    if earning > max_earning[0]['earning']:
        max_earning[0]['earning'] = earning
        max_earning[0]['symbol_ltps'] = [atm_option_call, atm_option_put, future]

    if earning > minimum_pcp_earning:
        get_reduced_long_pcr_ltps (earning, atm_option_put, future, atm_option_call, lot)

        outil.sell_instrument(upstox_api, atm_option_put.symbol, nse_bse.NSE_FO, atm_option_put.ltp,
                              lot)
        outil.sell_instrument(upstox_api, future.symbol, nse_bse.NSE_FO, future.ltp, lot)
        outil.buy_instrument(upstox_api, atm_option_call.symbol, nse_bse.NSE_FO, atm_option_call.ltp,
                             lot)
        print ('Executed LONG PCR(CALL):', atm_option_call.symbol, atm_option_call.ltp)
        print ('Executed LONG PCR(PUT):', atm_option_put.symbol, atm_option_put.ltp)
        print ('Executed LONG PCR FUTURE:', future.symbol, future.ltp)
        print('Earning:', earning)
        # exit (0)
    else:
        print('Low earning:', earning)


def try_short_pcr(upstox_api, atm_option_call, atm_option_put, future, lot, max_earning):
    net_premium = atm_option_call.ltp - atm_option_put.ltp + atm_option_call.strike_price - future.ltp
    earning = net_premium * lot

    if earning > max_earning[0]['earning']:
        max_earning[0]['earning'] = earning
        max_earning[0]['symbol_ltps'] = [atm_option_call, atm_option_put, future]

    if earning > minimum_pcp_earning:
        get_reduced_short_pcr_ltps (earning, atm_option_put, future, atm_option_call, lot)

        outil.buy_instrument (upstox_api, atm_option_put.symbol, nse_bse.NSE_FO, atm_option_put.ltp,
                              lot)
        outil.buy_instrument (upstox_api, future.symbol, nse_bse.NSE_FO, future.ltp, lot)
        outil.sell_instrument (upstox_api, atm_option_call.symbol, nse_bse.NSE_FO, atm_option_call.ltp,
                               lot)
        print ('Executed SHORT PCR(CALL):', atm_option_call.symbol, atm_option_call.ltp)
        print ('Executed SHORT PCR(PUT):', atm_option_put.symbol, atm_option_put.ltp)
        print ('Executed SHORT PCR FUTURE:', future.symbol, future.ltp)
        print ('Earning:', earning)
        # exit (0)
    else:
        print ('Low earning:', earning)


def get_reduced_cs_ltps(earning, near_future, lot):
    print ('CS old ltps:', near_future['ltp'], near_future['c_ltp'])
    reduced_earning = (1 - cs_earning_sacrifical_ratio) * earning
    while ((near_future['ltp'] - near_future['c_ltp']) * lot) > reduced_earning:
        near_future['ltp'] -= util.min_tick_size
        near_future['c_ltp'] += util.min_tick_size

    near_future['ltp'] = round(near_future['ltp'], 2)
    near_future['c_ltp'] = round(near_future['c_ltp'], 2)
    print ('CS new ltps:', near_future['ltp'], near_future['c_ltp'])


def get_reduced_long_pcr_ltps(earning, atm_option_put, future, atm_option_call, lot):
    print('PCP old ltps:', atm_option_put.ltp, atm_option_call.ltp, future.ltp)
    reduced_earning = (1 - pcr_earning_sacrifical_ratio) * earning

    while ((
                   atm_option_put.ltp - atm_option_call.ltp + future.ltp - atm_option_call.strike_price) * lot) > reduced_earning:
        atm_option_put.ltp -= util.min_tick_size
        future.ltp -= util.min_tick_size
        atm_option_call.ltp += util.min_tick_size

    atm_option_call.ltp = round(atm_option_call.ltp, 2)
    atm_option_put.ltp = round(atm_option_put.ltp, 2)
    future.ltp = round(future.ltp, 2)
    print('PCP new ltps:', atm_option_put.ltp, atm_option_call.ltp, future.ltp)


def get_reduced_short_pcr_ltps(earning, atm_option_put, future, atm_option_call, lot):
    print ('PCP old ltps:', atm_option_put.ltp, atm_option_call.ltp, future.ltp)
    reduced_earning = (1 - pcr_earning_sacrifical_ratio) * earning

    while ((
                   atm_option_call.ltp - atm_option_put.ltp + atm_option_call.strike_price - future.ltp) * lot) > reduced_earning:
        atm_option_put.ltp += util.min_tick_size
        future.ltp += util.min_tick_size
        atm_option_call.ltp -= util.min_tick_size

    atm_option_call.ltp = round (atm_option_call.ltp, 2)
    atm_option_put.ltp = round (atm_option_put.ltp, 2)
    future.ltp = round (future.ltp, 2)
    print ('PCP new ltps:', atm_option_put.ltp, atm_option_call.ltp, future.ltp)


def wait_and_buy_current_future(upstox_api, near_future, lots):
    if util.check_if_instrument_in_positions(upstox_api, near_future['symbol'], max_timeout_for_trade):
        print('Sold', near_future['symbol'])
        outil.buy_instrument(upstox_api, near_future['c_symbol'], nse_bse.NSE_FO, near_future['c_ltp'], lots)
        print('Executed future 0:', near_future['c_symbol'], near_future['c_ltp'])
        if util.check_if_instrument_in_positions(upstox_api, near_future['c_symbol'], max_timeout_for_trade):
            print('Bought', near_future['c_symbol'])
        else:
            util.cancel_order(upstox_api, near_future['c_symbol'])
            print('Time out for buying future 0', near_future['c_symbol'])
            outil.buy_instrument(upstox_api, near_future['symbol'], nse_bse.NSE_FO, near_future['ltp'] - 1, lots)

        exit(0)
    else:
        util.cancel_order(upstox_api, near_future['symbol'])
        print('Time out for buying future 1', near_future['symbol'])


def get_atm_call_puts(stock_id, last_thurs):
    options = outil.get_all_strikes(stock_id, last_thurs.month, last_thurs.year)

    atm_strike = outil.get_atm_strike(options)

    atm_option_call = None

    atm_option_put = None

    for option in options:
        if option.strike_price == atm_strike:
            if option.is_call:
                atm_option_call = option
            else:
                atm_option_put = option

    return atm_option_call, atm_option_put


def check_pcr_future(future):
    if future is None:
        print('Null futures', future.symbol)
        return False

    if outil.is_instrument_liquid (future.bids, future.asks) is False:
        print('Illiquid futures', future.symbol)
        return False

    if future.volume is None or future.volume <= 0 or future.high is None or future.high <= 0 or future.low is None or future.low <= 0 or future.open is None or future.open <= 0:
        print('Illiquid futures', future.symbol)
        return False

    return True


def check_pcr_option(option):
    if option is None:
        print ('Null option_pe', option.symbol)
        return False

    if option.liquidity is False or option.volume is None or option.volume <= 0:
        print ('Illiquid option_pe', option.symbol)
        return False

    return True


def pcr_null_illiquid_check(atm_option_call, atm_option_put, futures, month_str):
    if atm_option_call is None:
        print('Null atm_option_call', month_str)
        return False

    if atm_option_call.liquidity is False or atm_option_call.volume is None or atm_option_call.volume <= 0:
        print('Illiquid atm_option_call', month_str)
        return False

    if atm_option_put is None:
        print('Null atm_option_put', month_str)
        return False

    if atm_option_put.liquidity is False or atm_option_put.volume is None or atm_option_put.volume <= 0:
        print('Illiquid atm_option_put', month_str)
        return False

    if futures is None:
        print('Null futures', month_str)
        return False

    if outil.is_instrument_liquid(futures.bids, futures.asks) is False:
        print('Illiquid futures', month_str)
        return False

    return True
