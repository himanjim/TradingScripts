import enum
import math


class Strategy(enum.Enum):
    BULL_CALL_SPREAD = 1
    BULL_PUT_SPREAD = 2
    BEAR_PUT_SPREAD = 3
    BEAR_CALL_SPREAD = 4


def get_bull_call_spreads(options):
    itm_options = [x for x in options if x.strike_price < x.spot_price and x.is_call and x.liquidity]
    otm_options = [x for x in options if x.strike_price > x.spot_price and x.is_call and x.liquidity]

    if len (itm_options) < 0:
        print ('No ITM options')
        return None, None, None, None, None

    if len (otm_options) < 0:
        print ('No ITM options')
        return None, None, None, None, None

    itm_option_to_trade = None
    otm_option_to_trade = None
    min_debit = math.inf
    for itm_option in itm_options:
        for otm_option in otm_options:
            itm_option.ltp = min (x['price'] for x in itm_option.asks)
            otm_option.ltp = max (x['price'] for x in otm_option.bids)
            net_debit = itm_option.ltp - otm_option.ltp
            if net_debit < min_debit:
                itm_option_to_trade = itm_option
                otm_option_to_trade = otm_option
                min_debit = net_debit

    net_debit = itm_option_to_trade.ltp - otm_option_to_trade.ltp

    print ('Bull Call Spread: Buy ITM:%d, Sell OTM:%d, max loss(debit):%s, max profit:%s. Need low volatility' % (
    itm_option_to_trade.strike_price, otm_option_to_trade.strike_price, net_debit,
    (otm_option_to_trade.strike_price - itm_option_to_trade.strike_price - net_debit)))

    return itm_option_to_trade, otm_option_to_trade, Strategy.BULL_CALL_SPREAD, -math.inf, net_debit


def get_bull_put_spreads(options):
    itm_options = [x for x in options if x.strike_price > x.spot_price and x.is_call is False and x.liquidity]
    otm_options = [x for x in options if x.strike_price < x.spot_price and x.is_call is False and x.liquidity]

    if len (itm_options) < 0:
        print ('No ITM options')
        return None, None, None, None, None

    if len (otm_options) < 0:
        print ('No ITM options')
        return None, None, None, None, None

    itm_option_to_trade = None
    otm_option_to_trade = None
    max_credit = -math.inf
    for itm_option in itm_options:
        for otm_option in otm_options:
            otm_option.ltp = min (x['price'] for x in otm_option.asks)
            itm_option.ltp = max (x['price'] for x in itm_option.bids)
            net_credit = itm_option.ltp - otm_option.ltp
            if net_credit > max_credit:
                itm_option_to_trade = itm_option
                otm_option_to_trade = otm_option
                max_credit = net_credit

    net_credit = itm_option_to_trade.ltp - otm_option_to_trade.ltp

    print ('Bull Put Spread: Buy OTM:%d, Sell ITM:%d, max loss:%s, max profit(credit):%s. Need High volatility' % (
    otm_option_to_trade.strike_price, itm_option_to_trade.strike_price,
    (otm_option_to_trade.strike_price - itm_option_to_trade.strike_price - net_credit), net_credit))

    return itm_option_to_trade, otm_option_to_trade, Strategy.BULL_PUT_SPREAD, net_credit, math.inf


def get_bear_put_spreads(options):
    itm_options = [x for x in options if x.strike_price > x.spot_price and x.is_call is False and x.liquidity]
    otm_options = [x for x in options if x.strike_price < x.spot_price and x.is_call is False and x.liquidity]

    if len (itm_options) < 0:
        print ('No ITM options')
        return None, None, None, None, None

    if len (otm_options) < 0:
        print ('No ITM options')
        return None, None, None, None, None

    itm_option_to_trade = None
    otm_option_to_trade = None
    min_debit = math.inf
    for itm_option in itm_options:
        for otm_option in otm_options:
            itm_option.ltp = min (x['price'] for x in itm_option.asks)
            otm_option.ltp = max (x['price'] for x in otm_option.bids)
            net_debit = itm_option.ltp - otm_option.ltp
            if net_debit < min_debit:
                itm_option_to_trade = itm_option
                otm_option_to_trade = otm_option
                min_debit = net_debit

    net_debit = itm_option_to_trade.ltp - otm_option_to_trade.ltp

    print ('Bear Put Spread: Sell OTM:%d, buy ITM:%d, max loss(debit):%s, max profit:%s. Need Low volatility' % (
    otm_option_to_trade.strike_price, itm_option_to_trade.strike_price, net_debit,
    (itm_option_to_trade.strike_price - otm_option_to_trade.strike_price - net_debit)))

    return itm_option_to_trade, otm_option_to_trade, Strategy.BEAR_PUT_SPREAD, -math.inf, net_debit


def get_bear_call_spreads(options):
    itm_options = [x for x in options if x.strike_price < x.spot_price and x.is_call and x.liquidity]
    otm_options = [x for x in options if x.strike_price > x.spot_price and x.is_call and x.liquidity]

    if len (itm_options) < 0:
        print ('No ITM options')
        return None, None, None, None, None

    if len (otm_options) < 0:
        print ('No ITM options')
        return None, None, None, None, None

    itm_option_to_trade = None
    otm_option_to_trade = None
    max_credit = -math.inf
    for itm_option in itm_options:
        for otm_option in otm_options:
            otm_option.ltp = min (x['price'] for x in otm_option.asks)
            itm_option.ltp = max (x['price'] for x in itm_option.bids)
            net_credit = itm_option.ltp - otm_option.ltp
            if net_credit > max_credit:
                itm_option_to_trade = itm_option
                otm_option_to_trade = otm_option
                max_credit = net_credit

    net_credit = itm_option_to_trade.ltp - otm_option_to_trade.ltp

    print ('Bear Call Spread: Sell ITM:%d, buy OTM:%d, max loss:%s, max profit(credit):%s. Need High volatility' % (
    itm_option_to_trade.strike_price, otm_option_to_trade.strike_price,
    (otm_option_to_trade.strike_price - itm_option_to_trade.strike_price - net_credit), net_credit))

    return itm_option_to_trade, otm_option_to_trade, Strategy.BEAR_CALL_SPREAD, net_credit, math.inf
