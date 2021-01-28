import PatternRecognition as pr
import ScrapUtils as nsebse
import Utils as util
from upstox_api.api import *


def event_handler_quote_update(quote):
    for order in orders_to_monitor:
        if quote['symbol'] == order['symbol']:
            if order['action'].value == pr.Action.LONG.value and quote['spot_price'] >= order['target']:
                upstox_api.place_order(TransactionType.Sell,
                                        upstox_api.get_instrument_by_symbol (order[nsebse.EXCHANGE], order['symbol']),
                                        order['quantity'], OrderType.Limit, ProductType.Delivery, order['target'], None, None,
                                        DurationType.DAY, None, None)

            elif order['action'].value == pr.Action.SHORT.value and quote['spot_price'] <= order['target']:
                upstox_api.place_order(TransactionType.Sell,
                                        upstox_api.get_instrument_by_symbol (order[nsebse.EXCHANGE], order['symbol']),
                                        order['quantity'], OrderType.Limit, ProductType.Delivery, (.8 * order['target']), None, None,
                                        DurationType.DAY, None, None)

            elif (order['action'].value == pr.Action.LONG.value and quote['spot_price'] <= order['stop_loss']) or (
                    order['action'].value == pr.Action.SHORT.value and quote['spot_price'] >= order['stop_loss']):
                upstox_api.place_order(TransactionType.Sell,
                                        upstox_api.get_instrument_by_symbol (order[nsebse.EXCHANGE], order['symbol']),
                                        order['quantity'], OrderType.Market, ProductType.Delivery, None, None, None,
                                        DurationType.DAY, None, None)


orders_to_monitor = [
    {'symbol': 'NIFTY19JANFUT', nsebse.EXCHANGE: nsebse.NSE, 'buy_price': 0, 'quantity': 0, 'stop_loss': 0, 'target': 0,
     'quantity': 0, 'action': pr.Action.SHORT},
    {'symbol': 'NIFTY19MARFUT', nsebse.EXCHANGE: nsebse.NSE, 'buy_price': 0, 'quantity': 0, 'stop_loss': 0, 'target': 0,
     'quantity': 0, 'action': pr.Action.LONG}]

exchanges = {}

for order in orders_to_monitor:
    exchanges.update({order[nsebse.EXCHANGE]})

upstox_api = util.intialize_upstox_api (list(exchanges))

upstox_api.set_on_order_update(event_handler_quote_update)

for order in orders_to_monitor:
    upstox_api.subscribe (upstox_api.get_instrument_by_symbol (order[nsebse.EXCHANGE], order['symbol']),
                          LiveFeedType.Full)

# u.place_order(TransactionType.Sell, u.get_instrument_by_symbol('NSE_FO', 'BANKNIFTY17JUN15FUT'), 40, OrderType.Limit, ProductType.OneCancelsOther, 23001.0, None, None, DurationType.DAY, 10.0, 10.0)
# https://upstox.com/forum/topic/261/oco-orders-with-stop-loss-and-target
# (u.place_order(TransactionType.Sell,  # transaction_type
# u.get_instrument_by_symbol('nse_fo', 'instrument'),  # instrument
# 75,  # quantity
# OrderType.Market,  # order_type
# ProductType.Delivery,  # product_type
# None,  # price
# None,  # trigger_price
# 0,  # disclosed_quantity
# DurationType.DAY,  # duration
# None,  # stop_loss
# None,  # square_off
# None  )# trailing_ticks
# )
