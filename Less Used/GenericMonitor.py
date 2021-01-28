import DerivativeUtils as dutil
import ScrapUtils as nsebse
import Utils as util
from PatternRecognition import Action
from upstox_api.api import *

orders_to_monitor = [
    {'symbol_to_track': 'KOTAKBANK', 'symbol_to_track_exch': nsebse.NSE, 'symbol_to_track_target': 1265,
     'symbol_to_track_stoploss': 1300, 'symbol_to_track_action': Action.SHORT, 'symbol_to_act': 'KOTAKBANK19JANFUT',
     'symbol_to_act_exch': nsebse.NSE_FO, 'lots': 800}]

exchanges = set()
for order in orders_to_monitor:
    exchanges.add(order['symbol_to_track_exch'])
    exchanges.add(order['symbol_to_act_exch'])

upstox_api = util.intialize_upstox_api(list(exchanges))


def event_handler_quote_update(quote):
    for order in orders_to_monitor:
        if quote['symbol'] == order['symbol_to_track']:
            if order['symbol_to_track_action'].value == Action.LONG.value and (
                    quote['ltp'] <= order['symbol_to_track_stoploss'] or quote['ltp'] >= order[
                'symbol_to_track_target']):
                dutil.sell_instrument(upstox_api, order['symbol_to_act'], order['symbol_to_act_exch'], None,
                                      order['lots'],
                                      OrderType.Market)
                exit(0)

            if order['symbol_to_track_action'].value == Action.SHORT.value and (
                    quote['ltp'] >= order['symbol_to_track_stoploss'] or quote['ltp'] <= order[
                'symbol_to_track_target']):
                dutil.buy_instrument(upstox_api, order['symbol_to_act'], order['symbol_to_act_exch'], None,
                                     order['lots'],
                                     OrderType.Market)
                exit(0)

    print(quote)


def event_handler_socket_disconnect(err):
    print("Socket Disconnected", err)
    upstox_api.start_websocket(False)


for order in orders_to_monitor:
    upstox_api.subscribe(upstox_api.get_instrument_by_symbol(order['symbol_to_track_exch'], order['symbol_to_track']),
                         LiveFeedType.LTP)

upstox_api.set_on_quote_update(event_handler_quote_update)

upstox_api.start_websocket(False)
