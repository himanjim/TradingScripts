import collections
import traceback
from datetime import datetime

from pytz import timezone, utc

import Utils as util

indian_timezone = timezone('Asia/Calcutta')
TRIGGER_POINT = 3
TAG = 'SCALP'

CLOSE = 'close'
HIGH = 'high'
OPEN = 'open'
LOW = 'low'

LAST_PRICE = 'last_price'
TRADING_SYMBOL = 12680706
LOTS = 25
STOP_LOSS = 50
TARGET = 100
TRAIL = 50
MIN_RISE = 50


def custom_time(*args):
    utc_dt = utc.localize(datetime.utcnow())
    my_tz = indian_timezone
    converted = utc_dt.astimezone(my_tz)
    return converted.timetuple()
