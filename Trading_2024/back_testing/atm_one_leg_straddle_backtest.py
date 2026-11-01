"""Second-close, read-only SQLite short-straddle replay. Python 3.10+, stdlib only.

All prices, P&L and thresholds are integer paise internally. See README.md for
execution assumptions, risk ordering, DTE and profit-giveback semantics.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import itertools
import json
import math
import sqlite3
import sys
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from pathlib import Path

SCRIPT_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()

def paise(value):
    try:
        number = Decimal(str(value))
        if not number.is_finite():
            raise ValueError('Prices and amounts must be finite')
        return int((number * 100).quantize(Decimal('1'), rounding=ROUND_HALF_UP))
    except InvalidOperation as exc:
        raise ValueError('Invalid price or amount') from exc


def rupees(value):
    return round(value / 100, 2)


class OrderCharges:
    """Source-script Zerodha formula, allocated to each executed option order.

    All rates are copied from the supplied V3OPT script (including its shared
    NSE rate for SENSEX). Round the total per order to paise, HALF_UP. This can
    differ by a few paise from rounding an entire four-order attempt once.
    No extra fee_per_leg_order is added when this model is enabled.
    """
    FIELDS = ('brokerage_per_order', 'stt_sell_rate', 'exchange_transaction_rate',
              'sebi_per_crore', 'stamp_buy_rate', 'ipft_per_crore', 'gst_rate')

    def __init__(self, config):
        self.settings = config.get('transaction_costs')
        self.flat = paise(config['fee_per_leg_order'])
        self.ratios = {}
        if self.settings is None:
            return
        values = {k: Decimal(str(self.settings[k])) for k in self.FIELDS}
        gst = values['gst_rate']
        self.fixed = values['brokerage_per_order'] * 100 * (1 + gst)
        common = ((values['exchange_transaction_rate'] + values['sebi_per_crore'] / 10_000_000)
                  * (1 + gst) + values['ipft_per_crore'] / 10_000_000)
        for action, extra in [('SELL', values['stt_sell_rate']), ('BUY', values['stamp_buy_rate'])]:
            # Compile Decimal constants once; per-second marks use integer math.
            fixed_n, fixed_d = self.fixed.as_integer_ratio()
            rate_n, rate_d = (common + extra).as_integer_ratio()
            denominator = math.lcm(fixed_d, rate_d)
            self.ratios[action] = (fixed_n * (denominator // fixed_d),
                                   rate_n * (denominator // rate_d), denominator)

    def total(self, action, fill_price, quantity):
        if action not in ('SELL', 'BUY'):
            raise ValueError('Order action must be SELL or BUY')
        if fill_price < 0 or quantity <= 0:
            raise ValueError('Charge calculation requires nonnegative price and positive quantity')
        if self.settings is None:
            return self.flat
        fixed, rate, denominator = self.ratios[action]
        numerator = fixed + fill_price * quantity * rate
        return (2 * numerator + denominator) // (2 * denominator)


def seconds(value):
    parts = [int(v) for v in value.split(':')]
    if len(parts) == 2:
        parts.append(0)
    h, m, s = parts
    if not (0 <= h < 24 and 0 <= m < 60 and 0 <= s < 60):
        raise ValueError(f'Invalid time: {value}')
    return h * 3600 + m * 60 + s


def clock(value):
    return f'{value // 3600:02d}:{value // 60 % 60:02d}:{value % 60:02d}'


def is_session(day, calendar, index):
    if day.year not in calendar['covered_years']:
        raise ValueError(f'Calendar does not cover {day.year}; supply its holiday list')
    text = day.isoformat()
    holidays = calendar.get('holidays_by_index', {}).get(index, calendar['holidays'])
    if text in calendar.get('special_open_dates', []):
        return True
    return day.weekday() < 5 and text not in holidays


def trading_dte(day, expiry, calendar, index):
    if expiry < day:
        return -1
    # DTE 0 on expiry; count open dates strictly after today, through expiry.
    if not is_session(day, calendar, index) or not is_session(expiry, calendar, index):
        raise ValueError(f'Non-session trading date or stored expiry: {day}, {expiry}')
    return sum(is_session(day + timedelta(days=n), calendar, index)
               for n in range(1, (expiry - day).days + 1))


def validate(config):
    def numeric(value):
        return type(value) in (int, float) and math.isfinite(value)

    if seconds(config['entry_time']) >= seconds(config['exit_time']):
        raise ValueError('entry_time must precede exit_time')
    for key in ('day_loss_limit', 'profit_giveback', 'profit_giveback_activation'):
        if not numeric(config[key]) or config[key] <= 0 or paise(config[key]) <= 0:
            raise ValueError(f'{key} must be positive')
    for key in ('cooldown_minutes', 'slippage_points_per_order', 'fee_per_leg_order'):
        if not numeric(config[key]) or config[key] < 0:
            raise ValueError(f'{key} must be nonnegative')
    if not numeric(config['minimum_premium_remaining']) or not 0 <= config['minimum_premium_remaining'] <= 1:
        raise ValueError('minimum_premium_remaining must be between 0 and 1')
    enabled = config.get('enabled_dtes', [0, 1, 2])
    if (not isinstance(enabled, list) or not enabled or
        any(type(d) is not int or d not in (0, 1, 2) for d in enabled) or len(set(enabled)) != len(enabled)):
        raise ValueError('enabled_dtes must be a nonempty list of unique integers from 0, 1, 2')
    stop_keys = set(config['stop_loss_by_dte'])
    if not {str(d) for d in enabled} <= stop_keys or not stop_keys <= {'0', '1', '2'}:
        raise ValueError('stop_loss_by_dte must specify every enabled DTE, using only keys 0, 1, 2')
    if any(not numeric(v) or v <= 0 or paise(v) <= 0 for v in config['stop_loss_by_dte'].values()):
        raise ValueError('Stop losses must be positive')
    for key in ('quantity', 'strike_step'):
        for value in config[key].values():
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ValueError(f'{key} values must be positive integers')
    age = config['max_quote_age_seconds']
    if age is not None and (type(age) is not int or age < 0):
        raise ValueError('max_quote_age_seconds must be null or a nonnegative integer')
    costs = config.get('transaction_costs')
    if costs is not None:
        if not isinstance(costs, dict) or costs.get('model') != 'zerodha_source':
            raise ValueError('transaction_costs.model must be zerodha_source')
        if config['fee_per_leg_order'] != 0:
            raise ValueError('Set fee_per_leg_order=0 with transaction_costs; brokerage is already included')
        for key in OrderCharges.FIELDS:
            if key not in costs or not numeric(costs[key]) or costs[key] < 0:
                raise ValueError(f'transaction_costs.{key} must be finite and nonnegative')
        for key in ('stt_sell_rate', 'exchange_transaction_rate', 'stamp_buy_rate', 'gst_rate'):
            if costs[key] > 1:
                raise ValueError(f'transaction_costs.{key} must be a decimal fraction, at most 1')


@dataclass(frozen=True)
class Instrument:
    token: int
    index: str
    symbol: str
    kind: str
    option_type: str
    strike: int | None
    expiry: str | None


@dataclass
class Session:
    source: Path
    schema: str
    day: str
    index: str
    expiry: str
    instruments: dict[int, Instrument]
    underlying_token: int
    last_second: int


def connect_readonly(path):
    connection = sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True)
    connection.execute('PRAGMA query_only=ON')
    return connection


def inspect_database(path):
    with closing(connect_readonly(path)) as con:
        tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        metadata = dict(con.execute('SELECT key,value FROM run_metadata'))
        if 'bars' in tables:
            schema = 'v3'
            day = metadata['trading_day']
            records = [Instrument(int(t), i.upper(), s, 'UNDERLYING' if k == 1 else 'OPTION',
                                  ot, st, exp)
                       for t, i, s, k, ot, st, exp in con.execute(
                           'SELECT token,index_name,symbol,kind,option_type,strike,expiry FROM instruments')]
            # Primary-key maximum is cheap even for a large source database.
            last = con.execute('SELECT k >> 32 FROM bars ORDER BY k DESC LIMIT 1').fetchone()
            last_second = int(last[0]) if last else -1
        elif 'second_bars' in tables:
            schema = 'v2'
            records = [Instrument(int(t), i.upper(), s, k.upper(), ot, st, exp)
                       for t, i, s, k, ot, st, exp in con.execute(
                           'SELECT instrument_token,index_name,tradingsymbol,instrument_kind,'
                           'option_type,strike,expiry FROM instruments')]
            first = con.execute('SELECT second_ts FROM second_bars ORDER BY second_ts LIMIT 1').fetchone()
            last = con.execute('SELECT second_ts FROM second_bars ORDER BY second_ts DESC LIMIT 1').fetchone()
            if not first:
                raise ValueError('Empty second_bars table')
            day = first[0][:10]
            if last[0][:10] != day:
                raise ValueError('Multiple dates in second_bars; split the database by day')
            last_second = seconds(last[0][11:19])
        else:
            raise ValueError('Unsupported database schema')
        sessions = []
        for index in sorted({r.index for r in records}):
            underlyings = [r for r in records if r.index == index and r.kind == 'UNDERLYING']
            expiries = sorted({r.expiry for r in records if r.index == index and r.kind == 'OPTION'
                               and r.expiry and r.expiry >= day})
            if len(underlyings) != 1 or not expiries:
                raise ValueError(f'{index}: need one underlying and an unexpired option expiry')
            expiry = expiries[0]  # Nearest expiry in the stored instrument universe.
            inst = {r.token: r for r in records if r.index == index and
                    (r.kind == 'UNDERLYING' or r.expiry == expiry)}
            if schema == 'v3':
                placeholders = ','.join('?' for _ in inst)
                own_last = con.execute(
                    f'SELECT k >> 32 FROM bars WHERE (k & 4294967295) IN ({placeholders}) '
                    'ORDER BY k DESC LIMIT 1', tuple(inst)).fetchone()
                last_second = int(own_last[0]) if own_last else -1
            else:
                own_last = con.execute(
                    'SELECT second_ts FROM second_bars WHERE index_name=? '
                    "AND (instrument_kind='UNDERLYING' OR expiry=?) ORDER BY second_ts DESC LIMIT 1",
                    (index, expiry)).fetchone()
                last_second = seconds(own_last[0][11:19]) if own_last else -1
            sessions.append(Session(path, schema, day, index, expiry, inst,
                                    underlyings[0].token, last_second))
        return sessions


def quote_groups(session, end):
    """Yield complete second batches; never evaluate a partly updated second."""
    with closing(connect_readonly(session.source)) as con:
        if session.schema == 'v3':
            scale = int(dict(con.execute('SELECT key,value FROM run_metadata')).get('price_scale', '100'))
            query = 'SELECT k >> 32,k & 4294967295,c FROM bars WHERE k < ? ORDER BY k'
            rows = con.execute(query, ((end + 1) << 32,))
            normalized = ((int(t), int(token), int(c) if scale == 100 else paise(Decimal(c) / scale))
                          for t, token, c in rows if token in session.instruments)
        else:
            query = ('SELECT second_ts,instrument_token,close FROM second_bars '
                     'WHERE index_name=? AND second_ts<? ORDER BY second_ts,instrument_token')
            # An exclusive next-second bound includes both ISO formats at end:
            # HH:MM:SS+05:30 and HH:MM:SS.000000+05:30. Comparing the latter to
            # a <= HH:MM:SS+05:30 bound incorrectly drops the whole exit second.
            upper = (datetime.fromisoformat(session.day) + timedelta(seconds=end + 1)).isoformat()
            rows = con.execute(query, (session.index, upper))
            normalized = ((seconds(t[11:19]), int(token), paise(c)) for t, token, c in rows
                          if token in session.instruments and c is not None)
        for second, group in itertools.groupby(normalized, key=lambda r: r[0]):
            yield second, [(token, price) for _, token, price in group]


@dataclass
class Leg:
    instrument: Instrument
    entry: int
    entry_second: int
    cycle: int
    entry_charge: int = 0


class Engine:
    def __init__(self, session, config, dte):
        if dte not in config.get('enabled_dtes', [0, 1, 2]):
            raise ValueError(f'DTE {dte} is not enabled by this configuration')
        self.session, self.config, self.dte = session, config, dte
        self.start, self.end = seconds(config['entry_time']), seconds(config['exit_time'])
        self.quantity = config['quantity'][session.index]
        self.step = config['strike_step'][session.index]
        self.stop = paise(config['stop_loss_by_dte'][str(dte)])
        self.slip, self.fee = paise(config['slippage_points_per_order']), paise(config['fee_per_leg_order'])
        self.charges = OrderCharges(config)
        self.limit = paise(config['day_loss_limit'])
        self.giveback = paise(config['profit_giveback'])
        self.activation = paise(config['profit_giveback_activation'])
        self.quotes, self.legs = {}, {}
        self.options = {(i.option_type, i.strike): i for i in session.instruments.values() if i.kind == 'OPTION'}
        self.realized = self.baseline = 0
        self.cycle = self.adjustments = 0
        self.adjusted = self.halted = self.armed = False
        self.next_entry = self.start
        self.peak = self.max_pnl = self.min_pnl = self.max_intraday_drawdown = 0
        self.events, self.trades, self.equity = [], [], []
        self.missing_entry_seconds = self.stale_mark_seconds = 0
        self.max_position_quote_age = 0
        self.last_evaluated = self.end_reason = None

    def timestamp(self, second):
        return f'{self.session.day}T{clock(second)}+05:30'

    def log(self, now, event, **details):
        self.events.append(dict(date=self.session.day, index=self.session.index, dte=self.dte,
                                time=self.timestamp(now), event=event, cycle=self.cycle,
                                daily_pnl=rupees(self.net()), **details))

    def net(self):
        return self.realized + sum((leg.entry - self.quotes[leg.instrument.token][0] - self.slip)
                                   * self.quantity - self.charges.total('BUY',
                                       self.quotes[leg.instrument.token][0] + self.slip, self.quantity)
                                   for leg in self.legs.values())

    def executable(self, token, now):
        quote = self.quotes.get(token)
        max_age = self.config['max_quote_age_seconds']
        return quote is not None and quote[0] > self.slip and (
            max_age is None or now - quote[1] <= max_age)

    def atm(self, now):
        token = self.session.underlying_token
        if not self.executable(token, now):
            return None
        # Round to the nearest strike; exact half-step ties round upward.
        spot = self.quotes[token][0]
        step = self.step * 100
        return ((spot + step // 2) // step) * self.step

    def underlying_details(self, now):
        price, quoted = self.quotes[self.session.underlying_token]
        return dict(underlying_price=rupees(price), underlying_quote_time=self.timestamp(quoted),
                    underlying_quote_age_seconds=now - quoted)

    def open_leg(self, inst, now, reason):
        price, quote_second = self.quotes[inst.token]
        entry = price - self.slip
        fee = self.charges.total('SELL', entry, self.quantity)
        self.realized -= fee
        self.legs[inst.option_type] = Leg(inst, entry, now, self.cycle, fee)
        self.max_position_quote_age = max(self.max_position_quote_age, now - quote_second)
        self.trades.append(dict(date=self.session.day, index=self.session.index, dte=self.dte,
                                time=self.timestamp(now), cycle=self.cycle, action='SELL', reason=reason,
                                token=inst.token, symbol=inst.symbol, option_type=inst.option_type,
                                strike=inst.strike, expiry=inst.expiry, quantity=self.quantity,
                                price=rupees(entry), mark_price=rupees(price), fee=rupees(fee),
                                quote_time=self.timestamp(quote_second), quote_age_seconds=now - quote_second,
                                leg_gross_pnl=0, realized_pnl_change=rupees(-fee)))

    def close_leg(self, option_type, now, reason):
        leg = self.legs.pop(option_type)
        inst = leg.instrument
        price, quote_second = self.quotes[inst.token]
        fill = price + self.slip
        gross = (leg.entry - fill) * self.quantity
        fee = self.charges.total('BUY', fill, self.quantity)
        self.realized += gross - fee
        self.trades.append(dict(date=self.session.day, index=self.session.index, dte=self.dte,
                                time=self.timestamp(now), cycle=self.cycle, action='BUY', reason=reason,
                                token=inst.token, symbol=inst.symbol, option_type=option_type,
                                strike=inst.strike, expiry=inst.expiry, quantity=self.quantity,
                                price=rupees(fill), mark_price=rupees(price), fee=rupees(fee),
                                quote_time=self.timestamp(quote_second), quote_age_seconds=now - quote_second,
                                leg_gross_pnl=rupees(gross), realized_pnl_change=rupees(gross - fee)))

    def close_all(self, now, reason, **details):
        for option_type in list(self.legs):
            self.close_leg(option_type, now, reason)
        self.log(now, reason, **details)
        self.end_reason = reason

    def cooldown(self, now, reason, **details):
        self.close_all(now, reason, **details)
        self.next_entry = now + int(round(self.config['cooldown_minutes'] * 60))
        self.log(now, 'COOLDOWN', next_entry_time=self.timestamp(self.next_entry))

    def enter(self, now):
        strike = self.atm(now)
        candidates = [self.options.get((ot, strike)) for ot in ('CE', 'PE')]
        if any(i is None or not self.executable(i.token, now) for i in candidates):
            self.missing_entry_seconds += 1
            return
        self.baseline = self.net()
        self.cycle += 1
        self.adjusted = False
        for inst in candidates:
            self.open_leg(inst, now, 'STRADDLE_ENTRY')
        self.log(now, 'STRADDLE_ENTRY', atm_strike=strike, baseline_pnl=rupees(self.baseline),
                 **self.underlying_details(now))

    def stop_hit(self, now):
        loss = self.net() - self.baseline
        if self.adjusted:
            self.cooldown(now, 'SECOND_STOP', trigger_segment_pnl=rupees(loss),
                          baseline_pnl=rupees(self.baseline))
            return
        # Classify each leg after its own entry fee and estimated buyback fee.
        # Fees are already booked/estimated by net(); this is classification only.
        leg_pnl = {ot: (leg.entry - self.quotes[leg.instrument.token][0] - self.slip) * self.quantity
                   - leg.entry_charge - self.charges.total('BUY',
                       self.quotes[leg.instrument.token][0] + self.slip, self.quantity)
                   for ot, leg in self.legs.items()}
        losers = [ot for ot, pnl in leg_pnl.items() if pnl < 0]
        winners = [ot for ot, pnl in leg_pnl.items() if pnl > 0]
        reason = 'FIRST_STOP_NO_PROFITABLE_LEG'
        if len(losers) == 1 and len(winners) == 1:
            winner = self.legs[winners[0]]
            remaining = self.quotes[winner.instrument.token][0] / winner.entry
            if remaining + 1e-12 >= self.config['minimum_premium_remaining']:
                strike = self.atm(now)
                replacement = self.options.get((losers[0], strike))
                if replacement and self.executable(replacement.token, now):
                    old_strike = self.legs[losers[0]].instrument.strike
                    self.close_leg(losers[0], now, 'ADJUST_LOSING_LEG')
                    self.open_leg(replacement, now, 'ATM_REPLACEMENT')
                    self.adjusted = True
                    self.adjustments += 1
                    self.baseline = self.net()
                    self.log(now, 'ADJUSTMENT', replaced_type=losers[0], old_strike=old_strike,
                             new_strike=strike, profitable_premium_remaining=remaining,
                             profitable_type=winners[0], profitable_entry_price=rupees(winner.entry),
                             profitable_current_price=rupees(self.quotes[winner.instrument.token][0]),
                             profitable_leg_net_pnl=rupees(leg_pnl[winners[0]]),
                             trigger_segment_pnl=rupees(loss), baseline_pnl=rupees(self.baseline),
                             **self.underlying_details(now))
                    return
                reason = 'FIRST_STOP_NO_ATM_QUOTE'
            else:
                reason = 'FIRST_STOP_LOW_PREMIUM'
        self.cooldown(now, reason, trigger_segment_pnl=rupees(loss),
                      baseline_pnl=rupees(self.baseline))

    def observe(self, now, pnl=None):
        if pnl is None:
            pnl = self.net()
        self.max_pnl = max(self.max_pnl, pnl)
        self.min_pnl = min(self.min_pnl, pnl)
        self.max_intraday_drawdown = max(self.max_intraday_drawdown, self.max_pnl - pnl)
        stamp = self.timestamp(now)
        existing = bool(self.equity and self.equity[-1]['time'] == stamp)
        if now % 60 == 0 or now == self.end or self.last_evaluated is None or existing:
            row = dict(date=self.session.day, index=self.session.index, dte=self.dte,
                                    time=self.timestamp(now), daily_pnl=rupees(pnl),
                                    realized_pnl=rupees(self.realized), open_legs=len(self.legs),
                                    cycle=self.cycle)
            if existing:
                self.equity[-1] = row
            else:
                self.equity.append(row)
        self.last_evaluated = now

    def on_second(self, now, updates=()):
        for token, price in updates:
            if price >= 0 and token in self.session.instruments:
                self.quotes[token] = (price, now)
        if now < self.start or self.halted:
            return
        for leg in self.legs.values():
            age = now - self.quotes[leg.instrument.token][1]
            self.max_position_quote_age = max(self.max_position_quote_age, age)
        max_age = self.config['max_quote_age_seconds']
        if max_age is not None and any(now - self.quotes[l.instrument.token][1] > max_age
                                       for l in self.legs.values()):
            self.stale_mark_seconds += 1
        pnl = self.net()
        self.peak = max(self.peak, pnl)
        if pnl >= self.activation:
            self.armed = True
        self.observe(now, pnl)
        # Deterministic priority: hard daily loss, scheduled exit, giveback, strategy stop.
        if self.legs and pnl <= -self.limit:
            self.close_all(now, 'DAY_LOSS_LIMIT')
            self.halted = True
        elif now >= self.end:
            if self.legs:
                self.close_all(now, 'END_OF_DAY')
            self.halted = True
        elif self.legs:
            if self.armed and self.peak - pnl >= self.giveback:
                self.cooldown(now, 'PROFIT_GIVEBACK', peak_pnl=rupees(self.peak),
                              drawdown_from_peak=rupees(self.peak - pnl))
                self.peak = self.net()
                self.armed = False
            elif pnl - self.baseline <= -self.stop:
                self.stop_hit(now)
        elif now >= self.next_entry:
            self.enter(now)
        # Orders can themselves cross the daily limit through fees/slippage.
        # Enforce it in this second, before any later quote can mask the breach.
        post_trade_pnl = self.net()
        if self.legs and post_trade_pnl <= -self.limit:
            self.close_all(now, 'DAY_LOSS_LIMIT')
            self.halted = True
        elif self.legs and self.armed and self.peak - post_trade_pnl >= self.giveback:
            self.cooldown(now, 'PROFIT_GIVEBACK', peak_pnl=rupees(self.peak),
                          drawdown_from_peak=rupees(self.peak - post_trade_pnl))
            self.peak = self.net()
            self.armed = False
        elif self.legs and post_trade_pnl - self.baseline <= -self.stop:
            # This can newly occur on entry through round-trip costs. Both
            # freshly sold legs are then unprofitable, so stop_hit closes both.
            self.stop_hit(now)
        self.observe(now, post_trade_pnl)

    def run(self):
        previous = None
        for now, updates in quote_groups(self.session, self.end):
            if previous is not None:
                for missing_second in range(max(previous + 1, self.start), now):
                    self.on_second(missing_second)
            self.on_second(now, updates)
            previous = now
            if self.halted:
                break
        if previous is not None and not self.halted:
            for now in range(max(previous + 1, self.start), self.end + 1):
                self.on_second(now)
        if self.legs:
            raise AssertionError('Unclosed positions')
        status = 'OK' if self.session.last_second >= self.end else 'INCOMPLETE_SOURCE'
        if not self.cycle:
            status = 'NO_TRADES'
        return dict(source_file=self.session.source.name, date=self.session.day, index=self.session.index,
                    expiry=self.session.expiry, dte=self.dte, status=status, quantity=self.quantity,
                    net_pnl=rupees(self.realized), cycles=self.cycle, adjustments=self.adjustments,
                    gross_pnl=rupees(self.realized + sum(paise(t['fee']) for t in self.trades)),
                    transaction_charges=rupees(sum(paise(t['fee']) for t in self.trades)),
                    max_intraday_pnl=rupees(self.max_pnl), min_intraday_pnl=rupees(self.min_pnl),
                    max_intraday_drawdown=rupees(self.max_intraday_drawdown),
                    day_loss_hit=any(e['event'] == 'DAY_LOSS_LIMIT' for e in self.events),
                    giveback_exits=sum(e['event'] == 'PROFIT_GIVEBACK' for e in self.events),
                    missing_entry_seconds=self.missing_entry_seconds,
                    max_position_quote_age_seconds=self.max_position_quote_age,
                    stale_mark_seconds=self.stale_mark_seconds,
                    last_source_time=clock(self.session.last_second) if self.session.last_second >= 0 else '',
                    end_reason=self.end_reason or '',
                    first_entry=next((t['time'] for t in self.trades if t['action'] == 'SELL'), ''),
                    last_exit=next((t['time'] for t in reversed(self.trades) if t['action'] == 'BUY'), ''))


def write_csv(path, rows):
    if not rows:
        path.write_text('', encoding='utf-8')
        return
    fields = list(dict.fromkeys(k for row in rows for k in row))
    with path.open('w', newline='', encoding='utf-8-sig') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def statistics(rows):
    rows = sorted(rows, key=lambda r: (r['date'], r['index']))
    daily = {}
    for row in rows:
        daily[row['date']] = daily.get(row['date'], 0) + paise(row['net_pnl'])
    equity = peak = max_drawdown = 0
    for pnl in daily.values():
        equity += pnl
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
    wins = sum(paise(r['net_pnl']) > 0 for r in rows)
    gain = sum(max(0, paise(r['net_pnl'])) for r in rows)
    loss = -sum(min(0, paise(r['net_pnl'])) for r in rows)
    result = dict(sessions=len(rows), trading_dates=len(daily), total_pnl=rupees(equity),
                winning_sessions=wins, losing_sessions=sum(r['net_pnl'] < 0 for r in rows),
                win_rate_pct=round(100 * wins / len(rows), 2) if rows else 0,
                average_session_pnl=round(rupees(equity) / len(rows), 2) if rows else 0,
                best_session=max((r['net_pnl'] for r in rows), default=0),
                worst_session=min((r['net_pnl'] for r in rows), default=0),
                max_drawdown_daily_close=rupees(max_drawdown),
                profit_factor=round(gain / loss, 4) if loss else None,
                cycles=sum(r['cycles'] for r in rows), adjustments=sum(r['adjustments'] for r in rows),
                day_loss_hits=sum(r['day_loss_hit'] for r in rows),
                giveback_exits=sum(r['giveback_exits'] for r in rows))
    if rows and all('transaction_charges' in row and 'gross_pnl' in row for row in rows):
        result['gross_pnl'] = rupees(sum(paise(r['gross_pnl']) for r in rows))
        result['transaction_charges'] = rupees(sum(paise(r['transaction_charges']) for r in rows))
    return result


def write_run_status(output, status, **details):
    path = output / 'run_status.json'
    temporary = output / 'run_status.json.tmp'
    payload = dict(status=status, updated_utc=datetime.now(timezone.utc).isoformat(), **details)
    temporary.write_text(json.dumps(payload, indent=2), encoding='utf-8')
    temporary.replace(path)


def load_result_summary(folder):
    status_path = folder / 'run_status.json'
    if status_path.exists():
        status = json.loads(status_path.read_text(encoding='utf-8'))
        if status.get('status') != 'complete':
            raise ValueError(f'{folder}: result run is not complete ({status.get("status")})')
    summary = json.loads((folder / 'summary.json').read_text(encoding='utf-8'))
    if summary.get('errors'):
        raise ValueError(f'{folder}: source errors must be resolved before combining/auditing')
    return summary


def verify_result_totals(summary, daily):
    included = [row for row in daily if row['status'] == 'OK']
    if (len(included) != summary['overall']['sessions'] or
        sum(paise(row['net_pnl']) for row in included) != paise(summary['overall']['total_pnl'])):
        raise ValueError('Daily results do not match the completed summary; possible interrupted overwrite')


def main(argv=None):
    state = {}
    try:
        code = _main(argv, state)
    except BaseException as exc:
        if state:
            for output in state.get('outputs', [state['output']]):
                write_run_status(output, 'failed', error=f'{type(exc).__name__}: {exc}')
        raise
    if state:
        for output in state.get('outputs', [state['output']]):
            write_run_status(output, 'complete' if code == 0 else 'failed', exit_code=code,
                             script_sha256=SCRIPT_SHA256)
    return code


def daily_totals(rows):
    """Aggregate included index sessions by trading date before drawdown math."""
    groups = {}
    for row in rows:
        if row['status'] != 'OK':
            continue
        item = groups.setdefault(row['date'], dict(net=0, gross=0, charges=0, sessions=0))
        item['net'] += paise(row['net_pnl'])
        item['gross'] += paise(row['gross_pnl'])
        item['charges'] += paise(row['transaction_charges'])
        item['sessions'] += 1
    equity = peak = 0
    result = []
    for day, item in sorted(groups.items()):
        equity += item['net']
        peak = max(peak, equity)
        result.append(dict(date=day, sessions=item['sessions'], net_pnl=rupees(item['net']),
                           gross_pnl=rupees(item['gross']), transaction_charges=rupees(item['charges']),
                           cumulative_net_pnl=rupees(equity), drawdown_daily_close=rupees(peak-equity)))
    return result


def _combined_main(args, state, paths):
    profiles, hashes, config_paths, data_dirs = {}, {}, {}, set()
    for path in paths:
        path = path.resolve()
        raw = path.read_bytes()
        config = json.loads(raw.decode('utf-8-sig'))
        validate(config)
        enabled = config.get('enabled_dtes', [0, 1, 2])
        if len(enabled) != 1:
            raise ValueError(f'{path}: combined mode requires exactly one enabled DTE per config')
        dte = enabled[0]
        if dte in profiles:
            raise ValueError(f'Duplicate DTE {dte} configs; provide one each for DTE 0, 1 and 2')
        directory = args.data_dir or Path(config['data_dir'])
        if args.data_dir is None and not directory.is_absolute():
            directory = path.parent / directory
        directory = directory.resolve()
        config['data_dir'] = str(directory)
        data_dirs.add(directory)
        profiles[dte] = config
        hashes[str(dte)] = hashlib.sha256(raw).hexdigest()
        config_paths[str(dte)] = str(path)
    if set(profiles) != {0, 1, 2}:
        raise ValueError('Combined mode requires one config each for DTE 0, 1 and 2')
    if len(data_dirs) != 1:
        raise ValueError('All three configs must use the same data_dir, or supply --data-dir')
    if any(cfg['calendar'] != profiles[0]['calendar'] for cfg in profiles.values()):
        raise ValueError('All three configs must use the same calendar for unambiguous DTE routing')
    data_dir = next(iter(data_dirs))
    output = (args.output or paths[0].resolve().parent / 'results' / 'combined').resolve()
    children = {d: output / f'dte{d}' for d in (0, 1, 2)}
    files = sorted(data_dir.glob(args.file or '*.sqlite3'))
    if not files:
        raise SystemExit(f'No databases found in {data_dir}')
    for folder in [output, *children.values()]:
        folder.mkdir(parents=True, exist_ok=True)
    state.update(output=output, outputs=[output, *children.values()])
    for folder in state['outputs']:
        write_run_status(folder, 'running', script_sha256=SCRIPT_SHA256)
    for dte, cfg in profiles.items():
        cfg['output_dir'] = str(children[dte])
    daily, trades, events, equity, inventory, errors = [], [], [], [], [], []
    seen = set()
    for n, path in enumerate(files, 1):
        print(f'[{n}/{len(files)}] {path.name}', flush=True)
        try:
            for session in inspect_database(path):
                item = dict(source_file=path.name, date=session.day, index=session.index,
                            expiry=session.expiry, schema=session.schema)
                inventory.append(item)
                if (args.index and args.index != session.index or
                    args.from_date and session.day < args.from_date or
                    args.to_date and session.day > args.to_date):
                    item['status'] = 'FILTERED'
                    continue
                dte = trading_dte(date.fromisoformat(session.day), date.fromisoformat(session.expiry),
                                  profiles[0]['calendar'], session.index)
                item['dte'] = dte
                if dte not in profiles:
                    item['status'] = 'DTE_OUTSIDE_0_1_2'
                    continue
                key = session.day, session.index
                if key in seen:
                    raise ValueError(f'Duplicate index/day {key}; refusing to double count')
                seen.add(key)
                engine = Engine(session, profiles[dte], dte)
                row = engine.run()
                daily.append(row)
                trades.extend(engine.trades)
                events.extend(engine.events)
                equity.extend(engine.equity)
                item['status'] = row['status']
                print(f"  {session.index} DTE={dte}: {row['status']}, P&L={row['net_pnl']:,.2f}, "
                      f"cycles={row['cycles']}, adjustments={row['adjustments']}", flush=True)
                write_csv(output / 'daily_results.csv', daily)
        except (sqlite3.Error, ValueError, KeyError, OSError, AssertionError) as exc:
            errors.append(dict(source_file=path.name, error=str(exc)))
            print(f'  ERROR: {exc}', file=sys.stderr, flush=True)
    daily.sort(key=lambda row: (row['date'], row['index']))
    for rows in (trades, events, equity):
        rows.sort(key=lambda row: (row['date'], row['index'], row['time']))

    def save(folder, dte=None):
        selected = daily if dte is None else [r for r in daily if r['dte'] == dte]
        included = [r for r in selected if r['status'] == 'OK']
        selected_inventory = inventory if dte is None else [r for r in inventory if r.get('dte') == dte]
        summary = dict(mode='combined_dte_profiles' if dte is None else 'dte_profile',
                       overall=statistics(included),
                       by_index={i: statistics([r for r in included if r['index'] == i])
                                 for i in sorted({r['index'] for r in included})},
                       by_dte={str(d): statistics([r for r in included if r['dte'] == d])
                               for d in ((0, 1, 2) if dte is None else (dte,))},
                       excluded_sessions=[r for r in selected if r['status'] != 'OK'],
                       source_files=len(files), errors=errors,
                       execution='second close, last known price, no lookahead; amounts in INR',
                       risk_scope='independent per index per trading day',
                       run_filters=dict(index=args.index, from_date=args.from_date, to_date=args.to_date,
                                        file_glob=args.file or '*.sqlite3'),
                       script_sha256=SCRIPT_SHA256)
        if dte is None:
            summary.update(config_paths=config_paths, config_sha256_by_dte=hashes)
            (folder / 'configs_used.json').write_text(
                json.dumps({str(d): profiles[d] for d in (0, 1, 2)}, indent=2), encoding='utf-8')
        else:
            summary.update(config_path=config_paths[str(dte)], config_sha256=hashes[str(dte)])
            (folder / 'config_used.json').write_text(json.dumps(profiles[dte], indent=2), encoding='utf-8')
        outputs = [('daily_results.csv', selected), ('daily_totals.csv', daily_totals(selected)),
                   ('inventory.csv', selected_inventory), ('errors.csv', errors)]
        for filename, all_rows in [('trades.csv', trades), ('events.csv', events), ('equity_1min.csv', equity)]:
            outputs.append((filename, all_rows if dte is None else [r for r in all_rows if r['dte'] == dte]))
        for filename, rows in outputs:
            write_csv(folder / filename, rows)
        (folder / 'summary.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
        return summary

    for dte, folder in children.items():
        component = save(folder, dte)
        s = component['overall']
        print(f"DTE {dte}: {s['sessions']} sessions, net P&L={s['total_pnl']:,.2f}", flush=True)
    summary = save(output)
    print('Combined DTE 0 / 1 / 2:', flush=True)
    print(json.dumps(summary['overall'], indent=2), flush=True)
    print(f'Results: {output}', flush=True)
    return 1 if errors else 0


def _main(argv, state):
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument('--config', type=Path, help='Run one config only')
    modes.add_argument('--configs', type=Path, nargs=3, metavar=('DTE0_JSON', 'DTE1_JSON', 'DTE2_JSON'),
                       help='Load three configs and route each session to its own DTE profile')
    parser.add_argument('--data-dir', type=Path)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--from-date', help='Inclusive YYYY-MM-DD')
    parser.add_argument('--to-date', help='Inclusive YYYY-MM-DD')
    parser.add_argument('--index', choices=['NIFTY', 'SENSEX'])
    parser.add_argument('--file', help='Optional filename glob, e.g. *20261030.sqlite3')
    args = parser.parse_args(argv)
    if args.configs:
        return _combined_main(args, state, args.configs)
    if args.config is None:
        defaults = [Path(__file__).resolve().parent / 'configs' / f'config_dte{d}.json' for d in (0, 1, 2)]
        if all(p.exists() for p in defaults):
            return _combined_main(args, state, defaults)
        if any(p.exists() for p in defaults):
            parser.error('Default combined run needs config_dte0.json, config_dte1.json and config_dte2.json; '
                         'use --config PATH to run just one')
        args.config = Path(__file__).with_name('config.json')
    config_path = args.config.resolve()
    config = json.loads(config_path.read_text(encoding='utf-8-sig'))
    validate(config)
    data_dir = args.data_dir or Path(config['data_dir'])
    if args.data_dir is None and not data_dir.is_absolute():
        data_dir = config_path.parent / data_dir
    output = args.output or config_path.parent / config['output_dir']
    config['data_dir'] = str(data_dir.absolute())
    config['output_dir'] = str(output.absolute())
    output.mkdir(parents=True, exist_ok=True)
    files = sorted(data_dir.glob(args.file or '*.sqlite3'))
    if not files:
        raise SystemExit(f'No databases found in {data_dir}')
    state['output'] = output
    write_run_status(output, 'running', script_sha256=SCRIPT_SHA256)
    daily, trades, events, equity, inventory, errors = [], [], [], [], [], []
    seen = set()
    for n, path in enumerate(files, 1):
        print(f'[{n}/{len(files)}] {path.name}', flush=True)
        try:
            sessions = inspect_database(path)
            for session in sessions:
                item = dict(source_file=path.name, date=session.day, index=session.index,
                            expiry=session.expiry, schema=session.schema)
                inventory.append(item)
                if (args.index and args.index != session.index or
                    args.from_date and session.day < args.from_date or
                    args.to_date and session.day > args.to_date):
                    item['status'] = 'FILTERED'
                    continue
                dte = trading_dte(date.fromisoformat(session.day), date.fromisoformat(session.expiry),
                                  config['calendar'], session.index)
                item['dte'] = dte
                if dte not in (0, 1, 2):
                    item['status'] = 'DTE_OUTSIDE_0_1_2'
                    continue
                if dte not in config.get('enabled_dtes', [0, 1, 2]):
                    item['status'] = 'DTE_NOT_ENABLED'
                    continue
                key = session.day, session.index
                if key in seen:
                    raise ValueError(f'Duplicate index/day {key}; refusing to double count')
                seen.add(key)
                engine = Engine(session, config, dte)
                row = engine.run()
                daily.append(row)
                trades.extend(engine.trades)
                events.extend(engine.events)
                equity.extend(engine.equity)
                item['status'] = row['status']
                print(f"  {session.index} DTE={dte}: {row['status']}, P&L={row['net_pnl']:,.2f}, "
                      f"cycles={row['cycles']}, adjustments={row['adjustments']}", flush=True)
                # Checkpoint after each session so a long folder replay is reviewable.
                write_csv(output / 'daily_results.csv', daily)
        except (sqlite3.Error, ValueError, KeyError, OSError, AssertionError) as exc:
            errors.append(dict(source_file=path.name, error=str(exc)))
            print(f'  ERROR: {exc}', file=sys.stderr, flush=True)
    daily.sort(key=lambda r: (r['date'], r['index']))
    included = [r for r in daily if r['status'] == 'OK']
    summary = dict(overall=statistics(included),
                   by_index={i: statistics([r for r in included if r['index'] == i])
                             for i in sorted({r['index'] for r in included})},
                   by_dte={d: statistics([r for r in included if str(r['dte']) == d])
                           for d in config['stop_loss_by_dte']},
                   excluded_sessions=[r for r in daily if r['status'] != 'OK'],
                   source_files=len(files), errors=errors,
                   execution='second close, last known price, no lookahead; amounts in INR',
                   risk_scope='independent per index per trading day',
                   run_filters=dict(index=args.index, from_date=args.from_date, to_date=args.to_date,
                                    file_glob=args.file or '*.sqlite3'),
                   config_sha256=hashlib.sha256(config_path.read_bytes()).hexdigest(),
                   script_sha256=SCRIPT_SHA256)
    for filename, rows in [('daily_results.csv', daily), ('trades.csv', trades), ('events.csv', events),
                           ('equity_1min.csv', equity), ('inventory.csv', inventory), ('errors.csv', errors)]:
        write_csv(output / filename, rows)
    (output / 'summary.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
    (output / 'config_used.json').write_text(json.dumps(config, indent=2), encoding='utf-8')
    print(json.dumps(summary['overall'], indent=2), flush=True)
    print(f'Results: {output.resolve()}', flush=True)
    return 1 if errors else 0


if __name__ == '__main__':
    sys.exit(main())
