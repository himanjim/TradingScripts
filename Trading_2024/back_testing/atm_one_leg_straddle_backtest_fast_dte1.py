"""Fast Strategy 0 replay with optional DTE=1 trade management.

How it is fast
  1. Each SQLite day file is read ONCE, with the original script's own readers
     (inspect_database / quote_groups), into a small forward-filled cache
     (.npz): underlying close per second, and CE/PE close per second for every
     strike that is ATM at some second of the day (the only strikes the
     strategy can ever trade). Later runs reuse the cache.
  2. The per-second Engine logic runs in a numba-compiled loop (falls back to
     plain Python if numba is not installed: still correct, just slower).
With the optional rules disabled, results match the original Engine.
Writes daily_results.csv, daily_totals.csv, summary.json and trades.csv (tradebook: every order).
No events/equity logs; use the original script for those.

Needs atm_one_leg_straddle_backtest.py in the same folder, plus numpy
(numba optional). Supported: max_quote_age_seconds = null (as in your configs).

Usage
  python atm_one_leg_straddle_backtest_fast_dte1.py --configs config_dte1_max_profit.json \
      --data-dir "C:/Users/himan/Documents/Accelpix_SQLite_1sec" \
      --data-dir "G:/My Drive/Trading/Kite_tick-data" --output results/fast_combined
  Several --data-dir are allowed; the same index/day found twice keeps the first.
  One DTE per day (lowest DTE, ties -> NIFTY) is ON; --all-dtes-per-day turns it off.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from datetime import date
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import atm_one_leg_straddle_backtest as bt  # noqa: E402

try:
    from numba import njit
    HAVE_NUMBA = True
except ImportError:  # pragma: no cover
    HAVE_NUMBA = False

    def njit(*args, **kwargs):
        if args and callable(args[0]):
            return args[0]
        return lambda f: f

# =============================================================================
# RUN SETTINGS - edit these to run straight from the IDE (PyCharm: just press Run).
# Anything given on the command line overrides them.
# -----------------------------------------------------------------------------
CONFIGS = [HERE / 'configs/config_dte1_max_profit.json']
DATA_DIRS = None                  # None = use "data_dir" from the configs; or a list, e.g.
                                  # [r'C:\Users\himan\Documents\Accelpix_SQLite_1sec', r'G:\My Drive\Trading\Kite_tick-data']
OUTPUT_DIR = HERE / 'results' / 'fast_dte1'
CACHE_DIR = HERE / 'atm_cache'    # per-day cache; first run builds it, later runs are fast
FROM_DATE = None                  # e.g. '2026-01-01' (inclusive) or None
TO_DATE = None                    # e.g. '2026-06-30' (inclusive) or None
INDEX = None                      # 'NIFTY', 'SENSEX' or None for both
ONE_DTE_PER_DAY = True            # False = old behaviour (all DTE 0-2 sessions each day)
# =============================================================================

T0, T1 = 9 * 3600 + 15 * 60, 15 * 3600 + 29 * 60 + 59   # cache grid 09:15:00 .. 15:29:59
N = T1 - T0 + 1
CACHE_VERSION = 1


# ----------------------------------------------------------------- cache build
def _series(secs, prices, mask):
    """Forward-filled per-second close on the grid; -1 = no quote yet."""
    arr = np.full(N, -1, np.int64)
    if not mask.any():
        return arr
    sc = np.clip(secs[mask] - T0, 0, N - 1)
    pr = prices[mask]
    _, first_rev = np.unique(sc[::-1], return_index=True)   # last update per second
    last = len(sc) - 1 - first_rev
    arr[sc[last]] = pr[last]
    idx = np.where(arr >= 0, np.arange(N), -1)
    np.maximum.accumulate(idx, out=idx)
    return np.where(idx >= 0, arr[np.maximum(idx, 0)], -1)


def build_session_cache(session, step):
    secs, toks, prices = [], [], []
    for sec, group in bt.quote_groups(session, T1):
        for tok, p in group:
            if p >= 0:
                secs.append(sec)
                toks.append(tok)
                prices.append(p)
    secs, toks, prices = (np.asarray(x, np.int64) for x in (secs, toks, prices))
    spot = _series(secs, prices, toks == session.underlying_token)
    sp = spot[spot >= 0]
    stepp = step * 100
    strikes = np.unique(((sp + stepp // 2) // stepp) * step).astype(np.int64)
    if len(strikes) == 0:
        strikes = np.zeros(1, np.int64)
    contracts = {}
    for tok, i in session.instruments.items():
        if i.kind == 'OPTION':
            key = (0 if i.option_type == 'CE' else 1, i.strike)
            if key in contracts:
                raise ValueError(f'{session.index} {session.day}: duplicate contract {key}')
            contracts[key] = tok
    opt = np.full((len(strikes), 2, N), -1, np.int64)
    for k, st in enumerate(strikes):
        for ot in (0, 1):
            tok = contracts.get((ot, int(st)))
            if tok is not None:
                opt[k, ot] = _series(secs, prices, toks == tok)
    return dict(spot=spot, opt=opt, strikes=strikes)


def load_sessions(data_dirs, cache_root, calendar, strike_steps, file_glob='*.sqlite3', log=print,
                  selected_dtes=(0, 1, 2)):
    """List of session dicts (one per index/day) with DTE computed from `calendar`."""
    out, seen = [], set()
    for data_dir in data_dirs:
        data_dir = Path(data_dir)
        cdir = Path(cache_root) / hashlib.sha1(str(data_dir.resolve()).encode()).hexdigest()[:10]
        cdir.mkdir(parents=True, exist_ok=True)
        files = sorted(data_dir.glob(file_glob))
        for n, path in enumerate(files, 1):
            st = path.stat()
            stamp = f'{CACHE_VERSION}|{st.st_size}|{st.st_mtime_ns}'
            try:
                sessions = bt.inspect_database(path)
            except Exception as exc:  # same spirit as original: report, continue
                log(f'  ERROR {path.name}: {exc}')
                continue
            for s in sessions:
                try:
                    dte = bt.trading_dte(date.fromisoformat(s.day), date.fromisoformat(s.expiry),
                                         calendar, s.index)
                except ValueError as exc:
                    log(f'  ERROR {path.name} {s.index}: {exc}')
                    continue
                if dte not in selected_dtes:
                    continue
                key = (s.day, s.index)
                if key in seen:
                    log(f'  duplicate {key} in {path.name}: kept the first data dir')
                    continue
                seen.add(key)
                step = strike_steps[s.index]
                cf = cdir / f'{path.stem}__{s.index}.npz'
                data = None
                if cf.exists():
                    z = np.load(cf)
                    if str(z['stamp']) == stamp and int(z['step']) == step:
                        data = dict(spot=z['spot'].astype(np.int64), opt=z['opt'].astype(np.int64),
                                    strikes=z['strikes'].astype(np.int64))
                if data is None:
                    log(f'  caching [{n}/{len(files)}] {path.name} {s.index}')
                    data = build_session_cache(s, step)
                    tmp = cf.with_suffix('.tmp.npz')
                    np.savez_compressed(tmp, spot=data['spot'].astype(np.int32),
                                        opt=data['opt'].astype(np.int32), strikes=data['strikes'],
                                        step=step, stamp=stamp)
                    tmp.replace(cf)
                out.append(dict(source_file=path.name, date=s.day, index=s.index, expiry=s.expiry,
                                dte=dte, step=step, last=s.last_second, **data))
    out.sort(key=lambda r: (r['date'], r['index']))
    return out


def one_dte_per_day(sessions):
    """Keep only the lowest-DTE session per day (ties -> alphabetical index)."""
    best = {}
    for s in sessions:
        if (s['dte'], s['index']) < best.get(s['date'], (9, '')):
            best[s['date']] = (s['dte'], s['index'])
    return [s for s in sessions if best[s['date']] == (s['dte'], s['index'])]


# ---------------------------------------------------------------- engine
def charge_tables(config, max_price):
    """[index (0 NIFTY, 1 SENSEX), action (0 SELL, 1 BUY), price paise] -> charge paise."""
    oc = bt.OrderCharges(config)
    tabs = np.zeros((2, 2, max_price + 1), np.int64)
    p = np.arange(max_price + 1, dtype=object)
    for i, idx in enumerate(('NIFTY', 'SENSEX')):
        if oc.settings is None:
            tabs[i] = oc.flat
            continue
        q = config['quantity'][idx]
        for a, act in enumerate(('SELL', 'BUY')):
            fixed, rate, den = oc.ratios[act]
            tabs[i, a] = np.array((2 * (fixed + p * q * rate) + den) // (2 * den), dtype=np.int64)
    return tabs


@njit(cache=True)
def run_session(spot, opt, strikes, step, qty, sell_t, buy_t,
                start, end, stop, slip, limit, giveback, activation, minprem, cool, L,
                pause_frac=-1.0, pause_secs=0, max_pauses=1, max_ratio=-1.0, max_pct=-1.0, max_straddles=0,
                max_lp_stops=0, max_stop_outs=0, allow=np.ones(1, np.uint8), gate=0,
                management=(T1, 0, 0, 0, 0, -1, 0, 0)):
    """Exact port of Engine.on_second/stop_hit/enter for max_quote_age=None.
    Returns (realized, cycles, adjustments, day_loss_hit, giveback_exits, charges, pauses, n_orders).
    Every order is written to L (rows: second, 0 SELL/1 BUY, 0 CE/1 PE, strike, fill, mark, fee,
    reason code, cycle, leg gross P&L) in the original script's order; see REASONS."""
    last_entry, daily_target, cycle_target, stop_window, stop_wait, giveback_wait, cycle_arm, cycle_giveback = management
    smin = strikes[0]
    nk = strikes.shape[0]
    span = (strikes[nk - 1] - smin) // step + 1
    kmap = np.full(span, -1, np.int64)
    for k in range(nk):
        kmap[(strikes[k] - smin) // step] = k
    stepp = step * 100
    realized = 0
    baseline = 0
    cycle_start_pnl = 0
    cycle_peak = 0
    previous_stop = -1
    cycle = 0
    adjustments = 0
    adjusted = False
    halted = False
    armed = False
    next_entry = start
    peak = 0
    dayloss = 0
    gbx = 0
    fees = 0
    pauses = 0
    npause = 0
    first_st = -1
    nlog = 0
    n_lp = 0
    n_so = 0
    first = 0          # which leg the original's legs dict lists first (close order)
    reason = 0
    act = np.zeros(2, np.bool_)
    lk = np.zeros(2, np.int64)
    le = np.zeros(2, np.int64)
    lc = np.zeros(2, np.int64)
    for t in range(start, end + 1):
        g = t - T0
        nl = int(act[0]) + int(act[1])
        pnl = realized
        for o in range(2):
            if act[o]:
                q = opt[lk[o], o, g]
                pnl += (le[o] - q - slip) * qty - buy_t[q + slip]
        if pnl > peak:
            peak = pnl
        if nl > 0 and cycle_arm > 0:
            cycle_peak = max(cycle_peak, pnl - cycle_start_pnl)
        if pnl >= activation:
            armed = True
        for stage in range(2):           # 0: main decision, 1: post-trade re-check
            if stage == 1:
                nl = int(act[0]) + int(act[1])
                if nl == 0:
                    break
                pnl = realized
                for o in range(2):
                    if act[o]:
                        q = opt[lk[o], o, g]
                        pnl += (le[o] - q - slip) * qty - buy_t[q + slip]
            do_close = 0   # 1 day-loss, 2 end-of-day, 3 giveback cooldown, 4 stop cooldown
            do_stop = False
            do_enter = False
            if nl > 0 and pnl <= -limit:
                do_close = 1
                reason = 3
            elif stage == 0 and t >= end:
                do_close = 2
                reason = 4
            elif nl > 0 and daily_target > 0 and pnl >= daily_target:
                do_close = 2
                reason = 14
            elif (stage == 0 and pause_frac > 0 and pauses < max_pauses
                  and pnl <= -pause_frac * limit * (pauses + 1)):
                do_close = 5   # loss pause
                reason = 10
            elif nl > 0:
                if armed and peak - pnl >= giveback:
                    do_close = 3
                    reason = 5
                    gbx += 1
                elif pnl - baseline <= -stop:
                    do_stop = True
                elif cycle_target > 0 and pnl - cycle_start_pnl >= cycle_target:
                    do_close = 6
                    reason = 15
                elif cycle_arm > 0 and cycle_peak >= cycle_arm and cycle_peak - (pnl - cycle_start_pnl) >= cycle_giveback:
                    do_close = 6
                    reason = 16
            elif (stage == 0 and t >= next_entry and t < last_entry
                  and (gate == 0 or allow[g] == 1 or (cycle == 0 and gate < 3))):
                do_enter = True
            if do_stop:
                if adjusted:
                    do_close = 4
                    reason = 6
                else:
                    reason = 7
                    nlos = 0
                    nwin = 0
                    lo = -1
                    wi = -1
                    for o in range(2):
                        q = opt[lk[o], o, g]
                        lp = (le[o] - q - slip) * qty - lc[o] - buy_t[q + slip]
                        if lp < 0:
                            nlos += 1
                            lo = o
                        elif lp > 0:
                            nwin += 1
                            wi = o
                    done = False
                    if nlos == 1 and nwin == 1:
                        rem = opt[lk[wi], wi, g] / le[wi]
                        reason = 8
                        if rem + 1e-12 >= minprem and gate >= 2 and allow[g] == 0:
                            reason = 13   # trending: no replacement, close the straddle
                        elif rem + 1e-12 >= minprem:
                            reason = 9
                            s = spot[g]
                            if s > slip:
                                strike = ((s + stepp // 2) // stepp) * step
                                j = (strike - smin) // step
                                k = -1
                                if strike >= smin and j < span:
                                    k = kmap[j]
                                if k >= 0 and opt[k, lo, g] > slip:
                                    q = opt[lk[lo], lo, g]
                                    fill = q + slip
                                    fee = buy_t[fill]
                                    realized += (le[lo] - fill) * qty - fee
                                    fees += fee
                                    if nlog < L.shape[0]:
                                        L[nlog, 0] = t; L[nlog, 1] = 1; L[nlog, 2] = lo; L[nlog, 3] = strikes[lk[lo]]; L[nlog, 4] = fill
                                        L[nlog, 5] = q; L[nlog, 6] = fee; L[nlog, 7] = 2; L[nlog, 8] = cycle; L[nlog, 9] = (le[lo] - fill) * qty
                                    nlog += 1
                                    e = opt[k, lo, g] - slip
                                    fee = sell_t[e]
                                    realized -= fee
                                    fees += fee
                                    if nlog < L.shape[0]:
                                        L[nlog, 0] = t; L[nlog, 1] = 0; L[nlog, 2] = lo; L[nlog, 3] = strikes[k]; L[nlog, 4] = e
                                        L[nlog, 5] = opt[k, lo, g]; L[nlog, 6] = fee; L[nlog, 7] = 1; L[nlog, 8] = cycle; L[nlog, 9] = 0
                                    nlog += 1
                                    first = 1 - lo
                                    lk[lo] = k
                                    le[lo] = e
                                    lc[lo] = fee
                                    adjusted = True
                                    adjustments += 1
                                    b = realized
                                    for o in range(2):
                                        qq = opt[lk[o], o, g]
                                        b += (le[o] - qq - slip) * qty - buy_t[qq + slip]
                                    baseline = b
                                    done = True
                    if not done:
                        do_close = 4
            if do_close > 0:
                for oo in range(2):
                    o = first if oo == 0 else 1 - first
                    if act[o]:
                        q = opt[lk[o], o, g]
                        fill = q + slip
                        fee = buy_t[fill]
                        realized += (le[o] - fill) * qty - fee
                        fees += fee
                        if nlog < L.shape[0]:
                            L[nlog, 0] = t; L[nlog, 1] = 1; L[nlog, 2] = o; L[nlog, 3] = strikes[lk[o]]; L[nlog, 4] = fill
                            L[nlog, 5] = q; L[nlog, 6] = fee; L[nlog, 7] = reason; L[nlog, 8] = cycle; L[nlog, 9] = (le[o] - fill) * qty
                        nlog += 1
                        act[o] = False
                if do_close == 1:
                    halted = True
                    dayloss = 1
                elif do_close == 2:
                    halted = True
                elif do_close == 5:
                    pauses += 1
                    if t + pause_secs > next_entry:
                        next_entry = t + pause_secs
                else:
                    next_entry = t + cool
                    if do_close == 3:
                        peak = realized
                        armed = False
                        if giveback_wait >= 0:
                            next_entry = t + giveback_wait
                    if do_close == 4:            # stop-out (SECOND_STOP / FIRST_STOP_*)
                        if stop_window > 0:
                            if previous_stop >= 0 and t - previous_stop <= stop_window:
                                next_entry = max(next_entry, t + stop_wait)
                                previous_stop = -1
                            else:
                                previous_stop = t
                        n_so += 1
                        if reason == 8:
                            n_lp += 1
                        if (max_lp_stops > 0 and n_lp >= max_lp_stops) or (max_stop_outs > 0 and n_so >= max_stop_outs):
                            halted = True
            elif do_enter and not (max_straddles > 0 and cycle >= max_straddles):
                s = spot[g]
                if s > slip:
                    strike = ((s + stepp // 2) // stepp) * step
                    j = (strike - smin) // step
                    k = -1
                    if strike >= smin and j < span:
                        k = kmap[j]
                    ok = k >= 0 and opt[k, 0, g] > slip and opt[k, 1, g] > slip
                    if ok:
                        st = opt[k, 0, g] + opt[k, 1, g]
                        if (max_pct > 0 and st > max_pct * s) or (max_ratio > 0 and first_st >= 0 and st > max_ratio * first_st):
                            ok = False   # premium bloated
                    if ok:
                        if first_st < 0:
                            first_st = st
                        baseline = realized
                        cycle_start_pnl = realized
                        cycle_peak = 0
                        cycle += 1
                        adjusted = False
                        first = 0
                        for o in range(2):
                            e = opt[k, o, g] - slip
                            fee = sell_t[e]
                            realized -= fee
                            fees += fee
                            if nlog < L.shape[0]:
                                L[nlog, 0] = t; L[nlog, 1] = 0; L[nlog, 2] = o; L[nlog, 3] = strikes[k]; L[nlog, 4] = e
                                L[nlog, 5] = opt[k, o, g]; L[nlog, 6] = fee; L[nlog, 7] = 0; L[nlog, 8] = cycle; L[nlog, 9] = 0
                            nlog += 1
                            act[o] = True
                            lk[o] = k
                            le[o] = e
                            lc[o] = fee
            if halted:
                break
        if halted:
            break
    return realized, cycle, adjustments, dayloss, gbx, fees, pauses, nlog, n_lp, n_so


# ------------------------------------------------ strategy='leg_stop' engine
# Exact numba port of LegStopEngine in the original script (verified identical).
# General form: action 0 = keep the other leg; extra options are fixed off here.
OFF = 1e12
@njit(cache=True)
def _mtm(realized, act, lk, le, opt, g, slip, qty, buy_t):
    p = realized
    for o in range(2):
        if act[o]:
            q = opt[lk[o], o, g]
            p += (le[o] - q - slip) * qty - buy_t[q + slip]
    return p


@njit(cache=True)
def run_leg_stop(spot, opt, strikes, step, qty, sell_t, buy_t, P, L):
    start = int(P[0]); end = int(P[1]); last_entry = int(P[2])
    sl = P[3]; limit = P[4]; gb = P[5]; actv = P[6]; leg_pct = P[7]
    action = int(P[8]); mtc = int(P[9]); max_cycles = int(P[10]); cool = int(P[11])
    target = P[12]; max_move = P[13]; max_adj = int(P[14]); slip = int(P[15])
    smin = strikes[0]; nk = strikes.shape[0]
    span = (strikes[nk - 1] - smin) // step + 1
    kmap = np.full(span, -1, np.int64)
    for k in range(nk):
        kmap[(strikes[k] - smin) // step] = k
    stepp = step * 100
    spot_open = -1
    for g in range(spot.shape[0]):
        if spot[g] >= 0:
            spot_open = spot[g]
            break
    realized = 0; baseline = 0; cycles = 0; adj_cycle = 0; legstops = 0; fees = 0
    armed = False; peak = 0; next_entry = start; dayloss = 0; gbx = 0
    act = np.zeros(2, np.bool_); lk = np.zeros(2, np.int64); le = np.zeros(2, np.int64)
    stp = np.zeros(2, np.float64)
    nlog = 0
    close_reason = np.array([0, 3, 4, 12, 5], np.int64)   # by closeall code
    for t in range(start, end + 1):
        g = t - T0
        pnl = _mtm(realized, act, lk, le, opt, g, slip, qty, buy_t)
        if pnl > peak:
            peak = pnl
        if pnl >= actv:
            armed = True
        nl = int(act[0]) + int(act[1])
        closeall = 0  # 1 halt-dayloss 2 halt-eod 3 cooldown 4 cooldown+peakreset
        if nl > 0 and pnl <= -limit:
            closeall = 1
        elif t >= end:
            closeall = 2
        elif nl > 0:
            cp = pnl - baseline
            if armed and peak - pnl >= gb:
                closeall = 4
                gbx += 1
            elif cp <= -sl:
                closeall = 3
            elif cp >= target:
                closeall = 3
            else:
                for o in range(2):
                    if act[o] and opt[lk[o], o, g] >= stp[o]:
                        q = opt[lk[o], o, g]; fill = q + slip; fee = buy_t[fill]
                        realized += (le[o] - fill) * qty - fee; fees += fee
                        if nlog < L.shape[0]:
                            L[nlog, 0] = t; L[nlog, 1] = 1; L[nlog, 2] = o; L[nlog, 3] = strikes[lk[o]]; L[nlog, 4] = fill
                            L[nlog, 5] = q; L[nlog, 6] = fee; L[nlog, 7] = 11; L[nlog, 8] = cycles; L[nlog, 9] = (le[o] - fill) * qty
                        nlog += 1
                        act[o] = False; legstops += 1
                        other = 1 - o
                        if action == 1:
                            closeall = 3
                            break
                        replaced = False
                        if action == 2 and adj_cycle < max_adj:
                            s = spot[g]
                            if s > slip:
                                strike = ((s + stepp // 2) // stepp) * step
                                j = (strike - smin) // step
                                k = -1
                                if strike >= smin and j < span:
                                    k = kmap[j]
                                if k >= 0 and opt[k, o, g] > slip:
                                    e = opt[k, o, g] - slip; fee = sell_t[e]
                                    realized -= fee; fees += fee
                                    if nlog < L.shape[0]:
                                        L[nlog, 0] = t; L[nlog, 1] = 0; L[nlog, 2] = o; L[nlog, 3] = strikes[k]; L[nlog, 4] = e
                                        L[nlog, 5] = opt[k, o, g]; L[nlog, 6] = fee; L[nlog, 7] = 1; L[nlog, 8] = cycles; L[nlog, 9] = 0
                                    nlog += 1
                                    act[o] = True; lk[o] = k; le[o] = e; stp[o] = e * (1.0 + leg_pct)
                                    adj_cycle += 1; replaced = True
                        if not replaced and mtc == 1 and act[other]:
                            if le[other] < stp[other]:
                                stp[other] = le[other]
                if closeall == 0 and not act[0] and not act[1]:
                    next_entry = t + cool
        elif t >= next_entry and cycles < max_cycles and t <= last_entry:
            s = spot[g]
            if s > slip and spot_open > 0 and abs(s / spot_open - 1.0) <= max_move:
                strike = ((s + stepp // 2) // stepp) * step
                j = (strike - smin) // step
                k = -1
                if strike >= smin and j < span:
                    k = kmap[j]
                if k >= 0 and opt[k, 0, g] > slip and opt[k, 1, g] > slip:
                    baseline = realized; cycles += 1; adj_cycle = 0
                    for o in range(2):
                        e = opt[k, o, g] - slip; fee = sell_t[e]
                        realized -= fee; fees += fee
                        if nlog < L.shape[0]:
                            L[nlog, 0] = t; L[nlog, 1] = 0; L[nlog, 2] = o; L[nlog, 3] = strikes[k]; L[nlog, 4] = e
                            L[nlog, 5] = opt[k, o, g]; L[nlog, 6] = fee; L[nlog, 7] = 0; L[nlog, 8] = cycles; L[nlog, 9] = 0
                        nlog += 1
                        act[o] = True; lk[o] = k; le[o] = e; stp[o] = e * (1.0 + leg_pct)
        if closeall > 0:
            for o in range(2):
                if act[o]:
                    q = opt[lk[o], o, g]; fill = q + slip; fee = buy_t[fill]
                    realized += (le[o] - fill) * qty - fee; fees += fee
                    if nlog < L.shape[0]:
                        L[nlog, 0] = t; L[nlog, 1] = 1; L[nlog, 2] = o; L[nlog, 3] = strikes[lk[o]]; L[nlog, 4] = fill
                        L[nlog, 5] = q; L[nlog, 6] = fee; L[nlog, 7] = close_reason[closeall]; L[nlog, 8] = cycles; L[nlog, 9] = (le[o] - fill) * qty
                    nlog += 1
                    act[o] = False
            if closeall <= 2:
                if closeall == 1:
                    dayloss = 1
                break
            next_entry = t + cool
            if closeall == 4:
                peak = realized
                armed = False
    return realized, cycles, legstops, dayloss, fees, gbx, nlog


# --------------------------------------------------- entry filter ("settled market")
# cfg["entry_filter"] = {"apply_to": "reentry" | "reentry_and_replacement" | "all", "rules": [...]}
# A new straddle (and, with *_and_replacement, a leg replacement) is allowed at a second only if every
# rule holds then; entries wait, a blocked replacement closes the straddle (FIRST_STOP_TRENDING).
# Rules (bps = basis points of spot, minutes look back from the current second):
#   {"type": "range", "minutes": W, "max_bps": X}   high-low of spot over the last W min <= X
#   {"type": "move",  "minutes": W, "max_bps": X}   |spot now - spot W min ago| <= X
#   {"type": "seen",  "from_min": A, "to_min": B, "band_bps": X, "min_seconds": K}
#        spot now was within X bps of spot for at least K seconds between A and B minutes ago
#        ("this price has traded before")
GATE_MODES = {'reentry': 1, 'reentry_and_replacement': 2, 'all': 3}


@njit(cache=True)
def _roll_range(x, w):
    """max-min of x over the trailing window [t-w, t] (x <= 0 = no data -> ignored)."""
    n = x.shape[0]
    out = np.zeros(n, np.float64)
    dq_max = np.empty(n, np.int64); dq_min = np.empty(n, np.int64)
    h1 = t1 = h2 = t2 = 0
    for t in range(n):
        if x[t] > 0:
            while t1 > h1 and x[dq_max[t1 - 1]] <= x[t]:
                t1 -= 1
            dq_max[t1] = t; t1 += 1
            while t2 > h2 and x[dq_min[t2 - 1]] >= x[t]:
                t2 -= 1
            dq_min[t2] = t; t2 += 1
        while t1 > h1 and dq_max[h1] < t - w:
            h1 += 1
        while t2 > h2 and dq_min[h2] < t - w:
            h2 += 1
        if t1 > h1 and t2 > h2:
            out[t] = x[dq_max[h1]] - x[dq_min[h2]]
    return out


@njit(cache=True)
def _seen(x, a, b, band_bps, k, out):
    """out[t] &= #seconds in [t-a, t-b] with |x - x[t]| <= band  >= k."""
    n = x.shape[0]
    for t in range(n):
        if out[t] == 0:
            continue
        if x[t] <= 0:
            out[t] = 0
            continue
        band = x[t] * band_bps / 1e4
        c = 0
        for u in range(max(0, t - a), max(0, t - b + 1)):
            if x[u] > 0 and abs(x[u] - x[t]) <= band:
                c += 1
                if c >= k:
                    break
        if c < k:
            out[t] = 0


def settle_mask(spot, rules):
    x = spot.astype(np.float64)
    ok = np.ones(x.shape[0], np.uint8)
    ok[x <= 0] = 0
    for r in rules:
        typ = r['type']
        if typ == 'range':
            rr = _roll_range(x, int(r['minutes'] * 60))
            ok &= (rr <= np.maximum(x, 1) * r['max_bps'] / 1e4).astype(np.uint8)
        elif typ == 'move':
            w = int(r['minutes'] * 60)
            past = np.concatenate([np.full(w, -1.0), x[:-w]]) if w > 0 else x
            first = x[np.argmax(x > 0)] if (x > 0).any() else 0
            past = np.where(past > 0, past, first)
            ok &= (np.abs(x - past) <= np.maximum(x, 1) * r['max_bps'] / 1e4).astype(np.uint8)
        elif typ == 'seen':
            _seen(x, int(r['from_min'] * 60), int(r['to_min'] * 60), float(r['band_bps']), int(r['min_seconds']), ok)
        else:
            raise ValueError(f'unknown entry_filter rule {typ!r}')
    return ok


REASONS = ['STRADDLE_ENTRY', 'ATM_REPLACEMENT', 'ADJUST_LOSING_LEG', 'DAY_LOSS_LIMIT', 'END_OF_DAY', 'PROFIT_GIVEBACK',
           'SECOND_STOP', 'FIRST_STOP_NO_PROFITABLE_LEG', 'FIRST_STOP_LOW_PREMIUM', 'FIRST_STOP_NO_ATM_QUOTE',
           'LOSS_PAUSE', 'LEG_STOP', 'STRADDLE_STOP', 'FIRST_STOP_TRENDING', 'DAY_PROFIT_TARGET', 'CYCLE_PROFIT_TARGET',
           'CYCLE_PROFIT_GIVEBACK']
MAX_ORDERS = 5000   # per session; far above anything seen (about 30)


def management_args(cfg):
    """Validate optional Strategy 0 settings and convert them to engine units."""
    def units(key, convert, default=0, allow_zero=False):
        value = cfg.get(key)
        if value is None:
            return default
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
            raise ValueError(f'{key} must be finite and nonnegative')
        amount = convert(value)
        if not allow_zero and amount <= 0:
            raise ValueError(f'{key} must be positive')
        return amount

    last_entry = bt.seconds(cfg.get('last_entry_time') or cfg['exit_time'])
    if not bt.seconds(cfg['entry_time']) < last_entry <= bt.seconds(cfg['exit_time']):
        raise ValueError('last_entry_time must be after entry_time and at or before exit_time')
    daily_target = units('daily_profit_target', bt.paise)
    cycle_target = units('cycle_profit_target', bt.paise)
    def minutes_to_seconds(value):
        return round(value * 60)
    stop_window = units('stop_cluster_window_minutes', minutes_to_seconds)
    stop_wait = units('stop_cluster_wait_minutes', minutes_to_seconds)
    if bool(stop_window) != bool(stop_wait):
        raise ValueError('Set both stop_cluster_window_minutes and stop_cluster_wait_minutes')
    giveback_wait = units('giveback_cooldown_minutes', minutes_to_seconds, default=-1, allow_zero=True)
    cycle_arm = units('cycle_giveback_activation', bt.paise)
    cycle_giveback = units('cycle_profit_giveback', bt.paise)
    if bool(cycle_arm) != bool(cycle_giveback):
        raise ValueError('Set both cycle_giveback_activation and cycle_profit_giveback')
    return last_entry, daily_target, cycle_target, stop_window, stop_wait, giveback_wait, cycle_arm, cycle_giveback


class Runner:
    """Holds sessions + charge tables; run(profiles) -> daily rows like the original."""

    def __init__(self, sessions, base_config):
        self.sessions = sessions
        self.max_price = max([int(s['opt'].max()) for s in sessions] + [0])
        self._tabs = {}
        self._log = np.zeros((MAX_ORDERS, 10), np.int64)   # order log buffer, reused per session
        self.base = base_config
        self._allow_all = np.ones(1, np.uint8)
        self._masks = {}

    def tabs(self, cfg):
        slip = bt.paise(cfg['slippage_points_per_order'])
        key = (json.dumps(cfg.get('transaction_costs'), sort_keys=True), cfg['fee_per_leg_order'],
               cfg['quantity']['NIFTY'], cfg['quantity']['SENSEX'], slip)
        if key not in self._tabs:
            self._tabs[key] = charge_tables(cfg, self.max_price + slip + 1)
        return self._tabs[key]

    def gate_args(self, s, cfg):
        """(allow mask, gate mode) for cfg['entry_filter']; mask cached per session and filter."""
        f = cfg.get('entry_filter')
        if not f or not f.get('rules'):
            return self._allow_all, 0
        key = (s['date'], s['index'], json.dumps(f['rules'], sort_keys=True))
        if key not in self._masks:
            self._masks[key] = settle_mask(s['spot'], f['rules'])
        return self._masks[key], GATE_MODES[f.get('apply_to', 'reentry_and_replacement')]

    def run_one(self, s, cfg, trades=None):
        """One session. If `trades` is a list, the session's orders are appended to it
        (same rows as the original script's trades.csv, minus token/symbol/quote time)."""
        if cfg['max_quote_age_seconds'] is not None:
            raise ValueError('fast engine supports max_quote_age_seconds = null only')
        dte = s['dte']
        start, end = bt.seconds(cfg['entry_time']), bt.seconds(cfg['exit_time'])
        if start < T0 or end > T1:
            raise ValueError('entry/exit must lie within 09:15:00-15:29:59 for the fast engine')
        i = 0 if s['index'] == 'NIFTY' else 1
        tabs = self.tabs(cfg)
        L = self._log
        if cfg.get('strategy', 'adjust') == 'leg_stop':
            last_entry = bt.seconds(cfg.get('last_entry_time', cfg['exit_time']))
            P = np.array([start, end, last_entry, bt.paise(cfg['stop_loss_by_dte'][str(dte)]),
                          bt.paise(cfg['day_loss_limit']), bt.paise(cfg['profit_giveback']),
                          bt.paise(cfg['profit_giveback_activation']), float(cfg['leg_stop_pct']), 0, 0,
                          cfg['max_straddles'], int(round(cfg['cooldown_minutes'] * 60)), OFF, OFF, 0,
                          bt.paise(cfg['slippage_points_per_order'])], np.float64)
            net, cycles, adj, dayloss, fees, gbx, nlog = (int(x) for x in run_leg_stop(
                s['spot'], s['opt'], s['strikes'], s['step'], cfg['quantity'][s['index']], tabs[i, 0], tabs[i, 1], P, L))
            npause = n_lp = n_so = 0
        else:
            r = run_session(s['spot'], s['opt'], s['strikes'], s['step'], cfg['quantity'][s['index']],
                        tabs[i, 0], tabs[i, 1], start, end, bt.paise(cfg['stop_loss_by_dte'][str(dte)]),
                        bt.paise(cfg['slippage_points_per_order']), bt.paise(cfg['day_loss_limit']),
                        bt.paise(cfg['profit_giveback']), bt.paise(cfg['profit_giveback_activation']),
                        float(cfg['minimum_premium_remaining']), int(round(cfg['cooldown_minutes'] * 60)), L,
                        -1.0 if cfg.get('loss_pause_fraction') is None else float(cfg['loss_pause_fraction']),
                        int(round((cfg.get('loss_pause_minutes') or 0) * 60)), int(cfg.get('max_loss_pauses', 1)),
                        -1.0 if cfg.get('max_straddle_ratio') is None else float(cfg['max_straddle_ratio']),
                        -1.0 if cfg.get('max_straddle_pct_of_spot') is None else float(cfg['max_straddle_pct_of_spot']),
                        int(cfg.get('max_straddles') or 0),
                        int(cfg.get('max_low_premium_stops') or 0), int(cfg.get('max_stop_outs') or 0),
                        *self.gate_args(s, cfg), management_args(cfg))
            net, cycles, adj, dayloss, gbx, fees, npause, nlog, n_lp, n_so = (int(x) for x in r)
        if nlog > MAX_ORDERS:
            raise RuntimeError(f"{s['date']} {s['index']}: more than {MAX_ORDERS} orders; raise MAX_ORDERS")
        if trades is not None:
            qty = cfg['quantity'][s['index']]
            for row in L[:nlog]:
                t, act, o, strike, fill, mark, fee, reason, cyc, gross = (int(x) for x in row)
                trades.append(dict(date=s['date'], index=s['index'], dte=dte, time=f"{s['date']}T{bt.clock(t)}+05:30",
                                   cycle=cyc, action='BUY' if act else 'SELL', reason=REASONS[reason],
                                   option_type='PE' if o else 'CE', strike=strike, expiry=s['expiry'], quantity=qty,
                                   price=bt.rupees(fill), mark_price=bt.rupees(mark), fee=bt.rupees(fee),
                                   leg_gross_pnl=bt.rupees(gross), realized_pnl_change=bt.rupees(gross - fee if act else -fee)))
        status = 'OK' if s['last'] >= end else 'INCOMPLETE_SOURCE'
        if not cycles:
            status = 'NO_TRADES'
        return dict(source_file=s['source_file'], date=s['date'], index=s['index'], expiry=s['expiry'],
                    dte=dte, status=status, quantity=cfg['quantity'][s['index']],
                    net_pnl=bt.rupees(net), cycles=cycles, adjustments=adj,
                    gross_pnl=bt.rupees(net + fees), transaction_charges=bt.rupees(fees),
                    day_loss_hit=bool(dayloss), giveback_exits=gbx, loss_pauses=npause,
                    low_premium_stops=n_lp, stop_outs=n_so)

    def run(self, profiles, sessions=None, trades=None):
        """profiles: {dte: config}. Sessions whose DTE has no profile are skipped.
        Pass a list as `trades` to collect the tradebook."""
        return [self.run_one(s, profiles[s['dte']], trades) for s in (sessions or self.sessions)
                if s['dte'] in profiles]


def summarize(rows):
    included = [r for r in rows if r['status'] == 'OK']
    return dict(overall=bt.statistics(included),
                by_index={i: bt.statistics([r for r in included if r['index'] == i])
                          for i in sorted({r['index'] for r in included})},
                by_dte={str(d): bt.statistics([r for r in included if r['dte'] == d]) for d in (0, 1, 2)},
                excluded_sessions=[r for r in rows if r['status'] != 'OK'])


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--configs', type=Path, nargs='+', metavar='DTE_JSON',
                    help='One single-DTE config per requested DTE')
    ap.add_argument('--data-dir', type=Path, action='append',
                    help='Data folder; repeat for several. Default: the configs\' data_dir (path or list)')
    ap.add_argument('--cache-dir', type=Path, default=Path(CACHE_DIR))
    ap.add_argument('--output', type=Path, default=Path(OUTPUT_DIR))
    ap.add_argument('--from-date', default=FROM_DATE)
    ap.add_argument('--to-date', default=TO_DATE)
    ap.add_argument('--index', choices=['NIFTY', 'SENSEX'], default=INDEX)
    ap.add_argument('--file', default='*.sqlite3', help='filename glob')
    ap.add_argument('--all-dtes-per-day', dest='one_dte_per_day', action='store_false', default=ONE_DTE_PER_DAY)
    args = ap.parse_args(argv)
    if not args.data_dir and DATA_DIRS:
        args.data_dir = [Path(d) for d in DATA_DIRS]
    paths = args.configs or [Path(p) for p in CONFIGS]
    profiles = {}
    for p in paths:
        cfg = json.loads(Path(p).read_text(encoding='utf-8-sig'))
        bt.validate(cfg)
        if len(cfg['enabled_dtes']) != 1:
            raise SystemExit(f'{p}: needs exactly one enabled DTE')
        if cfg['enabled_dtes'][0] in profiles:
            raise SystemExit(f'{p}: duplicate enabled DTE')
        if cfg.get('strategy', 'adjust') != 'leg_stop':
            management_args(cfg)
        profiles[cfg['enabled_dtes'][0]] = cfg
    base = next(iter(profiles.values()))
    if any(c['calendar'] != base['calendar'] or c['strike_step'] != base['strike_step'] for c in profiles.values()):
        raise SystemExit('Configs must share the same calendar and strike steps')
    if not args.data_dir:
        dirs = {json.dumps(c['data_dir']) for c in profiles.values()}
        if len(dirs) != 1:
            raise SystemExit('The configs use different data_dir values; pass --data-dir')
        value = base['data_dir']
        args.data_dir = [Path(v) for v in ([value] if isinstance(value, str) else value)]
    print(f'numba: {"on" if HAVE_NUMBA else "OFF (pure Python, slow)"}', flush=True)
    sessions = load_sessions(args.data_dir, args.cache_dir, base['calendar'],
                             base['strike_step'], args.file, selected_dtes=profiles)
    if args.one_dte_per_day:      # decided before index/date filters, like the original
        sessions = one_dte_per_day(sessions)
    sessions = [s for s in sessions if not (args.index and s['index'] != args.index or
                                            args.from_date and s['date'] < args.from_date or
                                            args.to_date and s['date'] > args.to_date)]
    trades = []
    rows = Runner(sessions, base).run(profiles, trades=trades)
    args.output.mkdir(parents=True, exist_ok=True)
    bt.write_csv(args.output / 'daily_results.csv', rows)
    bt.write_csv(args.output / 'trades.csv', trades)          # tradebook: every order
    bt.write_csv(args.output / 'daily_totals.csv', bt.daily_totals(rows))
    summary = summarize(rows)
    summary.update(mode='fast_combined', one_dte_per_day=args.one_dte_per_day,
                   data_dirs=[str(d) for d in args.data_dir], config_paths=[str(p) for p in paths])
    (args.output / 'summary.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
    for d in (0, 1, 2):
        s = summary['by_dte'][str(d)]
        print(f"DTE {d}: {s['sessions']} sessions, net P&L={s['total_pnl']:,.2f}")
    print(json.dumps(summary['overall'], indent=2))
    print(f'Tradebook: {len(trades)} orders -> {(args.output / "trades.csv").resolve()}')
    print(f'Results: {args.output.resolve()}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
