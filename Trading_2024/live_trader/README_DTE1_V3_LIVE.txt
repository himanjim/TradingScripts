DTE-1 v3 live trader — updated files
==========================================

Files
-----
1. live_short_straddle_trader.py
2. live_trader_config.properties
3. OptionTradeUtils_env.py
4. .env

Install all four in the same directory. Keep PAPER_TRADING=1 for initial runs.
The strategy state filename is new, so old V3OPT state files are ignored.

Validation command
------------------
python -m py_compile live_short_straddle_trader.py OptionTradeUtils_env.py

Run
---
python -u live_short_straddle_trader.py

Strategy parity
---------------
- Calendar DTE-1 only
- Entry 09:20, hard exit 15:29
- Rs.3,000 capped attempt stop
- 10% target and one-minute target re-entry
- 7/10/13/16/19/22-minute stop/protect re-entry delays
- 1.32x re-entry premium gate
- Rs.20,000 daily net-loss breaker
- Rs.10,000 realised profit trail arm and give-back
- Maximum 10 re-attempts
- Minimum 10 minutes remaining for a fresh re-entry

Operational note
----------------
The live program triggers on observed LTP and books actual/simulated fills. It
cannot and does not reproduce the backtest's independent CE/PE intraminute-low
profit-target assumption.
