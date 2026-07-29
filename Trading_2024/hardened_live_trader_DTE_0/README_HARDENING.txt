HARDENED V3OPT LIVE TRADER
==========================

Files
-----
1. live_short_straddle_trader.py   Updated execution engine.
2. live_trader_config.properties   Same strategy values; bounded execution settings added.
3. OptionTradeUtils_env.py         Supplied helper, unchanged.

The .env file is deliberately NOT included because it contains live credentials.
Keep your existing .env beside these files, rotate exposed credentials, and keep
PAPER_TRADING=1 until broker-side verification logs have been reviewed.

Execution guarantees added
--------------------------
- A live entry succeeds only after both CE and PE orders are COMPLETE for the
  intended quantity and broker positions show exactly the intended short quantity.
- place_order is submitted once per logical order. If its response is lost or
  ambiguous, the order book is reconciled before any resubmission.
- At most four complete entry execution cycles are allowed.
- Every failed cycle cancels pending orders, flattens residual positions, and
  performs two position checks before another cycle is allowed.
- After all entry cycles fail, the book must be verified flat and the strategy
  defers to the next configured attempt/re-entry delay.
- API retries and process restarts are finite. A residual-position uncertainty
  raises FatalExecutionError and stops automation for manual inspection.

Validation performed
--------------------
- Python py_compile passed.
- Static scan found no unbounded while True loop.
- Mock broker tests passed for:
  1. normal two-leg success;
  2. lost place_order response recovered without duplicate submission;
  3. CE rejection after PE fill, cleanup to flat, then successful retry;
  4. four failed cycles ending with a verified-flat broker book.
- Strategy parameters in the properties file were compared key-by-key and were
  not changed; only execution/retry controls differ.
