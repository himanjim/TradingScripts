import asyncio

from CSPCPFinderUtils import *

fetched_futures = pickle.load(open(FETCHED_FUTURES_FILE_OBJ_LOC, 'rb'))
future_status = fetched_futures['future_status']
futures = fetched_futures['futures']


async def fetch_options(stock_id):
    future_near_mon_symbol = outil.get_future_symbol(stock_id, near_month_last_thurs)

    if stock_id in future_status:
        if future_status[stock_id]['curr_mon_enable']:
            options = get_atm_call_puts(stock_id, current_month_last_thurs)
            fetched_futures[future_near_mon_symbol].update(
                {'atm_option_call': options[0], 'atm_option_put': options[1]})
        else:
            print ('Stock curr month option_pe fetch disabled:', stock_id)

        if future_status[stock_id]['near_mon_enable']:
            options = get_atm_call_puts(stock_id, near_month_last_thurs)
            fetched_futures[future_near_mon_symbol].update(
                {'atm_option_call_near': options[0], 'atm_option_put_near': options[1]})
        else:
            print ('Stock near month option_pe fetch disabled:', stock_id)

        pickle.dump(fetched_futures, open(FETCHED_FUTURES_FILE_OBJ_LOC, 'wb'))


async def run_fetch_options(futures):
    tasks = []
    while loop.is_running():
        for future in futures:
            stock_id = outil.get_stock_id(future[nse_bse.STOCK_ID])
            tasks.append(asyncio.ensure_future(fetch_options(stock_id)))

        await asyncio.gather(*tasks)
        tasks = []
        fetched_futures = pickle.load(open(FETCHED_FUTURES_FILE_OBJ_LOC, 'rb'))
        future_status = fetched_futures['future_status']
        futures = fetched_futures['futures']


loop = asyncio.get_event_loop()
loop.create_task(run_fetch_options(futures))
loop.run_forever()
