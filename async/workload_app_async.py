import numpy as np
import json
import random
import time
import sys
import os

# Add the parent directory to Python path to find iocl module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from iocl.iocl_utils import send_request, await_request
import utils


def create(session_id, clientid, explen):
    api = [
        "update_company_market_capitalization",
        "get_ranks_by_sort_key",
        "get_ranks_by_symbols",
        "get_zrange",
    ]
    t_start = time.time()
    t_end = t_start + int(explen)
    # ramp-up and ramp-down windows in seconds
    rampUp = 10
    rampDown = 10
    # start time marker
    print("#start,0,0")

    # Sample company symbols for workload
    symbols = ["aapl", "goog", "amzn", "msft", "tsla", "fb", "nvda", "nflx"]

    while time.time() < t_end:
        app_request_type = np.random.uniform(0, 100)
        # use nanoseconds for latency only
        before = int(time.time() * 1e9)

        if app_request_type < 10:
            selector = 0
            amount = random.uniform(1e6, 1e9)
            symbol = random.choice(symbols)
            utils.update_company_market_capitalization(session_id, amount, symbol)
        elif app_request_type < 40:
            selector = 1
            sort_key = random.choice(["top10", "bottom10"])
            utils.get_ranks_by_sort_key(session_id, sort_key)
        elif app_request_type < 70:
            selector = 2
            selected_symbols = random.sample(symbols, 2)
            utils.get_ranks_by_symbols(session_id, selected_symbols)
        else:
            selector = 3
            start_index = random.randint(0, 10)
            end_index = start_index + random.randint(1, 5)
            utils.get_zrange(session_id, start_index, end_index)

        after = int(time.time() * 1e9)
        lat = after - before  # latency in nanoseconds
        optime = int((time.time() - t_start) * 1e9)  # time since start in nanoseconds
        optype = api[selector]

        # Only record/print latencies during steady-state (after rampUp and before rampDown)
        now = time.time()
        if now >= (t_start + rampUp) and now < (t_end - rampDown):
            print(f"app,{lat},{optime},{clientid}")
            print(f"{optype},{lat},{optime},{clientid}")

    # end marker exactly compatible with current parser
    elapsed = time.time() - t_start
    end_sec = int(elapsed)
    end_usec = int((elapsed - end_sec) * 1e6)
    print(f"#end,{end_sec},{end_usec},{clientid}")
