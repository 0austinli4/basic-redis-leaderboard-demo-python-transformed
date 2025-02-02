import asyncio
from .companies_redis_IOC import CompaniesRanks
import math
import numpy as np
import json
import random
import time
from mdlin import AppRequest, AppResponse


def run_workload():
    num_minutes = 1
    api = [
        "update_company_market_capitalization",
        "get_ranks_by_sort_key",
        "get_ranks_by_symbols",
        "get_zrange",
    ]
    t_end = time.time() + 60 * num_minutes
    selector = 0

    while time.time() < t_end:
        app_request_type = random.randint(1, 100)
        before = time.time_ns()
        if app_request_type <= 10:
            selector = 0
            amount = random.uniform(1e6, 1e9)
            symbol = random.choice(["AAPL", "GOOG", "AMZN", "MSFT"])
            CompaniesRanks().update_company_market_capitalization(amount, symbol)
        elif app_request_type <= 40:
            selector = 1
            sort_key = random.choice(["market_cap", "revenue", "profit"])
            CompaniesRanks().get_ranks_by_sort_key(sort_key)
        elif app_request_type <= 70:
            selector = 2
            symbols = random.sample(["AAPL", "GOOG", "AMZN", "MSFT", "TSLA"], 2)
            CompaniesRanks().get_ranks_by_symbols(symbols)
        else:
            selector = 3
            start_index = random.randint(0, 10)
            end_index = start_index + random.randint(1, 5)
            CompaniesRanks().get_zrange(start_index, end_index)

        after = time.time_ns()
        lat = after - before
        optype = api[selector]
        print(f"app,{lat}")
        print(f"{optype},{lat}")
