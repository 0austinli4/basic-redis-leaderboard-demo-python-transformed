import asyncio
from .companies_redis_IOC import CompaniesRanks, RedisClient
import math
import numpy as np
import json
import random
import time
from mdlin import AppRequest, AppResponse


def run_workload(exp_length):
    num_seconds = int(exp_length)
    api = [
        "update_company_market_capitalization",
        "get_ranks_by_sort_key",
        "get_ranks_by_symbols",
        "get_zrange",
    ]
    t_end = time.time() + num_seconds
    selector = 0

    redis_client = RedisClient()
    company = CompaniesRanks()
    redis_client.set_init_data()

    while time.time() < t_end:
        app_request_type = random.randint(1, 100)
        before = time.time_ns()
        if app_request_type <= 10:
            selector = 0
            amount = random.uniform(1e6, 1e9)
            symbol = random.choice(["aapl", "goog", "amzn", "msft"])
            company.update_company_market_capitalization(amount, symbol)
        elif app_request_type <= 40:
            selector = 1
            # "all",
            sort_key = random.choice(["top10", "bottom10"])
            company.get_ranks_by_sort_key(sort_key)
        elif app_request_type <= 70:
            selector = 2
            symbols = random.sample(["aapl", "goog", "amzn", "msft", "tsla"], 2)
            company.get_ranks_by_symbols(symbols)
        else:
            selector = 3
            start_index = random.randint(0, 10)
            end_index = start_index + random.randint(1, 5)
            company.get_zrange(start_index, end_index)

        after = time.time_ns()
        lat = after - before
        optype = api[selector]
        print(f"app,{lat}")
        print(f"{optype},{lat}")
