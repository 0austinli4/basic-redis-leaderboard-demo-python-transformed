from collections import deque
import json
from mdlin import AppRequest, AppResponse, InitCustom
from server.core.companies_redis_IOC import RankSortKeys
from django.conf import settings
from redis import Redis, RedisError, ConnectionError
import os
import sys
import django


def set_init_data():
    pending_awaits = {*()}
    with open(
        "/users/akalaba/basic-redis-leaderboard-demo-python-transformed/companies_subset.json",
        "r",
    ) as init_data:
        companies = json.load(init_data)
        all_symbols = []
        try:
            for company in companies:
                symbol = add_prefix_to_symbol("redis", company.get("symbol").lower())
                future_0 = AppRequest(
                    "ZADD",
                    "leaderboard",
                    symbol,
                    str(company.get("marketCap")),
                )
                pending_awaits.add(future_0)
                future_1 = AppRequest("HSET", symbol, "company", company.get("company"))
                pending_awaits.add(future_1)
                future_2 = AppRequest("HSET", symbol, "country", company.get("country"))
                pending_awaits.add(future_2)
                all_symbols.append(symbol)
        except ConnectionError:
            print("Error")
        for future in pending_awaits:
            AppResponse(future)
    return (pending_awaits, None)


def update_company_market_capitalization(amount, symbol):
    pending_awaits = {*()}
    future_0 = AppRequest(
        "ZINCRBY",
        "leaderboard",
        amount,
        add_prefix_to_symbol("redis", symbol),
    )
    pending_awaits.add(future_0)
    for future in pending_awaits:
        AppResponse(future)
    return (pending_awaits, None)


def get_ranks_by_sort_key(key):
    sort_key = RankSortKeys(key)
    if sort_key is RankSortKeys.ALL:
        return get_zrange(0, -1)
    elif sort_key is RankSortKeys.TOP10:
        return get_zrange(0, 9)
    elif sort_key is RankSortKeys.BOTTOM10:
        return get_zrange(0, 9, False)


def get_ranks_by_symbols(symbols):
    dep_vars_queue = deque()
    pending_awaits = {*()}
    companies_capitalization = []

    for symbol in symbols:
        future_0 = AppRequest(
            "ZSCORE",
            "leaderboard",
            add_prefix_to_symbol("redis", symbol),
        )
        dep_vars_queue.append(future_0)
    for symbol in symbols:
        zscore = AppResponse(dep_vars_queue.popleft())
        companies_capitalization.append(zscore)
    companies = []
    for index, market_capitalization in enumerate(companies_capitalization):
        companies.extend(
            [
                add_prefix_to_symbol("redis", symbols[index]),
                market_capitalization,
            ]
        )
    for future in pending_awaits:
        AppResponse(future)
    return get_result(companies)


def get_zrange(start_index, stop_index, desc=True):
    pending_awaits = []
    query_args = {
        "name": "leaderboard",
        "start": start_index,
        "end": stop_index,
        "withscores": True,
        "score_cast_func": str,
    }
    if desc:
        future_0 = AppRequest("ZREVRANGE", "leaderboard", start_index, stop_index)
        pending_awaits.append(future_0)
    else:
        future_1 = AppRequest("ZRANGE", "leaderboard", start_index, stop_index)
        pending_awaits.append(future_1)
    companies = AppResponse(pending_awaits.pop())
    res = get_result(companies, start_index, desc)
    return res


def get_result(companies, start_index=0, desc=True):
    dep_vars_queue = deque()
    pending_awaits = {*()}
    start_rank = int(start_index) + 1 if desc else len(companies) - start_index
    increase_factor = 1 if desc else -1
    results = []

    for i in range(0, len(companies), 2):
        symbol = companies[i]
        market_cap = companies[i + 1] if i + 1 < len(companies) else None

        future_0 = AppRequest("HGETALL", symbol)
        dep_vars_queue.append(future_0)
        dep_vars_queue.append(market_cap)
        dep_vars_queue.append(start_rank)
        dep_vars_queue.append(symbol)
        start_rank += increase_factor

    for i in range(0, len(companies), 2):
        company_info = AppResponse(dep_vars_queue.popleft())

        if company_info and isinstance(company_info, dict):
            results.append(
                {
                    "company": company_info["company"],
                    "country": company_info["country"],
                    "marketCap": dep_vars_queue.popleft(),
                    "rank": dep_vars_queue.popleft(),
                    "symbol": remove_prefix_to_symbol(
                        "redis", dep_vars_queue.popleft()
                    ),
                }
            )
    for future in pending_awaits:
        AppResponse(future)
    return (pending_awaits, json.dumps(results))


def add_prefix_to_symbol(prefix, symbol):
    return f"{prefix}:{symbol}"


def remove_prefix_to_symbol(prefix, symbol):
    return symbol.replace(f"{prefix}:", "")


if __name__ == "__main__":
    # Initialize custom configurations or settings
    InitCustom("0", "mdl")

    # Test RedisClient and CompaniesRanks

    # Test setting initial data from the companies_data.json file
    print("Running set_init_data...")
    set_init_data()

    # # Test updating a company's market capitalization
    # print("Updating market capitalization for 'AAPL'...")
    print("Updating market capitalization for multiple companies...")
    updates = {
        "AMZN": 1600000000,
        "MSFT": 1700000000,
        "TSLA": 850000000,
    }
    for symbol, new_cap in updates.items():
        update_company_market_capitalization(new_cap, symbol)

    # Test getting ranks by sort key (e.g., TOP10)
    print("Fetching TOP10 ranks...")
    top_ranks = get_ranks_by_sort_key(RankSortKeys.TOP10.value)
    print("Top Ranks:", top_ranks)

    # Test getting ranks by specific symbols
    ranks = get_ranks_by_symbols(["aapl", "goog"])
    print("Ranks by Symbols:", ranks)
