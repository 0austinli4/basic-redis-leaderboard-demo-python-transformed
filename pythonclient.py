import json
from mdlin import SyncAppRequest, InitCustom
from server.core.companies_redis_sync import CompaniesRanks, RedisClient, RankSortKeys
from django.conf import settings
from redis import Redis, RedisError, ConnectionError
import os
import sys
import django


def set_init_data():
    with open(
        "/users/akalaba/basic-redis-leaderboard-demo-python-transformed/server/core/companies_data.json",
        "r",
    ) as init_data:
        companies = json.load(init_data)
        try:
            for company in companies:
                symbol = add_prefix_to_symbol("redis", company.get("symbol").lower())
                SyncAppRequest(
                    "ZADD",
                    "leaderboard",
                    {symbol: company.get("marketCap")},
                )
                SyncAppRequest("HSET", symbol, "company", company.get("company"))
                SyncAppRequest("HSET", symbol, "country", company.get("country"))
        except ConnectionError:
            print("Connection error")


def update_company_market_capitalization(amount, symbol):
    SyncAppRequest(
        "ZINCRBY",
        "leaderboard",
        amount,
        add_prefix_to_symbol("redis", symbol),
    )
    return None


def get_ranks_by_sort_key(key):
    sort_key = RankSortKeys(key)
    if sort_key is RankSortKeys.ALL:
        return get_zrange(0, -1)
    elif sort_key is RankSortKeys.TOP10:
        return get_zrange(0, 9)
    elif sort_key is RankSortKeys.BOTTOM10:
        return get_zrange(0, 9, False)


def get_ranks_by_symbols(symbols):
    companies_capitalization = []
    for symbol in symbols:
        companies_capitalization.append(
            SyncAppRequest(
                "ZSCORE",
                "leaderboard",
                add_prefix_to_symbol("redis", symbol),
            )
        )
    companies = []
    for index, market_capitalization in enumerate(companies_capitalization):
        companies.append(
            [
                add_prefix_to_symbol("redis", symbols[index]),
                market_capitalization,
            ]
        )
    return get_result(companies)


def get_zrange(start_index, stop_index, desc=True):
    query_args = {
        "name": "leaderboard",
        "start": start_index,
        "end": stop_index,
        "withscores": True,
        "score_cast_func": str,
    }
    if desc:
        companies = SyncAppRequest("ZREVRANGE", "leaderboard", start_index, stop_index)
    else:
        companies = SyncAppRequest("ZRANGE", "leaderboard", start_index, stop_index)
    return get_result(companies, start_index, desc)


def get_result(companies, start_index=0, desc=True):
    start_rank = int(start_index) + 1 if desc else len(companies) - start_index
    increase_factor = 1 if desc else -1
    results = []

    for i in range(0, len(companies), 2):
        member = companies[i]
        # Ensure there's a score following the member
        score = companies[i + 1] if i + 1 < len(companies) else None

        company_info = SyncAppRequest("HGETALL", member)
        if company_info and isinstance(company_info, dict):
            results.append(
                {
                    "company": company_info.get("company", ""),
                    "country": company_info.get("country", ""),
                    "marketCap": score,  # Using the score from the paired element
                    "rank": start_rank,
                    "symbol": remove_prefix_to_symbol("redis", member),
                }
            )
        start_rank += increase_factor
    return json.dumps(results)


def add_prefix_to_symbol(prefix, symbol):
    return f"{prefix}:{symbol}"


def remove_prefix_to_symbol(prefix, symbol):
    return symbol.replace(f"{prefix}:", "")


if __name__ == "__main__":
    # Initialize custom configurations or settings
    InitCustom("0", "multi_paxos")

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
    ranks = get_ranks_by_symbols(["AAPL", "GOOG"])
    print("Ranks by Symbols:", ranks)
