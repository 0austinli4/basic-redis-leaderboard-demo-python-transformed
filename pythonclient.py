import json
from mdlin import SyncAppRequest, InitCustom
from server.core.companies_redis_sync import CompaniesRanks, RedisClient, RankSortKeys
from django.conf import settings
from redis import Redis, RedisError, ConnectionError
import os
import sys
import django

if __name__ == "__main__":
    if not settings.configured:
        settings.configure()
        sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "server.configuration.settings")
        django.setup()
    # Initialize custom configurations or settings
    InitCustom("0", "multi_paxos")

    # Test RedisClient and CompaniesRanks
    redis_client = RedisClient()

    # Test setting initial data from the companies_data.json file
    print("Running set_init_data...")
    redis_client.set_init_data()

    # # Test updating a company's market capitalization
    # print("Updating market capitalization for 'AAPL'...")
    companies_ranks = CompaniesRanks()
    print("Updating market capitalization for multiple companies...")
    updates = {
        "AMZN": 1600000000,
        "MSFT": 1700000000,
        "TSLA": 850000000,
    }
    for symbol, new_cap in updates.items():
        companies_ranks.update_company_market_capitalization(new_cap, symbol)

    # Test getting ranks by sort key (e.g., TOP10)
    print("Fetching TOP10 ranks...")
    top_ranks = companies_ranks.get_ranks_by_sort_key(RankSortKeys.TOP10.value)
    print("Top Ranks:", top_ranks)

    # Test getting ranks by specific symbols
    ranks = companies_ranks.get_ranks_by_symbols(["AAPL", "GOOG"])
    print("Ranks by Symbols:", ranks)

    print("Fetching ranks for symbols AAPL and GOOG...")
    symbol_ranks = companies_ranks.get_ranks_by_symbols(["AAPL", "GOOG"])
    print("Ranks by Symbols:", symbol_ranks)
