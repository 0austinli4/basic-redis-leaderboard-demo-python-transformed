import os
import json
import enum
import logging
import sys
import django
from django.conf import settings
from redis import Redis, RedisError, ConnectionError
from mdlin import SyncAppRequest


logger = logging.getLogger(__name__)


class RankSortKeys(enum.Enum):
    ALL = "all"
    TOP10 = "top10"
    BOTTOM10 = "bottom10"


class RedisClient:
    def __init__(self):
        if not settings.configured:
            settings.configure()
            sys.path.append(
                os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
            )
            os.environ.setdefault(
                "DJANGO_SETTINGS_MODULE", "server.configuration.settings"
            )
            django.setup()

    def set_init_data(self):
        with open(
            "/users/akalaba/basic-redis-leaderboard-demo-python-transformed/server/core/companies_data.json",
            "r",
        ) as init_data:
            companies = json.load(init_data)
            try:
                for company in companies:
                    symbol = self.add_prefix_to_symbol(
                        settings.REDIS_PREFIX, company.get("symbol").lower()
                    )
                    SyncAppRequest(
                        "ZADD",
                        settings.REDIS_LEADERBOARD,
                        symbol,
                        str(company.get("marketCap")),
                    )
                    SyncAppRequest("HSET", symbol, "company", company.get("company"))
                    SyncAppRequest("HSET", symbol, "country", company.get("country"))
            except ConnectionError:
                if settings.REDIS_URL:
                    error_message = (
                        f"Redis connection time out to {settings.REDIS_URL}."
                    )
                else:
                    error_message = f"Redis connection time out to {settings.REDIS_HOST}:{settings.REDIS_PORT}."
                logger.error(error_message)
                return
        return None

    @staticmethod
    def add_prefix_to_symbol(prefix, symbol):
        return f"{prefix}:{symbol}"

    @staticmethod
    def remove_prefix_to_symbol(prefix, symbol):
        return symbol.replace(f"{prefix}:", "")


class CompaniesRanks(RedisClient):
    def update_company_market_capitalization(self, amount, symbol):
        SyncAppRequest(
            "ZINCRBY",
            settings.REDIS_LEADERBOARD,
            amount,
            self.add_prefix_to_symbol(settings.REDIS_PREFIX, symbol),
        )
        return None

    def get_ranks_by_sort_key(self, key):
        sort_key = RankSortKeys(key)
        if sort_key is RankSortKeys.ALL:
            return self.get_zrange(0, -1)
        elif sort_key is RankSortKeys.TOP10:
            return self.get_zrange(0, 9)
        elif sort_key is RankSortKeys.BOTTOM10:
            return self.get_zrange(0, 9, False)

    def get_ranks_by_symbols(self, symbols):
        companies_capitalization = []
        for symbol in symbols:
            companies_capitalization.append(
                SyncAppRequest(
                    "ZSCORE",
                    settings.REDIS_LEADERBOARD,
                    self.add_prefix_to_symbol(settings.REDIS_PREFIX, symbol),
                )
            )
        companies = []
        for index, market_capitalization in enumerate(companies_capitalization):
            companies.extend(
                [
                    self.add_prefix_to_symbol(settings.REDIS_PREFIX, symbols[index]),
                    market_capitalization,
                ]
            )
        return self.get_result(companies)

    def get_zrange(self, start_index, stop_index, desc=True):
        query_args = {
            "name": settings.REDIS_LEADERBOARD,
            "start": start_index,
            "end": stop_index,
            "withscores": True,
            "score_cast_func": str,
        }
        if desc:
            companies = SyncAppRequest(
                "ZREVRANGE", settings.REDIS_LEADERBOARD, start_index, stop_index
            )
        else:
            companies = SyncAppRequest(
                "ZRANGE", settings.REDIS_LEADERBOARD, start_index, stop_index
            )
        return self.get_result(companies, start_index, desc)

    def get_result(self, companies, start_index=0, desc=True):
        start_rank = int(start_index) + 1 if desc else len(companies) - start_index
        increase_factor = 1 if desc else -1
        results = []

        for i in range(0, len(companies), 2):
            symbol = companies[i]
            # Ensure there's a score following the member
            market_cap = companies[i + 1] if i + 1 < len(companies) else None

            company_info = SyncAppRequest("HGETALL", symbol)
            if company_info and isinstance(company_info, dict):
                results.append(
                    {
                        "company": company_info.get("company", ""),
                        "country": company_info.get("country", ""),
                        "marketCap": market_cap,
                        "rank": start_rank,
                        "symbol": self.remove_prefix_to_symbol(
                            settings.REDIS_PREFIX, symbol
                        ),
                    }
                )
            start_rank += increase_factor
        return json.dumps(results)
