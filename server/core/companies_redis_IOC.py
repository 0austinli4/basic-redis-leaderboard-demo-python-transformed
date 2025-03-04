from collections import deque
import os
import json
import enum
import logging
import sys
import django
from django.conf import settings
from redis import Redis, RedisError, ConnectionError
from mdlin import AppRequest, AppResponse

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
        pending_awaits = {*()}
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
                    future_0 = AppRequest(
                        "ZADD",
                        settings.REDIS_LEADERBOARD,
                        symbol,
                        str(company.get("marketCap")),
                    )
                    pending_awaits.add(future_0)
                    future_1 = AppRequest(
                        "HSET", symbol, "company", company.get("company")
                    )
                    pending_awaits.add(future_1)
                    future_2 = AppRequest(
                        "HSET", symbol, "country", company.get("country")
                    )
                    pending_awaits.add(future_2)
            except ConnectionError:
                if settings.REDIS_URL:
                    error_message = (
                        f"Redis connection time out to {settings.REDIS_URL}."
                    )
                else:
                    error_message = f"Redis connection time out to {settings.REDIS_HOST}:{settings.REDIS_PORT}."
                logger.error(error_message)
        for future in pending_awaits:
            AppResponse(future)
        return None

    @staticmethod
    def add_prefix_to_symbol(prefix, symbol):
        return f"{prefix}:{symbol}"

    @staticmethod
    def remove_prefix_to_symbol(prefix, symbol):
        return symbol.replace(f"{prefix}:", "")


class CompaniesRanks(RedisClient):
    def update_company_market_capitalization(self, amount, symbol):
        pending_awaits = {*()}
        future_0 = AppRequest(
            "ZINCRBY",
            settings.REDIS_LEADERBOARD,
            amount,
            self.add_prefix_to_symbol(settings.REDIS_PREFIX, symbol),
        )
        pending_awaits.add(future_0)
        for future in pending_awaits:
            AppResponse(future)
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
        dep_vars_queue = deque()
        pending_awaits = {*()}
        companies_capitalization = []

        for symbol in symbols:
            future_0 = AppRequest(
                "ZSCORE",
                settings.REDIS_LEADERBOARD,
                self.add_prefix_to_symbol(settings.REDIS_PREFIX, symbol),
            )
            dep_vars_queue.append(future_0)
        for symbol in symbols:
            zscore = AppResponse(dep_vars_queue.popleft())
            companies_capitalization.append(zscore)
        companies = []
        for index, market_capitalization in enumerate(companies_capitalization):
            companies.extend(
                [
                    self.add_prefix_to_symbol(settings.REDIS_PREFIX, symbols[index]),
                    market_capitalization,
                ]
            )
        for future in pending_awaits:
            AppResponse(future)
        return self.get_result(companies)

    def get_zrange(self, start_index, stop_index, desc=True):
        pending_awaits = []
        query_args = {
            "name": settings.REDIS_LEADERBOARD,
            "start": start_index,
            "end": stop_index,
            "withscores": True,
            "score_cast_func": str,
        }
        if desc:
            future_0 = AppRequest(
                "ZREVRANGE", settings.REDIS_LEADERBOARD, start_index, stop_index
            )
            pending_awaits.append(future_0)
        else:
            future_1 = AppRequest(
                "ZRANGE", settings.REDIS_LEADERBOARD, start_index, stop_index
            )
            pending_awaits.append(future_1)
        companies = AppResponse(pending_awaits.pop())
        res = self.get_result(companies, start_index, desc)
        return res

    def get_result(self, companies, start_index=0, desc=True):
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

            if not company_info or not isinstance(company_info, dict):
                company_info = {"company": "temp", "country": "temp"}

            results.append(
                {
                    "company": company_info["company"],
                    "country": company_info["country"],
                    "marketCap": dep_vars_queue.popleft(),
                    "rank": dep_vars_queue.popleft(),
                    "symbol": self.remove_prefix_to_symbol(
                        settings.REDIS_PREFIX, dep_vars_queue.popleft()
                    ),
                }
            )
        for future in pending_awaits:
            AppResponse(future)
        return json.dumps(results)
