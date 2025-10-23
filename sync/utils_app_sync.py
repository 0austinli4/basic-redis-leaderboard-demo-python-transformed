import json
from iocl.iocl_utils import send_request_and_await
import sys


def update_company_market_capitalization(session_id, amount, symbol):
    """Update the market capitalization of a company by incrementing its score in the leaderboard."""
    company_key = f"company:{symbol.lower()}"
    send_request_and_await(
        session_id, "ZINCRBY", "companyLeaderboard", str(amount), company_key
    )
    return None


def get_ranks_by_sort_key(session_id, sort_key):
    """Get companies by sort key (top10, bottom10, or all)."""
    if sort_key == "all":
        return get_zrange(session_id, 0, -1)
    elif sort_key == "top10":
        return get_zrange(session_id, 0, 9)
    elif sort_key == "bottom10":
        return get_zrange(session_id, 0, 9, desc=False)


def get_ranks_by_symbols(session_id, symbols):
    """Get ranks for specific company symbols."""
    companies_capitalization = []
    for symbol in symbols:
        company_key = f"company:{symbol.lower()}"
        market_cap = send_request_and_await(
            session_id, "ZSCORE", "companyLeaderboard", company_key, ""
        )
        companies_capitalization.append(market_cap)

    companies = []
    for index, market_capitalization in enumerate(companies_capitalization):
        companies.extend([
            f"company:{symbols[index].lower()}",
            market_capitalization
        ])

    return get_result(session_id, companies)


def get_zrange(session_id, start_index, stop_index, desc=True):
    """Get a range of companies from the leaderboard."""
    if desc:
        companies = send_request_and_await(
            session_id, "ZREVRANGE", "companyLeaderboard", start_index, stop_index
        )
    else:
        companies = send_request_and_await(
            session_id, "ZRANGE", "companyLeaderboard", start_index, stop_index
        )

    return get_result(session_id, companies, start_index, desc)


def get_result(session_id, companies, start_index=0, desc=True):
    """Format the companies data into a structured result."""
    if not companies or len(companies) == 0:
        return json.dumps([])

    start_rank = int(start_index) + 1 if desc else len(companies) - start_index
    increase_factor = 1 if desc else -1
    results = []

    for i in range(0, len(companies), 2):
        symbol = companies[i]
        market_cap = companies[i + 1] if i + 1 < len(companies) else None

        company_info = send_request_and_await(
            session_id, "HGETALL", symbol, "", ""
        )

        if company_info and isinstance(company_info, dict):
            results.append({
                "company": company_info.get("company", ""),
                "country": company_info.get("country", ""),
                "marketCap": market_cap,
                "rank": start_rank,
                "symbol": symbol.replace("company:", "")
            })
        start_rank += increase_factor

    return json.dumps(results)
