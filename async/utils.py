import json
from iocl.iocl_utils import send_request, await_request
import sys


def update_company_market_capitalization(session_id, amount, symbol):
    """Update the market capitalization of a company by incrementing its score in the leaderboard."""
    pending_awaits = {*()}
    company_key = f"company:{symbol.lower()}"
    future_0 = send_request(
        session_id, "ZINCRBY", "companyLeaderboard", str(amount), company_key
    )
    pending_awaits.add(future_0)
    for future in pending_awaits:
        await_request(session_id, future)
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
    pending_awaits = []
    companies_capitalization = []

    # Send all requests
    for symbol in symbols:
        company_key = f"company:{symbol.lower()}"
        future = send_request(
            session_id, "ZSCORE", "companyLeaderboard", company_key
        )
        pending_awaits.append(future)

    # Await all responses
    for future in pending_awaits:
        market_cap = await_request(session_id, future)
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
    pending_awaits = []

    if desc:
        future_0 = send_request(
            session_id, "ZREVRANGE", "companyLeaderboard", start_index, stop_index
        )
        pending_awaits.append(future_0)
    else:
        future_0 = send_request(
            session_id, "ZRANGE", "companyLeaderboard", start_index, stop_index
        )
        pending_awaits.append(future_0)

    companies = await_request(session_id, pending_awaits[0])
    return get_result(session_id, companies, start_index, desc)


def get_result(session_id, companies, start_index=0, desc=True):
    """Format the companies data into a structured result."""
    if not companies or len(companies) == 0:
        return json.dumps([])

    pending_awaits = []
    start_rank = int(start_index) + 1 if desc else len(companies) - start_index
    increase_factor = 1 if desc else -1

    # Send all HGETALL requests
    for i in range(0, len(companies), 2):
        symbol = companies[i]
        future = send_request(session_id, "HGETALL", symbol)
        pending_awaits.append(future)

    # Await and build results
    results = []
    current_rank = start_rank
    for i in range(0, len(companies), 2):
        symbol = companies[i]
        market_cap = companies[i + 1] if i + 1 < len(companies) else None

        company_info = await_request(session_id, pending_awaits[i // 2])

        if company_info and isinstance(company_info, dict):
            results.append({
                "company": company_info.get("company", ""),
                "country": company_info.get("country", ""),
                "marketCap": market_cap,
                "rank": current_rank,
                "symbol": symbol.replace("company:", "")
            })
        current_rank += increase_factor

    return json.dumps(results)
