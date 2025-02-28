import json
from mdlin import SyncAppRequest, InitCustom


def init():
    symbol = "AAPL"
    # Should add AAPL to the list
    SyncAppRequest(
        "ZADD",
        "leaderboard",
        {symbol: 12311400},
    )
    # SyncAppRequest("HSET", symbol, "company", "APPLE")
    # SyncAppRequest("HSET", symbol, "country", "USA")
    print("init completed")


# test the methods from mdlin
def get_zrange(start_index, stop_index, desc=True):
    print("Sending zrange request")
    companies = SyncAppRequest("ZRANGE", "leaderboard", start_index, stop_index)
    print("Result, companies", companies)

    return get_result(companies, start_index, desc)


def get_result(companies, start_index=0, desc=True):
    symbol = "AAPL"
    # company_info = SyncAppRequest("HGETALL", symbol)
    print("get reuslt mock")


if __name__ == "__main__":
    InitCustom("0", "multi_paxos")
    print("Running the python client")
    init()
    print("Compelted init")
    get_zrange(1, 10)
