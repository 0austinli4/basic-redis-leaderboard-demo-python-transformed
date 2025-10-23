import sys
import os
import workload_app_async
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import argparse
from iocl.config_env import set_env_from_command_line_args, init_benchmark_with_config
from iocl.iocl_utils import send_request, await_request
import redisstore


def run_app(session_id, client_id, client_type, explen):
    pending_awaits = {*()}
    if int(client_id) == 0:
        # Client 0 initializes the leaderboard
        future_0 = send_request(session_id, "EXISTS", "companyLeaderboard")
        pending_awaits.add(future_0)
        leaderboard_exists = await_request(session_id, future_0)
        pending_awaits.remove(future_0)

        if leaderboard_exists == "0":
            # Initialize with companies_data.json
            import json
            companies_file = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "companies_data.json"
            )
            with open(companies_file, "r") as f:
                companies = json.load(f)
                for company in companies:
                    symbol = f"company:{company['symbol'].lower()}"
                    # Add to sorted set
                    future = send_request(
                        session_id, "ZADD", "companyLeaderboard",
                        str(company["marketCap"]), symbol
                    )
                    pending_awaits.add(future)
                    # Store company details in hash
                    future_h = send_request(
                        session_id, "HSET", symbol,
                        {"company": company["company"], "country": company["country"]}
                    )
                    pending_awaits.add(future_h)

                # Await all initialization requests
                for future in list(pending_awaits):
                    await_request(session_id, future)
                    pending_awaits.remove(future)

    elif int(client_id) > 0:
        # Wait for client 0 to initialize
        while True:
            future_0 = send_request(session_id, "EXISTS", "companyLeaderboard")
            pending_awaits.add(future_0)
            leaderboard_exists = await_request(session_id, future_0)
            pending_awaits.remove(future_0)
            if leaderboard_exists != "0":
                break

    workload_app_async.create(session_id, client_id, explen)


if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="IOCL Benchmark Client")

    parser.add_argument(
        "--config",
        action="store",
        dest="config_path",
        default="/users/akalaba/IOCL/experiments/configs/1shard_transformed_test_wisc.json",
        help="Path to the JSON configuration file",
    )
    parser.add_argument(
        "--explen",
        action="store",
        dest="explen",
        default=30,
        help="Experiment length override",
    )
    parser.add_argument("--warmup_secs", type=int, default=0, help="Warmup seconds")
    parser.add_argument("--cooldown_secs", type=int, default=0, help="Cooldown seconds")
    parser.add_argument("--clientid", type=int, default=0, help="Client ID")
    parser.add_argument("--num_keys", type=int, default=1000000, help="Number of keys")
    parser.add_argument("--num_shards", type=int, default=1, help="Number of shards")
    parser.add_argument(
        "--replica_config_paths",
        type=str,
        default="",
        help="Path(s) to replica config(s)",
    )
    parser.add_argument(
        "--net_config_path", type=str, default="", help="Path to network config"
    )
    parser.add_argument(
        "--client_host", type=str, default="localhost", help="Client host name"
    )
    parser.add_argument(
        "--trans_protocol",
        type=str,
        choices=["tcp", "udp"],
        default="tcp",
        help="Transport protocol",
    )

    parser.add_argument("--partitioner", type=str, default="", help="Partitioner type")
    parser.add_argument("--key_selector", type=str, default="", help="Key selector")
    parser.add_argument(
        "--zipf_coefficient", type=float, default=0.0, help="Zipf coefficient"
    )
    parser.add_argument("--debug_stats", action="store_true", help="Enable debug stats")
    parser.add_argument("--delay", type=int, default=0, help="Random delay")
    parser.add_argument(
        "--ping_replicas", type=str, default="", help="Ping replicas flag"
    )
    parser.add_argument("--stats_file", type=str, default="", help="Stats file path")

    args = parser.parse_args()

    try:
        set_env_from_command_line_args(args)
        init_benchmark_with_config(args.config_path)
        session_id = redisstore.custom_init_session()
        run_app(session_id, args.clientid, "multi_paxos", args.explen)

    except FileNotFoundError:
        print(f"Error: Config file not found at {args.config_path}")
        sys.exit(1)
    except Exception as e:
        print(f"Error initializing client: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
