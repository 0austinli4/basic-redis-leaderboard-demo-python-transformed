import argparse
import os
import sys
from server.core.workload_app_sync import run_workload
from django.conf import settings
from mdlin import InitCustom
import django

def main():
    """Run administrative tasks."""
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'server.configuration.settings')

    # Print the current Python path
    print("Current Python Path:", sys.path)

    try:
        django.setup()
        print("Django setup complete.")
    except Exception as e:
        print("Error during Django setup:", e)

    print("Running manage sync")

    parser = argparse.ArgumentParser(
        description="Run workload with client_id and experiment length."
    )
    parser.add_argument('--clientid', type=int, required=True, help='Client ID')
    parser.add_argument('--explen', type=int, required=True, help='Experiment length in seconds')

    args = parser.parse_args()
    client_id = args.clientid
    exp_length = args.explen  # Now it's properly parsed as an integer

    print(f"Client ID: {client_id}, Experiment Length: {exp_length}")

    InitCustom(client_id, "multi_paxos")

    try:
        run_workload(exp_length)
    except Exception as e:
        print("Error while running workload:", e)

if __name__ == "__main__":
    main()