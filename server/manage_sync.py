#!/usr/bin/env python
"""Django's command-line utility for administrative tasks."""

import argparse
import os
import sys
from core.workload_app_sync import run_workload
from mdlin import InitCustom


def main():
    """Run administrative tasks."""
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "configuration.settings")

    parser = argparse.ArgumentParser(
        description="Run workload with client_id and experiment length."
    )
    parser.add_argument("--client_id", type=str, help="Client ID")
    parser.add_argument(
        "--explen", type=int, required=True, help="Experiment length in seconds"
    )

    args = parser.parse_args()
    client_id = args.client_id
    exp_length = args.explen  # Now it's properly parsed as an integer

    InitCustom(client_id, "multi_paxos")
    run_workload(exp_length)
    # try:
    #     from django.core.management import execute_from_command_line
    # except ImportError as exc:
    #     raise ImportError(
    #         "Couldn't import Django. Are you sure it's installed and "
    #         "available on your PYTHONPATH environment variable? Did you "
    #         "forget to activate a virtual environment?"
    #     ) from exc
    # execute_from_command_line(sys.argv)


if __name__ == "__main__":
    main()
