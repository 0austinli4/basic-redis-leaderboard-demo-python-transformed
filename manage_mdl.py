#!/usr/bin/env python
"""Django's command-line utility for administrative tasks."""

import os
import sys
from server.core.workload_app_sync import run_workload
from mdlin import InitCustom
import argparse
from django.conf import settings
import django
import os
import sys
from mdlin import InitCustom
from server.core.workload_app_sync import run_workload


def main():
    """Run administrative tasks."""
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "server.configuration.settings")

    try:
        django.setup()
        print("Django setup complete.")
    except Exception:
        pass

    parser = argparse.ArgumentParser(
        description="Run workload with client_id and experiment length."
    )
    parser.add_argument("--clientid", type=str, help="Client ID")
    parser.add_argument(
        "--explen", type=int, required=True, help="Experiment length in seconds"
    )

    args = parser.parse_args()
    client_id = args.clientid
    exp_length = args.explen  # Now it's properly parsed as an integer

    InitCustom(str(client_id), "mdl")
    settings.configure()
    run_workload(exp_length)


if __name__ == "__main__":
    main()
