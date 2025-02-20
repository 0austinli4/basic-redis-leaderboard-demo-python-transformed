#!/usr/bin/env python
"""Django's command-line utility for administrative tasks."""

import os
import sys
from core.workload_app_ioc import run_workload
from mdlin import InitCustom


def main():
    """Run administrative tasks."""
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "configuration.settings")

    if len(sys.argv) < 2:
        print("Usage: python app.py <client_id>")
        sys.exit(1)

    client_id = sys.argv[1]

    InitCustom(client_id, "mdl")
    run_workload()
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
