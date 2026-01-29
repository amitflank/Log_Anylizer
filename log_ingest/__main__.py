#!/usr/bin/env python3
"""Main entry point for log_ingest package.

Allows running as: python -m log_ingest
"""

from log_ingest.cli_commands.cli import cli

if __name__ == "__main__":
    cli()
