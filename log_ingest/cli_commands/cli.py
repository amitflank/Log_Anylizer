#!/usr/bin/env python3
"""Main CLI entry point for log_ingest commands.

Provides a unified interface to all CLI commands:
- bucket: Create and manage time-aligned buckets
- select-buckets: Select and filter bucket data from parquet files
"""

import click

from log_ingest.cli_commands.bucket_create_cli import cli as bucket_cli
from log_ingest.cli_commands.bucket_selector_cli import cli as selector_cli
from log_ingest.cli_commands.window_builder_cli import window_builder


@click.group()
def cli() -> None:
    """Log Ingest CLI - Process and analyze storage system logs."""
    pass


# Add selector commands under 'select-buckets' group
cli.add_command(selector_cli, name="select-buckets")

# Add bucket creation commands under 'bucket' group
cli.add_command(bucket_cli, name="bucket")

# Add look-back window builder command
cli.add_command(window_builder, name="window-builder")


if __name__ == "__main__":
    cli()
