#!/usr/bin/env python3

import argparse
import logging

from .utils import print_table


logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pretty-print rows from an Access or SQLite table."
    )
    parser.add_argument(
        "database",
        help="Path to the .accdb or .sqlite file.",
    )
    parser.add_argument(
        "table",
        help="Table name to inspect.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Maximum number of rows to display (default: 5).",
    )
    parser.add_argument(
        "--horizontal",
        action="store_true",
        help="Display rows horizontally (entries as rows). Vertical layout is the default.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity (default: INFO).",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    level = getattr(logging, args.log_level.upper(), logging.INFO)
    logging.basicConfig(
        format="%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s",
        level=level,
    )
    vertical = not args.horizontal
    orientation = "vertical" if vertical else "horizontal"
    logger.info(
        "Request to print up to %s rows from table '%s' in %s (layout: %s)",
        args.limit,
        args.table,
        args.database,
        orientation,
    )
    print_table(args.database, args.table, subsample=args.limit, vertical=vertical)


if __name__ == "__main__":
    main()
