#!/usr/bin/env python3
import os
import logging
import argparse

from .utils import list_tables, print_table
from .recreate_from_access import create_single_table, sync_access_to_sqlite

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Example script using argparse")
    parser.add_argument("accdb", help="Path to Microsoft Access database")
    parser.add_argument("sqlite", nargs="?", help="Path to SQLite database (optional)")
    args = parser.parse_args()
    logger.info(f"Access Database: {args.accdb}")
    if args.sqlite is None:
        base, _ = os.path.splitext(args.accdb)
        args.sqlite = base + ".sqlite"
        logger.info(f"SQLite Database: {args.sqlite}")

    tables = list_tables(args.accdb)
    _tables = "\n" + "\n".join(tables)
    logger.info(f"Tables: {_tables}")

    for table in tables:
        logger.info(f"Syncing {table}")
        create_single_table(args.accdb, args.sqlite, table.upper())
        sync_access_to_sqlite(args.accdb, args.sqlite, table.upper())
        print_table(args.sqlite, table.upper())
        break
