#!/usr/bin/env python3
import os
import logging
import argparse
import yaml

from .utils import list_tables, print_table
from .recreate_from_access import create_single_table, sync_access_to_sqlite

logger = logging.getLogger(__name__)

def load_pk_map(path: str) -> dict:
    if os.path.exists(path):
        with open(path, "r") as f:
            data = yaml.safe_load(f) or {}
            if not isinstance(data, dict):
                return {}
            return {str(k).upper(): list(v) for k, v in data.items()}
    return {}

def save_pk_map(path: str, data: dict) -> None:
    with open(path, "w") as f:
        yaml.safe_dump(data, f, sort_keys=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync Access and SQLite databases, remembering PKs in YAML")
    parser.add_argument("accdb", help="Path to Microsoft Access database")
    parser.add_argument("sqlite", nargs="?", help="Path to SQLite database (optional)")
    parser.add_argument("--tables", nargs="*", default=[], help="List of tables to sync.")
    args = parser.parse_args()
    logger.info(f"Access Database: {args.accdb}")

    # SQLite db has the same name as the Access db, but different extension
    if args.sqlite is None:
        base, _ = os.path.splitext(args.accdb)
        args.sqlite = base + ".sqlite"
        logger.info(f"SQLite Database: {args.sqlite}")

    # Load or create a PK map
    base, _ = os.path.splitext(args.accdb)
    pk_map_path = base + ".pk.yaml"
    pk_map = load_pk_map(pk_map_path)

    # Print all tables
    if not args.tables:
        args.tables = [x.upper() for x in list_tables(args.accdb)]
    _tables = "\n" + "\n".join(args.tables)
    logger.info(f"Tables: {_tables}")

    # Sync each table
    for table in args.tables:
        logger.info(f"Syncing {table}")
        # Check if primary key for table is defined in YAML file
        pk_override = pk_map.get(table)
        # Create table if it doesn't already exist
        create_single_table(args.accdb, args.sqlite, table, pk_override = pk_override)
        try:
            sync_access_to_sqlite(args.accdb, args.sqlite, table, pk_override = pk_override)
        except ValueError as e:
            # No primary key defined in YAML and in table
            if "cannot sync without a primary key" in str(e):
                cols = input("Primary key undefined. Please provide name of column(s) to be primary key(s):")
                pk_override = [x.strip() for x in cols.split(",")]
                if not pk_override:
                    raise ValueError(f"Undefined primary key for table {table}")
                logger.warning(f"Provided primary key(s): {pk_override}")
                # Need to re-create the table because it was created without a PK
                create_single_table(args.accdb, args.sqlite, table, overwrite = True, pk_override = pk_override)
                sync_access_to_sqlite(args.accdb, args.sqlite, table, pk_override = pk_override)
            else:
                raise Exception(e)
        if pk_override:
            pk_map[table] = pk_override
            save_pk_map(pk_map_path, pk_map)
        print_table(args.sqlite, table.upper(), subsample = 10)
