#!/usr/bin/env python3
import os
import logging
import argparse
import yaml

from typing import Dict, List, Optional

from .utils import list_tables, print_table, describe_table
from .recreate_from_access import (
    create_single_table,
    sync_access_to_sqlite,
    sync_sqlite_to_access,
    evaluate_primary_key,
    suggest_primary_keys,
    PK_SUGGESTION_MAX_COLUMNS,
)

logger = logging.getLogger(__name__)

def load_pk_map(path: str) -> dict:
    if os.path.exists(path):
        with open(path, "r") as f:
            data = yaml.safe_load(f) or {}
            if not isinstance(data, dict):
                return {}
            normalized: Dict[str, Dict[str, object]] = {}
            for key, value in data.items():
                entry: Dict[str, object] = {"columns": None, "skip": False}
                if isinstance(value, dict):
                    cols = value.get("columns")
                    if isinstance(cols, list) and cols:
                        entry["columns"] = [str(c) for c in cols]
                    elif isinstance(cols, str) and cols:
                        entry["columns"] = [cols]
                    entry["skip"] = bool(value.get("skip"))
                elif isinstance(value, list):
                    if value:
                        entry["columns"] = [str(c) for c in value]
                elif isinstance(value, str):
                    if value.strip().lower() == "skip":
                        entry["skip"] = True
                    elif value.strip():
                        entry["columns"] = [value.strip()]
                elif isinstance(value, bool):
                    entry["skip"] = value
                normalized[str(key).upper()] = entry
            return normalized
    return {}

def save_pk_map(path: str, data: dict) -> None:
    serializable: Dict[str, object] = {}
    for key, entry in data.items():
        skip = bool(entry.get("skip"))
        columns = entry.get("columns")
        payload: Dict[str, object] = {}
        if columns:
            payload["columns"] = columns
        if skip:
            payload["skip"] = True
        if payload:
            serializable[key] = payload
    with open(path, "w") as f:
        yaml.safe_dump(serializable, f, sort_keys=True)

def _normalize_columns(candidates: List[str], available: Dict[str, str]) -> List[str]:
    normalized = []
    for col in candidates:
        key = col.strip().lower()
        if key not in available:
            raise KeyError(col)
        normalized.append(available[key])
    return normalized

def resolve_primary_key(accdb: str, table: str, existing_entry: Optional[Dict[str, object]]) -> tuple[List[str], bool]:
    """
    Determine a usable primary key for the given Access table.

    Returns (columns, skip_flag) where `columns` is a list of column names when a valid
    key is identified, and `skip_flag` is True when the user opts to skip syncing the table.
    """
    cols_meta, _, _ = describe_table(accdb, table, verbose=False)
    available = {c["name"].lower(): c["name"] for c in cols_meta}
    existing_columns: Optional[List[str]] = None
    if existing_entry:
        if existing_entry.get("skip"):
            logger.info("Table %s was previously marked to skip syncing.", table)
            return [], True
        existing_columns = existing_entry.get("columns") or None

    def valid_or_none(columns: List[str] | None) -> List[str] | None:
        if not columns:
            return None
        if any(col.lower() not in available for col in columns):
            return None
        result = evaluate_primary_key(accdb, table, [available[col.lower()] for col in columns])
        if result["is_valid"]:
            return [available[col.lower()] for col in columns]
        logger.warning(
            "Stored primary key %s is invalid for %s (null rows=%s, duplicate groups=%s, duplicate rows=%s).",
            columns,
            table,
            result["null_rows"],
            result["duplicate_groups"],
            result["duplicate_rows"],
        )
        return None

    pk = valid_or_none(existing_columns)
    if pk:
        return pk, False

    logger.info("Previewing original Access data for %s to help choose a primary key.", table)
    print_table(accdb, table, subsample=5)

    attempts = suggest_primary_keys(accdb, table, PK_SUGGESTION_MAX_COLUMNS)
    valid_suggestions: List[Dict[str, object]] = []

    if attempts:
        logger.info("Primary key assessment for %s (first %s column combinations):", table, PK_SUGGESTION_MAX_COLUMNS)
        selection_counter = 1
        for attempt in attempts:
            cols_display = ", ".join(attempt["columns"])
            if attempt["is_valid"]:
                logger.info("  %d) %s (unique, no NULLs, duplicate rows=0)", selection_counter, cols_display)
                valid_suggestions.append({
                    "index": selection_counter,
                    "columns": attempt["columns"],
                })
                selection_counter += 1
            else:
                logger.info(
                    "  ✖ %s (null rows=%s, duplicate groups=%s, duplicate rows=%s)",
                    cols_display,
                    attempt["null_rows"],
                    attempt["duplicate_groups"],
                    attempt["duplicate_rows"],
                )
    else:
        logger.info(
            "No primary key candidates found among the first %s columns for %s.",
            PK_SUGGESTION_MAX_COLUMNS,
            table,
        )
        logger.info(
            "Note: duplicate group counts can stay the same or increase as more columns are evaluated, "
            "because each grouping level measures how many column combinations repeat. The additional "
            "metric `duplicate rows` shows the number of actual records that would violate uniqueness."
        )

    while True:
        prompt = (
            "Primary key undefined. Enter column names separated by commas"
            + (" or choose a suggested combination" if valid_suggestions else "")
            + " (type 'skip' to ignore this table)"
            + ": "
        )
        response = input(prompt).strip()
        if not response:
            logger.error("Primary key selection cannot be empty.")
            continue

        if response.lower() in {"skip", "s"}:
            logger.warning("User opted to skip syncing table %s due to missing primary key.", table)
            return [], True

        if valid_suggestions and response.isdigit():
            idx = int(response)
            match = next((cand for cand in valid_suggestions if cand["index"] == idx), None)
            if not match:
                logger.error("Invalid selection. Choose a number between 1 and %s.", len(valid_suggestions))
                continue
            candidate = match["columns"]
        else:
            raw_cols = [part.strip() for part in response.split(",") if part.strip()]
            if len(raw_cols) == 0:
                logger.error("Primary key selection cannot be empty.")
                continue
            if len(raw_cols) > PK_SUGGESTION_MAX_COLUMNS:
                logger.error(
                    "A maximum of %s columns is supported for the primary key suggestion process.",
                    PK_SUGGESTION_MAX_COLUMNS,
                )
                continue
            try:
                candidate = _normalize_columns(raw_cols, available)
            except KeyError as missing:
                logger.error(
                    "Column '%s' is not recognized. Available columns include: %s",
                    missing.args[0],
                    ", ".join(c["name"] for c in cols_meta[:10]),
                )
                continue

        result = evaluate_primary_key(accdb, table, candidate)
        if result["is_valid"]:
            return candidate, False

        logger.error(
            "Columns %s are not a valid primary key (null rows=%s, duplicate groups=%s, duplicate rows=%s).",
            ", ".join(candidate),
            result["null_rows"],
            result["duplicate_groups"],
            result["duplicate_rows"],
        )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync Access and SQLite databases, remembering PKs in YAML")
    parser.add_argument("accdb", help="Path to Microsoft Access database")
    parser.add_argument("sqlite", nargs="?", help="Path to SQLite database (optional)")
    parser.add_argument("--tables", nargs="*", default=[], help="List of tables to sync.")
    parser.add_argument("--direction",
                        choices=("access-to-sqlite", "sqlite-to-access"),
                        default="access-to-sqlite",
                        help="Direction of synchronization. Default is Access → SQLite.")
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

        stored_entry = pk_map.get(table, {"columns": None, "skip": False})
        if stored_entry.get("skip"):
            logger.info("Skipping %s (marked to skip in primary key map).", table)
            continue

        _, access_pk, _ = describe_table(args.accdb, table, verbose=False)
        pk_override = None

        if access_pk:
            logger.info("Using Access-defined primary key for %s: %s", table, ", ".join(access_pk))
            if table in pk_map:
                pk_map.pop(table, None)
                save_pk_map(pk_map_path, pk_map)
        else:
            pk_columns, skip_table = resolve_primary_key(args.accdb, table, stored_entry)
            if skip_table:
                pk_map[table] = {"columns": None, "skip": True}
                save_pk_map(pk_map_path, pk_map)
                logger.info("Skipping %s; decision recorded in PK map.", table)
                continue
            pk_override = pk_columns
            pk_map[table] = {"columns": pk_override, "skip": False}
            save_pk_map(pk_map_path, pk_map)
            logger.info("Using override primary key for %s: %s", table, ", ".join(pk_override))

        try:
            if args.direction == "access-to-sqlite":
                create_single_table(args.accdb, args.sqlite, table, pk_override=pk_override)
                sync_access_to_sqlite(args.accdb, args.sqlite, table, pk_override=pk_override)
            else:
                sync_sqlite_to_access(args.sqlite, args.accdb, table, pk_override=pk_override)
        except ValueError as e:
            raise Exception(f"Failed to sync table {table}: {e}") from e

        if args.direction == "access-to-sqlite":
            print_table(args.sqlite, table.upper(), subsample=10)
