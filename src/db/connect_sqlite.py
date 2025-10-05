#!/usr/bin/env python3
import os
import sqlite3
import logging
from contextlib import contextmanager


logging.basicConfig(
    format="%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


PRAGMAS = [
    "PRAGMA journal_mode=WAL",
    "PRAGMA synchronous=NORMAL",
    "PRAGMA foreign_keys=ON",
]


def apply_pragmas(conn: sqlite3.Connection):
    for p in PRAGMAS:
        logger.debug(f"Applying PRAGMA: {p}")
        conn.execute(p)


def get_connection(path: str) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    apply_pragmas(connection)
    logger.info(f"Opened database at {path}")
    return connection


@contextmanager
def connection(path = None):
    conn = get_connection(path)
    try:
        yield conn
    finally:
        conn.close()


if __name__ == "__main__":
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else None
    if not path:
        raise SystemExit("Usage: python connect_sqlite.py path/to/db.sqlite")
    with connection(path) as conn:
        cur = conn.cursor()
        print(f"Connected to {path}.")
        cur.execute("PRAGMA table_list;")
        print("Tables:")
        for row in cur.fetchall():
            if row["type"] in ("table", "view") and not row["name"].startswith("sqlite_"):
                print(f"{row['schema']}.{row['name']}")
