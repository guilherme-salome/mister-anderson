#!/usr/bin/env python3
import os
import logging

from .connect_access import connection as access_connection
from .connect_sqlite import connection as sqlite_connection

logging.basicConfig(
    format="%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


def describe_access(connection, table):
    cur = connection.cursor()
    # Columns
    cur.execute(f"""
        SELECT
          c.TABLE_NAME,
          c.COLUMN_NAME,
          c.DATA_TYPE,
          c.CHARACTER_MAXIMUM_LENGTH,
          c.IS_NULLABLE,
          pk.ORDINAL_POSITION AS PK_ORDINAL,
          CASE WHEN pk.COLUMN_NAME IS NULL THEN 0 ELSE 1 END AS IS_PK
        FROM INFORMATION_SCHEMA.COLUMNS c
        LEFT JOIN (
          SELECT kcu.TABLE_SCHEMA, kcu.TABLE_NAME, kcu.COLUMN_NAME, kcu.ORDINAL_POSITION
          FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
          JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
           AND kcu.TABLE_SCHEMA    = tc.TABLE_SCHEMA
           AND kcu.TABLE_NAME      = tc.TABLE_NAME
          WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        ) pk
          ON  pk.TABLE_SCHEMA = c.TABLE_SCHEMA
          AND pk.TABLE_NAME   = c.TABLE_NAME
          AND pk.COLUMN_NAME  = c.COLUMN_NAME
        WHERE c.TABLE_SCHEMA = 'PUBLIC'
          AND c.TABLE_NAME   = '{table.upper()}'
        ORDER BY c.ORDINAL_POSITION;
    """)
    for row in cur.fetchall():
        print(row)


def describe_sqlite(connection, table):
    import re
    cur = connection.cursor()

    def qident(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    # Allow optional "schema.table" (e.g., "main.People")
    if "." in table:
        schema, tbl = table.split(".", 1)
        target = f"{schema}.{qident(tbl)}"
        table_name = tbl
    else:
        target = qident(table)
        table_name = table

    cur.execute(f"PRAGMA table_info({target});")
    for row in cur.fetchall():
        dtype = row["type"] or ""
        m = re.search(r"\((\d+)\)", dtype)
        char_max = int(m.group(1)) if m else None
        pk_ord = row["pk"]  # 0 if not part of PK; 1..N gives PK position for composite PKs
        print((
            table_name,
            row["name"],
            dtype,
            char_max,
            "NO" if row["notnull"] else "YES",
            pk_ord if pk_ord else None,
            1 if pk_ord else 0,
        ))


if __name__ == "__main__":
    with access_connection(os.path.join("data", "sample.accdb")) as conn:
        print("sample.accdb")
        describe_access(conn, "people")
        describe_access(conn, "orders")
    with sqlite_connection(os.path.join("data", "sample.sqlite")) as conn:
        print("sample.accdb")
        describe_sqlite(conn, "people")
        describe_sqlite(conn, "orders")
