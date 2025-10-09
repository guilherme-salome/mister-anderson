#!/usr/bin/env python3
import os
import logging


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
    rows = cur.fetchall()
    cols = [
        {
            "name": r[1],                          # COLUMN_NAME
            "type_name": (r[2] or ""),             # DATA_TYPE (string in UCanAccess)
            "size": r[3],                          # CHARACTER_MAXIMUM_LENGTH
            "nullable": (str(r[4]).upper() == "YES"),
        }
        for r in rows
    ]
    pk_cols = [r[1] for r in sorted((rr for rr in rows if rr[6]), key=lambda rr: (rr[5] or 0))]
    return cols, pk_cols


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
    rows = cur.fetchall()
    def _size(dtype):
        m = re.search(r"\((\d+)\)", dtype or "")
        return int(m.group(1)) if m else None
    cols = [
        {
            "name": r["name"],
            "type_name": r["type"] or "",
            "size": _size(r["type"]),
            "nullable": (not r["notnull"]),
        }
        for r in rows
    ]
    pk_cols = [r["name"] for r in sorted(rows, key=lambda r: r["pk"]) if r["pk"]]
    return cols, pk_cols


if __name__ == "__main__":
    from .connect_access import connection as access_connection
    with access_connection(os.path.join("data", "sample.accdb")) as conn:
        print("sample.accdb")
        print(describe_access(conn, "people"))
        print(describe_access(conn, "orders"))
    from .connect_sqlite import connection as sqlite_connection
    with sqlite_connection(os.path.join("data", "sample.sqlite")) as conn:
        print("sample.sqlite")
        print(describe_sqlite(conn, "people"))
        print(describe_sqlite(conn, "orders"))
