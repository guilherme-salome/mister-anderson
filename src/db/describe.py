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
    cur.execute("""
      SELECT
        rc.CONSTRAINT_NAME,
        fk_cols.COLUMN_NAME      AS FK_COLUMN,
        fk_cols.ORDINAL_POSITION AS FK_ORD,
        tgt_tc.TABLE_NAME        AS PK_TABLE,
        pk_cols.COLUMN_NAME      AS PK_COLUMN,
        rc.UPDATE_RULE,
        rc.DELETE_RULE
      FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
      JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS fk_tc
        ON fk_tc.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
       AND fk_tc.TABLE_SCHEMA    = rc.CONSTRAINT_SCHEMA
      JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE fk_cols
        ON fk_cols.CONSTRAINT_NAME = fk_tc.CONSTRAINT_NAME
       AND fk_cols.TABLE_SCHEMA    = fk_tc.TABLE_SCHEMA
      JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tgt_tc
        ON tgt_tc.CONSTRAINT_NAME = rc.UNIQUE_CONSTRAINT_NAME
       AND tgt_tc.TABLE_SCHEMA    = rc.UNIQUE_CONSTRAINT_SCHEMA
      JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE pk_cols
        ON pk_cols.CONSTRAINT_NAME = tgt_tc.CONSTRAINT_NAME
       AND pk_cols.TABLE_SCHEMA    = tgt_tc.TABLE_SCHEMA
       AND pk_cols.ORDINAL_POSITION = fk_cols.ORDINAL_POSITION
      WHERE fk_tc.TABLE_SCHEMA = 'PUBLIC'
        AND fk_tc.TABLE_NAME   = UPPER(?)
      ORDER BY rc.CONSTRAINT_NAME, fk_cols.ORDINAL_POSITION
    """, [table])
    fk_rows = cur.fetchall()
    fks = {}
    for name, fk_col, ord_pos, pk_table, pk_col, on_upd, on_del in fk_rows:
        d = fks.setdefault(name, {
            "name": name,
            "columns": [],
            "ref_table": pk_table,
            "ref_columns": [],
            "update_rule": on_upd,
            "delete_rule": on_del,
        })
        d["columns"].append((ord_pos, fk_col))
        d["ref_columns"].append((ord_pos, pk_col))
    fks = [
        {
            **d,
            "columns": [c for _, c in sorted(d["columns"])],
            "ref_columns": [c for _, c in sorted(d["ref_columns"])],
        }
        for d in fks.values()
    ]
    return cols, pk_cols, fks


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
    qtable = '"' + table_name.replace('"','""') + '"'
    cur.execute(f"PRAGMA foreign_key_list({qtable});")
    fk_rows = cur.fetchall()
    fks_by_id = {}
    for r in fk_rows:
        d = fks_by_id.setdefault(r["id"], {
            "name": None,  # SQLite doesn't store FK names
            "columns": [],
            "ref_table": r["table"],
            "ref_columns": [],
            "update_rule": r["on_update"],
            "delete_rule": r["on_delete"],
            "match": r["match"],
        })
        d["columns"].append((r["seq"], r["from"]))
        d["ref_columns"].append((r["seq"], r["to"]))
    fks = [
        {
            **d,
            "columns": [c for _, c in sorted(d["columns"])],
            "ref_columns": [c for _, c in sorted(d["ref_columns"])],
        }
        for d in fks_by_id.values()
    ]
    return cols, pk_cols, fks


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Example script using argparse")
    parser.add_argument("accdb", help="Path to Microsoft Access database")
    parser.add_argument("sqlite", nargs="?", help="Path to SQLite database (optional)")
    parser.add_argument("table", help="Table name")
    args = parser.parse_args()
    if args.sqlite is None:
        base, _ = os.path.splitext(args.accdb)
        args.sqlite = base + ".sqlite"
        logger.info(f"SQLite Database: {args.sqlite}")

    from .connect_access import connection as access_connection
    with access_connection(args.accdb) as conn:
        logger.info("Access schema: %s", describe_access(conn, args.table))
    from .connect_sqlite import connection as sqlite_connection
    with sqlite_connection(args.sqlite) as conn:
        logger.info("SQLite schema: %s", describe_sqlite(conn, args.table))
