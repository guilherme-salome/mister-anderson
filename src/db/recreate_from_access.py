#!/usr/bin/env python3
import logging
from typing import List, Dict, Tuple

from .connect_access import connection as access_connection
from .connect_sqlite import connection as sqlite_connection
from .describe import describe_access  # returns (cols, pk_cols, fks)

logger = logging.getLogger(__name__)

def list_access_user_tables(acc_conn) -> List[str]:
    cur = acc_conn.cursor()
    cur.execute("""
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA='PUBLIC' AND TABLE_TYPE='BASE TABLE'
        ORDER BY TABLE_NAME
    """)
    return [r[0] for r in cur.fetchall()]

def access_to_sqlite_type(type_name: str) -> str:
    t = (type_name or "").upper()
    if t in ("COUNTER", "AUTOINCREMENT", "IDENTITY", "LONG", "INTEGER", "INT", "SMALLINT"):
        return "INTEGER"
    if t in ("DOUBLE", "FLOAT", "REAL", "SINGLE", "NUMERIC", "DECIMAL", "MONEY", "CURRENCY"):
        return "REAL"
    if t in ("DATETIME", "DATE", "TIME", "TIMESTAMP"):
        return "TEXT"  # store ISO8601
    if t in ("YESNO", "BOOLEAN", "BIT"):
        return "INTEGER"  # 0/1
    if t in ("BINARY", "VARBINARY", "IMAGE", "OLEOBJECT"):
        return "BLOB"
    return "TEXT"

def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'

def build_sqlite_create(table: str,
                        acc_cols: List[Dict],
                        pk_cols: List[str],
                        fks: List[Dict]) -> str:
    # columns
    defs = []
    # Prefer INTEGER PRIMARY KEY for single integer PK (rowid)
    single_int_pk = len(pk_cols) == 1 and access_to_sqlite_type(
        next(c for c in acc_cols if c["name"] == pk_cols[0])["type_name"]
    ) == "INTEGER"
    for c in acc_cols:
        name = qident(c["name"])
        t = access_to_sqlite_type(c["type_name"])
        if single_int_pk and c["name"] == pk_cols[0]:
            defs.append(f"{name} INTEGER PRIMARY KEY")
        else:
            nn = " NOT NULL" if not c.get("nullable", True) else ""
            defs.append(f"{name} {t}{nn}")
    # composite PK
    if pk_cols and not single_int_pk:
        defs.append("PRIMARY KEY (" + ", ".join(qident(c) for c in pk_cols) + ")")
    # FKs
    for fk in fks:
        cols = ", ".join(qident(c) for c in fk["columns"])
        ref_cols = ", ".join(qident(c) for c in fk["ref_columns"])
        on_upd = fk.get("update_rule")
        on_del = fk.get("delete_rule")
        upd = f" ON UPDATE {on_upd}" if on_upd and on_upd.upper() != "NO ACTION" else ""
        dele = f" ON DELETE {on_del}" if on_del and on_del.upper() != "NO ACTION" else ""
        defs.append(f"FOREIGN KEY ({cols}) REFERENCES {qident(fk['ref_table'])} ({ref_cols}){upd}{dele}")
    return f"CREATE TABLE {qident(table)} (\n  " + ",\n  ".join(defs) + "\n)"

def sqlite_table_exists(sql_conn, table: str) -> bool:
    cur = sql_conn.cursor()
    cur.execute("SELECT 1 FROM sqlite_schema WHERE type IN ('table','view') AND name = ?", [table])
    return cur.fetchone() is not None

def recreate_single_table(acc_conn, sql_conn, table: str, overwrite: bool = False, preview: bool = True):
    cols, pk_cols, fks = describe_access(acc_conn, table)
    if not cols:
        logger.warning(f"{table}: no columns found; skipping")
        return
    ddl = build_sqlite_create(table, cols, pk_cols, fks)
    if preview:
        print(f"\n-- {table}")
        print(ddl)

    cur = sql_conn.cursor()
    if sqlite_table_exists(sql_conn, table):
        if not overwrite:
            print(f"SQLite table {table} already exists. Skipping (use overwrite=True to drop).")
            return
        cur.execute(f"DROP TABLE IF EXISTS {qident(table)}")
        sql_conn.commit()
    cur.execute(ddl)
    sql_conn.commit()
    print(f"Created SQLite table {table}")

def interactive_recreate(accdb_path: str, sqlite_path: str):
    with access_connection(accdb_path) as acc, sqlite_connection(sqlite_path) as sq:
        tables = list_access_user_tables(acc)
        if not tables:
            print("No user tables found.")
            return
        print("User tables in Access:")
        for i, t in enumerate(tables, 1):
            print(f"{i:2d}. {t}")

        all_yes = False
        overwrite = None
        for t in tables:
            if not all_yes:
                ans = input(f"Recreate {t} in SQLite? [y/N/a=all/q] ").strip().lower()
                if ans == "q":
                    break
                if ans == "a":
                    all_yes = True
                elif ans not in ("y", "yes"):
                    continue
            if overwrite is None and sqlite_table_exists(sq, t):
                o = input(f"SQLite table {t} exists. Drop and recreate? [y/N] ").strip().lower()
                overwrite = o in ("y", "yes")
            recreate_single_table(acc, sq, t, overwrite=bool(overwrite), preview=True)

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        raise SystemExit("Usage: python -m db.recreate_from_access path/to/assets.accdb path/to/assets.sqlite")
    interactive_recreate(sys.argv[1], sys.argv[2])
