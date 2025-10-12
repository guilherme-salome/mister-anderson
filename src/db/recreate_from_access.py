#!/usr/bin/env python3
import logging
from typing import List, Dict, Tuple

from .connect_access import connection as access_connection
from .connect_sqlite import connection as sqlite_connection
from .utils import list_tables, describe_table, access_to_sqlite_type, qident, table_exists, same_columns

logger = logging.getLogger(__name__)


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

def create_single_table(accdb_path: str,
                        sqlite_path: str,
                        table: str,
                        overwrite: bool = False,
                        preview: bool = True):
    cols, pk_cols, fks = describe_table(accdb_path, table)
    if not cols:
        logger.warning(f"{table}: no columns found; skipping")
        return
    ddl = build_sqlite_create(table, cols, pk_cols, fks)
    if preview:
        print(f"\n-- {table}")
        print(ddl)
    with sqlite_connection(sqlite_path) as con:
        cur = con.cursor()
        if table_exists(sqlite_path, table):
            if not overwrite:
                print(f"SQLite table {table} already exists. Skipping (use overwrite=True to drop).")
                return
            cur.execute(f"DROP TABLE IF EXISTS {qident(table)}")
            con.commit()
        cur.execute(ddl)
        con.commit()
        print(f"Created SQLite table {table}")
    assert same_columns(accdb_path, sqlite_path, table), "Columns do not match"

def sync_access_to_sqlite(accdb_path: str,
                          sqlite_path: str,
                          table: str,
                          chunk_size: int = 1000):
    cols, pk_cols, _ = describe_table(accdb_path, table, verbose=False)
    if not pk_cols:
        raise ValueError(f"{table}: cannot sync without a primary key")
    col_names = [c["name"] for c in cols]
    placeholders = ", ".join(["?"] * len(col_names))
    quoted_cols = ", ".join(qident(c, "access") for c in col_names)
    # UPSERT clause (update all non-PK columns)
    upsert_clause = ", ".join(
        f"{qident(c, 'access')} = excluded.{qident(c, 'access')}"
        for c in col_names if c not in pk_cols
    )
    conflict_cols = ", ".join(qident(c, 'access') for c in pk_cols)
    sql = f"""
        INSERT INTO {qident(table)} ({quoted_cols})
        VALUES ({placeholders})
        ON CONFLICT({conflict_cols}) DO UPDATE SET
          {upsert_clause}
    """
    with access_connection(accdb_path) as acc_con, sqlite_connection(sqlite_path) as sqlite_con:
        acc_cur = acc_con.cursor()
        sqlite_cur = sqlite_con.cursor()
        acc_cur.execute(f"SELECT {quoted_cols} FROM {qident(table, 'access')}")
        while True:
            rows = acc_cur.fetchmany(chunk_size)
            if not rows:
                break
            sqlite_cur.executemany(sql, rows)
        sqlite_con.commit()
    print(f"Synchronized table {table}")



if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        raise SystemExit("Usage: python -m db.recreate_from_access path/to/assets.accdb path/to/assets.sqlite")
    interactive_recreate(sys.argv[1], sys.argv[2])
