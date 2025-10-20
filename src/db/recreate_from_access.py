#!/usr/bin/env python3
import logging
from typing import List, Dict, Tuple

from .connect_access import connection as access_connection
from .connect_sqlite import connection as sqlite_connection
from .utils import (
    list_tables,
    describe_table,
    access_to_sqlite_type,
    is_access_binary_type,
    qident,
    table_exists,
    same_columns,
)

logger = logging.getLogger(__name__)

PK_SUGGESTION_MAX_COLUMNS = 3


def evaluate_primary_key(accdb_path: str, table: str, columns: List[str]) -> Dict[str, object]:
    """
    Check whether `columns` form a valid primary key in the Access table.
    Returns metrics describing null rows, duplicate groups, duplicate row count, and validity.
    """
    if not columns:
        raise ValueError("At least one column is required to evaluate a primary key candidate.")

    with access_connection(accdb_path) as con:
        cur = con.cursor()
        table_ident = qident(table, "access")
        col_exprs = [qident(col, "access") for col in columns]

        null_rows = 0
        null_predicate = " OR ".join(f"{expr} IS NULL" for expr in col_exprs)
        if null_predicate:
            cur.execute(f"SELECT COUNT(*) FROM {table_ident} WHERE {null_predicate}")
            null_rows = cur.fetchone()[0] or 0

        cur.execute(
            "SELECT COUNT(*) AS group_count, SUM(dup_counts.cnt - 1) AS dup_rows FROM ("
            f" SELECT COUNT(*) AS cnt FROM {table_ident}"
            f" GROUP BY {', '.join(col_exprs)}"
            " HAVING COUNT(*) > 1"
            ") dup_counts"
        )
        result = cur.fetchone()
        duplicate_groups = (result[0] or 0) if result is not None else 0
        duplicate_rows = (result[1] or 0) if result is not None else 0

    return {
        "columns": columns,
        "null_rows": null_rows,
        "duplicate_groups": duplicate_groups,
        "duplicate_rows": duplicate_rows,
        "is_valid": null_rows == 0 and duplicate_groups == 0,
    }


def suggest_primary_keys(accdb_path: str, table: str, max_columns: int = PK_SUGGESTION_MAX_COLUMNS) -> List[Dict[str, object]]:
    """
    Evaluate potential primary keys by testing the first `max_columns` columns in order.
    Returns a list of evaluation dicts (one per attempt) for review.
    """
    cols, _, _ = describe_table(accdb_path, table, verbose=False)
    ordered = [c["name"] for c in cols]
    max_len = min(max_columns, len(ordered))

    attempts: List[Dict[str, object]] = []
    for length in range(1, max_len + 1):
        candidate = ordered[:length]
        result = evaluate_primary_key(accdb_path, table, candidate)
        attempts.append(result)
    return attempts


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
        is_binary = is_access_binary_type(c.get("type_name"))
        if single_int_pk and c["name"] == pk_cols[0]:
            defs.append(f"{name} INTEGER PRIMARY KEY")
        else:
            required = not c.get("nullable", True)
            if required and is_binary:
                logger.warning(
                    "%s.%s: relaxing NOT NULL constraint for binary column so values can be dropped",
                    table,
                    c["name"],
                )
            nn = " NOT NULL" if required and not is_binary else ""
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
                        preview: bool = True,
                        pk_override: List[str] | None = None):
    cols, pk_cols, fks = describe_table(accdb_path, table)
    if pk_override:
        pk_cols = pk_override
    if not cols:
        logger.warning(f"{table}: no columns found; skipping")
        return
    ddl = build_sqlite_create(table, cols, pk_cols, fks)
    if preview:
        logger.info(f"\n-- {table}")
        logger.info(ddl)
    with sqlite_connection(sqlite_path) as con:
        cur = con.cursor()
        if table_exists(sqlite_path, table):
            if not overwrite:
                logger.warn(f"SQLite table {table} already exists. Skipping (use overwrite=True to drop).")
                return
            cur.execute(f"DROP TABLE IF EXISTS {qident(table)}")
            con.commit()
        cur.execute(ddl)
        con.commit()
        logger.info(f"Created SQLite table {table}")
    assert same_columns(accdb_path, sqlite_path, table), "Columns do not match"

def sync_access_to_sqlite(accdb_path: str,
                          sqlite_path: str,
                          table: str,
                          chunk_size: int = 1000,
                          pk_override: List[str] | None = None):
    cols, pk_cols, _ = describe_table(accdb_path, table, verbose=False)
    if pk_override:
        pk_cols = pk_override
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
    binary_idx_set = {
        i for i, c in enumerate(cols)
        if is_access_binary_type(c.get("type_name"))
    }
    if binary_idx_set:
        logger.info(
            "Binary columns will be stored as NULLs in SQLite: %s",
            [cols[i]["name"] for i in binary_idx_set],
        )

    with access_connection(accdb_path) as acc_con, sqlite_connection(sqlite_path) as sqlite_con:
        acc_cur = acc_con.cursor()
        sqlite_cur = sqlite_con.cursor()
        acc_cur.execute(f"SELECT {quoted_cols} FROM {qident(table, 'access')}")
        while (rows := acc_cur.fetchmany(chunk_size)):
            if binary_idx_set:
                rows = [
                    tuple(None if idx in binary_idx_set else value for idx, value in enumerate(row))
                    for row in rows
                ]
            sqlite_cur.executemany(sql, rows)
        sqlite_con.commit()
    logger.info(f"Synchronized table {table}")

def sync_sqlite_to_access(sqlite_path: str,
                          accdb_path: str,
                          table: str,
                          chunk_size: int = 1000,
                          pk_override: List[str] | None = None):
    cols, pk_cols, _ = describe_table(accdb_path, table, verbose=False)
    if pk_override:
        pk_cols = pk_override
    if not pk_cols:
        raise ValueError(f"{table}: cannot sync without a primary key")

    col_names = [c["name"] for c in cols]
    quoted_cols = ", ".join(qident(c, "access") for c in col_names)
    placeholders = ", ".join(["?"] * len(col_names))
    set_clause = ", ".join(f"{qident(c, 'access')} = ?" for c in col_names if c not in pk_cols)
    where_clause = " AND ".join(f"{qident(c, 'access')} = ?" for c in pk_cols)

    with sqlite_connection(sqlite_path) as s_con, access_connection(accdb_path) as a_con:
        s_cur, a_cur = s_con.cursor(), a_con.cursor()
        s_cur.execute(f"SELECT {', '.join(qident(c, 'sqlite') for c in col_names)} FROM {qident(table, 'sqlite')}")
        while (rows := s_cur.fetchmany(chunk_size)):
            for r in rows:
                nonpk = [r[col_names.index(c)] for c in col_names if c not in pk_cols]
                pk = [r[col_names.index(c)] for c in pk_cols]
                a_cur.execute(f"UPDATE {qident(table, 'access')} SET {set_clause} WHERE {where_clause}", nonpk + pk)
                if not a_cur.rowcount:
                    a_cur.execute(f"INSERT INTO {qident(table, 'access')} ({quoted_cols}) VALUES ({placeholders})", r)
        a_con.commit()
    logger.info(f"Synchronized table {table} (SQLite → Access)")


if __name__ == "__main__":
    import os
    import argparse
    parser = argparse.ArgumentParser(description="Create Access table in SQLite")
    parser.add_argument("accdb", help="Path to Microsoft Access database")
    parser.add_argument("sqlite", nargs="?", help="Path to SQLite database (optional)")
    parser.add_argument("table", help="Table name")
    args = parser.parse_args()
    logger.info(f"Access Database: {args.accdb}")
    # SQLite db has the same name as the Access db, but different extension
    if args.sqlite is None:
        base, _ = os.path.splitext(args.accdb)
        args.sqlite = base + ".sqlite"
        logger.info(f"SQLite Database: {args.sqlite}")

    create_single_table(args.accdb, args.sqlite, args.table, True)
