#!/usr/bin/env python3
import logging

import pandas as pd

from .connect_access import connection as access_connection
from .connect_sqlite import connection as sqlite_connection
from .describe import describe_access, describe_sqlite

logger = logging.getLogger(__name__)

def list_tables(db_path: str):
    if db_path.endswith("accdb"):
        with access_connection(db_path) as con:
            cur = con.cursor()
            cur.execute("""
                SELECT TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA='PUBLIC' AND TABLE_TYPE='BASE TABLE'
                ORDER BY TABLE_NAME
            """)
            rows = cur.fetchall()
    elif db_path.endswith("sqlite"):
        with sqlite_connection(db_path) as con:
            cur = con.cursor()
            cur.execute("""
                SELECT name
                FROM sqlite_schema
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
            """)
            rows = cur.fetchall()
    else:
        raise ValueError("Unsupported database type; expected .accdb or .sqlite")
    return [r[0] for r in rows]

def describe_table(db_path, table, verbose = True):
    if db_path.endswith("accdb"):
        with access_connection(db_path) as con:
            cols, pk, fks = describe_access(con, table)
    elif db_path.endswith("sqlite"):
        with sqlite_connection(db_path) as con:
            cols, pk, fks = describe_sqlite(con, table)
    if verbose:
        print(f"Table: {table}")
        # Columns
        def type_disp(c):
            size = c.get("size")
            return f'{c.get("type_name","")}{f"({size})" if size is not None else ""}'

        name_w = max((len(c["name"]) for c in cols), default=4)
        type_w = max((len(type_disp(c)) for c in cols), default=4)
        print("Columns:")
        for c in cols:
            n = c["name"]
            t = type_disp(c)
            nn = "NULL" if c.get("nullable", True) else "NOT NULL"
            pk_mark = "*" if n in pk else " "
            print(f"  {pk_mark} {n:<{name_w}}  {t:<{type_w}}  {nn}")
        # Primary key
        if pk:
            print(f"Primary key: ({', '.join(pk)})")
        else:
            print("Primary key: <none>")
        # Foreign keys
        print("Foreign keys:")
        if fks:
            for fk in fks:
                fk_name = fk.get("name") or "<unnamed>"
                cols_s = ", ".join(fk.get("columns", []))
                ref_tbl = fk.get("ref_table", "<unknown>")
                ref_cols_s = ", ".join(fk.get("ref_columns", []))
                upd = fk.get("update_rule") or ""
                dele = fk.get("delete_rule") or ""
                extras = []
                if upd:
                    extras.append(f"ON UPDATE {upd}")
                if dele:
                    extras.append(f"ON DELETE {dele}")
                extras_s = f" [{' ,'.join(extras)}]" if extras else ""
                print(f"  - {fk_name}: ({cols_s}) -> {ref_tbl} ({ref_cols_s}){extras_s}")
        else:
            print("  <none>")

    return cols, pk, fks

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
    if t in ("OTHER", "BINARY", "VARBINARY", "IMAGE", "OLEOBJECT"):
        return "BLOB"
    return "TEXT"

def qident(name: str, dialect: str = 'sqlite') -> str:
    """
    Quote an SQL identifier safely for either Access or SQLite.
    """
    if dialect == "access":
        return f"[{name}]"
    elif dialect == "sqlite":
        return '"' + name.replace('"', '""') + '"'
    else:
        raise ValueError(f"Unknown dialect: {dialect}")

def table_exists(db_path: str, table: str) -> bool:
    table = table.strip()
    if not table:
        raise ValueError("Table name cannot be empty")
    if db_path.endswith(".accdb"):
        with access_connection(db_path) as con:
            cur = con.cursor()
            cur.execute("""
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE IN ('BASE TABLE', 'VIEW')
                  AND TABLE_NAME = ?
            """, [table])
            return cur.fetchone()[0] > 0
    elif db_path.endswith(".sqlite"):
        with sqlite_connection(db_path) as con:
            cur = con.cursor()
            cur.execute("""
                SELECT 1
                FROM sqlite_schema
                WHERE type IN ('table','view')
                  AND name = ?
            """, [table])
            return cur.fetchone() is not None
    else:
        raise ValueError("Unsupported database type; expected .accdb or .sqlite")

def same_columns(accdb_path: str, sqlite_path: str, table: str) -> bool:
    def sig(db):
        cols, _, _ = describe_table(db, table, verbose=False)
        return [c["name"] for c in cols]
    return sig(accdb_path) == sig(sqlite_path)

def print_table(db_path: str, table: str, subsample: int = 100):
    """Pretty-print up to `subsample` rows from a table in Access or SQLite."""
    if db_path.endswith("accdb"):
        con_func, dialect = access_connection, "access"
    elif db_path.endswith("sqlite"):
        con_func, dialect = sqlite_connection, "sqlite"
    else:
        raise ValueError("Unsupported database type; expected .accdb or .sqlite")

    with con_func(db_path) as con:
        cur = con.cursor()
        cur.execute(f"SELECT * FROM {qident(table, dialect)}")
        rows = cur.fetchmany(subsample)
        headers = [desc[0] for desc in cur.description]

    if not rows:
        logger.warn(f"Table '{table}' is empty.")
        return

    df = pd.DataFrame(rows, columns=headers)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 180)
    pd.set_option("display.max_rows", 20)

    _msg = f"\n-- Showing up to {subsample} rows from '{table}' ({len(rows)} retrieved) --\n" \
        + df.to_string(index=False)
    logger.info(_msg)

def blob_to_bytes(x):
    if x is None:
        return None
    if isinstance(x, (bytes, bytearray, memoryview)):
        return bytes(x)
    # Primitive java byte[] often works with bytes(); catch failures
    try:
        return bytes(x)
    except Exception:
        pass
    # java.sql.Blob-like (getBytes(start,len), length())
    if hasattr(x, "getBytes") and hasattr(x, "length"):
        try:
            return bytes(x.getBytes(1, int(x.length())))
        except Exception:
            pass
    # Multi-attachment: take first item
    if hasattr(x, "__iter__") and not isinstance(x, (str, bytes)):
        for it in x:
            return blob_to_bytes(it)
    return None
