#!/usr/bin/env python3
import os
import logging
import glob
from contextlib import contextmanager
import platform
import sqlite3


logging.basicConfig(
    format="%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


def find_ucanaccess_jars():
    env = os.environ.get("UCANACCESS_CLASSPATH", "").strip()
    if not env:
        raise EnvironmentError("Undefined environment variable 'UCANACCESS_CLASSPATH'")
    glob.glob(os.path.join(env, "*.jar"))
    return [*glob.glob(os.path.join(env, "*.jar")), *glob.glob(os.path.join(env, "lib", "*.jar"))]


def _connect_via_ucanaccess(accdb_path: str, new_db: str | None = None):
    import jaydebeapi  # local import to avoid hard dep in callers

    jars = find_ucanaccess_jars()
    if not jars:
        raise RuntimeError("UCanAccess jars not found. Set UCANACCESS_CLASSPATH to the folder with /.jar files.")
    url = f"jdbc:ucanaccess://{os.path.abspath(accdb_path)}"
    if new_db:
        url += f";newdatabaseversion={new_db}"
    logger.debug("Connecting to %s via UCanAccess", url)
    driver = "net.ucanaccess.jdbc.UcanaccessDriver"
    return jaydebeapi.connect(driver, url, jars=jars)


def _connect_via_pyodbc(accdb_path: str):
    try:
        import pyodbc  # type: ignore[import]
    except ImportError as exc:
        raise RuntimeError(
            "pyodbc is required for Access connections on Windows. "
            "Install it with `pip install pyodbc`."
        ) from exc

    conn_str = (
        r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
        rf"Dbq={os.path.abspath(accdb_path)};"
    )
    logger.debug("Connecting to %s via pyodbc", accdb_path)
    return pyodbc.connect(conn_str, autocommit=True)


def connect_access(accdb_path: str, new_db: str | None = None):
    """
    Connect to Access .accdb. Uses native ACE driver on Windows and UCanAccess elsewhere.
    """
    if platform.system() == "Windows":
        return _connect_via_pyodbc(accdb_path)
    return _connect_via_ucanaccess(accdb_path, new_db)


@contextmanager
def connection(accdb_path: str, new_db: str = None):
    conn = connect_access(accdb_path, new_db)
    try:
        yield conn
    finally:
        conn.close()


if __name__ == "__main__":
    # Basic test connecting to a sample .accdb file
    import sys
    accdb = sys.argv[1] if len(sys.argv) > 1 else None
    if not accdb:
        raise SystemExit("Usage: python connect_access.py path/to/db.accdb")
    with connection(accdb) as conn:
        cur = conn.cursor()
        logger.info("Connected to %s.", accdb)
        cur.execute("""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'PUBLIC'
            ORDER BY TABLE_NAME;
        """)
        logger.info("Tables:")
        for (name,) in cur.fetchall():
            logger.info("%s", name)
