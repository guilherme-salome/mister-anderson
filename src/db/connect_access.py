#!/usr/bin/env python3
import os
import sqlite3
import logging
import glob
from contextlib import contextmanager


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


def connect_access(accdb_path: str, new_db: str = None):
    """
    Connect to Access .accdb via UCanAccess using jaydebeapi.
    Requires:
      - pip install jaydebeapi
      - Java installed
      - UCanAccess jars visible (set UCANACCESS_CLASSPATH to folder with jars)
    If new_db is set, will create a new database using the version specified in new_db (e.g., "V2010").
    """
    import jaydebeapi  # local import to avoid hard dep in callers
    jars = find_ucanaccess_jars()
    if not jars:
        raise RuntimeError("UCanAccess jars not found. Set UCANACCESS_CLASSPATH to the folder with /.jar files.")
    url = f"jdbc:ucanaccess://{os.path.abspath(accdb_path)}" + \
        (f";newdatabaseversion={new_db}" if new_db else "")
    logger.info(f"Connection URL: {url}")
    driver = "net.ucanaccess.jdbc.UcanaccessDriver"
    conn = jaydebeapi.connect(driver, url, jars=jars)
    logger.info(f"Connected to {accdb_path}")
    return conn


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
        print(f"Connected to {accdb}.")
        cur.execute("""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'PUBLIC'
            ORDER BY TABLE_NAME;
        """)
        print("Tables:")
        for (name,) in cur.fetchall():
            print(name)
