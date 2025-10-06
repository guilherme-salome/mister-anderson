import os
import logging

from .connect_access import connection as access_connection
from .connect_sqlite import connection as sqlite_connection

logging.basicConfig(
    format="%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


def drop_object_if_exists(cur, name: str):
    # Is there a table/view with this name?
    cur.execute("""
        SELECT TABLE_TYPE
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA='PUBLIC' AND TABLE_NAME=UPPER(?)
    """, [name])
    row = cur.fetchone()
    if not row:
        return False
    obj_type = row[0]
    if obj_type == "VIEW":
        cur.execute(f"DROP VIEW {name}")
    else:
        cur.execute(f"DROP TABLE {name}")
    return True


def accdb(path: str, version: str = "V2010"):
    with access_connection(path, version) as conn:
        logger.info(f"Creating Access database at {path}")
        cur = conn.cursor()
        # Create a couple of sample tables
        drop_object_if_exists(cur, "People")
        drop_object_if_exists(cur, "Orders")
        conn.commit()
        cur.execute("""
            CREATE TABLE People (
                ID AUTOINCREMENT PRIMARY KEY,
                Name TEXT(100),
                Email TEXT(255)
            )
        """)
        cur.execute("""
            CREATE TABLE Orders (
                ID AUTOINCREMENT PRIMARY KEY,
                PersonID INTEGER,
                Amount DOUBLE,
                OrderDate DATETIME
            )
        """)
        # Seed data
        cur.executemany(
            "INSERT INTO People (Name, Email) VALUES (?, ?)",
            [
                ("Alice", "alice@example.com"),
                ("Bob", "bob@example.com"),
                ("Carol", "carol@example.com"),
            ],
        )
        cur.executemany(
            "INSERT INTO Orders (PersonID, Amount, OrderDate) VALUES (?, ?, ?)",
            [
                (1, 120.50, "2024-06-01 10:00:00"),
                (2,  75.00, "2024-06-02 14:30:00"),
                (1, 250.00, "2024-06-03 09:15:00"),
            ],
        )
        conn.commit()


def sqlite(path: str):
    with sqlite_connection(path) as conn:
        logger.info(f"Creating SQLite database at {path}")
        cur = conn.cursor()
        # Recreate tables
        cur.executescript("""
            DROP TABLE IF EXISTS Orders;
            DROP TABLE IF EXISTS People;

            CREATE TABLE People (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT,
                Email TEXT
            );

            CREATE TABLE Orders (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                PersonID INTEGER NOT NULL,
                Amount REAL,
                OrderDate TEXT,
                FOREIGN KEY (PersonID) REFERENCES People(ID)
            );
        """)
        # Seed data
        cur.executemany(
            "INSERT INTO People (Name, Email) VALUES (?, ?)",
            [
                ("Alice", "alice@example.com"),
                ("Bob", "bob@example.com"),
                ("Carol", "carol@example.com"),
            ],
        )
        cur.executemany(
            "INSERT INTO Orders (PersonID, Amount, OrderDate) VALUES (?, ?, ?)",
            [
                (1, 120.50, "2024-06-01 10:00:00"),
                (2,  75.00, "2024-06-02 14:30:00"),
                (1, 250.00, "2024-06-03 09:15:00"),
            ],
        )
        conn.commit()


if __name__ == "__main__":
    accdb(os.path.join("data", "sample.accdb"))
    sqlite(os.path.join("data", "sample.sqlite"))
