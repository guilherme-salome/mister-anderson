import os
import logging

from .connect_access import connection


logging.basicConfig(
    format="%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


def accdb(path: str, version: str = "V2010"):
    with connection(path, version) as conn:
        logger.info(f"Creating Access database at {path}")
        cur = conn.cursor()
        # Create a couple of sample tables
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


if __name__ == "__main__":
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else os.path.join("data", "sample.accdb")
    accdb(path)
    print(f"Created sample file at {path}")
