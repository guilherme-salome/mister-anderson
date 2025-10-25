#!/usr/bin/env python3

import os
import sqlite3
from typing import Dict, Iterable, List, Optional, Tuple


ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_DIR = os.path.join(ROOT_DIR, "data")
A1_DB_PATH = os.path.join(DATA_DIR, "A1ASSETS_DATABASE.sqlite")


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(A1_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def list_pickups(
    *,
    page: int = 1,
    page_size: int = 25,
    pickup_query: Optional[int] = None,
) -> Tuple[List[Dict[str, object]], int]:
    """Return pickups for the requested page along with the total count."""

    base_where = "pickup_number IS NOT NULL"
    params: List = []
    if pickup_query is not None:
        base_where += " AND pickup_number = ?"
        params.append(pickup_query)

    count_sql = f"SELECT COUNT(DISTINCT pickup_number) FROM IASSETS WHERE {base_where}"
    data_sql = (
        """
        SELECT
            pickup_number AS pickup,
            COUNT(*) AS item_count,
            SUM(COALESCE(quantity, 0)) AS total_quantity,
            MAX(COALESCE(dt_update, dt, dt_processed, dt_pickup)) AS last_update
        FROM IASSETS
        WHERE {where}
        GROUP BY pickup_number
        ORDER BY pickup_number DESC
        LIMIT ? OFFSET ?
        """
    ).format(where=base_where)

    page = max(page, 1)
    offset = (page - 1) * page_size

    with _connect() as conn:
        total = conn.execute(count_sql, params).fetchone()[0]
        query_params = params + [page_size, offset]
        rows = conn.execute(data_sql, query_params).fetchall()

    pickups = [dict(row) for row in rows]
    return pickups, total


def fetch_pickup_items(pickup_number: int, limit: Optional[int] = None) -> List[Dict[str, object]]:
    columns = ["COD_PALLET", "COD_ASSETS", "COD_ASSETS_SQLITE", "QUANTITY", "DESCRIPTION"]
    select_clause = ", ".join(columns + ["ROWID AS row_id"])
    query = (
        f"SELECT {select_clause} FROM IASSETS "
        "WHERE pickup_number = ? ORDER BY COALESCE(COD_PALLET, 0), ROWID"
    )
    params: List = [pickup_number]
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)

    with _connect() as conn:
        rows = conn.execute(query, params).fetchall()

    result = []
    for row in rows:
        data = dict(row)
        data.setdefault("DESCRIPTION", "")
        result.append(data)
    return result
