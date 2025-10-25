#!/usr/bin/env python3

import os
import sqlite3
from typing import Dict, Iterable, List, Optional


ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_DIR = os.path.join(ROOT_DIR, "data")
A1_DB_PATH = os.path.join(DATA_DIR, "A1ASSETS_DATABASE.sqlite")


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(A1_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def list_recent_pickups(limit: int = 50) -> List[Dict[str, object]]:
    query = (
        """
        SELECT
            pickup_number AS pickup,
            COUNT(*) AS item_count,
            SUM(COALESCE(quantity, 0)) AS total_quantity,
            MAX(COALESCE(dt_update, dt, dt_processed, dt_pickup)) AS last_update
        FROM IASSETS
        WHERE pickup_number IS NOT NULL
        GROUP BY pickup_number
        ORDER BY pickup_number DESC
        LIMIT ?
        """
    )
    with _connect() as conn:
        rows = conn.execute(query, (limit,)).fetchall()
        return [dict(row) for row in rows]


def fetch_pickup_items(pickup_number: int, limit: Optional[int] = None) -> List[Dict[str, object]]:
    columns = ["COD_PALLET", "COD_ASSETS", "QUANTITY", "DESCRIPTION"]
    select_clause = ", ".join(columns + ["ROWID as row_id"])
    query = (
        f"SELECT {select_clause} FROM IASSETS "
        "WHERE pickup_number = ? ORDER BY COALESCE(COD_PALLET, 0), ROWID"
    )
    params = [pickup_number]
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

