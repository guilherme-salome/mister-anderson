#!/usr/bin/env python3

import os
import sqlite3
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple


ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_DIR = os.path.join(ROOT_DIR, "data")
A1_DB_PATH = os.path.join(DATA_DIR, "A1ASSETS_DATABASE.sqlite")


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(A1_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_support_tables() -> None:
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS local_pickups (
                pickup_number INTEGER PRIMARY KEY,
                created_by TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS local_pallets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pickup_number INTEGER NOT NULL,
                pallet_number INTEGER NOT NULL,
                created_by TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(pickup_number, pallet_number)
            )
            """
        )
        conn.commit()


def pickup_exists(pickup_number: int) -> bool:
    with _connect() as conn:
        cur = conn.execute(
            "SELECT 1 FROM IASSETS WHERE pickup_number = ? LIMIT 1",
            (pickup_number,),
        )
        if cur.fetchone():
            return True
        cur = conn.execute(
            "SELECT 1 FROM local_pickups WHERE pickup_number = ? LIMIT 1",
            (pickup_number,),
        )
        return cur.fetchone() is not None


def create_pickup(pickup_number: int, *, created_by: Optional[str] = None) -> None:
    if pickup_number <= 0:
        raise ValueError("Pickup number must be a positive integer.")
    with _connect() as conn:
        cur = conn.execute(
            "SELECT 1 FROM IASSETS WHERE pickup_number = ? LIMIT 1",
            (pickup_number,),
        )
        if cur.fetchone():
            raise ValueError("Pickup already exists in IASSETS.")
        cur = conn.execute(
            "SELECT 1 FROM local_pickups WHERE pickup_number = ? LIMIT 1",
            (pickup_number,),
        )
        if cur.fetchone():
            raise ValueError("Pickup already exists.")
        conn.execute(
            "INSERT INTO local_pickups (pickup_number, created_by, created_at) VALUES (?, ?, ?)",
            (pickup_number, created_by, datetime.utcnow().isoformat(timespec="seconds")),
        )
        conn.commit()


def list_pickups(
    *,
    page: int = 1,
    page_size: int = 25,
    pickup_query: Optional[int] = None,
) -> Tuple[List[Dict[str, object]], int]:
    """Return pickups for the requested page along with the total count."""

    page = max(page, 1)
    offset = (page - 1) * page_size

    with _connect() as conn:
        total = conn.execute(
            """
            WITH combined AS (
                SELECT DISTINCT pickup_number FROM IASSETS WHERE pickup_number IS NOT NULL
                UNION
                SELECT pickup_number FROM local_pickups
            )
            SELECT COUNT(*) FROM combined
            WHERE (? IS NULL) OR pickup_number = ?
            """,
            (pickup_query, pickup_query),
        ).fetchone()[0]

        rows = conn.execute(
            """
            WITH combined AS (
                SELECT DISTINCT pickup_number FROM IASSETS WHERE pickup_number IS NOT NULL
                UNION
                SELECT pickup_number FROM local_pickups
            ),
            agg AS (
                SELECT
                    pickup_number,
                    COUNT(*) AS item_count,
                    SUM(COALESCE(quantity, 0)) AS total_quantity,
                    MAX(COALESCE(dt_update, dt, dt_processed, dt_pickup)) AS last_update
                FROM IASSETS
                WHERE pickup_number IS NOT NULL
                GROUP BY pickup_number
            )
            SELECT
                c.pickup_number AS pickup,
                COALESCE(agg.item_count, 0) AS item_count,
                COALESCE(agg.total_quantity, 0) AS total_quantity,
                COALESCE(agg.last_update, lp.created_at) AS last_update
            FROM combined c
            LEFT JOIN agg ON agg.pickup_number = c.pickup_number
            LEFT JOIN local_pickups lp ON lp.pickup_number = c.pickup_number
            WHERE (? IS NULL) OR c.pickup_number = ?
            ORDER BY c.pickup_number DESC
            LIMIT ? OFFSET ?
            """,
            (pickup_query, pickup_query, page_size, offset),
        ).fetchall()

    pickups = [dict(row) for row in rows]
    return pickups, total


def create_pallet(
    pickup_number: int,
    pallet_number: int,
    *,
    created_by: Optional[str] = None,
) -> None:
    if pallet_number <= 0:
        raise ValueError("Pallet number must be a positive integer.")
    if not pickup_exists(pickup_number):
        raise ValueError("Pickup does not exist.")

    with _connect() as conn:
        cur = conn.execute(
            """
            SELECT 1
            FROM IASSETS
            WHERE pickup_number = ? AND COALESCE(COD_PALLET, 0) = ?
            LIMIT 1
            """,
            (pickup_number, pallet_number),
        )
        if cur.fetchone():
            raise ValueError("Pallet already exists in IASSETS.")

        cur = conn.execute(
            """
            SELECT 1
            FROM local_pallets
            WHERE pickup_number = ? AND pallet_number = ?
            LIMIT 1
            """,
            (pickup_number, pallet_number),
        )
        if cur.fetchone():
            raise ValueError("Pallet already exists.")

        conn.execute(
            """
            INSERT INTO local_pallets (pickup_number, pallet_number, created_by, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (pickup_number, pallet_number, created_by, datetime.utcnow().isoformat(timespec="seconds")),
        )
        conn.commit()


def list_pallets(pickup_number: int) -> List[Dict[str, object]]:
    with _connect() as conn:
        aggregated = conn.execute(
            """
            SELECT
                COD_PALLET AS pallet,
                COUNT(*) AS item_count,
                SUM(COALESCE(quantity, 0)) AS total_quantity,
                MAX(COALESCE(dt_update, dt, dt_processed, dt_pickup)) AS last_update
            FROM IASSETS
            WHERE pickup_number = ? AND COD_PALLET IS NOT NULL
            GROUP BY COD_PALLET
            """,
            (pickup_number,),
        ).fetchall()

        local = conn.execute(
            """
            SELECT pallet_number AS pallet, created_at
            FROM local_pallets
            WHERE pickup_number = ?
            """,
            (pickup_number,),
        ).fetchall()

    pallets: Dict[int, Dict[str, object]] = {}
    for row in aggregated:
        pallet = row["pallet"]
        if pallet is None:
            continue
        pallets[pallet] = {
            "pallet": pallet,
            "item_count": row["item_count"] or 0,
            "total_quantity": row["total_quantity"] or 0,
            "last_update": row["last_update"],
            "source": "iassets",
        }

    for row in local:
        pallet = row["pallet"]
        entry = pallets.get(pallet)
        if entry:
            # combines metadata if pallet also has IASSETS entries
            entry.setdefault("source", "mixed")
            entry.setdefault("created_at", row["created_at"])
        else:
            pallets[pallet] = {
                "pallet": pallet,
                "item_count": 0,
                "total_quantity": 0,
                "last_update": row["created_at"],
                "source": "local",
            }

    ordered = sorted(pallets.values(), key=lambda p: p["pallet"])
    return ordered


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


def fetch_pallet_items(pickup_number: int, pallet_number: int) -> List[Dict[str, object]]:
    columns = [
        "COD_ASSETS",
        "COD_ASSETS_SQLITE",
        "QUANTITY",
        "DESCRIPTION",
        "SN",
        "ASSET_TAG",
    ]
    select_clause = ", ".join(columns + ["ROWID AS row_id"])
    query = (
        f"SELECT {select_clause} FROM IASSETS "
        "WHERE pickup_number = ? AND COALESCE(COD_PALLET, 0) = ?"
        " ORDER BY ROWID"
    )
    with _connect() as conn:
        rows = conn.execute(query, (pickup_number, pallet_number)).fetchall()
    return [dict(row) for row in rows]
