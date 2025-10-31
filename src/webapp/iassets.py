#!/usr/bin/env python3

import os
import sqlite3
import logging
from datetime import datetime
import json
from typing import Dict, Iterable, List, Optional, Tuple

from ..db.recreate_from_access import (
    create_single_table,
    sync_access_to_sqlite,
    sync_sqlite_to_access,
)

try:
    import jpype  # type: ignore
except ImportError:  # pragma: no cover
    jpype = None


ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_DIR = os.path.join(ROOT_DIR, "data")
A1_DB_PATH = os.path.join(DATA_DIR, "A1ASSETS_DATABASE.sqlite")
A1_ACCESS_PATH = os.path.join(DATA_DIR, "A1ASSETS_DATABASE.accdb")

logger = logging.getLogger(__name__)


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
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS local_products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pickup_number INTEGER NOT NULL,
                pallet_number INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                serial_number TEXT,
                short_description TEXT,
                commodity TEXT,
                destination TEXT,
                description_raw TEXT,
                photos TEXT,
                created_by TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
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
                    SUM(COALESCE(quantity, 0)) AS total_quantity,
                    COUNT(DISTINCT COD_PALLET) AS pallet_count,
                    MAX(COALESCE(dt_update, dt, dt_processed, dt_pickup)) AS last_update
                FROM IASSETS
                WHERE pickup_number IS NOT NULL
                GROUP BY pickup_number
            )
            SELECT
                c.pickup_number AS PICKUP_NUMBER,
                COALESCE(agg.pallet_count, 0) AS TOTAL_PALLETS,
                COALESCE(agg.last_update, lp.created_at) AS DT_UPDATE
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
                COD_PALLET AS COD_PALLET,
                COUNT(*) AS TOTAL_ENTRIES,
                MAX(COALESCE(dt_update, dt, dt_processed, dt_pickup)) AS DT_UPDATE
            FROM IASSETS
            WHERE pickup_number = ? AND COD_PALLET IS NOT NULL
            GROUP BY COD_PALLET
            """,
            (pickup_number,),
        ).fetchall()

        local = conn.execute(
            """
            SELECT lp.pallet_number AS COD_PALLET,
                   lp.created_at,
                COALESCE(lp2.local_entries, 0) AS local_entries,
                COALESCE(lp2.local_dt, lp.created_at) AS local_dt
            FROM local_pallets lp
            LEFT JOIN (
                SELECT pallet_number,
                       COUNT(*) AS local_entries,
                       MAX(created_at) AS local_dt
                FROM local_products
                WHERE pickup_number = ?
                GROUP BY pallet_number
            ) lp2 ON lp2.pallet_number = lp.pallet_number
            WHERE lp.pickup_number = ?
            """,
            (pickup_number, pickup_number),
        ).fetchall()

    pallets: Dict[int, Dict[str, object]] = {}
    for row in aggregated:
        pallet = row["COD_PALLET"]
        if pallet is None:
            continue
        pallets[pallet] = {
            "COD_PALLET": pallet,
            "TOTAL_ENTRIES": row["TOTAL_ENTRIES"] or 0,
            "DT_UPDATE": row["DT_UPDATE"],
            "source": "iassets",
        }

    for row in local:
        pallet = row["COD_PALLET"]
        entry = pallets.get(pallet)
        if entry:
            entry.setdefault("source", "mixed")
            entry.setdefault("created_at", row["created_at"])
            entry["TOTAL_ENTRIES"] = (entry.get("TOTAL_ENTRIES") or 0) + (row["local_entries"] or 0)
            local_dt = row["local_dt"]
            if local_dt:
                current_dt = entry.get("DT_UPDATE")
                if not current_dt or local_dt > current_dt:
                    entry["DT_UPDATE"] = local_dt
        else:
            pallets[pallet] = {
                "COD_PALLET": pallet,
                "TOTAL_ENTRIES": row["local_entries"] or 0,
                "DT_UPDATE": row["local_dt"],
                "source": "local",
            }

    ordered = sorted(pallets.values(), key=lambda p: p["COD_PALLET"])
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


def create_local_product(
    *,
    pickup_number: int,
    pallet_number: int,
    quantity: int,
    serial_number: str,
    short_description: str,
    commodity: str,
    destination: str,
    description_raw: str,
    photos: List[str],
    created_by: Optional[str] = None,
) -> int:
    if quantity <= 0:
        raise ValueError("Quantity must be greater than zero.")

    with _connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO local_products (
                pickup_number,
                pallet_number,
                quantity,
                serial_number,
                short_description,
                commodity,
                destination,
                description_raw,
                photos,
                created_by,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                pickup_number,
                pallet_number,
                quantity,
                serial_number,
                short_description,
                commodity,
                destination,
                description_raw,
                json.dumps(photos),
                created_by,
                datetime.utcnow().isoformat(timespec="seconds"),
            ),
        )
        conn.commit()
        return int(cur.lastrowid)


def list_local_products(pickup_number: int, pallet_number: int) -> List[Dict[str, object]]:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT id,
                   quantity,
                   serial_number,
                   short_description,
                   commodity,
                   destination,
                   description_raw,
                   photos,
                   created_by,
                   created_at
            FROM local_products
            WHERE pickup_number = ? AND pallet_number = ?
            ORDER BY created_at DESC
            """,
            (pickup_number, pallet_number),
        ).fetchall()

    result = []
    for row in rows:
        data = dict(row)
        try:
            data["photos"] = json.loads(data.get("photos") or "[]")
        except json.JSONDecodeError:
            data["photos"] = []
        data["COD_ASSETS"] = None
        data["COD_ASSETS_SQLITE"] = row["id"]
        data["SN"] = row["serial_number"] or ""
        data["DESCRIPTION"] = row["short_description"] or row["description_raw"] or ""
        data["QUANTITY"] = row["quantity"]
        data.setdefault("ASSET_TAG", "")
        result.append(data)
    return result


_BASE_LOCAL_FIELDS = {
    "quantity": int,
    "serial_number": str,
    "short_description": str,
    "commodity": str,
    "destination": str,
}

_EDITABLE_PRODUCT_FIELDS = {
    **_BASE_LOCAL_FIELDS,
    **{key.upper(): value for key, value in _BASE_LOCAL_FIELDS.items()},
}


def update_local_product_field(
    *,
    product_id: int,
    pickup_number: int,
    pallet_number: int,
    field: str,
    raw_value: str,
) -> str:
    normalized = field.lower()
    if normalized not in _BASE_LOCAL_FIELDS:
        raise ValueError("Field is not editable.")

    with _connect() as conn:
        cur = conn.execute(
            """
            SELECT id FROM local_products
            WHERE id = ? AND pickup_number = ? AND pallet_number = ?
            """,
            (product_id, pickup_number, pallet_number),
        )
        if not cur.fetchone():
            raise ValueError("Product not found.")

    converter = _BASE_LOCAL_FIELDS[normalized]
    if converter is int:
        try:
            value = int(raw_value)
        except (TypeError, ValueError):
            raise ValueError("Quantity must be a positive integer.")
        if value <= 0:
            raise ValueError("Quantity must be a positive integer.")
    else:
        value = (raw_value or "").strip()

    with _connect() as conn:
        conn.execute(
            f"UPDATE local_products SET {normalized} = ? WHERE id = ?",
            (value, product_id),
        )
        conn.commit()

    return str(value)


_EDITABLE_IASSETS_FIELDS = {
    "SN": str,
    "ASSET_TAG": str,
    "DESCRIPTION": str,
    "QUANTITY": int,
}


def update_iassets_field(
    *,
    row_id: int,
    pickup_number: int,
    pallet_number: int,
    field: str,
    raw_value: str,
) -> str:
    if field not in _EDITABLE_IASSETS_FIELDS:
        raise ValueError("Field is not editable.")

    converter = _EDITABLE_IASSETS_FIELDS[field]
    if converter is int:
        try:
            value = int(raw_value)
        except (TypeError, ValueError):
            raise ValueError("Quantity must be a positive integer.")
        if value <= 0:
            raise ValueError("Quantity must be a positive integer.")
    else:
        value = (raw_value or "").strip()

    with _connect() as conn:
        cur = conn.execute(
            """
            SELECT ROWID FROM IASSETS
            WHERE ROWID = ? AND PICKUP_NUMBER = ? AND COALESCE(COD_PALLET, 0) = ?
            LIMIT 1
            """,
            (row_id, pickup_number, pallet_number),
        )
        if not cur.fetchone():
            raise ValueError("IASSETS entry not found.")

        conn.execute(
            f"UPDATE IASSETS SET {field} = ? WHERE ROWID = ?",
            (value, row_id),
        )
        conn.commit()

    return str(value)


def sync_iassets_with_access(
    *,
    access_path: Optional[str] = None,
    sqlite_path: Optional[str] = None,
    pk_override: Optional[List[str]] = None,
) -> None:
    access_path = access_path or os.environ.get("A1ASSETS_ACCESS_PATH", A1_ACCESS_PATH)
    sqlite_path = sqlite_path or os.environ.get("A1ASSETS_SQLITE_PATH", A1_DB_PATH)
    pk_override = pk_override or ["COD_IASSETS"]

    if not os.path.isfile(sqlite_path):
        logger.warning("SQLite path '%s' not found; skipping IASSETS sync.", sqlite_path)
        return

    if not os.path.isfile(access_path):
        logger.warning("Access path '%s' not found; skipping IASSETS sync.", access_path)
        return

    table = "IASSETS"
    logger.info("Preparing IASSETS table sync from %s", access_path)
    try:
        create_single_table(
            access_path,
            sqlite_path,
            table,
            overwrite=False,
            preview=False,
            pk_override=pk_override,
        )
    except Exception:
        logger.debug("create_single_table for %s skipped or failed", table, exc_info=True)

    try:
        logger.info("Loading IASSETS from Access → SQLite")
        sync_access_to_sqlite(
            access_path,
            sqlite_path,
            table,
            pk_override=pk_override,
        )

        logger.info("Pushing IASSETS from SQLite → Access")
        sync_sqlite_to_access(
            sqlite_path,
            access_path,
            table,
            pk_override=pk_override,
        )
    except Exception as exc:  # pragma: no cover - defensive
        if jpype and isinstance(exc, jpype.JVMNotRunning):
            logger.warning("JVM not running; skipping IASSETS synchronization.")
            return
        raise

    logger.info(
        "IASSETS sync complete (access=%s, sqlite=%s, pk=%s)",
        access_path,
        sqlite_path,
        pk_override,
    )


def sync_local_products_to_iassets(
    pickup_number: int,
    pallet_number: int,
) -> int:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT id, quantity, serial_number, short_description, commodity, destination, description_raw
            FROM local_products
            WHERE pickup_number = ? AND pallet_number = ?
            ORDER BY created_at
            """,
            (pickup_number, pallet_number),
        ).fetchall()

        if not rows:
            return 0

        insert_sql = (
            """
            INSERT INTO IASSETS (
                PICKUP_NUMBER,
                COD_PALLET,
                QUANTITY,
                DESCRIPTION,
                SN,
                COD_ASSETS_SQLITE
            ) VALUES (?, ?, ?, ?, ?, ?)
            """
        )

        for row in rows:
            description = row["short_description"] or row["description_raw"] or ""
            serial = row["serial_number"] or None
            conn.execute(
                insert_sql,
                (
                    pickup_number,
                    pallet_number,
                    row["quantity"],
                    description,
                    serial,
                    row["id"],
                ),
            )

        ids = [row["id"] for row in rows]
        placeholders = ",".join("?" for _ in ids)
        conn.execute(
            f"DELETE FROM local_products WHERE id IN ({placeholders})",
            ids,
        )
        conn.commit()

    return len(rows)
    return result


def update_local_product_photos(product_id: int, photos: List[str]) -> None:
    with _connect() as conn:
        conn.execute(
            "UPDATE local_products SET photos = ? WHERE id = ?",
            (json.dumps(photos), product_id),
        )
        conn.commit()


def delete_local_product(product_id: int) -> None:
    with _connect() as conn:
        conn.execute("DELETE FROM local_products WHERE id = ?", (product_id,))
        conn.commit()
