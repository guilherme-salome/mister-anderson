#!/usr/bin/env python3

import os
import logging
from contextlib import contextmanager
from datetime import datetime
import json
from threading import local
from typing import Any, Dict, List, Optional, Sequence, Tuple

from ..db.connect_access import connect_access


ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_DIR = os.path.join(ROOT_DIR, "data")
A1_ACCESS_PATH = os.path.join(DATA_DIR, "A1ASSETS_DATABASE.accdb")

logger = logging.getLogger(__name__)

_ACCESS_STATE = local()


@contextmanager
def _connect_access(path: Optional[str] = None):
    db_path = path or os.environ.get("A1ASSETS_ACCESS_PATH", A1_ACCESS_PATH)
    conn = getattr(_ACCESS_STATE, "conn", None)
    conn_path = getattr(_ACCESS_STATE, "path", None)

    if conn is not None:
        if conn_path != db_path:
            try:
                conn.close()
            except Exception:
                pass
            conn = None
    if conn is not None:
        try:
            if conn.jconn.isClosed():  # type: ignore[attr-defined]
                conn = None
        except AttributeError:
            pass
        except Exception:
            conn = None

    if conn is None:
        conn = connect_access(db_path)
        _ACCESS_STATE.conn = conn
        _ACCESS_STATE.path = db_path

    try:
        yield conn
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        if getattr(_ACCESS_STATE, "conn", None) is conn:
            _ACCESS_STATE.conn = None
            _ACCESS_STATE.path = None
        raise


def _rows_to_dicts(columns: Sequence[str], rows: Sequence[Sequence[Any]]) -> List[Dict[str, object]]:
    return [dict(zip(columns, row)) for row in rows]


def _fetch_access(query: str, params: Sequence[object] | None = None) -> List[Dict[str, object]]:
    with _connect_access() as conn:
        cur = conn.cursor()
        try:
            cur.execute(query, params or [])
            columns = [desc[0] for desc in cur.description] if cur.description else []
            results = cur.fetchall()
        finally:
            cur.close()
    return _rows_to_dicts(columns, results)


def _normalize_timestamp(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, str):
        trimmed = value.strip()
        if not trimmed:
            return None
        candidate = trimmed.replace("T", " ")
        try:
            parsed = datetime.fromisoformat(candidate)
            return parsed.isoformat(sep=" ")
        except ValueError:
            return trimmed
    try:
        # Attempt to coerce java.sql.Timestamp style objects
        text = str(value)
        candidate = text.replace("T", " ")
        parsed = datetime.fromisoformat(candidate)
        return parsed.isoformat(sep=" ")
    except Exception:
        return str(value)


def _table_exists_access(table_name: str) -> bool:
    normalized = table_name.upper()
    rows = _fetch_access(
        """
        SELECT COUNT(*) AS total
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'PUBLIC'
          AND UCASE(TABLE_NAME) = ?
        """,
        [normalized],
    )
    if not rows:
        return False
    return bool(rows[0].get("total") or rows[0].get("TOTAL"))


def ensure_support_tables() -> None:
    tables = {
        "LOCAL_PICKUPS": """
            CREATE TABLE LOCAL_PICKUPS (
                PICKUP_NUMBER LONG NOT NULL,
                CREATED_BY TEXT(255),
                CREATED_AT TEXT(32),
                CONSTRAINT PK_LOCAL_PICKUPS PRIMARY KEY (PICKUP_NUMBER)
            )
        """,
        "LOCAL_PALLETS": """
            CREATE TABLE LOCAL_PALLETS (
                ID AUTOINCREMENT PRIMARY KEY,
                PICKUP_NUMBER LONG NOT NULL,
                PALLET_NUMBER LONG NOT NULL,
                CREATED_BY TEXT(255),
                CREATED_AT TEXT(32),
                CONSTRAINT UQ_LOCAL_PALLETS UNIQUE (PICKUP_NUMBER, PALLET_NUMBER)
            )
        """,
        "LOCAL_PRODUCTS": """
            CREATE TABLE LOCAL_PRODUCTS (
                ID AUTOINCREMENT PRIMARY KEY,
                PICKUP_NUMBER LONG NOT NULL,
                PALLET_NUMBER LONG NOT NULL,
                QUANTITY LONG NOT NULL,
                SERIAL_NUMBER TEXT(255),
                SHORT_DESCRIPTION TEXT(255),
                COMMODITY TEXT(255),
                DESTINATION TEXT(255),
                DESCRIPTION_RAW MEMO,
                PHOTOS MEMO,
                CREATED_BY TEXT(255),
                CREATED_AT TEXT(32)
            )
        """,
    }

    for table, ddl in tables.items():
        if _table_exists_access(table):
            continue
        with _connect_access() as conn:
            cur = conn.cursor()
            try:
                cur.execute(ddl)
                conn.commit()
            finally:
                cur.close()


def pickup_exists(pickup_number: int) -> bool:
    rows = _fetch_access(
        """
        SELECT COUNT(*) AS total
        FROM IASSETS
        WHERE PICKUP_NUMBER = ?
        """,
        [pickup_number],
    )
    if rows and (rows[0].get("total") or rows[0].get("TOTAL")):
        count = rows[0].get("total") or rows[0].get("TOTAL") or 0
        if count:
            return True

    local_rows = _fetch_access(
        """
        SELECT 1 FROM LOCAL_PICKUPS WHERE PICKUP_NUMBER = ?
        """,
        [pickup_number],
    )
    return bool(local_rows)


def create_pickup(pickup_number: int, *, created_by: Optional[str] = None) -> None:
    if pickup_number <= 0:
        raise ValueError("Pickup number must be a positive integer.")
    rows = _fetch_access(
        """
        SELECT COUNT(*) AS total
        FROM IASSETS
        WHERE PICKUP_NUMBER = ?
        """,
        [pickup_number],
    )
    count = 0
    if rows:
        count = rows[0].get("total") or rows[0].get("TOTAL") or 0
    if count:
        raise ValueError("Pickup already exists in IASSETS.")

    local_existing = _fetch_access(
        """
        SELECT 1 FROM LOCAL_PICKUPS WHERE PICKUP_NUMBER = ?
        """,
        [pickup_number],
    )
    if local_existing:
        raise ValueError("Pickup already exists.")

    with _connect_access() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO LOCAL_PICKUPS (PICKUP_NUMBER, CREATED_BY, CREATED_AT)
                VALUES (?, ?, ?)
                """,
                (pickup_number, created_by, datetime.utcnow().isoformat(timespec="seconds")),
            )
            conn.commit()
        finally:
            cur.close()


def list_pickups(
    *,
    page: int = 1,
    page_size: int = 25,
    pickup_query: Optional[int] = None,
) -> Tuple[List[Dict[str, object]], int]:
    """Return pickups for the requested page along with the total count."""

    page = max(page, 1)
    pickups: Dict[int, Dict[str, object]] = {}

    where_clause = ""
    params: List[object] = []
    if pickup_query is not None:
        where_clause = "WHERE p.PICKUP_NUMBER = ?"
        params.append(pickup_query)

    access_rows = _fetch_access(
        f"""
        SELECT
            p.PICKUP_NUMBER,
            COALESCE(pc.PALLET_COUNT, 0) AS PALLET_COUNT,
            mv.MAX_DT
        FROM (
            SELECT PICKUP_NUMBER
            FROM IASSETS
            WHERE PICKUP_NUMBER IS NOT NULL
            GROUP BY PICKUP_NUMBER
        ) AS p
        LEFT JOIN (
            SELECT inner_tbl.PICKUP_NUMBER, COUNT(*) AS PALLET_COUNT
            FROM (
                SELECT PICKUP_NUMBER, COD_PALLET
                FROM IASSETS
                WHERE COD_PALLET IS NOT NULL
                GROUP BY PICKUP_NUMBER, COD_PALLET
            ) AS inner_tbl
            GROUP BY inner_tbl.PICKUP_NUMBER
        ) AS pc
            ON p.PICKUP_NUMBER = pc.PICKUP_NUMBER
        LEFT JOIN (
            SELECT PICKUP_NUMBER,
                   MAX(COALESCE(dt_update, dt, dt_processed, dt_pickup)) AS MAX_DT
            FROM IASSETS
            WHERE PICKUP_NUMBER IS NOT NULL
            GROUP BY PICKUP_NUMBER
        ) AS mv
            ON p.PICKUP_NUMBER = mv.PICKUP_NUMBER
        {where_clause}
        ORDER BY p.PICKUP_NUMBER DESC
        """,
        params,
    )

    for row in access_rows:
        pickup_number = row.get("PICKUP_NUMBER") or row.get("pickup_number")
        if pickup_number is None:
            continue
        try:
            pickup_key = int(pickup_number)
        except (TypeError, ValueError):
            continue
        total_pallets = row.get("PALLET_COUNT") or row.get("pallet_count") or 0
        dt_update = _normalize_timestamp(row.get("MAX_DT") or row.get("max_dt"))
        pickups[pickup_key] = {
            "PICKUP_NUMBER": pickup_key,
            "TOTAL_PALLETS": int(total_pallets or 0),
            "DT_UPDATE": dt_update,
            "source": "iassets",
        }

    local_where = "WHERE lp.PICKUP_NUMBER = ?" if pickup_query is not None else ""
    local_params: List[object] = [pickup_query] if pickup_query is not None else []

    local_rows = _fetch_access(
        f"""
        SELECT
            lp.PICKUP_NUMBER,
            lp.CREATED_AT,
            COALESCE(lpagg.PALLET_COUNT, 0) AS PALLET_COUNT,
            lpagg.LAST_PALLET_CREATED,
            prod.LAST_PRODUCT
        FROM LOCAL_PICKUPS lp
        LEFT JOIN (
            SELECT inner_lp.PICKUP_NUMBER,
                   COUNT(*) AS PALLET_COUNT,
                   MAX(inner_lp.CREATED_AT) AS LAST_PALLET_CREATED
            FROM (
                SELECT PICKUP_NUMBER, PALLET_NUMBER, MAX(CREATED_AT) AS CREATED_AT
                FROM LOCAL_PALLETS
                GROUP BY PICKUP_NUMBER, PALLET_NUMBER
            ) AS inner_lp
            GROUP BY inner_lp.PICKUP_NUMBER
        ) AS lpagg
            ON lp.PICKUP_NUMBER = lpagg.PICKUP_NUMBER
        LEFT JOIN (
            SELECT PICKUP_NUMBER, MAX(CREATED_AT) AS LAST_PRODUCT
            FROM LOCAL_PRODUCTS
            GROUP BY PICKUP_NUMBER
        ) AS prod
            ON lp.PICKUP_NUMBER = prod.PICKUP_NUMBER
        {local_where}
        """,
        local_params,
    )

    def _row_pickup_value(row: Dict[str, object]) -> Optional[int]:
        value = row.get("PICKUP_NUMBER") or row.get("pickup_number")
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    for row in local_rows:
        pickup_val = _row_pickup_value(row)
        if pickup_val is None:
            continue
        pickup_key = pickup_val
        entry = pickups.get(pickup_key)
        local_created = _normalize_timestamp(row.get("CREATED_AT") or row.get("created_at"))
        last_pallet_created = _normalize_timestamp(row.get("LAST_PALLET_CREATED") or row.get("last_pallet_created"))
        last_product = _normalize_timestamp(row.get("LAST_PRODUCT") or row.get("last_product"))
        candidates = [
            local_created,
            last_pallet_created,
            last_product,
        ]
        candidates = [value for value in candidates if value]
        latest_local_dt = max(candidates) if candidates else local_created

        if entry:
            current_dt = entry.get("DT_UPDATE")
            if latest_local_dt and (current_dt is None or latest_local_dt > current_dt):
                entry["DT_UPDATE"] = latest_local_dt
            if entry.get("source") == "iassets":
                entry["source"] = "mixed"
        else:
            entry = {
                "PICKUP_NUMBER": pickup_key,
                "TOTAL_PALLETS": int(row.get("PALLET_COUNT") or row.get("pallet_count") or 0),
                "DT_UPDATE": latest_local_dt,
                "source": "local",
            }
            pickups[pickup_key] = entry

    ordered_keys = sorted(pickups.keys(), reverse=True)
    total_count = len(ordered_keys)

    start = (page - 1) * page_size
    end = start + page_size
    page_keys = ordered_keys[start:end]
    result = [pickups[key] for key in page_keys]
    return result, total_count


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

    rows = _fetch_access(
        """
        SELECT COUNT(*) AS total
        FROM IASSETS
        WHERE PICKUP_NUMBER = ? AND IIF(COD_PALLET IS NULL, 0, COD_PALLET) = ?
        """,
        [pickup_number, pallet_number],
    )
    count = 0
    if rows:
        count = rows[0].get("total") or rows[0].get("TOTAL") or 0
    if count:
        raise ValueError("Pallet already exists in IASSETS.")

    local_rows = _fetch_access(
        """
        SELECT 1
        FROM LOCAL_PALLETS
        WHERE PICKUP_NUMBER = ? AND PALLET_NUMBER = ?
        """,
        [pickup_number, pallet_number],
    )
    if local_rows:
        raise ValueError("Pallet already exists.")

    with _connect_access() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO LOCAL_PALLETS (PICKUP_NUMBER, PALLET_NUMBER, CREATED_BY, CREATED_AT)
                VALUES (?, ?, ?, ?)
                """,
                (pickup_number, pallet_number, created_by, datetime.utcnow().isoformat(timespec="seconds")),
            )
            conn.commit()
        finally:
            cur.close()


def list_pallets(pickup_number: int) -> List[Dict[str, object]]:
    aggregated = _fetch_access(
        """
        SELECT
            COD_PALLET,
            COUNT(*) AS TOTAL_ENTRIES,
            MAX(dt_update) AS MAX_DT_UPDATE,
            MAX(dt) AS MAX_DT,
            MAX(dt_processed) AS MAX_DT_PROCESSED,
            MAX(dt_pickup) AS MAX_DT_PICKUP
        FROM IASSETS
        WHERE PICKUP_NUMBER = ? AND COD_PALLET IS NOT NULL
        GROUP BY COD_PALLET
        """,
        [pickup_number],
    )

    local = _fetch_access(
        """
        SELECT lp.PICKUP_NUMBER,
               lp.PALLET_NUMBER AS COD_PALLET,
               lp.CREATED_AT,
               COALESCE(lp2.LOCAL_ENTRIES, 0) AS LOCAL_ENTRIES,
               COALESCE(lp2.LOCAL_DT, lp.CREATED_AT) AS LOCAL_DT
        FROM LOCAL_PALLETS lp
        LEFT JOIN (
            SELECT PALLET_NUMBER,
                   COUNT(*) AS LOCAL_ENTRIES,
                   MAX(CREATED_AT) AS LOCAL_DT
            FROM LOCAL_PRODUCTS
            WHERE PICKUP_NUMBER = ?
            GROUP BY PALLET_NUMBER
        ) lp2 ON lp2.PALLET_NUMBER = lp.PALLET_NUMBER
        WHERE lp.PICKUP_NUMBER = ?
        """,
        [pickup_number, pickup_number],
    )

    pallets: Dict[int, Dict[str, object]] = {}
    for row in aggregated:
        pallet = row.get("COD_PALLET") or row.get("cod_pallet")
        if pallet is None:
            continue
        try:
            pallet_key = int(pallet)
        except (TypeError, ValueError):
            continue
        total_entries = row.get("TOTAL_ENTRIES") or row.get("total_entries") or 0
        dt_candidates = [
            row.get("MAX_DT_UPDATE") or row.get("max_dt_update"),
            row.get("MAX_DT") or row.get("max_dt"),
            row.get("MAX_DT_PROCESSED") or row.get("max_dt_processed"),
            row.get("MAX_DT_PICKUP") or row.get("max_dt_pickup"),
        ]
        dt_candidates = [_normalize_timestamp(val) for val in dt_candidates if val]
        dt_update = max(dt_candidates) if dt_candidates else None
        pallets[pallet_key] = {
            "COD_PALLET": pallet_key,
            "TOTAL_ENTRIES": total_entries or 0,
            "DT_UPDATE": dt_update,
            "source": "iassets",
        }

    for row in local:
        pallet = row["COD_PALLET"]
        if pallet is None:
            continue
        try:
            pallet_key = int(pallet)
        except (TypeError, ValueError):
            continue
        entry = pallets.get(pallet_key)
        if entry:
            entry.setdefault("source", "mixed")
            entry.setdefault("created_at", _normalize_timestamp(row.get("CREATED_AT") or row.get("created_at")))
            local_entries = row.get("LOCAL_ENTRIES") or row.get("local_entries") or 0
            entry["TOTAL_ENTRIES"] = (entry.get("TOTAL_ENTRIES") or 0) + local_entries
            local_dt = _normalize_timestamp(row.get("local_dt") or row.get("LOCAL_DT"))
            if local_dt:
                current_dt = entry.get("DT_UPDATE")
                if not current_dt or local_dt > current_dt:
                    entry["DT_UPDATE"] = local_dt
        else:
            pallets[pallet_key] = {
                "COD_PALLET": pallet_key,
                "TOTAL_ENTRIES": row.get("local_entries") or row.get("LOCAL_ENTRIES") or 0,
                "DT_UPDATE": _normalize_timestamp(row.get("local_dt") or row.get("LOCAL_DT")),
                "source": "local",
            }

    ordered = sorted(pallets.values(), key=lambda p: p["COD_PALLET"])
    return ordered


def fetch_pickup_items(pickup_number: int, limit: Optional[int] = None) -> List[Dict[str, object]]:
    select_clause = ", ".join(
        [
            "COD_IASSETS",
            "COD_PALLET",
            "COD_ASSETS",
            "COD_ASSETS_SQLITE",
            "QUANTITY",
            "DESCRIPTION",
        ]
    )
    query = (
        f"SELECT {select_clause} FROM IASSETS "
        "WHERE PICKUP_NUMBER = ? "
        "ORDER BY IIF(COD_PALLET IS NULL, 0, COD_PALLET), COD_IASSETS"
    )
    params: List[object] = [pickup_number]
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)

    rows = _fetch_access(query, params)

    result: List[Dict[str, object]] = []
    for row in rows:
        data = {
            "COD_PALLET": row.get("COD_PALLET"),
            "COD_ASSETS": row.get("COD_ASSETS"),
            "COD_ASSETS_SQLITE": row.get("COD_ASSETS_SQLITE"),
            "QUANTITY": row.get("QUANTITY"),
            "DESCRIPTION": row.get("DESCRIPTION") or "",
            "row_id": row.get("COD_IASSETS"),
        }
        result.append(data)
    return result


def fetch_pallet_items(pickup_number: int, pallet_number: int) -> List[Dict[str, object]]:
    select_clause = ", ".join(
        [
            "COD_IASSETS",
            "COD_ASSETS",
            "COD_ASSETS_SQLITE",
            "QUANTITY",
            "DESCRIPTION",
            "SN",
            "ASSET_TAG",
        ]
    )
    query = (
        f"SELECT {select_clause} FROM IASSETS "
        "WHERE PICKUP_NUMBER = ? AND IIF(COD_PALLET IS NULL, 0, COD_PALLET) = ? "
        "ORDER BY COD_IASSETS"
    )
    rows = _fetch_access(query, [pickup_number, pallet_number])
    results: List[Dict[str, object]] = []
    for row in rows:
        results.append(
            {
                "COD_ASSETS": row.get("COD_ASSETS"),
                "COD_ASSETS_SQLITE": row.get("COD_ASSETS_SQLITE"),
                "QUANTITY": row.get("QUANTITY"),
                "DESCRIPTION": row.get("DESCRIPTION") or "",
                "SN": row.get("SN") or "",
                "ASSET_TAG": row.get("ASSET_TAG") or "",
                "row_id": row.get("COD_IASSETS"),
            }
        )
    return results


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

    with _connect_access() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO LOCAL_PRODUCTS (
                    PICKUP_NUMBER,
                    PALLET_NUMBER,
                    QUANTITY,
                    SERIAL_NUMBER,
                    SHORT_DESCRIPTION,
                    COMMODITY,
                    DESTINATION,
                    DESCRIPTION_RAW,
                    PHOTOS,
                    CREATED_BY,
                    CREATED_AT
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
            cur.execute("SELECT @@IDENTITY")
            row = cur.fetchone()
            new_id = int(row[0]) if row and row[0] is not None else 0
            conn.commit()
        finally:
            cur.close()
    return new_id


def list_local_products(pickup_number: int, pallet_number: int) -> List[Dict[str, object]]:
    rows = _fetch_access(
        """
        SELECT ID,
               QUANTITY,
               SERIAL_NUMBER,
               SHORT_DESCRIPTION,
               COMMODITY,
               DESTINATION,
               DESCRIPTION_RAW,
               PHOTOS,
               CREATED_BY,
               CREATED_AT
        FROM LOCAL_PRODUCTS
        WHERE PICKUP_NUMBER = ? AND PALLET_NUMBER = ?
        ORDER BY CREATED_AT DESC
        """,
        [pickup_number, pallet_number],
    )

    result: List[Dict[str, object]] = []
    for row in rows:
        photos_raw = row.get("PHOTOS") or row.get("photos") or "[]"
        try:
            photos_list = json.loads(photos_raw)
        except (TypeError, json.JSONDecodeError):
            photos_list = []
        serial = row.get("SERIAL_NUMBER") or row.get("serial_number") or ""
        short_desc = row.get("SHORT_DESCRIPTION") or row.get("short_description") or ""
        desc_raw = row.get("DESCRIPTION_RAW") or row.get("description_raw") or ""
        quantity = row.get("QUANTITY") or row.get("quantity") or 0
        row_id = row.get("ID") or row.get("id")

        result.append(
            {
                "id": row_id,
                "QUANTITY": quantity,
                "SN": serial or "",
                "short_description": short_desc,
                "commodity": row.get("COMMODITY") or row.get("commodity"),
                "destination": row.get("DESTINATION") or row.get("destination"),
                "description_raw": desc_raw,
                "photos": photos_list,
                "created_by": row.get("CREATED_BY") or row.get("created_by"),
                "created_at": _normalize_timestamp(row.get("CREATED_AT") or row.get("created_at")),
                "COD_ASSETS": None,
                "COD_ASSETS_SQLITE": row_id,
                "DESCRIPTION": short_desc or desc_raw or "",
                "ASSET_TAG": "",
            }
        )

    return result


_BASE_LOCAL_FIELDS = {
    "quantity": int,
    "serial_number": str,
    "short_description": str,
    "commodity": str,
    "destination": str,
}

_LOCAL_COLUMN_NAMES = {
    "quantity": "QUANTITY",
    "serial_number": "SERIAL_NUMBER",
    "short_description": "SHORT_DESCRIPTION",
    "commodity": "COMMODITY",
    "destination": "DESTINATION",
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

    exists = _fetch_access(
        """
        SELECT ID FROM LOCAL_PRODUCTS
        WHERE ID = ? AND PICKUP_NUMBER = ? AND PALLET_NUMBER = ?
        """,
        [product_id, pickup_number, pallet_number],
    )
    if not exists:
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

    column = _LOCAL_COLUMN_NAMES[normalized]

    with _connect_access() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                f"UPDATE LOCAL_PRODUCTS SET {column} = ? WHERE ID = ?",
                (value, product_id),
            )
            conn.commit()
        finally:
            cur.close()

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

    with _connect_access() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT COD_IASSETS
                FROM IASSETS
                WHERE COD_IASSETS = ?
                  AND PICKUP_NUMBER = ?
                  AND IIF(COD_PALLET IS NULL, 0, COD_PALLET) = ?
                """,
                (row_id, pickup_number, pallet_number),
            )
            if not cur.fetchone():
                raise ValueError("IASSETS entry not found.")

            cur.execute(
                f"UPDATE IASSETS SET {field} = ? WHERE COD_IASSETS = ?",
                (value, row_id),
            )
            conn.commit()
        finally:
            cur.close()

    return str(value)


def sync_local_products_to_iassets(
    pickup_number: int,
    pallet_number: int,
) -> int:
    rows = _fetch_access(
        """
        SELECT ID, QUANTITY, SERIAL_NUMBER, SHORT_DESCRIPTION, COMMODITY, DESTINATION, DESCRIPTION_RAW
        FROM LOCAL_PRODUCTS
        WHERE PICKUP_NUMBER = ? AND PALLET_NUMBER = ?
        ORDER BY CREATED_AT
        """,
        [pickup_number, pallet_number],
    )

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

    ids: List[int] = []
    with _connect_access() as acc_conn:
        cur = acc_conn.cursor()
        try:
            for row in rows:
                short_desc = row.get("SHORT_DESCRIPTION") or row.get("short_description") or ""
                desc_raw = row.get("DESCRIPTION_RAW") or row.get("description_raw") or ""
                description = short_desc or desc_raw or ""
                serial = row.get("SERIAL_NUMBER") or row.get("serial_number") or None
                quantity_val = row.get("QUANTITY") or row.get("quantity") or 0
                try:
                    quantity = int(quantity_val)
                except (TypeError, ValueError):
                    quantity = 0
                row_id_val = row.get("ID") or row.get("id")
                if row_id_val is None:
                    continue
                row_id = int(row_id_val)
                ids.append(row_id)
                cur.execute(
                    insert_sql,
                    (
                        pickup_number,
                        pallet_number,
                        quantity,
                        description,
                        serial,
                        row_id,
                    ),
                )

            if ids:
                placeholders = ",".join("?" for _ in ids)
                cur.execute(
                    f"DELETE FROM LOCAL_PRODUCTS WHERE ID IN ({placeholders})",
                    ids,
                )
            acc_conn.commit()
        finally:
            cur.close()

    return len(rows)


def update_local_product_photos(product_id: int, photos: List[str]) -> None:
    with _connect_access() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                "UPDATE LOCAL_PRODUCTS SET PHOTOS = ? WHERE ID = ?",
                (json.dumps(photos), product_id),
            )
            conn.commit()
        finally:
            cur.close()


def delete_local_product(product_id: int) -> None:
    with _connect_access() as conn:
        cur = conn.cursor()
        try:
            cur.execute("DELETE FROM LOCAL_PRODUCTS WHERE ID = ?", (product_id,))
            conn.commit()
        finally:
            cur.close()
