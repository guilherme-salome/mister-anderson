#!/usr/bin/env python3

import os
import logging
from contextlib import contextmanager
from datetime import datetime
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


def _parse_string_field(value: Optional[str]) -> str:
    return (value or "").strip()


def _parse_optional_int_field(value: Optional[str]) -> Optional[int]:
    trimmed = (value or "").strip()
    if not trimmed:
        return None
    try:
        return int(trimmed)
    except ValueError as exc:  # pragma: no cover - defensive
        raise ValueError("Value must be a whole number.") from exc


def _parse_positive_quantity(value: Optional[str]) -> int:
    trimmed = (value or "").strip()
    try:
        qty = int(trimmed)
    except ValueError as exc:  # pragma: no cover - defensive
        raise ValueError("Quantity must be a positive integer.") from exc
    if qty <= 0:
        raise ValueError("Quantity must be a positive integer.")
    return qty


def ensure_support_tables() -> None:
    """No-op placeholder retained for compatibility."""
    return


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
    return False


def create_pickup(pickup_number: int, *, created_by: Optional[str] = None) -> None:
    if pickup_number <= 0:
        raise ValueError("Pickup number must be a positive integer.")
    rows = _fetch_access(
        """
        SELECT 1
        FROM IASSETS
        WHERE PICKUP_NUMBER = ?
        LIMIT 1
        """,
        [pickup_number],
    )
    if rows:
        raise ValueError("Pickup already exists in IASSETS.")

    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    with _connect_access() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                DELETE FROM IASSETS
                WHERE PICKUP_NUMBER = ?
                  AND IIF(COD_PALLET IS NULL, 0, COD_PALLET) = ?
                  AND QUANTITY = 0
                  AND (DESCRIPTION IS NULL OR DESCRIPTION = '')
                  AND (SN IS NULL OR SN = '')
                """,
                (pickup_number, pallet_number),
            )
            cur.execute(
                """
                INSERT INTO IASSETS (
                    PICKUP_NUMBER,
                    COD_PALLET,
                    QUANTITY,
                    DESCRIPTION,
                    SN,
                    WEBCAM2,
                    BATTERY2,
                    PARTMISSING,
                    DT,
                    DT_UPDATE,
                    FLAG,
                    FLAG_SEND,
                    UPLOAD_KYOZOU,
                    PROCBY
                ) VALUES (?, ?, ?, ?, ?, 0, 0, 0, ?, ?, 0, 0, 0, ?)
                """,
                (
                    pickup_number,
                    None,
                    0,
                    "",
                    "",
                    timestamp,
                    timestamp,
                    created_by or None,
                ),
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
        SELECT 1
        FROM IASSETS
        WHERE PICKUP_NUMBER = ? AND IIF(COD_PALLET IS NULL, 0, COD_PALLET) = ?
        LIMIT 1
        """,
        [pickup_number, pallet_number],
    )
    if rows:
        raise ValueError("Pallet already exists in IASSETS.")

    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    with _connect_access() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT COD_IASSETS
                FROM IASSETS
                WHERE PICKUP_NUMBER = ? AND COD_PALLET IS NULL
                ORDER BY COD_IASSETS
                LIMIT 1
                """,
                (pickup_number,),
            )
            placeholder = cur.fetchone()
            if placeholder:
                cur.execute(
                    """
                    UPDATE IASSETS
                    SET COD_PALLET = ?,
                        QUANTITY = 0,
                        DESCRIPTION = '',
                        SN = '',
                        DT_UPDATE = ?,
                        DT = COALESCE(DT, ?),
                        WEBCAM2 = COALESCE(WEBCAM2, 0),
                        BATTERY2 = COALESCE(BATTERY2, 0),
                        PARTMISSING = COALESCE(PARTMISSING, 0),
                        FLAG = COALESCE(FLAG, 0),
                        FLAG_SEND = COALESCE(FLAG_SEND, 0),
                        UPLOAD_KYOZOU = COALESCE(UPLOAD_KYOZOU, 0),
                        PROCBY = COALESCE(PROCBY, ?)
                    WHERE COD_IASSETS = ?
                    """,
                    (pallet_number, timestamp, timestamp, created_by or None, placeholder[0]),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO IASSETS (
                        PICKUP_NUMBER,
                        COD_PALLET,
                        QUANTITY,
                        DESCRIPTION,
                        SN,
                        WEBCAM2,
                        BATTERY2,
                        PARTMISSING,
                        DT,
                        DT_UPDATE,
                        FLAG,
                        FLAG_SEND,
                        UPLOAD_KYOZOU,
                        PROCBY
                    ) VALUES (?, ?, 0, '', '', 0, 0, 0, ?, ?, 0, 0, 0, ?)
                    """,
                    (pickup_number, pallet_number, timestamp, timestamp, created_by or None),
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
            MAX(COALESCE(dt_update, dt, dt_processed, dt_pickup)) AS MAX_DT
        FROM IASSETS
        WHERE PICKUP_NUMBER = ? AND COD_PALLET IS NOT NULL
        GROUP BY COD_PALLET
        """,
        [pickup_number],
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
        dt_update = _normalize_timestamp(row.get("MAX_DT") or row.get("max_dt"))
        pallets[pallet_key] = {
            "COD_PALLET": pallet_key,
            "TOTAL_ENTRIES": total_entries or 0,
            "DT_UPDATE": dt_update,
            "source": "iassets",
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


def create_product_entry(
    *,
    pickup_number: int,
    pallet_number: int,
    quantity: int,
    serial_number: str,
    short_description: str,
    description_raw: str,
    created_by: Optional[str] = None,
    cod_assets: Optional[int] = None,
    cod_assets_sqlite: Optional[int] = None,
    asset_tag: str = "",
) -> int:
    if quantity <= 0:
        raise ValueError("Quantity must be greater than zero.")

    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    description = short_description or description_raw or ""
    serial = serial_number or ""

    asset_tag_value = asset_tag.strip() if asset_tag else ""

    with _connect_access() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO IASSETS (
                    COD_ASSETS,
                    COD_ASSETS_SQLITE,
                    ASSET_TAG,
                    PICKUP_NUMBER,
                    COD_PALLET,
                    QUANTITY,
                    DESCRIPTION,
                    SN,
                    WEBCAM2,
                    BATTERY2,
                    PARTMISSING,
                    DT,
                    DT_UPDATE,
                    FLAG,
                    FLAG_SEND,
                    UPLOAD_KYOZOU,
                    PROCBY
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, ?, ?, 0, 0, 0, ?)
                """,
                (
                    cod_assets,
                    cod_assets_sqlite,
                    asset_tag_value or None,
                    pickup_number,
                    pallet_number,
                    quantity,
                    description,
                    serial,
                    timestamp,
                    timestamp,
                    created_by or None,
                ),
            )
            cur.execute("SELECT @@IDENTITY")
            row = cur.fetchone()
            new_id = int(row[0]) if row and row[0] is not None else 0
            if cod_assets_sqlite is None and new_id:
                cur.execute(
                    "UPDATE IASSETS SET COD_ASSETS_SQLITE = ? WHERE COD_IASSETS = ?",
                    (new_id, new_id),
                )
            conn.commit()
        finally:
            cur.close()
    return new_id


def delete_product_entry(row_id: int) -> None:
    with _connect_access() as conn:
        cur = conn.cursor()
        try:
            cur.execute("DELETE FROM IASSETS WHERE COD_IASSETS = ?", (row_id,))
            conn.commit()
        finally:
            cur.close()


_EDITABLE_IASSETS_FIELDS = {
    "SN": (_parse_string_field, None),
    "ASSET_TAG": (_parse_string_field, None),
    "DESCRIPTION": (_parse_string_field, None),
    "QUANTITY": (_parse_positive_quantity, "Quantity must be a positive integer."),
    "COD_ASSETS": (_parse_optional_int_field, "COD_ASSETS must be a whole number or left blank."),
    "COD_ASSETS_SQLITE": (_parse_optional_int_field, "COD_ASSETS_SQLITE must be a whole number or left blank."),
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

    parser, message = _EDITABLE_IASSETS_FIELDS[field]
    try:
        value = parser(raw_value)
    except ValueError as exc:
        raise ValueError(message or str(exc)) from exc

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

    return "" if value is None else str(value)
