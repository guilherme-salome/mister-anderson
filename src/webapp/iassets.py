#!/usr/bin/env python3

import os
import logging
import re
import time
from contextlib import contextmanager
from datetime import datetime
from threading import local
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from ..db.connect_access import connect_access


ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_DIR = os.path.join(ROOT_DIR, "data")
A1_ACCESS_PATH = os.path.join(DATA_DIR, "A1ASSETS_DATABASE.accdb")

logger = logging.getLogger(__name__)

_ACCESS_STATE = local()

_HINT_CACHE_TTL = 600.0  # seconds
_SUBCATEGORY_CACHE: Dict[str, object] = {
    "timestamp": 0.0,
    "values": (),
    "label_lookup": {},
    "code_lookup": {},
}
_DESTINY_CACHE: Dict[str, object] = {"timestamp": 0.0, "values": ()}


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


def _normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.casefold())


def _normalize_optional_string(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    trimmed = value.strip()
    return trimmed or None


def _normalize_optional_int(value: Optional[object]) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except (TypeError, ValueError):
        return None


def ensure_support_tables() -> None:
    """No-op placeholder retained for compatibility."""
    return


def _refresh_subcategory_cache() -> None:
    now = time.monotonic()
    cache_ts = _SUBCATEGORY_CACHE.get("timestamp", 0.0)
    if (now - float(cache_ts)) < _HINT_CACHE_TTL and _SUBCATEGORY_CACHE.get("values"):
        return

    label_lookup: Dict[str, Tuple[str, Optional[int]]] = {}
    code_lookup: Dict[int, str] = {}
    label_set: set[str] = set()

    # Pull configured fee names from client commodity-based fee table.
    try:
        fee_rows = _fetch_access(
            """
            SELECT COMMODITYBASEDID, CommodityBased_name
            FROM TBL_CLIENTS_COMMODITY_BASED_FEES
            WHERE CommodityBased_name IS NOT NULL
            """
        )
    except Exception:
        logger.exception("Failed to fetch commodity fee names from Access.")
        fee_rows = []

    for row in fee_rows:
        value = row.get("CommodityBased_name") or row.get("COMMODITYBASED_NAME")
        label = _normalize_optional_string(str(value) if value is not None else None)
        code_raw = row.get("COMMODITYBASEDID") or row.get("CommodityBasedID")
        code = _normalize_optional_int(code_raw)
        if not label:
            continue
        key = _normalize_key(label)
        label_lookup[key] = (label, code)
        label_set.add(label)
        if code is not None:
            code_lookup[code] = label

    # Pull existing IASSETS subcategories to augment list.
    try:
        subcategory_rows = _fetch_access(
            """
            SELECT DISTINCT SUBCATEGORY
            FROM IASSETS
            WHERE SUBCATEGORY IS NOT NULL
            """
        )
    except Exception:
        logger.exception("Failed to fetch IASSETS subcategory values from Access.")
        subcategory_rows = []

    for row in subcategory_rows:
        value = row.get("SUBCATEGORY")
        code = _normalize_optional_int(value)
        if code is not None:
            label = code_lookup.get(code)
            if label:
                label_set.add(label)
            else:
                label = str(code)
                label_set.add(label)
                code_lookup.setdefault(code, label)
                label_lookup.setdefault(_normalize_key(label), (label, code))
            continue
        label = _normalize_optional_string(str(value) if value is not None else None)
        if not label:
            continue
        key = _normalize_key(label)
        if key not in label_lookup:
            label_lookup[key] = (label, None)
        label_set.add(label)

    combined = sorted(label_set, key=lambda text: text.casefold())
    _SUBCATEGORY_CACHE["values"] = tuple(combined)
    _SUBCATEGORY_CACHE["label_lookup"] = label_lookup
    _SUBCATEGORY_CACHE["code_lookup"] = code_lookup
    _SUBCATEGORY_CACHE["timestamp"] = now


def get_subcategory_suggestions() -> List[str]:
    _refresh_subcategory_cache()
    values = _SUBCATEGORY_CACHE.get("values") or ()
    return list(values)


def _get_subcategory_lookups() -> Tuple[Dict[str, Tuple[str, Optional[int]]], Dict[int, str]]:
    _refresh_subcategory_cache()
    label_lookup = _SUBCATEGORY_CACHE.get("label_lookup") or {}
    code_lookup = _SUBCATEGORY_CACHE.get("code_lookup") or {}
    return label_lookup, code_lookup


def resolve_subcategory_code(value: Optional[str]) -> Tuple[Optional[int], Optional[str]]:
    label = _normalize_optional_string(value)
    if not label:
        return None, None
    label_lookup, code_lookup = _get_subcategory_lookups()
    key = _normalize_key(label)
    entry = label_lookup.get(key)
    if entry:
        canonical_label, code = entry
        return code, canonical_label
    numeric = _normalize_optional_int(label)
    if numeric is not None:
        canonical_label = code_lookup.get(numeric) or str(numeric)
        return numeric, canonical_label
    return None, label


def resolve_subcategory_label_from_code(code: Optional[object]) -> Optional[str]:
    numeric = _normalize_optional_int(code)
    if numeric is None:
        return None
    _, code_lookup = _get_subcategory_lookups()
    return code_lookup.get(numeric) or str(numeric)


def _refresh_destiny_cache() -> None:
    now = time.monotonic()
    cache_ts = _DESTINY_CACHE.get("timestamp", 0.0)
    if (now - float(cache_ts)) < _HINT_CACHE_TTL and _DESTINY_CACHE.get("values"):
        return

    try:
        rows = _fetch_access(
            """
            SELECT COD_DESTINY, DESTINY
            FROM DESTINY
            WHERE COD_DESTINY IS NOT NULL
            ORDER BY DESTINY
            """
        )
    except Exception:
        logger.exception("Failed to fetch DESTINY lookup values from Access.")
        rows = []

    options: List[Dict[str, object]] = []
    for row in rows:
        code = _normalize_optional_int(row.get("COD_DESTINY"))
        label_raw = row.get("DESTINY")
        label = _normalize_optional_string(str(label_raw) if label_raw is not None else None)
        if code is None or not label:
            continue
        options.append({"code": code, "label": label})
    _DESTINY_CACHE["values"] = tuple(options)
    _DESTINY_CACHE["timestamp"] = now


def get_destiny_options() -> List[Dict[str, object]]:
    _refresh_destiny_cache()
    values = _DESTINY_CACHE.get("values") or ()
    return [dict(item) for item in values]  # shallow copy to protect cache


def warm_access_connection() -> None:
    """Prime Access caches so the first request avoids connection overhead."""
    try:
        get_subcategory_suggestions()
        get_destiny_options()
    except Exception:
        logger.exception("Failed to warm Access connection.")


def canonicalize_subcategory(
    value: Optional[str],
    *,
    suggestions: Optional[Iterable[str]] = None,
) -> Optional[str]:
    raw = _normalize_optional_string(value)
    if not raw:
        return None

    candidates = suggestions or get_subcategory_suggestions()
    key = _normalize_key(raw)
    for option in candidates:
        if _normalize_key(option) == key:
            return option
    return raw


def resolve_cod_destiny(
    value: Optional[object],
    *,
    destiny_options: Optional[Sequence[Dict[str, object]]] = None,
    label_hint: Optional[str] = None,
) -> Tuple[Optional[int], Optional[str]]:
    options = destiny_options or get_destiny_options()
    if not options:
        return None, None

    code_lookup: Dict[int, str] = {}
    key_lookup: Dict[str, int] = {}
    for option in options:
        code = _normalize_optional_int(option.get("code"))
        label = _normalize_optional_string(str(option.get("label")) if option.get("label") is not None else None)
        if code is None or not label:
            continue
        code_lookup[code] = label
        key_lookup[_normalize_key(label)] = code

    resolved_code: Optional[int] = None
    resolved_label: Optional[str] = None

    def _try_from_candidate(candidate: Optional[object]) -> None:
        nonlocal resolved_code, resolved_label
        if candidate is None or resolved_code is not None:
            return
        if isinstance(candidate, int):
            code = candidate
        else:
            code = _normalize_optional_int(candidate)
        if code is not None and code in code_lookup:
            resolved_code = code
            resolved_label = code_lookup[code]
            return
        text = _normalize_optional_string(str(candidate) if candidate is not None else None)
        if not text:
            return
        key = _normalize_key(text)
        if key in key_lookup:
            code = key_lookup[key]
            resolved_code = code
            resolved_label = code_lookup[code]
            return
        # attempt partial match
        for lookup_key, code in key_lookup.items():
            if lookup_key.startswith(key) or key.startswith(lookup_key):
                resolved_code = code
                resolved_label = code_lookup[code]
                return

    _try_from_candidate(value)
    _try_from_candidate(label_hint)

    return resolved_code, resolved_label


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
    raise NotImplementedError("Pickups must be created through the external workflow.")


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
            NZ(pc.PALLET_COUNT, 0) AS PALLET_COUNT,
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
                SELECT PICKUP_NUMBER, COD_ASSETS
                FROM IASSETS
                WHERE COD_ASSETS IS NOT NULL
                GROUP BY PICKUP_NUMBER, COD_ASSETS
            ) AS inner_tbl
            GROUP BY inner_tbl.PICKUP_NUMBER
        ) AS pc
            ON p.PICKUP_NUMBER = pc.PICKUP_NUMBER
        LEFT JOIN (
            SELECT PICKUP_NUMBER,
                   MAX(NZ(dt_update, NZ(dt, NZ(dt_processed, dt_pickup)))) AS MAX_DT
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
    cod_assets: int,
    *,
    created_by: Optional[str] = None,
) -> None:
    raise NotImplementedError("Pallets must be created through the external workflow.")

def list_pallets(pickup_number: int) -> List[Dict[str, object]]:
    aggregated = _fetch_access(
        """
        SELECT
            COD_ASSETS,
            COUNT(*) AS TOTAL_ENTRIES,
            MAX(NZ(dt_update, NZ(dt, NZ(dt_processed, dt_pickup)))) AS MAX_DT
        FROM IASSETS
        WHERE PICKUP_NUMBER = ? AND COD_ASSETS IS NOT NULL
        GROUP BY COD_ASSETS
        """,
        [pickup_number],
    )

    pallets: List[Dict[str, object]] = []
    for row in aggregated:
        cod_assets_value = row.get("COD_ASSETS") or row.get("cod_assets")
        if cod_assets_value in (None, ""):
            continue
        try:
            numeric_cod_assets = int(cod_assets_value)
            display_cod_assets = numeric_cod_assets
        except (TypeError, ValueError):
            numeric_cod_assets = None
            display_cod_assets = str(cod_assets_value)
        total_entries = row.get("TOTAL_ENTRIES") or row.get("total_entries") or 0
        dt_update = _normalize_timestamp(row.get("MAX_DT") or row.get("max_dt"))
        pallets.append(
            {
                "COD_ASSETS": display_cod_assets,
                "COD_ASSETS_NUMERIC": numeric_cod_assets,
            "TOTAL_ENTRIES": total_entries or 0,
            "DT_UPDATE": dt_update,
            "source": "iassets",
            }
        )

    def _sort_key(entry: Dict[str, object]) -> tuple:
        cod_assets_int = entry.get("COD_ASSETS_NUMERIC")
        fallback = entry.get("COD_ASSETS")
        return (
            cod_assets_int if cod_assets_int is not None else float("inf"),
            fallback,
        )

    ordered = sorted(pallets, key=_sort_key)
    for entry in ordered:
        entry.pop("COD_ASSETS_NUMERIC", None)
    return ordered


def fetch_pickup_items(pickup_number: int, limit: Optional[int] = None) -> List[Dict[str, object]]:
    select_clause = ", ".join(
        [
            "COD_IASSETS",
            "COD_ASSETS",
            "COD_ASSETS_SQLITE",
            "QUANTITY",
            "DESCRIPTION",
        ]
    )
    query = (
        f"SELECT {select_clause} FROM IASSETS "
        "WHERE PICKUP_NUMBER = ? "
        "ORDER BY IIF(COD_ASSETS IS NULL, 0, COD_ASSETS), COD_IASSETS"
    )
    params: List[object] = [pickup_number]
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)

    rows = _fetch_access(query, params)

    result: List[Dict[str, object]] = []
    for row in rows:
        data = {
            "COD_ASSETS": row.get("COD_ASSETS"),
            "COD_ASSETS_SQLITE": row.get("COD_ASSETS_SQLITE"),
            "QUANTITY": row.get("QUANTITY"),
            "DESCRIPTION": row.get("DESCRIPTION") or "",
            "row_id": row.get("COD_IASSETS"),
        }
        result.append(data)
    return result


def fetch_pallet_items(pickup_number: int, cod_assets: int) -> List[Dict[str, object]]:
    select_clause = ", ".join(
        [
            "COD_IASSETS",
            "COD_ASSETS",
            "COD_ASSETS_SQLITE",
            "QUANTITY",
            "DESCRIPTION",
            "SN",
            "ASSET_TAG",
            "SUBCATEGORY",
            "COD_DESTINY",
        ]
    )
    query = (
        f"SELECT {select_clause} FROM IASSETS "
        "WHERE PICKUP_NUMBER = ? AND COD_ASSETS = ? "
        "ORDER BY COD_IASSETS"
    )
    rows = _fetch_access(query, [pickup_number, cod_assets])
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
                "SUBCATEGORY": row.get("SUBCATEGORY") or "",
                "COD_DESTINY": row.get("COD_DESTINY"),
                "row_id": row.get("COD_IASSETS"),
            }
        )
    return results


def create_product_entry(
    *,
    pickup_number: int,
    cod_assets: int,
    quantity: int,
    serial_number: str,
    short_description: str,
    description_raw: str,
    created_by: Optional[str] = None,
    cod_assets_sqlite: Optional[int] = None,
    asset_tag: str = "",
    subcategory: Optional[str] = None,
    subcategory_code: Optional[int] = None,
    cod_destiny: Optional[object] = None,
    destination_label: Optional[str] = None,
    cod_destiny_secondary: Optional[object] = None,
    grade: Optional[object] = None,
    reason: Optional[str] = None,
) -> int:
    if quantity <= 0:
        raise ValueError("Quantity must be greater than zero.")

    logger.debug(
        "create_product_entry called with raw values: pickup=%s cod_assets=%s quantity=%s "
        "serial=%r short_description=%r description_raw=%r created_by=%r "
        "cod_assets_sqlite=%r asset_tag=%r subcategory=%r subcategory_code=%r cod_destiny=%r "
        "destination_label=%r cod_destiny_secondary=%r grade=%r reason=%r",
        pickup_number,
        cod_assets,
        quantity,
        serial_number,
        short_description,
        description_raw,
        created_by,
        cod_assets_sqlite,
        asset_tag,
        subcategory,
        subcategory_code,
        cod_destiny,
        destination_label,
        cod_destiny_secondary,
        grade,
        reason,
    )

    now = datetime.utcnow()
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")

    description = _normalize_optional_string(short_description) or _normalize_optional_string(description_raw) or ""
    serial = _normalize_optional_string(serial_number) or ""

    asset_tag_value = _normalize_optional_string(asset_tag) or ""
    subcategory_suggestions = get_subcategory_suggestions()
    subcategory_label = canonicalize_subcategory(subcategory, suggestions=subcategory_suggestions)
    subcategory_code_value = _normalize_optional_int(subcategory_code)
    resolved_label: Optional[str] = None
    if subcategory_code_value is None:
        subcategory_code_value, resolved_label = resolve_subcategory_code(subcategory_label)
    else:
        resolved_label = resolve_subcategory_label_from_code(subcategory_code_value)
    if resolved_label:
        subcategory_label = resolved_label
    destiny_options = get_destiny_options()
    cod_destiny_value, cod_destiny_label = resolve_cod_destiny(
        cod_destiny,
        destiny_options=destiny_options,
        label_hint=destination_label,
    )
    secondary_code, _ = resolve_cod_destiny(
        cod_destiny_secondary,
        destiny_options=destiny_options,
        label_hint=destination_label,
    )
    if cod_destiny_value is not None:
        cod_destiny_value = int(cod_destiny_value)
    if secondary_code is not None:
        secondary_code = int(secondary_code)

    grade_value = _normalize_optional_int(grade)
    reason_value = _normalize_optional_string(reason)

    cod_assets_value = int(cod_assets)
    pickup_value = int(pickup_number)

    if subcategory_code_value is None:
        raise ValueError("SUBCATEGORY is required for IASSETS entries.")
    if cod_destiny_value is None:
        raise ValueError("COD_DESTINY is required for IASSETS entries.")

    cod_assets_sqlite_value = int(cod_assets_sqlite) if cod_assets_sqlite is not None else None

    logger.debug(
        "Normalized IASSETS values: pickup=%s cod_assets=%s quantity=%s cod_assets_sqlite=%r "
        "serial=%r description=%r asset_tag=%r subcategory_code=%r subcategory_label=%r cod_destiny=%r "
        "cod_destiny_label=%r cod_destiny_secondary=%r grade=%r reason=%r timestamp=%s",
        pickup_value,
        cod_assets_value,
        quantity,
        cod_assets_sqlite_value,
        serial,
        description,
        asset_tag_value,
        subcategory_code_value,
        subcategory_label,
        cod_destiny_value,
        destination_label,
        secondary_code,
        grade_value,
        reason_value,
        timestamp,
    )

    with _connect_access() as conn:
        cur = conn.cursor()
        try:
            columns: List[Tuple[str, object]] = [
                ("COD_ASSETS", cod_assets_value),
                ("ASSET_TAG", asset_tag_value),
                ("PICKUP_NUMBER", pickup_value),
                ("QUANTITY", quantity),
                ("DESCRIPTION", description),
                ("SN", serial),
                ("SUBCATEGORY", subcategory_code_value),
                ("COD_DESTINY", cod_destiny_value),
                ("WEBCAM2", 0),
                ("BATTERY2", 0),
                ("PARTMISSING", 0),
                ("FLAG", 0),
                ("FLAG_SEND", 0),
                ("UPLOAD_KYOZOU", 0),
                ("DT", timestamp),
                ("DT_UPDATE", timestamp),
            ]

            if cod_assets_sqlite_value is not None:
                columns.insert(1, ("COD_ASSETS_SQLITE", cod_assets_sqlite_value))
            else:
                logger.debug(
                    "Omitting COD_ASSETS_SQLITE from IASSETS insert; will backfill after identity fetch."
                )

            if secondary_code is not None:
                columns.append(("COD_DESTINY2", secondary_code))
            if grade_value is not None:
                columns.append(("GRADE", grade_value))
            if reason_value:
                columns.append(("REASON", reason_value))

            column_names = ", ".join(name for name, _ in columns)
            placeholders = ", ".join("?" for _ in columns)
            params = tuple(value for _, value in columns)
            query = f"INSERT INTO IASSETS ({column_names}) VALUES ({placeholders})"

            logger.debug(
                "Executing IASSETS insert query=%s bindings=%s",
                query,
                _summarize_params(columns),
            )
            try:
                cur.execute(
                    query,
                    params,
                )
            except Exception:
                logger.exception(
                    "IASSETS insert failed; query=%s, bindings=%s",
                    query,
                    _summarize_params(columns),
                )
                raise
            cur.execute("SELECT @@IDENTITY")
            row = cur.fetchone()
            new_id = int(row[0]) if row and row[0] is not None else 0
            if cod_assets_sqlite is None and new_id:
                cur.execute(
                    "UPDATE IASSETS SET COD_ASSETS_SQLITE = ? WHERE COD_IASSETS = ?",
                    (new_id, new_id),
                )
                logger.debug(
                    "Backfilled COD_ASSETS_SQLITE=%s for COD_IASSETS=%s after insert.",
                    new_id,
                    new_id,
                )
            logger.debug(
                "IASSETS insert committed pickup=%s cod_assets=%s cod_iassets=%s cod_assets_sqlite_param=%r",
                pickup_value,
                cod_assets_value,
                new_id,
                cod_assets_sqlite,
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
}


def update_iassets_field(
    *,
    row_id: int,
    pickup_number: int,
    cod_assets: int,
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
                  AND COD_ASSETS = ?
                """,
                (row_id, pickup_number, cod_assets),
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
def _summarize_params(bindings: Sequence[Tuple[str, object]]) -> List[Dict[str, object]]:
    summary: List[Dict[str, object]] = []
    for name, value in bindings:
        summary.append(
            {
                "column": name,
                "value": value,
                "type": None if value is None else type(value).__name__,
            }
        )
    return summary
