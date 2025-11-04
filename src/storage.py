#!/usr/bin/env python3

import os
import sqlite3
import shutil
import logging
from typing import List, Dict, Any
from .product import Product

logger = logging.getLogger(__name__)

PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(PROJECT_DIR, "data")
PRODUCTS_DIR = os.path.join(DATA_DIR, "products")
DB_PATH = os.path.join(DATA_DIR, "products.db")

def _ensure_dirs():
    os.makedirs(PRODUCTS_DIR, exist_ok=True)

def init_db():
    _ensure_dirs()
    con = sqlite3.connect(DB_PATH)
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS products(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              asset_tag TEXT UNIQUE,
              created_by TEXT,
              created_at TEXT,
              quantity INTEGER,
              pickup TEXT,
              serial_number TEXT,
              short_description TEXT,
              subcategory TEXT,
              destination TEXT,
              description_raw TEXT,
              photos TEXT
            )
        """)
        # Backfill schema for legacy installs that still have `commodity`.
        columns = {
            row[1]: row[2]
            for row in con.execute("PRAGMA table_info(products)")
        }
        if "subcategory" not in columns and "commodity" in columns:
            con.execute("ALTER TABLE products ADD COLUMN subcategory TEXT")
        con.commit()
    finally:
        con.close()

def _to_int(val, default=1):
    try:
        return int(val)
    except Exception:
        return default

def persist_images(product: Product) -> List[str]:
    """
    Copy images into a persistent folder data/products/<asset_tag>/.
    Returns a list of relative paths (relative to PRODUCTS_DIR).
    """
    _ensure_dirs()
    dest_dir = os.path.join(PRODUCTS_DIR, product.asset_tag)
    os.makedirs(dest_dir, exist_ok=True)
    rel_paths = []
    for p in product.photos:
        if not p or not os.path.isfile(p):
            continue
        dst = os.path.join(dest_dir, os.path.basename(p))
        if os.path.abspath(p) != os.path.abspath(dst):
            shutil.copy2(p, dst)
        rel_paths.append(os.path.join(product.asset_tag, os.path.basename(p)))
    # Update product.photos to absolute persisted paths for convenience
    product.photos = [os.path.join(PRODUCTS_DIR, rp) for rp in rel_paths]
    return rel_paths

def save_product_sqlite(product: Product):
    """
    Copies images, ensures DB exists, then inserts (or replaces) the product row.
    """
    init_db()
    rel_paths = persist_images(product)
    photos_field = ";".join(rel_paths)

    con = sqlite3.connect(DB_PATH)
    try:
        con.execute("""
            INSERT INTO products(
              asset_tag, created_by, created_at, quantity, pickup,
              serial_number, short_description, subcategory, destination,
              description_raw, photos
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(asset_tag) DO UPDATE SET
              created_by=excluded.created_by,
              created_at=excluded.created_at,
              quantity=excluded.quantity,
              pickup=excluded.pickup,
              serial_number=excluded.serial_number,
              short_description=excluded.short_description,
              subcategory=excluded.subcategory,
              destination=excluded.destination,
              description_raw=excluded.description_raw,
              photos=excluded.photos
        """, (
            product.asset_tag,
            str(product.created_by),
            product.created_at,
            _to_int(product.quantity, 1),
            product.pickup,
            product.serial_number,
            product.short_description,
            product.subcategory,
            product.destination,
            product.description_raw,
            photos_field,
        ))
        con.commit()
        logger.info(f"Saved product {product.asset_tag} to {DB_PATH}")
    finally:
        con.close()

def _normalize_row(row: sqlite3.Row) -> Dict[str, Any]:
    record = dict(row)
    if "subcategory" not in record and "commodity" in record:
        record["subcategory"] = record.pop("commodity")
    else:
        record.pop("commodity", None)
    return record


def list_products(limit: int = 200) -> List[Dict[str, Any]]:
    init_db()
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute("""
            SELECT * FROM products
            ORDER BY id DESC
            LIMIT ?
        """, (limit,)).fetchall()
        return [_normalize_row(r) for r in rows]
    finally:
        con.close()

def get_product(asset_tag: str) -> Dict[str, Any]:
    init_db()
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    try:
        row = con.execute("SELECT * FROM products WHERE asset_tag = ?", (asset_tag,)).fetchone()
        return _normalize_row(row) if row else {}
    finally:
        con.close()

def list_tables() -> List[str]:
    init_db()
    con = sqlite3.connect(DB_PATH)
    try:
        rows = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
        return [r[0] for r in rows]
    finally:
        con.close()


if __name__ == "__main__":
    logger.info("Tables stored in %s:", DB_PATH)
    for name in list_tables():
        logger.info(name)
