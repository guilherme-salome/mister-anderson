#!/usr/bin/env python3

import base64
import hmac
import logging
import os
import sqlite3
from datetime import datetime
from hashlib import pbkdf2_hmac
from typing import Dict, Iterable, Optional

from .iassets import ACCESS_PATH, DATA_DIR, _connect_access

DB_PATH = DATA_DIR / "webapp.sqlite"

ALLOWED_ROLES = ("viewer", "employee", "supervisor", "admin")

_CREATE_USERS_SQL = """
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    full_name TEXT NOT NULL,
    role TEXT NOT NULL CHECK(role IN ('viewer', 'employee', 'supervisor', 'admin')),
    is_active INTEGER NOT NULL DEFAULT 1,
    password_hash BLOB NOT NULL,
    password_salt BLOB NOT NULL,
    created_at TEXT NOT NULL
)
"""


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _hash_password(password: str, salt: Optional[bytes] = None) -> tuple[bytes, bytes]:
    if not password:
        raise ValueError("Password cannot be empty.")
    if salt is None:
        salt = os.urandom(16)
    hash_bytes = pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 100_000)
    return hash_bytes, salt


def _password_matches(password: str, stored_hash: bytes, salt: bytes) -> bool:
    candidate, _ = _hash_password(password, salt)
    return hmac.compare_digest(candidate, stored_hash)


def init_db(seed_example: bool = True, sync_from_access: bool = True) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with _connect() as conn:
        conn.execute(_CREATE_USERS_SQL)
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username ON users(username)")
        conn.commit()
    if seed_example:
        _ensure_default_admin()
    if sync_from_access:
        sync_users_from_access()


def _ensure_default_admin() -> None:
    with _connect() as conn:
        cur = conn.execute("SELECT COUNT(*) FROM users")
        if cur.fetchone()[0] > 0:
            return
        _create_user(
            conn,
            username="admin",
            full_name="Administrator",
            password="admin123",
            role="admin",
            is_active=True,
        )
        conn.commit()


def _create_user(
    conn: sqlite3.Connection,
    *,
    username: str,
    full_name: str,
    password: str,
    role: str = "viewer",
    is_active: bool = True,
) -> int:
    if role not in ALLOWED_ROLES:
        raise ValueError(f"Invalid role '{role}'. Allowed roles: {ALLOWED_ROLES}")
    password_hash, salt = _hash_password(password)
    cur = conn.execute(
        """
        INSERT INTO users (username, full_name, role, is_active, password_hash, password_salt, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            username.strip().lower(),
            full_name.strip(),
            role,
            1 if is_active else 0,
            password_hash,
            salt,
            datetime.utcnow().isoformat(timespec="seconds"),
        ),
    )
    return int(cur.lastrowid)


def create_user(
    *,
    username: str,
    full_name: str,
    password: str,
    role: str = "viewer",
    is_active: bool = True,
) -> int:
    """Create a user record in the webapp database."""
    with _connect() as conn:
        user_id = _create_user(
            conn,
            username=username,
            full_name=full_name,
            password=password,
            role=role,
            is_active=is_active,
        )
        conn.commit()
        return user_id


def _upsert_user_from_access(
    *,
    username: str,
    full_name: str,
    password: str,
    is_active: bool,
    role: str = "employee",
) -> None:
    username_clean = username.strip().lower()
    full_name_clean = full_name.strip()

    with _connect() as conn:
        cur = conn.execute(
            "SELECT id, password_hash, password_salt, role, is_active FROM users WHERE username = ?",
            (username_clean,),
        )
        row = cur.fetchone()
        password_hash, salt = _hash_password(password)

        if row:
            conn.execute(
                """
                UPDATE users
                SET full_name = ?, role = ?, is_active = ?, password_hash = ?, password_salt = ?, created_at = created_at
                WHERE id = ?
                """,
                (
                    full_name_clean,
                    role,
                    1 if is_active else 0,
                    password_hash,
                    salt,
                    row["id"],
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO users (username, full_name, role, is_active, password_hash, password_salt, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    username_clean,
                    full_name_clean,
                    role,
                    1 if is_active else 0,
                    password_hash,
                    salt,
                    datetime.utcnow().isoformat(timespec="seconds"),
                ),
            )
        conn.commit()


def sync_users_from_access() -> None:
    query = """
        SELECT LOGIN, USUARIO, SENHA, ATIVADO
        FROM USUARIOS
        WHERE LOGIN IS NOT NULL AND TRIM(LOGIN) <> ''
    """
    try:
        with _connect_access() as conn:
            cur = conn.cursor()
            cur.execute(query)
            rows = cur.fetchall()
    except Exception:
        logger.exception("Failed to import users from Access USUARIOS table.")
        return

    for row in rows:
        login = row[0] if isinstance(row, (tuple, list)) else row.get("LOGIN")
        full_name = row[1] if isinstance(row, (tuple, list)) else row.get("USUARIO")
        password = row[2] if isinstance(row, (tuple, list)) else row.get("SENHA")
        activated = row[3] if isinstance(row, (tuple, list)) else row.get("ATIVADO")

        if not login or not password:
            continue

        is_active = False
        if isinstance(activated, str):
            is_active = activated.strip() not in {"0", "N", "n", "false", "False"}
        elif isinstance(activated, (int, float)):
            is_active = bool(activated)
        elif isinstance(activated, bool):
            is_active = activated

        _upsert_user_from_access(
            username=str(login),
            full_name=str(full_name or login),
            password=str(password),
            is_active=is_active,
            role="employee",
        )



def authenticate(username: str, password: str) -> Optional[Dict[str, object]]:
    username = username.strip().lower()
    with _connect() as conn:
        cur = conn.execute(
            "SELECT * FROM users WHERE username = ? AND is_active = 1",
            (username,),
        )
        row = cur.fetchone()
        if not row:
            return None
        if not _password_matches(password, row["password_hash"], row["password_salt"]):
            return None
        payload = dict(row)
        payload.pop("password_hash", None)
        payload.pop("password_salt", None)
        return payload


def get_user(user_id: int) -> Optional[Dict[str, object]]:
    with _connect() as conn:
        cur = conn.execute(
            """
            SELECT id, username, full_name, role, is_active, created_at
            FROM users
            WHERE id = ?
            """,
            (user_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_user_by_username(username: str) -> Optional[Dict[str, object]]:
    username = username.strip().lower()
    with _connect() as conn:
        cur = conn.execute(
            """
            SELECT id, username, full_name, role, is_active, created_at
            FROM users
            WHERE username = ?
            """,
            (username,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def list_users() -> Iterable[Dict[str, object]]:
    with _connect() as conn:
        cur = conn.execute(
            """
            SELECT id, username, full_name, role, is_active, created_at
            FROM users
            ORDER BY username
            """
        )
        for row in cur.fetchall():
            yield dict(row)


def update_password(user_id: int, new_password: str) -> None:
    password_hash, salt = _hash_password(new_password)
    with _connect() as conn:
        conn.execute(
            "UPDATE users SET password_hash = ?, password_salt = ? WHERE id = ?",
            (password_hash, salt, user_id),
        )
        conn.commit()


def update_user_status(user_id: int, *, is_active: bool) -> None:
    with _connect() as conn:
        conn.execute(
            "UPDATE users SET is_active = ? WHERE id = ?",
            (1 if is_active else 0, user_id),
        )
        conn.commit()


def update_user_role(user_id: int, *, role: str) -> None:
    if role not in ALLOWED_ROLES:
        raise ValueError(f"Invalid role '{role}'. Allowed roles: {ALLOWED_ROLES}")
    with _connect() as conn:
        conn.execute("UPDATE users SET role = ? WHERE id = ?", (role, user_id))
        conn.commit()
