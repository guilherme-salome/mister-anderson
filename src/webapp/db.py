#!/usr/bin/env python3

import base64
import hmac
import sqlite3
from datetime import datetime
from hashlib import pbkdf2_hmac
from typing import Dict, Iterable, Optional
from pathlib import Path
import os

from .iassets import DATA_DIR

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


def init_db(seed_example: bool = True) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with _connect() as conn:
        conn.execute(_CREATE_USERS_SQL)
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username ON users(username)")
        if seed_example:
            cur = conn.execute("SELECT COUNT(*) FROM users")
            if cur.fetchone()[0] == 0:
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
