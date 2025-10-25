#!/usr/bin/env python3

import os
import sqlite3
from typing import Optional, Tuple, Dict, Any

PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(PROJECT_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "webapp.sqlite")


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(seed_example: bool = True) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS usuarios (
                cod_usuario INTEGER PRIMARY KEY,
                usuario TEXT NOT NULL,
                tipo INTEGER NOT NULL,
                ativado INTEGER NOT NULL DEFAULT 1
            )
            """
        )
        if seed_example:
            cur = conn.execute("SELECT COUNT(*) FROM usuarios")
            count = cur.fetchone()[0]
            if count == 0:
                conn.executemany(
                    "INSERT INTO usuarios (cod_usuario, usuario, tipo, ativado) VALUES (?,?,?,?)",
                    [
                        (1001, "Alice Admin", 1, 1),
                        (1002, "Eddie Employee", 2, 1),
                        (1003, "Inactive User", 2, 0),
                    ],
                )
        conn.commit()


def get_user(cod_usuario: int) -> Optional[Dict[str, Any]]:
    with _connect() as conn:
        cur = conn.execute(
            "SELECT cod_usuario, usuario, tipo, ativado FROM usuarios WHERE cod_usuario = ?",
            (cod_usuario,),
        )
        row = cur.fetchone()
        return dict(row) if row else None
