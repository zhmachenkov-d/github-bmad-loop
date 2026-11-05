"""Sync psycopg connection helper."""

from __future__ import annotations

import os
from typing import Any

import psycopg
from psycopg.rows import dict_row

DEFAULT_DATABASE_URL = "postgresql://bmad:bmad@127.0.0.1:5432/bmad"


def database_url() -> str:
    return os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)


def connect(url: str | None = None) -> psycopg.Connection[dict[str, Any]]:
    return psycopg.connect(
        url or database_url(), row_factory=dict_row, autocommit=False
    )
