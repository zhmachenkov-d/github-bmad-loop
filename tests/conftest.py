"""Compose-backed fixtures: require Postgres 18 reachable via DATABASE_URL."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from core.stub_unit import JOB_KIND_RUN, JOB_KIND_UNIT, STUB_READY

DEFAULT_URL = "postgresql://bmad:bmad@127.0.0.1:5432/bmad"


def _load_dotenv(path: Path) -> None:
    """Load KEY=VALUE from repo `.env` without overriding already-set env."""
    if not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        if not key or key in os.environ:
            continue
        os.environ[key] = value.strip().strip("'").strip('"')


_load_dotenv(ROOT / ".env")


def _wait_for_db(url: str, attempts: int = 30) -> None:
    last: Exception | None = None
    for _ in range(attempts):
        try:
            with psycopg.connect(url, connect_timeout=2) as conn:
                conn.execute("SELECT 1")
            return
        except Exception as exc:  # noqa: BLE001 — probe only
            last = exc
            import time

            time.sleep(1)
    raise RuntimeError(f"Postgres not reachable at {url}: {last}")


@pytest.fixture(scope="session")
def db_url() -> str:
    url = os.environ.get("DATABASE_URL", DEFAULT_URL)
    os.environ.setdefault("DATABASE_URL", url)
    _wait_for_db(url)
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=ROOT,
        check=True,
        env={**os.environ, "DATABASE_URL": url},
    )
    return url


@pytest.fixture
def conn(db_url: str):
    with psycopg.connect(db_url, row_factory=dict_row, autocommit=False) as connection:
        with connection.transaction():
            connection.execute(
                """
                TRUNCATE signals, outbox, event_log, jobs, units, runs
                RESTART IDENTITY CASCADE
                """
            )
        yield connection
        connection.rollback()


@pytest.fixture
def seed_unit_job(conn):
    def _seed(
        *,
        state: str = STUB_READY,
        attempts: int = 0,
        max_attempts: int = 3,
        deadline_at=None,
        lease_expires_age: str | None = None,
        leased_by: str = "w",
        version: int = 0,
    ) -> dict:
        run_id = uuid4()
        unit_id = uuid4()
        job_id = uuid4()
        lease_id = uuid4() if lease_expires_age is not None else None
        with conn.transaction():
            conn.execute("INSERT INTO runs (id) VALUES (%s)", (run_id,))
            conn.execute(
                "INSERT INTO units (id, run_id, state) VALUES (%s, %s, %s)",
                (unit_id, run_id, state),
            )
            if lease_expires_age is not None:
                conn.execute(
                    """
                    INSERT INTO jobs (
                        id, kind, ref_id, version,
                        attempts, max_attempts, deadline_at,
                        lease_id, leased_by, lease_expires_at
                    )
                    VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, now() - (%s::interval)
                    )
                    """,
                    (
                        job_id,
                        JOB_KIND_UNIT,
                        unit_id,
                        version,
                        attempts,
                        max_attempts,
                        deadline_at,
                        lease_id,
                        leased_by,
                        lease_expires_age,
                    ),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO jobs (
                        id, kind, ref_id, version,
                        attempts, max_attempts, deadline_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        job_id,
                        JOB_KIND_UNIT,
                        unit_id,
                        version,
                        attempts,
                        max_attempts,
                        deadline_at,
                    ),
                )
        return {
            "run_id": run_id,
            "unit_id": unit_id,
            "job_id": job_id,
            "lease_id": lease_id,
        }

    return _seed


@pytest.fixture
def seed_run_job(conn):
    def _seed(
        *,
        attempts: int = 0,
        max_attempts: int = 3,
        deadline_at=None,
        lease_expires_age: str | None = None,
        leased_by: str = "w",
        version: int = 0,
    ) -> dict:
        run_id = uuid4()
        job_id = uuid4()
        lease_id = uuid4() if lease_expires_age is not None else None
        with conn.transaction():
            conn.execute("INSERT INTO runs (id) VALUES (%s)", (run_id,))
            if lease_expires_age is not None:
                conn.execute(
                    """
                    INSERT INTO jobs (
                        id, kind, ref_id, version,
                        attempts, max_attempts, deadline_at,
                        lease_id, leased_by, lease_expires_at
                    )
                    VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, now() - (%s::interval)
                    )
                    """,
                    (
                        job_id,
                        JOB_KIND_RUN,
                        run_id,
                        version,
                        attempts,
                        max_attempts,
                        deadline_at,
                        lease_id,
                        leased_by,
                        lease_expires_age,
                    ),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO jobs (
                        id, kind, ref_id, version,
                        attempts, max_attempts, deadline_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        job_id,
                        JOB_KIND_RUN,
                        run_id,
                        version,
                        attempts,
                        max_attempts,
                        deadline_at,
                    ),
                )
        return {"run_id": run_id, "job_id": job_id, "lease_id": lease_id}

    return _seed


@pytest.fixture
def seed_fleet_unit_jobs(conn):
    def _seed(n: int = 100) -> list[dict]:
        rows: list[dict] = []
        with conn.transaction():
            for _ in range(n):
                run_id = uuid4()
                unit_id = uuid4()
                job_id = uuid4()
                conn.execute("INSERT INTO runs (id) VALUES (%s)", (run_id,))
                conn.execute(
                    "INSERT INTO units (id, run_id, state) VALUES (%s, %s, %s)",
                    (unit_id, run_id, STUB_READY),
                )
                conn.execute(
                    """
                    INSERT INTO jobs (id, kind, ref_id, version)
                    VALUES (%s, %s, %s, 0)
                    """,
                    (job_id, JOB_KIND_UNIT, unit_id),
                )
                rows.append({"run_id": run_id, "unit_id": unit_id, "job_id": job_id})
        return rows

    return _seed


@pytest.fixture
def counts(conn):
    def _counts() -> dict[str, int]:
        event_n = conn.execute("SELECT count(*) AS n FROM event_log").fetchone()["n"]
        outbox_n = conn.execute("SELECT count(*) AS n FROM outbox").fetchone()["n"]
        return {"event_log": event_n, "outbox": outbox_n}

    return _counts


@pytest.fixture
def seed_dead_outbox(conn, seed_unit_job):
    def _seed(*, channel: str = "stub") -> dict:
        ids = seed_unit_job()
        with conn.transaction():
            row = conn.execute(
                """
                INSERT INTO outbox (
                    run_id, unit_id, channel, idempotency_key, payload, status, attempts
                )
                VALUES (%s, %s, %s, %s, %s::jsonb, 'dead', 10)
                RETURNING id
                """,
                (
                    ids["run_id"],
                    ids["unit_id"],
                    channel,
                    f"unit:{ids['unit_id']}:stub_effect:dead-seed",
                    Jsonb({"effect": "stub_effect", "seed": "dead"}),
                ),
            ).fetchone()
        return {**ids, "dead_outbox_id": row["id"]}

    return _seed
