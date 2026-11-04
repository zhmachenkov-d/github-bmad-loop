"""Initial control-plane DDL: runs, units, jobs, event_log, outbox, signals.

Revision ID: 0001_init
Revises:
Create Date: 2026-11-04

"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "0001_init"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE runs (
            id          UUID PRIMARY KEY,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE units (
            id          UUID PRIMARY KEY,
            run_id      UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
            state       TEXT NOT NULL,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE INDEX units_run_idx ON units (run_id);
        CREATE INDEX units_state_idx ON units (state);

        CREATE TABLE jobs (
            id                UUID PRIMARY KEY,
            kind              TEXT NOT NULL CHECK (kind IN ('run', 'unit')),
            ref_id            UUID NOT NULL,
            lease_id          UUID,
            leased_by         TEXT,
            lease_expires_at  TIMESTAMPTZ,
            version           BIGINT NOT NULL DEFAULT 0,
            created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE INDEX jobs_lease_idx ON jobs (kind, lease_expires_at)
            WHERE lease_expires_at IS NULL OR lease_expires_at IS NOT NULL;
        CREATE INDEX jobs_ref_idx ON jobs (kind, ref_id);

        CREATE TABLE event_log (
            id          BIGSERIAL PRIMARY KEY,
            run_id      UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
            unit_id     UUID REFERENCES units(id) ON DELETE CASCADE,
            from_state  TEXT,
            to_state    TEXT NOT NULL,
            event_type  TEXT NOT NULL,
            worker_id   TEXT,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE INDEX event_log_run_idx ON event_log (run_id, id);
        CREATE INDEX event_log_unit_idx ON event_log (unit_id, id);

        CREATE TABLE outbox (
            id               BIGSERIAL PRIMARY KEY,
            run_id           UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
            unit_id          UUID REFERENCES units(id) ON DELETE CASCADE,
            channel          TEXT NOT NULL,
            idempotency_key  TEXT NOT NULL UNIQUE,
            payload          JSONB NOT NULL DEFAULT '{}',
            status           TEXT NOT NULL DEFAULT 'pending'
                             CHECK (status IN ('pending', 'sent', 'failed', 'dead')),
            attempts         INT NOT NULL DEFAULT 0,
            next_attempt_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            external_id      TEXT,
            created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
            sent_at          TIMESTAMPTZ
        );
        CREATE INDEX outbox_pending_idx ON outbox (next_attempt_at)
            WHERE status = 'pending';

        CREATE TABLE signals (
            id           BIGSERIAL PRIMARY KEY,
            run_id       UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
            unit_id      UUID REFERENCES units(id) ON DELETE CASCADE,
            kind         TEXT NOT NULL,
            data         JSONB,
            consumed_at  TIMESTAMPTZ,
            created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE INDEX signals_pending_idx ON signals (run_id)
            WHERE consumed_at IS NULL;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP TABLE IF EXISTS signals;
        DROP TABLE IF EXISTS outbox;
        DROP TABLE IF EXISTS event_log;
        DROP TABLE IF EXISTS jobs;
        DROP TABLE IF EXISTS units;
        DROP TABLE IF EXISTS runs;
        """
    )
