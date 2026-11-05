"""Tracer integrity: unit state check, one job per kind+ref, lease indexes.

Revision ID: 0002_guards
Revises: 0001_init
Create Date: 2026-11-05

"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0002_guards"
down_revision: str | Sequence[str] | None = "0001_init"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE units
            ADD CONSTRAINT units_state_check
            CHECK (state IN ('stub_ready', 'stub_done'));

        ALTER TABLE jobs
            ADD CONSTRAINT jobs_kind_ref_unique UNIQUE (kind, ref_id);

        DROP INDEX IF EXISTS jobs_lease_idx;
        CREATE INDEX jobs_claim_idx ON jobs (kind, created_at)
            WHERE lease_expires_at IS NULL;
        CREATE INDEX jobs_reclaim_idx ON jobs (kind, lease_expires_at)
            WHERE lease_expires_at IS NOT NULL;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP INDEX IF EXISTS jobs_reclaim_idx;
        DROP INDEX IF EXISTS jobs_claim_idx;
        CREATE INDEX jobs_lease_idx ON jobs (kind, lease_expires_at)
            WHERE lease_expires_at IS NULL OR lease_expires_at IS NOT NULL;

        ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_kind_ref_unique;
        ALTER TABLE units DROP CONSTRAINT IF EXISTS units_state_check;
        """
    )
