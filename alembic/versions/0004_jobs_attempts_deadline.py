"""Add jobs attempts / max_attempts / deadline_at for reconcile fixtures.

Revision ID: 0004_jobs_attempts
Revises: 0003_unit_sm
Create Date: 2026-11-06

"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0004_jobs_attempts"
down_revision: str | Sequence[str] | None = "0003_unit_sm"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE jobs
            ADD COLUMN IF NOT EXISTS attempts INT NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS max_attempts INT NOT NULL DEFAULT 3,
            ADD COLUMN IF NOT EXISTS deadline_at TIMESTAMPTZ NULL;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE jobs
            DROP COLUMN IF EXISTS deadline_at,
            DROP COLUMN IF EXISTS max_attempts,
            DROP COLUMN IF EXISTS attempts;
        """
    )
