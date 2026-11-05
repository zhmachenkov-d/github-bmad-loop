"""Widen stub unit states; event_log.payload for SM apply audit.

Revision ID: 0003_unit_sm
Revises: 0002_guards
Create Date: 2026-11-06

"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0003_unit_sm"
down_revision: str | Sequence[str] | None = "0002_guards"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE units DROP CONSTRAINT IF EXISTS units_state_check;
        ALTER TABLE units
            ADD CONSTRAINT units_state_check
            CHECK (state IN (
                'stub_ready',
                'stub_running',
                'stub_paused',
                'stub_done',
                'stub_failed'
            ));

        ALTER TABLE event_log
            ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}';
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE event_log DROP COLUMN IF EXISTS payload;

        ALTER TABLE units DROP CONSTRAINT IF EXISTS units_state_check;
        UPDATE units
        SET state = 'stub_done'
        WHERE state NOT IN ('stub_ready', 'stub_done');
        ALTER TABLE units
            ADD CONSTRAINT units_state_check
            CHECK (state IN ('stub_ready', 'stub_done'));
        """
    )
