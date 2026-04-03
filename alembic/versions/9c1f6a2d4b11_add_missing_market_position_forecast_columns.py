"""add missing market position forecast columns

Revision ID: 9c1f6a2d4b11
Revises: f75ca21a6d1c
Create Date: 2026-03-15 14:10:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "9c1f6a2d4b11"
down_revision = "f75ca21a6d1c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Added with IF NOT EXISTS to be idempotent across environments.
    op.execute(
        sa.text(
            """
            ALTER TABLE aggregated_market_position
            ADD COLUMN IF NOT EXISTS sk_d1_fcst DOUBLE PRECISION;
            """
        )
    )
    op.execute(
        sa.text(
            """
            ALTER TABLE aggregated_market_position
            ADD COLUMN IF NOT EXISTS sk_d_fcst DOUBLE PRECISION;
            """
        )
    )


def downgrade() -> None:
    op.execute(
        sa.text(
            """
            ALTER TABLE aggregated_market_position
            DROP COLUMN IF EXISTS sk_d1_fcst;
            """
        )
    )
    op.execute(
        sa.text(
            """
            ALTER TABLE aggregated_market_position
            DROP COLUMN IF EXISTS sk_d_fcst;
            """
        )
    )
