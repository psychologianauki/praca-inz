"""Remove gas_pln

Revision ID: 48b19eb1fa59
Revises: 0e70adcf7700
Create Date: 2026-04-02 06:53:57.954345

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '48b19eb1fa59'
down_revision: Union[str, Sequence[str], None] = '0e70adcf7700'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute(sa.text("ALTER TABLE ml_features DROP COLUMN IF EXISTS gas_pln;"))


def downgrade() -> None:
    """Downgrade schema."""
    op.execute(
        sa.text(
            "ALTER TABLE ml_features ADD COLUMN IF NOT EXISTS gas_pln DOUBLE PRECISION;"
        )
    )
