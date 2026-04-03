"""add missing columns to ml_features

Revision ID: 0e70adcf7700
Revises: 75d679ac2464
Create Date: 2026-03-19 15:22:17.883784

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0e70adcf7700'
down_revision: Union[str, Sequence[str], None] = '75d679ac2464'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('ml_features', sa.Column('rain_forecast', sa.Float(), nullable=True))
    op.add_column('ml_features', sa.Column('precipitation_forecast', sa.Float(), nullable=True))
    op.add_column('ml_features', sa.Column('wind_direction_forecast', sa.Float(), nullable=True))
    op.add_column('ml_features', sa.Column('terrestrial_radiation_forecast', sa.Float(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('ml_features', 'terrestrial_radiation_forecast')
    op.drop_column('ml_features', 'wind_direction_forecast')
    op.drop_column('ml_features', 'precipitation_forecast')
    op.drop_column('ml_features', 'rain_forecast')
