"""
Add gas_eur column to ml_features table

Revision ID: 20260318_add_gas
Revises: 55d5a145e349
Create Date: 2026-03-18
"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '20260318_add_gas'
down_revision: Union[str, Sequence[str], None] = '55d5a145e349'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade():
    op.add_column('ml_features', sa.Column('gas_eur', sa.Float(), nullable=True))

def downgrade():
    op.drop_column('ml_features', 'gas_eur')
