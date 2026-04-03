"""add unique constraint to weather_forecast stacja_nazwa czas_prognozy

Revision ID: b3e7f9a2c1d8
Revises: 9c1f6a2d4b11
Create Date: 2026-03-15 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "b3e7f9a2c1d8"
down_revision: Union[str, None] = "9c1f6a2d4b11"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_weather_forecast_stacja_czas",
        "weather_forecast",
        ["stacja_nazwa", "czas_prognozy"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_weather_forecast_stacja_czas",
        "weather_forecast",
        type_="unique",
    )
