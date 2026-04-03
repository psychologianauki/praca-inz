import sys
import os
from logging.config import fileConfig

from sqlalchemy import engine_from_config, create_engine
from sqlalchemy import pool
from alembic import context

# ------------------------------------------------------------------------
# 1. Ustawienie ścieżki, aby widzieć folder 'app'
# ------------------------------------------------------------------------
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

# ------------------------------------------------------------------------
# 2. Import Twoich ustawień i Modeli
# ------------------------------------------------------------------------
from app.core.config import settings
from app.models import SQLModel

# ------------------------------------------------------------------------
# 3. Konfiguracja Alembic
# ------------------------------------------------------------------------
config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Przypisujemy metadane SQLModel do Alembika
target_metadata = SQLModel.metadata

def get_url():
    return str(settings.SQLALCHEMY_DATABASE_URI)

def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()

def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    
    # 1. Pobieramy poprawny URL z Twojego configa (Pydantic/Docker)
    db_url = get_url()

    # 2. Zamiast używać engine_from_config (który czasami zaciąga błędne "driver://" z .ini),
    # tworzymy silnik BEZPOŚREDNIO, wymuszając użycie naszego poprawnego URL-a.
    connectable = create_engine(
        db_url,
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection, 
            target_metadata=target_metadata
        )

        with context.begin_transaction():
            context.run_migrations()

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()