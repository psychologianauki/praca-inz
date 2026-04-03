#  Migracja bazy danych - Nowe tabele predykcji

"""
Migracja dodająca tabele dla systemu predykcji cen energii:
- EnergyPricePredictions - tabela z predykcjami cen
- ModelMetadata - tabela z metadanymi modeli ML

Revision ID: prediction_system_v1
Revises: b3adc48319e8
Create Date: 2024-01-01 12:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers
revision = 'prediction_system_v1'
down_revision = '02d7d7be9161'  # Zmieniamy na najnowszy head
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Tworzenie tabeli EnergyPricePredictions
    op.create_table(
        'energy_price_predictions',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('prediction_date', sa.Date(), nullable=False, comment='Data dla której jest predykcja'),
        sa.Column('predicted_price', sa.Numeric(precision=10, scale=2), nullable=False, comment='Przewidywana cena'),
        sa.Column('confidence_lower', sa.Numeric(precision=10, scale=2), nullable=True, comment='Dolna granica confidence interval'),
        sa.Column('confidence_upper', sa.Numeric(precision=10, scale=2), nullable=True, comment='Górna granica confidence interval'),
        sa.Column('model_version', sa.String(50), nullable=False, comment='Wersja modelu użytego do predykcji'),
        sa.Column('features_used', postgresql.JSON(), nullable=True, comment='JSON z listą features użytych w modelu'),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Indeksy dla wydajności
    op.create_index('ix_energy_price_predictions_prediction_date', 'energy_price_predictions', ['prediction_date'])
    op.create_index('ix_energy_price_predictions_model_version', 'energy_price_predictions', ['model_version'])
    op.create_index('ix_energy_price_predictions_created_at', 'energy_price_predictions', ['created_at'])
    
    # Unique constraint - jedna predykcja na dzień na model
    op.create_unique_constraint(
        'uq_prediction_date_model', 
        'energy_price_predictions', 
        ['prediction_date', 'model_version']
    )

    # Tworzenie tabeli ModelMetadata
    op.create_table(
        'model_metadata',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('model_name', sa.String(100), nullable=False, comment='Nazwa modelu (np. RandomForest, GradientBoosting)'),
        sa.Column('version', sa.String(50), nullable=False, comment='Wersja modelu'),
        sa.Column('parameters', postgresql.JSON(), nullable=True, comment='Parametry modelu jako JSON'),
        sa.Column('training_data_from', sa.Date(), nullable=True, comment='Początek okresu danych treningowych'),
        sa.Column('training_data_to', sa.Date(), nullable=True, comment='Koniec okresu danych treningowych'),
        sa.Column('feature_importance', postgresql.JSON(), nullable=True, comment='Ważność features jako JSON'),
        sa.Column('validation_score', sa.Numeric(precision=10, scale=6), nullable=True, comment='Wynik walidacji modelu'),
        sa.Column('mae', sa.Numeric(precision=10, scale=2), nullable=True, comment='Mean Absolute Error'),
        sa.Column('rmse', sa.Numeric(precision=10, scale=2), nullable=True, comment='Root Mean Square Error'),
        sa.Column('r2_score', sa.Numeric(precision=10, scale=6), nullable=True, comment='R² Score'),
        sa.Column('model_file_path', sa.String(500), nullable=True, comment='Ścieżka do zapisanego modelu'),
        sa.Column('is_active', sa.Boolean(), nullable=False, default=True, comment='Czy model jest aktywny'),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Indeksy dla ModelMetadata
    op.create_index('ix_model_metadata_model_name', 'model_metadata', ['model_name'])
    op.create_index('ix_model_metadata_version', 'model_metadata', ['version'])
    op.create_index('ix_model_metadata_is_active', 'model_metadata', ['is_active'])
    op.create_index('ix_model_metadata_created_at', 'model_metadata', ['created_at'])
    
    # Unique constraint - jedna wersja modelu o danej nazwie
    op.create_unique_constraint(
        'uq_model_name_version', 
        'model_metadata', 
        ['model_name', 'version']
    )

    # Dodanie trigger'ów do auto-update updated_at
    op.execute("""
        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ language 'plpgsql';
    """)
    
    op.execute("""
        CREATE TRIGGER update_energy_price_predictions_updated_at 
        BEFORE UPDATE ON energy_price_predictions 
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    """)
    
    op.execute("""
        CREATE TRIGGER update_model_metadata_updated_at 
        BEFORE UPDATE ON model_metadata 
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    """)

    # Dodanie komentarzy do tabel
    op.execute("COMMENT ON TABLE energy_price_predictions IS 'Tabela z predykcjami cen energii wygenerowanymi przez modele ML'")
    op.execute("COMMENT ON TABLE model_metadata IS 'Metadane modeli ML używanych do predykcji cen energii'")


def downgrade() -> None:
    # Usuwanie trigger'ów
    op.execute("DROP TRIGGER IF EXISTS update_model_metadata_updated_at ON model_metadata;")
    op.execute("DROP TRIGGER IF EXISTS update_energy_price_predictions_updated_at ON energy_price_predictions;")
    op.execute("DROP FUNCTION IF EXISTS update_updated_at_column();")
    
    # Usuwanie tabel w odwrotnej kolejności
    op.drop_table('model_metadata')
    op.drop_table('energy_price_predictions')