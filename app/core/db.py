from sqlmodel import Session, create_engine, select

# from app import crud
from app.core.config import settings
from app.models import *

# from app.models import User, UserCreate

engine = create_engine(str(settings.SQLALCHEMY_DATABASE_URI))


def get_session():
    """Dependency dla FastAPI do uzyskania sesji bazy danych"""
    with Session(engine) as session:
        yield session


def init_db(session: Session) -> None:
    # Tables should be created with Alembic migrations
    # the tables un-commenting the next lines
    from sqlmodel import SQLModel

    # This works because the models are already imported and registered from app.models
    SQLModel.metadata.create_all(engine)

    # user = session.exec(
    #     select(User).where(User.email == settings.FIRST_SUPERUSER)
    # ).first()
    # if not user:
    #     user_in = UserCreate(
    #         email=settings.FIRST_SUPERUSER,
    #         password=settings.FIRST_SUPERUSER_PASSWORD,
    #         is_superuser=True,
    #     )
    #     user = crud.create_user(session=session, user_create=user_in)
