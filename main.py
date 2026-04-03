import logging
import os

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlmodel import Session, create_engine

from app.api import api_router

from app.core.config import settings


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(filename)s %(message)s")

engine = create_engine(str(settings.SQLALCHEMY_DATABASE_URI), echo=False)


def get_session():
    """
    Dependency function - dostarcza sesję bazy danych do endpointów.
    Zamyka sesję automatycznie po zakończeniu żądania.
    """
    with Session(engine) as session:
        yield session


app = FastAPI(
    title="Inzynierka API",
    description="API do prognozowania cen energii i analizy danych",
    version="1.0.0",
    openapi_url=f"{settings.API_V1_STR}/openapi.json",
)

# CORS
if settings.all_cors_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[str(origin) for origin in settings.all_cors_origins],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

app.include_router(api_router, prefix=settings.API_V1_STR)

if __name__ == "__main__":
    # To pozwala uruchomić serwer komendą `python main.py`
    # host="0.0.0.0" jest wymagany w Dockerze, aby wystawić serwer na zewnątrz kontenera
    reload = os.getenv("UVICORN_RELOAD", "0") == "1"
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=reload)
