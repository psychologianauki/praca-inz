from fastapi import APIRouter

from .routes import data
from .endpoints import ml

api_router = APIRouter()
api_router.include_router(data.router)
api_router.include_router(ml.router, prefix="/ml", tags=["machine-learning"])
