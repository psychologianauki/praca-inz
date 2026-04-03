from fastapi import APIRouter
from ...endpoints.fetch_all import router as fetch_all_router

router = APIRouter(prefix="/data", tags=["data"])

router.include_router(fetch_all_router)

