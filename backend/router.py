from fastapi import APIRouter
from backend.src.endpoints.duplicates import router as duplicates_router

router = APIRouter()
router.include_router(duplicates_router, prefix="/duplicates", tags=["Duplicates"])