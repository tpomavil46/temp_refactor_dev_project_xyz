# src/itv_asset_tree/api/api_router.py

from fastapi import APIRouter
from .api import router as api_router
from .csv_workflow import router as csv_workflow_router
from .templates import router as templates_router

router = APIRouter()
router.include_router(api_router, prefix="/api/api")
router.include_router(csv_workflow_router, prefix="/api/csv_workflow")
router.include_router(templates_router, prefix="/api/templates")