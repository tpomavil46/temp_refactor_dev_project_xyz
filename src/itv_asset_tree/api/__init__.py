# src/itv_asset_tree/api/__init__.py
# initialize the routers for the various endpoints

from fastapi import APIRouter
from .api import router as api_router
from .csv_lookup_generator import router as csv_lookup_generator_router
from .templates import router as templates_router

router = APIRouter()
router.include_router(api_router, prefix="/api/api")
router.include_router(csv_lookup_generator_router, prefix="/api/csv_worklow")
router.include_router(templates_router, prefix="/api/templates")