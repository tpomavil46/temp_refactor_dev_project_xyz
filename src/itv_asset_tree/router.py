# API Router for the ITV Asset Tree API Lookup String creation and Duplicates resolution.

from fastapi import APIRouter
from itv_asset_tree.endpoints.duplicates import router as duplicates_router

router = APIRouter()

# Include Duplicates Router
router.include_router(duplicates_router, prefix="/duplicates", tags=["Duplicates"])

# Import `api.py` LAST to prevent circular import
from itv_asset_tree.api import router as asset_tree_router  
router.include_router(asset_tree_router, prefix="")  # Asset Tree Endpoints