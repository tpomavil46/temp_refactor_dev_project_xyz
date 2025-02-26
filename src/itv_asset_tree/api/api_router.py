# src/itv_asset_tree/api/api_router.py

from fastapi import APIRouter
from itv_asset_tree.api.api import router as asset_tree_router
from itv_asset_tree.api.routes.tree import router as tree_router
from itv_asset_tree.api.csv_lookup_generator import router as csv_lookup_router
from itv_asset_tree.api.templates import router as templates_router
from itv_asset_tree.web.frontend_router import router as frontend_router

router = APIRouter()
router.include_router(tree_router, prefix="/tree", tags=["Tree"])
router.include_router(tree_router, prefix="/asset-tree", tags=["Asset Tree"])
router.include_router(csv_lookup_router, prefix="/csv-lookup", tags=["CSV Lookup"])
router.include_router(templates_router, prefix="/templates", tags=["Templates"])
router.include_router(frontend_router, prefix="/frontend", tags=["Frontend"])