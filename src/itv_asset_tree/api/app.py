# src/itv_asset_tree/api/app.py
from fastapi import FastAPI
from itv_asset_tree.api.api_router import router
from itv_asset_tree.api.startup_handler import connect_to_seeq
from itv_asset_tree.config import settings
from itv_asset_tree.api.routes import item  # Ensure this import exists

app = FastAPI(
    title=settings.app_name,
    description="API for managing Seeq asset trees and CSV lookups",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    debug=settings.debug,  # Keeping debug setting from config
)

# Include all routers
app.include_router(router)
app.include_router(item.router, prefix="/api/v1", tags=["items"])

# Startup event for connecting to Seeq
@app.on_event("startup")
async def startup_event():
    connect_to_seeq()