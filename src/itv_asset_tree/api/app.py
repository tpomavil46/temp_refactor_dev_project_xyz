# src/itv_asset_tree/api/app.py
from fastapi import FastAPI
from itv_asset_tree.api.startup_handler import connect_to_seeq
from itv_asset_tree.config import settings
from itv_asset_tree.utils.logger import log_info, log_error
from pathlib import Path
import importlib

@app.middleware("http")
async def log_requests(request: Request, call_next):
    log_info(f"📥 Incoming request: {request.method} {request.url}")
    response = await call_next(request)
    log_info(f"📤 Response status: {response.status_code}")
    return response

app = FastAPI(
    title=settings.app_name,
    description="API for managing Seeq asset trees and CSV lookups",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    debug=settings.debug,
)

# Dynamically include all routers from the api/routes directory
routes_path = Path(__file__).parent / "routes"
for route_file in routes_path.glob("*.py"):
    if route_file.stem != "__init__":
        module_name = f"itv_asset_tree.api.routes.{route_file.stem}"
        module = importlib.import_module(module_name)
        if hasattr(module, "router"):
            app.include_router(getattr(module, "router"))

@app.on_event("startup")
async def startup_event():
    connect_to_seeq()