from fastapi import FastAPI, Request
from itv_asset_tree.api.startup_handler import connect_to_seeq
from itv_asset_tree.config import settings
from itv_asset_tree.utils.logger import log_info
from pathlib import Path
import importlib

app = FastAPI(
    title="ITV Asset Tree API",
    description="API for managing Seeq asset trees and CSV lookups",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

@app.on_event("startup")
async def startup_event():
    """Executes when FastAPI starts, ensuring Seeq login occurs."""
    log_info("🚀 FastAPI startup initiated...")
    connect_to_seeq()  # ✅ Ensure Seeq login runs at startup

# ✅ Middleware for logging requests
@app.middleware("http")
async def log_requests(request: Request, call_next):
    log_info(f"📥 Incoming request: {request.method} {request.url}")
    response = await call_next(request)
    log_info(f"📤 Response status: {response.status_code}")
    return response

# ✅ Dynamically include all routers from the `api/routes` directory
routes_path = Path(__file__).parent / "routes"
for route_file in routes_path.glob("*.py"):
    if route_file.stem != "__init__":
        module_name = f"itv_asset_tree.api.routes.{route_file.stem}"
        module = importlib.import_module(module_name)
        if hasattr(module, "router"):
            app.include_router(getattr(module, "router"))