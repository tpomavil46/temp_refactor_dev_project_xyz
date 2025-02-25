# src/itv_asset_tree/main.py

from fastapi import FastAPI
import uvicorn
from itv_asset_tree.api.app import app
from itv_asset_tree.api.api_router import router as api_router
from contextlib import asynccontextmanager
from itv_asset_tree.api.startup_handler import connect_to_seeq
from itv_asset_tree.utils.logger import log_info

@asynccontextmanager
async def lifespan(app: FastAPI):
    log_info("🔌 Starting application and logging into Seeq...")
    connect_to_seeq()
    yield
    log_info("Shutting down application...")

app = FastAPI(lifespan=lifespan)

app.include_router(api_router)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)