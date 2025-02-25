from fastapi import FastAPI
from itv_asset_tree.api.api_router import router
from itv_asset_tree.api.startup_handler import connect_to_seeq

app = FastAPI(
    title="ITV Asset Tree API",
    description="API for managing Seeq asset trees and CSV lookups",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.include_router(router)

@app.on_event("startup")
async def startup_event():
    connect_to_seeq()