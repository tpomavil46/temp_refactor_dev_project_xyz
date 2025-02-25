# src/itv_asset_tree/api/app.py

import os
from fastapi import FastAPI
from dotenv import load_dotenv
from itv_asset_tree.api.api_router import router
from seeq import spy

# Load environment variables
load_dotenv()
HOST = os.getenv("SERVER_HOST")
USERNAME = os.getenv("SERVER_USERNAME")
PASSWORD = os.getenv("SERVER_PASSWORD")

# Create FastAPI instance
app = FastAPI(
    title="ITV Asset Tree API",
    description="API for managing Seeq asset trees, lookup workflows, and duplicate resolution.",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# Register all API routers
app.include_router(router)

# FastAPI Startup Event for Seeq Login
@app.on_event("startup")
async def startup_event():
    """Executes when FastAPI starts, logging into Seeq."""
    print(f"🔌 Connecting to Seeq at {HOST} as {USERNAME}...")
    try:
        spy.options.compatibility = 193
        spy.options.friendly_exceptions = False
        spy.login(url=HOST, username=USERNAME, password=PASSWORD)
        print("✅ Successfully logged into Seeq.")
    except Exception as e:
        print(f"❌ Seeq login failed: {e}")