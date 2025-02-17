import os
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI
from itv_asset_tree.router import router
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

def main():
    """Start the FastAPI server."""
    host = os.getenv("SERVER_HOST", "127.0.0.1")
    port = int(os.getenv("SERVER_PORT", 8000))

    print(f"🚀 Starting FastAPI on {host}:{port}")
    uvicorn.run("itv_asset_tree.main:app", host=host, port=port, reload=True)

if __name__ == "__main__":
    main()