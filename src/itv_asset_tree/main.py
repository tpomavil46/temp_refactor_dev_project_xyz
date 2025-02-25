# src/itv_asset_tree/main.py

from fastapi import FastAPI
import uvicorn
from itv_asset_tree.api.app import app
from itv_asset_tree.api.api_router import router as api_router

app = FastAPI()
app.include_router(api_router)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)