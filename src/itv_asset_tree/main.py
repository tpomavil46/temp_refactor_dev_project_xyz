# src/itv_asset_tree/main.py

import uvicorn
from itv_asset_tree.api.app import app

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)