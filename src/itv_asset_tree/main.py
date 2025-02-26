from fastapi import FastAPI
import uvicorn
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

from itv_asset_tree.api.api_router import router as api_router
from itv_asset_tree.api.startup_handler import connect_to_seeq
from itv_asset_tree.utils.logger import log_info

# ✅ Set up Jinja templates
templates = Jinja2Templates(directory="src/itv_asset_tree/web/templates")

# ✅ FastAPI setup
app = FastAPI()

# ✅ Serve static files (CSS, JS, images)
app.mount("/static", StaticFiles(directory="src/itv_asset_tree/web/static"), name="static")

# ✅ Ensure Seeq login happens on startup
@app.on_event("startup")
async def startup_event():
    log_info("🔌 Starting application and logging into Seeq...")
    connect_to_seeq()

# ✅ Serve the UI
@app.get("/", response_class=HTMLResponse)
async def serve_index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# ✅ Include API routes
app.include_router(api_router)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)