from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
import os

router = APIRouter()

# Define correct path to templates directory
frontend_dir = os.path.dirname(__file__)
templates_dir = os.path.join(frontend_dir, "templates")

@router.get("/")
async def serve_frontend():
    """Serve the frontend index.html"""
    index_html_path = os.path.join(templates_dir, "index.html")

    # Debugging Output - Log the expected file path
    print(f"🔍 [DEBUG] Expected path for index.html: {index_html_path}")

    if not os.path.exists(index_html_path):
        print(f"❌ [ERROR] File not found at: {index_html_path}")
        raise HTTPException(status_code=500, detail=f"❌ File not found: {index_html_path}")
    
    print(f"✅ [DEBUG] index.html found, serving...")
    return FileResponse(index_html_path)