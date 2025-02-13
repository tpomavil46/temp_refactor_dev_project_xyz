import os
import io
import uvicorn
import pandas as pd
from contextlib import redirect_stdout
from dotenv import load_dotenv
from seeq import spy
from seeq.spy.assets import Tree

from fastapi import FastAPI, UploadFile, HTTPException, Query, Body, Request, File, Form
from fastapi import APIRouter
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from typing import Optional, Dict

# from itv_asset_tree.router import router
from itv_asset_tree.endpoints.duplicates import router as duplicates_router
from itv_asset_tree.managers.tree_builder import TreeBuilder
from itv_asset_tree.managers.tree_modifier import TreeModifier
from itv_asset_tree.managers.push_manager import PushManager
from itv_asset_tree.managers.tree_manager import TreeManager

# Load environment variables
load_dotenv()
USERNAME = os.getenv("SERVER_USERNAME")
PASSWORD = os.getenv("SERVER_PASSWORD")
HOST = os.getenv("SERVER_HOST")

# Initialize FastAPI app
router = APIRouter(tags=["Asset Tree"])
app = FastAPI()

# Include routers
app.include_router(duplicates_router, prefix="/duplicates")

# Setup CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global state tracking (avoid unnecessary reloading)
current_tree = None
current_workbook_name = "Default Workbook"
current_tree_name = None

# Ensure upload directory exists
UPLOAD_DIR = "./uploaded_files"
os.makedirs(UPLOAD_DIR, exist_ok=True)

# Define frontend directory
frontend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "frontend"))

# Attach Static Files (if serving assets like JS, CSS)
app.mount("/static", StaticFiles(directory=frontend_dir), name="static")

@app.get("/", tags=["Frontend"])
async def serve_frontend():
    """Serve the frontend index.html"""
    index_html_path = os.path.join(frontend_dir, "index.html")
    if not os.path.exists(index_html_path):
        raise RuntimeError(f"❌ File not found: {index_html_path}")
    return FileResponse(index_html_path)

# FastAPI Startup Event for Seeq Login
@app.on_event("startup")
async def startup_event():
    """Executes when FastAPI starts, logging into Seeq."""
    print(f"🔌 Connecting to Seeq at {HOST} as {USERNAME}...")

    # Debugging environment variable loading
    if not USERNAME or not PASSWORD or not HOST:
        print("❌ ERROR: Missing environment variables! Check .env file or OS environment.")
        return

    try:
        spy.options.compatibility = 193
        spy.options.friendly_exceptions = False
        spy.login(url=HOST, username=USERNAME, password=PASSWORD)
        print("✅ Successfully logged into Seeq.")
    except Exception as e:
        print(f"❌ Seeq login failed: {e}")

# Upload CSV File
@router.post("/upload_csv/", tags=["Asset Tree"])
async def upload_csv(file: UploadFile):
    file_location = os.path.join(UPLOAD_DIR, file.filename)
    try:
        with open(file_location, "wb") as f:
            f.write(await file.read())
        data = pd.read_csv(file_location)
        return {"filename": file.filename, "columns": list(data.columns)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload file: {str(e)}")

# Process CSV and Build Tree
@router.post("/process_csv/", tags=["Asset Tree"])
async def process_csv(workbook_name: str = Body(...), tree_name: str = Body(...)):
    global current_tree, current_workbook_name, current_tree_name
    
    # Ensure Seeq login happens here if not already logged in
    if not spy.user:
        print("🔌 Attempting Seeq login at request time...")
        spy.login(url=HOST, username=USERNAME, password=PASSWORD)

    try:
        uploaded_files = os.listdir(UPLOAD_DIR)
        if not uploaded_files:
            raise FileNotFoundError("❌ No CSV file found in the uploaded_files directory.")

        latest_file = max(uploaded_files, key=lambda f: os.path.getctime(os.path.join(UPLOAD_DIR, f)))
        file_path = os.path.join(UPLOAD_DIR, latest_file)

        data = pd.read_csv(file_path)
        if "Level 1" not in data.columns:
            raise ValueError("⚠️ CSV file must contain a 'Level 1' column.")

        current_tree_name = tree_name or data["Level 1"].dropna().unique()[0]
        current_workbook_name = workbook_name or "Default Workbook"

        builder = TreeBuilder(workbook=current_workbook_name, csv_file=file_path)
        builder.parse_csv()
        builder.build_tree_from_csv(friendly_name=current_tree_name, description="🌳 Tree built from CSV")

        current_tree = builder.tree
        push_manager = PushManager(tree=current_tree)
        push_manager.push()

        visualize_output = io.StringIO()
        with redirect_stdout(visualize_output):
            builder.tree.visualize()
        visualization = visualize_output.getvalue()

        return {
            "message": f"✅ CSV processed and tree '{current_tree_name}' pushed successfully.",
            "columns": list(builder.metadata.columns),
            "tree_structure": visualization.strip(),
        }
    except Exception as e:
        return {"message": f"❌ Failed to process and push CSV: {e}"}

# Create Empty Tree
@router.post("/create_empty_tree/", tags=["Asset Tree"])
async def create_empty_tree(request: Request):
    global current_tree, current_workbook_name, current_tree_name

    try:
        body = await request.json()
        tree_name = body.get("tree_name", "").strip()
        workbook_name = body.get("workbook_name", "").strip()

        if not tree_name or not workbook_name:
            raise HTTPException(status_code=400, detail="⚠️ Tree name and workbook name are required.")

        tree_builder = TreeBuilder(workbook=workbook_name)
        current_tree = tree_builder.build_empty_tree(friendly_name=tree_name, description="Empty tree created")

        push_manager = PushManager(tree=current_tree)
        push_manager.push()

        visualization = f"{tree_name}\n|-- (empty root node)"
        return {"message": f"✅ Empty tree '{tree_name}' created and pushed successfully.", "tree_structure": visualization}
    except Exception as e:
        return {"detail": f"❌ Failed to create and push empty tree: {e}"}

@router.get("/search_tree/", tags=["Asset Tree"])
async def search_tree(tree_name: str = Query(...), workbook_name: str = Query(...)):
    try:
        # Initialize TreeModifier and load the tree
        tree_modifier = TreeModifier(workbook=workbook_name, tree_name=tree_name)

        # Capture the visualization output from CLI
        visualize_output = io.StringIO()
        with redirect_stdout(visualize_output):
            tree_modifier.visualize_tree()
        visualization = visualize_output.getvalue()

        return {
            "message": f"✅ Tree '{tree_name}' found and visualized successfully.",
            "tree_structure": visualization.strip()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"❌ Failed to search and visualize tree: {e}")

# Push Tree
@router.post("/push_tree/", tags=["Asset Tree"])
async def push_tree(tree_name: str, workbook_name: str):
    global current_tree, current_tree_name

    try:
        tree_modifier = TreeModifier(workbook=workbook_name, tree_name=tree_name)
        tree_modifier.push_tree()

        current_tree = tree_modifier
        current_tree_name = tree_name

        return {"message": f"✅ Tree '{tree_name}' successfully pushed!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"❌ Failed to push tree: {e}")

# Assuming `current_tree` holds the updated tree in memory
current_tree = None  # Ensure it's defined at the top level

@router.get("/visualize_tree/")
async def visualize_tree(tree_name: str, workbook_name: str):
    """Fetch the latest in-memory tree instead of an old cached version."""
    global current_tree

    print(f"🔍 [DEBUG] Received visualization request for Tree: {tree_name}, Workbook: {workbook_name}")

    # Check if current_tree is already available
    if current_tree and current_tree.name == tree_name:
        print("✅ [DEBUG] Returning in-memory tree visualization.")
        visualization_output = io.StringIO()
        with redirect_stdout(visualization_output):
            current_tree.visualize()
        visualization = visualization_output.getvalue()
        return {"tree_structure": visualization.strip()}

    # Fallback: Fetch from Seeq if not in memory
    try:
        print(f"🔄 [DEBUG] Fetching tree from Seeq: {tree_name}")

        tree_modifier = TreeModifier(workbook=workbook_name, tree_name=tree_name)
        fetched_tree = tree_modifier.tree  # Load tree properly

        if not fetched_tree:
            print(f"❌ [DEBUG] Tree '{tree_name}' not found!")
            return {"error": "Tree not found!"}

        print(f"✅ [DEBUG] Successfully fetched tree: {tree_name}")

        # Update global reference
        current_tree = fetched_tree  

        # Capture visualization output from stdout
        visualization_output = io.StringIO()
        with redirect_stdout(visualization_output):
            fetched_tree.visualize()
        visualization = visualization_output.getvalue()

        print(f"📊 [DEBUG] Tree Visualization Output:\n{visualization}")

        return {"tree_structure": visualization.strip()}

    except Exception as e:
        print(f"❌ [ERROR] Failed to visualize tree: {e}")
        return {"error": f"❌ Failed to visualize tree: {e}"}
    
@router.post("/modify_tree/", tags=["Asset Tree"])
async def modify_tree(
    file: UploadFile,
    tree_name: str = Form(...),
    workbook_name: str = Form(...),
):
    """
    Modify an existing tree using a CSV file.
    """
    try:
        file_path = f"./uploaded_files/{file.filename}"
        os.makedirs("./uploaded_files", exist_ok=True)
        with open(file_path, "wb") as buffer:
            buffer.write(await file.read())

        data = pd.read_csv(file_path)

        if "Parent Path" in data.columns and "Name" in data.columns:
            print("✅ Detected item insertion CSV.")
            tree = Tree.load(workbook=workbook_name, tree=tree_name)

            for _, row in data.iterrows():
                parent_path = row["Parent Path"]
                name = row["Name"]
                formula = row.get("Formula", "")
                formula_params = row.get("Formula Parameters", "{}")

                item_definition = {
                    "Name": name,
                    "Formula": formula,
                    "Formula Parameters": eval(formula_params),
                }

                tree.insert(children=[item_definition], parent=parent_path)

            tree.push()
            return {"message": f"Items from '{file.filename}' inserted successfully."}
        else:
            raise ValueError("⚠️ Unsupported CSV format. Ensure required columns exist.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

class ItemDefinition(BaseModel):
    Name: str
    Type: str
    Formula: Optional[str] = None
    FormulaParams: Optional[Dict] = Field(default_factory=dict)

class InsertItemRequest(BaseModel):
    tree_name: str  # Dynamically received from the request
    workbook_name: str  # Dynamically received from the request
    parent_name: str
    item_definition: ItemDefinition

class ModifyRequest(BaseModel):
    tree_name: str
    workbook_name: str

class InsertRequest(ModifyRequest):
    parent_path: str
    name: str
    formula: str = None
    formula_params: str = None

class MoveRequest(ModifyRequest):
    source_path: str
    destination_path: str

class RemoveRequest(BaseModel):
    tree_name: str
    workbook_name: str
    item_path: str  # Ensure full path is provided

@app.post("/insert_item/")
async def insert_item(request: InsertItemRequest):
    try:
        modifier = TreeModifier(request.workbook_name, request.tree_name)
        parent = request.parent_name
        item_data = request.item_definition.dict()

        if not item_data["Name"] or not item_data["Type"]:
            raise HTTPException(status_code=400, detail="Missing Name or Type.")

        modifier.insert_item(parent, item_data)

        # Force update of current_tree
        global current_tree
        current_tree = modifier.tree

        return {"message": f"✅ Item '{item_data['Name']}' added under '{parent}'."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"❌ Insert failed: {str(e)}")

# @app.post("/move_item/")
# def move_item(request: MoveRequest):
#     """Move an item to a new location in the tree."""
#     print(f"🔍 Received move request: {request.dict()}")  # Debugging line

#     try:
#         modifier = TreeModifier(request.workbook_name, request.tree_name)
#         modifier.move_item(request.source_path, request.destination_path)
#         return {"message": f"✅ Moved item from '{request.source_path}' to '{request.destination_path}'."}
#     except Exception as e:
#         print(f"❌ Move failed: {e}")  # Debug
#         raise HTTPException(status_code=400, detail=str(e))

@app.post("/move_item/")
def move_item(request: MoveRequest):
    try:
        modifier = TreeModifier(request.workbook_name, request.tree_name)
        modifier.move_item(request.source_path, request.destination_path)

        # Force update of current_tree
        global current_tree
        current_tree = modifier.tree

        return {"message": f"✅ Moved item from '{request.source_path}' to '{request.destination_path}'."}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# @app.post("/remove_item/")
# async def remove_item(request: RemoveRequest):
#     """Remove an item from the tree."""
#     try:
#         print(f"🔍 [DEBUG] Received remove request payload: {request.dict()}")  # ✅ Debugging
#         modifier = TreeModifier(request.workbook_name, request.tree_name)
#         modifier.remove_item(request.item_path)
#         return {"message": f"✅ Removed item '{request.item_path}' from the tree."}
#     except Exception as e:
#         print(f"❌ Remove failed: {e}")  # Debug
#         raise HTTPException(status_code=400, detail=str(e))

@app.post("/remove_item/")
async def remove_item(request: RemoveRequest):
    try:
        modifier = TreeModifier(request.workbook_name, request.tree_name)
        modifier.remove_item(request.item_path)

        # Force update of current_tree
        global current_tree
        current_tree = modifier.tree

        return {"message": f"✅ Removed item '{request.item_path}' from the tree."}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
  
# Correctly include the router in FastAPI  
app.include_router(router)

# Function to run FastAPI
def run_server():
    """Run the FastAPI application."""
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
