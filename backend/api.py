from fastapi import FastAPI, UploadFile, HTTPException, Query, Body, Request
from starlette.requests import Request
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import os
import pandas as pd
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from backend.src.managers.tree_builder import TreeBuilder
from backend.src.managers.tree_modifier import TreeModifier
from backend.src.managers.push_manager import PushManager
import io
from contextlib import redirect_stdout
import json
from seeq.spy.assets import Tree

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="frontend"), name="static")

@app.get("/")
async def serve_frontend():
    return FileResponse(os.path.join("frontend", "index.html"))

@app.post("/upload_csv/")
async def upload_csv(file: UploadFile):
    file_location = f"./uploaded_files/{file.filename}"
    try:
        os.makedirs("./uploaded_files", exist_ok=True)
        with open(file_location, "wb") as f:
            f.write(await file.read())
        data = pd.read_csv(file_location)
        return {"filename": file.filename, "columns": list(data.columns)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload file: {str(e)}")

# Global variables for current tree state
current_tree = None
current_workbook_name = "Default Workbook"  # Default value if not provided
current_tree_name = None

@app.post("/process_csv/")
async def process_csv(
    workbook_name: str = Body(..., embed=True), 
    tree_name: str = Body(..., embed=True)
):
    global current_tree, current_workbook_name, current_tree_name

    try:
        uploaded_files = os.listdir("./uploaded_files")
        if not uploaded_files:
            raise FileNotFoundError("No CSV file found in the uploaded_files directory.")

        latest_file = max(uploaded_files, key=lambda f: os.path.getctime(f"./uploaded_files/{f}"))
        file_path = f"./uploaded_files/{latest_file}"

        # Parse the CSV file
        data = pd.read_csv(file_path)
        if "Level 1" not in data.columns:
            raise ValueError("CSV file must contain a 'Level 1' column.")

        # Update tree name and workbook name from the request body
        current_tree_name = tree_name or data["Level 1"].dropna().unique()[0]
        current_workbook_name = workbook_name or "Default Workbook"

        # Initialize the TreeBuilder
        builder = TreeBuilder(workbook=current_workbook_name, csv_file=file_path)
        builder.parse_csv()
        builder.build_tree_from_csv(friendly_name=current_tree_name, description="Tree built from CSV")

        # Store the tree in the global variable
        current_tree = builder.tree

        # Push the tree using PushManager
        push_manager = PushManager(tree=current_tree)
        push_manager.push()

        # Capture the output of Tree.visualize()
        visualize_output = io.StringIO()
        with redirect_stdout(visualize_output):
            builder.tree.visualize()
        visualization = visualize_output.getvalue()

        return {
            "message": f"CSV processed and tree '{current_tree_name}' pushed successfully.",
            "columns": list(builder.metadata.columns),
            "tree_structure": visualization.strip()
        }
    except Exception as e:
        return {"message": f"Failed to process and push CSV: {e}"}

class TreeRequest(BaseModel):
    tree_name: str

@app.post("/create_empty_tree/")
async def create_empty_tree(request: Request):
    global current_tree, current_workbook_name, current_tree_name

    try:
        # Parse the JSON body to extract tree and workbook names
        body = await request.json()
        tree_name = body.get("tree_name", "").strip()
        workbook_name = body.get("workbook_name", "").strip()

        if not tree_name or not workbook_name:
            raise HTTPException(status_code=400, detail="Tree name and workbook name are required.")

        # Set the global variables
        current_tree_name = tree_name
        current_workbook_name = workbook_name

        # Create the empty tree
        tree_builder = TreeBuilder(workbook=current_workbook_name)
        current_tree = tree_builder.build_empty_tree(friendly_name=current_tree_name, description="Empty tree created")

        # Push the tree using PushManager
        push_manager = PushManager(tree=current_tree)
        push_manager.push()

        # Generate visualization
        visualization = f"{current_tree_name}\n|-- (empty root node)"

        return {
            "message": f"Empty tree '{current_tree_name}' created and pushed successfully.",
            "tree_structure": visualization,
        }
    except Exception as e:
        return {"detail": f"Failed to create and push empty tree: {e}"}
        
@app.get("/search_tree/")
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
            "message": f"Tree '{tree_name}' found and visualized successfully.",
            "tree_structure": visualization.strip()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to search and visualize tree: {e}")
    
@app.post("/push_tree/")
async def push_tree(tree_name: str = Body(..., embed=True), workbook_name: str = Body(..., embed=True)):
    global current_tree, current_workbook_name, current_tree_name

    try:
        # Debugging logs
        print(f"Received tree_name: {tree_name}")
        print(f"Received workbook_name: {workbook_name}")
        print(f"Current tree: {current_tree}")
        print(f"Current workbook_name: {current_workbook_name}")
        print(f"Current tree_name: {current_tree_name}")

        # Validate input
        if not tree_name or not workbook_name:
            raise HTTPException(status_code=400, detail="Tree name and workbook name are required.")

        # Decide which tree to push
        if current_tree and current_tree_name == tree_name and current_workbook_name == workbook_name:
            # Use the current global tree
            print("Using the currently loaded tree for pushing.")
            tree_to_push = current_tree
        else:
            # Attempt to load the existing tree
            print("Rebuilding the tree from scratch.")
            tree_modifier = TreeModifier(workbook=workbook_name, tree_name=tree_name)
            tree_to_push = tree_modifier.tree
            current_tree = tree_to_push  # Update global state
            current_tree_name = tree_name
            current_workbook_name = workbook_name

        # Push the tree using PushManager
        push_manager = PushManager(tree=tree_to_push)
        push_manager.push()

        return {"message": f"Tree '{tree_name}' pushed successfully to workbook '{workbook_name}'."}
    except ValueError as e:
        print(f"Tree loading error: {e}")
        raise HTTPException(status_code=400, detail=f"Tree loading failed: {e}")
    except Exception as e:
        print(f"Error in push_tree: {e}")  # Log the error for debugging
        raise HTTPException(status_code=500, detail=f"Failed to push tree: {e}")
    
@app.get("/visualize_tree/")
async def visualize_tree():
    try:
        global current_tree, current_tree_name
        if current_tree is None:
            raise HTTPException(status_code=404, detail="No tree loaded to visualize.")

        # Capture the visualization output from CLI
        visualize_output = io.StringIO()
        with redirect_stdout(visualize_output):
            current_tree.visualize()
        visualization = visualize_output.getvalue()

        return {
            "message": f"Tree '{current_tree_name}' visualized successfully.",
            "tree_structure": visualization.strip()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to visualize tree: {e}")