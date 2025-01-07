from fastapi import FastAPI, UploadFile, HTTPException, Query, Body, Request, File, APIRouter,Form
from backend.router import router
from backend.src.endpoints.duplicates import router as duplicates_router
from starlette.requests import Request
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import os
import sys
import pandas as pd
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "src")))

from backend.src.managers.tree_builder import TreeBuilder
from backend.src.managers.tree_modifier import TreeModifier
from backend.src.managers.push_manager import PushManager
from backend.src.utilities.duplicate_resolution import (
    DuplicateResolver,
    KeepFirstStrategy,
    KeepLastStrategy,
    RemoveAllStrategy,
)
import io
from contextlib import redirect_stdout
import json
from seeq.spy.assets import Tree
from typing import List
from backend.router import router

app = FastAPI()

# Include the router
app.include_router(router)
app.include_router(duplicates_router, prefix="/duplicates")

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
    
# Lookup string generation section ---------------------------------------------------

UPLOAD_DIR = "./uploaded_files"
os.makedirs(UPLOAD_DIR, exist_ok=True)

@app.post("/upload_raw_csv/")
async def upload_raw_csv(file: UploadFile = File(...)):
    file_path = os.path.join(UPLOAD_DIR, file.filename)
    with open(file_path, "wb") as buffer:
        buffer.write(await file.read())

    # Basic validation of the uploaded CSV
    try:
        pd.read_csv(file_path)  # Ensure it's readable
        return {"message": f"File '{file.filename}' uploaded successfully."}
    except Exception as e:
        os.remove(file_path)  # Clean up invalid files
        return {"message": f"Error processing file: {e}"}

resolve_duplicates = APIRouter()

@resolve_duplicates.post("/resolve_duplicates/")
async def resolve_duplicates_endpoint(
    file: UploadFile,
    group_column: str = Form(...),  # Group column name
    key_column: str = Form(...),    # Key column name
    value_column: str = Form(...),  # Value column name
    strategy: str = Form(...),      # Duplicate resolution strategy
    rows_to_keep: List[int] = Body(default=None),  # Rows to keep for 'user_specific'
):
    try:
        # Log incoming data for debugging
        print(f"Received strategy: {strategy}")
        print(f"Received rows_to_keep: {rows_to_keep}")

        # Save the uploaded file temporarily
        file_path = f"./uploaded_files/{file.filename}"
        with open(file_path, "wb") as f:
            f.write(await file.read())

        # Load the CSV data
        data = pd.read_csv(file_path)

        # Validate required columns
        for column in [group_column, key_column]:
            if column not in data.columns:
                raise ValueError(f"Column '{column}' not found in the uploaded CSV.")

        # Select the resolution strategy
        if strategy == "keep_first":
            resolver = DuplicateResolver(KeepFirstStrategy())
        elif strategy == "keep_last":
            resolver = DuplicateResolver(KeepLastStrategy())
        elif strategy == "remove_all":
            resolver = DuplicateResolver(RemoveAllStrategy())
        elif strategy == "user_specific":
            if not rows_to_keep:
                raise ValueError("Rows to keep must be specified for 'user_specific' strategy.")
            resolver = DuplicateResolver(UserSpecificStrategy(rows_to_keep))
        else:
            raise ValueError(f"Invalid strategy provided: {strategy}")

        # Group and resolve duplicates
        grouped_data = data.groupby(group_column)
        resolved_data_frames = []
        for group_name, group in grouped_data:
            resolved_group = resolver.resolve_group(group, group_name=group_name, key_column=key_column)
            resolved_data_frames.append(resolved_group)

        # Combine the resolved data
        resolved_data = pd.concat(resolved_data_frames)

        # Save the resolved data
        resolved_file_path = f"./output/resolved_{file.filename}"
        resolved_data.to_csv(resolved_file_path, index=False)

        return {
            "message": "Duplicates resolved successfully.",
            "resolved_file": resolved_file_path,
        }
    except Exception as e:
        print(f"Error in resolve_duplicates_endpoint: {e}")  # Log the error
        raise HTTPException(status_code=500, detail=str(e))

class ParentPathsRequest(BaseModel):
    parent_paths: dict  # { "Group1": "Path1", "Group2": "Path2" }

@app.post("/set_parent_paths/")
async def set_parent_paths(request: ParentPathsRequest):
    resolved_path = os.path.join(UPLOAD_DIR, "resolved_data.csv")
    if not os.path.exists(resolved_path):
        return {"message": "Resolved data file not found. Resolve duplicates first."}

    data = pd.read_csv(resolved_path)
    parent_paths = request.parent_paths

    # Add a "Parent Path" column based on group names
    data["Parent Path"] = data["Group"].map(parent_paths)
    data.to_csv(resolved_path, index=False)
    return {"message": "Parent paths assigned successfully."}

class GenerateLookupRequest(BaseModel):
    output_file: str

@app.post("/generate_lookup/")
async def generate_lookup(request: GenerateLookupRequest):
    resolved_path = os.path.join(UPLOAD_DIR, "resolved_data.csv")
    if not os.path.exists(resolved_path):
        return {"message": "Resolved data file not found. Ensure duplicates are resolved and parent paths are set."}

    data = pd.read_csv(resolved_path)

    # Group data and generate lookup string
    grouped_data = data.groupby("Group")
    lookup_output = []
    for group, group_data in grouped_data:
        table = [[str(row["Key"]), str(row["Value"])] for _, row in group_data.iterrows()]
        formula_parts = [f"['{k}', '{v}']" for k, v in table]
        formula = f"[{', '.join(formula_parts)}]"
        parent_path = group_data["Parent Path"].iloc[0]

        lookup_output.append({
            "Name": f"{group}_LookupString",
            "Formula": formula,
            "Formula Parameters": "{}",
            "Parent Path": parent_path,
        })

    output_path = os.path.join(UPLOAD_DIR, request.output_file)
    with open(output_path, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=["Name", "Formula", "Formula Parameters", "Parent Path"])
        writer.writeheader()
        writer.writerows(lookup_output)

    return {"message": f"Lookup file '{request.output_file}' created successfully.", "output_file": request.output_file}