from fastapi import FastAPI, UploadFile, HTTPException, Query, Body, Request
from starlette.requests import Request
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import os
import pandas as pd
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from backend.src.managers.tree_builder import TreeBuilder
import io
from contextlib import redirect_stdout
import json

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

@app.post("/process_csv/")
async def process_csv():
    try:
        # Get the latest uploaded file
        uploaded_files = os.listdir("./uploaded_files")
        if not uploaded_files:
            raise FileNotFoundError("No CSV file found in the uploaded_files directory.")

        latest_file = max(uploaded_files, key=lambda f: os.path.getctime(f"./uploaded_files/{f}"))
        file_path = f"./uploaded_files/{latest_file}"

        # Parse the CSV file
        data = pd.read_csv(file_path)
        if "Level 1" not in data.columns:
            raise ValueError("CSV file must contain a 'Level 1' column.")

        # Use the first unique value from 'Level 1' as the tree name
        tree_name = data["Level 1"].dropna().unique()[0]
        print(f"Using tree name from 'Level 1': {tree_name}")

        # Initialize the TreeBuilder
        builder = TreeBuilder(workbook="Your Workbook Name", csv_file=file_path)
        builder.parse_csv()
        builder.build_tree_from_csv(friendly_name=tree_name, description="Tree built from CSV")

        # Capture the output of Tree.visualize()
        visualize_output = io.StringIO()
        with redirect_stdout(visualize_output):
            builder.tree.visualize()
        visualization = visualize_output.getvalue()

        return {
            "message": "CSV processed successfully.",
            "columns": list(builder.metadata.columns),
            "tree_structure": visualization.strip()  # Send the visualization output
        }
    except FileNotFoundError as e:
        return {
            "message": f"Failed to process CSV: {e}",
            "tree_structure": None
        }
    except Exception as e:
        return {
            "message": f"An unexpected error occurred: {e}",
            "tree_structure": None
        }

class TreeRequest(BaseModel):
    tree_name: str

@app.post("/create_empty_tree/")
async def create_empty_tree(request: Request):
    try:
        # Parse the JSON body to extract the tree name
        body = await request.json()
        tree_name = body.get("tree_name", "").strip()

        if not tree_name:
            raise HTTPException(status_code=400, detail="Tree name is missing or empty.")

        # Create the empty tree
        tree_builder = TreeBuilder(workbook="Your Workbook Name")
        created_tree = tree_builder.build_empty_tree(friendly_name=tree_name, description="Empty tree created")

        # Generate visualization
        visualization = f"{tree_name}\n|-- (empty root node)"

        return {
            "message": f"Empty tree '{tree_name}' created successfully.",
            "tree_structure": visualization
        }
    except Exception as e:
        return {"detail": f"Failed to create empty tree: {e}"}
        
@app.get("/search_tree/")
async def search_tree(tree_name: str = Query(...)):
    try:
        builder = TreeBuilder(workbook="Your Workbook Name")
        tree = builder.search_tree_by_name(tree_name)  # Assuming this function exists
        if not tree:
            return {"message": f"Tree '{tree_name}' not found.", "tree_structure": None}

        tree_structure = tree.visualize()
        return {"message": "Tree found.", "tree_structure": tree_structure}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to search tree: {e}")