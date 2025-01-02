from fastapi import FastAPI, UploadFile, Form, HTTPException
from fastapi.staticfiles import StaticFiles
import os
import pandas as pd
from backend.src.managers.tree_builder import TreeBuilder
from backend.src.managers.push_manager import PushManager
from backend.src.utilities.csv_parser import CSVHandler

app = FastAPI()

# Ensure directories for file handling exist
os.makedirs("./uploaded_files", exist_ok=True)

# Serve frontend files
app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")

@app.post("/upload_csv/")
async def upload_csv(file: UploadFile):
    """
    Endpoint to upload a CSV file.
    """
    file_location = f"./uploaded_files/{file.filename}"
    try:
        os.makedirs("./uploaded_files", exist_ok=True)  # Ensure directory exists
        with open(file_location, "wb") as f:
            f.write(await file.read())
        # Example: Process the file here (e.g., read columns for validation)
        data = pd.read_csv(file_location)
        return {"filename": file.filename, "columns": list(data.columns)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload file: {str(e)}")

@app.post("/create_lookup/")
async def create_lookup(
    group_column: str = Form(...),
    key_column: str = Form(...),
    value_column: str = Form(...),
):
    # Your endpoint logic here
    pass

@app.post("/push_tree/")
async def push_tree(tree_name: str = Form(...)):
    # Your endpoint logic here
    pass

@app.get("/visualize_tree/")
def visualize_tree():
    # Your endpoint logic here
    pass

@app.get("/")
async def read_root():
    return {
        "message": "Welcome to the API!",
        "endpoints": [
            {"path": "/upload_csv/", "method": "POST", "description": "Upload a CSV file"},
            {"path": "/create_lookup/", "method": "POST", "description": "Create a lookup table"},
            {"path": "/push_tree/", "method": "POST", "description": "Push the tree to Seeq"},
            {"path": "/visualize_tree/", "method": "GET", "description": "Visualize the asset tree"},
        ]
    }
    
@app.get("/visualize_tree/")
def visualize_tree():
    """
    Endpoint to visualize the tree structure.
    """
    try:
        # Example: Replace with your tree.visualize() logic
        tree_structure = {
            "Root": {
                "Child1": {"Subchild1": {}, "Subchild2": {}},
                "Child2": {"Subchild3": {}, "Subchild4": {}},
            }
        }
        return {"tree_structure": tree_structure}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to visualize tree: {str(e)}")