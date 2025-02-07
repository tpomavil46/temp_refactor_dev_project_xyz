from fastapi import APIRouter, UploadFile, HTTPException, Form, Body, File
from pydantic import BaseModel
import pandas as pd
from typing import List, Dict
import traceback
import json
import os
from backend.src.utilities.duplicate_resolution import (
    DuplicateResolver,
    KeepFirstStrategy,
    KeepLastStrategy,
    RemoveAllStrategy,
    UserSpecificStrategy,
)
from backend.src.utilities.csv_parser import CSVHandler
from backend.src.utilities.lookup_builder import LookupTableBuilder
from backend.src.managers.tree_modifier import TreeModifier

UPLOAD_DIR = "./output" # Directory to store uploaded files

router = APIRouter()

@router.post("/upload_raw_csv/")
async def upload_raw_csv(file: UploadFile = File(...)):
    """
    Endpoint to upload and validate a raw CSV file.
    """
    try:
        # Ensure the output directory exists
        os.makedirs(UPLOAD_DIR, exist_ok=True)

        # Save the file
        file_path = os.path.join(UPLOAD_DIR, file.filename)
        with open(file_path, "wb") as buffer:
            buffer.write(await file.read())

        # Validate the file (ensure it's a readable CSV)
        pd.read_csv(file_path)  # This will raise an error if not a valid CSV
        return {"message": f"✅ File '{file.filename}' uploaded successfully."}
    except Exception as e:
        print(f"❌ Error uploading raw CSV: {str(e)}")
        raise HTTPException(status_code=500, detail=f"❌ Failed to upload raw CSV: {str(e)}")

# Endpoint to identify duplicates
@router.post("/get_duplicates/")
async def get_duplicates(
    file: UploadFile,
    group_column: str = Form(...),
    key_column: str = Form(...),
    value_column: str = Form(...)
):
    try:
        file_path = f"./uploaded_files/{file.filename}"
        with open(file_path, "wb") as f:
            f.write(await file.read())

        data = pd.read_csv(file_path)
        for column in [group_column, key_column, value_column]:
            if column not in data.columns:
                raise ValueError(f"❌ Column '{column}' not found in the uploaded CSV.")

        duplicates = data[data.duplicated(subset=[group_column, key_column], keep=False)]
        duplicates_json = duplicates.to_dict(orient="records")

        return {"message": "Duplicates found!", "duplicates": duplicates_json}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Endpoint to resolve duplicates
@router.post("/resolve_duplicates/")
async def resolve_duplicates_endpoint(
    file: UploadFile,
    group_column: str = Form(...),
    key_column: str = Form(...),
    value_column: str = Form(...),
    rows_to_remove: str = Form(default=None)  # Rows to explicitly remove
):
    try:
        # Save the uploaded file temporarily
        file_path = f"./uploaded_files/{file.filename}"
        with open(file_path, "wb") as f:
            f.write(await file.read())

        # Load the CSV data
        data = pd.read_csv(file_path)

        # Validate required columns
        for column in [group_column, key_column, value_column]:
            if column not in data.columns:
                raise ValueError(f"❌ Column '{column}' not found in the uploaded CSV.")

        # Parse rows_to_remove if provided
        rows_to_remove = json.loads(rows_to_remove) if rows_to_remove else []

        # Create a mask to exclude selected rows
        rows_to_keep = data.index.difference(rows_to_remove)

        # Filter the data
        resolved_data = data.loc[rows_to_keep]

        # Save the resolved data to resolved_data.csv
        resolved_file_path = "./output/resolved_data.csv"
        resolved_data.to_csv(resolved_file_path, index=False)

        return {
            "message": "✅ Duplicates resolved successfully. Resolved data saved.",
            "resolved_file": resolved_file_path,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# function to get the names of lookup strings
@router.get("/names/")
async def get_lookup_string_names():
    """
    Fetch names for lookup strings from the resolved_data.csv file.
    """
    resolved_path = os.path.join(UPLOAD_DIR, "resolved_data.csv")
    if not os.path.exists(resolved_path):
        return {"lookup_names": []}

    # Load the resolved_data.csv file
    data = pd.read_csv(resolved_path)

    # Ensure that the expected column for grouping exists
    if "Equipment_Desc" not in data.columns:  # Replace with your "Group" column name
        return {"lookup_names": []}

    # Generate lookup string names
    lookup_names = [f"{group.replace(' ', '_')}_LookupString" for group in data["Equipment_Desc"].unique()]
    return {"lookup_names": lookup_names}

# function to generate lookup strings
@router.post("/generate_lookup/")
async def generate_lookup(
    group_column: str = Form(...),
    key_column: str = Form(...),
    value_column: str = Form(...),
    output_file: str = Form(...)
):
    resolved_path = os.path.join(UPLOAD_DIR, "resolved_data.csv")
    if not os.path.exists(resolved_path):
        return {"message": "❌ Resolved data file not found. Ensure duplicates are resolved first."}

    # Load resolved data
    csv_handler = CSVHandler(resolved_path)
    resolved_data = csv_handler.load_csv()

    # Generate lookup table
    lookup_builder = LookupTableBuilder(group_column, key_column, value_column)
    lookup_data = lookup_builder.build(resolved_data)

    # Save initial lookup output
    parent_paths = {f"{group.replace(' ', '_')}_LookupString": "Set this path (i.e. Reactor Plant >> Reactor 1)" for group in lookup_data.keys()}  # Ensure _LookupString is appended
    output_path = os.path.join(UPLOAD_DIR, output_file)
    lookup_builder.save_lookup_to_csv(lookup_data, parent_paths, output_path)

    return {"message": f"✅ Lookup file '{output_file}' created successfully.", "output_file": output_file}

class ParentPathsRequest(BaseModel):
    parent_paths: Dict[str, str]  # Example: {"GroupName_LookupString": "ParentPath"}
    group_column: str  # Column to group data by
    key_column: str  # Column for the key values
    value_column: str  # Column for the value descriptions

@router.post("/set_parent_paths/")
async def set_parent_paths(request: ParentPathsRequest):
    """
    Assign Parent Paths to lookup strings and save the final lookup_output.csv.
    """
    try:
        resolved_path = os.path.join(UPLOAD_DIR, "resolved_data.csv")
        if not os.path.exists(resolved_path):
            raise HTTPException(status_code=404, detail="❌ Resolved data file not found.")

        # Load the resolved data
        data = pd.read_csv(resolved_path)

        # Ensure required columns are present
        group_column = request.group_column
        key_column = request.key_column
        value_column = request.value_column

        for column in [group_column, key_column, value_column]:
            if column not in data.columns:
                raise HTTPException(
                    status_code=422,
                    detail=f"❌ Column '{column}' not found in resolved data."
                )

        # Assign parent paths from the request
        parent_paths = request.parent_paths
        if not parent_paths:
            raise HTTPException(status_code=400, detail="❌ Parent paths are missing.")

        print("✔️ Parent Paths received:", parent_paths)
        print("✔️ Received request payload:", request.dict())

        # Generate lookup strings and assign parent paths
        lookup_builder = LookupTableBuilder(group_column, key_column, value_column)
        lookup_tables = lookup_builder.build(data)

        lookup_data = []
        for name, formula in lookup_tables.items():
            # Normalize name for matching
            normalized_name = f"{name.replace(' ', '_')}_LookupString"
            parent_path = parent_paths.get(normalized_name, "Root Asset")
            lookup_data.append({
                "Name": normalized_name,
                "Formula": str(formula).replace('"', "'"),
                "Formula Parameters": "{}",
                "Parent Path": parent_path,
            })

        # Save the final lookup_output.csv
        output_file = os.path.join(UPLOAD_DIR, "lookup_output.csv")
        lookup_df = pd.DataFrame(lookup_data)
        lookup_df.to_csv(output_file, index=False)

        return {"message": f"✅ Lookup file created successfully and saved to {output_file}."}
    except Exception as e:
        print("❌ Error during processing:", str(e))
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/push_lookup/")
async def push_lookup(tree_name: str = Form(...), workbook_name: str = Form(...)):
    """
    Pushes lookup_output.csv to the specified tree in Seeq.
    """
    try:
        print(f"📌 Received request to push lookup. Tree: {tree_name}, Workbook: {workbook_name}")

        lookup_file = os.path.join(UPLOAD_DIR, "lookup_output.csv")
        if not os.path.exists(lookup_file):
            print("❌ lookup_output.csv NOT FOUND!")
            raise HTTPException(status_code=404, detail="❌ lookup_output.csv not found. Ensure it has been generated.")

        print(f"✅ Found lookup_output.csv at: {lookup_file}")

        # Load the lookup CSV
        data = pd.read_csv(lookup_file)
        print(f"📊 Loaded CSV with {len(data)} rows")

        # Load the tree
        tree_modifier = TreeModifier(workbook=workbook_name, tree_name=tree_name)
        print("🌳 TreeModifier initialized.")

        # Insert lookup items
        for _, row in data.iterrows():
            parent_path = row["Parent Path"].strip()
            name = row["Name"].strip()
            
            formatted_formula = f'"{row["Formula"]}"'  
            formula_parameters = row.get("Formula Parameters", "{}")

            try:
                formula_parameters = json.loads(formula_parameters) if formula_parameters.strip() else {}
            except json.JSONDecodeError:
                print(f"❌ Invalid JSON in Formula Parameters: {formula_parameters}")
                raise HTTPException(status_code=400, detail="❌ Invalid JSON in Formula Parameters")

            item_definition = {
                "Name": name,
                "Formula": formatted_formula,
                "Formula Parameters": formula_parameters,
            }

            print(f"➕ Inserting '{name}' under '{parent_path}' with formula: {formatted_formula}")

            tree_modifier.tree.insert(children=[item_definition], parent=parent_path)
    
        # Push the tree to Seeq
        print("🚀 Pushing tree to Seeq...")
        tree_modifier.tree.push()
        print("✅ Lookup table successfully pushed!")

        # 🔥 **FIX: Reload `current_tree` after push**
        global current_tree, current_tree_name
        tree_modifier = TreeModifier(workbook=workbook_name, tree_name=tree_name)  # Reload tree from Seeq
        current_tree = tree_modifier.tree  # Assign updated tree
        current_tree_name = tree_name  # Track tree name
        
        print("✅ Tree successfully reloaded into memory after push.")

        return {"message": "Lookup table successfully pushed to Seeq."}

    except Exception as e:
        print(f"❌ ERROR pushing lookup table: {str(e)}")
        raise HTTPException(status_code=500, detail=f"❌ Error pushing lookup table: {str(e)}")