from fastapi import APIRouter, UploadFile, HTTPException, Form, Body, File
import pandas as pd
from typing import List
import traceback
from backend.src.utilities.duplicate_resolution import (
    DuplicateResolver,
    KeepFirstStrategy,
    KeepLastStrategy,
    RemoveAllStrategy,
    UserSpecificStrategy,
)

router = APIRouter()

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
                raise ValueError(f"Column '{column}' not found in the uploaded CSV.")

        duplicates = data[data.duplicated(subset=[group_column, key_column], keep=False)]
        duplicates_json = duplicates.to_dict(orient="records")

        return {"message": "Duplicates found!", "duplicates": duplicates_json}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint to resolve duplicates
@router.post("/resolve_duplicates/")
async def resolve_duplicates_endpoint(
    file: UploadFile,
    group_column: str = Form(...),  # Group column name
    key_column: str = Form(...),    # Key column name
    value_column: str = Form(...),  # Value column name
    strategy: str = Form(...),      # Duplicate resolution strategy
    rows_to_keep: List[int] = Body(default=None),  # Rows to keep for 'user_specific'
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

        # Save the resolved data to resolved_data.csv
        resolved_file_path = "./output/resolved_data.csv"
        resolved_data.to_csv(resolved_file_path, index=False)

        # Return success response with the next step instructions
        return {
            "message": f"Duplicates resolved successfully. File saved as {resolved_file_path}.",
            "next_step": "Specify Parent Paths for groups to continue."
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/generate_lookup/")
async def generate_lookup(output_file: str = Form(...)):
    try:
        # Read resolved data from resolved_data.csv
        resolved_file_path = "./output/resolved_data.csv"
        if not os.path.exists(resolved_file_path):
            raise ValueError("Resolved data file not found. Resolve duplicates first.")

        data = pd.read_csv(resolved_file_path)

        # Group data and generate lookup strings
        grouped_data = data.groupby("Group")
        lookup_output = []
        for group, group_data in grouped_data:
            table = [[row["Key"], row["Value"]] for _, row in group_data.iterrows()]
            formula_parts = [f"['{k}', '{v}']" for k, v in table]
            formula = f"[{', '.join(formula_parts)}]"
            parent_path = group_data["Parent Path"].iloc[0] if "Parent Path" in group_data.columns else ""

            lookup_output.append({
                "Name": f"{group}_LookupString",
                "Formula": formula,
                "Formula Parameters": "{}",
                "Parent Path": parent_path,
            })

        # Save to lookup_output.csv
        output_path = f"./output/{output_file}"
        pd.DataFrame(lookup_output).to_csv(output_path, index=False)

        return {
            "message": f"Lookup file '{output_file}' created successfully.",
            "output_file": output_path,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))