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
    group_column: str = Form(...),
    key_column: str = Form(...),
    value_column: str = Form(...),
    strategy: str = Form(...),
    rows_to_keep: List[int] = Body(default=None)
):
    try:
        file_path = f"./uploaded_files/{file.filename}"
        with open(file_path, "wb") as f:
            f.write(await file.read())

        data = pd.read_csv(file_path)
        for column in [group_column, key_column, value_column]:
            if column not in data.columns:
                raise ValueError(f"Column '{column}' not found in the uploaded CSV.")

        resolver = None
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
            raise ValueError(f"Invalid strategy: {strategy}")

        grouped_data = data.groupby(group_column)
        resolved_frames = []
        for _, group in grouped_data:
            resolved_frames.append(resolver.resolve_group(group, key_column=key_column))

        resolved_data = pd.concat(resolved_frames)
        resolved_file_path = f"./output/resolved_{file.filename}"
        resolved_data.to_csv(resolved_file_path, index=False)

        return {"message": "Duplicates resolved successfully.", "resolved_file": resolved_file_path}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))