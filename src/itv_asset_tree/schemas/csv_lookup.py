# src/itv_asset_tree/schemas/csv_lookup.py
from pydantic import BaseModel

class CSVLookupRequest(BaseModel):
    csv_file_path: str

class CSVLookupResponse(BaseModel):
    data: dict