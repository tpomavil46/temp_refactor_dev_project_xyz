# src/itv_asset_tree/api/routes/csv_lookup_generator.py
from fastapi import APIRouter, Depends, BackgroundTasks
from sqlalchemy.orm import Session
from itv_asset_tree.api.dependencies import get_db
from itv_asset_tree.schemas.csv_lookup import CSVLookupRequest, CSVLookupResponse
from itv_asset_tree.services.csv_lookup_service import generate_lookup
from itv_asset_tree.utils.logger import log_info

router = APIRouter()

def background_csv_processing(request: CSVLookupRequest, db: Session):
    """Handles CSV processing in the background."""
    log_info("🛠️ Processing CSV file in the background...")
    response = generate_lookup(request, db)  # Long-running task
    log_info("✅ CSV processing completed!")
    return response

@router.post("/generate-lookup", response_model=CSVLookupResponse)
async def generate_csv_lookup(
    request: CSVLookupRequest, background_tasks: BackgroundTasks, db: Session = Depends(get_db)
):
    """Starts CSV lookup generation as a background task."""
    background_tasks.add_task(background_csv_processing, request, db)
    return {"message": "CSV processing started in the background!"}