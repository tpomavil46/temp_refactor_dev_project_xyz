# src/itv_asset_tree/api/templates.py

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import pandas as pd
from seeq import spy
import logging
from itv_asset_tree.services.template_loader import TemplateLoader
from itv_asset_tree.services.template_builder import TemplateBuilder
# import sys
import traceback

# sys.setrecursionlimit(100)

# Import template classes here:
from itv_asset_tree.templates.hvac_template import HVAC

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()
loader = TemplateLoader()
builder = TemplateBuilder()

@router.get("/templates/", tags=["Templates"])
async def get_templates():
    """
    Fetches available templates.
    """
    templates = loader.load_templates()
    if not templates:
        raise HTTPException(status_code=404, detail="No templates found.")
    return {"available_templates": templates}

class BuildRequest(BaseModel):
    template_name: str
    metadata: list  # Expecting a list of dicts for metadata DataFrame

@router.post("/build", tags=["Templates"])
def build_template(request: BuildRequest):
    try:
        logger.info(f"🔍 Received request: template_name={request.template_name}, metadata={request.metadata}")

        # Process metadata and search results
        metadata_df = pd.DataFrame(request.metadata)
        use_search_results_only = metadata_df.empty

        if use_search_results_only:
            logger.warning("⚠️ metadata_df is empty. Using search_results as the base DataFrame.")

        # Extract Datasource Name
        datasource_name = metadata_df["Datasource Name"].iloc[0] if "Datasource Name" in metadata_df.columns else "Example Data"
        logger.info(f"🔍 Using Datasource Name: {datasource_name}")

        # Run spy.search()
        query_payload = {
            "Name": "Area ?_*",
            "Type": "StoredSignal",
            "Datasource Name": datasource_name
        }
        logger.info(f"🔍 Running spy.search() with query: {query_payload}")
        search_results = spy.search(query_payload)

        if search_results.empty:
            raise HTTPException(status_code=400, detail="🚨 No matching signals found in Seeq! Check signal names and datasource.")

        # Process search results
        search_results = search_results[["ID", "Name", "Datasource Name"]]
        search_results["Build Asset"] = search_results["Name"].str.extract(r'(Area .)_.*')
        search_results["Build Path"] = "My HVAC Units >> Facility #1"

        metadata_df = search_results.copy() if use_search_results_only else metadata_df.merge(search_results, on="Name", how="left")

        # Select model class
        if request.template_name == "HVAC":
            model_class = HVAC
        else:
            raise ValueError(f"Unknown template name: {request.template_name}")

        # Build the asset structure
        build_df = spy.assets.build(model_class, metadata_df)
        logger.info(f"✅ Build DataFrame:\n{build_df}")

        # Ensure correct Type values
        build_df["Type"].fillna("StoredSignal", inplace=True)

        logger.info(f"✅ FINAL DataFrame before push:\n{build_df}")

        # Push to Seeq
        spy.push(metadata=build_df, workbook="SPy Documentation Examples >> spy.assets")
        logger.info("✅ Successfully pushed to Seeq.")

        # **Fix the Response Issue: Convert DataFrame to JSON-Safe List of Dicts**
        response_data = build_df.astype(str).to_dict(orient="records")

        return {
            "status": "success",
            "num_records": len(response_data),
            "records": response_data[:5]  # Only return a preview of 5 records
        }

    except ValueError as e:
        logger.error(f"❌ ValueError: {e}\n{traceback.format_exc()}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"❌ Unexpected Error: {e}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/templates/{template_name}/parameters", tags=["Templates"])
async def get_template_parameters(template_name: str):
    """
    Returns required parameters for the given template.
    """
    try:
        if template_name == "HVAC":
            return {
                "template_name": "HVAC",
                "required_parameters": {
                    "temperature_signal": "string (Tag name of temperature signal)",
                    "pressure_signal": "string (Tag name of pressure signal)",
                    "flow_signal": "string (Tag name of flow signal)"
                }
            }
        else:
            raise HTTPException(status_code=404, detail=f"Template '{template_name}' not found.")
    except Exception as e:
        return {"error": str(e)}