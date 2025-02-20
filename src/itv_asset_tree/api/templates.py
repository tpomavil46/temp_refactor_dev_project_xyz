from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
import pandas as pd
from seeq import spy
import logging
from typing import Optional
import traceback

# Import template classes
from itv_asset_tree.templates.hvac_template import HVAC

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()

# Define available templates
TEMPLATE_CLASSES = {
    "HVAC": HVAC,
    # Future expansion: "Pumps": PumpTemplate
}

class BuildRequest(BaseModel):
    template_name: str
    search_query: str
    type: str
    datasource_name: str
    build_asset_regex: Optional[str] = None
    build_path: Optional[str] = None


@router.get("/templates/", tags=["Templates"])
async def get_templates():
    """
    Fetches available templates for the UI dropdown.
    """
    templates = [{"name": name, "module": template.__module__} for name, template in TEMPLATE_CLASSES.items()]
    
    if not templates:
        raise HTTPException(status_code=404, detail="No templates found.")
    
    return {"available_templates": templates}


@router.post("/build", tags=["Templates"])
def build_template(request: BuildRequest):
    try:
        logger.info(f"🔍 Received request: {request.dict()}")

        # ✅ Validate required parameters
        if not request.search_query or not request.type or not request.datasource_name:
            raise HTTPException(status_code=400, detail="❌ Missing required search parameters (search_query, type, datasource_name).")

        logger.info(f"🔎 Running spy.search() with: Name='{request.search_query}', Type='{request.type}', Datasource='{request.datasource_name}'")

        # ✅ Perform search in Seeq
        query_payload = {
            "Name": request.search_query,
            "Type": request.type,
            "Datasource Name": request.datasource_name
        }
        search_results = spy.search(query_payload)

        # ✅ Log search results (Check if empty)
        if search_results.empty:
            logger.error("🚨 No matching signals found in Seeq! Check query.")
            raise HTTPException(status_code=400, detail="🚨 No matching signals found in Seeq! Check query.")

        logger.info(f"✅ Found {len(search_results)} signals matching query.")
        logger.info(f"📊 Search Results:\n{search_results.head(5)}")  # Print top 5 results

        # Process search results
        search_results = search_results[["ID", "Name", "Datasource Name"]]

        # ✅ Fix the extraction to avoid setting a DataFrame
        extracted_asset = search_results["Name"].str.extract(rf'({request.build_asset_regex})') if request.build_asset_regex else search_results[["Name"]]
        search_results["Build Asset"] = extracted_asset[0]  # Extract first column as a Series

        search_results["Build Path"] = request.build_path if request.build_path else "My HVAC Units >> Facility #1"

        logger.info(f"✅ Processed search results:\n{search_results.head(5)}")

        # Select model class
        if request.template_name == "HVAC":
            model_class = HVAC
        else:
            raise ValueError(f"Unknown template name: {request.template_name}")

        # Build the asset structure
        build_df = spy.assets.build(model_class, search_results)
        logger.info(f"✅ Build DataFrame:\n{build_df}")

        build_df["Type"].fillna("StoredSignal", inplace=True)

        # Push to Seeq
        spy.push(metadata=build_df, workbook="SPy Documentation Examples >> spy.assets")
        logger.info("✅ Successfully pushed to Seeq.")

        return {"message": f"✅ Successfully applied template '{request.template_name}'"}

    except Exception as e:
        logger.error(f"❌ Unexpected Error: {e}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"❌ Failed to apply template: {str(e)}")


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
                    "search_query": "Name pattern to match signals (e.g., 'Area ?_*')",
                    "type": "Signal Type (e.g., 'StoredSignal')",
                    "datasource_name": "Datasource Name (e.g., 'Example Data')",
                    "build_asset_regex": "Regex to extract asset group (e.g., '(Area .)_.*')",
                    "build_path": "Where to place the built assets (e.g., 'My HVAC Units >> Facility #1')"
                }
            }
        else:
            raise HTTPException(status_code=404, detail=f"Template '{template_name}' not found.")
    except Exception as e:
        return {"error": str(e)}