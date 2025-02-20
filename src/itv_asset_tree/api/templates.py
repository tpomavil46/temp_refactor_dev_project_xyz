# src/itv_asset_tree/api/templates.py

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
import pandas as pd
from seeq import spy
import logging
from typing import Optional
import traceback

# Import template classes
from itv_asset_tree.templates.hvac_template import HVAC, HVAC_With_Calcs

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()

# Define available templates
TEMPLATE_CLASSES = {
    "HVAC": HVAC,
    "HVAC_With_Calcs": HVAC_With_Calcs,
    # Future expansion: "Pumps": PumpTemplate
}

class BuildRequest(BaseModel):
    template_name: str
    search_query: str
    type: str
    datasource_name: str
    build_asset_regex: Optional[str] = None
    build_path: Optional[str] = Field("My HVAC Units >> Facility #1", description="Path for asset placement")

@router.get("/templates/", tags=["Templates"])
async def get_templates():
    """
    Fetches available templates.
    """
    return {"available_templates": list(TEMPLATE_CLASSES.keys())}

@router.get("/templates/{template_name}/parameters", tags=["Templates"])
async def get_template_parameters(template_name: str):
    """
    Returns required parameters for the given template.
    """
    try:
        if template_name not in TEMPLATE_CLASSES:
            raise HTTPException(status_code=404, detail=f"Template '{template_name}' not found.")

        return {
            "template_name": template_name,
            "required_parameters": TEMPLATE_CLASSES[template_name].get_required_parameters()
        }
    except Exception as e:
        return {"error": str(e)}

# Define available templates
TEMPLATE_CLASSES = {
    "HVAC": HVAC,
    "HVAC_With_Calcs": HVAC_With_Calcs,
    # Future expansion: "Pumps": PumpTemplate, "Pumps_With_Calcs": PumpTemplateWithCalcs
}

# Mapping calculated templates to their base versions
BASE_TEMPLATE_MAP = {template + "_With_Calcs": template for template in TEMPLATE_CLASSES if not template.endswith("_With_Calcs")}

# @router.post("/build", tags=["Templates"])
# def build_template(request: BuildRequest):
#     try:
#         logger.info(f"🔍 Received request: {request.dict()}")

#         if not request.search_query or not request.type or not request.datasource_name:
#             raise HTTPException(status_code=400, detail="❌ Missing required search parameters (search_query, type, datasource_name).")

#         # ✅ Determine if this is a calculated template and fetch base stored signals first
#         base_template = request.template_name.replace("_With_Calcs", "") if "_With_Calcs" in request.template_name else None

#         if base_template:
#             logger.info(f"🔄 Detected calculated template '{request.template_name}'. Fetching stored signals from '{base_template}' first...")

#             # ✅ Fetch base template signals using StoredSignal type
#             base_request = BuildRequest(
#                 template_name=base_template,
#                 search_query=request.search_query,
#                 type="StoredSignal",  # ✅ Always fetch stored signals first
#                 datasource_name=request.datasource_name,
#                 build_asset_regex=request.build_asset_regex,
#                 build_path=request.build_path
#             )

#             base_search_results = fetch_base_metadata(base_request)

#             if base_search_results.empty:
#                 raise HTTPException(status_code=400, detail=f"🚨 No matching stored signals found for base template '{base_template}'!")

#             logger.info(f"✅ Successfully retrieved base metadata from '{base_template}'. Now applying calculations...")

#             # ✅ Apply the requested calculated template on top of stored signals
#             model_class = TEMPLATE_CLASSES[request.template_name]
#             calculated_df = spy.assets.build(model_class, base_search_results)

#             # ✅ Merge stored signals and calculated attributes
#             build_df = pd.concat([base_search_results, calculated_df], ignore_index=True)

#         else:
#             # ✅ Perform search for non-calculated templates
#             query_payload = {
#                 "Name": request.search_query,
#                 "Type": request.type,  # ✅ Dynamically passed type
#                 "Datasource Name": request.datasource_name
#             }
#             search_results = spy.search(query_payload)

#             if search_results.empty:
#                 raise HTTPException(status_code=400, detail="🚨 No matching signals found in Seeq!")

#             search_results = search_results[["ID", "Name", "Datasource Name"]]

#             # ✅ Apply regex if provided
#             extracted_asset = search_results["Name"].str.extract(rf'({request.build_asset_regex})') if request.build_asset_regex else search_results[["Name"]]
#             search_results["Build Asset"] = extracted_asset[0]
#             search_results["Build Path"] = request.build_path

#             # ✅ Standard processing for stored signals
#             model_class = TEMPLATE_CLASSES[request.template_name]
#             build_df = spy.assets.build(model_class, search_results)

#         # ✅ Inspect for NaN before pushing
#         if build_df.isna().any().any():
#             logger.warning(f"⚠️ Detected NaN values in DataFrame before push:\n{build_df.isna().sum()}")

#         # ✅ Fix NaN values before push
#         build_df.fillna("", inplace=True)

#         logger.info(f"✅ Final Build DataFrame before push:\n{build_df}")

#         # ✅ Ensure 'Formula Parameters' is correctly formatted
#         if "Formula Parameters" in build_df.columns:
#             logger.info("🔍 Validating 'Formula Parameters' before push...")

#             def validate_formula_parameters(value):
#                 """Ensures each formula parameter follows 'var=ID' or 'var=Path' format or defaults to {}."""
#                 if pd.isna(value) or value == "":
#                     return {}  # ✅ Replace NaN or empty values with an empty dictionary
#                 if isinstance(value, dict):
#                     formatted_params = {}
#                     for k, v in value.items():
#                         if isinstance(v, dict) and "ID" in v:
#                             formatted_params[k] = f"{k}={v['ID']}"
#                         elif isinstance(v, dict) and "Path" in v:
#                             formatted_params[k] = f"{k}={v['Path']}"
#                         else:
#                             logger.warning(f"⚠️ Unexpected format for formula parameter: {k} -> {v}")
#                     return formatted_params
#                 return value  # ✅ Return as-is if already correctly formatted

#             # ✅ Only process if the column exists
#             build_df["Formula Parameters"] = build_df["Formula Parameters"].apply(validate_formula_parameters)

#             # ✅ Log any remaining problematic entries
#             invalid_entries = build_df[~build_df["Formula Parameters"].apply(lambda x: isinstance(x, dict))]
#             if not invalid_entries.empty:
#                 logger.warning(f"⚠️ Found invalid formula parameters:\n{invalid_entries[['Formula Parameters']]}")

#             logger.info(f"✅ Final Build DataFrame before push:\n{build_df}")

#         # ✅ Push to Seeq after cleaning formula parameters
#         spy.push(metadata=build_df, workbook="SPy Documentation Examples >> spy.assets")
#         logger.info(f"✅ Successfully pushed '{request.template_name}' to Seeq.")
        
#         # ✅ Push to Seeq
#         spy.push(metadata=build_df, workbook="SPy Documentation Examples >> spy.assets")
#         logger.info(f"✅ Successfully pushed '{request.template_name}' to Seeq.")

#         return {"message": f"✅ Successfully applied template '{request.template_name}'"}

#     except Exception as e:
#         logger.error(f"❌ Unexpected Error: {e}\n{traceback.format_exc()}")
#         raise HTTPException(status_code=500, detail=f"❌ Failed to apply template: {str(e)}")

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
    
def fetch_base_metadata(request: BuildRequest):
    """Helper function to retrieve base signals from Seeq before applying calculations."""
    query_payload = {
        "Name": request.search_query,
        "Type": "StoredSignal",  # ✅ Always fetch stored signals first
        "Datasource Name": request.datasource_name
    }
    logger.info(f"🔎 Fetching base metadata with: {query_payload}")

    search_results = spy.search(query_payload)

    if search_results.empty:
        logger.warning(f"⚠️ No matching stored signals found for '{request.template_name}'.")
        return pd.DataFrame()  # Return empty DF if no signals found

    search_results = search_results[["ID", "Name", "Datasource Name"]]

    # ✅ Apply regex if needed
    extracted_asset = search_results["Name"].str.extract(rf'({request.build_asset_regex})') if request.build_asset_regex else search_results[["Name"]]
    search_results["Build Asset"] = extracted_asset[0]
    search_results["Build Path"] = request.build_path

    return search_results