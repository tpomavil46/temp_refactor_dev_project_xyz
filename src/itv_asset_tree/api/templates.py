# src/itv_asset_tree/api/templates.py

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
import pandas as pd
from seeq import spy
import logging
from typing import Optional
import traceback
import json

# Import template classes
from itv_asset_tree.templates.hvac_template import HVAC, HVAC_With_Calcs, Refrigerator, Compressor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()

# Define available templates
TEMPLATE_CLASSES = {
    "HVAC": HVAC,
    "HVAC_With_Calcs": HVAC_With_Calcs,
    "Refrigerator": Refrigerator,  # ✅ Ensure hierarchical templates are included
    "Compressor": Compressor
}

class BuildRequest(BaseModel):
    template_name: str = Field(..., description="The name of the template to apply")
    type: str = Field(..., description="The type of signal (StoredSignal, CalculatedSignal, etc.)")
    search_query: Optional[str] = Field(None, description="Query used to find matching signals")  # ✅ ADDED
    build_asset_regex: str = Field(..., description="Regex to extract asset names from signals")
    build_path: str = Field(..., description="Path where the asset should be built")

    # ✅ Conditionally Required Fields
    datasource_name: Optional[str] = Field(None, description="Datasource Name (Only required for Stored Signals)")
    workbook_name: Optional[str] = Field(None, description="Workbook Name (Required for Seeq push)")

    # ✅ New Fields for Calculated Signals
    base_template: Optional[str] = Field(None, description="Base template used for stored signals")
    calculations_template: Optional[str] = Field(None, description="Template applied to add calculations")

@router.get("/templates/", tags=["Templates"])
async def get_templates():
    """
    Fetches available templates.
    """
    return {"available_templates": list(TEMPLATE_CLASSES.keys())}

@router.get("/templates/hierarchical", tags=["Templates"])
async def get_hierarchical_templates():
    """
    Fetches hierarchical templates that contain nested asset components.
    """
    try:
        logger.info("🔍 Fetching hierarchical templates...")

        hierarchical_templates = {
            name for name, cls in TEMPLATE_CLASSES.items() if hasattr(cls, "build_components")
        }

        if not hierarchical_templates:
            logger.warning("⚠️ No hierarchical templates found.")

        logger.info(f"✅ Hierarchical templates retrieved: {hierarchical_templates}")
        return {"hierarchical_templates": list(hierarchical_templates)}

    except Exception as e:
        logger.error(f"❌ Error fetching hierarchical templates: {e}")
        raise HTTPException(status_code=500, detail=f"❌ Failed to fetch hierarchical templates: {str(e)}")

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

# Define available templates (Hierarchical templates added)
TEMPLATE_CLASSES = {
    "HVAC": HVAC,
    "HVAC_With_Calcs": HVAC_With_Calcs,
    "Refrigerator": Refrigerator,  # Add hierarchical templates
    "Compressor": Compressor
}

# Mapping calculated templates to their base versions
BASE_TEMPLATE_MAP = {template + "_With_Calcs": template for template in TEMPLATE_CLASSES if not template.endswith("_With_Calcs")}

@router.post("/build", tags=["Templates"])
def build_template(request: BuildRequest):
    try:
        logger.info(f"🔍 Received request: {request.dict()}")

        # ✅ Validate required parameters based on type
        if request.type.startswith("Stored") and not request.datasource_name:
            raise HTTPException(status_code=400, detail="🚨 Datasource Name is required for Stored Signals.")
        if request.type.startswith("Calculated") and not request.asset_tree_name:
            raise HTTPException(status_code=400, detail="🚨 Asset Tree Name is required for Calculated Signals.")

        # ✅ Ensure we fetch stored signals first if needed
        if request.type.startswith("Calculated"):
            logger.info(f"🔄 Fetching existing tree: {request.asset_tree_name}")
            existing_tree = fetch_existing_tree(request.asset_tree_name)

            if existing_tree.empty:
                raise HTTPException(status_code=400, detail="🚨 No existing asset tree found in Seeq!")

            search_results = existing_tree
        else:
            logger.info(f"🔎 Running spy.search() with: Name='{request.search_query}', Type='{request.type}', Datasource='{request.datasource_name}'")

            # ✅ Perform search in Seeq
            query_payload = {
                "Name": request.search_query,
                "Type": request.type,
                "Datasource Name": request.datasource_name
            }
            search_results = spy.search(query_payload)

            if search_results.empty:
                raise HTTPException(status_code=400, detail="🚨 No matching signals found in Seeq! Check query.")

            logger.info(f"✅ Retrieved {len(search_results)} results:\n{search_results.head()}")

            # ✅ Ensure required columns exist
            if "Build Path" not in search_results.columns:
                search_results["Build Path"] = request.build_path  # ✅ Set from request
            if "Build Asset" not in search_results.columns:
                search_results["Build Asset"] = search_results["Name"].str.extract(rf'({request.build_asset_regex})')[0]  # ✅ Extract Asset Name

            logger.info(f"📋 Final Processed Search Results:\n{search_results.head()}")

        # ✅ Select template class
        model_class = TEMPLATE_CLASSES.get(request.template_name)
        if not model_class:
            raise HTTPException(status_code=400, detail=f"🚨 Unknown template: {request.template_name}")

        # ✅ Build and push asset tree
        build_df = spy.assets.build(model_class, search_results)
        spy.push(metadata=build_df, workbook="SPy Documentation Examples >> spy.assets")

        logger.info("✅ Successfully pushed to Seeq.")
        return {"message": f"✅ Successfully applied template '{request.template_name}'"}

    except Exception as e:
        logger.error(f"❌ Unexpected Error: {e}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"❌ Failed to apply template: {str(e)}")

def fetch_existing_tree(request: BuildRequest):
    """Helper function to fetch an existing asset tree in Seeq."""
    tree_query = {
        "Name": request.asset_tree_name or request.build_path,  # ✅ Try both name and path
        "Type": "Asset"  # ✅ Searching for an existing tree, NOT raw signals
    }

    logger.info(f"🔎 Searching for existing asset tree: {tree_query}")
    tree_results = spy.search(tree_query)

    if tree_results.empty:
        logger.warning(f"⚠️ No existing asset tree found for '{tree_query['Name']}'!")
        return pd.DataFrame()  # Return empty DF if the tree is missing

    logger.info(f"✅ Found asset tree:\n{tree_results}")
    return tree_results

@router.get("/fetch_signals", tags=["Templates"])
async def fetch_signals(search_query: str, datasource_name: str):
    """
    Fetch available signals from Seeq based on the user's search query.
    """
    try:
        logger.info(f"🔍 Fetching available signals for query: {search_query} in datasource: {datasource_name}")

        query_payload = {
            "Name": search_query,
            "Type": "StoredSignal",
            "Datasource Name": datasource_name
        }
        search_results = spy.search(query_payload)

        if search_results.empty:
            logger.warning("⚠️ No signals found!")
            return {"signals": []}

        signal_names = search_results["Name"].tolist()
        logger.info(f"✅ Found {len(signal_names)} signals.")

        return {"signals": signal_names}

    except Exception as e:
        logger.error(f"❌ Error fetching signals: {e}")
        raise HTTPException(status_code=500, detail=f"❌ Failed to fetch signals: {str(e)}")

@router.get("/fetch_components", tags=["Templates"])
async def fetch_components():
    """
    Fetch available hierarchical components from registered asset templates.
    """
    try:
        logger.info("🔍 Fetching available hierarchical components...")

        components_found = {}

        for template_name, template_class in TEMPLATE_CLASSES.items():
            components = []
            try:
                logger.info(f"🔎 Inspecting template: {template_name}")

                for attr_name in dir(template_class):
                    attr = getattr(template_class, attr_name, None)

                    # ✅ Ensure it’s a method AND is marked with `@Asset.Component()`
                    if callable(attr) and hasattr(attr, "_spy_component"):
                        components.append(attr_name)
                        logger.info(f"✅ Found component '{attr_name}' in {template_name}")

                components_found[template_name] = components

            except Exception as e:
                logger.warning(f"⚠️ Error extracting components from {template_name}: {e}")

        flat_component_list = [comp for comp_list in components_found.values() for comp in comp_list]

        # ✅ If no components were detected, use defaults
        if not flat_component_list:
            logger.warning("⚠️ No hierarchical components detected, using fallback values.")
            flat_component_list = ["Refrigerator", "Compressor", "Motor", "Pump"]

        logger.info(f"📋 Final extracted components: {json.dumps(flat_component_list, indent=2)}")
        return {"components": flat_component_list}

    except Exception as e:
        logger.error(f"❌ Error fetching components: {e}")
        raise HTTPException(status_code=500, detail=f"❌ Failed to fetch components: {str(e)}")

    
@router.post("/build_hierarchical", tags=["Templates"])
def build_hierarchical_template(request: BuildRequest):
    try:
        logger.info(f"🔍 Received request for hierarchical template: {request.dict()}")

        query_payload = {
            "Name": request.search_query,
            "Type": "StoredSignal",
            "Datasource Name": request.datasource_name
        }
        metadata_df = spy.search(query_payload)

        if metadata_df.empty:
            raise HTTPException(status_code=400, detail="🚨 No matching signals found in Seeq!")

        logger.info(f"✅ Retrieved {len(metadata_df)} signals:\n{metadata_df.head()}")

        # ✅ Apply assigned components from UI
        for signal_name, component in request.signal_assignments.items():
            metadata_df.loc[metadata_df["Name"] == signal_name, "Component"] = component

        # ✅ Assign Build Path
        metadata_df["Build Path"] = request.build_path

        # ✅ Select hierarchical model dynamically
        hierarchical_model = TEMPLATE_CLASSES.get(request.template_name)
        if not hierarchical_model:
            raise HTTPException(status_code=400, detail=f"🚨 Unknown hierarchical template: {request.template_name}")

        logger.info(f"📋 Final Processed DataFrame:\n{metadata_df.head()}")

        # ✅ Build and push to Seeq
        build_df = spy.assets.build(hierarchical_model, metadata_df)
        spy.push(metadata=build_df, workbook=request.workbook_name)

        logger.info("✅ Successfully pushed hierarchical template to Seeq.")
        return {"message": f"✅ Successfully applied hierarchical template '{request.template_name}'"}

    except Exception as e:
        logger.error(f"❌ Unexpected Error in build_hierarchical: {e}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"❌ Failed to apply hierarchical template: {str(e)}")
 
@router.post("/build_calculated", tags=["Templates"])
def build_calculated_template(request: BuildRequest):
    try:
        logger.info(f"🔍 Received request for calculated template: {request.dict()}")

        # ✅ Validate required parameters for Calculated Signals
        if not request.base_template:
            raise HTTPException(status_code=400, detail="🚨 Base Template Name is required for Calculated Signals.")
        if not request.calculations_template:
            raise HTTPException(status_code=400, detail="🚨 Calculations Template Name is required for Calculated Signals.")

        # ✅ Fetch stored signals metadata using the base template
        logger.info(f"🔄 Fetching stored signals using base template: {request.base_template}")

        base_request = BuildRequest(
            template_name=request.base_template,
            search_query=request.search_query,
            type="StoredSignal",
            datasource_name=request.datasource_name,
            build_asset_regex=request.build_asset_regex,
            build_path=request.build_path
        )

        base_results = build_template(base_request)  # ✅ Reuse /build to get stored signals

        if not base_results or "message" not in base_results:
            raise HTTPException(status_code=400, detail="🚨 Failed to retrieve base stored signals!")

        # ✅ Convert base results into a dataframe
        logger.info("🔍 Running spy.search() to get base stored signals dataframe...")
        base_df = spy.search({"Name": request.search_query, "Type": "StoredSignal", "Datasource Name": request.datasource_name})

        if base_df.empty:
            raise HTTPException(status_code=400, detail="🚨 No stored signals found in Seeq!")

        logger.info(f"✅ Retrieved {len(base_df)} stored signals:\n{base_df.head()}")

        # ✅ Copy the original metadata but change the build template
        logger.info("📋 Copying metadata dataframe for modification...")
        hvac_with_calcs_metadata_df = base_df.copy()

        # ✅ Ensure required columns are present
        if "Build Path" not in hvac_with_calcs_metadata_df.columns:
            hvac_with_calcs_metadata_df["Build Path"] = request.build_path  # ✅ Set from request
        if "Build Asset" not in hvac_with_calcs_metadata_df.columns:
            hvac_with_calcs_metadata_df["Build Asset"] = hvac_with_calcs_metadata_df["Name"].str.extract(rf'({request.build_asset_regex})')[0]  # ✅ Extract Asset Name

        logger.info(f"📋 Final Processed DataFrame for Calculations:\n{hvac_with_calcs_metadata_df.head()}")

        # ✅ Apply calculations template
        calc_model_class = TEMPLATE_CLASSES.get(request.calculations_template)
        if not calc_model_class:
            raise HTTPException(status_code=400, detail=f"🚨 Unknown calculations template: {request.calculations_template}")

        build_with_calcs_df = spy.assets.build(calc_model_class, hvac_with_calcs_metadata_df)  # ✅ Now has required columns!

        # ✅ Push updated data to Seeq
        logger.info("📤 Pushing updated calculated signals to Seeq...")
        spy.push(metadata=build_with_calcs_df, workbook="SPy Documentation Examples >> spy.assets")

        logger.info("✅ Successfully pushed calculated template to Seeq.")
        return {"message": f"✅ Successfully applied calculated template '{request.calculations_template}'"}

    except Exception as e:
        logger.error(f"❌ Unexpected Error in build_calculated: {e}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"❌ Failed to apply calculated template: {str(e)}")
    
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

    # ✅ Apply regex safely
    if request.build_asset_regex:
        extracted_asset = search_results["Name"].str.extract(rf'({request.build_asset_regex})')
        search_results["Build Asset"] = extracted_asset[0]  # ✅ Avoid assigning a DataFrame
    else:
        search_results["Build Asset"] = None  # Set explicitly to avoid errors

    # ✅ Ensure build path is always defined
    search_results["Build Path"] = request.build_path if request.build_path else "My HVAC Units >> Facility #1"

    return search_results