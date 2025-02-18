# src/itv_asset_tree/endpoints/template_api.py

from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from itv_asset_tree.managers.template_manager import AcceleratorTemplateManager
from itv_asset_tree.utilities.template_loader import TemplateLoader
from itv_asset_tree.utilities.template_parameters import TemplateParameters
from itv_asset_tree.utilities.schema_manager import SchemaManager
import os, json, logging

import pandas as pd
print("Pandas Version: ", pd.__version__)
print("Pandas Location: ", pd.__file__)

logging.basicConfig(level=logging.INFO)

router = APIRouter()

TEMPLATE_DIR = "src/itv_asset_tree/templates"

@router.get("/templates/")
def list_templates():
    try:
        loader = TemplateLoader(TEMPLATE_DIR)
        templates = loader.list_available_templates()
        return {"available_templates": templates}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/templates/load/")
def load_template(template_type: str = Form(...)):
    try:
        loader = TemplateLoader(TEMPLATE_DIR)
        template = loader.load_template(template_type)
        return {"template": template}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/templates/apply/")
def apply_template(template_type: str = Form(...), parameters: str = Form(...), asset_tree_name: str = Form(...)):
    try:
        logging.info(f"Received Data: {template_type}, {parameters}, {asset_tree_name}")
        parameters_dict = json.loads(parameters)
        schema_manager = SchemaManager()
        manager = AcceleratorTemplateManager(template_type, parameters_dict, schema_manager)

        template = manager.load_template()
        logging.info(f"Loaded Template: {template}")

        configured_template = manager.configure_template(template)
        logging.info(f"Configured Template: {configured_template}")

        # Generate 'items' column dynamically
        configured_template['items'] = [f"{asset_tree_name}/{name}" for name in configured_template.index]
        logging.info(f"Final DataFrame before push: {configured_template}")

        try:
            manager.apply_template(configured_template, asset_tree_name)
        except Exception as push_error:
            logging.error(f"Push Error: {push_error}")
            raise HTTPException(status_code=500, detail=f"Push Error: {push_error}")

        return {"message": f"Template '{template_type}' applied to asset tree '{asset_tree_name}' successfully."}
    except Exception as e:
        logging.error(f"Error during template application: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
