# src/itv_asset_tree/utilities/template_loader.py

import logging
import os
import json
import yaml
from csv import DictReader
from seeq import spy

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TemplateLoader:
    def __init__(self, template_directory: str):
        self.template_directory = template_directory

    def load_template(self, template_type: str):
        try:
            ext = os.path.splitext(template_type)[1]
            path = f"{self.template_directory}/{template_type}"
            logging.info(f"Loading template from {path}")
            if ext == '.json':
                with open(path, 'r') as f:
                    return json.load(f)
            elif ext == '.yaml' or ext == '.yml':
                with open(path, 'r') as f:
                    return yaml.safe_load(f)
            elif ext == '.csv':
                with open(path, 'r') as f:
                    return list(DictReader(f))
            else:
                raise ValueError("Unsupported file format")
        except Exception as e:
            logging.error(f"Error loading template: {e}")
            raise

    def list_available_templates(self):
        try:
            templates = [f for f in os.listdir(self.template_directory) if f.endswith(('.json', '.yaml', '.yml', '.csv'))]
            logging.info(f"Available templates: {templates}")
            return templates
        except Exception as e:
            logging.error(f"Error listing templates: {e}")
            raise
