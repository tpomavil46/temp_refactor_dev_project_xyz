# src/itv_asset_tree/utilities/template_parameters.py

from pydantic import BaseModel, ValidationError
from typing import Dict, Any
import json
import os

class TemplateParameters(BaseModel):
    template_type: str
    parameters: dict

    @classmethod
    def load_schema(cls, schema_path: str):
        try:
            with open(schema_path, 'r') as file:
                logging.info(f"Loading schema from {schema_path}")
                return json.load(file)
        except Exception as e:
            logging.error(f"Error loading schema: {e}")
            raise

    def validate_parameters(self):
        try:
            schema = self.load_schema(os.path.join('schemas', f'{self.template_type}_schema.json'))
            for key in schema:
                if key not in self.parameters:
                    raise ValidationError(f"Missing required parameter: {key}")
        except Exception as e:
            logging.error(f"Parameter validation failed: {e}")
            raise


# Example usage:
# schema = TemplateParameters.load_schema('schemas/hvac_template.json')
# params_dict = schema.to_dict()
