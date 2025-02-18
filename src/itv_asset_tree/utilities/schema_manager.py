from pydantic import BaseModel
from typing import Dict, Any
import json
import os
import logging

class SchemaManager:
    """Manages loading, validation, and dynamic fetching of JSON schemas."""

    def __init__(self, schema_directory: str = "schemas"):
        self.schema_directory = schema_directory
        self.logger = logging.getLogger(__name__)

    def load_schema(self, template_type: str) -> Dict[str, Any]:
        """Loads the JSON schema for the given template type."""
        schema_path = os.path.join(self.schema_directory, f"{template_type}_schema.json")
        if not os.path.exists(schema_path):
            self.logger.error(f"Schema file not found: {schema_path}")
            raise FileNotFoundError(f"Schema file not found: {schema_path}")

        with open(schema_path, 'r') as file:
            schema = json.load(file)
            self.logger.info(f"Loaded schema for {template_type}")
            return schema

    def validate_parameters(self, parameters: Dict[str, Any], schema: Dict[str, Any]):
        """Validates parameters against the provided schema."""
        missing_keys = [key for key in schema if key not in parameters]
        if missing_keys:
            self.logger.error(f"Missing required parameters: {missing_keys}")
            raise ValueError(f"Missing required parameters: {missing_keys}")

    def fetch_schema_from_remote(self, url: str) -> Dict[str, Any]:
        """Fetches a JSON schema from a remote API endpoint."""
        import requests
        try:
            response = requests.get(url)
            response.raise_for_status()
            schema = response.json()
            self.logger.info(f"Fetched schema from {url}")
            return schema
        except Exception as e:
            self.logger.error(f"Error fetching schema from {url}: {e}")
            raise

# Example usage:
# manager = SchemaManager()
# schema = manager.load_schema('hvac_template')
# manager.validate_parameters(parameters_dict, schema)