from seeq import spy
import pandas as pd
import logging
from itv_asset_tree.utilities.template_loader import TemplateLoader

logging.basicConfig(level=logging.INFO)

class AcceleratorTemplateManager:
    def __init__(self, template_type, parameters, schema_manager):
        self.template_type = template_type
        self.parameters = parameters
        self.schema_manager = schema_manager

    def load_template(self):
        logging.info(f"Loading template for {self.template_type}...")
        template_data = TemplateLoader("src/itv_asset_tree/templates").load_template(self.template_type)
        return template_data

    def configure_template(self, template_data):
        logging.info("Configuring template with provided parameters...")
        template_data.update(self.parameters)
        logging.info(f"Configured Template: {template_data}")
        return template_data

    def apply_template(self, configured_template, asset_tree_name: str):
        try:
            logging.info(f"Applying template to asset tree '{asset_tree_name}'...")

            template_df = pd.DataFrame.from_dict(configured_template, orient='index').reset_index()
            template_df.rename(columns={'index': 'items'}, inplace=True)
            template_df.set_index(pd.date_range(start=pd.Timestamp.now(), periods=template_df.shape[0], freq='s'), inplace=True)

            logging.info(f"Data being pushed:\n{template_df}")
            print(template_df.info())
            print(template_df.head())

            spy.push(data=template_df, workbook=asset_tree_name)
            logging.info("Template applied successfully.")
        except Exception as e:
            logging.error(f"Failed to apply template: {str(e)}")
            raise
