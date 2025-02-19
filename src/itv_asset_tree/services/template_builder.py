# src/itv_asset_tree/services/template_builder.py
import seeq.spy as spy
import pandas as pd
from seeq.spy.assets import Asset


class TemplateBuilder:
    def __init__(self):
        pass

    def build_template(self, template_class: Asset, metadata_df: pd.DataFrame):
        """
        Builds an asset tree using spy.assets.build().

        :param template_class: The template class (subclass of Asset).
        :param metadata_df: Pandas DataFrame with metadata ingredients.
        :return: DataFrame containing the build result.
        """
        try:
            build_df = spy.assets.build(template_class, metadata_df)
            return build_df
        except Exception as e:
            return {"error": str(e)}