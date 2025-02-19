# src/itv_asset_tree/templates/hvac_template.py
from seeq.spy.assets import Asset

class HVAC(Asset):
    """
    Defines the HVAC template for Seeq Asset Trees.
    - Areas are treated as high-level assets.
    - Equipment (Temperature, Humidity) is nested under each area.
    """

    @Asset.Attribute()
    def Temperature(self, metadata):
        return metadata[metadata['Name'].str.endswith('Temperature')]

    @Asset.Attribute()
    def Relative_Humidity(self, metadata):
        return metadata[metadata['Name'].str.contains('Humidity')]