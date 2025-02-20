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
    
    @staticmethod
    def get_required_parameters():
        return {
            "temperature_signal": "string (Tag name of temperature signal)",
            "pressure_signal": "string (Tag name of pressure signal)",
            "flow_signal": "string (Tag name of flow signal)"
        }
        
class HVAC_With_Calcs(HVAC):
    """
    Defines an extended HVAC template with calculated signals, conditions, and scalars.
    """

    @Asset.Attribute()
    def Temperature_Rate_Of_Change(self, metadata):
        return {
            'Type': 'Signal',
            'Formula': '$temp.lowPassFilter(150min, 3min, 333).derivative() * 3600 s/h',
            'Formula Parameters': {
                '$temp': self.Temperature(),
            }
        }

    @Asset.Attribute()
    def Too_Hot(self, metadata):
        return {
            'Type': 'Condition',
            'Formula': '$temp.valueSearch(isGreaterThan($threshold))',
            'Formula Parameters': {
                '$temp': self.Temperature(),
                '$threshold': self.Hot_Threshold()
            }
        }

    @Asset.Attribute()
    def Hot_Threshold(self, metadata):
        return {
            'Type': 'Scalar',
            'Formula': '80F'
        }

    @staticmethod
    def get_required_parameters():
        return {
            "temperature_signal": "string (Tag name of temperature signal)",
            "pressure_signal": "string (Tag name of pressure signal)",
            "flow_signal": "string (Tag name of flow signal)"
        }