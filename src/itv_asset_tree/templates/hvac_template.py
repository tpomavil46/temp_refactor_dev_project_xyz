# src/itv_asset_tree/templates/hvac_template.py

from seeq.spy.assets import Asset, ItemGroup

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
        
class Refrigerator(Asset):
    @Asset.Attribute()
    def Temperature(self, metadata):
        # This signal attribute is assigned to the Refrigerator asset
        return metadata[metadata['Name'].str.endswith('Temperature')]

    # Note the use of Asset.Component here, which allows us to return a list of definitions
    # instead of just a single definition.
    @Asset.Component()
    def Compressors(self, metadata):
        # Using the Compressor template class, we build all of the compressor definitions
        # associated with a particular Refrigerator. The column_name supplied tells the
        # build_components function which metadata column to use for the Compressor names.
        return self.build_components(template=Compressor, metadata=metadata, column_name='Compressor')
    
    @Asset.Attribute()
    def Compressor_Power_Max(self, metadata):
        # We can refer to the Compressors and "pick" attributes for which to perform a
        # roll up. In this example, we're picking the 'Power' signals that are on each
        # compressor and creating a new signal representing the maximum power across
        # all the compressors.
        return self.Compressors().pick({
            'Name': 'Power'
        }).roll_up('maximum')
    
    @Asset.Attribute()
    def Compressor_High_Power(self, metadata):
        # Similar to Compressor_Power_Max, we are rolling up a compressor calculation but
        # this time it's a condition. 'High Power' at the Refrigerator level will have
        # capsules if either compressor's 'High Power' condition is present.
        #
        # This time we'll use a different method of picking the child items than we used
        # in Compressor_Power_Max() above. In this case, we're going to select the set of
        # compressors that are owned by this asset from the entire set of assets, and use
        # Python conditional logic to find the "High Power" conditions. What you see
        # below is called a Python "list comprehension" that combines iteration over all
        # assets (the "for/in" construct) with filtering (the "if" statement).
        #
        # Helpful functions:
        #  asset.is_child_of(self)      - Is the asset one of my direct children?
        #  asset.is_parent_of(self)     - Is the asset my direct parent?
        #  asset.is_descendant_of(self) - Is the asset below me in the tree?
        #  asset.is_ancestor_of(self)   - Is the asset above me? (i.e. parent/grandparent/great-grandparent/etc)
        #
        return ItemGroup([
            asset.High_Power() for asset in self.all_assets()
            if asset.is_child_of(self)
        ]).roll_up('union')
    
class Compressor(Asset):
    @Asset.Attribute()
    def Power(self, metadata):
        # Each compressor has just a single attribute, Power
        return metadata[metadata['Name'].str.endswith('Power')]
    
    @Asset.Attribute()
    def High_Power(self, metadata):
        return {
            'Type': 'Condition',
            'Formula': '$a.valueSearch(isGreaterThan(20kW))',
            'Formula Parameters': {
                '$a': self.Power()
            }
        }
    
    @Asset.Attribute()
    def Other_Compressors_Are_High_Power(self, metadata):
        # This is a more complex example of using self.all_assets() where we want to
        # look at sibling assets as opposed to parents/children, and do a roll up.
        # Here the "if" statement selects Compressor assets where our parent and
        # their parent are the same but we exclude ourselves.
        return ItemGroup([
            asset.High_Power() for asset in self.all_assets()
            if isinstance(asset, Compressor) and self.parent == asset.parent and self != asset
        ]).roll_up('union')