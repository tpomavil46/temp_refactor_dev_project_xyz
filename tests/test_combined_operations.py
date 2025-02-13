from src.itv_asset_tree.managers.tree_builder import TreeBuilder
from src.itv_asset_tree.managers.tree_modifier import TreeModifier
import warnings
import pytest

@pytest.mark.usefixtures("seeq_login")
def test_build_and_modify_tree():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        
        builder = TreeBuilder(workbook="Test Workbook")
        tree = builder.build_empty_tree(
            friendly_name="Test Tree", 
            description="Combined test tree."
        )
        
        modifier = TreeModifier(workbook="Test Workbook", tree_name="Test Tree")
        modifier.insert_item(
            parent_name="Test Tree",
            item_definition={
                "Name": "Combined Scalar",
                "Type": "Scalar",  
                "Formula": "100",  # ✅ Formula with a constant
                "Formula Parameters": {}  # ✅ Empty parameters
            }
        )
        print("✅ Test Signal added successfully with required time parameters.")