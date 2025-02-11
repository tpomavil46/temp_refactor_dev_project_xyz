# tests/test_combined_operations.py

from itv_asset_tree.managers.tree_builder import TreeBuilder
from itv_asset_tree.managers.tree_modifier import TreeModifier
import warnings

def test_build_and_modify_tree():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        builder = TreeBuilder(workbook="Test Workbook")
        tree = builder.build_empty_tree(friendly_name="Test Tree", description="Combined test tree.")

        modifier = TreeModifier(workbook="Test Workbook", tree_name="Test Tree")
        modifier.add_item(
            parent_name="Test Tree",
            item_definition={"Name": "Combined Signal", "Type": "Signal", "Formula": "sin($time)"}
        )
        print("Test Signal added successfully. Warnings are internal to the Seeq SDK, and future updates to urllib3 could break the SDK if it doesn’t update its code.")