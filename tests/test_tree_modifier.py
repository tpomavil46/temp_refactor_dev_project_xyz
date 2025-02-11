# tests/test_tree_modifier.py

from itv_asset_tree.managers.tree_modifier import TreeModifier
import warnings

def test_move_item():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        modifier = TreeModifier(workbook="Add-on Exploration >> Data Lab Analysis", tree_name="Detroit")
        try:
            modifier.move_item(
                source="Detroit >> Reactor 1 >> Downtime",
                destination="Detroit >> Reactor 3"
            )
            print("Item moved successfully.")
        except ValueError as e:
            print(f"Error moving item: {e}")

        try:
            modifier.push()
            print("Tree pushed successfully.")
        except Exception as e:
            print(f"Error pushing tree: {e}")
        
# tests/test_tree_modifier.py
def test_add_and_move_items():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        modifier = TreeModifier(workbook="Test Workbook", tree_name="Test Tree")
        
        # Add the root asset
        modifier.add_item(
            parent_name=None,  # Root assets don't have a parent
            item_definition={"Name": "Root Asset", "Type": "Asset"}
        )
        print("Tree after adding 'Root Asset':")
        print(modifier.tree.visualize())

        # Add a destination to the tree
        modifier.add_item(
            parent_name="Root Asset",
            item_definition={"Name": "New Parent", "Type": "Asset"}
        )
        print("Tree after adding 'New Parent':")
        print(modifier.tree.visualize())

        # Add the source item
        modifier.add_item(
            parent_name="Root Asset",
            item_definition={"Name": "Test Signal", "Type": "Signal", "Formula": "cos($time)"}
        )
        print("Tree after adding 'Test Signal':")
        print(modifier.tree.visualize())

        # Move the source item to the new destination
        modifier.move_item(
            source="Root Asset >> Test Signal",
            destination="Root Asset >> New Parent"
        )
        print("Tree after moving 'Test Signal':")
        print(modifier.tree.visualize())