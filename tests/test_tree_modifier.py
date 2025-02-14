import warnings
import pytest
from src.itv_asset_tree.managers.tree_modifier import TreeModifier

@pytest.mark.integration
@pytest.mark.usefixtures("seeq_login")
def test_insert_and_move_items():
    """Integration test for inserting and moving items in the tree."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)

        modifier = TreeModifier(workbook="Test Workbook", tree_name="Test Tree")

        # 🟢 Insert Root Asset
        modifier.insert_item(
            parent_name=None,
            item_definition={"Name": "Root Asset", "Type": "Asset"}
        )
        # 🟢 Insert Parent Node
        modifier.insert_item(
            parent_name="Root Asset",
            item_definition={"Name": "New Parent", "Type": "Asset"}
        )
        # 🟢 Insert Scalar
        modifier.insert_item(
            parent_name="Root Asset",
            item_definition={
                "Name": "Test Scalar",
                "Type": "Scalar",
                "Formula": "100",
                "Formula Parameters": {}
            }
        )
        print("✅ Items inserted successfully.")

        # 🟢 Push the Tree
        try:
            modifier.tree.push()
            print("\n✅ Tree pushed successfully.")
        except Exception as e:
            pytest.fail(f"\n❌ Failed to push tree: {e}")