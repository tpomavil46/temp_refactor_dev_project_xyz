from src.itv_asset_tree.managers.tree_modifier import TreeModifier
import warnings
import pytest

@pytest.mark.usefixtures("seeq_login")
def test_insert_and_move_items():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)

        modifier = TreeModifier(workbook="Test Workbook", tree_name="Test Tree")

        # 1️⃣ Insert Root Asset
        modifier.insert_item(
            parent_name=None,
            item_definition={"Name": "Root Asset", "Type": "Asset"}
        )

        # 2️⃣ Insert Parent Node
        modifier.insert_item(
            parent_name="Root Asset",
            item_definition={"Name": "New Parent", "Type": "Asset"}
        )

        # 3️⃣ Insert Test Signal with Required Parameters
        modifier.insert_item(
            parent_name="Root Asset",
            item_definition={
                "Name": "Test Scalar",
                "Type": "Scalar",
                "Formula": "100",  # Formula with a constant
                "Formula Parameters": {}  # Empty parameters
            }
        )

        # 4️⃣ Push the Tree to Seeq
        try:
            modifier.tree.push()
            print("\n✅ Tree pushed successfully.")
        except Exception as e:
            print(f"\n❌ Error pushing tree: {e}")