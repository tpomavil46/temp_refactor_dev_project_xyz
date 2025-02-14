import warnings
import pytest
from src.itv_asset_tree.managers.tree_builder import TreeBuilder
from src.itv_asset_tree.managers.tree_modifier import TreeModifier

@pytest.mark.integration
@pytest.mark.usefixtures("seeq_login")
def test_build_and_modify_tree():
    """Integration test to build and modify the tree."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)

        builder = TreeBuilder(workbook="Test Workbook")
        tree = builder.build_empty_tree(
            friendly_name="Test Tree", 
            description="Combined test tree."
        )
        assert tree is not None, "Failed to build the tree."

        modifier = TreeModifier(workbook="Test Workbook", tree_name="Test Tree")
        modifier.insert_item(
            parent_name="Test Tree",
            item_definition={
                "Name": "Combined Scalar",
                "Type": "Scalar",  
                "Formula": "100",
                "Formula Parameters": {}
            }
        )
        print("✅ Combined Scalar added successfully.")

        # 🟢 Attempt to push changes
        try:
            modifier.tree.push()
            print("\n✅ Tree pushed successfully.")
        except Exception as e:
            print(f"\n❌ Error pushing tree: {e}")
            pytest.fail(f"Push failed: {e}")