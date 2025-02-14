import warnings
import pytest
from src.itv_asset_tree.managers.tree_builder import TreeBuilder

@pytest.mark.unit
def test_build_empty_tree():
    """Unit test for building an empty tree (mocked login)."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)

        builder = TreeBuilder(workbook="Test Workbook")
        tree = builder.build_empty_tree(
            friendly_name="Test Tree", 
            description="This is a test tree."
        )
        assert tree is not None, "Tree building failed."
        print("✅ Tree built successfully.")