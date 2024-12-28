# tests/test_tree_builder.py

from src.managers.tree_builder import TreeBuilder

def test_build_empty_tree():
    builder = TreeBuilder(workbook="Test Workbook")
    tree = builder.build_empty_tree(friendly_name="Test Tree", description="This is a test tree.")
    assert tree is not None
    print("Tree built successfully.")