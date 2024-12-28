# tests/test_combined_operations.py

from src.managers.tree_builder import TreeBuilder
from src.managers.tree_modifier import TreeModifier

def test_build_and_modify_tree():
    builder = TreeBuilder(workbook="Test Workbook")
    tree = builder.build_empty_tree(friendly_name="Test Tree", description="Combined test tree.")

    modifier = TreeModifier(workbook="Test Workbook", tree_name="Test Tree")
    modifier.add_item(
        parent_name="Test Tree",
        item_definition={"Name": "Combined Signal", "Type": "Signal"}
    )
    modifier.move_item(
        source="Test Tree >> Combined Signal",
        destination="Another Parent"
    )
    print("Combined build and modify test passed.")