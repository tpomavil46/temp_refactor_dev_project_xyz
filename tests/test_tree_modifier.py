# tests/test_tree_modifier.py

from src.managers.tree_modifier import TreeModifier

def test_move_item():
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
    modifier = TreeModifier(workbook="Test Workbook", tree_name="Test Tree")
    modifier.add_item(
        parent_name="Root Asset",
        item_definition={"Name": "Test Signal", "Type": "Signal"}
    )
    modifier.move_item(
        source="Root Asset >> Test Signal",
        destination="New Parent"
    )
    print("Item added and moved successfully.")