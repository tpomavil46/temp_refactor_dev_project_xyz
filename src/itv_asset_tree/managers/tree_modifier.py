# src/managers/tree_modifier.py

import csv
import json
from seeq.spy.assets import Tree
from .push_manager import PushManager

class TreeModifier(PushManager):
    """
    A class for modifying asset trees in Seeq.
    """

    def __init__(self, workbook: str, tree_name: str):
        self.workbook = workbook
        self.tree_name = tree_name
        self.tree = None
        self.load_tree()
        super().__init__(tree=self.tree)

    def load_tree(self):
        """Load an existing tree."""
        try:
            self.tree = Tree(self.tree_name, workbook=self.workbook)
            print(f"🌳 Tree '{self.tree_name}' loaded successfully.")
        except Exception as e:
            raise ValueError(f"❌ Error loading tree '{self.tree_name}': {e}")

    def add_item(self, parent_name: str, item_definition: dict):
        """Add an item under a specified parent."""
        if 'Name' not in item_definition or 'Type' not in item_definition:
            raise ValueError("⚠️ The item_definition must contain at least 'Name' and 'Type'.")
        item_definition['Name'] = item_definition['Name'].split('.')[-1]  # Strip hierarchical paths
        try:
            self.tree.insert(children=[item_definition], parent=parent_name)
            print(f"✅ Added item '{item_definition['Name']}' under parent '{parent_name}'.")
        except Exception as e:
            print(f"❌ Error inserting item: {e}")
            raise

    def move_item(self, source: str, destination: str):
        """Move an item to a new parent in the tree."""
        try:
            self.tree.move(source=source, destination=destination)
            print(f"🚚 Moved item from '{source}' to '{destination}'.")
        except Exception as e:
            raise ValueError(f"Error moving item: {e}")

    def remove_item(self, item_name: str):
        """Remove an item from the tree."""
        try:
            self.tree.remove(item_name)
            print(f"✅ Removed item '{item_name}'.")
        except Exception as e:
            print(f"✅ Error removing item: {e}")
            raise
        
    def visualize_tree(self):
        """Visualize the tree structure."""
        if not self.tree:
            raise ValueError("Tree is not loaded. Call 'load_tree()' first.")
        try:
            visualization = self.tree.visualize()
            print("🌳 Tree visualization generated successfully.")
            return visualization
        except Exception as e:
            raise RuntimeError(f"❌ Error visualizing tree: {e}")