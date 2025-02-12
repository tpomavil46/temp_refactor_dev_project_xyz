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
        """Force a full reload of the tree from Seeq to ensure changes are reflected."""
        try:
            print(f"🔄 Reloading tree '{self.tree_name}' from workbook '{self.workbook}'...")

            # ⚠️ Create a NEW Tree object to force a fresh load
            self.tree = None  # Drop the old reference first
            self.tree = Tree(self.tree_name, workbook=self.workbook)  # Reload from Seeq

            # ✅ Confirm tree loaded successfully
            print(f"🌳 Tree '{self.tree_name}' reloaded successfully!")

        except Exception as e:
            raise ValueError(f"❌ Error loading tree '{self.tree_name}': {e}")

    def insert_item(self, parent_name: str, item_definition: dict):
        """Insert an item under a specified parent in the asset tree."""
        
        print(f"📌 [DEBUG] insert_item() called with parent='{parent_name}', item_definition={item_definition}")
        
        # Ensure item definition is properly structured
        if not isinstance(item_definition, dict) or 'Name' not in item_definition or 'Type' not in item_definition:
            raise ValueError("⚠️ item_definition must be a dictionary containing at least 'Name' and 'Type'.")

        try:
            # Strip any unnecessary hierarchy from the Name
            item_definition['Name'] = item_definition['Name'].split('.')[-1]  

            # Insert into the tree
            self.tree.insert(children=[item_definition], parent=parent_name)

            print(f"✅ Successfully inserted '{item_definition['Name']}' under '{parent_name}'.")

            # ✅ Push the tree update to Seeq
            self.tree.push()

        except Exception as e:
            print(f"❌ [ERROR] Failed to insert item: {e}")
            raise ValueError(f"Error inserting item: {e}")

    def move_item(self, source: str, destination: str):
        """Move an item to a new parent in the tree."""
        try:
            print(f"📌 [DEBUG] move_item() called: source='{source}', destination='{destination}'")

            # Ensure the tree is reloaded
            del self.tree
            self.load_tree()

            # Verify that `self.tree` is not None
            if not self.tree:
                raise ValueError("❌ Tree object is None. Reload failed.")

            # Perform move operation
            self.tree.move(source=source, destination=destination)
            print(f"✅ Successfully moved '{source}' to '{destination}'.")

            # Explicitly push the tree to commit changes
            self.tree.push(metadata_state_file="Output/asset_tree_metadata_state_file.pickle.zip")
            print(f"✅ Tree update pushed successfully.")

        except Exception as e:
            print(f"❌ [ERROR] move_item failed: {e}")
            raise ValueError(f"Error moving item: {e}")

    def remove_item(self, item_path: str):
        """Remove an item from the tree by its full path."""
        try:
            print(f"🗑️ Removing item at path: {item_path}")

            if not self.tree:
                raise ValueError("❌ Tree object is None. Cannot remove item.")

            self.tree.remove(item_path)
            print(f"✅ Successfully removed '{item_path}'.")

            # Ensure the tree is pushed after modification
            self.tree.push()
            print("✅ Tree updated and pushed successfully.")

        except Exception as e:
            print(f"❌ [ERROR] remove_item failed: {e}")
            raise ValueError(f"Error removing item: {e}")
        
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