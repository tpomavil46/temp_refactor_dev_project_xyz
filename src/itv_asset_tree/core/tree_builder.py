# # src/managers/tree_builder.py

# import pandas as pd
# import io
# import contextlib
# from seeq.spy.assets import Tree
# from .push_manager import PushManager
# from typing import Optional

# class TreeBuilder:
#     """
#     Class to manage tree creation.
#     """

#     def __init__(self, workbook: str, csv_file: Optional[str] = None):
#         self.workbook = workbook
#         self.csv_file = csv_file
#         self.metadata = None
#         self.tree = None

#     def parse_csv(self):
#         """Parse the CSV file and load metadata."""
#         if not self.csv_file:
#             raise ValueError("CSV file not provided.")
#         self.metadata = pd.read_csv(self.csv_file)
#         print(f"✅ CSV parsed successfully: {self.csv_file}")

#     def build_empty_tree(self, friendly_name: str, description: str):
#         """
#         Build an empty tree with only the root node.
#         """
#         root_data = pd.DataFrame([{
#             'Path': '',
#             'Name': friendly_name,
#             'Type': 'Asset',
#             'Formula': None,
#             'Formula Parameters': None,
#             'Datasource ID': None,
#             'Datasource Class': None,
#             'Description': description,
#         }])
#         self.tree = Tree(
#             data=root_data,
#             workbook=self.workbook,
#             friendly_name=friendly_name,
#             description=description,
#         )
#         print(f"🌳 Empty tree '{friendly_name}' created successfully.")
#         return self.tree
    
#     def build_tree_from_csv(self, friendly_name: str, description: str):
#         """
#         Build and push a tree using the CSV file.

#         Parameters:
#         ----------
#         friendly_name : str
#             The friendly name of the tree.
#         description : str
#             A description for the tree.
#         """
#         if not self.csv_file:
#             raise ValueError("CSV file not provided.")

#         try:
#             # Build the tree using Seeq API
#             self.tree = Tree(
#                 data=self.csv_file,  # Pass the CSV file path directly
#                 workbook=self.workbook,
#                 friendly_name=friendly_name,
#                 description=description,
#             )
#             print(f"🌳 Tree '{friendly_name}' created successfully.")
#         except Exception as e:
#             raise RuntimeError(f"Error creating tree: {e}")

#     def visualize_tree(self):
#         """
#         Visualize the tree structure in a comprehensible format.
#         """
#         if not self.tree:
#             raise ValueError("Tree not built yet.")
        
#         try:
#             # Attempt to summarize the tree
#             structure = self.tree.summarize()
#             if not structure:
#                 raise ValueError("𐂷 Tree.summarize() returned an empty structure.")
#             return structure
#         except Exception as e:
#             print(f"❌ Tree.summarize() failed: {e}")
#             # Use the fallback method
#             return self._convert_tree_to_json()

#     def _convert_tree_to_json(self):
#         """
#         Convert the tree into a JSON-like nested dictionary for visualization.
#         """
#         if not self.tree:
#             return {"error": "Tree not built yet."}

#         from io import StringIO
#         import contextlib

#         # Capture the tree visualization as a string
#         with StringIO() as buf, contextlib.redirect_stdout(buf):
#             self.tree.visualize()
#             visualization = buf.getvalue()

#         # Build a JSON-like structure from the tree visualization
#         tree_json = {}
#         current_level = [tree_json]
#         lines = visualization.splitlines()
#         for line in lines:
#             # Calculate the indentation level
#             level = (len(line) - len(line.lstrip("| "))) // 2
#             node_name = line.strip("| ").strip()

#             # Navigate to the correct level
#             while len(current_level) > level + 1:
#                 current_level.pop()

#             # Add node to the current level
#             current_node = current_level[-1]
#             if node_name:
#                 current_node[node_name] = {}
#                 current_level.append(current_node[node_name])

#         return tree_json

#     def get_push_manager(self):
#         """Get a PushManager for the current tree."""
#         if not self.tree:
#             raise ValueError("❌ Tree is not built. Call 'build_empty_tree()' first.")
#         return PushManager(self.tree)

from itv_asset_tree.core.csv_parser import CSVParser

class TreeBuilder:
    """Handles the creation of asset trees in Seeq."""

    def __init__(self, workbook: str, csv_file: str):
        self.workbook = workbook
        self.csv_file = csv_file
        self.metadata = None

    def load_csv(self):
        """Loads the CSV file using CSVParser."""
        self.metadata = CSVParser.parse_csv(self.csv_file)

    def build_tree(self):
        """Builds the tree structure using loaded metadata."""
        if self.metadata is None:
            raise RuntimeError("CSV file must be loaded before building the tree.")

        # Tree-building logic goes here...
        print(f"🌳 Building tree for workbook: {self.workbook}")