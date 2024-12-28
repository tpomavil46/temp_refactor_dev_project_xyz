# src/managers/tree_builder.py

import pandas as pd
from seeq.spy.assets import Tree
from .push_manager import PushManager

class TreeBuilder:
    """
    Class to manage tree creation.
    """

    def __init__(self, workbook: str, csv_file: Optional[str] = None):
        self.workbook = workbook
        self.csv_file = csv_file
        self.metadata = None
        self.tree = None

        if self.csv_file:
            self.parse_csv()

    def parse_csv(self):
        """Parse the CSV file and load metadata."""
        if not self.csv_file:
            raise ValueError("CSV file not provided.")
        self.metadata = pd.read_csv(self.csv_file)
        print(f"CSV parsed successfully: {self.csv_file}")

    def build_empty_tree(self, friendly_name: str, description: str):
        """
        Build an empty tree with only the root node.
        """
        root_data = pd.DataFrame([{
            'Path': '',
            'Name': friendly_name,
            'Type': 'Asset',
            'Formula': None,
            'Formula Parameters': None,
            'Datasource ID': None,
            'Datasource Class': None,
            'Description': description,
        }])
        self.tree = Tree(
            data=root_data,
            workbook=self.workbook,
            friendly_name=friendly_name,
            description=description,
        )
        print(f"Empty tree '{friendly_name}' created successfully.")
        return self.tree

    def get_push_manager(self):
        """Get a PushManager for the current tree."""
        if not self.tree:
            raise ValueError("Tree is not built. Call 'build_empty_tree()' first.")
        return PushManager(self.tree)