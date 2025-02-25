class TreeInserter:
    """Handles inserting items into the asset tree."""

    def __init__(self, tree):
        self.tree = tree

    def insert_item(self, parent_path: str, item_definition: dict):
        """Inserts an item under a given parent path."""
        self.tree.insert(children=[item_definition], parent=parent_path)
        print(f"✅ Inserted item '{item_definition['Name']}' under '{parent_path}'.")