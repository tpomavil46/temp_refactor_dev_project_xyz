class TreeDeleter:
    """Handles deleting items from the asset tree."""

    def __init__(self, tree):
        self.tree = tree

    def delete_item(self, item_path: str):
        """Removes an item from the tree."""
        self.tree.remove(item_path)
        print(f"🗑️ Removed item at '{item_path}'.")