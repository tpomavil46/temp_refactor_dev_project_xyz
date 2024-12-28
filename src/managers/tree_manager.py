# src/managers/tree_manager.py

from abc import ABC, abstractmethod

class TreeManager(ABC):
    """
    Abstract base class for managing asset trees in Seeq.
    """

    def __init__(self, workbook: str):
        self.workbook = workbook
        self.tree = None

    @abstractmethod
    def build_tree(self, friendly_name: str, description: str):
        """Construct a tree with the given name and description."""
        pass

    @abstractmethod
    def clear_existing_tree(self, tree_name: str):
        """Clear an existing tree by name."""
        pass