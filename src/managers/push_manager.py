# src/managers/push_manager.py

class PushManager:
    """
    A simple push manager for pushing trees to Seeq.
    """

    def __init__(self, tree):
        self.tree = tree

    def push(self, metadata_state_file=None):
        """
        Push the tree to Seeq.

        Parameters:
        ----------
        metadata_state_file : str, optional
            Path to save the metadata state file.
        """
        try:
            return self.tree.push(metadata_state_file=metadata_state_file)
        except Exception as e:
            print(f"Error while pushing the tree: {e}")
            raise