# src/itv_asset_tree/managers/push_manager.py

from seeq.spy.assets import Tree

class PushManager:
    """
    Handles pushing an asset tree to Seeq.
    """

    def __init__(self, tree):
        """
        Initializes the PushManager with a given asset tree.

        Args:
            tree (Tree): The asset tree to be pushed.

        Raises:
            ValueError: If no tree is provided.
        """
        if not isinstance(tree, Tree):
            raise ValueError("❌ PushManager received an invalid tree.")

        print(f"✅ [DEBUG] PushManager initialized with tree '{tree.name}'.")
        self.tree = tree
        self.push_attempted = False  # Flag to track push attempts

    def push(self, metadata_state_file=None):
        """
        Pushes the asset tree to Seeq.

        Raises:
            Exception: If the push operation fails.
        """
        if self.push_attempted:
            print("🚨 [ERROR] Recursive push detected! Push will not be attempted again.")
            return {"error": "Recursive push detected."}

        self.push_attempted = True  # Mark that push is happening

        try:
            print(f"📊 [DEBUG] Attempting to push tree '{self.tree.name}' to Seeq...")
            result = self.tree.push(metadata_state_file=metadata_state_file)  # ✅ Direct push

            print(f"✅ [DEBUG] Tree '{self.tree.name}' successfully pushed.")
            return {"message": f"Tree '{self.tree.name}' pushed successfully.", "result": result}

        except RecursionError:
            print(f"❌ [ERROR] Recursion error detected while pushing tree '{self.tree.name}'.")
            return {"error": "Recursion error while pushing."}

        except Exception as e:
            print(f"❌ Push failed: {e}")
            return {"error": str(e)}  

        finally:
            self.push_attempted = False  # Reset flag after execution

import traceback
from seeq.spy.assets import Tree  # Required for handling asset trees in Seeq

class PushManager:
    """
    Handles pushing an asset tree to Seeq.
    """

    def __init__(self, tree):
        """
        Initializes the PushManager with a given asset tree.

        Args:
            tree (Tree): The asset tree to be pushed.

        Raises:
            ValueError: If no tree is provided.
        """
        if not isinstance(tree, Tree):
            raise ValueError("❌ PushManager received an invalid tree.")

        print(f"✅ [DEBUG] PushManager initialized with tree '{tree.name}'.")
        self.tree = tree
        self.push_attempted = False  # Flag to track push attempts

    def push(self, metadata_state_file=None):
        """
        Pushes the asset tree to Seeq.

        Raises:
            Exception: If the push operation fails.
        """
        if self.push_attempted:
            print("🚨 [ERROR] Recursive push detected! Push will not be attempted again.")
            return {"error": "Recursive push detected."}

        self.push_attempted = True  # Mark that push is happening

        try:
            print(f"📊 [DEBUG] Attempting to push tree '{self.tree.name}' to Seeq...")

            # Ensure tree object is valid
            assert isinstance(self.tree, Tree), f"❌ [ERROR] Expected 'Tree', got {type(self.tree)}"

            # Debugging: Check if push() exists
            print(f"🔍 [DEBUG] Checking if push() exists on tree: {hasattr(self.tree, 'push')}")
            print(f"📌 [DEBUG] Checking push method reference: {self.tree.push.__qualname__}")

            # Push the tree
            print(f"📊 [DEBUG] Calling `self.tree.push()`...")
            result = self.tree.push(metadata_state_file=metadata_state_file)
            print(f"✅ [DEBUG] Tree '{self.tree.name}' successfully pushed.")
            
            return {"message": f"Tree '{self.tree.name}' pushed successfully.", "result": result}

        except RecursionError:
            print(f"❌ [ERROR] Recursion error detected while pushing tree '{self.tree.name}'.")
            return {"error": "Recursion error while pushing."}

        except Exception as e:
            print(f"❌ Push failed: {e}")
            return {"error": str(e)}

        finally:
            self.push_attempted = False  # Reset flag after execution

import traceback

class PushManager:
    """
    Handles pushing an asset tree to Seeq.
    """

    def __init__(self, tree):
        if not tree:
            raise ValueError("❌ PushManager received an empty tree.")

        print(f"✅ [DEBUG] PushManager initialized with tree '{tree.name}'.")
        self.tree = tree

        # 🛑 **Add a safe-guard flag to prevent recursion**
        if not hasattr(self.tree, "_push_in_progress"):
            self.tree._push_in_progress = False  

    def push(self, metadata_state_file=None):
        """
        Pushes the asset tree to Seeq.

        Parameters:
        ----------
        metadata_state_file : str, optional
            Path to save the metadata state file.
        """
        if self.tree._push_in_progress:
            print("🚨 [ERROR] Recursive push detected! Preventing re-entry.")
            return {"error": "Recursive push prevented."}

        self.tree._push_in_progress = True  # ✅ Mark push in progress

        try:
            print(f"🚀 [DEBUG] About to push tree '{self.tree.name}' to Seeq...")

            # 🔍 **Check Call Stack**
            print("🔍 [DEBUG] Call stack before push:")
            print("".join(traceback.format_stack(limit=10)))  

            # ✅ **Check tree push method reference**
            print(f"📌 [DEBUG] self.tree.push reference: {self.tree.push}")

            # 🚨 **Ensure push() is callable**
            if not hasattr(self.tree, "push") or not callable(self.tree.push):
                print("❌ [ERROR] self.tree.push() is not callable! Something is wrong!")
                return {"error": "Tree push method is not callable!"}

            print(f"📊 [DEBUG] Calling `self.tree.push()` now...")

            # ✅ **Actually push the tree**
            result = self.tree.push(metadata_state_file=metadata_state_file)

            print(f"✅ [DEBUG] Tree '{self.tree.name}' successfully pushed.")
            return {"message": f"Tree '{self.tree.name}' pushed successfully.", "result": result}

        except RecursionError:
            print(f"❌ [ERROR] Recursion error detected while pushing tree '{self.tree.name}'!")
            return {"error": "Recursion error detected!"}

        except Exception as e:
            print(f"❌ [ERROR] Push failed: {e}")
            return {"error": str(e)}

        finally:
            self.tree._push_in_progress = False  # ✅ Reset flag after execution