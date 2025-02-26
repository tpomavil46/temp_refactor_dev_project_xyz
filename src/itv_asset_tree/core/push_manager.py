# src/itv_asset_tree/core/push_manager.py

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