def visualize_tree(tree, subtree=None):
    """Prints an ASCII visualization of the asset tree.

    Args:
        tree (Tree): The asset tree to visualize.
        subtree (str, optional): Specific subtree to visualize. Defaults to None.
    """
    try:
        tree.visualize(subtree=subtree)
    except Exception as e:
        print(f"Failed to visualize tree: {e}")