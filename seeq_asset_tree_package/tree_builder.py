from spy.assets import Tree

def build_asset_tree(df):
    """Builds an asset tree using the provided DataFrame.

    Args:
        df (pd.DataFrame): DataFrame containing the tree structure and formulas.

    Returns:
        Tree: Constructed Seeq asset tree.
    """
    tree = Tree()
    for index, row in df.iterrows():
        try:
            tree.insert(
                name=row['Node Name'],
                formula=row.get('Formula', None),  # Use formula if specified
                formula_parameters=row.get('Formula Parameters', None)  # Pass formula parameters if available
            )
        except Exception as e:
            print(f"Error inserting row {index}: {e}")
    return tree

def push_tree(tree):
    """Pushes the asset tree to Seeq.

    Args:
        tree (Tree): The asset tree to push.

    Returns:
        pd.DataFrame: Push results including any errors or statistics.
    """
    try:
        return tree.push()
    except Exception as e:
        raise RuntimeError(f"Failed to push tree: {e}")
    
def check_dependencies(df, dependency_column, base_column):
    """Validates that dependencies in the DataFrame are satisfied."""
    for index, row in df.iterrows():
        dependencies = row.get(dependency_column, "").split(",")
        for dep in dependencies:
            if dep and dep not in df[base_column].values:
                raise ValueError(f"Dependency {dep} for row {index} is not satisfied.")
    return True