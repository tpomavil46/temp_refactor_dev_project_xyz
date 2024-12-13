from my_seeq_package.csv_handler import read_csv, validate_csv
from my_seeq_package.tree_builder import build_asset_tree, push_tree, check_dependencies

def process_and_push_tree(file_path):
    """End-to-end process to build and push a tree from a CSV file."""
    required_columns = ['Node Name', 'Formula', 'Base Signal']
    try:
        # Step 1: Read and validate CSV
        df = read_csv(file_path)
        validate_csv(df, required_columns)

        # Step 2: Validate dependencies
        check_dependencies(df, dependency_column='Dependencies', base_column='Base Signal')

        # Step 3: Build the asset tree
        tree = build_asset_tree(df)

        # Step 4: Push the tree to Seeq
        result = push_tree(tree)
        print("Tree successfully pushed!")
        return result
    except Exception as e:
        print(f"Process failed: {e}")
        return None