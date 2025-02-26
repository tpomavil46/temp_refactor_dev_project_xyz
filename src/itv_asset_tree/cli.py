# import sys
# import os
# from dotenv import load_dotenv
# from seeq import spy

# # Add the project root to the Python path
# current_dir = os.path.dirname(os.path.abspath(__file__))
# project_root = os.path.abspath(os.path.join(current_dir, ".."))
# if project_root not in sys.path:
#     sys.path.insert(0, project_root)

# # Ensure `src` is in Python's module search path
# current_dir = os.path.dirname(os.path.abspath(__file__))
# project_root = os.path.abspath(os.path.join(current_dir, ".."))
# if project_root not in sys.path:
#     sys.path.insert(0, project_root)

# # Load environment variables from .env
# load_dotenv()

# # Get credentials from environment variables
# username = os.getenv("SERVER_USERNAME")
# password = os.getenv("SERVER_PASSWORD")
# host = os.getenv("SERVER_HOST")

# # Print connection information (without sensitive data)
# print(f"🔗 Connecting to {host} as {username}...")

# # Set compatibility mode
# spy.options.compatibility = 193
# spy.options.friendly_exceptions = False

# # Perform SPy login
# try:
#     spy.login(url=host, username=username, password=password)
#     print("✅ Seeq SPy login successful!")
# except Exception as e:
#     print(f"❌ Failed to login to Seeq: {e}")
    
# # Now import your modules
# from itv_asset_tree.core.tree_builder import TreeBuilder
# from itv_asset_tree.core.push_manager import PushManager
# from itv_asset_tree.utils.csv_parser import CSVHandler
# from itv_asset_tree.utils.duplicate_resolution import (
#     DuplicateResolver, 
#     KeepFirstStrategy, 
#     KeepLastStrategy, 
#     RemoveAllStrategy, 
#     UserSpecificStrategy
# )
# from itv_asset_tree.utils.lookup_builder import LookupTableBuilder
# from seeq.spy import search
# from seeq.spy.assets import Tree
# from tkinter import Tk, filedialog
# from dotenv import load_dotenv
# import pandas as pd


# # Function to browse for a file
# def browse_file():
#     root = Tk()
#     root.withdraw()
#     file_path = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv")])
#     if not file_path:
#         raise ValueError("❌ No file selected.")
#     return file_path

# def create_lookup_tables(resolved_csv_path):
#     """
#     Create lookup tables from resolved CSV.
#     """
#     from src.utilities.csv_parser import CSVHandler
#     from src.utilities.lookup_builder import LookupTableBuilder

#     print("\nLoading resolved CSV...")
#     csv_handler = CSVHandler(resolved_csv_path)
#     data = csv_handler.load_csv()

#     group_column = input("✔️ Enter the column name for grouping (e.g., Equipment_Desc): ").strip()
#     key_column = input("✔️ Enter the column name for keys (e.g., PLC_Tag_Value): ").strip()
#     value_column = input("✔️ Enter the column name for values (e.g., Reason Desc): ").strip()

#     builder = LookupTableBuilder(group_column, key_column, value_column)
#     lookup_tables = builder.build(data)

#     print("\nSpecify Parent Path for each group:")
#     parent_paths = {}
#     for group_name in lookup_tables.keys():
#         parent_path = input(f"✔️ Enter the Parent Path for group '{group_name}': ").strip()
#         parent_paths[group_name] = parent_path

#     output_file = "lookup_strings_output.csv"
#     LookupTableBuilder.save_lookup_to_csv(lookup_tables, parent_paths, output_file)
#     print(f"✅ Lookup tables saved to '{output_file}'.")

# # Function to resolve duplicates in CSV
# def resolve_duplicates(csv_path):
#     """Resolve duplicates in the CSV file."""
#     print("Starting duplicate resolution...")
#     csv_handler = CSVHandler(csv_path)
#     data = csv_handler.load_csv()
#     print("✅ CSV loaded successfully.")

#     group_column = input("✔️ Enter the column name for grouping (e.g., Equipment_Desc): ").strip()
#     key_column = input("✔️ Enter the column name for identifying duplicates (e.g., PLC_Tag_Value): ").strip()

#     print("Choose a strategy for resolving duplicates:")
#     print("1. Keep first occurrence")
#     print("2. Keep last occurrence")
#     print("3. Remove all duplicates")
#     print("4. Specify rows to keep")
#     choice = input("Enter your choice (1/2/3/4): ").strip()

#     if choice == "1":
#         strategy = KeepFirstStrategy()
#     elif choice == "2":
#         strategy = KeepLastStrategy()
#     elif choice == "3":
#         strategy = RemoveAllStrategy()
#     elif choice == "4":
#         rows = input("✔️ Enter row numbers to keep (comma-separated, e.g., '0,2'): ")
#         rows_to_keep = [int(i.strip()) for i in rows.split(",")]
#         strategy = UserSpecificStrategy(rows_to_keep)
#     else:
#         print("👀 Invalid choice. Defaulting to 'Keep First'.")
#         strategy = KeepFirstStrategy()

#     resolver = DuplicateResolver(strategy)

#     resolved_groups = []
#     for group_name, group in data.groupby(group_column):
#         resolved_group = resolver.resolve_group(group, group_name, key_column)
#         resolved_groups.append(resolved_group)

#     resolved_data = pd.concat(resolved_groups, ignore_index=True)
#     resolved_data.to_csv("resolved_data.csv", index=False)
#     print("✅ Resolved data saved to 'resolved_data.csv'.")
#     return resolved_data

# # Function to add items to the tree from lookup_strings_output.csv
# def add_items_from_lookup_csv(tree, csv_path="lookup_strings_output.csv"):
#     """
#     Add items to the tree from the lookup_strings_output.csv file.

#     Parameters:
#     ----------
#     tree : seeq.spy.assets.Tree
#         The tree object to modify.
#     csv_path : str
#         Path to the lookup_strings_output.csv file. Defaults to "lookup_strings_output.csv".
#     """
#     if not os.path.exists(csv_path):
#         print(f"→ File '{csv_path}' not found. Please create the file using option 6.")
#         return

#     data = pd.read_csv(csv_path)
#     for _, row in data.iterrows():
#         parent_path = row.get("Parent Path", "").strip()
#         name = row.get("Name", "").strip()
#         formula = row.get("Formula", "").strip()
#         formula_parameters = row.get("Formula Parameters", "{}")

#         if not parent_path or not name:
#             print(f"⏩ Skipping invalid row: {row}")
#             continue

#         item_definition = {
#             "Name": name,
#             # "Type": "Signal",  # Assuming all items are signals
#             "Formula": formula,
#             "Formula Parameters": eval(formula_parameters),  # Convert string to dictionary
#         }

#         try:
#             tree.insert(children=[item_definition], parent=parent_path)
#             print(f"Added item '{name}' under parent '{parent_path}'.")
#         except Exception as e:
#             print(f"❌ Error adding item '{name}': {e}")

# # Function to create lookup tables from a CSV
# def create_lookup_tables(resolved_csv_path):
#     """
#     Create lookup tables from resolved CSV.
#     """
#     print("\nLoading resolved CSV...")
#     csv_handler = CSVHandler(resolved_csv_path)
#     data = csv_handler.load_csv()

#     group_column = input("✔️ Enter the column name for grouping (e.g., Equipment_Desc): ").strip()
#     key_column = input("✔️ Enter the column name for keys (e.g., PLC_Tag_Value): ").strip()
#     value_column = input("✔️ Enter the column name for values (e.g., Reason Desc): ").strip()

#     builder = LookupTableBuilder(group_column, key_column, value_column)
#     lookup_tables = builder.build(data)

#     print("\nSpecify Parent Path for each group:")
#     parent_paths = {}
#     for group_name in lookup_tables.keys():
#         parent_path = input(f"✔️ Enter the Parent Path for group '{group_name}': ").strip()
#         parent_paths[group_name] = parent_path

#     output_file = "lookup_strings_output.csv"
#     builder.save_lookup_to_csv(lookup_tables, parent_paths, output_file)
#     print(f"✅ Lookup tables saved to '{output_file}'.")

# # Function to add an existing item to the tree
# def add_existing_item_to_tree(tree):
#     item_name = input("✔️ Enter the name of the existing item to search for: ").strip()
#     search_results = search({'Name': item_name, 'Type': 'Signal'})
#     if search_results.empty:
#         print(f"❓ No matches found for '{item_name}'.")
#         return
#     item_id = search_results.iloc[0]['ID']
#     parent = input("✔️ Enter the parent name in the tree where this item should be added: ").strip()
#     item_definition = {"Name": item_name, "Type": "Signal", "ID": item_id}
#     try:
#         tree.insert(children=[item_definition], parent=parent)
#         print(f"✅ Added '{item_name}' under '{parent}'.")
#     except Exception as e:
#         print(f"❌ Error adding item: {e}")

# # Function to add a new item to the tree
# def add_new_item_to_tree(tree):
#     parent = input("✔️ Enter the parent name: ").strip()
#     name = input("✔️ Enter the name of the new element: ").strip()
#     element_type = input("✔️ Enter the type of the element (Signal/Condition/Asset): ").strip()
#     formula = input("✔️ Enter the formula (or leave blank): ").strip() or None
#     item_definition = {"Name": name, "Type": element_type, "Formula": formula}
#     try:
#         tree.insert(children=[item_definition], parent=parent)
#         print(f"✅ Added '{name}' under '{parent}'.")
#     except Exception as e:
#         print(f"❌ Error adding item: {e}")

# # Function to add items to the tree from a CSV file        
# def add_items_from_csv(tree_modifier):
#     """
#     Add items to the tree from a CSV file using TreeModifier.
#     """
#     csv_file = browse_file()  # Let the user select the CSV file
#     try:
#         tree_modifier.add_items_from_csv(csv_file)
#         print("✅ Items added successfully from CSV.")
#     except Exception as e:
#         print(f"❌ Error adding items from CSV: {e}")

# # Main interactive menu
# def show_menu():
#     print("\nWhat would you like to do next?")
#     print("1. Visualize the tree")
#     print("2. Add a new item")
#     print("3. Add an existing item (search and add)")
#     print("4. Push the tree to Seeq")
#     print("5. Resolve duplicates in a CSV")
#     print("6. Create lookup tables from a CSV")
#     print("7. Add items to the tree from lookup_strings_output.csv")
#     print("8. Exit")

# if __name__ == "__main__":
#     print("🤗 Welcome to the Seeq Asset Tree Manager!")
#     workbook = input("📓 Enter the name of the workbook to use or create: ").strip()

#     try:
#         use_existing_tree = input("❔ Do you want to modify an existing tree? (Y/N): ").strip().upper()
#         csv_file = None
#         builder = None
#         if use_existing_tree == "Y":
#             tree_name = input("✔️ Enter the name of the existing tree: ").strip()
#             modifier = TreeModifier(workbook=workbook, tree_name=tree_name)
#         else:
#             use_csv = input("❔ Would you like to provide a CSV file to build the tree? (Y/N): ").strip().upper()
#             if use_csv == "Y":
#                 csv_file = browse_file()

#             builder = TreeBuilder(workbook=workbook, csv_file=csv_file)

#             if csv_file:
#                 builder.parse_csv()
#                 friendly_name = input("→ Enter a friendly name for the tree: ").strip()
#                 description = input("→ Enter a description for the tree: ").strip()
#                 builder.build_tree_from_csv(friendly_name=friendly_name, description=description)
#             else:
#                 friendly_name = input("→ Enter a friendly name for the tree: ").strip()
#                 description = input("→ Enter a description for the tree: ").strip()
#                 builder.build_empty_tree(friendly_name=friendly_name, description=description)

#         tree_manager = modifier if use_existing_tree == "Y" else builder.get_push_manager()

#         while True:
#             show_menu()
#             choice = input("Enter your choice (1/2/3/4/5/6/7/8): ").strip()
#             if choice == "1":
#                 print("\nTree Visualization:")
#                 print(builder.tree.visualize() if builder else modifier.tree.visualize())
#             elif choice == "2":
#                 add_new_item_to_tree(builder.tree if builder else modifier.tree)
#             elif choice == "3":
#                 add_existing_item_to_tree(builder.tree if builder else modifier.tree)
#             elif choice == "4":
#                 try:
#                     tree_manager.push()
#                     print("✅ Tree pushed successfully.")
#                 except Exception as e:
#                     print(f"❌ Error pushing tree: {e}")
#             elif choice == "5":
#                 csv_file = browse_file()
#                 resolve_duplicates(csv_file)
#             elif choice == "6":
#                 resolved_csv_path = browse_file()
#                 create_lookup_tables(resolved_csv_path)
#             elif choice == "7":
#                 lookup_csv_path = "lookup_strings_output.csv"  # Default path for the lookup file
#                 add_items_from_lookup_csv(builder.tree if builder else modifier.tree, lookup_csv_path)
#             elif choice == "8":
#                 print("👋 Exiting the application.")
#                 break
#             else:
#                 print("❌ Invalid choice. Please select 1, 2, 3, 4, 5, 6, 7, or 8.")
#     except Exception as e:
#         print(f"Error: {e}")

# src/itv_asset_tree/cli.py

import sys
import os
import click
from dotenv import load_dotenv
from seeq import spy
from seeq.spy.assets import Tree
from itv_asset_tree.core.tree_builder import TreeBuilder
from itv_asset_tree.core.tree_modifier import TreeModifier
from itv_asset_tree.core.push_manager import PushManager
from itv_asset_tree.utils.logger import log_info, log_error

# ✅ Load environment variables
load_dotenv()

# ✅ Retrieve Seeq credentials
USERNAME = os.getenv("SERVER_USERNAME")
PASSWORD = os.getenv("SERVER_PASSWORD")
HOST = os.getenv("SERVER_HOST")

# ✅ Ensure login before any action
def ensure_seeq_login():
    """Logs into Seeq if not already authenticated."""
    try:
        if not spy.user:
            log_info(f"🔑 Logging into Seeq at {HOST} as {USERNAME}...")
            spy.login(url=HOST, username=USERNAME, password=PASSWORD)
            log_info("✅ Successfully logged into Seeq.")
    except Exception as e:
        log_error(f"❌ Seeq login failed: {e}")
        sys.exit(1)

@click.group()
def cli():
    """CLI tool for interacting with Seeq Asset Trees."""
    ensure_seeq_login()  # ✅ Ensures login before executing commands

@click.command()
@click.argument("workbook_name")
@click.argument("csv_file_path")
def build_tree(workbook_name, csv_file_path):
    """CLI command to build a tree from a CSV.
    
    The tree name (root node) is extracted from the CSV automatically.
    """
    ensure_seeq_login()  # ✅ Ensure we're logged into Seeq

    log_info(f"CLI: Building tree in workbook '{workbook_name}' from CSV '{csv_file_path}'")

    tree_builder = TreeBuilder(workbook_name, csv_file_path)
    tree_builder.parse_csv()

    try:
        # ✅ Extract the root node name dynamically from the CSV
        root_candidates = tree_builder.metadata["Level 1"].dropna().unique()
        if len(root_candidates) > 1:
            raise ValueError(f"❌ Multiple root nodes detected: {root_candidates}")
        elif len(root_candidates) == 0:
            raise ValueError("❌ No valid root node found in CSV.")
        
        tree_name = root_candidates[0]  # ✅ Use the first Level 1 value as the root node
        log_info(f"🌳 Using '{tree_name}' as the root node.")

        # ✅ Build the tree with the extracted root node
        tree_builder.build_tree_from_csv(friendly_name=tree_name, description="Generated from CSV")
        log_info(f"✅ Tree '{tree_name}' created successfully in workbook '{workbook_name}'.")

        # ✅ Visualize the tree structure
        log_info("📊 Tree Structure:")
        tree_builder.tree.visualize()

        # ✅ Ask before pushing
        should_push = input("🚀 Do you want to push this tree to Seeq? (y/N): ").strip().lower()
        if should_push == "y":
            tree_builder.tree.push()
            log_info(f"✅ Tree '{tree_name}' pushed successfully to workbook '{workbook_name}'.")

    except Exception as e:
        log_error(f"❌ Error building tree: {e}")
        
@click.command()
@click.argument("workbook_name")
@click.option("--csv-file", "-c", type=str, default=None, help="Optional CSV file to determine the root node name")
@click.option("--description", "-d", type=str, default="Empty asset tree.", help="Description for the tree")
def create_empty_tree(workbook_name, csv_file, description):
    """CLI command to create an empty tree.

    - If a **CSV is provided**, the root node name is extracted from **Level 1**.
    - If **no CSV is provided**, the user is prompted for a tree name.
    - After creation, the user is asked if they want to visualize & push the tree.
    """
    ensure_seeq_login()  # ✅ Ensure we're logged into Seeq

    log_info(f"CLI: Creating an empty tree in workbook '{workbook_name}'")

    # ✅ Extract the root node name dynamically from the CSV (if provided)
    if csv_file:
        log_info(f"📄 Extracting tree name from CSV: {csv_file}")
        try:
            metadata = pd.read_csv(csv_file)
            root_candidates = metadata["Level 1"].dropna().unique()
            if len(root_candidates) > 1:
                raise ValueError(f"❌ Multiple root nodes detected: {root_candidates}")
            elif len(root_candidates) == 0:
                raise ValueError("❌ No valid root node found in CSV.")
            
            tree_name = root_candidates[0]  # ✅ Use the first Level 1 value as the root node
            log_info(f"🌳 Using '{tree_name}' as the root node.")

        except Exception as e:
            log_error(f"❌ Error extracting tree name from CSV: {e}")
            return
    else:
        tree_name = input("🌳 Enter the name of the empty tree: ").strip()

    # ✅ Proceed with tree creation
    tree_builder = TreeBuilder(workbook_name)

    try:
        tree_builder.build_empty_tree(tree_name, description)  # ✅ Pass both `tree_name` and `description`
        log_info(f"✅ Empty tree '{tree_name}' created successfully in workbook '{workbook_name}'.")
        
        # ✅ Ask user if they want to visualize the tree
        visualize_choice = input("👀 Do you want to visualize the tree? (Y/N): ").strip().upper()
        if visualize_choice == "Y":
            log_info(f"📊 Tree Visualization for '{tree_name}':")
            print(tree_builder.tree.visualize())  # ✅ Print the tree structure

        # ✅ Ask user if they want to push the tree
        push_choice = input("⬆️ Do you want to push the tree to Seeq? (Y/N): ").strip().upper()
        if push_choice == "Y":
            tree_builder.tree.push()
            log_info(f"✅ Tree '{tree_name}' pushed successfully to workbook '{workbook_name}'.")
    
    except Exception as e:
        log_error(f"❌ Error creating empty tree: {e}")
        
@click.command()
@click.argument("workbook_name")
@click.argument("tree_name")
def visualize_tree(workbook_name, tree_name):
    ensure_seeq_login() 
    """CLI command to visualize an existing tree."""
    log_info(f"CLI: Visualizing tree '{tree_name}' in workbook '{workbook_name}'")

    tree_modifier = TreeModifier(workbook_name, tree_name)

    try:
        structure = tree_modifier.visualize_tree()
        log_info(f"\nTree Structure:\n{structure}")
    except Exception as e:
        log_error(f"❌ Error visualizing tree: {e}")

@click.command()
@click.argument("workbook_name")
@click.argument("tree_name")
def push_tree(workbook_name, tree_name):
    ensure_seeq_login() 
    """CLI command to push changes to an existing tree."""
    log_info(f"CLI: Pushing tree '{tree_name}' in workbook '{workbook_name}'")

    tree_modifier = TreeModifier(workbook_name, tree_name)

    try:
        tree_modifier.push_tree()
        log_info(f"✅ Tree '{tree_name}' pushed successfully.")
    except Exception as e:
        log_error(f"❌ Error pushing tree: {e}")

@click.command()
@click.argument("workbook_name")
@click.argument("tree_name")
def modify_tree(workbook_name, tree_name):
    ensure_seeq_login() 
    """CLI command to modify an existing tree."""
    log_info(f"CLI: Modifying tree '{tree_name}' in workbook '{workbook_name}'")

    tree_modifier = TreeModifier(workbook_name, tree_name)

    while True:
        print("\nWhat would you like to do next?")
        print("1. Visualize the tree")
        print("2. Insert a new item")
        print("3. Move an item")
        print("4. Remove an item")
        print("5. Push the tree")
        print("6. Exit")

        choice = input("Enter your choice (1-6): ").strip()
        
        if choice == "1":
            print("\nTree Visualization:")
            print(tree_modifier.visualize_tree())
            
        elif choice == "2":  # Insert new item
            parent = input("✔️ Enter the parent path: ").strip()
            name = input("✔️ Enter the name of the new item: ").strip()
            element_type = input("✔️ Enter the type of the element (Signal/Condition/Asset/Scalar): ").strip()
            formula = input("✔️ Enter the formula (or leave blank): ").strip() or None

            # ✅ Construct item definition properly
            item_definition = {"Name": name, "Type": element_type}
            if element_type in ["Signal", "Condition", "Scalar"]:
                item_definition["Formula"] = formula

            try:
                tree_modifier.insert_item(parent_name=parent, item_definition=item_definition)
            except Exception as e:
                print(f"❌ Error adding item: {e}")
                print(f"✅ Successfully added '{name}' under '{parent}'.")

                # ✅ **Use a FRESH tree object before pushing**
                fresh_tree = Tree(workbook=tree_modifier.tree.workbook, data=tree_modifier.tree.name)
                
                print(f"🚀 Pushing tree '{fresh_tree.name}' after insert...")
                fresh_tree.push()
                print(f"✅ Tree '{fresh_tree.name}' successfully pushed.")

            except Exception as e:
                print(f"❌ Error adding item: {e}")
                
        elif choice == "3":
            source = input("Enter the source path: ").strip()
            destination = input("Enter the destination path: ").strip()

            try:
                tree_modifier.move_item(source, destination)
                print(f"✅ Moved item from '{source}' to '{destination}'.")
            except Exception as e:
                print(f"❌ Error moving item: {e}")
                
        elif choice == "4":
            item_path = input("Enter the item path to remove: ").strip()

            try:
                tree_modifier.remove_item(item_path)
                print(f"✅ Removed item at '{item_path}'.")
            except Exception as e:
                print(f"❌ Error removing item: {e}")
                
        elif choice == "5":  # Push Tree
            try:
                log_info("🚀 Attempting to push tree...")

                # ✅ Use PushManager instead of directly pushing
                push_manager = PushManager(tree_modifier.tree)

                # ✅ Allow pushing with metadata_state_file for larger trees
                push_manager.push(metadata_state_file="tree_metadata_state.pickle.zip")

                print("✅ Tree pushed successfully.")
        
            except Exception as e:
                log_error(f"❌ Error pushing tree: {e}")
                
        elif choice == "6":
            print("👋 Exiting interactive mode.")
            break
        else:
            print("❌ Invalid choice. Please select a valid option.")

@click.group()
def cli():
    pass

cli.add_command(build_tree)
cli.add_command(create_empty_tree)
cli.add_command(visualize_tree)  
cli.add_command(push_tree)       
cli.add_command(modify_tree)

if __name__ == "__main__":
    cli()
    
# Example usage:

# build-tree:
# python src/itv_asset_tree/cli.py build-tree "TestWorkbook" "/path/to/csv.csv"

# create-empty-tree:
# python src/itv_asset_tree/cli.py create-empty-tree "Workbook1"

# visualize-tree:
# python src/itv_asset_tree/cli.py visualize-tree "Workbook1" "Test Tree"

# push-tree:
# python src/itv_asset_tree/cli.py push-tree "Workbook1" "Test Tree"

# modify-tree:
# python src/itv_asset_tree/cli.py modify-tree "Workbook1" "Test Tree"  # Interactive mode
# python src/itv_asset_tree/cli.py modify-tree "Workbook1" "Test Tree"  # Interactive mode
# python src/itv_asset_tree/cli.py modify-tree "Workbook1" "Test Tree"  # Interactive mode
# python src/itv_asset_tree/cli.py modify-tree "Template Test" "My HVAC Units" "insert"