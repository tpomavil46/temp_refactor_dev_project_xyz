import sys
import os

# Add the project root to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Now import your modules
from src.managers.tree_builder import TreeBuilder
from src.managers.push_manager import PushManager
from src.utilities.csv_parser import CSVHandler
from src.utilities.duplicate_resolution import (
    DuplicateResolver, 
    KeepFirstStrategy, 
    KeepLastStrategy, 
    RemoveAllStrategy, 
    UserSpecificStrategy
)
from src.utilities.lookup_builder import LookupTableBuilder
from seeq.spy import search
from seeq.spy.assets import Tree
from tkinter import Tk, filedialog
from dotenv import load_dotenv
import pandas as pd

# Load environment variables
load_dotenv()

# Add src directory to sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(current_dir, "..", "src")
sys.path.insert(0, src_dir)

# Function to browse for a file
def browse_file():
    root = Tk()
    root.withdraw()
    file_path = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv")])
    if not file_path:
        raise ValueError("No file selected.")
    return file_path

def create_lookup_tables(resolved_csv_path):
    """
    Create lookup tables from resolved CSV.
    """
    from src.utilities.csv_parser import CSVHandler
    from src.utilities.lookup_builder import LookupTableBuilder

    print("\nLoading resolved CSV...")
    csv_handler = CSVHandler(resolved_csv_path)
    data = csv_handler.load_csv()

    group_column = input("Enter the column name for grouping (e.g., Equipment_Desc): ").strip()
    key_column = input("Enter the column name for keys (e.g., PLC_Tag_Value): ").strip()
    value_column = input("Enter the column name for values (e.g., Reason Desc): ").strip()

    builder = LookupTableBuilder(group_column, key_column, value_column)
    lookup_tables = builder.build(data)

    print("\nSpecify Parent Path for each group:")
    parent_paths = {}
    for group_name in lookup_tables.keys():
        parent_path = input(f"Enter the Parent Path for group '{group_name}': ").strip()
        parent_paths[group_name] = parent_path

    output_file = "lookup_strings_output.csv"
    LookupTableBuilder.save_lookup_to_csv(lookup_tables, parent_paths, output_file)
    print(f"Lookup tables saved to '{output_file}'.")

# Function to resolve duplicates in CSV
def resolve_duplicates(csv_path):
    """Resolve duplicates in the CSV file."""
    print("Starting duplicate resolution...")
    csv_handler = CSVHandler(csv_path)
    data = csv_handler.load_csv()
    print("CSV loaded successfully.")

    group_column = input("Enter the column name for grouping (e.g., Equipment_Desc): ").strip()
    key_column = input("Enter the column name for identifying duplicates (e.g., PLC_Tag_Value): ").strip()

    print("Choose a strategy for resolving duplicates:")
    print("1. Keep first occurrence")
    print("2. Keep last occurrence")
    print("3. Remove all duplicates")
    print("4. Specify rows to keep")
    choice = input("Enter your choice (1/2/3/4): ").strip()

    if choice == "1":
        strategy = KeepFirstStrategy()
    elif choice == "2":
        strategy = KeepLastStrategy()
    elif choice == "3":
        strategy = RemoveAllStrategy()
    elif choice == "4":
        rows = input("Enter row numbers to keep (comma-separated, e.g., '0,2'): ")
        rows_to_keep = [int(i.strip()) for i in rows.split(",")]
        strategy = UserSpecificStrategy(rows_to_keep)
    else:
        print("Invalid choice. Defaulting to 'Keep First'.")
        strategy = KeepFirstStrategy()

    resolver = DuplicateResolver(strategy)

    resolved_groups = []
    for group_name, group in data.groupby(group_column):
        resolved_group = resolver.resolve_group(group, group_name, key_column)
        resolved_groups.append(resolved_group)

    resolved_data = pd.concat(resolved_groups, ignore_index=True)
    resolved_data.to_csv("resolved_data.csv", index=False)
    print("Resolved data saved to 'resolved_data.csv'.")
    return resolved_data

# Function to add items to the tree from lookup_strings_output.csv
def add_items_from_lookup_csv(tree, csv_path="lookup_strings_output.csv"):
    """
    Add items to the tree from the lookup_strings_output.csv file.

    Parameters:
    ----------
    tree : seeq.spy.assets.Tree
        The tree object to modify.
    csv_path : str
        Path to the lookup_strings_output.csv file. Defaults to "lookup_strings_output.csv".
    """
    if not os.path.exists(csv_path):
        print(f"File '{csv_path}' not found. Please create the file using option 6.")
        return

    data = pd.read_csv(csv_path)
    for _, row in data.iterrows():
        parent_path = row.get("Parent Path", "").strip()
        name = row.get("Name", "").strip()
        formula = row.get("Formula", "").strip()
        formula_parameters = row.get("Formula Parameters", "{}")

        if not parent_path or not name:
            print(f"Skipping invalid row: {row}")
            continue

        item_definition = {
            "Name": name,
            # "Type": "Signal",  # Assuming all items are signals
            "Formula": formula,
            "Formula Parameters": eval(formula_parameters),  # Convert string to dictionary
        }

        try:
            tree.insert(children=[item_definition], parent=parent_path)
            print(f"Added item '{name}' under parent '{parent_path}'.")
        except Exception as e:
            print(f"Error adding item '{name}': {e}")

def create_lookup_tables(resolved_csv_path):
    """
    Create lookup tables from resolved CSV.
    """
    print("\nLoading resolved CSV...")
    csv_handler = CSVHandler(resolved_csv_path)
    data = csv_handler.load_csv()

    group_column = input("Enter the column name for grouping (e.g., Equipment_Desc): ").strip()
    key_column = input("Enter the column name for keys (e.g., PLC_Tag_Value): ").strip()
    value_column = input("Enter the column name for values (e.g., Reason Desc): ").strip()

    builder = LookupTableBuilder(group_column, key_column, value_column)
    lookup_tables = builder.build(data)

    print("\nSpecify Parent Path for each group:")
    parent_paths = {}
    for group_name in lookup_tables.keys():
        parent_path = input(f"Enter the Parent Path for group '{group_name}': ").strip()
        parent_paths[group_name] = parent_path

    output_file = "lookup_strings_output.csv"
    builder.save_lookup_to_csv(lookup_tables, parent_paths, output_file)
    print(f"Lookup tables saved to '{output_file}'.")

# Function to add an existing item to the tree
def add_existing_item_to_tree(tree):
    item_name = input("Enter the name of the existing item to search for: ").strip()
    search_results = search({'Name': item_name, 'Type': 'Signal'})
    if search_results.empty:
        print(f"No matches found for '{item_name}'.")
        return
    item_id = search_results.iloc[0]['ID']
    parent = input("Enter the parent name in the tree where this item should be added: ").strip()
    item_definition = {"Name": item_name, "Type": "Signal", "ID": item_id}
    try:
        tree.insert(children=[item_definition], parent=parent)
        print(f"Added '{item_name}' under '{parent}'.")
    except Exception as e:
        print(f"Error adding item: {e}")

# Function to add a new item to the tree
def add_new_item_to_tree(tree):
    parent = input("Enter the parent name: ").strip()
    name = input("Enter the name of the new element: ").strip()
    element_type = input("Enter the type of the element (Signal/Condition/Asset): ").strip()
    formula = input("Enter the formula (or leave blank): ").strip() or None
    item_definition = {"Name": name, "Type": element_type, "Formula": formula}
    try:
        tree.insert(children=[item_definition], parent=parent)
        print(f"Added '{name}' under '{parent}'.")
    except Exception as e:
        print(f"Error adding item: {e}")
        
def add_items_from_csv(tree_modifier):
    """
    Add items to the tree from a CSV file using TreeModifier.
    """
    csv_file = browse_file()  # Let the user select the CSV file
    try:
        tree_modifier.add_items_from_csv(csv_file)
        print("Items added successfully from CSV.")
    except Exception as e:
        print(f"Error adding items from CSV: {e}")

# Main interactive menu
def show_menu():
    print("\nWhat would you like to do next?")
    print("1. Visualize the tree")
    print("2. Add a new item")
    print("3. Add an existing item (search and add)")
    print("4. Push the tree to Seeq")
    print("5. Resolve duplicates in a CSV")
    print("6. Create lookup tables from a CSV")
    print("7. Add items to the tree from lookup_strings_output.csv")
    print("8. Exit")

if __name__ == "__main__":
    print("Welcome to the Seeq Asset Tree Manager!")
    workbook = input("Enter the name of the workbook to use or create: ").strip()

    try:
        use_existing_tree = input("Do you want to modify an existing tree? (Y/N): ").strip().upper()
        csv_file = None
        builder = None
        if use_existing_tree == "Y":
            tree_name = input("Enter the name of the existing tree: ").strip()
            modifier = TreeModifier(workbook=workbook, tree_name=tree_name)
        else:
            use_csv = input("Would you like to provide a CSV file to build the tree? (Y/N): ").strip().upper()
            if use_csv == "Y":
                csv_file = browse_file()

            builder = TreeBuilder(workbook=workbook, csv_file=csv_file)

            if csv_file:
                builder.parse_csv()
                friendly_name = input("Enter a friendly name for the tree: ").strip()
                description = input("Enter a description for the tree: ").strip()
                builder.build_tree_from_csv(friendly_name=friendly_name, description=description)
            else:
                friendly_name = input("Enter a friendly name for the tree: ").strip()
                description = input("Enter a description for the tree: ").strip()
                builder.build_empty_tree(friendly_name=friendly_name, description=description)

        tree_manager = modifier if use_existing_tree == "Y" else builder.get_push_manager()

        while True:
            show_menu()
            choice = input("Enter your choice (1/2/3/4/5/6/7/8): ").strip()
            if choice == "1":
                print("\nTree Visualization:")
                print(builder.tree.visualize() if builder else modifier.tree.visualize())
            elif choice == "2":
                add_new_item_to_tree(builder.tree if builder else modifier.tree)
            elif choice == "3":
                add_existing_item_to_tree(builder.tree if builder else modifier.tree)
            elif choice == "4":
                try:
                    tree_manager.push()
                    print("Tree pushed successfully.")
                except Exception as e:
                    print(f"Error pushing tree: {e}")
            elif choice == "5":
                csv_file = browse_file()
                resolve_duplicates(csv_file)
            elif choice == "6":
                resolved_csv_path = browse_file()
                create_lookup_tables(resolved_csv_path)
            elif choice == "7":
                lookup_csv_path = "lookup_strings_output.csv"  # Default path for the lookup file
                add_items_from_lookup_csv(builder.tree if builder else modifier.tree, lookup_csv_path)
            elif choice == "8":
                print("Exiting the application.")
                break
            else:
                print("Invalid choice. Please select 1, 2, 3, 4, 5, 6, 7, or 8.")
    except Exception as e:
        print(f"Error: {e}")