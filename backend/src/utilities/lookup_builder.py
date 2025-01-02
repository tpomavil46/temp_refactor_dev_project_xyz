# import csv

# class LookupTableBuilder:
#     """Builds lookup tables from cleaned data."""

#     def __init__(self, group_column, key_column, value_column):
#         self.group_column = group_column
#         self.key_column = key_column
#         self.value_column = value_column

#     def build(self, data):
#         """
#         Builds lookup tables as a dictionary.

#         Args:
#             data (DataFrame): The DataFrame containing the data.

#         Returns:
#             dict: Grouped lookup table data.
#         """
#         lookup_tables = {}
#         for group_name, group in data.groupby(self.group_column):
#             # Properly format keys and values with single quotes
#             table = [[f"'{str(row[self.key_column])}'", f"'{str(row[self.value_column])}'"] for _, row in group.iterrows()]
#             lookup_tables[group_name] = table
#         return lookup_tables

#     @staticmethod
#     def save_lookup_to_csv(lookup_data, parent_paths, output_file):
#         """
#         Save lookup table data to a CSV file with single quotes around both keys and values.

#         Args:
#             lookup_data (dict): Lookup table dictionary where keys are group names.
#             parent_paths (dict): Dictionary mapping group names to Parent Paths.
#             output_file (str): Path to save the output CSV file.
#         """
#         fields = ["Name", "Formula", "Formula Parameters", "Parent Path"]
#         rows = []

#         for group_name, table in lookup_data.items():
#             # Construct each key-value pair with single quotes
#             formatted_table = [
#                 f"[{key}, {value}]" for key, value in table
#             ]
#             # Join the pairs into a single list string
#             formatted_formula = "[" + ", ".join(formatted_table) + "]"

#             # Debug output for verification
#             print(f"DEBUG: Group '{group_name}' -> Formula: {formatted_formula}")

#             rows.append({
#                 "Name": group_name.replace(" ", "_") + "_LookupString",
#                 "Formula": formatted_formula,
#                 "Formula Parameters": "{}",
#                 "Parent Path": parent_paths.get(group_name, "Root Asset"),
#             })

#         # Write the rows to a CSV file
#         with open(output_file, mode="w", newline="", encoding="utf-8") as file:
#             writer = csv.DictWriter(file, fieldnames=fields)
#             writer.writeheader()
#             writer.writerows(rows)

#         print(f"Lookup CSV file '{output_file}' created successfully.")

import csv
import json

class LookupTableBuilder:
    """Builds lookup tables from cleaned data."""

    def __init__(self, group_column, key_column, value_column):
        self.group_column = group_column
        self.key_column = key_column
        self.value_column = value_column

    def build(self, data):
        """
        Builds lookup tables as a dictionary.

        Args:
            data (DataFrame): The DataFrame containing the data.

        Returns:
            dict: Grouped lookup table data.
        """
        lookup_tables = {}
        for group_name, group in data.groupby(self.group_column):
            table = [[str(row[self.key_column]), str(row[self.value_column])] for _, row in group.iterrows()]
            lookup_tables[group_name] = table
        return lookup_tables

    @staticmethod
    def save_lookup_to_csv(lookup_data, parent_paths, output_file):
        """
        Save lookup table data to a CSV file in the required format.

        Args:
            lookup_data (dict): Lookup table dictionary where keys are group names.
            parent_paths (dict): Dictionary mapping group names to Parent Paths.
            output_file (str): Path to save the output CSV file.
        """
        fields = ["Name", "Formula", "Formula Parameters", "Parent Path"]
        rows = []

        for group_name, table in lookup_data.items():
            # Manually construct the formula string to avoid excess escaping
            formatted_formula = '"' + "[" + ", ".join([f"['{key}', '{value}']" for key, value in table]) + "]" + '"'
            # Debugging output to verify construction
            print(f"DEBUG: Writing formula for group '{group_name}': {formatted_formula}")

            rows.append({
                "Name": group_name.replace(" ", "_") + "_LookupString",
                "Formula": formatted_formula,
                "Formula Parameters": "{}",
                "Parent Path": parent_paths.get(group_name, "Root Asset"),
            })

        # Write to CSV
        with open(output_file, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)

        print(f"Lookup CSV file '{output_file}' created successfully.")