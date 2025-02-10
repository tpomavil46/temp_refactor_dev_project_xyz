# src/utilities/csv_parser.py

import os
import pandas as pd

class CSVHandler:
    """
    Handles CSV loading and validation.
    """

    def __init__(self, csv_path):
        self.csv_path = csv_path
        self.data = None

    def load_csv(self):
        """Load the CSV file."""
        if not os.path.exists(self.csv_path):
            raise FileNotFoundError(f"❌ File '{self.csv_path}' not found.")

        self.data = pd.read_csv(self.csv_path)
        print("✅ CSV loaded successfully.\n")
        print("📊 Columns in the file:", list(self.data.columns))
        return self.data