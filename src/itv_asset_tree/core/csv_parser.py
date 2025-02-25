import pandas as pd

class CSVParser:
    """Handles CSV file parsing and validation."""

    @staticmethod
    def parse_csv(file_path: str) -> pd.DataFrame:
        """Loads a CSV file and validates required columns."""
        data = pd.read_csv(file_path)
        if "Level 1" not in data.columns:
            raise ValueError("⚠️ CSV file must contain a 'Level 1' column.")
        return data