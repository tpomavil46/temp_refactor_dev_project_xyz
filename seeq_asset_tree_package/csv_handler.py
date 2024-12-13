import pandas as pd

def read_csv(file_path):
    """Reads a CSV file into a pandas DataFrame.

    Args:
        file_path (str): Path to the CSV file.

    Returns:
        pd.DataFrame: DataFrame containing the CSV data.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the file is empty or has invalid structure.
    """
    try:
        df = pd.read_csv(file_path)
        if df.empty:
            raise ValueError("The CSV file is empty.")
        return df
    except FileNotFoundError:
        raise FileNotFoundError(f"The file at {file_path} was not found.")
    except Exception as e:
        raise ValueError(f"Failed to read CSV: {e}")

def validate_csv(df, required_columns):
    """Validates that the CSV DataFrame has the required columns.

    Args:
        df (pd.DataFrame): DataFrame to validate.
        required_columns (list): List of required column names.

    Returns:
        bool: True if validation passes, otherwise raises an error.

    Raises:
        ValueError: If required columns are missing.
    """
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"The following required columns are missing: {missing_columns}")
    return True