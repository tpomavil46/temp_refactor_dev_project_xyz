# src/itv_asset_tree/utils/common.py

import re
import logging

# Set up a common logger for the application
logger = logging.getLogger("itv_asset_tree")
logging.basicConfig(level=logging.INFO)

def normalize_string(value: str) -> str:
    """
    Normalizes a string by converting to lowercase, trimming spaces,
    and replacing multiple spaces with a single space.
    """
    return re.sub(r'\s+', ' ', value.strip().lower())

def log_info(message: str):
    """Logs an informational message using the common logger."""
    logger.info(message)

def log_error(message: str):
    """Logs an error message using the common logger."""
    logger.error(message)

def validate_filename(filename: str) -> bool:
    """
    Checks if a given filename is valid and does not contain
    forbidden characters.
    """
    return bool(re.match(r"^[\w,\s-]+\.[A-Za-z]{3,4}$", filename))

# Examples:

# from itv_asset_tree.utils.common import log_info, log_error

# log_info("This is an informational message.")
# log_error("An error occurred while processing.")

# ----------------------------------------------------

# from itv_asset_tree.utils.common import normalize_string

# raw_text = "   Some   Mixed CASE Text  "
# cleaned_text = normalize_string(raw_text)
# print(cleaned_text)  # Output: "some mixed case text"

# ----------------------------------------------------

# from itv_asset_tree.utils.common import validate_filename

# filename = "valid_file.csv"
# if validate_filename(filename):
#     print("✅ Filename is valid!")
# else:
#     print("❌ Invalid filename detected!")

# ----------------------------------------------------