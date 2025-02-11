# src/utilities/json_utils.py

import json

def parse_json_string(json_string: str):
    """
    Parse a JSON string and return the object.

    Parameters:
    ----------
    json_string : str
        The JSON string to parse.

    Returns:
    --------
    dict or list
        Parsed JSON object.
    """
    try:
        return json.loads(json_string)
    except json.JSONDecodeError as e:
        raise ValueError(f"❌ Invalid JSON string: {e}")