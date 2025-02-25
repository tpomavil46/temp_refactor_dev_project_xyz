# src/utilities/metadata_handler.py

import pickle

def save_metadata(metadata, file_path):
    """Save metadata to a file."""
    with open(file_path, "wb") as f:
        pickle.dump(metadata, f)
    print(f"✅ Metadata saved to {file_path}")

def load_metadata(file_path):
    """Load metadata from a file."""
    with open(file_path, "rb") as f:
        metadata = pickle.load(f)
    print(f"✅ Metadata loaded from {file_path}")
    return metadata