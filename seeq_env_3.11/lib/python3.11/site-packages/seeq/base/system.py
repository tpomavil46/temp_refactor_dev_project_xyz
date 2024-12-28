import os


def cleanse_path(path):
    # This is provided for compatibility with QSearch, which calls this function
    return os.path.normpath(path)
