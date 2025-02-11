# src/utilities/error_handling.py

def log_error(error_message: str):
    """Log error messages."""
    print(f"Error: {error_message}")

def handle_exception(func):
    """Decorator for handling exceptions in functions."""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            log_error(str(e))
            raise
    return wrapper