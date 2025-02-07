# src/utilities/retry.py

def retry_operation(operation, retries=3, *args, **kwargs):
    """
    Retry a given operation multiple times.

    Parameters:
    ----------
    operation : callable
        The function to retry.
    retries : int
        Number of retries before failing.
    args, kwargs
        Arguments to pass to the operation.
    """
    attempt = 0
    while attempt < retries:
        try:
            return operation(*args, **kwargs)
        except Exception as e:
            attempt += 1
            print(f"Attempt {attempt} failed: {e}")
    raise RuntimeError(f"🔄 Operation failed after {retries} attempts.")