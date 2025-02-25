from functools import lru_cache

@lru_cache(maxsize=128)
def cached_tree_data(tree_id: int):
    """Caches tree data for a given ID to prevent redundant queries."""
    print(f"Fetching tree data for {tree_id}...")  # Simulating DB call
    return {"tree_id": tree_id, "data": "Tree structure info"}