# src/utilities/duplicate_resolution.py

class DuplicateStrategy:
    """Base class for duplicate resolution strategies."""
    def resolve(self, group, key_column):
        raise NotImplementedError

class KeepFirstStrategy(DuplicateStrategy):
    def resolve(self, group, key_column):
        return group.drop_duplicates(subset=key_column, keep='first')

class KeepLastStrategy(DuplicateStrategy):
    def resolve(self, group, key_column):
        return group.drop_duplicates(subset=key_column, keep='last')

class RemoveAllStrategy(DuplicateStrategy):
    def resolve(self, group, key_column):
        return group[~group.duplicated(subset=key_column, keep=False)]

class UserSpecificStrategy(DuplicateStrategy):
    def __init__(self, rows_to_keep):
        self.rows_to_keep = rows_to_keep

    def resolve(self, group, key_column):
        return group.iloc[self.rows_to_keep]

class DuplicateResolver:
    """Resolves duplicates based on user-selected strategies."""
    def __init__(self, strategy: DuplicateStrategy):
        self.strategy = strategy

    def resolve_group(self, group, group_name, key_column):
        """Resolve duplicates within a single group."""
        duplicates = group[group.duplicated(subset=key_column, keep=False)]
        if duplicates.empty:
            print(f"✅ No duplicates found in group '{group_name}'.")
            return group

        print(f"\n✅ Resolving duplicates for group: {group_name}")
        print(duplicates[[key_column]])
        
        print(f"→ Group Name: {group_name}")
        print(f"⚠️ Duplicates detected: {len(duplicates)}")
        # print(f"Resolved group size: {len(resolved_group)}")

        resolved_group = self.strategy.resolve(group, key_column)
        return resolved_group