from __future__ import annotations

import dataclasses
import fnmatch
import functools
import itertools
import re
from collections import deque, defaultdict
from enum import Enum
from typing import Dict, List, Union, Tuple, Callable, Iterable, Set, Hashable, Optional

import numpy as np
import pandas as pd

from seeq.spy import _common
from seeq.spy._errors import *
from seeq.spy.assets._trees import _path
from seeq.spy.assets._trees._pandas import KeyedDataFrame

Pattern = Union[None, 'TreeNode', pd.DataFrame, List['Pattern'], dict, pd.Series, str, int]


@dataclasses.dataclass(frozen=True)
class TreeNode:
    """
    This is a helper class used to navigate the internal DataFrame of a spy.assets.Tree object in a more traditional
    object-oriented manner. Each object of this type corresponds to a row in said DataFrame.

    Currently this class is used for:
    - Resolving formula parameters referenced by path/name in the tree
    - Querying items in the tree by their name/path/depth/ID/other properties

    The KeyedDataFrame class exists solely so that the TreeNode.of() static constructor can have its results cached.
    This ensures that whenever a tree has its internal DataFrame state modified, a TreeNode version of its state is
    saved as well via the TreeNode.of() cache, and queries on its items can be made quickly without traversing the
    DataFrame again.

    NOTE: equality and hashing is determined entirely by the `index` attribute. Do not compare TreeNodes that
    originate from different DataFrames
    """
    id: str = dataclasses.field(init=True, repr=False, compare=False)
    name: str = dataclasses.field(init=True, repr=True, compare=False)
    type: str = dataclasses.field(init=True, repr=False, compare=False)
    depth: int = dataclasses.field(init=True, repr=False, compare=False)
    referenced_id: str = dataclasses.field(init=True, repr=False, compare=False)
    children: Tuple[TreeNode] = dataclasses.field(init=True, repr=False, compare=False)
    path: str = dataclasses.field(init=False, repr=True, compare=False)
    index: int = dataclasses.field(init=True, repr=False, compare=True)
    size: int = dataclasses.field(init=True, repr=False, compare=False)
    parent: TreeNode = dataclasses.field(init=False, repr=False, compare=False)
    root: TreeNode = dataclasses.field(init=False, repr=False, compare=False)

    @staticmethod
    def of(df: KeyedDataFrame) -> TreeNode:
        return TreeNode._of(df.reset_index().to_dict(orient='records'))

    @staticmethod
    def _of(records: List[dict], set_as_root=True, offset=0) -> TreeNode:
        root = records[offset]
        root_depth = root['Depth']
        size: int

        def children_generator() -> Iterable[TreeNode]:
            i = offset + 1
            while i < len(records) and records[i]['Depth'] > root_depth:
                child = TreeNode._of(records, set_as_root=False, offset=i)
                yield child
                i += child.size
            nonlocal size
            size = i - offset

        children = tuple(children_generator())

        this = TreeNode(id=_common.get(root, 'ID'),
                        name=_common.get(root, 'Name'),
                        type=_common.get(root, 'Type'),
                        depth=_common.get(root, 'Depth'),
                        referenced_id=_common.get(root, 'Referenced ID'),
                        index=root['index'],
                        size=size,
                        children=children)
        for child in children:
            object.__setattr__(child, 'parent', this)  # This bypasses immutability so we can late-initialize fields
        if set_as_root:
            object.__setattr__(this, 'parent', None)
            this._set_path_and_root_rec(path='', root=this)
        return this

    def _set_path_and_root_rec(self, path: str, root: TreeNode):
        object.__setattr__(self, 'path', path)
        object.__setattr__(self, 'root', root)
        for child in self.children:
            child._set_path_and_root_rec(self.full_path, root)

    @property
    def full_path(self) -> str:
        return f'{self.path} >> {self.name}' if self.path else self.name

    def __iter__(self) -> Iterable[TreeNode]:
        yield self
        for child in self.children:
            yield from child

    @property
    def ancestors(self) -> Iterable[TreeNode]:
        node = self
        while node != self.root:
            node = node.parent
            yield node
        yield node

    def matches(self, pattern: Pattern) -> bool:
        if pattern is None:
            return self == self.root
        if pd.api.types.is_scalar(pattern) and pd.isnull(pattern):
            # This case handles when the user only gives the 'Parent' column for some children, or gives a parent
            #  string that uses column values that aren't valid for some rows.
            return False
        if isinstance(pattern, TreeNode):
            return self == pattern
        if isinstance(pattern, pd.DataFrame):
            return self.matches(pattern.to_dict(orient='records'))
        if isinstance(pattern, list):
            return any(self.matches(sub_pattern) for sub_pattern in pattern)
        if pd.api.types.is_dict_like(pattern):
            if _common.present(pattern, 'ID'):
                return pattern['ID'] == self.id or pattern['ID'] == self.referenced_id
            if _common.present(pattern, 'Name'):
                return (not _common.present(pattern, 'Path') or _path.determine_path(pattern) == self.path) \
                    and pattern['Name'] == self.name
            return False
        if isinstance(pattern, str):
            if _common.is_guid(pattern):
                return pattern.upper() == self.id
            else:
                return self.matches_path(_common.path_string_to_list(pattern))
        if isinstance(pattern, int):
            return pattern == self.depth
        return False

    def matches_path(self, path_list: List[str]) -> bool:
        name_regex = exact_or_glob_or_regex(path_list.pop())
        if not cached_regex_eval(name_regex, self.name):
            return False
        if len(path_list) == 0:
            return True
        if not self.parent:
            return False
        return self.parent.matches_path(path_list)

    def is_name_match(self, name: str) -> bool:
        name_regex = exact_or_glob_or_regex(name)
        return bool(cached_regex_eval(name_regex, self.name))

    def resolve_reference(self, reference: str) -> TreeNode:
        reference_name = f'{self.path} >> {reference}'

        def reference_at_node(node: TreeNode, relative_path: str, full_name: str, check_self=False) -> TreeNode:
            _result = None
            for i, match in enumerate(node._relative_matches(relative_path, check_self=check_self)):
                if i == 0:
                    if match.type == 'Asset':
                        raise SPyRuntimeError(f'Formula parameter "{full_name}" is an asset. Formula parameters '
                                              f'must be conditions, scalars, or signals.')
                    _result = match
                else:
                    raise SPyRuntimeError(f'Formula parameter "{full_name}" matches multiple items in tree.')
            return _result

        relative = reference_at_node(self.parent, reference, reference_name, check_self=False)
        if relative is None:
            if reference.startswith(self.root.name):  # Assume that no one will try to wildcard-match the root
                absolute = reference_at_node(self.root, reference, reference, check_self=True)
                if absolute is None:
                    raise SPyRuntimeError(
                        f'Formula parameter is invalid, missing, or has been removed from tree: "{reference}".')
                else:
                    return absolute
            else:
                raise SPyRuntimeError(
                    f'Formula parameter is invalid, missing, or has been removed from tree: "{reference_name}".')
        else:
            return relative

    def resolve_references(self, reference: str) -> List[TreeNode]:
        reference_name = f'{self.path} >> {reference}'

        def references_at_node(node: TreeNode, relative_path: str, full_name: str, check_self=False) -> TreeNode:
            for match in node._relative_matches(relative_path, check_self=check_self):
                if match.type == 'Asset':
                    raise SPyRuntimeError(f'Formula parameter "{full_name}" is an asset. Formula parameters '
                                          f'must be conditions, scalars, or signals.')
                yield match

        relative = list(references_at_node(self.parent, reference, reference_name, check_self=False))
        if len(relative) == 0 and reference.startswith(self.root.name):
            absolute = list(references_at_node(self.root, reference, reference, check_self=True))
            if len(absolute) != 0:
                return absolute
        return relative

    def _relative_matches(self, relative_path: Union[str, List[str]], check_self=False, offset=0) -> Iterable[TreeNode]:
        components = _common.path_string_to_list(relative_path) if isinstance(relative_path, str) else relative_path
        if offset < len(components):
            if components[offset] == '..':
                if not check_self and self.parent is not None:
                    yield from self.parent._relative_matches(components, check_self=False, offset=offset + 1)
            else:
                if check_self:
                    if self.is_name_match(components[offset]):
                        if offset == len(components) - 1:
                            yield self
                        elif components[offset + 1] == '..':
                            if self.parent is not None:
                                yield from self.parent._relative_matches(components, check_self=False,
                                                                         offset=offset + 2)
                        else:
                            for child in self.children:
                                yield from child._relative_matches(components, check_self=True, offset=offset + 1)
                else:
                    for child in self.children:
                        yield from child._relative_matches(components, check_self=True, offset=offset)


class Query:
    """
    This non-user-facing class is used to select items from a tree's internal DataFrame.
    """

    class Operations(Enum):
        AND = 0
        OR = 1
        REPLACE = 2
        DIFFERENCE = 3

    class Matchers(Enum):
        CHILDREN = 0
        DESCENDANTS = 1
        SIBLINGS = 2
        PARENTS = 3
        ANCESTORS = 4

    MATCHING_FUNCTIONS: Dict[Query.Matchers, Callable[[TreeNode], Iterable[TreeNode]]] = {
        Matchers.CHILDREN: lambda node: node.children,
        Matchers.DESCENDANTS: lambda node: itertools.chain(*node.children),
        Matchers.SIBLINGS: lambda node: (sibling for sibling in node.parent.children if sibling != node),
        Matchers.PARENTS: lambda node: (node.parent,),
        Matchers.ANCESTORS: lambda node: node.ancestors,
    }

    def __init__(self, df: KeyedDataFrame):
        self.df = df
        self.tree = TreeNode.of(self.df)
        self.filters = deque()
        self.matched = set(self.tree)

    def copy(self) -> Query:
        new = Query(self.df)
        new.filter = self.filters.copy()
        if len(self.matched) != self.tree.size:
            new.matched = self.matched.copy()
        return new

    def matches(self, pattern: Pattern | Query | Callable) -> Query:
        self.filters.append((Query.Operations.AND, pattern))
        return self

    def has_type(self, item_type: str) -> Query:
        self.filters.append((Query.Operations.AND, lambda node: item_type in node.type))
        return self

    def exclude(self, pattern: Pattern) -> Query:
        self.filters.append((Query.Operations.DIFFERENCE, pattern))
        return self

    def with_children(self) -> Query:
        self.filters.append((Query.Operations.OR, Query.Matchers.CHILDREN))
        return self

    def children(self) -> Query:
        self.filters.append((Query.Operations.REPLACE, Query.Matchers.CHILDREN))
        return self

    def with_descendants(self) -> Query:
        self.filters.append((Query.Operations.OR, Query.Matchers.DESCENDANTS))
        return self

    def descendants(self) -> Query:
        self.filters.append((Query.Operations.REPLACE, Query.Matchers.DESCENDANTS))
        return self

    def with_siblings(self) -> Query:
        self.filters.append((Query.Operations.OR, Query.Matchers.SIBLINGS))
        return self

    def siblings(self) -> Query:
        self.filters.append((Query.Operations.REPLACE, Query.Matchers.SIBLINGS))
        return self

    def with_parents(self) -> Query:
        self.filters.append((Query.Operations.OR, Query.Matchers.PARENTS))
        return self

    def parents(self) -> Query:
        self.filters.append((Query.Operations.REPLACE, Query.Matchers.PARENTS))
        return self

    def with_ancestors(self) -> Query:
        self.filters.append((Query.Operations.OR, Query.Matchers.ANCESTORS))
        return self

    def ancestors(self) -> Query:
        self.filters.append((Query.Operations.REPLACE, Query.Matchers.ANCESTORS))
        return self

    def _execute(self):
        while self.filters:
            self._filter(*self.filters.popleft())

    def _filter(self, operation: Operations, matcher: Union[Query, Matchers, Callable, Pattern]):
        if isinstance(matcher, Query.Matchers):
            matching_function = Query.MATCHING_FUNCTIONS[matcher]
            new_matches = set(itertools.chain.from_iterable(matching_function(node) for node in self.matched))
        elif isinstance(matcher, Query):
            matcher._execute()
            new_matches = matcher.matched
        elif callable(matcher):
            nodes_to_check = self.matched if operation in (Query.Operations.AND,
                                                           Query.Operations.DIFFERENCE) else self.tree
            new_matches = set(node for node in nodes_to_check if matcher(node))
        else:
            nodes_to_check = self.matched if operation in (Query.Operations.AND,
                                                           Query.Operations.DIFFERENCE) else self.tree
            new_matches = set(node for node in nodes_to_check if node.matches(matcher))
        if operation == Query.Operations.AND:
            self.matched &= new_matches
        elif operation == Query.Operations.OR:
            self.matched |= new_matches
        elif operation == Query.Operations.REPLACE:
            self.matched = new_matches
        elif operation == Query.Operations.DIFFERENCE:
            self.matched -= new_matches

    def get_filtered_rows(self) -> KeyedDataFrame:
        self._execute()
        out_df = self.df.loc[self.get_mask()]
        return out_df

    def get_mask(self) -> np.ndarray:
        self._execute()
        return self.df.index.isin(set(node.index for node in self.matched))

    def get_node_list(self, sort=False) -> List[TreeNode]:
        self._execute()
        if sort:
            return [node for node in self.tree if node in self.matched]
        else:
            return list(self.matched)

    def get_node_set(self) -> Set[TreeNode]:
        self._execute()
        return self.matched

    def get_distinct_subtree_roots(self) -> Iterable[TreeNode]:
        self._execute()
        it = iter(self.tree)
        node = next(it, None)
        while node is not None:
            if node in self.matched:
                yield node
                node = next(itertools.islice(it, node.size - 1, node.size), None)
            else:
                node = next(it, None)

    def multimatch(self, pattern_series: pd.Series) -> Iterable[TreeNode, Set[int]]:
        """
        Make multiple queries at once using a pd.Series object containing multiple Patterns to match.
        Returns an generator, each element of which specifies a TreeNode that matched at least one query
        along with the indices of all given queries that it matched.

        Used by Tree.insert() when the user is inserting a DataFrame with a 'Parent' column
        """
        pattern_indices = defaultdict(set)
        unhashable_patterns = list()
        for index, pattern in pattern_series.items():
            if pd.isnull(pattern):
                continue
            if isinstance(pattern, Hashable):
                pattern_indices[pattern].add(index)
            else:
                unhashable_patterns.append((pattern, {index}))

        node_indices = defaultdict(set)
        for pattern, indices in itertools.chain(pattern_indices.items(), unhashable_patterns):
            for node in self.copy().matches(pattern).get_node_set():
                node_indices[node].update(indices)
        return node_indices.items()


def is_column_value_query(s):
    if not isinstance(s, str):
        return False
    if re.search(r'{{.*}.*}', s):
        return True
    return False


def fill_column_values(row, query: str = None, query_column=None):
    """
    Fills a column values query with actual column values from a row in a dataframe. Returns the output string
    """
    if pd.isnull(query):
        if query_column not in row:
            return np.nan
        query = row[query_column]
        if pd.isnull(query):
            return np.nan

    def _fill_column_value(col_val_query_match: re.Match):
        col_val_query = col_val_query_match[1]
        col, extract_pattern = re.fullmatch(r'{(.*?)}(.*)', col_val_query).groups(default='')
        if not _common.present(row, col):
            raise SPyValueError('Not a match')
        value = str(row[col])
        if extract_pattern == '':
            return value

        # Match against a glob pattern first, then try regex
        for pattern in (glob_with_capture_groups_to_regex(extract_pattern), extract_pattern):
            try:
                extraction = re.fullmatch(pattern, value)
                if extraction:
                    if len(extraction.groups()) != 0:
                        return extraction[1]
                    else:
                        return extraction[0]
            except re.error:
                # There may be a compilation error if the input wasn't intended to be interpreted as regex
                continue
        raise SPyValueError('Not a match')

    try:
        return re.sub(r'{({.*?}.*?)}', _fill_column_value, query)
    except SPyValueError:
        return np.nan


def fnmatch_translate_from_py3_7_8(pat):
    """
    This code is copied from Python 3.7.8 source code. This function changed significantly in 3.10 and is
    incompatible with the fixups that we do in glob_with_capture_groups_to_regex() to hack in a "capture group"
    facility to the glob.
    """

    i, n = 0, len(pat)
    res = ''
    while i < n:
        c = pat[i]
        i = i + 1
        if c == '*':
            res = res + '.*'
        elif c == '?':
            res = res + '.'
        elif c == '[':
            j = i
            if j < n and pat[j] == '!':
                j = j + 1
            if j < n and pat[j] == ']':
                j = j + 1
            while j < n and pat[j] != ']':
                j = j + 1
            if j >= n:
                res = res + '\\['
            else:
                stuff = pat[i:j]
                if '--' not in stuff:
                    stuff = stuff.replace('\\', r'\\')
                else:
                    chunks = []
                    k = i + 2 if pat[i] == '!' else i + 1
                    while True:
                        k = pat.find('-', k, j)
                        if k < 0:
                            break
                        chunks.append(pat[i:k])
                        i = k + 1
                        k = k + 3
                    chunks.append(pat[i:j])
                    # Escape backslashes and hyphens for set difference (--).
                    # Hyphens that create ranges shouldn't be escaped.
                    stuff = '-'.join(s.replace('\\', r'\\').replace('-', r'\-')
                                     for s in chunks)
                # Escape set operations (&&, ~~ and ||).
                stuff = re.sub(r'([&~|])', r'\\\1', stuff)
                i = j + 1
                if stuff[0] == '!':
                    stuff = '^' + stuff[1:]
                elif stuff[0] in ('^', '['):
                    stuff = '\\' + stuff
                res = '%s[%s]' % (res, stuff)
        else:
            res = res + re.escape(c)
    return r'(?s:%s)\Z' % res


def glob_with_capture_groups_to_regex(glob):
    """
    Converts a glob to a regex, but does not escape parentheses, so that the glob can be written with capture groups
    """
    return re.sub(r'\\([()])', r'\1', fnmatch_translate_from_py3_7_8(glob))


@functools.lru_cache(maxsize=2048)
def exact_or_glob_or_regex(pat: str) -> re.Pattern:
    try:
        return re.compile('(?i)' + '(' + ')|('.join([re.escape(pat), fnmatch.translate(pat), pat]) + ')')
    except re.error:
        return re.compile('(?i)' + '(' + ')|('.join([re.escape(pat), fnmatch.translate(pat)]) + ')')


@functools.lru_cache(maxsize=2048)
def cached_regex_eval(pattern: re.Pattern, value: str) -> Optional[re.Match]:
    return pattern.fullmatch(value)
