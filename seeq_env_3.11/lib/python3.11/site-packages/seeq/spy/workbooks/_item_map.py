from __future__ import annotations

from typing import Dict, List, Optional, Union

import pandas as pd

from seeq import spy
from seeq.spy import _common
from seeq.spy._errors import *


class ItemMap:
    """
    Represents a map of item identifiers from a source (usually a saved set of workbooks) to the destination on the
    Seeq Server. This class is used extensively by spy.workbooks.push() operations.
    """
    _item_map: Union[dict, ItemMap]
    _lookup_df: Optional[pd.DataFrame]
    _logs: Dict[str, List[str]]
    _dummy_items: pd.DataFrame

    only_override_maps: bool
    fall_through: bool
    data_item_cache: _common.LRUCache

    def __getstate__(self):
        return (self._item_map, self._lookup_df, self._logs, self._dummy_items, self.only_override_maps,
                self.fall_through)

    def __setstate__(self, state):
        self.data_item_cache = _common.LRUCache()
        (self._item_map, self._lookup_df, self._logs, self._dummy_items, self.only_override_maps,
         self.fall_through) = state

    def __init__(self, item_map=None, lookup_df: Optional[pd.DataFrame] = None):
        self._item_map = item_map if item_map is not None else dict()
        self._lookup_df = lookup_df
        self._logs = dict()
        self._dummy_items = pd.DataFrame({'ID': pd.Series(dtype=str)})
        self.only_override_maps = False
        self.fall_through = False
        self.data_item_cache = _common.LRUCache()

    def __contains__(self, key):
        key = _common.ensure_upper_case_id('ID', key)
        contains = self._item_map.__contains__(key)
        if not contains and self._should_fall_through(key):
            return key
        else:
            return contains

    def __getitem__(self, key):
        key = _common.ensure_upper_case_id('ID', key)
        try:
            return self._item_map.__getitem__(key)
        except KeyError:
            if self._should_fall_through(key):
                return key

            raise KeyError(f"Item {key} not found in item map. It is either not in the workbooks' inventory or "
                           f"the inventory is not being pushed as part of this operation.")

    def __setitem__(self, key, val):
        key = _common.ensure_upper_case_id('ID', key)
        val = _common.ensure_upper_case_id('ID', val)
        self._item_map.__setitem__(key, val)

    def __delitem__(self, key):
        key = _common.ensure_upper_case_id('ID', key)
        self._item_map.__delitem__(key)

    def get(self, key, default=None):
        key = _common.ensure_upper_case_id('ID', key)
        if key in self._item_map:
            return self._item_map[key]

        if self._should_fall_through(key):
            return key
        else:
            return default

    def keys(self):
        return self._item_map.keys()

    @property
    def has_look_up_df(self):
        return self._lookup_df is not None

    def look_up_id(self, value):
        row = _common.look_up_in_df(value, self._lookup_df)
        return row['ID']

    def _should_fall_through(self, key):
        return self.fall_through and _common.is_guid(key)

    def log(self, key, message, at_top=False):
        if key not in self._logs:
            self._logs[key] = list()
        if at_top:
            self._logs[key].insert(0, message)
        else:
            self._logs[key].append(message)

    def add_dummy_item(self, dummy_item: pd.Series):
        self._dummy_items = pd.concat([self._dummy_items, dummy_item.to_frame().T])

    @property
    def dummy_items(self):
        return self._dummy_items

    def explain(self, key: Union[str, pd.DataFrame, pd.Series]):
        """
        Returns a string explaining the mapping outcome of a particular item.
        This is useful for debugging datasource mapping, both for things that
        fail and things that succeed to learn how the mapping was applied.
        Typically, you will use print() to show the returned value in the
        console.

        :param key: The item ID of the item to be explained. This can also be
        a one-row DataFrame or a Series with an 'ID' column. Note that this
        ID is of the item in the source workbook, not the ID of the pushed item
        in the destination workbook.
        :return: A string explaining the mapping outcome of a particular item.
        """
        if isinstance(key, pd.DataFrame):
            if 'ID' not in key.columns:
                raise SPyValueError('explain() requires a one-row DataFrame with an "ID" column')
            if len(key) == 0:
                raise SPyValueError('DataFrame supplied is empty')
            if len(key) > 1:
                raise SPyValueError('DataFrame supplied has more than one row')
            key = key.iloc[0]['ID']
        elif isinstance(key, pd.Series):
            if 'ID' not in key:
                raise SPyValueError('explain() requires a Series with "ID"')
            key = key['ID']
        elif not isinstance(key, str):
            raise SPyTypeError(f'Argument to explain() must be a str, DataFrame, or Series, '
                               f'not {key.__class__.__name__}')

        if key not in self._logs:
            return f'There are no mapping logs for {key}'

        return '\n'.join(self._logs[key])

    def clear_logs(self, key):
        self._logs[key] = list()


class OverrideItemMap(ItemMap):
    """
    Takes an existing ItemMap and overrides various keys. This is used extensively in the templating system to
    temporarily (for the extent of a particular frame in a callstack) override the inner ItemMap using
    template_parameters or an override_map.
    """

    _override: dict
    _parameters: Optional[dict]

    def __init__(self, item_map: ItemMap, *, template_parameters: dict = None, override_map: dict = None):
        super().__init__(item_map)

        self._override = dict() if override_map is None else override_map
        self._parameters = None
        if template_parameters:
            self._parameters = template_parameters
            self._override_from_template_parameters(template_parameters)

    def __contains__(self, key):
        key = _common.ensure_upper_case_id('ID', key)
        return self._override.__contains__(key) or self._item_map.__contains__(key)

    def __getitem__(self, key):
        key = _common.ensure_upper_case_id('ID', key)
        if self._override.__contains__(key):
            return self._override.__getitem__(key)
        else:
            return super().__getitem__(key)

    def __delitem__(self, key):
        key = _common.ensure_upper_case_id('ID', key)
        if self._override.__contains__(key):
            self._override.__delitem__(key)
        else:
            self._item_map.__delitem__(key)

    def get(self, key, default=None):
        key = _common.ensure_upper_case_id('ID', key)
        return self._override.get(key, self._item_map.get(key, default))

    @property
    def has_look_up_df(self):
        return self._item_map.has_look_up_df

    def look_up_id(self, value):
        return self._item_map.look_up_id(value)

    def keys(self):
        keys = set(self._item_map.keys())
        keys.update(self._override.keys())
        return keys

    def override(self, key, value):
        self._override[key] = value

    @property
    def parameters(self):
        if self._parameters is not None:
            return self._parameters

        if isinstance(self._item_map, OverrideItemMap):
            return self._item_map.parameters

        return None

    def _override_from_template_parameters(self, template_parameters: dict):

        for key, value in template_parameters.items():
            if value is None or (value is float and pd.isna(value)):
                continue

            _id, _type, _fqn = spy.workbooks.ItemTemplate.code_key_tuple(key)

            if _id is None:
                # This is the case of a Mustachioed annotation with {{variable}} tokens in it
                continue

            if isinstance(value, pd.DataFrame):
                if len(value) > 1:
                    raise SPyValueError(f'Multiple rows in template_parameters dict for "{key}":\n{value}')
                if len(value) == 0:
                    raise SPyValueError(f'Empty DataFrame in template_parameters dict for "{key}"')
                value = value.iloc[0].to_dict()
            elif isinstance(value, str):
                value = {'ID': value} if _common.is_guid(value) else {'Name': value}
            elif isinstance(value, spy.workbooks.ItemTemplate):
                continue

            if _common.present(value, 'ID') and not _common.get(value, 'Reference', default=False):
                self._override[_id] = _common.get(value, 'ID')
            else:
                if not self._item_map.has_look_up_df:
                    raise SPyValueError(f'Attempted lookup for template parameter "{key}" but no lookup_df argument '
                                        f'was supplied to spy.workbooks.push(). Perhaps you are using '
                                        f'spy.workbooks.push() when you meant to use spy.push(metadata=<metadata>, '
                                        f'workbook=<workbook>)?\n{value}')

                self._override[_id] = self.look_up_id(value)
