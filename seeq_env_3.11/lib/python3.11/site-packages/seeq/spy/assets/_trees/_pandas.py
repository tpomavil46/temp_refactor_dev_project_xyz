import functools
import inspect
import secrets
import types

import numpy as np
import pandas as pd

from seeq.spy.assets._trees import _constants


class KeyedDataFrame(pd.DataFrame):
    """
    This class extends the pd.DataFrame class and adds the following additional structure:

    - Every unique KeyedDataFrame object has its own unique key, which is used for hashing and comparison
    - Every KeyedDataFrame method call that returns a DataFrame returns a KeyedDataFrame with a new key
    - Every KeyedDataFrame method call that returns None resets the instance's key (using the greedy assumption that
        every such method modifies the object in place)

    We use this class in favor of pd.DataFrame to represent the internal state of a spy.assets.Tree instance so that
    we may cache the results of operations on its state.
    """

    _key: bytes

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.reset_key()
        self.set_column_types()

    @staticmethod
    def of(df: pd.DataFrame):
        if isinstance(df, KeyedDataFrame):
            return df
        else:
            # The following three lines are adapted from the pd.DataFrame.copy() source code
            data = df._mgr.copy(deep=False)
            df._clear_item_cache()
            return KeyedDataFrame(data).__finalize__(df)

    # noinspection PyMethodParameters
    def do_not_override(method: types.MethodType, *_):
        @functools.wraps(method)
        def f(*args, **kwargs):
            return method(*args, **kwargs)

        setattr(f, '_do_not_override', True)
        return f

    @do_not_override
    def reset_key(self):
        self._key = secrets.token_bytes()

    def set_column_types(self):
        # Pandas 2 is stricter about column types. Set the types of known columns upfront.
        for column in _constants.dataframe_columns:
            if column in self.columns:
                self[column] = self[column].astype(object)
            else:
                self[column] = pd.Series(np.nan, self.index, dtype=object)

    @do_not_override
    def __hash__(self):
        return hash((self._key,))

    @do_not_override
    def __eq__(self, other):
        return isinstance(other, KeyedDataFrame) and self._key == other._key

    @do_not_override
    def __getattribute__(self, item):
        attr = super().__getattribute__(item)
        if inspect.ismethod(attr) and not getattr(attr, '_do_not_override', False):
            return KeyedDataFrame._wrap_frame_method(attr)
        else:
            return attr

    @staticmethod
    def _wrap_frame_method(method: types.MethodType):
        @functools.wraps(method)
        def overridden_method(*args, **kwargs):
            result = method(*args, **kwargs)
            if result is None:
                method.__self__.reset_key()
            if isinstance(result, pd.DataFrame):
                result = KeyedDataFrame.of(result)
            return result

        return overridden_method
