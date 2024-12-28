from __future__ import annotations

import queue
import re
from threading import Event, Thread
from typing import Callable

import pandas as pd

from seeq.spy import _common

SCOPED_DATA_ID_REGEX = re.compile(r'^\[(.*?)] \{(.*?)} (.*)$')

ORIGINAL_INDEX_COLUMN = '__Original Index__'


class PushResults(dict):
    _by_asset_index: dict
    _by_workbook_and_path_index: dict
    _by_data_id_index: dict
    _by_id_index: dict

    _post_thread: Thread
    _post_thread_shut_down: bool
    _post_thread_has_shut_down: Event
    _post_queue: queue.Queue
    _response_queue: queue.Queue
    _asset_side_effect_count: int

    flush_section_count: int

    POST_QUEUE_SIZE = 8

    def __init__(self, df: pd.DataFrame):
        self._by_asset_index = dict()
        self._by_workbook_and_path_index = dict()
        self._by_data_id_index = dict()
        self._by_id_index = dict()
        self._post_queue = queue.Queue(maxsize=PushResults.POST_QUEUE_SIZE)
        self._response_queue = queue.Queue()
        self._flush_section_count = 0
        self._asset_side_effect_count = 0

        records = df.to_dict(orient='index')
        super().__init__({index: PushItem(index, record, self._set_item_callback)
                          for index, record in records.items()})

    def _post_thread_main_loop(self):
        try:
            while True:
                if self._post_thread_shut_down and self._post_queue.empty():
                    break

                try:
                    func, args = self._post_queue.get(block=True, timeout=0.1)
                    func(*args)
                except queue.Empty:
                    pass
                except Exception as e:
                    self._response_queue.put((PushResults._raise_exception, (e,)))
                except BaseException as e:
                    self._response_queue.put((PushResults._raise_exception, (e,)))
                    break

        finally:
            self._post_thread_has_shut_down.set()

    def add_post(self, func, args):
        self._post_queue.put((func, args))

    def add_response(self, func, args):
        self._response_queue.put((func, args))

    def add_side_effect_asset(self, asset_dict) -> (str, PushItem):
        self._asset_side_effect_count += 1
        index = f'__side_effect_asset_{self._asset_side_effect_count}__'
        asset_dict[ORIGINAL_INDEX_COLUMN] = index
        self[index] = asset_dict
        return index, self[index]

    @staticmethod
    def _raise_exception(e):
        raise e

    def drain_responses(self):
        while True:
            try:
                func, args = self._response_queue.get_nowait()
                func(*args)
            except queue.Empty:
                break

    def start_post_thread(self):
        self._post_thread_shut_down = False
        self._post_thread_has_shut_down = Event()
        self._post_thread = Thread(target=self._post_thread_main_loop)
        self._post_thread.start()

    def shut_down_post_thread(self):
        self._post_thread_shut_down = True
        self._post_thread_has_shut_down.wait()

    def __setitem__(self, key, value):
        if not isinstance(value, dict):
            raise ValueError('value must be dict')

        if not isinstance(value, PushItem):
            value = PushItem(key, value, self._set_item_callback)
        else:
            self._set_item_callback(key, value, None)

        value.callback = self._set_item_callback

        super().__setitem__(key, value)

    def __getitem__(self, item):
        return super().__getitem__(item)

    def __contains__(self, item):
        return super().__contains__(item)

    def _set_item_callback(self, index, item, key):
        if key not in [None, 'Type', 'Name', 'Path', 'Data ID']:
            return

        self._update_indices(index, item)

    def _update_indices(self, index, item):
        if _common.get(item, 'Type') == 'Asset':
            self._by_asset_index[(_common.get(item, 'Name'), _common.get(item, 'Path'))] = index

        if _common.present(item, 'ID'):
            self._by_id_index[item['ID']] = index

        if _common.present(item, 'Data ID'):
            self._by_data_id_index[item['Data ID']] = index
            matcher = SCOPED_DATA_ID_REGEX.match(item['Data ID'])
            if matcher is not None:
                workbook_id = matcher.group(1)
                path = matcher.group(3)
                self._by_workbook_and_path_index[(workbook_id, path)] = index

    def get_by_asset(self, name, path):
        return self._by_asset_index.get((name, path))

    def get_by_workbook_and_path(self, workbook_id, path):
        return self._by_workbook_and_path_index.get((workbook_id, path))

    def get_by_data_id(self, data_id):
        return self._by_data_id_index.get(data_id)

    def get_by_id(self, _id):
        return self._by_id_index.get(_id)

    class AtIndexer:
        _parent: PushResults

        def __init__(self, push_results):
            self._parent = push_results

        def __getitem__(self, item):
            return self._parent[item[0]][item[1]]

        def __setitem__(self, key, value):
            self._parent[key[0]][key[1]] = value

        def __contains__(self, item):
            return item[0] in self._parent and item[1] in self._parent[item[0]]

    class LocIndexer:
        _parent: PushResults

        def __init__(self, push_results):
            self._parent = push_results

        def __getitem__(self, item):
            return self._parent[item]

        def __setitem__(self, key, value):
            self._parent[key] = value

        def __contains__(self, item):
            return item in self._parent

    @property
    def at(self):
        return PushResults.AtIndexer(self)

    @property
    def loc(self):
        return PushResults.LocIndexer(self)


class PushItem(dict):
    _index: object
    _callback: Callable

    def __init__(self, index, value, callback):
        super().__init__(value)
        self._index = index
        self._callback = callback
        self._callback(index, value, None)

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        self._callback(self._index, self, key)

    def __getitem__(self, item):
        return super().__getitem__(item)

    def __contains__(self, item):
        return super().__contains__(item)

    def update(self, other, **kwargs):
        super().update(other, **kwargs)
        self._callback(self._index, self, None)
