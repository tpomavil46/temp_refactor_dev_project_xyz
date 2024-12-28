from __future__ import annotations

import datetime
import json
import os
import re
from typing import Union, Optional

import numpy as np
import pandas as pd
import pytz
from deprecated import deprecated

from seeq.base import util
from seeq.sdk import *
from seeq.spy import _common
from seeq.spy import _login
from seeq.spy._errors import *
from seeq.spy._redaction import request_safely
from seeq.spy._session import Session
from seeq.spy._status import Status
from seeq.spy.workbooks._data import StoredSignal, StoredCondition, StoredOrCalculatedItem, CalculatedItem
from seeq.spy.workbooks._data import ThresholdMetric
from seeq.spy.workbooks._item import Item, Reference, replace_items
from seeq.spy.workbooks._item_map import ItemMap
from seeq.spy.workbooks._trend_toolbar import TrendToolbar


class Workstep(Item):
    # This class is used as a key within a dictionary so that it gets filtered out when we do json.dumps(skipkeys=True)
    class OriginalDict:
        pass

    def __new__(cls, *args, **kwargs):
        if cls is Workstep:
            # There used to be a TopicWorkstep. This is somewhat obsolete but we decided not to consolidate the
            # Workstep base class into AnalysisWorkstep yet.
            raise SPyTypeError("Workstep may not be instantiated directly, create AnalysisWorkstep")

        return object.__new__(cls)

    def __init__(self, worksheet=None, definition=None):
        super().__init__(definition)

        self.worksheet = worksheet
        if self.worksheet:
            self.worksheet.worksteps[self.definition['ID']] = self
        self.previous_workstep_id = None
        if 'Data' not in self._definition:
            self._definition['Data'] = json.loads(_common.DEFAULT_WORKBOOK_STATE)

    @property
    def definition_hash(self):
        return Item.digest_hash(self.data)

    @property
    def name(self):
        # noinspection PyBroadException
        try:
            return self.definition_hash
        except Exception:
            # This can happen if the content of the workstep is something json.dumps doesn't like
            return self.id

    @property
    def fqn(self):
        return f'{self.worksheet.fqn} >> Workstep {self.id}'

    @staticmethod
    def pull(workstep_tuple, *, worksheet=None, session: Session = None, status=None):
        session = Session.validate(session)
        # Note that worksteps from other workbooks/worksheets can be referenced in Journals due to copy/paste
        # operations, so we can't assume that this workstep's self.worksheet actually represents the one to pull.
        workbook_id, worksheet_id, workstep_id = workstep_tuple
        workstep = AnalysisWorkstep(worksheet, {'ID': workstep_id})
        workstep._pull(session, workstep_tuple, status)
        return workstep

    def _pull(self, session: Session, workstep_tuple, status: Status):
        workbook_id, worksheet_id, workstep_id = workstep_tuple
        workbooks_api = WorkbooksApi(session.client)

        @request_safely(action_description=f'get Workstep details at {workbook_id}/{worksheet_id}/{workstep_id}',
                        status=status)
        def _request_workstep():
            workstep_output = workbooks_api.get_workstep(workbook_id=workbook_id,
                                                         worksheet_id=worksheet_id,
                                                         workstep_id=workstep_id)  # type: WorkstepOutputV1

            self._definition['Data'] = json.loads(workstep_output.data)
            self.previous_workstep_id = workstep_output.previous

        _request_workstep()

    def _validate_before_push(self):
        pass

    def _apply_map(self, item_map: ItemMap, **kwargs):
        derived_class = AnalysisWorkstep
        definition_str = _common.safe_json_dumps(self.definition_dict)
        replaced_str = replace_items(definition_str, item_map)
        definition_dict = json.loads(replaced_str)
        kwargs['definition'] = definition_dict
        return derived_class(**kwargs)

    def push_to_specific_worksheet(self, session: Session, pushed_workbook_id, pushed_worksheet_output, item_map,
                                   include_inventory, *, no_workstep_message=None):
        workbooks_api = WorkbooksApi(session.client)

        item_map = item_map if item_map is not None else dict()

        self._validate_before_push()

        workstep_to_push = self._apply_map(item_map)

        workstep_input = WorkstepInputV1(data=_common.safe_json_dumps(workstep_to_push.data))
        workstep_output = workbooks_api.create_workstep(workbook_id=pushed_workbook_id,
                                                        worksheet_id=pushed_worksheet_output.id,
                                                        no_workstep_message=no_workstep_message,
                                                        body=workstep_input)  # type: WorkstepOutputV1
        item_map[self.id] = workstep_output.id

        return workstep_output.id

    @property
    def data(self):
        return _common.get(self.definition, 'Data', default=dict())

    @property
    def referenced_items(self):
        referenced_items = set()

        matches = re.finditer(_common.GUID_REGEX, _common.safe_json_dumps(self.data), re.IGNORECASE)

        for match in matches:
            referenced_items.add(Reference(match.group(0), Reference.DETAILS, self.worksheet))

        return list(referenced_items)

    def referenced_items_df(self):
        unique_ids = {r.id for r in self.referenced_items}
        return pd.DataFrame([self.worksheet.workbook.item_inventory[_id].definition_dict for _id in unique_ids])

    def get_stored_item_references(self, item_inventory: dict) -> set:
        referenced_items = set()
        for reference in self.referenced_items:
            self._stored_item_reference_processor(reference.id, item_inventory, referenced_items)

        return referenced_items

    def _stored_item_reference_processor(self, item_id, item_inventory, referenced_items: set):
        if item_id not in item_inventory:
            # This can happen when there are references to workbooks, worksheets or worksteps within the workstep's
            # JSON data
            return

        item = item_inventory[item_id]
        if isinstance(item, (StoredSignal, StoredCondition)):
            referenced_items.add(item)
        elif isinstance(item, CalculatedItem):
            if isinstance(item, ThresholdMetric):
                parameters = item['Formula Parameters']
                if 'Bounding Condition' in parameters:
                    self._stored_item_reference_processor(parameters['Bounding Condition'], item_inventory,
                                                          referenced_items)
                if 'Measured Item' in parameters:
                    self._stored_item_reference_processor(parameters['Measured Item'], item_inventory, referenced_items)
                for threshold_dict in parameters.get('Thresholds', list()):
                    if 'Item ID' in threshold_dict:
                        self._stored_item_reference_processor(threshold_dict['Item ID'], item_inventory,
                                                              referenced_items)
            else:
                self._find_stored_item_in_calc_hierarchy(item, item_inventory, referenced_items)

    def _find_stored_item_in_calc_hierarchy(self, calc_item: Item, item_inventory: dict, referenced_items: set):
        formula_parameters = calc_item['Formula Parameters']
        for parameter_id in formula_parameters.values():
            if not _common.is_guid(parameter_id):
                # This can happen for Chart objects, specifically the "viewCapsule" parameter
                continue

            if parameter_id not in item_inventory:
                # This can happen when there are pull errors
                continue

            parameter = item_inventory[parameter_id]
            if isinstance(parameter, (StoredSignal, StoredCondition)):
                referenced_items.add(parameter)
            elif isinstance(parameter, CalculatedItem):
                self._find_stored_item_in_calc_hierarchy(parameter, item_inventory, referenced_items)

    def get_workstep_stores(self):
        workstep_data = _common.get(self.definition, 'Data', default=dict(), assign_default=True)
        workstep_state = _common.get(workstep_data, 'state', default=dict(), assign_default=True)
        return _common.get(workstep_state, 'stores', default=dict(), assign_default=True)

    def _get_workstep_version(self):
        workstep_data = _common.get(self.definition, 'Data', default=dict())
        return _common.get(workstep_data, 'version', default=0)

    @staticmethod
    def _get_workstep_json_file(workbook_folder, worksheet_id, workstep_id):
        return os.path.join(workbook_folder, 'Worksheet_%s_Workstep_%s.json' % (worksheet_id, workstep_id))

    def save(self, workbook_folder):
        workstep_json_file = Workstep._get_workstep_json_file(workbook_folder, self.worksheet.id, self.id)
        with util.safe_open(workstep_json_file, 'w', encoding='utf-8') as f:
            json.dump(self._definition, f, indent=4)

    def _load(self, workbook_folder, workstep_id):
        workstep_json_file = Workstep._get_workstep_json_file(workbook_folder, self.worksheet.id, workstep_id)

        with util.safe_open(workstep_json_file, 'r', encoding='utf-8') as f:
            self._definition = json.load(f)

    @staticmethod
    def load_from_workbook_folder(worksheet, workbook_folder, workstep_id):
        workstep = AnalysisWorkstep(worksheet, {'ID': workstep_id})
        workstep._load(workbook_folder, workstep_id)
        return workstep

    def _get_store(self, store_name):
        workstep_stores = self.get_workstep_stores()
        return _common.get(workstep_stores, store_name, default=dict(), assign_default=True)

    @property
    def timezone(self):
        return self._get_timezone()

    @timezone.setter
    def timezone(self, value):
        self._set_timezone(value)

    def _get_timezone(self):
        worksheet_store = self._get_store('sqWorksheetStore')
        timezone = _common.get(worksheet_store, 'timezone')
        return timezone

    def _set_timezone(self, timezone):
        if timezone not in pytz.all_timezones:
            raise SPyRuntimeError(f'The timezone {timezone} is not a known timezone')
        worksheet_store = self._get_store('sqWorksheetStore')
        worksheet_store['timezone'] = timezone


class AnalysisWorkstep(Workstep):
    def __init__(self, worksheet=None, definition=None):
        super().__init__(worksheet, definition)

        # initialize displayed items
        if self.display_items.empty:
            display_items_stores = self._store_map_from_type('all', self._get_workstep_version())
            stores = self.get_workstep_stores()
            for store in display_items_stores:
                current_store = _common.get(stores, store, default=dict(), assign_default=True)
                current_store['items'] = []

        # initialize the display and investigate ranges
        if self.display_range is None and self.investigate_range is None:
            self.set_display_range(
                {'Start': datetime.datetime.now() - pd.Timedelta(days=1), 'End': datetime.datetime.now()},
                check_investigate=False)
            self.set_investigate_range(
                {'Start': datetime.datetime.now() - pd.Timedelta(days=7), 'End': datetime.datetime.now()},
                check_display=False)
        # initialize the view
        if self.view is None:
            self._set_view_key()
        self._trend_toolbar = TrendToolbar(self)

    @property
    def trend_toolbar(self):
        """
        Trend toolbar for configuring the worksheet's trend toolbar settings.
        """
        return self._trend_toolbar

    @property
    def display_items(self):
        return self._get_display_items()

    @display_items.setter
    def display_items(self, value):
        self._set_display_items(value)

    @property
    def display_range(self):
        return self._get_display_range()

    @display_range.setter
    def display_range(self, value):
        self.set_display_range(value)

    @property
    def investigate_range(self):
        return self._get_investigate_range()

    @investigate_range.setter
    def investigate_range(self, value):
        self.set_investigate_range(value)

    @property
    def view(self):
        return self._get_view_key()

    @view.setter
    def view(self, value):
        self._set_view_key(value)

    @property
    @deprecated(reason='Use self.table_date_display instead')
    def scorecard_date_display(self):
        return self.table_date_display

    @scorecard_date_display.setter
    @deprecated(reason='Use self.table_date_display instead')
    def scorecard_date_display(self, value):
        self.table_date_display = value

    @property
    @deprecated(reason='Use self.table_date_format instead')
    def scorecard_date_format(self):
        return self.table_date_format

    @scorecard_date_format.setter
    @deprecated(reason='Use self.table_date_format instead')
    def scorecard_date_format(self, value):
        self.table_date_format = value

    @property
    def table_date_display(self):
        return self._get_table_date_display()

    @table_date_display.setter
    def table_date_display(self, value):
        self._set_table_date_display(value)

    @property
    def table_date_format(self):
        return self._get_table_date_format()

    @table_date_format.setter
    def table_date_format(self, value):
        self._set_table_date_format(value)

    @property
    def table_mode(self):
        return self._get_table_mode()

    @table_mode.setter
    def table_mode(self, value):
        self._set_table_mode(value)

    @property
    def scatter_plot_series(self):
        return self._get_scatter_plot_series()

    @scatter_plot_series.setter
    def scatter_plot_series(self, value):
        self._set_scatter_plot_series(value)

    def set_as_current(self):
        self.worksheet['Current Workstep ID'] = self.id

    def _get_display_range(self):
        """
        Get the display range

        See worksheet properties "display_range" for docs
        :return:
        """
        duration_store = self._get_store('sqDurationStore')
        display_range = _common.get(duration_store, 'displayRange', default=dict())
        if not display_range:
            return None
        # noinspection PyBroadException
        try:
            # Note: If the frontend stores the values as numeric, it's Epoch seconds rather than ns.
            start_raw = display_range['start']
            start_val = start_raw * 1_000_000 if not isinstance(start_raw, str) else start_raw
            start = _common.convert_to_timestamp(start_val, 'UTC')
            end_raw = display_range['end']
            end_val = end_raw * 1_000_000 if not isinstance(end_raw, str) else end_raw
            end = _common.convert_to_timestamp(end_val, 'UTC')
        except KeyboardInterrupt:
            raise
        except Exception:
            # Because this is accessed as a property, we don't want to raise an exception here
            return None
        return {'Start': start, 'End': end}

    def set_display_range(self, display_start_end, check_investigate=True, session: Optional[Session] = None):
        """
        Set the display range

        See worksheet properties "display_range" for docs
        :return:
        """
        session = Session.validate(session)

        if isinstance(display_start_end, pd.DataFrame):
            if len(display_start_end) > 1:
                raise SPyValueError('Display Range DataFrames are limited to one row')
            display_start_end = display_start_end.squeeze()

        start_ts, end_ts = _login.validate_start_and_end(session, display_start_end['Start'], display_start_end['End'])

        if not isinstance(start_ts, (str, datetime.datetime)) or not isinstance(end_ts, (str, datetime.datetime)):
            raise SPyValueError('Display range times must be ISO8601 strings or pandas datetime objects')
        try:
            if isinstance(start_ts, str):
                start_ts = pd.to_datetime(start_ts)
            if isinstance(end_ts, str):
                end_ts = pd.to_datetime(end_ts)
        except ValueError as e:
            raise SPyValueError(f'Display range times must be valid ISO8601 strings. Error parsing dates: {e}')

        if check_investigate:
            investigate_range = self.investigate_range
            if investigate_range is None:
                self.set_investigate_range(display_start_end, check_display=False, session=session)
            else:
                i_start = pd.to_datetime(investigate_range['Start'])
                i_end = pd.to_datetime(investigate_range['End'])
                i_duration = i_end - i_start
                if i_duration < end_ts - start_ts:
                    self.set_investigate_range(display_start_end, check_display=False, session=session)
                elif start_ts > i_end:
                    self.set_investigate_range({'Start': end_ts - i_duration, 'End': end_ts},
                                               check_display=False, session=session)
                elif end_ts < i_start:
                    self.set_investigate_range({'Start': start_ts, 'End': start_ts + i_duration},
                                               check_display=False, session=session)

        start_unix_ms = int(start_ts.timestamp() * 1000)
        end_unix_ms = int(end_ts.timestamp() * 1000)
        duration_store = self._get_store('sqDurationStore')
        auto_update = _common.get(duration_store, 'autoUpdate', default=dict(), assign_default=True)
        auto_update['offset'] = int(end_unix_ms - datetime.datetime.now().timestamp() * 1000)
        display_range = _common.get(duration_store, 'displayRange', default=dict(), assign_default=True)
        display_range['start'] = start_unix_ms
        display_range['end'] = end_unix_ms

    def _get_investigate_range(self):
        """
        Get the investigate range

        See worksheet property "investigate_range" for docs
        :return:
        """
        duration_store = self._get_store('sqDurationStore')
        investigate_range = _common.get(duration_store, 'investigateRange', default=None)
        if investigate_range is None:
            return None
        # noinspection PyBroadException
        try:
            # Note: If the frontend stores the values as numeric, it's Epoch seconds rather than ns.
            start_raw = investigate_range['start']
            start_val = start_raw * 1_000_000 if not isinstance(start_raw, str) else start_raw
            start = _common.convert_to_timestamp(start_val, 'UTC')
            end_raw = investigate_range['end']
            end_val = end_raw * 1_000_000 if not isinstance(end_raw, str) else end_raw
            end = _common.convert_to_timestamp(end_val, 'UTC')
        except KeyboardInterrupt:
            raise
        except Exception:
            # Because this is accessed as a property, we don't want to raise an exception here
            return None
        return {'Start': start, 'End': end}

    def set_investigate_range(self, investigate_start_end, check_display=True, session: Optional[Session] = None):
        """
        Set the investigate range

        See worksheet property "investigate_range" for docs
        """
        session = Session.validate(session)

        if isinstance(investigate_start_end, pd.DataFrame):
            if len(investigate_start_end) > 1:
                raise SPyValueError('Investigate Range DataFrames are limited to one row ')
            investigate_start_end = investigate_start_end.squeeze()

        start_ts, end_ts = _login.validate_start_and_end(session,
                                                         investigate_start_end['Start'],
                                                         investigate_start_end['End'])

        if not isinstance(start_ts, (str, datetime.datetime)) or not isinstance(end_ts, (str, datetime.datetime)):
            raise SPyValueError('Investigate range times must be ISO8601 strings or pandas datetime objects')
        try:
            if isinstance(start_ts, str):
                start_ts = pd.to_datetime(start_ts)
            if isinstance(end_ts, str):
                end_ts = pd.to_datetime(end_ts)
        except ValueError as e:
            raise SPyValueError(f'Investigate range times must be valid ISO8601 strings. Error parsing dates: {e}')

        if check_display:
            display_range = self.display_range
            if display_range is None:
                self.set_display_range(investigate_start_end, check_investigate=False, session=session)
            else:
                d_start = pd.to_datetime(display_range['Start'])
                d_end = pd.to_datetime(display_range['End'])
                d_duration = d_end - d_start
                if d_duration > end_ts - start_ts:
                    self.set_display_range(investigate_start_end, check_investigate=False, session=session)
                elif d_start > end_ts:
                    self.set_display_range({'Start': end_ts - d_duration, 'End': end_ts},
                                           check_investigate=False, session=session)
                elif d_end < start_ts:
                    self.set_display_range({'Start': start_ts, 'End': start_ts + d_duration},
                                           check_investigate=False, session=session)

        start_unix_ms = int(start_ts.timestamp() * 1000)
        end_unix_ms = int(end_ts.timestamp() * 1000)
        duration_store = self._get_store('sqDurationStore')
        duration_store['investigateRange'] = {'start': start_unix_ms, 'end': end_unix_ms}

    def _get_table_date_display(self):
        """
        Get the current table date display

        See worksheet property "table_date_display" for docs

        :return: str the current date display
        """
        workstep_stores = self.get_workstep_stores()

        table_builder_stores = _common.get(workstep_stores, 'sqTableBuilderStore', default=dict(), assign_default=True)
        headers = _common.get(table_builder_stores, 'headers', default=dict(), assign_default=True)
        if 'type' in headers:
            # Workbook has been migrated from version 51
            return self._table_date_display_workstep_to_user[headers['type']]
        if 'mode' in table_builder_stores and table_builder_stores['mode'] in headers:
            if 'type' in headers[table_builder_stores['mode']]:
                return self._table_date_display_workstep_to_user[headers[table_builder_stores['mode']]['type']]

        metric_stores = _common.get(workstep_stores, 'sqTrendMetricStore', default=dict())
        if 'scorecardHeaders' in metric_stores and 'type' in metric_stores['scorecardHeaders']:
            # Workbook has been migrated from version <= 50
            return self._table_date_display_workstep_to_user[metric_stores['scorecardHeaders']['type']]

        return None

    def _set_table_date_display(self, date_display):
        """
        Set the table date display.

        See worksheet property "table_date_display" for docs

        Parameters
        ----------
        date_display : str or None
            The string defining the date display. Can be one of:
            [None, 'Start', 'End', 'Start And End']
        """
        if date_display not in self._table_date_display_user_to_workstep.keys():
            raise SPyValueError(f'The Table date display value of {date_display} is not recognized. '
                                f'Valid values are {self._table_date_display_user_to_workstep.keys()}')
        workstep_stores = self.get_workstep_stores()
        table_builder_stores = _common.get(workstep_stores, 'sqTableBuilderStore',
                                           default=self._default_table_state,
                                           assign_default=True)
        headers = _common.get(table_builder_stores, 'headers', default=dict(), assign_default=True)
        for mode in ['simple', 'condition']:
            mode_headers = _common.get(headers, mode, default=dict(), assign_default=True)
            mode_headers['type'] = self._table_date_display_user_to_workstep[date_display]

    _table_date_display_user_to_workstep = {
        None: 'none',
        'Start': 'start',
        'End': 'end',
        'Start And End': 'startEnd'
    }

    _table_date_display_workstep_to_user = {v: k for k, v in _table_date_display_user_to_workstep.items()}

    def _get_table_date_format(self):
        """
        Get the formatting string used for the table date.

        See worksheet property "table_date_format" for docs

        :return: str the table date formatting string
        """
        workstep_stores = self.get_workstep_stores()

        table_builder_stores = _common.get(workstep_stores, 'sqTableBuilderStore',
                                           default=self._default_table_state,
                                           assign_default=True)
        headers = _common.get(table_builder_stores, 'headers', default=dict(), assign_default=True)
        if 'format' in headers:
            # Workbook has been migrated from version 51
            return headers['format']
        if 'mode' in table_builder_stores and table_builder_stores['mode'] in headers:
            if 'format' in headers[table_builder_stores['mode']]:
                return headers[table_builder_stores['mode']]['format']

        metric_stores = _common.get(workstep_stores, 'sqTrendMetricStore', default=dict())
        if 'scorecardHeaders' in metric_stores and 'format' in metric_stores['scorecardHeaders']:
            # Workbook has been migrated from version <= 50
            return metric_stores['scorecardHeaders']['format']

        return None

    def _set_table_date_format(self, date_format):
        """
        Set the table date format

        See worksheet property "table_date_format" for docs

        Parameters
        ----------
        date_format : str
            The string defining the date format. Formats are parsed using
            momentjs. The full documentation for the momentjs date parsing
            can be found at https://momentjs.com/docs/#/displaying/

        Examples
        --------
        "d/m/yyy" omitting leading zeros (eg, 4/27/2020): l
        "Mmm dd, yyyy, H:MM AM/PM" (eg, Apr 27, 2020 5:00 PM) : lll
        "H:MM AM/PM" (eg, "5:00 PM"): LT
        """
        workstep_stores = self.get_workstep_stores()
        table_builder_stores = _common.get(workstep_stores, 'sqTableBuilderStore',
                                           default=self._default_table_state,
                                           assign_default=True)
        headers = _common.get(table_builder_stores, 'headers', default=dict(), assign_default=True)
        for mode in ['simple', 'condition']:
            mode_headers = _common.get(headers, mode, default=dict(), assign_default=True)
            mode_headers['format'] = date_format

    def _get_table_mode(self):
        """
        Get the current Table view mode.

        See worksheet property "table_mode" for docs

        :return: str table mode
        """
        workstep_stores = self.get_workstep_stores()
        table_builder_stores = _common.get(workstep_stores, 'sqTableBuilderStore',
                                           default=self._default_table_state,
                                           assign_default=True)
        if 'mode' in table_builder_stores:
            return table_builder_stores['mode']
        else:
            return None

    def _set_table_mode(self, mode):
        """
        Set the Table view mode of the current workstep.

        See worksheet property "table_mode" for docs
        """
        if mode not in ['simple', 'condition']:
            raise SPyValueError(f'The Table view mode "{mode}" is not recognized. Valid values are '
                                f'"simple" and "condition".')

        workstep_stores = self.get_workstep_stores()
        table_builder_stores = _common.get(workstep_stores, 'sqTableBuilderStore',
                                           default=self._default_table_state,
                                           assign_default=True)
        table_builder_stores['mode'] = mode

    @staticmethod
    def _validate_input(df):
        if isinstance(df, list):
            # This is a list of dicts, likely coming via spy.assets.build(). Turn it into a DataFrame.
            new_list = list()
            for d in df:
                if isinstance(d, StoredOrCalculatedItem):
                    new_list.append({'ID': d.id, 'Type': d.type, 'Name': d.name})
                elif isinstance(d, dict):
                    if _common.present(d, 'Item'):
                        d.update(d['Item'])
                        del d['Item']
                    new_list.append(d)
                else:
                    raise SPyValueError(f'Unrecognized type in display_items list: {type(d)}')

            df = pd.DataFrame(new_list)

        if len(df) == 0:
            return df

        _common.validate_unique_dataframe_index(df, 'items_df')

        for col in ['Type', 'Name']:
            if col not in df.columns:
                raise SPyValueError('%s column required in display_items DataFrame' % col)

        if 'ID' not in df.columns:
            # Check to see if this the spy.assets.build case, which references things by Path/Asset/Name
            if 'Path' not in df.columns or 'Asset' not in df.columns:
                raise SPyValueError('ID column required in display_items DataFrame')

        elif any(df.dropna(subset=['ID']).duplicated(['ID'])):
            raise SPyRuntimeError(f'Duplicate IDs detected in display_items DataFrame:\n'
                                  f'{df[df.duplicated(["ID"], keep=False)][["ID", "Name"]]}')

        AnalysisWorkstep.add_display_columns(df, inplace=True)

        return df

    def _set_display_items(self, items_df):
        """
        Set the display items

        See worksheet property "display_items" for docs

        :param items_df:
        :return:
        """
        items_df = AnalysisWorkstep._validate_input(items_df)

        workstep_stores = self.get_workstep_stores()
        workstep_version = self._get_workstep_version()

        # get the axes identifiers and convert them to the canonical "A", "B", "C"...
        axis_map = dict()
        if _common.present(items_df, 'Axis Group'):
            axis_map = AnalysisWorkstep._generate_axis_map(items_df['Axis Group'])

        # clear all items from the workstep
        for store_name in self._store_map_from_type('all', workstep_version):
            store = _common.get(workstep_stores, store_name, default=dict(), assign_default=True)
            store['items'] = []

        lanes = set()
        axes = set()
        axis_limit_keys = ['Axis Max', 'Axis Min']  # these keys map one input value to two outputs
        for _, item in items_df.iterrows():
            store_name = self._store_map_from_type(item['Type'], workstep_version)
            if not store_name:
                continue
            store_items = _common.get(workstep_stores, store_name)['items']
            store_items.append(dict())
            store_items[-1]['name'] = item['Name']
            store_items[-1]['id'] = item['ID'] if 'ID' in item else np.nan
            store_items[-1][Workstep.OriginalDict] = item.dropna().to_dict()
            if _common.present(item, 'Asset'):
                # The Asset column will be present if we're using spy.assets.build()
                workstep_stores.update({
                    'sqTrendStore': {
                        'enabledColumns': {
                            'SERIES': {
                                'asset': True
                            }
                        }
                    }
                })
            for column in item.keys():
                value = _common.get(item, column)
                if isinstance(value, (float, int, str)) and pd.notna(value):
                    if column in self._workstep_display_user_to_workstep and column not in axis_limit_keys:
                        if column == 'Line Style':
                            value = self._workstep_dashStyle_user_to_workstep[value]
                        elif column == 'Samples Display':
                            value = self._workstep_sampleDisplay_user_to_workstep[value]
                        elif column == 'Axis Group':
                            value = axis_map[value]
                            axes.add(value)
                        elif column == 'Lane':
                            value = int(value) if int(value) > 0 else 1
                            lanes.add(value)
                        elif column == 'Line Width':
                            value = value if value > 0 else 1
                        elif column == 'Axis Align':
                            value = self._workstep_rightAxis_user_to_workstep[value]
                        store_items[-1][self._workstep_display_user_to_workstep[column]] = value
                    if column in axis_limit_keys and isinstance(value, (float, int)):
                        current_limits = \
                            [_common.get(store_items[-1], 'yAxisMin'), _common.get(store_items[-1], 'yAxisMax')]
                        which = 'lower' if column == 'Axis Min' else 'upper'
                        value = self._determine_axis_limits(value, which, current_limits)
                        store_items[-1]['yAxisMin'] = value[0]
                        store_items[-1]['yAxisMax'] = value[1]

            if 'yAxisMin' in store_items[-1] or 'yAxisMax' in store_items[-1]:
                store_items[-1]['axisAutoScale'] = False
            if 'lane' not in store_items[-1]:
                if lanes:
                    store_items[-1]['lane'] = max(lanes) + 1
                else:
                    store_items[-1]['lane'] = 1
                lanes.add(store_items[-1]['lane'])
            if 'axisAlign' not in store_items[-1]:
                if axes:
                    max_axis_number = max(list(map(
                        lambda x: AnalysisWorkstep.axes_number_from_identifier(x), list(axes))))
                    store_items[-1]['axisAlign'] = AnalysisWorkstep.axes_identifier_from_number(max_axis_number + 1)
                else:
                    store_items[-1]['axisAlign'] = 'A'
                axes.add(store_items[-1]['axisAlign'])

    def _get_display_items(self, item_type='all'):
        """
        Get the items of a given type displayed in the workstep, regardless of the worksheet view.

        See worksheet property "display_items" for docs

        Parameters
        ----------
        item_type : {'all', 'signals', 'conditions', 'scalars', 'metrics', 'tables'}, default 'all'
            The type of items to return.

        Returns
        -------
        {pandas.DataFrame, None}
            A list of the items present in the workstep or None if there is not workstep.data
        """
        empty_df = pd.DataFrame(dtype=object)

        if not self.data:
            return empty_df

        workstep_version = self._get_workstep_version()
        stores = list()
        if item_type in ['all', 'signals']:
            stores.append('sqTrendSeriesStore')
        if item_type in ['all', 'conditions']:
            if workstep_version >= 62:
                stores.append('sqTrendConditionStore')
            else:
                stores.append('sqTrendCapsuleSetStore')
        if item_type in ['all', 'scalars']:
            stores.append('sqTrendScalarStore')
        if item_type in ['all', 'metrics']:
            stores.append('sqTrendMetricStore')
        if item_type in ['all', 'tables']:
            stores.append('sqTrendTableStore')

        items = list()
        for store in stores:
            workstep_store = self._get_store(store)
            for item in _common.get(workstep_store, 'items', default=list(), assign_default=True):
                output_item = _common.get(item, Workstep.OriginalDict, dict())
                output_item['Name'] = item['name'] if 'name' in item else np.nan
                if 'id' in item:
                    output_item['ID'] = item['id']
                else:
                    # Any workstep that has one or more items with no id is invalid and should not be returned
                    return empty_df
                output_item['Type'] = AnalysisWorkstep._type_from_store_name(store, workstep_version)
                for k in self._workstep_display_workstep_to_user.keys():
                    if k in item:
                        value = item[k]
                        if k == 'dashStyle':
                            output_item[self._workstep_display_workstep_to_user[k]] = \
                                self._workstep_dashStyle_workstep_to_user[value]
                        elif k == 'sampleDisplayOption':
                            output_item[self._workstep_display_workstep_to_user[k]] = \
                                self._workstep_sampleDisplay_workstep_to_user[value]
                        elif k == 'rightAxis':
                            output_item[self._workstep_display_workstep_to_user[k]] = \
                                self._workstep_rightAxis_workstep_to_user[value]
                        else:
                            output_item[self._workstep_display_workstep_to_user[k]] = value

                items.append(output_item)

        items_df = pd.DataFrame({'Name': pd.Series([], dtype=str), 'ID': pd.Series([], dtype=str)})
        items_df = pd.concat([items_df, pd.DataFrame(items)], ignore_index=True).astype(object)
        return items_df

    def _get_scatter_plot_series(self):
        """
        Get the ID of the one of the items plotted on the x or y axis of a scatter plot

        :return: dict or none
            A dict with keys of 'X' and 'Y'. For 'X', the values is a dict with a key of
            'ID' and a value of either the Seeq ID of the item or None if not
            specified. For 'Y', it is an array of the previous listed possible values.
            Returns None if neither is specified
        """
        workstep_stores = self.get_workstep_stores()
        scatterplot_store = _common.get(workstep_stores, 'sqScatterPlotStore', default=dict(), assign_default=True)

        # For each axis and series get the stored value or None if it's not set
        series = dict()
        if 'xSignal' in scatterplot_store and 'id' in scatterplot_store['xSignal'] \
                and scatterplot_store['xSignal']['id']:
            series['X'] = {'ID': scatterplot_store['xSignal']['id']}
        else:
            series['X'] = None
        if 'ySignals' in scatterplot_store and len(scatterplot_store['ySignals']) > 0:
            series['Y'] = [{'ID': y_signal['id']} for y_signal in scatterplot_store['ySignals']]
        else:
            series['Y'] = None

        # If they're all none, return None
        if all([v is None for v in series.values()]):
            series = None
        return series

    def _set_scatter_plot_series(self, series_id):
        """
        Set the ID of one of the items to use the x or y axis of a scatter plot

        :param series_id: dict
            A dict with keys of the axis name (either 'X' or 'Y') and values of
            dicts, series, or one row DataFrame with the Seeq ID of the item to
            use for the axis. Additionally, for y axis, an array of any of the
            prior three is supported
        """

        def _check_series_df(df):
            if len(df) > 1:
                raise SPyValueError(f'DataFrames used to set the scatter plot series are limited to one row. '
                                    f'Got {df}')

        if len(series_id.keys()) > 2 or any([a not in ['X', 'Y'] for a in series_id.keys()]):
            raise SPyValueError(f'Series name {series_id} not recognized when setting the scatter plot axes. '
                                f'Valid values are {["X", "Y"]}')
        workstep_stores = self.get_workstep_stores()
        scatterplot_store = _common.get(workstep_stores, 'sqScatterPlotStore', default=dict(), assign_default=True)

        for axis, item in series_id.items():
            if isinstance(item, pd.DataFrame):
                _check_series_df(item)
                series_item = item.squeeze(axis='index').to_dict()
            elif isinstance(item, pd.Series):
                series_item = item.to_dict()
            else:
                series_item = item
            if axis == 'X':
                scatterplot_store['xSignal'] = {'id': series_item['ID']}
            elif axis == 'Y':
                if isinstance(series_item, list):
                    scatterplot_store['ySignals'] = [{'id': y_signal['ID']} for y_signal in series_item]
                else:
                    scatterplot_store['ySignals'] = [{'id': series_item['ID']}]

    def _validate_before_push(self):
        df = self.display_items
        if len(df) == 0:
            return

        if 'ID' not in df.columns:
            raise SPyValueError(f'No ID column in display_items DataFrame:\n{df}')

        if df['ID'].isna().values.any():
            raise SPyValueError(f'Missing (NaN) IDs detected in display_items DataFrame:\n{df[["ID", "Name"]]}')

        if any(df.duplicated(['ID'])):
            self.display_items = df.drop_duplicates(subset='ID')

    @staticmethod
    def _store_map_from_type(item_type, workstep_version):
        """
        Return a list with the name of the workstep store corresponding to the item type and workstep version given.
        :param item_type: The string type of the item. If 'all' all stores will be returned
        :param workstep_version: The number version of the workstep.
        :return: str store name, for all item_types except 'all'. If item_type == 'all', a tuple of all the store
        names. If item_type is not recognized, returns None.
        """
        if not item_type or not isinstance(item_type, str):
            return None

        if 'Signal' in item_type:
            return 'sqTrendSeriesStore'
        if 'Condition' in item_type:
            if workstep_version >= 62:
                return 'sqTrendConditionStore'
            else:
                return 'sqTrendCapsuleSetStore'
        if 'Scalar' in item_type:
            return 'sqTrendScalarStore'
        if 'Metric' in item_type:
            return 'sqTrendMetricStore'
        if 'Table' in item_type:
            return 'sqTrendTableStore'
        if item_type == 'all':
            stores = ['sqTrendSeriesStore', 'sqTrendCapsuleSetStore', 'sqTrendScalarStore', 'sqTrendMetricStore',
                      'sqTrendTableStore']
            if workstep_version >= 62:
                stores[1] = 'sqTrendConditionStore'

            return tuple(stores)
        else:
            return None

    @staticmethod
    def _type_from_store_name(store_name, workstep_version):
        """
        Return the type of an item that is stored in a given data store
        Parameters
        ----------
        store_name : str
            The name of the data store
        workstep_version : int
            The version of the workstep

        Returns
        -------
        str
            The item type
        """
        if not store_name or not isinstance(store_name, str):
            return None

        store_map = {
            'sqTrendSeriesStore': 'Signal',
            'sqTrendScalarStore': 'Scalar',
            'sqTrendMetricStore': 'Metric',
            'sqTrendTableStore': 'Table'
        }
        if workstep_version >= 62:
            store_map['sqTrendConditionStore'] = 'Condition'
        else:
            store_map['sqTrendCapsuleSetStore'] = 'Condition'

        if store_name not in store_map:
            return None

        return store_map[store_name]

    _workstep_display_user_to_workstep = {
        'Color': 'color',
        'Line Style': 'dashStyle',
        'Line Width': 'lineWidth',
        'Lane': 'lane',
        'Samples Display': 'sampleDisplayOption',
        'Axis Auto Scale': 'axisAutoScale',
        'Axis Align': 'rightAxis',
        'Axis Group': 'axisAlign',
        'Axis Show': 'axisVisibility',
        'Axis Max': 'yAxisMax',
        'Axis Min': 'yAxisMin',
        'Stack': 'stack',
        'Selected': 'selected'
    }

    _workstep_display_workstep_to_user = dict((v, k) for k, v in _workstep_display_user_to_workstep.items())

    _workstep_sampleDisplay_user_to_workstep = {
        'Line': 'line',
        'Line and Sample': 'lineAndSample',
        'Samples': 'sample',
        'Bars': 'bar'
    }

    _workstep_sampleDisplay_workstep_to_user = dict((v, k) for k, v in _workstep_sampleDisplay_user_to_workstep.items())

    _workstep_dashStyle_user_to_workstep = {
        'Solid': 'Solid',
        'Short Dash': 'ShortDash',
        'Short Dash-Dot': 'ShortDashDot',
        'Short Dash-Dot-Dot': 'ShortDashDotDot',
        'Dot': 'Dot',
        'Dash': 'Dash',
        'Long Dash': 'LongDash',
        'Dash-Dot': 'DashDot',
        'Long Dash-Dot': 'LongDashDot',
        'Long Dash-Dot-Dot': 'LongDashDotDot'
    }

    _workstep_dashStyle_workstep_to_user = dict((v, k) for k, v in _workstep_dashStyle_user_to_workstep.items())

    _workstep_rightAxis_user_to_workstep = {
        'Right': True,
        'Left': False
    }

    _workstep_rightAxis_workstep_to_user = dict((v, k) for k, v in _workstep_rightAxis_user_to_workstep.items())

    @staticmethod
    def _determine_axis_limits(value: Union[int, float], which: str, current_limits: list) -> list:
        """
        Return a list containing the upper and lower limits for the y-axis. Since both are required for the frontend
        to honor either one, they must both be set, even if only one is specified. If the opposing limit in
        current_limits is None, or is invalid for the new limit (eg, a lower limit > upper limit) a guess will be made
        at a value for the opposing limit.

        Parameters
        ----------
        value : {number.Real}
            A numerical value for the limit
        which : str
            A string of 'upper' or 'lower' specifying which limit
        current_limits : list
            a list with the current limits as [lower, upper] as either numbers or None

        Returns
        -------
        list
            A list with the new [lower, upper] limits as numbers
        """

        if which == 'upper':
            if current_limits[0] is None or current_limits[0] > value:
                if value > 0:
                    return [-value, value]
                elif value < 0:
                    return [2 * value, value]
                else:
                    return [-1, value]
            else:
                return [current_limits[0], value]
        elif which == 'lower':
            if current_limits[1] is None or current_limits[1] < value:
                if value < 0:
                    return [value, -value]
                elif value > 0:
                    return [value, 2 * value]
                else:
                    return [value, 1]
            else:
                return [value, current_limits[1]]
        else:
            raise SPyValueError(f'Limit specification "{which}" is not a valid selection to specify axis limits')

    @staticmethod
    def _generate_axis_map(axis_group):
        specified_axes = axis_group.dropna().drop_duplicates().to_list()
        canonical_axes = list(AnalysisWorkstep.axes_identifier_list_from_number(len(specified_axes)))
        axis_map = dict()
        for ax in specified_axes:
            if ax in canonical_axes:
                axis_map[ax] = ax
                canonical_axes.remove(ax)
                continue
            else:
                axis_map[ax] = canonical_axes[0]
                del canonical_axes[0]
        return axis_map

    @staticmethod
    def axes_identifier_list_from_number(n):
        """
        A Generator that produces a canonical list of axes identifiers of the
        form "A", "B", "C",..., "AA", "AB", "AC",...
        Parameters
        ----------
        n : integer
            The number of identifiers required.

        Returns
        -------
        str
            The string for the current axis identifier.
        """
        generated = 0

        while generated < n:
            yield AnalysisWorkstep.axes_identifier_from_number(generated)
            generated += 1

    @staticmethod
    def axes_identifier_from_number(number):
        decimal_a, number_letters = 65, 26

        if number >= number_letters:
            return AnalysisWorkstep.axes_identifier_from_number(number // number_letters - 1) + chr(
                number % number_letters + decimal_a)
        else:
            return chr(number % number_letters + decimal_a)

    @staticmethod
    def axes_number_from_identifier(axis_id):
        """
        Converts from an alpha axis identifier, eg "A", "B",...,"AA", "AB",...
        to a decimal, essentially doing a conversion from base 26 to base 10.

        Parameters
        ----------
        axis_id: str
            The alpha identifier

        Returns
        -------
        int
            Integer base 10 equivalent
        """
        decimal_a, number_letters = 65, 26
        base_26_multipliers = [ord(i) + 1 - decimal_a for i in axis_id]
        base_10_components = \
            [b26 * number_letters ** (len(base_26_multipliers) - 1 - i) for i, b26 in enumerate(base_26_multipliers)]
        return sum(base_10_components)

    @staticmethod
    def add_display_columns(df, inplace):
        """
        See documentation for Worksheet.add_display_columns
        """
        working_df = df.copy(deep=True) if not inplace else df

        for attribute in AnalysisWorkstep._workstep_display_user_to_workstep.keys():
            if attribute not in working_df:
                working_df.insert(working_df.shape[1], attribute, np.nan)

        return None if inplace else working_df

    def _get_view_key(self):
        """
        Get the view of the current workstep. If no view key is found the default value is returned

        See worksheet property "view" for docs

        :return: str view key
        """
        workstep_stores = self.get_workstep_stores()
        worksheet_store = _common.get(workstep_stores, 'sqWorksheetStore', default=dict(), assign_default=True)
        if 'viewKey' in worksheet_store:
            # Add-ons will not have an entry in _view_key_workstep_to_user so just return the raw viewKey value.
            # Example: Parallel Coordinates is 'com.seeq.plugin.parallel-coordinates'
            return self._view_key_workstep_to_user.get(worksheet_store['viewKey'], worksheet_store['viewKey'])
        else:
            return None

    def _set_view_key(self, view='Trend'):
        """
        Set the view of the current workstep.

        See worksheet property "view" for docs
        """
        if view not in self._view_key_user_to_workstep.keys():
            raise SPyValueError(f'The view key "{view}" is not recognized. Valid values are '
                                f'{self._view_key_user_to_workstep.keys()}')

        workstep_stores = self.get_workstep_stores()
        worksheet_store = _common.get(workstep_stores, 'sqWorksheetStore', default=dict(), assign_default=True)
        worksheet_store['viewKey'] = self._view_key_user_to_workstep[view]

    _view_key_user_to_workstep = {
        'Table': 'TABLE',
        'Scorecard': 'TABLE',
        'Treemap': 'TREEMAP',
        'Scatter Plot': 'SCATTER_PLOT',
        'Trend': 'TREND',
        'Asset Group Editor': 'ASSET_GROUP_EDITOR'
    }
    _view_key_workstep_to_user = {
        k.upper().replace(' ', '_'): v.title().replace('_', ' ')
        for k, v in _view_key_user_to_workstep.items()
    }

    _default_table_state = {
        'headers': {
            'condition': {
                'type': 'startEnd',
                'format': 'lll'
            },
            'simple': {
                'type': 'startEnd',
                'format': 'lll'
            }
        },
        'columns': {
            'condition': [],
            'simple': []
        }
    }
