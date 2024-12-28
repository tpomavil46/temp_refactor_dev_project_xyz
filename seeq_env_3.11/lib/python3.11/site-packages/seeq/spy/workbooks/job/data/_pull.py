from __future__ import annotations

import copy
import json
import os
import sys
import textwrap
from datetime import datetime, timedelta
from typing import Optional, Union

import pandas as pd

from seeq import spy
from seeq.base import util
from seeq.spy import _common
from seeq.spy import _login
from seeq.spy._errors import *
from seeq.spy._session import Session
from seeq.spy._status import Status
from seeq.spy.workbooks.job.data._push import load_data_results, save_data_results


@Status.handle_keyboard_interrupt()
def pull(job_folder, *, resume: bool = True, errors: Optional[str] = None, quiet: Optional[bool] = None,
         status: Optional[Status] = None, session: Optional[Session] = None) -> pd.DataFrame:
    """
    Pulls all the data that is used by the workbooks according to the Data
    Usages sections of the data_usage.json file in the job folder.

    Parameters
    ----------
    job_folder : {str}
        A full or partial path to the job folder created by
        spy.workbooks.job.pull().

    resume : bool, default True
        True if the pull should resume from where it left off, False if it
        should pull everything again.

    errors : {'raise', 'catalog'}, default 'raise'
        If 'raise', any errors encountered will cause an exception. If
        'catalog', errors will be added to a 'Result' column in the status.df
        DataFrame (errors='catalog' must be combined with
        status=<Status object>).

    quiet : bool
        If True, suppresses progress output. Note that when status is
        provided, the quiet setting of the Status object that is passed
        in takes precedence.

    status : spy.Status, optional
        If specified, the supplied Status object will be updated as the command
        progresses. It gets filled in with the same information you would see
        in Jupyter in the blue/green/red table below your code while the
        command is executed. The table itself is accessible as a DataFrame via
        the status.df property.

    session : spy.Session, optional
        If supplied, the Session object (and its Options) will be used to
        store the login session state. This is useful to log in to different
        Seeq servers at the same time or with different credentials.

    """
    _common.validate_argument_types([
        (job_folder, 'job_folder', str),
        (resume, 'resume', bool),
        (errors, 'errors', str),
        (quiet, 'quiet', bool),
        (status, 'status', Status),
        (session, 'session', Session)
    ])

    session = Session.validate(session)
    status = Status.validate(status, session, quiet, errors)
    _login.validate_login(session, status)

    all_usages = load_data_usage(job_folder)

    _set_up_start_end_and_calculation(all_usages)

    def _keep(_item_id, _usage):
        if not _common.present(_usage['Definition'], 'Start'):
            return False
        if not resume:
            return True
        if not util.safe_exists(get_df_filename(job_folder, _item_id)):
            return True
        return False

    pull_df = pd.DataFrame([usage['Definition'] for item_id, usage in all_usages.items() if _keep(item_id, usage)])

    data_results = load_data_results(job_folder, 'pull')
    data_results.loc[data_results['Result'] == 'Success', 'Result'] = 'Success: Already pulled'
    data_results = data_results[data_results['ID'].isin(all_usages.keys())]

    def _consumer(_row, _df):
        _df_filename = get_df_filename(job_folder, _row['ID'])
        util.safe_makedirs(os.path.dirname(_df_filename), exist_ok=True)
        _df.to_pickle(_df_filename, protocol=4)

    spy.pull(pull_df, grid=None, shape=_consumer, header='ID', status=status, session=session)

    status_df = status.df.copy()
    status_df.set_index('ID', drop=False, inplace=True)

    data_results.update(status_df, overwrite=True)
    additional_results = status_df[~status_df.index.isin(data_results.index)]
    if len(data_results) > 0 and len(additional_results) > 0:
        data_results = pd.concat([data_results, additional_results])
    elif len(data_results) == 0:
        data_results = additional_results
    save_data_results(job_folder, data_results, 'pull')

    return data_results


def get_df_filename(job_folder: str, item_id: str):
    return os.path.join(job_folder, 'Data', f'{item_id}.pickle')


def get_data_usage_filename(job_folder):
    return os.path.join(job_folder, 'data_usage.json')


def load_data_usage(job_folder):
    data_usage_filename = get_data_usage_filename(job_folder)
    with util.safe_open(data_usage_filename, 'r') as f:
        loaded = json.load(f)

    for usage_dict in loaded.values():
        for period in usage_dict['Periods']:
            period['Start'] = pd.to_datetime(period['Start'])
            period['End'] = pd.to_datetime(period['End'])

    return loaded


def save_data_usage(job_folder, data_usage_dict):
    data_usage_filename = get_data_usage_filename(job_folder)
    with util.safe_open(data_usage_filename, 'w') as f:
        return f.write(_common.safe_json_dumps(data_usage_dict))


def _set_up_start_end_and_calculation(all_usages):
    for item_id, usage_dict in all_usages.items():
        item_dict = usage_dict['Definition']
        periods = usage_dict['Periods']

        def _compare(_ts1, _ts2):
            if pd.to_datetime(_ts1) > pd.to_datetime(_ts2):
                return 1
            elif pd.to_datetime(_ts1) < pd.to_datetime(_ts2):
                return -1
            else:
                return 0

        capsules = list()
        periods = copy.deepcopy(periods)
        to_add = [period for period in periods if not period.get('Remove', False)]
        to_remove = [period for period in periods if period.get('Remove', False)]
        final_periods = list()
        for period in to_add:
            periods_to_add = [period]
            for remove_period in to_remove:
                remove_start = (remove_period['Start']
                                if remove_period['Start'] is not None else pd.Timestamp(0, tz='utc'))
                remove_end = (remove_period['End']
                              if remove_period['End'] is not None else pd.Timestamp(sys.maxsize, tz='utc'))
                for period_to_add in periods_to_add.copy():
                    if _compare(remove_start, period_to_add['End']) > 0:
                        # Period to remove is completely after subject period
                        continue
                    if _compare(remove_end, period_to_add['Start']) < 0:
                        # Period to remove is completely before subject period
                        continue

                    if (_compare(remove_start, period_to_add['Start']) > 0 >
                            _compare(remove_end, period_to_add['End'])):
                        # Period to remove bisects subject period to create two separate periods
                        periods_to_add.append({'Start': remove_end, 'End': period_to_add['End']})
                        period_to_add['End'] = remove_start
                    elif (_compare(remove_start, period_to_add['Start']) >= 0 and
                          _compare(remove_end, period_to_add['End']) >= 0):
                        # Period to remove overlaps on the right
                        period_to_add['End'] = remove_start
                    elif (_compare(remove_start, period_to_add['Start']) <= 0 and
                          _compare(remove_end, period_to_add['End']) <= 0):
                        # Period to remove overlaps on the left
                        period_to_add['Start'] = remove_end
                    elif (_compare(remove_start, period_to_add['Start']) < 0 <
                          _compare(remove_end, period_to_add['End'])):
                        # Period to remove encapsulates subject period entirely
                        periods_to_add.remove(period_to_add)

            final_periods.extend(periods_to_add)

        for period in final_periods:
            if _compare(item_dict.setdefault('Start', period['Start']), period['Start']) > 0:
                item_dict['Start'] = period['Start']
            if _compare(item_dict.setdefault('End', period['End']), period['End']) < 0:
                item_dict['End'] = period['End']

            capsules.append(f'capsule("{period["Start"].isoformat()}", "{period["End"].isoformat()}")')

        capsules_str = ',\n                   '.join(capsules)
        if 'Signal' in item_dict['Type']:
            formula = usage_dict.get('Calculation', '$signal')
            formula = formula.replace('$signal', '$final')

            item_dict['Calculation'] = textwrap.dedent(f"""
                $within = condition(
                   {capsules_str}
                )

                $final = $signal.within($within)

                {formula}
            """).strip()
        elif 'Condition' in item_dict['Type']:
            formula = usage_dict.get('Calculation', '$condition')
            formula = formula.replace('$condition', '$final')
            max_duration = item_dict.get('Maximum Duration', '40h')
            item_dict['Calculation'] = textwrap.dedent(f"""
                $touches = condition(
                  {capsules_str}
                )

                $final = $condition.removeLongerThan({max_duration}).touches($touches)

                {formula}
            """).strip()


def manifest(job_folder, *, reset=False):
    """
    Generates and returns a DataFrame with the list of items and data to be
    pulled. The manifest is initially generated by spy.workbooks.job.pull(),
    and is constructed by examining all of the Analyses and Topics that "touch"
    a signal or condition and noting the display ranges at play.

    You can modify the manifest using its sibling functions: expand(), add()
    or remove().

    The DataFrame returned is in a format suitable for spy.pull(), but in
    general it is expected that you will just use
    spy.workbooks.job.data.pull(), which has all the resume-ability of the
    spy.workbooks.job family of functions.

    Parameters
    ----------
    job_folder : {str}
        A full or partial path to the job folder created by
        spy.workbooks.job.pull().

    reset: {bool}, default False
        If True, the manifest will be reset to the original state (i.e. all
        modifications made by expand(), add(), and remove() will be
        undone).

    Returns
    -------
    pandas.DataFrame
        A DataFrame with the list of items that would be pulled by
        spy.workbooks.data.pull(). Note the presence of "Start", "End" and
        "Calculation" columns:

        Start        The earliest timestamp that will be requested for the item
        End          The latest timestamp that will be requested for the item
        Calculation  A within() or touches() calculation that will be used to
                     more precisely request the non-contiguous time periods
                     represented by the manifest.
    """
    if reset:
        _reset_manifest(job_folder, {'ID': '*'})

    all_usages = load_data_usage(job_folder)
    _set_up_start_end_and_calculation(all_usages)
    manifest_df = pd.DataFrame([row['Definition'] for item_id, row in all_usages.items()])
    manifest_df.dropna(subset=['Start'], inplace=True)
    return manifest_df


def _query(items, all_usages, job_folder):
    query_list = list()
    if not isinstance(items, list):
        query_list = [items]

    for query in query_list:
        if query is None:
            for usage_dict in all_usages.values():
                yield usage_dict
        elif isinstance(query, str):
            if query not in all_usages:
                raise SPyValueError(
                    f'Item ID {query} not found in data manifest ("{get_data_usage_filename(job_folder)}")')
            yield all_usages[query]
        elif isinstance(query, dict):
            for item_id, usage_dict in all_usages.items():
                if _common.does_definition_match_criteria(query, usage_dict['Definition']):
                    yield usage_dict
        else:
            raise SPyTypeError(f'Invalid entry in items argument: type must be str or dict, but was '
                               f'{type(query.__class__.__name__)}')


def expand(job_folder: str, items: Union[str, list, dict] = None, *, by: Union[str, timedelta, pd.Timedelta] = None,
           start_by: Union[str, timedelta, pd.Timedelta] = None, end_by: Union[str, timedelta, pd.Timedelta] = None):
    """
    Expands the start and end of the time periods to be pulled (by
    spy.workbooks.job.data.pull) by the specified amount.

    Parameters
    ----------
    job_folder : {str}
        A full or partial path to the job folder created by
        spy.workbooks.job.pull().

    items : {str, list, dict}, optional
        The items to affect in the data manifest.

        If not specified, all items are affected.

        If a string, it is interpreted as an item ID.

        If a dict, it is interpreted as a query to match
        against the properties of each item. The keys of the dict are
        property names and the values are the matching criteria, and
        can include wildcards (*, ?) and regular expressions (aka Regex).
        Regex expressions must be enclosed in forward slashes (/).

        If a list, it is interpreted as a list of item IDs and/or
        dictionaries.

    by : {str, timedelta, pd.Timedelta}, optional
        A str, timedelta or pd.Timedelta object specifying the amount of time
        to expand both the start and end times of the periods to be pulled.

    start_by : {str, timedelta, pd.Timedelta}, optional
        A str, timedelta or pd.Timedelta object specifying the amount of time
        to expand the start times of the periods to be pulled.

    end_by : {str, timedelta, pd.Timedelta}, optional
        A str, timedelta or pd.Timedelta object specifying the amount of time
        to expand the start times of the periods to be pulled.
    """
    _common.validate_argument_types([
        (job_folder, 'job_folder', str),
        (items, 'items', (str, dict, list)),
        (by, 'by', (str, timedelta, pd.Timedelta)),
        (start_by, 'start_by', (str, timedelta, pd.Timedelta)),
        (end_by, 'end_by', (str, timedelta, pd.Timedelta))
    ])

    all_usages = load_data_usage(job_folder)
    affected_item_ids = set()

    if by is not None:
        if start_by is not None or end_by is not None:
            raise SPyValueError('Cannot specify both by and start_by/end_by')
        start_by = by
        end_by = by

    if isinstance(start_by, str):
        start_by = _common.parse_str_time_to_timedelta(start_by)
    if isinstance(end_by, str):
        end_by = _common.parse_str_time_to_timedelta(end_by)

    if ((start_by is not None and start_by < pd.Timedelta(0)) or
            (end_by is not None and end_by < pd.Timedelta(0))):
        raise SPyValueError('timedeltas must be positive')

    for item in _query(items, all_usages, job_folder):
        new_periods = list()
        for period in item['Periods']:
            new_period = {'Start': period['Start'], 'End': period['End'], 'Added by User': True}
            if start_by is not None:
                new_period['Start'] = pd.to_datetime(period['Start']) - start_by
            if end_by is not None:
                new_period['End'] = pd.to_datetime(period['End']) + end_by
            new_periods.append(new_period)
        item['Periods'].extend(new_periods)
        affected_item_ids.add(item['Definition']['ID'])

    save_data_usage(job_folder, all_usages)
    return _affected_items_df(all_usages, affected_item_ids)


def add(job_folder: str, items: Union[str, list, dict] = None, *,
        start: Union[str, pd.Timestamp], end: Union[str, pd.Timestamp] = None):
    """
    Adds a time period (start and end) to the list of the time periods to be
    pulled (by spy.workbooks.job.data.pull) for a particular item or set of
    items.

    Parameters
    ----------
    job_folder : {str}
        A full or partial path to the job folder created by
        spy.workbooks.job.pull().

    items : {str, list, dict}, optional
        The items to affect in the data manifest.

        If not specified, all items are affected.

        If a string, it is interpreted as an item ID.

        If a dict, it is interpreted as a query to match
        against the properties of each item. The keys of the dict are
        property names and the values are the matching criteria, and
        can include wildcards (*, ?) and regular expressions (aka Regex).
        Regex expressions must be enclosed in forward slashes (/).

        If a list, it is interpreted as a list of item IDs and/or
        dictionaries.

    start : {str, pd.Timestamp}
        The starting time for which to pull data. This argument must be a
        string that pandas.to_datetime() can parse, or a pandas.Timestamp.

    end : {str, pd.Timestamp}, optional
        The end time for which to pull data. This argument must be a string
        that pandas.to_datetime() can parse, or a pandas.Timestamp.
        If not provided, 'end' will default to now.
    """
    _common.validate_argument_types([
        (job_folder, 'job_folder', str),
        (items, 'items', (str, dict, list)),
        (start, 'start', (str, datetime, pd.Timestamp)),
        (end, 'end', (str, datetime, pd.Timestamp)),
    ])

    all_usages = load_data_usage(job_folder)
    affected_item_ids = set()

    if start is None:
        raise SPyValueError('start argument must be specified')

    start = pd.to_datetime(start)

    if end is not None:
        end = pd.to_datetime(end)
    else:
        end = pd.Timestamp.utcnow()

    for item in _query(items, all_usages, job_folder):
        item['Periods'].append({'Start': start, 'End': end, 'Added by User': True})
        affected_item_ids.add(item['Definition']['ID'])

    save_data_usage(job_folder, all_usages)
    return _affected_items_df(all_usages, affected_item_ids)


def remove(job_folder: str, items: Union[str, list, dict] = None, *,
           start: Union[str, pd.Timestamp] = None, end: Union[str, pd.Timestamp] = None):
    """
    Removes a time period (start and end) to the set of the time periods to be
    pulled (by spy.workbooks.job.data.pull) for a particular item or set of
    items.

    Parameters
    ----------
    job_folder : {str}
        A full or partial path to the job folder created by
        spy.workbooks.job.pull().

    items : {str, list, dict}, optional
        The items to affect in the data manifest.

        If not specified, all items are affected.

        If a string, it is interpreted as an item ID.

        If a dict, it is interpreted as a query to match
        against the properties of each item. The keys of the dict are
        property names and the values are the matching criteria, and
        can include wildcards (*, ?) and regular expressions (aka Regex).
        Regex expressions must be enclosed in forward slashes (/).

        If a list, it is interpreted as a list of item IDs and/or
        dictionaries.

    start : {str, pd.Timestamp}, optional
        The starting time for the time period to remove. This argument must
        be a string that pandas.to_datetime() can parse, or a pandas.Timestamp.
        If not provided, 'start' will default to "the beginning of time,"
        meaning that all time prior to 'end' will be removed. (If neither
        'start' nor 'end' are specified, no data for the item will be pulled.)

    end : {str, pd.Timestamp}, optional
        The end time for the time period to remove. This argument must be a
        string that pandas.to_datetime() can parse, or a pandas.Timestamp.
        If not provided, 'end' will default to "the end of time," meaning that
        all time after 'start' will be removed. (If neither 'start' nor 'end'
        are specified, no data for the item will be pulled.)

    session : spy.Session, optional
        If supplied, the Session object (and its Options) will be used to
        store the login session state. This is useful to log in to different
        Seeq servers at the same time or with different credentials.
    """
    _common.validate_argument_types([
        (job_folder, 'job_folder', str),
        (items, 'items', (str, dict, list)),
        (start, 'start', (str, datetime, pd.Timestamp)),
        (end, 'end', (str, datetime, pd.Timestamp))
    ])

    all_usages = load_data_usage(job_folder)
    affected_item_ids = set()

    if start is not None:
        start = pd.to_datetime(start)

    if end is not None:
        end = pd.to_datetime(end)

    for item in _query(items, all_usages, job_folder):
        item['Periods'].append({'Start': start, 'End': end, 'Added by User': True, 'Remove': True})
        affected_item_ids.add(item['Definition']['ID'])

    save_data_usage(job_folder, all_usages)
    return _affected_items_df(all_usages, affected_item_ids)


def calculation(job_folder: str, items: Union[str, list, dict] = None, *, formula: str = None):
    """
    Apply a specific formula to the items when pulling. For example,
    you may wish to specify formula="resample($signal, 1h)" to
    reduce the density of a signal. This calculation will be applied
    for all time periods that are pulled for the item.

    Parameters
    ----------
    job_folder : {str}
        A full or partial path to the job folder created by
        spy.workbooks.job.pull().

    items : {str, list, dict}, optional
        The items to affect in the data manifest.

        If not specified, all items are affected.

        If a string, it is interpreted as an item ID.

        If a dict, it is interpreted as a query to match
        against the properties of each item. The keys of the dict are
        property names and the values are the matching criteria, and
        can include wildcards (*, ?) and regular expressions (aka Regex).
        Regex expressions must be enclosed in forward slashes (/).

        If a list, it is interpreted as a list of item IDs and/or
        dictionaries.

    formula : {str}
        The calculation to apply to the set of items. For signals, the
        formula must contain a reference to $signal, for conditions it
        must contain a reference to $condition.
    """
    _common.validate_argument_types([
        (job_folder, 'job_folder', str),
        (items, 'items', (str, dict, list)),
        (formula, 'formula', str)
    ])

    all_usages = load_data_usage(job_folder)
    affected_item_ids = set()

    if formula is None:
        raise SPyValueError('formula argument must be specified')

    for item in _query(items, all_usages, job_folder):
        for item_type in ['Signal', 'Condition']:
            if item_type in item['Definition']['Type'] and f'${item_type.lower()}' not in formula:
                raise SPyValueError(f'formula must contain a reference to f{item_type.lower()}')

        if '$final' in formula:
            raise SPyValueError('formula cannot contain $final')

        item['Calculation'] = formula
        affected_item_ids.add(item['Definition']['ID'])

    save_data_usage(job_folder, all_usages)
    return _affected_items_df(all_usages, affected_item_ids)


def _reset_manifest(job_folder: str, items: Union[str, list, dict] = None):
    """
    Resets any modifications you have made to the data manifest by
    the expand/add/remove functions.

    Parameters
    ----------
    job_folder : {str}
        A full or partial path to the job folder created by
        spy.workbooks.job.pull().

    items : {str, list, dict}, optional
        The items to affect in the data manifest.

        If not specified, all items are affected.

        If a string, it is interpreted as an item ID.

        If a dict, it is interpreted as a query to match
        against the properties of each item. The keys of the dict are
        property names and the values are the matching criteria, and
        can include wildcards (*, ?) and regular expressions (aka Regex).
        Regex expressions must be enclosed in forward slashes (/).

        If a list, it is interpreted as a list of item IDs and/or
        dictionaries.
    """
    _common.validate_argument_types([
        (job_folder, 'job_folder', str),
        (items, 'items', (str, dict, list))
    ])

    all_usages = load_data_usage(job_folder)
    affected_item_ids = set()

    for item in _query(items, all_usages, job_folder):
        new_periods = list()
        affected = False
        for period in item['Periods']:
            if period.get('Added by User', False):
                affected = True
            else:
                new_periods.append(period)

        if affected:
            item['Periods'] = new_periods
            affected_item_ids.add(item['Definition']['ID'])

        if 'Calculation' in item:
            del item['Calculation']
            affected_item_ids.add(item['Definition']['ID'])

    save_data_usage(job_folder, all_usages)
    return _affected_items_df(all_usages, affected_item_ids)


def _affected_items_df(all_usages, affected_item_ids: set):
    _set_up_start_end_and_calculation(all_usages)
    return pd.DataFrame([row['Definition'] for item_id, row in all_usages.items()
                         if item_id in affected_item_ids])


def redo(job_folder: str, status: Status):
    item_ids: pd.Series = status.df['ID']
    for index, item_id in item_ids.items():
        pickle_file = os.path.join(job_folder, 'Data', f'{item_id}.pickle')
        if util.safe_exists(pickle_file):
            util.safe_remove(pickle_file)
            status.df.at[index, 'Result'] = 'Data pull will be redone'
        else:
            status.df.at[index, 'Result'] = 'Not found'
