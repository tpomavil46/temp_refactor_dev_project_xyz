from __future__ import annotations

import os
from typing import Optional

import pandas as pd

from seeq import spy
from seeq.base import util
from seeq.spy import _common
from seeq.spy import _login
from seeq.spy._errors import *
from seeq.spy._session import Session
from seeq.spy._status import Status


@Status.handle_keyboard_interrupt()
def push(job_folder, *, resume: bool = True, replace: Optional[dict] = None,
         datasource: str = None, errors: Optional[str] = None, quiet: Optional[bool] = None,
         status: Optional[Status] = None, session: Optional[Session] = None) -> pd.DataFrame:
    """
    Pulls all the data that is used by the workbooks according to the Data
    Usages sections of the data_usage.json file in the job folder.

    Parameters
    ----------
    job_folder : {str}
        A full or partial path to the job folder created by
        spy.workbooks.job.pull() and populated with data by
        spy.workbooks.job.pull_data().

    resume : bool, default True
        True if the pull should resume from where it left off, False if it
        should pull everything again.

    replace : dict, default None
        A dict with the keys 'Start' and 'End'. If provided, any existing samples
        or capsules with the start date in the provided time period will be
        replaced. The start of the time period is inclusive and the end of the
        time period is exclusive. If replace is provided but data is not
        specified, all samples/capsules within the provided time period will be
        removed.

    datasource : str, optional, default 'Seeq Data Lab'
        The name of the datasource within which to contain all the pushed items.
        Items inherit access control permissions from their datasource unless it
        has been overridden at a lower level. If you specify a datasource using
        this argument, you can later manage access control (using spy.acl functions)
        at the datasource level for all the items you have pushed.

        If you instead want access control for your items to be inherited from the
        workbook they are scoped to, specify `spy.INHERIT_FROM_WORKBOOK`.

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
        (replace, 'replace', dict),
        (datasource, 'datasource', str),
        (errors, 'errors', str),
        (quiet, 'quiet', bool),
        (status, 'status', Status),
        (session, 'session', Session)
    ])

    session = Session.validate(session)
    status = Status.validate(status, session, quiet, errors)
    _login.validate_login(session, status)

    data_results = load_data_results(job_folder, 'push')
    item_map = spy.workbooks.job._push.load_item_map(job_folder)
    all_usages = spy.workbooks.job.data._pull.load_data_usage(job_folder)

    data_results.loc[data_results['Result'] == 'Success', 'Result'] = 'Success: Already pushed'

    to_push = list()
    all_ids = set()
    for data_usage in all_usages.values():
        item_dict = data_usage['Definition']
        all_ids.add(item_dict['ID'])
        if (resume and item_dict['ID'] in data_results.index
                and data_results.at[item_dict['ID'], 'Result'].startswith('Success')):
            continue

        if item_dict['ID'] not in item_map:
            raise SPyRuntimeError(f'Item {item_dict["ID"]} not found in item map, you may need to run '
                                  f'spy.workbooks.job.push() first or do the whole job over again.')

        dummy_row = item_map.dummy_items[item_map.dummy_items['ID'] == item_map[item_dict['ID']]]

        if len(dummy_row) == 0:
            continue

        dummy_dict = dummy_row.iloc[0].to_dict()
        to_push.append(dummy_dict)

    metadata = pd.DataFrame(to_push)

    if len(metadata) > 0:
        metadata.set_index('Original ID', drop=False, inplace=True)

    if 'ID' in metadata.columns:
        # Remove the ID column so that metadata is pushed via batch endpoints. (cleanse_data_ids=False is
        # used below to eliminate any tampering with Data IDs.)
        metadata.drop(columns=['ID'], inplace=True)

    def _load_df(_index):
        _pickle_filename = spy.workbooks.job.data._pull.get_df_filename(job_folder, _index)
        if not util.safe_exists(_pickle_filename):
            return None

        return pd.read_pickle(_pickle_filename)

    def _on_update(_index):
        data_results.update(status.df, overwrite=True)
        save_data_results(job_folder, data_results, 'push')

    status.on_update = _on_update
    spy.push(_load_df, metadata=metadata, workbook=spy.workbooks.job._push.get_dummy_workbook_name(job_folder),
             replace=replace, cleanse_data_ids=False, datasource=datasource, status=status, session=session)

    data_results = data_results[data_results.index.isin(all_ids)]
    data_results.update(status.df, overwrite=True)

    additional_results = status.df[~status.df.index.isin(data_results.index)]
    if len(data_results) > 0 and len(additional_results) > 0:
        data_results = pd.concat([data_results, additional_results])
    elif len(data_results) == 0:
        data_results = additional_results
    save_data_results(job_folder, data_results, 'push')

    return data_results


def get_data_results_filename(job_folder: str, prefix: str):
    return os.path.join(job_folder, f'{prefix}_data_results.pickle')


def load_data_results(job_folder: str, prefix: str) -> pd.DataFrame:
    data_results_filename = get_data_results_filename(job_folder, prefix)
    data_results: pd.DataFrame = pd.DataFrame(
        columns=['Result', 'ID', 'Type', 'Path', 'Asset', 'Name', 'Time', 'Count', 'Pages'])
    if util.safe_exists(data_results_filename):
        try:
            data_results = pd.read_pickle(data_results_filename)
        except Exception as e:
            raise SPyRuntimeError(
                f'Error loading "{data_results_filename}", please delete it manually to continue:\n{e}')

    return data_results


def save_data_results(job_folder: str, data_results: pd.DataFrame, prefix: str):
    data_results.to_pickle(get_data_results_filename(job_folder, prefix), protocol=4)


def redo(job_folder: str, status: Status):
    data_results = load_data_results(job_folder, 'push')
    data_results_ids = data_results['ID'].to_list()
    item_ids: pd.Series = status.df['ID']
    for index, item_id in item_ids.items():
        if item_id in data_results_ids:
            data_results.loc[data_results['ID'] == item_id, 'Result'] = 'Pushing'
            status.df.at[index, 'Result'] = 'Data push will be redone'
        else:
            status.df.at[index, 'Result'] = 'Not found'

    save_data_results(job_folder, data_results, 'push')
