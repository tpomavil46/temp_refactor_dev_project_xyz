from __future__ import annotations

import os
import re
import types
from typing import Dict, Optional, Union

import pandas as pd

from seeq import spy
from seeq.base import util
from seeq.spy import _common
from seeq.spy import _login
from seeq.spy._session import Session
from seeq.spy._status import Status


@Status.handle_keyboard_interrupt()
def pull(job_folder: str, workbooks_df: Union[pd.DataFrame, str], *, resume: bool = True,
         include_referenced_workbooks: bool = True,
         include_rendered_content: bool = False,
         errors: Optional[str] = None, quiet: Optional[bool] = None,
         status: Optional[Status] = None,
         session: Optional[Session] = None) -> pd.DataFrame:
    """
    Pulls the definitions for each workbook specified by workbooks_df on to
    disk, in a restartable "job"-like fashion.

    Parameters
    ----------
    job_folder : {str}
        A full or partial path to a folder on disk where the workbooks
        definitions will be saved. If the folder does not exist, it will be
        created. If the folder exists, the job will continue where it left off.

    workbooks_df : {str, pd.DataFrame}
        A DataFrame containing 'ID', 'Type' and 'Workbook Type' columns that
        can be used to identify the workbooks to pull. This is usually created
        via a call to spy.workbooks.search(). Alternatively, you can supply a
        workbook ID directly as a str or the URL of a Seeq Workbench worksheet
        as a str.

    resume : bool, default True
        True if the pull should resume from where it left off, False if it
        should pull everything again.

    include_referenced_workbooks : bool, default True
        If True, Analyses that are depended upon by Topics will be
        automatically included in the resulting list even if they were not part
        of the workbooks_df DataFrame.

    include_rendered_content : bool, default False
        If True, any Organizer Topics pulled will include rendered content
        images, which will cause spy.workbooks.save() to include a folder for
        each Topic Document with its HTML and images such that it can be loaded
        and viewed in a browser.

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
    input_args = _common.validate_argument_types([
        (job_folder, 'job_folder', str),
        (workbooks_df, 'workbooks_df', (pd.DataFrame, str)),
        (resume, 'resume', bool),
        (include_referenced_workbooks, 'include_referenced_workbooks', bool),
        (include_rendered_content, 'include_rendered_content', bool),
        (errors, 'errors', str),
        (quiet, 'quiet', bool),
        (status, 'status', Status),
        (session, 'session', Session)
    ])

    session = Session.validate(session)
    status = Status.validate(status, session, quiet, errors)
    _login.validate_login(session, status)

    util.safe_makedirs(job_folder, exist_ok=True)

    job_workbooks_folder = get_workbooks_folder(job_folder)
    util.safe_makedirs(job_workbooks_folder, exist_ok=True)

    existing = {workbook_id: workbook_folder for workbook_folder, workbook_id in
                walk_workbook_folders(job_workbooks_folder)}

    # Remove existing workbook folders that did not complete successfully
    to_remove = set()
    for workbook_id in existing:
        workbook_folder = os.path.join(job_workbooks_folder, existing[workbook_id])
        if not util.safe_exists(os.path.join(workbook_folder, 'Complete')):
            # Something must have gone wrong during the save. Delete the whole thing so we start over
            to_remove.add(workbook_id)

    for workbook_id in to_remove:
        util.safe_rmtree(os.path.join(job_workbooks_folder, existing[workbook_id]))
        del existing[workbook_id]

    def _load_if_exists(_workbook_id: str):
        if resume and _workbook_id in existing:
            return spy.workbooks.load(os.path.join(job_workbooks_folder, existing[_workbook_id]), quiet=True)[0]
        else:
            return None

    data_usages: Dict[str, dict] = dict()
    all_workstep_usages: Dict[str, Dict[str, Union[list, set]]] = dict()

    def _ensure(_workstep_id):
        if _workstep_id not in all_workstep_usages:
            all_workstep_usages[_workstep_id] = {'Periods': list(), 'Stored Items': set()}

    for workbook in spy.workbooks._pull.do_pull(
            workbooks_df, status=status, session=session,
            include_referenced_workbooks=include_referenced_workbooks,
            include_rendered_content=include_rendered_content,
            preprocess_callback=_load_if_exists):
        workstep_usages = workbook.get_workstep_usages(now=pd.Timestamp.utcnow())
        for workstep_id, periods in workstep_usages.items():
            _ensure(workstep_id)
            all_workstep_usages[workstep_id]['Periods'].extend(periods)

        if isinstance(workbook, spy.workbooks.Analysis):
            for worksheet in workbook.worksheets:
                for workstep_id, workstep in worksheet.worksteps.items():
                    _ensure(workstep_id)
                    stored_item_references = workstep.get_stored_item_references(workbook.item_inventory)
                    all_workstep_usages[workstep_id]['Stored Items'].update({r.id for r in stored_item_references})
                    for r in stored_item_references:
                        data_usages[r.id] = {'Definition': r.definition_dict, 'Periods': list()}

        if not resume or workbook.id not in existing:
            spy.workbooks.save(workbook, job_workbooks_folder, overwrite=True, quiet=True)

    for workstep_id, workstep_usages in all_workstep_usages.items():
        for stored_item_id in workstep_usages['Stored Items']:
            if stored_item_id not in data_usages:
                raise Exception(f'Something went wrong-- no data usage found for workstep {stored_item_id}')
            data_usages[stored_item_id]['Periods'].extend(workstep_usages['Periods'])

    for entry in data_usages.values():
        entry['Periods'] = flatten_timestamps(entry['Periods'])

    spy.workbooks.job.data._pull.save_data_usage(job_folder, data_usages)

    copy_datasource_maps_to_root(job_folder)

    results_df = status.df.copy()

    results_df_properties = types.SimpleNamespace(
        func='spy.workbooks.job.pull',
        kwargs=input_args,
        status=status)

    _common.put_properties_on_df(results_df, results_df_properties)

    return results_df


def flatten_timestamps(timestamps):
    if not timestamps:
        return list()

    # Sort the timestamp pairs based on their start times
    sorted_timestamps = sorted(timestamps, key=lambda x: x['Start'])

    # Initialize the result list with the first timestamp pair
    flattened = [sorted_timestamps[0]]

    for current in sorted_timestamps[1:]:
        # Get the last merged interval from the result list
        last = flattened[-1]

        # Check for overlap and merge if needed
        if current['Start'] <= last['End']:
            # Merge the intervals by updating the end time
            flattened[-1] = {'Start': last['Start'], 'End': max(last['End'], current['End'])}
        else:
            # No overlap, add the current interval to the result list
            flattened.append({'Start': current['Start'], 'End': current['End']})

    return flattened


def copy_datasource_maps_to_root(job_folder):
    job_datasource_maps_folder = get_datasource_maps_folder(job_folder)
    job_workbooks_folder = get_workbooks_folder(job_folder)
    util.safe_makedirs(job_datasource_maps_folder, exist_ok=True)
    for workbook_folder, _, in walk_workbook_folders(job_workbooks_folder):
        workbook_folder = os.path.join(job_workbooks_folder, workbook_folder)
        for datasource_map_file in os.listdir(workbook_folder):  # type: str
            if not datasource_map_file.startswith('Datasource_Map_'):
                continue

            src = os.path.join(workbook_folder, datasource_map_file)
            dest = os.path.join(job_datasource_maps_folder, datasource_map_file)
            if not util.safe_exists(dest):
                util.safe_copy(src, dest)


def get_datasource_maps_folder(job_folder):
    job_datasource_maps_folder = os.path.join(job_folder, 'Datasource Maps')
    return job_datasource_maps_folder


def get_workbooks_folder(job_folder):
    return os.path.join(job_folder, 'Workbooks')


def walk_workbook_folders(job_workbooks_folder):
    for folder in os.listdir(job_workbooks_folder):
        path = os.path.join(job_workbooks_folder, folder)
        if not util.safe_isdir(path):
            continue

        match = re.match(r'^.*? \(([^)]+)\)$', folder)
        if match is None:
            continue

        yield match.group(0), match.group(1)


def redo(job_folder: str, status: Status):
    job_workbooks_folder = get_workbooks_folder(job_folder)
    workbook_folders = {i: f for f, i in walk_workbook_folders(job_workbooks_folder)}
    workbook_ids: pd.Series = status.df['ID']
    for index, workbook_id in workbook_ids.items():
        if workbook_id in workbook_folders:
            util.safe_rmtree(os.path.join(job_workbooks_folder, workbook_folders[workbook_id]))
            status.df.at[index, 'Result'] = 'Pull will be redone'
        else:
            status.df.at[index, 'Result'] = 'Not found'
