from __future__ import annotations

from typing import Optional, Union

import pandas as pd

from seeq.base import util
from seeq.spy import _common
from seeq.spy._errors import *
from seeq.spy._status import Status
from seeq.spy.workbooks.job import _pull, _push


def redo(job_folder: str, workbooks_df: Union[pd.DataFrame, str, list], action: Optional[str] = None,
         *, quiet: Optional[bool] = None, status: Optional[Status] = None):
    """
    Creates a zip file of the job folder for easy sharing.

    Parameters
    ----------
    job_folder : {str}
        A full or partial path to the job folder to be zipped.

    workbooks_df : {pd.DataFrame, str, list}
        A DataFrame containing an 'ID' column that can be used to identify the
        workbooks to affect. These IDs are based on the source system (not the
        destination system). Alternatively, you can supply a workbook ID
        directly as a str or list of strs.

    action : str
        If supplied, limits the redo to the specified actions. You can specify
        'pull' or 'push'. If not supplied, both pull and push are affected.
        Note that 'pull' automatically includes 'push'.

    quiet : bool
        If True, suppresses progress output. Note that when status is
        provided, the quiet setting of the Status object that is passed
        in takes precedence.

    status : spy.Status, optional
        If specified, the supplied Status object will be updated as the command
        progresses. It gets filled in with the same information you would see
        in Jupyter in the blue/green/red table below your code while the
        command is executed.
    """
    _common.validate_argument_types([
        (job_folder, 'job_folder', str),
        (workbooks_df, 'workbooks_df', (pd.DataFrame, str, list)),
        (action, 'action', str),
        (quiet, 'quiet', bool),
        (status, 'status', Status)
    ])

    status = Status.validate(status, None, quiet)

    if not util.safe_exists(job_folder):
        raise SPyValueError(f'Job folder "{job_folder}" does not exist.')

    if isinstance(workbooks_df, str):
        workbooks_df = pd.DataFrame({'ID': [workbooks_df]})
    elif isinstance(workbooks_df, list):
        workbooks_df = pd.DataFrame({'ID': workbooks_df})

    if 'ID' not in workbooks_df:
        raise SPyValueError('workbooks_df must contain an ID column.')

    if action is not None:
        if action not in ['pull', 'push']:
            raise SPyValueError('action must be "pull" or "push".')
    else:
        action = 'pull'

    workbooks_df: pd.DataFrame
    status_columns = [c for c in ['ID', 'Name', 'Type', 'Workbook Type'] if c in workbooks_df]

    status.df = workbooks_df[status_columns].copy()

    if action == 'pull':
        _pull.redo(job_folder, status)

    if action == 'push':
        _push.redo(job_folder, status)

    status.update(
        'Successfully marked specified items to be redone. Execute spy.job.workbooks.pull() and/or '
        'spy.job.workbooks.push() again.', Status.SUCCESS)
