from __future__ import annotations

import glob
import os
import tempfile
import zipfile
from typing import Optional

from seeq.base import util
from seeq.spy import _common
from seeq.spy._errors import *
from seeq.spy._status import Status
from seeq.spy.workbooks._template import package_as_templates
from seeq.spy.workbooks._workbook import Workbook, WorkbookList


def load(folder_or_zipfile, *, as_template_with_label: str = None, errors: Optional[str] = None,
         quiet: Optional[bool] = None, status: Optional[Status] = None) -> WorkbookList:
    """
    Loads a list of workbooks from a folder on disk into Workbook objects in
    memory.

    Parameters
    ----------
    folder_or_zipfile : str
        A folder or zip file on disk containing workbooks to be loaded. Note
        that any subfolder structure will work -- this function will scan for
        any subfolders that contain a Workbook.json file and assume they should
        be loaded.

    as_template_with_label : str
        Causes the workbooks to be loaded as templates (either AnalysisTemplate
        or TopicTemplate) with the label specified. See the Workbook Templates
        documentation notebook for more information about templates.

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
        command is executed.
    """
    _common.validate_argument_types([
        (folder_or_zipfile, 'folder_or_zipfile', str),
        (as_template_with_label, 'as_template_with_label', str),
        (errors, 'errors', str),
        (quiet, 'quiet', bool),
        (status, 'status', Status)
    ])

    status = Status.validate(status, None, quiet, errors)

    folder_or_zipfile = os.path.normpath(folder_or_zipfile)

    try:
        if not util.safe_exists(folder_or_zipfile):
            raise SPyRuntimeError('Folder/zipfile "%s" does not exist' % folder_or_zipfile)

        if folder_or_zipfile.lower().endswith('.zip'):
            with tempfile.TemporaryDirectory() as temp:
                with zipfile.ZipFile(util.handle_long_filenames(folder_or_zipfile), "r") as z:
                    status.update('Unzipping "%s"' % folder_or_zipfile, Status.RUNNING)
                    z.extractall(temp)

                status.update('Loading from "%s"' % temp, Status.RUNNING)
                workbooks = _load_from_folder(temp)
        else:
            status.update('Loading from "%s"' % folder_or_zipfile, Status.RUNNING)
            workbooks = _load_from_folder(folder_or_zipfile)

        if as_template_with_label is not None:
            workbooks = package_as_templates(workbooks, as_template_with_label)

        status.update('Success', Status.SUCCESS)
        return workbooks

    except KeyboardInterrupt:
        status.update('Load canceled', Status.CANCELED)


def _load_from_folder(folder):
    folder_escaped = glob.escape(folder)
    workbook_json_files = util.safe_glob(os.path.join(folder_escaped, '**', 'Workbook.json'), recursive=True)

    workbooks = WorkbookList()
    for workbook_json_file in workbook_json_files:
        workbooks.append(Workbook.load(os.path.dirname(workbook_json_file)))

    return workbooks
