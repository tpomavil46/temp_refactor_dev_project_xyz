from __future__ import annotations

import os
import zipfile
from typing import Optional

from seeq.base import util
from seeq.spy import _common
from seeq.spy._errors import *
from seeq.spy._status import Status


# noinspection PyShadowingBuiltins
def zip(job_folder: str, *, overwrite: bool = False, quiet: Optional[bool] = None, status: Optional[Status] = None):
    """
    Creates a zip file of the job folder for easy sharing.

    Parameters
    ----------
    job_folder : {str}
        A full or partial path to the job folder to be zipped.

    overwrite : {bool}, default False
        If True, the zip file will be overwritten if it already exists.

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
        (overwrite, 'overwrite', bool),
        (quiet, 'quiet', bool),
        (status, 'status', Status)
    ])

    status = Status.validate(status, None, quiet)

    if not util.safe_exists(job_folder):
        raise SPyValueError(f'Job folder "{job_folder}" does not exist.')

    job_folder = os.path.abspath(job_folder)
    job_folder_zip = job_folder + '.zip'

    if util.safe_exists(job_folder_zip):
        if not overwrite:
            raise SPyRuntimeError(
                f'Zip file "{job_folder_zip}" already exists. Specify overwrite=True to overwrite it.')

        util.safe_remove(job_folder_zip)

    with zipfile.ZipFile(util.handle_long_filenames(job_folder_zip), "w", zipfile.ZIP_DEFLATED) as z:
        for root, dirs, files in util.safe_walk(job_folder):
            for file in files:
                filename = os.path.join(root, file)
                if os.path.isfile(filename):  # regular files only
                    archive_name = os.path.join(util.safe_relpath(root, job_folder), file)
                    _common.print_output('Archiving %s' % archive_name)
                    z.write(filename, archive_name)

    status.update(f'Success: Zip file written to "{job_folder_zip}"', Status.SUCCESS)


def unzip(job_folder_zip: str, *, overwrite: bool = False,
          quiet: Optional[bool] = None, status: Optional[Status] = None):
    """
    Unzips a job folder file created with spy.workbooks.job.zip(). The job
    folder will be the name of the zip file (without the .zip extension).

    Parameters
    ----------
    job_folder_zip : {str}
        A full or partial path to the job folder zip file to be un-zipped.

    overwrite : {bool}, default False
        If True, the job folder will be overwritten if it already exists.

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
        (job_folder_zip, 'job_folder_zip', str),
        (overwrite, 'overwrite', bool),
        (quiet, 'quiet', bool),
        (status, 'status', Status)
    ])

    status = Status.validate(status, None, quiet)

    if not util.safe_exists(job_folder_zip):
        raise SPyValueError(f'Zip file "{job_folder_zip}" does not exist.')

    job_folder_zip = os.path.abspath(job_folder_zip)
    job_folder = os.path.splitext(job_folder_zip)[0]

    if util.safe_exists(job_folder):
        if not overwrite:
            raise SPyRuntimeError(
                f'Zip file "{job_folder_zip}" already exists. Specify overwrite=True to overwrite it.')

        util.safe_rmtree(job_folder)

    with zipfile.ZipFile(util.handle_long_filenames(job_folder_zip), 'r') as zipf:
        zipf.extractall(util.handle_long_filenames(job_folder))

    status.update(f'Success: Zip file unzipped to "{job_folder}"', Status.SUCCESS)
