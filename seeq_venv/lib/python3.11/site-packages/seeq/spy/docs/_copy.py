from __future__ import annotations

import os

from seeq.base import util
from seeq.spy import _common
from seeq.spy._errors import *


def copy(folder=None, *, overwrite=False, advanced=True):
    """
    Copies the SPy Documentation (Jupyter Notebooks) to a particular folder. This is typically used when the seeq
    module is installed via PyPI.

    This function should be called again with overwrite=True if the seeq module is updated.

    Parameters
    ----------
    folder : str
        The folder to receive the documentation. By default it will be copied to a 'SPy Documentation' folder in the
        current working directory.

    overwrite : bool
        If True, any existing files in the specified folder will be deleted before the documentation is copied in.

    advanced : bool
        Deprecated.
    """
    _common.validate_argument_types([
        (folder, 'folder', str),
        (overwrite, 'overwrite', bool),
        (advanced, 'advanced', bool)
    ])

    if not folder:
        folder = os.path.join(os.getcwd(), 'SPy Documentation')

    if util.safe_exists(folder):
        if not overwrite:
            raise SPyRuntimeError('The "%s" folder already exists. If you would like to overwrite it, supply the '
                                  'overwrite=True parameter. Make sure you don\'t have any of your own work in that '
                                  'folder!' % folder)

        util.safe_rmtree(folder)

    library_doc_folder = os.path.join(os.path.dirname(__file__), 'Documentation')

    util.safe_copytree(library_doc_folder, folder)

    print('Copied SPy library documentation to "%s"' % folder)
