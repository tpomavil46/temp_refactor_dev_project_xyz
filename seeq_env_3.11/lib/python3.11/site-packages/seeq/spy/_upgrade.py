from __future__ import annotations

from typing import Optional, List

from seeq import spy
from seeq.spy import _common, _login, Session, Status, _datalab
from seeq.spy._errors import *


def upgrade(version: Optional[str] = None, force_restart: bool = False, use_testpypi: bool = False,
            status: Optional[Status] = None, session: Optional[Session] = None,
            dependencies: Optional[List[str]] = None):
    """
    Upgrades to the latest version of SPy that is compatible with this version of Seeq Server.

    An internet connection is required since this uses pip to pull the latest version from PyPI. This must be
    invoked from a Jupyter notebook or other IPython-compatible environment.

    Parameters
    ----------
    version : str, optional
        Attempts to upgrade to the provided version exactly as specified. The full SPy version must be
        provided (e.g. 221.13). For Seeq versions prior to R60, You must specify the full "seeq" module
        version (e.g. 58.0.2.184.12).

    force_restart : bool, optional
        If True, forces the kernel to shut down and restart after the upgrade. All in-memory variables and
        imports will be lost.

    use_testpypi : bool, optional
        For Seeq internal testing only.

    status : spy.Status, optional
        If specified, the supplied Status object will be updated as the command progresses. It gets filled
        in with the same information you would see in Jupyter in the blue/green/red table below your code
        while the command is executed.

    session : spy.Session, optional
        If supplied, the Session object (and its Options) will be used to store the login session state.
        This is used to access the server's current version.

    dependencies : List[str], optional
        Valid options include ["templates", "widgets", "jobs", "jupyter", "all"]
        If specified, which specific extra dependencies to install. If not specified, the installed packages
        will be scanned, and existing extras will be updated automatically. If an empty list is provided, no
        extras will be updated.

    Examples
    --------
    Upgrade to the latest version of SPy compatible with your Seeq server's major version.
    >>> spy.upgrade()

    Upgrade to version '221.13' of SPy.
    >>> spy.upgrade(version='221.13')

    """
    _common.validate_argument_types([
        (version, 'version', str),
        (force_restart, 'force_restart', bool),
        (use_testpypi, 'use_testpypi', bool),
        (status, 'status', Status),
        (session, 'session', Session),
        (dependencies, 'dependencies', List)
    ])
    session = Session.validate(session)
    status = Status.validate(status, session)

    if session.client is None:
        raise SPyRuntimeError('Not logged in. Execute spy.login() before calling this function so that the upgrade '
                              'mechanism knows what version of Seeq Server you are interfacing with.')

    pip_command = _login.generate_pip_upgrade_command(session, version, use_testpypi, dependencies)

    try:
        import IPython
    except ImportError:
        print(f'Unable to import `IPython`. Please run `{pip_command}` in a terminal to upgrade SPy.')
        return

    ipython = IPython.get_ipython()
    if not _datalab.is_ipython() or not _datalab.is_ipython_interactive() or not ipython:
        raise SPyValueError(f'spy.upgrade() must be invoked from a Jupyter notebook or other IPython-compatible '
                            f'environment. Unable to run "{pip_command}".')
    restart_message = 'The kernel will automatically be shut down afterward.' if force_restart else \
        'Please restart the kernel once the packages have been upgraded.'
    status.update(f'Running "{pip_command}". {restart_message}', Status.RUNNING)
    ipython.run_cell(pip_command)

    if force_restart:
        if not ipython.kernel:
            raise SPyValueError(f'Unable get IPython kernel to complete restart')
        ipython.kernel.do_shutdown(True)
