from __future__ import annotations

from typing import Optional

import seeq.spy._errors as errors
from seeq.sdk import ApiClient
from seeq.sdk.configuration import Configuration
from seeq.sdk.models import UserOutputV1
from seeq.spy import acl
from seeq.spy import addons
from seeq.spy import assets
from seeq.spy import docs
from seeq.spy import jobs
from seeq.spy import notifications
from seeq.spy import utils
from seeq.spy import widgets
from seeq.spy import workbooks
from seeq.spy._common import PATH_ROOT, DEFAULT_WORKBOOK_PATH, GLOBALS_ONLY, GLOBALS_AND_ALL_WORKBOOKS, \
    INHERIT_FROM_WORKBOOK
from seeq.spy._errors import SPyValueError, SPyTypeError, SPyRuntimeError, SPyKeyboardInterrupt, SPyDependencyNotFound
from seeq.spy._login import login, logout
from seeq.spy._plot import plot
from seeq.spy._pull import pull
from seeq.spy._push import push
from seeq.spy._search import search
from seeq.spy._session import Session, Options
from seeq.spy._status import Status
from seeq.spy._swap import swap
from seeq.spy._upgrade import upgrade

# noinspection DuplicatedCode
session: Session = Session(client_configuration=Configuration())
"""
The default session used by SPy functions that interact with Seeq Server
"""

options: Options = session.options
"""
Equivalent to `spy.session.options`
"""

client: Optional[ApiClient] = None
"""
Equivalent to `spy.session.client`
"""

user: Optional[UserOutputV1] = None
"""
Equivalent to `spy.session.user`
"""

server_version: Optional[str] = None
"""
Equivalent to `spy.session.server_version`
"""

__all__ = ['acl', 'addons', 'assets', 'docs', 'workbooks', 'widgets', 'login', 'logout', 'plot', 'pull', 'push',
           'search', 'swap', 'PATH_ROOT', 'DEFAULT_WORKBOOK_PATH', 'GLOBALS_ONLY', 'GLOBALS_AND_ALL_WORKBOOKS',
           'INHERIT_FROM_WORKBOOK', 'Session', 'Status', 'options', 'session', 'client', 'user',
           'server_version', 'jobs', 'notifications', 'upgrade', 'utils', 'errors',
           'SPyValueError', 'SPyTypeError', 'SPyRuntimeError', 'SPyKeyboardInterrupt', 'SPyDependencyNotFound']

__version__ = '%d.%d' % (int('$SQ(VERSION_MAJOR_PYPI)'), int('$SQ(VERSION_MINOR_PYPI)'))
