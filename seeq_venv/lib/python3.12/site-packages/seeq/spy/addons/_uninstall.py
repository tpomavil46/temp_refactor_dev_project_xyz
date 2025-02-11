from __future__ import annotations

import datetime
import types
from typing import Optional, Union, Hashable

import pandas as pd

from seeq import spy
from seeq.sdk import *
from seeq.spy import _common
from seeq.spy import _login
from seeq.spy._errors import *
from seeq.spy._redaction import request_safely
from seeq.spy._session import Session
from seeq.spy._status import Status


@Status.handle_keyboard_interrupt()
def uninstall(items: Union[pd.DataFrame, pd.Series], *, errors: Optional[str] = None, quiet: Optional[bool] = None,
              status: Status = None, session: Optional[Session] = None) -> pd.DataFrame:
    """
    Uninstalls Add-on Tool from the Seeq Workbench. It does not remove the
    target_url contents. Uninstalling Add-on tools requires administrator access.

    Parameters
    ----------
    items : {pd.DataFrame, pd.Series}
        A DataFrame or Series containing ID column that can be used to
        identify the Add-on tool to uninstall. This is usually created via a call
        to spy.addons.search().

    errors : {'raise', 'catalog'}, default 'raise'
        If 'raise', any errors encountered will cause an exception. If
        'catalog', errors will be added to a 'Result' column in the status.df
        DataFrame.

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

    Returns
    -------
    pandas.DataFrame
        A DataFrame with the metadata of the Add-on tools uninstalled, along with
        any errors and statistics about the operation.

        Additionally, the following properties are stored on the "spy"
        attribute of the output DataFrame:

        =================== ===================================================
        Property            Description
        =================== ===================================================
        func                A str value of 'spy.addons.uninstall'
        kwargs              A dict with the values of the input parameters
                            passed to spy.addons.uninstall to get the output
                            DataFrame
        status              A spy.Status object with the status of the
                            spy.addons.uninstall call
        =================== ===================================================

    Examples
    --------
    Search for a tool with a specific name and uninstall it

    >>> search_results = spy.addons.search({'Name': 'Obsolete Tool'})
    >>> uninstalled_tool = spy.addons.uninstall(search_results)

    """

    input_args = _common.validate_argument_types([
        (items, 'items', (pd.DataFrame, pd.Series)),
        (errors, 'errors', str),
        (quiet, 'quiet', bool),
        (status, 'status', Status),
        (session, 'session', Session)
    ])

    session = Session.validate(session)
    status = Status.validate(status, session, quiet, errors)
    _login.validate_login(session, status)
    system_api = SystemApi(session.client)

    if 'ID' not in items:
        raise SPyValueError('items DataFrame must include "ID" column which is typically obtained from '
                            '"spy.addons.search"')
    if isinstance(items, pd.Series):
        items = pd.DataFrame([items])
    if not isinstance(items, pd.DataFrame):
        raise SPyTypeError('item must be a pandas.Series or pandas.DataFrame')

    status_columns = [c for c in ['ID', 'Name', 'Target URL'] if c in items]

    status.df = items[status_columns].copy()
    status.df['Count'] = 0
    status.df['Time'] = datetime.timedelta(0)
    status.df['Result'] = 'Uninstalling'
    timer = _common.timer_start()

    for row_index, row in items.iterrows():
        @request_safely(action_description=f"uninstalling addon with id {row['ID']}", status=status)
        def uninstall_addon(index: Hashable, df_row: pd.Series):
            system_api.delete_add_on_tool(id=df_row['ID'])
            status.df.at[index, 'Time'] = _common.timer_elapsed(timer)
            status.df.at[index, 'Count'] = 1
            status.df.at[index, 'Result'] = 'Uninstalled'

        uninstall_addon(row_index, row)

    status.update(f'Add-on tool{"s" if len(items) > 1 else ""} uninstall successful', Status.SUCCESS)

    uninstall_result_df = status.df.copy()
    push_df_properties = types.SimpleNamespace(
        func='spy.addons.uninstall',
        kwargs=input_args,
        status=status)

    _common.put_properties_on_df(uninstall_result_df, push_df_properties)

    return uninstall_result_df
