from __future__ import annotations

import types
from typing import Optional, Union

import pandas as pd

from seeq import spy
from seeq.sdk import *
from seeq.spy import _common, _login, _metadata
from seeq.spy._errors import *
from seeq.spy._session import Session
from seeq.spy._status import Status
from seeq.spy.acl import _pull
from seeq.spy.workbooks import Item


@Status.handle_keyboard_interrupt()
def push(items: Union[pd.DataFrame, dict, list, str, Item], acl: Union[pd.DataFrame, dict, list], *,
         replace: bool = False, disable_inheritance: bool = None, errors: Optional[str] = None,
         quiet: Optional[bool] = None, status: Optional[Status] = None, session: Optional[Session] = None
         ) -> pd.DataFrame:
    """
    Pushes new access control entries against a set of items as specified
    by their IDs. The most common way to invoke this command is directly
    after having done a spy.search() or spy.push() and produced a DataFrame
    full of items to work with.

    Parameters
    ----------
    items : {pandas.DataFrame, dict, list, str, Item}
        The item IDs to push against. This argument can take the following
        form:

        - a DataFrame with an "ID" column
        - a single dict with an "ID" key
        - a list of dicts with "ID" keys
        - a single Workbook object
        - a list of Workbook objects
        - a single string representing the ID of the item
        - a list of strings representing the IDs of the items
        - a SPy Item instance

    acl : {pandas.DataFrame, dict, list}
        The Access Control List can be either a Pandas DataFrame
        a list of dicts, or just a dict. The dict or DataFrame rows must
        consist of either 'ID' OR ('Name' or 'Username') to specify the
        identity and then a 'Read', 'Write' and/or 'Manage' entry that is
        a boolean True or False value.

    replace : {bool}, optional, default False
        - If False, then existing access control entries will not be disturbed
          but new entries will be added.
        - If True, then existing access control entries will be removed
          and replaced with the entries from the acl argument.

    disable_inheritance : {bool}, optional, default None
        - If None, then the inheritance will remain unchanged. (This is the
          default behavior.)
        - If False, then the ACL inherits from either the datasource or the
          workbook (for signals/scalars/conditions/assets/metrics). Workbooks
          inherit their ACLs from their parent folder.
        - If True, then the ACL will no longer inherit from the parent.

    errors : {'raise', 'catalog'}, default 'raise'
        If 'raise', any errors encountered will cause an exception. If
        'catalog', errors will be added to a 'Push Result' column in the
        status.df DataFrame.

    quiet : bool, default False
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
        A DataFrame with the metadata for the items pushed, along with any
        errors (in the 'Push Result' column).

        Additionally, the following properties are stored on the "spy"
        attribute of the output DataFrame:

        =================== ===================================================
        Property            Description
        =================== ===================================================
        func                A str value of 'spy.acl.push'
        kwargs              A dict with the values of the input parameters
                            passed to spy.acl.push to get the output DataFrame
        status              A spy.Status object with the status of the
                            spy.acl.push call
        =================== ===================================================

    Examples
    --------
    Search for signals with the name 'Hydrocracker' and add a single ACL for Walter:

    >>> search_results = spy.search({'Name': 'Hydrocracker'})
    >>> push_results = spy.acl.push(search_results, {'Username': 'walter.reed@va.com', 'Read': True})

    Add multiple ACLs -- one for Reed Abook (a user) and one for the Everyone group:

    >>> search_results = spy.search({'Name': 'Hydrocracker'})
    >>> push_results = spy.acl.push(search_results, [{
    >>>                                 'Username': 'reed.abook@seeq.com',
    >>>                                 'Read': True,
    >>>                                 'Write': True,
    >>>                                 'Manage': True
    >>>                             }, {
    >>>                                 'Name': 'Everyone',
    >>>                                 'Type': 'UserGroup',
    >>>                                 'Read': True
    >>>                             }])
    """
    input_args = _common.validate_argument_types([
        (items, 'items', (pd.DataFrame, list, dict, str, Item)),
        (acl, 'acl', (pd.DataFrame, list, dict)),
        (replace, 'replace', bool),
        (disable_inheritance, 'disable_inheritance', bool),
        (errors, 'errors', str),
        (quiet, 'quiet', bool),
        (status, 'status', Status),
        (session, 'session', Session)
    ])

    session = Session.validate(session)
    status = Status.validate(status, session, quiet, errors)
    _login.validate_login(session, status)

    users_api = UsersApi(session.client)

    items = _pull.items_to_data_frame(items)

    acl = _common.force_to_data_frame(acl)

    if not isinstance(acl, pd.DataFrame):
        raise SPyTypeError('acl argument is not a DataFrame or DataFrame-like object')

    status.df = pd.DataFrame({
        'Count': pd.Series([0], dtype=int)
    })

    push_result_df = items.copy()
    push_result_df.drop(columns=[c for c in _metadata.RESERVED_SPY_STATUS_COLUMN_NAMES if c in push_result_df],
                        inplace=True)

    count = 0
    timer = _common.timer_start()
    for acl_index, acl_row in acl.iterrows():
        if not _common.present(acl_row, 'Read'):
            acl.at[acl_index, 'Read'] = False
        if not _common.present(acl_row, 'Write'):
            acl.at[acl_index, 'Write'] = False
        if not _common.present(acl_row, 'Manage'):
            acl.at[acl_index, 'Manage'] = False

        # The UI implicitly sets Read/Write if a higher permission is granted, so we'll do the same here
        if acl.at[acl_index, 'Manage']:
            acl.at[acl_index, 'Write'] = True
        if acl.at[acl_index, 'Write']:
            acl.at[acl_index, 'Read'] = True

        if not _common.present(acl_row, 'ID'):
            if not _common.present(acl_row, 'Name') and not _common.present(acl_row, 'Username'):
                raise SPyValueError(
                    f'Access Control DataFrame must include value in either ID or Name or Username column:\n'
                    f'{acl_row}')

            matches = list()
            query = _common.get(acl_row, 'Username') if _common.present(acl_row, 'Username') else \
                _common.get(acl_row, 'Name')

            identities = users_api.autocomplete_users_and_groups(query=query, limit=100000)

            for identity in identities.items:  # type: IdentityPreviewV1
                if (_common.present(acl_row, 'Name') and
                        _insensitive_not_equal(identity.name, _common.get(acl_row, 'Name'))):
                    continue

                if (_common.present(acl_row, 'Username') and
                        _insensitive_not_equal(identity.username, _common.get(acl_row, 'Username'))):
                    continue

                if (_common.present(acl_row, 'Type') and
                        _insensitive_not_equal(identity.type, _common.get(acl_row, 'Type'))):
                    continue

                if (_common.present(acl_row, 'Directory') and
                        _insensitive_not_equal(identity.datasource.name, _common.get(acl_row, 'Directory'))):
                    continue

                matches.append(identity)

            if len(matches) == 0:
                raise SPyValueError(
                    f'Could not find identity "{query}" for Access Control entry:\n{acl_row}')
            elif len(matches) > 1:
                raise SPyValueError(
                    f'Multiple matches found for "{query}" for Access Control entry:\n{acl_row}\n'
                    'Narrow down by supplying "Name", "Username", "Type" and/or "Directory" columns (if you '
                    "haven't already)")
            else:
                acl.at[acl_index, 'ID'] = matches[0].id
                acl.at[acl_index, 'Name'] = matches[0].name
                acl.at[acl_index, 'Directory'] = matches[0].datasource.name

    status.df['Time'] = _common.timer_elapsed(timer)
    status.update('Pushing ACLs...', Status.RUNNING)

    push_result_df['Push Result'] = None
    for metadata_index, metadata_row in push_result_df.iterrows():
        count += 1
        status.df['Count'] = count
        try:
            item_id = metadata_row['ID']

            _metadata.push_access_control(session, item_id, acl, replace,
                                          disable_permission_inheritance=disable_inheritance)

            status.df['Time'] = _common.timer_elapsed(timer)
            status.update('Pushing ACLs...', Status.RUNNING)
            push_result_df.at[metadata_index, 'Push Result'] = 'Success'

        except Exception as e:
            if status.errors == 'raise':
                raise

            push_result_df.at[metadata_index, 'Push Result'] = _common.format_exception(e)

    if push_result_df['Push Result'].drop_duplicates().tolist() != ['Success']:
        status.update('Pushed ACLs with errors, see "Push Result" column in returned DataFrame.', Status.FAILURE)
    else:
        status.update('Pushed ACLs successfully.', Status.SUCCESS)

    push_df_properties = types.SimpleNamespace(
        func='spy.acl.push',
        kwargs=input_args,
        status=status)

    _common.put_properties_on_df(push_result_df, push_df_properties)

    return push_result_df


def _insensitive_not_equal(a: Optional[str], b: Optional[str]):
    if a is None and b is None:
        return False
    elif a is not None and b is None:
        return True
    elif a is None and b is not None:
        return True
    else:
        return a.lower() != b.lower()
