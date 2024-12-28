from __future__ import annotations

import types
from typing import Optional, Union

import numpy as np
import pandas as pd

from seeq import spy
from seeq.sdk import *
from seeq.spy import _common, _login, _metadata
from seeq.spy._errors import *
from seeq.spy._redaction import safely
from seeq.spy._session import Session
from seeq.spy._status import Status
from seeq.spy.workbooks import Item


@Status.handle_keyboard_interrupt()
def pull(items: Union[pd.DataFrame, dict, list, str, Item], *, include_my_effective_permissions: bool = False,
         errors: Optional[str] = None, quiet: Optional[bool] = None, status: Optional[Status] = None,
         session: Optional[Session] = None) -> pd.DataFrame:
    """
    Pulls access control entries for a set of items as specified
    by their IDs. The most common way to invoke this command is directly
    after having done a spy.search() and produced a DataFrame full of
    items to work with.

    Parameters
    ----------
    items : {pandas.DataFrame, dict, list, str, Item}
        The item IDs to pull for. This argument can take the following form:

        - a DataFrame with an "ID" column
        - a single dict with an "ID" key
        - a list of dicts with "ID" keys
        - a single Workbook object
        - a list of Workbook objects
        - a single string representing the ID of the item
        - a list of strings representing the IDs of the items
        - a SPy Item instance

    include_my_effective_permissions : bool, default False
        If True, adds 'Read/Write/Manage Permission' columns to the output
        DataFrame that reflect the calling user's permissions for the item.

    errors : {'raise', 'catalog'}, default 'raise'
        If 'raise', any errors encountered will cause an exception. If
        'catalog', errors will be added to a 'Pull Result' column in the
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
        A DataFrame with the metadata for the items pulled and an
        'Access Control' column with an embedded DataFrame in each cell that
        represents the Access Control List.

        Additionally, the following properties are stored on the "spy"
        attribute of the output DataFrame:

        =================== ===================================================
        Property            Description
        =================== ===================================================
        func                A str value of 'spy.acl.pull'
        kwargs              A dict with the values of the input parameters
                            passed to spy.acl.pull to get the output DataFrame
        status              A spy.Status object with the status of the
                            spy.acl.pull call
        =================== ===================================================

    Examples
    --------
    Search for signals with the name 'Humid' on the asset tree under
    'Example >> Cooling Tower 1', then retrieve ACLs for the results:

    >>> search_results = spy.search({'Name': 'Humid', 'Path': 'Example >> Cooling Tower 1'})
    >>> pull_results = spy.acl.pull(search_results)
    >>> pull_results.at[0, 'Access Control']  # Retrieves an inner DataFrame representing the ACL

    """
    input_args = _common.validate_argument_types([
        (items, 'items', (pd.DataFrame, list, dict, str, Item)),
        (include_my_effective_permissions, 'include_my_effective_permissions', bool),
        (errors, 'errors', str),
        (quiet, 'quiet', bool),
        (status, 'status', Status),
        (session, 'session', Session)
    ])

    session = Session.validate(session)
    status = Status.validate(status, session, quiet, errors)
    _login.validate_login(session, status)

    items = items_to_data_frame(items)

    status.df = pd.DataFrame([{'Count': 0}])
    pull_result_df = items.copy()
    pull_result_df.drop(columns=[c for c in _metadata.RESERVED_SPY_STATUS_COLUMN_NAMES if c in pull_result_df],
                        inplace=True)

    count = 0
    timer = _common.timer_start()
    status.df['Time'] = _common.timer_elapsed(timer)
    status.update('Pulling ACLs...', Status.RUNNING)

    pull_result_df['Pull Result'] = None
    for index, metadata_row in pull_result_df.iterrows():
        count += 1
        status.df['Count'] = count
        item_id = metadata_row['ID']

        put_permissions_on_df_row(session, pull_result_df, index, item_id, include_my_effective_permissions,
                                  status=status)

        status.df['Time'] = _common.timer_elapsed(timer)
        status.update('Pulling ACLs...', Status.RUNNING)
        if ('Pull Result' not in pull_result_df.columns) or (pull_result_df.at[index, 'Pull Result'] is None):
            pull_result_df.at[index, 'Pull Result'] = 'Success'

    if pull_result_df['Pull Result'].drop_duplicates().tolist() != ['Success']:
        status.update('Pulled ACLs with errors, see "Pull Result" column in returned DataFrame.', Status.FAILURE)
    else:
        status.update('Pulled ACLs successfully.', Status.SUCCESS)

    pull_df_properties = types.SimpleNamespace(
        func='spy.acl.pull',
        kwargs=input_args,
        status=status)

    _common.put_properties_on_df(pull_result_df, pull_df_properties)

    return pull_result_df


def put_permissions_on_df_row(session: Session, df, index, item_id, include_my_effective_permissions, status):
    items_api = ItemsApi(session.client)

    acl_df = pd.DataFrame({
        'ID': pd.Series(dtype=str),
        'Type': pd.Series(dtype=str),
        'Name': pd.Series(dtype=str),
        'Username': pd.Series(dtype=str),
        'Email': pd.Series(dtype=str),
        'Directory': pd.Series(dtype=str),
        'Archived': pd.Series(dtype=bool),
        'Enabled': pd.Series(dtype=bool),
        'Redacted': pd.Series(dtype=bool),
        'Role': pd.Series(dtype=str),
        'Origin Type': pd.Series(dtype=str),
        'Origin Name': pd.Series(dtype=str),
        'Read': pd.Series(dtype=bool),
        'Write': pd.Series(dtype=bool),
        'Manage': pd.Series(dtype=bool)
    })

    def _add_error_message_and_warn(msg):
        df.at[index, 'Pull Result'] = msg
        status.warn(msg)

    acl_output = safely(lambda: items_api.get_access_control(id=item_id),
                        action_description=f'get ACL for item {item_id}',
                        on_error=_add_error_message_and_warn,
                        status=status)

    if acl_output is None:
        return

    for entry in acl_output.entries:  # type: AceOutputV1
        identity: IdentityPreviewV1 = entry.identity
        acl_df.at[entry.id, 'ID'] = identity.id
        acl_df.at[entry.id, 'Type'] = identity.type
        acl_df.at[entry.id, 'Name'] = identity.name
        acl_df.at[entry.id, 'Username'] = identity.username
        acl_df.at[entry.id, 'Email'] = identity.email
        acl_df.at[entry.id, 'Directory'] = identity.datasource.name if identity.datasource is not None else None
        acl_df.at[entry.id, 'Archived'] = identity.is_archived
        acl_df.at[entry.id, 'Enabled'] = identity.is_enabled
        acl_df.at[entry.id, 'Redacted'] = identity.is_redacted
        acl_df.at[entry.id, 'Role'] = entry.role
        origin: ItemPreviewV1 = entry.origin
        acl_df.at[entry.id, 'Origin Type'] = origin.type if origin is not None else np.nan
        acl_df.at[entry.id, 'Origin Name'] = origin.name if origin is not None else np.nan
        permissions: PermissionsV1 = entry.permissions
        acl_df.at[entry.id, 'Read'] = permissions.read
        acl_df.at[entry.id, 'Write'] = permissions.write
        acl_df.at[entry.id, 'Manage'] = permissions.manage

    columns_to_add = [
        ('Access Control', object, (bool, object)),
        ('Permissions Inheritance Disabled', bool, (bool, object)),
        ('Permissions From Datasource', bool, (bool, object))
    ]

    if include_my_effective_permissions:
        columns_to_add.extend([
            ('Read Permission', bool, (bool, object)),
            ('Write Permission', bool, (bool, object)),
            ('Manage Permission', bool, (bool, object))
        ])

    for column_name, dtype, acceptable_dtype in columns_to_add:
        if column_name not in df.columns or df[column_name].dtype not in acceptable_dtype:
            df[column_name] = pd.Series(dtype=dtype)

    df.at[index, 'Permissions Inheritance Disabled'] = acl_output.permissions_inheritance_disabled
    df.at[index, 'Permissions From Datasource'] = acl_output.permissions_managed_by_datasource
    df.at[index, 'Access Control'] = acl_df

    if include_my_effective_permissions:
        item_output = safely(lambda: items_api.get_item_and_all_properties(id=item_id),
                             action_description=f'get all properties for item {item_id}',
                             on_error=_add_error_message_and_warn,
                             status=status)

        if item_output is not None:
            effective_permissions: PermissionsV1 = item_output.effective_permissions
            df.at[index, 'Read Permission'] = effective_permissions.read
            df.at[index, 'Write Permission'] = effective_permissions.write
            df.at[index, 'Manage Permission'] = effective_permissions.manage


def items_to_data_frame(items):
    if isinstance(items, str):
        new_items = pd.DataFrame([{'ID': items}])
    elif isinstance(items, list):
        new_items = pd.DataFrame()
        for item in items:
            if isinstance(item, str):
                new_items = pd.concat([new_items, pd.DataFrame([{'ID': item}])], ignore_index=True)
            elif isinstance(item, dict):
                new_items = pd.concat([new_items, pd.DataFrame([item])], ignore_index=True)
            elif isinstance(item, Item):
                new_items = pd.concat([new_items, pd.DataFrame([{
                    'ID': item.id,
                    'Name': item.name
                }])], ignore_index=True)
    elif isinstance(items, dict):
        new_items = pd.DataFrame([items])
    elif isinstance(items, pd.DataFrame):
        new_items = items
    elif isinstance(items, Item):
        new_items = pd.DataFrame([{
            'ID': items.id,
            'Name': items.name
        }])
    else:
        raise SPyTypeError('"items" argument is not recognized. It must be one of the following:\n'
                           '- a DataFrame with an "ID" column\n'
                           '- a single dict with an "ID" key\n',
                           '- a list of dicts with "ID" keys\n',
                           '- a single Workbook object\n',
                           '- a list of Workbook objects\n',
                           '- a single string representing the ID of the item\n'
                           '- a list of strings representing the IDs of the items')

    if len(new_items) == 0:
        new_items = pd.DataFrame({'ID': pd.Series(dtype=str)})
    if len(new_items) > 0 and 'ID' not in new_items:
        raise SPyValueError('"items" must have "ID" column')

    return new_items
