from __future__ import annotations

import datetime
import types
from typing import List, Optional

import numpy as np
import pandas as pd

from seeq.base.seeq_names import SeeqNames
from seeq.sdk import *
from seeq.spy import _common
from seeq.spy import _login
from seeq.spy import _metadata
from seeq.spy._context import WorkbookContext
from seeq.spy._errors import *
from seeq.spy._redaction import safely
from seeq.spy._session import Session
from seeq.spy._status import Status
from seeq.spy.workbooks import _folder
from seeq.spy.workbooks import _pull
from seeq.spy.workbooks import _user
from seeq.spy.workbooks._annotation import Annotation
from seeq.spy.workbooks._context import WorkbookPushContext
from seeq.spy.workbooks._item import Item, ItemMap
from seeq.spy.workbooks._template import ItemTemplate
from seeq.spy.workbooks._workbook import Workbook, WorkbookList, DatasourceMapList


@Status.handle_keyboard_interrupt()
def push(workbooks, *, path: Optional[str] = None, owner: Optional[str] = None,
         label: Optional[str] = None, datasource: Optional[str] = None, datasource_map_folder: Optional[str] = None,
         use_full_path: bool = False, access_control: Optional[str] = None, override_max_interp: bool = False,
         include_inventory: Optional[bool] = None, include_annotations: bool = True, refresh: bool = True,
         lookup_df: pd.DataFrame = None, specific_worksheet_ids: Optional[List[str]] = None,
         create_dummy_items_in_workbook: Optional[str] = None, assume_dependencies_exist: bool = False,
         reconcile_inventory_by: str = 'id', global_inventory: Optional[str] = None, item_map: Optional[ItemMap] = None,
         errors: Optional[str] = None, quiet: Optional[bool] = None, status: Optional[Status] = None,
         session: Optional[Session] = None, scope_globals_to_workbook: Optional[bool] = None) -> pd.DataFrame:
    """
    Pushes workbooks into Seeq using a list of Workbook object definitions.

    Parameters
    ----------
    workbooks : {Workbook, list[Workbook]}
        A Workbook object or list of Workbook objects to be pushed into Seeq.

    path : str, default None
        A '>>'-delimited folder path to create to contain the workbooks. Note
        that a further subfolder hierarchy will be created to preserve the
        relative paths that the folders were in when they were searched for
        and pulled. If you specify None, then the workbook will stay where
        it is (if it has already been pushed once).

        If you specify spy.workbooks.MY_FOLDER, it will be moved to the user's
        home folder.

        If you specify a folder ID directly, it will be pushed to that folder.

        If you specify spy.workbooks.ORIGINAL_FOLDER, it will be pushed to the
        folder it was in when it was originally pulled. The folder hierarchy
        will be recreated on the target server if it doesn't already exist.
        (You must be an admin to use this to ensure you have permissions to
        put things where they need to go.)

    owner : str, default None
        Determines the ownership of pushed workbooks and folders.

        By default, the current owner will be preserved. If the content doesn't
        exist yet, the logged-in user will be the owner.

        All other options require that the logged-in user is an admin:

        If spy.workbooks.ORIGINAL_OWNER, ownership is assigned according to the
        original owner of the pulled content. (You must be an admin to use
        this.)

        If spy.workbooks.FORCE_ME_AS_OWNER, existing content will be
        reassigned to the logged-in user.

        If a username or a user's Seeq ID is supplied, that user will be
        assigned as owner.

        You may need to supply an appropriate datasource map if the usernames
        are different between the original and the target servers.

    label : str
        A user-defined label that differentiates this push operation from
        others. By default, the label will be the logged-in user's username
        OR the username from the 'owner' argument so that push activity will
        generally be isolated by user. But you can override this with a label
        of your choosing.

    datasource : str, optional, default 'Seeq Data Lab'
        The name of the datasource within which to contain all the pushed items.
        Items inherit access control permissions from their datasource unless it
        has been overridden at a lower level. If you specify a datasource using
        this argument, you can later manage access control (using spy.acl functions)
        at the datasource level for all the items you have pushed.

        If you instead want access control for your items to be inherited from the
        workbook they are scoped to, specify `spy.INHERIT_FROM_WORKBOOK`.

    datasource_map_folder : str, default None
        A folder containing Datasource_Map_Xxxx_Yyyy_Zzzz.json files that can
        provides a means to map stored items (i.e., those originating from
        external datasources like OSIsoft PI) from one server to another or
        from one datasource to another (i.e., for workbook swapping). A default
        set of datasource map files is created during a pull/save sequence, and
        you can copy these default files to a folder, alter them, and then
        specify the folder as this argument.

    use_full_path : bool, default False
        If True, the original full path for an item is reconstructed, as
        opposed to the path that is relative to the Path property supplied to
        the spy.workbooks.search() call that originally helped create these
        workbook definitions. Note that this full path will still be inside
        the folder specified by the 'path' argument, if supplied.

    access_control : str, default None
        Specifies how Access Control Lists should be treated, via the
        following keywords: add/replace,loose/strict

        - If None, then no access control entries are pushed.
        - If 'add', then existing access control entries will not be disturbed
          but new entries will be added.
        - If 'replace', then existing access control entries will be removed
          and replaced with the entries from workbook definitions.
        - If 'loose', then any unmapped users/groups from the workbook
          definitions will be silently ignored.
        - If 'strict', then any unmapped users/groups will result in errors.

        Example: access_control='replace,loose'

    override_max_interp : bool, default False
        If True, then the Maximum Interpolation overrides from the source
        system will be written to the destination system.

    include_inventory : bool, optional
        If True, then all calculated items that are scoped to the workbook
        will be pushed as well.

        If omitted, SPy will push inventory in any non-template scenarios.

    include_annotations : bool, default True
        If True, downloads the HTML for Journal and Organizer Topic content.

    refresh : bool, default True
        If True, then the Workbook objects that were supplied as input will
        be updated to be "fresh" from the server after the push. All
        identifiers will reflect the actual pushed IDs. Since refreshing
        takes time, you can set this to False if you don't plan to make
        further modifications to the Workbook objects or use the new IDs.

    lookup_df : pd.DataFrame, optional
        A DataFrame of item metadata that can be used to look up identifiers
        that correspond to template parameters (when pushing AnalysisTemplate
        objects). This argument is automatically specified by the spy.push()
        function.

    specific_worksheet_ids : List[str], default None
        If supplied, only the worksheets with IDs specified in the supplied
        list will be pushed. This should be used when it would otherwise take
        too long to push all worksheets and you're interested in optimizing
        the push operation. No existing worksheets will be archived or moved
        in their ordering.

    create_dummy_items_in_workbook : str, optional
        If specified, then "dummy" items will be created for any stored items
        that are not successfully mapped to the target system. This is useful
        when you want to push a workbook to a system that doesn't have the same
        datasources/items as the source system. The dummy items will be
        created in the target system's "Seeq Data Lab" datasource and will
        have the same name as the original item, and no data. They will be
        scoped to the workbook specified.

    assume_dependencies_exist : bool, default False
        If True, then the push operation will assume that any dependencies
        not found in the set of workbooks pushed are already present on the
        target server. This is useful, for example, when you are pushing a
        subset of workbooks (like a single Organizer Topic) and don't need
        or want to push all of the referenced Workbench Analyses.

    reconcile_inventory_by : {'id', 'name'}, default 'id'
        Determines how SPy correlates inventory items being pushed with items
        that may already exist on the server. The default value is 'id', which
        reconciles (via the Data ID property) using the original ID of the
        item (which may or may not exist depending on whether you're pushing to
        the same server and/or using a label). The other option is 'name',
        which reconciles using the fully-qualified name (FQN) of the item. (The
        FQN includes the Path, Asset and Name.) This option makes the pushed
        items "compatible" with the spy.push(metadata) function, which allows
        you to use spy.Tree() to make modifications to an asset tree.

    global_inventory : {'copy global', 'copy local', 'always reuse'}, default 'copy global'
        Determines how SPy handles global inventory items, especially in
        conjunction with a label argument. 'copy global' will cause different
        global items to be created if their labels differ. If no label is
        specified, the existing global item will be reused/updated if possible.
        'copy local' will scope the global items to the pushed workbook,
        making copies for different workbooks if necessary. 'always reuse' will
        use an existing global item if possible and ignore the label.

    item_map : dict
        A dictionary that "pre-maps" original IDs to new IDs. This is useful
        when you want to directly manage certain items and exclude them from the
        push. You can effectively tell SPy not to push/update an item just by
        adding a map where the original ID is the same as the new ID.

    errors : {'raise', 'catalog'}, default 'raise'
        If 'raise', any errors encountered will cause an exception. If
        'catalog', errors will be added to a 'Result' column in the status.df
        DataFrame (errors='catalog' must be combined with
        status=<Status object>).

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

    scope_globals_to_workbook : bool, default False
        Deprecated. Use global_inventory instead: 'copy global' is the
        equivalent of False, 'copy local' is the equivalent of True.

    Returns
    -------
    pandas.DataFrame
        A DataFrame with status on the items pushed. The IDs for the pushed
        workbooks are in the "Pushed Workbook ID" column.

        Additionally, the following properties are stored on the "spy"
        attribute of the output DataFrame:

        =================== ===================================================
        Property            Description
        =================== ===================================================
        func                A str value of 'spy.workbooks.push'
        kwargs              A dict with the values of the input parameters
                            passed to this function
        input               The set of workbooks passed in to this function
        output              The set of workbooks pulled from the server if
                            refresh=True
        datasource          The datasource that all pushed items will fall
                            under (as a DatasourceOutputV1 object).
        item_map            A dictionary of IDs that map from the input
                            workbooks and inventory to the actual IDs as they
                            were created/updated on the server
        status              A spy.Status object with the status of the
                            spy.push call
        =================== ===================================================
    """
    input_args = _common.validate_argument_types([
        (workbooks, 'workbooks', (Workbook, list)),
        (path, 'path', str),
        (owner, 'owner', str),
        (label, 'label', str),
        (datasource, 'datasource', str),
        (datasource_map_folder, 'datasource_map_folder', str),
        (use_full_path, 'use_full_path', bool),
        (access_control, 'access_control', str),
        (override_max_interp, 'override_max_interp', bool),
        (include_inventory, 'include_inventory', bool),
        (include_annotations, 'include_annotations', bool),
        (refresh, 'refresh', bool),
        (lookup_df, 'lookup_df', pd.DataFrame),
        (specific_worksheet_ids, 'specific_worksheet_ids', list),
        (create_dummy_items_in_workbook, 'create_dummy_items_in_workbook', str),
        (assume_dependencies_exist, 'assume_dependencies_exist', bool),
        (reconcile_inventory_by, 'reconcile_inventory_by', str),
        (global_inventory, 'global_inventory', str),
        (errors, 'errors', str),
        (quiet, 'quiet', bool),
        (status, 'status', Status),
        (session, 'session', Session),
        (scope_globals_to_workbook, 'scope_globals_to_workbook', bool),
        (item_map, 'item_map', (dict, ItemMap))
    ])

    session = Session.validate(session)
    status = Status.validate(status, session, quiet, errors)
    _login.validate_login(session, status)

    if path == _folder.ORIGINAL_FOLDER and not use_full_path:
        raise SPyValueError('You must specify use_full_path=True when path=spy.workbooks.ORIGINAL_FOLDER')

    if path == _folder.ORIGINAL_FOLDER and not session.user.is_admin:
        raise SPyValueError('Must be an admin to use path=spy.workbooks.ORIGINAL_FOLDER')

    if owner == _user.ORIGINAL_OWNER and not session.user.is_admin:
        raise SPyValueError('Must be an admin to use owner=spy.workbooks.ORIGINAL_OWNER')

    if path == _folder.ORIGINAL_FOLDER and owner not in (_user.ORIGINAL_OWNER, _user.FORCE_ME_AS_OWNER):
        raise SPyValueError(
            'You must specify owner=spy.workbooks.ORIGINAL_OWNER or owner=spy.workbooks.FORCE_ME_AS_OWNER to use '
            'path=spy.workbooks.ORIGINAL_FOLDER')

    if label is not None and ('[' in label or ']' in label):
        raise SPyValueError('label argument cannot contain square brackets []')

    if reconcile_inventory_by not in ['id', 'name']:
        raise SPyValueError('reconcile_inventory_by must be either "id" or "name"')

    if global_inventory is not None:
        if global_inventory not in ['copy global', 'copy local', 'always reuse']:
            raise SPyValueError('global_inventory must be either "copy global", "copy local", or "always reuse"')

        if scope_globals_to_workbook is not None:
            raise SPyValueError('scope_globals_to_workbook argument cannot be combined with global_inventory. Use '
                                'global_inventory only.')
    else:
        if scope_globals_to_workbook is not None:
            if scope_globals_to_workbook:
                status.warn('scope_globals_to_workbook=True is deprecated. Use global_inventory="copy local" instead.')
                global_inventory = 'copy local'
            else:
                status.warn(
                    'scope_globals_to_workbook=False is deprecated. Use global_inventory="copy global" instead.')
                global_inventory = 'copy global'
        else:
            global_inventory = 'copy global'

    if isinstance(item_map, dict):
        for k, v in item_map.values():
            if not _common.is_guid(k) or not _common.is_guid(v):
                raise SPyValueError('item_map keys and values must be IDs')
        item_map = ItemMap(item_map)

    owner_identity = None
    if owner not in [None, _user.ORIGINAL_OWNER, _user.FORCE_ME_AS_OWNER]:
        owner_identity = _login.find_user(session, owner)
        owner = owner_identity.id

    status.update('Pushing workbooks', Status.RUNNING)

    if item_map is None:
        item_map = ItemMap(lookup_df=lookup_df)

    if not isinstance(workbooks, list):
        workbooks = [workbooks]

    # Make sure the datasource exists
    datasource_output = _metadata.create_datasource(session, datasource)

    workbook_context: Optional[WorkbookContext] = None
    if create_dummy_items_in_workbook is not None:
        workbook_context = WorkbookContext.from_args(session, status, create_dummy_items_in_workbook, None, datasource)

    # Sort such that Analyses are pushed before Topics, since the latter usually depends on the former
    remaining_workbooks = list()
    sorted_workbooks = sorted(list(workbooks), key=lambda w: w['Workbook Type'])
    status.df = pd.DataFrame(columns=['ID', 'Name', 'Type', 'Workbook Type', 'Count', 'Time', 'Errors', 'Result'])
    for index in range(len(sorted_workbooks)):  # type: int
        workbook = sorted_workbooks[index]
        remaining_workbooks.append((index, workbook))
        status.df.at[index, 'ID'] = workbook.id if workbook.id else np.nan
        status.df.at[index, 'Name'] = workbook.name
        status.df.at[index, 'Type'] = workbook.type
        status.df.at[index, 'Workbook Type'] = workbook['Workbook Type']
        status.df.at[index, 'Count'] = 0
        status.df.at[index, 'Time'] = datetime.timedelta(0)
        status.df.at[index, 'Errors'] = 0
        status.df.at[index, 'Result'] = 'Queued'

    datasource_map_overrides: DatasourceMapList = DatasourceMapList()
    if datasource_map_folder:
        datasource_map_overrides = Workbook.load_datasource_maps(datasource_map_folder, overrides=True)

    folder_id = _create_folder_path_if_necessary(session, path, status)

    context = WorkbookPushContext(
        access_control=access_control,
        datasource=datasource,
        dummy_items_workbook_context=workbook_context,
        include_annotations=include_annotations,
        override_max_interp=override_max_interp,
        owner=owner,
        reconcile_inventory_by=reconcile_inventory_by,
        global_inventory=global_inventory,
        session=session,
        specific_worksheet_ids=specific_worksheet_ids,
        status=status
    )

    at_least_one_thing_pushed = False
    while len(remaining_workbooks) > 0:
        at_least_one_thing_pushed_this_iteration = False

        dependencies_not_found = list()
        for index, workbook in remaining_workbooks.copy():  # type: (int, Workbook)
            if not isinstance(workbook, Workbook):
                raise SPyRuntimeError('"workbooks" argument contains a non Workbook item: %s' % workbook)

            if isinstance(workbook, ItemTemplate) and label is not None:
                raise SPyValueError('Cannot specify a label when pushing a Template workbook')

            try:
                status.reset_timer()

                status.current_df_index = index
                status.put('Count', 0)
                status.put('Time', datetime.timedelta(0))
                status.put('Result', 'Pushing')

                if label is None:
                    # If a label is not supplied, check to see if we should be automatically isolating by user.
                    isolate_by_user = False
                    if _common.get(workbook.definition, 'Isolate By User', default=False):
                        # Workbooks can be marked as 'Isolate By User' so they're not stepping on each other when
                        # they do spy.workbook.push(). All of the Example Exports are marked with "Isolate By User"
                        # equal to True.
                        isolate_by_user = True

                    if workbook.provenance == Item.CONSTRUCTOR:
                        # If a Workbook is constructed (and not persisted -- i.e., not pulled/loaded) then the best
                        # policy is to isolate such workbooks so that their Data IDs can't collide in the event that
                        # two users happen to name their workbooks the same.
                        isolate_by_user = True

                    if isolate_by_user:
                        label = owner_identity.username if owner_identity is not None else session.user.username

                if not isinstance(workbook, ItemTemplate):
                    workbook.datasource_maps.extend(datasource_map_overrides.copy(), overwrite=True)

                try:
                    include_inventory_for_this_workbook = (not isinstance(workbook, ItemTemplate)
                                                           if include_inventory is None else include_inventory)

                    workbook_folder_id = workbook.push_containing_folders(session, item_map, datasource_output,
                                                                          use_full_path, folder_id, owner, label,
                                                                          access_control, status)

                    # Grab the success message now because already_pushed will always be true after the push
                    success_message = 'Success'
                    if workbook.already_pushed:
                        success_message += ': Already pushed'

                    try:
                        workbook.push(context=context, folder_id=workbook_folder_id, item_map=item_map, label=label,
                                      include_inventory=include_inventory_for_this_workbook)
                    except SPyException as e:
                        status.raise_or_put(e, 'Result')
                        continue

                    at_least_one_thing_pushed = True
                    at_least_one_thing_pushed_this_iteration = True

                    remaining_workbooks.remove((index, workbook))

                    status.put('Time', status.get_timer())
                    status.put('Errors', len(workbook.push_errors))

                    if len(workbook.push_errors) > 0:
                        success_message += f', but with errors:\n{workbook.push_errors_str}'
                        status.put('Result', success_message)
                        if status.errors == 'raise':
                            raise SPyRuntimeError(workbook.push_errors_str)
                    else:
                        status.put('Result', success_message)

                except SPyDependencyNotFound as e:
                    status.put('Count', 0)
                    status.put('Time', datetime.timedelta(0))
                    status.put('Errors', 0)
                    status.put('Result', f'Need dependency: {str(e)}')

                    dependencies_not_found.append(str(e))

            except ApiException as e:
                raise SPyRuntimeError(_common.format_exception(e)) from e

        if not at_least_one_thing_pushed_this_iteration:
            if assume_dependencies_exist and not item_map.fall_through:
                item_map.fall_through = True
                continue

            if status.errors == 'raise':
                raise SPyRuntimeError('Could not find the following dependencies:\n%s\n'
                                      'Therefore, could not import the following workbooks:\n%s\n' %
                                      ('\n'.join(dependencies_not_found),
                                       '\n'.join([str(workbook) for _, workbook in remaining_workbooks])))

            break

    Annotation.push_fixups(session, status, item_map)

    new_workbooks = None
    if refresh and at_least_one_thing_pushed:
        refresh_workbook_inner_status = status.create_inner('Refresh Workbook')

        new_workbooks = WorkbookList()
        for workbook in workbooks:
            new_workbook_id = item_map[workbook.id]

            specific_worksheet_ids_to_pull = None
            if specific_worksheet_ids is not None:
                specific_worksheet_ids_to_pull = [item_map[ws] for ws in specific_worksheet_ids]

            include_inventory_for_refresh = include_inventory if include_inventory is not None else True
            pulled_workbooks = _pull.pull(new_workbook_id, include_inventory=include_inventory_for_refresh,
                                          specific_worksheet_ids=specific_worksheet_ids_to_pull,
                                          include_annotations=include_annotations, status=refresh_workbook_inner_status,
                                          session=session)

            new_workbook = pulled_workbooks[0]
            new_workbooks.append(new_workbook)

            if isinstance(workbook, ItemTemplate):
                continue

            workbook.refresh_from(new_workbook, item_map, refresh_workbook_inner_status,
                                  include_inventory=include_inventory_for_refresh,
                                  specific_worksheet_ids=specific_worksheet_ids)
            if folder_id is not None and folder_id != _common.PATH_ROOT and folder_id != _folder.ORIGINAL_FOLDER:
                workbook['Search Folder ID'] = folder_id

    max_errors = status.df['Errors'].max()
    if max_errors > 0:
        status.update('Errors encountered, look at Result column in returned DataFrame', Status.FAILURE)
    else:
        status.update('Push successful', Status.SUCCESS)

    output_df_properties = types.SimpleNamespace(
        func='spy.workbooks.push',
        kwargs=input_args,
        input=workbooks,
        output=new_workbooks,
        datasource=datasource_output,
        item_map=item_map,
        status=status)

    output_df = status.df.copy()

    _common.put_properties_on_df(output_df, output_df_properties)

    return output_df


def _create_folder_path_if_necessary(session: Session, path, status: Status):
    if path == _folder.ORIGINAL_FOLDER:
        return _folder.ORIGINAL_FOLDER

    if _common.is_guid(path):
        return path

    folders_api = FoldersApi(session.client)

    if path is None:
        return None

    path = path.strip()

    if not path:
        return None

    if path == _folder.MY_FOLDER:
        return folders_api.get_folder(folder_id='mine').id

    workbook_path = _common.path_string_to_list(path)

    parent_id = None
    folder_id = None
    folder_filter = 'owner'
    for i in range(0, len(workbook_path)):
        existing_content_id = None
        content_name = workbook_path[i]

        if content_name in [_folder.SHARED, _folder.ALL, _folder.USERS]:
            raise SPyRuntimeError(f'"path" argument cannot contain {content_name} folder in "{path}"')

        if content_name == _folder.CORPORATE:
            if not session.corporate_folder:
                raise SPyRuntimeError(f'Attempting to push to Corporate folder but user does not have access')

            parent_id = session.corporate_folder.id
            folder_id = session.corporate_folder.id
            folder_filter = 'corporate'
            continue

        kwargs = {
            'filter': folder_filter,
            'types': [SeeqNames.Types.folder],
            'text_search': content_name,
            'is_exact': True,
            'limit': session.options.search_page_size,
        }
        if parent_id:
            kwargs['folder_id'] = parent_id
        folders = safely(lambda: folders_api.get_folders(**kwargs),
                         action_description=f'get Folders using filter "{folder_filter}" and name "{content_name}" '
                                            f'within {parent_id}',
                         additional_errors=[400],
                         status=status)  # type: WorkbenchItemOutputListV1

        if folders is not None:
            for content in folders.content:  # type: WorkbenchSearchResultPreviewV1
                if content.type == 'Folder' and content_name == content.name:
                    if (parent_id is not None and content.ancestors is not None and len(content.ancestors) >= 1
                            and content.ancestors[-1].id != parent_id):
                        continue
                    existing_content_id = content.id
                    break

        if not existing_content_id:
            folder_input = FolderInputV1()
            folder_input.name = content_name
            folder_input.description = 'Created by Seeq Data Lab'
            folder_input.owner_id = session.user.id
            folder_input.parent_folder_id = parent_id
            folder_output = safely(lambda: folders_api.create_folder(body=folder_input),
                                   action_description=f'create Folder {folder_input.name}',
                                   status=status)  # type: FolderOutputV1
            if folder_output is not None:
                existing_content_id = folder_output.id

        parent_id = existing_content_id
        folder_id = existing_content_id

    return folder_id
