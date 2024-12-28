from __future__ import annotations

import json
import os
import pickle
import types
from typing import List, Optional

import pandas as pd

from seeq import spy
from seeq.base import util
from seeq.spy import _common
from seeq.spy import _login
from seeq.spy._errors import *
from seeq.spy._session import Session
from seeq.spy._status import Status
from seeq.spy.workbooks._context import WorkbookPushContext
from seeq.spy.workbooks._item import Item
from seeq.spy.workbooks._item_map import ItemMap
from seeq.spy.workbooks._workbook import Workbook
from seeq.spy.workbooks.job import _pull


@Status.handle_keyboard_interrupt()
def push(job_folder, *, resume: bool = True, path: str = None, owner: str = None, label: str = None, datasource=None,
         use_full_path: bool = False, access_control: str = None, override_max_interp: bool = False,
         global_inventory: Optional[str] = None, create_dummy_items: bool = False,
         errors: Optional[str] = None, quiet: Optional[bool] = None,
         status: Optional[Status] = None, session: Optional[Session] = None,
         scope_globals_to_workbook: bool = False) -> pd.DataFrame:
    """
    Pushes the definitions for each workbook that was pulled by the
    spy.workbooks.job.pull() function, in a restartable "job"-like fashion.

    Parameters
    ----------
    job_folder : {str}
        A full or partial path to the job folder created by
        spy.workbooks.job.pull().

    resume : bool, default True
        True if the push should resume from where it left off, False if it
        should push everything again.

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
        original owner of the pulled content.

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

    global_inventory : {'copy global', copy local', 'always reuse'}, default 'copy global'
        Determines how SPy handles global inventory items, especially in
        conjunction with a label argument. 'copy global' will cause different
        global items to be created if their labels differ. If no label is
        specified, the existing global item will be reused/updated if possible.
        'copy local' will scope the global items to the pushed workbook,
        making copies for different workbooks if necessary. 'always reuse' will
        use an existing global item if possible and ignore the label.

    create_dummy_items : bool, default False
        If true, then "dummy" items will be created for any stored items
        that are not successfully mapped to the target system. This should
        be specified as True if you intend to use the spy.workbooks.job.data
        module. The dummy items will be created in the target system's
        "Seeq Data Lab" datasource and will have the same name as the original
        item, and no data. They will be scoped to a workbook under a
        "SPy Workbook Jobs" folder and named after the job folder.

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
        command is executed. The table itself is accessible as a DataFrame via
        the status.df property.

    session : spy.Session, optional
        If supplied, the Session object (and its Options) will be used to
        store the login session state. This is useful to log in to different
        Seeq servers at the same time or with different credentials.

    scope_globals_to_workbook : bool, default False
        Deprecated. Use global_inventory instead: 'copy global' is the
        equivalent of False, 'copy local' is the equivalent of True.

    """
    input_args = _common.validate_argument_types([
        (job_folder, 'job_folder', str),
        (resume, 'resume', bool),
        (path, 'path', str),
        (owner, 'owner', str),
        (label, 'label', str),
        (datasource, 'datasource', str),
        (use_full_path, 'use_full_path', bool),
        (access_control, 'access_control', str),
        (override_max_interp, 'override_max_interp', bool),
        (global_inventory, 'global_inventory', str),
        (create_dummy_items, 'create_dummy_items', bool),
        (errors, 'errors', str),
        (quiet, 'quiet', bool),
        (status, 'status', Status),
        (session, 'session', Session),
        (scope_globals_to_workbook, 'scope_globals_to_workbook', bool)
    ])

    session = Session.validate(session)
    status = Status.validate(status, session, quiet, errors)
    _login.validate_login(session, status)

    if not util.safe_exists(job_folder):
        raise SPyValueError(f'Job folder "{job_folder}" does not exist.')

    item_map = ItemMap()
    if resume:
        item_map = load_item_map(job_folder)

    item_map.only_override_maps = True

    workbook_list: List[WorkbookFolderRef] = list()
    job_workbooks_folder = _pull.get_workbooks_folder(job_folder)
    for workbook_folder, _ in _pull.walk_workbook_folders(job_workbooks_folder):
        completely_pushed_filename = os.path.join(job_workbooks_folder, workbook_folder, 'Completely Pushed')
        if not resume and util.safe_exists(completely_pushed_filename):
            util.safe_remove(completely_pushed_filename)
        workbook_list.append(WorkbookFolderRef(os.path.join(job_workbooks_folder, workbook_folder), job_folder))

    job_datasource_maps_folder = _pull.get_datasource_maps_folder(job_folder)
    if not util.safe_exists(job_datasource_maps_folder):
        job_datasource_maps_folder = None

    create_dummy_items_in_workbook = get_dummy_workbook_name(job_folder) if create_dummy_items else None

    spy.workbooks.push(workbook_list, path=path, owner=owner, label=label, datasource=datasource,
                       use_full_path=use_full_path, access_control=access_control,
                       override_max_interp=override_max_interp, global_inventory=global_inventory,
                       datasource_map_folder=job_datasource_maps_folder, refresh=False,
                       create_dummy_items_in_workbook=create_dummy_items_in_workbook,
                       status=status, session=session, item_map=item_map,
                       scope_globals_to_workbook=scope_globals_to_workbook)

    results_df = status.df.copy()

    results_df_properties = types.SimpleNamespace(
        func='spy.workbooks.job.push',
        kwargs=input_args,
        status=status,
        item_map=item_map
    )

    _common.put_properties_on_df(results_df, results_df_properties)

    return results_df


def get_item_map_filename(job_folder):
    return os.path.join(job_folder, 'item_map.pickle')


def load_item_map(job_folder):
    item_map_file = get_item_map_filename(job_folder)
    if not util.safe_exists(item_map_file):
        return ItemMap()

    with util.safe_open(item_map_file, 'rb') as f:
        return pickle.load(f)


def save_item_map(job_folder, item_map):
    item_map_file = get_item_map_filename(job_folder)
    with util.safe_open(item_map_file, 'wb') as f:
        pickle.dump(item_map, f, protocol=4)


def get_dummy_workbook_name(job_folder):
    return f'SPy Dummy Workbooks >> Dummy Workbook for {job_folder}'


def redo(job_folder: str, status: Status):
    job_workbooks_folder = _pull.get_workbooks_folder(job_folder)
    workbook_folders = {i: f for f, i in _pull.walk_workbook_folders(job_workbooks_folder)}
    workbook_ids: pd.Series = status.df['ID']
    for index, workbook_id in workbook_ids.items():
        if workbook_id in workbook_folders:
            completely_pushed_filename = os.path.join(
                job_workbooks_folder, workbook_folders[workbook_id], 'Completely Pushed')
            if util.safe_exists(completely_pushed_filename):
                util.safe_remove(completely_pushed_filename)
            status.df.at[index, 'Result'] = 'Push will be redone'
        else:
            status.df.at[index, 'Result'] = 'Not found'


class WorkbookFolderRef(Workbook):
    workbook_folder: str
    _real_workbook: Optional[Workbook]
    _parent_folder_id: Optional[str]

    def __init__(self, workbook_folder, job_folder):
        super().__init__()

        self.workbook_folder = workbook_folder
        self.job_folder = job_folder
        self._provenance = Item.LOAD
        self._real_workbook = None
        self._parent_folder_id = None

        with util.safe_open(os.path.join(self.workbook_folder, 'Workbook.json'), 'r', encoding='utf-8') as f:
            self._definition = json.load(f)

    def _already_pushed_filename(self):
        return os.path.join(self.workbook_folder, 'Completely Pushed')

    @property
    def already_pushed(self):
        return util.safe_exists(self._already_pushed_filename())

    def _ensure_loaded(self):
        if self._real_workbook is not None:
            return

        self._real_workbook = Workbook.load(self.workbook_folder)
        self._real_workbook.datasource_maps = self.datasource_maps

    def push_containing_folders(self, session: Session, item_map: ItemMap, datasource_output, use_full_path,
                                parent_folder_id, owner, label, access_control, status: Status):
        if self.already_pushed:
            with util.safe_open(self._already_pushed_filename(), 'r') as f:
                d = json.load(f)
            self._parent_folder_id = d['Parent Folder ID']
            return self._parent_folder_id

        self._ensure_loaded()
        self._parent_folder_id = self._real_workbook.push_containing_folders(
            session, item_map, datasource_output, use_full_path, parent_folder_id, owner, label, access_control, status)

        return self._parent_folder_id

    def push(self, *, context: WorkbookPushContext, folder_id=None, item_map: ItemMap = None, label=None,
             include_inventory=True):
        if self.already_pushed:
            self._push_errors = Workbook.load_push_errors(self.workbook_folder)
            context.status.put('Pushed Workbook ID', item_map[self.id])
            link_url = Workbook.construct_url(
                context.session,
                folder_id,
                item_map[self.id]
            )
            context.status.put('URL', link_url)
            return

        self._real_workbook.push(context=context, folder_id=folder_id, item_map=item_map, label=label,
                                 include_inventory=include_inventory)

        save_item_map(self.job_folder, item_map)
        self._push_errors = self._real_workbook.push_errors
        Workbook.save_push_errors(self.workbook_folder, self._push_errors)
        if context.status.errors == 'catalog' or len(self._real_workbook.push_errors) == 0:
            with util.safe_open(self._already_pushed_filename(), 'w') as f:
                json.dump({'Parent Folder ID': self._parent_folder_id}, f)
