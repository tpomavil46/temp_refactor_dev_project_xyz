from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import Callable, Dict, Generator, List, Optional, Set, Union

import pandas as pd

from seeq import spy
from seeq.spy import _common
from seeq.spy import _login
from seeq.spy._errors import *
from seeq.spy._session import Session
from seeq.spy._status import Status
from seeq.spy.workbooks._template import package_as_templates
from seeq.spy.workbooks._user import User, UserGroup
from seeq.spy.workbooks._workbook import Workbook, WorkbookList


@Status.handle_keyboard_interrupt()
def pull(workbooks_df: Union[pd.DataFrame, str], *, include_referenced_workbooks: bool = True,
         include_inventory: bool = True, include_annotations: bool = True, include_images: bool = True,
         include_rendered_content: bool = False, include_access_control: bool = True,
         include_archived: bool = True, specific_worksheet_ids: Optional[List[str]] = None,
         as_template_with_label: str = None, errors: Optional[str] = None, quiet: Optional[bool] = None,
         status: Optional[Status] = None, session: Optional[Session] = None) -> Optional[WorkbookList]:
    """
    Pulls the definitions for each workbook specified by workbooks_df into
    memory as a list of Workbook items.

    Parameters
    ----------
    workbooks_df : {str, pd.DataFrame}
        A DataFrame containing 'ID', 'Type' and 'Workbook Type' columns that
        can be used to identify the workbooks to pull. This is usually created
        via a call to spy.workbooks.search(). Alternatively, you can supply a
        workbook ID directly as a str or the URL of a Seeq Workbench worksheet
        as a str.

    include_referenced_workbooks : bool, default True
        If True, Analyses that are depended upon by Topics will be
        automatically included in the resulting list even if they were not part
        of the workbooks_df DataFrame.

    include_inventory : bool, default True
        If True, all items that are referenced in worksheets, journals, topics
        etc are included in the Workbook object's "inventory", along with
        anything that is scoped to the workbook.

    include_annotations : bool, default True
        If True, downloads the HTML for Journal and Organizer Topic content.

    include_images : bool, default True
        If True, downloads all static images (not including embedded content --
        see include_rendered_content for that).

    include_rendered_content : bool, default False
        If True, any Organizer Topics pulled will include rendered content
        images, which will cause spy.workbooks.save() to include a folder for
        each Topic Document with its HTML and images such that it can be loaded
        and viewed in a browser.

    include_access_control : bool, default True
        If True, includes the access control information for the workbook and
        all worksheets.

    include_archived : bool, default True
        If True, includes trashed/archived items and content. If False, no
        trashed/archived items or content are included which means that any
        worksheets or documents that referred to such items may not function.

    specific_worksheet_ids : List[str], default None
        If supplied, only the worksheets with IDs specified in the supplied
        list will be pulled. This should be used when it would otherwise take
        too long to pull all worksheets and you're only interested in a small
        subset. Be careful not to push the resulting workbook(s) back because
        the other worksheets will get archived.

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
        command is executed. The table itself is accessible as a DataFrame via
        the status.df property.

    session : spy.Session, optional
        If supplied, the Session object (and its Options) will be used to
        store the login session state. This is useful to log in to different
        Seeq servers at the same time or with different credentials.

    """
    _common.validate_argument_types([
        (workbooks_df, 'workbooks_df', (pd.DataFrame, str)),
        (include_referenced_workbooks, 'include_referenced_workbooks', bool),
        (include_inventory, 'include_inventory', bool),
        (include_annotations, 'include_annotations', bool),
        (include_images, 'include_images', bool),
        (include_rendered_content, 'include_rendered_content', bool),
        (include_access_control, 'include_access_control', bool),
        (include_archived, 'include_archived', bool),
        (specific_worksheet_ids, 'specific_worksheet_ids', list),
        (as_template_with_label, 'as_template_with_label', str),
        (errors, 'errors', str),
        (quiet, 'quiet', bool),
        (status, 'status', Status),
        (session, 'session', Session)
    ])

    session = Session.validate(session)
    status = Status.validate(status, session, quiet, errors)
    _login.validate_login(session, status)

    workbooks = WorkbookList(do_pull(workbooks_df, status=status, session=session,
                                     include_referenced_workbooks=include_referenced_workbooks,
                                     include_inventory=include_inventory, include_annotations=include_annotations,
                                     include_images=include_images, include_rendered_content=include_rendered_content,
                                     include_access_control=include_access_control, include_archived=include_archived,
                                     specific_worksheet_ids=specific_worksheet_ids))

    if as_template_with_label is not None:
        workbooks = package_as_templates(workbooks, as_template_with_label)

    return workbooks


@dataclass
class WorkbookPullInfo:
    index: int
    workbook_type: str
    search_folder_id: Optional[str]
    worksteps: Set[str]


def do_pull(workbooks: Union[pd.DataFrame, str], *, status: Status, session: Session,
            include_referenced_workbooks: bool = True, include_inventory: bool = True, include_annotations: bool = True,
            include_images: bool = True, include_rendered_content: bool = False, include_access_control: bool = True,
            include_archived: bool = None, specific_worksheet_ids: Optional[List[str]] = None,
            preprocess_callback: Optional[Callable[[str], Optional[Workbook]]] = None
            ) -> Generator[Workbook]:
    workbooks_df: Optional[pd.DataFrame] = None
    if isinstance(workbooks, str):
        if _common.is_guid(workbooks):
            workbook_id = workbooks
        else:
            url = workbooks
            workbook_id = spy.utils.get_workbook_id_from_url(url)

        # If workbooks_df is a URL, get the actual items from the worksheet and overwrite workbooks_df as a DataFrame
        workbooks_df = spy.workbooks.search({'ID': workbook_id},
                                            status=status.create_inner('Workbook URL Search', quiet=True),
                                            session=session)
        if len(workbooks_df) == 0:
            raise SPyValueError(f'"{workbooks_df}" is not a valid workbook ID or URL')
    elif isinstance(workbooks, pd.DataFrame):
        workbooks_df = workbooks

    _common.validate_unique_dataframe_index(workbooks_df, 'workbooks_df')

    if len(workbooks_df) == 0:
        status.update('workbooks_df is empty', Status.SUCCESS)
        return

    for required_column in ['ID', 'Type', 'Workbook Type']:
        if required_column not in workbooks_df.columns:
            raise SPyValueError('"%s" column must be included in workbooks_df' % required_column)

    workbooks_df = workbooks_df[workbooks_df['Type'] == 'Workbook'].reset_index(drop=True)

    status.df = _initialize_status_df(workbooks_df)

    status.update('Pulling workbooks', Status.RUNNING)

    workbooks_to_pull: Dict[str, Union[WorkbookPullInfo, Workbook]] = {_common.get(row, 'ID'): WorkbookPullInfo(
        index=index,
        workbook_type=_common.get(row, 'Workbook Type'),
        search_folder_id=_common.get(row, 'Search Folder ID'),
        worksteps=set()
    ) for index, row in workbooks_df.iterrows()}

    workbooks_pulled: Set[str] = set()

    item_cache = _common.LRUCache()

    # Process Topics first so that we can discover Analyses and Worksteps (of embedded content) to add to our list (
    # assuming include_referenced_workbooks is True). Note that it is possible for Analyses to reference Topics (via
    # a link in a Journal) and they will not be properly included in the output. We have accepted this hole because
    # otherwise if we add a Topic to our list and that Topic refers to a workstep in an already-processed Analysis,
    # we would have to go back and re-pull the Analysis. (Maybe someday we'll do that, but not now. :-) )
    for phase in ['Topic', 'Analysis']:
        while True:
            at_least_one_item_pulled = False
            for item_id, pull_info in workbooks_to_pull.copy().items():
                try:
                    if item_id in workbooks_pulled:
                        # Already pulled
                        continue

                    if pull_info.workbook_type != phase:
                        continue

                    status.reset_timer()
                    status.current_df_index = pull_info.index
                    status.put('Result', 'Pulling')

                    already_pulled: bool = False
                    workbook: Optional[Workbook] = None
                    if preprocess_callback is not None:
                        workbook = preprocess_callback(item_id)

                    if workbook is None:
                        workbook = Workbook.pull(item_id,
                                                 extra_workstep_tuples=pull_info.worksteps,
                                                 specific_worksheet_ids=specific_worksheet_ids,
                                                 include_inventory=include_inventory,
                                                 include_annotations=include_annotations,
                                                 include_images=include_images,
                                                 include_access_control=include_access_control,
                                                 include_archived=include_archived,
                                                 item_cache=item_cache,
                                                 status=status,
                                                 session=session)  # type: Workbook

                        if include_rendered_content:
                            status.put('Result', 'Pulling rendered content')
                            workbook.pull_rendered_content(
                                status=status.create_inner(f'Pull Embedded Content {workbook}', quiet=True),
                                session=session)
                    else:
                        status.put('Errors', len(workbook.pull_errors))
                        status.update('[%d/%d] Already pulled %s "%s"' %
                                      (len(status.df[status.df['Result'] != 'Queued']),
                                       len(status.df), workbook['Workbook Type'], workbook['Name']),
                                      Status.RUNNING)

                        already_pulled = True

                    if workbook is None:
                        continue

                    # The "Search Folder ID" is a means by which we can establish "relative paths" like a file
                    # system. The idea is that whatever folder you specified for spy.workbooks.search() is probably
                    # the folder to serve as the "start" and all subfolders become relative to whatever folder is
                    # specified as the "start" during the spy.workbook.push() call. ("start" in this case is very
                    # similar to os.path.relpath()'s "start" argument.) If we didn't have this relative mechanism, it
                    # would be difficult to take a folder full of stuff and duplicate it or put it in a new/different
                    # location.
                    if pull_info.search_folder_id:
                        workbook['Search Folder ID'] = pull_info.search_folder_id
                    elif len(workbook['Ancestors']) > 0:
                        # If there was no Search Folder ID in the pull_info BUT there are Ancestors, that means this
                        # workbook is being pulled without having done spy.workbook.search() first. If so, then make
                        # the closest ancestor folder its "start", that way when spy.workbooks.push() comes around,
                        # it won't be trying to push any containing folders.
                        workbook['Search Folder ID'] = workbook['Ancestors'][-1]

                    if include_referenced_workbooks:
                        for workbook_id, workstep_tuples in workbook.referenced_workbooks.items():
                            _add_if_necessary(workbook_id, workstep_tuples, workbooks_to_pull, workbooks_pulled, phase,
                                              pull_info, status, session)

                        for workbook_id, workstep_tuples in \
                                workbook.find_workbook_links(session, status).items():
                            _add_if_necessary(workbook_id, workstep_tuples, workbooks_to_pull, workbooks_pulled, phase,
                                              pull_info, status, session)

                    yield workbook

                    workbooks_pulled.add(item_id)
                    at_least_one_item_pulled = True

                    status.put('Time', status.get_timer())
                    status.put('Errors', len(workbook.pull_errors))

                    success_message = 'Success' + (': Already pulled' if already_pulled else '')
                    if len(workbook.pull_errors) > 0:
                        success_message += f', but with errors:\n{workbook.pull_errors_str}'
                        status.put('Result', success_message)
                        if status.errors == 'raise':
                            raise SPyRuntimeError(workbook.pull_errors_str)
                    else:
                        status.put('Result', success_message)

                except ApiException as e:
                    raise SPyRuntimeError(_common.format_exception(e)) from e

            if not at_least_one_item_pulled:
                break

    max_errors = status.df['Errors'].max()
    if max_errors > 0:
        status.update('Errors encountered, look at Result column in returned DataFrame', Status.FAILURE)
    else:
        status.update('Pull successful', Status.SUCCESS)

    # Clear all the LRU caches so that, if seldom-changing objects (like Users/Groups) happen to change, we'll pick up
    # the change next time this function is run. This affects all threads, but that's OK because we don't encourage
    # multiple workbook push/pull operations from multiple threads and if the user happens to do it, it'll just slow
    # things down (rather than cause any errors).
    User.pull.cache_clear()
    UserGroup.pull.cache_clear()


def _initialize_status_df(initial_df):
    status_columns = list()

    for col in ['ID', 'Path', 'Name', 'Workbook Type']:
        if col in initial_df:
            status_columns.append(col)

    status_df = initial_df[status_columns].copy().reset_index(drop=True)
    status_df['Count'] = 0
    status_df['Time'] = datetime.timedelta(0)
    status_df['Errors'] = 0
    status_df['Result'] = 'Queued'
    return status_df


def _add_if_necessary(workbook_id, workstep_tuples, workbooks_to_pull, workbooks_pulled, phase, pull_info, status,
                      session):
    if workbook_id not in workbooks_to_pull:
        search_df = spy.workbooks.search({'ID': workbook_id},
                                         status=status.create_inner(
                                             f'Find Workbook {workbook_id}', quiet=True),
                                         session=session)
        if len(search_df) == 0:
            # Workbook with this ID not found, probably as a result of a tool making an
            # invalid link in a Journal or Topic Document.
            return

        workbook_type_to_add = _common.get(search_df.iloc[0], 'Workbook Type')
        if phase == 'Analysis' and workbook_type_to_add == 'Topic':
            # We can't process more Topics once we're in the Analysis phase. (See comment
            # above the "for phase" loop.)
            return

        workbooks_to_pull[workbook_id] = WorkbookPullInfo(
            index=len(status.df),
            workbook_type=workbook_type_to_add,
            search_folder_id=pull_info.search_folder_id,
            worksteps=set()
        )

        to_add_df = _initialize_status_df(search_df)
        status.df = pd.concat([status.df, to_add_df], ignore_index=True)

    if workbook_id in workbooks_pulled:
        # We've already pulled it, that ship has sailed. This can happen when there are a
        # web of links between Topics / Journals. We only do a "best effort" to make the
        # links work.
        return

    workbooks_to_pull[workbook_id].worksteps.update(workstep_tuples)
