from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

from seeq import spy
from seeq.spy import _common
from seeq.spy._errors import *
from seeq.spy._session import Session
from seeq.spy._status import Status


@dataclass
class WorkbookContext:
    workbook_object: Optional[spy.workbooks.Analysis] = None
    worksheet_object: Optional[spy.workbooks.AnalysisWorksheet] = None
    folder_id: Optional[str] = None
    to_push: list = field(default_factory=list)
    item_map: spy.workbooks.ItemMap = None

    def _lookup(self, _id):
        if self.item_map is not None and _id in self.item_map:
            return self.item_map[_id]
        else:
            return _id

    @property
    def workbook_id(self):
        if self.workbook_object is None:
            return None

        return self._lookup(self.workbook_object.id)

    @property
    def worksheet_id(self):
        if self.worksheet_object is None:
            return None

        return self._lookup(self.worksheet_object.id)

    def determine_folder_id(self):
        # This is called deliberately at the top of the push function so that it's calculated once and then used
        # consistently throughout, even if the workbook_object changes.

        if self.workbook_object is None:
            return None

        self.folder_id = None
        if not isinstance(self.workbook_object, spy.workbooks.AnalysisTemplate):
            if (_common.present(self.workbook_object, 'Ancestors') and
                    len(self.workbook_object['Ancestors']) > 0):
                self.folder_id = self.workbook_object['Ancestors'][-1]

        # Here we cover the case where an admin is pushing data to a single workbook that they don't own. In R51,
        # there's a proper user folder in the path that can be used as the folder_id. In R50 there is no such user
        # folder, so the folder_id will end up being __All__ or __Shared__, which is not viable to use as a parent
        # folder. But in the case where we're pushing to only the primary workbook, then we don't need to supply the
        # folder_id because the workbook is not being moved and we're not adding other sibling workbooks (and therefore
        # we don't need to know the actual folder that the workbook is in).
        if (self.folder_id in spy.workbooks.SYNTHETIC_FOLDERS and len(self.to_push) == 1 and
                self.to_push[0] == self.workbook_object):
            self.folder_id = None

    def get_workbook_url(self, session: Session):
        if self.workbook_object is None:
            return None

        return '%s/%sworkbook/%s/worksheet/%s' % (
            session.public_url,
            (self.folder_id + '/') if self.folder_id is not None else '',
            self.workbook_id,
            self.worksheet_id
        )

    @staticmethod
    def from_id(session: Session, status: Status, workbook_arg, worksheet_arg) -> \
            WorkbookContext:
        primary_workbooks = spy.workbooks.pull(pd.DataFrame([{
            'ID': _common.sanitize_guid(workbook_arg),
            'Type': 'Workbook',
            'Workbook Type': 'Analysis'
        }]),
            include_inventory=False,
            # Pulling worksheets can be expensive. If we just want to push items without changing the UI, skip that.
            specific_worksheet_ids=list() if worksheet_arg is None else None,
            status=status.create_inner('Pull Workbook', quiet=True),
            session=session)

        if len(primary_workbooks) == 0 or primary_workbooks[0].__class__.__name__ == 'Workbook':
            raise SPyRuntimeError(f'Workbook with ID "{_common.sanitize_guid(workbook_arg)}" not found')

        # noinspection PyTypeChecker
        primary_workbook: spy.workbooks.Analysis = primary_workbooks[0]
        if isinstance(worksheet_arg, str):
            primary_worksheet = primary_workbook.worksheet(worksheet_arg)
        elif isinstance(workbook_arg, spy.workbooks.AnalysisWorksheetTemplate):
            primary_worksheet = workbook_arg
        else:
            primary_worksheet = None

        return WorkbookContext(workbook_object=primary_workbook, worksheet_object=primary_worksheet)

    @staticmethod
    def from_list(session: Session, status: Status, workbook_arg, datasource_arg):
        analysis_workbooks = [w for w in workbook_arg if w['Workbook Type'] == 'Analysis']
        if len(analysis_workbooks) == 0:
            raise SPyValueError(
                f'workbook argument is a WorkbookList that contains no Analysis workbooks. You must supply at '
                f'least one (and ideally only one, otherwise things can get confusing) Analysis workbook in the '
                f'list')

        primary_workbook = analysis_workbooks[0]

        if len(analysis_workbooks) > 1:
            status.warn(f'workbook argument is a WorkbookList that contains more than one Analysis workbook. As a '
                        f'result, the first workbook "{primary_workbook}" is the one to which '
                        f'signals/conditions/scalars (etc) will be pushed')

        push_workbooks_df = spy.workbooks.push(workbook_arg, datasource=datasource_arg, specific_worksheet_ids=list(),
                                               include_inventory=False,
                                               status=status.create_inner('Create Workbook', quiet=True),
                                               session=session)

        return WorkbookContext(workbook_object=primary_workbook, to_push=workbook_arg,
                               item_map=push_workbooks_df.spy.item_map)

    @staticmethod
    def from_analysis_template(session: Session, status: Status, workbook_arg, datasource_arg):
        push_workbooks_df = spy.workbooks.push(workbook_arg, datasource=datasource_arg, specific_worksheet_ids=list(),
                                               include_inventory=False,
                                               status=status.create_inner('Create Workbook', quiet=True),
                                               session=session)

        # noinspection PyTypeChecker
        primary_workbook: spy.workbooks.Analysis = push_workbooks_df.spy.output[0]

        return WorkbookContext(workbook_object=primary_workbook, to_push=[workbook_arg])

    @staticmethod
    def from_string(session: Session, status: Status, workbook_arg, worksheet_arg,
                    datasource_arg):
        search_query, workbook_name = WorkbookContext.create_analysis_search_query(workbook_arg)
        search_df = spy.workbooks.search(search_query, quiet=True, session=session)
        workbooks_to_push = list()
        worksheet_object = None
        if len(search_df) == 0:
            workbook_object = spy.workbooks.Analysis({'Name': workbook_name})
            path = _common.get(search_query, 'Path',
                               # If the user doesn't specify a path, default to directly within the user's home folder
                               default=_common.PATH_ROOT)
            inner_status = status.create_inner('Create Workbook', quiet=True)
            if isinstance(worksheet_arg, spy.workbooks.AnalysisWorksheetTemplate):
                workbooks_to_push = [workbook_object]
                workbook_object.worksheets.append(worksheet_arg)

                # Don't push any worksheets, since we can't resolve template parameters yet
                specific_worksheet_ids = list()

                spy.workbooks.push(
                    workbook_object, path=path, include_inventory=False, datasource=datasource_arg,
                    specific_worksheet_ids=specific_worksheet_ids, status=inner_status, session=session)
            else:
                worksheet_name = worksheet_arg if isinstance(worksheet_arg, str) else _common.DEFAULT_WORKSHEET_NAME
                worksheet_object = workbook_object.worksheet(worksheet_name)

                spy.workbooks.push(workbook_object, path=path, include_inventory=False, datasource=datasource_arg,
                                   status=inner_status, session=session)
        else:
            # noinspection PyTypeChecker
            workbook_object: spy.workbooks.Analysis = spy.workbooks.pull(
                search_df,
                include_inventory=False,
                # Pulling worksheets can be expensive. If we just want to push items without changing the UI, skip that.
                specific_worksheet_ids=list() if worksheet_arg is None else None,
                status=status.create_inner('Pull Workbook', quiet=True),
                session=session)[0]

            if isinstance(worksheet_arg, spy.workbooks.AnalysisWorksheetTemplate):
                workbooks_to_push = [workbook_object]
                worksheet_object = worksheet_arg
                if worksheet_object.name in workbook_object.worksheets:
                    existing_worksheet = workbook_object.worksheets[worksheet_object.name]
                    worksheet_object['ID'] = existing_worksheet['ID']
                    workbook_object.worksheets[worksheet_object['ID']] = worksheet_object
                else:
                    workbook_object.worksheets.append(worksheet_object)

            elif worksheet_arg is not None:
                worksheet_name = worksheet_arg if isinstance(worksheet_arg, str) else _common.DEFAULT_WORKSHEET_NAME
                worksheet_object = workbook_object.worksheet(worksheet_name)

        return WorkbookContext(workbook_object=workbook_object, worksheet_object=worksheet_object,
                               to_push=workbooks_to_push)

    @staticmethod
    def from_args(session: Session, status: Status, workbook_arg, worksheet_arg, datasource_arg) -> WorkbookContext:
        if _common.is_guid(workbook_arg):
            workbook_context = WorkbookContext.from_id(session, status, workbook_arg, worksheet_arg)
        elif isinstance(workbook_arg, list):
            workbook_context = WorkbookContext.from_list(session, status, workbook_arg, datasource_arg)
        elif isinstance(workbook_arg, spy.workbooks.AnalysisTemplate):
            workbook_context = WorkbookContext.from_analysis_template(session, status, workbook_arg, datasource_arg)
        elif workbook_arg is not None:
            workbook_context = WorkbookContext.from_string(session, status, workbook_arg, worksheet_arg, datasource_arg)
        else:
            workbook_context = WorkbookContext()

        workbook_context.determine_folder_id()

        return workbook_context

    @staticmethod
    def create_analysis_search_query(workbook) -> tuple[dict[str, str], str]:
        workbook_spec_parts = _common.path_string_to_list(workbook)
        search_query = dict()
        if len(workbook_spec_parts) > 1:
            search_query['Path'] = _common.path_list_to_string(workbook_spec_parts[0:-1])
            workbook_name = workbook_spec_parts[-1]
        else:
            workbook_name = workbook_spec_parts[0]
        search_query['Name'] = f'/^{re.escape(workbook_name)}$/'
        search_query['Workbook Type'] = 'Analysis'
        return search_query, workbook_name
