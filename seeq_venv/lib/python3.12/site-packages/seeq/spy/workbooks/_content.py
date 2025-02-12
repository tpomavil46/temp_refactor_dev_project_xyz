from __future__ import annotations

import copy
import re
from datetime import datetime, timedelta
from typing import Optional, Union

from seeq import spy
from seeq.sdk import *
from seeq.spy import _common
from seeq.spy import _login
from seeq.spy._errors import *
from seeq.spy._redaction import safely
from seeq.spy._session import Session
from seeq.spy._status import Status
from seeq.spy.workbooks import _report_content_utilities
from seeq.spy.workbooks._item import Item
from seeq.spy.workbooks._item_map import ItemMap


class Content(Item):
    """
    The SPy representation of a Seeq Content Item. Content can be created with a number of different sizing
    parameters defined in the display() function within an HTML template.

    If no size, shape, width, or height are specified in the display() function, size and shape will default to medium
    and rectangle, respectively. If a height and width are specified, they will take precedence over size and shape.
    A height must be specified with a width and vice-versa.

    ===================== ==================================================
    Input Column          Content Size Attribute
    ===================== ==================================================
    Size                  The size of the content. Can be 'small', 'medium',
                          or 'large'. Defaults to 'medium'
    Shape                 The content's shape. Can be 'strip', 'rectangle',
                          or 'square'. Defaults to 'rectangle'
    Height                The height of the content. Takes precedence over
                          the size/shape parameter, if specified
    Width                 The width of the content. Takes precedence over
                          the size/shape parameter, if specified
    Scale                 The content's desired scale. A value greater than
                          1 will increase the size of elements within the
                          screenshot. A value less than 1 will shrink the
                          size of elements within the screenshot
    selector              The content's css styling.  The
                          '.screenshotSizeToContent' style can be used to
                          trim whitespace content, which is useful for
                          tables, which have an arbitrary shape.
                          Defaults to None.
    ===================== ==================================================
    """
    CONTENT_SHAPE = {'strip': {'width': 16, 'height': 4},
                     'rectangle': {'width': 16, 'height': 9},
                     'square': {'width': 15, 'height': 15}}

    CONTENT_SIZE = {'small': 350, 'medium': 700, 'large': 1050}

    def __init__(self, definition, report=None):
        if not _common.present(definition, 'selector'):
            # We don't want to allow an absent or None-valued 'selector' because the server turns None into '' and
            # this discrepancy causes the name field (which is a hash of the values) to be different
            definition['selector'] = ''

        super().__init__(definition)
        self.report = report

    @property
    def name(self):
        # Since we don't have a way of uniquely identifying a piece of Content that results from a display()
        # declaration in an HTML report template, we instead hash some state for Content items and use that
        # as the name. Then we can use that name to match up content from an HTML template with an in-memory
        # object.
        return self.definition_hash

    @property
    def definition_hash(self):
        definition_copy = copy.deepcopy(self.definition_dict)
        for key in ['ID', 'Name']:
            if key in definition_copy:
                del definition_copy[key]
        return Item.digest_hash(definition_copy)

    def push(self, session: Session, item_map: ItemMap, existing_contents: dict, status: Status):
        session = Session.validate(session)
        content_input = self._create_content_input(item_map)

        existing_content = existing_contents.get(self.id)
        if not existing_content:
            # Make a Content item and map the IDs, then use the name field (which is a hash of the values) to see if
            # we've already got a piece of content that matches this. This is purely to cut down on the number of
            # Content items we create.
            content_to_push = Content(self.definition_dict, self.report)
            for key in ['Asset Selection ID', 'Date Range ID', 'Workbook ID', 'Worksheet ID', 'Workstep ID']:
                if key in item_map:
                    content_to_push[key] = item_map[self[key]]
            existing_content = existing_contents.get(content_to_push.name)

        content_api = ContentApi(session.client)
        if not existing_content:

            content_output = safely(
                lambda: content_api.create_content(body=content_input),
                action_description=f'create Content {content_input.name} '
                                   f'{content_input.worksheet_id}/{content_input.workstep_id}',
                status=status)  # type: ContentOutputV1
        else:
            content_output = safely(
                lambda: content_api.update_content(id=existing_content.id, body=content_input),
                action_description=f'update Content {content_input.name} '
                                   f'{content_input.worksheet_id}/{content_input.workstep_id}',
                status=status)  # type: ContentOutputV1
        if content_output is None:
            return None

        item_map[self.id] = content_output.id

        return content_output

    def _create_content_input(self, item_map=None):
        self._validate_fields_before_push()

        not_found_text = 'not found. Are you sure you are including all the necessary workbooks in your push operation?'

        date_range_id = None
        if _common.present(self.definition, 'Date Range ID'):
            if self.definition['Date Range ID'] not in item_map:
                raise SPyDependencyNotFound(
                    f'Date Range {self.definition["Date Range ID"]} for {self} {not_found_text}')

            date_range_id = item_map[self.definition['Date Range ID']]

        asset_selection_id = None
        if _common.present(self.definition, 'Asset Selection ID'):
            if self.definition['Asset Selection ID'] not in item_map:
                raise SPyDependencyNotFound(
                    f'Asset Selection {self.definition["Asset Selection ID"]} for {self} {not_found_text}')

            asset_selection_id = item_map[self.definition['Asset Selection ID']]

        if self.definition['Worksheet ID'] not in item_map:
            raise SPyDependencyNotFound(f'Worksheet {self.definition["Worksheet ID"]} {not_found_text}')

        if self.definition['Workstep ID'] not in item_map:
            raise SPyDependencyNotFound(f'Workstep {self.definition["Workstep ID"]} {not_found_text}')

        # Report has definitely already been pushed by this time
        report_id = item_map[self.report.id]

        content_input = ContentInputV1()
        self.set_input_via_attribute_map(content_input, Content.INPUT_ATTRIBUTE_MAP)
        content_input.name = (f'content_{item_map[self.definition["Worksheet ID"]]}'
                              f'_{item_map[self.definition["Workstep ID"]]}')
        content_input.asset_selection_id = asset_selection_id
        content_input.date_range_id = date_range_id
        content_input.report_id = report_id
        content_input.worksheet_id = item_map[self.definition['Worksheet ID']]
        content_input.workstep_id = item_map[self.definition['Workstep ID']]

        return content_input

    INPUT_ATTRIBUTE_MAP = {
        'archived': 'Archived',
        'description': 'Description',
        'height': 'Height',
        'hide_uncertainty': 'Hide Uncertainty',
        'react': 'Interactive',
        'scale': 'Scale',
        'selector': 'selector',
        'summary_type': 'Summary Type',
        'summary_value': 'Summary Value',
        'timezone': 'Timezone',
        'warning': 'Warning',
        'width': 'Width',
    }

    def _validate_fields_before_push(self):
        for field in ['Name', 'Width', 'Height', 'Worksheet ID', 'Workstep ID', 'selector']:
            if field not in self.definition:
                raise SPyValueError(f'Unable to push Content with ID {self.id}: missing "{field}" field')

    @staticmethod
    def pull(item_id, *, allowed_types=None, report=None, session: Session = None, status=None):
        session = Session.validate(session)
        content_api = ContentApi(session.client)
        content_output = safely(lambda: content_api.get_content(id=item_id),
                                action_description=f'get Content {item_id}',
                                status=status)  # type: ContentOutputV1
        if content_output is None:
            return None

        return Content.from_content_output(content_output, report)

    OUTPUT_ATTRIBUTE_MAP = {
        'id': 'ID',
        'name': 'Name',
        'description': 'Description',
        'is_archived': 'Archived',
        'is_redacted': 'Redacted',
        'height': 'Height',
        'hide_uncertainty': 'Hide Uncertainty',
        'react': 'Interactive',
        'scale': 'Scale',
        'selector': 'selector',
        'source_worksheet': 'Worksheet ID',
        'source_workstep': 'Workstep ID',
        'source_workbook': 'Workbook ID',
        'status_message': 'Status Message',
        'summary_type': 'Summary Type',
        'summary_value': 'Summary Value',
        'timezone': 'Timezone',
        'warning': 'Warning',
        'width': 'Width',
    }

    @staticmethod
    def from_content_output(content_output: Union[ContentOutputV1, ContentWithMetadataOutputV1], report):
        new_content_definition = Item.dict_via_attribute_map(content_output, Content.OUTPUT_ATTRIBUTE_MAP)

        new_content_definition['Date Range ID'] = (
            None if content_output.date_range is None else content_output.date_range.id)
        new_content_definition['Asset Selection ID'] = (
            None if content_output.asset_selection is None else content_output.asset_selection.id)

        return Content(new_content_definition, report)

    @property
    def date_range(self):
        if 'Date Range ID' not in self.definition:
            return None

        return self.report.date_ranges[self.definition['Date Range ID']]

    @date_range.setter
    def date_range(self, value):
        if value is None:
            self.definition['Date Range ID'] = None
        else:
            self.definition['Date Range ID'] = value['ID']

    @property
    def asset_selection(self):
        if 'Asset Selection ID' not in self.definition:
            return None

        return self.report.asset_selections[self.definition['Asset Selection ID']]

    @asset_selection.setter
    def asset_selection(self, value):
        if value is None:
            self.definition['Asset Selection ID'] = None
        else:
            self.definition['Asset Selection ID'] = value['ID']

    @property
    def html(self):
        return f'<a href="/api/content/{self.id}/sourceUrl" rel="nofollow noopener noreferrer"> ' \
               f'<img data-seeq-content="{self.id}" class="report-image-border fr-fic fr-dii contentLoaded ' \
               f'fr-draggable" src="/api/content/{self.id}/image"></a>'

    @staticmethod
    def push_with_check_for_existing(self: Union[DateRange, AssetSelection], existing_dict: dict,
                                     input_object: Union[DateRangeInputV1, AssetSelectionInputV1], item_map: ItemMap,
                                     create_func, update_func):
        existing_object = existing_dict.get(self.id)
        if not existing_object:
            existing_object = existing_dict.get(self.name)

        if not existing_object:
            output_object = create_func(body=input_object)  # type: DateRangeOutputV1
            existing_id = output_object.id
        else:
            existing_id = existing_object.id

        # We update even if we just created it, because if we're trying to create an archived item, the create
        # function doesn't look at the archived flag on the input object.
        output_object = update_func(id=existing_id, body=input_object)

        item_map[self.id] = output_object.id

        return output_object


class DateRange(Item):
    """
    The SPy representation of a Seeq date range. Date ranges can be created with a number of different input
    properties that a user can define when creating a Date Range asset. Date ranges can either be static,
    specified with just a Name, Start, and End, or they can be live, denoted by including Auto Enabled,
    Auto Duration, Auto offset, and Auto Offset Direction values.

    ===================== =============================================
    Input Column          Date Range Attribute
    ===================== =============================================
    ID                    The id of the date range. If not provided one
                          will be generated
    Name                  The name of the date range. Eg "Date Range 1"
    Start                 The ISO 8601 string or datetime object start
                          of the date range
    End                   The ISO 8601 string or datetime object end of
                          the date range
    Auto Enabled          Boolean if automatic update is enabled
    Auto Duration         The duration of the automatic update sliding
                          window. Eg, 10min, 1hr, 1d, etc
    Auto Offset           The offset of the automatic update sliding
                          window. Eg, 10min, 1day, etc
    Auto Offset Direction The direction of the offset. Either 'Past' or
                          'Future'. Default 'Past'
    Condition ID          The id of the condition defining the date
                          range, if applicable
    ===================== =============================================
    """

    DATE_RANGE_COLUMN_NAMES = ['ID', 'Name', 'Start', 'End', 'Auto Enabled', 'Auto Duration', 'Auto Offset',
                               'Auto Offset Direction', 'Condition ID', 'Type']

    def __init__(self, definition, report):
        super().__init__(definition)
        self.report = report

    def push(self, session: Session, item_map: ItemMap, existing_date_ranges: dict, status: Status):
        date_range_input = self._create_date_range_input(session, item_map)
        content_api = ContentApi(session.client)
        return Content.push_with_check_for_existing(self, existing_date_ranges, date_range_input, item_map,
                                                    content_api.create_date_range, content_api.update_date_range)

    STATIC_DATE_RANGE_REGEX = r'capsule\(\d+.*\d+.*\)'

    @staticmethod
    def pull(item_id, *, allowed_types=None, report=None, annotation_output: Optional[AnnotationOutputV1] = None,
             session: Session = None, status=None):
        session = Session.validate(session)
        if isinstance(item_id, DateRangeOutputV1):
            # This is an optimization because get_contents_with_all_metadata returns both the content and date range
            date_range_output = item_id
        else:
            content_api = ContentApi(session.client)
            date_range_output = safely(lambda: content_api.get_date_range(id=item_id),
                                       action_description=f'get Date Range for Content {item_id}',
                                       status=status)
            if date_range_output is None:
                return None

        DateRange.fix_up_date_range_object_for_compatibility(date_range_output, annotation_output)

        date_range_dict = dict()
        date_range_dict['Name'] = date_range_output.name
        date_range_dict['ID'] = date_range_output.id
        date_range_dict['Enabled'] = date_range_output.is_enabled
        date_range_dict['Archived'] = date_range_output.is_archived

        if date_range_output.condition:
            date_range_dict['Condition ID'] = date_range_output.condition.id

        # We use a port of the frontend code to take the date_range_output and parse it into fields in a dictionary
        # that is identical to what the frontend uses. This is particularly important for the formula field,
        # which has an implicit (and strict) format that the frontend uses. From there, we take the dictionary fields
        # and populate the user-friendly dictionary format of SPy so that spy.assets users can easily formulate date
        # ranges.
        frontend_date_range_dict = _report_content_utilities.format_date_range_from_api_output(date_range_output)

        if not frontend_date_range_dict['auto'].get('enabled', False):
            date_range_dict['Start'] = \
                _login.parse_content_datetime_with_timezone(session, date_range_output.date_range.start).isoformat()
            date_range_dict['End'] = \
                _login.parse_content_datetime_with_timezone(session, date_range_output.date_range.end).isoformat()
        else:
            date_range_dict['Auto Enabled'] = True
            date_range_dict['Auto Offset Direction'] = frontend_date_range_dict['auto']['offsetDirection'].capitalize()
            offset = frontend_date_range_dict['auto']['offset']
            date_range_dict['Auto Offset'] = f'{offset["value"]}{offset["units"]}'
            date_range_dict['Auto Duration'] = str(frontend_date_range_dict['auto']['duration'] / 1000) + 's'

        condition_dict = frontend_date_range_dict['condition']
        capsule_picker_dict = dict()
        if 'range' in condition_dict:
            if condition_dict['range'].get('start') is not None:
                capsule_picker_dict['Search Start'] = \
                    _login.parse_content_datetime_with_timezone(session, condition_dict["range"]["start"]).isoformat()
            if condition_dict['range'].get('end') is not None:
                capsule_picker_dict['Search End'] = \
                    _login.parse_content_datetime_with_timezone(session, condition_dict["range"]["end"]).isoformat()

        if condition_dict.get('strategy') is not None:
            capsule_picker_dict['Strategy'] = condition_dict['strategy']
        if condition_dict.get('reference') is not None:
            capsule_picker_dict['Reference'] = condition_dict['reference']
        if condition_dict.get('offset') is not None:
            capsule_picker_dict['Offset'] = condition_dict['offset']
        if condition_dict.get('columns') is not None:
            capsule_picker_dict['Columns'] = condition_dict['columns']
        if condition_dict.get('sortBy') is not None:
            capsule_picker_dict['Sort By'] = condition_dict['sortBy']
        if condition_dict.get('sortAsc') is not None:
            capsule_picker_dict['Sort Asc'] = condition_dict['sortAsc']
        if condition_dict.get('maximumDuration') is not None:
            capsule_picker_dict['Maximum Duration'] = (f'{condition_dict["maximumDuration"]["value"]}'
                                                       f'{condition_dict["maximumDuration"]["units"]}')

        if len(capsule_picker_dict) > 0:
            date_range_dict['Capsule Picker'] = capsule_picker_dict

        return DateRange(date_range_dict, report)

    @staticmethod
    def fix_up_date_range_object_for_compatibility(date_range_output,
                                                   annotation_output: Optional[AnnotationOutputV1] = None):
        # For compatibility purposes, we put these attributes on the date_range_output if they don't exist. This
        # allows SPy to be compatible with older versions of the API.
        if not hasattr(date_range_output, 'is_enabled'):
            setattr(date_range_output, 'is_enabled', getattr(date_range_output, 'enabled'))
        if not hasattr(date_range_output, 'is_archived'):
            setattr(date_range_output, 'is_archived', getattr(date_range_output, 'archived'))
        if not hasattr(date_range_output, 'is_auto_updating'):
            setattr(date_range_output, 'is_auto_updating',
                    re.search(DateRange.STATIC_DATE_RANGE_REGEX, date_range_output.formula) is None)
        if not hasattr(date_range_output, 'is_background'):
            setattr(date_range_output, 'is_background',
                    (annotation_output.is_background
                     if annotation_output is not None and hasattr(annotation_output, 'is_background') else None))
        if not hasattr(date_range_output, 'cron_schedule'):
            setattr(date_range_output, 'cron_schedule',
                    (annotation_output.cron_schedule
                     if annotation_output is not None and hasattr(annotation_output, 'cron_schedule') else None))

    def _to_frontend_date_range_dict(self, session: Session):
        # This function populates a dictionary with fields in the same layout and format that is used by the
        # frontend, which allows us to use the _report_content_utilities functions that have been ported 1:1 from
        # TypeScript.

        frontend_date_range_dict = copy.deepcopy(_report_content_utilities.DEFAULT_DATE_RANGE)
        frontend_date_range_dict['name'] = self.definition['Name']

        # Need a default here. Unfortunately, this should have come from the Organizer Topic document's workstep,
        # but we didn't change SPy when the V20190724162900__Content_and_Date_Ranges.kt was written.
        frontend_date_range_dict['condition']['maximumDuration'] = {'value': 40, 'units': 'h'}

        if _common.present(self.definition, 'Auto Enabled'):
            frontend_date_range_dict['auto']['enabled'] = _common.get(self.definition, 'Auto Enabled')
            frontend_date_range_dict['auto']['offset']['value'] = int(_common.parse_str_time(
                self.definition['Auto Offset'])[0])
            frontend_date_range_dict['auto']['offset']['units'] = _common.parse_str_time(
                self.definition['Auto Offset'])[1]
            frontend_date_range_dict['auto']['duration'] = int(_common.parse_str_time(
                self.definition['Auto Duration'])[2])
            frontend_date_range_dict['auto']['background'] = (_common.get(self.report.schedule, 'Background')
                                                              if self.report.schedule is not None else False)
            frontend_date_range_dict['auto']['cronSchedule'] = (_common.get(self.report.schedule, 'Cron Schedule')
                                                                if self.report.schedule is not None else list())
        else:
            frontend_date_range_dict['range']['start'] = int(self.get_start(session).value / 1_000_000)
            frontend_date_range_dict['range']['end'] = int(self.get_end(session).value / 1_000_000)

        if _common.present(self.definition, 'Condition ID'):
            frontend_date_range_dict['condition']['id'] = _common.get(self.definition, 'Condition ID')

        # Old saved workbooks may not have a capsule picker, so we need to set the default values
        frontend_date_range_dict['condition']['range'] = {
            'start': datetime.utcnow().timestamp() * 1000 - timedelta(days=7).total_seconds() * 1000,
            'end': datetime.utcnow().timestamp() * 1000,
        }

        if _common.present(self.definition, 'Capsule Picker'):
            capsule_picker_dict = _common.get(self.definition, 'Capsule Picker')
            if _common.present(capsule_picker_dict, 'Search Start'):
                frontend_date_range_dict['condition']['range'] = {
                    'start': int(_login.parse_content_datetime_with_timezone(
                        session, _common.get(capsule_picker_dict, 'Search Start')).value / 1_000_000),
                    'end': int(_login.parse_content_datetime_with_timezone(
                        session, _common.get(capsule_picker_dict, 'Search End')).value / 1_000_000),
                }
            if _common.present(capsule_picker_dict, 'Strategy'):
                frontend_date_range_dict['condition']['strategy'] = _common.get(capsule_picker_dict, 'Strategy')
            if _common.present(capsule_picker_dict, 'Reference'):
                frontend_date_range_dict['condition']['reference'] = _common.get(capsule_picker_dict, 'Reference')
            if _common.present(capsule_picker_dict, 'Offset'):
                frontend_date_range_dict['condition']['offset'] = _common.get(capsule_picker_dict, 'Offset')
            if _common.present(capsule_picker_dict, 'Columns'):
                frontend_date_range_dict['condition']['columns'] = _common.get(capsule_picker_dict, 'Columns')
            frontend_date_range_dict['condition']['isCapsuleFromTable'] = _common.present(
                capsule_picker_dict, 'Columns')
            if _common.present(capsule_picker_dict, 'Sort By'):
                frontend_date_range_dict['condition']['sortBy'] = _common.get(capsule_picker_dict, 'Sort By')
            if _common.present(capsule_picker_dict, 'Sort Asc'):
                frontend_date_range_dict['condition']['sortAsc'] = _common.get(capsule_picker_dict, 'Sort Asc')
            if _common.present(capsule_picker_dict, 'Maximum Duration'):
                frontend_date_range_dict['condition']['maximumDuration']['value'] = int(_common.parse_str_time(
                    capsule_picker_dict['Maximum Duration'])[0])
                frontend_date_range_dict['condition']['maximumDuration']['units'] = _common.parse_str_time(
                    capsule_picker_dict['Maximum Duration'])[1]

        return frontend_date_range_dict

    def _create_date_range_input(self, session: Session, item_map: ItemMap):
        self._validate_fields_before_push()

        # We put a dummy cron schedule here so that we can create a live date range prior to creating a report with a
        # schedule
        cron_schedule = ['59 59 23 31 12 ? 2099'] if self._is_date_range_live(session) else None

        condition_id = None
        if _common.present(self.definition, 'Condition ID'):
            condition_id = _common.get(self.definition, 'Condition ID')
            if condition_id not in item_map:
                raise SPyValueError(f'Unable to find Condition {condition_id} in item_map. You may need to specify '
                                    f'the include_inventory=True argument for the push.')

            condition_id = item_map[_common.get(self.definition, 'Condition ID')]

        return DateRangeInputV1(name=self.definition['Name'],
                                formula=self.get_formula(session),
                                cron_schedule=cron_schedule,
                                report_id=item_map[self.report.id],
                                enabled=_common.get(self.definition, 'Enabled', True),
                                archived=_common.get(self.definition, 'Archived', False),
                                condition_id=condition_id)

    def _validate_fields_before_push(self):
        for field in ['Name']:
            if field not in self.definition:
                raise SPyValueError(f'Unable to push Date Range with ID {self.id}: missing "{field}" field')

    def _is_date_range_live(self, session: Session):
        return '$now' in self.get_formula(session)

    @staticmethod
    def _validate_user_date_range(date_range):
        errors = list()

        for k in date_range.keys():
            if k not in DateRange.DATE_RANGE_COLUMN_NAMES:
                errors.append(
                    f'Unrecognized Date Range property "{k}". Valid properties:\n'
                    f'{DateRange.DATE_RANGE_COLUMN_NAMES}')

        if 'Name' not in date_range:
            errors.append('All date ranges require a "Name"')

        if 'Start' in date_range and 'End' not in date_range:
            errors.append('Date Range "End" must be supplied with Date Range "Start"')
        elif 'End' in date_range and 'Start' not in date_range:
            errors.append('Date Range "Start" must be supplied with Date Range "End"')

        # if auto enabled is defined, all the other auto fields must be defined.
        if 'Auto Enabled' in date_range:
            if 'Auto Offset' not in date_range:
                errors.append('"Auto Offset" is required if "Auto Enabled" is True')
            if 'Auto Duration' not in date_range:
                errors.append('"Auto Duration" is required if "Auto Enabled" is True')
            if 'Auto Offset Direction' not in date_range:
                errors.append('"Auto Offset Direction" is required if "Auto Enabled" is True')

        if errors:
            msg = f'There was 1 error ' if len(errors) == 1 else f'There were {len(errors)} errors '
            msg += f'detected in date range "{date_range["Name"]}": {errors}'
            raise SPyRuntimeError(msg)

    @property
    def start(self):
        return self.get_start(spy.session)

    def get_start(self, session: Session):
        return _login.parse_content_datetime_with_timezone(session, _common.get(self.definition, 'Start'))

    @property
    def end(self):
        return self.get_end(spy.session)

    def get_end(self, session: Session):
        return _login.parse_content_datetime_with_timezone(session, _common.get(self.definition, 'End'))

    def get_formula(self, session: Session):
        return _report_content_utilities.create_date_range_formula(self._to_frontend_date_range_dict(session))


class AssetSelection(Item):
    """
    The SPy representation of a Seeq asset selection.

    ===================== =============================================
    Input Column          Asset Selection Attribute
    ===================== =============================================
    ID                    The id of the asset selection. If not
                          provided one will be generated
    Name                  The name of the asset selection. Eg "Asset
                          Selection 1"
    Asset ID              The id of the asset selected
    Path Levels           Number of levels shown in the asset path for
                          the asset selection
    ===================== =============================================
    """

    ASSET_SELECTION_COLUMN_NAMES = ['ID', 'Name', 'Asset ID', 'Path Levels', 'Type']

    def __init__(self, definition, report):
        super().__init__(definition)
        self.report = report

    def push(self, session: Session, item_map: ItemMap, existing_asset_selections: dict, status: Status):
        asset_selection_input = self._create_asset_selection_input(item_map)

        content_api = ContentApi(session.client)
        return Content.push_with_check_for_existing(self, existing_asset_selections, asset_selection_input, item_map,
                                                    content_api.create_asset_selection,
                                                    content_api.update_asset_selection)

    @staticmethod
    def pull(item_id, *, allowed_types=None, report=None, session: Session = None, status=None):
        session = Session.validate(session)
        if isinstance(item_id, AssetSelectionOutputV1):
            asset_selection_output = item_id
        else:
            content_api = ContentApi(session.client)
            asset_selection_output = safely(lambda: content_api.get_asset_selection(id=item_id),
                                            action_description=f'get Asset Selection for Content {item_id}',
                                            status=status)
            if asset_selection_output is None:
                return None

        asset_selection_dict = dict()
        asset_selection_dict['Name'] = asset_selection_output.name
        asset_selection_dict['ID'] = asset_selection_output.id
        asset_selection_dict['Asset ID'] = asset_selection_output.asset.id
        asset_selection_dict['Path Levels'] = asset_selection_output.asset_path_depth
        asset_selection_dict['Archived'] = asset_selection_output.is_archived

        return AssetSelection(asset_selection_dict, report)

    def _create_asset_selection_input(self, item_map):
        self._validate_fields_before_push()

        asset_id = (item_map[_common.get(self.definition, 'Asset ID')]
                    if _common.present(self.definition, 'Asset ID') else None)

        return AssetSelectionInputV1(name=self.definition['Name'],
                                     selection_id=self.definition['ID'],
                                     asset_id=asset_id,
                                     asset_path_depth=self.definition['Path Levels'],
                                     report_id=item_map[self.report.id],
                                     archived=_common.get(self.definition, 'Archived', False))

    def _validate_fields_before_push(self):
        for field in ['Name', 'Asset ID', 'Path Levels']:
            if field not in self.definition:
                raise SPyValueError(f'Unable to push Asset Selection with ID {self.id}: missing "{field}" field')
