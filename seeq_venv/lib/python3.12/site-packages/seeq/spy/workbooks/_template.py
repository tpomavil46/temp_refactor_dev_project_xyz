from __future__ import annotations

import copy
import json
import os
import re
from typing import Optional, List, Callable, Union

from seeq.base import util
from seeq.spy import _common
from seeq.spy._errors import *
from seeq.spy._session import Session
from seeq.spy._status import Status
from seeq.spy.workbooks._annotation import Annotation, Journal, Report
from seeq.spy.workbooks._content import DateRange, AssetSelection, Content
from seeq.spy.workbooks._context import WorkbookPushContext
from seeq.spy.workbooks._item import Item, ItemList, Reference
from seeq.spy.workbooks._item_map import ItemMap, OverrideItemMap
from seeq.spy.workbooks._mustache import MustachioedAnnotation
from seeq.spy.workbooks._workbook import Workbook, Analysis, Topic, WorkbookList
from seeq.spy.workbooks._worksheet import Worksheet, AnalysisWorksheet, TopicDocument
from seeq.spy.workbooks._workstep import AnalysisWorkstep


#
# This templating system allows a user to define an Analysis or Topic in Workbench or Organizer, save it to disk and
# then use it as a template whereby the items within a worksheet/workstep/date range are replaced programmatically or
# the context in a Topic Document is replaced with references to a worksheet/workstep.
#
# This file contains a series of XxxxxTemplate classes that wrap an "inner" Xxxxx instance object that serves as the
# template. The wrapper class provides the same interface as the inner object but does some work to substitute/override
# items, worksheets/worksteps, and Mustache variables before pushing to Seeq Server.
#
# If the user sets any values on the definition, it goes into the _definition_override. The inner object is never
# touched.
#
# Every template is necessarily created with a label, which is used to differentiate between multiple instances of a
# template. This allows the normal spy.workbooks.push() operation to properly keep track of identifiers and keep
# idempotency.
#
# Each XxxxxTemplate class exposes a "code" attribute which provides boilerplate code that the user copy/pastes in
# order to override the template "parameters".
#
# Template instances are typically created using the spy.workbooks.load(as_template_with_label) argument.
#
# See "Workbook Templates.ipynb" for a tutorial on how to use.
#

class ItemTemplate:
    _definition_override: dict
    _label: str
    _parameters: Optional[dict]
    _template: Item
    _is_copy: bool

    PARAMETER_KEY_REGEX = re.compile(
        r'(?P<id>[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12})?'
        r'\s*(\[(?P<type>[^]]+)])?(\s*(?P<fqn>.*))?'
    )

    def __init__(self, label: str, template: Item, package: ItemList, is_copy: bool):
        if isinstance(template, ItemTemplate):
            raise SPyTypeError('template parameter cannot be a XxxxxTemplate object, it must be a "regular" object')

        self._definition_override = dict()
        self._label = label
        self._parameters = None
        self._template = template
        self._package = package if package is not None else ItemList()
        self._is_copy = is_copy

        # This one of the key concepts of the template: The ID of a template is the combination of the wrapped item
        # ID and the label for the template that is supplied by the user.
        self._definition_override['ID'] = f'{template.id} {self._label}'

    #
    # The following dictionary-like indexing functions facilitate the "override" mechanism that the template provides
    #

    def __contains__(self, key):
        return _common.present(self._definition_override, key) or key in self._template

    def __getitem__(self, key):
        value = self._definition_override[key] if key in self._definition_override else self._template[key]
        if isinstance(self.parameters, dict) and isinstance(value, str):
            mustache = MustachioedAnnotation(value)
            value = mustache.render(self.parameters)
        return value

    def __setitem__(self, key, val):
        self._definition_override[key] = _common.ensure_upper_case_id(key, val)

    def __delitem__(self, key):
        del self._definition_override[key]

    def __repr__(self):
        return '%s "%s" (%s)' % (self.type, self.fqn, self.id)

    @property
    def id(self):
        return self['ID']

    @property
    def name(self):
        return self['Name']

    @name.setter
    def name(self, val):
        self._definition_override['Name'] = val

    @property
    def type(self):
        return self['Type']

    def refresh_from(self, new_item, item_map: ItemMap, status: Status):
        # We don't allow refreshing of Template items because it's just too confusing... users would not expect to
        # have a Template object trying to directly represent something on the server
        pass

    @property
    def label(self):
        return self._label

    @label.setter
    def label(self, val):
        self._label = val

    @property
    def template(self):
        return self._template

    @property
    def is_copy(self):
        return self._is_copy

    @property
    def package(self):
        return self._package

    @property
    def parameters(self) -> dict:
        return self._parameters

    @parameters.setter
    def parameters(self, val: dict):
        self._set_parameters(val)

    def _validate_parameters_dict(self, code_dict, parameters_dict):
        final = code_dict.copy()
        for k, v in parameters_dict.items():
            if k not in code_dict:
                matching_keys = list()
                regex_match = ItemTemplate.PARAMETER_KEY_REGEX.match(k)
                if not regex_match:
                    raise SPyValueError(f'parameters key not recognized: "{k}"\nKey format must be "ID [Type] FQN"')

                _id = regex_match.group('id')
                _type = regex_match.group('type')
                _fqn = regex_match.group('fqn')

                for code_key_string in code_dict.keys():
                    code_key = ItemTemplate.code_key_tuple(code_key_string)
                    if _id is not None and _id == code_key[0]:
                        matching_keys.append(code_key_string)
                    elif (_fqn is not None and _fqn == code_key[2]) and (_type is None or _type == code_key[1]):
                        matching_keys.append(code_key_string)

                if len(matching_keys) == 0:
                    raise SPyValueError(
                        f'parameters key "{k}" could not be mapped to anything in the template code:\n' +
                        self.code)

                if len(matching_keys) >= 2:
                    raise SPyValueError(f'parameters key "{k}" matches multiple keys in the template code:\n' +
                                        '\n'.join([k for k in matching_keys]))

                k = matching_keys[0]

            if isinstance(code_dict[k], list):
                if not isinstance(code_dict[k], type(v)):
                    raise SPyValueError(f'parameters key "{k}" is a {type(v)}, should be a {type(code_dict[k])}')

                if len(code_dict[k]) == 0:
                    raise SPyValueError(f'parameters key "{k}" should be omitted as there are no nested moustache '
                                        f'elements present in the Topic Document for it')

                final[k] = self._validate_parameters_list(code_dict[k][0], v)
            elif isinstance(code_dict[k], dict):
                final[k] = self._validate_parameters_dict(code_dict[k], v)
            else:
                final[k] = v

        return final

    def _validate_parameters_list(self, code_dict_for_list, parameters_list):
        final = list()

        for list_item in parameters_list:
            if isinstance(list_item, dict):
                final.append(self._validate_parameters_dict(code_dict_for_list, list_item))
            else:
                final.append(list_item)

        return final

    def _set_parameters(self, parameters):
        if parameters is None:
            self._parameters = None
            return

        self._parameters = self._validate_parameters_dict(self.code_dict(), parameters)

    @property
    def provenance(self):
        return Item.TEMPLATE

    @property
    def fqn(self):
        parts = list()
        if _common.present(self, 'Path'):
            parts.append(self['Path'])
        if self.type != 'Asset' and _common.present(self, 'Asset'):
            parts.append(self['Asset'])
        if _common.present(self, 'Name'):
            parts.append(self['Name'])

        return ' >> '.join(parts)

    @property
    def definition_dict(self):
        d = copy.deepcopy(self._template.definition_dict)
        d.update(self._definition_override)
        return d

    @staticmethod
    def code_key_string(_id, _type, _fqn):
        return f'{_id} [{_type}] {_fqn}'

    @staticmethod
    def code_key_tuple(s):
        match = ItemTemplate.PARAMETER_KEY_REGEX.match(s)
        return match.group('id'), match.group('type'), match.group('fqn')

    @staticmethod
    def _get_template_code_string(_code_dict):
        return json.dumps(_code_dict, indent=4).replace(': null', ': None')

    def _code_variable(self):
        pass

    def code_dict(self):
        return dict()

    @property
    def code(self):
        return f'{self._code_variable()}.parameters = {ItemTemplate._get_template_code_string(self.code_dict())}'

    def is_overridden(self, key) -> bool:
        return key in self._definition_override

    @staticmethod
    def _code_dict_for_analysis(workbook: Analysis, worksheets: List[Union[AnalysisWorksheet, AnalysisWorkstep]]):
        code_dict = dict()
        for worksheet in worksheets:
            item_ids = {r.id for r in worksheet.referenced_items}
            for item_id in item_ids:
                if item_id not in workbook.item_inventory:
                    # This can happen if the workbook was not pulled with spy.workbooks.pull() or if a referenced
                    # item isn't in the set of allowed_types in Workbook._scrape_inventory_from_item()
                    continue

                obj = workbook.item_inventory[item_id]
                clean_fqn = obj.fqn.replace("\"", "")
                clean_type = obj.type.replace("Stored", "").replace("Calculated", "").replace("Literal", "")
                code_dict[ItemTemplate.code_key_string(item_id, clean_type, clean_fqn)] = None

            if not isinstance(worksheet, AnalysisWorkstep):
                mustache = MustachioedAnnotation(
                    worksheet.journal.html,
                    lambda content_id: worksheet.journal.resolve_content_code_key(content_id))

                code_dict.update(mustache.code_dict)

            mustache = MustachioedAnnotation(json.dumps(worksheet.definition_dict))
            code_dict.update(mustache.code_dict)

        return code_dict

    @staticmethod
    def _code_dict_for_topic(documents: List[ReportTemplate]) -> dict:
        code_dict = dict()
        for document in documents:
            for reference in document.referenced_items:
                if reference.id not in document.worksheet.workbook.item_inventory:
                    raise SPyValueError(f'Topic Document {document.worksheet} has '
                                        f'{reference.provenance} with ID "{reference.id}" '
                                        f'but that item is not found in the item inventory of workbook '
                                        f'{document.worksheet.workbook}')

                item = document.worksheet.workbook.item_inventory[reference.id]
                code_dict[ItemTemplate.code_key_string(reference.id, reference.provenance, item.fqn)] = None

            mustache = MustachioedAnnotation(document.html,
                                             lambda content_id: document.resolve_content_code_key(content_id))
            code_dict.update(mustache.code_dict)

            mustache = MustachioedAnnotation(json.dumps(document.worksheet.definition_dict))
            code_dict.update(mustache.code_dict)

        return code_dict

    def _after_push(self, item_map):
        if not self.is_copy:
            # We also map to the non-templated ID in case there's a piece of content that has no template parameters
            item_map[self.template.id] = item_map[self.id]


class WorkbookTemplate(ItemTemplate, Workbook):
    def __init__(self, label: str, workbook: Workbook, *, package: ItemList = None, is_copy: bool):
        if not isinstance(workbook, Workbook):
            raise SPyTypeError('template parameter must be a Workbook (Analysis or Topic)')

        ItemTemplate.__init__(self, label, workbook, package, is_copy)

    @property
    def template(self) -> Workbook:
        # noinspection PyTypeChecker
        return self._template

    @property
    def item_inventory(self):
        return self.template.item_inventory

    @property
    def datasource_maps(self):
        return self.template.datasource_maps

    @property
    def datasource_inventory(self):
        return self.template.datasource_inventory

    def _find_existing_workbook_template(self, workbook_id, label):
        for workbook in self.package:
            if workbook.id == f'{workbook_id} {label}':
                return workbook

        return None

    def push(self, *, context: WorkbookPushContext, folder_id=None, item_map: ItemMap = None, label=None,
             include_inventory=True):
        if context.specific_worksheet_ids is None or len(context.specific_worksheet_ids) > 0:
            override_item_map = OverrideItemMap(item_map, template_parameters=self.parameters)
        else:
            override_item_map = item_map

        output = super().push(context=context, folder_id=folder_id, item_map=override_item_map,
                              label=self.label, include_inventory=include_inventory)

        self._after_push(item_map)

        return output


class AnalysisTemplate(WorkbookTemplate, Analysis):
    def __init__(self, label: str, workbook: Analysis, *, package: ItemList = None, is_copy: bool = False):
        if not isinstance(workbook, Analysis):
            raise SPyTypeError('workbook parameter must be an Analysis')

        WorkbookTemplate.__init__(self, label, workbook, package=package, is_copy=is_copy)
        Analysis.__init__(self)

        # Clear definition for clarity, since it's not used in a template
        self._definition = dict()

        for worksheet in workbook.worksheets:  # type: AnalysisWorksheet
            AnalysisWorksheetTemplate(label, self, worksheet, package=package, is_copy=is_copy)

    @property
    def template(self) -> Analysis:
        # noinspection PyTypeChecker
        return self._template

    def _code_variable(self):
        return 'workbook'

    def code_dict(self):
        return ItemTemplate._code_dict_for_analysis(self.template, self.template.worksheets)

    def copy(self, label):
        existing_workbook = self._find_existing_workbook_template(self.template.id, label)
        if existing_workbook is not None:
            return existing_workbook

        return AnalysisTemplate(label, self.template, package=self.package, is_copy=True)


class TopicTemplate(WorkbookTemplate, Topic):
    def __init__(self, label: str, topic: Topic, *, package: ItemList = None, is_copy: bool = False):
        if not isinstance(topic, Topic):
            raise SPyTypeError('topic parameter must be a Topic')

        WorkbookTemplate.__init__(self, label, topic, package=package, is_copy=is_copy)
        Topic.__init__(self)

        # Clear definition for clarity, since it's not used in a template
        self._definition = dict()

        for worksheet in topic.worksheets:  # type: TopicDocument
            TopicDocumentTemplate(label, self, worksheet, package=package, is_copy=is_copy)

    @property
    def template(self) -> Topic:
        # noinspection PyTypeChecker
        return self._template

    def _code_variable(self):
        return 'topic'

    def code_dict(self):
        return ItemTemplate._code_dict_for_topic([worksheet.report for worksheet in self.worksheets])

    def copy(self, label):
        existing_workbook = self._find_existing_workbook_template(self.template.id, label)
        if existing_workbook is not None:
            return existing_workbook

        return TopicTemplate(label, self.template, package=self.package, is_copy=True)


class WorksheetTemplate(ItemTemplate, Worksheet):
    _annotation: Union[JournalTemplate, ReportTemplate]

    def __init__(self, label: str, worksheet: Worksheet, package: ItemList, is_copy: bool):
        if not isinstance(worksheet, Worksheet):
            raise SPyTypeError('worksheet parameter must be a Worksheet')

        ItemTemplate.__init__(self, label, worksheet, package, is_copy)

    def _find_existing_worksheet_template(self, worksheet_id, label):
        for worksheet in self.workbook.worksheets:
            if worksheet.id == f'{worksheet_id} {label}':
                return worksheet

        return None


class AnalysisWorksheetTemplate(WorksheetTemplate, AnalysisWorksheet):
    _annotation: JournalTemplate

    def __init__(self, label: str, workbook, worksheet: AnalysisWorksheet, *, package: ItemList = None,
                 is_copy: bool = False):
        if not isinstance(worksheet, AnalysisWorksheet):
            raise SPyTypeError('worksheet parameter must be a AnalysisWorksheet')

        WorksheetTemplate.__init__(self, label, worksheet, package, is_copy)
        AnalysisWorksheet.__init__(self, workbook, add_defaults=False)

        self._annotation = JournalTemplate(label, worksheet.journal, self, package)

        # Clear definition for clarity, since it's not used in a template
        self._definition = dict()

        self.worksteps = dict()
        for _, workstep in worksheet.worksteps.items():
            AnalysisWorkstepTemplate(label, self, workstep, package=package, is_copy=is_copy)

        self['Current Workstep ID'] = f"{self['Current Workstep ID']} {label}"

    @property
    def template(self) -> AnalysisWorksheet:
        # noinspection PyTypeChecker
        return self._template

    @property
    def parameters(self):
        if self._parameters is None and hasattr(self.workbook, 'parameters'):
            return self.workbook.parameters
        return self._parameters

    @parameters.setter
    def parameters(self, val):
        super()._set_parameters(val)

    def _code_variable(self):
        return 'worksheet'

    def code_dict(self):
        return ItemTemplate._code_dict_for_analysis(self.template.workbook, [self.template])

    @property
    def journal(self) -> JournalTemplate:
        return self._annotation

    def push(self, context: WorkbookPushContext, pushed_workbook_id, item_map, datasource_output,
             existing_worksheet_identifiers, include_inventory, label=None):
        output = super().push(context, pushed_workbook_id,
                              OverrideItemMap(item_map, template_parameters=self.parameters), datasource_output,
                              existing_worksheet_identifiers, include_inventory, self.label)

        self._after_push(item_map)

        return output

    def current_workstep(self) -> AnalysisWorkstepTemplate:
        # noinspection PyTypeChecker
        return super().current_workstep()

    def _branch_current_workstep(self):
        current_workstep_template = self.current_workstep()
        new_workstep = AnalysisWorkstepTemplate(
            _common.new_placeholder_guid(), self, current_workstep_template.template, package=self.package)

        self.worksteps[new_workstep['ID']] = new_workstep
        self.definition['Current Workstep ID'] = new_workstep['ID']

        return new_workstep

    def copy(self, label):
        existing_worksheet = self._find_existing_worksheet_template(self.template.id, label)
        if existing_worksheet is not None:
            return existing_worksheet

        return AnalysisWorksheetTemplate(label, self.workbook, self.template, package=self.package, is_copy=True)


class TopicDocumentTemplate(WorksheetTemplate, TopicDocument):
    _annotation: ReportTemplate

    def __init__(self, label: str, topic, topic_document: TopicDocument, *, package: ItemList = None,
                 is_copy: bool = False):
        if not isinstance(topic_document, TopicDocument):
            raise SPyTypeError('topic_document parameter must be a TopicDocument')

        WorksheetTemplate.__init__(self, label, topic_document, package, is_copy)
        TopicDocument.__init__(self, topic, add_defaults=False)

        self._annotation = ReportTemplate(label, topic_document.report, self, package, is_copy=is_copy)

        # Clear definition for clarity, since it's not used in a template
        self._definition = dict()

    @property
    def template(self) -> TopicDocument:
        # noinspection PyTypeChecker
        return self._template

    @property
    def parameters(self):
        if self._parameters is None:
            return self.workbook.parameters
        return self._parameters

    @parameters.setter
    def parameters(self, val):
        super()._set_parameters(val)

    def _code_variable(self):
        return 'document'

    def code_dict(self):
        return self._code_dict_for_topic([self._annotation])

    @property
    def report(self) -> ReportTemplate:
        return self._annotation

    def push(self, context: WorkbookPushContext, pushed_workbook_id, item_map, datasource_output,
             existing_worksheet_identifiers, include_inventory, label=None):
        output = super().push(context, pushed_workbook_id,
                              OverrideItemMap(item_map, template_parameters=self.parameters), datasource_output,
                              existing_worksheet_identifiers, include_inventory, self.label)

        self._after_push(item_map)

        return output

    def copy(self, label):
        existing_worksheet = self._find_existing_worksheet_template(self.template.id, label)
        if existing_worksheet is not None:
            return existing_worksheet

        return TopicDocumentTemplate(label, self.workbook, self.template, package=self.package, is_copy=True)


class AnalysisWorkstepTemplate(ItemTemplate, AnalysisWorkstep):
    def __init__(self, label: str, worksheet, workstep: AnalysisWorkstep, *, package: ItemList = None,
                 is_copy: bool = False):
        if not isinstance(workstep, AnalysisWorkstep):
            raise SPyTypeError('workstep parameter must be a AnalysisWorkstep')

        ItemTemplate.__init__(self, label, workstep, package, is_copy)
        AnalysisWorkstep.__init__(self, worksheet)

        # Clear definition for clarity, since it's not used in a template
        self._definition = dict()

        # Copy the Data property to the wrapper class so that it can be overridden
        self['Data'] = copy.deepcopy(workstep['Data'])

    @property
    def template(self) -> AnalysisWorkstep:
        # noinspection PyTypeChecker
        return self._template

    @property
    def parameters(self):
        if self._parameters is None:
            return self.worksheet.parameters
        return self._parameters

    @parameters.setter
    def parameters(self, val):
        super()._set_parameters(val)

    def _code_variable(self):
        return 'workstep'

    def code_dict(self):
        return ItemTemplate._code_dict_for_analysis(self.template.worksheet.workbook, [self.template])

    def push_to_specific_worksheet(self, session: Session, pushed_workbook_id, pushed_worksheet_output,
                                   item_map: OverrideItemMap, include_inventory, *,
                                   no_workstep_message=None):
        output = super().push_to_specific_worksheet(
            session, pushed_workbook_id, pushed_worksheet_output,
            OverrideItemMap(item_map, template_parameters=self.parameters), include_inventory,
            no_workstep_message=no_workstep_message)

        item_map.override(self.template.id, item_map[self.id])

        self._after_push(item_map)

        return output

    def _find_existing_workstep_template(self, workstep_id, label):
        for workstep in self.worksheet.worksteps.values():
            if workstep.id == f'{workstep_id} {label}':
                return workstep

        return None

    def copy(self, label):
        existing_workstep = self._find_existing_workstep_template(self.template.id, label)
        if existing_workstep is not None:
            return existing_workstep

        return AnalysisWorkstepTemplate(label, self.worksheet, self.template, package=self.package, is_copy=True)


class AnnotationTemplate(Annotation):
    def __init__(self, label: str, annotation: Annotation, worksheet: Worksheet, annotation_type: str,
                 is_copy: bool = False):
        if not isinstance(annotation, Annotation):
            raise SPyTypeError('annotation parameter must be a Annotation')

        Annotation.__init__(self, worksheet, annotation_type)

        self.label = label
        self.template = annotation
        self.is_copy = is_copy

    @property
    def id(self):
        return f'{self.template.id} {self.label}'

    @property
    def html(self):
        return self.template.html

    @property
    def images(self):
        return self.template.images

    def resolve_content_code_key(self, content_id):
        return content_id

    @staticmethod
    def _walk_dict(_dict: dict, f: Callable):
        n = dict()
        for k, v in _dict.items():
            if isinstance(v, dict):
                n[k] = AnnotationTemplate._walk_dict(v, f)
            elif isinstance(v, list):
                n[k] = AnnotationTemplate._walk_list(v, f)
            else:
                n[k] = f(k, v)

        return n

    @staticmethod
    def _walk_list(_list: list, f: Callable):
        n = list()
        for i in _list:
            if isinstance(i, dict):
                n.append(AnnotationTemplate._walk_dict(i, f))
            elif isinstance(i, list):
                n.append(AnnotationTemplate._walk_list(i, f))
            else:
                n.append(i)

        return n

    def _push_specific(self, session: Session, item_map: OverrideItemMap, datasource_output, label, new_annotation,
                       existing_annotation, access_control, override_content_dict=None, status: Status = None):
        item_map.override(self.template.id, existing_annotation.id)

        html = super()._push_specific(session, item_map, datasource_output, label, new_annotation, existing_annotation,
                                      access_control, override_content_dict=override_content_dict, status=status)

        return html

    def _after_push(self, item_map):
        if not self.is_copy:
            # We also map to the non-templated ID in case there's a piece of content that has no template parameters
            item_map[self.template.id] = item_map[self.id]


class JournalTemplate(AnnotationTemplate, Journal):
    def __init__(self, label: str, journal: Journal, worksheet: AnalysisWorksheet, package: ItemList, *,
                 is_copy: bool = False):
        if not isinstance(journal, Journal):
            raise SPyTypeError('journal parameter must be a Journal')

        AnnotationTemplate.__init__(self, label, journal, worksheet, 'Journal', is_copy=is_copy)

    def _push_specific(self, session: Session, item_map: OverrideItemMap, datasource_output, label, new_annotation,
                       existing_annotation, access_control, override_content_dict=None, status: Status = None):
        parameters = item_map.parameters

        if parameters is None:
            parameters = self.worksheet.code_dict()

        html = super()._push_specific(session, item_map, datasource_output, label, new_annotation, existing_annotation,
                                      access_control)

        mustache = MustachioedAnnotation(html)
        new_html = mustache.render(parameters)

        self._after_push(item_map)

        return new_html


class ReportTemplate(AnnotationTemplate, Report):
    def __init__(self, label: str, report: Report, topic_document: TopicDocumentTemplate, package: ItemList, *,
                 is_copy: bool = False):
        if not isinstance(report, Report):
            raise SPyTypeError('report parameter must be a Report')

        AnnotationTemplate.__init__(self, label, report, topic_document, 'Report', is_copy=is_copy)

        def _to_template(_input_dict, _clazz):
            _dict = dict()
            for _id, _dr in _input_dict.items():
                _instance = _clazz(label, _dr, self, package=package, is_copy=is_copy)
                _dict[_id] = _instance

            return _dict

        self.date_ranges = _to_template(report.date_ranges, DateRangeTemplate)
        self.asset_selections = _to_template(report.asset_selections, AssetSelectionTemplate)
        self.content = _to_template(report.content, ContentTemplate)

        self.schedule = copy.deepcopy(report.schedule)
        self.data = report.data

    def resolve_content_code_key(self, content_id):
        content = self.content[content_id]
        workbook_templates = [w for w in self.worksheet.package
                              if w.template.id == content['Workbook ID'] and not w.is_copy]
        if len(workbook_templates) == 0:
            raise SPyValueError(f'Content with ID "{content.id}" references workbook ID "'
                                f'{content["Workbook ID"]}" but that workbook was not loaded at the same time as '
                                f'this topic.')

        workbook_template = workbook_templates[0]
        # noinspection PyUnresolvedReferences
        workbook = workbook_template.template
        worksheet = workbook.worksheets[content['Worksheet ID']]
        return ItemTemplate.code_key_string(content_id, Reference.EMBEDDED_CONTENT, worksheet.fqn)

    def _push_specific(self, session: Session, item_map: OverrideItemMap, datasource_output, label, new_annotation,
                       existing_annotation, access_control, override_content_dict=None, status: Status = None):
        current_content_dict = self.content.copy()
        new_content_dict = dict()

        def _add_to_map(_id):
            # This is used when the content is being mapped to a non-template worksheet/workstep. In such a case,
            # it is assumed that the ID already exists on the server and we can use it directly.
            if _id not in item_map:
                item_map[_id] = _id
            return _id

        def _workstep_to_content(_key, _val):
            _id, _type, _fqn = ItemTemplate.code_key_tuple(_key)
            if _id is None or _id not in current_content_dict:
                return _val

            _current_content: ContentTemplate = current_content_dict[_id]

            if _val is None:
                new_content_dict[_current_content.id] = _current_content
                return _id

            _new_content = _current_content.copy(f'{self.label} {_val}')

            if isinstance(_val, AnalysisWorkstepTemplate):
                _new_content['Workbook ID'] = _val.worksheet.workbook.id
                _new_content['Worksheet ID'] = _val.worksheet.id
                _new_content['Workstep ID'] = _val.id
            elif isinstance(_val, AnalysisWorksheetTemplate):
                _new_content['Workbook ID'] = _val.workbook.id
                _new_content['Worksheet ID'] = _val.id
                _new_content['Workstep ID'] = _val.current_workstep().id
            elif isinstance(_val, AnalysisWorkstep):
                _new_content['Workbook ID'] = _add_to_map(_val.worksheet.workbook.id)
                _new_content['Worksheet ID'] = _add_to_map(_val.worksheet.id)
                _new_content['Workstep ID'] = _add_to_map(_val.id)
            elif isinstance(_val, AnalysisWorksheet):
                _new_content['Workbook ID'] = _add_to_map(_val.workbook.id)
                _new_content['Worksheet ID'] = _add_to_map(_val.id)
                _new_content['Workstep ID'] = _add_to_map(_val.current_workstep().id)
            else:
                raise SPyTypeError(f'Template parameter {_key} is of type {type(_val)} -- needs to '
                                   f'be AnalysisWorksheet or AnalysisWorkstep')

            new_content_dict[_new_content.id] = _new_content

            return _new_content

        parameters = item_map.parameters

        if parameters is None:
            parameters = self.worksheet.code_dict()

        massaged_parameters = AnnotationTemplate._walk_dict(parameters, _workstep_to_content)

        html = super()._push_specific(session, item_map, datasource_output, label, new_annotation, existing_annotation,
                                      access_control, new_content_dict, status)

        item_map[f'Content for {self.id}'] = new_content_dict

        def _content_to_id(_key, _val):
            if not isinstance(_val, ContentTemplate):
                return _val

            return item_map[_val.id]

        massaged_parameters = AnnotationTemplate._walk_dict(massaged_parameters, _content_to_id)

        def _image_file_to_img_src(_key, _val):
            if '[Image]' not in _key:
                return _val

            if _val is None:
                raise SPyValueError(f'Template parameter value for "{_key}" is missing. You must supply a filename '
                                    f'for an image file.')

            if not isinstance(_val, str):
                raise SPyTypeError(f'Template parameter value for "{_key}" must be a string (the filename of the '
                                   f'image)')

            if not util.safe_exists(_val):
                raise SPyValueError(f'Image file "{_val}" does not exist (for template parameter "{_key}")')

            _, _image_format = os.path.splitext(_val)
            _image_name = f'{_common.new_placeholder_guid()}{_image_format.lower()}'
            with util.safe_open(_val, 'rb') as _img:
                self.images[(item_map[self.id], _image_name)] = _img.read()

            return f'/api/annotations/{item_map[self.id]}/images/{_image_name}'

        massaged_parameters = AnnotationTemplate._walk_dict(massaged_parameters, _image_file_to_img_src)

        mustache = MustachioedAnnotation(html, lambda content_id: self.resolve_content_code_key(content_id))

        new_html = mustache.render(massaged_parameters)

        self._after_push(item_map)

        return new_html


class DateRangeTemplate(ItemTemplate, DateRange):
    def __init__(self, label: str, date_range: DateRange, report_template: ReportTemplate, package: ItemList, *,
                 is_copy: bool = False):
        if not isinstance(date_range, DateRange):
            raise SPyTypeError('date_range parameter must be a DateRange')

        ItemTemplate.__init__(self, label, date_range, package, is_copy)
        DateRange.__init__(self, date_range, report_template)

        # Clear definition for clarity, since it's not used in a template
        self._definition = dict()

    def push(self, session: Session, item_map: OverrideItemMap, existing_date_ranges: dict, status: Status):
        output = super().push(session, item_map, existing_date_ranges, status)

        item_map.override(self.template.id, item_map[self.id])

        self._after_push(item_map)

        return output


class AssetSelectionTemplate(ItemTemplate, AssetSelection):
    def __init__(self, label: str, asset_selection: AssetSelection, report_template: ReportTemplate,
                 *, package: ItemList = None, is_copy: bool = False):
        if not isinstance(asset_selection, AssetSelection):
            raise SPyTypeError('asset_selection parameter must be a AssetSelection')

        ItemTemplate.__init__(self, label, asset_selection, package, is_copy)
        AssetSelection.__init__(self, asset_selection, report_template)

        # Clear definition for clarity, since it's not used in a template
        self._definition = dict()

    def push(self, session: Session, item_map: OverrideItemMap, existing_asset_selections: dict, status: Status):
        output = super().push(session, item_map, existing_asset_selections, status)

        item_map.override(self.template.id, item_map[self.id])

        self._after_push(item_map)

        return output


class ContentTemplate(ItemTemplate, Content):
    template: Content

    def __init__(self, label: str, content: Content, report_template: ReportTemplate, package: ItemList, *,
                 is_copy: bool = False):
        if not isinstance(content, Content):
            raise SPyTypeError('content parameter must be a Content')

        ItemTemplate.__init__(self, label, content, package, is_copy)
        Content.__init__(self, content, report_template)

    def copy(self, label):
        return ContentTemplate(label, self.template, self.report, package=self.package, is_copy=True)

    def push(self, session: Session, item_map: OverrideItemMap, existing_contents: dict, status: Status):
        output = super().push(session, item_map, existing_contents, status)

        item_map.override(self.template.id, item_map[self.id])

        self._after_push(item_map)

        return output

    @property
    def workstep(self) -> AnalysisWorkstepTemplate:
        workbook_templates = [w for w in self.package
                              if w.template.id == self['Workbook ID'] and not w.is_copy]
        if len(workbook_templates) == 0:
            raise SPyValueError(f'Content with ID "{self.id}" references workbook ID "'
                                f'{self["Workbook ID"]}" but that workbook was not loaded at the same time as '
                                f'this topic.')

        workbook_template = workbook_templates[0]
        worksheet_templates = [w for w in workbook_template.worksheets
                               if w.template.id == self['Worksheet ID'] and not w.is_copy]
        if len(worksheet_templates) == 0:
            raise SPyValueError(f'Content with ID "{self.id}" references worksheet ID "'
                                f'{self["Worksheet ID"]}" but it was not found in the workbook ID '
                                f'{self["Workbook ID"]}.')

        worksheet_template = worksheet_templates[0]
        workstep_templates = [w for w in worksheet_template.worksteps.values()
                              if w.template.id == self['Workstep ID'] and not w.is_copy]
        if len(workstep_templates) == 0:
            raise SPyValueError(f'Content with ID "{self.id}" references workstep ID "'
                                f'{self["Workstep ID"]}" but it was not found '
                                f'in the worksheet ID {self["Worksheet ID"]} '
                                f'in the workbook ID {self["Workbook ID"]}.')

        return workstep_templates[0]


def _get_template_class(workbook):
    return AnalysisTemplate if isinstance(workbook, Analysis) else TopicTemplate


def package_as_templates(workbooks: WorkbookList, label: str) -> WorkbookList:
    package = WorkbookList()
    for workbook in workbooks:
        package.append(_get_template_class(workbook)(label, workbook, package=package))
    return package
