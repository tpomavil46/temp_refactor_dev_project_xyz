from __future__ import annotations

import io
import json
import os
import re
import types
from typing import List, Optional, Union

import requests

from seeq.base import util
from seeq.base.seeq_names import SeeqNames
from seeq.sdk import *
from seeq.spy import _common
from seeq.spy import _login
from seeq.spy._errors import *
from seeq.spy._redaction import safely, request_safely
from seeq.spy._session import Session
from seeq.spy._status import Status
from seeq.spy.workbooks import _item, _render
from seeq.spy.workbooks._content import Content, DateRange, AssetSelection
from seeq.spy.workbooks._item import Item
from seeq.spy.workbooks._item_map import ItemMap, OverrideItemMap
from seeq.spy.workbooks._user import ItemWithOwnerAndAcl, Identity
from seeq.spy.workbooks._workstep import AnalysisWorkstep

FIXUP_PREFIX = 'Annotation to fix up'


class Annotation(Item):
    _html: str
    _images: dict

    def __init__(self, worksheet, annotation_type):
        super().__init__()
        self.annotation_type = annotation_type
        self.worksheet = worksheet
        self._html = ''
        self._images = dict()
        self.plots_to_render = set()
        if not _common.present(self.worksheet, 'Annotation ID'):
            self.worksheet['Annotation ID'] = _common.new_placeholder_guid()

    @property
    def id(self):
        # We store this on the worksheet because it's the only piece of metadata we need for the annotation and it's
        # convenient to have it in the worksheet's JSON when saved.
        return _common.get(self.worksheet, 'Annotation ID')

    @id.setter
    def id(self, _id):
        self.worksheet['Annotation ID'] = _id

    def refresh_from(self, new_item, item_map: ItemMap, status: Status):
        super().refresh_from(new_item, item_map, status)

        self.annotation_type = new_item.annotation_type
        # Note that we purposefully don't touch the worksheet reference, since it will have stayed the same
        self._set_html(new_item.html)
        self._images = new_item.images

    @staticmethod
    def _find_image_references(html):
        if not html:
            return list()

        matches = re.finditer(r'src="/api(/annotations/(.*?)/images/(.*?))"', html)
        return [(match.group(1), match.group(2), match.group(3)) for match in matches]

    @property
    def html(self):
        return self._html

    @html.setter
    def html(self, value):
        self._set_html(value)

    def _set_html(self, value):
        if value is None:
            self._html = ''
        else:
            self._html = value

    @property
    def images(self):
        return self._images

    @property
    def referenced_items(self):
        return list()

    @property
    def referenced_worksteps(self):
        return self.find_workstep_references()

    def find_workstep_references(self, item_map: Optional[ItemMap] = None):
        return set()

    def find_workbook_links(self, session: Session, status: Status):
        if not self.html:
            return dict()

        url = _common.get(self.worksheet.workbook, 'Original Server URL')
        if not url:
            return dict()

        # TODO can this be converted to use the _common.workbook_worksheet_url_regex methods?
        edit_link_no_folder_regex = \
            r'%s/workbook/(?P<workbook>%s)/worksheet/(?P<worksheet>%s)' % (url,
                                                                           _common.GUID_REGEX,
                                                                           _common.GUID_REGEX)

        edit_link_with_folder_regex = \
            r'%s/%s/workbook/(?P<workbook>%s)/worksheet/(?P<worksheet>%s)' % (url,
                                                                              _common.GUID_REGEX,
                                                                              _common.GUID_REGEX,
                                                                              _common.GUID_REGEX)

        view_link_regex = \
            r'%s/view/(?P<worksheet>%s)' % (url, _common.GUID_REGEX)

        present_link_regex = \
            r'%s/present/worksheet/(?P<workbook>%s)/(?P<worksheet>%s)' % (url,
                                                                          _common.GUID_REGEX,
                                                                          _common.GUID_REGEX)

        workstep_tuples = dict()
        for regex in [edit_link_no_folder_regex, edit_link_with_folder_regex, view_link_regex, present_link_regex]:
            matches = re.finditer(regex, self.html, re.IGNORECASE)

            for match in matches:
                group_dict = dict(match.groupdict())
                if 'workbook' not in group_dict:
                    items_api = ItemsApi(session.client)
                    _worksheet_id = group_dict['worksheet']
                    item_output = safely(
                        lambda: items_api.get_item_and_all_properties(id=_worksheet_id),
                        action_description=f'get the Workbook ID for Worksheet {_worksheet_id}',
                        status=status)  # type: ItemOutputV1
                    if item_output is not None:
                        group_dict['workbook'] = item_output.workbook_id

                if group_dict['workbook'].upper() not in workstep_tuples:
                    workstep_tuples[group_dict['workbook'].upper()] = set()

                workstep_tuples[group_dict['workbook'].upper()].add(
                    (group_dict['workbook'].upper(), group_dict['worksheet'].upper(), None))

        return workstep_tuples

    def pull(self, session: Session, *, include_images=True, include_archived=True,
             status=None) -> Optional[AnnotationOutputV1]:
        session = Session.validate(session)
        self._images = dict()
        annotations_api = AnnotationsApi(session.client)
        # Note: get_annotations() won't error if the user doesn't have access to the `annotates` IDs
        _annotations = annotations_api.get_annotations(annotates=[self.worksheet.id])  # type: AnnotationListOutputV1

        annotation_output = None
        for annotation_item in _annotations.items:  # type: AnnotationOutputV1
            candidate = safely(lambda: annotations_api.get_annotation(id=annotation_item.id),
                               action_description=f'get details for Annotation {annotation_item.id}',
                               status=status)  # AnnotationOutputV1
            if candidate is not None and candidate.type == self.annotation_type:
                annotation_output = candidate
                break

        if not annotation_output:
            # This seems possible (although rare) and it seems like the annotation gets created immediately if the
            # user clicks on the Journal tab. So just return nothing but allow this Annotation object to persist with
            # a placeholder ID.
            return None

        self.id = annotation_output.id
        self._set_html(annotation_output.document)

        definition = safely(
            lambda: Item._dict_from_item_output(Item._get_item_output(session, self.id)),
            action_description=f'get properties of Annotation {self.id}',
            status=status)

        self._definition = definition

        if include_images:
            image_references = Annotation._find_image_references(self.html)
            for query_params, annotation_id, image_id in image_references:
                if (annotation_id, image_id) in self.images:
                    continue

                self.worksheet.workbook.update_status('Pulling image', 1)

                api_client_url = session.get_api_url()
                request_url = api_client_url + query_params

                self.images[(annotation_id, image_id)] = _login.pull_image(session, request_url)

        return annotation_output

    def push(self, session: Session, pushed_workbook_id, pushed_worksheet_id, item_map, datasource_output,
             access_control, push_images, label, status=None):
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            raise SPyDependencyNotFound(f'`BeautifulSoup4` is required to push annotations. Please install it using '
                                        f'`pip install seeq-spy[templates]` to use this feature.')
        self.render_plots()

        new_annotation = AnnotationInputV1()
        new_annotation.document = 'Placeholder document'
        new_annotation.name = 'Unnamed'
        new_annotation.description = 'Placeholder description'
        new_annotation.type = self.annotation_type

        annotations_api = AnnotationsApi(session.client)
        items_api = ItemsApi(session.client)
        # Note: get_annotations() won't error if the user doesn't have access to the `annotates` IDs
        existing_annotations = annotations_api.get_annotations(
            annotates=[pushed_worksheet_id])  # type: AnnotationListOutputV1

        relevant_annotations = [a for a in existing_annotations.items if a.type == self.annotation_type]
        if len(relevant_annotations) == 0:
            if isinstance(self, Report):
                # Creating a report requires an OptionalReportInputV1
                new_annotation.report_input = OptionalReportInputV1()
            else:
                new_annotation.interests = [
                    AnnotationInterestInputV1(interest_id=pushed_worksheet_id)
                ]

                if isinstance(self, Journal):
                    # Reports cannot have an interest to the workbook, see CRAB-18738
                    new_annotation.interests.append(AnnotationInterestInputV1(interest_id=pushed_workbook_id))

            relevant_annotation = safely(lambda: annotations_api.create_annotation(body=new_annotation),
                                         action_description=f'create {new_annotation.type} {new_annotation.name}',
                                         status=status)  # type: AnnotationOutputV1
            if relevant_annotation is None:
                return
        else:
            relevant_annotation = relevant_annotations[0]

        item_map[self.id] = relevant_annotation.id

        html = self._push_specific(session, item_map, datasource_output, label, new_annotation, relevant_annotation,
                                   access_control, status=status)

        # Create an override map for image mapping, which can be different for the same annotation in the case of a
        # template
        images_map = dict()

        # We add a prefix to existing image references in the HTML so that we can handle cases where
        # the names that are picked by "POST /annotations/{id}/images" overlap with existing names and would
        # otherwise cause the find & replace that happens later to be non-deterministic
        image_prefix = 'ToBeMapped_'

        if push_images:
            item_map[f'Images for {self.id}'] = images_map

            for _, annotation_id, image_id in Annotation._find_image_references(html):
                api_client_url = session.get_api_url()
                request_url = api_client_url + '/annotations/%s/images' % relevant_annotation.id

                self.worksheet.workbook.update_status('Pushing image', 1)

                response = requests.post(url=request_url,
                                         files={
                                             "file": (image_id, io.BytesIO(self.images[(annotation_id, image_id)]))
                                         },
                                         headers={
                                             "Accept": "application/vnd.seeq.v1+json",
                                             "x-sq-auth": session.client.auth_token
                                         },
                                         verify=session.https_verify_ssl)

                if response.status_code != 201:
                    raise SPyRuntimeError(
                        f'Could not upload image file {image_id} for worksheet {pushed_worksheet_id}:\n'
                        f'Response code: {response.status_code}\n'
                        f'Response content: {response.content}')

                link_json = json.loads(response.content)

                match = re.match(r'.*?images/(.*)', link_json['link'])
                new_image_id = match.group(1)

                images_map[f'{image_prefix}{image_id}'] = new_image_id
        else:
            images_map = item_map[f'Images for {self.id}']

        html = re.sub(r'src="(/api/annotations/.*?/images/)(.*?)"', rf'src="\1{image_prefix}\2"', html)

        bs = BeautifulSoup(html, features='html.parser')
        find_result = bs.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'title'])
        name = 'Unnamed'
        description = None

        if len(find_result) > 0:
            name = ' '.join(re.split(r'[\s\n]+', find_result[0].get_text().strip())[:20])
        if len(find_result) > 1:
            description = ' '.join(re.split(r'[\s\n]+', find_result[1].get_text().strip())[:50])

        html = self._replace_items_in_html(OverrideItemMap(item_map, override_map=images_map), html)

        worksheet_link_replacement = r'links?type=workstep&amp;workbook=%s&amp;worksheet=%s&amp;' % (
            pushed_workbook_id, pushed_worksheet_id
        )

        html = re.sub(_common.WORKSHEET_LINK_REGEX, worksheet_link_replacement, html, flags=re.IGNORECASE)

        original_server_url = _common.get(self.worksheet.workbook, 'Original Server URL')
        new_server_url = _item.get_canonical_server_url(session)
        if original_server_url is not None and new_server_url is not None:
            item_map[original_server_url] = new_server_url

        new_annotation.name = name if len(name.strip()) > 0 else 'Unnamed'
        new_annotation.description = description
        new_annotation.document = html
        new_annotation.interests = list()

        for interest in relevant_annotation.interests:  # type: AnnotationInterestOutputV1
            interest_item = interest.item  # type: ItemPreviewV1
            # At Chevron, we encountered a case where there were multiple interests returned with the same ID, which
            # caused Appserver to choke when updating the annotation. So filter those out.
            if any(interest_item.id == i.interest_id for i in new_annotation.interests):
                continue
            if isinstance(self, Report) and interest_item.id == pushed_workbook_id:
                # Reports cannot have an interest to the workbook, see CRAB-18738
                continue
            new_interest = AnnotationInterestInputV1()
            new_interest.interest_id = interest_item.id
            if interest.capsule is not None:
                new_interest.detail_id = interest.capsule.id
            new_annotation.interests.append(new_interest)

        new_annotation.created_by_id = relevant_annotation.created_by.id
        safely(lambda: annotations_api.update_annotation(id=relevant_annotation.id, body=new_annotation),
               action_description=f'update Annotation {relevant_annotation.id}',
               status=status, additional_errors=[400])

        # Annotations have a lot of properties that are managed by Appserver and shouldn't be pushed. We need to push
        # only a small, curated set that are actually configuration items on the Topic Document.
        properties_to_push = [
            SeeqNames.Properties.margin_top,
            SeeqNames.Properties.margin_bottom,
            SeeqNames.Properties.margin_left,
            SeeqNames.Properties.margin_right,
            SeeqNames.Properties.orientation,
            SeeqNames.Properties.page_size,
            SeeqNames.Properties.is_fixed_with,
            SeeqNames.Properties.discoverable,
            SeeqNames.Properties.timezone,
            SeeqNames.Properties.is_ck_enabled
        ]

        added_or_updated_properties = [ScalarPropertyV1(name=k, value=v)
                                       for k, v in self.definition_dict.items()
                                       if k in properties_to_push and v is not None]

        deleted_properties = [ScalarPropertyV1(name=k, value=v)
                              for k, v in self.definition_dict.items()
                              if k in properties_to_push and v is None]

        if len(added_or_updated_properties) > 0:
            safely(lambda: items_api.set_properties(id=relevant_annotation.id, body=added_or_updated_properties),
                   action_description=f'set properties for Annotation {relevant_annotation.id}',
                   status=status, additional_errors=[400])

        if len(deleted_properties) > 0:
            for deleted_property in deleted_properties:
                safely(lambda: items_api.delete_property(id=relevant_annotation.id,
                                                         property_name=deleted_property.name),
                       action_description=f'delete property {deleted_property.name} '
                                          f'for Annotation {relevant_annotation.id}',
                       status=status, additional_errors=[400])

        item_map[f'{FIXUP_PREFIX} - {self.id}'] = types.SimpleNamespace(id=relevant_annotation.id, body=new_annotation)

    @staticmethod
    def push_fixups(session: Session, status: Status, item_map: ItemMap):
        annotations_api = AnnotationsApi(session.client)

        for key in item_map.keys():
            if not key.startswith(FIXUP_PREFIX):
                continue

            annotation = item_map[key]

            annotation.body.document = _item.replace_items(annotation.body.document, item_map)

            safely(lambda: annotations_api.update_annotation(id=annotation.id, body=annotation.body),
                   action_description=f'fixup Annotation {annotation.id}',
                   status=status, additional_errors=[400])

    def _replace_items_in_html(self, item_map: ItemMap, html):
        # When a workbook is duplicated via the Workbench UI, the workstep links within Journals actually refer to
        # the original workbook. This works in the UI because workstep content has no real dependency on the
        # workbook/worksheet they're associated with. When pulling, we accommodate this by pulling a Workstep and
        # associating it with the "proper" Worksheet object, but then during push we have to fix up the links in case
        # the "original" workbook/worksheet wasn't included in the workbooks to be pushed.
        workstep_map = _common.get(item_map, self.worksheet.item_map_worksteps_key())
        if workstep_map:
            html = _item.replace_items(html, workstep_map)

        html = _item.replace_items(html, item_map)

        return html

    def _push_specific(self, session: Session, item_map, datasource_output, label,
                       new_annotation: AnnotationInputV1, annotation: AnnotationOutputV1,
                       access_control, override_content_dict=None, status=None):
        # This will be overridden in derived classes to do some specific work like pushing Content and DateRanges
        return self.html

    def _get_annotation_file(self, workbook_folder):
        return os.path.join(workbook_folder, '%s_%s' % (self.annotation_type, self.worksheet.id))

    @staticmethod
    def _get_image_file(workbook_folder, image_id_tuple):
        return os.path.join(workbook_folder, 'Image_%s_%s' % image_id_tuple)

    @staticmethod
    def _get_html_attr(fragment, attribute):
        attr_match = re.findall(r'\s+%s="(.*?)"' % attribute, fragment)
        return attr_match[0] if len(attr_match) > 0 else None

    def save(self, workbook_folder, *, include_rendered_content=False, pretty_print_html=False):
        self.render_plots()
        html_file = f'{self._get_annotation_file(workbook_folder)}.html'
        with util.safe_open(html_file, 'w', encoding='utf-8') as f:
            if self.html:
                if pretty_print_html:
                    try:
                        from bs4 import BeautifulSoup
                    except ImportError:
                        raise SPyDependencyNotFound(f'`BeautifulSoup4` is required to save pretty-printed HTML. Please '
                                                    f'install it using `pip install seeq-spy[templates]`'
                                                    f' to use this feature.')
                    html_to_save = BeautifulSoup(self.html, features='html.parser').prettify()
                    html_to_save = _common.fix_up_ckeditor_curly_brace_weirdness(html_to_save)
                    # If we don't trim the spaces within <a> tags, you'll get extra spaces underlined in the UI
                    html_to_save = re.sub(r'(<a .*?>)[\s\n]+(.*?)[\s\n]+(</a>)', r'\1\2\3', html_to_save)
                else:
                    html_to_save = self.html

                f.write(html_to_save)

        _common.save_image_files(self.images, workbook_folder)

        json_file = f'{self._get_annotation_file(workbook_folder)}.json'
        json_dict = {
            'Definition': self.definition_dict,
        }
        self._update_save_json_dict(json_dict)
        with util.safe_open(json_file, 'w', encoding='utf-8') as f:
            json.dump(json_dict, f, indent=4, sort_keys=True)

    def _update_save_json_dict(self, json_dict):
        # Overridden in base class
        pass

    def _load(self, workbook_folder: str) -> dict:
        html_file = f'{self._get_annotation_file(workbook_folder)}.html'
        json_file = f'{self._get_annotation_file(workbook_folder)}.json'

        if util.safe_exists(json_file):
            with util.safe_open(json_file, 'r', encoding='utf-8') as f:
                json_dict = json.load(f)
        else:
            json_dict = dict()

        if 'Definition' in json_dict:
            self._definition = json_dict['Definition']

        with util.safe_open(html_file, 'r', encoding='utf-8') as f:
            self.html = f.read()

        matches = re.finditer(r'src="/api(/annotations/(.*?)/images/(.*?))"', self.html)
        for match in matches:
            image_id_tuple = (match.group(2), match.group(3))
            image_file = _common.get_image_file(workbook_folder, image_id_tuple)

            with util.safe_open(image_file, 'rb') as f:
                self.images[image_id_tuple] = f.read()

        return json_dict

    def add_image(self, *, filename=None, buffer=None, image_format=None, placement=None, just_src=False):
        """
        Add an image to the annotation.

        Parameters
        ----------
        filename: str
            The full path to the image file
        buffer: str
            The bytes of the image in memory (must also specify image_format)
        image_format
            The image format of what is supplied in bytes (e.g. 'png', 'jpg')
        placement : {'end', 'beginning', None}, default None
            The location to add the image to an existing document.
        just_src : bool
            False if full <img> html tags desired, True if you just want the
            url to put in the <img src="<url>"> attribute yourself.
        """
        if filename and buffer:
            raise SPyValueError('Either filename or buffer must be supplied to image function -- not both')

        if buffer and not image_format:
            raise SPyValueError('image_format must be specified if buffer is supplied')

        if placement not in ['end', 'beginning', None]:
            raise SPyValueError(f"placement must be one of {['end', 'beginning', None]}")

        if placement and just_src:
            raise SPyValueError(f"placement must None if just_src is True")

        html = self.html
        if filename:
            image_name = os.path.basename(filename)
            with util.safe_open(filename, 'rb') as img:
                self.images[(self.id, image_name)] = img.read()
        else:
            image_name = f'{_common.new_placeholder_guid()}.{image_format}'
            self.images[(self.id, image_name)] = buffer

        url = f'/api/annotations/{self.id}/images/{image_name}'
        if just_src:
            return url

        image_html = f'<img class="fr-fic fr-fin fr-dii" src="{url}">'
        if placement is not None:
            if placement == 'beginning':
                html = image_html + html
            else:
                html += image_html
            self._set_html(html)

        return image_html

    def add_plot_to_render(self, plot_render_info, date_range):
        image_id = _common.new_placeholder_guid()
        filename = f'{image_id}.{plot_render_info.image_format}'
        self.plots_to_render.add((self.id, filename, plot_render_info, date_range))
        return f'<img class="fr-fic fr-fin fr-dii" src="/api/annotations/{self.id}/images/{filename}"/>'

    def render_plots(self):
        for annotation_id, filename, plot_render_info, date_range in self.plots_to_render:
            self.images[annotation_id, filename] = plot_render_info.render_function(date_range)

        self.plots_to_render.clear()


class Journal(Annotation):

    def __init__(self, worksheet):
        super().__init__(worksheet, 'Journal')

    @staticmethod
    def load(worksheet, workbook_folder):
        journal = Journal(worksheet)
        journal._load(workbook_folder)
        return journal

    @property
    def referenced_items(self):
        referenced_items = set()
        if self.html:
            matches = re.finditer(r'item%s(%s)' % (_common.HTML_EQUALS_REGEX, _common.GUID_REGEX), self.html,
                                  re.IGNORECASE)
            for match in matches:
                referenced_items.add(_item.Reference(match.group(1).upper(), _item.Reference.JOURNAL,
                                                     self.worksheet))

        return list(referenced_items)

    def find_workstep_references(self, *, item_map: Optional[ItemMap] = None):
        if not self.html:
            return set()

        workstep_references = set()
        regex = r'workbook%s(%s)&amp;worksheet%s(%s)&amp;workstep%s(%s)' % (
            _common.HTML_EQUALS_REGEX, _common.GUID_REGEX,
            _common.HTML_EQUALS_REGEX, _common.GUID_REGEX,
            _common.HTML_EQUALS_REGEX, _common.GUID_REGEX)
        matches = re.finditer(regex, self.html, re.IGNORECASE)

        for match in matches:
            workstep_references.add((match.group(1).upper(), match.group(2).upper(), match.group(3).upper()))

        return workstep_references


class Report(Annotation):
    rendered_content_images: Optional[dict]

    def __init__(self, worksheet):
        super().__init__(worksheet, 'Report')

        self.rendered_content_images = None

        self.date_ranges = dict()
        self.asset_selections = dict()
        self.content = dict()
        self.schedule = None
        self.data = None

    @property
    def referenced_items(self):
        referenced_items = set()

        for date_range in self.date_ranges.values():
            if _common.present(date_range, 'Condition ID'):
                referenced_items.add(
                    _item.Reference(date_range['Condition ID'], _item.Reference.DATE_RANGE_CONDITION,
                                    self.worksheet))

        for asset_selection in self.asset_selections.values():
            if _common.present(asset_selection, 'Asset ID'):
                referenced_items.add(_item.Reference(asset_selection['Asset ID'], _item.Reference.ASSET_SELECTION,
                                                     self.worksheet))

        return list(referenced_items)

    def pull(self, session: Session, *, include_images=True, include_archived=True,
             status=None) -> Optional[AnnotationOutputV1]:
        session = Session.validate(session)
        annotation_output = super().pull(session, include_images=include_images, include_archived=include_archived,
                                         status=status)
        if annotation_output is None:
            return

        # A lot of these come down as item properties, but we map them here anyways just to be double-sure.
        # This also converts date/times to human-readable.
        self._definition.update(Item.dict_via_attribute_map(annotation_output, {
            'ck_enabled': SeeqNames.Properties.is_ck_enabled,
            'created_at': 'Created At',
            'updated_at': 'Updated At',
            'discoverable': SeeqNames.Properties.discoverable,
            'fixed_width': SeeqNames.Properties.is_fixed_with,
            'margin_bottom': SeeqNames.Properties.margin_bottom,
            'margin_left': SeeqNames.Properties.margin_left,
            'margin_right': SeeqNames.Properties.margin_right,
            'margin_top': SeeqNames.Properties.margin_top,
            'next_run_time': 'Next Run Time',
            'page_orientation': SeeqNames.Properties.orientation,
            'page_size': SeeqNames.Properties.page_size,
            'published_at': 'Published At',
            'timezone': SeeqNames.Properties.timezone,
        }))

        content_api = ContentApi(session.client)
        contents = safely(
            lambda: content_api.get_contents_with_all_metadata(report_id=annotation_output.id),
            action_description=f'get Content items within Report {annotation_output.id}',
            status=status)
        if contents is None:
            return

        for content in contents.content_items:  # type: ContentWithMetadataOutputV1
            if content.is_archived and not include_archived:
                continue

            if content.date_range is not None:
                new_date_range = DateRange.pull(content.date_range, report=self, annotation_output=annotation_output,
                                                session=session, status=status)
                self.date_ranges[new_date_range.id] = new_date_range

            if content.asset_selection is not None:
                new_asset_selection = AssetSelection.pull(content.asset_selection, report=self, session=session,
                                                          status=status)
                self.asset_selections[new_asset_selection.id] = new_asset_selection

            new_content = Content.from_content_output(content, self)
            self.content[new_content.definition['ID']] = new_content

        # Go back through the date range IDs on the annotation in case they aren't associated with any content and
        # therefore won't be in the output from the get_contents_with_all_metadata call.
        for date_range_id in annotation_output.date_range_ids:
            if date_range_id not in self.date_ranges.keys():
                new_date_range = DateRange.pull(date_range_id, report=self, session=session, status=status)

                if new_date_range.get('Archived', False) and not include_archived:
                    continue

                self.date_ranges[new_date_range.id] = new_date_range

        # Go back through the asset selection IDs on the annotation in case they aren't associated with any content
        # and therefore won't be in the output from the get_contents_with_all_metadata call.
        for asset_selection_id in annotation_output.asset_selection_ids:
            if asset_selection_id not in self.asset_selections.keys():
                new_asset_selection = AssetSelection.pull(asset_selection_id, report=self, session=session,
                                                          status=status)

                if new_asset_selection.get('Archived', False) and not include_archived:
                    continue

                self.asset_selections[new_asset_selection.id] = new_asset_selection

        # There is a cron schedule only if it is not null or the list is not empty
        if annotation_output.cron_schedule:
            self.schedule = {
                'Cron Schedule': annotation_output.cron_schedule,
                'Background': annotation_output.background,
                'Enabled': annotation_output.enabled
            }

            self._pull_notification_config(status, annotation_output)

    def _pull_notification_config(self, status: Status, annotation_output):
        # noinspection PyBroadException
        try:
            notification_configurations_api = NotificationConfigurationsApi(status.session.client)
            notification_config = notification_configurations_api.get_notification_configuration(
                id=annotation_output.id)

            self.schedule['Notification'] = {
                'To Email Recipients': Report._email_recipients_to_dict(
                    status, notification_config.to_email_recipients
                ),
                'CC Email Recipients': Report._email_recipients_to_dict(
                    status, notification_config.cc_email_recipients
                ),
                'BCC Email Recipients': Report._email_recipients_to_dict(
                    status, notification_config.bcc_email_recipients
                ),
                'Report Format': notification_config.report_format
            }

        except NameError:
            # We're on an earlier version of Seeq that doesn't have the NotificationConfigurationsApi
            pass

        except ApiException as e:
            # A 404 error means that the Notification Configuration doesn't exist on this document
            if e.status != 404:
                raise

    @staticmethod
    def _email_recipients_to_dict(status: Status, email_recipients: List[EmailNotificationRecipientOutputV1]):
        recipient_dicts = list()
        for email_recipient in email_recipients:
            recipient_dict = dict()
            if email_recipient.identity_id is not None:
                identity = Identity.pull(email_recipient.identity_id, status=status, session=status.session)
                recipient_dict['Identity'] = identity.definition_dict
                recipient_dict['Email'] = identity.get('Email')
                recipient_dict['Name'] = identity.get('Name')

            if email_recipient.email_address:
                recipient_dict['Email'] = email_recipient.email_address

            if email_recipient.name:
                recipient_dict['Name'] = email_recipient.name

            if email_recipient.redacted is not None:
                recipient_dict['Redacted'] = email_recipient.redacted

            recipient_dicts.append(recipient_dict)

        return recipient_dicts

    @staticmethod
    def _refresh_child_dict(existing_item_dict: dict, new_item_dict: dict, item_map: ItemMap, status: Status):
        items = list(existing_item_dict.items())
        for _original_id, existing_item in items:
            if existing_item.id not in item_map or item_map[existing_item.id] not in new_item_dict:
                # The Content, AssetSelection, or DateRange failed to get pushed. Keep going.
                existing_item_dict.pop(existing_item.id, None)
                continue

            new_item_id = item_map[existing_item.id]
            new_item = new_item_dict[new_item_id]

            # Update the map
            del existing_item_dict[_original_id]
            existing_item_dict[new_item_id] = existing_item

            # Refresh item itself
            existing_item.refresh_from(new_item, item_map, status)

    def refresh_from(self, new_item, item_map: ItemMap, status: Status):
        super().refresh_from(new_item, item_map, status)

        Report._refresh_child_dict(self.date_ranges, new_item.date_ranges, item_map, status)
        Report._refresh_child_dict(self.asset_selections, new_item.asset_selections, item_map, status)
        Report._refresh_child_dict(self.content, new_item.content, item_map, status)

        self.schedule = new_item.schedule

    def _push_specific(self, session: Session, item_map: ItemMap, datasource_output, label,
                       new_annotation: AnnotationInputV1, existing_annotation: AnnotationOutputV1,
                       access_control, override_content_dict=None, status=None):
        date_range_ids_to_archive = list(existing_annotation.date_range_ids)
        asset_selection_ids_to_archive = list(existing_annotation.asset_selection_ids)
        content_ids_to_archive = list(existing_annotation.content_ids)

        existing_date_ranges = dict()
        for date_range_id in existing_annotation.date_range_ids:
            existing_date_range = DateRange.pull(date_range_id, session=session, status=status)
            existing_date_ranges[existing_date_range.id] = existing_date_range
            existing_date_ranges[existing_date_range.name] = existing_date_range

        existing_asset_selections = dict()
        for asset_selection_id in existing_annotation.asset_selection_ids:
            existing_asset_selection = AssetSelection.pull(asset_selection_id, session=session, status=status)
            existing_asset_selections[existing_asset_selection.id] = existing_asset_selection
            existing_asset_selections[existing_asset_selection.name] = existing_asset_selection

        existing_contents = dict()
        for content_id in existing_annotation.content_ids:
            existing_content = Content.pull(content_id, session=session, status=status)
            existing_contents[existing_content.id] = existing_content
            existing_contents[existing_content.name] = existing_content

        # Pushing date ranges, asset selections, and content has to be done in a couple of steps. First, we push the
        # date ranges and get their corresponding Seeq guids. Next, we push the asset selections and get their
        # corresponding Seeq guids. Then, we update the content to use those date range and asset selection guids so
        # that we can create content in the backend with the correct date ranges and asset selections. Finally,
        # we push up the report. The association to a report is made by identifying the reportId when creating or
        # updating the date ranges, asset selections, and/or content. The existing content, date ranges,
        # and asset selections here already have the correct reportIds attached.
        optional_report = OptionalReportInputV1(enabled=False, cron_schedule=None)

        def _push_it(_item_type, _item_dict, _existing, _ids_to_archive):
            for _item_object in _item_dict.values():  # type: Union[DateRange, AssetSelection, Content]
                try:
                    # This push has custom error handling so always use errors='raise'
                    _item_output = _item_object.push(session, item_map, _existing, status=Status(errors='raise'))
                    if _item_output.id in _ids_to_archive:
                        _ids_to_archive.remove(_item_output.id)
                except ApiException as e:
                    self.worksheet.workbook._push_context.status.on_error(
                        f'Error processing {_item_type}: {_item_object}\n{_common.format_exception(e)}')

        _push_it('Date Range', self.date_ranges, existing_date_ranges, date_range_ids_to_archive)
        _push_it('Asset Selection', self.asset_selections, existing_asset_selections, asset_selection_ids_to_archive)
        _push_it('Content', self.content if override_content_dict is None else override_content_dict,
                 existing_contents, content_ids_to_archive)

        if self.schedule is not None:
            optional_report.enabled = self.schedule.get('Enabled', self.get('Enabled', True))
            optional_report.background = self.schedule['Background']
            optional_report.cron_schedule = self.schedule['Cron Schedule']

        content_api = ContentApi(session.client)
        items_api = ItemsApi(session.client)
        for content_id_to_archive in content_ids_to_archive:
            @request_safely(action_description=f'archive Content {content_id_to_archive}', status=status)
            def _archive_content():
                # Something about this call is importantly different from the regular archive_item(). Using that
                #  method makes the Content creation act like POST instead of like PUT calls for subsequent runs.
                content_output = content_api.get_content(id=content_id_to_archive)  # type: ContentOutputV1
                content_api.update_content(id=content_id_to_archive,
                                           body=ContentInputV1(name='SPy archived this',
                                                               date_range_id=None,
                                                               asset_selection_id=None,
                                                               width=content_output.width,
                                                               height=content_output.height,
                                                               worksheet_id=content_output.source_worksheet,
                                                               workstep_id=content_output.source_workstep,
                                                               selector=content_output.selector,
                                                               summary_type=content_output.summary_type,
                                                               summary_value=content_output.summary_value,
                                                               report_id=None,
                                                               archived=True))

            _archive_content()

        self._push_notification_config(status, item_map, access_control, existing_annotation.id)

        for asset_selection_id_to_archive in asset_selection_ids_to_archive:
            safely(
                lambda: items_api.archive_item(id=asset_selection_id_to_archive),
                action_description=f'archive Asset Selection {asset_selection_id_to_archive}',
                status=status
            )

        for date_range_id_to_archive in date_range_ids_to_archive:
            safely(
                lambda: items_api.archive_item(id=date_range_id_to_archive),
                action_description=f'archive Date Range {date_range_id_to_archive}',
                status=status
            )

        new_annotation.report_input = optional_report

        return self.html

    def _push_notification_config(self, status: Status, item_map: ItemMap, access_control: str,
                                  existing_annotation_id: str):
        if self.schedule is None or self.schedule.get('Notification') is None:
            return

        _, strict = ItemWithOwnerAndAcl.parse_access_control_str(access_control)
        datasource_maps = self.worksheet.workbook.datasource_maps

        def _lookup_identity(_recipient):
            if _recipient.get('Identity') is None:
                return _recipient.get('Email')

            try:
                return Identity.find_identity(status.session, _recipient['Identity'], datasource_maps, item_map)
            except SPyDependencyNotFound:
                if strict:
                    raise
                else:
                    return None

        def _lookup_identities(_recipients):
            _identities = {_lookup_identity(r) for r in _recipients}
            return [_id for _id in _identities if _id is not None]

        # noinspection PyBroadException
        try:
            notification = self.schedule.get('Notification')
            notification_configurations_api = NotificationConfigurationsApi(status.session.client)
            body = ReportNotificationConfigurationInputV1(
                to_email_recipients=_lookup_identities(notification.get('To Email Recipients', [])),
                cc_email_recipients=_lookup_identities(notification.get('CC Email Recipients', [])),
                bcc_email_recipients=_lookup_identities(notification.get('BCC Email Recipients', [])),
                report_format=notification.get('Report Format')
            )
            safely(
                lambda: notification_configurations_api.set_notification_configuration_for_report(
                    body=body, id=existing_annotation_id
                ),
                action_description=f'set notification for Annotation Schedule {existing_annotation_id}',
                status=status
            )  # NotificationConfigurationOutputV1

        except NameError:
            # We're on an earlier version of Seeq that doesn't have the NotificationConfigurationsApi
            pass

    def save(self, workbook_folder, *, include_rendered_content=False, pretty_print_html=False):
        super().save(workbook_folder, include_rendered_content=include_rendered_content,
                     pretty_print_html=pretty_print_html)

        if include_rendered_content:
            _render.save(self, workbook_folder)

    def _update_save_json_dict(self, json_dict):
        json_dict.update({
            'Schedule': self.schedule,
            'Date Ranges': [d.definition_dict for d in self.date_ranges.values()],
            'Asset Selections': [a.definition_dict for a in self.asset_selections.values()],
            'Content': [c.definition_dict for c in self.content.values()]
        })

    @staticmethod
    def load(worksheet, workbook_folder):
        report = Report(worksheet)
        report._load(workbook_folder)
        return report

    def _load(self, workbook_folder: str) -> dict:
        json_dict = super()._load(workbook_folder)

        self.schedule = json_dict['Schedule']

        for date_range_dict in json_dict['Date Ranges']:
            new_date_range = DateRange(date_range_dict, self)
            self.date_ranges[new_date_range.id] = new_date_range

        if 'Asset Selections' in json_dict:
            for asset_selection_dict in json_dict['Asset Selections']:
                new_asset_selection = AssetSelection(asset_selection_dict, self)
                self.asset_selections[new_asset_selection.id] = new_asset_selection

        for content_dict in json_dict['Content']:
            new_content = Content(content_dict, self)
            self.content[new_content.id] = new_content

        return json_dict

    def pull_rendered_content(self, session: Session, status: Status):
        _render.pull(session, self, status)

    def get_embedded_content_html(self,
                                  display,  # type: AnalysisWorkstep
                                  date_range=None,  # type: Optional[DateRange]
                                  size='medium',  # type: str
                                  shape='rectangle',  # type: str
                                  width=None,  # type: Optional[int]
                                  height=None,  # type: Optional[int]
                                  scale=1.0,  # type: float
                                  selector='',  # type: str
                                  asset_selection=None,  # type: Optional[AssetSelection]
                                  summary_type=None,  # type: Optional[str]
                                  summary_value=None  # type: Optional[str]
                                  ):
        # type: (...) -> str
        if width is None and height is None:
            shape = Content.CONTENT_SHAPE[shape]
            width = Content.CONTENT_SIZE[size]
            height = int(width * shape['height'] / shape['width'])
        elif width is None and height is not None:
            raise SPyValueError('You must specify a width if you specify a height')
        elif height is None and width is not None:
            raise SPyValueError('You must specify a height if you specify a width')

        new_content_definition = {'Name': f'SPy_content_{_common.new_placeholder_guid()}',
                                  'Width': width,
                                  'Height': height,
                                  'Workbook ID': display.worksheet.workbook.id,
                                  'Worksheet ID': display.worksheet.id,
                                  'Workstep ID': display.id,
                                  'selector': selector,
                                  'Scale': float(scale)}

        new_content = Content(new_content_definition, self)
        self.content[new_content.id] = new_content

        if date_range is not None:
            existing_date_range = None
            # If a date range with an existing name is input, use that date range for the new content
            for dr in self.date_ranges.values():
                if dr.definition['Name'] == date_range.definition['Name']:
                    existing_date_range = dr

            if existing_date_range is not None:
                new_content.definition['Date Range ID'] = existing_date_range.id
            else:
                new_date_range = DateRange(date_range.definition, self)
                new_content = Content(new_content_definition, self)
                new_content.definition['Date Range ID'] = new_date_range.id
                self.date_ranges[new_date_range.id] = new_date_range

        if asset_selection is not None:
            existing_asset_selection = None
            # If an asset selection with an existing name is input, use that asset selection for the new content
            for selection in self.asset_selections.values():
                if selection.definition['Name'] == asset_selection.definition['Name']:
                    existing_asset_selection = selection

            if existing_asset_selection is not None:
                new_content.definition['Asset Selection ID'] = existing_asset_selection.id
            else:
                new_asset_selection = AssetSelection(asset_selection.definition, self)
                new_content = Content(new_content_definition, self)
                new_content.definition['Asset Selection ID'] = new_asset_selection.id
                self.asset_selections[new_asset_selection.id] = new_asset_selection

        if summary_type is not None:
            new_content.definition['Summary Type'] = summary_type
        if summary_value is not None:
            new_content.definition['Summary Value'] = summary_value

        return new_content.html

    def find_workstep_references(self, item_map: Optional[ItemMap] = None):
        if not self.html:
            return set()

        content_dict = self.content
        if item_map is not None and f'Content for {self.id}' in item_map:
            content_dict = item_map[f'Content for {self.id}']

        workstep_references = set()
        for content in content_dict.values():
            workstep_references.add((content.definition['Workbook ID'], content.definition['Worksheet ID'],
                                     content.definition['Workstep ID']))

        return workstep_references
