from __future__ import annotations

import copy
import json
import os
import re
import textwrap
from typing import List, Dict, Union, Optional, Tuple

import pandas as pd

from seeq.base import util
from seeq.sdk import *
from seeq.spy import _common, _metadata
from seeq.spy._errors import *
from seeq.spy._redaction import safely
from seeq.spy._session import Session
from seeq.spy._status import Status
from seeq.spy.workbooks._context import WorkbookPushContext
from seeq.spy.workbooks._item import Item, ItemMap, ItemExists


class StoredOrCalculatedItem(Item):
    def __init__(self, definition=None, *, provenance=None):
        super().__init__(definition, provenance=provenance)

    def _populate_asset_and_path(self, session: Session, item_id, status, item_search_preview: ItemSearchPreviewV1):
        if item_search_preview is None:
            trees_api = TreesApi(session.client)
            asset_tree_output = safely(
                lambda: trees_api.get_tree(id=item_id),
                action_description=f'get Tree information for {item_id}',
                status=status)  # type: AssetTreeOutputV1
            if asset_tree_output is None or asset_tree_output.item is None:
                return

            ancestors = [a.name for a in asset_tree_output.item.ancestors]
            if len(asset_tree_output.item.ancestors) > 0:
                self.definition['Parent ID'] = asset_tree_output.item.ancestors[-1].id
        else:
            ancestors = [a.name for a in item_search_preview.ancestors]
            if len(item_search_preview.ancestors) > 0:
                self.definition['Parent ID'] = item_search_preview.ancestors[-1].id

        _common.add_ancestors_to_definition(self.type, self.name, ancestors, self.definition, old_asset_format=False)

    def _pull(self, session: Session, item_id, status: Status, item_search_preview: ItemSearchPreviewV1):
        super()._pull(session, item_id, status, item_search_preview)

        if 'Path' not in self.definition:
            self._populate_asset_and_path(session, item_id, status, item_search_preview)

    def get_metadata_to_push(self, context: WorkbookPushContext, datasource_maps, datasource_output,
                             item_inventory: dict, *, pushed_workbook_id=None, item_map: ItemMap = None, label=None,
                             item_exists: ItemExists = ItemExists.MAYBE,
                             item_search_preview: ItemSearchPreviewV1 = None):
        raise SPyRuntimeError('get_metadata_to_push() called but StoredOrCalculatedItem.get_metadata_to_push() not '
                              'overloaded')

    def _attach_to_parent(self, session: Session, item_map: ItemMap, item_id: str):
        if self['Scoped To'] is None:
            # Do not move around global items
            return

        if 'Parent ID' not in self.definition:
            return

        parent_id = self.definition['Parent ID']
        if parent_id != Item.ROOT and self.definition['Parent ID'] not in item_map:
            return

        trees_api = TreesApi(session.client)
        if parent_id == Item.ROOT:
            trees_api.move_nodes_to_root_of_tree(body=ItemIdListInputV1(items=[item_id]))
        else:
            trees_api.move_nodes_to_parent(parent_id=item_map[self.definition['Parent ID']],
                                           body=ItemIdListInputV1(items=[item_id]))

    @staticmethod
    def _find_datasource_name(datasource_class, datasource_id, datasource_maps):
        for datasource_map in datasource_maps:
            if datasource_map['Datasource Class'] == datasource_class and \
                    datasource_map['Datasource ID'] == datasource_id:
                return datasource_map['Datasource Name']

        raise SPyRuntimeError('Could not find Datasource Class "%s" and Datasource ID "%s" in datasource maps' %
                              (datasource_class, datasource_id))

    def _execute_regex_map(self, old_definition, regex_map, regex_map_index, item_map: ItemMap, *,
                           allow_missing_properties=False):
        for prop in ['Old', 'New']:
            if prop not in regex_map:
                raise SPyValueError(f'RegEx-Based Map {regex_map_index} must have "{prop}" property')

        capture_groups = dict()
        for prop, regex in regex_map['Old'].items():
            if prop not in StoredItem.SEARCHABLE_PROPS:
                searchable_props_string = '\n'.join(StoredItem.SEARCHABLE_PROPS)
                raise SPyRuntimeError(
                    f'Datasource Map contains an unsearchable property "{prop}".'
                    f'Searchable properties:\n{searchable_props_string}')

            if prop not in old_definition:
                if allow_missing_properties:
                    continue
                else:
                    item_map.log(self.id, f'- RegEx-Based Map {regex_map_index}: Unsuccessful match. Details:')
                    item_map.log(self.id, f'    "{prop}" not defined for this item')
                    return None, None

            pythonized_regex = util.pythonize_regex_capture_group_names(regex)
            match = re.fullmatch(pythonized_regex, old_definition[prop])
            if not match:
                item_map.log(self.id, f'- RegEx-Based Map {regex_map_index}: Unsuccessful match. Details:')
                item_map.log(self.id, f'    "{prop}"')
                item_map.log(self.id, f'        regex          "{regex}"')
                item_map.log(self.id, f'        does not match "{old_definition[prop]}"')
                return None, None

            capture_groups.update(match.groupdict())

        new_definition = dict()
        for prop, regex in regex_map['New'].items():
            new_definition[prop] = util.replace_tokens_in_regex(regex, capture_groups, escape=False)

        return new_definition, capture_groups

    def _lookup_item_via_datasource_map(
            self, session: Session, pushed_workbook_id: str, datasource_maps: List[Dict],
            item_map: ItemMap, *, only_override_maps: bool = False, force_direct_lookup: bool = False,
            item_exists: ItemExists = ItemExists.MAYBE,
            item_search_preview: Optional[ItemSearchPreviewV1] = None) -> Optional[Item]:
        item: Optional[Item] = None

        item_map.clear_logs(self.id)

        # First, we process the "overrides". These are the cases where, even if the item with the ID exists in the
        # destination, we still want to map to something else. Useful for swapping datasources on the same server.
        overrides = [m for m in datasource_maps if _common.get(m, 'Override', default=False)]
        if len(overrides) > 0:
            if "File" in overrides[0]:
                item_map.log(self.id, f'Using overrides from {os.path.dirname(overrides[0]["File"])}:')
            item = self._lookup_in_datasource_map(session, pushed_workbook_id, overrides, item_map)
        else:
            item_map.log(self.id, f'No datasource map overrides found')

        if item is not None:
            item_map.log(self.id, f'Successful mapping:')
            item_map.log(self.id, f'  Old: {self}')
            item_map.log(self.id, f'  New: {item}')
            return item

        if only_override_maps:
            item_map.log(self.id, f'{self} not mapped, only override maps used', at_top=True)
            if not force_direct_lookup:
                return None

        # Second, we just try to look the item up by its ID. This case will occur when the user pulls a workbook,
        # makes a change, and pushes it back.
        if item_exists != ItemExists.NO:
            try:
                item = Item.pull(self.id, session=session, item_search_preview=item_search_preview)
            except ApiException:
                item_map.log(self.id, f"Item's ID {self.id} not found directly in target server")
        else:
            item_map.log(self.id, f"Item's ID {self.id} not found directly in target server")

        if item is not None:
            item_map.log(self.id, f"Item's ID {self.id} found directly in target server")
            return item

        if only_override_maps:
            return None

        # Finally, we try to use the non-override maps to find the item. This case will occur mostly when
        # transferring workbooks between servers.
        non_overrides = [m for m in datasource_maps if not _common.get(m, 'Override', default=False)]
        if len(non_overrides) > 0:
            if "File" in non_overrides[0]:
                item_map.log(self.id, f'Using non-overrides from {os.path.dirname(non_overrides[0]["File"])}:')
            item = self._lookup_in_datasource_map(session, pushed_workbook_id, non_overrides, item_map)
        else:
            item_map.log(self.id, f'No (non-override) datasource maps found')

        if item is None:
            item_map.log(self.id, f'{self} not successfully mapped', at_top=True)
            return None

        item_map.log(self.id, f'Successful mapping:')
        item_map.log(self.id, f'  Old: {self}')
        item_map.log(self.id, f'  New: {item}')

        return item

    def _lookup_in_datasource_map(self, session: Session, pushed_workbook_id, datasource_maps,
                                  item_map: ItemMap) -> Optional[Item]:
        item: Optional[Item] = None

        relevant_maps = list()
        for datasource_map in datasource_maps:
            if _common.get(self, 'Datasource Class') != _common.get(datasource_map, 'Datasource Class') or \
                    _common.get(self, 'Datasource ID') != _common.get(datasource_map, 'Datasource ID'):
                continue

            relevant_maps.append(datasource_map)
            if "File" in datasource_map:
                item_map.log(self.id, f'- Used "{datasource_map["File"]}"')
            else:
                item_map.log(self.id,
                             f'- Used datasource map for Datasource Class "{datasource_map["Datasource Class"]}" '
                             f'and Datasource ID "{datasource_map["Datasource ID"]}"')

            if _common.DATASOURCE_MAP_ITEM_LEVEL_MAP_FILES in datasource_map:
                item = self._lookup_in_item_level_map_files(session, datasource_map, item_map)

            if item is not None:
                break

            if _common.DATASOURCE_MAP_REGEX_BASED_MAPS in datasource_map:
                item = self._lookup_in_regex_based_maps(session, datasource_map, datasource_maps, item_map,
                                                        pushed_workbook_id)

            if item is not None:
                break

        if len(relevant_maps) == 0:
            item_map.log(
                self.id,
                f'- No Datasource Maps found that match Datasource Class "{_common.get(self, "Datasource Class")}" '
                f'and Datasource ID "{_common.get(self, "Datasource ID")}"')

        return item

    def _lookup_in_item_level_map_files(self, session: Session, datasource_map,
                                        item_map: ItemMap) -> Optional[Item]:
        item: Optional[ItemOutputV1] = None
        item_level_map_files = datasource_map['Item-Level Map Files']
        if not isinstance(item_level_map_files, list):
            item_level_map_files = [item_level_map_files]

        if 'DataFrames' not in datasource_map:
            datasource_map['DataFrames'] = dict()

        df_map = datasource_map['DataFrames']

        for item_level_map_file in item_level_map_files:
            if item_level_map_file not in df_map:
                if not util.safe_exists(item_level_map_file):
                    raise SPyValueError('Item-Level Map File "%s" does not exist' % item_level_map_file)

                df_map[item_level_map_file] = pd.read_csv(item_level_map_file)

            df = df_map[item_level_map_file]

            if 'Old ID' not in df.columns or 'New ID' not in df.columns:
                raise SPyValueError('Item-Level Map File must have "Old ID" and "New ID" columns')

            mapped_row = df[df['Old ID'] == self.id]

            if len(mapped_row) > 1:
                raise SPyValueError('Ambiguous map: %d rows in "%s" match "Old ID" == "%s"\n%s' % (
                    len(mapped_row), item_level_map_file, self.id, mapped_row))

            if len(mapped_row) == 0:
                item_map.log(
                    self.id,
                    f'- Item-Level Map File "{item_level_map_file}" did not match for "Old ID" value "{self.id}"')
                continue

            new_id = mapped_row.iloc[0]['New ID']

            item = Item.pull(new_id, session=session)
            break

        return item

    def _lookup_in_regex_based_maps(
            self, session: Session, datasource_map, datasource_maps, item_map: ItemMap,
            pushed_workbook_id) -> Optional[Union[ItemOutputV1, UserOutputV1, IdentityPreviewV1]]:

        for i in range(len(datasource_map['RegEx-Based Maps'])):
            regex_map_index = i
            regex_map = datasource_map['RegEx-Based Maps'][regex_map_index]
            item_object, old_criteria_matched = self._lookup_in_regex_based_map(
                session, regex_map_index, regex_map, datasource_maps, item_map, pushed_workbook_id)

            if item_object is not None:
                return item_object

            on_match_default = 'Stop' if session.options.wants_compatibility_with(191) else 'Continue'
            on_match = str(_common.get(regex_map, 'On Match', on_match_default)).lower()
            if not isinstance(on_match, str) and on_match not in ['continue', 'stop']:
                raise SPyValueError('RegEx-Based Map "On Match" parameter must be either "Continue" or "Stop"')

            if old_criteria_matched and on_match == 'stop':
                item_map.log(self.id,
                             f'- "On Match" parameter is "Stop", so no further RegEx-Based Maps will be used')
                break

        return None

    def _lookup_in_regex_based_map(
            self, session: Session, regex_map_index, regex_map, datasource_maps, item_map: ItemMap,
            pushed_workbook_id) -> Tuple[Optional[Union[ItemOutputV1, UserOutputV1, IdentityPreviewV1]], bool]:
        items_api = ItemsApi(session.client)
        old_definition = self.definition_dict
        new_definition = None
        item: Optional[Item] = None
        capture_groups = None

        if 'Type' not in old_definition:
            raise SPyValueError('"Type" property required in "Old" datasource map definitions')

        if 'Datasource Class' in old_definition and 'Datasource ID' in old_definition:
            old_definition['Datasource Name'] = \
                StoredOrCalculatedItem._find_datasource_name(old_definition['Datasource Class'],
                                                             old_definition['Datasource ID'],
                                                             datasource_maps)

        new_definition, capture_groups = self._execute_regex_map(
            old_definition, regex_map, regex_map_index, item_map,
            allow_missing_properties=self.type.endswith('Datasource'))

        if new_definition is None:
            return None, False

        if 'Type' not in new_definition:
            raise SPyValueError('"Type" property required in "New" datasource map definitions')

        if 'Datasource Name' in new_definition:
            if 'Datasource ID' not in new_definition:
                if 'Datasource Class' not in new_definition:
                    raise SPyValueError('"Datasource Class" required with "Datasource Name" in map:\n%s' %
                                        json.dumps(new_definition))

                datasource_results = items_api.search_items(
                    # Note here that 'Name' supports RegEx but 'Datasource Class' doesn't. Users have to be careful in
                    # the Datasource Maps not to supply a RegEx. See Workbook._construct_default_datasource_maps().
                    filters=['Datasource Class==%s&&Name==/%s/' % (new_definition['Datasource Class'],
                                                                   new_definition['Datasource Name']),
                             '@includeUnsearchable'],
                    types=['Datasource'],
                    limit=2)  # type: ItemSearchPreviewPaginatedListV1

                items = datasource_results.items
                if len(items) > 1:
                    # Try filtering out any archived items first
                    items = [i for i in items if not i.is_archived]

                if len(items) > 1:
                    multiple_items_str = "\n".join([i.id for i in items])
                    raise SPyRuntimeError(
                        f'Multiple datasources found that match "{new_definition["Datasource Name"]}":\n'
                        f'{multiple_items_str}'
                    )
                elif len(items) == 0:
                    item_map.log(self.id,
                                 f'- No datasource found that matches "{new_definition["Datasource Name"]}"')
                    return None, True

                new_datasource = items[0]  # type: ItemSearchPreviewV1
                try:
                    new_definition['Datasource ID'] = items_api.get_property(
                        id=new_datasource.id, property_name='Datasource ID').value
                except ApiException:
                    raise SPyRuntimeError(f'Could not get Datasource ID for '
                                          f'"{new_definition["Datasource Name"]}" {new_datasource.id}')

            del new_definition['Datasource Name']

        new_type = _common.get(new_definition, 'Type', '?')
        # Simplify the types so that the search will find stored or calculated signals/conditions etc. This
        # helps when mapping, for example, an AF tree to a Data Lab tree.
        for t in ['Signal', 'Condition', 'Scalar', 'Datasource']:
            if t in new_type:
                new_definition['Type'] = t

        if new_type not in ['User', 'UserGroup']:
            query_df = pd.DataFrame([new_definition])
            from seeq.spy import _search
            if 'Path' in new_definition:
                # If we're matching based on path, unfortunately we will not be able to match against archived items
                # due to a limitation of the API. See CRAB-18439.
                search_df = _search.search(query_df, workbook=pushed_workbook_id, recursive=False,
                                           ignore_unindexed_properties=False, session=session, quiet=True)
            else:
                search_df = _search.search(query_df, workbook=pushed_workbook_id, include_archived=True,
                                           ignore_unindexed_properties=False, session=session, quiet=True)

            # If several things are returned, but some of them are archived, then filter out the archived ones.
            # Tested by spy.workbooks.tests.test_push.test_datasource_map_by_name()
            if len(search_df) > 1 and 'Archived' in search_df.columns:
                search_df = search_df[~search_df['Archived']]

            def _log_criteria(_and_found=None):
                for key, value in new_definition.items():
                    item_map.log(self.id, f'    "{key}"')
                    if key in regex_map['Old']:
                        item_map.log(self.id, f'        regex          "{regex_map["Old"][key]}"')
                        item_map.log(self.id, f'        matched on     "{self[key]}"')
                    item_map.log(self.id, f'        searched for   "{value}"')
                    if _and_found is not None:
                        item_map.log(self.id, f'        and found      "{_and_found[key]}"')
                if capture_groups is not None and len(capture_groups) > 0:
                    item_map.log(self.id, f'    Capture groups:')
                    for n, v in capture_groups.items():
                        item_map.log(self.id, f'        {n:14} "{v}"')

            if len(search_df) == 0:
                item_map.log(self.id, f'- RegEx-Based Map {regex_map_index}: Item not found on server. Details:')
                _log_criteria()
            elif len(search_df) > 1:
                item_map.log(self.id,
                             f'- RegEx-Based Map {regex_map_index}: Multiple possibilities for item found. Details:')
                _log_criteria()
                item_map.log(self.id, f'  Matching items:')
                item_map.log(self.id, textwrap.indent(f'{str(search_df[["ID", "Name"]])}', '    '))
            else:
                item = Item.pull(search_df.iloc[0]['ID'], session=session)
                item_map.log(self.id, f'- RegEx-Based Map {regex_map_index}: Successfully mapped. Details:')
                _log_criteria(item)
        else:
            if new_type == 'User':
                try:
                    users_api = UsersApi(session.client)
                    user = users_api.get_user_from_username(
                        auth_datasource_class=new_definition['Datasource Class'],
                        auth_datasource_id=new_definition['Datasource ID'],
                        username=new_definition['Username'])
                    from seeq.spy.workbooks._user import User
                    item = User.pull(user.id, session=session)
                except ApiException:
                    # Fall through, item not found
                    pass
            else:
                users_api = UsersApi(session.client)
                offset = 0
                limit = 100

                while True:
                    identity_preview_list = users_api.autocomplete_users_and_groups(query=new_definition['Name'],
                                                                                    offset=offset,
                                                                                    limit=limit)

                    for identity in identity_preview_list.items:  # type: IdentityPreviewV1
                        if identity.type == 'UserGroup' and identity.name == new_definition['Name']:
                            from seeq.spy.workbooks._user import UserGroup
                            item = UserGroup.from_identity(identity, session=session)
                            break

                    if item is not None or len(identity_preview_list.items) < limit:
                        break

                    offset += limit

        return item, True


class PushThis(Exception):
    def __init__(self, definition):
        self.definition = definition


class StoredItem(StoredOrCalculatedItem):
    SEARCHABLE_PROPS = ['Datasource Class', 'Datasource ID', 'Datasource Name', 'Data ID',
                        'Type', 'Name', 'Description', 'Username', 'Path', 'Asset']

    def get_metadata_to_push(self, context: WorkbookPushContext, datasource_maps, datasource_output,
                             item_inventory: dict, *, pushed_workbook_id=None, item_map: ItemMap = None, label=None,
                             item_exists: ItemExists = ItemExists.MAYBE,
                             item_search_preview: ItemSearchPreviewV1 = None):
        try:
            self._lookup(context.session, datasource_maps, datasource_output,
                         pushed_workbook_id=pushed_workbook_id, item_map=item_map, label=label,
                         override_max_interp=context.override_max_interp,
                         dummy_items_workbook_context=context.dummy_items_workbook_context,
                         item_exists=item_exists, item_search_preview=item_search_preview, context=context,
                         item_inventory=item_inventory)
        except PushThis as e:
            return e.definition

        return None

    def _push_dummy_item(self, context: WorkbookPushContext, label: str, item_map: ItemMap,
                         datasource_output: DatasourceOutputV1, dummy_items_workbook_context):
        metadata_dict = self.definition_dict.copy()

        if context.global_inventory == 'always reuse' and not _common.present(self.definition, 'Scoped To'):
            label = None

        metadata_dict['Dummy Item'] = True

        metadata_dict['Original ID'] = metadata_dict['ID']
        metadata_dict['Original Datasource Class'] = metadata_dict['Datasource Class']
        metadata_dict['Original Datasource ID'] = metadata_dict['Datasource ID']
        metadata_dict['Original Data ID'] = metadata_dict['Data ID']

        del metadata_dict['ID']
        metadata_dict['Datasource Class'] = datasource_output.datasource_class
        metadata_dict['Datasource ID'] = datasource_output.datasource_id
        metadata_dict['Data ID'] = self._construct_data_id(
            label, for_item=self, reconcile_by=context.reconcile_inventory_by,
            workbook_id=dummy_items_workbook_context.workbook_object.id)

        if 'Parent ID' in metadata_dict:
            # Dummy items will be pushed with a Path and Asset so that it gets put in a (minimal) tree and therefore can
            # be asset-swapped.
            del metadata_dict['Parent ID']

        # Scope it to the dummy item workbook
        metadata_dict['Scoped To'] = dummy_items_workbook_context.workbook_object.id

        item_map.log(self.id, f'Dummy item pushed: {self}')

        raise PushThis(metadata_dict)

    def _push_local_item(self, context: WorkbookPushContext, label: str, item_map: ItemMap,
                         datasource_output: DatasourceOutputV1, pushed_workbook_id: str,
                         item_inventory: dict, item_exists: bool = False):
        metadata_dict = self.definition_dict.copy()

        metadata_dict['Dummy Item'] = (metadata_dict.get('Type') != 'Asset')

        if 'Parent ID' in metadata_dict and metadata_dict['Parent ID'] == Item.ROOT:
            del metadata_dict['Parent ID']

        if not item_exists:
            del metadata_dict['ID']
            metadata_dict['Datasource Class'] = datasource_output.datasource_class
            metadata_dict['Datasource ID'] = datasource_output.datasource_id
            metadata_dict['Data ID'] = self._construct_data_id(
                label, for_item=self, reconcile_by=context.reconcile_inventory_by,
                workbook_id=pushed_workbook_id)

            if 'Parent ID' in metadata_dict:
                metadata_dict['Parent Data ID'] = self._construct_data_id(
                    label, for_item=item_inventory[self.definition['Parent ID']],
                    reconcile_by=context.reconcile_inventory_by, workbook_id=pushed_workbook_id)
                del metadata_dict['Parent ID']

        # Scope it to the pushed workbook
        metadata_dict['Scoped To'] = pushed_workbook_id

        item_map.log(self.id, f'Local item pushed: {self}')

        raise PushThis(metadata_dict)

    def _lookup(self, session: Session, datasource_maps, datasource_output, *,
                pushed_workbook_id=None, item_map: ItemMap = None, label=None, override_max_interp=False,
                dummy_items_workbook_context=None, item_exists: ItemExists = ItemExists.MAYBE,
                item_search_preview: Optional[ItemSearchPreviewV1] = None,
                context: Optional[WorkbookPushContext] = None, item_inventory: Optional[dict] = None):
        # Note: The caller of this function is in charge of API safety

        if self.id in item_map.data_item_cache:
            return item_map.data_item_cache[self.id]

        local: bool = self['Scoped To'] is not None
        only_override_maps = item_map.only_override_maps
        if local:
            only_override_maps = True

        force_direct_lookup = label is None

        item = self._lookup_item_via_datasource_map(session, pushed_workbook_id, datasource_maps, item_map,
                                                    only_override_maps=only_override_maps,
                                                    force_direct_lookup=force_direct_lookup,
                                                    item_exists=item_exists,
                                                    item_search_preview=item_search_preview)

        if item is None:
            if local:
                self._push_local_item(context, label, item_map, datasource_output, pushed_workbook_id, item_inventory)
            else:
                if dummy_items_workbook_context is None:
                    raise SPyDependencyNotFound(item_map.explain(self.id))

                self._push_dummy_item(context, label, item_map, datasource_output, dummy_items_workbook_context)
        else:
            if local:
                item_scope = _common.get(item, 'Scoped To')
                item_is_scoped_to_other_workbook = item_scope is not None and item_scope != pushed_workbook_id
                self_already_exists_and_may_need_update = (
                        item_scope is not None
                        and item_scope == pushed_workbook_id
                        and _common.get(self, 'ID') == _common.get(item, 'ID')
                        and not label
                )
                if item_is_scoped_to_other_workbook or self_already_exists_and_may_need_update:
                    self._push_local_item(
                        context, label, item_map, datasource_output, pushed_workbook_id, item_inventory,
                        self_already_exists_and_may_need_update)

        item_map[self.id] = item.id
        item_map.data_item_cache[self.id] = item

        if item.type not in ['User', 'UserGroup']:
            # We need to exclude Example Data, because it is set explicitly by the connector
            datasource_class = item['Datasource Class']
            datasource_id = item['Datasource ID']
            is_example_data = (datasource_class and datasource_class == 'Time Series CSV Files' and
                               datasource_id and datasource_id == 'Example Data')

            if override_max_interp and item.type == 'StoredSignal' and not is_example_data and \
                    'Maximum Interpolation' in self:
                src_max_interp = Item._property_input_from_scalar_str(self['Maximum Interpolation'])
                dst_max_interp_prop = item['Maximum Interpolation']
                if (dst_max_interp_prop and hasattr(dst_max_interp_prop, 'value')
                        and hasattr(dst_max_interp_prop, 'unit_of_measure')):
                    dst_max_interp = Item._property_input_from_scalar_str(dst_max_interp_prop.value)
                    if src_max_interp.unit_of_measure != dst_max_interp.unit_of_measure or \
                            src_max_interp.value != dst_max_interp.value:
                        items_api = ItemsApi(session.client)
                        items_api.set_property(id=item.id,
                                               property_name='Override Maximum Interpolation',
                                               body=src_max_interp)

        return item


class Asset(StoredItem):
    def __init__(self, definition=None, *, provenance=None):
        super().__init__(definition, provenance=provenance)

        if self.provenance == Item.LOAD:
            if self.definition.get('Old Asset Format', True):
                if self.type == 'Asset':
                    if self.definition.get('Path') in [None, '']:
                        self.definition['Path'] = self.definition['Asset']
                    else:
                        self.definition['Path'] += ' >> ' + self.definition['Asset']
                        self.definition['Asset'] = self.definition['Name']

        self.definition['Old Asset Format'] = False


class Datasource(StoredItem):
    @staticmethod
    def from_datasource_output(datasource_output: Union[DatasourceOutputV1, DatasourcePreviewV1]):
        return Datasource({
            'ID': datasource_output.id,
            'Name': datasource_output.name,
            'Datasource Class': datasource_output.datasource_class,
            'Datasource ID': datasource_output.datasource_id,
            'Archived': datasource_output.is_archived
        })


class CalculatedItem(StoredOrCalculatedItem):
    def _lookup(self, session: Session, datasource_maps, pushed_workbook_id, item_map: ItemMap,
                label: str, item_exists: ItemExists, item_search_preview: Optional[ItemSearchPreviewV1]):
        if self.id in item_map.data_item_cache:
            return item_map.data_item_cache[self.id]

        # This is a key piece of datasource mapping logic. CalculatedItems that are in trees are more like "source
        # data" in the context of workbooks/worksheets. In other words, when we push them, we want to map in existing
        # items rather than creating a standalone CalculatedItem that isn't a part of a tree, has no associated asset
        # and cannot be swapped.
        is_standalone_item = _common.get(self, 'Asset') is None

        # So if it's in a global tree, we process the datasource maps just like we would if it were a StoredItem. If
        # it's *not* in a global tree, then we only process datasource maps that have been supplied in a
        # datasource_map_folder and are therefore treated as "overrides". CalculatedItems that are associated with the
        # particular analysis of a workbook should be created/overwritten as part of the push, and since they won't
        # exist in a tree, we purposefully don't attempt to do any mapping and we fall through to the CalculatedItem's
        # create/update code.
        local: bool = self['Scoped To'] is not None
        only_override_maps = item_map.only_override_maps
        if local or is_standalone_item:
            only_override_maps = True

        item = self._lookup_item_via_datasource_map(
            session, pushed_workbook_id, datasource_maps, item_map, only_override_maps=only_override_maps,
            item_exists=item_exists, item_search_preview=item_search_preview)

        if not local and not is_standalone_item and item is None:
            raise SPyDependencyNotFound(item_map.explain(self.id))

        if item:
            item_map[self.id] = item.id
            item_map.data_item_cache[self.id] = item

        return item

    def _find_by_name(self, session: Session, workbook_id):
        return Item.find_item_by_search(session, f'Name=={self.name}', scope=[workbook_id], types=[self.type])

    def get_metadata_to_push(self, context: WorkbookPushContext, datasource_maps, datasource_output,
                             item_inventory: dict, *, pushed_workbook_id=None, item_map: ItemMap = None, label=None,
                             item_exists: ItemExists = ItemExists.MAYBE,
                             item_search_preview: ItemSearchPreviewV1 = None):
        item = self._lookup(context.session, datasource_maps, pushed_workbook_id, item_map, label, item_exists,
                            item_search_preview)

        if item:
            return None

        item_output = item_search_preview
        if self.provenance == Item.CONSTRUCTOR:
            if item_exists == ItemExists.MAYBE:
                item_output = self.find_me(context.session, label, datasource_output)

            if item_output is None:
                item_output = self._find_by_name(context.session, pushed_workbook_id)

        if context.global_inventory == 'always reuse' and not _common.present(self.definition, 'Scoped To'):
            label = None

        if label is None and item_exists != ItemExists.NO and item_search_preview is None:
            item_output = self.find_me(context.session, label, datasource_output)

        metadata_dict = copy.deepcopy(self.definition_dict)

        if 'Formula' in self.definition:
            metadata_dict['Formula'] = Item.formula_string_from_list(self.definition['Formula'])

        if context.global_inventory == 'copy local' or _common.present(self.definition, 'Scoped To'):
            metadata_dict['Scoped To'] = pushed_workbook_id

        if 'Scoped To' not in metadata_dict:
            pushed_workbook_id = None

        # Custody ID preserves the original ID of the item which helps maintain a consistent Data ID for the same
        # item through multiple round-trips.
        metadata_dict['Custody ID'] = self.get('Custody ID', self.id)

        if (label is None or self.provenance == Item.CONSTRUCTOR) and item_output is not None:
            def _prop(_n):
                if isinstance(item_output, ItemSearchPreviewV1):
                    return item_output.included_properties[_n].value
                elif isinstance(item_output, ItemOutputV1):
                    return [_p.value for _p in item_output.properties if _p.name == _n][0]

            metadata_dict['Datasource Class'] = _prop('Datasource Class')
            metadata_dict['Datasource ID'] = _prop('Datasource ID')
            metadata_dict['Data ID'] = _prop('Data ID')

            if (metadata_dict['Datasource Class'] == datasource_output.datasource_class and
                    metadata_dict['Datasource ID'] == datasource_output.datasource_id):
                # This allows the batch endpoints to be used
                del metadata_dict['ID']

                if 'Parent ID' in metadata_dict:
                    metadata_dict['Parent Data ID'] = self._construct_data_id(
                        label, for_item=item_inventory[self.definition['Parent ID']],
                        reconcile_by=context.reconcile_inventory_by, workbook_id=pushed_workbook_id)
        else:
            del metadata_dict['ID']
            metadata_dict['Datasource Class'] = datasource_output.datasource_class
            metadata_dict['Datasource ID'] = datasource_output.datasource_id
            metadata_dict['Data ID'] = self._construct_data_id(
                label, for_item=self, reconcile_by=context.reconcile_inventory_by, workbook_id=pushed_workbook_id)

            if 'Parent ID' in metadata_dict:
                metadata_dict['Parent Data ID'] = self._construct_data_id(
                    label, for_item=item_inventory[self.definition['Parent ID']],
                    reconcile_by=context.reconcile_inventory_by, workbook_id=pushed_workbook_id)

        return metadata_dict

    def _set_ui_config(self, session: Session, _id):
        if 'UIConfig' in self.definition:
            try:
                items_api = ItemsApi(session.client)
                items_api.set_property(id=_id, property_name='UIConfig',
                                       body=PropertyInputV1(value=json.dumps(self.definition['UIConfig'])))
            except ValueError:  # ValueError includes JSONDecodeError, but ApiException will not be caught
                pass


class StoredSignal(StoredItem):
    pass


class CalculatedSignal(CalculatedItem):
    def _pull(self, session: Session, item_id, status, item_search_preview: ItemSearchPreviewV1):
        super()._pull(session, item_id, status, item_search_preview)

        if item_search_preview is None:
            # Note: The caller of this function is in charge of API safety
            signals_api = SignalsApi(session.client)
            signal_output = signals_api.get_signal(id=item_id)  # type: SignalOutputV1
            self._set_formula_based_item_properties(signal_output.parameters)
        else:
            self._set_formula_based_item_properties(item_search_preview.parameters)


class StoredCondition(StoredItem):
    pass


class CalculatedCondition(CalculatedItem):
    def _pull(self, session: Session, item_id, status, item_search_preview: ItemSearchPreviewV1):
        super()._pull(session, item_id, status, item_search_preview)

        if item_search_preview is None:
            # Note: The caller of this function is in charge of API safety
            conditions_api = ConditionsApi(session.client)
            condition_output = conditions_api.get_condition(id=item_id)  # type: ConditionOutputV1
            self._set_formula_based_item_properties(condition_output.parameters)
        else:
            self._set_formula_based_item_properties(item_search_preview.parameters)


class LiteralScalar(StoredItem):
    pass


class CalculatedScalar(CalculatedItem):
    def _pull(self, session: Session, item_id, status: Status, item_search_preview: ItemSearchPreviewV1):
        super()._pull(session, item_id, status, item_search_preview)
        if item_search_preview is None:
            # Note: The caller of this function is in charge of API safety
            scalars_api = ScalarsApi(session.client)
            calculated_item_output = scalars_api.get_scalar(id=item_id)  # type: CalculatedItemOutputV1
            self._set_formula_based_item_properties(calculated_item_output.parameters)
        else:
            self._set_formula_based_item_properties(item_search_preview.parameters)


class Chart(CalculatedItem):
    def _pull(self, session: Session, item_id, status: Status, item_search_preview: ItemSearchPreviewV1):
        super()._pull(session, item_id, status, item_search_preview)

        if item_search_preview is None:
            # Note: The caller of this function is in charge of API safety
            formulas_api = FormulasApi(session.client)
            calculated_item_output = formulas_api.get_function(id=item_id)  # type: CalculatedItemOutputV1

            self._set_formula_based_item_properties(calculated_item_output.parameters)
        else:
            self._set_formula_based_item_properties(item_search_preview.parameters)

        if 'FormulaParameters' in self.definition:
            # Charts have these superfluous properties
            del self.definition['FormulaParameters']


class ThresholdMetric(CalculatedItem):
    def _pull(self, session: Session, item_id, status: Status, item_search_preview: ItemSearchPreviewV1):
        super()._pull(session, item_id, status, item_search_preview)
        # Note: The caller of this function is in charge of API safety
        formula_parameters = _metadata.formula_parameters_dict_from_threshold_metric(session, item_id)

        # These properties come through in the GET /items/{id} call, and for clarity's sake we remove them
        for ugly_duplicate_property in ['AggregationFunction', 'BoundingConditionMaximumDuration']:
            if ugly_duplicate_property in self.definition:
                del self.definition[ugly_duplicate_property]

        self.definition['Formula'] = '<ThresholdMetric>'
        self.definition['Formula Parameters'] = formula_parameters
