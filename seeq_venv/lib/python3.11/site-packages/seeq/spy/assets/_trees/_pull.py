from __future__ import annotations

import re
import types

import numpy as np
import pandas as pd

from seeq.sdk import *
from seeq.spy import _common, _search
from seeq.spy._errors import *
from seeq.spy._redaction import request_safely
from seeq.spy._session import Session
from seeq.spy._status import Status
from seeq.spy.assets._trees import _constants, _path, _properties, _utils


def pull_tree(session: Session, node_id, workbook_id, display_template_map, datasource, status: Status):
    """
    Given the ID of an Item, pulls that node and all children and returns the resulting sanitized dataframe
    """
    # Determine if node_id is root of pre-existing SPy tree
    existing_tree_df = get_existing_spy_tree(session, pd.DataFrame([{'ID': node_id}]), workbook_id, datasource)
    if existing_tree_df is not None:
        display_ids_to_archive = list(existing_tree_df.loc[(existing_tree_df.Type == 'Display') & (
                existing_tree_df['Datasource Name'] != datasource), 'ID'])
        if hasattr(existing_tree_df.spy, 'workbook_id'):
            workbook_id = existing_tree_df.spy.workbook_id
    else:
        display_ids_to_archive = list()

    # Get the requested node itself
    df = _properties.process_properties(session,
                                        pd.DataFrame([{'ID': node_id}], columns=_constants.dataframe_columns),
                                        display_template_map, datasource, status, existing_tree_df=existing_tree_df)

    df = pull_all_children_of_all_nodes(session, df, workbook_id, display_template_map, datasource, status,
                                        existing_tree_df=existing_tree_df)
    _common.put_properties_on_df(df, types.SimpleNamespace(spy_tree=existing_tree_df is not None,
                                                           display_ids_to_archive=display_ids_to_archive,
                                                           workbook_id=workbook_id))

    return df


def pull_all_children_of_all_nodes(session: Session, df, workbook_id, display_template_map, datasource, status: Status,
                                   *, existing_tree_df=None, item_ids_to_ignore=None):
    """
    For each node in the tree that contains an 'ID' or 'Referenced ID', pull in any asset tree children.
    If any nodes already exist in the dataframe (by case-insensitive Path+Name), the existing row will be kept.
    """
    if df.empty:
        return df

    for col in ['ID', 'Referenced ID']:
        if col not in df.columns:
            df[col] = pd.Series(np.nan, dtype=object)
    # Gather all Paths+IDs into a list upfront
    items_to_pull_children = df[(~pd.isnull(df['ID'])) | (~pd.isnull(df['Referenced ID']))]

    for _, row in items_to_pull_children.iterrows():
        if _common.get(row, 'Type') != 'Asset':
            continue

        # Pull based on ID if it exists, otherwise use Referenced ID
        if _common.present(row, 'ID'):
            node_id = row['ID']
            row_is_reference = False
        else:
            node_id = row['Referenced ID']
            row_is_reference = True
        node_full_path = _path.get_full_path(row)
        parent_value = _common.get(row, 'Parent')
        df = pull_all_children_of_node(session, df, node_id, node_full_path, workbook_id, display_template_map,
                                       datasource=datasource,
                                       existing_tree_df=existing_tree_df if not row_is_reference else None,
                                       item_ids_to_ignore=item_ids_to_ignore,
                                       parent_value=parent_value,
                                       status=status)
    return df


def pull_all_children_of_node(session: Session, df, node_id, node_full_path, workbook_id, display_template_map,
                              datasource, existing_tree_df, item_ids_to_ignore, parent_value, status: Status):
    """
    Given the ID of an Item in an asset tree, pulls all children and places them into the given dataframe.
    Does not overwrite existing data.
    """

    # Get all children of the requested asset
    @request_safely(
        action_description='pull children of node',
        status=status)
    def get_search_results():
        return _search.search(query={'Asset': node_id}, all_properties=True,
                              workbook=_get_search_workbook(session, node_id, workbook_id),
                              old_asset_format=True, order_by=['ID'], limit=None,
                              quiet=True, session=session)

    search_results = get_search_results()
    if search_results is None or len(search_results) == 0:
        return df

    if item_ids_to_ignore is not None:
        search_results = search_results[~search_results['ID'].isin(item_ids_to_ignore)]
        if len(search_results) == 0:
            return df

    _utils.increment_status_df(status, pulled_items=search_results)

    # Step 1: Convert the search results dataframe into a Tree-style dataframe.
    insert_df = _properties.process_properties(session, search_results, display_template_map, datasource,
                                               status, existing_tree_df=existing_tree_df, pull_nodes=False)
    # If we are pulling additional children to be inserted into the Tree, and a 'Parent' column is specified,
    #  we must propagate it to pulled children
    if parent_value is not None:
        insert_df['Parent'] = parent_value

    # Step 2: If the node_id's original name does not match what the node's name is in the Tree, trim off any extra
    # path from the input.
    _path.decorate_with_full_path(insert_df)
    parent_name = _common.path_string_to_list(node_full_path)[-1]
    if parent_name:
        maintain_last_shared_root = parent_name in insert_df.iloc[0]['Full Path List']
        insert_df = _path.trim_unneeded_paths(insert_df, node_full_path, maintain_last_shared_root)

    # Step 3: Actually insert the nodes
    df = _utils.upsert(df, insert_df, prefer_right=False)
    return df


def _get_search_workbook(session: Session, asset_id, tree_workbook_id):
    items_api = ItemsApi(session.client)
    try:
        existing_scope = items_api.get_property(id=asset_id, property_name='Scoped To').value
        if (tree_workbook_id and existing_scope and tree_workbook_id != _constants.UNKNOWN
                and tree_workbook_id != _common.EMPTY_GUID and tree_workbook_id.lower() != existing_scope.lower()):
            raise SPyValueError(f"Root asset with ID {asset_id} is scoped to workbook {existing_scope}, "
                                f"but requested workbook's ID is {tree_workbook_id}. If you want to push a tree to a "
                                f"different workbook, use `tree.workbook = 'My Workbook Name'` after it's pulled.")
        return existing_scope
    except ApiException:
        if tree_workbook_id is None or tree_workbook_id == _constants.UNKNOWN:
            return _common.EMPTY_GUID
        return tree_workbook_id


def find_root_nodes(session: Session, workbook_id, matcher):
    trees_api = TreesApi(session.client)
    matching_root_nodes = list()

    offset = 0
    limit = session.options.search_page_size
    kwargs = dict()
    # Can't use get_tree_root_nodes()'s `properties` filter for scoped_to because the endpoint is case-sensitive and
    # we want both global and scoped nodes. If workbook_id is the empty guid (tree's workbook hasn't been created yet),
    # we will get only global nodes.
    if workbook_id == _constants.UNKNOWN or workbook_id is None:
        kwargs['scope'] = [_common.EMPTY_GUID]
    else:
        kwargs['scope'] = [workbook_id]

    keep_going = True
    while keep_going:
        kwargs['offset'] = offset
        kwargs['limit'] = limit
        root_nodes = trees_api.get_tree_root_nodes(**kwargs)  # type: AssetTreeOutputV1
        for root_node in root_nodes.children:  # type: TreeItemOutputV1
            if matcher(root_node):
                # A root node matching the name was already found. Choose a best_root_node based on this priority:
                # Workbook-scoped SPy assets > workbook-scoped assets > global SPy assets > global assets
                workbook_scoped_score = 2 if root_node.scoped_to is not None else 0
                spy_created_score = 1 if item_output_has_sdl_datasource(root_node) else 0
                setattr(root_node, 'score', workbook_scoped_score + spy_created_score)
                matching_root_nodes.append(root_node)
        keep_going = root_nodes.next is not None
        offset = offset + limit
    return matching_root_nodes


def find_root_node_by_name(session: Session, name, workbook_id, status: Status):
    """
    Finds the Seeq ID of a case-insensitive name match of existing root nodes.
    """
    if not session.client:
        # User is not logged in or this is a unit test. We must create a new tree.
        return None
    status.update('Finding best root.', Status.RUNNING)

    name_pattern = re.compile('(?i)^' + re.escape(name) + '$')
    matching_root_nodes = find_root_nodes(session, workbook_id, lambda root_node: name_pattern.match(root_node.name))
    if len(matching_root_nodes) == 0:
        status.update(f"No existing root items were found matching '{name}'.", Status.RUNNING)
        return None
    best_score = max([getattr(n, 'score') for n in matching_root_nodes])
    best_root_nodes = list(filter(lambda n: getattr(n, 'score') == best_score, matching_root_nodes))
    if len(best_root_nodes) > 1:
        e = SPyValueError(
            f"More than one existing tree was found with name '{name}'. Please use an ID to prevent ambiguities.")
        status.exception(e, throw=True)
    best_id = best_root_nodes[0].id
    if len(matching_root_nodes) > 1:
        status.update(f"{len(matching_root_nodes)} root items were found matching '{name}'. Selecting {best_id}.",
                      Status.RUNNING)
    return best_id


def item_output_has_sdl_datasource(item_output, datasource=None):
    checked_datasource_properties = 0
    for prop in item_output.properties:  # type: PropertyOutputV1
        if prop.name == 'Datasource Class':
            if prop.value == _common.DEFAULT_DATASOURCE_CLASS:
                checked_datasource_properties += 1
            else:
                return False
        elif datasource and prop.name == 'Datasource ID':
            if prop.value == datasource:
                checked_datasource_properties += 1
            else:
                return False
        if checked_datasource_properties == (2 if datasource else 1):
            return True
    return False


def get_existing_spy_tree(session: Session, df, workbook_id, datasource):
    if 'ID' not in df.columns or not session.client:
        return None

    df = df[df['ID'].notnull()]
    if 'Path' in df.columns:
        df = df[(df['Path'] == '') | (df['Path'].isnull())]

    def _spy_tree_root_filter(root):
        if workbook_id is None or workbook_id == _common.EMPTY_GUID:
            return (root.scoped_to is None
                    and item_output_has_sdl_datasource(root, datasource))
        else:
            return (root.scoped_to is not None
                    and root.scoped_to.upper() == workbook_id
                    and item_output_has_sdl_datasource(root, datasource))

    existing_spy_trees = find_root_nodes(session, workbook_id, _spy_tree_root_filter)

    def _row_is_spy_tree_root(_row, root_id, root_name):
        return (_common.present(_row, 'ID') and _row['ID'].casefold() == root_id.casefold()) \
            and (not _common.present(_row, 'Name') or _row['Name'].casefold() == root_name.casefold()) \
            and (not _common.get(_row, 'Path'))

    df_root_id = None
    for spy_tree in existing_spy_trees:
        for _, row in df.iterrows():
            if _row_is_spy_tree_root(row, spy_tree.id, spy_tree.name):
                if df_root_id is None:
                    df_root_id = row['ID']
                else:
                    return None
    if df_root_id is None and len(df) == 1 and workbook_id == _constants.UNKNOWN and _common.is_guid(df.iloc[0]['ID']):
        # A specific tree was specified as the input, but without the workbook. Imply the workbook.
        df_root_id = df.iloc[0]['ID']
        workbook_id = _get_search_workbook(session, df_root_id, workbook_id)
    if df_root_id is not None:
        existing_tree_df = _search.search([{'ID': df_root_id}, {'Asset': df_root_id}], workbook=workbook_id,
                                          old_asset_format=True, order_by=['ID'], limit=None,
                                          quiet=True, session=session)
        existing_tree_df['Path'] = existing_tree_df.apply(_path.determine_path, axis=1)
        if 'Datasource Name' not in existing_tree_df.columns:
            existing_tree_df['Datasource Name'] = np.nan
        existing_tree_df = existing_tree_df[['ID', 'Path', 'Name', 'Type', 'Datasource Name']]
        if workbook_id != _constants.UNKNOWN and workbook_id != _common.EMPTY_GUID:
            _common.put_properties_on_df(existing_tree_df, types.SimpleNamespace(workbook_id=workbook_id))
        return existing_tree_df
    else:
        return None


def get_active_conditions(session: Session, condition_ids, pd_start, pd_end, status: Status):
    formulas_api = FormulasApi(session.client)
    active_condition_ids = set()
    start_string = '%s ns' % pd_start.value
    end_string = '%s ns' % pd_end.value

    status.update('Evaluating conditions', Status.RUNNING)
    status.df = status.df.reindex(columns=['Result'])

    def _is_active(_condition_id, start, end) -> bool:
        formula_run_output = formulas_api.run_formula(
            formula='$condition',
            parameters=[f'condition={_condition_id}'],
            start=start,
            end=end,
            limit=1)
        return len(formula_run_output.capsules.capsules) > 0

    def _on_success(_condition_id, _job_result):
        if _job_result:
            active_condition_ids.add(_condition_id)

    for condition_id in condition_ids:
        job = (_is_active, condition_id, start_string, end_string)
        status.add_job(condition_id, job, _on_success)

    status.execute_jobs(session)
    error_rows = status.df.loc[status.df['Result'] != 'Success']

    status.update('Evaluated conditions successfully', Status.SUCCESS)

    return active_condition_ids, error_rows
