from __future__ import annotations

import copy
import re
import types
from typing import Optional

import numpy as np
import pandas as pd

from seeq.sdk import *
from seeq.spy import _common, _login, _metadata, _redaction
from seeq.spy._errors import *
from seeq.spy._redaction import safely
from seeq.spy._session import Session
from seeq.spy._status import Status
from seeq.spy.assets._model import ItemGroup
from seeq.spy.assets._trees import _constants, _match, _path, _utils
from seeq.spy.assets._trees._pandas import KeyedDataFrame


def apply_friendly_name(df) -> None:
    if 'Friendly Name' not in df.columns or df['Friendly Name'].isnull().all():
        _common.put_properties_on_df(df, types.SimpleNamespace(modified_items=set()))
        return

    # If we are changing the names of items in a dataframe whose paths are dependent on one another, then
    # record those dependencies so we can modify paths afterwards as well
    relationships = path_relationships(df)

    modified_items = set()
    for i in df.index:
        if pd.isnull(df.loc[i, 'Friendly Name']):
            continue
        if _match.is_column_value_query(df.loc[i, 'Friendly Name']):
            new_name = _match.fill_column_values(df.loc[i], df.loc[i, 'Friendly Name'])
        else:
            new_name = df.loc[i, 'Friendly Name']
        if pd.isnull(new_name):
            continue
        df.loc[i, 'Name'] = new_name
        if _common.present(df.loc[i], 'ID'):
            modified_items.add(df.loc[i, 'ID'])

    recover_relationships(df, relationships)
    _common.put_properties_on_df(df, types.SimpleNamespace(modified_items=modified_items))


class _PathRelationshipNode:
    """
    Helper class for path_relationships()
    """

    def __init__(self, name, index=None):
        self.name = name
        self.index = index
        self.children = dict()

    @staticmethod
    def tree_root() -> _PathRelationshipNode:
        return _PathRelationshipNode(None)

    def add_branch(self, path_list, index, offset=0):
        if offset >= len(path_list):
            return
        include_index = offset == len(path_list) - 1
        name = path_list[offset].casefold()
        if name not in self.children:
            self.children[name] = _PathRelationshipNode(name)
        self.children[name].add_branch(path_list, index, offset + 1)
        if include_index:
            self.children[name].index = index

    def calculate_relationships(self, relationship_dict=None, branch_references=None, offset=0) -> dict:
        if relationship_dict is None:
            relationship_dict = dict()
        if branch_references is None:
            branch_references = dict()
        for node in self.children.values():
            if node.index is not None:
                if len(branch_references) > 0:
                    relationship_dict[node.index] = branch_references.copy()
                branch_references[offset] = node.index
            node.calculate_relationships(relationship_dict, branch_references, offset + 1)
            branch_references.pop(offset, None)
        return relationship_dict


def path_relationships(df) -> Optional[dict]:
    """
    Return a dict of dicts indicating via integers how the paths of the input rows are dependent on one another.

    Example:
        df = pd.DataFrame([{
            'Path': 'Root', 'Name': 'Item 1'
        }, {
            'Path': 'Root >> Item 1', 'Name': 'Item 2'
        }])

        output = {
            1: { # 1 refers here to the row in df with index 1, i.e., Item 2
                1: 0 # 1 refers here to the item in Item 2's path with index 1, i.e. 'Item 1'
                     # 0 refers here to the index of Item 1's row in df
            }
        }
    """
    if len(df) == 0 or 'Name' not in df.columns or 'Path' not in df.columns:
        return None
    full_paths = list(df.apply(_path.get_full_path, axis=1, check_asset_column=True).apply(_common.path_string_to_list))
    path_relationship_tree = _PathRelationshipNode.tree_root()
    for i, full_path in enumerate(full_paths):
        path_relationship_tree.add_branch(full_path, i)
    return path_relationship_tree.calculate_relationships()


def recover_relationships(df, relationships) -> None:
    """
    Takes a list of relationships (in the format described in _path_relationships) and modifies paths in
    df to reflect those relationships
    """
    if relationships is None:
        return
    for i, path_ref_dict in relationships.items():
        path = _path.determine_path(df.loc[i])
        path_list = _common.path_string_to_list(path) if path else []
        for j, reference in path_ref_dict.items():
            if 0 <= reference < len(df) and 0 <= j < len(path_list):
                path_list[j] = df.loc[reference, 'Name']
        df.loc[i, 'Path'] = _common.path_list_to_string(path_list)
    if 'Asset' in df.columns:
        df.drop(columns='Asset', inplace=True)


def process_properties(session: Session, df, display_template_map, datasource, status: Status, *, existing_tree_df=None,
                       pull_nodes=True, keep_parent_column=False) -> KeyedDataFrame:
    """
    Sanitize and pull item properties into an input dataframe. Steps in order:
    -- Pulls missing properties for items with ID provided
    -- Filters out properties not in _constants.dataframe_columns
    -- Determines tree depth
    -- Determines (if possible_tree_copy is True) if the input dataframe corresponds to an existing SPy tree
        -- If it is indeed a copy of a SPy tree, pulls in calculations from the original tree
        -- Otherwise, it converts all items with IDs into references
    -- Ensures all formula parameters are NAN or dict
    """
    df = df.reset_index(drop=True)
    df = KeyedDataFrame.of(df)

    _safely = _redaction.request_safely(
        action_description='get properties for requested item',
        status=status,
        default_value=pd.Series(np.nan, index=_constants.dataframe_columns, dtype=object))

    df = df.apply(_safely(process_row_properties), axis=1,
                  session=session,
                  status=status,
                  pull_nodes=pull_nodes,
                  keep_parent_column=keep_parent_column)

    datasource = datasource if datasource is not None else _common.DEFAULT_DATASOURCE_ID

    def _row_is_from_existing_tree(row):
        if existing_tree_df is None or not _common.present(row, 'ID'):
            return 'new'
        same_id_rows = existing_tree_df[existing_tree_df.ID.str.casefold() == row['ID'].casefold()]
        if len(same_id_rows) != 1:
            return 'new'
        if _common.present(row, 'Type') and row['Type'].casefold() != same_id_rows.Type.iloc[0].casefold():
            return 'new'
        if _common.present(row, 'Datasource Class') and row['Datasource Class'] != _common.DEFAULT_DATASOURCE_CLASS:
            return 'new'
        if _common.present(row, 'Datasource ID') and row['Datasource ID'] != datasource:
            return 'new'
        if _common.present(row, 'Name') and row['Name'].casefold() != same_id_rows.Name.iloc[0].casefold():
            return 'modified'
        if _common.present(row, 'Path') and row['Path'].casefold() != same_id_rows.Path.iloc[0].casefold():
            return 'modified'
        return 'pre-existing'

    row_type = df.apply(_row_is_from_existing_tree, axis=1)
    df.drop(columns=['Datasource Class', 'Datasource ID'], inplace=True, errors='ignore')
    modified_items = df.loc[row_type == 'modified', 'ID'] if 'ID' in df.columns else set()

    # For the nodes that originated from the pre-existing SPy tree we are modifying, we want to pull
    # pre-existing calculations directly.
    df.loc[row_type == 'pre-existing', :] = df.loc[row_type == 'pre-existing', :] \
        .apply(_safely(pull_calculation), axis=1, session=session)

    # For the nodes that originate from places other than the pre-existing SPy tree we are modifying,
    # we want to build references so we create and modify *copies* and not the original items.
    df.loc[row_type != 'pre-existing', :] = df.loc[row_type != 'pre-existing', :] \
        .apply(_safely(make_node_reference), axis=1, display_template_map=display_template_map,
               session=session)

    if 'Formula Parameters' in df.columns:
        df['Formula Parameters'] = df['Formula Parameters'].apply(formula_parameters_to_dict)

    # Drop all-NaN rows created by safely() calls
    # TODO CRAB-41833: KeyedDataFrame.dropna doesn't work as expected.
    df = KeyedDataFrame.of(pd.DataFrame(df).dropna(how='all'))
    if 'Depth' in df:
        df['Depth'] = df['Depth'].astype('int32')

    _common.put_properties_on_df(df, types.SimpleNamespace(modified_items=modified_items))

    return df


# Note that the session argument is second in this special case because we call this from pd.DataFrame.apply(),
# which requires that row is the first argument.
def process_row_properties(row, session: Session, status, pull_nodes, keep_parent_column) -> pd.Series:
    if _common.present(row, 'ID') and pull_nodes:
        new_row = pull_node(session, row['ID'])
        _utils.increment_status_df(status, pulled_items=[new_row])
    else:
        new_row = pd.Series(index=_constants.dataframe_columns, dtype=object)

    # In case that properties are specified, but IDs are given, the user-given properties
    # override those pulled from Seeq
    for prop, value in row.items():
        if prop == 'Type' and _common.present(new_row, 'Type'):
            continue
        if prop in ['Path', 'Asset']:
            prop = 'Path'
            value = _path.determine_path(row)
        add_tree_property(new_row, prop, value)

    if not _common.present(new_row, 'Type') and not _common.present(new_row, 'Formula') \
            and not _common.present(new_row, 'Roll Up Statistic'):
        if _common.present(new_row, 'Measured Item'):
            new_row['Type'] = 'ThresholdMetric'
        else:
            new_row['Type'] = 'Asset'

    if not _common.present(new_row, 'Path'):
        new_row['Path'] = ''
    new_row['Depth'] = new_row['Path'].count('>>') + 2 if new_row['Path'] else 1

    if keep_parent_column and _common.present(row, 'Parent'):
        new_row['Parent'] = row['Parent']

    return new_row


def make_node_reference(row, display_template_map, session: Session) -> pd.Series:
    row = row.copy()
    if _common.present(row, 'ID'):
        if _common.get(row, 'Type') in _constants.data_types and not is_reference(row):
            _metadata.build_reference(session, row)
        elif 'Metric' in _common.get(row, 'Type'):
            row = pull_calculation(row, session)
        elif _common.get(row, 'Type') == 'Display':
            row = row.copy()
            displays_api = DisplaysApi(session.client)
            template = displays_api.get_display(id=row['ID']).template  # type: DisplayTemplateOutputV1
            template_id = template.id.upper()
            row['Template ID'] = template_id

            if template_id not in display_template_map:
                display_template_map[template_id] = DisplayTemplateInputV1(
                    name=template.name,
                    description=template.description,
                    source_workstep_id=template.source_workstep_id,
                    swap_source_asset_id=template.swap_source_asset_id
                )
        if _common.present(row, 'ID'):
            row['Referenced ID'] = row['ID']
    row['ID'] = np.nan
    return row


def is_reference(row) -> bool:
    if not _common.get(row, 'Referenced ID') or not _common.get(row, 'Formula Parameters'):
        return False
    formula = _common.get(row, 'Formula')
    if formula is not None and re.match(r'^\$\w+$', formula):
        return True
    else:
        return False


def pull_calculation(row, session: Session) -> pd.Series:
    formulas_api = FormulasApi(session.client)
    metrics_api = MetricsApi(session.client)
    displays_api = DisplaysApi(session.client)
    if _common.get(row, 'Type') in _constants.calculated_types and _common.present(row, 'ID'):
        row = row.copy()
        formula_output = formulas_api.get_item(id=row['ID'])  # type: FormulaItemOutputV1
        row['Formula'] = formula_output.formula
        row['Formula Parameters'] = {
            p.name: p.item.id if p.item else p.formula
            for p in formula_output.parameters
        }
    elif _common.get(row, 'Type') == 'Display' and _common.present(row, 'ID'):
        row = row.copy()
        display_output = displays_api.get_display(id=row['ID'])
        row['Template ID'] = display_output.template.id.upper()
    elif 'Metric' in _common.get(row, 'Type') and _common.present(row, 'ID'):
        row = row.copy()
        row['Type'] = 'ThresholdMetric'

        def str_from_scalar_value_output(scalar_value_output):
            # noinspection PyProtectedMember
            return _metadata.str_from_scalar_value_dict(
                _metadata.dict_from_scalar_value_output(scalar_value_output)).strip()

        def set_metric_value_if_present(column_name, val):
            if val is not None:
                if isinstance(val, ScalarValueOutputV1):
                    row[column_name] = str_from_scalar_value_output(val)
                elif isinstance(val, ItemPreviewWithAssetsV1):
                    row[column_name] = val.id
                else:
                    row[column_name] = val

        metric = metrics_api.get_metric(id=row['ID'])  # type: ThresholdMetricOutputV1
        set_metric_value_if_present('Aggregation Function', metric.aggregation_function)
        set_metric_value_if_present('Bounding Condition', metric.bounding_condition)
        set_metric_value_if_present('Bounding Condition Maximum Duration', metric.bounding_condition_maximum_duration)
        set_metric_value_if_present('Duration', metric.duration)
        set_metric_value_if_present('Measured Item', metric.measured_item)
        set_metric_value_if_present('Metric Neutral Color', metric.neutral_color)
        set_metric_value_if_present('Number Format', metric.number_format)
        set_metric_value_if_present('Period', metric.period)
        set_metric_value_if_present('Process Type', metric.process_type)
        if metric.thresholds:
            thresholds_dict = dict()
            for threshold in metric.thresholds:  # type: ThresholdOutputV1
                # Key can be 'HiHi#FF0000' or 'HiHi'
                key = threshold.priority.name
                if threshold.priority.color:
                    key += threshold.priority.color
                # Value could get pulled as an ID or as a string-ified value
                value = ''
                if not threshold.is_generated and threshold.item:
                    value = threshold.item.id
                elif threshold.value is not None:
                    if isinstance(threshold.value, ScalarValueOutputV1):
                        value = str_from_scalar_value_output(threshold.value)
                    else:
                        value = threshold.value
                if key and value:
                    thresholds_dict[key] = value
            row['Thresholds'] = thresholds_dict

    return row


def pull_node(session: Session, node_id) -> pd.Series:
    """
    Returns a dataframe row corresponding to the item given by node_id
    """
    items_api = _login.get_api(session, ItemsApi)

    item_output = items_api.get_item_and_all_properties(id=node_id)  # type: ItemOutputV1
    node = pd.Series(index=_constants.dataframe_columns, dtype=object)

    # Extract only the properties we use
    node['Name'] = item_output.name
    node['Type'] = item_output.type
    node['ID'] = item_output.id  # If this should be a copy, it'll be converted to 'Referenced ID' later
    for prop in item_output.properties:  # type: PropertyOutputV1
        add_tree_property(node, prop.name, prop.value)
    if 'Metric' in node['Type']:
        node = pull_calculation(node, session)

    return node


def add_tree_property(properties, key, value) -> pd.Series:
    """
    If the property is one which is used by SPy Trees, adds the key+value pair to the dict.
    """
    if key in _constants.dataframe_columns or key in ['Datasource Class', 'Datasource ID']:
        value = _common.none_to_nan(value)
        if key not in properties or not (pd.api.types.is_scalar(value) and pd.isnull(value)):
            properties[key] = value
    return properties


def formula_parameters_to_dict(formula_parameters) -> dict:
    if isinstance(formula_parameters, dict) or (pd.api.types.is_scalar(formula_parameters) and pd.isnull(
            formula_parameters)):
        return formula_parameters

    if isinstance(formula_parameters, str):  # formula_parameters == 'x=2b17adfd-3308-4c03-bdfb-bf4419bf7b3a'
        # handle an empty string case
        if len(formula_parameters) == 0:
            return dict()
        else:
            formula_parameters = [formula_parameters]

    if isinstance(formula_parameters, pd.Series):
        formula_parameters = formula_parameters.tolist()

    formula_dictionary = dict()
    if isinstance(formula_parameters, list):  # formula_parameters == ['x=2b17adfd-3308-4c03-bdfb-bf4419bf7b3a', ...]
        for param in formula_parameters:  # type: str
            split_list = param.split('=')  # ['x', '2b17...']
            if len(split_list) != 2:
                raise SPyException(f'Formula Parameter: {param} needs to be in the format \'paramName=inputItem\'.')
            formula_dictionary[split_list[0].strip()] = split_list[1].strip()
    return formula_dictionary  # output == {'x': '2b17adfd-3308-4c03-bdfb-bf4419bf7b3a'}


def format_references(df) -> pd.DataFrame:
    out_df = df.copy()
    out_df['Formula Parameters'] = pd.Series(np.nan, index=df.index, dtype=object)
    out_df['Thresholds'] = pd.Series(np.nan, index=df.index, dtype=object)

    for node in _match.TreeNode.of(df):  # type: _match.TreeNode
        if pd.api.types.is_dict_like(df.loc[node.index, 'Formula Parameters']):
            formula_parameters = copy.deepcopy(df.loc[node.index, 'Formula Parameters'])
            for name, reference in formula_parameters.items():
                if not isinstance(reference, str) or _common.is_guid(reference):
                    continue
                formula_parameters[name] = node.resolve_reference(reference).full_path
            out_df.at[node.index, 'Formula Parameters'] = formula_parameters

        if isinstance((df.loc[node.index, 'Measured Item']), str):
            if not _common.is_guid(df.loc[node.index, 'Measured Item']):
                out_df.at[node.index, 'Measured Item'] = node.resolve_reference(
                    df.loc[node.index, 'Measured Item']).full_path

        if isinstance(df.loc[node.index, 'Bounding Condition'], str):
            if not _common.is_guid(df.loc[node.index, 'Bounding Condition']):
                out_df.at[node.index, 'Bounding Condition'] = node.resolve_reference(
                    df.loc[node.index, 'Bounding Condition']).full_path

        if pd.api.types.is_dict_like(df.loc[node.index, 'Thresholds']):
            thresholds = copy.deepcopy(df.loc[node.index, 'Thresholds'])
            for name, reference in thresholds.items():
                if not isinstance(reference, str) or _common.is_guid(reference):
                    continue
                try:
                    path_list = _common.path_string_to_list(node.resolve_reference(reference).full_path)
                    # Use a dict so _push can differentiate a string Threshold and a reference to another item
                    thresholds[name] = {
                        'Path': _common.path_list_to_string(path_list[:-1]),
                        'Name': path_list[-1]
                    }
                except SPyRuntimeError as e:
                    # If reference was not found, we treat it like a string threshold and ignore
                    if 'invalid, missing, or has been removed' not in e.args[0]:
                        raise
            out_df.at[node.index, 'Thresholds'] = thresholds

        if isinstance(df.loc[node.index, 'Roll Up Parameters'], str):
            statistic = df.loc[node.index, 'Roll Up Statistic'].casefold()
            fn = next((fn for fn in _common.ROLL_UP_FUNCTIONS if fn.statistic == statistic), None)
            if fn is None:
                valid_statistics = set(fn.statistic.capitalize() for fn in _common.ROLL_UP_FUNCTIONS)
                raise SPyRuntimeError(f"Roll up statistic '{statistic}' not found. Valid options: "
                                      f"{', '.join(valid_statistics)}")
            items = node.resolve_references(df.loc[node.index, 'Roll Up Parameters'])
            formula_parameters = ItemGroup([item.full_path for item in items]).as_parameters()
            formula = fn.generate_formula(formula_parameters)
            out_df.at[node.index, 'Formula'] = formula
            out_df.at[node.index, 'Formula Parameters'] = formula_parameters

    return out_df


def push_and_replace_display_templates(session: Session, df, display_template_map, datasource, workbook_id,
                                       status: Status):
    display_templates_api = DisplayTemplatesApi(session.client)
    template_id_map = dict()
    datasource_output = _metadata.create_datasource(session, datasource)
    for original_id, display_template_input in display_template_map.items():
        display_template_input.datasource_class = datasource_output.datasource_class
        display_template_input.datasource_id = datasource_output.datasource_id
        display_template_input.scoped_to = workbook_id
        display_template_output = safely(
            lambda: display_templates_api.create_display_template(body=display_template_input),
            action_description=f'create new Display Template to replace original {original_id}',
            status=status)
        template_id_map[original_id] = display_template_output.id if display_template_output else None
    df['Template ID'] = df['Template ID'].replace(template_id_map)


def archive_and_remove_displays(session: Session, display_ids):
    displays_api = DisplaysApi(session.client)
    trees_api = TreesApi(session.client)
    for display_id in display_ids:
        try:
            trees_api.remove_node_from_tree(id=display_id)
        except ApiException:
            pass
        try:
            displays_api.archive_display(id=display_id)
        except ApiException:
            pass
