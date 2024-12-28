from __future__ import annotations

from collections import defaultdict
from typing import Tuple, List

import pandas as pd

from seeq.spy import _common, _metadata, _status
from seeq.spy._errors import *
from seeq.spy._session import Session
from seeq.spy.assets._trees import _constants, _match, _path, _utils
from seeq.spy.assets._trees._constants import supported_input_types
from seeq.spy.assets._trees._pandas import KeyedDataFrame


def validate_and_filter(
        session: Session,
        df_to_validate: pd.DataFrame,
        status: _status.Status,
        stage: str,
        temporal_description='',
        raise_if_all_filtered=False,
        subtract_errors_from_status=False
) -> KeyedDataFrame:
    """
    This is the main validation function. It takes a dataframe as input, and returns a dataframe containing the rows
    for which no errors were found, and a dataframe containing the rows for which errors were found.

    :param session: Login session to use for validation
    :param df_to_validate: DataFrame input
    :param status: Status object to raise exception to if error is found and status.errors='raise'

    :param stage: {'input', 'final'} This must be a key in property_validations. Indicates which
    property validations we want to perform on df_to_validate, depending on whether it is a user-input
    dataframe, a dataframe about to be saved to Tree._dataframe, or some intermediary dataframe. Additionally,
    we only check the tree structure of the dataframe if stage='final'

    :param temporal_description: An adverbial phrase describing the time at which the error(s) were encountered
    :return filtered_df: A validated dataframe with invalid rows removed
    """
    df_to_validate = KeyedDataFrame.of(df_to_validate)
    raise_if_all_filtered = raise_if_all_filtered or stage == 'final'
    error_summaries, error_series = validate(session, df_to_validate, status, stage)
    if len(error_summaries) != 0:
        if status.errors == 'raise':
            df_to_validate['Error Message'] = error_series
            status.df = df_to_validate[error_series != ''].dropna(axis=1, how='all')
            raise_error_summaries(temporal_description, error_summaries, status)
        else:
            keep_items = error_series == ''
            bad_results = df_to_validate[~keep_items].copy()
            bad_results['Error Message'] = error_series[~keep_items]
            filtered_df = df_to_validate[keep_items].reset_index(drop=True)
            _utils.increment_status_df(status, error_items=bad_results, subtract_errors=subtract_errors_from_status)

            if raise_if_all_filtered and filtered_df.empty:
                status.df = bad_results.dropna(axis=1, how='all')
                status.warn('All rows encountered errors and tree could not be constructed')
                raise_error_summaries(temporal_description, error_summaries, status)

            if stage == 'final':
                # We validate again to ensure that self._dataframe will stay valid. Something is fatally wrong
                # with validation if the following code is reached.
                further_error_summaries, error_series = validate(session, filtered_df)
                if len(further_error_summaries) != 0:
                    filtered_df['Error Message'] = error_series
                    status.df = filtered_df[error_series != ''].dropna(axis=1, how='all')
                    raise_error_summaries('while validating tree', error_summaries, status)

            warn_error_summaries(temporal_description, error_summaries, status)

            return rectify_column_order(filtered_df)
    else:
        return rectify_column_order(df_to_validate)


def validate(
        session: Session, df: KeyedDataFrame, status: _status.Status = None, stage='final'
) -> Tuple[List[str], pd.Series]:
    if status is None:
        status = _status.Status(quiet=True)

    error_summaries_properties, error_series_properties = validate_properties(session, df, stage)
    if stage == 'final':
        # Only do tree validation in the final validation step, i.e., when this df represents a tree
        # Don't do tree validation on rows that had property errors
        ignore_rows = error_series_properties != ''
        error_summaries_tree, error_series_tree = validate_tree_structure(df, ignore_rows=ignore_rows)

        ignore_rows = ignore_rows | (error_series_tree != '')
        error_summaries_dependencies, error_series_dependencies = validate_dependencies(df, status,
                                                                                        ignore_rows=ignore_rows)
    else:
        error_summaries_tree, error_series_tree = list(), pd.Series('', index=df.index)
        error_summaries_dependencies, error_series_dependencies = list(), pd.Series('', index=df.index)

    error_summaries = error_summaries_properties + error_summaries_tree + error_summaries_dependencies
    error_series = update_error_msg(error_series_properties, error_series_tree)
    error_series = update_error_msg(error_series, error_series_dependencies)

    return error_summaries, error_series


def get_error_message(temporal_description, error_summaries):
    count = len(error_summaries)
    msg = '%s encountered %s' % ('1 error was' if count == 1 else f'{count} errors were', temporal_description)
    msg += ':\n- ' + '\n- '.join(error_summaries[:_constants.MAX_ERRORS_DISPLAYED])
    if count > _constants.MAX_ERRORS_DISPLAYED:
        additional_errors = count - _constants.MAX_ERRORS_DISPLAYED
        msg += f'\n- {additional_errors} additional issue{"s" if additional_errors > 1 else ""} found.'
    return msg


def raise_error_summaries(temporal_description, error_summaries, status):
    msg = get_error_message(temporal_description, error_summaries)
    status.exception(SPyRuntimeError(msg), throw=True, use_error_message=True)


def warn_error_summaries(temporal_description, error_summaries, status):
    msg = get_error_message(temporal_description, error_summaries)
    status.warn(msg)
    status.update()


def update_error_msg(old_msg, new_msg):
    if new_msg is None or isinstance(new_msg, str) and new_msg == '':
        return old_msg
    out = old_msg + ' ' + new_msg
    if isinstance(out, pd.Series):
        return out.str.strip()
    else:
        return out.strip()


def validate_tree_structure(df, ignore_rows=None):
    # Asserts that:
    # - The tree is non-empty
    # - The root doesn't have a path, and is the only item with depth 1
    # - The dataframe is sorted by path
    # - There are no missing assets referenced in paths
    # - Paths reflect names of preceding items
    # - Depths reflects lengths of paths

    size = len(df)
    if size == 0:
        return ['Tree must be non-empty.'], pd.Series(dtype=str)

    error_series = pd.Series('', index=df.index)
    error_summaries = []
    if ignore_rows is None:
        ignore_rows = pd.Series(False, index=df.index)

    prev_path = list()
    prev_type = 'Asset'
    _path.decorate_with_full_path(df)
    for i, row in df.iterrows():
        if error_series.iloc[i]:
            # Node has an error message already due to a bad ancestor
            continue

        depth = row.Depth
        this_path = row['Full Path List']

        try:
            if ignore_rows[i]:
                # Ignore tree errors on this row because of a property validation error
                # We still want invalidate its children if possible, so we raise an assertion error with no message
                assert False, ''

            assert depth == len(this_path), 'Item\'s depth does not match its path.'

            if i == 0:
                assert len(this_path) == 1, 'The root of the tree cannot be assigned a path.'
                # The following assertion will be handled differently to include node names in the error message
                assert (df['Full Path List'].iloc[1:].apply(len) != 1).all(), 'A tree can only have one root but ' \
                                                                              'multiple were given: '
            else:
                assert depth >= 1, 'Only depths greater or equal to 1 are valid.'

            if depth <= len(prev_path):
                assert prev_path[:depth - 1] == this_path[:depth - 1], 'Item\'s position in tree ' \
                                                                       'does not match its path.'
                assert prev_path[depth - 1] != this_path[depth - 1], 'Item has the same name and path ' \
                                                                     'of another item in the tree.'
                assert prev_path[depth - 1] < this_path[depth - 1], 'Item is not stored in proper ' \
                                                                    'position sorted by path.'
            else:
                assert depth == len(prev_path) + 1, 'Item has an ancestor not stored in this tree.'
                assert prev_path[:depth - 1] == this_path[:depth - 1], 'Item\'s position in tree ' \
                                                                       'does not match its path.'
                assert prev_type == 'Asset', 'Item\'s parent must be an Asset.'

            prev_path = this_path
            prev_type = row.Type

        except AssertionError as e:
            message = str(e)
            if message.startswith('A tree can only have one root'):
                roots = df.Depth == 1
                message += '"%s".' % '\", \"'.join(df.Name[roots])
                error_series[roots] = message
                error_series[~roots] = 'Item\'s parent is invalid.'
                error_summaries.append(message)
                break
            error_series[i] = message
            children = df['Full Path List'].apply(
                lambda path: len(path) > len(this_path) and path[:len(this_path)] == this_path)
            error_series[children] = 'Item\'s parent is invalid.'
            if message:
                error_summaries.append(f'Invalid item with path "{" >> ".join(this_path)}": ' + message)

    _path.remove_full_path(df)

    return error_summaries, error_series


def validate_dependencies(df, status: _status.Status, ignore_rows=None):
    error_series = pd.Series('', index=df.index)
    error_summaries = set()
    if ignore_rows is None:
        ignore_rows = pd.Series(False, index=df.index)

    if len(df) == 0 or ignore_rows.all():
        return list(), pd.Series('', index=df.index)

    if ignore_rows.any():
        df = df.loc[~ignore_rows]
    dependency_graph = defaultdict(set)

    for node in _match.TreeNode.of(df):  # type: _match.TreeNode
        dependencies = list()
        if 'Formula Parameters' in df.columns and pd.api.types.is_dict_like(df.loc[node.index, 'Formula Parameters']):
            dependencies.extend(df.loc[node.index, 'Formula Parameters'].values())
        if 'Measured Item' in df.columns and isinstance(df.loc[node.index, 'Measured Item'], str):
            dependencies.append(df.loc[node.index, 'Measured Item'])
        if 'Bounding Condition' in df.columns and isinstance(df.loc[node.index, 'Bounding Condition'], str):
            dependencies.append(df.loc[node.index, 'Bounding Condition'])
        for dependency in dependencies:
            if not isinstance(dependency, str) or _common.is_guid(dependency):
                continue
            try:
                resolved_dependency = node.resolve_reference(dependency)
                dependency_graph[node].add(resolved_dependency)
            except SPyRuntimeError as e:
                message = e.args[0]
                error_series[node.index] = update_error_msg(error_series[node.index], message)
                error_summaries.add(message)
        if 'Roll Up Parameters' in df.columns and isinstance(df.loc[node.index, 'Roll Up Parameters'], str):
            try:
                resolved_dependencies = node.resolve_references(
                    df.loc[node.index, 'Roll Up Parameters'])
                dependency_graph[node].update(resolved_dependencies)
                if len(resolved_dependencies) == 0:
                    status.warn(f'Roll up parameter "{df.loc[node.index, "Roll Up Parameters"]}" does not match any '
                                f'items in the tree.')
            except SPyRuntimeError as e:
                message = e.args[0]
                error_series[node.index] = update_error_msg(error_series[node.index], message)
                error_summaries.add(message)

    node_results = dict()

    def check_for_invalid_dependencies(node, depth=0):
        if depth > _constants.MAX_FORMULA_DEPENDENCY_DEPTH:
            raise SPyRuntimeError('Circular dependencies detected. Check that the formula parameters for the specified '
                                  'items do not create a loop.')

        if error_series[node.index] != '' or ignore_rows[node.index]:
            node_results[node] = False
        if node in node_results:
            return node_results[node]
        for dependency in dependency_graph[node]:
            if not check_for_invalid_dependencies(dependency, depth + 1):
                error_series[node.index] = 'Item references an invalid tree item.'
                break
        result = error_series[node.index] == ''
        node_results[node] = result
        return result

    for node in list(dependency_graph.keys()):
        check_for_invalid_dependencies(node)

    return list(error_summaries), error_series


def validate_properties(session: Session, df, stage):
    """
    :param session: Login session to use for validation
    :param df: The dataframe to be validated for errors related to presence of properties, type of properties,
    and ill-defined properties
    """
    error_series = pd.Series('', index=df.index)
    error_message_map = dict()  # maps error messages to the rows that encountered the error
    for index, node in df.iterrows():
        errors = validate_node_properties(session, node, stage)
        for error in errors:
            _common.get(error_message_map, error, default=list(), assign_default=True).append((index, node))
        if errors:
            error_series[index] = ' '.join(errors)

    error_summaries = collect_error_messages(error_message_map)

    return error_summaries, error_series


def collect_error_messages(error_message_map):
    def _get_row_description(index, row):
        description_properties = dict()
        # Prefer Friendly Name over Name
        if _common.present(row, 'Friendly Name'):
            description_properties['friendly name'] = row['Friendly Name']
        elif _common.present(row, 'Name'):
            description_properties['name'] = row['Name']
        # If a Name or Friendly Name has been found, add a Path too if it is present
        if len(description_properties) != 0 and (_common.present(row, 'Path') or _common.present(row, 'Asset')):
            description_properties['path'] = _path.determine_path(row)
        # Use ID next if it is present
        if len(description_properties) == 0 and _common.present(row, 'ID'):
            description_properties['ID'] = row['ID']

        # Use index if none of the above are present
        if len(description_properties) == 0:
            return f'The item with index {index}'
        else:
            return 'The item with ' + ' and '.join([f'{prop_name} "{prop_value}"' for prop_name, prop_value in
                                                    description_properties.items()])

    def _get_row_descriptiveness_score(row):
        _, row = row
        if _common.present(row, 'Name') or _common.present(row, 'Friendly Name'):
            if _common.present(row, 'Path') or _common.present(row, 'Asset'):
                return 3
            else:
                return 2
        elif _common.present(row, 'ID'):
            return 1
        else:
            return 0

    def _get_most_descriptive_row(_rows):
        index, row = max(_rows, key=_get_row_descriptiveness_score)
        return _get_row_description(index, row)

    collected_messages = list()
    for message, rows in error_message_map.items():
        if len(collected_messages) >= _constants.MAX_ERRORS_DISPLAYED:
            # No need to fuss with error messaging formatting that won't be displayed. We pass in placeholder string
            collected_messages.extend(('' for _ in range(len(error_message_map) - _constants.MAX_ERRORS_DISPLAYED)))
            break
        if len(rows) == 1:
            collected_messages.append(f'{_get_row_description(*rows[0])} has the following issue: {message}')
        else:
            collected_messages.append(f'{_get_most_descriptive_row(rows)} and {len(rows) - 1} other '
                                      f'item{"s" if len(rows) > 2 else ""} has the following issue: {message}')
    return collected_messages


def validate_node_properties(session, node, stage):
    def has_bad_type(column, dtype):
        if _common.present(node, column):
            datum = _utils.safe_int_cast(node[column])
            try:
                _common.validate_argument_types([(datum, '', dtype)])
            except TypeError:
                return True
        return False

    def dtype_names(dtype):
        if isinstance(dtype, tuple):
            return tuple(x.__name__ for x in dtype)
        return dtype.__name__

    errors = [f"The property '{column}' must have one of the following types: {dtype_names(dtype)}."
              for column, dtype in _constants.dataframe_dtypes.items() if has_bad_type(column, dtype)]

    # The conditions in property_validations assume that values have the correct datatype
    # Therefore, return only type errors if they exist.
    if errors:
        return errors
    errors += [message for requirement, message in property_validations(session, node, stage) if not requirement]

    return errors


def no_repeated_nested_paths(path_list, name, recurse=True):
    if len(path_list) == 0:
        return True
    if path_list[-1] == name:
        return False
    return no_repeated_nested_paths(path_list[:-1], path_list[-1]) if recurse else True


def rectify_column_order(df):
    standard_columns = [col for col in _constants.dataframe_columns if col in df.columns]
    extra_columns = sorted([col for col in df.columns if col not in _constants.dataframe_columns])
    columns = standard_columns + extra_columns
    return df[columns]


def property_validations(session: Session, node, stage):
    if stage == 'input':
        return [
            (_common.get(node, 'ID') or _common.get(node, 'Name') or _common.get(node, 'Friendly Name'),
             "The property 'Name' or 'Friendly Name' is required for all nodes without ID."),
            (not _common.get(node, 'Type') or _common.get(node, 'Formula') or _common.get(node, 'Roll Up Statistic') or
             ('Condition' not in node['Type'] and 'Signal' not in node['Type']) or _common.get(node, 'ID'),
             "Stored Signals and Conditions are not yet supported. "
             "All Signals and Conditions require either a formula or an ID."),
            (not _common.present(node, 'Formula Parameters') or _common.present(node, 'Formula'),
             "Must have a Formula if Formula Parameters are defined."),
            (not (_common.present(node, 'ID') or _common.present(node, 'Referenced ID')) or session.client,
             "Must log in via spy.login() before inserting an item via ID or Referenced ID."),
            (not _common.present(node, 'ID') or _common.is_guid(node['ID']),
             f"The property 'ID' must be a valid GUID. Given: '{_common.get(node, 'ID')}'"),
            (not _common.present(node, 'Referenced ID') or _common.is_guid(node['Referenced ID']),
             f"The property 'Referenced ID' must be a valid GUID. Given: '{_common.get(node, 'Referenced ID')}'"),
            (not (_common.get(node, 'Path') and _common.get(node, 'Name')) or
             no_repeated_nested_paths(_common.path_string_to_list(node['Path']), node['Name']),
             "Paths with repeated names are not valid."),
            ((not (_common.present(node, 'Formula') or _common.present(node, 'Formula Parameters'))
              or not _common.get(node, 'Type') == 'Asset'),
             "Assets cannot have formulas or formula parameters."),
            # Start being more stringent about what types can be inserted into a SPy Tree
            (session.options.wants_compatibility_with(190) or _common.get(node, 'Type') in supported_input_types or
             (not _common.present(node, 'Type')),
             f"Items of type '{_common.get(node, 'Type')}' are not supported"),
            (not _common.get(node, 'Type') == 'Asset' or not _common.present(node, 'Roll Up Statistic'),
             "Assets cannot be roll ups."),
            (_is_node_metric_valid(stage, node, session)),
            (_common.present(node, 'Roll Up Statistic') == _common.present(node, 'Roll Up Parameters'),
             "Roll ups must specify both 'Roll Up Statistic' and 'Roll Up Parameters'."),
            (not _common.present(node, 'Roll Up Statistic') or not _common.present(node, 'Formula'),
             "Roll ups cannot have a formula."),
            (not _common.present(node, 'Roll Up Statistic')
             or node['Roll Up Statistic'].casefold() in (fn.statistic for fn in _common.ROLL_UP_FUNCTIONS),
             f"Roll up statistic '{_common.get(node, 'Roll Up Statistic')}' not found. Valid options: "
             f"{', '.join(set(fn.statistic.capitalize() for fn in _common.ROLL_UP_FUNCTIONS))}")
        ]
    elif stage == 'final':
        return [
            (_common.get(node, 'Formula') or _common.get(node, 'Type') or _common.get(node, 'Roll Up Statistic'),
             "The property 'Type' is required for all items without formulas or roll-up statistics."),
            (not _common.get(node, 'Type') or _common.get(node, 'Formula') or _common.get(node, 'Roll Up Statistic') or
             ('Condition' not in node['Type'] and 'Signal' not in node['Type']),
             "Stored Signals and Conditions are not yet supported. All Signals and Conditions require a formula."),
            (not _common.present(node, 'Formula Parameters') or _common.present(node, 'Formula'),
             "Must have a Formula if Formula Parameters are defined."),
            (_common.present(node, 'Name'),
             "The property 'Name' is required."),
            (_common.present(node, 'Path'),
             "The property 'Path' is required."),
            (_common.present(node, 'Depth'),
             "The property 'Depth' is required."),
            (not _common.present(node, 'Name') or not _common.present(node, 'Path') or
             no_repeated_nested_paths(_common.path_string_to_list(node['Path']), node['Name'], recurse=False),
             "Paths with repeated names are not valid."),
            ((not (_common.present(node, 'Formula') or _common.present(node, 'Formula Parameters'))
              or not _common.get(node, 'Type') == 'Asset'),
             "Assets cannot have formulas or formula parameters."),
            # Start being more stringent about what types can be inserted into a SPy Tree
            (session.options.wants_compatibility_with(190) or _common.get(node, 'Type') in supported_input_types or
             (not _common.present(node, 'Type')),
             f"Items of type '{_common.get(node, 'Type')}' are not supported"),
            (not _common.get(node, 'Type') == 'Asset' or not _common.present(node, 'Roll Up Statistic'),
             "Assets cannot be roll ups."),
            (_is_node_metric_valid(stage, node, session)),
            (_common.present(node, 'Roll Up Statistic') == _common.present(node, 'Roll Up Parameters'),
             "Roll ups must specify both 'Roll Up Statistic' and 'Roll Up Parameters'."),
            (not _common.present(node, 'Roll Up Statistic') or not _common.present(node, 'Formula'),
             "Roll ups cannot have a formula."),
        ]


def _is_node_metric_valid(stage: str, node: pd.Series, session: Session) -> (bool, str):
    if not _common.present(node, 'Type') or 'Metric' not in _common.get(node, 'Type'):
        return True, None  # Okay because this isn't a metric
    if not _common.present(node, 'Measured Item'):
        if stage == 'input' and not _common.present(node, 'ID'):
            return False, 'Metrics must have a Measured Item or an ID.'
        elif stage == 'final':
            return False, 'Metrics must have a Measured Item.'
    if _common.present(node, 'Metric Neutral Color'):
        neutral_color = _common.get(node, 'Metric Neutral Color')
        if not neutral_color.startswith('#') or not (len(neutral_color) == 4 or len(neutral_color) == 7):
            return False, "Metric neutral color must start with a '#' character and be a valid hex value."
    if _common.present(node, 'Statistic'):
        try:
            _common.statistic_to_aggregation_function(node['Statistic'])
        except SPyValueError as e:
            return False, str(e)
    if _common.present(node, 'Thresholds'):
        thresholds = _common.get(node, 'Thresholds')
        if isinstance(thresholds, dict):
            session_validated = Session.validate(session)
            if session_validated:
                try:
                    _metadata.convert_threshold_levels_from_system(session_validated, thresholds, node['Name'])
                except SPyRuntimeError as e:
                    return False, str(e)
        # Other valid `thresholds` types include a List, but that's likely directly from Appserver so do not
        # validate it here. All other types will have been caught by the dataframe_dtypes check.
    return True, None
