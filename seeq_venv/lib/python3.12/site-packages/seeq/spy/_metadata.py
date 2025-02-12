from __future__ import annotations

import json
import os
import re
import types
import warnings
from dataclasses import dataclass
from typing import Callable, Dict, Optional, List, Tuple, Set, Union

import numpy as np
import pandas as pd

from seeq.base import util
from seeq.base.seeq_names import SeeqNames
from seeq.sdk import *
from seeq.spy import _common, _compatibility, _login
from seeq.spy._common import EMPTY_GUID
from seeq.spy._errors import *
from seeq.spy._metadata_push_results import PushResults, ORIGINAL_INDEX_COLUMN
from seeq.spy._push import WorkbookContext
from seeq.spy._redaction import safely
from seeq.spy._session import Session
from seeq.spy._status import Status
from seeq.spy.workbooks._item import Item
from seeq.spy.workbooks._workstep import AnalysisWorkstep


@dataclass
class PushContext:
    workbook_context: WorkbookContext
    datasource_output: DatasourceOutputV1

    planning_to_archive: bool
    cleanse_data_ids: bool
    override_scope: bool
    validate_ui_configs: bool
    sync_token: str
    batch_size: int

    roots: Dict[str, AssetInputV1]
    reified_assets: Set[str]
    last_scalar_datasource: Optional[Tuple[str, str]]
    flush_now: bool
    push_results: PushResults
    previous_results: PushResults

    asset_batch_input: AssetBatchInputV1
    condition_batch_input: ConditionBatchInputV1
    put_scalars_input: PutScalarsInputV1
    put_signals_input: PutSignalsInputV1
    tree_batch_input: AssetTreeBatchInputV1
    threshold_metric_inputs: List[ThresholdMetricInputV1]
    display_template_inputs: List[DisplayTemplateInputV1]
    display_inputs: List[DisplayInputV1]


class SkippedPush(Exception):
    pass


def push(session: Session, metadata, workbook_context: WorkbookContext, datasource_output,
         status: Status, *, sync_token: Optional[str] = None, row_filter: Optional[Callable] = None,
         cleanse_df_first: bool = True, state_file: Optional[str] = None, planning_to_archive: bool = False,
         cleanse_data_ids: bool = True, global_inventory: str = 'copy local',
         validate_ui_configs: bool = True) -> pd.DataFrame:
    items_api = ItemsApi(session.client)
    trees_api = TreesApi(session.client)

    metadata_df = metadata  # type: pd.DataFrame

    timer = _common.timer_start()

    status_columns = [
        'Signal',
        'Scalar',
        'Condition',
        'Threshold Metric',
        'Display',
        'Display Template',
        'Asset',
        'Relationship',
        'Overall',
        'Time'
    ]

    status_dict = dict()
    for status_column in status_columns:
        status_dict[status_column] = 0

    status.df = pd.DataFrame([status_dict], index=['Items pushed'])

    status.update('Pushing metadata to datasource <strong>%s [%s]</strong> scoped to workbook ID '
                  '<strong>%s</strong>' % (
                      datasource_output.name, datasource_output.datasource_id, workbook_context.workbook_id),
                  Status.RUNNING)

    def _print_push_progress():
        status.df['Time'] = _common.timer_elapsed(timer)
        status.update('Pushing metadata to datasource <strong>%s [%s]</strong> scoped to workbook ID '
                      '<strong>%s</strong>' % (
                          datasource_output.name, datasource_output.datasource_id, workbook_context.workbook_id),
                      Status.RUNNING)

    if cleanse_df_first:
        _common.validate_unique_dataframe_index(metadata_df, 'metadata')

        # Make sure the columns of the dataframe can accept anything we put in them since metadata_df might have
        # specific dtypes.
        metadata_df = metadata_df.copy().astype(object)

        if 'Push Result' in metadata_df.columns:
            metadata_df = metadata_df.drop(columns=['Push Result'])

    # We need the index to be regular (unique, integer, ascending) so that we can add Asset entries to the bottom.
    # This is tested by _metadata.test_metadata_dataframe_weird_index()
    original_index_name = metadata_df.index.name
    metadata_df.index.set_names([ORIGINAL_INDEX_COLUMN], inplace=True)
    metadata_df.reset_index(inplace=True)

    if 'Type' not in metadata_df.columns:
        metadata_df['Type'] = pd.Series(dtype=object)

    if not _login.is_server_version_at_least(64, session=session):
        if 'Asset Group Member' in metadata_df.columns:
            # In R63 and earlier, there is a guard against setting "Asset Group Member" directly. (This property allows
            # users with read-only access to a workbook to still modify an Asset Group. So in R63 and earlier we
            # concede that the read-only functionality won't be present if you're pushing things from SPy.)
            metadata_df.drop(columns=['Asset Group Member'], inplace=True)

    push_results = PushResults(metadata_df)

    previous_results = None
    if state_file is not None and util.safe_exists(state_file):
        # noinspection PyBroadException
        try:
            previous_df = pd.read_pickle(state_file)
            previous_results = PushResults(previous_df.reset_index(drop=True)) if previous_df is not None else None
        except Exception:
            # If we encounter a problem reading the pickle file and status.errors isn't 'raise', then just swallow
            # the error and do a full push.
            if status.errors == 'raise':
                raise

    if session.options.force_calculated_scalars and _compatibility.is_force_calculated_scalars_available():
        put_scalars_input = PutScalarsInputV1(scalars=list(), force_calculated_scalars=True)
    else:
        put_scalars_input = PutScalarsInputV1(scalars=list())

    push_context = PushContext(
        reified_assets=set(),
        roots=dict(),
        batch_size=session.options.metadata_push_batch_size,
        put_signals_input=PutSignalsInputV1(signals=list()),
        put_scalars_input=put_scalars_input,
        condition_batch_input=ConditionBatchInputV1(conditions=list()),
        threshold_metric_inputs=list(),
        display_template_inputs=list(),
        display_inputs=list(),
        asset_batch_input=AssetBatchInputV1(assets=list()),
        tree_batch_input=AssetTreeBatchInputV1(relationships=list(), parent_host_id=datasource_output.id,
                                               child_host_id=datasource_output.id),
        last_scalar_datasource=None,
        datasource_output=datasource_output,
        flush_now=False,
        planning_to_archive=planning_to_archive,
        cleanse_data_ids=cleanse_data_ids,
        override_scope=(global_inventory == 'copy local'),
        validate_ui_configs=validate_ui_configs,
        sync_token=sync_token,
        workbook_context=workbook_context,
        push_results=push_results,
        previous_results=previous_results
    )

    friendly_error_string = None
    while True:
        push_results.start_post_thread()
        dependency_exceptions: Dict[object, SPyDependencyNotFound] = dict()
        dependency_errors: Dict[object, str] = dict()
        at_least_one_item_created = False

        try:
            for index, row in list(push_results.items()):  # type: (object, dict)
                if row_filter is not None and not row_filter(row):
                    continue

                if _common.present(row, 'Push Result'):
                    continue

                try:
                    _process_push_row(session, status, push_context, index, row)
                    status.df['Overall'] += 1

                except SPyDependencyNotFound as e:
                    dependency_errors[index] = str(e)
                    dependency_index = e.dependent_identifier if e.dependent_identifier is not None else index
                    dependency_exceptions[dependency_index] = SPyDependencyNotFound(
                        f'{_common.repr_from_row(row)}: {e}',
                        e.dependent_identifier,
                        e.dependency_identifier)
                    continue

                except SkippedPush:
                    pass

                except SPyException as e:
                    if status.errors == 'raise':
                        raise

                    push_results.at[index, 'Push Result'] = str(e)
                    continue

                at_least_one_item_created = True

                if int(status.df['Overall'].iloc[0]) % push_context.batch_size == 0 or push_context.flush_now:
                    _print_push_progress()

                    _flush(session, status, push_context)

            _print_push_progress()

            _flush(session, status, push_context)

        finally:
            push_results.shut_down_post_thread()
            push_results.drain_responses()

        item_errors = [f"{_common.repr_from_row(pr)}: {pr['Push Result']}"
                       for pr in push_results.values()
                       if _common.present(pr, 'Push Result') and not pr['Push Result'].startswith('Success')]

        if len(item_errors) > 0:
            item_error_string = '\n'.join(sorted(item_errors))
            friendly_error_string = f'Item errors:\n{item_error_string}'

        if len(dependency_exceptions) == 0:
            break

        if not at_least_one_item_created:
            dependency_error_string = SPyDependencyNotFound.generate_error_string(dependency_exceptions)
            if friendly_error_string is None:
                friendly_error_string = f'Dependency errors:\n{dependency_error_string}'
            else:
                friendly_error_string += f'\n\nDependency errors:\n{dependency_error_string}'

            for index, push_result in dependency_errors.items():
                push_results.at[index, 'Push Result'] = push_result

            if status.errors == 'raise':
                raise SPyRuntimeError('Errors encountered. Check "Push Result" column for error details.\n' +
                                      friendly_error_string)

            break

    for asset_input in push_context.roots.values():
        _filter = (f'Datasource Class=={datasource_output.datasource_class} && Datasource ID=='
                   f'{datasource_output.datasource_id} && Data ID=={asset_input.data_id}')
        results = items_api.search_items(filters=[_filter, '@includeUnsearchable'])
        if len(results.items) == 0:
            raise SPyRuntimeError('Root item "%s" not found' % asset_input.name)
        item_id_list = ItemIdListInputV1()
        item_id_list.items = [results.items[0].id]
        trees_api.move_nodes_to_root_of_tree(body=item_id_list)

    results_df = pd.DataFrame(push_results.values())
    if ORIGINAL_INDEX_COLUMN in results_df.columns:
        results_df.set_index(ORIGINAL_INDEX_COLUMN, inplace=True)
        results_df.index.name = original_index_name

    status.df['Time'] = _common.timer_elapsed(timer)
    status.update('Pushed metadata successfully to datasource <strong>%s [%s]</strong> scoped to workbook ID '
                  '<strong>%s</strong>' % (datasource_output.name,
                                           datasource_output.datasource_id,
                                           workbook_context.workbook_id),
                  Status.SUCCESS)

    if state_file is not None:
        state_file_folder = os.path.dirname(state_file)
        if len(state_file_folder) != 0:
            util.safe_makedirs(state_file_folder, exist_ok=True)

        # Specifically use protocol 4 so that the file is portable back to Python 3.4
        results_df.to_pickle(state_file, protocol=4)

    results_df_properties = types.SimpleNamespace(
        func='spy.push(metadata)',
        workbook_id=workbook_context.workbook_id,
        worksheet_id=workbook_context.worksheet_id,
        datasource=datasource_output,
        friendly_error_string=friendly_error_string,
        status=status)

    _common.put_properties_on_df(results_df, results_df_properties)

    return results_df


def _process_push_row(session: Session, status: Status, push_context: PushContext, index: object, row_dict: dict):
    if _common.present(row_dict, 'Path'):
        row_dict['Path'] = _common.sanitize_path_string(row_dict['Path'])

    if not _common.present(row_dict, 'Name'):
        raise SPyRuntimeError('Metadata must have a "Name" column.')

    if _common.get(row_dict, 'Reference') is True:
        already_is_reference = _common.present(row_dict, 'Referenced ID')
        if not already_is_reference:
            if not _common.present(row_dict, 'ID'):
                raise SPyRuntimeError('"ID" column required when "Reference" column is True')
            build_reference(session, row_dict)

    # Set 'Cache Enabled' to False for jump tags
    if isinstance(_common.get(row_dict, 'Formula'), str) and re.match(r'^\s*\$\w+\s*$', row_dict['Formula']):
        row_dict['Cache Enabled'] = False

    if push_context.override_scope:
        row_dict['Scoped To'] = push_context.workbook_context.workbook_id

    if not _common.present(row_dict, 'Type') or not _is_handled_type(row_dict['Type']):
        if not _common.present(row_dict, 'Formula'):
            _common.raise_or_catalog(status, df=push_context.push_results, column='Push Result',
                                     index=index, exception_type=SPyRuntimeError,
                                     message='Items with no valid type specified cannot be pushed unless they are '
                                             'calculations. "Formula" column is required for such items.')
        else:
            formula = _common.get(row_dict, 'Formula')
            if _common.present(row_dict, 'Formula Parameters'):
                formula_parameters = _process_formula_parameters(_common.get(row_dict, 'Formula Parameters'),
                                                                 push_context)
            else:
                formula_parameters = []

            try:
                formulas_api = FormulasApi(session.client)
                if _compatibility.is_compile_formula_and_parameters_available():
                    formula_compile_output = formulas_api.compile_formula_and_parameters(
                        body=FormulaCompileInputV1(formula=formula, parameters=formula_parameters))
                else:
                    with warnings.catch_warnings():
                        warnings.filterwarnings("ignore", category=DeprecationWarning)
                        formula_compile_output = formulas_api.compile_formula(formula=formula,
                                                                              parameters=formula_parameters)
                if formula_compile_output.errors or formula_compile_output.return_type == '':
                    _common.raise_or_catalog(status, df=push_context.push_results, column='Push Result',
                                             index=index, exception_type=SPyRuntimeError,
                                             message=f'Formula compilation failed with message: '
                                                     f'{formula_compile_output.status_message}')
                    return
                else:
                    row_dict['Type'] = formula_compile_output.return_type
            except ApiException as e:
                _common.raise_or_catalog(status, df=push_context.push_results, column='Push Result',
                                         index=index, e=e)

    if _common.present(row_dict, 'Push Result'):
        return

    def _row_datasource_matches_context_datasource() -> bool:
        # We can't update tree relationships using the batch endpoint across multiple datasources. Don't bother with
        # Asset updates if this item appears to be in a datasource that doesn't match the context. CRAB-37889
        if _common.get(row_dict, 'Reference') is True:
            return True
        elif _common.present(row_dict, 'Datasource Class') and _common.present(row_dict, 'Datasource ID'):
            return (_common.get(row_dict, 'Datasource Class') == push_context.datasource_output.datasource_class
                    and _common.get(row_dict, 'Datasource ID') == push_context.datasource_output.datasource_id)
        elif _common.present(row_dict, 'Datasource Name'):
            return _common.get(row_dict, 'Datasource Name') == push_context.datasource_output.name
        else:
            return True

    datasource_matches = _row_datasource_matches_context_datasource()

    scoped_data_id = get_scoped_data_id(row_dict, push_context.workbook_context.workbook_id,
                                        push_context.cleanse_data_ids)
    if not _common.present(row_dict, 'Datasource Class'):
        row_dict['Datasource Class'] = push_context.datasource_output.datasource_class

    if not _common.present(row_dict, 'Datasource ID'):
        row_dict['Datasource ID'] = push_context.datasource_output.datasource_id

    path = determine_path(row_dict)
    parent_data_id = _common.get(row_dict, 'Parent Data ID')
    if parent_data_id is not None:
        if push_context.push_results.get_by_data_id(parent_data_id) is None:
            raise SPyDependencyNotFound(f'Parent Data ID {parent_data_id} was never pushed.',
                                        scoped_data_id, parent_data_id)
    elif _common.present(row_dict, 'Parent ID'):
        parent_id = _common.get(row_dict, 'Parent ID')
        parent_row_index = push_context.push_results.get_by_id(parent_id)
        if parent_row_index is None:
            raise SPyDependencyNotFound(f'Parent ID {parent_id} was never pushed.',
                                        _common.get(row_dict, 'ID'), parent_id)
    elif path and datasource_matches:
        parent_data_id = _reify_path(session, status, push_context, path)

    _cleanse_attributes(row_dict)

    if push_context.validate_ui_configs:
        _validate_ui_config(session, row_dict)

    def _set_properties_by_id():
        _row_sync_token = push_context.sync_token if _needs_sync_token(session, row_dict) else None
        _set_item_properties(session, row_dict, _row_sync_token)
        _set_existing_item_push_results(session, index, push_context.push_results, row_dict)

    try:
        if 'Signal' in row_dict['Type']:
            _process_signal(index, push_context, row_dict, scoped_data_id, session, status)
        elif 'Scalar' in row_dict['Type']:
            _process_scalar(index, push_context, row_dict, scoped_data_id, session, status, _set_properties_by_id)
        elif 'Condition' in row_dict['Type']:
            _process_condition(index, push_context, row_dict, scoped_data_id, session, status, _set_properties_by_id)
        elif row_dict['Type'] == 'Asset':
            _process_asset(index, push_context, row_dict, scoped_data_id, session, status, _set_properties_by_id)
        elif 'Chart' in row_dict['Type']:
            _process_chart(index, push_context, row_dict, session, status)
        elif 'Metric' in row_dict['Type']:
            _process_metric(index, push_context, row_dict, scoped_data_id, session, status)
        elif row_dict['Type'] == 'Display':
            _process_display(index, push_context, row_dict, scoped_data_id, session, status)
        elif 'Template' in row_dict['Type']:
            _process_display_template(index, push_context, row_dict, scoped_data_id, session, status)
    except SPyDependencyNotFound as e:
        e.dependent_identifier = scoped_data_id
        raise e

    if parent_data_id is not None:
        # Now we finally add a relationship for the leaf node to the most-recently-processed child asset
        tree_input = AssetTreeSingleInputV1()
        tree_input.parent_data_id = parent_data_id
        tree_input.child_data_id = scoped_data_id
        setattr(tree_input, 'dataframe_index', index)
        status.df['Relationship'] += _add_no_dupe(push_context.tree_batch_input.relationships, tree_input,
                                                  'child_data_id')
    elif _common.present(row_dict, 'ID') and _common.present(row_dict, 'Parent ID'):
        trees_api = TreesApi(session.client)
        trees_api.move_nodes_to_parent(
            parent_id=_common.get(row_dict, 'Parent ID'),
            body=ItemIdListInputV1(items=[_common.get(row_dict, 'ID')]))


def _process_display_template(index, push_context, row_dict, scoped_data_id, session, status):
    try:
        if _common.present(row_dict, 'Path') or _common.present(row_dict, 'Asset'):
            raise SPyRuntimeError('Display Template cannot have a path or asset.')
        _maybe_skip_item(session, push_context, scoped_data_id, row_dict)
        display_template_input = DisplayTemplateInputV1()
        dict_to_display_template_input(row_dict, display_template_input)
        if _common.present(row_dict, 'ID'):
            display_templates_api = DisplayTemplatesApi(session.client)
            display_templates_api.update_template(id=row_dict['ID'], body=display_template_input)
            _set_existing_item_push_results(session, index, push_context.push_results, row_dict)
        else:
            setattr(display_template_input, 'dataframe_index', index)
            push_context.display_template_inputs.append(display_template_input)
        status.df['Display Template'] += 1
    except (ApiException, SPyException) as e:
        _common.raise_or_catalog(status, df=push_context.push_results, column='Push Result',
                                 index=index, e=e)


def _process_display(index, push_context, row_dict, scoped_data_id, session, status):
    try:
        displays_api = DisplaysApi(session.client)
        display_templates_api = DisplayTemplatesApi(session.client)
        display_input = DisplayInputV1()

        if not _common.present(row_dict, 'Source Workstep ID') \
                and isinstance(_common.get(row_dict, 'Object'), AnalysisWorkstep):
            workstep_id = row_dict['Object'].id
            if ((push_context.workbook_context.item_map is not None) and (
                    workstep_id in push_context.workbook_context.item_map)):
                workstep_id = push_context.workbook_context.item_map[workstep_id]
            row_dict['Source Workstep ID'] = workstep_id

        if _common.present(row_dict, 'Template ID'):
            template_output = display_templates_api.get_display_template(id=row_dict['Template ID'])
            for attr in DisplayTemplateInputV1.attribute_map:
                prop = attr.replace('_', ' ').title().replace('Id', 'ID')

                def check_case(s):
                    return s.lower() if 'ID' in prop and s is not None else s

                if _common.present(row_dict, prop) \
                        and check_case(getattr(template_output, attr)) != check_case(row_dict[prop]):
                    raise SPyRuntimeError(f'{prop} of display must match that of its template')

        elif not _common.present(row_dict, 'ID'):
            if not _common.present(row_dict, 'Source Workstep ID'):
                raise SPyRuntimeError('Items of type Display require either a "Template ID" or "Source '
                                      'Workstep ID" property.')
            display_template_input = DisplayTemplateInputV1()
            dict_to_display_template_input(row_dict, display_template_input)
            display_templates_api = DisplayTemplatesApi(session.client)
            template_output = display_templates_api.create_display_template(body=display_template_input)
            row_dict['Template ID'] = template_output.id

        _maybe_skip_item(session, push_context, scoped_data_id, row_dict)

        display_input.template_id = _common.get(row_dict, 'Template ID')
        display_input.sync_token = push_context.sync_token
        display_input.data_id = scoped_data_id

        if _common.present(row_dict, 'ID'):
            display_output = displays_api.update_display(id=row_dict['ID'], body=display_input)
            template_output = display_output.template
            status.df['Display'] += 1
        else:
            # Set datasource attributes on input object so duplicate Data triplets can be checked for when flushing
            # noinspection PyUnboundLocalVariable
            setattr(display_input, 'datasource_class', template_output.datasource_class)
            setattr(display_input, 'datasource_id', template_output.datasource_id)
            setattr(display_input, 'dataframe_index', index)
            status.df['Display'] += _add_no_dupe(push_context.display_inputs, display_input)
        row_dict['Template ID'] = template_output.id

    except (ApiException, SPyException) as e:
        _common.raise_or_catalog(status, df=push_context.push_results, column='Push Result',
                                 index=index, e=e)


def _process_metric(index, push_context, row_dict, scoped_data_id, session, status):
    threshold_metric_input = ThresholdMetricInputV1()
    dict_to_threshold_metric_input(row_dict, threshold_metric_input)
    if _common.get(row_dict, 'Formula') == '<ThresholdMetric>':
        threshold_metric_input_from_formula_parameters(
            threshold_metric_input, _common.get(row_dict, 'Formula Parameters'), push_context)
    else:
        _set_threshold_levels_from_system(session, threshold_metric_input)
        if threshold_metric_input.measured_item is not None:
            threshold_metric_input.measured_item = _item_id_from_parameter_value(
                threshold_metric_input.measured_item, push_context)
        if threshold_metric_input.bounding_condition is not None:
            threshold_metric_input.bounding_condition = _item_id_from_parameter_value(
                threshold_metric_input.bounding_condition, push_context)

        threshold_metric_input.thresholds = _convert_thresholds_to_input(
            threshold_metric_input.thresholds, push_context, row_dict)

        if _common.present(row_dict, 'Statistic'):
            threshold_metric_input.aggregation_function = _common.statistic_to_aggregation_function(
                row_dict['Statistic'])
            push_context.push_results.at[
                index, 'Aggregation Function'] = threshold_metric_input.aggregation_function
    if threshold_metric_input.measured_item is not None:
        row_dict['Measured Item'] = threshold_metric_input.measured_item
    if threshold_metric_input.bounding_condition is not None:
        row_dict['Bounding Condition'] = threshold_metric_input.bounding_condition
    if threshold_metric_input.thresholds is not None:
        row_dict['Thresholds'] = threshold_metric_input.thresholds
    _maybe_skip_item(session, push_context, scoped_data_id, row_dict)
    threshold_metric_input.sync_token = push_context.sync_token
    if _common.present(row_dict, 'ID'):
        try:
            metrics_api = MetricsApi(session.client)
            threshold_metric_output = _put_threshold_metric(metrics_api, row_dict['ID'], threshold_metric_input)
            _push_special_properties(session, threshold_metric_input, threshold_metric_output)
            _set_existing_item_push_results(session, index, push_context.push_results, row_dict)
            status.df['Threshold Metric'] += 1
        except (ApiException, SPyException) as e:
            _common.raise_or_catalog(status, df=push_context.push_results, column='Push Result',
                                     index=index, e=e)
    else:
        threshold_metric_input.datasource_class = row_dict['Datasource Class']
        threshold_metric_input.datasource_id = row_dict['Datasource ID']
        threshold_metric_input.data_id = scoped_data_id
        setattr(threshold_metric_input, 'dataframe_index', index)
        status.df['Threshold Metric'] += _add_no_dupe(push_context.threshold_metric_inputs,
                                                      threshold_metric_input)
    row_dict['Push Result'] = 'Success'


def _process_chart(index, push_context, row_dict, session, status):
    items_api = ItemsApi(session.client)
    function_input = FunctionInputV1()
    dict_to_function_input(row_dict, function_input)
    function_input.formula = row_dict['Formula']
    function_input.parameters = list()
    for parameter_name, parameter_id in row_dict['Formula Parameters'].items():
        if isinstance(parameter_id, dict) or _common.is_guid(parameter_id):
            parameter_id = _item_id_from_parameter_value(parameter_id, push_context)
            function_input.parameters.append(FormulaParameterInputV1(name=parameter_name,
                                                                     id=parameter_id))
        else:
            function_input.parameters.append(FormulaParameterInputV1(name=parameter_name,
                                                                     formula=parameter_id,
                                                                     unbound=True))
    item_output = Item.find_item(session, row_dict.get('ID'), row_dict.get('Datasource Class'),
                                 row_dict.get('Datasource ID'), row_dict.get('Data ID'))
    formulas_api = FormulasApi(session.client)
    try:
        if item_output is None:
            calculated_item_output = formulas_api.create_function(
                body=function_input)  # type: CalculatedItemOutputV1

            items_api.set_properties(
                id=calculated_item_output.id,
                body=[ScalarPropertyV1(name='Datasource Class', value=row_dict['Datasource Class']),
                      ScalarPropertyV1(name='Datasource ID', value=row_dict['Datasource ID']),
                      ScalarPropertyV1(name='Data ID', value=function_input.data_id)])
        else:
            calculated_item_output = formulas_api.update_function(
                id=item_output.id, body=function_input)  # type: CalculatedItemOutputV1

        row_dict['ID'] = calculated_item_output.id
        _push_special_properties(session, function_input, calculated_item_output)
        _set_existing_item_push_results(session, index, push_context.push_results, row_dict)

    except (ApiException, SPyException) as e:
        _common.raise_or_catalog(status, df=push_context.push_results, column='Push Result',
                                 index=index, e=e)


def _process_asset(index, push_context, row_dict, scoped_data_id, session, status, _set_properties_by_id):
    _maybe_skip_item(session, push_context, scoped_data_id, row_dict)
    if _common.present(row_dict, 'ID'):
        status.df['Asset'] += 1
        safely(_set_properties_by_id,
               action_description=f'set common properties for Asset {row_dict["ID"]}',
               additional_errors=[400],
               status=status)
    else:
        asset_input = AssetInputV1()
        dict_to_asset_input(row_dict, asset_input)
        asset_input.data_id = scoped_data_id
        asset_input.sync_token = push_context.sync_token
        setattr(asset_input, 'dataframe_index', index)
        status.df['Asset'] += _add_no_dupe(push_context.asset_batch_input.assets, asset_input, overwrite=True)
        push_context.asset_batch_input.host_id = push_context.datasource_output.id
        if _common.present(row_dict, 'Path') and len(row_dict['Path']) == 0:
            push_context.roots[asset_input.data_id] = asset_input
    push_context.reified_assets.add(scoped_data_id)


def _process_capsule_property_units(row_dict: dict, condition_update_input: ConditionUpdateInputV1):
    if _common.present(row_dict, 'Formula'):
        return

    if _common.present(row_dict, SeeqNames.Properties.metadata_properties):
        metadata_properties = row_dict.get(SeeqNames.Properties.metadata_properties)
        if not isinstance(metadata_properties, str):
            raise SPyValueError(f'"{SeeqNames.Properties.metadata_properties}" must be a string')

        try:
            row_dict['Capsule Property Units'] = Item.decode_metadata_properties(metadata_properties)
        except Exception as e:
            raise SPyValueError(f'Invalid Metadata Properties string: {metadata_properties}\n{e}')

    if _common.present(row_dict, 'Capsule Property Units'):
        condition_update_input.capsule_properties = [CapsulePropertyInputV1(name, uom) for name, uom in
                                                     row_dict['Capsule Property Units'].items()]


def _process_condition(index, push_context, row_dict, scoped_data_id, session, status, _set_properties_by_id):
    condition_update_input = ConditionUpdateInputV1()
    dict_to_condition_update_input(row_dict, condition_update_input)
    condition_update_input.parameters = _process_formula_parameters(
        condition_update_input.parameters, push_context)
    row_dict['Formula Parameters'] = condition_update_input.parameters
    if condition_update_input.formula is None and condition_update_input.maximum_duration is None:
        raise SPyRuntimeError('"Maximum Duration" column required for stored conditions')
    _process_capsule_property_units(row_dict, condition_update_input)
    _maybe_skip_item(session, push_context, scoped_data_id, row_dict)
    if _common.present(row_dict, 'ID'):
        status.df['Condition'] += 1
        if _common.present(row_dict, 'Capsule Property Units'):
            status.warn("Updating condition's Capsule Property Units by ID is not supported yet.")
        safely(_set_properties_by_id,
               action_description=f'set common properties for Condition {row_dict["ID"]}',
               additional_errors=[400],
               status=status)
    else:
        condition_update_input.datasource_class = row_dict['Datasource Class']
        condition_update_input.datasource_id = row_dict['Datasource ID']
        condition_update_input.data_id = scoped_data_id
        condition_update_input.sync_token = push_context.sync_token
        setattr(condition_update_input, 'dataframe_index', index)
        status.df['Condition'] += _add_no_dupe(push_context.condition_batch_input.conditions,
                                               condition_update_input)


def _process_scalar(index, push_context, row_dict, scoped_data_id, session, status, _set_properties_by_id):
    scalar_input = ScalarInputV1()
    dict_to_scalar_input(row_dict, scalar_input)
    scalar_input.parameters = _process_formula_parameters(scalar_input.parameters, push_context)
    row_dict['Formula Parameters'] = scalar_input.parameters
    _maybe_skip_item(session, push_context, scoped_data_id, row_dict)
    if _common.present(row_dict, 'ID'):
        status.df['Scalar'] += 1
        safely(_set_properties_by_id,
               action_description=f'set common properties for Scalar {row_dict["ID"]}',
               additional_errors=[400],
               status=status)
    else:
        push_context.put_scalars_input.datasource_class = row_dict['Datasource Class']
        push_context.put_scalars_input.datasource_id = row_dict['Datasource ID']
        scalar_input.data_id = scoped_data_id
        scalar_input.sync_token = push_context.sync_token
        setattr(scalar_input, 'dataframe_index', index)
        status.df['Scalar'] += _add_no_dupe(push_context.put_scalars_input.scalars, scalar_input)

        # Since with scalars we have to put the Datasource Class and Datasource ID on the batch, we have to
        # recognize if it changed and, if so, flush the current batch.
        if push_context.last_scalar_datasource is not None and \
                push_context.last_scalar_datasource != (row_dict['Datasource Class'], row_dict['Datasource ID']):
            push_context.flush_now = True

        push_context.last_scalar_datasource = (row_dict['Datasource Class'], row_dict['Datasource ID'])


def _process_signal(index, push_context, row_dict, scoped_data_id, session, status):
    items_api = ItemsApi(session.client)
    signal_input = SignalInputV1() if _common.present(row_dict, 'ID') else SignalWithIdInputV1()
    dict_to_signal_input(row_dict, signal_input)
    signal_input.formula_parameters = _process_formula_parameters(signal_input.formula_parameters, push_context)
    row_dict['Formula Parameters'] = signal_input.formula_parameters
    _maybe_skip_item(session, push_context, scoped_data_id, row_dict)
    if signal_input.formula:
        # Calculated properties that must be None for Appserver to accept our input
        signal_input.maximum_interpolation = None
        signal_input.interpolation_method = None
        signal_input.key_unit_of_measure = None
        signal_input.value_unit_of_measure = None
    if _common.present(row_dict, 'ID'):
        status.df['Signal'] += 1
        if _needs_sync_token(session, row_dict):
            signal_input.sync_token = push_context.sync_token

        # Unfortunately we can't use the _set_item_properties(d) function like we can for Scalar and Condition
        # because we are not allowed to directly set the Value Unit Of Measure.
        try:
            signals_api = SignalsApi(session.client)
            try:
                signal_output = signals_api.put_signal(id=row_dict['ID'], body=signal_input)
            except ApiException as e:
                if SeeqNames.API.ErrorMessages.attempted_to_set_scope_on_a_globally_scoped_item in str(e):
                    # This handles CRAB-25450 by forcing global scope if we encounter the error.
                    signal_input.scoped_to = None
                    signal_output = signals_api.put_signal(id=row_dict['ID'],
                                                           body=signal_input)  # type: SignalOutputV1
                else:
                    raise

            _push_special_properties(session, signal_input, signal_output)

            # For some reason this is the only way to override Maximum Interpolation
            if _common.present(row_dict, SeeqNames.Properties.override_maximum_interpolation):
                items_api.set_property(id=row_dict['ID'],
                                       property_name=SeeqNames.Properties.override_maximum_interpolation,
                                       body=PropertyInputV1(
                                           value=row_dict[SeeqNames.Properties.override_maximum_interpolation]))

            # For some reason overriding Number Format doesn't follow the same pattern as for Maximum
            # Interpolation. But SPy will harmonize it so that it's the same.
            if _common.present(row_dict, 'Override ' + SeeqNames.Properties.number_format):
                items_api.set_property(id=row_dict['ID'],
                                       property_name=SeeqNames.Properties.number_format,
                                       body=PropertyInputV1(
                                           value=row_dict['Override ' + SeeqNames.Properties.number_format]))

            _set_existing_item_push_results(session, index, push_context.push_results,
                                            row_dict, signal_output)
        except ApiException as e:
            _common.raise_or_catalog(status, df=push_context.push_results, index=index,
                                     column='Push Result', e=e)
    else:
        signal_input.datasource_class = row_dict['Datasource Class']
        signal_input.datasource_id = row_dict['Datasource ID']
        signal_input.data_id = scoped_data_id
        signal_input.sync_token = push_context.sync_token
        setattr(signal_input, 'dataframe_index', index)
        status.df['Signal'] += _add_no_dupe(push_context.put_signals_input.signals, signal_input)


PUSH_DIRECTIVE_CREATE_ONLY = 'CreateOnly'
PUSH_DIRECTIVE_UPDATE_ONLY = 'UpdateOnly'
POSSIBLE_PUSH_DIRECTIVES = [
    PUSH_DIRECTIVE_CREATE_ONLY,
    PUSH_DIRECTIVE_UPDATE_ONLY
]


def _cleanse_attributes(d):
    for p, v in d.items():
        if (p in ['Cache Enabled', 'Archived', 'Enabled', 'Unsearchable', 'Asset Group Member']
                and not isinstance(v, bool)):
            # Ensure that these are booleans. Otherwise Seeq Server will silently ignore them.
            # noinspection PyUnresolvedReferences
            if isinstance(v, str):
                v = (v.lower() == 'true')
            elif np.isscalar(v) and not np.isnan(v):
                v = (v != 0)

        v = _cleanse_attr(_common.ensure_unicode(v))

        d[p] = v


def _validate_ui_config(session: Session, d):
    ui_config = _common.get(d, 'UIConfig')
    if isinstance(ui_config, str) and not session.options.wants_compatibility_with(189):
        try:
            # Make sure it's valid JSON, to give the user earlier warning that it's going to be a problem
            # See https://www.seeq.org/topic/2221-frustrating-problem-with-regression-formula-in-datalab/
            json.loads(ui_config)
        except json.JSONDecodeError:
            raise SPyValueError(f'UIConfig is not a valid JSON string:\n{ui_config}')


def _get_push_directives(row_dict):
    push_directives = _common.get(row_dict, 'Push Directives')
    if push_directives is None:
        return list()

    if isinstance(push_directives, str):
        push_directives = push_directives.split(';')

    if not isinstance(push_directives, list):
        raise SPyTypeError('Push Directives should be either a semi-colon-delimited string or a list of strings')

    push_directives = [d.strip() for d in push_directives]

    for d in push_directives:
        if d not in POSSIBLE_PUSH_DIRECTIVES:
            raise SPyValueError(
                f'Push Directive "{d}" not recognized. Possible values:\n' + '\n'.join(POSSIBLE_PUSH_DIRECTIVES))

    if PUSH_DIRECTIVE_CREATE_ONLY in push_directives and PUSH_DIRECTIVE_UPDATE_ONLY in push_directives:
        raise SPyValueError(f'Push Directives "{PUSH_DIRECTIVE_CREATE_ONLY}" and "{PUSH_DIRECTIVE_UPDATE_ONLY}" are '
                            f'mutually exclusive')

    return push_directives


TYPE_SIMPLIFIER_REGEX = re.compile(r'(Stored|Calculated|Threshold|Literal)')


def _maybe_skip_item(session: Session, push_context: PushContext, scoped_data_id: str, row_dict: dict,
                     raise_exception=True) -> bool:
    skip_it = False
    item_id = None
    previous_push_item = None
    push_result = None
    items_api = ItemsApi(session.client)

    if push_context.previous_results is not None:
        previous_index = push_context.previous_results.get_by_data_id(scoped_data_id)
        if previous_index is not None:
            previous_push_item = push_context.previous_results[previous_index]

    push_directives = _get_push_directives(row_dict)
    if PUSH_DIRECTIVE_CREATE_ONLY in push_directives or PUSH_DIRECTIVE_UPDATE_ONLY in push_directives:
        exists = False
        if previous_push_item is not None:
            item_id = _common.get(previous_push_item, 'ID')
            exists = True
        else:
            ds_filter = f'Data ID=={scoped_data_id}'
            if push_context.datasource_output.datasource_class != _common.INHERIT_FROM_WORKBOOK:
                ds_filter += f'&&Datasource Class=={push_context.datasource_output.datasource_class}' \
                             f'&&Datasource ID=={push_context.datasource_output.datasource_id}'

            search_results = items_api.search_items(filters=[ds_filter])
            if len(search_results.items) > 0:
                item_id = search_results.items[0].id
                exists = True

        if exists:
            if PUSH_DIRECTIVE_CREATE_ONLY in push_directives:
                push_result = f'Success: Skipped due to {PUSH_DIRECTIVE_CREATE_ONLY} push directive -- item already ' \
                              f'exists'
                skip_it = True
        else:
            if PUSH_DIRECTIVE_UPDATE_ONLY in push_directives:
                push_result = f'Success: Skipped due to {PUSH_DIRECTIVE_UPDATE_ONLY} push directive -- item does not ' \
                              f'exist'
                skip_it = True

    if not skip_it and previous_push_item is not None:
        if previous_push_item['Push Result'].startswith('Success'):
            different = False

            previous_type = _common.get(previous_push_item, 'Type')
            current_type = _common.get(row_dict, 'Type')
            if TYPE_SIMPLIFIER_REGEX.sub('', previous_type) != TYPE_SIMPLIFIER_REGEX.sub('', current_type):
                different = True

            if determine_path(row_dict) != determine_path(previous_push_item):
                different = True

            for k, v in row_dict.items():
                if k in [ORIGINAL_INDEX_COLUMN, 'Path', 'Asset', 'Type']:
                    # We took care of this specially a few lines above
                    continue

                if _common.present(row_dict, k) and (
                        not _common.present(previous_push_item, k) or previous_push_item[k] != v):
                    different = True
                    break

            if not different:
                push_result = 'Success: Unchanged'
                skip_it = True

    if skip_it:
        if item_id is not None:
            row_dict['ID'] = item_id

        if previous_push_item is not None:
            row_dict.update(previous_push_item)

        if push_result is not None:
            row_dict['Push Result'] = push_result

        if push_context.planning_to_archive:
            # We have to set the sync token so it doesn't get archived
            items_api.set_property(id=row_dict['ID'],
                                   property_name=SeeqNames.Properties.sync_token,
                                   body=PropertyInputV1(value=push_context.sync_token))

        if raise_exception:
            raise SkippedPush()
        else:
            return True


def _push_special_properties(session: Session, input_object, item):
    items_api = ItemsApi(session.client)
    if hasattr(input_object, '_ui_config'):
        items_api.set_property(id=item.id,
                               property_name='UIConfig',
                               body=PropertyInputV1(value=getattr(input_object, '_ui_config')))

    if hasattr(input_object, '_archive'):
        items_api.archive_item(id=item.id,
                               archived_reason='BY_USER',
                               note='Archived by user via SPy')


def _set_item_properties(session: Session, row_dict, sync_token):
    items_api = ItemsApi(session.client)

    do_not_exclude = [
        SeeqNames.Properties.name,
        SeeqNames.Properties.description,
        SeeqNames.Properties.interpolation_method,
        SeeqNames.Properties.value_uom,
        SeeqNames.Properties.uom,
        SeeqNames.Properties.number_format
    ]

    if _common.get(row_dict, 'Type') == 'StoredCondition':
        # We can't set this on calculated conditions but we can (and want to) on stored conditions
        do_not_exclude.append(SeeqNames.Properties.maximum_duration)

    excluded_properties = [p for p in IGNORED_PROPERTIES if p not in do_not_exclude]

    if _common.get(row_dict, 'Type') == 'LiteralScalar':
        # LiteralScalar doesn't allow updating value uom directly since it's updated when setting value
        excluded_properties.append(SeeqNames.Properties.value_uom)

    props = [
        ScalarPropertyV1(name=_name, value=_value) for _name, _value in row_dict.items()
        if _name not in excluded_properties and (isinstance(_value, list) or not pd.isna(_value))
    ]
    if sync_token:
        props.append(ScalarPropertyV1(name=SeeqNames.Properties.sync_token, value=sync_token))

    item_output = items_api.set_properties(id=row_dict['ID'], body=props)

    if item_output.scoped_to is not None and 'Scoped To' in row_dict:
        # This handles CRAB-25450 by only attempting to set scope if the item is not already globally scoped
        scoped_to = _common.get(row_dict, 'Scoped To')
        if scoped_to is not None:
            items_api.set_scope(id=row_dict['ID'], workbook_id=scoped_to)
        else:
            items_api.set_scope(id=row_dict['ID'])

    if _common.present(row_dict, 'Formula'):
        items_api.set_formula(id=row_dict['ID'], body=FormulaUpdateInputV1(
            formula=row_dict['Formula'], parameters=row_dict['Formula Parameters']))


def _needs_sync_token(session: Session, d):
    """
    The sync token allows us to clean up (i.e., archive) items in the
    datasource that have been pushed previously but are no longer desired.
    However, there is a use case where the user pull items that belong to
    an external datasource (e.g., OSIsoft PI), makes a property change
    (like "Maximum Interpolation"), and then pushes them back. In such a
    case, we do not want to modify the sync token because it will have
    adverse effects on the indexing operation by the corresponding
    connector. So we check first to see if this item was pushed from Data
    Lab originally.
    """
    return not _common.present(d, 'ID') or _item_is_from_datalab(session, d['ID'])


def _item_is_from_datalab(session: Session, item_id):
    items_api = ItemsApi(session.client)
    try:
        datasource_class = items_api.get_property(id=item_id, property_name=SeeqNames.Properties.datasource_class)
        return datasource_class and datasource_class.value == _common.DEFAULT_DATASOURCE_CLASS
    except ApiException:
        return False


def determine_path(d):
    return _common.path_list_to_string(determine_path_list(d))


def determine_path_list(d):
    path = list()
    if _common.present(d, 'Path'):
        path.extend(_common.path_string_to_list(_common.get(d, 'Path')))

    _type = _common.get(d, 'Type')

    if _type != 'Asset' and _common.present(d, 'Asset'):
        path.append(_common.get(d, 'Asset'))

    return path


def get_scoped_data_id(d: dict, workbook_id: Optional[str], cleanse_data_ids: bool = True) -> str:
    """
    :param d: The dictionary representing this item.
    :param workbook_id: The ID of the workbook that the item is scoped to. None or EMPTY_GUID represents global.
    :param cleanse_data_ids: Whether to cleanse the Data ID to ensure it is unique to the workbook.
    :return: The generated Data ID representing this item. This is generally in the form of
             "[workbook_id] {item_type} path >> to >> item".
    """
    path = determine_path(d)

    if not _common.present(d, 'Data ID'):
        if path:
            scoped_data_id = '%s >> %s' % (path, d['Name'])
        else:
            scoped_data_id = d['Name']
    else:
        scoped_data_id = d['Data ID']

    if cleanse_data_ids and not _is_scoped_data_id(scoped_data_id):
        if not _common.present(d, 'Type'):
            raise SPyRuntimeError('Type is required for all item definitions')

        guid = workbook_id if workbook_id else EMPTY_GUID

        _type = _common.simplify_type(d['Type'])
        if 'Metric' in _type:
            _type = 'ThresholdMetric'

        # Need to scope the Data ID to the workbook so it doesn't collide with other workbooks
        scoped_data_id = '[%s] {%s} %s' % (guid, _type, str(scoped_data_id))

    return scoped_data_id.strip()


def _is_scoped_data_id(data_id):
    return re.match(r'^\[%s] \{\w+}.*' % _common.GUID_REGEX, data_id) is not None


def _get_unscoped_data_id(scoped_data_id):
    return re.sub(r'^\[%s] \{\w+}\s*' % _common.GUID_REGEX, '', scoped_data_id)


def _cleanse_attr(v):
    if isinstance(v, np.generic):
        # Swagger can't handle NumPy types, so we have to retrieve an underlying Python type
        return v.item()
    else:
        return v


def dict_to_input(d, _input, properties_attr, attr_map, capsule_property_units=None):
    lower_case_known_attrs = {k.lower(): k for k in attr_map.keys()}
    for k, v in d.items():
        if k.lower() in lower_case_known_attrs and k not in attr_map:
            raise SPyRuntimeError(f'Incorrect case used for known property: "{k}" should be '
                                  f'"{lower_case_known_attrs[k.lower()]}"')

        if k in attr_map:
            if attr_map[k] is not None:
                v = _common.get(d, k)
                if isinstance(v, (list, pd.DataFrame, pd.Series)) or not pd.isna(v):
                    setattr(_input, attr_map[k], _cleanse_attr(v))
        elif properties_attr is not None:
            p = ScalarPropertyV1()
            p.name = _common.ensure_unicode(k)

            if p.name in IGNORED_PROPERTIES:
                continue

            uom = None
            if capsule_property_units is not None:
                uom = _common.get(capsule_property_units, p.name.lower())
                if isinstance(v, dict) and uom is not None:
                    raise SPyTypeError(f'Property "{p.name}" cannot have type dict when unit of measure is specified '
                                       f'in metadata')
            if isinstance(v, dict):
                uom = _common.get(v, 'Unit Of Measure')
                v = _common.get(v, 'Value')
            else:
                v = _common.get(d, k)

            if not pd.isna(v):
                p.value = v

                if uom is not None:
                    p.unit_of_measure = _common.ensure_unicode(uom)

                if p.name == 'Archived' and v:
                    setattr(_input, '_archive', True)
                else:
                    _properties = getattr(_input, properties_attr)
                    if _properties is None:
                        _properties = list()
                    _properties.append(p)
                    setattr(_input, properties_attr, _properties)

    if _common.present(d, 'UIConfig'):
        ui_config = _common.get(d, 'UIConfig')
        if isinstance(ui_config, dict):
            ui_config = json.dumps(ui_config)
        setattr(_input, '_ui_config', ui_config)


def _set_threshold_levels_from_system(session: Session, threshold_input: ThresholdMetricInputV1):
    """
    Read the threshold limits from the systems endpoint and update the values in the threshold limits. Allows users
    to set thresholds as those defined in the system endpoint such as 'Lo', 'LoLo', 'Hi', 'HiHi', etc.

    :param threshold_input: A Threshold Metric input with a dict in the thresholds with keys of the priority level and
    values of the threshold. Keys are either a numeric value of the threshold, or strings contained in the
    systems/configuration. Values are either scalars or metadata dataframes. If a key is a string that maps to a number
    that is already used in the limits, a RuntimeError will be raised.
    :return: The threshold input with a limits dict with the string values replaced with numbers.
    """
    if not isinstance(threshold_input.thresholds, dict):
        return

    # noinspection PyTypeChecker
    thresholds = threshold_input.thresholds  # type: dict
    threshold_input.thresholds = convert_threshold_levels_from_system(session, thresholds, threshold_input.name)


def convert_threshold_levels_from_system(session: Session, thresholds: dict, item_name) -> dict:
    system_api = SystemApi(session.client)

    # get the priority names and their corresponding levels
    system_settings = system_api.get_server_status()  # type: ServerStatusOutputV1
    priority_levels = {p.name: p.level for p in system_settings.priorities if p.name != 'Neutral'}
    updated_threshold_limits = dict()

    def get_numeric_threshold(threshold):
        # Returns an int representing a threshold priority level
        if isinstance(threshold, int):
            return threshold
        elif isinstance(threshold, str):
            threshold = threshold.split('#')[0].strip()
            if threshold in priority_levels:
                return priority_levels[threshold]
            else:
                try:
                    if int(threshold) in priority_levels.values():
                        return int(threshold)
                    else:
                        raise ValueError
                except ValueError:
                    raise SPyRuntimeError(f'The threshold {threshold} for metric {item_name} is not a valid '
                                          f'threshold level. Valid threshold levels: {list(priority_levels)}')
        else:
            raise SPyRuntimeError(f'The threshold {threshold} is of invalid type {type(threshold)}')

    def get_color_code(threshold):
        # Extracts and returns the color code from a threshold if it exists
        if isinstance(threshold, str):
            parts = threshold.split('#')
            if len(parts) == 2:
                code = parts[1].strip()
                if not re.match(r'^[0-9a-fA-F]{6}$', code):
                    raise SPyRuntimeError(f'"#{code}" is not a valid color hex code')
                return code.lower()
            elif len(parts) > 2:
                raise SPyRuntimeError(f'Threshold "{k}" contains unknown formatting')
        return None

    for k, v in thresholds.items():
        numeric = get_numeric_threshold(k)
        color_code = get_color_code(k)

        if numeric in [get_numeric_threshold(threshold) for threshold in updated_threshold_limits]:
            raise SPyRuntimeError(
                f'Threshold "{k}" maps to a duplicate threshold value for metric {item_name}')

        updated_threshold = '#'.join([str(numeric), color_code]) if color_code is not None else str(numeric)
        updated_threshold_limits[updated_threshold] = v

    return updated_threshold_limits


def dict_to_datasource_input(d, datasource_input):
    dict_to_input(d, datasource_input, None, {
        'Name': 'name',
        'Description': 'description',
        'Datasource Name': 'name',
        'Datasource Class': 'datasource_class',
        'Datasource ID': 'datasource_id'
    })


def dict_to_asset_input(d, asset_input):
    dict_to_input(d, asset_input, 'properties', {
        'Type': None,
        'Name': 'name',
        'Description': 'description',
        'Datasource Class': 'datasource_class',
        'Datasource ID': 'datasource_id',
        'Data ID': 'data_id',
        'Scoped To': 'scoped_to'
    })


def dict_to_signal_input(d, signal_input):
    dict_to_input(d, signal_input, 'additional_properties', {
        'Type': None,
        'Cache ID': None,
        'Name': 'name',
        'Description': 'description',
        'Datasource Class': 'datasource_class',
        'Datasource ID': 'datasource_id',
        'Data ID': 'data_id',
        'Formula': 'formula',
        'Formula Parameters': 'formula_parameters',
        'Interpolation Method': 'interpolation_method',
        'Maximum Interpolation': 'maximum_interpolation',
        'Scoped To': 'scoped_to',
        'Key Unit Of Measure': 'key_unit_of_measure',
        'Value Unit Of Measure': 'value_unit_of_measure',
        'Number Format': 'number_format'
    })


def dict_to_scalar_input(d, scalar_input):
    dict_to_input(d, scalar_input, 'properties', {
        'Type': None,
        'Name': 'name',
        'Description': 'description',
        'Datasource Class': 'datasource_class',
        'Datasource ID': 'datasource_id',
        'Data ID': 'data_id',
        'Formula': 'formula',
        'Formula Parameters': 'parameters',
        'Scoped To': 'scoped_to',
        'Number Format': 'number_format'
    })


def dict_to_condition_input(d, signal_input):
    if _common.present(d, 'Formula'):
        dict_to_input(d, signal_input, 'properties', {
            'Type': None,
            'Cache ID': None,
            'Name': 'name',
            'Description': 'description',
            'Datasource Class': 'datasource_class',
            'Datasource ID': 'datasource_id',
            'Data ID': 'data_id',
            'Formula': 'formula',
            'Formula Parameters': 'parameters',
            'Scoped To': 'scoped_to'
        })
    else:
        dict_to_input(d, signal_input, 'properties', {
            'Type': None,
            'Cache ID': None,
            'Name': 'name',
            'Description': 'description',
            'Datasource Class': 'datasource_class',
            'Datasource ID': 'datasource_id',
            'Data ID': 'data_id',
            'Maximum Duration': 'maximum_duration',
            'Scoped To': 'scoped_to'
        })


def dict_to_condition_update_input(d, condition_update_input: ConditionUpdateInputV1):
    dict_to_condition_input(d, condition_update_input)
    if condition_update_input.formula is not None:
        if 'Replace Capsule Properties' in d.keys() and d['Replace Capsule Properties'] is not True:
            raise SPyRuntimeError('"Replace Capsule Properties" must be True for calculated conditions')
        else:
            condition_update_input.replace_capsule_properties = True


def dict_to_function_input(d, function_input: FunctionInputV1):
    dict_to_input(d, function_input, 'additional_properties', {
        'Type': 'type',
        'Cache ID': None,
        'Name': 'name',
        'Description': 'description',
        'Datasource Class': 'datasource_class',
        'Datasource ID': 'datasource_id',
        'Data ID': 'data_id',
        'Scoped To': 'scoped_to'
    })


def dict_to_capsule(d, capsule, capsule_property_units=None):
    dict_to_input(d, capsule, 'properties', {
        'Capsule Start': None,
        'Capsule End': None
    }, capsule_property_units=capsule_property_units)


def dict_to_threshold_metric_input(d, metric_input):
    dict_to_input(d, metric_input, 'additional_properties', {
        'Type': None,
        'Name': 'name',
        'Duration': 'duration',
        'Bounding Condition Maximum Duration': 'bounding_condition_maximum_duration',
        'Period': 'period',
        'Thresholds': 'thresholds',
        'Measured Item': 'measured_item',
        'Number Format': 'number_format',
        'Bounding Condition': 'bounding_condition',
        'Metric Neutral Color': 'neutral_color',
        'Scoped To': 'scoped_to',
        'Aggregation Function': 'aggregation_function',
        'Aggregation Condition': None
    })


def dict_to_display_template_input(d, display_template_input):
    dict_to_input(d, display_template_input, None, {
        'Datasource Class': 'datasource_class',
        'Datasource ID': 'datasource_id',
        'Name': 'name',
        'Swap Source Asset ID': 'swap_source_asset_id',
        'Scoped To': 'scoped_to',
        'Description': 'description',
        'Source Workstep ID': 'source_workstep_id'
    })


def _handle_reference_uom(session: Session, definition, key):
    if not _common.present(definition, key):
        return

    unit = definition[key]
    if _login.is_valid_unit(session, unit):
        if unit != 'string':
            definition['Formula'] += f".setUnits('{unit}')"
        else:
            definition['Formula'] += f".toString()"
    else:
        # This is the canonical place for unrecognized units
        definition[f'Source {key}'] = unit

    del definition[key]


def _build_reference_signal(session: Session, definition):
    definition['Type'] = 'CalculatedSignal'
    definition['Formula'] = '$signal'

    if _common.present(definition, 'Interpolation Method'):
        definition['Formula'] += f".to{definition['Interpolation Method']}()"
        del definition['Interpolation Method']

    _handle_reference_uom(session, definition, 'Value Unit Of Measure')

    definition['Formula Parameters'] = 'signal=%s' % definition['ID']
    definition['Cache Enabled'] = False

    for key in ['ID', 'Datasource Class', 'Datasource ID', 'Data ID']:
        if _common.present(definition, key) and not _common.present(definition, 'Referenced ' + key):
            definition['Referenced ' + key] = definition[key]
            del definition[key]


def _build_reference_condition(session: Session, definition):
    definition['Type'] = 'CalculatedCondition'
    definition['Formula'] = '$condition'
    definition['Formula Parameters'] = 'condition=%s' % definition['ID']
    definition['Cache Enabled'] = False

    for key in ['ID', 'Datasource Class', 'Datasource ID', 'Data ID', 'Unit Of Measure', 'Maximum Duration']:
        if _common.present(definition, key) and not _common.present(definition, 'Referenced ' + key):
            definition['Referenced ' + key] = definition[key]
            del definition[key]


def _build_reference_scalar(session: Session, definition):
    definition['Type'] = 'CalculatedScalar'
    definition['Formula'] = '$scalar'
    definition['Formula Parameters'] = 'scalar=%s' % definition['ID']
    definition['Cache Enabled'] = False

    _handle_reference_uom(session, definition, 'Unit Of Measure')

    for key in ['ID', 'Datasource Class', 'Datasource ID', 'Data ID']:
        if _common.present(definition, key) and not _common.present(definition, 'Referenced ' + key):
            definition['Referenced ' + key] = definition[key]
            del definition[key]


def build_reference(session: Session, definition):
    {
        'StoredSignal': _build_reference_signal,
        'CalculatedSignal': _build_reference_signal,
        'StoredCondition': _build_reference_condition,
        'CalculatedCondition': _build_reference_condition,
        'LiteralScalar': _build_reference_scalar,
        'CalculatedScalar': _build_reference_scalar
    }[definition['Type']](session, definition)


def _process_formula_parameters(parameters, push_context: PushContext):
    workbook_id = push_context.workbook_context.workbook_id
    if parameters is None:
        return list()

    if isinstance(parameters, str):
        parameters = [parameters]

    if isinstance(parameters, dict):
        pairs = parameters.items()

    elif isinstance(parameters, list):
        pairs = []
        for param_entry in parameters:
            if not isinstance(param_entry, str):
                raise SPyValueError(f'Formula Parameter entry {param_entry} has invalid type. Must be string.')
            try:
                k, v = param_entry.split('=')
                pairs.append((k, v))
            except ValueError:
                raise SPyValueError(
                    f'Formula Parameter entry "{param_entry}" not recognized. Must be "var=ID" or "var=Path".')

    else:
        raise SPyValueError(f'Formula Parameters have invalid type {type(parameters)}. Valid types are str, list, '
                            f'and dict.')

    processed_parameters = list()
    for k, v in pairs:
        # Strip off leading dollar-sign if it's there
        parameter_name = re.sub(r'^\$', '', k)
        try:
            parameter_id = _item_id_from_parameter_value(v, push_context)
        except (ValueError, TypeError) as e:
            raise SPyRuntimeError(f'Error processing {parameter_name}: {e}')
        processed_parameters.append(f'{parameter_name}={parameter_id}')

    processed_parameters.sort(key=lambda param: param.split('=')[0])
    return processed_parameters


def _item_id_from_parameter_value(dict_value, push_context: PushContext):
    workbook_id = push_context.workbook_context.workbook_id
    push_results = push_context.push_results

    if isinstance(dict_value, pd.DataFrame):
        if len(dict_value) == 0:
            raise SPyValueError('The parameter had an empty dataframe')
        if len(dict_value) > 1:
            raise SPyValueError('The parameter had multiple entries in the dataframe')
        dict_value = dict_value.iloc[0]

    def find_id_matching_path(full_path):
        matching_row = push_results.get_by_workbook_and_path((workbook_id if workbook_id else EMPTY_GUID), full_path)
        if matching_row is not None and _common.present(push_results[matching_row], 'ID'):
            return push_results[matching_row]['ID']
        else:
            raise SPyDependencyNotFound(f'Item "{full_path}" was never pushed (error code 4)')

    if isinstance(dict_value, (dict, pd.Series)):
        if _common.present(dict_value, 'ID') and not _common.get(dict_value, 'Reference', default=False):
            return dict_value['ID']
        elif not _common.present(dict_value, 'Type') and _common.present(dict_value, 'Name'):
            path = _common.path_list_to_string(determine_path_list(dict_value) + [dict_value['Name']])
            return find_id_matching_path(path)
        else:
            try:
                scoped_data_id = get_scoped_data_id(dict_value, workbook_id, push_context.cleanse_data_ids)
            except SPyRuntimeError:
                # This can happen if the dependency didn't get pushed and therefore doesn't have a proper Type
                raise SPyDependencyNotFound(f'Item {dict_value} was never pushed (error code 1)')

            pushed_row_i_need = push_results.get_by_data_id(scoped_data_id)
            if pushed_row_i_need is not None and _common.present(push_results[pushed_row_i_need], 'ID'):
                return push_results[pushed_row_i_need]['ID']
            else:
                raise SPyDependencyNotFound(f'Item {scoped_data_id} was never pushed (error code 2)',
                                            None, scoped_data_id)

    elif isinstance(dict_value, str):
        if _common.is_guid(dict_value):
            return dict_value
        # Now treat string like a path
        path = _common.sanitize_path_string(dict_value)
        return find_id_matching_path(path)
    elif dict_value is None:
        raise SPyTypeError('A formula parameter is None, which is not allowed. Check your logic for assigning formula '
                           'parameters and, if you\'re using spy.assets.build(), look for optional Requirements that '
                           'are were not found.')
    else:
        raise SPyTypeError(f'Formula parameter type "{type(dict_value)}" not allowed. Must be DataFrame, Series, '
                           f'dict or ID string')


def _set_push_result_string__from_post_loop(dfi, iuo, errors, push_results: PushResults):
    result_string = 'Success'
    non_batch_item_types = [ThresholdMetricOutputV1, DisplayOutputV1, DisplayTemplateOutputV1]
    values = dict()
    if isinstance(iuo, ItemUpdateOutputV1):
        if iuo.error_message is not None:
            if errors == 'raise':
                raise SPyRuntimeError('Error pushing "%s": %s' % (iuo.data_id, iuo.error_message))
            result_string = iuo.error_message
        else:
            values['Datasource Class'] = iuo.datasource_class
            values['Datasource ID'] = iuo.datasource_id
            values['Data ID'] = iuo.data_id
            values['ID'] = iuo.item.id
            values['Type'] = iuo.item.type
    elif any(isinstance(iuo, _type) for _type in non_batch_item_types):
        values['Datasource Class'] = getattr(iuo, 'datasource_class', np.nan)
        values['Datasource ID'] = getattr(iuo, 'datasource_id', np.nan)
        values['Data ID'] = getattr(iuo, 'data_id', np.nan)
        values['ID'] = iuo.id
        values['Type'] = iuo.type
    elif isinstance(iuo, str):
        if errors == 'raise':
            raise SPyRuntimeError('Error pushing "%s": %s' % (dfi, iuo))
        result_string = iuo
    else:
        raise SPyTypeError('Unrecognized output type from API: %s' % type(iuo))

    push_results.add_response(_set_push_result_string__from_main_loop, (dfi, values, result_string, push_results))


def _set_push_result_string__from_main_loop(dfi, values: dict, result_string: str, push_results: PushResults):
    row = push_results.loc[dfi]

    if not _common.present(row, 'Push Result') or row['Push Result'] == 'Success':
        values['Push Result'] = result_string

    row.update(values)


def _set_existing_item_push_results(session: Session, index, push_results: PushResults, item_dict, output_object=None):
    if output_object is None:
        if 'Signal' in item_dict['Type']:
            signals_api = SignalsApi(session.client)
            output_object = signals_api.get_signal(id=item_dict['ID'])
        elif 'Scalar' in item_dict['Type']:
            scalars_api = ScalarsApi(session.client)
            output_object = scalars_api.get_scalar(id=item_dict['ID'])
        elif 'Condition' in item_dict['Type']:
            conditions_api = ConditionsApi(session.client)
            output_object = conditions_api.get_condition(id=item_dict['ID'])
        elif 'Chart' in item_dict['Type']:
            formulas_api = FormulasApi(session.client)
            output_object = formulas_api.get_function(id=item_dict['ID'])
        elif 'Metric' in item_dict['Type']:
            metrics_api = MetricsApi(session.client)
            output_object = metrics_api.get_metric(id=item_dict['ID'])
        elif 'Asset' in item_dict['Type']:
            assets_api = AssetsApi(session.client)
            output_object = assets_api.get_asset(id=item_dict['ID'])
        elif item_dict['Type'] == 'Display':
            displays_api = DisplaysApi(session.client)
            output_object = displays_api.get_display(id=item_dict['ID'])
        elif 'Template' in item_dict['Type']:
            display_templates_api = DisplayTemplatesApi(session.client)
            output_object = display_templates_api.get_display_template(id=item_dict['ID'])

    push_item = push_results.loc[index]
    for p in ['ID', 'Type', 'Data ID', 'Datasource Class', 'Datasource ID']:
        attr = p.lower().replace(' ', '_')
        if hasattr(output_object, attr):
            push_item[p] = getattr(output_object, attr)
    push_item['Push Result'] = 'Success'


def _process_batch_output(session: Session, item_inputs, item_updates, status: Status, push_results: PushResults):
    repost = False
    for i in range(0, len(item_inputs)):
        item_input = item_inputs[i]
        item_update_output = item_updates[i]  # type: ItemUpdateOutputV1

        if (item_update_output.error_message and
                (SeeqNames.API.ErrorMessages.attempted_to_set_scope_on_a_globally_scoped_item in
                 item_update_output.error_message)):
            # This handles CRAB-25450. Metadata that was posted prior to that bugfix may have a non-fixable
            # global-scope applied, so rather than error out, just repost with global-scope. This effectively
            # preserves status quo for those users.
            setattr(item_input, 'scoped_to', None)
            repost = True
            continue

        if item_update_output.item is not None:
            _push_special_properties(session, item_input, item_update_output.item)

        if hasattr(item_input, 'dataframe_index'):
            _set_push_result_string__from_post_loop(item_input.dataframe_index, item_update_output, status.errors,
                                                    push_results)

    return repost


def _post_batch_async(session: Session, post_function, item_inputs, status: Status, push_results: PushResults):
    push_results.flush_section_count += 1
    push_results.add_post(_post_batch, (session, post_function, item_inputs, status, push_results))


def _post_batch(session: Session, post_function, item_inputs, status: Status, push_results: PushResults):
    if len(item_inputs) == 0:
        return

    def _post_it():
        try:
            return post_function()
        except Exception as e:
            for item_input in item_inputs:
                if hasattr(item_input, 'dataframe_index'):
                    _set_push_result_string__from_post_loop(
                        item_input.dataframe_index, _common.format_exception(e), status.errors, push_results)
            return None

    item_batch_output = _post_it()

    if item_batch_output is None:
        return

    repost = _process_batch_output(session, item_inputs, item_batch_output.item_updates, status, push_results)
    if repost:
        # This means we're supposed to repost the batch because _process_batch_output() has modified the scoped_to
        # property on some items to overcome CRAB-25450. See test_metadata.test_crab_25450().
        item_batch_output = _post_it()
        _process_batch_output(session, item_inputs, item_batch_output.item_updates, status, push_results)


def call_metric_api_with_retry(func, body):
    while True:
        try:
            return func()
        except ApiException as e:
            # We have to handle a case where a condition on which a metric depends has been changed from bounded
            # to unbounded. In the UI, it automatically fills in the default of 40h when you edit such a metric,
            # so we do roughly the same thing here. This is tested by test_push.test_bad_metric().
            exception_text = _common.format_exception(e)
            if 'Maximum Capsule Duration for Bounding Condition must be provided' in exception_text:
                body.bounding_condition_maximum_duration = '40h'
            else:
                raise


def _put_threshold_metric(metrics_api, metric_id, body: ThresholdMetricInputV1):
    return call_metric_api_with_retry(lambda: metrics_api.put_threshold_metric(id=metric_id, body=body), body)


def _flush(session: Session, status: Status, push_context: PushContext):
    signals_api = SignalsApi(session.client)
    scalars_api = ScalarsApi(session.client)
    conditions_api = ConditionsApi(session.client)
    assets_api = AssetsApi(session.client)
    trees_api = TreesApi(session.client)
    metrics_api = MetricsApi(session.client)
    displays_api = DisplaysApi(session.client)
    display_templates_api = DisplayTemplatesApi(session.client)

    push_context.push_results.flush_section_count = 0

    push_context.push_results.drain_responses()

    signals_body = push_context.put_signals_input
    _post_batch_async(session, lambda: signals_api.put_signals(body=signals_body), signals_body.signals, status,
                      push_context.push_results)

    scalars_body = push_context.put_scalars_input
    _post_batch_async(session, lambda: scalars_api.put_scalars(body=scalars_body), scalars_body.scalars, status,
                      push_context.push_results)

    conditions_body = push_context.condition_batch_input
    _post_batch_async(session, lambda: conditions_api.put_conditions(body=conditions_body), conditions_body.conditions,
                      status, push_context.push_results)

    def _create_threshold_metric(body):
        return call_metric_api_with_retry(lambda: metrics_api.create_threshold_metric(body=body), body)

    def _handle_metric_conflict(tm_input, search_result):
        tm_output = metrics_api.get_metric(id=search_result.id)
        if tm_output.scoped_to is None and tm_input.scoped_to is not None:
            # This handles CRAB-25450
            tm_input.scoped_to = None

        # Workaround for CRAB-29202: Explicitly un-archive the metric using Additional Properties
        if tm_input.additional_properties is None:
            tm_input.additional_properties = list()
        if all((prop.name != 'Archived' for prop in tm_input.additional_properties)):
            tm_input.additional_properties.append(ScalarPropertyV1(name='Archived', value=False))

        return _put_threshold_metric(metrics_api, search_result.id, tm_input)

    _polyfill_post_batch_async(session, push_context.threshold_metric_inputs, _create_threshold_metric,
                               _handle_metric_conflict, push_context.push_results, status)

    _polyfill_post_batch_async(session, push_context.display_template_inputs,
                               display_templates_api.create_display_template,
                               None, push_context.push_results, status)

    def _handle_display_conflict(display_input, search_result):
        return displays_api.update_display(id=search_result.id, body=display_input)

    _polyfill_post_batch_async(session, push_context.display_inputs, displays_api.create_display,
                               _handle_display_conflict,
                               push_context.push_results, status)

    assets_body = push_context.asset_batch_input
    _post_batch_async(session, lambda: assets_api.batch_create_assets(body=assets_body), assets_body.assets, status,
                      push_context.push_results)

    tree_body = push_context.tree_batch_input
    _post_batch_async(session, lambda: trees_api.batch_move_nodes_to_parents(body=tree_body), tree_body.relationships,
                      status, push_context.push_results)

    if session.options.force_calculated_scalars and _compatibility.is_force_calculated_scalars_available():
        push_context.put_scalars_input = PutScalarsInputV1(scalars=list(), force_calculated_scalars=True)
    else:
        push_context.put_scalars_input = PutScalarsInputV1(scalars=list())
    push_context.put_signals_input = PutSignalsInputV1(signals=list())
    push_context.condition_batch_input = ConditionBatchInputV1(conditions=list())
    push_context.asset_batch_input = AssetBatchInputV1(assets=list())
    push_context.tree_batch_input = AssetTreeBatchInputV1(relationships=list(),
                                                          parent_host_id=push_context.datasource_output.id,
                                                          child_host_id=push_context.datasource_output.id)

    if push_context.push_results.flush_section_count != PushResults.POST_QUEUE_SIZE:
        # Why does it need to be equal? Because we want the queue to block on the next flush call (if necessary) so that
        # we aren't stacking up a bunch of batches and taking up more memory than we need to. This exception would
        # never occur in production, it would only occur if a dev makes a change and will therefore be caught by
        # system tests.
        raise Exception('The number of "sections" in the _flush() function [the sum of calls to _post_batch_async() '
                        'and _polyfill_post_batch_async()] must equal PushResults.POST_QUEUE_SIZE. It currently does '
                        'not. If this is expected (because, for example, you added a new item type that gets pushed), '
                        'then just change PushResults.POST_QUEUE_SIZE to be '
                        f'{push_context.push_results.flush_section_count} instead of {PushResults.POST_QUEUE_SIZE}')


def _polyfill_post_batch_async(session: Session, item_inputs, create_item_endpoint, action_on_data_triplet_match,
                               push_results: PushResults, status: Status):
    push_results.flush_section_count += 1
    push_results.add_post(_polyfill_post_batch,
                          (session, item_inputs, create_item_endpoint, action_on_data_triplet_match,
                           push_results, status))


def _polyfill_post_batch(session: Session, item_inputs, create_item_endpoint, action_on_data_triplet_match,
                         push_results: PushResults, status: Status):
    """
    Used for pushing items types that don't have batch POST endpoints
    """
    items_api = ItemsApi(session.client)
    for item_input in item_inputs:
        try:
            if action_on_data_triplet_match is not None:
                # Check if the item already exists
                search_query = f'Datasource Class == {item_input.datasource_class}' \
                               f' && Datasource ID == {item_input.datasource_id}' \
                               f' && Data ID == {item_input.data_id}'
                item_results = items_api.search_items(
                    filters=[search_query, '@includeUnsearchable']).items
            else:
                item_results = list()
            if len(item_results) > 1:
                raise SPyRuntimeError(f'More than one item had the data triplet '
                                      f'({item_input.datasource_class}, {item_input.datasource_id}, {item_input.data_id}): '
                                      f'{", ".join(item_result.id for item_result in item_results)}')
            if len(item_results) == 1:
                item_push_output = action_on_data_triplet_match(item_input, item_results[0])
            else:
                item_push_output = create_item_endpoint(body=item_input)
        except Exception as e:
            item_push_output = _common.format_exception(e)

        _set_push_result_string__from_post_loop(item_input.dataframe_index, item_push_output, status.errors,
                                                push_results)

    item_inputs.clear()


def _add_no_dupe(lst, obj, attr='data_id', overwrite=False):
    for i in range(0, len(lst)):
        o = lst[i]
        if hasattr(o, attr):
            if getattr(o, attr) == getattr(obj, attr):
                if overwrite:
                    lst[i] = obj
                return 0

    lst.append(obj)
    return 1


def _is_handled_type(type_value):
    try:
        return ('Signal' in type_value
                or 'Scalar' in type_value
                or 'Condition' in type_value
                or 'Chart' in type_value
                or 'Metric' in type_value
                or 'Template' in type_value
                or type_value == 'Display'
                or type_value == 'Asset')
    except TypeError:
        return False


def _reify_path(session: Session, status: Status, push_context: PushContext, path: str):
    path_items = _common.path_string_to_list(path)

    root_data_id = get_scoped_data_id({
        'Name': '',
        'Type': 'Asset'
    }, push_context.workbook_context.workbook_id)

    path_so_far = list()

    # This function works from the top of the tree down, making sure the assets have been
    # created. These two variables get updated as we work our way down.
    parent_data_id = root_data_id
    child_data_id = root_data_id

    for path_item in path_items:
        if len(path_item) == 0:
            raise SPyValueError('Path contains blank / zero-length segments: "%s"' % path)

        asset_input = AssetInputV1()
        asset_input.name = path_item
        asset_input.scoped_to = push_context.workbook_context.workbook_id
        asset_input.host_id = push_context.datasource_output.id
        asset_input.sync_token = push_context.sync_token

        tree_input = AssetTreeSingleInputV1()
        tree_input.parent_data_id = parent_data_id

        path_so_far.append(path_item)

        asset_dict = {
            'Type': 'Asset',
            'Name': path_so_far[-1],
            'Asset': path_so_far[-1],
            'Path': _common.path_list_to_string(path_so_far[0:-1]) if len(path_so_far) > 1 else ''
        }

        child_data_id = get_scoped_data_id(asset_dict, push_context.workbook_context.workbook_id,
                                           push_context.cleanse_data_ids)

        asset_input.data_id = child_data_id
        tree_input.child_data_id = child_data_id

        if asset_input.data_id not in push_context.reified_assets:
            # Look to see if this asset in the path has an entry in the push_results and if so, use its index to
            # store the results later when we push (using the 'dataframe_index' attribute to correlate to a row).
            existing_asset_row = push_context.push_results.get_by_asset(asset_dict['Asset'], asset_dict.get('Path'))
            if existing_asset_row is not None:
                asset_index = existing_asset_row
            else:
                # No row was found, so add a row at the end
                asset_index, asset_dict = push_context.push_results.add_side_effect_asset(asset_dict)

            if not _maybe_skip_item(session, push_context, child_data_id, asset_dict, raise_exception=False):
                if tree_input.parent_data_id != root_data_id:
                    status.df['Relationship'] += 1
                    setattr(tree_input, 'dataframe_index', asset_index)
                    push_context.tree_batch_input.relationships.append(tree_input)
                else:
                    push_context.roots[asset_input.data_id] = asset_input

                setattr(asset_input, 'dataframe_index', asset_index)
                status.df['Asset'] += _add_no_dupe(push_context.asset_batch_input.assets, asset_input)

            push_context.reified_assets.add(asset_input.data_id)

        # The child becomes the parent for the next item in the hierarchy
        parent_data_id = child_data_id

    return child_data_id


def create_datasource(session: Session, datasource=None) -> DatasourceOutputV1:
    items_api = ItemsApi(session.client)
    datasources_api = DatasourcesApi(session.client)
    users_api = UsersApi(session.client)

    datasource_input = _common.get_data_lab_datasource_input()
    if datasource is not None:
        if not isinstance(datasource, (str, dict)):
            raise SPyValueError('"datasource" parameter must be str or dict')

        if isinstance(datasource, str):
            if datasource == _common.INHERIT_FROM_WORKBOOK:
                datasource_input.name = SeeqNames.LocalDatasources.SeeqMetadataItemStorage.datasource_name
                datasource_input.datasource_class = SeeqNames.LocalDatasources.SeeqMetadataItemStorage.datasource_class
                datasource_input.datasource_id = SeeqNames.LocalDatasources.SeeqMetadataItemStorage.datasource_id
            else:
                datasource_input.name = datasource
                datasource_input.datasource_id = datasource_input.name
        else:
            if 'Datasource Name' not in datasource:
                raise SPyValueError(
                    '"Datasource Name" required for datasource. This is the specific data set being pushed. '
                    'For example, "Permian Basin Well Data"')

            if 'Datasource Class' in datasource:
                raise SPyValueError(
                    '"Datasource Class" cannot be specified for datasource. It will always be '
                    f'"{_common.DEFAULT_DATASOURCE_CLASS}".')

            dict_to_datasource_input(datasource, datasource_input)

        if datasource_input.datasource_id == _common.DEFAULT_DATASOURCE_ID:
            datasource_input.datasource_id = datasource_input.name

    datasource_output_list = datasources_api.get_datasources(datasource_class=datasource_input.datasource_class,
                                                             datasource_id=datasource_input.datasource_id,
                                                             limit=2)  # type: DatasourceOutputListV1

    if len(datasource_output_list.datasources) > 1:
        raise SPyRuntimeError(f'Multiple datasources found with class {datasource_input.datasource_class} '
                              f'and ID {datasource_input.datasource_id}')

    if len(datasource_output_list.datasources) == 1:
        return datasource_output_list.datasources[0]

    datasource_output = datasources_api.create_datasource(body=datasource_input)  # type: DatasourceOutputV1

    # Due to CRAB-23806, we have to immediately call get_datasource to get the right set of additional properties
    datasource_output = datasources_api.get_datasource(id=datasource_output.id)

    # We need to add Everyone with Manage permissions so that all users can push asset trees
    identity_preview_list = users_api.autocomplete_users_and_groups(query='Everyone')  # type: IdentityPreviewListV1
    everyone_user_group_id = None
    for identity_preview in identity_preview_list.items:  # type: IdentityPreviewV1
        if identity_preview.type == 'UserGroup' and \
                identity_preview.name == 'Everyone' and \
                identity_preview.datasource.name == 'Seeq' and \
                identity_preview.is_enabled:
            everyone_user_group_id = identity_preview.id
            break

    if everyone_user_group_id:
        items_api.add_access_control_entry(id=datasource_output.id, body=AceInputV1(
            identity_id=everyone_user_group_id,
            permissions=PermissionsV1(manage=True, read=True, write=True)
        ))

    return datasource_output


def push_access_control(session: Session, item_id: str, acl_df: pd.DataFrame, replace: bool,
                        disable_permission_inheritance: Optional[bool] = None):
    items_api = ItemsApi(session.client)
    acl_output: AclOutputV1 = items_api.get_access_control(id=item_id)

    if disable_permission_inheritance is None:
        # None means "don't change it"
        disable_permission_inheritance = acl_output.permissions_inheritance_disabled

    if disable_permission_inheritance != acl_output.permissions_inheritance_disabled:
        items_api.set_acl_inheritance(id=item_id, inherit_acl=not disable_permission_inheritance)
        acl_output = items_api.get_access_control(id=item_id)

    ace_inputs = list()
    for _, ace_to_push in acl_df.iterrows():
        found = False

        # We sort so that the system-managed entries are found first, and item-specific entries will be removed
        # if they're already covered by the system-managed entries
        sorted_acl_entries = sorted(acl_output.entries, key=lambda e: e.role != 'OWNER' and e.origin is None)
        for existing_ace in sorted_acl_entries:  # type: AceOutputV1
            if (existing_ace.identity.id == ace_to_push['ID'] and
                    existing_ace.permissions.read == ace_to_push['Read'] and
                    existing_ace.permissions.write == ace_to_push['Write'] and
                    existing_ace.permissions.manage == ace_to_push['Manage']):
                found = True
                setattr(existing_ace, 'used', True)
                break

        if found:
            continue

        permissions = PermissionsV1(read=ace_to_push['Read'], write=ace_to_push['Write'], manage=ace_to_push['Manage'])
        ace_inputs.append((permissions, ace_to_push['ID']))

    if replace:
        # We need to add the requested permissions first even if in some cases these will be silently ignored because
        # manage permission can be inherited from a group and by removing the respective permissions we lose the
        # ability to manage the permissions (e.g. agent_api_key gets the manage permission from Agents group in R63+).
        # Any silently-ignored additions will be correctly added after existing ACE entries are removed.
        for permissions, identity_id in ace_inputs:
            items_api.add_access_control_entry(id=item_id,
                                               body=AceInputV1(permissions=permissions,
                                                               identity_id=identity_id))

        # It's important to remove the entries that need to be removed before adding the final permissions. Otherwise,
        # if you try to add an ACE that conflicts with an existing entry, it will be silently ignored.
        for existing_ace in acl_output.entries:  # type: AceOutputV1
            if existing_ace.role == 'OWNER' or (not disable_permission_inheritance and existing_ace.origin is not None):
                # You can't remove OWNER or inherited permissions
                continue

            if hasattr(existing_ace, 'used') and getattr(existing_ace, 'used'):
                continue

            items_api.remove_access_control_entry(id=item_id, ace_id=existing_ace.id)

    for permissions, identity_id in ace_inputs:
        items_api.add_access_control_entry(id=item_id,
                                           body=AceInputV1(permissions=permissions,
                                                           identity_id=identity_id))


def dict_from_scalar_value_output(scalar_value_output):
    """
    :type scalar_value_output: ScalarValueOutputV1
    """
    d = dict()
    d['Value'] = scalar_value_output.value
    d['Unit Of Measure'] = scalar_value_output.uom
    return d


def str_from_scalar_value_dict(scalar_value_dict):
    value = _common.get(scalar_value_dict, 'Value')
    uom = _common.get(scalar_value_dict, 'Unit Of Measure')
    if isinstance(value, str) or not uom:
        return str(value)
    elif _common.is_numeric(value):
        return f'{value} {uom}'


def formula_parameters_dict_from_threshold_metric(session: Session, item_id):
    metrics_api = MetricsApi(session.client)
    metric = metrics_api.get_metric(id=item_id)  # type: ThresholdMetricOutputV1
    formula_parameters = dict()
    if metric.aggregation_function is not None:
        formula_parameters['Aggregation Function'] = metric.aggregation_function
    if metric.bounding_condition is not None:
        formula_parameters['Bounding Condition'] = metric.bounding_condition.id
    if metric.bounding_condition_maximum_duration is not None:
        formula_parameters['Bounding Condition Maximum Duration'] = \
            dict_from_scalar_value_output(metric.bounding_condition_maximum_duration)
    if metric.duration is not None:
        formula_parameters['Duration'] = dict_from_scalar_value_output(metric.duration)
    if metric.measured_item is not None:
        formula_parameters['Measured Item'] = metric.measured_item.id
    if hasattr(metric, 'number_format') and metric.number_format is not None:
        formula_parameters['Number Format'] = metric.number_format
    if metric.period is not None:
        formula_parameters['Period'] = dict_from_scalar_value_output(metric.period)
    if metric.process_type is not None:
        formula_parameters['Process Type'] = metric.process_type
    if hasattr(metric, 'neutral_color') and metric.neutral_color is not None:
        formula_parameters['Metric Neutral Color'] = metric.neutral_color
    if hasattr(metric, 'aggregation_condition_id') and metric.neutral_color is not None:
        formula_parameters['Aggregation Condition'] = metric.aggregation_condition_id

    def _add_thresholds(_thresholds_name, _threshold_output_list):
        formula_parameters[_thresholds_name] = list()
        for threshold in _threshold_output_list:  # type: ThresholdOutputV1
            threshold_dict = dict()
            if threshold.priority is not None:
                priority = threshold.priority  # type: PriorityV1
                threshold_dict['Priority'] = {
                    'Name': priority.name,
                    'Level': priority.level,
                    'Color': priority.color
                }

            if not threshold.is_generated and threshold.item:
                threshold_dict['Item ID'] = threshold.item.id

            if threshold.value is not None:
                if isinstance(threshold.value, ScalarValueOutputV1):
                    threshold_dict['Value'] = dict_from_scalar_value_output(threshold.value)
                else:
                    threshold_dict['Value'] = threshold.value

            formula_parameters[_thresholds_name].append(threshold_dict)

    if metric.thresholds:
        _add_thresholds('Thresholds', metric.thresholds)

    return formula_parameters


def _get_threshold_level(threshold_entry: dict) -> str:
    """
    :param threshold_entry: A dictionary of a single Threshold, like Appserver outputs, which includes the
    Priority (Name + Level + optional Color) and a Value (either a Scalar or ID).
    :return: The Level as a string. Includes the numeric Level value plus the optional Color.
    """
    return '{}{}'.format(threshold_entry['Priority']['Level'],
                         # A custom color can be specified for the threshold by appending it to the priority
                         # number as a hex code. Example: '1#00ff00=10'
                         threshold_entry['Priority']['Color'] if 'Color' in threshold_entry['Priority'] else '')


def _get_threshold_value(threshold_entry: dict, push_context: PushContext) -> Optional[str]:
    _threshold_value = _common.get(threshold_entry, 'Value')
    if _common.present(threshold_entry, 'Item ID'):
        return _item_id_from_parameter_value(threshold_entry['Item ID'], push_context)
    elif _threshold_value is not None:
        if isinstance(_threshold_value, dict):
            return str_from_scalar_value_dict(_threshold_value)
        else:
            return _threshold_value


def _convert_thresholds_to_input(thresholds_obj: Union[dict[str, any], list[Union[str, dict]]],
                                 push_context: PushContext, current_item) -> list[str]:
    """
    Convert a dictionary with keys threshold levels and values of either scalars or metadata to a list of strings
    with level=value/ID of the threshold.

    :param thresholds_obj: Either a dictionary with keys of threshold levels and values of either number of metadata
    dataframes or a list of threshold dictionaries like what Appserver returns.
    :param push_context: PushContext used for looking up mapped item IDs.
    :param current_item: The current item being pushed, just used for outputting errors.
    :return: A list of strings 'level[#optional_color]=value' or 'level[#optional_color]=ID'
    """
    thresholds_list = list()
    if thresholds_obj is None:
        return thresholds_list

    if isinstance(thresholds_obj, dict):
        thresholds_dict = thresholds_obj
    elif isinstance(thresholds_obj, list):
        thresholds_dict = dict()
        for threshold_entry in thresholds_obj:
            level = _get_threshold_level(threshold_entry)
            value = _get_threshold_value(threshold_entry, push_context)
            if level and value:
                thresholds_dict[level] = value
    else:
        raise SPyTypeError(f'{current_item} Threshold Metric "Thresholds" value should be dict or list, '
                           f'but instead is type {type(thresholds_obj).__name__} with value:\n{thresholds_obj}')

    for k, v in thresholds_dict.items():
        threshold = f'{k}={_item_id_from_parameter_value(v, push_context)}' \
            if isinstance(v, pd.DataFrame) or isinstance(v, dict) \
            else f'{k}={v}'
        thresholds_list.append(threshold)

    return thresholds_list


def threshold_metric_input_from_formula_parameters(
        threshold_metric_input: ThresholdMetricInputV1, parameters, push_context: PushContext):
    def _add_scalar_value(_attr, _key):
        if _common.present(parameters, _key):
            setattr(threshold_metric_input, _attr, str_from_scalar_value_dict(parameters[_key]))

    def _add_mapped_item(_attr, _key):
        if _common.present(parameters, _key):
            mapped_id = _item_id_from_parameter_value(parameters[_key], push_context)

            setattr(threshold_metric_input, _attr, mapped_id)

    def _get_thresholds() -> list[str]:
        _thresholds_list = list()
        if not _common.present(parameters, 'Thresholds'):
            return _thresholds_list
        for threshold_dict in parameters['Thresholds']:
            level = _get_threshold_level(threshold_dict)
            value = _get_threshold_value(threshold_dict, push_context)
            if level and value:
                _thresholds_list.append(f'{level}={value}')
        return _thresholds_list

    threshold_metric_input.aggregation_function = _common.get(parameters, 'Aggregation Function')
    threshold_metric_input.neutral_color = _common.get(parameters, 'Metric Neutral Color')
    _add_mapped_item('bounding_condition', 'Bounding Condition')
    _add_scalar_value('bounding_condition_maximum_duration', 'Bounding Condition Maximum Duration')
    _add_scalar_value('duration', 'Duration')
    _add_mapped_item('measured_item', 'Measured Item')
    _add_scalar_value('period', 'Period')
    threshold_metric_input.thresholds = _get_thresholds()


RESERVED_SPY_STATUS_COLUMN_NAMES = [
    'Push Result', 'Push Count', 'Push Time',
    'Pull Result', 'Pull Count', 'Pull Time',
    'Build Result', 'Push Directives'
]

RESERVED_SPY_COLUMN_NAMES = [
                                'Build Path', 'Build Asset', 'Build Template',
                                'ID', 'Type', 'Path', 'Asset', 'Object', 'Asset Object', 'Depth',
                                'Formula Parameters', 'Capsule Is Uncertain', 'Capsule Property Units',
                                'Override Number Format', 'Condition', 'Datasource Name',
                                'Permissions Inheritance Disabled', 'Access Control',
                                'Roll Up Statistic', 'Roll Up Parameters', 'Old Asset Format',
                                'Template ID', 'Swap Out Asset ID', 'Swap In Asset ID', 'Source Workstep ID',
                                'Parent ID', 'Parent Data ID', 'Dummy Item'
                            ] + RESERVED_SPY_STATUS_COLUMN_NAMES

# Properties that will not be supplied via the "additionalProperties" field.
# This must be kept in sync with RESERVED_ITEM_PROPERTIES in StoredItemOutput.java
RESERVED_ITEM_PROPERTIES = [
    SeeqNames.Properties.guid,
    SeeqNames.Properties.datasource_class,
    SeeqNames.Properties.datasource_id,
    SeeqNames.Properties.data_id,
    SeeqNames.Properties.name,
    SeeqNames.Properties.description,
    SeeqNames.Properties.metadata_properties,
    SeeqNames.Properties.interpolation_method,
    SeeqNames.Properties.maximum_duration,
    SeeqNames.Properties.capsule_id_property,
    SeeqNames.Properties.maximum_interpolation,
    SeeqNames.Properties.source_maximum_interpolation,
    SeeqNames.Properties.override_maximum_interpolation,
    SeeqNames.Properties.value_uom,
    SeeqNames.Properties.uom,
    SeeqNames.Properties.number_format,
    SeeqNames.Properties.source_number_format,
    SeeqNames.Properties.formula,
    SeeqNames.Properties.formula_parameters,
    SeeqNames.Properties.u_i_config,
    SeeqNames.Properties.sync_token,
    SeeqNames.Properties.permission_inheritance_disabled,
    SeeqNames.Properties.permissions_from_datasource,
    SeeqNames.Properties.source_security_string,
    SeeqNames.Properties.security_string,
    SeeqNames.Properties.cache_id,
    SeeqNames.Properties.stale_metadata,
    SeeqNames.Properties.column_definitions,
    SeeqNames.Properties.locked,
]

ADDITIONAL_RESERVED_PROPERTIES = [
    SeeqNames.Properties.aggregation_function,
    SeeqNames.Properties.created_at,
    SeeqNames.Properties.created_by,
    SeeqNames.Properties.formula_version,
    SeeqNames.Properties.last_viewed_at,
    SeeqNames.Properties.metric_configuration_migration,
    SeeqNames.Properties.neutral_color,
    SeeqNames.Properties.scoped_to,
    SeeqNames.Properties.storage_location,
    SeeqNames.Properties.stored_in_seeq,
    SeeqNames.Properties.swap_key,
    SeeqNames.Properties.swap_source_asset_id,
    SeeqNames.Properties.swap_source_id,
    SeeqNames.Properties.updated_at,
    'Cached By Service',
    'Data Version Check',
    'Key Unit Of Measure'
]

IGNORED_PROPERTIES = (RESERVED_SPY_COLUMN_NAMES + RESERVED_ITEM_PROPERTIES + ADDITIONAL_RESERVED_PROPERTIES +
                      [ORIGINAL_INDEX_COLUMN])
