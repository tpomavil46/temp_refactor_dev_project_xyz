import os
import types
from typing import List, Optional

import numpy as np
import pandas as pd

from seeq.sdk.api import ItemsApi, TreesApi
from seeq.sdk.models import SwapInputV1, SwapOptionV1, ItemWithSwapPairsV1, ItemDependencyOutputV1, \
    ItemParameterOfOutputV1, ItemPreviewWithAssetsV1
from seeq.spy import _common, _login
from seeq.spy._errors import *
from seeq.spy._session import Session
from seeq.spy._status import Status


@Status.handle_keyboard_interrupt()
def swap(items: pd.DataFrame, assets: pd.DataFrame, *, partial_swaps_ok: bool = False,
         old_asset_format: Optional[bool] = None, errors: Optional[str] = None, quiet: Optional[bool] = None,
         status: Optional[Status] = None, session: Optional[Session] = None) -> pd.DataFrame:
    """
    Operates on a DataFrame of items by swapping out the assets that those
    items are based on. The returned DataFrame can be supplied to
    spy.pull() to retrieve the resulting data.

    Parameters
    ----------
    items : {pd.DataFrame}
        A DataFrame of items over which to perform the swapping operation. The
        only required column is ID. Typically, you will have derived this
        DataFrame via a spy.search() or spy.push(metadata) call.

    assets : {pd.DataFrame}
        A DataFrame of Asset items (and ONLY Asset items) to swap IN. Each row
        must have valid ID, Type, Path, Asset, and Name columns. Typically, you
        will have derived this DataFrame via a spy.search() call.

        When a calculation depends on multiple assets, you must specify Swap
        Groups (with the notable exception of a "multi-level swap", see below.
        You can determine the set of "swappable assets" by invoking
        spy.search(include_swappable_assets=True). Then you must assemble an
        assets DataFrame where each row is the asset to be swapped in and there
        is a "Swap Out" column that specifies the corresponding asset (from
        the list of swappable assets) to be swapped out. The "Swap Out" column
        can be a string ID, the latter portion of an asset path/name, or a
        DataFrame row representing the asset (which could be used directly
        from the "Swappable Assets" column of your search result).
        Additionally, assuming you want to produce several swapped
        calculations, you must group the swap rows together by specifying a
        "Swap Group" column where unique values are used to group together
        the rows that comprise the set of assets to be swapped in/out.

        If a calculation depends on asset(s) where the immediate
        parent asset is a "categorical" name (e.g. "Raw" and "Cleansed"), it
        is referred to as a "multi-level swap", where you actually wish to
        swap at a level higher than the immediate parent. You can do so by
        specifying that higher level, and spy will automatically figure out
        the actual lower-level items that are appropriate to swap.

    partial_swaps_ok : bool, default False
        If True, allows partial swaps to occur. A partial swap occurs when
        the incoming asset has children that only partially match the outgoing
        asset.

    old_asset_format : bool, default True
        If your DataFrame doesn't use the "old" asset format, you can specify
        False for this argument. See spy.search() documentation for more info.

    quiet : bool, default False
        If True, suppresses progress output. Note that when status is
        provided, the quiet setting of the Status object that is passed
        in takes precedence.

    errors : {'raise', 'catalog'}, default 'raise'
        If 'raise', any errors encountered will cause an exception. If 'catalog',
        errors will be added to a 'Result' column in the status.df DataFrame
        (or 'Swap Result' column if using `spy.options.compatibility = 189` or lower).

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

    Returns
    -------
    pandas.DataFrame
        A DataFrame with rows for each item swapped. Includes a "Result"
        column that is either "Success" or an error message (when
        errors='catalog'). Also includes a "Swap Performed" column that
        specifies the details of what swap pairs were utilized.

        Additionally, the following properties are stored on the "spy"
        attribute of the output DataFrame:

        =================== ===================================================
        Property            Description
        =================== ===================================================
        func                A str value of 'spy.swap'
        kwargs              A dict with the values of the input parameters
                            passed to spy.swap to get the output DataFrame
        status              A spy.Status object with the status of the
                            spy.swap call
        =================== ===================================================

    """
    input_args = _common.validate_argument_types([
        (items, 'items', pd.DataFrame),
        (assets, 'assets', pd.DataFrame),
        (partial_swaps_ok, 'partial_swaps_ok', bool),
        (old_asset_format, 'old_asset_format', bool),
        (errors, 'errors', str),
        (quiet, 'quiet', bool),
        (status, 'status', Status),
        (session, 'session', Session)
    ])

    session = Session.validate(session)
    status = Status.validate(status, session, quiet, errors)
    _login.validate_login(session, status)

    old_asset_format = _common.resolve_old_asset_format_arg(old_asset_format, assets)
    # Swap originally used 'Swap Result' as the status output, but 'Result' is more consistent with the rest of SPy.
    result_key = 'Swap Result' if session.options.wants_compatibility_with(189) else 'Result'

    items_api = ItemsApi(session.client)

    if 'ID' not in items.columns:
        raise ValueError('"ID" column not found in items DataFrame')

    for col in ['ID', 'Type', 'Path', 'Asset', 'Name']:
        if col not in assets.columns:
            raise ValueError(f'"{col}" column not found in assets DataFrame')

    status.update(f'Performing swaps for {len(items)} items across {len(assets)} assets', Status.RUNNING)

    item_list = list()
    for index, row in items.iterrows():
        # noinspection PyBroadException
        try:
            if not _common.present(row, 'ID'):
                raise Exception(f'items DataFrame contains row without valid ID at index {index}')

            item_dict = dict(row.to_dict())

            item_id = _common.get(row, 'ID')
            item_dependency_output = items_api.get_formula_dependencies(id=item_id)
            swappable_assets = get_swappable_assets(item_dependency_output)

            if len(swappable_assets) == 0:
                item_dict[result_key] = ("This item does not depend on any assets, either as the item's parent or "
                                         "as the parent of its formula parameters.")
                continue

            item_list.append((item_dependency_output, swappable_assets))

        except Exception:
            if status.errors == 'raise':
                raise
            else:
                pass

    if 'Swap Group' not in assets:
        assets['Swap Group'] = np.arange(len(assets))

    swap_groups = list(dict.fromkeys(assets['Swap Group'].to_list()))
    row_index = 0
    staged_swaps = [pd.DataFrame({'Original ID': pd.Series(dtype='str'),
                                  'ID': pd.Series(dtype='str'),
                                  'Type': pd.Series(dtype='str'),
                                  'Name': pd.Series(dtype='str'),
                                  result_key: pd.Series(dtype='str'),
                                  'Swap Performed': pd.Series(dtype='str')})]
    for swap_group in swap_groups:
        swap_rows = assets[assets['Swap Group'] == swap_group]

        for item_dependency_output, dependencies_with_relevant_assets in item_list:
            staged_swaps.append(pd.DataFrame({
                'Original ID': item_dependency_output.id,
                'ID': np.nan,
                'Type': item_dependency_output.type,
                'Name': item_dependency_output.name,
                result_key: 'Pending',
                'Swap Performed': np.nan
            }, index=[0]))

            def _on_success(_row_index, _job_result):
                for _key, _value in _job_result.items():
                    status.df.at[_row_index, _key] = _value

            status.add_job(row_index, (_do_swap, session, dependencies_with_relevant_assets,
                                       item_dependency_output,
                                       old_asset_format,
                                       partial_swaps_ok, swap_rows), _on_success)

            row_index += 1

    status.df = pd.concat(staged_swaps, ignore_index=True)
    status.execute_jobs(session)

    _construct_header_column(status.df)

    push_df_properties = types.SimpleNamespace(
        func='spy.swap',
        kwargs=input_args,
        status=status)

    result_df: pd.DataFrame = status.df.copy()

    result_df.drop_duplicates(subset=['Original ID', 'ID'], inplace=True)

    _common.put_properties_on_df(result_df, push_df_properties)

    error_count = len(result_df[result_df[result_key] != 'Success'])
    success_count = len(result_df[result_df[result_key] == 'Success'])
    if error_count > 0:
        status.update(f'Returned {success_count} swapped item(s). {error_count} item(s) had '
                      f'errors, see "Result" column.', Status.FAILURE)
    else:
        status.update(f'Success: Returned {success_count} swapped item(s).', Status.SUCCESS)

    return result_df


def _do_swap(session: Session, dependencies_with_relevant_assets, item_dependency_output, old_asset_format,
             partial_swaps_ok, swap_rows):
    items_api = ItemsApi(session.client)
    result_key = 'Swap Result' if session.options.wants_compatibility_with(189) else 'Result'

    swap_result = {
        'Original ID': item_dependency_output.id,
        'ID': np.nan,
        'Type': item_dependency_output.type,
        'Name': item_dependency_output.name,
        result_key: 'Success'
    }

    swap_list: List[SwapInputV1] = list()
    for index, swap_row in swap_rows.iterrows():
        if _common.get(swap_row, 'Type', 'Asset') != 'Asset':
            raise SPyValueError(f'assets DataFrame contains non-Asset type at row index {index}')

        swap_out = _common.get(swap_row, 'Swap Out')
        if swap_out is None:
            if len(dependencies_with_relevant_assets) == 1:
                swap_out = dependencies_with_relevant_assets[0].ancestors[-1].id
            else:
                raise SPyRuntimeError(
                    f'Item "{item_dependency_output.name}" ({item_dependency_output.id}) depends on '
                    'multiple assets. You must supply "Swap Group" and "Swap Out" columns in the '
                    'DataFrame, see spy.swap() function documentation for details.')

        try:
            swap_inputs = _create_swap_inputs(session, item_dependency_output.id, swap_row,
                                              old_asset_format, swap_out,
                                              dependencies_with_relevant_assets,
                                              partial_swaps_ok)
        except Exception as e:
            raise SPyRuntimeError(f'Error attempting to create swap for item '
                                  f'"{item_dependency_output.name}" ({item_dependency_output.id}):\n'
                                  f'{_common.format_exception(e)}')

        swap_list.extend(swap_inputs)

    try:
        swapped_item = items_api.find_swap(id=item_dependency_output.id, body=swap_list)
    except ApiException as e:
        raise SPyRuntimeError(f'Error finding swap for item '
                              f'"{item_dependency_output.name}" ({item_dependency_output.id}):\n'
                              f'{_common.format_exception(e)}')

    swap_result['ID'] = swapped_item.id
    swap_result['Swap Performed'] = '\n'.join([
        (getattr(s, 'friendly_swap_out_name') + ' --> ' + getattr(s, 'friendly_swap_in_name'))
        for s in swap_list
    ])

    return swap_result


def _construct_header_column(df: pd.DataFrame):
    verbose_headers = list()
    results = df.to_dict('records')
    for result in results:
        if pd.isna(result['Swap Performed']):
            continue

        for swap_instance in result['Swap Performed'].split('\n'):
            to_from = swap_instance.split(' --> ')
            verbose_headers.append(to_from[0] + ' >> ' + result['Name'])
            verbose_headers.append(to_from[1] + ' >> ' + result['Name'])
    new_headers = remove_common_prefix_suffix(verbose_headers)
    new_header_map = dict(zip(verbose_headers, new_headers))
    header_column = list()
    for result in results:
        if pd.isna(result['Swap Performed']):
            header_column.append(np.nan)
            continue

        new_name_pieces = list()
        for swap_instance in result['Swap Performed'].split('\n'):
            path_list = swap_instance.split(' --> ')[-1] + ' >> ' + result['Name']
            new_name_pieces.append(new_header_map[path_list])
        header_column.append(', '.join(new_name_pieces))

    df['Header'] = pd.Series(header_column, dtype=object)


def _create_swap_inputs(session: Session, item_id, swap_in_row, old_asset_format, swap_out,
                        possibilities, partial_swaps_ok) -> List[SwapInputV1]:
    swap_in_id = swap_in_row['ID']

    matched_possibilities = list()
    for possibility in possibilities:
        if len(possibility.ancestors) == 0:
            continue

        if _common.is_guid(swap_out):
            if swap_out == possibility.ancestors[-1].id:
                matched_possibilities.append(possibility)
                break

            continue

        elif isinstance(swap_out, str):
            swap_out_path_list = _common.path_string_to_list(swap_out)
            if len(swap_out_path_list) > len(possibility.ancestors):
                continue
            relevant_ancestors = [a.name for a in possibility.ancestors[-len(swap_out_path_list):]]
            if relevant_ancestors == swap_out_path_list:
                matched_possibilities.append(possibility)
                continue

        else:
            if not _common.present(swap_out, 'ID'):
                raise SPyValueError(f'"Swap Out" value does not have valid "ID":\n{swap_out}')

            if _common.get(swap_out, 'ID') == possibility.ancestors[-1].id:
                matched_possibilities.append(possibility)
                break

    if len(matched_possibilities) == 0:
        # Try to perform a multi-level swap
        items_api = ItemsApi(session.client)
        swap_options = items_api.get_swap_options(id=swap_in_id, swap_out_item_ids=[item_id])
        if len(swap_options.swap_options) == 1:
            swap_option: SwapOptionV1 = swap_options.swap_options[0]
            swap_root_candidate = swap_option.swap_root_candidate
            it_matches = False
            if _common.is_guid(swap_out):
                if swap_out == swap_root_candidate.id:
                    it_matches = True

            elif isinstance(swap_out, str):
                swap_out_path_list = _common.path_string_to_list(swap_out)
                if len(swap_out_path_list) <= (len(swap_root_candidate.ancestors) + 1):
                    relevant_ancestor_count = len(swap_out_path_list) - 1
                    relevant_ancestors = list()
                    if relevant_ancestor_count > 0:
                        relevant_ancestors += [a.name for a in swap_root_candidate.ancestors[-relevant_ancestor_count:]]
                    relevant_ancestors += [swap_root_candidate.name]
                    if relevant_ancestors == swap_out_path_list:
                        it_matches = True

            else:
                # Assume it's a dict or a Pandas series (i.e., a row)
                if not _common.present(swap_out, 'ID'):
                    raise SPyValueError(f'"Swap Out" value does not have valid "ID":\n{swap_out}')

                if _common.get(swap_out, 'ID') == swap_root_candidate.id:
                    it_matches = True

            if it_matches:
                # We take the best match of the bunch
                sorted_items_with_swap_pairs = swap_option.items_with_swap_pairs.copy()
                sorted_items_with_swap_pairs.sort(key=lambda item: item.parameter_match, reverse=True)
                chosen_pair: ItemWithSwapPairsV1 = sorted_items_with_swap_pairs[0]
                if chosen_pair.parameter_match < 1.0 and not partial_swaps_ok:
                    invalid_swap_outs_str = '\n'.join([
                        f'{invalid_swap_out.item.type} "{invalid_swap_out.item.name}" ({invalid_swap_out.item.id})'
                        for invalid_swap_out in swap_option.invalid_swap_outs
                    ])
                    raise SPyValueError(f'For swap-in row...\n\n{swap_in_row}\n\n...the best match is only '
                                        f'{int(chosen_pair.parameter_match * 100)}% similar.\n\n'
                                        f'Items we could not find swaps for:\n{invalid_swap_outs_str}\n\n'
                                        f'Specify partial_swaps_ok=True to swap in whatever matches and leave '
                                        f'the rest un-swapped.')

                swap_inputs = chosen_pair.swap_pairs
                for swap_input in swap_inputs:  # type: SwapInputV1
                    # We only have IDs, so we have to make an API call to get the ancestors
                    trees_api = TreesApi(session.client)
                    swap_out_tree_item: ItemPreviewWithAssetsV1 = trees_api.get_tree(id=swap_input.swap_out).item
                    swap_in_tree_item: ItemPreviewWithAssetsV1 = trees_api.get_tree(id=swap_input.swap_in).item

                    swap_out_path = (' >> '.join([a.name for a in swap_out_tree_item.ancestors]) +
                                     f' >> {swap_out_tree_item.name}')
                    swap_in_path = (' >> '.join([a.name for a in swap_in_tree_item.ancestors]) +
                                    f' >> {swap_in_tree_item.name}')

                    setattr(swap_input, 'friendly_swap_out_name', swap_out_path)
                    setattr(swap_input, 'friendly_swap_in_name', swap_in_path)

                # Sort the outputs by swap_out so that headers are assembled deterministically
                return sorted(swap_inputs, key=lambda s: s.swap_out)

    if len(matched_possibilities) == 0:
        swappable_assets_str = "\n".join(
            [_common.path_list_to_string([a.name for a in m.ancestors]) for m in possibilities])
        raise SPyValueError(f'"Swap Out" value...\n{swap_out}\n'
                            '...could not be matched against any of the following swappable assets:\n'
                            f'{swappable_assets_str}')

    if len(matched_possibilities) > 1:
        matched_assets_str = "\n".join(
            [_common.path_list_to_string([a.name for a in m.ancestors]) for m in matched_possibilities])
        raise SPyValueError(f'"Swap Out" value...\n{swap_out}\n...matched multiple swappable assets:\n'
                            f'{matched_assets_str}')

    chosen_possibility = matched_possibilities[0]

    swap_input = SwapInputV1(swap_in=swap_in_id, swap_out=chosen_possibility.ancestors[-1].id)
    setattr(swap_input, 'friendly_swap_out_name', ' >> '.join([a.name for a in chosen_possibility.ancestors]))
    if old_asset_format:
        setattr(swap_input, 'friendly_swap_in_name',
                f"{swap_in_row['Path']} >> {swap_in_row['Asset']} >> {swap_in_row['Name']}")
    else:
        setattr(swap_input, 'friendly_swap_in_name', f"{swap_in_row['Path']} >> {swap_in_row['Name']}")

    return [swap_input]


def get_swappable_assets(item_dependency_output: ItemDependencyOutputV1):
    items = _get_dependencies_with_relevant_assets(item_dependency_output, item_dependency_output.dependencies)
    results = list()
    ids = set()
    for item in items:
        if len(item.ancestors) == 0:
            continue

        asset = item.ancestors[-1]
        if asset.id in ids:
            continue

        ids.add(asset.id)
        results.append(item)

    return results


def _get_dependencies_with_relevant_assets(item: ItemDependencyOutputV1, dependencies: List[ItemParameterOfOutputV1]):
    # ******************************************************************************************************************
    # This is a DIRECT port of formula.utilities.ts#getDependenciesWithRelevantAssets(). Please keep this in sync with
    # that file if necessary and don't add any extra functionality to it on the Python side.
    # ******************************************************************************************************************

    # Keep track of every ID we find that matters.
    # This should end up being items with assets, or assetless leaf nodes.
    result_id_set = list()

    # TypeScript code from client/packages/webserver/app/src/utilities/formula.utilities.ts:
    #
    # find_parameters = _.chain(dependencies)
    #     .flatMap((dependency) => _.map(dependency.parameterOf, (item) => [item.id, dependency]))
    #     .groupBy(_.first)
    #     .map((pairs, dependee) => [dependee, _.map(pairs, (pair) => pair[1])])
    #     .fromPairs()
    #     .value()
    find_parameters = dict()
    for dependency in dependencies:
        for parameter_of in dependency.parameter_of:  # type: ItemPreviewWithAssetsV1
            if parameter_of.id not in find_parameters:
                find_parameters[parameter_of.id] = list()
            find_parameters[parameter_of.id].append(dependency)

    # Items whose assets we're currently trying to find out. Start with the root item.
    queue = [item]

    while len(queue) > 0:
        # take the first param.
        current = queue.pop()

        if current.id in result_id_set:
            continue

        if len(current.ancestors) > 0:
            # if it has an ancestor, put its id in the resultIds, we're done with it.
            result_id_set.append(current.id)
        else:
            # if not, add its parameters to the queue of possible ancestors
            parameters = find_parameters.get(current.id)

            if parameters is None or len(parameters) == 0:
                # leaf node; add to results so we don't bother with it anymore
                result_id_set.append(current.id)
            else:
                # not already examined, no asset ancestor - recurse to its parameters
                queue += parameters

    # Include the original item in the possible results we pick from
    # noinspection PyTypeChecker
    possible_set = dependencies + [item]

    return list(filter(lambda d: (d.id in result_id_set), possible_set))


def remove_common_prefix_suffix(fqns):
    list_of_lists = [_common.path_string_to_list(fqn) for fqn in fqns]

    common_prefix = os.path.commonprefix(list_of_lists)
    common_suffix = os.path.commonprefix([s[::-1] for s in list_of_lists])[::-1]

    common_prefix_len = len(common_prefix)
    common_suffix_len = len(common_suffix)

    if common_prefix == common_suffix:
        # Cover the edge case where the item is being swapped with itself. In such a case, remove all but
        # the last element (as part of the prefix removal) and don't do anything for suffix removal.
        common_prefix_len -= 1
        common_suffix = ''

    # Remove common prefix and suffix from each sublist
    result = [
        _common.path_list_to_string(
            inner_list[common_prefix_len: -common_suffix_len] if common_suffix else inner_list[common_prefix_len:]
        )
        for inner_list in list_of_lists
    ]

    return result
