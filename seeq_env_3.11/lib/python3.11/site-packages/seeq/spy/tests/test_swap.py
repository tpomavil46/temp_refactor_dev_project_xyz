from unittest import mock

import numpy as np
import pandas as pd
import pytest

from seeq import spy
from seeq.spy._errors import *
from seeq.spy.tests import test_common


def setup_module():
    test_common.initialize_sessions()


@pytest.mark.system
def test_identity():
    items_df = spy.search({
        'Path': 'Example >> Cooling Tower 1',
        'Asset': 'Area A',
        'Name': 'Temperature'
    })

    area_a_df = spy.search({
        'Path': 'Example >> Cooling Tower 1',
        'Type': 'Asset',
        'Name': 'Area A'
    })

    swap_df = spy.swap(items_df, area_a_df, errors='catalog')
    assert swap_df.spy.status.message == 'Success: Returned 1 swapped item(s).'
    assert swap_df['Header'].iloc[0] == 'Temperature'


@pytest.mark.system
def test_duplicate_items():
    items_df = spy.search({
        'Path': 'Example >> Cooling Tower 1',
        'Asset': 'Area A',
        'Name': 'Temperature'
    })

    area_a_df = spy.search({
        'Path': 'Example >> Cooling Tower 1',
        'Type': 'Asset',
        'Name': 'Area A'
    })

    duplicated_items_df = pd.concat([items_df, items_df], ignore_index=True)

    swap_df = spy.swap(duplicated_items_df, area_a_df, errors='catalog')
    assert swap_df.spy.status.message == 'Success: Returned 1 swapped item(s).'
    assert len(swap_df) == 1
    assert swap_df['Header'].iloc[0] == 'Temperature'


@pytest.mark.system
def test_stored_item_swap():
    items_df = spy.search({
        'Path': 'Example >> Cooling Tower 1',
        'Asset': 'Area A',
        'Name': '/(Temperature|Relative Humidity)/'
    })

    temperature_id = items_df[items_df['Name'] == 'Temperature'].iloc[0]['ID']
    relative_humidity_id = items_df[items_df['Name'] == 'Relative Humidity'].iloc[0]['ID']

    assets_df = spy.search({
        'Path': 'Example >> Cooling Tower 2',
        'Type': 'Asset',
    })

    with pytest.raises(RuntimeError, match='unable to swap out Area A and swap in Area F'):
        spy.swap(items_df, assets_df)

    # Prove that only ID column is necessary
    items_df = items_df[['ID']]

    swap_df = spy.swap(items_df, assets_df, errors='catalog')

    assert swap_df.spy.status.message == 'Returned 4 swapped item(s). 2 item(s) had errors, see "Result" column.'

    actual_swap_df = swap_df.copy()
    actual_swap_df = actual_swap_df[['Swap Performed', 'Name', 'Type', 'Result', 'Header']]
    actual_swap_df.sort_values(by=['Swap Performed', 'Name', 'Type'], inplace=True)
    actual_swap_df.reset_index(drop=True, inplace=True)

    expected_swap_df = pd.DataFrame([
        {'Swap Performed': 'Example >> Cooling Tower 1 >> Area A --> Example >> Cooling Tower 2 >> Area D',
         'Name': 'Relative Humidity', 'Type': 'StoredSignal', 'Result': 'Success',
         'Header': 'Cooling Tower 2 >> Area D >> Relative Humidity'},
        {'Swap Performed': 'Example >> Cooling Tower 1 >> Area A --> Example >> Cooling Tower 2 >> Area D',
         'Name': 'Temperature', 'Type': 'StoredSignal', 'Result': 'Success',
         'Header': 'Cooling Tower 2 >> Area D >> Temperature'},
        {'Swap Performed': 'Example >> Cooling Tower 1 >> Area A --> Example >> Cooling Tower 2 >> Area E',
         'Name': 'Relative Humidity', 'Type': 'StoredSignal', 'Result': 'Success',
         'Header': 'Cooling Tower 2 >> Area E >> Relative Humidity'},
        {'Swap Performed': 'Example >> Cooling Tower 1 >> Area A --> Example >> Cooling Tower 2 >> Area E',
         'Name': 'Temperature', 'Type': 'StoredSignal', 'Result': 'Success',
         'Header': 'Cooling Tower 2 >> Area E >> Temperature'},
        {'Swap Performed': np.nan,
         'Name': 'Relative Humidity', 'Type': 'StoredSignal',
         'Result': '[SPyRuntimeError] Error finding swap for item "Relative Humidity" ('
                   f'{relative_humidity_id}):\n(400) Bad Request - Relative Humidity not swapped, '
                   'unable to swap out Area A and swap in Area F',
         'Header': np.nan},
        {'Swap Performed': np.nan,
         'Name': 'Temperature', 'Type': 'StoredSignal',
         'Result': '[SPyRuntimeError] Error finding swap for item "Temperature" ('
                   f'{temperature_id}):\n(400) Bad Request - Temperature not swapped, '
                   'unable to swap out Area A and swap in Area F',
         'Header': np.nan},
    ])

    assert actual_swap_df.equals(expected_swap_df)

    swap_df.dropna(inplace=True)

    swapped_pull_df = spy.pull(swap_df, start='2021-01-01', end='2021-01-01', header='ID')
    swapped_pull_df = swapped_pull_df[sorted(swapped_pull_df.columns)]
    direct_pull_df = spy.pull(spy.search({
        'Path': 'Example >> Cooling Tower 2',
        'Asset': '/Area [DE]/',
        'Name': '/(Temperature|Relative Humidity)/'
    }), start='2021-01-01', end='2021-01-01', header='ID')
    direct_pull_df = direct_pull_df[sorted(direct_pull_df.columns)]

    assert swapped_pull_df.equals(direct_pull_df)


@pytest.mark.system
def test_calculated_item_swap_single_asset():
    workbook = 'test_calculated_item_swap_single_asset'

    area_a_df = spy.search({
        'Path': 'Example >> Cooling Tower 1',
        'Asset': 'Area A',
        'Name': 'Temperature'
    })

    def _push(_area_df, i):
        _area = _area_df.iloc[0]
        return spy.push(metadata=pd.DataFrame([{
            'Name': f'Added {i}',
            'Formula': '$t + 10',
            'Formula Parameters': {'$t': _area}
        }, {
            'Name': f'Hot {i}',
            'Formula': '$t > 80',
            'Formula Parameters': {'$t': _area}
        }, {
            'Name': f'Average on One Day in 2016 {i}',
            'Formula': "$t.average(capsule('2016-12-18'))",
            'Formula Parameters': {'$t': _area}
        }]), workbook=workbook, datasource=workbook, worksheet=None)

    push_a_df = _push(area_a_df, 1)

    area_b_df = spy.search({
        'Path': 'Example >> Cooling Tower 1',
        'Asset': 'Area B',
        'Name': 'Temperature'
    })

    push_b_df = _push(area_b_df, 2)

    with pytest.raises(SPyValueError, match='assets DataFrame contains non-Asset type'):
        spy.swap(push_a_df, area_b_df)

    area_b_asset_df = spy.search({
        'Path': 'Example >> Cooling Tower 1',
        'Name': 'Area B',
        'Type': 'Asset'
    })

    swap_df = spy.swap(push_a_df, area_b_asset_df)

    assert swap_df.spy.status.message == 'Success: Returned 3 swapped item(s).'

    assert sorted(swap_df['Header'].to_list()) == sorted([
        'Area B >> Added 1', 'Area B >> Average on One Day in 2016 1', 'Area B >> Hot 1'])

    swapped_pull_df = spy.pull(swap_df, start='2021-01-01', end='2021-01-01')
    swapped_pull_df = swapped_pull_df[sorted(swapped_pull_df.columns)]
    swapped_pull_df.columns = pd.Index(['Signal', 'Scalar', 'Condition'])
    direct_pull_area_a_df = spy.pull(push_a_df, start='2021-01-01', end='2021-01-01')
    direct_pull_area_a_df = direct_pull_area_a_df[sorted(direct_pull_area_a_df.columns)]
    direct_pull_area_a_df.columns = pd.Index(['Signal', 'Scalar', 'Condition'])
    direct_pull_area_b_df = spy.pull(push_b_df, start='2021-01-01', end='2021-01-01')
    direct_pull_area_b_df = direct_pull_area_b_df[sorted(direct_pull_area_b_df.columns)]
    direct_pull_area_b_df.columns = pd.Index(['Signal', 'Scalar', 'Condition'])

    assert swapped_pull_df.equals(direct_pull_area_b_df)
    assert not swapped_pull_df.equals(direct_pull_area_a_df)

    # Now try shape='capsules'
    swapped_pull_df = spy.pull(swap_df, shape='capsules', start='2021-01-01', end='2021-01-01')
    assert len(swapped_pull_df) == 1
    assert swapped_pull_df.columns.to_list() == ['Condition', 'Capsule Start', 'Capsule End', 'Capsule Is Uncertain',
                                                 'Area B >> Average on One Day in 2016 1']

    # Check all the data types to make sure they're correct for this scalar-only table
    assert swapped_pull_df['Condition'].dtype.name == 'object'
    assert swapped_pull_df['Capsule Start'].dtype.name == 'datetime64[ns, UTC]'
    assert swapped_pull_df['Capsule End'].dtype.name == 'datetime64[ns, UTC]'
    assert swapped_pull_df['Capsule Is Uncertain'].dtype.name == 'bool'

    swapped_pull_df = spy.pull(swap_df, shape='capsules', start='2021-01-01', end='2021-02-01')
    unique_conditions = set(swapped_pull_df['Condition'].to_list())
    assert unique_conditions == {'Area B >> Hot 1'}
    assert len(swapped_pull_df) > 20
    assert swapped_pull_df.columns.to_list() == ['Condition', 'Capsule Start', 'Capsule End', 'Capsule Is Uncertain',
                                                 'Area B >> Added 1 (average)',
                                                 'Area B >> Average on One Day in 2016 1']
    unique_scalars = set(swapped_pull_df['Area B >> Average on One Day in 2016 1'].to_list())
    assert len(unique_scalars) == 1


@pytest.mark.system
def test_calculated_item_swap_multiple_assets():
    workbook = 'test_calculated_item_swap_multiple_assets'

    areas_df = spy.search({
        'Path': 'Example >> Cooling Tower 1',
        'Asset': '/Area [AB]/',
        'Name': 'Compressor Power'
    })

    push_df = spy.push(metadata=pd.DataFrame([{
        'Name': f'Subtracted Compressor Power',
        'Formula': '$a - $b',
        'Formula Parameters': {
            '$a': areas_df[areas_df['Asset'] == 'Area A'],
            '$b': areas_df[areas_df['Asset'] == 'Area B']
        }
    }]), workbook=workbook, datasource=workbook, worksheet=None)

    pushed_items_df = spy.search(push_df, include_swappable_assets=True)

    cooling_tower_1_df = spy.search({
        'Path': 'Example >> Cooling Tower 1',
        'Type': 'Asset'
    }).sort_values(by='Name')

    cooling_tower_2_df = spy.search({
        'Path': 'Example >> Cooling Tower 2',
        'Type': 'Asset'
    }).sort_values(by='Name')

    swappable_assets_df = pushed_items_df.iloc[0]['Swappable Assets'].sort_values(by='Asset')
    swap_groups_df = pd.DataFrame([
        # Identity swap: Area A --> Area A, Area B --> Area B
        cooling_tower_1_df.iloc[0],
        cooling_tower_1_df.iloc[1],

        # Cris-cross swap: Area A --> Area B, Area B --> Area A
        cooling_tower_1_df.iloc[1],
        cooling_tower_1_df.iloc[0],

        # Area A --> Area D, Area B --> Area E
        cooling_tower_2_df.iloc[0],
        cooling_tower_2_df.iloc[1],

        # Area A --> Area E, Area B --> Area F
        cooling_tower_2_df.iloc[1],
        cooling_tower_2_df.iloc[2],
    ])

    swap_groups_df.reset_index(drop=True, inplace=True)

    swap_groups_df['Swap Group'] = pd.Series([1, 1, 2, 2, 3, 3, 4, 4])
    swap_groups_df['Swap Out'] = pd.Series([
        swappable_assets_df.iloc[0],
        swappable_assets_df.iloc[1],
        swappable_assets_df.iloc[0],
        swappable_assets_df.iloc[1],
        swappable_assets_df.iloc[0],
        swappable_assets_df.iloc[1],
        swappable_assets_df.iloc[0],
        swappable_assets_df.iloc[1]
    ])

    swap_df = spy.swap(pushed_items_df, swap_groups_df)
    headers = sorted(swap_df['Header'].to_list())
    assert sorted(headers[0].split(', ')) == ['Cooling Tower 1 >> Area A', 'Cooling Tower 1 >> Area B']
    assert sorted(headers[1].split(', ')) == ['Cooling Tower 1 >> Area A', 'Cooling Tower 1 >> Area B']
    assert sorted(headers[2].split(', ')) == ['Cooling Tower 2 >> Area D', 'Cooling Tower 2 >> Area E']
    assert sorted(headers[3].split(', ')) == ['Cooling Tower 2 >> Area E', 'Cooling Tower 2 >> Area F']
    swapped_pull_df = spy.pull(swap_df, start='2021-01-01', end='2021-01-01')
    unswapped_pull_df = spy.pull(push_df, start='2021-01-01', end='2021-01-01')
    swapped_values = swapped_pull_df.iloc[0].to_list()
    unswapped_values = unswapped_pull_df.iloc[0].to_list()
    assert push_df.iloc[0]['ID'] == swap_df.iloc[0]['ID']
    assert swapped_values[0] == unswapped_values[0]
    assert swapped_values[1] != swapped_values[2] != swapped_values[3] != unswapped_values[0]

    swap_groups_df['Swap Out'] = pd.Series([
        swappable_assets_df.iloc[0]['ID'],
        swappable_assets_df.iloc[1]['ID'],
        swappable_assets_df.iloc[0]['ID'],
        swappable_assets_df.iloc[1]['ID'],
        swappable_assets_df.iloc[0]['ID'],
        swappable_assets_df.iloc[1]['ID'],
        swappable_assets_df.iloc[0]['ID'],
        swappable_assets_df.iloc[1]['ID']
    ])

    swap2_df = spy.swap(pushed_items_df, swap_groups_df)
    swapped2_pull_df = spy.pull(swap2_df, start='2021-01-01', end='2021-01-01', header='ID')
    swapped2_values = swapped2_pull_df.iloc[0].to_list()
    for i in range(len(swapped_values)):
        assert swapped_values[i] == swapped2_values[i]

    swap_groups_df['Swap Out'] = pd.Series([
        swappable_assets_df.iloc[0]['Asset'],
        swappable_assets_df.iloc[1]['Asset'],
        swappable_assets_df.iloc[0]['Asset'],
        swappable_assets_df.iloc[1]['Asset'],
        swappable_assets_df.iloc[0]['Asset'],
        swappable_assets_df.iloc[1]['Asset'],
        swappable_assets_df.iloc[0]['Asset'],
        swappable_assets_df.iloc[1]['Asset']
    ])

    swap3_df = spy.swap(pushed_items_df, swap_groups_df)
    swapped3_pull_df = spy.pull(swap3_df, start='2021-01-01', end='2021-01-01', header='ID')
    swapped3_values = swapped3_pull_df.iloc[0].to_list()
    for i in range(len(swapped_values)):
        assert swapped_values[i] == swapped3_values[i]

    swap_groups_df.at[0, 'Swap Out'] = 'Bad Asset'
    with pytest.raises(SPyRuntimeError, match='could not be matched against'):
        spy.swap(pushed_items_df, swap_groups_df)

    swap4_df = spy.swap(pushed_items_df, swap_groups_df, errors='catalog')
    bad_asset = swap4_df.iloc[0]
    swapped_d_e = swap4_df.iloc[2]
    swapped_e_f = swap4_df.iloc[3]
    assert 'could not be matched against' in bad_asset['Result']
    assert 'Success' == swapped_d_e['Result']
    assert 'Success' == swapped_e_f['Result']

    assert swapped_d_e['Swap Performed'] == (
        'Example >> Cooling Tower 1 >> Area A --> Example >> Cooling Tower 2 >> Area D\n'
        'Example >> Cooling Tower 1 >> Area B --> Example >> Cooling Tower 2 >> Area E')

    assert swapped_e_f['Swap Performed'] == (
        'Example >> Cooling Tower 1 >> Area A --> Example >> Cooling Tower 2 >> Area E\n'
        'Example >> Cooling Tower 1 >> Area B --> Example >> Cooling Tower 2 >> Area F')


@pytest.mark.system
def test_multilevel_swap():
    # This test creates a tree that looks like this:
    #
    #    test_multilevel_swap >> Area A >> Raw      >> Temperature
    #                                      Cleansed >> Temperature
    #                            Area B >> Raw      >> Temperature
    #                                      Cleansed >> Temperature
    #                            Area C >> Raw      >> Temperature
    #                                      Cleansed >> Optimizer
    #
    # and a calculated signal that sums the Raw and Cleansed temperature signals for Area A.
    #
    # Then it swaps out Area A for Area B, which necessarily needs to use the multi-level asset swapping capabilities
    # in Appserver.

    workbook = 'test_multilevel_swap'

    area_ab_df = spy.search({
        'Path': 'Example >> Cooling Tower 1',
        'Asset': '/Area [ABC]/',
        'Name': 'Temperature'
    }, old_asset_format=False)

    push_df_1 = area_ab_df.copy()
    push_df_1['Path'] = workbook + ' >> Areas >> ' + push_df_1['Asset']
    push_df_1['Asset'] = 'Raw'
    push_df_1['Reference'] = True

    push_df_2 = area_ab_df.copy()
    push_df_2['Path'] = workbook + ' >> Areas >> ' + push_df_2['Asset']
    push_df_2['Asset'] = 'Cleansed'
    push_df_2['Reference'] = True

    push_df_2.at[push_df_2['Path'].str.endswith('Area C').idxmax(), 'Name'] = 'Optimizer'

    push_df_3 = pd.DataFrame([{
        'Name': 'Raw Plus Cleansed',
        'Type': 'Signal',
        'Formula': '$r + $c',
        'Formula Parameters': {
            '$r': {'Path': workbook + ' >> Areas >> Area A', 'Asset': 'Raw',
                   'Name': 'Temperature'},
            '$c': {'Path': workbook + ' >> Areas >> Area A', 'Asset': 'Cleansed',
                   'Name': 'Temperature'},
        }
    }])

    push_df = pd.concat([push_df_1, push_df_2, push_df_3], ignore_index=True).reset_index(drop=True)

    pushed_signals_df = spy.push(metadata=push_df, workbook=workbook, datasource=workbook, worksheet=None)

    calc_df = spy.search({'Name': 'Raw Plus Cleansed'}, workbook=workbook, include_swappable_assets=True,
                         old_asset_format=False)

    pushed_signals_df.reset_index(inplace=True, drop=True)
    pushed_signals_df.dropna(inplace=True, subset=['Path'])
    area_a_df = pushed_signals_df[pushed_signals_df['Asset'] == 'Area A']
    area_b_df = pushed_signals_df[pushed_signals_df['Asset'] == 'Area B']
    area_c_df = pushed_signals_df[pushed_signals_df['Asset'] == 'Area C']

    # Create a Swap In / Swap Out pairing that uses the unique level of the tree, namely:
    #  Swap Out:  test_multilevel_swap >> Example >> Cooling Tower 1 >> Area A
    #  Swap In:   test_multilevel_swap >> Example >> Cooling Tower 1 >> Area B
    assets_df = area_b_df.copy()
    assets_df['Swap Out'] = [area_a_df.iloc[0]]

    def _assert_header(_header):
        assert sorted(_header.split(', ')) == ['Area B >> Cleansed', 'Area B >> Raw']

    swap_results = spy.swap(calc_df, assets_df)
    _assert_header(swap_results['Header'].iloc[0])

    swapped_pull_df = spy.pull(swap_results, start='2021-01-01', end='2021-01-01', header='ID')
    unswapped_pull_df = spy.pull(calc_df, start='2021-01-01', end='2021-01-01', header='ID')

    assert swapped_pull_df.iloc[0].to_list() != unswapped_pull_df.iloc[0].to_list()

    expected_swap_performed = sorted([f'{workbook} >> Areas >> Area A >> Cleansed '
                                      f'--> {workbook} >> Areas >> Area B >> Cleansed',
                                      f'{workbook} >> Areas >> Area A >> Raw '
                                      f'--> {workbook} >> Areas >> Area B >> Raw'])

    actual_swap_performed = sorted(swap_results.iloc[0]['Swap Performed'].split('\n'))
    assert actual_swap_performed == expected_swap_performed

    # Now specify the Swap Out via partial paths
    assets_df['Swap Out'] = 'Area A'
    swap_results = spy.swap(calc_df, assets_df)
    _assert_header(swap_results['Header'].iloc[0])
    actual_swap_performed = sorted(swap_results.iloc[0]['Swap Performed'].split('\n'))
    assert actual_swap_performed == expected_swap_performed

    assets_df['Swap Out'] = 'test_multilevel_swap >> Areas >> Area A'
    swap_results = spy.swap(calc_df, assets_df)
    _assert_header(swap_results['Header'].iloc[0])
    actual_swap_performed = sorted(swap_results.iloc[0]['Swap Performed'].split('\n'))
    assert actual_swap_performed == expected_swap_performed

    # Partial case:
    #  Swap Out:  test_multilevel_swap >> Example >> Cooling Tower 1 >> Area A
    #  Swap In:   test_multilevel_swap >> Example >> Cooling Tower 1 >> Area C
    assets_df = area_c_df.copy()
    assets_df['Swap Out'] = [area_a_df.iloc[0]]
    with pytest.raises(SPyRuntimeError, match='the best match is only 50% similar'):
        spy.swap(calc_df, assets_df)

    swap_results = spy.swap(calc_df, assets_df, partial_swaps_ok=True)
    assert swap_results['Header'].iloc[0] == 'Area C'

    expected_swap_performed = sorted([f'{workbook} >> Areas >> Area A >> Raw '
                                      f'--> {workbook} >> Areas >> Area C >> Raw'])

    actual_swap_performed = sorted(swap_results.iloc[0]['Swap Performed'].split('\n'))
    assert actual_swap_performed == expected_swap_performed

    # Error case:
    #  Swap Out:  test_multilevel_swap >> Area A
    #  Swap In:   Example >> Cooling Tower 1 >> Area D
    area_d_df = spy.search({
        'Path': 'Example >> Cooling Tower 2',
        'Name': 'Area D',
        'Type': 'Asset'
    }, old_asset_format=False)
    area_d_df['Swap Out'] = [area_a_df.iloc[0]]
    swap_results = spy.swap(calc_df, area_d_df, errors='catalog')
    swap_result = swap_results.iloc[0]['Result']

    assert 'could not be matched against any of the following swappable assets' in swap_result
    assert 'test_multilevel_swap >> Areas >> Area A >> Cleansed' in swap_result
    assert 'test_multilevel_swap >> Areas >> Area A >> Raw' in swap_result


@pytest.mark.system
def test_tutorial():
    workbook = 'test_swap.test_tutorial'

    area_a_df = spy.search({
        'Path': 'Example >> Cooling Tower 1',
        'Asset': 'Area A',
        'Name': 'Temperature'
    })

    pushed_items_df = spy.push(metadata=pd.DataFrame([{
        'Name': f'Added',
        'Formula': '$t + 10',
        'Formula Parameters': {'$t': area_a_df.iloc[0]}
    }, {
        'Name': f'Hot',
        'Formula': '$t > 80',
        'Formula Parameters': {'$t': area_a_df.iloc[0]}
    }, {
        'Name': f'Average on One Day in 2016',
        'Formula': "$t.average(capsule('2016-12-18'))",
        'Formula Parameters': {'$t': area_a_df.iloc[0]}
    }]), workbook=workbook, datasource=workbook, worksheet=None)

    areas_def_df = spy.search({
        'Path': 'Example >> Cooling Tower 2',
        'Name': '/Area [DEF]/',
        'Type': 'Asset'
    }, old_asset_format=False)

    swapped_df = spy.swap(pushed_items_df, areas_def_df, errors='catalog')

    successful_swaps_df = swapped_df[swapped_df['Result'] == 'Success']

    data_df = spy.pull(successful_swaps_df, start='2021-01-01', end='2021-01-02')

    assert len(data_df) > 0
    assert sorted(data_df.columns.to_list()) == [
        'Cooling Tower 2 >> Area D >> Added',
        'Cooling Tower 2 >> Area D >> Average on One Day in 2016',
        'Cooling Tower 2 >> Area D >> Hot',
        'Cooling Tower 2 >> Area E >> Added',
        'Cooling Tower 2 >> Area E >> Average on One Day in 2016',
        'Cooling Tower 2 >> Area E >> Hot',
    ]

    #
    # Multi-asset swaps
    #

    areas_df = spy.search({
        'Path': 'Example >> Cooling Tower 1',
        'Asset': '/Area [AB]/',
        'Name': 'Compressor Power'
    })

    spy.push(metadata=pd.DataFrame([{
        'Name': 'Added Together',
        'Formula': '$a + $b',
        'Formula Parameters': {
            '$a': areas_df[areas_df['Asset'] == 'Area A'],
            '$b': areas_df[areas_df['Asset'] == 'Area B']
        }
    }]), workbook=workbook, datasource=workbook, worksheet=None)

    added_together_df = spy.search({
        'Name': 'Added Together'
    }, include_swappable_assets=True, workbook=workbook)

    swappable_assets_df = added_together_df.iloc[0]['Swappable Assets']

    # Grab a DataFrame that contains Areas D, E and F
    cooling_tower_2_df = spy.search({
        'Path': 'Example >> Cooling Tower 2',
        'Type': 'Asset'
    }, old_asset_format=False).sort_values(by='Name')

    # Create a DataFrame that includes the combination of areas:
    swap_groups_df = pd.DataFrame([
        cooling_tower_2_df.iloc[0],  # Area D
        cooling_tower_2_df.iloc[1],  # Area E

        cooling_tower_2_df.iloc[0],  # Area D
        cooling_tower_2_df.iloc[2]])  # Area F

    # Now we'll specify our "Swap Group" and "Swap Out" columns. The Swap Group column can be any signifier you wish to use
    # to group the swaps together. Any rows with the same value for the Swap Group will be grouped together for swapping
    # purposes.
    swap_groups_df['Swap Group'] = [1, 1, 2, 2]

    # The "Swap Out" column can be an asset ID string, the latter part of an asset's path, or a DataFrame row for an asset.
    # It must be for an asset that is part of the "Swappable Assets" found above.
    swap_groups_df['Swap Out'] = [
        swappable_assets_df.iloc[0],  # Area A
        swappable_assets_df.iloc[1],  # Area B

        swappable_assets_df.iloc[0],  # Area A
        swappable_assets_df.iloc[1],  # Area B
    ]

    swapped_df = spy.swap(added_together_df, swap_groups_df, old_asset_format=False)

    data_df = spy.pull(swapped_df, start='2021-01-01', end='2021-01-02')

    assert len(data_df) > 0
    assert sorted(data_df.columns.to_list()) == [
        'Cooling Tower 2 >> Area D, Cooling Tower 2 >> Area E',
        'Cooling Tower 2 >> Area D, Cooling Tower 2 >> Area F'
    ]

    #
    # Multi-level swaps
    #

    area_ab_df = spy.search({
        'Path': 'Example >> Cooling Tower 1',
        'Asset': '/Area [AB]/',
        'Name': 'Temperature'
    }, old_asset_format=False)

    raw_signals = area_ab_df.copy()
    raw_signals['Path'] = 'Multi-level Swap Example >> ' + raw_signals['Asset']
    raw_signals['Asset'] = 'Raw'
    raw_signals['Reference'] = True

    cleansed_signals = area_ab_df.copy()
    cleansed_signals['Path'] = 'Multi-level Swap Example >> ' + cleansed_signals['Asset']
    cleansed_signals['Asset'] = 'Cleansed'
    cleansed_signals['Reference'] = True

    combined_signal = pd.DataFrame([{
        'Name': 'Raw and Cleansed Averaged',
        'Type': 'Signal',
        'Formula': '($r + $c) / 2',
        'Formula Parameters': {
            '$r': {'Path': 'Multi-level Swap Example >> Area A', 'Asset': 'Raw', 'Name': 'Temperature'},
            '$c': {'Path': 'Multi-level Swap Example >> Area A', 'Asset': 'Cleansed', 'Name': 'Temperature'}
        }
    }])

    pushed_items_df = spy.push(metadata=pd.concat([raw_signals, cleansed_signals, combined_signal], ignore_index=True),
                               workbook=workbook, datasource=workbook, worksheet=None)

    raw_plus_cleansed = pushed_items_df[pushed_items_df['Name'] == 'Raw and Cleansed Averaged']
    area_a_df = pushed_items_df[(pushed_items_df['Type'] == 'Asset') & (pushed_items_df['Name'] == 'Area A')]
    area_b_df = pushed_items_df[(pushed_items_df['Type'] == 'Asset') & (pushed_items_df['Name'] == 'Area B')]

    # Create a Swap In / Swap Out pairing that uses the unique level of the tree, namely:
    #  Swap Out:  test_multilevel_swap >> Example >> Cooling Tower 1 >> Area A
    #  Swap In:   test_multilevel_swap >> Example >> Cooling Tower 1 >> Area B

    assets_df = area_b_df.copy()  # Swap In
    assets_df['Swap Out'] = [area_a_df.iloc[0]]  # Swap Out

    swap_results = spy.swap(raw_plus_cleansed, assets_df)

    assert len(swap_results) == 1
    assert sorted(swap_results['Header'].iloc[0].split(', ')) == ['Area B >> Cleansed', 'Area B >> Raw']


@pytest.mark.system
def test_189_compatibility():
    items_df = spy.search({
        'Path': 'Example >> Cooling Tower 1',
        'Asset': 'Area A',
        'Name': 'Temperature'
    })
    area_a_df = spy.search({
        'Path': 'Example >> Cooling Tower 1',
        'Type': 'Asset',
        'Name': 'Area A'
    })
    try:
        spy.options.compatibility = 189
        swap_df = spy.swap(items_df, area_a_df)
        assert swap_df.spy.status.message == 'Success: Returned 1 swapped item(s).'
        assert 'Swap Result' in swap_df.columns
        assert 'Result' not in swap_df.columns
        assert swap_df.iloc[0]['Swap Result'] == 'Success'
    finally:
        spy.options.compatibility = None


@pytest.mark.system
def test_quiet_arg():
    items_df = spy.search({
        'Path': 'Example >> Cooling Tower 1',
        'Asset': 'Area A',
        'Name': 'Temperature'
    })
    area_a_df = spy.search({
        'Path': 'Example >> Cooling Tower 1',
        'Type': 'Asset',
        'Name': 'Area A'
    })
    swap_df = spy.swap(items_df, area_a_df, quiet=True)
    assert swap_df.spy.status.quiet
    assert swap_df.spy.status.message == 'Success: Returned 1 swapped item(s).'


@pytest.mark.system
def test_empty_dataframe():
    empty = pd.DataFrame(columns=['ID', 'Type', 'Path', 'Asset', 'Name'])
    items_df = spy.search({
        'Path': 'Example >> Cooling Tower 1',
        'Asset': 'Area A',
        'Name': 'Temperature'
    })
    area_a_df = spy.search({
        'Path': 'Example >> Cooling Tower 1',
        'Type': 'Asset',
        'Name': 'Area A'
    })

    swap_df = spy.swap(items_df, empty)
    assert swap_df.spy.status.message == 'Success: Returned 0 swapped item(s).'

    swap_df = spy.swap(empty, area_a_df)
    assert swap_df.spy.status.message == 'Success: Returned 0 swapped item(s).'


@pytest.mark.system
def test_interrupt_swap():
    items_df = spy.search({'Path': 'Example >> Cooling Tower 1 >> Area A', 'Name': 'Temperature'})
    assets_df = spy.search({'Path': 'Example >> Cooling Tower 1', 'Type': 'Asset'})
    with mock.patch('seeq.sdk.ItemsApi.find_swap', side_effect=KeyboardInterrupt):
        with pytest.raises(KeyboardInterrupt):
            spy.swap(items_df, assets_df)

        with pytest.raises(KeyboardInterrupt):
            spy.swap(items_df, assets_df, errors='catalog')

    with mock.patch('seeq.sdk.ItemsApi.find_swap', side_effect=ValueError):
        with pytest.raises(ValueError):
            spy.swap(items_df, assets_df)

        swapped_df = spy.swap(items_df, assets_df, errors='catalog')
        assert 'ValueError' in swapped_df.iloc[0]['Result']
