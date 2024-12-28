import itertools
import os
import pathlib
import re
import tempfile

import numpy as np
import pandas as pd
import pytest

from seeq import spy
from seeq.base import util
from seeq.sdk import *
from seeq.spy import _common
from seeq.spy._context import WorkbookContext
from seeq.spy.assets import Tree
from seeq.spy.assets._trees import _constants, _csv, _path
from seeq.spy.tests import test_common
from seeq.spy.tests.test_common import Sessions


def assert_frame_equal(df1, df2):
    # noinspection PyProtectedMember
    return pd._testing.assert_frame_equal(df1.sort_index(axis=1),
                                          df2.sort_index(axis=1),
                                          check_dtype=False,
                                          check_frame_type=False)


def setup_module():
    test_common.initialize_sessions()


@pytest.mark.system
def test_double_push_handling_metadata():
    workbook = 'test_double_push_handling_metadata'
    tree_name = f'{workbook}_{_common.new_placeholder_guid()}'
    tree = Tree(tree_name, workbook=workbook, datasource=workbook)
    example_calc = spy.search({'Name': 'Area A_Temperature', 'Datasource ID': 'Example Data'},
                              workbook=spy.GLOBALS_ONLY)
    example_calc_id = example_calc.iloc[0]['ID']
    tree.insert(pd.DataFrame([{
        'Name': 'calc',
        'Type': 'Condition',
        'Formula': '$x > 100',
        'Formula Parameters': [f'x={example_calc_id}']
    }]))
    tree.push()
    tree.push()
    expected = list()
    expected.append({
        'Name': tree_name,
        'Type': 'Asset',
        'Path': '',
        'Push Result': 'Success'
    })
    expected.append({
        'Name': 'calc',
        'Type': 'CalculatedCondition',
        'Formula': '$x > 100',
        'Formula Parameters': {'$x': example_calc_id}
    })


def test_my_csv_tree_with_incremental_push():
    # This mimics the code found in the SPy Trees tutorial notebook
    workbook = 'test_my_csv_tree ' + _common.new_placeholder_guid()
    csv_file = (pathlib.Path(__file__).absolute().parent
                / ".." / ".." / ".." / "docs" / "Documentation" / "Support Files" / "spy_tree_example.csv")
    my_csv_tree = spy.assets.Tree(str(csv_file), workbook=workbook, datasource=workbook)
    my_csv_tree.insert(name='Dew Point',
                       formula='$t - ((100 - $rh.setUnits(""))/5)',
                       # From https://iridl.ldeo.columbia.edu/dochelp/QA/Basic/dewpoint.html
                       formula_parameters={'$t': 'Temperature', '$rh': 'Relative Humidity'},
                       parent='Area ?')
    push_result = my_csv_tree.push()
    assert push_result['Push Result'].drop_duplicates().to_list() == ['Success']
    assert push_result.spy.datasource.name == workbook

    with tempfile.TemporaryDirectory() as temp_dir:
        pickle_file_name = os.path.join(temp_dir, f'{workbook}.pickle.zip')
        push_result = my_csv_tree.push(metadata_state_file=pickle_file_name)
        assert push_result['Push Result'].drop_duplicates().to_list() == ['Success']
        push_result = my_csv_tree.push(metadata_state_file=pickle_file_name)
        assert push_result['Push Result'].drop_duplicates().to_list() == ['Success: Unchanged']


@pytest.mark.system
def test_insert_calculation_regex_several_asset_levels_and_regex_glob_combo():
    workbook = 'test_insert_calculation_regex_several_asset_levels_and_regex_glob_combo'
    tree_name = f'{workbook}_{_common.new_placeholder_guid()}'
    tree = Tree(tree_name, workbook=workbook, datasource=workbook)
    example_calc = spy.search({'Name': 'Area A_Temperature', 'Datasource ID': 'Example Data'},
                              workbook=spy.GLOBALS_ONLY)
    example_calc_id = example_calc.iloc[0]['ID']

    tree.insert('Asset 1')
    tree.insert('Asset 2', parent='Asset 1')
    tree.insert('Asset 3', parent='Asset 2')

    tree.insert(pd.DataFrame([{
        'Name': "Where's Waldo",
        'Formula Parameters': [f'x={example_calc_id}'],
        'Formula': '$x'
    }]), parent='Asset 3')

    tree.insert(None, 'Asset 2', friendly_name='Guess 1 Level', formula_parameters='x=a* >> w*', formula='$x')

    tree.insert(pd.DataFrame([{
        'Name': 'Guess 2 Levels',
        'Formula Parameters': "x=T?[A-Z][a-z]* 2 >> Asset 3|4 >> w*",  # Asset 2 >> Asset 3 >> WW
        'Formula': '$x'
    }]), parent='Asset 1')

    push_result = tree.push()
    calc_id = push_result.loc[4, 'ID']

    expected_result = list()
    expected_result.append({
        'Name': tree_name,
        'Path': '',
        'Type': 'Asset',
        'Push Result': 'Success'
    })
    expected_result.append({
        'Name': 'Asset 1',
        'Path': tree_name,
        'Type': 'Asset',
        'Push Result': 'Success'
    })
    expected_result.append({
        'Name': 'Asset 2',
        'Path': f'{tree_name} >> Asset 1',
        'Type': 'Asset',
        'Push Result': 'Success'
    })
    expected_result.append({
        'Name': 'Asset 3',
        'Path': f'{tree_name} >> Asset 1 >> Asset 2',
        'Type': 'Asset',
        'Push Result': 'Success'
    })
    expected_result.append({
        'Name': "Where's Waldo",
        'Path': f'{tree_name} >> Asset 1 >> Asset 2 >> Asset 3',
        'Type': 'CalculatedSignal',
        'Formula Parameters': [f'x={example_calc_id}'],
        'Formula': '$x',
        'Push Result': 'Success'
    })
    expected_result.append({
        'Name': 'Guess 1 Level',
        'Path': f'{tree_name} >> Asset 1 >> Asset 2',
        'Type': 'CalculatedSignal',
        'Formula Parameters': [f'x={calc_id}'],
        'Formula': '$x',
        'Push Result': 'Success'
    })
    expected_result.append({
        'Name': 'Guess 2 Levels',
        'Path': f'{tree_name} >> Asset 1',
        'Type': 'CalculatedSignal',
        'Formula Parameters': [f'x={calc_id}'],
        'Formula': '$x',
        'Push Result': 'Success'
    })
    assert_dataframe_equals_expected(push_result, expected_result)

    expected_user_facing = list()
    expected_user_facing.append({
        'Name': tree_name,
        'Path': '',
        'Type': 'Asset'
    })
    expected_user_facing.append({
        'Name': 'Asset 1',
        'Path': tree_name,
        'Type': 'Asset'
    })
    expected_user_facing.append({
        'Name': 'Asset 2',
        'Path': f'{tree_name} >> Asset 1',
        'Type': 'Asset'
    })
    expected_user_facing.append({
        'Name': 'Asset 3',
        'Path': f'{tree_name} >> Asset 1 >> Asset 2',
        'Type': 'Asset'
    })
    expected_user_facing.append({
        'Name': "Where's Waldo",
        'Path': f'{tree_name} >> Asset 1 >> Asset 2 >> Asset 3',
        'Type': 'CalculatedSignal',
        'Formula Parameters': [f'x={example_calc_id}'],
        'Formula': '$x'
    })
    expected_user_facing.append({
        'Name': 'Guess 1 Level',
        'Path': f'{tree_name} >> Asset 1 >> Asset 2',
        'Type': 'CalculatedSignal',
        'Formula Parameters': [f'x={calc_id}'],
        'Formula': '$x'
    })
    expected_user_facing.append({
        'Name': 'Guess 2 Levels',
        'Path': f'{tree_name} >> Asset 1',
        'Type': 'CalculatedSignal',
        'Formula Parameters': [f'x={calc_id}'],
        'Formula': '$x'
    })


@pytest.mark.system
def test_insert_calculation_regex_failure_cases():
    workbook = 'test_insert_calculation_regex_failure_cases'
    tree_name = f'test_insert_calculation_regex_failure_cases_{_common.new_placeholder_guid()}'
    tree = Tree(tree_name, workbook=workbook, datasource=workbook)
    example_calc = spy.search({'Name': 'Area A_Temperature', 'Datasource ID': 'Example Data'},
                              workbook=spy.GLOBALS_ONLY)
    example_calc_id = example_calc.iloc[0]['ID']
    tree.insert('Buffer')
    tree.insert('Test Insert Regex', parent='Buffer')

    tree.insert(pd.DataFrame([{
        'Name': 'Same Name',
        'Formula Parameters': [f'x={example_calc_id}'],
        'Formula': '$x'
    }]), parent='Test Insert Regex')

    tree.insert(pd.DataFrame([{
        'Name': 'Same Name 2',
        'Formula Parameters': [f'x={example_calc_id}'],
        'Formula': '$x'
    }]), parent='Test Insert Regex')

    tree.insert(None, 'Test Insert Regex', friendly_name='Same Name 2', formula='$x',
                formula_parameters=[f'x={example_calc_id}'])

    with pytest.raises(RuntimeError, match='matches multiple items in tree'):
        tree.insert(pd.DataFrame([{
            'Name': 'Multiple Matches',
            'Formula Parameters': 'x=same*',
            'Formula': '$x'
        }]), parent='Test Insert Regex')

    with pytest.raises(RuntimeError, match='Formula parameters must be conditions, scalars, or signals.'):
        tree.insert(pd.DataFrame([{
            'Name': 'Guess Only First Asset',
            'Formula Parameters': 'x=test insert r*',
            'Formula': '$x'
        }]), parent='Buffer')

    with pytest.raises(Exception, match='Formula parameter is invalid, missing, or has been removed from tree'):
        tree.insert(pd.DataFrame([{
            'Name': 'Guess End',
            'Formula Parameters': f'x=.* Name$',
            'Formula': '$x'
        }]), parent='Buffer')

    with pytest.raises(Exception, match='Formula parameter is invalid, missing, or has been removed from tree'):
        tree.insert(pd.DataFrame([{
            'Name': 'Insert Invalid ID Param',
            'Formula Parameters': 'x=3F3ECW84G38J389SGH2H93848',
            'Formula': '$x'
        }]), parent='Buffer')


@pytest.mark.system
def test_insert_calculation_regex_glob_test():
    workbook = 'test_insert_calculation_regex_glob_test'
    tree_name = f'test_insert_calculation_regex_glob_test_{_common.new_placeholder_guid()}'
    tree = Tree(tree_name, workbook=workbook, datasource=workbook)
    example_calc = spy.search({'Name': 'Area A_Temperature', 'Datasource ID': 'Example Data'},
                              workbook=spy.GLOBALS_ONLY)
    example_calc_id = example_calc.iloc[0]['ID']
    tree.insert('Test Insert Regex')

    tree.insert(pd.DataFrame([{
        'Name': 'Test Calculation',
        'Formula Parameters': [f'x={example_calc_id}'],
        'Formula': '$x'
    }]), parent='Test Insert Regex')

    tree.insert(pd.DataFrame([{
        'Name': 'Test Glob Notation',
        'Formula Parameters': 'x=test calc*',
        'Formula': '$x'
    }]), parent='Test Insert Regex')

    tree.insert(pd.DataFrame([{
        'Name': 'Test Regex Notation',
        'Formula Parameters': 'x=T?est ?[A-Z][a-z]*',
        'Formula': '$x'
    }]), parent='Test Insert Regex')

    tree.insert(pd.DataFrame([{
        'Name': 'Test Regex Glob Combo',
        'Formula Parameters': 'x=t* C[a-z]lculation',
        'Formula': '$x'
    }]), parent='Test Insert Regex')
    result = tree.push()
    test_calc_id = result.loc[2, 'ID']

    expected_result = list()
    expected_result.append({
        'Name': tree_name,
        'Path': '',
        'Type': 'Asset',
        'Push Result': 'Success'
    })
    expected_result.append({
        'Name': 'Test Insert Regex',
        'Path': tree_name,
        'Type': 'Asset',
        'Push Result': 'Success'
    })
    expected_result.append({
        'Name': 'Test Calculation',
        'Path': f'{tree_name} >> Test Insert Regex',
        'Type': 'CalculatedSignal',
        'Formula Parameters': [f'x={example_calc_id}'],
        'Formula': '$x',
        'Push Result': 'Success'
    })
    expected_result.append({
        'Name': 'Test Glob Notation',
        'Path': f'{tree_name} >> Test Insert Regex',
        'Type': 'CalculatedSignal',
        'Formula Parameters': [f'x={test_calc_id}'],
        'Formula': '$x',
        'Push Result': 'Success'
    })
    expected_result.append({
        'Name': 'Test Regex Notation',
        'Path': f'{tree_name} >> Test Insert Regex',
        'Type': 'CalculatedSignal',
        'Formula Parameters': [f'x={test_calc_id}'],
        'Formula': '$x',
        'Push Result': 'Success'
    })
    expected_result.append({
        'Name': 'Test Regex Glob Combo',
        'Path': f'{tree_name} >> Test Insert Regex',
        'Type': 'CalculatedSignal',
        'Formula Parameters': [f'x={test_calc_id}'],
        'Formula': '$x',
        'Push Result': 'Success'
    })
    assert_dataframe_equals_expected(result, expected_result)

    expected = list()
    expected.append({
        'Name': tree_name,
        'Path': '',
        'Type': 'Asset'
    })
    expected.append({
        'Name': 'Test Insert Regex',
        'Path': tree_name,
        'Type': 'Asset'
    })
    expected.append({
        'Name': 'Test Calculation',
        'Path': f'{tree_name} >> Test Insert Regex',
        'Type': 'CalculatedSignal',
        'Formula Parameters': {'x': example_calc_id},
        'Formula': '$x'
    })
    expected.append({
        'Name': 'Test Glob Notation',
        'Path': f'{tree_name} >> Test Insert Regex',
        'Type': 'CalculatedSignal',
        'Formula Parameters': {'x': 'test calc*'},
        'Formula': '$x'
    })
    expected.append({
        'Name': 'Test Regex Notation',
        'Path': f'{tree_name} >> Test Insert Regex',
        'Type': 'CalculatedSignal',
        'Formula Parameters': {'x': 'T?est ?[A-Z][a-z]*'},
        'Formula': '$x'
    })
    expected.append({
        'Name': 'Test Regex Glob Combo',
        'Path': f'{tree_name} >> Test Insert Regex',
        'Type': 'CalculatedSignal',
        'Formula Parameters': {'x': 't* C[a-z]lculation'},
        'Formula': '$x'
    })
    assert_tree_equals_expected(tree, expected)


@pytest.mark.system
def test_insert_formulas_with_complex_formula_parameters():
    workbook = 'test_insert_formulas_with_complex_formula_parameters'
    tree_name = f'test_insert_formulas_with_complex_formula_parameters_{_common.new_placeholder_guid()}'
    tree = Tree(tree_name, workbook=workbook, datasource=workbook)
    result_a_df = spy.search({'Name': 'Area A_Temperature', 'Datasource ID': 'Example Data'})
    result_b_df = spy.search({'Name': 'Area B_Temperature', 'Datasource ID': 'Example Data'})
    result_c_df = spy.search({'Name': 'Area C_Temperature', 'Datasource ID': 'Example Data'})
    result_a_id = result_a_df.iloc[0]['ID']
    result_b_id = result_b_df.iloc[0]['ID']
    result_c_id = result_c_df.iloc[0]['ID']
    tree.insert('Test Asset')
    tree.insert('Test Asset Child', parent='Test Asset')

    # test formula parameters as list get converted to dict
    tree.insert(pd.DataFrame([{
        'Name': 'Insert By ID',
        'Formula Parameters': [f'x={result_a_id}', f'y={result_b_id}', f'z={result_c_id}'],
        'Formula': '$x + $y + $z'
    }]), parent='Test Asset Child')

    tree.insert(None, 'Test Asset Child', friendly_name='Insert Formula Params List', formula='$a + $b',
                formula_parameters=[f'a={result_a_id}', f'b={result_b_id}'])

    tree.insert(pd.DataFrame([{
        'Name': 'Insert Empty List',
        'Formula Parameters': [],
        'Formula': 'sinusoid()'
    }]), parent='Test Asset Child')

    # test formula parameters as string get converted to dict
    tree.insert(pd.DataFrame([{
        'Name': 'Sibling Name Match',
        'Formula Parameters': 'x=Insert By ID',
        'Formula': '$x + $x + $x'
    }]), parent='Test Asset Child')

    # test formula parameters as empty dict get converted to dict
    tree.insert(pd.DataFrame([{
        'Name': 'Insert empty params',
        'Formula Parameters': {},
        'Formula': 'sinusoid()'
    }]), parent='Test Asset Child')

    # check formula parameters set to NA get handled properly (set to empty dict)
    tree.insert(pd.DataFrame([{
        'Name': 'Insert with NA Params',
        'Formula Parameters': pd.NA,
        'Formula': 'sinusoid()'
    }]), parent='Test Asset Child')

    tree.insert(pd.DataFrame([{
        'Name': 'Insert By Name and Path',
        'Formula Parameters': ['x=Test Asset Child >> Insert By ID', f'z={result_c_id}',
                               f'y={result_b_id}'],
        'Formula': '$x + $z + $x + $z + $x'
    }]), parent='Test Asset')

    expected = list()
    expected.append({
        'Name': tree_name,
        'Path': '',
        'Type': 'Asset'
    })
    expected.append({
        'Name': 'Test Asset',
        'Path': tree_name,
        'Type': 'Asset'
    })
    expected.append({
        'Name': 'Test Asset Child',
        'Path': f'{tree_name} >> Test Asset',
        'Type': 'Asset'
    })
    expected.append({
        'Name': 'Insert By ID',
        'Path': f'{tree_name} >> Test Asset >> Test Asset Child',
        'Type': pd.NA,
        'Formula Parameters': {'x': result_a_id, 'y': result_b_id, 'z': result_c_id},
        'Formula': '$x + $y + $z'
    })
    expected.append({
        'Name': 'Insert Formula Params List',
        'Path': f'{tree_name} >> Test Asset >> Test Asset Child',
        'Type': pd.NA,
        'Formula Parameters': {'a': result_a_id, 'b': result_b_id},
        'Formula': '$a + $b'
    })
    expected.append({
        'Name': 'Insert Empty List',
        'Path': f'{tree_name} >> Test Asset >> Test Asset Child',
        'Type': pd.NA,
        'Formula Parameters': {},
        'Formula': 'sinusoid()'
    })
    expected.append({
        'Name': 'Sibling Name Match',
        'Path': f'{tree_name} >> Test Asset >> Test Asset Child',
        'Type': pd.NA,
        'Formula Parameters': {'x': 'Insert By ID'},
        'Formula': '$x + $x + $x'
    })
    expected.append({
        'Name': 'Insert empty params',
        'Path': f'{tree_name} >> Test Asset >> Test Asset Child',
        'Type': pd.NA,
        'Formula Parameters': {},
        'Formula': 'sinusoid()'
    })
    expected.append({
        'Name': 'Insert with NA Params',
        'Path': f'{tree_name} >> Test Asset >> Test Asset Child',
        'Type': pd.NA,
        'Formula Parameters': pd.NA,
        'Formula': 'sinusoid()'
    })
    expected.append({
        'Name': 'Insert By Name and Path',
        'Path': f'{tree_name} >> Test Asset',
        'Type': pd.NA,
        'Formula Parameters': {'x': 'Test Asset Child >> Insert By ID', 'z': result_c_id,
                               'y': result_b_id},
        'Formula': '$x + $z + $x + $z + $x'
    })
    assert_tree_equals_expected(tree, expected)

    tree.push()
    for i in range(0, len(expected)):
        if pd.isnull(expected[i]['Type']):
            expected[i]['Type'] = 'CalculatedSignal'
    assert_tree_equals_expected(tree, expected)


@pytest.mark.system
def test_create_tree_from_subtree_of_pushed_tree():
    workbook = 'test_create_tree_from_subtree_of_pushed_tree'

    tree1 = Tree('tree1', workbook=workbook, datasource=workbook)
    tree1.insert(spy.search({'Name': 'Cooling Tower 2', 'Path': 'Example', 'Datasource ID': 'Example Data'},
                            workbook=spy.GLOBALS_ONLY))
    tree1.push()

    tree2 = Tree(spy.search({'Name': 'Cooling Tower 2', 'Path': 'tree1', 'Datasource ID': workbook},
                            workbook=workbook),
                 workbook=workbook, datasource=workbook)
    tree2.push()

    df1 = tree1.df
    df2 = tree2.df
    assert len(df2) == len(df1) - 1
    assert not df2.ID.isin(df1.ID).any()
    assert list(df2['Referenced ID']) == list(df1.loc[1:, 'ID'])


@pytest.mark.system
def test_create_new_tree_then_repull_and_edit():
    workbook = 'test_create_new_tree_then_repull_and_edit'
    tree_name = f'{workbook}_{_common.new_placeholder_guid()}'
    tree = Tree(tree_name, workbook=workbook, datasource=workbook)
    tree.insert(['Cooling Tower 1', 'Cooling Tower 2'])
    tree.insert(children=['Area A', 'Area B', 'Area C'], parent='Cooling Tower 1')
    tree.insert(children=['Area E', 'Area F', 'Area G', 'Area H'], parent='Cooling Tower 2')
    tree.insert(children=['Temperature', 'Optimizer', 'Compressor'], parent=3)

    tower1_areas = ['Area A', 'Area B', 'Area C']
    tower2_areas = ['Area E', 'Area F', 'Area G', 'Area H']
    leaves = ['Temperature', 'Optimizer', 'Compressor']

    expected = list()
    expected.append({
        'Name': tree_name,
        'Path': '',
        'Type': 'Asset'
    })
    expected.append({
        'Name': 'Cooling Tower 1',
        'Path': tree_name,
        'Type': 'Asset'
    })
    expected.append({
        'Name': 'Cooling Tower 2',
        'Path': tree_name,
        'Type': 'Asset'
    })
    for area in tower1_areas:
        expected.append({
            'Name': area,
            'Path': f'{tree_name} >> Cooling Tower 1',
            'Type': 'Asset'
        })
        for leaf in leaves:
            expected.append({
                'Name': leaf,
                'Path': f'{tree_name} >> Cooling Tower 1 >> {area}',
                'Type': 'Asset'
            })
    for area in tower2_areas:
        expected.append({
            'Name': area,
            'Path': f'{tree_name} >> Cooling Tower 2',
            'Type': 'Asset'
        })
        for leaf in leaves:
            expected.append({
                'Name': leaf,
                'Path': f'{tree_name} >> Cooling Tower 2 >> {area}',
                'Type': 'Asset'
            })
    assert_tree_equals_expected(tree, expected)

    tree.push()
    assert not tree.df['ID'].isnull().values.any(), "Pushing should set the dataframe's ID for all items"
    assert not tree.df['Type'].isnull().values.any(), "Pushing should set the dataframe's Type for all items"
    search_results_df = spy.search({
        'Path': tree_name
    }, old_asset_format=True, workbook=workbook)
    expected.pop(0)  # Since we're searching using Path, the root node won't be retrieved.

    assert_search_results_equals_expected(search_results_df, expected)

    # Pull in the previously-created test_tree_system by name
    tree = Tree(tree_name, workbook=workbook, datasource=workbook)
    original_root_id, original_root_referenced_id = get_root_node_ids(tree)
    assert _common.is_guid(original_root_id), \
        f'Pulled root ID should be a GUID: {original_root_id}'
    assert str(original_root_referenced_id) == str(np.nan), \
        f'Pulled root Reference ID should be {np.nan}: {original_root_referenced_id}'

    expected_existing_items = 1 + 2 + 3 + 4 + (3 * 3) + (4 * 3)
    assert len(tree.df) == expected_existing_items, \
        f'Pulled tree items do not match count: Real={len(tree.df)}, Expected={expected_existing_items}'
    expected_nodes = create_expected_list_from_tree(tree)

    # Add a single node
    tree.insert(children='Area I', parent='Cooling Tower 2')
    expected_nodes.append({
        'Name': 'Area I',
        'Path': f'{tree_name} >> Cooling Tower 2',
        'Type': 'Asset'
    })
    expected_existing_items += 1
    assert_tree_equals_expected(tree, expected_nodes)
    tree.push()
    # The ID column should be fully filled in when the push occurs
    assert not tree.df['ID'].isnull().any()

    # Pull it again, but by ID
    tree2 = Tree(original_root_id, workbook=workbook, datasource=workbook)
    assert len(tree2.df) == expected_existing_items, \
        f'Edited tree items do not match count: Real={len(tree2.df)}, Expected={expected_existing_items}'
    updated_root_id, updated_root_referenced_id = get_root_node_ids(tree2)
    assert original_root_id == updated_root_id, \
        f'Pulled root ID should be the same as before: Original={original_root_id}, Updated={updated_root_id}'
    assert str(original_root_referenced_id) == str(np.nan), \
        f'Pulled root Reference ID should be the same as before: ' \
        f'Original={original_root_referenced_id}, Updated={updated_root_referenced_id}'
    assert_tree_equals_expected(tree2, expected_nodes)


@pytest.mark.system
def test_insert_referenced_single_item():
    # Setup: Find the IDs of actual Example Data items
    items_api = ItemsApi(spy.session.client)
    result = items_api.search_items(filters=['Name==Area A && Datasource ID==Example Data'],
                                    types=['Asset'])  # type: ItemSearchPreviewPaginatedListV1
    assert len(result.items) >= 1, 'There should be at least one global Area A asset'
    area_a_asset = result.items[0].id
    result = items_api.search_items(filters=['Name==Temperature'], types=['StoredSignal'], asset=area_a_asset)
    assert len(result.items) >= 1, 'There should be at least one global Area A Temperature signal'
    area_a_temperature = result.items[0].id

    # Test inserting an item by ID. It should be made into a reference.
    workbook = 'test_insert_referenced_single_item'
    tree_name = f'{workbook}_{_common.new_placeholder_guid()}'
    tree = Tree(tree_name, workbook=workbook, datasource=workbook)
    tree.insert(area_a_temperature)
    expected = [{
        'Name': tree_name,
        'Path': '',
        'Type': 'Asset'
    }, {
        'Referenced ID': area_a_temperature,
        'Name': 'Temperature',
        'Path': tree_name,
        'Type': 'CalculatedSignal',
        'Formula Parameters': {'signal': area_a_temperature},
    }]
    # Formula Parameters': f'signal={area_a_temperature}',
    assert_tree_equals_expected(tree, expected)
    # Inserting it again will result in no change
    tree.insert(area_a_temperature)
    assert_tree_equals_expected(tree, expected)

    # Test inserting a dataframe with a custom name and ID. It too should be made into a reference that is distinct
    # from the previous one.
    df = pd.DataFrame([{'Name': 'Temperature with new name', 'ID': area_a_temperature}])
    tree.insert(df)
    expected.append({
        'Referenced ID': area_a_temperature,
        'Name': 'Temperature with new name',
        'Path': tree_name,
        'Type': 'CalculatedSignal',
        'Formula Parameters': {'signal': area_a_temperature},
    })
    assert_tree_equals_expected(tree, expected)
    # Inserting it again will still result in no change
    tree.insert(df)
    assert_tree_equals_expected(tree, expected)

    # 'Friendly Name' should work in the same way as above.
    df = pd.DataFrame([{'Friendly Name': 'Temperature with friendly name', 'ID': area_a_temperature}])
    tree.insert(df)
    expected.append({
        'Referenced ID': area_a_temperature,
        'Name': 'Temperature with friendly name',
        'Path': tree_name,
        'Type': 'CalculatedSignal',
        'Formula Parameters': {'signal': area_a_temperature},
    })
    assert_tree_equals_expected(tree, expected)
    # Inserting it again will still result in no change
    tree.insert(df)
    assert_tree_equals_expected(tree, expected)


@pytest.mark.system
def test_insert_referenced_tree_item():
    # Setup: Find the IDs of actual Example Data items
    items_api = ItemsApi(spy.session.client)
    result = items_api.search_items(filters=['Name==Area A && Datasource ID==Example Data'], types=['Asset'])
    assert len(result.items) >= 1, 'There should be at least one global Area A asset'
    area_a_asset = result.items[0].id
    result = items_api.search_items(types=['StoredSignal'], asset=area_a_asset, order_by=['Name'])
    assert len(result.items) >= 5, 'There should be at least five global Area A signals'
    area_a_signals = list()
    for item in result.items:
        area_a_signals.append({'Name': item.name, 'ID': item.id})

    workbook = 'test_insert_referenced_single_item'
    tree_name = f'{workbook}_{_common.new_placeholder_guid()}'

    def create_expected_tuples(asset_name):
        expected_items = [{
            'Referenced ID': area_a_asset,
            'Name': asset_name,
            'Path': tree_name,
            'Type': 'Asset'
        }]
        for signal in area_a_signals:
            expected_items.append({
                'Referenced ID': signal['ID'],
                'Name': signal['Name'],
                'Path': f'{tree_name} >> {asset_name}',
                'Type': 'CalculatedSignal',
                'Formula Parameters': {'signal': signal['ID']}
            })
            # 'Formula Parameters': f"signal={signal['ID']}"
        return expected_items

    # Test inserting an asset by ID. It should be made into a reference and children pulled.
    tree = Tree(tree_name, workbook=workbook, datasource=workbook)
    tree.insert(area_a_asset)
    expected = [{
        'Name': tree_name,
        'Path': '',
        'Type': 'Asset'
    }] + create_expected_tuples('Area A')
    assert_tree_equals_expected(tree, expected)
    # Inserting it again will result in no change
    tree.insert(area_a_asset)
    assert_tree_equals_expected(tree, expected)

    # Test inserting a dataframe with a custom name and ID. It too should be made into a reference that is distinct
    # from the previous one.
    df = pd.DataFrame([{'Name': 'Area A with new name', 'ID': area_a_asset}])
    tree.insert(df)
    expected.extend(create_expected_tuples('Area A with new name'))
    assert_tree_equals_expected(tree, expected)
    # Inserting it again will still result in no change
    tree.insert(df)
    assert_tree_equals_expected(tree, expected)

    # 'Friendly Name' should work in the same way as above.
    df = pd.DataFrame([{'Friendly Name': 'Area A with friendly name', 'ID': area_a_asset}])
    tree.insert(df)
    expected.extend(create_expected_tuples('Area A with friendly name'))
    assert_tree_equals_expected(tree, expected)
    # Inserting it again will still result in no change
    tree.insert(df)
    assert_tree_equals_expected(tree, expected)

    # Inserting a mix of names+IDs should automatically figure out which is which. In this case, insert only
    # existing items. The lack of new rows will prove the resolution was successful (although some of the properties
    # will be lost on 'Area A with new name' due to this call no longer being a reference).
    tree.insert(['Area A with new name', area_a_asset])
    assert len(tree.df) == len(expected)


@pytest.mark.system
def test_remove_from_example_data():
    workbook = 'test_remove_from_example_data'
    tree = spy.assets.Tree('Example', workbook=workbook, datasource=workbook, friendly_name=workbook)
    tree.push()

    df = tree.df
    items_to_be_removed = df[(df.Name == 'Cooling Tower 1') | (df.Path.str.contains('Cooling Tower 1'))]

    status = spy.Status()
    tree.remove('Cooling Tower 1', status=status)
    assert status.df.squeeze()['Total Items Removed'] == 57

    df = tree.df
    assert not ((df['Name'] == 'Cooling Tower 1') | (df['Path'].str.contains('Cooling Tower 1'))).any()

    tree.push()

    items_api = ItemsApi(spy.session.client)
    for guid in items_to_be_removed['ID']:
        item_output = items_api.get_item_and_all_properties(id=guid)
        assert item_output.is_archived is True
    for guid in items_to_be_removed['Referenced ID']:
        item_output = items_api.get_item_and_all_properties(id=guid)
        assert item_output.is_archived is False


@pytest.mark.system
def test_comprehension_funcs_on_example_data():
    example = Tree('Example')

    assert example.height == 4
    assert example.size == 113

    counts = example.count()
    expected_counts = {
        'Asset': 19,
        'Signal': 94
    }
    for key in ['Asset', 'Signal']:
        assert counts[key] == expected_counts[key]
        assert example.count(key) == expected_counts[key]
    for key in ['Condition', 'Scalar', 'Formula']:
        assert example.count(key) == 0

    missing_items_dict = example.missing_items('dict')
    area_f = 'Example >> Cooling Tower 2 >> Area F'
    expected_missing_names = ['Compressor Stage', 'Optimizer', 'Relative Humidity', 'Temperature', 'Wet Bulb']
    assert len(missing_items_dict) == 1
    assert area_f in missing_items_dict
    assert len(missing_items_dict[area_f]) == 5
    for name in expected_missing_names:
        assert name in missing_items_dict[area_f]


@pytest.mark.system
def test_constructor_and_insert_tree_dataframe():
    root_name = 'test_constructor_and_insert_tree_dataframe'
    push_results = spy.push(metadata=pd.DataFrame([{
        'Name': root_name,
        'Type': 'Asset'
    }, {
        'Name': 'Leaf',
        'Type': 'Signal',
        'Path': root_name
    }]), workbook=root_name, worksheet=None, datasource=root_name)

    # The input properties (particularly the Referenced ID and Formula information) should come though
    root = {'Name': root_name,
            'Type': 'Asset',
            'Referenced ID': push_results.ID[0],
            'Path': '',
            'Depth': 1}
    signal = {'Name': 'Leaf',
              'Type': 'Signal',
              'Referenced ID': push_results.ID[1],
              'Formula': '$signal',
              'Formula Parameters': {'signal': _common.new_placeholder_guid()},
              'Path': root_name,
              'Depth': 2}
    expected = pd.DataFrame(columns=_constants.dataframe_columns)
    expected = pd.concat([expected, pd.DataFrame([root, signal])], ignore_index=True)
    tree = Tree(pd.DataFrame([root, signal]), workbook=root_name, datasource=root_name)
    assert_frame_equal(tree.df, expected)


@pytest.mark.system
def test_move_pushed_items():
    workbook = 'test_move_pushed_items'
    tree = Tree('Example', workbook=workbook, datasource=workbook, friendly_name=workbook)
    tree.push()

    before_df = tree.df.copy()
    tree.move('Area *', destination='Cooling Tower 2')
    after_df = tree.df.copy()

    for _, before_row in before_df.iterrows():
        if re.search(r'Area [A-Z]', _path.get_full_path(before_row)):
            after_row_query = (after_df.Path == before_row.Path.replace('1', '2')) & (after_df.Name == before_row.Name)
            assert len(after_df[after_row_query]) == 1
            after_row = after_df[after_row_query].iloc[0]
            # The following is the key part we are testing. We want the IDs to be reset only for things
            # that were actually moved.
            if 'Cooling Tower 1' in before_row.Path:
                assert pd.isnull(after_row.ID)
            elif 'Cooling Tower 2' in before_row.Path:
                assert _common.is_guid(after_row.ID)


@pytest.mark.system
def test_root_only_asset_tree_visible():
    # Insert a Tree that has no children.
    workbook = 'test_root_only_asset_tree_visible'
    trees_api = TreesApi(spy.client)
    tree = Tree(workbook, workbook=workbook, datasource=workbook)
    tree.push()
    roots = trees_api.get_tree_root_nodes(scope=[tree._workbook_id])
    result = [x.name for x in roots.children if workbook == x.name]
    assert len(result) == 1


@pytest.mark.system
def test_modify_existing_spy_tree_with_constructor():
    workbook = 'test_modify_existing_spy_tree_with_constructor'
    tree1 = Tree(pd.DataFrame([{
        'Name': 'root'
    }, {
        'Name': 'leaf 1',
        'Path': 'root >> asset'
    }, {
        'Name': 'leaf 2',
        'Path': 'root >> asset'
    }, {
        'Name': 'leaf 3',
        'Path': 'root >> asset >> asset to be modified'
    }]), workbook=workbook, datasource=workbook)
    tree1.push()

    # Because tree2 will be defined upon the items of tree1, it will pull and modify what we just pushed via tree1.
    # However, the dataframe input will include modifications that we expect to be reflected in the resulting tree
    #  object, so that spy.push doesn't fail to update pre-existing items in certain ways (changing name or path).

    tree2_df = tree1.df.copy()
    # Change the name of an existing item.
    # We expect the result to be a reference to the old item.
    tree2_df.loc[tree2_df.Name == 'leaf 1', 'Name'] = 'new leaf 1 name'
    # Change the path of an existing item.
    # We expect the result to be a reference to the old item.
    tree2_df.loc[tree2_df.Name == 'leaf 2', 'Path'] = 'root >> new leaf 2 path'
    # Rename an asset with children.
    # We expect the result to be a reference to the old asset, and all of the new children to be references to old
    # children.
    tree2_df.loc[tree2_df.Name == 'asset to be modified', 'Name'] = 'new asset name'
    tree2_df.loc[tree2_df.Name == 'leaf 3', 'Path'] = 'root >> asset >> new asset name'
    # Add a new item. We expect the result to be a fresh item.
    tree2_df = pd.concat(
        [tree2_df, pd.DataFrame([{'Name': 'additional leaf', 'Path': 'root >> asset'}])], ignore_index=True)

    tree2 = Tree(tree2_df, workbook=workbook, datasource=workbook)

    def tree1_id(name):
        rows = tree1.df[tree1.df.Name == name]
        if len(rows) != 1:
            raise RuntimeError('tree1 did not push correctly')
        return rows.ID.iloc[0]

    expected_df = pd.DataFrame([
        ['', 'root', 'Asset', tree1_id('root'), np.nan],
        ['root', 'asset', 'Asset', tree1_id('asset'), np.nan],
        ['root >> asset', 'additional leaf', 'Asset', np.nan, np.nan],
        ['root >> asset', 'new asset name', 'Asset', np.nan, tree1_id('asset to be modified')],
        ['root >> asset >> new asset name', 'leaf 3', 'Asset', np.nan, tree1_id('leaf 3')],
        ['root >> asset', 'new leaf 1 name', 'Asset', np.nan, tree1_id('leaf 1')],
        ['root', 'new leaf 2 path', 'Asset', np.nan, np.nan],
        ['root >> new leaf 2 path', 'leaf 2', 'Asset', np.nan, tree1_id('leaf 2')]
    ], columns=['Path', 'Name', 'Type', 'ID', 'Referenced ID'])

    assert_frame_equal(tree2.df[expected_df.columns], expected_df)

    # Assert equal after push as well, except for new IDs
    tree2.push()
    assert list(tree2.df.ID[expected_df.ID.notnull()]) == list(expected_df.ID[expected_df.ID.notnull()])
    columns_no_id = ['Path', 'Name', 'Type', 'Referenced ID']
    assert_frame_equal(tree2.df[columns_no_id], expected_df[columns_no_id])


@pytest.mark.system
def test_pull_calculations():
    area_a_temp_search = spy.search({'Name': 'Area A_Temperature'}, workbook=spy.GLOBALS_ONLY)
    assert len(area_a_temp_search) > 0
    area_a_temp_id = area_a_temp_search.ID[0]

    workbook = 'test_pull_calcs'
    tree_name = f'{workbook}_{_common.new_placeholder_guid()}'
    orig_tree = Tree(tree_name, workbook=workbook, datasource=workbook)
    orig_tree.insert(pd.DataFrame([{
        'Name': 'Calc with Parameters',
        'Formula': '$x + $x',
        'Formula Parameters': [f'x={area_a_temp_id}']
    }, {
        'Name': 'Condition Calc',
        'Formula': 'days()'
    }, {
        'Name': 'Scalar Calc',
        'Formula': '1'
    }, {
        'Name': 'Signal Calc',
        'Formula': 'sinusoid()'
    }]))
    orig_tree.push()
    orig_root_id = orig_tree.df.ID[0]

    expected_df = orig_tree.df.copy()
    for i in (2, 3, 4):
        expected_df.at[i, 'Formula Parameters'] = dict()

    # First pull the same tree without references
    tree1 = Tree(pd.DataFrame([{
        'ID': orig_root_id
    }]), workbook=workbook, datasource=workbook)
    assert_frame_equal(expected_df, tree1.df)

    # Then pull the same tree, but rename the root node so it is forced to become a reference
    tree2 = Tree(pd.DataFrame([{
        'Name': 'New Root Name',
        'ID': orig_root_id
    }]), workbook=workbook, datasource=workbook)
    df = tree2.df.copy()
    assert (df.Path.iloc[1:] == 'New Root Name').all()
    assert df.ID.isnull().all()
    assert list(df['Referenced ID']) == list(expected_df.ID)
    assert list(df['Formula'].iloc[1:]) == ['$signal', '$condition', '$scalar', '$signal']
    assert df['Formula Parameters'].iloc[1:].str.fullmatch(r'[a-z]+\=' + _common.GUID_REGEX).all()

    # Make sure that pulling the same tree but specifying the correct tree does not result in references
    tree3 = Tree(pd.DataFrame([{
        'Name': tree_name,
        'ID': orig_root_id
    }]), workbook=workbook, datasource=workbook)
    assert_frame_equal(expected_df, tree3.df)

    # Pull as references using Referenced ID column
    tree4 = Tree(pd.DataFrame([{
        'Name': tree_name,
        'Referenced ID': orig_root_id
    }]), workbook=workbook, datasource=workbook)
    # Assert that this is equal to tree2 except for the root name change
    df = tree2.df.copy()
    df['Name'] = df['Name'].str.replace('New Root Name', tree_name)
    df['Path'] = df['Path'].str.replace('New Root Name', tree_name)
    assert_frame_equal(tree4.df, df)


@pytest.mark.system
def test_remove_by_dataframe():
    example_data = spy.search({'Datasource ID': 'Example Data', 'Name': 'Example', 'Type': 'Asset'},
                              old_asset_format=True, workbook=spy.GLOBALS_ONLY)
    workbook = 'test_remove_by_dataframe'
    tree = Tree(example_data, workbook=workbook, datasource=workbook, friendly_name=workbook)

    df = tree.df
    tree_without_cooling_tower_1 = df[(~df['Path'].str.contains('Cooling Tower 1')) & (df['Name'] != 'Cooling Tower 1')]
    tree_without_cooling_tower_1.reset_index(drop=True, inplace=True)

    cooling_tower_1 = spy.search({'Datasource ID': 'Example Data', 'Name': 'Cooling Tower 1', 'Type': 'Asset'},
                                 workbook=spy.GLOBALS_ONLY)
    tree.remove(cooling_tower_1)

    assert_frame_equal(tree.df, tree_without_cooling_tower_1)


@pytest.mark.system
def test_friendly_name_example_data():
    example_data = spy.search([{'Datasource ID': 'Example Data', 'Name': 'Example', 'Type': 'Asset'},
                               {'Datasource ID': 'Example Data', 'Path': 'Example'}],
                              all_properties=True, workbook=spy.GLOBALS_ONLY)
    workbook = 'test_friendly_name_example_data'
    tree = Tree(example_data, friendly_name='{{Type}*Signal*()}{{Asset}}_{{Name}}',
                workbook=workbook, datasource=workbook)

    df = tree.df
    assert ((df['Type'].str.contains('Signal')) == (df['Name'].str.contains('_'))).all()


@pytest.mark.system
def test_csv_validation():
    # csv file that doesn't exist
    name = "midvale.csv"
    message = f"File {name} not found. Please ensure you have it in the correct working directory."
    with pytest.raises(ValueError, match=message):
        Tree(name)

    csv_dir = os.path.join(os.path.dirname(__file__), 'tree_csv_files')

    # csv with missing names
    message = f"Either 'Name' or 'ID' column must be complete, without missing values."
    with pytest.raises(ValueError, match=message):
        Tree(os.path.join(csv_dir, 'missing_names.csv'))

    # csv without a name or ID column
    message = f"A 'Name' or 'ID' column is required"
    with pytest.raises(ValueError, match=message):
        Tree(os.path.join(csv_dir, "no_name_col.csv"))

    # csv without a Level 1 column
    message = f"Levels columns or a path column must be provided"
    with pytest.raises(ValueError, match=message):
        Tree(os.path.join(csv_dir, "no_level1.csv"))


@pytest.mark.system
def test_csv_with_non_unique_names():
    # check for warning when search result should return more than a one-to-one matching
    warning = f"The following names returned multiple search results, so the first result was " \
              f"used: ['Compressor Stage']"
    status = spy.Status()
    csv_dir = os.path.join(os.path.dirname(__file__), 'tree_csv_files')
    Tree(os.path.join(csv_dir, 'multiple_search_results.csv'), status=status)
    assert warning in status.warnings


@pytest.mark.system
def test_csv_with_non_existent_names():
    # check for warning when search result should be missing an item from the csv
    warning = f"The following names did not return search results and were ignored: " \
              f"['Area A_Tempearture']"
    status = spy.Status()
    csv_dir = os.path.join(os.path.dirname(__file__), 'tree_csv_files')
    Tree(os.path.join(csv_dir, 'missing_search_results.csv'), status=status)
    assert warning in status.warnings

    # csv with only non-existent names
    error = re.escape("No items were found with the specified names: ['3254ff34516f', '9a8f8639a240']")
    with pytest.raises(ValueError, match=error):
        Tree(os.path.join(csv_dir, 'missing_all_search_results.csv'))


@pytest.mark.system
def test_csv_with_unsupported_type():
    # ensure we only search for allowed types from csv
    warnings = {"The following names did not return search results and were ignored: "
                "['CSV Trees Type Testing Workbook']",
                "The following names specify unsupported types and were ignored: ['My Workbook']"}
    workbook_body = WorkbookInputV1(name='CSV Trees Type Testing', owner_id=spy.user.id)
    WorkbooksApi(spy.client).create_workbook(body=workbook_body)

    status = spy.Status()
    csv_dir = os.path.join(os.path.dirname(__file__), 'tree_csv_files')

    Tree(os.path.join(csv_dir, 'unsupported_types.csv'), status=status)

    assert warnings.issubset(status.warnings)


@pytest.mark.system
def test_csv_creates():
    csv_dir = os.path.join(os.path.dirname(__file__), 'tree_csv_files')

    # set up the expected dataframe
    signal1_id = _get_first_id_from_signal_name('Area A_Compressor Power')
    signal2_id = _get_first_id_from_signal_name('Area B_Compressor Stage')
    root = {'Name': 'My Root',
            'Type': 'Asset',
            'Path': '',
            'Depth': 1}
    tower1 = {'Name': 'Cooling Tower A',
              'Type': 'Asset',
              'Path': 'My Root',
              'Depth': 2}
    tower2 = {'Name': 'Cooling Tower B',
              'Type': 'Asset',
              'Path': 'My Root',
              'Depth': 2}
    area1 = {'Name': 'Area 1',
             'Type': 'Asset',
             'Path': 'My Root >> Cooling Tower A',
             'Depth': 3}
    area2 = {'Name': 'Area 2',
             'Type': 'Asset',
             'Path': 'My Root >> Cooling Tower B',
             'Depth': 3}
    signal1 = {'Name': 'Area A_Compressor Power',
               'Type': 'CalculatedSignal',
               'Referenced ID': signal1_id,
               'Formula': '$signal',
               'Formula Parameters': {'signal': signal1_id},
               'Path': 'My Root >> Cooling Tower A >> Area 1',
               'Depth': 4}
    signal2 = {'Name': 'Area B_Compressor Stage',
               'Type': 'CalculatedSignal',
               'Referenced ID': signal2_id,
               'Formula': '$signal',
               'Formula Parameters': {'signal': signal2_id},
               'Path': 'My Root >> Cooling Tower B >> Area 2',
               'Depth': 4}

    expected = pd.DataFrame(columns=_constants.dataframe_columns)
    expected = pd.concat([expected, pd.DataFrame([root, tower1, area1, signal1, tower2, area2, signal2])],
                         ignore_index=True)

    # check simplest tree
    tree = Tree(os.path.join(csv_dir, 'simplest.csv'))
    assert_frame_equal(expected, tree.df)

    # write to a csv and read back from it
    filename = 'id_test.csv'
    filename_temp = 'id_test_temp.csv'

    csv_df = pd.read_csv(os.path.join(csv_dir, filename))
    csv_df['ID'] = [signal1_id, signal2_id]
    csv_df.to_csv(os.path.join(csv_dir, filename_temp), index=False)

    tree_id = Tree(os.path.join(csv_dir, filename_temp))
    csv_df['ID'] = ''
    csv_df.to_csv(os.path.join(csv_dir, filename_temp), index=False)

    assert_frame_equal(expected, tree_id.df)
    # Clean up the file that we've written
    util.safe_remove(os.path.join(csv_dir, filename_temp))

    # check that friendly names are used when provided
    # updated expected to use friendly names
    expected.at[3, 'Name'] = 'Compressor Power'
    expected.at[6, 'Name'] = 'Compressor Stage'
    tree_friendly = Tree(os.path.join(csv_dir, 'simple_friendly.csv'))
    assert_frame_equal(expected, tree_friendly.df)

    # check that forward-fill works as expected
    tree_ff = Tree(os.path.join(csv_dir, 'simple_forward_fill.csv'))
    assert_frame_equal(expected, tree_ff.df)


@pytest.mark.system
def test_get_ids_by_name_from_user_input():
    # set up the df to search on
    search1 = {'Name': 'Area A_Compressor Power',
               'Type': 'Signal',
               'Path': 'My Root >> Cooling Tower A >> Area 1',
               'Depth': 4}
    search2 = {'Name': 'Area B_Compressor Stage',
               'Type': 'Signal',
               'Path': 'My Root >> Cooling Tower B >> Area 2',
               'Depth': 4}
    search_df = pd.DataFrame()
    search_df = pd.concat([search_df, pd.DataFrame([search1, search2])], ignore_index=True)
    status = spy.Status()
    results = _csv.get_ids_by_name_from_user_input(search_df, status)

    # set up the expected dataframe
    signal1_id = _get_first_id_from_signal_name('Area A_Compressor Power')
    signal2_id = _get_first_id_from_signal_name('Area B_Compressor Stage')

    signal1 = {'Name': 'Area A_Compressor Power',
               'Type': 'Signal',
               'ID': signal1_id,
               'Path': 'My Root >> Cooling Tower A >> Area 1',
               'Depth': 4}
    signal2 = {'Name': 'Area B_Compressor Stage',
               'Type': 'Signal',
               'ID': signal2_id,
               'Path': 'My Root >> Cooling Tower B >> Area 2',
               'Depth': 4}

    expected = pd.DataFrame([signal1, signal2])
    assert_frame_equal(expected, results)


@pytest.mark.system
def test_process_csv_data():
    csv_dir = os.path.join(os.path.dirname(__file__), 'tree_csv_files')
    status = spy.Status()
    results = _csv.process_csv_data(os.path.join(csv_dir, 'simplest.csv'), status)

    signal1_id = _get_first_id_from_signal_name('Area A_Compressor Power')
    signal2_id = _get_first_id_from_signal_name('Area B_Compressor Stage')
    signal1 = {'Level 1': 'My Root',
               'Level 2': 'Cooling Tower A',
               'Level 3': 'Area 1',
               'Name': 'Area A_Compressor Power',
               'ID': signal1_id
               }
    signal2 = {'Level 1': 'My Root',
               'Level 2': 'Cooling Tower B',
               'Level 3': 'Area 2',
               'Name': 'Area B_Compressor Stage',
               'ID': signal2_id
               }

    expected = pd.DataFrame([signal1, signal2])
    assert_frame_equal(expected, results)


@pytest.mark.system
def test_double_push():
    workbook = 'test_double_push'
    tree = Tree('My Root', workbook=workbook, datasource=workbook)
    tree.insert('My Asset')
    tree.insert('My Signal', formula='sinusoid()', parent='My Asset')
    tree.insert('My Condition', formula='days()', parent='My Asset')
    tree.insert('My Scalar', formula='1', parent='My Asset')

    tree.push()
    df_after_first_push = tree.df.copy()

    tree.push()
    df_after_second_push = tree.df.copy()

    assert_frame_equal(df_after_second_push, df_after_first_push)

    search_results = spy.search(df_after_second_push[['ID']], all_properties=True, old_asset_format=True)
    assert_frame_equal(search_results[['Name', 'Type', 'ID', 'Formula']],
                       df_after_second_push[['Name', 'Type', 'ID', 'Formula']])
    pd.testing.assert_series_equal(search_results.apply(_path.determine_path, axis=1),
                                   df_after_second_push['Path'],
                                   check_dtype=False,
                                   check_names=False)


@pytest.mark.system
def test_metrics_id_params():
    workbook = 'test_metrics_id_params'
    tree_name = f'{workbook}_{_common.new_placeholder_guid()}'

    # Setup: Get the ID of a signal and a condition to use as inputs
    signal_id = spy.search({'Name': 'Area A_Temperature', 'Datasource ID': 'Example Data'}).iloc[0]['ID']
    condition_results = spy.push(metadata=pd.DataFrame([{
        'Name': 'test metrics basic condition',
        'Formula Parameters': [f'x={signal_id}'],
        'Formula': '$x < 85'
    }]), workbook=workbook, datasource=workbook)
    condition_id = condition_results.iloc[0]['ID']
    other_tree_name = f'{workbook}_{_common.new_placeholder_guid()}'
    metric_results = spy.push(metadata=pd.DataFrame([{
        'Name': other_tree_name,
        'Type': 'Asset'
    }, {
        'Name': 'test metrics basic metric',
        'Asset': other_tree_name,
        'Type': 'Metric',
        'Measured Item': signal_id,
        'Aggregation Function': 'percentile(25)',
        'Bounding Condition': condition_id,
        'Bounding Condition Maximum Duration': '48h'
    }]), workbook=workbook, datasource=workbook)
    asset_id = metric_results.iloc[0]['ID']
    metric_id = metric_results.iloc[1]['ID']

    tree = Tree(tree_name, workbook=workbook, datasource=workbook)
    tree.insert(['Asset 1', 'Asset 2', 'Asset 3', 'Asset 4', 'Asset 5', 'Asset 6'])

    # The most basic metric possible. Use ID-based params.
    basic_metric_df = pd.DataFrame([{
        'Name': 'Test Metric 1',
        'Type': 'Metric',
        'Measured Item': signal_id
    }])
    tree.insert(basic_metric_df, parent='Asset 1')

    # A continuous metric using all the available properties. Note that the Statistic uses 'Range' instead of 'range()'.
    continuous_metric_df = pd.DataFrame([{
        'Name': 'Test Metric 2',
        'Description': 'Testing metric inputs',
        'Type': 'ThresholdMetric',
        'Measured Item': signal_id,
        'Statistic': 'Range',
        'Duration': '2h',
        'Period': '1h',
        'Number Format': '#,##0.0000',
        'Process Type': 'Continuous',
        'Metric Neutral Color': '#ffffff',
        'Thresholds': {
            'HiHiHi#FF0000': 60,
            'HiHi': 40,
            'LoLo': signal_id
        }
    }])
    tree.insert(continuous_metric_df, parent='Asset 2')

    # A condition metric. Note that Process Type is not specified.
    condition_metric_df = pd.DataFrame([{
        'Name': 'Test Metric 3',
        'Type': 'Metric',
        'Measured Item': signal_id,
        'Aggregation Function': 'percentile(75)',
        'Bounding Condition': condition_id,
        'Bounding Condition Maximum Duration': '48h'
    }])
    tree.insert(condition_metric_df, parent='Asset 3')

    # Deep-copy an existing metric by ID
    tree.insert(metric_id, parent='Asset 4')

    # Deep-copy an existing metric by pulling in a parent asset by ID
    tree.insert(asset_id, parent='Asset 5')

    # Deep-copy an existing metric by ID from a partial dataframe
    partial_metric_df = pd.DataFrame([{
        'Name': 'Test Metric 4',
        'Type': 'Metric',
        'ID': metric_id
    }])
    tree.insert(partial_metric_df, parent='Asset 6')

    push_results = tree.push()

    expected_push_results = pd.DataFrame([
        ['', tree_name, 'Asset', np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
        [tree_name, 'Asset 1', 'Asset', np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
        [tree_name + ' >> Asset 1', 'Test Metric 1', 'ThresholdMetric',
         signal_id, np.nan, np.nan, np.nan, np.nan, np.nan],
        [tree_name, 'Asset 2', 'Asset', np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
        [tree_name + ' >> Asset 2', 'Test Metric 2', 'ThresholdMetric',
         signal_id, 'range()', '2h', '1h', '#,##0.0000', 'Continuous'],
        [tree_name, 'Asset 3', 'Asset', np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
        [tree_name + ' >> Asset 3', 'Test Metric 3', 'ThresholdMetric',
         signal_id, 'percentile(75)', np.nan, np.nan, np.nan, np.nan],
        [tree_name, 'Asset 4', 'Asset', np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
        [tree_name + ' >> Asset 4', 'test metrics basic metric', 'ThresholdMetric',
         signal_id, 'percentile(25)', np.nan, np.nan, np.nan, 'Condition'],
        [tree_name, 'Asset 5', 'Asset', np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
        [tree_name + ' >> Asset 5', other_tree_name, 'Asset', np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
        [tree_name + ' >> Asset 5 >> ' + other_tree_name, 'test metrics basic metric', 'ThresholdMetric',
         signal_id, 'percentile(25)', np.nan, np.nan, np.nan, 'Condition'],
        [tree_name, 'Asset 6', 'Asset', np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
        [tree_name + ' >> Asset 6', 'Test Metric 4', 'ThresholdMetric',
         signal_id, 'percentile(25)', np.nan, np.nan, np.nan, 'Condition']
    ], columns=['Path', 'Name', 'Type', 'Measured Item',
                'Aggregation Function', 'Duration', 'Period', 'Number Format', 'Process Type'])

    assert_frame_equal(push_results[['Path', 'Name', 'Type', 'Measured Item', 'Aggregation Function',
                                     'Duration', 'Period', 'Number Format', 'Process Type']],
                       expected_push_results)


@pytest.mark.system
def test_metrics_name_and_path_params_round_trip():
    workbook = 'test_metrics_name_and_path_params_round_trip'
    tree_name = f'{workbook}_{_common.new_placeholder_guid()}'

    tree = Tree(tree_name, workbook=workbook, datasource=workbook)
    tree.insert(['Asset 1', 'Asset 2'])
    tree.insert('Sub Asset', parent='Asset*')
    tree.insert('A Sibling Signal', formula='sinusoid()', parent='Asset*')
    tree.insert('Z Sibling Scalar', formula='0.5', parent='Asset*')
    tree.insert('A Child Signal', formula='sinusoid()', parent='Sub Asset')
    tree.insert('M Child Condition', formula='hours()', parent='Sub Asset')
    tree.insert('Z Child Scalar', formula='0', parent='Sub Asset')

    # A basic metric with a sibling input by name
    basic_metric_df = pd.DataFrame([{
        'Name': 'Test Metric 1',
        'Type': 'Metric',
        'Measured Item': 'A Sibling Signal'
    }])
    tree.insert(basic_metric_df, parent='Asset *')

    # Use name and relative path names as inputs
    condition_metric_df = pd.DataFrame([{
        'Name': 'Test Metric 2',
        'Type': 'Metric',
        'Measured Item': 'Sub Asset >> A Child Signal',
        'Bounding Condition': 'Sub Asset >> M Child Condition',
        'Statistic': 'Average',
        'Thresholds': {
            # Mixed Threshold types - names, relative paths, strings, and numbers
            'HiHiHi#123456': 'Z Sibling Scalar',
            'HiHi': 'A Sibling Signal',
            'Hi': 'Some string value',
            'Lo': 6,
            'LoLo': '3m',
            'LoLoLo': 'Sub Asset >> Z Child Scalar',
        }
    }])
    tree.insert(condition_metric_df, parent='Asset *')

    push_results_1 = tree.push(errors='catalog')

    path_regex = re.compile(r'^(.*) >> (.*?)$')

    def _id(_path):
        _matcher = path_regex.match(_path)
        return push_results_1[(push_results_1['Path'] == _matcher.group(1)) &
                              (push_results_1['Name'] == _matcher.group(2))].iloc[0]['ID']

    # CRAB-37888: We are now forcing SPy scalars to be CalculatedScalar, so they can be edited in the UI
    literal_scalar_type = 'CalculatedScalar'

    # Verify basic properties from the Results dataframe
    expected_push_results = pd.DataFrame([
        ['', tree_name, 'Asset', np.nan, np.nan, np.nan, np.nan],
        [tree_name, 'Asset 1', 'Asset', np.nan, np.nan, np.nan, np.nan],
        [tree_name + ' >> Asset 1', 'A Sibling Signal', 'CalculatedSignal', 'sinusoid()', np.nan, np.nan, np.nan],
        [tree_name + ' >> Asset 1', 'Sub Asset', 'Asset', np.nan, np.nan, np.nan, np.nan],
        [tree_name + ' >> Asset 1 >> Sub Asset', 'A Child Signal', 'CalculatedSignal', 'sinusoid()',
         np.nan, np.nan, np.nan],
        [tree_name + ' >> Asset 1 >> Sub Asset', 'M Child Condition', 'CalculatedCondition', 'hours()',
         np.nan, np.nan, np.nan],
        [tree_name + ' >> Asset 1 >> Sub Asset', 'Z Child Scalar', literal_scalar_type, '0', np.nan, np.nan, np.nan],
        [tree_name + ' >> Asset 1', 'Test Metric 1', 'ThresholdMetric', np.nan,
         _id(tree_name + ' >> Asset 1 >> A Sibling Signal'), np.nan, np.nan],
        [tree_name + ' >> Asset 1', 'Test Metric 2', 'ThresholdMetric', np.nan,
         _id(tree_name + ' >> Asset 1 >> Sub Asset >> A Child Signal'),
         _id(tree_name + ' >> Asset 1 >> Sub Asset >> M Child Condition'), 'Average'],
        [tree_name + ' >> Asset 1', 'Z Sibling Scalar', literal_scalar_type, '0.5', np.nan, np.nan, np.nan],

        [tree_name, 'Asset 2', 'Asset', np.nan, np.nan, np.nan, np.nan],
        [tree_name + ' >> Asset 2', 'A Sibling Signal', 'CalculatedSignal', 'sinusoid()', np.nan, np.nan, np.nan],
        [tree_name + ' >> Asset 2', 'Sub Asset', 'Asset', np.nan, np.nan, np.nan, np.nan],
        [tree_name + ' >> Asset 2 >> Sub Asset', 'A Child Signal', 'CalculatedSignal', 'sinusoid()',
         np.nan, np.nan, np.nan],
        [tree_name + ' >> Asset 2 >> Sub Asset', 'M Child Condition', 'CalculatedCondition', 'hours()',
         np.nan, np.nan, np.nan],
        [tree_name + ' >> Asset 2 >> Sub Asset', 'Z Child Scalar', literal_scalar_type, '0', np.nan, np.nan, np.nan],
        [tree_name + ' >> Asset 2', 'Test Metric 1', 'ThresholdMetric', np.nan,
         _id(tree_name + ' >> Asset 2 >> A Sibling Signal'), np.nan, np.nan],
        [tree_name + ' >> Asset 2', 'Test Metric 2', 'ThresholdMetric', np.nan,
         _id(tree_name + ' >> Asset 2 >> Sub Asset >> A Child Signal'),
         _id(tree_name + ' >> Asset 2 >> Sub Asset >> M Child Condition'), 'Average'],
        [tree_name + ' >> Asset 2', 'Z Sibling Scalar', literal_scalar_type, '0.5', np.nan, np.nan, np.nan],
    ], columns=['Path', 'Name', 'Type', 'Formula', 'Measured Item', 'Bounding Condition', 'Statistic'])

    assert_frame_equal(push_results_1[['Path', 'Name', 'Type', 'Formula', 'Measured Item', 'Bounding Condition',
                                       'Statistic']], expected_push_results)

    def verify_metric_inputs_by_id(pulled_tree, push_results):
        for _, row in pulled_tree.df[pulled_tree.df['Type'] == 'Metric'].iterrows():
            sibling_signal_id = push_results.loc[
                (push_results['Path'] == row['Path']) & (push_results['Name'] == 'A Sibling Signal')]['ID'].iloc[0]
            sibling_scalar_id = push_results.loc[(push_results['Path'] == row['Path'])
                                                 & (push_results['Name'] == 'Z Sibling Scalar')]['ID'].iloc[0]
            child_signal_id = push_results.loc[(push_results['Path'] == row['Path'] + ' >> Sub Asset')
                                               & (push_results['Name'] == 'A Child Signal')]['ID'].iloc[0]
            child_scalar_id = push_results.loc[(push_results['Path'] == row['Path'] + ' >> Sub Asset')
                                               & (push_results['Name'] == 'Z Child Scalar')]['ID'].iloc[0]
            child_condition_id = push_results.loc[(push_results['Path'] == row['Path'] + ' >> Sub Asset')
                                                  & (push_results['Name'] == 'M Child Condition')]['ID'].iloc[0]
            if row['Name'] == 'Test Metric 1':
                assert row['Measured Item'] == sibling_signal_id, \
                    f"'{row}'should have Measured Item {sibling_signal_id}, but was {row['Measured Item']}"
            else:
                assert row['Measured Item'] == child_signal_id, \
                    f"'{row}'should have Measured Item {child_signal_id}, but was {row['Measured Item']}"
                assert row['Bounding Condition'] == child_condition_id, \
                    f"'{row}' should have Bounding Condition {child_condition_id}, but was {row['Bounding Condition']}"
                thresholds = row['Thresholds']
                assert len(thresholds) == 6, f"Six thresholds should be present {thresholds}"
                for level, value in thresholds.items():
                    if level.startswith('HiHiHi#'):
                        assert level == 'HiHiHi#123456', f"HiHiHi#123456 threshold level color does not match in {row}"
                        assert value == sibling_scalar_id, f"HiHiHi threshold was not {sibling_scalar_id} in {row}"
                    elif level.startswith('HiHi#'):
                        assert value == sibling_signal_id, f"HiHi threshold was not {sibling_signal_id} in {row}"
                    elif level.startswith('Hi#'):
                        assert value == 'Some string value', f"Hi threshold was not 'Some string value' in {row}"
                    elif level.startswith('Lo#'):
                        assert value == '6', f"Lo threshold was not '6' in {row}"
                    elif level.startswith('LoLo#'):
                        assert value == '3 m', f"LoLo threshold was not '3 m' in {row}"
                    elif level.startswith('LoLoLo#'):
                        assert value == child_scalar_id, f"LoLoLo threshold was not {child_scalar_id} in {row}"

    # Pull the tree again to compare the Params and Thresholds as IDs with the previous push results' IDs to verify
    # that we're using the correct objects in the tree and not just pushing basic strings.
    tree_2 = Tree(tree_name, workbook=workbook, datasource=workbook)
    verify_metric_inputs_by_id(tree_2, push_results_1)

    # Remove Asset 2 and re-push to verify metrics can be round-tripped
    tree_2.remove('Asset 2')
    push_results_2 = tree_2.push()
    expected_push_results_2 = pd.DataFrame([
        ['', tree_name, 'Asset'],
        [tree_name, 'Asset 1', 'Asset'],
        [tree_name + ' >> Asset 1', 'A Sibling Signal', 'CalculatedSignal'],
        [tree_name + ' >> Asset 1', 'Sub Asset', 'Asset'],
        [tree_name + ' >> Asset 1 >> Sub Asset', 'A Child Signal', 'CalculatedSignal'],
        [tree_name + ' >> Asset 1 >> Sub Asset', 'M Child Condition', 'CalculatedCondition'],
        [tree_name + ' >> Asset 1 >> Sub Asset', 'Z Child Scalar', literal_scalar_type],
        [tree_name + ' >> Asset 1', 'Test Metric 1', 'ThresholdMetric'],
        [tree_name + ' >> Asset 1', 'Test Metric 2', 'ThresholdMetric'],
        [tree_name + ' >> Asset 1', 'Z Sibling Scalar', literal_scalar_type],
    ], columns=['Path', 'Name', 'Type'])
    assert_frame_equal(push_results_2[['Path', 'Name', 'Type']], expected_push_results_2)

    # Verify the removed items were cleaned up
    tree_3 = Tree(tree_name, workbook=workbook, datasource=workbook)
    verify_metric_inputs_by_id(tree_3, push_results_2)
    archived_metric_id = push_results_1.loc[(push_results_1['Path'].str.endswith('>> Asset 2'))
                                            & (push_results_1['Name'] == 'Test Metric 2')]['ID'].iloc[0]
    metric_output = ItemsApi(spy.session.client).get_item_and_all_properties(id=archived_metric_id)
    assert metric_output.is_archived is True


@pytest.mark.system
def test_metrics_archive_and_unarchive():
    # The purpose of this test is to ensure the Metrics archival workarounds are functioning (CRAB-26246, CRAB-29202).
    workbook = 'test_metrics_archive_and_unarchive'
    tree_name = f'{workbook}_{_common.new_placeholder_guid()}'
    items_api = ItemsApi(spy.session.client)

    # Create a basic tree with a signal and a metric.
    tree = Tree(tree_name, workbook=workbook, datasource=workbook)
    tree.insert('Asset')
    tree.insert('Signal', formula='sinusoid()', parent='Asset')
    basic_metric_df = pd.DataFrame([{
        'Name': 'Z Metric',
        'Type': 'Metric',
        'Measured Item': 'Signal'
    }])
    tree.insert(basic_metric_df, parent='Asset')
    push_result = tree.push()
    assert push_result.shape[0] == 4
    # The metric should be alphabetically last. Grab its ID for later.
    assert push_result.iloc[3]['Name'] == 'Z Metric'
    metric_id = push_result.iloc[3]['ID']
    assert items_api.get_item_and_all_properties(id=metric_id).is_archived is False

    # Remove the metric from the tree and repush. Verify the metric actually is archived.
    tree.remove('Z Metric')
    push_result = tree.push()
    assert push_result.shape[0] == 3
    assert items_api.get_item_and_all_properties(id=metric_id).is_archived is True

    # Add the metric back and verify the original metric has become unarchived.
    tree.insert(basic_metric_df, parent='Asset')
    push_result = tree.push()
    assert push_result.shape[0] == 4
    assert items_api.get_item_and_all_properties(id=metric_id).is_archived is False


@pytest.mark.system
def test_metrics_invalid_thresholds():
    workbook = 'test_metrics_invalid_thresholds'
    tree_name = f'{workbook}_{_common.new_placeholder_guid()}'
    signal_id = spy.search({'Name': 'Area A_Temperature', 'Datasource ID': 'Example Data'}).iloc[0]['ID']
    tree = Tree(tree_name, workbook=workbook, datasource=workbook)

    metric_df = pd.DataFrame([{
        'Name': 'Test Metric',
        'Type': 'Metric',
        'Measured Item': signal_id,
        'Thresholds': {
            'InvalidThresholdLevel': 60
        }
    }])
    with pytest.raises(Exception, match="The threshold InvalidThresholdLevel for metric Test Metric "
                                        "is not a valid threshold level."):
        tree.insert(metric_df)

    metric_df = pd.DataFrame([{
        'Name': 'Test Metric',
        'Type': 'Metric',
        'Measured Item': signal_id,
        'Thresholds': {
            ('Not a string type key', True): 60
        }
    }])
    with pytest.raises(Exception, match=f" is of invalid type "):
        tree.insert(metric_df)

    metric_df = pd.DataFrame([{
        'Name': 'Test Metric',
        'Type': 'Metric',
        'Measured Item': signal_id,
        'Thresholds': {
            'Hi#InvalidThresholdColor': 60
        }
    }])
    with pytest.raises(Exception, match='"#InvalidThresholdColor" is not a valid color hex code'):
        tree.insert(metric_df)

    metric_df = pd.DataFrame([{
        'Name': 'Test Metric',
        'Type': 'Metric',
        'Measured Item': signal_id,
        'Thresholds': {
            'Hi#Invalid#Format': 60
        }
    }])
    with pytest.raises(Exception, match='Threshold "Hi#Invalid#Format" contains unknown formatting'):
        tree.insert(metric_df)


@pytest.mark.system
def test_insert_move_and_remove_displays():
    displays_api = DisplaysApi(spy.session.client)

    datasource = 'test_insert_move_and_remove_displays'
    workbook, _, _, template = test_common.create_workbook_workstep_asset_template(datasource=datasource)
    tree = Tree('My Tree', workbook=workbook.id, datasource=datasource)
    tree.insert(['Area %s' % s for s in 'ABC'])

    tree.insert(pd.DataFrame([{
        'Name': 'My Display',
        'Type': 'Display',
        'Template ID': template.id
    }]), parent='Area ?')

    push_results = tree.push()
    display_ids = list(push_results[push_results.Type == 'Display'].ID)
    area_a = push_results[push_results.Name == 'Area A'].ID.squeeze()
    assert displays_api.get_display(id=display_ids[0]).swap.swap_in.upper() == area_a

    tree.remove('Area B >> My Display')
    tree.push()
    assert displays_api.get_display(id=display_ids[1]).is_archived is True

    tree.insert('Area D')
    tree.move('Area C >> My Display', 'Area D')
    push_results = tree.push()
    assert displays_api.get_display(id=display_ids[2]).is_archived is True

    display_ids = list(push_results[push_results.Type == 'Display'].ID)
    area_d = push_results[push_results.Name == 'Area D'].ID.squeeze()
    assert displays_api.get_display(id=display_ids[1]).swap.swap_in.upper() == area_d


@pytest.mark.system
def test_replace_non_sdl_displays():
    displays_api = DisplaysApi(spy.session.client)
    trees_api = TreesApi(spy.session.client)

    datasource = 'test_replace_non_sdl_displays'
    datasource_non_sdl = 'test_replace_non_sdl_displays - non-sdl'
    workbook, _, _, template = test_common.create_workbook_workstep_asset_template(datasource=datasource_non_sdl)
    tree = Tree('Example', workbook=workbook.id, datasource=datasource)
    push_results = tree.push()

    non_sdl_displays = list()
    for asset_id in push_results.loc[push_results.Name.str.startswith('Area'), 'ID']:
        display_output = displays_api.create_display(body=DisplayInputV1(template_id=template.id))
        non_sdl_displays.append(display_output)
        trees_api.move_nodes_to_parent(parent_id=asset_id, body=ItemIdListInputV1(items=[display_output.id]))

    tree = Tree(push_results.at[0, 'ID'], workbook=workbook.id, datasource=datasource)
    tree.push()

    for display_output in non_sdl_displays:
        assert displays_api.get_display(id=display_output.id).is_archived is True

    new_displays = spy.search({'Type': 'Display', 'Asset': push_results.at[0, 'ID']}, workbook=workbook.id)
    assert len(new_displays) == 11  # Areas A through K
    assert (new_displays['Datasource Name'] == datasource).all()

    sample_display_output = displays_api.get_display(id=new_displays.loc[0, 'ID'])
    for attr in ('name', 'scoped_to', 'source_workstep_id', 'swap_source_asset_id'):
        assert getattr(sample_display_output.template, attr) == getattr(template, attr)
    assert sample_display_output.datasource_id == datasource


def test_create_tree_with_new_workbook():
    def search_workbooks(name):
        search_query, _ = WorkbookContext.create_analysis_search_query(name)
        return spy.workbooks.search(search_query)

    workbook = f'Workbook {_common.new_placeholder_guid()}'

    assert len(search_workbooks(workbook)) == 0

    tree = Tree('My Root', workbook=workbook, datasource=workbook)

    assert len(search_workbooks(workbook)) == 0
    assert tree._workbook == workbook
    assert tree._workbook_id == _constants.UNKNOWN

    tree.push()

    search_results = search_workbooks(workbook)
    assert len(search_results) == 1
    assert tree._workbook == workbook
    assert tree._workbook_id == search_results.iloc[0].ID

    push_results = spy.push(metadata=pd.DataFrame([{
        'Name': 'New Locally Scoped Item',
        'Formula': 'days()'
    }]), workbook=workbook, datasource=workbook)

    tree.insert(push_results)

    assert len(tree) == 2
    assert tree.df.loc[1, 'Referenced ID'] == push_results.iloc[0].ID


@pytest.mark.system
def test_create_csv_tree_with_new_workbook():
    def search_workbooks(name):
        search_query, _ = WorkbookContext.create_analysis_search_query(name)
        return spy.workbooks.search(search_query)

    workbook = f'Workbook {_common.new_placeholder_guid()}'

    assert len(search_workbooks(workbook)) == 0

    csv_dir = os.path.join(os.path.dirname(__file__), 'tree_csv_files')
    tree = Tree(os.path.join(csv_dir, 'simplest.csv'), workbook=workbook, datasource=workbook)

    assert len(search_workbooks(workbook)) == 0
    assert tree._workbook == workbook
    assert tree._workbook_id == _constants.UNKNOWN


@pytest.mark.system
def test_insert_rollup():
    workbook = f'Workbook {_common.new_placeholder_guid()}'
    tree = Tree('Rollup Tree', workbook=workbook, datasource=workbook)
    tree.insert(['Cooling Tower 1', 'Cooling Tower 2'])
    tree.insert(['Area %s' % s for s in ('A', 'B', 'C')], 'Cooling Tower 1')
    tree.insert(['Area %s' % s for s in ('D', 'E', 'F')], 'Cooling Tower 2')
    tree.insert('Area Signal', formula='sinusoid()', parent='Area ?')
    tree.insert('Area Scalar', formula='1', parent='Area ?')
    tree.insert('Area Condition', formula='days()', parent='Area ?')

    for i, roll_up in enumerate(_common.ROLL_UP_FUNCTIONS):
        tree.insert(name=f'Roll Up {i}',
                    roll_up_statistic=roll_up.statistic,
                    roll_up_parameters=f'Area ? >> Area {roll_up.input_type}',
                    parent='Cooling Tower ?')

    push_results = tree.push()

    for i, roll_up in enumerate(_common.ROLL_UP_FUNCTIONS):
        push_result1 = push_results[push_results.Name == f'Roll Up {i}'].iloc[0]
        push_result2 = push_results[push_results.Name == f'Roll Up {i}'].iloc[1]

        if roll_up.style == 'union':
            assert 'or' in push_result1.Formula
            assert 'or' in push_result2.Formula
        elif roll_up.style == 'intersect':
            assert 'and' in push_result1.Formula
            assert 'and' in push_result2.Formula
        else:
            assert roll_up.function in push_result1.Formula
            assert roll_up.function in push_result2.Formula
        assert roll_up.output_type in push_result1.Type
        assert roll_up.output_type in push_result2.Type

        parameter_ids1 = [s.split('=')[1] for s in push_result1['Formula Parameters']]
        parameter_ids2 = [s.split('=')[1] for s in push_result2['Formula Parameters']]

        assert parameter_ids1 == list(push_results[(push_results.Name == f'Area {roll_up.input_type}')
                                                   & push_results.Path.str.contains('Cooling Tower 1')].ID)
        assert parameter_ids2 == list(push_results[(push_results.Name == f'Area {roll_up.input_type}')
                                                   & push_results.Path.str.contains('Cooling Tower 2')].ID)


@pytest.mark.system
def test_formula_parameter_absolute_path():
    workbook = f'Workbook {_common.new_placeholder_guid()}'
    tree = Tree('Absolute Path Tree', workbook=workbook, datasource=workbook)
    tree.insert(['A', 'B'])
    tree.insert('Parameter', formula='sinusoid()', parent='A')
    tree.insert('Formula', formula='$x', formula_parameters='x=Absolute Path Tree >> A >> Parameter', parent='B')
    push_results = tree.push()

    parameter_id = push_results.loc[push_results.Name == 'Parameter', 'ID'].squeeze()
    assert parameter_id in push_results.loc[push_results.Name == 'Formula', 'Formula Parameters'].squeeze()[0]


@pytest.mark.system
def test_formula_parameter_relative_path_above():
    workbook = f'Workbook {_common.new_placeholder_guid()}'
    tree = Tree('Relative Path Tree', workbook=workbook, datasource=workbook)
    tree.insert(['A', 'B'])
    tree.insert('Parameter', formula='sinusoid()', parent='A')
    tree.insert('Formula 1', formula='$x', formula_parameters='x=.. >> A >> Parameter', parent='B')
    tree.insert('Formula 2', formula='$x', formula_parameters='x=Relative Path Tree >> B >> .. >> A >> Parameter',
                parent='B')
    push_results = tree.push()

    parameter_id = push_results.loc[push_results.Name == 'Parameter', 'ID'].squeeze()
    assert parameter_id in push_results.loc[push_results.Name == 'Formula 1', 'Formula Parameters'].squeeze()[0]
    assert parameter_id in push_results.loc[push_results.Name == 'Formula 2', 'Formula Parameters'].squeeze()[0]


@pytest.mark.system
def test_select():
    workbook = 'Workbook %s' % _common.new_placeholder_guid()
    tree = spy.assets.Tree('My Tree', workbook=workbook, datasource=workbook)

    tree.insert(['Cooling Tower 1', 'Cooling Tower 2'])
    tree.insert([f'Area {s}' for s in 'ABC'], 'Cooling Tower 1')
    tree.insert([f'Area {s}' for s in 'DEF'], 'Cooling Tower 2')
    tree.insert('My Signal', formula='sinusoid()', parent='Area ?')
    tree.insert('My Condition', formula='condition(capsule("2021-01-01"))', parent='Area [ABDE]')
    tree.insert('My Condition', formula='condition(capsule("2022-01-01"))', parent='Area [CF]')
    tree.push()

    def expect_to_contain_areas(selected_tree, areas):
        root_expected, cooling_tower_1_expected, cooling_tower_2_expected = False, False, False
        tree_names = set(selected_tree.items()['Name'])
        for area in 'ABCDEF':
            if area in areas:
                root_expected = True
                if area in 'ABC':
                    cooling_tower_1_expected = True
                if area in 'DEF':
                    cooling_tower_2_expected = True
                assert f'Area {area}' in tree_names
            else:
                assert f'Area {area}' not in tree_names
        expected_length = len(areas) * 3 + sum((root_expected, cooling_tower_1_expected, cooling_tower_2_expected))
        assert len(selected_tree) == expected_length

    expect_to_contain_areas(tree.select(within='Cooling Tower 1'),
                            'ABC')
    expect_to_contain_areas(tree.select(within='Area A'),
                            'A')
    expect_to_contain_areas(tree.select(condition='My Condition', start='2021-01-01', end='2021-01-02'),
                            'ABDE')
    expect_to_contain_areas(tree.select(condition='My Condition', start='2020-01-01', end='2020-01-02'),
                            '')
    expect_to_contain_areas(tree.select(condition='My Condition', start='2022', within='Cooling Tower 1'),
                            'C')
    expect_to_contain_areas(tree.select(condition='My Signal', start='2020'),
                            '')
    expect_to_contain_areas(tree.select(condition='My Condition', start='2020', within='My Tree'),
                            'ABCDEF')


def populate_rainbow_tree(tree: Tree):
    signal_id = spy.search({'Name': 'Area A_Temperature', 'Datasource ID': 'Example Data'}).iloc[0]['ID']
    metric_df = pd.DataFrame([{
        'Name': 'Z Metric',
        'Type': 'Metric',
        'Measured Item': signal_id
    }])
    tree.insert('My Signal', formula='sinusoid()')
    tree.insert('My Condition', formula='days()')
    tree.insert('My Scalar', formula='1')
    tree.insert(metric_df)


@pytest.mark.system
def test_change_tree_scope():
    items_api = ItemsApi(spy.client)
    datasource = 'test_change_tree_scope'
    workbook_1 = f'Workbook {_common.new_placeholder_guid()}'
    workbook_2 = f'Workbook {_common.new_placeholder_guid()}'
    tree = Tree('Test Tree Scope Change', workbook=workbook_1, datasource=datasource)
    populate_rainbow_tree(tree)
    tree.push()

    assert len(spy.search({'Name': 'Test Tree Scope Change'}, workbook=workbook_1)) == 1

    # Workbook to another workbook scope
    tree.workbook = workbook_2
    tree.push()

    assert len(spy.search({'Name': 'Test Tree Scope Change'}, workbook=workbook_2)) == 1

    # Workbook to global scope
    assert len(spy.search({'Name': 'Test Tree Scope Change'})) == 0

    tree.workbook = None
    tree.push()

    assert len(spy.search({'Name': 'Test Tree Scope Change'})) == 1

    # Clean up
    items_api.archive_item(id=tree.df.loc[0, 'ID'])


@pytest.mark.system
def test_workbook_duplication():
    if not spy.utils.is_server_version_at_least(64):
        # This test requires CRAB-40301 to be fixed
        return

    workbooks_api = WorkbooksApi(spy.client)
    tree_name = 'Test Tree Workbook Duplication'
    datasource = 'test_tree_system.test_workbook_duplication'
    workbook_1 = f'{tree_name} {_common.new_placeholder_guid()}'
    tree = Tree(tree_name, workbook=workbook_1, datasource=datasource)

    populate_rainbow_tree(tree)
    push_df = tree.push()

    search_df = spy.search({'Path': tree_name}, workbook=workbook_1, all_properties=True)
    assert len(search_df) == 4
    data_ids = search_df[search_df['Data ID'].str.startswith(f'[{push_df.spy.workbook_id}]')]
    assert len(data_ids) == 4

    workbook_output = workbooks_api.create_workbook(body=WorkbookInputV1(
        name=f'{workbook_1} Clone',
        branch_from=push_df.spy.workbook_id
    ))

    # Make sure the Data IDs got fixed up when it was cloned
    search_df = spy.search({'Path': tree_name}, workbook=workbook_output.id, all_properties=True)
    assert len(search_df) == 4
    data_ids = search_df[search_df['Data ID'].str.startswith(f'[{workbook_output.id}]')]
    assert len(data_ids) == 4

    # Now add something to the tree
    cloned_tree = Tree(tree_name, workbook=workbook_output.id, datasource=datasource)
    cloned_tree.insert('Additional Scalar', formula='2')
    cloned_push_df = cloned_tree.push()

    assert cloned_push_df.spy.workbook_id == workbook_output.id

    search_df = spy.search({'Path': tree_name}, workbook=workbook_1, all_properties=True)
    assert len(search_df) == 4
    data_ids = search_df[search_df['Data ID'].str.startswith(f'[{push_df.spy.workbook_id}]')]
    assert len(data_ids) == 4

    search_df = spy.search({'Path': tree_name}, workbook=workbook_output.id, all_properties=True)
    assert len(search_df) == 5
    data_ids = search_df[search_df['Data ID'].str.startswith(f'[{workbook_output.id}]')]
    assert len(data_ids) == 5


@pytest.mark.system
def test_create_globally_scoped_tree():
    items_api = ItemsApi(spy.session.client)
    datasource = 'test_create_globally_scoped_tree'
    tree = Tree('Test Create Global Scope Tree', workbook=None, datasource=datasource)

    tree.insert('My Signal', formula='sinusoid()')
    tree.insert('My Condition', formula='days()')
    tree.insert('My Scalar', formula='1')

    push_results_df = tree.push()

    assert (push_results_df['Push Result'] == 'Success').all()
    assert len(spy.search({'Name': 'Test Create Global Scope Tree'})) == 1

    # Clean up
    items_api.archive_item(id=tree.df.loc[0, 'ID'])


@pytest.mark.system
def test_tree_redaction():
    workbook = 'Workbook %s' % _common.new_placeholder_guid()
    admin_session = test_common.get_session(Sessions.admin)

    admin_tree = Tree('Admin Tree', workbook=workbook, datasource=workbook, session=admin_session)
    admin_tree.insert(['My Asset'])
    admin_tree.insert(pd.DataFrame([{
        'Name': 'My Signal',
        'Formula': 'sinusoid()',
    }, {
        'Name': 'My Metric',
        'Measured Item': 'My Signal'
    }]), parent='My Asset')
    push_results = admin_tree.push()
    workbook_id = push_results.spy.workbook_id

    spy.acl.push(workbook_id,
                 {'ID': spy.session.user.id, 'Read': True},
                 session=admin_session)

    # There are 6 different subsets of the admin tree that the non-admin may have
    # access to, assuming that there isn't a middle section of the tree redacted.
    # We test the constructor and the insert method against each of these.

    for subset in itertools.chain.from_iterable(itertools.combinations(range(4), r) for r in range(5)):
        # If the root is included, the whole tree must be included
        if 0 in subset and not len(subset) == 4:
            continue

        # If the middle asset is included, its children must be included
        if 1 in subset and not (2 in subset and 3 in subset):
            continue

        subset = list(subset)
        expected_indices = [i for i in range(4) if i not in subset]

        # Reset permissions on tree
        spy.acl.push(push_results,
                     {'ID': spy.session.user.id, 'Read': True},
                     replace=True,
                     disable_inheritance=False,
                     session=admin_session)

        # Revoke permissions on chosen subset
        if len(subset):
            spy.acl.push(push_results.loc[subset], list(),
                         replace=True,
                         disable_inheritance=True,
                         session=admin_session)

        # Test insert
        tree = Tree('My Tree', workbook=workbook_id, datasource=workbook)
        status = spy.Status(errors='catalog')
        tree.insert(admin_tree, status=status)
        assert len(tree) == len(expected_indices) + 1
        for i in expected_indices:
            new_tree_row = tree.items()[tree.items()['Referenced ID'] == push_results.loc[i, 'ID']]
            assert len(new_tree_row) == 1
            new_tree_row = new_tree_row.squeeze()
            assert new_tree_row['Name'] == push_results.loc[i, 'Name']
            expected_path = f"My Tree >> {push_results.loc[i, 'Path']}" if push_results.loc[i, 'Path'] else 'My Tree'
            assert new_tree_row['Path'] == expected_path
        for i in subset:
            assert any(push_results.loc[i, 'ID'].lower() in warning for warning in status.warnings)

        if len(subset) == 4:
            # Don't test constructor on fully redacted tree
            continue

        # Test constructor
        status = spy.Status(errors='catalog')
        tree = Tree(admin_tree.items(), workbook=workbook_id, datasource=workbook, status=status)
        assert len(tree) == len(expected_indices)
        for i in expected_indices:
            new_tree_row = tree.items()[tree.items()['ID'] == push_results.loc[i, 'ID']]
            assert len(new_tree_row) == 1
            new_tree_row = new_tree_row.squeeze()
            assert new_tree_row['Name'] == push_results.loc[i, 'Name']
            assert new_tree_row['Path'] == push_results.loc[i, 'Path']
        for i in subset:
            assert any(push_results.loc[i, 'ID'].lower() in warning for warning in status.warnings)


@pytest.mark.system
def test_update_global_spy_tree():
    datasource = 'test_update_global_spy_tree'
    original_tree = spy.assets.Tree(f'My Tree {_common.new_placeholder_guid()}', workbook=None, datasource=datasource)
    original_tree.insert(name='My Signal', formula='sinusoid()')
    original_tree.push()

    pulled_tree = spy.assets.Tree(original_tree.items().loc[0, 'ID'], workbook=None, datasource=datasource)
    assert list(pulled_tree.items()['ID']) == list(original_tree.items()['ID'])

    pulled_tree.insert(name='New Signal', formula='sinusoid()')
    push_results = pulled_tree.push()

    assert len(push_results) == 3


@pytest.mark.system
def test_tree_seeq_internal_datasource():
    name = f'test_tree_seeq_internal_datasource {_common.new_placeholder_guid()}'
    tree = spy.assets.Tree(name, workbook=name, datasource=spy.INHERIT_FROM_WORKBOOK)
    tree.insert(pd.DataFrame([{
        'Name': 'My Asset',
    }, {
        'Name': 'My Signal',
        'Formula': 'sinusoid()',
    }, {
        'Name': 'My Scalar',
        'Formula': '1m',
    }, {
        'Name': 'My Condition',
        'Formula': 'days()',
    }, {
        'Name': 'My Metric',
        'Measured Item': 'My Signal',
    }]))
    # Verify the results are all pushed to the Seeq-internal datasources and that their ACLs inherit from the workbook
    push_result_1 = tree.push()
    actual_datasources = push_result_1['Datasource Class'].unique()
    unexpected = list(actual_datasources).remove(spy.INHERIT_FROM_WORKBOOK)
    error = f"Unexpected datasources were pushed incorrectly to: {unexpected}" if unexpected else ''
    assert not error
    acls = spy.acl.pull(push_result_1)
    for _, row in acls.iterrows():
        item_name = row['Name']
        from_datasource = row['Permissions From Datasource']
        error = f"{item_name} has permissions from datasource" if from_datasource else ''
        assert not error
        acl = row['Access Control'].reset_index()
        error = f"{item_name} has incorrect number of ACEs: {acl}" if len(acl) != 1 else ''
        assert not error
        origin = acl.at[0, 'Origin Type']
        incorrect_origin = origin not in ['Analysis', 'Asset']
        error = f"{item_name} has incorrect permission origin: {origin}" if incorrect_origin else ''
        assert not error

    # Re-pushing the same tree should keep the datasource
    push_result_2 = tree.push()
    actual_datasources = push_result_2['Datasource Class'].unique()
    unexpected = list(actual_datasources).remove(spy.INHERIT_FROM_WORKBOOK)
    error = f"Unexpected datasources were re-pushed incorrectly to: {unexpected}" if unexpected else ''
    assert not error

    # Round-tripping should maintain the datasource if specified
    tree = spy.assets.Tree(push_result_1.loc[0, 'ID'], workbook=name, datasource=spy.INHERIT_FROM_WORKBOOK)
    push_result_3 = tree.push()
    actual_datasources = push_result_3['Datasource Class'].unique()
    unexpected = list(actual_datasources).remove(spy.INHERIT_FROM_WORKBOOK)
    error = f"Unexpected datasources were round-tripped incorrectly to: {unexpected}" if unexpected else ''
    assert not error

    # Pulling the tree with a different datasource set should push to the specified datasource
    tree = spy.assets.Tree(push_result_1.loc[0, 'ID'], workbook=name, datasource=name)
    push_result_4 = tree.push()
    actual_datasource_classes = push_result_4['Datasource Class'].unique()
    unexpected = list(actual_datasource_classes).remove(_common.DEFAULT_DATASOURCE_CLASS)
    error = f"Unexpected Datasource Classes were newly-pushed incorrectly to: {unexpected}" if unexpected else ''
    assert not error
    actual_datasource_ids = push_result_4['Datasource ID'].unique()
    unexpected = list(actual_datasource_ids).remove(name)
    error = f"Unexpected Datasource IDs were newly-pushed incorrectly to: {unexpected}" if unexpected else ''
    assert not error


@pytest.mark.system
def test_pull_tree_incorrect_workbook():
    # Create a Tree with a reference signal and push it.
    workbook1 = f'test_pull_tree_incorrect_workbook_1_{_common.new_placeholder_guid()}'
    area_a_temp = spy.search({'Name': 'Area A_Temperature', 'Datasource ID': 'Example Data'}, workbook=spy.GLOBALS_ONLY)
    tree1 = spy.assets.Tree(workbook1, workbook=workbook1)
    tree1.insert(area_a_temp)
    tree1.insert('Temp Plus 1', formula='$t+1', formula_parameters=['t=Area A_Temperature'])
    tree1.push()
    tree1_root_id, _ = get_root_node_ids(tree1)
    tree1_workbook_id = tree1._workbook_id
    # Create a second Tree that's a reference of the first tree and push it to a different workbook.
    workbook2 = f'test_pull_tree_incorrect_workbook_2_{_common.new_placeholder_guid()}'
    tree2 = spy.assets.Tree(workbook2, workbook=workbook2)
    tree2.insert(tree1.df[['ID', 'Name', 'Type']].loc[1:2], parent=workbook2)
    tree2.push()
    tree2_root_id, _ = get_root_node_ids(tree2)
    tree2_workbook_id = tree2._workbook_id
    assert tree1_root_id != tree2_root_id
    assert tree1_workbook_id != tree2_workbook_id

    # Try pulling the second Tree by ID, but specifying the first workbook (which doesn't match).
    with pytest.raises(spy._errors.SPyValueError,
                       match="Root asset with ID .* is scoped to workbook .*, but requested workbook's ID is"):
        spy.assets.Tree(tree1_root_id, workbook=tree2_workbook_id)

    # Pulling with the correct workbook arg should still work like normal.
    tree4 = spy.assets.Tree(tree1_root_id, workbook=workbook1)
    assert tree1_workbook_id == tree4._workbook_id
    tree5 = spy.assets.Tree(tree1_root_id, workbook=tree1_workbook_id)
    assert tree1_workbook_id == tree5._workbook_id


def _get_first_id_from_signal_name(name):
    return spy.search(pd.DataFrame.from_dict({'Name': [name], 'Type': ['Signal']}),
                      workbook=spy.GLOBALS_ONLY, order_by=["ID"])['ID'][0]


def assert_tree_equals_expected(tree, expected_nodes):
    tree_dataframe = tree.df
    assert_dataframe_equals_expected(tree_dataframe, expected_nodes)


def assert_dataframe_equals_expected(tree_dataframe, expected_nodes):
    pd.set_option('display.max_columns', None)  # Print all columns if something errors
    for expected_node in expected_nodes:
        found_series = pd.Series(data=([True] * len(tree_dataframe)), dtype=bool)
        for key, value in expected_node.items():
            if pd.isnull(value):
                found_series = found_series & (tree_dataframe[key].isnull())
            elif isinstance(value, list) or isinstance(value, dict):
                found_series = found_series & tree_dataframe[key].apply(lambda x: x == value)
            else:
                found_series = found_series & (tree_dataframe[key] == value)

        assert found_series.sum() == 1, \
            f"Found item {expected_node}" \
            f"\n{found_series.sum()} times in Dataframe" \
            f"\n{tree_dataframe}"
    assert len(tree_dataframe) == len(expected_nodes), \
        f'Tree items do not match count: Real={len(tree_dataframe)}, Expected={len(expected_nodes)}'


def assert_search_results_equals_expected(search_results_df, expected_nodes):
    pd.set_option('display.max_columns', None)  # Print all columns if something errors

    for expected_node in expected_nodes:
        asset = np.nan
        # Extract the parent asset from that path
        if expected_node['Path'].count('>>') > 0:
            asset = expected_node['Path'].rpartition(' >> ')[2]
        elif expected_node['Path'] != '':
            asset = expected_node['Path']

        node_df = search_results_df[
            (search_results_df['Name'] == expected_node['Name']) &
            (search_results_df['Asset'] == asset) &
            (search_results_df['Type'] == expected_node['Type'])]

        assert len(node_df) == 1, \
            f"Expected item ({expected_node['Name']}, {asset}, {expected_node['Type']})" \
            f"\n was not found in Dataframe" \
            f"\n{search_results_df}"
    assert len(search_results_df) == len(expected_nodes), \
        f'Search result items do not match count: Real={len(search_results_df)}, Expected={len(expected_nodes)}'


def create_expected_list_from_tree(tree):
    # Create a list of node dicts from an existing tree.
    tree_dataframe = tree.df
    expected = list()
    for index, row in tree_dataframe.iterrows():
        expected.append({
            'Name': row['Name'],
            'Path': row['Path'],
            'Type': row['Type']
        })
    return expected


def get_root_node_ids(tree):
    # Get the ID and Reference ID from the tree's root
    tree_dataframe = tree.df
    root_df = tree_dataframe[(tree_dataframe['Path'] == '')]
    assert len(root_df) == 1, \
        f"Exactly one root node was not found in Dataframe: \n{tree_dataframe}"
    id = root_df['ID'].values[0]
    referenced_id = root_df['Referenced ID'].values[0]
    return id, referenced_id
