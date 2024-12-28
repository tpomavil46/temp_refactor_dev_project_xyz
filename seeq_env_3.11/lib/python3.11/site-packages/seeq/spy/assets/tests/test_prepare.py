import pandas as pd
import pytest
from seeq import spy
from seeq.spy.tests import test_common


def setup_module():
    test_common.initialize_sessions()


@pytest.mark.system
def test_prepare():
    old_asset_format_false_df = spy.search({'Path': 'Example'},
                                           recursive=True, workbook=spy.GLOBALS_ONLY, old_asset_format=False)

    spy.assets.prepare(old_asset_format_false_df, root_asset_name='test_prepare_old_asset_format_false')

    asset_column_values = old_asset_format_false_df['Asset'].to_list()
    build_asset_column_values = old_asset_format_false_df['Build Asset'].to_list()
    assert asset_column_values == build_asset_column_values
    path_column_values = old_asset_format_false_df['Path'].to_list()
    build_path_column_values = old_asset_format_false_df['Build Path'].to_list()
    expected_build_paths = [v.replace('Example', 'test_prepare_old_asset_format_false')
                            for v in path_column_values]
    assert expected_build_paths == build_path_column_values

    old_asset_format_true_df = spy.search({'Path': 'Example'},
                                          recursive=True, workbook=spy.GLOBALS_ONLY, old_asset_format=True)

    spy.assets.prepare(old_asset_format_true_df, root_asset_name='test_prepare_old_asset_format_true')

    build_asset_column_values = old_asset_format_true_df['Build Asset'].to_list()
    expected_build_assets = list()
    for _, row in old_asset_format_true_df.iterrows():
        expected_build_assets.append(row['Name'] if row['Type'] == 'Asset' else row['Asset'])
    assert expected_build_assets == build_asset_column_values
    build_path_column_values = old_asset_format_true_df['Build Path'].to_list()
    expected_build_paths = list()
    for _, row in old_asset_format_true_df.iterrows():
        if row['Type'] == 'Asset':
            if not pd.isna(row['Path']) and row['Path'] not in [None, '']:
                expected_build_path = row['Path'] + ' >> ' + row['Asset']
            else:
                expected_build_path = row['Asset']
        else:
            expected_build_path = row['Path']
        expected_build_paths.append(expected_build_path.replace('Example', 'test_prepare_old_asset_format_true'))

    assert expected_build_paths == build_path_column_values
