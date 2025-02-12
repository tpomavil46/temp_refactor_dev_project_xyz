import re
import tempfile
import time
import unittest.mock
import uuid
from pathlib import Path

import mock.mock
import numpy as np
import pandas as pd
import pytest

from seeq import spy
from seeq.base.seeq_names import SeeqNames
from seeq.sdk import *
from seeq.spy import _common, _login, _search, Session
from seeq.spy._errors import *
from seeq.spy.tests import test_common
from seeq.spy.tests.test_common import Sessions, ApiClientRecorder
from seeq.spy.workbooks import Topic, Analysis


def setup_module():
    test_common.initialize_sessions()


def _assert_result(df, status):
    assert df.spy.status.df['Result'].drop_duplicates().tolist() == [status]


@pytest.mark.system
def test_simple_search():
    search_results = spy.search({
        'Name': 'Area A_Temper'
    }, workbook=spy.GLOBALS_ONLY)

    assert len(search_results) == 1
    assert search_results.spy.status.df.iloc[0]['Pages'] == 1
    _assert_result(search_results, 'Success')
    assert 'Estimated Sample Period' not in search_results

    search_results = spy.search(pd.DataFrame([{
        'Name': 'Area A_Temper'
    }]), workbook=spy.GLOBALS_ONLY)

    # Nothing will be returned because we use an equal-to comparison when a DataFrame is passed in
    assert len(search_results) == 0
    _assert_result(search_results, 'Success')

    search_results = spy.search(pd.DataFrame([{
        'Name': 'Area A_Temperature'
    }]), workbook=spy.GLOBALS_ONLY)

    assert len(search_results) == 1
    _assert_result(search_results, 'Success')

    search_results = spy.search(pd.DataFrame([{
        'Name': 'Area A_Temperature'
    }]), workbook=spy.GLOBALS_ONLY, all_properties=True)

    assert len(search_results) == 1
    _assert_result(search_results, 'Success')
    assert 'Maximum Interpolation' in search_results.iloc[0]


@pytest.mark.system
def test_search_without_pushed_workbook():
    # This test ensures that spy.search() returns consistent results with and without a pushed default workbook.
    # In older versions, if a default workbook had not been pushed, spy.search() would return results from ANY
    # workbook.

    # Make sure there is only one thing that contains 'Area B_Rel'. It will be the (global) stored signal from the
    # example data.
    search_results = spy.search({
        'Name': 'Area B_Rel'
    })
    assert len(search_results) == 1
    _assert_result(search_results, 'Success')

    # Push a new workbook so that we can push a new signal that is scoped to it
    workbook = Analysis('test_signal_in_another_workbook')
    worksheet = workbook.worksheet('auto-created-worksheet')
    spy.workbooks.push(workbook)

    spy.push(metadata=pd.DataFrame([{
        'Type': 'Signal',
        'Name': 'Area B_Rel',
        'Formula': 'sinusoid()'
    }]), workbook=workbook.id, worksheet=worksheet.name)

    search_results = spy.search({
        'Name': 'Area B_Rel'
    })

    # We should still only have one result, since the workbook that we pushed to should be excluded
    assert len(search_results) == 1
    _assert_result(search_results, 'Success')

    # Push should create the default workbook (if it didn't exist)
    spy.push()

    search_results = spy.search({
        'Name': 'Area B_Rel'
    })

    # We should still have the 1 result (the global stored signal), but this time the scope of the search is global +
    # default workbook
    assert len(search_results) == 1
    _assert_result(search_results, 'Success')

    search_results = spy.search({
        'Name': 'Area B_Rel'
    }, workbook=spy.GLOBALS_AND_ALL_WORKBOOKS)

    # We should now have two results since we are searching across ALL workbooks
    assert len(search_results) == 2
    _assert_result(search_results, 'Success')

    # Now push a signal to the default workbook.
    push_df = spy.push(metadata=pd.DataFrame([{
        'Type': 'Signal',
        'Name': 'Area B_Rel',
        'Formula': 'sinusoid()'
    }]))

    try:
        search_results = spy.search({
            'Name': 'Area B_Rel'
        })

        # We should now have two results since we pushed a signal to the default workbook
        assert len(search_results) == 2
        _assert_result(search_results, 'Success')

        search_results = spy.search({
            'Name': 'Area B_Rel'
        }, workbook=spy.GLOBALS_ONLY)

        # We should only have one result because we are searching globals-only
        assert len(search_results) == 1
        _assert_result(search_results, 'Success')

        # Search in a non-existent workbook. This operation would raise an error.
        with pytest.raises(Exception, match='not found'):
            spy.search({
                'Name': 'Area B_Rel'
            }, workbook='My Folder >> folder1 >> Non-existent')

    finally:
        # Rename the pushed signal so that this test can be run multiple times
        items_api = ItemsApi(spy.session.client)
        items_api.set_property(id=push_df.iloc[0]['ID'], property_name='Name',
                               body=PropertyInputV1(value='test_search_without_pushed_workbook_trash'))


@pytest.mark.system
def test_dataframe_single_row_with_id():
    search_results = spy.search({
        'Name': 'Area A_Temper'
    })

    search_results = spy.search(search_results.iloc[0], workbook=spy.GLOBALS_ONLY)
    assert len(search_results) == 1
    assert search_results.iloc[0]['Name'] == 'Area A_Temperature'
    assert 'Data ID' not in search_results.columns
    _assert_result(search_results, 'Success')

    try:
        spy.session.options.compatibility = 188
        search_results = spy.search(search_results.iloc[0], workbook=spy.GLOBALS_ONLY)
        assert search_results.iloc[0]['Name'] == 'Area A_Temperature'
        assert search_results.iloc[0]['Data ID'] == '[Tag] Area A_Temperature.sim.ts.csv'
        _assert_result(search_results, 'Success')
    finally:
        spy.session.options.compatibility = None


@pytest.mark.system
def test_dataframe_multi_row():
    search_results = spy.search(pd.DataFrame([{
        'Name': 'Area A_Relative Humidity',
        'Datasource Name': 'Example Data'
    }, {
        'Name': 'Area A_Temperature',
        'Datasource Name': 'Example Data'
    }, {
        'Path': 'Example >> Cooling Tower 1 >> Area A',
        'Name': 'Relative Humidity'
    }]), workbook=spy.GLOBALS_ONLY)

    assert len(search_results) == 3
    _assert_result(search_results, 'Success')
    assert len(search_results[search_results['Name'] == 'Area A_Temperature']) == 1
    assert len(search_results[search_results['Name'] == 'Area A_Relative Humidity']) == 1
    assert len(search_results[search_results['Name'] == 'Relative Humidity']) == 1


@pytest.mark.system
def test_dataframe_warnings_and_duplicates():
    search_results = spy.search([{
        'Name': 'Humid',
        'Path': 'Example >> Cooling Tower 1'
    }, {
        'Name': 'Area A_',
        'Datasource Name': 'Example Data'
    }, {
        'Name': 'Area A_',
        'Datasource Name': 'Example Data'
    }], workbook=spy.GLOBALS_ONLY)

    # Make sure the duplicates will have been dropped
    assert len(search_results) == 14
    _assert_result(search_results, 'Success')

    assert len(search_results.spy.status.warnings) == 1
    assert '6 duplicates removed' in search_results.spy.status.warnings.pop()

    search_results.drop(columns=['ID'], inplace=True)

    search_results2 = spy.search(search_results, workbook=spy.GLOBALS_ONLY)

    # There will be a warning because 'Value Unit Of Measure' will be part of the DataFrame but it can't be searched on
    assert len(search_results2.spy.status.warnings) == 1
    _assert_result(search_results2, 'Success')
    assert 'are not indexed and will be ignored:\n"Value Unit Of Measure", "Archived"' in \
           search_results2.spy.status.warnings.pop()

    assert len(search_results2) == 14

    unique_ids = search_results2['ID'].drop_duplicates().to_list()
    assert len(unique_ids) == 14


@pytest.mark.system
def test_path_with_datasource():
    search_results = spy.search({
        'Name': 'Area ?_*',
        'Datasource Name': 'Example Data'
    }, workbook=spy.GLOBALS_ONLY)

    push_df = search_results.copy()
    push_df['Reference'] = True
    push_df['Path'] = 'test_path_with_datasource-tree >> branch-alpha'
    push_df['Asset'] = push_df['Name'].str.extract(r'(Area .)_.*')
    push_df['Name'] = push_df['Name'].str.extract(r'Area ._(.*)')

    spy.push(metadata=push_df, datasource='test_path_with_datasource-name-1',
             workbook='test_path_with_datasource', worksheet=None)

    push_df['Path'] = 'test_path_with_datasource-tree >> branch-bravo'

    spy.push(metadata=push_df, datasource='test_path_with_datasource-name-2',
             workbook='test_path_with_datasource', worksheet=None)

    search_results = spy.search({
        'Path': 'test_path_with_datasource-tree',
    }, workbook='test_path_with_datasource', recursive=False, old_asset_format=False).sort_values(by=['Asset'])

    _assert_result(search_results, 'Success')
    paths = search_results['Path'].drop_duplicates().dropna().tolist()
    assets = search_results['Asset'].drop_duplicates().tolist()
    names = sorted(search_results['Name'].tolist())
    types = search_results['Type'].drop_duplicates().tolist()
    assert paths == ['test_path_with_datasource-tree']
    assert assets == ['branch-alpha', 'branch-bravo']
    assert names == ['branch-alpha', 'branch-bravo']
    assert types == ['Asset']

    search_results = spy.search({
        'Path': 'test_path_with_datasource-tree',
    }, workbook='test_path_with_datasource', recursive=True, old_asset_format=True)

    assert len(search_results) > 100
    _assert_result(search_results, 'Success')
    types = sorted(search_results['Type'].drop_duplicates().tolist())
    assert types == ['Asset', 'CalculatedSignal']
    assert len(search_results[search_results['Asset'] == 'branch-alpha']) > 0
    assert len(search_results[search_results['Asset'] == 'branch-bravo']) > 0

    search_results = spy.search({
        'Path': 'test_path_with_datasource-tree >> branch-alpha',
    }, workbook='test_path_with_datasource', recursive=True, old_asset_format=True)

    assert len(search_results[search_results['Path'].str.endswith('branch-alpha')]) > 0
    assert len(search_results[search_results['Path'].str.endswith('branch-bravo')]) == 0

    search_results = spy.search({
        'Path': 'test_path_with_datasource-tree >> branch-*',
    }, workbook='test_path_with_datasource', recursive=True, old_asset_format=True)

    assert len(search_results[search_results['Path'].str.endswith('branch-alpha')]) > 0
    assert len(search_results[search_results['Path'].str.endswith('branch-bravo')]) > 0

    search_results = spy.search({
        'Path': 'test_path_with_datasource-* >> branch-bravo',
    }, workbook='test_path_with_datasource', recursive=True, old_asset_format=True)

    assert len(search_results[search_results['Path'].str.endswith('branch-alpha')]) == 0
    assert len(search_results[search_results['Path'].str.endswith('branch-bravo')]) > 0

    search_results = spy.search({
        'Path': 'test_path_with_datasource-* >> branch-bravo',
        'Datasource Name': 'test_path_with_datasource-name-1'
    }, workbook='test_path_with_datasource', recursive=True, old_asset_format=True)

    assert len(search_results) == 0

    search_results = spy.search({
        'Path': 'test_path_with_datasource-* >> branch-alpha',
        'Datasource Name': 'test_path_with_datasource-name-1'
    }, workbook='test_path_with_datasource', recursive=True, old_asset_format=True)

    assert len(search_results[search_results['Path'].str.endswith('branch-alpha')]) > 0
    assert len(search_results[search_results['Path'].str.endswith('branch-bravo')]) == 0


@pytest.mark.system
def test_dataframe_bad_datasource():
    with pytest.raises(RuntimeError):
        spy.search(pd.DataFrame([{
            'Name': 'Area A_Temperature',
            'Datasource Name': 'Bad Datasource'
        }]), workbook=spy.GLOBALS_ONLY)


@pytest.mark.system
def test_type_search():
    search_results = spy.search({
        'Datasource Class': 'Time Series CSV Files',
        'Type': 'Signal'
    }, workbook=spy.GLOBALS_ONLY)

    assert 230 < len(search_results) < 330
    _assert_result(search_results, 'Success')

    datasource_names = set(search_results['Datasource Name'].tolist())
    assert len(datasource_names) == 1
    assert datasource_names.pop() == 'Example Data'

    types = set(search_results['Type'].tolist())
    assert len(types) == 1
    assert types.pop() == 'StoredSignal'

    search_results = spy.search({
        'Datasource Class': 'Time Series CSV Files',
        'Type': 'Condition'
    }, workbook=spy.GLOBALS_ONLY)

    assert len(search_results) == 0
    _assert_result(search_results, 'Success')

    search_results = spy.search({
        'Datasource Class': 'Time Series CSV Files',
        'Type': 'Scalar'
    }, workbook=spy.GLOBALS_ONLY)

    assert len(search_results) == 0
    _assert_result(search_results, 'Success')

    search_results = spy.search({
        'Datasource Class': 'Time Series CSV Files',
        'Type': 'Asset'
    }, workbook=spy.GLOBALS_ONLY)

    assert 5 < len(search_results) < 20
    _assert_result(search_results, 'Success')

    # Multiple types
    search_results = spy.search({
        'Datasource Class': 'Time Series CSV Files',
        'Type': ['Signal', 'Asset']
    }, workbook=spy.GLOBALS_ONLY)

    _assert_result(search_results, 'Success')
    assert 250 < len(search_results) < 340
    assert 5 < len(search_results[search_results['Type'] == 'Asset']) < 20
    assert 230 < len(search_results[search_results['Type'].str.contains('Signal')]) < 330


@pytest.mark.system
def test_path_search_recursive():
    with ApiClientRecorder(spy.session) as recorder:
        search_results = spy.search({
            'Path': 'Non-existent >> Path'
        }, workbook=spy.GLOBALS_ONLY)

        # These recorder.calls assertions are an attempt to ensure we don't accidentally introduce a performance
        # regression in the future.
        assert recorder.calls.get('GET /trees/assets', 0) == 1
        assert recorder.calls.get('GET /trees/assets/{id}', 0) == 0
        assert recorder.calls.get('GET /items', 0) == 0
        assert recorder.calls.get('GET /items/{id}', 0) == 0
        assert recorder.calls.get('GET /formulas/items/{id}', 0) == 0

    assert len(search_results) == 0
    _assert_result(search_results, 'Success')

    with ApiClientRecorder(spy.session) as recorder:
        search_results = spy.search({
            'Path': 'Example >> Cooling Tower 1'
        }, workbook=spy.GLOBALS_ONLY)

        assert recorder.calls.get('GET /trees/assets', 0) == 1
        assert recorder.calls.get('GET /trees/assets/{id}', 0) == 1
        assert recorder.calls.get('GET /items', 0) == 1
        assert recorder.calls.get('GET /items/{id}', 0) == 1
        assert recorder.calls.get('GET /formulas/items/{id}', 0) == 0

    assert 40 < len(search_results) < 60
    _assert_result(search_results, 'Success')

    with ApiClientRecorder(spy.session) as recorder:
        search_results = spy.search({
            'Path': '*xamp* >> Cooling Tower *',
            'Name': '*Compressor*'
        }, workbook=spy.GLOBALS_ONLY)

        assert recorder.calls.get('GET /trees/assets', 0) == 1
        assert recorder.calls.get('GET /trees/assets/{id}', 0) == 1
        assert recorder.calls.get('GET /items', 0) == 2
        assert recorder.calls.get('GET /items/{id}', 0) == 1
        assert recorder.calls.get('GET /formulas/items/{id}', 0) == 0

    _assert_result(search_results, 'Success')
    names = search_results['Name'].drop_duplicates().tolist()
    assert len(names) == 2
    assert 'Compressor Power' in names
    assert 'Compressor Stage' in names
    paths = search_results['Path'].drop_duplicates().tolist()
    assert len(paths) == 2
    assert 'Example >> Cooling Tower 1' in paths
    assert 'Example >> Cooling Tower 2' in paths

    with ApiClientRecorder(spy.session) as recorder:
        search_results = spy.search({
            'Path': 'Example >> /Cooling Tower [2]/',
            'Name': '*Compressor*'
        }, workbook=spy.GLOBALS_ONLY)

        assert recorder.calls.get('GET /trees/assets', 0) == 1
        assert recorder.calls.get('GET /trees/assets/{id}', 0) == 1
        assert recorder.calls.get('GET /items', 0) == 1
        assert recorder.calls.get('GET /items/{id}', 0) == 1
        assert recorder.calls.get('GET /formulas/items/{id}', 0) == 0

    _assert_result(search_results, 'Success')
    names = search_results['Name'].drop_duplicates().tolist()
    assert len(names) == 2
    assert 'Compressor Power' in names
    assert 'Compressor Stage' in names
    paths = search_results['Path'].drop_duplicates().tolist()
    assert len(paths) == 1
    assert 'Example >> Cooling Tower 2' in paths


@pytest.mark.system
def test_path_search_non_recursive():
    with ApiClientRecorder(spy.session) as recorder:
        search_results = spy.search({
            'Path': 'Exampl',
            'Type': 'Asset'
        }, workbook=spy.GLOBALS_ONLY, recursive=None)

        assert recorder.calls.get('GET /trees/assets', 0) == 1
        assert recorder.calls.get('GET /trees/assets/{id}', 0) == 0
        assert recorder.calls.get('GET /items', 0) == 0
        assert recorder.calls.get('GET /items/{id}', 0) == 0
        assert recorder.calls.get('GET /formulas/items/{id}', 0) == 0

    assert len(search_results) == 0
    _assert_result(search_results, 'Success')

    with ApiClientRecorder(spy.session) as recorder:
        search_results = spy.search({
            'Path': 'Example',
            'Type': 'Asset'
        }, workbook=spy.GLOBALS_ONLY, recursive=None)

        assert recorder.calls.get('GET /trees/assets', 0) == 1
        assert recorder.calls.get('GET /trees/assets/{id}', 0) == 1
        assert recorder.calls.get('GET /items', 0) == 0
        assert recorder.calls.get('GET /items/{id}', 0) == 1
        assert recorder.calls.get('GET /formulas/items/{id}', 0) == 0

    assert len(search_results) == 3
    _assert_result(search_results, 'Success')

    with ApiClientRecorder(spy.session) as recorder:
        search_results = spy.search({
            'Path': 'Example >> Cooling Tower 1',
            'Name': '/Area [ABC]/'
        }, workbook=spy.GLOBALS_ONLY, recursive=False, include_properties=['Data ID'])

        if spy.utils.is_sdk_module_version_at_least(62):
            assert recorder.calls.get('GET /trees/assets', 0) == 1
            assert recorder.calls.get('GET /trees/assets/{id}', 0) == 2
            assert recorder.calls.get('GET /items', 0) == 1
            assert recorder.calls.get('GET /items/{id}', 0) == 1
            assert recorder.calls.get('GET /formulas/items/{id}', 0) == 0
        else:
            assert recorder.calls.get('GET /trees/assets', 0) == 1
            assert recorder.calls.get('GET /trees/assets/{id}', 0) == 2
            assert recorder.calls.get('GET /items', 0) == 0
            assert recorder.calls.get('GET /items/{id}', 0) == 4
            assert recorder.calls.get('GET /formulas/items/{id}', 0) == 0

    assert len(search_results) == 3
    assert 'Data ID' in search_results.columns
    _assert_result(search_results, 'Success')
    types = search_results['Type'].drop_duplicates().tolist()
    assert len(types) == 1
    assert types[0] == 'Asset'

    with ApiClientRecorder(spy.session) as recorder:
        search_results = spy.search({
            'Path': '*xamp* >> Cooling Tower *',
            'Asset': 'Area A',
            'Name': '*Compressor*'
        }, workbook=spy.GLOBALS_ONLY, recursive=False, all_properties=True)

        if spy.utils.is_sdk_module_version_at_least(62):
            assert recorder.calls.get('GET /trees/assets', 0) == 1
            assert recorder.calls.get('GET /trees/assets/{id}', 0) == 6
            assert recorder.calls.get('GET /items', 0) == 1
            assert recorder.calls.get('GET /items/{id}', 0) == 1
            assert recorder.calls.get('GET /formulas/items/{id}', 0) == 0
        else:
            assert recorder.calls.get('GET /trees/assets', 0) == 1
            assert recorder.calls.get('GET /trees/assets/{id}', 0) == 6
            assert recorder.calls.get('GET /items', 0) == 0
            assert recorder.calls.get('GET /items/{id}', 0) == 3
            assert recorder.calls.get('GET /formulas/items/{id}', 0) == 0

    assert len(search_results) == 2
    assert 'Source Value Unit Of Measure' in search_results.columns
    assert 'Maximum Interpolation' in search_results.columns
    assert 'Source Maximum Interpolation' in search_results.columns
    _assert_result(search_results, 'Success')
    names = search_results['Name'].tolist()
    assert len(names) == 2
    assert 'Compressor Power' in names
    assert 'Compressor Stage' in names
    paths = search_results['Path'].drop_duplicates().tolist()
    assert len(paths) == 1
    assert paths[0] == 'Example >> Cooling Tower 1'


@pytest.mark.system
def test_type_search_non_recursive():
    workbook = 'test_type_search_non_recursive'
    path = 'Test Type Non Recursive'

    area_a_signals = spy.search({
        'Path': 'Example >> Cooling Tower 1 >> Area A'
    })

    spy.push(metadata=pd.DataFrame([{
        'Type': 'Signal',
        'Name': 'Dew Point',
        'Path': path,
        'Formula': "$T - ((100 - $RH.setUnits(''))/5)",
        'Formula Parameters': {
            '$T': area_a_signals[area_a_signals['Name'] == 'Temperature'],
            '$RH': area_a_signals[area_a_signals['Name'] == 'Relative Humidity']
        }
    }]), workbook=workbook, worksheet=None)

    condition_data = pd.DataFrame([{
        'Capsule Start': pd.to_datetime('2019-01-01', utc=True),
        'Capsule End': pd.to_datetime('2019-01-02', utc=True),
    }, {
        'Capsule Start': pd.to_datetime('2019-01-03', utc=True),
        'Capsule End': pd.to_datetime('2019-01-04', utc=True),
    }])
    condition_metadata = pd.DataFrame([{
        'Path': path,
        'Name': 'My Condition',
        'Type': 'Condition',
        'Maximum Duration': '2 days'
    }])

    spy.push(condition_data, metadata=condition_metadata, workbook=workbook)
    search_results = spy.search({
        'Path': path,
        'Type': 'CalculatedSignal'
    }, workbook=workbook, recursive=False)

    _assert_result(search_results, 'Success')
    types = set(search_results['Type'].tolist())
    assert len(types) == 1
    assert 'CalculatedSignal' in types

    search_results = spy.search({
        'Path': path,
        'Type': 'StoredCondition'
    }, workbook=workbook, recursive=False)

    _assert_result(search_results, 'Success')
    types = set(search_results['Type'].tolist())
    assert len(types) == 1
    assert 'StoredCondition' in types


@pytest.mark.skip(reason="CRAB-45869")
@pytest.mark.system
def test_path_search_pagination():
    session = test_common.get_session(Sessions.test_path_search_pagination)
    # This tests the 'Path' finding code to make sure we'll find a path even if pagination
    # is required.
    session.options.search_page_size = 1
    search_results = spy.search({
        'Path': 'Example >> Cooling Tower 1 >> Area G'
    }, workbook=spy.GLOBALS_ONLY, recursive=False, session=session)

    assert len(search_results) == 6
    _assert_result(search_results, 'Success')
    assert search_results.spy.status.df.iloc[0]['Count'] == 6
    assert search_results.spy.status.df.iloc[0]['Pages'] == 7


@pytest.mark.system
def test_path_search_root_only():
    search_results = spy.search(pd.DataFrame({
        'Path': [''],
        'Name': ['Example'],
        'Type': ['Asset']
    }), workbook=spy.GLOBALS_ONLY, recursive=False, old_asset_format=False)

    assert len(search_results) == 1
    _assert_result(search_results, 'Success')
    assert search_results.iloc[0]['Type'] == 'Asset'
    assert search_results.iloc[0]['Name'] == 'Example'
    assert search_results.iloc[0]['Asset'] == 'Example'
    assert search_results.iloc[0]['Path'] == ''

    search_results = spy.search(pd.DataFrame({
        'Path': [''],
        'Name': ['Example'],
        'Type': ['Asset']
    }), workbook=spy.GLOBALS_ONLY, recursive=False, old_asset_format=True)

    assert search_results.iloc[0]['Type'] == 'Asset'
    assert search_results.iloc[0]['Name'] == 'Example'
    assert 'Asset' not in search_results.iloc[0]
    assert 'Path' not in search_results.iloc[0]


@pytest.mark.system
def test_asset_search():
    try:
        spy.session.options.compatibility = 191
        search_results = spy.search({
            'Path': 'Example >> Cooling Tower 1',
            'Asset': 'Area A',
            'Name': 'Area A'
        }, workbook=spy.GLOBALS_ONLY)

        assert len(search_results) == 0

        search_results = spy.search({
            'Path': 'Example >> Cooling Tower 1',
            'Asset': 'Area A',
        }, workbook=spy.GLOBALS_ONLY, all_properties=True)

        assert len(search_results) == 6
        assert len(search_results[search_results['Type'] == 'Asset']) == 0

    finally:
        spy.session.options.compatibility = None

    search_results = spy.search({
        'Path': 'Example >> Cooling Tower 1',
        'Asset': 'Area A',
        'Name': 'Area A'
    }, workbook=spy.GLOBALS_ONLY, include_properties=['Path', 'Asset'])

    assert len(search_results) == 1
    asset = search_results.iloc[0]
    assert asset['Type'] == 'Asset'
    assert asset['Name'] == 'Area A'
    assert asset['Asset'] == 'Cooling Tower 1'
    assert asset['Path'] == 'Example'
    assert 'Sync Token' not in asset

    search_results = spy.search({
        'Path': 'Example >> Cooling Tower 1',
        'Asset': 'Ar?a A',
        'Name': '/[Aa]rea [Aa]/'
    }, workbook=spy.GLOBALS_ONLY, all_properties=True, old_asset_format=False)

    assert len(search_results) == 1
    asset = search_results.iloc[0]
    assert asset['Type'] == 'Asset'
    assert asset['Name'] == 'Area A'
    assert asset['Asset'] == 'Area A'
    assert asset['Path'] == 'Example >> Cooling Tower 1'
    assert 'Sync Token' in asset

    search_results = spy.search({
        'Path': 'Example >> Cooling Tower 1',
        'Asset': 'Area A',
        'Name': 'Temperature'
    }, workbook=spy.GLOBALS_ONLY, all_properties=True, old_asset_format=False)

    assert len(search_results) == 1
    asset = search_results.iloc[0]
    assert asset['Type'] == 'StoredSignal'
    assert asset['Name'] == 'Temperature'
    assert asset['Asset'] == 'Area A'
    assert asset['Path'] == 'Example >> Cooling Tower 1'
    assert 'Sync Token' in asset

    search_results = spy.search({
        'Path': 'Example >> Cooling Tower 1',
        'Asset': 'Area A',
    }, workbook=spy.GLOBALS_ONLY, all_properties=True)

    assert len(search_results) == 7
    asset = search_results[search_results['Type'] == 'Asset'].iloc[0]
    assert asset['Type'] == 'Asset'
    assert asset['Name'] == 'Area A'
    assert asset['Asset'] == 'Cooling Tower 1'
    assert asset['Path'] == 'Example'
    assert 'Sync Token' in asset

    search_results = spy.search({
        'Path': 'Example >> Cooling Tower 1',
        'Asset': 'Area A',
    }, workbook=spy.GLOBALS_ONLY, all_properties=True, old_asset_format=False)

    assert len(search_results) == 7
    asset = search_results[search_results['Type'] == 'Asset'].iloc[0]
    assert asset['Type'] == 'Asset'
    assert asset['Name'] == 'Area A'
    assert asset['Asset'] == 'Area A'
    assert asset['Path'] == 'Example >> Cooling Tower 1'
    assert 'Sync Token' in asset

    search_results = spy.search({
        'Path': 'Example >> Cooling Tower 1',
        'Asset': 'Area A',
        'Type': 'Signal'
    }, workbook=spy.GLOBALS_ONLY, all_properties=True, old_asset_format=False)

    assert len(search_results) == 6
    asset = search_results[search_results['Type'] == 'Asset']
    assert len(asset) == 0

    search_results = spy.search({
        'Path': 'Example >> Cooling Tower 1',
        'Asset': 'Area A',
        'Type': 'Asset'
    }, workbook=spy.GLOBALS_ONLY, all_properties=True, old_asset_format=False)

    assert len(search_results) == 1
    asset = search_results[search_results['Type'] == 'Asset']
    assert len(asset) == 1

    search_results = spy.search({
        'Path': 'Example >> Cooling Tower 1',
        'Asset': 'Area A',
        'Type': ['Asset', 'Signal']
    }, workbook=spy.GLOBALS_ONLY, all_properties=True, old_asset_format=False)

    assert len(search_results) == 7
    asset = search_results[search_results['Type'] == 'Asset'].iloc[0]
    assert asset['Type'] == 'Asset'
    assert asset['Name'] == 'Area A'
    assert asset['Asset'] == 'Area A'
    assert asset['Path'] == 'Example >> Cooling Tower 1'
    assert 'Sync Token' in asset


@pytest.mark.system
def test_asset_id_search():
    search_results = spy.search({
        'Path': 'Example >> Cooling Tower 1',
        'Type': 'Asset'
    }, workbook=spy.GLOBALS_ONLY)

    _assert_result(search_results, 'Success')

    assert len(search_results.spy.status.warnings) == 1
    assert 'This search result includes Assets' in search_results.spy.status.warnings.pop()

    search_results = spy.search({
        'Path': 'Example >> Cooling Tower 1',
        'Type': 'Asset'
    }, workbook=spy.GLOBALS_ONLY, old_asset_format=True)
    assert len(search_results.spy.status.warnings) == 0

    asset_id = search_results[search_results['Name'] == 'Area C'].iloc[0]['ID']
    asset_search = spy.search({'Asset': asset_id}, workbook=spy.GLOBALS_ONLY)

    assert len(asset_search) > 1
    _assert_result(asset_search, 'Success')
    assert all([a == 'Area C' for a in asset_search['Asset'].tolist()])

    asset_search = spy.search({'Asset': asset_id, 'Name': 'Temperature'},
                              workbook=spy.GLOBALS_ONLY)

    assert len(asset_search) == 1
    _assert_result(asset_search, 'Success')
    asset = asset_search.iloc[0]
    assert asset['Asset'] == 'Area C'
    assert asset['Name'] == 'Temperature'


@pytest.mark.system
def test_datasource_name_search():
    with pytest.raises(RuntimeError):
        spy.search({
            'Datasource Name': 'Non-existent'
        }, workbook=spy.GLOBALS_ONLY)

    search_results = spy.search({
        'Datasource Name': 'Example Data'
    }, workbook=spy.GLOBALS_ONLY)

    assert 250 < len(search_results) < 345
    _assert_result(search_results, 'Success')


@pytest.mark.system
def test_search_pagination():
    session = test_common.get_session(Sessions.test_search_pagination)
    session.options.search_page_size = 2
    search_results = spy.search({
        'Name': 'Area A_*'
    }, workbook=spy.GLOBALS_ONLY, session=session)

    assert len(search_results) == 6
    assert search_results.spy.status.df.iloc[0]['Pages'] == 4
    _assert_result(search_results, 'Success')


@pytest.mark.system
def test_search_bad_workbook():
    with pytest.raises(RuntimeError):
        spy.search({
            'Name': 'Area A_*'
        }, workbook='bad')


@pytest.mark.system
def test_search_workbook_guid():
    # The workbook won't be found, so we'll get an access error
    with pytest.raises(ApiException):
        spy.search({
            'Name': 'Area A_*'
        }, workbook='A0B89103-E95D-4E32-A809-390C1FAE9D2F')


@pytest.mark.system
def test_include_archived():
    metadata_df = pd.DataFrame([{
        'Type': 'Signal',
        'Name': 'test_include_archived 1'
    }, {
        'Type': 'Signal',
        'Name': 'test_include_archived 2'
    }])
    metadata_df['Archived'] = True
    workbook = 'test_include_archived'
    push_df = spy.push(metadata=metadata_df, workbook=workbook, worksheet=None, datasource=workbook)

    search_df = spy.search({'Name': 'test_include_archived*', 'Scoped To': push_df.spy.workbook_id})
    assert len(search_df) == 0
    _assert_result(search_df, 'Success')
    search_df = spy.search({'Name': 'test_include_archived*', 'Scoped To': push_df.spy.workbook_id},
                           include_archived=True)
    assert len(search_df) == len(metadata_df)
    _assert_result(search_df, 'Success')

    with pytest.raises(ValueError):
        spy.search({'Path': 'Example', 'Datasource ID': 'Example Data'},
                   workbook=spy.GLOBALS_ONLY, recursive=False, include_archived=True)


@pytest.mark.system
def test_simple_search_with_estimated_sample_period():
    search_results = spy.search({
        'Name': 'Area A_Temper'
    }, workbook=spy.GLOBALS_ONLY, estimate_sample_period=dict(Start=None, End=None))

    assert len(search_results) == 1
    _assert_result(search_results, 'Success')
    assert 'Estimated Sample Period' in search_results
    assert (search_results['Estimated Sample Period'].map(type) == pd.Timedelta).all()

    search_results = spy.search(pd.DataFrame([{'Name': 'Area A_Temperature'}]),
                                workbook=spy.GLOBALS_ONLY,
                                estimate_sample_period=dict(Start='2018-01-01T00:00:00.000Z',
                                                            End='2018-06-01T00:00:00.000Z'))

    assert len(search_results) == 1
    _assert_result(search_results, 'Success')
    assert 'Estimated Sample Period' in search_results
    assert (search_results['Estimated Sample Period'].map(type) == pd.Timedelta).all()
    assert search_results.at[0, 'Estimated Sample Period'] == pd.to_timedelta(120.0, unit='s')


@pytest.mark.system
def test_dataframe_with_estimated_sample_period():
    search_results = spy.search(pd.DataFrame([{
        'Name': 'Area A_Relative Humidity',
        'Datasource Name': 'Example Data'
    }, {
        'Name': 'Area A_Temperature',
        'Datasource Name': 'Example Data'
    }, {
        'Path': 'Example >> Cooling Tower 1 >> Area A',
        'Name': 'Relative Humidity'
    }]), workbook=spy.GLOBALS_ONLY, estimate_sample_period=dict(Start=None, End=None))

    assert len(search_results) == 3
    _assert_result(search_results, 'Success')
    assert 'Estimated Sample Period' in search_results
    assert (search_results['Estimated Sample Period'].map(type) == pd.Timedelta).all()


@pytest.mark.system
def test_path_with_datasource_and_estimated_sample_period():
    search_results = spy.search({
        'Name': 'Area ?_*',
        'Datasource Name': 'Example Data'
    }, workbook=spy.GLOBALS_ONLY)

    assert 'Estimated Sample Period' not in search_results
    push_df = search_results.copy()
    push_df['Reference'] = True
    push_df['Path'] = 'test_path_with_datasource-tree >> branch-alpha'
    push_df['Asset'] = push_df['Name'].str.extract(r'(Area .)_.*')
    push_df['Name'] = push_df['Name'].str.extract(r'Area ._(.*)')
    workbook = 'test_path_with_datasource_and_estimated_sample_period'

    spy.push(metadata=push_df, datasource=f'{workbook}-1', workbook=workbook, worksheet=None)
    push_df['Path'] = 'test_path_with_datasource-tree >> branch-bravo'
    spy.push(metadata=push_df, datasource=f'{workbook}-2', workbook=workbook, worksheet=None)

    search_results = spy.search({
        'Path': 'test_path_with_datasource-tree',
    }, workbook=workbook, recursive=False, estimate_sample_period=dict(Start=None, End=None))

    assert len(search_results) == 2
    _assert_result(search_results, 'Success')
    assert 'Estimated Sample Period' in search_results
    assert search_results['Type'].values[0] == 'Asset' and pd.isna(search_results.at[0, 'Estimated Sample Period'])


@pytest.mark.system
def test_not_enough_data_for_estimated_sample_period():
    search_results = spy.search({
        'Name': 'Area ?_*',
        'Datasource Name': 'Example Data'
    },
        workbook=spy.GLOBALS_ONLY,
        estimate_sample_period=dict(Start='2018-01-01T01:00:00.000Z', End='2018-01-01T02:00:00.000Z'))
    assert 'Estimated Sample Period' in search_results
    _assert_result(search_results, 'Success')
    area_f_compressor_power = search_results[search_results['Name'] == 'Area F_Compressor Power'].squeeze()
    assert pd.isna(area_f_compressor_power['Estimated Sample Period'])


@pytest.mark.system
def test_estimated_sample_period_incorrect_capitalization():
    with pytest.raises(ValueError, match=r"estimate_sample_period must have 'Start' and 'End' keys but got "
                                         r"dict_keys\(\['start', 'End'\]\)"):
        spy.search({
            'Name': 'Area ?_*',
            'Datasource Name': 'Example Data'
        },
            workbook=spy.GLOBALS_ONLY,
            estimate_sample_period=dict(start='2018-01-01T01:00:00.000Z', End='2018-01-01T02:00:00.000Z'))

    with pytest.raises(ValueError, match=r"estimate_sample_period must have 'Start' and 'End' keys but got "
                                         r"dict_keys\(\['Start', 'end'\]\)"):
        spy.search({
            'Name': 'Area ?_*',
            'Datasource Name': 'Example Data'
        },
            workbook=spy.GLOBALS_ONLY,
            estimate_sample_period=dict(Start='2018-01-01T01:00:00.000Z', end='2018-01-01T02:00:00.000Z'))


@pytest.mark.system
def test_search_with_url():
    workbook = test_common.create_worksheet_for_url_tests('test_search_with_url')

    with ApiClientRecorder(spy.session) as recorder:
        search_results = spy.search(workbook.url, workbook=spy.GLOBALS_ONLY)

        # Make sure we don't make any unnecessary calls -- i.e., that include_access_control is False and
        # include_annotations is False
        assert recorder.calls.get('GET /users/{id}', 0) == 0
        assert recorder.calls.get('GET /items/{id}/acl', 0) == 0
        assert recorder.calls.get('GET /annotations', 0) == 0
        assert recorder.calls.get('GET /annotations/{id}', 0) == 0

    # CRAB-37888: We are now forcing SPy scalars to be CalculatedScalar, so they can be edited in the UI
    literal_scalar_type = 'CalculatedScalar'

    assert len(search_results) == 3
    _assert_result(search_results, 'Success')
    assert search_results[search_results['Name'] == 'Temperature Minus 5']['Type'].values[0] == 'CalculatedSignal'
    assert search_results[search_results['Name'] == 'Cold']['Type'].values[0] == 'CalculatedCondition'
    assert search_results[search_results['Name'] == 'Constant']['Type'].values[0] == literal_scalar_type

    search_results = spy.search(workbook.url, workbook=spy.GLOBALS_ONLY,
                                estimate_sample_period=dict(Start='2018-01-01T00:00:00.000Z',
                                                            End='2018-06-01T00:00:00.000Z'))
    assert 'Estimated Sample Period' in search_results


@pytest.mark.system
def test_search_quiet_flag_with_url():
    workbook = test_common.create_worksheet_for_url_tests('test_search_quiet_flag_with_url')
    quiet_error = "Invalid arguments: The quiet flag of the supplied status object is"
    errors_error = "Invalid arguments: The errors flag of the supplied status object is"

    # Test that there is no error when a status is provided and the quiet/errors flags are not set
    test_status_no_error = spy.Status()
    _ = spy.search(workbook.url, status=test_status_no_error)
    assert len(test_status_no_error.warnings) == 0

    # Test that there is an error when both a status is provided and the quiet/errors flag is supplied
    test_status_error = spy.Status()
    with pytest.raises(SPyValueError, match=quiet_error):
        spy.search(workbook.url, quiet=True, status=test_status_error)
    with pytest.raises(SPyValueError, match=errors_error):
        spy.search(workbook.url, errors='catalog', status=test_status_error)


@pytest.mark.system
def test_search_with_url_wrong_url():
    workbook = 'test_search_with_url_wrong_url'
    test_common.create_worksheet_for_url_tests(workbook)
    # test for invalid URLs
    with pytest.raises(ValueError, match=r"The supplied URL is not a valid Seeq address. Verify that both the "
                                         r"workbook ID and worksheet ID are valid Seeq references"):
        spy.search('http://localhost:34216/workbook/376F44F5-9243-A0CF/worksheet/2B7F2EC3-C484-49C6-9FEB-EDA68B9350B1')

    with pytest.raises(ValueError, match=r"The supplied URL is not a valid Seeq address. Verify that both the "
                                         r"workbook ID and worksheet ID are valid Seeq references"):
        spy.search('http://localhost:34216/workbook/376F44F5-9243-453C-A0CF-F14CB08B76FD/worksheet/2B7F2EC3-C484-49C6')

    workbook_id = str(uuid.uuid1()).upper()
    worksheet_id = str(uuid.uuid1()).upper()
    url = f'http://localhost:34216/workbook/{workbook_id}/worksheet/{worksheet_id}'
    with pytest.raises(RuntimeError, match=f'Could not find workbook with ID "{workbook_id}"'):
        spy.search(url)

    host = spy.session.public_url
    workbook_search = spy.workbooks.search({'Name': workbook})
    workbook = spy.workbooks.pull(workbook_search, include_referenced_workbooks=False, include_inventory=False)[0]
    with pytest.raises(RuntimeError, match=f'Worksheet with ID "{worksheet_id}" does not exist'):
        spy.search(f'{host}/workbook/{workbook.id}/worksheet/{worksheet_id}')


@pytest.mark.system
def test_search_with_url_archived_workbook_worksheet():
    # Get a workbook/worksheet URL for tests
    test_common.create_worksheet_for_url_tests('test_search_with_url_archived_workbook_worksheet')
    workbook_search = spy.workbooks.search({'Name': 'test_search_with_url_archived_workbook_worksheet'})
    workbook = spy.workbooks.pull(workbook_search, include_referenced_workbooks=False, include_inventory=False)[0]
    worksheet = [x for x in workbook.worksheets if x.name == 'search from URL'][0]

    items_api = ItemsApi(spy.session.client)
    items_api.set_property(id=workbook.id, property_name='Archived', body=PropertyInputV1(value=True))

    # tests for archived workbooks
    with pytest.raises(ValueError, match=f"Workbook '{workbook.id}' is archived. Supply 'include_archived=True' if "
                                         f"you want to retrieve the items of an archived workbook"):
        spy.search(workbook.url)

    search_results = spy.search(workbook.url, include_archived=True)
    assert len(search_results) == 3
    _assert_result(search_results, 'Success')

    # unarchive it in case we need it for another test
    items_api.set_property(id=workbook.id, property_name='Archived', body=PropertyInputV1(value=False))

    # tests for archived worksheet
    items_api.set_property(id=worksheet.id, property_name='Archived', body=PropertyInputV1(value=True))
    with pytest.raises(ValueError, match=f"Worksheet '{worksheet.id}' is archived. Supply 'include_archived=True' if "
                                         f"you want to retrieve archived items"):
        spy.search(workbook.url)

    # unarchive it in case we need it for another test
    items_api.set_property(id=worksheet.id, property_name='Archived', body=PropertyInputV1(value=False))


@pytest.mark.system
def test_search_of_topic_url():
    host = spy.session.public_url
    topic = Topic({'Name': "test_Topic_search_url"})
    document = topic.document('Doc1')
    spy.workbooks.push(topic)

    with pytest.raises(ValueError, match=f'URL must be for a valid Workbench Analysis. '
                                         f'You supplied a URL for a Topic.'):
        spy.search(f'{host}/workbook/{topic.id}/worksheet/{document.id}')


@pytest.mark.system
def test_search_kwargs_and_status_metadata():
    search_results = spy.search({'Name': 'Area A_Temperature'}, workbook=spy.GLOBALS_ONLY)
    with tempfile.TemporaryDirectory() as dir_path:
        search_results.to_pickle(str(Path(dir_path, 'search.pkl')))
        search_unpickle = pd.read_pickle(Path(dir_path, 'search.pkl'))
        assert search_unpickle.spy.func == 'spy.search'
        assert search_unpickle.spy.kwargs['query'] == {'Name': 'Area A_Temperature'}
        assert not search_unpickle.spy.kwargs['all_properties']
        assert search_unpickle.spy.kwargs['recursive']
        assert not search_unpickle.spy.kwargs['include_archived']
        assert search_unpickle.spy.kwargs['estimate_sample_period'] is None
        assert isinstance(search_unpickle.spy.status, spy.Status)

        # take a slice of the DataFrame to make sure the metadata is preserved
        search_manipulated = search_unpickle[['ID']]
        assert search_manipulated.spy.kwargs['query'] == {'Name': 'Area A_Temperature'}

    search_results = spy.search({'Name': 'Area A_Temperature'},
                                workbook=spy.GLOBALS_ONLY,
                                estimate_sample_period=dict(Start='2018-01-01T01:00:00.000Z',
                                                            End='2018-01-01T02:00:00.000Z'))
    _assert_result(search_results, 'Success')

    with tempfile.TemporaryDirectory() as dir_path:
        search_results.to_pickle(str(Path(dir_path, 'search.pkl')))
        search_unpickle = pd.read_pickle(Path(dir_path, 'search.pkl'))
        assert search_unpickle.spy.func == 'spy.search'
        assert search_unpickle.spy.kwargs['estimate_sample_period']['Start'] == '2018-01-01T01:00:00.000Z'
        assert search_unpickle.spy.kwargs['estimate_sample_period']['End'] == '2018-01-01T02:00:00.000Z'

        # create a copy of the search DataFrame and test the metadata is preserved on the copy
        duplicate = search_unpickle.copy()
        assert duplicate.spy.kwargs['estimate_sample_period']['Start'] == '2018-01-01T01:00:00.000Z'
        assert duplicate.spy.kwargs['estimate_sample_period']['End'] == '2018-01-01T02:00:00.000Z'


@pytest.mark.system
def test_ignore_unindexed_properties():
    with pytest.raises(ValueError, match=r'The following properties are not indexed:\n"Bilbo"\nUse any of .*'):
        spy.search({
            'Name': 'Area A_Temperature',
            'Bilbo': 'Baggins'
        }, workbook=spy.GLOBALS_ONLY, ignore_unindexed_properties=False)

    search_df = spy.search({
        'Name': 'Area A_Temperature',
        'Bilbo': 'Baggins'
    }, workbook=spy.GLOBALS_ONLY)

    assert len(search_df.spy.status.warnings) == 1
    _assert_result(search_df, 'Success')
    assert search_df.spy.status.warnings.pop().startswith(
        'The following properties are not indexed and will be ignored:\n"Bilbo"')


@pytest.mark.system
def test_order_by():
    _test_order_by_internal(spy.session)


def _test_order_by_internal(session: Session):
    # test each of "ID", "Name", "Description"

    # test ID as a list
    query = {'Name': 'Compressor'}
    id_results = spy.search(query, order_by=['ID'], workbook=spy.GLOBALS_ONLY, session=session)
    _assert_result(id_results, 'Success')
    id_list = list(id_results['ID'])
    assert id_list == sorted(id_list)
    assert id_results.spy.status.df.iloc[0]['Pages'] == int(len(id_results) / session.options.search_page_size) + 1

    # test ID as a string
    id_str_results = spy.search(query, order_by='ID', workbook=spy.GLOBALS_ONLY, session=session)
    _assert_result(id_str_results, 'Success')
    id_str_list = list(id_str_results['ID'])
    assert id_str_list == sorted(id_str_list)

    # Name
    name_results = spy.search(query, order_by=['Name'], workbook=spy.GLOBALS_ONLY, session=session)
    _assert_result(name_results, 'Success')
    name_list = list(name_results['Name'])
    assert name_list == sorted(name_list)

    # Description
    descript_results = spy.search(query, order_by=['Description'], workbook=spy.GLOBALS_ONLY, session=session)
    _assert_result(descript_results, 'Success')
    descript_list = list(descript_results['Description'].dropna())
    assert descript_list == sorted(descript_list)

    # test order_by works for multiple fields
    # if ["ID", "Name"] - should just be sorted by ID
    results = spy.search(query, order_by=['ID', 'Name'], workbook=spy.GLOBALS_ONLY, session=session)
    _assert_result(results, 'Success')
    id_list_2 = list(results['ID'])
    assert id_list_2 == sorted(id_list_2)

    # if ["Name", "ID"] - should be sorted by name and within same name, sorted by ID
    name_id_results = spy.search(query, order_by=['Name', 'ID'], workbook=spy.GLOBALS_ONLY, session=session)
    _assert_result(name_id_results, 'Success')
    copy_results = name_id_results.copy()
    copy_results = copy_results.sort_values(by=['Name', 'ID'])
    assert list(name_id_results['ID']) == list(copy_results['ID'])


@pytest.mark.system
def test_order_by_validation():
    query = {'Name': 'Compressor'}

    # test order_by validation works
    # test if order_by not a list or string
    with pytest.raises(TypeError, match="Argument 'order_by' should be type str or list, but is type int"):
        spy.search(query, order_by=3, workbook=spy.GLOBALS_ONLY)

    # test if order_by contains something other than "Name", "ID", "Description"
    # test with no valid field in list
    message = re.escape("Invalid order_by fields: ['Type']. Search results can only be ordered on ['ID', 'Name', "
                        "'Description'] fields.")
    with pytest.raises(ValueError, match=message):
        spy.search(query, order_by=['Type'], workbook=spy.GLOBALS_ONLY)

    # test with some valid field in list
    with pytest.raises(ValueError, match=message):
        spy.search(query, order_by=['Name', 'Type'], workbook=spy.GLOBALS_ONLY)

    # test with invalid string
    with pytest.raises(ValueError, match=message):
        spy.search(query, order_by='Type', workbook=spy.GLOBALS_ONLY)


@pytest.mark.system
def test_order_by_page_size():
    session = test_common.get_session(Sessions.test_order_by_page_size)

    # test it is correctly ordered if search_page_size < items

    # set search page size low
    session.options.search_page_size = 2
    # use the same tests as in test_order_by() with new config settings
    _test_order_by_internal(session)


@pytest.mark.system
def test_empty_query():
    # This can happen if the user imports a CSV file full of tag names into a DataFrame but the CSV file doesn't
    # have a header.
    query_df = pd.DataFrame({
        'DC3STEAMSO.INITIAL_VALUE': [
            'DC3STEAMSO.FINAL_VALUE',
            'DC3STEAMSO.MOVE',
            'DC3STEAMSO.BASE_CASE'
        ]})

    with pytest.raises(ValueError, match=r'No recognized properties present in "query" argument.'):
        spy.search(query_df, workbook=spy.GLOBALS_ONLY)

    with pytest.raises(ValueError, match=r'No recognized properties present in "query" argument.'):
        spy.search(dict(), workbook=spy.GLOBALS_ONLY)


@pytest.mark.system
def test_search_in_corporate_workbook():
    # Previously, other than specifying a workbook ID, there was no way to search in a workbook that is in the
    # corporate folder (or any other root directories than My Folder).
    workbook_name = f'test_sections_{_common.new_placeholder_guid()}'
    workbook = Analysis(workbook_name)
    workbook.worksheet('The Worksheet')
    spy.workbooks.push(workbook, path=f'{spy.workbooks.CORPORATE} >> test_search_in_corporate_workbook')

    # Search for the workbook by specifying the Corporate directory in the path
    wb_search_df = spy.workbooks.search({'Name': workbook_name,
                                         'Path': f'{spy.workbooks.CORPORATE} >> test_search_in_corporate_workbook'})

    wb_id = wb_search_df['ID'][0]

    # Check that the workbook ID in the corporate drive is correct
    assert wb_id == workbook.id

    # Push a signal to the workbook which is under corporate drive
    metadata = pd.DataFrame([{
        'Type': 'Scalar',
        'Name': 'Test Negative Number in a Corporate Drive Workbook',
        'Formula': np.int64(-12)
    }])

    spy.push(metadata=metadata, workbook=wb_id, worksheet=None)

    # Perform a spy.search in the corporate drive
    search_df = spy.search({
        'Name': 'Test Negative Number in a Corporate Drive Workbook',
    }, workbook=f'{spy.workbooks.CORPORATE} >> test_search_in_corporate_workbook >> {workbook_name}')

    # Check that spy.search() in the corporate drive found the signal
    assert len(search_df) > 0
    _assert_result(search_df, 'Success')


@pytest.mark.system
def test_archived():
    push_results = spy.push(
        metadata=pd.DataFrame([{'Name': 'My Calc %s' % _common.new_placeholder_guid(), 'Formula': 'sinusoid()'}]))

    search_results = spy.search({'ID': push_results.at[0, 'ID']})
    assert search_results.at[0, 'Archived'] is np.False_

    items_api = ItemsApi(spy.client)
    items_api.archive_item(id=push_results.at[0, 'ID'])
    search_results = spy.search({'ID': push_results.at[0, 'ID']})
    assert search_results.at[0, 'Archived'] is np.True_


@pytest.mark.system
def test_redacted_item_properties():
    reason = 'No can do'
    mock_exception_thrower = mock.Mock(side_effect=ApiException(status=404, reason=reason))
    with unittest.mock.patch('seeq.spy._search._add_included_property_to_dict', new=mock_exception_thrower):
        # Verify this blows up with a default `errors` setting
        with pytest.raises(ApiException, match=reason):
            spy.search({'Name': 'Area A_Temperature'}, all_properties=True)

        status = spy.Status(errors='catalog')
        search_results = spy.search({'Name': 'Area A_Temperature'},
                                    all_properties=True, status=status)

        # Assert that the dataframe includes the basic information from the search
        assert len(search_results.index) == 1
        assert search_results.at[0, 'Name'] == 'Area A_Temperature'
        assert search_results.at[0, 'Type'] == 'StoredSignal'

        # But the additional properties that would be pulled from the get_item_and_all_properties() are not present
        expected_missing_properties = ['Datasource Class', 'Datasource ID', 'Data ID', 'Maximum Interpolation']
        assert not any(item in expected_missing_properties for item in search_results.columns)

        # And that the status has a warning
        assert len(status.warnings) == 1
        warning = list(status.warnings)[0]
        assert 'Failed to get included item properties' in warning
        assert f'because it was not found: "(404) {reason}"' in warning


@pytest.mark.system
def test_redacted_search_by_id():
    if not _login.is_server_version_at_least(62):
        # This test only applies to R62+ because of the expanded Search-by-ID feature. R61- will return a normal
        # 403 error.
        return

    # Mock a redacted item
    redacted_id = _common.new_placeholder_guid()
    reason = f'The item {redacted_id} was not found or the acting user does not have permission to read it'
    mock_exception_thrower = mock.Mock(side_effect=ApiException(status=403, reason=reason))
    with unittest.mock.patch('seeq.sdk.ItemsApi.get_item_and_all_properties', new=mock_exception_thrower):
        # Verify this blows up with a default `errors` setting
        with pytest.raises(ApiException, match=reason):
            spy.search({'ID': redacted_id}, all_properties=True)

        # And that it works for errors='catalog'
        status = spy.Status(errors='catalog')
        search_results = spy.search({'ID': redacted_id}, all_properties=True, status=status)

        # The dataframe should have an ID, but no other properties
        assert len(search_results.index) == 1
        assert search_results.at[0, 'ID'] == redacted_id

        # And the status should have a warning
        assert len(status.warnings) == 1
        warning = list(status.warnings)[0]
        assert redacted_id in warning
        assert reason in warning


@pytest.mark.system
def test_redacted_item_formula_params():
    if _login.is_server_version_at_least(62):
        # In R62 and later, the server does not need to make a separate call to request formula parameters,
        # so this scenario can no longer happen.
        return

    search_name = 'test_bad_item_formula_params'

    # Ensure there's a calculation to search for
    push_name = search_name + _common.new_placeholder_guid()
    push_results = spy.push(metadata=pd.DataFrame([{'Name': push_name, 'Formula': 'sinusoid()'}]), workbook=None)
    assert len(push_results.index) == 1

    reason = 'How about no'
    mock_exception_thrower = mock.Mock(side_effect=ApiException(status=500, reason=reason))
    with unittest.mock.patch('seeq.sdk.FormulasApi.get_item', new=mock_exception_thrower):
        # Verify this blows up with a default `errors` setting
        with pytest.raises(ApiException, match=reason):
            spy.search({'Name': search_name}, workbook=spy.GLOBALS_ONLY, all_properties=True)

        status = spy.Status(errors='catalog')
        search_results = spy.search({
            'Name': search_name
        }, workbook=spy.GLOBALS_ONLY, all_properties=True, status=status)

        # Assert that the dataframe includes the properties from the search and get_item_and_all_properties
        assert len(search_results.index) >= 1
        assert search_name in search_results.at[0, 'Name']
        assert search_results.at[0, 'Type'] == 'CalculatedSignal'
        assert search_results.at[0, 'Formula'] == 'sinusoid()'
        assert search_results.at[0, 'Datasource Name'] == 'Seeq Data Lab'

        # But the parameters are not present
        assert 'Formula Parameters' not in search_results.columns

        # And that the status has a warning
        assert len(status.warnings) >= 1
        warning = list(status.warnings)[0]
        assert 'Failed to get included item properties' in warning
        assert 'because an internal server error occurred' in warning


@pytest.mark.system
def test_redacted_datasource():
    reason = 'Naw, man'
    mock_exception_thrower = mock.Mock(side_effect=ApiException(status=403, reason=reason))
    with unittest.mock.patch('seeq.sdk.ItemsApi.get_property', new=mock_exception_thrower):
        # Verify this blows up with a default `errors` setting
        with pytest.raises(ApiException, match=reason):
            spy.search({'Name': 'Temperature',
                        'Path': 'Example >> Cooling Tower 1 >> Area A',
                        'Datasource Name': 'Example Data'},
                       all_properties=True)

        status = spy.Status(errors='catalog')
        search_results = spy.search({'Name': 'Temperature',
                                     'Path': 'Example >> Cooling Tower 1 >> Area A',
                                     'Datasource Name': 'Example Data'},
                                    all_properties=True, status=status)

        # Assert that the dataframe includes the properties from the search and get_item_and_all_properties
        assert len(search_results.index) == 1
        assert search_results.at[0, 'Name'] == 'Temperature'
        assert search_results.at[0, 'Type'] == 'StoredSignal'
        assert search_results.at[0, 'Datasource Name'] == 'Example Data'

        # And that the status has a warning
        assert len(status.warnings) == 1
        warning = list(status.warnings)[0]
        assert 'Failed to get datasource details for "Example Data"' in warning
        assert f'due to insufficient access: "(403) {reason}' in warning


@pytest.mark.system
def test_redacted_tree_ancestors():
    if _login.is_server_version_at_least(62):
        # In R62 and later, the server does not need to make a separate call to get_tree, so this scenario can no longer
        # happen.
        return

    area_a_id = spy.search({'Name': 'Area A_Temperature'}).at[0, 'ID']
    reason = 'Hard pass'
    mock_exception_thrower = mock.Mock(side_effect=ApiException(status=403, reason=reason))
    with unittest.mock.patch('seeq.sdk.TreesApi.get_tree', new=mock_exception_thrower):
        # Verify this blows up with a default `errors` setting
        with pytest.raises(ApiException, match=reason):
            spy.search({'ID': area_a_id}, all_properties=True)

        status = spy.Status(errors='catalog')
        search_results = spy.search({'ID': area_a_id}, all_properties=True, status=status)

        # Assert that the dataframe includes the properties from the search and get_item_and_all_properties
        assert len(search_results.index) == 1
        assert search_results.at[0, 'Name'] == 'Area A_Temperature'
        assert search_results.at[0, 'Type'] == 'StoredSignal'
        assert search_results.at[0, 'Datasource ID'] == 'Example Data'

        # But the Asset/Path details are not present
        expected_missing_properties = ['Asset', 'Path']
        assert not any(item in expected_missing_properties for item in search_results.columns)

        # And that the status has a warning
        assert len(status.warnings) == 1
        warning = list(status.warnings)[0]
        assert 'Failed to get asset tree ancestors for' in warning
        assert f'due to insufficient access: "(403) {reason}' in warning


@pytest.mark.system
def test_redacted_estimate_sample_period():
    reason = 'But I am le tired'
    mock_exception_thrower = mock.Mock(side_effect=ApiException(status=500, reason=reason))
    with unittest.mock.patch('seeq.sdk.FormulasApi.run_formula', new=mock_exception_thrower):
        # Verify this blows up with a default `errors` setting
        with pytest.raises(ApiException, match=reason):
            spy.search({'Name': 'Area A_Temperature'},
                       estimate_sample_period={'Start': '2020-01-01T00:00:00Z', 'End': '2020-01-02T00:00:00Z'},
                       all_properties=True)

        status = spy.Status(errors='catalog')
        search_results = spy.search({'Name': 'Area A_Temperature'},
                                    estimate_sample_period={'Start': '2020-01-01T00:00:00Z',
                                                            'End': '2020-01-02T00:00:00Z'},
                                    all_properties=True, status=status)

        # Assert that the dataframe includes the properties from the search and get_item_and_all_properties
        assert len(search_results.index) == 1
        assert search_results.at[0, 'Name'] == 'Area A_Temperature'
        assert search_results.at[0, 'Type'] == 'StoredSignal'
        assert search_results.at[0, 'Datasource ID'] == 'Example Data'

        # But the Sample Period is not valid
        assert search_results.at[0, 'Estimated Sample Period'] is pd.NaT

        # And that the status has a warning
        warnings = sorted(list(status.warnings))
        assert len(warnings) == 2
        warning1 = warnings[0]
        assert 'Could not determine the sample period for signal "Area A_Temperature"' in warning1
        warning2 = warnings[1]
        assert 'Failed to estimate sample period' in warning2


@pytest.mark.system
def test_search_displays_and_templates():
    # Test setup: create workstep, display template, and displays using SDK
    workbook = spy.workbooks.Analysis('My Workbook %s' % _common.new_placeholder_guid())
    workbook.worksheet('My Worksheet')
    spy.workbooks.push([workbook])
    workbook_id = workbook.id
    workstep_id = workbook.worksheet('My Worksheet').current_workstep().id

    assets_api = AssetsApi(spy.client)
    displays_api = DisplaysApi(spy.client)
    display_templates_api = DisplayTemplatesApi(spy.client)
    trees_api = TreesApi(spy.client)

    asset_output_1 = assets_api.create_asset(body=AssetInputV1(name='Asset 1', scoped_to=workbook_id))
    asset_output_2 = assets_api.create_asset(body=AssetInputV1(name='Asset 2', scoped_to=workbook_id))

    display_template_output = display_templates_api.create_display_template(body=DisplayTemplateInputV1(
        name='My Display',
        source_workstep_id=workstep_id,
        swap_source_asset_id=asset_output_1.id,
        scoped_to=workbook_id
    ))
    display_output_1 = displays_api.create_display(body=DisplayInputV1(template_id=display_template_output.id))
    display_output_2 = displays_api.create_display(body=DisplayInputV1(template_id=display_template_output.id))
    trees_api.move_nodes_to_parent(parent_id=asset_output_2.id, body=ItemIdListInputV1(items=[display_output_1.id]))

    # Expected properties returned by spy.search
    expected_display_1 = {
        'Name': 'My Display',
        'ID': display_output_1.id,
        'Asset': asset_output_2.name,
        'Type': 'Display',
        'Datasource Name': SeeqNames.LocalDatasources.SeeqMetadataItemStorage.datasource_name
    }
    expected_display_1_all_props = {
        'Datasource ID': SeeqNames.LocalDatasources.SeeqMetadataItemStorage.datasource_id,
        'Datasource Class': SeeqNames.LocalDatasources.SeeqMetadataItemStorage.datasource_class,
        'Data ID': display_output_1.id,
        'Template ID': display_template_output.id,
        'Swap Out Asset ID': asset_output_1.id,
        'Swap In Asset ID': asset_output_2.id,
        'Scoped To': workbook_id
    }
    expected_display_1_all_props.update(expected_display_1)

    expected_display_2 = {
        'Name': 'My Display',
        'ID': display_output_2.id,
        'Asset': None,
        'Type': 'Display',
        'Datasource Name': SeeqNames.LocalDatasources.SeeqMetadataItemStorage.datasource_name
    }
    expected_display_2_all_props = {
        'Datasource ID': SeeqNames.LocalDatasources.SeeqMetadataItemStorage.datasource_id,
        'Datasource Class': SeeqNames.LocalDatasources.SeeqMetadataItemStorage.datasource_class,
        'Data ID': display_output_2.id,
        'Template ID': display_template_output.id,
        'Swap Out Asset ID': asset_output_1.id,
        'Swap In Asset ID': None,
        'Scoped To': workbook_id
    }
    expected_display_2_all_props.update(expected_display_2)

    expected_template = {
        'Name': display_template_output.name,
        'ID': display_template_output.id,
        'Datasource Name': SeeqNames.LocalDatasources.SeeqMetadataItemStorage.datasource_name,
        'Type': 'DisplayTemplate'
    }
    expected_template_all_props = {
        'Datasource ID': SeeqNames.LocalDatasources.SeeqMetadataItemStorage.datasource_id,
        'Datasource Class': SeeqNames.LocalDatasources.SeeqMetadataItemStorage.datasource_class,
        'Source Workstep ID': workstep_id,
        'Scoped To': workbook_id
    }
    expected_template_all_props.update(expected_template)

    def _assert_has_properties(search_result, expected_dict):
        for k, v in expected_dict.items():
            assert _common.get(search_result, k) == v

    # Test each spy.search call
    display_results = spy.search({'Type': 'Display'}, workbook=workbook_id)
    assert len(display_results) == 2
    _assert_has_properties(display_results[display_results.ID == display_output_1.id].iloc[0], expected_display_1)
    _assert_has_properties(display_results[display_results.ID == display_output_2.id].iloc[0], expected_display_2)

    display_results = spy.search({'Type': 'Display'}, workbook=workbook_id, all_properties=True)
    assert len(display_results) == 2
    _assert_has_properties(display_results[display_results.ID == display_output_1.id].iloc[0],
                           expected_display_1_all_props)
    _assert_has_properties(display_results[display_results.ID == display_output_2.id].iloc[0],
                           expected_display_2_all_props)

    template_results = spy.search({'Type': 'DisplayTemplate'}, workbook=workbook_id)
    assert len(template_results) == 1
    _assert_has_properties(template_results.iloc[0], expected_template)

    template_results = spy.search({'Type': 'DisplayTemplate'}, workbook=workbook_id, all_properties=True)
    assert len(template_results) == 1
    _assert_has_properties(template_results.iloc[0], expected_template_all_props)

    asset_search_results = spy.search({'Asset': asset_output_2.id}, workbook=workbook_id)
    assert len(asset_search_results) == 1
    _assert_has_properties(asset_search_results.iloc[0], expected_display_1)


@pytest.mark.system
def test_search_does_not_override_asset_and_path_properties():
    items_api = ItemsApi(spy.client)

    my_tree = spy.assets.Tree('My Tree', workbook='Search Override Test Workbook')
    my_tree.insert(children='Cooling Tower 1', parent='My Tree')
    search_results = spy.search({'Name': 'Area A_Temperature', 'Datasource Name': 'Example Data'})
    my_tree.insert(children=search_results, friendly_name='Temperature', parent='Cooling Tower 1')
    push_results = my_tree.push()

    tree_node_id = push_results.at[2, 'ID']

    property_value = PropertyInputV1(unit_of_measure="string", value='Test')
    property_depth_value = PropertyInputV1(unit_of_measure=None, value=99999)
    items_api.set_property(body=property_value, id=tree_node_id, property_name="Asset")
    items_api.set_property(body=property_value, id=tree_node_id, property_name="Path")
    items_api.set_property(body=property_depth_value, id=tree_node_id, property_name="Depth")

    # Ensure that the values do not come from the properties
    search_results = spy.search({'ID': tree_node_id}, all_properties=True)
    assert search_results.at[0, 'Asset'] == 'Cooling Tower 1'
    assert search_results.at[0, 'Path'] == 'My Tree'
    assert 'Depth' not in search_results.columns

    # Clean up
    items_api.archive_item(id=push_results.at[0, 'ID'])


@pytest.mark.system
def test_asset_group_search():
    workbook = 'test_asset_group_search ' + _common.new_placeholder_guid()

    workbook_id = test_common.create_test_asset_group(spy.session, workbook)

    search_df = spy.search({'Scoped To': workbook_id})

    assert sorted(search_df['Name'].to_list()) == [
        'My First Asset',
        'My Root Asset',
        'My Second Asset',
        'Temperature',
        'Temperature'
    ]

    search_df = spy.search({'Scoped To': workbook_id, 'Path': ''})

    assert sorted(search_df['Name'].to_list()) == [
        'My First Asset',
        'My Root Asset',
        'My Second Asset',
        'Temperature',
        'Temperature'
    ]

    search_df = spy.search({'Scoped To': workbook_id, 'Type': 'Asset', 'Path': ''})

    assert sorted(search_df['Name'].to_list()) == [
        'My First Asset',
        'My Root Asset',
        'My Second Asset'
    ]

    search_df = spy.search({'Scoped To': workbook_id, 'Path': ''}, recursive=False)

    assert sorted(search_df['Name'].to_list()) == [
        'My Root Asset'
    ]


@pytest.mark.system
def test_include_swap_info():
    workbook = 'test_include_swap_info'

    search_df = spy.search({'Path': 'Example >> Cooling Tower 2'}, include_swappable_assets=True)

    for _, row in search_df[search_df['Type'] != 'Asset'].iterrows():
        swappable_assets_df = row['Swappable Assets']
        assert len(swappable_assets_df) == 1
        assert row['Path'] == swappable_assets_df.iloc[0]['Path']
        assert row['Asset'] == swappable_assets_df.iloc[0]['Asset']

    push_df = spy.push(metadata=pd.DataFrame([{
        'Name': "Added Signals",
        'Formula': '$d + $e + $f',
        'Formula Parameters': {
            '$d': search_df[(search_df['Name'] == 'Compressor Power') & (search_df['Asset'] == 'Area D')],
            '$e': search_df[(search_df['Name'] == 'Compressor Power') & (search_df['Asset'] == 'Area E')],
            '$f': search_df[(search_df['Name'] == 'Compressor Power') & (search_df['Asset'] == 'Area F')]
        }
    }]), workbook=workbook, datasource=workbook, worksheet=None)

    search2_df = spy.search(push_df, include_swappable_assets=True)

    assert len(search2_df) == 1
    pushed_signal = search2_df.iloc[0]
    swappable_assets_df = pushed_signal['Swappable Assets']
    assert len(swappable_assets_df) == 3
    asset_ids = search_df[search_df['Type'] == 'Asset']['ID'].to_list()
    assert sorted(swappable_assets_df['ID'].to_list()) == sorted(asset_ids)
    assert sorted(swappable_assets_df['Asset'].to_list()) == ['Area D', 'Area E', 'Area F']

    flat_tags_df = spy.search({'Name': '/Area [DEF]_Compressor Power/'})
    push2_df = spy.push(metadata=pd.DataFrame([{
        'Name': "Added Flat Tag Signals",
        'Formula': '$d + $e + $f',
        'Formula Parameters': {
            '$d': flat_tags_df[(flat_tags_df['Name'] == 'Area D_Compressor Power')],
            '$e': flat_tags_df[(flat_tags_df['Name'] == 'Area E_Compressor Power')],
            '$f': flat_tags_df[(flat_tags_df['Name'] == 'Area F_Compressor Power')]
        }
    }]), workbook=workbook, datasource=workbook, worksheet=None)

    search3_df = spy.search(push2_df, include_swappable_assets=True)

    assert len(search2_df) == 1
    pushed_signal = search3_df.iloc[0]
    swappable_assets_df = pushed_signal['Swappable Assets']
    assert len(swappable_assets_df) == 0
    assert list(swappable_assets_df.columns) == ['ID', 'Type', 'Path', 'Asset']


@pytest.mark.system
def test_search_all_properties_swaps_unsearchable_non_admin():
    # Non-admin users can't search for swapped items in Seeq-native datasources because swaps don't get access
    # control entries. Ensure spy.search(all_properties=True) works for such items.
    assert not spy.session.user.is_admin
    # Set up: Create a calculation in a Seeq-native datasource, create a swap from it, load that swap into a worksheet
    workbook = 'test_search_all_properties_swaps_unsearchable_non_admin'
    input_signal = spy.search({
        'Path': 'Example >> Cooling Tower 1 >> Area A',
        'Name': 'Temperature',
        'Type': 'Signal'})
    original_signal = spy.push(metadata=pd.DataFrame([{
        'Name': f'{workbook}_signal',
        'Formula': '$a + 1',
        'Formula Parameters': {'$a': input_signal.iloc[0]['ID']},
        'Datasource Class': 'Seeq Calculations',
        'Datasource ID': 'Seeq Calculations',
    }]), workbook=workbook, worksheet=None)
    swap_asset = spy.search({
        'Path': 'Example >> Cooling Tower 1',
        'Name': 'Area B',
        'Type': 'Asset'
    }, old_asset_format=False)
    swapped_df = spy.swap(original_signal, swap_asset)
    pulled_workbook = spy.workbooks.pull(spy.workbooks.search({'Name': workbook}))
    pulled_workbook[0].worksheets[0].display_items = swapped_df
    spy.workbooks.push(pulled_workbook)

    # Test: SPy should be able to get all the properties of this swapped signal, even though the search endpoint
    # doesn't return swapped items for non-admins.
    search_results = spy.search(pulled_workbook[0].worksheets[0].url, all_properties=True)
    assert len(search_results) == 1
    assert search_results.iloc[0]['Name'] == f'{workbook}_signal'
    assert search_results.iloc[0]['ID'] == swapped_df.iloc[0]['ID']
    assert search_results.iloc[0]['Formula'] == '$a + 1'
    assert len(search_results.iloc[0]['Formula Parameters']) == 1
    assert swap_asset.iloc[0]['ID'].lower() in search_results.iloc[0]['Swap Key'].lower()


@pytest.mark.system
def test_assets_with_properties():
    workbook = 'test_assets_with_properties'
    spy.push(metadata=pd.DataFrame([{
        'Name': 'Asset 1',
        'Type': 'Asset',
        'Path': workbook,
        'Custom String Property': 'Blue',
        'Custom Number Property': 1,
    }, {
        'Name': 'Asset 2',
        'Type': 'Asset',
        'Path': workbook,
        'Custom String Property': 'Red',
        'Custom Number Property': 2,
    }, {
        'Name': 'Scalar 1',
        'Type': 'Scalar',
        'Path': workbook,
        'Asset': 'Asset 1',
        'Formula': '"three"',
        'Custom String Property': 'Green',
        'Custom Number Property': 3
    }, {
        'Name': 'Scalar 2',
        'Type': 'Scalar',
        'Path': workbook,
        'Asset': 'Asset 2',
        'Formula': '"four"',
        'Custom String Property': 'Orange',
        'Custom Number Property': 4
    }]), workbook=workbook, datasource=workbook, worksheet=None)

    search_df = spy.search({'Path': workbook},
                           include_properties=['Custom Number Property'],
                           workbook=workbook)

    sorted_number_properties = sorted(search_df['Custom Number Property'].to_list())
    if spy.utils.is_server_version_at_least(62):
        assert sorted_number_properties == [1, 2, 3, 4]
    else:
        assert sorted_number_properties == ['1', '2', '3', '4']

    # Scripts against v188 and earlier expect all included properties to be returned as strings
    try:
        spy.session.options.compatibility = 188
        search_df = spy.search({'Path': workbook},
                               include_properties=['Custom Number Property', 'Custom String Property'],
                               workbook=workbook)

        sorted_number_properties = sorted(search_df['Custom Number Property'].to_list())
        assert sorted_number_properties == ['1', '2', '3', '4']
    finally:
        spy.session.options.compatibility = None


@pytest.mark.system
def test_include_properties():
    with ApiClientRecorder(spy.session) as recorder:
        search_result = spy.search({'Name': '/Area [BC]_Temperature/'}, order_by='Name',
                                   include_properties=['Source Value Unit Of Measure', 'Maximum Interpolation'])

        # These recorder.calls assertions are an attempt to ensure we don't accidentally introduce a performance
        # regression in the future.
        if spy.utils.is_sdk_module_version_at_least(62):
            assert recorder.calls.get('GET /trees/assets', 0) == 0
            assert recorder.calls.get('GET /items', 0) == 1
            assert recorder.calls.get('GET /items/{id}', 0) == 0
            assert recorder.calls.get('GET /formulas/items/{id}', 0) == 0
        else:
            assert recorder.calls.get('GET /trees/assets', 0) == 0
            assert recorder.calls.get('GET /items', 0) == 1
            assert recorder.calls.get('GET /items/{id}', 0) == 2
            assert recorder.calls.get('GET /formulas/items/{id}', 0) == 0

    # Note that spy.search() guarantees that properties are returned in the order specified, after the default props
    assert search_result.columns.to_list() == ['ID', 'Name', 'Description', 'Type', 'Value Unit Of Measure',
                                               'Datasource Name', 'Archived', 'Source Value Unit Of Measure',
                                               'Maximum Interpolation']
    area_b_temp = search_result.iloc[0]
    area_c_temp = search_result.iloc[1]

    assert area_b_temp['Source Value Unit Of Measure'] == '°F'

    workbook = 'test_include_properties'
    push_results = spy.push(metadata=pd.DataFrame([{
        'Name': f'A. Signal {workbook}',
        'Type': 'Signal',
        'Formula': '($b1 + $c1).setMaxInterpolation(5.4d).convertUnits("C")',
        'Formula Parameters': {
            '$b1': area_b_temp,
            '$c1': area_c_temp
        }
    }, {
        'Name': f'B. Condition {workbook}',
        'Type': 'Condition',
        'Formula': '($b2 > $c2).removeLongerThan(2.2h)',
        'Formula Parameters': {
            '$b2': area_b_temp,
            '$c2': area_c_temp
        }
    }, {
        'Name': f'C. Scalar {workbook}',
        'Type': 'Scalar',
        'Formula': '$b3.average(capsule("2023-01-01T00:00:00Z", "2023-01-02T00:00:00Z")) + '
                   '$c3.average(capsule("2023-01-01T00:00:00Z", "2023-01-02T00:00:00Z"))',
        'Formula Parameters': {
            '$b3': area_b_temp,
            '$c3': area_c_temp
        }
    }]), workbook=workbook, datasource=workbook, worksheet=None)

    workbook_id = push_results.spy.workbook_id
    status = spy.Status()
    spy.search({'Name': f'* {workbook}', 'Scoped To': workbook_id},
               all_properties=True,
               include_properties=['Formula Parameters', 'Maximum Interpolation', 'Maximum Duration',
                                   'Nonexistent Property'],
               order_by='Name',
               status=status)

    assert status.warnings == {'If you specify all_properties=True, the include_properties argument is ignored.'}

    with ApiClientRecorder(spy.session) as recorder:
        search_result = spy.search({'Name': f'* {workbook}', 'Scoped To': workbook_id},
                                   include_properties=['Formula Parameters', 'Maximum Interpolation',
                                                       'Maximum Duration', 'Nonexistent Property'],
                                   order_by='Name')

        if spy.utils.is_sdk_module_version_at_least(62):
            assert recorder.calls.get('GET /trees/assets', 0) == 0
            assert recorder.calls.get('GET /items', 0) == 1
            assert recorder.calls.get('GET /items/{id}', 0) == 0
            assert recorder.calls.get('GET /formulas/items/{id}', 0) == 0
        else:
            assert recorder.calls.get('GET /trees/assets', 0) == 0
            assert recorder.calls.get('GET /items', 0) == 1
            assert recorder.calls.get('GET /items/{id}', 0) == 3
            assert recorder.calls.get('GET /formulas/items/{id}', 0) == 3

    assert len(search_result.spy.status.warnings) == 0
    assert search_result.columns.to_list() == ['ID', 'Name', 'Description', 'Type', 'Value Unit Of Measure',
                                               'Datasource Name', 'Archived', 'Formula Parameters',
                                               'Maximum Interpolation', 'Maximum Duration', 'Nonexistent Property']
    formula_parameters = [set(fp) for fp in search_result['Formula Parameters'].to_list()]
    assert formula_parameters == [
        {f'b1={area_b_temp["ID"]}', f'c1={area_c_temp["ID"]}'},
        {f'b2={area_b_temp["ID"]}', f'c2={area_c_temp["ID"]}'},
        {f'b3={area_b_temp["ID"]}', f'c3={area_c_temp["ID"]}'}
    ]

    assert search_result['Value Unit Of Measure'].dropna().to_list() == ['°C']

    if _login.is_server_version_at_least(62):
        assert search_result['Maximum Interpolation'].dropna().to_list() == [5.4]
        assert search_result['Maximum Duration'].dropna().to_list() == [2.2]
    else:
        assert search_result['Maximum Interpolation'].dropna().to_list() == ['5.4']
        assert search_result['Maximum Duration'].dropna().to_list() == ['2.2']

    assert len(search_result['Nonexistent Property'].dropna().to_list()) == 0


@pytest.mark.system
def test_search_property_column_types():
    # Ensure certain property columns result in expected data types (such as bool, dict, list, dataframe, etc.)
    name = 'test_search_property_column_types' + _common.new_placeholder_guid()
    # Set up: Create a reference item and a metric then assert that these columns match what's expected
    area_a_temp = spy.search({'Name': 'Area A_Temperature', 'Datasource Name': 'Example Data'}, limit=1)
    assert len(area_a_temp) == 1
    area_a_temp_id = area_a_temp.iloc[0]['ID']
    push_input = pd.DataFrame([{
        'Name': f'{name} Signal',
        'Reference': True,
        'Type': 'StoredSignal',
        'ID': area_a_temp_id,
        'My custom numeric property': 100,
        'My custom boolean property': True,
        'My custom string property': name,
    }, {
        'Name': f'{name} Metric',
        'Type': 'Metric',
        'Measured Item': area_a_temp_id,
        'Thresholds': {'HiHi': 75, 'LoLo': 60},
        'My custom numeric property': 100,
        'My custom boolean property': True,
        'My custom string property': name,
    }])
    push_result = spy.push(metadata=push_input, workbook=name, worksheet=None)

    # Assert that all the properties match the type and values we'd expect
    search_result = spy.search(push_result, workbook=name, all_properties=True)
    assert len(search_result) == 2

    signal = search_result[search_result['Name'] == f'{name} Signal'].iloc[0]
    assert signal['Type'] == 'CalculatedSignal'
    assert signal['Formula Parameters'] == [f'signal={area_a_temp_id}']
    # These bools come out as Numpy bools, not Python. `==` works, but not `is` or `isinstance`. Supress warnings.
    assert signal['Cache Enabled'] == False  # noqa: E712
    assert signal['Reference'] == True  # noqa: E712
    if spy.utils.is_sdk_module_version_at_least(62):
        assert signal['My custom numeric property'] == 100
        assert signal['My custom boolean property'] == True  # noqa: E712
        assert signal['My custom string property'] == name
    else:
        assert signal['My custom numeric property'] == '100'
        assert signal['My custom boolean property'].lower() == 'true'
        assert signal['My custom string property'] == name

    metric = search_result[search_result['Name'] == f'{name} Metric'].iloc[0]
    assert metric['Type'] == 'ThresholdMetric'
    assert isinstance(metric['Thresholds'], list)
    assert metric['Thresholds'][0]['Priority']['Name'] == 'HiHi'
    assert metric['Thresholds'][0]['Value']['Value'] == 75
    assert metric['Thresholds'][1]['Priority']['Name'] == 'LoLo'
    assert metric['Thresholds'][1]['Value']['Value'] == 60
    if spy.utils.is_sdk_module_version_at_least(62):
        assert metric['My custom numeric property'] == 100
        assert metric['My custom boolean property'] == True  # noqa: E712
        assert metric['My custom string property'] == name
    else:
        assert metric['My custom numeric property'] == '100'
        assert metric['My custom boolean property'].lower() == 'true'
        assert metric['My custom string property'] == name

    try:
        # 188 compatibility should transform user-created properties to strings, but not special Seeq/SPy ones
        spy.options.compatibility = 188
        search_result_188 = spy.search(push_result, workbook=name, all_properties=True)
        assert len(search_result) == 2

        signal = search_result_188[search_result_188['Name'] == f'{name} Signal'].iloc[0]
        assert signal['Type'] == 'CalculatedSignal'
        assert signal['Formula Parameters'] == [f'signal={area_a_temp_id}']
        assert signal['Cache Enabled'] == False  # noqa: E712
        assert signal['Reference'] == True  # noqa: E712
        assert signal['My custom numeric property'] == '100'
        assert signal['My custom boolean property'].lower() == 'true'
        assert signal['My custom string property'] == name

        metric = search_result_188[search_result_188['Name'] == f'{name} Metric'].iloc[0]
        assert metric['Type'] == 'ThresholdMetric'
        assert isinstance(metric['Thresholds'], list)
        assert metric['Thresholds'][0]['Priority']['Name'] == 'HiHi'
        assert metric['Thresholds'][0]['Value']['Value'] == 75
        assert metric['Thresholds'][1]['Priority']['Name'] == 'LoLo'
        assert metric['Thresholds'][1]['Value']['Value'] == 60
        assert metric['My custom numeric property'] == '100'
        assert metric['My custom boolean property'].lower() == 'true'
        assert metric['My custom string property'] == name

        training_time = {'Start': '2024-01-01T00:00:00Z', 'End': '2024-01-02T00:00:00Z'}
        estimate_sample_period_search = spy.search({'Name': 'Area A_Temperature', 'Datasource Name': 'Example Data'},
                                                   estimate_sample_period=training_time,
                                                   limit=1)
        expected_sample_period = pd.Timedelta(minutes=2)
        assert len(estimate_sample_period_search) == 1
        assert estimate_sample_period_search.iloc[0]['Estimated Sample Period'] == expected_sample_period
    finally:
        spy.options.compatibility = None

    # And they should be able to be round-tripped without erroring
    spy.push(metadata=search_result, workbook=name, worksheet=None)
    spy.push(metadata=search_result_188, workbook=name, worksheet=None)


@pytest.mark.system
def test_search_limit():
    workbook = 'test_search_limit'
    all_example_data = spy.search({'Name': 'Area *_*', 'Datasource Name': 'Example Data'}, limit=10, order_by=['ID'])
    assert len(all_example_data) == 10
    assert all_example_data.spy.status.warnings == {
        f'Search results limited to 10. Supply increased "limit" parameter to return more.'
    }

    all_example_data = spy.search({'Name': 'Area *_*', 'Datasource Name': 'Example Data'})
    assert len(all_example_data) > 25
    assert len(all_example_data.spy.status.warnings) == 0

    spy.push(metadata=pd.DataFrame([{
        'Name': f'{workbook} {n}',
        'Type': 'Scalar',
        'Formula': f'{n}'
    } for n in range(1002)]), workbook=workbook, datasource=workbook, worksheet=None)

    # spy.search() will return up to 1000 items if user doesn't specify a limit
    all_test_data = spy.search({'Datasource Name': workbook}, workbook=workbook, order_by=['ID'])
    assert len(all_test_data) == 1000
    assert all_test_data.spy.status.warnings == {
        f'Search results limited to 1000. Supply increased "limit" parameter to return more.'
    }

    all_test_data = spy.search({'Datasource Name': workbook}, workbook=workbook, order_by=['ID'], limit=None)
    assert len(all_test_data) == 1004  # includes the Datasource and the Analysis items
    assert len(all_test_data.spy.status.warnings) == 0

    try:
        # This will cause spy.search() to return all items, matching behavior in 188 and earlier
        spy.options.compatibility = 188
        all_test_data = spy.search({'Datasource Name': workbook}, workbook=workbook, order_by=['ID'])
        assert len(all_test_data) == 1004  # includes the Datasource and the Analysis items
        assert len(all_test_data.spy.status.warnings) == 0
    finally:
        spy.options.compatibility = None


@pytest.mark.system
def test_search_by_id():
    all_example_data = spy.search({'Datasource Name': 'Example Data'}, limit=50)
    assert len(all_example_data) == 50

    with ApiClientRecorder(spy.session) as recorder:
        # CRAB-38168 lowercase IDs should also work
        ids = all_example_data[['ID']].apply(lambda x: x.str.lower())
        search_results = spy.search(ids)
        if spy.utils.is_sdk_module_version_at_least(62):
            assert recorder.calls.get('GET /trees/assets', 0) == 0
            assert recorder.calls.get('GET /items', 0) == 1
            assert recorder.calls.get('GET /items/{id}', 0) == 0
            assert recorder.calls.get('GET /formulas/items/{id}', 0) == 0
        else:
            assert recorder.calls.get('GET /trees/assets', 0) == 0
            assert recorder.calls.get('GET /items', 0) == 0
            assert recorder.calls.get('GET /items/{id}', 0) == 50
            assert recorder.calls.get('GET /formulas/items/{id}', 0) == 0

    def _are_equal_enough(_by_search, _by_id):
        if spy.utils.is_sdk_module_version_at_least(62):
            assert _by_search.equals(_by_id)
        else:
            _df1 = _by_search.drop(columns=['Path', 'Asset'])
            _df2 = _by_id.drop(columns=['Scoped To'])
            assert _df1.equals(_df2)

    _are_equal_enough(all_example_data, search_results)

    original_page_size = _search.search_by_id_helper_page_size
    try:
        _search.search_by_id_helper_page_size = 10
        with ApiClientRecorder(spy.session) as recorder:
            search_results = spy.search(all_example_data[['ID']])
            if spy.utils.is_sdk_module_version_at_least(62):
                assert recorder.calls.get('GET /trees/assets', 0) == 0
                assert recorder.calls.get('GET /items', 0) == 5
                assert recorder.calls.get('GET /items/{id}', 0) == 0
                assert recorder.calls.get('GET /formulas/items/{id}', 0) == 0
            else:
                assert recorder.calls.get('GET /trees/assets', 0) == 0
                assert recorder.calls.get('GET /items', 0) == 0
                assert recorder.calls.get('GET /items/{id}', 0) == 50
                assert recorder.calls.get('GET /formulas/items/{id}', 0) == 0

        _are_equal_enough(all_example_data, search_results)
    finally:
        _search.search_by_id_helper_page_size = original_page_size


@pytest.mark.system
def test_retrieve_full_tree():
    test_name = 'test_retrieve_full_tree'
    spy.push(metadata=pd.DataFrame([{
        'Path': 'My Root',
        'Asset': 'My Asset',
        'Name': 'My Signal',
        'Formula': '1.toSignal()'
    }]), workbook=test_name, datasource=test_name, worksheet='My Worksheet')

    search_results = spy.search({'Path': '', 'Asset': 'My Root'}, workbook=test_name)
    assert len(search_results) == 3
    assert sorted(search_results['Name'].to_list()) == [
        'My Asset',
        'My Root',
        'My Signal',
    ]

    # This will not include the root node
    search_results = spy.search({'Path': 'My Root'}, workbook=test_name)
    assert len(search_results) == 2
    assert sorted(search_results['Name'].to_list()) == [
        'My Asset',
        'My Signal',
    ]


@pytest.mark.performance
def test_search_by_id_performance():
    workbook = 'test_search_by_id_performance'

    push_df = spy.push(metadata=pd.DataFrame([{
        'Name': f'{workbook} {n}',
        'Type': 'Scalar',
        'Formula': f'{n}'
    } for n in range(100000)]), workbook=workbook, datasource=workbook, worksheet=None)

    def _t(_df):
        _start = time.time()
        spy.search(_df, limit=1000000)
        print(f"Search time on {len(_df)} rows: {time.time() - _start} seconds")

    _t(push_df.head(100))
    _t(push_df.head(1000))
    _t(push_df.head(10000))
    _t(push_df.head(100000))
