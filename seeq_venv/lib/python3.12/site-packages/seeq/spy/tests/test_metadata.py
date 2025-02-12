import cProfile
import io
import json
import math
import os
import pstats
import tempfile
from unittest import mock

import numpy as np
import pandas as pd
import pytest

from seeq import spy
from seeq.sdk import *
from seeq.spy import _common
from seeq.spy import _metadata
from seeq.spy._errors import *
from seeq.spy.tests import test_common
from seeq.spy.workbooks import Analysis


def setup_module():
    test_common.initialize_sessions()


def assert_datasource_properties(datasource_output, name, datasource_class, datasource_id):
    assert datasource_output.datasource_class == datasource_class
    assert datasource_output.datasource_id == datasource_id
    assert datasource_output.name == name
    assert not datasource_output.is_archived
    assert datasource_output.stored_in_seeq
    assert not datasource_output.cache_enabled
    assert datasource_output.description == _common.DEFAULT_DATASOURCE_DESCRIPTION
    assert len(datasource_output.additional_properties) > 0
    expect_duplicates_property = list(filter(lambda x: x.name == 'Expect Duplicates During Indexing',
                                             datasource_output.additional_properties))
    assert len(expect_duplicates_property) == 1
    assert expect_duplicates_property[0].value


@pytest.mark.system
def test_create_datasource():
    datasources_api = DatasourcesApi(spy.session.client)

    with pytest.raises(ValueError):
        _metadata.create_datasource(spy.session, 1)

    _metadata.create_datasource(spy.session, 'test_datasource_name_1')

    datasource_output_list = datasources_api.get_datasources(limit=100000)  # type: DatasourceOutputListV1
    datasource_output = list(filter(lambda d: d.name == 'test_datasource_name_1',
                                    datasource_output_list.datasources))[0]  # type: DatasourceOutputV1

    assert_datasource_properties(datasource_output,
                                 'test_datasource_name_1',
                                 _common.DEFAULT_DATASOURCE_CLASS,
                                 'test_datasource_name_1')

    with pytest.raises(ValueError, match='"Datasource Name" required for datasource'):
        _metadata.create_datasource(spy.session, {
            'Blah': 'test_datasource_name_2'
        })

    datasource_output = _metadata.create_datasource(spy.session, {
        'Datasource Name': 'test_datasource_name_2'
    })
    assert_datasource_properties(datasource_output,
                                 'test_datasource_name_2',
                                 _common.DEFAULT_DATASOURCE_CLASS,
                                 'test_datasource_name_2')

    datasource_output = _metadata.create_datasource(spy.session, {
        'Datasource Name': 'test_datasource_name_3',
        'Datasource ID': 'test_datasource_id_3'
    })
    assert_datasource_properties(datasource_output,
                                 'test_datasource_name_3',
                                 _common.DEFAULT_DATASOURCE_CLASS,
                                 'test_datasource_id_3')

    with pytest.raises(ValueError):
        _metadata.create_datasource(spy.session, {
            'Datasource Class': 'test_datasource_class_4',
            'Datasource Name': 'test_datasource_name_4',
            'Datasource ID': 'test_datasource_id_4'
        })


@pytest.mark.system
def test_crab_25450():
    # This was a nasty bug. In the case where the user had a "Scoped To" column in their metadata DataFrame [possibly
    # as a result of creating it via spy.search(all_properties=True)], then _metadata.get_scoped_data_id() would
    # assign all items to global scope. The top of the asset tree would be locally scoped because it's treated
    # differently in _metadata._reify_path().
    #
    # _metadata.get_scoped_data_id() has been fixed so that it always sets a scope that is consistent with the Data
    # ID it is constructing. However, plenty of metadata has been pushed with the old bug in place, and we don't want
    # to cause a big headache of 'Attempted to set scope on a globally scoped item' errors coming back from Appserver
    # (read CRAB-25450 for more info).
    #
    # This test recreates the problem and then ensures the problem is handled by the code that detects the situation and
    # accommodates existing trees that have the problem.
    search_df = spy.search({'Name': 'Area E_Temperature'},
                           workbook=spy.GLOBALS_ONLY)

    # The key to reproducing the problem is including a 'Scoped To' column that is blank
    metadata_df = pd.DataFrame([
        {
            'Name': 'test_CRAB_25450 Asset',
            'Type': 'Asset',
            'Path': 'test_CRAB_25450',
            'Asset': 'test_CRAB_25450 Asset',
            'Scoped To': np.nan
        },
        {
            'Name': 'test_CRAB_25450 Signal',
            'Type': 'Signal',
            'Formula': 'sinusoid()',
            'Path': 'test_CRAB_25450',
            'Asset': 'test_CRAB_25450 Asset',
            'Scoped To': np.nan
        },
        {
            'Name': 'test_CRAB_25450 Condition',
            'Type': 'Condition',
            'Formula': 'weeks()',
            'Path': 'test_CRAB_25450',
            'Asset': 'test_CRAB_25450 Asset',
            'Scoped To': np.nan
        },
        {
            'Name': 'test_CRAB_25450 Scalar',
            'Type': 'Scalar',
            'Formula': '1',
            'Path': 'test_CRAB_25450',
            'Asset': 'test_CRAB_25450 Asset',
            'Scoped To': np.nan
        },
        {
            'Type': 'Threshold Metric',
            'Name': 'push test threshold metric',
            'Measured Item': search_df.iloc[0]['ID'],
        }
    ])
    workbook = 'test_crab_25450'
    push_df = spy.push(metadata=metadata_df, workbook=workbook, worksheet=None, datasource=workbook)

    assert len(push_df) == 6  # Not 5 because it will include the (implicitly-specified) top level asset

    items_api = ItemsApi(spy.client)
    assets_api = AssetsApi(spy.client)
    signals_api = SignalsApi(spy.client)
    conditions_api = ConditionsApi(spy.client)
    scalars_api = ScalarsApi(spy.client)
    metrics_api = MetricsApi(spy.client)

    for index, row in push_df.iterrows():
        # This recreates the bug by manually setting all the pushed items to global scope
        items_api.set_scope(id=row['ID'])

    def _get_outputs(_df):
        return (assets_api.get_asset(id=_df.iloc[0]['ID']),
                signals_api.get_signal(id=_df.iloc[1]['ID']),
                conditions_api.get_condition(id=_df.iloc[2]['ID']),
                scalars_api.get_scalar(id=_df.iloc[3]['ID']),
                metrics_api.get_metric(id=_df.iloc[4]['ID']))

    outputs = _get_outputs(push_df)

    for output in outputs:
        assert output.scoped_to is None

    # This will succeed due to our code to handle the situation.
    push2_df = spy.push(metadata=metadata_df, workbook=workbook, worksheet=None, datasource=workbook)

    for i in range(0, 5):
        assert push_df.iloc[i]['ID'] == push2_df.iloc[i]['ID']

    outputs = _get_outputs(push2_df)

    # The scope will still be wrong, but there's nothing we can do about it
    for output in outputs:
        assert output.scoped_to is None

    # Now push to a different workbook (without the recreation flag enabled)
    push3_df = spy.push(metadata=metadata_df, workbook=f'{workbook} - Corrected', worksheet=None, datasource=workbook)

    # Should be different items
    for i in range(0, 5):
        assert push_df.iloc[i]['ID'] != push3_df.iloc[i]['ID']

    outputs = _get_outputs(push3_df)

    # The scope will be correct
    for output in outputs:
        assert output.scoped_to is not None


@pytest.mark.system
def test_crab_25450_2():
    # In addition to the use case above, there is also a case where signals/conditions that are (implicitly) created
    # as part of a spy.push(data=df) can become globally scoped, and then it's not possible to push them again. Once
    # we fixed CRAB-25450, users started running into this. See SUP-41382.
    workbook = 'test_crab_25450_2'

    items_api = ItemsApi(spy.client)

    def _globally_scope(_items):
        for _item_id in _items['ID'].to_list():
            items_api.set_scope(id=_item_id)

    signal_data = pd.DataFrame(index=[
        pd.to_datetime('2019-01-01', utc=True),
        pd.to_datetime('2019-01-02', utc=True),
        pd.to_datetime('2019-01-03', utc=True),
        pd.to_datetime('2019-01-04', utc=True)
    ])
    signal_data['My Signal'] = [10, 20, 30, 40]

    signal_push_df = spy.push(signal_data, workbook=workbook, worksheet=None, datasource=workbook)

    _globally_scope(signal_push_df)
    signal_data['My Signal'] = [1, 2, 3, 4]
    signal_push_df = spy.push(signal_data, workbook=workbook, worksheet=None, datasource=workbook)

    condition_data = pd.DataFrame([{
        'Capsule Start': pd.to_datetime('2018-01-01', utc=True),
        'Capsule End': pd.to_datetime('2018-01-02', utc=True),
    }, {
        'Capsule Start': pd.to_datetime('2018-01-03', utc=True),
        'Capsule End': pd.to_datetime('2018-01-04', utc=True),
    }])
    condition_metadata = pd.DataFrame([{
        'Name': 'My Condition',
        'Type': 'Condition',
        'Maximum Duration': '2 days'
    }])

    condition_push_df = spy.push(condition_data, metadata=condition_metadata, workbook=workbook,
                                 worksheet=None, datasource=workbook)

    _globally_scope(condition_push_df)
    condition_data = pd.DataFrame([{
        'Capsule Start': pd.to_datetime('2019-01-01', utc=True),
        'Capsule End': pd.to_datetime('2019-01-02', utc=True),
    }, {
        'Capsule Start': pd.to_datetime('2019-01-03', utc=True),
        'Capsule End': pd.to_datetime('2019-01-04', utc=True),
    }])
    condition_push_df = spy.push(condition_data, metadata=condition_metadata, workbook=workbook,
                                 worksheet=None, datasource=workbook)

    pull_signal_df1 = spy.pull(signal_push_df, start='2019-01-01T00:00:00Z', end='2019-01-05T00:00:00Z', grid=None)

    test_common.make_index_naive(pull_signal_df1)

    assert len(pull_signal_df1) == 4
    assert pull_signal_df1.at[pd.to_datetime('2019-01-01'), 'My Signal'] == 1
    assert pull_signal_df1.at[pd.to_datetime('2019-01-02'), 'My Signal'] == 2
    assert pull_signal_df1.at[pd.to_datetime('2019-01-03'), 'My Signal'] == 3
    assert pull_signal_df1.at[pd.to_datetime('2019-01-04'), 'My Signal'] == 4

    pull_condition_df1 = spy.pull(condition_push_df, start='2019-01-01T00:00:00Z', end='2019-01-05T00:00:00Z',
                                  grid=None)

    assert len(pull_condition_df1) == 2
    assert pull_condition_df1.at[0, 'Capsule Start'] == pd.to_datetime('2019-01-01', utc=True)
    assert pull_condition_df1.at[0, 'Capsule End'] == pd.to_datetime('2019-01-02', utc=True)
    assert pull_condition_df1.at[1, 'Capsule Start'] == pd.to_datetime('2019-01-03', utc=True)
    assert pull_condition_df1.at[1, 'Capsule End'] == pd.to_datetime('2019-01-04', utc=True)


@pytest.mark.system
def test_bad_formula_error_message():
    search_df = spy.search({'Name': 'Area B_Temperature'},
                           workbook=spy.GLOBALS_ONLY)
    temperature_id = search_df.iloc[0]['ID']

    search_df = spy.search({'Name': 'Area B_Compressor Power'},
                           workbook=spy.GLOBALS_ONLY)
    power_id = search_df.iloc[0]['ID']

    conditions_api = ConditionsApi(spy.session.client)

    condition_input = ConditionInputV1(
        name='test_bad_formula',
        formula='$power > 20 kW and $temp < 60 Faq',
        parameters=[
            f'power={power_id}',
            f'temp={temperature_id}'
        ],
        datasource_id=_common.DEFAULT_DATASOURCE_ID,
        datasource_class=_common.DEFAULT_DATASOURCE_CLASS
    )
    condition_update_input = ConditionUpdateInputV1(
        name=condition_input.name,
        formula=condition_input.formula,
        parameters=condition_input.parameters,
        datasource_id=condition_input.datasource_id,
        datasource_class=condition_input.datasource_class,
        replace_capsule_properties=True
    )

    expected_error = 'Unknown unit of measure \'Faq\''
    error_message = None
    try:
        conditions_api.create_condition(body=condition_input)
    except ApiException as e:
        error_message = json.loads(e.body)['statusMessage']

    assert expected_error in error_message

    item_batch_output = conditions_api.put_conditions(body=ConditionBatchInputV1(
        conditions=[condition_update_input]
    ))

    error_message = item_batch_output.item_updates[0].error_message

    assert expected_error in error_message


@pytest.mark.system
def test_metadata_dataframe_weird_index():
    workbook = 'test_metadata_dataframe_weird_index'

    metadata_df = pd.DataFrame({
        'Type': ['Signal', 'Signal'],
        'Name': [f'{workbook}1', f'{workbook}2'],
        'Path': workbook,
        'Asset': ['Asset 1', 'Asset 2'],
        'Formula': ['sinusoid()', 'sawtooth()']
    },
        # An index of 3, 4 here will replicate the scenario -- if _metadata.py doesn't reset the index for the
        # push_result_df, it will be messed up because the wrong rows will be overwritten for the Push Result column.
        index=pd.Index([3, 4], name='Hey!')
    )

    push_result_df = spy.push(metadata=metadata_df, workbook=workbook, worksheet=None, datasource=workbook)

    # Three Asset entries will be added to the end of the resulting DataFrame with conspicuous index entries
    assert push_result_df.index.equals(pd.Index([
        3.0, 4.0, '__side_effect_asset_1__', '__side_effect_asset_2__', '__side_effect_asset_3__']))
    assert push_result_df.index.name == 'Hey!'


@pytest.mark.system
def test_incremental_metadata_push():
    workbook = 'test_incremental_metadata_push'
    metadata = pd.DataFrame([{
        'Type': 'Metric',
        'Name': 'My Metric',
        'Asset': 'Asset 1',
        'Path': workbook,
        'Measured Item': {'Name': 'My Signal', 'Asset': 'Asset 1', 'Path': workbook}
    }, {
        'Type': 'Condition',
        'Name': 'My Condition',
        'Asset': 'Asset 1',
        'Path': workbook,
        'Formula': '$s > $c',
        'Formula Parameters': {
            '$s': {'Name': 'My Signal', 'Asset': 'Asset 1', 'Path': workbook},
            '$c': {'Name': 'My Scalar', 'Asset': 'Asset 1', 'Path': workbook}
        }
    }, {
        'Type': 'Signal',
        'Name': 'My Signal',
        'Asset': 'Asset 1',
        'Path': workbook,
        'Formula': 'sinusoid(10min)'
    }, {
        'Type': 'Scalar',
        'Name': 'My Scalar',
        'Asset': 'Asset 1',
        'Path': workbook,
        'Formula': '5'
    }, {
        'Type': 'Asset',
        'Name': 'Asset 2',
        'Asset': 'Asset 2',
        'Path': workbook
    }, {
        'Type': 'Scalar',
        'Name': 'Scalar to Drop',
        'Asset': 'Asset 1',
        'Path': workbook,
        'Formula': '15'
    }])

    expected_length = len(metadata) + 2

    with tempfile.TemporaryDirectory() as temp_dir:
        pickle_file_name = os.path.join(temp_dir, f'{workbook}.pickle.zip')

        push_results_df = spy.push(metadata=metadata, workbook=workbook, datasource=workbook, worksheet=None,
                                   metadata_state_file=pickle_file_name)
        assert len(push_results_df) == expected_length
        assert push_results_df['Push Result'].drop_duplicates().to_list() == ['Success']

        # No changes
        push_results_df = spy.push(metadata=metadata, workbook=workbook, datasource=workbook, worksheet=None,
                                   metadata_state_file=pickle_file_name)
        assert len(push_results_df) == expected_length
        assert push_results_df['Push Result'].drop_duplicates().to_list() == ['Success: Unchanged']

        # Change a formula
        metadata.at[1, 'Formula'] = '$s < $c + 10'
        push_results_df = spy.push(metadata=metadata, workbook=workbook, datasource=workbook, worksheet=None,
                                   metadata_state_file=pickle_file_name)
        assert len(push_results_df) == expected_length
        assert len(push_results_df[push_results_df['Push Result'] == 'Success']) == 1
        assert len(push_results_df[push_results_df['Push Result'] == 'Success: Unchanged']) == expected_length - 1

        # Add an item
        my_other_scalar_row_index = len(metadata)
        metadata.loc[my_other_scalar_row_index] = {
            'Type': 'Scalar',
            'Name': 'My Other Scalar',
            'Asset': 'Asset 1',
            'Path': workbook,
            'Formula': '10'
        }
        push_results_df = spy.push(metadata=metadata, workbook=workbook, datasource=workbook, worksheet=None,
                                   metadata_state_file=pickle_file_name)
        expected_length += 1
        assert len(push_results_df) == expected_length
        my_other_scalar_row = push_results_df.loc[my_other_scalar_row_index]
        assert my_other_scalar_row['Push Result'] == 'Success'
        push_results_df.drop(my_other_scalar_row_index, inplace=True)
        assert push_results_df['Push Result'].drop_duplicates().to_list() == ['Success: Unchanged']

        # Cause an error and make sure the correction can be pushed
        metadata.at[1, 'Formula'] = 'this will not work'
        push_results_df = spy.push(metadata=metadata, workbook=workbook, datasource=workbook, worksheet=None,
                                   metadata_state_file=pickle_file_name, errors='catalog')
        assert len(push_results_df) == expected_length
        my_condition = push_results_df.loc[1]
        assert my_condition['Push Result'].startswith('Failed to write')
        push_results_df.drop(1, inplace=True)
        assert push_results_df['Push Result'].drop_duplicates().to_list() == ['Success: Unchanged']

        # Correct the error (we'll verify it at the end)
        metadata.at[1, 'Formula'] = '$s < $c + 100'

        # Change a formula parameter on a condition
        metadata.at[1, 'Formula Parameters'] = {
            '$s': {'Name': 'My Signal', 'Asset': 'Asset 1', 'Path': workbook},
            '$c': {'Name': 'My Other Scalar', 'Asset': 'Asset 1', 'Path': workbook}
        }
        push_results_df = spy.push(metadata=metadata, workbook=workbook, datasource=workbook, worksheet=None,
                                   metadata_state_file=pickle_file_name)
        assert len(push_results_df) == expected_length
        my_condition = push_results_df.loc[1]
        assert my_condition['Push Result'] == 'Success'
        push_results_df.drop(1, inplace=True)
        assert push_results_df['Push Result'].drop_duplicates().to_list() == ['Success: Unchanged']

        # Change a parameter on a metric
        metadata.at[0, 'Measured Item'] = {'Name': 'My Condition', 'Asset': 'Asset 1', 'Path': workbook}
        push_results_df = spy.push(metadata=metadata, workbook=workbook, datasource=workbook, worksheet=None,
                                   metadata_state_file=pickle_file_name)
        assert len(push_results_df) == expected_length
        my_metric = push_results_df.loc[0]
        assert my_metric['Push Result'] == 'Success'
        push_results_df.drop(0, inplace=True)
        assert push_results_df['Push Result'].drop_duplicates().to_list() == ['Success: Unchanged']

        # Change a path
        metadata.at[3, 'Asset'] = 'Asset 2'
        push_results_df = spy.push(metadata=metadata, workbook=workbook, datasource=workbook, worksheet=None,
                                   metadata_state_file=pickle_file_name)
        assert len(push_results_df) == expected_length
        my_signal = push_results_df.loc[3]
        assert my_signal['Push Result'] == 'Success'
        push_results_df.drop(3, inplace=True)
        assert push_results_df['Push Result'].drop_duplicates().to_list() == ['Success: Unchanged']

        # Drop an item
        metadata.drop(5, inplace=True)
        push_results_df = spy.push(metadata=metadata, workbook=workbook, datasource=workbook, worksheet=None,
                                   metadata_state_file=pickle_file_name, archive=True)
        expected_length -= 1
        assert len(push_results_df) == expected_length
        assert push_results_df['Push Result'].drop_duplicates().to_list() == ['Success: Unchanged']

    search_df = spy.search({'Path': workbook}, workbook=workbook, recursive=True, all_properties=True,
                           old_asset_format=True)
    assert len(search_df) == 7
    search_df = search_df[['ID', 'Type', 'Path', 'Asset', 'Name', 'Formula', 'Formula Parameters', 'Measured Item']]
    my_metric = search_df[search_df['Name'] == 'My Metric'].iloc[0]
    my_condition = search_df[search_df['Name'] == 'My Condition'].iloc[0]
    my_signal = search_df[search_df['Name'] == 'My Signal'].iloc[0]
    assert len(search_df[search_df['Name'] == 'My Scalar']) == 1
    my_scalar = search_df[search_df['Name'] == 'My Scalar'].iloc[0]
    asset_2 = search_df[search_df['Name'] == 'Asset 2'].iloc[0]
    my_other_scalar = search_df[search_df['Name'] == 'My Other Scalar'].iloc[0]
    assert len(search_df[search_df['Name'] == 'Scalar to Drop']) == 0

    assert my_condition['Formula'] == '$s < $c + 100'
    assert sorted(my_condition['Formula Parameters']) == sorted([f's={my_signal["ID"]}', f'c={my_other_scalar["ID"]}'])
    assert my_metric['Measured Item'] == my_condition['ID']
    assert my_scalar['Asset'] == 'Asset 2'
    assert asset_2['Asset'] == workbook
    assert my_other_scalar['Formula'] == '10'


@pytest.mark.system
def test_push_directives_create_and_update_only():
    workbook = 'test_push_directives_create_and_update_only'
    _test_push_directives_create_and_update_only(None, workbook)


@pytest.mark.system
def test_push_directives_create_and_update_only_incremental():
    workbook = 'test_push_directives_create_and_update_only_incremental'
    with tempfile.TemporaryDirectory() as temp_dir:
        pickle_file_name = os.path.join(temp_dir, f'test_push_directives_create_and_update_only.pickle.zip')
        _test_push_directives_create_and_update_only(pickle_file_name, workbook)


def _test_push_directives_create_and_update_only(metadata_state_file, workbook):
    metadata = pd.DataFrame([{
        'Type': 'Signal',
        'Name': 'My Signal ' + _common.new_placeholder_guid(),
        'Formula': 'sinusoid(400min)',
        'Push Directives': 1
    }])

    with pytest.raises(SPyTypeError):
        spy.push(metadata=metadata, workbook=workbook, datasource=workbook, worksheet=None,
                 metadata_state_file=metadata_state_file)

    metadata['Push Directives'] = 'Bogus'
    with pytest.raises(SPyValueError, match='not recognized'):
        spy.push(metadata=metadata, workbook=workbook, datasource=workbook, worksheet=None,
                 metadata_state_file=metadata_state_file)

    metadata['Push Directives'] = 'CreateOnly;UpdateOnly'
    with pytest.raises(SPyValueError, match='mutually exclusive'):
        spy.push(metadata=metadata, workbook=workbook, datasource=workbook, worksheet=None,
                 metadata_state_file=metadata_state_file)

    metadata['Push Directives'] = 'UpdateOnly'
    push_df = spy.push(metadata=metadata, workbook=workbook, datasource=workbook, worksheet=None,
                       metadata_state_file=metadata_state_file)
    assert push_df.iloc[0]['Push Result'] == 'Success: Skipped due to UpdateOnly push directive -- item does not exist'

    metadata['Push Directives'] = 'CreateOnly'
    push_df = spy.push(metadata=metadata, workbook=workbook, datasource=workbook, worksheet=None,
                       metadata_state_file=metadata_state_file)
    assert push_df.iloc[0]['Push Result'] == 'Success'

    push_df = spy.push(metadata=metadata, workbook=workbook, datasource=workbook, worksheet=None,
                       metadata_state_file=metadata_state_file)
    assert push_df.iloc[0]['Push Result'] == 'Success: Skipped due to CreateOnly push directive -- item already exists'

    metadata['Push Directives'] = 'UpdateOnly'
    metadata['Formula'] = 'sawtooth(100s)'
    push_df = spy.push(metadata=metadata, workbook=workbook, datasource=workbook, worksheet=None,
                       metadata_state_file=metadata_state_file)
    assert push_df.iloc[0]['Push Result'] == 'Success'

    # Make sure an item doesn't get archived just because it is skipped
    metadata['Push Directives'] = 'CreateOnly'
    push_df = spy.push(metadata=metadata, workbook=workbook, datasource=workbook, worksheet=None, archive=True,
                       metadata_state_file=metadata_state_file)

    search_df = spy.search({'ID': push_df.iloc[0]['ID']}, all_properties=True)
    assert len(search_df) == 1
    assert search_df.iloc[0]['Formula'] == 'sawtooth(100s)'
    assert 'Push Directives' not in search_df.columns


@pytest.mark.system
def test_push_bad_uiconfig():
    test_name = 'test_push_bad_uiconfig'
    with pytest.raises(SPyValueError, match='^UIConfig is not a valid JSON string:.*\n{bad$'):
        spy.push(metadata=pd.DataFrame([{
            'Type': 'Condition',
            'Name': 'My Condition',
            'Formula': 'days()',
            'UIConfig': '{bad'
        }]), workbook=test_name, worksheet=None, datasource=test_name)

    try:
        spy.options.compatibility = 189
        pushed_df = spy.push(metadata=pd.DataFrame([{
            'Type': 'Condition',
            'Name': 'My Condition',
            'Formula': 'days()',
            'UIConfig': '{bad'
        }]), workbook=test_name, worksheet=None, datasource=test_name, errors='catalog')
        assert 'Success' in pushed_df.iloc[0]['Push Result']
    finally:
        spy.options.compatibility = None

    pushed_df = spy.push(metadata=pd.DataFrame([{
        'Type': 'Condition',
        'Name': 'My Condition',
        'Formula': 'days()',
        'UIConfig': '{bad'
    }]), workbook=test_name, worksheet=None, datasource=test_name, errors='catalog')

    assert 'UIConfig is not a valid JSON string' in pushed_df.iloc[0]['Push Result']

    # This one will succeed
    pushed_df = spy.push(metadata=pd.DataFrame([{
        'Type': 'Condition',
        'Name': 'My Condition',
        'Formula': 'days()',
        'UIConfig': '{"bad": "good"}'
    }]), workbook=test_name, worksheet=None, datasource=test_name)

    items_api = ItemsApi(spy.client)
    item_output = items_api.get_item_and_all_properties(id=pushed_df.iloc[0]['ID'])
    properties = {prop.name: prop.value for prop in item_output.properties}
    assert properties['UIConfig'] == '{"bad": "good"}'


@pytest.mark.system
def test_push_archived_item():
    workbook = 'test_push_archived_item'

    area_a_df = spy.search({
        'Name': 'Area A_Temperature'
    })

    push_df = spy.push(metadata=pd.DataFrame([{
        'Type': 'Signal',
        'Name': f'{workbook} Signal',
        'Formula': 'sinusoid()',
        'Archived': False
    }, {
        'Type': 'Condition',
        'Name': f'{workbook} Condition',
        'Formula': 'days()',
        'Archived': False
    }, {
        'Type': 'Scalar',
        'Name': f'{workbook} Scalar',
        'Formula': '1',
        'Archived': False
    }, {
        'Type': 'Asset',
        'Name': f'{workbook} Asset',
        'Archived': False
    }, {
        'Type': 'Threshold Metric',
        'Name': f'{workbook} Threshold Metric',
        'Measured Item': area_a_df.iloc[0],
        'Archived': False,
    }]), workbook=workbook, worksheet=None)

    search_df = spy.search({
        'Name': f'{workbook} *'
    }, workbook=workbook, all_properties=True)
    assert len(search_df) == 5

    # Note that we handle both boolean and string in the Archived field
    search_df['Archived'] = 'true'
    spy.push(metadata=search_df, workbook=workbook, worksheet=None)

    empty_df = spy.search({
        'Name': f'{workbook} *'
    }, workbook=workbook)
    assert len(empty_df) == 0

    search_df['Archived'] = False
    spy.push(metadata=search_df, workbook=workbook, worksheet=None)

    not_empty_df = spy.search({
        'Name': f'{workbook} *'
    }, workbook=workbook)
    assert len(not_empty_df) == 5


@pytest.mark.system
def test_push_literal_scalar():
    metadata = pd.DataFrame([{
        'Type': 'Scalar',
        'Name': 'Stored Negative Number',
        'Formula': np.int64(-12)
    }])

    push_df = spy.push(metadata=metadata, workbook='test_push_scalar', worksheet=None)

    search_df = spy.search(push_df, all_properties=True)
    if spy.utils.is_server_version_at_least(64):
        assert search_df.iloc[0]['Type'] == 'CalculatedScalar'
    assert search_df.iloc[0]['Formula'] == '-12'

    pull_df = spy.pull(push_df)
    assert pull_df.iloc[0]['Stored Negative Number'] == -12

    try:
        spy.options.force_calculated_scalars = False
        push_df = spy.push(metadata=metadata, workbook='test_push_scalar_2', worksheet=None)
        search_df = spy.search(push_df, all_properties=True)
        if spy.utils.is_server_version_at_least(64):
            assert search_df.iloc[0]['Type'] == 'LiteralScalar'
        assert search_df.iloc[0]['Formula'] == '-12'

    finally:
        spy.options.force_calculated_scalars = True


@pytest.mark.system
def test_push_calculated_scalar():
    metadata = pd.DataFrame([{
        'Type': 'Scalar',
        'Name': 'Calculated Negative Number',
        'Formula': '-12m + 4m'
    }])

    push_df = spy.push(metadata=metadata, workbook='test_push_scalar', worksheet=None)

    search_df = spy.search(push_df, all_properties=True)
    assert search_df.iloc[0]['Formula'] == '-12m + 4m'

    pull_df = spy.pull(push_df)
    assert pull_df.iloc[0]['Calculated Negative Number'] == -8


@pytest.mark.system
def test_push_threshold_metric_metadata():
    signals_for_testing = spy.search({
        'Path': 'Example >> Cooling Tower 1 >> Area A'
    })

    # test an expected successful push
    test_dict = {'Type': 'Threshold Metric',
                 'Name': 'push test threshold metric',
                 # Test passing in a pd.Series
                 'Measured Item': signals_for_testing[signals_for_testing['Name'] == 'Temperature'].iloc[0],
                 # Test passing in a one-row pd.DataFrame
                 'Thresholds': {'LoLo': signals_for_testing[signals_for_testing['Name'] == 'Wet Bulb'], '3': 95}
                 }
    test_metadata = pd.DataFrame([test_dict])

    push_output = spy.push(metadata=test_metadata, workbook='test_push_threshold_metric_metadata', worksheet=None)

    metrics_api = MetricsApi(spy.session.client)
    pushed_id = push_output['ID'].iloc[0]
    confirm_push_output = metrics_api.get_metric(id=pushed_id)
    assert confirm_push_output.measured_item.id == test_metadata['Measured Item'].iloc[0]['ID']
    tp = [t.priority.level for t in confirm_push_output.thresholds]
    assert (confirm_push_output.thresholds[tp.index(-2)].item.id ==
            test_metadata['Thresholds'].iloc[0]['LoLo'].iloc[0]['ID'])
    assert confirm_push_output.thresholds[tp.index(3)].value.value == 95

    # test push using ID
    test_metadata['ID'] = push_output['ID'].iloc[0]
    del test_metadata['Thresholds'].iloc[0]['LoLo']
    test_metadata['Thresholds'].iloc[0]['Lo'] = \
        signals_for_testing[signals_for_testing['Name'] == 'Wet Bulb']['ID'].iloc[0]
    test_metadata['Thresholds'].iloc[0]['3'] = 90
    push_output = spy.push(metadata=test_metadata, workbook='test_push_threshold_metric_metadata', worksheet=None)

    assert push_output['ID'].iloc[0] == pushed_id
    confirm_push_output = metrics_api.get_metric(id=push_output['ID'].iloc[0])
    tp = [t.priority.level for t in confirm_push_output.thresholds]
    assert confirm_push_output.thresholds[tp.index(-1)].item.id == test_metadata['Thresholds'].iloc[0]['Lo']
    assert confirm_push_output.thresholds[tp.index(3)].value.value == 90

    # Test using metric string levels not defined on the system
    test_metadata['Thresholds'].iloc[0]['9'] = 100
    with pytest.raises(Exception, match="The threshold 9 for metric push test threshold metric is "
                                        "not a valid threshold level."):
        spy.push(metadata=test_metadata, workbook='test_push_threshold_metric_metadata', worksheet=None)

    # Test using metric string levels that map to multiple values at the same level
    test_metadata.at[0, 'Thresholds'] = {
        'Lo': signals_for_testing[signals_for_testing['Name'] == 'Wet Bulb']['ID'].iloc[0],
        '-1': 90}

    with pytest.raises(RuntimeError):
        spy.push(metadata=test_metadata, workbook='test_push_threshold_metric_metadata', worksheet=None)

    # Test specifying threshold colors
    test_metadata.at[0, 'Thresholds'] = {
        'Lo#Ff0000': signals_for_testing[signals_for_testing['Name'] == 'Wet Bulb']['ID'].iloc[0],
        '3#00fF00': 90
    }

    push_output = spy.push(metadata=test_metadata, workbook='test_push_threshold_metric_metadata', worksheet=None)
    confirm_push_output = metrics_api.get_metric(id=push_output['ID'].iloc[0])
    tp = [t.priority.level for t in confirm_push_output.thresholds]
    assert confirm_push_output.thresholds[tp.index(-1)].priority.color == '#ff0000'
    assert confirm_push_output.thresholds[tp.index(3)].priority.color == '#00ff00'

    # Test bad color code
    test_metadata.at[0, 'Thresholds'] = {
        'Lo#gg0000': signals_for_testing[signals_for_testing['Name'] == 'Wet Bulb']['ID'].iloc[0],
        '3#00ff00': 90
    }

    with pytest.raises(RuntimeError):
        spy.push(metadata=test_metadata, workbook='test_push_threshold_metric_metadata', worksheet=None)

    # Test converting a measured item defined by a dataframe
    temperature_index = signals_for_testing[signals_for_testing['Name'] == 'Temperature'].index.to_list()[0]
    test_dict = [{'Type': 'Threshold Metric',
                  'Name': 'push test threshold metric',
                  'Measured Item': signals_for_testing.iloc[temperature_index].to_dict(),
                  'Thresholds': {'Lo': signals_for_testing[signals_for_testing['Name'] == 'Wet Bulb']['ID'].iloc[0],
                                 '3': 90}}]
    test_metadata = pd.DataFrame(test_dict)

    push_output = spy.push(metadata=test_metadata, workbook='test_push_threshold_metric_metadata', worksheet=None)
    confirm_push_output = metrics_api.get_metric(id=push_output.at[0, 'ID'])
    assert confirm_push_output.measured_item.name == 'Temperature'

    # Test a threshold defined by a dataframe
    wetbulb_index = signals_for_testing[signals_for_testing['Name'] == 'Wet Bulb'].index.to_list()[0]
    test_dict = [{'Type': 'Threshold Metric',
                  'Name': 'push test threshold metric',
                  'Measured Item': signals_for_testing[signals_for_testing['Name'] == 'Temperature']['ID'].iloc[0],
                  'Thresholds': {'Lo': signals_for_testing.iloc[wetbulb_index].to_dict(),
                                 '3': 90}}]
    test_metadata = pd.DataFrame(test_dict)

    push_output = spy.push(metadata=test_metadata, workbook='test_push_threshold_metric_metadata', worksheet=None)
    confirm_push_output = metrics_api.get_metric(id=push_output.at[0, 'ID'])
    threshold_items = [t.item.name for t in confirm_push_output.thresholds]
    assert 'Wet Bulb' in threshold_items

    # Test pushing a threshold metric with a percentile
    test_dict = [{'Type': 'Threshold Metric',
                  'Name': 'push test threshold metric',
                  'Measured Item': signals_for_testing.iloc[temperature_index].to_dict(),
                  'Statistic': 'Percentile(50)'}]
    test_metadata = pd.DataFrame(test_dict)

    push_output = spy.push(metadata=test_metadata, workbook='test_push_threshold_metric_metadata', worksheet=None)
    confirm_push_output = metrics_api.get_metric(id=push_output.at[0, 'ID'])
    assert confirm_push_output.aggregation_function == 'percentile(50)'

    # Test pushing a threshold metric with a rate
    test_dict = [{'Type': 'Threshold Metric',
                  'Name': 'push test threshold metric',
                  'Measured Item': signals_for_testing.iloc[temperature_index].to_dict(),
                  'Statistic': 'Rate("min")'}]
    test_metadata = pd.DataFrame(test_dict)

    push_output = spy.push(metadata=test_metadata, workbook='test_push_threshold_metric_metadata', worksheet=None)
    confirm_push_output = metrics_api.get_metric(id=push_output.at[0, 'ID'])
    assert confirm_push_output.aggregation_function == 'rate("min")'

    # Test pushing a threshold metric with a total duration
    test_condition = pd.DataFrame([
        {'Type': 'Condition',
         'Name': 'Test condition for threshold metrics',
         'Formula': '$a>80',
         'Formula Parameters': {'a': signals_for_testing.iloc[temperature_index].to_dict()}}
    ])
    test_condition_push_result = spy.push(metadata=test_condition, workbook='test_push_threshold_metric_metadata',
                                          worksheet=None)
    test_dict = [{'Type': 'Threshold Metric',
                  'Name': 'push test threshold metric',
                  'Measured Item': test_condition_push_result.iloc[0].to_dict(),
                  'Measured Item Maximum Duration': '40h',
                  'Statistic': 'Total Duration("min")'}]
    test_metadata = pd.DataFrame(test_dict)

    push_output = spy.push(metadata=test_metadata, workbook='test_push_threshold_metric_metadata', worksheet=None)
    confirm_push_output = metrics_api.get_metric(id=push_output.at[0, 'ID'])
    assert confirm_push_output.aggregation_function == 'totalDuration("min")'


@pytest.mark.system
def test_metric_threshold_item_overrides_value():
    # This test ensures that the Item ID part of a Threshold definition takes priority over the Value part
    test_name = 'test_metric_threshold_item_overrides_value'

    metric_json = r"""
    [{
        "Archived": false,
        "Cache Enabled": true,
        "Cache ID": "0EF54429-6E4D-EA90-8384-1320A252BB39",
        "Enabled": true,
        "Formula": "\"    \".toSignal()",
        "Formula Parameters": {},
        "Formula Version": 27,
        "Interpolation Method": "Step",
        "Maximum Interpolation": {
            "Unit Of Measure": "d",
            "Value": 1
        },
        "Name": "Purple signal",
        "Type": "CalculatedSignal",
        "UIConfig": {
            "advancedParametersCollapsed": true,
            "configVersion": 11,
            "helpShown": true,
            "helpView": "documentation",
            "type": "formula"
        },
        "Unsearchable": false,
        "Value Unit Of Measure": "string"
    }, {
        "Archived": false,
        "Cache Enabled": true,
        "Cache ID": "0EF5442A-784D-75D0-BEBC-8D530B6C30B2",
        "Enabled": true,
        "Formula": "//String scalar comprised of four spaces\n\"    \"",
        "Formula Parameters": {},
        "Formula Version": 27,
        "Name": "Purple Scaler",
        "Type": "CalculatedScalar",
        "UIConfig": {
            "advancedParametersCollapsed": true,
            "configVersion": 11,
            "helpShown": true,
            "helpView": "documentation",
            "type": "formula"
        },
        "Unit Of Measure": "string",
        "Unsearchable": false
    }, {
        "Archived": false,
        "Cache Enabled": false,
        "Enabled": true,
        "Formula": "<ThresholdMetric>",
        "Formula Parameters": {
            "Measured Item": {"Name": "Purple signal"},
            "Process Type": "Simple",
            "Thresholds": [
                {
                    "Item ID": {"Name": "Purple Scaler"},
                    "Priority": {
                        "Color": "#7030a0",
                        "Level": 3,
                        "Name": "HiHiHi"
                    },
                    "Value": {
                        "Unit Of Measure": "string",
                        "Value": "    "
                    }
                }
            ]
        },
        "MetricReconfigurationMigration": true,
        "Name": "Actual (In Progress)",
        "Type": "ThresholdMetric",
        "UIConfig": {
            "advancedParametersCollapsed": true,
            "type": "threshold-metric"
        },
        "Unsearchable": false
    }]
    """

    metric_metadata = pd.DataFrame(json.loads(metric_json))
    pushed_df = spy.push(metadata=metric_metadata, workbook=test_name, worksheet=None, datasource=test_name)
    metric_id = pushed_df[pushed_df['Name'] == 'Actual (In Progress)'].iloc[0]['ID']
    threshold_item_id = pushed_df[pushed_df['Name'] == 'Purple Scaler'].iloc[0]['ID']

    metrics_api = MetricsApi(spy.client)
    metric = metrics_api.get_metric(id=metric_id)
    assert len(metric.thresholds) == 1
    assert metric.thresholds[0].item.id == threshold_item_id
    assert metric.thresholds[0].priority.color == '#7030a0'
    assert metric.thresholds[0].priority.level == 3
    assert metric.thresholds[0].priority.name == 'HiHiHi'


@pytest.mark.system
def test_asset_group_push():
    if not spy.utils.is_server_version_at_least(64):
        return

    workbook = 'test_asset_group_push ' + _common.new_placeholder_guid()

    workbook_id = test_common.create_test_asset_group(spy.session, workbook)

    full_group_df = spy.search({'Scoped To': workbook_id}, old_asset_format=False, all_properties=True)

    assert sorted(full_group_df['Name'].to_list()) == [
        'My First Asset',
        'My Root Asset',
        'My Second Asset',
        'Temperature',
        'Temperature'
    ]

    # Now transfer to a new workbook
    new_workbook = f'{workbook} - New'
    new_asset_group_df = full_group_df.drop(columns=['ID', 'Data ID', 'Datasource Name', 'Datasource ID',
                                                     'Datasource Class'])

    push_df = spy.push(metadata=new_asset_group_df, workbook=new_workbook, datasource=workbook, worksheet=None)

    new_search_df = spy.search({'Scoped To': push_df.spy.workbook_id, 'Path': ''})

    assert sorted(new_search_df['Name'].to_list()) == [
        'My First Asset',
        'My Root Asset',
        'My Second Asset',
        'Temperature',
        'Temperature'
    ]

    assert sorted(full_group_df['ID'].to_list()) != sorted(new_search_df['ID'].to_list())


@pytest.mark.performance
def test_metadata_incremental_push_performance():
    workbook = 'test_metadata_incremental_push_performance'
    count = 100000
    metadata = pd.DataFrame({
        'Name': [f'Signal ' + str(i).zfill(math.floor(math.log(count, 10))) for i in range(count)],
        'Type': 'Signal',
        'Formula': [f'sinusoid({i + 1}s)' for i in range(count)]
    })

    with tempfile.TemporaryDirectory() as temp_dir:
        pickle_file_name = os.path.join(temp_dir, f'{workbook}.pickle.zip')
        timer = _common.timer_start()
        spy.push(metadata=metadata, workbook=workbook, datasource=workbook, worksheet=None,
                 metadata_state_file=pickle_file_name)
        print(f'Initial push of {count} items took {int(_common.timer_elapsed(timer).total_seconds() * 1000)} ms')

        timer = _common.timer_start()
        spy.push(metadata=metadata, workbook=workbook, datasource=workbook, worksheet=None,
                 metadata_state_file=pickle_file_name)
        print(f'Second push of {count} with no changes {int(_common.timer_elapsed(timer).total_seconds() * 1000)} ms')

        metadata.iloc[0]['Formula'] = 'sawtooth(15min)'
        timer = _common.timer_start()
        spy.push(metadata=metadata, workbook=workbook, datasource=workbook, worksheet=None,
                 metadata_state_file=pickle_file_name)
        print(f'Third push of {count} with one change {int(_common.timer_elapsed(timer).total_seconds() * 1000)} ms')


@pytest.mark.performance
def test_metadata_push_performance_flat_tags():
    workbook = 'test_metadata_push_performance_flat_tags'
    count = 200000
    metadata = pd.DataFrame({
        'Name': [f'Signal ' + str(i).zfill(math.floor(math.log(count, 10))) for i in range(count)],
        'Type': 'Signal',
        'Formula': [f'sinusoid({i + 1}s)' for i in range(count)]
    })

    timer = _common.timer_start()
    pr = cProfile.Profile()
    pr.enable()

    spy.push(metadata=metadata, workbook=workbook, datasource=workbook, worksheet=None)

    pr.disable()
    s = io.StringIO()
    sort_by = pstats.SortKey.CUMULATIVE
    ps = pstats.Stats(pr, stream=s).sort_stats(sort_by)
    ps.print_stats()
    print(s.getvalue())

    print(f'Push of {count} items took {int(_common.timer_elapsed(timer).total_seconds() * 1000)} ms')


@pytest.mark.performance
def test_metadata_push_performance_tree():
    workbook = 'test_metadata_push_performance_tree'
    count = 100000
    order = math.floor(math.log(count, 10))

    def _path(n):
        path_parts = list()
        for i in range(order - 1):
            modulo = int(math.pow(10, i + 1))
            section = n - int(n % modulo)
            path_parts.insert(0, f'Section {section}')

        return ' >> '.join(path_parts)

    metadata = pd.DataFrame({
        'Name': 'The Signal',
        'Asset': [f'Asset ' + str(i).zfill(order) for i in range(count)],
        'Path': [_path(i) for i in range(count)],
        'Type': 'Signal',
        'Formula': [f'sinusoid({i + 1}s)' for i in range(count)]
    })

    timer = _common.timer_start()
    pr = cProfile.Profile()
    pr.enable()

    push_results_df = spy.push(metadata=metadata, workbook=workbook, datasource=workbook, worksheet=None)

    pr.disable()
    s = io.StringIO()
    sort_by = pstats.SortKey.CUMULATIVE
    ps = pstats.Stats(pr, stream=s).sort_stats(sort_by)
    ps.print_stats()
    print(s.getvalue())

    print(f'Push of {count} items took {int(_common.timer_elapsed(timer).total_seconds() * 1000)} ms')

    print(f'Length of DataFrame: {len(push_results_df)}')


@pytest.mark.system
def test_workbook_duplication():
    if not spy.utils.is_server_version_at_least(64):
        # This test requires CRAB-40301 to be fixed
        return

    do_workbook_duplication('test_workbook_duplication_datasource')

    try:
        spy.options.force_calculated_scalars = False
        do_workbook_duplication(spy.INHERIT_FROM_WORKBOOK)
    finally:
        spy.options.force_calculated_scalars = True


def do_workbook_duplication(datasource):
    workbooks_api = WorkbooksApi(spy.client)
    tree_name = 'test_metadata.test_workbook_duplication'
    workbook_1 = f'{tree_name} {_common.new_placeholder_guid()}'

    signal_id = spy.search({'Name': 'Area A_Temperature', 'Datasource ID': 'Example Data'}).iloc[0]['ID']
    metadata_df = pd.DataFrame([{
        'Path': tree_name,
        'Name': '1 Metric',
        'Type': 'Metric',
        'My Property': 1,
        'Measured Item': signal_id
    }, {
        'Path': tree_name,
        'Name': '2 Signal',
        'Formula': 'sinusoid()',
        'My Property': 2
    }, {
        'Path': tree_name,
        'Name': '3 Condition',
        'Formula': 'days()',
        'My Property': 3
    }, {
        'Path': tree_name,
        'Name': '4 Scalar',
        'Formula': '1',
        'My Property': 4
    }])

    push_df = spy.push(metadata=metadata_df, workbook=workbook_1, datasource=datasource, worksheet=None)
    if spy.options.force_calculated_scalars:
        assert push_df[push_df['Name'] == '4 Scalar'].iloc[0]['Type'] == 'CalculatedScalar'
    else:
        assert push_df[push_df['Name'] == '4 Scalar'].iloc[0]['Type'] == 'LiteralScalar'

    search_df = spy.search({'Path': tree_name}, workbook=workbook_1, all_properties=True)
    assert len(search_df) == 4
    data_ids = search_df[search_df['Data ID'].str.startswith(f'[{push_df.spy.workbook_id}]')]
    assert len(data_ids) == 4
    assert search_df['Datasource Name'].drop_duplicates().to_list() == [datasource]

    workbook_output = workbooks_api.create_workbook(body=WorkbookInputV1(
        name=f'{workbook_1} Clone',
        branch_from=push_df.spy.workbook_id
    ))

    # Make sure the Data IDs got fixed up when it was cloned
    cloned_df = spy.search({'Path': tree_name}, workbook=workbook_output.id, all_properties=True)
    cloned_df = cloned_df.sort_values(by='Name')
    assert len(cloned_df) == 4
    data_ids = cloned_df[cloned_df['Data ID'].str.startswith(f'[{workbook_output.id}]')]
    assert len(data_ids) == 4
    assert search_df['Datasource Name'].drop_duplicates().to_list() == [datasource]
    assert cloned_df['My Property'].to_list() == [1, 2, 3, 4]

    # Now add something to the tree
    new_metadata_df = pd.concat([cloned_df, pd.DataFrame([{
        'Path': tree_name,
        'Name': '5 Additional Scalar',
        'Formula': '2',
        'My Property': 5
    }])]).reset_index(drop=True)
    cloned_push_df = spy.push(metadata=new_metadata_df, workbook=workbook_output.id, datasource=datasource,
                              worksheet=None)

    assert cloned_push_df.spy.workbook_id == workbook_output.id

    # Make sure nothing got added to the original tree
    search_df = spy.search({'Path': tree_name}, workbook=workbook_1, all_properties=True)
    assert len(search_df) == 4
    data_ids = search_df[search_df['Data ID'].str.startswith(f'[{push_df.spy.workbook_id}]')]
    assert len(data_ids) == 4

    # Make sure the thing was added to the duplicated tree
    search_df = spy.search({'Path': tree_name}, workbook=workbook_output.id, all_properties=True)
    assert len(search_df) == 5
    data_ids = search_df[search_df['Data ID'].str.startswith(f'[{workbook_output.id}]')]
    assert len(data_ids) == 5
    assert search_df['Datasource Name'].drop_duplicates().to_list() == [datasource]

    # Now make a new (blank) workbook that we can clone a worksheet into. (This exercises a different codepath in
    # appserver.)
    workbook_3 = Analysis(f'{tree_name} {_common.new_placeholder_guid()}')
    workbook_3.worksheet('Sheet 1')
    spy.workbooks.push(workbook_3)

    search_df = spy.search({'Path': tree_name}, workbook=workbook_3.id, all_properties=True)
    assert len(search_df) == 0

    asset_tree_root_id = push_df.loc['__side_effect_asset_1__']['ID']
    workbooks_api.create_worksheet(workbook_id=workbook_3.id,
                                   body=WorksheetInputV1(
                                       name='Duplicated',
                                       branch_from=push_df.spy.worksheet_id,
                                       item_ids_to_clone=[asset_tree_root_id]
                                   ))

    search_df = spy.search({'Path': tree_name}, workbook=workbook_3.id, all_properties=True)
    assert len(search_df) == 4
    data_ids = search_df[search_df['Data ID'].str.startswith(f'[{workbook_3.id}]')]
    assert len(data_ids) == 4


@pytest.mark.system
def test_condition_metadata_properties_roundtrip():
    test_name = 'test_condition_metadata_properties'

    metadata_json = r"""
    [{
        "Formula": "$sb.merge(30d, true, 'Visual Factory Batch ID')",
        "Formula Parameters": {
            "sb": {"Name": "Source - Baseline", "Asset": "Segment 01", "Path": "Level 2 >> Product 1"}
        },
        "Formula Version": 26,
        "Metadata Properties": "Visual+Factory+Batch+ID=string",
        "Name": "Formula 5",
        "Type": "CalculatedCondition",
        "UIConfig": {
            "advancedParametersCollapsed": true,
            "configVersion": 11,
            "helpShown": true,
            "helpView": "documentation",
            "type": "formula"
        }
    }, {
        "Asset": "Segment 01",
        "Formula": "$condition",
        "Formula Parameters": {
            "condition": {"Name": "Source - Baseline", "Asset": "Segment 01", "Path": "Product 1"}
        },
        "Formula Version": 26,
        "Maximum Duration": {
            "Unit Of Measure": "d",
            "Value": 30
        },
        "Metadata Properties": "Product=string&Activity=string&Visual+Factory+Batch+ID=string&VisualFactoryInfo=string&VisualFactoryAsset=string&VisualFactoryProcessStep=string&BatchID=string&DisplayBatchID=string",
        "Name": "Source - Baseline",
        "Path": "Level 2 >> Product 1",
        "Type": "CalculatedCondition"
    }, {
        "Asset": "Segment 01",
        "Formula": [
            "/*",
            "Condition from RTMS containing the",
            "Baseline schedule",
            "",
            "If substep is not used, use the condition ",
            "'Empty Condition' scoped to this workbook",
            "*/",
            "$baseline"
        ],
        "Formula Parameters": {
            "baseline": {"Name": "RTMS - L2 - Baseline"}
        },
        "Formula Version": 27,
        "Maximum Duration": {
            "Unit Of Measure": "d",
            "Value": 30
        },
        "Metadata Properties": "Product=string&Activity=string&Visual+Factory+Batch+ID=string&VisualFactoryInfo=string&VisualFactoryAsset=string&VisualFactoryProcessStep=string&BatchID=string&DisplayBatchID=string",
        "Name": "Source - Baseline",
        "Path": "Product 1",
        "Type": "CalculatedCondition",
        "columnType": "Calculation",
        "manuallyAdded": "D915FCDE-6C32-41DE-9A47-74AEED9FFEF0",
        "parameterToColumn": "[{\"parameterName\":\"baseline\",\"columnName\":\"RTMS - L2 - Baseline\",\"id\":\"0EF5FEC9-14AB-EE50-A5E2-2712636663E2\",\"type\":\"StoredCondition\"}]"
    }, {
        "Debounce Duration": {
            "Unit Of Measure": "min",
            "Value": 5
        },
        "Maximum Duration": "30d",
        "Metadata Properties": "Product=string&Activity=string&Visual+Factory+Batch+ID=string&VisualFactoryInfo=string&VisualFactoryAsset=string&VisualFactoryProcessStep=string&BatchID=string&DisplayBatchID=string",
        "Name": "RTMS - L2 - Baseline",
        "Stored Series Cache Version": 1,
        "Type": "StoredCondition",
        "Uncertainty Override": {
            "Unit Of Measure": "d",
            "Value": 3
        }
    }]
    """

    metadata_dict = json.loads(metadata_json)
    for d in metadata_dict:
        if isinstance(d.get('Formula'), list):
            d['Formula'] = '\n'.join(d['Formula'])
    metadata = pd.DataFrame(metadata_dict)
    push_df = spy.push(metadata=metadata, workbook=test_name, worksheet=None, datasource=test_name)
    workbooks = spy.workbooks.pull(push_df.spy.workbook_id)
    workbook = workbooks[0]
    item_inventory = {item.name: item for item in workbook.item_inventory.values()}
    rtms_baseline = item_inventory['RTMS - L2 - Baseline']
    assert 'Capsule Property Units' in rtms_baseline
    assert 'Metadata Properties' not in rtms_baseline
    assert rtms_baseline['Capsule Property Units'] == {'Activity': 'string',
                                                       'BatchID': 'string',
                                                       'DisplayBatchID': 'string',
                                                       'Product': 'string',
                                                       'Visual Factory Batch ID': 'string',
                                                       'VisualFactoryAsset': 'string',
                                                       'VisualFactoryInfo': 'string',
                                                       'VisualFactoryProcessStep': 'string'}
    formula_5_baseline = item_inventory['Formula 5']
    formula_5_baseline.name = 'My Formula'
    formula_5_baseline_id = formula_5_baseline.id
    spy.workbooks.push(workbook)

    items_api = ItemsApi(spy.client)
    my_formula = items_api.get_item_and_all_properties(id=formula_5_baseline_id)
    assert my_formula.name == 'My Formula'
    metadata_properties = [p for p in my_formula.properties if p.name == 'Metadata Properties'][0]
    assert metadata_properties.value == 'Visual+Factory+Batch+ID=string'


@pytest.mark.system
def test_interrupt_metadata_push():
    test_name = 'test_interrupt_metadata_push'
    metadata_df = pd.DataFrame({'Numbers': range(10)})
    metadata_df['Formula'] = metadata_df['Numbers'].apply(lambda x: f'sinusoid({x}s)')
    metadata_df['Type'] = 'Signal'
    metadata_df['Name'] = metadata_df['Numbers'].apply(lambda x: f'Signal {x}')

    with mock.patch('seeq.sdk.SignalsApi.put_signals', side_effect=KeyboardInterrupt):
        with pytest.raises(KeyboardInterrupt):
            spy.push(metadata=metadata_df, workbook=test_name, worksheet=None, datasource=test_name)

        with pytest.raises(KeyboardInterrupt):
            spy.push(metadata=metadata_df, workbook=test_name, worksheet=None, datasource=test_name, errors='catalog')

    with mock.patch('seeq.sdk.SignalsApi.put_signals', side_effect=ValueError):
        with pytest.raises(SPyRuntimeError):
            spy.push(metadata=metadata_df, workbook=test_name, worksheet=None, datasource=test_name)

        pushed_df = spy.push(metadata=metadata_df, workbook=test_name, worksheet=None, datasource=test_name,
                             errors='catalog')
        assert 'ValueError' in pushed_df['Push Result'].iloc[0]
