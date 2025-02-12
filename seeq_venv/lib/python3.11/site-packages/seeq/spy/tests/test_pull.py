from __future__ import annotations

import datetime
import tempfile
import time
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd
import pytest
import pytz

from seeq import spy
from seeq.sdk import *
from seeq.spy import _common, _compatibility
from seeq.spy._errors import *
from seeq.spy._pull import RETURN_TYPE_COLUMN
from seeq.spy._status import Status
from seeq.spy.assets.tests import test_assets_system
from seeq.spy.tests import test_common
from seeq.spy.tests.test_common import Sessions


def setup_module():
    test_common.initialize_sessions()


@pytest.mark.system
def test_pull_signal_with_grid():
    session = test_common.get_session(Sessions.test_pull_signal_with_grid)

    search_results = spy.search({
        "Path": "Example >> Cooling Tower 1 >> Area A"
    }, workbook=spy.GLOBALS_ONLY, session=session)

    search_results = search_results.loc[
        search_results['Name'].isin(['Compressor Power', 'Compressor Stage'])]

    # Make sure paging works properly
    session.options.pull_page_size = 10000

    df = spy.pull(search_results, start='2019-01-01', end='2019-03-07', grid='5min', header='Name',
                  tz_convert='US/Central', session=session)

    # We assert an exact value here to draw attention to any changes. The only reason this number should change is if
    # the Example Data changes in some way or there is a (possibly unexpected) change to SPy.
    assert len(df) == 18721
    assert df.spy.status.df.iloc[0]['Pages'] == 2
    assert 200_000 < df.spy.status.df.iloc[0]['Data Processed'].bytes_processed < 2_000_000

    # Note that the canonical timezone for US/Central appears to be CST
    assert df.index[0].tzname() == 'CST'

    assert isinstance(df.iloc[0]['Compressor Power'], np.float64)
    assert isinstance(df.iloc[0]['Compressor Stage'], str)


@pytest.mark.system
def test_pull_signal_with_auto_grid():
    # 1) when the search_results don't collect the estimated sample period and spy.pull is issued with grid='auto'
    search_results = spy.search({
        "Path": "Example >> Cooling Tower 1 >> Area A"
    }, workbook=spy.GLOBALS_ONLY)

    search_results = search_results.loc[
        search_results['Name'].isin(['Compressor Power', 'Compressor Stage'])]

    df = spy.pull(search_results, start='2019-01-01T00:00:00.000Z', end='2019-03-07T00:00:00.000Z', grid='auto',
                  header='Name', tz_convert='US/Central')

    assert pd.infer_freq(df.index) == f'2{pd.offsets.Minute._prefix}'

    # We assert an exact value here to draw attention to any changes. The only reason this number should change is if
    # the Example Data changes in some way or there is a (possibly unexpected) change to SPy.
    assert len(df) == 46801

    # Note that the canonical timezone for US/Central appears to be CST
    assert df.index[0].tzname() == 'CST'
    assert isinstance(df.iloc[0]['Compressor Power'], np.float64)
    assert isinstance(df.iloc[0]['Compressor Stage'], str)

    # 2) when the search_results has the estimated sample period and spy.pull is issued with grid='auto'
    search_results = spy.search({
        'Name': 'Area ?_*',
        'Datasource Name': 'Example Data'
    }, workbook=spy.GLOBALS_ONLY, estimate_sample_period=dict(
        Start='2018-01-01T01:00:00.000Z', End='2018-01-01T02:00:00.000Z'))

    search_results = search_results.loc[search_results['Name'].isin(['Area H_Compressor Power', 'Area Z_Optimizer',
                                                                     'Area Z_Wet Bulb', 'Area I_Compressor Stage',
                                                                     'Area F_Compressor Power'])]

    df = spy.pull(search_results, start='2018-01-01T01:00:00.000Z', end='2018-01-01T02:00:00.000Z', grid='auto',
                  header='Name', tz_convert='US/Central')
    assert len(df) == 59
    assert pd.infer_freq(df.index) == f'61{pd.offsets.Second._prefix}'


@pytest.mark.system
def test_pull_signal_no_grid():
    # This test ensures that time series with non-matching timestamps are returned
    # in a DataFrame with index entries properly interleaved and NaNs where one
    # series has a value and one doesn't.

    data1_df = spy.push(pd.DataFrame({'test_pull_signal_no_grid_1': [1, 2, 3]},
                                     index=[
                                         pd.to_datetime('2019-01-01T00:00:00.000Z'),
                                         pd.to_datetime('2019-01-01T01:00:00.000Z'),
                                         pd.to_datetime('2019-01-01T02:00:00.000Z'),
                                     ]), workbook='test_pull_signal_no_grid', worksheet=None)

    data2_df = spy.push(pd.DataFrame({'test_pull_signal_no_grid_2': [10, 20, 30]},
                                     index=[
                                         pd.to_datetime('2019-01-01T00:10:00.000Z'),
                                         pd.to_datetime('2019-01-01T01:10:00.000Z'),
                                         pd.to_datetime('2019-01-01T02:10:00.000Z'),
                                     ]), workbook='test_pull_signal_no_grid', worksheet=None)

    data3_df = spy.push(pd.DataFrame({'test_pull_signal_no_grid_3': [100, 200, 300]},
                                     index=[
                                         pd.to_datetime('2019-01-01T00:20:00.000Z'),
                                         pd.to_datetime('2019-01-01T01:20:00.000Z'),
                                         pd.to_datetime('2019-01-01T02:20:00.000Z'),
                                     ]), workbook='test_pull_signal_no_grid', worksheet=None)

    all_df = pd.concat([data1_df, data2_df, data3_df])

    pull_df = spy.pull(all_df, start='2018-12-01T00:00:00Z', end='2019-12-01T00:00:00Z', grid=None)

    expected_df = pd.DataFrame({
        'test_pull_signal_no_grid_1': [1, np.nan, np.nan, 2, np.nan, np.nan, 3, np.nan, np.nan],
        'test_pull_signal_no_grid_2': [np.nan, 10, np.nan, np.nan, 20, np.nan, np.nan, 30, np.nan],
        'test_pull_signal_no_grid_3': [np.nan, np.nan, 100, np.nan, np.nan, 200, np.nan, np.nan, 300]
    }, index=[
        pd.to_datetime('2019-01-01T00:00:00.000Z'),
        pd.to_datetime('2019-01-01T00:10:00.000Z'),
        pd.to_datetime('2019-01-01T00:20:00.000Z'),
        pd.to_datetime('2019-01-01T01:00:00.000Z'),
        pd.to_datetime('2019-01-01T01:10:00.000Z'),
        pd.to_datetime('2019-01-01T01:20:00.000Z'),
        pd.to_datetime('2019-01-01T02:00:00.000Z'),
        pd.to_datetime('2019-01-01T02:10:00.000Z'),
        pd.to_datetime('2019-01-01T02:20:00.000Z')
    ])

    assert pull_df.equals(expected_df)


@pytest.mark.system
def test_pull_signal_with_bad_index():
    areas_df = spy.search({'Name': 'Area ?_Temperature'}, workbook=spy.GLOBALS_ONLY)
    area_a = areas_df[areas_df['Name'] == 'Area A_Temperature'].reset_index()
    area_b = areas_df[areas_df['Name'] == 'Area B_Temperature'].reset_index()
    to_pull_df = pd.concat([area_a, area_b])
    with pytest.raises(SPyValueError, match='index must be unique'):
        spy.pull(to_pull_df, start='2019-01-01T00:00:00.000Z', end='2019-01-01T01:00:00.000Z')


@pytest.mark.system
def test_pull_empty_results():
    no_data_signal_df = spy.push(metadata=pd.DataFrame([{
        'Name': 'No Data Signal',
        'Type': 'Signal'
    }]), workbook='test_pull_empty_results', worksheet=None)

    no_data_condition_df = spy.push(metadata=pd.DataFrame([{
        'Name': 'No Data Condition',
        'Maximum Duration': '1d',
        'Type': 'Condition'
    }]), workbook='test_pull_empty_results', worksheet=None)

    area_a_df = spy.search({'Name': 'Area A_Temperature'},
                           workbook=spy.GLOBALS_ONLY)

    combo1_df = pd.concat([no_data_signal_df, no_data_condition_df, area_a_df], sort=True).reset_index(drop=True)
    combo2_df = pd.concat([area_a_df, no_data_signal_df, no_data_condition_df], sort=True).reset_index(drop=True)
    combo3_df = pd.concat([no_data_condition_df, no_data_signal_df, area_a_df], sort=True).reset_index(drop=True)

    pull_df = spy.pull(no_data_signal_df, start='2019-01-01T00:00:00.000Z', end='2019-01-01T01:00:00.000Z', grid=None)
    assert len(pull_df) == 0

    pull_df = spy.pull(combo1_df, start='2019-01-01T00:00:00.000Z', end='2019-01-01T01:00:00.000Z', grid=None)
    assert len(pull_df) > 0
    assert pull_df.columns.tolist() == ['No Data Signal', 'No Data Condition', 'Area A_Temperature']

    pull_df = spy.pull(combo2_df, start='2019-01-01T00:00:00.000Z', end='2019-01-01T01:00:00.000Z', grid=None)
    assert len(pull_df) > 0
    assert pull_df.columns.tolist() == ['Area A_Temperature', 'No Data Signal', 'No Data Condition']

    pull_df = spy.pull(combo3_df, start='2019-01-01T00:00:00.000Z', end='2019-01-01T01:00:00.000Z', grid=None)
    assert len(pull_df) > 0
    assert pull_df.columns.tolist() == ['No Data Condition', 'No Data Signal', 'Area A_Temperature']

    pull_df = spy.pull(no_data_signal_df, start='2019-01-01T00:00:00.000Z', end='2019-01-01T01:00:00.000Z')

    assert len(pull_df) == 5
    assert len(pull_df.drop_duplicates()) == 1
    assert np.isnan(pull_df.drop_duplicates().iloc[0]['No Data Signal'])


@pytest.mark.system
def test_pull_across_assets():
    search_results = spy.search({
        "Path": "Example >> Cooling Tower 2",
    }, workbook=spy.GLOBALS_ONLY)

    search_results = search_results[search_results['Name'].isin(['Temperature', 'Relative Humidity'])]

    with pytest.raises(RuntimeError):
        # This will throw an error because header='Name' results in non-unique headers
        spy.pull(search_results, start='2019-01-01', end='2019-01-02', grid='5min', header='Name')

    with pytest.raises(ValueError):
        # This will throw an error because there's not a column named 'Stuff'
        spy.pull(search_results, start='2019-01-01', end='2019-01-02', grid='5min', tz_convert='US/Central',
                 group_by='Stuff')

    # Pull data twice-- once by pulling Area D Temperature directly, then again by pulling all Cooling Tower 2
    # Temperature signals with a group_by argument
    pull_df1 = spy.pull(search_results[search_results['Asset'] == 'Area D'],
                        start='2019-01-01', end='2019-01-02', grid='5min', tz_convert='US/Central', header='Name')

    pull_df2 = spy.pull(search_results,
                        start='2019-01-01', end='2019-01-02', grid='5min', tz_convert='US/Central', header='Name',
                        group_by=['Path', 'Asset'])

    # Now select only Area D from the second pull, using MultiIndex manipulation
    assert len(pull_df2.columns) == 2
    assert 'Temperature' in pull_df2.columns
    assert 'Relative Humidity' in pull_df2.columns

    # Make sure the columns are ordered the same (ordering can depend on how the search results came back)
    pull_df1 = pull_df1[['Temperature', 'Relative Humidity']]
    pull_df2 = pull_df2[['Temperature', 'Relative Humidity']]

    subset = pull_df2.xs('Area D', level='Asset').droplevel('Path')

    # They should be equal!
    assert pull_df1.equals(subset)


@pytest.mark.system
def test_pull_across_assets_2():
    # This is equivalent to what's in the spy.pull.ipynb example notebook

    workbook = 'test_pull_across_assets_2'

    area_a_signals = spy.search({
        'Path': 'Example >> Cooling Tower 1 >> Area A'
    })

    dew_point_calc = spy.push(metadata=pd.DataFrame([{
        'Type': 'Signal',
        'Name': 'Dew Point',
        # From https://iridl.ldeo.columbia.edu/dochelp/QA/Basic/dewpoint.html
        'Formula': "$T - ((100 - $RH.setUnits(''))/5)",
        'Formula Parameters': {
            '$T': area_a_signals[area_a_signals['Name'] == 'Temperature'],
            '$RH': area_a_signals[area_a_signals['Name'] == 'Relative Humidity']
        }
    }]), workbook=workbook, worksheet=None)

    all_areas = spy.search({
        'Path': 'Example >> Cooling Tower 1'
    }, recursive=False)

    pull_df = spy.pull(all_areas, start='2022-01-01T00:00:00Z', end='2022-01-01T01:00:00Z', calculation=dew_point_calc)

    assert len(pull_df) == 5
    assert pull_df.index.tolist() == [
        pd.to_datetime('2022-01-01T00:00:00Z'),
        pd.to_datetime('2022-01-01T00:15:00Z'),
        pd.to_datetime('2022-01-01T00:30:00Z'),
        pd.to_datetime('2022-01-01T00:45:00Z'),
        pd.to_datetime('2022-01-01T01:00:00Z'),
    ]
    assert pull_df.columns.tolist() == [
        'Example >> Cooling Tower 1 >> Area A',
        'Example >> Cooling Tower 1 >> Area B',
        'Example >> Cooling Tower 1 >> Area C',
        'Example >> Cooling Tower 1 >> Area G',
        'Example >> Cooling Tower 1 >> Area H',
        'Example >> Cooling Tower 1 >> Area I',
        'Example >> Cooling Tower 1 >> Area J',
        'Example >> Cooling Tower 1 >> Area K'
    ]

    status_df = pull_df.spy.status.df
    assert status_df['Type'].drop_duplicates().tolist() == ['CalculatedSignal']

    # Now make sure we get the right column names if we use the new asset format
    all_areas = spy.search({
        'Path': 'Example >> Cooling Tower 1'
    }, recursive=False, old_asset_format=False)

    # Also test grid=None to prove CRAB-40228 is fixed
    pull_df = spy.pull(all_areas, start='2022-01-01T00:00:00Z', end='2022-01-01T01:00:00Z',
                       calculation=dew_point_calc, grid=None)

    assert pull_df.columns.tolist() == [
        'Example >> Cooling Tower 1 >> Area A',
        'Example >> Cooling Tower 1 >> Area B',
        'Example >> Cooling Tower 1 >> Area C',
        'Example >> Cooling Tower 1 >> Area G',
        'Example >> Cooling Tower 1 >> Area H',
        'Example >> Cooling Tower 1 >> Area I',
        'Example >> Cooling Tower 1 >> Area J',
        'Example >> Cooling Tower 1 >> Area K'
    ]


@pytest.mark.system
def test_group_by_with_scalars_and_conditions():
    test_assets_system.build_and_push_hvac_tree('test_group_by_with_scalars_and_conditions')

    search_results = pd.concat([spy.search({
        "Path": "My HVAC Units >> Facility #1 >> Area A",
    }, workbook='test_group_by_with_scalars_and_conditions'),
        spy.search({
            "Path": "My HVAC Units >> Facility #1 >> Area B",
        }, workbook='test_group_by_with_scalars_and_conditions')]).reset_index(drop=True)

    search_results = search_results[search_results['Type'] != 'Asset']

    pull_df = spy.pull(search_results, start='2019-01-01', end='2019-01-02', grid='5min', tz_convert='US/Central',
                       header='Name', group_by=['Path', 'Asset'])

    # Make sure the Equipment ID string scalar is correct for the individual assets
    area_a_df = pull_df.query("Asset == 'Area A'")
    unique_equipment_id = area_a_df['Equipment ID'].drop_duplicates()
    assert len(unique_equipment_id) == 1
    assert unique_equipment_id.iloc[0] == 'Area A'

    area_b_df = pull_df.query("Asset == 'Area B'")
    unique_equipment_id = area_b_df['Equipment ID'].drop_duplicates()
    assert len(unique_equipment_id) == 1
    assert unique_equipment_id.iloc[0] == 'Area B'

    # The Too Hot condition for Area A and Area B should be different, if we did our bookkeeping correctly.
    too_hot_a = area_a_df['Too Hot'].reset_index(['Path', 'Asset'], drop=True)
    too_hot_b = area_b_df['Too Hot'].reset_index(['Path', 'Asset'], drop=True)
    assert not too_hot_a.equals(too_hot_b)


@pytest.mark.system
def test_bad_timezone():
    with pytest.raises(ValueError):
        spy.pull(pd.DataFrame(), tz_convert='CDT')


@pytest.mark.system
def test_omit_dates():
    search_results = spy.search({
        "Name": "Area A_Temperature"
    }, workbook=spy.GLOBALS_ONLY)

    margin_in_seconds = 5 * 60

    df = spy.pull(search_results, grid=None)
    assert 25 <= len(df) <= 35
    expected_start = pytz.utc.localize(datetime.datetime.utcnow()) - pd.Timedelta(hours=1)
    expected_end = pytz.utc.localize(datetime.datetime.utcnow())
    assert abs(pd.Timedelta(df.index[0].tz_convert('UTC') - expected_start).total_seconds()) < margin_in_seconds
    assert abs(pd.Timedelta(df.index[-1].tz_convert('UTC') - expected_end).total_seconds()) < margin_in_seconds

    # Note that this naive timestamp will be interpreted as UTC since the agent_api_key will not have a timezone set
    start = datetime.datetime.utcnow() - pd.Timedelta(hours=2)
    df = spy.pull(search_results, start=start, grid=None)

    assert 50 <= len(df) <= 70
    expected_start = pytz.utc.localize(datetime.datetime.utcnow() - pd.Timedelta(hours=2))
    assert abs(pd.Timedelta(df.index[0].tz_convert('UTC') - expected_start).total_seconds()) < margin_in_seconds
    assert abs(pd.Timedelta(df.index[-1].tz_convert('UTC') - expected_end).total_seconds()) < margin_in_seconds

    # Now add a timezone and make sure we handle that properly

    # noinspection PyTypeChecker
    start = datetime.datetime.now(tz=pytz.timezone('US/Pacific')) - pd.Timedelta(hours=1)
    df = spy.pull(search_results, start=start, grid=None)

    assert 25 <= len(df) <= 35
    expected_start = pd.to_datetime(start)
    assert abs(pd.Timedelta(
        df.index[0].tz_convert('UTC') - expected_start.tz_convert('UTC')).total_seconds()) < margin_in_seconds
    assert abs(pd.Timedelta(
        df.index[-1].tz_convert('UTC') - expected_end).total_seconds()) < margin_in_seconds


@pytest.mark.system
def test_bounding_values():
    metadata_df = pd.DataFrame([{
        'Name': 'test_bounding_values',
        'Type': 'Signal',
        'Interpolation Method': 'step'
    }], index=['test_bounding_values'])

    data_df = pd.DataFrame({'test_bounding_values': [1, 2, 3]},
                           index=[
                               pd.to_datetime('2019-01-01T00:00:00.000Z'),
                               pd.to_datetime('2019-01-01T00:01:00.000Z'),
                               pd.to_datetime('2019-01-01T00:02:00.000Z'),
                           ])

    push_df = spy.push(data=data_df, metadata=metadata_df, workbook='test_bounding_values', worksheet=None)

    pull_df = spy.pull(push_df, start=pd.to_datetime('2019-01-01T00:01:00.000Z'),
                       end=pd.to_datetime('2019-01-01T00:02:00.000Z'), grid=None)

    expected_df = pd.DataFrame({
        'test_bounding_values': [2, 3]
    }, index=[
        pd.to_datetime('2019-01-01T00:01:00.000Z'),
        pd.to_datetime('2019-01-01T00:02:00.000Z')
    ])

    assert pull_df.equals(expected_df)

    pull_df = spy.pull(push_df, start=pd.to_datetime('2019-01-01T00:00:50.000Z'),
                       end=pd.to_datetime('2019-01-01T00:01:50.000Z'), grid=None)

    expected_df = pd.DataFrame({
        'test_bounding_values': [2]
    }, index=[
        pd.to_datetime('2019-01-01T00:01:00.000Z')
    ])

    assert pull_df.equals(expected_df)

    pull_df = spy.pull(push_df, start=pd.to_datetime('2019-01-01T00:00:50.000Z'),
                       end=pd.to_datetime('2019-01-01T00:01:50.000Z'), grid=None, bounding_values=True)

    expected_df = pd.DataFrame({
        'test_bounding_values': [1, 2, 3]
    }, index=[
        pd.to_datetime('2019-01-01T00:00:00.000Z'),
        pd.to_datetime('2019-01-01T00:01:00.000Z'),
        pd.to_datetime('2019-01-01T00:02:00.000Z')
    ])

    assert pull_df.equals(expected_df)


@pytest.mark.system
def test_pull_signal_as_capsules():
    search_results = spy.search([{
        'Name': 'Area A_Temperature'
    }], workbook=spy.GLOBALS_ONLY)

    pull_df = spy.pull(search_results, shape='capsules')
    assert pull_df.columns.to_list() == ['Condition', 'Capsule Start', 'Capsule End', 'Capsule Is Uncertain']
    assert len(pull_df) == 0


@pytest.mark.system
def test_pull_condition_as_capsules():
    session = test_common.get_session(Sessions.test_pull_condition_as_capsules)

    search_results = spy.search([{
        'Name': 'Area A_Temperature'
    }, {
        'Name': 'Area Z_Temperature'
    }], workbook=spy.GLOBALS_ONLY, session=session)
    # Ensure the search results are in the requested order
    assert search_results.iloc[0]['Name'] == 'Area A_Temperature'

    push_df = spy.push(metadata=pd.DataFrame([{
        'Type': 'Condition',
        'Name': 'Hot',
        'Formula': '$a.validValues().valueSearch(isGreaterThan(80)).setProperty("My Prop", 1)',
        'Formula Parameters': {
            '$a': search_results.iloc[0]
        }
    }, {
        'Type': 'Condition',
        'Name': 'Days',
        'Formula': "days().setCursor(toTime('2019-01-01T00:00:00Z'))"
    }]), workbook='test_pull_condition_as_capsules', worksheet=None, session=session)

    # Make sure paging works properly
    session.options.pull_page_size = 100

    pull_df = spy.pull(push_df.iloc[0], start='2019-01-01T00:00:00.000Z', end='2019-06-01T00:00:00.000Z',
                       session=session)

    assert 'Condition' in pull_df
    assert 'Capsule Start' in pull_df
    assert 'Capsule End' in pull_df
    assert not pull_df['Capsule Is Uncertain'].any()

    # Make sure that the column data types are what we expect
    assert pull_df['Condition'].dtype == object
    assert pull_df['Capsule Start'].dtype.name.startswith('datetime64[ns')
    assert pull_df['Capsule End'].dtype.name.startswith('datetime64[ns')
    assert pull_df['Capsule Is Uncertain'].dtype == bool

    # As an additional sanity check, and to reflect what users commonly do, make sure we can do some date/time math
    # on entire columns and where NaTs are present
    duration = pull_df['Capsule End'] - pull_df['Capsule Start']
    assert pd.isna(duration.iloc[0])
    assert duration.iloc[1] > pd.Timedelta('0 days 00:10:00')

    assert 201 <= len(pull_df) <= 230
    assert pull_df.spy.status.df.iloc[0]['Pages'] == 3

    # Data consumption limit based on 5 months of samples (109,575) and 215 capsules, calculated 3 times.
    # (109,575 * 16 + 215 * 64) * 3 = 5,300,880 Bytes
    # We use "3" because the results are limited to 100 capsules per request, paged 3 times (100, 100, 15)
    # The actual amount should be ~1.7MB, but we allow for some variance due to cache eviction between requests. See comments on CRAB-46037.
    assert 10_000 < pull_df.spy.status.df.iloc[0]['Data Processed'].bytes_processed < 5_300_880

    assert 'My Prop' in pull_df
    only_my_prop = pull_df.drop_duplicates('My Prop')
    assert len(only_my_prop) == 1
    assert only_my_prop.loc[0]['My Prop'] == 1

    # Uncertain capsules
    pull_df = spy.pull(push_df.iloc[1], start='2019-02-01T00:00:00.000Z', end='2019-06-01T00:00:00.000Z',
                       session=session)
    assert pull_df['Capsule Is Uncertain'].all()
    assert 100 <= len(pull_df) <= 200

    # No matching extra properties
    pull_df = spy.pull(push_df.iloc[0], start='2019-01-01T00:00:00.000Z', end='2019-01-03T00:00:00.000Z',
                       capsule_properties=['Does not match anything'], session=session)

    assert 'My Prop' not in pull_df

    # No capsules
    pull_df = spy.pull(push_df.iloc[0], start='2019-01-02T10:00:00.000Z', end='2019-01-02T11:00:00.000Z',
                       session=session)
    assert len(pull_df) == 0

    # Check all the data types to make sure they're correct for this zero-length table
    assert pull_df['Condition'].dtype.name == 'object'
    assert pull_df['Capsule Start'].dtype.name == 'datetime64[ns, UTC]'
    assert pull_df['Capsule End'].dtype.name == 'datetime64[ns, UTC]'
    assert pull_df['Capsule Is Uncertain'].dtype.name == 'bool'

    # With signal aggregates
    area_a_temperature_count = search_results.iloc[0].to_dict()
    area_a_temperature_count['Statistic'] = 'Count'
    area_z_temperature_count = search_results.iloc[1].to_dict()
    area_z_temperature_count['Statistic'] = 'Count'

    area_a_temperature_max = search_results.iloc[0].to_dict()
    area_a_temperature_max['Statistic'] = 'Maximum'
    area_z_temperature_max = search_results.iloc[1].to_dict()
    area_z_temperature_max['Statistic'] = 'Maximum'

    area_a_temperature_rate = search_results.iloc[0].to_dict()
    area_a_temperature_rate['Statistic'] = 'Rate'
    area_z_temperature_rate = search_results.iloc[1].to_dict()
    area_z_temperature_rate['Statistic'] = 'Rate'

    with_signals_df = pd.concat([
        push_df[push_df['Name'] == 'Hot'],
        pd.DataFrame(
            [area_a_temperature_count, area_a_temperature_max, area_a_temperature_rate, area_z_temperature_count,
             area_z_temperature_max, area_z_temperature_rate])]).reset_index(drop=True)
    pull_df = spy.pull(with_signals_df, start='2019-01-01T00:00:00.000Z', end='2019-06-01T00:00:00.000Z',
                       shape='capsules', capsule_properties=['My Prop'], session=session)
    assert 200 <= len(pull_df) <= 230
    assert 'My Prop' in pull_df
    assert 'Area A_Temperature (Count)' in pull_df
    assert 'Area A_Temperature (Maximum)' in pull_df
    assert 'Area A_Temperature (Rate)' in pull_df
    assert 'Area Z_Temperature (Count)' in pull_df
    assert 'Area Z_Temperature (Maximum)' in pull_df
    assert 'Area Z_Temperature (Rate)' in pull_df


@pytest.mark.system
def test_pull_condition_flyover():
    workbook = f'test_pull_condition_flyover_{_common.new_placeholder_guid()}'
    push_df = spy.push(metadata=pd.DataFrame([{
        'Type': 'Condition',
        'Name': 'Flyover Condition',
        'Formula': '1.toSignal() > 0'
    }]), workbook=workbook, worksheet=None)

    pull_df = spy.pull(push_df.iloc[0], start='2024-01-01T00:00:00Z', end='2024-01-02T00:00:00Z')

    # Make sure the required columns exist and are the expected types
    assert 'Condition' in pull_df
    assert 'Capsule Start' in pull_df
    assert 'Capsule End' in pull_df
    assert 'Capsule Is Uncertain' in pull_df
    assert pull_df['Condition'].dtype == object
    assert pull_df['Capsule Start'].dtype.name == 'datetime64[ns, UTC]'
    assert pull_df['Capsule End'].dtype.name == 'datetime64[ns, UTC]'
    assert pull_df['Capsule Is Uncertain'].dtype == bool

    # There should be one row with NaT values for the start and end
    assert len(pull_df) == 1
    assert pull_df.iloc[0]['Condition'] == 'Flyover Condition'
    assert pd.isna(pull_df.iloc[0]['Capsule Start'])
    assert pd.isna(pull_df.iloc[0]['Capsule End'])
    assert not pull_df.iloc[0]['Capsule Is Uncertain']


@pytest.mark.system
def test_condition_small_page_size_can_error():
    session = test_common.get_session(Sessions.nonadmin)

    workbook = 'test_conditions_small_page_size'
    capsule_data = pd.DataFrame([{
        'Capsule Start': pd.to_datetime('2019-01-01', utc=True),
        'Capsule End': pd.to_datetime('2019-01-02', utc=True),
    }, {
        'Capsule Start': pd.to_datetime('2019-01-01', utc=True),
        'Capsule End': pd.to_datetime('2019-01-03', utc=True),
    }, {
        'Capsule Start': pd.to_datetime('2019-01-01', utc=True),
        'Capsule End': pd.to_datetime('2019-01-04', utc=True),
    }])
    spy.push(data=capsule_data, metadata=pd.DataFrame([{
        'Name': 'Condition Small Page Test',
        'Type': 'Condition',
        'Maximum Duration': '10 days'
    }]), workbook=workbook, worksheet=None, session=session)

    search_df = spy.search({"Name": "Condition Small Page Test"}, workbook=workbook, session=session)

    original_page_size = session.options.pull_page_size
    session.options.pull_page_size = 2

    try:
        if _compatibility.is_continuation_token_used():
            with pytest.raises(ApiException,
                               match="Cannot paginate this window. Submit larger limit"):
                spy.pull(search_df,
                         start='2019-01-01', end='2019-01-04',
                         session=session)
        else:
            with pytest.raises(RuntimeError,
                               match="Too much data: found too many capsules with same start time.\n Increase "
                                     "spy.options.pull_page_size.capsule count"):
                spy.pull(search_df,
                         start='2019-01-01', end='2019-01-04',
                         session=session)
    finally:
        session.options.pull_page_size = original_page_size


@pytest.mark.system
def test_conditions_with_stats():
    # Portion of spy.pull Data Lab example that failed after the switch to Formula
    # The following tests that pull_condition is successful when there are no capsule properties passed in,
    # as well as testing that column names are used if header='Name'
    area_a_compressor_power = spy.search({
        'Name': 'Area A_Temperature'
    }, workbook=spy.GLOBALS_ONLY)

    compressor_low_df = spy.push(metadata=pd.DataFrame([{
        'Type': 'Condition',
        'Name': 'test_conditions_with_stats_compressor_low',
        'Formula': '$a < 85',
        'Formula Parameters': {
            '$a': area_a_compressor_power.iloc[0]
        }
    }]), workbook='test_conditions_with_stats', worksheet=None)

    area_a_compressor_power_max = area_a_compressor_power.iloc[0].copy()
    area_a_compressor_power_max['Statistic'] = 'Minimum'

    area_a_compressor_power_delta = area_a_compressor_power.iloc[0].copy()
    area_a_compressor_power_delta['Statistic'] = 'Totalized'

    conditions_with_stats = pd.DataFrame([
        compressor_low_df.iloc[0],
        area_a_compressor_power_max,
        area_a_compressor_power_delta
    ]).reset_index(drop=True)

    pull_df = spy.pull(conditions_with_stats,
                       start='2019-01-01T00:00:00Z', end='2019-01-07T00:00:00Z',
                       shape='capsules', header='Name', grid='1h')

    assert 'Area A_Temperature (Minimum)' in pull_df
    assert 'Area A_Temperature (Totalized)' in pull_df
    assert pull_df.spy.status.df['Result'].tolist() == ['Success', 'Success', 'Success']


@pytest.mark.system
def test_conditions_unique_dataframe_index():
    # Ensure spy.pull returns a dataframe with unique indices
    session = test_common.get_session(Sessions.nonadmin)

    workbook = 'test_conditions_unique_dataframe_index'
    capsule_data = pd.DataFrame([{
        'Capsule Start': pd.to_datetime('2019-01-01', utc=True),
        'Capsule End': pd.to_datetime('2019-01-02', utc=True),
    }, {
        'Capsule Start': pd.to_datetime('2019-01-04', utc=True),
        'Capsule End': pd.to_datetime('2019-01-05', utc=True),
    }, {
        'Capsule Start': pd.to_datetime('2019-01-07', utc=True),
        'Capsule End': pd.to_datetime('2019-01-08', utc=True),
    }])
    spy.push(data=capsule_data, metadata=pd.DataFrame([{
        'Name': 'Condition Indexing Test 1',
        'Type': 'Condition',
        'Maximum Duration': '1 days'
    }]), workbook=workbook, worksheet=None, session=session)
    spy.push(data=capsule_data, metadata=pd.DataFrame([{
        'Name': 'Condition Indexing Test 2',
        'Type': 'Condition',
        'Maximum Duration': '1 days'
    }]), workbook=workbook, worksheet=None, session=session)

    search_df = spy.search({"Name": "Condition Indexing Test"}, workbook=workbook, session=session)

    original_page_size = session.options.pull_page_size
    session.options.pull_page_size = 2

    try:
        pull_df = spy.pull(search_df,
                           start='2019-01-01', end='2019-01-08',
                           session=session)

        assert 'Condition Indexing Test 1' in pull_df['Condition'].values
        assert 'Condition Indexing Test 2' in pull_df['Condition'].values
        assert pull_df.spy.status.df['Result'].tolist() == ['Success', 'Success']
        assert pull_df.index.is_unique
    finally:
        session.options.pull_page_size = original_page_size


@pytest.mark.system
def test_pull_bad_id():
    # Error
    bad_df = pd.DataFrame([{
        'ID': 'BAD!',
        'Type': 'Signal'
    }])

    pull_df = spy.pull(bad_df, start='2019-01-02T10:00:00.000Z', end='2019-01-02T11:00:00.000Z', errors='catalog')
    assert len(pull_df) == 0
    assert len(pull_df.spy.status.df) == 1

    status = Status(errors='catalog')
    pull_df = spy.pull(bad_df,
                       start='2019-01-02T10:00:00.000Z', end='2019-01-02T11:00:00.000Z',
                       status=status)

    assert len(pull_df) == 0
    assert len(status.df) == 1


@pytest.mark.system
def test_pull_condition_as_signal():
    search_results = spy.search({
        'Name': 'Area A_Temperature'
    }, workbook=spy.GLOBALS_ONLY)

    push_result = spy.push(metadata=pd.DataFrame([{
        'Type': 'Condition',
        'Name': 'Hot',
        'Formula': '$a.validValues().valueSearch(isGreaterThan(80))',
        'Formula Parameters': {
            '$a': search_results.iloc[0]
        }
    }]), workbook='test_pull_condition_as_signal', worksheet=None)

    pull_result = spy.pull(push_result, start='2019-01-01T00:00:00.000Z', end='2019-01-02T00:00:00.000Z',
                           shape='samples')

    assert len(pull_result) == 97
    assert len(pull_result['Hot'].drop_duplicates()) == 2
    assert pull_result.loc[pd.to_datetime('2019-01-01T00:00:00.000Z')]['Hot'] == 1
    assert pull_result.loc[pd.to_datetime('2019-01-01T12:45:00.000Z')]['Hot'] == 1
    assert pull_result.loc[pd.to_datetime('2019-01-01T13:00:00.000Z')]['Hot'] == 0
    assert pull_result.loc[pd.to_datetime('2019-01-01T19:30:00.000Z')]['Hot'] == 0
    assert pull_result.loc[pd.to_datetime('2019-01-01T19:45:00.000Z')]['Hot'] == 1
    assert pull_result.loc[pd.to_datetime('2019-01-01T20:00:00.000Z')]['Hot'] == 0
    assert pull_result.loc[pd.to_datetime('2019-01-01T20:15:00.000Z')]['Hot'] == 1
    assert pull_result.loc[pd.to_datetime('2019-01-01T22:00:00.000Z')]['Hot'] == 1
    assert pull_result.loc[pd.to_datetime('2019-01-02T00:00:00.000Z')]['Hot'] == 0

    pull_df = pd.concat([search_results, push_result], ignore_index=True, sort=True)

    pull_result = spy.pull(pull_df, start='2019-01-01T00:00:00.000Z', end='2019-02-01T00:00:00.000Z',
                           shape='samples')

    for ts, row in pull_result.iterrows():
        if row['Area A_Temperature'] > 80:
            assert row['Hot'] == 1
        else:
            assert row['Hot'] == 0


@pytest.mark.system
def test_pull_condition_as_signal_with_no_grid():
    search_results = spy.search({
        'Name': 'Area A_Temperature'
    }, workbook=spy.GLOBALS_ONLY)

    push_result = spy.push(metadata=pd.DataFrame([{
        'Type': 'Condition',
        'Name': 'Hot',
        'Formula': '$a.validValues().valueSearch(isGreaterThan(80))',
        'Formula Parameters': {
            '$a': search_results.iloc[0]
        }
    }]), workbook='test_pull_condition_as_signal_with_no_grid', worksheet=None)

    with pytest.raises(RuntimeError,
                       match="Pull cannot include conditions when no signals are present with shape='samples' "
                             "and grid=None"):
        spy.pull(push_result, start='2019-01-01T00:00:00.000Z', end='2019-01-02T00:00:00.000Z', grid=None,
                 shape='samples')

    with pytest.raises(RuntimeError,
                       match="Pull cannot include conditions when no signals are present with shape='samples' "
                             "and grid='auto'"):
        spy.pull(push_result, start='2019-01-01T00:00:00.000Z', end='2019-01-02T00:00:00.000Z', grid='auto',
                 shape='samples')


@pytest.mark.system
def test_pull_swapped_condition():
    search_results = spy.search({
        'Name': 'Temperature',
        'Path': 'Example >> Cooling Tower 1 >> Area A'
    }, workbook=spy.GLOBALS_ONLY, old_asset_format=False)

    push_result = spy.push(metadata=pd.DataFrame([{
        'Type': 'Signal',
        'Name': 'Temperature Minus 5',
        'Formula': '$a - 5',
        'Formula Parameters': {
            '$a': search_results.iloc[0]
        }
    }]), workbook='test_pull_swapped_condition', worksheet=None)

    push_result = spy.push(metadata=pd.DataFrame([{
        'Type': 'Condition',
        'Name': 'Cold',
        'Formula': '$a.validValues().valueSearch(isLessThan(80))',
        'Formula Parameters': {
            '$a': push_result.iloc[0]
        }
    }]), workbook='test_pull_swapped_condition', worksheet=None)

    pull_df = spy.search({
        'Type': 'Asset',
        'Path': 'Example >> Cooling Tower 2'
    }, workbook=spy.GLOBALS_ONLY, old_asset_format=False)

    # There will be an error related to trying to swap in Area F
    with pytest.raises(ApiException):
        spy.pull(pull_df, start='2019-01-01T00:00:00.000Z', end='2019-06-01T00:00:00.000Z',
                 calculation=push_result)

    status = Status(errors='catalog')
    pull_df1 = spy.pull(pull_df, start='2019-01-01T00:00:00.000Z', end='2019-06-01T00:00:00.000Z',
                        calculation=push_result, shape='capsules', status=status)

    assert len(pull_df1) > 800

    conditions = pull_df1['Condition'].drop_duplicates().tolist()

    assert len(conditions) == 2
    assert 'Example >> Cooling Tower 2 >> Area D' in conditions
    assert 'Example >> Cooling Tower 2 >> Area E' in conditions

    errors_df = status.df[status.df['Result'] != 'Success']

    assert len(errors_df) == 1
    assert 'unable to swap out Area A and swap in Area F' in errors_df.iloc[0]['Result']

    pull_df1 = spy.pull(pull_df, start='2019-01-01T00:00:00.000Z', end='2019-06-01T00:00:00.000Z',
                        calculation=push_result, shape='samples', status=status)

    assert 'Example >> Cooling Tower 2 >> Area D' in pull_df1.columns
    assert 'Example >> Cooling Tower 2 >> Area E' in pull_df1.columns
    assert len(pull_df1['Example >> Cooling Tower 2 >> Area D'].drop_duplicates().tolist()) == 2
    assert len(pull_df1['Example >> Cooling Tower 2 >> Area E'].drop_duplicates().tolist()) == 2

    pull_df2 = spy.pull(pull_df, start='2019-01-01T00:00:00.000Z', end='2019-06-01T00:00:00.000Z',
                        calculation=push_result, shape='samples', status=status,
                        group_by='Path', header='Name')

    assert 'Area D' in pull_df2.columns
    assert 'Area E' in pull_df2.columns
    assert len(pull_df2['Area D'].drop_duplicates().tolist()) == 2
    assert len(pull_df2['Area E'].drop_duplicates().tolist()) == 2

    # Now select only Area D
    pull_df1 = pull_df1[['Example >> Cooling Tower 2 >> Area D']]
    pull_df2 = pull_df2['Area D']
    subset = pull_df2.droplevel('Path')

    # They should be equal!
    assert pull_df1['Example >> Cooling Tower 2 >> Area D'].equals(subset)

    mock_exception_thrower = mock.Mock(side_effect=ApiException(status=403, reason='No thanks'))
    with mock.patch('seeq.sdk.ItemsApi.get_formula_dependencies', new=mock_exception_thrower):
        status = Status(errors='catalog')
        spy.pull(pull_df, start='2019-01-01T00:00:00.000Z', end='2019-06-01T00:00:00.000Z',
                 calculation=push_result, shape='samples', status=status)
        # We should have a swap warning for each of the Areas D, E, and F
        assert len(status.warnings) == 3
        for warning in status.warnings:
            assert 'Failed to find swapped calculations below asset "Example >> Cooling Tower 2 >> ' in warning


@pytest.mark.system
def test_seeq_server_error():
    datasources_api = DatasourcesApi(spy.session.client)
    signals_api = SignalsApi(spy.session.client)

    datasource_input = DatasourceInputV1()
    datasource_input.name = 'SPy Tests'
    datasource_input.description = 'Signals, conditions and scalars from Seeq Data Lab.'
    datasource_input.datasource_class = 'SPy Tests'
    datasource_input.datasource_id = 'SPy Tests'
    datasource_input.stored_in_seeq = False
    datasource_input.additional_properties = [ScalarPropertyV1(name='Expect Duplicates During Indexing', value=True)]
    datasource_output = datasources_api.create_datasource(body=datasource_input)  # type: DatasourceOutputV1

    signals_api.put_signal_by_data_id(datasource_class=datasource_output.datasource_class,
                                      datasource_id=datasource_output.datasource_id,
                                      data_id='A Signal With No Home',
                                      body=SignalInputV1(name='A Signal With No Home'))

    search_results = spy.search({
        'Name': 'A Signal With No Home'
    }, workbook=spy.GLOBALS_ONLY)

    # noinspection PyBroadException
    try:
        spy.pull(search_results, start='2019-01-01', end='2019-03-07')
    except Exception as e:
        assert "Connection to \'SPy Tests: SPy Tests\' is offline" in str(e)

    status = Status(errors='catalog')
    spy.pull(search_results, start='2019-01-01', end='2019-03-07', status=status)

    assert len(status.df) == 1
    assert "Connection to \'SPy Tests: SPy Tests\' is offline" in status.df.iloc[0]['Result']


@pytest.mark.system
def test_pull_scalar_only():
    compressor_power_limit = spy.push(metadata=pd.DataFrame([{
        'Name': 'Compressor Power Limit',
        'Type': 'Scalar',
        'Formula': '50kW'
    }]), workbook='test_pull_scalar_only', worksheet=None, errors='raise')

    pull_df = spy.pull(compressor_power_limit)

    assert len(pull_df) == 1
    assert pull_df.at[0, 'Compressor Power Limit'] == 50

    invalid_scalar = spy.push(metadata=pd.DataFrame([{
        'Name': 'Invalid Scalar',
        'Type': 'Scalar',
        'Formula': 'SCALAR.INVALID'
    }]), workbook='test_pull_scalar_only', worksheet=None, errors='raise')

    pull_df = spy.pull(invalid_scalar)

    assert len(pull_df) == 1
    assert pd.isna(pull_df.at[0, 'Invalid Scalar'])

    pull_df = spy.pull(invalid_scalar, invalid_values_as='INVALID')

    assert pull_df.at[0, 'Invalid Scalar'] == 'INVALID'
    assert pull_df.spy.status.df.iloc[0]['Data Processed'].bytes_processed == 0

    pull_df = spy.pull(invalid_scalar, invalid_values_as=-999)

    assert pull_df.at[0, 'Invalid Scalar'] == -999
    assert pull_df.spy.status.df.iloc[0]['Data Processed'].bytes_processed == 0


@pytest.mark.system
def test_pull_calculated_scalar():
    compressor_power = spy.search({'Name': 'Area B_Compressor Power'})

    compressor_power_limit = spy.push(metadata=pd.DataFrame([{
        'Name': 'Compressor Power Average In March 2020',
        'Type': 'Scalar',
        'Formula': "$cp.average(capsule('March 2020'))",
        'Formula Parameters': {'$cp': compressor_power}
    }]), workbook='test_pull_calculated_scalar', worksheet=None, errors='raise')

    pull_df = spy.pull(compressor_power_limit, start='2020-03-01', end='2020-04-01')

    # Check for 0, because it might be read from scalar cache
    if pull_df.spy.status.df.iloc[0]['Data Processed'].bytes_processed != 0:
        assert 100_000 < pull_df.spy.status.df.iloc[0]['Data Processed'].bytes_processed < 1_000_000


@pytest.mark.system
def test_pull_grid_auto_fail():
    search_results = spy.search({
        'Name': 'Area ?_*',
        'Datasource Name': 'Example Data'
    }, workbook=spy.GLOBALS_ONLY, estimate_sample_period=dict(Start='01/01/2018 1:00AM', End='01/01/2018 2:00AM'))
    search_results = search_results.loc[search_results['Name'].isin(['Area G_Optimizer', 'Area H_Optimizer',
                                                                     'Area I_Optimizer', 'Area J_Optimizer',
                                                                     'Area F_Compressor Power'])]
    with pytest.raises(RuntimeError, match="Could not determine sample period for any of the signals "):
        spy.pull(search_results, start='01/01/2018 1:00AM', end='01/01/2018 2:00AM', grid='auto')


@pytest.mark.system
def test_pull_from_url():
    workbook = test_common.create_worksheet_for_url_tests('test_pull_from_url')
    pull_results = spy.pull(workbook.url)
    assert len(pull_results.columns) == 3
    assert set(pull_results.columns) == {'Temperature Minus 5', 'Cold', 'Constant'}
    assert len(pd.unique(pull_results['Constant'])) == 1
    assert len(pd.unique(pull_results['Cold'])) == 2

    pull_results = spy.pull(workbook.url, start='2020-01-01T00:00Z', end='2020-01-01T17:00Z')
    assert len(pull_results.columns) == 3
    assert pull_results.index[0] == pd.Timestamp('2020-01-01T00:00Z')
    assert pull_results.index[-1] == pd.Timestamp('2020-01-01T17:00Z')


@pytest.mark.system
def test_pull_input_params_property():
    search_results = spy.search({
        "Path": "Example >> Cooling Tower 1"
    }, workbook=spy.GLOBALS_ONLY, all_properties=True)

    search_results = search_results.loc[
                         search_results['Name'].isin(['Wet Bulb'])].iloc[:3]

    df = spy.pull(search_results, start=None, end=None, grid='auto')
    with tempfile.TemporaryDirectory() as dir_path:
        df.to_pickle(str(Path(dir_path, 'pull.pickle')))
        df = pd.read_pickle(Path(dir_path, 'pull.pickle'))

        # test kwargs
        assert df.spy.kwargs['items'].equals(search_results)
        assert df.spy.kwargs['start'] is None
        assert df.spy.kwargs['end'] is None
        assert df.spy.kwargs['grid'] == 'auto'
        assert df.spy.kwargs['header'] == '__auto__'
        assert df.spy.kwargs['group_by'] is None
        assert df.spy.kwargs['shape'] == 'auto'
        assert df.spy.kwargs['capsule_properties'] is None
        assert df.spy.kwargs['tz_convert'] is None
        assert df.spy.kwargs['calculation'] is None
        assert not df.spy.kwargs['bounding_values']
        assert np.isnan(df.spy.kwargs['invalid_values_as'])
        assert df.spy.kwargs['capsules_as'] is None

        # test effective values
        assert isinstance(df.spy.query_df, pd.DataFrame)
        assert isinstance(df.spy.start, pd.Timestamp)
        assert isinstance(df.spy.end, pd.Timestamp)
        assert df.spy.grid == '120000ms'
        assert df.spy.tz_convert == df.spy.start.tz
        assert all('Success' == result for result in df.spy.status.df['Result'])
        assert df.spy.func == 'spy.pull'

    workbook = test_common.create_worksheet_for_url_tests('test_pull_input_params_property')
    pull_results = spy.pull(workbook.url, grid='auto')
    with tempfile.TemporaryDirectory() as dir_path:
        pull_results.to_pickle(str(Path(dir_path, 'pull.pickle')))
        pull_results = pd.read_pickle(Path(dir_path, 'pull.pickle'))

        # test kwargs
        assert pull_results.spy.kwargs['items'] == workbook.url  # this is a string
        assert pull_results.spy.kwargs['start'] is None
        assert pull_results.spy.kwargs['end'] is None
        assert pull_results.spy.kwargs['grid'] == 'auto'

        # test effective values
        assert isinstance(pull_results.spy.query_df, pd.DataFrame)
        assert isinstance(pull_results.spy.start, pd.Timestamp)
        assert isinstance(pull_results.spy.end, pd.Timestamp)
        assert pull_results.spy.grid == '120000ms'
        assert pull_results.spy.func == 'spy.pull'


@pytest.mark.system
def test_pull_enums():
    # Here we simulate PI enums
    data_df = spy.push(pd.DataFrame({'test_pull_enums': ['ENUM{{0|VALUE1}}', None, 'ENUM{{2|VALUE3}}']},
                                    index=[
                                        pd.to_datetime('2019-01-01T00:00:00.000Z'),
                                        pd.to_datetime('2019-01-01T01:00:00.000Z'),
                                        pd.to_datetime('2019-01-01T02:00:00.000Z'),
                                    ]),
                       type_mismatches='invalid', workbook='test_pull_enums', worksheet=None)

    pull_initial_df = spy.pull(data_df, start='2019-01-01T00:00:00.000Z', end='2019-01-01T02:00:00.000Z', grid=None,
                               enums_as=None)
    expected_initial_df = pd.DataFrame({
        'test_pull_enums': ['ENUM{{0|VALUE1}}', np.nan, 'ENUM{{2|VALUE3}}'],
    }, index=[
        pd.to_datetime('2019-01-01T00:00:00.000Z'),
        pd.to_datetime('2019-01-01T01:00:00.000Z'),
        pd.to_datetime('2019-01-01T02:00:00.000Z'),
    ])
    assert pull_initial_df.equals(expected_initial_df)

    pull_numeric_df = spy.pull(data_df, start='2019-01-01T00:00:00.000Z', end='2019-01-01T02:00:00.000Z', grid=None,
                               enums_as='numeric')
    expected_numeric_df = pd.DataFrame({
        'test_pull_enums': [0, np.nan, 2],
    }, index=[
        pd.to_datetime('2019-01-01T00:00:00.000Z'),
        pd.to_datetime('2019-01-01T01:00:00.000Z'),
        pd.to_datetime('2019-01-01T02:00:00.000Z'),
    ])
    assert pull_numeric_df.equals(expected_numeric_df)

    pull_string_df = spy.pull(data_df, start='2019-01-01T00:00:00.000Z', end='2019-01-01T02:00:00.000Z', grid=None,
                              enums_as='string')
    expected_string_df = pd.DataFrame({
        'test_pull_enums': ['VALUE1', np.nan, 'VALUE3'],
    }, index=[
        pd.to_datetime('2019-01-01T00:00:00.000Z'),
        pd.to_datetime('2019-01-01T01:00:00.000Z'),
        pd.to_datetime('2019-01-01T02:00:00.000Z'),
    ])
    assert pull_string_df.equals(expected_string_df)

    pull_tuple_df = spy.pull(data_df, start='2019-01-01T00:00:00.000Z', end='2019-01-01T02:00:00.000Z', grid=None,
                             enums_as='tuple')
    expected_tuple_df = pd.DataFrame({
        'test_pull_enums': [(0, 'VALUE1'), np.nan, (2, 'VALUE3')],
    }, index=[
        pd.to_datetime('2019-01-01T00:00:00.000Z'),
        pd.to_datetime('2019-01-01T01:00:00.000Z'),
        pd.to_datetime('2019-01-01T02:00:00.000Z'),
    ])
    assert pull_tuple_df.equals(expected_tuple_df)


@pytest.mark.system
def test_empty_query():
    # Test that a search that returns nothing leads to the pulled data being an empty dataframe.
    search_results = spy.search({'Name': 'foobar'}, workbook=spy.GLOBALS_ONLY)
    assert len(search_results) == 0
    pull_results = spy.pull(search_results, start='2021-01-01T00:00:00.000Z', end='2021-01-02T00:00:00.000Z')
    assert pull_results.empty

    # Test that pulling data with a query not including 'ID' and 'Type' raises an error.
    with pytest.raises(ValueError, match='items DataFrame must include "ID" column and "Type" column'):
        spy.pull(pd.DataFrame([{'Name': 'Gilligan'}]), start='2021-01-01T00:00:00.000Z', end='2021-01-02T00:00:00.000Z')


@pytest.mark.system
def test_pull_condition_with_partial_capsule_properties():
    condition_metadata = pd.DataFrame([{
        'Name': 'CRAB-28208',
        'Type': 'Condition',
        'Maximum Duration': '1d'
    }])

    # capsule 1
    start_c1 = pd.to_datetime('2021-12-03T01:00:00+00:00')
    end_c1 = pd.to_datetime('2021-12-03T02:00:00+00:00')
    data_c1 = {'Capsule Start': start_c1, 'Capsule End': end_c1,
               'Property 1': 'ABC', 'Property 2': 'DEF', 'Property 3': 'GHI', 'Property 4': 'JKL'}

    # capsule 2
    start_c2 = pd.to_datetime('2021-12-03T04:00:00+00:00')
    end_c2 = pd.to_datetime('2021-12-03T05:00:00+00:00')
    data_c2 = {'Capsule Start': start_c2, 'Capsule End': end_c2,
               'Property 3': 'GHI', 'Property 4': 'JKL'}

    push_data = pd.DataFrame([data_c1, data_c2])
    push_results = spy.push(data=push_data, metadata=condition_metadata,
                            workbook='test_pull_condition_with_partial_capsule_properties', worksheet=None)

    pulled_capsules = spy.pull(push_results,
                               start=pd.to_datetime('2021-12-03T00:00+00:00'),
                               end=pd.to_datetime('2021-12-03T06:00:00+00:00'))

    expected_columns = ['Condition', 'Capsule Start', 'Capsule End', 'Capsule Is Uncertain']
    expected_properties = [f'Property {n}' for n in range(1, 5)]

    # Assert that the columns of the output look as we expect.
    # Most importantly, all properties are present, not just those from the last capsule
    assert len(pulled_capsules.columns) == 8
    assert list(pulled_capsules.columns[:4]) == expected_columns
    assert set(pulled_capsules.columns[4:]) == set(expected_properties)

    assert pulled_capsules.loc[0, 'Property 1'] == 'ABC'
    assert pulled_capsules.loc[0, 'Property 2'] == 'DEF'
    assert pulled_capsules.loc[0, 'Property 3'] == 'GHI'
    assert pulled_capsules.loc[0, 'Property 4'] == 'JKL'

    assert pd.isnull(pulled_capsules.loc[1, 'Property 1'])
    assert pd.isnull(pulled_capsules.loc[1, 'Property 2'])
    assert pulled_capsules.loc[1, 'Property 3'] == 'GHI'
    assert pulled_capsules.loc[1, 'Property 4'] == 'JKL'


@pytest.mark.system
def test_pull_condition_with_timezone_convert():
    condition_metadata = pd.DataFrame([{
        'Name': 'CRAB-42224',
        'Type': 'Condition',
        'Formula': 'days("US/Pacific")'
    }, {
        'Name': 'CRAB-44235',
        'Type': 'Condition',
        'Formula': '1.toSignal() > 0'
    }])
    push_results = spy.push(metadata=condition_metadata,
                            workbook='test_pull_condition_with_timezone_convert', worksheet=None)

    pulled_capsules = spy.pull(push_results,
                               start=pd.to_datetime('2024-01-01T00:00-08:00'),
                               end=pd.to_datetime('2024-01-03T00:00-08:00'),
                               tz_convert='US/Pacific')

    # Pandas' DateTimes will == even in different timezones. Convert to str to assert that they're localized correctly.
    expected_starts = {'2024-01-01 00:00:00-08:00', '2024-01-02 00:00:00-08:00', str(pd.NaT)}
    assert set(pulled_capsules['Capsule Start'].astype(str).to_list()) == expected_starts
    expected_ends = {'2024-01-02 00:00:00-08:00', '2024-01-03 00:00:00-08:00', str(pd.NaT)}
    assert set(pulled_capsules['Capsule End'].astype(str).to_list()) == expected_ends


@pytest.mark.system
def test_pull_multiple_stats_per_signal():
    query_df = pd.DataFrame()
    query_df['Name'] = ['Temperature', 'Wet Bulb', 'Optimizer']
    query_df['Datasource Name'] = 'Example Data'
    query_df['Path'] = 'Example >> Cooling Tower 1'
    query_df['Asset'] = 'Area A'
    search_df = spy.search(query_df)

    condition_push_df = spy.push(metadata=pd.DataFrame([{
        'Name': 'Manual Condition',
        'Formula': 'condition('
                   '    capsule('
                   '        "2021-01-02T00:00Z",'
                   '        "2021-01-02T12:00Z"'
                   '    ),'
                   '    capsule('
                   '        "2021-01-03T00:00Z",'
                   '        "2021-01-03T12:00Z"'
                   '    )'
                   ')'
    }]), workbook='test_pull_multiple_stats_per_signal')

    signal_df_max = search_df.copy()
    signal_df_max['Statistic'] = 'maximum'
    signal_df_min = search_df.copy()
    signal_df_min['Statistic'] = 'minimum'
    signal_df_avg = search_df.copy()
    signal_df_avg['Statistic'] = 'average'

    pull_all_query = pd.concat([signal_df_max,
                                signal_df_min,
                                signal_df_avg,
                                condition_push_df], ignore_index=True).drop(columns=['Path', 'Asset'])

    pull_optimizer_query = pull_all_query.query('Name == "Optimizer" or Name == "Manual Condition"')
    pull_temperature_query = pull_all_query.query('Name == "Temperature" or Name == "Manual Condition"')
    pull_wet_bulb_query = pull_all_query.query('Name == "Wet Bulb" or Name == "Manual Condition"')

    start = '1/01/2021 12:00 AM'
    end = '1/5/2021 12:00 AM'

    pull_all = spy.pull(pull_all_query,
                        start=start,
                        end=end,
                        shape='capsules')
    pull_optimizer = spy.pull(pull_optimizer_query,
                              start=start,
                              end=end,
                              shape='capsules')
    pull_temperature = spy.pull(pull_temperature_query,
                                start=start,
                                end=end,
                                shape='capsules')
    pull_wet_bulb = spy.pull(pull_wet_bulb_query,
                             start=start,
                             end=end,
                             shape='capsules')

    optimizer_columns = ['Optimizer (%s)' % s for s in ('average', 'minimum', 'maximum')]
    temperature_columns = ['Temperature (%s)' % s for s in ('average', 'minimum', 'maximum')]
    wet_bulb_columns = ['Wet Bulb (%s)' % s for s in ('average', 'minimum', 'maximum')]

    pd.testing.assert_frame_equal(pull_all[optimizer_columns], pull_optimizer[optimizer_columns])
    pd.testing.assert_frame_equal(pull_all[temperature_columns], pull_temperature[temperature_columns])
    pd.testing.assert_frame_equal(pull_all[wet_bulb_columns], pull_wet_bulb[wet_bulb_columns])


@pytest.mark.system
def test_data_processed_reconciliation():
    original_label = spy.session.request_origin_label
    original_page_size = spy.options.pull_page_size
    new_label = 'test_data_processed_reconciliation ' + _common.new_placeholder_guid()
    try:
        spy.session.request_origin_label = new_label
        spy.options.pull_page_size = 10000

        pull_df = spy.pull(spy.search({'Name': 'Area B_Compressor Power'}),
                           start='2019-01-01', end='2020-01-01', grid=None)

        spy_bytes_processed = pull_df.spy.status.df.iloc[0]['Data Processed'].bytes_processed

        usage_api = UsageApi(test_common.get_session(Sessions.admin).client)
        timer = _common.timer_start()

        while True:
            start = (datetime.datetime.utcnow() - datetime.timedelta(hours=1)).isoformat() + 'Z'
            end = datetime.datetime.utcnow().isoformat() + 'Z'
            usage_output_list = None

            try:
                usage_output_list = usage_api.get_usage(start_time=start, end_time=end, source=new_label)
                last_exception = None

            except ApiException as e:
                last_exception = e

                # 400 appears to be possible if the usage API is queried before any data has been logged.
                if e.status != 400:
                    raise

            appserver_bytes_processed = 0
            if usage_output_list is not None:
                for usage_output in usage_output_list.content:  # type: UsageOutputV1
                    appserver_bytes_processed += usage_output.bytes

            if appserver_bytes_processed == spy_bytes_processed:
                break

            if _common.timer_elapsed(timer).total_seconds() > 300:
                pytest.fail(f'Bytes processed did not reconcile within 5 minutes.\n'
                            f'SPy bytes processed: {spy_bytes_processed}\n'
                            f'Appserver bytes processed: {appserver_bytes_processed}\n'
                            f'Last exception: {last_exception}')

            time.sleep(0.1)

    finally:
        spy.session.request_origin_label = original_label
        spy.options.pull_page_size = original_page_size


@pytest.mark.system
def test_pull_callback():
    test_name = 'test_pull_callback'

    condition_metadata = pd.DataFrame([{
        'Name': f'{test_name} Condition',
        'Type': 'Condition',
        'Maximum Duration': '1d'
    }])

    # capsule 1
    start_c1 = pd.to_datetime('2021-12-03T01:00:00+00:00')
    end_c1 = pd.to_datetime('2021-12-03T02:00:00+00:00')
    data_c1 = {'Capsule Start': start_c1, 'Capsule End': end_c1,
               'Property 1': 'ABC', 'Property 2': 'DEF', 'Property 3': 'GHI', 'Property 4': 'JKL'}

    # capsule 2
    start_c2 = pd.to_datetime('2021-12-03T04:00:00+00:00')
    end_c2 = pd.to_datetime('2021-12-03T05:00:00+00:00')
    data_c2 = {'Capsule Start': start_c2, 'Capsule End': end_c2,
               'Property 3': 'GHI', 'Property 4': 'JKL'}

    condition_push_data = pd.DataFrame([data_c1, data_c2])
    condition_push_results = spy.push(data=condition_push_data, metadata=condition_metadata, workbook=test_name,
                                      worksheet=None)

    signal_push_data = pd.DataFrame({f'{test_name} Signal': [1, 2, 3]},
                                    index=[
                                        pd.to_datetime('2019-01-01T00:00:00.000Z'),
                                        pd.to_datetime('2019-01-01T01:00:00.000Z'),
                                        pd.to_datetime('2019-01-01T02:00:00.000Z'),
                                    ])

    signal_push_results = spy.push(signal_push_data, workbook=test_name, worksheet=None)

    scalar_push_data = pd.DataFrame({f'{test_name} Scalar': [50]}, index=[0])
    scalar_push_results = spy.push(metadata=pd.DataFrame([{
        'Name': f'{test_name} Scalar',
        'Type': 'Scalar',
        'Formula': '50kW'
    }]), workbook=test_name, worksheet=None)

    items_to_pull = pd.DataFrame([{
        'ID': condition_push_results.iloc[0]['ID'],
        'Name': condition_push_results.iloc[0]['Name'],
        'Type': 'Condition',
        'Start': '2021-12-03T00:00:00+00:00',
        'End': '2021-12-03T06:00:00+00:00'
    }, {
        'ID': signal_push_results.iloc[0]['ID'],
        'Name': signal_push_results.iloc[0]['Name'],
        'Type': 'Signal',
        'Start': '2019-01-01T00:00:00+00:00',
        'End': '2019-01-01T03:00:00+00:00'
    }, {
        'ID': scalar_push_results.iloc[0]['ID'],
        'Name': scalar_push_results.iloc[0]['Name'],
        'Type': 'Scalar'
    }])

    def _callback(_row, _result):
        _name = _row['Name']
        if _name.endswith('Signal'):
            assert _result.equals(signal_push_data)
        elif _name.endswith('Condition'):
            assert _result[['Capsule Start', 'Capsule End', 'Property 1', 'Property 2', 'Property 3',
                            'Property 4']].equals(condition_push_data)
        elif _name.endswith('Scalar'):
            assert _result.equals(scalar_push_data)

    pull_result = spy.pull(items_to_pull, shape=_callback, grid=None)

    assert pull_result is None


@pytest.mark.system
def test_pull_with_bad_calculation():
    workbook = 'test_pull_with_bad_calculation'
    pushed_signals_df = spy.push(metadata=pd.DataFrame([{
        'Type': 'Signal',
        'Name': f'{workbook} 1',
        'Formula': '1958$.toSignal(1d).toDiscrete()'
    }]), workbook=workbook, datasource=workbook, worksheet=None)

    bad_df = pushed_signals_df.copy()
    bad_df['Calculation'] = 'bad()'

    pull_df = spy.pull(bad_df, errors='catalog')
    status_df = pull_df.spy.status.df

    assert len(set(status_df['Result'].to_list())) == 1
    assert status_df['Result'].iloc[0].startswith('(400) Bad Request')


@pytest.mark.system
def test_pull_with_calculations():
    workbook = 'test_pull_with_calculations'
    pushed_signals_df = spy.push(metadata=pd.DataFrame([{
        'Type': 'Signal',
        'Name': f'{workbook} Signal',
        'Formula': '(sawtooth(1d)+8).setUnits("m")'
    }, {
        'Type': 'Condition',
        'Name': f'{workbook} Condition',
        'Formula': 'hours()'
    }, {
        'Type': 'Scalar',
        'Name': f'{workbook} Condition',
        'Formula': '42m'
    }]), workbook=workbook, datasource=workbook, worksheet=None)

    # 'samples' output
    calculations = [
        (1, 'Scalar', 'Signal', '$signal.aggregate(sum(), '
                                'capsule("2020-01-01T00:00:00Z", "2021-01-01T00:00:00Z"))', 5856),
        (2, 'Signal', 'Condition', 'splice(0, 1, $condition)', 1),
        (3, 'Scalar', 'Scalar', '$scalar + 4', 46),
        (4, 'Scalar', 'Signal', '$signal.aggregate(sum(), '
                                'capsule("2020-01-01T00:00:00Z", "2020-02-01T00:00:00Z"))', 496),
        (5, 'Signal', 'Condition', 'splice(10, 20, $condition)', 20),
        (6, 'Scalar', 'Scalar', '$scalar - 8', 34)
    ]

    calculation_df = pd.concat([pushed_signals_df, pushed_signals_df]).reset_index()
    calculation_df['Calculation'] = pd.Series([f for _, _, _, f, _ in calculations])

    with pytest.raises(SPyRuntimeError, match='Column headers not unique'):
        spy.pull(pd.DataFrame(calculation_df), grid=None)

    headers = [f'{n} {t} from {f}' for n, t, f, _, _ in calculations]
    calculation_df['Header'] = pd.Series(headers)
    pull_df = spy.pull(calculation_df, start='2020-01-01', end='2020-01-10')
    assert RETURN_TYPE_COLUMN not in pull_df.columns
    status_df = pull_df.spy.status.df
    assert status_df['Type'].to_list() == [f'Calculated{t}' for _, t, _, _, _ in calculations]
    assert sorted(list(pull_df.columns)) == headers
    assert pull_df.iloc[0].to_list() == [v for _, _, _, _, v in calculations]

    # 'capsules' output
    calculations = [
        (1, 'Condition', 'Signal', '$signal.toCondition()'),
        (2, 'Condition', 'Condition', '$condition'),
        (3, 'Scalar', 'Scalar', '$scalar + 4'),
        (4, 'Condition', 'Signal', '($signal+10).toCondition()'),
        (5, 'Condition', 'Condition', '$condition'),
        (6, 'Scalar', 'Scalar', '$scalar - 8')
    ]

    calculation_df = pd.concat([pushed_signals_df, pushed_signals_df]).reset_index()
    calculation_df['Calculation'] = pd.Series([f for _, _, _, f in calculations])
    headers = [f'{n} {t} from {f}' for n, t, f, _ in calculations]
    calculation_df['Header'] = pd.Series(headers)
    pull_df = spy.pull(calculation_df, start='2020-01-01', end='2020-01-10')
    status_df = pull_df.spy.status.df
    assert status_df['Type'].to_list() == [f'Calculated{t}' for _, t, _, _ in calculations]
    assert sorted(list(pull_df.columns)) == ['3 Scalar from Scalar', '6 Scalar from Scalar',
                                             'Capsule End', 'Capsule Is Uncertain', 'Capsule Start', 'Condition',
                                             'Hour', 'Value']
    assert len(pull_df) > 5

    # scalar-only output
    calculations = [
        (1, 'Scalar', 'Signal', '$signal.aggregate(sum(), '
                                'capsule("2020-01-01T00:00:00Z", "2021-01-01T00:00:00Z"))'),
        (2, 'Scalar', 'Condition', 'splice(0, 1, $condition).aggregate(sum(), '
                                   'capsule("2020-01-01T00:00:00Z", "2021-01-01T00:00:00Z"))')
    ]

    calculation_df = pushed_signals_df.iloc[:2].copy()
    calculation_df['Calculation'] = pd.Series([f for _, _, _, f in calculations])
    headers = [f'{n} {t} from {f}' for n, t, f, _ in calculations]
    calculation_df['Header'] = pd.Series(headers)
    pull_df = spy.pull(calculation_df, start='2020-01-01', end='2020-01-10')
    status_df = pull_df.spy.status.df
    assert status_df['Type'].to_list() == [f'Calculated{t}' for _, t, _, _ in calculations]
    assert sorted(list(pull_df.columns)) == headers
    assert len(pull_df) == 1
    assert pull_df.iloc[0].to_list() == [5856, 366]


@pytest.mark.performance
def test_pull_signal_performance():
    workbook = 'test_pull_signal_performance'

    size = 1
    start = '2010-01-01'
    end = '2020-01-01'

    push_df = pd.DataFrame({
        'Name': pd.Series(np.arange(1, size + 1, 1)).apply(lambda v: f't{v}'),
        'Type': 'Signal',
        'Formula': pd.Series(np.arange(1, size + 1, 1)).apply(lambda v: f'{v}.toSignal(5min)')
    })

    timer = _common.timer_start()
    push_results = spy.push(metadata=push_df, workbook=workbook, worksheet=None)
    seconds = _common.timer_elapsed(timer)
    print(f'Push of {size} signals definitions took {seconds}')

    timer = _common.timer_start()
    pull_df = spy.pull(push_results, start=start, end=end, grid=None)
    seconds = _common.timer_elapsed(timer)
    print(f'Pull of {size} signals ({len(pull_df)} samples each) took {seconds}')


@pytest.mark.performance
def test_pull_signal_performance_group_by():
    workbook = 'test_pull_signal_performance_group_by'
    size = 2000
    push_df = pd.DataFrame({
        'Path': workbook,
        'Asset': pd.Series(np.arange(1, size, 1)).apply(lambda v: f't{v}'),
        'Name': 't',
        'Type': 'Signal',
        'Formula': pd.Series(np.arange(1, size, 1)).apply(lambda v: f'{v}.toSignal()')
    })

    timer = _common.timer_start()
    push_results = spy.push(metadata=push_df, workbook=workbook, worksheet=None)
    seconds = _common.timer_elapsed(timer)
    print(f'Push of {size} signals definitions took {seconds}')

    push_results = push_results[push_results['Type'].str.endswith('Signal')]

    timer = _common.timer_start()
    pull_df = spy.pull(push_results, start='2021-01-01', end='2021-03-01', header='Name', group_by=['Path', 'Asset'])
    seconds = _common.timer_elapsed(timer)
    print(f'Pull of {size} signals ({len(pull_df)} samples each) took {seconds}')


@pytest.mark.performance
def test_pull_condition_performance():
    workbook = 'test_pull_condition_performance'

    size = 1
    start = '2010-01-01'
    end = '2020-01-01'

    push_df = pd.DataFrame({
        'Name': pd.Series(np.arange(1, size + 1, 1)).apply(lambda v: f't{v}'),
        'Type': 'Condition',
        'Formula': pd.Series(np.arange(1, size + 1, 1)).apply(lambda v: f'periods(10min, 10min)')
    })

    timer = _common.timer_start()
    push_results = spy.push(metadata=push_df, workbook=workbook, worksheet=None)
    seconds = _common.timer_elapsed(timer)
    print(f'Push of {size} conditions definitions took {seconds}')

    timer = _common.timer_start()
    pull_df = spy.pull(push_results, start=start, end=end, grid=None)
    seconds = _common.timer_elapsed(timer)
    print(f'Pull of {size} conditions ({len(pull_df)} capsules each) took {seconds}')


@pytest.mark.system
def test_interrupt_data_pull():
    to_pull_df = spy.search({'Path': 'Example', 'Name': 'Temperature'}, recursive=True)

    with mock.patch('seeq.sdk.FormulasApi.run_formula_with_http_info', side_effect=KeyboardInterrupt):
        with pytest.raises(KeyboardInterrupt):
            spy.pull(to_pull_df)

        with pytest.raises(KeyboardInterrupt):
            spy.pull(to_pull_df, errors='catalog')

    with mock.patch('seeq.sdk.FormulasApi.run_formula_with_http_info', side_effect=ValueError):
        with pytest.raises(ValueError):
            spy.pull(to_pull_df)

        pulled_df = spy.pull(to_pull_df, errors='catalog')
        assert 'ValueError' in pulled_df.spy.status.df['Result'].iloc[0]
