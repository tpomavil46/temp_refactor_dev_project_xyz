from __future__ import annotations

import json
import os
import pickle
import textwrap
from datetime import timedelta

import pandas as pd
import pytest

from seeq import spy
from seeq.base import util
from seeq.spy._errors import *
from seeq.spy.tests import test_common
from seeq.spy.workbooks.job import _pull
from seeq.spy.workbooks.tests import test_load

content_folder = 'test_workbooks_job_folder'
content_label = 'test_workbooks_job'
push_folder = 'test_workbooks_job_push_folder'
push_label = 'test_workbooks_job_push'
job_folder = os.path.join(os.path.dirname(__file__), 'job_folder_test_workbooks')
job_workbooks_folder = _pull.get_workbooks_folder(job_folder)


def setup_module():
    test_common.initialize_sessions()


def _push_test_workbooks():
    for i in range(2):
        spy.workbooks.push(test_load.load_example_export(),
                           path=content_folder, label=f'{content_label} {i}', refresh=False)


@pytest.mark.system
def test_workbooks_job_with_cleanup():
    if util.safe_exists(job_folder):
        util.safe_rmtree(job_folder)

    try:
        test_workbooks_job()
    finally:
        # We have to remove the folder because it has long filenames, and when pytest scans that folder looking for
        # tests (the second time around), it will fail because it doesn't support long filenames properly.
        if util.safe_exists(job_folder):
            util.safe_rmtree(job_folder)


@pytest.mark.ignore
def test_workbooks_job():
    _push_test_workbooks()
    test_workbooks_job_pull()
    test_workbooks_job_data_manifest()
    test_workbooks_job_data_pull()
    test_workbooks_job_zip()
    test_workbooks_job_push()
    test_workbooks_job_data_push()


@pytest.mark.ignore
def test_workbooks_job_pull():
    # This test can be executed in the debugger environment for fast debug cycles, if test_workbooks_job() has
    # been executed once before.

    workbooks_df = spy.workbooks.search({'Path': content_folder})
    assert len(workbooks_df) == 4

    results_df = spy.workbooks.job.pull(job_folder, workbooks_df, resume=False)

    assert all(['Success' in r for r in results_df['Result'].to_list()])

    # Now remove a couple of the workbooks from the job folder and try again
    removed_ids = results_df['ID'].to_list()
    removed_ids = removed_ids[1:-1]
    spy.workbooks.job.redo(job_folder, removed_ids, action='pull')

    results_df = spy.workbooks.job.pull(job_folder, workbooks_df)

    for index, row in results_df.iterrows():
        assert row['Result'] == ('Success' if row['ID'] in removed_ids else 'Success: Already pulled')

    # Now pull one last time, which should be a no-op
    results_df = spy.workbooks.job.pull(job_folder, workbooks_df)
    assert all([r == 'Success: Already pulled' for r in results_df['Result'].to_list()])


@pytest.mark.ignore
def test_workbooks_job_data_manifest():
    # This test can be executed in the debugger environment for fast debug cycles, if test_workbooks_job() has
    # been executed once before.
    spy.workbooks.job.data.manifest(job_folder, reset=True)

    all_usages = spy.workbooks.job.data._pull.load_data_usage(job_folder)
    area_a_wb_id = [usage['Definition']['ID'] for usage in all_usages.values()
                    if usage['Definition']['Name'] == 'Area A_Wet Bulb'][0]
    area_a_temp_id = [usage['Definition']['ID'] for usage in all_usages.values()
                      if (usage['Definition'].get('Asset') == 'Area A' and
                          usage['Definition']['Name'] == 'Temperature')][0]
    periods = all_usages[area_a_wb_id]['Periods']
    assert len(periods) == 2

    manifest_df = spy.workbooks.job.data.manifest(job_folder)
    area_a_wb_row = manifest_df[manifest_df['Name'] == 'Area A_Wet Bulb'].iloc[0]
    assert area_a_wb_row['Start'] == pd.Timestamp('2018-11-11 04:22:45.084000+00:00')
    assert area_a_wb_row['End'] == pd.Timestamp('2018-12-17 06:18:49.287000+00:00')
    assert area_a_wb_row['Calculation'] == textwrap.dedent("""
        $within = condition(
           capsule("2018-11-11T04:22:45.084000+00:00", "2018-11-12T04:22:45.084000+00:00"),
           capsule("2018-12-16T06:18:49.287000+00:00", "2018-12-17T06:18:49.287000+00:00")
        )

        $final = $signal.within($within)

        $final
    """).strip()

    area_a_temp_row = manifest_df[
        (manifest_df['Asset'] == 'Area A') & (manifest_df['Name'] == 'Temperature')].iloc[0]
    assert area_a_temp_row['Start'] == pd.Timestamp('2019-09-07 13:23:27.130000+00:00')
    assert area_a_temp_row['End'] == pd.Timestamp('2019-10-07 23:23:27.130000+00:00')
    assert area_a_temp_row['Calculation'] == textwrap.dedent("""
        $within = condition(
           capsule("2019-09-07T13:23:27.130000+00:00", "2019-10-07T23:23:27.130000+00:00")
        )

        $final = $signal.within($within)

        $final
    """).strip()

    affected_df = spy.workbooks.job.data.expand(job_folder, area_a_wb_id, by='1d')
    assert len(affected_df) == 1
    area_a_wb_row = affected_df.iloc[0]
    assert area_a_wb_row['Start'] == pd.Timestamp('2018-11-10 04:22:45.084000+00:00')
    assert area_a_wb_row['End'] == pd.Timestamp('2018-12-18 06:18:49.287000+00:00')

    with pytest.raises(SPyValueError, match='timedeltas must be positive'):
        spy.workbooks.job.data.expand(job_folder, area_a_wb_id, end_by='-2d')

    affected_df = spy.workbooks.job.data.expand(
        job_folder,
        {
            'Type': 'Signal',
            'Path': 'Example >> Cooling Tower *',
            'Asset': 'Area Q'
        }, by=timedelta(days=2))

    assert len(affected_df) == 0

    affected_df = spy.workbooks.job.data.expand(
        job_folder,
        {
            'Type': 'Signal',
            'Path': 'Example >> Cooling Tower *',
            'Asset': '/Area [ABC]/'
        }, by=timedelta(days=2))

    assert len(affected_df) == 1
    area_a_temp_row = affected_df.iloc[0]
    assert area_a_temp_row['Start'] == pd.Timestamp('2019-09-05T13:23:27.130000+00:00')
    assert area_a_temp_row['End'] == pd.Timestamp('2019-10-09T23:23:27.130000+00:00')

    affected_df = spy.workbooks.job.data.add(job_folder, area_a_temp_id,
                                             start='1975-02-12T06:00:00Z', end='1980-08-26T00:00:00Z')
    area_a_temp_row = affected_df.iloc[0]
    assert area_a_temp_row['Start'] == pd.Timestamp('1975-02-12T06:00:00+00:00')
    assert area_a_temp_row['End'] == pd.Timestamp('2019-10-09T23:23:27.130000+00:00')

    spy.workbooks.job.data.calculation(job_folder, area_a_temp_id, formula='resample($signal, 1d)')
    spy.workbooks.job.data.remove(job_folder, area_a_temp_id, end='1976-01-01T00:00:00Z')
    spy.workbooks.job.data.remove(job_folder, area_a_temp_id, start='1977-01-01T00:00:00Z', end='1978-01-01T00:00:00Z')
    spy.workbooks.job.data.remove(job_folder, area_a_temp_id, start='1979-01-01T00:00:00Z', end='1981-01-01T00:00:00Z')
    spy.workbooks.job.data.remove(job_folder, area_a_temp_id, start='2000-01-01T00:00:00Z')

    manifest_df = spy.workbooks.job.data.manifest(job_folder)
    area_a_temp_row = manifest_df[
        (manifest_df['Asset'] == 'Area A') & (manifest_df['Name'] == 'Temperature')].iloc[0]
    assert area_a_temp_row['Start'] == pd.Timestamp('1976-01-01T00:00:00Z')
    assert area_a_temp_row['End'] == pd.Timestamp('1979-01-01T00:00:00Z')
    assert area_a_temp_row['Calculation'] == textwrap.dedent("""
        $within = condition(
           capsule("1976-01-01T00:00:00+00:00", "1977-01-01T00:00:00+00:00"),
           capsule("1978-01-01T00:00:00+00:00", "1979-01-01T00:00:00+00:00")
        )

        $final = $signal.within($within)

        resample($final, 1d)
    """).strip()

    affected_df = spy.workbooks.job.data.remove(job_folder, area_a_temp_id)
    assert len(affected_df) == 1
    assert 'Start' not in affected_df.columns
    assert 'End' not in affected_df.columns

    manifest_df = spy.workbooks.job.data.manifest(job_folder)
    area_a_temp_row = manifest_df[(manifest_df['Asset'] == 'Area A') & (manifest_df['Name'] == 'Temperature')]
    assert len(area_a_temp_row) == 0


@pytest.mark.ignore
def test_workbooks_job_data_pull():
    # This test can be executed in the debugger environment for fast debug cycles, if test_workbooks_job() has
    # been executed once before.

    results_df = spy.workbooks.job.data.pull(job_folder=job_folder, resume=False)
    assert len(results_df) == 7
    results = results_df['Result'].to_list()
    assert all([r == 'Success' for r in results])

    area_a_temperature_id = results_df[results_df['Name'] == 'Area A_Temperature'].iloc[0]['ID']
    spy.workbooks.job.data.redo(job_folder, area_a_temperature_id, 'pull')

    results_df = spy.workbooks.job.data.pull(job_folder=job_folder)
    assert len(results_df) == 7
    results = results_df[results_df['Name'] == 'Area A_Temperature']['Result'].to_list()
    assert results == ['Success']
    results = results_df[results_df['Name'] != 'Area A_Temperature']['Result'].to_list()
    assert all([r == 'Success: Already pulled' for r in results])

    # Now pull one last time, which should be a no-op
    results_df = spy.workbooks.job.data.pull(job_folder=job_folder)
    assert len(results_df) == 7
    assert all([r == 'Success: Already pulled' for r in results])


@pytest.mark.ignore
def test_workbooks_job_zip():
    # This test can be executed in the debugger environment for fast debug cycles, if test_workbooks_job() has
    # been executed once before.

    spy.workbooks.job.zip(job_folder, overwrite=True)

    with pytest.raises(SPyRuntimeError, match='already exists'):
        spy.workbooks.job.zip(job_folder)

    with pytest.raises(SPyRuntimeError, match='already exists'):
        spy.workbooks.job.unzip(job_folder + '.zip')

    spy.workbooks.job.unzip(job_folder + '.zip', overwrite=True)


@pytest.mark.ignore
def test_workbooks_job_push():
    # This test can be executed in the debugger environment for fast debug cycles, if test_workbooks_job() has
    # been executed once before.
    write_example_data_export_map_to_nowhere()

    results_df = spy.workbooks.job.push(job_folder, path=push_folder, resume=False, label=push_label,
                                        create_dummy_items=True)

    assert all(['Success' in r for r in results_df['Result'].to_list()])

    push_context_file = os.path.join(job_folder, 'item_map.pickle')
    with util.safe_open(push_context_file, 'rb') as f:
        push_context = pickle.load(f)

    # Now reset a couple of the workbooks and try again
    removed_ids = results_df['ID'].to_list()
    removed_ids = removed_ids[1:-1]
    spy.workbooks.job.redo(job_folder, removed_ids, action='push')

    with util.safe_open(push_context_file, 'wb') as f:
        pickle.dump(push_context, f, protocol=4)

    results_df = spy.workbooks.job.push(job_folder, path=push_folder, label=push_label, create_dummy_items=True)

    for index, row in results_df.iterrows():
        assert row['Result'] == ('Success' if row['ID'] in removed_ids else 'Success: Already pushed')

    # Now push one last time, which should be a no-op
    results_df = spy.workbooks.job.push(job_folder, path=push_folder, label=push_label, create_dummy_items=True)

    assert all([r == 'Success: Already pushed' for r in results_df['Result'].to_list()])


@pytest.mark.ignore
def test_workbooks_job_data_push():
    # This test can be executed in the debugger environment for fast debug cycles, if test_workbooks_job() has
    # been executed once before.
    write_example_data_export_map_to_nowhere()
    results_df = spy.workbooks.job.data.push(job_folder=job_folder, resume=False)
    results = results_df[results_df['Type'] != 'Asset']['Result'].to_list()
    assert len(results) == 8
    assert all([r == 'Success' for r in results])
    results = results_df[results_df['Type'] == 'Asset']['Result'].to_list()
    assert len(results) == 3
    assert all([r == 'N/A' for r in results])

    # Reset one item so that it gets pushed again
    area_a_id = results_df[results_df['Name'] == 'Area A_Temperature'].iloc[0]['ID']
    spy.workbooks.job.data.redo(job_folder, area_a_id, 'push')

    results_df = spy.workbooks.job.data.push(job_folder=job_folder)
    results = results_df[results_df['Type'] == 'Asset']['Result'].to_list()
    assert all([r == 'N/A' for r in results])
    non_assets = results_df[results_df['Type'] != 'Asset']
    results = non_assets[non_assets['Name'] == 'Area A_Temperature']['Result'].to_list()
    assert results == ['Success']
    results = non_assets[non_assets['Name'] != 'Area A_Temperature']['Result'].to_list()
    assert len(results) == 7
    assert all([r == 'Success: Already pushed' for r in results])

    # Now push one last time, which should be a no-op
    results_df = spy.workbooks.job.data.push(job_folder=job_folder)
    results = results_df[results_df['Type'] != 'Asset']['Result'].to_list()
    assert len(results) == 8
    assert all([r == 'Success: Already pushed' for r in results])


def write_example_data_export_map_to_nowhere():
    example_data_map = {
        "Datasource Class": "Time Series CSV Files",
        "Datasource ID": "Example Data",
        "Datasource Name": "Example Data",
        "Item-Level Map Files": [],
        "RegEx-Based Maps": [
            {
                "Old": {
                    "Type": "(?<type>.*)",
                    "Datasource Class": "Time Series CSV Files",
                    "Datasource Name": "Example Data",
                    "Data ID": "(?<data_id>.*)"
                },
                "New": {
                    "Type": "${type}",
                    "Datasource Class": "Nowhere Historian",
                    "Datasource Name": "Nowhere Datasource",
                    "Data ID": "${data_id}"
                }
            }
        ]
    }

    job_datasource_maps_folder = _pull.get_datasource_maps_folder(job_folder)
    util.safe_makedirs(job_datasource_maps_folder, exist_ok=True)
    example_data_json_filename = os.path.join(
        job_datasource_maps_folder, 'Datasource_Map_Time Series CSV Files_Example Data_Example Data.json')
    with util.safe_open(example_data_json_filename, 'w') as f:
        json.dump(example_data_map, f, indent=4)
