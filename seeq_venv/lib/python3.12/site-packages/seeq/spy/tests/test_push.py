import concurrent.futures
import datetime
import os
import random
import re
import sys
import time
import unittest
import uuid
from unittest import mock

import numpy as np
import pandas as pd
import pytest

from seeq import spy
from seeq.base.seeq_names import SeeqNames
from seeq.sdk import *
from seeq.spy import _common, _push, _url, Status
from seeq.spy._errors import *
from seeq.spy.tests import test_common
from seeq.spy.tests.test_common import Sessions
from seeq.spy.workbooks import _folder


def setup_module():
    test_common.initialize_sessions()


def _pull_workbook(_id, include_inventory=True, session=None):
    return spy.workbooks.pull(pd.DataFrame([{
        'ID': _common.sanitize_guid(_id),
        'Type': 'Workbook',
        'Workbook Type': 'Analysis'
    }]), include_inventory=include_inventory, session=session)[0]


@pytest.mark.system
def test_push_to_workbook():
    folder_name = f'test_push_to_workbook_{_common.new_placeholder_guid()}'

    numeric_data_df = pd.DataFrame()
    numeric_data_df['test_push_to_workbook_with_data'] = \
        pd.Series([3, 4], index=[pd.to_datetime('2019-01-01T00:00:00Z'), pd.to_datetime('2019-01-03T00:00:00Z')])

    signal_df = spy.push(numeric_data_df,
                         workbook=f'{folder_name} >> test_push_to_workbook >> My Workbook!', worksheet='My Worksheet!')

    search_df = spy.workbooks.search({'Path': f'{folder_name} >> test_push_to_workbook'})

    assert len(search_df) == 1
    assert search_df.iloc[0]['Name'] == 'My Workbook!'

    workbooks = spy.workbooks.pull(search_df, include_inventory=False)

    assert len(workbooks) == 1
    workbook = workbooks[0]
    assert workbook['Name'] == 'My Workbook!'
    assert len(workbook.worksheets) == 1
    assert workbook.worksheets[0].name == 'My Worksheet!'
    assert workbook.path == f'{folder_name} >> test_push_to_workbook'

    # Now that we have a workbook, test that _determine_primary_worksheet functions correctly
    worksheet = _push._determine_primary_worksheet(spy.session, Status(errors='raise'), workbook.id)
    assert worksheet.name == 'My Worksheet!'

    # CRAB-29456 Ensure SPy can push to workbook with 'Asset Group Editor' view open
    workbook.worksheets[0].view = 'Asset Group Editor'

    # Make sure the signal is scoped to the workbook
    signals_api = SignalsApi(spy.session.client)
    signal_output = signals_api.get_signal(id=signal_df.iloc[0]['ID'])
    assert signal_output.scoped_to == workbook.id

    # Push again, but this time using the workbook's ID
    spy.push(metadata=pd.DataFrame([{
        'Type': 'Signal',
        'Name': 'test_push_to_workbook2'
    }]), workbook=workbook.id, worksheet='My Worksheet!')

    # CRAB-22062 ensure workbook path remains unchanged when workbook is pushed
    workbook_id = workbook.id
    workbook = _pull_workbook(workbook_id, include_inventory=False)
    assert workbook.path == f'{folder_name} >> test_push_to_workbook'

    non_admin_name = spy.user.name

    workbook_in_root_name = f'Workbook in Root {_common.new_placeholder_guid()}'

    # Now push to a workbook in the root of My Items
    spy.push(metadata=pd.DataFrame([{
        'Type': 'Signal',
        'Name': 'test_push_to_workbook3'
    }]), workbook=workbook_in_root_name, worksheet='My Root Worksheet!')

    workbook_in_root_search_df = spy.workbooks.search({'Name': workbook_in_root_name})

    workbook_in_root = _pull_workbook(workbook_in_root_search_df.iloc[0]['ID'], include_inventory=False)

    assert workbook_in_root.path == ''

    spy.push(metadata=pd.DataFrame([{
        'Type': 'Signal',
        'Name': 'test_push_to_workbook4'
    }]), workbook=workbook_in_root.id, worksheet='My Root Worksheet!')

    assert workbook_in_root.path == ''

    # Now operate as admin
    admin_session = test_common.get_session(Sessions.admin)

    workbook = _pull_workbook(workbook_id, include_inventory=False, session=admin_session)

    # It will now appear under the admin-only "Users" folder, since the admin is not the owner
    assert workbook.path == f'{spy.workbooks.USERS} >> {non_admin_name} >> {folder_name} >> test_push_to_workbook'

    # Now try to push it using the ID, which handles the case where it's in the USERS folder but has some
    # intermediate ancestors
    spy.push(pd.DataFrame(), workbook=workbook.id, session=admin_session)

    workbook_in_root = _pull_workbook(workbook_in_root_search_df.iloc[0]['ID'], include_inventory=False,
                                      session=admin_session)

    # Now try to push this one using the ID, which handles the case where it's in the USERS folder
    spy.push(pd.DataFrame(), workbook=workbook_in_root.id, session=admin_session)

    workbook_in_root = _pull_workbook(workbook_in_root_search_df.iloc[0]['ID'], include_inventory=False,
                                      session=admin_session)

    assert workbook_in_root.path == f'{spy.workbooks.USERS} >> {non_admin_name}'

    # Now share the workbook so it would appear under Shared on the Home Screen
    items_api = ItemsApi(admin_session.client)
    everyone_group = test_common.get_group(admin_session, 'Everyone')
    items_api.add_access_control_entry(
        id=workbook_id,
        body=AceInputV1(identity_id=everyone_group.id,
                        permissions=PermissionsV1(read=True)))

    try:
        admin_session.options.compatibility = 189
        workbook = _pull_workbook(workbook_id, include_inventory=False, session=admin_session)
    finally:
        admin_session.options.compatibility = None

    # Now it will be under the admin's Shared folder
    assert workbook.path == f'{spy.workbooks.SHARED} >> {folder_name} >> test_push_to_workbook'

    with pytest.raises(RuntimeError, match=spy.workbooks.SHARED):
        spy.workbooks.push(workbook, path=spy.workbooks.SHARED, use_full_path=True, session=admin_session)

    with pytest.raises(RuntimeError, match=spy.workbooks.ALL):
        spy.workbooks.push(workbook, path=spy.workbooks.ALL, use_full_path=True, session=admin_session)

    with pytest.raises(RuntimeError, match=spy.workbooks.USERS):
        spy.workbooks.push(workbook, path=spy.workbooks.USERS, use_full_path=True, session=admin_session)

    spy.workbooks.push(workbook, path=spy.workbooks.CORPORATE, use_full_path=True, session=admin_session)
    workbook = _pull_workbook(workbook_id, include_inventory=False, session=admin_session)

    # Now it will be under the Corporate folder
    assert workbook.path == f'{spy.workbooks.CORPORATE} >> {folder_name} >> test_push_to_workbook'

    # If the user doesn't have permission to the Folder the Workbook is contained within, spy.push and
    # spy.workbooks.push should succeed when errors='catalog' is used and fail if not.
    non_admin_session = test_common.get_session(Sessions.nonadmin)
    assert len(workbook['Ancestors']) > 1
    folder_id = workbook['Ancestors'][-1]
    assert _common.is_guid(folder_id)
    items_api.set_acl(id=folder_id, body=AclInputV1(entries=[], disable_permission_inheritance=True))
    items_api.add_access_control_entry(id=workbook_id,
                                       body=AceInputV1(identity_id=non_admin_session.user.id,
                                                       permissions=PermissionsV1(read=True, write=True, manage=True)))
    with pytest.raises(SPyRuntimeError, match='.*does not have access for the Folder.*'):
        spy.workbooks.push(workbook, session=non_admin_session)
    spy.workbooks.push(workbook, errors='catalog', session=non_admin_session)
    with pytest.raises(SPyRuntimeError, match='.*does not have access for the Folder.*'):
        spy.push(metadata=pd.DataFrame([{
            'Type': 'Signal', 'Name': 'test_push_to_workbook_unshared_folder', 'Formula': 'sinusoid()'
        }]), workbook=workbook_id, session=non_admin_session)
    spy.push(metadata=pd.DataFrame([{
        'Type': 'Signal', 'Name': 'test_push_to_workbook_unshared_folder', 'Formula': 'sinusoid()'
    }]), workbook=workbook_id, errors='catalog', session=non_admin_session)

    # Pull & Push should still work as a non-admin when the workbook is shared directly (not within a folder) CRAB-32827
    non_admin_session = test_common.get_session(Sessions.nonadmin)
    shared_workbook_name = f'shared_without_folder_{_common.new_placeholder_guid()}'
    shared_workbook = spy.workbooks.Analysis(shared_workbook_name)
    shared_workbook.worksheet('First')
    shared_workbook_result = spy.workbooks.push(shared_workbook, path=spy.workbooks.MY_FOLDER,
                                                session=admin_session).iloc[0]
    shared_workbook_id = shared_workbook_result['Pushed Workbook ID']
    # Pushing to the workbook should fail before the user has permission to the workbook
    with pytest.raises(SPyRuntimeError, match=f'Workbook with ID "{shared_workbook_id}" not found'):
        spy.push(metadata=pd.DataFrame([{
            'Type': 'Signal',
            'Name': 'test_push_to_workbook_unshared',
            'Formula': 'sinusoid()'
        }]), workbook=shared_workbook_id, session=non_admin_session, errors='catalog')

    items_api.add_access_control_entry(
        id=shared_workbook_id,
        body=AceInputV1(identity_id=non_admin_session.user.id,
                        permissions=PermissionsV1(read=True, write=True, manage=True)))
    workbook_non_admin = spy.workbooks.pull(pd.DataFrame([{'ID': shared_workbook_id,
                                                           'Type': 'Workbook',
                                                           'Workbook Type': 'Analysis'}]),
                                            include_inventory=False,
                                            session=non_admin_session)[0]
    assert workbook_non_admin.path == spy.workbooks.SHARED
    spy.push(metadata=pd.DataFrame([{
        'Type': 'Signal',
        'Name': 'test_push_to_workbook_shared_without_folder',
        'Formula': 'sinusoid()'
    }]), workbook=shared_workbook_id, session=non_admin_session)
    spy.workbooks.push(shared_workbook, session=non_admin_session)


# CRAB-22132: Push to Workbook without specifying worksheet
@pytest.mark.system
def test_push_to_workbook_data_worksheet_none():
    # Test Scenario: Push data to new workbook, Worksheet=None
    workbook_name = f'test_push_to_workbook_data_worksheet_none_{_common.new_placeholder_guid()}'

    numeric_data_df = pd.DataFrame()
    numeric_data_df['Number'] = pd.Series([
        1,
        2
    ], index=[
        pd.to_datetime('2019-01-01T00:00:00Z'),
        pd.to_datetime('2019-01-02T00:00:00Z')
    ])

    spy.push(numeric_data_df, workbook=f'My Folder >> {workbook_name}', worksheet=None)

    search_df = spy.workbooks.search({'Path': 'My Folder', 'Name': workbook_name})
    assert len(search_df) == 1
    assert search_df.iloc[0]['Name'] == workbook_name

    workbooks = spy.workbooks.pull(search_df, include_inventory=False)
    assert len(workbooks) == 1
    workbook = workbooks[0]
    assert len(workbook.worksheets) == 1
    assert workbook.worksheets[0].name == _common.DEFAULT_WORKSHEET_NAME

    # There should be no display items because Worksheet=None prevents any updates to trendview
    assert len(workbook.worksheets[0].display_items) == 0

    # Test Scenario: Push data to existing workbook, Worksheet=None
    # Start by populating trendview with display items so just use same data as above to create a display item
    spy.push(numeric_data_df, workbook=f'My Folder >> {workbook_name}')
    workbook = _pull_workbook(workbook.id, include_inventory=False)
    assert len(workbook.worksheets) == 1

    # There should be one display items because Worksheet=None was removed
    assert len(workbook.worksheets[0].display_items) == 1

    # Now create another signal
    numeric_data_df = pd.DataFrame()
    numeric_data_df['New Number'] = pd.Series([
        3,
        4
    ], index=[
        pd.to_datetime('2019-01-01T00:00:00Z'),
        pd.to_datetime('2019-01-02T00:00:00Z')
    ])

    spy.push(numeric_data_df, workbook=f'My Folder >> {workbook_name}', worksheet=None)

    workbook = _pull_workbook(workbook.id, include_inventory=False)

    assert len(workbook.worksheets) == 1
    assert workbook.worksheets[0].name == _common.DEFAULT_WORKSHEET_NAME

    # There should be one display items because Worksheet=None prevents any updates to trendview
    assert len(workbook.worksheets[0].display_items) == 1

    # When `worksheet=None` is specified, verify that we do not fully pull any worksheets
    def pull_worksheet_mock(self, session, worksheet_id, extra_workstep_tuples=None, include_images=True,
                            errors='raise', status=None):
        raise Exception('This function should not be pulling a worksheet')

    with unittest.mock.patch('seeq.spy.workbooks._worksheet.Worksheet.pull_worksheet', new=pull_worksheet_mock):
        spy.push(metadata=pd.DataFrame([{
            'Type': 'Signal',
            'Name': 'test_push_to_workbook_data_worksheet_none_signal',
            'Formula': 'sinusoid()'
        }]), workbook=workbook.id, worksheet=None)


# CRAB-22132: Push to Workbook without specifying worksheet
@pytest.mark.system
def test_push_to_workbook_metadata_worksheet_none():
    # Test Scenario: Push metadata to new workbook, Worksheet=None
    folder_name = f'test_push_to_workbook_metadata_worksheet_none_{_common.new_placeholder_guid()}'

    spy.push(metadata=pd.DataFrame([{
        'Type': 'Signal',
        'Name': 'test_push_to_workbook_metadata_worksheet_none'
    }]), workbook=f'{folder_name} >> test_push_to_workbook_metadata_worksheet_none >> My Workbook!', worksheet=None)

    search_df = spy.workbooks.search({'Path': f'{folder_name} >> test_push_to_workbook_metadata_worksheet_none'})
    assert len(search_df) == 1
    assert search_df.iloc[0]['Name'] == 'My Workbook!'

    workbooks = spy.workbooks.pull(search_df, include_inventory=False)
    assert len(workbooks) == 1
    workbook = workbooks[0]
    assert workbook['Name'] == 'My Workbook!'
    assert len(workbook.worksheets) == 1
    assert workbook.path == f'{folder_name} >> test_push_to_workbook_metadata_worksheet_none'
    assert workbook.worksheets[0].name == _common.DEFAULT_WORKSHEET_NAME

    # There should be no display items because Worksheet=None prevents any updates to trendview
    assert len(workbook.worksheets[0].display_items) == 0

    # Test Scenario: Push metadata to existing workbook, Worksheet=None
    # Start by populating trendview with display items so just use same data as above to create a display item
    spy.push(metadata=pd.DataFrame([{
        'Type': 'Signal',
        'Name': 'test_push_to_workbook_metadata_worksheet_none'
    }]), workbook=f'{folder_name} >> test_push_to_workbook_metadata_worksheet_none >> My Workbook!')
    workbook = _pull_workbook(workbook.id, include_inventory=False)
    assert len(workbook.worksheets) == 1

    # There should be one display items because Worksheet=None was removed
    assert len(workbook.worksheets[0].display_items) == 1

    spy.push(metadata=pd.DataFrame([{
        'Type': 'Signal',
        'Name': 'new_test_push_to_workbook_metadata_worksheet_none'
    }]), workbook=f'{folder_name} >> test_push_to_workbook_metadata_worksheet_none >> My Workbook!', worksheet=None)

    workbook = _pull_workbook(workbook.id, include_inventory=False)

    assert len(workbook.worksheets) == 1
    assert workbook.worksheets[0].name == _common.DEFAULT_WORKSHEET_NAME

    # There should be one display items because Worksheet=None prevents any updates to trendview
    assert len(workbook.worksheets[0].display_items) == 1


@pytest.mark.system
def test_push_to_workbook_without_path_makes_different_workbooks():
    folder_name = f'test_push_to_workbook_without_path_makes_different_workbooks_{_common.new_placeholder_guid()}'
    workbook_name = f'test_push_to_workbook_without_path_makes_different_workbooks_{_common.new_placeholder_guid()}'
    worksheet_name = f'test_push_to_workbook_without_path_makes_different_workbooks_{_common.new_placeholder_guid()}'
    workbook_path = f'{folder_name} >> {workbook_name}'
    # Setup: Create a folder, a workbook, and a worksheet with a signal
    signal_1 = pd.DataFrame([{'Name': 'CRAB-46227 Signal 1', 'Formula': 'sinusoid()'}])
    push_results_1 = spy.push(metadata=signal_1, workbook=workbook_path, worksheet=worksheet_name)

    # Pushing another signal to the same workbook name, but without folder path should make a different workbook
    signal_2 = pd.DataFrame([{'Name': 'CRAB-46227 Signal 2', 'Formula': 'sinusoid()'}])
    push_results_2 = spy.push(metadata=signal_2, workbook=workbook_name, worksheet=None)
    assert push_results_1.spy.workbook_id != push_results_2.spy.workbook_id

    # The original workbook should be unchanged
    workbook = spy.workbooks.pull(push_results_1.spy.workbook_id)[0]
    assert workbook.path == folder_name
    assert workbook.name == workbook_name
    assert len(workbook.worksheets) == 1
    assert workbook.worksheets[0].name == worksheet_name
    assert len(workbook.worksheets[0].display_items) == 1
    assert workbook.worksheets[0].display_items.iloc[0]['Name'] == 'CRAB-46227 Signal 1'
    assert workbook.item_inventory_df()['Name'].str.contains('CRAB-46227 Signal 1').any()
    assert not workbook.item_inventory_df()['Name'].str.contains('CRAB-46227 Signal 2').any()

    # The second workbook should be a default workbook in the root folder
    workbook = spy.workbooks.pull(push_results_2.spy.workbook_id)[0]
    assert workbook.path == ''
    assert workbook.name == workbook_name
    assert len(workbook.worksheets) == 1
    assert workbook.worksheets[0].name == 'From Data Lab'
    assert len(workbook.worksheets[0].display_items) == 0
    assert not workbook.item_inventory_df()['Name'].str.contains('CRAB-46227 Signal 1').any()
    assert workbook.item_inventory_df()['Name'].str.contains('CRAB-46227 Signal 2').any()


@pytest.mark.system
def test_push_to_existing_worksheet():
    workbooks_api = WorkbooksApi(spy.session.client)
    workbook_input = WorkbookInputV1()
    workbook_input.name = 'test_push_to_existing_worksheet'
    workbook_output = workbooks_api.create_workbook(body=workbook_input)
    worksheet_input = WorksheetInputV1()
    worksheet_input.name = 'auto-created-worksheet'
    worksheet_output = workbooks_api.create_worksheet(workbook_id=workbook_output.id, body=worksheet_input)
    new_annotation = AnnotationInputV1()
    new_annotation.document = ''
    new_annotation.name = 'auto-created-document'
    new_annotation.interests = [{'interestId': worksheet_output.id}]
    annotations_api = AnnotationsApi(spy.session.client)
    annotations_api.create_annotation(body=new_annotation)

    spy.push(pd.DataFrame({'My Data': [1]}, index=[pd.to_datetime('2019-01-01')]),
             workbook=workbook_output.id, worksheet=worksheet_input.name)

    search_df = spy.workbooks.search({'ID': workbook_output.id})
    workbooks = spy.workbooks.pull(search_df)
    assert len(workbooks) == 1
    workbook = workbooks[0]
    assert workbook.id == workbook_output.id
    assert len(workbook.worksheets) == 1
    worksheet = workbook.worksheets[0]
    assert worksheet.id == worksheet_output.id
    assert worksheet.name == worksheet_output.name


@pytest.mark.system
def test_current_worksteps_crab_21217():
    # Create a workbook with five signals in the details pane
    workbook_name = 'test_current_worksteps_CRAB_21217'
    worksheet_name = '1'
    signals = map(lambda i: {'Name': f'Signal {i}', 'Type': 'Signal'}, range(1, 6))
    spy.push(metadata=pd.DataFrame(signals),
             workbook=workbook_name, worksheet=worksheet_name)
    workbook_id = spy.workbooks.search({'Name': workbook_name})['ID'][0]
    workbook = _pull_workbook(workbook_id)
    worksheet_id = workbook.worksheets[0].id
    workstep_id = workbook.worksheets[0].current_workstep().id

    # Add a journal entry with a link to the current workstep containing five signals
    annotations_api = AnnotationsApi(spy.session.client)
    annotation_id = annotations_api.get_annotations(annotates=[worksheet_id]).items[0].id
    document = f'''
        <p><a href="/links?type=workstep&workbook={workbook_id}&worksheet={worksheet_id}&workstep={workstep_id}">
        Workstep Link
        </a></p>
    '''
    annotation = AnnotationInputV1()
    annotation.document = document
    annotation.name = 'journal_entry_with_workstep_link'
    annotation.interests = [{'interestId': worksheet_id}]
    annotations_api.update_annotation(id=annotation_id, body=annotation)

    # Push another workstep that clears the details pane
    workstep_input = WorkstepInputV1()
    workstep_input.data = _common.DEFAULT_WORKBOOK_STATE
    workbooks_api = WorkbooksApi(spy.session.client)
    workbooks_api.create_workstep(workbook_id=workbook_id,
                                  worksheet_id=worksheet_id,
                                  body=workstep_input)

    # Now push a single signal into the details pane
    only_signal_name = 'Only Signal'
    spy.push(metadata=pd.DataFrame([{'Name': only_signal_name, 'Type': 'Signal'}]),
             workbook=workbook_name, worksheet=worksheet_name)

    # Pull the workbook and verify the details pane contains only one signal
    workbook = _pull_workbook(workbook_id)
    details_items = workbook.worksheets[0].current_workstep().data['state']['stores']['sqTrendSeriesStore']['items']
    assert len(details_items) == 1
    assert details_items[0]['name'] == only_signal_name


@pytest.mark.system
def test_push_signal():
    numeric_data_df = pd.DataFrame()
    string_data_df = pd.DataFrame()

    numeric_data_df['Numeric'] = pd.Series([
        1,
        'invalid',
        3,
        None
    ], index=[
        pd.to_datetime('2019-01-01'),
        pd.to_datetime('2019-01-02'),
        pd.to_datetime('2019-01-03'),
        pd.to_datetime('2019-01-04')
    ])

    string_data_df['String'] = pd.Series([
        'ON',
        'OFF',
        None,
        np.nan,
        np.nan
    ], index=[
        pd.to_datetime('2019-01-01'),
        pd.to_datetime('2019-01-02'),
        pd.to_datetime('2019-01-03'),
        pd.to_datetime('2019-01-04'),
        pd.to_datetime('2019-01-05')  # This timestamp won't show up in the pull
    ])

    with pytest.raises(
            RuntimeError,
            match=re.escape('Column "Numeric" was detected as numeric-valued, but string '
                            'value at (2019-01-02 00:00:00+00:00, invalid)')):
        spy.push(numeric_data_df, workbook='test_push_signal')

    with pytest.raises(
            RuntimeError,
            match=re.escape('Column "String" was detected as string-valued, but numeric '
                            'value at (2019-01-03 00:00:00+00:00, None)')):
        spy.push(string_data_df, workbook='test_push_signal')

    data_df = numeric_data_df.combine_first(string_data_df)

    push_df = spy.push(data_df, type_mismatches='invalid', workbook='test_push_signal', worksheet=None)

    try:
        spy.options.compatibility = 188
        search_df = spy.search(push_df)

        assert search_df[search_df['Name'] == 'Numeric'].iloc[0]['Value Unit Of Measure'] == ''
        assert search_df[search_df['Name'] == 'String'].iloc[0]['Value Unit Of Measure'] == 'string'
    finally:
        spy.options.compatibility = None

    pull_df = spy.pull(push_df, start='2019-01-01T00:00:00Z', end='2019-01-05T00:00:00Z', grid=None)

    test_common.make_index_naive(pull_df)

    assert len(pull_df) == 4

    assert pull_df.at[pd.to_datetime('2019-01-01'), 'Numeric'] == 1
    assert pd.isna(pull_df.at[pd.to_datetime('2019-01-02'), 'Numeric'])
    assert pull_df.at[pd.to_datetime('2019-01-03'), 'Numeric'] == 3
    assert pd.isna(pull_df.at[pd.to_datetime('2019-01-04'), 'Numeric'])

    assert pull_df.at[pd.to_datetime('2019-01-01'), 'String'] == 'ON'
    assert pull_df.at[pd.to_datetime('2019-01-02'), 'String'] == 'OFF'
    assert pd.isna(pull_df.at[pd.to_datetime('2019-01-03'), 'String'])
    assert pd.isna(pull_df.at[pd.to_datetime('2019-01-04'), 'String'])

    with pytest.raises(ValueError,
                       match=re.escape('invalid_values_as cannot be None (because Pandas treats it the same as NaN)')):
        spy.pull(push_df, start='2019-01-01T00:00:00Z', end='2019-01-05T00:00:00Z', grid=None, invalid_values_as=None)

    pull_df = spy.pull(push_df, start='2019-01-01T00:00:00Z', end='2019-01-05T00:00:00Z', grid=None,
                       invalid_values_as='INVALID')

    test_common.make_index_naive(pull_df)

    assert pull_df.at[pd.to_datetime('2019-01-01'), 'Numeric'] == 1
    assert pull_df.at[pd.to_datetime('2019-01-02'), 'Numeric'] == 'INVALID'
    assert pull_df.at[pd.to_datetime('2019-01-03'), 'Numeric'] == 3
    assert pull_df.at[pd.to_datetime('2019-01-04'), 'Numeric'] == 'INVALID'

    assert pull_df.at[pd.to_datetime('2019-01-01'), 'String'] == 'ON'
    assert pull_df.at[pd.to_datetime('2019-01-02'), 'String'] == 'OFF'
    assert pull_df.at[pd.to_datetime('2019-01-03'), 'String'] == 'INVALID'
    assert pd.isna(pull_df.at[pd.to_datetime('2019-01-04'), 'String'])

    pull_df = spy.pull(push_df, start='2019-01-01T00:00:00Z', end='2019-01-05T00:00:00Z', grid=None,
                       invalid_values_as=-999)

    test_common.make_index_naive(pull_df)

    assert pull_df.at[pd.to_datetime('2019-01-01'), 'Numeric'] == 1
    assert pull_df.at[pd.to_datetime('2019-01-02'), 'Numeric'] == -999
    assert pull_df.at[pd.to_datetime('2019-01-03'), 'Numeric'] == 3
    assert pull_df.at[pd.to_datetime('2019-01-04'), 'Numeric'] == -999

    assert pull_df.at[pd.to_datetime('2019-01-01'), 'String'] == 'ON'
    assert pull_df.at[pd.to_datetime('2019-01-02'), 'String'] == 'OFF'
    assert pull_df.at[pd.to_datetime('2019-01-03'), 'String'] == -999
    assert pd.isna(pull_df.at[pd.to_datetime('2019-01-04'), 'String'])


@pytest.mark.system
def test_push_signal_time_index_variations():
    test_name = 'test_push_signal_timezone_variations'

    data_df = pd.DataFrame([
        {'Time': '2019-01-01', 'Signal 1': 1},
        {'Time': '2019-01-02', 'Signal 1': 2},
        {'Time': '2019-01-03', 'Signal 1': 3}
    ])

    with pytest.raises(SPyRuntimeError, match='data index must be a pd.DatetimeIndex'):
        spy.push(data_df, workbook=test_name, worksheet=None, datasource=test_name)

    data_df.set_index('Time', inplace=True)
    with pytest.raises(SPyRuntimeError, match='data index must be a pd.DatetimeIndex'):
        spy.push(data_df, workbook=test_name, worksheet=None, datasource=test_name)

    data_df.index = pd.DatetimeIndex(data_df.index)

    pushed_df = spy.push(data_df, workbook=test_name, worksheet=None, datasource=test_name)
    pulled_df = spy.pull(pushed_df, start='2018-01-01', end='2019-01-05', grid=None)
    assert pulled_df.index.to_list() == [
        pd.Timestamp('2019-01-01T00:00:00.000Z'),
        pd.Timestamp('2019-01-02T00:00:00.000Z'),
        pd.Timestamp('2019-01-03T00:00:00.000Z')
    ]

    old_default_timezone = spy.options.default_timezone

    try:
        spy.options.default_timezone = 'Etc/GMT-7'
        data_df.rename(columns={'Signal 1': 'Signal 2'}, inplace=True)
        pushed_df = spy.push(data_df, workbook=test_name, worksheet=None, datasource=test_name)
        pulled_df = spy.pull(pushed_df, start='2018-01-01', end='2019-01-05', grid=None)
        assert pulled_df.index.to_list() == [
            pd.Timestamp('2019-01-01T00:00:00.000+0700'),
            pd.Timestamp('2019-01-02T00:00:00.000+0700'),
            pd.Timestamp('2019-01-03T00:00:00.000+0700')
        ]

    finally:
        spy.options.default_timezone = old_default_timezone

    pulled_df = spy.pull(pushed_df, start='2018-01-01', end='2019-01-05', grid=None)
    assert pulled_df.index.to_list() == [
        pd.Timestamp('2018-12-31T17:00:00.000Z'),
        pd.Timestamp('2019-01-01T17:00:00.000Z'),
        pd.Timestamp('2019-01-02T17:00:00.000Z')
    ]


@pytest.mark.system
def test_push_signal_with_replace():
    numeric_data_df = pd.DataFrame()

    numeric_data_df['Numeric'] = pd.Series([
        1,
        2,
        3,
        4
    ], index=[
        pd.to_datetime('2019-01-01', utc=True),
        pd.to_datetime('2019-01-02', utc=True),
        pd.to_datetime('2019-01-03', utc=True),
        pd.to_datetime('2019-01-04', utc=True)
    ])

    push_df = spy.push(numeric_data_df, workbook='test_push_signal_with_replace', worksheet=None)

    pull_df = spy.pull(push_df, start='2019-01-01T00:00:00Z', end='2019-01-05T00:00:00Z', grid=None)

    assert len(pull_df) == 4
    assert pull_df.at[pd.to_datetime('2019-01-01', utc=True), 'Numeric'] == 1
    assert pull_df.at[pd.to_datetime('2019-01-02', utc=True), 'Numeric'] == 2
    assert pull_df.at[pd.to_datetime('2019-01-03', utc=True), 'Numeric'] == 3
    assert pull_df.at[pd.to_datetime('2019-01-04', utc=True), 'Numeric'] == 4

    numeric_data_df2 = pd.DataFrame()
    numeric_data_df2['Numeric'] = pd.Series([
        11
    ], index=[
        pd.to_datetime('2019-01-03', utc=True)
    ])

    push_df = spy.push(numeric_data_df2,
                       replace={
                           'Start': pd.to_datetime('2019-01-01', utc=True),
                           'End': pd.to_datetime('2019-01-04', utc=True)
                       },
                       workbook='test_push_signal_with_replace', worksheet=None)

    pull_df2 = spy.pull(push_df, start='2019-01-01T00:00:00Z', end='2019-01-05T00:00:00Z', grid=None)

    assert len(pull_df2) == 2
    assert pull_df2.at[pd.to_datetime('2019-01-03', utc=True), 'Numeric'] == 11
    assert pull_df2.at[pd.to_datetime('2019-01-04', utc=True), 'Numeric'] == 4

    push_df = spy.push(
        metadata=pd.DataFrame([{
            'Type': 'Signal',
            'Name': 'Numeric'
        }]),
        replace={
            'Start': pd.to_datetime('2019-01-01', utc=True),
            'End': pd.to_datetime('2019-01-04', utc=True)
        },
        workbook='test_push_signal_with_replace',
        worksheet=None)

    pull_df3 = spy.pull(push_df, start='2019-01-01T00:00:00Z', end='2019-01-07T00:00:00Z', grid=None)
    assert len(pull_df3) == 1
    assert pull_df3.at[pd.to_datetime('2019-01-04', utc=True), 'Numeric'] == 4


@pytest.mark.system
def test_push_signal_with_replace_pagination():
    session = test_common.get_session(Sessions.nonadmin)
    try:
        session.options.push_page_size = 100
        name = 'test_push_signal_with_replace_pagination' + _common.new_placeholder_guid()

        # Create a basic signal with many pages worth of data
        start = pd.to_datetime('2020-01-01T00:00:00Z')
        end = pd.to_datetime('2020-01-25T00:00:00Z')
        signal_data = pd.DataFrame()
        for i in range(0, 24 * 24):
            signal_data.at[pd.to_datetime('2020-01-01T01:00:00Z') + pd.to_timedelta(i, unit='h'), name] = i % 2
        signal_data = signal_data.astype({name: np.int64})

        # Push & re-pull should round-trip without losing data
        push_results = spy.push(data=signal_data,
                                workbook=name, worksheet=None, session=session)

        assert all([x == 'Success' for x in push_results.spy.status.df['Result'].tolist()])
        assert push_results.spy.status.df.iloc[0]['Pages'] == 6

        pull_df = spy.pull(push_results, start=start, end=end, grid=None, session=session)
        pull_df.index.name = 'TIME(unitless)'

        assert len(pull_df) > 500
        pull_df = pull_df.astype({name: np.int64})
        assert pull_df.equals(signal_data)

        # Push with `replace` & re-pull should give us the updated result
        signal_data[name] += 10
        push_results = spy.push(data=signal_data, replace={'Start': start, 'End': end},
                                workbook=name, worksheet=None, session=session)

        assert all([x == 'Success' for x in push_results.spy.status.df['Result'].tolist()])
        assert push_results.spy.status.df.iloc[0]['Pages'] == 6

        pull_df = spy.pull(push_results, start=start, end=end, grid=None, session=session)
        pull_df.index.name = 'TIME(unitless)'

        assert len(pull_df) > 500
        pull_df = pull_df.astype({name: np.int64})
        assert pull_df.equals(signal_data)

    finally:
        session.options.push_page_size = session.options._DEFAULT_PUSH_PAGE_SIZE


@pytest.mark.system
def test_push_condition_with_replace_pagination():
    session = test_common.get_session(Sessions.nonadmin)
    try:
        session.options.push_page_size = 100
        name = 'test_push_condition_with_replace_pagination' + _common.new_placeholder_guid()

        # Create a basic condition with many pages worth of data
        start = pd.to_datetime('2020-01-01T00:00:00Z')
        end = pd.to_datetime('2020-02-25T00:00:00Z')
        condition_data = pd.DataFrame()
        for i in range(0, 24 * 24):
            cap_start = pd.to_datetime('2020-01-01T01:00:00Z') + pd.to_timedelta(i, unit='h')
            condition_data.at[i, 'Capsule Start'] = cap_start
            condition_data.at[i, 'Capsule End'] = cap_start + pd.to_timedelta(1, unit='min')

        # Push & re-pull should round-trip without losing data
        condition_metadata = pd.DataFrame([{
            'Name': name,
            'Type': 'Condition',
            'Maximum Duration': '1 hour'
        }])

        push_results = spy.push(data=condition_data, metadata=condition_metadata,
                                workbook=name, worksheet=None, session=session)

        assert all([x == 'Success' for x in push_results.spy.status.df['Result'].tolist()])
        assert push_results.spy.status.df.iloc[0]['Pages'] == 6

        pull_df = spy.pull(push_results, start=start, end=end, grid=None, session=session)
        assert len(pull_df) == len(condition_data)
        # All capsules should end at the specified XX:01:00
        assert all([cap_end_min == 1 for cap_end_min in pull_df['Capsule End'].dt.minute.tolist()])

        # Push with `replace` & re-pull should give us the same result
        condition_data['Capsule End'] += pd.to_timedelta(10, unit='min')
        push_results = spy.push(data=condition_data, metadata=condition_metadata, replace={'Start': start, 'End': end},
                                workbook=name, worksheet=None, session=session)

        assert all([x == 'Success' for x in push_results.spy.status.df['Result'].tolist()])
        assert push_results.spy.status.df.iloc[0]['Pages'] == 6

        pull_df = spy.pull(push_results, start=start, end=end, grid=None, session=session)
        assert len(pull_df) == len(condition_data)
        # All capsules should end at the updated XX:11:00
        assert all([cap_end_min == 11 for cap_end_min in pull_df['Capsule End'].dt.minute.tolist()])

    finally:
        session.options.push_page_size = session.options._DEFAULT_PUSH_PAGE_SIZE


@pytest.mark.system
def test_delete_samples_and_capsules():
    signal_data = pd.DataFrame(index=[
        pd.to_datetime('2019-01-01', utc=True),
        pd.to_datetime('2019-01-02', utc=True),
        pd.to_datetime('2019-01-03', utc=True),
        pd.to_datetime('2019-01-04', utc=True)
    ])
    signal_data['My Signal'] = [1, 2, 3, 4]

    signal_push_df = spy.push(signal_data, workbook='test_delete_samples_and_capsules', worksheet=None)

    condition_data = pd.DataFrame([{
        'Capsule Start': pd.to_datetime('2019-01-01', utc=True),
        'Capsule End': pd.to_datetime('2019-01-02', utc=True),
    }, {
        'Capsule Start': pd.to_datetime('2019-01-03', utc=True),
        'Capsule End': pd.to_datetime('2019-01-04', utc=True),
    }])
    condition_metadata = pd.DataFrame([{
        'Name': 'My Condition',
        'Type': 'Condition',
        'Maximum Duration': '2 days'
    }])

    condition_push_df = spy.push(condition_data, metadata=condition_metadata,
                                 workbook='test_delete_samples_and_capsules', worksheet=None)

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

    condition_push_df.drop(columns=[col for col in condition_push_df.columns if 'Push' in col], inplace=True)
    spy.push(metadata=pd.concat([signal_push_df, condition_push_df]),
             workbook='test_delete_samples_and_capsules', worksheet=None,
             replace={
                 'Start': pd.to_datetime('2019-01-01', utc=True),
                 'End': pd.to_datetime('2019-01-03', utc=True)}
             )

    pull_signal_df2 = spy.pull(signal_push_df, start='2019-01-01T00:00:00Z', end='2019-01-05T00:00:00Z', grid=None)

    assert len(pull_signal_df2) == 2
    assert pull_signal_df2.at[pd.to_datetime('2019-01-03', utc=True), 'My Signal'] == 3
    assert pull_signal_df2.at[pd.to_datetime('2019-01-04', utc=True), 'My Signal'] == 4

    pull_condition_df2 = spy.pull(condition_push_df, start='2019-01-01T00:00:00Z', end='2019-01-05T00:00:00Z',
                                  grid=None)

    assert len(pull_condition_df2) == 1
    assert pull_condition_df2.at[0, 'Capsule Start'] == pd.to_datetime('2019-01-03', utc=True)
    assert pull_condition_df2.at[0, 'Capsule End'] == pd.to_datetime('2019-01-04', utc=True)


@pytest.mark.system
def test_crab_19638():
    datasources_api = DatasourcesApi(spy.session.client)
    signals_api = SignalsApi(spy.session.client)

    datasource_1_input = DatasourceInputV1()
    datasource_1_input.name = 'datasource_name_1'
    datasource_1_input.datasource_class = 'datasource_class'
    datasource_1_input.datasource_id = 'datasource_id_1'
    datasource_1_input.stored_in_seeq = True
    datasource_1_output = datasources_api.create_datasource(body=datasource_1_input)  # type: DatasourceOutputV1

    datasource_2_input = DatasourceInputV1()
    datasource_2_input.name = 'datasource_name_2'
    datasource_2_input.datasource_class = 'datasource_class'
    datasource_2_input.datasource_id = 'datasource_id_2'
    datasource_2_input.stored_in_seeq = True
    datasource_2_output = datasources_api.create_datasource(body=datasource_2_input)  # type: DatasourceOutputV1

    signal_1_input = SignalInputV1()
    signal_1_input.name = 'bad_signal'
    signal_1_output = signals_api.put_signal_by_data_id(datasource_class=datasource_1_output.datasource_class,
                                                        datasource_id=datasource_1_output.datasource_id,
                                                        data_id='bad_signal',
                                                        body=signal_1_input)  # type: SignalOutputV1

    add_samples_1_input = SamplesInputV1()
    add_samples_1_input.samples = [
        SampleInputV1(key='2020-04-05T00:00:00Z', value=1)
    ]
    signals_api.add_samples(id=signal_1_output.id,
                            body=add_samples_1_input)  # type: SamplesOutputV1

    get_samples_1_output = signals_api.get_samples(id=signal_1_output.id,
                                                   start='2020-04-04T00:00:00Z',
                                                   end='2020-04-07T00:00:00Z')  # type: GetSamplesOutputV1

    assert len(get_samples_1_output.samples) == 1
    assert get_samples_1_output.samples[0].key == '2020-04-05T00:00:00Z'
    assert get_samples_1_output.samples[0].value == 1

    signal_2_input = SignalInputV1()
    signal_2_input.name = 'bad_signal'
    signal_2_output = signals_api.put_signal_by_data_id(datasource_class=datasource_2_output.datasource_class,
                                                        datasource_id=datasource_2_output.datasource_id,
                                                        data_id='bad_signal',
                                                        body=signal_2_input)  # type: SignalOutputV1

    add_samples_2_input = SamplesInputV1()
    add_samples_2_input.samples = [
        SampleInputV1(key='2020-04-06T00:00:00Z', value=2)
    ]
    signals_api.add_samples(id=signal_2_output.id,
                            body=add_samples_2_input)  # type: SamplesOutputV1

    get_samples_2_output = signals_api.get_samples(id=signal_2_output.id,
                                                   start='2020-04-04T00:00:00Z',
                                                   end='2020-04-07T00:00:00Z')  # type: GetSamplesOutputV1

    # Prior to CRAB-19638 getting fixed, this assertion used to fail because samples had size 2
    assert len(get_samples_2_output.samples) == 1
    assert get_samples_2_output.samples[0].key == '2020-04-06T00:00:00Z'
    assert get_samples_2_output.samples[0].value == 2


@pytest.mark.system
def test_push_to_existing_signal():
    # First create a signal that ends up in the "default" Datasource (which currently is PostgresDatums)
    signal_input = SignalInputV1()
    signal_input.name = 'test_push_to_existing_signal'
    signal_input.interpolation_method = 'linear'

    signals_api = SignalsApi(spy.session.client)
    signal_output = signals_api.create_signal(body=signal_input)  # type: SignalOutputV1

    search_df = spy.search({
        'ID': signal_output.id
    })

    data_df = pd.DataFrame()

    data_df[signal_output.id] = pd.Series([
        1,
        2
    ], index=[
        pd.to_datetime('2019-01-01T00:00:00Z'),
        pd.to_datetime('2019-01-02T00:00:00Z')
    ])

    # Now we push data to the signal we created at the beginning. We do not want a new signal to be created.
    push_df = spy.push(data=data_df, workbook='test_push_to_existing_signal', worksheet=None)
    assert push_df.at[signal_output.id, 'Push Count'] == 2

    pull_df = spy.pull(search_df, start='2019-01-01T00:00:00Z', end='2020-01-01T00:00:00Z', grid=None, header='ID')

    assert len(pull_df) == 2
    assert pull_df.equals(data_df)


@pytest.mark.system
def test_push_to_non_standard_datasource():
    data_1_df = pd.DataFrame()

    # Once CRAB-19638 is fixed, change this to test_push_to_non_standard_datasource
    data_1_df['test_push_to_non_standard_datasource_1'] = pd.Series([
        1,
    ], index=[
        pd.to_datetime('2019-01-01T00:00:00Z')
    ])

    workbook = 'test_push_to_non_standard_datasource'
    push_1_df = spy.push(data_1_df, workbook=workbook, worksheet=None, datasource='non_standard_datasource_1')

    assert push_1_df.spy.datasource.datasource_class == 'Seeq Data Lab'
    assert push_1_df.spy.datasource.name == 'non_standard_datasource_1'

    data_2_df = pd.DataFrame()

    # Once CRAB-19638 is fixed, change this to test_push_to_non_standard_datasource
    data_2_df['test_push_to_non_standard_datasource_2'] = pd.Series([
        2,
    ], index=[
        pd.to_datetime('2019-01-02T00:00:00Z')
    ])

    push_2_df = spy.push(data_2_df, workbook=workbook, worksheet=None, datasource='non_standard_datasource_2')

    assert push_2_df.spy.datasource.name == 'non_standard_datasource_2'

    assert len(push_1_df) > 0
    assert len(push_2_df) > 0

    pull_df = spy.pull(push_1_df, start='2019-01-01T00:00:00Z', end='2019-01-02T00:00:00Z', grid=None)
    assert len(pull_df) == 1
    assert pull_df['test_push_to_non_standard_datasource_1'][pd.to_datetime('2019-01-01T00:00:00Z')] == 1

    pull_df = spy.pull(push_2_df, start='2019-01-01T00:00:00Z', end='2019-01-02T00:00:00Z', grid=None)
    assert len(pull_df) == 1
    assert pull_df['test_push_to_non_standard_datasource_2'][pd.to_datetime('2019-01-02T00:00:00Z')] == 2


@pytest.mark.system
def test_push_from_csv():
    session = test_common.get_session(Sessions.test_push_from_csv)

    csv_file = pd.read_csv(
        os.path.join(os.path.dirname(__file__), '..', 'docs', 'Documentation', 'Support Files',
                     'csv_import_example.csv'),
        parse_dates=['TIME(unitless)'],
        index_col='TIME(unitless)')

    csv_file.index = csv_file.index.tz_convert('UTC-06:00')

    session.options.push_page_size = 5000
    session.options.max_concurrent_requests = 2

    fewer_signals = csv_file.iloc[:, :-4]

    push_results = spy.push(data=fewer_signals, workbook='test_push_from_csv', worksheet=None, session=session)

    assert all([x == 'Success' for x in push_results.spy.status.df['Result'].tolist()])
    assert push_results.spy.status.df.iloc[0]['Pages'] == 3

    start = pd.to_datetime('2018-07-25T23:31:01.0000000-06:00')
    end = pd.to_datetime('2018-07-25T23:31:07.0000000-06:00')
    expected_df = fewer_signals.loc[start:end]

    pull_df = spy.pull(push_results, start=start, end=end, grid=None, tz_convert='UTC-06:00',
                       session=session)
    pull_df.index.name = 'TIME(unitless)'

    assert pull_df.equals(expected_df)


@pytest.mark.system
def test_bad_calculation():
    with pytest.raises(RuntimeError):
        spy.push(metadata=pd.DataFrame([{
            'Type': 'Signal',
            'Name': 'Bad Calc',
            'Formula': 'hey(nothing)'
        }]), workbook='test_bad_calculation', worksheet=None)


@pytest.mark.system
def test_push_calculated_signal():
    area_a_signals = spy.search({
        'Path': 'Example >> Cooling Tower 1 >> Area A'
    })

    push_df = spy.push(metadata=pd.DataFrame([{
        'Type': 'Signal',
        'Name': 'Dew Point',
        # From https://iridl.ldeo.columbia.edu/dochelp/QA/Basic/dewpoint.html
        'Formula': "$T - ((100 - $RH.setUnits(''))/5)",
        'Formula Parameters': {
            '$T': area_a_signals[area_a_signals['Name'] == 'Temperature'],
            '$RH': area_a_signals[area_a_signals['Name'] == 'Relative Humidity']
        }
    }]), workbook='test_push_calculated_signal', worksheet=None)

    assert len(push_df) == 1
    dew_point_calc = push_df.iloc[0]
    assert 'ID' in dew_point_calc

    assert dew_point_calc['Datasource Class'] == _common.DEFAULT_DATASOURCE_CLASS
    assert dew_point_calc['Datasource ID'] == _common.DEFAULT_DATASOURCE_ID

    # Make sure Everyone got Manage permissions on the datasource
    items_api = ItemsApi(spy.session.client)
    acl_output = items_api.get_access_control(id=dew_point_calc['ID'])  # type: AclOutputV1
    everyone_entries = [ace for ace in acl_output.entries if ace.identity.name == 'Everyone']

    assert len(everyone_entries) == 1
    assert everyone_entries[0].permissions.manage
    assert everyone_entries[0].permissions.read
    assert everyone_entries[0].permissions.write


@pytest.mark.system
def test_push_calculated_signal_without_specifying_type():
    area_a_signals = spy.search({
        'Path': 'Example >> Cooling Tower 1 >> Area A'
    })

    push_df = spy.push(metadata=pd.DataFrame([{
        'Type': '',
        'Name': 'Dew Point',
        # From https://iridl.ldeo.columbia.edu/dochelp/QA/Basic/dewpoint.html
        'Formula': "$T - ((100 - $RH.setUnits(''))/5)",
        'Formula Parameters': {
            '$T': area_a_signals[area_a_signals['Name'] == 'Temperature'],
            '$RH': area_a_signals[area_a_signals['Name'] == 'Relative Humidity']
        }
    }]), workbook='test_push_calculated_signal_without_specifying_type', worksheet=None)

    assert len(push_df) == 1
    dew_point_calc = push_df.iloc[0]
    assert 'ID' in dew_point_calc

    assert dew_point_calc['Datasource Class'] == _common.DEFAULT_DATASOURCE_CLASS
    assert dew_point_calc['Datasource ID'] == _common.DEFAULT_DATASOURCE_ID

    # Make sure Everyone got Manage permissions on the datasource
    items_api = ItemsApi(spy.session.client)
    acl_output = items_api.get_access_control(id=dew_point_calc['ID'])  # type: AclOutputV1
    everyone_entries = [ace for ace in acl_output.entries if ace.identity.name == 'Everyone']

    assert len(everyone_entries) == 1
    assert everyone_entries[0].permissions.manage
    assert everyone_entries[0].permissions.read
    assert everyone_entries[0].permissions.write


@pytest.mark.system
def test_push_calculation_with_dependencies_in_metadata():
    metadata = [{
        'Name': 'Calc 1',
        'Formula': 'sinusoid()',
        'Path': 'Calc Parent'
    }, {
        'Name': 'Calc 2',
        'Formula': 'sinusoid()',
        'Path': 'Calc Parent'
    }]
    for i in range(3, 10):
        metadata.append({
            'Name': 'Calc %s' % i,
            'Formula': '$a + $b',
            'Path': 'Calc Parent',
            'Formula Parameters': {
                'a': 'Calc Parent >> Calc %s' % random.randrange(1, i),
                'b': 'Calc Parent >> Calc %s' % random.randrange(1, i)
            }
        })
    # Shuffle so that the dependencies are out of order
    random.shuffle(metadata)
    metadata = pd.DataFrame(metadata)

    push_df = spy.push(metadata=metadata, workbook='test_push_calculation_with_dependencies_in_metadata',
                       worksheet=None)
    assert (push_df['Push Result'] == 'Success').all()


@pytest.mark.system
def test_edit_existing_calculated_items():
    signals_api = SignalsApi(spy.session.client)
    conditions_api = ConditionsApi(spy.session.client)
    scalars_api = ScalarsApi(spy.session.client)

    area_a_signals = spy.search({
        'Path': 'Example >> Cooling Tower 1 >> Area A'
    })

    formula_parameters = [
        'RH=%s' % area_a_signals[area_a_signals['Name'] == 'Relative Humidity'].iloc[0]['ID'],
        'T=%s' % area_a_signals[area_a_signals['Name'] == 'Temperature'].iloc[0]['ID']
    ]

    # Create a signal, condition and scalar that we will later edit

    signal_input = SignalInputV1()
    signal_input.name = 'test_edit_existing_calculated_items Signal'
    signal_input.formula = "$T - ((100 - $RH.setUnits(''))/5)"
    signal_input.formula_parameters = formula_parameters
    signal_output = signals_api.create_signal(body=signal_input)  # type: SignalOutputV1

    condition_input = ConditionInputV1()
    condition_input.name = 'test_edit_existing_calculated_items Condition'
    condition_input.formula = "$T.valueSearch(isLessThan(80)).union($RH.valueSearch(isLessThan(40)))"
    condition_input.parameters = formula_parameters
    condition_output = conditions_api.create_condition(body=condition_input)  # type: ConditionOutputV1

    scalar_input = ScalarInputV1()
    scalar_input.name = 'test_edit_existing_calculated_items Scalar'
    scalar_input.formula = "$T.average(capsule('2016-12-18')) + $RH.average(capsule('2016-12-18'))"
    scalar_input.parameters = formula_parameters
    scalar_output = scalars_api.create_calculated_scalar(body=scalar_input)  # type: CalculatedItemOutputV1

    created_items = spy.search(pd.DataFrame([{'ID': signal_output.id},
                                             {'ID': condition_output.id},
                                             {'ID': scalar_output.id}]),
                               all_properties=True)

    assert created_items.iloc[0]['Formula'] == "$T - ((100 - $RH.setUnits(''))/5)"
    assert sorted(created_items.iloc[0]['Formula Parameters']) == formula_parameters
    assert created_items.iloc[1]['Formula'] == "$T.valueSearch(isLessThan(80)).union($RH.valueSearch(isLessThan(40)))"
    assert sorted(created_items.iloc[1]['Formula Parameters']) == formula_parameters
    assert created_items.iloc[2]['Formula'] == "$T.average(capsule('2016-12-18')) + $RH.average(capsule('2016-12-18'))"
    assert sorted(created_items.iloc[2]['Formula Parameters']) == formula_parameters

    # Edit them by just changing values in the DataFrame, then push

    created_items.at[0, 'Formula'] = '$T + 100'
    created_items.at[1, 'Formula'] = 'weekends()'
    created_items.at[2, 'Formula'] = '10kW'

    push_df = spy.push(metadata=created_items, workbook=None)

    assert push_df.iloc[0]['ID'] == signal_output.id
    assert push_df.iloc[1]['ID'] == condition_output.id
    assert push_df.iloc[2]['ID'] == scalar_output.id

    pushed_signal = spy.search(pd.DataFrame([{'ID': signal_output.id},
                                             {'ID': condition_output.id},
                                             {'ID': scalar_output.id}]),
                               all_properties=True)

    assert pushed_signal.iloc[0]['Formula'] == '$T + 100'
    assert pushed_signal.iloc[0]['Formula Parameters'] == [formula_parameters[1]]
    assert pushed_signal.iloc[1]['Formula'] == 'weekends()'
    assert pushed_signal.iloc[1]['Formula Parameters'] == []
    assert pushed_signal.iloc[2]['Formula'] == '10kW'
    assert pushed_signal.iloc[2]['Formula Parameters'] == []


@pytest.mark.system
def test_push_signal_with_metadata():
    witsml_folder = os.path.dirname(__file__)
    witsml_file = '011_02_0.csv'
    witsml_df = pd.read_csv(os.path.join(witsml_folder, witsml_file))
    timestamp_column = witsml_df.columns[0]
    witsml_df = pd.read_csv(os.path.join(witsml_folder, witsml_file), parse_dates=[timestamp_column])
    witsml_df = witsml_df.drop(list(witsml_df.filter(regex='.*Unnamed.*')), axis=1)
    witsml_df = witsml_df.dropna(axis=1, how='all')
    witsml_df = witsml_df.set_index(timestamp_column)

    metadata = pd.DataFrame({'Header': witsml_df.columns.values})
    metadata['Type'] = 'Signal'
    metadata['Tag'] = metadata['Header'].str.extract(r'(.*)\(')
    metadata['Value Unit Of Measure'] = metadata['Header'].str.extract(r'\((.*)\)')
    metadata['File'] = witsml_file
    metadata['Well Number'] = metadata['File'].str.extract(r'(\d+)_\d+_\d+\.csv')
    metadata['Wellbore ID'] = metadata['File'].str.extract(r'\d+_(\d+)_\d+\.csv')

    metadata = metadata.set_index('Header')

    # Without a Name column, we expect the push metadata to fail
    with pytest.raises(RuntimeError):
        spy.push(data=witsml_df, metadata=metadata, workbook='test_push_signal_with_metadata', worksheet=None)

    metadata['Name'] = "Well_" + metadata['Well Number'] + "_" + "Wellbore_" + \
                       metadata['Wellbore ID'] + "_" + metadata['Tag']

    push_results_df = spy.push(data=witsml_df, metadata=metadata, workbook='test_push_signal_with_metadata',
                               worksheet=None)

    search_results_df = spy.search(push_results_df.iloc[0],
                                   workbook='test_push_signal_with_metadata')

    assert len(search_results_df) == 1
    assert search_results_df.iloc[0]['Name'] == metadata.iloc[0]['Name']
    assert 'Push Result' not in search_results_df
    assert 'Push Count' not in search_results_df
    assert 'Push Time' not in search_results_df

    pull_results_df = spy.pull(search_results_df,
                               start='2016-07-25T15:00:00.000-07:00',
                               end='2019-07-25T17:00:00.000-07:00',
                               grid=None)

    assert len(pull_results_df) == 999

    # noinspection PyUnresolvedReferences
    assert (witsml_df.index == pull_results_df.index).all()

    witsml_list = witsml_df['BITDEP(ft)'].tolist()
    pull_list = pull_results_df['Well_011_Wellbore_02_BITDEP'].tolist()
    assert witsml_list == pull_list


@pytest.mark.system
def test_push_capsules():
    capsule_data = pd.DataFrame([{
        'Capsule Start': pd.to_datetime('2019-01-10T09:00:00.000Z'),
        'Capsule End': pd.to_datetime('2019-01-10T17:00:00.000Z'),
        'Operator On Duty': 'Mark'
    }, {
        'Capsule Start': pd.to_datetime('2019-01-11T09:00:00.000Z'),
        'Capsule End': pd.to_datetime('2019-01-11T17:00:00.000Z'),
        'Operator On Duty': 'Hedwig'
    }])

    with pytest.raises(SPyRuntimeError, match='Condition requires a metadata argument, see docstring for details'):
        spy.push(data=capsule_data, workbook='test_push_capsules', worksheet=None)

    with pytest.raises(SPyRuntimeError, match='Maximum Duration'):
        spy.push(data=capsule_data,
                 metadata=pd.DataFrame([{
                     'Name': 'Push capsules test',
                     'Type': 'Condition'
                 }]),
                 workbook='test_push_capsules', worksheet=None)

    push_result = spy.push(data=capsule_data,
                           metadata=pd.DataFrame([{
                               'Name': 'Push capsules test',
                               'Type': 'Condition',
                               'Maximum Duration': '2d'
                           }]),
                           workbook='test_push_capsules', worksheet=None)

    assert len(push_result) == 1
    assert push_result.iloc[0]['Name'] == 'Push capsules test'
    assert push_result.iloc[0]['Push Count'] == 2

    pull_result = spy.pull(push_result, start='2019-01-01T09:00:00.000Z', end='2019-02-01T09:00:00.000Z')

    assert len(pull_result) == 2
    assert pull_result.iloc[0]['Condition'] == 'Push capsules test'
    assert pull_result.iloc[0]['Capsule Start'] == pd.to_datetime('2019-01-10T09:00:00.000Z')
    assert pull_result.iloc[0]['Capsule End'] == pd.to_datetime('2019-01-10T17:00:00.000Z')
    assert pull_result.iloc[0]['Operator On Duty'] == 'Mark'
    assert pull_result.iloc[1]['Condition'] == 'Push capsules test'
    assert pull_result.iloc[1]['Capsule Start'] == pd.to_datetime('2019-01-11T09:00:00.000Z')
    assert pull_result.iloc[1]['Capsule End'] == pd.to_datetime('2019-01-11T17:00:00.000Z')
    assert pull_result.iloc[1]['Operator On Duty'] == 'Hedwig'


@pytest.mark.system
def test_push_capsules_condition_column_and_single_row_metadata():
    test_name = 'test_push_capsules_condition_column_and_single_row_metadata ' + _common.new_placeholder_guid()
    item = spy.push(metadata=pd.DataFrame([{
        'Name': f'{test_name} Calculated Days',
        'Type': 'Condition',
        'Formula': 'days()'
    }]), workbook=test_name, datasource=test_name, worksheet=None)

    # This pull will include a Condition column, let's try to push the data right back to a different (stored) condition
    data = spy.pull(item, start='2020-01-01T00:00:00Z', end='2020-01-04T00:00:00Z')

    # Note that the data DataFrame has a Condition column that doesn't match the metadata's index. That's OK,
    # because we handle a one-row metadata DataFrame differently (for convenience and for compatibility with older
    # versions).
    condition = spy.push(data=data,
                         metadata=pd.DataFrame([{
                             'Name': f'{test_name} Stored Days',
                             'Type': 'Condition',
                             'Maximum Duration': '10d'
                         }]),
                         workbook=test_name, datasource=test_name, worksheet=None)

    pulled_data = spy.pull(condition, start='2020-01-01T00:00:00Z', end='2020-01-04T00:00:00Z')

    df1 = data[['Capsule Start', 'Capsule End', 'Capsule Is Uncertain', 'Day of Year', 'Day of Month',
                'Day of Week']]
    df2 = pulled_data[['Capsule Start', 'Capsule End', 'Capsule Is Uncertain', 'Day of Year', 'Day of Month',
                       'Day of Week']]
    assert df1.equals(df2)


@pytest.mark.system
def test_push_capsules_property_unit_in_metadata():
    condition_name = 'Push capsules uoms test'
    workbook = 'test_push_capsules_uom'
    capsule_data = pd.DataFrame([{
        'Capsule Start': pd.to_datetime('2019-01-10T09:00:00.000Z'),
        'Capsule End': pd.to_datetime('2019-01-10T17:00:00.000Z'),
        'Distance': 5
    }, {
        'Capsule Start': pd.to_datetime('2019-01-11T09:00:00.000Z'),
        'Capsule End': pd.to_datetime('2019-01-11T17:00:00.000Z'),
        'Distance': 8
    }])

    try:
        spy.push(data=capsule_data,
                 metadata=pd.DataFrame([{
                     'Name': condition_name,
                     'Type': 'Condition',
                     'Capsule Property Units': {'Distance': 'in'}
                 }]),
                 workbook=workbook, worksheet=None)

        assert False, 'Without a Maximum Duration, we expect the push to fail'

    except RuntimeError as e:
        assert 'Maximum Duration' in str(e)

    push_result = spy.push(data=capsule_data,
                           metadata=pd.DataFrame([{
                               'Name': condition_name,
                               'Type': 'Condition',
                               'Maximum Duration': '2d',
                               'Capsule Property Units': {'Distance': 'in'}
                           }]),
                           workbook=workbook, worksheet=None)

    assert len(push_result) == 1
    assert push_result.iloc[0]['Name'] == condition_name
    assert push_result.iloc[0]['Push Count'] == 2

    pull_result = spy.pull(push_result, start='2019-01-01T09:00:00.000Z', end='2019-02-01T09:00:00.000Z')

    assert len(pull_result) == 2
    assert pull_result.iloc[0]['Condition'] == condition_name
    assert pull_result.iloc[0]['Capsule Start'] == pd.to_datetime('2019-01-10T09:00:00.000Z')
    assert pull_result.iloc[0]['Capsule End'] == pd.to_datetime('2019-01-10T17:00:00.000Z')
    assert pull_result.iloc[0]['Distance'] == 5
    assert pull_result.iloc[1]['Condition'] == condition_name
    assert pull_result.iloc[1]['Capsule Start'] == pd.to_datetime('2019-01-11T09:00:00.000Z')
    assert pull_result.iloc[1]['Capsule End'] == pd.to_datetime('2019-01-11T17:00:00.000Z')
    assert pull_result.iloc[1]['Distance'] == 8

    search_result = spy.search(push_result, all_properties=True)

    assert search_result.iloc[0]['Metadata Properties'] == 'Distance=in'

    bad_capsule_data = pd.DataFrame([{
        'Capsule Start': pd.to_datetime('2019-01-10T09:00:00.000Z'),
        'Capsule End': pd.to_datetime('2019-01-10T17:00:00.000Z'),
        'Distance': {'Value': 9, 'Unit Of Measure': 'm'}
    }, {
        'Capsule Start': pd.to_datetime('2019-01-11T09:00:00.000Z'),
        'Capsule End': pd.to_datetime('2019-01-11T17:00:00.000Z'),
        'Distance': 8
    }])

    try:
        spy.push(data=bad_capsule_data,
                 metadata=pd.DataFrame([{
                     'Name': condition_name,
                     'Type': 'Condition',
                     'Maximum Duration': '2d',
                     'Capsule Property Units': {'Distance': 'in'}
                 }]),
                 workbook=workbook, worksheet=None)
        assert False, 'Property "Distance" cannot have type dict when unit of measure is specified in metadata'
    except SPyTypeError as e:
        assert 'Property "Distance" cannot have type dict when unit of measure is specified in metadata' in str(e)


@pytest.mark.system
def test_push_capsules_property_name_matches_metadata_case():
    condition_name = 'Push capsules uoms case insensitivity test'
    workbook = 'test_push_capsules_uom_case_insensitive'
    capsule_data = pd.DataFrame([{
        'Capsule Start': pd.to_datetime('2019-01-10T09:00:00.000Z'),
        'Capsule End': pd.to_datetime('2019-01-10T17:00:00.000Z'),
        'Distance': 5
    }, {
        'Capsule Start': pd.to_datetime('2019-01-11T09:00:00.000Z'),
        'Capsule End': pd.to_datetime('2019-01-11T17:00:00.000Z'),
        'distance': 8
    }])

    push_result = spy.push(data=capsule_data,
                           metadata=pd.DataFrame([{
                               'Name': condition_name,
                               'Type': 'Condition',
                               'Maximum Duration': '2d',
                               'Capsule Property Units': {'Distance': 'in'}
                           }]),
                           workbook=workbook, worksheet=None)

    assert len(push_result) == 1
    assert push_result.iloc[0]['Name'] == condition_name
    assert push_result.iloc[0]['Push Count'] == 2

    pull_result = spy.pull(push_result, start='2019-01-01T09:00:00.000Z', end='2019-02-01T09:00:00.000Z')

    assert len(pull_result) == 2
    assert pull_result.iloc[0]['Condition'] == condition_name
    assert pull_result.iloc[0]['Capsule Start'] == pd.to_datetime('2019-01-10T09:00:00.000Z')
    assert pull_result.iloc[0]['Capsule End'] == pd.to_datetime('2019-01-10T17:00:00.000Z')
    assert pull_result.iloc[0]['Distance'] == 5
    assert pull_result.iloc[1]['Condition'] == condition_name
    assert pull_result.iloc[1]['Capsule Start'] == pd.to_datetime('2019-01-11T09:00:00.000Z')
    assert pull_result.iloc[1]['Capsule End'] == pd.to_datetime('2019-01-11T17:00:00.000Z')
    # Lowercase name was converted to match case of the metadata. See CRAB-32505.
    assert pull_result.iloc[1]['Distance'] == 8

    search_result = spy.search(push_result, all_properties=True)

    assert search_result.iloc[0]['Metadata Properties'] == 'Distance=in'


@pytest.mark.system
def test_push_condition_only_with_capsules_property_unit_in_metadata():
    condition_name = 'Push capsule properties uoms test'
    workbook = 'test_push_capsules_uom'

    push_result = spy.push(metadata=pd.DataFrame([{
        'Name': condition_name,
        'Type': 'Condition',
        'Maximum Duration': '2d',
        'Capsule Property Units': {'Distance': 'in'}
    }]),
        workbook=workbook, worksheet=None)

    assert len(push_result) == 1
    assert push_result.iloc[0]['Name'] == condition_name

    search_result = spy.search(push_result, all_properties=True)

    assert search_result.iloc[0]['Metadata Properties'] == 'Distance=in'


@pytest.mark.system
def test_capsule_property_unit_metadata_persistence():
    condition_name = 'Push capsule properties uoms persistence test'
    workbook = 'test_push_capsules_uom_persistence'

    push_result = spy.push(metadata=pd.DataFrame([{
        'Name': condition_name,
        'Type': 'Condition',
        'Maximum Duration': '2d',
        'Capsule Property Units': {'Distance': 'in',
                                   'Mass': 'kg'}
    }]),
        workbook=workbook, worksheet=None)

    search_result = spy.search(push_result, all_properties=True)
    assert search_result.iloc[0]['Metadata Properties'] == 'Distance=in&Mass=kg'

    push_result = spy.push(metadata=pd.DataFrame([{
        'Name': condition_name,
        'Type': 'Condition',
        'Maximum Duration': '2d',
        'Capsule Property Units': {'Distance': 'in'}
    }]),
        workbook=workbook, worksheet=None)

    search_result = spy.search(push_result, all_properties=True)
    assert search_result.iloc[0]['Metadata Properties'] == 'Distance=in&Mass=kg'

    capsule_data = pd.DataFrame([{
        'Capsule Start': pd.to_datetime('2019-01-10T09:00:00.000Z'),
        'Capsule End': pd.to_datetime('2019-01-10T17:00:00.000Z'),
        'Distance': 5
    }, {
        'Capsule Start': pd.to_datetime('2019-01-11T09:00:00.000Z'),
        'Capsule End': pd.to_datetime('2019-01-11T17:00:00.000Z'),
        'Distance': 8
    }])

    push_result = spy.push(data=capsule_data, metadata=pd.DataFrame([{
        'Name': condition_name,
        'Type': 'Condition',
        'Maximum Duration': '2d',
        'Capsule Property Units': {'Distance': 'in'}
    }]), workbook=workbook, worksheet=None)

    assert len(push_result) == 1
    assert push_result.iloc[0]['Name'] == condition_name

    search_result = spy.search(push_result, all_properties=True)

    assert search_result.iloc[0]['Metadata Properties'] == 'Distance=in&Mass=kg'


@pytest.mark.system
def test_push_capsule_property_units_push_result_as_metadata():
    condition_name = 'Push capsule properties uoms double push test'
    workbook = 'Capsule push' + str(uuid.uuid4())

    push_result = spy.push(metadata=pd.DataFrame([{
        'Name': condition_name,
        'Type': 'Condition',
        'Maximum Duration': '2d',
        'Capsule Property Units': {'Distance': 'in',
                                   'Mass': 'kg'}
    }]), workbook=workbook, worksheet=None)

    capsule_data = pd.DataFrame([{
        'Capsule Start': pd.to_datetime('2019-01-10T09:00:00.000Z'),
        'Capsule End': pd.to_datetime('2019-01-10T17:00:00.000Z'),
        'Distance': 5
    }, {
        'Capsule Start': pd.to_datetime('2019-01-11T09:00:00.000Z'),
        'Capsule End': pd.to_datetime('2019-01-11T17:00:00.000Z'),
        'Distance': 8
    }])

    data_second_push = spy.push(data=capsule_data, metadata=push_result,
                                workbook=workbook, worksheet=None)

    search_result = spy.search(data_second_push, all_properties=True)
    assert search_result.iloc[0]['Metadata Properties'] == 'Distance=in&Mass=kg'

    push_result = spy.push(metadata=pd.DataFrame([{
        'Name': condition_name,
        'Type': 'Condition',
        'Maximum Duration': '2d',
        'Capsule Property Units': {'Distance': 'in',
                                   'Mass': 'kg'}
    }]),
        workbook=workbook, worksheet=None)

    push_result.at[0, 'Capsule Property Units'].pop('Distance')
    push_result.at[0, 'Capsule Property Units'].update({'Newtons': 'N'})

    no_data_second_push = spy.push(metadata=push_result,
                                   workbook=workbook, worksheet=None)

    search_result = spy.search(no_data_second_push, all_properties=True)

    # Disabled due to CRAB-30422
    # assert search_result.iloc[0]['Metadata Properties'] == 'Distance=in&Mass=kg&Newtons=N'


@pytest.mark.system
def test_push_capsules_nat_start_end():
    capsule_data = pd.DataFrame([{
        'Capsule Start': pd.to_datetime('2019-01-11T09:00:00.000Z'),
        'Capsule End': pd.to_datetime('2019-01-11T17:00:00.000Z'),
        'Operator On Duty': 'BillGates'
    }, {
        'Capsule Start': pd.to_datetime('NaN'),
        'Capsule End': pd.to_datetime('NaN'),
        'Operator On Duty': 'ElonMusk'
    }, {
        'Capsule Start': pd.to_datetime('2019-01-13T09:00:00.000Z'),
        'Capsule End': pd.to_datetime('2019-01-13T17:00:00.000Z'),
        'Operator On Duty': 'JeffBezos'
    }, ])

    push_result = spy.push(data=capsule_data,
                           metadata=pd.DataFrame([{
                               'Name': 'Capsule with NaT push test',
                               'Type': 'Condition',
                               'Maximum Duration': '2mo'
                           }]),
                           workbook='test_push_capsules_nat_start_end', worksheet=None)
    assert len(push_result) == 1
    assert push_result.iloc[0]['Push Count'] == 3

    pull_result = spy.pull(push_result, start='2019-01-01T09:00:00.000Z', end='2019-01-15T09:00:00.000Z')
    assert len(pull_result) == 2  # the capsule with NaT won't show up in this time window as it is created between now
    # and an hour ago

    pull_result = spy.pull(push_result, start='2019-01-01T09:00:00.000Z',
                           end=pd.to_datetime(datetime.datetime.utcnow()))
    assert len(pull_result) > 2  # now the capsule with NaT will show up

    assert pull_result.iloc[0]['Operator On Duty'] == 'BillGates'
    assert pull_result.iloc[1]['Operator On Duty'] == 'JeffBezos'
    assert pull_result.iloc[2]['Operator On Duty'] == 'ElonMusk'
    assert pull_result.iloc[1]['Capsule Start'] == pd.to_datetime('2019-01-13T09:00:00.000Z')
    assert pull_result.iloc[1]['Capsule End'] == pd.to_datetime('2019-01-13T17:00:00.000Z')


@pytest.mark.system
def test_push_capsules_with_interval_deletion():
    capsule_data = pd.DataFrame([{
        'Capsule Start': pd.to_datetime('2019-01-10T09:00:00.000Z'),
        'Capsule End': pd.to_datetime('2019-01-10T17:00:00.000Z'),
        'Operator On Duty': 'test1'
    }, {
        'Capsule Start': pd.to_datetime('2019-01-11T09:00:00.000Z'),
        'Capsule End': pd.to_datetime('2019-01-11T17:00:00.000Z'),
        'Operator On Duty': 'test2'
    }, {
        'Capsule Start': pd.to_datetime('2019-01-12T09:00:00.000Z'),
        'Capsule End': pd.to_datetime('2019-01-12T17:00:00.000Z'),
        'Operator On Duty': 'test3'
    }, {
        'Capsule Start': pd.to_datetime('2019-01-13T09:00:00.000Z'),
        'Capsule End': pd.to_datetime('2019-01-13T17:00:00.000Z'),
        'Operator On Duty': 'test4'
    }, {
        'Capsule Start': pd.to_datetime('2019-01-14T09:00:00.000Z'),
        'Capsule End': pd.to_datetime('2019-01-14T17:00:00.000Z'),
        'Operator On Duty': 'test5'
    }, {
        'Capsule Start': pd.to_datetime('2019-01-15T09:00:00.000Z'),
        'Capsule End': pd.to_datetime('2019-01-15T17:00:00.000Z'),
        'Operator On Duty': 'test6'
    }])

    push_result = spy.push(data=capsule_data,
                           metadata=pd.DataFrame([{
                               'Name': 'Capsule deletion',
                               'Type': 'Condition',
                               'Maximum Duration': '2d'
                           }]),
                           workbook='test_push_capsules_with_interval_deletion', worksheet=None)

    assert len(push_result) == 1
    assert push_result.iloc[0]['Name'] == 'Capsule deletion'
    assert push_result.iloc[0]['Push Count'] == 6

    pull_result = spy.pull(push_result, start='2019-01-01T09:00:00.000Z', end='2019-02-01T09:00:00.000Z')

    assert len(pull_result) == 6
    assert pull_result.iloc[0]['Condition'] == 'Capsule deletion'
    assert pull_result.iloc[0]['Capsule Start'] == pd.to_datetime('2019-01-10T09:00:00.000Z')
    assert pull_result.iloc[0]['Capsule End'] == pd.to_datetime('2019-01-10T17:00:00.000Z')
    assert pull_result.iloc[0]['Operator On Duty'] == 'test1'
    assert pull_result.iloc[5]['Condition'] == 'Capsule deletion'
    assert pull_result.iloc[5]['Capsule Start'] == pd.to_datetime('2019-01-15T09:00:00.000Z')
    assert pull_result.iloc[5]['Capsule End'] == pd.to_datetime('2019-01-15T17:00:00.000Z')
    assert pull_result.iloc[5]['Operator On Duty'] == 'test6'

    push_result = spy.push(
        metadata=pd.DataFrame([{
            'Name': 'Capsule deletion',
            'Type': 'Condition',
            'Maximum Duration': '2d'
        }]),
        replace={
            'Start': pd.to_datetime('2019-01-12T09:00:00.000Z'),
            'End': pd.to_datetime('2019-01-14T09:00:00.000Z')
        },
        workbook='test_push_capsules_with_interval_deletion', worksheet=None
    )

    pull_result = spy.pull(push_result, start='2019-01-01T09:00:00.000Z', end='2019-02-01T09:00:00.000Z')

    assert len(pull_result) == 4
    assert pull_result.iloc[2]['Condition'] == 'Capsule deletion'
    assert pull_result.iloc[2]['Capsule Start'] == pd.to_datetime('2019-01-14T09:00:00.000Z')
    assert pull_result.iloc[2]['Capsule End'] == pd.to_datetime('2019-01-14T17:00:00.000Z')
    assert pull_result.iloc[2]['Operator On Duty'] == 'test5'


@pytest.mark.system
def test_push_multiple_conditions():
    test_name = 'test_push_multiple_conditions ' + _common.new_placeholder_guid()
    capsule_data = pd.DataFrame([{
        'Condition': 'Condition 1',
        'Capsule Start': pd.to_datetime('2019-01-10T09:00:00.000Z'),
        'Capsule End': pd.to_datetime('2019-01-10T17:00:00.000Z'),
        'Operator On Duty': 'Rick'
    }, {
        'Condition': 'Condition 2',
        'Capsule Start': pd.to_datetime('2019-01-11T09:00:00.000Z'),
        'Capsule End': pd.to_datetime('2019-01-11T17:00:00.000Z'),
        'Operator On Duty': 'Morty'
    }, {
        'Condition': 'Condition 1',
        'Capsule Start': pd.to_datetime('2019-01-12T09:00:00.000Z'),
        'Capsule End': pd.to_datetime('2019-01-12T17:00:00.000Z'),
        'Operator On Duty': 'Mark'
    }, {
        'Condition': 'Condition 2',
        'Capsule Start': pd.to_datetime('2019-01-13T09:00:00.000Z'),
        'Capsule End': pd.to_datetime('2019-01-13T17:00:00.000Z'),
        'Operator On Duty': 'Hedwig'
    }, {
        'Condition': 'Condition 2',
        'Capsule Start': pd.to_datetime('2019-01-14T09:00:00.000Z'),
        'Capsule End': pd.to_datetime('2019-01-14T17:00:00.000Z'),
        'Operator On Duty': 'Angry Bill'
    }])

    metadata = pd.DataFrame([{
        'Name': 'Condition 1',
        'Type': 'Condition',
        'Maximum Duration': '2d'
    }, {
        'Name': 'Condition 2',
        'Type': 'Condition',
        'Maximum Duration': '2d'
    }]).set_index('Name', drop=False)

    # Only push the first row in the metadata
    push_result = spy.push(data=capsule_data, metadata=metadata.head(1),
                           workbook=f'{test_name}-1', worksheet=None, datasource=test_name)

    assert len(push_result) == 1
    assert push_result.iloc[0]['Name'] == 'Condition 1'
    assert push_result.iloc[0]['Push Count'] == 5
    assert push_result.spy.status.warnings == {
        'Condition metadata has only one row, but data has multiple conditions.'
    }

    pull_result = spy.pull(push_result, start='2019-01-01T09:00:00.000Z', end='2019-02-01T09:00:00.000Z')
    assert len(pull_result) == 5
    assert push_result.iloc[0]['Name'] == 'Condition 1'
    assert push_result.iloc[0]['Push Count'] == 5

    push_result = spy.push(data=capsule_data, metadata=metadata,
                           workbook=f'{test_name}-2', worksheet=None, datasource=test_name)

    assert len(push_result) == 2
    assert push_result.iloc[0]['Name'] == 'Condition 1'
    assert push_result.iloc[0]['Push Count'] == 2
    assert push_result.iloc[1]['Name'] == 'Condition 2'
    assert push_result.iloc[1]['Push Count'] == 3

    pull_result = spy.pull(push_result, start='2019-01-01T09:00:00.000Z', end='2019-02-01T09:00:00.000Z')
    assert len(pull_result) == 5
    condition_1 = pull_result[pull_result['Condition'] == 'Condition 1']
    assert len(condition_1) == 2
    assert condition_1['Capsule Start'].iloc[0] == pd.to_datetime('2019-01-10T09:00:00.000Z')
    assert condition_1['Capsule End'].iloc[0] == pd.to_datetime('2019-01-10T17:00:00.000Z')
    assert condition_1['Capsule Start'].iloc[1] == pd.to_datetime('2019-01-12T09:00:00.000Z')
    assert condition_1['Capsule End'].iloc[1] == pd.to_datetime('2019-01-12T17:00:00.000Z')
    assert condition_1['Operator On Duty'].iloc[0] == 'Rick'
    assert condition_1['Operator On Duty'].iloc[1] == 'Mark'

    condition_2 = pull_result[pull_result['Condition'] == 'Condition 2']
    assert len(condition_2) == 3
    assert condition_2['Capsule Start'].iloc[0] == pd.to_datetime('2019-01-11T09:00:00.000Z')
    assert condition_2['Capsule End'].iloc[0] == pd.to_datetime('2019-01-11T17:00:00.000Z')
    assert condition_2['Capsule Start'].iloc[1] == pd.to_datetime('2019-01-13T09:00:00.000Z')
    assert condition_2['Capsule End'].iloc[1] == pd.to_datetime('2019-01-13T17:00:00.000Z')
    assert condition_2['Capsule Start'].iloc[2] == pd.to_datetime('2019-01-14T09:00:00.000Z')
    assert condition_2['Capsule End'].iloc[2] == pd.to_datetime('2019-01-14T17:00:00.000Z')
    assert condition_2['Operator On Duty'].iloc[0] == 'Morty'
    assert condition_2['Operator On Duty'].iloc[1] == 'Hedwig'
    assert condition_2['Operator On Duty'].iloc[2] == 'Angry Bill'


@pytest.mark.system
def test_push_mixed_items():
    test_name = 'test_push_mixed_items'
    capsule_data = pd.DataFrame([{
        'Condition': 'My Condition',
        'Capsule Start': pd.to_datetime('2019-01-10T09:00:00.000Z'),
        'Capsule End': pd.to_datetime('2019-01-10T17:00:00.000Z'),
        'Money Lost': '$$$'
    }])
    sample_data = pd.DataFrame({'My Signal': [
        1,
        2,
        3
    ]}, index=[
        pd.to_datetime('2019-01-01'),
        pd.to_datetime('2019-01-02'),
        pd.to_datetime('2019-01-03')
    ])

    metadata = pd.DataFrame([{
        'Name': 'My Condition',
        'Type': 'Condition',
        'Maximum Duration': '2d'
    }, {
        'Name': 'My Signal',
        'Type': 'Signal',
        'Value Unit Of Measure': 'm/s'
    }]).set_index('Name', drop=False)

    data = {'My Condition': capsule_data, 'My Signal': sample_data}

    def _supplier(_index):
        return data[_index]

    push_result = spy.push(data=_supplier, metadata=metadata,
                           workbook=test_name, worksheet=None, datasource=test_name)

    assert len(push_result) == 2
    assert push_result.iloc[0]['Name'] == 'My Condition'
    assert push_result.iloc[0]['Push Count'] == 1
    assert push_result.iloc[1]['Name'] == 'My Signal'
    assert push_result.iloc[1]['Push Count'] == 3

    pull_result = spy.pull(push_result, start='2019-01-01T09:00:00.000Z', end='2019-02-01T09:00:00.000Z')

    assert pull_result.at[pd.to_datetime('2019-01-09T10:00:00.000Z'), 'My Condition'] == 0
    assert pull_result.at[pd.to_datetime('2019-01-10T10:00:00.000Z'), 'My Condition'] == 1
    assert pd.isna(pull_result.at[pd.to_datetime('2019-01-09T10:00:00.000Z'), 'My Condition - Money Lost'])
    assert pull_result.at[pd.to_datetime('2019-01-10T10:00:00.000Z'), 'My Condition - Money Lost'] == '$$$'
    assert pull_result.at[pd.to_datetime('2019-01-03T00:00:00.000Z'), 'My Signal'] == 3
    assert pd.isna(pull_result.at[pd.to_datetime('2019-01-03T00:15:00.000Z'), 'My Signal'])


@pytest.mark.system
def test_push_signal_metadata_with_bad_case_on_uom_property():
    # Written to address https://www.seeq.org/index.php?/forums/topic/672-handling-of-invalid-units

    date_index = pd.date_range('01/14/2020 01:00:00', periods=115, freq='h')

    samples = np.arange(0, 115)
    data = pd.DataFrame(data=samples, index=date_index, columns=['Testdataset2'])

    metadata = {
        'Name': 'Testsignal 5',
        'Type': 'Signal',
        'Maximum Interpolation': '1h',
        'Value Unit of Measure': '1/Min',
        'Interpolation Method': 'Step'
    }

    with pytest.raises(RuntimeError, match='Incorrect case'):
        spy.push(data, metadata=pd.DataFrame([metadata], index=['Testdataset2']),
                 workbook='test_push_signal_metadata_with_bad_case_on_uom_property', worksheet=None)

    del metadata['Value Unit of Measure']
    metadata['Value Unit Of Measure'] = '1/Min'

    spy.push(data, metadata=pd.DataFrame([metadata], index=['Testdataset2']),
             workbook='test_push_signal_metadata_with_bad_case_on_uom_property', worksheet=None)


@pytest.mark.system
def test_push_jump_tag_cache_disabled():
    workbook = 'test_push_jump_tag_cache_disabled'
    area_a_temp = spy.search({'Name': 'Area A_Temperature', 'Datasource Name': 'Example Data'}).at[0, 'ID']

    push_calculation = spy.push(metadata=pd.DataFrame([{
        'Name': 'My Calculation',
        'Formula': '$x + $x',
        'Formula Parameters': {'x': area_a_temp}
    }]), workbook=workbook)
    calculation_search = spy.search({'ID': push_calculation.at[0, 'ID']}, all_properties=True)
    assert calculation_search.at[0, 'Cache Enabled'] is np.True_

    push_jump_tag = spy.push(metadata=pd.DataFrame([{
        'Name': 'My Jump Tag',
        'Formula': '  $x\n',
        'Formula Parameters': {'x': area_a_temp}
    }]), workbook=workbook)
    jump_tag_search = spy.search({'ID': push_jump_tag.at[0, 'ID']}, all_properties=True)
    assert jump_tag_search.at[0, 'Cache Enabled'] is np.False_


# Disabled because of CRAB-19041
@pytest.mark.disabled
def test_push_archived_item_in_tree():
    spy.push(metadata=pd.DataFrame([{
        'Path': 'test_push_archived_item_in_tree',
        'Asset': 'The Asset',
        'Name': 'The Thing',
        'Type': 'Signal',
        'Archived': True
    }]), workbook='test_push_archived_item_in_tree', worksheet=None)

    search_df = spy.search({
        'Path': 'test_push_archived_item_in_tree'
    }, include_archived=False)

    assert len(search_df) == 1
    assert search_df.iloc[0]['Type'] == 'Asset'
    # No signal was found, only the asset -- that's good.

    search_df = spy.search({
        'Path': 'test_push_archived_item_in_tree'
    }, include_archived=True)

    # This currently fails due to CRAB-19041. The spy.search() call just above will have the @includeUnsearchable flag,
    # but for some reason "The Thing" is not returned. HOWEVER, if you push it with Archived as False, then push it
    # again with Archived as True, it gets returned properly from that point forward.
    assert len(search_df) > 1


@pytest.mark.system
def test_push_reference():
    search_df = spy.search({'Name': 'Area A_Temperature'})
    area_a_temp = search_df.squeeze()
    push_df = pd.DataFrame([
        {
            'Type': area_a_temp['Type'],
            'ID': area_a_temp['ID'],
            'Name': 'Coldness Conductivity',
            'Asset': 'Winter',
            'Path': 'Seasons',
            # _metadata._build_reference_signal will set the units to what's in the DataFrame
            'Value Unit Of Measure': 'µS/cm',
            'Reference': True
        },
        {
            'Type': area_a_temp['Type'],
            'ID': area_a_temp['ID'],
            'Name': 'Susceptance',
            'Asset': 'Winter',
            'Path': 'Seasons',
            # µS/cm is specifically stated as supported, but µS by itself is not specifically stated -- it is implied
            'Value Unit Of Measure': 'µS',
            'Reference': True
        },
        {
            'Type': area_a_temp['Type'],
            'ID': area_a_temp['ID'],
            'Name': 'Volume of Coldness',
            'Asset': 'Winter',
            'Path': 'Seasons',
            'Value Unit Of Measure': 'cm³·°F',
            'Reference': True
        },
        {
            'Type': area_a_temp['Type'],
            'ID': area_a_temp['ID'],
            'Name': 'Cold Barrels',
            'Asset': 'Winter',
            'Path': 'Seasons',
            'Value Unit Of Measure': 'bbl/mol',
            'Reference': True
        }
    ])

    push_results_df = spy.push(metadata=push_df, workbook='test_push_reference', worksheet=None)

    search_push_results_df = spy.search(push_results_df, all_properties=True)

    assert len(search_push_results_df) == 6

    coldness_conductivity = search_push_results_df[search_push_results_df['Name'] == 'Coldness Conductivity'].squeeze()
    assert coldness_conductivity['Value Unit Of Measure'] == 'µS/cm'
    assert coldness_conductivity['Referenced ID'] == area_a_temp['ID']
    assert coldness_conductivity['ID'] != area_a_temp['ID']

    susceptance = search_push_results_df[search_push_results_df['Name'] == 'Susceptance'].squeeze()
    assert susceptance['Value Unit Of Measure'] == 'µS'
    assert susceptance['Referenced ID'] == area_a_temp['ID']
    assert susceptance['ID'] != area_a_temp['ID']

    volume_of_coldness = search_push_results_df[search_push_results_df['Name'] == 'Volume of Coldness'].squeeze()
    assert volume_of_coldness['Value Unit Of Measure'] == 'cm³·°F'
    assert volume_of_coldness['Referenced ID'] == area_a_temp['ID']
    assert volume_of_coldness['ID'] != area_a_temp['ID']

    cold_barrels = search_push_results_df[search_push_results_df['Name'] == 'Cold Barrels'].squeeze()
    assert cold_barrels['Value Unit Of Measure'] == 'bbl/mol'
    assert cold_barrels['Referenced ID'] == area_a_temp['ID']
    assert cold_barrels['ID'] != area_a_temp['ID']

    # The reference items should be able to be round-tripped from the spy.search without erroring
    spy.push(metadata=search_push_results_df, workbook='test_push_reference', worksheet=None)


@pytest.mark.system
def test_crab_21092():
    workbook = 'test_crab_21092'
    worksheet = 'timezones'
    data_df = pd.DataFrame()
    data_df['String'] = pd.Series([
        1.,
        2.,
        1.4,
        1.6,
        1.8
    ], index=[
        pd.Timestamp('2019-01-01 00:00', tz='US/Central'),
        pd.Timestamp('2019-01-01 00:00', tz='US/Central'),
        pd.Timestamp('2019-01-01 00:00', tz='US/Central'),
        pd.Timestamp('2019-01-01 00:00', tz='US/Central'),
        pd.Timestamp('2019-01-01 00:00', tz='US/Central')  # This timestamp won't show up in the pull
    ])

    spy.push(data_df, workbook=workbook, worksheet=worksheet)
    workbooks_df = spy.workbooks.search({
        'Name': workbook
    })
    workbooks = spy.workbooks.pull(workbooks_df, include_inventory=False, quiet=True)
    worksheet_start = workbooks[0].worksheets[0].display_range['Start'].value
    assert worksheet_start == data_df.index[0].value


@pytest.mark.system
def test_push_spaces_in_path_separator():
    signal_name = 'push_spaces_in_path_separator_signal'

    spy.push(metadata=pd.DataFrame([{
        'Type': 'StoredSignal',
        'Name': signal_name,
        'Path': "A>>B >>C"
    }]), workbook='test_push_spaces_in_path_separator', worksheet=None)
    spy.push(metadata=pd.DataFrame([{
        'Type': 'StoredSignal',
        'Name': signal_name,
        'Path': "A >> B >> C"
    }]), workbook='test_push_spaces_in_path_separator', worksheet=None)
    spy.push(metadata=pd.DataFrame([{
        'Type': 'StoredSignal',
        'Name': signal_name,
        'Path': " A>> B>>C "
    }]), workbook='test_push_spaces_in_path_separator', worksheet=None)

    pushed_results = spy.search({'Name': signal_name},
                                workbook='test_push_spaces_in_path_separator')
    assert len(pushed_results) == 1


@pytest.mark.system
def test_push_dataframe_nan_column():
    workbook = 'test_push_dataframe_nan_column'
    worksheet = 'nan worksheet'

    data_df = pd.DataFrame()
    df_index = pd.date_range('2020-01-01', periods=5, freq='s')
    data_df['Normal Signal'] = pd.Series(np.arange(5.0), index=df_index, dtype=object)
    data_df['NaN Signal Numerical'] = pd.Series(np.full(5, np.nan), index=df_index, dtype=object)
    data_df['NaN Signal String'] = pd.Series(np.full(5, np.nan), index=df_index, dtype=object)

    # Test that pushing NaN-only columns without units of measure fails with correct error message
    push_results_df = spy.push(data_df, workbook=workbook, worksheet=worksheet, errors='catalog')
    message = 'contains no data, does not correspond to a pre-existing signal, and has no Value Unit of Measure'
    assert message in push_results_df.loc['NaN Signal Numerical', 'Push Result']
    assert message in push_results_df.loc['NaN Signal String', 'Push Result']

    metadata_df = push_results_df.copy()
    metadata_df['Value Unit Of Measure'] = ['seconds', 'kg', 'string']
    metadata_df['Type'] = 'Signal'
    metadata_df.drop(columns='ID', inplace=True)

    # Test that pushing NaN-only columns with units of measure succeeds
    push_results_df = spy.push(data_df, metadata=metadata_df, workbook=workbook, worksheet=worksheet)
    assert (push_results_df['Push Result'] == 'Success').all()
    assert (push_results_df['Push Count'] == [5, 0, 0]).all()

    # Test pushing only NaNs to a pre-existing signal
    data_df['Normal Signal'] = np.nan
    # Test pushing numerical data to a signal created by a NaN-only spy.push() call
    data_df.loc['2020-01-01 00:00:00', 'NaN Signal Numerical'] = 3.14
    # Test pushing string data to a signal created by a NaN-only spy.push() call
    data_df.loc['2020-01-01 00:00:00', 'NaN Signal String'] = 'foo'
    # Test using metadata that contains ID but not Value Unit of Measure
    metadata_df = push_results_df.copy()
    metadata_df.drop(columns='Value Unit Of Measure')

    new_push_results_df = spy.push(data_df, metadata=metadata_df, workbook=workbook, worksheet=worksheet)
    assert (new_push_results_df['Push Result'] == 'Success').all()
    assert (new_push_results_df['Push Count'] == [0, 1, 1]).all()


@pytest.mark.system
def test_push_src_max_interp():
    push_df = spy.push(metadata=pd.DataFrame([{
        'Type': 'Signal',
        'Name': 'test_src_max_interp',
        'Source Maximum Interpolation': '2min'
    }]), workbook='test_push_src_max_interp', worksheet=None)
    assert (push_df['Push Result'] == 'Success').all()


@pytest.mark.system
def test_archive():
    # Cannot use archive=True if no metadata is given
    with pytest.raises(ValueError, match='metadata must be provided'):
        spy.push(metadata=pd.DataFrame(), archive=True, workbook='test_archive', worksheet=None)

    # Cannot use archive=True if metadata does not share an asset tree root
    with pytest.raises(ValueError, match='metadata must all belong to the same asset tree'):
        spy.push(metadata=pd.DataFrame([{
            'Path': 'Root 1',
            'Name': 'My Signal'
        }, {
            'Path': 'Root 2',
            'Name': 'My Condition'
        }]), archive=True, workbook='test_archive', worksheet=None)

    push_one = spy.push(metadata=pd.DataFrame([{
        'Type': 'Asset',
        'Path': 'Root 1 >> Some Path',
        'Name': 'My Asset 1'
    }, {
        'Type': 'Asset',
        'Path': 'Root 2 >> Some Path',
        'Name': 'My Asset 2'
    }]), workbook='test_archive_by_path', worksheet=None)
    asset_1_id = push_one.ID[0]
    asset_2_id = push_one.ID[1]

    push_two = spy.push(metadata=pd.DataFrame([{
        'Type': 'Asset',
        'Path': 'Root 1 >> Some Other Path',
        'Name': 'My Asset 3'
    }]), workbook='test_archive_by_path', worksheet=None, archive=True)
    asset_3_id = push_two.ID[0]

    items_api = ItemsApi(spy.session.client)
    item_1_output = items_api.get_item_and_all_properties(id=asset_1_id)
    item_2_output = items_api.get_item_and_all_properties(id=asset_2_id)
    item_3_output = items_api.get_item_and_all_properties(id=asset_3_id)

    # We expect that My Asset 1 is archived because it is in the "Root 1" asset tree but was not part of the second push
    assert item_1_output.is_archived is True
    # We expect that My Asset 2 is not archived because it is not in the "Root 1" asset tree
    assert item_2_output.is_archived is False
    # We expect that My Asset 3 is not archived by its own push call
    assert item_3_output.is_archived is False


@pytest.mark.system
def test_ignore_properties_on_repeated_push():
    # Test on a calculated condition
    push_results = spy.push(metadata=pd.DataFrame([{
        'Name': 'My Calc 1',
        'Path': 'test_ignore_spy_reserved_columns >> My Asset',
        'Formula': 'days()',
        'Object': 'bad_data'
    }]), workbook='test_ignore_properties_on_repeated_push', worksheet=None)

    # The index won't be unique as a result of the Assets added to the end
    push_results.reset_index(drop=True, inplace=True)

    # This test checks that the following doesn't push properties 'Path' and 'Object' to the condition in Seeq
    spy.push(metadata=push_results, workbook='test_ignore_properties_on_repeated_push', worksheet=None)

    condition_id = push_results.ID[0]
    search_results = spy.search({'ID': condition_id}, all_properties=True)

    assert len(search_results) == 1
    assert 'Object' not in search_results.columns
    assert search_results.at[0, 'Path'] == 'test_ignore_spy_reserved_columns'
    assert search_results.at[0, 'Asset'] == 'My Asset'

    # Test on a calculated scalar
    push_results = spy.push(metadata=pd.DataFrame([{
        'Name': 'My Calc 2',
        'Path': 'test_ignore_spy_reserved_columns >> My Asset',
        'Formula': '0',
        'Object': 'bad_data'
    }]), workbook='test_ignore_properties_on_repeated_push', worksheet=None)

    push_results.reset_index(drop=True, inplace=True)

    # This test checks that the following doesn't push properties 'Path' and 'Object' to the condition in Seeq
    spy.push(metadata=push_results, workbook='test_ignore_properties_on_repeated_push', worksheet=None)

    scalar_id = push_results.ID[0]
    search_results = spy.search({'ID': scalar_id}, all_properties=True)

    assert len(search_results) == 1
    assert 'Object' not in search_results.columns
    assert search_results.at[0, 'Path'] == 'test_ignore_spy_reserved_columns'
    assert search_results.at[0, 'Asset'] == 'My Asset'


@pytest.mark.system
def test_push_all_calculated_types_by_id():
    workbook = 'test_push_all_types_by_id'
    item_name = f'{workbook} {_common.new_placeholder_guid()}'

    signals_for_testing = spy.search({
        'Path': 'Example >> Cooling Tower 1 >> Area A'
    })

    items = [{
        'Name': f'{item_name} Scalar',
        'Type': 'Scalar',
        'Formula': '0'
    }, {
        'Name': f'{item_name} Signal',
        'Type': 'Signal',
        'Formula': '0.toSignal()'
    }, {
        'Name': f'{item_name} Condition',
        'Type': 'Condition',
        'Formula': 'hours()'
    }, {
        'Name': f'{item_name} Metric',
        'Type': 'Threshold Metric',
        'Measured Item': signals_for_testing[signals_for_testing['Name'] == 'Temperature']['ID'].iloc[0]
    }, {
        'Name': f'{item_name} Asset',
        'Type': 'Asset',
        'Path': f'{item_name} Root',
        'Asset': f'{item_name} Asset'
    }]

    push_results = spy.push(metadata=pd.DataFrame(items), workbook=workbook, worksheet=None)
    search_df = spy.search(push_results, workbook=workbook, all_properties=True)

    for _, row in search_df.iterrows():
        assert 'Tweaked' not in row['Name']

    search_df['Path'] = search_df['Path'].astype(object)
    search_df.at[(search_df['Name'] == f'{item_name} Asset').idxmax(), 'Path'] = f'{item_name} Root Tweaked'
    search_df['Name'] = search_df['Name'] + ' Tweaked'

    push_results = spy.push(metadata=search_df, workbook=workbook, worksheet=None)
    search_df = spy.search(push_results, workbook=workbook, all_properties=True)

    for _, row in search_df.iterrows():
        assert 'Tweaked' in row['Name']


@pytest.mark.system
def test_push_all_stored_types_by_id():
    workbook = 'test_push_all_stores_types_by_id'
    item_name = f'{workbook} {_common.new_placeholder_guid()}'

    items = [{
        'Name': f'{item_name} Signal',
        'Type': 'Signal',
        'Maximum Interpolation': '1d',
        'Value Unit Of Measure': 'm',
        'Number Format': '#,##0.00'
    }, {
        'Name': f'{item_name} Condition',
        'Type': 'Condition',
        'Maximum Duration': '1h'
    }, {
        'Name': f'{item_name} Scalar',
        'Type': 'Scalar',
        'Formula': '1.1m',
        'Custom Property': 'My Test'
    }]

    push_results = spy.push(metadata=pd.DataFrame(items), workbook=workbook, worksheet=None)
    search_df = spy.search(push_results, workbook=workbook, all_properties=True)

    # CRAB-37888: We are now forcing SPy scalars to be CalculatedScalar, so they can be edited in the UI
    literal_scalar_type = 'CalculatedScalar'

    assert search_df.at[0, 'Name'] == f'{item_name} Signal'
    assert search_df.at[0, 'Interpolation Method'] == 'Linear'

    # 'Maximum Interpolation', when sent in via the batch endpoints, is converted to 'Source Maximum Interpolation'
    assert search_df.at[0, 'Maximum Interpolation'] == '1d'

    # 'Value Unit Of Measure', when sent in via the batch endpoints, is converted to 'Source Value Unit Of Measure'
    assert search_df.at[0, 'Source Value Unit Of Measure'] == 'm'

    # The 'Value Unit Of Measure' seen in output is "calculated", see below
    assert search_df.at[0, 'Value Unit Of Measure'] == 'm'

    # 'Number Format', when sent in via the batch endpoints, is converted to 'Source Number Format'
    assert search_df.at[0, 'Source Number Format'] == '#,##0.00'

    assert search_df.at[1, 'Name'] == f'{item_name} Condition'
    assert search_df.at[1, 'Maximum Duration'] == '1h'

    assert search_df.at[2, 'Name'] == f'{item_name} Scalar'
    assert search_df.at[2, 'Type'] == literal_scalar_type
    assert search_df.at[2, 'Formula'] == '1.1m'
    assert search_df.at[2, 'Custom Property'] == 'My Test'

    # Now tweak a bunch of stuff and see if it gets accepted
    search_df = search_df.astype(object)

    search_df.at[0, 'Name'] = f'{item_name} Signal - Tweaked'
    search_df.at[0, 'Description'] = f'{item_name} Signal Description'

    # 'Interpolation Method' can be changed directly -- if this is a connector-backed signal, it'll just get changed
    # back upon next indexing
    search_df.at[0, 'Interpolation Method'] = 'step'

    # 'Maximum Interpolation' can be overridden
    search_df.at[0, 'Override Maximum Interpolation'] = '2d'

    # 'Value Unit Of Measure' is overridden just by specifying it directly in the set_properties call, but it'll get
    # overwritten again upon next indexing
    search_df.at[0, 'Value Unit Of Measure'] = 'ft'

    # 'Number Format' is a weird beast. When you use the set_property call with "Number Format" as the name of the
    # property, it somehow erases the "Source Number Format" completely.
    search_df.at[0, 'Number Format'] = np.nan
    search_df.at[0, 'Override Number Format'] = '0.0000E+0'

    search_df.at[1, 'Name'] = f'{item_name} Condition - Tweaked'
    search_df.at[1, 'Description'] = f'{item_name} Condition Description'
    search_df.at[1, 'Maximum Duration'] = '2h'

    search_df.at[2, 'Name'] = f'{item_name} Scalar - Tweaked'
    search_df.at[2, 'Description'] = f'{item_name} Scalar Description'
    search_df.at[2, 'Formula'] = '-2.2m'
    search_df.at[2, 'Custom Property'] = 'My Test Update'
    search_df.at[2, 'New Custom Property'] = 'My Test 2'

    push_results = spy.push(metadata=search_df, workbook=workbook, worksheet=None)
    search_df = spy.search(push_results, workbook=workbook, all_properties=True)

    assert search_df.at[0, 'Name'] == f'{item_name} Signal - Tweaked'
    assert search_df.at[0, 'Description'] == f'{item_name} Signal Description'
    assert search_df.at[0, 'Interpolation Method'] == 'Step'
    assert search_df.at[0, 'Source Maximum Interpolation'] == '1d'
    assert search_df.at[0, 'Maximum Interpolation'] == '2d'
    assert search_df.at[0, 'Override Maximum Interpolation'] == '2d'
    assert search_df.at[0, 'Source Value Unit Of Measure'] == 'm'
    assert search_df.at[0, 'Value Unit Of Measure'] == 'ft'
    assert 'Source Number Format' not in search_df
    assert search_df.at[0, 'Number Format'] == '0.0000E+0'

    assert search_df.at[1, 'Name'] == f'{item_name} Condition - Tweaked'
    assert search_df.at[1, 'Description'] == f'{item_name} Condition Description'
    assert search_df.at[1, 'Maximum Duration'] == '2h'

    assert search_df.at[2, 'Name'] == f'{item_name} Scalar - Tweaked'
    assert search_df.at[2, 'Type'] == literal_scalar_type
    assert search_df.at[2, 'Description'] == f'{item_name} Scalar Description'
    assert search_df.at[2, 'Formula'] == '-2.2m'
    assert search_df.at[2, 'Custom Property'] == 'My Test Update'
    assert search_df.at[2, 'New Custom Property'] == 'My Test 2'

    # Now push again without using IDs and we should see that the overrides stay (where possible)
    push_results = spy.push(metadata=pd.DataFrame(items), workbook=workbook, worksheet=None)
    search_df = spy.search(push_results, workbook=workbook, all_properties=True)

    assert search_df.at[0, 'Name'] == f'{item_name} Signal'
    assert search_df.at[0, 'Description'] == f'{item_name} Signal Description'
    # Interpolation Method is overwritten
    assert search_df.at[0, 'Interpolation Method'] == 'Linear'
    assert search_df.at[0, 'Source Maximum Interpolation'] == '1d'
    assert search_df.at[0, 'Maximum Interpolation'] == '2d'
    assert search_df.at[0, 'Override Maximum Interpolation'] == '2d'
    assert search_df.at[0, 'Source Value Unit Of Measure'] == 'm'
    # Value Unit Of Measure is overwritten
    assert search_df.at[0, 'Value Unit Of Measure'] == 'm'
    # Source Number Format has been restored
    assert search_df.at[0, 'Source Number Format'] == '#,##0.00'
    assert search_df.at[0, 'Number Format'] == '0.0000E+0'

    assert search_df.at[1, 'Name'] == f'{item_name} Condition'
    assert search_df.at[1, 'Description'] == f'{item_name} Condition Description'
    assert search_df.at[1, 'Maximum Duration'] == '1h'


@pytest.mark.system
def test_push_url_has_correct_worksheet():
    # First test pushing by workbook ID
    workbook = spy.workbooks.Analysis('My Workbook %s' % _common.new_placeholder_guid())
    workbook.worksheet('First')
    workbook.worksheet('Second')
    spy.workbooks.push([workbook])

    # Test new workbook with new worksheet
    push_results = spy.push(metadata=pd.DataFrame([{'Name': 'My Calc 1', 'Formula': 'sinusoid()'}]),
                            workbook=workbook['ID'],
                            worksheet='Second')
    assert _url.get_workbook_id_from_url(push_results.spy.workbook_url) == workbook['ID']
    assert _url.get_worksheet_id_from_url(push_results.spy.workbook_url) == workbook.worksheet('Second')['ID']

    # Test existing workbook with new worksheet
    push_results = spy.push(metadata=pd.DataFrame([{'Name': 'My Calc 2', 'Formula': 'sinusoid()'}]),
                            workbook=workbook['ID'],
                            worksheet='Third')
    workbook = spy.workbooks.pull(workbook.url)[0]
    assert isinstance(workbook, spy.workbooks.Analysis)
    assert _url.get_workbook_id_from_url(push_results.spy.workbook_url) == workbook['ID']
    assert _url.get_worksheet_id_from_url(push_results.spy.workbook_url) == workbook.worksheet('Third')['ID']

    # Test existing workbook with existing worksheet
    push_results = spy.push(metadata=pd.DataFrame([{'Name': 'My Calc 3', 'Formula': 'sinusoid()'}]),
                            workbook=workbook['ID'],
                            worksheet='Third')
    assert _url.get_workbook_id_from_url(push_results.spy.workbook_url) == workbook['ID']
    assert _url.get_worksheet_id_from_url(push_results.spy.workbook_url) == workbook.worksheet('Third')['ID']

    # Now test pushing by workbook name
    workbook_name = 'My Workbook %s' % _common.new_placeholder_guid()
    workbook = spy.workbooks.Analysis(workbook_name)
    workbook.worksheet('First')
    workbook.worksheet('Second')
    spy.workbooks.push([workbook])

    # Test new workbook with new worksheet
    push_results = spy.push(metadata=pd.DataFrame([{'Name': 'My Calc 1', 'Formula': 'sinusoid()'}]),
                            workbook=workbook_name,
                            worksheet='Second')
    assert _url.get_workbook_id_from_url(push_results.spy.workbook_url) == workbook['ID']
    assert _url.get_worksheet_id_from_url(push_results.spy.workbook_url) == workbook.worksheet('Second')['ID']

    # Test existing workbook with new worksheet
    push_results = spy.push(metadata=pd.DataFrame([{'Name': 'My Calc 2', 'Formula': 'sinusoid()'}]),
                            workbook=workbook_name,
                            worksheet='Third')
    workbook = spy.workbooks.pull(workbook.url)[0]
    assert isinstance(workbook, spy.workbooks.Analysis)
    assert _url.get_workbook_id_from_url(push_results.spy.workbook_url) == workbook['ID']
    assert _url.get_worksheet_id_from_url(push_results.spy.workbook_url) == workbook.worksheet('Third')['ID']

    # Test existing workbook with existing worksheet
    push_results = spy.push(metadata=pd.DataFrame([{'Name': 'My Calc 3', 'Formula': 'sinusoid()'}]),
                            workbook=workbook_name,
                            worksheet='Third')
    assert _url.get_workbook_id_from_url(push_results.spy.workbook_url) == workbook['ID']
    assert _url.get_worksheet_id_from_url(push_results.spy.workbook_url) == workbook.worksheet('Third')['ID']


@pytest.mark.system
def test_push_display_template():
    displays_templates_api = DisplayTemplatesApi(spy.session.client)

    workbook, workstep, asset, _ = test_common.create_workbook_workstep_asset_template()
    datasource = 'Datasource %s' % spy._common.new_placeholder_guid()

    # Test 1: create display template, good inputs
    push_results = spy.push(metadata=pd.DataFrame([{
        'Name': 'My Display',
        'Type': 'DisplayTemplate',
        'Description': 'this is a display template',
        'Source Workstep ID': workstep.id,
        'Swap Source Asset ID': asset.id,
        'Extra Property': 'Hello',
    }]), workbook=workbook.id, datasource=datasource)

    display_template_output = displays_templates_api.get_display_template(id=push_results.at[0, 'ID'])
    assert display_template_output.name == 'My Display'
    assert display_template_output.description == 'this is a display template'
    assert display_template_output.source_workstep_id == workstep.id
    assert display_template_output.swap_source_asset_id == asset.id
    assert display_template_output.scoped_to == workbook.id
    assert display_template_output.datasource_class == 'Seeq Data Lab'
    assert display_template_output.datasource_id == datasource
    assert display_template_output.is_archived is False
    assert display_template_output.display_count == 0

    _, new_workstep, new_asset, _ = test_common.create_workbook_workstep_asset_template(workbook_id=workbook.id)

    # Test 2: update display template, good inputs
    push_results = spy.push(metadata=pd.DataFrame([{
        'ID': push_results.at[0, 'ID'],
        'Name': 'My Display Renamed',
        'Type': 'DisplayTemplate',
        'Description': 'this is an updated display template',
        'Source Workstep ID': new_workstep.id,
        'Swap Source Asset ID': new_asset.id,
        'Extra Property': 'Goodbye',
    }]), workbook=workbook.id, datasource=datasource)

    display_template_output = displays_templates_api.get_display_template(id=push_results.at[0, 'ID'])
    assert display_template_output.name == 'My Display Renamed'
    assert display_template_output.description == 'this is an updated display template'
    assert display_template_output.source_workstep_id == new_workstep.id
    assert display_template_output.swap_source_asset_id == new_asset.id
    assert display_template_output.scoped_to == workbook.id
    assert display_template_output.datasource_class == 'Seeq Data Lab'
    assert display_template_output.datasource_id == datasource
    assert display_template_output.is_archived is False
    assert display_template_output.display_count == 0

    # Test 3: cannot push template with a path
    with pytest.raises(RuntimeError, match='Display Template cannot have a path or asset'):
        spy.push(metadata=pd.DataFrame([{
            'Name': 'My Display',
            'Type': 'Template',
            'Source Workstep ID': workstep.id,
            'Path': 'My Path >> My Asset',
        }]), workbook=workbook.id)


@pytest.mark.system
def test_push_display():
    displays_api = DisplaysApi(spy.session.client)
    items_api = ItemsApi(spy.session.client)

    datasource = 'Datasource %s' % spy._common.new_placeholder_guid()
    workbook, workstep, asset, template = test_common.create_workbook_workstep_asset_template(
        template_name='My Display', datasource=datasource)

    # Test 1: create display from existing template, good inputs
    push_results1 = spy.push(metadata=pd.DataFrame([{
        'Name': 'My Display',
        'Type': 'Display',
        'Template ID': template.id,
        'Swap Source Asset ID': asset.id,
        'Source Workstep ID': workstep.id,
        'Path': 'My Path >> My Asset',
    }]), workbook=workbook.id, datasource=datasource)

    display_output1 = displays_api.get_display(id=push_results1.at[0, 'ID'])
    assert display_output1.name == 'My Display'
    assert display_output1.datasource_class == 'Seeq Data Lab'
    assert display_output1.datasource_id == datasource
    assert display_output1.data_id == '[%s] {Display} My Path >> My Asset >> My Display' % workbook.id
    assert display_output1.scoped_to == workbook.id
    assert display_output1.template.id == template.id
    assert display_output1.swap is not None
    assert display_output1.swap.swap_out == asset.id

    # Test 2: create display from existing template, bad inputs
    with pytest.raises(RuntimeError, match='Datasource ID of display must match'):
        spy.push(metadata=pd.DataFrame([{
            'Name': 'My Display',
            'Type': 'Display',
            'Template ID': template.id,
        }]), workbook=workbook.id, datasource='Different Datasource')

    with pytest.raises(RuntimeError, match='Name of display must match'):
        spy.push(metadata=pd.DataFrame([{
            'Name': 'Different Name',
            'Type': 'Display',
            'Template ID': template.id,
        }]), workbook=workbook.id, datasource=datasource)

    with pytest.raises(RuntimeError, match='Scoped To of display must match'):
        spy.push(metadata=pd.DataFrame([{
            'Name': 'My Display',
            'Type': 'Display',
            'Template ID': template.id,
        }]), workbook='Different Workbook', datasource=datasource)

    with pytest.raises(RuntimeError, match='Source Workstep ID of display must match'):
        spy.push(metadata=pd.DataFrame([{
            'Name': 'My Display',
            'Type': 'Display',
            'Template ID': template.id,
            'Source Workstep ID': 'Different workstep'
        }]), workbook=workbook.id, datasource=datasource)

    with pytest.raises(RuntimeError, match='Swap Source Asset ID of display must match'):
        spy.push(metadata=pd.DataFrame([{
            'Name': 'My Display',
            'Type': 'Display',
            'Template ID': template.id,
            'Swap Source Asset ID': 'Different asset'
        }]), workbook=workbook.id, datasource=datasource)

    # Test 3: create display with new template
    push_results3 = spy.push(metadata=pd.DataFrame([{
        'Name': 'My Display',
        'Type': 'Display',
        'Swap Source Asset ID': asset.id,
        'Source Workstep ID': workstep.id,
        'Path': 'My Path >> My Asset',
    }]), workbook=workbook.id, datasource=datasource)

    display_output3 = displays_api.get_display(id=push_results3.at[0, 'ID'])
    assert display_output3.name == 'My Display'
    assert display_output3.datasource_class == 'Seeq Data Lab'
    assert display_output3.datasource_id == datasource
    assert display_output3.data_id == '[%s] {Display} My Path >> My Asset >> My Display' % workbook.id
    assert display_output3.scoped_to == workbook.id
    assert display_output3.template.id != template.id
    assert display_output3.template.source_workstep_id == workstep.id
    assert display_output3.template.swap_source_asset_id == asset.id
    assert display_output3.swap is not None
    assert display_output3.swap.swap_out == asset.id

    # Test 4: update existing display, good inputs
    display_templates_api = DisplayTemplatesApi(spy.session.client)
    new_template = display_templates_api.create_display_template(body=DisplayTemplateInputV1(
        name='My Updated Display',
        datasource_class=template.datasource_class,
        datasource_id=template.datasource_id,
        scoped_to=template.scoped_to,
        source_workstep_id=template.source_workstep_id
    ))

    push_results4 = spy.push(metadata=pd.DataFrame([{
        'ID': push_results1.at[0, 'ID'],
        'Name': 'My Updated Display',
        'Type': 'Display',
        'Template ID': new_template.id,
        'Source Workstep ID': workstep.id,
        'Path': 'My Path >> My Asset',
    }, {
        'Name': 'Another Item',
        'Type': 'Asset',
        'Path': 'My Path >> My Asset',
    }]), workbook=workbook.id, datasource=datasource)

    display_output4 = displays_api.get_display(id=push_results4.at[0, 'ID'])
    assert display_output4.sync_token == items_api.get_property(id=push_results4.at[1, 'ID'],
                                                                property_name='Sync Token').value
    assert display_output4.template.id == new_template.id
    assert display_output4.name == new_template.name
    assert display_output4.template.source_workstep_id == new_template.source_workstep_id

    # Test 5: update existing display, bad inputs
    for prop in ('Swap Source Asset ID', 'Source Workstep ID', 'Name'):
        with pytest.raises(RuntimeError, match='must match that of its template'):
            spy.push(metadata=pd.DataFrame([{
                'ID': push_results1.at[0, 'ID'],
                'Name': 'My Updated Display',
                'Type': 'Display',
                'Template ID': new_template.id,
                prop: 'bad value',
            }]), workbook=workbook.id, datasource=datasource)

    # Test 6: missing both Template ID and Source Workstep ID
    with pytest.raises(RuntimeError, match='require either a "Template ID" or "Source Workstep ID"'):
        spy.push(metadata=pd.DataFrame([{
            'Name': 'My Display',
            'Type': 'Display'
        }]))


@pytest.mark.system
def test_push_cache_enabled():
    # Added based on Craig's comment here:
    # https://bitbucket.org/seeq12/crab/pull-requests/18814#comment-315914944

    test_name = 'test_push_cache_enabled'
    push_df = spy.push(metadata=pd.DataFrame([{
        'Type': 'Signal',
        'Name': 'My Sinusoid',
        'Formula': 'sinusoid(1h)',
        'Cache Enabled': False
    }, {
        'Type': 'Signal',
        'Name': 'My Sawtooth',
        'Formula': 'sawtooth(1h)'
    }]), workbook=test_name, worksheet=None)

    search_df = spy.search(push_df, all_properties=True)
    sinusoid = search_df[search_df['Name'] == 'My Sinusoid'].iloc[0]
    sawtooth = search_df[search_df['Name'] == 'My Sawtooth'].iloc[0]
    assert not sinusoid['Cache Enabled']
    assert sawtooth['Cache Enabled']

    # Push again with no Cache Enabled column, it shouldn't have changed
    push2_df = spy.push(metadata=pd.DataFrame([{
        'Type': 'Signal',
        'Name': 'My Sinusoid',
        'Formula': 'sinusoid(1h)'
    }, {
        'Type': 'Signal',
        'Name': 'My Sawtooth',
        'Formula': 'sawtooth(1h)'
    }]), workbook=test_name, worksheet=None)

    search2_df = spy.search(push2_df, all_properties=True)
    sinusoid2 = search2_df[search2_df['Name'] == 'My Sinusoid'].iloc[0]
    sawtooth2 = search2_df[search2_df['Name'] == 'My Sawtooth'].iloc[0]
    assert sinusoid['ID'] == sinusoid2['ID']
    assert sawtooth['ID'] == sawtooth2['ID']
    assert not sinusoid2['Cache Enabled']
    assert sawtooth2['Cache Enabled']


@pytest.mark.system
def test_push_manual_archive():
    previous_threshold = _push.DATASOURCE_CLEANUP_ITEM_COUNT_THRESHOLD
    _push.DATASOURCE_CLEANUP_ITEM_COUNT_THRESHOLD = 0
    workbook = 'Workbook %s' % _common.new_placeholder_guid()

    for _ in range(2):
        spy.push(metadata=pd.DataFrame([{
            'Name': 'My Signal',
            'Type': 'Signal',
            'Path': 'Tree Root >> Signal Path',
            'Formula': 'sinusoid()'
        }, {
            'Name': 'My Condition',
            'Type': 'Condition',
            'Path': 'Tree Root >> Condition Path',
            'Formula': 'days()'
        }]), workbook=workbook, archive=True)

        search_results = spy.search([
            {'Path': 'Tree Root'},
            {'Path': '', 'Name': 'Tree Root'}
        ], workbook=workbook, include_archived=True)

        assert len(search_results) == 5
        assert sum(search_results['Archived']) == 0

    spy.push(metadata=pd.DataFrame([{
        'Name': 'My Signal',
        'Type': 'Signal',
        'Path': 'Tree Root >> Signal Path',
        'Formula': 'sinusoid()'
    }]), workbook=workbook, archive=True)

    search_results = spy.search([
        {'Path': 'Tree Root'},
        {'Path': '', 'Name': 'Tree Root'}
    ], workbook=workbook, include_archived=True)

    assert len(search_results) == 5
    assert sum(search_results['Archived']) == 2
    assert set(search_results.Name[search_results.Archived]) == {'My Condition', 'Condition Path'}

    _push.DATASOURCE_CLEANUP_ITEM_COUNT_THRESHOLD = previous_threshold


@pytest.mark.system
def test_push_global_manual_archive():
    previous_threshold = _push.DATASOURCE_CLEANUP_ITEM_COUNT_THRESHOLD
    _push.DATASOURCE_CLEANUP_ITEM_COUNT_THRESHOLD = 0
    root = f'test_push_global_manual_archive_{_common.new_placeholder_guid()}'

    for _ in range(2):
        spy.push(metadata=pd.DataFrame([{
            'Name': 'My Signal',
            'Type': 'Signal',
            'Path': f'{root} >> Signal Path',
            'Formula': 'sinusoid()'
        }, {
            'Name': 'My Condition',
            'Type': 'Condition',
            'Path': f'{root} >> Condition Path',
            'Formula': 'days()'
        }]), workbook=None, archive=True)

        search_results = spy.search([
            {'Path': root},
            {'Path': '', 'Name': root}
        ], workbook=None, include_archived=True)

        assert len(search_results) == 5
        assert sum(search_results['Archived']) == 0

    spy.push(metadata=pd.DataFrame([{
        'Name': 'My Signal',
        'Type': 'Signal',
        'Path': f'{root} >> Signal Path',
        'Formula': 'sinusoid()'
    }]), workbook=None, archive=True)

    search_results = spy.search([
        {'Path': root},
        {'Path': '', 'Name': root}
    ], workbook=None, include_archived=True)

    assert len(search_results) == 5
    assert sum(search_results['Archived']) == 2
    assert set(search_results.Name[search_results.Archived]) == {'My Condition', 'Condition Path'}

    _push.DATASOURCE_CLEANUP_ITEM_COUNT_THRESHOLD = previous_threshold


@pytest.mark.system
def test_push_seeq_internal_datasource():
    name = f'test_push_seeq_internal_datasource {_common.new_placeholder_guid()}'
    input_df = pd.DataFrame([{
        'Name': f'{name} Asset',
        'Type': 'Asset',
    }, {
        'Name': f'{name} Calculated Signal',
        'Formula': 'sinusoid()',
        'Type': 'Signal',
    }, {
        'Name': f'{name} Stored Signal',
        'Maximum Interpolation': '1d',
        'Type': 'Signal',
    }, {
        'Name': f'{name} Simple Scalar',
        'Formula': '1m',
        'Type': 'Scalar',
    }, {
        'Name': f'{name} Calculated Scalar',
        'Formula': '$s.average(capsule("2023-01-01"))',
        'Formula Parameters': [f's={name} Calculated Signal'],
        'Type': 'Scalar',
    }, {
        'Name': f'{name} Calculated Condition',
        'Formula': 'days()',
        'Type': 'Condition',
    }, {
        'Name': f'{name} Stored Condition',
        'Maximum Duration': '1d',
        'Type': 'Condition',
    }, {
        'Name': f'{name} Metric',
        'Measured Item': f'{name} Calculated Signal',
        'Type': 'Metric',
    }])
    # Verify the results are all pushed to the Seeq-internal datasources and that their ACLs inherit from the workbook
    push_result_1 = spy.push(metadata=input_df, workbook=name, datasource=spy.INHERIT_FROM_WORKBOOK)
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
        error = f"{item_name} has incorrect permission origin: {origin}" if origin != 'Analysis' else ''
        assert not error

    # Re-pushing the same items should keep the datasource
    push_result_2 = spy.push(metadata=input_df, workbook=name, datasource=spy.INHERIT_FROM_WORKBOOK)
    actual_datasources = push_result_2['Datasource Class'].unique()
    unexpected = list(actual_datasources).remove(spy.INHERIT_FROM_WORKBOOK)
    error = f"Unexpected datasources were re-pushed incorrectly to: {unexpected}" if unexpected else ''
    assert not error

    # Round-tripping should maintain the datasource if specified
    search_results = spy.search(query={'Name': f'{name}*'}, workbook=name, all_properties=True)
    search_results = search_results[search_results['Type'] != 'Analysis']
    actual_datasources = search_results['Datasource Class'].unique()
    unexpected = list(actual_datasources).remove(spy.INHERIT_FROM_WORKBOOK)
    error = f"Unexpected datasources were found in search results: {unexpected}" if unexpected else ''
    assert not error
    push_result_3 = spy.push(metadata=search_results, workbook=name, datasource=spy.INHERIT_FROM_WORKBOOK)
    actual_datasources = push_result_3['Datasource Class'].unique()
    unexpected = list(actual_datasources).remove(spy.INHERIT_FROM_WORKBOOK)
    error = f"Unexpected datasources were round-tripped incorrectly to: {unexpected}" if unexpected else ''
    assert not error

    # Pushing the items with a different datasource set should push to the specified datasource
    push_result_4 = spy.push(metadata=input_df, workbook=name, datasource=name)
    actual_datasource_classes = push_result_4['Datasource Class'].unique()
    unexpected = list(actual_datasource_classes).remove(_common.DEFAULT_DATASOURCE_CLASS)
    error = f"Unexpected Datasource Classes were newly-pushed incorrectly to: {unexpected}" if unexpected else ''
    assert not error
    actual_datasource_ids = push_result_4['Datasource ID'].unique()
    unexpected = list(actual_datasource_ids).remove(name)
    error = f"Unexpected Datasource IDs were newly-pushed incorrectly to: {unexpected}" if unexpected else ''
    assert not error


@pytest.mark.system
def test_round_trip_permission_inheritance_disabled():
    name = f'test_round_trip_permission_inheritance_disabled_{_common.new_placeholder_guid()}'
    df = pd.DataFrame([{
        'Name': name,
        'Type': 'Signal',
        'Formula': 'sinusoid()'
    }])
    create_result = spy.push(metadata=df, workbook=None)
    assert len(create_result) == 1
    permissions_result = spy.acl.push(create_result,
                                      [{'ID': spy.session.user.id, 'Read': True, 'Write': True, 'Manage': True}],
                                      disable_inheritance=True)
    assert len(permissions_result) == 1
    search_result = spy.search({'Name': name}, all_properties=True)
    assert len(search_result) == 1
    assert SeeqNames.Properties.permission_inheritance_disabled in search_result.columns
    re_push_result = spy.push(metadata=search_result, workbook=None)
    assert len(re_push_result) == 1


@pytest.mark.system
def test_round_trip_non_spy_tree_items():
    # Setup: Create a workbook displaying a non-SPy item that's in an asset tree.
    name = f'test_round_trip_non_spy_tree_items_{_common.new_placeholder_guid()}'
    area_a_temp = spy.search({'Name': 'Temperature',
                              'Path': 'Example >> Cooling Tower 1 >> Area A',
                              'Datasource Name': 'Example Data',
                              }, all_properties=True)
    assert len(area_a_temp) == 1
    workbook = spy.workbooks.Analysis({'Name': name})
    worksheet = workbook.worksheet(name)
    worksheet.display_items = area_a_temp
    spy.workbooks.push(workbook)

    # Test: That should be able to do a round-trip Search via workbook URL & Push
    push_results = spy.push(metadata=spy.search(workbook.url),
                            workbook=name, worksheet=name)
    assert len(push_results) == 1
    # Also a Push from all_properties metadata
    push_results = spy.push(metadata=area_a_temp,
                            workbook=name, worksheet=name)
    assert len(push_results) == 1


@pytest.mark.performance
def test_push_performance():
    # This test is not currently publishing anything to "sq perf", but my hope is that eventually we have a set of
    # these SPy performance tests that are monitored for performance regressions. For now it's just a convenient
    # way to look at push performance.
    sample_count = 200000
    data_df = pd.DataFrame(np.random.randint(0, 100, size=(sample_count, 4)), columns=list('ABCD'))
    timestamps = pd.date_range(start='2020-01-01', end='2020-06-01', periods=sample_count)
    data_df.index = timestamps

    push_df = spy.push(data_df, workbook='test_push_performance', worksheet=None)
    status_df = push_df.spy.status.df
    print(status_df[['Count', 'Time']])


worker_count = 10


@pytest.mark.system
def test_concurrent_push():
    seeq_server_major, _, _ = spy.utils.get_server_version_tuple(spy.session)
    if seeq_server_major < 61:
        # This requires CRAB-35817 to be fixed, which was merged into R61
        return

    def _spawn(_command_line):
        _subprocess = subprocess.run(_command_line)
        print(f'Subprocess with command line {_command_line} returned with exit code {_subprocess.returncode}')

    with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as executor:
        _futures = list()

        for i in range(worker_count):
            import subprocess
            _futures.append(executor.submit(_spawn, ['python', __file__, str(i)]))

        concurrent.futures.wait(_futures)

        for _future in _futures:
            if _future.exception():
                raise _future.exception()

    print('test_concurrent_push: Finished')


@pytest.mark.system
def test_interrupt_data_push():
    test_name = 'test_interrupt_data_push'
    data_df = pd.DataFrame({'My Signal': range(10)}, index=pd.date_range('2020-01-01', periods=10, freq='D'))

    with mock.patch('seeq.sdk.SignalsApi.add_samples', side_effect=KeyboardInterrupt):
        with pytest.raises(KeyboardInterrupt):
            spy.push(data_df, workbook=test_name, worksheet=None, datasource=test_name)

        with pytest.raises(KeyboardInterrupt):
            spy.push(data_df, workbook=test_name, worksheet=None, datasource=test_name, errors='catalog')

    with mock.patch('seeq.sdk.SignalsApi.add_samples', side_effect=ValueError):
        with pytest.raises(ValueError):
            spy.push(data_df, workbook=test_name, worksheet=None, datasource=test_name)

        pushed_df = spy.push(data_df, workbook=test_name, worksheet=None, datasource=test_name, errors='catalog')
        assert 'ValueError' in pushed_df.iloc[0]['Push Result']


def _push_tree(tree_number):
    Configuration().retry_timeout_in_seconds = 20
    metadata_df = pd.DataFrame({'Count': np.arange(1000)})
    metadata_df['Path'] = f'test_concurrent_push_{tree_number}'
    metadata_df['Asset'] = 'Asset ' + metadata_df['Count'].astype(str)
    metadata_df['Name'] = 'A Number'
    metadata_df['Type'] = 'Signal'
    metadata_df['Formula'] = '10.toSignal()'
    status = Status()
    print(f'[Tree Number {tree_number}]: Starting push at {time.time()}')
    spy.push(metadata=metadata_df, status=status, workbook=f'test_concurrent_push_{tree_number}', worksheet=None)
    print(f'[Tree Number {tree_number}]: Finished push at {time.time()}')


# This is used by test_concurrent_push() above when it is spawning worker processes
if __name__ == '__main__':
    test_common.log_in_default_user()
    _push_tree(sys.argv[1])
