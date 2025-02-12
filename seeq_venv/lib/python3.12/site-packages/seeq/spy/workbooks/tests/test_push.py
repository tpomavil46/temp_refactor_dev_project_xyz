import json
import os
import re
import tempfile
import textwrap
import uuid
import zipfile
from time import time
from unittest import mock

import pandas as pd
import pytest
import requests
from dateutil import parser as isodate

from seeq import spy
from seeq.base import util
from seeq.sdk import *
from seeq.spy import _common
from seeq.spy._errors import *
from seeq.spy.assets import Asset
from seeq.spy.tests import test_common
from seeq.spy.tests.test_common import Sessions
from seeq.spy.workbooks._content import Content, DateRange, AssetSelection
from seeq.spy.workbooks._data import StoredSignal, CalculatedSignal, StoredCondition, CalculatedCondition, LiteralScalar
from seeq.spy.workbooks._item import Item
from seeq.spy.workbooks._workbook import Workbook, Analysis, Topic
from seeq.spy.workbooks._worksheet import Worksheet, AnalysisWorksheet
from seeq.spy.workbooks.tests import test_load


def setup_module():
    test_common.initialize_sessions()


def _get_exports_folder():
    return os.path.join(os.path.dirname(__file__), 'Exports')


def get_full_path_of_export(subfolder):
    return os.path.join(_get_exports_folder(), subfolder)


def _load_and_push(subfolder, label):
    workbooks = spy.workbooks.load(get_full_path_of_export(subfolder))
    return _push(workbooks, label)


def _push(workbooks, label):
    push_df = spy.workbooks.push(workbooks, path=label, label=label, datasource=label, refresh=False)
    return push_df.iloc[0]['Pushed Workbook ID']


@pytest.mark.export
def test_export_example_and_test_data():
    # Use this function to re-create the Example Export and test-related exports.
    # First copy the contents of "crab/sdk/pypi/spy-example-and-test-data-folder.zip" into "crab/sq-run-data-dir"
    # and start Seeq Server by doing "sq run" from crab.
    #
    # You MUST log in as "mark.derbecker@seeq.com" with password "SeeQ2013!". (If you don't log in as
    # mark.derbecker@seeq.com, then some of the ACL tests may get screwed up.)
    #
    # You will need to create a development license (in the licenses project, do "sq license create -h" to see
    # the command arguments needed to do that) and put it in the "licenses" folder of the data folder.
    #
    # If you add workbooks, make sure to share them with Everyone because the tests will log in as Agent API Key.
    #
    # When finished, change the sdk-system-tests Run Configuration in IntelliJ to have an "-m export" flag so that only
    # this test gets executed. It will copy everything into the right spot.
    #
    # Then make sure to zip up the contents of "crab/sq-run-data-dir" and replace
    # "crab/sdk/pypi/spy-example-and-test-data-folder.zip" and commit it to the repo.
    search_df = spy.workbooks.search({
        'Path': 'Example Export'
    }, content_filter='ALL')

    workbooks = spy.workbooks.pull(search_df)
    for workbook in workbooks:
        # Make "Isolate By User" true so that, by default, a label will be added on spy.workbooks.push() that will
        # isolate users from each other.
        workbook.definition['Isolate By User'] = True

    _save(workbooks, test_load.get_example_export_path())

    search_df = spy.workbooks.search({}, content_filter='ALL')
    search_df = search_df[search_df['Type'] == 'Workbook']

    workbooks = spy.workbooks.pull(search_df)
    path = _get_exports_folder()
    _save(workbooks, path, pretty_print_html=True)

    _delete_max_capsule_duration_on_bad_metric()

    search_df = spy.workbooks.search({
        'Path': 'ACL Test Folder'
    }, content_filter='ALL')

    workbooks = spy.workbooks.pull(search_df)

    spy.workbooks.save(workbooks, _get_exports_folder(), pretty_print_html=True)

    _pull_and_save('Workbook Template Tests', test_load.get_workbook_template_tests_path())
    _pull_and_save('Report and Dashboard Templates', test_load.get_report_and_dashboard_templates_path())
    _pull_and_save('Workbook Templates', test_load.get_workbook_templates_path())


def _pull_and_save(workbook_folder_path, destination_file_path):
    search_df = spy.workbooks.search({
        'Path': workbook_folder_path
    }, content_filter='ALL')

    workbooks = spy.workbooks.pull(search_df)

    _save(workbooks, destination_file_path)


def _save(workbooks, destination_file_path, *, pretty_print_html=False):
    if util.safe_exists(destination_file_path):
        if util.safe_isdir(destination_file_path):
            util.safe_rmtree(destination_file_path)
        else:
            util.safe_remove(destination_file_path)

    spy.workbooks.save(workbooks, destination_file_path, pretty_print_html=pretty_print_html)


def _delete_max_capsule_duration_on_bad_metric():
    with util.safe_open(os.path.join(_get_exports_folder(),
                                     'Bad Metric (0459C5F0-E5BD-491A-8DB7-BA4329E585E8)', 'Items.json'), 'r') as f:
        bad_metrics_items_json = json.load(f)

    del bad_metrics_items_json['1541C121-A38E-41C3-BFFA-AB01D0D0F30C']["MeasuredItemMaximumDuration"]

    del bad_metrics_items_json['1AA91F16-D476-4AF8-81AB-A2120FDA68E5']["Formula Parameters"][
        "Bounding Condition Maximum Duration"]

    with util.safe_open(os.path.join(_get_exports_folder(),
                                     'Bad Metric (0459C5F0-E5BD-491A-8DB7-BA4329E585E8)', 'Items.json'), 'w') as f:
        json.dump(bad_metrics_items_json, f, indent=4)


def _find_item(original_id, label):
    items_api = ItemsApi(spy.session.client)
    data_id = f'[{label}] {original_id}'
    _filters = [
        'Datasource Class==%s && Datasource ID==%s && Data ID==%s' % (
            _common.DEFAULT_DATASOURCE_CLASS, label, data_id),
        '@includeUnsearchable']

    search_results = items_api.search_items(
        filters=_filters,
        offset=0,
        limit=2)  # type: ItemSearchPreviewPaginatedListV1

    if len(search_results.items) == 0:
        return None

    if len(search_results.items) > 1:
        raise RuntimeError('Multiple items found with Data ID of "%s"', data_id)

    return search_results.items[0]


def _assert_explanation(actual: str, expected: str):
    expected = re.escape(textwrap.dedent(expected).strip())
    expected = expected.replace(r'\{\.\.\.\}', '.*')
    if re.match(expected, actual, re.DOTALL) is None:
        pytest.fail(f'Expected:\n\n{expected}\n\nActual:\n\n{actual}')


@pytest.mark.system
def test_example_export():
    workbooks = test_load.load_example_export()

    # Make sure the Topic is processed first, so that we test the logic that ensures all Topic dependencies are
    # pushed before the Topic is pushed. (Otherwise the IDs in the Topic will not be properly replaced.)
    reordered_workbooks = list()
    reordered_workbooks.extend(filter(lambda w: w['Workbook Type'] == 'Topic', workbooks))
    reordered_workbooks.extend(filter(lambda w: w['Workbook Type'] == 'Analysis', workbooks))

    assert isinstance(reordered_workbooks[0], Topic)
    assert isinstance(reordered_workbooks[1], Analysis)

    label = 'agent_api_key'  # We are testing the isolation by user
    status_df = spy.workbooks.push(reordered_workbooks, path='test_example_export', refresh=False,
                                   datasource=label, label=label).set_index('ID')

    assert status_df.spy.datasource.name == label
    analysis_result = status_df.loc['D833DC83-9A38-48DE-BF45-EB787E9E8375']['Result']
    assert 'Success' in analysis_result

    explanation = status_df.spy.item_map.explain('47D091C1-1780-4EA0-BD54-AA6E13213EB8')
    _assert_explanation(
        explanation,
        """
        No datasource map overrides found
        Item's ID 47D091C1-1780-4EA0-BD54-AA6E13213EB8 not found directly in target server
        Using non-overrides from {...}Example Analysis (D833DC83-9A38-48DE-BF45-EB787E9E8375):
        - Used "{...}Datasource_Map_Time Series CSV Files_Example Data_Example Data.json"
        - RegEx-Based Map 0: Successfully mapped. Details:
            "Type"
                regex          "(?<type>.*)"
                matched on     "StoredSignal"
                searched for   "Signal"
                and found      "StoredSignal"
            "Datasource Class"
                regex          "Time Series CSV Files"
                matched on     "Time Series CSV Files"
                searched for   "Time Series CSV Files"
                and found      "Time Series CSV Files"
            "Data ID"
                regex          "(?<data_id>.*)"
                matched on     "[Tag] Area A_Optimizer.sim.ts.csv"
                searched for   "[Tag] Area A_Optimizer.sim.ts.csv"
                and found      "[Tag] Area A_Optimizer.sim.ts.csv"
            "Datasource ID"
                searched for   "Example Data"
                and found      "Example Data"
            Capture groups:
                type           "StoredSignal"
                data_id        "[Tag] Area A_Optimizer.sim.ts.csv"
        Successful mapping:
          Old: StoredSignal "Area A_Optimizer" (47D091C1-1780-4EA0-BD54-AA6E13213EB8)
          New: StoredSignal "Area A_Optimizer" ({...})
        """
    )

    explanation = status_df.spy.item_map.explain('8A4F0E26-8A0C-4127-9E11-B67E031C6049')

    _assert_explanation(
        explanation,
        """
        No datasource map overrides found
        Item's ID 8A4F0E26-8A0C-4127-9E11-B67E031C6049 not found directly in target server
        Using non-overrides from {...}Example Analysis (D833DC83-9A38-48DE-BF45-EB787E9E8375):
        - Used "{...}Datasource_Map_Time Series CSV Files_Example Data_Example Data.json"
        - RegEx-Based Map 0: Successfully mapped. Details:
            "Type"
                regex          "(?<type>.*)"
                matched on     "StoredSignal"
                searched for   "Signal"
                and found      "StoredSignal"
            "Datasource Class"
                regex          "Time Series CSV Files"
                matched on     "Time Series CSV Files"
                searched for   "Time Series CSV Files"
                and found      "Time Series CSV Files"
            "Data ID"
                regex          "(?<data_id>.*)"
                matched on     "Area A_Temperature.sim.ts.csv"
                searched for   "Area A_Temperature.sim.ts.csv"
                and found      "Area A_Temperature.sim.ts.csv"
            "Datasource ID"
                searched for   "Example Data"
                and found      "Example Data"
            Capture groups:
                type           "StoredSignal"
                data_id        "Area A_Temperature.sim.ts.csv"
        Successful mapping:
          Old: StoredSignal "Example >> Cooling Tower 1 >> Area A >> Temperature" (8A4F0E26-8A0C-4127-9E11-B67E031C6049)
          New: StoredSignal "Example >> Cooling Tower 1 >> Area A >> Temperature" ({...})
        """
    )

    smooth_temperature_signal = _find_item('FBBCD4E0-CE26-4A33-BE59-3E215553FB1F', label)

    items_api = ItemsApi(spy.session.client)
    item_output = items_api.get_item_and_all_properties(id=smooth_temperature_signal.id)  # type: ItemOutputV1
    item_properties = {p.name: p.value for p in item_output.properties}

    # Make sure we don't change the Data ID format, since users will have pushed lots of items in this format.
    assert item_properties['Data ID'] == '[agent_api_key] FBBCD4E0-CE26-4A33-BE59-3E215553FB1F'

    assert 'UIConfig' in item_properties
    ui_config_properties_dict = json.loads(item_properties['UIConfig'])
    assert ui_config_properties_dict['type'] == 'low-pass-filter'

    high_power_condition = _find_item('8C048548-8E83-4380-8B24-9DAD56B5C2CF', label)

    item_output = items_api.get_item_and_all_properties(id=high_power_condition.id)  # type: ItemOutputV1
    item_properties = {p.name: p.value for p in item_output.properties}

    assert 'UIConfig' in item_properties
    ui_config_properties_dict = json.loads(item_properties['UIConfig'])
    assert ui_config_properties_dict['type'] == 'limits'


@pytest.mark.system
def test_push_repeatedly():
    workbooks = test_load.load_example_export()
    workbooks = [w for w in workbooks if isinstance(w, Analysis)]

    label = 'bad[label]bad'
    with pytest.raises(SPyValueError, match='label argument cannot contain square brackets'):
        spy.workbooks.push(workbooks, path=label, label=label, datasource=label, refresh=True)

    label = 'test_push_repeatedly'
    push1_df = spy.workbooks.push(workbooks, path=label, label=label, datasource=label, refresh=True)
    assert push1_df.iloc[0]['ID'] != push1_df.iloc[0]['Pushed Workbook ID'] == workbooks[0].id

    # CRAB-22768: Ensure that the Data property is not present
    assert 'Data' not in workbooks[0].definition

    # Now push without a label, because we want the push operation to locate already-pushed items by ID
    push2_df = spy.workbooks.push(workbooks, path=label, datasource=label, refresh=False)
    assert push2_df.iloc[0]['ID'] == push2_df.iloc[0]['Pushed Workbook ID'] == workbooks[0].id

    push3_df = spy.workbooks.push(workbooks, datasource=label, use_full_path=True)
    assert push3_df.iloc[0]['ID'] == push3_df.iloc[0]['Pushed Workbook ID'] == workbooks[0].id

    # CRAB-22768: See above
    assert 'Data' not in workbooks[0].definition

    push4_df = spy.workbooks.push(workbooks, datasource=label, use_full_path=True, refresh=False)
    assert push4_df.iloc[0]['ID'] == push4_df.iloc[0]['Pushed Workbook ID'] == workbooks[0].id

    # CRAB-27080: Push to Corporate folder repeatedly
    push_corp1_df = spy.workbooks.push(workbooks, datasource=label,
                                       path=f'{spy.workbooks.CORPORATE} >> test_push_repeatedly_corporate')
    assert push_corp1_df.iloc[0]['ID'] == push_corp1_df.iloc[0]['Pushed Workbook ID'] == workbooks[0].id

    push_corp2_df = spy.workbooks.push(workbooks, datasource=label,
                                       path=f'{spy.workbooks.CORPORATE} >> test_push_repeatedly_corporate')
    assert push_corp2_df.iloc[0]['ID'] == push_corp2_df.iloc[0]['Pushed Workbook ID'] == workbooks[0].id

    push_corp3_df = spy.workbooks.push(workbooks, datasource=label,
                                       path=f'{spy.workbooks.CORPORATE} >> test_push_repeatedly_corporate')
    assert push_corp3_df.iloc[0]['ID'] == push_corp3_df.iloc[0]['Pushed Workbook ID'] == workbooks[0].id


@pytest.mark.system
def test_push_with_inventory_modifications():
    workbooks = test_load.load_example_export()
    workbook: Analysis = [w for w in workbooks if isinstance(w, Analysis)][0]

    label = 'test_push_with_inventory_modifications'
    push1_df = spy.workbooks.push(workbook, path=label, label=label, datasource=label, refresh=True)
    assert push1_df.iloc[0]['ID'] != push1_df.iloc[0]['Pushed Workbook ID'] == workbook.id

    item_inventory_df = workbook.item_inventory_df()
    daily_condition_id = item_inventory_df[item_inventory_df['Name'] == 'Daily'].iloc[0]['ID']
    daily_condition_item = workbook.item_inventory[daily_condition_id]
    daily_condition_item['Name'] = 'Weeks'
    daily_condition_item['Formula'] = 'weeks()'

    spy.workbooks.push(workbook, path=label, label=label, datasource=label)
    daily_condition_item = Item.pull(daily_condition_id)
    assert daily_condition_item['Name'] == 'Weeks'
    assert daily_condition_item['Formula'] == 'weeks()'

    # Make a duplicate and make sure we get an error if we use reconcile_inventory_by='name'
    daily_condition_item['ID'] = _common.new_placeholder_guid()
    workbook.add_to_inventory(daily_condition_item)

    with pytest.raises(SPyRuntimeError, match='Data ID.*collision'):
        spy.workbooks.push(workbook, path=label, label=label, datasource=label, reconcile_inventory_by='name')

    pushed_df = spy.workbooks.push(workbook, path=label, label=label, datasource=label, reconcile_inventory_by='name',
                                   errors='catalog')

    push_result_str = pushed_df.spy.status.df.iloc[0]['Result']
    assert re.match(r'.*Data ID.*collision.*', push_result_str, re.DOTALL) is not None


@pytest.mark.system
def test_workbook_paths():
    workbooks = test_load.load_example_export()

    label = 'test_workbook_paths'

    # This call will put the folder of workbooks ('Example Export') in a top-level 'Use Full Path Folder'
    status_df = spy.workbooks.push(
        workbooks, label=label, datasource=label, path='Use Full Path Folder', use_full_path=True,
        refresh=False).set_index('ID')
    analysis_result = status_df.loc['D833DC83-9A38-48DE-BF45-EB787E9E8375']['Result']
    assert 'Success' in analysis_result

    workbooks_df = spy.workbooks.search({
        'Path': 'Use Full Path Folder >> Example Export'
    })
    assert len(workbooks_df) == 2

    # This call will effectively move the folder of workbooks ('Example Export') to the user's home folder and clean
    # out the 'Use Full Path Folder'. Note that prior calls need to have refresh=False.
    status_df = spy.workbooks.push(
        workbooks, label=label, datasource=label, path=_common.PATH_ROOT, use_full_path=True,
        refresh=False).set_index('ID')
    analysis_result = status_df.loc['D833DC83-9A38-48DE-BF45-EB787E9E8375']['Result']
    assert 'Success' in analysis_result

    workbooks_df = spy.workbooks.search({
        'Path': 'Use Full Path Folder'
    })
    assert len(workbooks_df) == 0

    workbooks_df = spy.workbooks.search({
        'Path': 'Example Export'
    })
    assert len(workbooks_df) == 2

    # This call will not move the workbooks out of the 'Example Export' folder, because the 'Search Folder ID' property
    # in the workbook gives them a no-op "relative path" such that they will be put in the folder specified in the
    # spy.workbooks.push(path='<path>') argument. Since a zero-length path argument is specified here, they will not
    # be moved.
    status_df = spy.workbooks.push(workbooks, label=label, datasource=label, path='', refresh=False).set_index('ID')
    analysis_result = status_df.loc['D833DC83-9A38-48DE-BF45-EB787E9E8375']['Result']
    assert 'Success' in analysis_result

    workbooks_df = spy.workbooks.search({
        'Path': 'Example Export'
    })
    assert len(workbooks_df) == 2

    workbooks_df = spy.workbooks.search({
        'Name': '/Example (?:Analysis|Topic)/'
    })
    assert len(workbooks_df) == 0

    # Remove the "Search Folder ID" so that the workbooks have an "absolute path"
    for workbook in workbooks:
        del workbook['Search Folder ID']

    # This call will once again put the workbooks in the 'Example Export' folder, using the "absolute path" mentioned
    # above.
    status_df = spy.workbooks.push(workbooks, label=label, datasource=label, path='', refresh=False).set_index('ID')
    analysis_result = status_df.loc['D833DC83-9A38-48DE-BF45-EB787E9E8375']['Result']
    assert 'Success' in analysis_result

    workbooks_df = spy.workbooks.search({
        'Path': 'Example Export'
    })
    assert len(workbooks_df) == 2

    workbooks_df = spy.workbooks.search({
        'Name': '/Example (?:Analysis|Topic)/'
    })
    assert len(workbooks_df) == 0


@pytest.mark.system
def test_workbook_path_partial_match():
    # Create a workbook in a specific folder with multiple levels
    name = f'test_workbook_path_partial_match_{_common.new_placeholder_guid()}'
    path_long = f'{spy.workbooks.CORPORATE} >> ABC >> DEF >> {name}'
    workbook_long_path = Analysis(name)
    workbook_long_path.worksheet(name)
    push_results_long = spy.workbooks.push(workbook_long_path, path=path_long)
    assert len(push_results_long) == 1
    assert push_results_long.iloc[0]['Result'] == 'Success'
    assert workbook_long_path.path == path_long

    # Create another workbook that is in a folder with the same name, but a different path. This should result in
    # two different workbooks.
    workbook_short_path = Analysis(name)
    workbook_short_path.worksheet(name)
    path_short = f'{spy.workbooks.CORPORATE} >> {name}'
    push_results_short = spy.workbooks.push(workbook_short_path, path=path_short)
    assert len(push_results_short) == 1
    assert push_results_short.iloc[0]['Result'] == 'Success'
    assert workbook_short_path.path == path_short
    assert workbook_short_path['Ancestors'] != workbook_long_path['Ancestors']
    assert push_results_short.iloc[0]['Pushed Workbook ID'] != push_results_long.iloc[0]['ID']

    # Push to the same workbooks using spy.push(metadata) to ensure it resolves consistently
    metadata_push_results_long = spy.push(metadata=pd.DataFrame([{'Name': name, 'Type': 'Signal'}]),
                                          workbook=f'{path_long} >> {name}', worksheet=name)
    assert metadata_push_results_long.spy.workbook_id == push_results_long.iloc[0]['Pushed Workbook ID']

    metadata_push_results_short = spy.push(metadata=pd.DataFrame([{'Name': name, 'Type': 'Signal'}]),
                                           workbook=f'{path_short} >> {name}', worksheet=name)
    assert metadata_push_results_short.spy.workbook_id == push_results_short.iloc[0]['Pushed Workbook ID']


@pytest.mark.system
def test_original_folder():
    test_name = f'test_original_folder {_common.new_placeholder_guid()}'
    workbook_name = f'{test_name} Workbook'
    workbook = Analysis(workbook_name)
    workbook.worksheet('The Worksheet')

    subfolder = f'{test_name} Subfolder'

    # push to corporate, search with different root directories and matching and non-matching content_filters
    pushed_df = spy.workbooks.push(workbook, path=f'{spy.workbooks.CORPORATE} >> {subfolder}')
    spy.acl.push(pushed_df.iloc[0]['Pushed Workbook ID'], {'Name': 'Everyone', 'Manage': True})
    assert workbook['Ancestors'][0] == spy.workbooks.CORPORATE

    # Push using Ren's account
    spy.workbooks.push(workbook, path=subfolder, session=test_common.get_session(Sessions.ren))

    # Ren can find it
    assert len(spy.workbooks.search({'Name': workbook_name}, recursive=True,
                                    session=test_common.get_session(Sessions.ren))) == 1

    # Stimpy's account won't find it
    assert len(spy.workbooks.search({'Name': workbook_name}, recursive=True,
                                    session=test_common.get_session(Sessions.stimpy))) == 0

    # Admin account finds it
    admin_search_df = spy.workbooks.search(
        {'Name': workbook_name}, recursive=True, content_filter='all',
        session=test_common.get_session(Sessions.admin))

    assert len(admin_search_df) == 1

    admin_pulled_workbooks = spy.workbooks.pull(admin_search_df, session=test_common.get_session(Sessions.admin))
    assert len(admin_pulled_workbooks) == 1

    admin_pulled_workbook = admin_pulled_workbooks[0]

    # Make a second workbook (that doesn't exist yet) that simulates having come from a different server
    second_workbook = Analysis(f'{test_name} Second Workbook', provenance=Item.PULL)
    second_workbook.worksheet('The Worksheet')
    second_workbook['Ancestors'] = admin_pulled_workbook['Ancestors']
    second_workbook['Owner'] = admin_pulled_workbook['Owner']
    second_workbook._item_inventory = admin_pulled_workbook.item_inventory
    admin_pulled_workbooks.append(second_workbook)

    if not spy.utils.is_sdk_module_version_at_least(64):
        # We don't support ORIGINAL_FOLDER / ORIGINAL_OWNER well enough in R63 for the remaining test cases to
        # actually work
        return

    spy.workbooks.push(admin_pulled_workbooks, path=spy.workbooks.ORIGINAL_FOLDER,
                       owner=spy.workbooks.ORIGINAL_OWNER, use_full_path=True,
                       session=test_common.get_session(Sessions.admin))

    for admin_pulled_workbook in admin_pulled_workbooks:
        assert admin_pulled_workbook.fqn.startswith(f'Users >> Ren Hoek >> {subfolder} >> {test_name}')

    spy.acl.push(second_workbook, {'Name': 'Everyone', 'Read': True, 'Write': True, 'Manage': True},
                 session=test_common.get_session(Sessions.admin))

    # Pull with Stimpy's account
    stimpy_pulled_workbooks = spy.workbooks.pull(second_workbook.id, errors='catalog', session=test_common.get_session(
        Sessions.stimpy))
    assert len(stimpy_pulled_workbooks) == 1
    assert stimpy_pulled_workbooks[0]['Ancestors'][0] == '__Shared__'
    # We don't have access to this folder, so it won't be in the inventory
    assert stimpy_pulled_workbooks[0]['Ancestors'][1] not in stimpy_pulled_workbooks[0].item_inventory
    assert stimpy_pulled_workbooks[0].fqn == f'Shared >> {subfolder} >> {test_name} Second Workbook'

    with pytest.raises(SPyValueError, match='Must be an admin'):
        spy.workbooks.push(stimpy_pulled_workbooks, path=spy.workbooks.ORIGINAL_FOLDER,
                           use_full_path=True, session=test_common.get_session(Sessions.stimpy))

    spy.workbooks.push(stimpy_pulled_workbooks, use_full_path=True, refresh=False, errors='catalog',
                       session=test_common.get_session(Sessions.stimpy))

    assert stimpy_pulled_workbooks[0].fqn == f'Shared >> {subfolder} >> {test_name} Second Workbook'


@pytest.mark.system
def test_workbook_path_with_folder_id():
    folders_api = FoldersApi(spy.session.client)
    folder_name = f'test_workbook_path_with_folder_id_Folder_{_common.new_placeholder_guid()}'
    folder_output = folders_api.create_folder(body=FolderInputV1(
        name=folder_name))

    workbook_name = f'test_workbook_path_with_folder_id_Analysis_{_common.new_placeholder_guid()}'
    workbook = Analysis(workbook_name)
    workbook.worksheet('The Worksheet')

    # First push it to the root of My Items
    spy.workbooks.push(workbook)

    # Make sure it's there
    search_df = spy.workbooks.search({
        'Name': workbook_name
    }, recursive=False)
    assert len(search_df) == 1

    # Now move it to the folder
    spy.workbooks.push(workbook, path=folder_output.id)

    # Make sure it's no longer in the root of My Items
    search_df = spy.workbooks.search({
        'Name': workbook_name
    }, recursive=False)
    assert len(search_df) == 0

    # Make sure it's in the folder
    search_df = spy.workbooks.search({
        'Path': folder_name,
        'Name': workbook_name
    })
    assert len(search_df) == 1


@pytest.mark.system
def test_owner():
    workbook_name = str(uuid.uuid4())
    workbook = Analysis({
        'Name': workbook_name
    })
    workbook.worksheet('one_and_only')
    workbooks = [workbook]

    def _confirm(username, in_my_folder, session):
        assert workbook['Owner']['Username'] == username
        search_df = spy.workbooks.search({
            'Name': workbook_name
        }, content_filter='owner', session=session)
        assert len(search_df) == (1 if in_my_folder else 0)

    push_df1 = spy.workbooks.push(workbooks)
    _confirm(spy.user.username, True, spy.session)

    admin_session = test_common.get_session(Sessions.admin)
    admin_user = admin_session.user
    nonadmin_user = test_common.get_session(Sessions.nonadmin).user

    with pytest.raises(RuntimeError, match='User "agent_api_key" not found'):
        # agent_api_key is not discoverable via user search, it is purposefully hidden
        spy.workbooks.push(workbooks, refresh=False, owner='agent_api_key', session=admin_session)

    push_df2 = spy.workbooks.push(workbooks, owner=nonadmin_user.id, session=admin_session)
    _confirm(nonadmin_user.username, False, admin_session)

    push_df3 = spy.workbooks.push(workbooks, owner=spy.workbooks.FORCE_ME_AS_OWNER, session=admin_session)
    _confirm(admin_user.username, True, admin_session)

    push_df4 = spy.workbooks.push(workbooks, owner=nonadmin_user.username, session=admin_session)
    _confirm(nonadmin_user.username, False, admin_session)

    assert push_df1.iloc[0]['Pushed Workbook ID'] == push_df2.iloc[0]['Pushed Workbook ID'] == push_df3.iloc[0][
        'Pushed Workbook ID'] == push_df4.iloc[0]['Pushed Workbook ID']

    with pytest.raises(RuntimeError):
        spy.workbooks.push(workbooks, refresh=False, owner='non_existent_user', session=admin_session)


@pytest.mark.system
def test_worksheet_order():
    workbooks = spy.workbooks.load(get_full_path_of_export('Worksheet Order (2BBDCFA7-D25C-4278-922E-D99C8DBF6582)'))

    label = 'test_worksheet_order'
    spy.workbooks.push(workbooks, path=label, label=label, datasource=label, refresh=False)
    workbook_item = _find_item('2BBDCFA7-D25C-4278-922E-D99C8DBF6582', label)

    pushed_worksheet_names = [
        '1',
        '2',
        '3'
    ]

    workbooks_api = WorkbooksApi(spy.session.client)
    worksheet_output_list = workbooks_api.get_worksheets(workbook_id=workbook_item.id)  # type: WorksheetOutputListV1
    assert len(worksheet_output_list.worksheets) == 3
    assert [w.name for w in worksheet_output_list.worksheets] == pushed_worksheet_names

    workbooks[0].worksheets = list(reversed(workbooks[0].worksheets))
    spy.workbooks.push(workbooks, path=label, label=label, datasource=label, refresh=False)
    worksheet_output_list = workbooks_api.get_worksheets(workbook_id=workbook_item.id)  # type: WorksheetOutputListV1
    assert len(worksheet_output_list.worksheets) == 3
    assert [w.name for w in worksheet_output_list.worksheets] == list(reversed(pushed_worksheet_names))

    workbooks[0].worksheets = list(filter(lambda w: w.id != '2BEC414E-2F58-45A0-83A6-AAB098812D38',
                                          reversed(workbooks[0].worksheets)))
    pushed_worksheet_names.remove('3')
    spy.workbooks.push(workbooks, path=label, label=label, datasource=label, refresh=False)
    worksheet_output_list = workbooks_api.get_worksheets(workbook_id=workbook_item.id)  # type: WorksheetOutputListV1
    assert len(worksheet_output_list.worksheets) == 2
    assert [w.name for w in worksheet_output_list.worksheets] == pushed_worksheet_names


@pytest.mark.system
def test_missing_worksteps():
    with tempfile.TemporaryDirectory() as temp_folder:
        missing_worksteps_folder = os.path.join(temp_folder, 'Missing Worksteps')
        if util.safe_exists(missing_worksteps_folder):
            util.safe_rmtree(missing_worksteps_folder)
        with zipfile.ZipFile(test_load.get_example_export_path(), 'r') as zip_ref:
            zip_ref.extractall(missing_worksteps_folder)

        # Removing this workstep will cause an error because it is referenced in the Example Topic document
        util.safe_remove(os.path.join(
            missing_worksteps_folder,
            'Example Analysis (D833DC83-9A38-48DE-BF45-EB787E9E8375)',
            'Worksheet_1F02C6C7-5009-4A13-9343-CDDEBB6AF7E6_Workstep_221933FE-7956-4888-A3C9-AF1F3971EBA5.json'))

        # Removing this workstep will cause an error because it is referenced in an Example Analysis journal
        util.safe_remove(os.path.join(
            missing_worksteps_folder,
            'Example Analysis (D833DC83-9A38-48DE-BF45-EB787E9E8375)',
            'Worksheet_10198C29-C93C-4055-B313-3388227D0621_Workstep_FD90346A-BF72-4319-9134-3922A012C0DB.json'))

        workbooks = spy.workbooks.load(missing_worksteps_folder)
        topic = [w for w in workbooks if w['Workbook Type'] == 'Topic'][0]
        for worksheet in topic.worksheets:
            if worksheet.name == 'Static Doc':
                fields = {'Name': f'content_1',
                          'Width': 1,
                          'Height': 1,
                          'Worksheet ID': '1F02C6C7-5009-4A13-9343-CDDEBB6AF7E6',
                          'Workstep ID': '221933FE-7956-4888-A3C9-AF1F3971EBA5',
                          'Workbook ID': 'D833DC83-9A38-48DE-BF45-EB787E9E8375'}
                content_1 = Content(fields, worksheet.report)

            worksheet.report.content = {'content_1': content_1}

        label = 'test_missing_worksteps'
        push_df = spy.workbooks.push(workbooks, path=label, label=label, datasource=label, refresh=False,
                                     errors='catalog')

        topic_row = push_df[push_df['Name'] == 'Example Topic'].iloc[0]
        analysis_row = push_df[push_df['Name'] == 'Example Analysis'].iloc[0]

        assert '221933FE-7956-4888-A3C9-AF1F3971EBA5' in topic_row['Result']
        assert 'FD90346A-BF72-4319-9134-3922A012C0DB' in analysis_row.loc['Result']


@pytest.mark.system
def test_bad_workstep():
    test_name = 'test_bad_workstep'
    workbooks = spy.workbooks.load(get_full_path_of_export('Bad Metric (0459C5F0-E5BD-491A-8DB7-BA4329E585E8)'))
    worksheet = workbooks[0].worksheets[0]
    current_workstep = worksheet.worksteps[worksheet.definition['Current Workstep ID']]

    area_a_temperature = spy.search({'Datasource Name': 'Example Data', 'Name': 'Area A_Temperature'})
    duplicate_item_workstep = current_workstep
    duplicate_item_workstep.display_items = area_a_temperature
    duplicate_item_workstep.data['state']['stores']['sqTrendSeriesStore']['items'].append({
        'name': area_a_temperature.iloc[0]['Name'],
        'id': area_a_temperature.iloc[0]['ID']
    })

    # The de-duplication logic doesn't kick in until we push
    assert len(duplicate_item_workstep.display_items) == 2

    # Prior to fixing CRAB-25915, this would produce an error... now it will just drop the duplicates
    spy.workbooks.push(workbooks, label=test_name, datasource=test_name)

    bad_item_workstep = current_workstep
    bad_item_workstep.data['state']['stores']['sqTrendSeriesStore']['items'] = [{
        'name': 'id? What id?'
    }]
    assert bad_item_workstep.display_items.empty

    no_data_workstep = current_workstep
    no_data_workstep.definition['Data'] = None
    assert no_data_workstep.display_items.empty


@pytest.mark.system
def test_bad_metric():
    label = 'test_bad_metric'
    _load_and_push('Bad Metric (0459C5F0-E5BD-491A-8DB7-BA4329E585E8)', label)

    metrics_api = MetricsApi(spy.session.client)

    # To see the code that this exercises, search for test_bad_metric in _metadata.py
    metric_item = _find_item('1AA91F16-D476-4AF8-81AB-A2120FDA68E5', label)
    threshold_metric_output = metrics_api.get_metric(id=metric_item.id)  # type: ThresholdMetricOutputV1
    assert threshold_metric_output.bounding_condition_maximum_duration.value == 40
    assert threshold_metric_output.bounding_condition_maximum_duration.uom == 'h'


def _find_worksheet(workbook_id, worksheet_name, is_archived=False):
    workbooks_api = WorkbooksApi(spy.session.client)
    worksheet_output_list = workbooks_api.get_worksheets(
        workbook_id=workbook_id, is_archived=is_archived)  # type: WorksheetOutputListV1

    return [w for w in worksheet_output_list.worksheets if w.name == worksheet_name][0]


@pytest.mark.system
def test_archived_worksheets():
    workbooks = list()
    workbooks.extend(spy.workbooks.load(get_full_path_of_export(
        'Archived Worksheet - Topic (F662395E-FEBB-4772-8B3B-B2D7EB7C0C3B)')))
    workbooks.extend(spy.workbooks.load(get_full_path_of_export(
        'Archived Worksheet - Analysis (DDB5F823-3B6A-42DC-9C44-566466C2BA82)')))

    label = 'test_archived_worksheets'
    push_df = spy.workbooks.push(workbooks, path=label, label=label, datasource=label, refresh=False)

    analysis_workbook_id = push_df[push_df['ID'] == 'DDB5F823-3B6A-42DC-9C44-566466C2BA82'] \
        .iloc[0]['Pushed Workbook ID']

    archived_worksheet = _find_worksheet(analysis_workbook_id, 'Archived', is_archived=True)

    items_api = ItemsApi(spy.session.client)
    assert items_api.get_property(id=archived_worksheet.id, property_name='Archived').value


@pytest.mark.system
def test_images():
    label = 'test_images'
    pushed_workbook_id = _load_and_push('Images (130FF777-26B3-4A2D-BA95-0AFE7A2CA946)', label)

    image_worksheet = _find_worksheet(pushed_workbook_id, 'Main')

    doc = _get_journal_html(image_worksheet.id)

    assert doc.find('/api/annotations/A3757559-163D-4DDF-81EE-043B61332B12/images/1573580600045_v1.png') == -1

    match = re.match(r'.*src="/api(.*?)".*', doc, re.DOTALL)

    assert match is not None

    api_client_url = spy.session.get_api_url()
    request_url = api_client_url + match.group(1)
    response = requests.get(request_url, headers={
        "Accept": "application/vnd.seeq.v1+json",
        "x-sq-auth": spy.session.client.auth_token
    }, verify=Configuration().verify_ssl)

    with util.safe_open(os.path.join(get_full_path_of_export('Images (130FF777-26B3-4A2D-BA95-0AFE7A2CA946)'),
                                     'Image_A3757559-163D-4DDF-81EE-043B61332B12_1573580600045_v1.png'), 'rb') as f:
        expected_content = f.read()

    assert response.content == expected_content


@pytest.mark.system
def test_copied_workbook_with_journal():
    label = 'test_copied_workbook_with_journal'
    workbook_id = _load_and_push('Journal - Copy (3D952B33-70A7-460B-B71C-E2380EDBAA0A)', label)

    copied_worksheet = _find_worksheet(workbook_id, 'Main')

    doc = _get_journal_html(copied_worksheet.id)

    # We should not find mention of the "original" workbook/worksheet IDs. See _workbook.Annotation.push() for the
    # relevant code that fixes this stuff up.
    assert doc.find('1C5F8E9D-93E5-4C38-B4C6-4DBDBB4CF3D2') == -1
    assert doc.find('35D190B1-6AD7-4DEA-B8B7-178EBA2AFBAC') == -1

    _verify_workstep_links(copied_worksheet.id)

    duplicated_worksheet = _find_worksheet(workbook_id, 'Main - Duplicated')
    _verify_workstep_links(duplicated_worksheet.id)

    copy_and_paste_worksheet = _find_worksheet(workbook_id, 'Copy and Paste')
    _verify_workstep_links(copy_and_paste_worksheet.id)


def _verify_workstep_links(worksheet_id):
    doc = _get_journal_html(worksheet_id)

    workbooks_api = WorkbooksApi(spy.session.client)
    for match in re.finditer(_common.WORKSTEP_LINK_REGEX, doc):
        # Make sure we don't get a 404
        workbooks_api.get_workstep(workbook_id=match.group(1),
                                   worksheet_id=match.group(2),
                                   workstep_id=match.group(3))


def _get_journal_html(worksheet_id):
    annotations_api = AnnotationsApi(spy.session.client)
    annotations = annotations_api.get_annotations(
        annotates=[worksheet_id])  # type: AnnotationListOutputV1
    journal_annotations = [a for a in annotations.items if a.type == 'Journal']
    assert len(journal_annotations) == 1
    annotation_output = annotations_api.get_annotation(id=journal_annotations[0].id)  # AnnotationOutputV1
    return annotation_output.document


@pytest.mark.system
def test_topic_links():
    # Log in slightly differently so that the URLs change
    test_common.log_in_default_user('http://127.0.0.1:34216')

    workbooks = list()
    workbooks.extend(spy.workbooks.load(get_full_path_of_export(
        'Referenced By Link - Topic (1D589AC0-CA54-448D-AC3F-B3C317F7C195)')))
    workbooks.extend(spy.workbooks.load(get_full_path_of_export(
        'Referenced By Link - Analysis (3C71C580-F1FA-47DF-B953-4646D0B1F98F)')))

    label = 'test_topic_links'
    push_df = spy.workbooks.push(workbooks, path=label, label=label, datasource=label, refresh=False)

    analysis_workbook_id = push_df[push_df['ID'] == '1D589AC0-CA54-448D-AC3F-B3C317F7C195'] \
        .iloc[0]['Pushed Workbook ID']

    document_worksheet = _find_worksheet(analysis_workbook_id, 'Only Document')

    annotations_api = AnnotationsApi(spy.session.client)

    annotations = annotations_api.get_annotations(
        annotates=[document_worksheet.id])  # type: AnnotationListOutputV1

    report_annotations = [a for a in annotations.items if a.type == 'Report']
    assert len(report_annotations) == 1

    annotation_output = annotations_api.get_annotation(id=report_annotations[0].id)  # AnnotationOutputV1

    assert annotation_output.document.find('http://localhost') == -1

    test_common.log_in_default_user()


@pytest.mark.system
def test_workbook_to_workbook_links():
    test_file_folder = os.path.dirname(__file__)
    workbooks = spy.workbooks.load(os.path.join(test_file_folder, 'Workbook Link Fixups Tests.zip'))

    analysis_html = workbooks['An Analysis With Links'].worksheets['A Journal with Links'].journal.html
    topic_html = workbooks['A Topic with Links'].worksheets['A Document with Links'].report.html

    # Check to make sure it loaded and has the links we expect
    assert 'http://theservername' in analysis_html
    assert 'http://theservername' in topic_html

    label = 'test_workbook_to_workbook_links'
    spy.workbooks.push(workbooks, path=label, label=label, datasource=label)

    analysis_html = workbooks['An Analysis With Links'].worksheets['A Journal with Links'].journal.html
    topic_html = workbooks['A Topic with Links'].worksheets['A Document with Links'].report.html

    assert 'http://theservername' not in analysis_html
    assert 'http://theservername' not in topic_html

    search_df = spy.workbooks.search({'Path': label})
    pushed_workbooks = spy.workbooks.pull(search_df)

    analysis_html = pushed_workbooks['An Analysis With Links'].worksheets['A Journal with Links'].journal.html
    topic_html = pushed_workbooks['A Topic with Links'].worksheets['A Document with Links'].report.html

    assert 'http://theservername' not in analysis_html
    assert 'http://theservername' not in topic_html


@pytest.mark.system
def test_replace_acl():
    workbooks = spy.workbooks.load(get_full_path_of_export(
        'ACL Test (FF092494-FB04-4578-A12E-249417D93125)'))

    label = 'test_replace_acl'

    # First we'll push with acls='replace,loose', which will work but won't push all the ACLs
    push_df = spy.workbooks.push(workbooks, path=label, label=label, datasource=label, refresh=False,
                                 use_full_path=True, access_control='replace,loose')
    assert len(push_df) == 1
    assert push_df.iloc[0]['Result'] == 'Success'

    acl_test_workbook = _find_item('FF092494-FB04-4578-A12E-249417D93125', label)
    acl_test_folder = _find_item('6C513058-C1DA-4603-9498-75492B9BC119', label)

    items_api = ItemsApi(spy.session.client)

    def _assert_acl_entry(_acl_output, name, _type, has_origin, role, read, write, manage):
        matches = [e for e in _acl_output.entries if
                   (e.identity.username == name if _type == 'User' else e.identity.name == name) and
                   e.identity.type == _type and
                   e.role == role and
                   ((e.origin is not None) if has_origin else (e.origin is None)) and
                   e.permissions.read == read and
                   e.permissions.write == write and
                   e.permissions.manage == manage]

        assert len(matches) == 1

    def _confirm_loose():
        _acl_output = items_api.get_access_control(id=acl_test_workbook.id)  # type: AclOutputV1
        assert len(_acl_output.entries) == 3
        _assert_acl_entry(_acl_output, 'agent_api_key', 'User', has_origin=False, role='OWNER',
                          read=True, write=True, manage=True)
        _assert_acl_entry(_acl_output, 'agent_api_key', 'User', has_origin=True, role=None,
                          read=True, write=True, manage=True)
        _assert_acl_entry(_acl_output, 'Everyone', 'UserGroup', has_origin=True, role=None,
                          read=True, write=False, manage=False)

        _acl_output = items_api.get_access_control(id=acl_test_folder.id)  # type: AclOutputV1
        assert len(_acl_output.entries) == 3
        _assert_acl_entry(_acl_output, 'agent_api_key', 'User', has_origin=False, role='OWNER',
                          read=True, write=True, manage=True)
        _assert_acl_entry(_acl_output, 'Everyone', 'UserGroup', has_origin=False, role=None,
                          read=True, write=False, manage=False)

    _confirm_loose()

    # Next we'll push with access_control='add,loose' and confirm that duplicate ACLs are not created
    push_df = spy.workbooks.push(workbooks, path=label, label=label, datasource=label, refresh=False,
                                 use_full_path=True, access_control='add,loose')
    assert len(push_df) == 1
    assert push_df.iloc[0]['Result'] == 'Success'

    _confirm_loose()

    with pytest.raises(_common.SPyRuntimeError, match='.*"Just Mark".*not successfully mapped.*'):
        # Now we'll try access_control='replace,strict' which won't work because we don't know how to map the
        # "Just Mark" group or the "mark.derbecker@seeq.com" user
        spy.workbooks.push(workbooks, path=label, label=label, datasource=label, refresh=False, use_full_path=True,
                           access_control='replace,strict')

    # Now we'll try access_control='replace,strict' again but this time provide a map that will convert the group and
    # user to the built-in Everyone and Agent API Key
    with tempfile.TemporaryDirectory() as temp:
        datasource_map = {
            "Datasource Class": "Auth",
            "Datasource ID": "Seeq",
            "Datasource Name": "Seeq",
            "RegEx-Based Maps": [
                {
                    "Old": {
                        "Type": "User",
                    },
                    "New": {
                        "Type": "User",
                        "Datasource Class": "Auth",
                        "Datasource ID": "Seeq",
                        "Username": "agent_api_key"
                    }
                },
                {
                    "Old": {
                        "Type": "UserGroup",
                    },
                    "New": {
                        "Type": "UserGroup",
                        "Datasource Class": "Auth",
                        "Datasource ID": "Seeq",
                        "Name": "Everyone"
                    }
                }
            ]
        }

        with util.safe_open(os.path.join(temp, 'Datasource_Map_Auth_Seeq_Seeq.json'), 'w') as f:
            json.dump(datasource_map, f)

        spy.workbooks.push(workbooks, path=label, label=label, datasource=label, refresh=False, use_full_path=True,
                           access_control='replace,strict', datasource_map_folder=temp)

    push_df = spy.workbooks.push(workbooks, path=label, label=label, datasource=label, refresh=False,
                                 use_full_path=True, access_control='replace,loose')
    assert len(push_df) == 1
    assert push_df.iloc[0]['Result'] == 'Success'

    acl_output = items_api.get_access_control(id=acl_test_workbook.id)  # type: AclOutputV1
    assert len(acl_output.entries) == 4
    _assert_acl_entry(acl_output, 'agent_api_key', 'User', has_origin=False, role='OWNER',
                      read=True, write=True, manage=True)
    _assert_acl_entry(acl_output, 'agent_api_key', 'User', has_origin=True, role=None,
                      read=True, write=True, manage=True)
    _assert_acl_entry(acl_output, 'Everyone', 'UserGroup', has_origin=False, role=None,
                      read=True, write=False, manage=False)
    _assert_acl_entry(acl_output, 'Everyone', 'UserGroup', has_origin=True, role=None,
                      read=True, write=True, manage=True)

    acl_output = items_api.get_access_control(id=acl_test_folder.id)  # type: AclOutputV1
    assert len(acl_output.entries) == 3
    _assert_acl_entry(acl_output, 'agent_api_key', 'User', has_origin=False, role='OWNER',
                      read=True, write=True, manage=True)
    _assert_acl_entry(acl_output, 'Everyone', 'UserGroup', has_origin=False, role=None,
                      read=True, write=True, manage=True)


@pytest.mark.system
def test_tree_items():
    # If a CalculatedItem is part of a tree, then it is most likely that we want to find it using the datasource map
    # as opposed to creating a standalone CalculatedItem that is not in the tree. In other words, you expect that
    # when you push a workbook that has items from the TreeFileConnector, the worksheets will reference those items
    # and not some new CalculatedSignal that has no asset.

    tests_folder = os.path.dirname(__file__)
    mydata_trees_folder = os.path.join(test_common.get_test_data_folder(), 'mydata', 'trees')
    connector_config_folder = os.path.join(test_common.get_test_data_folder(), 'configuration', 'link')

    # Copy over the Tree File Connector stuff so that it gets indexed
    util.safe_copy(os.path.join(tests_folder, 'tree1.csv'), mydata_trees_folder)
    util.safe_copy(os.path.join(tests_folder, 'tree2.csv'), mydata_trees_folder)
    util.safe_copy(os.path.join(tests_folder, 'Tree File Connector.json'), connector_config_folder)

    assert test_common.wait_for(lambda: test_common.is_jvm_agent_connection_indexed(spy.session, 'mydata\\trees'))

    example_signals = spy.search({
        'Datasource Name': 'Example Data',
        'Name': 'Area ?_*',
        'Type': 'StoredSignal'
    }, workbook=spy.GLOBALS_ONLY)

    metadata_df = pd.DataFrame()

    metadata_df['ID'] = example_signals['ID']
    metadata_df['Type'] = example_signals['Type']
    metadata_df['Path'] = 'test_item_references'
    metadata_df['Asset'] = example_signals['Name'].str.extract(r'(.*)_.*')
    metadata_df['Name'] = example_signals['Name'].str.extract(r'.*_(.*)')
    metadata_df['Reference'] = True

    data_lab_items_df = spy.push(metadata=metadata_df, workbook=None)

    tree_file_items_df = spy.search({
        'Path': 'Tree 1 >> Cooling Tower - Area A',
        'Name': 'Compressor'
    })
    assert len(tree_file_items_df) == 2

    workbooks = spy.workbooks.load(get_full_path_of_export(
        'Item References (23DC9E6A-FCC3-456E-9A58-62D5CFF05816)'))

    pushed_df = spy.workbooks.push(workbooks, refresh=False)
    search_df = spy.workbooks.search({
        'Name': 'Item References'
    })
    pushed_workbooks = spy.workbooks.pull(search_df)

    def _verify_correct_items(_area, _workbooks):
        _correct_item_ids = [
            data_lab_items_df[(data_lab_items_df['Asset'] == _area) &
                              (data_lab_items_df['Name'] == 'Compressor Power')].iloc[0]['ID'],
            data_lab_items_df[(data_lab_items_df['Asset'] == _area) &
                              (data_lab_items_df['Name'] == 'Compressor Stage')].iloc[0]['ID'],
            tree_file_items_df.iloc[0]['ID'],
            tree_file_items_df.iloc[1]['ID']
        ]

        for _worksheet in _workbooks[0].worksheets:  # type: Worksheet
            _current_workstep = _worksheet.worksteps[_worksheet['Current Workstep ID']]
            for _trend_item in _current_workstep.data['state']['stores']['sqTrendSeriesStore']['items']:
                assert _trend_item['id'] in _correct_item_ids

    _verify_correct_items('Area A', pushed_workbooks)

    explanation = pushed_df.spy.item_map.explain('C403EA41-64B3-43AB-9151-8C6085A4BB6B')
    _assert_explanation(
        explanation,
        """
        No datasource map overrides found
        Item's ID C403EA41-64B3-43AB-9151-8C6085A4BB6B not found directly in target server
        Using non-overrides from {...}Item References (23DC9E6A-FCC3-456E-9A58-62D5CFF05816):
        - Used "{...}Datasource_Map_Tree File_5db22156-858a-41f9-915e-6e54c91b216c_mydata_trees.json"
        - RegEx-Based Map 0: Successfully mapped. Details:
            "Type"
                regex          "(?<type>.*)"
                matched on     "CalculatedSignal"
                searched for   "Signal"
                and found      "CalculatedSignal"
            "Datasource Class"
                regex          "Tree File"
                matched on     "Tree File"
                searched for   "Tree File"
                and found      "Tree File"
            "Data ID"
                regex          "(?<data_id>.*)"
                matched on     "tree1.csv | Tree1-Signal-Area A-Compressor Power"
                searched for   "tree1.csv | Tree1-Signal-Area A-Compressor Power"
                and found      "tree1.csv | Tree1-Signal-Area A-Compressor Power"
            "Datasource ID"
                searched for   "5db22156-858a-41f9-915e-6e54c91b216c"
                and found      "5db22156-858a-41f9-915e-6e54c91b216c"
            Capture groups:
                type           "CalculatedSignal"
                data_id        "tree1.csv | Tree1-Signal-Area A-Compressor Power"
        Successful mapping:
          Old: CalculatedSignal "Tree 1 >> Cooling Tower - Area A >> Tree 1 - Compressor Power" (C403EA41-64B3-43AB-9151-8C6085A4BB6B)
          New: CalculatedSignal "Tree 1 >> Cooling Tower - Area A >> Tree 1 - Compressor Power" ({...})
        """
    )

    explanation = pushed_df.spy.item_map.explain('45D6F55F-0899-4833-A807-56742F005B4C')
    _assert_explanation(
        explanation,
        """
        No datasource map overrides found
        Item's ID 45D6F55F-0899-4833-A807-56742F005B4C not found directly in target server
        Using non-overrides from {...}Item References (23DC9E6A-FCC3-456E-9A58-62D5CFF05816):
        - Used "{...}Datasource_Map_Tree File_5db22156-858a-41f9-915e-6e54c91b216c_mydata_trees.json"
        - RegEx-Based Map 0: Successfully mapped. Details:
            "Type"
                regex          "(?<type>.*)"
                matched on     "Asset"
                searched for   "Asset"
                and found      "Asset"
            "Datasource Class"
                regex          "Tree File"
                matched on     "Tree File"
                searched for   "Tree File"
                and found      "Tree File"
            "Data ID"
                regex          "(?<data_id>.*)"
                matched on     "tree1.csv | Tree1-Asset-Area A"
                searched for   "tree1.csv | Tree1-Asset-Area A"
                and found      "tree1.csv | Tree1-Asset-Area A"
            "Datasource ID"
                searched for   "5db22156-858a-41f9-915e-6e54c91b216c"
                and found      "5db22156-858a-41f9-915e-6e54c91b216c"
            Capture groups:
                type           "Asset"
                data_id        "tree1.csv | Tree1-Asset-Area A"
        Successful mapping:
          Old: Asset "Tree 1 >> Cooling Tower - Area A" (45D6F55F-0899-4833-A807-56742F005B4C)
          New: Asset "Tree 1 >> Cooling Tower - Area A" ({...})
        """
    )

    # Now map to a different Area
    with tempfile.TemporaryDirectory() as temp:
        sdl_datasource_map = {
            "Datasource Class": "Seeq Data Lab",
            "Datasource ID": "Seeq Data Lab",
            "Datasource Name": "Seeq Data Lab",
            "RegEx-Based Maps": [
                {
                    "Old": {
                        "Type": "CalculatedSignal",
                        "Path": "test_item_references",
                        "Asset": ".*Area Does Not Exist.*",
                        "Name": "(?<name>.*)"
                    },
                    "New": {
                        "Type": "CalculatedSignal",
                        "Path": "test_item_references",
                        "Asset": "Area B",
                        "Name": "${name}"
                    }
                },
                {
                    "Old": {
                        "Type": "CalculatedSignal",
                        "Path": "test_item_references",
                        "Asset": "Area A",
                        "Name": "(?<name>.*)"
                    },
                    "New": {
                        "Type": "CalculatedSignal",
                        "Path": "test_item_references",
                        "Asset": "Area B",
                        "Name": "${name}"
                    }
                },
                {
                    "Old": {
                        "Type": "(?<type>.*)",
                        "Datasource Class": "Seeq Data Lab",
                        "Datasource ID": "Seeq Data Lab",
                        "Data ID": "(?<data_id>.*)"
                    },
                    "New": {
                        "Type": "${type}",
                        "Datasource Class": "Seeq Data Lab",
                        "Datasource ID": "Seeq Data Lab",
                        "Data ID": "${data_id}"
                    }
                }
            ]
        }

        with util.safe_open(os.path.join(temp, 'Datasource_Map_Seeq_Data_Lab_Override.json'), 'w') as f:
            json.dump(sdl_datasource_map, f)

        pushed_df = spy.workbooks.push(workbooks, refresh=False, datasource_map_folder=temp)

    explanation = pushed_df.spy.item_map.explain('B18469DA-D719-4E21-ABD1-EE336500178C')
    _assert_explanation(
        explanation,
        """
        Using overrides from {...}:
        - Used "{...}Datasource_Map_Seeq_Data_Lab_Override.json"
        - RegEx-Based Map 0: Unsuccessful match. Details:
            "Asset"
                regex          ".*Area Does Not Exist.*"
                does not match "Area A"
        - RegEx-Based Map 1: Successfully mapped. Details:
            "Type"
                regex          "CalculatedSignal"
                matched on     "CalculatedSignal"
                searched for   "Signal"
                and found      "CalculatedSignal"
            "Path"
                regex          "test_item_references"
                matched on     "test_item_references"
                searched for   "test_item_references"
                and found      "test_item_references"
            "Asset"
                regex          "Area A"
                matched on     "Area A"
                searched for   "Area B"
                and found      "Area B"
            "Name"
                regex          "(?<name>.*)"
                matched on     "Compressor Stage"
                searched for   "Compressor Stage"
                and found      "Compressor Stage"
            Capture groups:
                name           "Compressor Stage"
        Successful mapping:
          Old: CalculatedSignal "test_item_references >> Area A >> Compressor Stage" (B18469DA-D719-4E21-ABD1-EE336500178C)
          New: CalculatedSignal "test_item_references >> Area B >> Compressor Stage" ({...})
        """
    )

    pushed_workbooks = spy.workbooks.pull(search_df)

    _verify_correct_items('Area B', pushed_workbooks)


@pytest.mark.system
def test_datasource_map_by_name():
    # In this test we push a few signals, two of which have the same name but one of which is archived because there
    # is special code in data.py to choose the un-archived signal if multiple signals are returned.
    push_df = spy.push(metadata=pd.DataFrame([{
        'Type': 'Signal',
        'Name': 'test_datasource_map_by_name-Old',
        'Data ID': 'test_datasource_map_by_name-Old'
    }, {
        'Type': 'Signal',
        'Name': 'test_datasource_map_by_name-New',
        'Data ID': 'test_datasource_map_by_name-New'
    }, {
        'Type': 'Signal',
        'Name': 'test_datasource_map_by_name-New',
        'Data ID': 'test_datasource_map_by_name-New-Archived',
        'Archived': True
    }]), workbook=None)

    items_api = ItemsApi(spy.client)
    item_output = items_api.get_item_and_all_properties(id=push_df.iloc[2]['ID'])

    workbook = spy.workbooks.Analysis({
        'Name': 'test_datasource_map_by_name'
    })
    worksheet = workbook.worksheet('the one worksheet')

    display_items = push_df.loc[0:0]
    worksheet.display_items = display_items
    spy.workbooks.push(workbook, path='test_datasource_map_by_name')

    with tempfile.TemporaryDirectory() as temp:
        sdl_datasource_map = {
            "Datasource Class": "Seeq Data Lab",
            "Datasource ID": "Seeq Data Lab",
            "Datasource Name": "Seeq Data Lab",
            "RegEx-Based Maps": [
                {
                    "Old": {
                        "Type": "StoredSignal",
                        "Name": "test_datasource_map_by_name-Old",
                    },
                    "New": {
                        "Type": "StoredSignal",
                        "Name": "test_datasource_map_by_name-New",
                    }
                }
            ]
        }

        with util.safe_open(os.path.join(temp, 'Datasource_Map_Seeq_Data_Lab.json'), 'w') as f:
            json.dump(sdl_datasource_map, f)

        spy.workbooks.push(workbook, path='test_datasource_map_by_path', datasource_map_folder=temp)

    assert workbook.worksheet('the one worksheet').display_items.iloc[0]['ID'] == push_df.iloc[1]['ID']


@pytest.mark.system
def test_datasource_map_multiple_matching_datasources():
    datasources_api = DatasourcesApi(spy.session.client)

    datasource_input = DatasourceInputV1()
    datasource_input.datasource_class = 'test_push'
    datasource_input.datasource_id = 'datasource_id_1'
    datasource_input.name = 'test_datasource_map_multiple_matching_datasources'
    datasource_output_1 = datasources_api.create_datasource(body=datasource_input)  # type: DatasourceOutputV1

    datasource_input.datasource_id = 'datasource_id_2'
    datasources_api.create_datasource(body=datasource_input)  # type: DatasourceOutputV1

    analysis = Analysis({
        'Name': datasource_input.name
    })

    analysis.worksheet('the only worksheet')

    signal = StoredSignal()
    signal['ID'] = _common.new_placeholder_guid()
    signal['Name'] = 'A Signal'
    signal['Datasource Class'] = datasource_output_1.datasource_class
    signal['Datasource ID'] = datasource_output_1.datasource_id
    analysis.item_inventory[signal['ID']] = signal

    with tempfile.TemporaryDirectory() as temp:
        datasource_map = {
            "Datasource Class": datasource_output_1.datasource_class,
            "Datasource ID": datasource_output_1.datasource_id,
            "Datasource Name": datasource_output_1.name,
            "RegEx-Based Maps": [
                {
                    "Old": {
                        "Type": "(?<type>.*)",
                        "Datasource Class": datasource_output_1.datasource_class,
                        "Datasource Name": datasource_output_1.name,
                        'Name': "(?<name>.*)"
                    },
                    "New": {
                        "Type": "${type}",
                        "Datasource Class": datasource_output_1.datasource_class,
                        "Datasource Name": datasource_output_1.name,
                        'Name': "${name}"
                    }
                }
            ]
        }

        with util.safe_open(os.path.join(temp, 'Datasource_Map_1.json'), 'w') as f:
            json.dump(datasource_map, f)

        with pytest.raises(
                RuntimeError,
                match='.*Multiple datasources found that match "test_datasource_map_multiple_matching_datasources".*'):
            spy.workbooks.push(analysis, datasource_map_folder=temp)


@pytest.mark.system
def test_datasource_map_push_errors():
    analysis = Analysis({
        'Name': 'test_datasource_map_push_errors'
    })

    analysis.worksheet('the only worksheet')

    stored_signal = StoredSignal()
    stored_signal['ID'] = _common.new_placeholder_guid()
    stored_signal['Name'] = 'A Stored Signal'
    stored_signal['Datasource Class'] = 'Seeq - Signal Datasource'
    stored_signal['Datasource ID'] = 'default'
    analysis.item_inventory[stored_signal['ID']] = stored_signal

    calculated_signal = CalculatedSignal()
    calculated_signal['ID'] = _common.new_placeholder_guid()
    calculated_signal['Name'] = 'A Calculated Signal'
    calculated_signal['Formula'] = '$a'
    calculated_signal['Formula Parameters'] = {
        '$a': stored_signal['ID']
    }
    analysis.item_inventory[calculated_signal['ID']] = calculated_signal

    try:
        spy.workbooks.push(analysis)
    except RuntimeError as e:
        assert re.match(r'.*"A Stored Signal".*not successfully mapped.*', str(e), re.DOTALL)
        assert re.match(r'.*Item\'s ID.*not found directly.*', str(e), re.DOTALL)

    with tempfile.TemporaryDirectory() as temp:
        datasource_map = {
            "Datasource Class": "Seeq - Signal Datasource",
            "Datasource ID": "default",
            "Datasource Name": "Seeq Signal Datasource",
            "RegEx-Based Maps": [
                {
                    "Old": {
                        "Type": "Alien",
                    },
                    "New": {
                        "Type": "Predator",
                    }
                },
                {
                    "Old": {
                        "Type": "(?<type>.*)",
                        "Datasource Class": "Seeq - Signal Datasource",
                        "Datasource Name": "Seeq Signal Datasource",
                        'Name': "(?<name>.*)"
                    },
                    "New": {
                        "Type": "${type}",
                        "Datasource Class": "Seeq - Signal Datasource",
                        "Datasource Name": "Seeq Signal Datasource",
                        'Name': "${name}"
                    }
                }
            ]
        }

        with util.safe_open(os.path.join(temp, 'Datasource_Map_1.json'), 'w') as f:
            json.dump(datasource_map, f)

        try:
            spy.workbooks.push(analysis, datasource_map_folder=temp)
        except RuntimeError as e:
            explanation = str(e)
            _assert_explanation(
                explanation,
                """
                {...}StoredSignal "A Stored Signal" ({...}) not successfully mapped
                Using overrides from {...}:
                - Used "{...}Datasource_Map_1.json"
                - RegEx-Based Map 0: Unsuccessful match. Details:
                    "Type"
                        regex          "Alien"
                        does not match "StoredSignal"
                - RegEx-Based Map 1: Item not found on server. Details:
                    "Type"
                        regex          "(?<type>.*)"
                        matched on     "StoredSignal"
                        searched for   "Signal"
                    "Datasource Class"
                        regex          "Seeq - Signal Datasource"
                        matched on     "Seeq - Signal Datasource"
                        searched for   "Seeq - Signal Datasource"
                    "Name"
                        regex          "(?<name>.*)"
                        matched on     "A Stored Signal"
                        searched for   "A Stored Signal"
                    "Datasource ID"
                        searched for   "Signal Datasource"
                    Capture groups:
                        type           "StoredSignal"
                        name           "A Stored Signal"
                Item's ID {...} not found directly in target server
                No (non-override) datasource maps found{...}
                """
            )
            _assert_explanation(
                explanation,
                """
                {...}CalculatedSignal "A Calculated Signal" ({...}): Formula dependency $$a=StoredSignal "A Stored Signal" ({...}) not found/mapped/pushed
                """
            )

        datasource_map = {
            "Datasource Class": "Seeq - Signal Datasource",
            "Datasource ID": "default",
            "Datasource Name": "Seeq Signal Datasource",
            "RegEx-Based Maps": [
                {
                    "Old": {
                        "Type": "StoredSignal",
                        "Datasource Class": "Seeq - Signal Datasource",
                        "Datasource Name": "Seeq Signal Datasource",
                        'Name': "Wallace and Gromit"
                    },
                    "New": {
                        "Type": "StoredSignal",
                        "Datasource Class": "Time Series CSV Files",
                        "Datasource Name": "Example Data",
                        'Name': "Area A_Temperature"
                    }
                },
                {
                    "Old": {
                        "Type": "StoredSignal",
                        "Datasource Class": "Seeq - Signal Datasource",
                        "Datasource Name": "Seeq Signal Datasource",
                        'Name': "A Stored Signal"
                    },
                    "New": {
                        "Type": "StoredSignal",
                        "Datasource Class": "Time Series CSV Files",
                        "Datasource Name": "Example Data",
                        'Name': "Area *_Temperature"
                    }
                }
            ]
        }

        with util.safe_open(os.path.join(temp, 'Datasource_Map_1.json'), 'w') as f:
            json.dump(datasource_map, f)

        try:
            spy.workbooks.push(analysis, datasource_map_folder=temp)
        except RuntimeError as e:
            explanation = str(e)
            assert re.match(r'.*regex.*?"Wallace and Gromit".*?does not match.*?"A Stored Signal".*', explanation,
                            re.DOTALL)
            assert re.match(r'.*Multiple possibilities for item found..*', explanation, re.DOTALL)

        with util.safe_open(os.path.join(temp, 'Datasource_Map_2.json'), 'w') as f:
            json.dump(datasource_map, f)

        with pytest.raises(SPyRuntimeError, match='Duplicate datasource map for Datasource Class'):
            spy.workbooks.push(analysis, datasource_map_folder=temp)


@pytest.mark.system
def test_datasource_map_by_path():
    temperature_signals = spy.search({
        'Datasource ID': 'Example Data',
        'Path': 'Example',
        'Name': 'Temperature'
    }, workbook=spy.GLOBALS_ONLY)
    workbook = spy.workbooks.Analysis({
        'Name': 'test_datasource_map_by_path'
    })
    worksheet = workbook.worksheet('the one worksheet')

    worksheet.display_items = temperature_signals.sort_values(by='Asset')
    spy.workbooks.push(workbook, path='test_datasource_map_by_path')

    search_df = spy.search({
        'Type': 'Signal',
        'Datasource ID': 'Example Data',
        'Path': 'Example',
        'Name': 'Temperature'
    }, workbook=spy.GLOBALS_ONLY)

    new_tree_df = search_df.copy()
    new_tree_df = new_tree_df[['ID', 'Type', 'Path', 'Asset', 'Name']]
    new_tree_df['Path'] = 'test_datasource_map_by_path >> ' + search_df['Path']
    new_tree_df['Reference'] = True
    spy.push(metadata=new_tree_df, workbook=workbook.id, worksheet=None)

    with tempfile.TemporaryDirectory() as temp:
        example_datasource_map = {
            "Datasource Class": "Time Series CSV Files",
            "Datasource ID": "Example Data",
            "Datasource Name": "Example Data",
            "RegEx-Based Maps": [
                {
                    "Old": {
                        "Type": "(?<type>.*)",
                        "Datasource Class": "Time Series CSV Files",
                        "Datasource Name": "Example Data",
                        'Path': "(?<path>Example >> .*)",
                        'Asset': "(?<asset>.*)",
                        'Name': "(?<name>.*)"
                    },
                    "New": {
                        "Type": "${type}",
                        'Path': "test_datasource_map_by_path >> ${path}",
                        'Asset': "${asset}",
                        'Name': "${name}"
                    }
                }
            ]
        }

        with util.safe_open(os.path.join(temp, 'Datasource_Map_Time Series CSV Files_Example Data_Example Data.json'),
                            'w') as f:
            json.dump(example_datasource_map, f)

        sdl_datasource_map = {
            "Datasource Class": "Seeq Data Lab",
            "Datasource ID": "Seeq Data Lab",
            "Datasource Name": "Seeq Data Lab",
            "RegEx-Based Maps": [
                {
                    "Old": {
                        "Type": "(?<type>.*)",
                        "Datasource Class": "Seeq Data Lab",
                        "Datasource Name": "Seeq Data Lab",
                        'Data ID': "(?<data_id>.*)"
                    },
                    "New": {
                        "Type": "${type}",
                        "Data ID": "${data_id}"
                    }
                }
            ]
        }

        with util.safe_open(os.path.join(temp, 'Datasource_Map_Seeq_Data_Lab.json'), 'w') as f:
            json.dump(sdl_datasource_map, f)

        spy.workbooks.push(workbook, path='test_datasource_map_by_path', datasource_map_folder=temp)

        display_items = spy.search(worksheet.display_items, all_properties=True)

        for _, display_item in display_items.iterrows():
            assert display_item['Datasource Class'] == 'Seeq Data Lab'
            assert display_item['Path'].startswith('test_datasource_map_by_path')


@pytest.mark.system
def test_datasource_map_by_asset():
    test_name = 'test_datasource_map_by_asset'
    workbooks = spy.workbooks.load(get_full_path_of_export('Example Analysis (D833DC83-9A38-48DE-BF45-EB787E9E8375)'))

    with tempfile.TemporaryDirectory() as temp:
        example_datasource_map = {
            "Datasource Class": "Time Series CSV Files",
            "Datasource ID": "Example Data",
            "Datasource Name": "Example Data",
            "RegEx-Based Maps": [
                {
                    "Old": {
                        "Type": "(?<type>.*)",
                        "Datasource Class": "Time Series CSV Files",
                        "Datasource Name": "Example Data",
                        'Name': "Area A"
                    },
                    "New": {
                        "Type": "${type}",
                        "Datasource Class": "Time Series CSV Files",
                        "Datasource Name": "Example Data",
                        'Name': "Area Q"
                    },
                }, {
                    "Old": {
                        "Type": "(?<type>.*)",
                        "Datasource Class": "Time Series CSV Files",
                        "Datasource Name": "Example Data",
                        'Path': "(?<path>.*)",
                        'Asset': "Area A",
                        'Name': "Area A"
                    },
                    "New": {
                        "Type": "${type}",
                        "Datasource Class": "Time Series CSV Files",
                        "Datasource Name": "Example Data",
                        'Path': "${path}",
                        'Asset': "Area B",
                        'Name': "Area B"
                    },
                }, {
                    "Old": {
                        "Type": "(?<type>.*)",
                        "Datasource Class": "Time Series CSV Files",
                        "Datasource Name": "Example Data",
                        'Path': "(?<path>.*)",
                        'Asset': "Area A",
                        'Name': "(?<name>.*)"
                    },
                    "New": {
                        "Type": "${type}",
                        "Datasource Class": "Time Series CSV Files",
                        "Datasource Name": "Example Data",
                        'Path': "${path}",
                        'Asset': "Area B",
                        'Name': "${name}"
                    },
                }, {
                    "Old": {
                        "Type": "(?<type>.*)",
                        "Datasource Class": "Time Series CSV Files",
                        "Datasource Name": "Example Data",
                        'Data ID': "(?<data_id>.*)"
                    },
                    "New": {
                        "Type": "${type}",
                        "Datasource Class": "Time Series CSV Files",
                        "Datasource Name": "Example Data",
                        'Data ID': "${data_id}"
                    },
                }
            ]
        }

        with util.safe_open(os.path.join(temp, 'Datasource_Map_Time Series CSV Files_Example Data_Example Data.json'),
                            'w') as f:
            json.dump(example_datasource_map, f)

        push_df = spy.workbooks.push(workbooks, label=test_name, path=test_name, datasource_map_folder=temp)
        explanation = push_df.spy.item_map.explain('4B40EAFC-91ED-4AB0-8199-F21AF40A8350')
        assert 'New: Asset "Example >> Cooling Tower 1 >> Area B"' in explanation


@pytest.mark.system
def test_datasource_map_on_match_parameter():
    test_name = 'test_datasource_map_on_match_parameter'
    workbooks = spy.workbooks.load(get_full_path_of_export('Example Analysis (D833DC83-9A38-48DE-BF45-EB787E9E8375)'))

    with tempfile.TemporaryDirectory() as temp:
        example_datasource_map = {
            "Datasource Class": "Time Series CSV Files",
            "Datasource ID": "Example Data",
            "Datasource Name": "Example Data",
            "RegEx-Based Maps": [
                {
                    "Old": {
                        "Type": "(?<type>.*)",
                        "Datasource Class": "Time Series CSV Files",
                        "Datasource Name": "Example Data",
                        'Name': "Area A"
                    },
                    "New": {
                        "Type": "${type}",
                        "Datasource Class": "Time Series CSV Files",
                        "Datasource Name": "Example Data",
                        'Name': "Area Q"
                    },
                    "On Match": "Stop"
                }, {
                    "Old": {
                        "Type": "(?<type>.*)",
                        "Datasource Class": "Time Series CSV Files",
                        "Datasource Name": "Example Data",
                        'Path': "(?<path>.*)",
                        'Asset': "Area A",
                        'Name': "Area A"
                    },
                    "New": {
                        "Type": "${type}",
                        "Datasource Class": "Time Series CSV Files",
                        "Datasource Name": "Example Data",
                        'Path': "${path}",
                        'Asset': "Area B",
                        'Name': "Area B"
                    },
                }
            ]
        }

        def _write(_map):
            with util.safe_open(os.path.join(
                    temp, 'Datasource_Map_Time Series CSV Files_Example Data_Example Data.json'), 'w') as f:
                json.dump(_map, f)

        _write(example_datasource_map)

        push_df = spy.workbooks.push(workbooks, label=test_name, path=test_name, datasource_map_folder=temp,
                                     errors='catalog')
        explanation = push_df.spy.item_map.explain('4B40EAFC-91ED-4AB0-8199-F21AF40A8350')
        assert '"On Match" parameter is "Stop", so no further RegEx-Based Maps will be used' in explanation
        assert ('Asset "Example >> Cooling Tower 1 >> Area A" (4B40EAFC-91ED-4AB0-8199-F21AF40A8350) '
                'not successfully mapped' in explanation)

        example_datasource_map['RegEx-Based Maps'][0]['On Match'] = 'Continue'
        _write(example_datasource_map)

        push_df = spy.workbooks.push(workbooks, label=test_name, path=test_name, datasource_map_folder=temp,
                                     errors='catalog')
        explanation = push_df.spy.item_map.explain('4B40EAFC-91ED-4AB0-8199-F21AF40A8350')
        assert '"On Match" parameter is "Stop", so no further RegEx-Based Maps will be used' not in explanation
        assert ('RegEx-Based Map 1: Successfully mapped' in explanation)


@pytest.mark.system
def test_datasource_map_by_data_id():
    # This test ensures that, if a datasource_map_folder argument is supplied, it will cause existing items to be
    # mapped to new items, which supports the case where you want to pull a workbook and swap to a different datasource.

    workbooks = spy.workbooks.load(get_full_path_of_export('Worksheet Order (2BBDCFA7-D25C-4278-922E-D99C8DBF6582)'))
    workbooks[0].name = 'Datasource Map Test'
    push_df = spy.workbooks.push(workbooks, refresh=False, label='test_datasource_map_by_data_id')

    push_df.drop(columns=['ID'], inplace=True)
    push_df.rename(columns={'Pushed Workbook ID': 'ID'}, inplace=True)
    push_df['Type'] = 'Workbook'

    workbooks = spy.workbooks.pull(push_df)

    # This map will simply convert the tree-based example signals to their flat-name equivalents
    with tempfile.TemporaryDirectory() as temp:
        datasource_map = {
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
                        "Datasource Class": "Time Series CSV Files",
                        "Datasource Name": "Example Data",
                        # Note that only Name and Description can contain wildcards
                        "Data ID": "[Tag] ${data_id}"
                    }
                }
            ]
        }

        with util.safe_open(os.path.join(temp, 'Datasource_Map_Time Series CSV Files_Example Data_Example Data.json'),
                            'w') as f:
            json.dump(datasource_map, f)

        spy.workbooks.push(workbooks, refresh=False, datasource_map_folder=temp)

    workbooks = spy.workbooks.pull(push_df)

    _confirm_flat_tag(workbooks)


@pytest.mark.system
def test_datasource_map_by_id_scoped_stored_item_to_global():
    # This test ensures that datasource mapping works for converting from a workbook-scoped stored item to a
    # globally-scoped one. This feature is useful for mapping customer data to Example Data/Chaos Monkey items.
    test_name = f'test_datasource_map_by_id_scoped_stored_item_to_global {_common.new_placeholder_guid()}'
    dir_name = 'Scoped Stored Item (D833DC83-9A38-48DE-BF45-EB787E9E8375)'
    scenarios_folder = test_common.unzip_to_temp(os.path.join(os.path.dirname(__file__), 'Scenarios.zip'))
    try:
        workbook_folder = os.path.join(scenarios_folder, dir_name)
        workbooks = spy.workbooks.load(workbook_folder)
        workbooks[0].name = test_name

        # Map from a scoped StoredSignal from items.json
        to_map_name = 'Test Stored Signal'
        to_map_id = '0EEC53D6-7ECE-7780-A708-1F86D4788B86'
        # To Area E Temperature
        area_e_temp = spy.search({'Name': 'Area E_Temperature', 'Datasource Name': 'Example Data'},
                                 workbook=None, limit=1)
        assert len(area_e_temp) == 1

        with tempfile.TemporaryDirectory() as temp:
            item_map_filename = f'item_map_{_common.new_placeholder_guid()}.csv'
            item_map_df = pd.DataFrame.from_dict({'Old ID': [to_map_id],
                                                  'New ID': [area_e_temp.iloc[0].ID]})
            item_map_df.to_csv(os.path.join(temp, item_map_filename), index=False)

            datasource_map_filename = 'Datasource_Map_Seeq Data Lab_Seeq Data Lab_Seeq Data Lab.json'
            datasource_map = {
                'Datasource Class': 'Seeq Data Lab',
                'Datasource ID': 'Seeq Data Lab',
                'Datasource Name': 'Seeq Data Lab',
                'Item-Level Map Files': [os.path.join(temp, item_map_filename)],
                'RegEx-Based Maps': []
            }
            with util.safe_open(os.path.join(temp, datasource_map_filename), 'w') as f:
                json.dump(datasource_map, f)

            map_push_result = spy.workbooks.push(workbooks, label=test_name, refresh=False, datasource_map_folder=temp)

            # The map.explain() should tell us that the old item was swapped out for the Example Data signal. There
            # should not be a new local/dummy item pushed.
            map_explain = map_push_result.spy.item_map.explain(to_map_id)
            assert 'Successful mapping' in map_explain
            assert f'Old: StoredSignal "{to_map_name}" ({to_map_id})' in map_explain
            assert f'New: StoredSignal "Area E_Temperature" ({area_e_temp.iloc[0].ID})' in map_explain
            assert 'Local item pushed' not in map_explain
            assert 'Dummy item pushed' not in map_explain
    finally:
        util.safe_rmtree(scenarios_folder)


@pytest.mark.system
def test_datasource_map_by_id_scoped_calculated_item_to_global():
    # This test ensures that datasource mapping works for converting from a workbook-scoped calculated item to a
    # globally-scoped one. This feature is useful for mapping customer data to Example Data/Chaos Monkey items.
    test_name = f'test_datasource_map_by_id_scoped_calculated_item_to_global {_common.new_placeholder_guid()}'
    workbooks = spy.workbooks.load(get_full_path_of_export('Example Analysis (D833DC83-9A38-48DE-BF45-EB787E9E8375)'))
    workbooks[0].name = test_name

    # Map from a scoped CalculatedSignal in `Example Analysis (D833DC83-9A38-48DE-BF45-EB787E9E8375)`'s items.json
    to_map_name = 'Smooth Temperature'
    to_map_id = 'FBBCD4E0-CE26-4A33-BE59-3E215553FB1F'
    # To Area E Temperature
    area_e_temp = spy.search({'Name': 'Area E_Temperature', 'Datasource Name': 'Example Data'},
                             workbook=None, limit=1)
    assert len(area_e_temp) == 1

    with tempfile.TemporaryDirectory() as temp:
        item_map_filename = f'item_map_{_common.new_placeholder_guid()}.csv'
        item_map_df = pd.DataFrame.from_dict({'Old ID': [to_map_id],
                                              'New ID': [area_e_temp.iloc[0].ID]})
        item_map_df.to_csv(os.path.join(temp, item_map_filename), index=False)

        datasource_map_filename = 'Datasource_Map_Seeq Calculations_Seeq Calculations_Seeq Calculations.json'
        datasource_map = {
            'Datasource Class': 'Seeq Calculations',
            'Datasource ID': 'Seeq Calculations',
            'Datasource Name': 'Seeq Calculations',
            'Item-Level Map Files': [os.path.join(temp, item_map_filename)],
            'RegEx-Based Maps': []
        }
        with util.safe_open(os.path.join(temp, datasource_map_filename), 'w') as f:
            json.dump(datasource_map, f)

        map_push_result = spy.workbooks.push(workbooks, label=test_name, refresh=False, datasource_map_folder=temp)

        # The map.explain() should tell us that the old item was swapped out for the Example Data signal. There
        # should not be a new local/dummy item pushed.
        map_explain = map_push_result.spy.item_map.explain(to_map_id)
        assert 'Successful mapping' in map_explain
        assert f'Old: CalculatedSignal "{to_map_name}" ({to_map_id})' in map_explain
        assert f'New: StoredSignal "Area E_Temperature" ({area_e_temp.iloc[0].ID})' in map_explain
        assert 'Local item pushed' not in map_explain
        assert 'Dummy item pushed' not in map_explain


@pytest.mark.system
def test_datasource_map_by_id_scoped_stored_item_to_already_pushed():
    # This test ensures that datasource mapping works for converting from a workbook-scoped stored item to another
    # workbook-scoped signal that already exists in the target workbook. This is used by the example use case Add-on
    # by pushing the signals with data then mapping by those pushed IDs.
    test_name = f'test_datasource_map_by_id_scoped_stored_item_to_already_pushed {_common.new_placeholder_guid()}'
    dir_name = 'Scoped Stored Item (D833DC83-9A38-48DE-BF45-EB787E9E8375)'
    scenarios_folder = test_common.unzip_to_temp(os.path.join(os.path.dirname(__file__), 'Scenarios.zip'))
    try:
        workbook_folder = os.path.join(scenarios_folder, dir_name)
        workbooks = spy.workbooks.load(workbook_folder)
        workbooks[0].name = test_name
        workbook_df = spy.workbooks.push(workbooks, label=test_name, refresh=False)
        workbook_df.drop(columns=['ID'], inplace=True)
        workbook_df.rename(columns={'Pushed Workbook ID': 'ID'}, inplace=True)
        workbook_df['Type'] = 'Workbook'

        # Reload the workbooks from file to ensure nothing is altered
        workbooks = spy.workbooks.load(workbook_folder)
        workbooks[0].name = test_name

        # Map from a scoped StoredSignal in items.json
        to_map_name = 'Test Stored Signal'
        to_map_id = '0EEC53D6-7ECE-7780-A708-1F86D4788B86'
        # To a newly-pushed workbook-scoped CalculatedSignal
        pushed_signal = spy.push(metadata=pd.DataFrame([{
            'Name': 'Locally scoped signal',
            'Formula': 'sinusoid()',
        }]), workbook=workbook_df.iloc[0].ID, worksheet=None)

        with tempfile.TemporaryDirectory() as temp:
            item_map_filename = f'item_map_{_common.new_placeholder_guid()}.csv'
            item_map_df = pd.DataFrame.from_dict({'Old ID': [to_map_id],
                                                  'New ID': [pushed_signal.iloc[0].ID]})
            item_map_df.to_csv(os.path.join(temp, item_map_filename), index=False)

            datasource_map_filename = 'Datasource_Map_Seeq Data Lab_Seeq Data Lab_Seeq Data Lab.json'
            datasource_map = {
                'Datasource Class': 'Seeq Data Lab',
                'Datasource ID': 'Seeq Data Lab',
                'Datasource Name': 'Seeq Data Lab',
                'Item-Level Map Files': [os.path.join(temp, item_map_filename)],
                'RegEx-Based Maps': []
            }
            with util.safe_open(os.path.join(temp, datasource_map_filename), 'w') as f:
                json.dump(datasource_map, f)

            map_push_result = spy.workbooks.push(workbooks, label=test_name, refresh=False, datasource_map_folder=temp)

            # The map.explain() should tell us that the old signal was swapped out for the new signal. There should not be
            # a new local/dummy item pushed.
            map_explain = map_push_result.spy.item_map.explain(to_map_id)
            assert 'Successful mapping' in map_explain
            assert f'Old: StoredSignal "{to_map_name}" ({to_map_id})' in map_explain
            assert f'New: CalculatedSignal "{pushed_signal.iloc[0].Name}" ({pushed_signal.iloc[0].ID})' in map_explain
            assert 'Local item pushed' not in map_explain
            assert 'Dummy item pushed' not in map_explain

    finally:
        util.safe_rmtree(scenarios_folder)


@pytest.mark.system
def test_datasource_map_by_id_scoped_calculated_item_to_already_pushed():
    # This test ensures that datasource mapping works for converting from a workbook-scoped calculated item to another
    # workbook-scoped one that already exists in the target workbook. This is used by the example use case Add-on
    # by pushing the signals with data then mapping by those pushed IDs.
    test_name = f'test_datasource_map_by_id_scoped_calculated_item_to_already_pushed {_common.new_placeholder_guid()}'
    workbooks = spy.workbooks.load(get_full_path_of_export('Example Analysis (D833DC83-9A38-48DE-BF45-EB787E9E8375)'))
    workbooks[0].name = test_name
    workbook_df = spy.workbooks.push(workbooks, label=test_name, refresh=False)
    workbook_df.drop(columns=['ID'], inplace=True)
    workbook_df.rename(columns={'Pushed Workbook ID': 'ID'}, inplace=True)
    workbook_df['Type'] = 'Workbook'

    # Reload the workbooks from file to ensure nothing is altered
    workbooks = spy.workbooks.load(get_full_path_of_export('Example Analysis (D833DC83-9A38-48DE-BF45-EB787E9E8375)'))
    workbooks[0].name = test_name

    # Map from a scoped CalculatedSignal in `Example Analysis (D833DC83-9A38-48DE-BF45-EB787E9E8375)`'s items.json
    to_map_name = 'Smooth Temperature'
    to_map_id = 'FBBCD4E0-CE26-4A33-BE59-3E215553FB1F'
    # To a newly-pushed workbook-scoped CalculatedSignal
    pushed_signal = spy.push(metadata=pd.DataFrame([{
        'Name': 'Locally scoped signal',
        'Formula': 'sinusoid()',
    }]), workbook=workbook_df.iloc[0].ID, worksheet=None)

    with tempfile.TemporaryDirectory() as temp:
        item_map_filename = f'item_map_{_common.new_placeholder_guid()}.csv'
        item_map_df = pd.DataFrame.from_dict({'Old ID': [to_map_id],
                                              'New ID': [pushed_signal.iloc[0].ID]})
        item_map_df.to_csv(os.path.join(temp, item_map_filename), index=False)

        datasource_map_filename = 'Datasource_Map_Seeq Calculations_Seeq Calculations_Seeq Calculations.json'
        datasource_map = {
            'Datasource Class': 'Seeq Calculations',
            'Datasource ID': 'Seeq Calculations',
            'Datasource Name': 'Seeq Calculations',
            'Item-Level Map Files': [os.path.join(temp, item_map_filename)],
            'RegEx-Based Maps': []
        }
        with util.safe_open(os.path.join(temp, datasource_map_filename), 'w') as f:
            json.dump(datasource_map, f)

        map_push_result = spy.workbooks.push(workbooks, label=test_name, refresh=False, datasource_map_folder=temp)

        # The map.explain() should tell us that the old item was swapped out for the new signal. There should not be
        # a new local/dummy item pushed.
        map_explain = map_push_result.spy.item_map.explain(to_map_id)
        assert 'Successful mapping' in map_explain
        assert f'Old: CalculatedSignal "{to_map_name}" ({to_map_id})' in map_explain
        assert f'New: CalculatedSignal "{pushed_signal.iloc[0].Name}" ({pushed_signal.iloc[0].ID})' in map_explain
        assert 'Local item pushed' not in map_explain
        assert 'Dummy item pushed' not in map_explain


@pytest.mark.system
def test_datasource_map_by_file():
    workbooks = spy.workbooks.load(get_full_path_of_export('Worksheet Order (2BBDCFA7-D25C-4278-922E-D99C8DBF6582)'))
    workbooks[0].name = 'Datasource Map By File Test'

    spy.workbooks.push(workbooks)

    tree_tags = spy.search({
        'Datasource ID': 'Example Data',
        'Path': 'Example',
        'Type': 'Signal'
    }, workbook=spy.GLOBALS_ONLY)

    flat_tags = spy.search({
        'Datasource ID': 'Example Data',
        'Name': 'Area*',
        'Data ID': '[Tag]*'
    }, workbook=spy.GLOBALS_ONLY)

    tree_tags['Flat Name'] = tree_tags['Asset'] + '_' + tree_tags['Name']

    tree_tags.drop(columns=['Name'], inplace=True)
    tree_tags.rename(columns={'ID': 'Old ID', 'Flat Name': 'Name'}, inplace=True)
    flat_tags.rename(columns={'ID': 'New ID'}, inplace=True)

    mapped_tags = tree_tags.merge(flat_tags, on='Name')

    with tempfile.TemporaryDirectory() as temp:
        csv_filename = os.path.join(temp, 'example_map.csv')
        mapped_tags.to_csv(csv_filename)

        datasource_map = {
            "Datasource Class": "Time Series CSV Files",
            "Datasource ID": "Example Data",
            "Datasource Name": "Example Data",
            "Item-Level Map Files": [csv_filename],
            "RegEx-Based Maps": []
        }

        with util.safe_open(os.path.join(temp, 'Datasource_Map_Time Series CSV Files_Example Data_Example Data.json'),
                            'w') as f:
            json.dump(datasource_map, f)

        spy.workbooks.push(workbooks, datasource_map_folder=temp)

    _confirm_flat_tag(workbooks)


def _confirm_flat_tag(workbooks):
    items_api = ItemsApi(spy.session.client)
    search_output = items_api.search_items(
        filters=['Name==Area C_Compressor Power'])  # type: ItemSearchPreviewPaginatedListV1
    area_c_compressor_power_id = search_output.items[0].id
    first_worksheet = workbooks[0].worksheets[0]  # type: AnalysisWorksheet
    display_item = first_worksheet.display_items.iloc[0]
    assert display_item['ID'] == area_c_compressor_power_id


@pytest.mark.system
def test_datasource_map_with_dummy_items():
    test_name = 'test_datasource_map_with_dummy_items'
    workbook: Analysis = Analysis()
    workbook.name = test_name
    workbook.worksheet('My Only Sheet')

    stored_signal_numeric = StoredSignal({
        'ID': 'D687C648-A1BF-4F68-8E82-5892671A16DB',
        'Type': 'StoredSignal',
        'Name': 'Stored Numeric Signal Dummy',
        'Description': 'This is a dummy signal for testing purposes',
        'Datasource Class': 'SuperCool Historian',
        'Datasource ID': 'Shangri-La Plant Datasource ID',
        'Data ID': 'Stored Numeric Signal Dummy Data ID',
        'Value Unit Of Measure': 'm/s',
        'Custom Property 1': 'My Customer Property, Number One!'
    })

    stored_signal_string = StoredSignal({
        'ID': '6384F1C0-175A-4E55-93F2-DE8521E175D5',
        'Type': 'StoredSignal',
        'Name': 'Stored String Signal Dummy',
        'Description': 'This is a dummy signal for testing purposes',
        'Datasource Class': 'SuperCool Historian',
        'Datasource ID': 'Shangri-La Plant Datasource ID',
        'Data ID': 'Stored String Signal Dummy Data ID',
        'Value Unit Of Measure': 'string',
        'Custom Property 2': 2
    })

    stored_condition = StoredCondition({
        'ID': '25145DD9-F36F-438F-9C39-FBC128A86C6D',
        'Type': 'StoredCondition',
        'Name': 'Stored Condition Dummy',
        'Description': 'This is a dummy condition for testing purposes',
        'Datasource Class': 'SuperCool Historian',
        'Datasource ID': 'Shangri-La Plant Datasource ID',
        'Data ID': 'Stored Condition Dummy Data ID',
        'Maximum Duration': '26.3h',
        'Custom Property 3': 'Custom Property Three'
    })

    literal_scalar = LiteralScalar({
        'ID': 'C1B5FA9A-5047-4D71-A9D0-0F1017A39CC9',
        'Type': 'LiteralScalar',
        'Name': 'Literal Scalar Dummy',
        'Description': 'This is a dummy scalar for testing purposes',
        'Datasource Class': 'SuperCool Historian',
        'Datasource ID': 'Shangri-La Plant Datasource ID',
        'Data ID': 'Literal Scalar Dummy Data ID',
        'Formula': '-10C',
        'Custom Property 4': 'Custom Property Four'
    })

    items = [stored_signal_numeric, stored_signal_string, stored_condition, literal_scalar]

    for item in items:
        workbook.item_inventory[item.id] = item

    pushed_df = spy.workbooks.push(workbook, label=test_name, refresh=False, errors='catalog')

    explanation = pushed_df.spy.item_map.explain('D687C648-A1BF-4F68-8E82-5892671A16DB')

    _assert_explanation(
        explanation,
        """
        StoredSignal "Stored Numeric Signal Dummy" (D687C648-A1BF-4F68-8E82-5892671A16DB) not successfully mapped
        No datasource map overrides found
        Item's ID D687C648-A1BF-4F68-8E82-5892671A16DB not found directly in target server
        No (non-override) datasource maps found
        """
    )

    pushed_df = spy.workbooks.push(workbook, label=test_name, refresh=False,
                                   create_dummy_items_in_workbook=f"My Dummy Workbook for {test_name}",
                                   errors='catalog')

    explanation = pushed_df.spy.item_map.explain('D687C648-A1BF-4F68-8E82-5892671A16DB')

    _assert_explanation(
        explanation,
        """
        StoredSignal "Stored Numeric Signal Dummy" (D687C648-A1BF-4F68-8E82-5892671A16DB) not successfully mapped
        No datasource map overrides found
        Item's ID D687C648-A1BF-4F68-8E82-5892671A16DB not found directly in target server
        No (non-override) datasource maps found
        Dummy item pushed: StoredSignal "Stored Numeric Signal Dummy" ({...})
        """
    )


@pytest.mark.system
def test_workbook_push_and_refresh():
    test_name = 'test_workbook_push_and_refresh'

    workbooks_api = WorkbooksApi(spy.session.client)
    items_api = ItemsApi(spy.session.client)

    with pytest.raises(TypeError, match='Workbook may not be instantiated directly, create either Analysis or Topic'):
        Workbook({'Name': 'My First From-Scratch Workbook'})

    workbook = Analysis({'Name': 'My First From-Scratch Workbook'})

    with pytest.raises(TypeError, match='Worksheet may not be instantiated directly, create either AnalysisWorksheet '
                                        'or TopicWorksheet'):
        Worksheet(workbook, {'Name': 'My First From-Scratch Worksheet'})

    worksheet = workbook.worksheet('My First From-Scratch Worksheet')

    sinusoid = CalculatedSignal({
        'Name': 'My First Sinusoid',
        'Formula': 'sinusoid()'
    })

    workbook.add_to_scope(sinusoid)

    worksheet.display_items = [sinusoid]

    first_workbook_id = workbook.id
    first_worksheet_id = worksheet.id
    first_sinusoid_id = sinusoid.id
    spy.workbooks.push(workbook, path=test_name, refresh=False)

    # Since refresh=False, the IDs will not have changed from the generated IDs on the objects
    assert first_workbook_id == workbook.id
    assert first_worksheet_id == worksheet.id
    assert first_sinusoid_id == sinusoid.id

    # However, the ID that is actually used on the server is different from the ID in the object. (That's why refresh
    # defaults to True-- users can get confused if the IDs are not the same.)
    with pytest.raises(ApiException, match='The item with ID.*could not be found'):
        workbooks_api.get_workbook(id=workbook.id)

    # Now if we push with refresh=True, the IDs will be updated to reflect what the server used for IDs
    spy.workbooks.push(workbook, path=test_name, refresh=True)
    assert first_workbook_id != workbook.id
    assert first_worksheet_id != worksheet.id
    assert first_sinusoid_id != sinusoid.id

    search_df = spy.workbooks.search({'Path': test_name})
    assert len(search_df) == 1

    second_workbook_id = workbook.id
    second_worksheet_id = worksheet.id
    second_sinusoid_id = sinusoid.id

    # Because we refreshed the in-memory objects with the correct IDs, we can change names around and it will update
    # the ones we already pushed
    workbook.name = 'My Second From-Scratch Workbook'
    worksheet.name = 'My Second From-Scratch Worksheet'
    sinusoid.name = 'My Second Sinusoid'
    spy.workbooks.push(workbook, path='test_workbook_push_and_refresh')
    assert second_workbook_id == workbook.id
    assert second_worksheet_id == worksheet.id
    assert second_sinusoid_id == sinusoid.id

    search_df = spy.workbooks.search({'Path': test_name})
    assert len(search_df) == 1

    workbook_output = workbooks_api.get_workbook(id=workbook.id)  # type: WorkbookOutputV1
    assert workbook_output.name == 'My Second From-Scratch Workbook'

    worksheet_output = workbooks_api.get_worksheet(workbook_id=workbook.id,
                                                   worksheet_id=worksheet.id)  # type: WorksheetOutputV1
    assert worksheet_output.name == 'My Second From-Scratch Worksheet'

    search_results = items_api.search_items(filters=['My*Sinusoid'],
                                            scope=[workbook.id])  # type: ItemSearchPreviewPaginatedListV1

    assert len(search_results.items) == 1
    assert search_results.items[0].id == sinusoid.id

    item_output = items_api.get_item_and_all_properties(id=sinusoid.id)  # type: ItemOutputV1
    assert item_output.name == 'My Second Sinusoid'

    # Now change it all back so that this test can run successfully twice
    workbook.name = 'My First From-Scratch Workbook'
    worksheet.name = 'My First From-Scratch Worksheet'
    sinusoid.name = 'My First Sinusoid'
    spy.workbooks.push(workbook, path=test_name)

    search_df = spy.workbooks.search({'Path': test_name})
    assert len(search_df) == 1


@pytest.mark.system
def test_globals():
    test_name = 'test_globals'
    workbooks = spy.workbooks.load(get_full_path_of_export('Globals (3ACFCBA0-F390-414F-BD9D-4AF93AB631A1)'))
    workbook = workbooks[0]
    workbook_id = workbook.id
    global_compressor_high_id = '2EF5FA09-A221-475D-AF19-5FBDF717E9FE'
    local_compressor_high_id = '0EF7A678-538F-FF60-BF5C-9FBF24A0DACE'

    items_api = ItemsApi(spy.session.client)

    #
    # Make sure a global item is reused if global_inventory='always reuse'
    #
    for reconcile_by in ['name', 'id']:
        workbooks = spy.workbooks.load(get_full_path_of_export('Globals (3ACFCBA0-F390-414F-BD9D-4AF93AB631A1)'))

        label = 'test_globals_' + _common.new_placeholder_guid()
        push_df = spy.workbooks.push(workbooks, label=label, datasource=test_name, global_inventory='always reuse',
                                     refresh=False, reconcile_inventory_by=reconcile_by)

        pushed_global_item_id_1 = push_df.spy.item_map[global_compressor_high_id]
        pushed_local_item_id_1 = push_df.spy.item_map[local_compressor_high_id]

        item_output = items_api.get_item_and_all_properties(id=pushed_global_item_id_1)
        assert item_output.scoped_to is None

        item_output = items_api.get_item_and_all_properties(id=pushed_local_item_id_1)
        assert item_output.scoped_to == push_df.spy.item_map[workbook_id]

        label = 'test_globals_' + _common.new_placeholder_guid()
        push_df = spy.workbooks.push(workbooks, label=label, datasource=test_name, global_inventory='always reuse',
                                     reconcile_inventory_by=reconcile_by)

        assert pushed_global_item_id_1 == push_df.spy.item_map[global_compressor_high_id]
        assert pushed_local_item_id_1 != push_df.spy.item_map[local_compressor_high_id]
        formula_parameters = workbooks[0].item_inventory[
            push_df.spy.item_map[local_compressor_high_id]]['Formula Parameters']
        assert formula_parameters == {'gch': pushed_global_item_id_1}

    # The last push was refresh=True, so we'll be pushing this time with an updated ID. The existing item will be
    # found and its identifiers used.
    label = 'test_globals_' + _common.new_placeholder_guid()
    push_df = spy.workbooks.push(workbooks, label=label, datasource=test_name, global_inventory='always reuse')

    # We confirm that the item was mapped to itself
    assert pushed_global_item_id_1 == push_df.spy.item_map[pushed_global_item_id_1]

    item_output = items_api.get_item_and_all_properties(id=pushed_global_item_id_1)
    assert item_output.scoped_to is None

    # noinspection PyUnresolvedReferences
    display_items_1 = workbooks[0].worksheets[0].display_items
    display_items_1_dict = display_items_1.set_index('Name')['ID'].to_dict()

    #
    # Make sure a new global item is created and used if the label differs
    #
    label = 'test_globals_' + _common.new_placeholder_guid()
    push_df = spy.workbooks.push(workbooks, label=label, datasource=test_name, scope_globals_to_workbook=False)

    assert push_df.spy.status.warnings == {
        'scope_globals_to_workbook=False is deprecated. Use global_inventory="copy global" instead.'}

    # noinspection PyUnresolvedReferences
    display_items_2 = workbooks[0].worksheets[0].display_items
    display_items_2_dict = display_items_2.set_index('Name')['ID'].to_dict()

    assert global_compressor_high_id not in push_df.spy.item_map

    pushed_global_item_id_2 = push_df.spy.item_map[display_items_1_dict['Global Compressor High']]
    assert display_items_2_dict['Global Compressor High'] == pushed_global_item_id_2

    assert pushed_global_item_id_1 != pushed_global_item_id_2

    item_output = items_api.get_item_and_all_properties(id=pushed_global_item_id_2)
    assert item_output.scoped_to is None

    # Reset, since workbooks had been refreshed
    workbooks = spy.workbooks.load(get_full_path_of_export('Globals (3ACFCBA0-F390-414F-BD9D-4AF93AB631A1)'))

    #
    # Make sure the global item is scoped to the workbook if scope_globals_to_workbook=True
    #
    label = 'test_globals_' + _common.new_placeholder_guid()
    push_df = spy.workbooks.push(workbooks, label=label, datasource=test_name, scope_globals_to_workbook=True)

    assert push_df.spy.status.warnings == {
        'scope_globals_to_workbook=True is deprecated. Use global_inventory="copy local" instead.'}

    item_output = items_api.get_item_and_all_properties(id=push_df.spy.item_map[global_compressor_high_id])
    assert item_output.scoped_to == push_df.spy.item_map[workbook_id]


@pytest.mark.system
def test_scalar_edit():
    scalars_api = ScalarsApi(spy.session.client)

    calculated_item_input = ScalarInputV1()
    calculated_item_input.name = 'A Scalar I Will Edit'
    calculated_item_input.formula = '42'
    calculated_item_output = scalars_api.create_calculated_scalar(
        body=calculated_item_input)  # type: CalculatedItemOutputV1

    workbook = spy.workbooks.Analysis('test_scalar_edit')
    worksheet = workbook.worksheet('The Only Worksheet')
    worksheet.display_items = spy.search({'ID': calculated_item_output.id})
    spy.workbooks.push(workbook)

    scalar = workbook.item_inventory[calculated_item_output.id]
    scalar['Formula'] = '43'
    spy.workbooks.push(workbook, scope_globals_to_workbook=False)

    scalar = Item.pull(calculated_item_output.id)
    assert scalar['Formula'] == '43'


@pytest.mark.system
def test_topic_document_archive_and_resurrect():
    topic = spy.workbooks.Topic('test_topic_document_archive_and_resurrect')
    topic.document('My Doc 1')
    spy.workbooks.push(topic)

    # Now clear the worksheets so that My Doc 1 gets archived
    topic.worksheets = list()
    topic.document('My Doc 2')
    spy.workbooks.push(topic)

    # Make sure we can push the topic with My Doc 1 again
    topic.document('My Doc 1')
    spy.workbooks.push(topic)


@pytest.mark.system
def test_push_workbooks_all_fail():
    workbook = Analysis({'Name': 'workbook_that_cant_be_pushed'})
    # We are just testing that the following line does not raise an exception
    push_df = spy.workbooks.push(workbook, errors='catalog')
    assert 'Error' in push_df.at[0, 'Result']


@pytest.mark.system
def test_pull_workbook():
    workbook = Analysis({'Name': 'test_pull_workbook'})
    worksheet = workbook.worksheet('worksheet')
    spy.workbooks.push(workbook)
    items_api = ItemsApi(spy.session.client)
    item_output = items_api.get_item_and_all_properties(id=worksheet.id)
    assert item_output.workbook_id == workbook.id


@pytest.mark.system
def test_content_selector_not_stripped():
    label = 'test_content_selector_not_stripped'

    class AssetForScorecardMetric(Asset):

        @Asset.Attribute()
        def power(self, metadata):
            return metadata[metadata['Name'].str.endswith('Power')]

        @Asset.Attribute()
        def power_kpi(self, metadata):
            return {
                'Type': 'Metric',
                'Measured Item': self.power(),
                'Statistic': 'Minimum',
                'Duration': '6h',
                'Period': '4h',
            }

        @Asset.Display()
        def my_display(self, metadata, analysis):
            analysis.definition['Name'] = label
            worksheet = analysis.worksheet(f'{label}_worksheet')
            worksheet.display_items = metadata
            worksheet.view = 'Table'
            return worksheet.current_workstep()

        @Asset.Document()
        def my_document(self, metadata, topic):
            topic.definition['Name'] = f'{label}_document'
            document = topic.document(f'{label}_document')
            document.render_template(asset=self, filename='', text="""
            <html>
              <body>
                <p>${display(display=asset.my_display(), height=500, width=500, selector=.screenshotSizeToContent)}</p>
              </body>
            </html>
            """)

    def _topic_document_search():
        spy_search = spy.search(query={'Name': f'{label}_document'})
        return spy.workbooks.search(query={"ID": str(spy_search[spy_search['Type'] == 'Topic']['ID'].iloc[0])})

    def _pull_workbooks_and_assert_selector_intact(wb_search_df):
        wbs = spy.workbooks.pull(wb_search_df)
        workbook = [wb for wb in wbs if wb['Name'] == f'{label}_document'][0]
        for worksheet in workbook.worksheets:
            for _, content in worksheet.content.items():
                assert content.definition['selector'] == '.screenshotSizeToContent'
        return wbs

    # Build a scorecard metric and insert it into the topic
    metadata_df = spy.search({'Name': 'Area A_*Power', 'Datasource Class': 'Time Series CSV Files'})
    metadata_df['Build Asset'] = 'Test Scorecard Content Asset'
    metadata_df['Build Path'] = 'Test Scorecard Content Path'
    build_df = spy.assets.build(AssetForScorecardMetric, metadata_df)
    spy.push(metadata=build_df, workbook=label, datasource=label, worksheet=f'{label}_worksheet')

    # Verify the selector is intact when initially pulled into spy...
    search_results = _topic_document_search()
    workbooks = _pull_workbooks_and_assert_selector_intact(search_results)

    # Verify the selector is still intact after pushing back to spy and pulling again...
    spy.workbooks.push(workbooks)
    _pull_workbooks_and_assert_selector_intact(search_results)


@pytest.mark.system
def test_date_range_condition_not_stripped():
    workbook = 'test_date_range_condition_not_stripped'

    def _pull_workbook_and_assert_condition_intact(condition_id):
        _workbooks_search = spy.workbooks.search(query={'Name': workbook})
        _workbooks = spy.workbooks.pull(_workbooks_search)
        _topic = [w for w in _workbooks if isinstance(w, Topic)][0]
        _worksheet = _topic.worksheets[0]
        assert len(_worksheet.date_ranges.values()) == 1
        _date_range = list(_worksheet.date_ranges.values())[0]
        assert _date_range.definition['Condition ID'] == condition_id
        return _workbooks

    condition = Item.load({
        'Name': 'Daily Condition',
        'Type': 'CalculatedCondition',
        'Formula': 'days()'
    })

    # Create and push workbooks with a DateRange with the above condition
    analysis = Analysis(f'{workbook}_Analysis')
    worksheet = analysis.worksheet(f'{workbook} Worksheet')
    worksheet.display_items = pd.DataFrame([{'ID': condition.id, 'Type': 'CalculatedCondition',
                                             'Name': condition.name}])
    analysis.item_inventory[condition.id] = condition
    topic = Topic(f'{workbook}_Topic')
    doc = topic.document('test_date_range_condition_not_stripped_Worksheet')
    test_date_range = DateRange({
        'Name': 'Testing DateRange',
        'Start': '2021-09-30T00:00:00Z',
        'End': '2021-10-01T00:00:00Z',
        'Condition ID': condition.id
    }, doc.report)
    doc.date_ranges[test_date_range.id] = test_date_range
    spy.workbooks.push([analysis, topic])

    # Pull the workbooks and ensure that the condition still exists
    pulled_workbook = _pull_workbook_and_assert_condition_intact(condition.id)

    # Push and pull the workbooks one more time to ensure that condition still exists
    spy.workbooks.push(pulled_workbook)
    _pull_workbook_and_assert_condition_intact(condition.id)


@pytest.mark.system
def test_interactive_content():
    workbooks = spy.workbooks.load(get_full_path_of_export(
        'Interactivity and Summarization (FDE3887F-85B7-4AC7-A8FE-792FE6A96551)'))
    workbooks += spy.workbooks.load(get_full_path_of_export(
        'Example Analysis (D833DC83-9A38-48DE-BF45-EB787E9E8375)'))

    label = 'test_interactive_content'
    push_df = spy.workbooks.push(workbooks, path=label, label=label, datasource=label, refresh=False)
    push_df.drop(columns=['ID'], inplace=True)
    push_df.rename(columns={'Pushed Workbook ID': 'ID'}, inplace=True)

    pulled_workbooks = spy.workbooks.pull(push_df[push_df['Name'] == 'Interactivity and Summarization'])
    interactivity_doc = pulled_workbooks[0].documents['Interactivity']
    content_items = list(interactivity_doc.content.values())
    interactivity = sorted([d['Interactive'] for d in content_items])
    assert interactivity == [False, True]


@pytest.mark.system
def test_asset_selection_summarization():
    def _pull_workbooks_and_assert_asset_selection_summarization_intact():
        workbooks_search = spy.workbooks.search({'Name': 'test_asset_selection_topic'})
        workbooks = spy.workbooks.pull(workbooks_search)
        test_topic = [wb for wb in workbooks if wb['Name'] == 'test_asset_selection_topic'][0]
        test_worksheet = test_topic.worksheets[0]

        # Assert that asset selection exists
        assert len(test_worksheet.asset_selections.values()) == 1
        test_asset_selection = list(test_worksheet.asset_selections.values())[0]
        assert test_asset_selection.definition['Name'] == 'My Asset Selection'
        test_asset_selection_id = test_asset_selection.id

        # Assert that content is associated with both asset selection and summarization
        assert len(test_worksheet.content.values()) == 1
        test_content = list(test_worksheet.content.values())[0]
        assert test_content.definition['Asset Selection ID'] == test_asset_selection_id
        assert test_content.definition['Summary Type'] == 'DISCRETE'
        assert test_content.definition['Summary Value'] == '30min'

        return workbooks

    analysis = Analysis('test_asset_selection_analysis')
    worksheet = analysis.worksheet('test_asset_selection_worksheet')
    asset_search = spy.search({
        'Name': 'Area A',
        'Path': 'Example >> Cooling Tower 1',
        'Type': 'Asset'
    })
    asset_id = asset_search['ID'][0]
    signal_search = spy.search({
        'Asset': asset_id
    })
    worksheet.display_items = signal_search
    topic = Topic('test_asset_selection_topic')
    doc = topic.document('test_asset_selection_document')

    asset_selection = AssetSelection({
        'Name': 'My Asset Selection',
        'Asset ID': asset_id,
        'Path Levels': 2
    }, report=doc.report)
    doc.asset_selections[asset_selection.id] = asset_selection
    analysis.item_inventory[asset_id] = Item.pull(asset_id)

    content = Content({
        'Name': 'test_asset_selection_content',
        'Width': 200,
        'Height': 100,
        'Asset Selection ID': asset_selection.id,
        'Summary Type': 'DISCRETE',
        'Summary Value': '30min',
        'Workbook ID': analysis.id,
        'Worksheet ID': worksheet.id,
        'Workstep ID': worksheet.current_workstep().id,
        'selector': None
    }, report=doc.report)

    doc.content[content.id] = content
    doc.html = content.html
    spy.workbooks.push([analysis, topic])

    pushed_content = Content.pull(content.id)
    hash_before_push = content.name
    hash_after_push = pushed_content.name
    assert hash_before_push == hash_after_push

    # Pull the workbooks and ensure that asset selection and summarization are intact
    pulled_workbooks = _pull_workbooks_and_assert_asset_selection_summarization_intact()

    # Push and pull the workbooks one more time to ensure asset selection and summarization still intact
    spy.workbooks.push(pulled_workbooks)
    _pull_workbooks_and_assert_asset_selection_summarization_intact()


@pytest.mark.system
def test_push_workbook_seeq_internal_datasource():
    path = f'test_push_workbook_seeq_internal_datasource {_common.new_placeholder_guid()}'

    # Verify a new workbook and all its items are pushed to the Seeq-internal datasources and that their ACLs
    # inherit from the workbook rather than a datasource
    workbooks = test_load.load_example_export()
    workbooks = [w for w in workbooks if isinstance(w, Analysis)]
    push_result_1 = spy.workbooks.push(workbooks, path=path, datasource=spy.INHERIT_FROM_WORKBOOK)
    assert len(push_result_1) == 1
    workbook_id = push_result_1.at[0, 'Pushed Workbook ID']
    search_results = spy.search(query={'Name': '*', 'Scoped To': workbook_id}, all_properties=True)
    actual_datasources = search_results['Datasource Class'].unique()
    unexpected = list(actual_datasources).remove(spy.INHERIT_FROM_WORKBOOK)
    error = f"Unexpected datasources were found in newly-pushed search results: {unexpected}" if unexpected else ''
    assert not error
    acls = spy.acl.pull(search_results)
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
    push_result_2 = spy.workbooks.push(workbooks, path=path, datasource=spy.INHERIT_FROM_WORKBOOK)
    assert push_result_2.at[0, 'Pushed Workbook ID'] == workbook_id
    search_results = spy.search(query={'Name': '*', 'Scoped To': workbook_id}, all_properties=True)
    actual_datasources = search_results['Datasource Class'].unique()
    unexpected = list(actual_datasources).remove(spy.INHERIT_FROM_WORKBOOK)
    error = f"Unexpected datasources were found in re-pushed search results: {unexpected}" if unexpected else ''
    assert not error

    # Round-tripping should maintain the datasource if specified
    workbook_search = spy.workbooks.search({'ID': workbook_id, 'Workbook Type': 'Analysis'}, all_properties=True)
    workbook_pull = spy.workbooks.pull(workbook_search)
    push_result_3 = spy.workbooks.push(workbook_pull, path=path, datasource=spy.INHERIT_FROM_WORKBOOK)
    assert push_result_3.at[0, 'Pushed Workbook ID'] == workbook_id
    search_results = spy.search(query={'Name': '*', 'Scoped To': workbook_id}, all_properties=True)
    actual_datasources = search_results['Datasource Class'].unique()
    unexpected = list(actual_datasources).remove(spy.INHERIT_FROM_WORKBOOK)
    error = f"Unexpected datasources were found in re-pushed search results: {unexpected}" if unexpected else ''
    assert not error


def _push_and_assert_redaction(workbooks, path, expected_warning, errors_from_workbook: bool):
    status = spy.Status(errors='catalog')
    push_results = spy.workbooks.push(workbooks, path=path, label=path, status=status)
    if errors_from_workbook:
        errors = {e for wb in workbooks for e in wb.push_errors}
    else:
        errors = status.warnings
    assert len(push_results) == len(workbooks), f'Push results size should match push input. ' \
                                                f'\nInput: {workbooks}' \
                                                f'\nOutput: {push_results}' \
                                                f'\nWarnings: {errors}'
    assert len(errors) >= 1, f'No warnings found in status {status}'
    warning_matches = [w for w in errors if expected_warning in w]
    assert warning_matches, f'Expected warning "{expected_warning}" not found in {errors}'


@pytest.mark.system
def test_redacted_push_create_content():
    content_name = f'test_redacted_push_content_{_common.new_placeholder_guid()}'
    reason = 'Negative'
    mock_exception_thrower = mock.Mock(side_effect=ApiException(status=403, reason=reason))

    with mock.patch('seeq.sdk.ContentApi.create_content', new=mock_exception_thrower):
        workbooks = test_load.load_example_export()
        for workbook in workbooks:
            workbook.name += content_name
        _push_and_assert_redaction(workbooks, content_name, 'Error processing Content:', True)


@pytest.mark.system
def test_redacted_push_create_folder():
    folder_name = f'test_redacted_push_folder_{_common.new_placeholder_guid()}'
    reason = 'Negative'
    mock_exception_thrower = mock.Mock(side_effect=ApiException(status=403, reason=reason))
    with mock.patch('seeq.sdk.FoldersApi.create_folder', new=mock_exception_thrower):
        workbooks = test_load.load_example_export()
        for workbook in workbooks:
            workbook.name += folder_name
        _push_and_assert_redaction(workbooks, folder_name, 'Failed to create Folder', False)


@pytest.mark.system
def test_invalid_ui_config():
    # Set up: Create two calcs. Set one to have a valid UIConfig and the other to invalid.
    name = f'test_invalid_ui_config_{_common.new_placeholder_guid()}'
    wb = f'workbook_{name}'
    invalid_ui_config = 'Not{a:valid:json_string]'
    valid_ui_config = '{"type": "formula", "advancedParametersCollapsed": true, "helpShown": true}'
    workbook = Analysis({'Name': wb})
    worksheet = workbook.worksheet(wb)

    signal = CalculatedSignal({
        'Name': f'signal_calc_{name}',
        'Formula': 'sinusoid()',
        'UIConfig': valid_ui_config
    })
    workbook.add_to_scope(signal)
    condition = CalculatedCondition({
        'Name': f'condition_calc_{name}',
        'Formula': 'days()',
        'UIConfig': invalid_ui_config
    })
    workbook.add_to_scope(condition)
    worksheet.display_items = [signal, condition]
    spy.workbooks.push(workbook, refresh=True)

    # Assert that the UIConfigs were pushed
    items_api = ItemsApi(spy.session.client)
    assert 'advancedParametersCollapsed' in items_api.get_property(id=signal.id, property_name='UIConfig').value
    assert invalid_ui_config in items_api.get_property(id=condition.id, property_name='UIConfig').value

    # Re-pull and verify the properties
    search_results = spy.workbooks.search({'Name': wb})
    workbooks_pull = spy.workbooks.pull(search_results)
    assert len(workbooks_pull) == 1
    workbook_pull = workbooks_pull[0]
    workbook_pull.item_inventory.items()
    pulled_signal = [i[1] for i in workbook_pull.item_inventory.items() if 'signal' in i[1].name][0]
    assert pulled_signal['UIConfig'] == json.loads(valid_ui_config)
    pulled_condition = [i[1] for i in workbook_pull.item_inventory.items() if 'condition' in i[1].name][0]
    assert pulled_condition['UIConfig'] == invalid_ui_config
    # Re-pushing is also fine
    spy.workbooks.push(workbooks_pull)


def test_worksheet_push_workstep():
    temperature_signals = spy.search({
        'Datasource ID': 'Example Data',
        'Path': 'Example >> Cooling Tower 1',
        'Name': 'Temperature'
    }, workbook=spy.GLOBALS_ONLY)

    # search for a base signal to compute aggregates on (you'd likely have this handy already)
    base_signal = temperature_signals[temperature_signals['Asset'] == 'Area C'].iloc[0]

    # create workbook with one worksheet that contains all the above temperature signals
    workbook_name = 'test_worksheet_push_workstep'
    worksheet_name = 'the one worksheet'
    workbook = spy.workbooks.Analysis({'Name': workbook_name})
    worksheet = workbook.worksheet(worksheet_name)
    worksheet.display_items = temperature_signals.sort_values(by='Asset')
    workbook_df = spy.workbooks.push(workbook, path=workbook_name, quiet=True)
    workbook_id = workbook_df.iloc[0]['Pushed Workbook ID']
    worksheet_id = worksheet.id

    # Do the work!
    start = time()
    workbooks = spy.workbooks.pull(workbook_id, specific_worksheet_ids=[worksheet_id], include_annotations=False,
                                   include_images=False, include_inventory=False, include_referenced_workbooks=False)
    worksheet = workbooks[0].worksheets[worksheet_id]
    print(f"Took {int((time() - start) * 1000)} milliseconds to pull the worksheet")

    start = time()

    # new items we want to add or update
    new_signals = spy.push(metadata=pd.DataFrame([{
        'Name': f'{base_signal["Name"]}: Mean 1h',
        'Formula': '$s.aggregate(average(), periods(1h), middleKey())',
        'Formula Parameters': {'$s': base_signal['ID']},
        'Line Style': 'Short Dash',
    }, {
        'Name': f'{base_signal["Name"]}: StdDev 1h',
        'Formula': '$s.aggregate(stdDev(), periods(1h), middleKey())',
        'Formula Parameters': {'$s': base_signal['ID']}
    }]), workbook=workbook_id, worksheet=None)

    display_items = worksheet.display_items
    display_items = pd.concat([display_items, new_signals], ignore_index=True)

    # remove items
    area_b_ids = temperature_signals[temperature_signals['Asset'] == 'Area B']['ID'].to_list()
    area_b_display_items = display_items[display_items['ID'].isin(area_b_ids)]
    display_items.drop(area_b_display_items.index, inplace=True)

    worksheet.display_items = display_items
    # worksheet.update_display_items(new_items, remove_items)
    worksheet.display_range = {'Start': isodate.parse('2022-08-01T00:00:00Z'),
                               'End': isodate.parse('2022-08-07T00:00:00Z')}
    worksheet.push_current_workstep()
    print(f"Took {int((time() - start) * 1000)} milliseconds to update workstep")

    # Assert new items were created/updated properly
    worksheet.pull_current_workstep()
    display_items = worksheet.display_items
    for _, item in new_signals.iterrows():
        found = display_items[display_items['Name'] == item['Name']]
        assert len(found) == 1
        for p in item.to_dict().keys():
            if p == 'Type':
                assert item[p].replace('Calculated', '') == found[p].iloc[0]
            elif p in found.columns:
                assert (item[p] == found[p].iloc[0]) or (pd.isna(item[p]) and pd.isna(found[p].iloc[0]))

    # Assert remove items are not present
    for area_b_id in area_b_ids:
        found = display_items[display_items['ID'] == area_b_id]
        assert len(found) == 0


@pytest.mark.system
def test_date_range_permutations():
    template_workbooks = spy.workbooks.load(test_load.get_workbook_templates_path())
    permutations_workbooks = spy.workbooks.load(get_full_path_of_export(
        'Date Range Permutations (8DB737D0-9EE4-45C2-AA0C-F36A2F3265F3)'))

    label = 'test_date_range_permutations'
    push_df = spy.workbooks.push(template_workbooks + permutations_workbooks, path=label, label=label, refresh=False)

    push_df = push_df.drop(columns=['ID']).rename(columns={'Pushed Workbook ID': 'ID'})

    pulled_workbooks = spy.workbooks.pull(push_df)

    topic = [wb for wb in pulled_workbooks if 'Permutations' in wb.name][0]

    def _get_date_range(_date_range_name):
        for worksheet in topic.worksheets:
            for _date_range in worksheet.date_ranges.values():
                if _date_range.name == _date_range_name and not _date_range['Archived']:
                    return _date_range
        return None

    date_range = _get_date_range('Date Range: Fixed')
    assert not date_range.get('Auto Enabled', False)
    assert pd.to_datetime(date_range['Start']) == pd.to_datetime("2023-04-11T17:04:23.012000+00:00")
    assert pd.to_datetime(date_range['End']) == pd.to_datetime("2023-04-12T17:04:23.012000+00:00")
    assert date_range['Capsule Picker'] == {'Strategy': 'closestTo', 'Reference': 'end', 'Offset': 1}

    date_range = _get_date_range('Date Range: Fixed: Condition')
    assert not date_range.get('Auto Enabled', False)
    assert pd.to_datetime(date_range['Start']) == pd.to_datetime("2023-04-11T15:00:00+00:00")
    assert pd.to_datetime(date_range['End']) == pd.to_datetime("2023-04-11T23:00:00+00:00")

    date_range = _get_date_range('Date Range: Auto Update: Daily Schedule')
    assert date_range.get('Auto Enabled', False)
    assert date_range['Auto Duration'] == '86400.0s'
    assert date_range['Auto Offset'] == '30min'
    assert date_range['Auto Offset Direction'] == 'Past'

    date_range = _get_date_range('Date Range: Auto Update: Condition')
    assert date_range.get('Auto Enabled', False)
    assert date_range['Auto Duration'] == '604800.0s'
    assert date_range['Auto Offset'] == '12min'
    assert date_range['Auto Offset Direction'] == 'Past'
    assert date_range['Condition ID'] == [i for i in topic.item_inventory.values() if i.name == 'Shifts'][0].id

    assert date_range.report.schedule == {
        'Enabled': True,
        'Background': True,
        'Cron Schedule': [
            "0 0 8 ? * 1,2,3,4,5,6,7",
            "0 0 9 ? * 1,2,3,4,5,6,7"
        ]
    }

    date_range = _get_date_range('Date Range: Auto Update: Live')
    assert date_range.get('Auto Enabled', False)
    assert date_range['Auto Duration'] == '3600.0s'
    assert date_range['Auto Offset'] == '0min'
    assert date_range['Auto Offset Direction'] == 'Past'
    assert 'Condition ID' not in date_range

    assert date_range.report.schedule == {
        'Enabled': True,
        'Background': False,
        'Cron Schedule': [
            "*/5 * * * * ?"
        ]
    }


@pytest.mark.system
def test_annotation_interested_in_capsule():
    # CRAB-37591: Push had a bug where round-tripping an Analysis would fail if a Journal was InterestedIn a Capsule.
    # Setup: Create a Workbook, Worksheet, and Condition, then make the Worksheet's Journal be InterestedIn
    # a Capsule (in addition to the Workbook, Worksheet, and Condition).
    name = f'test_annotation_interested_in_capsule_{_common.new_placeholder_guid()}'
    input_workbook = spy.workbooks.Analysis(name)
    input_workbook.worksheet(name)
    spy.workbooks.push(input_workbook)
    workbook_id = input_workbook.id
    worksheet_id = input_workbook.worksheets[0].id
    journal_id = input_workbook.worksheets[0].journal.id
    condition_df = spy.push(metadata=pd.DataFrame([{'Name': name, 'Formula': 'days()'}]), workbook=workbook_id)
    condition_id = condition_df.iloc[0]['ID']
    formulas_api = FormulasApi(spy.session.client)
    capsules_result = formulas_api.run_formula(formula='$days',
                                               start='2023-01-01T00:00:00Z',
                                               end='2023-01-02T00:00:00Z',
                                               parameters=[f'days={condition_id}'])
    capsule_id = capsules_result.capsules.capsules[0].id
    annotations_api = AnnotationsApi(spy.session.client)
    annotation_input = AnnotationInputV1(name=name,
                                         document=name,
                                         interests=[
                                             AnnotationInterestInputV1(interest_id=workbook_id),
                                             AnnotationInterestInputV1(interest_id=worksheet_id),
                                             AnnotationInterestInputV1(interest_id=condition_id),
                                             AnnotationInterestInputV1(interest_id=condition_id, detail_id=capsule_id)
                                         ])
    annotation_orig = annotations_api.update_annotation(id=journal_id, body=annotation_input)

    # Test: That workbook should successfully be round-tripped without erroring
    search_result = spy.workbooks.search({'ID': f'{workbook_id}'})
    pull_result = spy.workbooks.pull(search_result)
    push_result = spy.workbooks.push(pull_result)
    assert len(push_result) == 1
    # And the Journal's document should not have been altered, although it will have been updated
    annotation_updated = annotations_api.get_annotation(id=journal_id)
    assert annotation_input.document == annotation_updated.document
    assert annotation_orig.updated_at != annotation_updated.updated_at


@pytest.mark.system
def test_round_trip_metrics_with_custom_colors():
    # CRAB-37789: Push had a bug where round-tripping a Threshold Metric would drop custom Threshold colors.
    # Setup: Create a Workbook, input Signals/Conditions, and a Metric with custom colored Thresholds
    name = f'test_round_trip_metrics_with_custom_colors_{_common.new_placeholder_guid()}'
    color_1 = '#ff0000'
    color_2 = '#00ff00'
    color_3 = '#0000ff'
    color_neutral = '#ffffff'
    initial_metadata = pd.DataFrame([{
        'Name': f'Measured Signal {name}',
        'Formula': 'sinusoid()'
    }, {
        'Name': f'Threshold Signal {name}',
        'Formula': 'toSignal(0.5)'
    }, {
        'Name': f'Bounding Condition {name}',
        'Formula': 'hours()'
    }, {
        'Name': f'Metric {name}',
        'Type': 'Metric',
        'Measured Item': f'Measured Signal {name}',
        'Aggregation Function': 'average()',
        'Bounding Condition': f'Bounding Condition {name}',
        'Metric Neutral Color': color_neutral,
        'Thresholds': {
            f'Hi{color_1}': 0.25,
            f'HiHi{color_2}': f'Threshold Signal {name}',
            f'HiHiHi{color_3}': 0.75
        }
    }])
    initial_push_results = spy.push(metadata=initial_metadata, workbook=name)
    metric_id = initial_push_results[initial_push_results['Type'] == 'ThresholdMetric'].iloc[0]['ID']

    # Verify that the metric was created with the expected colors from the API directly
    def _assert_metric_thresholds_match_expected(_neutral):
        metrics_api = MetricsApi(spy.session.client)
        _metric_data = metrics_api.get_metric(id=metric_id)  # ThresholdMetricOutputV1
        assert _metric_data.name == f'Metric {name}'
        assert _metric_data.neutral_color == _neutral
        for threshold in _metric_data.thresholds:
            if threshold.priority.level == 1:
                assert threshold.priority.color == color_1
                assert threshold.value.value == 0.25
            elif threshold.priority.level == 2:
                assert threshold.priority.color == color_2
            elif threshold.priority.level == 3:
                assert threshold.priority.color == color_3
                assert threshold.value.value == 0.75
            else:
                raise AssertionError(f'Unexpected Threshold priority level {threshold}')

    _assert_metric_thresholds_match_expected(color_neutral)

    # Round-tripping through Metadata Push should result in unchanged thresholds
    metadata_pull = spy.search({'ID': metric_id}, all_properties=True)
    spy.push(metadata=metadata_pull)
    _assert_metric_thresholds_match_expected(color_neutral)

    # Round-tripping through Workbooks Push should result in unchanged thresholds
    workbooks_pull = spy.workbooks.pull(spy.workbooks.search({'Name': name}))
    new_color_neutral = '#DDAADD'
    item_inventory_by_name = {v['Name']: v for k, v in workbooks_pull[0].item_inventory.items()}
    metric_dict = item_inventory_by_name[f'Metric {name}']
    metric_dict['Formula Parameters']['Metric Neutral Color'] = new_color_neutral
    spy.workbooks.push(workbooks_pull)
    _assert_metric_thresholds_match_expected(new_color_neutral)


@pytest.mark.system
def test_push_local_asset_group():
    unique_guid = _common.new_placeholder_guid()
    workbook = 'test_push_local_asset_group ' + unique_guid
    workbook_id = test_common.create_test_asset_group(spy.session, workbook)

    workbooks_1 = spy.workbooks.pull(workbook_id)
    workbook_1 = workbooks_1[0]
    assert len(workbook_1.item_inventory) == 8
    sorted_inventory_1 = sorted(workbook_1.item_inventory.values(), key=lambda x: x.fqn)

    # Here we change some values to confirm that the push will update existing local StoredItems. Note that push
    # currently (as of Jan 2024) does NOT update local CalculatedItems. This is a limitation that maybe should be
    # addressed at some point in the future, but it hasn't been requested AFAIK and we'd have to be careful not to cause
    # a performance issue (because we'd suddenly start pushing items in a lot more cases).
    for item in workbook_1.item_inventory.values():
        item['Name'] = item['Name'].replace('First', '1st').replace('Second', '2nd')

    pushed_2_df = spy.workbooks.push(workbook_1, refresh=False)
    pushed_2_workbook_id = pushed_2_df.iloc[0]['Pushed Workbook ID']
    workbooks_2 = spy.workbooks.pull(pushed_2_workbook_id)
    workbook_2 = workbooks_2[0]
    sorted_inventory_2 = sorted(workbook_2.item_inventory.values(), key=lambda x: x.fqn)
    assert [item['ID'] for item in sorted_inventory_1] == [item['ID'] for item in sorted_inventory_2]

    pushed_3_df = spy.workbooks.push(workbook_2, label=unique_guid, refresh=False)
    pushed_3_workbook_id = pushed_3_df.iloc[0]['Pushed Workbook ID']
    workbooks_3 = spy.workbooks.pull(pushed_3_workbook_id)
    assert len(workbooks_3) == 1
    workbook_3 = workbooks_3[0]
    assert len(workbook_3.item_inventory) == 8
    sorted_inventory_3 = sorted(workbook_3.item_inventory.values(), key=lambda x: x.fqn)
    assert [item['ID'] for item in sorted_inventory_2] != [item['ID'] for item in sorted_inventory_3]

    reprs = [re.sub(_common.GUID_REGEX, 'GUID', str(item)) for item in sorted_inventory_3]
    assert reprs == [
        'Folder "Agent API Key" (GUID)',
        'StoredSignal "Area A_Temperature" (GUID)',
        'StoredSignal "Area B_Temperature" (GUID)',
        'Asset "My Root Asset" (GUID)',
        'Asset "My Root Asset >> My 1st Asset" (GUID)',
        'CalculatedSignal "My Root Asset >> My 1st Asset >> Temperature" (GUID)',
        'Asset "My Root Asset >> My 2nd Asset" (GUID)',
        'CalculatedSignal "My Root Asset >> My 2nd Asset >> Temperature" (GUID)'
    ]

    assert 'Parent ID' not in sorted_inventory_3[1]
    assert 'Parent ID' not in sorted_inventory_3[2]
    assert sorted_inventory_3[3]['Parent ID'] == Item.ROOT
    assert sorted_inventory_3[4]['Parent ID'] == sorted_inventory_3[3]['ID']
    assert sorted_inventory_3[5]['Parent ID'] == sorted_inventory_3[4]['ID']
    assert sorted_inventory_3[6]['Parent ID'] == sorted_inventory_3[3]['ID']
    assert sorted_inventory_3[7]['Parent ID'] == sorted_inventory_3[6]['ID']


@pytest.mark.system
def test_push_local_asset_tree():
    workbook = 'test_push_local_asset_tree'

    pushed_metadata_df = spy.push(metadata=pd.DataFrame([{
        'Type': 'StoredSignal',
        'Name': 'Spin Rate',
        'Path': f'{workbook} >> Building 12',
        'Asset': 'Mangler',
    }, {
        'Type': 'StoredSignal',
        'Name': 'Spin Rate',
        'Path': f'{workbook} >> Building 42',
        'Asset': 'Mangler'
    }]), workbook=workbook, worksheet=None, datasource=workbook)

    workbook_id = pushed_metadata_df.spy.workbook_id
    workbooks_1 = spy.workbooks.pull(workbook_id)
    assert len(workbooks_1) == 1
    workbook_1 = workbooks_1[0]
    sorted_inventory_1 = sorted(workbook_1.item_inventory.values(), key=lambda x: x.fqn)
    assert workbook_id in sorted_inventory_1[1]['Data ID']

    pushed_2_df = spy.workbooks.push(workbook_1, label='new', refresh=False)
    pushed_2_workbook_id = pushed_2_df.iloc[0]['Pushed Workbook ID']
    workbooks_2 = spy.workbooks.pull(pushed_2_workbook_id)
    assert len(workbooks_2) == 1
    workbook_2 = workbooks_2[0]
    assert len(workbook_2.item_inventory) == 8
    sorted_inventory_2 = sorted(workbook_2.item_inventory.values(), key=lambda x: x.fqn)
    assert [item['ID'] for item in sorted_inventory_1] != [item['ID'] for item in sorted_inventory_2]
    assert sorted_inventory_1[1]['ID'] in sorted_inventory_2[1]['Data ID']

    reprs = [re.sub(_common.GUID_REGEX, 'GUID', str(item)) for item in sorted_inventory_2]
    assert reprs == [
        'Folder "Agent API Key" (GUID)',
        'Asset "test_push_local_asset_tree" (GUID)',
        'Asset "test_push_local_asset_tree >> Building 12" (GUID)',
        'Asset "test_push_local_asset_tree >> Building 12 >> Mangler" (GUID)',
        'StoredSignal "test_push_local_asset_tree >> Building 12 >> Mangler >> Spin Rate" (GUID)',
        'Asset "test_push_local_asset_tree >> Building 42" (GUID)',
        'Asset "test_push_local_asset_tree >> Building 42 >> Mangler" (GUID)',
        'StoredSignal "test_push_local_asset_tree >> Building 42 >> Mangler >> Spin Rate" (GUID)'
    ]

    assert sorted_inventory_2[1]['Parent ID'] == Item.ROOT
    assert sorted_inventory_2[2]['Parent ID'] == sorted_inventory_2[1]['ID']
    assert sorted_inventory_2[3]['Parent ID'] == sorted_inventory_2[2]['ID']
    assert sorted_inventory_2[4]['Parent ID'] == sorted_inventory_2[3]['ID']
    assert sorted_inventory_2[5]['Parent ID'] == sorted_inventory_2[1]['ID']
    assert sorted_inventory_2[6]['Parent ID'] == sorted_inventory_2[5]['ID']
    assert sorted_inventory_2[7]['Parent ID'] == sorted_inventory_2[6]['ID']


@pytest.mark.system
def test_display_items_workstep_version_part1():
    test_name = 'test_display_items_workstep_version'
    workbook = Analysis({'Name': test_name, 'ID': '42B28FE8-0CB4-4B3B-89B6-B0322B5CADC4'})
    worksheet = workbook.worksheet('The Worksheet')
    signals_df = spy.search({'Name': 'Area A_Temperature', 'Datasource ID': 'Example Data'},
                            workbook=spy.GLOBALS_ONLY)
    condition_df = spy.push(metadata=pd.DataFrame([{
        'Name': 'Temperature A above 90',
        'Type': 'CalculatedCondition',
        'Formula': '$Temperature > 90',
        'Formula Parameters': {
            '$Temperature': signals_df[signals_df['Name'] == 'Area A_Temperature'].iloc[0]['ID']
        }
    }]))
    worksheet.display_items = pd.concat([signals_df, condition_df], ignore_index=True)

    assert worksheet.current_workstep()._get_workstep_version() == 1

    spy.workbooks.push(workbook)

    assert len(worksheet.display_items) == 2


@pytest.mark.ignore
def test_display_items_workstep_version_part2():
    #
    # THIS TEST REQUIRES A MANUAL STEP.
    #
    # Run Seeq Server via "sq run" so you can log into it.
    # Make sure test_display_items_workstep_version_part1() has been run and succeeds.
    # Log in to Seeq Server using the credentials in the sq-run-data-dir/keys/agent.key file.
    # Go to "My Folder" and open the "test_display_items_workstep_version" workbook (pushed in part1).
    # There should be a signal and a condition in the Display pane.
    # Change the display range slightly so that a new workstep is written. This will upgrade the
    # workstep to the latest version and rename sqTrendCapsuleSetStore to sqTrendConditionStore.
    #
    # Once you have done this, you can run this test.
    #

    search_df = spy.workbooks.search({'Name': 'test_display_items_workstep_version'})
    workbooks = spy.workbooks.pull(search_df)
    assert len(workbooks) == 1
    worksheet = workbooks[0].worksheets['The Worksheet']

    workstep = worksheet.current_workstep()
    workstep_version = workstep._get_workstep_version()
    assert workstep_version > 1

    if workstep_version >= 62:
        assert 'sqTrendConditionStore' in workstep['Data']['state']['stores']
        assert 'sqTrendCapsuleSetStore' not in workstep['Data']['state']['stores']
    else:
        assert 'sqTrendCapsuleSetStore' in workstep['Data']['state']['stores']
        assert 'sqTrendConditionStore' not in workstep['Data']['state']['stores']

    signals_df = spy.search({'Name': 'Area B_Temperature', 'Datasource ID': 'Example Data'},
                            workbook=spy.GLOBALS_ONLY)
    condition_df = spy.push(metadata=pd.DataFrame([{
        'Name': 'Temperature B above 90',
        'Type': 'CalculatedCondition',
        'Formula': '$Temperature > 90',
        'Formula Parameters': {
            '$Temperature': signals_df[signals_df['Name'] == 'Area B_Temperature'].iloc[0]['ID']
        }
    }]))

    new_display_items = pd.concat([worksheet.display_items, signals_df, condition_df], ignore_index=True)
    new_display_items.drop_duplicates(subset='ID', inplace=True)
    worksheet.display_items = new_display_items

    workstep = worksheet.current_workstep()
    workstep_version = workstep._get_workstep_version()
    if workstep_version >= 62:
        assert 'sqTrendConditionStore' in workstep['Data']['state']['stores']
        assert 'sqTrendCapsuleSetStore' not in workstep['Data']['state']['stores']
    else:
        assert 'sqTrendCapsuleSetStore' in workstep['Data']['state']['stores']
        assert 'sqTrendConditionStore' not in workstep['Data']['state']['stores']

    spy.workbooks.push(workbooks)

    assert len(worksheet.display_items) == 4


@pytest.mark.system
def test_push_tree_with_mixed_scopes():
    test_name = 'test_push_tree_with_mixed_scopes ' + _common.new_placeholder_guid()
    workbook_1 = f'{test_name} 1'
    workbook_2 = f'{test_name} 2'

    push_1_df = spy.push(metadata=pd.DataFrame([{
        'Type': 'Signal',
        'Name': 'Signal 1',
        'Path': workbook_1
    }]), workbook=workbook_1, worksheet=None, datasource=test_name)
    workbook_1_id = push_1_df.spy.workbook_id

    push_2_df = spy.push(metadata=pd.DataFrame([{
        'Type': 'Signal',
        'Name': 'Signal 2',
        'Formula': '1.toSignal()'
    }]), workbook=workbook_2, worksheet=None, datasource=test_name)
    workbook_2_id = push_2_df.spy.workbook_id

    tree_root_1_id = push_1_df[push_1_df['Type'] == 'Asset'].iloc[0]['ID']
    signal_2_id = push_2_df.iloc[0]['ID']

    trees_api = TreesApi(spy.client)
    trees_api.move_nodes_to_parent(
        parent_id=tree_root_1_id,
        body=ItemIdListInputV1(items=[signal_2_id])
    )

    workbooks_2 = spy.workbooks.pull(push_2_df.spy.workbook_id)
    workbook_2 = workbooks_2[0]

    assert tree_root_1_id in workbook_2.item_inventory
    assert workbook_2.item_inventory[tree_root_1_id]['Scoped To'] == workbook_1_id
    assert workbook_2.item_inventory[signal_2_id]['Scoped To'] == workbook_2_id

    # Since the tree root is scoped to workbook 1, pushing workbook 2 should not change the tree root's scope but
    # instead make a copy for workbook 2.
    spy.workbooks.push(workbooks_2, datasource=test_name, refresh=True, reconcile_inventory_by='name')
    assert tree_root_1_id not in workbook_2.item_inventory
    assert workbook_2.item_inventory[signal_2_id]['Scoped To'] == workbook_2_id
    tree_root_2_id = [k for k, v in workbook_2.item_inventory.items() if v['Name'] == workbook_1][0]
    assert workbook_2.item_inventory[tree_root_2_id]['Scoped To'] == workbook_2_id


@pytest.mark.system
def test_reconcile_inventory_by_name():
    test_name = f'test_reconcile_inventory_by {_common.new_placeholder_guid()}'
    pushed_df = spy.push(metadata=pd.DataFrame([{
        'Path': 'My Root',
        'Asset': 'My Asset',
        'Name': 'My Signal 1',
        'Formula': '1.toSignal()'
    }]), workbook=test_name, datasource=test_name, worksheet='My Worksheet')

    workbooks = spy.workbooks.pull(pushed_df.spy.workbook_id)
    workbook = workbooks[0]

    with pytest.raises(SPyValueError, match='reconcile_inventory_by must be either "id" or "name"'):
        spy.workbooks.push(workbook, label=test_name, reconcile_inventory_by='ID')

    spy.workbooks.push(workbook, label=test_name, reconcile_inventory_by='name')

    tree = spy.assets.Tree('My Root', workbook=workbook.id)
    assert str(tree) == textwrap.dedent("""
        My Root
        |-- My Asset
            |-- My Signal 1
    """).strip()
    tree.insert('My Signal 2', 'My Asset', formula='2.toSignal()')
    tree.push()

    tree = spy.assets.Tree('My Root', workbook=workbook.id)
    assert str(tree) == textwrap.dedent("""
        My Root
        |-- My Asset
            |-- My Signal 1
            |-- My Signal 2
    """).strip()

    search_results = spy.search({'Path': '', 'Asset': 'My Root'}, workbook=workbook.id, recursive=True)
    assert len(search_results) == 4
    assert sorted(search_results['Name'].to_list()) == [
        'My Asset',
        'My Root',
        'My Signal 1',
        'My Signal 2'
    ]
