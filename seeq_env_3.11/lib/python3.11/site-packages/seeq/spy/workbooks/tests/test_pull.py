import json
import os
import tempfile
from unittest import mock

import pandas as pd
import pytest

from seeq import spy
from seeq.base import util
from seeq.sdk import WorkbooksApi
from seeq.sdk.rest import ApiException
from seeq.spy import _common
from seeq.spy._errors import SPyRuntimeError
from seeq.spy.tests import test_common
from seeq.spy.tests.test_common import Sessions
from seeq.spy.workbooks import Workbook, Topic, TopicDocument
from seeq.spy.workbooks._item import Reference
from seeq.spy.workbooks.tests import test_load


def setup_module():
    test_common.initialize_sessions()


def _push_example_export(label, session=None):
    example_export_push_df = spy.workbooks.push(
        test_load.load_example_export(), refresh=False, path=label, label=label, session=session)
    example_export_push_df.drop(columns=['ID'], inplace=True)
    example_export_push_df.rename(columns={'Pushed Workbook ID': 'ID'}, inplace=True)
    example_export_push_df['Type'] = 'Workbook'
    return example_export_push_df


# The tests for pulling workbooks are light because so much of the functionality is tested in the push code. I.e.,
# the push code wouldn't work if the pull code had a problem, since the pull code is what produced the saved workbooks.
# (Same goes for the spy.workbooks.save() functionality.)

@pytest.mark.system
def test_pull():
    example_export_push_df = _push_example_export('test_pull')

    # Make sure the "include_references" functionality works properly by just specifying the Topic. It'll pull in
    # the Analyses
    to_pull_df = example_export_push_df[example_export_push_df['Workbook Type'] == 'Topic'].copy()

    pull_workbooks = spy.workbooks.pull(to_pull_df)

    pull_workbooks = sorted(pull_workbooks, key=lambda w: w['Workbook Type'])

    analysis = pull_workbooks[0]  # type: Workbook
    assert analysis.id == (example_export_push_df[
        example_export_push_df['Workbook Type'] == 'Analysis'].iloc[0]['ID'])
    assert analysis.name == (example_export_push_df[
        example_export_push_df['Workbook Type'] == 'Analysis'].iloc[0]['Name'])
    assert len(analysis.datasource_maps) >= 3
    assert len(analysis.item_inventory) >= 25

    assert analysis['URL'] == example_export_push_df[
        example_export_push_df['Workbook Type'] == 'Analysis'].iloc[0]['URL']

    worksheet_names = [w.name for w in analysis.worksheets]
    assert worksheet_names == [
        'Details Pane',
        'Calculated Items',
        'Histogram',
        'Metrics',
        'Journal',
        'Global',
        'Boundaries'
    ]

    topic = pull_workbooks[1]
    worksheet_names = [w.name for w in topic.worksheets]
    assert len(topic.datasource_maps) == 2
    assert worksheet_names == [
        'Static Doc',
        'Live Doc'
    ]

    # Pull specific worksheets
    to_pull_df = example_export_push_df[example_export_push_df['Workbook Type'] == 'Analysis'].copy()
    specific_worksheet_ids = {ws.id for ws in analysis.worksheets if ws.name in ['Metrics', 'Journal']}
    pull_workbooks = spy.workbooks.pull(to_pull_df, specific_worksheet_ids=list(specific_worksheet_ids))

    assert len(pull_workbooks) == 1
    assert len(pull_workbooks[0].worksheets) == 2
    worksheet_ids = {ws.id for ws in pull_workbooks[0].worksheets}
    assert worksheet_ids == specific_worksheet_ids


@pytest.mark.system
def test_minimal_pull():
    example_export_push_df = _push_example_export('test_minimal_pull')
    analysis_df = example_export_push_df[example_export_push_df['Workbook Type'] == 'Analysis']
    worksheet_id = spy.utils.get_worksheet_id_from_url(analysis_df.iloc[0]['URL'])

    timer = _common.timer_start()
    workbooks = spy.workbooks.pull(analysis_df, specific_worksheet_ids=[worksheet_id],
                                   include_annotations=False, include_images=False,
                                   include_inventory=False, include_referenced_workbooks=False,
                                   include_rendered_content=False, quiet=True)
    print(f'Workbook pull took {_common.timer_elapsed(timer)}')

    timer = _common.timer_start()
    spy.workbooks.push(workbooks, include_inventory=False, include_annotations=False,
                       specific_worksheet_ids=[worksheet_id], refresh=False, quiet=True)
    print(f'Workbook push took {_common.timer_elapsed(timer)}')

    new_worksheet_name = 'New worksheet ' + _common.new_placeholder_guid()
    new_worksheet = workbooks[0].worksheet(new_worksheet_name)
    timer = _common.timer_start()
    spy.workbooks.push(workbooks, include_inventory=False, include_annotations=False,
                       specific_worksheet_ids=[new_worksheet.id], refresh=False, quiet=True)
    print(f'Workbook push with extra worksheet took {_common.timer_elapsed(timer)}')

    workbooks = spy.workbooks.pull(analysis_df)
    assert workbooks[0].worksheets[new_worksheet_name] is not None


@pytest.mark.system
def test_pull_no_owner():
    # Items in the Corporate Drive could have no owner. Ensure they can be pulled via SPy when that's the case.
    example_export_push_df = _push_example_export(spy.workbooks.CORPORATE + ' >> test_pull_no_owner')
    analysis_df = example_export_push_df[example_export_push_df['Workbook Type'] == 'Analysis']

    # Baseline assertion - The owner should actually exist
    workbooks = spy.workbooks.pull(analysis_df)
    assert workbooks[0]['Owner'] is not None

    # If the API returns without an owner entirely, SPy should not error.
    mock_workbook_output = WorkbooksApi(spy.session.client).get_workbook(id=analysis_df.iloc[0]['ID'])
    mock_workbook_output.owner = None
    with mock.patch('seeq.sdk.WorkbooksApi.get_workbook', return_value=mock_workbook_output):
        workbooks = spy.workbooks.pull(analysis_df)
        assert workbooks[0]['Owner'] is None


@pytest.mark.system
def test_render():
    _push_example_export('test_render')

    search_df = spy.workbooks.search({
        'Workbook Type': 'Topic',
        'Path': 'test_render',
        'Name': 'Example Topic'
    }, recursive=True)

    spy.options.clear_content_cache_before_render = True

    workbooks = spy.workbooks.pull(search_df, include_rendered_content=True, include_referenced_workbooks=False,
                                   include_inventory=False)

    with tempfile.TemporaryDirectory() as temp:
        spy.workbooks.save(workbooks, temp, include_rendered_content=True)
        topic = [w for w in workbooks if isinstance(w, Topic)][0]

        topic_folder = os.path.join(temp, f'Example Topic ({topic.id})')
        assert util.safe_exists(topic_folder)

        render_folder = os.path.join(topic_folder, 'RenderedTopic')
        assert util.safe_exists(os.path.join(render_folder, 'index.html'))
        for worksheet in topic.worksheets:
            assert util.safe_exists(os.path.join(render_folder, f'{worksheet.report.id}.html'))
            for content_image in worksheet.report.rendered_content_images.keys():
                assert util.safe_exists(_common.get_image_file(render_folder, content_image))

            for static_image in worksheet.report.images.keys():
                assert util.safe_exists(_common.get_image_file(render_folder, static_image))


@pytest.mark.system
def test_pull_url():
    topic = Topic('test_pull_url')
    topic.document('test_pull_url_worksheet')
    push_result = spy.workbooks.push(topic)
    url = push_result['URL'][0]

    # Pull the pushed workbook by URL
    workbooks = spy.workbooks.pull(url)
    assert len(workbooks) == 1
    pulled_topic = workbooks[0]
    assert pulled_topic['Name'] == 'test_pull_url'
    assert pulled_topic['Workbook Type'] == 'Topic'


@pytest.mark.system
def test_redacted_pull():
    workbooks = test_load.load_example_export()

    path = f'Pull Folder Redaction {_common.new_placeholder_guid()}'
    spy.workbooks.push(workbooks, path=path, label=path, errors='catalog')
    search_df = spy.workbooks.search({'Path': path})
    assert len(search_df) >= 1
    reason = 'No thanks'
    mock_exception_thrower = mock.Mock(side_effect=ApiException(status=403, reason=reason))

    def _pull_and_assert_redaction_individual_items(expected_warning):
        with pytest.raises(SPyRuntimeError, match=reason):
            spy.workbooks.pull(search_df, include_rendered_content=True)

        _status = spy.Status(errors='catalog')
        _pull_results = spy.workbooks.pull(search_df, include_rendered_content=True, status=_status)
        assert len(_pull_results) >= 1
        all_errors = {e for pr in _pull_results for e in pr.pull_errors}
        assert len(all_errors) >= 1, f'No warnings found in status {_status}'
        warning_matches = [w for w in all_errors if expected_warning in w]
        assert warning_matches, f'Expected warning "{expected_warning}" not found in {all_errors}'

    with mock.patch('seeq.sdk.ItemsApi.get_access_control', new=mock_exception_thrower):
        _pull_and_assert_redaction_individual_items('Failed to get access control list for Item')

    with mock.patch('seeq.sdk.ContentApi.get_contents_with_all_metadata', new=mock_exception_thrower):
        _pull_and_assert_redaction_individual_items('Failed to get Content items within Report')

    with mock.patch('seeq.sdk.WorkbooksApi.get_workbook', new=mock_exception_thrower):
        _pull_and_assert_redaction_individual_items('Failed to get details for Workbook')

    with mock.patch('seeq.sdk.WorkbooksApi.get_worksheets', new=mock_exception_thrower):
        _pull_and_assert_redaction_individual_items('Failed to gather all Worksheets within Workbook')

    with mock.patch('seeq.sdk.WorkbooksApi.get_worksheet', new=mock_exception_thrower):
        _pull_and_assert_redaction_individual_items('Failed to get Worksheet details for')

    with mock.patch('seeq.sdk.WorkbooksApi.get_workstep', new=mock_exception_thrower):
        _pull_and_assert_redaction_individual_items('Failed to get Workstep details at')

    with mock.patch('seeq.sdk.UsersApi.get_user', new=mock_exception_thrower):
        _pull_and_assert_redaction_individual_items('Failed to get User')

    # Catching errors while scraping workbook inventory is higher-level so it has slightly different expected
    # behavior. Using a mock, we force the code paths that have us calling individual get_signal/condition/scalar
    # functions (by returning None for the item_search_preview property).
    with mock.patch.object(Reference, 'item_search_preview', new_callable=mock.PropertyMock) as mock_getter:
        mock_getter.return_value = None

        with mock.patch('seeq.sdk.SignalsApi.get_signal', new=mock_exception_thrower):
            _pull_and_assert_redaction_individual_items(reason)

        with mock.patch('seeq.sdk.ConditionsApi.get_condition', new=mock_exception_thrower):
            _pull_and_assert_redaction_individual_items(reason)

        with mock.patch('seeq.sdk.ScalarsApi.get_scalar', new=mock_exception_thrower):
            _pull_and_assert_redaction_individual_items(reason)


@pytest.mark.system
def test_pull_topic_document_with_timezone():
    topic_name = 'test_pull_topic_document_with_timezone'
    topic = Topic({'Name': topic_name})
    topic.document('No Timezone')
    topic_document_with_timezone = topic.document('With Timezone')
    topic_document_with_timezone.report['Timezone'] = 'America/Los_Angeles'

    with tempfile.TemporaryDirectory() as temp_dir:
        spy.workbooks.save(topic, temp_dir)
        workbooks = spy.workbooks.load(temp_dir)

    report = workbooks[topic_name].worksheets['No Timezone'].report
    assert 'Timezone' not in workbooks[topic_name].worksheets['No Timezone'].report
    assert ('America/Los_Angeles' ==
            workbooks[topic_name].worksheets['With Timezone'].report['Timezone'])

    push_df = spy.workbooks.push(topic, refresh=False)
    pull_df = push_df.drop(columns=['ID']).rename(columns={'Pushed Workbook ID': 'ID'})
    workbooks = spy.workbooks.pull(pull_df)
    assert not workbooks[topic_name].worksheets['No Timezone'].report.get('Timezone', '')
    assert ('America/Los_Angeles' ==
            workbooks[topic_name].worksheets['With Timezone'].report['Timezone'])

    # Remove it (to reset to default) and push/pull
    workbooks[topic_name].worksheets['With Timezone'].report['Timezone'] = None
    push_df = spy.workbooks.push(workbooks, refresh=False)
    pull_df = push_df.drop(columns=['ID']).rename(columns={'Pushed Workbook ID': 'ID'})
    workbooks = spy.workbooks.pull(pull_df)
    assert not workbooks[topic_name].worksheets['No Timezone'].report.get('Timezone', '')
    assert not workbooks[topic_name].worksheets['With Timezone'].report.get('Timezone', '')


@pytest.mark.system
def test_date_range_type_compatibility():
    expected_start = pd.Timestamp(year=2020, month=1, day=1, hour=0, tz='UTC')
    expected_end = pd.Timestamp(year=2020, month=1, day=1, hour=1, tz='UTC')

    # String date-times should get parsed
    workstep = spy.workbooks.AnalysisWorkstep()
    workstep.definition['Data']['state']['stores']['sqDurationStore']['displayRange'] = {
        'start': '2020-01-01T00:00:00.000Z',
        'end': '2020-01-01T01:00:00.000Z'
    }
    assert workstep.display_range['Start'] == expected_start
    assert workstep.display_range['End'] == expected_end

    workstep.definition['Data']['state']['stores']['sqDurationStore']['investigateRange'] = {
        'start': '2020-01-01T00:00:00.000Z',
        'end': '2020-01-01T01:00:00.000Z'
    }
    assert workstep.investigate_range['Start'] == expected_start
    assert workstep.investigate_range['End'] == expected_end

    # Nanoseconds should get converted
    workstep = spy.workbooks.AnalysisWorkstep()
    workstep.definition['Data']['state']['stores']['sqDurationStore']['displayRange'] = {
        'start': 1_577_836_800_000,
        'end': 1_577_840_400_000
    }
    assert workstep.display_range['Start'] == expected_start
    assert workstep.display_range['End'] == expected_end

    workstep.definition['Data']['state']['stores']['sqDurationStore']['investigateRange'] = {
        'start': 1_577_836_800_000,
        'end': 1_577_840_400_000
    }
    assert workstep.investigate_range['Start'] == expected_start
    assert workstep.investigate_range['End'] == expected_end

    # Anything else should cause it to return None
    # Non-date string
    workstep = spy.workbooks.AnalysisWorkstep()
    workstep.definition['Data']['state']['stores']['sqDurationStore']['displayRange'] = {
        'start': 'Something else',
        'end': 'that should error'
    }
    assert workstep.display_range is None

    workstep = spy.workbooks.AnalysisWorkstep()
    workstep.definition['Data']['state']['stores']['sqDurationStore']['investigateRange'] = {
        'start': 'Something else',
        'end': 'that should error'
    }
    assert workstep.investigate_range is None

    # Random object
    workstep = spy.workbooks.AnalysisWorkstep()
    workstep.definition['Data']['state']['stores']['sqDurationStore']['displayRange'] = {
        'start': spy.workbooks.AnalysisWorkstep(),
        'end': spy.workbooks.AnalysisWorkstep()
    }
    assert workstep.display_range is None

    workstep = spy.workbooks.AnalysisWorkstep()
    workstep.definition['Data']['state']['stores']['sqDurationStore']['investigateRange'] = {
        'start': spy.workbooks.AnalysisWorkstep(),
        'end': spy.workbooks.AnalysisWorkstep()
    }
    assert workstep.investigate_range is None

    # NoneType
    workstep = spy.workbooks.AnalysisWorkstep()
    workstep.definition['Data']['state']['stores']['sqDurationStore']['displayRange'] = {
        'start': None,
        'end': None
    }
    assert workstep.display_range is None

    workstep = spy.workbooks.AnalysisWorkstep()
    workstep.definition['Data']['state']['stores']['sqDurationStore']['investigateRange'] = {
        'start': None,
        'end': None
    }
    assert workstep.investigate_range is None


@pytest.mark.system
def test_pull_from_url_with_session():
    # Setup: Push a workbook as Ren. Get the URL for that workbook.
    ren_session = test_common.get_session(Sessions.ren)
    stimpy_session = test_common.get_session(Sessions.stimpy)
    push_result = _push_example_export('test_pull_from_url_with_session', session=ren_session)
    url = push_result[push_result['Workbook Type'] == 'Analysis'].iloc[0]['URL']

    # spy.utils.get_analysis_worksheet_from_url() should work for Ren when the session is passed in.
    pulled_worksheet_ren = spy.utils.pull_worksheet_via_url(url=url, session=ren_session)
    assert pulled_worksheet_ren.name == 'Details Pane'
    # But not for Stimpy because they don't have permission.
    with pytest.raises(SPyRuntimeError, match="Could not find workbook with ID"):
        spy.utils.pull_worksheet_via_url(url=url, session=stimpy_session)

    # Make sure the older deprecated function still works
    pulled_worksheet_ren = spy.utils.get_analysis_worksheet_from_url(url=url, session=ren_session)
    assert pulled_worksheet_ren.name == 'Details Pane'

    # spy.search(url) should also work (via the same util function) in the same way.
    searched_items = spy.search(url, session=ren_session)
    assert len(searched_items) > 1
    with pytest.raises(SPyRuntimeError, match="Could not find workbook with ID"):
        spy.search(url, session=stimpy_session)

    # Same with spy.pull(url).
    pulled_data = spy.pull(url, start='2024-01-01T00:00:00Z', end='2024-01-02T00:00:00Z', session=ren_session)
    assert len(pulled_data) > 50
    with pytest.raises(SPyRuntimeError, match="Could not find workbook with ID"):
        spy.pull(url, start='2024-01-01T00:00:00Z', end='2024-01-02T00:00:00Z', session=stimpy_session)


def test_pull_with_complex_schedule_and_pdf_settings():
    dir_name = 'Topic for Pulling_Pushing Schedules (0EEC0A15-CE54-7120-B168-6A3866C558DF)'
    scenarios_folder = test_common.unzip_to_temp(os.path.join(os.path.dirname(__file__), 'Scenarios.zip'))
    try:
        workbook_folder = os.path.join(scenarios_folder, dir_name)
        workbooks = spy.workbooks.load(workbook_folder)
    finally:
        util.safe_rmtree(scenarios_folder)

    with pytest.raises(SPyRuntimeError, match="User .* not successfully mapped"):
        spy.workbooks.push(workbooks, access_control='strict')

    pushed_df = spy.workbooks.push(workbooks, access_control='loose', refresh=False)

    pushed_workbook_id = pushed_df.iloc[0]['Pushed Workbook ID']
    pulled_workbooks = spy.workbooks.pull(pushed_workbook_id)

    pulled_workbook = pulled_workbooks[0]
    pulled_report = pulled_workbook.documents[0].report
    for prop, value in {
        "Margin Bottom": "4cm",
        "Margin Left": "1in",
        "Margin Right": "2px",
        "Margin Top": "3mm",
        "Page Orientation": "Landscape",
        "Page Size": "Tabloid",
        "Timezone": "Pacific/Midway",
    }.items():
        assert pulled_report[prop] == value

    assert not pulled_report.schedule['Enabled']
    assert pulled_report.schedule['Background']
    assert pulled_report.schedule['Notification']['To Email Recipients'] == []
    assert pulled_report.schedule['Notification']['CC Email Recipients'] == [{'Email': 'bill.gates@microsoft.com'}]
    assert pulled_report.schedule['Notification']['BCC Email Recipients'] == []
    assert pulled_report.schedule['Notification']['Report Format'] == 'PDF'

    with tempfile.TemporaryDirectory() as temp:
        datasource_map = json.loads(r"""
        {
            "Datasource Class": "OAuth 2.0",
            "Datasource ID": "9ecc7e74-4c32-436d-9af0-70d8aa99386b",
            "Datasource Name": "Seeq Azure SSO",
            "Item-Level Map Files": [],
            "RegEx-Based Maps": [
                {
                    "Old": {
                        "Type": "User",
                        "Datasource Class": "OAuth 2\\.0",
                        "Datasource Name": "Seeq Azure SSO",
                        "Username": "mark.derbecker@seeq.com"
                    },
                    "New": {
                        "Type": "User",
                        "Datasource Class": "Auth",
                        "Datasource Name": "Seeq",
                        "Username": "admin@seeq.com"
                    }
                },
                {
                    "Old": {
                        "Type": "User",
                        "Datasource Class": "OAuth 2\\.0",
                        "Datasource Name": "Seeq Azure SSO",
                        "Username": "mike.daly@seeq.com"
                    },
                    "New": {
                        "Type": "User",
                        "Datasource Class": "Auth",
                        "Datasource Name": "Seeq",
                        "Username": "non_admin.tester@seeq.com"
                    }
                },
                {
                    "Old": {
                        "Type": "User",
                        "Datasource Class": "OAuth 2\\.0",
                        "Datasource Name": "Seeq Azure SSO",
                        "Username": "corinne.ilvedson@seeq.com"
                    },
                    "New": {
                        "Type": "User",
                        "Datasource Class": "Auth",
                        "Datasource Name": "Seeq",
                        "Username": "ren"
                    }
                },
                {
                    "Old": {
                        "Type": "User",
                        "Datasource Class": "OAuth 2\\.0",
                        "Datasource Name": "Seeq Azure SSO",
                        "Username": "Dustin.Johnson@seeq.com"
                    },
                    "New": {
                        "Type": "User",
                        "Datasource Class": "Auth",
                        "Datasource Name": "Seeq",
                        "Username": "stimpy"
                    }
                }
            ]
        }
        """)

        with util.safe_open(os.path.join(temp, 'Datasource_Map_OAuth.json'), 'w') as f:
            json.dump(datasource_map, f)

        workbooks[0].documents[0].report.schedule['Enabled'] = True
        spy.workbooks.push(workbooks, access_control='strict', datasource_map_folder=temp)

        # refresh=True so we can just access the original objects
        pulled_report = workbooks[0].documents[0].report

        assert pulled_report.schedule['Enabled']

        def _get_recipient(r):
            return r['Identity']['Username'] if 'Identity' in r else r['Email']

        assert {_get_recipient(r) for r in pulled_report.schedule['Notification']['To Email Recipients']} == {
            'admin@seeq.com',
            'non_admin.tester@seeq.com'
        }
        assert {_get_recipient(r) for r in pulled_report.schedule['Notification']['CC Email Recipients']} == {
            'bill.gates@microsoft.com',
            'ren'
        }
        assert {_get_recipient(r) for r in pulled_report.schedule['Notification']['BCC Email Recipients']} == {
            'stimpy'
        }


@pytest.mark.system
def test_pull_archived_content():
    test_name = 'test_pull_archived_content'
    scenarios_folder = test_common.unzip_to_temp(os.path.join(os.path.dirname(__file__), 'Scenarios.zip'))
    try:
        workbook_folder = os.path.join(scenarios_folder, test_name)
        workbooks = spy.workbooks.load(workbook_folder)
        spy.workbooks.push(workbooks, path=test_name)
    finally:
        util.safe_rmtree(scenarios_folder)

    search_df = spy.workbooks.search({'Path': test_name, 'Name': 'My Topic'})

    workbooks = spy.workbooks.pull(search_df)
    assert len(workbooks) == 3
    inventory = {v['Name']: v for v in workbooks['My Workbook (Archived Content)'].item_inventory.values()}
    assert 'Smoothed Temperature' in inventory
    document: TopicDocument = workbooks['My Topic'].documents[0]
    assert len(document.content) == 3

    workbooks = spy.workbooks.pull(search_df, include_archived=False)
    # The workbook with archived content will be excluded, it is not referenced by any non-archived content in the topic
    assert len(workbooks) == 2
    assert 'My Workbook (Archived Content)' not in workbooks
    document: TopicDocument = workbooks['My Topic'].documents[0]
    assert len(document.content) == 1

    search_df = spy.workbooks.search({'Path': test_name, 'Name': 'My Workbook (Archived Content)'})
    workbooks = spy.workbooks.pull(search_df, include_archived=False)
    assert len(workbooks) == 1
    inventory = {v['Name']: v for v in workbooks['My Workbook (Archived Content)'].item_inventory.values()}
    assert 'Smoothed Temperature' not in inventory
