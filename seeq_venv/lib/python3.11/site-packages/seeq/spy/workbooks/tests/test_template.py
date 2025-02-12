import os
import re
import tempfile

import matplotlib.pyplot as plt
import pandas as pd
import pytest
import requests

from seeq import spy
from seeq.base import util
from seeq.sdk import *
from seeq.spy import _common
from seeq.spy._errors import *
from seeq.spy.assets import Asset
from seeq.spy.tests import test_common
from seeq.spy.workbooks import *
from seeq.spy.workbooks.tests import test_load


def setup_module():
    test_common.initialize_sessions()


def _load_test_templates(as_template):
    return spy.workbooks.load(os.path.join(os.path.dirname(__file__), 'Workbook Template Tests.zip'),
                              as_template_with_label=as_template)


def _load_doc_templates(as_template):
    return spy.workbooks.load(os.path.normpath(
        os.path.join(os.path.dirname(__file__),
                     '..', '..', 'docs', 'Documentation', 'Support Files', 'Workbook Templates.zip')),
        as_template_with_label=as_template)


def _construct_sinusoid_metadata():
    metadata_df = pd.DataFrame([{
        'Name': 'Sinusoid 1',
        'Type': 'Signal',
        'Formula': 'sinusoid(1h)'
    }, {
        'Name': 'Sinusoid 2',
        'Type': 'Signal',
        'Formula': 'sinusoid(2h)'
    }, {
        'Name': 'Sinusoid 3',
        'Type': 'Signal',
        'Formula': 'sinusoid(3h)'
    }])
    return metadata_df


@pytest.mark.system
def test_push_worksheet():
    test_name = 'template.test_push_worksheet'
    # Added some regex characters to test that the fix for SUP-52442 is in place
    workbooks = _load_doc_templates(test_name + r' (here are some regex characters: .+*?^$()[]{}|\)')
    workbook = workbooks['Analysis Template']
    worksheet = workbook.worksheets['Trend Template']

    print(worksheet.code)

    metadata_df = _construct_sinusoid_metadata()

    worksheet.parameters = {
        "9564A6B8-8A8F-4F6D-AC63-00EA38962B7A [Signal] Area A_Temperature": 'Sinusoid 1',
        "DCED9C36-A4BE-4783-9216-DC06B3F57D8C [Signal] Area A_Compressor Power": 'Sinusoid 2',
        "D2C089B6-CE85-46FC-8392-E11CC0C08336 [Signal] Area A_Compressor Stage": 'DOES NOT EXIST',
        "Temperature Journal Link Text": 'SINUSOID 1',
        "Compressor Power Journal Link Text": 'SINUSOID 2',
        "Compressor Stage Journal Link Text": 'SINUSOID 3',
        "favorite color": 'blue'
    }

    # Give the worksheet a different name than "Trend Template"
    worksheet.name = 'My Trend'

    with pytest.raises(SPyRuntimeError, match=r'Could not find ID for pushed item where\nName = "DOES NOT EXIST"'):
        spy.push(metadata=metadata_df, workbook=test_name, worksheet=worksheet, datasource=test_name)

    status = spy.Status(errors='catalog')
    bad_push_df = spy.push(metadata=metadata_df, workbook=test_name, worksheet=worksheet, datasource=test_name,
                           status=status)
    push_workbooks_status = status.inner['Push Workbooks']
    push_workbooks_result = push_workbooks_status.df.iloc[0]['Result']
    assert 'Could not find ID for pushed item where\nName = "DOES NOT EXIST"' in push_workbooks_result
    warnings_str = '\n'.join(status.warnings)
    assert 'Could not find ID for pushed item where\nName = "DOES NOT EXIST"' in warnings_str
    assert 'As a result, no URL link is available.' in warnings_str
    assert bad_push_df.spy.workbook_url is None

    worksheet.parameters["D2C089B6-CE85-46FC-8392-E11CC0C08336 [Signal] Area A_Compressor Stage"] = 'Sinusoid 3'
    push_df = spy.push(metadata=metadata_df, workbook=test_name, worksheet=worksheet, datasource=test_name)

    pushed_ids = sorted(push_df['ID'].to_list())

    pulled_workbooks = spy.workbooks.pull(push_df.spy.workbook_url)
    pulled_worksheet = pulled_workbooks[test_name].worksheets['My Trend']
    display_item_ids = sorted(pulled_worksheet.display_items['ID'].to_list())

    assert pushed_ids == display_item_ids

    journal_html = pulled_worksheet.html

    for display_item_id in display_item_ids:
        assert display_item_id in journal_html

    assert 'blue' in journal_html


@pytest.mark.system
def test_copy_worksheet_repeatedly():
    # Tests the fix for CRAB-39846
    test_name = 'template.test_copy_worksheet_repeatedly'
    example_workbooks = test_load.load_example_export()
    example_workbook = example_workbooks['Example Analysis']
    template_workbooks = _load_doc_templates(test_name)
    template_workbook = template_workbooks['Analysis Template']
    # noinspection PyTypeChecker
    template_worksheet: AnalysisWorksheetTemplate = template_workbook.worksheets['Trend Template']

    for i in range(0, 100):
        example_workbook.worksheets.append(template_worksheet.copy(f'Copy {i}'))

    assert len(example_workbook.datasource_maps) == 3


@pytest.mark.system
def test_push_worksheet_with_preexisting_items():
    test_name = 'template.test_push_worksheet_with_preexisting_items'
    workbooks = _load_doc_templates(test_name)
    workbook = workbooks['Analysis Template']
    worksheet = workbook.worksheets['Trend Template']

    metadata_df = _construct_sinusoid_metadata()

    pushed_df = spy.push(metadata=metadata_df, workbook=test_name, datasource=test_name)

    worksheet.parameters = {
        "9564A6B8-8A8F-4F6D-AC63-00EA38962B7A [Signal] Area A_Temperature":
            pushed_df[pushed_df['Name'] == 'Sinusoid 1'],
        "DCED9C36-A4BE-4783-9216-DC06B3F57D8C [Signal] Area A_Compressor Power":
            pushed_df[pushed_df['Name'] == 'Sinusoid 2'],
        "D2C089B6-CE85-46FC-8392-E11CC0C08336 [Signal] Area A_Compressor Stage":
            pushed_df[pushed_df['Name'] == 'Sinusoid 3'].iloc[0]['ID'],  # Make sure we can specify an ID directly
        "Temperature Journal Link Text": None,
        "Compressor Power Journal Link Text": None
    }

    # Give the worksheet a different name than "Trend Template"
    worksheet.name = 'My Trend'

    pushed_workbooks_df = spy.workbooks.push(workbook, datasource=test_name)

    pulled_workbook = pushed_workbooks_df.spy.output[0]
    pulled_worksheet = pulled_workbook.worksheets['My Trend']
    actual_display_item_ids = sorted(pulled_worksheet.display_items['ID'].to_list())
    expected_display_item_ids = sorted(pushed_df['ID'].to_list())

    assert actual_display_item_ids == expected_display_item_ids


def _test_push_workbook_metadata():
    return pd.DataFrame([
        {'Path': 'Waveforms', 'Asset': 'Waveforms 1', 'Name': 'Sinusoid', 'Type': 'Signal', 'Formula': 'sinusoid(1h)'},
        {'Path': 'Waveforms', 'Asset': 'Waveforms 1', 'Name': 'Sawtooth', 'Type': 'Signal', 'Formula': 'sawtooth(1h)'},
        {'Path': 'Waveforms', 'Asset': 'Waveforms 1', 'Name': 'Square', 'Type': 'Signal', 'Formula': 'squareWave(1h)'},
        {'Path': 'Waveforms', 'Asset': 'Waveforms 1', 'Name': 'Hours', 'Type': 'Condition', 'Formula': 'hours()'},
        {'Path': 'Waveforms', 'Asset': 'Waveforms 2', 'Name': 'Sinusoid', 'Type': 'Signal', 'Formula': 'sinusoid(12h)'},
        {'Path': 'Waveforms', 'Asset': 'Waveforms 2', 'Name': 'Sawtooth', 'Type': 'Signal', 'Formula': 'sawtooth(12h)'},
        {'Path': 'Waveforms', 'Asset': 'Waveforms 2', 'Name': 'Square', 'Type': 'Signal', 'Formula': 'squareWave(12h)'},
        {'Path': 'Waveforms', 'Asset': 'Waveforms 2', 'Name': 'Hours', 'Type': 'Condition', 'Formula': 'hours()'},
    ])


@pytest.mark.ignore  # CRAB-46355
def test_push_workbook():
    test_name = 'template.test_push_workbook'
    workbooks = _load_doc_templates(test_name)
    workbook = workbooks['Analysis Template']

    metadata_df = _test_push_workbook_metadata()

    workbook.name = test_name
    workbook.parameters = {
        "DCED9C36-A4BE-4783-9216-DC06B3F57D8C [Signal] Area A_Compressor Power": {'Asset': 'Waveforms 1',
                                                                                  'Name': 'Sinusoid'},
        "9564A6B8-8A8F-4F6D-AC63-00EA38962B7A [Signal] Area A_Temperature": {'Asset': 'Waveforms 1',
                                                                             'Name': 'Sawtooth'},
        "D2C089B6-CE85-46FC-8392-E11CC0C08336 [Signal] Area A_Compressor Stage": {'Asset': 'Waveforms 1',
                                                                                  'Name': 'Square'},
        "Temperature Journal Link Text": 'Sawtooth',
        "Compressor Power Journal Link Text": 'Sinusoid',
        "Compressor Stage Journal Link Text": 'Square',
        "favorite color": 'beige',
        "4B40EAFC-91ED-4AB0-8199-F21AF40A8350 [Asset] Example >> Area A": {'Asset': 'Waveforms 1', 'Type': 'Asset'},
        "07F3161F-6644-4505-BC33-16D6155B004E [Condition] Hot": {'Asset': 'Waveforms 1', 'Name': 'Hours'},
        "8A4F0E26-8A0C-4127-9E11-B67E031C6049 [Signal] Example >> Cooling Tower 1 >> Area A >> Temperature":
            {'Asset': 'Waveforms 1', 'Name': 'Sawtooth'},
        "D16FC368-AE8E-47B4-B1A3-9C2A3FBA2BB6 [Asset] Cooling Tower 1": {'Asset': 'Waveforms 1', 'Type': 'Asset'}
    }

    pushed_df = spy.push(metadata=metadata_df, workbook=workbook, datasource=test_name)

    pushed_workbooks = spy.workbooks.pull(spy.workbooks.search({'Name': test_name}),
                                          include_inventory=False, include_images=False,
                                          include_referenced_workbooks=False)
    pushed_workbook = pushed_workbooks[test_name]
    pushed_worksheet = pushed_workbook.worksheets[0]
    assert re.match(rf'.*workbook/{pushed_workbook.id}/worksheet/{pushed_worksheet.id}$', pushed_df.spy.workbook_url)
    display_items = set(pushed_worksheet.display_items['ID'].to_list())
    waveforms_1_ids = set(pushed_df[pushed_df['Asset'] == 'Waveforms 1']['ID'].to_list())
    assert display_items.issubset(waveforms_1_ids)

    #
    # Push again using a different label, but to the same workbook. Covers SUP-42381
    #

    workbook_name = test_name + '-v2'
    workbooks = _load_doc_templates(workbook_name)
    workbook = workbooks['Analysis Template']

    workbook['ID'] = pushed_workbook.id
    workbook.name = workbook_name

    def _lookup(_d):
        return pushed_df[(pushed_df['Asset'] == _d['Asset']) & (pushed_df['Name'] == _d['Name'])].iloc[0]['ID']

    workbook.parameters = {
        "DCED9C36-A4BE-4783-9216-DC06B3F57D8C [Signal] Area A_Compressor Power": _lookup({'Asset': 'Waveforms 2',
                                                                                          'Name': 'Sinusoid'}),
        "9564A6B8-8A8F-4F6D-AC63-00EA38962B7A [Signal] Area A_Temperature": _lookup({'Asset': 'Waveforms 2',
                                                                                     'Name': 'Sawtooth'}),
        "D2C089B6-CE85-46FC-8392-E11CC0C08336 [Signal] Area A_Compressor Stage": _lookup({'Asset': 'Waveforms 2',
                                                                                          'Name': 'Square'}),
        "Temperature Journal Link Text": 'Sawtooth',
        "Compressor Power Journal Link Text": 'Sinusoid',
        "Compressor Stage Journal Link Text": 'Square',
        "favorite color": 'beige',
        "4B40EAFC-91ED-4AB0-8199-F21AF40A8350 [Asset] Example >> Area A":
            pushed_df[(pushed_df['Asset'] == 'Waveforms 2') & (pushed_df['Type'] == 'Asset')].iloc[0]['ID'],
        "07F3161F-6644-4505-BC33-16D6155B004E [Condition] Hot": _lookup({'Asset': 'Waveforms 2', 'Name': 'Hours'}),
        "8A4F0E26-8A0C-4127-9E11-B67E031C6049 [Signal] Example >> Cooling Tower 1 >> Area A >> Temperature":
            _lookup({'Asset': 'Waveforms 2', 'Name': 'Sawtooth'}),
        "D16FC368-AE8E-47B4-B1A3-9C2A3FBA2BB6 [Asset] Cooling Tower 1":
            pushed_df[(pushed_df['Asset'] == 'Waveforms 2') & (pushed_df['Type'] == 'Asset')].iloc[0]['ID']
    }

    spy.workbooks.push(workbook, datasource=test_name)

    pushed_workbooks = spy.workbooks.pull(spy.workbooks.search({'Name': workbook_name}),
                                          include_inventory=False, include_images=False,
                                          include_referenced_workbooks=False)
    pushed_workbook = pushed_workbooks[workbook_name]
    pushed_worksheet = pushed_workbook.worksheets[0]
    display_items = set(pushed_worksheet.display_items['ID'].to_list())
    waveforms_2_ids = set(pushed_df[pushed_df['Asset'] == 'Waveforms 2']['ID'].to_list())
    assert display_items.issubset(waveforms_2_ids)


@pytest.mark.system
def test_add_multiple_template_worksheets_to_non_template_workbook():
    test_name = 'template.test_add_multiple_template_worksheets_to_non_template_workbook'
    template_workbooks = _load_test_templates(test_name)
    template_workbook = template_workbooks['My Analysis Template']
    template_worksheet: AnalysisWorksheetTemplate = template_workbook.worksheets['A Trend']

    pushed_df = spy.push(metadata=pd.DataFrame([{
        'Name': 'Sawtooth',
        'Path': test_name,
        'Asset': 'Asset 1',
        'Type': 'Signal',
        'Formula': 'sinusoid(1h)'
    }, {
        'Name': 'Triangle',
        'Path': test_name,
        'Asset': 'Asset 1',
        'Type': 'Signal',
        'Formula': 'triangleWave(2h)'
    }, {
        'Name': 'Sawtooth',
        'Path': test_name,
        'Asset': 'Asset 2',
        'Type': 'Signal',
        'Formula': 'sinusoid(2h)'
    }, {
        'Name': 'Triangle',
        'Path': test_name,
        'Asset': 'Asset 2',
        'Type': 'Signal',
        'Formula': 'triangleWave(4h)'
    }]), workbook=test_name, datasource=test_name)

    template_worksheet_1 = template_worksheet.copy(label=f'{test_name} Worksheet 1')
    template_worksheet_1.name = f'{test_name} Worksheet 1'
    template_worksheet_1.parameters = {
        "B9CDE282-7A1A-4E28-A173-12E7347AB891 [Signal] Example >> Cooling Tower 1 >> Area A >> Relative Humidity":
            pushed_df[(pushed_df['Asset'] == 'Asset 1') & (pushed_df['Name'] == 'Sawtooth')],
        "4B40EAFC-91ED-4AB0-8199-F21AF40A8350 [Asset] Example >> Cooling Tower 1 >> Area A":
            pushed_df[(pushed_df['Asset'] == 'Asset 1') & (pushed_df['Type'] == 'Asset')],
        "8A4F0E26-8A0C-4127-9E11-B67E031C6049 [Signal] Example >> Cooling Tower 1 >> Area A >> Temperature":
            pushed_df[(pushed_df['Asset'] == 'Asset 1') & (pushed_df['Name'] == 'Triangle')],
    }

    template_worksheet_2 = template_worksheet.copy(label=f'{test_name} Worksheet 2')
    template_worksheet_2.name = f'{test_name} Worksheet 2'
    template_worksheet_2.parameters = {
        "B9CDE282-7A1A-4E28-A173-12E7347AB891 [Signal] Example >> Cooling Tower 1 >> Area A >> Relative Humidity":
            pushed_df[(pushed_df['Asset'] == 'Asset 2') & (pushed_df['Name'] == 'Sawtooth')],
        "4B40EAFC-91ED-4AB0-8199-F21AF40A8350 [Asset] Example >> Cooling Tower 1 >> Area A":
            pushed_df[(pushed_df['Asset'] == 'Asset 2') & (pushed_df['Type'] == 'Asset')],
        "8A4F0E26-8A0C-4127-9E11-B67E031C6049 [Signal] Example >> Cooling Tower 1 >> Area A >> Temperature":
            pushed_df[(pushed_df['Asset'] == 'Asset 2') & (pushed_df['Name'] == 'Triangle')],
    }

    workbook_name = f'{test_name} With Templates Added'
    workbook = Analysis({'Name': workbook_name})

    workbook.worksheets.append(template_worksheet_1)
    workbook.worksheets.append(template_worksheet_2)

    spy.workbooks.push(workbook, datasource=test_name)

    pushed_workbooks = spy.workbooks.pull(spy.workbooks.search({'Name': workbook_name}),
                                          include_inventory=False, include_images=False,
                                          include_referenced_workbooks=False)

    pushed_workbook = pushed_workbooks[workbook_name]
    assert len(pushed_workbook.worksheets) == 2
    pushed_worksheet_1 = pushed_workbook.worksheets[f'{test_name} Worksheet 1']
    pushed_worksheet_2 = pushed_workbook.worksheets[f'{test_name} Worksheet 2']

    assert (sorted(pushed_worksheet_1.display_items['ID'].to_list()) ==
            sorted(pushed_df[(pushed_df['Asset'] == 'Asset 1') & (pushed_df['Type'] != 'Asset')]['ID'].to_list()))
    assert (sorted(pushed_worksheet_2.display_items['ID'].to_list()) ==
            sorted(pushed_df[(pushed_df['Asset'] == 'Asset 2') & (pushed_df['Type'] != 'Asset')]['ID'].to_list()))


@pytest.mark.system
def test_push_workbook_list():
    test_name = 'template.test_push_workbook_list'
    workbooks = _load_doc_templates(test_name)

    # noinspection PyTypeChecker
    topic: TopicTemplate = workbooks['Topic Template']
    analysis = workbooks['Analysis Template']

    document = topic.documents['Doc Template']

    print(document.code)

    metadata_df = _test_push_workbook_metadata()

    trend_template = analysis.worksheets['Trend Template']
    xy_plot_template = analysis.worksheets['XY Plot Template']
    treemap_template = analysis.worksheets['Treemap Template']
    table_template = analysis.worksheets['Table Template']

    print(trend_template.code)
    print(xy_plot_template.code)
    print(treemap_template.code)
    print(table_template.code)

    # Create a dictionary to store our worksheet templates
    worksheet_templates = dict()

    for asset in ['Waveforms 1', 'Waveforms 2']:
        # We create a copy of the worksheets for each asset, and it must have a unique label. So we incorporate
        # the asset name into the label.
        unique_label = f'{test_name} - {asset}'

        trend_worksheet = trend_template.copy(unique_label)
        trend_worksheet.name = f'{asset} Trend'
        trend_worksheet.parameters = {
            "DCED9C36-A4BE-4783-9216-DC06B3F57D8C [Signal] Area A_Compressor Power": {'Asset': asset,
                                                                                      'Name': 'Sinusoid'},
            "9564A6B8-8A8F-4F6D-AC63-00EA38962B7A [Signal] Area A_Temperature": {'Asset': asset, 'Name': 'Sawtooth'},
            "D2C089B6-CE85-46FC-8392-E11CC0C08336 [Signal] Area A_Compressor Stage": {'Asset': asset, 'Name': 'Square'},
            "Temperature Journal Link Text": 'Sawtooth',
            "Compressor Power Journal Link Text": 'Sinusoid',
            "Compressor Stage Journal Link Text": 'Square',
            "favorite color": 'eggshell' if asset.endswith('1') else 'violet'
        }

        xy_plot_worksheet = xy_plot_template.copy(unique_label)
        xy_plot_worksheet.name = f'{asset} XY Plot'
        xy_plot_worksheet.parameters = {
            "DCED9C36-A4BE-4783-9216-DC06B3F57D8C [Signal] Area A_Compressor Power": {'Asset': asset,
                                                                                      'Name': 'Sinusoid'},
            "9564A6B8-8A8F-4F6D-AC63-00EA38962B7A [Signal] Area A_Temperature": {'Asset': asset, 'Name': 'Sawtooth'}
        }

        treemap_worksheet = treemap_template.copy(unique_label)
        treemap_worksheet.name = f'{asset} Treemap'
        treemap_worksheet.parameters = {
            "8A4F0E26-8A0C-4127-9E11-B67E031C6049 [Signal] Example >> Cooling Tower 1 >> Area A >> Temperature": {
                'Asset': asset, 'Name': 'Sawtooth'},
            "07F3161F-6644-4505-BC33-16D6155B004E [Condition] Hot": {'Asset': asset, 'Name': 'Hours'},
            "D16FC368-AE8E-47B4-B1A3-9C2A3FBA2BB6 [Asset] Cooling Tower 1": {'Asset': 'Waveforms', 'Type': 'Asset'},
            "4B40EAFC-91ED-4AB0-8199-F21AF40A8350 [Asset] Example >> Area A": {'Asset': asset, 'Type': 'Asset'}
        }

        table_worksheet = table_template.copy(unique_label)
        table_worksheet.name = f'{asset} Table'
        table_worksheet.parameters = {
            "DCED9C36-A4BE-4783-9216-DC06B3F57D8C [Signal] Area A_Compressor Power": {'Asset': asset,
                                                                                      'Name': 'Sinusoid'},
            "D2C089B6-CE85-46FC-8392-E11CC0C08336 [Signal] Area A_Compressor Stage": {'Asset': asset, 'Name': 'Square'},
            "9564A6B8-8A8F-4F6D-AC63-00EA38962B7A [Signal] Area A_Temperature": {'Asset': asset, 'Name': 'Sawtooth'}
        }

        worksheet_templates[asset] = {
            'Trend': trend_worksheet,
            'XY Plot': xy_plot_worksheet,
            'Treemap': treemap_worksheet,
            'Table': table_worksheet,
        }

    document.name = test_name

    document.parameters = {
        "Document Name": "Waveform Monitoring and Diagnostics",
        "Assets": [
            {
                "Asset Name": asset,
                "25AF462C-602A-4E13-B50D-5DEB92CB37B5 [Embedded Content] Users >> mark.derbecker@seeq.com >> Workbook Templates >> Analysis Template >> Trend Template":
                    worksheet_templates[asset]['Trend'],
                "76B4412D-3B08-48B7-ABF9-ED8432DAA5CE [Embedded Content] Users >> mark.derbecker@seeq.com >> Workbook Templates >> Analysis Template >> XY Plot Template":
                    worksheet_templates[asset]['XY Plot'],
                "44392AAB-D443-4D2D-ACF0-055869875558 [Embedded Content] Users >> mark.derbecker@seeq.com >> Workbook Templates >> Analysis Template >> Treemap Template":
                    worksheet_templates[asset]['Treemap'],
                "D4F167B3-E8BA-4FAA-86B3-05877E2D4250 [Embedded Content] Users >> mark.derbecker@seeq.com >> Workbook Templates >> Analysis Template >> Table Template":
                    worksheet_templates[asset]['Table']
            }
            for asset in ['Waveforms 1', 'Waveforms 2']
        ]
    }

    topic.name = f'{test_name} - Topic'
    analysis.name = f'{test_name} - Analysis'

    analysis.worksheets['Trend Template']['Archived'] = True
    analysis.worksheets['XY Plot Template']['Archived'] = True
    analysis.worksheets['Treemap Template']['Archived'] = True
    analysis.worksheets['Table Template']['Archived'] = True

    workbooks_to_push = [topic, analysis]

    # Note that we pass in the list of workbooks (which includes the Analysis and the Topic)
    pushed_df = spy.push(metadata=metadata_df, workbook=workbooks_to_push, datasource=test_name)

    pushed_workbooks = spy.workbooks.pull(spy.workbooks.search({'Name': test_name}),
                                          include_inventory=False, include_images=False,
                                          include_referenced_workbooks=False, include_archived=False)

    # noinspection PyTypeChecker
    pushed_workbook: Analysis = pushed_workbooks[f'{test_name} - Analysis']

    sorted_worksheet_names = sorted([w.name for w in pushed_workbook.worksheets])
    assert sorted_worksheet_names == [
        'Waveforms 1 Table',
        'Waveforms 1 Treemap',
        'Waveforms 1 Trend',
        'Waveforms 1 XY Plot',
        'Waveforms 2 Table',
        'Waveforms 2 Treemap',
        'Waveforms 2 Trend',
        'Waveforms 2 XY Plot'
    ]

    for asset in ['Waveforms 1', 'Waveforms 2']:
        trend_display_items = pushed_workbook.worksheets[f'{asset} Trend'].display_items
        trend_display_item_ids = sorted(trend_display_items['ID'].to_list())
        trend_expected_item_ids = sorted(
            pushed_df[(pushed_df['Asset'] == asset) & (pushed_df['Type'] == 'CalculatedSignal')]['ID'].to_list())

        assert trend_display_item_ids == trend_expected_item_ids

    # Push using "real" worksheets (from the spy.workbooks.pull() call) instead of templates
    topic.name = f'{test_name} - Topic (Second Push)'
    document.parameters = {
        "Document Name": "Waveform Monitoring and Diagnostics (Second Push)",
        "Assets": [
            {
                "Asset Name": 'Waveform 2 (Second Push)',
                "25AF462C-602A-4E13-B50D-5DEB92CB37B5 [Embedded Content] Users >> mark.derbecker@seeq.com >> Workbook Templates >> Analysis Template >> Trend Template":
                    pushed_workbook.worksheets['Waveforms 2 Trend'],
                "76B4412D-3B08-48B7-ABF9-ED8432DAA5CE [Embedded Content] Users >> mark.derbecker@seeq.com >> Workbook Templates >> Analysis Template >> XY Plot Template":
                    pushed_workbook.worksheets['Waveforms 2 XY Plot'],
                "44392AAB-D443-4D2D-ACF0-055869875558 [Embedded Content] Users >> mark.derbecker@seeq.com >> Workbook Templates >> Analysis Template >> Treemap Template":
                    pushed_workbook.worksheets['Waveforms 2 Treemap'],
                "D4F167B3-E8BA-4FAA-86B3-05877E2D4250 [Embedded Content] Users >> mark.derbecker@seeq.com >> Workbook Templates >> Analysis Template >> Table Template":
                    None  # Leave this out so we can confirm the template's "default" is used
            }
        ]
    }

    with pytest.raises(SPyValueError, match='Attempted lookup for template parameter'):
        spy.workbooks.push(workbooks_to_push, datasource=test_name)

    second_push_df = spy.workbooks.push(workbooks_to_push, datasource=test_name, lookup_df=pushed_df)

    pushed_topic = second_push_df.spy.output[f'{test_name} - Topic (Second Push)']

    assert len(pushed_topic.documents) == 1
    pushed_document = pushed_topic.documents[0]
    assert 'Waveform 1' not in pushed_document.html
    assert 'Waveform 2' in pushed_document.html
    pushed_content_worksheet_ids = [c['Worksheet ID'] for c in pushed_document.content.values()]
    assert pushed_workbook.worksheets['Waveforms 2 Trend'].id in pushed_content_worksheet_ids
    assert pushed_workbook.worksheets['Waveforms 2 XY Plot'].id in pushed_content_worksheet_ids
    assert pushed_workbook.worksheets['Waveforms 2 Treemap'].id in pushed_content_worksheet_ids
    assert pushed_workbook.worksheets['Waveforms 2 Table'].id not in pushed_content_worksheet_ids


@pytest.mark.system
def test_push_metadata_with_workbook_template():
    test_name = 'test_push_metadata_with_workbook_template'

    metadata_df = pd.DataFrame([{
        'Type': 'Signal',
        'Name': f'{test_name} Signal Alpha'
    }, {
        'Type': 'Signal',
        'Name': f'{test_name} Signal Bravo'
    }, {
        'Type': 'Signal',
        'Name': f'{test_name} Signal Charlie'
    }])

    # Load NOT as templates (as_template=None)
    workbooks = _load_test_templates(None)

    # noinspection PyTypeChecker
    workbook: Analysis = workbooks['My Analysis Template']

    workbook_template = AnalysisTemplate(test_name, workbook)
    workbook_template.name = f'{test_name} Analysis'

    code = workbook_template.code
    assert len(code) > 0

    trend_worksheet_template = workbook_template.worksheets['A Trend']

    with pytest.raises(SPyValueError, match='could not be mapped to anything in the template code'):
        trend_worksheet_template.parameters = {
            "E57C2071-0B57-4E18-BDC5-95B391DE94C6 [Asset] Cooling Tower 1": None,
        }

    trend_worksheet_template.parameters = {
        "39D2E22E-7757-42C1-AD2F-F7107FA67CD3 [Signal] Example >> Cooling Tower 1 >> Area A >> Relative Humidity":
            f'{test_name} Signal Alpha',
        "E29581FD-C0DD-49D3-BD9B-2A7BB7F5CD25 [Signal] Example >> Cooling Tower 1 >> Area A >> Temperature":
            f'{test_name} Signal Bravo'
    }

    # noinspection PyTypeChecker
    xyplot_worksheet_template = workbook_template.worksheets['An XY Plot']  # type: AnalysisWorksheetTemplate

    xyplot_worksheet_template.parameters = {
        "E29581FD-C0DD-49D3-BD9B-2A7BB7F5CD25 [Signal] Example >> Cooling Tower 1 >> Area A >> Temperature":
            f'{test_name} Signal Charlie',
        "B9E96DB3-0C9D-46DA-BC0E-7E9E5074582E [Signal] Example >> Cooling Tower 1 >> Area A >> Compressor Power":
            f'{test_name} Signal Bravo',
        "50D438D9-C93E-4F9A-804F-A96AEE60D3F1 [Asset] Example >> Cooling Tower 1 >> Area A": None
    }

    xyplot_worksheet_template_2 = xyplot_worksheet_template.copy(
        f'{test_name} xyplot 2')

    xyplot_worksheet_template_2.name = 'Another Scatterplot'

    xyplot_worksheet_template_2.parameters = {
        "E29581FD-C0DD-49D3-BD9B-2A7BB7F5CD25 [Signal] Example >> Cooling Tower 1 >> Area A >> Temperature":
            f'{test_name} Signal Alpha',
        "B9E96DB3-0C9D-46DA-BC0E-7E9E5074582E [Signal] Example >> Cooling Tower 1 >> Area A >> Compressor Power":
            f'{test_name} Signal Charlie',
        "50D438D9-C93E-4F9A-804F-A96AEE60D3F1 [Asset] Example >> Cooling Tower 1 >> Area A": None
    }

    push_results_df = spy.push(metadata=metadata_df, workbook=workbook_template, datasource=test_name)

    workbook_url = push_results_df.spy.workbook_url
    pushed_workbooks = spy.workbooks.pull(workbook_url)

    # noinspection PyTypeChecker
    pushed_workbook: spy.workbooks.Analysis = pushed_workbooks[0]

    signal_alpha_id = push_results_df[push_results_df['Name'] == f'{test_name} Signal Alpha'].iloc[0]['ID']
    signal_bravo_id = push_results_df[push_results_df['Name'] == f'{test_name} Signal Bravo'].iloc[0]['ID']
    signal_charlie_id = push_results_df[push_results_df['Name'] == f'{test_name} Signal Charlie'].iloc[0]['ID']

    pushed_trend_worksheet = pushed_workbook.worksheets['A Trend']
    pushed_trend_worksheet_display_item_ids = pushed_trend_worksheet.display_items['ID'].tolist()
    assert signal_alpha_id in pushed_trend_worksheet_display_item_ids
    assert signal_bravo_id in pushed_trend_worksheet_display_item_ids
    assert signal_charlie_id not in pushed_trend_worksheet_display_item_ids

    pushed_xyplot_worksheet = pushed_workbook.worksheets['An XY Plot']
    pushed_xyplot_worksheet_display_item_ids = pushed_xyplot_worksheet.display_items['ID'].tolist()
    assert signal_alpha_id not in pushed_xyplot_worksheet_display_item_ids
    assert signal_bravo_id in pushed_xyplot_worksheet_display_item_ids
    assert signal_charlie_id in pushed_xyplot_worksheet_display_item_ids

    pushed_xyplot_worksheet_2 = pushed_workbook.worksheets['Another Scatterplot']
    pushed_xyplot_worksheet_display_item_ids_2 = pushed_xyplot_worksheet_2.display_items['ID'].tolist()
    assert signal_alpha_id in pushed_xyplot_worksheet_display_item_ids_2
    assert signal_bravo_id not in pushed_xyplot_worksheet_display_item_ids_2
    assert signal_charlie_id in pushed_xyplot_worksheet_display_item_ids_2


@pytest.mark.system
def test_push_metadata_with_worksheet_template():
    test_name = 'test_push_metadata_with_worksheet_template'

    metadata_df = pd.DataFrame([{
        'Type': 'Signal',
        'Name': f'{test_name} Signal Alpha'
    }, {
        'Type': 'Signal',
        'Name': f'{test_name} Signal Bravo'
    }])

    area_c_df = spy.search({'Path': 'Example', 'Name': 'Area C', 'Type': 'Asset'})

    # Load NOT as templates (as_template=None)
    workbooks = _load_test_templates(None)

    # noinspection PyTypeChecker
    workbook: Analysis = workbooks['My Analysis Template']
    workbook_template = AnalysisTemplate(test_name, workbook)
    trend_worksheet_template = workbook_template.worksheets['A Trend']

    trend_worksheet_template.parameters = {
        "4B40EAFC-91ED-4AB0-8199-F21AF40A8350 [Asset] Example >> Area A": area_c_df,
        "39D2E22E-7757-42C1-AD2F-F7107FA67CD3 [Signal] Example >> Cooling Tower 1 >> Area A >> Relative Humidity":
            f'{test_name} Signal Alpha',
        "E29581FD-C0DD-49D3-BD9B-2A7BB7F5CD25 [Signal] Example >> Cooling Tower 1 >> Area A >> Temperature":
            f'{test_name} Signal Bravo'
    }

    # Do it twice, since there is some different code if the workbook already exists
    for i in range(2):
        push_results_df = spy.push(metadata=metadata_df, workbook=test_name,
                                   worksheet=trend_worksheet_template, datasource=test_name)

        workbook_url = push_results_df.spy.workbook_url
        pushed_workbooks = spy.workbooks.pull(workbook_url)

        # noinspection PyTypeChecker
        pushed_workbook: spy.workbooks.Analysis = pushed_workbooks[0]

        signal_alpha_id = push_results_df[push_results_df['Name'] == f'{test_name} Signal Alpha'].iloc[0]['ID']
        signal_bravo_id = push_results_df[push_results_df['Name'] == f'{test_name} Signal Bravo'].iloc[0]['ID']

        pushed_trend_worksheet = pushed_workbook.worksheets['A Trend']
        pushed_trend_worksheet_display_item_ids = pushed_trend_worksheet.display_items['ID'].tolist()
        assert signal_alpha_id in pushed_trend_worksheet_display_item_ids
        assert signal_bravo_id in pushed_trend_worksheet_display_item_ids


@pytest.mark.system
def test_push_metadata_with_topic_template_using_worksteps():
    test_name = 'test_push_metadata_with_topic_template_using_worksteps'

    search_df = spy.search({'Name': 'Area ?_*'})

    workbooks = _load_test_templates(test_name)

    analysis = workbooks['My Analysis Template']
    analysis.name = f'{test_name} Analysis'
    topic = workbooks['My Topic Template']
    topic.name = f'{test_name} Topic'
    # noinspection PyUnresolvedReferences
    topic_document: TopicDocumentTemplate = topic.documents['My Document Template']
    # noinspection PyUnresolvedReferences
    trend_worksheet = analysis.worksheets['A Trend']
    trend_workstep: AnalysisWorkstepTemplate = trend_worksheet.current_workstep()
    # noinspection PyUnresolvedReferences
    xyplot_worksheet = analysis.worksheets['An XY Plot']
    xyplot_workstep: AnalysisWorkstepTemplate = xyplot_worksheet.current_workstep()

    doc_code = topic_document.code
    assert '[Date Range Condition] Too Much Power' in doc_code
    assert '[Asset Selection] Example >> Cooling Tower 1 >> Area B' in doc_code
    assert ('[Embedded Content] Users >> mark-derbecker@seeq-com >> '
            'Workbook Template Tests >> My Analysis Template >> A Trend' in doc_code)
    assert ('[Embedded Content] Users >> mark-derbecker@seeq-com >> '
            'Workbook Template Tests >> My Analysis Template >> A Treemap') in doc_code
    assert ('[Embedded Content] Users >> mark-derbecker@seeq-com >> '
            'Workbook Template Tests >> My Analysis Template >> A Table') in doc_code
    assert ('[Embedded Content] Users >> mark-derbecker@seeq-com >> '
            'Workbook Template Tests >> My Analysis Template >> An XY Plot') in doc_code

    area_c_df = spy.search({'Path': 'Example', 'Name': 'Area C', 'Type': 'Asset'})
    hours_df = spy.push(metadata=pd.DataFrame([{
        'Name': f'{test_name} Weekdays', 'Type': 'Condition', 'Formula': 'hours()'}]),
        workbook=test_name, worksheet=None, datasource=test_name)

    for area in ['G', 'H']:
        label = f'{test_name} Area {area}'

        area_document = topic_document.copy(label)
        area_document.name = label

        area_trend_workstep = trend_workstep.copy(label)
        area_trend_workstep.parameters = {
            "39D2E22E-7757-42C1-AD2F-F7107FA67CD3 [Signal] Example >> Cooling Tower 1 >> Area A >> Relative Humidity":
                search_df[search_df['Name'] == f'Area {area}_Relative Humidity'],
            "E29581FD-C0DD-49D3-BD9B-2A7BB7F5CD25 [Signal] Example >> Cooling Tower 1 >> Area A >> Temperature":
                search_df[search_df['Name'] == f'Area {area}_Temperature']
        }

        area_xyplot_workstep = xyplot_workstep.copy(label)
        area_xyplot_workstep.parameters = {
            "E29581FD-C0DD-49D3-BD9B-2A7BB7F5CD25 [Signal] Example >> Cooling Tower 1 >> Area A >> Temperature":
                search_df[search_df['Name'] == f'Area {area}_Temperature'],
            "B9E96DB3-0C9D-46DA-BC0E-7E9E5074582E [Signal] Example >> Cooling Tower 1 >> Area A >> Compressor Power":
                search_df[search_df['Name'] == f'Area {area}_Compressor Power']
        }

        area_document.parameters = {
            '[Date Range Condition] Too Much Power': hours_df,
            "[Asset Selection] Example >> Cooling Tower 1 >> Area B": area_c_df,
            "Users >> mark-derbecker@seeq-com >> Workbook Template Tests >> My Analysis Template >> A Trend":
                area_trend_workstep,
            "Users >> mark-derbecker@seeq-com >> Workbook Template Tests >> My Analysis Template >> An XY Plot":
                area_xyplot_workstep,
        }

    push_df = spy.workbooks.push(workbooks, datasource=test_name, refresh=False, include_inventory=True)

    push_df['ID'] = push_df['Pushed Workbook ID']

    pulled_workbooks = spy.workbooks.pull(push_df)
    assert len(pulled_workbooks) == 2

    assert f'{test_name} Analysis' in pulled_workbooks
    # noinspection PyTypeChecker
    pulled_analysis: Analysis = pulled_workbooks[f'{test_name} Analysis']
    assert len(pulled_analysis.worksheets) == 4
    assert pulled_analysis.worksheets[0].name == 'A Trend'
    assert pulled_analysis.worksheets[1].name == 'An XY Plot'
    assert pulled_analysis.worksheets[2].name == 'A Treemap'
    assert pulled_analysis.worksheets[3].name == 'A Table'

    assert f'{test_name} Topic' in pulled_workbooks
    # noinspection PyTypeChecker
    pulled_topic: Topic = pulled_workbooks[f'{test_name} Topic']
    assert len(pulled_topic.worksheets) == 4
    doc_template = pulled_topic.worksheets[0]
    doc_mustache = pulled_topic.worksheets[1]
    doc_area_g = pulled_topic.worksheets[2]
    doc_area_h = pulled_topic.worksheets[3]

    assert doc_template.name == 'My Document Template'
    assert doc_mustache.name == 'My Mustache'
    assert doc_area_g.name == f'{test_name} Area G'
    assert doc_area_h.name == f'{test_name} Area H'

    area_a_search_df_for_trend = spy.search({'Path': 'Example >> Cooling Tower 1 >> Area A',
                                             'Name': '/(Temperature|Relative Humidity)/'})

    area_a_search_df_for_xyplot = spy.search({'Path': 'Example >> Cooling Tower 1 >> Area A',
                                              'Name': '/(Temperature|Compressor Power)/'})

    expected_trend_display_ids = {
        doc_template: area_a_search_df_for_trend['ID'].to_list(),
        doc_area_g: search_df[search_df['Name'].isin(
            ['Area G_Relative Humidity', 'Area G_Temperature'])]['ID'].to_list(),
        doc_area_h: search_df[search_df['Name'].isin(
            ['Area H_Relative Humidity', 'Area H_Temperature'])]['ID'].to_list()
    }

    expected_xyplot_display_ids = {
        doc_template: area_a_search_df_for_xyplot['ID'].to_list(),
        doc_area_g: search_df[search_df['Name'].isin(
            ['Area G_Compressor Power', 'Area G_Temperature'])]['ID'].to_list(),
        doc_area_h: search_df[search_df['Name'].isin(
            ['Area H_Compressor Power', 'Area H_Temperature'])]['ID'].to_list()
    }

    for doc in [doc_template, doc_area_g, doc_area_h]:
        _assert_doc(doc, pulled_workbooks, expected_trend_display_ids[doc],
                    expected_xyplot_display_ids[doc])

        doc_date_range = list(doc.date_ranges.values())[0]
        doc_asset_selection = list(doc.asset_selections.values())[0]

        if doc is doc_template:
            assert doc_date_range['Condition ID'] != hours_df.iloc[0]['ID']
            assert doc_asset_selection['Asset ID'] != area_c_df.iloc[0]['ID']
        else:
            assert doc_date_range['Condition ID'] == hours_df.iloc[0]['ID']
            assert doc_asset_selection['Asset ID'] == area_c_df.iloc[0]['ID']


def _assert_doc(topic_document, pulled_workbooks, expected_trend_display_ids, expected_xyplot_display_ids):
    content_ids = re.findall(r'data-seeq-content="([^"]+)"', topic_document.html)

    trend_content = topic_document.content[content_ids[0]]
    # noinspection PyTypeChecker
    trend_workbook: Analysis = pulled_workbooks[trend_content['Workbook ID']]
    trend_worksheet: AnalysisWorksheet = trend_workbook.worksheets[trend_content['Worksheet ID']]
    trend_workstep = trend_worksheet.worksteps[trend_content['Workstep ID']]
    assert trend_workstep.view == 'Trend'

    trend_journal = trend_worksheet.journal
    trend_journal_references = trend_journal.referenced_worksteps
    assert len(trend_journal_references) == 1
    ref_workbook_id, ref_worksheet_id, ref_workstep_id = trend_journal_references.pop()
    assert ref_workstep_id in trend_worksheet.worksteps

    expected_signal_ids = sorted(expected_trend_display_ids)
    trend_display_item_ids = sorted(trend_workstep.display_items['ID'].to_list())
    assert expected_signal_ids == trend_display_item_ids

    xyplot_content = topic_document.content[content_ids[1]]
    # noinspection PyTypeChecker
    xyplot_workbook: Analysis = pulled_workbooks[xyplot_content['Workbook ID']]
    xyplot_worksheet = xyplot_workbook.worksheets[xyplot_content['Worksheet ID']]
    xyplot_workstep = xyplot_worksheet.worksteps[xyplot_content['Workstep ID']]
    assert xyplot_workstep.view == 'Scatter Plot'

    expected_signal_ids = sorted(expected_xyplot_display_ids)
    xyplot_display_item_ids = sorted(xyplot_workstep.display_items['ID'].to_list())
    assert expected_signal_ids == xyplot_display_item_ids

    treemap_content = topic_document.content[content_ids[2]]
    # noinspection PyTypeChecker
    treemap_workbook: Analysis = pulled_workbooks[treemap_content['Workbook ID']]
    treemap_worksheet = treemap_workbook.worksheets[treemap_content['Worksheet ID']]
    treemap_workstep = treemap_worksheet.worksteps[treemap_content['Workstep ID']]
    assert treemap_workstep.view == 'Treemap'

    table_content = topic_document.content[content_ids[3]]
    # noinspection PyTypeChecker
    table_workbook: Analysis = pulled_workbooks[table_content['Workbook ID']]
    table_worksheet = table_workbook.worksheets[table_content['Worksheet ID']]
    table_workstep = table_worksheet.worksteps[table_content['Workstep ID']]
    assert table_workstep.view == 'Table'


@pytest.mark.system
def test_push_metadata_with_topic_template_using_worksteps():
    test_name = 'test_push_metadata_with_topic_template_using_worksheets'

    search_df = spy.search({'Name': 'Area ?_*'})

    workbooks = _load_test_templates(test_name)

    analysis = workbooks['My Analysis Template']
    analysis.name = f'{test_name} Analysis'
    topic = workbooks['My Topic Template']
    topic.name = f'{test_name} Topic'
    # noinspection PyUnresolvedReferences
    topic_document: TopicDocumentTemplate = topic.documents['My Document Template']
    # noinspection PyUnresolvedReferences
    trend_worksheet = analysis.worksheets['A Trend']
    # noinspection PyUnresolvedReferences
    xyplot_worksheet = analysis.worksheets['An XY Plot']

    for area in ['G', 'H']:
        label = f'{test_name} Area {area}'

        area_document = topic_document.copy(label)
        area_document.name = label

        area_trend_worksheet = trend_worksheet.copy(label)
        area_trend_worksheet.name = f'This is Area {area} Trend!'
        area_trend_worksheet.parameters = {
            "39D2E22E-7757-42C1-AD2F-F7107FA67CD3 [Signal] Example >> Cooling Tower 1 >> Area A >> Relative Humidity":
                search_df[search_df['Name'] == f'Area {area}_Relative Humidity'],
            "E29581FD-C0DD-49D3-BD9B-2A7BB7F5CD25 [Signal] Example >> Cooling Tower 1 >> Area A >> Temperature":
                search_df[search_df['Name'] == f'Area {area}_Temperature']
        }

        area_xyplot_worksheet = xyplot_worksheet.copy(label)
        area_xyplot_worksheet.name = f'This is Area {area} XY Plot!'
        area_xyplot_worksheet.parameters = {
            "E29581FD-C0DD-49D3-BD9B-2A7BB7F5CD25 [Signal] Example >> Cooling Tower 1 >> Area A >> Temperature":
                search_df[search_df['Name'] == f'Area {area}_Temperature'],
            "B9E96DB3-0C9D-46DA-BC0E-7E9E5074582E [Signal] Example >> Cooling Tower 1 >> Area A >> Compressor Power":
                search_df[search_df['Name'] == f'Area {area}_Compressor Power']
        }

        area_document.parameters = {
            "Users >> mark-derbecker@seeq-com >> Workbook Template Tests >> My Analysis Template >> A Trend":
                area_trend_worksheet,
            "Users >> mark-derbecker@seeq-com >> Workbook Template Tests >> My Analysis Template >> An XY Plot":
                area_xyplot_worksheet,
        }

    push_df = spy.workbooks.push(workbooks, datasource=test_name, refresh=False, include_inventory=True)

    push_df['ID'] = push_df['Pushed Workbook ID']

    pulled_workbooks = spy.workbooks.pull(push_df)
    assert len(pulled_workbooks) == 2

    assert f'{test_name} Analysis' in pulled_workbooks
    # noinspection PyTypeChecker
    pulled_analysis: Analysis = pulled_workbooks[f'{test_name} Analysis']
    assert len(pulled_analysis.worksheets) == 8
    assert pulled_analysis.worksheets[0].name == 'A Trend'
    assert pulled_analysis.worksheets[1].name == 'An XY Plot'
    assert pulled_analysis.worksheets[2].name == 'A Treemap'
    assert pulled_analysis.worksheets[3].name == 'A Table'
    assert pulled_analysis.worksheets[4].name == 'This is Area G Trend!'
    assert pulled_analysis.worksheets[5].name == 'This is Area G XY Plot!'
    assert pulled_analysis.worksheets[6].name == 'This is Area H Trend!'
    assert pulled_analysis.worksheets[7].name == 'This is Area H XY Plot!'

    assert f'{test_name} Topic' in pulled_workbooks
    # noinspection PyTypeChecker
    pulled_topic: Topic = pulled_workbooks[f'{test_name} Topic']
    assert len(pulled_topic.worksheets) == 4
    doc_template: TopicDocument = pulled_topic.worksheets[0]
    doc_mustache: TopicDocument = pulled_topic.worksheets[1]
    doc_area_g: TopicDocument = pulled_topic.worksheets[2]
    doc_area_h: TopicDocument = pulled_topic.worksheets[3]

    assert doc_template.name == 'My Document Template'
    assert doc_mustache.name == 'My Mustache'
    assert doc_area_g.name == f'{test_name} Area G'
    assert doc_area_h.name == f'{test_name} Area H'

    area_a_search_df_for_trend = spy.search({'Path': 'Example >> Cooling Tower 1 >> Area A',
                                             'Name': '/(Temperature|Relative Humidity)/'})

    area_a_search_df_for_xyplot = spy.search({'Path': 'Example >> Cooling Tower 1 >> Area A',
                                              'Name': '/(Temperature|Compressor Power)/'})

    expected_trend_display_ids = {
        doc_template: area_a_search_df_for_trend['ID'].to_list(),
        doc_area_g: search_df[search_df['Name'].isin(
            ['Area G_Relative Humidity', 'Area G_Temperature'])]['ID'].to_list(),
        doc_area_h: search_df[search_df['Name'].isin(
            ['Area H_Relative Humidity', 'Area H_Temperature'])]['ID'].to_list()
    }

    expected_xyplot_display_ids = {
        doc_template: area_a_search_df_for_xyplot['ID'].to_list(),
        doc_area_g: search_df[search_df['Name'].isin(
            ['Area G_Compressor Power', 'Area G_Temperature'])]['ID'].to_list(),
        doc_area_h: search_df[search_df['Name'].isin(
            ['Area H_Compressor Power', 'Area H_Temperature'])]['ID'].to_list()
    }

    for topic_document in [doc_template, doc_area_g, doc_area_h]:
        _assert_doc(topic_document, pulled_workbooks, expected_trend_display_ids[topic_document],
                    expected_xyplot_display_ids[topic_document])


@pytest.mark.system
def test_push_metadata_with_topic_template_with_mustache_using_worksheets():
    test_name = 'test_push_metadata_with_topic_template_with_mustache_using_worksheets'

    search_df = spy.search({'Name': '/Area [GH]_.*/'})

    workbooks = _load_test_templates(test_name)

    analysis = workbooks['My Analysis Template']
    analysis.name = f'{test_name} Analysis'
    topic = workbooks['My Topic Template']
    topic.name = f'{test_name} Topic'
    # noinspection PyUnresolvedReferences
    topic_document: TopicDocumentTemplate = topic.documents['My Mustache']
    # noinspection PyUnresolvedReferences
    trend_worksheet = analysis.worksheets['A Trend']
    # noinspection PyUnresolvedReferences
    xyplot_worksheet = analysis.worksheets['An XY Plot']

    trends = dict()
    xy_plots = dict()
    for area in ['G', 'H']:
        label = f'{test_name} Area {area}'

        area_trend_worksheet = trend_worksheet.copy(label)
        area_trend_worksheet.name = f'This is Area {area} Trend!'
        area_trend_worksheet.parameters = {
            "39D2E22E-7757-42C1-AD2F-F7107FA67CD3 [Signal] Example >> Cooling Tower 1 >> Area A >> Relative Humidity":
                search_df[search_df['Name'] == f'Area {area}_Relative Humidity'],
            "E29581FD-C0DD-49D3-BD9B-2A7BB7F5CD25 [Signal] Example >> Cooling Tower 1 >> Area A >> Temperature":
                search_df[search_df['Name'] == f'Area {area}_Temperature']
        }

        area_xyplot_worksheet = xyplot_worksheet.copy(label)
        area_xyplot_worksheet.name = f'This is Area {area} XY Plot!'
        area_xyplot_worksheet.parameters = {
            "E29581FD-C0DD-49D3-BD9B-2A7BB7F5CD25 [Signal] Example >> Cooling Tower 1 >> Area A >> Temperature":
                search_df[search_df['Name'] == f'Area {area}_Temperature'],
            "B9E96DB3-0C9D-46DA-BC0E-7E9E5074582E [Signal] Example >> Cooling Tower 1 >> Area A >> Compressor Power":
                search_df[search_df['Name'] == f'Area {area}_Compressor Power']
        }

        trends[area] = area_trend_worksheet
        xy_plots[area] = area_xyplot_worksheet

    topic_document.parameters = {
        "date": 'May 18, 1980',
        "place": 'Randall, Washington',
        "areas": [
            {
                "area": f'Area {area}',
                "code block": f'This is Area {area}, friend!',
                "43AEEEA7-BC98-446A-BAD9-7BB3B1526A62": trends[area],
                "D49C555A-8C8A-42A5-8E21-A824218A7C87": xy_plots[area]
            }
            for area in ['G', 'H']
        ]
    }

    push_df = spy.workbooks.push(workbooks, datasource=test_name, refresh=False, include_inventory=True)

    push_df['ID'] = push_df['Pushed Workbook ID']

    pulled_workbooks = spy.workbooks.pull(push_df)
    assert len(pulled_workbooks) == 2
    assert f'{test_name} Analysis' in pulled_workbooks
    assert f'{test_name} Topic' in pulled_workbooks

    # noinspection PyTypeChecker
    pulled_topic: Topic = pulled_workbooks[f'{test_name} Topic']
    assert len(pulled_topic.worksheets) == 2
    doc_mustache: TopicDocument = pulled_topic.worksheets[1]

    assert doc_mustache.name == 'My Mustache'

    content_ids = re.findall(r'data-seeq-content="([^"]+)"', doc_mustache.html)

    area_g_trend_content = doc_mustache.content[content_ids[0]]
    area_g_xyplot_content = doc_mustache.content[content_ids[1]]
    area_h_trend_content = doc_mustache.content[content_ids[2]]
    area_h_xyplot_content = doc_mustache.content[content_ids[3]]

    def _get_workstep(_content):
        # noinspection PyTypeChecker
        _trend_workbook: Analysis = pulled_workbooks[_content['Workbook ID']]
        _trend_worksheet: AnalysisWorksheet = _trend_workbook.worksheets[_content['Worksheet ID']]
        _trend_workstep = _trend_worksheet.worksteps[_content['Workstep ID']]
        return _trend_workstep

    def _get_id(_df, _name):
        return _df[_df['Name'] == _name].iloc[0]['ID']

    area_g_trend_workstep: AnalysisWorkstep = _get_workstep(area_g_trend_content)
    area_h_trend_workstep: AnalysisWorkstep = _get_workstep(area_h_trend_content)
    assert area_g_trend_workstep.view == 'Trend'
    assert area_h_trend_workstep.view == 'Trend'
    g_ids = area_g_trend_workstep.display_items['ID'].to_list()
    h_ids = area_h_trend_workstep.display_items['ID'].to_list()
    assert _get_id(search_df, 'Area G_Relative Humidity') in g_ids
    assert _get_id(search_df, 'Area G_Temperature') in g_ids
    assert _get_id(search_df, 'Area H_Relative Humidity') in h_ids
    assert _get_id(search_df, 'Area H_Temperature') in h_ids

    area_g_xyplot_workstep: AnalysisWorkstep = _get_workstep(area_g_xyplot_content)
    area_h_xyplot_workstep: AnalysisWorkstep = _get_workstep(area_h_xyplot_content)
    assert area_g_xyplot_workstep.view == 'Scatter Plot'
    assert area_h_xyplot_workstep.view == 'Scatter Plot'
    g_ids = area_g_xyplot_workstep.display_items['ID'].to_list()
    h_ids = area_h_xyplot_workstep.display_items['ID'].to_list()
    assert _get_id(search_df, 'Area G_Temperature') in g_ids
    assert _get_id(search_df, 'Area G_Compressor Power') in g_ids
    assert _get_id(search_df, 'Area H_Temperature') in h_ids
    assert _get_id(search_df, 'Area H_Compressor Power') in h_ids


@pytest.mark.system
def test_push_metadata_with_topic_template_with_mustache_using_worksteps():
    test_name = 'test_push_metadata_with_topic_template_with_mustache_using_worksteps'

    search_df = spy.search({'Name': '/Area [GH]_.*/'})

    workbooks = _load_test_templates(test_name)

    analysis = workbooks['My Analysis Template']
    analysis.name = f'{test_name} Analysis'
    topic = workbooks['My Topic Template']
    topic.name = f'{test_name} Topic'
    # noinspection PyUnresolvedReferences
    topic_document: TopicDocumentTemplate = topic.documents['My Mustache']
    # noinspection PyUnresolvedReferences
    trend_workstep = analysis.worksheets['A Trend'].current_workstep()

    trends = dict()
    for area in ['G', 'H']:
        label = f'{test_name} Area {area}'

        area_trend_workstep = trend_workstep.copy(label)
        area_trend_workstep.parameters = {
            "39D2E22E-7757-42C1-AD2F-F7107FA67CD3 [Signal] Example >> Cooling Tower 1 >> Area A >> Relative Humidity":
                search_df[search_df['Name'] == f'Area {area}_Relative Humidity'],
            "E29581FD-C0DD-49D3-BD9B-2A7BB7F5CD25 [Signal] Example >> Cooling Tower 1 >> Area A >> Temperature":
                search_df[search_df['Name'] == f'Area {area}_Temperature']
        }

        trends[area] = area_trend_workstep

    topic_document.parameters = {
        "date": 'May 18, 1980',
        "place": 'Randall, Washington',
        "areas": [
            {
                "area": f'Area {area}',
                "code block": f'This is Area {area}, friend!',
                "43AEEEA7-BC98-446A-BAD9-7BB3B1526A62": trends[area]
            }
            for area in ['G', 'H']
        ]
    }

    push_df = spy.workbooks.push(workbooks, datasource=test_name, refresh=False, include_inventory=True)

    push_df['ID'] = push_df['Pushed Workbook ID']

    pulled_workbooks = spy.workbooks.pull(push_df)
    assert len(pulled_workbooks) == 2
    assert f'{test_name} Analysis' in pulled_workbooks
    assert f'{test_name} Topic' in pulled_workbooks

    # noinspection PyTypeChecker
    pulled_topic: Topic = pulled_workbooks[f'{test_name} Topic']
    assert len(pulled_topic.worksheets) == 2
    doc_mustache: TopicDocument = pulled_topic.worksheets[1]

    assert doc_mustache.name == 'My Mustache'

    content_ids = re.findall(r'data-seeq-content="([^"]+)"', doc_mustache.html)

    area_g_trend_content = doc_mustache.content[content_ids[0]]
    area_g_xyplot_content = doc_mustache.content[content_ids[1]]
    area_h_trend_content = doc_mustache.content[content_ids[2]]
    area_h_xyplot_content = doc_mustache.content[content_ids[3]]

    def _get_workstep(_content):
        # noinspection PyTypeChecker
        _trend_workbook: Analysis = pulled_workbooks[_content['Workbook ID']]
        _trend_worksheet: AnalysisWorksheet = _trend_workbook.worksheets[_content['Worksheet ID']]
        _trend_workstep = _trend_worksheet.worksteps[_content['Workstep ID']]
        return _trend_workstep

    def _get_id(_df, _name):
        return _df[_df['Name'] == _name].iloc[0]['ID']

    area_g_trend_workstep: AnalysisWorkstep = _get_workstep(area_g_trend_content)
    area_h_trend_workstep: AnalysisWorkstep = _get_workstep(area_h_trend_content)
    assert area_g_trend_workstep.view == 'Trend'
    assert area_h_trend_workstep.view == 'Trend'
    g_ids = area_g_trend_workstep.display_items['ID'].to_list()
    h_ids = area_h_trend_workstep.display_items['ID'].to_list()
    assert _get_id(search_df, 'Area G_Relative Humidity') in g_ids
    assert _get_id(search_df, 'Area G_Temperature') in g_ids
    assert _get_id(search_df, 'Area H_Relative Humidity') in h_ids
    assert _get_id(search_df, 'Area H_Temperature') in h_ids

    # We purposefully did not map the XY Plot in the parameters, so they should just be what was in the template
    tree_df = spy.search({'Path': 'Example >> Cooling Tower 1 >> Area A', 'Name': '/Temperature|Compressor Power/'})
    area_g_xyplot_workstep: AnalysisWorkstep = _get_workstep(area_g_xyplot_content)
    area_h_xyplot_workstep: AnalysisWorkstep = _get_workstep(area_h_xyplot_content)
    assert area_g_xyplot_workstep.view == 'Scatter Plot'
    assert area_h_xyplot_workstep.view == 'Scatter Plot'
    g_ids = area_g_xyplot_workstep.display_items['ID'].to_list()
    h_ids = area_h_xyplot_workstep.display_items['ID'].to_list()
    assert _get_id(tree_df, 'Temperature') in g_ids
    assert _get_id(tree_df, 'Compressor Power') in g_ids
    assert _get_id(tree_df, 'Temperature') in h_ids
    assert _get_id(tree_df, 'Compressor Power') in h_ids


class SpyAssetsTemplateTest(Asset):

    @Asset.Attribute()
    def Temperature(self, metadata):
        return metadata[metadata['Name'].str.contains('Temperature')]

    @Asset.Attribute()
    def Optimizer(self, metadata):
        return metadata[metadata['Name'].str.contains('Optimizer')]

    @Asset.Attribute()
    def Hours(self, metadata):
        return {
            'Type': 'Condition',
            'Formula': 'hours()'
        }

    @Asset.Attribute()
    def Weeks(self, metadata):
        return {
            'Type': 'Condition',
            'Formula': 'weeks()'
        }


class SpyAssetsTemplateTest_ByWorkstep(SpyAssetsTemplateTest):

    @Asset.Display()
    def Trend_Workstep(self, metadata, analysis):
        trend_template: AnalysisWorkstepTemplate = analysis.worksheets['A Trend'].current_workstep()
        workstep = trend_template.copy(self.fqn)
        workstep.parameters = {
            "4B40EAFC-91ED-4AB0-8199-F21AF40A8350 [Asset] Example >> Area A": self,
            "B9CDE282-7A1A-4E28-A173-12E7347AB891 [Signal] Example >> Cooling Tower 1 >> Area A >> Relative "
            "Humidity": self.Optimizer(),
            "8A4F0E26-8A0C-4127-9E11-B67E031C6049 [Signal] Example >> Cooling Tower 1 >> Area A >> Temperature":
                self.Temperature()
        }
        return workstep

    @Asset.Document()
    def Topic_Via_Workstep(self, metadata, topic):
        doc_template = topic.documents['My Document Template']
        document = doc_template.copy(self.fqn)
        document.name = f'Dashboard for {self["Name"]}'
        document.parameters = {
            "3DD9D37A-B3C2-45E9-9118-2D3F307F5CA6 [Embedded Content] Shared >> Templates >> My Analysis Template >> A Trend":
                self.Trend_Workstep(),
            "AFFC25A9-CE0B-4583-9CD6-6FFDD49F262A [Date Range Condition] Too Much Power": self.Weeks(),
            "563891EC-97FD-4C25-8BDF-BF321E13A1F7 [Embedded Content] Shared >> Templates >> My Analysis Template >> A Treemap": None,
            "8E47C8E5-278F-40AD-AAE3-8F3D91E82E73 [Asset Selection] Example >> Area B": self,
            "58E2DFB0-4D24-421B-9203-AD9B3AB10E9F [Embedded Content] Shared >> Templates >> My Analysis Template >> A Table": None,
            "9B110784-3A9C-495E-9EE4-02DE8E831F40 [Embedded Content] Shared >> Templates >> My Analysis Template >> An XY Plot": None
        }
        return document


class SpyAssetsTemplateTest_ByWorksheet(SpyAssetsTemplateTest):

    @Asset.Display()
    def Trend_Worksheet(self, metadata, analysis):
        trend_template: AnalysisWorksheetTemplate = analysis.worksheets['A Trend']
        worksheet = trend_template.copy(self.fqn)
        worksheet.name = f'Trend for {self["Name"]}'
        worksheet.parameters = {
            "4B40EAFC-91ED-4AB0-8199-F21AF40A8350 [Asset] Example >> Area A": self,
            "B9CDE282-7A1A-4E28-A173-12E7347AB891 [Signal] Example >> Cooling Tower 1 >> Area A >> Relative "
            "Humidity": self.Optimizer(),
            "8A4F0E26-8A0C-4127-9E11-B67E031C6049 [Signal] Example >> Cooling Tower 1 >> Area A >> Temperature":
                self.Temperature()
        }
        return worksheet

    @Asset.Document()
    def Topic_Via_Worksheet(self, metadata, topic):
        doc_template = topic.documents['My Document Template']
        document = doc_template.copy(self.fqn)
        document.name = f'Dashboard for {self["Name"]}'
        document.parameters = {
            "3DD9D37A-B3C2-45E9-9118-2D3F307F5CA6 [Embedded Content] Shared >> Templates >> My Analysis Template >> A Trend":
                self.Trend_Worksheet(),
            "AFFC25A9-CE0B-4583-9CD6-6FFDD49F262A [Date Range Condition] Too Much Power": self.Hours(),
            "563891EC-97FD-4C25-8BDF-BF321E13A1F7 [Embedded Content] Shared >> Templates >> My Analysis Template >> A Treemap": None,
            "8E47C8E5-278F-40AD-AAE3-8F3D91E82E73 [Asset Selection] Example >> Area B": self,
            "58E2DFB0-4D24-421B-9203-AD9B3AB10E9F [Embedded Content] Shared >> Templates >> My Analysis Template >> A Table": None,
            "9B110784-3A9C-495E-9EE4-02DE8E831F40 [Embedded Content] Shared >> Templates >> My Analysis Template >> An XY Plot": None
        }
        return document


@pytest.mark.system
def test_spy_assets_with_templates_with_worksheets():
    test_name = 'test_spy_assets_with_templates_with_worksheets'
    model = SpyAssetsTemplateTest_ByWorksheet

    def _assert(area, pushed_document, pushed_analysis, push_df):
        trend_worksheet: AnalysisWorksheet = pushed_analysis.worksheets[f'Trend for Area {area}1']
        content_ids = re.findall(r'data-seeq-content="([^"]+)"', pushed_document.html)
        trend_content_id = content_ids[0]
        trend_content = pushed_document.content[trend_content_id]
        assert trend_content['Workbook ID'] == pushed_analysis.id
        assert trend_content['Worksheet ID'] == trend_worksheet.id
        assert trend_content['Workstep ID'] == trend_worksheet.current_workstep().id
        display_item_ids = sorted(trend_worksheet.display_items['ID'].to_list())
        expected_item_ids = sorted(push_df[(push_df['Asset'] == f'Area {area}1') &
                                           (push_df['Type'] == 'CalculatedSignal')]['ID'].to_list())
        assert display_item_ids == expected_item_ids
        assert 'a cat' in trend_worksheet.journal.html

    _test_spy_assets_with_templates(test_name, model, _assert, 'Hours')


@pytest.mark.system
def test_spy_assets_with_templates_with_worksteps():
    test_name = 'test_spy_assets_with_templates_with_worksteps'
    model = SpyAssetsTemplateTest_ByWorkstep

    def _assert(area, pushed_document, pushed_analysis, push_df):
        trend_worksheet: AnalysisWorksheet = pushed_analysis.worksheets[f'A Trend']
        content_ids = re.findall(r'data-seeq-content="([^"]+)"', pushed_document.html)
        trend_content_id = content_ids[0]
        trend_content = pushed_document.content[trend_content_id]
        assert trend_content['Workbook ID'] == pushed_analysis.id
        assert trend_content['Worksheet ID'] == trend_worksheet.id
        workstep: AnalysisWorkstep = trend_worksheet.worksteps[trend_content['Workstep ID']]
        display_item_ids = sorted(workstep.display_items['ID'].to_list())
        expected_item_ids = sorted(push_df[(push_df['Asset'] == f'Area {area}1') &
                                           (push_df['Type'] == 'CalculatedSignal')]['ID'].to_list())
        assert display_item_ids == expected_item_ids
        assert 'a cat' in trend_worksheet.journal.html

    _test_spy_assets_with_templates(test_name, model, _assert, 'Weeks')


def _test_spy_assets_with_templates(test_name, model, assert_func, date_range_condition_name):
    areas = 'ABC'
    metadata_df = spy.search({'Name': f'/Area [{areas}]_(Temperature|Optimizer)/'})
    metadata_df['Build Asset'] = metadata_df['Name'].str.extract(rf'(Area [{areas}])_.*') + '1'
    metadata_df['Build Path'] = test_name
    metadata_df['Name'] = metadata_df['Name'].str.extract(rf'Area [{areas}]_(.*)')

    workbooks = _load_test_templates(test_name)
    analysis_template = workbooks['My Analysis Template']
    topic_template = workbooks['My Topic Template']
    analysis_template.name = None
    topic_template.name = None

    build_df = spy.assets.build(model, metadata_df, errors='raise', workbooks=workbooks)

    if test_name == 'test_spy_assets_with_templates_with_worksheets':
        analysis_template.worksheets['A Trend']['Archived'] = True

    analysis_template.worksheets['An XY Plot']['Archived'] = True
    analysis_template.worksheets['A Treemap']['Archived'] = True
    analysis_template.worksheets['A Table']['Archived'] = True

    push_df = spy.push(metadata=build_df, workbook=test_name, datasource=test_name, include_workbook_inventory=True)

    search_df = spy.workbooks.search({'Name': test_name})
    pushed_workbooks = spy.workbooks.pull(search_df)

    pushed_analysis = [w for w in pushed_workbooks if isinstance(w, Analysis)][0]
    pushed_topic = [w for w in pushed_workbooks if isinstance(w, Topic)][0]
    for area in areas:
        pushed_document: Report = pushed_topic.documents[f'Dashboard for Area {area}1']
        assert_func(area, pushed_document, pushed_analysis, push_df)

        assert len(pushed_document.date_ranges) == 1
        date_range = list(pushed_document.date_ranges.values()).pop()
        assert date_range['Condition ID'] == push_df[(push_df['Asset'] == f'Area {area}1') &
                                                     (push_df['Name'] == date_range_condition_name)]['ID'].squeeze()
        assert len(pushed_document.asset_selections) == 1
        asset_selection = list(pushed_document.asset_selections.values()).pop()
        assert asset_selection['Asset ID'] == push_df[(push_df['Asset'] == f'Area {area}1') &
                                                      (push_df['Type'] == 'Asset')]['ID'].squeeze()


class SpyAssetsTemplateTest_Mustache_Leaf(SpyAssetsTemplateTest):

    @Asset.Display()
    def Trend(self, metadata, analysis):
        trend_template: AnalysisWorksheetTemplate = analysis.worksheets['A Trend'].current_workstep()
        workstep = trend_template.copy(self.fqn)
        workstep.parameters = {
            "4B40EAFC-91ED-4AB0-8199-F21AF40A8350 [Asset] Example >> Area A": None,
            "B9CDE282-7A1A-4E28-A173-12E7347AB891 [Signal] Example >> Cooling Tower 1 >> Area A >> Relative Humidity":
                self.Optimizer(),
            "8A4F0E26-8A0C-4127-9E11-B67E031C6049 [Signal] Example >> Cooling Tower 1 >> Area A >> Temperature":
                self.Temperature()
        }
        return workstep

    @Asset.Attribute()
    def Sinusoid(self, metadata):
        return {
            'Type': 'Signal',
            'Formula': 'sinusoid()'
        }

    @Asset.Display()
    def XYPlot(self, metadata, analysis):
        trend_template: AnalysisWorksheetTemplate = analysis.worksheets['An XY Plot'].current_workstep()
        workstep = trend_template.copy(self.fqn)
        workstep.parameters = {
            "E97BBE1D-F227-4AD7-ADAC-15B54FC50DB3 [Signal] Example >> Cooling Tower 1 >> Area A >> Compressor Power":
                self.Sinusoid(),
            "8A4F0E26-8A0C-4127-9E11-B67E031C6049 [Signal] Example >> Cooling Tower 1 >> Area A >> Temperature":
                self.Temperature(),
            "4B40EAFC-91ED-4AB0-8199-F21AF40A8350 [Asset] Example >> Area A": None
        }
        return workstep


class SpyAssetsTemplateTest_Mustache_Rollup(Asset):
    @Asset.Document()
    def Topic_Via_Worksheet(self, metadata, topic):
        doc_template = topic.documents['My Mustache']
        document = doc_template.copy(self.fqn)
        document.name = f'Mustache for {self["Name"]}'
        document.parameters = {
            "date": 'June 21, 2022',
            "place": 'White Salmon, WA',
            "areas": [
                {
                    "area": asset['Name'],
                    "code block": f'This here is {asset["Name"]}',
                    "Users >> mark-derbecker@seeq-com >> Workbook Template Tests >> My Analysis Template >> A Trend":
                        asset.Trend(),
                    "Users >> mark-derbecker@seeq-com >> Workbook Template Tests >> My Analysis Template >> An XY Plot":
                        asset.XYPlot()
                } for asset in sorted(self.all_assets(), key=lambda a: a['Name']) if self.is_parent_of(asset)
            ]
        }
        return document


@pytest.mark.system
def test_spy_assets_with_templates_with_mustache():
    test_name = 'test_spy_assets_with_templates_with_mustache'
    model = [SpyAssetsTemplateTest_Mustache_Leaf, SpyAssetsTemplateTest_Mustache_Rollup]

    areas = 'ABC'
    root = 'Mustachioed Tree'
    metadata_df = spy.search({'Name': f'/Area [{areas}]_(Temperature|Optimizer)/'})
    metadata_df['Build Asset'] = metadata_df['Name'].str.extract(rf'(Area [{areas}])_.*') + '1'
    metadata_df['Build Path'] = root
    metadata_df['Build Template'] = 'SpyAssetsTemplateTest_Mustache_Leaf'
    metadata_df['Name'] = metadata_df['Name'].str.extract(rf'Area [{areas}]_(.*)')

    metadata_df = pd.concat([metadata_df, pd.DataFrame([{
        'Build Asset': root,
        'Build Template': 'SpyAssetsTemplateTest_Mustache_Rollup',
        'Type': 'Asset'
    }])])

    workbooks = _load_test_templates(test_name)
    analysis_template = workbooks['My Analysis Template']
    topic_template = workbooks['My Topic Template']
    analysis_template.name = None
    topic_template.name = None

    build_df = spy.assets.build(model, metadata_df, errors='raise', workbooks=workbooks)

    analysis_template.worksheets['A Treemap']['Archived'] = True
    analysis_template.worksheets['A Table']['Archived'] = True

    spy.push(metadata=build_df, workbook=test_name, datasource=test_name, include_workbook_inventory=True)

    search_df = spy.workbooks.search({'Name': test_name})
    pulled_workbooks = spy.workbooks.pull(search_df)

    pulled_topic = [w for w in pulled_workbooks if isinstance(w, Topic)][0]
    pulled_document: Report = pulled_topic.documents[f'Mustache for Mustachioed Tree']
    content_ids = re.findall(r'data-seeq-content="([^"]+)"', pulled_document.html)

    def _get_workstep(_content):
        # noinspection PyTypeChecker
        _trend_workbook: Analysis = pulled_workbooks[_content['Workbook ID']]
        _trend_worksheet: AnalysisWorksheet = _trend_workbook.worksheets[_content['Worksheet ID']]
        _trend_workstep = _trend_worksheet.worksteps[_content['Workstep ID']]
        return _trend_workstep

    content_ids.reverse()
    expected_areas = list(areas)
    expected_areas.reverse()
    while len(content_ids) > 0:
        trend_content_id = content_ids.pop()
        xyplot_content_id = content_ids.pop()
        expected_area = expected_areas.pop()

        trend_workstep = _get_workstep(pulled_document.content[trend_content_id])
        xyplot_workstep = _get_workstep(pulled_document.content[xyplot_content_id])

        workstep_items_df = spy.search(pd.concat([trend_workstep.display_items, xyplot_workstep.display_items]),
                                       all_properties=True)
        workstep_paths = workstep_items_df['Path'].to_list()
        workstep_assets = workstep_items_df['Asset'].to_list()
        workstep_names = workstep_items_df['Name'].to_list()

        assert all(path == root for path in workstep_paths)
        assert all(path == f'Area {expected_area}1' for path in workstep_assets)
        assert sorted(workstep_names) == ['Optimizer', 'Sinusoid', 'Temperature']


@pytest.mark.system
def test_workbook_copy():
    workbook_a = Analysis()
    workbook_b = Topic()
    package = WorkbookList()
    template_a_a_1 = AnalysisTemplate('A', workbook_a, package=package)
    template_b_a_1 = TopicTemplate('A', workbook_b, package=package)
    package.extend([template_a_a_1, template_b_a_1])

    template_a_a_2 = template_a_a_1.copy('A')
    assert template_a_a_1 is template_a_a_2

    template_b_a_2 = template_b_a_1.copy('A')
    assert template_b_a_1 is template_b_a_2

    template_a_b_1 = template_a_a_1.copy('B')
    assert template_a_a_1 is not template_a_b_1


@pytest.mark.system
def test_push_to_specific_workbook_id():
    #
    # This use case is for when the user has pushed a bunch of signals to a specific workbook and then later wants
    # to push new worksheets from a template to that specific workbook.
    #

    test_name = 'test_push_to_specific_workbook_id ' + _common.new_placeholder_guid()

    metadata_df = pd.DataFrame([{
        'Name': 'Sawtooth 1',
        'Type': 'Signal',
        'Formula': 'Sawtooth(1h)'
    }, {
        'Name': 'Sawtooth 2',
        'Type': 'Signal',
        'Formula': 'Sawtooth(2h)'
    }, {
        'Name': 'Sawtooth 3',
        'Type': 'Signal',
        'Formula': 'Sawtooth(3h)'
    }])

    workbooks = _load_doc_templates(test_name)
    workbook = workbooks['Analysis Template']

    # First push all the signals to a named workbook
    push_df = spy.push(metadata=metadata_df, workbook=test_name, worksheet=None, datasource=test_name)

    workbook_id = push_df.spy.workbook_id

    # Now we will push a template to that specific workbook
    workbook['ID'] = workbook_id
    workbook['Name'] = 'Super Cool'
    workbook.parameters = {
        "9564A6B8-8A8F-4F6D-AC63-00EA38962B7A [Signal] Area A_Temperature":
            push_df[push_df['Name'] == 'Sawtooth 1'],
        "DCED9C36-A4BE-4783-9216-DC06B3F57D8C [Signal] Area A_Compressor Power": push_df[
            push_df['Name'] == 'Sawtooth 2'],
        "D2C089B6-CE85-46FC-8392-E11CC0C08336 [Signal] Area A_Compressor Stage": push_df[
            push_df['Name'] == 'Sawtooth 3'],
        "Temperature Journal Link Text": 'Sawtooth 1',
        "Compressor Power Journal Link Text": 'Sawtooth 2',
        "Compressor Stage Journal Link Text": 'Sawtooth 3',
        "favorite color": 'blue'
    }

    # Clear the label (on the workbook only, not the worksheets) so it pushes to a specific ID
    workbook.label = None

    push_workbooks_df = spy.workbooks.push(workbook, datasource=test_name)
    pushed_workbook_id = push_workbooks_df.iloc[0]['Pushed Workbook ID']

    assert pushed_workbook_id == workbook_id

    pushed_workbooks = spy.workbooks.pull(spy.workbooks.search({'ID': pushed_workbook_id}), include_archived=False)
    assert len(pushed_workbooks) == 1
    pushed_workbook = pushed_workbooks['Super Cool']
    assert len(pushed_workbook.worksheets) == 4
    pushed_worksheet = pushed_workbook.worksheets['Trend Template']
    expected_sawtooth_ids = sorted(push_df['ID'].to_list())
    actual_sawtooth_ids = sorted(pushed_worksheet.display_items['ID'].to_list())
    assert expected_sawtooth_ids == actual_sawtooth_ids


@pytest.mark.system
def test_push_with_overridden_workstep():
    test_name = 'test_push_with_overridden_workstep'
    workbooks = _load_doc_templates(test_name)
    workbook = workbooks['Analysis Template']
    workbook.name = test_name
    trend_template_worksheet = workbook.worksheets['Trend Template']
    worksheet1: AnalysisWorksheetTemplate = trend_template_worksheet.copy(test_name + '1')
    worksheet2: AnalysisWorksheetTemplate = trend_template_worksheet.copy(test_name + '2')

    worksheet1.name = test_name + '1'
    worksheet2.name = test_name + '2'

    area_g_items = spy.search({'Name': 'Area G_*'})
    worksheet1.display_items = area_g_items

    area_h_items = spy.search({'Name': 'Area H_*'})
    worksheet2.display_items = area_h_items

    del workbook.worksheets['Trend Template']
    del workbook.worksheets['XY Plot Template']
    del workbook.worksheets['Treemap Template']
    del workbook.worksheets['Table Template']

    push_df = spy.workbooks.push(workbook, datasource=test_name)

    pulled_workbooks = spy.workbooks.pull(push_df.iloc[0]['URL'])
    # noinspection PyTypeChecker
    pulled_workbook: Analysis = pulled_workbooks[test_name]

    pulled_worksheet1: AnalysisWorksheet = pulled_workbook.worksheets[test_name + '1']
    pulled_worksheet2: AnalysisWorksheet = pulled_workbook.worksheets[test_name + '2']

    expected_area_g_ids = sorted(area_g_items['ID'].to_list())
    expected_area_h_ids = sorted(area_h_items['ID'].to_list())

    actual_area_g_ids = sorted(pulled_worksheet1.display_items['ID'].to_list())
    actual_area_h_ids = sorted(pulled_worksheet2.display_items['ID'].to_list())

    assert actual_area_g_ids == expected_area_g_ids
    assert actual_area_h_ids == expected_area_h_ids


@pytest.mark.system
def test_data_lab_visualization():
    test_name = 'test_data_lab_visualization'
    workbooks = _load_doc_templates(test_name)
    topic_template = workbooks['Data Lab Visualization Example']
    topic_document_template = topic_template.worksheets['Visualization Template']
    assert '[Image] My Visualization 1' in topic_document_template.code
    assert '[AltText] My Visualization 1' in topic_document_template.code

    pull_df = spy.pull(spy.search({'Name': 'Area B_Relative Humidity'}))
    area_b_rh = pull_df['Area B_Relative Humidity']

    fig, ax = plt.subplots()
    ax.hist(area_b_rh, linewidth=0.5, edgecolor="white")

    with pytest.raises(SPyValueError, match=r'Template parameter value for "\[Image\] My Visualization 1" is missing'):
        spy.workbooks.push(topic_template, datasource=test_name)

    topic_document_template.parameters = {
        "[Image] My Visualization 1": 1
    }

    with pytest.raises(SPyTypeError,
                       match=r'Template parameter value for "\[Image\] My Visualization 1" must be a string '
                             r'\(the filename of the image\)'):
        spy.workbooks.push(topic_template, datasource=test_name)

    topic_document_template.parameters = {
        "[Image] My Visualization 1": 'z'
    }

    with pytest.raises(SPyValueError, match=r'Image file "z" does not exist \(for template parameter '
                                            r'"\[Image\] My Visualization 1"\)'):
        spy.workbooks.push(topic_template, datasource=test_name)

    with tempfile.TemporaryDirectory() as tempdir:
        file_name = os.path.join(tempdir, f'{test_name}.png')
        plt.savefig(file_name)

        topic_document_template.parameters = {
            "[Image] My Visualization 1": file_name,
            "[AltText] My Visualization 1": 'A Histogram for the Ages'
        }

        push_df = spy.workbooks.push(topic_template, datasource=test_name)

        topics = spy.workbooks.pull(push_df.drop(columns=['ID']).rename(columns={'Pushed Workbook ID': 'ID'}))

        topic = topics[0]
        topic_doc = topic.documents[0]

        match = re.search(r'src="/api(/annotations/[^"]+)"', topic_doc.html)

        api_client_url = spy.session.get_api_url()
        request_url = api_client_url + match.group(1)
        response = requests.get(request_url, headers={
            "Accept": "application/vnd.seeq.v1+json",
            "x-sq-auth": spy.session.client.auth_token
        }, verify=Configuration().verify_ssl)

        with util.safe_open(file_name, 'rb') as f:
            expected_content = f.read()

        assert response.content == expected_content


@pytest.mark.system
def test_scale_across_assets_using_asset_selector():
    test_name = 'template.test_scale_across_assets_using_asset_selector'
    doc_workbooks = _load_doc_templates(None)

    workbooks_to_push = [
        doc_workbooks['Asset Selector Analysis Template'],
        doc_workbooks['Asset Selector Topic Template']
    ]

    # Push as normal workbooks first
    pushed_workbooks_df = spy.workbooks.push(workbooks_to_push, include_inventory=True, datasource=test_name)
    workbooks_to_pull_df = pushed_workbooks_df[pushed_workbooks_df['Workbook Type'] == 'Topic'].copy()
    workbooks_to_pull_df.drop(columns=['ID'], inplace=True)
    workbooks_to_pull_df.rename(columns={'Pushed Workbook ID': 'ID'}, inplace=True)
    templates = spy.workbooks.pull(workbooks_to_pull_df, as_template_with_label=test_name)

    topic_template = templates['Asset Selector Topic Template']
    doc_template = topic_template.documents[0]
    assert doc_template.name == 'Asset: {{Asset Name}}'
    doc_template.parameters = {'Asset Name': None}
    assert doc_template.name == 'Asset: {{Asset Name}}'
    doc_template.parameters = {'Asset Name': 'Bear'}
    assert doc_template.name == 'Asset: Bear'
    doc_template.parameters = None
    assert doc_template.name == 'Asset: {{Asset Name}}'

    areas = spy.search({'Path': 'Example >> Cooling Tower 1', 'Type': 'Asset'}, recursive=False)

    hour = 2
    minute = 17
    for _, area in areas.iterrows():
        area_doc = doc_template.copy(area['ID'])
        area_doc.parameters = {
            "0EF59E85-EC04-FB80-B010-7265E1EF939F [Asset Selection] Example >> Cooling Tower 1 >> Area A": area,

            # Leave these as None, because the Asset Selection is going to be used to scale across assets
            "0EF59EDA-A5CE-62B0-994B-E57B2D08BB84 [Embedded Content] My Folder >> Asset Selector Analysis Template >> Trend": None,
            "0EF59EDA-A5A2-7390-9460-3BC789E197FA [Embedded Content] My Folder >> Asset Selector Analysis Template >> XY Plot": None,

            "Asset Name": area['Name']
        }
        area_doc.schedule['Cron Schedule'] = [f'0 {minute} {hour} ? * 1,2,3,4,5,6,7']
        minute += 10
        if minute >= 60:
            minute -= 60
            hour += 1

    topic_template.documents.remove(doc_template)

    topic_template.name = test_name
    pushed_workbooks_df = spy.workbooks.push(topic_template, datasource=test_name, assume_dependencies_exist=True)
    workbooks_to_pull_df = pushed_workbooks_df[pushed_workbooks_df['Workbook Type'] == 'Topic'].copy()
    workbooks_to_pull_df.drop(columns=['ID'], inplace=True)
    workbooks_to_pull_df.rename(columns={'Pushed Workbook ID': 'ID'}, inplace=True)
    pushed_workbooks = spy.workbooks.pull(workbooks_to_pull_df)

    pushed_topic = pushed_workbooks[test_name]
    assert len(pushed_topic.documents) == len(areas)
    expected_doc_names = {f'Asset: {a}' for a in areas['Name'].to_list()}
    actual_doc_names = {d.name for d in pushed_topic.documents}
    assert actual_doc_names == expected_doc_names
    actual_schedules = {d.schedule['Cron Schedule'][0] for d in pushed_topic.documents}
    assert len(actual_schedules) == len(areas)
    actual_asset_selections = [next(iter(d.asset_selections.values())) for d in pushed_topic.documents]
    actual_asset_selection_asset_ids = {a['Asset ID'] for a in actual_asset_selections}
    expected_asset_selection_asset_ids = set(areas['ID'].to_list())
    assert actual_asset_selection_asset_ids == expected_asset_selection_asset_ids
