import pandas as pd
import pytest

from seeq.spy import _common
from seeq.spy._metadata_push_results import PushResults


@pytest.mark.unit
def test_push_result():
    push_results = PushResults(pd.DataFrame([
        {
            'Name': 'Name 0',
            'Description': 'Description 0'
        },
        {
            'Name': 'Name 1',
            'Description': 'Description 1',
            'Type': 'Type 1'
        }
    ]))

    assert push_results[0]['Name'] == 'Name 0'
    assert push_results[0]['Description'] == 'Description 0'
    assert not _common.present(push_results[0], 'Type')

    assert push_results[1]['Name'] == 'Name 1'
    assert push_results[1]['Description'] == 'Description 1'
    assert push_results[1]['Type'] == 'Type 1'

    assert push_results.at[0, 'Name'] == 'Name 0'
    assert pd.isna(push_results.at[0, 'Type'])

    assert push_results.loc[1] == {
        'Name': 'Name 1',
        'Description': 'Description 1',
        'Type': 'Type 1'
    }

    push_results.at[0, 'Type'] = 'Type 0'

    assert push_results.loc[0] == {
        'Name': 'Name 0',
        'Description': 'Description 0',
        'Type': 'Type 0'
    }


@pytest.mark.unit
def test_indexes():
    item_0 = {
        'Name': 'Name 0',
        'Type': 'Scalar',
        'Path': 'This >> Is >> Not >> Cool',
        'Data ID': '[WorkbookID0] {Scalar} This >> Is >> Not >> Cool'
    }
    item_1 = {
        'Name': 'Name 1',
        'Type': 'Asset',
        'Path': 'This >> Is >> Cool',
        'Data ID': 'RandomDataID'
    }
    push_results = PushResults(pd.DataFrame([item_0, item_1]))

    assert push_results.get_by_asset('Bogus Name', 'Bogus Path') is None
    assert push_results.get_by_asset('Name 0', 'This >> Is >> Not >> Cool') is None  # not an asset
    assert push_results.get_by_asset('Name 1', 'This >> Is >> Cool') == 1

    assert push_results.get_by_workbook_and_path('WorkbookID0', 'This >> Is >> Not >> Cool') == 0
    assert push_results.get_by_data_id('[WorkbookID0] {Scalar} This >> Is >> Not >> Cool') == 0
    assert push_results.get_by_workbook_and_path('WorkbookID1', 'This >> Is >> Cool') is None
    push_results.at[1, 'Data ID'] = '[WorkbookID1] {Asset} This >> Is >> Cool'
    assert push_results.get_by_workbook_and_path('WorkbookID1', 'This >> Is >> Cool') == 1
    assert push_results.get_by_data_id('[WorkbookID1] {Asset} This >> Is >> Cool') == 1
