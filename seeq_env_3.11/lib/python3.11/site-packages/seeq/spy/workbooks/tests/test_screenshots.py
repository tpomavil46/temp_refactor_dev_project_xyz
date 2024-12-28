from __future__ import annotations

import pytest

from seeq import spy
from seeq.spy import _common
from seeq.spy.tests import test_common


def setup_module():
    test_common.initialize_sessions()


@pytest.mark.ignore
@test_common.visual_diff
def test_example_visual_diff():
    workbook = spy.workbooks.Analysis(f'Workbook {_common.new_placeholder_guid()}')
    worksheet = workbook.worksheet('1')

    display_items = spy.search({'Datasource Name': 'Example Data', 'Name': 'Area A_*'}, order_by='Name')
    display_items['Color'] = [
        '#0000FF',
        '#00FF00',
        '#FF0000',
        '#FFFF00',
        '#FF00FF',
        '#00FFFF'
    ]
    display_items['Lane'] = range(1, 7)
    worksheet.timezone = 'UTC'
    worksheet.display_items = display_items

    worksheet.investigate_range = {'Start': '2020-01-01', 'End': '2020-01-02'}
    worksheet.date_range = {'Start': '2020-01-01', 'End': '2020-01-02'}

    workstep = worksheet.current_workstep()

    spy.workbooks.push([workbook])

    yield workstep
