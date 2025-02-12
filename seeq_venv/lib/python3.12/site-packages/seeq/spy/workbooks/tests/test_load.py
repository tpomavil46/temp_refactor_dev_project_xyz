import os

import pytest

from seeq import spy
from seeq.base import util
from seeq.spy.tests import test_common


def get_example_export_path():
    return os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..', 'docs', 'Documentation',
                                         'Support Files', 'Example Export.zip'))


def get_workbook_template_tests_path():
    return os.path.normpath(os.path.join(os.path.dirname(__file__), 'Workbook Template Tests.zip'))


def get_report_and_dashboard_templates_path():
    return os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..', 'docs', 'Documentation',
                                         'Support Files', 'Report and Dashboard Templates.zip'))


def get_workbook_templates_path():
    return os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..', 'docs', 'Documentation',
                                         'Support Files', 'Workbook Templates.zip'))


def load_example_export():
    return spy.workbooks.load(get_example_export_path())


@pytest.mark.system
def test_load_folder():
    workbooks = load_example_export()
    assert len(workbooks) == 2


@pytest.mark.system
def test_load_zipfile():
    workbooks = spy.workbooks.load(get_example_export_path())
    assert len(workbooks) == 2


@pytest.mark.system
def test_load_special_chars():
    dir_name = '[Analysis] Name with special chars (0EF50398-4E47-FF60-8926-E25EE509EF23)'
    scenarios_folder = test_common.unzip_to_temp(os.path.join(os.path.dirname(__file__), 'Scenarios.zip'))
    try:
        workbook_folder = os.path.join(scenarios_folder, dir_name)
        analysis_name = '[Analysis] Name with *special* chars?'
        worksheet_name = '[Worksheet] Name with *special* chars?'
        workbooks = spy.workbooks.load(workbook_folder)
        assert len(workbooks) == 1
        assert workbooks[0].name == analysis_name
        assert len(workbooks[0].worksheets) == 1
        assert workbooks[0].worksheets[0].name == worksheet_name
        assert len(workbooks[0].worksheets[0].worksteps) > 0
        assert len(workbooks[0].worksheets[0].display_items) == 1
    finally:
        util.safe_rmtree(scenarios_folder)
