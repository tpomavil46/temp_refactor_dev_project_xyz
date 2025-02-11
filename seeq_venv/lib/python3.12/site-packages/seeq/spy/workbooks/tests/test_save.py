import os
import tempfile

import pytest

from seeq import spy
from seeq.base import util
from seeq.sdk import *
from seeq.spy import SPyRuntimeError
from seeq.spy.tests import test_common
from seeq.spy.workbooks.tests import test_load


def setup_module():
    test_common.initialize_sessions()


@pytest.mark.system
def test_save_folder():
    workbooks = test_load.load_example_export()
    folder = os.path.join(tempfile.gettempdir(), 'Output', 'My Save Folder')
    try:
        spy.workbooks.save(workbooks, folder)
        assert util.safe_exists(os.path.join(folder, 'Example Analysis (D833DC83-9A38-48DE-BF45-EB787E9E8375)'))

        with pytest.raises(SPyRuntimeError, match='folder exists'):
            spy.workbooks.save(workbooks, folder)

        file_to_be_deleted = os.path.join(
            folder, 'Example Analysis (D833DC83-9A38-48DE-BF45-EB787E9E8375)', 'to be deleted.txt')
        with util.safe_open(file_to_be_deleted, 'w') as f:
            f.write('I am a file to be removed')

        assert util.safe_exists(file_to_be_deleted)
        spy.workbooks.save(workbooks, folder, overwrite=True)
        assert not util.safe_exists(file_to_be_deleted)
    finally:
        if util.safe_exists(folder):
            util.safe_rmtree(folder)


@pytest.mark.system
def test_save_zipfile():
    workbooks = spy.workbooks.load(test_load.get_example_export_path())
    zip_file = os.path.join(tempfile.gettempdir(), 'Output', 'My Save File.zip')
    try:
        spy.workbooks.save(workbooks, zip_file)
        assert util.safe_exists(zip_file)

        with pytest.raises(SPyRuntimeError, match='already exists'):
            spy.workbooks.save(workbooks, zip_file)

        spy.workbooks.save(workbooks, zip_file, overwrite=True)
    finally:
        if util.safe_exists(zip_file):
            util.safe_remove(zip_file)


@pytest.mark.system
def test_bad_filename():
    workbooks = test_load.load_example_export()
    assert len(workbooks) == 2

    workbook = [w for w in workbooks if 'Analysis' in w.name][0]
    workbook.name = r'My\Workbook&Has|Bad\Characters*In?It:And"That<Sucks>Dude'
    with tempfile.TemporaryDirectory() as temp:
        if util.is_windows():
            with pytest.raises(OSError):
                # We just barf immediately if we are supplied a bad folder name
                spy.workbooks.save(workbook, os.path.join(temp, r'Bad|Folder*Name'))

        spy.workbooks.save(workbook, temp)

        assert util.safe_exists(os.path.join(temp, 'My_Workbook_Has_Bad_Characters_In_It_And_That_Sucks_Dude '
                                                   '(D833DC83-9A38-48DE-BF45-EB787E9E8375)'))


@pytest.mark.system
def test_none_datasource():
    scalars_api = ScalarsApi(spy.session.client)

    calculated_item_input = ScalarInputV1()
    calculated_item_input.name = 'My Scalar in the None Datasource'
    calculated_item_input.formula = '42'
    calculated_item_output = scalars_api.create_calculated_scalar(
        body=calculated_item_input)  # type: CalculatedItemOutputV1

    workbook = spy.workbooks.Analysis('test_none_datasource')
    worksheet = workbook.worksheet('The Only Worksheet')
    worksheet.display_items = spy.search({'ID': calculated_item_output.id})
    spy.workbooks.push(workbook)

    with tempfile.TemporaryDirectory() as temp:
        # This will barf if we don't handle the null (None) datasource correctly
        spy.workbooks.save(workbook, temp)
        spy.workbooks.load(temp)
