import os
from unittest.mock import patch

import pytest
from IPython import InteractiveShell
from ipykernel.zmqshell import ZMQInteractiveShell

from seeq.spy import _datalab

# noinspection HttpUrlsUsage
SERVER_URL = 'http://seeq.com'
PROJECT_UUID = '12345678-9ABC-DEF0-1234-56789ABCDEF0'


def setup_environment_variables():
    os.environ['SEEQ_SERVER_URL'] = SERVER_URL
    os.environ['SEEQ_PROJECT_UUID'] = PROJECT_UUID


def setup_module():
    setup_environment_variables()


@pytest.mark.unit
def test_sdl_project_uuid():
    assert _datalab.get_data_lab_project_id() == PROJECT_UUID


@pytest.mark.unit
def test_sdl_project_url():
    expected_project_url = f'{SERVER_URL}/data-lab/{PROJECT_UUID}'
    assert _datalab.get_data_lab_project_url() == expected_project_url


@pytest.mark.unit
def test_sdl_project_uuid():
    assert _datalab.get_data_lab_project_id() == PROJECT_UUID


@pytest.mark.unit
def test_sdl_project_url():
    expected_project_url = f'{SERVER_URL}/data-lab/{PROJECT_UUID}'
    assert _datalab.get_data_lab_project_url() == expected_project_url


@pytest.mark.unit
def test_ipython():
    with patch('IPython.get_ipython') as mock:
        mock.return_value = None
        assert _datalab.is_ipython() is False

        mock.return_value = InteractiveShell()
        assert _datalab.is_ipython() is True

        mock.return_value = ZMQInteractiveShell()
        assert _datalab.is_ipython() is True


@pytest.mark.unit
def test_ipython_vs_rkernel_vs_jupyter():
    with patch('seeq.spy._datalab.is_rkernel') as r_mock:
        r_mock.return_value = False
        with patch('IPython.get_ipython') as mock:
            mock.return_value = None
            assert _datalab.is_ipython() is False
            assert _datalab.is_jupyter() is False

            mock.return_value = InteractiveShell()
            assert _datalab.is_ipython() is True
            assert _datalab.is_jupyter() is False

            mock.return_value = ZMQInteractiveShell()
            assert _datalab.is_ipython() is True
            assert _datalab.is_jupyter() is True

            mock.return_value = None
            r_mock.return_value = True
            assert _datalab.is_ipython() is False
            assert _datalab.is_jupyter() is True


@pytest.mark.unit
def test_is_datalab():
    os.environ['SEEQ_SDL_CONTAINER_IS_DATALAB'] = ''
    assert _datalab.is_datalab() is False

    os.environ['SEEQ_SDL_CONTAINER_IS_DATALAB'] = 'true'
    assert _datalab.is_datalab() is True


@pytest.mark.unit
def test_running_in_executor():
    os.environ['SEEQ_SDL_CONTAINER_IS_EXECUTOR'] = ''
    assert _datalab.is_executor() is False

    os.environ['SEEQ_SDL_CONTAINER_IS_EXECUTOR'] = 'true'
    assert _datalab.is_executor() is True


@pytest.mark.unit
def test_label_from_executor():
    assert _datalab.get_label_from_executor() == ''

    os.environ['SEEQ_SDL_LABEL'] = ''
    assert _datalab.get_label_from_executor() == ''

    os.environ['SEEQ_SDL_LABEL'] = 'explicit'
    assert _datalab.get_label_from_executor() == 'explicit'
