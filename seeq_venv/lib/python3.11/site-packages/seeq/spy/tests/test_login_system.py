import os
import socket
import ssl
import urllib
from unittest.mock import patch, Mock

import pytest

from seeq import spy
from seeq.sdk import UserOutputV1
from seeq.sdk.rest import ApiException
from seeq.spy import Session
from seeq.spy._config import Setting
from seeq.spy._errors import SPyValueError
from seeq.spy.tests import test_common
from seeq.spy.tests.test_common import Sessions


def setup_module():
    test_common.initialize_sessions()


def login_and_assert_consumption_tracking(expected_origin: str, expected_label: str, expected_url: str,
                                          request_origin_url=None, request_origin_label=None) -> None:
    session = Session()
    spy.login(username=test_common.SESSION_CREDENTIALS[Sessions.agent].username,
              password=test_common.SESSION_CREDENTIALS[Sessions.agent].password,
              session=session,
              request_origin_url=request_origin_url,
              request_origin_label=request_origin_label,
              force=True)
    assert session.client.default_headers['x-sq-origin'] == expected_origin
    assert session.client.default_headers['x-sq-origin-label'] == expected_label
    assert session.client.default_headers['x-sq-origin-url'] == expected_url


@pytest.mark.system
def test_default_session_bad_login():
    try:
        with pytest.raises(ValueError):
            spy.login('mark.derbecker@seeq.com', 'DataLab!', auth_token='Got any cheese?')

        assert spy.client is None
        assert spy.user is None

    finally:
        test_common.log_in_default_user()


# noinspection HttpUrlsUsage
@pytest.mark.system
def test_default_session_good_login():
    assert spy.client is not None
    assert spy.client.user_agent == f'Seeq-Python-SPy/{spy.__version__}/python'
    assert isinstance(spy.user, UserOutputV1)
    assert spy.user.username == 'agent_api_key'
    status = spy.Status()
    spy.session.request_origin_label = None

    # force=False will mean that we don't try to login since we're already logged in
    spy.login(username='blah', password='wrong', force=False, status=status)

    assert spy.client is not None
    assert isinstance(spy.user, UserOutputV1)
    assert spy.user.username == 'agent_api_key'
    assert len(status.warnings) == 1
    warnings = sorted(list(status.warnings))
    assert 'request_origin_label argument was not specified' in warnings[0]
    # It will be filled in with the name of the calling script
    assert spy.session.request_origin_label is not None

    auth_token = spy.client.auth_token
    spy.session.client = None
    spy.client = None

    # Data Lab uses this pattern, and so we have to support it. We use gethostname() here just to make sure that the
    # default of http://localhost:34216 is not being used.
    url = f'http://{socket.gethostname().lower()}:34216'
    spy._config.set_seeq_url(url)

    status = spy.Status()
    spy.login(auth_token=auth_token, request_origin_label='My Cool Script', status=status)
    assert spy.client is not None
    assert len(status.warnings) == 0
    assert spy.session.request_origin_label == 'My Cool Script'

    # If we login again we want to make sure the session was not invalidated and auth_token is still valid
    spy.login(auth_token=auth_token, request_origin_label='My Cooler Script', status=status)

    assert spy.client is not None
    assert isinstance(spy.user, UserOutputV1)
    assert spy.user.username == 'agent_api_key'
    assert spy.session.public_url == url

    # Make sure we can do a simple search
    df = spy.search({'Name': 'Area A_Temperature'}, workbook=spy.GLOBALS_ONLY)
    assert len(df) == 1

    spy.login(auth_token=auth_token)


# noinspection HttpUrlsUsage
@pytest.mark.system
def test_login_with_private_url():
    public_url = 'http://localhost:34216'
    private_url = f'http://{socket.gethostname().lower()}:34216'
    with pytest.raises(SPyValueError):
        spy.login(username=test_common.SESSION_CREDENTIALS[Sessions.agent].username,
                  password=test_common.SESSION_CREDENTIALS[Sessions.agent].password,
                  private_url=private_url)

    spy.login(username=test_common.SESSION_CREDENTIALS[Sessions.agent].username,
              password=test_common.SESSION_CREDENTIALS[Sessions.agent].password,
              url=public_url,
              private_url=private_url)
    assert spy.client is not None
    assert isinstance(spy.user, UserOutputV1)
    assert spy.user.username == 'agent_api_key'
    assert spy.session.public_url == public_url
    assert spy.session.private_url == private_url


@pytest.mark.system
def test_login_to_different_server_than_data_lab():
    try:
        # Data Lab sets these environment variables and they are used if the user does
        # not supply URLs in the login call. This test makes sure that if the user DOES
        # supply a URL, that the environment variables are ignored.
        os.environ[Setting.SEEQ_URL.get_env_name()] = 'bad:@#$bad-public-url'
        os.environ[Setting.PRIVATE_URL.get_env_name()] = 'bad:@#$bad-private-url'
        session = Session()
        spy.login(username=test_common.SESSION_CREDENTIALS[Sessions.agent].username,
                  password=test_common.SESSION_CREDENTIALS[Sessions.agent].password,
                  url=f'http://localhost:34216',
                  session=session)
    finally:
        del os.environ[Setting.SEEQ_URL.get_env_name()]
        del os.environ[Setting.PRIVATE_URL.get_env_name()]


@pytest.mark.system
def test_good_login_user_switch():
    # login and get the token
    auth_token = spy.client.auth_token
    assert spy.user.username == 'agent_api_key'

    # create the state where kernel has no spy.user attached yet
    spy.client = None
    spy.user = None

    # do the initial auth_token login
    spy.login(auth_token=auth_token)
    # noinspection PyUnresolvedReferences
    assert spy.user.username == 'agent_api_key'

    # change the user inside the notebook
    spy.login(username=test_common.SESSION_CREDENTIALS[Sessions.nonadmin].username,
              password=test_common.SESSION_CREDENTIALS[Sessions.nonadmin].password)
    # noinspection PyUnresolvedReferences
    assert spy.user.username == test_common.SESSION_CREDENTIALS[Sessions.nonadmin].username

    # login again as when re-opening the notebook
    spy.login(auth_token=auth_token)
    # noinspection PyUnresolvedReferences
    assert spy.user.username == 'agent_api_key'


@pytest.mark.system
def test_credentials_file_with_username():
    try:
        with pytest.raises(ValueError):
            spy.login('mark.derbecker@seeq.com', 'DataLab!', credentials_file='credentials.key')
    finally:
        test_common.log_in_default_user()


@pytest.mark.system
def test_errors_in_login_and_logout_rethrows_the_login_error():
    try:
        auth_token = spy.client.auth_token
        spy.client = None
        spy.user = None

        # Mock an unhandled API exception in the core of the login code and on logout.
        login_error = ApiException(status=500, reason='get_server_status is dead')
        with patch('seeq.sdk.SystemApi.get_server_status', new=Mock(side_effect=login_error)):
            logout_error = ApiException(status=500, reason='Logout is also dead')
            with patch('seeq.sdk.AuthApi.logout', new=Mock(side_effect=logout_error)):
                # The resulting error should be the login error, not the logout error.
                with pytest.raises(ApiException, match='get_server_status is dead'):
                    spy.login(auth_token=auth_token, force=True)
    finally:
        test_common.log_in_default_user()


@pytest.mark.system
def test_login_with_ignore_ssl_errors_clears_all_ssl_properties():
    # Default values for SSL properties (most localhost servers won't have SSL set up, but it should still be REQUIRED)
    assert spy.session.client_configuration.verify_ssl is True
    assert spy.session.client.rest_client.pool_manager.connection_pool_kw['cert_reqs'] == ssl.CERT_REQUIRED
    assert spy.session.client.rest_client.pool_manager.connection_pool_kw['cert_file'] is None
    assert spy.session.client.rest_client.pool_manager.connection_pool_kw['key_file'] is None

    try:
        # Logging in with ignore_ssl_errors=True should clear all SSL properties universally
        session = Session()
        spy.login(username=test_common.SESSION_CREDENTIALS[Sessions.agent].username,
                  password=test_common.SESSION_CREDENTIALS[Sessions.agent].password,
                  url=f'http://localhost:34216', session=session, ignore_ssl_errors=True)

        assert session.client_configuration.verify_ssl is False
        assert session.client.rest_client.pool_manager.connection_pool_kw['cert_reqs'] == ssl.CERT_NONE
        assert session.client.rest_client.pool_manager.connection_pool_kw['cert_file'] is None
        assert session.client.rest_client.pool_manager.connection_pool_kw['key_file'] is None
    finally:
        test_common.log_in_default_user()


@pytest.mark.system
def test_correct_origin_label_and_url_headers_in_data_lab():
    try:
        os.environ['SEEQ_SDL_CONTAINER_IS_DATALAB'] = 'true'
        os.environ['SEEQ_DATALAB_API'] = 'false'
        os.environ['SEEQ_SDL_CONTAINER_IS_EXECUTOR'] = 'false'
        project_name = 'Project Name'
        project_id = os.environ['SEEQ_PROJECT_UUID'] = 'totally-real-id'
        file_path = 'path/to/notebook in question'
        origin = 'Data Lab (Interactive)'

        with patch('seeq.spy._datalab.get_data_lab_project_name') as name_mock:
            name_mock.return_value = project_name
            with patch('seeq.spy._datalab.get_notebook_url') as url_mock:
                url_mock.return_value = f"https://seeq-labs.com/data-lab/{project_id}/notebooks/{file_path}"
                login_and_assert_consumption_tracking(
                    expected_origin=origin,
                    expected_label=f'{project_name} - {file_path}',
                    expected_url=f'/data-lab/{project_id}/notebooks/{urllib.parse.quote(file_path)}')

            # Test for a user created folder named 'notebooks'
            file_path = f'notebooks/{file_path}'
            with patch('seeq.spy._datalab.get_notebook_url') as url_mock:
                url_mock.return_value = f"https://seeq-labs.com/data-lab/{project_id}/notebooks/{file_path}"
                login_and_assert_consumption_tracking(origin, f'{project_name} - {file_path}',
                                                      f'/data-lab/{project_id}/notebooks/{urllib.parse.quote(file_path)}')
                login_and_assert_consumption_tracking(
                    expected_origin=origin,
                    expected_label=f'{project_name} - {file_path}',
                    expected_url=f'/data-lab/{project_id}/notebooks/{urllib.parse.quote(file_path)}')

    finally:
        del os.environ['SEEQ_SDL_CONTAINER_IS_DATALAB']
        del os.environ['SEEQ_DATALAB_API']
        del os.environ['SEEQ_SDL_CONTAINER_IS_EXECUTOR']
        del os.environ['SEEQ_PROJECT_UUID']


@pytest.mark.system
def test_correct_origin_label_and_url_headers_in_data_lab_non_latin_chars():
    try:
        os.environ['SEEQ_SDL_CONTAINER_IS_DATALAB'] = 'true'
        os.environ['SEEQ_DATALAB_API'] = 'false'
        os.environ['SEEQ_SDL_CONTAINER_IS_EXECUTOR'] = 'false'
        project_id = os.environ['SEEQ_PROJECT_UUID'] = 'totally-real-id'
        project_name = 'Project 言葉 сүз ꯋꯥꯍꯩ 單詞 ʻōlelo 단어 😱'
        project_name_encoded = urllib.parse.quote('Project 言葉 сүз ꯋꯥꯍꯩ 單詞 ʻōlelo 단어 😱')
        file_path = 'path/to/notebook 言葉 сүз ꯋꯥꯍꯩ 單詞 ʻōlelo 단어 😱.ipynb'
        file_path_encoded = urllib.parse.quote(file_path)
        origin = 'Data Lab (Interactive)'

        with patch('seeq.spy._datalab.get_data_lab_project_name') as name_mock:
            name_mock.return_value = project_name
            with patch('seeq.spy._datalab.get_notebook_url') as url_mock:
                url_mock.return_value = f"https://seeq-labs.com/data-lab/{project_id}/notebooks/{file_path}"
                login_and_assert_consumption_tracking(
                    expected_origin=origin,
                    expected_label=f'{project_name_encoded} - {file_path_encoded}',
                    expected_url=f'/data-lab/{project_id}/notebooks/{urllib.parse.quote(file_path)}')

    finally:
        del os.environ['SEEQ_SDL_CONTAINER_IS_DATALAB']
        del os.environ['SEEQ_DATALAB_API']
        del os.environ['SEEQ_SDL_CONTAINER_IS_EXECUTOR']
        del os.environ['SEEQ_PROJECT_UUID']


@pytest.mark.system
def test_manually_setting_origin_label_and_url_headers():
    try:
        os.environ['SEEQ_SDL_CONTAINER_IS_DATALAB'] = 'false'
        os.environ['SEEQ_DATALAB_API'] = 'false'
        os.environ['SEEQ_SDL_CONTAINER_IS_EXECUTOR'] = 'false'
        url = '/totally/a/real/url'
        origin = 'SPy (Standalone)'
        label = 'custom label'

        login_and_assert_consumption_tracking(expected_origin=origin, expected_label=label, expected_url=url,
                                              request_origin_url=url, request_origin_label=label)
    finally:
        del os.environ['SEEQ_SDL_CONTAINER_IS_DATALAB']
        del os.environ['SEEQ_DATALAB_API']
        del os.environ['SEEQ_SDL_CONTAINER_IS_EXECUTOR']


@pytest.mark.system
def test_correct_origin_label_and_url_headers_in_scheduled_notebooks():
    try:
        os.environ['SEEQ_SDL_CONTAINER_IS_EXECUTOR'] = 'true'
        project_id = os.environ['SEEQ_PROJECT_UUID'] = 'totally-real-id'
        project_name = os.environ['SEEQ_PROJECT_NAME'] = 'Project Name'
        file_path = os.environ['SEEQ_SDL_FILE_PATH'] = 'path/to/notebook in question'

        login_and_assert_consumption_tracking(
            expected_origin='Data Lab (Job)',
            expected_label=f'[Scheduled] {project_name} - {file_path}',
            expected_url=f'/data-lab/{project_id}/notebooks/{urllib.parse.quote(file_path)}')

    finally:
        del os.environ['SEEQ_SDL_CONTAINER_IS_EXECUTOR']
        del os.environ['SEEQ_PROJECT_UUID']
        del os.environ['SEEQ_PROJECT_NAME']
        del os.environ['SEEQ_SDL_FILE_PATH']


@pytest.mark.system
def test_correct_origin_label_and_url_headers_in_data_lab_api():
    try:
        os.environ['SEEQ_SDL_CONTAINER_IS_DATALAB'] = 'true'
        os.environ['SEEQ_DATALAB_API'] = 'true'
        project_name = 'Project Name'
        project_id = os.environ['SEEQ_PROJECT_UUID'] = 'totally-real-id'
        file_path = 'path/to/notebook in question'
        notebook_url = f"https://seeq-labs.com/data-lab/{project_id}/notebooks/{file_path}"

        login_and_assert_consumption_tracking(expected_origin='Add-on',
                                              expected_label=f'{project_name} - {file_path}',
                                              expected_url=notebook_url, request_origin_url=notebook_url,
                                              request_origin_label=f'{project_name} - {file_path}')

    finally:
        del os.environ['SEEQ_SDL_CONTAINER_IS_DATALAB']
        del os.environ['SEEQ_DATALAB_API']
        del os.environ['SEEQ_PROJECT_UUID']


@pytest.mark.system
def test_correct_origin_label_and_url_headers_in_data_lab_functions():
    try:
        os.environ['SEEQ_SDL_CONTAINER_IS_DATALAB'] = 'true'
        os.environ['SEEQ_DATALAB_API'] = 'true'
        os.environ['SEEQ_PROJECT_TYPE'] = 'DATA_LAB_FUNCTIONS'
        project_name = 'Project Name'
        project_id = os.environ['SEEQ_PROJECT_UUID'] = 'totally-real-id'
        file_path = 'path/to/notebook in question'
        notebook_url = f"https://seeq-labs.com/data-lab/{project_id}/notebooks/{file_path}"

        login_and_assert_consumption_tracking(expected_origin='Add-on',
                                              expected_label=f'[Data Lab Functions] {project_name} - {file_path}',
                                              expected_url=notebook_url, request_origin_url=notebook_url,
                                              request_origin_label=f'{project_name} - {file_path}')

    finally:
        del os.environ['SEEQ_SDL_CONTAINER_IS_DATALAB']
        del os.environ['SEEQ_DATALAB_API']
        del os.environ['SEEQ_PROJECT_UUID']
        del os.environ['SEEQ_PROJECT_TYPE']
