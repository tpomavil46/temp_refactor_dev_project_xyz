from __future__ import annotations

import datetime
import functools
import inspect
import io
import os
import pathlib
import sys
import tarfile
import tempfile
import textwrap
import time
import types
import unittest
import warnings
import zipfile
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict, ClassVar, Callable, Union, Iterable

import docker
import numpy as np
import pandas as pd
import pytest
import urllib3
from PIL import Image, ImageChops
from _pytest.mark.expression import Expression

from seeq import spy
from seeq.base import util
from seeq.sdk import *
from seeq.spy import Session
from seeq.spy import _common, _metadata
from seeq.spy._errors import SPyException
from seeq.spy.workbooks import Analysis, AnalysisWorkstep

ADMIN_USER_NAME = 'admin@seeq.com'
ADMIN_PASSWORD = 'myadminpassword'

NON_ADMIN_NAME = 'non_admin'
NON_ADMIN_LAST_NAME = 'tester'
NON_ADMIN_USERNAME = f'{NON_ADMIN_NAME}.{NON_ADMIN_LAST_NAME}@seeq.com'
NON_ADMIN_PASSWORD = 'mynonadminpassword'


class Sessions(Enum):
    agent = 'agent'
    admin = 'admin'
    nonadmin = 'nonadmin'
    ren = 'ren'
    stimpy = 'stimpy'
    test_path_search_pagination = 'test_path_search_pagination'
    test_search_pagination = 'test_search_pagination'
    test_order_by_page_size = 'test_order_by_page_size'
    test_pull_signal_with_grid = 'test_pull_signal_with_grid'
    test_pull_condition_as_capsules = 'test_pull_condition_as_capsules'
    test_push_from_csv = 'test_push_from_csv'


@dataclass
class Credential:
    username: Optional[str] = None
    password: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None


SESSION_CREDENTIALS = {
    Sessions.agent: Credential(username='agent_api_key', password=None, first_name=None, last_name=None),
    Sessions.admin: Credential(username='admin@seeq.com', password='myadminpassword', first_name=None, last_name=None),
    Sessions.nonadmin: Credential(username='non_admin.tester@seeq.com', password='mynonadminpassword',
                                  first_name='non_admin', last_name='tester'),
    Sessions.ren: Credential(username='ren', password='ren12345',
                             first_name='Ren', last_name='Hoek'),
    Sessions.stimpy: Credential(username='stimpy', password='stimpy12',
                                first_name='Stimpson J.', last_name='Cat')
}

sessions: Optional[Dict[Sessions, Session]] = None

DOCKER_TIMEOUT_SECONDS = 1800
docker_client: Optional[docker.client.DockerClient] = None


def ensure_docker_is_running():
    global docker_client
    if docker_client is None:
        docker_client = docker.from_env(timeout=DOCKER_TIMEOUT_SECONDS)

    docker_client.ping()


def running_in_dev_compose():
    # noinspection PyBroadException
    try:
        ensure_docker_is_running()
    except Exception:
        return False
    return any(container['Image'] == 'appserver' for container in docker_client.api.containers())


def get_appserver_server_container():
    for container in docker_client.api.containers():
        if container['Image'] == 'appserver':
            return docker_client.containers.get(container['Id'])
    return None


def get_agent_key_path():
    if running_in_dev_compose():
        return os.path.join(get_dev_compose_folder(), 'keys', 'agent.key')
    else:
        return os.path.join(get_test_data_folder(), 'keys', 'agent.key')


def get_session(session_name):
    global sessions
    if sessions is None:
        add_all_credentials()
        util.os_lock('seeq_spy_system_tests', _initialize_seeq_database, timeout=60)
        log_in_all_test_users()
        set_retry_timeout_for_all_sessions(600)  # Set high to mitigate CRAB-28592
        wait_for_example_data(spy.session)

    return sessions[session_name]


def set_retry_timeout_for_all_sessions(timeout):
    # This will cause the value to flow down into child kernels spawned by test_run_notebooks().
    Session.set_global_sdk_retry_timeout_in_seconds(timeout)

    for session in sessions.values():
        session.options.retry_timeout_in_seconds = timeout


def log_in_default_user(url=None):
    agent_credential = SESSION_CREDENTIALS[Sessions.agent]

    # this will be the default agent_api_key user which is a non-auto complete identity
    with util.safe_open(get_agent_key_path(), "r") as f:
        SESSION_CREDENTIALS[Sessions.agent].username, SESSION_CREDENTIALS[Sessions.agent].password = \
            f.read().splitlines()

    spy.login(agent_credential.username, agent_credential.password, url=url, session=spy.session)


def log_out_default_user():
    spy.logout(session=spy.session)


def _initialize_seeq_database():
    global sessions
    sessions = dict()

    log_in_default_user()
    sessions[Sessions.agent] = spy.session

    create_admin_user(spy.session)
    admin_session = Session()
    spy.login(SESSION_CREDENTIALS[Sessions.admin].username, SESSION_CREDENTIALS[Sessions.admin].password,
              session=admin_session)
    sessions[Sessions.admin] = admin_session

    # It is important to create the Seeq Data Lab datasource while we're inside the os_lock() call, otherwise there
    # is a race condition where it can be created twice which kills all the tests.
    _metadata.create_datasource(spy.session)

    add_all_test_users()


def add_all_credentials():
    global sessions
    for session_name in Sessions:
        if session_name not in SESSION_CREDENTIALS:
            SESSION_CREDENTIALS[session_name] = \
                Credential(username=session_name.value, password=f'{session_name.value}12345678',
                           first_name=session_name.value, last_name=session_name.value)


def add_all_test_users():
    global sessions
    for session_name in Sessions:
        if session_name in [Sessions.agent, Sessions.admin]:
            continue

        credential = SESSION_CREDENTIALS[session_name]

        add_normal_user(sessions[Sessions.admin],
                        credential.first_name, credential.last_name, credential.username, credential.password)

        user_session = Session()
        spy.login(credential.username, credential.password, session=user_session)
        sessions[session_name] = user_session


def log_in_all_test_users():
    global sessions
    for session_name in Sessions:
        if session_name in [Sessions.agent, Sessions.admin]:
            continue

        credential = SESSION_CREDENTIALS[session_name]

        user_session = Session()
        spy.login(credential.username, credential.password, session=user_session)
        sessions[session_name] = user_session


def initialize_sessions():
    """
    This function should be called in the setup_module() function for all system tests that require Seeq Server to be
    running.
    """
    check_if_server_is_running()
    get_session(Sessions.agent)


def check_if_server_is_running():
    try:
        system_api = SystemApi(ApiClient(host='http://localhost:34216/api'))
        system_api.get_server_status()
    except Exception as e:
        raise RuntimeError("Seeq Server is not responding. If you're running this from IntelliJ, make sure you "
                           "ran 'sq run -c' from the top-level crab folder first, and wait for Seeq Server to "
                           f"boot up fully. Here's the exception that system_api.get_server_status() returned:\n{e}")


def get_user(session: Session, username) -> Optional[UserOutputV1]:
    users_api = UsersApi(session.client)
    user_output_list = users_api.get_users(username_search=username)
    for user in user_output_list.users:  # type: UserOutputV1
        if user.username == username:
            return user

    return None


def get_group(session: Session, group_name) -> Optional[IdentityPreviewV1]:
    user_groups_api = UserGroupsApi(session.client)
    user_groups_output_list = user_groups_api.get_user_groups()
    for group in user_groups_output_list.items:  # type: IdentityPreviewV1
        if group.name == group_name:
            return group

    return None


def add_normal_user(session: Session, first_name, last_name, username, password) -> UserOutputV1:
    user = get_user(session, username)
    if user:
        return user

    users_api = UsersApi(session.client)
    return users_api.create_user(body=UserInputV1(
        first_name=first_name,
        last_name=last_name,
        email=username,
        username=username,
        password=password
    ))


def create_admin_user(session: Session):
    user = get_user(session, ADMIN_USER_NAME)
    if user:
        return user

    if running_in_dev_compose():
        admin_reset_properties = os.path.join(tempfile.tempdir, 'admin_reset.properties')
    else:
        admin_reset_properties = os.path.join(get_test_data_folder(), 'configuration', 'admin_reset.properties')
    with util.safe_open(admin_reset_properties, 'w') as f:
        f.write(textwrap.dedent(f"""
                    email = {ADMIN_USER_NAME}
                    password = {ADMIN_PASSWORD}
                """))

    if running_in_dev_compose():
        with io.BytesIO() as stream:
            with tarfile.open(fileobj=stream, mode='w|') as tar, open(admin_reset_properties, 'rb') as f:
                info = tar.gettarinfo(fileobj=f)
                info.name = 'admin_reset.properties'
                tar.addfile(info, f)
            get_appserver_server_container().put_archive('/seeq/data/configuration', stream.getvalue())

    timeout = time.time()
    while True:
        if time.time() - timeout > 30:
            raise Exception(f'Timed out creating admin user {ADMIN_USER_NAME}')

        if get_user(session, ADMIN_USER_NAME):
            break

        time.sleep(0.01)


def get_test_repo_root():
    return util.get_test_with_root_dir()


def get_test_data_folder():
    return os.path.normpath(os.path.join(get_test_repo_root(), 'sq-run-data-dir'))


def get_dev_compose_folder():
    return os.path.normpath(os.path.join(get_test_repo_root(), 'dev-compose'))


def wait_for(boolean_function):
    start = time.time()
    while True:
        if boolean_function():
            break

        if time.time() - start > 240:
            return False

        time.sleep(1.0)

    return True


def wait_for_example_data(session: Session):
    start = time.time()
    while True:
        if is_jvm_agent_connection_indexed(session, 'Example Data'):
            return

        if time.time() - start > 240:
            raise Exception("Timed out waiting for Example Data to finish indexing")

        time.sleep(1.0)


def is_jvm_agent_connection_indexed(session: Session, connection_name):
    # noinspection PyBroadException
    try:
        agents_api = AgentsApi(session.client)
        agent_status = agents_api.get_agent_status()
        for agents in agent_status:
            if 'JVM Agent' in agents.id:
                if agents.status != 'CONNECTED':
                    return False

                for connection in agents.connections:
                    if connection_name in connection.name and \
                            connection.status == 'CONNECTED' and \
                            connection.sync_status == 'SYNC_SUCCESS':
                        return True

    except Exception:
        return False

    return False


def create_worksheet_for_url_tests(name):
    search_results = spy.search({
        'Name': 'Temperature',
        'Path': 'Example >> Cooling Tower 1 >> Area A'
    }, workbook=spy.GLOBALS_ONLY)

    display_items = pd.DataFrame([{
        'Type': 'Signal',
        'Name': 'Temperature Minus 5',
        'Formula': '$a - 5',
        'Formula Parameters': {
            '$a': search_results.iloc[0]
        }
    }, {
        'Type': 'Condition',
        'Name': 'Cold',
        'Formula': '$a.validValues().valueSearch(isLessThan(80))',
        'Formula Parameters': {
            '$a': search_results.iloc[0]
        }
    }, {
        'Type': 'Scalar',
        'Name': 'Constant',
        'Formula': '5',
    }])

    push_df = spy.push(metadata=display_items, workbook=None)

    workbook = Analysis({
        'Name': name
    })

    worksheet = workbook.worksheet('search from URL')
    worksheet.display_range = {
        'Start': '2019-01-01T00:00Z',
        'End': '2019-01-02T00:00Z'
    }
    worksheet.display_items = push_df

    spy.workbooks.push(workbook)

    return workbook


@dataclass(frozen=True)
class VisualDiffTest:
    test_name: str
    worksheet_id: str
    workstep_id: str
    width: int = 700
    height: int = 394

    PIXEL_ERROR_THRESHOLD: ClassVar[int] = 75
    DIFF_OUTPUT_ORIGINAL_IMAGE_ALPHA: ClassVar[int] = 100
    DIFF_OUTPUT_HIGHLIGHT_ALPHA: ClassVar[int] = 180
    VISUAL_DIFF_FOLDER: ClassVar[str] = os.path.join(pathlib.Path(__file__).parent.resolve(), 'visual_diff')
    BASELINE_FOLDER: ClassVar[str] = f'{sys.platform}_baseline'
    CURRENT_FOLDER: ClassVar[str] = 'current'
    DIFF_FOLDER: ClassVar[str] = 'difference'
    FILE_FORMAT: ClassVar[str] = 'png'

    @property
    def file_name(self) -> str:
        return f'{self.test_name}.{VisualDiffTest.FILE_FORMAT}'

    def run(self):
        content_output = self.create_content()
        image_bytes = VisualDiffTest.get_image_bytes(content_output.id)
        with Image.open(io.BytesIO(image_bytes)) as im:
            self.save_image(im, VisualDiffTest.CURRENT_FOLDER)

        self.diff_images()

    def create_content(self) -> ContentOutputV1:
        content_api = ContentApi(spy.session.client)
        return content_api.create_content(body=ContentInputV1(
            name=f'Test content for worksheet {self.worksheet_id} and workstep {self.workstep_id}',
            worksheet_id=self.worksheet_id,
            workstep_id=self.workstep_id,
            width=self.width,
            height=self.height
        ))

    @staticmethod
    def get_image_bytes(content_id: str) -> bytes:
        content_api = ContentApi(spy.session.client)

        # noinspection PyTypeChecker
        # noinspection PyNoneFunctionAssignment
        response: urllib3.response.HTTPResponse = content_api.get_image(id=content_id, _preload_content=False)

        return response.data

    def save_image(self, image: Image, subdirectory: str):
        directory = os.path.join(VisualDiffTest.VISUAL_DIFF_FOLDER, subdirectory)
        util.safe_makedirs(directory, exist_ok=True)

        file_path = os.path.join(directory, self.file_name)
        image.save(file_path, VisualDiffTest.FILE_FORMAT)

    def diff_images(self):
        baseline_file_name = os.path.join(VisualDiffTest.VISUAL_DIFF_FOLDER, VisualDiffTest.BASELINE_FOLDER,
                                          self.file_name)
        actual_file_name = os.path.join(VisualDiffTest.VISUAL_DIFF_FOLDER, VisualDiffTest.CURRENT_FOLDER,
                                        self.file_name)
        if not os.path.isfile(baseline_file_name):
            raise AssertionError(f'Baseline image "{baseline_file_name}" does not exist.')
        with Image.open(baseline_file_name).convert('RGB') as im_expected, \
                Image.open(actual_file_name).convert('RGB') as im_actual:
            # calculate the diff
            diff = ImageChops.difference(im_actual, im_expected)

            # Convert the diff to an array and count the non-zero pixels
            # noinspection PyTypeChecker
            diff_array = np.array(diff)
            diff_pixels_mask = np.any(diff_array != [0, 0, 0], axis=-1)

            # If the number of pixels is above the threshold, save the diff and fail
            pixel_error_count = np.sum(diff_pixels_mask)
            if pixel_error_count >= VisualDiffTest.PIXEL_ERROR_THRESHOLD:
                # make actual image transparent
                im_actual.putalpha(VisualDiffTest.DIFF_OUTPUT_ORIGINAL_IMAGE_ALPHA)

                # create new red highlight image that matches the diff
                highlight_array = np.empty_like(im_actual)
                highlight_array[diff_pixels_mask] = [255, 0, 0, VisualDiffTest.DIFF_OUTPUT_HIGHLIGHT_ALPHA]
                highlight_array[~diff_pixels_mask] = [0, 0, 0, 0]
                highlight = Image.fromarray(highlight_array)

                # overlay the highlight onto the actual image
                diff = Image.alpha_composite(im_actual, highlight)
                self.save_image(diff, VisualDiffTest.DIFF_FOLDER)

                # fail
                raise AssertionError(f'Difference between expected and actual content image higher than threshold. '
                                     f'Difference can be found at {VisualDiffTest.VISUAL_DIFF_FOLDER}.')
            else:
                # If this test has recently failed, remove the diff image written by the previous failed run so only
                # diff images from failed tests remain
                self.remove_diff_image()

    def remove_diff_image(self):
        file_path = os.path.join(VisualDiffTest.VISUAL_DIFF_FOLDER, VisualDiffTest.DIFF_FOLDER, self.file_name)
        if util.safe_exists(file_path):
            util.safe_remove(file_path)


def visual_diff(*args, **kwargs):
    # noinspection PyIncorrectDocstring
    """
    Decorator for tests that push worksteps and yield or
    return configurations for screenshot visual diff tests

    Parameters
    ----------
    image_name : str, optional
        A file name to save the screenshot as.
        Default is the name of the decorated test.

    width : int, optional
        The width in pixels of the screenshot to take.
        Default is 700px.

    height : int, optional
        The height in pixels of the screenshot to take.
        Default is 394px.

    Examples
    -----

    from seeq.spy.tests import test_common

    @pytest.mark.system
    @pytest.mark.visual_diff
    @test_common.visual_diff
    def test_my_stuff():
        # Push a worksheet/workstep to Seeq as part of this system test
        my_workstep = spy.workbooks.AnalysisWorkstep()
        yield my_workstep
    """
    if len(args) == 1 and len(kwargs) == 0 and isinstance(args[0], types.FunctionType):
        return _visual_diff_decorator()(args[0])

    return _visual_diff_decorator(*args, **kwargs)


def _visual_diff_decorator(image_name: Optional[str] = None, *, width=700, height=394):
    def decorator(test_func: Callable[..., Union[AnalysisWorkstep, Iterable[AnalysisWorkstep]]]):
        validated_image_name = image_name or test_func.__name__

        @functools.wraps(test_func)
        def wrapper(mark_expression, *args, **kwargs):
            test_func_result = test_func(*args, **kwargs)

            # If the current pytest run does not accept a test with only the mark 'visual_diff',
            #  then do not run the visual_diff test. This way running system/isolate tests locally
            #  will not run visual_diff tests against baseline images sourced from the CI builds.
            if not Expression.compile(mark_expression).evaluate(lambda m: m == 'visual_diff'):
                return test_func_result

            if isinstance(test_func_result, types.GeneratorType):
                worksteps = list(test_func_result)
            elif isinstance(test_func_result, AnalysisWorkstep):
                worksteps = [test_func_result]
            else:
                raise RuntimeError('Bad visual diff test setup')
            enumerate_image_name = len(worksteps) > 1
            for i, workstep in enumerate(worksteps):  # type: (int, AnalysisWorkstep)
                test_name = f'{validated_image_name}_{i + 1}' if enumerate_image_name else validated_image_name
                visual_diff_test = VisualDiffTest(
                    test_name=test_name,
                    worksheet_id=workstep.worksheet.id,
                    workstep_id=workstep.id,
                    width=width,
                    height=height)
                visual_diff_test.run()

        # Update signature of decorated test to request the 'mark_expression' pytest fixture
        signature = inspect.signature(test_func)
        parameters = list(signature.parameters.values())
        parameters.insert(0, inspect.Parameter('mark_expression', inspect.Parameter.POSITIONAL_OR_KEYWORD))
        wrapper.__signature__ = inspect.Signature(parameters)
        return wrapper

    return decorator


def create_workbook_workstep_asset_template(template_name=None, datasource=None, workbook_id=None):
    display_templates_api = DisplayTemplatesApi(spy.session.client)
    assets_api = AssetsApi(spy.session.client)

    if workbook_id is None:
        workbook_name = 'Workbook %s' % spy._common.new_placeholder_guid()
        workbook = spy.workbooks.Analysis(workbook_name)
        worksheet = workbook.worksheet('1')
        spy.workbooks.push([workbook], quiet=True)
    else:
        workbooks_df = spy.workbooks.search({'ID': workbook_id, 'Workbook Type': 'Analysis'})
        workbooks = spy.workbooks.pull(workbooks_df)
        workbook = workbooks[0]
        # noinspection PyUnresolvedAttribute
        worksheet = workbook.worksheet('1')

    workstep = worksheet.current_workstep()

    asset = assets_api.create_asset(body=AssetInputV1(
        name='Swap Source Asset %s' % spy._common.new_placeholder_guid(),
        scoped_to=workbook.id,
    ))

    display_template = display_templates_api.create_display_template(body=DisplayTemplateInputV1(
        name=(template_name if template_name is not None else 'My Display'),
        scoped_to=workbook.id,
        datasource_class='Seeq Data Lab',
        datasource_id=datasource if datasource is not None else 'Seeq Data Lab',
        source_workstep_id=workstep.id,
        swap_source_asset_id=asset.id
    ))

    return workbook, workstep, asset, display_template


@pytest.mark.unit
def test_escape_regex():
    assert _common.escape_regex(r'mydata\trees') == r'mydata\\trees'
    assert _common.escape_regex(r'Hello There') == r'Hello There'
    assert _common.escape_regex(r'Hello\ There') == r'Hello\\ There'
    assert _common.escape_regex(r' Hello There ') == r' Hello There '
    assert _common.escape_regex('\\ Hello   There  \\') == '\\\\ Hello   There  \\\\'
    assert _common.escape_regex(r'\ Hello <>! There') == r'\\ Hello <>! There'


@pytest.mark.unit
def test_is_guid():
    assert _common.is_guid('2B17ADFD-3308-4C03-BDFB-BF4419BF7B3A') is True
    assert _common.is_guid('2b17adfd-3308-4c03-bdfb-bf4419bf7b3a') is True
    assert _common.is_guid('test 2b17adfd-3308-4c03-bdfb-bf4419bf7b3a') is False
    assert _common.is_guid('2b17adfd-3308-4c03-bdfb-bf4419bf7b3a test') is False
    assert _common.is_guid('2G17adfd-3308-4c03-bdfb-bf4419bf7b3a') is False
    assert _common.is_guid('2b17adfd-3308-4c03-bdfb') is False
    assert _common.is_guid('Hello world') is False
    assert _common.is_guid('') is False
    # noinspection PyTypeChecker
    assert _common.is_guid(123) is False


@pytest.mark.unit
def test_is_email():
    assert _common.is_email('abc@abc.com') is True
    assert _common.is_email('a_b.c@abc.com') is True
    assert _common.is_email('a.b.c@abc.com') is True
    assert _common.is_email(' abc@abc.com') is False
    assert _common.is_email('abc@abc.com ') is False
    assert _common.is_email('abc') is False
    assert _common.is_email('abc@') is False
    assert _common.is_email('c@abc.') is False
    assert _common.is_email('@abc.com') is False
    assert _common.is_email('.@abc.com') is False


@pytest.mark.unit
def test_string_to_formula_literal():
    with pytest.raises(ValueError):
        _common.string_to_formula_literal(1)

    assert _common.string_to_formula_literal("mark") == "'mark'"
    assert _common.string_to_formula_literal("'''") == r"'\'\'\''"
    assert _common.string_to_formula_literal(r"\path\to\thing") == r"'\\path\\to\\thing'"


def test_warning(t):
    with pytest.raises(t):
        warnings.warn(f"{t} warning should be thrown as an error by pytest in .py", t)


@pytest.mark.unit
def test_warning_as_error():
    for t in [UserWarning, SyntaxWarning, RuntimeWarning, FutureWarning, UnicodeWarning, BytesWarning,
              ResourceWarning, ImportWarning]:
        test_warning(t)


@pytest.mark.unit
def test_lru_cache():
    lru_cache = _common.LRUCache(max_size=2)
    assert len(lru_cache.cache) == 0
    lru_cache['a'] = 1
    assert len(lru_cache.cache) == 1
    lru_cache['b'] = 2
    assert lru_cache['a'] == 1
    assert lru_cache['b'] == 2
    assert len(lru_cache.cache) == 2
    lru_cache['c'] = 3
    assert sorted(lru_cache.cache.keys()) == ['b', 'c']
    lru_cache['a'] = 1
    assert sorted(lru_cache.cache.keys()) == ['a', 'c']
    lru_cache['d'] = 4
    assert sorted(lru_cache.cache.keys()) == ['a', 'd']


@pytest.mark.unit
def test_do_names_match_criteria():
    assert not _common.do_paths_match_criteria(
        '** >> Wood',
        'North America >> Canada >> Ontario >> Woodstock'
    )
    assert not _common.do_paths_match_criteria(
        '* >> Canada >> O*c >> **',
        'North America >> Canada >> Ontario >> Woodstock'
    )
    assert _common.do_paths_match_criteria(
        '* >> Canada >> O*o >> **',
        'North America >> Canada >> Ontario >> Woodstock'
    )
    assert _common.do_paths_match_criteria(
        '* >> Canada >> Ontar?o >> **',
        'North America >> Canada >> Ontario >> Woodstock'
    )
    assert not _common.do_paths_match_criteria(
        'North America >> Canada >> Nova Scotia >> **',
        'North America >> Canada >> Ontario >> Woodstock'
    )
    assert _common.do_paths_match_criteria(
        'North America >> **',
        'North America >> Canada >> Ontario >> Woodstock'
    )
    assert _common.do_paths_match_criteria(
        '**',
        'North America >> Canada >> Ontario >> Woodstock'
    )
    assert _common.do_paths_match_criteria(
        'North America >> Canada >> Ontario >> **',
        'North America >> Canada >> Ontario >> Woodstock'
    )
    assert _common.do_paths_match_criteria(
        'North America >> Canada >> Ontario >> Woodstock >> **',
        'North America >> Canada >> Ontario >> Woodstock'
    )
    assert _common.do_paths_match_criteria(
        'North America >> ** >> Canada >> ** >> Woodstock',
        'North America >> Canada >> Ontario >> Woodstock'
    )
    assert not _common.do_paths_match_criteria(
        'North America >> ** >> Canada >> ** >> Woodstock',
        'North America >> USA >> New York >> Woodstock'
    )
    assert _common.do_paths_match_criteria(
        'North America >> ** >> Canada >> **',
        'North America >> Canada >> Ontario >> Woodstock'
    )
    assert _common.do_paths_match_criteria(
        'North America >> ** >> Woodstock',
        'North America >> Canada >> Ontario >> Woodstock'
    )
    assert _common.do_paths_match_criteria(
        'North America >> * >> * >> Woodstock',
        'North America >> Canada >> Ontario >> Woodstock'
    )


@pytest.mark.unit
def test_raise_or_catalog_summarization():
    status_df_dict = {
        'Signal': 0,
        'Scalar': 0,
        'Condition': 0,
        'Threshold Metric': 0,
        'Display': 0,
        'Display Template': 0,
        'Asset': 0,
        'Relationship': 0,
        'Overall': 0,
        'Time': datetime.timedelta(0)
    }
    # A keyboard interrupt should add a Results column saying that it was cancelled
    status = spy.Status(errors='catalog')
    status.df = pd.DataFrame([status_df_dict], index=['Items pushed'])
    exception = KeyboardInterrupt('test_raise_or_catalog_summarization exception')
    _common.raise_or_catalog(e=exception, status=status)
    assert len(status.df) == 1
    assert status.df.at['Items pushed', 'Result'] == 'Canceled'

    # A general error without an index should add a Results column with that message
    status.df = pd.DataFrame([status_df_dict], index=['Items pushed'])
    exception = SPyException('test_raise_or_catalog_summarization exception')
    _common.raise_or_catalog(e=exception, status=status)
    assert status.df.at[None, 'Result'] == '[SPyException] test_raise_or_catalog_summarization exception'

    # An error at a specific index will not get put into the summary
    status.df = pd.DataFrame([status_df_dict], index=['Items pushed'])
    _common.raise_or_catalog(e=exception, status=status, index=1)
    assert len(status.df) == 1
    assert 'Result' not in status.df.columns

    # An error at an index will get put there if a Result column already exists
    status.df = pd.DataFrame([{
        'Type': 'Signal',
        'Name': 'Temperature Minus 5',
        'Formula': '$a - 5',
        'Result': 'Original result',
    }, {
        'Type': 'Condition',
        'Name': 'Cold',
        'Formula': '$a.validValues().valueSearch(isLessThan(80))',
    }, {
        'Type': 'Scalar',
        'Name': 'Constant',
        'Formula': '5',
    }])
    _common.raise_or_catalog(e=exception, status=status, index=1)
    assert len(status.df) == 3
    assert status.df.at[0, 'Result'] == 'Original result'
    assert status.df.at[1, 'Result'] == '[SPyException] test_raise_or_catalog_summarization exception'
    assert pd.isna(status.df.at[2, 'Result'])


def make_index_naive(df):
    # Pandas 2.0 does not allow mixing of naive/aware timestamps when performing operations, so make the index naive
    # like the input data was
    df.index = df.index.tz_localize(None)


class ApiClientRecorder(object):
    def __init__(self, session: Session):
        self.session = session
        self.original = ApiClient.call_api
        self.patch = None
        self.calls = dict()

    def _record_call(self, *args, **kwargs):
        call = f'{args[1]} {args[0]}'
        self.calls[call] = self.calls.get(call, 0) + 1
        return self.original(self.session.client, *args, **kwargs)

    def __enter__(self):
        self.patch = unittest.mock.patch('seeq.sdk.ApiClient.call_api', wraps=self._record_call)
        self.patch.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.patch.__exit__(exc_type, exc_val, exc_tb)


def create_test_asset_group(session: Session, workbook: str) -> str:
    trees_api = TreesApi(session.client)
    push_df = spy.push(pd.DataFrame(), workbook=workbook, datasource=workbook, worksheet=None)
    area_items = spy.search({'Name': '/Area [AB]_Temperature/'})
    area_a_temp_id = area_items[area_items['Name'] == 'Area A_Temperature'].iloc[0]['ID']
    area_b_temp_id = area_items[area_items['Name'] == 'Area B_Temperature'].iloc[0]['ID']

    workbook_id = push_df.spy.workbook_id

    # This matches what the frontend typically does
    trees_api.create_tree(body=AssetGroupInputV1(
        root_asset=AssetGroupRootInputV1(
            name='My Root Asset',
            scoped_to=workbook_id,
            additional_properties=[
                ScalarPropertyV1(name='Tree Type', value='Seeq Workbench'),
                ScalarPropertyV1(name='Created By', value=spy.user.id)
            ]
        ),
        child_assets=[
            AssetGroupAssetInputV1(
                name='My First Asset',
                scoped_to=workbook_id,
                additional_properties=[
                    ScalarPropertyV1(name='Tree Type', value='Seeq Workbench'),
                    ScalarPropertyV1(name='manuallyAdded', value=spy.user.id)
                ],
                children=[
                    FormulaItemInputV1(
                        name='Temperature',
                        scoped_to=workbook_id,
                        formula='$signal',
                        parameters=[FormulaParameterInputV1(name='signal', id=area_a_temp_id)],
                        additional_properties=[
                            ScalarPropertyV1(name='Tree Type', value='Seeq Workbench'),
                            ScalarPropertyV1(name='manuallyAdded', value=spy.user.id)
                        ]
                    )
                ]
            ),
            AssetGroupAssetInputV1(
                name='My Second Asset',
                scoped_to=workbook_id,
                additional_properties=[
                    ScalarPropertyV1(name='Tree Type', value='Seeq Workbench'),
                    ScalarPropertyV1(name='manuallyAdded', value=spy.user.id)
                ],
                children=[
                    FormulaItemInputV1(
                        name='Temperature',
                        scoped_to=workbook_id,
                        formula='$signal',
                        parameters=[FormulaParameterInputV1(name='signal', id=area_b_temp_id)],
                        additional_properties=[
                            ScalarPropertyV1(name='Tree Type', value='Seeq Workbench'),
                            ScalarPropertyV1(name='manuallyAdded', value=spy.user.id)
                        ]
                    )
                ]
            )
        ]
    ))

    return workbook_id


def unzip_to_temp(file_to_unzip):
    with zipfile.ZipFile(file_to_unzip, 'r') as zip_file:
        temp_dir = tempfile.mkdtemp()
        zip_file.extractall(temp_dir)
        return temp_dir
