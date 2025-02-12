import logging
import os
from pathlib import Path

import mock
import pytest

from seeq.spy.jobs import _execute

execution_notebook_file = "data-lab/jupyter/seeq/scheduling/ExecutionNotebook.ipynb"
test_notebook_path = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'docs', 'Documentation')
test_notebook_file = "spy.pull.ipynb"
label = 'run-in-executor-label'
test_sched_index = '0'
job_key = '2ff2df94-a532-4f20-8864-959813ab4c17_7726F74691C5'
execution_notebook = os.path.normpath(
    os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', '..', '..', '..', execution_notebook_file))
notebook_file_scheduled = os.path.join(test_notebook_path, test_notebook_file)
scheduled_file_filename: str = Path(notebook_file_scheduled).name
scheduled_file_folder: Path = Path(notebook_file_scheduled).parent


def setup_module():
    setup_run_in_executor()


def teardown_module():
    # reset values for other tests
    cleanup()


@pytest.mark.unit
@mock.patch('seeq.spy._datalab.get_execution_notebook', return_value=execution_notebook)
@mock.patch('seeq.spy._datalab.is_executor', return_value=True)
def test_execution_instance(*args):
    execution_instance = _execute.ExecutionInstance()
    assert execution_instance.file_path == notebook_file_scheduled
    assert execution_instance.label == label
    assert execution_instance.index == test_sched_index
    assert execution_instance.job_key == job_key

    expected_merged_notebook_path = os.path.join(test_notebook_path, scheduled_file_folder,
                                                 f'.spy.pull.executor.{test_sched_index}.{label}.ipynb')
    assert os.path.normpath(execution_instance.merged_notebook_path) == \
           os.path.normpath(expected_merged_notebook_path)

    expected_job_result_path = os.path.join(test_notebook_path,
                                            _execute.RESULTS_FOLDER,
                                            f'spy.pull.executor.{test_sched_index}.{label}.html')
    assert os.path.normpath(execution_instance.job_result_path) == os.path.normpath(expected_job_result_path)


@pytest.mark.unit
@mock.patch('seeq.spy._datalab.get_execution_notebook', return_value=execution_notebook)
@mock.patch('seeq.spy._datalab.is_executor', return_value=True)
def test_executor_logger(*args):
    assert _execute.get_log_level_from_executor() == 'INFO'

    exec_logger = _execute.ExecutionInstance().logger
    assert exec_logger.name == "executor_logger"
    assert logging.getLevelName(exec_logger.level) == 'INFO'

    os.environ['LOG_LEVEL'] = 'DEBUG'
    exec_logger = _execute.ExecutionInstance().logger
    assert _execute.get_log_level_from_executor() == 'DEBUG'
    assert logging.getLevelName(exec_logger.level) == 'DEBUG'

    os.environ['LOG_LEVEL'] = 'TRACE'
    exec_logger = _execute.ExecutionInstance().logger
    assert _execute.get_log_level_from_executor() == 'TRACE'
    # assert DEBUG here since python logging doesnt have TRACE
    assert logging.getLevelName(exec_logger.level) == "DEBUG"


def setup_run_in_executor():
    os.environ['SEEQ_SDL_FILE_PATH'] = notebook_file_scheduled
    os.environ['SEEQ_SDL_LABEL'] = label
    os.environ['SEEQ_SDL_SCHEDULE_INDEX'] = test_sched_index
    os.environ['SEEQ_SDL_JOB_KEY'] = job_key


def cleanup():
    del os.environ['SEEQ_SDL_FILE_PATH']
    del os.environ['SEEQ_SDL_LABEL']
    del os.environ['SEEQ_SDL_SCHEDULE_INDEX']
    del os.environ['SEEQ_SDL_JOB_KEY']
