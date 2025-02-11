from __future__ import annotations

import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import Union, Optional

from seeq import spy
from seeq.base import util
from seeq.spy import _datalab
from seeq.spy._errors import *

RESULTS_FOLDER = '_Job Results/'
CELL_EXECUTION_TIMEOUT = 86400


class ExecutionInstance:
    _logger = None

    def __init__(self):
        if not _datalab.is_executor():
            raise SPyRuntimeError('Execution of notebooks is not supported through SPy interface.')

        self.project_name = os.environ.get('SEEQ_PROJECT_NAME', '')
        self.project_uuid = os.environ.get('SEEQ_PROJECT_UUID', '')
        self.seeq_server_url = os.environ.get('SEEQ_SERVER_URL', '')
        self.file_path = os.environ.get('SEEQ_SDL_FILE_PATH', '')
        self.label = os.environ.get('SEEQ_SDL_LABEL', '')
        self.index = os.environ.get('SEEQ_SDL_SCHEDULE_INDEX', '')
        self.job_key = os.environ.get('SEEQ_SDL_JOB_KEY', '')
        scheduled_file_filename: str = Path(self.file_path).name
        scheduled_file_folder: Path = Path(self.file_path).parent
        # merged_notebook_path is our new scheduled notebook with login cell
        self.merged_notebook_path = _compose_filename(self.index, self.label,
                                                      scheduled_file_filename,
                                                      scheduled_file_folder,
                                                      extension='.ipynb', result=False)
        # This is our resulting html file after execution
        self.job_result_path = _compose_filename(self.index, self.label,
                                                 scheduled_file_filename,
                                                 scheduled_file_folder,
                                                 extension='.html', result=True)

    @property
    def logger(self) -> logging.Logger:
        """
        Logger to be used for logging inside executor container
        """
        if self._logger is not None:
            return self._logger

        log_level = get_log_level_from_executor()

        # python logging doesnt have TRACE
        log_level = "DEBUG" if log_level == "TRACE" else log_level

        executor_logger = logging.getLogger("executor_logger")
        exec_handler = logging.StreamHandler(sys.stdout)
        exec_formatter = logging.Formatter(
            f'%(levelname)s - Notebook {self.file_path.replace("%", "%%")} with jobKey {self.job_key} %(message)s')
        exec_handler.setFormatter(exec_formatter)
        executor_logger.addHandler(exec_handler)
        executor_logger.setLevel(log_level)

        self._logger = executor_logger
        return executor_logger

    def execute(self):
        # Abort if scheduled notebook does not exist
        if not Path(self.file_path).exists():
            self._unschedule_and_notify("The notebook cannot be found at the scheduled path.")
            self.logger.error('not found, aborting')
            return

        # noinspection PyBroadException
        try:
            # Create the new scheduled notebook with login cell
            self.create_merged_notebook()

            # Execute the new scheduled notebook with login cell
            self.execute_merged_notebook()

            # Convert to html and save
            self.export_html()

            # Log success for executor
            self.logger.info('succeeded')
        except Exception as e:
            message = getattr(e, 'message', repr(e))

            try:
                from nbclient.exceptions import DeadKernelError
                if isinstance(e, DeadKernelError):
                    from cgroupspy import trees
                    cgroups_tree = trees.Tree()
                    memory_node = cgroups_tree.get_node_by_path('/memory/')
                    max_memory = memory_node.controller.max_usage_in_bytes
                    message += f' It used {max_memory} bytes of memory.'
            except ImportError:
                pass  # cgroupspy is not guaranteed to be installed on old data lab

            self.notify_on_skipped_execution(message)
            self.logger.error('encountered error', exc_info=e)

            # Raise so that job status message will indicate failure
            raise SPyRuntimeError(message) from e
        finally:
            # Cleanup
            if Path(self.merged_notebook_path).exists():
                Path(self.merged_notebook_path).unlink()

    def create_merged_notebook(self):
        try:
            import nbformat
            from nbformat import NotebookNode
        except ImportError:
            SPyDependencyNotFound(f'`nbformat` is not installed. Please use `pip install seeq-spy[jobs]` '
                                  f'to use this feature.')

        # Open the notebook that has been scheduled for execution
        with util.safe_open(self.file_path) as f_notebook_scheduled:
            nb_notebook_scheduled = nbformat.read(f_notebook_scheduled, nbformat.NO_CONVERT)

        # Get kernel language
        language = _datalab.get_notebook_language(nb_notebook_scheduled)
        if not language:
            error_message = f'could not determine language for {f_notebook_scheduled}'
            self._unschedule_and_notify(error_message)
            self.logger.error(error_message)
            raise SPyRuntimeError(error_message)

        # Get execution notebook
        execution_notebook = _datalab.get_execution_notebook(language)
        if not execution_notebook:
            error_message = f'could not find execution notebook for {f_notebook_scheduled} with language {language}'
            self._unschedule_and_notify(error_message)
            self.logger.error(error_message)
            raise SPyRuntimeError(error_message)

        # Open the dummy notebook with spy.login cell
        with util.safe_open(execution_notebook) as f_notebook_execution:
            nb_notebook_execution = nbformat.read(f_notebook_execution, nbformat.NO_CONVERT)

        # Create new notebook dynamically that includes login cell first
        nb_notebook_merged = NotebookNode(nb_notebook_execution.copy())

        # Add in cells from scheduled notebook
        nb_notebook_merged['cells'].extend(nb_notebook_scheduled.cells.copy())

        # Use the metadata (including kernel) specified in the scheduled notebook
        nb_notebook_merged.metadata = nb_notebook_scheduled.metadata

        # Write out the new joined notebook as hidden notebook with the same name as scheduled notebook
        with util.safe_open(self.merged_notebook_path, 'w') as f_notebook_merged:
            nbformat.write(nb_notebook_merged, f_notebook_merged)

        # Log to executor
        self.logger.debug('successfully merged execution notebook with scheduled notebook')

    def execute_merged_notebook(self):
        try:
            import nbformat
            from nbformat import NotebookNode
        except ImportError:
            SPyDependencyNotFound(f'`nbformat` is not installed. Please use `pip install seeq-spy[jobs]` '
                                  f'to use this feature.')

        # Open the notebook for execution
        with util.safe_open(self.merged_notebook_path, 'r+') as f_notebook_merged:
            nb_notebook_merged = nbformat.read(f_notebook_merged, nbformat.NO_CONVERT)

        # Configure the execute processor to allow errors and the output path
        try:
            import nbconvert
        except ImportError:
            SPyDependencyNotFound(f'`nbconvert` is not installed. Please use `pip install seeq-spy[jobs]` '
                                  f'to use this feature.')

        proc = nbconvert.preprocessors.ExecutePreprocessor(timeout=CELL_EXECUTION_TIMEOUT,
                                                           allow_errors=True)
        proc.preprocess(nb_notebook_merged, {'metadata': {'path': Path(self.file_path).parent}})

        # Log to executor
        self.logger.debug('successfully executed merged notebook')

        # Python logger has no TRACE level. Special logging case here since dumping notebook
        # contents can clog the log
        if is_log_level_trace_from_executor():
            self.logger.debug(f'executed notebook contents from {self.merged_notebook_path}:{nb_notebook_merged}')

        # Remove login cell from notebook
        del nb_notebook_merged['cells'][0]

        # Decrement the "execution_count" by 1 to correct notebook cell numbering
        # "execution_count" can have 'None' as value so just pass on any exception to continue
        for cell in nb_notebook_merged['cells']:
            # noinspection PyBroadException
            try:
                if 'execution_count' in cell:
                    execution_count = int(cell['execution_count'])
                    execution_count -= 1
                    cell['execution_count'] = execution_count
            except Exception:
                pass

            if 'outputs' in cell:
                for output in cell['outputs']:
                    # noinspection PyBroadException
                    try:
                        if 'execution_count' in output:
                            execution_count = int(output['execution_count'])
                            execution_count -= 1
                            output['execution_count'] = execution_count
                    except Exception:
                        pass

        # Write the scheduled notebook
        with util.safe_open(self.merged_notebook_path, 'w') as f_notebook_merged:
            nbformat.write(nb_notebook_merged, f_notebook_merged)

        # Log to executor
        self.logger.debug('successfully edited merged notebook')

        # The executed notebook will be returned if spy.jobs.execute was called by a user
        return nb_notebook_merged

    def export_html(self):
        try:
            import nbformat
        except ImportError:
            SPyDependencyNotFound(f'`nbformat` is not installed. Please use `pip install seeq-spy[jobs]` '
                                  f'to use this feature.')
        # Open the modified notebook that has been scheduled for execution
        with util.safe_open(self.merged_notebook_path) as f_notebook_merged:
            nb_notebook_merged = nbformat.read(f_notebook_merged, nbformat.NO_CONVERT)

        # Configure the HTML exporter and export
        try:
            import nbconvert
        except ImportError:
            SPyDependencyNotFound(f'`nbconvert` is not installed. Please use `pip install seeq-spy[jobs]` '
                                  f'to use this feature.')
        html_exporter = nbconvert.HTMLExporter()
        job_result_html, _ = html_exporter.from_notebook_node(nb_notebook_merged)

        # Create parent folder if not existing and write out the exported html to file
        Path(self.job_result_path).parent.mkdir(parents=True, exist_ok=True)
        with util.safe_open(self.job_result_path, 'w') as f_job_result_file:
            f_job_result_file.write(job_result_html)

        # Log to executor
        self.logger.debug('successfully exported merged notebook')

    def _build_email_content(self, subject: str, skipped_execution: bool, error_message: str) -> str:
        project_url = f"{self.seeq_server_url}/data-lab/{self.project_uuid.upper()}"
        unsubscribe_notification = \
            "If you don't want to receive skipped execution notifications, you can re-schedule " \
            "the notebook via SPy Jobs using the parameter <code>notify_on_skipped_execution=False</code>" \
                if skipped_execution else \
                "If you don't want to receive notifications when a notebook is unscheduled, you can schedule the " \
                "notebook via SPy Jobs using the parameter <code>notify_on_automatic_unschedule=False</code>"

        return f"""
            <html><body>
            <h3>{subject}</h3>
            <p>Click <a href="{project_url}">here<a> to navigate to the project, or copy-paste the following URL into
            your browser: {project_url}</p>
            <p><b>Project name:</b> {self.project_name}</p>
            <p><b>Notebook path:</b> {self.file_path}</p>
            <p><b>Execution label:</b> {self.label}</p>
            <p><b>Error message:</b> {error_message}</p>
            <p>{unsubscribe_notification}</p>
            </body></html>
        """

    def notify_on_skipped_execution(self, error_message: Optional[str] = None) -> None:
        try:
            if is_notify_on_skipped_execution():
                _login()
                self.logger.debug(
                    f'Notifying user {spy.session.user.username} about skipped execution of the notebook in project '
                    f'{self.project_name}, path {self.file_path}, label {self.label}. The notebook failed to execute.')
                subject = "A scheduled notebook skipped execution"
                spy.notifications.send_email(
                    to=spy.session.user.email if spy.session.user.email else spy.session.user.username,
                    subject=subject,
                    content=self._build_email_content(subject, skipped_execution=True, error_message=error_message)
                )
        except Exception as e:
            self.logger.error(e, exc_info=e)

    def _unschedule_and_notify(self, error_message: Optional[str] = None) -> None:
        try:
            _login()
            spy.jobs.unschedule(label=self.label)
            if is_notify_on_automatic_unschedule():
                self.logger.debug(
                    f'Notifying user {spy.session.user.username} about automatic unschedule of the notebook in '
                    f'project {self.project_name}, path {self.file_path}, label {self.label}. The notebook was '
                    f'moved or deleted.')
                subject = "A scheduled notebook was automatically unscheduled"
                spy.notifications.send_email(
                    to=spy.session.user.email if spy.session.user.email else spy.session.user.username,
                    subject=subject,
                    content=self._build_email_content(subject, skipped_execution=False, error_message=error_message)
                )
        except Exception as e:
            self.logger.error(e, exc_info=e)


def execute():
    """
    Execute a notebook. (Internal Seeq function: Not intended for end-users)
    """
    file_path = os.environ.get('SEEQ_SDL_FILE_PATH', '')
    job_key = os.environ.get('SEEQ_SDL_JOB_KEY', '')

    spy_job_command = 'from seeq import spy; from seeq.spy.jobs._execute import ExecutionInstance; spy.jobs._execute.ExecutionInstance().execute();'

    # Run the Spy job as a subprocess. Let stdout/stderr go to the parent process
    process = subprocess.Popen(['python3', '-c', spy_job_command])

    # Wait for the subprocess to finish and get the exit status
    exit_code = process.wait()

    job_status_message = f'Notebook {file_path.replace("%", "%%")} with jobKey {job_key}'

    if exit_code == 0:
        job_status_message += " completed successfully."
    else:
        reason = 'terminated' if exit_code in [-9, -15, 137] else 'failed'
        job_status_message += f" {reason} with exit code {exit_code}."

    print(job_status_message)


def _compose_filename(index: str, label: str, scheduled_file_filename: str, scheduled_file_folder: Path,
                      extension: str = '.html', result: bool = True) -> str:
    folder_path: Union[str, Path] = Path(scheduled_file_folder, RESULTS_FOLDER) if result else \
        scheduled_file_folder
    hidden_file_prefix = '.' if not result else ''
    filename_no_ext: str = hidden_file_prefix + Path(scheduled_file_filename).stem

    # Build up the job result html file name
    folder = str(folder_path)
    executor = ".executor"
    index = '.' + index if len(index) > 0 else ''
    label = '.' + label if len(label) > 0 else ''

    result_filename = f'{folder}/{filename_no_ext}{executor}{index}{label}{extension}'
    return result_filename


def _login():
    spy.login(url=os.environ.get('SEEQ_SERVER_URL'), private_url=os.environ.get('SEEQ_PRIVATE_URL'),
              auth_token=os.environ.get('SEEQ_SDL_AUTH_TOKEN'),
              ignore_ssl_errors=os.environ.get('SEEQ_VERIFY_SSL') != '1', quiet=True)


def get_log_level_from_executor() -> str:
    return str(os.environ.get('LOG_LEVEL', 'INFO')).upper()


def is_log_level_trace_from_executor() -> bool:
    return get_log_level_from_executor() == "TRACE"


def is_notify_on_skipped_execution() -> bool:
    return os.environ.get('SEEQ_SDL_NOTIFY_ON_SKIPPED_EXECUTION', '') == 'true'


def is_notify_on_automatic_unschedule() -> bool:
    return os.environ.get('SEEQ_SDL_NOTIFY_ON_AUTOMATIC_UNSCHEDULE', '') == 'true'
