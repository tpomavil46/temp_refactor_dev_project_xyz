from __future__ import annotations

import concurrent.futures
import datetime
import queue
import re
import threading
from functools import wraps
from typing import Tuple, Callable, Dict, Optional

import pandas as pd

import seeq
from seeq.spy import _common, _datalab
from seeq.spy._errors import *
from seeq.spy._session import Session
from seeq.spy._usage import Usage


class Status:
    """
    Tracks the progress status of various SPy functions.

    Parameters
    ----------
    quiet : bool, default False
        If True, suppresses progress output. Supersedes the quiet flag
        of any function the status is passed to.

    errors : str, default 'raise'
        'raise' to raise exceptions immediately, 'catalog' to track them in an error catalog
    """

    RUNNING = 0
    SUCCESS = 1
    FAILURE = 2
    CANCELED = 3

    class Interrupt(BaseException):
        pass

    jobs: Dict[object, Tuple[Tuple, Optional[Callable[[object, object], None]]]]

    def __init__(self, quiet: Optional[bool] = None, errors: Optional[str] = None,
                 *, session: Session = None, on_update: Optional[Callable[[object], None]] = None):
        self.quiet = quiet if quiet is not None else False
        self.errors = errors if errors is not None else 'raise'
        self._df = pd.DataFrame()
        self.timer = _common.timer_start()
        self.message = None
        self.code = None
        self.warnings = set()
        self.printed_warnings = set()
        self.inner = dict()
        self.update_queue = queue.Queue()
        self.interrupted_event = threading.Event()
        self.jobs = dict()
        self.current_df_index = None
        self.on_update = on_update
        self.on_error = None
        self.session = session if session is not None else seeq.spy.session

    def __str__(self):
        return self.message if self.message else 'Uninitialized'

    def __getstate__(self):
        # We can only pickle certain members. This has to mirror __setstate__().
        return self.quiet, self.df, self.message, self.code, self.warnings, self.inner

    def __setstate__(self, state):
        self.quiet, self.df, self.message, self.code, self.warnings, self.inner = state

    @property
    def df(self) -> pd.DataFrame:
        """
        DataFrame containing info about the results of the SPy function
        using this Status object
        """
        _common.clear_properties_on_df(self._df)
        return self._df

    @df.setter
    def df(self, value: pd.DataFrame):
        self._df = value.copy()
        _common.clear_properties_on_df(self._df)

    def create_inner(self, name: str, quiet: bool = None, errors: str = None):
        inner_status = Status(quiet=self.quiet if quiet is None else quiet,
                              errors=self.errors if errors is None else errors)
        self.inner[name] = inner_status
        return inner_status

    def metrics(self, d):
        self.df = pd.DataFrame(d).transpose()

    def put(self, column, value):
        self.df.at[self.current_df_index, column] = value

    def get(self, column):
        return self.df.at[self.current_df_index, column]

    def warn(self, warning):
        self.warnings.add(warning)

    def raise_or_put(self, e, column):
        self.put(column, _common.format_exception(e))
        if self.errors == 'raise':
            raise e

    @staticmethod
    def handle_keyboard_interrupt(*, errors=None, quiet=None):
        def decorator(func: Callable):
            @wraps(func)
            def out(*args, **kwargs):
                kwargs['status'] = Status.validate(
                    kwargs.get('status'), kwargs.get('session'),
                    kwargs.get('quiet', quiet), kwargs.get('errors', errors))

                for kwarg in ['quiet', 'errors']:
                    if kwarg in kwargs:
                        del kwargs[kwarg]

                try:
                    return func(*args, **kwargs)
                except KeyboardInterrupt as e:
                    kwargs['status'].update('Operation canceled', Status.CANCELED)

                    raise SPyKeyboardInterrupt('Operation canceled')
                finally:
                    # Ensures that any warnings that are added at the end of an operation are still displayed
                    kwargs['status'].display()

            return out

        return decorator

    def _drain_updates(self):
        while True:
            try:
                _index, _updates = self.update_queue.get_nowait()

                for _update_column, _update_value in _updates.items():
                    self.df.at[_index, _update_column] = _update_value

                if len(_updates) > 0 and self.on_update is not None:
                    self.on_update(_index)

            except queue.Empty:
                break

        self.update()

    def send_update(self, index: object, updates: Dict[str, object]):
        if self.is_interrupted():
            # Raise the exception before we put the update on the queue -- we don't want to incorrectly report success
            raise Status.Interrupt()

        self.update_queue.put((index, updates))

    def _skip_display(self):
        return self.quiet or _datalab.is_datalab_api()

    def interrupt(self):
        self.interrupted_event.set()

    def is_interrupted(self):
        return self.interrupted_event.is_set()

    def add_job(self, index: object, func_with_args: Tuple, on_job_success: Callable = None):
        self.jobs[index] = (func_with_args, on_job_success)

    def clear_jobs(self):
        self.jobs = dict()

    def execute_jobs(self, session: Session, *, simple=False):
        try:
            exception_raised = None
            with concurrent.futures.ThreadPoolExecutor(max_workers=session.options.max_concurrent_requests) as executor:
                _futures = dict()
                for job_index, (func_with_args, on_job_success) in self.jobs.items():
                    _futures[executor.submit(*func_with_args)] = (job_index, on_job_success)

                while True:

                    # noinspection PyBroadException
                    try:
                        self._drain_updates()

                        # Now we wait for all the futures to complete, breaking out every half second to drain status
                        # updates (see TimeoutError except block).
                        for future in concurrent.futures.as_completed(_futures, 0.5):
                            job_index, on_job_success = _futures[future]
                            del _futures[future]
                            self._drain_updates()

                            if future.cancelled() or isinstance(future.exception(), Status.Interrupt):
                                if not simple:
                                    self.df.at[job_index, 'Result'] = 'Canceled'
                                continue

                            if future.exception():
                                if simple:
                                    raise future.exception()

                                if isinstance(future.exception(), KeyboardInterrupt):
                                    self.df.at[job_index, 'Result'] = 'Canceled'
                                else:
                                    self.df.at[job_index, 'Result'] = _common.format_exception(future.exception())

                                if self.errors == 'raise' or not isinstance(future.exception(), Exception):
                                    raise future.exception()
                                else:
                                    continue

                            if on_job_success:
                                # noinspection PyBroadException
                                try:
                                    on_job_success(job_index, future.result())
                                except Exception:
                                    if simple:
                                        raise

                                    self.df.at[job_index, 'Result'] = _common.format_exception()
                                    if self.errors == 'raise':
                                        raise
                                    else:
                                        continue

                        # We got all the way through the iterator without encountering a TimeoutError, so break
                        break

                    except concurrent.futures.TimeoutError:
                        # Start the loop again from the top, draining the status updates first
                        pass

                    except BaseException as e:
                        for future in _futures.keys():
                            future.cancel()
                        self.interrupt()
                        exception_raised = e

            if exception_raised:
                self.exception(exception_raised, throw=True)

            self._drain_updates()
            self.clear_jobs()

        except Exception:
            # We drain on non-BaseException. If it's BaseException (like KeyboardInterrupt or SystemExit), we bomb out
            # immediately.
            self._drain_updates()
            self.clear_jobs()
            raise

    def update(self, new_message=None, new_code=None):
        if new_message is None:
            new_message = self.message

        if new_code is not None:
            self.code = new_code

        if self._skip_display():
            self.message = new_message
        else:
            if not _common.display_supports_html():
                self.message = self._display_text(new_message)
            else:
                self.message = self._display_html(new_message)

    def _display_text(self, new_message):
        if new_message == self.message:
            return new_message
        try:
            from IPython.display import display
        except ImportError:
            return new_message

        for warning in (self.warnings - self.printed_warnings):
            display(warning)
        self.printed_warnings = set(self.warnings)

        text = re.sub(r'</?[^>]+>', '', new_message)

        # noinspection PyTypeChecker
        display(text)

        return new_message

    def display(self):
        """
        Force the Status object to output its HTML-based display to the Notebook or the console. Note that this will
        still honor the quiet flag and effectively do nothing if quiet is True.
        """
        if self._skip_display() or self.message is None:
            return

        if not _common.display_supports_html():
            self.message = self._display_text(self.message)
        else:
            self.message = self._display_html(self.message)

    def _display_html(self, new_message):
        _common.ipython_clear_output(wait=True)

        display_df = self.df
        if self.code == Status.RUNNING and len(self.df) > 20 and 'Result' in self.df.columns:
            display_df = self.df[~self.df['Result'].isin(['Queued', 'Success'])]

        display_df = display_df.head(20)

        if self.code == Status.RUNNING:
            color = '#EEEEFF'
        elif self.code == Status.SUCCESS:
            color = '#EEFFEE'
        else:
            color = '#FFEEEE'

        html = ''
        if len(self.warnings) > 0:
            for warning in self.warnings:
                html += '<div style="background-color: #FFFFCC; color:black;">%s</div>' % (
                    Status._massage_cell(warning))

        style = 'background-color: %s;' % color
        html += '<div style="%s">%s</div>' % (
            style + 'color:black; text-align: left;', Status._massage_cell(new_message))

        if len(display_df) > 0:
            # Ignore mathjax renderings so $...$ isn't converted to latex
            html += '<table class="tex2jax_ignore" style="color:black;">'
            html += '<tr><td style="%s"></td>' % style

            for col in display_df.columns:
                align = 'left' if display_df.dtypes[col] == object else 'right'
                html += '<td style="%s text-align: %s;">%s</td>' % (style, align, Status._massage_cell(col))

            html += '</tr>'

            for index, row in display_df.iterrows():
                html += '<tr style="%s">' % style
                html += '<td style="vertical-align: top;">%s</td>' % index
                for cell in row:
                    if isinstance(cell, datetime.timedelta):
                        hours, remainder = divmod(cell.seconds, 3600)
                        minutes, seconds = divmod(remainder, 60)
                        html += '<td style="vertical-align: top;">{:02}:{:02}:{:02}.{:02}</td>'.format(
                            int(hours), int(minutes), int(seconds), int((cell.microseconds + 5000) / 10000))
                    elif isinstance(cell, Usage):
                        html += f'<td style="text-align: right; vertical-align: top;">{cell}</td>'
                    else:
                        align = 'left' if isinstance(cell, str) else 'right'
                        html += '<td style="text-align: %s; vertical-align: top;">%s</td>' % \
                                (align, Status._massage_cell(cell, links=True))
                html += '</tr>'

            html += '</table>'

        # noinspection PyTypeChecker
        if _datalab.is_ipython():
            try:
                from IPython.display import display, HTML
            except ImportError:
                return new_message
            _common.ipython_clear_output(wait=True)
            display(HTML(html))
            return new_message
        elif _datalab.is_rkernel():
            # R users must wrap status messages in `display_html` to render them in the notebook. For example:
            # status <- spy$Status()
            # search_results <- spy$search(..., status=status)
            # `display_html(status$message)`
            return html
        else:
            return html

    @staticmethod
    def _massage_cell(cell, links=False):
        cell = str(cell)

        def markdown_bullets_to_html_bullets(match):
            lines = [re.sub(r'^- ', '', line) for line in match[1].split('\n')]
            return '<ul><li>%s</li></ul>' % '</li><li>'.join(lines)

        cell = re.sub(r'\n(- .*((\n- .*)+|$))', markdown_bullets_to_html_bullets, cell)
        cell = cell.replace('\n', '<br>')
        if links:
            cell = re.sub(r'(http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+{}]|[!*(), ]|(%[0-9a-fA-F][0-9a-fA-F]))+)',
                          r'<a target="_blank" href="\1">link</a>',
                          cell)

        return cell

    def get_timer(self):
        return _common.timer_elapsed(self.timer)

    def reset_timer(self):
        self.timer = _common.timer_start()

    def exception(self, e, throw=False, use_error_message=False):
        if isinstance(e, KeyboardInterrupt):
            status_message = 'Canceled'
            status_code = Status.CANCELED
        else:
            status_message = 'Error encountered, scroll down to view' if not use_error_message else str(e)
            status_code = Status.FAILURE

        self.update(status_message, status_code)

        if isinstance(e, KeyboardInterrupt):
            raise SPyKeyboardInterrupt('Operation canceled')

        # We check for Exception as a base class because we want to raise BaseException no matter what
        if throw or not isinstance(e, Exception):
            raise e

    @staticmethod
    def validate(status: Status, session: Optional[Session],
                 quiet: Optional[bool] = None, errors: Optional[str] = None) -> Status:
        """
        :param status: An already-instantiated Status object
        :type status: Status
        :param session: The session object being used for this operation
        :type status: Session
        :param quiet: If True, suppresses output to Jupyter/console
        :type quiet: bool
        :param errors: 'raise' to raise exceptions immediately, 'catalog' to track them in an error catalog
        :type errors: str

        :rtype Status
        :return: The already-instantiated Status object passed in, or a newly-instantiated Status object

        :meta private:
        """
        if errors not in [None, 'raise', 'catalog']:
            raise SPyValueError("errors argument must be either 'raise' or 'catalog'")

        if quiet is not False and quiet is not True and quiet is not None:
            raise SPyValueError("quiet argument must be either True or False")

        if session is not None and not isinstance(session, Session):
            raise SPyTypeError(f'Argument session must be of type Session, not {type(session)}')

        if status is None:
            status = Status(quiet=quiet, errors=errors, session=session)
        else:
            if not isinstance(status, Status):
                raise SPyTypeError(f'Argument status must be of type Status, not {type(status)}')
            if quiet is not None and quiet != status.quiet:
                raise SPyValueError(
                    f'Invalid arguments: The quiet flag of the supplied status object is `{status.quiet}` while the '
                    f'quiet argument is set to `{quiet}`. Please set the quiet argument to `{status.quiet}` or `None` '
                    f'and try again.')
            if errors is not None and errors != status.errors:
                raise SPyValueError(
                    f'Invalid arguments: The errors flag of the supplied status object is "{status.errors}" while the '
                    f'errors argument is set to "{errors}". Please set the errors argument to "{status.errors}" or '
                    f'`None` and try again.')

            status.session = session

        return status


# For pickling compatibility, see CRAB-33735. Tested by test_pickle_from_python37_and_spy_182_37()
setattr(_common, 'Status', Status)
