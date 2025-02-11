from __future__ import annotations

import inspect
import json
import re
import sys
from typing import Dict, Optional

from seeq.sdk.rest import ApiException
from seeq.spy import _datalab

__all__ = ['SPyDependencyNotFound', 'SPyException', 'SPyKeyboardInterrupt', 'SPyRuntimeError',
           'SPyValueError', 'SPyTypeError', 'SchedulePostingError', 'ApiException', 'get_api_exception_message']


class SPyDependencyNotFound(Exception):
    def __init__(self, message: str,
                 dependent_identifier: Optional[str] = None,
                 dependency_identifier: Optional[str] = None):
        super().__init__(message)
        self.dependent_identifier = dependent_identifier
        self.dependency_identifier = dependency_identifier

    @staticmethod
    def generate_error_string(dependency_exceptions: Dict[object, SPyDependencyNotFound]):
        noise_reduction_count = 0
        error_messages = list()
        for item_id, dependency_exception in dependency_exceptions.items():
            if dependency_exception.dependency_identifier in dependency_exceptions:
                # Reduce the noise during troubleshooting. If a dependency of a dependency is missing, it's not
                # that useful to know.
                noise_reduction_count += 1
                continue

            error_messages.append(str(dependency_exception))

        error_messages.sort()

        if noise_reduction_count > 0:
            error_messages.append(f'{noise_reduction_count} additional dependency errors not shown (for brevity)')

        return '\n'.join(error_messages)


class SPyBaseException(BaseException):
    """
    Base exception class of all errors internally handled and raised by SPy.
    """
    pass


class SPyException(SPyBaseException, Exception):
    """
    Exception class of all errors internally handled and raised by SPy
    """
    pass


class SPyKeyboardInterrupt(SPyBaseException, KeyboardInterrupt):
    """
    Raised when the kernel is interrupted, e.g. by pressing Ctrl+C or by clicking Stop in a Jupyter Notebook
    """


class SPyRuntimeError(SPyException, RuntimeError):
    """
    Raised when an error is detected by SPy that does not fall in any of the other categories
    """
    pass


class SPyValueError(SPyException, ValueError):
    """
    Raised when a SPy operation or function receives an argument that has the right type but an inappropriate value
    """
    pass


class SPyTypeError(SPyException, TypeError):
    """
    Raised when a SPy operation or function is applied to an object of inappropriate type
    """
    pass


class SchedulePostingError(SPyRuntimeError):
    """
    Raised by the spy.jobs module when a scheduling a notebook fails
    """
    pass


def get_ipython_cell_no(frame):
    matched = re.match(r'^<ipython-input-(\d+)-[a-z\d]+>$', frame.f_code.co_filename)
    if matched:
        return matched[1]
    # ipykernel now creates cell filenames that don't include their input number.
    # The following workaround searches the internal ipython cell input variables for the frame's sourcecode
    if 'ipykernel' not in frame.f_code.co_filename:
        return None
    # noinspection PyBroadException
    try:
        # Sometimes there is an extra newline character at end of source string versus the ipython saved input
        sourcecode = inspect.getsource(frame).strip()
        cell_inputs = frame.f_globals.get('_ih')
        if cell_inputs is None or not isinstance(cell_inputs, list):
            return None
        for input_no in reversed(range(len(cell_inputs))):
            if sourcecode in cell_inputs[input_no]:
                return input_no
        return None
    except Exception:
        return None


def get_api_exception_message(e):
    if not isinstance(e, ApiException):
        raise ValueError('Exception is not an ApiException.')
    content = e.reason if e.reason and e.reason.strip() else ''
    if e.body:
        # noinspection PyBroadException
        try:
            body = json.loads(e.body)
            if len(content) > 0:
                content += ' - '
            content += body.get('statusMessage', '')
            content += ",".join(error.get('message', '') for error in body.get('_embedded', {}).get('errors', []))
        except Exception:
            pass

    return '(%d) %s' % (e.status, content)


def get_exception_message(e):
    if isinstance(e, ApiException):
        return get_api_exception_message(e)
    if isinstance(e.args, tuple) and len(e.args) == 1 and isinstance(e.args[0], Exception):
        return get_exception_message(e.args[0])
    return str(e)


def get_exception_name(e):
    if isinstance(e, ApiException):
        return 'Seeq API Error'
    if isinstance(e, SchedulePostingError):
        return 'Scheduling Error'
    if isinstance(e, SPyException):
        return 'SPy Error'
    return type(e).__name__


def format_tb_header(colors, exception, lineno, ipy_inputno=None, filename=None, warning=None, pretty_name=True):
    exc_name = get_exception_name(exception) if pretty_name else type(exception).__name__
    exc_msg = get_exception_message(exception)

    header = f'{colors.excName}{exc_name}: {colors.Normal}{exc_msg}'
    location = None
    if ipy_inputno is not None:
        location = f'Error found at {colors.em}line {lineno}{colors.Normal} in ' \
                   f'{colors.filenameEm}cell {ipy_inputno}{colors.Normal}.'
    elif filename is not None:
        location = f'Error found at {colors.em}line {lineno}{colors.Normal} in ' \
                   f'{colors.filenameEm}file {filename}{colors.Normal}.'
    return '\n' + '\n\n'.join([s for s in [header, location, warning] if s is not None])


def show_stacktrace_button(self, exc_tuple):
    try:
        import ipywidgets as widgets
        from IPython.display import display
    except ImportError:
        raise SPyDependencyNotFound(f'`ipywidgets` is required to display stack trace buttons in Jupyter. Please '
                                    f'use `pip install seeq-spy[widgets]` to use this feature.')
    layout_kwargs = dict()
    layout_kwargs['width'] = 'auto'
    layout_kwargs['height'] = 'auto'
    if not _datalab.is_datalab():
        # Data Lab has nice widget layouts preconfigured. If not in Data Lab, just adding border can help prettify
        layout_kwargs['border'] = 'solid 1px black'
    button = widgets.Button(description='Click to show stack trace',
                            layout=widgets.Layout(**layout_kwargs))

    colors = self.InteractiveTB.Colors
    exc_options_msg = '(To show stack trace by default, set %sspy.options.friendly_exceptions%s to %sFalse%s)' % (
        colors.valEm, colors.Normal, colors.name, colors.Normal
    )

    def click_func(b):
        b.close()
        if _datalab.is_datalab_addon_mode():
            with traceback_output:
                self.showtraceback(exc_tuple=exc_tuple, running_compiled_code=True)
                print(exc_options_msg)
        else:
            self.showtraceback(exc_tuple=exc_tuple, running_compiled_code=True)
            print(exc_options_msg)

            from ipylab import JupyterFrontEnd
            app = JupyterFrontEnd()
            app.on_ready(app.commands.execute('logconsole:open'))

    button.on_click(click_func)
    if _datalab.is_datalab_addon_mode():
        traceback_output = widgets.Output()
        display(traceback_output)
    return button


def spy_exception_handler(self, etype, value, tb, tb_offset=None):
    # Surround the whole exception handler with try/catch, so if anything goes
    # wrong, the user will see the normal stack trace with hopefully no side effects
    try:
        from IPython.display import display
    except ImportError:
        raise SPyDependencyNotFound(f'`IPython` is required to use the SPy exception handler. Please install it using '
                                    f'`pip install seeq-spy[jupyter]` to use this feature.')

    # noinspection PyBroadException
    try:
        # Disable exception chaining. It's confusing and displays messages we don't want to display
        value.__suppress_context__ = True
        # The following loop traverses through the traceback and identifies
        # the traceback head that calls SPy, if it exists
        spy_call_head = None
        runner = tb
        while runner:
            try:
                if 'seeq.spy' in inspect.getmodule(runner.tb_next.tb_frame.f_code).__name__:
                    spy_call_head = runner
                    break
            except AttributeError:
                # If the module can't be determined, this catches the None.__name__ call
                pass
            runner = runner.tb_next

        if spy_call_head is not None:
            warning = None
            if issubclass(etype, ApiException):
                # ApiException
                # We don't truncate the stacktrace because seeing the code that calls the API could be useful for
                # both the user and Seeq support
                button = True

            elif issubclass(etype, SPyBaseException):
                # Handled SPy error that user can fix
                # Truncate stacktrace so that no internal SPy code is unnecessarily displayed
                spy_call_head.tb_next = None
                button = True

            else:
                # Fatal SPy error
                warning = 'This error was not handled by SPy. Please contact Seeq support if this problem persists.'
                button = False

            spy_call_cell_no = get_ipython_cell_no(spy_call_head.tb_frame)
            spy_call_lineno = spy_call_head.tb_lineno
            spy_call_filename = spy_call_head.tb_frame.f_code.co_filename
            header = format_tb_header(self.InteractiveTB.Colors, value, spy_call_lineno, spy_call_cell_no,
                                      spy_call_filename, warning)

        else:
            # User error outside of SPy

            # get last ipython cell in stacktrace
            last_cell = tb
            while last_cell.tb_next is not None and get_ipython_cell_no(last_cell.tb_next.tb_frame) is not None:
                last_cell = last_cell.tb_next

            # If the previous loop iterated zero times, then there has been a break in compatibility (likely
            # "ipykernel" is no longer in the ipython cell filename). Fast-forward to the last frame in this case,
            # so we don't display the run_cell frame in interactiveshell.py
            if last_cell == tb:
                while last_cell.tb_next:
                    last_cell = last_cell.tb_next

            cell_no = get_ipython_cell_no(last_cell.tb_frame)
            line_no = last_cell.tb_lineno
            filename = last_cell.tb_frame.f_code.co_filename
            header = format_tb_header(self.InteractiveTB.Colors, value, line_no, cell_no, filename, pretty_name=False)
            button = True

        print(header)
        if button and _datalab.is_jupyter():
            # Display a button, which will show the (possibly truncated) traceback when clicked
            display(show_stacktrace_button(self, (etype, value, tb)))
        else:
            # If not running in Jupyter, or when handling a fatal SPy error, show traceback automatically
            self.showtraceback((etype, value, tb), running_compiled_code=True)

    except KeyboardInterrupt:
        print('Operation canceled', file=sys.stderr)

    except Exception:
        self.showtraceback((etype, value, tb), running_compiled_code=True)


def add_spy_exception_handler():
    if not _datalab.is_ipython():
        raise SPyRuntimeError('Friendly SPy exception handling is unavailable when not using IPython.')

    # Set IPython to use this exception handler for all exceptions of type SPyBaseException
    try:
        from IPython import get_ipython
    except ImportError:
        raise SPyDependencyNotFound('`IPython` is required to use the SPy exception handler. Please install it using '
                                    f'`pip install seeq-spy[jupyter]` to use this feature.')

    get_ipython().set_custom_exc((BaseException,), spy_exception_handler)


def remove_spy_exception_handler():
    if _datalab.is_ipython():
        try:
            from IPython import get_ipython
        except ImportError:
            raise SPyDependencyNotFound(
                '`IPython` is required to use the SPy exception handler. Please install it using '
                f'`pip install seeq-spy[jupyter]` to use this feature.')
        get_ipython().set_custom_exc((), None)
