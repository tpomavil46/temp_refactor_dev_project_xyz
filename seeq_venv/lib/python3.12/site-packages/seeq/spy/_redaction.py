from functools import wraps
from typing import Callable, List, Any

from seeq.spy._common import *
from seeq.spy._status import Status


def request_safely(*,
                   action_description: str = '',
                   status: Optional[Status] = None,
                   additional_errors: Optional[List[int]] = None,
                   ignore_errors: Optional[List[int]] = None,
                   on_error: Optional[Callable[[str], Any]] = None,
                   default_value: Optional[Any] = None):
    """
    Safely make request(s) to the Seeq API while catching known problematic exceptions. The status codes to be
    caught are 403 (Forbidden), 404 (Not Found), and 500 (Internal Server Error).

    :param action_description: The description for the action(s) being performed. It should include the overall
        purpose and specific information about the item(s) being interacted with. It should be written such that the
        format of 'Failed to [action_description] because...' will be grammatically correct and be meaningful to
        users and Seeq support.
    :param status: The status object used to log warnings.
    :param additional_errors: Allow these additional HTTP error statuses to be caught. Must be a list of ints.
    :param ignore_errors: Ignore these HTTP error statuses and return default_value. Must be a list of ints.
    :param on_error: A callable to override how to handle an error. Should accept a single string parameter that
        describes the problem. Default behavior is to log a warning to the status.
    :param default_value: The default value to return if a 403, 404, or 500 error occurs.
    :return: The output of the decorated function or default_value if a failure occurred.
    """

    def decorator(func: Callable):
        @wraps(func)
        def out(*args, **kwargs):
            if status is not None and not isinstance(status, Status):
                raise TypeError(f'Status parameter must be of type spy.Status, but was {status.__class__.__name__}')

            def _no_action(msg):
                pass

            error_action = _no_action
            if callable(on_error):
                error_action = on_error
            elif status is not None:
                if status.on_error is not None:
                    error_action = status.on_error
                else:
                    error_action = status.warn

            try:
                return func(*args, **kwargs)
            except ApiException as e:
                if ignore_errors and e.status in ignore_errors:
                    return default_value
                if status is None or status.errors != 'catalog':
                    raise
                reason = get_api_exception_message(e)
                if e.status == 403:
                    error_action(f'Failed to {action_description} due to insufficient access: "{reason}"')
                elif e.status == 404:
                    error_action(f'Failed to {action_description} because it was not found: "{reason}"')
                elif e.status == 500:
                    error_action(f'Failed to {action_description} because an internal server error occurred. '
                                 f'If this persists, please submit logs to Seeq support.')
                elif additional_errors and e.status in additional_errors:
                    error_action(f'Failed to {action_description}: "{reason}"')
                else:
                    # Other statuses (409 particularly) should be re-raised so the normal error handling can work
                    raise
                return default_value

        return out

    return decorator


def safely(func: Callable,
           *,
           action_description: str = '',
           status: Optional[Status] = None,
           additional_errors: Optional[List[int]] = None,
           ignore_errors: Optional[List[int]] = None,
           on_error: Optional[Callable[[str], Any]] = None,
           default_value: Optional[Any] = None):
    """
    Safely make request(s) to the Seeq API while catching known problematic exceptions. The status codes to be
    caught are 403 (Forbidden), 404 (Not Found), and 500 (Internal Server Error).

    :param func: The function or lambda that will be called with error handling.
    :param action_description: The description for the action(s) being performed. It should include the overall
        purpose and specific information about the item(s) being interacted with. It should be written such that the
        format of 'Failed to [action_description] because...' will be grammatically correct and be meaningful to
        users and Seeq support.
    :param status: The status object used to log warnings.
    :param additional_errors: Allow these additional HTTP error statuses to be caught. Must be a list of ints.
    :param ignore_errors: Ignore these HTTP error statuses and return default_value. Must be a list of ints.
    :param on_error: A callable to describe how to handle an error. Should accept a single string parameter that
        describes the problem. Default behavior is to log a warning to the status.
    :param default_value: The default value to return if a 403, 404, or 500 error occurs.
    :return: The output of the decorated function or default_value if a failure occurred.
    """
    return request_safely(action_description=action_description,
                          status=status,
                          additional_errors=additional_errors,
                          ignore_errors=ignore_errors,
                          on_error=on_error,
                          default_value=default_value)(func)()
