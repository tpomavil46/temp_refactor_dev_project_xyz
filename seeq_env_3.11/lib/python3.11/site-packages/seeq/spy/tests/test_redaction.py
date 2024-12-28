import warnings

import pytest

from seeq import spy
from seeq.sdk.rest import ApiException
from seeq.spy._errors import SPyRuntimeError
from seeq.spy._redaction import safely, request_safely


def _raise_api_exception(status: int = 403, reason: str = 'Test error'):
    raise ApiException(status=status, reason=reason)


_valid_object = {'Name': 'Value to be returned', 'Unexpected Behaviors': 0}


def _return_valid_object():
    return _valid_object


@pytest.mark.unit
def test_happy_path():
    value = safely(_return_valid_object,
                   action_description='safely() should return the resulting value',
                   status=spy.Status())
    assert value == _valid_object

    @request_safely(action_description='@request_safely should return the resulting value',
                    status=spy.Status())
    def _decorated_return_valid_object():
        return _return_valid_object()

    value = _decorated_return_valid_object()
    assert value == _valid_object


@pytest.mark.unit
def test_return_value_and_default_value():
    value = safely(_return_valid_object,
                   action_description='succeeding should return the function return value',
                   status=spy.Status(errors='catalog'),
                   default_value=_valid_object)
    assert value == _valid_object

    value = safely(_raise_api_exception,
                   action_description='erroring should return the default_value',
                   status=spy.Status(errors='catalog'),
                   default_value=_valid_object)
    assert value == _valid_object


@pytest.mark.unit
def test_raise_errors():
    with pytest.raises(ApiException, match='Test error'):
        safely(_raise_api_exception, action_description='rethrow when "raise" is specified',
               status=spy.Status(errors='raise'))

    with pytest.raises(ApiException, match='Test error'):
        safely(_raise_api_exception, action_description='rethrow when non-"catalog" is specified',
               status=spy.Status(errors='something'))


@pytest.mark.unit
def test_status_param():
    # Providing None will always rethrow
    with pytest.raises(ApiException, match='Test error'):
        safely(_raise_api_exception, action_description='rethrow when status is None', status=None)

    # Providing an incorrect object results in a direct exception
    warnings.simplefilter('ignore')
    with pytest.raises(TypeError, match='Status parameter must be of type spy.Status, but was dict'):
        invalid_status = {'errors': 'catalog'}
        safely(_return_valid_object, action_description='throw when status is a Dict', status=invalid_status)

    with pytest.raises(TypeError, match='Status parameter must be of type spy.Status, but was list'):
        invalid_status = ['errors', 'warn']
        safely(_return_valid_object, action_description='throw when status is a List', status=invalid_status)

    with pytest.raises(TypeError, match='Status parameter must be of type spy.Status, but was ApiException'):
        invalid_status = ApiException(status=418)
        safely(_return_valid_object, action_description='throw when status is an Object', status=invalid_status)


@pytest.mark.unit
def test_using_the_status():
    safely(_raise_api_exception,
           action_description='catalog with no status should be a no-op',
           status=spy.Status(errors='catalog'))

    status = spy.Status(errors='catalog')
    safely(_raise_api_exception,
           action_description='catalog with a status should warn',
           status=status)
    assert len(status.warnings) == 1
    assert 'due to insufficient access' in list(status.warnings)[0]
    assert 'Test error' in list(status.warnings)[0]

    status = spy.Status(errors='catalog')
    custom_output = None

    def set_custom_output(msg):
        nonlocal custom_output
        custom_output = msg

    safely(_raise_api_exception,
           action_description='catalog with a custom action should not warn',
           status=status,
           on_error=set_custom_output)
    assert len(status.warnings) == 0
    assert 'due to insufficient access' in custom_output
    assert 'Test error' in custom_output


@pytest.mark.unit
def test_error_types():
    def error_type_causes_warning(test_name, error_status, expected_additional_message,
                                  error_message=None, additional_errors=None):
        status = spy.Status(errors='catalog')
        safely(lambda: _raise_api_exception(error_status, error_message),
               action_description=test_name,
               status=status,
               additional_errors=additional_errors)
        assert len(status.warnings) == 1
        assert expected_additional_message in list(status.warnings)[0]
        if error_message is not None:
            assert error_message in list(status.warnings)[0]

    error_type_causes_warning(test_name='403 is caught',
                              error_status=403,
                              expected_additional_message='due to insufficient access',
                              error_message='Some permissions problem')

    error_type_causes_warning(test_name='404 is caught',
                              error_status=404,
                              expected_additional_message='was not found',
                              error_message='IDK where it is')

    error_type_causes_warning(test_name='500 is caught',
                              error_status=500,
                              expected_additional_message='an internal server error occurred')

    error_type_causes_warning(test_name='400 is caught if we allow it',
                              error_status=400,
                              expected_additional_message='Failed',
                              error_message='No, you messed up',
                              additional_errors=[400])

    msg = 'I feel conflicted'
    with pytest.raises(ApiException, match=msg):
        safely(lambda: _raise_api_exception(409, msg),
               action_description='409 is not caught',
               status=spy.Status(errors='catalog'))

    msg = 'Something wrong in SPy'

    def _raise_spy_exception():
        raise SPyRuntimeError(msg)

    with pytest.raises(SPyRuntimeError, match=msg):
        safely(_raise_spy_exception,
               action_description='non-ApiExceptions are not caught',
               status=spy.Status(errors='catalog'))


@pytest.mark.unit
def test_decorator():
    # This test file assumes the @request_safely decorator is being called by the safely() function so code coverage
    # is best done using safely(). This test just verifies the most basic functionality of the decorator.
    status_catalog = spy.Status(errors='catalog')

    @request_safely(action_description='@request_safely using "catalog" should catch and log like normal',
                    status=status_catalog)
    def _decorated_raise_api_exception_catalog():
        _raise_api_exception()

    status_raise = spy.Status(errors='raise')

    @request_safely(action_description='@request_safely using "raise" should rethrow like normal',
                    status=status_raise)
    def _decorated_raise_api_exception_raise():
        _raise_api_exception()

    _decorated_raise_api_exception_catalog()
    assert len(status_catalog.warnings) == 1
    assert 'Test error' in list(status_catalog.warnings)[0]

    with pytest.raises(ApiException, match='Test error'):
        _decorated_raise_api_exception_raise()
