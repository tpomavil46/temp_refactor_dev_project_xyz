import json
import re
import types
from unittest import mock

import pandas as pd
import pytest

from seeq import spy, sdk
from seeq.spy import _login
from seeq.spy._errors import SPyRuntimeError, SPyTypeError


@pytest.mark.unit
def test_get_spy_module_version_tuple():
    with mock.patch('seeq.spy.__version__', '283.45'):
        assert _login.get_spy_module_version_tuple() == (283, 45)

    with mock.patch('seeq.spy.__version__', '58.4.81.112.90'):
        assert _login.get_spy_module_version_tuple() == (112, 90)


@pytest.mark.unit
def test_validate_seeq_server_version():
    module_major, module_minor, module_patch = _login.get_sdk_module_version_tuple()

    spy.options.allow_version_mismatch = True

    status = spy.Status()
    spy.session.server_version = 'R22.0.36.01-v202007160902-SNAPSHOT'
    seeq_server_major, seeq_server_minor, seeq_server_patch = _login.validate_seeq_server_version(spy.session, status)
    assert len(status.warnings) == 1
    assert 'The major/minor version' in status.warnings.pop()
    assert (seeq_server_major, seeq_server_minor, seeq_server_patch) == (0, 36, 1)

    status = spy.Status()
    spy.session.server_version = 'R5000000.21.06-v203008100902-BETA'
    seeq_server_major, seeq_server_minor, seeq_server_patch = _login.validate_seeq_server_version(spy.session, status)
    assert len(status.warnings) == 1
    assert 'The major version' in status.warnings.pop()
    assert (seeq_server_major, seeq_server_minor, seeq_server_patch) == (5000000, 21, 6)

    status = spy.Status()
    spy.session.server_version = f'R{module_major}.{module_minor}.{module_patch}'
    seeq_server_major, seeq_server_minor, seeq_server_patch = _login.validate_seeq_server_version(spy.session, status)
    assert len(status.warnings) == 0
    assert (seeq_server_major, seeq_server_minor, seeq_server_patch) == (module_major, module_minor, module_patch)

    spy.options.allow_version_mismatch = False

    status = spy.Status()
    spy.session.server_version = 'R22.0.36.01-v202007160902-SNAPSHOT'
    with pytest.raises(RuntimeError, match=r'The major/minor version'):
        _login.validate_seeq_server_version(spy.session, status)


@pytest.mark.unit
def test_validate_start_and_end():
    # Save off the user from login so we can restore it at the end
    original_user = spy.session.user

    def _assert_with_margin(actual, expected):
        margin_in_seconds = 5 * 60
        actual_start, actual_end = actual
        expected_start = pd.to_datetime(expected[0])
        expected_end = pd.to_datetime(expected[1])
        assert abs(pd.Timedelta(actual_start - expected_start).total_seconds()) < margin_in_seconds
        assert abs(pd.Timedelta(actual_end - expected_end).total_seconds()) < margin_in_seconds
        assert actual_start.tz == expected_start.tz
        assert actual_end.tz == expected_end.tz

    try:
        utc_now = pd.Timestamp.now(tz='UTC')
        utc_yesterday = pd.Timestamp.now(tz='UTC') - pd.Timedelta(days=1)
        eastern_now = pd.Timestamp.now(tz='UTC').tz_convert('US/Eastern')
        eastern_yesterday = pd.Timestamp.now(tz='UTC').tz_convert(
            'US/Eastern') - pd.Timedelta(days=1)
        naive_yesterday = pd.Timestamp.utcnow().tz_localize(None) - pd.Timedelta(days=1)

        spy.session.user = None

        _assert_with_margin(
            _login.validate_start_and_end(spy.session, None, None), (utc_now - pd.Timedelta(hours=1), utc_now))

        _assert_with_margin(
            _login.validate_start_and_end(spy.session, utc_yesterday, None), (utc_yesterday, utc_now))

        _assert_with_margin(
            _login.validate_start_and_end(spy.session, None, utc_yesterday),
            (utc_yesterday - pd.Timedelta(hours=1), utc_yesterday))

        _assert_with_margin(
            _login.validate_start_and_end(spy.session, eastern_yesterday, None), (eastern_yesterday, eastern_now))

        _assert_with_margin(
            _login.validate_start_and_end(spy.session, None, eastern_yesterday),
            (eastern_yesterday - pd.Timedelta(hours=1), eastern_yesterday))

        _assert_with_margin(
            _login.validate_start_and_end(spy.session, naive_yesterday, None), (utc_yesterday, utc_now))

        _assert_with_margin(
            _login.validate_start_and_end(spy.session, None, naive_yesterday),
            (utc_yesterday - pd.Timedelta(hours=1), utc_yesterday))

        spy.session.user = types.SimpleNamespace(workbench=json.dumps({
            'state': {'stores': {'sqWorkbenchStore': {'userTimeZone': 'US/Eastern'}}}
        }))

        _assert_with_margin(_login.validate_start_and_end(
            spy.session,
            # Make timestamps timezone-naive
            eastern_yesterday.tz_localize(None), None),
            (eastern_yesterday, eastern_now))

        _assert_with_margin(_login.validate_start_and_end(
            spy.session,
            # Make timestamps timezone-naive
            None, eastern_yesterday.tz_localize(None)),
            (eastern_yesterday - pd.Timedelta(hours=1), eastern_yesterday))

    finally:
        spy.session.user = original_user


@pytest.mark.unit
def test_parse_input_datetime():
    session = spy.Session()

    def _assert_with_timezone(actual, expected):
        assert actual.value == expected.value
        assert actual.tz == expected.tz

    utc_time = pd.Timestamp('2022-01-01T09:00', tz='UTC')
    naive_time = pd.Timestamp('2022-01-01T09:00')

    session.user = types.SimpleNamespace(workbench=json.dumps({
        'state': {'stores': {'sqWorkbenchStore': {'userTimeZone': 'US/Pacific'}}}
    }))

    _assert_with_timezone(
        _login.parse_input_datetime(session, utc_time), utc_time)

    _assert_with_timezone(
        _login.parse_input_datetime(session, naive_time), pd.Timestamp('2022-01-01T09:00', tz='US/Pacific'))

    session.options.default_timezone = 'EST'

    _assert_with_timezone(
        _login.parse_input_datetime(session, utc_time), utc_time)

    _assert_with_timezone(
        _login.parse_input_datetime(session, naive_time), pd.Timestamp('2022-01-01T09:00', tz='EST'))


@pytest.mark.unit
def test_login_client_warning():
    # Note that since warnings-as-errors are enabled for pytest (in pytest.ini), a UserWarning exception will be
    # raised here. In "normal" use cases, warnings are likely not errors and old code will continue to work as a
    # result of spy._login.__getattr__() returning spy.client.
    with pytest.raises(UserWarning, match='Use of spy._login.client deprecated, use spy.client instead'):
        client = spy._login.client
        assert client is None  # Never reached due to exception on previous line


@pytest.mark.unit
def test_validate_data_lab_license_crab_25594():
    session = spy.Session()

    valid_license = sdk.LicenseStatusOutputV1(additional_features=[
        sdk.LicensedFeatureStatusOutputV1(name='Data_Lab', validity='Valid')])
    with mock.patch('seeq.sdk.SystemApi.get_license', return_value=valid_license):
        _login.validate_data_lab_license(session)

    expired_license = sdk.LicenseStatusOutputV1(additional_features=[
        sdk.LicensedFeatureStatusOutputV1(name='Data_Lab', validity='Expired')])
    with mock.patch('seeq.sdk.SystemApi.get_license', return_value=expired_license):
        with pytest.raises(SPyRuntimeError, match='Seeq Data Lab license is "Expired", could not log in.*'):
            _login.validate_data_lab_license(session)

    no_dl_license = sdk.LicenseStatusOutputV1(additional_features=[
        sdk.LicensedFeatureStatusOutputV1(name='Something_Else', validity='Valid')])
    with mock.patch('seeq.sdk.SystemApi.get_license', return_value=no_dl_license):
        with pytest.raises(SPyRuntimeError, match='Seeq Data Lab is not licensed for this server, could not log in.*'):
            _login.validate_data_lab_license(session)

    with pytest.raises(SPyTypeError, match="Argument 'session' should be type Session, but is type str"):
        _login.validate_data_lab_license(session="invalid")


@pytest.mark.unit
def test_get_user_timezone():
    session = spy.Session()
    session.user = types.SimpleNamespace(workbench=json.dumps({
        'state': {'stores': {'sqWorkbenchStore': {'userTimeZone': 'US/Pacific'}}}
    }))
    assert _login.get_user_timezone(session) == 'US/Pacific'

    with pytest.raises(SPyTypeError, match="Argument 'session' should be type Session, but is type SimpleNamespace"):
        _login.get_user_timezone(session=session.user)


@pytest.mark.unit
def test_get_fallback_timezone():
    session = spy.Session()
    session.user = types.SimpleNamespace(workbench=json.dumps({
        'state': {'stores': {'sqWorkbenchStore': {'userTimeZone': 'US/Pacific'}}}
    }))
    assert _login.get_fallback_timezone(session) == 'US/Pacific'

    with pytest.raises(SPyTypeError, match="Argument 'session' should be type Session, but is type SimpleNamespace"):
        _login.get_fallback_timezone(session=session.user)


@pytest.mark.unit
def test_validate_login():
    session = spy.Session()
    status = spy.Status()

    with pytest.raises(SPyRuntimeError, match=re.escape('Not logged in. Execute spy.login() before calling this '
                                                        'function.')):
        _login.validate_login(session=session, status=status)

    with pytest.raises(SPyTypeError, match="Argument 'session' should be type Session, but is type Status"):
        _login.validate_login(session=status, status=status)
