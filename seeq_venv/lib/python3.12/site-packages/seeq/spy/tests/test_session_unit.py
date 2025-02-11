import pytest

from seeq import spy
from seeq.sdk.configuration import ClientConfiguration


@pytest.mark.unit
def test_session_segregation():
    session1 = spy.Session()
    session2 = spy.Session()

    default_retry = ClientConfiguration.DEFAULT_RETRY_TIMEOUT_IN_SECONDS

    assert session1.options.retry_timeout_in_seconds == default_retry
    assert session2.options.retry_timeout_in_seconds == default_retry
    assert session1.client_configuration.verify_ssl
    assert session2.client_configuration.verify_ssl

    session1.client_configuration.verify_ssl = False
    session2.options.retry_timeout_in_seconds = 3254

    assert session1.client_configuration.retry_timeout_in_seconds == default_retry
    assert session2.client_configuration.retry_timeout_in_seconds == 3254
    assert not session1.client_configuration.verify_ssl
    assert session2.client_configuration.verify_ssl


@pytest.mark.unit
def test_compatibility_setter():
    session = spy.Session()
    session.options = spy.Options(None)

    with pytest.warns(UserWarning,
                      match='Compatibility value 30 is below the minimum value 188. Defaulting to the minimum value.'):
        session.options.compatibility = 30
        assert session.options.compatibility == spy.Options._DEFAULT_MIN_COMPATIBILITY

    with pytest.warns(UserWarning,
                      match=r'Compatibility value 9999999999 is above the maximum value \d+. Defaulting to the maximum '
                            r'value.'):
        session.options.compatibility = 9999999999
        current_major_version, _ = spy.utils.get_spy_module_version_tuple()
        assert session.options.compatibility == current_major_version

    session.options.compatibility = 188
    assert session.options.compatibility == 188

    session.options.compatibility = 190.9
    assert session.options.compatibility == 190
