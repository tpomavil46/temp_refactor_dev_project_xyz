import mock
import pytest

from seeq import spy
from seeq.spy._errors import *


@pytest.mark.unit
def test_get_sdk_module_version_tuple():
    with mock.patch('seeq.spy._login.get_sdk_module_version_tuple', lambda: (5, 1, 3)):
        assert spy.utils.is_sdk_module_version_at_least(4, 0, 0)
        assert spy.utils.is_sdk_module_version_at_least(5, 0, 2)
        assert spy.utils.is_sdk_module_version_at_least(5, 1, 3)
        assert spy.utils.is_sdk_module_version_at_least(5, 1)
        assert spy.utils.is_sdk_module_version_at_least(5)
        assert not spy.utils.is_sdk_module_version_at_least(6)
        assert not spy.utils.is_sdk_module_version_at_least(5, 2)
        assert not spy.utils.is_sdk_module_version_at_least(5, 1, 4)
        assert not spy.utils.is_sdk_module_version_at_least(5, 2, 3)
        assert not spy.utils.is_sdk_module_version_at_least(6, 1, 3)


@pytest.mark.unit
def test_get_spy_module_version_tuple():
    with mock.patch('seeq.spy._login.get_spy_module_version_tuple', lambda: (183, 3)):
        assert spy.utils.is_spy_module_version_at_least(182, 4)
        assert spy.utils.is_spy_module_version_at_least(183, 3)
        assert spy.utils.is_spy_module_version_at_least(183)
        assert not spy.utils.is_spy_module_version_at_least(184)
        assert not spy.utils.is_spy_module_version_at_least(183, 4)
        assert not spy.utils.is_spy_module_version_at_least(184, 3)


@pytest.mark.unit
def test_get_server_version_tuple():
    with pytest.raises(SPyRuntimeError, match='Not logged in'):
        spy.utils.is_server_version_at_least(75, session=spy.Session())

    with mock.patch('seeq.spy._login.get_server_version_tuple', lambda session: (54, 7, 4)):
        assert spy.utils.is_server_version_at_least(53, 7, 4)
        assert spy.utils.is_server_version_at_least(54, 6, 4)
        assert spy.utils.is_server_version_at_least(54, 7, 4)
        assert spy.utils.is_server_version_at_least(54, 7)
        assert spy.utils.is_server_version_at_least(54)
        assert not spy.utils.is_server_version_at_least(55)
        assert not spy.utils.is_server_version_at_least(54, 8)
        assert not spy.utils.is_server_version_at_least(54, 7, 5)
        assert not spy.utils.is_server_version_at_least(54, 8, 4)
        assert not spy.utils.is_server_version_at_least(55, 7, 4)
