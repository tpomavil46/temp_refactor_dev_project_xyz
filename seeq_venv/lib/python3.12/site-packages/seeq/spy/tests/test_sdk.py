import time
import uuid
from datetime import timedelta, datetime, date

from pytz import timezone

import pandas as pd
import pytest
from seeq import spy
from seeq.sdk import SignalsApi, GetSamplesOutputV1, api_client
from seeq.sdk.rest import ApiException
from seeq.spy import search, utils
from seeq.spy.tests import test_common


@pytest.fixture(scope='module')
def login_seeq_with_example_data():
    test_common.log_in_default_user()
    test_common.wait_for_example_data(spy.session)


@pytest.mark.system
def test_get_signals_json(login_seeq_with_example_data):
    signal = \
        search(pd.DataFrame.from_dict({'Name': ['Area A_Temperature'], 'Type': ['StoredSignal']}), quiet=True).iloc[0]
    begin = time.time()
    response = SignalsApi(spy.session.client).get_samples(id=signal['ID'],
                                                          start='2022-01-01T00:00:00Z',
                                                          end='2022-01-30T00:00:00Z',
                                                          limit=10000,
                                                          _response_type="json")
    print(f"\nget_samples_json took {(time.time() - begin) * 1000} millis")
    assert isinstance(response, dict)
    assert 'samples' in response
    assert isinstance(response['samples'], list)
    assert len(response['samples']) == 10000


@pytest.mark.system
def test_get_signals_default(login_seeq_with_example_data):
    signal = \
        search(pd.DataFrame.from_dict({'Name': ['Area A_Temperature'], 'Type': ['StoredSignal']}), quiet=True).iloc[0]
    begin = time.time()
    response = SignalsApi(spy.session.client).get_samples(id=signal['ID'],
                                                          start='2022-01-01T00:00:00Z',
                                                          end='2022-01-30T00:00:00Z',
                                                          limit=10000)
    print(f"\nget_samples_default took {(time.time() - begin) * 1000} millis")
    assert isinstance(response, GetSamplesOutputV1)
    assert len(response.samples) == 10000


@pytest.mark.unit
def test_swagger_serializer():
    if not utils.is_sdk_module_version_at_least(64):
        return

    # arrange
    uuid_str = '0904363f-d1d1-447a-aedd-6d270224d3ff'
    uuid_obj = uuid.UUID(uuid_str)
    test_client = api_client.ApiClient()

    # act
    uuid_serialized = test_client.sanitize_for_serialization(uuid_obj)

    # assert
    assert uuid_serialized == uuid_str.upper()

    # arrange
    timedelta_obj = timedelta(
        days=1,
        hours=2,
        minutes=3,
        seconds=4,
        milliseconds=5,
        microseconds=6.789
    )

    # act
    timedelta_sec = timedelta_obj.total_seconds()
    timedelta_serialized = test_client.sanitize_for_serialization(timedelta_obj)

    # assert
    assert timedelta_serialized == timedelta_sec
    assert int(timedelta_serialized * 1e6) == timedelta_serialized * 1e6  # microsecond precision

    # arrange
    timedelta_ns_obj = pd.Timedelta(
        days=1,
        hours=2,
        minutes=3,
        seconds=4,
        milliseconds=5,
        microseconds=6
    ) + pd.Timedelta(789, unit='ns')

    # act
    timedelta_ns_serialized = test_client.sanitize_for_serialization(timedelta_ns_obj)

    # assert
    assert int((timedelta_ns_serialized * 1e9) % 1000) == 789  # nanosecond precision


@pytest.mark.unit
def test_datetime_timezone_chcek():
    if not utils.is_sdk_module_version_at_least(64):
        return

    # arrange
    dt_without_tz = datetime(2021, 1, 1, 0, 0, 0)
    dt_with_tz = timezone('UTC').localize(dt_without_tz)
    date_test = date(2021, 1, 1)
    test_client = api_client.ApiClient()

    # act
    sanitized_datetime_with_tz = test_client.sanitize_for_serialization(dt_with_tz)
    sanitized_date = test_client.sanitize_for_serialization(date_test)

    # assert
    with pytest.raises(ApiException) as excinfo:
        test_client.sanitize_for_serialization(dt_without_tz)
    assert excinfo.value.status == 400
    assert "datetimes must have a timezone" in excinfo.value.reason

    assert sanitized_datetime_with_tz == dt_with_tz.isoformat()
    assert sanitized_date == date_test.isoformat()
