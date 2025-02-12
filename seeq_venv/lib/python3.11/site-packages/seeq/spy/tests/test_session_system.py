import mock
import pandas as pd
import pytest

from seeq import spy
from seeq.spy._errors import *
from seeq.spy.tests import test_common
from seeq.spy.tests.test_common import Sessions


def setup_module():
    test_common.initialize_sessions()


@pytest.mark.system
def test_multiple_login_sessions():
    # Log out so that the default session cannot be incorrectly used for any API calls
    test_common.log_out_default_user()

    try:
        session_ren = test_common.get_session(Sessions.ren)
        session_stimpy = test_common.get_session(Sessions.stimpy)

        numeric_data_df = pd.DataFrame()
        numeric_data_df['Spumco Signal'] = \
            pd.Series([3, 4], index=[pd.to_datetime('2019-01-01T00:00:00Z'), pd.to_datetime('2019-01-03T00:00:00Z')])
        ren_push_df = spy.push(numeric_data_df, session=session_ren)

        numeric_data_df = pd.DataFrame()
        numeric_data_df['Spumco Signal'] = \
            pd.Series([4, 5], index=[pd.to_datetime('2019-01-02T00:00:00Z'), pd.to_datetime('2019-01-04T00:00:00Z')])
        stimpy_push_df = spy.push(numeric_data_df, session=session_stimpy)

        ren_search_df = spy.search({'Name': 'Spumco Signal'}, session=session_ren, all_properties=True)
        stimpy_search_df = spy.search({'Name': 'Spumco Signal'}, session=session_stimpy, all_properties=True)

        assert ren_search_df.iloc[0]['ID'] == ren_push_df.iloc[0]['ID']
        assert stimpy_search_df.iloc[0]['ID'] == stimpy_push_df.iloc[0]['ID']

        assert ren_search_df.iloc[0]['ID'] != stimpy_search_df.iloc[0]['ID']

        ren_pull_df = spy.pull(ren_search_df, start='2019-01-01T00:00:00Z', end='2019-01-04T00:00:00Z',
                               grid=None, session=session_ren)
        stimpy_pull_df = spy.pull(stimpy_search_df, start='2019-01-01T00:00:00Z', end='2019-01-04T00:00:00Z',
                                  grid=None, session=session_stimpy)

        assert len(ren_pull_df) == 2
        assert len(stimpy_pull_df) == 2
        assert ren_pull_df.index.tolist() == [pd.to_datetime('2019-01-01T00:00:00Z'),
                                              pd.to_datetime('2019-01-03T00:00:00Z')]
        assert stimpy_pull_df.index.tolist() == [pd.to_datetime('2019-01-02T00:00:00Z'),
                                                 pd.to_datetime('2019-01-04T00:00:00Z')]
        assert ren_pull_df['Spumco Signal'].tolist() == [3, 4]
        assert stimpy_pull_df['Spumco Signal'].tolist() == [4, 5]

        ren_workbooks = spy.workbooks.pull(ren_push_df.spy.workbook_url, session=session_ren)
        stimpy_workbooks = spy.workbooks.pull(stimpy_push_df.spy.workbook_url, session=session_stimpy)

        # Ren doesn't have access to Stimpy's workbook and vice-versa
        with pytest.raises(ApiException, match='does not have access'):
            spy.workbooks.pull(stimpy_push_df.spy.workbook_url, session=session_ren)

        with pytest.raises(ApiException, match='does not have access'):
            spy.workbooks.pull(ren_push_df.spy.workbook_url, session=session_stimpy)

        assert len(ren_workbooks) == 1
        assert len(stimpy_workbooks) == 1

        ren_workbook = ren_workbooks[0]
        stimpy_workbook = stimpy_workbooks[0]

        assert ren_workbook.worksheets[0].display_items.iloc[0]['ID'] == ren_push_df.iloc[0]['ID']
        assert stimpy_workbook.worksheets[0].display_items.iloc[0]['ID'] == stimpy_push_df.iloc[0]['ID']

    finally:
        test_common.log_in_default_user()


@pytest.mark.system
def test_validated_requests():
    session = test_common.get_session(Sessions.nonadmin)

    test_url = 'seeq.com'

    def validate_args(*args, **kwargs):
        assert 'headers' in kwargs
        assert 'x-sq-auth' in kwargs['headers']
        assert kwargs['headers']['x-sq-auth']

        assert 'cookies' in kwargs
        assert 'sq-auth' in kwargs['cookies']
        assert kwargs['cookies']['sq-auth']

        assert 'verify' in kwargs
        assert kwargs['verify'] is not None

        assert 'timeout' in kwargs
        assert kwargs['timeout'] is not None

        assert kwargs['headers']['Content-Type'] == "application/vnd.seeq.v1+json"
        assert kwargs['headers']['Accept'] == "application/vnd.seeq.v1+json"

        assert kwargs['json'] == {"Hello": "world"}

        assert args[0] == test_url

    with mock.patch('requests.post', mock.Mock(side_effect=validate_args)):
        session.requests.post(test_url,
                              headers={
                                  "Content-Type": "application/vnd.seeq.v1+json",
                                  "Accept": "application/vnd.seeq.v1+json",
                              },
                              json={
                                  "Hello": "world"
                              })


@pytest.mark.ignore
def test_request_timeout():
    # This test is ignored because we can't deterministically cause a timeout, so it's just here to assist in manual
    # testing during development of the timeout option.
    original_retry_timeout = spy.options.retry_timeout_in_seconds
    original_request_timeout = spy.options.request_timeout_in_seconds

    try:
        with pytest.raises(SPyValueError):
            spy.options.request_timeout_in_seconds = 0.2

        with pytest.raises(SPyValueError):
            spy.options.retry_timeout_in_seconds = None

        with pytest.raises(SPyValueError):
            spy.options.retry_timeout_in_seconds = 0

        with pytest.raises(SPyValueError):
            spy.options.retry_timeout_in_seconds = -1

        spy.options.retry_timeout_in_seconds = 0.01

        with pytest.raises(SPyValueError):
            spy.options.request_timeout_in_seconds = 0

        with pytest.raises(SPyValueError):
            spy.options.request_timeout_in_seconds = -1

        spy.options.request_timeout_in_seconds = 0.01

        items = spy.search({'Name': 'Area A_Temperature'})
        spy.pull(items, start='2010-01-01', end='2090-01-01', grid=None)

    finally:
        spy.options.request_timeout_in_seconds = original_request_timeout
        spy.options.retry_timeout_in_seconds = original_retry_timeout
