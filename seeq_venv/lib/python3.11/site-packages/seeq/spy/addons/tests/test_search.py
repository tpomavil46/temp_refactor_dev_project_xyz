import unittest
from unittest import mock

import pandas as pd
import pytest
from seeq import spy
from seeq.sdk import *
from seeq.sdk.rest import ApiException
from seeq.spy import addons, Session, _common
from seeq.spy.addons.tests import test_common as addons_test_common
from seeq.spy.tests import test_common
from seeq.spy.tests.test_common import Sessions


def setup_module():
    test_common.initialize_sessions()
    addons_test_common.enable_addon_tools(test_common.get_session(Sessions.admin), True)


def _create_test_tools(session: Session, suffix_=''):
    my_tools = [{"Name": f'test tool 1_{suffix_}',
                 "Description": f"test tool 1{suffix_}",
                 "Target URL": "https://www.google.com",
                 "Icon": "fa fa-icon",
                 "Link Type": "tab",
                 "Users": [session.user.username]},
                {"Name": f'test tool 2_{suffix_}',
                 "Description": f"test tool 2{suffix_}",
                 "Target URL": "https://www.seeq.com",
                 "Icon": "fa fa-icon",
                 "Users": [session.user.username]
                 }]
    # searching for tools doesn't require admin access but creating tools does
    tools = addons.install(my_tools, session=test_common.get_session(Sessions.admin))
    return tools


def _uninstall_test_tools(ids):
    for idd in ids:
        system_api = SystemApi(test_common.get_session(Sessions.admin).client)
        system_api.delete_add_on_tool(id=idd)


@pytest.mark.system
def test_search_df_metadata():
    query = {'Name': '*'}
    search_results = addons.search(query, errors='catalog')

    assert isinstance(search_results.spy.status.df, pd.DataFrame)
    assert search_results.spy.kwargs['query'] == query


@pytest.mark.system
def test_search_with_wildcard_plus_another_prop():
    unique_name_suffix = _common.new_placeholder_guid()
    session = test_common.get_session(Sessions.nonadmin)
    df_tools = _create_test_tools(session, suffix_=unique_name_suffix)

    query = {'Name': '*', "Description": f"2{unique_name_suffix}"}
    search_results = addons.search(query, session=session, errors='catalog')

    assert len(search_results) == 1
    # clean up
    _uninstall_test_tools(df_tools['ID'].values)


@pytest.mark.system
def test_search_with_wildcard_plus_id():
    unique_name_suffix = _common.new_placeholder_guid()
    session = test_common.get_session(Sessions.nonadmin)
    df_tools = _create_test_tools(session, suffix_=unique_name_suffix)

    query = {'Name': '*', "ID": df_tools['ID'].values[0]}
    search_results = addons.search(query, session=session, errors='catalog')

    assert len(search_results) == 1

    # clean up
    _uninstall_test_tools(df_tools['ID'].values)


@pytest.mark.system
def test_search_by_id():
    unique_name_suffix = _common.new_placeholder_guid()
    session = test_common.get_session(Sessions.nonadmin)
    df_tools = _create_test_tools(session, suffix_=unique_name_suffix)
    idd = df_tools['ID'][0]
    search_results = addons.search({"ID": idd}, session=session, errors='catalog')
    assert len(search_results) == 1
    # clean up
    _uninstall_test_tools(df_tools['ID'].values)


@pytest.mark.system
def test_search_with_df():
    unique_name_suffix = _common.new_placeholder_guid()
    session = test_common.get_session(Sessions.nonadmin)
    df_tools = _create_test_tools(session, suffix_=unique_name_suffix)
    my_items = pd.DataFrame(
        {'Name': [f'test tool 1_{unique_name_suffix}', f'test tool 2_{unique_name_suffix}'],
         'Link Type': 'window'})
    search_results = addons.search(my_items, session=session, errors='catalog')
    assert len(search_results) == 1
    assert search_results['Link Type'][0] == 'window'

    # clean up
    _uninstall_test_tools(df_tools['ID'].values)


@pytest.mark.system
def test_search_with_multiple_props():
    unique_name_suffix = _common.new_placeholder_guid()
    session = test_common.get_session(Sessions.nonadmin)
    df_tools = _create_test_tools(session, suffix_=unique_name_suffix)

    search = addons.search({"Name": "test tool", "Description": "test tool"}, session=session, errors='catalog')
    assert len(search) >= 2

    search_results_no_match = addons.search(
        pd.DataFrame([{"Name": f'test tool', "Description": "test tool"}]), errors='catalog')
    search_results_match = addons.search(pd.DataFrame([{"Name": f'test tool 1_{unique_name_suffix}',
                                                        "Description": f"test tool 1{unique_name_suffix}"},
                                                       {"Name": f'test tool 2_{unique_name_suffix}',
                                                        "Description": f"test tool 2{unique_name_suffix}"}]),
                                         session=session, errors='catalog')
    assert len(search_results_match) - len(search_results_no_match) == 2

    # clean up
    _uninstall_test_tools(df_tools['ID'].values)


@pytest.mark.system
def test_redacted_item_properties():
    unique_name_suffix = str(_common.new_placeholder_guid())
    session = test_common.get_session(Sessions.nonadmin)
    df_tools = _create_test_tools(session, suffix_=unique_name_suffix)

    reason = 'No can do'
    mock_exception_thrower = mock.Mock(side_effect=ApiException(status=403, reason=reason))
    with unittest.mock.patch('seeq.sdk.ItemsApi.get_access_control', new=mock_exception_thrower):
        with pytest.raises(ApiException, match=reason):
            addons.search({"Name": "test tool", "Description": "test tool"}, session=session)

        status = spy.Status(errors='catalog')
        search_results = addons.search({"Name": f"test tool 1_{unique_name_suffix}", "Description": "test tool"},
                                       session=session, status=status)
        # Assert that the dataframe includes the basic information from the search
        assert len(search_results.index) == 1
        # and the results contain expected values
        assert search_results.at[0, 'Name'].startswith('test tool')
        # but the permissions are missing
        assert search_results.at[0, 'Groups'] == ['needs admin rights to display']
        assert search_results.at[0, 'Users'] == ['needs admin rights to display']
        # and the status has warnings
        assert len(status.warnings) >= 1
        warning = list(status.warnings)[0]
        assert 'Failed to find permissions for addon with id' in warning
        assert f'due to insufficient access: "(403) {reason}"' in warning

    # clean up
    _uninstall_test_tools(df_tools['ID'].values)
