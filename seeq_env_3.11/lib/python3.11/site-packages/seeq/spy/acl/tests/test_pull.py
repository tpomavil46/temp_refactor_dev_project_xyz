import pandas as pd
import pytest

from seeq import spy
from seeq.spy import _common
from seeq.spy._errors import *
from seeq.spy.acl import _pull
from seeq.spy.tests import test_common
from seeq.spy.workbooks import Analysis


def setup_module():
    test_common.initialize_sessions()


def get_first_element_or_none(elements):
    if len(elements) > 0:
        return elements[0]
    else:
        return None


@pytest.mark.system
def test_acl_pull():
    search_df = spy.search({'Name': 'Area ?_Temperature'})

    # Put some columns with incorrect types in there to make sure they get replaced
    search_df['Access Control'] = pd.Series(dtype=int)
    search_df['Permissions Inheritance Disabled'] = pd.Series(dtype=int)
    search_df['Permissions From Datasource'] = pd.Series(dtype=int)

    pull_df = spy.acl.pull(search_df)

    assert 'Permissions Inheritance Disabled' in pull_df
    assert 'Permissions From Datasource' in pull_df
    assert 'Access Control' in pull_df

    for _, row in pull_df.iterrows():
        assert not row['Permissions Inheritance Disabled']
        assert not row['Permissions From Datasource']

        acl_df = row['Access Control']

        acl_df.reset_index(drop=True, inplace=True)
        acl_df.drop(columns=['ID'], inplace=True)
        acl_dicts = acl_df.to_dict(orient='records')

        assert len(acl_dicts) == 2
        apikey_dict = get_first_element_or_none([acl for acl in acl_dicts if acl['Name'] == 'Agent API Key'])
        if apikey_dict:
            del apikey_dict['Role']
            assert apikey_dict == {
                'Type': 'User',
                'Name': 'Agent API Key',
                'Username': 'agent_api_key',
                'Email': None,
                'Directory': 'Seeq',
                'Archived': False,
                'Enabled': True,
                'Redacted': False,
                'Origin Type': 'Datasource',
                'Origin Name': 'Example Data',
                'Read': True,
                'Write': True,
                'Manage': True
            }

        agents_group_dict = get_first_element_or_none([acl for acl in acl_dicts if acl['Name'] == 'Agents'])
        if agents_group_dict:
            del agents_group_dict['Role']
            assert agents_group_dict == {
                'Type': 'UserGroup',
                'Name': 'Agents',
                'Username': None,
                'Email': None,
                'Directory': 'Seeq',
                'Archived': False,
                'Enabled': True,
                'Redacted': False,
                'Origin Type': 'Datasource',
                'Origin Name': 'Example Data',
                'Read': True,
                'Write': True,
                'Manage': True
            }

        # We need permissions either for agent_api_key or for Agents group
        assert agents_group_dict is not None or apikey_dict is not None

        everyone_dict = [acl for acl in acl_dicts if acl['Name'] == 'Everyone'][0]
        del everyone_dict['Role']

        assert everyone_dict == {
            'Type': 'UserGroup',
            'Name': 'Everyone',
            'Username': None,
            'Email': None,
            'Directory': 'Seeq',
            'Archived': False,
            'Enabled': True,
            'Redacted': False,
            'Origin Type': 'Datasource',
            'Origin Name': 'Example Data',
            'Read': True,
            'Write': True,
            'Manage': False
        }


@pytest.mark.system
def test_acl_pull_no_access():
    workbook = 'test_acl_pull_no_access'
    scalar_name = f'{workbook} {_common.new_placeholder_guid()}'

    # Push without access_control arg
    push_results = spy.push(metadata=pd.DataFrame([{
        'Name': scalar_name,
        'Formula': '0',
    }]), workbook=workbook, worksheet=None)

    # Remove Everyone permissions
    push_results = spy.acl.push(items=push_results, acl=[], replace=True, disable_inheritance=True,
                                session=test_common.get_session(test_common.Sessions.admin))

    with pytest.raises(ApiException):
        spy.acl.pull(push_results)

    status = spy.Status(errors='catalog')
    pull_results = spy.acl.pull(push_results, status=status)

    assert 'Pulled ACLs with errors' in pull_results.spy.status.message
    assert 'does not have access' in pull_results.at[0, 'Pull Result']
    assert len(status.warnings) == 1
    warning = list(status.warnings)[0]
    assert 'Failed to get ACL for item' in warning

    push_results = spy.acl.push(items=push_results, acl=[{
        'ID': spy.session.user.id,
        'Read': True
    }], replace=True, disable_inheritance=True, session=test_common.get_session(test_common.Sessions.admin))

    pull_results = spy.acl.pull(push_results, include_my_effective_permissions=True)

    assert pull_results.at[0, 'Read Permission']
    assert not pull_results.at[0, 'Write Permission']
    assert not pull_results.at[0, 'Manage Permission']


@pytest.mark.system
def test_items_to_data_frame():
    df = _pull.items_to_data_frame('MyID1')
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.at[0, 'ID'] == 'MyID1'

    df = _pull.items_to_data_frame(['MyID1', 'MyID2'])
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert df.at[0, 'ID'] == 'MyID1'
    assert df.at[1, 'ID'] == 'MyID2'

    df = _pull.items_to_data_frame({'ID': 'MyID1'})
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.at[0, 'ID'] == 'MyID1'

    df = _pull.items_to_data_frame([{'ID': 'MyID1'}, {'ID': 'MyID2'}])
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert df.at[0, 'ID'] == 'MyID1'
    assert df.at[1, 'ID'] == 'MyID2'

    df = _pull.items_to_data_frame(Analysis({'ID': 'MyID1', 'Name': 'MyName1'}))
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.at[0, 'ID'] == 'MyID1'
    assert df.at[0, 'Name'] == 'MyName1'

    df = _pull.items_to_data_frame([
        Analysis({'ID': 'MyID1', 'Name': 'MyName1'}),
        Analysis({'ID': 'MyID2', 'Name': 'MyName2'})
    ])

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert df.at[0, 'ID'] == 'MyID1'
    assert df.at[0, 'Name'] == 'MyName1'
    assert df.at[1, 'ID'] == 'MyID2'
    assert df.at[1, 'Name'] == 'MyName2'

    df = _pull.items_to_data_frame(pd.DataFrame([
        {'ID': 'MyID1', 'Name': 'MyName1'},
        {'ID': 'MyID2', 'Name': 'MyName2'}
    ]))

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert df.at[0, 'ID'] == 'MyID1'
    assert df.at[0, 'Name'] == 'MyName1'
    assert df.at[1, 'ID'] == 'MyID2'
    assert df.at[1, 'Name'] == 'MyName2'

    df = _pull.items_to_data_frame(pd.DataFrame())
    assert len(df) == 0
    assert 'ID' in df

    with pytest.raises(SPyValueError, match='"items" must have "ID" column'):
        _pull.items_to_data_frame(pd.DataFrame([{'Name': 'blah'}]))

    with pytest.raises(SPyTypeError, match='"items" argument is not recognized'):
        _pull.items_to_data_frame(1)
