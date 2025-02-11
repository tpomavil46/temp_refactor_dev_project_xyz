import pandas as pd
import pytest

from seeq import spy
from seeq.sdk import *
from seeq.spy import _common
from seeq.spy._errors import *
from seeq.spy.tests import test_common
from seeq.spy.tests.test_common import Sessions


def setup_module():
    test_common.initialize_sessions()


@pytest.mark.system
def test_acl_push_various_args():
    workbook = 'test_acl_push_various_args'
    scalar_name = f'{workbook} {_common.new_placeholder_guid()}'

    push_results = spy.push(metadata=pd.DataFrame([{
        'Name': scalar_name,
        'Formula': '0',
    }]), workbook=workbook, worksheet=None)

    admin_session = test_common.get_session(Sessions.admin)
    items_api = ItemsApi(admin_session.client)
    acl_output = items_api.get_access_control(id=push_results.iloc[0]['ID'])
    assert len(acl_output.entries) == 2

    # disable inheritance and replace the permissions with Read, Write, Manage for himself
    push_results = spy.acl.push(items=push_results, acl={
        'ID': spy.user.id,
        'Read': True,
        'Write': True,
        'Manage': True
    }, replace=True, disable_inheritance=True)
    assert push_results.spy.status.message in 'Pushed ACLs successfully.'
    assert push_results.spy.status.df.at[0, 'Count'] == 1

    # enable the inheritance again
    push_results = spy.acl.push(items=push_results, acl=[], disable_inheritance=False)
    assert push_results.spy.status.message in 'Pushed ACLs successfully.'
    assert push_results.spy.status.df.at[0, 'Count'] == 1

    # Push with replace
    push_results = spy.acl.push(items=push_results, acl={
        'Username': 'StImpY',
        'Read': True
    }, replace=True)

    assert push_results.spy.status.message in 'Pushed ACLs successfully.'
    assert push_results.spy.status.df.at[0, 'Count'] == 1

    pull_results = spy.acl.pull(push_results)

    assert len(pull_results) == 1
    acl_df = pull_results.at[0, 'Access Control']
    assert len(acl_df) == 3
    stimpy = acl_df[acl_df['Username'] == 'stimpy'].iloc[0]
    assert stimpy['Read']
    assert not stimpy['Write']
    assert not stimpy['Manage']

    # Now add Ren without removing Stimpy
    push_results = spy.acl.push(items=push_results, acl={
        'Name': 'Ren Hoek',
        'Write': True
    })

    acl_output = items_api.get_access_control(id=push_results.iloc[0]['ID'])
    assert len(acl_output.entries) == 4
    stimpy = [entry for entry in acl_output.entries if entry.identity.username == 'stimpy'][0]
    assert stimpy.permissions.read
    assert not stimpy.permissions.write
    assert not stimpy.permissions.manage
    ren = [entry for entry in acl_output.entries if entry.identity.username == 'ren'][0]
    assert ren.permissions.read
    assert ren.permissions.write
    assert not ren.permissions.manage

    user_groups_api = UserGroupsApi(admin_session.client)
    try:
        user_groups_api.create_user_group(body=UserGroupInputV1(name=workbook))
    except ApiException as e:
        if 'is already in use' not in str(e):
            raise

    multiple_aces = [{
        'Username': 'non_admin.tester@seeq.com',
        'Read': True,
        'Write': True,
        'Manage': True
    }, {
        'Name': workbook.upper(),
        'Type': 'UserGroup',
        'Read': True
    }]

    # Now push with more than one entry in the ACL and remove what was there previously (Ren and Stimpy)
    push_results = spy.acl.push(items=push_results, acl=multiple_aces, replace=True)

    acl_output = items_api.get_access_control(id=push_results.iloc[0]['ID'])
    assert len(acl_output.entries) == 4
    spumco = [entry for entry in acl_output.entries if entry.identity.username in ['ren', 'stimpy']]
    assert len(spumco) == 0
    non_admin = [entry for entry in acl_output.entries if 'non_admin' in entry.identity.name][0]
    group = [entry for entry in acl_output.entries if workbook in entry.identity.name][0]
    assert non_admin.permissions.read
    assert non_admin.permissions.write
    assert non_admin.permissions.manage
    assert group.permissions.read
    assert not group.permissions.write
    assert not group.permissions.manage

    # Now replace the system-managed items by disabling inheritance
    multiple_aces[0]['Manage'] = False
    multiple_aces[1]['Write'] = True
    push_results = spy.acl.push(items=push_results, acl=multiple_aces, replace=True, disable_inheritance=True,
                                session=admin_session)

    acl_output = items_api.get_access_control(id=push_results.iloc[0]['ID'])
    assert len(acl_output.entries) == 2
    non_admin = [entry for entry in acl_output.entries if 'non_admin' in entry.identity.name][0]
    group = [entry for entry in acl_output.entries if workbook in entry.identity.name][0]
    assert non_admin.permissions.read
    assert non_admin.permissions.write
    assert not non_admin.permissions.manage
    assert group.permissions.read
    assert group.permissions.write
    assert not group.permissions.manage

    # Don't affect inheritance
    push_results = spy.acl.push(items=push_results, acl=multiple_aces, replace=True, session=admin_session)
    acl_output = items_api.get_access_control(id=push_results.iloc[0]['ID'])
    assert acl_output.permissions_inheritance_disabled
    assert len(acl_output.entries) == 2

    # Re-enable inheritance
    push_results = spy.acl.push(items=push_results, acl={
        'Name': "Everyone",
        'Type': 'UserGroup',
        'Read': True,
        'Write': True,
        'Manage': False
    }, replace=True, disable_inheritance=False, session=admin_session)

    acl_output = items_api.get_access_control(id=push_results.iloc[0]['ID'])

    # Because our Everyone ACE doesn't match the one inherited from the datasource (it has manage=False), there will be
    # an additional ACE entry
    assert len(acl_output.entries) == 3

    push_results = spy.acl.push(items=push_results, acl={
        'Name': "Everyone",
        'Type': 'UserGroup',
        'Manage': True
    }, replace=True, session=admin_session)

    acl_output = items_api.get_access_control(id=push_results.iloc[0]['ID'])

    # Now there will be only 2 entries because we will collapse down to the inherited entry
    assert len(acl_output.entries) == 2

    # Push with unknown user
    with pytest.raises(SPyValueError, match='Could not find identity'):
        spy.acl.push(items=push_results, acl={
            'Username': 'muddy.mudskipper',
            'Read': True
        }, replace=True, session=admin_session)

    # Remove Everyone permissions
    push_results = spy.acl.push(items=push_results, acl=[], replace=True, disable_inheritance=True,
                                session=admin_session)

    # Now try to push with non-admin
    push_results = spy.acl.push(items=push_results, acl={
        'Name': "Everyone",
        'Type': 'UserGroup',
        'Read': True,
        'Write': True,
        'Manage': True
    }, replace=True, errors='catalog', session=test_common.get_session(Sessions.nonadmin))

    assert 'Pushed ACLs with errors' in push_results.spy.status.message
    assert 'does not have access' in push_results.at[0, 'Push Result']


# This test is disabled because it has to disable the Everyone group, which wreaks havoc on other tests. So we can
# run this test manually when necessary to confirm spy.acl behavior with Everyone disabled.
@pytest.mark.disabled
def test_acl_push_everyone_disabled():
    workbook = 'test_acl_push_everyone_disabled'
    scalar_name = f'{workbook} {_common.new_placeholder_guid()}'
    ren_session = test_common.get_session(Sessions.ren)
    stimpy_session = test_common.get_session(Sessions.stimpy)
    admin_session = test_common.get_session(Sessions.admin)
    nonadmin_session = test_common.get_session(Sessions.nonadmin)

    push_results = spy.push(metadata=pd.DataFrame([{
        'Name': scalar_name,
        'Formula': '0',
    }]), datasource=workbook, workbook=workbook, worksheet=None, session=stimpy_session)

    spy.acl.push(push_results, acl=[{
        'ID': nonadmin_session.user.id,
        'Read': True
    }, {
        'Username': 'stimpy',
        'Manage': True
    }], session=stimpy_session)

    pull_results = spy.acl.pull(push_results, session=stimpy_session)
    acl_df = pull_results.at[0, 'Access Control']
    assert len(acl_df) == 3

    users_api = UsersApi(admin_session.client)
    user_groups_api = UserGroupsApi(admin_session.client)

    query_results = users_api.autocomplete_users_and_groups(query='Everyone', limit=100000)
    everyone_group_identity: IdentityPreviewV1 = \
        [item for item in query_results.items if item.type == 'UserGroup' and item.name == 'Everyone'][0]
    everyone_group: UserGroupOutputV1 = user_groups_api.get_user_group(user_group_id=everyone_group_identity.id)

    try:
        # We need to put Ren and Stimpy in a group so that they will see each other
        ren_and_stimpy = user_groups_api.create_user_group(body=UserGroupInputV1(
            name='Ren and Stimpy'
        ))

        user_groups_api.add_identity_to_user_group(
            user_group_id=ren_and_stimpy.id, identity_id=stimpy_session.user.id)

        user_groups_api.add_identity_to_user_group(
            user_group_id=ren_and_stimpy.id, identity_id=ren_session.user.id)

    except ApiException:
        # Likely already created due to multiple test runs
        pass

    try:
        user_groups_api.disable_user_group(user_group_id=everyone_group.id, remove_permissions=False)

        pull_results = spy.acl.pull(push_results, session=stimpy_session)
        acl_df = pull_results.at[0, 'Access Control']
        assert len(acl_df) == 3

        nonadmin_row = acl_df[acl_df['ID'] == nonadmin_session.user.id].iloc[0]
        stimpy_row = acl_df[acl_df['ID'] == stimpy_session.user.id].iloc[0]
        everyone_row = acl_df[acl_df['ID'] == everyone_group.id].iloc[0]
        assert nonadmin_row['Redacted']
        assert not stimpy_row['Redacted']
        assert everyone_row['Redacted']

        # See if we can push exactly the same ACL even though some items are redacted
        push_results = spy.acl.push(items=push_results, acl=acl_df, replace=True, session=stimpy_session)

        # Let's try to add an ACE for Ren, which should succeed since Stimpy can see him
        push_results = spy.acl.push(items=push_results, acl={
            'Username': 'ren',
            'Write': True
        }, replace=False, session=stimpy_session)

        pull_results = spy.acl.pull(push_results, session=stimpy_session)
        acl_df = pull_results.at[0, 'Access Control']
        assert len(acl_df) == 4

        nonadmin_row = acl_df[acl_df['ID'] == nonadmin_session.user.id].iloc[0]
        ren_row = acl_df[acl_df['ID'] == ren_session.user.id].iloc[0]
        stimpy_row = acl_df[acl_df['ID'] == stimpy_session.user.id].iloc[0]
        everyone_row = acl_df[acl_df['ID'] == everyone_group.id].iloc[0]
        assert nonadmin_row['Redacted']
        assert not ren_row['Redacted']
        assert not stimpy_row['Redacted']
        assert everyone_row['Redacted']

        # We shouldn't be able to add an admin ACE because Stimpy doesn't have access to the admin user
        push_results = spy.acl.push(items=push_results, acl={
            'ID': admin_session.user.id,
            'Write': True
        }, replace=False, errors='catalog', session=stimpy_session)

        push_result = push_results.at[0, 'Push Result']
        assert 'does not have access' in push_result

        # Now let's disable inheritance and put only one ACL there
        push_results = spy.acl.push(items=push_results, acl={
            'Username': 'StImpY',
            'Manage': True
        }, disable_inheritance=True, replace=True, session=stimpy_session)

        pull_results = spy.acl.pull(push_results, session=stimpy_session)
        acl_df = pull_results.at[0, 'Access Control']
        assert len(acl_df) == 1

    finally:
        user_groups_api.update_user_group(user_group_id=everyone_group.id, body=UserGroupInputV1(
            name=everyone_group.name,
            description=everyone_group.description,
            is_enabled=True
        ))


@pytest.mark.system
def test_acl_push_workbook():
    name = 'test_push_with_access_control_workbook'
    workbook = f'{name} {_common.new_placeholder_guid()}'

    push_results = spy.push(metadata=pd.DataFrame([{
        'Name': name,
        'Path': f'{name} >> Level 1',
        'Asset': 'Asset 1',
        'Formula': '0'
    }]), workbook=workbook, worksheet=None)

    items_api = ItemsApi(spy.session.client)
    stimpy_session = test_common.get_session(Sessions.stimpy)
    acl_output = items_api.get_access_control(id=push_results.spy.workbook_id)
    stimpy_entries = [entry for entry in acl_output.entries if entry.identity.id == stimpy_session.user.id]
    assert len(stimpy_entries) == 0

    spy.acl.push(push_results.spy.workbook_id, {'Name': 'Stimpson J. Cat', 'Read': True})

    acl_output = items_api.get_access_control(id=push_results.spy.workbook_id)
    stimpy_entries = [entry for entry in acl_output.entries if entry.identity.id == stimpy_session.user.id]
    assert len(stimpy_entries) == 1
    assert stimpy_entries[0].permissions.read
    assert not stimpy_entries[0].permissions.write
    assert not stimpy_entries[0].permissions.manage
