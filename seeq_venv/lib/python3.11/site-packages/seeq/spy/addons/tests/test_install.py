import pandas as pd
import pytest

from seeq import spy
from seeq.sdk import *
from seeq.spy import _login, Session, _common
from seeq.spy import addons
from seeq.spy._errors import *
from seeq.spy.addons import _permissions
from seeq.spy.addons.tests import test_common as addons_test_common
from seeq.spy.tests import test_common
from seeq.spy.tests.test_common import Sessions


def setup_module():
    test_common.initialize_sessions()
    addons_test_common.enable_addon_tools(test_common.get_session(Sessions.admin), True)


def _create_testing_group():
    admin_session = test_common.get_session(Sessions.admin)
    user_groups_api = UserGroupsApi(admin_session.client)
    group_name = 'testers'

    # Note this code used to employ a read-then-write pattern where we would attempt to find the group and,
    # if not found, create it. This was vulnerable to a race condition when running tests in parallel: One test would
    # create the group in between the other test's find/create operations. Now we just try to create the group and if
    # that fails due to the group already existing, then find the group.

    group = None
    try:
        group = user_groups_api.create_user_group(body={"name": group_name})
    except ApiException as e:
        if e.status == 400:
            # This indicates that the group has already been created
            pass
        else:
            raise

    if group is None:
        group = _login.find_group(admin_session, group_name)

    return group


def _cleanup_installed_plugins(name: str):
    admin_session = test_common.get_session(Sessions.admin)
    df = addons.search(pd.DataFrame([{'Name': f'{name}'}]), session=admin_session, errors='catalog')
    if len(df) > 0:
        addons.uninstall(df, session=admin_session)


def tool_names(session: Session):
    system_api = SystemApi(session.client)
    tools = system_api.get_add_on_tools().add_on_tools
    return [tool.name for tool in tools]


@pytest.mark.system
def test_install_one_tool():
    items_api = ItemsApi(spy.session.client)

    new_name = f"test_install_one_tool{_common.new_placeholder_guid()}"
    my_tool = {"Name": new_name,
               "Description": "My new tool",
               "Target URL": "https://www.google.com",
               "Groups": ["Everyone"]}

    try:
        new_install_err_msg = 'Non-administrators may only install Add-on Tools in Development Mode'
        old_install_err_msg = 'Only administrators may create add-on tools'
        expected_non_admin_install_errors = f'{new_install_err_msg}|{old_install_err_msg}'
        with pytest.raises(Exception, match=expected_non_admin_install_errors):
            addons.install(my_tool)

        admin_session = test_common.get_session(Sessions.admin)
        df = addons.install(my_tool, session=admin_session)
        assert items_api.get_item_and_all_properties(id=df['ID'].values[0])
        permissions = _permissions.get_addon_permissions(df['ID'].values[0], session=admin_session)
        assert 'Everyone' in permissions['Groups']
        assert new_name in tool_names(admin_session)

        new_uninstall_err_msg = 'does not have MANAGE access for the AddOnTool'
        old_uninstall_err_msg = 'Only administrators may delete add-on tools'
        expected_non_admin_uninstall_errors = f'{new_uninstall_err_msg}|{old_uninstall_err_msg}'
        with pytest.raises(Exception, match=expected_non_admin_uninstall_errors):
            addons.uninstall(df, session=spy.session)

    finally:
        _cleanup_installed_plugins(new_name)


@pytest.mark.system
def test_install_one_tool_in_development():
    items_api = ItemsApi(spy.session.client)

    new_name = f"test_install_one_tool{_common.new_placeholder_guid()}"
    my_tool = {"Name": new_name,
               "Description": "My new tool",
               "Target URL": "https://www.google.com",
               "Groups": ["Everyone"]}

    try:
        new_install_err_msg = 'Non-administrators may only install Add-on Tools in Development Mode'
        old_install_err_msg = 'Only administrators may create add-on tools'
        expected_non_admin_install_errors = f'{new_install_err_msg}|{old_install_err_msg}'
        with pytest.raises(Exception, match=expected_non_admin_install_errors):
            addons.install(my_tool)

        permissions_err_msg = "In-Development add-ons can't assign Groups or Users permissions"
        with pytest.raises(Exception, match=permissions_err_msg):
            addons.install(my_tool, in_development=True)

        my_tool = {"Name": new_name,
                   "Description": "My new tool",
                   "Target URL": "https://www.google.com"}

        df = addons.install(my_tool, in_development=True)
        assert items_api.get_item_and_all_properties(id=df['ID'].values[0])

        update_tool_error = 'You can update the existing tool with `update_tool=True`'
        change_in_development_status_error = 'Add-on Tools may not change their In Development setting after creation.'

        with pytest.raises(Exception, match=update_tool_error):
            addons.install(my_tool, in_development=True)

        df2 = addons.install(my_tool, update_tool=True, in_development=True)
        assert df['ID'].values[0] == df2['ID'].values[0]

        with pytest.raises(Exception, match=change_in_development_status_error):
            addons.install(my_tool, update_tool=True, in_development=False)

        df3 = addons.uninstall(df)
        assert df['ID'].values[0] == df3['ID'].values[0]

    finally:
        _cleanup_installed_plugins(new_name)


@pytest.mark.system
def test_install_one_tool_with_existing_query_params():
    admin_session = test_common.get_session(Sessions.admin)
    items_api = ItemsApi(admin_session.client)

    new_name = f"test_install_one_tool_with_existing_query_params{_common.new_placeholder_guid()}"
    my_tool = {"Name": new_name,
               "Description": "My new tool",
               "Target URL": "https://www.google.com?previous=123",
               "Icon": "fa fa-icon"}

    try:
        df = addons.install(my_tool, include_workbook_parameters=True, session=admin_session)
        tool = items_api.get_item_and_all_properties(id=df['ID'].values[0])
        workbook_params = f'&workbookId={{workbookId}}&worksheetId={{worksheetId}}&workstepId={{' \
                          f'workstepId}}&seeqVersion={{seeqVersion}}'
        target_url = f"{my_tool['Target URL']}{workbook_params}"
        assert target_url == [x for x in tool.properties if x.name == 'Target URL'][0].value

    finally:
        _cleanup_installed_plugins(new_name)


@pytest.mark.system
def test_install_one_tool_with_workbook_params():
    admin_session = test_common.get_session(Sessions.admin)
    items_api = ItemsApi(admin_session.client)

    new_name = f"test_install_one_tool_with_workbook_params{_common.new_placeholder_guid()}"
    my_tool = {"Name": new_name,
               "Description": "My new tool",
               "Target URL": "https://www.google.com?",
               "Icon": "fa fa-icon"}

    try:
        df = addons.install(my_tool, include_workbook_parameters=True, session=admin_session)
        tool = items_api.get_item_and_all_properties(id=df['ID'].values[0])
        workbook_params = f'workbookId={{workbookId}}&worksheetId={{worksheetId}}&workstepId={{' \
                          f'workstepId}}&seeqVersion={{seeqVersion}}'
        target_url = f"{my_tool['Target URL']}{workbook_params}"
        assert target_url == [x for x in tool.properties if x.name == 'Target URL'][0].value

    finally:
        _cleanup_installed_plugins(new_name)


@pytest.mark.system
def test_install_multiple_tools():
    admin_session = test_common.get_session(Sessions.admin)
    items_api = ItemsApi(admin_session.client)

    user_id = UsersApi(admin_session.client).get_me().id
    projects_api = ProjectsApi(admin_session.client)
    sdl_project1 = projects_api.create_project(body=dict(name='test project p1', ownerId=user_id))
    sdl_project2 = projects_api.create_project(body=dict(name='test project p2', ownerId=user_id))

    name = f'test_install_multiple_tools{_common.new_placeholder_guid()}'
    new_tools = [
        {
            "Name": f'My {name}',
            "Description": "this is an awesome tool",
            "Target URL": f"https://www.my.seeq.com/data-lab/{sdl_project1.id}/addon/my_tool.ipynb",
            "Icon": "fa fa-bell"},
        {
            "Name": f'Your {name}',
            "Description": "this is an awesome tool",
            "Target URL": f"https://www.my.seeq.com/data-lab/{sdl_project2.id}/addon/tool2.ipynb",
            "Icon": "fa fa-bars"}]

    try:
        df = addons.install(new_tools, errors='raise', session=admin_session)
        for idd in df['ID'].values:
            assert items_api.get_item_and_all_properties(id=idd)

        for tool in new_tools:
            assert tool['Name'] in tool_names(admin_session)

    finally:
        _cleanup_installed_plugins(f'*{name}')


@pytest.mark.system
def test_update_tool_false():
    admin_session = test_common.get_session(Sessions.admin)
    items_api = ItemsApi(admin_session.client)

    name = f"test_update_tool_false{_common.new_placeholder_guid()}"
    my_tool = {"Name": name,
               "Description": "My new tool",
               "Target URL": "https://www.google.com",
               "Icon": "fa fa-icon"}

    try:
        df = addons.install(my_tool, update_tool=False, session=admin_session)
        assert items_api.get_item_and_all_properties(id=df['ID'].values[0])
        assert name in tool_names(admin_session)
        with pytest.raises(SPyException,
                           match=f'Add-on tool with name "{name}" already exists. '
                                 f'You can update the existing tool with `update_tool=True`'):
            addons.install(my_tool, update_tool=False, session=admin_session)

    finally:
        _cleanup_installed_plugins(name)


@pytest.mark.system
def test_update_tools_from_df():
    admin_session = test_common.get_session(Sessions.admin)
    projects_api = ProjectsApi(admin_session.client)
    users_api = UsersApi(admin_session.client)

    user_id = users_api.get_me().id
    sdl_project1 = projects_api.create_project(body=dict(name='test project p3', ownerId=user_id))
    sdl_project2 = projects_api.create_project(body=dict(name='test project p4', ownerId=user_id))

    name = f'test_update_tools_from_df{_common.new_placeholder_guid()}'
    new_tools = [
        {
            "Name": f'My {name}',
            "Description": "this is an awesome tool",
            "Target URL": f"https://www.my.seeq.com/data-lab/{sdl_project1.id}/addon/my_tool.ipynb",
            "Icon": "fa fa-bell",
            "Link Type": "window"},
        {
            "Name": f'Your {name}',
            "Description": "this is an awesome tool",
            "Target URL": f"https://www.my.seeq.com/data-lab/{sdl_project2.id}/addon/tool2.ipynb",
            "Icon": "fa fa-bars",
            "Link Type": "window"}]

    try:
        df = addons.install(new_tools, session=admin_session)

        searched_tools = addons.search(df, session=admin_session, errors='catalog')

        for _, tool in searched_tools.iterrows():
            assert tool['Link Type'] == 'window'

        searched_tools["Link Type"] = ['tab'] * len(searched_tools)
        addons.install(searched_tools, update_tool=True, session=admin_session)

        searched_tools = addons.search(df, session=admin_session, errors='catalog')

        for _, tool in searched_tools.iterrows():
            assert tool['Link Type'] == 'tab'

    finally:
        _cleanup_installed_plugins(f'*{name}')


@pytest.mark.system
def test_change_permissions():
    admin_session = test_common.get_session(Sessions.admin)
    projects_api = ProjectsApi(admin_session.client)
    users_api = UsersApi(admin_session.client)
    group = _create_testing_group()

    user_id = users_api.get_me().id
    sdl_project1 = projects_api.create_project(body=dict(name='test project p5', ownerId=user_id))
    sdl_project2 = projects_api.create_project(body=dict(name='test project p6', ownerId=user_id))
    name = f'test_change_permissions{_common.new_placeholder_guid()}'
    new_tools = [
        {
            "Name": f'My {name}',
            "Description": "this is an awesome tool",
            "Target URL": f"https://www.my.seeq.com/data-lab/{sdl_project1.id}/addon/my_tool.ipynb",
            "Icon": "fa fa-bell",
            "Link Type": "window",
            "Groups": ["testers"]},
        {
            "Name": f'Your {name}',
            "Description": "this is an awesome tool",
            "Target URL": f"https://www.my.seeq.com/data-lab/{sdl_project2.id}/addon/tool2.ipynb",
            "Icon": "fa fa-bars",
            "Link Type": "window",
            "Groups": ["testers"]}]

    try:
        addons.install(new_tools, session=admin_session)

        search_df = addons.search({"Name": f'My test_change_permissions'}, session=admin_session, errors='catalog')

        # Pass only the ID to make sure it's not grabbing other properties from the search_df
        idd = search_df['ID'][0]

        # Remove group permissions
        addons.install(pd.Series({"ID": idd, "Groups": []}), update_permissions=True, update_tool=False,
                       session=admin_session)

        permissions = _permissions.get_addon_permissions(idd, session=admin_session)
        assert len(permissions['Groups']) == 0
        assert len(permissions["Users"]) == 0

        addons.install({"ID": idd, "Groups": [group.name]}, update_permissions=True, session=admin_session)

    finally:
        _cleanup_installed_plugins(f'*{name}')


@pytest.mark.system
def test_update_tools_and_update_permissions_passing_id():
    _create_testing_group()

    projects_api = ProjectsApi(spy.session.client)
    users_api = UsersApi(spy.session.client)

    user_id = users_api.get_me().id
    sdl_project1 = projects_api.create_project(body=dict(name='test project p7', ownerId=user_id))
    sdl_project2 = projects_api.create_project(body=dict(name='test project p8', ownerId=user_id))

    name = f'test_update_tools_and_update_permissions_passing_id{_common.new_placeholder_guid()}'
    new_tools = [
        {
            "Name": f'My {name}',
            "Description": "this is an awesome tool",
            "Target URL": f"https://www.my.seeq.com/data-lab/{sdl_project1.id}/addon/my_tool.ipynb",
            "Icon": "fa fa-bell",
            "Link Type": "window",
            "Groups": ["Everyone"]
        },
        {
            "Name": f'Your {name}',
            "Description": "this is an awesome tool",
            "Target URL": f"https://www.my.seeq.com/data-lab/{sdl_project2.id}/addon/tool2.ipynb",
            "Icon": "fa fa-bars",
            "Link Type": "window"}]

    admin_session = test_common.get_session(Sessions.admin)
    try:
        df = addons.install(new_tools, session=admin_session)

        searched_tools = addons.search(df, errors='catalog')

        for _, tool in searched_tools.iterrows():
            assert tool['Link Type'] == 'window'

        searched_tools["Link Type"] = ['tab'] * len(searched_tools)
        searched_tools['Groups'] = [['testers']] * len(searched_tools)
        addons.install(searched_tools, update_tool=True, update_permissions=True, session=admin_session)

        searched_tools = addons.search(df, errors='catalog')

        for _, tool in searched_tools.iterrows():
            assert tool['Link Type'] == 'tab'
            assert 'testers' in tool['Groups']
            assert 'Everyone' not in tool['Groups']

    finally:
        _cleanup_installed_plugins(f'*{name}')


@pytest.mark.system
def test_mix_update_tool_and_new_tool():
    admin_session = test_common.get_session(Sessions.admin)
    items_api = ItemsApi(admin_session.client)

    name = f"test_mix_update_tool_and_new_tool{_common.new_placeholder_guid()}"
    my_tool = {"Name": name,
               "Description": "an awesome tool",
               "Target URL": "https://www.google.com?",
               "Icon": "fa fa-icon"}

    try:
        df = addons.install(my_tool, include_workbook_parameters=True, update_tool=True, session=admin_session)
        previous_id = df['ID'].values[0]
        tool = items_api.get_item_and_all_properties(id=previous_id)
        assert my_tool['Description'] == [x for x in tool.properties if x.name == 'Description'][0].value

        new_install = [{"Name": name,
                        "Description": "an awesome tool updated",
                        "Target URL": "https://www.google.com?",
                        "Icon": "fa fa-icon"},
                       {"Name": f'New {name}',
                        "Description": "this is new",
                        "Target URL": "https://www.google.com?",
                        "Icon": "fa fa-icon"}]

        df = addons.install(new_install, include_workbook_parameters=True, update_tool=True, session=admin_session)
        tool = items_api.get_item_and_all_properties(id=previous_id)
        tool2 = items_api.get_item_and_all_properties(id=df['ID'][1])
        assert new_install[0]['Description'] == [x for x in tool.properties if x.name == 'Description'][0].value
        assert new_install[1]['Description'] == [x for x in tool2.properties if x.name == 'Description'][0].value
        assert len(df) == 2
        assert df['Result'][0] == 'Updated'
        assert df['Result'][1] == 'Installed'

    finally:
        _cleanup_installed_plugins(f'*{name}')


@pytest.mark.system
def test_update_duplicated_tool():
    admin_session = test_common.get_session(Sessions.admin)
    system_api = SystemApi(admin_session.client)
    name = f"test_update_duplicated_tool{_common.new_placeholder_guid()}"

    new_tool_config = dict(
        name=name,
        description="duplicated test",
        iconClass='fa fa-bars',
        targetUrl=f'https://www.seeq.com',
        linkType="window",
        windowDetails="toolbar=0,height=600,width=600",
        sortKey="a",
        reuseWindow=True,
    )

    try:
        tool1_id = system_api.create_add_on_tool(body=new_tool_config).id
        system_api.create_add_on_tool(body=new_tool_config)

        my_tool = {"Name": name,
                   "Description": "an awesome tool",
                   "Target URL": "https://www.google.com?",
                   "Icon": "fa fa-icon"}

        # addons.install fails to update if update_tool=False
        with pytest.raises(SPyException,
                           match=f'Add-on tool with name "{name}" already exists. '
                                 f'You can update the existing tool with `update_tool=True`'):
            addons.install(my_tool, include_workbook_parameters=True, update_tool=False, session=admin_session)

        # addons.install fails to update even with update_tool=True since there are two tools with the same name
        with pytest.raises(SPyException,
                           match=f'There exists 2 tools with the name "{name}". '
                                 f'You can supply the ID to avoid ambiguity and modify only the intended tool'):
            addons.install(my_tool, include_workbook_parameters=True, update_tool=True, session=admin_session)

        my_tool = {"ID": tool1_id,
                   "Name": name,
                   "Description": "an awesome tool",
                   "Target URL": "https://www.google.com?",
                   "Icon": "fa fa-icon"}

        # passing the ID resolves the ambiguity and allows the update
        df = addons.install(my_tool, include_workbook_parameters=True, update_tool=True, session=admin_session)

        assert len(df) == 1
        assert df['Result'][0] == 'Updated'

    finally:
        _cleanup_installed_plugins(name)
