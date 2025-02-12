from __future__ import annotations

from seeq.spy import _login, _url, acl
from seeq.spy._errors import SPyRuntimeError
from seeq.spy._session import Session


def _add_addon_permissions(identity_id: str, tool_id: str, session: Session):
    # Add permissions to the add-on tool
    ace_input = {'ID': identity_id, 'Read': True}
    acl.push(tool_id, ace_input, quiet=True, session=session)


def _add_datalab_permissions(identity_id: str, data_lab_project_id: str, session: Session):
    if not data_lab_project_id:
        return
    ace_input = {'ID': identity_id, 'Read': True}
    acl.push(data_lab_project_id, ace_input, quiet=True, session=session)


def _remove_addon_permissions(tool_id: str, session: Session):
    acl.push(tool_id, [], replace=True, quiet=True, session=session)


def get_addon_permissions(tool_id: str, session: Session):
    permissions = acl.pull(tool_id, include_my_effective_permissions=True, quiet=True, session=session)
    entries = permissions.at[0, 'Access Control']

    return {"Groups": list(entries.loc[entries['Type'] == 'UserGroup']['Name']),
            "Users": list(entries.loc[entries['Type'] == 'User']['Name'])}


def set_permissions(session: Session, new_tool_config: dict, tool_id: str):
    try:
        data_lab_project_id = _url.get_data_lab_project_id_from_url(new_tool_config["targetUrl"])
    except SPyRuntimeError:
        data_lab_project_id = None

    groups = new_tool_config["permissions"]["groups"]
    users = new_tool_config["permissions"]["users"]

    # revoke previous permissions
    _remove_addon_permissions(tool_id, session)

    for group_name in groups:
        group = _login.find_group(session, group_name, exact_match=True)
        if group:
            # assign group permissions to add-on tool and data lab project
            _add_addon_permissions(group.id, tool_id, session)
            if data_lab_project_id:
                _add_datalab_permissions(group.id, data_lab_project_id, session)

    for user_name in users:
        _user = _login.find_user(session, user_name, exact_match=True)
        if _user:
            # assign user permissions to add-on tool and data lab project
            _add_addon_permissions(_user.id, tool_id, session)
            if data_lab_project_id:
                _add_datalab_permissions(_user.id, data_lab_project_id, session)
