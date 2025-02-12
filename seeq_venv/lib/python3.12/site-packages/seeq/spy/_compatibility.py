from seeq.sdk import ApiClient
from seeq.sdk.api import UsersApi, UserGroupsApi, FormulasApi
from seeq.sdk.models import PutScalarsInputV1, UserOutputV1, UserGroupOutputV1, AddOnToolInputV1
from seeq.spy import _login


def is_continuation_token_used() -> bool:
    """
    Check if compute API endpoints should use the continuation_token field for pagination. On false, should use the
    old start key increment and deduplicate capsules scheme.

    Returns
    -------
    True if the SDK version is equal to or greater than 63.
    """
    return _login.is_sdk_module_version_at_least(63)


def is_force_calculated_scalars_available() -> bool:
    """
    Check if the force_calculated_scalars parameter is available on the PutScalars endpoint.

    Returns
    -------
    True if the force_calculated_scalars parameter is available on the PutScalars endpoint.
    """
    return 'force_calculated_scalars' in PutScalarsInputV1.attribute_map


def is_launch_location_available() -> bool:
    """
    Check if the launch_location parameter is available on the AddOnTools endpoint.

    Returns
    -------
    True if the launch_location parameter is available on the AddOnTools endpoint.
    """
    return 'launch_location' in AddOnToolInputV1.attribute_map


def is_compile_formula_and_parameters_available() -> bool:
    """
    Check if the compile_formula_and_parameters method is available in the FormulasApi.

    Returns
    -------
    True if the compile_formula_and_parameters method is available in the FormulasApi.
    """
    return hasattr(FormulasApi, 'compile_formula_and_parameters')


def get_user(client: ApiClient, id: str, *, include_groups: bool = True) -> UserOutputV1:
    """
    Get a user

    Parameters
    ----------
    client : ApiClient
        The api_client to use for the SDK call.
    id : str
        ID of the user to get (required)
    include_groups : bool
        Include the groups of which the user is a member. Note that, depending on the version of Seeq, this parameter
        may not be supported.
    """
    users_api = UsersApi(client)
    try:
        return users_api.get_user(id=id, include_groups=include_groups)
    except TypeError:
        return users_api.get_user(id=id)


def get_user_group(client: ApiClient, user_group_id: str, *, include_members: bool = True) -> UserGroupOutputV1:
    """
    Get a user group.

    client : ApiClient
        The session to use for the request (required)
    user_group_id : str
        ID of the user group to get (required)
    include_members : bool
        Include the members (users) of the user group. Note that, depending on the version of Seeq, this parameter may
        not be supported.
    """
    usergroups_api = UserGroupsApi(client)
    try:
        return usergroups_api.get_user_group(user_group_id=user_group_id, include_members=include_members)
    except TypeError:
        return usergroups_api.get_user_group(user_group_id=user_group_id)
