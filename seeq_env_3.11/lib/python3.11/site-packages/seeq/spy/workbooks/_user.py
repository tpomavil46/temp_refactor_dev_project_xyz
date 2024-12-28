from __future__ import annotations

from functools import lru_cache
from typing import List, Optional, Dict, Union

import pandas as pd

from seeq.sdk import *
from seeq.spy import _common, _metadata, _compatibility
from seeq.spy._errors import *
from seeq.spy._redaction import request_safely, safely
from seeq.spy._session import Session
from seeq.spy._status import Status
from seeq.spy.workbooks._data import StoredItem
from seeq.spy.workbooks._item import Item, ItemMap

ORIGINAL_OWNER = '__original__'
FORCE_ME_AS_OWNER = '__me__'


class ItemWithOwnerAndAcl(Item):
    def decide_owner(self, session: Session, datasource_maps, item_map: ItemMap, *, owner=None, current_owner_id=None):
        requires_admin = True
        if _common.is_guid(owner):
            owner_id = owner
        elif owner is None:
            requires_admin = False
            if current_owner_id is None:
                owner_id = session.user.id
            else:
                owner_id = current_owner_id
        elif owner == ORIGINAL_OWNER:
            if _common.get(self, 'Owner'):
                owner_id = Identity.find_identity(session, self['Owner'], datasource_maps=datasource_maps,
                                                  item_map=item_map)
            else:
                # There is no original owner so make it the current user as a placeholder
                owner_id = session.user.id
        elif owner == FORCE_ME_AS_OWNER:
            owner_id = session.user.id
        else:
            raise SPyValueError('Invalid owner: %s' % owner)

        if current_owner_id is None or current_owner_id == owner_id:
            requires_admin = False

        if requires_admin and not session.user.is_admin:
            raise SPyRuntimeError("Logged in user must be an admin as a result of owner='%s'" % owner)

        return owner_id

    def _pull_owner_and_acl(self, session: Session, owner: IdentityPreviewV1, status: Status):
        items_api = ItemsApi(session.client)

        if owner is not None:
            self['Owner'] = {'ID': owner.id}
            self['Owner']['Redacted'] = owner.is_redacted
            if not owner.is_redacted:
                owner_details = User.pull(owner.id, session=session, status=status)
                if owner_details is not None:
                    self['Owner'] = owner_details.definition

        @request_safely(action_description=f'get access control list for Item {self["ID"]}', status=status)
        def _request_acl(_id):
            acl_output = items_api.get_access_control(id=_id)  # type: AclOutputV1
            access_control = list()

            for ace_output in acl_output.entries:  # type: AceOutputV1
                ace_dict = Item.dict_via_attribute_map(ace_output, {
                    'created_at': 'Created At',
                    'id': 'ID',
                    'role': 'Role'
                })

                ace_dict['Origin'] = ace_output.origin.id if ace_output.origin is not None else None
                ace_dict['Permissions'] = ItemWithOwnerAndAcl._permissions_to_dict(ace_output.permissions)
                ace_dict['Redacted'] = ace_output.identity.is_redacted

                if not ace_output.identity.is_redacted:
                    if ace_output.identity.type == 'User':
                        identity = User.pull(ace_output.identity.id, session=session, status=status)
                    else:
                        identity = UserGroup.pull(ace_output.identity.id, session=session, status=status)

                    if identity is not None:
                        ace_dict['Identity'] = identity.definition
                access_control.append(ace_dict)
            return access_control

        maybe_acl = _request_acl(self['ID'])
        self.definition['Access Control'] = list() if maybe_acl is None else maybe_acl

    @staticmethod
    def _push_owner_and_location(session: Session, item_output, owner_id, folder_id, status):
        items_api = ItemsApi(session.client)
        folders_api = FoldersApi(session.client)

        if item_output.owner.id != owner_id:
            safely(lambda: items_api.change_owner(item_id=item_output.id, new_owner_id=owner_id),
                   action_description=f'change owner of {item_output.id} to {owner_id}',
                   status=status)

        if folder_id:
            safely(lambda: folders_api.move_item_to_folder(folder_id=folder_id, item_id=item_output.id),
                   action_description=f'change Folder of {item_output.id} to {folder_id}',
                   status=status)

    def _push_acl(self, session: Session, pushed_id, datasource_maps, item_map: ItemMap, access_control):
        replace, strict = ItemWithOwnerAndAcl.parse_access_control_str(access_control)

        if 'Access Control' not in self:
            return

        acl_df = pd.DataFrame({
            'ID': pd.Series(dtype=str),
            'Read': pd.Series(dtype=bool),
            'Write': pd.Series(dtype=bool),
            'Manage': pd.Series(dtype=bool)
        })

        for acl_to_push in self['Access Control']:
            try:
                identity_id = Identity.find_identity(session, acl_to_push['Identity'], datasource_maps, item_map)
            except SPyDependencyNotFound:
                if strict:
                    raise

                continue

            acl_df = pd.concat([acl_df, pd.DataFrame([{
                'ID': identity_id,
                'Read': acl_to_push['Permissions']['Read'],
                'Write': acl_to_push['Permissions']['Write'],
                'Manage': acl_to_push['Permissions']['Manage']
            }])], ignore_index=True)

        _metadata.push_access_control(session, pushed_id, acl_df, replace)

    @staticmethod
    def parse_access_control_str(access_control):
        replace = False
        strict = False
        if access_control:
            treatment_parts = access_control.split(',')
            for treatment_part in treatment_parts:
                if treatment_part == 'add':
                    replace = False
                elif treatment_part == 'replace':
                    replace = True
                elif treatment_part == 'loose':
                    strict = False
                elif treatment_part == 'strict':
                    strict = True
                else:
                    raise SPyValueError("access_control argument must be 'add' or 'replace' comma 'loose' or "
                                        "'strict'. For example: replace,strict")
        return replace, strict

    @staticmethod
    def _dict_to_permissions(d):
        return PermissionsV1(
            manage=_common.get(d, 'Manage', False),
            read=_common.get(d, 'Read', False),
            write=_common.get(d, 'Write', False)
        )

    @staticmethod
    def _permissions_to_dict(permissions):
        """
        :type permissions: PermissionsV1
        """
        return {
            'Read': permissions.read,
            'Write': permissions.write,
            'Manage': permissions.manage
        }

    def _pull_ancestors(self, session: Session, ancestors: List[ItemPreviewV1]):
        if ancestors is None:
            return

        self.definition['Ancestors'] = \
            [(ancestor.id if ancestor.id is not None else f'__{ancestor.name}__')
             for ancestor in ancestors]

    @staticmethod
    def should_use_full_ancestry(session: Session):
        # We generally want the true ancestors if at all possible, because this helps us transfer workbooks/folders
        # accurately between servers.
        return session.user.is_admin and not session.options.wants_compatibility_with(189)

    @staticmethod
    def _find_auth_provider(session: Session, datasource_class, datasource_id) -> Optional[DatasourceOutputV1]:
        for auth_provider in session.auth_providers:
            if auth_provider.datasource_class == datasource_class and auth_provider.datasource_id == datasource_id:
                return auth_provider

        return None

    def _scrape_auth_datasources(self, session: Session) -> Dict[str, DatasourceOutputV1]:
        referenced_datasources: Dict[str, DatasourceOutputV1] = dict()

        def _scrape_auth_datasource(d, key):
            if key in d and 'Datasource Class' in d[key] and 'Datasource ID' in d[key]:
                auth_provider = ItemWithOwnerAndAcl._find_auth_provider(
                    session, d[key]['Datasource Class'], d[key]['Datasource ID'])
                if auth_provider:
                    referenced_datasources[auth_provider.id] = auth_provider

        _scrape_auth_datasource(self, 'Owner')
        if 'Access Control' in self:
            for acl in self['Access Control']:
                _scrape_auth_datasource(acl, 'Identity')

        return referenced_datasources


class Identity(StoredItem):
    @staticmethod
    def find_identity(session: Session, identity_dict, datasource_maps, item_map: ItemMap) -> str:
        if _common.get(identity_dict, 'ID') in item_map:
            return item_map[identity_dict['ID']]

        if _common.get(identity_dict, 'Type') == 'User':
            identity = User(identity_dict)
        else:
            identity = UserGroup(identity_dict)

        pushed_identity = identity._lookup(session, datasource_maps=datasource_maps, datasource_output=None,
                                           item_map=item_map)

        return pushed_identity.id

    def pull_datasource(self, session: Session, identity: Union[UserOutputV1, UserGroupOutputV1, IdentityPreviewV1]):
        datasource_name = (identity.datasource.name if isinstance(identity, IdentityPreviewV1) else
                           identity.datasource_name)

        # noinspection PyBroadException
        try:
            for auth_provider in session.auth_providers:  # type: DatasourceOutputV1
                if auth_provider.name == datasource_name:
                    self['Datasource Class'] = auth_provider.datasource_class
                    self['Datasource ID'] = auth_provider.datasource_id
                    self['Datasource Name'] = auth_provider.name
                    break

        except Exception:
            # If we can't get extra data on the user, that's OK
            pass

    @staticmethod
    @lru_cache()
    def pull(item_id, *, allowed_types=None, status: Status = None, session: Optional[Session] = None):
        try:
            user = User.pull(item_id, allowed_types=allowed_types, status=status, session=session)
            if user is not None:
                return user
        except ApiException as e:
            if e.status not in (400, 404):
                raise

        try:
            user_group = UserGroup.pull(item_id, allowed_types=allowed_types, status=status, session=session)
            if user_group is not None:
                return user_group
        except ApiException as e:
            if e.status not in (400, 404):
                raise

        return None


class User(Identity):
    @staticmethod
    @lru_cache()
    def pull(item_id, *, allowed_types=None, status: Status = None, session: Optional[Session] = None):
        session = Session.validate(session)

        user_output = safely(lambda: _compatibility.get_user(session.client, id=item_id, include_groups=False),
                             action_description=f'get User {item_id}',
                             status=status)  # type: UserOutputV1
        if user_output is None:
            return None

        item = User({
            'ID': user_output.id,
            'Type': user_output.type,
            'Name': user_output.name,
            'Username': user_output.username,
            'First Name': user_output.first_name,
            'Last Name': user_output.last_name,
            'Email': user_output.email,
            'Is Admin': user_output.is_admin
        })

        item.pull_datasource(session, user_output)
        return item


class UserGroup(Identity):
    @staticmethod
    @lru_cache()
    def pull(item_id, *, allowed_types=None, status: Status = None, session: Optional[Session] = None):
        session = Session.validate(session)
        usergroup_output = safely(
            lambda: _compatibility.get_user_group(session.client, user_group_id=item_id, include_members=False),
            action_description=f'get User Group {item_id}',
            status=status)  # type: UserGroupOutputV1
        if usergroup_output is None:
            return None

        item = UserGroup({
            'ID': usergroup_output.id,
            'Type': usergroup_output.type,
            'Name': usergroup_output.name
        })

        item.pull_datasource(session, usergroup_output)
        return item

    @staticmethod
    def from_identity(identity: IdentityPreviewV1, *, session: Optional[Session] = None):
        session = Session.validate(session)

        item = UserGroup({
            'ID': identity.id,
            'Type': identity.type,
            'Name': identity.name
        })

        item.pull_datasource(session, identity)
        return item
