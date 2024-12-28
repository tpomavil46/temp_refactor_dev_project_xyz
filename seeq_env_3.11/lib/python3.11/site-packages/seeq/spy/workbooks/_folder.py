from __future__ import annotations

from typing import List, Optional

from seeq.sdk import *
from seeq.spy import _common
from seeq.spy._errors import *
from seeq.spy._redaction import safely, request_safely
from seeq.spy._session import Session
from seeq.spy._status import Status
from seeq.spy.workbooks._item import Item, ItemMap
from seeq.spy.workbooks._user import ItemWithOwnerAndAcl

SHARED = '__Shared__'
CORPORATE = '__Corporate__'
ALL = '__All__'
USERS = '__Users__'
MY_FOLDER = '__My_Folder__'
ORIGINAL_FOLDER = '__Original_Folder__'
PUBLIC = '__Public__'

SYNTHETIC_FOLDERS = [MY_FOLDER, SHARED, CORPORATE, ALL, USERS, PUBLIC]


def massage_ancestors(session: Session, item: Item):
    if not session.corporate_folder:
        return

    if len(item.definition['Ancestors']) > 0 and item.definition['Ancestors'][0] == session.corporate_folder.id:
        # Why replace the Corporate folder ID with the CORPORATE string token? The user doesn't want to have to
        # deal with the ID of the corporate folder, especially when importing/exporting workbooks from one server to
        # another.
        item.definition['Ancestors'][0] = CORPORATE


def synthetic_folder_to_content_filter(synthetic_folder):
    # If synthetic_folder is MY_FOLDER, the corresponding content_filter would be 'OWNER', otherwise,  it would be the
    #  synthetic_folder formatted for use by the Folders API
    content_filter = 'OWNER' if synthetic_folder == MY_FOLDER else synthetic_folder.replace('__', '').upper()
    return content_filter


class Folder(ItemWithOwnerAndAcl):
    @staticmethod
    def pull(item_id, *, include_access_control=True,
             status: Status = None, session: Optional[Session] = None) -> Optional[Folder]:
        session = Session.validate(session)
        status = Status.validate(status, session)
        folders_api = FoldersApi(session.client)

        item_output = safely(lambda: Item._get_item_output(session, item_id),
                             action_description=f'pull Item {item_id}', status=status)
        if item_output is None:
            return None

        @request_safely(action_description=f'get Folder {item_id}', status=status)
        def _get_folder() -> FolderOutputV1:
            try:
                return folders_api.get_folder(
                    folder_id=item_id, full_ancestry=ItemWithOwnerAndAcl.should_use_full_ancestry(session))
            except TypeError:
                # full_ancestry is not supported in this version of Seeq
                return folders_api.get_folder(folder_id=item_id)

        folder_output = _get_folder()
        if folder_output is None:
            return None

        definition = Item._dict_from_item_output(item_output)
        folder = Folder(definition, provenance=Item.PULL)
        if include_access_control:
            folder._pull_owner_and_acl(session, folder_output.owner, status)
        folder._pull_ancestors(session, folder_output.ancestors)

        return folder

    def _pull_ancestors(self, session: Session, ancestors: List[ItemPreviewV1]):
        super()._pull_ancestors(session, ancestors)
        massage_ancestors(session, self)

    def _find_by_name(self, session: Session, folder_id, status: Status):
        folders_api = FoldersApi(session.client)

        offset = 0
        limit = session.options.search_page_size
        while True:
            if folder_id and folder_id != _common.PATH_ROOT:
                folders = safely(lambda: folders_api.get_folders(filter='owner' if folder_id is None else 'all',
                                                                 folder_id=folder_id,
                                                                 offset=offset,
                                                                 limit=limit),
                                 action_description=f'get "owner" folders within {folder_id}',
                                 additional_errors=[400],
                                 status=status)  # type: WorkbenchItemOutputListV1
            else:
                folders = folders_api.get_folders(filter='owner',
                                                  offset=offset,
                                                  limit=limit)  # type: WorkbenchItemOutputListV1

            content_dict = {content.name.lower(): content for content in folders.content}
            if self.name.lower() in content_dict and content_dict[self.name.lower()].type == 'Folder':
                return content_dict[self.name.lower()]

            if len(folders.content) < limit:
                break

            offset += limit

        return None

    def push(self, session: Session, parent_folder_id, datasource_maps, datasource_output, item_map: ItemMap, *,
             owner=None, label=None, access_control=None, status=None):
        items_api = ItemsApi(session.client)
        folders_api = FoldersApi(session.client)

        if _common.get(self, 'Unmodifiable', False):
            # Unmodifiable are system-managed folders, including the Corporate folder and individual user folders.
            # If we got this far, we have to assume that this is one of the user folders. We can't "push" to them and
            # we can't change their ACLs. But we need to map them appropriately. Unfortunately, we can only do this
            # if we are logged in as an admin.
            if not session.user.is_admin:
                return None

            owner_id = self.decide_owner(session, datasource_maps, item_map, owner=owner)
            home_folder = session.get_user_folder(owner_id)
            if home_folder is None:
                raise SPyRuntimeError(f'Could not find user folder for {self["Owner"]}')
            item_map[self.id] = home_folder.id
            return home_folder

        folder_item = self.find_me(session, label, datasource_output)

        if folder_item is None and self.provenance == Item.CONSTRUCTOR:
            folder_item = self._find_by_name(session, parent_folder_id, status)

        if not folder_item:
            folder_input = FolderInputV1()
            folder_input.name = self['Name']
            if 'Description' in self:
                folder_input.description = self['Description']
            folder_input.owner_id = self.decide_owner(session, datasource_maps, item_map, owner=owner)
            folder_input.parent_folder_id = parent_folder_id if parent_folder_id != _common.PATH_ROOT else None

            folder_output = safely(lambda: folders_api.create_folder(body=folder_input),
                                   action_description=f'create Folder {folder_input.name}',
                                   status=status)  # type: FolderOutputV1
            if folder_output is None:
                return None
            props = [
                ScalarPropertyV1(name='Datasource Class', value=datasource_output.datasource_class),
                ScalarPropertyV1(name='Datasource ID', value=datasource_output.datasource_id),
                ScalarPropertyV1(name='Data ID', value=self._construct_data_id(label))]
            safely(lambda: items_api.set_properties(id=folder_output.id, body=props),
                   action_description=f'set common properties for new Folder {folder_output.id}',
                   status=status)
        else:
            folder_output = safely(lambda: folders_api.get_folder(folder_id=folder_item.id),
                                   action_description=f'get Folder {folder_item.id}',
                                   status=status)  # type: FolderOutputV1
            if folder_output is None:
                return None

            props = [ScalarPropertyV1(name='Name', value=self['Name'])]
            if 'Description' in self:
                props.append(ScalarPropertyV1(name='Description', value=self['Description']))

            # If the folder happens to be archived, un-archive it. If you're pushing a new copy it seems likely
            # you're intending to revive it.
            props.append(ScalarPropertyV1(name='Archived', value=False))

            safely(lambda: items_api.set_properties(id=folder_output.id, body=props),
                   action_description=f'set common properties for existing Folder {folder_output.id}',
                   status=status)

            owner_id = self.decide_owner(session, datasource_maps, item_map, owner=owner,
                                         current_owner_id=folder_output.owner.id)

            self._push_owner_and_location(session, folder_output, owner_id, parent_folder_id, status)

        item_map[self.id] = folder_output.id

        if access_control:
            self._push_acl(session, folder_output.id, datasource_maps, item_map, access_control)

        return folder_output
