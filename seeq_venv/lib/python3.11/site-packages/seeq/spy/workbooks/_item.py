from __future__ import annotations

import copy
import hashlib
import json
import re
import urllib
from enum import Enum, auto
from typing import List, Optional, Dict, Union

from seeq.sdk import *
from seeq.spy import _common, _login, _metadata, _search
from seeq.spy._errors import *
from seeq.spy._redaction import safely
from seeq.spy._session import Session
from seeq.spy._status import Status
from seeq.spy.workbooks._item_map import ItemMap


class ItemExists(Enum):
    YES = auto()
    NO = auto()
    MAYBE = auto()


class Item:
    """
    The base class of all objects in the spy.workbooks module that represent
    items in Seeq. Each item is described by the dictionary stored at its
    `dictionary` property.
    """

    _definition: dict
    _datasource: Optional[DatasourcePreviewV1]
    _provenance: str

    available_types = dict()

    CONSTRUCTOR = 'CONSTRUCTOR'
    PULL = 'PULL'
    LOAD = 'LOAD'
    TEMPLATE = 'TEMPLATE'
    ROOT = '<root>'

    def __init__(self, definition=None, *, provenance=None):
        if isinstance(definition, Item):
            definition = definition.definition_dict

        _common.validate_argument_types([(definition, 'definition', dict)])

        self._definition = definition if definition else dict()
        self._datasource = None

        if 'ID' not in self._definition:
            self._definition['ID'] = _common.new_placeholder_guid()

        if 'Type' not in self._definition:
            self._definition['Type'] = self.__class__.__name__

            # Reduce the derived classes down to their base class
            if self._definition['Type'] in ['Analysis', 'Topic']:
                self._definition['Type'] = 'Workbook'
            if 'Worksheet' in self._definition['Type']:
                self._definition['Type'] = 'Worksheet'
            if 'Workstep' in self._definition['Type']:
                self._definition['Type'] = 'Workstep'

        self._provenance = Item.CONSTRUCTOR if provenance is None else provenance

    def __contains__(self, key):
        return _common.present(self._definition, key)

    def __getitem__(self, key):
        return _common.get(self._definition, key)

    def __setitem__(self, key, val):
        self._definition.__setitem__(key, _common.ensure_upper_case_id(key, val))

    def __delitem__(self, key):
        self._definition.__delitem__(key)

    def __repr__(self):
        return _common.repr_from_row(self._definition)

    # The following properties/functions (definition, get) exist for backward compatibility for a time when
    # code directly accessed the definition object.
    @property
    def definition(self):
        return self

    @definition.setter
    def definition(self, val):
        self._definition = val

    def get(self, key, value=None):
        return self[key] if key in self else value

    @property
    def fqn(self):
        """
        The "fully qualified name" (FQN) of this item, which includes the Path and Asset (if present).
        For example: "Example >> Cooling Tower 1 >> Area A >> Temperature"
        :return: The fully qualified name of this item.
        """
        return _common.fqn_from_row(self.definition)

    @property
    def id(self):
        return _common.get(self.definition, 'ID')

    @property
    def name(self):
        return _common.get(self.definition, 'Name')

    @name.setter
    def name(self, value):
        self.definition['Name'] = value

    @property
    def type(self):
        return _common.get(self.definition, 'Type')

    @property
    def datasource(self) -> DatasourcePreviewV1:
        return self._datasource

    @property
    def definition_hash(self):
        return self.digest_hash(self.definition_dict)

    @property
    def definition_dict(self):
        return self._definition

    @property
    def provenance(self):
        return self._provenance

    @staticmethod
    def digest_hash(d):
        # We can't use Python's built-in hash() method as it produces non-deterministic hash values due to using a
        # random seed
        hashed = hashlib.md5()
        hash_string = str(json.dumps(d, sort_keys=True, skipkeys=True))
        hashed.update(hash_string.encode('utf-8'))
        return hashed.hexdigest()

    def refresh_from(self, new_item, item_map: ItemMap, status: Status):
        self._definition = new_item.definition_dict
        self._provenance = new_item.provenance

    def _construct_data_id(self, label, *, for_item: Optional[Item] = None, reconcile_by: str = 'id',
                           workbook_id: Optional[str] = None):
        definition = self.definition_dict.copy() if for_item is None else for_item.definition_dict.copy()
        if reconcile_by == 'name':
            definition.pop('Data ID')
            return _metadata.get_scoped_data_id(definition, workbook_id)
        else:
            # Note that we use Custody ID here if it's available, it's the mechanism that allows us to track the same
            # item through multiple round-trips.
            return f"[{label}] {definition.get('Custody ID', definition['ID'])}"

    def find_me(self, session: Session, label, datasource_output):
        return Item.find_item(session, self.id, datasource_output.datasource_class, datasource_output.datasource_id,
                              self._construct_data_id(label), label=label)

    @staticmethod
    def find_item(session: Session, item_id: str, datasource_class: str, datasource_id: str, data_id: str, *,
                  label: str = None):
        # This is arguably the trickiest part of the spy.workbooks codebase: identifier management. This function is
        # a key piece to understanding how it works.
        #
        # When we push items to a server, we are trying our best not to create duplicates when the user doesn't
        # intend to. In other words, operations should be idempotent whenever it is expected that they should be.
        #
        # First, we look up an item by its identifier in case our in-memory object actually corresponds directly to
        # an existing item. This will happen whenever items are constructed by virtue of executing spy.workbooks.pull(),
        # or if the user does a spy.workbooks.push() without specifying refresh=False.
        #
        # If that method doesn't work, then we try to look up the item using a canonical Data ID format,
        # which incorporates the ID field from the in-memory object, which may have been generated (in the case where
        # self.provenance equals Item.CONSTRUCTOR) or may come from a different system (in the case where
        # self.provenance equals Item.PULL or Item.Load).
        #
        # If that doesn't work and the item was created by just constructing it in memory (i.e. self.provenance
        # equals Item.CONSTRUCTOR), then the item types will try to look it up by name.
        #
        # A label can be used to purposefully isolate or duplicate items. If a label is specified, then we never look
        # up an item directly by its ID, we fall through to the canonical Data ID format (which incorporates the
        # label). This allows many copies of a workbook to be created, for example during training scenarios.

        items_api = ItemsApi(session.client)

        if not label and item_id is not None:
            try:
                item_output = items_api.get_item_and_all_properties(id=item_id)  # type: ItemOutputV1
                return item_output
            except ApiException:
                # Fall through to looking via Data ID
                pass

        return Item.find_item_by_search(
            session,
            f'Datasource Class=={datasource_class} && Datasource ID=={datasource_id} && Data ID=={data_id}')

    @staticmethod
    def find_item_by_search(session: Session, search: str, *, scope: Optional[List[str]] = None,
                            types: Optional[List[str]] = None) -> Optional[Union[ItemOutputV1, ItemSearchPreviewV1]]:
        items_api = ItemsApi(session.client)

        _filters = [search, '@includeUnsearchable']

        if scope is not None:
            _filters.append('@excludeGloballyScoped')

        kwargs = dict(
            filters=_filters,
            offset=0,
            limit=2
        )

        if scope is not None:
            kwargs['scope'] = scope

        if types is not None:
            kwargs['types'] = types

        if _login.is_sdk_module_version_at_least(62):
            kwargs['include_properties'] = _search.ALL_PROPERTIES

        search_results = items_api.search_items(**kwargs)

        if len(search_results.items) == 0:
            return None

        if len(search_results.items) > 1:
            raise SPyRuntimeError('Multiple workbook/worksheet/workstep items found via search: "%s"', search)

        if not _login.is_sdk_module_version_at_least(62):
            return items_api.get_item_and_all_properties(id=search_results.items[0].id)

        return search_results.items[0]

    @staticmethod
    def _get_item_output(session: Session, item_id: str) -> ItemOutputV1:
        items_api = ItemsApi(session.client)
        return items_api.get_item_and_all_properties(id=item_id)

    @staticmethod
    def _massage_definition_dict(definition):
        if 'UIConfig' in definition:
            try:
                definition['UIConfig'] = json.loads(definition['UIConfig'])
            except ValueError:  # ValueError includes JSONDecodeError
                pass

        if 'Metadata Properties' in definition:
            definition['Capsule Property Units'] = Item.decode_metadata_properties(definition['Metadata Properties'])
            del definition['Metadata Properties']

        # For some reason, these are coming back as lower case, which makes things inconsistent
        if 'Scoped To' in definition and isinstance(definition['Scoped To'], str):
            definition['Scoped To'] = definition['Scoped To'].upper()

    @staticmethod
    def _convert_to_numeric(s):
        try:
            # Try to convert to integer first
            return int(s)
        except ValueError:
            try:
                # If integer conversion fails, try converting to float
                return float(s)
            except ValueError:
                # If both conversions fail, return the original string
                return s

    @staticmethod
    def decode_metadata_properties(metadata_properties: str) -> Dict[str, str]:
        # The "Metadata Properties" field format is dictated by PropertyMetadataSerializer.serialize(). SPy needs to
        # decode this into a dictionary so that it can be supplied to _metadata.py via the "Capsule Property Units"
        # field.
        # See https://seeq.slack.com/archives/C0B2WUTL7/p1724788001221549?thread_ts=1724295696.384829&cid=C0B2WUTL7
        # for discussion.
        specs = metadata_properties.split('&')
        pieces = [tuple(piece.split('=')) for piece in specs if '=' in piece]
        return {Item._decode_capsule_property_unit_piece(name): Item._decode_capsule_property_unit_piece(uom)
                for name, uom in pieces}

    @staticmethod
    def _decode_capsule_property_unit_piece(s):
        # noinspection PyUnresolvedReferences
        return urllib.parse.unquote(s.replace('+', ' '))

    @staticmethod
    def _dict_from_item_output(item_output: ItemOutputV1):
        def _parse(_prop: PropertyOutputV1):
            # These various clauses try to make the returned PropertyOutputV1 behave like ScalarPropertyV1 returned
            # by item search
            if _prop.name in ('Created At', 'Updated At'):
                return _prop.value
            elif _prop.name in ('Formula Version', 'Stored Series Cache Version'):
                return Item._convert_to_numeric(_prop.value)
            elif _prop.name == 'Cache ID' and _common.is_guid(_prop.value):
                return _prop.value.upper()
            elif _prop.name == 'Data ID' and _prop.value.upper() == item_output.id:
                return _prop.value.upper()
            elif _prop.unit_of_measure and _prop.unit_of_measure != 'string':
                return {'Value': Item._convert_to_numeric(_prop.value), 'Unit Of Measure': _prop.unit_of_measure}
            elif _prop.value == 'true':
                return True
            elif _prop.value == 'false':
                return False
            else:
                return _prop.value

        definition = {prop.name: _parse(prop) for prop in item_output.properties}
        definition['Name'] = item_output.name
        definition['Type'] = item_output.type
        Item._massage_definition_dict(definition)
        return definition

    @staticmethod
    def _dict_from_item_search_preview(item_search_preview: ItemSearchPreviewV1):
        def _to_prop(_prop: ScalarPropertyV1):
            if _prop.unit_of_measure and _prop.unit_of_measure != 'string':
                return {'Value': _prop.value, 'Unit Of Measure': _prop.unit_of_measure}
            else:
                return _prop.value

        definition = {k: _to_prop(v) for k, v in item_search_preview.included_properties.items()}
        definition['ID'] = item_search_preview.id
        definition['Name'] = item_search_preview.name
        definition['Type'] = item_search_preview.type
        Item._massage_definition_dict(definition)
        return definition

    @staticmethod
    def dict_via_attribute_map(item, attribute_map):
        d = dict()
        for attr, prop in attribute_map.items():
            if hasattr(item, attr):
                d[prop] = getattr(item, attr)

        return d

    def set_input_via_attribute_map(self, body, attribute_map):
        for attr, prop in attribute_map.items():
            if hasattr(body, attr) and prop in self:
                setattr(body, attr, self[prop])

    @staticmethod
    def _property_input_from_scalar_str(scalar_str):
        match = re.fullmatch(r'([+\-\d.]+)(.*)', scalar_str)
        if not match:
            return None

        uom = match.group(2) if match.group(2) else None
        return PropertyInputV1(unit_of_measure=uom, value=float(match.group(1)))

    @staticmethod
    def formula_string_from_list(formula_list):
        return '\n'.join(formula_list) if isinstance(formula_list, list) else str(formula_list)

    @staticmethod
    def formula_list_from_string(formula_string):
        return formula_string.split('\n') if '\n' in formula_string else formula_string

    @staticmethod
    def _get_derived_class(_type):
        if _type not in Item.available_types:
            raise SPyTypeError('Type "%s" not supported in this version of seeq module' % _type)

        return Item.available_types[_type]

    @staticmethod
    def pull(item_id, *, allowed_types=None, item_search_preview: Optional[ItemSearchPreviewV1] = None,
             session: Session = None, status: Optional[Status] = None):
        session = Session.validate(session)

        datasource = None
        if item_search_preview is None or not hasattr(item_search_preview, 'included_properties'):
            item_output: ItemOutputV1 = safely(lambda: Item._get_item_output(session, item_id),
                                               action_description=f'pull Item {item_id}', status=status)
            if item_output is None:
                return None
            definition = Item._dict_from_item_output(item_output)
            datasource = item_output.datasource
        else:
            definition = Item._dict_from_item_search_preview(item_search_preview)
            if item_search_preview.datasource is not None:
                if item_search_preview.datasource.id in session.datasource_output_cache:
                    datasource = session.datasource_output_cache[item_search_preview.datasource.id]
                else:
                    datasources_api = DatasourcesApi(session.client)
                    datasource = datasources_api.get_datasource(id=item_search_preview.datasource.id)
                    session.datasource_output_cache[datasource.id] = datasource

                # ItemSearchPreviewV1 will incorrectly capitalize 'Datasource ID' sometimes -- namely the Tree File
                # datasource. So we always use the ID from the DatasourceOutputV1 object.
                definition['Datasource ID'] = datasource.datasource_id

        if allowed_types and definition['Type'] not in allowed_types:
            return None

        derived_class = Item._get_derived_class(definition['Type'])
        item = derived_class(definition, provenance=Item.PULL)  # type: Item
        item._pull(session, item_id, status, item_search_preview)
        item._datasource = datasource
        return item

    @staticmethod
    def from_dict(definition, *, provenance=PULL):
        derived_class = Item._get_derived_class(definition['Type'])
        item = derived_class(definition, provenance=provenance)  # type: Item
        return item

    def _pull(self, session: Session, item_id: str, status: Status, item_search_preview: ItemSearchPreviewV1):
        pass

    @staticmethod
    def load(definition):
        derived_class = Item._get_derived_class(definition['Type'])
        item = derived_class(definition, provenance=Item.LOAD)
        return item

    def _set_formula_based_item_properties(self, parameters: List[FormulaParameterOutputV1]):
        self._definition['Formula'] = Item.formula_list_from_string(self._definition['Formula'])
        self._definition['Formula Parameters'] = dict()
        for parameter in parameters:  # type: FormulaParameterOutputV1
            if parameter.item:
                self._definition['Formula Parameters'][parameter.name] = parameter.item.id
            else:
                self._definition['Formula Parameters'][parameter.name] = parameter.formula

    def _scrape_auth_datasources(self, session: Session) -> Dict[str, DatasourceOutputV1]:
        return dict()


class Reference:
    JOURNAL = 'Journal'
    DETAILS = 'Details'
    SCOPED = 'Scoped'
    ANCESTOR = 'Ancestor'
    INVENTORY = 'Inventory'
    DEPENDENCY = 'Dependency'
    EMBEDDED_CONTENT = 'Embedded Content'
    DATE_RANGE_CONDITION = 'Date Range Condition'
    ASSET_SELECTION = 'Asset Selection'

    def __init__(self, _id, _provenance, worksheet=None, item_search_preview=None):
        """
        :type _id: str
        :type _provenance: str
        :type worksheet: Worksheet
        """
        self.id = _id.upper()
        self.provenance = _provenance
        self.worksheet = worksheet
        self._item_search_preview = item_search_preview

    @property
    def worksheet_id(self):
        return self.worksheet.id if self.worksheet is not None else None

    @property
    def item_search_preview(self) -> ItemSearchPreviewV1:
        return self._item_search_preview

    @item_search_preview.setter
    def item_search_preview(self, val):
        self._item_search_preview = val

    @property
    def worksheet_id(self):
        return self.worksheet.id if self.worksheet is not None else None

    def __repr__(self):
        if self.worksheet is not None:
            return '%s reference on "%s" (%s)' % (self.provenance, self.worksheet.name, self.id)
        else:
            return '%s (%s)' % (self.provenance, self.id)

    def __hash__(self):
        return hash((self.id, self.provenance, self.worksheet_id))

    def __eq__(self, other):
        return self.id == other.id and self.provenance == other.provenance and self.worksheet_id == other.worksheet_id


def replace_items(document, item_map: ItemMap):
    if document is None:
        return

    new_report = copy.deepcopy(document)
    for _id in item_map.keys():
        matches = re.finditer(re.escape(_id), document, flags=re.IGNORECASE)
        for match in matches:
            _replacement = item_map[_id]
            try:
                new_report = re.sub(re.escape(match.group(0)), _replacement, new_report, flags=re.IGNORECASE)
            except (TypeError, IndexError):
                pass

    return new_report


def get_canonical_server_url(session: Session):
    url = session.client.host.replace('/api', '').lower()  # type: str
    if url.startswith('http:') and url.endswith(':80'):
        url = url.replace(':80', '')
    if url.startswith('https:') and url.endswith(':443'):
        url = url.replace(':443', '')

    return url


class ItemList(list):

    def _index_of(self, key) -> Optional[int]:
        if isinstance(key, str):
            candidates = list()
            for i in range(len(self)):
                item: Item = super().__getitem__(i)
                if key == item.id:
                    candidates.append((i, item))
                elif key == item.name:
                    candidates.append((i, item))
                elif key == item.fqn:
                    candidates.append((i, item))

            if len(candidates) == 0:
                return None
            elif len(candidates) > 1:
                error_str = '\n'.join([f'{i}: {item}' for i, item in candidates])
                raise IndexError(f'"{key}" matches multiple items in list:\n{error_str}')
            else:
                return candidates[0][0]

        return key

    def __contains__(self, key):
        return self._index_of(key) is not None

    def __getitem__(self, key) -> Item:
        index = self._index_of(key)
        if index is None:
            raise IndexError(f'"{key}" not found in list')

        return super().__getitem__(index)

    def __setitem__(self, key, val: Item):
        index = self._index_of(key)
        return super().__setitem__(index, val)

    def __delitem__(self, key):
        index = self._index_of(key)
        return super().__delitem__(index)
