import os
import re
import time
from copy import deepcopy
from datetime import datetime, timedelta

import dateutil.parser
from source import helper, shared, tables
from source.shared import utc
from source.tables import ColumnSpec
from urllib3.connectionpool import MaxRetryError

from seeq import sdk, spy
from seeq.base import util
from seeq.base.seeq_names import SeeqNames
from seeq.sdk import *
from seeq.sdk.rest import ApiException

SORT_BY_ITEM_GUID = ["ID"]
SORT_BY_ITEM_NAME = ["Name"]


def datasource_list(**kwargs):
    """
    List registered datasources and associated information.
    :param csv: CSV file to write items to.
    """
    client = spy.client

    _filter = ""
    _types = ["Datasource"]

    items_api = sdk.ItemsApi(client)

    tables.print_table_from_sdk_output(
        lambda offset: items_api.search_items(
            filters=[_filter, SeeqNames.API.Flags.include_unsearchable],
            types=_types,
            offset=offset,
            limit=helper.get_default_page_size(),
            order_by=SORT_BY_ITEM_NAME,
        ),
        "items",
        [
            ColumnSpec("count", "#", ">", 4),
            ColumnSpec("name", "Datasource Name", "<", 40),
            ColumnSpec(
                "",
                "Datasource Class",
                "<",
                30,
                lambda value, item: _get_item_property(
                    item.id, SeeqNames.Properties.datasource_class, items_api
                ),
            ),
            ColumnSpec(
                "",
                "Datasource ID",
                "<",
                36,
                lambda value, item: _get_item_property(
                    item.id, SeeqNames.Properties.datasource_id, items_api
                ),
            ),
            ColumnSpec(
                "",
                "Stored",
                "<",
                6,
                lambda value, item: _get_item_property(
                    item.id, SeeqNames.Properties.stored_in_seeq, items_api
                ),
            ),
            ColumnSpec(
                "",
                "Cached",
                "<",
                6,
                lambda value, item: _get_item_property(
                    item.id, SeeqNames.Properties.cache_enabled, items_api
                ),
            ),
            ColumnSpec(
                "",
                "Archived",
                "<",
                8,
                lambda value, item: _get_item_property(
                    item.id, SeeqNames.Properties.archived, items_api
                ),
            ),
            ColumnSpec("id", "ID", "<", 36),
        ],
        csv_file_name=kwargs.get("csv"),
    )


def _get_item_property(_id, property_name, items_api):
    try:
        return items_api.get_property(id=_id, property_name=property_name).value
    except ApiException:
        return ""


def get_datasource_item(client, **kwargs):
    items_api = sdk.ItemsApi(client)

    if kwargs.get("id"):
        datasource_item = items_api.get_item_and_all_properties(id=kwargs.get("id"))
        if datasource_item.type != "Datasource":
            raise RuntimeError(
                "Provided Seeq ID does not match a Datasource; please try again with the ID of a "
                "Datasource.\n"
                'Execute "datasource.datasource_list()" to find the ID you want and then re-run this command.'
            )

    else:
        _filters = list()
        if kwargs.get("datasource_class"):
            _filters.append("Datasource Class==" + kwargs.get("datasource_class"))
        if kwargs.get("datasource_id"):
            _filters.append("Datasource ID==" + kwargs.get("datasource_id"))
        if kwargs.get("datasource_name"):
            _filters.append("Name==" + kwargs.get("datasource_name"))

        filters_arg = [" && ".join(_filters), SeeqNames.API.Flags.include_unsearchable]

        item_search_list = items_api.search_items(
            types=["Datasource"],
            filters=filters_arg,
            limit=helper.get_default_page_size(),
        )  # type: ItemSearchPreviewPaginatedListV1
        if len(item_search_list.items) == 0:
            raise RuntimeError("Datasource not found")
        if len(item_search_list.items) > 1:
            tables.print_table_from_sdk_output(
                lambda offset: items_api.search_items(
                    filters=filters_arg,
                    offset=offset,
                    limit=helper.get_default_page_size(),
                    order_by=SORT_BY_ITEM_GUID,
                ),
                "items",
                [
                    ColumnSpec("name", "Datasource Name", "<", 30),
                    ColumnSpec(
                        "",
                        "Datasource Class",
                        "<",
                        25,
                        transform=lambda value, item: _get_item_property(
                            item.id, SeeqNames.Properties.datasource_class, items_api
                        ),
                    ),
                    ColumnSpec(
                        "",
                        "Datasource ID",
                        "<",
                        36,
                        transform=lambda value, item: _get_item_property(
                            item.id, SeeqNames.Properties.datasource_id, items_api
                        ),
                    ),
                ],
            )

            raise RuntimeError(
                "Multiple datasources found, please narrow your search.\n"
                'Execute "datasource.datasource_list()" to find the ID you want and then re-run this command\n'
                "with the --id flag."
            )

        datasource_item = item_search_list.items[0]

    datasource_id = items_api.get_property(
        id=datasource_item.id, property_name=SeeqNames.Properties.datasource_id
    ).value
    datasource_class = items_api.get_property(
        id=datasource_item.id, property_name=SeeqNames.Properties.datasource_class
    ).value
    datasource_name = items_api.get_property(
        id=datasource_item.id, property_name=SeeqNames.Properties.name
    ).value

    return shared.Bunch(
        id=datasource_item.id,
        name=datasource_name,
        datasource_class=datasource_class,
        datasource_id=datasource_id,
    )


def _ancestor_path(ancestors):
    if not ancestors:
        return ""

    path = ""
    for ancestor in ancestors:
        if len(path) > 0:
            path = path + " "

        path += ancestor.name + " >>"

    return path


def items(**kwargs):
    """
    List the items associated with the datasource.
    :param datasource_name: Datasource Name of datasource to search.
    :param datasource_class: Datasource Class of datasource to search.
    :param datasource_id: Datasource ID of datasource to search.
    :param id: Seeq ID of datasource to search
    :param filter: filter (e.g. "*.PV").
    :param include_archive: include archived items
    :param csv: CSV file to write items to.
    """

    client = spy.client

    items_api = sdk.ItemsApi(client)

    datasource_item = get_datasource_item(client, **kwargs)

    _filter = "Datasource Class==" + datasource_item.datasource_class
    _filter += " && Datasource ID==" + datasource_item.datasource_id

    if kwargs.get("filter"):
        _filter += " && Name ~= " + kwargs.get("filter")

    _filters = [_filter]
    if kwargs.get("include_archived"):
        _filters.append(SeeqNames.API.Flags.include_unsearchable)

    tables.print_table_from_sdk_output(
        lambda offset: items_api.search_items(
            filters=_filters,
            offset=offset,
            limit=helper.get_default_page_size(),
            order_by=SORT_BY_ITEM_NAME,
        ),
        "items",
        [
            ColumnSpec("count", "#", ">", 8),
            ColumnSpec(
                "ancestors",
                "Path",
                ">",
                40,
                lambda ancestors, item: _ancestor_path(ancestors),
            ),
            ColumnSpec("name", "Name", "<", 40),
            ColumnSpec("type", "Type", "<", 15),
            ColumnSpec("value_unit_of_measure", "UOM", "<", 8),
            ColumnSpec("id", "ID", "<", 36),
        ],
        csv_file_name=kwargs.get("csv"),
    )


def recount(**kwargs):
    """
    Recount the totals for items associated with the datasource.
    :param datasource_name: Datasource Name of datasource to search.
    :param datasource_class: Datasource Class of datasource to search.
    :param datasource_id: Datasource ID of datasource to search.
    :param id: Seeq ID of datasource to search
    :param filter: filter (e.g. "*.PV").
    :param include_archive: include archived items
    :param csv: CSV file to write items to.
    """

    client = spy.client

    items_api = sdk.ItemsApi(client)
    datasources_api = sdk.DatasourcesApi(client)

    datasource_item = get_datasource_item(client, **kwargs)

    print("Current counts:")

    _print_datasource_item_counts(items_api, datasource_item)

    print(
        "\nRe-counting items associated with this datasource. This may take several minutes depending on"
    )
    print("the number of items in the datasource.")

    datasources_api.recount(id=datasource_item.id)

    print("Finished successfully. New counts:")

    _print_datasource_item_counts(items_api, datasource_item, kwargs.get("csv"))

    print('\nTotals in the Seeq Workbench "Connections" list should be correct.')


def _print_datasource_item_counts(items_api, datasource_item, csv_file=None):
    table = tables.start_table(
        [
            ColumnSpec("item_type", "Item Type", "<", 20),
            ColumnSpec("item_count", "Item Count", ">", 10),
        ],
        csv_file,
    )

    tables.write_table_row(
        table,
        shared.Bunch(
            item_type="Signals",
            item_count=_get_property_or_default(
                items_api, datasource_item.id, "Signal Count", "0"
            ),
        ),
    )

    tables.write_table_row(
        table,
        shared.Bunch(
            item_type="Conditions",
            item_count=_get_property_or_default(
                items_api, datasource_item.id, "Condition Count", "0"
            ),
        ),
    )

    tables.write_table_row(
        table,
        shared.Bunch(
            item_type="Assets",
            item_count=_get_property_or_default(
                items_api, datasource_item.id, "Asset Count", "0"
            ),
        ),
    )

    tables.write_table_row(
        table,
        shared.Bunch(
            item_type="Scalars",
            item_count=_get_property_or_default(
                items_api, datasource_item.id, "Scalar Count", "0"
            ),
        ),
    )

    tables.write_table_row(
        table,
        shared.Bunch(
            item_type="Relationships",
            item_count=_get_property_or_default(
                items_api, datasource_item.id, "Relationship Count", "0"
            ),
        ),
    )

    tables.finish_table(table)


def _get_property_or_default(items_api, _id, property_name, default):
    try:
        prop = items_api.get_property(
            id=_id, property_name=property_name
        )  # type: PropertyOutputV1
        return prop.value
    except ApiException:
        return default


def archive(**kwargs):
    """
    Archives a datasource and associated items.
    :param datasource_name: Datasource Name of datasource to search.
    :param datasource_class: Datasource Class of datasource to search.
    :param datasource_id: Datasource ID of datasource to search.
    :param id: Seeq ID of datasource to search.
    :param filter: filter (e.g. "*.PV").
    :param restore: restore the archive status.
    :param csv: CSV file to write items to.
    """

    client = spy.client

    items_api = sdk.ItemsApi(client)

    datasource_item = get_datasource_item(client, **kwargs)

    datasource_id = items_api.get_property(
        id=datasource_item.id, property_name=SeeqNames.Properties.datasource_id
    ).value
    datasource_class = items_api.get_property(
        id=datasource_item.id, property_name=SeeqNames.Properties.datasource_class
    ).value

    _filter = "Datasource Class==" + datasource_class
    _filter += " && Datasource ID==" + datasource_id

    if kwargs.get("filter"):
        _filter += " && Name ~= " + kwargs.get("filter")

    if not kwargs.get("filter") and kwargs.get("restore"):
        # we're reverting the archive flag
        # the datasource needs to be restore before the items so that permissions are inherited correctly
        _process_archive_command_on_item(datasource_item, client, kwargs.get("restore"))
        print("\nDatasource item has been UN-ARCHIVED.")

    tables.print_table_from_sdk_output(
        lambda offset: items_api.search_items(
            filters=[_filter, SeeqNames.API.Flags.include_unsearchable],
            offset=offset,
            limit=helper.get_default_page_size(),
            order_by=SORT_BY_ITEM_GUID,
        ),
        "items",
        [
            ColumnSpec("count", "#", ">", 8),
            ColumnSpec("total_time_str", "Time", ">", 8),
            ColumnSpec(
                "ancestors",
                "Path",
                ">",
                40,
                lambda ancestors, item: _ancestor_path(ancestors),
            ),
            ColumnSpec("name", "Name", "<", 40),
            ColumnSpec("type", "Type", "<", 15),
            ColumnSpec("value_unit_of_measure", "UOM", "<", 8),
            ColumnSpec("id", "ID", "<", 36),
        ],
        # Note that we exclude the datasource item here so that we archive everything else first, then do the
        # datasource at the end.
        lambda item: (
            _process_archive_command_on_item(item, client, kwargs.get("restore"))
            if item.type != "Datasource"
            else ""
        ),
        csv_file_name=kwargs.get("csv"),
    )

    print(
        "\nAll of the above items have been %s."
        % ("UN-ARCHIVED" if kwargs.get("restore") else "ARCHIVED")
    )

    if not kwargs.get("filter") and not kwargs.get("restore"):
        # We're archiving the whole datasource
        # datasource should be archived last since we want the permissions to be inherited correctly
        _process_archive_command_on_item(datasource_item, client, kwargs.get("restore"))
        print("\nDatasource item has been ARCHIVED.")


def _process_archive_command_on_item(item, client, restore):
    items_api = sdk.ItemsApi(client)

    property_input = PropertyInputV1()
    property_input.value = True if not restore else False
    items_api.set_property(id=item.id, property_name="Archived", body=property_input)


def cache(**kwargs):
    """
    Turn persistent datasource caching on/off and clear persistent caching to force reload from datasource.
    :param command: on, off, clear
    :param datasource_name: Datasource Name of datasource to search.
    :param datasource_class: Datasource Class of datasource to search.
    :param datasource_id: Datasource ID of datasource to search.
    :param id: Seeq ID of datasource to search.
    :param filter: filter (e.g. "*.PV").
    :param input_csv: CSV file with ID or Name column for items to affect
    :param restore: restore the archive status.
    :param csv: CSV file to write items to.
    """

    if kwargs.get("command"):
        if kwargs.get("command") not in ["on", "off", "clear"]:
            print("You must provide a command: on, off, clear")
            return
        cmd = kwargs.get("command")
    else:
        print("You must provide a command: on, off, clear")
        return

    client = spy.client

    items_api = sdk.ItemsApi(client)

    datasource_item = get_datasource_item(client, **kwargs)

    _types = [
        "StoredSignal",
        "CalculatedSignal",
        "StoredCondition",
        "CalculatedCondition",
        "CalculatedScalar",
    ]
    _filter = "Datasource Class==" + datasource_item.datasource_class
    _filter += " && Datasource ID==" + datasource_item.datasource_id

    # Be as selective as possible. This means pagination is only necessary for the 'clear' command.
    # For 'clear' or 'off' we want to find items where cache is enabled; for 'on' we want those with it disabled.
    _filter += " && Cache Enabled == %s" % ("true" if cmd != "on" else "false")

    if kwargs.get("filter") and kwargs.get("input_csv"):
        raise RuntimeError("filter and input_csv arguments are mutually exclusive")

    if kwargs.get("input_csv"):
        column_specs = [
            ColumnSpec("count", "#", ">", 8),
            ColumnSpec("name", "Name", "<", 40),
            ColumnSpec("type", "Type", "<", 15, console=False),
            ColumnSpec("id", "ID", "<", 36, console=False),
        ]

        header, rows = tables.read_table(kwargs.get("input_csv"))

        id_column = tables.find_column_index(header, "ID")
        name_column = tables.find_column_index(header, "Name")

        if name_column is None and id_column is None:
            raise RuntimeError('"ID" nor "Name" column not found in input CSV file')

        table = tables.start_table(column_specs, kwargs.get("csv"))

        for row in rows:
            item_id = row[id_column] if id_column is not None else None
            item_name = row[name_column] if name_column is not None else None

            items = []
            if item_name:
                _filter += "&& Name == %s" % item_name
                search_results = items_api.search_items(
                    filters=[_filter, SeeqNames.API.Flags.include_unsearchable],
                    types=_types,
                )  # type: ItemSearchPreviewPaginatedListV1
                if len(search_results.items) > 0:
                    items = search_results.items
                else:
                    items = [shared.Bunch(name=item_name, type="", id="NOT FOUND")]
            elif item_id:
                _filter += "&& ID == %s" % item_id
                try:
                    item = items_api.get_item_and_all_properties(
                        id=item_id
                    )  # type: ItemOutputV1
                    items = [shared.Bunch(name=item.name, type=item.type, id=item.id)]
                except Exception:
                    items = [shared.Bunch(name=item_name, type="", id="NOT FOUND")]

            for _item in items:
                if _item.id != "NOT FOUND":
                    _process_cache_command_on_item(cmd, _item, client)
                tables.write_table_row(table, _item)

        tables.finish_table(table)

    else:
        column_specs = [
            ColumnSpec("count", "#", ">", 8),
            ColumnSpec("total_time_str", "Time", ">", 8),
            ColumnSpec(
                "ancestors",
                "Path",
                ">",
                40,
                lambda ancestors, _item: _ancestor_path(ancestors),
            ),
            ColumnSpec("name", "Name", "<", 40),
            ColumnSpec("type", "Type", "<", 15, console=False),
            ColumnSpec("id", "ID", "<", 36, console=False),
        ]

        if kwargs.get("filter"):
            _filter += " && Name ~= " + kwargs.get("filter")

        tables.print_table_from_sdk_output(
            lambda offset: items_api.search_items(
                filters=[_filter, SeeqNames.API.Flags.include_unsearchable],
                types=_types,
                offset=offset if cmd == "clear" else 0,  # only paginate 'clear'
                limit=helper.get_default_page_size(),
                order_by=SORT_BY_ITEM_GUID,
            ),
            "items",
            column_specs,
            lambda _item: _process_cache_command_on_item(cmd, _item, client),
            csv_file_name=kwargs.get("csv"),
        )

    if cmd == "on":
        message = "caching turned ON"
    elif cmd == "off":
        message = "caching turned OFF"
    else:
        message = "cache CLEARED"

    print("\nPersistent %s for all of the above items." % message)

    if cmd != "on":
        print("Note: Only items with caching ON were affected.")

    if not kwargs.get("filter") and not kwargs.get("input_csv") and cmd != "clear":
        # We're turning cache on/off for the whole datasource, so set it on the datasource item too
        _process_cache_command_on_item(cmd, datasource_item, client)

        print(
            "\nPersistent caching turned %s at datasource level. (All newly indexed items will have caching turned "
            "%s by default.)" % (cmd.upper(), cmd.upper())
        )


def _process_cache_command_on_item(cmd, item, client):
    items_api = sdk.ItemsApi(client)

    if cmd == "off":
        property_input = PropertyInputV1()
        property_input.value = False
        items_api.set_property(
            id=item.id, property_name="Cache Enabled", body=property_input
        )
    elif cmd == "on":
        property_input = PropertyInputV1()
        property_input.value = True
        items_api.set_property(
            id=item.id, property_name="Cache Enabled", body=property_input
        )
    elif cmd == "clear":
        items_api.clear_cache(id=item.id)


def map_same_server(**kwargs):
    """
    Create a map of items in a datasource to other items (probably in a different
    datasource).
    :param datasource_name: Datasource Name of the OLD datasource to search.
    :param datasource_class: Datasource Class of the OLD datasource to search.
    :param datasource_id: Datasource ID of the OLD datasource to search.
    :param id: Seeq ID of the OLD datasource to search.
    :param name_regex: Regular expression that matches on OLD item name.
    :param description_regex: Regular expression that matches on OLD item description.
    :param data_id_regex: Regular expression that matches on OLD Data ID.
    :param new_datasource_class: Datasource Class of datasource containing NEW item.
    :param new_datasource_id: Datasource ID of datasource containing NEW item.
    :param new_id: Seeq ID of datasource containing NEW item.
    :param new_name_regex: Regular expression that matches on NEW item name.
    :param new_description_regex: Regular expression that matches on NEW item description.
    :param new_data_id_regex: Regular expression that matches on NEW Data ID.
    :param csv: CSV file to write items to.
    :param append_csv: Append to the existing CSV file if possible.
    """

    client = spy.client
    items_api = sdk.ItemsApi(client)

    datasource_item = get_datasource_item(client, **kwargs)

    _filter = "Datasource Class==" + datasource_item.datasource_class
    _filter += " && Datasource ID==" + datasource_item.datasource_id

    if kwargs.get("name_regex"):
        _filter += " && Name ~= /%s/" % _remove_regex_capture_group_names(
            kwargs.get("name_regex")
        )

    if kwargs.get("description_regex"):
        _filter += " && Description ~= /%s/" % _remove_regex_capture_group_names(
            kwargs.get("description_regex")
        )

    if kwargs.get("data_id_regex"):
        _filter += " && Data ID ~= /%s/" % _remove_regex_capture_group_names(
            kwargs.get("data_id_regex")
        )

    new_datasource_args = {
        "datasource_class": kwargs.get("new_datasource_class"),
        "datasource_id": kwargs.get("new_datasource_id"),
        "id": kwargs.get("new_id"),
    }

    new_datasource_item = get_datasource_item(client, **new_datasource_args)

    tables.print_table_from_sdk_output(
        lambda offset: items_api.search_items(
            filters=[_filter, SeeqNames.API.Flags.include_unsearchable],
            offset=offset,
            limit=helper.get_default_page_size(),
            order_by=SORT_BY_ITEM_GUID,
        ),
        "items",
        [
            ColumnSpec("count", "#", ">", 8),
            ColumnSpec("type", "Type", "<", 15, console=False),
            ColumnSpec(
                "ancestors",
                "Old Path",
                ">",
                40,
                lambda ancestors, item: _ancestor_path(ancestors),
            ),
            ColumnSpec("name", "Old Name", "<", 40),
            ColumnSpec("description", "Old Description", "<", 40, console=False),
            ColumnSpec("value_unit_of_measure", "Old UOM", "<", 8, console=False),
            ColumnSpec("id", "Old ID", "<", 36, console=False),
            ColumnSpec("new_ancestors", "New Path", ">", 40),
            ColumnSpec("new_name", "New Name", "<", 40),
            ColumnSpec("new_description", "New Description", "<", 40, console=False),
            ColumnSpec("new_value_unit_of_measure", "New UOM", "<", 8, console=False),
            ColumnSpec("new_id", "New ID", "<", 36, console=False),
        ],
        lambda item: _process_map_same_server_command_on_item(item, client, new_datasource_item, **kwargs),
        csv_file_name=kwargs.get("csv"),
        append_csv=kwargs.get("append_csv"),
    )


def _remove_regex_capture_group_names(regex):
    # PostgreSQL will choke on named capture groups
    return re.sub(r"\?<\w+>", "", regex)


def _pythonize_regex_capture_group_names(regex):
    # Unlike standard regex syntax, Python capture groups have a capital P:  (?P<group_name>.*?)
    return re.sub(r"\?<(\w+)>", r"?P<\1>", regex)


def _process_map_same_server_command_on_item(item, client, new_datasource_item, **kwargs):
    items_api = sdk.ItemsApi(client)
    item_type = None

    new_filter = 'Datasource Class==' + new_datasource_item.datasource_class
    new_filter += ' && Datasource ID==' + new_datasource_item.datasource_id

    simple_match = False
    if (
            not kwargs.get("name_regex")
            and not kwargs.get("description_regex")
            and not kwargs.get("data_id_regex")
            and not kwargs.get("new_name_regex")
            and not kwargs.get("new_description_regex")
            and not kwargs.get("new_data_id_regex")
    ):
        # This is "identical datasources" mode, where the names of the items in each datasource are the same.
        # So we just filter on the name, assuming it's unique.
        new_filter += " && Name==%s" % item.name

        # Storing this and using it in the where clause instead of
        # using a filter results in a faster query that uses less RAM.
        item_type = item.type
        simple_match = True
    else:
        capture_groups = dict()
        new_name_regex = None

        def _update_capture_groups(_regex, _prop):
            _match = re.match(_pythonize_regex_capture_group_names(_regex), _prop)
            if _match:
                capture_groups.update(_match.groupdict())

        if kwargs.get("name_regex"):
            _update_capture_groups(kwargs.get("name_regex"), item.name)

        if kwargs.get("new_name_regex"):
            new_name_regex = kwargs.get("new_name_regex")
            new_name_regex = _replace_tokens_in_regex(new_name_regex, capture_groups)

        if new_name_regex:
            new_filter += " && Name ~= /%s/" % new_name_regex

        new_description_regex = None

        if kwargs.get("description_regex"):
            _update_capture_groups(kwargs.get("description_regex"), item.description)

        if kwargs.get("new_description_regex"):
            new_description_regex = kwargs.get("new_description_regex")
            new_description_regex = _replace_tokens_in_regex(
                new_description_regex, capture_groups
            )

        if new_description_regex:
            new_filter += " && Description ~= /%s/" % new_description_regex

        new_data_id_regex = None

        if kwargs.get("data_id_regex"):
            data_id_property = items_api.get_property(
                id=item.id, property_name=SeeqNames.Properties.data_id
            )  # type: PropertyOutputV1
            _update_capture_groups(kwargs.get("data_id_regex"), data_id_property.value)

        if kwargs.get("new_data_id_regex"):
            new_data_id_regex = kwargs.get("new_data_id_regex")
            new_data_id_regex = _replace_tokens_in_regex(
                new_data_id_regex, capture_groups
            )

        if new_data_id_regex:
            new_filter += " && Data ID == %s" % new_data_id_regex

    offset = 0
    matching_items = list()
    while True:
        search_results = items_api.search_items(
            filters=[new_filter, SeeqNames.API.Flags.include_unsearchable],
            offset=offset,
            limit=helper.get_default_page_size(),
            types=([item_type]) if item_type is not None else [],
        )  # type: ItemSearchPreviewPaginatedListV1

        for matching_item in search_results.items:
            if not simple_match or _ancestor_path(
                    matching_item.ancestors
            ) == _ancestor_path(item.ancestors):
                matching_items.append(matching_item)

        if len(search_results.items) < helper.get_default_page_size():
            break

        offset += len(search_results.items)

    if len(matching_items) == 1:
        new_item = matching_items[0]

        return [
            ("new_id", new_item.id),
            ("new_ancestors", _ancestor_path(new_item.ancestors)),
            ("new_name", new_item.name),
            ("new_description", new_item.description),
            ("new_value_unit_of_measure", new_item.value_unit_of_measure),
        ]

    elif len(matching_items) > 1:
        return [
            ("new_id", "MULTIPLE MATCHES"),
            ("new_ancestors", "MULTIPLE MATCHES"),
            ("new_name", "MULTIPLE MATCHES"),
            ("new_description", "MULTIPLE MATCHES"),
            ("new_value_unit_of_measure", ""),
        ]
    else:
        return [
            ("new_id", "NO MATCHES"),
            ("new_ancestors", "NO MATCHES"),
            ("new_name", "NO MATCHES"),
            ("new_description", "NO MATCHES"),
            ("new_value_unit_of_measure", ""),
        ]


def _replace_tokens_in_regex(regex, group_dict):
    for name, value in group_dict.items():
        regex = regex.replace("${%s}" % name, value)

    return regex


def map_old_server(**kwargs):
    """
    Create a list of items to be mapped later using "datasource_map_new_server()".
    :param datasource_name: Datasource Name of the OLD datasource to search.
    :param datasource_class: Datasource Class of the OLD datasource to search.
    :param datasource_id: Datasource ID of the OLD datasource to search.
    :param id: Seeq ID of the OLD datasource to search.
    :param name_regex: Regular expression that matches on OLD item name.
    :param description_regex: Regular expression that matches on OLD item description.
    :param data_id_regex: Regular expression that matches on OLD Data ID.
    :param csv: CSV file to write items to.
    :param append_csv: Append to the existing CSV file if possible.
    """
    client = spy.client

    items_api = sdk.ItemsApi(client)

    datasource_item = get_datasource_item(client, **kwargs)

    _filter = "Datasource Class==" + datasource_item.datasource_class
    _filter += " && Datasource ID==" + datasource_item.datasource_id

    if kwargs.get("name_regex"):
        _filter += " && Name ~= /%s/" % _remove_regex_capture_group_names(
            kwargs.get("name_regex")
        )

    if kwargs.get("description_regex"):
        _filter += " && Description ~= /%s/" % _remove_regex_capture_group_names(
            kwargs.get("description_regex")
        )

    if kwargs.get("data_id_regex"):
        _filter += " && Data ID ~= /%s/" % _remove_regex_capture_group_names(
            kwargs.get("data_id_regex")
        )

    tables.print_table_from_sdk_output(
        lambda offset: items_api.search_items(
            filters=[_filter, SeeqNames.API.Flags.include_unsearchable],
            offset=offset,
            limit=helper.get_default_page_size(),
            order_by=SORT_BY_ITEM_GUID,
        ),
        "items",
        [
            ColumnSpec("count", "#", ">", 8),
            ColumnSpec("type", "Type", "<", 15),
            ColumnSpec("name", "Old Name", "<", 40),
            ColumnSpec("description", "Old Description", "<", 40),
            ColumnSpec("id", "Old ID", "<", 36, console=False),
            ColumnSpec("datasource_id", "Old Datasource ID", "<", 36, console=False),
            ColumnSpec("data_id", "Old Data ID", "<", 36, console=False),
        ],
        lambda item: _process_map_old_server_command_on_item(item, client, **kwargs),
        csv_file_name=kwargs.get("csv"),
        append_csv=kwargs.get("append_csv"),
    )


def _process_map_old_server_command_on_item(item, client, **kwargs):
    items_api = sdk.ItemsApi(client)

    datasource_id = items_api.get_property(
        id=item.id, property_name=SeeqNames.Properties.datasource_id
    )  # type: PropertyOutputV1

    data_id = items_api.get_property(
        id=item.id, property_name=SeeqNames.Properties.data_id
    )  # type: PropertyOutputV1

    return [("datasource_id", datasource_id.value), ("data_id", data_id.value)]


def map_new_server(**kwargs):
    """
    Map the items from datasource_map_old_server() to items on a new server.
    :param old_server_csv: Filename of CSV created by datasource_map_old())server
    :param datasource_id: Datasource ID of the OLD datasource to search.
    :param name_regex: Regular expression that matches on OLD item name.
    :param description_regex: Regular expression that matches on OLD item description.
    :param data_id_regex: Regular expression that matches on OLD Data ID.
    :param new_datasource_class: New datasource class to map to.
    :param new_datasource_id: ID of datasource containing NEW item.
    :param new_name_regex: Regular expression that matches on NEW item name.
    :param new_description_regex: Regular expression that matches on NEW item description.
    :param new_data_id_regex: Regular expression that matches on NEW Data ID.
    :param csv: CSV file to write items to.
    :param append_csv: Append to the existing CSV file if possible.
    """
    if not kwargs.get("datasource_id") or not kwargs.get("old_server_csv"):
        print("Parameters datasource_id and old_server_id are required.")
        return

    (header, rows) = tables.read_table(kwargs.get("old_server_csv"))

    old_items = dict()
    old_items["id_index"] = tables.find_column_index(header, "Old ID")
    old_items["datasource_id_index"] = tables.find_column_index(
        header, "Old Datasource ID"
    )
    old_items["name_index"] = tables.find_column_index(header, "Old Name")
    old_items["type_index"] = tables.find_column_index(header, "Type")
    old_items["description_index"] = tables.find_column_index(header, "Old Description")
    old_items["uom_index"] = tables.find_column_index(header, "Old UOM")
    old_items["data_id_index"] = tables.find_column_index(header, "Old Data ID")

    column_specs = [
        ColumnSpec("count", "#", ">", 8),
        ColumnSpec("type", "Type", "<", 15),
        ColumnSpec("name", "Old Name", "<", 40),
        ColumnSpec("description", "Old Description", "<", 40, console=False),
        ColumnSpec("id", "Old ID", "<", 36, console=False),
        ColumnSpec("datasource_id", "Old Datasource ID", "<", 36, console=False),
        ColumnSpec("data_id", "Old Data ID", "<", 36, console=False),
        ColumnSpec("new_name", "New Name", "<", 40),
        ColumnSpec("new_description", "New Description", "<", 40, console=False),
        ColumnSpec("new_id", "New ID", "<", 36, console=False),
        ColumnSpec("new_datasource_id", "New Datasource ID", "<", 36, console=False),
        ColumnSpec("new_data_id", "New Data ID", "<", 36, console=False),
    ]

    table = None
    try:
        table = tables.start_table(
            column_specs, kwargs.get("csv"), kwargs.get("append_csv")
        )

        for row in rows:
            if row[old_items["datasource_id_index"]] == kwargs.get("datasource_id"):
                item = _process_map_new_server_command_on_item(row, old_items, **kwargs)
                tables.write_table_row(table, item)

    finally:
        if table:
            tables.finish_table(table)


def _process_map_new_server_command_on_item(row, old_items, **kwargs):
    client = spy.client

    items_api = sdk.ItemsApi(client)

    new_filter = "Datasource ID==%s" % (
        kwargs.get("new_datasource_id")
        if kwargs.get("new_datasource_id")
        else kwargs.get("datasource_id")
    )

    new_filter += " && Datasource Class==%s" % (
        kwargs.get("new_datasource_class")
        if kwargs.get("new_datasource_class")
        else kwargs.get("datasource_class")
    )
    types_filter = []

    if (
            not kwargs.get("name_regex")
            and not kwargs.get("description_regex")
            and not kwargs.get("data_id_regex")
            and not kwargs.get("new_name_regex")
            and not kwargs.get("new_description_regex")
            and not kwargs.get("new_data_id_regex")
    ):
        # This is "identical datasources" mode, where the names of the items in each datasource are the same.
        # So we just filter on the name and type, assuming that combination is unique
        new_filter += " && Name==%s" % row[old_items["name_index"]]
        types_filter = [row[old_items["type_index"]]]
    else:
        capture_groups = dict()
        new_name_regex = None

        if kwargs.get("name_regex"):
            capture_groups.update(
                re.match(
                    _pythonize_regex_capture_group_names(kwargs.get("name_regex")),
                    row[old_items["name_index"]],
                ).groupdict()
            )

        if kwargs.get("new_name_regex"):
            new_name_regex = kwargs.get("new_name_regex")
            new_name_regex = _replace_tokens_in_regex(new_name_regex, capture_groups)

        if new_name_regex:
            new_filter += " && Name ~= /%s/" % new_name_regex

        new_description_regex = None

        if kwargs.get("description_regex"):
            capture_groups.update(
                re.match(
                    _pythonize_regex_capture_group_names(
                        kwargs.get("description_regex")
                    ),
                    row[old_items["description_index"]],
                ).groupdict()
            )

        if kwargs.get("new_description_regex"):
            new_description_regex = kwargs.get("new_description_regex")
            new_description_regex = _replace_tokens_in_regex(
                new_description_regex, capture_groups
            )

        if new_description_regex:
            new_filter += " && Description ~= /%s/" % new_description_regex

        new_data_id_regex = None

        if kwargs.get("data_id_regex"):
            capture_groups.update(
                re.match(
                    _pythonize_regex_capture_group_names(kwargs.get("data_id_regex")),
                    row[old_items["data_id_index"]],
                ).groupdict()
            )

        if kwargs.get("new_data_id_regex"):
            new_data_id_regex = kwargs.get("new_data_id_regex")
            new_data_id_regex = _replace_tokens_in_regex(
                new_data_id_regex, capture_groups
            )

        if new_data_id_regex:
            new_filter += " && Data ID == %s" % new_data_id_regex

    search_results = items_api.search_items(
        filters=[new_filter, SeeqNames.API.Flags.include_unsearchable],
        types=types_filter,
        offset=0,
        limit=2,
    )  # type: ItemSearchPreviewPaginatedListV1

    if len(search_results.items) == 1:
        new_item = search_results.items[0]

        datasource_id = items_api.get_property(
            id=new_item.id, property_name=SeeqNames.Properties.datasource_id
        )  # type: PropertyOutputV1

        data_id = items_api.get_property(
            id=new_item.id, property_name=SeeqNames.Properties.data_id
        )  # type: PropertyOutputV1

        new_id = new_item.id
        new_name = new_item.name
        new_type = new_item.type
        new_description = new_item.description
        new_datasource_id = datasource_id.value
        new_data_id = data_id.value
    elif len(search_results.items) > 1:
        new_id = "MULTIPLE MATCHES"
        new_name = "MULTIPLE MATCHES"
        new_type = "MULTIPLE MATCHES"
        new_description = "MULTIPLE MATCHES"
        new_datasource_id = "MULTIPLE MATCHES"
        new_data_id = "MULTIPLE MATCHES"
    else:
        new_id = "NO MATCHES"
        new_name = "NO MATCHES"
        new_type = "NO MATCHES"
        new_description = "NO MATCHES"
        new_datasource_id = "NO MATCHES"
        new_data_id = "NO MATCHES"

    return shared.Bunch(
        id=row[old_items["id_index"]],
        name=row[old_items["name_index"]],
        type=row[old_items["type_index"]],
        description=row[old_items["description_index"]],
        datasource_id=row[old_items["datasource_id_index"]],
        data_id=row[old_items["data_id_index"]],
        new_id=new_id,
        new_type=new_type,
        new_name=new_name,
        new_description=new_description,
        new_datasource_id=new_datasource_id,
        new_data_id=new_data_id,
    )


def _process_cache_fill_on_item(item, client, start, end, ignore_errors):
    formulas_api = sdk.FormulasApi(client)

    formula = (
            'group(capsule("%s","%s")).toTable("stats").addStatColumn("series",$series,count())'
            % (start, end)
    )

    parameter = "series=%s" % item.id

    while True:
        try:
            formula_run_output, _, http_headers = (
                formulas_api.run_formula_with_http_info(
                    formula=formula, parameters=[parameter]
                )
            )
            break
        except MaxRetryError:
            # Server is down, keep trying in this loop
            print("Seeq Server appears to be down, trying to reconnect")
        except ApiException as e:
            if e.status == 401:
                # This can happen if the server is restarted while the script is running
                print("Logging in again")
                client = spy.client
            else:
                print("Seeq Server error encountered: %s" % e)

        except Exception as e:
            print("Seeq Server error encountered: %s" % e)

        if ignore_errors:
            # If we're asked to ignore errors, then stop retrying the current item and just log an error to move on.
            # This prevents the script from getting hung up on a single item.
            return [("new_samples", "ERROR"), ("all_samples", "ERROR")]

        time.sleep(5)

    all_samples_in_range = int(formula_run_output.table.data[0][2])

    server_meters_string = http_headers[SeeqNames.API.Headers.server_meters]

    samples_read_from_datasource = 0
    server_meters = server_meters_string.split(",")
    for server_meter_string in server_meters:
        server_meter = server_meter_string.split(";")
        if len(server_meter) < 3:
            continue

        dur_string = server_meter[1]
        desc_string = server_meter[2]

        dur_value = int(dur_string.split("=")[1])

        if (
                desc_string
                == f'desc="{SeeqNames.API.Headers.Meters.datasource_samples_read}"'
        ):
            samples_read_from_datasource = int(dur_value)
            break

    return [
        ("new_samples", samples_read_from_datasource),
        # New samples may be less than all samples due to boundary values, so take the max below so that it isn't
        # 'incongruent' in users' eyes.
        ("all_samples", max(all_samples_in_range, samples_read_from_datasource)),
    ]


def _get_cache_fill_command_item_filter(client, **kwargs):
    datasource_item = get_datasource_item(client, **kwargs)

    _filter = " && Datasource Class==" + datasource_item.datasource_class
    _filter += " && Datasource ID==" + datasource_item.datasource_id

    _filter += " && Cache Enabled == true"

    return _filter


def _cache_file_input_csv(**kwargs):
    try:
        if not util.safe_exists(kwargs.get("input_csv")):
            print('Path "%s" does not exist.' % kwargs.get("input_csv"))
            return list(), 0

        if util.safe_isdir(kwargs.get("input_csv")):
            input_files = [
                os.path.join(kwargs.get("input_csv"), f)
                for f in os.listdir(kwargs.get("input_csv"))
            ]
        else:
            input_files = [kwargs.get("input_csv")]

        latest_mtime = 0
        for input_file in input_files:
            latest_mtime = max(latest_mtime, os.path.getmtime(input_file))

        return input_files, latest_mtime

    except Exception as e:
        print("Exception encountered: %s" % e)
        return list(), 0


def _cache_fill_items_from_item_id(client, item_id, _types, item_name):
    items_api = sdk.ItemsApi(client)

    try:
        item = items_api.get_item_and_all_properties(id=item_id)  # type: ItemOutputV1
        if item.type not in _types:
            return list()

        cache_enabled = False
        for _property in item.properties:  # type: PropertyOutputV1
            if _property.name == "Cache Enabled" and _property.value == "true":
                cache_enabled = True
                break

        if not cache_enabled:
            return list()

        return [
            shared.Bunch(
                name=item.name,
                type=item.type,
                id=item.id,
                new_samples=0,
                all_samples=0,
                total_time_str="",
                action_time_str="",
                start="",
                end="",
            )
        ]
    except Exception:
        return [
            shared.Bunch(
                name=item_name,
                type="",
                id="NOT FOUND",
                new_samples=0,
                all_samples=0,
                total_time_str="",
                action_time_str="",
                start="",
                end="",
            )
        ]


def _cache_fill_items_from_item_name(client, _filter, _types, item_name, **kwargs):
    items_api = sdk.ItemsApi(client)

    if not _filter:
        _filter = _get_cache_fill_command_item_filter(client, **kwargs)

    search_results = items_api.search_items(
        filters=[_filter + "&& Name == " + item_name], types=_types
    )  # type: ItemSearchPreviewPaginatedListV1
    if len(search_results.items) > 0:
        return search_results.items
    else:
        return [
            shared.Bunch(
                name=item_name,
                type="",
                id="NOT FOUND",
                new_samples=0,
                all_samples=0,
                total_time_str="",
                action_time_str="",
                start="",
                end="",
            )
        ]


class InputFilesChanged(Exception):
    def __init__(self):
        pass


def _cache_fill_process_via_input_files(
        client, _types, _filter, current_start_iso, current_end_iso, **kwargs
):
    input_files, latest_mtime = _cache_file_input_csv(**kwargs)

    if len(input_files) == 0:
        print("No CSV files to process.")
        time.sleep(5)
        return

    column_specs = [
        ColumnSpec("count", "#", ">", 8),
        ColumnSpec("action_time_str", "Time", ">", 8),
        ColumnSpec("total_time_str", "Total", ">", 8),
        ColumnSpec("new_samples", "New", ">", 8),
        ColumnSpec("all_samples", "All", ">", 8),
        ColumnSpec("start", "Start", "<", 20),
        ColumnSpec("end", "End", "<", 20),
        ColumnSpec("name", "Name", "<", 40),
        ColumnSpec("type", "Type", "<", 15, console=False),
        ColumnSpec("id", "ID", "<", 36),
    ]

    try:
        for input_file in input_files:
            print('Processing "%s"...' % input_file)

            total_start = time.time()

            header, rows = tables.read_table(input_file)

            id_column = tables.find_column_index(header, "ID")
            name_column = tables.find_column_index(header, "Name")
            duration_column = tables.find_column_index(header, "Duration")

            if name_column is None and id_column is None:
                raise RuntimeError('"ID" nor "Name" column not found in input CSV file')

            table = tables.start_table(column_specs, kwargs.get("csv"))

            try:
                for row in rows:
                    try:
                        new_input_files, new_latest_mtime = _cache_file_input_csv(
                            **kwargs
                        )
                        if kwargs.get("continuous") and (
                                latest_mtime != new_latest_mtime
                                or input_files != new_input_files
                        ):
                            raise InputFilesChanged()

                        item_id = row[id_column] if id_column is not None else None
                        item_name = (
                            row[name_column] if name_column is not None else None
                        )

                        if duration_column is not None:
                            duration = int(row[duration_column])
                            this_end = datetime.now(utc)
                            this_start = this_end - timedelta(hours=duration)
                            current_start_iso = this_start.strftime(
                                "%Y-%m-%dT%H:%M:%SZ"
                            )
                            current_end_iso = this_end.strftime("%Y-%m-%dT%H:%M:%SZ")

                        items = []
                        if item_id:
                            items = _cache_fill_items_from_item_id(
                                client, item_id, _types, item_name
                            )
                        elif item_name:
                            items = _cache_fill_items_from_item_name(
                                client, _filter, _types, item_name, **kwargs
                            )

                        for _item in items:
                            action_start = time.time()
                            if _item.id != "NOT FOUND":
                                fill_metrics = _process_cache_fill_on_item(
                                    _item,
                                    client,
                                    current_start_iso,
                                    current_end_iso,
                                    kwargs.get("ignore_errors"),
                                )
                                action_time_sec = time.time() - action_start
                                total_time_sec = time.time() - total_start
                                _, new_samples = fill_metrics[0]
                                _, all_samples = fill_metrics[1]
                                _item = shared.Bunch(
                                    name=_item.name,
                                    type=_item.type,
                                    id=_item.id,
                                    new_samples=str(new_samples),
                                    all_samples=str(all_samples),
                                    total_time_str=tables._print_time_string(
                                        total_time_sec
                                    ),
                                    action_time_str=tables._print_time_string(
                                        action_time_sec
                                    ),
                                    start=current_start_iso,
                                    end=current_end_iso,
                                )
                            tables.write_table_row(table, _item)
                    except InputFilesChanged:
                        raise
                    except Exception as e:
                        if kwargs.get("continuous") and kwargs.get("ignore_errors"):
                            print(
                                "Error encountered on row, ignoring:\n%s\n%s" % (row, e)
                            )
                        else:
                            raise
            finally:
                tables.finish_table(table)

    except InputFilesChanged:
        print('File(s) "%s" have changed, restarting' % kwargs.get("input_csv"))


def _cache_fill_process_via_item_search(
        client, _types, _filter, current_start_iso, current_end_iso, **kwargs
):
    items_api = sdk.ItemsApi(client)

    print(
        "\nLooping through items to cache from %s to %s\n"
        % (current_start_iso, current_end_iso)
    )

    column_specs = [
        ColumnSpec("count", "#", ">", 8),
        ColumnSpec("action_time_str", "Time", ">", 8),
        ColumnSpec("total_time_str", "Total", ">", 8),
        ColumnSpec("new_samples", "New", ">", 8),
        ColumnSpec("all_samples", "All", ">", 8),
        ColumnSpec(
            "ancestors",
            "Path",
            ">",
            40,
            lambda ancestors, _item: _ancestor_path(ancestors),
        ),
        ColumnSpec("name", "Name", "<", 40),
        ColumnSpec("type", "Type", "<", 15, console=False),
        ColumnSpec("id", "ID", "<", 36, console=False),
    ]

    if not _filter:
        _filter = _get_cache_fill_command_item_filter(client, **kwargs)

    if kwargs.get("filter"):
        _filter += " && Name ~= " + kwargs.get("filter")

    tables.print_table_from_sdk_output(
        lambda offset: items_api.search_items(
            filters=[_filter],
            offset=offset,
            types=_types,
            limit=helper.get_default_page_size(),
            order_by=SORT_BY_ITEM_GUID,
        ),
        "items",
        column_specs,
        lambda _item: _process_cache_fill_on_item(
            _item,
            client,
            current_start_iso,
            current_end_iso,
            kwargs.get("ignore_errors"),
        ),
        csv_file_name=kwargs.get("csv"),
    )


def cache_fill(**kwargs):
    """
    Fill the cache with data from the datasource.
    :param datasource_name: Datasource Name of datasource to search.
    :param datasource_class: Datasource Class of datasource to search.
    :param datasource_id: Datasource ID of datasource to search.
    :param id: Seeq ID of datasource to search.
    :param input_csv: CSV file with ID or Name column for items to affect.
    :param start: start date for fill (e.g. 2018-08-02T00:00:00Z).
    :param end: end date for fill (e.g. 2018-09-31T00:00:00Z).
    :param continuous: continuously fill the cache (Do not specify end).
    :param ignore_errors: ignore errors while accessing data.
    :param minimum: minimum query size (in hours).
    :param csv: name of CSV file to route output to.
    """
    kwargs["continuous"] = kwargs.get("continuous") or False
    kwargs["ignore_errors"] = kwargs.get("ignore_errors") or False

    client = spy.client

    current_start = (
        dateutil.parser.parse(kwargs.get("start"))
        if kwargs.get("start")
        else datetime.now(utc)
    )

    _types = ["StoredSignal", "CalculatedSignal"]
    _filter = None

    minimum = int(kwargs.get("minimum") or 0)

    while True:
        current_end = (
            dateutil.parser.parse(kwargs.get("end"))
            if kwargs.get("end")
            else datetime.now(utc)
        )

        try:
            time_delta = current_end - current_start
            if (time_delta.total_seconds() / 60 / 60) < minimum:
                current_start = current_end - timedelta(hours=minimum)

            current_start_iso = current_start.strftime("%Y-%m-%dT%H:%M:%SZ")
            current_end_iso = current_end.strftime("%Y-%m-%dT%H:%M:%SZ")

            if kwargs.get("filter") and kwargs.get("input_csv"):
                raise RuntimeError(
                    "filter and input-csv arguments are mutually exclusive"
                )

            if kwargs.get("input_csv"):
                _cache_fill_process_via_input_files(
                    client,
                    _types,
                    _filter,
                    current_start_iso,
                    current_end_iso,
                    **kwargs,
                )
            else:
                _cache_fill_process_via_item_search(
                    client,
                    _types,
                    _filter,
                    current_start_iso,
                    current_end_iso,
                    **kwargs,
                )

        except ApiException as e:
            if e.status == 401:
                print("Logging in again")
                client = spy.client
            else:
                if kwargs.get("continuous") and kwargs.get("ignore_errors"):
                    print("Error encountered, ignoring: %s" % e)
                else:
                    raise
        except Exception as e:
            if kwargs.get("continuous") and kwargs.get("ignore_errors"):
                print("Error encountered, ignoring: %s" % e)
            else:
                raise

        if not kwargs.get("continuous"):
            break

        current_start = current_end

        time.sleep(5)

    if not kwargs.get("continuous"):
        print("\nPersistent cache FILLED for all of the above items.")
        print("Note: Only items with caching ON were affected.")
        print(
            'Note: Time ranges near "now" may not be fully cached to allow for soon-arriving data.'
        )
