import csv
import json
import logging
import re
import uuid
from typing import List

from seeq.base.seeq_names import SeeqNames
from seeq import sdk, spy
from seeq.sdk.rest import ApiException
from seeq.sdk import *
from seeq.base import util


from source import helper, shared, tables
from source.tables import ColumnSpec

GUID_REGEX = r"[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}"


def items(**kwargs):
    """
    List datasource items in use within workbooks.
    :param csv: CSV file to write items to.
    :param workbook_id: Id of the workbook whose items will be returned.
    :param workbook_csv: CSV file with workbook ids.
    :param ignore_errors: Ignore errors.
    :param include_calculations: Include formulas in the items returned.
    """
    ignore_errors = kwargs.get("ignore_errors") or False
    include_calculations = kwargs.get("include_calculations") or False

    get_workbook_items(
        kwargs.get("csv"),
        kwargs.get("workbook_id"),
        kwargs.get("workbook_csv"),
        ignore_errors,
        True,
        include_calculations,
        False,
        False,
    )
    return None


def get_workbook_items(
    local_csv,
    workbook_id,
    workbook_csv,
    ignore_errors,
    include_dependents,
    include_calculations,
    only_include_supports_caching,
    only_include_leaf_calculations,
):
    """
    This method returns a list of items in the given workbook(s)/topic(s).
    :param local_csv: CSV file to write items to.
    :param workbook_id: Id of the workbook whose items will be returned.
    :param workbook_csv: CSV file with workbook ids.
    :param ignore_errors: Ignore errors.
    :param include_dependents: Include indirectly referenced items.
    :param include_calculations: Include formulas in the items returned.
    :param only_include_supports_caching: Only include items that can be cached.
    :param only_include_leaf_calculations: Only include formulas that don't depend on other items.
           If this flag is used without the include_calculations flag, it has no effect.
    :return: List of items
    """
    client = spy.client
    items_api = sdk.ItemsApi(client)
    datasources_api = sdk.DatasourcesApi(client)
    workbooks_api = sdk.WorkbooksApi(client)

    datasources = datasources_api.get_datasources(
        limit=helper.get_default_page_size(), include_archived=True
    )  # type: DatasourceOutputListV1

    column_specs = [
        ColumnSpec("count", "#", ">", 4),
        ColumnSpec("workbook_id", "Workbook ID", console=False),
        ColumnSpec("workbook_name", "Workbook Name", "<", 36),
        ColumnSpec("workbook_owner", "Workbook Owner", "<", 25),
        ColumnSpec("workbook_type", "Workbook Type", console=False),
        ColumnSpec("updated_at", "Workbook Updated", console=False),
        ColumnSpec("name", "Item Name", "<", 40),
        ColumnSpec("type", "Item Type", console=False),
        ColumnSpec("used_in", "Used In", console=False),
        ColumnSpec("item_id", "Item ID", console=False),
        ColumnSpec("datasource", "Datasource", "<", 30),
        ColumnSpec("id_datasource", "ID (Datasource)", console=False),
        ColumnSpec("data_id", "Data ID", console=False),
        ColumnSpec("max_interp_or_duration", "Max Interp/Duration", console=False),
        ColumnSpec("formula", "Formula", console=False),
        ColumnSpec("formula_inputs", "Formula Inputs", console=False),
    ]

    table = tables.start_table(column_specs, local_csv)

    if not workbook_id and not workbook_csv:
        raise RuntimeError("You must specify either --workbook-id or --workbook-csv")

    items = []
    workbook_ids = []
    if workbook_id:
        workbook_ids = [workbook_id]
    elif workbook_csv:
        header, rows = tables.read_table(workbook_csv)

        id_column = tables.find_column_index(header, "ID")

        if id_column is None:
            raise RuntimeError('"ID" column not found in workbook CSV file')

        workbook_ids = [row[id_column] for row in rows]

    try:
        for workbook_id in workbook_ids:
            try:
                workbook = workbooks_api.get_workbook(
                    id=workbook_id
                )  # type: WorkbookOutputV1
                worksheets = workbooks_api.get_worksheets(
                    workbook_id=workbook.id
                )  # type: WorksheetOutputListV1
            except ApiException as e:
                if e.status == 403:
                    item = shared.Bunch(
                        workbook_id=workbook_id,
                        workbook_name="Access denied",
                        workbook_owner="Access denied",
                        workbook_type="Access denied",
                        updated_at="",
                        item_id="Access denied",
                        type="",
                        name="",
                        datasource="",
                        ids_datasource="",
                        data_id="",
                        used_in="",
                        max_interp_or_duration="",
                        formula="",
                        formula_inputs="",
                    )

                    tables.write_table_row(table, item)

                    if not ignore_errors:
                        raise_admin_credentials_error()

                elif not ignore_errors:
                    raise

                continue

            item_ids = dict()
            for worksheet in worksheets.worksheets:  # type: WorksheetOutputV1

                try:
                    if include_dependents:
                        item_ids.update(
                            _scrape_item_ids_from_calculated_items(
                                client, workbook_id, include_calculations
                            )
                        )
                    item_ids.update(
                        _scrape_item_ids_from_workbook(
                            client, workbook, worksheet, ignore_errors
                        )
                    )
                except Exception:
                    if not ignore_errors:
                        raise
                    else:
                        continue

            item_ids_copy = dict(item_ids)
            for item_id, _ in item_ids_copy.items():
                if include_dependents:
                    item_ids.update(
                        _scrape_item_ids_from_dependencies(client, item_id, item_ids)
                    )
            for item_id, used_in in item_ids.items():
                try:
                    item = items_api.get_item_and_all_properties(
                        id=item_id
                    )  # type: ItemOutputV1
                    item_type = item.type if item else ""
                except ApiException:
                    continue
                cachable = [
                    "StoredSignal",
                    "CalculatedSignal",
                    "StoredCondition",
                    "CalculatedCondition",
                    "CalculatedScalar",
                ]
                if item_type in cachable or not only_include_supports_caching:
                    datasource_id = _get_property(item, "Datasource ID")
                    datasource_class = _get_property(item, "Datasource Class")
                    data_id = _get_property(item, "Data ID")

                    if not include_calculations:
                        if (
                            not datasource_id
                            or not data_id
                            or datasource_id == "default"
                        ):
                            continue

                    is_generated = _get_property(item, "Is Generated")
                    if is_generated:
                        # We exclude generated items, like scalars on metrics, because they are just noise for the user
                        continue

                    datasource = _get_datasource(
                        datasources, datasource_id, datasource_class
                    )
                    datasource_name = datasource.name if datasource else ""
                    id_datasource = datasource.id if datasource else ""
                    item_name = item.name if item else ""

                    max_interp_or_duration = None
                    if "Signal" in item_type:
                        max_interp_or_duration = _get_property(
                            item, "Override Maximum Interpolation"
                        )
                        if not max_interp_or_duration:
                            max_interp_or_duration = _get_property(
                                item, "Source Maximum Interpolation"
                            )
                    elif "Condition" in item_type:
                        max_interp_or_duration = _get_property(item, "Maximum Duration")
                        if not max_interp_or_duration:
                            max_interp_or_duration = _get_property(
                                item, "maximumDuration"
                            )

                    formula = None
                    formula_inputs = None
                    if item_type.startswith("Calculated"):
                        formula = _get_property(item, "Formula")

                        inputs = []
                        calculated_item = _get_calculated_item(client, item)
                        if calculated_item is None:
                            continue
                        for (
                            parameter
                        ) in (
                            calculated_item.parameters
                        ):  # type: FormulaParameterOutputV1
                            reference = (
                                parameter.item.id
                                if parameter.item
                                else parameter.formula
                            )
                            inputs.append("$%s=%s" % (parameter.name, reference))
                        formula_inputs = "\n".join(inputs)

                    item = shared.Bunch(
                        workbook_id=workbook_id,
                        workbook_name=workbook.name,
                        workbook_owner=workbook.owner.name,
                        workbook_type=workbook.type,
                        updated_at=workbook.updated_at,
                        item_id=item_id,
                        type=item_type,
                        name=item_name,
                        datasource=datasource_name,
                        id_datasource=id_datasource,
                        data_id=data_id,
                        used_in=used_in,
                        max_interp_or_duration=max_interp_or_duration,
                        formula=formula,
                        formula_inputs=formula_inputs,
                    )

                    is_calculation = item.formula_inputs is not None
                    is_leaf_calculation = (
                        is_calculation and "$" not in item.formula_inputs
                    )
                    if (
                        (not include_calculations)
                        or (not is_calculation)
                        or (not only_include_leaf_calculations)
                        or is_leaf_calculation
                    ):
                        items.append(item)
                        tables.write_table_row(table, item)
    finally:
        if table:
            tables.finish_table(table)

    return items


def _scrape_item_ids_from_dependencies(client, item_id, item_ids):
    items_api = sdk.ItemsApi(client)

    dependencies = None
    try:
        dependencies = items_api.get_formula_dependencies(
            id=item_id
        )  # type: ItemDependencyOutputV1
    except ApiException as e:
        if e.status == 404:
            # For some reason, the item_id is unknown. So just skip it.
            print("Ignoring: %s" % e)
        else:
            print(f"API error {e}")

    if dependencies and dependencies.dependencies:
        for dependency in dependencies.dependencies:  # type: ItemParameterOfOutputV1
            if dependency.id not in item_ids:
                # We specify "Global Calc" here because the only way we'll make it to this line is if this item is a
                # dependency of calculation that wasn't already found in the calculations scoped to the workbook
                item_ids[dependency.id] = "Global Calc"

    return item_ids


def _scrape_item_ids_from_calculated_items(
    client, workbook_id, include_calculated_item
):
    items_api = sdk.ItemsApi(client)

    item_ids = dict()

    tables.iterate_over_output(
        lambda offset: items_api.search_items(
            filters=["", SeeqNames.API.Flags.exclude_globally_scoped],
            scope=[workbook_id],
            offset=offset,
            limit=helper.get_default_page_size(),
            types=[
                "CalculatedScalar",
                "CalculatedSignal",
                "CalculatedCondition",
                "Chart",
                "ThresholdMetric",
            ],
        ),
        "items",
        lambda item: _scrape_item_ids_from_calculated_item(
            client, item, item_ids, include_calculated_item
        ),
    )

    return item_ids


def _get_calculated_item(client, item):
    scalars_api = sdk.ScalarsApi(client)
    signals_api = sdk.SignalsApi(client)
    conditions_api = sdk.ConditionsApi(client)
    formulas_api = sdk.FormulasApi(client)
    metrics_api = sdk.MetricsApi(client)

    try:
        if item.type == "CalculatedScalar":
            item = scalars_api.get_scalar(id=item.id)  # type: CalculatedItemOutputV1
        elif item.type == "CalculatedSignal":
            item = signals_api.get_signal(id=item.id)  # type: SignalOutputV1
        elif item.type == "CalculatedCondition":
            item = conditions_api.get_condition(id=item.id)  # type: ConditionOutputV1
        elif item.type == "Chart":
            item = formulas_api.get_function(id=item.id)  # type: CalculatedItemOutputV1
        elif item.type == "ThresholdMetric":
            item = metrics_api.get_metric(id=item.id)  # type: ThresholdMetricOutputV1
    except Exception as e:
        return None
        print(f"Error getting calculated item: {e}")

    return item


def _scrape_item_ids_from_calculated_item(
    client, item, item_ids, include_calculated_item
):
    item = _get_calculated_item(client, item)
    if item is None:
        return

    if not item.scoped_to:
        return

    if include_calculated_item:
        item_ids[item.id] = "Workbook Calc"

    if item.type == "ThresholdMetric":
        metric = item  # type: ThresholdMetricOutputV1
        if metric.measured_item:
            item_ids[metric.measured_item.id] = "Metric Measured Item"

        if metric.bounding_condition:
            item_ids[metric.bounding_condition.id] = "Metric Bounding Condition"

        if metric.thresholds:
            for threshold in metric.thresholds:  # type: ThresholdOutputV1
                if threshold.is_generated or threshold.value:
                    continue

                item_ids[threshold.item.id] = "Metric Threshold Item"

    else:
        for parameter in item.parameters:  # type: FormulaParameterOutputV1
            if parameter.item:
                item_ids[parameter.item.id] = "Calculation"


def _scrape_item_ids_from_workstep(client, workbook_id, worksheet, workstep_id):
    workbooks_api = sdk.WorkbooksApi(client)

    item_ids = dict()

    try:
        workstep = workbooks_api.get_workstep(
            workbook_id=workbook_id, worksheet_id=worksheet.id, workstep_id=workstep_id
        )
    except ApiException as e:
        if e.status == 404:
            # Apparently it's possible for a workstep to be referenced and yet not exist
            return item_ids

        raise

    matches = re.finditer(GUID_REGEX, workstep.data, re.IGNORECASE)

    for match in matches:
        item_ids[match.group(0).upper()] = "Details (%s)" % worksheet.name

    return item_ids


def _scrape_item_ids_from_workbook(client, workbook, worksheet, ignore_errors=False):
    if _is_organizer_topic(workbook):
        return _scrape_item_ids_from_topic(client, worksheet, ignore_errors)
    else:
        return _scrape_item_ids_from_analysis(client, workbook, worksheet)


def _scrape_item_ids_from_analysis(client, workbook, worksheet):
    annotations_api = sdk.AnnotationsApi(client)

    item_ids = dict()

    if not worksheet.workstep:
        # It's possible for a workstep to have never been posted to a worksheet
        return item_ids

    workstep_id = worksheet.workstep.split("/")[-1]
    item_ids.update(
        _scrape_item_ids_from_workstep(client, workbook.id, worksheet, workstep_id)
    )

    annotations = annotations_api.get_annotations(
        annotates=[worksheet.id]
    )  # type: AnnotationListOutputV1

    for annotation in annotations.items:  # type: AnnotationOutputV1
        annotation = annotations_api.get_annotation(id=annotation.id)

        if not annotation.document:
            continue

        matches = re.finditer(
            r"item&#61;(" + GUID_REGEX + r")", annotation.document, re.IGNORECASE
        )
        for match in matches:
            item_ids[match.group(1).upper()] = "Journal (%s)" % worksheet.name

        matches = re.finditer(
            r"workstep&#61;(" + GUID_REGEX + r")", annotation.document, re.IGNORECASE
        )
        for match in matches:
            item_ids.update(
                _scrape_item_ids_from_workstep(
                    client, workbook.id, worksheet, match.group(1).upper()
                )
            )

    return item_ids


def _scrape_item_ids_from_topic(client, worksheet, ignore_errors=False):
    annotations_api = skd.AnnotationsApi(client)
    workbooks_api = sdk.WorkbooksApi(client)
    content_api = sdk.ContentApi(client)

    item_ids = dict()

    annotations = annotations_api.get_annotations(
        annotates=[worksheet.id]
    )  # type: AnnotationListOutputV1

    for annotation in annotations.items:  # type: AnnotationOutputV1
        annotation = annotations_api.get_annotation(id=annotation.id)
        all_contents: List[ContentOutputV1] = [
            content_api.get_content(id=content_id)
            for content_id in annotation.content_ids
        ]

        if not annotation.document:
            continue

        for content in all_contents:
            try:
                worksheet = workbooks_api.get_worksheet(
                    workbook_id=content.source_workbook,
                    worksheet_id=content.source_worksheet,
                )
                item_ids.update(
                    _scrape_item_ids_from_workstep(
                        client,
                        content.source_workbook,
                        worksheet,
                        content.source_workstep,
                    )
                )
                content_api.clear_image_cache(id=content.id)
            except ApiException as e:
                if not ignore_errors:
                    raise

                print(
                    'Could not get items from worksheet "%s", skipping. Exception:\n%s'
                    % (content.source_worksheet, e)
                )
                continue

    for key, value in item_ids.items():
        item_ids[key] = "Embedded Content"

    return item_ids


def _is_organizer_topic(workbook):
    return workbook.type == "Topic"


def _get_property(item, property_name):
    """
    :type item: ItemOutputV1
    """
    for prop in item.properties:  # type: PropertyOutputV1
        if prop.name == property_name:
            if prop.unit_of_measure and prop.unit_of_measure != "string":
                return prop.value + prop.unit_of_measure
            else:
                return prop.value


def _get_datasource(datasources, datasource_id, datasource_class):
    """
    :type datasources: DatasourceOutputListV1
    """
    for datasource in datasources.datasources:  # type: DatasourceOutputV1
        if (
            datasource.datasource_id == datasource_id
            and datasource.datasource_class == datasource_class
        ):
            return datasource


def _find_user(user_spec, client):
    users_api = sdk.UsersApi(client)
    users = users_api.get_users(limit=1000000)  # type: UserOutputListV1

    found_users = []
    for user in users.users:  # type: UserOutputV1
        if (
            user.name.lower() == user_spec.lower()
            or user.id.lower() == user_spec.lower()
            or user.username.lower() == user_spec.lower()
        ):
            found_users.append(user)

    if len(found_users) == 0:
        raise RuntimeError('User "%s" not found' % user_spec)

    if len(found_users) > 1:
        table = tables.tables.start_table(
            [
                ColumnSpec("name", "Name", "<", 40),
                ColumnSpec("id", "ID", "<", 36),
                ColumnSpec("username", "Username", "<", 40),
                ColumnSpec("datasource_name", "Directory", "<", 40),
            ]
        )

        for found_user in found_users:
            tables.tables.write_table_row(table, found_user)

        tables.tables.finish_table(table)

        raise RuntimeError(
            'Multiple users found that match "%s" (see above), specify ID field to narrow down.'
            % user_spec
        )

    return found_users[0]


def workbook_list(**kwargs):
    """
    List workbooks datasources with enclosing folder information.
    :param filter: [{ALL,PUBLIC,OWNER,SHARED}]
                        Filter to apply to the workbooks returned
    :param csv: CSV file to write items to.
    :param archived: Return only trashed/archived workbooks
    :param owner: Filter for particular owner (specify name, username or ID).
    :param ignore_errors: Ignore errors.
    """

    filter = kwargs.get("filter") or "OWNER"
    archived = kwargs.get("archived") or False
    ignore_errors = kwargs.get("ignore_errors") or False

    client = spy.client

    folders_api = sdk.FoldersApi(client)
    projects_api = sdk.ProjectsApi(client)
    workbooks_api = sdk.WorkbooksApi(client)

    user_id = None
    if kwargs.get("owner"):
        user_id = _find_user(kwargs.get("owner"), client).id

    types_to_search = [
        SeeqNames.Types.analysis,
        SeeqNames.Types.topic,
        SeeqNames.Types.folder,
    ]
    if user_id:
        folders = folders_api.get_folders(
            filter=filter,
            is_archived=archived,
            user_id=user_id,
            limit=100000,
            types=types_to_search,
        )  # type: WorkbenchItemOutputListV1
    else:
        folders = folders_api.get_folders(
            filter=filter, is_archived=archived, limit=100000, types=types_to_search
        )  # type: WorkbenchItemOutputListV1

    folder_stack = list()
    folder_stack.append(folders)

    content_index_stack = list()
    content_index_stack.append(0)

    column_specs = [
        ColumnSpec("count", "#", ">", 4),
        ColumnSpec("path", "Folder", ">", 36),
        ColumnSpec("name", "Name", "<", 36),
        ColumnSpec("type", "Type", "<", 10),
        ColumnSpec("owner", "Owner", "<", 30),
        ColumnSpec("id", "ID", "<", 36),
        ColumnSpec("created_at", "Created At", "<", 24, console=False),
        ColumnSpec("updated_at", "Updated At", "<", 24, console=False),
    ]

    table = tables.start_table(column_specs, kwargs.get("csv"))

    try:
        while True:
            folders = folder_stack[-1]

            if content_index_stack[-1] >= len(folders.content):
                folder_stack.pop()

                if len(folder_stack) == 0:
                    break

                content_index_stack.pop()
                content_index_stack[-1] += 1

                continue

            base_output = folders.content[content_index_stack[-1]]  # type: BaseOutput

            if base_output.type == "Folder":
                try:
                    folders = folders_api.get_folders(
                        filter=filter, folder_id=base_output.id, limit=100000
                    )  # type: WorkbenchItemOutputListV1

                    folder_stack.append(folders)

                    content_index_stack.append(0)
                except Exception:
                    content_index_stack[-1] += 1
                    if not ignore_errors:
                        raise
            elif base_output.type == "Project":
                try:
                    project = projects_api.get_project(
                        id=base_output.id
                    )  # type: ProjectOutputV1

                    owner = project.owner  # type: IdentityPreviewV1
                    item = shared.Bunch(
                        id=project.id,
                        name=project.name,
                        description=project.description,
                        type=project.type,
                        owner=owner.name,
                        created_at=project.created_at,
                        updated_at=project.updated_at,
                        path=_get_folder_path(folder_stack, content_index_stack),
                    )

                    tables.write_table_row(table, item)
                except Exception:
                    if not ignore_errors:
                        raise

                content_index_stack[-1] += 1
            else:  # Analysis or Topic (use Workbooks API)
                try:
                    workbook = workbooks_api.get_workbook(
                        id=base_output.id
                    )  # type: WorkbookOutputV1

                    owner = workbook.owner  # type: IdentityPreviewV1
                    item = shared.Bunch(
                        id=workbook.id,
                        name=workbook.name,
                        description=workbook.description,
                        type=workbook.type,
                        owner=owner.name,
                        created_at=workbook.created_at,
                        updated_at=workbook.updated_at,
                        path=_get_folder_path(folder_stack, content_index_stack),
                    )

                    tables.write_table_row(table, item)
                except Exception:
                    if not ignore_errors:
                        raise

                content_index_stack[-1] += 1
    finally:
        if table:
            tables.finish_table(table)


def _get_folder_path(folder_stack, content_index_stack):
    path = ""
    for i in range(0, len(folder_stack) - 1):
        if len(path) > 0:
            path = path + " "

        path += str(folder_stack[i].content[content_index_stack[i]].name) + " >>"

    return path


def clear_cache(**kwargs):
    """
    Clear cache of items in workbook.
    :param csv: CSV file to write items to.
    :param workbook_id: Seeq ID of workbook (either topic or analysis) to
                        clear item caches for
    :param workbook_csv: Iterate over workbooks in CSV file produced by "seeq
                        workbook list"
    :param ignore_errors: Ignore errors.
    :param recursive: Recursively clear item caches.
    """

    ignore_errors = kwargs.get("ignore_errors")
    recursive = kwargs.get("recursive")

    client = spy.client
    items_api = sdk.ItemsApi(client)

    if not kwargs.get("workbook_id") and not kwargs.get("workbook_csv"):
        raise RuntimeError("You must specify either workbook_id or workbook_csv")
    items = get_workbook_items(
        kwargs.get("csv"),
        kwargs.get("workbook_id"),
        kwargs.get("workbook_csv"),
        ignore_errors,
        recursive,
        True,
        True,
        recursive,
    )
    for item in items:
        if item.item_id:
            try:
                # Clear item caches asynchronously
                items_api.clear_cache(id=item.item_id, callback=lambda response: None)
            except Exception:
                if not ignore_errors:
                    raise


def _read_item_map(map_file):
    header, rows = _read_swap_map_csv(map_file)

    old_id_index = tables.find_column_index(header, "Old ID")
    new_id_index = tables.find_column_index(header, "New ID")

    if old_id_index is None:
        raise Exception('Column "Old ID" not found in datasource map file.')

    if new_id_index is None:
        raise Exception('Column "New ID" not found in datasource map file.')

    item_map = dict()
    item_map["header"] = header
    item_map["rows"] = rows
    item_map["old_id_index"] = old_id_index
    item_map["new_id_index"] = new_id_index

    # Weed out any non-GUIDs, like "NO MATCHES" and "MULTIPLE MATCHES"
    item_map["rows"] = [
        row
        for row in rows
        if _is_guid(row[item_map["old_id_index"]])
        and _is_guid(row[item_map["new_id_index"]])
    ]

    return item_map


def _is_guid(s):
    try:
        uuid.UUID(s)
        return True
    except Exception:
        return False


def swap(**kwargs):
    """
    Swap datasource items within the workbook for other items.
    :param map_csv: CSV file with item mapping, see datasource_map() function.
    :param workbook_id: Seeq ID of workbook to perform swap on.
    :param workbook_csv: Iterate over workbooks in CSV file produced by workbook_list().
    :param reverse: Swap in the reverse direction, new to old.
    :param include_globals: Include items referenced by globally-scoped calculations.
    :param include_dependencies: Include items references as parameters/dependencies.
    :param ignore_errors: Ignore errors encountered while accessing data.
    :param csv: Name of CSV file to route output to.
    :param append_csv: Append to the existing CSV file if possible.

    Original design doc:
    https://seeq.atlassian.net/wiki/spaces/SQ/pages/443646015/CRAB-13251+Ability+to+re-map+a+workbook+to+a+different+datasource

    Here is the sequence of actions this command executes:

    1 Iterate over all workbooks:
    1.1 Iterate over all worksheets:
    1.1.1 For Analysis workbooks:
    1.1.1.1 For the current workstep, scrape anything that looks like a GUID. Look it up in our map, and if it needs
            to be swapped for a new ID, replace it in the workstep.
    1.1.1.2 Look at the Journal text:
    1.1.1.2.1 Swap any direct "item" references as necessary.
    1.1.1.2.2 If there are workstep links, scrape & swap that workstep as we did in 1.1.1.1
    1.1.1.3 Search for calculated items that are scoped to the workbook. Swap any of their parameters as necessary.
            CalculatedScalar, CalculatedSignal, CalculatedCondition and Chart (histogram) all need special
            treatment.
    1.1.2 For Topic workbooks:
    1.1.2.1 Look for embedded content in the Topic document and scrape & swap the workstep as we did in 1.1.1.1.
    1.1.2.2 Remove the <img src> attributes for all embedded content found in 1.1.2.2.
    2 Optionally iterate over globally-scoped calculated items. Swap as necessary similar to 1.1.1.3.
    """
    item_map = _read_item_map(kwargs.get("map_csv"))

    kwargs["reverse"] = kwargs.get("reverse") or False
    kwargs["include_globals"] = kwargs.get("include_globals") or False
    kwargs["include_dependencies"] = kwargs.get("include_dependencies") or False
    kwargs["ignore_errors"] = kwargs.get("ignore_errors") or False

    logging.basicConfig(
        filename="workbook_swap.log",
        level=logging.INFO,
        format="%(asctime)s - %(message)s",
    )

    client = spy.client

    workbooks_api = sdk.WorkbooksApi(client)
    items_api = sdk.ItemsApi(client)

    column_specs = [
        ColumnSpec("count", "#", ">", 4),
        ColumnSpec("workbook_id", "Workbook ID", "<", 36),
        ColumnSpec("workbook_name", "Workbook Name", "<", 36),
        ColumnSpec("workbook_type", "Workbook Type", "<", 13, console=False),
        ColumnSpec("old_item_id", "Old Item ID", "<", 36, console=False),
        ColumnSpec("new_item_id", "New Item ID", "<", 36),
        ColumnSpec("used_in", "Used In", "<", 16),
    ]

    if not kwargs.get("workbook_id") and not kwargs.get("workbook_csv"):
        raise RuntimeError("You must specify either workbook_id or workbook_csv")

    workbook_ids = []
    if kwargs.get("workbook_id"):
        workbook_ids = [kwargs.get("workbook_id")]
    elif kwargs.get("workbook_csv"):
        header, rows = tables.read_table(kwargs.get("workbook_csv"))

        id_column = tables.find_column_index(header, "ID")

        if id_column is None:
            raise RuntimeError('"ID" column not found in workbook CSV file')

        workbook_ids = [row[id_column] for row in rows]

    table = tables.start_table(
        column_specs,
        csv_file_name=kwargs.get("csv"),
        append_csv=kwargs.get("append_csv"),
    )

    try:
        for workbook_id in workbook_ids:
            log("Processing workbook %s" % workbook_id)
            try:
                workbook = workbooks_api.get_workbook(
                    id=workbook_id
                )  # type: WorkbookOutputV1
                worksheets = workbooks_api.get_worksheets(
                    workbook_id=workbook.id,  # type: WorksheetOutputListV1
                    limit=helper.get_default_page_size(),
                )
            except ApiException as e:
                if e.status == 403:
                    tables.write_table_row(
                        table,
                        shared.Bunch(
                            workbook_id=workbook_id,
                            workbook_name="Access Denied",
                            workbook_type="",
                            used_in="",
                            old_item_id="",
                            new_item_id="",
                        ),
                    )

                    if not kwargs.get("ignore_errors"):
                        raise_admin_credentials_error()

                    continue

                if not kwargs.get("ignore_errors"):
                    raise

                tables.write_table_row(
                    table,
                    shared.Bunch(
                        workbook_id=workbook_id,
                        workbook_name="Error: %s" % e,
                        workbook_type="",
                        used_in="",
                        old_item_id="",
                        new_item_id="",
                    ),
                )
                continue

            for worksheet in worksheets.worksheets:  # type: WorksheetOutputV1
                log(
                    "Processing worksheet %s (%s)" % (worksheet.id, worksheet.name),
                    indentation_level=1,
                )
                # This swaps non-calculated items in the details pane of the current worksheet
                try:
                    if workbook.type == "Analysis":
                        _swap_analysis_worksheet(
                            client,
                            table,
                            item_map,
                            workbook,
                            workbook.type,
                            worksheet,
                            kwargs.get("reverse"),
                        )
                    else:
                        _swap_topic_document(
                            client, table, item_map, worksheet, kwargs.get("reverse")
                        )
                except ApiException as e:
                    if not kwargs.get("ignore_errors"):
                        raise

                    tables.write_table_row(
                        table,
                        shared.Bunch(
                            workbook_id=workbook_id,
                            workbook_name="Error: %s" % e,
                            workbook_type="",
                            used_in="",
                            old_item_id=worksheet.id,
                            new_item_id="",
                        ),
                    )
                    continue

                # This swaps global calculated items present in the details pane of the worksheet
                if kwargs.get("include_globals"):
                    # If we're supposed to include globals that are referenced by this workbook, first scrape all
                    # item IDs and feed them into _swap_calculated_item() with allow_globals=True
                    item_ids = _scrape_item_ids_from_workbook(
                        client, workbook, worksheet, kwargs.get("ignore_errors")
                    )  # type: dict
                    log("Swapping global calculated items", indentation_level=1)
                    for item_id in item_ids.keys():
                        try:
                            item = items_api.get_item_and_all_properties(
                                id=item_id
                            )  # type: ItemOutputV1
                            if item.scoped_to:
                                # Ignore non-global calculated items because they'll be processed later below
                                continue

                            _swap_calculated_item(
                                client,
                                table,
                                item_map,
                                workbook,
                                workbook.type,
                                item,
                                kwargs.get("reverse"),
                                allow_globals=True,
                                ignore_errors=kwargs.get("ignore_errors"),
                                include_dependencies=kwargs.get("include_dependencies"),
                                log_indentation_level=2,
                            )
                        except ApiException as e:
                            if not kwargs.get("ignore_errors"):
                                raise

                            tables.write_table_row(
                                table,
                                shared.Bunch(
                                    workbook_id=workbook_id,
                                    workbook_name="Error: %s" % e,
                                    workbook_type="",
                                    used_in="",
                                    old_item_id=item_id,
                                    new_item_id="",
                                ),
                            )
                            continue

            log(
                "Swapping calculated items for workbook %s" % workbook.id,
                indentation_level=1,
            )
            try:
                tables.iterate_over_output(
                    lambda offset: items_api.search_items(
                        filters=[
                            "",
                            SeeqNames.API.Flags.exclude_globally_scoped,
                            SeeqNames.API.Flags.include_unsearchable,
                        ],
                        scope=[workbook_id],
                        offset=offset,
                        limit=helper.get_default_page_size(),
                        types=[
                            "CalculatedScalar",
                            "CalculatedSignal",
                            "CalculatedCondition",
                            "Chart",
                            "ThresholdMetric",
                        ],
                    ),
                    "items",
                    lambda item: _swap_calculated_item(
                        client,
                        table,
                        item_map,
                        workbook,
                        workbook.type,
                        item,
                        kwargs.get("reverse"),
                        allow_globals=False,
                        ignore_errors=kwargs.get("ignore_errors"),
                        include_dependencies=kwargs.get("include_dependencies"),
                        log_indentation_level=2,
                    ),
                )
            except Exception as e:
                log(
                    'Could not retrieve data for workbook "%s", skipping. Exception:\n%s'
                    % (workbook_id, e),
                    indentation_level=0,
                )

    finally:
        tables.finish_table(table)


def _swap_topic_document(client, table, item_map, worksheet, reverse):
    log(
        "Swap topic document %s (%s)" % (worksheet.id, worksheet.name),
        indentation_level=2,
    )

    workbooks_api = sdk.WorkbooksApi(client)
    annotations_api = sdk.AnnotationsApi(client)
    content_api = sdk.ContentApi(client)

    annotations = annotations_api.get_annotations(
        annotates=[worksheet.id]
    )  # type: AnnotationListOutputV1

    for annotation in annotations.items:  # type: AnnotationOutputV1
        annotation = annotations_api.get_annotation(id=annotation.id)

        if not annotation.document:
            continue

        all_contents: List[ContentOutputV1] = [
            content_api.get_content(id=content_id)
            for content_id in annotation.content_ids
        ]

        for content in all_contents:
            embedded_workbook = workbooks_api.get_workbook(id=content.source_workbook)
            embedded_worksheet = workbooks_api.get_worksheet(
                workbook_id=content.source_workbook,
                worksheet_id=content.source_worksheet,
            )

            _swap_workstep(
                client,
                table,
                item_map,
                embedded_workbook,
                "Analysis",
                embedded_worksheet,
                content.source_workstep,
                reverse,
                used_in="Embedded Content",
                log_indentation_level=3,
            )

            content_api.clear_image_cache(id=content.id)


def _update_annotation_document(client, annotation, new_document):
    """
    :type annotation: AnnotationOutputV1
    """
    annotations_api = sdk.AnnotationsApi(client)

    new_annotation = AnnotationInputV1()
    new_annotation.discoverable = annotation.discoverable
    new_annotation.document = new_document
    new_annotation.name = annotation.name
    new_annotation.description = annotation.description
    new_annotation.interests = []
    for interest in annotation.interests:  # type: AnnotationInterestOutputV1
        interest_item = interest.item  # type: ItemPreviewV1
        # we've encountered a case where there were multiple interests returned with the same ID, which
        # caused Appserver to choke when updating the annotation. So filter those out.
        if any(interest_item.id == i.interest_id for i in new_annotation.interests):
            continue
        new_interest = AnnotationInterestInputV1()
        new_interest.interest_id = interest_item.id
        if interest.capsule is not None:
            new_interest.detail_id = interest.capsule.id
        new_annotation.interests.append(new_interest)
    new_annotation.type = annotation.type
    if annotation.type == "Report":
        new_annotation.report_input = OptionalReportInputV1()
    new_annotation.created_by_id = annotation.created_by.id

    annotations_api.update_annotation(id=annotation.id, body=new_annotation)


def _swap_analysis_worksheet(
    client, table, item_map, workbook, workbook_type, worksheet, reverse
):
    annotations_api = sdk.AnnotationsApi(client)

    log(
        "Swap analysis worksheet %s (%s)" % (worksheet.id, worksheet.name),
        indentation_level=2,
    )

    # Short-circuit if this worksheet does not have a workstep
    if not worksheet.workstep:
        return

    # Gets the current workstep id from a string that looks like:
    # "workbook/WORKBOOK_GUID/worksheet/WORKSHEET_GUID/workstep/WORKSTEP_GUID"
    current_workstep_id = worksheet.workstep.split("/")[-1]

    # Searches the .data property of the workstep using a regular expression and swaps GUIDs if they are present in
    # the mapping CSV (supplied as an argument)
    # Effectively performs a swap on non-calculated items in the details pane of the worksheet
    _swap_workstep(
        client,
        table,
        item_map,
        workbook,
        workbook_type,
        worksheet,
        current_workstep_id,
        reverse,
        log_indentation_level=3,
    )

    # Now it is time to swap annotations!
    annotations = annotations_api.get_annotations(
        annotates=[worksheet.id]
    )  # type: AnnotationListOutputV1

    log(
        "Swap annotations for analysis worksheet %s (%s)"
        % (worksheet.id, worksheet.name),
        indentation_level=2,
    )
    for annotation in annotations.items:  # type: AnnotationOutputV1
        annotation = annotations_api.get_annotation(
            id=annotation.id
        )  # type: AnnotationOutputV1
        log("Swap annotation %s " % annotation.id, indentation_level=3)

        if not annotation.document:
            continue

        annotation_modified = False
        new_document = annotation.document

        # Obtains an iterator over all GUIDs in the annotation
        matches = re.finditer(
            r"item&#61;(" + GUID_REGEX + r")", annotation.document, re.IGNORECASE
        )
        for match in matches:
            # If the GUID should be swapped, swap it.
            row_index = _lookup_old_id_in_map(match.group(1).upper(), item_map, reverse)

            if row_index is not None:
                old_id = item_map["rows"][row_index][
                    item_map["new_id_index"] if reverse else item_map["old_id_index"]
                ]
                new_id = item_map["rows"][row_index][
                    item_map["old_id_index"] if reverse else item_map["new_id_index"]
                ]

                new_document = new_document.replace(match.group(1), new_id)

                annotation_modified = True

                tables.write_table_row(
                    table,
                    shared.Bunch(
                        workbook_id=workbook.id,
                        workbook_name=workbook.name,
                        workbook_type=workbook_type,
                        used_in="Journal",
                        old_item_id=old_id,
                        new_item_id=new_id,
                    ),
                )

        if annotation_modified:
            log("Updating annotation document.", indentation_level=3)
            _update_annotation_document(client, annotation, new_document)
        else:
            log("... annotation not modified.", indentation_level=3)

        # If the annotation links to another workstep, perform the swap on that workstep too
        matches = re.finditer(
            r"workstep&#61;(" + GUID_REGEX + r")", annotation.document, re.IGNORECASE
        )
        for match in matches:
            workstep_id = match.group(1)
            _swap_workstep(
                client,
                table,
                item_map,
                workbook,
                workbook_type,
                worksheet,
                workstep_id,
                reverse,
                log_indentation_level=4,
            )


def _swap_workstep(
    client,
    table,
    item_map,
    workbook,
    workbook_type,
    worksheet,
    workstep_id,
    reverse,
    used_in="Details Pane",
    log_indentation_level=0,
):
    workbooks_api = sdk.WorkbooksApi(client)
    items_api = sdk.ItemsApi(client)

    # Get this workstep via API call
    try:
        workstep = workbooks_api.get_workstep(
            workbook_id=workbook.id, worksheet_id=worksheet.id, workstep_id=workstep_id
        )  # type: WorkstepOutputV1
    except ApiException as e:
        if e.status == 404:
            # Apparently it's possible for a workstep to be referenced and yet not exist
            return

        raise

    log(
        "[[ START: Swapping workstep %s" % workstep.id,
        indentation_level=log_indentation_level,
    )
    items_swapped = False  # Becomes true if at least one item is swapped
    new_data = workstep.data  # this will be modified with swapped GUIDs later

    # Iterate over all GUIDs present in the .data property of the workstep
    matches = re.finditer(GUID_REGEX, workstep.data, re.IGNORECASE)
    for match in matches:
        log(
            "* Found GUID %s in workstep data" % match.group(0).upper(),
            indentation_level=log_indentation_level + 1,
        )
        # row_index will be assigned a value if this GUID should be swapped
        row_index = _lookup_old_id_in_map(match.group(0).upper(), item_map, reverse)

        if row_index is None:
            log(
                "... no mapping found for GUID",
                indentation_level=log_indentation_level + 2,
            )
        else:
            log(
                "... mapping found for GUID. Row index in map: %d" % row_index,
                indentation_level=log_indentation_level + 2,
            )

        if row_index is not None:
            # Swap the GUIDs and store the result in new_data
            old_id = item_map["rows"][row_index][
                item_map["new_id_index"] if reverse else item_map["old_id_index"]
            ]
            new_id = item_map["rows"][row_index][
                item_map["old_id_index"] if reverse else item_map["new_id_index"]
            ]
            log(
                "Replacing %s -> %s" % (old_id, new_id),
                indentation_level=log_indentation_level + 1,
            )
            new_data = new_data.replace(match.group(0), new_id)

            items_swapped = True

            # Write the output to the console and result CSV file
            tables.write_table_row(
                table,
                shared.Bunch(
                    workbook_id=workbook.id,
                    workbook_name=workbook.name,
                    workbook_type=workbook_type,
                    used_in=used_in,
                    old_item_id=old_id,
                    new_item_id=new_id,
                ),
            )
    if items_swapped:
        # If the user is swapping assets from Area A to Area B, a situation may arise where the user already has 1 or
        # more Area B assets in the current worksheet. Swapping an Area A asset to Area B could result in a duplicate.
        # This removes these duplicates.
        new_data = _dedup_details_pane_items(new_data)

        # Upload the changes.
        property_input = PropertyInputV1()
        property_input.value = new_data
        log(
            "END ]] - Items were swapped. Setting Data property for workstep %s"
            % workstep.id,
            indentation_level=log_indentation_level,
        )
        items_api.set_property(
            id=workstep.id, property_name="Data", body=property_input
        )
    else:
        log(
            "END ]] - No items swapped for workstep %s" % workstep.id,
            indentation_level=log_indentation_level,
        )


def _dedup_details_pane_items(new_data):
    new_data_json = json.loads(new_data)
    if (
        "state" not in new_data_json
        or "stores" not in new_data_json["state"]
        or "sqTrendSeriesStore" not in new_data_json["state"]["stores"]
        or "items" not in new_data_json["state"]["stores"]["sqTrendSeriesStore"]
    ):
        return new_data

    items = new_data_json["state"]["stores"]["sqTrendSeriesStore"]["items"]
    new_items = []
    for item in items:
        if sum(i["id"] == item["id"] for i in new_items) == 0:
            new_items.append(item)

    new_data_json["state"]["stores"]["sqTrendSeriesStore"]["items"] = new_items

    return json.dumps(new_data_json)


def _swap_calculated_item(
    client,
    table,
    item_map,
    workbook,
    workbook_type,
    item,
    reverse,
    allow_globals,
    ignore_errors,
    include_dependencies,
    log_indentation_level,
):
    scalars_api = sdk.ScalarsApi(client)
    signals_api = sdk.SignalsApi(client)
    conditions_api = sdk.ConditionsApi(client)
    formulas_api = sdk.FormulasApi(client)
    metrics_api = sdk.MetricsApi(client)
    items_api = sdk.ItemsApi(client)

    log(
        "[[ START Swapping calculated item %s (%s, %s)"
        % (item.id, item.type, item.name),
        indentation_level=log_indentation_level,
    )

    items = [item]  # always include the current item
    # Add all of the dependencies to the items list
    if include_dependencies:
        item_ids = dict()
        item_ids = _scrape_item_ids_from_dependencies(client, item.id, item_ids)

        # add the corresponding items
        for item_id in item_ids.keys():
            try:
                items.append(items_api.get_item_and_all_properties(id=item_id))
            except ApiException as e:
                if not ignore_errors:
                    raise
                log(
                    'Could not retrieve data for calculated item "%s", skipping. Exception:\n%s'
                    % (item.id, e),
                    indentation_level=log_indentation_level,
                )

    # Iterate over the current item and all of its dependencies
    for item in items:
        try:
            # Determine the type of item we are considering
            if item.type == "CalculatedScalar":
                item = scalars_api.get_scalar(
                    id=item.id
                )  # type: CalculatedItemOutputV1
            elif item.type == "CalculatedSignal":
                item = signals_api.get_signal(id=item.id)  # type: SignalOutputV1
            elif item.type == "CalculatedCondition":
                item = conditions_api.get_condition(
                    id=item.id
                )  # type: ConditionOutputV1
            elif item.type == "Chart":
                item = formulas_api.get_function(
                    id=item.id
                )  # type: CalculatedItemOutputV1
            elif item.type == "ThresholdMetric":
                item = metrics_api.get_metric(
                    id=item.id
                )  # type: ThresholdMetricOutputV1
            else:
                continue

            log(
                "Swapping parameters for item %s (%s, %s)"
                % (item.id, item.type, item.name),
                indentation_level=log_indentation_level + 1,
            )
            if item.scoped_to or allow_globals:
                if item.type == "ThresholdMetric":
                    _swap_threshold_metric_parameters(
                        client,
                        table,
                        item_map,
                        workbook,
                        workbook_type,
                        item,
                        reverse,
                        log_indentation_level=log_indentation_level + 2,
                    )
                else:
                    _swap_calculated_item_parameters(
                        client,
                        table,
                        item_map,
                        workbook,
                        workbook_type,
                        item,
                        reverse,
                        log_indentation_level=log_indentation_level + 2,
                    )
        except ApiException as e:
            if not ignore_errors:
                raise

            log(
                'Could not swap calculated item "%s", skipping. Exception:\n%s'
                % (item.id, e),
                indentation_level=log_indentation_level + 1,
            )
    log(
        "END ]] - Swapping finished for calculated item %s (%s, %s)"
        % (item.id, item.type, item.name),
        indentation_level=log_indentation_level,
    )


def _swap_threshold_metric_parameters(
    client,
    table,
    item_map,
    workbook,
    workbook_type,
    old_item,
    reverse,
    log_indentation_level,
):
    """
    :type old_item:  ThresholdMetricOutputV1
    """
    metrics_api = sdk.MetricsApi(client)

    metric_modified = False

    log(
        "Swapping threshold metric parameters for %s (%s)"
        % (old_item.id, old_item.name),
        indentation_level=log_indentation_level,
    )

    new_item = ThresholdMetricInputV1()
    new_item.name = old_item.name
    if old_item.duration:
        new_item.duration = str(old_item.duration.value) + old_item.duration.uom
    if old_item.period:
        new_item.period = str(old_item.period.value) + old_item.period.uom
    new_item.scoped_to = old_item.scoped_to
    new_item.aggregation_function = old_item.aggregation_function

    if old_item.measured_item:
        row_index = _lookup_old_id_in_map(old_item.measured_item.id, item_map, reverse)
        if row_index is not None:
            new_item.measured_item = item_map["rows"][row_index][
                item_map["old_id_index"] if reverse else item_map["new_id_index"]
            ]

            metric_modified = True
            tables.write_table_row(
                table,
                shared.Bunch(
                    workbook_id=workbook.id,
                    workbook_name=workbook.name,
                    workbook_type=workbook_type,
                    used_in="Metric Measured Item",
                    old_item_id=old_item.measured_item.id,
                    new_item_id=new_item.measured_item,
                ),
            )
        else:
            new_item.measured_item = old_item.measured_item.id

    if old_item.bounding_condition:
        row_index = _lookup_old_id_in_map(
            old_item.bounding_condition.id, item_map, reverse
        )
        if row_index is not None:
            new_item.bounding_condition = item_map["rows"][row_index][
                item_map["old_id_index"] if reverse else item_map["new_id_index"]
            ]

            metric_modified = True
            tables.write_table_row(
                table,
                shared.Bunch(
                    workbook_id=workbook.id,
                    workbook_name=workbook.name,
                    workbook_type=workbook_type,
                    used_in="Metric Bounding Condition",
                    old_item_id=old_item.bounding_condition.id,
                    new_item_id=new_item.bounding_condition,
                ),
            )
        else:
            new_item.bounding_condition = old_item.bounding_condition.id

    new_item.thresholds = []  # type: str
    if old_item.thresholds:
        for threshold in old_item.thresholds:  # type: ThresholdOutputV1
            priority = threshold.priority  # type: PriorityV1
            if threshold.value:
                if isinstance(threshold.value, ScalarValueOutputV1):
                    scalar_value = threshold.value  # type: ScalarValueOutputV1
                    val = "%s%s" % (
                        scalar_value.value,
                        scalar_value.uom if scalar_value.uom else "",
                    )
                    new_item.thresholds.append("%s=%s" % (priority.level, val))
                else:
                    new_item.thresholds.append(
                        "%s=%s" % (priority.level, threshold.value)
                    )
            else:
                new_threshold_id = threshold.item.id
                row_index = _lookup_old_id_in_map(threshold.item.id, item_map, reverse)

                if row_index is not None:
                    new_threshold_id = item_map["rows"][row_index][
                        (
                            item_map["old_id_index"]
                            if reverse
                            else item_map["new_id_index"]
                        )
                    ]

                    metric_modified = True
                    tables.write_table_row(
                        table,
                        shared.Bunch(
                            workbook_id=workbook.id,
                            workbook_name=workbook.name,
                            workbook_type=workbook_type,
                            used_in="Metric Threshold",
                            old_item_id=threshold.item.id,
                            new_item_id=new_threshold_id,
                        ),
                    )

                new_item.thresholds.append("%s=%s" % (priority.level, new_threshold_id))

    if metric_modified:
        log("... metric modified", indentation_level=log_indentation_level + 1)
        metrics_api.put_threshold_metric(id=old_item.id, body=new_item)
    else:
        log("... metric not modified", indentation_level=log_indentation_level + 1)


def _swap_calculated_item_parameters(
    client,
    table,
    item_map,
    workbook,
    workbook_type,
    old_item,
    reverse,
    log_indentation_level,
):
    items_api = sdk.ItemsApi(client)
    formulas_api = sdk.FormulasApi(client)

    parameters_swapped = 0

    # For Signals, Conditions and Scalars
    formula_input = FormulaUpdateInputV1()
    formula_input.formula = old_item.formula
    formula_input.parameters = []

    # For Histograms
    function_input = FunctionInputV1()
    function_input.name = old_item.name
    function_input.formula = old_item.formula
    function_input.parameters = []

    for old_parameter in old_item.parameters:  # type: FormulaParameterOutputV1
        row_index = None
        if old_parameter.item:
            row_index = _lookup_old_id_in_map(old_parameter.item.id, item_map, reverse)

        formula_parameter_input = FormulaParameterInputV1()
        formula_parameter_input.name = old_parameter.name
        formula_parameter_input.formula = old_parameter.formula
        formula_parameter_input.unbound = old_parameter.unbound

        if row_index is not None:
            log(
                "Mapping found for %s. Row index is %d"
                % (old_parameter.item.id, row_index),
                indentation_level=log_indentation_level + 1,
            )
            old_id = item_map["rows"][row_index][
                item_map["new_id_index"] if reverse else item_map["old_id_index"]
            ]
            new_id = item_map["rows"][row_index][
                item_map["old_id_index"] if reverse else item_map["new_id_index"]
            ]
            log(
                "Swapping parameter %s -> %s" % (old_id, new_id),
                indentation_level=log_indentation_level + 1,
            )
            if old_item.type == "Chart":
                formula_parameter_input.id = new_id
                function_input.parameters.append(formula_parameter_input)
            else:
                formula_input.parameters.append(old_parameter.name + "=" + new_id)

            tables.write_table_row(
                table,
                shared.Bunch(
                    workbook_id=workbook.id,
                    workbook_name=workbook.name,
                    workbook_type=workbook_type,
                    used_in="Calculation",
                    old_item_id=old_id,
                    new_item_id=new_id,
                ),
            )

            parameters_swapped += 1

        else:
            if old_parameter.item:
                log(
                    "... mapping NOT found for %s." % old_parameter.item.id,
                    indentation_level=log_indentation_level + 1,
                )
            if old_item.type == "Chart":
                formula_parameter_input.id = (
                    old_parameter.item.id if old_parameter.item else None
                )
                function_input.parameters.append(formula_parameter_input)
            else:
                formula_input.parameters.append(
                    old_parameter.name + "=" + old_parameter.item.id
                )

    if parameters_swapped > 0:
        log(
            "%d parameters swapped. Setting the new formula" % parameters_swapped,
            indentation_level=log_indentation_level,
        )
        try:
            if old_item.type == "Chart":
                formulas_api.update_function(id=old_item.id, body=function_input)
            else:
                items_api.set_formula(id=old_item.id, body=formula_input)
        except ApiException as e:
            log(
                'Could not swap calculated item "%s", skipping. Exception:\n%s'
                % (old_item.id, e),
                indentation_level=log_indentation_level,
            )


def _lookup_old_id_in_map(old_id, item_map, reverse):
    old_id_index = item_map["new_id_index"] if reverse else item_map["old_id_index"]
    new_id_index = item_map["old_id_index"] if reverse else item_map["new_id_index"]
    for i in range(0, len(item_map["rows"])):
        if item_map["rows"][i][old_id_index] == old_id:
            # If it's not a guid, then it's not mapped or there were multiple matches
            if re.match(GUID_REGEX, item_map["rows"][i][new_id_index]):
                return i

    return None


def _read_swap_map_csv(map_filename):
    header = None
    rows = []
    with util.safe_open(map_filename, "r", encoding="utf-8-sig") as csv_file:
        csv_reader = csv.reader(csv_file)
        for row in csv_reader:
            if not header:
                header = row
            else:
                rows.append(row)

    return header, rows


def log(msg, indentation_level=0):
    if indentation_level > 0:
        msg = ".." * indentation_level + msg

    logging.info(msg)
