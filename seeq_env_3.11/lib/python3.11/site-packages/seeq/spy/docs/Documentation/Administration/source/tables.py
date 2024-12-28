from __future__ import annotations

"""
This module facilitates reading and writing tables of data to the console and to CSV files.
"""

import csv
import os
import time

from seeq.base import util


def start_table(
    column_specs, csv_file_name=None, append_csv=False, console=True, encoding="utf-8"
):
    table = dict()

    table["column_specs"] = column_specs
    table["csv_file_name"] = csv_file_name
    table["console"] = console

    table["csv_file"] = None
    count = 1
    try:
        table["csv_writer"] = None
        file_exists = False
        if csv_file_name:
            file_exists = util.safe_exists(csv_file_name)
            if file_exists and append_csv:
                header, rows = read_table(csv_file_name)
                count = int(rows[-1][0]) + 1

            open_command = "a+" if append_csv else "w+"
            table["csv_file"] = util.safe_open(
                csv_file_name, open_command, newline="", encoding=encoding
            )
            table["csv_writer"] = csv.writer(table["csv_file"])

        if table["console"]:
            _print_console_header(column_specs)
            print_console_underline(column_specs)

        if table["csv_writer"] and (not file_exists or not append_csv):
            _print_csv_header(table["csv_writer"], column_specs)

    except Exception:
        if table["csv_file"]:
            table["csv_file"].close()

        raise

    table["extra_values"] = {"count": count, "start_time_sec": time.time()}

    return table


def write_table_row(table, item):
    if table["console"]:
        _print_console_row(table["column_specs"], item, table["extra_values"])

    if table["csv_writer"]:
        _print_csv_row(
            table["csv_writer"], table["column_specs"], item, table["extra_values"]
        )

    table["extra_values"]["count"] = table["extra_values"]["count"] + 1


def finish_table(table):
    if table["csv_file"]:
        table["csv_file"].close()

        if table["console"]:
            print('\nOutput also written to "%s"' % table["csv_file_name"])

            hidden_columns = [
                cs.header for cs in table["column_specs"] if not cs.console
            ]
            if len(hidden_columns) > 0:
                print("with more columns: " + ", ".join(hidden_columns))


def read_table(csv_file_name):
    header = None
    rows = []

    with util.safe_open(csv_file_name, "r", encoding="utf-8-sig") as csv_file:
        # Even though (by default) we don't write tables with 'utf-8-sig' encoding, reading with 'utf-8-sig' allows to
        # handle both 'utf-8' and 'utf-8-sig'. (utf-8-sig includes a byte-order-marker at the beginning of the file.)
        csv_reader = csv.reader(csv_file)
        for row in csv_reader:
            if not header:
                header = row
            else:
                rows.append(row)

    return header, rows


def get_table_rows(table):
    header, rows = table
    return rows


def get_table_column(table, row, column_name):
    header, rows = table
    column_index = find_column_index(header, column_name)
    if column_index is None:
        raise Exception('Column "%s" not found in table' % column_name)

    return row[column_index]


def find_column_index(header, column_name):
    for i in range(0, len(header)):
        if header[i].strip() == column_name:
            return i

    return None


def print_table_from_sdk_output(
    func,
    collection_name,
    column_specs,
    action_func=None,
    csv_file_name=None,
    append_csv=False,
):
    """
    This helper method prints a nicely formatted table to the console and (optionally) outputs values to a CSV file
    by executing a Seeq SDK function that follows typical pagination with offset/limit parameters and outputs. It
    paginates through the data by executing the supplied 'func' function with a new offset.

    :param func: A function that takes a single parameter 'offset' which specifies the current offset in pagination
    :param collection_name: Paginated output always has a top-level collection field, specify it here
    :param column_specs: An array of ColumnSpec objects that define each column in the output.
    :param action_func: An optional "action function" to execute on every item in the collection
    :param csv_file_name: Optional path for a CSV file to write the output to
    :param append_csv: True to append to existing CSV file
    """
    table = None
    try:
        table = start_table(column_specs, csv_file_name, append_csv)

        iterate_over_output(
            func,
            collection_name,
            lambda item: _print_table_from_sdk_output_action(table, item, action_func),
        )

    finally:
        if table:
            finish_table(table)


class ItemSkippedException(Exception):
    def __init__(self):
        pass


def iterate_over_output(output_func, collection_name, action_func):
    """
    Executes the output_func and then iterates over the items in collection_name, making additional paginated calls
    as necessary to get through all the items.
    :param output_func: A function that takes a single parameter 'offset' which specifies the current offset in
    pagination
    :param collection_name: Paginated output always has a top-level collection field, specify it here
    :param action_func: An optional "action function" to execute on every item in the collection
    """
    offset = 0
    while True:
        try:
            output = output_func(offset)

            collection = getattr(output, collection_name)

            for item in collection:
                try:
                    action_func(item)
                except ItemSkippedException:
                    pass

            if len(collection) != output.limit:
                break

            offset += output.limit
        except Exception as e:
            raise e


class ColumnSpec:
    """
    This class contains a specification of a column in the output for print_table_from_sdk_output(). See constructor
    docs for info on what each field means.
    """

    field = ""  # type: str
    header = ""  # type: str
    align = ""  # type: str
    width = 0  # type: int
    transform = None
    console = True

    def __init__(
        self, field, header, align="<", width=16, transform=None, console=True
    ):
        """
        :param field: The field name within the returned item that will populate this column
        :type field: str
        :param header: The column header to be output in the console and in the CSV header row
        :type header: str
        :param align: Alignment for the column. '<' is left-align, '>' is right-align
        :type align: str
        :param width: The width of the column for the console output.
        :type width: int
        :param transform: A function to call to transform the field value into something else before printing.
        :type transform: function
        """
        self.field = field
        self.header = header
        self.align = align
        self.width = width
        self.transform = transform
        self.console = console


def _print_time_string(seconds):
    return "{:02}:{:04.1f}".format(int(seconds / 60 % 60), seconds % 60)


def _print_table_from_sdk_output_action(table, item, action_func=None):
    if action_func:
        start = time.time()
        action_return = action_func(item)
        table["extra_values"]["action_time_sec"] = time.time() - start
        table["extra_values"]["action_time_str"] = _print_time_string(
            table["extra_values"]["action_time_sec"]
        )
        if action_return:
            for pairs in action_return:
                extra_key, extra_value = pairs
                table["extra_values"][extra_key] = extra_value

    table["extra_values"]["total_time_sec"] = (
        time.time() - table["extra_values"]["start_time_sec"]
    )
    table["extra_values"]["total_time_str"] = _print_time_string(
        table["extra_values"]["total_time_sec"]
    )

    write_table_row(table, item)


def _print_console_header(column_specs):
    header = ""
    for column_spec in column_specs:
        if column_spec.console:
            header += ("{:%s%d} " % (column_spec.align, column_spec.width)).format(
                column_spec.header
            )

    print(header)


def print_console_underline(column_specs):
    underline = ""
    for column_spec in column_specs:
        if column_spec.console:
            underline += "-" * column_spec.width + " "

    print(underline)


def _print_csv_header(csv_writer, column_specs):
    csv_writer.writerow([column_spec.header for column_spec in column_specs])


def _transform_if_necessary(column_spec, value, item):
    if column_spec.transform:
        transform = column_spec.transform
        value = transform(value, item)

    if not isinstance(value, str) and not isinstance(value, str):
        value = str(value)

    return value


def _print_console_row(column_specs, item, extra_values=None):
    row = ""

    for column_spec in column_specs:
        if not column_spec.console:
            continue

        value = ""
        if column_spec.field in extra_values:
            value = str(
                extra_values[column_spec.field]
                if extra_values[column_spec.field] is not None
                else ""
            )
        elif column_spec.field:
            value = _get_field_from_obj_or_dict(item, column_spec.field)
            if value is None:
                value = ""

        value = _transform_if_necessary(column_spec, value, item)

        value = value.replace("\n", ", ")

        if len(value) > column_spec.width:
            if column_spec.align == "<":
                value = value[0: column_spec.width - 3] + "..."
            else:
                value = "..." + value[len(value) - column_spec.width + 3:]

        row += ("{:%s%d} " % (column_spec.align, column_spec.width)).format(value)

    # commented out to hide output on data lab
    print(row)


def _get_field_from_obj_or_dict(item, field):
    try:
        value = item[field]
    except TypeError:
        value = getattr(item, field)

    return value


def _print_csv_row(csv_writer, column_specs, item, extra_values):
    columns = []

    for column_spec in column_specs:

        value = ""
        if column_spec.field in extra_values:
            value = str(
                extra_values[column_spec.field]
                if extra_values[column_spec.field] is not None
                else ""
            )
        elif column_spec.field:
            value = _get_field_from_obj_or_dict(item, column_spec.field)
            if value is None:
                value = ""

        value = _transform_if_necessary(column_spec, value, item)

        if not value:
            value = ""

        columns.append(value)

    csv_writer.writerow([s for s in columns])
