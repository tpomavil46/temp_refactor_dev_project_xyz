from __future__ import annotations

import datetime
import os
import re
from typing import Optional, Union

import numpy as np
import pandas as pd

from seeq import spy
from seeq.spy import _common
from seeq.spy import _push
from seeq.spy._context import WorkbookContext
from seeq.spy._errors import *
from seeq.spy._login import validate_start_and_end
from seeq.spy._session import Session
from seeq.spy._status import Status
from seeq.spy.assets._trees import _csv, _constants, _match, _path, _properties, _pull, _utils, _validate
from seeq.spy.assets._trees._pandas import KeyedDataFrame


class Tree:
    """
    Manages an asset tree as a collection of item definitions in the form of
    a metadata DataFrame. Allows users to manipulate the tree using various functions.

    Parameters
    ----------
    data : {pandas.DataFrame, str}
        Defines which element will be inserted at the root.
        If an existing tree already exists in Seeq, the entire tree will be pulled recursively.
        If this tree doesn't already within the scope of the workbook, new tree elements
        will be created (by deep-copy or reference if applicable).

        The following options are allowed:

        1) A name string. If an existing tree with that name (case-insensitive) is found,
           all children will be recursively pulled in.
        2) An ID string of an existing item in Seeq. If that item is in a tree, all
           children will be recursively pulled in.
        3) spy.search results or other custom dataframes. The 'Path' column must be present
           and represent a single tree structure.
        4) A filename or relative file path to a CSV file. The CSV file should have either
           a complete Name column or a complete ID column, and should specify the tree
           path for each item either in a 'Path' column formatted as "Root >> Next Level":

           +--------------------+-----------+
           | Path               | Name      |
           +--------------------+-----------+
           | Root >> Next Level | Item_Name |
           +--------------------+-----------+

           or as a series of 'Levels' columns, e.g. "Level 1" and "Level 2" columns,
           where "Level 1" would be "Root" and "Level 2" would be "Next Level":

           +---------+------------+-----------+
           | Level 1 | Level 2    | Name      |
           +---------+------------+-----------+
           | Root    | Next Level | Item_Name |
           +---------+------------+-----------+

          The 'Level' columns will be forward-filled.

    friendly_name : str, optional
        Use this specified name rather than the referenced item's original name.

    description : str, optional
        The description to set on the root-level asset.

    workbook : str, default 'Data Lab >> Data Lab Analysis'
        The path to a workbook (in the form of 'Folder >> Path >> Workbook Name')
        or an ID that all pushed items will be 'scoped to'. You can
        push to the Corporate folder by using the following pattern:
        '__Corporate__ >> Folder >> Path >> Workbook Name'. Scoped items will not
        be visible/searchable using the data panel in other workbooks.
        A Tree can be globally scoped (and therefore visible across all
        workbooks) by specifying workbook=None.

    datasource : str, optional, default 'Seeq Data Lab'
        The name of the datasource within which to contain all the pushed items.
        Items inherit access control permissions from their datasource unless it
        has been overridden at a lower level. If you specify a datasource using
        this argument, you can later manage access control (using spy.acl functions)
        at the datasource level for all the items you have pushed.

        If you instead want access control for your items to be inherited from the
        workbook they are scoped to, specify `spy.INHERIT_FROM_WORKBOOK`.

    convert_displays_to_sdl : bool, default True
        If True, then if displays from a non-Seeq Data Lab datasource are found in
        a tree that is otherwise from a Seeq Data Lab datasource, then they will
        be replaced by displays from a Seeq Data Lab datasource when this tree
        is pushed.

    quiet : bool, default False
        If True, suppresses progress output. This setting will be the default for all
        operations on this Tree. This option can be changed later using
        `tree.quiet = True` or by specifying the option for individual function calls.
        Note that when status is provided, the quiet setting of the Status object
        that is passed in takes precedence.

    errors : {'raise', 'catalog'}, default 'raise'
        If 'raise', any errors encountered will cause an exception. If 'catalog',
        errors will be added to a 'Result' column in the status.df DataFrame. The
        option chosen here will be the default for all other operations on this Tree.
        This option can be changed later using `tree.errors = 'catalog'` or by
        specifying the option for individual function calls.

    status : spy.Status, optional
        If specified, the supplied Status object will be updated as the command
        progresses. It gets filled in with the same information you would see
        in Jupyter in the blue/green/red table below your code while the
        command is executed. The table itself is accessible as a DataFrame via
        the status.df property.

    session : spy.Session, optional
        If supplied, the Session object (and its Options) will be used to
        store the login session state. This is useful to log in to different
        Seeq servers at the same time or with different credentials.
    """

    _dataframe = KeyedDataFrame()
    _workbook = _common.DEFAULT_WORKBOOK_PATH
    _workbook_id = _constants.UNKNOWN  # Placeholder for the ID of a workbook that hasn't been pushed yet

    session: Session
    status: Status
    is_dirty = False

    @Status.handle_keyboard_interrupt()
    def __init__(self, data, *, friendly_name=None, description=None, workbook=_common.DEFAULT_WORKBOOK_PATH,
                 datasource=None, convert_displays_to_sdl=True, quiet=None, errors=None, status=None,
                 session: Optional[Session] = None):
        _common.validate_argument_types([
            (data, 'data', (pd.DataFrame, str)),
            (friendly_name, 'friendly_name', str),
            (description, 'description', str),
            (workbook, 'workbook', str),
            (datasource, 'datasource', str),
            (convert_displays_to_sdl, 'convert_displays_to_sdl', bool),
            (errors, 'errors', str),
            (quiet, 'quiet', bool),
            (status, 'status', Status),
            (session, 'session', Session)
        ])
        self.session = Session.validate(session)
        self.status = Status.validate(status, self.session, quiet, errors)

        self._workbook = workbook
        self._datasource = datasource if datasource else _common.DEFAULT_DATASOURCE_ID
        self._display_template_map = dict()
        self._display_ids_to_archive = list()
        if workbook is not None:
            self._find_workbook_id(self.status.create_inner('Find Workbook'))
        else:
            self._workbook_id = _common.EMPTY_GUID

        # check if csv file
        if isinstance(data, str):
            ext = os.path.splitext(data)[1]
            if ext == '.csv':
                # read and process csv file
                data = _csv.process_csv_data(data, self.status, workbook=self._workbook_id)
                # get a path column from levels columns
                _csv.make_paths_from_levels(data)
                # data is now a pd.DataFrame and can be handled as any df

        # If friendly_name is a column value query, we will apply it to the dataframe.
        # Otherwise, we will rename the root.
        rename_root = friendly_name is not None and not (isinstance(data, pd.DataFrame) and (
                _match.is_column_value_query(friendly_name) or len(data) == 1))

        if isinstance(data, pd.DataFrame):
            if len(data) == 0:
                raise SPyValueError("A tree may not be created from a DataFrame with no rows")

            _utils.initialize_status_df(self.status, 'Created', 'Constructing Tree object from dataframe input.',
                                        Status.RUNNING)

            # Check user input for errors, filter if errors='catalog'
            df: KeyedDataFrame = _validate.validate_and_filter(self.session, data, self.status, stage='input',
                                                               temporal_description='in input',
                                                               raise_if_all_filtered=True)

            df = df.reset_index(drop=True)

            # If the dataframe specifies a root with ID and Name corresponding to a previously pushed SPy tree,
            # then we want this object to modify the same tree rather than create a copy of it. If such a tree
            # exists, then we store its current state in existing_tree_df
            existing_tree_df = _pull.get_existing_spy_tree(self.session, df, self._workbook_id, self._datasource)
            if existing_tree_df is not None and convert_displays_to_sdl:
                self._display_ids_to_archive = list(existing_tree_df.loc[(existing_tree_df.Type == 'Display') & (
                        existing_tree_df['Datasource Name'] != self._datasource), 'ID'])

            if friendly_name is not None:
                if _match.is_column_value_query(friendly_name):
                    df['Friendly Name'] = friendly_name
                elif len(df) == 1:
                    df['Name'] = friendly_name
            _properties.apply_friendly_name(df)
            modified_items = df.spy.modified_items

            # Sanitize data and pull in properties of items with IDs. Make items with IDs into references unless
            # they are contained in existing_tree_df
            df = _properties.process_properties(self.session, df, self._display_template_map, self._datasource,
                                                self.status, existing_tree_df=existing_tree_df)
            modified_items.update(df.spy.modified_items)

            # Rectify paths
            df = _path.trim_unneeded_paths(df)
            df = _path.reify_missing_assets(df)

            # Pull children of items with IDs
            df = _pull.pull_all_children_of_all_nodes(self.session, df, self._workbook_id,
                                                      self._display_template_map, self._datasource,
                                                      self.status, existing_tree_df=existing_tree_df,
                                                      item_ids_to_ignore=modified_items)
            _utils.increment_status_df(self.status, new_items=df)

            status_message = f"Tree successfully created from DataFrame."
            if existing_tree_df is None:
                self.is_dirty = True
            else:
                status_message += f' This tree modifies a pre-existing SPy-created tree with name ' \
                                  f'"{existing_tree_df.ID.iloc[0]}".'

        elif data and isinstance(data, str):
            if _common.is_guid(data):
                existing_node_id = data
            else:
                self.status.update(f'Searching for existing asset tree roots with name "{data}"', Status.RUNNING)
                existing_node_id = _pull.find_root_node_by_name(self.session, data, self._workbook_id, self.status)

            if existing_node_id:
                _utils.initialize_status_df(self.status, 'Created', f'Pulling existing asset tree "{data}".',
                                            Status.RUNNING)

                # Pull an existing tree. Detect whether it originated from SPy
                df = _pull.pull_tree(self.session, existing_node_id, self._workbook_id, self._display_template_map,
                                     self._datasource, self.status)
                if convert_displays_to_sdl:
                    self._display_ids_to_archive = df.spy.display_ids_to_archive
                _utils.increment_status_df(self.status, new_items=df)

                self.is_dirty = not df.spy.spy_tree
                if df.spy.spy_tree and self._workbook_id == _constants.UNKNOWN and hasattr(df.spy, 'workbook_id'):
                    self.workbook = df.spy.workbook_id

                status_message = f"Recursively pulled {'SPy-created' if df.spy.spy_tree else 'existing'} " \
                                 f"asset tree."
            else:
                _utils.initialize_status_df(self.status, 'Created', f'Creating asset tree with new root "{data}".',
                                            Status.RUNNING)

                # Define a brand new root asset
                df = KeyedDataFrame([{
                    'Type': 'Asset',
                    'Path': '',
                    'Depth': 1,
                    'Name': data,
                    'Description': description if description else np.nan
                }], columns=_constants.dataframe_columns)
                _utils.increment_status_df(self.status, new_items=df)

                self.is_dirty = True
                status_message = f'No existing root found. Tree created using new root "{data}".' \
                                 f'{"" if self.session.client else " If an existing tree was expected, please log in."}'

        else:
            raise SPyTypeError("Input 'data' must be a name, name of a csv file, Seeq ID, or Metadata dataframe when "
                               "creating a Tree")

        _path.sort_by_node_path(df)
        if description:
            df.loc[0, 'Description'] = description
        if rename_root:
            df = _utils.set_name(df, friendly_name)

        df = _validate.validate_and_filter(self.session, df, self.status, stage='final',
                                           temporal_description='while creating tree',
                                           subtract_errors_from_status=True)

        self._dataframe = df
        self.status.update(f'{status_message} {self.summarize(ret=True)}', Status.SUCCESS)

    # @Status.handle_keyboard_interrupt()
    def insert(self,
               children: Optional[Union[pd.DataFrame, pd.Series, dict, str, list[str], Tree]] = None,
               parent: Optional[Union[pd.DataFrame, str, list[str]]] = None,
               *,
               name: Optional[str] = None,
               formula: Optional[str] = None,
               formula_parameters: Optional[Union[dict, list, str]] = None,
               roll_up_statistic: Optional[str] = None,
               roll_up_parameters: Optional[str] = None,
               friendly_name: Optional[str] = None,
               errors: Optional[str] = None,
               quiet: Optional[bool] = None,
               status: Optional[Status] = None):
        """
        Insert the specified elements into the tree.

        Parameters
        ----------
        children : {pandas.DataFrame, pandas.Series, dict, str, list, Tree}, optional
            Defines which element or elements will be inserted below each parent. If an existing
            node already existed at the level in the tree with that name (case-insensitive),
            it will be updated. If it doesn't already exist, a new node will be created
            (by deep-copy or reference if applicable).

            The following options are allowed:

            1) A basic string or list of strings to create a new asset.
            2) Another SPy Tree.
            3) spy.search results or other custom dataframes.

        parent : {pandas.DataFrame, str, int, list}, optional
            Defines which element or elements the children will be inserted below.

            The following options are allowed:

            1) No parent specified will insert directly to the root of the tree.
            2) String name match (case-insensitive equality, globbing, regex, column
               values) will find any existing nodes in the tree that match.
            3) String path match, including partial path matches.
            4) ID. This can either be the actual ID of the tree.push()ed node or the
               ID of the source item.
            5) Number specifying tree level. This will add the children below every
               node at the specified level in the tree (1 being the root node).
            6) spy.search results or other custom dataframe.

        name: str, optional
            An alternative to the `children` parameter for specifying a single name
            for an asset or calculation to be inserted.

        formula : str, optional
            The formula for a calculated item. The `formula` and `formula_parameters` are
            used in place of the `children` argument.

        formula_parameters : dict, optional
            The parameters used in the specified `formula`. The following options are allowed:

            1) The name of an item that is an asset sibling of this calculation.
            2) The relative path to an item in this Tree (E.G. '.. >> Area E >> Temperature').
            3) The absolute path to an item in this Tree (E.G. 'Example >> Cooling Tower 2 >> Area E >> Temperature').
            4) The ID of an item (including ones not in the Tree).

        roll_up_statistic : str, optional
            The statistic to use when inserting a roll-up calculation. Valid options are
            Average, Maximum, Minimum, Range, Sum, Multiply, Union, Intersect, Counts,
            Count Overlaps, Combine With.

        roll_up_parameters : str, optional
            A wildcard or regex string that matches all of the parameters for a roll-up
            calculation. The roll-up calculation will apply the function specified by
            `roll_up_statistic` to all parameters that match this string. For example,
            `roll_up_statistic='Average', roll_up_parameters='Area ? >> Temperature'`
            will calculate the average of all signals with path 'Area ? >> Temperature'
            relative to the location of the roll-up in the tree.

        friendly_name : str, optional
            Use this specified name rather than the referenced item's original name.

        errors : {'raise', 'catalog'}, optional
            If 'raise', any errors encountered will cause an exception. If 'catalog',
            errors will be added to a 'Result' column in the status.df DataFrame. This
            input will be used only for the duration of this function; it will default
            to the setting on the Tree if not specified.

        quiet : bool, optional
            If True, suppresses progress output. This input will be used only for the
            duration of this function; it will default to the setting on the Tree if
            not specified. Note that when status is provided, the quiet setting of
            the Status object that is passed in takes precedence.

        status : spy.Status, optional
            If specified, the supplied Status object will be updated as the command
            progresses. It gets filled in with the same information you would see
            in Jupyter in the blue/green/red table below your code while the
            command is executed. The table itself is accessible as a DataFrame via
            the status.df property.
        """

        _common.validate_argument_types([
            (children, 'children', (pd.DataFrame, pd.Series, dict, Tree, str, list)),
            (parent, 'parent', (pd.DataFrame, list, str, int)),
            (name, 'name', str),
            (friendly_name, 'friendly_name', str),
            (formula, 'formula', str),
            (formula_parameters, 'formula_parameters', (dict, list, str)),
            (roll_up_statistic, 'roll_up_statistic', str),
            (roll_up_parameters, 'roll_up_parameters', str),
            (errors, 'errors', str),
            (quiet, 'quiet', bool),
            (status, 'status', Status)
        ])

        status = Status.validate(status, self.session, quiet, errors)

        if children is None:
            names = [arg for arg in (name, friendly_name) if arg is not None]
            if len(names) != 1:
                raise SPyValueError('If no `children` argument is given, exactly one of the following arguments must '
                                    'be given: `name`, `friendly_name`')
            else:
                children = pd.DataFrame([{
                    'Name': names[0]
                }])
        elif name is not None:
            raise SPyValueError('Only one of the following arguments may be given: `name`, `children`')

        def _child_element_to_dict(child):
            if isinstance(child, dict):
                return child
            if isinstance(child, str):
                return {'ID': child} if _common.is_guid(child) else {'Name': child}
            if isinstance(child, pd.Series):
                return child.to_dict()
            else:
                raise SPyValueError(f'List input to children argument contained data not of type str, '
                                    f'dict, or pandas.Series: {child}')

        if isinstance(children, str) or isinstance(children, dict) or isinstance(children, pd.Series):
            children = [children]
        if isinstance(children, list):
            children = pd.DataFrame(map(_child_element_to_dict, children))
        elif isinstance(children, Tree):
            children = children._dataframe.copy()

        if roll_up_statistic or formula:
            if 'Formula' in children.columns or 'Formula Parameters' in children.columns:
                raise SPyValueError("Children DataFrame cannot contain a 'Formula' or 'Formula Parameters' column "
                                    "when inserting a roll up.")

        if roll_up_statistic:
            if formula:
                raise SPyValueError(f'Cannot specify a formula and a roll-up statistic simultaneously.')
            if 'Roll Up Statistic' in children.columns or 'Roll Up Parameters' in children.columns:
                raise SPyValueError("Children DataFrame cannot contain a 'Roll Up Statistic' or 'Roll Up Parameters' "
                                    "column when inserting a roll up.")
            children['Roll Up Statistic'] = roll_up_statistic
            children['Roll Up Parameters'] = roll_up_parameters

        if formula:
            children['Formula'] = formula
            children['Formula Parameters'] = [formula_parameters] * len(children)

        _utils.initialize_status_df(status, 'Inserted',
                                    'Processing item properties and finding children to be inserted.',
                                    Status.RUNNING)

        # Check user input for errors, filter if errors='catalog'
        children = _validate.validate_and_filter(self.session, children, status, stage='input',
                                                 temporal_description='in input')

        children = children.reset_index(drop=True)

        if parent is not None and 'Parent' in children.columns:
            raise SPyRuntimeError('If a "Parent" column is specified in the children dataframe, then the parent '
                                  'argument of the insert() method must be None')

        if _match.is_column_value_query(parent):
            children['Parent'] = children.apply(_match.fill_column_values, axis=1, query=parent)
        elif 'Parent' in children.columns:
            children['Parent'] = children.apply(_match.fill_column_values, axis=1,
                                                query_column='Parent')

        if friendly_name is not None:
            if _match.is_column_value_query(friendly_name):
                children['Friendly Name'] = friendly_name
            else:
                children['Name'] = friendly_name
        _properties.apply_friendly_name(children)

        display_template_map = self._display_template_map.copy()

        # Sanitize data and pull in properties of items with IDs
        children = _properties.process_properties(self.session, children, display_template_map, self._datasource,
                                                  status, keep_parent_column=True)

        # Pull children of items with pre-existing IDs
        children = _pull.pull_all_children_of_all_nodes(self.session, children, self._workbook_id, display_template_map,
                                                        self._datasource, status)

        # We concatenate all children to be inserted into one dataframe before
        # inserting them using a single pd.merge call
        additions = list()
        if 'Parent' in children.columns:
            for node, matched_indices in _match.Query(self._dataframe).multimatch(children['Parent']):
                children_to_add = _path.set_children_path(children.loc[list(matched_indices)].copy(), node.full_path,
                                                          node.depth)
                additions.append(children_to_add)
        else:
            for node in _match.Query(self._dataframe).matches(parent).get_node_set():
                children_to_add = _path.set_children_path(children.copy(), node.full_path, node.depth)
                additions.append(children_to_add)

        additions = pd.concat(additions, ignore_index=True) if additions else pd.DataFrame()
        # Remove duplicate items in case the user has passed duplicate information to the children parameter
        _utils.drop_duplicate_items(additions)

        _utils.increment_status_df(status, new_items=additions)

        # Merge the dataframes on case-insensitive 'Path' and 'Name' columns
        working_df = _utils.upsert(self._dataframe.copy(), additions)
        _path.sort_by_node_path(working_df)

        # If errors occur during the following validation step, they are "our fault", i.e., we inserted into the tree
        # incorrectly. We ideally want all feasible user errors to be reported before this point
        working_df = _validate.validate_and_filter(self.session, working_df, status, stage='final',
                                                   temporal_description='while inserting',
                                                   subtract_errors_from_status=True)
        self._dataframe = working_df
        self._display_template_map = display_template_map
        self.is_dirty = True

        if status.df.squeeze()['Total Items Inserted'] == 0 and status.df.squeeze()['Errors Encountered'] == 0:
            status.warn('No matching parents found. Nothing was inserted.')
        status.update(f'Successfully inserted items into the tree. {self.summarize(ret=True)}', Status.SUCCESS)

    def remove(self,
               elements: Union[pd.DataFrame, str, int],
               *,
               errors: Optional[str] = None,
               quiet: Optional[bool] = None,
               status: Optional[Status] = None):
        """
        Remove the specified elements from the tree recursively.

        Parameters
        ----------
        elements : {pandas.DataFrame, str, int}
            Defines which element or elements will be removed.

            1) String name match (case-insensitive equality, globbing, regex, column
               values) will find any existing nodes in the tree that match.
            2) String path match, including partial path matches.
            3) ID. This can either be the actual ID of the tree.push()ed node or the
               ID of the source item.
            4) Number specifying tree level. This will add the children below every
               node at the specified level in the tree (1 being the root node).
            5) spy.search results or other custom dataframe.

        errors : {'raise', 'catalog'}, optional
            If 'raise', any errors encountered will cause an exception. If 'catalog',
            errors will be added to a 'Result' column in the status.df DataFrame. This
            input will be used only for the duration of this function; it will default
            to the setting on the Tree if not specified.

        quiet : bool, optional
            If True, suppresses progress output. This input will be used only for the
            duration of this function; it will default to the setting on the Tree if
            not specified. Note that when status is provided, the quiet setting of
            the Status object that is passed in takes precedence.

        status : spy.Status, optional
            If specified, the supplied Status object will be updated as the command
            progresses. It gets filled in with the same information you would see
            in Jupyter in the blue/green/red table below your code while the
            command is executed. The table itself is accessible as a DataFrame via
            the status.df property.
        """

        _common.validate_argument_types([
            (elements, 'elements', (pd.DataFrame, str, int)),
            (errors, 'errors', str),
            (quiet, 'quiet', bool),
            (status, 'status', Status)
        ])

        status = Status.validate(status, self.session, quiet, errors)

        _utils.initialize_status_df(status, 'Removed', 'Removing items from tree', Status.RUNNING)

        drop_rows_mask = _match.Query(self._dataframe).matches(elements).with_descendants().get_mask()
        working_df = self._dataframe.loc[~drop_rows_mask].reset_index(drop=True)

        _utils.increment_status_df(status, new_items=self._dataframe.loc[drop_rows_mask])

        working_df = _validate.validate_and_filter(self.session, working_df, status, stage='final',
                                                   temporal_description='while removing')
        self._dataframe = working_df
        self.is_dirty = True

        if status.df.squeeze()['Total Items Removed'] == 0 and status.df.squeeze()['Errors Encountered'] == 0:
            status.warn('No matches found. Nothing was removed.')
        status.update(f'Successfully removed items from the tree. {self.summarize(ret=True)}',
                      Status.SUCCESS)

    def move(self,
             source: Union[pd.DataFrame, str],
             destination: Optional[Union[pd.DataFrame, str]] = None,
             *,
             errors: Optional[str] = None,
             quiet: Optional[bool] = None,
             status: Optional[Status] = None):
        """
        Move the specified elements (and all children) from one location in
        the tree to another.

        Parameters
        ----------
        source : {pandas.DataFrame, str}
            Defines which element or elements will be moved.

            1) String path match.
            2) ID. This can either be the actual ID of the tree.push()ed node or the
               ID of the source item.
            3) spy.search results or other custom dataframe.

        destination : {pandas.DataFrame, str}; optional
            Defines the new parent for the source elements.

            1) No destination specified will move the elements to just below
               the root of the tree.
            2) String path match.
            3) ID. This can either be the actual ID of the tree.push()ed node or the
               ID of the source item.
            4) spy.search results or other custom dataframe.

        errors : {'raise', 'catalog'}, optional
            If 'raise', any errors encountered will cause an exception. If 'catalog',
            errors will be added to a 'Result' column in the status.df DataFrame. This
            input will be used only for the duration of this function; it will default
            to the setting on the Tree if not specified.

        quiet : bool, optional
            If True, suppresses progress output. This input will be used only for the
            duration of this function; it will default to the setting on the Tree if
            not specified. Note that when status is provided, the quiet setting of
            the Status object that is passed in takes precedence.

        status : spy.Status, optional
            If specified, the supplied Status object will be updated as the command
            progresses. It gets filled in with the same information you would see
            in Jupyter in the blue/green/red table below your code while the
            command is executed. The table itself is accessible as a DataFrame via
            the status.df property.
        """

        _common.validate_argument_types([
            (source, 'source', (pd.DataFrame, str)),
            (destination, 'destination', (pd.DataFrame, str)),
            (errors, 'errors', str),
            (quiet, 'quiet', bool),
            (status, 'status', Status)
        ])

        status = Status.validate(status, self.session, quiet, errors)

        _utils.initialize_status_df(status, 'Moved', 'Moving items in tree.', Status.RUNNING)

        # Find the destination. Fail if there is not exactly one match for the input
        destination_query = _match.Query(self._dataframe).matches(destination)
        destination_nodes = destination_query.get_node_list()
        if len(destination_nodes) == 0:
            raise SPyValueError('Destination does not match any item in the tree.')
        elif len(destination_nodes) > 1:
            matched_names = '"%s"' % '", "'.join([node.full_path for node in destination_nodes])
            raise SPyValueError(f'Destination must match a single element of the tree. Specified destination '
                                f'matches: "{matched_names}".')
        destination_node = destination_nodes[0]
        if destination_node.type != 'Asset':
            raise SPyValueError('Destination must be an asset.')

        # Find all source items, collect all of their children, and separate all matches into discrete subtrees.
        source_query = _match.Query(self._dataframe) \
            .matches(source) \
            .exclude(destination_query.children()) \
            .with_descendants()
        if destination_node in source_query.get_node_set():
            raise SPyValueError('Source cannot contain the destination')

        additions = list()
        for subtree_root in source_query.get_distinct_subtree_roots():
            subtree_df = _match.Query(self._dataframe) \
                .matches(subtree_root) \
                .with_descendants() \
                .get_filtered_rows() \
                .copy()
            additions.append(_path.trim_unneeded_paths(subtree_df, parent_full_path=destination_node.full_path))

        additions = pd.concat(additions, ignore_index=True) if additions else pd.DataFrame()
        additions['ID'] = np.nan

        _utils.increment_status_df(status, new_items=additions)

        # Drop the old items and utils.upsert the modified items
        working_df = self._dataframe.loc[~source_query.get_mask()].copy()
        working_df = _utils.upsert(working_df, additions)
        _path.sort_by_node_path(working_df)

        working_df = _validate.validate_and_filter(self.session, working_df, status, stage='final',
                                                   temporal_description='after moving')
        self._dataframe = working_df
        self.is_dirty = True

        if status.df.squeeze()['Total Items Moved'] == 0 and status.df.squeeze()['Errors Encountered'] == 0:
            status.warn('No matches found. Nothing was moved.')
        status.update(f'Successfully moved items within the tree. {self.summarize(ret=True)}',
                      Status.SUCCESS)

    @property
    def size(self) -> int:
        """
        Property that gives the number of elements currently in the tree.
        """
        return len(self._dataframe)

    def __len__(self):
        return self.size

    @property
    def height(self) -> int:
        """
        Property that gives the current height of the tree. This is the length
        of the longest item path within the tree.
        """
        return self._dataframe['Depth'].max() if len(self._dataframe) > 0 else 0

    def items(self) -> pd.DataFrame:
        """
        Return a copy of the dataframe that represents the metadata of the tree.

        Returns
        -------
        pandas.DataFrame
        """
        return self._dataframe.copy()

    def count(self, item_type: Optional[str] = None) -> int:
        """
        Count the number of elements in the tree of each Seeq type. If item_type
        is not specified, then returns a dictionary with keys 'Asset', 'Signal',
        'Condition', 'Scalar', and 'Unknown'. If item_type is specified, then
        returns an int.

        Parameters
        ----------
        item_type : {'Asset', 'Signal', 'Condition', 'Scalar', 'Uncompiled Formula'}, optional
            If specified, then the method will return an int representing the
            number of elements with Type item_type. Otherwise, a dict will be
            returned.
        """
        if len(self._dataframe) == 0:
            return 0 if item_type else dict()

        simple_types = ['Asset', 'Signal', 'Condition', 'Scalar', 'Metric', 'Display', 'Uncompiled Formula']
        if item_type:
            if not isinstance(item_type, str) or item_type.capitalize() not in (simple_types + ['Formula',
                                                                                                'Uncompiled']):
                raise SPyValueError(f'"{item_type}" is not a valid node type. Valid types are: '
                                    f'{", ".join(simple_types)}')
            if item_type in ['Uncompiled Formula', 'Uncompiled', 'Formula']:
                return sum(pd.isnull(self._dataframe['Type']) | (self._dataframe['Type'] == ''))
            else:
                return sum(self._dataframe['Type'].str.contains(item_type.capitalize(), na=False))

        def _simplify_type(t):
            if not pd.isnull(t):
                for simple_type in simple_types:
                    if simple_type in t:
                        return simple_type
            return 'Uncompiled Formula'

        return self._dataframe['Type'] \
            .apply(_simplify_type) \
            .value_counts() \
            .to_dict()

    def summarize(self, ret: Optional[bool] = False):
        """
        Generate a human-readable summary of the tree.

        Parameters
        ----------
        ret : bool, default False
            If True, then this method returns a string summary of the tree. If
            False, then this method prints the summary and returns nothing.
        """
        counts = self.count()

        def _get_descriptor(k, v):
            singular_descriptors = {
                key: key.lower() if key != 'Uncompiled Formula' else 'calculation whose type has not '
                                                                     'yet been determined'
                for key in counts.keys()
            }
            plural_descriptors = {
                key: f'{key.lower()}s' if key != 'Uncompiled Formula' else 'calculations whose types have not '
                                                                           'yet been determined'
                for key in counts.keys()
            }
            if v == 1:
                return singular_descriptors[k]
            else:
                return plural_descriptors[k]

        nonzero_counts = {k: v for k, v in counts.items() if v != 0}
        if len(nonzero_counts) == 1:
            count_string = ''.join([f'{v} {_get_descriptor(k, v)}' for k, v in nonzero_counts.items()])
        elif len(nonzero_counts) == 2:
            count_string = ' and '.join([f'{v} {_get_descriptor(k, v)}' for k, v in nonzero_counts.items()])
        elif len(nonzero_counts) > 2:
            count_string = ', '.join([f'{v} {_get_descriptor(k, v)}' for k, v in nonzero_counts.items()])
            last_comma = count_string.rfind(',')
            count_string = count_string[:last_comma + 2] + 'and ' + count_string[last_comma + 2:]
        else:
            return '' if ret else None

        root_name = self._dataframe.iloc[0]['Name']

        summary = f'The tree "{root_name}" has height {self.height} and contains {count_string}.'

        if ret:
            return summary
        else:
            return _common.print_output(summary)

    def missing_items(self, return_type: Optional[str] = 'print'):
        """
        Identify elements that may be missing child elements based on the contents of other sibling nodes.

        Parameters
        ----------
        return_type : {'print', 'string', 'dict'}, default 'print'
            If 'print', then a string that enumerates the missing items will be
            printed. If 'string', then that same string will be returned and not
            printed. If 'dict', then a dictionary that maps element paths to lists
            of their potential missing children will be returned.
        """
        if return_type.lower() not in ['print', 'str', 'string', 'dict', 'dictionary', 'map']:
            raise SPyValueError(f"Illegal argument {return_type} for return_type. Acceptable values are 'print', "
                                f"'string', and 'dict'.")
        return_type = return_type.lower()

        if self.count(item_type='Asset') == self.size:
            missing_string = 'There are no non-asset items in your tree.'
            if return_type in ['dict', 'dictionary', 'map']:
                return dict()
            elif return_type == 'print':
                _common.print_output(missing_string)
                return
            else:
                return missing_string

        repeated_grandchildren = dict()

        prev_row = None
        path_stack = []
        for _, row in self._dataframe.iterrows():
            if prev_row is None:
                pass
            elif row.Depth > prev_row.Depth:
                path_stack.append((prev_row, set()))
            else:
                path_stack = path_stack[:row.Depth - 1]
            if len(path_stack) > 1:
                grandparent, grandchildren_set = path_stack[-2]
                if row.Name in grandchildren_set:
                    repeated_grandchildren.setdefault(_path.get_full_path(grandparent),
                                                      set()).add(row.Name)
                else:
                    grandchildren_set.add(row.Name)
            prev_row = row

        missing_item_map = dict()
        path_stack = []
        for _, row in self._dataframe.iterrows():
            if prev_row is None:
                pass
            elif row.Depth > prev_row.Depth:
                if path_stack and _path.get_full_path(
                        path_stack[-1][0]) in repeated_grandchildren:
                    required_children = repeated_grandchildren[
                        _path.get_full_path(path_stack[-1][0])].copy()
                else:
                    required_children = set()
                path_stack.append((prev_row, required_children))
            else:
                for parent, required_children in path_stack[row.Depth - 1:]:
                    if len(required_children) != 0:
                        missing_item_map[_path.get_full_path(parent)] = sorted(required_children)
                path_stack = path_stack[:row.Depth - 1]
            if len(path_stack) != 0:
                _, required_children = path_stack[-1]
                required_children.discard(row.Name)
            prev_row = row
        for parent, required_children in path_stack:
            if len(required_children) != 0:
                missing_item_map[_path.get_full_path(parent)] = sorted(required_children)

        if return_type in ['dict', 'dictionary', 'map']:
            return missing_item_map

        if len(missing_item_map):
            missing_string = 'The following elements appear to be missing:'
            for parent_path, missing_children in missing_item_map.items():
                missing_string += f"\n{parent_path} is missing: {', '.join(missing_children)}"
        else:
            missing_string = 'No items are detected as missing.'

        if return_type == 'print':
            return _common.print_output(missing_string)
        else:
            return missing_string

    @property
    def name(self) -> str:
        """
        Property that gives the name of the tree's root asset.
        """
        return self._dataframe.loc[0, 'Name'] if len(self._dataframe) > 0 else ''

    @name.setter
    def name(self, value: str):
        _common.validate_argument_types([(value, 'name', str)])

        df = _utils.set_name(self._dataframe, value)
        _validate.validate_and_filter(self.session, df, Status(quiet=True, errors='raise'), stage='final',
                                      temporal_description='after changing tree root name')

        self._dataframe = df
        self.is_dirty = True

    @property
    def workbook(self) -> Optional[str]:
        """ Property that gives the current workbook of the tree. Returns None if tree is globally scoped """
        return self._workbook

    @workbook.setter
    def workbook(self, workbook: Optional[str]):
        _common.validate_argument_types([(workbook, 'workbook', str)])
        self._workbook = workbook
        status = Status(quiet=False)
        if workbook is not None:
            self._find_workbook_id(status.create_inner('Find Workbook'))
        if 'ID' in self._dataframe.columns:
            self._dataframe['ID'] = pd.Series(np.nan, self._dataframe.index, dtype=object)

    def visualize(self, subtree: Optional[str] = None, print_tree: Optional[bool] = True):
        """
        Prints an ASCII visualization of this tree to stdout.

        subtree : str, optional
            Specifies an asset in the tree. Only the part of the tree below this asset
            will be visualized.
        print_tree: bool, optional
            True (default) to print the tree visualization, False to return it as a string
        """
        if len(self._dataframe) == 0:
            return
        if subtree is None:
            df = self._dataframe[['Name', 'Depth']]
        else:
            query = _match.Query(self._dataframe).matches(subtree)
            results = query.get_node_set()
            if len(results) == 0:
                raise SPyValueError('Subtree query did not match any node in the tree.')
            elif len(results) > 1:
                error_list = '\n- '.join([node.full_path for node in results])
                raise SPyValueError(f'Subtree query matched multiple nodes in the tree:\n- {error_list}')
            df = query.with_descendants().get_filtered_rows()[['Name', 'Depth']]

        tree_vis = _utils.visualize(df)
        if print_tree:
            return _common.print_output(tree_vis)
        return tree_vis

    def select(self,
               within: Optional[Union[pd.DataFrame, list[str], str]] = None,
               *,
               condition: Optional[Union[pd.DataFrame, list[str], str]] = None,
               start: Optional[Union[pd.Timestamp, str]] = None,
               end: Optional[Union[pd.Timestamp, str]] = None,
               errors: Optional[str] = None,
               quiet: Optional[bool] = None,
               status: Optional[Status] = None) -> Tree:
        """
        Select a subtree based on the specified parameters.
        Selects nodes of this Tree - creating a sub-tree - based on one or more Seeq Conditions present in the tree
        and evaluated during the given date range.  If the within argument is given, then only the related nodes are
        evaluated and possibly selected.

        Parameters
        ----------
        within : {pandas.DataFrame, list, str}, optional
            Specifies a pattern by which to select elements from the tree. If an element of the tree matches this
            pattern, then itself, its descendants, and its ancestors will be included in the resulting subtree

            The following rules apply:

            1) If not specified, the selection will return all items matching the given `condition`, `start`,
            and `end` criterion.
            2) String name match (case-insensitive equality, globbing, regex) will find any existing
            nodes in the tree that match.
            3) String path match, including partial path matches.
            4) ID. This can either be the actual ID of the pushed node or the ID of the source item.
            5) spy.search results or other custom dataframe.

        condition : {pandas.DataFrame, list, str}, optional
            If specified, then only assets that contain a condition matching this argument that have at least one
            capsule present in the start-end range specified will be included in the subtree.

        start: {str, pd.Timestamp}, optional
            The start date for which to evaluate the specified condition(s).

        end: {str, pd.Timestamp}, optional
            The end date for which to evaluate the specified condition(s).

        errors : {'raise', 'catalog'}, optional - default 'catalog'
            If 'raise', any errors encountered will cause an exception. If 'catalog',
            errors will be added to a 'Result' column in the status.df DataFrame. This
            input will be used only for the duration of this function; it will default
            to the setting on the Tree if not specified.

        quiet : bool, optional
            If True, suppresses progress output. This input will be used only for the
            duration of this function; it will default to the setting on the Tree if
            not specified. Note that when status is provided, the quiet setting of
            the Status object that is passed in takes precedence.

        status : spy.Status, optional
            If specified, the supplied Status object will be updated as the command
            progresses. It gets filled in with the same information you would see
            in Jupyter in the blue/green/red table below your code while the
            command is executed. The table itself is accessible as a DataFrame via
            the status.df property.

        Returns
        -------
        Tree
            A new Tree containing the items matching the given selection criterion.
        """

        _common.validate_argument_types([
            (within, 'within', (pd.DataFrame, list, str, dict)),
            (condition, 'condition', (pd.DataFrame, list, str, dict)),
            (start, 'start', (str, pd.Timestamp, datetime.date)),
            (end, 'end', (str, pd.Timestamp, datetime.date)),
            (errors, 'errors', str),
            (quiet, 'quiet', bool),
            (status, 'status', Status)
        ])

        status = Status.validate(status, self.session, quiet, errors)

        if condition is None and within is None:
            raise SPyValueError("At least one of `condition` or `within` arguments must be provided")

        if condition is not None:
            if self.is_dirty:
                raise SPyRuntimeError("Tree is dirty. Call the push function to commit the changes before calling "
                                      "`select` with `condition`")
            start, end = validate_start_and_end(self.session, start, end)

        if self.size == 0:
            return self

        _utils.initialize_status_df(status, 'Selected',
                                    'Selecting elements from tree based on query.',
                                    Status.RUNNING)

        query = _match.Query(self._dataframe)
        if within is not None:
            query = query.matches(within).with_descendants()

        error_conditions = None
        if condition is not None:
            query = query.matches(condition).has_type('Condition')
            condition_ids = (node.id for node in query.get_node_set())
            inner_status = status.create_inner('Evaluating conditions')
            active_condition_ids, error_conditions = _pull.get_active_conditions(self.session, condition_ids, start,
                                                                                 end, inner_status)
            query = query.matches(lambda node: node.id in active_condition_ids).parents().with_descendants()

        query = query.with_ancestors()
        result_df = query.get_filtered_rows().reset_index(drop=True)

        result = DownSelectedTree(self)
        result._dataframe = result_df

        _utils.increment_status_df(status, new_items=result_df, error_items=error_conditions)

        tree_summary = re.sub(r'^The tree ".*"', 'The down-selected tree', result.summarize(ret=True))
        status.update(f'Successfully created down-selected tree. {tree_summary}', Status.SUCCESS)

        return result

    @Status.handle_keyboard_interrupt()
    def push(self, *,
             metadata_state_file: Optional[str] = None,
             errors: Optional[str] = None,
             quiet: Optional[bool] = None,
             status: Optional[Status] = None) -> pd.DataFrame:
        """
        Imports the tree into Seeq Server.

        metadata_state_file : str, optional
            The file name (with full path, if desired) to a "metadata state file"
            to use for "incremental" pushing, which can dramatically speed up
            pushing of a large asset tree. If supplied, the metadata push
            operation uses the state file to determine what changed since the
            last time the metadata was pushed and it will only act on those
            changes. Note that if a pushed calculation is manually changed via
            Workbench but your spy.push() metadata has not changed, you must
            exclude this argument in order for the push to affect that calculation
            again.

        errors : {'raise', 'catalog'}, optional
            If 'raise', any errors encountered will cause an exception. If 'catalog',
            errors will be added to a 'Result' column in the status.df DataFrame. This
            input will be used only for the duration of this function; it will default
            to the setting on the Tree if not specified.

        quiet : bool, optional
            If True, suppresses progress output. This input will be used only for the
            duration of this function; it will default to the setting on the Tree if
            not specified. Note that when status is provided, the quiet setting of
            the Status object that is passed in takes precedence.

        status : spy.Status, optional
            If specified, the supplied Status object will be updated as the command
            progresses. It gets filled in with the same information you would see
            in Jupyter in the blue/green/red table below your code while the
            command is executed. The table itself is accessible as a DataFrame via
            the status.df property.

        Returns
        -------
        pandas.DataFrame
            A DataFrame with the metadata for the items pushed, along with any
            errors and statistics about the operation. See spy.push()
            documentation for further details on this returned DataFrame.
        """
        _common.validate_argument_types([
            (errors, 'errors', str),
            (quiet, 'quiet', bool),
            (status, 'status', Status)
        ])

        status = Status.validate(status, self.session, quiet, errors)

        if self._workbook is not None:
            self._push_workbook()

        _properties.push_and_replace_display_templates(self.session, self._dataframe, self._display_template_map,
                                                       self._datasource, self._workbook_id, status)
        self._display_template_map.clear()

        _properties.archive_and_remove_displays(self.session, self._display_ids_to_archive)
        self._display_ids_to_archive.clear()

        df_to_push = _properties.format_references(self._dataframe)

        push_results = _push.push(metadata=df_to_push, workbook=self._workbook, datasource=self._datasource,
                                  archive=True, metadata_state_file=metadata_state_file, status=status,
                                  session=self.session)

        if self._workbook and self._workbook_id == _constants.UNKNOWN:
            self._find_workbook_id(status.create_inner('Find Workbook', quiet=True))

        successfully_pushed = push_results['Push Result'].str.startswith('Success')
        self._dataframe.loc[successfully_pushed, 'ID'] = push_results.loc[successfully_pushed, 'ID']
        self._dataframe.loc[successfully_pushed, 'Type'] = push_results.loc[successfully_pushed, 'Type']

        self.is_dirty = False
        return push_results

    def _push_workbook(self):
        self._find_workbook_id(status=spy.Status(quiet=True))
        if self._workbook_id != _constants.UNKNOWN:
            return
        search_query, workbook_name = WorkbookContext.create_analysis_search_query(self._workbook)
        workbook = spy.workbooks.Analysis({'Name': workbook_name})
        workbook.worksheet(_common.DEFAULT_WORKSHEET_NAME)
        spy.workbooks.push(workbook, path=_common.get(search_query, 'Path'), include_inventory=False,
                           datasource=self._datasource, status=spy.Status(quiet=True),
                           session=self.session)
        self._workbook_id = workbook.id

    def _ipython_display_(self):
        self.visualize()

    def __repr__(self, *args, **kwargs):
        return self.visualize(print_tree=False)

    def __iter__(self):
        return self._dataframe.itertuples(index=False, name='Item')

    def _find_workbook_id(self, status):
        """
        Set the _workbook_id based on the workbook input. This will enable us to know whether we should set
        the `ID` or `Referenced ID` column when pulling an item.
        """
        if _common.is_guid(self._workbook):
            self._workbook_id = _common.sanitize_guid(self._workbook)
        elif self.session.client:
            search_query, _ = WorkbookContext.create_analysis_search_query(self._workbook)
            search_df = spy.workbooks.search(search_query, status=status, session=self.session)
            self._workbook_id = search_df.iloc[0]['ID'] if len(search_df) > 0 else _constants.UNKNOWN
        else:
            self._workbook_id = _constants.UNKNOWN

    @property
    def df(self):
        # This is a convenience function so that IntelliJ can recognize and render it as a DataFrame
        return pd.DataFrame(self._dataframe)


class DownSelectedTree(Tree):
    # noinspection PyMissingConstructor
    def __init__(self, original: Tree):
        self._dataframe = pd.DataFrame()
        self._datasource = original._datasource
        self._workbook = original._workbook
        self._workbook_id = original._workbook_id
        self.status = original.status
        self.session = original.session
        self.original_name = original.name
        self.original_tree = original
        self.is_dirty = original.is_dirty

    def _operation_not_allowed(self, name: str):
        raise SPyTypeError(
            f'Calling `{name}` is not allowed because this Tree was constructed by selecting from another '
            f'Tree ({self.original_name})')

    def push(self, *args, **kwargs):
        self._operation_not_allowed('push')

    def move(self, *args, **kwargs):
        self._operation_not_allowed('move')

    def insert(self, *args, **kwargs):
        self._operation_not_allowed('insert')

    def remove(self, *args, **kwargs):
        self._operation_not_allowed('remove')

    @Tree.name.setter
    def name(self, *args, **kwargs):
        self._operation_not_allowed('name')
