from copy import deepcopy
from unittest import mock

import pytest

from seeq import spy, sdk
from seeq.spy import Session
from seeq.spy._dependencies import Dependencies
from seeq.spy._errors import SPyValueError


@pytest.mark.unit
def test_upgrade():
    mock_session = mock.Mock(Session)
    mock_session.client = {}
    mock_session.server_version = '999999.456.789'

    def _override_is_ipython():
        return True

    def _override_is_ipython_interactive():
        return True

    mock_kernel = mock.Mock()
    mock_ipython = mock.Mock()
    mock_ipython.kernel = mock_kernel

    def _override_get_ipython():
        return mock_ipython

    with mock.patch('seeq.spy._datalab.is_ipython', _override_is_ipython), \
            mock.patch('seeq.spy._datalab.is_ipython_interactive', _override_is_ipython_interactive), \
            mock.patch('IPython.get_ipython', _override_get_ipython):
        patched_variable = 'seeq.spy.__version__' if not hasattr(sdk, '__version__') else 'seeq.sdk.__version__'
        patched_version = '888888.0.0.100.10' if not hasattr(sdk, '__version__') else '888888.0.0'
        with mock.patch(patched_variable, patched_version):
            # Seeq Server version mismatch, so we'll install a compatible seeq module in addition to SPy
            spy.upgrade(session=mock_session, dependencies=[])
            mock_ipython.run_cell.assert_called_with(
                f'pip install -U seeq~=999999.456 && pip install -U seeq-spy')
            assert not mock_kernel.do_shutdown.called

            # Upgrade can specify a particular SPy version.
            spy.upgrade(version='223.18', session=mock_session, dependencies=[])
            mock_ipython.run_cell.assert_called_with(
                f'pip install -U seeq~=999999.456 && pip install -U seeq-spy==223.18')

        # Seeq Server version matches, so we'll only upgrade SPy
        patched_version = '999999.0.0.100.10' if not hasattr(sdk, '__version__') else '999999.0.0'
        with mock.patch(patched_variable, patched_version):
            spy.upgrade(session=mock_session, dependencies=[])
            mock_ipython.run_cell.assert_called_with(f'pip install -U seeq-spy')

            # Upgrade can specify a particular SPy version.
            spy.upgrade(version='223.18', session=mock_session, force_restart=True, dependencies=[])
            mock_ipython.run_cell.assert_called_with(f'pip install -U seeq-spy==223.18')
            mock_kernel.do_shutdown.assert_called_with(True)

            # Old server versioning scheme (before R50)
            mock_session.server_version = '0.456.789'
            with pytest.raises(SPyValueError, match='incompatible with Seeq Server version 0.456.789'):
                spy.upgrade(version='223.18', session=mock_session, dependencies=[])

            spy.upgrade(session=mock_session, dependencies=[])
            mock_ipython.run_cell.assert_called_with('pip uninstall -y seeq-spy && pip install -U seeq~=0.456.789')

            # New server versioning scheme (R50 and after)
            mock_session.server_version = '50.456.789'
            with pytest.raises(SPyValueError, match='incompatible with Seeq Server version 50.456.789'):
                spy.upgrade(version='223.18', session=mock_session, dependencies=[])

            spy.upgrade(session=mock_session, dependencies=[])
            mock_ipython.run_cell.assert_called_with('pip uninstall -y seeq-spy && pip install -U seeq~=50.456')

            # Going from R60+ SDK version to before R60 server version
            spy.upgrade(version='R50.0.1.184.15', session=mock_session, dependencies=[])
            mock_ipython.run_cell.assert_called_with('pip uninstall -y seeq-spy && pip install -U seeq==50.0.1.184.15')

            # Compatible module is found on PYTHONPATH
            with mock.patch('seeq.spy._login.find_compatible_module', lambda session: 'not None'):
                mock_session.server_version = '0.456.789'
                spy.upgrade(session=mock_session, dependencies=[])
                mock_ipython.run_cell.assert_called_with('pip uninstall -y seeq-spy && pip install -U seeq~=0.456.789')

                mock_session.server_version = '50.456.789'
                spy.upgrade(session=mock_session, dependencies=[])
                mock_ipython.run_cell.assert_called_with('pip uninstall -y seeq-spy && pip install -U seeq~=50.456')

                mock_session.server_version = '888888.456.789'
                spy.upgrade(session=mock_session, dependencies=[])
                mock_ipython.run_cell.assert_called_with('pip uninstall -y seeq && pip install -U seeq-spy')

        # Going from before R60 server version to R60+ SDK version
        patched_version = '58.0.0.100.10' if not hasattr(sdk, '__version__') else '58.0.0'
        with mock.patch(patched_variable, patched_version):
            mock_session.server_version = '999999.456.789'
            spy.upgrade(session=mock_session, dependencies=[])
            mock_ipython.run_cell.assert_called_with(f'pip install -U seeq~=999999.456 && pip install -U seeq-spy')

            spy.upgrade(version='223.18', session=mock_session, force_restart=True, dependencies=[])
            mock_ipython.run_cell.assert_called_with(
                f'pip install -U seeq~=999999.456 && pip install -U seeq-spy==223.18')
            mock_kernel.do_shutdown.assert_called_with(True)

            # Compatible module is found on PYTHONPATH
            with mock.patch('seeq.spy._login.find_compatible_module', lambda session: 'not None'):
                spy.upgrade(session=mock_session, dependencies=[])
                mock_ipython.run_cell.assert_called_with('pip uninstall -y seeq && pip install -U seeq-spy')

                spy.upgrade(version='283.12', session=mock_session, dependencies=[])
                mock_ipython.run_cell.assert_called_with('pip uninstall -y seeq && pip install -U seeq-spy==283.12')

            mock_session.server_version = '58.456.789'
            spy.upgrade(session=mock_session, dependencies=[])
            mock_ipython.run_cell.assert_called_with(f'pip install -U seeq~=58.456')

        with pytest.raises(SPyValueError, match='version argument "blah" is not a full version'):
            spy.upgrade(version='blah', session=mock_session, dependencies=[])

    # Error expected if not in IPython
    with mock.patch('seeq.spy._datalab.is_ipython_interactive', _override_is_ipython_interactive), \
            mock.patch('IPython.get_ipython', _override_get_ipython), \
            mock.patch('seeq.spy._datalab.is_ipython', lambda: False):
        with pytest.raises(SPyValueError, match='must be invoked from a Jupyter notebook'):
            spy.upgrade(session=mock_session, dependencies=[])

    # Error expected if not in a Jupyter notebook
    with mock.patch('seeq.spy._datalab.is_ipython', _override_is_ipython), \
            mock.patch('IPython.get_ipython', _override_get_ipython):
        with pytest.raises(SPyValueError, match='must be invoked from a Jupyter notebook'):
            spy.upgrade(session=mock_session, dependencies=[])

    # Error expected if not able to get IPython instance
    with mock.patch('seeq.spy._datalab.is_ipython', _override_is_ipython), \
            mock.patch('seeq.spy._datalab.is_ipython_interactive', _override_is_ipython_interactive), \
            mock.patch('IPython.get_ipython', lambda: None):
        with pytest.raises(SPyValueError, match='must be invoked from a Jupyter notebook'):
            spy.upgrade(session=mock_session, dependencies=[])

    # Error expected if not able to get kernel for restart
    mock_ipython.kernel = None
    with mock.patch('seeq.spy._datalab.is_ipython', _override_is_ipython), \
            mock.patch('seeq.spy._datalab.is_ipython_interactive', _override_is_ipython_interactive), \
            mock.patch('IPython.get_ipython', _override_get_ipython):
        with pytest.raises(SPyValueError, match='Unable get IPython kernel to complete restart'):
            spy.upgrade(session=mock_session, force_restart=True, dependencies=[])


# Since Dependencies class uses __import__ function to check for installed dependencies, we need to mock it
# the extras argument is used to simulate the installed dependencies
def mock_import_factory(extras=None):
    # Create a deep copy of the dependency mapping so that we can remove the 'all' key
    dependencies_mapping = deepcopy(Dependencies.dependency_mapping)
    del dependencies_mapping['all']

    def mock_import(name, globals=None, locals=None, fromlist=(), level=0):
        if extras is not None:
            # Keep track of which extras are being mocked
            extras_list = list(Dependencies.dependency_mapping.keys())
            for extra in extras:
                del extras_list[extras_list.index(extra)]

                # If the dependency currently being imported is in the "installed" extra, return a mock object
                if name in dependencies_mapping.get(extra, []):
                    # IPython is a special case where we need to return the original import so that the test can
                    # check if the upgrade function is calling the correct commands
                    if name == 'IPython':
                        return original_import(name, globals, locals, fromlist, level)
                    return mock.Mock()

            # All the extras that are not "installed" need to be mocked to raise an ImportError
            for extra in extras_list:
                if name in dependencies_mapping.get(extra, []):
                    raise ImportError(f'No module named {name}')

        # ipylab is a special case where we always need to return a mock object
        # This is due to an unsuppressable warning that is raised when importing ipylab
        if name == 'ipylab':
            return mock.Mock()

        # If the dependency is not one of the SPy extras, implying it is SPy core or not part of the SPy library,
        # then return the original import
        return original_import(name, globals, locals, fromlist, level)

    return mock_import


original_import = __builtins__['__import__']  # Save the original import function for later use


@pytest.mark.unit
def test_upgrade_with_dependencies():
    mock_session = mock.Mock(spec=Session)
    mock_session.server_version = '999999.456.789'
    mock_kernel = mock.Mock()
    mock_ipython = mock.Mock()
    mock_ipython.kernel = mock_kernel

    # Test upgrade with dependencies override
    with mock.patch('seeq.spy._datalab.is_ipython', return_value=True), \
            mock.patch('seeq.spy._datalab.is_ipython_interactive', return_value=True), \
            mock.patch('IPython.get_ipython', return_value=mock_ipython):
        # Test with multiple dependencies
        spy.upgrade(session=mock_session, dependencies=['widgets', 'templates'])
        expected_command = f'pip install -U seeq~=999999.456 && pip install -U seeq-spy[widgets,templates]'
        mock_ipython.run_cell.assert_called_with(expected_command)

        # Test with single dependency
        spy.upgrade(session=mock_session, dependencies=['jupyter'])
        expected_command = f'pip install -U seeq~=999999.456 && pip install -U seeq-spy[jupyter]'
        mock_ipython.run_cell.assert_called_with(expected_command)

        # Test with all dependencies
        spy.upgrade(session=mock_session, dependencies=['all'])
        expected_command = f'pip install -U seeq~=999999.456 && pip install -U seeq-spy[all]'
        mock_ipython.run_cell.assert_called_with(expected_command)

        # Test with no dependencies
        spy.upgrade(session=mock_session, dependencies=[])
        expected_command = f'pip install -U seeq~=999999.456 && pip install -U seeq-spy'
        mock_ipython.run_cell.assert_called_with(expected_command)

        # Test with invalid dependencies
        spy.upgrade(session=mock_session, dependencies=['invalid_option'])
        expected_command = f'pip install -U seeq~=999999.456 && pip install -U seeq-spy'
        mock_ipython.run_cell.assert_called_with(expected_command)

    # Test upgrade if SPy is partially installed
    with mock.patch('seeq.spy._datalab.is_ipython', return_value=True), \
            mock.patch('seeq.spy._datalab.is_ipython_interactive', return_value=True), \
            mock.patch('IPython.get_ipython', return_value=mock_ipython):
        # Test that all dependencies are installed
        with mock.patch('builtins.__import__', side_effect=mock_import_factory()):
            spy.upgrade(session=mock_session)
            expected_command = 'pip install -U seeq~=999999.456 && pip install -U seeq-spy[all]'
            mock_ipython.run_cell.assert_called_with(expected_command)

        # Test upgrade with only templates extra installed
        with mock.patch('builtins.print') as mock_print:
            with mock.patch('builtins.__import__', side_effect=mock_import_factory(extras=['templates'])):
                spy.upgrade(session=mock_session)
                mock_print.assert_called_with("Unable to import `IPython`. Please run " +
                                              "`pip install -U seeq~=999999.456 && pip install -U seeq-spy[templates]`"
                                              + " in a terminal to upgrade SPy.")

        # Test upgrade with templates and widgets extras installed
        with mock.patch('builtins.__import__', side_effect=mock_import_factory(['templates', 'widgets'])):
            spy.upgrade(session=mock_session)
            expected_command = 'pip install -U seeq~=999999.456 && pip install -U seeq-spy[templates,widgets]'
            mock_ipython.run_cell.assert_called_with(expected_command)

        # Test upgrade override with partial dependencies
        with mock.patch('builtins.__import__', side_effect=mock_import_factory(['templates', 'widgets'])):
            spy.upgrade(session=mock_session, dependencies=['widgets'])
            expected_command = 'pip install -U seeq~=999999.456 && pip install -U seeq-spy[widgets]'
            mock_ipython.run_cell.assert_called_with(expected_command)

        # Test with no extras installed
        with mock.patch('builtins.print') as mock_print:
            with mock.patch('builtins.__import__', side_effect=mock_import_factory([])):
                spy.upgrade(session=mock_session)
                mock_print.assert_called_with("Unable to import `IPython`. Please run " +
                                              "`pip install -U seeq~=999999.456 && pip install -U seeq-spy`"
                                              + " in a terminal to upgrade SPy.")
