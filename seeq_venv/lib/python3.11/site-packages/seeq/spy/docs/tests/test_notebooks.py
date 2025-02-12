"""Smoke test ensuring that example notebooks run without error."""

import concurrent.futures
import os
import pathlib
import re

import nbformat
import pytest
from nbconvert.preprocessors import ExecutePreprocessor

from seeq import spy
from seeq.base import util
from seeq.spy.tests import test_common

THIS_DIR = pathlib.Path(__file__).absolute().parent
DOCUMENTATION_DIR = THIS_DIR / ".." / "Documentation"


def setup_module():
    test_common.initialize_sessions()


def load_notebook(path):
    with util.safe_open(path, encoding='utf-8') as f:
        return nbformat.read(f, as_version=4)


def run_notebook(notebook_name):
    print(f'Running Jupyter Notebook "{notebook_name}"')

    # 7-bit C1 ANSI sequences
    def escape_ansi_control(error):
        ansi_escape = re.compile(r'''
            \x1B    # ESC
            [@-_]   # 7-bit C1 Fe
            [0-?]*  # Parameter bytes
            [ -/]*  # Intermediate bytes
            [@-~]   # Final byte
        ''', re.VERBOSE)
        sanitized = ""
        for line in error:
            sanitized += ansi_escape.sub('', line) + "\n"
        return sanitized

    path = os.path.normpath(DOCUMENTATION_DIR / notebook_name)
    nb = load_notebook(path)
    # Override kernel name since the notebook tests run in CRAB where ipykernel installs the kernel using the
    # default `python3` name
    nb.metadata.kernelspec['name'] = 'python3'

    all_cells = nb['cells']

    for cell_index in range(len(all_cells)):
        # Replace cells that contain set_trace with print
        source = all_cells[cell_index]['source']
        if 'set_trace()' in source:
            # Convert to dictionary to modify content
            content_to_modify = dict(all_cells[cell_index])
            content_to_modify['source'] = "#print('skip_cell_because_of_set_trace_function')"
            nb['cells'][cell_index] = nbformat.from_dict(content_to_modify)

    proc = ExecutePreprocessor(timeout=1200)
    proc.allow_errors = True
    util.do_with_retry(
        lambda: proc.preprocess(nb, {'metadata': {'path': os.path.dirname(path)}}),
        timeout_sec=1200)

    for cell in nb.cells:
        if 'outputs' in cell:
            for output in cell['outputs']:
                if output.output_type == 'error':
                    pytest.fail("\nNotebook '{}':\n{}".format(notebook_name, escape_ansi_control(output.traceback)))


def check_notebook_kernelspec(notebook_name):
    path = os.path.normpath(DOCUMENTATION_DIR / notebook_name)
    nb = load_notebook(path)
    kernelspec = nb.metadata.kernelspec
    if kernelspec['name'] != 'python311':
        pytest.fail(f'Notebook "{notebook_name}" has an unexpected kernel name: {kernelspec["name"]}')
    if kernelspec['language'] != 'python':
        pytest.fail(f'Notebook "{notebook_name}" has an unexpected kernel language: {kernelspec["language"]}')
    if kernelspec['display_name'] != 'Python 3.11':
        pytest.fail(f'Notebook "{notebook_name}" has an unexpected kernel display name: {kernelspec["display_name"]}')


def check_links(notebook_name):
    print(f'Checking links in Jupyter Notebook "{notebook_name}"')

    path = os.path.normpath(DOCUMENTATION_DIR / notebook_name)
    nb = load_notebook(path)

    all_cells = nb['cells']

    for cell_index in range(len(all_cells)):
        source = all_cells[cell_index]['source']

        if all_cells[cell_index]['cell_type'] != 'markdown':
            continue

        for match in re.finditer(r'(\[[^]]+]\((.*?\.ipynb)\))', source):
            link = match.group(1)
            ipynb_file = match.group(2).replace('%20', ' ')
            path_to_ipynb = os.path.join(os.path.dirname(path), ipynb_file)
            if not util.safe_exists(path_to_ipynb):
                pytest.fail(f'Link in cell {cell_index} of "{path}" has broken link:\n{link}')

        match = re.match(r'.*(seeq\.atlassian\.net/wiki).*', source)
        if match:
            pytest.fail(f'Old support website link found in cell {cell_index} of "{path}":\n{source}')


def cleanup_files(files_to_cleanup):
    for file_to_cleanup in files_to_cleanup:
        if util.safe_exists(file_to_cleanup):
            if os.path.isfile(file_to_cleanup):
                util.safe_remove(file_to_cleanup)
            else:
                util.safe_rmtree(file_to_cleanup)


def scan_for_notebooks(only_runnable=False):
    for root, dirs, files in util.safe_walk(DOCUMENTATION_DIR):
        for file in files:
            if not file.endswith(".ipynb"):
                continue

            if file.endswith("-checkpoint.ipynb"):
                continue

            if file == "Datasource Commands.ipynb" or file == "Workbook Commands.ipynb":
                continue

            if only_runnable:
                if "spy.jobs.ipynb" in file or "spy.login.ipynb" in file or "Advanced Scheduling" in root:
                    continue

            yield util.safe_relpath(os.path.join(root, file), DOCUMENTATION_DIR)


@pytest.mark.system
def test_notebook_links():
    notebooks = scan_for_notebooks()
    for notebook in notebooks:
        check_links(notebook)


@pytest.mark.system
def test_run_notebooks():
    notebooks = scan_for_notebooks(only_runnable=True)

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            _futures = list()
            for notebook in notebooks:
                _futures.append(executor.submit(run_notebook, notebook))

            concurrent.futures.wait(_futures)

            for _future in _futures:
                if _future.exception():
                    raise _future.exception()

    finally:
        cleanup_files([DOCUMENTATION_DIR / 'pickled_search.pkl',
                       DOCUMENTATION_DIR / 'pickled_pull.pkl',
                       DOCUMENTATION_DIR / '..' / 'My First Export'])


@pytest.mark.system
def test_compat_lines():
    compat_regex = re.compile(r'spy\.options\.compatibility\s*=\s*(\d+)')
    notebooks = scan_for_notebooks()
    for notebook in notebooks:
        if notebook in ['Command Reference.ipynb',
                        os.path.join('Advanced Scheduling', 'Email Notification Add-on Installer.ipynb'),
                        os.path.join('Advanced Scheduling', 'Email Notification Scheduler.ipynb'),
                        os.path.join('Advanced Scheduling', 'Email Notifier.ipynb'),
                        os.path.join('Advanced Scheduling', 'Email Unsubscriber.ipynb')]:
            # These notebooks are not meant to be run, so they don't need a compatibility line
            continue

        path = os.path.normpath(DOCUMENTATION_DIR / notebook)
        with util.safe_open(path, encoding='utf-8') as f:
            notebook_content = f.read()

        match = compat_regex.search(notebook_content)

        if not match:
            pytest.fail(f'Notebook "{notebook}" does not have a spy.options.compatibility line. Please add based on '
                        'what you see in the other documentation notebooks, it is usually right after the spy import '
                        'call.')

        current_version = int(match.group(1))

        spy_major_version, _ = spy.utils.get_spy_module_version_tuple()

        if current_version != spy_major_version:
            if os.environ.get('IS_CI', 0) == 1:
                pytest.fail(f'Notebook "{notebook}" has spy.options.compatibility line set to {current_version}, '
                            f'but should be {spy_major_version}. Run test_notebooks.test_compat_lines() locally, '
                            f'which will fix the files and leave them uncommitted in your repo. Then commit and push.')

            notebook_content = compat_regex.sub(f'spy.options.compatibility = {spy_major_version}', notebook_content)

            with util.safe_open(path, 'w', encoding='utf-8') as f:
                f.write(notebook_content)


@pytest.mark.system
def test_notebook_kernelspec():
    for notebook in scan_for_notebooks():
        check_notebook_kernelspec(notebook)
