import os

import pytest

from seeq.base import util


@pytest.mark.unit
def test_ensure_safe_file_calls():
    for root, folders, filenames in util.safe_walk(os.path.join(os.path.dirname(__file__), '..')):
        for filename in filenames:
            if filename in ['util.py', 'test_safe_calls.py']:
                continue

            if filename.endswith('.py'):
                with util.safe_open(os.path.join(root, filename), 'r', encoding='utf-8') as f:
                    contents = f.read()

                for bad in ['os.remove(', 'shutil.rmtree(', 'with open(', 'os.makedirs(', 'os.walk(', 'shutil.copy(',
                            'shutil.copytree(os.path.isfile(', 'os.path.isdir(', 'os.path.path(', 'os.path.exists(',
                            'glob.glob(']:
                    if bad in contents:
                        assert False, f'Found "{bad})" call in {filename}, use util.safe_XXXX equivalent'
