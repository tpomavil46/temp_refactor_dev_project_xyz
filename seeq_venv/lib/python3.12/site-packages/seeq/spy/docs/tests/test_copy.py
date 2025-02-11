import os
import tempfile

import pytest

from seeq.base import util
from seeq.spy.docs import _copy


@pytest.mark.unit
def test_copy():
    with tempfile.TemporaryDirectory() as temp_folder:
        long_folder_name = os.path.join(temp_folder, 'long_' * 10)
        _copy.copy(long_folder_name)

        assert util.safe_exists(long_folder_name)

        with pytest.raises(RuntimeError):
            _copy.copy(long_folder_name)

        assert util.safe_exists(long_folder_name)

        _copy.copy(long_folder_name, overwrite=True, advanced=True)

        assert util.safe_exists(long_folder_name)
        assert util.safe_exists(os.path.join(long_folder_name, 'spy.workbooks.ipynb'))
        assert util.safe_exists(os.path.join(long_folder_name, 'Asset Trees 3 - Report and Dashboard Templates.ipynb'))

        util.safe_rmtree(long_folder_name)
