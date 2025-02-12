import os

import pandas as pd
import pytest


@pytest.mark.unit
def test_pickle_from_python37_and_spy_182_37():
    test_dir = os.path.dirname(__file__)

    # Make sure we can load old pickled DataFrames. Users will save search and pull DataFrames because they can be
    # expensive to search/pull again from Seeq Server. So ideally we don't unintentionally break the pickling.
    # See CRAB-33735 for an example.

    search_df = pd.read_pickle(os.path.join(test_dir, 'spy.search.py37.R55.0.1.182.37.pickle'))

    assert len(search_df) > 0
    assert search_df.status.message == 'Query successful'
    assert len(search_df.status.df) > 0

    pull_df = pd.read_pickle(os.path.join(test_dir, 'spy.pull.py37.R55.0.1.182.37.pickle'))
    assert len(pull_df) > 0
    assert pull_df.status.message.startswith('Pull successful')
    assert len(pull_df.status.df) > 0
