from __future__ import annotations

import pandas as pd

reference_types = ['LiteralScalar', 'StoredSignal', 'StoredCondition']
calculated_types = ['CalculatedScalar', 'CalculatedSignal', 'CalculatedCondition']
unspecified_types = ['Scalar', 'Signal', 'Condition']
metric_types = ['Metric', 'ThresholdMetric']
data_types = calculated_types + reference_types
# types used when searching for items to insert by Name
supported_search_input_types = data_types + metric_types + unspecified_types + ['Asset']
# types we let get inserted into trees
supported_input_types = supported_search_input_types + ['Display']

dataframe_dtypes = {
    'ID': str,
    'Referenced ID': str,
    'Path': str,
    'Name': str,
    'Type': str,
    'Depth': int,
    'Description': str,
    'Formula': str,
    'Formula Parameters': (str, list, dict, pd.Series, pd.DataFrame),
    'Roll Up Statistic': str,
    'Roll Up Parameters': str,
    # Below are metric-specific columns
    'Aggregation Function': str,
    'Statistic': str,  # 'Statistic' is the friendly SPy input for 'Aggregation Function'
    'Bounding Condition': (str, dict),
    'Bounding Condition Maximum Duration': str,
    'Duration': str,
    'Number Format': str,
    'Measured Item': (str, dict),
    'Metric Neutral Color': str,
    'Period': str,
    'Process Type': str,
    'Thresholds': (dict, list),
    'Template ID': str
}
dataframe_columns = list(dataframe_dtypes.keys())

MAX_ERRORS_DISPLAYED = 3
MAX_FORMULA_DEPENDENCY_DEPTH = 1000
UNKNOWN = '____UNKNOWN_____'
