from seeq.spy._dependencies import Dependencies
from seeq.spy._errors import SPyDependencyNotFound
from seeq.spy.widgets._ipy_utils import WidgetLogHandler
from seeq.spy.widgets._widgets import SeeqItemSelect, LogWindowWidget


class DataLabEnvMgr:
    def __new__(cls, *args, **kwargs):
        raise SPyDependencyNotFound(f'`seeq-data-lab-env-mgr` is not installed. Please use `pip install seeq-spy[widgets]` '
                                    f'to use this feature.')


try:
    from seeq.data_lab_env_mgr import DataLabEnvMgr
except ImportError:
    pass

__all__ = ['LogWindowWidget', 'SeeqItemSelect', 'WidgetLogHandler', 'DataLabEnvMgr']
