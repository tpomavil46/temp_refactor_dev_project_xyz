from seeq.base import util
from seeq.spy import _datalab
from seeq.spy.jobs._pull import pull
from seeq.spy.jobs._push import push
from seeq.spy.jobs._schedule import schedule, unschedule


def get_notebook_url(*args, **kwargs):
    util.deprecation_warning('Use of spy.jobs.get_notebook_url() deprecated, use spy.utils.get_notebook_url() instead')
    return _datalab.get_notebook_url(*args, **kwargs)


__all__ = ['push', 'pull', 'schedule', 'unschedule', 'get_notebook_url']
