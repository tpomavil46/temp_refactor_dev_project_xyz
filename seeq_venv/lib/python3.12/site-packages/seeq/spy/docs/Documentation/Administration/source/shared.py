from __future__ import annotations

"""
This module houses all the shared functionality in the build lib that is used among multiple modules. Try to use these
functions wherever possible.
"""
from datetime import timedelta, tzinfo

#
# See https://stackoverflow.com/a/2331635/2452569 for why this is necessary
#
ZERO = timedelta(0)


class UTC(tzinfo):
    """UTC"""

    def utcoffset(self, dt):
        return ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return ZERO


utc = UTC()


# See http://code.activestate.com/recipes/52308-the-simple-but-handy-collector-of-a-bunch-of-named
class Bunch:

    def __init__(self, **kwds):
        self.__dict__.update(kwds)
