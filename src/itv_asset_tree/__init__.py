import versioneer

__version__ = versioneer.get_version()
__all__ = ["managers", "utilities"]

from . import _version
__version__ = _version.get_versions()['version']
