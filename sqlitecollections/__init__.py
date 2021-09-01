__version__ = "0.5.0"
__author__ = "osoken"
__description__ = "sqlitecollections supports some collections with cache backended by sqlite3 daabases"
__long_description__ = __description__
__email__ = "osoken.devel@outlook.jp"
__package_name__ = "sqlitecollections"


from .base import RebuildStrategy
from .dict import Dict
from .list import List
from .set import Set

__all__ = ["Dict", "List", "Set", "RebuildStrategy"]
