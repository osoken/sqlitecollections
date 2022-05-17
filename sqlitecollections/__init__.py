__version__ = "0.9.8"
__author__ = "osoken"
__description__ = "Python collections that are backended by sqlite3 DB and are compatible with the built-in collections"
__email__ = "osoken.devel@outlook.jp"
__package_name__ = "sqlitecollections"


from .dict import Dict
from .list import List
from .set import Set

__all__ = ["Dict", "List", "Set"]
