__version__ = "1.2.0"
__author__ = "osoken"
__description__ = "Python collections that are backended by sqlite3 DB and are compatible with the built-in collections"
__email__ = "osoken.devel@outlook.jp"
__package_name__ = "sqlitecollections"


from .base import PicklingStrategy
from .dict import Dict
from .factory import DictFactory, ListFactory, SetFactory
from .list import List, SortingStrategy
from .set import Set

__all__ = ["Dict", "List", "Set", "ListFactory", "DictFactory", "SetFactory", "PicklingStrategy", "SortingStrategy"]
