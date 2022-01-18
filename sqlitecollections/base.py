import sqlite3
import sys
from abc import ABCMeta, abstractmethod
from collections.abc import Hashable
from enum import Enum
from pickle import dumps, loads
from tempfile import NamedTemporaryFile
from types import TracebackType
from typing import Callable, Generic, Optional, Type, TypeVar, Union, cast
from uuid import uuid4

from .logger import logger

if sys.version_info >= (3, 9):
    from contextlib import AbstractContextManager

    ContextManager = AbstractContextManager
else:
    from typing import ContextManager

T = TypeVar("T")
KT = TypeVar("KT")
VT = TypeVar("VT")
_T = TypeVar("_T")
_S = TypeVar("_S")


class RebuildStrategy(Enum):
    CHECK_WITH_FIRST_ELEMENT = 1
    ALWAYS = 2
    SKIP = 3


def sanitize_table_name(table_name: str) -> str:
    ret = "".join(c for c in table_name if c.isalnum() or c == "_")
    if ret != table_name:
        logger.warning(f"The table name is changed to {ret} due to illegal characters")
    return ret


def create_random_name(prefix: str) -> str:
    return f"{prefix}_{str(uuid4()).replace('-', '')}"


def is_hashable(x: object) -> bool:
    return isinstance(x, Hashable)


class TemporaryTableContext(ContextManager[str]):
    def __init__(self, cur: sqlite3.Cursor, reference_table_name: str):
        self._cursor = cur
        self._reference_table_name = reference_table_name
        self._table_name = create_random_name("tmp")

    def __enter__(self) -> str:
        self._cursor.execute(
            f"CREATE TABLE {self._table_name} AS SELECT * FROM {self._reference_table_name} WHERE 0 = 1"
        )
        return self._table_name

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        excinst: Optional[BaseException],
        exctb: Optional[TracebackType],
    ) -> None:
        self._cursor.execute(f"DROP TABLE {self._table_name}")
        return None


class _SqliteCollectionBaseDatabaseDriver(metaclass=ABCMeta):
    @classmethod
    def initialize_metadata_table(cls, cur: sqlite3.Cursor) -> None:
        if not cls.is_metadata_table_initialized(cur):
            cls.do_initialize_metadata_table(cur)

    @classmethod
    def is_metadata_table_initialized(cls, cur: sqlite3.Cursor) -> bool:
        try:
            cur.execute("SELECT 1 FROM metadata LIMIT 1")
            _ = list(cur)
            return True
        except sqlite3.OperationalError as _:
            pass
        return False

    @classmethod
    def do_initialize_metadata_table(cls, cur: sqlite3.Cursor) -> None:
        cur.execute(
            """
            CREATE TABLE metadata (
                table_name TEXT PRIMARY KEY,
                schema_version TEXT NOT NULL,
                container_type TEXT NOT NULL,
                UNIQUE (table_name, container_type)
            )
            """
        )

    @classmethod
    def initialize_table(
        cls, table_name: str, container_type_name: str, schema_version: str, cur: sqlite3.Cursor
    ) -> None:
        if not cls.is_table_initialized(table_name, container_type_name, schema_version, cur):
            cls.do_create_table(table_name, container_type_name, schema_version, cur)
            cls.do_tidy_table_metadata(table_name, container_type_name, schema_version, cur)

    @classmethod
    def is_table_initialized(
        self, table_name: str, container_type_name: str, schema_version: str, cur: sqlite3.Cursor
    ) -> bool:
        try:
            cur.execute(
                "SELECT schema_version FROM metadata WHERE table_name=? AND container_type=?",
                (table_name, container_type_name),
            )
            buf = cur.fetchone()
            if buf is None:
                return False
            version = buf[0]
            if version != schema_version:
                return False
            cur.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
            _ = list(cur)
            return True
        except sqlite3.OperationalError as _:
            pass
        return False

    @classmethod
    def do_tidy_table_metadata(
        cls, table_name: str, container_type_name: str, schema_version: str, cur: sqlite3.Cursor
    ) -> None:
        cur.execute(
            "INSERT INTO metadata (table_name, schema_version, container_type) VALUES (?, ?, ?)",
            (table_name, schema_version, container_type_name),
        )

    @classmethod
    @abstractmethod
    def do_create_table(
        cls, table_name: str, container_type_name: str, schema_version: str, cur: sqlite3.Cursor
    ) -> None:
        ...

    @classmethod
    def drop_table(cls, table_name: str, container_type_name: str, cur: sqlite3.Cursor) -> None:
        cur.execute(
            "DELETE FROM metadata WHERE table_name=? AND container_type=?",
            (table_name, container_type_name),
        )
        cur.execute(f"DROP TABLE {table_name}")

    @classmethod
    def alter_table_name(cls, table_name: str, new_table_name: str, cur: sqlite3.Cursor) -> None:
        cur.execute("UPDATE metadata SET table_name=? WHERE table_name=?", (new_table_name, table_name))
        cur.execute(f"ALTER TABLE {table_name} RENAME TO {new_table_name}")


class SqliteCollectionBase(Generic[T], metaclass=ABCMeta):
    _driver_class = _SqliteCollectionBaseDatabaseDriver

    @classmethod
    def _default_serializer(cls, x: T) -> bytes:
        return dumps(x, protocol=3)

    @classmethod
    def _default_deserializer(cls, x: bytes) -> T:
        return cast(T, loads(x))

    def __init__(
        self,
        connection: Optional[Union[str, sqlite3.Connection]] = None,
        table_name: Optional[str] = None,
        serializer: Optional[Callable[[T], bytes]] = None,
        deserializer: Optional[Callable[[bytes], T]] = None,
        persist: bool = True,
        rebuild_strategy: RebuildStrategy = RebuildStrategy.CHECK_WITH_FIRST_ELEMENT,
    ):
        super(SqliteCollectionBase, self).__init__()
        self._serializer = self._default_serializer if serializer is None else serializer
        self._deserializer = self._default_deserializer if deserializer is None else deserializer
        self._persist = persist
        if connection is None:
            self._connection = sqlite3.connect(NamedTemporaryFile().name)
        elif isinstance(connection, str):
            self._connection = sqlite3.connect(connection)
        elif isinstance(connection, sqlite3.Connection):
            self._connection = connection
        else:
            raise TypeError(
                f"connection argument must be None or a string or a sqlite3.Connection, not '{type(connection)}'"
            )
        self._table_name = (
            sanitize_table_name(create_random_name(self.container_type_name))
            if table_name is None
            else sanitize_table_name(table_name)
        )
        self._initialize(rebuild_strategy=rebuild_strategy)

    def __del__(self) -> None:
        if not self.persist:
            cur = self.connection.cursor()
            self._driver_class.drop_table(self.table_name, self.container_type_name, cur)
            self.connection.commit()

    def _initialize(self, rebuild_strategy: RebuildStrategy) -> None:
        cur = self.connection.cursor()
        self._driver_class.initialize_metadata_table(cur)
        self._driver_class.initialize_table(self.table_name, self.container_type_name, self.schema_version, cur)
        if self._should_rebuild(rebuild_strategy):
            self._do_rebuild()
        self.connection.commit()

    def _should_rebuild(self, rebuild_strategy: RebuildStrategy) -> bool:
        if rebuild_strategy == RebuildStrategy.ALWAYS:
            return True
        if rebuild_strategy == RebuildStrategy.SKIP:
            return False
        return self._rebuild_check_with_first_element()

    @abstractmethod
    def _rebuild_check_with_first_element(self) -> bool:
        ...

    @abstractmethod
    def _do_rebuild(self) -> None:
        ...

    @property
    def persist(self) -> bool:
        return self._persist

    def set_persist(self, persist: bool) -> None:
        self._persist = persist

    @property
    def serializer(self) -> Callable[[T], bytes]:
        return self._serializer

    def serialize(self, x: T) -> bytes:
        return self.serializer(x)

    @property
    def deserializer(self) -> Callable[[bytes], T]:
        return self._deserializer

    def deserialize(self, blob: bytes) -> T:
        return self.deserializer(blob)

    @property
    def table_name(self) -> str:
        return self._table_name

    @table_name.setter
    def table_name(self, table_name: str) -> None:
        cur = self.connection.cursor()
        new_table_name = sanitize_table_name(table_name)
        try:
            self._driver_class.alter_table_name(self.table_name, new_table_name, cur)
        except sqlite3.IntegrityError as e:
            raise ValueError(table_name)
        self._table_name = new_table_name

    @property
    def connection(self) -> sqlite3.Connection:
        return self._connection

    @property
    def container_type_name(self) -> str:
        return self.__class__.__name__

    @property
    @abstractmethod
    def schema_version(self) -> str:
        ...
