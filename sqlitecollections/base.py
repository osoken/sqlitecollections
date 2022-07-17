import sqlite3
import sys
import warnings
from abc import ABCMeta, abstractmethod
from collections.abc import Hashable
from enum import Enum
from pickle import dumps, loads
from tempfile import NamedTemporaryFile
from types import TracebackType
from typing import IO, Generic, Optional, Tuple, Type, TypeVar, Union, cast, overload
from uuid import uuid4

from .logger import logger

if sys.version_info >= (3, 9):
    from contextlib import AbstractContextManager

    ContextManager = AbstractContextManager
    from collections.abc import Callable, Collection, Iterable, Iterator
else:
    from typing import Callable, Collection, ContextManager, Iterable, Iterator

T = TypeVar("T")
KT = TypeVar("KT")
VT = TypeVar("VT")
_T = TypeVar("_T")
_S = TypeVar("_S")


def sanitize_table_name(table_name: str, prefix: str) -> str:
    ret = "".join(c for c in table_name if c.isalnum() or c == "_")
    if len(ret) == 0:
        ret = create_random_name(prefix)
    if ret[0].isnumeric():
        ret = f"{prefix}_{ret}"
    if ret != table_name:
        logger.warning(f"The table name is changed to {ret} due to illegal characters")
    return ret


def create_random_name(prefix: str) -> str:
    return f"{prefix}_{str(uuid4()).replace('-', '')}"


def is_hashable(x: object) -> bool:
    return isinstance(x, Hashable)


def create_temporary_db_file() -> IO[bytes]:
    return NamedTemporaryFile(prefix="sc_", suffix=".db")


def create_tempfile_connection() -> sqlite3.Connection:
    return sqlite3.connect(create_temporary_db_file().name)


def tidy_connection(connection: Optional[Union[str, sqlite3.Connection]] = None) -> sqlite3.Connection:
    if connection is None:
        return create_tempfile_connection()
    elif isinstance(connection, str):
        return sqlite3.connect(connection)
    elif isinstance(connection, sqlite3.Connection):
        return connection
    else:
        raise TypeError(
            f"connection argument must be None or a string or a sqlite3.Connection, not '{type(connection)}'"
        )


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
    if sys.version_info >= (3, 9):

        @classmethod
        @property
        def schema_version(cls) -> str:
            return ""

    else:
        schema_version = ""

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
        cls, table_name: str, container_type: str, reference_table_name: Union[None, str], cur: sqlite3.Cursor
    ) -> None:
        if not cls.is_table_initialized(table_name, container_type, cur):
            if reference_table_name is None:
                cls.do_create_table(table_name, container_type, cur)
            else:
                cls.do_create_table_with_reference_table(table_name, reference_table_name, cur)
            cls.do_tidy_table_metadata(table_name, container_type, cur)

    @classmethod
    def is_table_initialized(cls, table_name: str, container_type_name: str, cur: sqlite3.Cursor) -> bool:
        try:
            cur.execute(
                "SELECT schema_version FROM metadata WHERE table_name=? AND container_type=?",
                (table_name, container_type_name),
            )
            buf = cur.fetchone()
            if buf is None:
                return False
            version = buf[0]
            if version != cls.schema_version:
                return False
            cur.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
            _ = list(cur)
            return True
        except sqlite3.OperationalError as _:
            pass
        return False

    @classmethod
    def do_tidy_table_metadata(cls, table_name: str, container_type_name: str, cur: sqlite3.Cursor) -> None:
        cur.execute(
            "INSERT INTO metadata (table_name, schema_version, container_type) VALUES (?, ?, ?)",
            (table_name, cls.schema_version, container_type_name),
        )

    @classmethod
    @abstractmethod
    def do_create_table(cls, table_name: str, container_type_name: str, cur: sqlite3.Cursor) -> None:
        ...

    @classmethod
    def do_create_table_with_reference_table(
        cls, table_name: str, reference_table_name: str, cur: sqlite3.Cursor
    ) -> None:
        cur.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {reference_table_name}")

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


class MetadataItem(Hashable):
    def __init__(self, table_name: str, schema_version: str, container_type: str):
        self._table_name = table_name
        self._schema_version = schema_version
        self._container_type = container_type

    @property
    def table_name(self) -> str:
        return self._table_name

    @property
    def schema_version(self) -> str:
        return self._schema_version

    @property
    def container_type(self) -> str:
        return self._container_type

    def __eq__(self, __o: object) -> bool:
        if isinstance(__o, MetadataItem):
            return (
                self.table_name == __o.table_name
                and self.container_type == __o.container_type
                and self.schema_version == __o.schema_version
            )
        return False

    def __hash__(self) -> int:
        return hash((self.table_name, self.schema_version, self.container_type))


class MetadataDatabaseDriver:
    @classmethod
    def get_count(cls, cur: sqlite3.Cursor) -> int:
        try:
            cur.execute(f"SELECT COUNT(1) FROM metadata")
            res = cur.fetchone()
            return cast(int, res[0])
        except sqlite3.OperationalError:
            return 0

    @classmethod
    def is_metadata_in(cls, cur: sqlite3.Cursor, metadata: MetadataItem) -> bool:
        try:
            cur.execute(
                "SELECT COUNT(1) FROM metadata WHERE table_name=? AND schema_version=? AND container_type=?",
                (metadata.table_name, metadata.schema_version, metadata.container_type),
            )
            return cast(int, cur.fetchone()[0]) == 1
        except sqlite3.OperationalError:
            return False

    @classmethod
    def get_metadata(cls, cur: sqlite3.Cursor) -> Iterable[Tuple[str, str, str]]:
        try:
            cur.execute("SELECT table_name, schema_version, container_type FROM metadata")
            yield from cur
        except sqlite3.OperationalError:
            yield from tuple()


class MetadataReader(Collection[MetadataItem]):
    def __init__(self, connection: Union[str, sqlite3.Connection]):
        self._connection = tidy_connection(connection)

    def __len__(self) -> int:
        return MetadataDatabaseDriver.get_count(self._connection.cursor())

    def __contains__(self, __x: object) -> bool:
        if isinstance(__x, MetadataItem):
            return MetadataDatabaseDriver.is_metadata_in(self._connection.cursor(), __x)
        return False

    def __iter__(self) -> Iterator[MetadataItem]:
        for d in MetadataDatabaseDriver.get_metadata(self._connection.cursor()):
            yield MetadataItem(table_name=d[0], schema_version=d[1], container_type=d[2])


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
        reference_table_name: Optional[str] = None,
    ):
        super(SqliteCollectionBase, self).__init__()
        self._serializer = self._default_serializer if serializer is None else serializer
        self._deserializer = self._default_deserializer if deserializer is None else deserializer
        self._connection = tidy_connection(connection)
        self._persist = persist
        self._table_name = (
            sanitize_table_name(create_random_name(self.container_type_name), self.container_type_name)
            if table_name is None
            else sanitize_table_name(table_name, self.container_type_name)
        )
        self._initialize(reference_table_name=reference_table_name)

    def __del__(self) -> None:
        if hasattr(self, "persist") and not self.persist:
            cur = self.connection.cursor()
            self._driver_class.drop_table(self.table_name, self.container_type_name, cur)
            self.connection.commit()

    def _initialize(self, reference_table_name: Optional[str] = None) -> None:
        cur = self.connection.cursor()
        self._driver_class.initialize_metadata_table(cur)
        self._driver_class.initialize_table(self.table_name, self.container_type_name, reference_table_name, cur)
        self.connection.commit()

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
        new_table_name = sanitize_table_name(table_name, self.container_type_name)
        if self._table_name != new_table_name:
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
