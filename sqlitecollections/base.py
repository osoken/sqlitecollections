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
    return "".join(c for c in table_name if c.isalnum() or c == "_")


def create_random_name(suffix: str) -> str:
    return f"{suffix}_{str(uuid4()).replace('-', '')}"


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


class SqliteCollectionBase(Generic[T], metaclass=ABCMeta):
    def __init__(
        self,
        connection: Optional[Union[str, sqlite3.Connection]] = None,
        table_name: Optional[str] = None,
        serializer: Optional[Callable[[T], bytes]] = None,
        deserializer: Optional[Callable[[bytes], T]] = None,
        persist: bool = True,
        rebuild_strategy: RebuildStrategy = RebuildStrategy.CHECK_WITH_FIRST_ELEMENT,
        do_initialize: bool = True,
    ):
        super(SqliteCollectionBase, self).__init__()
        self._serializer = cast(Callable[[T], bytes], dumps) if serializer is None else serializer
        self._deserializer = cast(Callable[[bytes], T], loads) if deserializer is None else deserializer
        self._persist = persist
        self._rebuild_strategy = rebuild_strategy
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
        if do_initialize:
            self._initialize(commit=True)

    def __del__(self) -> None:
        if not self.persist:
            cur = self.connection.cursor()
            cur.execute(
                "DELETE FROM metadata WHERE table_name=? AND container_type=?",
                (self.table_name, self.container_type_name),
            )
            cur.execute(f"DROP TABLE {self.table_name}")
            self.connection.commit()

    def _initialize(self, commit: bool = False) -> None:
        self._initialize_metadata_table(commit=commit)
        self._initialize_table(commit=commit)
        if self._should_rebuild():
            self._do_rebuild(commit=commit)

    def _should_rebuild(self) -> bool:
        if self.rebuild_strategy == RebuildStrategy.ALWAYS:
            return True
        if self.rebuild_strategy == RebuildStrategy.SKIP:
            return False
        return self._rebuild_check_with_first_element()

    @abstractmethod
    def _rebuild_check_with_first_element(self) -> bool:
        ...

    @abstractmethod
    def _do_rebuild(self, commit: bool = False) -> None:
        ...

    @property
    def rebuild_strategy(self) -> RebuildStrategy:
        return self._rebuild_strategy

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

    def _is_table_initialized(self) -> bool:
        try:
            cur = self._connection.cursor()
            cur.execute(
                "SELECT schema_version FROM metadata WHERE table_name=? AND container_type=?",
                (self.table_name, self.container_type_name),
            )
            buf = cur.fetchone()
            if buf is None:
                return False
            version = buf[0]
            if version != self.schema_version:
                return False
            cur.execute(f"SELECT 1 FROM {self.table_name}")
            return True
        except sqlite3.OperationalError as _:
            pass
        return False

    def _do_tidy_table_metadata(self, commit: bool = False) -> None:
        cur = self.connection.cursor()
        cur.execute(
            "INSERT INTO metadata (table_name, schema_version, container_type) VALUES (?, ?, ?)",
            (self.table_name, self.schema_version, self.container_type_name),
        )
        if commit:
            self.connection.commit()

    def _initialize_table(self, commit: bool = False) -> None:
        if not self._is_table_initialized():
            self._do_create_table()
            self._do_tidy_table_metadata()
        if commit:
            self.connection.commit()

    @abstractmethod
    def _do_create_table(self, commit: bool = False) -> None:
        ...

    def _is_metadata_table_initialized(self) -> bool:
        try:
            cur = self.connection.cursor()
            cur.execute("SELECT 1 FROM metadata")
            return True
        except sqlite3.OperationalError as _:
            pass
        return False

    def _do_initialize_metadata_table(self, commit: bool = False) -> None:
        cur = self.connection.cursor()
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
        if commit:
            self.connection.commit()

    def _initialize_metadata_table(self, commit: bool = False) -> None:
        if not self._is_metadata_table_initialized():
            self._do_initialize_metadata_table(commit)
