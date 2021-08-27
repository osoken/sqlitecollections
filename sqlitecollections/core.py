import collections
import sqlite3
import sys
from abc import ABCMeta, abstractmethod
from collections.abc import Hashable
from enum import Enum
from pickle import dumps, loads
from sqlite3.dbapi2 import Cursor
from tempfile import NamedTemporaryFile
from typing import Callable, Generic, Optional, Tuple, TypeVar, Union, cast
from uuid import uuid4

if sys.version_info >= (3, 9):
    from collections.abc import (
        Iterable,
        Iterator,
        Mapping,
        MutableMapping,
        MutableSet,
        Reversible,
    )
else:
    from typing import Iterable, Iterator, Mapping, MutableMapping, MutableSet
if sys.version_info >= (3, 8):
    from typing import Reversible


T = TypeVar("T")
KT = TypeVar("KT")
VT = TypeVar("VT")


class RebuildStrategy(Enum):
    CHECK_WITH_FIRST_ELEMENT = 1
    ALWAYS = 2
    SKIP = 3


class SqliteCollectionBase(Generic[T], metaclass=ABCMeta):
    def __init__(
        self,
        connection: Optional[Union[str, sqlite3.Connection]] = None,
        table_name: Optional[str] = None,
        serializer: Optional[Callable[[T], bytes]] = None,
        deserializer: Optional[Callable[[bytes], T]] = None,
        destruct_table_on_delete: bool = False,
        rebuild_strategy: RebuildStrategy = RebuildStrategy.CHECK_WITH_FIRST_ELEMENT,
        do_initialize: bool = True,
    ):
        super(SqliteCollectionBase, self).__init__()
        self._serializer = cast(Callable[[T], bytes], dumps) if serializer is None else serializer
        self._deserializer = cast(Callable[[bytes], T], loads) if deserializer is None else deserializer
        self._destruct_table_on_delete = destruct_table_on_delete
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
            f"{self.container_type_name}_{str(uuid4()).replace('-', '')}" if table_name is None else table_name
        )
        if do_initialize:
            self._initialize(commit=True)

    def __del__(self) -> None:
        if self.destruct_table_on_delete:
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
    def destruct_table_on_delete(self) -> bool:
        return self._destruct_table_on_delete

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
        return "".join(c for c in self._table_name if c.isalnum() or c == "_")

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


class _Dict(Generic[KT, VT], SqliteCollectionBase[VT], MutableMapping[KT, VT]):
    def __init__(
        self,
        connection: Optional[Union[str, sqlite3.Connection]] = None,
        table_name: Optional[str] = None,
        serializer: Optional[Callable[[VT], bytes]] = None,
        deserializer: Optional[Callable[[bytes], VT]] = None,
        key_serializer: Optional[Callable[[KT], bytes]] = None,
        key_deserializer: Optional[Callable[[bytes], KT]] = None,
        destruct_table_on_delete: bool = False,
        rebuild_strategy: RebuildStrategy = RebuildStrategy.CHECK_WITH_FIRST_ELEMENT,
        data: Optional[Mapping[KT, VT]] = None,
        **kwargs: VT,
    ) -> None:
        super(_Dict, self).__init__(
            connection=connection,
            table_name=table_name,
            serializer=serializer,
            deserializer=deserializer,
            destruct_table_on_delete=destruct_table_on_delete,
            rebuild_strategy=rebuild_strategy,
            do_initialize=False,
        )
        self._key_serializer = (
            cast(Callable[[KT], bytes], self.serializer) if key_serializer is None else key_serializer
        )
        self._key_deserializer = (
            cast(Callable[[bytes], KT], self.deserializer) if key_deserializer is None else key_deserializer
        )
        self._initialize(commit=True)
        self.update({} if data is None else data, **kwargs)

    @property
    def key_serializer(self) -> Callable[[KT], bytes]:
        return self._key_serializer

    @property
    def key_deserializer(self) -> Callable[[bytes], KT]:
        return self._key_deserializer

    @property
    def schema_version(self) -> str:
        return "0"

    def _do_create_table(self, commit: bool = False) -> None:
        cur = self.connection.cursor()
        cur.execute(
            f"CREATE TABLE {self.table_name} ("
            "serialized_key BLOB NOT NULL UNIQUE, "
            "serialized_value BLOB NOT NULL, "
            "item_order INTEGER PRIMARY KEY)"
        )
        if commit:
            self._connection.commit()

    def _rebuild_check_with_first_element(self) -> bool:
        cur = self.connection.cursor()
        cur.execute(f"SELECT serialized_key FROM {self.table_name} ORDER BY item_order LIMIT 1")
        res = cur.fetchone()
        if res is None:
            return False
        serialized_key = cast(bytes, res[0])
        key = self.deserialize_key(serialized_key)
        return serialized_key != self.serialize_key(key)

    def _do_rebuild(self, commit: bool = False) -> None:
        cur = self.connection.cursor()
        last_order = -1
        while last_order is not None:
            cur.execute(
                f"SELECT item_order FROM {self.table_name} WHERE item_order > ? ORDER BY item_order LIMIT 1",
                (last_order,),
            )
            res = cur.fetchone()
            if res is None:
                break
            i = res[0]
            cur.execute(
                f"SELECT serialized_key, serialized_value FROM {self.table_name} WHERE item_order=?",
                (i,),
            )
            serialized_key, serialized_value = cur.fetchone()
            cur.execute(
                f"UPDATE {self.table_name} SET serialized_key=?, serialized_value=? WHERE item_order=?",
                (
                    self.serialize_key(self.deserialize_key(serialized_key)),
                    self.serialize(self.deserialize(serialized_value)),
                    i,
                ),
            )
            last_order = i
        if commit:
            self.connection.commit()

    def _is_hashable(self, key: KT) -> bool:
        return isinstance(key, Hashable)

    def _delete_single_record_by_serialized_key(self, cur: sqlite3.Cursor, serialized_key: bytes) -> None:
        cur.execute(f"DELETE FROM {self.table_name} WHERE serialized_key=?", (serialized_key,))

    def _is_serialized_key_in(self, cur: sqlite3.Cursor, serialized_key: bytes) -> bool:
        cur.execute(f"SELECT 1 FROM {self.table_name} WHERE serialized_key=?", (serialized_key,))
        return len(list(cur)) > 0

    def _get_serialized_value_by_serialized_key(self, cur: sqlite3.Cursor, serialized_key: bytes) -> Union[None, bytes]:
        cur.execute(
            f"SELECT serialized_value FROM {self.table_name} WHERE serialized_key=?",
            (serialized_key,),
        )
        res = cur.fetchone()
        if res is None:
            return None
        return cast(bytes, res[0])

    def _get_next_order(self, cur: sqlite3.Cursor) -> int:
        cur.execute(f"SELECT MAX(item_order) FROM {self.table_name}")
        res = cur.fetchone()[0]
        if res is None:
            return 0
        return cast(int, res) + 1

    def _get_count(self, cur: sqlite3.Cursor) -> int:
        cur.execute(f"SELECT COUNT(*) FROM {self.table_name}")
        res = cur.fetchone()
        return cast(int, res[0])

    def _get_serialized_keys(self, cur: sqlite3.Cursor) -> Iterable[bytes]:
        cur.execute(f"SELECT serialized_key FROM {self.table_name} ORDER BY item_order")
        for res in cur:
            yield cast(bytes, res[0])

    def _upsert(
        self,
        cur: sqlite3.Cursor,
        serialized_key: bytes,
        serialized_value: bytes,
    ) -> None:
        if self._is_serialized_key_in(cur, serialized_key):
            cur.execute(
                f"UPDATE {self.table_name} SET serialized_value=? WHERE serialized_key=?",
                (serialized_value, serialized_key),
            )
        else:
            item_order = self._get_next_order(cur)
            cur.execute(
                f"INSERT INTO {self.table_name} (serialized_key, serialized_value, item_order) VALUES (?, ?, ?)",
                (serialized_key, serialized_value, item_order),
            )

    def _get_last_serialized_item(self, cur: sqlite3.Cursor) -> Tuple[bytes, bytes]:
        cur.execute(f"SELECT serialized_key, serialized_value FROM {self.table_name} ORDER BY item_order DESC LIMIT 1")
        return cast(Tuple[bytes, bytes], cur.fetchone())

    def serialize_key(self, key: KT) -> bytes:
        if not self._is_hashable(key):
            raise TypeError(f"unhashable type: '{type(key).__name__}'")
        return self.key_serializer(key)

    def deserialize_key(self, serialized_key: bytes) -> KT:
        return self.key_deserializer(serialized_key)

    def __delitem__(self, key: KT) -> None:
        serialized_key = self.serialize_key(key)
        cur = self.connection.cursor()
        if not self._is_serialized_key_in(cur, serialized_key):
            raise KeyError(key)
        self._delete_single_record_by_serialized_key(cur, serialized_key)
        self.connection.commit()

    def __getitem__(self, key: KT) -> VT:
        serialized_key = self.serialize_key(key)
        cur = self.connection.cursor()
        serialized_value = self._get_serialized_value_by_serialized_key(cur, serialized_key)
        if serialized_value is None:
            raise KeyError(key)
        return self.deserialize(serialized_value)

    def __iter__(self) -> Iterator[KT]:
        cur = self.connection.cursor()
        for serialized_key in self._get_serialized_keys(cur):
            yield self.deserialize_key(serialized_key)

    def __len__(self) -> int:
        cur = self.connection.cursor()
        return self._get_count(cur)

    def __setitem__(self, key: KT, value: VT) -> None:
        serialized_key = self.serialize_key(key)
        cur = self.connection.cursor()
        serialized_value = self.serializer(value)
        self._upsert(cur, serialized_key, serialized_value)
        self.connection.commit()

    def copy(self) -> "Dict[KT, VT]":
        raise NotImplementedError

    @classmethod
    def fromkeys(cls, iterable: Iterable[KT], value: Optional[VT]) -> "Dict[KT, VT]":
        raise NotImplementedError

    def popitem(self) -> Tuple[KT, VT]:
        cur = self.connection.cursor()
        serialized_item = self._get_last_serialized_item(cur)
        if serialized_item is None:
            raise KeyError("popitem(): dictionary is empty")
        self._delete_single_record_by_serialized_key(cur, serialized_item[0])
        self.connection.commit()
        return (
            self.deserialize_key(serialized_item[0]),
            self.deserialize(serialized_item[1]),
        )


if sys.version_info >= (3, 8):

    class _ReversibleDict(_Dict[KT, VT], Reversible[KT]):
        def _get_reversed_serialized_keys(self, cur: sqlite3.Cursor) -> Iterable[bytes]:
            cur.execute(f"SELECT serialized_key FROM {self.table_name} ORDER BY item_order DESC")
            for res in cur:
                yield cast(bytes, res[0])

        def reversed_keys(self) -> Iterator[KT]:
            cur = self.connection.cursor()
            for serialized_key in self._get_reversed_serialized_keys(cur):
                yield self.deserialize_key(serialized_key)

        def __reversed__(self) -> Iterator[KT]:
            return self.reversed_keys()


if sys.version_info >= (3, 9):

    class Dict(_ReversibleDict[KT, VT]):
        def __or__(self, other: Mapping[KT, VT]) -> "Dict[KT, VT]":
            tmp = Dict(
                connection=self.connection,
                serializer=self.serializer,
                deserializer=self.deserializer,
                key_serializer=self.key_serializer,
                key_deserializer=self.key_deserializer,
                destruct_table_on_delete=self.destruct_table_on_delete,
                data=self,
            )
            tmp |= other
            return tmp

        def __ior__(self, other: Mapping[KT, VT]) -> "Dict[KT, VT]":
            self.update(other)
            return self


elif sys.version_info >= (3, 8):

    class Dict(_ReversibleDict[KT, VT]):
        ...


else:

    class Dict(_Dict[KT, VT]):
        ...


class Set(SqliteCollectionBase[T], MutableSet[T]):
    def __init__(
        self,
        connection: Optional[Union[str, sqlite3.Connection]] = None,
        table_name: Optional[str] = None,
        serializer: Optional[Callable[[T], bytes]] = None,
        deserializer: Optional[Callable[[bytes], T]] = None,
        destruct_table_on_delete: bool = False,
        rebuild_strategy: RebuildStrategy = RebuildStrategy.CHECK_WITH_FIRST_ELEMENT,
        data: Optional[Iterable[T]] = None,
    ) -> None:
        super(Set, self).__init__(
            connection=connection,
            table_name=table_name,
            serializer=serializer,
            deserializer=deserializer,
            destruct_table_on_delete=destruct_table_on_delete,
            rebuild_strategy=rebuild_strategy,
            do_initialize=True,
        )
        if data is not None:
            self.update(data)

    def __contains__(self, value: object) -> bool:
        cur = self.connection.cursor()
        serialized_value = self.serialize(cast(T, value))
        return self._is_serialized_value_in(cur, serialized_value)

    def __iter__(self) -> Iterator[T]:
        cur = self.connection.cursor()
        for d in self._get_serialized_values(cur):
            yield self.deserialize(d)

    def _get_serialized_values(self, cur: sqlite3.Cursor) -> Iterable[bytes]:
        cur.execute(f"SELECT serialized_value FROM {self.table_name}")
        for d in cur:
            yield cast(bytes, d[0])

    def __len__(self) -> int:
        cur = self.connection.cursor()
        return self._get_count(cur)

    def _do_create_table(self, commit: bool = False) -> None:
        cur = self.connection.cursor()
        cur.execute(f"CREATE TABLE {self.table_name} (serialized_value BLOB PRIMARY KEY)")
        if commit:
            self.connection.commit()

    def _do_rebuild(self, commit: bool = False) -> None:
        cur = self.connection.cursor()
        backup_table_name = f'bk_{self.container_type_name}_{str(uuid4()).replace("-", "")}'
        cur.execute(f"CREATE TABLE {backup_table_name} AS SELECT * FROM {self.table_name}")
        cur.execute(f"DELETE FROM {self.table_name}")
        iter_old_records = self.connection.cursor()
        iter_old_records.execute(f"SELECT serialized_value FROM {backup_table_name}")
        delete_old_records = self.connection.cursor()
        insert_new_records = self.connection.cursor()
        for d in iter_old_records:
            insert_new_records.execute(
                f"INSERT INTO {self.table_name} (serialized_value) VALUES (?)",
                (self.serialize(self.deserialize(d[0])),),
            )
            delete_old_records.execute(f"DELETE FROM {backup_table_name} WHERE serialized_value = ?", d)
        cur.execute(f"DROP TABLE {backup_table_name}")
        if commit:
            self.connection.commit()

    def _rebuild_check_with_first_element(self) -> bool:
        cur = self.connection.cursor()
        cur.execute(f"SELECT serialized_value FROM {self.table_name} LIMIT 1")
        res = cur.fetchone()
        if res is None:
            return False
        serialized_value = cast(bytes, res[0])
        value = self.deserialize(serialized_value)
        return serialized_value != self.serialize(value)

    def update(self, *others: Iterable[T]) -> None:
        for container in others:
            for d in container:
                self.add(d)

    def serialize(self, value: T) -> bytes:
        if not self._is_hashable(value):
            raise TypeError(f"unhashable type: '{type(value).__name__}'")
        return self.serializer(value)

    def add(self, value: T) -> None:
        serialized_value = self.serialize(value)
        cur = self.connection.cursor()
        self._upsert(cur, serialized_value)
        self.connection.commit()

    def _upsert(self, cur: sqlite3.Cursor, serialized_value: bytes) -> None:
        if not self._is_serialized_value_in(cur, serialized_value):
            cur.execute(
                f"INSERT INTO {self.table_name} (serialized_value) VALUES (?)",
                (serialized_value,),
            )

    def _is_serialized_value_in(self, cur: sqlite3.Cursor, serialized_value: bytes) -> bool:
        cur.execute(f"SELECT 1 FROM {self.table_name} WHERE serialized_value=?", (serialized_value,))
        return len(list(cur)) > 0

    def discard(self, value: T) -> None:
        return super().discard(value)

    @property
    def schema_version(self) -> str:
        return "0"

    def _is_hashable(self, value: T) -> bool:
        return isinstance(value, Hashable)

    def _get_count(self, cur: sqlite3.Cursor) -> int:
        cur.execute(f"SELECT COUNT(*) FROM {self.table_name}")
        res = cur.fetchone()
        return cast(int, res[0])

    def issubset(self, other: Iterable[T]) -> bool:
        return len(self) == len(self.intersection(other))

    def _intersection_single(self, other: Iterable[T]) -> "Set[T]":
        res = Set[T](
            connection=self.connection,
            serializer=self.serializer,
            deserializer=self.deserializer,
            destruct_table_on_delete=True,
            rebuild_strategy=RebuildStrategy.SKIP,
        )
        for d in other:
            if d in self:
                res.add(d)
        return res

    def intersection(self, *others: Iterable[T]) -> "Set[T]":
        res = Set[T](
            connection=self.connection,
            serializer=self.serializer,
            deserializer=self.deserializer,
            rebuild_strategy=RebuildStrategy.SKIP,
            destruct_table_on_delete=True,
            data=self,
        )
        for other in others:
            res.intersection_update(other)
        return res

    def intersection_update(self, *others: Iterable[T]) -> None:
        for other in others:
            self._intersection_update_single(other)
        self.connection.commit()

    def _intersection_update_single(self, other: Iterable[T]) -> None:
        buf = Set[T](
            connection=self.connection,
            serializer=self.serializer,
            deserializer=self.deserializer,
            destruct_table_on_delete=True,
            rebuild_strategy=RebuildStrategy.SKIP,
            data=other,
        )
        cur = self.connection.cursor()
        cur.execute(
            f"DELETE FROM {self.table_name} WHERE NOT EXISTS (SELECT serialized_value FROM {buf.table_name} WHERE {self.table_name}.serialized_value = {buf.table_name}.serialized_value)"
        )

    def issuperset(self, other: Iterable[T]) -> bool:
        for d in other:
            if not d in self:
                return False
        return True

    def union(self, *others: Iterable[T]) -> "Set[T]":
        res = Set[T](
            connection=self.connection,
            serializer=self.serializer,
            deserializer=self.deserializer,
            rebuild_strategy=RebuildStrategy.SKIP,
            destruct_table_on_delete=True,
            data=self,
        )
        for other in others:
            res.union_update(other)
        return res

    def union_update(self, *others: Iterable[T]) -> None:
        for other in others:
            self._union_update_single(other)

    def _union_update_single(self, other: Iterable[T]) -> None:
        for d in other:
            self.add(d)
