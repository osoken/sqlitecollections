import sqlite3
import sys
import warnings
from itertools import chain
from typing import Callable, Generic, Optional, Tuple, Union, cast, overload

if sys.version_info >= (3, 9):
    from collections.abc import Iterable, Iterator, Mapping, MutableMapping, Reversible
else:
    from typing import Iterable, Iterator, Mapping, MutableMapping
if sys.version_info >= (3, 8):
    from typing import Reversible

from . import RebuildStrategy
from .base import (
    KT,
    VT,
    SqliteCollectionBase,
    T,
    _SqliteCollectionBaseDatabaseDriver,
    is_hashable,
)


class _DictDatabaseDriver(_SqliteCollectionBaseDatabaseDriver):
    @classmethod
    def do_create_table(
        cls, table_name: str, container_type_nam: str, schema_version: str, cur: sqlite3.Cursor
    ) -> None:
        cur.execute(
            f"CREATE TABLE {table_name} ("
            "serialized_key BLOB NOT NULL UNIQUE, "
            "serialized_value BLOB NOT NULL, "
            "item_order INTEGER PRIMARY KEY)"
        )

    @classmethod
    def delete_single_record_by_serialized_key(
        cls, table_name: str, cur: sqlite3.Cursor, serialized_key: bytes
    ) -> None:
        cur.execute(f"DELETE FROM {table_name} WHERE serialized_key=?", (serialized_key,))

    @classmethod
    def delete_all_records(cls, table_name: str, cur: sqlite3.Cursor) -> None:
        cur.execute(f"DELETE FROM {table_name}")

    @classmethod
    def is_serialized_key_in(cls, table_name: str, cur: sqlite3.Cursor, serialized_key: bytes) -> bool:
        cur.execute(f"SELECT 1 FROM {table_name} WHERE serialized_key=?", (serialized_key,))
        return len(list(cur)) > 0

    @classmethod
    def get_serialized_value_by_serialized_key(
        cls, table_name: str, cur: sqlite3.Cursor, serialized_key: bytes
    ) -> Union[None, bytes]:
        cur.execute(
            f"SELECT serialized_value FROM {table_name} WHERE serialized_key=?",
            (serialized_key,),
        )
        res = cur.fetchone()
        if res is None:
            return None
        return cast(bytes, res[0])

    @classmethod
    def get_next_order(cls, table_name: str, cur: sqlite3.Cursor) -> int:
        cur.execute(f"SELECT MAX(item_order) FROM {table_name}")
        res = cur.fetchone()[0]
        if res is None:
            return 0
        return cast(int, res) + 1

    @classmethod
    def get_count(cls, table_name: str, cur: sqlite3.Cursor) -> int:
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        res = cur.fetchone()
        return cast(int, res[0])

    @classmethod
    def get_serialized_keys(cls, table_name: str, cur: sqlite3.Cursor) -> Iterable[bytes]:
        cur.execute(f"SELECT serialized_key FROM {table_name} ORDER BY item_order")
        for res in cur:
            yield cast(bytes, res[0])

    @classmethod
    def insert_serialized_value_by_serialized_key(
        cls, table_name: str, cur: sqlite3.Cursor, serialized_key: bytes, serialized_value: bytes
    ) -> None:
        item_order = cls.get_next_order(table_name, cur)
        cur.execute(
            f"INSERT INTO {table_name} (serialized_key, serialized_value, item_order) VALUES (?, ?, ?)",
            (serialized_key, serialized_value, item_order),
        )

    @classmethod
    def update_serialized_value_by_serialized_key(
        cls, table_name: str, cur: sqlite3.Cursor, serialized_key: bytes, serialized_value: bytes
    ) -> None:
        cur.execute(
            f"UPDATE {table_name} SET serialized_value=? WHERE serialized_key=?",
            (serialized_value, serialized_key),
        )

    @classmethod
    def upsert(
        cls,
        table_name: str,
        cur: sqlite3.Cursor,
        serialized_key: bytes,
        serialized_value: bytes,
    ) -> None:
        if cls.is_serialized_key_in(table_name, cur, serialized_key):
            cls.update_serialized_value_by_serialized_key(table_name, cur, serialized_key, serialized_value)
        else:
            cls.insert_serialized_value_by_serialized_key(table_name, cur, serialized_key, serialized_value)

    @classmethod
    def get_last_serialized_item(cls, table_name: str, cur: sqlite3.Cursor) -> Tuple[bytes, bytes]:
        cur.execute(f"SELECT serialized_key, serialized_value FROM {table_name} ORDER BY item_order DESC LIMIT 1")
        return cast(Tuple[bytes, bytes], cur.fetchone())

    @classmethod
    def get_reversed_serialized_keys(cls, table_name: str, cur: sqlite3.Cursor) -> Iterable[bytes]:
        cur.execute(f"SELECT serialized_key FROM {table_name} ORDER BY item_order DESC")
        for res in cur:
            yield cast(bytes, res[0])


class _Dict(Generic[KT, VT], SqliteCollectionBase[KT], MutableMapping[KT, VT]):
    _driver_class = _DictDatabaseDriver

    def __init__(
        self,
        connection: Optional[Union[str, sqlite3.Connection]] = None,
        table_name: Optional[str] = None,
        key_serializer: Optional[Callable[[KT], bytes]] = None,
        key_deserializer: Optional[Callable[[bytes], KT]] = None,
        value_serializer: Optional[Callable[[VT], bytes]] = None,
        value_deserializer: Optional[Callable[[bytes], VT]] = None,
        serializer: Optional[Callable[[VT], bytes]] = None,
        deserializer: Optional[Callable[[bytes], VT]] = None,
        persist: bool = True,
        rebuild_strategy: RebuildStrategy = RebuildStrategy.CHECK_WITH_FIRST_ELEMENT,
        data: Optional[Union[Iterable[Tuple[KT, VT]], Mapping[KT, VT]]] = None,
    ) -> None:
        if serializer is not None:
            warnings.warn(
                "serializer argument is deprecated. use key_serializer or value_serializer instead",
                DeprecationWarning,
                stacklevel=2,
            )
        self._value_serializer = (
            value_serializer
            if value_serializer is not None
            else serializer
            if serializer is not None
            else cast(Callable[[VT], bytes], self._default_serializer if key_serializer is None else key_serializer)
        )
        if deserializer is not None:
            warnings.warn(
                "deserializer argument is deprecated. use key_deserializer or value_deserializer instead",
                DeprecationWarning,
                stacklevel=2,
            )
        self._value_deserializer = (
            value_deserializer
            if value_deserializer is not None
            else deserializer
            if deserializer is not None
            else cast(
                Callable[[bytes], VT], self._default_deserializer if key_deserializer is None else key_deserializer
            )
        )
        super(_Dict, self).__init__(
            connection=connection,
            table_name=table_name,
            serializer=key_serializer,
            deserializer=key_deserializer,
            persist=persist,
            rebuild_strategy=rebuild_strategy,
        )
        if data is not None:
            self.clear()
            self.update(data)

    @property
    def key_serializer(self) -> Callable[[KT], bytes]:
        return self._serializer

    @property
    def key_deserializer(self) -> Callable[[bytes], KT]:
        return self._deserializer

    @property
    def value_serializer(self) -> Callable[[VT], bytes]:
        return self._value_serializer

    @property
    def value_deserializer(self) -> Callable[[bytes], VT]:
        return self._value_deserializer

    @property
    def schema_version(self) -> str:
        return "0"

    def _rebuild_check_with_first_element(self) -> bool:
        cur = self.connection.cursor()
        cur.execute(f"SELECT serialized_key FROM {self.table_name} ORDER BY item_order LIMIT 1")
        res = cur.fetchone()
        if res is None:
            return False
        serialized_key = cast(bytes, res[0])
        key = self.deserialize_key(serialized_key)
        return serialized_key != self.serialize_key(key)

    def _do_rebuild(self) -> None:
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
                    self.serialize_value(self.deserialize_value(serialized_value)),
                    i,
                ),
            )
            last_order = i

    def serialize_key(self, key: KT) -> bytes:
        if not is_hashable(key):
            raise TypeError(f"unhashable type: '{type(key).__name__}'")
        return self.key_serializer(key)

    def deserialize_key(self, serialized_key: bytes) -> KT:
        return self.key_deserializer(serialized_key)

    def serialize_value(self, value: VT) -> bytes:
        return self.value_serializer(value)

    def deserialize_value(self, value: bytes) -> VT:
        return self.value_deserializer(value)

    def __delitem__(self, key: KT) -> None:
        serialized_key = self.serialize_key(key)
        cur = self.connection.cursor()
        if not self._driver_class.is_serialized_key_in(self.table_name, cur, serialized_key):
            raise KeyError(key)
        self._driver_class.delete_single_record_by_serialized_key(self.table_name, cur, serialized_key)
        self.connection.commit()

    def __getitem__(self, key: KT) -> VT:
        serialized_key = self.serialize_key(key)
        cur = self.connection.cursor()
        serialized_value = self._driver_class.get_serialized_value_by_serialized_key(
            self.table_name, cur, serialized_key
        )
        if serialized_value is None:
            raise KeyError(key)
        return self.deserialize_value(serialized_value)

    def __iter__(self) -> Iterator[KT]:
        cur = self.connection.cursor()
        for serialized_key in self._driver_class.get_serialized_keys(self.table_name, cur):
            yield self.deserialize_key(serialized_key)

    def __len__(self) -> int:
        cur = self.connection.cursor()
        return self._driver_class.get_count(self.table_name, cur)

    def __setitem__(self, key: KT, value: VT) -> None:
        serialized_key = self.serialize_key(key)
        cur = self.connection.cursor()
        serialized_value = self.serialize_value(value)
        self._driver_class.upsert(self.table_name, cur, serialized_key, serialized_value)
        self.connection.commit()

    def _create_volatile_copy(
        self,
        data: Optional[Mapping[KT, VT]] = None,
    ) -> "Dict[KT, VT]":

        return Dict[KT, VT](
            connection=self.connection,
            key_serializer=self.key_serializer,
            key_deserializer=self.key_deserializer,
            value_serializer=self.value_serializer,
            value_deserializer=self.value_deserializer,
            rebuild_strategy=RebuildStrategy.SKIP,
            persist=False,
            data=(self if data is None else data),
        )

    def copy(self) -> "Dict[KT, VT]":
        return self._create_volatile_copy()

    @classmethod
    def fromkeys(cls, iterable: Iterable[KT], value: Optional[VT]) -> "Dict[KT, VT]":
        raise NotImplementedError

    @overload
    def pop(self, k: KT) -> VT:
        ...

    @overload
    def pop(self, k: KT, default: Union[VT, T] = ...) -> Union[VT, T]:
        ...

    def pop(self, k: KT, default: Optional[Union[VT, object]] = None) -> Union[VT, object]:
        cur = self.connection.cursor()
        serialized_key = self.serialize_key(k)
        serialized_value = self._driver_class.get_serialized_value_by_serialized_key(
            self.table_name, cur, serialized_key
        )
        if serialized_value is None:
            if default is None:
                raise KeyError(k)
            return default
        self._driver_class.delete_single_record_by_serialized_key(self.table_name, cur, serialized_key)
        self.connection.commit()
        return self.deserialize_value(serialized_value)

    def popitem(self) -> Tuple[KT, VT]:
        cur = self.connection.cursor()
        serialized_item = self._driver_class.get_last_serialized_item(self.table_name, cur)
        if serialized_item is None:
            raise KeyError("popitem(): dictionary is empty")
        self._driver_class.delete_single_record_by_serialized_key(self.table_name, cur, serialized_item[0])
        self.connection.commit()
        return (
            self.deserialize_key(serialized_item[0]),
            self.deserialize_value(serialized_item[1]),
        )

    @overload
    def update(self, __other: Mapping[KT, VT], **kwargs: VT) -> None:
        ...

    @overload
    def update(self, __other: Iterable[Tuple[KT, VT]], **kwargs: VT) -> None:
        ...

    @overload
    def update(self, **kwargs: VT) -> None:
        ...

    def update(self, __other: Optional[Union[Iterable[Tuple[KT, VT]], Mapping[KT, VT]]] = None, **kwargs: VT) -> None:
        cur = self.connection.cursor()
        for k, v in chain(
            tuple() if __other is None else __other.items() if isinstance(__other, Mapping) else __other,
            cast(Mapping[KT, VT], kwargs).items(),
        ):
            self._driver_class.upsert(self.table_name, cur, self.serialize_key(k), self.serialize_value(v))
        self.connection.commit()

    def clear(self) -> None:
        cur = self.connection.cursor()
        self._driver_class.delete_all_records(self.table_name, cur)
        self.connection.commit()

    def __contains__(self, o: object) -> bool:
        return self._driver_class.is_serialized_key_in(
            self.table_name, self.connection.cursor(), self.serialize_key(cast(KT, o))
        )

    @overload
    def get(self, key: KT) -> Union[VT, None]:
        ...

    @overload
    def get(self, key: KT, default_value: Union[VT, T]) -> Union[VT, T]:
        ...

    def get(self, key: KT, default_value: Optional[Union[VT, object]] = None) -> Union[VT, None, object]:
        serialized_key = self.serialize_key(key)
        cur = self.connection.cursor()
        serialized_value = self._driver_class.get_serialized_value_by_serialized_key(
            self.table_name, cur, serialized_key
        )
        if serialized_value is None:
            return default_value
        return self.deserialize_value(serialized_value)

    def setdefault(self, key: KT, default: VT = None) -> VT:  # type: ignore
        serialized_key = self.serialize_key(key)
        cur = self.connection.cursor()
        serialized_value = self._driver_class.get_serialized_value_by_serialized_key(
            self.table_name, cur, serialized_key
        )
        if serialized_value is None:
            self._driver_class.insert_serialized_value_by_serialized_key(
                self.table_name, cur, serialized_key, self.serialize_value(default)
            )
            return default
        return self.deserialize_value(serialized_value)


if sys.version_info >= (3, 8):

    class _ReversibleDict(_Dict[KT, VT], Reversible[KT]):
        def __reversed__(self) -> Iterator[KT]:
            cur = self.connection.cursor()
            for serialized_key in self._driver_class.get_reversed_serialized_keys(self.table_name, cur):
                yield self.deserialize_key(serialized_key)


if sys.version_info >= (3, 9):

    class Dict(_ReversibleDict[KT, VT]):
        def __or__(self, other: Mapping[KT, VT]) -> "Dict[KT, VT]":
            tmp = Dict(
                connection=self.connection,
                key_serializer=self.key_serializer,
                key_deserializer=self.key_deserializer,
                value_serializer=self.value_serializer,
                value_deserializer=self.value_deserializer,
                persist=self.persist,
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
