import sqlite3
import sys
import warnings
from typing import AbstractSet, Any, Optional, Union, cast
from uuid import uuid4

if sys.version_info >= (3, 9):
    from collections.abc import Callable, Iterable, Iterator, MutableSet
else:
    from typing import Iterable, Iterator, MutableSet, Callable

from .base import (
    _S,
    _T,
    SqliteCollectionBase,
    T,
    TemporaryTableContext,
    _SqliteCollectionBaseDatabaseDriver,
    is_hashable,
)


class _SetDatabaseDriver(_SqliteCollectionBaseDatabaseDriver):
    if sys.version_info >= (3, 9):

        @classmethod
        @property
        def schema_version(cls) -> str:
            return "0"

    else:
        schema_version = "0"

    @classmethod
    def do_create_table(cls, table_name: str, container_type_nam: str, cur: sqlite3.Cursor) -> None:
        cur.execute(f"CREATE TABLE {table_name} (serialized_value BLOB PRIMARY KEY)")

    @classmethod
    def delete_all(cls, table_name: str, cur: sqlite3.Cursor) -> None:
        cur.execute(f"DELETE FROM {table_name}")

    @classmethod
    def insert(cls, table_name: str, cur: sqlite3.Cursor, serialized_value: bytes) -> None:
        cur.execute(
            f"INSERT INTO {table_name} (serialized_value) VALUES (?)",
            (serialized_value,),
        )

    @classmethod
    def upsert(cls, table_name: str, cur: sqlite3.Cursor, serialized_value: bytes) -> None:
        if not cls.is_serialized_value_in(table_name, cur, serialized_value):
            cls.insert(table_name, cur, serialized_value)

    @classmethod
    def delete_by_serialized_value(cls, table_name: str, cur: sqlite3.Cursor, serialized_value: bytes) -> None:
        cur.execute(f"DELETE FROM {table_name} WHERE serialized_value = ?", (serialized_value,))

    @classmethod
    def is_serialized_value_in(cls, table_name: str, cur: sqlite3.Cursor, serialized_value: bytes) -> bool:
        cur.execute(f"SELECT 1 FROM {table_name} WHERE serialized_value=?", (serialized_value,))
        return len(list(cur)) > 0

    @classmethod
    def get_one_serialized_value(cls, table_name: str, cur: sqlite3.Cursor) -> Union[None, bytes]:
        cur.execute(f"SELECT serialized_value FROM {table_name} LIMIT 1")
        res = cur.fetchone()
        if res is None:
            return None
        return cast(bytes, res[0])

    @classmethod
    def get_count(cls, table_name: str, cur: sqlite3.Cursor) -> int:
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        res = cur.fetchone()
        return cast(int, res[0])

    @classmethod
    def get_serialized_values(cls, table_name: str, cur: sqlite3.Cursor) -> Iterable[bytes]:
        cur.execute(f"SELECT serialized_value FROM {table_name}")
        for d in cur:
            yield cast(bytes, d[0])

    @classmethod
    def intersection_update_single(cls, table_name: str, cur: sqlite3.Cursor, data: Iterable[bytes]) -> None:
        with TemporaryTableContext(cur, table_name) as temp_table_name:
            for d in data:
                cls.upsert(temp_table_name, cur, d)
            cur.execute(
                f"DELETE FROM {table_name} WHERE NOT EXISTS (SELECT serialized_value FROM {temp_table_name} WHERE {table_name}.serialized_value = {temp_table_name}.serialized_value)"
            )

    @classmethod
    def difference_update_single(cls, table_name: str, cur: sqlite3.Cursor, data: Iterable[bytes]) -> None:
        for d in data:
            cls.delete_by_serialized_value(table_name, cur, d)

    @classmethod
    def union_update_single(cls, table_name: str, cur: sqlite3.Cursor, data: Iterable[bytes]) -> None:
        for d in data:
            cls.upsert(table_name, cur, d)

    @classmethod
    def symmetric_difference_update_single(
        cls, table_name: str, cur: sqlite3.Cursor, cur2: sqlite3.Cursor, data: Iterable[bytes]
    ) -> None:
        with TemporaryTableContext(cur, table_name) as temp_table_name:
            for d in data:
                cls.upsert(temp_table_name, cur, d)
            for serialized_value in cls.get_serialized_values(temp_table_name, cur2):
                if cls.is_serialized_value_in(table_name, cur, serialized_value):
                    cls.delete_by_serialized_value(table_name, cur, serialized_value)
                else:
                    cls.insert(table_name, cur, serialized_value)

    @classmethod
    def is_proper_superset(
        cls, table_name: str, cur: sqlite3.Cursor, cur2: sqlite3.Cursor, data: Iterable[bytes]
    ) -> bool:
        with TemporaryTableContext(cur, table_name) as temp_table_name:
            for d in data:
                if not cls.is_serialized_value_in(table_name, cur2, d):
                    return False
                cls.upsert(temp_table_name, cur2, d)
            return cls.get_count(temp_table_name, cur2) < cls.get_count(table_name, cur2)

    @classmethod
    def is_proper_subset(
        cls, table_name: str, cur: sqlite3.Cursor, cur2: sqlite3.Cursor, data: Iterable[bytes]
    ) -> bool:
        is_proper = False
        with TemporaryTableContext(cur, table_name) as temp_table_name:
            for d in data:
                if not cls.is_serialized_value_in(table_name, cur2, d):
                    is_proper = True
                else:
                    cls.upsert(temp_table_name, cur2, d)
            return is_proper and cls.get_count(temp_table_name, cur2) == cls.get_count(table_name, cur2)


class Set(SqliteCollectionBase[T], MutableSet[T]):
    _driver_class = _SetDatabaseDriver

    def __init__(
        self,
        __data: Optional[Iterable[T]] = None,
        connection: Optional[Union[str, sqlite3.Connection]] = None,
        table_name: Optional[str] = None,
        serializer: Optional[Callable[[T], bytes]] = None,
        deserializer: Optional[Callable[[bytes], T]] = None,
        persist: bool = True,
    ) -> None:
        super(Set, self).__init__(
            connection=connection,
            table_name=table_name,
            serializer=serializer,
            deserializer=deserializer,
            persist=persist,
        )
        if __data is not None:
            self.clear()
            self.update(__data)

    def __contains__(self, value: object) -> bool:
        cur = self.connection.cursor()
        serialized_value = self.serialize(cast(T, value))
        return self._driver_class.is_serialized_value_in(self.table_name, cur, serialized_value)

    def __iter__(self) -> Iterator[T]:
        cur = self.connection.cursor()
        for d in self._driver_class.get_serialized_values(self.table_name, cur):
            yield self.deserialize(d)

    def __len__(self) -> int:
        cur = self.connection.cursor()
        return self._driver_class.get_count(self.table_name, cur)

    def serialize(self, value: T) -> bytes:
        if not is_hashable(value):
            raise TypeError(f"unhashable type: '{type(value).__name__}'")
        return self.serializer(value)

    def add(self, value: T) -> None:
        serialized_value = self.serialize(value)
        cur = self.connection.cursor()
        self._driver_class.upsert(self.table_name, cur, serialized_value)
        self.connection.commit()

    def clear(self) -> None:
        cur = self.connection.cursor()
        self._driver_class.delete_all(self.table_name, cur)
        self.connection.commit()

    def discard(self, value: T) -> None:
        cur = self.connection.cursor()
        self._driver_class.delete_by_serialized_value(self.table_name, cur, self.serialize(value))
        self.connection.commit()

    def remove(self, value: T) -> None:
        cur = self.connection.cursor()
        serialized_value = self.serialize(value)
        if not self._driver_class.is_serialized_value_in(self.table_name, cur, serialized_value):
            raise KeyError(value)
        self._driver_class.delete_by_serialized_value(self.table_name, cur, serialized_value)
        self.connection.commit()

    def pop(self) -> T:
        cur = self.connection.cursor()
        serialized_value = self._driver_class.get_one_serialized_value(self.table_name, cur)
        if serialized_value is None:
            raise KeyError("'pop from an empty set'")
        self._driver_class.delete_by_serialized_value(self.table_name, cur, serialized_value)
        self.connection.commit()
        return self.deserialize(serialized_value)

    def issubset(self, other: Iterable[T]) -> bool:
        return len(self) == len(self.intersection(other))

    def __lt__(self, other: AbstractSet[T]) -> bool:
        if len(self) >= len(other):
            return False
        for d in self:
            if d not in other:
                return False
        return True

    def __le__(self, other: AbstractSet[T]) -> bool:
        for d in self:
            if d not in other:
                return False
        return True

    def intersection(self, *others: Iterable[T]) -> "Set[T]":
        res = self.copy()
        res.intersection_update(*others)
        return res

    def intersection_update(self, *others: Iterable[T]) -> None:
        cur = self.connection.cursor()
        for other in others:
            self._driver_class.intersection_update_single(self.table_name, cur, (self.serialize(d) for d in other))
        self.connection.commit()

    def issuperset(self, other: Iterable[T]) -> bool:
        cur = self.connection.cursor()
        for d in other:
            if not self._driver_class.is_serialized_value_in(self.table_name, cur, self.serialize(d)):
                return False
        return True

    def __gt__(self, other: AbstractSet[T]) -> bool:
        return self._driver_class.is_proper_superset(
            self.table_name, self.connection.cursor(), self.connection.cursor(), (self.serialize(d) for d in other)
        )

    def __ge__(self, other: AbstractSet[T]) -> bool:
        return self.issuperset(other)

    def union(self, *others: Iterable[T]) -> "Set[T]":
        res = self.copy()
        res.update(*others)
        return res

    def update(self, *others: Iterable[T]) -> None:
        cur = self.connection.cursor()
        for other in others:
            self._driver_class.union_update_single(self.table_name, cur, (self.serialize(d) for d in other))
        self.connection.commit()

    def isdisjoint(self, other: Iterable[T]) -> bool:
        cur = self.connection.cursor()
        for d in other:
            if self._driver_class.is_serialized_value_in(self.table_name, cur, self.serialize(d)):
                return False
        return True

    @classmethod
    def _from_iterable(cls, *args: T) -> "Set[T]":
        raise NotImplementedError

    def __or__(self, s: AbstractSet[_T]) -> "Set[T]":
        return self.union(cast(Iterable[T], s))

    def __and__(self, s: AbstractSet[Any]) -> "Set[T]":
        return self.intersection(cast(Iterable[T], s))

    def difference(self, *others: Iterable[T]) -> "Set[T]":
        res = self.copy()
        res.difference_update(*others)
        return res

    def difference_update(self, *others: Iterable[T]) -> None:
        cur = self.connection.cursor()
        for other in others:
            self._driver_class.difference_update_single(self.table_name, cur, (self.serialize(d) for d in other))
        self.connection.commit()

    def _create_volatile_copy(self, data: Optional[Iterable[T]] = None) -> "Set[T]":
        return Set[T](
            data if data is not None else self,
            connection=self.connection,
            serializer=self.serializer,
            deserializer=self.deserializer,
            persist=False,
        )

    def copy(self) -> "Set[T]":
        return self._create_volatile_copy()

    def __sub__(self, s: AbstractSet[Any]) -> "Set[T]":
        return self.difference(cast(Iterable[T], s))

    def symmetric_difference(self, *others: Iterable[T]) -> "Set[T]":
        res = self.copy()
        res.symmetric_difference_update(*others)
        return res

    def symmetric_difference_update(self, *others: Iterable[T]) -> None:
        cur = self.connection.cursor()
        cur2 = self.connection.cursor()
        for other in others:
            self._driver_class.symmetric_difference_update_single(
                self.table_name, cur, cur2, (self.serialize(d) for d in other)
            )
        self.connection.commit()

    def __xor__(self, s: AbstractSet[_T]) -> "Set[T]":
        return self.symmetric_difference(cast(Iterable[T], s))

    def __ixor__(self, s: AbstractSet[_S]) -> "Set[Union[_T, T]]":
        self.symmetric_difference_update(cast(Iterable[T], s))
        return cast(Set[Union[_T, T]], self)

    def __isub__(self, s: AbstractSet[Any]) -> "Set[T]":
        self.difference_update(cast(Iterable[T], s))
        return self

    def __iand__(self, s: AbstractSet[Any]) -> "Set[T]":
        self.intersection_update(cast(Iterable[T], s))
        return self

    def __ior__(self, s: AbstractSet[_S]) -> "Set[Union[_T, T]]":
        self.update(cast(Iterable[T], s))
        return cast(Set[Union[_T, T]], self)
