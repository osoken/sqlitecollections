import sqlite3
import sys
from typing import AbstractSet, Any, Callable, Optional, Union, cast
from uuid import uuid4

if sys.version_info >= (3, 9):
    from collections.abc import Iterable, Iterator, MutableSet
else:
    from typing import Iterable, Iterator, MutableSet

from . import RebuildStrategy
from .base import _S, _T, SqliteCollectionBase, T, TemporaryTableContext, is_hashable


class _SetDatabaseDriver:
    def __init__(self, table_name: str):
        self.table_name = table_name

    def delete_all(self, cur: sqlite3.Cursor) -> None:
        cur.execute(f"DELETE FROM {self.table_name}")

    def insert(self, cur: sqlite3.Cursor, serialized_value: bytes) -> None:
        cur.execute(
            f"INSERT INTO {self.table_name} (serialized_value) VALUES (?)",
            (serialized_value,),
        )

    def upsert(self, cur: sqlite3.Cursor, serialized_value: bytes) -> None:
        if not self.is_serialized_value_in(cur, serialized_value):
            self.insert(cur, serialized_value)

    def delete_by_serialized_value(self, cur: sqlite3.Cursor, serialized_value: bytes) -> None:
        cur.execute(f"DELETE FROM {self.table_name} WHERE serialized_value = ?", (serialized_value,))

    def is_serialized_value_in(self, cur: sqlite3.Cursor, serialized_value: bytes) -> bool:
        cur.execute(f"SELECT 1 FROM {self.table_name} WHERE serialized_value=?", (serialized_value,))
        return len(list(cur)) > 0

    def get_one_serialized_value(self, cur: sqlite3.Cursor) -> Union[None, bytes]:
        cur.execute(f"SELECT serialized_value FROM {self.table_name} LIMIT 1")
        res = cur.fetchone()
        if res is None:
            return None
        return cast(bytes, res[0])

    def get_count(self, cur: sqlite3.Cursor) -> int:
        cur.execute(f"SELECT COUNT(*) FROM {self.table_name}")
        res = cur.fetchone()
        return cast(int, res[0])

    def get_serialized_values(self, cur: sqlite3.Cursor) -> Iterable[bytes]:
        cur.execute(f"SELECT serialized_value FROM {self.table_name}")
        for d in cur:
            yield cast(bytes, d[0])

    def intersection_update_single(self, cur: sqlite3.Cursor, data: Iterable[bytes]) -> None:
        with TemporaryTableContext(cur, self.table_name) as temp_table_name:
            temporay_driver = _SetDatabaseDriver(temp_table_name)
            for d in data:
                temporay_driver.upsert(cur, d)
            cur.execute(
                f"DELETE FROM {self.table_name} WHERE NOT EXISTS (SELECT serialized_value FROM {temp_table_name} WHERE {self.table_name}.serialized_value = {temp_table_name}.serialized_value)"
            )

    def difference_update_single(self, cur: sqlite3.Cursor, data: Iterable[bytes]) -> None:
        for d in data:
            self.delete_by_serialized_value(cur, d)

    def union_update_single(self, cur: sqlite3.Cursor, data: Iterable[bytes]) -> None:
        for d in data:
            self.upsert(cur, d)

    def symmetric_difference_update_single(
        self, cur: sqlite3.Cursor, cur2: sqlite3.Cursor, data: Iterable[bytes]
    ) -> None:
        with TemporaryTableContext(cur, self.table_name) as temp_table_name:
            temporay_driver = _SetDatabaseDriver(temp_table_name)
            for d in data:
                temporay_driver.upsert(cur, d)
            for serialized_value in temporay_driver.get_serialized_values(cur2):
                if self.is_serialized_value_in(cur, serialized_value):
                    self.delete_by_serialized_value(cur, serialized_value)
                else:
                    self.insert(cur, serialized_value)

    def is_proper_superset(self, cur: sqlite3.Cursor, cur2: sqlite3.Cursor, data: Iterable[bytes]) -> bool:
        with TemporaryTableContext(cur, self.table_name) as temp_table_name:
            temporay_driver = _SetDatabaseDriver(temp_table_name)
            for d in data:
                if not self.is_serialized_value_in(cur2, d):
                    return False
                temporay_driver.upsert(cur2, d)
            return temporay_driver.get_count(cur2) < self.get_count(cur2)

    def is_proper_subset(self, cur: sqlite3.Cursor, cur2: sqlite3.Cursor, data: Iterable[bytes]) -> bool:
        is_proper = False
        with TemporaryTableContext(cur, self.table_name) as temp_table_name:
            temporary_driver = _SetDatabaseDriver(temp_table_name)
            for d in data:
                if not self.is_serialized_value_in(cur2, d):
                    is_proper = True
                else:
                    temporary_driver.upsert(cur2, d)
            return is_proper and temporary_driver.get_count(cur2) == self.get_count(cur2)


class Set(SqliteCollectionBase[T], MutableSet[T]):
    def __init__(
        self,
        connection: Optional[Union[str, sqlite3.Connection]] = None,
        table_name: Optional[str] = None,
        serializer: Optional[Callable[[T], bytes]] = None,
        deserializer: Optional[Callable[[bytes], T]] = None,
        persist: bool = True,
        rebuild_strategy: RebuildStrategy = RebuildStrategy.CHECK_WITH_FIRST_ELEMENT,
        data: Optional[Iterable[T]] = None,
    ) -> None:
        super(Set, self).__init__(
            connection=connection,
            table_name=table_name,
            serializer=serializer,
            deserializer=deserializer,
            persist=persist,
            rebuild_strategy=rebuild_strategy,
            do_initialize=True,
        )
        self._database_driver = _SetDatabaseDriver(self.table_name)
        if data is not None:
            self.clear()
            self.update(data)

    def __contains__(self, value: object) -> bool:
        cur = self.connection.cursor()
        serialized_value = self.serialize(cast(T, value))
        return self._database_driver.is_serialized_value_in(cur, serialized_value)

    def __iter__(self) -> Iterator[T]:
        cur = self.connection.cursor()
        for d in self._database_driver.get_serialized_values(cur):
            yield self.deserialize(d)

    def __len__(self) -> int:
        cur = self.connection.cursor()
        return self._database_driver.get_count(cur)

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

    def serialize(self, value: T) -> bytes:
        if not is_hashable(value):
            raise TypeError(f"unhashable type: '{type(value).__name__}'")
        return self.serializer(value)

    def add(self, value: T) -> None:
        serialized_value = self.serialize(value)
        cur = self.connection.cursor()
        self._database_driver.upsert(cur, serialized_value)
        self.connection.commit()

    def clear(self) -> None:
        cur = self.connection.cursor()
        self._database_driver.delete_all(cur)
        self.connection.commit()

    def discard(self, value: T) -> None:
        cur = self.connection.cursor()
        self._database_driver.delete_by_serialized_value(cur, self.serialize(value))
        self.connection.commit()

    def remove(self, value: T) -> None:
        cur = self.connection.cursor()
        serialized_value = self.serialize(value)
        if not self._database_driver.is_serialized_value_in(cur, serialized_value):
            raise KeyError(value)
        self._database_driver.delete_by_serialized_value(cur, serialized_value)
        self.connection.commit()

    def pop(self) -> T:
        cur = self.connection.cursor()
        serialized_value = self._database_driver.get_one_serialized_value(cur)
        if serialized_value is None:
            raise KeyError("'pop from an empty set'")
        self._database_driver.delete_by_serialized_value(cur, serialized_value)
        self.connection.commit()
        return self.deserialize(serialized_value)

    @property
    def schema_version(self) -> str:
        return "0"

    def issubset(self, other: Iterable[T]) -> bool:
        return len(self) == len(self.intersection(other))

    def __lt__(self, other: Iterable[T]) -> bool:
        return self._database_driver.is_proper_subset(
            self.connection.cursor(), self.connection.cursor(), (self.serialize(d) for d in other)
        )

    def __le__(self, other: Iterable[T]) -> bool:
        return self.issubset(other)

    def intersection(self, *others: Iterable[T]) -> "Set[T]":
        res = self.copy()
        res.intersection_update(*others)
        return res

    def intersection_update(self, *others: Iterable[T]) -> None:
        cur = self.connection.cursor()
        for other in others:
            self._database_driver.intersection_update_single(cur, (self.serialize(d) for d in other))
        self.connection.commit()

    def issuperset(self, other: Iterable[T]) -> bool:
        cur = self.connection.cursor()
        for d in other:
            if not self._database_driver.is_serialized_value_in(cur, self.serialize(d)):
                return False
        return True

    def __gt__(self, other: Iterable[T]) -> bool:
        return self._database_driver.is_proper_superset(
            self.connection.cursor(), self.connection.cursor(), (self.serialize(d) for d in other)
        )

    def __ge__(self, other: Iterable[T]) -> bool:
        return self.issuperset(other)

    def union(self, *others: Iterable[T]) -> "Set[T]":
        res = self.copy()
        res.update(*others)
        return res

    def update(self, *others: Iterable[T]) -> None:
        cur = self.connection.cursor()
        for other in others:
            self._database_driver.union_update_single(cur, (self.serialize(d) for d in other))
        self.connection.commit()

    def isdisjoint(self, other: Iterable[T]) -> bool:
        cur = self.connection.cursor()
        for d in other:
            if self._database_driver.is_serialized_value_in(cur, self.serialize(d)):
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
            self._database_driver.difference_update_single(cur, (self.serialize(d) for d in other))
        self.connection.commit()

    def _create_volatile_copy(self, data: Optional[Iterable[T]] = None) -> "Set[T]":
        return Set[T](
            connection=self.connection,
            serializer=self.serializer,
            deserializer=self.deserializer,
            rebuild_strategy=RebuildStrategy.SKIP,
            persist=False,
            data=data if data is not None else self,
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
            self._database_driver.symmetric_difference_update_single(cur, cur2, (self.serialize(d) for d in other))
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
