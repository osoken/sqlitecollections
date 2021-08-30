import sqlite3
import sys
from collections.abc import Hashable
from typing import Callable, Generic, Optional, Tuple, TypeVar, Union, cast

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
    from typing import Iterable, Iterator, Mapping, MutableMapping
if sys.version_info >= (3, 8):
    from typing import Reversible

from .base import RebuildStrategy, SqliteCollectionBase

T = TypeVar("T")
KT = TypeVar("KT")
VT = TypeVar("VT")


class _Dict(Generic[KT, VT], SqliteCollectionBase[VT], MutableMapping[KT, VT]):
    def __init__(
        self,
        connection: Optional[Union[str, sqlite3.Connection]] = None,
        table_name: Optional[str] = None,
        serializer: Optional[Callable[[VT], bytes]] = None,
        deserializer: Optional[Callable[[bytes], VT]] = None,
        key_serializer: Optional[Callable[[KT], bytes]] = None,
        key_deserializer: Optional[Callable[[bytes], KT]] = None,
        persist: bool = True,
        rebuild_strategy: RebuildStrategy = RebuildStrategy.CHECK_WITH_FIRST_ELEMENT,
        data: Optional[Mapping[KT, VT]] = None,
    ) -> None:
        super(_Dict, self).__init__(
            connection=connection,
            table_name=table_name,
            serializer=serializer,
            deserializer=deserializer,
            persist=persist,
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
        if data is not None:
            self.update(data)

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

    def _create_volatile_copy(
        self,
        data: Optional[Mapping[KT, VT]] = None,
    ) -> "Dict[KT, VT]":

        return Dict[KT, VT](
            connection=self.connection,
            serializer=self.serializer,
            deserializer=self.deserializer,
            key_serializer=self.key_serializer,
            key_deserializer=self.key_deserializer,
            rebuild_strategy=RebuildStrategy.SKIP,
            persist=False,
            data=(self if data is None else data),
        )

    def copy(self) -> "Dict[KT, VT]":
        return self._create_volatile_copy()

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
