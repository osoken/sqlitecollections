import sqlite3
import sys
from typing import Optional, Union, cast, overload

if sys.version_info > (3, 9):
    from collections.abc import Callable, Iterable, MutableSequence
else:
    from typing import Callable, Iterable, MutableSequence

from . import RebuildStrategy
from .base import SqliteCollectionBase, T


class List(SqliteCollectionBase[T], MutableSequence[T]):
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
        super(List, self).__init__(
            connection=connection,
            table_name=table_name,
            serializer=serializer,
            deserializer=deserializer,
            persist=persist,
            rebuild_strategy=rebuild_strategy,
            do_initialize=True,
        )

    def _do_create_table(self, commit: bool = False) -> None:
        cur = self.connection.cursor()
        cur.execute(f"CREATE TABLE {self.table_name} (serialized_value BLOB, item_index INTEGER PRIMARY KEY)")
        if commit:
            self.connection.commit()

    def _do_rebuild(self, commit: bool = False) -> None:
        cur = self.connection.cursor()
        last_index = -1
        while last_index is not None:
            cur.execute(
                f"SELECT serialized_value, item_index FROM {self.table_name} WHERE item_index > ? ORDER BY item_index LIMIT 1",
                (last_index,),
            )
            res = cur.fetchone()
            if res is None:
                break
            cur.execute(
                f"UPDATE {self.table_name} SET serialized_value=? WHERE item_index=?",
                (self.serialize(self.deserialize(res[0])), res[1]),
            )
            last_index = res[1]
        if commit:
            self.connection.commit()

    def _rebuild_check_with_first_element(self) -> bool:
        cur = self.connection.cursor()
        cur.execute(f"SELECT serialized_value FROM {self.table_name} ORDER BY item_index LIMIT 1")
        res = cur.fetchone()
        if res is None:
            return False
        serialized_value = cast(bytes, res[0])
        value = self.deserialize(serialized_value)
        return serialized_value != self.serialize(value)

    @property
    def schema_version(self) -> str:
        return "0"

    def _get_count(self, cur: sqlite3.Cursor) -> int:
        cur.execute(f"SELECT COUNT(*) FROM {self.table_name}")
        return cast(int, cur.fetchone()[0])

    def _get_index_by_serialized_value(self, cur: sqlite3.Cursor, serialized_value: bytes) -> int:
        cur.execute(f"SELECT item_index FROM {self.table_name} WHERE serialized_value = ? LIMIT 1", (serialized_value,))
        res = cur.fetchone()
        if res is None:
            return -1
        return cast(int, res[0])

    def _get_serialized_value_by_index(self, cur: sqlite3.Cursor, index: int) -> Union[None, bytes]:
        if index < 0:
            l = self._get_count(cur)
            cur.execute(f"SELECT serialized_value FROM {self.table_name} WHERE item_index = ?", (l + index,))
        else:
            cur.execute(f"SELECT serialized_value FROM {self.table_name} WHERE item_index = ?", (index,))
        res = cur.fetchone()
        if res is None:
            return None
        return cast(bytes, res[0])

    def __delitem__(self, i: Union[int, slice]) -> None:
        raise NotImplementedError

    @overload
    def __getitem__(self, i: int) -> T:
        ...

    @overload
    def __getitem__(self, i: slice) -> "List[T]":
        ...

    def __getitem__(self, i: Union[int, slice]) -> "Union[T, List[T]]":
        if isinstance(i, int):
            cur = self.connection.cursor()
            serialized_value = self._get_serialized_value_by_index(cur, i)
            if serialized_value is None:
                raise IndexError("list index out of range")
            return self.deserialize(serialized_value)
        raise NotImplementedError

    def __setitem__(self, i: Union[int, slice], v: Union[T, Iterable[T]]) -> None:
        raise NotImplementedError

    def __len__(self) -> int:
        raise NotImplementedError

    def insert(self, i: int, v: T) -> None:
        raise NotImplementedError

    def __contains__(self, x: T) -> bool:
        cur = self.connection.cursor()
        serialized_value = self.serialize(x)
        index = self._get_index_by_serialized_value(cur, serialized_value)
        return index != -1
