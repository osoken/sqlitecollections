import math
import os
import sqlite3
import sys
import warnings
from enum import Enum
from itertools import count, repeat
from typing import Any, Optional, Tuple, Union, cast, overload

if sys.version_info >= (3, 9):
    from collections.abc import (
        Callable,
        Iterable,
        Iterator,
        Mapping,
        MutableSequence,
        Sequence,
    )
else:
    from typing import Callable, Iterable, MutableSequence, Iterator, Sequence, Mapping

from .base import (
    PicklingStrategy,
    SqliteCollectionBase,
    T,
    _SqliteCollectionBaseDatabaseDriver,
    create_tempfile_connection,
    tidy_connection,
)


def _generate_indices_from_slice(l: int, s: slice) -> Iterator[int]:
    step = 1 if s.step is None else s.step
    if step == 0:
        raise ValueError("slice step cannot be zero")
    if step > 0:
        step = step
        start = min(max(0 if s.start is None else (s.start if s.start >= 0 else l + s.start), 0), l)
        stop = min(max(l if s.stop is None else (s.stop if s.stop >= 0 else l + s.stop), 0), l)
    else:
        step = step
        start = min(max(l - 1 if s.start is None else (s.start if s.start >= 0 else l + s.start), -1), l - 1)
        stop = min(max(-1 if s.stop is None else (s.stop if s.stop >= 0 else l + s.stop), -1), l)
    yield from range(start, stop, step)


class NoMoreElements(Exception):
    ...


class DifferentLengthDetected(Exception):
    def __init__(self, length1: int, length2: int) -> None:
        self.length1 = length1
        self.length2 = length2


def _consume_one_or_raise_no_more_elements(iter: Iterable[Any]) -> Any:
    for d in iter:
        return d
    raise NoMoreElements()


def _strict_zip(iter1: Iterable[Any], iter2: Iterable[Any]) -> Iterable[Tuple[Any, Any]]:
    element_count = 0
    while True:
        iter1_is_active = True
        try:
            next1 = _consume_one_or_raise_no_more_elements(iter1)
        except NoMoreElements:
            iter1_is_active = False

        iter2_is_active = True
        try:
            next2 = _consume_one_or_raise_no_more_elements(iter2)
        except NoMoreElements:
            iter2_is_active = False

        if iter1_is_active and iter2_is_active:
            yield (next1, next2)
            element_count += 1
        elif not iter1_is_active and not iter2_is_active:
            break
        else:
            iter1_unused_count = sum(d[0] for d in zip(repeat(1), iter1)) + int(iter1_is_active)
            iter2_unused_count = sum(d[0] for d in zip(repeat(1), iter2)) + int(iter2_is_active)
            raise DifferentLengthDetected(element_count + iter1_unused_count, element_count + iter2_unused_count)


class SortingStrategy(str, Enum):
    fastest = "fastest"
    balanced = "balanced"
    memory_saving = "memory_saving"


class _ListDatabaseDriver(_SqliteCollectionBaseDatabaseDriver):
    if sys.version_info >= (3, 9):

        @classmethod
        @property
        def schema_version(cls) -> str:
            return "0"

    else:
        schema_version = "0"

    @classmethod
    def do_create_table(cls, table_name: str, container_type_nam: str, cur: sqlite3.Cursor) -> None:
        cur.execute(f"CREATE TABLE {table_name} (serialized_value BLOB, item_index INTEGER PRIMARY KEY)")

    @classmethod
    def get_max_index_plus_one(cls, table_name: str, cur: sqlite3.Cursor) -> int:
        cur.execute(f"SELECT MAX(item_index) FROM {table_name}")
        res = cur.fetchone()
        if res[0] is None:
            return 0
        return cast(int, res[0]) + 1

    @classmethod
    def get_index_by_serialized_value(cls, table_name: str, cur: sqlite3.Cursor, serialized_value: bytes) -> int:
        cur.execute(f"SELECT item_index FROM {table_name} WHERE serialized_value = ? LIMIT 1", (serialized_value,))
        res = cur.fetchone()
        if res is None:
            return -1
        return cast(int, res[0])

    @classmethod
    def get_serialized_value_by_index(cls, table_name: str, cur: sqlite3.Cursor, index: int) -> Union[None, bytes]:
        if index < 0:
            l = cls.get_max_index_plus_one(table_name, cur)
            cur.execute(f"SELECT serialized_value FROM {table_name} WHERE item_index = ?", (l + index,))
        else:
            cur.execute(f"SELECT serialized_value FROM {table_name} WHERE item_index = ?", (index,))
        res = cur.fetchone()
        if res is None:
            return None
        return cast(bytes, res[0])

    @classmethod
    def tidy_indices(cls, table_name: str, cur: sqlite3.Cursor, cur2: sqlite3.Cursor, start: int = 0) -> None:
        cur.execute(f"SELECT item_index FROM {table_name} WHERE item_index >= ? ORDER BY item_index", (start,))
        for idx, d in zip(count(start), cur):
            idx_ = cast(int, d[0])
            if idx != idx_:
                cur2.execute(f"UPDATE {table_name} SET item_index = ? WHERE item_index = ?", (idx, idx_))

    @classmethod
    def delete_record_by_index(
        cls, table_name: str, cur: sqlite3.Cursor, index: int, length: Optional[int] = None
    ) -> Union[None, int]:
        _index = index
        if _index < 0:
            _length = length
            if _length is None:
                _length = cls.get_max_index_plus_one(table_name, cur)
            _index = _length + _index
        cur.execute(f"SELECT 1 FROM {table_name} WHERE item_index = ?", (_index,))
        if cur.fetchone() is None:
            return None
        cur.execute(f"DELETE FROM {table_name} WHERE item_index = ?", (_index,))
        return _index

    @classmethod
    def set_serialized_value_by_index(
        cls, table_name: str, cur: sqlite3.Cursor, serialized_value: bytes, index: int
    ) -> bool:
        _index = index
        l = cls.get_max_index_plus_one(table_name, cur)
        if _index < 0:
            _index = l + index
        if _index < 0 or l <= _index:
            return False
        cur.execute(f"UPDATE {table_name} SET serialized_value = ? WHERE item_index = ?", (serialized_value, _index))
        return True

    @classmethod
    def delete_all(cls, table_name: str, cur: sqlite3.Cursor) -> None:
        cur.execute(f"DELETE FROM {table_name}")

    @classmethod
    def add_record_by_serialized_value_and_index(
        cls, table_name: str, cur: sqlite3.Cursor, serialized_value: bytes, index: int
    ) -> None:
        cur.execute(f"INSERT INTO {table_name} (serialized_value, item_index) VALUES (?, ?)", (serialized_value, index))

    @classmethod
    def remap_index(cls, table_name: str, cur: sqlite3.Cursor, indices_map: Iterable[int]) -> None:
        l = cls.get_max_index_plus_one(table_name, cur)
        cur.execute(f"UPDATE {table_name} SET item_index = item_index - ?", (l,))
        cur.executemany(
            f"UPDATE {table_name} SET item_index = ? WHERE item_index = ?",
            ((i, j - l) for i, j in enumerate(indices_map)),
        )

    @classmethod
    def translate_index(cls, table_name: str, cur: sqlite3.Cursor, index_from: int = 0) -> None:
        l = cls.get_max_index_plus_one(table_name, cur)
        cur.execute(f"UPDATE {table_name} SET item_index = item_index - {l} WHERE item_index >= ?", (index_from,))

    @classmethod
    def undo_translate_index(cls, table_name: str, cur: sqlite3.Cursor) -> None:
        sz = cls.get_count(table_name, cur)
        cur.execute(f"UPDATE {table_name} SET item_index = {sz} + item_index WHERE 0 > item_index")

    @classmethod
    def get_count(cls, table_name: str, cur: sqlite3.Cursor) -> int:
        cur.execute(f"SELECT COUNT(1) FROM {table_name}")
        return cast(int, list(cur)[0][0])

    @classmethod
    def iter_serialized_value(cls, table_name: str, cur: sqlite3.Cursor) -> Iterable[bytes]:
        cur.execute(f"SELECT serialized_value FROM {table_name} ORDER BY item_index")
        for d in cur:
            yield cast(bytes, d[0])

    @classmethod
    def get_index_by_serialized_value_in_range(
        cls, table_name: str, cur: sqlite3.Cursor, serialized_value: bytes, normalized_start: int, normalized_stop: int
    ) -> Union[None, int]:
        cur.execute(
            f"SELECT item_index FROM {table_name} WHERE serialized_value = ? AND item_index >= ? AND item_index < ?",
            (serialized_value, normalized_start, normalized_stop),
        )
        res = cur.fetchone()
        if res is None:
            return None
        return cast(int, res[0])

    @classmethod
    def count_serialized_value(cls, table_name: str, cur: sqlite3.Cursor, serialized_value: bytes) -> int:
        cur.execute(
            f"SELECT COUNT(*) FROM {table_name} WHERE serialized_value = ?",
            (serialized_value,),
        )
        res = cur.fetchone()
        return cast(int, res[0])

    @classmethod
    def increment_indices(cls, table_name: str, cur: sqlite3.Cursor, start: int) -> None:
        idx = cls.get_max_index_plus_one(table_name, cur) - 1
        while idx >= start:
            cur.execute(f"UPDATE {table_name} SET item_index = ? WHERE item_index = ?", (idx + 1, idx))
            idx -= 1

    @classmethod
    def reverse_indices(cls, table_name: str, cur: sqlite3.Cursor) -> None:
        l = cls.get_max_index_plus_one(table_name, cur)
        cur.execute(f"UPDATE {table_name} SET item_index = -1 - item_index")
        cur.execute(f"UPDATE {table_name} SET item_index = item_index + ?", (l,))

    @classmethod
    def dump_serialized_records(cls, table_name: str, cur: sqlite3.Cursor) -> Sequence[Tuple[bytes, int]]:
        cur.execute(f"SELECT serialized_value, item_index FROM {table_name}")
        return list(cur)

    @classmethod
    def load_serialized_records(
        cls, table_name: str, cur: sqlite3.Cursor, serialized_records: Iterable[Tuple[bytes, int]]
    ) -> None:
        for d in serialized_records:
            cur.execute(f"INSERT INTO {table_name} (serialized_value, item_index) VALUES (?, ?)", d)

    @classmethod
    def swap_indices(cls, table_name: str, cur: sqlite3.Cursor, idx1: int, idx2: int) -> None:
        cur.execute(f"UPDATE {table_name} SET item_index = -1 WHERE item_index = ?", (idx1,))
        cur.execute(f"UPDATE {table_name} SET item_index = ? WHERE item_index = ?", (idx1, idx2))
        cur.execute(f"UPDATE {table_name} SET item_index = ? WHERE item_index = -1", (idx2,))

    @classmethod
    def increase_indices_in_range(cls, table_name: str, cur: sqlite3.Cursor, start: int, end: int, value: int) -> None:
        cur.execute(
            f"UPDATE {table_name} SET item_index = item_index + ? WHERE item_index >= ? AND item_index < ?",
            (value, start, end),
        )

    @classmethod
    def set_index_by_index(cls, table_name: str, cur: sqlite3.Cursor, old_index: int, new_index: int) -> None:
        cur.execute(f"UPDATE {table_name} SET item_index = ? WHERE item_index = ?", (new_index, old_index))


class List(SqliteCollectionBase[T], MutableSequence[T]):
    _driver_class = _ListDatabaseDriver

    def __init__(
        self,
        __data: Optional[Iterable[T]] = None,
        connection: Optional[Union[str, sqlite3.Connection]] = None,
        table_name: Optional[str] = None,
        serializer: Optional[Callable[[T], bytes]] = None,
        deserializer: Optional[Callable[[bytes], T]] = None,
        persist: bool = True,
        pickling_strategy: PicklingStrategy = PicklingStrategy.whole_table,
        sorting_strategy: SortingStrategy = SortingStrategy.balanced,
    ) -> None:
        if (
            isinstance(__data, self.__class__)
            and __data.connection == connection
            and __data.serializer == serializer
            and __data.deserializer == deserializer
        ):
            super(List, self).__init__(
                connection=connection,
                table_name=table_name,
                serializer=serializer,
                deserializer=deserializer,
                persist=persist,
                pickling_strategy=pickling_strategy,
                reference_table_name=__data.table_name,
            )
        else:
            super(List, self).__init__(
                connection=connection,
                table_name=table_name,
                serializer=serializer,
                deserializer=deserializer,
                persist=persist,
                pickling_strategy=pickling_strategy,
            )
            if __data is not None:
                self.clear()
                self.extend(__data)
        self._sorting_strategy = sorting_strategy

    @property
    def sorting_strategy(self) -> SortingStrategy:
        if hasattr(self, "_sorting_strategy"):
            return self._sorting_strategy
        return SortingStrategy.balanced

    def __delitem__(self, i: Union[int, slice]) -> None:
        cur = self.connection.cursor()
        cur2 = self.connection.cursor()
        if isinstance(i, int):
            deleted_index = self._driver_class.delete_record_by_index(self.table_name, cur, i)
            if deleted_index is None:
                raise IndexError("list assignment index out of range")
            self._driver_class.tidy_indices(self.table_name, cur, cur2, deleted_index)
            self.connection.commit()
            return
        reindexing_offset = None
        l = self._driver_class.get_max_index_plus_one(self.table_name, cur)
        for idx in _generate_indices_from_slice(l, i):
            self._driver_class.delete_record_by_index(self.table_name, cur, idx, l)
            if reindexing_offset is None or idx < reindexing_offset:
                reindexing_offset = idx
        if reindexing_offset is not None:
            self._driver_class.tidy_indices(self.table_name, cur, cur2, reindexing_offset)
        self.connection.commit()

    @overload
    def __getitem__(self, i: int) -> T:
        ...

    @overload
    def __getitem__(self, i: slice) -> "List[T]":
        ...

    def __getitem__(self, i: Union[int, slice]) -> "Union[T, List[T]]":
        cur = self.connection.cursor()
        if isinstance(i, int):
            serialized_value = self._driver_class.get_serialized_value_by_index(self.table_name, cur, i)
            if serialized_value is None:
                raise IndexError("list index out of range")
            return self.deserialize(serialized_value)
        l = self._driver_class.get_max_index_plus_one(self.table_name, cur)
        buf = self._create_volatile_copy([])
        bufcur = buf.connection.cursor()
        for next_idx, idx in enumerate(_generate_indices_from_slice(l, i)):
            buf._driver_class.add_record_by_serialized_value_and_index(
                buf.table_name,
                bufcur,
                cast(bytes, self._driver_class.get_serialized_value_by_index(self.table_name, cur, idx)),
                next_idx,
            )
        buf.connection.commit()
        return buf

    def _create_volatile_copy(self, data: Optional[Iterable[T]] = None) -> "List[T]":
        return List[T](
            (self if data is None else data),
            connection=self.connection,
            serializer=self.serializer,
            deserializer=self.deserializer,
            persist=False,
        )

    def copy(self) -> "List[T]":
        return self._create_volatile_copy()

    def __setitem__(self, i: Union[int, slice], v: Union[T, Iterable[T]]) -> None:
        cur = self.connection.cursor()
        if isinstance(i, int):
            if not self._driver_class.set_serialized_value_by_index(
                self.table_name, cur, self.serialize(cast(T, v)), i
            ):
                raise IndexError("list assignment index out of range")
            self.connection.commit()
            return
        if not isinstance(v, Iterable):
            raise TypeError("must assign iterable to extended slice")
        l = self._driver_class.get_max_index_plus_one(self.table_name, cur)
        if i.step is None or i.step == 1:
            offset = min(max(0 if i.start is None else (i.start if i.start >= 0 else l + i.start), 0), l)
            del self[i]
            self._driver_class.translate_index(self.table_name, cur, offset)
            for idx, d in enumerate(v):
                self._driver_class.add_record_by_serialized_value_and_index(
                    self.table_name, cur, self.serialize(d), offset + idx
                )
            self._driver_class.undo_translate_index(self.table_name, cur)
            self.connection.commit()
        else:
            try:
                for idx, d in _strict_zip(_generate_indices_from_slice(l, i), v):
                    self._driver_class.set_serialized_value_by_index(self.table_name, cur, self.serialize(d), idx)
            except DifferentLengthDetected as e:
                raise ValueError(
                    f"attempt to assign sequence of size {e.length2} to extended slice of size {e.length1}"
                )
            self.connection.commit()
        return

    def __len__(self) -> int:
        cur = self.connection.cursor()
        return self._driver_class.get_max_index_plus_one(self.table_name, cur)

    def insert(self, i: int, v: T) -> None:
        cur = self.connection.cursor()
        index_ = i
        length = self._driver_class.get_max_index_plus_one(self.table_name, cur)
        if index_ < 0:
            index_ = length + index_
        index_ = max(0, min(length, index_))
        self._driver_class.increment_indices(self.table_name, cur, index_)
        self._driver_class.add_record_by_serialized_value_and_index(self.table_name, cur, self.serialize(v), index_)
        self.connection.commit()

    def __contains__(self, x: object) -> bool:
        cur = self.connection.cursor()
        serialized_value = self.serialize(cast(T, x))
        index = self._driver_class.get_index_by_serialized_value(self.table_name, cur, serialized_value)
        return index != -1

    def append(self, value: T) -> None:
        cur = self.connection.cursor()
        length = self._driver_class.get_max_index_plus_one(self.table_name, cur)
        self._driver_class.add_record_by_serialized_value_and_index(self.table_name, cur, self.serialize(value), length)
        self.connection.commit()

    def clear(self) -> None:
        cur = self.connection.cursor()
        self._driver_class.delete_all(self.table_name, cur)
        self.connection.commit()

    def extend(self, values: Iterable[T]) -> None:
        cur = self.connection.cursor()
        idx = self._driver_class.get_max_index_plus_one(self.table_name, cur)
        for v in values:
            self._driver_class.add_record_by_serialized_value_and_index(self.table_name, cur, self.serialize(v), idx)
            idx += 1
        self.connection.commit()

    def __iadd__(self, x: Iterable[T]) -> "List[T]":
        self.extend(x)
        return self

    def __add__(self, x: Iterable[T]) -> "List[T]":
        res = self.copy()
        res += x
        return res

    def __imul__(self, i: int) -> "List[T]":
        if not isinstance(i, int):
            raise TypeError(f"can't multiply sequence by non-int of type '{type(i).__name__}'")
        if i <= 0:
            self.clear()
            return self
        if i == 1:
            return self
        cur = self.connection.cursor()
        original_length = self._driver_class.get_max_index_plus_one(self.table_name, cur)
        for m in range(1, i):
            for j in range(original_length):
                serialized_value = cast(
                    bytes, self._driver_class.get_serialized_value_by_index(self.table_name, cur, j)
                )
                self._driver_class.add_record_by_serialized_value_and_index(
                    self.table_name, cur, serialized_value, m * original_length + j
                )
        self.connection.commit()
        return self

    def __mul__(self, i: int) -> "List[T]":
        res = self.copy()
        res *= i
        return res

    def index(self, value: Any, start: int = 0, stop: int = 0) -> int:
        cur = self.connection.cursor()
        length = None
        start_ = start
        if start_ < 0:
            length = self._driver_class.get_max_index_plus_one(self.table_name, cur)
            start_ = length + start_
        stop_ = stop
        if stop_ <= 0:
            if length is None:
                length = self._driver_class.get_max_index_plus_one(self.table_name, cur)
            stop_ = length + stop_
        serialized_value = self.serialize(cast(T, value))
        res = self._driver_class.get_index_by_serialized_value_in_range(
            self.table_name, cur, serialized_value, start_, stop_
        )
        if res is None:
            raise ValueError(f"'{value}' is not in list")
        return res

    def count(self, value: Any) -> int:
        cur = self.connection.cursor()
        return self._driver_class.count_serialized_value(self.table_name, cur, self.serialize(cast(T, value)))

    def pop(self, index: int = -1) -> T:
        cur = self.connection.cursor()
        cur2 = self.connection.cursor()
        length = self._driver_class.get_max_index_plus_one(self.table_name, cur)
        if length == 0:
            raise IndexError("pop from empty list")
        index_ = index
        if index_ < 0:
            index_ = length + index_
        if index_ < 0 or length <= index_:
            raise IndexError("pop index out of range")
        serialized_value = cast(bytes, self._driver_class.get_serialized_value_by_index(self.table_name, cur, index_))
        self._driver_class.delete_record_by_index(self.table_name, cur, index_)
        self._driver_class.tidy_indices(self.table_name, cur, cur2, index_)
        self.connection.commit()
        return self.deserialize(serialized_value)

    def sort(self, reverse: bool = False, key: Optional[Callable[[T], Any]] = None) -> None:
        key_ = (lambda x: x) if key is None else key
        if self.sorting_strategy == SortingStrategy.fastest:
            self._sort_cached_keys(reverse=reverse, key=key_)
        elif self.sorting_strategy == SortingStrategy.memory_saving:
            self._merge_sort(reverse, key_, 0, len(self))
        else:
            self._sort_indices(reverse=reverse, key=key_)
        self.connection.commit()

    def _sort_indices(self, reverse: bool, key: Callable[[T], Any]) -> None:
        indices = list(range(len(self)))
        indices.sort(key=lambda i: key(self[i]), reverse=reverse)  # type: ignore
        self._driver_class.remap_index(self.table_name, self.connection.cursor(), indices)

    def _sort_cached_keys(self, reverse: bool, key: Callable[[T], Any]) -> None:
        key_cache = [(key(v), i) for i, v in enumerate(self)]
        key_cache.sort(key=lambda t: t[0], reverse=reverse)  # type: ignore
        self._driver_class.remap_index(self.table_name, self.connection.cursor(), [t[1] for t in key_cache])

    def _merge_sort(self, reverse: bool, key: Callable[[T], Any], idx0: int, idx1: int) -> None:
        sz = idx1 - idx0
        if sz <= 3:
            if sz == 3:
                self.__sort3(reverse, key, idx0)
            elif sz == 2:
                self.__sort2(reverse, key, idx0)
        else:
            cur = self.connection.cursor()
            mid = idx0 + math.ceil(sz / 2)
            self._merge_sort(reverse, key, idx0, mid)
            self._merge_sort(reverse, key, mid, idx1)
            l = len(self)
            self._driver_class.increase_indices_in_range(self.table_name, cur, idx0, mid, l)
            p = idx0
            i0 = idx0 + l
            i1 = mid
            k0 = key(self[i0])
            k1 = key(self[i1])
            if reverse:
                while True:
                    if k0 < k1:
                        self._driver_class.set_index_by_index(self.table_name, cur, i1, p)
                        p += 1
                        i1 += 1
                        if i1 == idx1:
                            self._driver_class.increase_indices_in_range(self.table_name, cur, i0, mid + l, p - i0)
                            break
                        k1 = key(self[i1])
                    else:
                        self._driver_class.set_index_by_index(self.table_name, cur, i0, p)
                        p += 1
                        i0 += 1
                        if i0 == mid + l:
                            break
                        k0 = key(self[i0])
            else:
                while True:
                    if k0 > k1:
                        self._driver_class.set_index_by_index(self.table_name, cur, i1, p)
                        p += 1
                        i1 += 1
                        if i1 == idx1:
                            self._driver_class.increase_indices_in_range(self.table_name, cur, i0, mid + l, p - i0)
                            break
                        k1 = key(self[i1])
                    else:
                        self._driver_class.set_index_by_index(self.table_name, cur, i0, p)
                        p += 1
                        i0 += 1
                        if i0 == mid + l:
                            break
                        k0 = key(self[i0])

    def __sort3(self, reverse: bool, key: Callable[[T], Any], idx: int) -> None:
        a = key(self[idx])
        b = key(self[idx + 1])
        c = key(self[idx + 2])
        cur = self.connection.cursor()
        if not reverse:
            if a > b:
                self._driver_class.swap_indices(self.table_name, cur, idx, idx + 1)
                a, b = b, a
            if a > c:
                self._driver_class.swap_indices(self.table_name, cur, idx, idx + 2)
                a, c = c, a
            if b > c:
                self._driver_class.swap_indices(self.table_name, cur, idx + 1, idx + 2)
        else:
            if a < b:
                self._driver_class.swap_indices(self.table_name, cur, idx, idx + 1)
                a, b = b, a
            if a < c:
                self._driver_class.swap_indices(self.table_name, cur, idx, idx + 2)
                a, c = c, a
            if b < c:
                self._driver_class.swap_indices(self.table_name, cur, idx + 1, idx + 2)

    def __sort2(self, reverse: bool, key: Callable[[T], Any], idx: int) -> None:
        a = key(self[idx])
        b = key(self[idx + 1])
        cur = self.connection.cursor()
        if (reverse and a < b) or (not reverse and a > b):
            self._driver_class.swap_indices(self.table_name, cur, idx, idx + 1)

    def reverse(self) -> None:
        cur = self.connection.cursor()
        self._driver_class.reverse_indices(self.table_name, cur)
        self.connection.commit()

    def remove(self, value: T) -> None:
        cur = self.connection.cursor()
        cur2 = self.connection.cursor()
        index = self._driver_class.get_index_by_serialized_value(self.table_name, cur, self.serialize(value))
        if index == -1:
            raise ValueError(f"'{value}' is not in list")
        self._driver_class.delete_record_by_index(self.table_name, cur, index)
        self._driver_class.tidy_indices(self.table_name, cur, cur2, index)
        self.connection.commit()
        return None

    def __getstate__(self) -> Mapping[str, Any]:
        state = self.__dict__.copy()
        del state["_connection"]
        cur = self.connection.cursor()
        if self.pickling_strategy == PicklingStrategy.whole_table:
            state["metadata"] = self._driver_class.dump_metadata_record_by_table_name(self.table_name, cur)
            state["records"] = self._driver_class.dump_serialized_records(self.table_name, cur)
        else:
            state["db_file_name"] = os.path.relpath(self._driver_class.get_db_filename(cur))
        return state

    def __setstate__(self, state: Mapping[str, Any]) -> None:
        if state["_pickling_strategy"] == PicklingStrategy.whole_table:
            self.__dict__.update(
                dict(
                    filter(lambda d: d[0] not in ("metadata", "records"), state.items()),
                    _connection=create_tempfile_connection(),
                )
            )
            cur = self._connection.cursor()
            self._driver_class.load_metadata_record(cur, state["metadata"])
            self._connection.commit()
            self._driver_class.load_serialized_records(self.table_name, cur, state["records"])
            self._connection.commit()
        else:
            self.__dict__.update(
                dict(
                    filter(lambda d: d[0] not in ("db_file_name",), state.items()),
                    _connection=tidy_connection(state["db_file_name"]),
                )
            )
