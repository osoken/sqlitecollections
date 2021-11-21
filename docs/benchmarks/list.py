import gc
import os
import random
import sys
from typing import Any, cast

if sys.version_info > (3, 9):
    from collections.abc import MutableSequence
else:
    from typing import MutableSequence

from sqlitecollections import List

from common import BenchmarkBase, Comparison

benchmarks_dir = os.path.dirname(os.path.abspath(__file__))

target_list_len = 100000
target_list = list([f"{i:016}" for i in range(target_list_len)])
target_list_element_t = str
target_list_t = MutableSequence[target_list_element_t]
random.seed(5432)
random.shuffle(target_list)


class BuiltinListBenchmarkBase:
    def __init__(self) -> None:
        super(BuiltinListBenchmarkBase, self).__init__()
        self._sut_orig = target_list.copy()
        self._sut: target_list_t

    @property
    def name(self) -> str:
        return "builtin_list"

    def setup(self) -> None:
        gc.collect()
        gc.collect()
        self._sut = self._sut_orig.copy()
        gc.collect()
        gc.collect()

    def teardown(self) -> None:
        del self._sut
        gc.collect()
        gc.collect()


class SqliteCollectionsListBenchmarkBase:
    def __init__(self) -> None:
        super(SqliteCollectionsListBenchmarkBase, self).__init__()
        self._sut_orig = List[target_list_element_t](data=target_list)
        self._sut: target_list_t

    @property
    def name(self) -> str:
        return "sqlitecollections_list"

    def setup(self) -> None:
        gc.collect()
        gc.collect()
        self._sut = self._sut_orig.copy()
        gc.collect()
        gc.collect()

    def teardown(self) -> None:
        del self._sut
        gc.collect()
        gc.collect()


class BenchmarkDelitemBase(BenchmarkBase[target_list_t]):
    def exec(self) -> target_list_t:
        self._sut: target_list_t
        del self._sut[5000]
        return self._sut

    def assertion(self, result: target_list_t) -> bool:
        return (
            len(result) == (target_list_len - 1)
            and result[4999] == target_list[4999]
            and result[5000] == target_list[5001]
        )


class BenchmarkGetitemBase(BenchmarkBase[target_list_element_t]):
    def exec(self) -> target_list_element_t:
        self._sut: target_list_t
        return self._sut[5000]

    def assertion(self, result: target_list_element_t) -> bool:
        return result == target_list[5000]


class BenchmarkGetitemSliceBase(BenchmarkBase[target_list_t]):
    def exec(self) -> target_list_t:
        self._sut: target_list_t
        return self._sut[10:5010]

    def assertion(self, result: target_list_t) -> bool:
        return all([a == b for a, b in zip(target_list[10:5010], result)])


class BenchmarkCreateWithInitialDataBase(BenchmarkBase[target_list_t]):
    def assertion(self, result: target_list_t) -> bool:
        return len(result) == target_list_len and all(a == b for a, b in zip(result, target_list))


class BenchmarkContainsBase(BenchmarkBase[bool]):
    def exec(self) -> bool:
        self._sut: target_list_t
        return f"{651:016}" in self._sut

    def assertion(self, result: bool) -> bool:
        return result == True


class BenchmarkNotContainsBase(BenchmarkBase[bool]):
    def exec(self) -> bool:
        self._sut: target_list_t
        return f"{(target_list_len + 123):016}" in self._sut

    def assertion(self, result: bool) -> bool:
        return result == False


class BenchmarkSetitemBase(BenchmarkBase[target_list_t]):
    def exec(self) -> target_list_t:
        self._sut: target_list_t
        self._sut[0] = "-123"
        self._sut[1000] = "-2"
        self._sut[-3] = "-10"
        return self._sut

    def assertion(self, result: target_list_t) -> bool:
        return result[0] == "-123" and result[1000] == "-2" and result[-3] == "-10"


class BenchmarkInsertBase(BenchmarkBase[target_list_t]):
    def exec(self) -> target_list_t:
        self._sut: target_list_t
        self._sut.insert(0, "-123")
        return self._sut

    def assertion(self, result: target_list_t) -> bool:
        return len(result) == (target_list_len + 1) and result[0] == "-123" and result[1] == target_list[0]


class BenchmarkAppendBase(BenchmarkBase[target_list_t]):
    def exec(self) -> target_list_t:
        self._sut: target_list_t
        self._sut.append("-123")
        return self._sut

    def assertion(self, result: target_list_t) -> bool:
        return len(result) == (target_list_len + 1) and result[target_list_len] == "-123"


class BenchmarkClearBase(BenchmarkBase[target_list_t]):
    def exec(self) -> target_list_t:
        self._sut: target_list_t
        self._sut.clear()
        return self._sut

    def assertion(self, result: target_list_t) -> bool:
        return len(result) == 0


class BenchmarkExtendBase(BenchmarkBase[target_list_t]):
    def exec(self) -> target_list_t:
        self._sut: target_list_t
        self._sut.extend(["-1", "-2", "-3"])
        return self._sut

    def assertion(self, result: target_list_t) -> bool:
        return (
            len(result) == (target_list_len + 3)
            and result[target_list_len - 1] == target_list[target_list_len - 1]
            and result[target_list_len] == "-1"
        )


class BenchmarkCopyBase(BenchmarkBase[target_list_t]):
    def exec(self) -> target_list_t:
        self._sut: target_list_t
        retval = self._sut.copy()
        return retval

    def assertion(self, result: target_list_t) -> bool:
        return all([a == b for a, b in zip(result, target_list)])


class BuiltinListBenchmarkDelitem(BuiltinListBenchmarkBase, BenchmarkDelitemBase):
    pass


class SqliteCollectionsListBenchmarkDelitem(SqliteCollectionsListBenchmarkBase, BenchmarkDelitemBase):
    pass


class BuiltinListBenchmarkGetitem(BuiltinListBenchmarkBase, BenchmarkGetitemBase):
    pass


class SqliteCollectionsListBenchmarkGetitem(SqliteCollectionsListBenchmarkBase, BenchmarkGetitemBase):
    pass


class BuiltinListBenchmarkGetitemSlice(BuiltinListBenchmarkBase, BenchmarkGetitemSliceBase):
    pass


class SqliteCollectionsListBenchmarkGetitemSlice(SqliteCollectionsListBenchmarkBase, BenchmarkGetitemSliceBase):
    pass


class BuiltinListBenchmarkCreateWithInitialData(BuiltinListBenchmarkBase, BenchmarkCreateWithInitialDataBase):
    def exec(self) -> Any:
        return list(iter(target_list))


class SqliteCollectionsListBenchmarkCreateWithInitialData(BuiltinListBenchmarkBase, BenchmarkCreateWithInitialDataBase):
    def exec(self) -> Any:
        return List[target_list_element_t](data=iter(target_list))


class BuiltinListBenchmarkContains(BuiltinListBenchmarkBase, BenchmarkContainsBase):
    pass


class BuiltinListBenchmarkNotContains(BuiltinListBenchmarkBase, BenchmarkNotContainsBase):
    pass


class SqliteCollectionsListBenchmarkContains(SqliteCollectionsListBenchmarkBase, BenchmarkContainsBase):
    pass


class SqliteCollectionsListBenchmarkNotContains(SqliteCollectionsListBenchmarkBase, BenchmarkNotContainsBase):
    pass


class BuiltinListBenchmarkSetitem(BuiltinListBenchmarkBase, BenchmarkSetitemBase):
    pass


class SqliteCollectionsListBenchmarkSetitem(SqliteCollectionsListBenchmarkBase, BenchmarkSetitemBase):
    pass


class BuiltinListBenchmarkInsert(BuiltinListBenchmarkBase, BenchmarkInsertBase):
    pass


class SqliteCollectionsListBenchmarkInsert(SqliteCollectionsListBenchmarkBase, BenchmarkInsertBase):
    pass


class BuiltinListBenchmarkAppend(BuiltinListBenchmarkBase, BenchmarkAppendBase):
    pass


class SqliteCollectionsListBenchmarkAppend(SqliteCollectionsListBenchmarkBase, BenchmarkAppendBase):
    pass


class BuiltinListBenchmarkClear(BuiltinListBenchmarkBase, BenchmarkClearBase):
    pass


class SqliteCollectionsListBenchmarkClear(SqliteCollectionsListBenchmarkBase, BenchmarkClearBase):
    pass


class BuiltinListBenchmarkExtend(BuiltinListBenchmarkBase, BenchmarkExtendBase):
    pass


class SqliteCollectionsListBenchmarkExtend(SqliteCollectionsListBenchmarkBase, BenchmarkExtendBase):
    pass


class BuiltinListBenchmarkCopy(BuiltinListBenchmarkBase, BenchmarkCopyBase):
    pass


class SqliteCollectionsListBenchmarkCopy(SqliteCollectionsListBenchmarkBase, BenchmarkCopyBase):
    pass


if __name__ == "__main__":
    print(Comparison("__delitem__", BuiltinListBenchmarkDelitem(), SqliteCollectionsListBenchmarkDelitem())().dict())
    print(Comparison("__getitem__", BuiltinListBenchmarkGetitem(), SqliteCollectionsListBenchmarkGetitem())().dict())
    print(
        Comparison(
            "__getitem__ (slice)", BuiltinListBenchmarkGetitemSlice(), SqliteCollectionsListBenchmarkGetitemSlice()
        )().dict()
    )
    print(
        Comparison(
            "__init__",
            BuiltinListBenchmarkCreateWithInitialData(),
            SqliteCollectionsListBenchmarkCreateWithInitialData(),
        )().dict()
    )
    print(Comparison("__contains__", BuiltinListBenchmarkContains(), SqliteCollectionsListBenchmarkContains())().dict())
    print(
        Comparison(
            "__contains__ (not)", BuiltinListBenchmarkNotContains(), SqliteCollectionsListBenchmarkNotContains()
        )().dict()
    )
    print(Comparison("__setitem__", BuiltinListBenchmarkSetitem(), SqliteCollectionsListBenchmarkSetitem())().dict())
    print(Comparison("insert", BuiltinListBenchmarkInsert(), SqliteCollectionsListBenchmarkInsert())().dict())
    print(Comparison("append", BuiltinListBenchmarkAppend(), SqliteCollectionsListBenchmarkAppend())().dict())
    print(Comparison("clear", BuiltinListBenchmarkClear(), SqliteCollectionsListBenchmarkClear())().dict())
    print(Comparison("extend", BuiltinListBenchmarkExtend(), SqliteCollectionsListBenchmarkExtend())().dict())
    print(Comparison("copy", BuiltinListBenchmarkCopy(), SqliteCollectionsListBenchmarkCopy())().dict())
