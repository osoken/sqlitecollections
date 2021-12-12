import gc
import os
import sys
from typing import Any

if sys.version_info > (3, 9):
    from collections.abc import MutableSet
else:
    from typing import MutableSet

from sqlitecollections import Set

from common import BenchmarkBase, Comparison

benchmarks_dir = os.path.dirname(os.path.abspath(__file__))

target_set_len = 100000
target_set = set(str(i) for i in range(target_set_len))
target_set_item_t = str
target_set_t = MutableSet[target_set_item_t]


class BuiltinSetBenchmarkBase:
    def __init__(self) -> None:
        super(BuiltinSetBenchmarkBase, self).__init__()
        self._sut_orig = target_set.copy()
        self._sut: target_set_t

    @property
    def name(self) -> str:
        return "`set`"

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


class SqliteCollectionsSetBenchmarkBase:
    def __init__(self) -> None:
        super(SqliteCollectionsSetBenchmarkBase, self).__init__()
        self._sut_orig = Set[target_set_item_t](data=target_set)
        self._sut: target_set_t

    @property
    def name(self) -> str:
        return "`sqlitecollections.set`"

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


class BenchmarkInitBase(BenchmarkBase[target_set_t]):
    def assertion(self, result: target_set_t) -> bool:
        return len(result) == len(target_set) and all(d in target_set for d in result)


class BenchmarkLenBase(BenchmarkBase[int]):
    def exec(self) -> int:
        return len(self._sut)

    def assertion(self, result: int) -> bool:
        return result == target_set_len


class BenchmarkContainsBase(BenchmarkBase[bool]):
    def exec(self) -> bool:
        self._sut: target_set_t
        return "651" in self._sut

    def assertion(self, result: bool) -> bool:
        return result


class BenchmarkNotContainsBase(BenchmarkBase[bool]):
    def exec(self) -> bool:
        self._sut: target_set_t
        return "-651" not in self._sut

    def assertion(self, result: bool) -> bool:
        return result


class BuiltinSetBenchmarkInit(BuiltinSetBenchmarkBase, BenchmarkInitBase):
    def exec(self) -> target_set_t:
        return set(s for s in target_set)


class SqliteCollectionsSetBenchmarkInit(SqliteCollectionsSetBenchmarkBase, BenchmarkInitBase):
    def exec(self) -> target_set_t:
        return Set[target_set_item_t](data=(s for s in target_set))


class BuiltinSetBenchmarkLen(BuiltinSetBenchmarkBase, BenchmarkLenBase):
    ...


class SqliteCollectionsSetBenchmarkLen(SqliteCollectionsSetBenchmarkBase, BenchmarkLenBase):
    ...


class BuiltinSetBenchmarkContains(BuiltinSetBenchmarkBase, BenchmarkContainsBase):
    ...


class SqliteCollectionsSetBenchmarkContains(SqliteCollectionsSetBenchmarkBase, BenchmarkContainsBase):
    ...


class BuiltinSetBenchmarkNotContains(BuiltinSetBenchmarkBase, BenchmarkNotContainsBase):
    ...


class SqliteCollectionsSetBenchmarkNotContains(SqliteCollectionsSetBenchmarkBase, BenchmarkNotContainsBase):
    ...


if __name__ == "__main__":
    print(Comparison("`__init__`", BuiltinSetBenchmarkInit(), SqliteCollectionsSetBenchmarkInit())().dict())
    print(Comparison("`__len__`", BuiltinSetBenchmarkLen(), SqliteCollectionsSetBenchmarkLen())().dict())
    print(Comparison("`__contains__`", BuiltinSetBenchmarkContains(), SqliteCollectionsSetBenchmarkContains())().dict())
    print(
        Comparison(
            "`__contains__` (unsuccessful search)",
            BuiltinSetBenchmarkNotContains(),
            SqliteCollectionsSetBenchmarkNotContains(),
        )().dict()
    )
