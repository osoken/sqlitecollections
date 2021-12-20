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
larger_set_diff = 100
smaller_set_diff = -99900
target_set = set(str(i) for i in range(target_set_len))
larger_set = set(str(i) for i in range(target_set_len + larger_set_diff))
smaller_set = set(str(i) for i in range(target_set_len + smaller_set_diff))
larger_target_diff = set(str(i) for i in range(target_set_len, target_set_len + larger_set_diff))
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


class BenchmarkIsdisjointBase(BenchmarkBase[bool]):
    def exec(self) -> bool:
        self._sut: target_set_t
        return self._sut.isdisjoint({"-1"})

    def assertion(self, result: bool) -> bool:
        return result


class BenchmarkIsdisjointNotBase(BenchmarkBase[bool]):
    def exec(self) -> bool:
        self._sut: target_set_t
        return self._sut.isdisjoint({"1"})

    def assertion(self, result: bool) -> bool:
        return not result


class BenchmarkIssubsetBase(BenchmarkBase[bool]):
    def exec(self) -> bool:
        self._sut: target_set_t
        return self._sut.issubset(iter(target_set))

    def assertion(self, result: bool) -> bool:
        return result


class BenchmarkIssubsetNotBase(BenchmarkBase[bool]):
    def exec(self) -> bool:
        self._sut: target_set_t
        return self._sut.issubset(iter([]))

    def assertion(self, result: bool) -> bool:
        return not result


class BenchmarkLeBase(BenchmarkBase[bool]):
    def exec(self) -> bool:
        self._sut: target_set_t
        return self._sut <= target_set

    def assertion(self, result: bool) -> bool:
        return result


class BenchmarkLeNotBase(BenchmarkBase[bool]):
    def exec(self) -> bool:
        self._sut: target_set_t
        return self._sut <= set()

    def assertion(self, result: bool) -> bool:
        return not result


class BenchmarkLtBase(BenchmarkBase[bool]):
    def exec(self) -> bool:
        self._sut: target_set_t
        return self._sut < larger_set

    def assertion(self, result: bool) -> bool:
        return result


class BenchmarkLtNotBase(BenchmarkBase[bool]):
    def exec(self) -> bool:
        self._sut: target_set_t
        return self._sut < target_set

    def assertion(self, result: bool) -> bool:
        return not result


class BenchmarkIssupersetBase(BenchmarkBase[bool]):
    def exec(self) -> bool:
        self._sut: target_set_t
        return self._sut.issuperset(iter(target_set))

    def assertion(self, result: bool) -> bool:
        return result


class BenchmarkIssupersetNotBase(BenchmarkBase[bool]):
    def exec(self) -> bool:
        self._sut: target_set_t
        return self._sut.issuperset(iter(larger_set))

    def assertion(self, result: bool) -> bool:
        return not result


class BenchmarkGeBase(BenchmarkBase[bool]):
    def exec(self) -> bool:
        self._sut: target_set_t
        return self._sut >= set()

    def assertion(self, result: bool) -> bool:
        return result


class BenchmarkGeNotBase(BenchmarkBase[bool]):
    def exec(self) -> bool:
        self._sut: target_set_t
        return self._sut >= larger_set

    def assertion(self, result: bool) -> bool:
        return not result


class BenchmarkGtBase(BenchmarkBase[bool]):
    def exec(self) -> bool:
        self._sut: target_set_t
        return self._sut > smaller_set

    def assertion(self, result: bool) -> bool:
        return result


class BenchmarkGtNotBase(BenchmarkBase[bool]):
    def exec(self) -> bool:
        self._sut: target_set_t
        return self._sut > target_set

    def assertion(self, result: bool) -> bool:
        return not result


class BenchmarkUnionBase(BenchmarkBase[target_set_t]):
    def exec(self) -> target_set_t:
        self._sut: target_set_t
        return self._sut.union(iter(larger_target_diff))

    def assertion(self, result: target_set_t):
        return result == larger_set


class BenchmarkOrBase(BenchmarkBase[target_set_t]):
    def exec(self) -> target_set_t:
        self._sut: target_set_t
        return self._sut | larger_target_diff

    def assertion(self, result: target_set_t):
        return result == larger_set


class BenchmarkIntersectionBase(BenchmarkBase[target_set_t]):
    def exec(self) -> target_set_t:
        self._sut: target_set_t
        return self._sut.intersection(iter(smaller_set))

    def assertion(self, result: target_set_t):
        return result == smaller_set


class BenchmarkAndBase(BenchmarkBase[target_set_t]):
    def exec(self) -> target_set_t:
        self._sut: target_set_t
        return self._sut & smaller_set

    def assertion(self, result: target_set_t):
        return result == smaller_set


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


class BuiltinSetBenchmarkIsdisjoint(BuiltinSetBenchmarkBase, BenchmarkIsdisjointBase):
    ...


class SqliteCollectionsSetBenchmarkIsdisjoint(SqliteCollectionsSetBenchmarkBase, BenchmarkIsdisjointBase):
    ...


class BuiltinSetBenchmarkIsdisjointNot(BuiltinSetBenchmarkBase, BenchmarkIsdisjointNotBase):
    ...


class SqliteCollectionsSetBenchmarkIsdisjointNot(SqliteCollectionsSetBenchmarkBase, BenchmarkIsdisjointNotBase):
    ...


class BuiltinSetBenchmarkIssubset(BuiltinSetBenchmarkBase, BenchmarkIssubsetBase):
    ...


class SqliteCollectionsSetBenchmarkIssubset(SqliteCollectionsSetBenchmarkBase, BenchmarkIssubsetBase):
    ...


class BuiltinSetBenchmarkIssubsetNot(BuiltinSetBenchmarkBase, BenchmarkIssubsetNotBase):
    ...


class SqliteCollectionsSetBenchmarkIssubsetNot(SqliteCollectionsSetBenchmarkBase, BenchmarkIssubsetNotBase):
    ...


class BuiltinSetBenchmarkLe(BuiltinSetBenchmarkBase, BenchmarkLeBase):
    ...


class SqliteCollectionsSetBenchmarkLe(SqliteCollectionsSetBenchmarkBase, BenchmarkLeBase):
    ...


class BuiltinSetBenchmarkLeNot(BuiltinSetBenchmarkBase, BenchmarkLeNotBase):
    ...


class SqliteCollectionsSetBenchmarkLeNot(SqliteCollectionsSetBenchmarkBase, BenchmarkLeNotBase):
    ...


class BuiltinSetBenchmarkLt(BuiltinSetBenchmarkBase, BenchmarkLtBase):
    ...


class SqliteCollectionsSetBenchmarkLt(SqliteCollectionsSetBenchmarkBase, BenchmarkLtBase):
    ...


class BuiltinSetBenchmarkLtNot(BuiltinSetBenchmarkBase, BenchmarkLtNotBase):
    ...


class SqliteCollectionsSetBenchmarkLtNot(SqliteCollectionsSetBenchmarkBase, BenchmarkLtNotBase):
    ...


class BuiltinSetBenchmarkIssuperset(BuiltinSetBenchmarkBase, BenchmarkIssupersetBase):
    ...


class SqliteCollectionsSetBenchmarkIssuperset(SqliteCollectionsSetBenchmarkBase, BenchmarkIssupersetBase):
    ...


class BuiltinSetBenchmarkIssupersetNot(BuiltinSetBenchmarkBase, BenchmarkIssupersetNotBase):
    ...


class SqliteCollectionsSetBenchmarkIssupersetNot(SqliteCollectionsSetBenchmarkBase, BenchmarkIssupersetNotBase):
    ...


class BuiltinSetBenchmarkGe(BuiltinSetBenchmarkBase, BenchmarkGeBase):
    ...


class SqliteCollectionsSetBenchmarkGe(SqliteCollectionsSetBenchmarkBase, BenchmarkGeBase):
    ...


class BuiltinSetBenchmarkGeNot(BuiltinSetBenchmarkBase, BenchmarkGeNotBase):
    ...


class SqliteCollectionsSetBenchmarkGeNot(SqliteCollectionsSetBenchmarkBase, BenchmarkGeNotBase):
    ...


class BuiltinSetBenchmarkGt(BuiltinSetBenchmarkBase, BenchmarkGtBase):
    ...


class SqliteCollectionsSetBenchmarkGt(SqliteCollectionsSetBenchmarkBase, BenchmarkGtBase):
    ...


class BuiltinSetBenchmarkGtNot(BuiltinSetBenchmarkBase, BenchmarkGtNotBase):
    ...


class SqliteCollectionsSetBenchmarkGtNot(SqliteCollectionsSetBenchmarkBase, BenchmarkGtNotBase):
    ...


class BuiltinSetBenchmarkUnion(BuiltinSetBenchmarkBase, BenchmarkUnionBase):
    ...


class SqliteCollectionsSetBenchmarkUnion(SqliteCollectionsSetBenchmarkBase, BenchmarkUnionBase):
    ...


class BuiltinSetBenchmarkOr(BuiltinSetBenchmarkBase, BenchmarkOrBase):
    ...


class SqliteCollectionsSetBenchmarkOr(SqliteCollectionsSetBenchmarkBase, BenchmarkOrBase):
    ...


class BuiltinSetBenchmarkIntersection(BuiltinSetBenchmarkBase, BenchmarkIntersectionBase):
    ...


class SqliteCollectionsSetBenchmarkIntersection(SqliteCollectionsSetBenchmarkBase, BenchmarkIntersectionBase):
    ...


class BuiltinSetBenchmarkAnd(BuiltinSetBenchmarkBase, BenchmarkAndBase):
    ...


class SqliteCollectionsSetBenchmarkAnd(SqliteCollectionsSetBenchmarkBase, BenchmarkAndBase):
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
    print(
        Comparison("`isdisjoint`", BuiltinSetBenchmarkIsdisjoint(), SqliteCollectionsSetBenchmarkIsdisjoint())().dict()
    )
    print(
        Comparison(
            "`isdisjoint` (not disjoint)",
            BuiltinSetBenchmarkIsdisjointNot(),
            SqliteCollectionsSetBenchmarkIsdisjointNot(),
        )().dict()
    )
    print(Comparison("`issubset`", BuiltinSetBenchmarkIssubset(), SqliteCollectionsSetBenchmarkIssubset())().dict())
    print(
        Comparison(
            "`issubset` (not subset)",
            BuiltinSetBenchmarkIssubsetNot(),
            SqliteCollectionsSetBenchmarkIssubsetNot(),
        )().dict()
    )
    print(Comparison("`__le__`", BuiltinSetBenchmarkLe(), SqliteCollectionsSetBenchmarkLe())().dict())
    print(
        Comparison(
            "`__le__` (not less than or equals to)",
            BuiltinSetBenchmarkLeNot(),
            SqliteCollectionsSetBenchmarkLeNot(),
        )().dict()
    )
    print(Comparison("`__lt__`", BuiltinSetBenchmarkLt(), SqliteCollectionsSetBenchmarkLt())().dict())
    print(
        Comparison(
            "`__lt__` (not less than)",
            BuiltinSetBenchmarkLtNot(),
            SqliteCollectionsSetBenchmarkLtNot(),
        )().dict()
    )
    print(
        Comparison("`issuperset`", BuiltinSetBenchmarkIssuperset(), SqliteCollectionsSetBenchmarkIssuperset())().dict()
    )
    print(
        Comparison(
            "`issuperset` (not superset)",
            BuiltinSetBenchmarkIssupersetNot(),
            SqliteCollectionsSetBenchmarkIssupersetNot(),
        )().dict()
    )
    print(Comparison("`__ge__`", BuiltinSetBenchmarkGe(), SqliteCollectionsSetBenchmarkGe())().dict())
    print(
        Comparison(
            "`__ge__` (not greater than or equals to)",
            BuiltinSetBenchmarkGeNot(),
            SqliteCollectionsSetBenchmarkGeNot(),
        )().dict()
    )
    print(Comparison("`__gt__`", BuiltinSetBenchmarkGt(), SqliteCollectionsSetBenchmarkGt())().dict())
    print(
        Comparison(
            "`__gt__` (not greater than)",
            BuiltinSetBenchmarkGtNot(),
            SqliteCollectionsSetBenchmarkGtNot(),
        )().dict()
    )
    print(Comparison("`union`", BuiltinSetBenchmarkUnion(), SqliteCollectionsSetBenchmarkUnion())().dict())
    print(Comparison("`__or__`", BuiltinSetBenchmarkOr(), SqliteCollectionsSetBenchmarkOr())().dict())
    print(
        Comparison(
            "`intersection`", BuiltinSetBenchmarkIntersection(), SqliteCollectionsSetBenchmarkIntersection()
        )().dict()
    )
    print(Comparison("`__and__`", BuiltinSetBenchmarkAnd(), SqliteCollectionsSetBenchmarkAnd())().dict())
