import gc
import os
import sys
from typing import Any, Optional

if sys.version_info >= (3, 9):
    from collections.abc import MutableSet
else:
    from typing import MutableSet

from typing import Tuple

import sqlitecollections as sc

from .common import BenchmarkBase, Comparison

benchmarks_dir = os.path.dirname(os.path.abspath(__file__))

target_set_len = 500
larger_set_diff = 50
smaller_set_diff = -950
target_set = set(str(i) for i in range(target_set_len))
larger_set = set(str(i) for i in range(target_set_len + larger_set_diff))
smaller_set = set(str(i) for i in range(target_set_len + smaller_set_diff))
larger_target_diff = set(str(i) for i in range(target_set_len, target_set_len + larger_set_diff))
target_set_item_t = str
target_set_t = MutableSet[target_set_item_t]


class BuiltinSetBenchmarkBase:
    def __init__(self, timeout: Optional[float] = None, debug: bool = False) -> None:
        super(BuiltinSetBenchmarkBase, self).__init__(timeout=timeout, debug=debug)
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
    def __init__(self, timeout: Optional[float] = None, debug: bool = False) -> None:
        super(SqliteCollectionsSetBenchmarkBase, self).__init__(timeout=timeout, debug=debug)
        self._sut_orig = sc.Set[target_set_item_t](data=target_set)
        self._sut: target_set_t

    @property
    def name(self) -> str:
        return "`sqlitecollections.Set`"

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
    @property
    def subject(self) -> str:
        return "`__init__`"

    def assertion(self, result: target_set_t) -> bool:
        return len(result) == len(target_set) and all(d in target_set for d in result)


class BenchmarkLenBase(BenchmarkBase[int]):
    @property
    def subject(self) -> str:
        return "`__len__`"

    def exec(self) -> int:
        return len(self._sut)

    def assertion(self, result: int) -> bool:
        return result == target_set_len


class BenchmarkContainsBase(BenchmarkBase[bool]):
    @property
    def subject(self) -> str:
        return "`__contains__`"

    def exec(self) -> bool:
        self._sut: target_set_t
        return "51" in self._sut

    def assertion(self, result: bool) -> bool:
        return result


class BenchmarkNotContainsBase(BenchmarkBase[bool]):
    @property
    def subject(self) -> str:
        return "`__contains__` (unsuccessful search)"

    def exec(self) -> bool:
        self._sut: target_set_t
        return "-51" not in self._sut

    def assertion(self, result: bool) -> bool:
        return result


class BenchmarkIsdisjointBase(BenchmarkBase[bool]):
    @property
    def subject(self) -> str:
        return "`isdisjoint`"

    def exec(self) -> bool:
        self._sut: target_set_t
        return self._sut.isdisjoint({"-1"})

    def assertion(self, result: bool) -> bool:
        return result


class BenchmarkIsdisjointNotBase(BenchmarkBase[bool]):
    @property
    def subject(self) -> str:
        return "`isdisjoint` (not disjoint)"

    def exec(self) -> bool:
        self._sut: target_set_t
        return self._sut.isdisjoint({"1"})

    def assertion(self, result: bool) -> bool:
        return not result


class BenchmarkIssubsetBase(BenchmarkBase[bool]):
    @property
    def subject(self) -> str:
        return "`issubset`"

    def exec(self) -> bool:
        self._sut: target_set_t
        return self._sut.issubset(iter(target_set))

    def assertion(self, result: bool) -> bool:
        return result


class BenchmarkIssubsetNotBase(BenchmarkBase[bool]):
    @property
    def subject(self) -> str:
        return "`issubset` (not subset)"

    def exec(self) -> bool:
        self._sut: target_set_t
        return self._sut.issubset(iter([]))

    def assertion(self, result: bool) -> bool:
        return not result


class BenchmarkLeBase(BenchmarkBase[bool]):
    @property
    def subject(self) -> str:
        return "`__le__`"

    def exec(self) -> bool:
        self._sut: target_set_t
        return self._sut <= target_set

    def assertion(self, result: bool) -> bool:
        return result


class BenchmarkLeNotBase(BenchmarkBase[bool]):
    @property
    def subject(self) -> str:
        return "`__le__` (not less than or equals to)"

    def exec(self) -> bool:
        self._sut: target_set_t
        return self._sut <= set()

    def assertion(self, result: bool) -> bool:
        return not result


class BenchmarkLtBase(BenchmarkBase[bool]):
    @property
    def subject(self) -> str:
        return "`__lt__`"

    def exec(self) -> bool:
        self._sut: target_set_t
        return self._sut < larger_set

    def assertion(self, result: bool) -> bool:
        return result


class BenchmarkLtNotBase(BenchmarkBase[bool]):
    @property
    def subject(self) -> str:
        return "`__lt__` (not less than)"

    def exec(self) -> bool:
        self._sut: target_set_t
        return self._sut < target_set

    def assertion(self, result: bool) -> bool:
        return not result


class BenchmarkIssupersetBase(BenchmarkBase[bool]):
    @property
    def subject(self) -> str:
        return "`issuperset`"

    def exec(self) -> bool:
        self._sut: target_set_t
        return self._sut.issuperset(iter(target_set))

    def assertion(self, result: bool) -> bool:
        return result


class BenchmarkIssupersetNotBase(BenchmarkBase[bool]):
    @property
    def subject(self) -> str:
        return "`issuperset` (not superset)"

    def exec(self) -> bool:
        self._sut: target_set_t
        return self._sut.issuperset(iter(larger_set))

    def assertion(self, result: bool) -> bool:
        return not result


class BenchmarkGeBase(BenchmarkBase[bool]):
    @property
    def subject(self) -> str:
        return "`__ge__`"

    def exec(self) -> bool:
        self._sut: target_set_t
        return self._sut >= set()

    def assertion(self, result: bool) -> bool:
        return result


class BenchmarkGeNotBase(BenchmarkBase[bool]):
    @property
    def subject(self) -> str:
        return "`__ge__` (not greater than or equals to)"

    def exec(self) -> bool:
        self._sut: target_set_t
        return self._sut >= larger_set

    def assertion(self, result: bool) -> bool:
        return not result


class BenchmarkGtBase(BenchmarkBase[bool]):
    @property
    def subject(self) -> str:
        return "`__gt__`"

    def exec(self) -> bool:
        self._sut: target_set_t
        return self._sut > smaller_set

    def assertion(self, result: bool) -> bool:
        return result


class BenchmarkGtNotBase(BenchmarkBase[bool]):
    @property
    def subject(self) -> str:
        return "`__gt__` (not greater than)"

    def exec(self) -> bool:
        self._sut: target_set_t
        return self._sut > target_set

    def assertion(self, result: bool) -> bool:
        return not result


class BenchmarkUnionBase(BenchmarkBase[target_set_t]):
    @property
    def subject(self) -> str:
        return "`union`"

    def exec(self) -> target_set_t:
        self._sut: target_set_t
        return self._sut.union(iter(larger_target_diff))

    def assertion(self, result: target_set_t):
        return result == larger_set


class BenchmarkOrBase(BenchmarkBase[target_set_t]):
    @property
    def subject(self) -> str:
        return "`__or__`"

    def exec(self) -> target_set_t:
        self._sut: target_set_t
        return self._sut | larger_target_diff

    def assertion(self, result: target_set_t):
        return result == larger_set


class BenchmarkIntersectionBase(BenchmarkBase[target_set_t]):
    @property
    def subject(self) -> str:
        return "`intersection`"

    def exec(self) -> target_set_t:
        self._sut: target_set_t
        return self._sut.intersection(iter(smaller_set))

    def assertion(self, result: target_set_t):
        return result == smaller_set


class BenchmarkAndBase(BenchmarkBase[target_set_t]):
    @property
    def subject(self) -> str:
        return "`__and__`"

    def exec(self) -> target_set_t:
        self._sut: target_set_t
        return self._sut & smaller_set

    def assertion(self, result: target_set_t):
        return result == smaller_set


class BenchmarkDifferenceBase(BenchmarkBase[target_set_t]):
    @property
    def subject(self) -> str:
        return "`difference`"

    def exec(self) -> target_set_t:
        self._sut: target_set_t
        return self._sut.difference(iter(smaller_set))

    def assertion(self, result: target_set_t) -> bool:
        return len(result) == (len(target_set) - len(smaller_set)) and all(d not in smaller_set for d in result)


class BenchmarkSubBase(BenchmarkBase[target_set_t]):
    @property
    def subject(self) -> str:
        return "`__sub__`"

    def exec(self) -> target_set_t:
        self._sut: target_set_t
        return self._sut - smaller_set

    def assertion(self, result: target_set_t) -> bool:
        return len(result) == (len(target_set) - len(smaller_set)) and all(d not in smaller_set for d in result)


class BenchmarkSymmetricDifferenceBase(BenchmarkBase[target_set_t]):
    @property
    def subject(self) -> str:
        return "`symmetric_difference`"

    def exec(self) -> target_set_t:
        self._sut: target_set_t
        return self._sut.symmetric_difference(iter(larger_set))

    def assertion(self, result: target_set_t) -> bool:
        return result == larger_target_diff


class BenchmarkXorBase(BenchmarkBase[target_set_t]):
    @property
    def subject(self) -> str:
        return "`__xor__`"

    def exec(self) -> target_set_t:
        self._sut: target_set_t
        return self._sut ^ larger_set

    def assertion(self, result: target_set_t) -> bool:
        return result == larger_target_diff


class BenchmarkCopyBase(BenchmarkBase[target_set_t]):
    @property
    def subject(self) -> str:
        return "`copy`"

    def exec(self) -> target_set_t:
        self._sut: target_set_t
        return self._sut.copy()

    def assertion(self, result: target_set_t) -> bool:
        return result == target_set


class BenchmarkUpdateBase(BenchmarkBase[target_set_t]):
    @property
    def subject(self) -> str:
        return "`update`"

    def exec(self) -> target_set_t:
        self._sut: target_set_t
        self._sut.update(larger_target_diff)
        return self._sut

    def assertion(self, result: target_set_t) -> bool:
        return result == larger_set


class BenchmarkIorBase(BenchmarkBase[target_set_t]):
    @property
    def subject(self) -> str:
        return "`__ior__`"

    def exec(self) -> target_set_t:
        self._sut: target_set_t
        self._sut |= larger_target_diff
        return self._sut

    def assertion(self, result: target_set_t) -> bool:
        return result == larger_set


class BenchmarkIntersectionUpdateBase(BenchmarkBase[target_set_t]):
    @property
    def subject(self) -> str:
        return "`intersection_update`"

    def exec(self) -> target_set_t:
        self._sut: target_set_t
        self._sut.intersection_update(smaller_set)
        return self._sut

    def assertion(self, result: target_set_t) -> bool:
        return result == smaller_set


class BenchmarkIandBase(BenchmarkBase[target_set_t]):
    @property
    def subject(self) -> str:
        return "`__iand__`"

    def exec(self) -> target_set_t:
        self._sut: target_set_t
        self._sut &= smaller_set
        return self._sut

    def assertion(self, result: target_set_t) -> bool:
        return result == smaller_set


class BenchmarkSymmetricDifferenceUpdateBase(BenchmarkBase[target_set_t]):
    @property
    def subject(self) -> str:
        return "`symmetric_difference_update`"

    def exec(self) -> target_set_t:
        self._sut: target_set_t
        self._sut.symmetric_difference_update(larger_set)
        return self._sut

    def assertion(self, result: target_set_t) -> bool:
        return result == larger_target_diff


class BenchmarkIxorBase(BenchmarkBase[target_set_t]):
    @property
    def subject(self) -> str:
        return "`__ixor__`"

    def exec(self) -> target_set_t:
        self._sut: target_set_t
        self._sut ^= larger_set
        return self._sut

    def assertion(self, result: target_set_t) -> bool:
        return result == larger_target_diff


class BenchmarkAddExistingItemBase(BenchmarkBase[target_set_t]):
    @property
    def subject(self) -> str:
        return "`add (existing item)`"

    def exec(self) -> target_set_t:
        self._sut: target_set_t
        self._sut.add("51")
        return self._sut

    def assertion(self, result: target_set_t) -> bool:
        return result == target_set


class BenchmarkAddNewItemBase(BenchmarkBase[target_set_t]):
    @property
    def subject(self) -> str:
        return "`add (new item)`"

    def exec(self) -> target_set_t:
        self._sut: target_set_t
        self._sut.add("-1")
        return self._sut

    def assertion(self, result: target_set_t) -> bool:
        return len(result) == (target_set_len + 1) and "-1" in result and target_set < result


class BenchmarkRemoveBase(BenchmarkBase[target_set_t]):
    @property
    def subject(self) -> str:
        return "`remove`"

    def exec(self) -> target_set_t:
        self._sut: target_set_t
        self._sut.remove("51")
        return self._sut

    def assertion(self, result: target_set_t) -> bool:
        return len(result) == (target_set_len - 1) and "51" not in result


class BenchmarkDiscardBase(BenchmarkBase[target_set_t]):
    @property
    def subject(self) -> str:
        return "`discard`"

    def exec(self) -> target_set_t:
        self._sut: target_set_t
        self._sut.discard("51")
        return self._sut

    def assertion(self, result: target_set_t) -> bool:
        return len(result) == (target_set_len - 1) and "51" not in result


class BenchmarkDiscardNoChangesBase(BenchmarkBase[target_set_t]):
    @property
    def subject(self) -> str:
        return "`discard (no changes)`"

    def exec(self) -> target_set_t:
        self._sut: target_set_t
        self._sut.discard("-1")
        return self._sut

    def assertion(self, result: target_set_t) -> bool:
        return result == target_set


class BenchmarkPopBase(BenchmarkBase[Tuple[target_set_t, target_set_item_t]]):
    @property
    def subject(self) -> str:
        return "`pop`"

    def exec(self) -> target_set_t:
        self._sut: target_set_t
        ret = self._sut.pop()
        return (self._sut, ret)

    def assertion(self, result: Tuple[target_set_t, target_set_item_t]) -> bool:
        return len(result[0]) == (target_set_len - 1) and result[1] in target_set and result[1] not in result[0]


class BenchmarkClearBase(BenchmarkBase[target_set_t]):
    @property
    def subject(self) -> str:
        return "`clear`"

    def exec(self) -> target_set_t:
        self._sut: target_set_t
        self._sut.clear()
        return self._sut

    def assertion(self, result: target_set_t) -> bool:
        return len(result) == 0


class BuiltinSetBenchmarkInit(BuiltinSetBenchmarkBase, BenchmarkInitBase):
    def exec(self) -> target_set_t:
        return set(s for s in target_set)


class SqliteCollectionsSetBenchmarkInit(SqliteCollectionsSetBenchmarkBase, BenchmarkInitBase):
    def exec(self) -> target_set_t:
        return sc.Set[target_set_item_t]((s for s in target_set))
