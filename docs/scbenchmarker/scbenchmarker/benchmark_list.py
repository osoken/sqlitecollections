import gc
import os
import random
import sys
from typing import Any, Optional, Tuple

if sys.version_info >= (3, 9):
    from collections.abc import MutableSequence
else:
    from typing import MutableSequence

import sqlitecollections as sc

from .common import BenchmarkBase, Comparison

benchmarks_dir = os.path.dirname(os.path.abspath(__file__))

target_list_len = 2000
target_list = list([str(i) for i in range(target_list_len)])
target_list_element_t = str
target_list_t = MutableSequence[target_list_element_t]
random.seed(5432)
random.shuffle(target_list)


class BuiltinListBenchmarkBase:
    def __init__(self, timeout: Optional[float] = None, debug: bool = False) -> None:
        super(BuiltinListBenchmarkBase, self).__init__(timeout=timeout, debug=debug)
        self._sut_orig = target_list.copy()
        self._sut: target_list_t

    @property
    def name(self) -> str:
        return "`list`"

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
    def __init__(self, timeout: Optional[float] = None, debug: bool = False) -> None:
        super(SqliteCollectionsListBenchmarkBase, self).__init__(timeout=timeout, debug=debug)
        self._sut_orig = sc.List[target_list_element_t](target_list)
        self._sut: target_list_t

    @property
    def name(self) -> str:
        return "`sqlitecollections.List`"

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
    @property
    def subject(self) -> str:
        return "`__delitem__`"

    def exec(self) -> target_list_t:
        self._sut: target_list_t
        del self._sut[1000]
        return self._sut

    def assertion(self, result: target_list_t) -> bool:
        return (
            len(result) == (target_list_len - 1)
            and result[999] == target_list[999]
            and result[1000] == target_list[1001]
        )


class BenchmarkGetitemBase(BenchmarkBase[target_list_element_t]):
    @property
    def subject(self) -> str:
        return "`__getitem__`"

    def exec(self) -> target_list_element_t:
        self._sut: target_list_t
        return self._sut[1000]

    def assertion(self, result: target_list_element_t) -> bool:
        return result == target_list[1000]


class BenchmarkGetitemSliceBase(BenchmarkBase[target_list_t]):
    @property
    def subject(self) -> str:
        return "`__getitem__` (slice)"

    def exec(self) -> target_list_t:
        self._sut: target_list_t
        return self._sut[10:1010]

    def assertion(self, result: target_list_t) -> bool:
        return all([a == b for a, b in zip(target_list[10:1010], result)])


class BenchmarkCreateWithInitialDataBase(BenchmarkBase[target_list_t]):
    @property
    def subject(self) -> str:
        return "`__init__`"

    def assertion(self, result: target_list_t) -> bool:
        return len(result) == target_list_len and all(a == b for a, b in zip(result, target_list))


class BenchmarkContainsBase(BenchmarkBase[bool]):
    @property
    def subject(self) -> str:
        return "`__contains__`"

    def exec(self) -> bool:
        self._sut: target_list_t
        return str(651) in self._sut

    def assertion(self, result: bool) -> bool:
        return result == True


class BenchmarkNotContainsBase(BenchmarkBase[bool]):
    @property
    def subject(self) -> str:
        return "`__contains__` (unsuccessful search)"

    def exec(self) -> bool:
        self._sut: target_list_t
        return str(target_list_len + 123) in self._sut

    def assertion(self, result: bool) -> bool:
        return result == False


class BenchmarkSetitemBase(BenchmarkBase[target_list_t]):
    @property
    def subject(self) -> str:
        return "`__setitem__`"

    def exec(self) -> target_list_t:
        self._sut: target_list_t
        self._sut[0] = "-123"
        return self._sut

    def assertion(self, result: target_list_t) -> bool:
        return result[0] == "-123" and result[1] == target_list[1]


class BenchmarkInsertBase(BenchmarkBase[target_list_t]):
    @property
    def subject(self) -> str:
        return "`insert`"

    def exec(self) -> target_list_t:
        self._sut: target_list_t
        self._sut.insert(0, "-123")
        return self._sut

    def assertion(self, result: target_list_t) -> bool:
        return len(result) == (target_list_len + 1) and result[0] == "-123" and result[1] == target_list[0]


class BenchmarkAppendBase(BenchmarkBase[target_list_t]):
    @property
    def subject(self) -> str:
        return "`append`"

    def exec(self) -> target_list_t:
        self._sut: target_list_t
        self._sut.append("-123")
        return self._sut

    def assertion(self, result: target_list_t) -> bool:
        return len(result) == (target_list_len + 1) and result[target_list_len] == "-123"


class BenchmarkClearBase(BenchmarkBase[target_list_t]):
    @property
    def subject(self) -> str:
        return "`clear`"

    def exec(self) -> target_list_t:
        self._sut: target_list_t
        self._sut.clear()
        return self._sut

    def assertion(self, result: target_list_t) -> bool:
        return len(result) == 0


class BenchmarkExtendBase(BenchmarkBase[target_list_t]):
    @property
    def subject(self) -> str:
        return "`extend`"

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
    @property
    def subject(self) -> str:
        return "`copy`"

    def exec(self) -> target_list_t:
        self._sut: target_list_t
        retval = self._sut.copy()
        return retval

    def assertion(self, result: target_list_t) -> bool:
        return all([a == b for a, b in zip(result, target_list)])


class BenchmarkAddBase(BenchmarkBase[target_list_t]):
    @property
    def subject(self) -> str:
        return "`__add__`"

    def exec(self) -> target_list_t:
        return self._sut + ["-1", "-2", "-3"]

    def assertion(self, result: target_list_t) -> bool:
        return len(result) == (target_list_len + 3) and result[target_list_len] == "-1"


class BenchmarkMultBase(BenchmarkBase[target_list_t]):
    @property
    def subject(self) -> str:
        return "`__mult__`"

    def exec(self) -> target_list_t:
        return self._sut * 2

    def assertion(self, result: target_list_t) -> bool:
        return len(result) == (target_list_len * 2) and result[target_list_len] == target_list[0]


class BenchmarkGetitemSliceSkipBase(BenchmarkBase[target_list_t]):
    @property
    def subject(self) -> str:
        return "`__getitem__` (slice with skip)"

    def exec(self) -> target_list_t:
        return self._sut[0:target_list_len:100]

    def assertion(self, result: target_list_t) -> bool:
        return len(result) == (target_list_len // 100) and all(
            [result[i] == target_list[j] for i, j in zip(range(target_list_len), range(0, target_list_len, 100))]
        )


class BenchmarkLenBase(BenchmarkBase[int]):
    @property
    def subject(self) -> str:
        return "`__len__`"

    def exec(self) -> int:
        return len(self._sut)

    def assertion(self, result: int) -> bool:
        return result == target_list_len


class BenchmarkIndexBase(BenchmarkBase[int]):
    @property
    def subject(self) -> str:
        return "`index`"

    def exec(self) -> int:
        return self._sut.index(target_list[target_list_len // 2])

    def assertion(self, result: int) -> bool:
        return result == (target_list_len // 2)


class BenchmarkIndexUnsuccessfulSearchBase(BenchmarkBase[int]):
    @property
    def subject(self) -> str:
        return "`index` (unsuccessful search)"

    def exec(self) -> int:
        try:
            return self._sut.index("-123")
        except ValueError:
            return -1

    def assertion(self, result: int) -> bool:
        return result == -1


class BenchmarkCountBase(BenchmarkBase[int]):
    @property
    def subject(self) -> str:
        return "`count`"

    def exec(self) -> int:
        return self._sut.count("0")

    def assertion(self, result: int) -> bool:
        return result == 1


class BenchmarkSetitemSliceBase(BenchmarkBase[target_list_t]):
    @property
    def subject(self) -> str:
        return "`__setitem__` (slice)"

    def exec(self) -> target_list_t:
        self._sut[1:101] = (str(i) for i in range(100))
        return self._sut

    def assertion(self, result: target_list_t) -> bool:
        return (
            self._sut[0] == target_list[0]
            and all((self._sut[i] == str(i - 1) for i in range(1, 101)))
            and self._sut[101] == target_list[101]
        )


class BenchmarkDelitemSliceBase(BenchmarkBase[target_list_t]):
    @property
    def subject(self) -> str:
        return "`__delitem__` (slice)"

    def exec(self) -> target_list_t:
        del self._sut[1:101]
        return self._sut

    def assertion(self, result: target_list_t) -> bool:
        return (
            len(self._sut) == (target_list_len - 100)
            and self._sut[0] == target_list[0]
            and self._sut[1] == target_list[101]
        )


class BenchmarkSetitemSliceSkipBase(BenchmarkBase[target_list_t]):
    @property
    def subject(self) -> str:
        return "`__setitem__` (slice with skip)"

    def exec(self) -> target_list_t:
        self._sut[1:101:2] = (str(i) for i in range(1, 101, 2))
        return self._sut

    def assertion(self, result: target_list_t) -> bool:
        return (
            self._sut[0] == target_list[0]
            and self._sut[1] == "1"
            and self._sut[2] == target_list[2]
            and self._sut[3] == "3"
        )


class BenchmarkDelitemSliceSkipBase(BenchmarkBase[target_list_t]):
    @property
    def subject(self) -> str:
        return "`__delitem__` (slice with skip)"

    def exec(self) -> target_list_t:
        del self._sut[1:101:2]
        return self._sut

    def assertion(self, result: target_list_t) -> bool:
        return (
            len(result) == (target_list_len - 50)
            and result[0] == target_list[0]
            and result[1] == target_list[2]
            and result[2] == target_list[4]
            and result[49] == target_list[98]
            and result[50] == target_list[100]
            and result[51] == target_list[101]
        )


class BenchmarkIaddBase(BenchmarkBase[target_list_t]):
    @property
    def subject(self) -> str:
        return "`__iadd__`"

    def exec(self) -> target_list_t:
        self._sut += ["-1", "-2", "-3"]
        return self._sut

    def assertion(self, result: target_list_t) -> bool:
        return len(result) == (target_list_len + 3) and result[target_list_len] == "-1"


class BenchmarkImultBase(BenchmarkBase[target_list_t]):
    @property
    def subject(self) -> str:
        return "`__imult__`"

    def exec(self) -> target_list_t:
        self._sut *= 2
        return self._sut

    def assertion(self, result: target_list_t) -> bool:
        return len(result) == (target_list_len * 2) and result[target_list_len] == target_list[0]


class BenchmarkPopBase(BenchmarkBase[Tuple[target_list_t, str]]):
    @property
    def subject(self) -> str:
        return "`pop`"

    def exec(self) -> target_list_t:
        retval = self._sut.pop(1000)
        return (self._sut, retval)

    def assertion(self, result: Tuple[target_list_t, str]) -> bool:
        return (
            len(result[0]) == (target_list_len - 1)
            and result[0][999] == target_list[999]
            and result[0][1000] == target_list[1001]
            and result[1] == target_list[1000]
        )


class BenchmarkRemoveBase(BenchmarkBase[target_list_t]):
    @property
    def subject(self) -> str:
        return "`remove`"

    def exec(self) -> target_list_t:
        self._sut.remove("651")
        return self._sut

    def assertion(self, result: target_list_t) -> bool:
        if len(result) != target_list_len - 1:
            return False
        idx = target_list.index("651")
        if idx == target_list_len - 1:
            return result[idx - 1] == target_list[idx - 1]
        return result[idx] == target_list[idx + 1]


class BenchmarkSortBase(BenchmarkBase[target_list_t]):
    @property
    def subject(self) -> str:
        return "`sort`"

    def exec(self) -> target_list_t:
        self._sut.sort()
        return self._sut

    def assertion(self, result: target_list_t) -> bool:
        if len(result) != target_list_len:
            return False
        return all((a < b for a, b in zip(result, result[1:])))


class BenchmarkSortFastestBase(BenchmarkSortBase):
    @property
    def subject(self) -> str:
        return "`sort (fastest)`"


class BenchmarkSortBalancedBase(BenchmarkSortBase):
    @property
    def subject(self) -> str:
        return "`sort (balanced)`"


class BuiltinListBenchmarkCreateWithInitialData(BuiltinListBenchmarkBase, BenchmarkCreateWithInitialDataBase):
    def exec(self) -> Any:
        return list(iter(target_list))


class SqliteCollectionsListBenchmarkCreateWithInitialData(
    SqliteCollectionsListBenchmarkBase, BenchmarkCreateWithInitialDataBase
):
    def exec(self) -> Any:
        return sc.List[target_list_element_t](iter(target_list))


class SqliteCollectionsListBenchmarkSortFastest(SqliteCollectionsListBenchmarkBase, BenchmarkSortFastestBase):
    def setup(self) -> None:
        gc.collect()
        gc.collect()
        self._sut = self._sut_orig.copy()
        self._sut._sorting_strategy = sc.SortingStrategy.fastest
        gc.collect()
        gc.collect()


class BuiltinListBenchmarkSortFastest(BuiltinListBenchmarkBase, BenchmarkSortFastestBase):
    ...


class SqliteCollectionsListBenchmarkSortBalanced(SqliteCollectionsListBenchmarkBase, BenchmarkSortBalancedBase):
    def setup(self) -> None:
        gc.collect()
        gc.collect()
        self._sut = self._sut_orig.copy()
        self._sut._sorting_strategy = sc.SortingStrategy.balanced
        gc.collect()
        gc.collect()


class BuiltinListBenchmarkSortBalanced(BuiltinListBenchmarkBase, BenchmarkSortBalancedBase):
    ...
