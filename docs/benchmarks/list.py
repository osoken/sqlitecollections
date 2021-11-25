import gc
import os
import random
import sys
from typing import Any, cast

if sys.version_info > (3, 9):
    from collections.abc import MutableSequence, Tuple
else:
    from typing import MutableSequence, Tuple

from sqlitecollections import List

from common import BenchmarkBase, Comparison

benchmarks_dir = os.path.dirname(os.path.abspath(__file__))

target_list_len = 100000
target_list = list([str(i) for i in range(target_list_len)])
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
    def __init__(self) -> None:
        super(SqliteCollectionsListBenchmarkBase, self).__init__()
        self._sut_orig = List[target_list_element_t](data=target_list)
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
        return str(651) in self._sut

    def assertion(self, result: bool) -> bool:
        return result == True


class BenchmarkNotContainsBase(BenchmarkBase[bool]):
    def exec(self) -> bool:
        self._sut: target_list_t
        return str(target_list_len + 123) in self._sut

    def assertion(self, result: bool) -> bool:
        return result == False


class BenchmarkSetitemBase(BenchmarkBase[target_list_t]):
    def exec(self) -> target_list_t:
        self._sut: target_list_t
        self._sut[0] = "-123"
        return self._sut

    def assertion(self, result: target_list_t) -> bool:
        return result[0] == "-123" and result[1] == target_list[1]


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


class BenchmarkAddBase(BenchmarkBase[target_list_t]):
    def exec(self) -> target_list_t:
        return self._sut + ["-1", "-2", "-3"]

    def assertion(self, result: target_list_t) -> bool:
        return len(result) == (target_list_len + 3) and result[target_list_len] == "-1"


class BenchmarkMultBase(BenchmarkBase[target_list_t]):
    def exec(self) -> target_list_t:
        return self._sut * 2

    def assertion(self, result: target_list_t) -> bool:
        return len(result) == (target_list_len * 2) and result[target_list_len] == target_list[0]


class BenchmarkGetitemSliceSkipBase(BenchmarkBase[target_list_t]):
    def exec(self) -> target_list_t:
        return self._sut[0:target_list_len:100]

    def assertion(self, result: target_list_t) -> bool:
        return len(result) == (target_list_len // 100) and all(
            [result[i] == target_list[j] for i, j in zip(range(target_list_len), range(0, target_list_len, 100))]
        )


class BenchmarkLenBase(BenchmarkBase[int]):
    def exec(self) -> int:
        return len(self._sut)

    def assertion(self, result: int) -> bool:
        return result == target_list_len


class BenchmarkIndexBase(BenchmarkBase[int]):
    def exec(self) -> int:
        return self._sut.index(target_list[target_list_len // 2])

    def assertion(self, result: int) -> bool:
        return result == (target_list_len // 2)


class BenchmarkIndexUnsuccessfulSearchBase(BenchmarkBase[int]):
    def exec(self) -> int:
        try:
            return self._sut.index("-123")
        except ValueError:
            return -1

    def assertion(self, result: int) -> bool:
        return result == -1


class BenchmarkCountBase(BenchmarkBase[int]):
    def exec(self) -> int:
        return self._sut.count("0")

    def assertion(self, result: int) -> bool:
        return result == 1


class BenchmarkSetitemSliceBase(BenchmarkBase[target_list_t]):
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
    def exec(self) -> target_list_t:
        self._sut += ["-1", "-2", "-3"]
        return self._sut

    def assertion(self, result: target_list_t) -> bool:
        return len(result) == (target_list_len + 3) and result[target_list_len] == "-1"


class BenchmarkImultBase(BenchmarkBase[target_list_t]):
    def exec(self) -> target_list_t:
        self._sut *= 2
        return self._sut

    def assertion(self, result: target_list_t) -> bool:
        return len(result) == (target_list_len * 2) and result[target_list_len] == target_list[0]


class BenchmarkPopBase(BenchmarkBase[Tuple[target_list_t, str]]):
    def exec(self) -> target_list_t:
        retval = self._sut.pop(5000)
        return (self._sut, retval)

    def assertion(self, result: Tuple[target_list_t, str]) -> bool:
        return (
            len(result[0]) == (target_list_len - 1)
            and result[0][4999] == target_list[4999]
            and result[0][5000] == target_list[5001]
            and result[1] == target_list[5000]
        )


class BenchmarkRemoveBase(BenchmarkBase[target_list_t]):
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
    def exec(self) -> target_list_t:
        self._sut.sort()
        return self._sut

    def assertion(self, result: target_list_t) -> bool:
        if len(result) != target_list_len:
            return False
        return all((a < b for a, b in zip(result, result[1:])))


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


class BuiltinListBenchmarkGetitemSliceSkip(BuiltinListBenchmarkBase, BenchmarkGetitemSliceSkipBase):
    pass


class SqliteCollectionsListBenchmarkGetitemSliceSkip(SqliteCollectionsListBenchmarkBase, BenchmarkGetitemSliceSkipBase):
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


class BuiltinListBenchmarkAdd(BuiltinListBenchmarkBase, BenchmarkAddBase):
    pass


class SqliteCollectionsListBenchmarkAdd(SqliteCollectionsListBenchmarkBase, BenchmarkAddBase):
    pass


class BuiltinListBenchmarkMult(BuiltinListBenchmarkBase, BenchmarkMultBase):
    pass


class SqliteCollectionsListBenchmarkMult(SqliteCollectionsListBenchmarkBase, BenchmarkMultBase):
    pass


class BuiltinListBenchmarkLen(BuiltinListBenchmarkBase, BenchmarkLenBase):
    pass


class SqliteCollectionsListBenchmarkLen(SqliteCollectionsListBenchmarkBase, BenchmarkLenBase):
    pass


class BuiltinListBenchmarkIndex(BuiltinListBenchmarkBase, BenchmarkIndexBase):
    pass


class SqliteCollectionsListBenchmarkIndex(SqliteCollectionsListBenchmarkBase, BenchmarkIndexBase):
    pass


class BuiltinListBenchmarkIndexUnsuccessfulSearch(BuiltinListBenchmarkBase, BenchmarkIndexUnsuccessfulSearchBase):
    pass


class SqliteCollectionsListBenchmarkIndexUnsuccessfulSearch(
    SqliteCollectionsListBenchmarkBase, BenchmarkIndexUnsuccessfulSearchBase
):
    pass


class BuiltinListBenchmarkCount(BuiltinListBenchmarkBase, BenchmarkCountBase):
    pass


class SqliteCollectionsListBenchmarkCount(SqliteCollectionsListBenchmarkBase, BenchmarkCountBase):
    pass


class BuiltinListBenchmarkSetitemSlice(BuiltinListBenchmarkBase, BenchmarkSetitemSliceBase):
    pass


class SqliteCollectionsListBenchmarkSetitemSlice(SqliteCollectionsListBenchmarkBase, BenchmarkSetitemSliceBase):
    pass


class BuiltinListBenchmarkDelitemSlice(BuiltinListBenchmarkBase, BenchmarkDelitemSliceBase):
    pass


class SqliteCollectionsListBenchmarkDelitemSlice(SqliteCollectionsListBenchmarkBase, BenchmarkDelitemSliceBase):
    pass


class BuiltinListBenchmarkSetitemSliceSkip(BuiltinListBenchmarkBase, BenchmarkSetitemSliceSkipBase):
    pass


class SqliteCollectionsListBenchmarkSetitemSliceSkip(SqliteCollectionsListBenchmarkBase, BenchmarkSetitemSliceSkipBase):
    pass


class BuiltinListBenchmarkDelitemSliceSkip(BuiltinListBenchmarkBase, BenchmarkDelitemSliceSkipBase):
    pass


class SqliteCollectionsListBenchmarkDelitemSliceSkip(SqliteCollectionsListBenchmarkBase, BenchmarkDelitemSliceSkipBase):
    pass


class BuiltinListBenchmarkIadd(BuiltinListBenchmarkBase, BenchmarkIaddBase):
    pass


class SqliteCollectionsListBenchmarkIadd(SqliteCollectionsListBenchmarkBase, BenchmarkIaddBase):
    pass


class BuiltinListBenchmarkImult(BuiltinListBenchmarkBase, BenchmarkImultBase):
    pass


class SqliteCollectionsListBenchmarkImult(SqliteCollectionsListBenchmarkBase, BenchmarkImultBase):
    pass


class BuiltinListBenchmarkPop(BuiltinListBenchmarkBase, BenchmarkPopBase):
    pass


class SqliteCollectionsListBenchmarkPop(SqliteCollectionsListBenchmarkBase, BenchmarkPopBase):
    pass


class BuiltinListBenchmarkRemove(BuiltinListBenchmarkBase, BenchmarkRemoveBase):
    pass


class SqliteCollectionsListBenchmarkRemove(SqliteCollectionsListBenchmarkBase, BenchmarkRemoveBase):
    pass


class BuiltinListBenchmarkSort(BuiltinListBenchmarkBase, BenchmarkSortBase):
    pass


class SqliteCollectionsListBenchmarkSort(SqliteCollectionsListBenchmarkBase, BenchmarkSortBase):
    pass


if __name__ == "__main__":
    print(
        Comparison(
            "`__init__`",
            BuiltinListBenchmarkCreateWithInitialData(),
            SqliteCollectionsListBenchmarkCreateWithInitialData(),
        )().dict()
    )
    print(
        Comparison("`__contains__`", BuiltinListBenchmarkContains(), SqliteCollectionsListBenchmarkContains())().dict()
    )
    print(
        Comparison(
            "`__contains__` (unsuccessful search)",
            BuiltinListBenchmarkNotContains(),
            SqliteCollectionsListBenchmarkNotContains(),
        )().dict()
    )
    print(Comparison("`__add__`", BuiltinListBenchmarkAdd(), SqliteCollectionsListBenchmarkAdd())().dict())
    print(Comparison("`__mult__`", BuiltinListBenchmarkMult(), SqliteCollectionsListBenchmarkMult())().dict())

    print(Comparison("`__getitem__`", BuiltinListBenchmarkGetitem(), SqliteCollectionsListBenchmarkGetitem())().dict())
    print(
        Comparison(
            "`__getitem__` (slice)", BuiltinListBenchmarkGetitemSlice(), SqliteCollectionsListBenchmarkGetitemSlice()
        )().dict()
    )
    print(
        Comparison(
            "`__getitem__` (slice with skip)",
            BuiltinListBenchmarkGetitemSliceSkip(),
            SqliteCollectionsListBenchmarkGetitemSliceSkip(),
        )().dict()
    )
    print(Comparison("`__len__`", BuiltinListBenchmarkLen(), SqliteCollectionsListBenchmarkLen())().dict())
    print(Comparison("`index`", BuiltinListBenchmarkIndex(), SqliteCollectionsListBenchmarkIndex())().dict())
    print(
        Comparison(
            "`index` (unsuccessful search)",
            BuiltinListBenchmarkIndexUnsuccessfulSearch(),
            SqliteCollectionsListBenchmarkIndexUnsuccessfulSearch(),
        )().dict()
    )
    print(
        Comparison(
            "`count`",
            BuiltinListBenchmarkCount(),
            SqliteCollectionsListBenchmarkCount(),
        )().dict()
    )
    print(Comparison("`__setitem__`", BuiltinListBenchmarkSetitem(), SqliteCollectionsListBenchmarkSetitem())().dict())
    print(Comparison("`__delitem__`", BuiltinListBenchmarkDelitem(), SqliteCollectionsListBenchmarkDelitem())().dict())
    print(
        Comparison(
            "`__setitem__` (slice)", BuiltinListBenchmarkSetitemSlice(), SqliteCollectionsListBenchmarkSetitemSlice()
        )().dict()
    )
    print(
        Comparison(
            "`__delitem__` (slice)", BuiltinListBenchmarkDelitemSlice(), SqliteCollectionsListBenchmarkDelitemSlice()
        )().dict()
    )
    print(
        Comparison(
            "`__setitem__` (slice with skip)",
            BuiltinListBenchmarkSetitemSliceSkip(),
            SqliteCollectionsListBenchmarkSetitemSliceSkip(),
        )().dict()
    )
    print(
        Comparison(
            "`__delitem__` (slice with skip)",
            BuiltinListBenchmarkDelitemSliceSkip(),
            SqliteCollectionsListBenchmarkDelitemSliceSkip(),
        )().dict()
    )
    print(Comparison("`append`", BuiltinListBenchmarkAppend(), SqliteCollectionsListBenchmarkAppend())().dict())
    print(Comparison("`clear`", BuiltinListBenchmarkClear(), SqliteCollectionsListBenchmarkClear())().dict())
    print(Comparison("`copy`", BuiltinListBenchmarkCopy(), SqliteCollectionsListBenchmarkCopy())().dict())
    print(Comparison("`extend`", BuiltinListBenchmarkExtend(), SqliteCollectionsListBenchmarkExtend())().dict())
    print(Comparison("`__iadd__`", BuiltinListBenchmarkIadd(), SqliteCollectionsListBenchmarkIadd())().dict())
    print(Comparison("`__imult__`", BuiltinListBenchmarkImult(), SqliteCollectionsListBenchmarkImult())().dict())
    print(Comparison("`insert`", BuiltinListBenchmarkInsert(), SqliteCollectionsListBenchmarkInsert())().dict())
    print(Comparison("`pop`", BuiltinListBenchmarkPop(), SqliteCollectionsListBenchmarkPop())().dict())
    print(Comparison("`remove`", BuiltinListBenchmarkRemove(), SqliteCollectionsListBenchmarkRemove())().dict())
    print(Comparison("`sort`", BuiltinListBenchmarkSort(), SqliteCollectionsListBenchmarkSort())().dict())
