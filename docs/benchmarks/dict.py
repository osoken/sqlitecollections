import gc
import os
import sys
from typing import Any, cast

if sys.version_info > (3, 9):
    from collections.abc import MutableMapping, Set, Tuple
else:
    from typing import MutableMapping, Tuple, Set

from sqlitecollections import Dict

from common import BenchmarkBase, Comparison

benchmarks_dir = os.path.dirname(os.path.abspath(__file__))

target_dict_len = 100000
target_dict = {str(i): i for i in range(target_dict_len)}
target_dict_key_t = str
target_dict_value_t = int
target_dict_t = MutableMapping[target_dict_key_t, target_dict_value_t]


class BuiltinDictBenchmarkBase:
    def __init__(self) -> None:
        super(BuiltinDictBenchmarkBase, self).__init__()
        self._sut_orig = target_dict.copy()
        self._sut: target_dict_t

    @property
    def name(self) -> str:
        return "`dict`"

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


class SqliteCollectionsDictBenchmarkBase:
    def __init__(self) -> None:
        super(SqliteCollectionsDictBenchmarkBase, self).__init__()
        self._sut_orig = Dict[target_dict_key_t, target_dict_value_t](data=target_dict)
        self._sut: target_dict_t

    @property
    def name(self) -> str:
        return "`sqlitecollections.Dict`"

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


class BenchmarkInitBase(BenchmarkBase[target_dict_t]):
    def assertion(self, result: target_dict_t) -> bool:
        return len(result) == target_dict_len and all((result[k] == target_dict[k] for k in target_dict.keys()))


class BenchmarkLenBase(BenchmarkBase[target_dict_value_t]):
    def exec(self) -> target_dict_value_t:
        return len(self._sut)

    def assertion(self, result: target_dict_value_t) -> bool:
        return result == target_dict_len


class BenchmarkGetitemBase(BenchmarkBase[target_dict_value_t]):
    def exec(self) -> target_dict_value_t:
        return self._sut["651"]

    def assertion(self, result: target_dict_value_t) -> bool:
        return result == 651


class BenchmarkSetitemReplaceBase(BenchmarkBase[target_dict_t]):
    def exec(self) -> target_dict_t:
        self._sut["651"] = -651
        return self._sut

    def assertion(self, result: target_dict_t) -> bool:
        return (
            len(result) == target_dict_len
            and all((result[k] == target_dict[k] for k in target_dict.keys() if k != "651"))
            and result["651"] == -651
        )


class BenchmarkSetitemAddNewItemBase(BenchmarkBase[target_dict_t]):
    def exec(self) -> target_dict_t:
        self._sut["-651"] = -651
        return self._sut

    def assertion(self, result: target_dict_t) -> bool:
        return (
            len(result) == (target_dict_len + 1)
            and all((result[k] == target_dict[k] for k in target_dict.keys()))
            and result["-651"] == -651
            and "-651" not in target_dict
        )


class BenchmarkDelitemBase(BenchmarkBase[target_dict_t]):
    def exec(self) -> target_dict_t:
        del self._sut["651"]
        return self._sut

    def assertion(self, result: target_dict_t) -> bool:
        return len(result) == (target_dict_len - 1) and all(
            (result[k] == target_dict[k] for k in target_dict.keys() if k != "651")
        )


class BenchmarkContainsBase(BenchmarkBase[bool]):
    def exec(self) -> bool:
        return "651" in self._sut

    def assertion(self, result: bool) -> bool:
        return result


class BenchmarkNotContainsBase(BenchmarkBase[bool]):
    def exec(self) -> bool:
        return "-651" not in self._sut

    def assertion(self, result: bool) -> bool:
        return result


class BenchmarkIterBase(BenchmarkBase[Set[target_dict_key_t]]):
    def exec(self) -> Set[target_dict_key_t]:
        return set(self._sut)

    def assertion(self, result: Set[target_dict_key_t]) -> bool:
        return result == set(target_dict)


class BenchmarkClearBase(BenchmarkBase[target_dict_t]):
    def exec(self) -> target_dict_t:
        self._sut.clear()
        return self._sut

    def assertion(self, result: target_dict_t) -> bool:
        return len(result) == 0


class BenchmarkCopyBase(BenchmarkBase[target_dict_t]):
    def exec(self) -> target_dict_t:
        return self._sut.copy()

    def assertion(self, result: target_dict_t) -> bool:
        return len(result) == target_dict_len and all((result[k] == target_dict[k] for k in target_dict))


class BuiltinDictBenchmarkInit(BuiltinDictBenchmarkBase, BenchmarkInitBase):
    def exec(self) -> target_dict_t:
        return dict(target_dict.items())


class SqliteCollectionsDictBenchmarkInit(SqliteCollectionsDictBenchmarkBase, BenchmarkInitBase):
    def exec(self) -> target_dict_t:
        return Dict[target_dict_key_t, target_dict_value_t](data=target_dict.items())


class BuiltinDictBenchmarkLen(BuiltinDictBenchmarkBase, BenchmarkLenBase):
    pass


class SqliteCollectionsDictBenchmarkLen(SqliteCollectionsDictBenchmarkBase, BenchmarkLenBase):
    pass


class BuiltinDictBenchmarkGetitem(BuiltinDictBenchmarkBase, BenchmarkGetitemBase):
    pass


class SqliteCollectionsDictBenchmarkGetitem(SqliteCollectionsDictBenchmarkBase, BenchmarkGetitemBase):
    pass


class BuiltinDictBenchmarkSetitemReplace(BuiltinDictBenchmarkBase, BenchmarkSetitemReplaceBase):
    pass


class SqliteCollectionsDictBenchmarkSetitemReplace(SqliteCollectionsDictBenchmarkBase, BenchmarkSetitemReplaceBase):
    pass


class BuiltinDictBenchmarkSetitemAddNewItem(BuiltinDictBenchmarkBase, BenchmarkSetitemAddNewItemBase):
    pass


class SqliteCollectionsDictBenchmarkSetitemAddNewItem(
    SqliteCollectionsDictBenchmarkBase, BenchmarkSetitemAddNewItemBase
):
    pass


class BuiltinDictBenchmarkDelitem(BuiltinDictBenchmarkBase, BenchmarkDelitemBase):
    pass


class SqliteCollectionsDictBenchmarkDelitem(SqliteCollectionsDictBenchmarkBase, BenchmarkDelitemBase):
    pass


class BuiltinDictBenchmarkContains(BuiltinDictBenchmarkBase, BenchmarkContainsBase):
    pass


class SqliteCollectionsDictBenchmarkContains(SqliteCollectionsDictBenchmarkBase, BenchmarkContainsBase):
    pass


class BuiltinDictBenchmarkNotContains(BuiltinDictBenchmarkBase, BenchmarkNotContainsBase):
    pass


class SqliteCollectionsDictBenchmarkNotContains(SqliteCollectionsDictBenchmarkBase, BenchmarkNotContainsBase):
    pass


class BuiltinDictBenchmarkIter(BuiltinDictBenchmarkBase, BenchmarkIterBase):
    pass


class SqliteCollectionsDictBenchmarkIter(SqliteCollectionsDictBenchmarkBase, BenchmarkIterBase):
    pass


class BuiltinDictBenchmarkClear(BuiltinDictBenchmarkBase, BenchmarkClearBase):
    pass


class SqliteCollectionsDictBenchmarkClear(SqliteCollectionsDictBenchmarkBase, BenchmarkClearBase):
    pass


class BuiltinDictBenchmarkCopy(BuiltinDictBenchmarkBase, BenchmarkCopyBase):
    pass


class SqliteCollectionsDictBenchmarkCopy(SqliteCollectionsDictBenchmarkBase, BenchmarkCopyBase):
    pass


if __name__ == "__main__":
    print(Comparison("`__init__`", BuiltinDictBenchmarkInit(), SqliteCollectionsDictBenchmarkInit())().dict())
    print(Comparison("`__len__`", BuiltinDictBenchmarkLen(), SqliteCollectionsDictBenchmarkLen())().dict())
    print(Comparison("`__getitem__`", BuiltinDictBenchmarkGetitem(), SqliteCollectionsDictBenchmarkGetitem())().dict())
    print(
        Comparison(
            "`__setitem__` (replace)",
            BuiltinDictBenchmarkSetitemReplace(),
            SqliteCollectionsDictBenchmarkSetitemReplace(),
        )().dict()
    )
    print(
        Comparison(
            "`__setitem__` (add new item)",
            BuiltinDictBenchmarkSetitemAddNewItem(),
            SqliteCollectionsDictBenchmarkSetitemAddNewItem(),
        )().dict()
    )
    print(Comparison("`__delitem__`", BuiltinDictBenchmarkDelitem(), SqliteCollectionsDictBenchmarkDelitem())().dict())
    print(
        Comparison("`__contains__`", BuiltinDictBenchmarkContains(), SqliteCollectionsDictBenchmarkContains())().dict()
    )
    print(
        Comparison(
            "`__contains__` (unsuccessful search)",
            BuiltinDictBenchmarkNotContains(),
            SqliteCollectionsDictBenchmarkNotContains(),
        )().dict()
    )
    print(Comparison("`__iter__`", BuiltinDictBenchmarkIter(), SqliteCollectionsDictBenchmarkIter())().dict())
    print(Comparison("`clear`", BuiltinDictBenchmarkClear(), SqliteCollectionsDictBenchmarkClear())().dict())
    print(Comparison("`copy`", BuiltinDictBenchmarkCopy(), SqliteCollectionsDictBenchmarkCopy())().dict())
