import gc
import os
import sys
from typing import Any, cast

if sys.version_info > (3, 9):
    from collections.abc import MutableMapping, Set
else:
    from typing import MutableMapping, Set

from typing import Tuple

if sys.version_info >= (3, 8):
    if sys.version_info > (3, 9):
        from collections.abc import Sequence
    else:
        from typing import Sequence

from sqlitecollections import Dict

from .common import BenchmarkBase, Comparison

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
        self._sut: target_dict_t
        return len(self._sut)

    def assertion(self, result: target_dict_value_t) -> bool:
        return result == target_dict_len


class BenchmarkGetitemBase(BenchmarkBase[target_dict_value_t]):
    def exec(self) -> target_dict_value_t:
        self._sut: target_dict_t
        return self._sut["651"]

    def assertion(self, result: target_dict_value_t) -> bool:
        return result == 651


class BenchmarkSetitemReplaceBase(BenchmarkBase[target_dict_t]):
    def exec(self) -> target_dict_t:
        self._sut: target_dict_t
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
        self._sut: target_dict_t
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
        self._sut: target_dict_t
        del self._sut["651"]
        return self._sut

    def assertion(self, result: target_dict_t) -> bool:
        return len(result) == (target_dict_len - 1) and all(
            (result[k] == target_dict[k] for k in target_dict.keys() if k != "651")
        )


class BenchmarkContainsBase(BenchmarkBase[bool]):
    def exec(self) -> bool:
        self._sut: target_dict_t
        return "651" in self._sut

    def assertion(self, result: bool) -> bool:
        return result


class BenchmarkNotContainsBase(BenchmarkBase[bool]):
    def exec(self) -> bool:
        self._sut: target_dict_t
        return "-651" not in self._sut

    def assertion(self, result: bool) -> bool:
        return result


class BenchmarkIterBase(BenchmarkBase[Set[target_dict_key_t]]):
    def exec(self) -> Set[target_dict_key_t]:
        self._sut: target_dict_t
        return set(self._sut)

    def assertion(self, result: Set[target_dict_key_t]) -> bool:
        return result == set(target_dict)


class BenchmarkClearBase(BenchmarkBase[target_dict_t]):
    def exec(self) -> target_dict_t:
        self._sut: target_dict_t
        self._sut.clear()
        return self._sut

    def assertion(self, result: target_dict_t) -> bool:
        return len(result) == 0


class BenchmarkCopyBase(BenchmarkBase[target_dict_t]):
    def exec(self) -> target_dict_t:
        self._sut: target_dict_t
        retval = self._sut.copy()
        return retval

    def assertion(self, result: target_dict_t) -> bool:
        return len(result) == target_dict_len and all((result[k] == target_dict[k] for k in target_dict))


class BenchmarkGetBase(BenchmarkBase[target_dict_value_t]):
    def exec(self) -> target_dict_value_t:
        self._sut: target_dict_t
        return cast(target_dict_value_t, self._sut.get("651"))

    def assertion(self, result: target_dict_value_t) -> bool:
        return result == 651


class BenchmarkGetDefaultBase(BenchmarkBase[target_dict_value_t]):
    def exec(self) -> target_dict_value_t:
        self._sut: target_dict_t
        return self._sut.get("-1", -1)

    def assertion(self, result: target_dict_value_t) -> bool:
        return result == -1


class BenchmarkItemsBase(BenchmarkBase[Set[Tuple[target_dict_key_t, target_dict_value_t]]]):
    def exec(self) -> Set[Tuple[target_dict_key_t, target_dict_value_t]]:
        self._sut: target_dict_t
        return set(self._sut.items())

    def assertion(self, result: Set[Tuple[target_dict_key_t, target_dict_value_t]]) -> bool:
        return result == set(target_dict.items())


class BenchmarkKeysBase(BenchmarkBase[Set[target_dict_key_t]]):
    def exec(self) -> Set[target_dict_key_t]:
        self._sut: target_dict_t
        return set(self._sut.keys())

    def assertion(self, result: Set[target_dict_key_t]) -> bool:
        return result == set(target_dict.keys())


class BenchmarkPopBase(BenchmarkBase[Tuple[target_dict_value_t, target_dict_t]]):
    def exec(self) -> Tuple[target_dict_value_t, target_dict_t]:
        self._sut: target_dict_t
        val = self._sut.pop("651")
        return (val, self._sut)

    def assertion(self, result: Tuple[target_dict_value_t, target_dict_t]) -> bool:
        return result[0] == 651 and len(result[1]) == (target_dict_len - 1) and "651" not in result[1]


class BenchmarkPopDefaultBase(BenchmarkBase[Tuple[target_dict_value_t, target_dict_t]]):
    def exec(self) -> Tuple[target_dict_value_t, target_dict_t]:
        self._sut: target_dict_t
        val = self._sut.pop("-1", -1)
        return (val, self._sut)

    def assertion(self, result: Tuple[target_dict_value_t, target_dict_t]) -> bool:
        return result[0] == -1 and len(result[1]) == target_dict_len


class BenchmarkPopitemBase(BenchmarkBase[Tuple[Tuple[target_dict_key_t, target_dict_value_t], target_dict_t]]):
    def exec(self) -> Tuple[Tuple[target_dict_key_t, target_dict_value_t], target_dict_t]:
        self._sut: target_dict_t
        retval = self._sut.popitem()
        return (retval, self._sut)

    def assertion(self, result: Tuple[Tuple[target_dict_key_t, target_dict_value_t], target_dict_t]) -> bool:
        return len(result[1]) == (target_dict_len - 1) and result[0][0] not in result


if sys.version_info >= (3, 8):

    class BenchmarkReversedBase(BenchmarkBase[Sequence[target_dict_key_t]]):
        def exec(self) -> Sequence[target_dict_key_t]:
            self._sut: target_dict_t
            retkeys = list(reversed(self._sut))
            return retkeys

        def assertion(self, result: Sequence[target_dict_key_t]):
            keys = list(target_dict)
            return len(result) == len(keys) and all((a == b for a, b in zip(result, keys[::-1])))


class BenchmarkSetdefaultBase(BenchmarkBase[Tuple[target_dict_value_t, target_dict_t]]):
    def exec(self) -> Tuple[target_dict_value_t, target_dict_t]:
        self._sut: target_dict_t
        retval = self._sut.setdefault("651")
        return (retval, self._sut)

    def assertion(self, result: Tuple[target_dict_value_t, target_dict_t]) -> bool:
        return len(result[1]) == target_dict_len and result[0] == 651


class BenchmarkSetdefaultAddItemBase(BenchmarkBase[Tuple[target_dict_value_t, target_dict_t]]):
    def exec(self) -> Tuple[target_dict_value_t, target_dict_t]:
        self._sut: target_dict_t
        retval = self._sut.setdefault("-1", -1)
        return (retval, self._sut)

    def assertion(self, result: Tuple[target_dict_value_t, target_dict_t]) -> bool:
        return len(result[1]) == (target_dict_len + 1) and result[0] == -1 and result[1]["-1"] == -1


class BenchmarkUpdateBase(BenchmarkBase[target_dict_t]):
    def exec(self) -> target_dict_t:
        self._sut.update({"-1": -1})
        return self._sut

    def assertion(self, result: target_dict_t) -> bool:
        return len(self._sut) == (target_dict_len + 1) and self._sut["-1"] == -1


class BenchmarkUpdateManyBase(BenchmarkBase[target_dict_t]):
    def exec(self) -> target_dict_t:
        self._sut.update((str(-v), -v) for v in target_dict.values())
        return self._sut

    def assertion(self, result: target_dict_t) -> bool:
        return len(self._sut) == (target_dict_len * 2 - 1) and self._sut["-1"] == -1


class BenchmarkValuesBase(BenchmarkBase[Set[target_dict_value_t]]):
    def exec(self) -> Set[target_dict_value_t]:
        self._sut: target_dict_t
        return set(self._sut.values())

    def assertion(self, result: Set[target_dict_value_t]) -> bool:
        return result == set(target_dict.values())


if sys.version_info >= (3, 9):

    class BenchmarkOrBase(BenchmarkBase[target_dict_t]):
        def exec(self) -> target_dict_t:
            return self._sut | {"-1": -1}

        def assertion(self, result: target_dict_t) -> bool:
            return len(result) == (target_dict_len + 1) and result["-1"] == -1

    class BenchmarkOrManyBase(BenchmarkBase[target_dict_t]):
        def exec(self) -> target_dict_t:
            return self._sut | {str(-v): -v for v in target_dict.values()}

        def assertion(self, result: target_dict_t) -> bool:
            return len(result) == (target_dict_len * 2 - 1) and result["-1"] == -1

    class BenchmarkIorBase(BenchmarkBase[target_dict_t]):
        def exec(self) -> target_dict_t:
            self._sut |= {"-1": -1}
            return self._sut

        def assertion(self, result: target_dict_t) -> bool:
            return len(result) == (target_dict_len + 1) and result["-1"] == -1

    class BenchmarkIorManyBase(BenchmarkBase[target_dict_t]):
        def exec(self) -> target_dict_t:
            self._sut |= {str(-v): -v for v in target_dict.values()}
            return self._sut

        def assertion(self, result: target_dict_t) -> bool:
            return len(result) == (target_dict_len * 2 - 1) and result["-1"] == -1


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


class BuiltinDictBenchmarkGet(BuiltinDictBenchmarkBase, BenchmarkGetBase):
    pass


class SqliteCollectionsDictBenchmarkGet(SqliteCollectionsDictBenchmarkBase, BenchmarkGetBase):
    pass


class BuiltinDictBenchmarkGetDefault(BuiltinDictBenchmarkBase, BenchmarkGetDefaultBase):
    pass


class SqliteCollectionsDictBenchmarkGetDefault(SqliteCollectionsDictBenchmarkBase, BenchmarkGetDefaultBase):
    pass


class BuiltinDictBenchmarkItems(BuiltinDictBenchmarkBase, BenchmarkItemsBase):
    pass


class SqliteCollectionsDictBenchmarkItems(SqliteCollectionsDictBenchmarkBase, BenchmarkItemsBase):
    pass


class BuiltinDictBenchmarkKeys(BuiltinDictBenchmarkBase, BenchmarkKeysBase):
    pass


class SqliteCollectionsDictBenchmarkKeys(SqliteCollectionsDictBenchmarkBase, BenchmarkKeysBase):
    pass


class BuiltinDictBenchmarkPop(BuiltinDictBenchmarkBase, BenchmarkPopBase):
    pass


class SqliteCollectionsDictBenchmarkPop(SqliteCollectionsDictBenchmarkBase, BenchmarkPopBase):
    pass


class BuiltinDictBenchmarkPopDefault(BuiltinDictBenchmarkBase, BenchmarkPopDefaultBase):
    pass


class SqliteCollectionsDictBenchmarkPopDefault(SqliteCollectionsDictBenchmarkBase, BenchmarkPopDefaultBase):
    pass


class BuiltinDictBenchmarkPopitem(BuiltinDictBenchmarkBase, BenchmarkPopitemBase):
    pass


class SqliteCollectionsDictBenchmarkPopitem(SqliteCollectionsDictBenchmarkBase, BenchmarkPopitemBase):
    pass


class BuiltinDictBenchmarkSetdefault(BuiltinDictBenchmarkBase, BenchmarkSetdefaultBase):
    pass


class SqliteCollectionsDictBenchmarkSetdefault(SqliteCollectionsDictBenchmarkBase, BenchmarkSetdefaultBase):
    pass


class BuiltinDictBenchmarkSetdefaultAddItem(BuiltinDictBenchmarkBase, BenchmarkSetdefaultAddItemBase):
    pass


class SqliteCollectionsDictBenchmarkSetdefaultAddItem(
    SqliteCollectionsDictBenchmarkBase, BenchmarkSetdefaultAddItemBase
):
    pass


if sys.version_info >= (3, 8):

    class BuiltinDictBenchmarkReversed(BuiltinDictBenchmarkBase, BenchmarkReversedBase):
        pass

    class SqliteCollectionsDictBenchmarkReversed(SqliteCollectionsDictBenchmarkBase, BenchmarkReversedBase):
        pass


class BuiltinDictBenchmarkUpdate(BuiltinDictBenchmarkBase, BenchmarkUpdateBase):
    pass


class SqliteCollectionsDictBenchmarkUpdate(SqliteCollectionsDictBenchmarkBase, BenchmarkUpdateBase):
    pass


class BuiltinDictBenchmarkUpdateMany(BuiltinDictBenchmarkBase, BenchmarkUpdateManyBase):
    pass


class SqliteCollectionsDictBenchmarkUpdateMany(SqliteCollectionsDictBenchmarkBase, BenchmarkUpdateManyBase):
    pass


class BuiltinDictBenchmarkValues(BuiltinDictBenchmarkBase, BenchmarkValuesBase):
    pass


class SqliteCollectionsDictBenchmarkValues(SqliteCollectionsDictBenchmarkBase, BenchmarkValuesBase):
    pass


if sys.version_info >= (3, 9):

    class BuiltinDictBenchmarkOr(BuiltinDictBenchmarkBase, BenchmarkOrBase):
        pass

    class SqliteCollectionsDictBenchmarkOr(SqliteCollectionsDictBenchmarkBase, BenchmarkOrBase):
        pass

    class BuiltinDictBenchmarkOrMany(BuiltinDictBenchmarkBase, BenchmarkOrManyBase):
        pass

    class SqliteCollectionsDictBenchmarkOrMany(SqliteCollectionsDictBenchmarkBase, BenchmarkOrManyBase):
        pass

    class BuiltinDictBenchmarkIor(BuiltinDictBenchmarkBase, BenchmarkIorBase):
        pass

    class SqliteCollectionsDictBenchmarkIor(SqliteCollectionsDictBenchmarkBase, BenchmarkIorBase):
        pass

    class BuiltinDictBenchmarkIorMany(BuiltinDictBenchmarkBase, BenchmarkIorManyBase):
        pass

    class SqliteCollectionsDictBenchmarkIorMany(SqliteCollectionsDictBenchmarkBase, BenchmarkIorManyBase):
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
    print(Comparison("`get`", BuiltinDictBenchmarkGet(), SqliteCollectionsDictBenchmarkGet())().dict())
    print(
        Comparison(
            "`get (unsuccessful search)`", BuiltinDictBenchmarkGetDefault(), SqliteCollectionsDictBenchmarkGetDefault()
        )().dict()
    )
    print(Comparison("`items`", BuiltinDictBenchmarkItems(), SqliteCollectionsDictBenchmarkItems())().dict())
    print(Comparison("`keys`", BuiltinDictBenchmarkKeys(), SqliteCollectionsDictBenchmarkKeys())().dict())
    print(Comparison("`pop`", BuiltinDictBenchmarkPop(), SqliteCollectionsDictBenchmarkPop())().dict())
    print(
        Comparison(
            "`pop (unsuccessful search)`", BuiltinDictBenchmarkPopDefault(), SqliteCollectionsDictBenchmarkPopDefault()
        )().dict()
    )
    print(Comparison("`popitem`", BuiltinDictBenchmarkPopitem(), SqliteCollectionsDictBenchmarkPopitem())().dict())
    if sys.version_info >= (3, 8):
        print(
            Comparison("`reversed`", BuiltinDictBenchmarkReversed(), SqliteCollectionsDictBenchmarkReversed())().dict()
        )
    print(
        Comparison(
            "`setdefault`", BuiltinDictBenchmarkSetdefault(), SqliteCollectionsDictBenchmarkSetdefault()
        )().dict()
    )
    print(
        Comparison(
            "`setdefault (unsuccessful search)`",
            BuiltinDictBenchmarkSetdefaultAddItem(),
            SqliteCollectionsDictBenchmarkSetdefaultAddItem(),
        )().dict()
    )
    print(Comparison("`update`", BuiltinDictBenchmarkUpdate(), SqliteCollectionsDictBenchmarkUpdate())().dict())
    print(
        Comparison(
            "`update` (many)", BuiltinDictBenchmarkUpdateMany(), SqliteCollectionsDictBenchmarkUpdateMany()
        )().dict()
    )
    print(Comparison("`values`", BuiltinDictBenchmarkValues(), SqliteCollectionsDictBenchmarkValues())().dict())
    if sys.version_info >= (3, 9):
        print(Comparison("`__or__`", BuiltinDictBenchmarkOr(), SqliteCollectionsDictBenchmarkOr())().dict())
        print(
            Comparison("`__or__` (many)", BuiltinDictBenchmarkOrMany(), SqliteCollectionsDictBenchmarkOrMany())().dict()
        )
        print(Comparison("`__ior__`", BuiltinDictBenchmarkIor(), SqliteCollectionsDictBenchmarkIor())().dict())
        print(
            Comparison(
                "`__ior__` (many)", BuiltinDictBenchmarkIorMany(), SqliteCollectionsDictBenchmarkIorMany()
            )().dict()
        )
