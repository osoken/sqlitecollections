import gc
import os
import sys
from typing import Any, Optional, cast

if sys.version_info >= (3, 9):
    from collections.abc import MutableMapping, Set
else:
    from typing import MutableMapping, Set

from typing import Tuple

if sys.version_info >= (3, 8):
    if sys.version_info >= (3, 9):
        from collections.abc import Sequence
    else:
        from typing import Sequence

import sqlitecollections as sc

from .common import BenchmarkBase, Comparison

benchmarks_dir = os.path.dirname(os.path.abspath(__file__))

target_dict_len = 2000
target_dict = {str(i): i for i in range(target_dict_len)}
target_dict_key_t = str
target_dict_value_t = int
target_dict_t = MutableMapping[target_dict_key_t, target_dict_value_t]


class BuiltinDictBenchmarkBase:
    def __init__(self, timeout: Optional[float] = None, debug: bool = False) -> None:
        super(BuiltinDictBenchmarkBase, self).__init__(timeout=timeout, debug=debug)
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
    def __init__(self, timeout: Optional[float] = None, debug: bool = False) -> None:
        super(SqliteCollectionsDictBenchmarkBase, self).__init__(timeout=timeout, debug=debug)
        self._sut_orig = sc.Dict[target_dict_key_t, target_dict_value_t](data=target_dict)
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
    @property
    def subject(self) -> str:
        return "`__init__`"

    def assertion(self, result: target_dict_t) -> bool:
        return len(result) == target_dict_len and all((result[k] == target_dict[k] for k in target_dict.keys()))


class BenchmarkLenBase(BenchmarkBase[target_dict_value_t]):
    @property
    def subject(self) -> str:
        return "`__len__`"

    def exec(self) -> target_dict_value_t:
        self._sut: target_dict_t
        return len(self._sut)

    def assertion(self, result: target_dict_value_t) -> bool:
        return result == target_dict_len


class BenchmarkGetitemBase(BenchmarkBase[target_dict_value_t]):
    @property
    def subject(self) -> str:
        return "`__getitem__`"

    def exec(self) -> target_dict_value_t:
        self._sut: target_dict_t
        return self._sut["651"]

    def assertion(self, result: target_dict_value_t) -> bool:
        return result == 651


class BenchmarkSetitemReplaceBase(BenchmarkBase[target_dict_t]):
    @property
    def subject(self) -> str:
        return "`__setitem__` (replace)"

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
    @property
    def subject(self) -> str:
        return "`__setitem__` (add new item)"

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
    @property
    def subject(self) -> str:
        return "`__delitem__`"

    def exec(self) -> target_dict_t:
        self._sut: target_dict_t
        del self._sut["651"]
        return self._sut

    def assertion(self, result: target_dict_t) -> bool:
        return len(result) == (target_dict_len - 1) and all(
            (result[k] == target_dict[k] for k in target_dict.keys() if k != "651")
        )


class BenchmarkContainsBase(BenchmarkBase[bool]):
    @property
    def subject(self) -> str:
        return "`__contains__`"

    def exec(self) -> bool:
        self._sut: target_dict_t
        return "651" in self._sut

    def assertion(self, result: bool) -> bool:
        return result


class BenchmarkNotContainsBase(BenchmarkBase[bool]):
    @property
    def subject(self) -> str:
        return "`__contains__` (unsuccessful search)"

    def exec(self) -> bool:
        self._sut: target_dict_t
        return "-651" not in self._sut

    def assertion(self, result: bool) -> bool:
        return result


class BenchmarkIterBase(BenchmarkBase[Set[target_dict_key_t]]):
    @property
    def subject(self) -> str:
        return "`__iter__`"

    def exec(self) -> Set[target_dict_key_t]:
        self._sut: target_dict_t
        return set(self._sut)

    def assertion(self, result: Set[target_dict_key_t]) -> bool:
        return result == set(target_dict)


class BenchmarkClearBase(BenchmarkBase[target_dict_t]):
    @property
    def subject(self) -> str:
        return "`clear`"

    def exec(self) -> target_dict_t:
        self._sut: target_dict_t
        self._sut.clear()
        return self._sut

    def assertion(self, result: target_dict_t) -> bool:
        return len(result) == 0


class BenchmarkCopyBase(BenchmarkBase[target_dict_t]):
    @property
    def subject(self) -> str:
        return "`copy`"

    def exec(self) -> target_dict_t:
        self._sut: target_dict_t
        retval = self._sut.copy()
        return retval

    def assertion(self, result: target_dict_t) -> bool:
        return len(result) == target_dict_len and all((result[k] == target_dict[k] for k in target_dict))


class BenchmarkGetBase(BenchmarkBase[target_dict_value_t]):
    @property
    def subject(self) -> str:
        return "`get`"

    def exec(self) -> target_dict_value_t:
        self._sut: target_dict_t
        return cast(target_dict_value_t, self._sut.get("651"))

    def assertion(self, result: target_dict_value_t) -> bool:
        return result == 651


class BenchmarkGetDefaultBase(BenchmarkBase[target_dict_value_t]):
    @property
    def subject(self) -> str:
        return "`get (unsuccessful search)`"

    def exec(self) -> target_dict_value_t:
        self._sut: target_dict_t
        return self._sut.get("-1", -1)

    def assertion(self, result: target_dict_value_t) -> bool:
        return result == -1


class BenchmarkItemsBase(BenchmarkBase[Set[Tuple[target_dict_key_t, target_dict_value_t]]]):
    @property
    def subject(self) -> str:
        return "`items`"

    def exec(self) -> Set[Tuple[target_dict_key_t, target_dict_value_t]]:
        self._sut: target_dict_t
        return set(self._sut.items())

    def assertion(self, result: Set[Tuple[target_dict_key_t, target_dict_value_t]]) -> bool:
        return result == set(target_dict.items())


class BenchmarkKeysBase(BenchmarkBase[Set[target_dict_key_t]]):
    @property
    def subject(self) -> str:
        return "`keys`"

    def exec(self) -> Set[target_dict_key_t]:
        self._sut: target_dict_t
        return set(self._sut.keys())

    def assertion(self, result: Set[target_dict_key_t]) -> bool:
        return result == set(target_dict.keys())


class BenchmarkPopBase(BenchmarkBase[Tuple[target_dict_value_t, target_dict_t]]):
    @property
    def subject(self) -> str:
        return "`pop`"

    def exec(self) -> Tuple[target_dict_value_t, target_dict_t]:
        self._sut: target_dict_t
        val = self._sut.pop("651")
        return (val, self._sut)

    def assertion(self, result: Tuple[target_dict_value_t, target_dict_t]) -> bool:
        return result[0] == 651 and len(result[1]) == (target_dict_len - 1) and "651" not in result[1]


class BenchmarkPopDefaultBase(BenchmarkBase[Tuple[target_dict_value_t, target_dict_t]]):
    @property
    def subject(self) -> str:
        return "`pop (unsuccessful search)`"

    def exec(self) -> Tuple[target_dict_value_t, target_dict_t]:
        self._sut: target_dict_t
        val = self._sut.pop("-1", -1)
        return (val, self._sut)

    def assertion(self, result: Tuple[target_dict_value_t, target_dict_t]) -> bool:
        return result[0] == -1 and len(result[1]) == target_dict_len


class BenchmarkPopitemBase(BenchmarkBase[Tuple[Tuple[target_dict_key_t, target_dict_value_t], target_dict_t]]):
    @property
    def subject(self) -> str:
        return "`popitem`"

    def exec(self) -> Tuple[Tuple[target_dict_key_t, target_dict_value_t], target_dict_t]:
        self._sut: target_dict_t
        retval = self._sut.popitem()
        return (retval, self._sut)

    def assertion(self, result: Tuple[Tuple[target_dict_key_t, target_dict_value_t], target_dict_t]) -> bool:
        return len(result[1]) == (target_dict_len - 1) and result[0][0] not in result


if sys.version_info >= (3, 8):

    class BenchmarkReversedBase(BenchmarkBase[Sequence[target_dict_key_t]]):
        @property
        def subject(self) -> str:
            return "`reversed`"

        def exec(self) -> Sequence[target_dict_key_t]:
            self._sut: target_dict_t
            retkeys = list(reversed(self._sut))
            return retkeys

        def assertion(self, result: Sequence[target_dict_key_t]):
            keys = list(target_dict)
            return len(result) == len(keys) and all((a == b for a, b in zip(result, keys[::-1])))


class BenchmarkSetdefaultBase(BenchmarkBase[Tuple[target_dict_value_t, target_dict_t]]):
    @property
    def subject(self) -> str:
        return "`setdefault`"

    def exec(self) -> Tuple[target_dict_value_t, target_dict_t]:
        self._sut: target_dict_t
        retval = self._sut.setdefault("651")
        return (retval, self._sut)

    def assertion(self, result: Tuple[target_dict_value_t, target_dict_t]) -> bool:
        return len(result[1]) == target_dict_len and result[0] == 651


class BenchmarkSetdefaultAddItemBase(BenchmarkBase[Tuple[target_dict_value_t, target_dict_t]]):
    @property
    def subject(self) -> str:
        return "`setdefault (unsuccessful search)`"

    def exec(self) -> Tuple[target_dict_value_t, target_dict_t]:
        self._sut: target_dict_t
        retval = self._sut.setdefault("-1", -1)
        return (retval, self._sut)

    def assertion(self, result: Tuple[target_dict_value_t, target_dict_t]) -> bool:
        return len(result[1]) == (target_dict_len + 1) and result[0] == -1 and result[1]["-1"] == -1


class BenchmarkUpdateBase(BenchmarkBase[target_dict_t]):
    @property
    def subject(self) -> str:
        return "`update`"

    def exec(self) -> target_dict_t:
        self._sut.update({"-1": -1})
        return self._sut

    def assertion(self, result: target_dict_t) -> bool:
        return len(self._sut) == (target_dict_len + 1) and self._sut["-1"] == -1


class BenchmarkUpdateManyBase(BenchmarkBase[target_dict_t]):
    @property
    def subject(self) -> str:
        return "`update` (many)"

    def exec(self) -> target_dict_t:
        self._sut.update((str(-v), -v) for v in target_dict.values())
        return self._sut

    def assertion(self, result: target_dict_t) -> bool:
        return len(self._sut) == (target_dict_len * 2 - 1) and self._sut["-1"] == -1


class BenchmarkValuesBase(BenchmarkBase[Set[target_dict_value_t]]):
    @property
    def subject(self) -> str:
        return "`values`"

    def exec(self) -> Set[target_dict_value_t]:
        self._sut: target_dict_t
        return set(self._sut.values())

    def assertion(self, result: Set[target_dict_value_t]) -> bool:
        return result == set(target_dict.values())


if sys.version_info >= (3, 9):

    class BenchmarkOrBase(BenchmarkBase[target_dict_t]):
        @property
        def subject(self) -> str:
            return "`__or__`"

        def exec(self) -> target_dict_t:
            return self._sut | {"-1": -1}

        def assertion(self, result: target_dict_t) -> bool:
            return len(result) == (target_dict_len + 1) and result["-1"] == -1

    class BenchmarkOrManyBase(BenchmarkBase[target_dict_t]):
        @property
        def subject(self) -> str:
            return "`__or__` (many)"

        def exec(self) -> target_dict_t:
            return self._sut | {str(-v): -v for v in target_dict.values()}

        def assertion(self, result: target_dict_t) -> bool:
            return len(result) == (target_dict_len * 2 - 1) and result["-1"] == -1

    class BenchmarkIorBase(BenchmarkBase[target_dict_t]):
        @property
        def subject(self) -> str:
            return "`__ior__`"

        def exec(self) -> target_dict_t:
            self._sut |= {"-1": -1}
            return self._sut

        def assertion(self, result: target_dict_t) -> bool:
            return len(result) == (target_dict_len + 1) and result["-1"] == -1

    class BenchmarkIorManyBase(BenchmarkBase[target_dict_t]):
        @property
        def subject(self) -> str:
            return "`__ior__` (many)"

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
        return sc.Dict[target_dict_key_t, target_dict_value_t](target_dict.items())
