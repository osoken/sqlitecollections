import gc
import os
import sys
from typing import Any, cast

if sys.version_info > (3, 9):
    from collections.abc import MutableMapping, Tuple
else:
    from typing import MutableMapping, Tuple

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


class BenchmarkLenBase(BenchmarkBase[int]):
    def exec(self) -> int:
        return len(self._sut)

    def assertion(self, result: int) -> bool:
        return result == target_dict_len


class BenchmarkGetitemBase(BenchmarkBase[target_dict_value_t]):
    def exec(self) -> target_dict_value_t:
        return self._sut["651"]

    def assertion(self, result: target_dict_value_t) -> bool:
        return result == 651


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


if __name__ == "__main__":
    print(Comparison("`__init__`", BuiltinDictBenchmarkInit(), SqliteCollectionsDictBenchmarkInit())().dict())
    print(Comparison("`__len__`", BuiltinDictBenchmarkLen(), SqliteCollectionsDictBenchmarkLen())().dict())
    print(Comparison("`__getitem__`", BuiltinDictBenchmarkGetitem(), SqliteCollectionsDictBenchmarkGetitem())().dict())
