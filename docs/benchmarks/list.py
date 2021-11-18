import gc
import os
import random

from sqlitecollections import List

from common import BenchmarkBase, Comparison

benchmarks_dir = os.path.dirname(os.path.abspath(__file__))

target_list = list(range(10000))
random.seed(5432)
random.shuffle(target_list)


class BuiltinListBenchmarkBase(BenchmarkBase):
    def __init__(self):
        super(BuiltinListBenchmarkBase, self).__init__()
        self._sut_orig = target_list.copy()

    @property
    def name(self) -> str:
        return "builtin_list"

    def setup(self):
        self._sut = self._sut_orig.copy()

    def teardown(self):
        del self._sut
        gc.collect()
        gc.collect()


class SqliteCollectionsListBenchmarkBase(BenchmarkBase):
    def __init__(self):
        super(SqliteCollectionsListBenchmarkBase, self).__init__()
        self._sut_orig = List[int](data=target_list)

    @property
    def name(self) -> str:
        return "sqlitecollections_list"

    def setup(self):
        self._sut = self._sut_orig.copy()

    def teardown(self):
        del self._sut


class BenchmarkDelitemBase(BenchmarkBase):
    def exec(self):
        del self._sut[5000]
        return self._sut

    def assertion(self, result):
        return len(result) == 9999 and result[4999] == target_list[4999] and result[5000] == target_list[5001]


class BenchmarkGetitemBase(BenchmarkBase):
    def exec(self):
        return self._sut[5000]

    def assertion(self, result):
        return result == target_list[5000]


class BenchmarkGetitemSliceBase(BenchmarkBase):
    def exec(self):
        return self._sut[10:5010]

    def assertion(self, result):
        return all([a == b for a, b in zip(target_list[10:5010], result)])


class BenchmarkCreateWithInitialDataBase(BenchmarkBase):
    def assertion(self, result):
        return len(result) == 10000 and all(a == b for a, b in zip(result, target_list))


class BenchmarkContainsBase(BenchmarkBase):
    def exec(self):
        return 651 in self._sut

    def assertion(self, result):
        return result == True


class BenchmarkNotContainsBase(BenchmarkBase):
    def exec(self):
        return 12345 in self._sut

    def assertion(self, result):
        return result == False


class BenchmarkSetitemBase(BenchmarkBase):
    def exec(self):
        self._sut[0] = -123
        self._sut[1000] = -2
        self._sut[-3] = -10
        return self._sut

    def assertion(self, result) -> bool:
        return self._sut[0] == -123 and self._sut[1000] == -2 and self._sut[-3] == -10


class BenchmarkInsertBase(BenchmarkBase):
    def exec(self):
        self._sut.insert(0, -123)
        return self._sut

    def assertion(self, result) -> bool:
        return len(self._sut) == 10001 and self._sut[0] == -123 and self._sut[1] == target_list[0]


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
    def exec(self):
        return list(iter(target_list))


class SqliteCollectionsListBenchmarkCreateWithInitialData(BuiltinListBenchmarkBase, BenchmarkCreateWithInitialDataBase):
    def exec(self):
        return List[int](data=iter(target_list))


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
