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


if __name__ == "__main__":
    print(Comparison(BuiltinListBenchmarkDelitem(), SqliteCollectionsListBenchmarkDelitem())().dict())
    print(Comparison(BuiltinListBenchmarkGetitem(), SqliteCollectionsListBenchmarkGetitem())().dict())
    print(Comparison(BuiltinListBenchmarkGetitemSlice(), SqliteCollectionsListBenchmarkGetitemSlice())().dict())
    print(
        Comparison(
            BuiltinListBenchmarkCreateWithInitialData(), SqliteCollectionsListBenchmarkCreateWithInitialData()
        )().dict()
    )
    print(Comparison(BuiltinListBenchmarkContains(), SqliteCollectionsListBenchmarkContains())().dict())
    print(Comparison(BuiltinListBenchmarkNotContains(), SqliteCollectionsListBenchmarkNotContains())().dict())
