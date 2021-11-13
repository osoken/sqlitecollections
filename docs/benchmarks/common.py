import statistics
import sys
import time
from abc import ABCMeta, abstractmethod
from timeit import timeit
from typing import Any, Tuple

if sys.version_info > (3, 9):
    from collections.abc import Mapping
else:
    from typing import Mapping

from memory_profiler import memory_usage, profile


class BenchmarkResult:
    def __init__(self, timing: float, memory: float):
        self._timing = timing
        self._memory = memory

    @property
    def timing(self) -> float:
        return self._timing

    @property
    def memory(self) -> float:
        return self._memory

    def dict(self) -> Mapping[str, float]:
        return {"timing": self.timing, "memory": self.memory}


class BenchmarkBase(metaclass=ABCMeta):
    def __init__(self, number: int = 32, interval: float = 0.01):
        self._number = number
        self._interval = interval

    @abstractmethod
    def setup(self) -> None:
        pass

    @abstractmethod
    def teardown(self) -> None:
        pass

    @abstractmethod
    def exec(self) -> Any:
        pass

    @abstractmethod
    def assertion(self, result: Any) -> bool:
        return True

    def _get_current_memory(self) -> float:
        return memory_usage((lambda: None,), max_usage=True)

    def __call__(self) -> BenchmarkResult:
        memory_after_setup_buffer = []
        memory_during_exec_buffer = []
        timing_buffer = []
        for i in range(self._number):
            self.setup()
            memory_after_setup = self._get_current_memory()

            t1 = time.time()
            memory_during_exec, result = memory_usage(
                (self.exec,), interval=self._interval, max_usage=True, retval=True, max_iterations=1
            )
            t2 = time.time()
            if not self.assertion(result):
                raise AssertionError()
            self.teardown()
            timing = t2 - t1
            timing_buffer.append(timing)
            memory_after_setup_buffer.append(memory_after_setup)
            memory_during_exec_buffer.append(memory_during_exec)
        return BenchmarkResult(
            statistics.mean(timing_buffer),
            max(m2 - m1 for m1, m2 in zip(memory_after_setup_buffer, memory_during_exec_buffer)),
        )


class BenchmarkRatio:
    def __init__(self, timing: float, memory: float):
        self._timing = timing
        self._memory = memory

    @property
    def timing(self) -> float:
        return self._timing

    @property
    def memory(self) -> float:
        return self._memory

    def dict(self) -> Mapping[str, float]:
        return {"timing": self.timing, "memory": self.memory}

    @classmethod
    def calc(self, one: BenchmarkResult, another: BenchmarkResult) -> "BenchmarkRatio":
        if one.timing == 0.0:
            if another.timing == 0.0:
                timing_ratio = 1.0
            else:
                timing_ratio = float("inf")
        else:
            timing_ratio = another.timing / one.timing
        if one.memory == 0.0:
            if another.memory == 0.0:
                memory_ratio = 1.0
            else:
                memory_ratio = float("inf")
        else:
            memory_ratio = another.memory / one.memory
        return BenchmarkRatio(timing_ratio, memory_ratio)


class ComparisonResult:
    def __init__(self, one: BenchmarkResult, another: BenchmarkResult):
        self._one = one

        self._another = another

    @property
    def ratio(self) -> BenchmarkRatio:
        return BenchmarkRatio.calc(self._one, self._another)

    def dict(self) -> Mapping[str, Mapping[str, float]]:
        return {"one": self._one.dict(), "another": self._another.dict(), "ratio": self.ratio.dict()}


class Comparison:
    def __init__(self, one: BenchmarkBase, another: BenchmarkBase):
        self._one = one
        self._another = another

    def __call__(
        self,
    ) -> ComparisonResult:
        one_result = self._one()
        another_result = self._another()
        return ComparisonResult(one_result, another_result)
