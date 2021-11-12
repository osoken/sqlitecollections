import statistics
import sys
import time
from abc import ABCMeta, abstractmethod
from timeit import timeit
from typing import Any, Tuple

if sys.version_info > (3, 9):
    from collections.abc import Dict
else:
    from typing import Dict

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

    def dict(self) -> Dict[str, float]:
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
