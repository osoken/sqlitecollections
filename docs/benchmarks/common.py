import gc
import statistics
import time
from abc import ABCMeta, abstractmethod
from timeit import timeit
from typing import Any, Tuple

from memory_profiler import memory_usage, profile


class BenchmarkBase(metaclass=ABCMeta):
    def __init__(self, number: int = 32, interval: float = 0.01):
        self._memory_after_setup_buffer = []
        self._memory_during_exec_buffer = []
        self._timing_buffer = []
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

    def __call__(self) -> None:
        for i in range(self._number):
            gc.collect()
            gc.collect()
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
            self._timing_buffer.append(timing)
            self._memory_after_setup_buffer.append(memory_after_setup)
            self._memory_during_exec_buffer.append(memory_during_exec)

    def summary(self) -> Tuple[float, float]:
        return statistics.mean(self._timing_buffer), max(
            m2 - m1 for m1, m2 in zip(self._memory_after_setup_buffer, self._memory_during_exec_buffer)
        )
