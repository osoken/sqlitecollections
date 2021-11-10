import gc
import statistics
import time
from timeit import timeit
from typing import Tuple

from memory_profiler import memory_usage, profile


class BenchmarkBase:
    def __init__(self, number: int = 32, interval: float = 0.1):
        self._memory_after_setup_buffer = []
        self._memory_during_exec_buffer = []
        self._timing_buffer = []
        self._number = number
        self._interval = interval

    def setup(self):
        pass

    def teardown(self):
        pass

    def exec(self):
        pass

    def _get_current_memory(self):
        return max(memory_usage((lambda: None,)))

    def __call__(self):
        for i in range(self._number):
            gc.collect()
            gc.collect()
            self.setup()
            memory_after_setup = self._get_current_memory()
            t1 = time.time()
            memory_during_exec_buffer = max(memory_usage((self.exec,), interval=self._interval))
            t2 = time.time()
            self.teardown()
            timing = t2 - t1
            self._timing_buffer.append(timing)
            self._memory_after_setup_buffer.append(memory_after_setup)
            self._memory_during_exec_buffer.append(memory_during_exec_buffer)

    def summary(self):
        return statistics.mean(self._timing_buffer), max(
            m2 - m1 for m1, m2 in zip(self._memory_after_setup_buffer, self._memory_during_exec_buffer)
        )
