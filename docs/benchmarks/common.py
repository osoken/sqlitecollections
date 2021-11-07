from timeit import timeit
from typing import Tuple

from memory_profiler import memory_usage


class Benchmark:
    def __init__(self, statement: str):
        self.statement = statement

    def __call__(self) -> Tuple[float, float]:
        timing = None

        current_memory = max(memory_usage(lambda: 0))

        def _():
            nonlocal timing
            timing = timeit(self.statement, number=10, globals=globals())

        memory = max(memory_usage((_,))) - current_memory
        return memory, timing
