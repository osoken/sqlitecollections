from timeit import timeit
from typing import Tuple

from memory_profiler import memory_usage


def benchmark_statement(
    statement: str, number: int = 10, interval: float = 0.1, scope: dict = globals()
) -> Tuple[float, float]:
    timing = None

    current_memory = max(memory_usage(lambda: 0))

    def _():
        nonlocal timing
        timing = timeit(statement, number=number, globals=scope)

    memory = max(memory_usage((_,), interval=interval)) - current_memory
    return memory, timing
