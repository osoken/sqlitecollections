from abc import ABCMeta, abstractmethod
from collections import Collection
from timeit import timeit


class ContainerFactory(metaclass=ABCMeta):
    
class BenchmarkBase(metaclass=ABCMeta):
    def __init__(self, reference: Collection, sut: Collection) -> None:
        self._reference = reference
        self._sut = sut

    @abstractmethod
    @classmethod
    @property
    def name(cls) -> str:
        ...

    def __call__(self) -> None:
        reference_time = timeit(self.test(self._reference))
        sut_time = timeit(self.test(self._sut))
        return {"name": self.name, "reference": reference_time, "sut": sut_time, "ratio": sut_time / reference_time}

    @abstractmethod
    def test(self, sut: Collection) -> float:
        ...
