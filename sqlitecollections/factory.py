import sqlite3
import sys
from abc import ABCMeta, abstractmethod
from typing import Generic, Optional, Union

if sys.version_info > (3, 9):
    from collections.abc import Callable, Iterable
else:
    from typing import Callable, Iterable

from .base import SqliteCollectionBase, T, tidy_connection


class FactoryBase(Generic[T], metaclass=ABCMeta):
    def __init__(
        self,
        connection: Optional[Union[str, sqlite3.Connection]] = None,
        serializer: Optional[Callable[[T], bytes]] = None,
        deserializer: Optional[Callable[[bytes], T]] = None,
    ):
        self._connection = tidy_connection(connection)
        self._serializer = SqliteCollectionBase._default_serializer if serializer is None else serializer
        self._deserializer = SqliteCollectionBase._default_deserializer if deserializer is None else deserializer

    @property
    def connection(self) -> sqlite3.Connection:
        return self._connection

    @property
    def serializer(self) -> Callable[[T], bytes]:
        return self._serializer

    @property
    def deserializer(self) -> Callable[[bytes], T]:
        return self._deserializer

    @classmethod
    @abstractmethod
    def _get_container_class(
        cls,
    ) -> Callable[..., SqliteCollectionBase[T],]:
        ...

    def create(self, __data: Optional[Iterable[T]] = None) -> SqliteCollectionBase[T]:
        if __data is None:
            return self._get_container_class()(
                connection=self.connection, serializer=self.serializer, deserializer=self.deserializer
            )
        return self._get_container_class()(
            data=__data, connection=self.connection, serializer=self.serializer, deserializer=self.deserializer
        )

    def __call__(self, __data: Optional[Iterable[T]] = None) -> SqliteCollectionBase[T]:
        return self.create(__data)
