import sqlite3
import sys
from abc import ABCMeta, abstractmethod
from itertools import chain
from typing import Generic, Optional, Tuple, Union, cast

if sys.version_info >= (3, 9):
    from collections.abc import Callable, Iterable, Mapping
else:
    from typing import Callable, Iterable, Mapping

from .base import KT, VT, SqliteCollectionBase, T, tidy_connection
from .dict import Dict
from .list import List
from .set import Set


class FactoryBase(Generic[T], metaclass=ABCMeta):
    def __init__(
        self,
        connection: Optional[Union[str, sqlite3.Connection]] = None,
        table_name: Optional[str] = None,
        serializer: Optional[Callable[[T], bytes]] = None,
        deserializer: Optional[Callable[[bytes], T]] = None,
    ):
        self._connection = tidy_connection(connection)
        self._table_name = table_name
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

    @property
    def table_name(self) -> Union[str, None]:
        return self._table_name

    @classmethod
    @abstractmethod
    def _get_container_class(
        cls,
    ) -> Callable[..., SqliteCollectionBase[T]]:
        ...


class SequenceFactoryBase(FactoryBase[T]):
    def create(self, __data: Optional[Iterable[T]] = None) -> SqliteCollectionBase[T]:
        if __data is None:
            return self._get_container_class()(
                connection=self.connection,
                table_name=self.table_name,
                serializer=self.serializer,
                deserializer=self.deserializer,
            )
        return self._get_container_class()(
            __data,
            connection=self.connection,
            table_name=self.table_name,
            serializer=self.serializer,
            deserializer=self.deserializer,
        )

    def __getitem__(self, table_name: str) -> "SequenceFactoryBase[T]":
        return self.__class__(
            connection=self.connection,
            table_name=table_name,
            serializer=self.serializer,
            deserializer=self.deserializer,
        )

    def __call__(self, __data: Optional[Iterable[T]] = None) -> SqliteCollectionBase[T]:
        return self.create(__data)


class SetFactory(SequenceFactoryBase[T]):
    @classmethod
    def _get_container_class(cls) -> Callable[..., Set[T]]:
        return Set[T]


class ListFactory(SequenceFactoryBase[T]):
    @classmethod
    def _get_container_class(cls) -> Callable[..., List[T]]:
        return List[T]


class DictFactory(FactoryBase[KT], Generic[KT, VT]):
    def __init__(
        self,
        connection: Optional[Union[str, sqlite3.Connection]] = None,
        table_name: Optional[str] = None,
        key_serializer: Optional[Callable[[KT], bytes]] = None,
        key_deserializer: Optional[Callable[[bytes], KT]] = None,
        value_serializer: Optional[Callable[[VT], bytes]] = None,
        value_deserializer: Optional[Callable[[bytes], VT]] = None,
    ):
        super(DictFactory, self).__init__(
            connection=connection, table_name=table_name, serializer=key_serializer, deserializer=key_deserializer
        )
        self._value_serializer = (
            cast(Callable[[VT], bytes], self.key_serializer) if value_serializer is None else value_serializer
        )
        self._value_deserializer = (
            cast(Callable[[bytes], VT], self.key_deserializer) if value_deserializer is None else value_deserializer
        )

    @classmethod
    def _get_container_class(cls) -> Callable[..., Dict[KT, VT]]:
        return Dict[KT, VT]

    @property
    def key_serializer(self) -> Callable[[KT], bytes]:
        return self.serializer

    @property
    def key_deserializer(self) -> Callable[[bytes], KT]:
        return self.deserializer

    @property
    def value_serializer(self) -> Callable[[VT], bytes]:
        return self._value_serializer

    @property
    def value_deserializer(self) -> Callable[[bytes], VT]:
        return self._value_deserializer

    def __getitem__(self, table_name: str) -> "DictFactory[KT, VT]":
        return self.__class__(
            connection=self.connection,
            table_name=table_name,
            key_serializer=self.key_serializer,
            key_deserializer=self.key_deserializer,
            value_serializer=self.value_serializer,
            value_deserializer=self.value_deserializer,
        )

    def create(
        self, __data: Optional[Union[Iterable[Tuple[KT, VT]], Mapping[KT, VT]]] = None, **kwargs: VT
    ) -> Dict[KT, VT]:
        if __data is None:
            if len(kwargs) == 0:
                return self._get_container_class()(
                    connection=self.connection,
                    table_name=self.table_name,
                    key_serializer=self.key_serializer,
                    key_deserializer=self.key_deserializer,
                    value_serializer=self.value_serializer,
                    value_deserializer=self.value_deserializer,
                )
            return self._get_container_class()(
                kwargs,
                connection=self.connection,
                table_name=self.table_name,
                key_serializer=self.key_serializer,
                key_deserializer=self.key_deserializer,
                value_serializer=self.value_serializer,
                value_deserializer=self.value_deserializer,
            )
        return self._get_container_class()(
            chain(__data.items() if isinstance(__data, Mapping) else __data, kwargs.items()),
            connection=self.connection,
            table_name=self.table_name,
            key_serializer=self.key_serializer,
            key_deserializer=self.key_deserializer,
            value_serializer=self.value_serializer,
            value_deserializer=self.value_deserializer,
        )

    def __call__(
        self, __data: Optional[Union[Iterable[Tuple[KT, VT]], Mapping[KT, VT]]] = None, **kwargs: VT
    ) -> Dict[KT, VT]:
        return self.create(__data, **kwargs)
