import sqlite3
import sys
from typing import Any, Optional, Union
from unittest import TestCase
from unittest.mock import MagicMock, patch

if sys.version_info > (3, 9):
    from collections.abc import Callable, Iterable
else:
    from typing import Callable, Iterable

from test_base import ConcreteSqliteCollectionClass

from sqlitecollections import base, factory


class ConcreteFactory(factory.SequenceFactoryBase[str]):
    @classmethod
    def _get_container_class(
        cls,
    ) -> Callable[..., ConcreteSqliteCollectionClass]:
        return ConcreteSqliteCollectionClass


class FactoryBaseTestCase(TestCase):
    @patch("sqlitecollections.factory.tidy_connection")
    def test_init_with_defaults(self, tidy_connection: MagicMock) -> None:
        sut = ConcreteFactory()
        self.assertEqual(sut.connection, tidy_connection.return_value)
        self.assertEqual(sut.serializer, base.SqliteCollectionBase._default_serializer)
        self.assertEqual(sut.deserializer, base.SqliteCollectionBase._default_deserializer)
        tidy_connection.assert_called_once_with(None)

    @patch("sqlitecollections.factory.tidy_connection")
    def test_init_with_specified_values(self, tidy_connection: MagicMock) -> None:
        connection = MagicMock(spec=str)
        serializer = MagicMock(spec=Callable[[str], bytes])
        deserializer = MagicMock(spec=Callable[[bytes], str])
        sut = ConcreteFactory(connection=connection, serializer=serializer, deserializer=deserializer)
        self.assertEqual(sut.connection, tidy_connection.return_value)
        self.assertEqual(sut.serializer, serializer)
        self.assertEqual(sut.deserializer, deserializer)
        tidy_connection.assert_called_once_with(connection)


class SequenceFactoryBaseTestCase(TestCase):
    @patch.object(ConcreteFactory, "_get_container_class")
    @patch("sqlitecollections.factory.tidy_connection")
    def test_create(self, _: MagicMock, ConcreteFactory_get_container_class: MagicMock) -> None:
        sut = ConcreteFactory()
        expected = ConcreteFactory_get_container_class.return_value.return_value
        actual = sut.create()
        self.assertEqual(actual, expected)
        ConcreteFactory_get_container_class.return_value.assert_called_once_with(
            connection=sut.connection, serializer=sut.serializer, deserializer=sut.deserializer
        )

    @patch.object(ConcreteFactory, "_get_container_class")
    @patch("sqlitecollections.factory.tidy_connection")
    def test_create_with_data(self, _: MagicMock, ConcreteFactory_get_container_class: MagicMock) -> None:
        sut = ConcreteFactory()
        data = ["a", "b", "c"]
        expected = ConcreteFactory_get_container_class.return_value.return_value
        actual = sut.create(data)
        self.assertEqual(actual, expected)
        ConcreteFactory_get_container_class.return_value.assert_called_once_with(
            data=data, connection=sut.connection, serializer=sut.serializer, deserializer=sut.deserializer
        )

    @patch("sqlitecollections.factory.SequenceFactoryBase.create")
    @patch("sqlitecollections.factory.tidy_connection")
    def test_call_is_alias_of_create(self, _: MagicMock, create: MagicMock) -> None:
        sut = ConcreteFactory()
        data = MagicMock(spec=Iterable[str])
        expected = create.return_value
        actual = sut(data)
        self.assertEqual(actual, expected)
        create.assert_called_once_with(data)


class SetFactoryTestCase(TestCase):
    @patch("sqlitecollections.factory.Set")
    def test_call_with_no_arg(self, Set: MagicMock) -> None:
        conn = MagicMock(spec=sqlite3.Connection)
        serializer = MagicMock(spec=Callable[[str], bytes])
        deserializer = MagicMock(spec=Callable[[bytes], str])
        sut = factory.SetFactory[str](connection=conn, serializer=serializer, deserializer=deserializer)
        expected = Set.__getitem__.return_value.return_value
        actual = sut()
        self.assertEqual(actual, expected)
        Set.__getitem__.return_value.assert_called_once_with(
            connection=conn, serializer=serializer, deserializer=deserializer
        )

    @patch("sqlitecollections.factory.Set")
    def test_call_with_arg(self, Set: MagicMock) -> None:
        conn = MagicMock(spec=sqlite3.Connection)
        serializer = MagicMock(spec=Callable[[str], bytes])
        deserializer = MagicMock(spec=Callable[[bytes], str])
        sut = factory.SetFactory[str](connection=conn, serializer=serializer, deserializer=deserializer)
        expected = Set.__getitem__.return_value.return_value
        actual = sut(["1", "2", "3"])
        self.assertEqual(actual, expected)
        Set.__getitem__.return_value.assert_called_once_with(
            data=["1", "2", "3"], connection=conn, serializer=serializer, deserializer=deserializer
        )


class ListFactoryTestCase(TestCase):
    @patch("sqlitecollections.factory.List")
    def test_call_with_no_arg(self, List: MagicMock) -> None:
        conn = MagicMock(spec=sqlite3.Connection)
        serializer = MagicMock(spec=Callable[[str], bytes])
        deserializer = MagicMock(spec=Callable[[bytes], str])
        sut = factory.ListFactory[str](connection=conn, serializer=serializer, deserializer=deserializer)
        expected = List.__getitem__.return_value.return_value
        actual = sut()
        self.assertEqual(actual, expected)
        List.__getitem__.return_value.assert_called_once_with(
            connection=conn, serializer=serializer, deserializer=deserializer
        )

    @patch("sqlitecollections.factory.List")
    def test_call_with_arg(self, List: MagicMock) -> None:
        conn = MagicMock(spec=sqlite3.Connection)
        serializer = MagicMock(spec=Callable[[str], bytes])
        deserializer = MagicMock(spec=Callable[[bytes], str])
        sut = factory.ListFactory[str](connection=conn, serializer=serializer, deserializer=deserializer)
        expected = List.__getitem__.return_value.return_value
        actual = sut(["1", "2", "3"])
        self.assertEqual(actual, expected)
        List.__getitem__.return_value.assert_called_once_with(
            data=["1", "2", "3"], connection=conn, serializer=serializer, deserializer=deserializer
        )


class DictFactoryTestCase(TestCase):
    @patch("sqlitecollections.factory.Dict")
    def test_call_with_no_arg(self, Dict: MagicMock) -> None:
        conn = MagicMock(spec=sqlite3.Connection)
        key_serializer = MagicMock(spec=Callable[[str], bytes])
        key_deserializer = MagicMock(spec=Callable[[bytes], str])
        value_serializer = MagicMock(spec=Callable[[int], bytes])
        value_deserializer = MagicMock(spec=Callable[[bytes], int])
        sut = factory.DictFactory[str, int](
            connection=conn,
            key_serializer=key_serializer,
            key_deserializer=key_deserializer,
            value_serializer=value_serializer,
            value_deserializer=value_deserializer,
        )
        expected = Dict.__getitem__.return_value.return_value
        actual = sut()
        self.assertEqual(actual, expected)
        Dict.__getitem__.return_value.assert_called_once_with(
            connection=conn,
            key_serializer=key_serializer,
            key_deserializer=key_deserializer,
            value_serializer=value_serializer,
            value_deserializer=value_deserializer,
        )

    @patch("sqlitecollections.factory.chain")
    @patch("sqlitecollections.factory.Dict")
    def test_call_with_arg(self, Dict: MagicMock, chain: MagicMock) -> None:

        conn = MagicMock(spec=sqlite3.Connection)
        key_serializer = MagicMock(spec=Callable[[str], bytes])
        key_deserializer = MagicMock(spec=Callable[[bytes], str])
        value_serializer = MagicMock(spec=Callable[[int], bytes])
        value_deserializer = MagicMock(spec=Callable[[bytes], int])
        sut = factory.DictFactory[str, int](
            connection=conn,
            key_serializer=key_serializer,
            key_deserializer=key_deserializer,
            value_serializer=value_serializer,
            value_deserializer=value_deserializer,
        )
        expected = Dict.__getitem__.return_value.return_value
        data = (("a", 1), ("b", 2))
        actual = sut(data)
        self.assertEqual(actual, expected)
        Dict.__getitem__.return_value.assert_called_once_with(
            data=chain.return_value,
            connection=conn,
            key_serializer=key_serializer,
            key_deserializer=key_deserializer,
            value_serializer=value_serializer,
            value_deserializer=value_deserializer,
        )
        chain.assert_called_once_with(data, {}.items())

    @patch("sqlitecollections.factory.Dict")
    def test_call_with_kwarg(self, Dict: MagicMock) -> None:
        conn = MagicMock(spec=sqlite3.Connection)
        key_serializer = MagicMock(spec=Callable[[str], bytes])
        key_deserializer = MagicMock(spec=Callable[[bytes], str])
        value_serializer = MagicMock(spec=Callable[[int], bytes])
        value_deserializer = MagicMock(spec=Callable[[bytes], int])
        sut = factory.DictFactory[str, int](
            connection=conn,
            key_serializer=key_serializer,
            key_deserializer=key_deserializer,
            value_serializer=value_serializer,
            value_deserializer=value_deserializer,
        )
        expected = Dict.__getitem__.return_value.return_value
        actual = sut(a=1, b=2)
        self.assertEqual(actual, expected)
        Dict.__getitem__.return_value.assert_called_once_with(
            data={"a": 1, "b": 2},
            connection=conn,
            key_serializer=key_serializer,
            key_deserializer=key_deserializer,
            value_serializer=value_serializer,
            value_deserializer=value_deserializer,
        )
