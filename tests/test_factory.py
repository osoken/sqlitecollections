import sqlite3
import sys
from typing import Optional, Union
from unittest import TestCase
from unittest.mock import MagicMock, patch

if sys.version_info > (3, 9):
    from collections.abc import Callable, Iterable
else:
    from typing import Callable, Iterable

from sqlitecollections import base, factory

from test_base import ConcreteSqliteCollectionClass


class ConcreteFactory(factory.FactoryBase[str]):
    _container_class = ConcreteSqliteCollectionClass


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

    @patch.object(ConcreteFactory, "_container_class")
    @patch("sqlitecollections.factory.tidy_connection")
    def test_create(self, _: MagicMock, ConcreteFactory_container_class: MagicMock) -> None:
        sut = ConcreteFactory()
        expected = ConcreteFactory._container_class.return_value
        actual = sut.create()
        self.assertEqual(actual, expected)
        ConcreteFactory_container_class.assert_called_once_with(
            connection=sut.connection, serializer=sut.serializer, deserializer=sut.deserializer
        )

    @patch.object(ConcreteFactory, "_container_class")
    @patch("sqlitecollections.factory.tidy_connection")
    def test_create_with_data(self, _: MagicMock, ConcreteFactory_container_class: MagicMock) -> None:
        sut = ConcreteFactory()
        data = ["a", "b", "c"]
        expected = ConcreteFactory._container_class.return_value
        actual = sut.create(data)
        self.assertEqual(actual, expected)
        ConcreteFactory_container_class.assert_called_once_with(
            data=data,
            connection=sut.connection, serializer=sut.serializer, deserializer=sut.deserializer
        )

    @patch("sqlitecollections.factory.FactoryBase.create")
    @patch("sqlitecollections.factory.tidy_connection")
    def test_call_is_alias_of_create(self, _: MagicMock, create: MagicMock) -> None:
        sut = ConcreteFactory()
        data = MagicMock(spec=Iterable[str])
        expected = create.return_value
        actual = sut(data)
        self.assertEqual(actual, expected)
        create.assert_called_once_with(data)
