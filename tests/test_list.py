import pickle
import sqlite3
import sys
from unittest import TestCase
from unittest.mock import MagicMock, patch

from sqlitecollections import List, RebuildStrategy

if sys.version_info > (3, 9):
    from collections.abc import Callable
else:
    from typing import Callable

from typing import Any

from test_base import SqlTestCase


class ListTestCase(SqlTestCase):
    def assert_db_state_equals(self, conn: sqlite3.Connection, expected: Any) -> None:
        return self.assert_sql_result_equals(
            conn,
            "SELECT serialized_value, item_index FROM items ORDER BY item_index",
            expected,
        )

    @patch("sqlitecollections.List._initialize", return_value=None)
    @patch("sqlitecollections.base.SqliteCollectionBase.__init__", return_value=None)
    @patch("sqlitecollections.base.SqliteCollectionBase.__del__", return_value=None)
    def test_init(
        self, SqliteCollectionBase_del: MagicMock, SqliteCollectionBase_init: MagicMock, _initialize: MagicMock
    ) -> None:
        memory_db = sqlite3.connect(":memory:")
        table_name = "items"
        serializer = MagicMock(spec=Callable[[Any], bytes])
        deserializer = MagicMock(spec=Callable[[bytes], Any])
        persist = False
        rebuild_strategy = RebuildStrategy.SKIP
        sut = List[Any](
            connection=memory_db,
            table_name=table_name,
            serializer=serializer,
            deserializer=deserializer,
            persist=persist,
            rebuild_strategy=rebuild_strategy,
        )
        SqliteCollectionBase_init.assert_called_once_with(
            connection=memory_db,
            table_name=table_name,
            serializer=serializer,
            deserializer=deserializer,
            persist=persist,
            rebuild_strategy=rebuild_strategy,
            do_initialize=True,
        )

    def test_initialize(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        sut = List[Any](connection=memory_db, table_name="items")
        self.assert_sql_result_equals(
            memory_db,
            "SELECT table_name, schema_version, container_type FROM metadata",
            [("items", sut.schema_version, sut.container_type_name)],
        )
        self.assert_db_state_equals(memory_db, [])

    def test_rebuild(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/rebuild.sql")

        def serializer(x: str) -> bytes:
            return x.upper().encode("utf-8")

        def deserializer(x: bytes) -> str:
            return str(x)

        sut = List[str](
            connection=memory_db,
            table_name="items",
            serializer=serializer,
            deserializer=deserializer,
            rebuild_strategy=RebuildStrategy.ALWAYS,
        )
        self.assert_db_state_equals(
            memory_db,
            [
                (b"A", 0),
                (b"B", 1),
                (b"C", 2),
                (b"D", 3),
                (b"E", 4),
                (b"F", 5),
                (b"G", 6),
                (b"H", 7),
            ],
        )
