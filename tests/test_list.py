import pickle
import sqlite3
import sys
from unittest import TestCase
from unittest.mock import MagicMock, patch

from sqlitecollections import List, RebuildStrategy

if sys.version_info > (3, 9):
    from collections.abc import Callable, Sequence
else:
    from typing import Callable, Sequence

from typing import Any, Tuple

from test_base import SqlTestCase


class ListTestCase(SqlTestCase):
    def assert_db_state_equals(self, conn: sqlite3.Connection, expected: Any, table_name: str = "items") -> None:
        return self.assert_sql_result_equals(
            conn,
            f"SELECT serialized_value, item_index FROM {table_name} ORDER BY item_index",
            expected,
        )

    def assert_items_table_only(self, conn: sqlite3.Connection) -> None:
        return self.assert_metadata_state_equals(conn, [("items", "0", "List")])

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

    def test_getitem_int(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/getitem_int.sql")
        sut = List[str](connection=memory_db, table_name="items")
        expected = "a"
        actual = sut[0]
        self.assertEqual(actual, expected)

        expected = "b"
        actual = sut[1]
        self.assertEqual(actual, expected)

        expected = "c"
        actual = sut[2]
        self.assertEqual(actual, expected)

        with self.assertRaisesRegex(IndexError, "list index out of range"):
            _ = sut[3]

        expected = "c"
        actual = sut[-1]
        self.assertEqual(actual, expected)
        expected = "b"
        actual = sut[-2]
        self.assertEqual(actual, expected)
        expected = "a"
        actual = sut[-3]
        self.assertEqual(actual, expected)

        with self.assertRaisesRegex(IndexError, "list index out of range"):
            _ = sut[-4]

    def test_getitem_slice(self) -> None:
        from itertools import product

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/getitem_slice.sql")
        sut = List[str](connection=memory_db, table_name="items")
        expected_array = [chr(i) for i in range(ord("a"), ord("z") + 1)]

        def generate_expectation(s: slice) -> Sequence[Tuple[bytes, int]]:
            return [(pickle.dumps(c), i) for i, c in enumerate(expected_array[s])]

        for si in product(
            (None, -100, -20, 0, 20, 100), (None, -100, -20, 0, 20, 100), (None, -100, -20, -10, 10, 20, 100)
        ):
            s = slice(*si)
            actual = sut[s]
            expected = generate_expectation(s)
            self.assert_db_state_equals(
                memory_db,
                expected,
                actual.table_name,
            )
        del actual
        self.assert_items_table_only(memory_db)

    def test_contains(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/contains.sql")
        sut = List[Any](connection=memory_db, table_name="items")
        self.assertTrue("a" in sut)
        self.assertTrue(b"a" in sut)
        self.assertTrue(None in sut)
        self.assertTrue(0 in sut)
        self.assertFalse(100 in sut)
        self.assertTrue(((0, 1), "a") in sut)
        self.assertFalse([0, 1] in sut)

        self.assertFalse("a" not in sut)
        self.assertFalse(b"a" not in sut)
        self.assertFalse(None not in sut)
        self.assertFalse(0 not in sut)
        self.assertFalse(((0, 1), "a") not in sut)
        self.assertTrue(100 not in sut)
        self.assertTrue([0, 1] not in sut)
