import pickle
import sqlite3
import sys
import warnings
from itertools import product
from typing import Any, Tuple, Union
from unittest.mock import MagicMock, patch

if sys.version_info >= (3, 9):
    from collections.abc import Callable, Iterable, Sequence
else:
    from typing import Callable, Sequence, Iterable

import sqlitecollections as sc

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

    @patch("sqlitecollections.List.table_name", return_value="items")
    @patch("sqlitecollections.List._initialize", return_value=None)
    @patch("sqlitecollections.base.SqliteCollectionBase.__init__", return_value=None)
    @patch("sqlitecollections.base.SqliteCollectionBase.__del__", return_value=None)
    def test_init(
        self,
        SqliteCollectionBase_del: MagicMock,
        SqliteCollectionBase_init: MagicMock,
        _initialize: MagicMock,
        _table_name: MagicMock,
    ) -> None:
        memory_db = sqlite3.connect(":memory:")
        table_name = "items"
        serializer = MagicMock(spec=Callable[[Any], bytes])
        deserializer = MagicMock(spec=Callable[[bytes], Any])
        persist = False
        sut = sc.List[Any](
            connection=memory_db,
            table_name=table_name,
            serializer=serializer,
            deserializer=deserializer,
            persist=persist,
        )
        SqliteCollectionBase_init.assert_called_once_with(
            connection=memory_db,
            table_name=table_name,
            serializer=serializer,
            deserializer=deserializer,
            persist=persist,
        )

    def test_initialize(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        sut = sc.List[Any](connection=memory_db, table_name="items")
        self.assert_sql_result_equals(
            memory_db,
            "SELECT table_name, schema_version, container_type FROM metadata",
            [("items", sut._driver_class.schema_version, sut.container_type_name)],
        )
        self.assert_db_state_equals(memory_db, [])

    def test_init_with_kwarg_data_raises_error(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        with self.assertRaisesRegex(TypeError, ".+ got an unexpected keyword argument 'data'"):
            _ = sc.List[Any](connection=memory_db, table_name="items", data=("a", "b"))  # type: ignore

    def test_init_with_initial_data(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        sut = sc.List[Any]([0], connection=memory_db, table_name="items")
        self.assert_db_state_equals(memory_db, [(sc.base.SqliteCollectionBase._default_serializer(0), 0)])
        sut = sc.List[Any]([1], connection=memory_db, table_name="items")
        self.assert_db_state_equals(memory_db, [(sc.base.SqliteCollectionBase._default_serializer(1), 0)])

    def test_getitem_int(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/getitem_int.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
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

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/getitem_slice.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        expected_array = [chr(i) for i in range(ord("a"), ord("z") + 1)]

        def generate_expectation(s: slice) -> Sequence[Tuple[bytes, int]]:
            return [(sc.base.SqliteCollectionBase._default_serializer(c), i) for i, c in enumerate(expected_array[s])]

        for si in product(
            (None, -100, -26, -25, -20, 0, 20, 25, 26, 100),
            (None, -100, -26, -25, -20, 0, 20, 25, 26, 100),
            (None, -100, -26, -25, -20, -10, 0, 10, 20, 25, 26, 100),
        ):
            s = slice(*si)
            if si[-1] == 0:
                with self.assertRaisesRegex(ValueError, "slice step cannot be zero"):
                    _ = sut[s]
            else:
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
        sut = sc.List[Any](connection=memory_db, table_name="items")
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

    def test_setitem_int(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/setitem_int.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 2),
            ],
        )
        sut[0] = "A"
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("A"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 2),
            ],
        )
        sut[-1] = "C"
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("A"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("C"), 2),
            ],
        )
        with self.assertRaisesRegex(IndexError, "list assignment index out of range"):
            sut[100] = "Z"
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("A"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("C"), 2),
            ],
        )
        with self.assertRaisesRegex(IndexError, "list assignment index out of range"):
            sut[-100] = "z"
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("A"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("C"), 2),
            ],
        )

    def test_setitem_slice(self) -> None:
        def generate_expectation(s: slice, t: Iterable[str]) -> Union[Exception, Sequence[Tuple[bytes, int]]]:
            a = ["a", "b", "c", "d", "e"]
            try:
                a[s] = t
            except Exception as e:
                return e
            return [(sc.base.SqliteCollectionBase._default_serializer(c), i) for i, c in enumerate(a)]

        def generate_new_value(length: int) -> Iterable[str]:
            t = [chr(ord("A") + i) for i in range(length)]
            return iter(t)

        for start, stop, step, length in product(
            (None, -10, -5, -2, -1, 0, 1, 2, 5, 10),
            (None, -10, -5, -2, -1, 0, 1, 2, 5, 10),
            (None, -10, -5, -2, -1, 0, 1, 2, 5, 10),
            (0, 1, 2, 3, 6),
        ):
            memory_db = sqlite3.connect(":memory:")
            self.get_fixture(memory_db, "list/base.sql", "list/setitem_slice.sql")
            sut = sc.List[Any](connection=memory_db, table_name="items")
            s = slice(start, stop, step)
            expected = generate_expectation(s, generate_new_value(length))
            if isinstance(expected, Exception):
                with self.assertRaisesRegex(type(expected), str(expected)):
                    sut[s] = generate_new_value(length)
            else:
                sut[s] = generate_new_value(length)
                self.assert_db_state_equals(
                    memory_db,
                    expected,
                )
        with self.assertRaisesRegex(TypeError, "must assign iterable to extended slice"):
            memory_db = sqlite3.connect(":memory:")
            self.get_fixture(memory_db, "list/base.sql", "list/setitem_slice.sql")
            sut = sc.List[Any](connection=memory_db, table_name="items")
            sut[::] = 1

    def test_delitem_int(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/delitem_int.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 2),
            ],
        )
        del sut[0]
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("b"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 1),
            ],
        )

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/delitem_int.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        del sut[1]
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 1),
            ],
        )

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/delitem_int.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        del sut[2]
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
            ],
        )

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/delitem_int.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        with self.assertRaisesRegex(IndexError, "list index out of range"):
            _ = sut[3]

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/delitem_int.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        del sut[-3]
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("b"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 1),
            ],
        )

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/delitem_int.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        del sut[-2]
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 1),
            ],
        )

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/delitem_int.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        del sut[-1]
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
            ],
        )

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/delitem_int.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        with self.assertRaisesRegex(IndexError, "list index out of range"):
            _ = sut[-4]

    def test_delitem_slice(self) -> None:
        def generate_expectation(s: slice) -> Union[Exception, Sequence[Tuple[bytes, int]]]:
            a = ["a", "b", "c", "d", "e", "f", "g"]
            try:
                del a[s]
            except Exception as e:
                return e
            return [(sc.base.SqliteCollectionBase._default_serializer(c), i) for i, c in enumerate(a)]

        for start, stop, step in product(
            (None, -10, -8, -7, -2, -1, 0, 1, 2, 7, 8, 10),
            (None, -10, -8, -7, -2, -1, 0, 1, 2, 7, 8, 10),
            (None, -10, -8, -7, -2, -1, 0, 1, 2, 7, 8, 10),
        ):
            memory_db = sqlite3.connect(":memory:")
            self.get_fixture(memory_db, "list/base.sql", "list/delitem_slice.sql")
            sut = sc.List[str](connection=memory_db, table_name="items")
            s = slice(start, stop, step)
            expected = generate_expectation(s)
            if isinstance(expected, Exception):
                with self.assertRaisesRegex(type(expected), str(expected)):
                    del sut[s]
            else:
                del sut[s]
                self.assert_db_state_equals(
                    memory_db,
                    expected,
                )

    def test_insert(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/insert.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 2),
            ],
        )
        sut.insert(0, "z")
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("z"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("a"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 2),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 3),
            ],
        )

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/insert.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 2),
            ],
        )
        sut.insert(2, "z")
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("z"), 2),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 3),
            ],
        )

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/insert.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 2),
            ],
        )
        sut.insert(3, "z")
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 2),
                (sc.base.SqliteCollectionBase._default_serializer("z"), 3),
            ],
        )

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/insert.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 2),
            ],
        )
        sut.insert(300000, "z")
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 2),
                (sc.base.SqliteCollectionBase._default_serializer("z"), 3),
            ],
        )

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/insert.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 2),
            ],
        )
        sut.insert(-3, "z")
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("z"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("a"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 2),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 3),
            ],
        )

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/insert.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 2),
            ],
        )
        sut.insert(-1, "z")
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("z"), 2),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 3),
            ],
        )

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/insert.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 2),
            ],
        )
        sut.insert(-3000, "z")
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("z"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("a"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 2),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 3),
            ],
        )

    def test_append(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        self.assert_db_state_equals(memory_db, [])
        sut.append("a")
        self.assert_db_state_equals(memory_db, [(sc.base.SqliteCollectionBase._default_serializer("a"), 0)])
        sut.append("z")
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("z"), 1),
            ],
        )

    def test_clear(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        sut.clear()
        self.assert_db_state_equals(memory_db, [])

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/clear.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 2),
            ],
        )
        sut.clear()
        self.assert_db_state_equals(memory_db, [])

    def test_copy(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        actual = sut.copy()
        self.assert_db_state_equals(memory_db, [], actual.table_name)

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/copy.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 2),
            ],
        )
        actual = sut.copy()
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 2),
            ],
            actual.table_name,
        )
        del actual
        self.assert_items_table_only(memory_db)

    def test_extend(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        sut.extend(["a", "b", "c"])
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 2),
            ],
        )

        sut.extend(["a", "b", "c"])
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 2),
                (sc.base.SqliteCollectionBase._default_serializer("a"), 3),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 4),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 5),
            ],
        )

    def test_iadd(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        sut += ["a", "b", "c"]
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 2),
            ],
        )

        sut += ["a", "b", "c"]
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 2),
                (sc.base.SqliteCollectionBase._default_serializer("a"), 3),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 4),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 5),
            ],
        )

    def test_add(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        actual = sut + ["a", "b", "c"]
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 2),
            ],
            actual.table_name,
        )

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/add.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        actual = sut + ["a", "b", "c"]
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 2),
                (sc.base.SqliteCollectionBase._default_serializer("a"), 3),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 4),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 5),
            ],
            actual.table_name,
        )
        del actual
        self.assert_items_table_only(memory_db)

    def test_imul(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        sut *= 0
        self.assert_db_state_equals(memory_db, [])
        sut *= 1
        self.assert_db_state_equals(memory_db, [])
        sut *= -1
        self.assert_db_state_equals(memory_db, [])
        with self.assertRaisesRegex(TypeError, "can't multiply sequence by non-int of type 'float'"):
            _ = sut * 1.2  # type: ignore
        self.assert_items_table_only(memory_db)

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/imul.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")

        sut *= 1
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 2),
            ],
        )
        sut *= 2
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 2),
                (sc.base.SqliteCollectionBase._default_serializer("a"), 3),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 4),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 5),
            ],
        )
        sut *= -1
        self.assert_db_state_equals(
            memory_db,
            [],
        )
        self.assert_items_table_only(memory_db)
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/imul.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        sut *= 0
        self.assert_db_state_equals(
            memory_db,
            [],
        )
        self.assert_items_table_only(memory_db)

    def test_mul(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/mul.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        actual = sut * 0
        self.assert_db_state_equals(memory_db, [], actual.table_name)

        actual = sut * 1
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 2),
            ],
            actual.table_name,
        )

        actual = sut * 2
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 2),
                (sc.base.SqliteCollectionBase._default_serializer("a"), 3),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 4),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 5),
            ],
            actual.table_name,
        )

        actual = sut * -1
        self.assert_db_state_equals(
            memory_db,
            [],
            actual.table_name,
        )
        del actual
        self.assert_items_table_only(memory_db)

        with self.assertRaisesRegex(TypeError, "can't multiply sequence by non-int of type 'float'"):
            _ = sut * 1.2  # type: ignore

    def test_len(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        expected = 0
        actual = len(sut)
        self.assertEqual(actual, expected)

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/len.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        expected = 3
        actual = len(sut)
        self.assertEqual(actual, expected)

    def test_index(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        with self.assertRaisesRegex(ValueError, "'z' is not in list"):
            sut.index("z")

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/index.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        expected = 0
        actual = sut.index("a")
        self.assertEqual(actual, expected)
        expected = 1
        actual = sut.index("b")
        self.assertEqual(actual, expected)
        expected = 2
        actual = sut.index("c")
        self.assertEqual(actual, expected)
        with self.assertRaisesRegex(ValueError, "'z' is not in list"):
            sut.index("z")
        with self.assertRaisesRegex(ValueError, "'a' is not in list"):
            sut.index("a", 1, 3)
        expected = 3
        actual = sut.index("a", 1)
        self.assertEqual(actual, expected)
        expected = 6
        actual = sut.index("a", 4)
        self.assertEqual(actual, expected)
        expected = 3
        actual = sut.index("a", 1, 4)
        self.assertEqual(actual, expected)
        with self.assertRaisesRegex(ValueError, "'a' is not in list"):
            sut.index("a", 7, 0)
        expected = 8
        actual = sut.index("c", 6)
        self.assertEqual(actual, expected)

    def test_count(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        expected = 0
        actual = sut.count("z")

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/count.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        expected = 0
        actual = sut.count("z")
        self.assertEqual(actual, expected)
        expected = 1
        actual = sut.count("a")
        self.assertEqual(actual, expected)
        expected = 2
        actual = sut.count("b")
        self.assertEqual(actual, expected)
        expected = 3
        actual = sut.count("c")
        self.assertEqual(actual, expected)

    def test_pop(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        with self.assertRaisesRegex(IndexError, "pop from empty list"):
            _ = sut.pop()
        with self.assertRaisesRegex(IndexError, "pop from empty list"):
            _ = sut.pop(0)
        with self.assertRaisesRegex(IndexError, "pop from empty list"):
            _ = sut.pop(1)
        with self.assertRaisesRegex(IndexError, "pop from empty list"):
            _ = sut.pop(-1)

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/pop.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        expected = "c"
        actual = sut.pop()
        self.assertEqual(actual, expected)
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
            ],
        )
        expected = "b"
        actual = sut.pop()
        self.assertEqual(actual, expected)
        self.assert_db_state_equals(memory_db, [(sc.base.SqliteCollectionBase._default_serializer("a"), 0)])
        expected = "a"
        actual = sut.pop()
        self.assertEqual(actual, expected)
        self.assert_db_state_equals(memory_db, [])
        with self.assertRaisesRegex(IndexError, "pop from empty list"):
            _ = sut.pop()

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/pop.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        expected = "b"
        actual = sut.pop(1)
        self.assertEqual(actual, expected)
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 1),
            ],
        )

        with self.assertRaisesRegex(IndexError, "pop index out of range"):
            _ = sut.pop(3)

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/pop.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")
        expected = "c"
        actual = sut.pop(-1)
        self.assertEqual(actual, expected)
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
            ],
        )

        with self.assertRaisesRegex(IndexError, "pop index out of range"):
            _ = sut.pop(-3)

    def test_remove(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/remove.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")

        with self.assertRaisesRegex(ValueError, "'z' is not in list"):
            sut.remove('z')

        sut.remove("a")
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("b"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("a"), 2),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 3),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 4),
            ],
        )
        sut.remove("a")
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("b"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 2),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 3),
            ],
        )
        with self.assertRaisesRegex(ValueError, "'a' is not in list"):
            sut.remove('a')

    def test_reverse(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/reverse.sql")
        sut = sc.List[str](connection=memory_db, table_name="items")

        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 2),
            ],
        )
        sut.reverse()
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("c"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("a"), 2),
            ],
        )
        sut.reverse()
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 2),
            ],
        )

    def test_sort(self) -> None:
        def generate_expected(l: Sequence[Tuple[int, int]]) -> Sequence[Tuple[bytes, int]]:
            return [(sc.base.SqliteCollectionBase._default_serializer(d), i) for i, d in enumerate(l)]

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/sort.sql")
        sut = sc.List[Tuple[int, int]](connection=memory_db, table_name="items")
        sut.sort()
        self.assert_db_state_equals(
            memory_db,
            generate_expected([(0, 0), (1, 3), (2, 2), (3, 0), (4, 1), (5, 1), (6, 0), (7, 2), (8, 1), (9, 0)]),
        )
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/sort.sql")
        sut = sc.List[Tuple[int, int]](connection=memory_db, table_name="items")
        sut.sort(key=lambda x: x[1])
        self.assert_db_state_equals(
            memory_db,
            generate_expected([(9, 0), (3, 0), (0, 0), (6, 0), (5, 1), (8, 1), (4, 1), (2, 2), (7, 2), (1, 3)]),
        )
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/sort.sql")
        sut = sc.List[Tuple[int, int]](connection=memory_db, table_name="items")
        sut.sort(reverse=True)
        self.assert_db_state_equals(
            memory_db,
            generate_expected([(9, 0), (8, 1), (7, 2), (6, 0), (5, 1), (4, 1), (3, 0), (2, 2), (1, 3), (0, 0)]),
        )
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "list/base.sql", "list/sort.sql")
        sut = sc.List[Tuple[int, int]](connection=memory_db, table_name="items")
        sut.sort(key=lambda x: x[1], reverse=True)
        self.assert_db_state_equals(
            memory_db,
            generate_expected([(1, 3), (2, 2), (7, 2), (5, 1), (8, 1), (4, 1), (9, 0), (3, 0), (0, 0), (6, 0)]),
        )

    def test_pickle(self) -> None:
        import os
        import pickle

        wd = os.path.dirname(os.path.abspath(__file__))

        db = sqlite3.connect(os.path.join(wd, "fixtures", "list", "pickle.db"))
        sut = sc.List[str](connection=db, table_name="items")
        actual = pickle.dumps(sut)
        loaded = pickle.loads(actual)
        self.assert_db_state_equals(
            loaded.connection,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"), 0),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 1),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 2),
                (sc.base.SqliteCollectionBase._default_serializer("a"), 3),
                (sc.base.SqliteCollectionBase._default_serializer("b"), 4),
                (sc.base.SqliteCollectionBase._default_serializer("c"), 5),
            ],
        )
