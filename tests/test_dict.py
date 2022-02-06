import sqlite3
import sys
import warnings
from collections.abc import Hashable
from typing import Any
from unittest.mock import MagicMock, patch

if sys.version_info >= (3, 9):
    from collections.abc import Callable, ItemsView, Iterator, KeysView, ValuesView
else:
    from typing import ItemsView, KeysView, ValuesView, Iterator, Callable

from test_base import SqlTestCase

import sqlitecollections as sc


class DictAndViewTestCase(SqlTestCase):
    def assert_items_table_only(self, conn: sqlite3.Connection) -> None:
        return self.assert_metadata_state_equals(conn, [("items", "0", "Dict")])


class DictTestCase(DictAndViewTestCase):
    def assert_dict_state_equals(self, conn: sqlite3.Connection, expected: Any) -> None:
        return self.assert_sql_result_equals(
            conn,
            """
                SELECT serialized_key, serialized_value, item_order
                FROM items ORDER BY item_order
            """,
            expected,
        )

    @patch("sqlitecollections.Dict.table_name", return_value="items")
    @patch("sqlitecollections.Dict._initialize", return_value=None)
    @patch("sqlitecollections.base.SqliteCollectionBase.__init__", return_value=None)
    @patch("sqlitecollections.base.SqliteCollectionBase.__del__", return_value=None)
    def test_serializer_argument_is_deprecated(
        self,
        SqliteCollectionBase_del: MagicMock,
        SqliteCollectionBase_init: MagicMock,
        _initialize: MagicMock,
        _table_name: MagicMock,
    ) -> None:
        def serializer(x: str) -> bytes:
            return x.encode("utf-8")

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            sut = sc.Dict[Hashable, Any](serializer=serializer)
            self.assertEqual(len(w), 1)
            self.assertTrue(issubclass(w[0].category, DeprecationWarning))
            self.assertEqual(
                str(w[0].message), "serializer argument is deprecated. use key_serializer or value_serializer instead"
            )

    @patch("sqlitecollections.Dict.table_name", return_value="items")
    @patch("sqlitecollections.Dict._initialize", return_value=None)
    @patch("sqlitecollections.base.SqliteCollectionBase.__init__", return_value=None)
    @patch("sqlitecollections.base.SqliteCollectionBase.__del__", return_value=None)
    def test_deserializer_argument_is_deprecated(
        self,
        SqliteCollectionBase_del: MagicMock,
        SqliteCollectionBase_init: MagicMock,
        _initialize: MagicMock,
        _table_name: MagicMock,
    ) -> None:
        def deserializer(x: bytes) -> str:
            return x.decode("utf-8")

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            sut = sc.Dict[Hashable, Any](deserializer=deserializer)
            self.assertEqual(len(w), 1)
            self.assertTrue(issubclass(w[0].category, DeprecationWarning))
            self.assertEqual(
                str(w[0].message),
                "deserializer argument is deprecated. use key_deserializer or value_deserializer instead",
            )

    @patch("sqlitecollections.Dict.table_name", return_value="items")
    @patch("sqlitecollections.Dict._initialize", return_value=None)
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
        value_serializer = MagicMock(spec=Callable[[Any], bytes])
        value_deserializer = MagicMock(spec=Callable[[bytes], Any])
        key_serializer = MagicMock(spec=Callable[[Hashable], bytes])
        key_deserializer = MagicMock(spec=Callable[[bytes], Hashable])
        persist = False
        rebuild_strategy = sc.RebuildStrategy.SKIP
        sut = sc.Dict[Hashable, Any](
            connection=memory_db,
            table_name=table_name,
            value_serializer=value_serializer,
            value_deserializer=value_deserializer,
            key_serializer=key_serializer,
            key_deserializer=key_deserializer,
            persist=persist,
            rebuild_strategy=rebuild_strategy,
        )
        SqliteCollectionBase_init.assert_called_once_with(
            connection=memory_db,
            table_name=table_name,
            serializer=key_serializer,
            deserializer=key_deserializer,
            persist=persist,
            rebuild_strategy=rebuild_strategy,
        )
        self.assertEqual(sut.value_serializer, value_serializer)
        self.assertEqual(sut.value_deserializer, value_deserializer)

    def test_initialize(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        self.assert_sql_result_equals(
            memory_db,
            "SELECT table_name, schema_version, container_type FROM metadata",
            [
                (
                    "items",
                    sut.schema_version,
                    sut.container_type_name,
                ),
            ],
        )
        self.assert_dict_state_equals(
            memory_db,
            [],
        )

    def test_rebuild(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/rebuild.sql")

        def serializer(x: str) -> bytes:
            return x.upper().encode("utf-8")

        def deserializer(x: bytes) -> str:
            return str(x)

        sut = sc.Dict[str, str](
            connection=memory_db,
            table_name="items",
            key_serializer=serializer,
            key_deserializer=deserializer,
            value_serializer=serializer,
            value_deserializer=deserializer,
            rebuild_strategy=sc.RebuildStrategy.ALWAYS,
        )
        self.assert_dict_state_equals(memory_db, [(b"A", b"B", 0)])

    def test_init_with_initial_data(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items", data=(("a", 1), ("b", 2)))
        self.assert_dict_state_equals(
            memory_db,
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("a"),
                    sc.base.SqliteCollectionBase._default_serializer(1),
                    0,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("b"),
                    sc.base.SqliteCollectionBase._default_serializer(2),
                    1,
                ),
            ],
        )
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items", data=(("c", 3), ("d", 4)))
        self.assert_dict_state_equals(
            memory_db,
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("c"),
                    sc.base.SqliteCollectionBase._default_serializer(3),
                    0,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("d"),
                    sc.base.SqliteCollectionBase._default_serializer(4),
                    1,
                ),
            ],
        )

    def test_getitem(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/getitem.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        expected1 = 4
        actual1 = sut["a"]
        self.assertEqual(actual1, expected1)
        expected2 = [1, 2]
        actual2 = sut["d"]
        self.assertEqual(actual2, expected2)
        with self.assertRaisesRegex(KeyError, "nonsuch"):
            _ = sut["nonsuch"]
        with self.assertRaisesRegex(TypeError, r"unhashable type:"):
            _ = sut[[0, 1]]  # type: ignore

    def test_delitem(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/delitem.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        del sut["b"]
        self.assert_dict_state_equals(
            memory_db,
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("a"),
                    sc.base.SqliteCollectionBase._default_serializer(4),
                    0,
                ),
            ],
        )
        with self.assertRaisesRegex(KeyError, "b"):
            del sut["b"]

        del sut["a"]
        self.assert_dict_state_equals(
            memory_db,
            [],
        )
        with self.assertRaisesRegex(TypeError, r"unhashable type:"):
            del sut[[0, 1]]  # type: ignore

    def test_setitem(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        self.assert_dict_state_equals(
            memory_db,
            [],
        )
        sut["akey"] = {"a": "dict"}
        self.assert_dict_state_equals(
            memory_db,
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("akey"),
                    sc.base.SqliteCollectionBase._default_serializer({"a": "dict"}),
                    0,
                )
            ],
        )
        sut["anotherkey"] = ["a", "b"]
        self.assert_dict_state_equals(
            memory_db,
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("akey"),
                    sc.base.SqliteCollectionBase._default_serializer({"a": "dict"}),
                    0,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("anotherkey"),
                    sc.base.SqliteCollectionBase._default_serializer(["a", "b"]),
                    1,
                ),
            ],
        )
        sut["akey"] = None
        self.assert_dict_state_equals(
            memory_db,
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("akey"),
                    sc.base.SqliteCollectionBase._default_serializer(None),
                    0,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("anotherkey"),
                    sc.base.SqliteCollectionBase._default_serializer(["a", "b"]),
                    1,
                ),
            ],
        )
        with self.assertRaisesRegex(TypeError, r"unhashable type:"):
            sut[[0, 1]] = 0  # type: ignore

    def test_len(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        expected = 0
        actual = len(sut)
        self.assertEqual(actual, expected)
        self.get_fixture(memory_db, "dict/len.sql")
        expected = 4
        actual = len(sut)
        self.assertEqual(actual, expected)

    def test_contains(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/contains.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        self.assertTrue("a" in sut)
        self.assertTrue(b"a" in sut)
        self.assertTrue(None in sut)
        self.assertTrue(0 in sut)
        self.assertFalse(100 in sut)
        self.assertTrue(((0, 1), "a") in sut)
        with self.assertRaisesRegex(TypeError, r"unhashable type:"):
            _ = [0, 1] in sut  # type: ignore

        self.assertFalse("a" not in sut)
        self.assertFalse(b"a" not in sut)
        self.assertFalse(None not in sut)
        self.assertFalse(0 not in sut)
        self.assertFalse(((0, 1), "a") not in sut)
        self.assertTrue(100 not in sut)
        with self.assertRaisesRegex(TypeError, r"unhashable type:"):
            _ = [0, 1] not in sut  # type: ignore

    def test_iter(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        actual = iter(sut)
        self.assertIsInstance(actual, Iterator)
        self.assertEqual(list(actual), [])
        self.assertEqual(list(actual), [])
        self.get_fixture(memory_db, "dict/iter.sql")
        actual = iter(sut)
        self.assertIsInstance(actual, Iterator)
        self.assertEqual(list(actual), ["a", "b", "c", "d"])

    def test_clear(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        self.assert_dict_state_equals(memory_db, [])
        sut.clear()
        self.assert_dict_state_equals(memory_db, [])
        self.get_fixture(memory_db, "dict/clear.sql")
        sut = sc.Dict(connection=memory_db, table_name="items")
        self.assert_dict_state_equals(
            memory_db,
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("a"),
                    sc.base.SqliteCollectionBase._default_serializer(4),
                    0,
                )
            ],
        )
        sut.clear()
        self.assert_dict_state_equals(memory_db, [])

    def test_get(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/get.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        expected1 = 4
        actual1 = sut.get("a")
        self.assertEqual(actual1, expected1)
        expected2 = [1, 2]
        actual2 = sut.get("d")
        self.assertEqual(actual2, expected2)
        expected3 = None
        actual3 = sut.get("nonsuch")
        self.assertEqual(actual3, expected3)
        expected4 = "default"
        actual4 = sut.get("nonsuch", "default")
        self.assertEqual(actual4, expected4)
        with self.assertRaisesRegex(TypeError, r"unhashable type:"):
            _ = sut.get([0, 1])  # type: ignore

    def test_items(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/items.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        actual = sut.items()
        self.assertIsInstance(actual, ItemsView)
        expected = [("a", 4), ("b", 2)]
        self.assertEqual(list(actual), expected)

    def test_keys(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/keys.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        actual = sut.keys()
        self.assertIsInstance(actual, KeysView)
        expected = ["a", "b"]
        self.assertEqual(list(actual), expected)

    def test_pop(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/pop.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        self.assert_dict_state_equals(
            memory_db,
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("a"),
                    sc.base.SqliteCollectionBase._default_serializer(4),
                    0,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("b"),
                    sc.base.SqliteCollectionBase._default_serializer(2),
                    1,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("c"),
                    sc.base.SqliteCollectionBase._default_serializer(None),
                    2,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("d"),
                    sc.base.SqliteCollectionBase._default_serializer([1, 2]),
                    3,
                ),
            ],
        )
        expected1 = 4
        actual1 = sut.pop("a")
        self.assertEqual(actual1, expected1)
        self.assert_dict_state_equals(
            memory_db,
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("b"),
                    sc.base.SqliteCollectionBase._default_serializer(2),
                    1,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("c"),
                    sc.base.SqliteCollectionBase._default_serializer(None),
                    2,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("d"),
                    sc.base.SqliteCollectionBase._default_serializer([1, 2]),
                    3,
                ),
            ],
        )
        expected2 = "default"
        actual2 = sut.pop("nonsuch", "default")
        self.assertEqual(actual2, expected2)
        self.assert_dict_state_equals(
            memory_db,
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("b"),
                    sc.base.SqliteCollectionBase._default_serializer(2),
                    1,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("c"),
                    sc.base.SqliteCollectionBase._default_serializer(None),
                    2,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("d"),
                    sc.base.SqliteCollectionBase._default_serializer([1, 2]),
                    3,
                ),
            ],
        )
        with self.assertRaisesRegex(KeyError, "nonsuch"):
            _ = sut.pop("nonsuch")
        self.assert_dict_state_equals(
            memory_db,
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("b"),
                    sc.base.SqliteCollectionBase._default_serializer(2),
                    1,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("c"),
                    sc.base.SqliteCollectionBase._default_serializer(None),
                    2,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("d"),
                    sc.base.SqliteCollectionBase._default_serializer([1, 2]),
                    3,
                ),
            ],
        )

    def test_popitem(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/popitem.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        self.assert_dict_state_equals(
            memory_db,
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("a"),
                    sc.base.SqliteCollectionBase._default_serializer(4),
                    0,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("b"),
                    sc.base.SqliteCollectionBase._default_serializer(2),
                    1,
                ),
            ],
        )
        if sys.version_info < (3, 7):
            expected = sorted([("b", 2), ("a", 4)])
            actual = sorted([sut.popitem(), sut.popitem()])
            self.assertEqual(actual, expected)
            self.assert_dict_state_equals(memory_db, [])

            with self.assertRaisesRegex(KeyError, r"'popitem\(\): dictionary is empty'"):
                _ = sut.popitem()
        else:
            expected = ("b", 2)
            actual = sut.popitem()
            self.assertEqual(actual, expected)
            self.assert_dict_state_equals(
                memory_db,
                [
                    (
                        sc.base.SqliteCollectionBase._default_serializer("a"),
                        sc.base.SqliteCollectionBase._default_serializer(4),
                        0,
                    ),
                ],
            )

            expected = ("a", 4)
            actual = sut.popitem()
            self.assertEqual(actual, expected)
            self.assert_dict_state_equals(memory_db, [])

            with self.assertRaisesRegex(KeyError, r"'popitem\(\): dictionary is empty'"):
                _ = sut.popitem()

    def test_reversed(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/reversed.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        if sys.version_info < (3, 8):
            with self.assertRaisesRegex(TypeError, "'Dict' object is not reversible"):
                _ = reversed(sut)  # type: ignore
        else:
            actual = reversed(sut)
            self.assertIsInstance(actual, Iterator)
            expected = ["b", "a"]
            self.assertEqual(list(actual), expected)

    def test_setdefault(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/setdefault.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        self.assert_dict_state_equals(
            memory_db,
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("a"),
                    sc.base.SqliteCollectionBase._default_serializer(4),
                    0,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("b"),
                    sc.base.SqliteCollectionBase._default_serializer(2),
                    1,
                ),
            ],
        )
        expected1 = 4
        actual1 = sut.setdefault("a")
        self.assertEqual(actual1, expected1)
        self.assert_dict_state_equals(
            memory_db,
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("a"),
                    sc.base.SqliteCollectionBase._default_serializer(4),
                    0,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("b"),
                    sc.base.SqliteCollectionBase._default_serializer(2),
                    1,
                ),
            ],
        )
        expected2 = None
        actual2 = sut.setdefault("c")
        self.assertEqual(actual2, expected2)
        self.assert_dict_state_equals(
            memory_db,
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("a"),
                    sc.base.SqliteCollectionBase._default_serializer(4),
                    0,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("b"),
                    sc.base.SqliteCollectionBase._default_serializer(2),
                    1,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("c"),
                    sc.base.SqliteCollectionBase._default_serializer(None),
                    2,
                ),
            ],
        )
        expected3 = 2
        actual3 = sut.setdefault("b", "default")
        self.assertEqual(actual3, expected3)
        self.assert_dict_state_equals(
            memory_db,
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("a"),
                    sc.base.SqliteCollectionBase._default_serializer(4),
                    0,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("b"),
                    sc.base.SqliteCollectionBase._default_serializer(2),
                    1,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("c"),
                    sc.base.SqliteCollectionBase._default_serializer(None),
                    2,
                ),
            ],
        )
        expected4 = "default"
        actual4 = sut.setdefault("d", "default")
        self.assertEqual(actual4, expected4)
        self.assert_dict_state_equals(
            memory_db,
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("a"),
                    sc.base.SqliteCollectionBase._default_serializer(4),
                    0,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("b"),
                    sc.base.SqliteCollectionBase._default_serializer(2),
                    1,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("c"),
                    sc.base.SqliteCollectionBase._default_serializer(None),
                    2,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("d"),
                    sc.base.SqliteCollectionBase._default_serializer("default"),
                    3,
                ),
            ],
        )

    def test_update(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/update.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut.update({"a": 1, "e": 10})
        self.assert_dict_state_equals(
            memory_db,
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("a"),
                    sc.base.SqliteCollectionBase._default_serializer(1),
                    0,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("b"),
                    sc.base.SqliteCollectionBase._default_serializer(2),
                    1,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("e"),
                    sc.base.SqliteCollectionBase._default_serializer(10),
                    2,
                ),
            ],
        )

    def test_values(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/values.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        actual = sut.values()
        self.assertIsInstance(actual, ValuesView)
        expected = [4, 2]
        self.assertEqual(list(actual), expected)

    def test_or(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/or.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        if sys.version_info < (3, 9):
            with self.assertRaisesRegex(
                TypeError,
                r"unsupported operand type\(s\) for \|: 'Dict' and '[a-zA-Z]+'",
            ):
                sut | {"a": 1, "e": 10}  # type: ignore
        else:
            actual = sut | {"a": 1, "e": 10}
            self.assert_sql_result_equals(
                memory_db,
                f"SELECT serialized_key, serialized_value, item_order FROM {actual.table_name} ORDER BY item_order",
                [
                    (
                        sc.base.SqliteCollectionBase._default_serializer("a"),
                        sc.base.SqliteCollectionBase._default_serializer(1),
                        0,
                    ),
                    (
                        sc.base.SqliteCollectionBase._default_serializer("b"),
                        sc.base.SqliteCollectionBase._default_serializer(2),
                        1,
                    ),
                    (
                        sc.base.SqliteCollectionBase._default_serializer("e"),
                        sc.base.SqliteCollectionBase._default_serializer(10),
                        2,
                    ),
                ],
            )

    def test_ior(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/ior.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        if sys.version_info < (3, 9):
            with self.assertRaisesRegex(
                TypeError,
                r"unsupported operand type\(s\) for \|=: 'Dict' and '[a-zA-Z]+'",
            ):
                sut |= {"a": 1, "e": 10}  # type: ignore
        else:
            sut |= {"a": 1, "e": 10}
            self.assert_dict_state_equals(
                memory_db,
                [
                    (
                        sc.base.SqliteCollectionBase._default_serializer("a"),
                        sc.base.SqliteCollectionBase._default_serializer(1),
                        0,
                    ),
                    (
                        sc.base.SqliteCollectionBase._default_serializer("b"),
                        sc.base.SqliteCollectionBase._default_serializer(2),
                        1,
                    ),
                    (
                        sc.base.SqliteCollectionBase._default_serializer("e"),
                        sc.base.SqliteCollectionBase._default_serializer(10),
                        2,
                    ),
                ],
            )

    def test_copy(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/copy.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        actual = sut.copy()

        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_key, serialized_value, item_order FROM {actual.table_name}",
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("a"),
                    sc.base.SqliteCollectionBase._default_serializer(4),
                    0,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("b"),
                    sc.base.SqliteCollectionBase._default_serializer(2),
                    1,
                ),
            ],
        )
        del actual
        self.assert_items_table_only(memory_db)


class KeysViewTestCase(DictAndViewTestCase):
    def test_len(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.keys()
        expected = 0
        actual = len(sut)
        self.assertEqual(actual, expected)

        self.get_fixture(memory_db, "dict/keysview_len.sql")
        expected = 4
        actual = len(sut)
        self.assertEqual(actual, expected)

    def test_iter(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.keys()
        actual = iter(sut)
        self.assertIsInstance(actual, Iterator)
        self.assertEqual(list(actual), [])
        self.assertEqual(list(actual), [])
        self.get_fixture(memory_db, "dict/keysview_iter.sql")
        actual = iter(sut)
        self.assertIsInstance(actual, Iterator)
        self.assertEqual(list(actual), ["a", "b", "c", "d"])
        del parent["b"]
        actual = iter(sut)
        self.assertIsInstance(actual, Iterator)
        self.assertEqual(list(actual), ["a", "c", "d"])

    def test_contains(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/keysview_contains.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.keys()
        self.assertTrue("a" in sut)
        self.assertTrue(b"a" in sut)
        self.assertTrue(None in sut)
        self.assertTrue(0 in sut)
        self.assertFalse(100 in sut)
        self.assertTrue(((0, 1), "a") in sut)
        with self.assertRaisesRegex(TypeError, r"unhashable type:"):
            _ = [0, 1] in sut  # type: ignore

        self.assertFalse("a" not in sut)
        self.assertFalse(b"a" not in sut)
        self.assertFalse(None not in sut)
        self.assertFalse(0 not in sut)
        self.assertFalse(((0, 1), "a") not in sut)
        self.assertTrue(100 not in sut)
        with self.assertRaisesRegex(TypeError, r"unhashable type:"):
            _ = [0, 1] not in sut  # type: ignore

        del parent["a"]
        self.assertFalse("a" in sut)

    def test_reversed(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/keysview_reversed.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.keys()
        if sys.version_info < (3, 8):
            with self.assertRaisesRegex(TypeError, "'KeysView' object is not reversible"):
                _ = reversed(sut)  # type: ignore
        else:
            actual = reversed(sut)
            self.assertIsInstance(actual, Iterator)
            expected = ["b", "a"]
            self.assertEqual(list(actual), expected)

    def test_and(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/keysview_and.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.keys()
        actual = sut & {1, 2, 3}
        self.assertIsInstance(actual, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name}",
            [],
        )
        del actual
        self.assert_items_table_only(memory_db)

        actual = sut & {"a", "b"} & {"b"}
        self.assertIsInstance(actual, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name}",
            [
                (sc.base.SqliteCollectionBase._default_serializer("b"),),
            ],
        )
        del actual
        self.assert_items_table_only(memory_db)

    def test_rand(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/keysview_rand.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.keys()
        actual = iter((1, 2, 3)) & sut
        self.assertIsInstance(actual, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name}",
            [],
        )
        del actual
        self.assert_items_table_only(memory_db)

        actual2 = iter(("a", "b")) & sut
        self.assertIsInstance(actual2, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual2.table_name}",
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"),),
                (sc.base.SqliteCollectionBase._default_serializer("b"),),
            ],
        )
        del actual2
        self.assert_items_table_only(memory_db)

    def test_or(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/keysview_or.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.keys()
        actual = sut | iter((1, 2, 3))
        self.assertIsInstance(actual, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name} ORDER BY serialized_value",
            sorted(
                [
                    (sc.base.SqliteCollectionBase._default_serializer("a"),),
                    (sc.base.SqliteCollectionBase._default_serializer("b"),),
                    (sc.base.SqliteCollectionBase._default_serializer("c"),),
                    (sc.base.SqliteCollectionBase._default_serializer(1),),
                    (sc.base.SqliteCollectionBase._default_serializer(2),),
                    (sc.base.SqliteCollectionBase._default_serializer(3),),
                ]
            ),
        )
        del actual
        self.assert_items_table_only(memory_db)

        actual2 = sut | iter(("a", "b"))
        self.assertIsInstance(actual2, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual2.table_name} ORDER BY serialized_value",
            sorted(
                [
                    (sc.base.SqliteCollectionBase._default_serializer("a"),),
                    (sc.base.SqliteCollectionBase._default_serializer("b"),),
                    (sc.base.SqliteCollectionBase._default_serializer("c"),),
                ]
            ),
        )
        del actual2
        self.assert_items_table_only(memory_db)

    def test_ror(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/keysview_ror.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.keys()
        actual = iter((1, 2, 3)) | sut
        self.assertIsInstance(actual, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name} ORDER BY serialized_value",
            sorted(
                [
                    (sc.base.SqliteCollectionBase._default_serializer("a"),),
                    (sc.base.SqliteCollectionBase._default_serializer("b"),),
                    (sc.base.SqliteCollectionBase._default_serializer("c"),),
                    (sc.base.SqliteCollectionBase._default_serializer(1),),
                    (sc.base.SqliteCollectionBase._default_serializer(2),),
                    (sc.base.SqliteCollectionBase._default_serializer(3),),
                ]
            ),
        )
        del actual
        self.assert_items_table_only(memory_db)

        actual2 = iter(("a", "b")) | sut
        self.assertIsInstance(actual2, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual2.table_name} ORDER BY serialized_value",
            sorted(
                [
                    (sc.base.SqliteCollectionBase._default_serializer("a"),),
                    (sc.base.SqliteCollectionBase._default_serializer("b"),),
                    (sc.base.SqliteCollectionBase._default_serializer("c"),),
                ]
            ),
        )
        del actual2
        self.assert_items_table_only(memory_db)

    def test_sub(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/keysview_sub.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.keys()
        actual = sut - iter((1, 2, 3))
        self.assertIsInstance(actual, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name} ORDER BY serialized_value",
            sorted(
                [
                    (sc.base.SqliteCollectionBase._default_serializer("a"),),
                    (sc.base.SqliteCollectionBase._default_serializer("b"),),
                    (sc.base.SqliteCollectionBase._default_serializer("c"),),
                ]
            ),
        )
        del actual
        self.assert_items_table_only(memory_db)

        actual2 = sut - iter(("a", "b"))
        self.assertIsInstance(actual2, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual2.table_name} ORDER BY serialized_value",
            sorted(
                [
                    (sc.base.SqliteCollectionBase._default_serializer("c"),),
                ]
            ),
        )
        del actual2
        self.assert_items_table_only(memory_db)

    def test_rsub(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/keysview_rsub.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.keys()
        actual = iter((1, 2, 3)) - sut
        self.assertIsInstance(actual, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name} ORDER BY serialized_value",
            sorted(
                [
                    (sc.base.SqliteCollectionBase._default_serializer(1),),
                    (sc.base.SqliteCollectionBase._default_serializer(2),),
                    (sc.base.SqliteCollectionBase._default_serializer(3),),
                ]
            ),
        )
        del actual
        self.assert_items_table_only(memory_db)

        actual2 = iter(("a", "b")) - sut
        self.assertIsInstance(actual2, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual2.table_name} ORDER BY serialized_value",
            sorted([]),
        )
        del actual2
        self.assert_items_table_only(memory_db)

    def test_xor(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/keysview_xor.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.keys()
        actual = sut ^ iter((1, 2, 3))
        self.assertIsInstance(actual, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name} ORDER BY serialized_value",
            sorted(
                [
                    (sc.base.SqliteCollectionBase._default_serializer("a"),),
                    (sc.base.SqliteCollectionBase._default_serializer("b"),),
                    (sc.base.SqliteCollectionBase._default_serializer("c"),),
                    (sc.base.SqliteCollectionBase._default_serializer(1),),
                    (sc.base.SqliteCollectionBase._default_serializer(2),),
                    (sc.base.SqliteCollectionBase._default_serializer(3),),
                ]
            ),
        )
        del actual
        self.assert_items_table_only(memory_db)

        actual2 = sut ^ iter(("a", "b", "d"))
        self.assertIsInstance(actual2, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual2.table_name} ORDER BY serialized_value",
            sorted(
                [
                    (sc.base.SqliteCollectionBase._default_serializer("c"),),
                    (sc.base.SqliteCollectionBase._default_serializer("d"),),
                ]
            ),
        )
        del actual2
        self.assert_items_table_only(memory_db)

    def test_rxor(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/keysview_rxor.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.keys()
        actual = iter((1, 2, 3)) ^ sut
        self.assertIsInstance(actual, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name} ORDER BY serialized_value",
            sorted(
                [
                    (sc.base.SqliteCollectionBase._default_serializer("a"),),
                    (sc.base.SqliteCollectionBase._default_serializer("b"),),
                    (sc.base.SqliteCollectionBase._default_serializer("c"),),
                    (sc.base.SqliteCollectionBase._default_serializer(1),),
                    (sc.base.SqliteCollectionBase._default_serializer(2),),
                    (sc.base.SqliteCollectionBase._default_serializer(3),),
                ]
            ),
        )
        del actual
        self.assert_items_table_only(memory_db)

        actual2 = iter(("a", "b", "d")) ^ sut
        self.assertIsInstance(actual2, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual2.table_name} ORDER BY serialized_value",
            sorted(
                [
                    (sc.base.SqliteCollectionBase._default_serializer("c"),),
                    (sc.base.SqliteCollectionBase._default_serializer("d"),),
                ]
            ),
        )
        del actual2
        self.assert_items_table_only(memory_db)


class ValuesViewTestCase(DictAndViewTestCase):
    def test_len(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.values()
        expected = 0
        actual = len(sut)
        self.assertEqual(actual, expected)

        self.get_fixture(memory_db, "dict/valuesview_len.sql")
        expected = 4
        actual = len(sut)
        self.assertEqual(actual, expected)

    def test_contains(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/valuesview_contains.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.values()

        self.assertTrue("a" in sut)
        self.assertTrue(b"a" in sut)
        self.assertTrue(None in sut)
        self.assertTrue(0 in sut)
        self.assertFalse(100 in sut)
        self.assertTrue(((0, 1), "a") in sut)

        self.assertFalse("a" not in sut)
        self.assertFalse(b"a" not in sut)
        self.assertFalse(None not in sut)
        self.assertFalse(0 not in sut)
        self.assertFalse(((0, 1), "a") not in sut)
        self.assertTrue(100 not in sut)

    def test_iter(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.values()
        actual = iter(sut)
        self.assertIsInstance(actual, Iterator)
        self.assertEqual(list(actual), [])
        self.assertEqual(list(actual), [])
        self.get_fixture(memory_db, "dict/valuesview_iter.sql")
        actual = iter(sut)
        self.assertIsInstance(actual, Iterator)
        self.assertEqual(list(actual), [4, 2, None, [1, 2]])
        del parent["b"]
        actual = iter(sut)
        self.assertIsInstance(actual, Iterator)
        self.assertEqual(list(actual), [4, None, [1, 2]])

    def test_reversed(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/valuesview_reversed.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.values()
        if sys.version_info < (3, 8):
            with self.assertRaisesRegex(TypeError, "'ValuesView' object is not reversible"):
                _ = reversed(sut)  # type: ignore
        else:
            actual = reversed(sut)
            self.assertIsInstance(actual, Iterator)
            expected = [2, 4]
            self.assertEqual(list(actual), expected)


class ItemsViewTestCase(DictAndViewTestCase):
    def test_len(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.items()
        expected = 0
        actual = len(sut)
        self.assertEqual(actual, expected)

        self.get_fixture(memory_db, "dict/itemsview_len.sql")
        expected = 4
        actual = len(sut)
        self.assertEqual(actual, expected)

    def test_iter(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.items()
        actual = iter(sut)
        self.assertIsInstance(actual, Iterator)
        self.assertEqual(list(actual), [])
        self.assertEqual(list(actual), [])
        self.get_fixture(memory_db, "dict/itemsview_iter.sql")
        actual = iter(sut)
        self.assertIsInstance(actual, Iterator)
        self.assertEqual(list(actual), [("a", 4), ("b", 2), ("c", None), ("d", [1, 2])])
        del parent["b"]
        actual = iter(sut)
        self.assertIsInstance(actual, Iterator)
        self.assertEqual(list(actual), [("a", 4), ("c", None), ("d", [1, 2])])

    def test_and(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/itemsview_and.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.items()
        actual = sut & {("a", 1), ("b", 2)}
        self.assertIsInstance(actual, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name}",
            [],
        )
        del actual
        self.assert_items_table_only(memory_db)

        actual = sut & {("a", 4), ("b", 4)} & {("b", 4)}
        self.assertIsInstance(actual, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name}",
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer(
                        (
                            sc.base.SqliteCollectionBase._default_serializer("b"),
                            sc.base.SqliteCollectionBase._default_serializer(4),
                        )
                    ),
                ),
            ],
        )
        del actual
        self.assert_items_table_only(memory_db)

    def test_and_fails_if_unhashable_value_exists(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/itemsview_and_unhashable_type_error.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.items()
        with self.assertRaisesRegex(TypeError, r"unhashable type: '[a-zA-Z0-9_]+'"):
            _ = sut & {("a", 1)}
